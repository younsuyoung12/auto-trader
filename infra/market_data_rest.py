from __future__ import annotations

"""
========================================================
FILE: infra/market_data_rest.py
ROLE:
- Binance Futures 공개 REST /fapi/v1/klines 엔드포인트에서 히스토리 캔들을 조회한다.
- run_bot_ws 시작 시 WS 버퍼 부트스트랩(backfill) 용도로만 사용한다.
- 라이브 운영 중 의사결정은 WebSocket 데이터만 사용한다.
- 반환 포맷은 WS backfill이 그대로 사용할 수 있는 공통 포맷 list[list] 이다.
  [openTime, open, high, low, close, volume, closeTime]

CORE RESPONSIBILITIES:
- Binance Futures REST Kline 조회
- 부트스트랩용 최근 N개 캔들 STRICT 정규화
- interval 기반 연속성(contiguity) 검증
- WS/backfill 공통 포맷 반환
- 네트워크/HTTP/파싱 실패를 숨기지 않고 즉시 예외 전파

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- settings.SETTINGS 단일 객체만 사용
- 숨은 기본값 금지
- REST는 부트스트랩 용도만 사용
- 한 행이라도 손상/누락/비연속이면 전체 실패
- 행 드랍 / 정렬 무시 / print 폴백 금지
- HTTP 429/5xx만 제한적 재시도, 그 외 상태코드는 즉시 실패

CHANGE HISTORY:
- 2026-03-10:
  1) FIX(SSOT): load_settings() 재호출 제거, settings.SETTINGS 단일 객체 사용
  2) FIX(SSOT): Binance REST base URL 하드코딩 제거, settings.binance_futures_rest_base 사용
  3) FIX(STRICT): ws_backfill_limit 숨은 기본값 제거
  4) FIX(ROOT-CAUSE): openTime strict contiguity(interval_ms 단위) 검증 추가
  5) FIX(STRICT): OHLC/closeTime/volume 도메인 검증 강화
- 2026-03-04:
  1) HTTP 5xx/429 재시도 + exponential backoff 추가(실전 내구성 강화)
  2) Session 재사용(성능/안정성)
  3) 정렬 실패/정규화 누락/무결성 위반 시 즉시 예외(STRICT)
  4) 예외 삼키기/행 드랍/print 폴백 제거(절대 NO-FALLBACK)
========================================================
"""

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests

from infra.telelog import log
from settings import SETTINGS

SET = SETTINGS

# Binance /fapi/v1/klines 최대 limit
_MAX_KLINES_LIMIT = 1500


class KlineRestError(RuntimeError):
    """REST 캔들 조회 실패시 사용하는 예외."""


# interval 문자열 → 밀리초 변환
_MINUTE_MS = 60_000
_HOUR_MS = 60 * 60_000
_DAY_MS = 24 * _HOUR_MS

_INTERVAL_MS: Dict[str, int] = {
    "1m": _MINUTE_MS,
    "3m": 3 * _MINUTE_MS,
    "5m": 5 * _MINUTE_MS,
    "15m": 15 * _MINUTE_MS,
    "30m": 30 * _MINUTE_MS,
    "1h": _HOUR_MS,
    "2h": 2 * _HOUR_MS,
    "4h": 4 * _HOUR_MS,
    "6h": 6 * _HOUR_MS,
    "12h": 12 * _HOUR_MS,
    "1d": _DAY_MS,
    "3d": 3 * _DAY_MS,
    "1w": 7 * _DAY_MS,
    # 월봉은 고정 길이가 아니므로 REST 연속성 검증에서는 예외 취급하지 않고
    # 단위 변환용 근사값으로만 사용한다.
    "1M": 30 * _DAY_MS,
}


def _require_setting_attr(name: str) -> Any:
    if not hasattr(SET, name):
        raise RuntimeError(f"settings.{name} is required (STRICT)")
    return getattr(SET, name)


def _require_positive_int(v: Any, *, name: str) -> int:
    if isinstance(v, bool):
        raise KlineRestError(f"{name} must be int (bool not allowed)")
    try:
        iv = int(v)
    except Exception as e:
        raise KlineRestError(f"{name} not int-convertible: {v!r}") from e
    if iv <= 0:
        raise KlineRestError(f"{name} must be > 0 (STRICT)")
    return iv


def _require_positive_float(v: Any, *, name: str) -> float:
    if isinstance(v, bool):
        raise KlineRestError(f"{name} must be float (bool not allowed)")
    try:
        fv = float(v)
    except Exception as e:
        raise KlineRestError(f"{name} not float-convertible: {v!r}") from e
    if fv != fv:
        raise KlineRestError(f"{name} is NaN")
    if fv in (float("inf"), float("-inf")):
        raise KlineRestError(f"{name} is infinite")
    if fv <= 0.0:
        raise KlineRestError(f"{name} must be > 0 (STRICT)")
    return fv


def _require_nonempty_symbol(symbol: Any) -> str:
    s = str(symbol or "").replace("-", "").replace("/", "").upper().strip()
    if not s:
        raise KlineRestError("symbol is empty (STRICT)")
    return s


def _require_rest_base_url() -> str:
    raw = str(_require_setting_attr("binance_futures_rest_base") or "").strip().rstrip("/")
    if not raw:
        raise RuntimeError("settings.binance_futures_rest_base is required (STRICT)")
    if not (raw.startswith("https://") or raw.startswith("http://")):
        raise RuntimeError(f"settings.binance_futures_rest_base invalid (STRICT): {raw!r}")
    return raw


def _default_backfill_limit() -> int:
    return _require_positive_int(_require_setting_attr("ws_backfill_limit"), name="settings.ws_backfill_limit")


def _rest_timeout_sec() -> float:
    return _require_positive_float(_require_setting_attr("ws_rest_timeout_sec"), name="settings.ws_rest_timeout_sec")


def _interval_to_ms(interval: str) -> int:
    iv = str(interval or "").strip()
    if iv not in _INTERVAL_MS:
        raise KlineRestError(f"지원하지 않는 interval (STRICT): {interval!r}")
    return _INTERVAL_MS[iv]


def _require_int(v: Any, *, name: str) -> int:
    if isinstance(v, bool):
        raise KlineRestError(f"{name} not int-convertible: bool")
    try:
        iv = int(v)
    except Exception as e:
        raise KlineRestError(f"{name} not int-convertible: {v!r}") from e
    return iv


def _require_float(v: Any, *, name: str) -> float:
    if isinstance(v, bool):
        raise KlineRestError(f"{name} not float-convertible: bool")
    try:
        fv = float(v)
    except Exception as e:
        raise KlineRestError(f"{name} not float-convertible: {v!r}") from e
    if fv != fv:
        raise KlineRestError(f"{name} is NaN")
    if fv in (float("inf"), float("-inf")):
        raise KlineRestError(f"{name} is infinite")
    return fv


def _extract_ts_from_rest_row_strict(row: Any) -> int:
    """
    REST kline 응답 한 행에서 openTime(ms)를 STRICT 추출한다.
    - list/tuple: row[0]
    - dict: openTime/t/time/startTime
    """
    if isinstance(row, (list, tuple)) and row:
        ts = _require_int(row[0], name="row.openTime")
        if ts <= 0:
            raise KlineRestError(f"row.openTime invalid (<=0): {ts}")
        return ts

    if isinstance(row, dict):
        ts_val = (
            row.get("openTime")
            or row.get("t")
            or row.get("time")
            or row.get("startTime")
        )
        if ts_val is None:
            raise KlineRestError("row missing openTime (dict)")
        ts = _require_int(ts_val, name="row.openTime(dict)")
        if ts <= 0:
            raise KlineRestError(f"row.openTime(dict) invalid (<=0): {ts}")
        return ts

    raise KlineRestError(f"row type invalid (expected list/tuple/dict): {type(row).__name__}")


def _normalize_rows_to_common_format_strict(rows: List[Any], interval_ms: int) -> List[List[Any]]:
    """
    Binance /fapi/v1/klines 응답 rows를 공통 포맷 list[list] 로 STRICT 정규화한다.

    공통 포맷:
        [openTime_ms, open, high, low, close, volume, closeTime_ms]

    STRICT:
    - 한 행이라도 형식/타입/값이 깨지면 즉시 예외
    - OHLC는 모두 > 0 이어야 한다
    - high/low/open/close 관계 검증
    - volume >= 0
    - closeTime >= openTime 이어야 한다
    """
    if not isinstance(rows, list) or not rows:
        raise KlineRestError("rows missing/empty (STRICT)")

    normalized: List[List[Any]] = []

    for i, r in enumerate(rows):
        if not isinstance(r, (list, tuple)) or len(r) < 7:
            raise KlineRestError(f"row[{i}] invalid kline row shape (need>=7): {r!r}")

        ts = _require_int(r[0], name=f"row[{i}].openTime")
        if ts <= 0:
            raise KlineRestError(f"row[{i}].openTime invalid (<=0): {ts}")

        o = _require_float(r[1], name=f"row[{i}].open")
        h = _require_float(r[2], name=f"row[{i}].high")
        l = _require_float(r[3], name=f"row[{i}].low")
        c = _require_float(r[4], name=f"row[{i}].close")
        v = _require_float(r[5], name=f"row[{i}].volume")

        if o <= 0 or h <= 0 or l <= 0 or c <= 0:
            raise KlineRestError(f"row[{i}] OHLC must be > 0 (STRICT)")
        if h < l:
            raise KlineRestError(f"row[{i}] invalid OHLC: high<low (h={h}, l={l})")
        if not (l <= o <= h):
            raise KlineRestError(f"row[{i}] invalid OHLC: open out of range (o={o}, l={l}, h={h})")
        if not (l <= c <= h):
            raise KlineRestError(f"row[{i}] invalid OHLC: close out of range (c={c}, l={l}, h={h})")
        if v < 0:
            raise KlineRestError(f"row[{i}] invalid volume (<0): {v}")

        close_time = _require_int(r[6], name=f"row[{i}].closeTime")
        if close_time < ts:
            raise KlineRestError(
                f"row[{i}] invalid closeTime (<openTime): openTime={ts}, closeTime={close_time}"
            )

        # closeTime은 통상 openTime + interval_ms - 1 여야 한다.
        # 거래소 응답 계약 이상을 숨기지 않기 위해 범위 이탈은 즉시 실패.
        min_close_time = ts + interval_ms - 1
        if close_time != min_close_time:
            raise KlineRestError(
                f"row[{i}] invalid closeTime contract (STRICT): "
                f"expected={min_close_time}, got={close_time}, interval_ms={interval_ms}"
            )

        normalized.append([ts, o, h, l, c, v, close_time])

    return normalized


def _validate_contiguous_open_times_strict(rows: List[List[Any]], interval: str, interval_ms: int) -> None:
    if not rows:
        raise KlineRestError("rows empty after normalization (STRICT)")

    prev_ts: Optional[int] = None
    for i, row in enumerate(rows):
        ts = _extract_ts_from_rest_row_strict(row)
        if prev_ts is not None:
            delta = ts - prev_ts
            if delta != interval_ms:
                # 월봉은 고정 길이가 아니므로 interval_ms 단위 연속성 강제를 하지 않는다.
                if interval == "1M":
                    if delta <= 0:
                        raise KlineRestError(
                            f"REST kline ts not strictly increasing at idx={i}: prev={prev_ts}, cur={ts}"
                        )
                else:
                    raise KlineRestError(
                        f"REST kline contiguity violated (STRICT): interval={interval} "
                        f"idx={i} prev={prev_ts} cur={ts} delta={delta} expected={interval_ms}"
                    )
        prev_ts = ts


@dataclass(frozen=True, slots=True)
class _RetryPolicy:
    max_attempts: int = 5
    base_delay_sec: float = 0.6
    max_delay_sec: float = 8.0
    jitter_sec: float = 0.15  # deterministic-ish jitter (time-based)


def _sleep_backoff(policy: _RetryPolicy, attempt_idx: int, *, reason: str) -> None:
    delay = policy.base_delay_sec * (2.0 ** attempt_idx)
    if delay > policy.max_delay_sec:
        delay = policy.max_delay_sec

    frac = time.time() % 1.0
    delay = delay + min(policy.jitter_sec, frac * policy.jitter_sec)

    log(f"[REST-KLINES BINANCE] retry backoff sleep={delay:.3f}s reason={reason}")
    time.sleep(delay)


# Session 재사용(성능/커넥션 안정성)
_SESSION: requests.Session = requests.Session()
_SESSION.headers.update(
    {
        "Accept": "application/json",
        "User-Agent": "auto-trader/binance-usdtm/rest-klines",
    }
)


def _request_klines_with_retry(
    *,
    url: str,
    params: Dict[str, Any],
    timeout_sec: float,
    policy: _RetryPolicy,
) -> requests.Response:
    last_err: Optional[BaseException] = None

    for attempt in range(policy.max_attempts):
        try:
            resp = _SESSION.get(url, params=params, timeout=timeout_sec)
        except (requests.Timeout, requests.ConnectionError) as e:
            last_err = e
            if attempt >= policy.max_attempts - 1:
                raise KlineRestError(f"REST kline request network error after retries: {e.__class__.__name__}") from e
            _sleep_backoff(policy, attempt, reason=f"network:{e.__class__.__name__}")
            continue
        except Exception as e:
            raise KlineRestError(f"REST kline request unexpected error: {e.__class__.__name__}") from e

        sc = int(getattr(resp, "status_code", 0) or 0)

        if sc == 200:
            return resp

        if sc == 429:
            if attempt >= policy.max_attempts - 1:
                body_preview = resp.text[:300] if getattr(resp, "text", None) else ""
                raise KlineRestError(f"REST kline rate-limited (429) after retries: {body_preview}")
            retry_after = resp.headers.get("Retry-After") if hasattr(resp, "headers") else None
            if retry_after is not None:
                try:
                    ra = float(str(retry_after).strip())
                    if ra > 0:
                        log(f"[REST-KLINES BINANCE] 429 Retry-After={ra} sec")
                        time.sleep(min(ra, policy.max_delay_sec))
                        continue
                except Exception:
                    pass
            _sleep_backoff(policy, attempt, reason="http:429")
            continue

        if sc in (502, 503, 504):
            if attempt >= policy.max_attempts - 1:
                body_preview = resp.text[:300] if getattr(resp, "text", None) else ""
                raise KlineRestError(f"REST kline HTTP {sc} after retries: {body_preview}")
            _sleep_backoff(policy, attempt, reason=f"http:{sc}")
            continue

        body_preview = resp.text[:300] if getattr(resp, "text", None) else ""
        raise KlineRestError(f"REST kline HTTP error: status={sc}, body={body_preview}")

    if last_err is not None:
        raise KlineRestError(f"REST kline failed: last_error={last_err.__class__.__name__}") from last_err
    raise KlineRestError("REST kline failed: unknown (no response)")


def fetch_klines_rest(symbol: str, interval: str, limit: Optional[int] = None) -> List[List[Any]]:
    """
    Binance Futures REST /fapi/v1/klines 히스토리를 조회해서 list[list] 로 반환한다.

    STRICT:
    - limit/symbol/interval 무결성 위반 시 즉시 예외
    - 네트워크/HTTP/파싱/정렬/정규화/연속성 실패 시 즉시 예외
    - 행 드랍/정렬 무시/폴백 없음
    - 반환은 정확히 limit개, 공통 포맷 list[list]
    """
    request_limit = _default_backfill_limit() if limit is None else _require_positive_int(limit, name="limit")
    if request_limit > _MAX_KLINES_LIMIT:
        raise KlineRestError(f"limit must be <= {_MAX_KLINES_LIMIT} (STRICT)")

    norm_symbol = _require_nonempty_symbol(symbol)
    iv = str(interval or "").strip()
    interval_ms = _interval_to_ms(iv)

    rest_base = _require_rest_base_url()
    timeout_sec = _rest_timeout_sec()

    url = f"{rest_base}/fapi/v1/klines"
    params: Dict[str, Any] = {
        "symbol": norm_symbol,
        "interval": iv,
        "limit": request_limit,
    }

    log(
        "[REST-KLINES BINANCE] request "
        f"symbol={norm_symbol}, interval={iv}, limit={request_limit}"
    )

    resp = _request_klines_with_retry(
        url=url,
        params=params,
        timeout_sec=timeout_sec,
        policy=_RetryPolicy(),
    )

    try:
        data = resp.json()
    except ValueError as e:
        body_preview = resp.text[:300] if getattr(resp, "text", None) else ""
        log(f"[REST-KLINES BINANCE] JSON decode error: {e.__class__.__name__}, body={body_preview}")
        raise KlineRestError(f"REST kline JSON 파싱 실패: symbol={norm_symbol}, interval={iv}") from e

    if not isinstance(data, list):
        log(
            "[REST-KLINES BINANCE] unexpected response type: "
            f"{type(data)} payload={str(data)[:300]}"
        )
        raise KlineRestError(f"REST kline 응답 형식 오류: symbol={norm_symbol}, interval={iv}")

    if not data:
        log(
            "[REST-KLINES BINANCE] empty rows: "
            f"symbol={norm_symbol}, interval={iv}"
        )
        raise KlineRestError(f"REST kline 조회 실패(빈 응답): symbol={norm_symbol}, interval={iv}")

    log(f"[REST-KLINES BINANCE] first row sample: {str(data[0])[:300]}")

    rows_normalized = _normalize_rows_to_common_format_strict(data, interval_ms)

    try:
        rows_normalized.sort(key=_extract_ts_from_rest_row_strict)
    except Exception as e:
        raise KlineRestError(f"REST kline sort failed (STRICT): {e.__class__.__name__}") from e

    _validate_contiguous_open_times_strict(rows_normalized, iv, interval_ms)

    first_ts = _extract_ts_from_rest_row_strict(rows_normalized[0])
    last_ts = _extract_ts_from_rest_row_strict(rows_normalized[-1])
    log(f"[REST-KLINES BINANCE] normalized ts range: first_ts={first_ts}, last_ts={last_ts}")

    if len(rows_normalized) != request_limit:
        raise KlineRestError(
            f"REST kline buffer 부족(STRICT): need={request_limit} got={len(rows_normalized)} "
            f"(symbol={norm_symbol}, interval={iv})"
        )

    log(
        f"[REST-KLINES BINANCE] loaded {len(rows_normalized)} rows for "
        f"{norm_symbol} {iv} (requested limit={request_limit})"
    )
    return rows_normalized


__all__ = [
    "fetch_klines_rest",
    "KlineRestError",
]