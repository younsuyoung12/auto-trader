"""
========================================================
FILE: infra/market_data_rest.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할
--------------------------------------------------------
- Binance Futures 공개 REST /fapi/v1/klines 엔드포인트에서 히스토리 캔들을 조회한다.
- run_bot_ws 시작 시 WS 버퍼 부트스트랩(backfill) 용도로만 사용한다.
- 라이브 운영 중에는 WebSocket 데이터만 사용한다.
- 반환 포맷은 WS backfill이 그대로 사용할 수 있는 공통 포맷 list[list] 이다.
  [openTime, open, high, low, close, volume, closeTime]

변경 이력
--------------------------------------------------------
- 2026-03-04:
  1) HTTP 5xx/429 재시도 + exponential backoff 추가(실전 내구성 강화)
  2) connect/read timeout 분리 + Session 재사용(성능/안정성)
  3) 정렬 실패/정규화 누락/무결성 위반 시 즉시 예외(STRICT)
  4) 예외 삼키기/행 드랍/print 폴백 제거(절대 NO-FALLBACK)
- 2026-03-05:
  1) SETTINGS PATCH:
     - fetch_klines_rest() 기본 limit을 settings(ws_backfill_limit)에서 읽도록 변경
========================================================
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests

# SETTINGS PATCH: settings SSOT 연동 (백필 limit 기본값을 settings에서 읽는다)
from settings import load_settings  # SETTINGS PATCH

SET = load_settings()  # SETTINGS PATCH

# STRICT: telelog는 운영 필수. 없으면 즉시 예외 (NO-FALLBACK).
try:
    from infra.telelog import log
except Exception as e:  # pragma: no cover
    raise RuntimeError(
        "infra.telelog.log is required (STRICT · NO-FALLBACK). "
        "Do not fallback to print()."
    ) from e


# Binance USDT-M Futures REST base URL (공개 Kline 조회용)
BINANCE_FUTURES_API_BASE = "https://fapi.binance.com"


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
    # 월봉은 고정 길이가 아니므로 REST 윈도우 계산용으로만 30일 근사값 사용
    "1M": 30 * _DAY_MS,
}


def _interval_to_ms(interval: str) -> int:
    if interval not in _INTERVAL_MS:
        raise ValueError(f"지원하지 않는 interval: {interval}")
    return _INTERVAL_MS[interval]


def _require_int(v: Any, *, name: str) -> int:
    try:
        iv = int(v)
    except Exception as e:
        raise KlineRestError(f"{name} not int-convertible: {v!r}") from e
    return iv


def _require_float(v: Any, *, name: str) -> float:
    try:
        fv = float(v)
    except Exception as e:
        raise KlineRestError(f"{name} not float-convertible: {v!r}") from e
    if fv != fv:  # NaN
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
    - 한 행이라도 형식/타입/값이 깨지면 즉시 예외(드랍 금지)
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

        # 기본적인 OHLCV 무결성 (STRICT)
        if h < l:
            raise KlineRestError(f"row[{i}] invalid OHLC: high<low (h={h}, l={l})")
        if not (l <= o <= h):
            raise KlineRestError(f"row[{i}] invalid OHLC: open out of range (o={o}, l={l}, h={h})")
        if not (l <= c <= h):
            raise KlineRestError(f"row[{i}] invalid OHLC: close out of range (c={c}, l={l}, h={h})")
        if v < 0:
            raise KlineRestError(f"row[{i}] invalid volume (<0): {v}")

        close_time = _require_int(r[6], name=f"row[{i}].closeTime")
        # Binance는 보통 closeTime = openTime + interval_ms - 1 형태. 다만 데이터 소스별 차이를 고려하여
        # 최소 조건만 강제한다.
        if close_time < ts:
            raise KlineRestError(
                f"row[{i}] invalid closeTime (<openTime): openTime={ts}, closeTime={close_time}"
            )

        # closeTime 누락/이상치를 보정하지 않는다(NO-FALLBACK). interval_ms 기반 계산도 금지.
        normalized.append([ts, o, h, l, c, v, close_time])

    return normalized


@dataclass(frozen=True)
class _RetryPolicy:
    max_attempts: int = 5
    base_delay_sec: float = 0.6
    max_delay_sec: float = 8.0
    jitter_sec: float = 0.15  # deterministic-ish jitter (time-based)


def _sleep_backoff(policy: _RetryPolicy, attempt_idx: int, *, reason: str) -> None:
    # attempt_idx: 0..N-1
    delay = policy.base_delay_sec * (2.0 ** attempt_idx)
    if delay > policy.max_delay_sec:
        delay = policy.max_delay_sec

    # 작은 지터(동시 봇 다수 실행 시 동기화 방지). random 사용 없이 time 기반.
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

# connect/read timeout 분리
_DEFAULT_TIMEOUT: Tuple[float, float] = (3.5, 10.0)


def _request_klines_with_retry(
    *,
    url: str,
    params: Dict[str, Any],
    timeout: Tuple[float, float],
    policy: _RetryPolicy,
) -> requests.Response:
    last_err: Optional[BaseException] = None

    for attempt in range(policy.max_attempts):
        try:
            resp = _SESSION.get(url, params=params, timeout=timeout)
        except (requests.Timeout, requests.ConnectionError) as e:
            last_err = e
            if attempt >= policy.max_attempts - 1:
                raise KlineRestError(f"REST kline request network error after retries: {e.__class__.__name__}") from e
            _sleep_backoff(policy, attempt, reason=f"network:{e.__class__.__name__}")
            continue
        except Exception as e:
            # requests 내부/기타 예외도 폴백 없이 즉시 실패
            raise KlineRestError(f"REST kline request unexpected error: {e.__class__.__name__}") from e

        # HTTP 레벨 처리
        sc = int(getattr(resp, "status_code", 0) or 0)

        # 200 OK
        if sc == 200:
            return resp

        # 429 rate limit
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
                    # Retry-After 파싱 실패는 폴백이 아니라, 표준 backoff로 처리한다.
                    pass
            _sleep_backoff(policy, attempt, reason="http:429")
            continue

        # 5xx transient
        if sc in (502, 503, 504):
            if attempt >= policy.max_attempts - 1:
                body_preview = resp.text[:300] if getattr(resp, "text", None) else ""
                raise KlineRestError(f"REST kline HTTP {sc} after retries: {body_preview}")
            _sleep_backoff(policy, attempt, reason=f"http:{sc}")
            continue

        # 기타 상태코드는 즉시 실패(폴백/재시도 남발 금지)
        body_preview = resp.text[:300] if getattr(resp, "text", None) else ""
        raise KlineRestError(f"REST kline HTTP error: status={sc}, body={body_preview}")

    # 이론상 도달 불가. 방어적으로 예외.
    if last_err is not None:
        raise KlineRestError(f"REST kline failed: last_error={last_err.__class__.__name__}") from last_err
    raise KlineRestError("REST kline failed: unknown (no response)")


def fetch_klines_rest(symbol: str, interval: str, limit: Optional[int] = None) -> List[List[Any]]:
    """
    Binance Futures REST /fapi/v1/klines 히스토리를 조회해서 list[list] 로 반환한다.

    STRICT:
    - limit/symbol/interval 무결성 위반 시 즉시 예외
    - 네트워크/HTTP/파싱/정렬/정규화 실패 시 즉시 예외
    - 행 드랍/정렬 무시/print 폴백 없음
    """
    # SETTINGS PATCH: limit 미지정이면 settings.ws_backfill_limit 사용
    if limit is None:
        limit = int(getattr(SET, "ws_backfill_limit", 120) or 120)  # SETTINGS PATCH

    if limit <= 0:
        raise ValueError("limit 은 1 이상이어야 합니다.")
    # Binance /fapi/v1/klines 최대 limit=1500
    if limit > 1500:
        raise ValueError("limit 은 1500 이하여야 합니다. (Binance REST 제한)")

    norm_symbol = str(symbol).upper().strip()
    if not norm_symbol:
        raise ValueError("symbol 이 비어 있습니다.")

    iv_ms = _interval_to_ms(interval)
    end_ms = int(time.time() * 1000)

    # 약간 여유를 두고 더 넓게 요청
    # (여유분 자체는 REST window 계산용이며, 반환은 아래에서 limit로 강제 절단)
    start_ms = end_ms - iv_ms * (limit + 20)

    raw_limit = min(limit + 20, 1500)

    url = f"{BINANCE_FUTURES_API_BASE}/fapi/v1/klines"
    params: Dict[str, Any] = {
        "symbol": norm_symbol,
        "interval": interval,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": raw_limit,
    }

    log(
        "[REST-KLINES BINANCE] request "
        f"symbol={norm_symbol}, interval={interval}, "
        f"start={start_ms}, end={end_ms}, limit≈{limit}, raw_limit={raw_limit}"
    )

    resp = _request_klines_with_retry(
        url=url,
        params=params,
        timeout=_DEFAULT_TIMEOUT,
        policy=_RetryPolicy(),
    )

    # JSON 파싱 STRICT
    try:
        data = resp.json()
    except ValueError as e:
        body_preview = resp.text[:300] if getattr(resp, "text", None) else ""
        log(f"[REST-KLINES BINANCE] JSON decode error: {e.__class__.__name__}, body={body_preview}")
        raise KlineRestError(f"REST kline JSON 파싱 실패: symbol={norm_symbol}, interval={interval}") from e

    if not isinstance(data, list):
        log(
            "[REST-KLINES BINANCE] unexpected response type: "
            f"{type(data)} payload={str(data)[:300]}"
        )
        raise KlineRestError(f"REST kline 응답 형식 오류: symbol={norm_symbol}, interval={interval}")

    if not data:
        log(
            "[REST-KLINES BINANCE] empty rows: "
            f"symbol={norm_symbol}, interval={interval}, start={start_ms}, end={end_ms}"
        )
        raise KlineRestError(f"REST kline 조회 실패(빈 응답): symbol={norm_symbol}, interval={interval}")

    # 첫 행 샘플 로그(민감정보 없음)
    log(f"[REST-KLINES BINANCE] first row sample: {str(data[0])[:300]}")

    # 공통 포맷으로 STRICT 정규화 (행 드랍 금지)
    rows_normalized = _normalize_rows_to_common_format_strict(data, iv_ms)

    # openTime 기준 STRICT 정렬 (실패 시 즉시 예외)
    try:
        rows_normalized.sort(key=_extract_ts_from_rest_row_strict)
    except Exception as e:
        raise KlineRestError(f"REST kline sort failed (STRICT): {e.__class__.__name__}") from e

    # 정렬 결과 무결성 검사: openTime strictly increasing
    prev_ts: Optional[int] = None
    for i, row in enumerate(rows_normalized):
        ts = _extract_ts_from_rest_row_strict(row)
        if prev_ts is not None and ts <= prev_ts:
            raise KlineRestError(
                f"REST kline ts not strictly increasing at idx={i}: prev={prev_ts}, cur={ts}"
            )
        prev_ts = ts

    first_ts = _extract_ts_from_rest_row_strict(rows_normalized[0])
    last_ts = _extract_ts_from_rest_row_strict(rows_normalized[-1])
    log(f"[REST-KLINES BINANCE] normalized ts range: first_ts={first_ts}, last_ts={last_ts}")

    # limit 개수만큼만 뒤에서 자르기 (요구 개수만 반환)
    if len(rows_normalized) > limit:
        rows_normalized = rows_normalized[-limit:]

    if len(rows_normalized) < limit:
        # STRICT: 요청 limit을 못 채우면 즉시 예외 (부트스트랩 데이터 부족)
        raise KlineRestError(
            f"REST kline buffer 부족(STRICT): need={limit} got={len(rows_normalized)} "
            f"(symbol={norm_symbol}, interval={interval})"
        )

    log(
        f"[REST-KLINES BINANCE] loaded {len(rows_normalized)} rows for "
        f"{norm_symbol} {interval} (requested limit={limit}, raw_limit={raw_limit})"
    )
    return rows_normalized


__all__ = [
    "fetch_klines_rest",
    "KlineRestError",
]