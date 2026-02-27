# market_data_rest.py
# ====================================================
# Binance USDT-M Futures REST /fapi/v1/klines 히스토리 조회 모듈
#
# 주요 역할
# ----------------------------------------------------
# - Binance Futures 공개 REST /fapi/v1/klines 엔드포인트에서
#   히스토리 캔들을 조회한다.
# - run_bot_ws 시작 시 market_data_ws.backfill_klines_from_rest(...)
#   에 넘겨 WS 버퍼를 부트스트랩하는 용도로만 사용한다.
# - 라이브 운영 중에는 WebSocket 데이터만 사용한다.
# - 반환 포맷은 market_data_ws.backfill_klines_from_rest(...) 가
#   그대로 사용할 수 있는 공통 포맷 list[list] 이다.
#   [openTime, open, high, low, close, volume, closeTime]
# ====================================================

from __future__ import annotations

import time
from typing import Any, List

import requests

try:
    from infra.telelog import log
except Exception:
    # telelog 이 없으면 print 로만 대체 (로컬 테스트용)
    def log(msg: str) -> None:  # type: ignore
        print(msg)


# Binance USDT-M Futures REST base URL (공개 Kline 조회용)
BINANCE_FUTURES_API_BASE = "https://fapi.binance.com"


def _extract_ts_from_rest_row(row: Any) -> int:
    """REST kline 응답 한 행에서 openTime(ms)를 추출한다.

    - list/tuple: row[0] 를 openTime 으로 간주
    - dict      : openTime/t/time/startTime 중 하나를 사용 (방어적 처리)
    - 실패 시 0 반환
    """
    try:
        if isinstance(row, (list, tuple)) and row:
            return int(row[0])
        if isinstance(row, dict):
            ts_val = (
                row.get("openTime")
                or row.get("t")
                or row.get("time")
                or row.get("startTime")
                or 0
            )
            return int(ts_val)
    except Exception:
        return 0
    return 0


def _normalize_rows_to_common_format(
    rows: List[Any], interval_ms: int
) -> List[List[Any]]:
    """Binance /fapi/v1/klines 응답 rows를 공통 포맷 list[list] 로 정규화한다.

    공통 포맷:
        [openTime_ms, open, high, low, close, volume, closeTime_ms]
    """
    normalized: List[List[Any]] = []
    dropped = 0

    for r in rows:
        try:
            if not isinstance(r, (list, tuple)) or len(r) < 7:
                dropped += 1
                continue

            ts = int(r[0])
            o = float(r[1])
            h = float(r[2])
            l = float(r[3])
            c = float(r[4])
            v = float(r[5])

            try:
                close_time = int(r[6])
            except Exception:
                close_time = ts + interval_ms

            normalized.append([ts, o, h, l, c, v, close_time])

        except Exception:
            dropped += 1

    if dropped:
        log(
            f"[REST-KLINES BINANCE] dropped {dropped} rows during normalization "
            f"(interval_ms={interval_ms})"
        )

    return normalized


# interval 문자열 → 밀리초 변환
_MINUTE_MS = 60_000
_HOUR_MS = 60 * 60_000
_DAY_MS = 24 * _HOUR_MS

_INTERVAL_MS = {
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


class KlineRestError(RuntimeError):
    """REST 캔들 조회 실패시 사용하는 예외."""


def _interval_to_ms(interval: str) -> int:
    if interval not in _INTERVAL_MS:
        raise ValueError(f"지원하지 않는 interval: {interval}")
    return _INTERVAL_MS[interval]


def fetch_klines_rest(
    symbol: str,
    interval: str,
    limit: int = 120,
) -> List[List[Any]]:
    """Binance Futures REST /fapi/v1/klines 히스토리를 조회해서 list[list] 로 반환한다.

    매개변수
    ------------------------------------------------
    - symbol  : "BTCUSDT" 같은 심볼 (WS와 동일 포맷)
    - interval: Binance 지원 K라인 interval (예: "1m", "5m", "1h", "1d" 등)
    - limit   : 원하는 최대 캔들 수 (기본 120)

    반환값 예시
    ------------------------------------------------
    [
      [openTime, open, high, low, close, volume, closeTime],
      ...
    ]

    주의
    ------------------------------------------------
    - startTime / endTime 은 현재 시각 기준 뒤로 limit+여유분 만큼 잡는다.
    - 단일 엔드포인트(/fapi/v1/klines)만 사용한다.
    - 실패 시 KlineRestError 예외를 raise 한다.
    """
    if limit <= 0:
        raise ValueError("limit 은 1 이상이어야 합니다.")

    norm_symbol = symbol.upper().strip()
    if not norm_symbol:
        raise ValueError("symbol 이 비어 있습니다.")

    iv_ms = _interval_to_ms(interval)
    end_ms = int(time.time() * 1000)

    # 약간 여유를 두고 더 넓게 요청
    start_ms = end_ms - iv_ms * (limit + 20)

    # Binance /fapi/v1/klines 최대 limit=1500
    raw_limit = min(limit + 20, 1500)

    url = f"{BINANCE_FUTURES_API_BASE}/fapi/v1/klines"
    params = {
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

    try:
        resp = requests.get(url, params=params, timeout=10)
    except Exception as e:
        log(f"[REST-KLINES BINANCE] request error: {e}")
        raise KlineRestError(
            f"REST kline 요청 실패: symbol={norm_symbol}, interval={interval}, error={e}"
        ) from e

    if resp.status_code != 200:
        body_preview = resp.text[:300] if getattr(resp, "text", None) else ""
        log(
            f"[REST-KLINES BINANCE] HTTP {resp.status_code}: {body_preview}"
        )
        raise KlineRestError(
            f"REST kline HTTP 오류: status={resp.status_code}, "
            f"symbol={norm_symbol}, interval={interval}"
        )

    try:
        data = resp.json()
    except ValueError as e:
        body_preview = resp.text[:300] if getattr(resp, "text", None) else ""
        log(f"[REST-KLINES BINANCE] JSON decode error: {e}, body={body_preview}")
        raise KlineRestError(
            f"REST kline JSON 파싱 실패: symbol={norm_symbol}, interval={interval}"
        ) from e

    if not isinstance(data, list):
        log(
            "[REST-KLINES BINANCE] unexpected response type: "
            f"{type(data)} payload={str(data)[:300]}"
        )
        raise KlineRestError(
            f"REST kline 응답 형식 오류: symbol={norm_symbol}, interval={interval}"
        )

    if not data:
        log(
            "[REST-KLINES BINANCE] empty rows: "
            f"symbol={norm_symbol}, interval={interval}, start={start_ms}, end={end_ms}"
        )
        raise KlineRestError(
            f"REST kline 조회 실패(빈 응답): symbol={norm_symbol}, interval={interval}"
        )

    # 첫 행 샘플 로그
    try:
        log(f"[REST-KLINES BINANCE] first row sample: {str(data[0])[:300]}")
    except Exception:
        pass

    # 공통 포맷으로 정규화
    rows_normalized = _normalize_rows_to_common_format(data, iv_ms)
    if not rows_normalized:
        log(
            "[REST-KLINES BINANCE] no normalized rows after format conversion: "
            f"symbol={norm_symbol}, interval={interval}"
        )
        raise KlineRestError(
            f"REST kline 정규화 실패: symbol={norm_symbol}, interval={interval}"
        )

    # openTime 기준 정렬
    try:
        rows_normalized.sort(key=_extract_ts_from_rest_row)
    except Exception as e:
        # 정렬 실패 시 그대로 진행
        log(f"[REST-KLINES BINANCE] sort error (ignore): {e}")

    # 정렬 결과 first/last ts 진단
    if rows_normalized:
        first_ts = _extract_ts_from_rest_row(rows_normalized[0])
        last_ts = _extract_ts_from_rest_row(rows_normalized[-1])
        if first_ts <= 0 or last_ts <= 0:
            log(
                "[REST-KLINES BINANCE] WARN: unable to extract valid ts "
                f"(first_ts={first_ts}, last_ts={last_ts})"
            )
        else:
            log(
                f"[REST-KLINES BINANCE] normalized ts range: "
                f"first_ts={first_ts}, last_ts={last_ts}"
            )

    # limit 개수만큼만 뒤에서 자르기
    if len(rows_normalized) > limit:
        rows_normalized = rows_normalized[-limit:]

    log(
        f"[REST-KLINES BINANCE] loaded {len(rows_normalized)} rows for "
        f"{norm_symbol} {interval} (requested limit={limit}, raw_limit={raw_limit})"
    )
    return rows_normalized


__all__ = [
    "fetch_klines_rest",
    "KlineRestError",
]