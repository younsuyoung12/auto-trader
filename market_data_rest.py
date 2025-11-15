# market_data_rest.py
# ====================================================
# BingX REST 캔들 히스토리 조회 모듈
#
# 2025-11-15 변경 사항 (2차: 디버그/심볼 정규화 보강)
# ----------------------------------------------------
# 1) 심볼 정규화 유틸 추가
#    - _normalize_symbol_for_rest(symbol):
#        - "BTCUSDT" → "BTC-USDT" 로 변환 (기타 *USDT 현물/선물 심볼 동일 처리)
#        - 이미 "BTC-USDT" 형식이면 그대로 사용.
#    - REST 요청/로그에서 모두 정규화된 심볼을 사용하도록 변경.
# 2) 디버그 로그 강화
#    - V3/V2 요청 시 실제 요청 파라미터(symbol/interval/start/end/limit)를 한 번 더 로그.
#    - code != 0 또는 HTTP 오류 시 응답 payload 앞부분을 함께 로그.
#    - V3/V2 모두 실패 후 rows 가 비어 있을 때, startTime/endTime/raw_limit 를 포함한 요약 로그를 남김.
#
# 2025-11-15 변경 사항 (1차: limit 파라미터 추가)
# ----------------------------------------------------
# 1) V3/V2 요청에 limit 파라미터를 추가해서, REST 에서 원하는 개수만큼
#    캔들을 받아오도록 수정.
#    - fetch_klines_rest(limit=...) → 내부에서 raw_limit = limit + 20 으로 여유 있게 요청.
#    - BingX 응답이 raw_limit 개 이상이면 openTime 기준 정렬 후 마지막 limit 개만 사용.
# 2) 디버깅용으로 REST 요청 로그에 raw_limit 를 함께 출력.
#
# 역할
# ----------------------------------------------------
# - swap/futures용 REST /kline 엔드포인트를 호출해서
#   히스토리 캔들을 받아온다.
# - run_bot_ws.py 에서는 이 모듈의 fetch_klines_rest(...) 결과를
#   market_data_ws.backfill_klines_from_rest(...) 에 넘겨서
#   WS 버퍼를 시작 시점에 한 번에 채운다.
#
# 반환 포맷
# ----------------------------------------------------
# - BingX REST 응답의 data 그대로(list[list])를 돌려준다.
#   예시:
#       [
#         [openTime, open, high, low, close, volume, closeTime, ...],
#         ...
#       ]
#
# 사용 예시 (run_bot_ws 쪽):
# ----------------------------------------------------
#   from market_data_rest import fetch_klines_rest
#   from market_data_ws import backfill_klines_from_rest
#
#   rest_5m = fetch_klines_rest("BTC-USDT", "5m", limit=120)
#   backfill_klines_from_rest("BTC-USDT", "5m", rest_5m)
#
# 주의
# ----------------------------------------------------
# - 여기서는 *공개 마켓 데이터* 엔드포인트만 사용하므로
#   서명/비공개 키가 필요 없다.
# - 엔드포인트/파라미터는 BingX 공식 문서 기준으로 작성했다.
#   에러 발생 시 telelog.log(...) 로만 남기고, 호출 측에서
#   적절히 예외를 처리하도록 한다.
# ====================================================

from __future__ import annotations

import os
import time
from typing import Any, List, Optional

import requests

try:
    from telelog import log
except Exception:
    # telelog 이 없으면 print 로만 대체 (로컬 테스트용)
    def log(msg: str) -> None:  # type: ignore
        print(msg)


# 기본 REST base URL
BINGX_API_BASE = os.getenv("BINGX_API_BASE", "https://open-api.bingx.com")


# interval 문자열 → 밀리초 변환
_INTERVAL_MS = {
    "1m": 60_000,
    "5m": 5 * 60_000,
    "15m": 15 * 60_000,
    "30m": 30 * 60_000,
    "1h": 60 * 60_000,
}


class KlineRestError(RuntimeError):
    """REST 캔들 조회 실패시 사용하는 예외."""


def _interval_to_ms(interval: str) -> int:
    if interval not in _INTERVAL_MS:
        raise ValueError(f"지원하지 않는 interval: {interval}")
    return _INTERVAL_MS[interval]


def _normalize_symbol_for_rest(symbol: str) -> str:
    """REST /kline 요청용 심볼 정규화.

    - "BTCUSDT" 처럼 하이픈(-) 없이 USDT 로 끝나는 경우 → "BTC-USDT" 로 변환.
    - 이미 "BTC-USDT" 형식이면 그대로 사용.
    - 그 외 심볼은 대문자 + strip 만 적용.
    """
    s = symbol.upper().strip()
    if "-" not in s and s.endswith("USDT"):
        return s[:-4] + "-USDT"
    return s


def _request_klines_v3(
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int,
) -> Optional[List[List[Any]]]:
    """swap V3 quote klines 시도

    - 문서: /openApi/swap/v3/quote/klines (공개 마켓 데이터)
    - limit 파라미터를 함께 보내서 최대 캔들 개수를 제어한다.
    """
    norm_symbol = _normalize_symbol_for_rest(symbol)
    url = f"{BINGX_API_BASE}/openApi/swap/v3/quote/klines"
    params = {
        "symbol": norm_symbol,
        "interval": interval,
        "startTime": start_ms,
        "endTime": end_ms,
        # BingX 문서 기준으로 필요 시 limit/pageSize 등으로 조정 가능
        "limit": limit,
    }

    log(
        "[REST-KLINES V3] request "
        f"symbol={norm_symbol}, interval={interval}, start={start_ms}, end={end_ms}, limit={limit}"
    )

    resp = requests.get(url, params=params, timeout=10)
    if resp.status_code != 200:
        log(f"[REST-KLINES V3] HTTP {resp.status_code}: {resp.text[:200]}")
        return None

    try:
        data = resp.json()
    except ValueError as e:
        log(f"[REST-KLINES V3] JSON decode error: {e}, body={resp.text[:200]}")
        return None

    code = data.get("code")
    if code != 0:
        log(
            f"[REST-KLINES V3] code={code}, msg={data.get('msg')}, "
            f"payload={str(data)[:200]}"
        )
        return None

    rows = data.get("data") or []
    if not isinstance(rows, list):
        log(f"[REST-KLINES V3] unexpected data type: {type(rows)}")
        return None

    if not rows:
        log(
            f"[REST-KLINES V3] empty data: symbol={norm_symbol}, interval={interval}, "
            f"start={start_ms}, end={end_ms}, limit={limit}"
        )
    return rows


def _request_klines_v2(
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int,
) -> Optional[List[List[Any]]]:
    """swap V2 market kline 시도 (폴백용)

    - 문서: /openApi/swap/v2/market/kline 혹은 유사 경로
    - 일부 환경에서 V3 엔드포인트가 동작하지 않을 경우를 대비.
    - limit 파라미터를 함께 보내서 최대 캔들 개수를 제어한다.
    """
    norm_symbol = _normalize_symbol_for_rest(symbol)
    # 실제 문서 기준으로 경로가 다를 수 있어서,
    # 여기서는 가장 많이 쓰이는 market/kline 경로를 우선 사용.
    url = f"{BINGX_API_BASE}/openApi/swap/v2/market/kline"
    params = {
        "symbol": norm_symbol,
        "interval": interval,
        "startTime": start_ms,
        "endTime": end_ms,
        # 필요 시 BingX 문서에 맞춰 limit/pageSize 이름 조정
        "limit": limit,
    }

    log(
        "[REST-KLINES V2] request "
        f"symbol={norm_symbol}, interval={interval}, start={start_ms}, end={end_ms}, limit={limit}"
    )

    resp = requests.get(url, params=params, timeout=10)
    if resp.status_code != 200:
        log(f"[REST-KLINES V2] HTTP {resp.status_code}: {resp.text[:200]}")
        return None

    try:
        data = resp.json()
    except ValueError as e:
        log(f"[REST-KLINES V2] JSON decode error: {e}, body={resp.text[:200]}")
        return None

    code = data.get("code")
    if code != 0:
        log(
            f"[REST-KLINES V2] code={code}, msg={data.get('msg')}, "
            f"payload={str(data)[:200]}"
        )
        return None

    rows = data.get("data") or []
    if not isinstance(rows, list):
        log(f"[REST-KLINES V2] unexpected data type: {type(rows)}")
        return None

    if not rows:
        log(
            f"[REST-KLINES V2] empty data: symbol={norm_symbol}, interval={interval}, "
            f"start={start_ms}, end={end_ms}, limit={limit}"
        )
    return rows


def fetch_klines_rest(
    symbol: str,
    interval: str,
    limit: int = 120,
) -> List[List[Any]]:
    """BingX REST /kline 히스토리를 조회해서 list[list] 로 반환한다.

    매개변수
    ------------------------------------------------
    - symbol  : "BTC-USDT" 같은 심볼 (WS와 동일 포맷)
    - interval: "1m", "5m", "15m" 등
    - limit   : 원하는 최대 캔들 수 (기본 120)

    반환값 예시
    ------------------------------------------------
    [
      [openTime, open, high, low, close, volume, closeTime, ...],
      ...
    ]

    주의
    ------------------------------------------------
    - startTime / endTime 은 현재 시각 기준 뒤로 limit+여유분 만큼 잡는다.
    - V3(/swap/v3/quote/klines) → 실패 시 V2(/swap/v2/market/kline) 순으로 시도.
    - 둘 다 실패하면 KlineRestError 예외를 raise 한다.
    """
    if limit <= 0:
        raise ValueError("limit 은 1 이상이어야 합니다.")

    iv_ms = _interval_to_ms(interval)
    end_ms = int(time.time() * 1000)
    # 약간 여유를 두고 더 넓게 요청
    start_ms = end_ms - iv_ms * (limit + 20)

    # 서버에는 limit 보다 약간 큰 raw_limit 로 요청 (여유분)
    raw_limit = limit + 20

    norm_symbol = _normalize_symbol_for_rest(symbol)

    log(
        "[REST-KLINES] request "
        f"symbol={symbol} (normalized={norm_symbol}) interval={interval} "
        f"start={start_ms} end={end_ms} limit≈{limit}, raw_limit={raw_limit}"
    )

    rows: Optional[List[List[Any]]] = None

    # 1) V3 먼저 시도
    try:
        rows = _request_klines_v3(norm_symbol, interval, start_ms, end_ms, raw_limit)
    except Exception as e:
        log(f"[REST-KLINES] V3 request error: {e}")

    # 2) V3 가 실패했거나 빈 결과 → V2 폴백
    if not rows:
        try:
            rows = _request_klines_v2(norm_symbol, interval, start_ms, end_ms, raw_limit)
        except Exception as e:
            log(f"[REST-KLINES] V2 request error: {e}")

    if not rows:
        log(
            "[REST-KLINES] no rows from REST after V3/V2: "
            f"symbol={norm_symbol}, interval={interval}, start={start_ms}, "
            f"end={end_ms}, raw_limit={raw_limit}"
        )
        raise KlineRestError(
            f"REST kline 조회 실패: symbol={norm_symbol}, interval={interval}"
        )

    # openTime 기준 정렬 후 limit 개까지만 반환
    try:
        rows.sort(key=lambda r: int(r[0]))
    except Exception as e:
        # 정렬 실패하면 그대로 사용 (최악의 경우지만 로직은 계속)
        log(f"[REST-KLINES] sort error (ignore): {e}")

    if len(rows) > limit:
        rows = rows[-limit:]

    log(
        f"[REST-KLINES] loaded {len(rows)} rows for "
        f"{norm_symbol} {interval} (requested limit={limit}, raw_limit={raw_limit})"
    )
    return rows


__all__ = [
    "fetch_klines_rest",
    "KlineRestError",
]
