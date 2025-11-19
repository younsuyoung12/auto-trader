# market_data_rest.py
# ====================================================
# BingX REST /kline 히스토리 조회 모듈
#
# 주요 역할
# ----------------------------------------------------
# - swap/futures 공개 /kline 엔드포인트에서 히스토리 캔들을 조회한다.
# - run_bot_ws 시작 시 market_data_ws.backfill_klines_from_rest(...) 에 넘겨
#   WS 버퍼를 부트스트랩하는 용도로만 사용한다.
# - 라이브 운영 중에는 WS 데이터만 사용하고, REST 는 초기 히스토리 로딩 전용이다.
# - 반환 포맷은 market_data_ws.backfill_klines_from_rest(...) 가 그대로 사용할 수 있는
#   공통 포맷 list[list] 이다.
#   [openTime, open, high, low, close, volume, closeTime]
#
# 2025-11-20 변경 사항 (WS 1m/5m/15m 백필 B안 정리)
# ----------------------------------------------------
# 1) run_bot_ws._backfill_ws_kline_history 에서 1m/5m/15m REST 히스토리 백필 전용으로
#    사용하는 모듈임을 주석으로 명시했다.
# 2) market_data_ws.backfill_klines_from_rest(...) 와의 연동 포맷을 상단에 명시했다.
#    - fetch_klines_rest(...) → [openTime, open, high, low, close, volume, closeTime] 리스트 반환.
#    - backfill_klines_from_rest(...) 에서는 앞 6개 필드(openTime~volume)만 사용한다.
# 3) 로직/시그니처 변경은 없으며, 2025-11-19 버전과 완전 호환된다.
#
# 2025-11-19 변경 사항 (WS 멀티 타임프레임 정합성)
# ----------------------------------------------------
# 1) interval→ms 매핑을 WebSocket 모듈과 동일한 타임프레임 세트로 확장했다.
#    - "1m","3m","5m","15m","30m","1h","2h","4h","6h","12h","1d","3d","1w","1M" 지원.
# 2) fetch_klines_rest(...) 가 항상 "정상 히스토리 or KlineRestError" 만 반환하도록
#    하는 기존 정책을 유지하며, 상단 주석을 실제 운영에 필요한 수준으로 압축했다.
# 3) dict/list 응답을 공통 포맷(list[list])으로 정규화하는 2025-11-15 변경 사항은
#    요약만 남기고 내부 구현은 그대로 유지했다.
#
# 이전 변경 요약 (2025-11-15)
# ----------------------------------------------------
# - V3/V2 응답 포맷을 공통 list[list] 로 정규화.
# - 심볼 정규화(_normalize_symbol_for_rest) 및 디버그 로그 보강.
# - limit/raw_limit 기반으로 startTime/endTime 윈도우와 개수 제어.
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


def _extract_ts_from_rest_row(row: Any) -> int:
    """REST /kline 응답 한 행에서 타임스탬프(openTime 계열)를 추출한다.

    - list/tuple: row[0] 를 ts 로 간주
    - dict      : t / T / time / openTime / startTime 중 하나를 ts 로 사용
    - 실패 시 0 반환
    """
    try:
        if isinstance(row, (list, tuple)) and row:
            return int(row[0])
        if isinstance(row, dict):
            ts_val = (
                row.get("t")
                or row.get("T")
                or row.get("time")
                or row.get("openTime")
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
    """REST 응답 rows(list[dict|list|tuple])를 공통 포맷 list[list] 로 정규화.

    공통 포맷:
        [openTime_ms, open, high, low, close, volume, closeTime_ms]
    """
    normalized: List[List[Any]] = []
    dropped = 0

    for r in rows:
        try:
            if isinstance(r, dict):
                ts = _extract_ts_from_rest_row(r)
                if ts <= 0:
                    dropped += 1
                    continue
                o = float(r.get("open") or r.get("o") or 0)
                h = float(r.get("high") or r.get("h") or 0)
                l = float(r.get("low") or r.get("l") or 0)
                c = float(r.get("close") or r.get("c") or 0)
                v = float(r.get("volume") or r.get("v") or 0)
                close_time = ts + interval_ms
                normalized.append([ts, o, h, l, c, v, close_time])
            elif isinstance(r, (list, tuple)):
                if len(r) < 6:
                    dropped += 1
                    continue
                # [openTime, open, high, low, close, volume, (optional closeTime ...)]
                lst = list(r)
                ts = int(lst[0])
                o = float(lst[1])
                h = float(lst[2])
                l = float(lst[3])
                c = float(lst[4])
                v = float(lst[5])
                # closeTime 이 없으면 interval 기반으로 보정
                if len(lst) > 6:
                    try:
                        close_time = int(lst[6])
                    except Exception:
                        close_time = ts + interval_ms
                else:
                    close_time = ts + interval_ms
                normalized.append([ts, o, h, l, c, v, close_time])
            else:
                dropped += 1
        except Exception:
            dropped += 1

    if dropped:
        log(
            f"[REST-KLINES] dropped {dropped} rows during normalization "
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
    # 월 봉은 정확한 길이가 고정되어 있지 않으므로 REST 윈도우 계산용으로만
    # 30일 기준 근사값을 사용한다.
    "1M": 30 * _DAY_MS,
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
) -> Optional[List[Any]]:
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

    # 첫 행 샘플 로그
    if rows:
        sample = rows[0]
        log(f"[REST-KLINES V3] first row sample: {str(sample)[:200]}")

    return rows


def _request_klines_v2(
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int,
) -> Optional[List[Any]]:
    """swap V2 market kline 시도

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

    # 첫 행 샘플 로그
    if rows:
        sample = rows[0]
        log(f"[REST-KLINES V2] first row sample: {str(sample)[:200]}")

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
    - interval: BingX 지원 K라인 interval (예: "1m", "5m", "1h", "1d" 등)
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

    rows: Optional[List[Any]] = None

    # 1) V3 먼저 시도
    try:
        rows = _request_klines_v3(norm_symbol, interval, start_ms, end_ms, raw_limit)
    except Exception as e:
        log(f"[REST-KLINES] V3 request error (ignore and fallback to V2): {e}")

    # 2) V3 가 실패했거나 빈 결과면 V2 시도
    if not rows:
        try:
            rows = _request_klines_v2(norm_symbol, interval, start_ms, end_ms, raw_limit)
        except Exception as e:
            log(f"[REST-KLINES] V2 request error: {e}")

    if not rows:
        log(
            "[REST-KLINES] both V3/V2 returned no rows: "
            f"symbol={norm_symbol}, interval={interval}, start={start_ms}, end={end_ms}, raw_limit={raw_limit}"
        )
        raise KlineRestError(
            f"REST kline 조회 실패: symbol={norm_symbol}, interval={interval}"
        )

    # 3) 공통 포맷으로 정규화
    rows_normalized = _normalize_rows_to_common_format(rows, iv_ms)
    if not rows_normalized:
        log(
            "[REST-KLINES] no normalized rows after format conversion: "
            f"symbol={norm_symbol}, interval={interval}"
        )
        raise KlineRestError(
            f"REST kline 정규화 실패: symbol={norm_symbol}, interval={interval}"
        )

    # 4) openTime 기반 정렬 (list/tuple/dict 모두 지원) → 이제는 list[list] 기준
    try:
        rows_normalized.sort(key=_extract_ts_from_rest_row)
    except Exception as e:
        # 정렬 실패하면 그대로 사용 (최악의 경우지만 로직은 계속)
        log(f"[REST-KLINES] sort error (ignore): {e}")

    # 정렬 결과 first/last ts 진단
    if rows_normalized:
        first_ts = _extract_ts_from_rest_row(rows_normalized[0])
        last_ts = _extract_ts_from_rest_row(rows_normalized[-1])
        if first_ts <= 0 or last_ts <= 0:
            log(
                "[REST-KLINES] WARN: unable to extract valid ts from normalized rows "
                f"(first_ts={first_ts}, last_ts={last_ts})"
            )
        else:
            log(
                f"[REST-KLINES] normalized ts range: first_ts={first_ts}, last_ts={last_ts}"
            )

    # 5) limit 개수만큼만 뒤에서 자르기
    if len(rows_normalized) > limit:
        rows_normalized = rows_normalized[-limit:]

    log(
        f"[REST-KLINES] loaded {len(rows_normalized)} rows for "
        f"{norm_symbol} {interval} (requested limit={limit}, raw_limit={raw_limit})"
    )
    return rows_normalized


__all__ = [
    "fetch_klines_rest",
    "KlineRestError",
]
