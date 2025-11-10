"""market_data.py
시세/호가(depth) 관련 함수만 모아 둔 모듈.

이 모듈이 하는 일:
- BingX 에서 캔들(klines)을 가져와서 `(ts, open, high, low, close)` 형태의 리스트로 정규화
- 최신 캔들이 지연됐는지 판단할 수 있도록 정렬까지 마친 상태로 리턴
- 진입 직전에 호가 스프레드가 얼마나 벌어져 있는지 확인하기 위해 orderbook(depth) 조회

중요 (2025-11-09 메모):
- 여기서는 BingX **선물(swap)** 엔드포인트(`/openApi/swap/...`)만 사용한다.
- 그래서 run_bot.py 에서 1분/3분/15분 캔들을 호출하면 전부 "선물 차트" 기준으로 신호를 보게 된다.
- 현물로 바꾸고 싶으면 아래 URL 을 spot 용으로만 바꿔주면 된다.

2025-11-10 보강 (심볼 폴백):
- 주문/포지션은 `BTC-USDT` 이렇게 하이픈이 있는 심볼을 쓰는데,
  어떤 계정에서는 캔들 엔드포인트가 `BTC-USDT`로는 빈 배열을 주고
  `BTCUSDT`(하이픈 없는 것)으로만 캔들을 준다.
- 그래서 이 파일에서는 캔들 요청할 때
    1) 원래 심볼로 한 번 호출하고
    2) 결과가 비었고 심볼에 '-'가 있으면, '-'를 뺀 심볼로 한 번 더 호출
  하는 폴백을 넣었다.
- 이렇게 하면 settings 에는 계속 "BTC-USDT" 넣어두고, 주문도 그걸로 날리면서,
  캔들만 자동으로 살릴 수 있다.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import requests

from settings import load_settings
from telelog import log

# 설정을 전역으로 보관 (BASE URL 등)
SET = load_settings()
BASE = SET.bingx_base  # 예: https://open-api.bingx.com


def _fetch_klines_raw(symbol: str, interval: str, limit: int) -> List[Any]:
    """실제로 BingX 선물 캔들 엔드포인트를 한 번 호출하는 저수준 함수."""
    resp = requests.get(
        f"{BASE}/openApi/swap/v2/quote/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
        timeout=12,
    )
    raw = resp.json()
    data = raw.get("data", []) if isinstance(raw, dict) else raw
    if not isinstance(data, list):
        return []
    return data


def _normalize_5cols(data: List[Any]) -> List[Tuple[int, float, float, float, float]]:
    """BingX 캔들 배열을 (ts,o,h,l,c) 형태로 변환."""
    out: List[Tuple[int, float, float, float, float]] = []
    for it in data:
        if isinstance(it, dict):
            ts_val = it.get("time") or it.get("openTime") or it.get("t")
            if not ts_val:
                continue
            try:
                ts = int(ts_val)
                o = float(it.get("open"))
                h = float(it.get("high"))
                l = float(it.get("low"))
                c = float(it.get("close"))
            except Exception:
                continue
            out.append((ts, o, h, l, c))
        else:
            # list 형식: [ts, o, h, l, c, ...]
            try:
                ts = int(it[0])
                o, h, l, c = map(float, it[1:5])
                out.append((ts, o, h, l, c))
            except Exception:
                continue
    out.sort(key=lambda x: x[0])
    return out


def _normalize_6cols(data: List[Any]) -> List[Tuple[int, float, float, float, float, float]]:
    """BingX 캔들 배열을 (ts,o,h,l,c,vol) 형태로 변환."""
    out: List[Tuple[int, float, float, float, float, float]] = []
    for it in data:
        if isinstance(it, dict):
            ts_val = it.get("time") or it.get("openTime") or it.get("t")
            if not ts_val:
                continue
            try:
                ts = int(ts_val)
                o = float(it.get("open"))
                h = float(it.get("high"))
                l = float(it.get("low"))
                c = float(it.get("close"))
                v = float(it.get("volume") or it.get("vol") or 0.0)
            except Exception:
                continue
            out.append((ts, o, h, l, c, v))
        else:
            try:
                ts = int(it[0])
                o = float(it[1])
                h = float(it[2])
                l = float(it[3])
                c = float(it[4])
                v = float(it[5]) if len(it) > 5 else 0.0
            except Exception:
                continue
            out.append((ts, o, h, l, c, v))
    out.sort(key=lambda x: x[0])
    return out


# ─────────────────────────────
# 캔들 데이터 가져오기 (5개짜리)
# ─────────────────────────────
def get_klines(
    symbol: str,
    interval: str,
    limit: int = 120,
) -> List[Tuple[int, float, float, float, float]]:
    """
    1) symbol 그대로 요청
    2) 비어 있고 '-'가 있으면 symbol.replace('-', '') 로 한 번 더 요청
    """
    try:
        raw = _fetch_klines_raw(symbol, interval, limit)
        if not raw and "-" in symbol:
            # 캔들이 안 왔으면 하이픈 뺀 버전으로 재시도
            sym2 = symbol.replace("-", "")
            raw = _fetch_klines_raw(sym2, interval, limit)
    except Exception as e:
        log(f"[KLINES ERROR] symbol={symbol} interval={interval} err={e}")
        return []

    out = _normalize_5cols(raw)

    if out:
        log(f"[KLINES {interval}] ok count={len(out)} last_close={out[-1][4]}")
    else:
        log(f"[KLINES {interval}] empty for {symbol}")

    return out


# ─────────────────────────────
# 캔들 데이터 + 거래량까지 가져오기 (6개짜리)
# ─────────────────────────────
def get_klines_with_volume(
    symbol: str,
    interval: str,
    limit: int = 120,
) -> List[Tuple[int, float, float, float, float, float]]:
    """
    1) symbol 그대로 요청
    2) 비어 있고 '-'가 있으면 symbol.replace('-', '') 로 한 번 더 요청
    """
    try:
        raw = _fetch_klines_raw(symbol, interval, limit)
        if not raw and "-" in symbol:
            sym2 = symbol.replace("-", "")
            raw = _fetch_klines_raw(sym2, interval, limit)
    except Exception as e:
        log(f"[KLINES VOL ERROR] symbol={symbol} interval={interval} err={e}")
        return []

    out = _normalize_6cols(raw)

    if out:
        log(
            f"[KLINES {interval} VOL] ok count={len(out)} "
            f"last_close={out[-1][4]} vol={out[-1][5]}"
        )
    else:
        log(f"[KLINES {interval} VOL] empty for {symbol}")

    return out


# ─────────────────────────────
# 호가(오더북) 데이터 가져오기 (선물 기준)
# ─────────────────────────────
def get_orderbook(symbol: str, limit: int = 5) -> Optional[Dict[str, Any]]:
    """진입 직전에 스프레드를 확인하기 위해 선물 depth 를 조회한다."""
    try:
        resp = requests.get(
            f"{BASE}/openApi/swap/v2/quote/depth",
            params={"symbol": symbol, "limit": limit},
            timeout=8,
        )
        data = resp.json()
        return data.get("data") or data
    except Exception as e:
        log(f"[ORDERBOOK ERROR] symbol={symbol} err={e}")
        return None


__all__ = ["get_klines", "get_klines_with_volume", "get_orderbook"]
