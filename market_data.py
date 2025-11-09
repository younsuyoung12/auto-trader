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
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import requests

from settings import load_settings
from telelog import log

# 설정을 전역으로 보관 (BASE URL 등)
SET = load_settings()
BASE = SET.bingx_base  # 예: https://open-api.bingx.com


# ─────────────────────────────
# 캔들 데이터 가져오기 (선물 기준)
# ─────────────────────────────
def get_klines(
    symbol: str,
    interval: str,
    limit: int = 120,
) -> List[Tuple[int, float, float, float, float]]:
    """BingX 선물(swap) klines 엔드포인트에서 캔들을 받아와서 정규화한다.

    반환 형식:
        [ (timestamp(ms), open, high, low, close), ... ]
    - timestamp 는 int(ms) 로 맞춰서 정렬해서 리턴한다.
    - 일부 응답은 dict 형식이고, 일부는 list 형식이라 둘 다 처리한다.
    - 실패하면 빈 리스트를 리턴한다 (상위에서 재시도/대기하도록).
    """
    try:
        resp = requests.get(
            f"{BASE}/openApi/swap/v2/quote/klines",  # ← 선물용
            params={"symbol": symbol, "interval": interval, "limit": limit},
            timeout=12,
        )
        raw = resp.json()
    except Exception as e:
        log(f"[KLINES ERROR] symbol={symbol} interval={interval} err={e}")
        return []

    # BingX 는 보통 {"data": [...]} 형태로 준다.
    data = raw.get("data", []) if isinstance(raw, dict) else raw
    out: List[Tuple[int, float, float, float, float]] = []

    for it in data:
        # dict 형식일 때
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
                # 숫자 변환 실패하면 해당 캔들은 건너뛴다.
                continue
            out.append((ts, o, h, l, c))
        else:
            # list 형식일 때: [ts, open, high, low, close, ...]
            try:
                ts = int(it[0])
                o, h, l, c = map(float, it[1:5])
                out.append((ts, o, h, l, c))
            except Exception:
                continue

    # 타임스탬프 기준으로 정렬 (오름차순)
    out.sort(key=lambda x: x[0])

    if out:
        log(f"[KLINES {interval}] ok count={len(out)} last_close={out[-1][4]}")
    else:
        log(f"[KLINES {interval}] empty for {symbol}")

    return out


# ─────────────────────────────
# 호가(오더북) 데이터 가져오기 (선물 기준)
# ─────────────────────────────
def get_orderbook(symbol: str, limit: int = 5) -> Optional[Dict[str, Any]]:
    """진입 직전에 스프레드를 확인하기 위해 선물 depth 를 조회한다.

    반환 형식은 BingX 원본 그대로(dict) 를 리턴하고,
    상위 코드에서 `data.get("bids")`, `data.get("asks")` 식으로 접근해서
    최우선 호가를 뽑아 쓰게 한다.

    실패하면 None 을 리턴해서 상위에서 '호가 확인 불가 → 그냥 진행 or 스킵' 을 결정하도록 한다.
    """
    try:
        resp = requests.get(
            f"{BASE}/openApi/swap/v2/quote/depth",  # ← 선물용
            params={"symbol": symbol, "limit": limit},
            timeout=8,
        )
        data = resp.json()
        # BingX 는 보통 {"data": {...}} 형태로 준다.
        return data.get("data") or data
    except Exception as e:
        log(f"[ORDERBOOK ERROR] symbol={symbol} err={e}")
        return None


__all__ = ["get_klines", "get_orderbook"]
