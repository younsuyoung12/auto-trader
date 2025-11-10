"""strategies_range.py
박스장(레인지) 전략 전용 모듈.

이 모듈이 하는 일:
1. 최근 N개 3분봉에서 박스가 형성돼 있는지 본다.
2. 박스 상단 쪽이면 숏, 하단 쪽이면 롱 신호를 낸다.
3. 너무 좁은 박스는 무시한다.
4. ATR 수축/15분 이격 과다일 때는 '오늘은 박스 안 쓰자' 라고 신호를 낼 수 있게 한다.

2025-11-10 완화 (최종):
- 15m EMA 이격 기준을 0.3%가 아니라 1%로 올렸다. (0.01)
  → 지금처럼 dist=0.0068 (=0.68%) 나와도 막지 말자는 뜻.
- ATR 수축 조건은 0.6 그대로 둔다.
- 왜 막혔는지는 log(...)로 콘솔에 남긴다.
"""

from __future__ import annotations

import math
from typing import List, Optional

from indicators import ema, calc_atr, Candle
from telelog import log  # 왜 막혔는지 콘솔에서 바로 보려고 추가

# 타입 별칭
Candles = List[Candle]


# ─────────────────────────────
# 기본 박스장 신호
# ─────────────────────────────
def decide_signal_range(candles_3m: Candles, lookback: int = 40) -> Optional[str]:
    """3분봉 캔들로 박스장 상단/하단을 판단해서 롱/숏을 결정한다.

    반환값:
        "LONG" / "SHORT" / None
    """
    if len(candles_3m) < lookback:
        return None

    # 최근 3m 구간에서 박스 높이 계산
    recent = candles_3m[-lookback:]
    hi = max(c[2] for c in recent)  # high
    lo = min(c[3] for c in recent)  # low
    now_price = candles_3m[-1][4]

    if lo == 0:
        return None

    box_h = hi - lo
    box_pct = box_h / lo

    # 박스 폭이 너무 좁으면 박스로 보지 않는다 (0.15% 미만)
    if box_pct < 0.0015:
        return None

    # 박스 안에서 상단/하단 영역 나누기
    upper_line = lo + box_h * 0.75
    lower_line = lo + box_h * 0.25

    # 상단으로 치우쳐 있으면 숏
    if now_price >= upper_line:
        return "SHORT"
    # 하단으로 치우쳐 있으면 롱
    if now_price <= lower_line:
        return "LONG"

    return None


# ─────────────────────────────
# 박스장 자체를 오늘은 막을지 판단
# ─────────────────────────────
def should_block_range_today(candles_3m: Candles, candles_15m: Candles) -> bool:
    """박스장 진입 전에 시장 상태를 한 번 더 필터링한다.

    1) ATR 이 너무 죽어 있으면 → True
    2) 15m EMA 이격이 너무 크면 → True
    아니면 False.
    """
    # 1) ATR 수축 체크
    atr_fast = calc_atr(candles_3m, 14)
    atr_slow = calc_atr(candles_3m, 40)
    if atr_fast and atr_slow and atr_slow > 0:
        # 완화 버전: fast < slow * 0.6 일 때만 막는다
        if atr_fast < atr_slow * 0.6:
            log(
                f"[RANGE_BLOCK] ATR compressed: fast={atr_fast:.6f} slow={atr_slow:.6f} → fast < slow*0.6"
            )
            return True

    # 2) 15m 추세 강도 체크
    if candles_15m:
        closes_15 = [c[4] for c in candles_15m]
        if len(closes_15) >= 50:
            e20_15 = ema(closes_15, 20)
            e50_15 = ema(closes_15, 50)
            if not math.isnan(e20_15[-1]) and not math.isnan(e50_15[-1]):
                dist = abs(e20_15[-1] - e50_15[-1]) / e50_15[-1]
                # 최종 완화: dist > 0.01 (1%) 일 때만 막는다
                if dist > 0.01:
                    log(
                        f"[RANGE_BLOCK] 15m EMA distance too wide: dist={dist:.6f} > 0.01"
                    )
                    return True

    # 여기까지 안 걸리면 박스 허용
    return False


__all__ = ["decide_signal_range", "should_block_range_today"]
