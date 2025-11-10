"""strategies_range.py
박스장(레인지) 전략 전용 모듈.

이 모듈이 하는 일:
1. 최근 N개 3분봉에서 박스가 형성돼 있는지 본다.
2. 박스 상단 쪽이면 숏, 하단 쪽이면 롱 신호를 낸다.
3. 너무 좁은 박스는 무시한다.
4. ATR 수축/15분 이격 과다일 때는 '오늘은 박스 안 쓰자' 라고 신호를 낼 수 있게 한다.

2025-11-10 완화:
- 박스가 “너무 쉽게” 막혀서 실제 진입이 0이 되는 상황이 나와서,
  should_block_range_today() 안의 차단 임계값을 조금 올렸다.
  1) ATR 빠른 값이 느린 값의 50% 미만 → 60% 미만일 때만 막도록 완화
  2) 15m EMA 이격 0.2% 초과 → 0.3% 초과일 때만 막도록 완화
이렇게 하면 강한 추세가 있어도 박스가 전부 닫히지는 않는다.
"""

from __future__ import annotations

import math
from typing import List, Optional

from indicators import ema, calc_atr, Candle

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

    # 박스 범위 계산
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

    원래 코드의 의도:
    1) 최근 3m ATR 이 예전(느린 ATR)보다 확 줄어 있으면 → 변동성이 너무 죽은 날이라 박스가 안 먹힌다 → True
    2) 15m EMA 이격이 너무 크면 → 추세가 강한 날이라 박스가 안 먹힌다 → True
    둘 다 아니면 False.
    """
    # 1) ATR 수축 체크
    atr_fast = calc_atr(candles_3m, 14)
    atr_slow = calc_atr(candles_3m, 40)
    if atr_fast and atr_slow and atr_slow > 0:
        # 기존: 0.5 → 완화: 0.6
        if atr_fast < atr_slow * 0.6:
            return True

    # 2) 15m 추세 강도 체크
    if candles_15m:
        closes_15 = [c[4] for c in candles_15m]
        if len(closes_15) >= 50:
            e20_15 = ema(closes_15, 20)
            e50_15 = ema(closes_15, 50)
            if not math.isnan(e20_15[-1]) and not math.isnan(e50_15[-1]):
                dist = abs(e20_15[-1] - e50_15[-1]) / e50_15[-1]
                # 기존: dist > 0.002 (0.2%)
                # 완화: dist > 0.003 (0.3%) 일 때만 막음
                if dist > 0.003:
                    return True

    return False


__all__ = ["decide_signal_range", "should_block_range_today"]
