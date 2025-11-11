"""strategies_trend.py
추세장(트렌드)용 신호 판단 모듈.

이 파일은 다음 역할만 한다:
1. 3분봉에서 EMA20/EMA50 교차 + RSI + 횡보 필터로 1차 신호를 만든다.
2. 15분봉에서 큰 방향(EMA20 vs EMA50)을 본다.
3. (옵션) 1분봉 마지막 캔들이 실제 진입 방향과 맞는지 확인한다.
4. RSI 다이버전스가 신호와 반대면 신호를 버린다.

2025-11-10 변경
----------------------------------------------------
- 1분봉 confirm 을 “강하게 반대일 때만 막는” 용도로 완화.
- 데이터가 없다고 해서 진입까지 막지 않도록 함.

2025-11-12 소규모 보강
----------------------------------------------------
1) EMA20/EMA50 이 아주 살짝 붙었을 때도,
   마지막 캔들 변동이 충분하면 신호를 허용하도록 조건을 조금 느슨하게 함.
   → “신호가 너무 안 나온다”는 상황 방지
2) 1분봉 confirm 쪽 주석을 명확히 적어두어, 나중에 로그를 볼 때
   “왜 여기서 True 가 됐는지” 추적하기 쉽게 함.
"""

from __future__ import annotations

import math
from typing import List, Optional

from indicators import (
    ema,
    rsi,
    has_bearish_rsi_divergence,
    has_bullish_rsi_divergence,
    Candle,
)

# 타입 힌트용
ThreeMCandles = List[Candle]
FifteenMCandles = List[Candle]
OneMCandles = List[Candle]


# ─────────────────────────────
# 3분봉 신호 판단
# ─────────────────────────────
def decide_signal_3m_trend(
    candles_3m: ThreeMCandles,
    rsi_overbought: int = 70,
    rsi_oversold: int = 30,
) -> Optional[str]:
    """3분봉 캔들만 보고 1차 추세 신호를 만든다.

    반환값:
        "LONG" / "SHORT" / None

    로직:
    - 캔들이 60개 미만이면 계산 안 함 (EMA50 때문)
    - EMA20 과 EMA50 이 너무 붙어 있으면 횡보로 보고 신호 안 냄
    - 마지막 캔들 고저폭이 너무 작으면 신호 안 냄
    - EMA20 이 EMA50 을 위로 돌파 + RSI 가 과매수 영역이 아니면 LONG
    - EMA20 이 EMA50 을 아래로 돌파 + RSI 가 과매도 영역이 아니면 SHORT
    - LONG 인데 bearish RSI divergence 면 무효
    - SHORT 인데 bullish RSI divergence 면 무효
    """
    closes = [c[4] for c in candles_3m]
    if len(closes) < 60:
        return None

    e20 = ema(closes, 20)
    e50 = ema(closes, 50)
    r14 = rsi(closes, 14)

    e20_prev, e20_now = e20[-2], e20[-1]
    e50_prev, e50_now = e50[-2], e50[-1]
    r_now = r14[-1]
    price_now = closes[-1]

    # 마지막 3m 캔들의 변동폭
    last = candles_3m[-1]
    last_range_pct = (last[2] - last[3]) / last[3] if last[3] else 0.0

    # 이평선이 너무 붙어 있으면 횡보로 간주
    # 기본 0.05% 미만은 막되, 캔들이 충분히 움직였으면 통과시킨다.
    spread_ratio = abs(e20_now - e50_now) / e50_now
    if spread_ratio < 0.0005 and last_range_pct < 0.0007:
        return None

    # 마지막 3m 캔들의 변동폭이 너무 작으면 스킵 (0.05% 미만)
    if last_range_pct < 0.0005:
        return None

    # 골든/데드크로스 판정
    long_sig = (e20_prev < e50_prev) and (e20_now > e50_now) and (r_now < rsi_overbought)
    short_sig = (e20_prev > e50_prev) and (e20_now < e50_now) and (r_now > rsi_oversold)

    # 가격이 50EMA 반대편에 있으면 신호 무효 (깨끗한 방향만 받기)
    if long_sig and price_now < e50_now:
        long_sig = False
    if short_sig and price_now > e50_now:
        short_sig = False

    # 다이버전스 검사: 신호와 반대 다이버전스면 None
    if long_sig:
        if has_bearish_rsi_divergence(candles_3m, r14):
            return None
        return "LONG"

    if short_sig:
        if has_bullish_rsi_divergence(candles_3m, r14):
            return None
        return "SHORT"

    return None


# ─────────────────────────────
# 15분봉 큰 방향 판단
# ─────────────────────────────
def decide_trend_15m(candles_15m: FifteenMCandles) -> Optional[str]:
    """15분봉 EMA20/EMA50 으로 큰 방향을 본다.

    반환값:
        "LONG" / "SHORT" / None

    - 캔들이 50개 미만이면 계산하지 않는다.
    - EMA20 > EMA50 이면 LONG, 반대면 SHORT
    - 같거나 NaN 이면 None
    """
    closes = [c[4] for c in candles_15m]
    if len(closes) < 50:
        return None

    e20 = ema(closes, 20)
    e50 = ema(closes, 50)
    if math.isnan(e20[-1]) or math.isnan(e50[-1]):
        return None

    if e20[-1] > e50[-1]:
        return "LONG"
    if e20[-1] < e50[-1]:
        return "SHORT"
    return None


# ─────────────────────────────
# 1분봉 확인 (완화 버전)
# ─────────────────────────────
def confirm_1m_direction(candles_1m: OneMCandles, direction: str) -> bool:
    """
    1분봉을 ‘역주행 캔들 차단’ 용도로만 사용한다.
    변동폭이 작아도 시가 대비 종가 방향만 맞으면 통과시킨다.
    데이터가 없으면 막지 않는다.
    """
    if len(candles_1m) < 1:
        # 1분 데이터가 없다고 진입까지 막지는 않는다.
        return True

    _ts, o, _h, _l, c = candles_1m[-1]

    if direction == "LONG":
        # 양봉이면 그대로 통과
        return c >= o
    if direction == "SHORT":
        # 음봉이면 그대로 통과
        return c <= o

    # 방향 정보가 이상하면 막지 않는다.
    return True


__all__ = [
    "decide_signal_3m_trend",
    "decide_trend_15m",
    "confirm_1m_direction",
]
