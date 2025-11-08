"""indicators.py
기술적 지표 및 다이버전스 보조 함수 모듈.

이 모듈은 신호 판단 모듈(strategies_*.py)에서 공통으로 사용하는 계산만 모아 두었다.
원래 bot.py 안에 섞여 있던 EMA, RSI, ATR, 피벗 탐색, RSI 다이버전스 검사가 여기로 옮겨졌다.

주요 기능:
- ema(values, length): 단순 EMA 계산
- rsi(closes, length): 표준 RSI 계산 (기본 14)
- calc_atr(candles, length): (ts, o, h, l, c) 캔들에서 ATR 계산
- _find_last_two_pivot_highs / _find_last_two_pivot_lows: 최근 두 개 피벗 탐색
- has_bearish_rsi_divergence / has_bullish_rsi_divergence: RSI 다이버전스 확인

여기서는 로깅을 최소화해서, 지표 계산 자체는 가볍게 유지한다.
"""

from __future__ import annotations

import math
from typing import List, Tuple, Optional

Candle = Tuple[int, float, float, float, float]  # (ts, open, high, low, close)


# ─────────────────────────────
# EMA
# ─────────────────────────────
def ema(values: List[float], length: int) -> List[float]:
    """EMA(지수이동평균) 계산.

    반환값은 입력 values 와 같은 길이의 리스트이며,
    초기 구간(length 이전)은 NaN 으로 채워서 인덱스 정합을 맞춘다.
    """
    n = len(values)
    if n < length:
        # 길이가 부족하면 전부 NaN 으로 리턴
        return [math.nan] * n

    k = 2 / (length + 1)  # EMA 계수
    out: List[float] = [math.nan] * (length - 1)

    # 첫 EMA 는 단순이동평균으로 시작
    ema_prev = sum(values[:length]) / length
    out.append(ema_prev)

    # 그 다음부터는 EMA 공식 적용
    for v in values[length:]:
        ema_prev = v * k + ema_prev * (1 - k)
        out.append(ema_prev)

    return out


# ─────────────────────────────
# RSI
# ─────────────────────────────
def rsi(closes: List[float], length: int = 14) -> List[float]:
    """표준 RSI 계산.

    - 입력은 종가 리스트
    - 반환은 원래 길이를 맞춘 RSI 리스트
    - length+1 개 미만이면 전부 NaN
    """
    n = len(closes)
    if n < length + 1:
        return [math.nan] * n

    gains: List[float] = []
    losses: List[float] = []
    for i in range(1, n):
        change = closes[i] - closes[i - 1]
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))

    # 초기 평균값
    avg_gain = sum(gains[:length]) / length
    avg_loss = sum(losses[:length]) / length

    rsis: List[float] = [math.nan] * length
    rs = (avg_gain / avg_loss) if avg_loss > 0 else float("inf")
    rsis.append(100 - 100 / (1 + rs))

    # 이후 값들
    for i in range(length, len(gains)):
        avg_gain = (avg_gain * (length - 1) + gains[i]) / length
        avg_loss = (avg_loss * (length - 1) + losses[i]) / length
        rs = (avg_gain / avg_loss) if avg_loss > 0 else float("inf")
        rsis.append(100 - 100 / (1 + rs))

    return rsis


# ─────────────────────────────
# ATR
# ─────────────────────────────
def calc_atr(candles: List[Candle], length: int = 14) -> Optional[float]:
    """기본 ATR 계산.

    candles 는 (ts, open, high, low, close) 튜플 리스트여야 한다.
    length+1 개 미만이면 None 을 리턴한다.
    """
    if len(candles) < length + 1:
        return None

    trs: List[float] = []
    for i in range(1, len(candles)):
        _, _, high, low, close = candles[i]
        _, _, prev_high, prev_low, prev_close = candles[i - 1]
        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )
        trs.append(tr)

    if len(trs) < length:
        return None

    atr = sum(trs[-length:]) / length
    return atr


# ─────────────────────────────
# 다이버전스 보조 피벗 찾기
# ─────────────────────────────
def _find_last_two_pivot_highs(candles: List[Candle]) -> List[int]:
    """최근 두 개의 고점 피벗 인덱스를 뒤에서부터 찾아서 리턴한다.

    - i 가 마지막 이전부터 1까지 거꾸로 내려오면서 i-1, i, i+1 을 비교해 고점인지 본다.
    - 두 개를 찾으면 그 두 개의 인덱스를 시간순(오래된 것 먼저)으로 반환한다.
    - 두 개 미만이면 빈 리스트 또는 1개만 들어있는 리스트가 리턴된다.
    """
    idxs: List[int] = []
    for i in range(len(candles) - 2, 1, -1):  # 뒤에서부터 찾기
        if candles[i][2] > candles[i - 1][2] and candles[i][2] > candles[i + 1][2]:
            idxs.append(i)
            if len(idxs) == 2:
                break
    return list(reversed(idxs))  # 오래된 것부터 앞으로 오게


def _find_last_two_pivot_lows(candles: List[Candle]) -> List[int]:
    """최근 두 개의 저점 피벗 인덱스를 뒤에서부터 찾아서 리턴한다."""
    idxs: List[int] = []
    for i in range(len(candles) - 2, 1, -1):
        if candles[i][3] < candles[i - 1][3] and candles[i][3] < candles[i + 1][3]:
            idxs.append(i)
            if len(idxs) == 2:
                break
    return list(reversed(idxs))


# ─────────────────────────────
# RSI 다이버전스
# ─────────────────────────────
def has_bearish_rsi_divergence(candles: List[Candle], rsi_vals: List[float]) -> bool:
    """상승 다이버전스(매도 관점):
    - 가격은 고점을 높였는데
    - RSI 는 오히려 낮아졌으면 매도 다이버전스라고 본다.
    """
    piv = _find_last_two_pivot_highs(candles)
    if len(piv) < 2:
        return False
    i1, i2 = piv
    price_up = candles[i2][2] > candles[i1][2]
    rsi_down = rsi_vals[i2] < rsi_vals[i1]
    return price_up and rsi_down


def has_bullish_rsi_divergence(candles: List[Candle], rsi_vals: List[float]) -> bool:
    """하락 다이버전스(매수 관점):
    - 가격은 저점을 낮췄는데
    - RSI 는 오히려 높아졌으면 매수 다이버전스라고 본다.
    """
    piv = _find_last_two_pivot_lows(candles)
    if len(piv) < 2:
        return False
    i1, i2 = piv
    price_down = candles[i2][3] < candles[i1][3]
    rsi_up = rsi_vals[i2] > rsi_vals[i1]
    return price_down and rsi_up


__all__ = [
    "ema",
    "rsi",
    "calc_atr",
    "has_bearish_rsi_divergence",
    "has_bullish_rsi_divergence",
    "_find_last_two_pivot_highs",
    "_find_last_two_pivot_lows",
    "Candle",
]
