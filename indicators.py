"""indicators.py
====================================================
기술적 지표 및 다이버전스 보조 함수 모듈.

역할
----------------------------------------------------
- EMA, RSI, ATR 같은 기본 기술적 지표 계산을 전담한다.
- 최근 고점/저점 피벗과 RSI 다이버전스를 판별하는 보조 함수를 제공한다.
- 신호 판단(strategies_*.py, signal_flow_ws.py 등)에서 공통으로 사용하는
  "계산 전용" 유틸리티 레이어이며, DB I/O 나 텔레그램 로깅은 하지 않는다.

2025-11-14 변경 / 중요사항
----------------------------------------------------
1) 기존 bot.py 안에 섞여 있던 EMA, RSI, ATR, RSI 다이버전스 계산을 분리해
   단일 모듈(indicators.py)에 통합했다.
2) Candle 타입을 (ts_ms, open, high, low, close) 튜플로 정의해
   - WebSocket 캔들(1m/5m/15m)과
   - DB 기반 분석(signal_analysis_worker)의 공용 입력 포맷으로 사용 가능하게 했다.
3) 이 모듈은 **계산만** 담당하며, 신호/레짐 판단 및 DB 저장은
   strategies_*, signal_flow_ws.py, signal_analysis_worker.py 에서 처리한다.

제공 함수
----------------------------------------------------
- ema(values, length): 단순 EMA 리스트 계산
- rsi(closes, length=14): 표준 RSI 리스트 계산
- calc_atr(candles, length=14): (ts_ms, o, h, l, c) 캔들 시퀀스에서 ATR 값 1개 계산
- has_bearish_rsi_divergence(...): RSI 매도 다이버전스 여부
- has_bullish_rsi_divergence(...): RSI 매수 다이버전스 여부

여기서는 로깅을 최소화해 지표 계산 자체를 가볍게 유지한다.
"""

from __future__ import annotations

import math
from typing import List, Tuple, Optional

# WebSocket / DB 공용 캔들 타입
# ts_ms: epoch milliseconds (int)
# open, high, low, close: float
Candle = Tuple[int, float, float, float, float]  # (ts_ms, open, high, low, close)


# ─────────────────────────────
# EMA
# ─────────────────────────────
def ema(values: List[float], length: int) -> List[float]:
    """EMA(지수이동평균) 계산.

    반환값은 입력 values 와 같은 길이의 리스트이며,
    초기 구간(length 이전)은 NaN 으로 채워서 인덱스 정합을 맞춘다.
    """
    n = len(values)
    if n == 0:
        return []
    if length <= 0:
        # 방어적으로 length <= 0 이면 단순 복사만 반환
        return list(values)
    if n < length:
        # 길이가 부족하면 전부 NaN 으로 리턴
        return [math.nan] * n

    k = 2 / (length + 1)  # EMA 계수
    out: List[float] = [math.nan] * (length - 1)

    # 첫 EMA 는 단순이동평균(SMA)으로 시작
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

    - 입력: 종가 리스트(closes)
    - 반환: 원본 길이에 맞춘 RSI 리스트
    - 길이가 length+1 미만이면 전부 NaN 으로 채운다.
    """
    n = len(closes)
    if n == 0:
        return []
    if length <= 0:
        # 방어적으로 length <= 0 이면 전부 NaN
        return [math.nan] * n
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

    # 이후 값들 (Wilders smoothing)
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

    candles 는 (ts_ms, open, high, low, close) 튜플 리스트여야 한다.
    length+1 개 미만이면 None 을 리턴한다.

    반환값은 "마지막 구간" 기준 ATR 1개(float) 이다.
    """
    if length <= 0:
        return None
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

    # 단순 평균 ATR (필요시 Wilder 방식으로 변경 가능)
    atr = sum(trs[-length:]) / length
    return atr


# ─────────────────────────────
# 다이버전스 보조 피벗 찾기
# ─────────────────────────────
def _find_last_two_pivot_highs(candles: List[Candle]) -> List[int]:
    """최근 두 개의 고점 피벗 인덱스를 뒤에서부터 찾아서 리턴.

    - i 를 마지막 이전부터 1까지 거꾸로 내려오면서
      i-1, i, i+1 의 high 를 비교해 고점인지 판별한다.
    - 두 개를 찾으면 그 두 개의 인덱스를 시간순(오래된 것 먼저)으로 반환.
    - 두 개 미만이면 빈 리스트 또는 1개만 들어있는 리스트가 리턴된다.
    """
    idxs: List[int] = []
    if len(candles) < 3:
        return idxs

    for i in range(len(candles) - 2, 0, -1):  # 뒤에서부터 찾기 (i+1 접근 위해 0까지)
        if i + 1 >= len(candles):
            continue
        _, _, high, _, _ = candles[i]
        _, _, prev_high, _, _ = candles[i - 1]
        _, _, next_high, _, _ = candles[i + 1]
        if high > prev_high and high > next_high:
            idxs.append(i)
            if len(idxs) == 2:
                break
    return list(reversed(idxs))  # 오래된 것부터 앞으로 오게


def _find_last_two_pivot_lows(candles: List[Candle]) -> List[int]:
    """최근 두 개의 저점 피벗 인덱스를 뒤에서부터 찾아서 리턴."""
    idxs: List[int] = []
    if len(candles) < 3:
        return idxs

    for i in range(len(candles) - 2, 0, -1):
        if i + 1 >= len(candles):
            continue
        _, _, _, low, _ = candles[i]
        _, _, _, prev_low, _ = candles[i - 1]
        _, _, _, next_low, _ = candles[i + 1]
        if low < prev_low and low < next_low:
            idxs.append(i)
            if len(idxs) == 2:
                break
    return list(reversed(idxs))


# ─────────────────────────────
# RSI 다이버전스
# ─────────────────────────────
def has_bearish_rsi_divergence(candles: List[Candle], rsi_vals: List[float]) -> bool:
    """RSI 매도(하락) 다이버전스 여부.

    조건:
    - 가격: 최근 두 피벗 고점에서 고점이 높아졌고
    - RSI: 같은 지점의 RSI 값은 낮아졌다면 매도 다이버전스로 본다.
    """
    piv = _find_last_two_pivot_highs(candles)
    if len(piv) < 2:
        return False
    i1, i2 = piv
    if i1 >= len(rsi_vals) or i2 >= len(rsi_vals):
        return False

    _, _, high1, _, _ = candles[i1]
    _, _, high2, _, _ = candles[i2]
    price_up = high2 > high1
    rsi_down = rsi_vals[i2] < rsi_vals[i1]
    return price_up and rsi_down


def has_bullish_rsi_divergence(candles: List[Candle], rsi_vals: List[float]) -> bool:
    """RSI 매수(상승) 다이버전스 여부.

    조건:
    - 가격: 최근 두 피벗 저점에서 저점이 낮아졌고
    - RSI: 같은 지점의 RSI 값은 높아졌다면 매수 다이버전스로 본다.
    """
    piv = _find_last_two_pivot_lows(candles)
    if len(piv) < 2:
        return False
    i1, i2 = piv
    if i1 >= len(rsi_vals) or i2 >= len(rsi_vals):
        return False

    _, _, _, low1, _ = candles[i1]
    _, _, _, low2, _ = candles[i2]
    price_down = low2 < low1
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
