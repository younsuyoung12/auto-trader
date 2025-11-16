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

2025-11-17 변경 / 레짐·대시보드용 피처 헬퍼 추가
----------------------------------------------------
4) 단순 이동평균(sma) 함수를 추가해 EMA 외에 기본 평균선도 손쉽게 계산할 수 있게 했다.
5) build_regime_features_from_candles(...) 함수를 추가해
   - 최근 캔들 시퀀스에서 추세/변동성/박스 여부를 가늠하는 간단한 피처 집합을
   - 하나의 dict로 만들어 돌려준다.
   이 dict는
     · signal_analysis_worker 에서 레짐 점수 계산·저장용으로 쓰거나,
     · signal_flow_ws / position_watch_ws 에서 extra["regime_features"] 에 그대로 실어
       GPT 의사결정 레이어에 참고 정보로 전달하는 용도로 사용한다.
   DB 접근은 하지 않고, 순수 캔들 리스트만 받아서 계산하는 순수 함수로 유지한다.

제공 함수
----------------------------------------------------
- sma(values, length): 단순 이동평균(SMA) 리스트 계산
- ema(values, length): 지수 이동평균(EMA) 리스트 계산
- rsi(closes, length=14): 표준 RSI 리스트 계산
- calc_atr(candles, length=14): (ts_ms, o, h, l, c) 캔들 시퀀스에서 ATR 값 1개 계산
- has_bearish_rsi_divergence(...): RSI 매도 다이버전스 여부
- has_bullish_rsi_divergence(...): RSI 매수 다이버전스 여부
- build_regime_features_from_candles(...): 캔들 배열에서 레짐 힌트·변동성 피처 dict 생성

여기서는 로깅을 최소화해 지표 계산 자체를 가볍게 유지한다.
"""

from __future__ import annotations

import math
from typing import Dict, List, Tuple, Optional

# WebSocket / DB 공용 캔들 타입
# ts_ms: epoch milliseconds (int)
# open, high, low, close: float
Candle = Tuple[int, float, float, float, float]  # (ts_ms, open, high, low, close)


# ─────────────────────────────
# SMA (단순 이동평균)
# ─────────────────────────────

def sma(values: List[float], length: int) -> List[float]:
    """단순 이동평균(SMA) 계산.

    - 반환 리스트 길이는 입력 values 와 동일하게 맞춘다.
    - length 이전 구간은 NaN 으로 채워 인덱스 정합을 유지한다.
    - length <= 0 이면 방어적으로 원본 값을 그대로 복사해 반환한다.
    """
    n = len(values)
    if n == 0:
        return []
    if length <= 0:
        return list(values)
    if n < length:
        return [math.nan] * n

    out: List[float] = [math.nan] * (length - 1)
    window_sum = sum(values[:length])
    out.append(window_sum / length)

    for i in range(length, n):
        window_sum += values[i] - values[i - length]
        out.append(window_sum / length)

    return out


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
# 레짐·대시보드용 피처 헬퍼 (순수 캔들 기반)
# ─────────────────────────────

def build_regime_features_from_candles(
    candles: List[Candle],
    *,
    fast_ema_len: int = 20,
    slow_ema_len: int = 50,
    atr_len: int = 14,
    rsi_len: int = 14,
    range_window: int = 50,
) -> Optional[Dict[str, float]]:
    """캔들 배열에서 추세/변동성/박스 여부를 가늠하는 간단한 피처 묶음을 생성한다.

    반환 예시(dict):
        {
          "last_close": 94500.0,
          "ema_fast": 94480.0,
          "ema_slow": 94300.0,
          "ema_dist_pct": 0.0019,
          "ema_fast_slope_pct": 0.0003,
          "atr": 350.0,
          "atr_pct": 0.0037,
          "range_pct": 0.0052,
          "rsi_last": 57.3,
          "trend_strength": 1.5,
          "range_strength": 0.8,
          "regime_hint": "TREND_LIKE"  # / "RANGE_LIKE" / "MIXED" / "UNKNOWN"
        }

    - DB 를 직접 보지 않고, 넘겨받은 candles 리스트만 사용한다.
    - signal_analysis_worker, signal_flow_ws, position_watch_ws 에서
      이 함수를 호출해 extra["regime_features"] 에 그대로 실어 보내면
      GPT / 대시보드 양쪽에서 재사용 가능하다.
    """
    n = len(candles)
    need = max(fast_ema_len, slow_ema_len, atr_len + 1, rsi_len + 1, range_window)
    if n < need or n == 0:
        return None

    closes = [c[4] for c in candles]
    highs = [c[2] for c in candles[-range_window:]]
    lows = [c[3] for c in candles[-range_window:]]
    last_close = closes[-1]
    if last_close <= 0:
        return None

    # EMA 기반 추세 강도
    ema_fast_list = ema(closes, fast_ema_len)
    ema_slow_list = ema(closes, slow_ema_len)
    ema_fast_val = ema_fast_list[-1]
    ema_slow_val = ema_slow_list[-1]

    # 유효한 이전 EMA 값을 하나 더 찾아 기울기(최근 기울기)를 계산한다.
    def _last_two_valid(vals: List[float]) -> Tuple[Optional[float], Optional[float]]:
        valid: List[float] = [v for v in vals if not math.isnan(v)]
        if len(valid) < 2:
            return None, None
        return valid[-2], valid[-1]

    ema_fast_prev, ema_fast_last = _last_two_valid(ema_fast_list)

    if math.isnan(ema_fast_val) or math.isnan(ema_slow_val):
        ema_dist_pct = math.nan
    else:
        ema_dist_pct = (ema_fast_val - ema_slow_val) / last_close

    if ema_fast_prev is None or ema_fast_last is None:
        ema_fast_slope_pct = math.nan
    else:
        ema_fast_slope_pct = (ema_fast_last - ema_fast_prev) / last_close

    # ATR 및 가격 밴드 폭
    atr_val = calc_atr(candles[-(atr_len + 1) :], length=atr_len)
    atr_pct = (atr_val / last_close) if (atr_val is not None and last_close > 0) else math.nan

    high_recent = max(highs)
    low_recent = min(lows)
    range_pct = (high_recent - low_recent) / last_close if last_close > 0 else math.nan

    # RSI 마지막 값
    rsi_vals = rsi(closes, length=rsi_len)
    rsi_last = rsi_vals[-1] if rsi_vals else math.nan

    # 간단한 레짐 힌트 (실제 전략 로직이 아니라 GPT/대시보드 참고용)
    regime_hint = "UNKNOWN"
    trend_strength = math.nan
    range_strength = math.nan

    if not any(math.isnan(x) for x in (ema_dist_pct, atr_pct, range_pct)):
        # 추세 강도: EMA 벌어짐 대비 ATR
        if atr_pct > 0:
            trend_strength = abs(ema_dist_pct) / atr_pct
        else:
            trend_strength = 0.0

        # 박스 강도: 가격 밴드 폭 대비 ATR (단순 휴리스틱)
        range_strength = range_pct / atr_pct if atr_pct > 0 else range_pct

        # 매우 단순한 휴리스틱 레이블링
        if abs(ema_dist_pct) >= 0.003 and atr_pct >= 0.003 and trend_strength >= 1.2:
            regime_hint = "TREND_LIKE"
        elif range_pct <= 0.004 and abs(ema_dist_pct) <= 0.002:
            regime_hint = "RANGE_LIKE"
        else:
            regime_hint = "MIXED"

    features: Dict[str, float] = {
        "last_close": float(last_close),
        "ema_fast": float(ema_fast_val),
        "ema_slow": float(ema_slow_val),
        "ema_dist_pct": float(ema_dist_pct),
        "ema_fast_slope_pct": float(ema_fast_slope_pct),
        "atr": float(atr_val) if atr_val is not None else math.nan,
        "atr_pct": float(atr_pct),
        "range_pct": float(range_pct),
        "rsi_last": float(rsi_last),
        "trend_strength": float(trend_strength),
        "range_strength": float(range_strength),
    }

    # regime_hint 는 문자열이라 dict 를 Dict[str, float] 로만 한정하지 않고
    # 호출부에서 필요하면 별도 필드로 저장해서 사용한다.
    # 여기서는 편의상 dict 에 같이 실어 보내되, 타입 경고를 피하기 위해
    # 호출부에서 typing.TypedDict 등을 사용해 래핑하는 것을 권장한다.
    features["_regime_hint"] = {  # type: ignore[assignment]
        "TREND_LIKE": 1.0,
        "RANGE_LIKE": 0.0,
        "MIXED": 0.5,
        "UNKNOWN": math.nan,
    }.get(regime_hint, math.nan)

    # 문자열 레이블은 별도 키로 돌려준다.
    # (GPT extra / 대시보드에서 그대로 쓰기 좋게)
    features["_regime_label"] = {  # type: ignore[assignment]
        "TREND_LIKE": 1.0,
        "RANGE_LIKE": -1.0,
        "MIXED": 0.0,
        "UNKNOWN": math.nan,
    }.get(regime_hint, math.nan)

    return features


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
    "sma",
    "ema",
    "rsi",
    "calc_atr",
    "has_bearish_rsi_divergence",
    "has_bullish_rsi_divergence",
    "build_regime_features_from_candles",
    "_find_last_two_pivot_highs",
    "_find_last_two_pivot_lows",
    "Candle",
]
