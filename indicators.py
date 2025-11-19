"""indicators.py
====================================================
기술적 지표 및 레짐 피처 계산 모듈.

역할
----------------------------------------------------
- WebSocket / DB 에서 얻은 캔들(1m/5m/15m ...)에 대해
  EMA, RSI, ATR, MACD, 스토캐스틱, 볼린저 밴드, ADX 등을 계산한다.
- 신호 판단/전략/GPT 레이어는 이 모듈의 함수만 호출해서
  순수 수치 지표를 얻고, I/O 나 텔레그램 로깅은 전혀 수행하지 않는다.

2025-11-19 변경 요약
----------------------------------------------------
1) MACD, 스토캐스틱(Stoch), 볼린저 밴드(BBands), ADX, OBV 지표를 추가했다.
2) build_regime_features_from_candles(...) 를 확장해
   - EMA20/50/100/200, ATR, RSI, BBands 폭, ADX, Stoch, MACD 를 한 번에 계산해
   - GPT 컨텍스트에 바로 실어 보낼 수 있는 피처 dict 를 돌려준다.
3) 기존 함수 시그니처는 유지해 이전 호출부와 완전히 호환된다.
"""

from __future__ import annotations

import math
from typing import Dict, List, Tuple, Optional, Iterable

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
        _, _, _, _, prev_close = candles[i - 1]
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
# MACD
# ─────────────────────────────

def macd(
    values: List[float],
    fast_len: int = 12,
    slow_len: int = 26,
    signal_len: int = 9,
) -> Tuple[List[float], List[float], List[float]]:
    """MACD(12·26·9 기본값) 계산.

    반환:
        macd_line, signal_line, hist 리스트 (길이는 values 와 동일)
    초반부 데이터가 부족하면 NaN 으로 채운다.
    """
    n = len(values)
    if n == 0:
        return [], [], []

    ema_fast = ema(values, fast_len)
    ema_slow = ema(values, slow_len)

    macd_line: List[float] = []
    for f, s in zip(ema_fast, ema_slow):
        if math.isnan(f) or math.isnan(s):
            macd_line.append(math.nan)
        else:
            macd_line.append(f - s)

    # signal 은 macd_line 에 대해 EMA 를 적용하되, 앞쪽 NaN 은 그대로 둔다.
    signal_line: List[float] = [math.nan] * n
    # 유효 구간만 잘라서 EMA 계산
    first_valid = next((i for i, v in enumerate(macd_line) if not math.isnan(v)), None)
    if first_valid is not None:
        segment = macd_line[first_valid:]
        ema_seg = ema(segment, signal_len)
        for idx, v in enumerate(ema_seg):
            signal_line[first_valid + idx] = v

    hist: List[float] = []
    for m, s in zip(macd_line, signal_line):
        if math.isnan(m) or math.isnan(s):
            hist.append(math.nan)
        else:
            hist.append(m - s)

    return macd_line, signal_line, hist


# ─────────────────────────────
# 볼린저 밴드
# ─────────────────────────────

def bollinger_bands(
    values: List[float],
    length: int = 20,
    num_std: float = 2.0,
) -> Tuple[List[float], List[float], List[float]]:
    """볼린저 밴드 (중심선, 상단, 하단) 계산.

    - 길이가 length 미만인 구간은 NaN.
    """
    n = len(values)
    if n == 0:
        return [], [], []
    if length <= 0:
        return [math.nan] * n, [math.nan] * n, [math.nan] * n

    mid: List[float] = [math.nan] * n
    upper: List[float] = [math.nan] * n
    lower: List[float] = [math.nan] * n

    for i in range(length - 1, n):
        window = values[i - length + 1 : i + 1]
        if len(window) < length:
            continue
        mean = sum(window) / length
        var = sum((x - mean) ** 2 for x in window) / length
        std = math.sqrt(var)

        mid[i] = mean
        upper[i] = mean + num_std * std
        lower[i] = mean - num_std * std

    return mid, upper, lower


# ─────────────────────────────
# OBV (On-Balance Volume)
# ─────────────────────────────

def obv(closes: List[float], volumes: List[float]) -> List[float]:
    """OBV 계산. 길이가 맞지 않으면 공통 구간까지만 사용한다."""
    n = min(len(closes), len(volumes))
    if n == 0:
        return []

    closes = closes[:n]
    volumes = volumes[:n]

    out: List[float] = [0.0] * n
    for i in range(1, n):
        if closes[i] > closes[i - 1]:
            out[i] = out[i - 1] + volumes[i]
        elif closes[i] < closes[i - 1]:
            out[i] = out[i - 1] - volumes[i]
        else:
            out[i] = out[i - 1]
    return out


# ─────────────────────────────
# ADX
# ─────────────────────────────

def adx(candles: List[Candle], length: int = 14) -> Optional[float]:
    """ADX(평균 방향성 지수) 마지막 값 1개를 계산한다.

    - candles 가 2 * length 개 미만이면 None.
    - Wilder 방식으로 +DM, -DM, TR, DX, ADX 를 계산한다.
    """
    n = len(candles)
    if length <= 0 or n < 2 * length:
        return None

    plus_dm: List[float] = []
    minus_dm: List[float] = []
    trs: List[float] = []

    for i in range(1, n):
        _, _, high, low, close = candles[i]
        _, _, prev_high, prev_low, prev_close = candles[i - 1]

        up_move = high - prev_high
        down_move = prev_low - low

        plus = up_move if (up_move > down_move and up_move > 0) else 0.0
        minus = down_move if (down_move > up_move and down_move > 0) else 0.0

        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )

        plus_dm.append(plus)
        minus_dm.append(minus)
        trs.append(tr)

    if len(trs) < length:
        return None

    # 초기 smoothed 값
    tr_n = sum(trs[:length])
    plus_dm_n = sum(plus_dm[:length])
    minus_dm_n = sum(minus_dm[:length])

    def _calc_di(tr_val: float, plus_val: float, minus_val: float) -> Tuple[float, float, float]:
        if tr_val <= 0:
            return 0.0, 0.0, 0.0
        plus_di = 100.0 * plus_val / tr_val
        minus_di = 100.0 * minus_val / tr_val
        denom = plus_di + minus_di
        if denom <= 0:
            return plus_di, minus_di, 0.0
        dx = 100.0 * abs(plus_di - minus_di) / denom
        return plus_di, minus_di, dx

    _, _, first_dx = _calc_di(tr_n, plus_dm_n, minus_dm_n)
    dx_values: List[float] = [first_dx]

    # 이후 smoothed 값
    for i in range(length, len(trs)):
        tr_n = tr_n - (tr_n / length) + trs[i]
        plus_dm_n = plus_dm_n - (plus_dm_n / length) + plus_dm[i]
        minus_dm_n = minus_dm_n - (minus_dm_n / length) + minus_dm[i]
        _, _, dx_val = _calc_di(tr_n, plus_dm_n, minus_dm_n)
        dx_values.append(dx_val)

    if len(dx_values) < length:
        # 샘플이 부족하면 마지막 DX 를 그대로 ADX 로 사용
        return dx_values[-1]

    # 첫 ADX 는 앞 length 개 DX 의 평균
    adx_val = sum(dx_values[:length]) / length
    for dx_val in dx_values[length:]:
        adx_val = ((adx_val * (length - 1)) + dx_val) / length

    return adx_val


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
    """캔들 배열에서 추세/변동성/박스 여부를 가늠하는 피처 묶음을 생성한다.

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
          "adx_last": 27.1,
          "bb_width_pct": 0.004,
          "stoch_k": 65.0,
          "stoch_d": 60.0,
          "macd": ...,
          "macd_signal": ...,
          "macd_hist": ...,
          "trend_strength": 1.5,
          "range_strength": 0.8,
          "_regime_hint": ...,
          "_regime_label": ...,
        }

    - DB 를 직접 보지 않고, 넘겨받은 candles 리스트만 사용한다.
    - signal_analysis_worker, signal_flow_ws, position_watch_ws 에서
      이 함수를 호출해 extra["regime_features"] 에 그대로 실어 보내면
      GPT / 대시보드 양쪽에서 재사용 가능하다.
    """
    n = len(candles)
    need = max(fast_ema_len, slow_ema_len, atr_len + 1, rsi_len + 1, range_window)
    if n == 0 or n < need:
        return None

    closes = [c[4] for c in candles]
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    last_close = closes[-1]
    if last_close <= 0:
        return None

    # EMA 기반 추세 강도 (20/50 기본)
    ema_fast_list = ema(closes, fast_ema_len)
    ema_slow_list = ema(closes, slow_ema_len)
    ema_fast_val = ema_fast_list[-1]
    ema_slow_val = ema_slow_list[-1]

    # 추가로 EMA100/200 계산 (유명 트레이더 기준선)
    ema_100_list = ema(closes, 100)
    ema_200_list = ema(closes, 200)
    ema_100_val = ema_100_list[-1] if ema_100_list else math.nan
    ema_200_val = ema_200_list[-1] if ema_200_list else math.nan

    # 유효한 이전 EMA 값을 하나 더 찾아 기울기(최근 기울기)를 계산한다.
    def _last_two_valid(vals: Iterable[float]) -> Tuple[Optional[float], Optional[float]]:
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

    # EMA200 대비 종가 위치
    if ema_200_list and not math.isnan(ema_200_val):
        ema_200_dist_pct = (last_close - ema_200_val) / last_close
    else:
        ema_200_dist_pct = math.nan

    # ATR 및 가격 밴드 폭
    atr_val = calc_atr(candles[-(atr_len + 1) :], length=atr_len)
    atr_pct = (atr_val / last_close) if (atr_val is not None and last_close > 0) else math.nan

    high_recent = max(highs[-range_window:])
    low_recent = min(lows[-range_window:])
    range_pct = (high_recent - low_recent) / last_close if last_close > 0 else math.nan

    # RSI 마지막 값
    rsi_vals = rsi(closes, length=rsi_len)
    rsi_last = rsi_vals[-1] if rsi_vals else math.nan

    # MACD / 시그널 / 히스토그램
    macd_line, macd_signal, macd_hist = macd(closes)
    macd_last = macd_line[-1] if macd_line else math.nan
    macd_signal_last = macd_signal[-1] if macd_signal else math.nan
    macd_hist_last = macd_hist[-1] if macd_hist else math.nan

    # 볼린저 밴드 폭/위치
    bb_mid, bb_upper, bb_lower = bollinger_bands(closes)
    bb_width_pct = math.nan
    bb_pos = math.nan
    if bb_upper and bb_lower:
        u = bb_upper[-1]
        l = bb_lower[-1]
        if not math.isnan(u) and not math.isnan(l) and (u - l) > 0:
            bb_width_pct = (u - l) / last_close
            bb_pos = (last_close - l) / (u - l)

    # 스토캐스틱 (14, 3 기본)
    stoch_k_vals, stoch_d_vals = stochastic_oscillator(highs, lows, closes)
    stoch_k_last = stoch_k_vals[-1] if stoch_k_vals else math.nan
    stoch_d_last = stoch_d_vals[-1] if stoch_d_vals else math.nan

    # ADX
    adx_val = adx(candles, length=atr_len)

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

        # 매우 단순한 휴리스틱 레이블링 (+ ADX 반영)
        strong_trend = (
            abs(ema_dist_pct) >= 0.003
            and atr_pct >= 0.003
            and (trend_strength >= 1.2 or (adx_val is not None and adx_val >= 25))
        )
        strong_range = (
            range_pct <= 0.004
            and abs(ema_dist_pct) <= 0.002
            and (adx_val is None or adx_val < 20)
        )

        if strong_trend:
            regime_hint = "TREND_LIKE"
        elif strong_range:
            regime_hint = "RANGE_LIKE"
        else:
            regime_hint = "MIXED"

    features: Dict[str, float] = {
        "last_close": float(last_close),
        "ema_fast": float(ema_fast_val),
        "ema_slow": float(ema_slow_val),
        "ema_dist_pct": float(ema_dist_pct),
        "ema_fast_slope_pct": float(ema_fast_slope_pct),
        "ema_100": float(ema_100_val),
        "ema_200": float(ema_200_val),
        "ema_200_dist_pct": float(ema_200_dist_pct),
        "atr": float(atr_val) if atr_val is not None else math.nan,
        "atr_pct": float(atr_pct),
        "range_pct": float(range_pct),
        "rsi_last": float(rsi_last),
        "adx_last": float(adx_val) if adx_val is not None else math.nan,
        "bb_width_pct": float(bb_width_pct),
        "bb_pos": float(bb_pos),
        "stoch_k": float(stoch_k_last),
        "stoch_d": float(stoch_d_last),
        "macd": float(macd_last),
        "macd_signal": float(macd_signal_last),
        "macd_hist": float(macd_hist_last),
        "trend_strength": float(trend_strength),
        "range_strength": float(range_strength),
    }

    # regime_hint 를 숫자값으로 같이 싣는다 (GPT / 통계처리 편의용)
    features["_regime_hint"] = {
        "TREND_LIKE": 1.0,
        "RANGE_LIKE": 0.0,
        "MIXED": 0.5,
        "UNKNOWN": math.nan,
    }.get(regime_hint, math.nan)

    # 문자열 레이블을 -1/0/1 로 인코딩
    features["_regime_label"] = {
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


# ─────────────────────────────
# 스토캐스틱 (보조지표로도 직접 사용 가능)
# ─────────────────────────────

def stochastic_oscillator(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    k_len: int = 14,
    d_len: int = 3,
) -> Tuple[List[float], List[float]]:
    """스토캐스틱 오실레이터 (%K, %D) 계산."""
    n = min(len(highs), len(lows), len(closes))
    if n == 0:
        return [], []

    highs = highs[:n]
    lows = lows[:n]
    closes = closes[:n]

    k_vals: List[float] = [math.nan] * n
    for i in range(n):
        if i + 1 < k_len:
            continue
        start = i + 1 - k_len
        window_high = max(highs[start : i + 1])
        window_low = min(lows[start : i + 1])
        denom = window_high - window_low
        if denom <= 0:
            k_vals[i] = 50.0  # 변동성 거의 없으면 중립값
        else:
            k_vals[i] = 100.0 * (closes[i] - window_low) / denom

    # %D = %K 의 이동평균 (NaN 은 제외한 윈도우 평균)
    d_vals: List[float] = [math.nan] * n
    for i in range(n):
        if i + 1 < d_len:
            continue
        window = k_vals[i + 1 - d_len : i + 1]
        valid = [v for v in window if not math.isnan(v)]
        if len(valid) < d_len:
            continue
        d_vals[i] = sum(valid) / len(valid)

    return k_vals, d_vals


__all__ = [
    "sma",
    "ema",
    "rsi",
    "calc_atr",
    "macd",
    "bollinger_bands",
    "obv",
    "adx",
    "stochastic_oscillator",
    "has_bearish_rsi_divergence",
    "has_bullish_rsi_divergence",
    "build_regime_features_from_candles",
    "_find_last_two_pivot_highs",
    "_find_last_two_pivot_lows",
    "Candle",
]
