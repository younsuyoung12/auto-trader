from __future__ import annotations

"""indicators.py
====================================================
기술적 지표 및 레짐 피처 계산 모듈 (TA-Lib 백엔드 버전).

역할
----------------------------------------------------
- WebSocket / DB 에서 얻은 캔들(1m/5m/15m ...)에 대해
  EMA, RSI, ATR, MACD, 스토캐스틱, 볼린저 밴드, ADX, OBV 등을 계산한다.
- 이 모듈은 **순수 수치 계산 전용 레이어**로, I/O 나 텔레그램 로깅을 전혀 수행하지 않는다.
- GPT 레이어는 이 모듈이 돌려주는 지표 값만 사용하며, 지표 계산을 직접 수행하지 않는다.

2025-12-01 변경 요약 (TA-Lib 마이그레이션 및 레짐 휴리스틱 파라미터화)
----------------------------------------------------
1) EMA / SMA / RSI / ATR / MACD / Bollinger Bands / OBV / ADX / Stochastic
   계산을 모두 TA-Lib 기반으로 교체했다.
   - talib 이 import 되지 않으면 RuntimeError 를 바로 발생시켜
     잘못된 환경에서 지표를 "대충" 계산하는 일을 원천 차단한다.
2) build_regime_features_from_candles(...) 는 TA-Lib 결과를 사용하도록 변경했지만,
   기존 시그니처와 반환 키/형식은 완전히 동일하게 유지해 상위 모듈과 100% 호환된다.
3) 과거 순수 파이썬 지표 구현 코드는 모두 제거하고,
   이 모듈을 "지표 계산 전용 · 무 사이드이펙트" 레이어로 단순화했다.
4) build_regime_features_from_candles(...) 내부 레짐 판별용 휴리스틱 임계값
   (EMA 괴리도, ATR %, Range 폭, ADX 기준치 등)을 RegimeThresholds 데이터클래스로
   파라미터화해, 자산/타임프레임별 튜닝이 가능하도록 했다.
"""

import math
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Iterable

import numpy as np

# TA-Lib 로 백엔드를 통일한다.
try:  # pragma: no cover - 환경별 talib 설치 유무 차이 방어
    import talib  # type: ignore[import]
except Exception as e:  # pragma: no cover
    talib = None  # type: ignore[assignment]
    _talib_import_error: Optional[Exception] = e
else:
    _talib_import_error = None


def _require_talib() -> None:
    """TA-Lib 이 필수인 함수 호출 전에 환경을 강제 체크한다."""
    if talib is None:
        raise RuntimeError(
            "TA-Lib(talib) import 에 실패했습니다.\n"
            "indicators.py 는 TA-Lib 기반으로 리팩터링된 상태이며, "
            "지표를 정확하게 계산하려면 TA-Lib 이 반드시 설치되어 있어야 합니다.\n"
            "· pip install ta-lib (및 시스템 라이브러리 설치)를 확인해 주세요."
        ) from _talib_import_error


def _to_np(values: List[float]) -> "np.ndarray":
    """Python 리스트를 float64 numpy 배열로 변환 (지표 계산용)."""
    if not values:
        return np.empty(0, dtype="float64")
    return np.asarray(values, dtype="float64")


# WebSocket / DB 공용 캔들 타입
# ts_ms: epoch milliseconds (int)
# open, high, low, close: float
Candle = Tuple[int, float, float, float, float]  # (ts_ms, open, high, low, close)


# 레짐 판별 휴리스틱 임계값 묶음
@dataclass(frozen=True)
class RegimeThresholds:
    """build_regime_features_from_candles 에서 사용하는 레짐 판별용 임계값 모음.

    - 기본값은 BTC/고변동성 코인 1m~15m 기준으로 튜닝한 휴리스틱에 해당한다.
    - 자산/타임프레임별로 다른 기준이 필요할 때 인스턴스를 만들어 인자로 넘긴다.
    """
    # 추세 레짐 판정
    trend_ema_dist_min: float = 0.003      # EMA 괴리도 최소 비율
    trend_atr_pct_min: float = 0.003      # ATR / price 최소 비율
    trend_strength_min: float = 1.2       # |ema_dist_pct| / atr_pct 최소 비율
    trend_adx_min: float = 25.0           # ADX 최소 기준값

    # 박스(횡보) 레짐 판정
    range_width_max: float = 0.004        # (최근 고가-저가) / price 최대 비율
    range_ema_dist_max: float = 0.002     # EMA 괴리도 최대 비율
    range_adx_max: float = 20.0           # ADX 최대 기준값


# ─────────────────────────────
# SMA (단순 이동평균) - TA-Lib
# ─────────────────────────────

def sma(values: List[float], length: int) -> List[float]:
    """단순 이동평균(SMA) 계산 (TA-Lib 백엔드).

    - 반환 리스트 길이는 입력 values 와 동일하다.
    - length 이전 구간은 NaN 으로 채워 인덱스 정합을 유지한다.
    - length <= 0 인 경우, 방어적으로 원본 값을 그대로 복사해 반환한다.
    """
    n = len(values)
    if n == 0:
        return []
    if length <= 0:
        return list(values)

    _require_talib()
    arr = _to_np(values)
    out = talib.SMA(arr, timeperiod=length)
    return out.tolist()


# ─────────────────────────────
# EMA - TA-Lib
# ─────────────────────────────

def ema(values: List[float], length: int) -> List[float]:
    """EMA(지수이동평균) 계산 (TA-Lib 백엔드).

    - 반환 값 길이는 입력과 동일하며, 초기 구간은 NaN 으로 채워진다.
    - length <= 0 인 경우, 방어적으로 원본 값을 그대로 복사해 반환한다.
    """
    n = len(values)
    if n == 0:
        return []
    if length <= 0:
        return list(values)

    _require_talib()
    arr = _to_np(values)
    out = talib.EMA(arr, timeperiod=length)
    return out.tolist()


# ─────────────────────────────
# RSI - TA-Lib
# ─────────────────────────────

def rsi(closes: List[float], length: int = 14) -> List[float]:
    """표준 RSI 계산 (TA-Lib 백엔드).

    - 입력: 종가 리스트(closes)
    - 반환: 원본 길이에 맞춘 RSI 리스트
    - length <= 0 인 경우 전체 NaN 리스트 반환.
    """
    n = len(closes)
    if n == 0:
        return []
    if length <= 0:
        return [math.nan] * n

    _require_talib()
    arr = _to_np(closes)
    out = talib.RSI(arr, timeperiod=length)
    return out.tolist()


# ─────────────────────────────
# ATR - TA-Lib
# ─────────────────────────────

def calc_atr(candles: List[Candle], length: int = 14) -> Optional[float]:
    """기본 ATR 계산 (TA-Lib 백엔드).

    candles 는 (ts_ms, open, high, low, close) 튜플 리스트여야 한다.
    length+1 개 미만이면 None 을 리턴한다.

    반환값은 "마지막 구간" 기준 ATR 1개(float) 이다.
    """
    if length <= 0:
        return None
    if len(candles) < length + 1:
        return None

    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    closes = [c[4] for c in candles]

    _require_talib()
    h_arr = _to_np(highs)
    l_arr = _to_np(lows)
    c_arr = _to_np(closes)
    atr_series = talib.ATR(h_arr, l_arr, c_arr, timeperiod=length)
    if atr_series.size == 0 or math.isnan(float(atr_series[-1])):
        return None
    return float(atr_series[-1])


# ─────────────────────────────
# MACD - TA-Lib
# ─────────────────────────────

def macd(
    values: List[float],
    fast_len: int = 12,
    slow_len: int = 26,
    signal_len: int = 9,
) -> Tuple[List[float], List[float], List[float]]:
    """MACD(12·26·9 기본값) 계산 (TA-Lib 백엔드).

    반환:
        macd_line, signal_line, hist 리스트 (길이는 values 와 동일)
    TA-Lib 의 결과를 그대로 사용하므로, 선두 구간은 NaN 으로 채워진다.
    """
    n = len(values)
    if n == 0:
        return [], [], []

    _require_talib()
    arr = _to_np(values)
    macd_line, signal_line, hist = talib.MACD(
        arr,
        fastperiod=fast_len,
        slowperiod=slow_len,
        signalperiod=signal_len,
    )
    return macd_line.tolist(), signal_line.tolist(), hist.tolist()


# ─────────────────────────────
# 볼린저 밴드 - TA-Lib
# ─────────────────────────────

def bollinger_bands(
    values: List[float],
    length: int = 20,
    num_std: float = 2.0,
) -> Tuple[List[float], List[float], List[float]]:
    """볼린저 밴드 (중심선, 상단, 하단) 계산 (TA-Lib 백엔드).

    - 길이가 length 미만인 구간은 NaN.
    """
    n = len(values)
    if n == 0:
        return [], [], []
    if length <= 0:
        return [math.nan] * n, [math.nan] * n, [math.nan] * n

    _require_talib()
    arr = _to_np(values)
    upper, mid, lower = talib.BBANDS(
        arr,
        timeperiod=length,
        nbdevup=num_std,
        nbdevdn=num_std,
        matype=0,
    )
    return mid.tolist(), upper.tolist(), lower.tolist()


# ─────────────────────────────
# OBV (On-Balance Volume) - TA-Lib
# ─────────────────────────────

def obv(closes: List[float], volumes: List[float]) -> List[float]:
    """OBV 계산 (TA-Lib 백엔드). 길이가 맞지 않으면 공통 구간까지만 사용한다."""
    n = min(len(closes), len(volumes))
    if n == 0:
        return []

    _require_talib()
    c_arr = _to_np(closes[:n])
    v_arr = _to_np(volumes[:n])
    out = talib.OBV(c_arr, v_arr)
    return out.tolist()


# ─────────────────────────────
# ADX - TA-Lib
# ─────────────────────────────

def adx(candles: List[Candle], length: int = 14) -> Optional[float]:
    """ADX(평균 방향성 지수) 마지막 값 1개 계산 (TA-Lib 백엔드).

    - candles 가 2 * length 개 미만이면 None.
    - TA-Lib ADX 의 마지막 유효 값을 그대로 사용한다.
    """
    n = len(candles)
    if length <= 0 or n < 2 * length:
        return None

    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    closes = [c[4] for c in candles]

    _require_talib()
    h_arr = _to_np(highs)
    l_arr = _to_np(lows)
    c_arr = _to_np(closes)
    adx_series = talib.ADX(h_arr, l_arr, c_arr, timeperiod=length)
    if adx_series.size == 0 or math.isnan(float(adx_series[-1])):
        return None
    return float(adx_series[-1])


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
    regime_thresholds: Optional["RegimeThresholds"] = None,
) -> Optional[Dict[str, float]]:
    """캔들 배열에서 추세/변동성/박스 여부를 가늠하는 피처 묶음을 생성한다.

    반환 예시(dict):
        {
          "last_close": 94500.0,
          "ema_fast": 94480.0,
          "ema_slow": 94300.0,
          "ema_dist_pct": 0.0019,
          "ema_fast_slope_pct": 0.0003,
          "ema_100": ...,
          "ema_200": ...,
          "atr": 350.0,
          "atr_pct": 0.0037,
          "range_pct": 0.0052,
          "rsi_last": 57.3,
          "adx_last": 27.1,
          "bb_width_pct": 0.004,
          "bb_pos": 0.6,
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
    - position_watch_ws, market_features_ws 등에서 extra["regime_features"] 용으로 사용된다.
    - regime_thresholds 를 넘기면 레짐 판별 기준(EMA 괴리도, ATR %, ADX 등)을
      자산/타임프레임별로 커스터마이즈할 수 있다.
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

    # 레짐 휴리스틱 임계값 (None 이면 기본값 사용)
    if regime_thresholds is None:
        regime_thresholds = RegimeThresholds()

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
    if not math.isnan(ema_200_val):
        ema_200_dist_pct = (last_close - ema_200_val) / last_close
    else:
        ema_200_dist_pct = math.nan

    # ATR 및 가격 밴드 폭
    atr_val = calc_atr(candles[-(atr_len + 1):], length=atr_len)
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
        t = regime_thresholds
        strong_trend = (
            abs(ema_dist_pct) >= t.trend_ema_dist_min
            and atr_pct >= t.trend_atr_pct_min
            and (
                trend_strength >= t.trend_strength_min
                or (adx_val is not None and adx_val >= t.trend_adx_min)
            )
        )
        strong_range = (
            range_pct <= t.range_width_max
            and abs(ema_dist_pct) <= t.range_ema_dist_max
            and (adx_val is None or adx_val < t.range_adx_max)
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
# 다이버전스 보조 피벗 찾기 (순수 파이썬 유지)
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
# RSI 다이버전스 (순수 파이썬 유지)
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
# 스토캐스틱 (Stochastic Oscillator) - TA-Lib
# ─────────────────────────────

def stochastic_oscillator(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    k_len: int = 14,
    d_len: int = 3,
) -> Tuple[List[float], List[float]]:
    """스토캐스틱 오실레이터 (%K, %D) 계산 (TA-Lib STOCH 기반).

    - fastK 기간은 k_len, slowK 기간은 3, slowD 기간은 d_len 으로 설정한다.
    - 반환값은 (slowK, slowD) ≒ (표준 %K, %D) 로 사용한다.
    """
    n = min(len(highs), len(lows), len(closes))
    if n == 0:
        return [], []

    _require_talib()
    h_arr = _to_np(highs[:n])
    l_arr = _to_np(lows[:n])
    c_arr = _to_np(closes[:n])

    slowk, slowd = talib.STOCH(
        h_arr,
        l_arr,
        c_arr,
        fastk_period=k_len,
        slowk_period=3,
        slowk_matype=0,
        slowd_period=d_len,
        slowd_matype=0,
    )
    return slowk.tolist(), slowd.tolist()


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
    "RegimeThresholds",
]
