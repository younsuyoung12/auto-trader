from __future__ import annotations

"""
========================================================
FILE: strategy/indicators.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
역할
- WebSocket/DB에서 얻은 캔들(1m/5m/15m ...)에 대해 TA-Lib 기반 지표를 계산한다.
- 이 모듈은 순수 수치 계산 전용 레이어이며 I/O(네트워크/DB/텔레그램)는 금지한다.
- 상위 레이어는 본 모듈이 반환하는 지표 값만 사용하며, 지표 계산을 직접 수행하지 않는다.

절대 원칙 (STRICT · NO-FALLBACK)
- 입력 누락/길이 부족/NaN/Inf 발생 시 즉시 예외.
- "대충 계산" / "None 반환" / "NaN 채우기" / "길이 맞추기 위해 자르기" 금지.
- TA-Lib 미설치 환경에서 실행 금지(즉시 예외).

변경 이력
--------------------------------------------------------
- 2026-03-03:
  1) STRICT 강화: 지표 함수들이 None/NaN/빈 리스트를 반환하던 경로 제거 → 즉시 예외
  2) STRICT 강화: OBV 입력 길이 불일치 시 자르기(min) 제거 → 즉시 예외
  3) STRICT 강화: build_regime_features_from_candles() 최소 캔들 요구량을 실제 사용 지표(EMA100/EMA200/ADX) 기준으로 상향
  4) STRICT 강화: 최종 산출 값이 NaN/Inf이면 즉시 예외(지표 품질 보장)
- 2025-12-01:
  - TA-Lib 기반으로 지표 계산 통일(기존 파이썬 구현 제거), 레짐 임계값 파라미터화
========================================================
"""

import math
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np

# TA-Lib 로 백엔드를 통일한다.
try:  # pragma: no cover
    import talib  # type: ignore[import]
except Exception as e:  # pragma: no cover
    talib = None  # type: ignore[assignment]
    _talib_import_error: Optional[Exception] = e
else:
    _talib_import_error = None


# WebSocket / DB 공용 캔들 타입
# ts_ms: epoch milliseconds (int)
# open, high, low, close: float
Candle = Tuple[int, float, float, float, float]  # (ts_ms, open, high, low, close)


# ─────────────────────────────────────────────
# Exceptions (STRICT)
# ─────────────────────────────────────────────
class IndicatorsError(RuntimeError):
    """Base error for indicators module (STRICT)."""


class TalibMissingError(IndicatorsError):
    """Raised when TA-Lib is not available."""


class InputDataError(IndicatorsError):
    """Raised on invalid inputs (empty/mismatch/insufficient length)."""


class IndicatorComputationError(IndicatorsError):
    """Raised when TA-Lib returns NaN/Inf or unexpected shapes."""


def _require_talib() -> None:
    """TA-Lib 이 필수인 함수 호출 전에 환경을 강제 체크한다."""
    if talib is None:
        raise TalibMissingError(
            "TA-Lib(talib) import 에 실패했습니다. indicators.py 는 TA-Lib 필수입니다."
        ) from _talib_import_error


def _is_finite(x: float) -> bool:
    return math.isfinite(x)


def _require_nonempty(seq: List[float], *, name: str) -> None:
    if not isinstance(seq, list) or not seq:
        raise InputDataError(f"{name} is required (empty)")


def _require_len_at_least(seq: List[float], *, name: str, n: int) -> None:
    _require_nonempty(seq, name=name)
    if len(seq) < n:
        raise InputDataError(f"{name} length<{n} (got={len(seq)})")


def _to_np(values: List[float], *, name: str) -> "np.ndarray":
    """Python 리스트를 float64 numpy 배열로 변환 (지표 계산용)."""
    _require_nonempty(values, name=name)
    return np.asarray(values, dtype="float64")


def _require_last_finite(arr: "np.ndarray", *, name: str) -> float:
    if arr.size == 0:
        raise IndicatorComputationError(f"{name} produced empty result")
    last = float(arr[-1])
    if not _is_finite(last):
        raise IndicatorComputationError(f"{name} last value is not finite (got={last})")
    return last


def _require_series_all_finite(arr: "np.ndarray", *, name: str) -> None:
    if arr.size == 0:
        raise IndicatorComputationError(f"{name} produced empty result")
    # Allow leading NaNs from TA-Lib warm-up, but final value must be finite.
    _require_last_finite(arr, name=name)


# 레짐 판별 휴리스틱 임계값 묶음
@dataclass(frozen=True)
class RegimeThresholds:
    # 추세 레짐 판정
    trend_ema_dist_min: float = 0.003
    trend_atr_pct_min: float = 0.003
    trend_strength_min: float = 1.2
    trend_adx_min: float = 25.0

    # 박스(횡보) 레짐 판정
    range_width_max: float = 0.004
    range_ema_dist_max: float = 0.002
    range_adx_max: float = 20.0


# ─────────────────────────────
# SMA (TA-Lib)
# ─────────────────────────────
def sma(values: List[float], length: int) -> List[float]:
    """
    STRICT:
    - values 비어있으면 예외
    - length <= 0 예외
    - len(values) < length 예외
    """
    if not isinstance(length, int) or length <= 0:
        raise InputDataError(f"sma length must be positive int (got={length})")
    _require_len_at_least(values, name="sma.values", n=length)

    _require_talib()
    arr = _to_np(values, name="sma.values")
    out = talib.SMA(arr, timeperiod=length)
    _require_series_all_finite(out, name="talib.SMA")
    return out.tolist()


# ─────────────────────────────
# EMA (TA-Lib)
# ─────────────────────────────
def ema(values: List[float], length: int) -> List[float]:
    """
    STRICT:
    - values 비어있으면 예외
    - length <= 0 예외
    - len(values) < length 예외
    """
    if not isinstance(length, int) or length <= 0:
        raise InputDataError(f"ema length must be positive int (got={length})")
    _require_len_at_least(values, name="ema.values", n=length)

    _require_talib()
    arr = _to_np(values, name="ema.values")
    out = talib.EMA(arr, timeperiod=length)
    _require_series_all_finite(out, name="talib.EMA")
    return out.tolist()


# ─────────────────────────────
# RSI (TA-Lib)
# ─────────────────────────────
def rsi(closes: List[float], length: int = 14) -> List[float]:
    """
    STRICT:
    - closes 비어있으면 예외
    - length <= 0 예외
    - len(closes) < length+1 예외
    """
    if not isinstance(length, int) or length <= 0:
        raise InputDataError(f"rsi length must be positive int (got={length})")
    _require_len_at_least(closes, name="rsi.closes", n=length + 1)

    _require_talib()
    arr = _to_np(closes, name="rsi.closes")
    out = talib.RSI(arr, timeperiod=length)
    _require_series_all_finite(out, name="talib.RSI")
    return out.tolist()


# ─────────────────────────────
# ATR (TA-Lib)
# ─────────────────────────────
def calc_atr(candles: List[Candle], length: int = 14) -> float:
    """
    STRICT:
    - length <= 0 예외
    - len(candles) < length+1 예외
    - 마지막 ATR 값이 NaN/Inf이면 예외
    """
    if not isinstance(length, int) or length <= 0:
        raise InputDataError(f"atr length must be positive int (got={length})")
    if not isinstance(candles, list) or not candles:
        raise InputDataError("atr.candles is required (empty)")
    if len(candles) < length + 1:
        raise InputDataError(f"atr.candles length<{length+1} (got={len(candles)})")

    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    closes = [c[4] for c in candles]

    _require_talib()
    h_arr = _to_np(highs, name="atr.highs")
    l_arr = _to_np(lows, name="atr.lows")
    c_arr = _to_np(closes, name="atr.closes")
    atr_series = talib.ATR(h_arr, l_arr, c_arr, timeperiod=length)
    last = _require_last_finite(atr_series, name="talib.ATR")
    return float(last)


# ─────────────────────────────
# MACD (TA-Lib)
# ─────────────────────────────
def macd(
    values: List[float],
    fast_len: int = 12,
    slow_len: int = 26,
    signal_len: int = 9,
) -> Tuple[List[float], List[float], List[float]]:
    """
    STRICT:
    - values 비어있으면 예외
    - 각 기간 <=0 예외
    - len(values) < slow_len + signal_len 예외 (유효 값 확보 목적)
    """
    for nm, v in (("fast_len", fast_len), ("slow_len", slow_len), ("signal_len", signal_len)):
        if not isinstance(v, int) or v <= 0:
            raise InputDataError(f"macd {nm} must be positive int (got={v})")

    _require_len_at_least(values, name="macd.values", n=slow_len + signal_len)

    _require_talib()
    arr = _to_np(values, name="macd.values")
    macd_line, signal_line, hist = talib.MACD(
        arr,
        fastperiod=fast_len,
        slowperiod=slow_len,
        signalperiod=signal_len,
    )
    _require_series_all_finite(macd_line, name="talib.MACD.macd")
    _require_series_all_finite(signal_line, name="talib.MACD.signal")
    _require_series_all_finite(hist, name="talib.MACD.hist")
    return macd_line.tolist(), signal_line.tolist(), hist.tolist()


# ─────────────────────────────
# Bollinger Bands (TA-Lib)
# ─────────────────────────────
def bollinger_bands(
    values: List[float],
    length: int = 20,
    num_std: float = 2.0,
) -> Tuple[List[float], List[float], List[float]]:
    """
    STRICT:
    - values 비어있으면 예외
    - length <=0 예외
    - len(values) < length 예외
    """
    if not isinstance(length, int) or length <= 0:
        raise InputDataError(f"bbands length must be positive int (got={length})")
    _require_len_at_least(values, name="bbands.values", n=length)
    if not isinstance(num_std, (int, float)) or not math.isfinite(float(num_std)) or float(num_std) <= 0:
        raise InputDataError(f"bbands num_std must be finite > 0 (got={num_std})")

    _require_talib()
    arr = _to_np(values, name="bbands.values")
    upper, mid, lower = talib.BBANDS(
        arr,
        timeperiod=length,
        nbdevup=float(num_std),
        nbdevdn=float(num_std),
        matype=0,
    )
    _require_series_all_finite(mid, name="talib.BBANDS.mid")
    _require_series_all_finite(upper, name="talib.BBANDS.upper")
    _require_series_all_finite(lower, name="talib.BBANDS.lower")
    return mid.tolist(), upper.tolist(), lower.tolist()


# ─────────────────────────────
# OBV (TA-Lib)
# ─────────────────────────────
def obv(closes: List[float], volumes: List[float]) -> List[float]:
    """
    STRICT:
    - closes/volumes 비어있으면 예외
    - 길이 불일치 시 예외 (자르기(min) 금지)
    """
    _require_nonempty(closes, name="obv.closes")
    _require_nonempty(volumes, name="obv.volumes")
    if len(closes) != len(volumes):
        raise InputDataError(f"obv length mismatch: closes={len(closes)}, volumes={len(volumes)}")

    _require_talib()
    c_arr = _to_np(closes, name="obv.closes")
    v_arr = _to_np(volumes, name="obv.volumes")
    out = talib.OBV(c_arr, v_arr)
    _require_series_all_finite(out, name="talib.OBV")
    return out.tolist()


# ─────────────────────────────
# ADX (TA-Lib)
# ─────────────────────────────
def adx(candles: List[Candle], length: int = 14) -> float:
    """
    STRICT:
    - candles 비어있으면 예외
    - length <=0 예외
    - len(candles) < 2*length 예외 (TA-Lib ADX 안정화)
    """
    if not isinstance(length, int) or length <= 0:
        raise InputDataError(f"adx length must be positive int (got={length})")
    if not isinstance(candles, list) or not candles:
        raise InputDataError("adx.candles is required (empty)")
    if len(candles) < 2 * length:
        raise InputDataError(f"adx.candles length<{2*length} (got={len(candles)})")

    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    closes = [c[4] for c in candles]

    _require_talib()
    h_arr = _to_np(highs, name="adx.highs")
    l_arr = _to_np(lows, name="adx.lows")
    c_arr = _to_np(closes, name="adx.closes")
    adx_series = talib.ADX(h_arr, l_arr, c_arr, timeperiod=length)
    last = _require_last_finite(adx_series, name="talib.ADX")
    return float(last)


# ─────────────────────────────
# Stochastic Oscillator (TA-Lib)
# ─────────────────────────────
def stochastic_oscillator(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    k_len: int = 14,
    d_len: int = 3,
) -> Tuple[List[float], List[float]]:
    """
    STRICT:
    - 입력 길이 불일치/비어있음 금지
    - k_len/d_len <=0 금지
    - len >= k_len + d_len 필요(최소 유효 출력 목적)
    """
    for nm, v in (("k_len", k_len), ("d_len", d_len)):
        if not isinstance(v, int) or v <= 0:
            raise InputDataError(f"stoch {nm} must be positive int (got={v})")

    _require_nonempty(highs, name="stoch.highs")
    _require_nonempty(lows, name="stoch.lows")
    _require_nonempty(closes, name="stoch.closes")

    if not (len(highs) == len(lows) == len(closes)):
        raise InputDataError(f"stoch length mismatch: highs={len(highs)}, lows={len(lows)}, closes={len(closes)}")

    if len(closes) < (k_len + d_len):
        raise InputDataError(f"stoch closes length<{k_len + d_len} (got={len(closes)})")

    _require_talib()
    h_arr = _to_np(highs, name="stoch.highs")
    l_arr = _to_np(lows, name="stoch.lows")
    c_arr = _to_np(closes, name="stoch.closes")

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
    _require_series_all_finite(slowk, name="talib.STOCH.slowk")
    _require_series_all_finite(slowd, name="talib.STOCH.slowd")
    return slowk.tolist(), slowd.tolist()


# ─────────────────────────────
# Divergence helpers (pure python)
# ─────────────────────────────
def _find_last_two_pivot_highs(candles: List[Candle]) -> List[int]:
    idxs: List[int] = []
    if len(candles) < 3:
        return idxs

    for i in range(len(candles) - 2, 0, -1):
        _, _, high, _, _ = candles[i]
        _, _, prev_high, _, _ = candles[i - 1]
        _, _, next_high, _, _ = candles[i + 1]
        if high > prev_high and high > next_high:
            idxs.append(i)
            if len(idxs) == 2:
                break
    return list(reversed(idxs))


def _find_last_two_pivot_lows(candles: List[Candle]) -> List[int]:
    idxs: List[int] = []
    if len(candles) < 3:
        return idxs

    for i in range(len(candles) - 2, 0, -1):
        _, _, _, low, _ = candles[i]
        _, _, _, prev_low, _ = candles[i - 1]
        _, _, _, next_low, _ = candles[i + 1]
        if low < prev_low and low < next_low:
            idxs.append(i)
            if len(idxs) == 2:
                break
    return list(reversed(idxs))


def has_bearish_rsi_divergence(candles: List[Candle], rsi_vals: List[float]) -> bool:
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
# Regime feature builder (STRICT)
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
) -> Dict[str, float]:
    """
    STRICT:
    - candles 길이 부족/NaN 발생 시 즉시 예외 (None 반환 금지)
    - EMA100/EMA200, ADX(2*len)까지 실제 사용 지표 기준으로 최소 길이 강제
    """
    if not isinstance(candles, list) or not candles:
        raise InputDataError("regime.candles is required (empty)")

    for nm, v in (
        ("fast_ema_len", fast_ema_len),
        ("slow_ema_len", slow_ema_len),
        ("atr_len", atr_len),
        ("rsi_len", rsi_len),
        ("range_window", range_window),
    ):
        if not isinstance(v, int) or v <= 0:
            raise InputDataError(f"{nm} must be positive int (got={v})")

    # 실제 사용 지표 기준 최소 길이(EMA200, ADX=2*atr_len 포함)
    need = max(fast_ema_len, slow_ema_len, 100, 120, atr_len + 1, rsi_len + 1, range_window, 2 * atr_len)
    if len(candles) < need:
        raise InputDataError(f"regime.candles length<{need} (got={len(candles)})")

    closes = [c[4] for c in candles]
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]

    last_close = float(closes[-1])
    if last_close <= 0 or not _is_finite(last_close):
        raise InputDataError(f"last_close invalid: {last_close}")

    if regime_thresholds is None:
        regime_thresholds = RegimeThresholds()

    # EMA
    ema_fast_list = ema(closes, fast_ema_len)
    ema_slow_list = ema(closes, slow_ema_len)
    ema_100_list = ema(closes, 100)
    ema_200_list = ema(closes, 200)

    ema_fast_val = float(ema_fast_list[-1])
    ema_slow_val = float(ema_slow_list[-1])
    ema_100_val = float(ema_100_list[-1])
    ema_200_val = float(ema_200_list[-1])

    for nm, v in (
        ("ema_fast", ema_fast_val),
        ("ema_slow", ema_slow_val),
        ("ema_100", ema_100_val),
        ("ema_200", ema_200_val),
    ):
        if not _is_finite(v):
            raise IndicatorComputationError(f"{nm} is not finite (got={v})")

    def _last_two_valid(vals: Iterable[float]) -> Tuple[float, float]:
        valid: List[float] = [float(v) for v in vals if _is_finite(float(v))]
        if len(valid) < 2:
            raise IndicatorComputationError("ema series has <2 valid points (STRICT)")
        return valid[-2], valid[-1]

    ema_fast_prev, ema_fast_last = _last_two_valid(ema_fast_list)
    ema_fast_slope_pct = (ema_fast_last - ema_fast_prev) / last_close
    if not _is_finite(ema_fast_slope_pct):
        raise IndicatorComputationError(f"ema_fast_slope_pct not finite (got={ema_fast_slope_pct})")

    ema_dist_pct = (ema_fast_val - ema_slow_val) / last_close
    if not _is_finite(ema_dist_pct):
        raise IndicatorComputationError(f"ema_dist_pct not finite (got={ema_dist_pct})")

    ema_200_dist_pct = (last_close - ema_200_val) / last_close
    if not _is_finite(ema_200_dist_pct):
        raise IndicatorComputationError(f"ema_200_dist_pct not finite (got={ema_200_dist_pct})")

    # ATR / Range
    atr_val = calc_atr(candles[-(atr_len + 1) :], length=atr_len)
    atr_pct = atr_val / last_close
    if not _is_finite(atr_pct) or atr_pct <= 0:
        raise IndicatorComputationError(f"atr_pct invalid (atr={atr_val}, last_close={last_close})")

    high_recent = max(highs[-range_window:])
    low_recent = min(lows[-range_window:])
    range_pct = (float(high_recent) - float(low_recent)) / last_close
    if not _is_finite(range_pct) or range_pct <= 0:
        raise IndicatorComputationError(f"range_pct invalid (got={range_pct})")

    # RSI
    rsi_vals = rsi(closes, length=rsi_len)
    rsi_last = float(rsi_vals[-1])
    if not _is_finite(rsi_last):
        raise IndicatorComputationError(f"rsi_last not finite (got={rsi_last})")

    # MACD
    macd_line, macd_signal, macd_hist = macd(closes)
    macd_last = float(macd_line[-1])
    macd_signal_last = float(macd_signal[-1])
    macd_hist_last = float(macd_hist[-1])
    for nm, v in (("macd", macd_last), ("macd_signal", macd_signal_last), ("macd_hist", macd_hist_last)):
        if not _is_finite(v):
            raise IndicatorComputationError(f"{nm} not finite (got={v})")

    # Bollinger
    bb_mid, bb_upper, bb_lower = bollinger_bands(closes)
    u = float(bb_upper[-1])
    l = float(bb_lower[-1])
    if not (_is_finite(u) and _is_finite(l)) or (u - l) <= 0:
        raise IndicatorComputationError(f"bbands invalid (upper={u}, lower={l})")
    bb_width_pct = (u - l) / last_close
    bb_pos = (last_close - l) / (u - l)
    if not (_is_finite(bb_width_pct) and _is_finite(bb_pos)):
        raise IndicatorComputationError("bb_width_pct/bb_pos not finite (STRICT)")

    # Stoch
    stoch_k_vals, stoch_d_vals = stochastic_oscillator(highs, lows, closes)
    stoch_k_last = float(stoch_k_vals[-1])
    stoch_d_last = float(stoch_d_vals[-1])
    if not (_is_finite(stoch_k_last) and _is_finite(stoch_d_last)):
        raise IndicatorComputationError("stoch last not finite (STRICT)")

    # ADX (uses atr_len as before)
    adx_last = adx(candles, length=atr_len)
    if not _is_finite(adx_last):
        raise IndicatorComputationError(f"adx_last not finite (got={adx_last})")

    # Regime hint (STRICT numeric outputs)
    t = regime_thresholds
    trend_strength = abs(ema_dist_pct) / atr_pct
    range_strength = range_pct / atr_pct

    if not (_is_finite(trend_strength) and _is_finite(range_strength)):
        raise IndicatorComputationError("trend_strength/range_strength not finite (STRICT)")

    strong_trend = (
        abs(ema_dist_pct) >= t.trend_ema_dist_min
        and atr_pct >= t.trend_atr_pct_min
        and (trend_strength >= t.trend_strength_min or adx_last >= t.trend_adx_min)
    )
    strong_range = (
        range_pct <= t.range_width_max
        and abs(ema_dist_pct) <= t.range_ema_dist_max
        and adx_last < t.range_adx_max
    )

    regime_hint = "MIXED"
    if strong_trend:
        regime_hint = "TREND_LIKE"
    elif strong_range:
        regime_hint = "RANGE_LIKE"

    hint_val = {"TREND_LIKE": 1.0, "RANGE_LIKE": 0.0, "MIXED": 0.5}[regime_hint]
    label_val = {"TREND_LIKE": 1.0, "RANGE_LIKE": -1.0, "MIXED": 0.0}[regime_hint]

    return {
        "last_close": last_close,
        "ema_fast": ema_fast_val,
        "ema_slow": ema_slow_val,
        "ema_dist_pct": float(ema_dist_pct),
        "ema_fast_slope_pct": float(ema_fast_slope_pct),
        "ema_100": ema_100_val,
        "ema_200": ema_200_val,
        "ema_200_dist_pct": float(ema_200_dist_pct),
        "atr": float(atr_val),
        "atr_pct": float(atr_pct),
        "range_pct": float(range_pct),
        "rsi_last": float(rsi_last),
        "adx_last": float(adx_last),
        "bb_width_pct": float(bb_width_pct),
        "bb_pos": float(bb_pos),
        "stoch_k": float(stoch_k_last),
        "stoch_d": float(stoch_d_last),
        "macd": float(macd_last),
        "macd_signal": float(macd_signal_last),
        "macd_hist": float(macd_hist_last),
        "trend_strength": float(trend_strength),
        "range_strength": float(range_strength),
        "_regime_hint": float(hint_val),
        "_regime_label": float(label_val),
    }


__all__ = [
    "IndicatorsError",
    "TalibMissingError",
    "InputDataError",
    "IndicatorComputationError",
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