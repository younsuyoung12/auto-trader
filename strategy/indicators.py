from __future__ import annotations

"""
========================================================
FILE: strategy/indicators.py
ROLE:
- WebSocket/DB에서 확보한 OHLC 캔들에 대해 TA-Lib 기반 보조지표를 계산한다.
- 순수 수치 계산 레이어만 담당하며 네트워크/DB/텔레그램 등 I/O는 금지한다.
- 상위 레이어는 본 모듈의 명시적 계약(STRICT validation + finite result)을 전제로 사용한다.

CORE RESPONSIBILITIES:
- 캔들/숫자 시리즈 입력 계약을 STRICT 검증한다.
- TA-Lib 기반 지표(SMA/EMA/RSI/ATR/MACD/BBANDS/OBV/ADX/STOCH)를 계산한다.
- 레짐 분류용 numeric feature set을 생성한다.
- divergence helper 및 pivot helper를 제공한다.

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · PRODUCTION / TRADE-GRADE
- 입력 누락/길이 부족/NaN/Inf/타입 불일치/캔들 계약 위반은 즉시 예외 처리한다.
- 캔들 timestamp 역전/중복은 즉시 예외 처리한다.
- TA-Lib 미설치 환경은 즉시 예외 처리한다.
- 인덱스 정렬 보존이 필요한 raw series API는 TA-Lib warm-up 구간의 leading non-finite만 허용한다.
- first-valid 이후 non-finite 재등장 또는 마지막 값 non-finite는 즉시 예외 처리한다.
- 스칼라 출력과 regime feature 출력은 최종 값이 모두 finite여야 한다.

CHANGE HISTORY:
--------------------------------------------------------
- 2026-03-11:
  1) FIX(ROOT-CAUSE): build_regime_features_from_candles() 최소 길이 계산을 EMA200 실제 사용 기준과 일치하도록 수정
  2) ADD(STRICT): candle 구조/가격 논리/timestamp 정렬/floating finite 검증 추가
  3) ADD(STRICT): TA-Lib 출력 시리즈 계약 정리(leading warm-up non-finite만 허용, first-valid 이후 non-finite 금지)
  4) FIX(STRICT): divergence helper 길이 불일치/비정상 RSI 시리즈를 False로 숨기지 않고 즉시 예외 처리
========================================================
"""

import math
from dataclasses import dataclass
from numbers import Real
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np

try:  # pragma: no cover
    import talib  # type: ignore[import]
except Exception as e:  # pragma: no cover
    talib = None  # type: ignore[assignment]
    _talib_import_error: Optional[Exception] = e
else:
    _talib_import_error = None


Candle = Tuple[int, float, float, float, float]  # (ts_ms, open, high, low, close)


# ─────────────────────────────────────────────
# Exceptions (STRICT)
# ─────────────────────────────────────────────
class IndicatorsError(RuntimeError):
    """Base error for indicators module (STRICT)."""


class TalibMissingError(IndicatorsError):
    """Raised when TA-Lib is not available."""


class InputDataError(IndicatorsError):
    """Raised on invalid inputs (empty/mismatch/insufficient length/invalid schema)."""


class IndicatorComputationError(IndicatorsError):
    """Raised when TA-Lib output or derived indicator state is invalid."""


def _require_talib() -> None:
    if talib is None:
        raise TalibMissingError(
            "TA-Lib(talib) import 에 실패했습니다. strategy/indicators.py 는 TA-Lib 필수입니다."
        ) from _talib_import_error


def _is_finite(x: float) -> bool:
    return math.isfinite(x)


def _is_real_number(value: object) -> bool:
    return isinstance(value, Real) and not isinstance(value, bool)


def _require_positive_int(value: object, *, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise InputDataError(f"{name} must be positive int (got={value!r})")
    return value


def _require_ts_ms(value: object, *, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise InputDataError(f"{name} must be positive epoch-ms int (got={value!r})")
    return value


def _require_finite_real(
    value: object,
    *,
    name: str,
    positive: bool = False,
    non_negative: bool = False,
) -> float:
    if not _is_real_number(value):
        raise InputDataError(f"{name} must be real number (got={value!r})")

    out = float(value)
    if not _is_finite(out):
        raise InputDataError(f"{name} must be finite (got={value!r})")

    if positive and out <= 0:
        raise InputDataError(f"{name} must be > 0 (got={out})")

    if non_negative and out < 0:
        raise InputDataError(f"{name} must be >= 0 (got={out})")

    return out


def _validate_float_series_strict(
    values: Sequence[object],
    *,
    name: str,
    min_len: int = 1,
    positive: bool = False,
    non_negative: bool = False,
) -> List[float]:
    _require_positive_int(min_len, name=f"{name}.min_len")

    if not isinstance(values, list):
        raise InputDataError(f"{name} must be list (got={type(values).__name__})")
    if len(values) < min_len:
        raise InputDataError(f"{name} length<{min_len} (got={len(values)})")

    out: List[float] = []
    for idx, raw in enumerate(values):
        out.append(
            _require_finite_real(
                raw,
                name=f"{name}[{idx}]",
                positive=positive,
                non_negative=non_negative,
            )
        )
    return out


def _to_np(
    values: Sequence[object],
    *,
    name: str,
    min_len: int = 1,
    positive: bool = False,
    non_negative: bool = False,
) -> np.ndarray:
    validated = _validate_float_series_strict(
        values,
        name=name,
        min_len=min_len,
        positive=positive,
        non_negative=non_negative,
    )
    return np.asarray(validated, dtype="float64")


def _validate_candle_strict(raw: object, *, index: int, name: str) -> Candle:
    if not isinstance(raw, (tuple, list)):
        raise InputDataError(f"{name}[{index}] must be tuple/list (got={type(raw).__name__})")
    if len(raw) != 5:
        raise InputDataError(f"{name}[{index}] must have len=5 (got={len(raw)})")

    ts_raw, open_raw, high_raw, low_raw, close_raw = raw
    ts_ms = _require_ts_ms(ts_raw, name=f"{name}[{index}].ts_ms")
    open_px = _require_finite_real(open_raw, name=f"{name}[{index}].open", positive=True)
    high_px = _require_finite_real(high_raw, name=f"{name}[{index}].high", positive=True)
    low_px = _require_finite_real(low_raw, name=f"{name}[{index}].low", positive=True)
    close_px = _require_finite_real(close_raw, name=f"{name}[{index}].close", positive=True)

    if high_px < low_px:
        raise InputDataError(
            f"{name}[{index}] invalid OHLC: high < low (high={high_px}, low={low_px})"
        )
    if high_px < max(open_px, close_px):
        raise InputDataError(
            f"{name}[{index}] invalid OHLC: high < max(open, close) "
            f"(open={open_px}, high={high_px}, close={close_px})"
        )
    if low_px > min(open_px, close_px):
        raise InputDataError(
            f"{name}[{index}] invalid OHLC: low > min(open, close) "
            f"(open={open_px}, low={low_px}, close={close_px})"
        )

    return (ts_ms, open_px, high_px, low_px, close_px)


def _validate_candles_strict(
    candles: Sequence[object],
    *,
    name: str,
    min_len: int = 1,
) -> List[Candle]:
    _require_positive_int(min_len, name=f"{name}.min_len")

    if not isinstance(candles, list):
        raise InputDataError(f"{name} must be list (got={type(candles).__name__})")
    if len(candles) < min_len:
        raise InputDataError(f"{name} length<{min_len} (got={len(candles)})")

    out: List[Candle] = []
    prev_ts: Optional[int] = None
    for idx, raw in enumerate(candles):
        candle = _validate_candle_strict(raw, index=idx, name=name)
        ts_ms = candle[0]
        if prev_ts is not None and ts_ms <= prev_ts:
            raise InputDataError(
                f"{name} timestamp must be strictly increasing "
                f"(prev_ts={prev_ts}, ts={ts_ms}, index={idx})"
            )
        prev_ts = ts_ms
        out.append(candle)
    return out


def _require_last_finite(arr: np.ndarray, *, name: str) -> float:
    if arr.ndim != 1 or arr.size == 0:
        raise IndicatorComputationError(f"{name} produced empty or invalid-shaped result")
    last = float(arr[-1])
    if not _is_finite(last):
        raise IndicatorComputationError(f"{name} last value is not finite (got={last})")
    return last


def _require_talib_series_alignment(arr: np.ndarray, *, name: str) -> None:
    """
    TA-Lib alignment-preserving series contract:
    - leading warm-up non-finite: allowed
    - first valid value가 나온 뒤 non-finite 재등장: 금지
    - 마지막 값 non-finite: 금지
    """
    if arr.ndim != 1 or arr.size == 0:
        raise IndicatorComputationError(f"{name} produced empty or invalid-shaped result")

    seen_valid = False
    for idx, raw in enumerate(arr.tolist()):
        value = float(raw)
        if _is_finite(value):
            seen_valid = True
            continue
        if seen_valid:
            raise IndicatorComputationError(
                f"{name} has non-finite after first valid point (index={idx}, value={value})"
            )

    if not seen_valid:
        raise IndicatorComputationError(f"{name} has no valid finite values")
    _require_last_finite(arr, name=name)


def _require_aligned_series_strict(
    values: Sequence[object],
    *,
    name: str,
    expected_len: int,
) -> List[float]:
    if not isinstance(values, list):
        raise InputDataError(f"{name} must be list (got={type(values).__name__})")
    if len(values) != expected_len:
        raise InputDataError(f"{name} length mismatch: expected={expected_len}, got={len(values)}")

    out: List[float] = []
    seen_valid = False
    for idx, raw in enumerate(values):
        if not _is_real_number(raw):
            raise InputDataError(f"{name}[{idx}] must be real number (got={raw!r})")
        value = float(raw)
        if _is_finite(value):
            seen_valid = True
            out.append(value)
            continue
        if seen_valid:
            raise IndicatorComputationError(
                f"{name}[{idx}] non-finite after first valid point (got={value})"
            )
        out.append(value)

    if not out:
        raise InputDataError(f"{name} is required (empty)")
    if not _is_finite(out[-1]):
        raise IndicatorComputationError(f"{name} last value is not finite (got={out[-1]})")
    return out


@dataclass(frozen=True)
class RegimeThresholds:
    trend_ema_dist_min: float = 0.003
    trend_atr_pct_min: float = 0.003
    trend_strength_min: float = 1.2
    trend_adx_min: float = 25.0

    range_width_max: float = 0.004
    range_ema_dist_max: float = 0.002
    range_adx_max: float = 20.0


# ─────────────────────────────
# SMA (TA-Lib)
# ─────────────────────────────
def sma(values: List[float], length: int) -> List[float]:
    """
    계약:
    - input values는 모두 finite여야 한다.
    - 반환 시리즈는 input과 같은 길이를 유지한다.
    - TA-Lib warm-up leading non-finite만 허용한다.
    """
    length = _require_positive_int(length, name="sma.length")
    arr = _to_np(values, name="sma.values", min_len=length)

    _require_talib()
    out = talib.SMA(arr, timeperiod=length)
    _require_talib_series_alignment(out, name="talib.SMA")
    return out.tolist()


# ─────────────────────────────
# EMA (TA-Lib)
# ─────────────────────────────
def ema(values: List[float], length: int) -> List[float]:
    """
    계약:
    - input values는 모두 finite여야 한다.
    - 반환 시리즈는 input과 같은 길이를 유지한다.
    - TA-Lib warm-up leading non-finite만 허용한다.
    """
    length = _require_positive_int(length, name="ema.length")
    arr = _to_np(values, name="ema.values", min_len=length)

    _require_talib()
    out = talib.EMA(arr, timeperiod=length)
    _require_talib_series_alignment(out, name="talib.EMA")
    return out.tolist()


# ─────────────────────────────
# RSI (TA-Lib)
# ─────────────────────────────
def rsi(closes: List[float], length: int = 14) -> List[float]:
    """
    계약:
    - closes는 모두 finite여야 한다.
    - 반환 시리즈는 input과 같은 길이를 유지한다.
    - TA-Lib warm-up leading non-finite만 허용한다.
    """
    length = _require_positive_int(length, name="rsi.length")
    arr = _to_np(closes, name="rsi.closes", min_len=length + 1, positive=True)

    _require_talib()
    out = talib.RSI(arr, timeperiod=length)
    _require_talib_series_alignment(out, name="talib.RSI")
    return out.tolist()


# ─────────────────────────────
# ATR (TA-Lib)
# ─────────────────────────────
def calc_atr(candles: List[Candle], length: int = 14) -> float:
    length = _require_positive_int(length, name="atr.length")
    validated = _validate_candles_strict(candles, name="atr.candles", min_len=length + 1)

    highs = [c[2] for c in validated]
    lows = [c[3] for c in validated]
    closes = [c[4] for c in validated]

    _require_talib()
    h_arr = _to_np(highs, name="atr.highs", min_len=length + 1, positive=True)
    l_arr = _to_np(lows, name="atr.lows", min_len=length + 1, positive=True)
    c_arr = _to_np(closes, name="atr.closes", min_len=length + 1, positive=True)

    atr_series = talib.ATR(h_arr, l_arr, c_arr, timeperiod=length)
    _require_talib_series_alignment(atr_series, name="talib.ATR")
    return float(_require_last_finite(atr_series, name="talib.ATR"))


# ─────────────────────────────
# MACD (TA-Lib)
# ─────────────────────────────
def macd(
    values: List[float],
    fast_len: int = 12,
    slow_len: int = 26,
    signal_len: int = 9,
) -> Tuple[List[float], List[float], List[float]]:
    fast_len = _require_positive_int(fast_len, name="macd.fast_len")
    slow_len = _require_positive_int(slow_len, name="macd.slow_len")
    signal_len = _require_positive_int(signal_len, name="macd.signal_len")

    if fast_len >= slow_len:
        raise InputDataError(
            f"macd.fast_len must be < macd.slow_len (fast_len={fast_len}, slow_len={slow_len})"
        )

    arr = _to_np(values, name="macd.values", min_len=slow_len + signal_len, positive=True)

    _require_talib()
    macd_line, signal_line, hist = talib.MACD(
        arr,
        fastperiod=fast_len,
        slowperiod=slow_len,
        signalperiod=signal_len,
    )
    _require_talib_series_alignment(macd_line, name="talib.MACD.macd")
    _require_talib_series_alignment(signal_line, name="talib.MACD.signal")
    _require_talib_series_alignment(hist, name="talib.MACD.hist")
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
    반환 순서:
    - (mid, upper, lower)
    """
    length = _require_positive_int(length, name="bbands.length")
    num_std_val = _require_finite_real(num_std, name="bbands.num_std", positive=True)

    arr = _to_np(values, name="bbands.values", min_len=length, positive=True)

    _require_talib()
    upper, mid, lower = talib.BBANDS(
        arr,
        timeperiod=length,
        nbdevup=num_std_val,
        nbdevdn=num_std_val,
        matype=0,
    )
    _require_talib_series_alignment(mid, name="talib.BBANDS.mid")
    _require_talib_series_alignment(upper, name="talib.BBANDS.upper")
    _require_talib_series_alignment(lower, name="talib.BBANDS.lower")
    return mid.tolist(), upper.tolist(), lower.tolist()


# ─────────────────────────────
# OBV (TA-Lib)
# ─────────────────────────────
def obv(closes: List[float], volumes: List[float]) -> List[float]:
    validated_closes = _validate_float_series_strict(
        closes,
        name="obv.closes",
        min_len=1,
        positive=True,
    )
    validated_volumes = _validate_float_series_strict(
        volumes,
        name="obv.volumes",
        min_len=1,
        non_negative=True,
    )

    if len(validated_closes) != len(validated_volumes):
        raise InputDataError(
            f"obv length mismatch: closes={len(validated_closes)}, volumes={len(validated_volumes)}"
        )

    _require_talib()
    c_arr = np.asarray(validated_closes, dtype="float64")
    v_arr = np.asarray(validated_volumes, dtype="float64")
    out = talib.OBV(c_arr, v_arr)
    _require_talib_series_alignment(out, name="talib.OBV")
    return out.tolist()


# ─────────────────────────────
# ADX (TA-Lib)
# ─────────────────────────────
def adx(candles: List[Candle], length: int = 14) -> float:
    length = _require_positive_int(length, name="adx.length")
    validated = _validate_candles_strict(candles, name="adx.candles", min_len=2 * length)

    highs = [c[2] for c in validated]
    lows = [c[3] for c in validated]
    closes = [c[4] for c in validated]

    _require_talib()
    h_arr = _to_np(highs, name="adx.highs", min_len=2 * length, positive=True)
    l_arr = _to_np(lows, name="adx.lows", min_len=2 * length, positive=True)
    c_arr = _to_np(closes, name="adx.closes", min_len=2 * length, positive=True)

    adx_series = talib.ADX(h_arr, l_arr, c_arr, timeperiod=length)
    _require_talib_series_alignment(adx_series, name="talib.ADX")
    return float(_require_last_finite(adx_series, name="talib.ADX"))


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
    k_len = _require_positive_int(k_len, name="stoch.k_len")
    d_len = _require_positive_int(d_len, name="stoch.d_len")

    validated_highs = _validate_float_series_strict(
        highs,
        name="stoch.highs",
        min_len=k_len + d_len,
        positive=True,
    )
    validated_lows = _validate_float_series_strict(
        lows,
        name="stoch.lows",
        min_len=k_len + d_len,
        positive=True,
    )
    validated_closes = _validate_float_series_strict(
        closes,
        name="stoch.closes",
        min_len=k_len + d_len,
        positive=True,
    )

    if not (len(validated_highs) == len(validated_lows) == len(validated_closes)):
        raise InputDataError(
            "stoch length mismatch: "
            f"highs={len(validated_highs)}, lows={len(validated_lows)}, closes={len(validated_closes)}"
        )

    _require_talib()
    h_arr = np.asarray(validated_highs, dtype="float64")
    l_arr = np.asarray(validated_lows, dtype="float64")
    c_arr = np.asarray(validated_closes, dtype="float64")

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
    _require_talib_series_alignment(slowk, name="talib.STOCH.slowk")
    _require_talib_series_alignment(slowd, name="talib.STOCH.slowd")
    return slowk.tolist(), slowd.tolist()


# ─────────────────────────────
# Divergence helpers (pure python)
# ─────────────────────────────
def _find_last_two_pivot_highs(candles: List[Candle]) -> List[int]:
    validated = _validate_candles_strict(candles, name="pivot_highs.candles", min_len=1)
    idxs: List[int] = []
    if len(validated) < 3:
        return idxs

    for i in range(len(validated) - 2, 0, -1):
        _, _, high, _, _ = validated[i]
        _, _, prev_high, _, _ = validated[i - 1]
        _, _, next_high, _, _ = validated[i + 1]
        if high > prev_high and high > next_high:
            idxs.append(i)
            if len(idxs) == 2:
                break
    return list(reversed(idxs))


def _find_last_two_pivot_lows(candles: List[Candle]) -> List[int]:
    validated = _validate_candles_strict(candles, name="pivot_lows.candles", min_len=1)
    idxs: List[int] = []
    if len(validated) < 3:
        return idxs

    for i in range(len(validated) - 2, 0, -1):
        _, _, _, low, _ = validated[i]
        _, _, _, prev_low, _ = validated[i - 1]
        _, _, _, next_low, _ = validated[i + 1]
        if low < prev_low and low < next_low:
            idxs.append(i)
            if len(idxs) == 2:
                break
    return list(reversed(idxs))


def has_bearish_rsi_divergence(candles: List[Candle], rsi_vals: List[float]) -> bool:
    validated_candles = _validate_candles_strict(candles, name="bearish_div.candles", min_len=1)
    aligned_rsi = _require_aligned_series_strict(
        rsi_vals,
        name="bearish_div.rsi_vals",
        expected_len=len(validated_candles),
    )

    piv = _find_last_two_pivot_highs(validated_candles)
    if len(piv) < 2:
        return False

    i1, i2 = piv
    rsi1 = aligned_rsi[i1]
    rsi2 = aligned_rsi[i2]

    if not (_is_finite(rsi1) and _is_finite(rsi2)):
        raise IndicatorComputationError(
            f"bearish_div RSI pivot values must be finite (rsi1={rsi1}, rsi2={rsi2})"
        )

    _, _, high1, _, _ = validated_candles[i1]
    _, _, high2, _, _ = validated_candles[i2]
    return (high2 > high1) and (rsi2 < rsi1)


def has_bullish_rsi_divergence(candles: List[Candle], rsi_vals: List[float]) -> bool:
    validated_candles = _validate_candles_strict(candles, name="bullish_div.candles", min_len=1)
    aligned_rsi = _require_aligned_series_strict(
        rsi_vals,
        name="bullish_div.rsi_vals",
        expected_len=len(validated_candles),
    )

    piv = _find_last_two_pivot_lows(validated_candles)
    if len(piv) < 2:
        return False

    i1, i2 = piv
    rsi1 = aligned_rsi[i1]
    rsi2 = aligned_rsi[i2]

    if not (_is_finite(rsi1) and _is_finite(rsi2)):
        raise IndicatorComputationError(
            f"bullish_div RSI pivot values must be finite (rsi1={rsi1}, rsi2={rsi2})"
        )

    _, _, _, low1, _ = validated_candles[i1]
    _, _, _, low2, _ = validated_candles[i2]
    return (low2 < low1) and (rsi2 > rsi1)


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
    regime_thresholds: Optional[RegimeThresholds] = None,
) -> Dict[str, float]:
    fast_ema_len = _require_positive_int(fast_ema_len, name="regime.fast_ema_len")
    slow_ema_len = _require_positive_int(slow_ema_len, name="regime.slow_ema_len")
    atr_len = _require_positive_int(atr_len, name="regime.atr_len")
    rsi_len = _require_positive_int(rsi_len, name="regime.rsi_len")
    range_window = _require_positive_int(range_window, name="regime.range_window")

    need = max(
        fast_ema_len,
        slow_ema_len,
        100,
        200,
        atr_len + 1,
        rsi_len + 1,
        range_window,
        2 * atr_len,
    )
    validated = _validate_candles_strict(candles, name="regime.candles", min_len=need)

    closes = [c[4] for c in validated]
    highs = [c[2] for c in validated]
    lows = [c[3] for c in validated]

    last_close = float(closes[-1])
    if last_close <= 0 or not _is_finite(last_close):
        raise InputDataError(f"regime.last_close invalid: {last_close}")

    if regime_thresholds is None:
        regime_thresholds = RegimeThresholds()

    ema_fast_list = ema(closes, fast_ema_len)
    ema_slow_list = ema(closes, slow_ema_len)
    ema_100_list = ema(closes, 100)
    ema_200_list = ema(closes, 200)

    ema_fast_val = float(ema_fast_list[-1])
    ema_slow_val = float(ema_slow_list[-1])
    ema_100_val = float(ema_100_list[-1])
    ema_200_val = float(ema_200_list[-1])

    for nm, value in (
        ("ema_fast", ema_fast_val),
        ("ema_slow", ema_slow_val),
        ("ema_100", ema_100_val),
        ("ema_200", ema_200_val),
    ):
        if not _is_finite(value):
            raise IndicatorComputationError(f"{nm} is not finite (got={value})")

    def _last_two_valid(vals: Iterable[float], *, name: str) -> Tuple[float, float]:
        valid: List[float] = []
        for raw in vals:
            value = float(raw)
            if _is_finite(value):
                valid.append(value)
        if len(valid) < 2:
            raise IndicatorComputationError(f"{name} has <2 valid points")
        return valid[-2], valid[-1]

    ema_fast_prev, ema_fast_last = _last_two_valid(ema_fast_list, name="ema_fast_list")
    ema_fast_slope_pct = (ema_fast_last - ema_fast_prev) / last_close
    if not _is_finite(ema_fast_slope_pct):
        raise IndicatorComputationError(
            f"ema_fast_slope_pct not finite (got={ema_fast_slope_pct})"
        )

    ema_dist_pct = (ema_fast_val - ema_slow_val) / last_close
    if not _is_finite(ema_dist_pct):
        raise IndicatorComputationError(f"ema_dist_pct not finite (got={ema_dist_pct})")

    ema_200_dist_pct = (last_close - ema_200_val) / last_close
    if not _is_finite(ema_200_dist_pct):
        raise IndicatorComputationError(
            f"ema_200_dist_pct not finite (got={ema_200_dist_pct})"
        )

    atr_val = calc_atr(validated[-(atr_len + 1) :], length=atr_len)
    atr_pct = atr_val / last_close
    if not _is_finite(atr_pct) or atr_pct <= 0:
        raise IndicatorComputationError(
            f"atr_pct invalid (atr={atr_val}, last_close={last_close})"
        )

    high_recent = max(highs[-range_window:])
    low_recent = min(lows[-range_window:])
    range_pct = (float(high_recent) - float(low_recent)) / last_close
    if not _is_finite(range_pct) or range_pct <= 0:
        raise IndicatorComputationError(f"range_pct invalid (got={range_pct})")

    rsi_vals = rsi(closes, length=rsi_len)
    rsi_last = float(rsi_vals[-1])
    if not _is_finite(rsi_last):
        raise IndicatorComputationError(f"rsi_last not finite (got={rsi_last})")

    macd_line, macd_signal, macd_hist = macd(closes)
    macd_last = float(macd_line[-1])
    macd_signal_last = float(macd_signal[-1])
    macd_hist_last = float(macd_hist[-1])
    for nm, value in (
        ("macd", macd_last),
        ("macd_signal", macd_signal_last),
        ("macd_hist", macd_hist_last),
    ):
        if not _is_finite(value):
            raise IndicatorComputationError(f"{nm} not finite (got={value})")

    bb_mid, bb_upper, bb_lower = bollinger_bands(closes)
    bb_mid_last = float(bb_mid[-1])
    bb_upper_last = float(bb_upper[-1])
    bb_lower_last = float(bb_lower[-1])

    if not (
        _is_finite(bb_mid_last)
        and _is_finite(bb_upper_last)
        and _is_finite(bb_lower_last)
    ):
        raise IndicatorComputationError(
            "bbands last values must be finite "
            f"(mid={bb_mid_last}, upper={bb_upper_last}, lower={bb_lower_last})"
        )
    if bb_upper_last <= bb_lower_last:
        raise IndicatorComputationError(
            f"bbands invalid width (upper={bb_upper_last}, lower={bb_lower_last})"
        )

    bb_width_pct = (bb_upper_last - bb_lower_last) / last_close
    bb_pos = (last_close - bb_lower_last) / (bb_upper_last - bb_lower_last)
    if not (_is_finite(bb_width_pct) and _is_finite(bb_pos)):
        raise IndicatorComputationError("bb_width_pct/bb_pos not finite")

    stoch_k_vals, stoch_d_vals = stochastic_oscillator(highs, lows, closes)
    stoch_k_last = float(stoch_k_vals[-1])
    stoch_d_last = float(stoch_d_vals[-1])
    if not (_is_finite(stoch_k_last) and _is_finite(stoch_d_last)):
        raise IndicatorComputationError(
            f"stoch last not finite (k={stoch_k_last}, d={stoch_d_last})"
        )

    adx_last = adx(validated, length=atr_len)
    if not _is_finite(adx_last):
        raise IndicatorComputationError(f"adx_last not finite (got={adx_last})")

    t = regime_thresholds
    trend_strength = abs(ema_dist_pct) / atr_pct
    range_strength = range_pct / atr_pct

    if not (_is_finite(trend_strength) and _is_finite(range_strength)):
        raise IndicatorComputationError("trend_strength/range_strength not finite")

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

    out: Dict[str, float] = {
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

    for key, value in out.items():
        if not _is_finite(float(value)):
            raise IndicatorComputationError(f"regime output {key} is not finite (got={value})")

    return out


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