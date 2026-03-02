"""
========================================================
strategy/regime_detector.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
역할:
- 시장 상태(Regime)를 3가지로 분류한다.
  1) TREND       : 추세장
  2) RANGE       : 횡보장
  3) HIGH_VOL    : 변동성 큰 장
- 분류 기준은 "수치"로 고정한다. (감/휴리스틱 금지)
- 입력 데이터가 부족/불완전하면 즉시 예외. (폴백 금지)
- 이 모듈은 DB/거래소 주문/네트워크 호출 금지. (순수 계산)

입력 형태(둘 중 하나만 완전하면 됨):
A) 지표 기반(precomputed)
   - close, ema_fast, ema_slow, atr  (또는 atr_pct)
B) OHLC 시계열 기반(raw ohlc)
   - highs, lows, closes  (list/tuple, 길이 충분)

========================================================

변경 이력
--------------------------------------------------------
2026-03-02
- 신규 생성
- Regime 3분류(TREND/RANGE/HIGH_VOL) + STRICT 검증
- 설정(settings) 기반 임계값 오버라이드 지원(없으면 기본값 사용)
- regime 결과를 entry_threshold / risk_multiplier로 변환하는 헬퍼 제공
========================================================
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Sequence, Tuple

logger = logging.getLogger(__name__)

REGIME_TREND = "TREND"
REGIME_RANGE = "RANGE"
REGIME_HIGH_VOL = "HIGH_VOL"
REGIMES = (REGIME_TREND, REGIME_RANGE, REGIME_HIGH_VOL)


# ─────────────────────────────────────────────────────────────
# STRICT helpers (NO-FALLBACK)
# ─────────────────────────────────────────────────────────────
def _require_float(d: Dict[str, Any], key: str) -> float:
    if key not in d:
        raise ValueError(f"features missing required field: {key}")
    v = d.get(key)
    if v is None:
        raise ValueError(f"features field is null: {key}")
    try:
        f = float(v)
    except Exception as e:
        raise ValueError(f"features field must be a number: {key}") from e
    if not math.isfinite(f):
        raise ValueError(f"features field must be finite: {key}")
    return f


def _require_seq(d: Dict[str, Any], key: str) -> Sequence[float]:
    if key not in d:
        raise ValueError(f"features missing required field: {key}")
    v = d.get(key)
    if not isinstance(v, (list, tuple)):
        raise ValueError(f"features field must be list/tuple: {key}")
    if len(v) == 0:
        raise ValueError(f"features field is empty: {key}")
    out: List[float] = []
    for i, x in enumerate(v):
        try:
            fx = float(x)
        except Exception as e:
            raise ValueError(f"{key}[{i}] must be a number") from e
        if not math.isfinite(fx):
            raise ValueError(f"{key}[{i}] must be finite")
        out.append(fx)
    return out


def _as_int(v: Any, name: str) -> int:
    try:
        iv = int(v)
    except Exception as e:
        raise ValueError(f"{name} must be int") from e
    if iv <= 0:
        raise ValueError(f"{name} must be > 0")
    return iv


def _as_pct(v: Any, name: str) -> float:
    try:
        f = float(v)
    except Exception as e:
        raise ValueError(f"{name} must be a number") from e
    if not math.isfinite(f):
        raise ValueError(f"{name} must be finite")
    if f < 0:
        raise ValueError(f"{name} must be >= 0")
    return f


# ─────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class RegimeConfig:
    # raw ohlc 기반 계산을 쓸 때만 의미 있음
    ema_fast_period: int = 20
    ema_slow_period: int = 50
    atr_period: int = 14

    # 판정 임계값(퍼센트 단위)
    trend_ema_gap_pct: float = 0.35      # EMA(20/50) 간격이 close 대비 이 이상이면 추세 후보
    trend_slope_pct_min: float = 0.02    # EMA_fast 변화율(%)이 이 이상이면 추세 확정(노이즈 제거)
    high_vol_atr_pct: float = 1.20       # ATR%가 이 이상이면 변동성 과다

    # regime별 보정(진입 임계값/리스크)
    range_entry_threshold_add: float = 0.0     # 횡보일 때 진입 기준을 얼마나 올릴지(점수 단위)
    high_vol_entry_threshold_add: float = 0.0  # 변동성 과다일 때 진입 기준을 얼마나 올릴지
    trend_entry_threshold_add: float = 0.0     # 추세일 때 진입 기준을 얼마나 내릴지(음수 권장)

    range_risk_multiplier: float = 0.70        # 횡보일 때 포지션/리스크 배수
    high_vol_risk_multiplier: float = 0.50     # 변동성 과다일 때
    trend_risk_multiplier: float = 1.00        # 추세일 때


def _load_config_from_settings(settings: Any | None) -> RegimeConfig:
    cfg = RegimeConfig()
    if settings is None:
        return cfg

    # settings에 값이 "존재하면" 엄격 검증 후 덮어쓴다. (없으면 기본값 유지)
    def g(name: str, default: Any) -> Any:
        return getattr(settings, name, default)

    ema_fast_period = _as_int(g("regime_ema_fast_period", cfg.ema_fast_period), "regime_ema_fast_period")
    ema_slow_period = _as_int(g("regime_ema_slow_period", cfg.ema_slow_period), "regime_ema_slow_period")
    atr_period = _as_int(g("regime_atr_period", cfg.atr_period), "regime_atr_period")

    trend_ema_gap_pct = _as_pct(g("regime_trend_ema_gap_pct", cfg.trend_ema_gap_pct), "regime_trend_ema_gap_pct")
    trend_slope_pct_min = _as_pct(g("regime_trend_slope_pct_min", cfg.trend_slope_pct_min), "regime_trend_slope_pct_min")
    high_vol_atr_pct = _as_pct(g("regime_high_vol_atr_pct", cfg.high_vol_atr_pct), "regime_high_vol_atr_pct")

    range_entry_add = float(g("regime_range_entry_threshold_add", cfg.range_entry_threshold_add))
    high_vol_entry_add = float(g("regime_high_vol_entry_threshold_add", cfg.high_vol_entry_threshold_add))
    trend_entry_add = float(g("regime_trend_entry_threshold_add", cfg.trend_entry_threshold_add))

    range_risk_mult = float(g("regime_range_risk_multiplier", cfg.range_risk_multiplier))
    high_vol_risk_mult = float(g("regime_high_vol_risk_multiplier", cfg.high_vol_risk_multiplier))
    trend_risk_mult = float(g("regime_trend_risk_multiplier", cfg.trend_risk_multiplier))

    for name, v in [
        ("regime_range_risk_multiplier", range_risk_mult),
        ("regime_high_vol_risk_multiplier", high_vol_risk_mult),
        ("regime_trend_risk_multiplier", trend_risk_mult),
    ]:
        if not math.isfinite(v) or v <= 0:
            raise ValueError(f"{name} must be finite and > 0")

    if ema_fast_period >= ema_slow_period:
        raise ValueError("regime_ema_fast_period must be < regime_ema_slow_period")

    return RegimeConfig(
        ema_fast_period=ema_fast_period,
        ema_slow_period=ema_slow_period,
        atr_period=atr_period,
        trend_ema_gap_pct=trend_ema_gap_pct,
        trend_slope_pct_min=trend_slope_pct_min,
        high_vol_atr_pct=high_vol_atr_pct,
        range_entry_threshold_add=range_entry_add,
        high_vol_entry_threshold_add=high_vol_entry_add,
        trend_entry_threshold_add=trend_entry_add,
        range_risk_multiplier=range_risk_mult,
        high_vol_risk_multiplier=high_vol_risk_mult,
        trend_risk_multiplier=trend_risk_mult,
    )


# ─────────────────────────────────────────────────────────────
# Indicators (raw ohlc path)
# ─────────────────────────────────────────────────────────────
def _ema(values: Sequence[float], period: int) -> List[float]:
    if period <= 0:
        raise ValueError("period must be > 0")
    if len(values) < period + 2:
        raise ValueError(f"not enough data for EMA(period={period}): len={len(values)}")

    k = 2.0 / (period + 1.0)
    # 초기값은 SMA
    sma = sum(values[:period]) / float(period)
    out: List[float] = [sma]
    for v in values[period:]:
        prev = out[-1]
        out.append((v - prev) * k + prev)
    return out


def _atr(highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], period: int) -> List[float]:
    if period <= 0:
        raise ValueError("period must be > 0")
    n = len(closes)
    if len(highs) != n or len(lows) != n:
        raise ValueError("highs/lows/closes length mismatch")
    if n < period + 2:
        raise ValueError(f"not enough data for ATR(period={period}): len={n}")

    trs: List[float] = []
    for i in range(1, n):
        h = highs[i]
        l = lows[i]
        pc = closes[i - 1]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)

    # trs length is n-1
    if len(trs) < period:
        raise ValueError("not enough TR values for ATR")

    first = sum(trs[:period]) / float(period)
    out: List[float] = [first]
    for tr in trs[period:]:
        prev = out[-1]
        out.append((prev * (period - 1) + tr) / float(period))
    return out


# ─────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────
def detect_regime(
    features: Dict[str, Any],
    *,
    settings: Any | None = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    STRICT regime 판정.

    반환:
      (regime, debug)

    debug에는 판정 근거(atr_pct, ema_gap_pct, slope_pct 등)를 넣는다.
    """
    if not isinstance(features, dict):
        raise ValueError("features must be dict")

    cfg = _load_config_from_settings(settings)

    debug: Dict[str, Any] = {"config": cfg.__dict__.copy()}

    # ── Path A: precomputed metrics ───────────────────────────
    has_pre = all(k in features for k in ("close", "ema_fast", "ema_slow")) and (
        ("atr_pct" in features) or ("atr" in features)
    )

    # ── Path B: raw OHLC ─────────────────────────────────────
    has_ohlc = all(k in features for k in ("highs", "lows", "closes"))

    if not has_pre and not has_ohlc:
        raise ValueError(
            "features must contain either precomputed metrics "
            "(close, ema_fast, ema_slow, atr or atr_pct) "
            "or raw ohlc (highs, lows, closes)"
        )

    # ===== Compute required values =====
    if has_pre:
        close = _require_float(features, "close")
        ema_fast = _require_float(features, "ema_fast")
        ema_slow = _require_float(features, "ema_slow")

        if close <= 0:
            raise ValueError("close must be > 0")

        if "atr_pct" in features:
            atr_pct = _require_float(features, "atr_pct")
        else:
            atr = _require_float(features, "atr")
            if atr <= 0:
                raise ValueError("atr must be > 0")
            atr_pct = (atr / close) * 100.0

        # slope는 제공되면 쓰고, 없으면 "판정 불가"로 간주하지 않도록
        # ema_fast_slope는 선택 입력. 없으면 0으로 두는 것이 아니라 '미제공'으로 처리한다.
        slope_pct: float | None = None
        if "ema_fast_prev" in features:
            ema_fast_prev = _require_float(features, "ema_fast_prev")
            slope_pct = abs(ema_fast - ema_fast_prev) / close * 100.0

        ema_gap_pct = abs(ema_fast - ema_slow) / close * 100.0

        debug.update(
            {
                "path": "precomputed",
                "close": close,
                "ema_fast": ema_fast,
                "ema_slow": ema_slow,
                "atr_pct": atr_pct,
                "ema_gap_pct": ema_gap_pct,
                "slope_pct": slope_pct,
            }
        )
    else:
        highs = _require_seq(features, "highs")
        lows = _require_seq(features, "lows")
        closes = _require_seq(features, "closes")

        n = len(closes)
        if len(highs) != n or len(lows) != n:
            raise ValueError("highs/lows/closes length mismatch")
        if closes[-1] <= 0:
            raise ValueError("last close must be > 0")

        ema_fast_series = _ema(closes, cfg.ema_fast_period)
        ema_slow_series = _ema(closes, cfg.ema_slow_period)
        atr_series = _atr(highs, lows, closes, cfg.atr_period)

        # ema series alignment: _ema returns length n - period + 1
        # slope를 계산하려면 최소 2개 필요(이미 _ema에서 보장)
        ema_fast_last = ema_fast_series[-1]
        ema_fast_prev = ema_fast_series[-2]
        ema_slow_last = ema_slow_series[-1]

        # atr series alignment: _atr returns length (n-1 - period + 1)
        atr_last = atr_series[-1]

        close = float(closes[-1])
        atr_pct = (atr_last / close) * 100.0
        ema_gap_pct = abs(ema_fast_last - ema_slow_last) / close * 100.0
        slope_pct = abs(ema_fast_last - ema_fast_prev) / close * 100.0

        debug.update(
            {
                "path": "ohlc",
                "close": close,
                "ema_fast": float(ema_fast_last),
                "ema_slow": float(ema_slow_last),
                "atr_pct": float(atr_pct),
                "ema_gap_pct": float(ema_gap_pct),
                "slope_pct": float(slope_pct),
                "n": n,
            }
        )

    # ===== Regime decision (STRICT thresholds) =====
    atr_pct_v = float(debug["atr_pct"])
    ema_gap_pct_v = float(debug["ema_gap_pct"])
    slope_pct_v = debug.get("slope_pct", None)

    # 1) 변동성 과다 최우선
    if atr_pct_v >= cfg.high_vol_atr_pct:
        regime = REGIME_HIGH_VOL
        debug["decision"] = f"atr_pct({atr_pct_v:.4f}) >= high_vol_atr_pct({cfg.high_vol_atr_pct:.4f})"
        return regime, debug

    # 2) 추세 판정: EMA 간격 + (가능하면) slope 확인
    if ema_gap_pct_v >= cfg.trend_ema_gap_pct:
        if slope_pct_v is None:
            # slope를 못 구하는 입력이면, EMA gap만으로 추세 인정(단 기준은 이미 빡빡하게)
            regime = REGIME_TREND
            debug["decision"] = (
                f"ema_gap_pct({ema_gap_pct_v:.4f}) >= trend_ema_gap_pct({cfg.trend_ema_gap_pct:.4f}) "
                f"and slope unavailable"
            )
            return regime, debug

        if float(slope_pct_v) >= cfg.trend_slope_pct_min:
            regime = REGIME_TREND
            debug["decision"] = (
                f"ema_gap_pct({ema_gap_pct_v:.4f}) >= trend_ema_gap_pct({cfg.trend_ema_gap_pct:.4f}) "
                f"and slope_pct({float(slope_pct_v):.4f}) >= trend_slope_pct_min({cfg.trend_slope_pct_min:.4f})"
            )
            return regime, debug

    # 3) 그 외는 횡보로 취급
    regime = REGIME_RANGE
    debug["decision"] = "default -> RANGE (not HIGH_VOL, not TREND thresholds)"
    return regime, debug


def apply_regime_adjustments(
    *,
    regime: str,
    base_entry_threshold: float,
    settings: Any | None = None,
) -> Tuple[float, float, Dict[str, Any]]:
    """
    regime 결과를 "진입 임계값"과 "리스크 배수"로 변환한다.

    반환:
      (adjusted_threshold, risk_multiplier, debug)

    STRICT:
    - regime 값이 3개 중 하나가 아니면 예외
    - base_entry_threshold가 유한수 아니면 예외
    """
    cfg = _load_config_from_settings(settings)

    try:
        thr = float(base_entry_threshold)
    except Exception as e:
        raise ValueError("base_entry_threshold must be a number") from e
    if not math.isfinite(thr):
        raise ValueError("base_entry_threshold must be finite")

    r = str(regime).upper().strip()
    if r not in REGIMES:
        raise ValueError(f"invalid regime: {regime!r}")

    if r == REGIME_TREND:
        adj = thr + float(cfg.trend_entry_threshold_add)
        mult = float(cfg.trend_risk_multiplier)
    elif r == REGIME_HIGH_VOL:
        adj = thr + float(cfg.high_vol_entry_threshold_add)
        mult = float(cfg.high_vol_risk_multiplier)
    else:
        adj = thr + float(cfg.range_entry_threshold_add)
        mult = float(cfg.range_risk_multiplier)

    if not math.isfinite(adj) or not math.isfinite(mult) or mult <= 0:
        raise RuntimeError("computed regime adjustments invalid")

    debug = {
        "regime": r,
        "base_entry_threshold": thr,
        "adjusted_entry_threshold": adj,
        "risk_multiplier": mult,
        "config": cfg.__dict__.copy(),
    }
    return adj, mult, debug


__all__ = [
    "REGIME_TREND",
    "REGIME_RANGE",
    "REGIME_HIGH_VOL",
    "REGIMES",
    "RegimeConfig",
    "detect_regime",
    "apply_regime_adjustments",
]