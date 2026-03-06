"""
========================================================
FILE: strategy/entry_flow.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할
--------------------------------------------------------
- 엔트리 진입 여부를 결정한다.
- ENTER 또는 SKIP 신호를 생성한다.
- 실행 엔진(execution_engine.py)과 대시보드 Decision 패널이
  바로 사용할 수 있도록 meta 필수 키를 엄격하게 채운다.

출력
--------------------------------------------------------
- strategy.signal.Signal 반환
- action:
  - ENTER
  - SKIP

필수 features 스키마
--------------------------------------------------------
{
  "symbol": "BTCUSDT",
  "regime": "TREND",
  "signal_source": "ws_signal_candidate",
  "signal_ts_ms": 1772740500000,
  "direction": "LONG" | "SHORT",
  "last_price": 62300.5,
  "entry_score": 1.92,
  "trend_strength": 0.73,
  "orderbook_imbalance": 0.41,
  "spread": 0.00018,
  "candles_5m": [...],
  "candles_5m_raw": [...],
  "equity_current_usdt": 1012.5,
  "equity_peak_usdt": 1030.0,
  "dd_pct": 1.70
}

선택 features 스키마
--------------------------------------------------------
{
  "micro_score_risk": 22.0,
  "extra": {...},
  "guard_adjustments": {...},
  "decision_id": "dec-...",
  "quant_decision_pre": {...},
  "quant_constraints": {...},
  "quant_final_decision": "ENTER"
}

필수 settings 필드
--------------------------------------------------------
- entry_score_threshold
- entry_tp_pct
- entry_sl_pct
- entry_risk_pct
- entry_max_spread_pct
- entry_min_trend_strength
- entry_min_abs_orderbook_imbalance

절대 원칙 (STRICT · NO-FALLBACK)
--------------------------------------------------------
- 필수 features 누락 시 즉시 예외.
- 필수 settings 누락 시 즉시 예외.
- 더미값/기본값/자동보정 금지.
- 숫자형은 finite 이어야 한다.
- direction / action / reason 은 공백 불가.
- 실행 엔진이 요구하는 meta 필수 키는 반드시 포함한다.

변경 이력
--------------------------------------------------------
- 2026-03-06:
  1) 신규 생성: 엔트리 판단 파일 추가
  2) ENTER / SKIP 판정 및 Signal 생성 추가
  3) Decision 패널용 meta(entry_score/trend_strength/spread/orderbook_imbalance/threshold) 연결
========================================================
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional

from strategy.signal import Signal


_ALLOWED_DIRECTIONS = ("LONG", "SHORT")


@dataclass(frozen=True, slots=True)
class EntryFlowDecision:
    action: str
    direction: str
    reason: str
    reasons: List[str]
    summary: str
    entry_score: float
    threshold: float
    trend_strength: float
    spread: float
    orderbook_imbalance: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "action": self.action,
            "direction": self.direction,
            "reason": self.reason,
            "reasons": list(self.reasons),
            "summary": self.summary,
            "entry_score": self.entry_score,
            "threshold": self.threshold,
            "trend_strength": self.trend_strength,
            "spread": self.spread,
            "orderbook_imbalance": self.orderbook_imbalance,
        }


def _require_mapping(value: Any, name: str) -> Dict[str, Any]:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if not isinstance(value, dict):
        raise RuntimeError(f"{name} must be dict (STRICT), got={type(value).__name__}")
    return dict(value)


def _require_nonempty_str(value: Any, name: str) -> str:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if not isinstance(value, str):
        raise RuntimeError(f"{name} must be str (STRICT), got={type(value).__name__}")
    s = value.strip()
    if not s:
        raise RuntimeError(f"{name} must not be empty (STRICT)")
    return s


def _require_direction(value: Any, name: str) -> str:
    direction = _require_nonempty_str(value, name).upper()
    if direction not in _ALLOWED_DIRECTIONS:
        raise RuntimeError(f"{name} must be LONG/SHORT (STRICT)")
    return direction


def _require_int(value: Any, name: str) -> int:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if isinstance(value, bool):
        raise RuntimeError(f"{name} must be int (STRICT), bool not allowed")
    try:
        iv = int(value)
    except Exception as exc:
        raise RuntimeError(f"{name} must be int (STRICT): {exc}") from exc
    if iv <= 0:
        raise RuntimeError(f"{name} must be > 0 (STRICT)")
    return iv


def _require_float(
    value: Any,
    name: str,
    *,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> float:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if isinstance(value, bool):
        raise RuntimeError(f"{name} must be numeric (STRICT), bool not allowed")
    try:
        fv = float(value)
    except Exception as exc:
        raise RuntimeError(f"{name} must be numeric (STRICT): {exc}") from exc
    if not math.isfinite(fv):
        raise RuntimeError(f"{name} must be finite (STRICT)")
    if min_value is not None and fv < min_value:
        raise RuntimeError(f"{name} must be >= {min_value} (STRICT)")
    if max_value is not None and fv > max_value:
        raise RuntimeError(f"{name} must be <= {max_value} (STRICT)")
    return fv


def _optional_float(
    value: Any,
    name: str,
    *,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> Optional[float]:
    if value is None:
        return None
    return _require_float(value, name, min_value=min_value, max_value=max_value)


def _require_list(value: Any, name: str) -> list:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if not isinstance(value, list):
        raise RuntimeError(f"{name} must be list (STRICT), got={type(value).__name__}")
    if not value:
        raise RuntimeError(f"{name} must not be empty (STRICT)")
    return value


def _optional_dict(value: Any, name: str) -> Optional[Dict[str, Any]]:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise RuntimeError(f"{name} must be dict when provided (STRICT)")
    return dict(value)


def _read_feature_str(features: Mapping[str, Any], key: str) -> str:
    return _require_nonempty_str(features.get(key), f"features.{key}")


def _read_feature_int(features: Mapping[str, Any], key: str) -> int:
    return _require_int(features.get(key), f"features.{key}")


def _read_feature_float(
    features: Mapping[str, Any],
    key: str,
    *,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> float:
    return _require_float(features.get(key), f"features.{key}", min_value=min_value, max_value=max_value)


def _read_setting_float(
    settings: Any,
    key: str,
    *,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> float:
    if not hasattr(settings, key):
        raise RuntimeError(f"settings.{key} is required (STRICT)")
    return _require_float(getattr(settings, key), f"settings.{key}", min_value=min_value, max_value=max_value)


def evaluate_entry_flow_strict(
    *,
    features: Dict[str, Any],
    settings: Any,
) -> EntryFlowDecision:
    f = _require_mapping(features, "features")
    if settings is None:
        raise RuntimeError("settings is required (STRICT)")

    direction = _require_direction(f.get("direction"), "features.direction")
    entry_score = _read_feature_float(f, "entry_score")
    trend_strength = _read_feature_float(f, "trend_strength")
    spread = _read_feature_float(f, "spread", min_value=0.0)
    orderbook_imbalance = _read_feature_float(f, "orderbook_imbalance")

    threshold = _read_setting_float(settings, "entry_score_threshold")
    max_spread_pct = _read_setting_float(settings, "entry_max_spread_pct", min_value=0.0)
    min_trend_strength = _read_setting_float(settings, "entry_min_trend_strength")
    min_abs_orderbook_imbalance = _read_setting_float(settings, "entry_min_abs_orderbook_imbalance", min_value=0.0)

    reasons: List[str] = []

    if entry_score < threshold:
        reasons.append("entry_score_below_threshold")

    if trend_strength < min_trend_strength:
        reasons.append("trend_strength_below_min")

    if spread > max_spread_pct:
        reasons.append("spread_above_max")

    if abs(orderbook_imbalance) < min_abs_orderbook_imbalance:
        reasons.append("orderbook_imbalance_weak")

    if direction == "LONG" and orderbook_imbalance <= 0.0:
        reasons.append("orderbook_not_bullish")
    if direction == "SHORT" and orderbook_imbalance >= 0.0:
        reasons.append("orderbook_not_bearish")

    if reasons:
        return EntryFlowDecision(
            action="SKIP",
            direction=direction,
            reason=reasons[0],
            reasons=reasons,
            summary="엔트리 조건 부족으로 진입하지 않음",
            entry_score=entry_score,
            threshold=threshold,
            trend_strength=trend_strength,
            spread=spread,
            orderbook_imbalance=orderbook_imbalance,
        )

    return EntryFlowDecision(
        action="ENTER",
        direction=direction,
        reason="entry_conditions_passed",
        reasons=["entry_conditions_passed"],
        summary="엔트리 조건 충족으로 진입 진행",
        entry_score=entry_score,
        threshold=threshold,
        trend_strength=trend_strength,
        spread=spread,
        orderbook_imbalance=orderbook_imbalance,
    )


def build_entry_signal_strict(
    *,
    features: Dict[str, Any],
    settings: Any,
) -> Signal:
    f = _require_mapping(features, "features")
    if settings is None:
        raise RuntimeError("settings is required (STRICT)")

    symbol = _read_feature_str(f, "symbol").upper()
    regime = _read_feature_str(f, "regime")
    signal_source = _read_feature_str(f, "signal_source")
    signal_ts_ms = _read_feature_int(f, "signal_ts_ms")
    direction = _require_direction(f.get("direction"), "features.direction")
    last_price = _read_feature_float(f, "last_price", min_value=0.0)
    if last_price <= 0.0:
        raise RuntimeError("features.last_price must be > 0 (STRICT)")

    candles_5m = _require_list(f.get("candles_5m"), "features.candles_5m")
    candles_5m_raw = _require_list(f.get("candles_5m_raw"), "features.candles_5m_raw")

    equity_current_usdt = _read_feature_float(f, "equity_current_usdt", min_value=0.0)
    equity_peak_usdt = _read_feature_float(f, "equity_peak_usdt", min_value=0.0)
    dd_pct = _read_feature_float(f, "dd_pct", min_value=0.0, max_value=100.0)

    if equity_current_usdt <= 0.0:
        raise RuntimeError("features.equity_current_usdt must be > 0 (STRICT)")
    if equity_peak_usdt <= 0.0:
        raise RuntimeError("features.equity_peak_usdt must be > 0 (STRICT)")

    decision = evaluate_entry_flow_strict(features=f, settings=settings)

    entry_tp_pct = _read_setting_float(settings, "entry_tp_pct", min_value=0.0, max_value=1.0)
    entry_sl_pct = _read_setting_float(settings, "entry_sl_pct", min_value=0.0, max_value=1.0)
    entry_risk_pct = _read_setting_float(settings, "entry_risk_pct", min_value=0.0, max_value=1.0)

    if entry_tp_pct <= 0.0:
        raise RuntimeError("settings.entry_tp_pct must be > 0 (STRICT)")
    if entry_sl_pct <= 0.0:
        raise RuntimeError("settings.entry_sl_pct must be > 0 (STRICT)")
    if entry_risk_pct <= 0.0:
        raise RuntimeError("settings.entry_risk_pct must be > 0 (STRICT)")

    extra = _optional_dict(f.get("extra"), "features.extra")
    guard_adjustments = _optional_dict(f.get("guard_adjustments"), "features.guard_adjustments")
    quant_decision_pre = _optional_dict(f.get("quant_decision_pre"), "features.quant_decision_pre")
    quant_constraints = _optional_dict(f.get("quant_constraints"), "features.quant_constraints")

    micro_score_risk = _optional_float(f.get("micro_score_risk"), "features.micro_score_risk", min_value=0.0, max_value=100.0)

    meta: Dict[str, Any] = {
        "symbol": symbol,
        "regime": regime,
        "signal_source": signal_source,
        "signal_ts_ms": int(signal_ts_ms),
        "last_price": float(last_price),
        "candles_5m": candles_5m,
        "candles_5m_raw": candles_5m_raw,
        "equity_current_usdt": float(equity_current_usdt),
        "equity_peak_usdt": float(equity_peak_usdt),
        "dd_pct": float(dd_pct),
        "entry_score": float(decision.entry_score),
        "trend_strength": float(decision.trend_strength),
        "spread": float(decision.spread),
        "orderbook_imbalance": float(decision.orderbook_imbalance),
        "entry_score_threshold": float(decision.threshold),
        "decision_summary": decision.summary,
        "decision_reasons": list(decision.reasons),
        "dynamic_allocation_ratio": float(entry_risk_pct),
        "decision_action": decision.action,
    }

    if micro_score_risk is not None:
        meta["micro_score_risk"] = float(micro_score_risk)

    if extra is not None:
        meta["extra"] = extra
    if guard_adjustments is not None:
        meta["guard_adjustments"] = guard_adjustments
    if "decision_id" in f:
        meta["decision_id"] = _require_nonempty_str(f.get("decision_id"), "features.decision_id")
    if quant_decision_pre is not None:
        meta["quant_decision_pre"] = quant_decision_pre
    if quant_constraints is not None:
        meta["quant_constraints"] = quant_constraints
    if "quant_final_decision" in f:
        meta["quant_final_decision"] = _require_nonempty_str(f.get("quant_final_decision"), "features.quant_final_decision")

    return Signal(
        action=decision.action,
        direction=direction,
        risk_pct=float(entry_risk_pct),
        tp_pct=float(entry_tp_pct),
        sl_pct=float(entry_sl_pct),
        reason=decision.reason,
        meta=meta,
        guard_adjustments=(guard_adjustments if guard_adjustments is not None else None),
    )


__all__ = [
    "EntryFlowDecision",
    "evaluate_entry_flow_strict",
    "build_entry_signal_strict",
]