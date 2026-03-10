"""
========================================================
FILE: strategy/entry_flow.py
ROLE:
- 엔트리 진입 여부를 결정한다.
- ENTER / SKIP 신호를 생성한다.
- 대시보드와 상위 실행 계층이 사용할 수 있는 pre-entry Signal 계약을 엄격히 구성한다.

CORE RESPONSIBILITIES:
- features / settings 입력 계약 strict 검증
- ENTER / SKIP 판단
- Signal.meta 필수 진단 정보 구성
- 실행 계층용 기본 entry_price_hint 제공
- quant / decision 메타 일관성 검증

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- 필수 features 누락 시 즉시 예외
- 필수 settings 누락 시 즉시 예외
- 더미값/기본값/자동보정 금지
- 숫자형은 finite 이어야 한다
- direction / action / reason 은 공백 불가
- 이 파일이 넣는 entry_price_hint 는 features.last_price 기반 기본 힌트다
- 최종 authoritative 실행 가격은 상위 계층(run_bot_ws)이 주입/정합화한다

CHANGE HISTORY:
- 2026-03-10:
  1) FIX(CONTRACT): Signal.meta 에 entry_price_hint / entry_price_source 기본 계약 추가
  2) FIX(STRICT): quant_final_decision 값/계산 decision 불일치 시 즉시 예외 처리
  3) FIX(STRICT): equity / dd_pct / direction / feature contract 검증 강화
  4) CLEANUP: 중복 meta 대입 제거, 상단 문서 구조 정리
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
_ALLOWED_ACTIONS = ("ENTER", "SKIP")
_ALLOWED_QUANT_FINAL_DECISIONS = ("ENTER", "SKIP")


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


def _require_action(value: Any, name: str) -> str:
    action = _require_nonempty_str(value, name).upper()
    if action not in _ALLOWED_ACTIONS:
        raise RuntimeError(f"{name} must be ENTER/SKIP (STRICT)")
    return action


def _require_quant_final_decision(value: Any, name: str) -> str:
    decision = _require_nonempty_str(value, name).upper()
    if decision not in _ALLOWED_QUANT_FINAL_DECISIONS:
        raise RuntimeError(f"{name} must be ENTER/SKIP (STRICT)")
    return decision


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


def _validate_core_feature_contract(features: Mapping[str, Any]) -> None:
    _read_feature_str(features, "symbol")
    _read_feature_str(features, "regime")
    _read_feature_str(features, "signal_source")
    _read_feature_int(features, "signal_ts_ms")
    _require_direction(features.get("direction"), "features.direction")

    last_price = _read_feature_float(features, "last_price", min_value=0.0)
    if last_price <= 0.0:
        raise RuntimeError("features.last_price must be > 0 (STRICT)")

    _require_list(features.get("candles_5m"), "features.candles_5m")
    _require_list(features.get("candles_5m_raw"), "features.candles_5m_raw")

    equity_current_usdt = _read_feature_float(features, "equity_current_usdt", min_value=0.0)
    equity_peak_usdt = _read_feature_float(features, "equity_peak_usdt", min_value=0.0)
    dd_pct = _read_feature_float(features, "dd_pct", min_value=0.0, max_value=100.0)

    if equity_current_usdt <= 0.0:
        raise RuntimeError("features.equity_current_usdt must be > 0 (STRICT)")
    if equity_peak_usdt <= 0.0:
        raise RuntimeError("features.equity_peak_usdt must be > 0 (STRICT)")
    if equity_current_usdt > equity_peak_usdt:
        raise RuntimeError("features.equity_current_usdt must be <= equity_peak_usdt (STRICT)")
    if dd_pct > 0.0 and equity_current_usdt == equity_peak_usdt:
        raise RuntimeError("features.dd_pct > 0 but equity_current_usdt == equity_peak_usdt (STRICT)")

    _read_feature_float(features, "entry_score")
    _read_feature_float(features, "trend_strength")
    _read_feature_float(features, "spread", min_value=0.0)
    _read_feature_float(features, "orderbook_imbalance")


def _validate_quant_contract(features: Mapping[str, Any], decision_action: str) -> None:
    if "quant_final_decision" not in features:
        return

    quant_final_decision = _require_quant_final_decision(
        features.get("quant_final_decision"),
        "features.quant_final_decision",
    )
    if quant_final_decision != decision_action:
        raise RuntimeError(
            "features.quant_final_decision conflicts with computed entry decision "
            f"(STRICT): quant_final_decision={quant_final_decision} decision_action={decision_action}"
        )


def evaluate_entry_flow_strict(
    *,
    features: Dict[str, Any],
    settings: Any,
) -> EntryFlowDecision:
    f = _require_mapping(features, "features")
    if settings is None:
        raise RuntimeError("settings is required (STRICT)")

    _validate_core_feature_contract(f)

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


def _build_signal_meta_strict(
    *,
    features: Dict[str, Any],
    decision: EntryFlowDecision,
    entry_risk_pct: float,
) -> Dict[str, Any]:
    symbol = _read_feature_str(features, "symbol").upper()
    regime = _read_feature_str(features, "regime")
    signal_source = _read_feature_str(features, "signal_source")
    signal_ts_ms = _read_feature_int(features, "signal_ts_ms")
    last_price = _read_feature_float(features, "last_price", min_value=0.0)
    candles_5m = _require_list(features.get("candles_5m"), "features.candles_5m")
    candles_5m_raw = _require_list(features.get("candles_5m_raw"), "features.candles_5m_raw")
    equity_current_usdt = _read_feature_float(features, "equity_current_usdt", min_value=0.0)
    equity_peak_usdt = _read_feature_float(features, "equity_peak_usdt", min_value=0.0)
    dd_pct = _read_feature_float(features, "dd_pct", min_value=0.0, max_value=100.0)

    extra = _optional_dict(features.get("extra"), "features.extra")
    guard_adjustments = _optional_dict(features.get("guard_adjustments"), "features.guard_adjustments")
    quant_decision_pre = _optional_dict(features.get("quant_decision_pre"), "features.quant_decision_pre")
    quant_constraints = _optional_dict(features.get("quant_constraints"), "features.quant_constraints")
    micro_score_risk = _optional_float(
        features.get("micro_score_risk"),
        "features.micro_score_risk",
        min_value=0.0,
        max_value=100.0,
    )

    meta: Dict[str, Any] = {
        "symbol": symbol,
        "regime": regime,
        "signal_source": signal_source,
        "signal_ts_ms": int(signal_ts_ms),
        "last_price": float(last_price),
        "entry_price_hint": float(last_price),
        "entry_price_source": "features.last_price",
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
        "decision_action": decision.action,
        "dynamic_allocation_ratio": float(entry_risk_pct),
    }

    if micro_score_risk is not None:
        meta["micro_score_risk"] = float(micro_score_risk)

    if extra is not None:
        meta["extra"] = extra
    if guard_adjustments is not None:
        meta["guard_adjustments"] = guard_adjustments
    if "decision_id" in features:
        meta["decision_id"] = _require_nonempty_str(features.get("decision_id"), "features.decision_id")
    if quant_decision_pre is not None:
        meta["quant_decision_pre"] = quant_decision_pre
    if quant_constraints is not None:
        meta["quant_constraints"] = quant_constraints
    if "quant_final_decision" in features:
        meta["quant_final_decision"] = _require_quant_final_decision(
            features.get("quant_final_decision"),
            "features.quant_final_decision",
        )

    return meta


def build_entry_signal_strict(
    *,
    features: Dict[str, Any],
    settings: Any,
) -> Signal:
    f = _require_mapping(features, "features")
    if settings is None:
        raise RuntimeError("settings is required (STRICT)")

    _validate_core_feature_contract(f)

    decision = evaluate_entry_flow_strict(features=f, settings=settings)
    _validate_quant_contract(f, decision.action)

    direction = _require_direction(f.get("direction"), "features.direction")

    entry_tp_pct = _read_setting_float(settings, "entry_tp_pct", min_value=0.0, max_value=1.0)
    entry_sl_pct = _read_setting_float(settings, "entry_sl_pct", min_value=0.0, max_value=1.0)
    entry_risk_pct = _read_setting_float(settings, "entry_risk_pct", min_value=0.0, max_value=1.0)

    if entry_tp_pct <= 0.0:
        raise RuntimeError("settings.entry_tp_pct must be > 0 (STRICT)")
    if entry_sl_pct <= 0.0:
        raise RuntimeError("settings.entry_sl_pct must be > 0 (STRICT)")
    if entry_risk_pct <= 0.0:
        raise RuntimeError("settings.entry_risk_pct must be > 0 (STRICT)")

    guard_adjustments = _optional_dict(f.get("guard_adjustments"), "features.guard_adjustments")
    meta = _build_signal_meta_strict(
        features=f,
        decision=decision,
        entry_risk_pct=float(entry_risk_pct),
    )

    return Signal(
        action=_require_action(decision.action, "decision.action"),
        direction=direction,
        risk_pct=float(entry_risk_pct),
        tp_pct=float(entry_tp_pct),
        sl_pct=float(entry_sl_pct),
        reason=_require_nonempty_str(decision.reason, "decision.reason"),
        meta=meta,
        guard_adjustments=(guard_adjustments if guard_adjustments is not None else None),
    )


__all__ = [
    "EntryFlowDecision",
    "evaluate_entry_flow_strict",
    "build_entry_signal_strict",
]