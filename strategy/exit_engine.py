"""
========================================================
FILE: strategy/exit_engine.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할
--------------------------------------------------------
- 오픈 포지션의 EXIT 여부를 결정한다.
- HOLD 또는 EXIT 판단을 생성한다.
- 대시보드 "의사결정 이유" 패널이 바로 사용할 수 있도록
  Decision payload를 엄격하게 생성한다.

출력
--------------------------------------------------------
- ExitFlowDecision 반환
- action:
  - HOLD
  - EXIT

필수 features 스키마
--------------------------------------------------------
{
  "symbol": "BTCUSDT",
  "regime": "TREND",
  "signal_source": "exit_engine",
  "signal_ts_ms": 1772740500000,
  "direction": "LONG" | "SHORT",
  "entry_price": 62300.0,
  "current_price": 62450.5,
  "position_qty": 0.01,
  "entry_score": 1.92,
  "exit_score": 0.84,
  "trend_strength": 0.73,
  "spread": 0.00018,
  "orderbook_imbalance": 0.41,
  "tp_pct": 0.02,
  "sl_pct": 0.01
}

선택 features 스키마
--------------------------------------------------------
{
  "holding_seconds": 180.0,
  "mark_price": 62451.0,
  "decision_id": "dec-...",
  "extra": {...}
}

필수 settings 필드
--------------------------------------------------------
- exit_score_threshold
- exit_max_spread_pct
- exit_trend_reversal_threshold
- exit_orderbook_reversal_threshold

절대 원칙 (STRICT · NO-FALLBACK)
--------------------------------------------------------
- 필수 features 누락 시 즉시 예외.
- 필수 settings 누락 시 즉시 예외.
- 더미값/기본값/자동보정 금지.
- 숫자형은 finite 이어야 한다.
- direction / action / reason / summary 는 공백 불가.

변경 이력
--------------------------------------------------------
- 2026-03-06:
  1) 신규 생성: HOLD / EXIT 판단 파일 추가
  2) TAKE_PROFIT / STOP_LOSS / EXIT_SIGNAL / HOLD 사유 생성 추가
  3) 대시보드 Decision payload(action/summary/reasons/entry_score/exit_score 등) 생성 추가
========================================================
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Tuple


_ALLOWED_DIRECTIONS: Tuple[str, ...] = ("LONG", "SHORT")
_ALLOWED_ACTIONS: Tuple[str, ...] = ("HOLD", "EXIT")
_ALLOWED_REASON_CODES: Tuple[str, ...] = (
    "TAKE_PROFIT",
    "STOP_LOSS",
    "EXIT_SIGNAL",
    "HOLD",
)


@dataclass(frozen=True, slots=True)
class ExitFlowDecision:
    action: str
    reason_code: str
    summary: str
    reasons: List[str]
    direction: str
    entry_score: float
    exit_score: float
    exit_score_threshold: float
    trend_strength: float
    spread: float
    orderbook_imbalance: float
    entry_price: float
    current_price: float
    target_price: float
    stop_price: float
    pnl_pct: float
    position_qty: float
    signal_source: str
    regime: str
    symbol: str
    signal_ts_ms: int
    holding_seconds: Optional[float]
    mark_price: Optional[float]
    decision_id: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "action": self.action,
            "reason_code": self.reason_code,
            "summary": self.summary,
            "reasons": list(self.reasons),
            "direction": self.direction,
            "entry_score": self.entry_score,
            "exit_score": self.exit_score,
            "exit_score_threshold": self.exit_score_threshold,
            "trend_strength": self.trend_strength,
            "spread": self.spread,
            "orderbook_imbalance": self.orderbook_imbalance,
            "entry_price": self.entry_price,
            "current_price": self.current_price,
            "target_price": self.target_price,
            "stop_price": self.stop_price,
            "pnl_pct": self.pnl_pct,
            "position_qty": self.position_qty,
            "signal_source": self.signal_source,
            "regime": self.regime,
            "symbol": self.symbol,
            "signal_ts_ms": self.signal_ts_ms,
            "holding_seconds": self.holding_seconds,
            "mark_price": self.mark_price,
            "decision_id": self.decision_id,
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


def _optional_nonempty_str(value: Any, name: str) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError(f"{name} must be str when provided (STRICT), got={type(value).__name__}")
    s = value.strip()
    if not s:
        raise RuntimeError(f"{name} must not be blank when provided (STRICT)")
    return s


def _require_direction(value: Any, name: str) -> str:
    direction = _require_nonempty_str(value, name).upper()
    if direction not in _ALLOWED_DIRECTIONS:
        raise RuntimeError(f"{name} must be LONG/SHORT (STRICT)")
    return direction


def _require_positive_int(value: Any, name: str) -> int:
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


def _read_feature_str(features: Mapping[str, Any], key: str) -> str:
    return _require_nonempty_str(features.get(key), f"features.{key}")


def _read_feature_direction(features: Mapping[str, Any], key: str) -> str:
    return _require_direction(features.get(key), f"features.{key}")


def _read_feature_int(features: Mapping[str, Any], key: str) -> int:
    return _require_positive_int(features.get(key), f"features.{key}")


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
    if settings is None:
        raise RuntimeError("settings is required (STRICT)")
    if not hasattr(settings, key):
        raise RuntimeError(f"settings.{key} is required (STRICT)")
    return _require_float(getattr(settings, key), f"settings.{key}", min_value=min_value, max_value=max_value)


def _calc_prices_and_pnl_pct(
    *,
    direction: str,
    entry_price: float,
    current_price: float,
    tp_pct: float,
    sl_pct: float,
) -> Tuple[float, float, float]:
    if entry_price <= 0.0:
        raise RuntimeError("entry_price must be > 0 (STRICT)")
    if current_price <= 0.0:
        raise RuntimeError("current_price must be > 0 (STRICT)")
    if tp_pct <= 0.0:
        raise RuntimeError("tp_pct must be > 0 (STRICT)")
    if sl_pct <= 0.0:
        raise RuntimeError("sl_pct must be > 0 (STRICT)")

    d = _require_direction(direction, "direction")

    if d == "LONG":
        pnl_pct = (current_price - entry_price) / entry_price
        target_price = entry_price * (1.0 + tp_pct)
        stop_price = entry_price * (1.0 - sl_pct)
    else:
        pnl_pct = (entry_price - current_price) / entry_price
        target_price = entry_price * (1.0 - tp_pct)
        stop_price = entry_price * (1.0 + sl_pct)

    if target_price <= 0.0 or stop_price <= 0.0:
        raise RuntimeError("target_price/stop_price invalid (STRICT)")

    return float(target_price), float(stop_price), float(pnl_pct)


def _is_reversal(
    *,
    direction: str,
    trend_strength: float,
    orderbook_imbalance: float,
    trend_reversal_threshold: float,
    orderbook_reversal_threshold: float,
) -> bool:
    d = _require_direction(direction, "direction")

    if d == "LONG":
        trend_reversal = trend_strength <= trend_reversal_threshold
        orderbook_reversal = orderbook_imbalance <= -orderbook_reversal_threshold
    else:
        trend_reversal = trend_strength >= -trend_reversal_threshold
        orderbook_reversal = orderbook_imbalance >= orderbook_reversal_threshold

    return bool(trend_reversal or orderbook_reversal)


def evaluate_exit_flow_strict(
    *,
    features: Dict[str, Any],
    settings: Any,
) -> ExitFlowDecision:
    f = _require_mapping(features, "features")
    if settings is None:
        raise RuntimeError("settings is required (STRICT)")

    symbol = _read_feature_str(f, "symbol").upper()
    regime = _read_feature_str(f, "regime")
    signal_source = _read_feature_str(f, "signal_source")
    signal_ts_ms = _read_feature_int(f, "signal_ts_ms")
    direction = _read_feature_direction(f, "direction")

    entry_price = _read_feature_float(f, "entry_price", min_value=0.0)
    current_price = _read_feature_float(f, "current_price", min_value=0.0)
    position_qty = _read_feature_float(f, "position_qty", min_value=0.0)
    if entry_price <= 0.0:
        raise RuntimeError("features.entry_price must be > 0 (STRICT)")
    if current_price <= 0.0:
        raise RuntimeError("features.current_price must be > 0 (STRICT)")
    if position_qty <= 0.0:
        raise RuntimeError("features.position_qty must be > 0 (STRICT)")

    entry_score = _read_feature_float(f, "entry_score")
    exit_score = _read_feature_float(f, "exit_score")
    trend_strength = _read_feature_float(f, "trend_strength")
    spread = _read_feature_float(f, "spread", min_value=0.0)
    orderbook_imbalance = _read_feature_float(f, "orderbook_imbalance")
    tp_pct = _read_feature_float(f, "tp_pct", min_value=0.0, max_value=1.0)
    sl_pct = _read_feature_float(f, "sl_pct", min_value=0.0, max_value=1.0)

    if tp_pct <= 0.0:
        raise RuntimeError("features.tp_pct must be > 0 (STRICT)")
    if sl_pct <= 0.0:
        raise RuntimeError("features.sl_pct must be > 0 (STRICT)")

    exit_score_threshold = _read_setting_float(settings, "exit_score_threshold")
    exit_max_spread_pct = _read_setting_float(settings, "exit_max_spread_pct", min_value=0.0)
    exit_trend_reversal_threshold = _read_setting_float(settings, "exit_trend_reversal_threshold")
    exit_orderbook_reversal_threshold = _read_setting_float(settings, "exit_orderbook_reversal_threshold", min_value=0.0)

    target_price, stop_price, pnl_pct = _calc_prices_and_pnl_pct(
        direction=direction,
        entry_price=entry_price,
        current_price=current_price,
        tp_pct=tp_pct,
        sl_pct=sl_pct,
    )

    holding_seconds = _optional_float(f.get("holding_seconds"), "features.holding_seconds", min_value=0.0)
    mark_price = _optional_float(f.get("mark_price"), "features.mark_price", min_value=0.0)
    decision_id = _optional_nonempty_str(f.get("decision_id"), "features.decision_id")

    if pnl_pct >= tp_pct:
        return ExitFlowDecision(
            action="EXIT",
            reason_code="TAKE_PROFIT",
            summary="목표 수익 도달로 청산",
            reasons=["tp_reached"],
            direction=direction,
            entry_score=entry_score,
            exit_score=exit_score,
            exit_score_threshold=exit_score_threshold,
            trend_strength=trend_strength,
            spread=spread,
            orderbook_imbalance=orderbook_imbalance,
            entry_price=entry_price,
            current_price=current_price,
            target_price=target_price,
            stop_price=stop_price,
            pnl_pct=pnl_pct,
            position_qty=position_qty,
            signal_source=signal_source,
            regime=regime,
            symbol=symbol,
            signal_ts_ms=signal_ts_ms,
            holding_seconds=holding_seconds,
            mark_price=mark_price,
            decision_id=decision_id,
        )

    if pnl_pct <= -sl_pct:
        return ExitFlowDecision(
            action="EXIT",
            reason_code="STOP_LOSS",
            summary="허용 손실 도달로 청산",
            reasons=["sl_reached"],
            direction=direction,
            entry_score=entry_score,
            exit_score=exit_score,
            exit_score_threshold=exit_score_threshold,
            trend_strength=trend_strength,
            spread=spread,
            orderbook_imbalance=orderbook_imbalance,
            entry_price=entry_price,
            current_price=current_price,
            target_price=target_price,
            stop_price=stop_price,
            pnl_pct=pnl_pct,
            position_qty=position_qty,
            signal_source=signal_source,
            regime=regime,
            symbol=symbol,
            signal_ts_ms=signal_ts_ms,
            holding_seconds=holding_seconds,
            mark_price=mark_price,
            decision_id=decision_id,
        )

    reversal = _is_reversal(
        direction=direction,
        trend_strength=trend_strength,
        orderbook_imbalance=orderbook_imbalance,
        trend_reversal_threshold=exit_trend_reversal_threshold,
        orderbook_reversal_threshold=exit_orderbook_reversal_threshold,
    )

    if exit_score >= exit_score_threshold and reversal:
        reasons: List[str] = ["exit_score_threshold_met"]
        if spread > exit_max_spread_pct:
            reasons.append("spread_above_max")
        if direction == "LONG":
            if trend_strength <= exit_trend_reversal_threshold:
                reasons.append("trend_reversal")
            if orderbook_imbalance <= -exit_orderbook_reversal_threshold:
                reasons.append("orderbook_reversal")
        else:
            if trend_strength >= -exit_trend_reversal_threshold:
                reasons.append("trend_reversal")
            if orderbook_imbalance >= exit_orderbook_reversal_threshold:
                reasons.append("orderbook_reversal")

        return ExitFlowDecision(
            action="EXIT",
            reason_code="EXIT_SIGNAL",
            summary="청산 신호 충족으로 포지션 종료",
            reasons=reasons,
            direction=direction,
            entry_score=entry_score,
            exit_score=exit_score,
            exit_score_threshold=exit_score_threshold,
            trend_strength=trend_strength,
            spread=spread,
            orderbook_imbalance=orderbook_imbalance,
            entry_price=entry_price,
            current_price=current_price,
            target_price=target_price,
            stop_price=stop_price,
            pnl_pct=pnl_pct,
            position_qty=position_qty,
            signal_source=signal_source,
            regime=regime,
            symbol=symbol,
            signal_ts_ms=signal_ts_ms,
            holding_seconds=holding_seconds,
            mark_price=mark_price,
            decision_id=decision_id,
        )

    hold_reasons: List[str] = []

    if pnl_pct < tp_pct:
        hold_reasons.append("tp_not_reached")
    if pnl_pct > -sl_pct:
        hold_reasons.append("sl_not_reached")
    if exit_score < exit_score_threshold:
        hold_reasons.append("exit_score_below_threshold")

    if direction == "LONG":
        if trend_strength > exit_trend_reversal_threshold:
            hold_reasons.append("trend_still_supportive")
        if orderbook_imbalance > -exit_orderbook_reversal_threshold:
            hold_reasons.append("orderbook_not_reversed")
    else:
        if trend_strength < -exit_trend_reversal_threshold:
            hold_reasons.append("trend_still_supportive")
        if orderbook_imbalance < exit_orderbook_reversal_threshold:
            hold_reasons.append("orderbook_not_reversed")

    if spread > exit_max_spread_pct:
        hold_reasons.append("spread_high_but_exit_not_confirmed")

    return ExitFlowDecision(
        action="HOLD",
        reason_code="HOLD",
        summary="청산 조건 미충족으로 보유 유지",
        reasons=hold_reasons,
        direction=direction,
        entry_score=entry_score,
        exit_score=exit_score,
        exit_score_threshold=exit_score_threshold,
        trend_strength=trend_strength,
        spread=spread,
        orderbook_imbalance=orderbook_imbalance,
        entry_price=entry_price,
        current_price=current_price,
        target_price=target_price,
        stop_price=stop_price,
        pnl_pct=pnl_pct,
        position_qty=position_qty,
        signal_source=signal_source,
        regime=regime,
        symbol=symbol,
        signal_ts_ms=signal_ts_ms,
        holding_seconds=holding_seconds,
        mark_price=mark_price,
        decision_id=decision_id,
    )


def build_exit_decision_payload_strict(
    *,
    features: Dict[str, Any],
    settings: Any,
) -> Dict[str, Any]:
    decision = evaluate_exit_flow_strict(features=features, settings=settings)

    return {
        "action": decision.action,
        "summary": decision.summary,
        "reasons": list(decision.reasons),
        "entry_score": decision.entry_score,
        "exit_score": decision.exit_score,
        "threshold": decision.exit_score_threshold,
        "trend_strength": decision.trend_strength,
        "spread": decision.spread,
        "orderbook_imbalance": decision.orderbook_imbalance,
        "current_price": decision.current_price,
        "entry_price": decision.entry_price,
        "target_price": decision.target_price,
        "stop_price": decision.stop_price,
        "pnl_pct": decision.pnl_pct,
        "position_qty": decision.position_qty,
        "signal_source": decision.signal_source,
        "reason_code": decision.reason_code,
        "regime": decision.regime,
        "side": decision.direction,
        "symbol": decision.symbol,
        "signal_ts_ms": decision.signal_ts_ms,
        "holding_seconds": decision.holding_seconds,
        "mark_price": decision.mark_price,
        "decision_id": decision.decision_id,
    }


__all__ = [
    "ExitFlowDecision",
    "evaluate_exit_flow_strict",
    "build_exit_decision_payload_strict",
]