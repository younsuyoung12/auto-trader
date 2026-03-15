# strategy/gpt_entry_filter.py
"""
========================================================
FILE: strategy/gpt_entry_filter.py
ROLE:
- ENTRY 신호에 대해 GPT 기반 최종 진입 필터를 수행한다.
- rule-based strategy_signal 을 그대로 실행하지 않고,
  시장 컨텍스트/점수/레짐/오더북 불균형/최근 5m 캔들 tail을 함께 검토해
  ENTER 유지 또는 SKIP veto 를 결정한다.
- GPT 호출 자체는 strategy.gpt_engine 의 단일 진입점(call_chat_json)만 사용한다.

CORE RESPONSIBILITIES:
- strategy_signal / features / settings 계약 strict 검증
- GPT entry filter gating(entry_score 기반 호출 최적화)
- compact user_payload 구성
- GPT JSON verdict strict 검증
- verdict 를 Signal 계약(ENTER/SKIP, LONG/SHORT, meta)으로 정규화
- GPT 판단 메타를 signal.meta 에 보존

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- 이 파일은 주문 실행 / 거래소 API 호출 / DB 접근을 하지 않는다.
- GPT transport 는 strategy.gpt_engine.call_chat_json()만 사용한다.
- GPT 실패/파싱 실패/계약 불일치는 즉시 예외 처리한다.
- GPT verdict 는 ENTER 또는 SKIP 만 허용한다.
- direction 은 기존 strategy_signal.direction 을 절대 바꾸지 않는다.
- risk/tp/sl 은 이 필터에서 임의 보정하지 않는다.
- settings 누락/불일치 시 즉시 예외 처리한다.
- hidden default / silent continue / silent bypass 금지.
========================================================

CHANGE HISTORY:
- 2026-03-15
  1) ADD(ARCH): ENTRY 전용 GPT veto layer 신규 추가
  2) ADD(CONTRACT): Signal/features/settings/GPT verdict strict 검증 추가
  3) ADD(OBSERVABILITY): gpt_entry_filter 결과를 signal.meta.gpt_entry_filter 에 보존
  4) ADD(OPERABILITY): entry_score 기반 GPT 호출 gate 추가
  5) KEEP(STRICT): direction/risk/tp/sl 임의 변경 금지, ENTER/SKIP verdict 만 허용
========================================================
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional

from common.exceptions_strict import (
    StrictConfigError,
    StrictDataError,
    StrictStateError,
)
from common.strict_validators import (
    StrictValidationError,
    require_bool as _sv_require_bool,
    require_choice as _sv_require_choice,
    require_float as _sv_require_float,
    require_int as _sv_require_int,
    require_mapping as _sv_require_mapping,
    require_nonempty_str as _sv_require_nonempty_str,
)
from strategy.gpt_engine import GptEngineError, GptJsonResponse, call_chat_json
from strategy.signal import Signal


_ALLOWED_ACTIONS = ("ENTER", "SKIP")
_ALLOWED_DIRECTIONS = ("LONG", "SHORT")
_ALLOWED_POLICY_TAGS = ("gpt_entry_filter_v1",)


class GptEntryFilterConfigError(StrictConfigError):
    """settings 계약 위반."""


class GptEntryFilterContractError(StrictDataError):
    """signal/features/gpt verdict 계약 위반."""


class GptEntryFilterRuntimeError(StrictStateError):
    """GPT 호출/런타임 상태 위반."""


@dataclass(frozen=True, slots=True)
class GptEntryFilterDecision:
    verdict: str
    confidence: float
    reason_code: str
    reason_text: str
    policy_tag: str
    latency_sec: float
    model: str

    def to_meta(self) -> Dict[str, Any]:
        return {
            "enabled": True,
            "verdict": self.verdict,
            "confidence": self.confidence,
            "reason_code": self.reason_code,
            "reason_text": self.reason_text,
            "policy_tag": self.policy_tag,
            "latency_sec": self.latency_sec,
            "model": self.model,
        }


def _raise_contract_from_strict(where: str, exc: StrictValidationError) -> GptEntryFilterContractError:
    return GptEntryFilterContractError(f"{where}: {exc}")


def _raise_config_from_strict(where: str, exc: StrictValidationError) -> GptEntryFilterConfigError:
    return GptEntryFilterConfigError(f"{where}: {exc}")


def _require_mapping(
    value: Any,
    name: str,
    *,
    non_empty: bool,
) -> Dict[str, Any]:
    try:
        mapping_obj = _sv_require_mapping(value, name, non_empty=non_empty)
    except StrictValidationError as exc:
        raise _raise_contract_from_strict(name, exc) from exc
    return dict(mapping_obj)


def _require_nonempty_str(value: Any, name: str) -> str:
    if value is None:
        raise GptEntryFilterContractError(f"{name} is required (STRICT)")
    if not isinstance(value, str):
        raise GptEntryFilterContractError(f"{name} must be str (STRICT), got={type(value).__name__}")
    try:
        return _sv_require_nonempty_str(value, name)
    except StrictValidationError as exc:
        raise _raise_contract_from_strict(name, exc) from exc


def _require_choice(value: Any, name: str, allowed: tuple[str, ...]) -> str:
    if value is None:
        raise GptEntryFilterContractError(f"{name} is required (STRICT)")
    if not isinstance(value, str):
        raise GptEntryFilterContractError(f"{name} must be str (STRICT), got={type(value).__name__}")
    try:
        return _sv_require_choice(value, name, allowed)
    except StrictValidationError as exc:
        raise _raise_contract_from_strict(name, exc) from exc


def _require_bool(value: Any, name: str) -> bool:
    try:
        return _sv_require_bool(value, name)
    except StrictValidationError as exc:
        raise _raise_contract_from_strict(name, exc) from exc


def _require_float(
    value: Any,
    name: str,
    *,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> float:
    try:
        return _sv_require_float(value, name, min_value=min_value, max_value=max_value)
    except StrictValidationError as exc:
        raise _raise_contract_from_strict(name, exc) from exc


def _require_int(
    value: Any,
    name: str,
    *,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
) -> int:
    try:
        iv = _sv_require_int(value, name, min_value=min_value)
    except StrictValidationError as exc:
        raise _raise_contract_from_strict(name, exc) from exc
    if max_value is not None and iv > max_value:
        raise GptEntryFilterContractError(f"{name} must be <= {max_value} (STRICT)")
    return int(iv)


def _read_setting_bool(settings: Any, key: str) -> bool:
    if settings is None:
        raise GptEntryFilterConfigError("settings is required (STRICT)")
    if not hasattr(settings, key):
        raise GptEntryFilterConfigError(f"settings.{key} is required (STRICT)")
    raw = getattr(settings, key)
    try:
        return _sv_require_bool(raw, f"settings.{key}")
    except StrictValidationError as exc:
        raise _raise_config_from_strict(f"settings.{key}", exc) from exc


def _read_setting_float(
    settings: Any,
    key: str,
    *,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> float:
    if settings is None:
        raise GptEntryFilterConfigError("settings is required (STRICT)")
    if not hasattr(settings, key):
        raise GptEntryFilterConfigError(f"settings.{key} is required (STRICT)")
    raw = getattr(settings, key)
    try:
        return _sv_require_float(raw, f"settings.{key}", min_value=min_value, max_value=max_value)
    except StrictValidationError as exc:
        raise _raise_config_from_strict(f"settings.{key}", exc) from exc


def _read_setting_int(
    settings: Any,
    key: str,
    *,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
) -> int:
    if settings is None:
        raise GptEntryFilterConfigError("settings is required (STRICT)")
    if not hasattr(settings, key):
        raise GptEntryFilterConfigError(f"settings.{key} is required (STRICT)")
    raw = getattr(settings, key)
    try:
        iv = _sv_require_int(raw, f"settings.{key}", min_value=min_value)
    except StrictValidationError as exc:
        raise _raise_config_from_strict(f"settings.{key}", exc) from exc
    if max_value is not None and iv > max_value:
        raise GptEntryFilterConfigError(f"settings.{key} must be <= {max_value} (STRICT)")
    return int(iv)


def _normalize_guard_adjustments_strict(value: Any) -> Dict[str, float]:
    raw = _require_mapping(value, "strategy_signal.guard_adjustments", non_empty=False)
    out: Dict[str, float] = {}
    for k, v in raw.items():
        key = _require_nonempty_str(k, f"strategy_signal.guard_adjustments[{k!r}].key")
        out[key] = _require_float(
            v,
            f"strategy_signal.guard_adjustments[{key}]",
        )
    return out


def _validate_signal_contract_or_raise(signal: Any) -> Signal:
    if not isinstance(signal, Signal):
        raise GptEntryFilterContractError(
            f"strategy_signal must be strategy.signal.Signal (STRICT), got={type(signal).__name__}"
        )

    action = _require_choice(signal.action, "strategy_signal.action", _ALLOWED_ACTIONS)
    direction = _require_choice(signal.direction, "strategy_signal.direction", _ALLOWED_DIRECTIONS)
    tp_pct = _require_float(signal.tp_pct, "strategy_signal.tp_pct", min_value=0.0, max_value=1.0)
    sl_pct = _require_float(signal.sl_pct, "strategy_signal.sl_pct", min_value=0.0, max_value=1.0)
    risk_pct = _require_float(signal.risk_pct, "strategy_signal.risk_pct", min_value=0.0, max_value=1.0)
    if tp_pct <= 0.0:
        raise GptEntryFilterContractError("strategy_signal.tp_pct must be > 0 (STRICT)")
    if sl_pct <= 0.0:
        raise GptEntryFilterContractError("strategy_signal.sl_pct must be > 0 (STRICT)")
    if risk_pct <= 0.0:
        raise GptEntryFilterContractError("strategy_signal.risk_pct must be > 0 (STRICT)")

    _ = _require_nonempty_str(signal.reason, "strategy_signal.reason")
    _ = _normalize_guard_adjustments_strict(signal.guard_adjustments)
    _ = _require_mapping(signal.meta, "strategy_signal.meta", non_empty=False)

    return Signal(
        action=action,
        direction=direction,
        tp_pct=float(tp_pct),
        sl_pct=float(sl_pct),
        risk_pct=float(risk_pct),
        reason=signal.reason,
        guard_adjustments=_normalize_guard_adjustments_strict(signal.guard_adjustments),
        meta=_require_mapping(signal.meta, "strategy_signal.meta", non_empty=False),
    )


def _validate_features_contract_or_raise(features: Any) -> Dict[str, Any]:
    f = _require_mapping(features, "features", non_empty=True)

    _ = _require_nonempty_str(f.get("symbol"), "features.symbol")
    _ = _require_nonempty_str(f.get("regime"), "features.regime")
    _ = _require_nonempty_str(f.get("signal_source"), "features.signal_source")
    _ = _require_int(f.get("signal_ts_ms"), "features.signal_ts_ms", min_value=1)
    _ = _require_choice(f.get("direction"), "features.direction", _ALLOWED_DIRECTIONS)
    _ = _require_float(f.get("last_price"), "features.last_price", min_value=0.0)
    _ = _require_float(f.get("entry_score"), "features.entry_score", min_value=0.0, max_value=1.0)
    _ = _require_float(f.get("trend_strength"), "features.trend_strength")
    _ = _require_float(f.get("spread"), "features.spread", min_value=0.0)
    _ = _require_float(f.get("orderbook_imbalance"), "features.orderbook_imbalance")
    _ = _require_float(f.get("equity_current_usdt"), "features.equity_current_usdt", min_value=0.0)
    _ = _require_float(f.get("equity_peak_usdt"), "features.equity_peak_usdt", min_value=0.0)
    _ = _require_float(f.get("dd_pct"), "features.dd_pct", min_value=0.0, max_value=100.0)

    candles_5m_raw = f.get("candles_5m_raw")
    if not isinstance(candles_5m_raw, list) or not candles_5m_raw:
        raise GptEntryFilterContractError("features.candles_5m_raw must be non-empty list (STRICT)")

    return f


def _build_candle_tail_payload_or_raise(features: Mapping[str, Any], limit: int = 8) -> List[Dict[str, Any]]:
    candles = features.get("candles_5m_raw")
    if not isinstance(candles, list) or not candles:
        raise GptEntryFilterContractError("features.candles_5m_raw must be non-empty list (STRICT)")

    tail = candles[-limit:]
    out: List[Dict[str, Any]] = []

    for idx, row in enumerate(tail):
        if not isinstance(row, (list, tuple)):
            raise GptEntryFilterContractError(
                f"features.candles_5m_raw[{idx}] must be list/tuple (STRICT), got={type(row).__name__}"
            )
        if len(row) < 6:
            raise GptEntryFilterContractError(
                f"features.candles_5m_raw[{idx}] length must be >= 6 (STRICT)"
            )

        out.append(
            {
                "open_time_ms": _require_int(row[0], f"features.candles_5m_raw[{idx}][0]", min_value=1),
                "open": _require_float(row[1], f"features.candles_5m_raw[{idx}][1]", min_value=0.0),
                "high": _require_float(row[2], f"features.candles_5m_raw[{idx}][2]", min_value=0.0),
                "low": _require_float(row[3], f"features.candles_5m_raw[{idx}][3]", min_value=0.0),
                "close": _require_float(row[4], f"features.candles_5m_raw[{idx}][4]", min_value=0.0),
                "volume": _require_float(row[5], f"features.candles_5m_raw[{idx}][5]", min_value=0.0),
            }
        )

    return out


def _compact_engine_scores_optional(engine_scores: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if engine_scores is None:
        return None

    es = _require_mapping(engine_scores, "engine_scores", non_empty=True)
    compact: Dict[str, Any] = {}

    for block_name, block_val in es.items():
        key = _require_nonempty_str(block_name, f"engine_scores[{block_name!r}]")
        block = _require_mapping(block_val, f"engine_scores.{key}", non_empty=True)

        if "score" not in block:
            raise GptEntryFilterContractError(f"engine_scores.{key}.score is required (STRICT)")

        compact[key] = {
            "score": _require_float(
                block["score"],
                f"engine_scores.{key}.score",
                min_value=0.0,
                max_value=100.0,
            )
        }

        if key == "total" and "weights" in block and block["weights"] is not None:
            weights = _require_mapping(block["weights"], "engine_scores.total.weights", non_empty=True)
            compact[key]["weights"] = {
                _require_nonempty_str(weight_name, f"engine_scores.total.weights[{weight_name!r}]"): _require_float(
                    weight_val,
                    f"engine_scores.total.weights[{weight_name}]",
                    min_value=0.0,
                    max_value=1.0,
                )
                for weight_name, weight_val in weights.items()
            }

    return compact


def _build_user_payload_or_raise(
    *,
    features: Dict[str, Any],
    strategy_signal: Signal,
    engine_scores: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    f = _validate_features_contract_or_raise(features)
    s = _validate_signal_contract_or_raise(strategy_signal)

    base_meta = _require_mapping(s.meta, "strategy_signal.meta", non_empty=False)

    payload: Dict[str, Any] = {
        "task": "gpt_entry_filter",
        "symbol": _require_nonempty_str(f.get("symbol"), "features.symbol"),
        "regime": _require_nonempty_str(f.get("regime"), "features.regime"),
        "signal_source": _require_nonempty_str(f.get("signal_source"), "features.signal_source"),
        "signal_ts_ms": _require_int(f.get("signal_ts_ms"), "features.signal_ts_ms", min_value=1),
        "proposed_signal": {
            "action": _require_choice(s.action, "strategy_signal.action", _ALLOWED_ACTIONS),
            "direction": _require_choice(s.direction, "strategy_signal.direction", _ALLOWED_DIRECTIONS),
            "tp_pct": _require_float(s.tp_pct, "strategy_signal.tp_pct", min_value=0.0, max_value=1.0),
            "sl_pct": _require_float(s.sl_pct, "strategy_signal.sl_pct", min_value=0.0, max_value=1.0),
            "risk_pct": _require_float(s.risk_pct, "strategy_signal.risk_pct", min_value=0.0, max_value=1.0),
            "reason": _require_nonempty_str(s.reason, "strategy_signal.reason"),
        },
        "market_context": {
            "entry_score": _require_float(f.get("entry_score"), "features.entry_score", min_value=0.0, max_value=1.0),
            "trend_strength": _require_float(f.get("trend_strength"), "features.trend_strength"),
            "spread": _require_float(f.get("spread"), "features.spread", min_value=0.0),
            "orderbook_imbalance": _require_float(f.get("orderbook_imbalance"), "features.orderbook_imbalance"),
            "last_price": _require_float(f.get("last_price"), "features.last_price", min_value=0.0),
            "equity_current_usdt": _require_float(
                f.get("equity_current_usdt"),
                "features.equity_current_usdt",
                min_value=0.0,
            ),
            "equity_peak_usdt": _require_float(
                f.get("equity_peak_usdt"),
                "features.equity_peak_usdt",
                min_value=0.0,
            ),
            "dd_pct": _require_float(f.get("dd_pct"), "features.dd_pct", min_value=0.0, max_value=100.0),
            "candles_5m_tail": _build_candle_tail_payload_or_raise(f),
        },
        "diagnostics": {
            "decision_summary": base_meta.get("decision_summary"),
            "decision_reasons": base_meta.get("decision_reasons"),
            "entry_score_threshold": base_meta.get("entry_score_threshold"),
            "guard_adjustments": _normalize_guard_adjustments_strict(s.guard_adjustments),
            "engine_scores": _compact_engine_scores_optional(engine_scores),
        },
        "output_contract": {
            "verdict": ["ENTER", "SKIP"],
            "confidence": "float 0..1",
            "reason_code": "non-empty string",
            "reason_text": "non-empty string",
            "policy_tag": "gpt_entry_filter_v1",
        },
    }

    return payload


def _build_system_prompt() -> str:
    return (
        "You are an institutional-grade entry veto filter for a Binance USDT-M futures engine. "
        "You do not place orders. "
        "You only decide whether a pre-approved rule-based entry should remain ENTER or be vetoed to SKIP.\n\n"
        "Decision principles:\n"
        "1) Respect the proposed direction and never change it.\n"
        "2) Use the provided market context, recent 5m candle tail, engine scores, spread, trend strength, "
        "   orderbook imbalance, and regime.\n"
        "3) Prefer SKIP when the setup looks fragile, contradictory, late, exhaustion-prone, "
        "   spread-risky, reversal-prone, or internally inconsistent.\n"
        "4) Prefer ENTER only when the evidence remains aligned and materially supports the proposed entry.\n"
        "5) Do not invent missing data. Use only supplied payload.\n\n"
        "Return exactly one JSON object with:\n"
        "- verdict: ENTER or SKIP\n"
        "- confidence: float between 0 and 1\n"
        "- reason_code: short snake_case string\n"
        "- reason_text: concise professional Korean sentence\n"
        "- policy_tag: gpt_entry_filter_v1"
    )


def _parse_gpt_decision_or_raise(resp: GptJsonResponse) -> GptEntryFilterDecision:
    if not isinstance(resp, GptJsonResponse):
        raise GptEntryFilterRuntimeError(
            f"GptJsonResponse expected (STRICT), got={type(resp).__name__}"
        )

    obj = resp.obj
    if not isinstance(obj, dict) or not obj:
        raise GptEntryFilterContractError("GPT response obj must be non-empty dict (STRICT)")

    verdict = _require_choice(obj.get("verdict"), "gpt_response.verdict", _ALLOWED_ACTIONS)
    confidence = _require_float(
        obj.get("confidence"),
        "gpt_response.confidence",
        min_value=0.0,
        max_value=1.0,
    )
    reason_code = _require_nonempty_str(obj.get("reason_code"), "gpt_response.reason_code")
    reason_text = _require_nonempty_str(obj.get("reason_text"), "gpt_response.reason_text")
    policy_tag = _require_choice(obj.get("policy_tag"), "gpt_response.policy_tag", _ALLOWED_POLICY_TAGS)

    if resp.latency_sec <= 0.0:
        raise GptEntryFilterRuntimeError("GPT response latency_sec must be > 0 (STRICT)")
    if not str(resp.model).strip():
        raise GptEntryFilterRuntimeError("GPT response model missing (STRICT)")

    return GptEntryFilterDecision(
        verdict=verdict,
        confidence=float(confidence),
        reason_code=reason_code,
        reason_text=reason_text,
        policy_tag=policy_tag,
        latency_sec=float(resp.latency_sec),
        model=str(resp.model).strip(),
    )


def _build_signal_with_meta(
    *,
    base_signal: Signal,
    action: str,
    reason: str,
    gpt_meta: Dict[str, Any],
) -> Signal:
    validated = _validate_signal_contract_or_raise(base_signal)
    base_meta = _require_mapping(validated.meta, "strategy_signal.meta", non_empty=False)

    if "gpt_entry_filter" in base_meta:
        raise GptEntryFilterContractError("strategy_signal.meta.gpt_entry_filter already exists (STRICT)")

    merged_meta = dict(base_meta)
    merged_meta["gpt_entry_filter"] = gpt_meta

    return Signal(
        action=_require_choice(action, "signal.action", _ALLOWED_ACTIONS),
        direction=validated.direction,
        tp_pct=validated.tp_pct,
        sl_pct=validated.sl_pct,
        risk_pct=validated.risk_pct,
        reason=_require_nonempty_str(reason, "signal.reason"),
        guard_adjustments=_normalize_guard_adjustments_strict(validated.guard_adjustments),
        meta=merged_meta,
    )


def apply_gpt_entry_filter_strict(
    *,
    features: Dict[str, Any],
    strategy_signal: Signal,
    settings: Any,
    engine_scores: Optional[Dict[str, Any]] = None,
) -> Signal:
    """
    ENTRY 신호에 GPT 필터를 적용한다.

    호출 규약
    - strategy_signal.action == SKIP 이면 GPT 호출 없이 그대로 반환
    - settings.gpt_entry_enabled == False 이면 GPT 호출 없이 meta만 남기고 그대로 반환
    - entry_score < settings.gpt_entry_min_entry_score 이면 GPT 호출 gate bypass
    - 그 외에는 GPT JSON verdict 를 strict 검증 후 ENTER/SKIP 으로 정규화
    """
    f = _validate_features_contract_or_raise(features)
    s = _validate_signal_contract_or_raise(strategy_signal)

    if s.action == "SKIP":
        return s

    enabled = _read_setting_bool(settings, "gpt_entry_enabled")
    gate_min_entry_score = _read_setting_float(
        settings,
        "gpt_entry_min_entry_score",
        min_value=0.0,
        max_value=1.0,
    )
    max_tokens = _read_setting_int(
        settings,
        "gpt_entry_max_tokens",
        min_value=1,
        max_value=4096,
    )
    timeout_sec = _read_setting_float(
        settings,
        "gpt_entry_timeout_sec",
        min_value=0.001,
        max_value=300.0,
    )

    entry_score = _require_float(
        f.get("entry_score"),
        "features.entry_score",
        min_value=0.0,
        max_value=1.0,
    )

    if not enabled:
        return _build_signal_with_meta(
            base_signal=s,
            action="ENTER",
            reason=s.reason,
            gpt_meta={
                "enabled": False,
                "status": "DISABLED",
                "reason": "settings.gpt_entry_enabled_false",
            },
        )

    if entry_score < gate_min_entry_score:
        return _build_signal_with_meta(
            base_signal=s,
            action="ENTER",
            reason=s.reason,
            gpt_meta={
                "enabled": True,
                "status": "BYPASS",
                "reason": "entry_score_below_gpt_gate",
                "gate_min_entry_score": gate_min_entry_score,
                "entry_score": entry_score,
            },
        )

    user_payload = _build_user_payload_or_raise(
        features=f,
        strategy_signal=s,
        engine_scores=engine_scores,
    )

    try:
        raw_resp = call_chat_json(
            system_prompt=_build_system_prompt(),
            user_payload=user_payload,
            max_tokens=max_tokens,
            max_latency_sec=timeout_sec,
        )
    except GptEngineError as exc:
        raise GptEntryFilterRuntimeError(f"gpt entry filter call failed (STRICT): {exc}") from exc
    except Exception as exc:
        raise GptEntryFilterRuntimeError(f"unexpected gpt entry filter failure (STRICT): {exc}") from exc

    decision = _parse_gpt_decision_or_raise(raw_resp)
    meta = decision.to_meta()

    if decision.verdict == "SKIP":
        return _build_signal_with_meta(
            base_signal=s,
            action="SKIP",
            reason=decision.reason_text,
            gpt_meta=meta,
        )

    return _build_signal_with_meta(
        base_signal=s,
        action="ENTER",
        reason=s.reason,
        gpt_meta=meta,
    )


__all__ = [
    "GptEntryFilterConfigError",
    "GptEntryFilterContractError",
    "GptEntryFilterRuntimeError",
    "GptEntryFilterDecision",
    "apply_gpt_entry_filter_strict",
]