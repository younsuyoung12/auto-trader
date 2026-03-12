"""
========================================================
FILE: meta/meta_risk_adjuster.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할
- Meta Strategy(레벨3) Recommendation을 "적용 가능한 오버레이"로 변환한다.
- 이 모듈은 주문/진입/청산 결정을 하지 않는다.
  (적용 결과는 호출자가 실행 엔진/전략 레이어에 반영한다.)

절대 원칙 (STRICT · NO-FALLBACK)
- 폴백 금지: None→0/기본값 주입/조용한 진행 금지.
- 예외 삼키기 금지: 검증/적용 실패는 즉시 예외.
- 환경 변수 직접 접근 금지(os.getenv 금지).
- 민감정보를 예외 메시지/로그에 포함하지 않는다.
- 공통 STRICT 예외 계층을 사용한다.

변경 이력
--------------------------------------------------------
- 2026-03-13:
  1) FIX(EXCEPTION): common.exceptions_strict 공통 예외 계층 적용
  2) FIX(CONTRACT): MetaRecommendation 타입/필드 계약 검증 강화
  3) FIX(STATE): final_allocation_ratio / allocation_ratio_cap 정합성 검증 추가
  4) FIX(CONFIG): settings guard key/value 계약 검증 강화
- 2026-03-04:
  1) 신규 구현: MetaRecommendation → (allocation_ratio, guard_adjustments) 변환 레이어 추가
  2) 범위/타입 STRICT 검증 + 충돌(conflict) 시 즉시 예외
========================================================
"""

from __future__ import annotations

import math
from typing import Any, Dict

from common.exceptions_strict import StrictConfigError, StrictDataError, StrictStateError
from meta.meta_strategy_engine import MetaRecommendation


class MetaApplyError(RuntimeError):
    """Meta recommendation apply failed (STRICT)."""


class MetaApplyConfigError(StrictConfigError):
    """Meta apply settings/config 계약 위반."""


class MetaApplyContractError(StrictDataError):
    """Meta recommendation/apply 입력 계약 위반."""


class MetaApplyStateError(StrictStateError):
    """Meta apply 결과 상태 불일치."""


def _fail_config(reason: str, exc: BaseException | None = None) -> None:
    if exc is None:
        raise MetaApplyConfigError(reason)
    raise MetaApplyConfigError(reason) from exc


def _fail_contract(reason: str, exc: BaseException | None = None) -> None:
    if exc is None:
        raise MetaApplyContractError(reason)
    raise MetaApplyContractError(reason) from exc


def _fail_state(reason: str, exc: BaseException | None = None) -> None:
    if exc is None:
        raise MetaApplyStateError(reason)
    raise MetaApplyStateError(reason) from exc


def _require_recommendation(rec: Any) -> MetaRecommendation:
    if not isinstance(rec, MetaRecommendation):
        _fail_contract(f"rec must be MetaRecommendation (STRICT), got={type(rec).__name__}")
    return rec


def _require_float(
    v: Any,
    name: str,
    *,
    min_value: float | None = None,
    max_value: float | None = None,
) -> float:
    if v is None:
        _fail_contract(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        _fail_contract(f"{name} must be float (bool not allowed) (STRICT)")
    try:
        fv = float(v)
    except Exception as e:
        _fail_contract(f"{name} must be float (STRICT)", e)
    if not math.isfinite(fv):
        _fail_contract(f"{name} must be finite (STRICT)")
    if min_value is not None and fv < float(min_value):
        _fail_contract(f"{name} must be >= {min_value} (STRICT)")
    if max_value is not None and fv > float(max_value):
        _fail_contract(f"{name} must be <= {max_value} (STRICT)")
    return float(fv)


def _require_bool(v: Any, name: str) -> bool:
    if not isinstance(v, bool):
        _fail_contract(f"{name} must be bool (STRICT)")
    return bool(v)


def _require_nonempty_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        _fail_contract(f"{name} is required (STRICT)")
    return s


def _require_setting_float(settings: Any, name: str, *, min_value: float | None = None, max_value: float | None = None) -> float:
    if settings is None:
        _fail_config("settings is required (STRICT)")
    if not hasattr(settings, name):
        _fail_config(f"settings.{name} missing (STRICT)")
    return _require_float(getattr(settings, name), f"settings.{name}", min_value=min_value, max_value=max_value)


def _require_recommendation_contract(rec: MetaRecommendation) -> MetaRecommendation:
    r = _require_recommendation(rec)

    _require_nonempty_str(r.version, "rec.version")
    _require_nonempty_str(r.action, "rec.action")
    _require_float(r.confidence, "rec.confidence", min_value=0.0, max_value=1.0)
    _require_float(r.risk_multiplier_delta, "rec.risk_multiplier_delta", min_value=-0.50, max_value=0.50)
    _require_float(r.allocation_ratio_cap, "rec.allocation_ratio_cap", min_value=0.0, max_value=1.0)
    _require_float(r.final_risk_multiplier, "rec.final_risk_multiplier", min_value=0.0, max_value=1.0)
    _require_float(r.effective_max_spread_pct, "rec.effective_max_spread_pct", min_value=0.0)
    _require_float(r.effective_max_price_jump_pct, "rec.effective_max_price_jump_pct", min_value=0.0, max_value=1.0)
    _require_float(r.effective_min_entry_volume_ratio, "rec.effective_min_entry_volume_ratio", min_value=0.0, max_value=1.0)
    _require_nonempty_str(r.rationale_short, "rec.rationale_short")

    if not isinstance(r.tags, list):
        _fail_contract("rec.tags must be list (STRICT)")
    if not isinstance(r.disable_directions, list):
        _fail_contract("rec.disable_directions must be list (STRICT)")
    if not isinstance(r.guard_multipliers, dict):
        _fail_contract("rec.guard_multipliers must be dict (STRICT)")

    for idx, tag in enumerate(r.tags):
        _require_nonempty_str(tag, f"rec.tags[{idx}]")

    for idx, direction in enumerate(r.disable_directions):
        d = _require_nonempty_str(direction, f"rec.disable_directions[{idx}]").upper()
        if d not in ("LONG", "SHORT"):
            _fail_contract(f"rec.disable_directions[{idx}] invalid (STRICT): {d!r}")

    for key, value in r.guard_multipliers.items():
        _require_nonempty_str(key, "rec.guard_multipliers.key")
        _require_float(value, f"rec.guard_multipliers[{key}]", min_value=0.50, max_value=2.00)

    return r


def apply_allocation_ratio_from_recommendation(base_allocation_ratio: float, rec: MetaRecommendation) -> float:
    """
    STRICT:
    - base_allocation_ratio: 0..1
    - rec.final_risk_multiplier: 0..1
    - rec.allocation_ratio_cap: 0..1
    - 결과는 0..1, 음수/NaN/Inf 금지.
    """
    r = _require_recommendation_contract(rec)

    base = _require_float(base_allocation_ratio, "base_allocation_ratio", min_value=0.0, max_value=1.0)
    mult = _require_float(r.final_risk_multiplier, "rec.final_risk_multiplier", min_value=0.0, max_value=1.0)
    cap = _require_float(r.allocation_ratio_cap, "rec.allocation_ratio_cap", min_value=0.0, max_value=1.0)

    adj = base * mult
    if not math.isfinite(adj):
        _fail_state("computed allocation_ratio is not finite (STRICT)")
    if adj < 0.0:
        _fail_state("computed allocation_ratio < 0 (STRICT)")

    if adj > cap:
        adj = cap

    if adj < 0.0 or adj > 1.0 or not math.isfinite(adj):
        _fail_state("final allocation_ratio out of range (STRICT)")
    if adj > cap:
        _fail_state("final allocation_ratio exceeds allocation_ratio_cap (STRICT)")

    return float(adj)


def build_guard_adjustments_from_recommendation(settings: Any, rec: MetaRecommendation) -> Dict[str, float]:
    """
    STRICT:
    - settings에 실제 가드가 참조하는 키가 존재해야 한다.
    - Recommendation의 effective_* 값을 그대로 override 값으로 사용한다.
    """
    r = _require_recommendation_contract(rec)

    _ = _require_setting_float(settings, "max_spread_pct", min_value=0.0)
    _ = _require_setting_float(settings, "max_price_jump_pct", min_value=0.0, max_value=1.0)
    _ = _require_setting_float(settings, "min_entry_volume_ratio", min_value=0.0, max_value=1.0)

    max_spread_pct = _require_float(r.effective_max_spread_pct, "rec.effective_max_spread_pct", min_value=0.0)
    max_price_jump_pct = _require_float(r.effective_max_price_jump_pct, "rec.effective_max_price_jump_pct", min_value=0.0, max_value=1.0)
    min_entry_volume_ratio = _require_float(r.effective_min_entry_volume_ratio, "rec.effective_min_entry_volume_ratio", min_value=0.0, max_value=1.0)

    return {
        "max_spread_pct": float(max_spread_pct),
        "max_price_jump_pct": float(max_price_jump_pct),
        "min_entry_volume_ratio": float(min_entry_volume_ratio),
    }


def build_meta_l3_audit_blob(
    rec: MetaRecommendation,
    *,
    base_allocation_ratio: float,
    final_allocation_ratio: float,
) -> Dict[str, Any]:
    """
    STRICT:
    - DB 스냅샷/이벤트에 저장 가능한 형태의 audit blob을 만든다(민감정보 금지).
    """
    r = _require_recommendation_contract(rec)

    base_ratio = _require_float(base_allocation_ratio, "base_allocation_ratio", min_value=0.0, max_value=1.0)
    final_ratio = _require_float(final_allocation_ratio, "final_allocation_ratio", min_value=0.0, max_value=1.0)

    if final_ratio > float(r.allocation_ratio_cap):
        _fail_state("final_allocation_ratio exceeds rec.allocation_ratio_cap (STRICT)")

    return {
        "version": str(r.version),
        "action": str(r.action),
        "severity": int(r.severity),
        "tags": list(r.tags),
        "confidence": float(r.confidence),
        "ttl_sec": int(r.ttl_sec),
        "risk_multiplier_delta": float(r.risk_multiplier_delta),
        "allocation_ratio_cap": float(r.allocation_ratio_cap),
        "disable_directions": list(r.disable_directions),
        "guard_multipliers": dict(r.guard_multipliers),
        "final_risk_multiplier": float(r.final_risk_multiplier),
        "effective": {
            "max_spread_pct": float(r.effective_max_spread_pct),
            "max_price_jump_pct": float(r.effective_max_price_jump_pct),
            "min_entry_volume_ratio": float(r.effective_min_entry_volume_ratio),
        },
        "base_allocation_ratio": float(base_ratio),
        "final_allocation_ratio": float(final_ratio),
        "stats_generated_at_utc": str(r.stats_generated_at_utc),
        "model": str(r.model),
        "latency_sec": float(r.latency_sec),
        "rationale_short": str(r.rationale_short),
    }


__all__ = [
    "MetaApplyError",
    "MetaApplyConfigError",
    "MetaApplyContractError",
    "MetaApplyStateError",
    "apply_allocation_ratio_from_recommendation",
    "build_guard_adjustments_from_recommendation",
    "build_meta_l3_audit_blob",
]