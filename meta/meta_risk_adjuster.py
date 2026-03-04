# meta/meta_risk_adjuster.py
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

변경 이력
--------------------------------------------------------
- 2026-03-04:
  1) 신규 구현: MetaRecommendation → (allocation_ratio, guard_adjustments) 변환 레이어 추가
  2) 범위/타입 STRICT 검증 + 충돌(conflict) 시 즉시 예외
========================================================
"""

from __future__ import annotations

import math
from typing import Any, Dict

from meta.meta_strategy_engine import MetaRecommendation


class MetaApplyError(RuntimeError):
    """Meta recommendation apply failed (STRICT)."""


def _require_float(v: Any, name: str, *, min_value: float | None = None, max_value: float | None = None) -> float:
    if v is None:
        raise MetaApplyError(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        raise MetaApplyError(f"{name} must be float (bool not allowed) (STRICT)")
    try:
        fv = float(v)
    except Exception as e:
        raise MetaApplyError(f"{name} must be float (STRICT)") from e
    if not math.isfinite(fv):
        raise MetaApplyError(f"{name} must be finite (STRICT)")
    if min_value is not None and fv < float(min_value):
        raise MetaApplyError(f"{name} must be >= {min_value} (STRICT)")
    if max_value is not None and fv > float(max_value):
        raise MetaApplyError(f"{name} must be <= {max_value} (STRICT)")
    return float(fv)


def _require_bool(v: Any, name: str) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        raise MetaApplyError(f"{name} is required (STRICT)")
    s = str(v).strip().lower()
    if s in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise MetaApplyError(f"{name} must be bool-like (STRICT)")


def apply_allocation_ratio_from_recommendation(base_allocation_ratio: float, rec: MetaRecommendation) -> float:
    """
    STRICT:
    - base_allocation_ratio: 0..1
    - rec.final_risk_multiplier: 0..1
    - rec.allocation_ratio_cap: 0..1
    - 결과는 0..1, 음수/NaN/Inf 금지.
    """
    base = _require_float(base_allocation_ratio, "base_allocation_ratio", min_value=0.0, max_value=1.0)
    mult = _require_float(rec.final_risk_multiplier, "rec.final_risk_multiplier", min_value=0.0, max_value=1.0)
    cap = _require_float(rec.allocation_ratio_cap, "rec.allocation_ratio_cap", min_value=0.0, max_value=1.0)

    adj = base * mult
    if not math.isfinite(adj):
        raise MetaApplyError("computed allocation_ratio is not finite (STRICT)")
    if adj < 0.0:
        raise MetaApplyError("computed allocation_ratio < 0 (STRICT)")
    if adj > cap:
        adj = cap

    if adj < 0.0 or adj > 1.0 or not math.isfinite(adj):
        raise MetaApplyError("final allocation_ratio out of range (STRICT)")
    return float(adj)


def build_guard_adjustments_from_recommendation(settings: Any, rec: MetaRecommendation) -> Dict[str, float]:
    """
    STRICT:
    - settings에 실제 가드가 참조하는 키가 존재해야 한다.
    - Recommendation의 effective_* 값을 그대로 override 값으로 사용한다.
    """
    required_keys = ("max_spread_pct", "max_price_jump_pct", "min_entry_volume_ratio")
    for k in required_keys:
        if not hasattr(settings, k):
            raise MetaApplyError(f"settings missing required guard key: {k} (STRICT)")

    max_spread_pct = _require_float(rec.effective_max_spread_pct, "rec.effective_max_spread_pct", min_value=0.0)
    max_price_jump_pct = _require_float(rec.effective_max_price_jump_pct, "rec.effective_max_price_jump_pct", min_value=0.0, max_value=1.0)
    min_entry_volume_ratio = _require_float(rec.effective_min_entry_volume_ratio, "rec.effective_min_entry_volume_ratio", min_value=0.0, max_value=1.0)

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
    _ = _require_float(base_allocation_ratio, "base_allocation_ratio", min_value=0.0, max_value=1.0)
    _ = _require_float(final_allocation_ratio, "final_allocation_ratio", min_value=0.0, max_value=1.0)

    return {
        "version": str(rec.version),
        "action": str(rec.action),
        "severity": int(rec.severity),
        "tags": list(rec.tags),
        "confidence": float(rec.confidence),
        "ttl_sec": int(rec.ttl_sec),
        "risk_multiplier_delta": float(rec.risk_multiplier_delta),
        "allocation_ratio_cap": float(rec.allocation_ratio_cap),
        "disable_directions": list(rec.disable_directions),
        "guard_multipliers": dict(rec.guard_multipliers),
        "final_risk_multiplier": float(rec.final_risk_multiplier),
        "effective": {
            "max_spread_pct": float(rec.effective_max_spread_pct),
            "max_price_jump_pct": float(rec.effective_max_price_jump_pct),
            "min_entry_volume_ratio": float(rec.effective_min_entry_volume_ratio),
        },
        "base_allocation_ratio": float(base_allocation_ratio),
        "final_allocation_ratio": float(final_allocation_ratio),
        "stats_generated_at_utc": str(rec.stats_generated_at_utc),
        "model": str(rec.model),
        "latency_sec": float(rec.latency_sec),
        "rationale_short": str(rec.rationale_short),
    }


__all__ = [
    "MetaApplyError",
    "apply_allocation_ratio_from_recommendation",
    "build_guard_adjustments_from_recommendation",
    "build_meta_l3_audit_blob",
]
