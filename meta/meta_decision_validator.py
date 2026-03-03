# meta/meta_decision_validator.py
"""
========================================================
FILE: meta/meta_decision_validator.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할
- Meta Strategy(레벨3) GPT 응답(JSON)을 “적용 가능한 추천안”으로 STRICT 검증한다.
- 이 모듈은 검증만 한다. 주문/진입/청산/TP·SL 결정은 절대 하지 않는다.

절대 원칙 (STRICT · NO-FALLBACK)
- 폴백 금지: None→0, 기본값 주입, clamp로 살려서 진행 금지.
- 예외 삼키기 금지: 검증 실패는 즉시 예외.
- 환경 변수 직접 접근 금지(os.getenv 금지). settings.py를 직접 로드하지 않는다.
  (검증에 필요한 현재 값은 호출자가 “스냅샷/컨텍스트”로 전달한다.)
- 민감정보는 예외 메시지에 포함하지 않는다.

변경 이력
--------------------------------------------------------
- 2026-03-03:
  1) 신규 생성: Meta Strategy(레벨3) GPT 응답 검증기
  2) 스키마 고정(unknown key 금지) + 범위 검증 + 금지 행위 검증
  3) base 값과 합산 결과가 범위를 벗어나면 즉시 예외(클램프 금지)
========================================================
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple


class MetaDecisionValidationError(RuntimeError):
    """STRICT: meta decision validation failure."""


# ─────────────────────────────────────────────
# Data models
# ─────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class MetaDecisionContext:
    """
    STRICT 검증 컨텍스트 (호출자가 제공)

    - base_risk_multiplier:
        RiskPhysics 등에서 계산된 “현재” 멀티플라이어(0..1).
        Meta recommendation의 delta는 이 값에 더해진다. (클램프 금지)
    - base_*:
        현재 운영 파라미터의 “기준값”.
        guard multipliers는 base 값에 곱해져 적용될 것을 전제로 검증한다.

    주의:
    - 이 컨텍스트는 민감정보를 포함하면 안 된다.
    """
    symbol: str

    base_risk_multiplier: float  # 0..1

    base_allocation_ratio: float  # 0..1
    base_max_spread_pct: float  # >=0
    base_max_price_jump_pct: float  # >=0
    base_min_entry_volume_ratio: float  # 0..1

    max_rationale_sentences: int = 3
    max_rationale_len: int = 800


@dataclass(frozen=True, slots=True)
class ValidatedMetaDecision:
    version: str
    action: str  # NO_CHANGE | ADJUST_PARAMS | RECOMMEND_SAFE_STOP
    severity: int  # 0..3
    tags: List[str]
    confidence: float  # 0..1
    ttl_sec: int  # 60..86400

    # raw recommendation
    risk_multiplier_delta: float  # -0.50..+0.50
    allocation_ratio_cap: float  # 0..1
    disable_directions: List[str]  # subset of LONG/SHORT
    guard_multipliers: Dict[str, float]  # required keys only

    rationale_short: str

    # derived, STRICT (no clamp)
    final_risk_multiplier: float
    effective_max_spread_pct: float
    effective_max_price_jump_pct: float
    effective_min_entry_volume_ratio: float


# ─────────────────────────────────────────────
# Strict primitives
# ─────────────────────────────────────────────
_ALLOWED_ACTIONS: Set[str] = {"NO_CHANGE", "ADJUST_PARAMS", "RECOMMEND_SAFE_STOP"}
_ALLOWED_DIRECTIONS: Set[str] = {"LONG", "SHORT"}

_TOP_KEYS: Set[str] = {
    "version",
    "action",
    "severity",
    "tags",
    "confidence",
    "ttl_sec",
    "recommendation",
    "rationale_short",
}

_REC_KEYS: Set[str] = {
    "risk_multiplier_delta",
    "allocation_ratio_cap",
    "disable_directions",
    "guard_multipliers",
}

_GUARD_KEYS: Set[str] = {
    "max_spread_pct_mult",
    "max_price_jump_pct_mult",
    "min_entry_volume_ratio_mult",
}


def _fail(msg: str, exc: Optional[BaseException] = None) -> None:
    if exc is None:
        raise MetaDecisionValidationError(msg)
    raise MetaDecisionValidationError(msg) from exc


def _require_nonempty_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        _fail(f"{name} is required (STRICT)")
    return s


def _require_int(v: Any, name: str, *, min_value: Optional[int] = None, max_value: Optional[int] = None) -> int:
    if v is None:
        _fail(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        _fail(f"{name} must be int (bool not allowed) (STRICT)")
    try:
        iv = int(v)
    except Exception as e:
        _fail(f"{name} must be int (STRICT)", e)
    if min_value is not None and iv < min_value:
        _fail(f"{name} must be >= {min_value} (STRICT)")
    if max_value is not None and iv > max_value:
        _fail(f"{name} must be <= {max_value} (STRICT)")
    return int(iv)


def _require_float(v: Any, name: str, *, min_value: Optional[float] = None, max_value: Optional[float] = None) -> float:
    if v is None:
        _fail(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        _fail(f"{name} must be float (bool not allowed) (STRICT)")
    try:
        fv = float(v)
    except Exception as e:
        _fail(f"{name} must be float (STRICT)", e)
    if not math.isfinite(fv):
        _fail(f"{name} must be finite (STRICT)")
    if min_value is not None and fv < min_value:
        _fail(f"{name} must be >= {min_value} (STRICT)")
    if max_value is not None and fv > max_value:
        _fail(f"{name} must be <= {max_value} (STRICT)")
    return float(fv)


def _require_dict(v: Any, name: str, *, non_empty: bool = False) -> Dict[str, Any]:
    if not isinstance(v, dict):
        _fail(f"{name} must be dict (STRICT)")
    if non_empty and not v:
        _fail(f"{name} must be non-empty dict (STRICT)")
    return v


def _require_list_str(v: Any, name: str, *, allow_empty: bool) -> List[str]:
    if not isinstance(v, list):
        _fail(f"{name} must be list (STRICT)")
    if not allow_empty and not v:
        _fail(f"{name} must be non-empty list (STRICT)")
    out: List[str] = []
    for i, it in enumerate(v):
        s = _require_nonempty_str(it, f"{name}[{i}]")
        out.append(s)
    return out


def _count_sentences_rough(text: str) -> int:
    s = str(text).strip()
    if not s:
        return 0
    parts: List[str] = []
    buf = ""
    for ch in s:
        buf += ch
        if ch in ".!?。\n":
            if buf.strip():
                parts.append(buf.strip())
            buf = ""
    if buf.strip():
        parts.append(buf.strip())
    return len(parts)


def _ensure_no_unknown_keys(obj: Dict[str, Any], allowed: Set[str], where: str) -> None:
    unknown = [k for k in obj.keys() if k not in allowed]
    if unknown:
        _fail(f"{where} contains unknown keys (STRICT): {sorted(unknown)}")


# ─────────────────────────────────────────────
# Public validation API
# ─────────────────────────────────────────────
def validate_meta_decision_dict(
    *,
    response_obj: Dict[str, Any],
    ctx: MetaDecisionContext,
) -> ValidatedMetaDecision:
    """
    STRICT: GPT 응답(JSON object)을 검증하고, 적용 가능한 ValidatedMetaDecision으로 반환한다.
    - response_obj는 이미 dict여야 한다(파싱 단계는 상위 레이어에서 수행).
    """

    if not isinstance(response_obj, dict):
        _fail("response_obj must be dict (STRICT)")
    if not isinstance(ctx, MetaDecisionContext):
        _fail("ctx must be MetaDecisionContext (STRICT)")

    # ctx basic validation
    _ = _require_nonempty_str(ctx.symbol, "ctx.symbol")
    _require_float(ctx.base_risk_multiplier, "ctx.base_risk_multiplier", min_value=0.0, max_value=1.0)
    _require_float(ctx.base_allocation_ratio, "ctx.base_allocation_ratio", min_value=0.0, max_value=1.0)
    _require_float(ctx.base_max_spread_pct, "ctx.base_max_spread_pct", min_value=0.0)
    _require_float(ctx.base_max_price_jump_pct, "ctx.base_max_price_jump_pct", min_value=0.0)
    _require_float(ctx.base_min_entry_volume_ratio, "ctx.base_min_entry_volume_ratio", min_value=0.0, max_value=1.0)

    if ctx.max_rationale_sentences <= 0 or ctx.max_rationale_sentences > 10:
        _fail("ctx.max_rationale_sentences must be 1..10 (STRICT)")
    if ctx.max_rationale_len <= 0 or ctx.max_rationale_len > 5000:
        _fail("ctx.max_rationale_len out of range (STRICT)")

    # schema: top-level keys fixed
    _ensure_no_unknown_keys(response_obj, _TOP_KEYS, "response")
    for k in _TOP_KEYS:
        if k not in response_obj:
            _fail(f"response missing key (STRICT): {k}")

    version = _require_nonempty_str(response_obj.get("version"), "resp.version")
    action = _require_nonempty_str(response_obj.get("action"), "resp.action").upper()
    if action not in _ALLOWED_ACTIONS:
        _fail(f"resp.action invalid (STRICT): {action!r}")

    severity = _require_int(response_obj.get("severity"), "resp.severity", min_value=0, max_value=3)
    tags = _require_list_str(response_obj.get("tags"), "resp.tags", allow_empty=False)
    if len(tags) > 24:
        _fail("resp.tags too many (STRICT)")

    confidence = _require_float(response_obj.get("confidence"), "resp.confidence", min_value=0.0, max_value=1.0)
    ttl_sec = _require_int(response_obj.get("ttl_sec"), "resp.ttl_sec", min_value=60, max_value=86400)

    rationale = _require_nonempty_str(response_obj.get("rationale_short"), "resp.rationale_short")
    if len(rationale) > ctx.max_rationale_len:
        _fail("resp.rationale_short too long (STRICT)")
    if _count_sentences_rough(rationale) > int(ctx.max_rationale_sentences):
        _fail("resp.rationale_short exceeds sentence limit (STRICT)")

    rec = _require_dict(response_obj.get("recommendation"), "resp.recommendation", non_empty=True)
    _ensure_no_unknown_keys(rec, _REC_KEYS, "recommendation")
    for k in _REC_KEYS:
        if k not in rec:
            _fail(f"recommendation missing key (STRICT): {k}")

    risk_delta = _require_float(rec.get("risk_multiplier_delta"), "rec.risk_multiplier_delta", min_value=-0.50, max_value=0.50)
    allocation_cap = _require_float(rec.get("allocation_ratio_cap"), "rec.allocation_ratio_cap", min_value=0.0, max_value=1.0)

    disable_raw = rec.get("disable_directions")
    disable_list = _require_list_str(disable_raw, "rec.disable_directions", allow_empty=True)
    disable_dirs: List[str] = []
    for i, d in enumerate(disable_list):
        dd = _require_nonempty_str(d, f"rec.disable_directions[{i}]").upper()
        if dd not in _ALLOWED_DIRECTIONS:
            _fail(f"rec.disable_directions[{i}] invalid (STRICT): {dd!r}")
        if dd not in disable_dirs:
            disable_dirs.append(dd)

    guard_mults = _require_dict(rec.get("guard_multipliers"), "rec.guard_multipliers", non_empty=True)
    _ensure_no_unknown_keys(guard_mults, _GUARD_KEYS, "guard_multipliers")
    for k in _GUARD_KEYS:
        if k not in guard_mults:
            _fail(f"guard_multipliers missing key (STRICT): {k}")

    max_spread_mult = _require_float(guard_mults.get("max_spread_pct_mult"), "guard.max_spread_pct_mult", min_value=0.50, max_value=2.00)
    max_jump_mult = _require_float(guard_mults.get("max_price_jump_pct_mult"), "guard.max_price_jump_pct_mult", min_value=0.50, max_value=2.00)
    min_vol_mult = _require_float(guard_mults.get("min_entry_volume_ratio_mult"), "guard.min_entry_volume_ratio_mult", min_value=0.50, max_value=2.00)

    # Derived values (NO CLAMP)
    final_risk_mult = float(ctx.base_risk_multiplier) + float(risk_delta)
    if final_risk_mult < 0.0 or final_risk_mult > 1.0 or not math.isfinite(final_risk_mult):
        _fail("final_risk_multiplier out of range 0..1 (STRICT)")

    eff_spread = float(ctx.base_max_spread_pct) * float(max_spread_mult)
    if eff_spread < 0.0 or not math.isfinite(eff_spread):
        _fail("effective_max_spread_pct invalid (STRICT)")

    eff_jump = float(ctx.base_max_price_jump_pct) * float(max_jump_mult)
    if eff_jump < 0.0 or not math.isfinite(eff_jump):
        _fail("effective_max_price_jump_pct invalid (STRICT)")
    if eff_jump > 1.0:
        _fail("effective_max_price_jump_pct > 1.0 (STRICT)")

    eff_min_vol = float(ctx.base_min_entry_volume_ratio) * float(min_vol_mult)
    if eff_min_vol < 0.0 or not math.isfinite(eff_min_vol):
        _fail("effective_min_entry_volume_ratio invalid (STRICT)")
    if eff_min_vol > 1.0:
        _fail("effective_min_entry_volume_ratio > 1.0 (STRICT)")

    # Action-specific invariants
    if action == "NO_CHANGE":
        if abs(risk_delta) > 1e-12:
            _fail("NO_CHANGE requires risk_multiplier_delta=0 (STRICT)")
        if abs(allocation_cap - 1.0) > 1e-12:
            _fail("NO_CHANGE requires allocation_ratio_cap=1 (STRICT)")
        if disable_dirs:
            _fail("NO_CHANGE requires disable_directions=[] (STRICT)")
        if abs(max_spread_mult - 1.0) > 1e-12:
            _fail("NO_CHANGE requires max_spread_pct_mult=1 (STRICT)")
        if abs(max_jump_mult - 1.0) > 1e-12:
            _fail("NO_CHANGE requires max_price_jump_pct_mult=1 (STRICT)")
        if abs(min_vol_mult - 1.0) > 1e-12:
            _fail("NO_CHANGE requires min_entry_volume_ratio_mult=1 (STRICT)")

    if action == "RECOMMEND_SAFE_STOP":
        if severity < 2:
            _fail("RECOMMEND_SAFE_STOP requires severity>=2 (STRICT)")

    # If both directions are disabled, it is effectively safe-stop.
    if set(disable_dirs) == {"LONG", "SHORT"}:
        if action != "RECOMMEND_SAFE_STOP":
            _fail("disable_directions=[LONG,SHORT] requires action=RECOMMEND_SAFE_STOP (STRICT)")

    return ValidatedMetaDecision(
        version=version,
        action=action,
        severity=int(severity),
        tags=[str(x).strip() for x in tags],
        confidence=float(confidence),
        ttl_sec=int(ttl_sec),

        risk_multiplier_delta=float(risk_delta),
        allocation_ratio_cap=float(allocation_cap),
        disable_directions=disable_dirs,
        guard_multipliers={
            "max_spread_pct_mult": float(max_spread_mult),
            "max_price_jump_pct_mult": float(max_jump_mult),
            "min_entry_volume_ratio_mult": float(min_vol_mult),
        },

        rationale_short=rationale,

        final_risk_multiplier=float(final_risk_mult),
        effective_max_spread_pct=float(eff_spread),
        effective_max_price_jump_pct=float(eff_jump),
        effective_min_entry_volume_ratio=float(eff_min_vol),
    )


def validate_meta_decision_text(
    *,
    response_text: str,
    ctx: MetaDecisionContext,
) -> ValidatedMetaDecision:
    """
    STRICT convenience:
    - 응답 전체가 단일 JSON object라는 전제에서 파싱+검증까지 수행한다.
    - (추가 텍스트/코드블록/마크다운 금지)
    """
    s = str(response_text or "").strip()
    if not s:
        _fail("response_text empty (STRICT)")
    if not (s.startswith("{") and s.endswith("}")):
        _fail("response must be a single JSON object only (STRICT)")

    try:
        obj = json.loads(s)
    except Exception as e:
        _fail("json.loads failed (STRICT)", e)

    if not isinstance(obj, dict):
        _fail("response json root must be object (STRICT)")
    return validate_meta_decision_dict(response_obj=obj, ctx=ctx)


__all__ = [
    "MetaDecisionValidationError",
    "MetaDecisionContext",
    "ValidatedMetaDecision",
    "validate_meta_decision_dict",
    "validate_meta_decision_text",
]