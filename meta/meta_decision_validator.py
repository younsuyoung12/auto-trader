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
- 2026-03-13:
  1) FIX(EXCEPTION): common.exceptions_strict 공통 예외 계층 적용
  2) FEAT(CONTRACT): MetaDecisionContext / ValidatedMetaDecision dataclass 자체 검증 추가
  3) FIX(STRICT): contract 오류와 state 불변식 오류 분리
  4) FIX(CONTRACT): guard_multipliers / tags / disable_directions 검증 강화
- 2026-03-11:
  1) FIX(STRICT): RECOMMEND_SAFE_STOP action 불변식 강화
     - allocation_ratio_cap=0.0 강제
     - disable_directions=[LONG, SHORT] 강제
     - risk_multiplier_delta>0 금지
  2) FIX(STRICT): ADJUST_PARAMS에서 양방향 비활성화 금지
  3) FIX(STRICT): tags 중복/공백/과다 길이 검증 추가
  4) FIX(CONTRACT): version 문자열 형식 검증 추가
- 2026-03-03:
  1) 신규 생성: Meta Strategy(레벨3) GPT 응답 검증기
  2) 스키마 고정(unknown key 금지) + 범위 검증 + 금지 행위 검증
  3) base 값과 합산 결과가 범위를 벗어나면 즉시 예외(클램프 금지)
========================================================
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

from common.exceptions_strict import StrictDataError, StrictStateError


class MetaDecisionValidationError(RuntimeError):
    """STRICT meta decision validation base error."""


class MetaDecisionContractError(StrictDataError):
    """GPT 응답/컨텍스트/결과 객체 계약 위반."""


class MetaDecisionStateError(StrictStateError):
    """액션별 불변식/정책 상태 위반."""


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

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _normalize_symbol_strict(self.symbol, "ctx.symbol"))
        object.__setattr__(
            self,
            "base_risk_multiplier",
            _require_float(self.base_risk_multiplier, "ctx.base_risk_multiplier", min_value=0.0, max_value=1.0),
        )
        object.__setattr__(
            self,
            "base_allocation_ratio",
            _require_float(self.base_allocation_ratio, "ctx.base_allocation_ratio", min_value=0.0, max_value=1.0),
        )
        object.__setattr__(
            self,
            "base_max_spread_pct",
            _require_float(self.base_max_spread_pct, "ctx.base_max_spread_pct", min_value=0.0),
        )
        object.__setattr__(
            self,
            "base_max_price_jump_pct",
            _require_float(self.base_max_price_jump_pct, "ctx.base_max_price_jump_pct", min_value=0.0),
        )
        object.__setattr__(
            self,
            "base_min_entry_volume_ratio",
            _require_float(self.base_min_entry_volume_ratio, "ctx.base_min_entry_volume_ratio", min_value=0.0, max_value=1.0),
        )
        object.__setattr__(
            self,
            "max_rationale_sentences",
            _require_int(self.max_rationale_sentences, "ctx.max_rationale_sentences", min_value=1, max_value=10),
        )
        object.__setattr__(
            self,
            "max_rationale_len",
            _require_int(self.max_rationale_len, "ctx.max_rationale_len", min_value=1, max_value=5000),
        )


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

    def __post_init__(self) -> None:
        version = _require_version_str(self.version, "validated.version")
        action = _require_nonempty_str(self.action, "validated.action").upper()
        if action not in _ALLOWED_ACTIONS:
            _fail_contract(f"validated.action invalid (STRICT): {action!r}")

        severity = _require_int(self.severity, "validated.severity", min_value=0, max_value=3)
        confidence = _require_float(self.confidence, "validated.confidence", min_value=0.0, max_value=1.0)
        ttl_sec = _require_int(self.ttl_sec, "validated.ttl_sec", min_value=60, max_value=86400)

        risk_delta = _require_float(
            self.risk_multiplier_delta,
            "validated.risk_multiplier_delta",
            min_value=-0.50,
            max_value=0.50,
        )
        allocation_cap = _require_float(
            self.allocation_ratio_cap,
            "validated.allocation_ratio_cap",
            min_value=0.0,
            max_value=1.0,
        )
        rationale = _require_nonempty_str(self.rationale_short, "validated.rationale_short")

        final_risk_multiplier = _require_float(
            self.final_risk_multiplier,
            "validated.final_risk_multiplier",
            min_value=0.0,
            max_value=1.0,
        )
        effective_max_spread_pct = _require_float(
            self.effective_max_spread_pct,
            "validated.effective_max_spread_pct",
            min_value=0.0,
        )
        effective_max_price_jump_pct = _require_float(
            self.effective_max_price_jump_pct,
            "validated.effective_max_price_jump_pct",
            min_value=0.0,
            max_value=1.0,
        )
        effective_min_entry_volume_ratio = _require_float(
            self.effective_min_entry_volume_ratio,
            "validated.effective_min_entry_volume_ratio",
            min_value=0.0,
            max_value=1.0,
        )

        tags = _require_list_str(self.tags, "validated.tags", allow_empty=False)
        if len(tags) > 24:
            _fail_contract("validated.tags too many (STRICT)")

        disable_dirs = _require_list_str(self.disable_directions, "validated.disable_directions", allow_empty=True)
        normalized_dirs: List[str] = []
        for i, d in enumerate(disable_dirs):
            dd = _require_nonempty_str(d, f"validated.disable_directions[{i}]").upper()
            if dd not in _ALLOWED_DIRECTIONS:
                _fail_contract(f"validated.disable_directions[{i}] invalid (STRICT): {dd!r}")
            if dd not in normalized_dirs:
                normalized_dirs.append(dd)

        guard_mults = _require_dict(self.guard_multipliers, "validated.guard_multipliers", non_empty=True)
        _ensure_no_unknown_keys(guard_mults, _GUARD_KEYS, "validated.guard_multipliers")
        for k in _GUARD_KEYS:
            if k not in guard_mults:
                _fail_contract(f"validated.guard_multipliers missing key (STRICT): {k}")
        max_spread_mult = _require_float(
            guard_mults["max_spread_pct_mult"],
            "validated.guard_multipliers.max_spread_pct_mult",
            min_value=0.50,
            max_value=2.00,
        )
        max_jump_mult = _require_float(
            guard_mults["max_price_jump_pct_mult"],
            "validated.guard_multipliers.max_price_jump_pct_mult",
            min_value=0.50,
            max_value=2.00,
        )
        min_vol_mult = _require_float(
            guard_mults["min_entry_volume_ratio_mult"],
            "validated.guard_multipliers.min_entry_volume_ratio_mult",
            min_value=0.50,
            max_value=2.00,
        )

        object.__setattr__(self, "version", version)
        object.__setattr__(self, "action", action)
        object.__setattr__(self, "severity", severity)
        object.__setattr__(self, "confidence", confidence)
        object.__setattr__(self, "ttl_sec", ttl_sec)
        object.__setattr__(self, "risk_multiplier_delta", risk_delta)
        object.__setattr__(self, "allocation_ratio_cap", allocation_cap)
        object.__setattr__(self, "rationale_short", rationale)
        object.__setattr__(self, "final_risk_multiplier", final_risk_multiplier)
        object.__setattr__(self, "effective_max_spread_pct", effective_max_spread_pct)
        object.__setattr__(self, "effective_max_price_jump_pct", effective_max_price_jump_pct)
        object.__setattr__(self, "effective_min_entry_volume_ratio", effective_min_entry_volume_ratio)
        object.__setattr__(self, "tags", tags)
        object.__setattr__(self, "disable_directions", normalized_dirs)
        object.__setattr__(
            self,
            "guard_multipliers",
            {
                "max_spread_pct_mult": max_spread_mult,
                "max_price_jump_pct_mult": max_jump_mult,
                "min_entry_volume_ratio_mult": min_vol_mult,
            },
        )


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

_VERSION_RE = re.compile(r"^[A-Za-z0-9._-]{1,32}$")


def _fail_contract(msg: str, exc: Optional[BaseException] = None) -> None:
    if exc is None:
        raise MetaDecisionContractError(msg)
    raise MetaDecisionContractError(msg) from exc


def _fail_state(msg: str, exc: Optional[BaseException] = None) -> None:
    if exc is None:
        raise MetaDecisionStateError(msg)
    raise MetaDecisionStateError(msg) from exc


def _require_nonempty_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        _fail_contract(f"{name} is required (STRICT)")
    return s


def _normalize_symbol_strict(v: Any, name: str) -> str:
    s = str(v or "").replace("-", "").replace("/", "").upper().strip()
    if not s:
        _fail_contract(f"{name} is required (STRICT)")
    return s


def _require_version_str(v: Any, name: str) -> str:
    s = _require_nonempty_str(v, name)
    if not _VERSION_RE.fullmatch(s):
        _fail_contract(f"{name} format invalid (STRICT)")
    return s


def _require_int(v: Any, name: str, *, min_value: Optional[int] = None, max_value: Optional[int] = None) -> int:
    if v is None:
        _fail_contract(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        _fail_contract(f"{name} must be int (bool not allowed) (STRICT)")
    try:
        iv = int(v)
    except Exception as e:
        _fail_contract(f"{name} must be int (STRICT)", e)
    if min_value is not None and iv < min_value:
        _fail_contract(f"{name} must be >= {min_value} (STRICT)")
    if max_value is not None and iv > max_value:
        _fail_contract(f"{name} must be <= {max_value} (STRICT)")
    return int(iv)


def _require_float(v: Any, name: str, *, min_value: Optional[float] = None, max_value: Optional[float] = None) -> float:
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
    if min_value is not None and fv < min_value:
        _fail_contract(f"{name} must be >= {min_value} (STRICT)")
    if max_value is not None and fv > max_value:
        _fail_contract(f"{name} must be <= {max_value} (STRICT)")
    return float(fv)


def _require_dict(v: Any, name: str, *, non_empty: bool = False) -> Dict[str, Any]:
    if not isinstance(v, dict):
        _fail_contract(f"{name} must be dict (STRICT)")
    if non_empty and not v:
        _fail_contract(f"{name} must be non-empty dict (STRICT)")
    return dict(v)


def _require_list_str(v: Any, name: str, *, allow_empty: bool) -> List[str]:
    if not isinstance(v, list):
        _fail_contract(f"{name} must be list (STRICT)")
    if not allow_empty and not v:
        _fail_contract(f"{name} must be non-empty list (STRICT)")

    out: List[str] = []
    seen: Set[str] = set()

    for i, it in enumerate(v):
        s = _require_nonempty_str(it, f"{name}[{i}]")
        if len(s) > 64:
            _fail_contract(f"{name}[{i}] too long (STRICT)")
        if s in seen:
            _fail_contract(f"{name} contains duplicate value (STRICT): {s!r}")
        seen.add(s)
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
        _fail_contract(f"{where} contains unknown keys (STRICT): {sorted(unknown)}")


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
        _fail_contract("response_obj must be dict (STRICT)")
    if not isinstance(ctx, MetaDecisionContext):
        _fail_contract("ctx must be MetaDecisionContext (STRICT)")

    _ensure_no_unknown_keys(response_obj, _TOP_KEYS, "response")
    for k in _TOP_KEYS:
        if k not in response_obj:
            _fail_contract(f"response missing key (STRICT): {k}")

    version = _require_version_str(response_obj.get("version"), "resp.version")
    action = _require_nonempty_str(response_obj.get("action"), "resp.action").upper()
    if action not in _ALLOWED_ACTIONS:
        _fail_contract(f"resp.action invalid (STRICT): {action!r}")

    severity = _require_int(response_obj.get("severity"), "resp.severity", min_value=0, max_value=3)
    tags = _require_list_str(response_obj.get("tags"), "resp.tags", allow_empty=False)
    if len(tags) > 24:
        _fail_contract("resp.tags too many (STRICT)")

    confidence = _require_float(response_obj.get("confidence"), "resp.confidence", min_value=0.0, max_value=1.0)
    ttl_sec = _require_int(response_obj.get("ttl_sec"), "resp.ttl_sec", min_value=60, max_value=86400)

    rationale = _require_nonempty_str(response_obj.get("rationale_short"), "resp.rationale_short")
    if len(rationale) > ctx.max_rationale_len:
        _fail_contract("resp.rationale_short too long (STRICT)")
    if _count_sentences_rough(rationale) > int(ctx.max_rationale_sentences):
        _fail_contract("resp.rationale_short exceeds sentence limit (STRICT)")

    rec = _require_dict(response_obj.get("recommendation"), "resp.recommendation", non_empty=True)
    _ensure_no_unknown_keys(rec, _REC_KEYS, "recommendation")
    for k in _REC_KEYS:
        if k not in rec:
            _fail_contract(f"recommendation missing key (STRICT): {k}")

    risk_delta = _require_float(
        rec.get("risk_multiplier_delta"),
        "rec.risk_multiplier_delta",
        min_value=-0.50,
        max_value=0.50,
    )
    allocation_cap = _require_float(
        rec.get("allocation_ratio_cap"),
        "rec.allocation_ratio_cap",
        min_value=0.0,
        max_value=1.0,
    )

    disable_raw = rec.get("disable_directions")
    disable_list = _require_list_str(disable_raw, "rec.disable_directions", allow_empty=True)
    disable_dirs: List[str] = []
    for i, d in enumerate(disable_list):
        dd = _require_nonempty_str(d, f"rec.disable_directions[{i}]").upper()
        if dd not in _ALLOWED_DIRECTIONS:
            _fail_contract(f"rec.disable_directions[{i}] invalid (STRICT): {dd!r}")
        if dd not in disable_dirs:
            disable_dirs.append(dd)

    guard_mults = _require_dict(rec.get("guard_multipliers"), "rec.guard_multipliers", non_empty=True)
    _ensure_no_unknown_keys(guard_mults, _GUARD_KEYS, "guard_multipliers")
    for k in _GUARD_KEYS:
        if k not in guard_mults:
            _fail_contract(f"guard_multipliers missing key (STRICT): {k}")

    max_spread_mult = _require_float(
        guard_mults.get("max_spread_pct_mult"),
        "guard.max_spread_pct_mult",
        min_value=0.50,
        max_value=2.00,
    )
    max_jump_mult = _require_float(
        guard_mults.get("max_price_jump_pct_mult"),
        "guard.max_price_jump_pct_mult",
        min_value=0.50,
        max_value=2.00,
    )
    min_vol_mult = _require_float(
        guard_mults.get("min_entry_volume_ratio_mult"),
        "guard.min_entry_volume_ratio_mult",
        min_value=0.50,
        max_value=2.00,
    )

    # Derived values (NO CLAMP)
    final_risk_mult = float(ctx.base_risk_multiplier) + float(risk_delta)
    if final_risk_mult < 0.0 or final_risk_mult > 1.0 or not math.isfinite(final_risk_mult):
        _fail_state("final_risk_multiplier out of range 0..1 (STRICT)")

    eff_spread = float(ctx.base_max_spread_pct) * float(max_spread_mult)
    if eff_spread < 0.0 or not math.isfinite(eff_spread):
        _fail_state("effective_max_spread_pct invalid (STRICT)")

    eff_jump = float(ctx.base_max_price_jump_pct) * float(max_jump_mult)
    if eff_jump < 0.0 or not math.isfinite(eff_jump):
        _fail_state("effective_max_price_jump_pct invalid (STRICT)")
    if eff_jump > 1.0:
        _fail_state("effective_max_price_jump_pct > 1.0 (STRICT)")

    eff_min_vol = float(ctx.base_min_entry_volume_ratio) * float(min_vol_mult)
    if eff_min_vol < 0.0 or not math.isfinite(eff_min_vol):
        _fail_state("effective_min_entry_volume_ratio invalid (STRICT)")
    if eff_min_vol > 1.0:
        _fail_state("effective_min_entry_volume_ratio > 1.0 (STRICT)")

    disabled_both = set(disable_dirs) == {"LONG", "SHORT"}

    if action == "NO_CHANGE":
        if abs(risk_delta) > 1e-12:
            _fail_state("NO_CHANGE requires risk_multiplier_delta=0 (STRICT)")
        if abs(allocation_cap - 1.0) > 1e-12:
            _fail_state("NO_CHANGE requires allocation_ratio_cap=1 (STRICT)")
        if disable_dirs:
            _fail_state("NO_CHANGE requires disable_directions=[] (STRICT)")
        if abs(max_spread_mult - 1.0) > 1e-12:
            _fail_state("NO_CHANGE requires max_spread_pct_mult=1 (STRICT)")
        if abs(max_jump_mult - 1.0) > 1e-12:
            _fail_state("NO_CHANGE requires max_price_jump_pct_mult=1 (STRICT)")
        if abs(min_vol_mult - 1.0) > 1e-12:
            _fail_state("NO_CHANGE requires min_entry_volume_ratio_mult=1 (STRICT)")

    if action == "ADJUST_PARAMS":
        if disabled_both:
            _fail_state("ADJUST_PARAMS cannot disable both LONG and SHORT (STRICT)")

    if action == "RECOMMEND_SAFE_STOP":
        if severity < 2:
            _fail_state("RECOMMEND_SAFE_STOP requires severity>=2 (STRICT)")
        if abs(allocation_cap - 0.0) > 1e-12:
            _fail_state("RECOMMEND_SAFE_STOP requires allocation_ratio_cap=0 (STRICT)")
        if not disabled_both:
            _fail_state("RECOMMEND_SAFE_STOP requires disable_directions=[LONG, SHORT] (STRICT)")
        if risk_delta > 0.0:
            _fail_state("RECOMMEND_SAFE_STOP cannot increase risk_multiplier_delta (STRICT)")

    if disabled_both and action != "RECOMMEND_SAFE_STOP":
        _fail_state("disable_directions=[LONG,SHORT] requires action=RECOMMEND_SAFE_STOP (STRICT)")

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
        _fail_contract("response_text empty (STRICT)")
    if not (s.startswith("{") and s.endswith("}")):
        _fail_contract("response must be a single JSON object only (STRICT)")

    try:
        obj = json.loads(s)
    except Exception as e:
        _fail_contract("json.loads failed (STRICT)", e)

    if not isinstance(obj, dict):
        _fail_contract("response json root must be object (STRICT)")
    return validate_meta_decision_dict(response_obj=obj, ctx=ctx)


__all__ = [
    "MetaDecisionValidationError",
    "MetaDecisionContractError",
    "MetaDecisionStateError",
    "MetaDecisionContext",
    "ValidatedMetaDecision",
    "validate_meta_decision_dict",
    "validate_meta_decision_text",
]