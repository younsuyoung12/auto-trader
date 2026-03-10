from __future__ import annotations

"""
========================================================
FILE: execution/risk_physics_engine.py
ROLE:
- Regime allocation + AccountState + Signal + AutoBlock 결과를 통합해
  최종 계좌 투입 비율(allocation)과 행동(ENTER/SKIP/STOP)을 결정한다.

CORE RESPONSIBILITIES:
- regime_allocation / dd_pct / consecutive_losses / planned_rr STRICT 검증
- DD / 연속 손실 / 최소 RR 정책 기반 allocation 조정
- auto_block_engine overlay 적용
- 최종 ENTER / SKIP / STOP 의사결정 반환
- audit 가능한 decision contract 보장

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- 입력 누락/형식 오류/범위 이탈 시 즉시 예외
- 임의 보정 금지, 정책 기반 명시적 보정만 허용
- 외부 I/O 금지(DB/네트워크 금지). caller가 모든 입력을 공급한다.
- decision 반환 계약(action_override / allocation_used / reason / audit fields) 일관성 유지

CHANGE HISTORY:
- 2026-03-10:
  1) FIX(STRICT): policy multiplier monotonicity(dd_reduce_2_mult <= dd_reduce_1_mult) 검증 추가
  2) FIX(ARCH): STOP/SKIP/ENTER 반환 로직을 _build_decision_strict()로 단일화
  3) FIX(CONTRACT): heatmap_status=NOT_READY 일 때 heatmap_n=0 / heatmap_ev=None 계약 강화
  4) ADD(OBSERVABILITY): decision audit fields(dd_pct / consecutive_losses / regime_allocation_input) 추가
  5) FIX(STRICT): action_override 값 검증 및 reason 생성 경로 일원화
========================================================
"""

import math
from dataclasses import dataclass
from typing import Any, List, Literal, Optional

from risk.auto_block_engine import AutoBlockDecision, AutoBlockError, decide_auto_block


# ─────────────────────────────────────────────
# Exceptions (STRICT)
# ─────────────────────────────────────────────
class RiskPhysicsError(RuntimeError):
    """Base error for risk physics engine."""


class RiskPhysicsInputError(RiskPhysicsError):
    """Raised when inputs are missing or invalid."""


# ─────────────────────────────────────────────
# Types / Data structures
# ─────────────────────────────────────────────
ActionOverride = Literal["ENTER", "SKIP", "STOP"]


@dataclass(frozen=True, slots=True)
class RiskPhysicsPolicy:
    # DD thresholds (percent)
    dd_reduce_1_pct: float = 5.0
    dd_reduce_1_mult: float = 0.8

    dd_reduce_2_pct: float = 10.0
    dd_reduce_2_mult: float = 0.5

    dd_block_entry_pct: float = 15.0
    dd_stop_engine_pct: float = 20.0

    # Consecutive losses
    consec_reduce_n: int = 2
    consec_reduce_mult: float = 0.7
    consec_block_n: int = 3

    # Minimum planned RR for entry
    min_planned_rr: float = 1.6

    # Allocation caps (전액 배분형)
    max_allocation: float = 1.0
    min_allocation_if_enter: float = 0.0001  # prevent "ENTER with ~0"


@dataclass(frozen=True, slots=True)
class RiskPhysicsDecision:
    # "ENTER" / "SKIP" / "STOP"
    action_override: ActionOverride
    effective_risk_pct: float  # "계좌 투입 비율"로 사용
    allocation_used: float
    planned_rr: float
    reason: str

    # TRADE-GRADE: audit fields
    auto_blocked: bool = False
    auto_risk_multiplier: float = 1.0
    micro_score_risk: Optional[float] = None
    heatmap_status: Optional[str] = None
    heatmap_ev: Optional[float] = None
    heatmap_n: Optional[int] = None

    # Additional audit fields
    dd_pct: Optional[float] = None
    consecutive_losses: Optional[int] = None
    regime_allocation_input: Optional[float] = None


# ─────────────────────────────────────────────
# Helpers (STRICT)
# ─────────────────────────────────────────────
def _as_float(v: Any, name: str, *, min_value: Optional[float] = None, max_value: Optional[float] = None) -> float:
    if isinstance(v, bool):
        raise RiskPhysicsInputError(f"{name} must be numeric (bool not allowed)")
    try:
        x = float(v)
    except Exception as e:
        raise RiskPhysicsInputError(f"{name} must be a number") from e
    if not math.isfinite(x):
        raise RiskPhysicsInputError(f"{name} must be finite")
    if min_value is not None and x < min_value:
        raise RiskPhysicsInputError(f"{name} must be >= {min_value}")
    if max_value is not None and x > max_value:
        raise RiskPhysicsInputError(f"{name} must be <= {max_value}")
    return x


def _as_int(v: Any, name: str, *, min_value: Optional[int] = None) -> int:
    if isinstance(v, bool):
        raise RiskPhysicsInputError(f"{name} must be int (bool not allowed)")
    try:
        x = int(v)
    except Exception as e:
        raise RiskPhysicsInputError(f"{name} must be int") from e
    if min_value is not None and x < min_value:
        raise RiskPhysicsInputError(f"{name} must be >= {min_value}")
    return x


def _require_ratio_0_1(v: Any, name: str) -> float:
    return _as_float(v, name, min_value=0.0, max_value=1.0)


def _require_nonempty_status(v: Any, name: str) -> str:
    s = str(v).strip().upper()
    if not s:
        raise RiskPhysicsInputError(f"{name} is empty")
    return s


def _compute_planned_rr(tp_pct: Any, sl_pct: Any) -> float:
    tp = _as_float(tp_pct, "signal.tp_pct", min_value=0.0)
    sl = _as_float(sl_pct, "signal.sl_pct", min_value=0.0)
    if tp <= 0.0 or sl <= 0.0:
        raise RiskPhysicsInputError("signal.tp_pct and signal.sl_pct must be > 0 for planned_rr")
    rr = tp / sl
    if not math.isfinite(rr) or rr <= 0.0:
        raise RiskPhysicsInputError(f"planned_rr invalid: {rr}")
    return rr


def _validate_auto_block_decision_strict(abd: AutoBlockDecision) -> None:
    if not isinstance(abd.block_entry, bool):
        raise RiskPhysicsError("auto_block_engine.block_entry must be bool (STRICT)")

    _ = _as_float(
        abd.risk_multiplier,
        "auto_block_engine.risk_multiplier",
        min_value=0.0,
        max_value=1.0,
    )

    if not isinstance(abd.reasons, list):
        raise RiskPhysicsError("auto_block_engine.reasons must be list (STRICT)")
    for i, reason in enumerate(abd.reasons):
        s = str(reason).strip()
        if not s:
            raise RiskPhysicsError(f"auto_block_engine.reasons[{i}] must be non-empty str (STRICT)")


def _compose_reason_strict(*parts: str) -> str:
    out: List[str] = []
    for p in parts:
        s = str(p).strip()
        if s:
            out.append(s)
    if not out:
        raise RiskPhysicsError("reason parts empty (STRICT)")
    return "|".join(out)


def _validate_action_override_strict(v: Any) -> ActionOverride:
    s = str(v).strip().upper()
    if s not in ("ENTER", "SKIP", "STOP"):
        raise RiskPhysicsError(f"invalid action_override (STRICT): {v!r}")
    return s  # type: ignore[return-value]


def _build_decision_strict(
    *,
    action_override: ActionOverride,
    effective_risk_pct: float,
    allocation_used: float,
    planned_rr: float,
    reason: str,
    auto_blocked: bool,
    auto_risk_multiplier: float,
    micro_score_risk: float,
    heatmap_status: str,
    heatmap_ev: Optional[float],
    heatmap_n: int,
    dd_pct: float,
    consecutive_losses: int,
    regime_allocation_input: float,
) -> RiskPhysicsDecision:
    action = _validate_action_override_strict(action_override)
    eff = _as_float(effective_risk_pct, "effective_risk_pct", min_value=0.0, max_value=1.0)
    alloc_used = _as_float(allocation_used, "allocation_used", min_value=0.0, max_value=1.0)
    rr = _as_float(planned_rr, "planned_rr", min_value=0.0)
    auto_mult = _as_float(auto_risk_multiplier, "auto_risk_multiplier", min_value=0.0, max_value=1.0)
    msr = _as_float(micro_score_risk, "micro_score_risk", min_value=0.0, max_value=100.0)
    st = _require_nonempty_status(heatmap_status, "heatmap_status")
    dd = _as_float(dd_pct, "dd_pct", min_value=0.0, max_value=100.0)
    consec = _as_int(consecutive_losses, "consecutive_losses", min_value=0)
    reg_alloc = _require_ratio_0_1(regime_allocation_input, "regime_allocation_input")

    reason_s = _compose_reason_strict(reason)

    if action == "ENTER" and eff <= 0.0:
        raise RiskPhysicsError("ENTER decision must have effective_risk_pct > 0 (STRICT)")
    if action in ("SKIP", "STOP") and eff != 0.0:
        raise RiskPhysicsError(f"{action} decision must have effective_risk_pct == 0 (STRICT)")
    if action == "STOP" and alloc_used != 0.0:
        raise RiskPhysicsError("STOP decision must have allocation_used == 0 (STRICT)")

    if st == "NOT_READY":
        if heatmap_ev is not None:
            raise RiskPhysicsError("heatmap_ev must be None when heatmap_status=NOT_READY (STRICT)")
        if heatmap_n != 0:
            raise RiskPhysicsError("heatmap_n must be 0 when heatmap_status=NOT_READY (STRICT)")
        hev: Optional[float] = None
    else:
        if heatmap_ev is None:
            raise RiskPhysicsError("heatmap_ev is required when heatmap_status!=NOT_READY (STRICT)")
        hev = _as_float(heatmap_ev, "heatmap_ev")
        if heatmap_n <= 0:
            raise RiskPhysicsError("heatmap_n must be > 0 when heatmap_status!=NOT_READY (STRICT)")

    return RiskPhysicsDecision(
        action_override=action,
        effective_risk_pct=float(eff),
        allocation_used=float(alloc_used),
        planned_rr=float(rr),
        reason=reason_s,
        auto_blocked=bool(auto_blocked),
        auto_risk_multiplier=float(auto_mult),
        micro_score_risk=float(msr),
        heatmap_status=st,
        heatmap_ev=hev,
        heatmap_n=int(heatmap_n),
        dd_pct=float(dd),
        consecutive_losses=int(consec),
        regime_allocation_input=float(reg_alloc),
    )


# ─────────────────────────────────────────────
# Engine
# ─────────────────────────────────────────────
class RiskPhysicsEngine:
    """
    STRICT · NO-FALLBACK · TRADE-GRADE

    입력(Caller 책임):
    - regime_allocation: 레짐 엔진 allocation (0~1)
    - dd_pct: drawdown percent (0~100)
    - consecutive_losses: int >=0
    - tp_pct / sl_pct: planned RR 계산용
    - micro_score_risk: 0~100 (microstructure_engine 결과)
    - heatmap_status/ev/n: ev_heatmap_engine 결과

    출력:
    - RiskPhysicsDecision(action_override, effective_risk_pct, ...)
      * ENTER: 실행 레이어가 ENTER 진행
      * SKIP : 신규 진입 차단
      * STOP : 엔진 정지 권고(즉시 SAFE_STOP 처리 권장)
    """

    def __init__(self, policy: Optional[RiskPhysicsPolicy] = None) -> None:
        self.policy = policy if policy is not None else RiskPhysicsPolicy()
        self._validate_policy(self.policy)

    @staticmethod
    def _validate_policy(p: RiskPhysicsPolicy) -> None:
        _as_float(p.dd_reduce_1_pct, "policy.dd_reduce_1_pct", min_value=0.0, max_value=100.0)
        _as_float(p.dd_reduce_2_pct, "policy.dd_reduce_2_pct", min_value=0.0, max_value=100.0)
        _as_float(p.dd_block_entry_pct, "policy.dd_block_entry_pct", min_value=0.0, max_value=100.0)
        _as_float(p.dd_stop_engine_pct, "policy.dd_stop_engine_pct", min_value=0.0, max_value=100.0)

        if not (p.dd_reduce_1_pct <= p.dd_reduce_2_pct <= p.dd_block_entry_pct <= p.dd_stop_engine_pct):
            raise ValueError("policy DD thresholds must be non-decreasing")

        _as_float(p.dd_reduce_1_mult, "policy.dd_reduce_1_mult", min_value=0.0, max_value=1.0)
        _as_float(p.dd_reduce_2_mult, "policy.dd_reduce_2_mult", min_value=0.0, max_value=1.0)

        if p.dd_reduce_2_mult > p.dd_reduce_1_mult:
            raise ValueError("policy.dd_reduce_2_mult must be <= policy.dd_reduce_1_mult")

        _as_int(p.consec_reduce_n, "policy.consec_reduce_n", min_value=0)
        _as_int(p.consec_block_n, "policy.consec_block_n", min_value=0)
        if p.consec_reduce_n > p.consec_block_n:
            raise ValueError("policy.consec_reduce_n must be <= policy.consec_block_n")
        _as_float(p.consec_reduce_mult, "policy.consec_reduce_mult", min_value=0.0, max_value=1.0)

        _as_float(p.min_planned_rr, "policy.min_planned_rr", min_value=0.1)

        _as_float(p.max_allocation, "policy.max_allocation", min_value=0.0, max_value=1.0)
        _as_float(
            p.min_allocation_if_enter,
            "policy.min_allocation_if_enter",
            min_value=0.0,
            max_value=1.0,
        )
        if p.min_allocation_if_enter > p.max_allocation:
            raise ValueError("policy.min_allocation_if_enter must be <= policy.max_allocation")

    def decide(
        self,
        *,
        regime_allocation: Any,
        dd_pct: Any,
        consecutive_losses: Any,
        tp_pct: Any,
        sl_pct: Any,
        # TRADE-GRADE inputs (required)
        micro_score_risk: Any,
        heatmap_status: Any,
        heatmap_ev: Any,
        heatmap_n: Any,
    ) -> RiskPhysicsDecision:
        alloc = _require_ratio_0_1(regime_allocation, "regime_allocation")
        dd = _as_float(dd_pct, "dd_pct", min_value=0.0, max_value=100.0)
        consec = _as_int(consecutive_losses, "consecutive_losses", min_value=0)

        rr = _compute_planned_rr(tp_pct, sl_pct)

        msr = _as_float(micro_score_risk, "micro_score_risk", min_value=0.0, max_value=100.0)
        st = _require_nonempty_status(heatmap_status, "heatmap_status")
        if st not in ("NOT_READY", "OK", "BLOCK"):
            raise RiskPhysicsInputError(f"heatmap_status invalid: {st!r}")

        hn = _as_int(heatmap_n, "heatmap_n", min_value=0)

        if st == "NOT_READY":
            if heatmap_ev is not None:
                raise RiskPhysicsInputError("heatmap_ev must be None when heatmap_status=NOT_READY")
            if hn != 0:
                raise RiskPhysicsInputError("heatmap_n must be 0 when heatmap_status=NOT_READY")
            hev: Optional[float] = None
        else:
            if heatmap_ev is None:
                raise RiskPhysicsInputError("heatmap_ev is required when heatmap_status!=NOT_READY")
            if hn <= 0:
                raise RiskPhysicsInputError("heatmap_n must be > 0 when heatmap_status!=NOT_READY")
            hev = _as_float(heatmap_ev, "heatmap_ev")

        # Policy: STOP
        if dd >= self.policy.dd_stop_engine_pct:
            return _build_decision_strict(
                action_override="STOP",
                effective_risk_pct=0.0,
                allocation_used=0.0,
                planned_rr=rr,
                reason=_compose_reason_strict(
                    f"dd_stop_engine(dd_pct={dd:.2f}>= {self.policy.dd_stop_engine_pct:.2f})"
                ),
                auto_blocked=False,
                auto_risk_multiplier=1.0,
                micro_score_risk=msr,
                heatmap_status=st,
                heatmap_ev=hev,
                heatmap_n=hn,
                dd_pct=dd,
                consecutive_losses=consec,
                regime_allocation_input=alloc,
            )

        # Policy: block entry
        if dd >= self.policy.dd_block_entry_pct:
            return _build_decision_strict(
                action_override="SKIP",
                effective_risk_pct=0.0,
                allocation_used=0.0,
                planned_rr=rr,
                reason=_compose_reason_strict(
                    f"dd_block_entry(dd_pct={dd:.2f}>= {self.policy.dd_block_entry_pct:.2f})"
                ),
                auto_blocked=False,
                auto_risk_multiplier=1.0,
                micro_score_risk=msr,
                heatmap_status=st,
                heatmap_ev=hev,
                heatmap_n=hn,
                dd_pct=dd,
                consecutive_losses=consec,
                regime_allocation_input=alloc,
            )

        if consec >= self.policy.consec_block_n and self.policy.consec_block_n > 0:
            return _build_decision_strict(
                action_override="SKIP",
                effective_risk_pct=0.0,
                allocation_used=0.0,
                planned_rr=rr,
                reason=_compose_reason_strict(
                    f"consec_block(consecutive_losses={consec}>= {self.policy.consec_block_n})"
                ),
                auto_blocked=False,
                auto_risk_multiplier=1.0,
                micro_score_risk=msr,
                heatmap_status=st,
                heatmap_ev=hev,
                heatmap_n=hn,
                dd_pct=dd,
                consecutive_losses=consec,
                regime_allocation_input=alloc,
            )

        if rr < self.policy.min_planned_rr:
            return _build_decision_strict(
                action_override="SKIP",
                effective_risk_pct=0.0,
                allocation_used=0.0,
                planned_rr=rr,
                reason=_compose_reason_strict(
                    f"planned_rr_too_low(rr={rr:.3f}< {self.policy.min_planned_rr:.3f})"
                ),
                auto_blocked=False,
                auto_risk_multiplier=1.0,
                micro_score_risk=msr,
                heatmap_status=st,
                heatmap_ev=hev,
                heatmap_n=hn,
                dd_pct=dd,
                consecutive_losses=consec,
                regime_allocation_input=alloc,
            )

        if alloc <= 0.0:
            return _build_decision_strict(
                action_override="SKIP",
                effective_risk_pct=0.0,
                allocation_used=0.0,
                planned_rr=rr,
                reason=_compose_reason_strict("regime_allocation_zero"),
                auto_blocked=False,
                auto_risk_multiplier=1.0,
                micro_score_risk=msr,
                heatmap_status=st,
                heatmap_ev=hev,
                heatmap_n=hn,
                dd_pct=dd,
                consecutive_losses=consec,
                regime_allocation_input=alloc,
            )

        # Apply DD multipliers
        alloc_adj = float(alloc)
        dd_reason = "dd_ok"
        if dd >= self.policy.dd_reduce_2_pct:
            alloc_adj *= float(self.policy.dd_reduce_2_mult)
            dd_reason = "dd_reduce_2"
        elif dd >= self.policy.dd_reduce_1_pct:
            alloc_adj *= float(self.policy.dd_reduce_1_mult)
            dd_reason = "dd_reduce_1"

        # Apply consecutive loss multiplier
        consec_reason = "consec_ok"
        if consec >= self.policy.consec_reduce_n and self.policy.consec_reduce_n > 0:
            alloc_adj *= float(self.policy.consec_reduce_mult)
            consec_reason = "consec_reduce"

        if alloc_adj < 0.0 or alloc_adj > 1.0 or not math.isfinite(alloc_adj):
            raise RiskPhysicsError(f"allocation_used invalid after policy adjustments: {alloc_adj}")

        # TRADE-GRADE: AutoBlock overlay
        try:
            abd: AutoBlockDecision = decide_auto_block(
                micro_score_risk=float(msr),
                heatmap_status=str(st),
                heatmap_ev=hev,
                heatmap_n=int(hn),
                consecutive_losses=int(consec),
                dd_pct=float(dd),
            )
        except AutoBlockError as e:
            raise RiskPhysicsError(f"auto_block_engine failed: {e}") from e

        _validate_auto_block_decision_strict(abd)

        auto_reason = "auto_ok"
        if abd.reasons:
            auto_reason = f"auto_reasons={','.join(abd.reasons)}"

        if abd.block_entry:
            return _build_decision_strict(
                action_override="SKIP",
                effective_risk_pct=0.0,
                allocation_used=0.0,
                planned_rr=rr,
                reason=_compose_reason_strict(
                    f"auto_block({','.join(abd.reasons)})",
                    dd_reason,
                    consec_reason,
                ),
                auto_blocked=True,
                auto_risk_multiplier=float(abd.risk_multiplier),
                micro_score_risk=msr,
                heatmap_status=st,
                heatmap_ev=hev,
                heatmap_n=hn,
                dd_pct=dd,
                consecutive_losses=consec,
                regime_allocation_input=alloc,
            )

        alloc_adj *= float(abd.risk_multiplier)

        if alloc_adj < 0.0 or alloc_adj > 1.0 or not math.isfinite(alloc_adj):
            raise RiskPhysicsError(f"allocation invalid after auto_block multiplier: {alloc_adj}")

        # Explicit cap policy
        if alloc_adj > self.policy.max_allocation:
            alloc_adj = float(self.policy.max_allocation)

        if alloc_adj <= 0.0:
            return _build_decision_strict(
                action_override="SKIP",
                effective_risk_pct=0.0,
                allocation_used=float(alloc_adj),
                planned_rr=rr,
                reason=_compose_reason_strict(
                    "effective_allocation_zero_after_adjustments",
                    dd_reason,
                    consec_reason,
                    auto_reason,
                ),
                auto_blocked=False,
                auto_risk_multiplier=float(abd.risk_multiplier),
                micro_score_risk=msr,
                heatmap_status=st,
                heatmap_ev=hev,
                heatmap_n=hn,
                dd_pct=dd,
                consecutive_losses=consec,
                regime_allocation_input=alloc,
            )

        if alloc_adj < self.policy.min_allocation_if_enter:
            return _build_decision_strict(
                action_override="SKIP",
                effective_risk_pct=0.0,
                allocation_used=float(alloc_adj),
                planned_rr=rr,
                reason=_compose_reason_strict(
                    f"effective_allocation_below_min(alloc={alloc_adj:.6f}< {self.policy.min_allocation_if_enter:.6f})",
                    dd_reason,
                    consec_reason,
                    auto_reason,
                ),
                auto_blocked=False,
                auto_risk_multiplier=float(abd.risk_multiplier),
                micro_score_risk=msr,
                heatmap_status=st,
                heatmap_ev=hev,
                heatmap_n=hn,
                dd_pct=dd,
                consecutive_losses=consec,
                regime_allocation_input=alloc,
            )

        return _build_decision_strict(
            action_override="ENTER",
            effective_risk_pct=float(alloc_adj),
            allocation_used=float(alloc_adj),
            planned_rr=rr,
            reason=_compose_reason_strict(
                "ok",
                dd_reason,
                consec_reason,
                auto_reason,
            ),
            auto_blocked=False,
            auto_risk_multiplier=float(abd.risk_multiplier),
            micro_score_risk=msr,
            heatmap_status=st,
            heatmap_ev=hev,
            heatmap_n=hn,
            dd_pct=dd,
            consecutive_losses=consec,
            regime_allocation_input=alloc,
        )


__all__ = [
    "RiskPhysicsError",
    "RiskPhysicsInputError",
    "RiskPhysicsPolicy",
    "RiskPhysicsDecision",
    "RiskPhysicsEngine",
]