from __future__ import annotations

"""
========================================================
FILE: execution/risk_physics_engine.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
역할
--------------------------------------------------------
- Regime allocation(0.0~1.0) + AccountState(DD/연속손실) + Signal(tp/sl) +
  (TRADE-GRADE) Microstructure + EV Heatmap 상태
  을 입력으로 받아, "최종 allocation(=계좌 투입 비율)"을 결정한다.
- 이 파일에서 말하는 effective_risk_pct 는
  "리스크% 상한"이 아니라 "계좌 투입 비율(0~1)" 이다.
  예) 1.0 = 전액, 0.7 = 70%, 0.4 = 40%

핵심 원칙 (공동 규칙)
--------------------------------------------------------
- 절대 폴백 금지:
  - 입력 누락/형식 오류/범위 이탈 시 즉시 예외.
  - None → 0 치환, 임의 추정/임의 보정 금지.
- 정책 기반의 "명시적" 보정만 허용:
  - DD/연속손실/최소 RR/AutoBlock(명시 룰)에 의한 감쇠/차단.
- 민감정보 로그 금지.
- settings 객체 불변(읽기만).
- 외부 I/O 금지(DB/네트워크 금지). caller가 모든 입력을 공급한다.

정책 (확정) — 전액 배분형
--------------------------------------------------------
1) Regime allocation(0.0~1.0) 은 caller가 공급한다.
2) DD 감쇠(Allocation multiplier)
   - DD >= 5%  -> x0.8
   - DD >= 10% -> x0.5
   - DD >= 15% -> 신규 진입 차단(SKIP)
   - DD >= 20% -> 엔진 정지 권고(STOP)
3) 연속 손실 감쇠(Allocation multiplier)
   - consecutive_losses >= 2 -> x0.7
   - consecutive_losses >= 3 -> 신규 진입 차단(SKIP)
4) 최소 계획 RR 강제(현재 신호 기준)
   - planned_rr = tp_pct / sl_pct
   - planned_rr < 1.6 -> 신규 진입 차단(SKIP)
5) (TRADE-GRADE) AutoBlock overlay
   - micro_score_risk(0~100) + EV Heatmap 상태로 block_entry / risk_multiplier 산출
   - block_entry=True면 SKIP
   - risk_multiplier는 allocation에 곱셈 적용
6) 최종 allocation 산출:
   - effective_alloc = allocation_adjusted
   - 상한(max_allocation)은 정책상 제한값(기본 1.0)
   - 최소(min_allocation_if_enter) 미만이면 SKIP

PATCH NOTES
--------------------------------------------------------
- 2026-03-03 (TRADE-GRADE)
  - AutoBlock(microstructure + EV heatmap) 반영:
    * decide() 입력에 micro_score_risk / heatmap_status / heatmap_ev / heatmap_n 추가
    * risk/auto_block_engine.decide_auto_block() 결과로 SKIP 또는 allocation 감쇠
- 2026-03-02
  - "전액 배분형"으로 전환:
    * base_risk_pct 제거
    * effective_risk_pct = allocation_adjusted
========================================================
"""

import math
from dataclasses import dataclass
from typing import Any, Optional

from risk.auto_block_engine import AutoBlockError, AutoBlockDecision, decide_auto_block


# ─────────────────────────────────────────────
# Exceptions (STRICT)
# ─────────────────────────────────────────────
class RiskPhysicsError(RuntimeError):
    """Base error for risk physics engine."""


class RiskPhysicsInputError(RiskPhysicsError):
    """Raised when inputs are missing or invalid."""


# ─────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────
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
    action_override: str
    effective_risk_pct: float  # 이제 "계좌 투입 비율"로 사용
    allocation_used: float
    planned_rr: float
    reason: str

    # TRADE-GRADE: audit fields (optional, for caller meta logging)
    auto_blocked: bool = False
    auto_risk_multiplier: float = 1.0
    micro_score_risk: Optional[float] = None
    heatmap_status: Optional[str] = None
    heatmap_ev: Optional[float] = None
    heatmap_n: Optional[int] = None


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


def _compute_planned_rr(tp_pct: Any, sl_pct: Any) -> float:
    tp = _as_float(tp_pct, "signal.tp_pct", min_value=0.0)
    sl = _as_float(sl_pct, "signal.sl_pct", min_value=0.0)
    if tp <= 0.0 or sl <= 0.0:
        raise RiskPhysicsInputError("signal.tp_pct and signal.sl_pct must be > 0 for planned_rr")
    rr = tp / sl
    if not math.isfinite(rr) or rr <= 0.0:
        raise RiskPhysicsInputError(f"planned_rr invalid: {rr}")
    return rr


# ─────────────────────────────────────────────
# Engine
# ─────────────────────────────────────────────
class RiskPhysicsEngine:
    """
    STRICT · NO-FALLBACK

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
        self.policy = policy or RiskPhysicsPolicy()
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

        _as_int(p.consec_reduce_n, "policy.consec_reduce_n", min_value=0)
        _as_int(p.consec_block_n, "policy.consec_block_n", min_value=0)
        if p.consec_reduce_n > p.consec_block_n:
            raise ValueError("policy.consec_reduce_n must be <= policy.consec_block_n")
        _as_float(p.consec_reduce_mult, "policy.consec_reduce_mult", min_value=0.0, max_value=1.0)

        _as_float(p.min_planned_rr, "policy.min_planned_rr", min_value=0.1)

        _as_float(p.max_allocation, "policy.max_allocation", min_value=0.0, max_value=1.0)
        _as_float(p.min_allocation_if_enter, "policy.min_allocation_if_enter", min_value=0.0, max_value=1.0)

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
        st = str(heatmap_status).strip().upper()
        if not st:
            raise RiskPhysicsInputError("heatmap_status is empty")
        if st not in ("NOT_READY", "OK", "BLOCK"):
            raise RiskPhysicsInputError(f"heatmap_status invalid: {st!r}")

        hn = _as_int(heatmap_n, "heatmap_n", min_value=0)

        if st == "NOT_READY":
            if heatmap_ev is not None:
                raise RiskPhysicsInputError("heatmap_ev must be None when heatmap_status=NOT_READY")
            hev: Optional[float] = None
        else:
            if heatmap_ev is None:
                raise RiskPhysicsInputError("heatmap_ev is required when heatmap_status!=NOT_READY")
            hev = _as_float(heatmap_ev, "heatmap_ev")

        # Policy: STOP
        if dd >= self.policy.dd_stop_engine_pct:
            return RiskPhysicsDecision(
                action_override="STOP",
                effective_risk_pct=0.0,
                allocation_used=0.0,
                planned_rr=rr,
                reason=f"dd_stop_engine(dd_pct={dd:.2f}>= {self.policy.dd_stop_engine_pct:.2f})",
                auto_blocked=False,
                auto_risk_multiplier=1.0,
                micro_score_risk=msr,
                heatmap_status=st,
                heatmap_ev=hev,
                heatmap_n=hn,
            )

        # Policy: block entry
        if dd >= self.policy.dd_block_entry_pct:
            return RiskPhysicsDecision(
                action_override="SKIP",
                effective_risk_pct=0.0,
                allocation_used=0.0,
                planned_rr=rr,
                reason=f"dd_block_entry(dd_pct={dd:.2f}>= {self.policy.dd_block_entry_pct:.2f})",
                auto_blocked=False,
                auto_risk_multiplier=1.0,
                micro_score_risk=msr,
                heatmap_status=st,
                heatmap_ev=hev,
                heatmap_n=hn,
            )

        if consec >= self.policy.consec_block_n and self.policy.consec_block_n > 0:
            return RiskPhysicsDecision(
                action_override="SKIP",
                effective_risk_pct=0.0,
                allocation_used=0.0,
                planned_rr=rr,
                reason=f"consec_block(consecutive_losses={consec}>= {self.policy.consec_block_n})",
                auto_blocked=False,
                auto_risk_multiplier=1.0,
                micro_score_risk=msr,
                heatmap_status=st,
                heatmap_ev=hev,
                heatmap_n=hn,
            )

        if rr < self.policy.min_planned_rr:
            return RiskPhysicsDecision(
                action_override="SKIP",
                effective_risk_pct=0.0,
                allocation_used=0.0,
                planned_rr=rr,
                reason=f"planned_rr_too_low(rr={rr:.3f}< {self.policy.min_planned_rr:.3f})",
                auto_blocked=False,
                auto_risk_multiplier=1.0,
                micro_score_risk=msr,
                heatmap_status=st,
                heatmap_ev=hev,
                heatmap_n=hn,
            )

        # If regime allocation says no-trade, skip explicitly
        if alloc <= 0.0:
            return RiskPhysicsDecision(
                action_override="SKIP",
                effective_risk_pct=0.0,
                allocation_used=0.0,
                planned_rr=rr,
                reason="regime_allocation_zero",
                auto_blocked=False,
                auto_risk_multiplier=1.0,
                micro_score_risk=msr,
                heatmap_status=st,
                heatmap_ev=hev,
                heatmap_n=hn,
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

        if abd.block_entry:
            return RiskPhysicsDecision(
                action_override="SKIP",
                effective_risk_pct=0.0,
                allocation_used=0.0,
                planned_rr=rr,
                reason=f"auto_block({','.join(abd.reasons)})|{dd_reason}|{consec_reason}",
                auto_blocked=True,
                auto_risk_multiplier=float(abd.risk_multiplier),
                micro_score_risk=msr,
                heatmap_status=st,
                heatmap_ev=hev,
                heatmap_n=hn,
            )

        alloc_adj *= float(abd.risk_multiplier)

        if alloc_adj < 0.0 or alloc_adj > 1.0 or not math.isfinite(alloc_adj):
            raise RiskPhysicsError(f"allocation invalid after auto_block multiplier: {alloc_adj}")

        # Policy cap (explicit rule)
        if alloc_adj > self.policy.max_allocation:
            alloc_adj = float(self.policy.max_allocation)

        if alloc_adj <= 0.0:
            return RiskPhysicsDecision(
                action_override="SKIP",
                effective_risk_pct=0.0,
                allocation_used=float(alloc_adj),
                planned_rr=rr,
                reason=f"effective_allocation_zero_after_adjustments|{dd_reason}|{consec_reason}|auto_mul={abd.risk_multiplier}",
                auto_blocked=False,
                auto_risk_multiplier=float(abd.risk_multiplier),
                micro_score_risk=msr,
                heatmap_status=st,
                heatmap_ev=hev,
                heatmap_n=hn,
            )

        if alloc_adj < self.policy.min_allocation_if_enter:
            return RiskPhysicsDecision(
                action_override="SKIP",
                effective_risk_pct=0.0,
                allocation_used=float(alloc_adj),
                planned_rr=rr,
                reason=f"effective_allocation_below_min(alloc={alloc_adj:.6f}< {self.policy.min_allocation_if_enter:.6f})",
                auto_blocked=False,
                auto_risk_multiplier=float(abd.risk_multiplier),
                micro_score_risk=msr,
                heatmap_status=st,
                heatmap_ev=hev,
                heatmap_n=hn,
            )

        return RiskPhysicsDecision(
            action_override="ENTER",
            effective_risk_pct=float(alloc_adj),
            allocation_used=float(alloc_adj),
            planned_rr=rr,
            reason=f"ok|{dd_reason}|{consec_reason}|auto_mul={abd.risk_multiplier}",
            auto_blocked=False,
            auto_risk_multiplier=float(abd.risk_multiplier),
            micro_score_risk=msr,
            heatmap_status=st,
            heatmap_ev=hev,
            heatmap_n=hn,
        )


__all__ = [
    "RiskPhysicsError",
    "RiskPhysicsInputError",
    "RiskPhysicsPolicy",
    "RiskPhysicsDecision",
    "RiskPhysicsEngine",
]