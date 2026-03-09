from __future__ import annotations

"""
========================================================
FILE: risk/auto_block_engine.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================

핵심 변경 요약
- AutoBlockDecision.reasons 타입을 Tuple[str, ...] → List[str]로 수정
- 반환부 reasons=tuple(reasons) 제거, STRICT 계약에 맞게 reasons=list 그대로 반환
- 미사용 import(Dict, Tuple) 제거
- 미사용 지역변수(ev_val) 제거로 코드 정리

코드 정리 내용
- 사용하지 않는 import 정리
- 검증 후 사용되지 않는 변수 제거
- 기존 정책/기능은 삭제 없이 유지

역할
- 자동 차단(Auto Block) 의사결정 엔진.
- 입력:
  1) microstructure 위험도(micro_score_risk, 0~100)
  2) EV 히트맵 셀 상태(EvHeatmapEngine.get_cell_status 결과)
  3) 기타 운영 상태(연속 손실, DD 등) — 선택
- 출력:
  - block_entry: bool (신규 진입 차단 여부)
  - risk_multiplier: float (0~1, 리스크/사이즈 곱셈 계수)
  - reasons: list[str] (차단/감쇠 근거 태그)

핵심 원칙 (STRICT · NO-FALLBACK)
- 입력 누락/범위 이탈/형식 불일치는 즉시 예외.
- "대충 통과" 금지. 규칙으로 결정 불가하면 예외.
- 이 모듈은 외부 I/O를 하지 않는다(DB/네트워크 금지).
- 최종 결정(ENTER/HOLD/EXIT)은 상위 엔진이 하며,
  이 모듈은 "차단/감쇠 룰"만 제공한다.

정책(명시 룰)
1) Microstructure Hard Block
- micro_score_risk >= 80 → block_entry=True, risk_multiplier=0.0

2) Microstructure Soft Penalty
- 60~80 → risk_multiplier=0.5
- 40~60 → risk_multiplier=0.8
- <40   → risk_multiplier=1.0

3) EV Heatmap Block
- cell_status == BLOCK → block_entry=True
  (단, micro_score_risk가 매우 낮아도 EV block은 우선)

4) Optional overlays (caller가 제공하는 경우만)
- consecutive_losses >= N → risk_multiplier 추가 감쇠
- dd_pct >= X → risk_multiplier 추가 감쇠
(기본값은 설정하지 않는다. caller 정책으로 주입)

변경 이력
--------------------------------------------------------
- 2026-03-09:
  1) STRICT 계약 불일치 수정: reasons 반환 타입을 tuple → list로 변경
  2) 미사용 import 및 미사용 지역변수 정리
========================================================
"""

import math
from dataclasses import dataclass
from typing import Any, List, Optional


class AutoBlockError(RuntimeError):
    """AutoBlockEngine 입력/정책 위반 오류(STRICT)."""


@dataclass(frozen=True, slots=True)
class AutoBlockDecision:
    block_entry: bool
    risk_multiplier: float  # 0.0~1.0
    reasons: List[str]


def _fail(stage: str, reason: str) -> None:
    raise AutoBlockError(f"[AUTO-BLOCK] {stage} 실패: {reason}")


def _require_float(stage: str, v: Any, name: str) -> float:
    try:
        fv = float(v)
    except Exception:
        _fail(stage, f"{name} must be float (got={v!r})")
    if not math.isfinite(fv):
        _fail(stage, f"{name} must be finite (got={fv})")
    return fv


def _require_int(stage: str, v: Any, name: str) -> int:
    try:
        iv = int(v)
    except Exception:
        _fail(stage, f"{name} must be int (got={v!r})")
    return iv


def _require_range(stage: str, v: float, name: str, lo: float, hi: float) -> float:
    if v < lo or v > hi:
        _fail(stage, f"{name} out of range [{lo},{hi}] (got={v})")
    return v


def _require_nonempty_str(stage: str, v: Any, name: str) -> str:
    if v is None:
        _fail(stage, f"{name} is None")
    s = str(v).strip()
    if not s:
        _fail(stage, f"{name} is empty")
    return s


def _micro_multiplier(micro_score_risk: float) -> float:
    # 명시 룰 (regime_engine과 동일 정책)
    if micro_score_risk < 40.0:
        return 1.0
    if micro_score_risk < 60.0:
        return 0.8
    if micro_score_risk < 80.0:
        return 0.5
    return 0.0


def decide_auto_block(
    *,
    micro_score_risk: float,
    heatmap_status: str,
    heatmap_ev: Optional[float],
    heatmap_n: int,
    consecutive_losses: Optional[int] = None,
    dd_pct: Optional[float] = None,
    consec_loss_penalty_threshold: Optional[int] = None,
    dd_penalty_threshold: Optional[float] = None,
) -> AutoBlockDecision:
    """
    STRICT Auto Block decision.

    Args:
        micro_score_risk: 0~100
        heatmap_status: "NOT_READY" | "OK" | "BLOCK"
        heatmap_ev: EV 값 (status=NOT_READY이면 None 허용)
        heatmap_n: 샘플 수
        consecutive_losses: 선택
        dd_pct: 선택(0~100)
        consec_loss_penalty_threshold: 선택(임계 이상이면 risk_multiplier 추가 감쇠)
        dd_penalty_threshold: 선택(임계 이상이면 risk_multiplier 추가 감쇠)
    """
    stage = "input"
    msr = _require_float(stage, micro_score_risk, "micro_score_risk")
    _require_range(stage, msr, "micro_score_risk", 0.0, 100.0)

    st = _require_nonempty_str(stage, heatmap_status, "heatmap_status").upper()
    if st not in ("NOT_READY", "OK", "BLOCK"):
        _fail(stage, f"heatmap_status invalid: {st!r}")

    n = _require_int(stage, heatmap_n, "heatmap_n")
    if n < 0:
        _fail(stage, f"heatmap_n must be >=0 (got={n})")

    if st == "NOT_READY":
        if heatmap_ev is not None:
            _fail(stage, "heatmap_ev must be None when status=NOT_READY")
    else:
        if heatmap_ev is None:
            _fail(stage, "heatmap_ev is required when status!=NOT_READY")
        _require_float(stage, heatmap_ev, "heatmap_ev")

    reasons: List[str] = []

    # 1) Heatmap block has priority
    if st == "BLOCK":
        reasons.append("ev_block")
        # Multiplier is still computed internally for rule consistency,
        # but block overrides final entry decision and multiplier.
        _micro_multiplier(msr)
        block_entry = True
        risk_mul = 0.0
    else:
        mul = _micro_multiplier(msr)
        block_entry = False
        risk_mul = float(mul)

        if mul == 0.0:
            block_entry = True
            risk_mul = 0.0
            reasons.append("micro_block")
        elif mul < 1.0:
            reasons.append("micro_penalty")

    # 2) Optional overlays (only if both value and threshold provided)
    if consecutive_losses is not None and consec_loss_penalty_threshold is not None:
        cl = _require_int("overlay", consecutive_losses, "consecutive_losses")
        th = _require_int("overlay", consec_loss_penalty_threshold, "consec_loss_penalty_threshold")
        if th <= 0:
            _fail("overlay", "consec_loss_penalty_threshold must be > 0")
        if cl < 0:
            _fail("overlay", "consecutive_losses must be >= 0")
        if cl >= th and not block_entry:
            risk_mul *= 0.7
            reasons.append("consecutive_loss_penalty")

    if dd_pct is not None and dd_penalty_threshold is not None:
        dd = _require_float("overlay", dd_pct, "dd_pct")
        _require_range("overlay", dd, "dd_pct", 0.0, 100.0)
        th = _require_float("overlay", dd_penalty_threshold, "dd_penalty_threshold")
        _require_range("overlay", th, "dd_penalty_threshold", 0.0, 100.0)
        if dd >= th and not block_entry:
            risk_mul *= 0.7
            reasons.append("dd_penalty")

    # 3) Final sanitize
    if not math.isfinite(risk_mul):
        _fail("compute", f"risk_multiplier not finite: {risk_mul}")
    if risk_mul < 0.0 or risk_mul > 1.0:
        _fail("compute", f"risk_multiplier out of range [0,1]: {risk_mul}")

    return AutoBlockDecision(
        block_entry=bool(block_entry),
        risk_multiplier=float(risk_mul),
        reasons=reasons,
    )


__all__ = [
    "AutoBlockError",
    "AutoBlockDecision",
    "decide_auto_block",
]