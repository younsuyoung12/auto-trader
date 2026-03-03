from __future__ import annotations

"""
========================================================
FILE: strategy/supervision_report_engine.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
역할
- 운영 자가 진단(Supervision Report) 엔진.
- 트레이드급 운영을 위해 "오늘/최근" 상태를 정량 요약한다.
- 이 모듈은 외부 I/O(DB/네트워크)를 하지 않는다.
  -> 호출자가 필요한 최신 값/집계치를 공급한다.

보고서 항목(요약)
- DD 상태: dd_pct, equity_current, equity_peak
- EV 상태: rolling_ev_20, rolling_ev_50 (R 단위)
- Execution Quality: execution_score rolling mean
- GPT Supervisor: severity 누적, 최근 N회의 평균/최댓값
- AutoBlock: 차단 횟수, 차단 원인 분포
- 전략 안정성 점수: 0~100 (명시 규칙 기반)

핵심 원칙 (STRICT · NO-FALLBACK)
- 입력 누락/형식 불일치/범위 이탈은 즉시 예외.
- "값이 없으니 0으로" 같은 폴백 금지.
- score 계산은 명시 룰만 사용한다(임의 추정 금지).

변경 이력
--------------------------------------------------------
- 2026-03-03:
  1) 신규 생성: 운영 자가 진단 보고서 생성기(STRICT)
========================================================
"""

import math
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


class SupervisionReportError(RuntimeError):
    """Supervision report engine 오류(STRICT)."""


@dataclass(frozen=True, slots=True)
class SupervisionReport:
    ts_ms: int

    # equity / DD
    equity_current_usdt: float
    equity_peak_usdt: float
    dd_pct: float

    # EV (R)
    rolling_ev_20: float
    rolling_ev_50: float

    # Execution quality
    execution_score_mean: float  # 0~100

    # GPT supervisor
    gpt_severity_mean: float  # 0~3
    gpt_severity_max: int  # 0~3
    gpt_events_count: int

    # AutoBlock
    auto_block_count: int
    auto_block_top_reasons: Tuple[Tuple[str, int], ...]  # (reason, count)

    # Derived stability score
    stability_score: float  # 0~100
    stability_grade: str  # A/B/C/D

    # Extra metadata
    meta: Dict[str, Any]


def _now_ms() -> int:
    return int(time.time() * 1000)


def _fail(stage: str, reason: str) -> None:
    raise SupervisionReportError(f"[SUPERVISION] {stage} 실패: {reason}")


def _require_float(stage: str, v: Any, name: str, *, lo: Optional[float] = None, hi: Optional[float] = None) -> float:
    try:
        fv = float(v)
    except Exception:
        _fail(stage, f"{name} must be float (got={v!r})")
    if not math.isfinite(fv):
        _fail(stage, f"{name} must be finite (got={fv})")
    if lo is not None and fv < lo:
        _fail(stage, f"{name} must be >= {lo} (got={fv})")
    if hi is not None and fv > hi:
        _fail(stage, f"{name} must be <= {hi} (got={fv})")
    return fv


def _require_int(stage: str, v: Any, name: str, *, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    try:
        iv = int(v)
    except Exception:
        _fail(stage, f"{name} must be int (got={v!r})")
    if lo is not None and iv < lo:
        _fail(stage, f"{name} must be >= {lo} (got={iv})")
    if hi is not None and iv > hi:
        _fail(stage, f"{name} must be <= {hi} (got={iv})")
    return iv


def _require_reason_counts(stage: str, v: Any) -> Dict[str, int]:
    if not isinstance(v, dict) or not v:
        _fail(stage, "auto_block_reason_counts must be non-empty dict")
    out: Dict[str, int] = {}
    for k, c in v.items():
        key = str(k).strip()
        if not key:
            _fail(stage, "auto_block_reason_counts has empty key")
        out[key] = _require_int(stage, c, f"auto_block_reason_counts[{key}]", lo=0)
    return out


def _top_k_reasons(reason_counts: Dict[str, int], k: int = 5) -> Tuple[Tuple[str, int], ...]:
    items = sorted(reason_counts.items(), key=lambda x: x[1], reverse=True)
    return tuple(items[:k])


def _clamp_0_100(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 100.0:
        return 100.0
    return x


def _compute_stability_score(
    *,
    dd_pct: float,
    ev20: float,
    ev50: float,
    exec_score_mean: float,
    gpt_sev_mean: float,
    gpt_sev_max: int,
    auto_block_count: int,
) -> float:
    """
    명시 규칙 기반 안정성 점수(0~100).
    - 목표: "수익 자랑"이 아니라 "운영 안정성"을 점수화.

    Score 구성(가중치 합=100):
    - DD (40): DD 낮을수록 좋다
    - EV (25): EV가 양수일수록 좋다 (단, 과대평가 방지 위해 하드 상한)
    - Execution (20): 체결 품질
    - GPT Supervisor (10): 경고 낮을수록 좋다
    - AutoBlock (5): 차단이 과도하면 운영 불안정

    NOTE: 점수는 운영 지표이며, 매매 결정을 직접 만들지 않는다.
    """
    # DD score (0..40): dd 0% -> 40, dd 20% -> 0 (linear, clamp)
    dd_score = _clamp_0_100((20.0 - dd_pct) / 20.0 * 100.0) * 0.40

    # EV score (0..25): use min(ev, 0.8) cap, map -0.5..+0.8 -> 0..100
    ev = (0.6 * ev20) + (0.4 * ev50)
    if ev > 0.8:
        ev = 0.8
    if ev < -0.5:
        ev = -0.5
    ev_norm = (ev - (-0.5)) / (0.8 - (-0.5)) * 100.0
    ev_score = _clamp_0_100(ev_norm) * 0.25

    # Execution score (0..20): exec_score_mean already 0..100
    exec_score = _clamp_0_100(exec_score_mean) * 0.20

    # GPT supervisor score (0..10): sev_mean 낮고 sev_max 낮을수록 좋다
    # sev_mean: 0..3 -> map to 100..0
    sev_mean_norm = (3.0 - gpt_sev_mean) / 3.0 * 100.0
    # sev_max: 0..3 -> penalty
    sev_max_penalty = {0: 0.0, 1: 5.0, 2: 25.0, 3: 60.0}.get(int(gpt_sev_max), 60.0)
    gpt_score = _clamp_0_100(sev_mean_norm - sev_max_penalty) * 0.10

    # AutoBlock score (0..5): 많을수록 감점 (0 -> 5, 10+ -> 0)
    ab_norm = _clamp_0_100((10.0 - float(auto_block_count)) / 10.0 * 100.0)
    ab_score = ab_norm * 0.05

    total = dd_score + ev_score + exec_score + gpt_score + ab_score
    return float(_clamp_0_100(total))


def _grade(score: float) -> str:
    if score >= 85.0:
        return "A"
    if score >= 70.0:
        return "B"
    if score >= 55.0:
        return "C"
    return "D"


def build_supervision_report(
    *,
    equity_current_usdt: Any,
    equity_peak_usdt: Any,
    dd_pct: Any,
    rolling_ev_20: Any,
    rolling_ev_50: Any,
    execution_score_mean: Any,
    gpt_severity_mean: Any,
    gpt_severity_max: Any,
    gpt_events_count: Any,
    auto_block_count: Any,
    auto_block_reason_counts: Dict[str, Any],
    meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    rep = build_supervision_report_obj(
        equity_current_usdt=equity_current_usdt,
        equity_peak_usdt=equity_peak_usdt,
        dd_pct=dd_pct,
        rolling_ev_20=rolling_ev_20,
        rolling_ev_50=rolling_ev_50,
        execution_score_mean=execution_score_mean,
        gpt_severity_mean=gpt_severity_mean,
        gpt_severity_max=gpt_severity_max,
        gpt_events_count=gpt_events_count,
        auto_block_count=auto_block_count,
        auto_block_reason_counts=auto_block_reason_counts,
        meta=meta,
    )
    return {
        "ts_ms": rep.ts_ms,
        "equity_current_usdt": rep.equity_current_usdt,
        "equity_peak_usdt": rep.equity_peak_usdt,
        "dd_pct": rep.dd_pct,
        "rolling_ev_20": rep.rolling_ev_20,
        "rolling_ev_50": rep.rolling_ev_50,
        "execution_score_mean": rep.execution_score_mean,
        "gpt_severity_mean": rep.gpt_severity_mean,
        "gpt_severity_max": rep.gpt_severity_max,
        "gpt_events_count": rep.gpt_events_count,
        "auto_block_count": rep.auto_block_count,
        "auto_block_top_reasons": list(rep.auto_block_top_reasons),
        "stability_score": rep.stability_score,
        "stability_grade": rep.stability_grade,
        "meta": dict(rep.meta),
    }


def build_supervision_report_obj(
    *,
    equity_current_usdt: Any,
    equity_peak_usdt: Any,
    dd_pct: Any,
    rolling_ev_20: Any,
    rolling_ev_50: Any,
    execution_score_mean: Any,
    gpt_severity_mean: Any,
    gpt_severity_max: Any,
    gpt_events_count: Any,
    auto_block_count: Any,
    auto_block_reason_counts: Dict[str, Any],
    meta: Optional[Dict[str, Any]] = None,
) -> SupervisionReport:
    stage = "input"

    eq_cur = _require_float(stage, equity_current_usdt, "equity_current_usdt", lo=0.0)
    eq_peak = _require_float(stage, equity_peak_usdt, "equity_peak_usdt", lo=0.0)
    if eq_cur <= 0.0 or eq_peak <= 0.0:
        _fail(stage, "equity values must be > 0")
    if eq_cur > eq_peak:
        # STRICT: peak should be >= current in DD definition; caller must provide consistent state.
        _fail(stage, f"equity_current_usdt({eq_cur}) > equity_peak_usdt({eq_peak})")

    dd = _require_float(stage, dd_pct, "dd_pct", lo=0.0, hi=100.0)

    ev20 = _require_float(stage, rolling_ev_20, "rolling_ev_20")
    ev50 = _require_float(stage, rolling_ev_50, "rolling_ev_50")

    exs = _require_float(stage, execution_score_mean, "execution_score_mean", lo=0.0, hi=100.0)

    sev_mean = _require_float(stage, gpt_severity_mean, "gpt_severity_mean", lo=0.0, hi=3.0)
    sev_max = _require_int(stage, gpt_severity_max, "gpt_severity_max", lo=0, hi=3)
    ge_cnt = _require_int(stage, gpt_events_count, "gpt_events_count", lo=0)

    ab_cnt = _require_int(stage, auto_block_count, "auto_block_count", lo=0)
    rc = _require_reason_counts(stage, auto_block_reason_counts)

    stability = _compute_stability_score(
        dd_pct=dd,
        ev20=ev20,
        ev50=ev50,
        exec_score_mean=exs,
        gpt_sev_mean=sev_mean,
        gpt_sev_max=sev_max,
        auto_block_count=ab_cnt,
    )
    grade = _grade(stability)

    m = meta if meta is not None else {}
    if not isinstance(m, dict):
        _fail(stage, "meta must be dict or None")

    return SupervisionReport(
        ts_ms=_now_ms(),
        equity_current_usdt=float(eq_cur),
        equity_peak_usdt=float(eq_peak),
        dd_pct=float(dd),
        rolling_ev_20=float(ev20),
        rolling_ev_50=float(ev50),
        execution_score_mean=float(exs),
        gpt_severity_mean=float(sev_mean),
        gpt_severity_max=int(sev_max),
        gpt_events_count=int(ge_cnt),
        auto_block_count=int(ab_cnt),
        auto_block_top_reasons=_top_k_reasons(rc, k=5),
        stability_score=float(stability),
        stability_grade=grade,
        meta=m,
    )


__all__ = [
    "SupervisionReportError",
    "SupervisionReport",
    "build_supervision_report",
    "build_supervision_report_obj",
]