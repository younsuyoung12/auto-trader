"""
========================================================
FILE: execution/invariant_guard.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할
- 전략/리스크/실행 단계에서 생성되는 “수학적 핵심 값”의 무결성(Invariant)을 STRICT로 검증한다.
- allocation/risk_pct, multiplier, dd_pct, micro_score_risk, tp/sl, qty/notional, slippage 등
  돈이 직접 걸리는 값들의 범위/finite/관계식을 강제한다.
- 이 모듈은 "검증"만 수행한다. (로그/알림/SAFE_STOP 결정은 호출자 책임)

설계 원칙:
- 폴백 금지(None→0 등 금지)
- 데이터 누락/불일치/비정상 값 발견 시 즉시 예외
- 예외 삼키기 금지
- 환경변수 직접 접근 금지(settings.py 불필요: 순수 검증 모듈)
- DB 접근 금지(순수 함수/순수 가드)
- 시간 역전(timestamp rollback)은 DataIntegrityGuard에서 담당(여긴 수학 invariant)

변경 이력
--------------------------------------------------------
- 2026-03-09:
  1) FIX(ROOT-CAUSE): equity_current/equity_peak/dd_pct 삼자 정합 검증 추가
     - dd_pct가 있으면 (peak-current)/peak*100 과 일치해야 함
  2) FIX(TRADE-GRADE): execution 수학 관계식 강화
     - qty_raw * price_for_qty ≈ notional
     - notional <= available_usdt * leverage
     - implied allocation ratio in (0,1]
  3) FIX(STRICT): entry_price_hint / last_price 상대 괴리 검증 추가
  4) 문서 정합:
     - slippage_pct / spread_pct_snapshot 는 "퍼센트 이름"이지만
       실행 엔진에서는 ratio(0..1)로 전달되므로 invariant도 ratio 기준으로 검증

- 2026-03-04:
  1) 신규 생성: 엔진 핵심 수학 값 Invariant 검증 레이어 추가
  2) allocation/risk_pct, multiplier, dd_pct, micro_score_risk, tp/sl 등 범위/finite 강제
  3) execution 관련 qty/notional/slippage 검증 추가(실행 직전/직후 재검증용)
========================================================
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Optional


class InvariantViolation(RuntimeError):
    """수학적 무결성(invariant) 위반(STRICT)."""


_FLOAT_REL_TOL: float = 1e-6
_FLOAT_ABS_TOL: float = 1e-9
_DD_PCT_ABS_TOL: float = 0.25
_MAX_EXEC_SLIPPAGE_RATIO: float = 1.0
_MAX_EXEC_SPREAD_RATIO: float = 1.0
_MAX_ENTRY_HINT_DEVIATION_RATIO: float = 1.0


# =============================================================================
# Core strict parsers
# =============================================================================
def _require_float(v: Any, name: str) -> float:
    if v is None:
        raise InvariantViolation(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        raise InvariantViolation(f"{name} must be numeric (bool not allowed)")
    try:
        x = float(v)
    except Exception as e:
        raise InvariantViolation(f"{name} must be numeric (STRICT): {e}") from e
    if not math.isfinite(x):
        raise InvariantViolation(f"{name} must be finite (STRICT)")
    return float(x)


def _require_float_range(v: Any, name: str, *, lo: float, hi: float) -> float:
    x = _require_float(v, name)
    if x < lo or x > hi:
        raise InvariantViolation(f"{name} out of range [{lo},{hi}] (STRICT): {x}")
    return float(x)


def _require_float_min(v: Any, name: str, *, min_value: float) -> float:
    x = _require_float(v, name)
    if x < min_value:
        raise InvariantViolation(f"{name} must be >= {min_value} (STRICT): {x}")
    return float(x)


def _require_nonempty_str(v: Any, name: str) -> str:
    if v is None:
        raise InvariantViolation(f"{name} is required (STRICT)")
    s = str(v).strip()
    if not s:
        raise InvariantViolation(f"{name} is required (STRICT)")
    return s


def _require_closeish(
    lhs: float,
    rhs: float,
    *,
    name: str,
    rel_tol: float = _FLOAT_REL_TOL,
    abs_tol: float = _FLOAT_ABS_TOL,
) -> None:
    if not math.isfinite(lhs) or not math.isfinite(rhs):
        raise InvariantViolation(f"{name} operands must be finite (STRICT)")
    if not math.isclose(lhs, rhs, rel_tol=rel_tol, abs_tol=abs_tol):
        raise InvariantViolation(f"{name} mismatch (STRICT): lhs={lhs} rhs={rhs}")


def _calc_drawdown_pct_strict(*, equity_current_usdt: float, equity_peak_usdt: float) -> float:
    cur = _require_float_min(equity_current_usdt, "meta.equity_current_usdt", min_value=0.0)
    peak = _require_float_min(equity_peak_usdt, "meta.equity_peak_usdt", min_value=0.0)
    if peak <= 0.0:
        raise InvariantViolation("meta.equity_peak_usdt must be > 0 when drawdown is derived (STRICT)")
    if cur <= 0.0:
        raise InvariantViolation("meta.equity_current_usdt must be > 0 when drawdown is derived (STRICT)")
    if cur > peak + _FLOAT_ABS_TOL:
        raise InvariantViolation(
            f"equity_current_usdt > equity_peak_usdt (STRICT): cur={cur} peak={peak}"
        )
    dd = (peak - cur) / peak * 100.0
    if dd < 0.0:
        raise InvariantViolation(f"derived dd_pct negative (STRICT): {dd}")
    return float(dd)


# =============================================================================
# Public invariant checks (STRICT)
# =============================================================================
@dataclass(frozen=True, slots=True)
class SignalInvariantInputs:
    """
    Signal / risk-level invariants (pre-execution).
    """
    symbol: str
    direction: str  # LONG/SHORT
    risk_pct: float  # allocation ratio 0..1
    tp_pct: float  # 0..1
    sl_pct: float  # 0..1
    dd_pct: Optional[float] = None  # 0..100
    micro_score_risk: Optional[float] = None  # 0..100
    final_risk_multiplier: Optional[float] = None  # 0..1 (Meta L3)
    equity_current_usdt: Optional[float] = None  # >=0
    equity_peak_usdt: Optional[float] = None  # >=0


@dataclass(frozen=True, slots=True)
class ExecutionInvariantInputs:
    """
    Execution-level invariants (execution_engine).

    주의:
    - slippage_pct / spread_pct_snapshot 는 이름은 pct지만
      현재 엔진에서는 ratio(예: 0.0025 = 0.25%)로 전달된다.
    """
    symbol: str
    direction: str  # LONG/SHORT
    leverage: float  # >0
    available_usdt: float  # >0
    notional: float  # >0
    price_for_qty: float  # >0
    qty_raw: float  # >0
    entry_price_hint: float  # >0
    last_price: float  # >0
    slippage_pct: Optional[float] = None  # ratio >=0
    spread_pct_snapshot: Optional[float] = None  # ratio >=0


def validate_signal_invariants_strict(x: SignalInvariantInputs) -> None:
    """
    STRICT:
    - Signal이 실행 레이어로 넘어가기 전에 반드시 만족해야 하는 수학 invariant 검증
    """
    sym = _require_nonempty_str(x.symbol, "signal.symbol").replace("-", "").replace("/", "").upper().strip()
    if not sym:
        raise InvariantViolation("signal.symbol normalized empty (STRICT)")

    direction = _require_nonempty_str(x.direction, "signal.direction").upper()
    if direction not in ("LONG", "SHORT"):
        raise InvariantViolation(f"signal.direction invalid (STRICT): {direction!r}")

    risk_pct = _require_float_range(x.risk_pct, "signal.risk_pct", lo=0.0, hi=1.0)
    if risk_pct <= 0.0:
        raise InvariantViolation("signal.risk_pct must be > 0 (STRICT)")

    tp = _require_float_range(x.tp_pct, "signal.tp_pct", lo=0.0, hi=1.0)
    sl = _require_float_range(x.sl_pct, "signal.sl_pct", lo=0.0, hi=1.0)
    if tp <= 0.0:
        raise InvariantViolation("signal.tp_pct must be > 0 (STRICT)")
    if sl <= 0.0:
        raise InvariantViolation("signal.sl_pct must be > 0 (STRICT)")

    if x.dd_pct is not None:
        _require_float_range(x.dd_pct, "meta.dd_pct", lo=0.0, hi=100.0)
    if x.micro_score_risk is not None:
        _require_float_range(x.micro_score_risk, "meta.micro_score_risk", lo=0.0, hi=100.0)

    if x.final_risk_multiplier is not None:
        frm = _require_float_range(x.final_risk_multiplier, "meta.final_risk_multiplier", lo=0.0, hi=1.0)
        if frm <= 0.0:
            raise InvariantViolation("meta.final_risk_multiplier must be > 0 when provided (STRICT)")

    cur: Optional[float] = None
    peak: Optional[float] = None

    if x.equity_current_usdt is not None:
        cur = _require_float_min(x.equity_current_usdt, "meta.equity_current_usdt", min_value=0.0)
    if x.equity_peak_usdt is not None:
        peak = _require_float_min(x.equity_peak_usdt, "meta.equity_peak_usdt", min_value=0.0)

    # Equity relation if both present
    if cur is not None and peak is not None:
        if peak <= 0.0 or cur <= 0.0:
            raise InvariantViolation("equity values must be > 0 when both provided (STRICT)")
        if cur > peak + _FLOAT_ABS_TOL:
            raise InvariantViolation(f"equity_current_usdt > equity_peak_usdt (STRICT): cur={cur} peak={peak}")

        if x.dd_pct is not None:
            dd_expected = _calc_drawdown_pct_strict(
                equity_current_usdt=float(cur),
                equity_peak_usdt=float(peak),
            )
            dd_actual = float(x.dd_pct)
            if abs(dd_expected - dd_actual) > _DD_PCT_ABS_TOL:
                raise InvariantViolation(
                    f"meta.dd_pct inconsistent with equity values (STRICT): expected={dd_expected:.6f} actual={dd_actual:.6f}"
                )


def validate_execution_invariants_strict(x: ExecutionInvariantInputs) -> None:
    """
    STRICT:
    - execution_engine에서 계산되는 금액/수량/가격/슬리피지의 수학 invariant 검증
    """
    sym = _require_nonempty_str(x.symbol, "exec.symbol").replace("-", "").replace("/", "").upper().strip()
    if not sym:
        raise InvariantViolation("exec.symbol normalized empty (STRICT)")

    direction = _require_nonempty_str(x.direction, "exec.direction").upper()
    if direction not in ("LONG", "SHORT"):
        raise InvariantViolation(f"exec.direction invalid (STRICT): {direction!r}")

    lev = _require_float_min(x.leverage, "exec.leverage", min_value=1.0)
    if lev <= 0:
        raise InvariantViolation("exec.leverage must be > 0 (STRICT)")

    avail = _require_float_min(x.available_usdt, "exec.available_usdt", min_value=0.0)
    if avail <= 0:
        raise InvariantViolation("exec.available_usdt must be > 0 (STRICT)")

    notional = _require_float_min(x.notional, "exec.notional", min_value=0.0)
    if notional <= 0:
        raise InvariantViolation("exec.notional must be > 0 (STRICT)")

    pfq = _require_float_min(x.price_for_qty, "exec.price_for_qty", min_value=0.0)
    if pfq <= 0:
        raise InvariantViolation("exec.price_for_qty must be > 0 (STRICT)")

    qty = _require_float_min(x.qty_raw, "exec.qty_raw", min_value=0.0)
    if qty <= 0:
        raise InvariantViolation("exec.qty_raw must be > 0 (STRICT)")

    eph = _require_float_min(x.entry_price_hint, "exec.entry_price_hint", min_value=0.0)
    lp = _require_float_min(x.last_price, "exec.last_price", min_value=0.0)
    if eph <= 0 or lp <= 0:
        raise InvariantViolation("exec prices must be > 0 (STRICT)")

    # 핵심 관계식 1: qty * price ≈ notional
    implied_notional = qty * pfq
    _require_closeish(
        implied_notional,
        notional,
        name="exec.notional == exec.qty_raw * exec.price_for_qty",
        rel_tol=_FLOAT_REL_TOL,
        abs_tol=_FLOAT_ABS_TOL,
    )

    # 핵심 관계식 2: notional <= available * leverage
    max_notional = avail * lev
    if max_notional <= 0:
        raise InvariantViolation("exec.available_usdt * exec.leverage must be > 0 (STRICT)")
    if notional > max_notional + max(_FLOAT_ABS_TOL, max_notional * _FLOAT_REL_TOL):
        raise InvariantViolation(
            f"exec.notional exceeds available_usdt*leverage (STRICT): notional={notional} max={max_notional}"
        )

    # 핵심 관계식 3: implied allocation ratio in (0,1]
    implied_alloc = notional / max_notional
    if implied_alloc <= 0.0 or implied_alloc > 1.0 + _FLOAT_REL_TOL:
        raise InvariantViolation(
            f"exec implied allocation ratio out of range (STRICT): {implied_alloc}"
        )

    # 가격 힌트와 현재가가 완전히 다른 단위/오류인지 점검
    hint_dev_ratio = abs(eph - lp) / lp
    if hint_dev_ratio > _MAX_ENTRY_HINT_DEVIATION_RATIO:
        raise InvariantViolation(
            f"exec.entry_price_hint deviates too much from exec.last_price (STRICT): ratio={hint_dev_ratio}"
        )

    # Slippage / spread constraints (ratio 기준)
    if x.slippage_pct is not None:
        s = _require_float_min(x.slippage_pct, "exec.slippage_pct", min_value=0.0)
        if s > _MAX_EXEC_SLIPPAGE_RATIO:
            raise InvariantViolation(f"exec.slippage_pct too large (STRICT): {s}")

    if x.spread_pct_snapshot is not None:
        sp = _require_float_min(x.spread_pct_snapshot, "exec.spread_pct_snapshot", min_value=0.0)
        if sp > _MAX_EXEC_SPREAD_RATIO:
            raise InvariantViolation(f"exec.spread_pct_snapshot too large (STRICT): {sp}")


def validate_signal_from_meta_strict(meta: Dict[str, Any]) -> None:
    """
    STRICT helper:
    - run_bot_ws / execution_engine에서 meta dict로부터 invariant inputs를 구성하기 전,
      최소 필수 키의 타입/범위를 빠르게 검증한다.
    """
    if not isinstance(meta, dict) or not meta:
        raise InvariantViolation("meta must be non-empty dict (STRICT)")

    _ = _require_nonempty_str(meta.get("symbol"), "meta.symbol")
    _ = _require_nonempty_str(meta.get("signal_source"), "meta.signal_source")
    _ = _require_nonempty_str(meta.get("regime"), "meta.regime")

    _ = _require_float_min(meta.get("last_price"), "meta.last_price", min_value=0.0)

    # Optional but critical fields when present
    if "dd_pct" in meta and meta.get("dd_pct") is not None:
        _require_float_range(meta.get("dd_pct"), "meta.dd_pct", lo=0.0, hi=100.0)
    if "micro_score_risk" in meta and meta.get("micro_score_risk") is not None:
        _require_float_range(meta.get("micro_score_risk"), "meta.micro_score_risk", lo=0.0, hi=100.0)

    if "dynamic_allocation_ratio" in meta and meta.get("dynamic_allocation_ratio") is not None:
        _require_float_range(meta.get("dynamic_allocation_ratio"), "meta.dynamic_allocation_ratio", lo=0.0, hi=1.0)

    # Equity/DD trio is STRICT-required in execution_engine; guard here if present
    if "equity_current_usdt" in meta and meta.get("equity_current_usdt") is not None:
        _require_float_min(meta.get("equity_current_usdt"), "meta.equity_current_usdt", min_value=0.0)
    if "equity_peak_usdt" in meta and meta.get("equity_peak_usdt") is not None:
        _require_float_min(meta.get("equity_peak_usdt"), "meta.equity_peak_usdt", min_value=0.0)

    if (
        "equity_current_usdt" in meta
        and meta.get("equity_current_usdt") is not None
        and "equity_peak_usdt" in meta
        and meta.get("equity_peak_usdt") is not None
        and "dd_pct" in meta
        and meta.get("dd_pct") is not None
    ):
        dd_expected = _calc_drawdown_pct_strict(
            equity_current_usdt=float(meta["equity_current_usdt"]),
            equity_peak_usdt=float(meta["equity_peak_usdt"]),
        )
        dd_actual = float(meta["dd_pct"])
        if abs(dd_expected - dd_actual) > _DD_PCT_ABS_TOL:
            raise InvariantViolation(
                f"meta.dd_pct inconsistent with equity values (STRICT): expected={dd_expected:.6f} actual={dd_actual:.6f}"
            )


__all__ = [
    "InvariantViolation",
    "SignalInvariantInputs",
    "ExecutionInvariantInputs",
    "validate_signal_invariants_strict",
    "validate_execution_invariants_strict",
    "validate_signal_from_meta_strict",
]