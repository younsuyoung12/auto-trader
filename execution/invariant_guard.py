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
- 2026-03-04:
  1) 신규 생성: 엔진 핵심 수학 값 Invariant 검증 레이어 추가
  2) allocation/risk_pct, multiplier, dd_pct, micro_score_risk, tp/sl 등 범위/finite 강제
  3) execution 관련 qty/notional/slippage 검증 추가(실행 직전/직후 재검증용)
========================================================
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, Tuple


class InvariantViolation(RuntimeError):
    """수학적 무결성(invariant) 위반(STRICT)."""


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


def _require_tzaware_dt(v: Any, name: str) -> datetime:
    if not isinstance(v, datetime):
        raise InvariantViolation(f"{name} must be datetime (STRICT)")
    if v.tzinfo is None or v.tzinfo.utcoffset(v) is None:
        raise InvariantViolation(f"{name} must be timezone-aware (STRICT)")
    return v


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
    slippage_pct: Optional[float] = None  # >=0 finite
    spread_pct_snapshot: Optional[float] = None  # >=0 finite (if present)


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
        _require_float_range(x.final_risk_multiplier, "meta.final_risk_multiplier", lo=0.0, hi=1.0)

    if x.equity_current_usdt is not None:
        _require_float_min(x.equity_current_usdt, "meta.equity_current_usdt", min_value=0.0)
    if x.equity_peak_usdt is not None:
        _require_float_min(x.equity_peak_usdt, "meta.equity_peak_usdt", min_value=0.0)

    # Equity relation if both present
    if x.equity_current_usdt is not None and x.equity_peak_usdt is not None:
        cur = float(x.equity_current_usdt)
        peak = float(x.equity_peak_usdt)
        if peak <= 0.0 or cur <= 0.0:
            raise InvariantViolation("equity values must be > 0 when both provided (STRICT)")
        if cur > peak + 1e-9:
            # allow tiny float noise only
            raise InvariantViolation(f"equity_current_usdt > equity_peak_usdt (STRICT): cur={cur} peak={peak}")


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

    # Slippage / spread constraints (if present)
    if x.slippage_pct is not None:
        s = _require_float_min(x.slippage_pct, "exec.slippage_pct", min_value=0.0)
        if s > 10.0:
            # 1000% 같은 터무니 없는 값은 계산/입력 오류로 본다.
            raise InvariantViolation(f"exec.slippage_pct too large (STRICT): {s}")

    if x.spread_pct_snapshot is not None:
        sp = _require_float_min(x.spread_pct_snapshot, "exec.spread_pct_snapshot", min_value=0.0)
        if sp > 1.0:
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


__all__ = [
    "InvariantViolation",
    "SignalInvariantInputs",
    "ExecutionInvariantInputs",
    "validate_signal_invariants_strict",
    "validate_execution_invariants_strict",
    "validate_signal_from_meta_strict",
]