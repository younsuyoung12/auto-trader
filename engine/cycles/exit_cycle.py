# engine/cycles/exit_cycle.py
"""
============================================================
FILE: engine/cycles/exit_cycle.py
ROLE:
- trading engine exit cycle
- open position 상태에서 1m candle 기준 청산 감시 전용 계층

CORE RESPONSIBILITIES:
- OPEN_TRADES 존재 여부 검사
- 1m ws candle ts 기준 exit cadence 제어
- ts rollback 감지
- state.exit_engine.maybe_exit_with_gpt(...) 호출
- 청산 성공 시 OPEN_TRADES 제거
- equity cache invalidation 및 last_close_ts 반영
- engine_loop open-position branch 를 전담

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- exit cycle 은 open position 이 있을 때만 동작한다
- 1m candle ts rollback 은 즉시 SAFE_STOP
- maybe_exit_with_gpt 의 반환 계약 위반은 즉시 예외
- hidden default / silent continue / 예외 삼키기 금지
- SAFE_STOP 상태 전이는 runtime.request_safe_stop(...) 만 사용한다
============================================================

CHANGE HISTORY:
- 2026-03-13:
  1) FIX(STATE-MACHINE): ts rollback 시 runtime.safe_stop_requested 직접 수정 제거 → runtime.request_safe_stop(...) 사용
  2) FIX(CONTRACT): exit cycle 실행 상태를 RUNNING/SAFE_STOP/RECOVERY 에서만 허용
  3) FIX(STRICT): open position branch 계약 명확화
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Callable, List, Optional

from infra.telelog import log, send_tg
from infra.async_worker import submit as submit_async
from infra.market_data_ws import get_klines_with_volume as ws_get_klines_with_volume
from state.exit_engine import maybe_exit_with_gpt
from state.trader_state import Trade

from engine.engine_loop import (
    ENGINE_STATE_RECOVERY,
    ENGINE_STATE_RUNNING,
    ENGINE_STATE_SAFE_STOP,
    EngineLoopRuntime,
)


class ExitCycleError(RuntimeError):
    """Base error for exit cycle."""


class ExitCycleContractError(ExitCycleError):
    """Raised when exit cycle contract is violated."""


@dataclass(frozen=True)
class ExitCycleConfig:
    exit_check_interval_tf: str = "1m"

    def validate_or_raise(self) -> None:
        tf = _require_nonempty_str(self.exit_check_interval_tf, "config.exit_check_interval_tf")
        if tf != "1m":
            raise ExitCycleContractError("config.exit_check_interval_tf must be '1m' (STRICT)")


@dataclass
class ExitCycleState:
    last_exit_candle_ts_1m: Optional[int] = None

    def validate_or_raise(self) -> None:
        _require_optional_positive_int(self.last_exit_candle_ts_1m, "state.last_exit_candle_ts_1m")


@dataclass(frozen=True)
class ExitCycleContext:
    settings: Any
    symbol: str
    open_trades_ref: List[Trade]
    invalidate_equity_cache_fn: Callable[[], None]
    on_trade_closed_fn: Callable[[Trade, float], None]
    config: ExitCycleConfig
    state: ExitCycleState

    def validate_or_raise(self) -> None:
        _ = _normalize_symbol_strict(self.symbol, name="context.symbol")

        if self.settings is None:
            raise ExitCycleContractError("context.settings is required (STRICT)")
        if not isinstance(self.open_trades_ref, list):
            raise ExitCycleContractError("context.open_trades_ref must be list (STRICT)")
        if not callable(self.invalidate_equity_cache_fn):
            raise ExitCycleContractError("context.invalidate_equity_cache_fn is required (STRICT)")
        if not callable(self.on_trade_closed_fn):
            raise ExitCycleContractError("context.on_trade_closed_fn is required (STRICT)")

        self.config.validate_or_raise()
        self.state.validate_or_raise()


def build_exit_cycle_context_or_raise(
    *,
    settings: Any,
    symbol: str,
    open_trades_ref: List[Trade],
    invalidate_equity_cache_fn: Callable[[], None],
    on_trade_closed_fn: Callable[[Trade, float], None],
    last_exit_candle_ts_1m: Optional[int],
) -> ExitCycleContext:
    ctx = ExitCycleContext(
        settings=settings,
        symbol=_normalize_symbol_strict(symbol),
        open_trades_ref=open_trades_ref,
        invalidate_equity_cache_fn=invalidate_equity_cache_fn,
        on_trade_closed_fn=on_trade_closed_fn,
        config=ExitCycleConfig(exit_check_interval_tf="1m"),
        state=ExitCycleState(last_exit_candle_ts_1m=last_exit_candle_ts_1m),
    )
    ctx.validate_or_raise()
    return ctx


def build_open_position_cycle_fn(ctx: ExitCycleContext) -> Callable[[float, EngineLoopRuntime], bool]:
    ctx.validate_or_raise()

    def _fn(now_ts: float, runtime: EngineLoopRuntime) -> bool:
        return run_open_position_cycle_or_raise(now_ts, runtime, ctx)

    return _fn


def run_open_position_cycle_or_raise(
    now_ts: float,
    runtime: EngineLoopRuntime,
    ctx: ExitCycleContext,
) -> bool:
    ctx.validate_or_raise()
    runtime.validate_or_raise()
    now_f = _require_float(now_ts, "now_ts", min_value=0.0)

    if runtime.engine_state not in (
        ENGINE_STATE_RUNNING,
        ENGINE_STATE_SAFE_STOP,
        ENGINE_STATE_RECOVERY,
    ):
        raise ExitCycleContractError(
            f"exit cycle invalid runtime.engine_state (STRICT): {runtime.engine_state!r}"
        )

    if not ctx.open_trades_ref:
        return False

    if len(ctx.open_trades_ref) != 1:
        raise ExitCycleContractError(
            f"OPEN_TRADES must be <= 1 in one-way mode (STRICT), got={len(ctx.open_trades_ref)}"
        )

    rows = ws_get_klines_with_volume(ctx.symbol, "1m", limit=1)
    if not isinstance(rows, list):
        raise ExitCycleContractError("ws_get_klines_with_volume(1m) must return list (STRICT)")

    if not rows:
        return True

    row = rows[0]
    if not isinstance(row, (list, tuple)) or len(row) < 1:
        raise ExitCycleContractError("1m ws kline row invalid (STRICT)")

    last_ts_ms = _require_int_ms(row[0], "ws[1m].openTime")

    prev_ts = ctx.state.last_exit_candle_ts_1m
    if prev_ts is None:
        ctx.state.last_exit_candle_ts_1m = int(last_ts_ms)
        return True

    if last_ts_ms < prev_ts:
        runtime.request_safe_stop("EXIT_CYCLE_TS_ROLLBACK", now_ts=now_f)
        msg = f"[SAFE_STOP][TS_ROLLBACK] 1m ts rollback: prev={prev_ts} now={last_ts_ms}"
        log(msg)
        _safe_send_tg(msg)
        raise ExitCycleError(msg)

    if last_ts_ms == prev_ts:
        return True

    for trade in list(ctx.open_trades_ref):
        if not isinstance(trade, Trade):
            raise ExitCycleContractError(
                f"OPEN_TRADES item must be Trade (STRICT), got={type(trade).__name__}"
            )

        exit_result = maybe_exit_with_gpt(trade, ctx.settings, scenario="RUNTIME_EXIT_CHECK")
        if not isinstance(exit_result, bool):
            raise ExitCycleContractError("maybe_exit_with_gpt must return bool (STRICT)")

        if exit_result:
            ctx.open_trades_ref[:] = [ot for ot in ctx.open_trades_ref if ot is not trade]
            ctx.invalidate_equity_cache_fn()
            ctx.on_trade_closed_fn(trade, now_f)

    ctx.state.last_exit_candle_ts_1m = int(last_ts_ms)
    return True


def _require_int(v: Any, name: str, *, min_value: Optional[int] = None) -> int:
    if isinstance(v, bool):
        raise ExitCycleContractError(f"{name} must be int (bool not allowed)")
    try:
        iv = int(v)
    except Exception as e:
        raise ExitCycleContractError(f"{name} must be int (STRICT): {e}") from e
    if min_value is not None and iv < min_value:
        raise ExitCycleContractError(f"{name} must be >= {min_value} (STRICT)")
    return int(iv)


def _require_int_ms(v: Any, name: str) -> int:
    return _require_int(v, name, min_value=1)


def _require_float(v: Any, name: str, *, min_value: Optional[float] = None) -> float:
    if isinstance(v, bool):
        raise ExitCycleContractError(f"{name} must be numeric (bool not allowed)")
    try:
        x = float(v)
    except Exception as e:
        raise ExitCycleContractError(f"{name} must be numeric (STRICT): {e}") from e
    if not math.isfinite(x):
        raise ExitCycleContractError(f"{name} must be finite (STRICT)")
    if min_value is not None and x < min_value:
        raise ExitCycleContractError(f"{name} must be >= {min_value} (STRICT)")
    return float(x)


def _require_optional_positive_int(v: Any, name: str) -> Optional[int]:
    if v is None:
        return None
    return _require_int(v, name, min_value=1)


def _require_nonempty_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        raise ExitCycleContractError(f"{name} is empty (STRICT)")
    return s


def _normalize_symbol_strict(symbol: Any, *, name: str = "symbol") -> str:
    s = str(symbol or "").replace("-", "").replace("/", "").upper().strip()
    if not s:
        raise ExitCycleContractError(f"{name} is empty (STRICT)")
    return s


def _safe_send_tg(msg: str) -> None:
    try:
        ok = submit_async(send_tg, msg, critical=False, label="send_tg")
        if not ok:
            log(f"[TG][DROP] async queue full: {msg}")
    except Exception as e:
        log(f"[TG] async submit error: {type(e).__name__}: {e} | msg={msg}")


__all__ = [
    "ExitCycleError",
    "ExitCycleContractError",
    "ExitCycleConfig",
    "ExitCycleState",
    "ExitCycleContext",
    "build_exit_cycle_context_or_raise",
    "build_open_position_cycle_fn",
    "run_open_position_cycle_or_raise",
]