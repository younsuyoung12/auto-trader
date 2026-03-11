# engine/cycles/execution_cycle.py
"""
============================================================
FILE: engine/cycles/execution_cycle.py
ROLE:
- trading engine execution cycle
- risk cycle 이 승인한 finalized signal packet 을 실제 execution layer 에 전달한다

CORE RESPONSIBILITIES:
- pending risk packet 소비
- execution latency 측정 및 상한 검증
- ExecutionEngine.execute(signal_final) 호출
- non-fatal entry rejection 을 explicit SKIP 으로 처리
- 성공 시 OPEN_TRADES 반영
- execution 후 equity cache invalidation 호출

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- execution cycle 은 risk 승인된 packet 만 실행한다
- hidden default / silent continue / 예외 삼키기 금지
- execution latency 초과는 즉시 SAFE_STOP
- invalid trade return / invalid packet contract 는 즉시 예외
- ExecutionEngine non-fatal None 반환은 명시적 SKIP 처리

CHANGE HISTORY:
- 2026-03-12:
  1) FIX(ROOT-CAUSE): Signal 타입 import 누락 수정
  2) KEEP(STRICT): execution packet / signal contract strict validation 유지
============================================================
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Any, Callable, List, Optional

from infra.telelog import log, send_tg
from infra.async_worker import submit as submit_async
from execution.execution_engine import ExecutionEngine
from state.trader_state import Trade
from strategy.signal import Signal

from engine.engine_loop import EngineLoopRuntime
from engine.cycles.risk_cycle import (
    RiskCyclePacket,
    consume_pending_risk_packet_or_none,
)


class ExecutionCycleError(RuntimeError):
    """Base error for execution cycle."""


class ExecutionCycleContractError(ExecutionCycleError):
    """Raised when execution cycle contract is violated."""


@dataclass(frozen=True)
class ExecutionCycleConfig:
    max_exec_latency_ms: float
    rejection_tg_cooldown_sec: int

    def validate_or_raise(self) -> None:
        _require_float(self.max_exec_latency_ms, "config.max_exec_latency_ms", min_value=1.0)
        _require_int(self.rejection_tg_cooldown_sec, "config.rejection_tg_cooldown_sec", min_value=1)


@dataclass
class ExecutionCycleState:
    last_rejection_tg_ts: float = 0.0
    last_rejection_tg_key: str = ""

    def validate_or_raise(self) -> None:
        _require_float(self.last_rejection_tg_ts, "state.last_rejection_tg_ts", min_value=0.0)
        if not isinstance(self.last_rejection_tg_key, str):
            raise ExecutionCycleContractError("state.last_rejection_tg_key must be str (STRICT)")


@dataclass(frozen=True)
class ExecutionCycleContext:
    settings: Any
    execution_engine: ExecutionEngine
    open_trades_ref: List[Trade]
    invalidate_equity_cache_fn: Callable[[], None]
    config: ExecutionCycleConfig
    state: ExecutionCycleState

    def validate_or_raise(self) -> None:
        if self.settings is None:
            raise ExecutionCycleContractError("context.settings is required (STRICT)")
        if not isinstance(self.execution_engine, ExecutionEngine):
            raise ExecutionCycleContractError("context.execution_engine must be ExecutionEngine (STRICT)")
        if not isinstance(self.open_trades_ref, list):
            raise ExecutionCycleContractError("context.open_trades_ref must be list (STRICT)")
        if not callable(self.invalidate_equity_cache_fn):
            raise ExecutionCycleContractError("context.invalidate_equity_cache_fn is required (STRICT)")

        self.config.validate_or_raise()
        self.state.validate_or_raise()

        required_setting_names = (
            "symbol",
            "max_exec_latency_ms",
        )
        for name in required_setting_names:
            if not hasattr(self.settings, name):
                raise ExecutionCycleContractError(f"settings.{name} is required (STRICT)")


def build_execution_cycle_context_or_raise(
    *,
    settings: Any,
    open_trades_ref: List[Trade],
    invalidate_equity_cache_fn: Callable[[], None],
) -> ExecutionCycleContext:
    ctx = ExecutionCycleContext(
        settings=settings,
        execution_engine=ExecutionEngine(settings),
        open_trades_ref=open_trades_ref,
        invalidate_equity_cache_fn=invalidate_equity_cache_fn,
        config=ExecutionCycleConfig(
            max_exec_latency_ms=float(_require_attr(settings, "max_exec_latency_ms", "settings")),
            rejection_tg_cooldown_sec=60,
        ),
        state=ExecutionCycleState(),
    )
    ctx.validate_or_raise()
    return ctx


def build_execution_cycle_fn(ctx: ExecutionCycleContext) -> Callable[[float, EngineLoopRuntime], None]:
    ctx.validate_or_raise()

    def _fn(now_ts: float, runtime: EngineLoopRuntime) -> None:
        run_execution_cycle_or_raise(now_ts, runtime, ctx)

    return _fn


def run_execution_cycle_or_raise(
    now_ts: float,
    runtime: EngineLoopRuntime,
    ctx: ExecutionCycleContext,
) -> None:
    ctx.validate_or_raise()
    runtime.validate_or_raise()
    _ = _require_float(now_ts, "now_ts", min_value=0.0)

    risk_packet = consume_pending_risk_packet_or_none(runtime)
    if risk_packet is None:
        return

    _validate_risk_packet_or_raise(risk_packet)

    if runtime.safe_stop_requested:
        msg = (
            "[SKIP][EXECUTION_ABORTED_SAFE_STOP] "
            f"symbol={risk_packet.symbol} reason=safe_stop_requested_before_execution"
        )
        log(msg)
        return

    if ctx.open_trades_ref:
        raise ExecutionCycleContractError(
            f"execution cycle requires empty OPEN_TRADES before entry execution (STRICT), got={len(ctx.open_trades_ref)}"
        )

    t0 = time.perf_counter()
    trade = ctx.execution_engine.execute(risk_packet.signal_final)
    dt_ms = (time.perf_counter() - t0) * 1000.0

    if dt_ms > ctx.config.max_exec_latency_ms:
        runtime.safe_stop_requested = True
        msg = f"[SAFE_STOP][LATENCY_EXEC] exec_ms={dt_ms:.1f} > {ctx.config.max_exec_latency_ms:.1f}"
        log(msg)
        _safe_send_tg(msg)
        raise ExecutionCycleError(msg)

    if trade is None:
        direction = _require_nonempty_str(
            _require_attr(risk_packet.signal_final, "direction", "signal_final"),
            "signal_final.direction",
        )
        msg = (
            "[SKIP][ENTRY_REJECTED_NONFATAL] "
            f"symbol={risk_packet.symbol} direction={direction} reason=nonfatal_entry_rejection"
        )
        log(msg)
        _maybe_send_rejection_tg(ctx, "ENTRY_REJECTED_NONFATAL", msg)
        return

    if not isinstance(trade, Trade):
        raise ExecutionCycleContractError(
            f"ExecutionEngine returned invalid type (STRICT): {type(trade).__name__}"
        )

    trade_symbol = _normalize_symbol_strict(_require_attr(trade, "symbol", "trade"), name="trade.symbol")
    if trade_symbol != risk_packet.symbol:
        raise ExecutionCycleContractError(
            f"trade.symbol mismatch vs risk packet (STRICT): trade={trade_symbol} packet={risk_packet.symbol}"
        )

    ctx.open_trades_ref.append(trade)
    ctx.invalidate_equity_cache_fn()


def _validate_risk_packet_or_raise(packet: RiskCyclePacket) -> None:
    if not isinstance(packet, RiskCyclePacket):
        raise ExecutionCycleContractError("risk packet must be RiskCyclePacket (STRICT)")

    _ = _normalize_symbol_strict(packet.symbol, name="risk_packet.symbol")
    if not isinstance(packet.market_data, dict):
        raise ExecutionCycleContractError("risk_packet.market_data must be dict (STRICT)")
    _ = _require_float(packet.created_at_ts, "risk_packet.created_at_ts", min_value=0.0)
    _ = _require_float(packet.available_usdt, "risk_packet.available_usdt", min_value=0.0)
    _ = _require_float(packet.entry_price_hint, "risk_packet.entry_price_hint", min_value=0.0)
    _ = _require_nonempty_str(packet.entry_price_source, "risk_packet.entry_price_source")
    _ = _require_nonempty_str(packet.hard_risk_reason, "risk_packet.hard_risk_reason")
    if not isinstance(packet.hard_risk_extra, dict):
        raise ExecutionCycleContractError("risk_packet.hard_risk_extra must be dict (STRICT)")

    signal_final = packet.signal_final
    if not isinstance(signal_final, Signal):
        raise ExecutionCycleContractError("risk_packet.signal_final must be Signal (STRICT)")

    action = _require_nonempty_str(_require_attr(signal_final, "action", "signal_final"), "signal_final.action").upper()
    if action != "ENTER":
        raise ExecutionCycleContractError(f"signal_final.action must be ENTER (STRICT), got={action!r}")

    direction = _require_nonempty_str(_require_attr(signal_final, "direction", "signal_final"), "signal_final.direction").upper()
    if direction not in ("LONG", "SHORT"):
        raise ExecutionCycleContractError(f"signal_final.direction invalid (STRICT): {direction!r}")

    tp_pct = _require_float(_require_attr(signal_final, "tp_pct", "signal_final"), "signal_final.tp_pct", min_value=0.0)
    sl_pct = _require_float(_require_attr(signal_final, "sl_pct", "signal_final"), "signal_final.sl_pct", min_value=0.0)
    risk_pct = _require_float(_require_attr(signal_final, "risk_pct", "signal_final"), "signal_final.risk_pct", min_value=0.0)

    if tp_pct <= 0.0:
        raise ExecutionCycleContractError("signal_final.tp_pct must be > 0 (STRICT)")
    if sl_pct <= 0.0:
        raise ExecutionCycleContractError("signal_final.sl_pct must be > 0 (STRICT)")
    if risk_pct <= 0.0:
        raise ExecutionCycleContractError("signal_final.risk_pct must be > 0 (STRICT)")

    meta = _require_attr(signal_final, "meta", "signal_final")
    if not isinstance(meta, dict):
        raise ExecutionCycleContractError("signal_final.meta must be dict (STRICT)")
    if "entry_price_hint" not in meta:
        raise ExecutionCycleContractError("signal_final.meta.entry_price_hint missing (STRICT)")
    if "entry_price_source" not in meta:
        raise ExecutionCycleContractError("signal_final.meta.entry_price_source missing (STRICT)")
    _ = _require_float(meta["entry_price_hint"], "signal_final.meta.entry_price_hint", min_value=0.0)
    _ = _require_nonempty_str(meta["entry_price_source"], "signal_final.meta.entry_price_source")


def _require_bool(v: Any, name: str) -> bool:
    if not isinstance(v, bool):
        raise ExecutionCycleContractError(f"{name} must be bool (STRICT)")
    return bool(v)


def _require_int(v: Any, name: str, *, min_value: Optional[int] = None) -> int:
    if isinstance(v, bool):
        raise ExecutionCycleContractError(f"{name} must be int (bool not allowed)")
    try:
        iv = int(v)
    except Exception as e:
        raise ExecutionCycleContractError(f"{name} must be int (STRICT): {e}") from e
    if min_value is not None and iv < min_value:
        raise ExecutionCycleContractError(f"{name} must be >= {min_value} (STRICT)")
    return int(iv)


def _require_float(v: Any, name: str, *, min_value: Optional[float] = None) -> float:
    if isinstance(v, bool):
        raise ExecutionCycleContractError(f"{name} must be numeric (bool not allowed)")
    try:
        x = float(v)
    except Exception as e:
        raise ExecutionCycleContractError(f"{name} must be numeric (STRICT): {e}") from e
    if not math.isfinite(x):
        raise ExecutionCycleContractError(f"{name} must be finite (STRICT)")
    if min_value is not None and x < min_value:
        raise ExecutionCycleContractError(f"{name} must be >= {min_value} (STRICT)")
    return float(x)


def _require_nonempty_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        raise ExecutionCycleContractError(f"{name} is empty (STRICT)")
    return s


def _normalize_symbol_strict(symbol: Any, *, name: str = "symbol") -> str:
    s = str(symbol or "").replace("-", "").replace("/", "").upper().strip()
    if not s:
        raise ExecutionCycleContractError(f"{name} is empty (STRICT)")
    return s


def _require_attr(obj: Any, attr_name: str, owner_name: str) -> Any:
    if obj is None:
        raise ExecutionCycleContractError(f"{owner_name} is None (STRICT)")
    if not hasattr(obj, attr_name):
        raise ExecutionCycleContractError(f"{owner_name}.{attr_name} missing (STRICT)")
    return getattr(obj, attr_name)


def _safe_send_tg(msg: str) -> None:
    try:
        ok = submit_async(send_tg, msg, critical=False, label="send_tg")
        if not ok:
            log(f"[TG][DROP] async queue full: {msg}")
    except Exception as e:
        log(f"[TG] async submit error: {type(e).__name__}: {e} | msg={msg}")


def _maybe_send_rejection_tg(
    ctx: ExecutionCycleContext,
    key: str,
    msg: str,
) -> None:
    now = time.time()
    if key == ctx.state.last_rejection_tg_key and (now - ctx.state.last_rejection_tg_ts) < ctx.config.rejection_tg_cooldown_sec:
        log(f"[SKIP_TG_SUPPRESS][EXEC] {msg}")
        return
    ctx.state.last_rejection_tg_key = str(key)
    ctx.state.last_rejection_tg_ts = now
    _safe_send_tg(msg)


__all__ = [
    "ExecutionCycleError",
    "ExecutionCycleContractError",
    "ExecutionCycleConfig",
    "ExecutionCycleState",
    "ExecutionCycleContext",
    "build_execution_cycle_context_or_raise",
    "build_execution_cycle_fn",
    "run_execution_cycle_or_raise",
]