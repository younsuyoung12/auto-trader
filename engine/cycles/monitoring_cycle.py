# engine/cycles/monitoring_cycle.py
"""
============================================================
FILE: engine/cycles/monitoring_cycle.py
ROLE:
- trading engine monitoring cycle
- ws/account/protection/reconcile 감시 전용 계층

CORE RESPONSIBILITIES:
- websocket transport liveness 검증
- account websocket 연결 상태 검증
- protection order 존재 및 정합성 검증
- reconcile engine 주기 실행
- SIGTERM grace deadline 도달 시 강제 정리
- monitoring failure 발생 시 SAFE_STOP 승격

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- monitoring cycle 은 entry/risk/execution 세부 구현을 가지지 않는다
- contract violation / connectivity failure / protection missing 은 즉시 예외 처리한다
- force close 는 설정에 명시된 경우에만 실행한다
- hidden default / silent continue / 예외 삼키기 금지
============================================================
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Protocol

from infra.telelog import log, send_tg
from infra.async_worker import submit as submit_async
from infra.account_ws import (
    get_account_protection_orders_snapshot,
    get_account_ws_status,
)
from infra.market_data_ws import get_ws_status as ws_get_ws_status
from execution.exchange_api import (
    fetch_open_orders,
    fetch_open_positions,
)
from engine.engine_loop import EngineLoopRuntime


DEFAULT_WS_LIVENESS_FAIL_HARDSTOP_N: int = 15
DEFAULT_ACCOUNT_WS_FAIL_HARDSTOP_N: int = 3
DEFAULT_PROTECTION_GUARD_INTERVAL_SEC: float = 1.0


class MonitoringCycleError(RuntimeError):
    """Base error for monitoring cycle."""


class MonitoringContractError(MonitoringCycleError):
    """Raised when monitoring cycle contract is violated."""


class ForceCloseFn(Protocol):
    def __call__(self, symbol: str) -> int: ...


def _require_bool(v: Any, name: str) -> bool:
    if not isinstance(v, bool):
        raise MonitoringContractError(f"{name} must be bool (STRICT)")
    return bool(v)


def _require_float(v: Any, name: str, *, min_value: Optional[float] = None) -> float:
    if isinstance(v, bool):
        raise MonitoringContractError(f"{name} must be numeric (bool not allowed)")
    try:
        x = float(v)
    except Exception as e:
        raise MonitoringContractError(f"{name} must be numeric (STRICT): {e}") from e
    if not math.isfinite(x):
        raise MonitoringContractError(f"{name} must be finite (STRICT)")
    if min_value is not None and x < min_value:
        raise MonitoringContractError(f"{name} must be >= {min_value} (STRICT)")
    return float(x)


def _require_int(v: Any, name: str, *, min_value: Optional[int] = None) -> int:
    if isinstance(v, bool):
        raise MonitoringContractError(f"{name} must be int (bool not allowed)")
    try:
        iv = int(v)
    except Exception as e:
        raise MonitoringContractError(f"{name} must be int (STRICT): {e}") from e
    if min_value is not None and iv < min_value:
        raise MonitoringContractError(f"{name} must be >= {min_value} (STRICT)")
    return int(iv)


def _require_nonempty_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        raise MonitoringContractError(f"{name} is empty (STRICT)")
    return s


def _normalize_symbol_strict(symbol: Any, *, name: str = "symbol") -> str:
    s = str(symbol or "").replace("-", "").replace("/", "").upper().strip()
    if not s:
        raise MonitoringContractError(f"{name} is empty (STRICT)")
    return s


def _join_reason_list_strict(v: Any, name: str) -> str:
    if not isinstance(v, list):
        raise MonitoringContractError(f"{name} must be list (STRICT)")
    cleaned = [str(x).strip() for x in v if str(x).strip()]
    return " | ".join(cleaned)


def _safe_send_tg(msg: str) -> None:
    try:
        ok = submit_async(send_tg, msg, critical=False, label="send_tg")
        if not ok:
            log(f"[TG][DROP] async queue full: {msg}")
    except Exception as e:
        log(f"[TG] async submit error: {type(e).__name__}: {e} | msg={msg}")


@dataclass(frozen=True)
class MonitoringCycleConfig:
    protection_guard_interval_sec: float
    ws_liveness_fail_hardstop_n: int
    account_ws_fail_hardstop_n: int

    def validate_or_raise(self) -> None:
        _require_float(
            self.protection_guard_interval_sec,
            "config.protection_guard_interval_sec",
            min_value=0.001,
        )
        _require_int(
            self.ws_liveness_fail_hardstop_n,
            "config.ws_liveness_fail_hardstop_n",
            min_value=1,
        )
        _require_int(
            self.account_ws_fail_hardstop_n,
            "config.account_ws_fail_hardstop_n",
            min_value=1,
        )


@dataclass
class MonitoringCycleState:
    ws_liveness_consec_fails: int = 0
    account_ws_consec_fails: int = 0
    last_protection_guard_ts: float = 0.0
    sigterm_deadline_handled: bool = False

    def validate_or_raise(self) -> None:
        _require_int(self.ws_liveness_consec_fails, "state.ws_liveness_consec_fails", min_value=0)
        _require_int(self.account_ws_consec_fails, "state.account_ws_consec_fails", min_value=0)
        _require_float(self.last_protection_guard_ts, "state.last_protection_guard_ts", min_value=0.0)
        _require_bool(self.sigterm_deadline_handled, "state.sigterm_deadline_handled")


@dataclass(frozen=True)
class MonitoringCycleContext:
    settings: Any
    symbol: str
    open_trades_ref: List[Any]
    reconcile_engine: Any
    force_close_fn: ForceCloseFn
    config: MonitoringCycleConfig
    state: MonitoringCycleState

    def validate_or_raise(self) -> None:
        _ = _normalize_symbol_strict(self.symbol, name="context.symbol")

        if self.settings is None:
            raise MonitoringContractError("context.settings is required (STRICT)")

        if not isinstance(self.open_trades_ref, list):
            raise MonitoringContractError("context.open_trades_ref must be list (STRICT)")

        if self.reconcile_engine is None:
            raise MonitoringContractError("context.reconcile_engine is required (STRICT)")
        run_if_due = getattr(self.reconcile_engine, "run_if_due", None)
        if not callable(run_if_due):
            raise MonitoringContractError("context.reconcile_engine.run_if_due callable is required (STRICT)")

        if not callable(self.force_close_fn):
            raise MonitoringContractError("context.force_close_fn is required (STRICT)")

        self.config.validate_or_raise()
        self.state.validate_or_raise()

        if not hasattr(self.settings, "force_close_on_desync"):
            raise MonitoringContractError("settings.force_close_on_desync is required (STRICT)")


def build_monitoring_cycle_context_or_raise(
    *,
    settings: Any,
    symbol: str,
    open_trades_ref: List[Any],
    reconcile_engine: Any,
    force_close_fn: ForceCloseFn,
) -> MonitoringCycleContext:
    ctx = MonitoringCycleContext(
        settings=settings,
        symbol=_normalize_symbol_strict(symbol),
        open_trades_ref=open_trades_ref,
        reconcile_engine=reconcile_engine,
        force_close_fn=force_close_fn,
        config=MonitoringCycleConfig(
            protection_guard_interval_sec=DEFAULT_PROTECTION_GUARD_INTERVAL_SEC,
            ws_liveness_fail_hardstop_n=DEFAULT_WS_LIVENESS_FAIL_HARDSTOP_N,
            account_ws_fail_hardstop_n=DEFAULT_ACCOUNT_WS_FAIL_HARDSTOP_N,
        ),
        state=MonitoringCycleState(),
    )
    ctx.validate_or_raise()
    return ctx


def _normalize_position_side_guard(v: Any, name: str) -> str:
    s = str(v or "").upper().strip()
    if s not in ("BOTH", "LONG", "SHORT"):
        raise MonitoringContractError(f"{name} must be BOTH/LONG/SHORT (STRICT), got={s!r}")
    return s


def _position_side_compatible(expected_position_side: str, order_position_side: str) -> bool:
    exp = _normalize_position_side_guard(expected_position_side, "expected_position_side")
    got = _normalize_position_side_guard(order_position_side, "order_position_side")

    if exp in ("LONG", "SHORT") and got in ("LONG", "SHORT"):
        return got == exp
    if exp == "BOTH":
        return got == "BOTH"
    if exp in ("LONG", "SHORT") and got == "BOTH":
        return True
    return False


def _exchange_position_is_open_strict(symbol: str) -> bool:
    sym = _normalize_symbol_strict(symbol)
    rows = fetch_open_positions(sym)
    if not isinstance(rows, list):
        raise MonitoringContractError("fetch_open_positions returned non-list (STRICT)")

    for r in rows:
        if not isinstance(r, dict):
            raise MonitoringContractError("fetch_open_positions contains non-dict row (STRICT)")
        if str(r.get("symbol") or "").upper().strip() != sym:
            continue
        if "positionAmt" not in r:
            raise MonitoringContractError("positionRisk.positionAmt missing (STRICT)")
        try:
            amt = float(r["positionAmt"])
        except Exception as e:
            raise MonitoringContractError(f"positionAmt parse failed (STRICT): {e}") from e
        if abs(amt) > 1e-12:
            return True
    return False


def _is_rest_protection_order_row_strict(order: Dict[str, Any], *, symbol: str, expected_position_side: str) -> bool:
    if not isinstance(order, dict):
        raise MonitoringContractError("open order row must be dict (STRICT)")

    sym = _normalize_symbol_strict(symbol)
    row_symbol = str(order.get("symbol") or "").upper().strip()
    if row_symbol != sym:
        return False

    order_type = str(order.get("type") or "").upper().strip()
    if order_type not in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT", "STOP_MARKET", "STOP"):
        return False

    reduce_only = bool(order.get("reduceOnly", False))
    close_position = bool(order.get("closePosition", False))
    if not reduce_only and not close_position:
        return False

    row_ps = str(order.get("positionSide") or "").upper().strip() or "BOTH"
    return _position_side_compatible(expected_position_side, row_ps)


def _has_account_ws_protection_order_strict(symbol: str, order_id: str, expected_position_side: str) -> bool:
    rows = get_account_protection_orders_snapshot(symbol, limit=200)
    if not isinstance(rows, list):
        raise MonitoringContractError("get_account_protection_orders_snapshot returned non-list (STRICT)")

    wanted = _require_nonempty_str(order_id, "order_id")
    for row in rows:
        if not isinstance(row, dict):
            raise MonitoringContractError("account protection order row must be dict (STRICT)")
        oid = str(row.get("order_id") or "").strip()
        if oid != wanted:
            continue

        row_ps = str(row.get("position_side") or "").upper().strip()
        if not row_ps:
            raise MonitoringContractError("account protection order missing position_side (STRICT)")
        if not _position_side_compatible(expected_position_side, row_ps):
            raise MonitoringContractError(
                f"account protection order positionSide mismatch (STRICT): order_id={wanted} row_ps={row_ps} expected={expected_position_side}"
            )
        return True

    return False


def _has_rest_protection_order_strict(symbol: str, order_id: str, expected_position_side: str) -> bool:
    sym = _normalize_symbol_strict(symbol)
    wanted = _require_nonempty_str(order_id, "order_id")

    orders = fetch_open_orders(sym)
    if not isinstance(orders, list):
        raise MonitoringContractError("fetch_open_orders returned non-list (STRICT)")

    for row in orders:
        if not isinstance(row, dict):
            raise MonitoringContractError("fetch_open_orders contains non-dict row (STRICT)")
        oid = row.get("orderId")
        if oid is None:
            raise MonitoringContractError("openOrders row missing orderId (STRICT)")
        if str(oid).strip() != wanted:
            continue

        if not _is_rest_protection_order_row_strict(
            row,
            symbol=sym,
            expected_position_side=expected_position_side,
        ):
            raise MonitoringContractError(
                f"open order exists but is not valid protection order (STRICT): order_id={wanted}"
            )
        return True

    return False


def _request_safe_stop_or_raise(runtime: EngineLoopRuntime, reason: str) -> None:
    if runtime is None:
        raise MonitoringContractError("runtime is required (STRICT)")
    _ = _require_nonempty_str(reason, "reason")
    runtime.safe_stop_requested = True


def _ws_liveness_guard_or_raise(
    now_ts: float,
    runtime: EngineLoopRuntime,
    ctx: MonitoringCycleContext,
) -> None:
    _ = _require_float(now_ts, "now_ts", min_value=0.0)
    ws_status = ws_get_ws_status(ctx.symbol)
    if not isinstance(ws_status, dict):
        raise MonitoringContractError("ws_get_ws_status returned non-dict (STRICT)")

    transport_ok = ws_status.get("transport_ok")
    market_feed_ok = ws_status.get("market_feed_ok")
    state = str(ws_status.get("state") or "").upper()
    
    if not isinstance(transport_ok, bool):
        raise MonitoringContractError("ws_status.transport_ok must be bool (STRICT)")
    if not isinstance(market_feed_ok, bool):
        raise MonitoringContractError("ws_status.market_feed_ok must be bool (STRICT)")

    # WS reconnect 과정은 정상 상태로 간주
    if transport_ok or state in ("CONNECTING", "RECONNECTING", "OPENING"):
        if ctx.state.ws_liveness_consec_fails != 0:
            log(f"[WS_LIVENESS][RECOVER] consecutive={ctx.state.ws_liveness_consec_fails} -> 0")
        ctx.state.ws_liveness_consec_fails = 0

        if not market_feed_ok:
            warnings = ws_status.get("warnings")
            if not isinstance(warnings, list):
                raise MonitoringContractError("ws_status.warnings must be list (STRICT)")
            
            joined = _join_reason_list_strict(warnings, "ws_status.warnings")
            if joined:
                log(f"[WS_LIVENESS][WARN] {joined}")
        return

    reasons = ws_status.get("reasons")
    if not isinstance(reasons, list):
        raise MonitoringContractError("ws_status.reasons must be list (STRICT)")
    joined = _join_reason_list_strict(reasons, "ws_status.reasons")

    ctx.state.ws_liveness_consec_fails += 1
    log(
        f"[WS_LIVENESS][FAIL] {joined} "
        f"consecutive={ctx.state.ws_liveness_consec_fails}/{ctx.config.ws_liveness_fail_hardstop_n}"
    )

    if ctx.state.ws_liveness_consec_fails >= ctx.config.ws_liveness_fail_hardstop_n:
        _request_safe_stop_or_raise(runtime, "WS_LIVENESS")
        msg = (
            "[SAFE_STOP][WS_LIVENESS] transport failure confirmed "
            f"consecutive={ctx.state.ws_liveness_consec_fails} reasons={joined}"
        )
        log(msg)
        _safe_send_tg(msg)
        raise MonitoringCycleError(msg)


def _account_ws_connected_guard_or_raise(
    runtime: EngineLoopRuntime,
    ctx: MonitoringCycleContext,
) -> None:
    st = get_account_ws_status()
    ok = bool(st.get("running")) and bool(st.get("connected")) and bool(st.get("listen_key_active"))

    if ok:
        if ctx.state.account_ws_consec_fails != 0:
            log(f"[ACCOUNT_WS][RECOVER] consecutive={ctx.state.account_ws_consec_fails} -> 0")
        ctx.state.account_ws_consec_fails = 0
        return

    ctx.state.account_ws_consec_fails += 1
    log(
        "[ACCOUNT_WS][FAIL] "
        f"running={st.get('running')} connected={st.get('connected')} "
        f"listen_key_active={st.get('listen_key_active')} "
        f"consecutive={ctx.state.account_ws_consec_fails}/{ctx.config.account_ws_fail_hardstop_n}"
    )

    if ctx.state.account_ws_consec_fails >= ctx.config.account_ws_fail_hardstop_n:
        _request_safe_stop_or_raise(runtime, "ACCOUNT_WS")
        msg = (
            "[SAFE_STOP][ACCOUNT_WS] account websocket disconnected or invalid "
            f"consecutive={ctx.state.account_ws_consec_fails}"
        )
        log(msg)
        _safe_send_tg(msg)
        raise MonitoringCycleError(msg)


def _protection_orders_guard_or_raise(
    now_ts: float,
    runtime: EngineLoopRuntime,
    ctx: MonitoringCycleContext,
) -> None:
    _ = _require_float(now_ts, "now_ts", min_value=0.0)

    if not ctx.open_trades_ref:
        return

    if (now_ts - ctx.state.last_protection_guard_ts) < ctx.config.protection_guard_interval_sec:
        return

    if len(ctx.open_trades_ref) != 1:
        raise MonitoringContractError(
            f"OPEN_TRADES must be <= 1 in one-way mode (STRICT), got={len(ctx.open_trades_ref)}"
        )

    trade = ctx.open_trades_ref[0]
    symbol = _normalize_symbol_strict(getattr(trade, "symbol", None), name="trade.symbol")

    if not _exchange_position_is_open_strict(symbol):
        ctx.state.last_protection_guard_ts = now_ts
        return

    expected_position_side = _normalize_position_side_guard(
        getattr(trade, "exchange_position_side", None),
        "trade.exchange_position_side",
    )

    tp_order_id = str(getattr(trade, "tp_order_id", "") or "").strip()
    sl_order_id = str(getattr(trade, "sl_order_id", "") or "").strip()

    missing_fields: List[str] = []
    if not tp_order_id:
        missing_fields.append("tp_order_id")
    if not sl_order_id:
        missing_fields.append("sl_order_id")

    if missing_fields:
        _request_safe_stop_or_raise(runtime, "PROTECTION_MISSING")
        msg = f"[SAFE_STOP][PROTECTION_MISSING] trade missing protection ids: {missing_fields} symbol={symbol}"
        log(msg)
        _safe_send_tg(msg)

        if bool(ctx.settings.force_close_on_desync):
            try:
                n = ctx.force_close_fn(symbol)
                log(f"[PROTECTION_MISSING] force close submitted count={n}")
                _safe_send_tg(f"🧯 보호주문 누락 강제정리 제출: {symbol} count={n}")
            except Exception as e:
                log(f"[PROTECTION_MISSING] force close failed: {type(e).__name__}: {e}")
                _safe_send_tg(f"❌ 보호주문 누락 강제정리 실패: {e}")
                raise MonitoringCycleError(
                    f"protection order force close failed: {type(e).__name__}: {e}"
                ) from e
        raise MonitoringCycleError(msg)

    tp_ok = _has_account_ws_protection_order_strict(symbol, tp_order_id, expected_position_side)
    sl_ok = _has_account_ws_protection_order_strict(symbol, sl_order_id, expected_position_side)

    if not tp_ok:
        tp_ok = _has_rest_protection_order_strict(symbol, tp_order_id, expected_position_side)
    if not sl_ok:
        sl_ok = _has_rest_protection_order_strict(symbol, sl_order_id, expected_position_side)

    if tp_ok and sl_ok:
        ctx.state.last_protection_guard_ts = now_ts
        return

    _request_safe_stop_or_raise(runtime, "PROTECTION_ORDERS_MISSING")
    missing_runtime: List[str] = []
    if not tp_ok:
        missing_runtime.append(f"TP({tp_order_id})")
    if not sl_ok:
        missing_runtime.append(f"SL({sl_order_id})")

    msg = f"[SAFE_STOP][PROTECTION_ORDERS_MISSING] symbol={symbol} missing={missing_runtime}"
    log(msg)
    _safe_send_tg(msg)

    if bool(ctx.settings.force_close_on_desync):
        try:
            n = ctx.force_close_fn(symbol)
            log(f"[PROTECTION_ORDERS_MISSING] force close submitted count={n}")
            _safe_send_tg(f"🧯 보호주문 미존재 강제정리 제출: {symbol} count={n}")
        except Exception as e:
            log(f"[PROTECTION_ORDERS_MISSING] force close failed: {type(e).__name__}: {e}")
            _safe_send_tg(f"❌ 보호주문 미존재 강제정리 실패: {e}")
            raise MonitoringCycleError(
                f"protection order force close failed: {type(e).__name__}: {e}"
            ) from e

    raise MonitoringCycleError(msg)


def _reconcile_or_raise(
    now_ts: float,
    ctx: MonitoringCycleContext,
) -> None:
    _ = _require_float(now_ts, "now_ts", min_value=0.0)
    ctx.reconcile_engine.run_if_due(now_ts=now_ts)


def _sigterm_deadline_guard_or_raise(
    now_ts: float,
    runtime: EngineLoopRuntime,
    ctx: MonitoringCycleContext,
) -> None:
    _ = _require_float(now_ts, "now_ts", min_value=0.0)
    if runtime.sigterm_deadline_ts is None:
        return
    if ctx.state.sigterm_deadline_handled:
        return
    if now_ts < runtime.sigterm_deadline_ts:
        return

    ctx.state.sigterm_deadline_handled = True
    log("[SIGTERM] grace deadline reached. force close attempt starts.")
    _safe_send_tg("🧯 SIGTERM grace 경과: 강제 정리 시도합니다.")

    try:
        n = ctx.force_close_fn(ctx.symbol)
        log(f"[SIGTERM] force close submitted count={n}")
        _safe_send_tg(f"🧯 SIGTERM 강제 정리 제출: count={n}")
    except Exception as e:
        log(f"[SIGTERM] force close failed: {type(e).__name__}: {e}")
        _safe_send_tg(f"❌ SIGTERM 강제 정리 실패: {e}")
        _request_safe_stop_or_raise(runtime, "SIGTERM_FORCE_CLOSE_FAIL")
        raise MonitoringCycleError(f"sigterm force close failed: {type(e).__name__}: {e}") from e

    _request_safe_stop_or_raise(runtime, "SIGTERM_DEADLINE")


def run_monitoring_cycle_or_raise(
    now_ts: float,
    runtime: EngineLoopRuntime,
    ctx: MonitoringCycleContext,
) -> None:
    ctx.validate_or_raise()
    runtime.validate_or_raise()
    _ = _require_float(now_ts, "now_ts", min_value=0.0)

    _ws_liveness_guard_or_raise(now_ts, runtime, ctx)
    _account_ws_connected_guard_or_raise(runtime, ctx)
    _protection_orders_guard_or_raise(now_ts, runtime, ctx)
    _reconcile_or_raise(now_ts, ctx)
    _sigterm_deadline_guard_or_raise(now_ts, runtime, ctx)


def build_monitoring_cycle_fn(ctx: MonitoringCycleContext) -> Callable[[float, EngineLoopRuntime], None]:
    ctx.validate_or_raise()

    def _fn(now_ts: float, runtime: EngineLoopRuntime) -> None:
        run_monitoring_cycle_or_raise(now_ts, runtime, ctx)

    return _fn


__all__ = [
    "MonitoringCycleError",
    "MonitoringContractError",
    "MonitoringCycleConfig",
    "MonitoringCycleState",
    "MonitoringCycleContext",
    "build_monitoring_cycle_context_or_raise",
    "build_monitoring_cycle_fn",
    "run_monitoring_cycle_or_raise",
]