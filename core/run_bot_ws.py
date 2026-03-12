# core/run_bot_ws.py
"""
============================================================
FILE: core/run_bot_ws.py
ROLE:
- Binance USDT-M Futures WebSocket 기반 메인 엔진 루프
- bootstrap / engine loop / cycle 계층을 조립해 실제 자동매매 런타임을 운영한다

CORE RESPONSIBILITIES:
- engine bootstrap 결과를 받아 runtime 상태를 초기화한다
- monitoring / exit / entry / risk / execution cycle 을 orchestration 한다
- periodic sync / balance check / fill close reconciliation 을 수행한다
- SAFE_STOP / HALTED 상태 전이를 관리한다
- cycle 계층 간 shared runtime state 를 단일 루프에서 유지한다
- async worker / commentary engine / watchdog / auto reporter 를 실제 런타임에 연결한다

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- settings 는 SETTINGS 단일 객체만 사용한다
- market data / signal / risk / execution 세부 로직은 cycle 계층으로 분리한다
- hidden default / silent continue / 예외 삼키기 금지
- runtime state rollback 금지
- 치명 오류는 SAFE_STOP + 예외 전파
- 공통 인프라(async/commentary/watchdog/reporting)는 main()에서 명시적으로 부팅/종료한다

CHANGE HISTORY:
- 2026-03-13:
  1) FEAT(WIRING): async_worker.start_worker() 실제 부팅 연결
  2) FEAT(WIRING): init_commentary_engine() 1회 초기화 연결
  3) FEAT(WIRING): start_watchdog() + on_fatal SAFE_STOP 콜백 연결
  4) FEAT(WIRING): AutoReporter 주기 실행 및 bt_events 기록 연결
  5) FIX(STRICT): main() finally 에서 watchdog / async worker 종료 정리 추가
- 2026-03-12:
  1) FIX(ROOT-CAUSE): import 누락(time, Callable) 수정
  2) KEEP(STRUCTURE): bootstrap / engine_loop / cycles orchestration 구조 유지
============================================================
"""

from __future__ import annotations

import datetime
import math
import signal
import time
import traceback
from collections import deque
from typing import Any, Callable, Dict, Iterable, List, Optional

from settings import SETTINGS
from infra.telelog import log, send_tg
from infra.async_worker import start_worker, stop_worker, submit as submit_async
from infra.engine_watchdog import start_watchdog, stop_watchdog
from analysis.auto_reporter import AutoReporter
from events.commentary_engine import init_commentary_engine
from events.event_store import record_quant_analysis_event_db

from execution.exchange_api import get_available_usdt
from execution.order_executor import close_all_positions_market
from execution.risk_physics_engine import RiskPhysicsEngine, RiskPhysicsPolicy
from state.sync_exchange import sync_open_trades_from_exchange
from state.trader_state import Trade, TraderState, check_closes, build_close_summary_strict
from events.signals_logger import log_signal
from strategy.regime_engine import RegimeEngine
from strategy.account_state_builder import AccountStateBuilder
from strategy.ev_heatmap_engine import EvHeatmapEngine

from engine.engine_bootstrap import EngineBootstrapArtifacts, bootstrap_engine_runtime_or_raise
from engine.engine_loop import (
    EngineLoopConfig,
    EngineLoopHalted,
    EngineLoopRuntime,
    run_engine_loop_or_raise,
)
from engine.cycles.monitoring_cycle import (
    build_monitoring_cycle_context_or_raise,
    build_monitoring_cycle_fn,
)
from engine.cycles.entry_cycle import (
    build_entry_cycle_context_or_raise,
    build_entry_cycle_fn,
)
from engine.cycles.risk_cycle import (
    build_risk_cycle_context_or_raise,
    build_risk_cycle_fn,
)
from engine.cycles.execution_cycle import (
    build_execution_cycle_context_or_raise,
    build_execution_cycle_fn,
)
from engine.cycles.exit_cycle import (
    build_exit_cycle_context_or_raise,
    build_open_position_cycle_fn,
)

SET = SETTINGS
RUNNING: bool = True
SAFE_STOP_REQUESTED: bool = False

SIGTERM_REQUESTED_AT: Optional[float] = None
SIGTERM_DEADLINE_TS: Optional[float] = None
_SIGTERM_NOTICE_SENT: bool = False
_HALT_NOTICE_SENT: bool = False
_COMMENTARY_ENGINE_INITIALIZED: bool = False

OPEN_TRADES: List[Trade] = []
TRADER_STATE: TraderState = TraderState()

LAST_CLOSE_TS: float = 0.0
LAST_EXCHANGE_SYNC_TS: float = 0.0
LAST_EXIT_CANDLE_TS_1M: Optional[int] = None

CONSEC_LOSSES: int = 0
_BALANCE_CONSEC_FAILS: int = 0
LAST_BALANCE_LOG_TS: float = 0.0
LAST_FILL_CHECK_TS: float = 0.0

_BALANCE_FAIL_HARDSTOP_N: int = 3

ENGINE_LOOP_TICK_SEC: float = 0.20
ENGINE_OPEN_POSITION_TICK_SEC: float = 0.20
ENGINE_IDLE_TICK_SEC: float = 0.20
ENGINE_SAFE_STOP_TICK_SEC: float = 0.20

ASYNC_WORKER_THREADS: int = 1
ASYNC_WORKER_MAX_QUEUE_SIZE: int = 2000
ASYNC_WORKER_THREAD_NAME_PREFIX: str = "engine-async"

_EQUITY_CACHE_VALUE: Optional[float] = None
_EQUITY_CACHE_TS: float = 0.0

CLOSED_TRADES_CACHE = deque(maxlen=50)
_AUTO_REPORTER: Optional[AutoReporter] = None


class _SettingsLoopView:
    """
    settings SSOT wrapper.
    - 기존 SETTINGS 를 그대로 사용한다.
    - cycle 계층이 요구하는 engine_loop_tick_sec 만 명시적으로 추가한다.
    """

    def __init__(self, base: Any, *, engine_loop_tick_sec: float) -> None:
        self._base = base
        self.engine_loop_tick_sec = engine_loop_tick_sec

    def __getattr__(self, item: str) -> Any:
        return getattr(self._base, item)


def _safe_send_tg(msg: str) -> None:
    try:
        ok = submit_async(send_tg, msg, critical=False, label="send_tg")
        if not ok:
            log(f"[TG][DROP] async queue full: {msg}")
    except Exception as e:
        log(f"[TG] async submit error: {type(e).__name__}: {e} | msg={msg}")


def _require_float(v: Any, name: str, *, min_value: Optional[float] = None) -> float:
    if isinstance(v, bool):
        raise RuntimeError(f"{name} must be numeric (bool not allowed)")
    try:
        x = float(v)
    except Exception as e:
        raise RuntimeError(f"{name} must be numeric: {e}") from e
    if not math.isfinite(x):
        raise RuntimeError(f"{name} must be finite")
    if min_value is not None and x < min_value:
        raise RuntimeError(f"{name} must be >= {min_value}")
    return float(x)


def _require_int(v: Any, name: str, *, min_value: Optional[int] = None) -> int:
    if isinstance(v, bool):
        raise RuntimeError(f"{name} must be int (bool not allowed)")
    try:
        iv = int(v)
    except Exception as e:
        raise RuntimeError(f"{name} must be int: {e}") from e
    if min_value is not None and iv < min_value:
        raise RuntimeError(f"{name} must be >= {min_value}")
    return int(iv)


def _require_bool(v: Any, name: str) -> bool:
    if not isinstance(v, bool):
        raise RuntimeError(f"{name} must be bool (STRICT)")
    return bool(v)


def _require_nonempty_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        raise RuntimeError(f"{name} is empty (STRICT)")
    return s


def _require_dict(v: Any, name: str) -> Dict[str, Any]:
    if not isinstance(v, dict):
        raise RuntimeError(f"{name} must be dict (STRICT)")
    if not v:
        raise RuntimeError(f"{name} must not be empty (STRICT)")
    return dict(v)


def _normalize_symbol_strict(symbol: Any, *, name: str = "symbol") -> str:
    s = str(symbol or "").replace("-", "").replace("/", "").upper().strip()
    if not s:
        raise RuntimeError(f"{name} is empty (STRICT)")
    return s


def _invalidate_equity_cache() -> None:
    global _EQUITY_CACHE_VALUE, _EQUITY_CACHE_TS
    _EQUITY_CACHE_VALUE = None
    _EQUITY_CACHE_TS = 0.0


def _closed_trades_getter() -> Iterable[Dict[str, Any]]:
    return list(CLOSED_TRADES_CACHE)


def _request_safe_stop(runtime: EngineLoopRuntime) -> None:
    global SAFE_STOP_REQUESTED
    SAFE_STOP_REQUESTED = True
    runtime.safe_stop_requested = True


def _stop_flag_getter() -> bool:
    return (not RUNNING) or SAFE_STOP_REQUESTED


def _resolve_closed_trade_pnl_total_strict(trade: Trade, summary: Dict[str, Any]) -> float:
    if not isinstance(summary, dict):
        raise RuntimeError("closed summary must be dict (STRICT)")
    if "pnl" not in summary:
        raise RuntimeError("close summary missing pnl (STRICT)")
    summary_pnl_f = _require_float(summary.get("pnl"), "summary.pnl")

    trade_realized = getattr(trade, "realized_pnl_usdt", None)
    if trade_realized is None:
        return float(summary_pnl_f)

    return _require_float(trade_realized, "trade.realized_pnl_usdt")


def _resolve_closed_trade_exit_ts_dt_strict(trade: Trade, summary: Dict[str, Any]) -> datetime.datetime:
    if not isinstance(summary, dict):
        raise RuntimeError("closed summary must be dict (STRICT)")

    close_ts = getattr(trade, "close_ts", None)
    if close_ts is not None:
        close_ts_f = _require_float(close_ts, "trade.close_ts", min_value=0.0)
        if close_ts_f > 0:
            dt = datetime.datetime.fromtimestamp(close_ts_f, tz=datetime.timezone.utc)
            if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
                raise RuntimeError("resolved close datetime must be tz-aware (STRICT)")
            return dt

    if "close_time" not in summary:
        raise RuntimeError("close summary missing close_time (STRICT)")
    close_time_ms_i = _require_int(summary.get("close_time"), "summary.close_time", min_value=1)
    dt2 = datetime.datetime.fromtimestamp(close_time_ms_i / 1000.0, tz=datetime.timezone.utc)
    if dt2.tzinfo is None or dt2.tzinfo.utcoffset(dt2) is None:
        raise RuntimeError("resolved close datetime from close_time must be tz-aware (STRICT)")
    return dt2


def _on_trade_closed_exit(trade: Trade, now_ts: float) -> None:
    global LAST_CLOSE_TS
    if not isinstance(trade, Trade):
        raise RuntimeError(f"trade must be Trade (STRICT), got={type(trade).__name__}")
    LAST_CLOSE_TS = _require_float(now_ts, "now_ts", min_value=0.0)


def _sigterm(*_: Any) -> None:
    global SAFE_STOP_REQUESTED, SIGTERM_REQUESTED_AT, SIGTERM_DEADLINE_TS, _SIGTERM_NOTICE_SENT
    SAFE_STOP_REQUESTED = True
    now = time.time()

    if SIGTERM_REQUESTED_AT is None:
        SIGTERM_REQUESTED_AT = now
        grace = _require_float(getattr(SET, "sigterm_grace_sec"), "settings.sigterm_grace_sec", min_value=0.001)
        SIGTERM_DEADLINE_TS = now + grace

        msg = f"🧯 SIGTERM 수신: 신규 진입 중단, 포지션 정리 후 종료 시도 (grace={int(grace)}s)"
        log(msg)
        if not _SIGTERM_NOTICE_SENT:
            _SIGTERM_NOTICE_SENT = True
            _safe_send_tg(msg)


signal.signal(signal.SIGTERM, _sigterm)


def _on_safe_stop_command() -> None:
    global SAFE_STOP_REQUESTED
    SAFE_STOP_REQUESTED = True
    _safe_send_tg("🛑 텔레그램 '종료' 요청: 포지션 정리 후 종료합니다.")


def _maybe_position_resync_or_raise(now_ts: float) -> None:
    global LAST_EXCHANGE_SYNC_TS

    position_resync_sec = _require_float(getattr(SET, "position_resync_sec"), "settings.position_resync_sec", min_value=0.001)
    if (now_ts - LAST_EXCHANGE_SYNC_TS) < position_resync_sec:
        return

    new_open_trades, _ = sync_open_trades_from_exchange(SET.symbol, replace=True, current_trades=OPEN_TRADES)
    if not isinstance(new_open_trades, list):
        raise RuntimeError("sync_open_trades_from_exchange must return list (STRICT)")

    OPEN_TRADES[:] = new_open_trades
    _invalidate_equity_cache()
    LAST_EXCHANGE_SYNC_TS = now_ts


def _maybe_balance_check_or_raise(now_ts: float, runtime: EngineLoopRuntime) -> None:
    global LAST_BALANCE_LOG_TS, _BALANCE_CONSEC_FAILS

    balance_log_interval_sec = 60.0
    if (now_ts - LAST_BALANCE_LOG_TS) < balance_log_interval_sec:
        return

    try:
        _ = get_available_usdt()
        _BALANCE_CONSEC_FAILS = 0
    except Exception as e:
        _BALANCE_CONSEC_FAILS += 1
        msg = f"[WARN][BALANCE_FAIL] {type(e).__name__}: {e} consecutive={_BALANCE_CONSEC_FAILS}/{_BALANCE_FAIL_HARDSTOP_N}"
        log(msg)
        _safe_send_tg(msg)
        if _BALANCE_CONSEC_FAILS >= _BALANCE_FAIL_HARDSTOP_N:
            _request_safe_stop(runtime)
            raise RuntimeError("balance check failed consecutively (STRICT)") from e
    finally:
        LAST_BALANCE_LOG_TS = now_ts


def _maybe_fill_check_or_raise(now_ts: float, runtime: EngineLoopRuntime) -> None:
    global LAST_FILL_CHECK_TS, CONSEC_LOSSES, LAST_CLOSE_TS

    poll_fills_sec = _require_float(getattr(SET, "poll_fills_sec"), "settings.poll_fills_sec", min_value=0.001)
    if (now_ts - LAST_FILL_CHECK_TS) < poll_fills_sec:
        return

    new_open_trades, closed_list = check_closes(OPEN_TRADES, TRADER_STATE)
    if not isinstance(new_open_trades, list):
        raise RuntimeError("check_closes open_trades result must be list (STRICT)")
    if not isinstance(closed_list, list):
        raise RuntimeError("check_closes closed_list result must be list (STRICT)")

    OPEN_TRADES[:] = new_open_trades
    if closed_list:
        _invalidate_equity_cache()

    for closed in closed_list:
        if not isinstance(closed, dict):
            raise RuntimeError("closed_list item must be dict (STRICT)")

        if "trade" not in closed:
            raise RuntimeError("closed item missing trade (STRICT)")
        t = closed["trade"]
        if not isinstance(t, Trade):
            raise RuntimeError(f"closed item trade must be Trade (STRICT), got={type(t).__name__}")

        reason = str(closed.get("reason") or "").strip()
        summary = closed.get("summary")

        if not isinstance(summary, dict) or not summary:
            reason2, summary2 = build_close_summary_strict(t)
            if not isinstance(summary2, dict) or not summary2:
                raise RuntimeError("close summary requery returned invalid (STRICT)")
            reason = str(reason2 or reason)
            summary = summary2

        if "avg_price" not in summary or summary["avg_price"] is None:
            raise RuntimeError("close summary missing avg_price (STRICT)")
        if "qty" not in summary or summary["qty"] is None:
            raise RuntimeError("close summary missing qty (STRICT)")

        pnl_total = _resolve_closed_trade_pnl_total_strict(t, summary)
        avg_price = _require_float(summary["avg_price"], "summary.avg_price", min_value=0.0)
        qty = _require_float(summary["qty"], "summary.qty", min_value=0.0)
        if qty <= 0.0:
            raise RuntimeError("summary.qty must be > 0 (STRICT)")

        LAST_CLOSE_TS = now_ts
        CONSEC_LOSSES = (CONSEC_LOSSES + 1) if pnl_total < 0 else 0

        trade_id = getattr(t, "id", None)
        if not isinstance(trade_id, int) or trade_id <= 0:
            raise RuntimeError("closed trade missing valid trade.id (STRICT)")

        exit_ts_dt = _resolve_closed_trade_exit_ts_dt_strict(t, summary)
        tp_pct_v = getattr(t, "tp_pct", None)
        sl_pct_v = getattr(t, "sl_pct", None)

        CLOSED_TRADES_CACHE.appendleft(
            {
                "id": int(trade_id),
                "exit_ts": exit_ts_dt,
                "pnl_usdt": float(pnl_total),
                "tp_pct": None if tp_pct_v is None else float(tp_pct_v),
                "sl_pct": None if sl_pct_v is None else float(sl_pct_v),
            }
        )

        log_signal(
            event="CLOSE",
            symbol=t.symbol,
            strategy_type=t.source,
            direction=_normalize_direction_for_events_strict(t.side),
            price=avg_price,
            qty=qty,
            reason=reason,
            pnl=float(pnl_total),
        )

        hard_consec_limit_i = _require_int(
            getattr(SET, "hard_consecutive_losses_limit"),
            "settings.hard_consecutive_losses_limit",
            min_value=0,
        )
        if hard_consec_limit_i > 0 and CONSEC_LOSSES >= hard_consec_limit_i:
            _request_safe_stop(runtime)
            msg = (
                "🛑 [SAFE_STOP][CONSEC_LOSS] 연속 손실 한도 도달: "
                f"consecutive_losses={CONSEC_LOSSES} limit={hard_consec_limit_i} "
                "(신규 진입 중단, 엔진 종료)"
            )
            log(msg)
            _safe_send_tg(msg)

    LAST_FILL_CHECK_TS = now_ts


def _normalize_direction_for_events_strict(v: Any) -> str:
    s = str(v or "").upper().strip()
    if s in ("LONG", "SHORT"):
        return s
    if s == "BUY":
        return "LONG"
    if s == "SELL":
        return "SHORT"
    raise RuntimeError(f"invalid trade side for events: {v!r}")


def _auto_report_event_writer(event_type: str, report_type: str, payload: Dict[str, Any]) -> None:
    et = _require_nonempty_str(event_type, "event_type")
    if et != "QUANT_ANALYSIS":
        raise RuntimeError(f"unexpected auto report event_type (STRICT): {et}")
    rt = _require_nonempty_str(report_type, "report_type")
    data = _require_dict(payload, "payload")
    generated_ts_ms = _require_int(data.get("generated_ts_ms"), "payload.generated_ts_ms", min_value=1)
    ts_utc = datetime.datetime.fromtimestamp(generated_ts_ms / 1000.0, tz=datetime.timezone.utc)
    record_quant_analysis_event_db(
        ts_utc=ts_utc,
        symbol=_normalize_symbol_strict(SET.symbol),
        report_type=rt,
        reason="auto_report_generated",
        analysis_payload=data,
        regime=None,
        is_test=False,
    )


def _auto_report_notifier(report_type: str, payload: Dict[str, Any]) -> None:
    rt = _require_nonempty_str(report_type, "report_type")
    data = _require_dict(payload, "payload")
    generated_ts_ms = _require_int(data.get("generated_ts_ms"), "payload.generated_ts_ms", min_value=1)
    dt_kst = datetime.datetime.fromtimestamp(generated_ts_ms / 1000.0, tz=datetime.timezone.utc).astimezone(
        datetime.timezone(datetime.timedelta(hours=9))
    )
    _safe_send_tg(
        f"🧠 자동 분석 리포트 생성\n"
        f"- 유형: {rt}\n"
        f"- 종목: {_normalize_symbol_strict(SET.symbol)}\n"
        f"- 시각: {dt_kst.strftime('%Y-%m-%d %H:%M:%S KST')}"
    )


def _build_auto_reporter_or_raise() -> AutoReporter:
    return AutoReporter(
        settings=SET,
        event_writer=_auto_report_event_writer,
        notifier=_auto_report_notifier,
    )


def _maybe_auto_report_or_raise(now_ts: float) -> None:
    global _AUTO_REPORTER
    if _AUTO_REPORTER is None:
        raise RuntimeError("auto reporter is not initialized (STRICT)")
    now_ms = _require_int(now_ts * 1000.0, "now_ms", min_value=1)
    _AUTO_REPORTER.run_due_reports(now_ms=now_ms)


def _ensure_commentary_engine_initialized_or_raise() -> None:
    global _COMMENTARY_ENGINE_INITIALIZED
    if _COMMENTARY_ENGINE_INITIALIZED:
        return
    init_commentary_engine()
    _COMMENTARY_ENGINE_INITIALIZED = True


def _on_watchdog_fatal(reason: str, detail: Dict[str, Any]) -> None:
    global SAFE_STOP_REQUESTED
    reason_s = _require_nonempty_str(reason, "reason")
    detail_d = _require_dict(detail, "detail")
    SAFE_STOP_REQUESTED = True
    log(f"[WATCHDOG][FATAL][CALLBACK] reason={reason_s} detail={detail_d}")
    _safe_send_tg(f"⛔ WATCHDOG 치명 상태 감지: {reason_s}")


def _monitoring_cycle_wrapper(
    now_ts: float,
    runtime: EngineLoopRuntime,
    monitoring_cycle_fn: Callable[[float, EngineLoopRuntime], None],
) -> None:
    runtime.sigterm_deadline_ts = SIGTERM_DEADLINE_TS
    monitoring_cycle_fn(now_ts, runtime)
    if runtime.safe_stop_requested:
        _request_safe_stop(runtime)

    _maybe_position_resync_or_raise(now_ts)
    _maybe_balance_check_or_raise(now_ts, runtime)
    _maybe_fill_check_or_raise(now_ts, runtime)
    _maybe_auto_report_or_raise(now_ts)

    if runtime.safe_stop_requested:
        _request_safe_stop(runtime)


def _entry_risk_execution_cycle_wrapper(
    now_ts: float,
    runtime: EngineLoopRuntime,
    entry_cycle_fn: Callable[[float, EngineLoopRuntime], None],
    risk_cycle_fn: Callable[[float, EngineLoopRuntime], None],
    execution_cycle_fn: Callable[[float, EngineLoopRuntime], None],
) -> None:
    entry_cycle_fn(now_ts, runtime)
    if runtime.safe_stop_requested:
        _request_safe_stop(runtime)
        return

    risk_cycle_fn(now_ts, runtime)
    if runtime.safe_stop_requested:
        _request_safe_stop(runtime)
        return

    execution_cycle_fn(now_ts, runtime)
    if runtime.safe_stop_requested:
        _request_safe_stop(runtime)


def _open_position_cycle_wrapper(
    now_ts: float,
    runtime: EngineLoopRuntime,
    open_position_cycle_fn: Callable[[float, EngineLoopRuntime], bool],
    exit_ctx: Any,
) -> bool:
    handled = open_position_cycle_fn(now_ts, runtime)
    if not isinstance(handled, bool):
        raise RuntimeError("open_position_cycle_fn must return bool (STRICT)")

    global LAST_EXIT_CANDLE_TS_1M
    if hasattr(exit_ctx, "state") and hasattr(exit_ctx.state, "last_exit_candle_ts_1m"):
        LAST_EXIT_CANDLE_TS_1M = exit_ctx.state.last_exit_candle_ts_1m

    if runtime.safe_stop_requested:
        _request_safe_stop(runtime)

    return handled


def _idle_safe_stop_fn(now_ts: float, runtime: EngineLoopRuntime) -> bool:
    global _HALT_NOTICE_SENT

    _ = _require_float(now_ts, "now_ts", min_value=0.0)

    if OPEN_TRADES:
        return False

    if not runtime.safe_stop_requested and not SAFE_STOP_REQUESTED:
        return False

    if not _HALT_NOTICE_SENT:
        _HALT_NOTICE_SENT = True
        _safe_send_tg("🛑 포지션 0 확인. 자동매매를 종료합니다.")
    return True


def main() -> None:
    global OPEN_TRADES, LAST_EXCHANGE_SYNC_TS, LAST_EXIT_CANDLE_TS_1M, SAFE_STOP_REQUESTED, _AUTO_REPORTER

    start_worker(
        num_threads=ASYNC_WORKER_THREADS,
        max_queue_size=ASYNC_WORKER_MAX_QUEUE_SIZE,
        thread_name_prefix=ASYNC_WORKER_THREAD_NAME_PREFIX,
    )
    _ensure_commentary_engine_initialized_or_raise()

    settings_view = _SettingsLoopView(
        SET,
        engine_loop_tick_sec=ENGINE_LOOP_TICK_SEC,
    )

    watchdog_started = False

    try:
        boot: EngineBootstrapArtifacts = bootstrap_engine_runtime_or_raise(
            settings_view,
            on_safe_stop=_on_safe_stop_command,
            stop_flag_getter=_stop_flag_getter,
        )

        OPEN_TRADES[:] = list(boot.open_trades)
        _invalidate_equity_cache()
        LAST_EXCHANGE_SYNC_TS = _require_float(boot.last_exchange_sync_ts, "boot.last_exchange_sync_ts", min_value=0.0)
        LAST_EXIT_CANDLE_TS_1M = boot.last_exit_candle_ts_1m

        persisted_peak = boot.persisted_equity_peak_usdt
        for row in reversed(list(boot.closed_trade_rows)):
            CLOSED_TRADES_CACHE.appendleft(row)

        _AUTO_REPORTER = _build_auto_reporter_or_raise()

        start_watchdog(
            settings=SET,
            symbol=str(SET.symbol),
            on_fatal=_on_watchdog_fatal,
        )
        watchdog_started = True

        reconcile_confirm_n = _require_int(getattr(SET, "reconcile_confirm_n"), "settings.reconcile_confirm_n", min_value=1)
        hard_consecutive_losses_limit = _require_int(
            getattr(SET, "hard_consecutive_losses_limit"),
            "settings.hard_consecutive_losses_limit",
            min_value=0,
        )
        _ = hard_consecutive_losses_limit

        from sync.reconcile_engine import ReconcileConfig, ReconcileEngine

        reconcile_engine = ReconcileEngine(
            ReconcileConfig(
                symbol=str(SET.symbol),
                interval_sec=_require_int(getattr(SET, "reconcile_interval_sec"), "settings.reconcile_interval_sec", min_value=1),
                desync_confirm_n=int(reconcile_confirm_n),
            ),
            fetch_exchange_position=lambda symbol=str(SET.symbol): _fetch_exchange_position_snapshot(symbol),
            get_local_position=lambda symbol=str(SET.symbol): _get_local_position_snapshot(symbol),
            fetch_exchange_open_orders=lambda symbol=str(SET.symbol): _fetch_exchange_open_orders_snapshot(symbol),
            on_desync=lambda result: _on_reconcile_desync(result),
        )

        regime_engine = RegimeEngine(window_size=200, percentile_min_history=60)
        ev_heatmap = EvHeatmapEngine(window_size=50, min_samples=20)
        account_builder = AccountStateBuilder(
            win_rate_window=20,
            min_trades_for_win_rate=5,
            initial_equity_peak_usdt=persisted_peak,
        )
        risk_physics = RiskPhysicsEngine(
            policy=RiskPhysicsPolicy(max_allocation=1.0)
        )

        monitoring_ctx = build_monitoring_cycle_context_or_raise(
            settings=settings_view,
            symbol=SET.symbol,
            open_trades_ref=OPEN_TRADES,
            reconcile_engine=reconcile_engine,
            force_close_fn=close_all_positions_market,
        )
        monitoring_cycle_fn = build_monitoring_cycle_fn(monitoring_ctx)

        exit_ctx = build_exit_cycle_context_or_raise(
            settings=settings_view,
            symbol=SET.symbol,
            open_trades_ref=OPEN_TRADES,
            invalidate_equity_cache_fn=_invalidate_equity_cache,
            on_trade_closed_fn=_on_trade_closed_exit,
            last_exit_candle_ts_1m=LAST_EXIT_CANDLE_TS_1M,
        )
        open_position_cycle_fn = build_open_position_cycle_fn(exit_ctx)

        entry_ctx = build_entry_cycle_context_or_raise(
            settings=settings_view,
            symbol=SET.symbol,
            last_close_ts_getter=lambda: LAST_CLOSE_TS,
        )
        if boot.last_entry_eval_signal_ts_ms is not None:
            entry_ctx.state.last_entry_eval_signal_ts_ms = int(boot.last_entry_eval_signal_ts_ms)
            entry_ctx.state.entry_gate_bootstrapped = True
        entry_cycle_fn = build_entry_cycle_fn(entry_ctx)

        risk_ctx = build_risk_cycle_context_or_raise(
            settings=settings_view,
            symbol=SET.symbol,
            regime_engine=regime_engine,
            account_builder=account_builder,
            ev_heatmap=ev_heatmap,
            risk_physics=risk_physics,
            closed_trades_getter=_closed_trades_getter,
            persisted_equity_peak_usdt=persisted_peak,
        )
        risk_cycle_fn = build_risk_cycle_fn(risk_ctx)

        execution_ctx = build_execution_cycle_context_or_raise(
            settings=settings_view,
            open_trades_ref=OPEN_TRADES,
            invalidate_equity_cache_fn=_invalidate_equity_cache,
        )
        execution_cycle_fn = build_execution_cycle_fn(execution_ctx)

        runtime = EngineLoopRuntime(
            running=True,
            safe_stop_requested=SAFE_STOP_REQUESTED,
            halted=False,
            sigterm_deadline_ts=SIGTERM_DEADLINE_TS,
        )

        config = EngineLoopConfig(
            tick_sec=ENGINE_LOOP_TICK_SEC,
            open_position_tick_sec=ENGINE_OPEN_POSITION_TICK_SEC,
            idle_tick_sec=ENGINE_IDLE_TICK_SEC,
            safe_stop_tick_sec=ENGINE_SAFE_STOP_TICK_SEC,
        )

        run_engine_loop_or_raise(
            config=config,
            runtime=runtime,
            monitoring_cycle_fn=lambda now_ts, rt: _monitoring_cycle_wrapper(
                now_ts,
                rt,
                monitoring_cycle_fn,
            ),
            open_position_cycle_fn=lambda now_ts, rt: _open_position_cycle_wrapper(
                now_ts,
                rt,
                open_position_cycle_fn,
                exit_ctx,
            ),
            entry_cycle_fn=lambda now_ts, rt: _entry_risk_execution_cycle_wrapper(
                now_ts,
                rt,
                entry_cycle_fn,
                risk_cycle_fn,
                execution_cycle_fn,
            ),
            idle_safe_stop_fn=_idle_safe_stop_fn,
            stop_flag_getter=_stop_flag_getter,
        )

    except EngineLoopHalted:
        return
    except Exception as e:
        SAFE_STOP_REQUESTED = True

        tb = traceback.format_exc()
        log(f"ERROR(FATAL): {e}\n{tb}")

        _safe_send_tg(f"❌ 치명 오류: {e}")

        log_signal(
            event="ERROR",
            symbol=SET.symbol,
            strategy_type="UNKNOWN",
            direction="CLOSE",
            reason=str(e),
        )
        raise
    finally:
        try:
            if watchdog_started:
                stop_watchdog()
        finally:
            stop_worker()


def _fetch_exchange_position_snapshot(symbol: str) -> Dict[str, Any]:
    from execution.exchange_api import fetch_open_positions

    sym = _normalize_symbol_strict(symbol)
    rows = fetch_open_positions(sym)
    if not isinstance(rows, list):
        raise RuntimeError("fetch_open_positions returned non-list (STRICT)")

    live: List[Dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            raise RuntimeError("fetch_open_positions contains non-dict row (STRICT)")
        if str(r.get("symbol") or "").upper().strip() != sym:
            continue
        if "positionAmt" not in r:
            raise RuntimeError("positionRisk.positionAmt missing (STRICT)")
        amt = _require_float(r.get("positionAmt"), "positionRisk.positionAmt")
        if abs(amt) > 1e-12:
            live.append(r)

    if not live:
        return {"symbol": sym, "positionAmt": "0", "entryPrice": "0"}

    dirs: set[str] = set()
    pos_sides: set[str] = set()
    total_signed_amt = 0.0
    weighted_entry_sum = 0.0

    for idx, r in enumerate(live):
        amt = _require_float(r.get("positionAmt"), f"positionRisk[{idx}].positionAmt")
        entry = _require_float(r.get("entryPrice"), f"positionRisk[{idx}].entryPrice", min_value=0.0)
        if entry <= 0:
            raise RuntimeError("positionRisk.entryPrice must be > 0 (STRICT)")

        ps_raw = str(r.get("positionSide") or "").upper().strip() or "BOTH"
        if ps_raw not in ("BOTH", "LONG", "SHORT"):
            raise RuntimeError(f"positionRisk.positionSide invalid (STRICT): {ps_raw!r}")
        pos_sides.add(ps_raw)

        row_dir = ps_raw if ps_raw in ("LONG", "SHORT") else ("LONG" if amt > 0 else "SHORT")
        dirs.add(row_dir)

        total_signed_amt += amt
        weighted_entry_sum += abs(amt) * entry

    if len(dirs) != 1:
        raise RuntimeError(f"ambiguous exchange positions / hedge topology (STRICT): dirs={sorted(list(dirs))}")
    if "BOTH" in pos_sides and len(pos_sides) > 1:
        raise RuntimeError(
            f"ambiguous exchange positionSide topology (STRICT): pos_sides={sorted(list(pos_sides))}"
        )
    if abs(total_signed_amt) <= 1e-12:
        raise RuntimeError("aggregated positionAmt became zero unexpectedly (STRICT)")

    abs_qty = abs(total_signed_amt)
    entry_price = weighted_entry_sum / abs_qty
    if not math.isfinite(entry_price) or entry_price <= 0:
        raise RuntimeError("aggregated entryPrice invalid (STRICT)")

    return {"symbol": sym, "positionAmt": str(total_signed_amt), "entryPrice": str(entry_price)}


def _get_local_position_snapshot(symbol: str) -> Dict[str, Any]:
    sym = _normalize_symbol_strict(symbol)
    if not OPEN_TRADES:
        return {"symbol": sym, "position_amt": "0", "entry_price": "0"}

    if len(OPEN_TRADES) != 1:
        raise RuntimeError(f"OPEN_TRADES must be <= 1 in one-way mode (STRICT), got={len(OPEN_TRADES)}")

    t = OPEN_TRADES[0]
    if str(t.symbol).upper().strip() != sym:
        raise RuntimeError(f"local trade symbol mismatch (STRICT): trade={t.symbol} expected={sym}")

    side = str(t.side or "").upper().strip()
    if side in ("LONG", "BUY"):
        sign = 1.0
    elif side in ("SHORT", "SELL"):
        sign = -1.0
    else:
        raise RuntimeError(f"invalid trade.side (STRICT): {t.side!r}")

    qty = _require_float(getattr(t, "remaining_qty", None), "trade.remaining_qty", min_value=0.0)
    if qty <= 0:
        raise RuntimeError("trade.remaining_qty must be > 0 (STRICT)")

    ep = _require_float(getattr(t, "entry_price", None), "trade.entry_price", min_value=0.0)
    if ep <= 0:
        raise RuntimeError("trade.entry_price must be > 0 (STRICT)")

    return {"symbol": sym, "position_amt": str(sign * qty), "entry_price": str(ep)}


def _fetch_exchange_open_orders_snapshot(symbol: str) -> List[Dict[str, Any]]:
    from execution.exchange_api import fetch_open_orders

    sym = _normalize_symbol_strict(symbol)
    orders = fetch_open_orders(sym)
    if not isinstance(orders, list):
        raise RuntimeError("fetch_open_orders returned non-list (STRICT)")
    return orders


def _on_reconcile_desync(result: Any) -> None:
    global SAFE_STOP_REQUESTED
    SAFE_STOP_REQUESTED = True

    desync_confirmed = getattr(result, "desync_confirmed", None)
    if not isinstance(desync_confirmed, bool):
        raise RuntimeError("result.desync_confirmed must be bool (STRICT)")
    if not desync_confirmed:
        raise RuntimeError("on_desync called but desync_confirmed is False (STRICT)")

    symbol = _normalize_symbol_strict(getattr(result, "symbol", None), name="result.symbol")
    issues = getattr(result, "issues", None)
    if not isinstance(issues, list):
        raise RuntimeError("result.issues must be list (STRICT)")

    msg = f"⛔ DESYNC 확정: {symbol} issues={len(issues)} (신규 진입 차단, 종료)"
    log(msg)
    for it in issues:
        code = getattr(it, "code", None)
        message = getattr(it, "message", None)
        details = getattr(it, "details", None)
        log(f"[DESYNC] {code} | {message} | {details}")
    _safe_send_tg(msg)

    force_close_on_desync = _require_bool(getattr(SET, "force_close_on_desync"), "settings.force_close_on_desync")
    if force_close_on_desync:
        try:
            n = close_all_positions_market(symbol)
            log(f"[DESYNC] force close submitted count={n}")
            _safe_send_tg(f"🧯 DESYNC 강제정리 제출: {symbol} count={n}")
        except Exception as e:
            log(f"[DESYNC] force close failed: {type(e).__name__}: {e}")
            _safe_send_tg(f"❌ DESYNC 강제정리 실패: {e}")
            raise RuntimeError(f"DESYNC force close failed: {type(e).__name__}: {e}") from e

    raise RuntimeError("DESYNC confirmed (STRICT): engine must stop")


if __name__ == "__main__":
    from core.run_bot_preflight import run_preflight

    run_preflight(preflight_only=False)