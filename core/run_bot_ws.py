# core/run_bot_ws.py
"""
============================================================
FILE: core/run_bot_ws.py
ROLE:
- Binance USDT-M Futures WebSocket 기반 메인 엔진 루프
- bootstrap / engine loop / cycle 계층을 조립해 실제 자동매매 런타임을 운영한다

CORE RESPONSIBILITIES:
- engine bootstrap 결과를 받아 runtime 상태를 초기화한다
- monitoring / position supervisor / reconciliation / exit / entry / risk / execution cycle 을 orchestration 한다
- periodic sync / balance check / fill close reconciliation 을 수행한다
- SAFE_STOP / HALTED 상태 전이를 관리한다
- cycle 계층 간 shared runtime state 를 단일 루프에서 유지한다
- async worker / commentary engine / watchdog / auto reporter 를 실제 런타임에 연결한다
- position supervisor / reconcile engine 을 engine loop 에 실제 연결한다

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- settings 는 SETTINGS 단일 객체만 사용한다
- market data / signal / risk / execution 세부 로직은 cycle 계층으로 분리한다
- hidden default / silent continue / 예외 삼키기 금지
- runtime state rollback 금지
- 치명 오류는 SAFE_STOP + 예외 전파
- 공통 인프라(async/commentary/watchdog/reporting)는 main()에서 명시적으로 부팅/종료한다
- 단, auto reporter 는 비핵심 분석 서브시스템으로 격리하며 실패 시 log/tg surface 후 엔진은 계속 운용한다
- position supervisor / reconcile 은 runtime loop 의 명시적 orchestration 슬롯으로만 실행한다

CHANGE HISTORY:
- 2026-03-15:
  1) FIX(ROOT-CAUSE): __main__ 에서 core.run_bot_preflight import 제거 → 순환 import 제거
  2) FIX(STATE): SAFE_STOP_REQUESTED 전역 플래그 제거 → EngineLoopRuntime.safe_stop_requested 단일화
  3) FIX(STATE): 외부 종료 요청(SIGTERM / watchdog / telegram / reconcile)은 runtime.request_safe_stop 또는 pre-runtime SIGTERM 상태로만 반영
  4) FIX(BOOT): watchdog 시작 시점을 runtime 생성 이후로 이동
  5) FIX(RELIABILITY): main() 시작 시 런타임 전역 상태 초기화 강화
- 2026-03-14:
  1) FEAT(WIRING): position_supervisor 를 main runtime 에 정식 연결
  2) FEAT(WIRING): reconciliation_cycle_fn 을 engine_loop 신규 시그니처에 맞게 연결
  3) FIX(ARCH): entry→risk→execution 직후 position supervisor 즉시 실행 경로 추가
  4) FIX(STRICT): reconcile / supervisor callback 입력 검증 강화 및 SAFE_STOP escalation 정리
  5) FIX(CONTRACT): engine_loop 신규 인자(position_supervisor_cycle_fn / reconciliation_cycle_fn) 정합 반영
- 2026-03-13:
  1) FEAT(WIRING): async_worker.start_worker() 실제 부팅 연결
  2) FEAT(WIRING): init_commentary_engine() 1회 초기화 연결
  3) FEAT(WIRING): start_watchdog() + on_fatal SAFE_STOP 콜백 연결
  4) FEAT(WIRING): AutoReporter 주기 실행 및 bt_events 기록 연결
  5) FIX(STRICT): main() finally 에서 watchdog / async worker 종료 정리 추가
  6) FIX(OPERABILITY): auto reporter 실패를 메인 엔진 치명 오류로 승격하지 않도록 격리
  7) FIX(CONTRACT): QUANT_ANALYSIS_ERROR 이벤트 타입을 event writer 에서 허용
  8) FIX(NOTIFY): auto report notifier 가 성공/실패 payload를 구분해 알림
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
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

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
from risk.position_supervisor import (
    PositionSupervisor,
    PositionSupervisorError,
    build_position_supervisor_or_raise,
    run_position_supervisor_once_or_raise,
)

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
_ENGINE_RUNTIME: Optional[EngineLoopRuntime] = None

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


def _require_attr_exists(obj: Any, attr_name: str, owner_name: str) -> Any:
    if obj is None:
        raise RuntimeError(f"{owner_name} is None (STRICT)")
    if not hasattr(obj, attr_name):
        raise RuntimeError(f"{owner_name}.{attr_name} missing (STRICT)")
    return getattr(obj, attr_name)


def _require_attr_value(obj: Any, attr_name: str, owner_name: str) -> Any:
    value = _require_attr_exists(obj, attr_name, owner_name)
    if value is None:
        raise RuntimeError(f"{owner_name}.{attr_name} missing (STRICT)")
    return value


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


def _request_safe_stop(runtime: EngineLoopRuntime, *, reason: str) -> None:
    if not isinstance(runtime, EngineLoopRuntime):
        raise RuntimeError(f"runtime must be EngineLoopRuntime (STRICT), got={type(runtime).__name__}")
    reason_s = _require_nonempty_str(reason, "reason")
    runtime.request_safe_stop(reason_s, time.time())


def _request_safe_stop_via_runtime_or_sigterm(*, reason: str) -> None:
    global SIGTERM_REQUESTED_AT, SIGTERM_DEADLINE_TS

    reason_s = _require_nonempty_str(reason, "reason")
    if _ENGINE_RUNTIME is not None:
        _request_safe_stop(_ENGINE_RUNTIME, reason=reason_s)
        return

    now = time.time()
    if SIGTERM_REQUESTED_AT is None:
        SIGTERM_REQUESTED_AT = now
        grace = _require_float(
            _require_attr_value(SET, "sigterm_grace_sec", "settings"),
            "settings.sigterm_grace_sec",
            min_value=0.001,
        )
        SIGTERM_DEADLINE_TS = now + grace


def _stop_flag_getter() -> bool:
    runtime_stop_requested = False
    if _ENGINE_RUNTIME is not None:
        runtime_stop_requested = _require_bool(
            _require_attr_value(_ENGINE_RUNTIME, "safe_stop_requested", "runtime"),
            "runtime.safe_stop_requested",
        )
    return (not RUNNING) or (SIGTERM_REQUESTED_AT is not None) or runtime_stop_requested


def _resolve_closed_trade_pnl_total_strict(trade: Trade, summary: Dict[str, Any]) -> float:
    if not isinstance(summary, dict):
        raise RuntimeError("closed summary must be dict (STRICT)")
    if "pnl" not in summary:
        raise RuntimeError("close summary missing pnl (STRICT)")
    summary_pnl_f = _require_float(summary["pnl"], "summary.pnl")

    if hasattr(trade, "realized_pnl_usdt"):
        trade_realized = getattr(trade, "realized_pnl_usdt")
        if trade_realized is not None:
            return _require_float(trade_realized, "trade.realized_pnl_usdt")

    return float(summary_pnl_f)


def _resolve_closed_trade_exit_ts_dt_strict(trade: Trade, summary: Dict[str, Any]) -> datetime.datetime:
    if not isinstance(summary, dict):
        raise RuntimeError("closed summary must be dict (STRICT)")

    if hasattr(trade, "close_ts"):
        close_ts = getattr(trade, "close_ts")
        if close_ts is not None:
            close_ts_f = _require_float(close_ts, "trade.close_ts", min_value=0.0)
            if close_ts_f > 0:
                dt = datetime.datetime.fromtimestamp(close_ts_f, tz=datetime.timezone.utc)
                if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
                    raise RuntimeError("resolved close datetime must be tz-aware (STRICT)")
                return dt

    if "close_time" not in summary:
        raise RuntimeError("close summary missing close_time (STRICT)")
    close_time_ms_i = _require_int(summary["close_time"], "summary.close_time", min_value=1)
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
    global SIGTERM_REQUESTED_AT, SIGTERM_DEADLINE_TS, _SIGTERM_NOTICE_SENT

    now = time.time()
    _request_safe_stop_via_runtime_or_sigterm(reason="sigterm")

    if SIGTERM_REQUESTED_AT is None:
        SIGTERM_REQUESTED_AT = now

    if SIGTERM_DEADLINE_TS is None:
        grace = _require_float(
            _require_attr_value(SET, "sigterm_grace_sec", "settings"),
            "settings.sigterm_grace_sec",
            min_value=0.001,
        )
        SIGTERM_DEADLINE_TS = now + grace
    else:
        grace = SIGTERM_DEADLINE_TS - SIGTERM_REQUESTED_AT
        if grace <= 0:
            raise RuntimeError("SIGTERM grace must be > 0 (STRICT)")

    msg = f"🧯 SIGTERM 수신: 신규 진입 중단, 포지션 정리 후 종료 시도 (grace={int(grace)}s)"
    log(msg)
    if not _SIGTERM_NOTICE_SENT:
        _SIGTERM_NOTICE_SENT = True
        _safe_send_tg(msg)


signal.signal(signal.SIGTERM, _sigterm)


def _on_safe_stop_command() -> None:
    _request_safe_stop_via_runtime_or_sigterm(reason="telegram_safe_stop")
    _safe_send_tg("🛑 텔레그램 '종료' 요청: 포지션 정리 후 종료합니다.")


def _maybe_position_resync_or_raise(now_ts: float) -> None:
    global LAST_EXCHANGE_SYNC_TS

    position_resync_sec = _require_float(
        _require_attr_value(SET, "position_resync_sec", "settings"),
        "settings.position_resync_sec",
        min_value=0.001,
    )
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
            _request_safe_stop(runtime, reason="balance_check_failed")
            raise RuntimeError("balance check failed consecutively (STRICT)") from e
    finally:
        LAST_BALANCE_LOG_TS = now_ts


def _maybe_fill_check_or_raise(now_ts: float, runtime: EngineLoopRuntime) -> None:
    global LAST_FILL_CHECK_TS, CONSEC_LOSSES, LAST_CLOSE_TS

    poll_fills_sec = _require_float(
        _require_attr_value(SET, "poll_fills_sec", "settings"),
        "settings.poll_fills_sec",
        min_value=0.001,
    )
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

        reason = str(closed["reason"]).strip() if "reason" in closed and closed["reason"] is not None else ""
        summary = closed["summary"] if "summary" in closed else None

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

        trade_id = _require_attr_value(t, "id", "trade")
        if not isinstance(trade_id, int) or trade_id <= 0:
            raise RuntimeError("closed trade missing valid trade.id (STRICT)")

        exit_ts_dt = _resolve_closed_trade_exit_ts_dt_strict(t, summary)

        tp_pct_v = getattr(t, "tp_pct") if hasattr(t, "tp_pct") else None
        sl_pct_v = getattr(t, "sl_pct") if hasattr(t, "sl_pct") else None

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
            _require_attr_value(SET, "hard_consecutive_losses_limit", "settings"),
            "settings.hard_consecutive_losses_limit",
            min_value=0,
        )
        _ = hard_consec_limit_i

        if hard_consec_limit_i > 0 and CONSEC_LOSSES >= hard_consec_limit_i:
            _request_safe_stop(runtime, reason="hard_consecutive_losses_limit_reached")
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
    if et not in ("QUANT_ANALYSIS", "QUANT_ANALYSIS_ERROR"):
        raise RuntimeError(f"unexpected auto report event_type (STRICT): {et}")

    rt = _require_nonempty_str(report_type, "report_type")
    data = _require_dict(payload, "payload")
    if "generated_ts_ms" not in data:
        raise RuntimeError("payload.generated_ts_ms missing (STRICT)")
    generated_ts_ms = _require_int(data["generated_ts_ms"], "payload.generated_ts_ms", min_value=1)
    ts_utc = datetime.datetime.fromtimestamp(generated_ts_ms / 1000.0, tz=datetime.timezone.utc)

    if et == "QUANT_ANALYSIS":
        reason = "auto_report_generated"
    else:
        reason = "auto_report_failed"

    record_quant_analysis_event_db(
        ts_utc=ts_utc,
        symbol=_normalize_symbol_strict(SET.symbol),
        report_type=rt,
        reason=reason,
        analysis_payload=data,
        regime=None,
        is_test=False,
    )


def _auto_report_notifier(report_type: str, payload: Dict[str, Any]) -> None:
    rt = _require_nonempty_str(report_type, "report_type")
    data = _require_dict(payload, "payload")
    if "generated_ts_ms" not in data:
        raise RuntimeError("payload.generated_ts_ms missing (STRICT)")
    generated_ts_ms = _require_int(data["generated_ts_ms"], "payload.generated_ts_ms", min_value=1)
    dt_kst = datetime.datetime.fromtimestamp(generated_ts_ms / 1000.0, tz=datetime.timezone.utc).astimezone(
        datetime.timezone(datetime.timedelta(hours=9))
    )

    error_type = data["error_type"] if "error_type" in data else None
    error_message = data["error_message"] if "error_message" in data else None

    if error_type is not None or error_message is not None:
        err_type = _require_nonempty_str(error_type or "UNKNOWN", "payload.error_type")
        err_msg = _require_nonempty_str(error_message or "unknown error", "payload.error_message")
        _safe_send_tg(
            f"⚠️ 자동 분석 리포트 실패\n"
            f"- 유형: {rt}\n"
            f"- 종목: {_normalize_symbol_strict(SET.symbol)}\n"
            f"- 시각: {dt_kst.strftime('%Y-%m-%d %H:%M:%S KST')}\n"
            f"- 오류: {err_type}: {err_msg}"
        )
        return

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


def _maybe_auto_report_best_effort(now_ts: float) -> None:
    global _AUTO_REPORTER
    if _AUTO_REPORTER is None:
        log("[AUTO_REPORT][SKIP] auto reporter is not initialized")
        return

    now_ms = _require_int(now_ts * 1000.0, "now_ms", min_value=1)

    try:
        result = _AUTO_REPORTER.run_due_reports(now_ms=now_ms)
    except Exception as e:
        log(f"[AUTO_REPORT][ERROR] isolated failure: {type(e).__name__}: {e}")
        _safe_send_tg(f"⚠️ 자동 분석 리포트 격리 실패: {type(e).__name__}: {e}")
        return

    reports = _require_attr_value(result, "reports", "auto_report_result")
    if not isinstance(reports, list):
        log("[AUTO_REPORT][ERROR] run_due_reports returned invalid reports payload")
        _safe_send_tg("⚠️ 자동 분석 리포트 결과 payload invalid")
        return


def _ensure_commentary_engine_initialized_or_raise() -> None:
    global _COMMENTARY_ENGINE_INITIALIZED
    if _COMMENTARY_ENGINE_INITIALIZED:
        return
    init_commentary_engine()
    _COMMENTARY_ENGINE_INITIALIZED = True


def _on_watchdog_fatal(reason: str, detail: Dict[str, Any]) -> None:
    reason_s = _require_nonempty_str(reason, "reason")
    detail_d = _require_dict(detail, "detail")
    _request_safe_stop_via_runtime_or_sigterm(reason=f"watchdog_fatal:{reason_s}")
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
        _request_safe_stop(runtime, reason="monitoring_cycle_requested_safe_stop")

    _maybe_position_resync_or_raise(now_ts)
    _maybe_balance_check_or_raise(now_ts, runtime)
    _maybe_fill_check_or_raise(now_ts, runtime)
    _maybe_auto_report_best_effort(now_ts)

    if runtime.safe_stop_requested:
        _request_safe_stop(runtime, reason="post_monitoring_runtime_safe_stop")


def _position_supervisor_cycle_wrapper(
    now_ts: float,
    runtime: EngineLoopRuntime,
    supervisor: PositionSupervisor,
) -> None:
    _ = _require_float(now_ts, "now_ts", min_value=0.0)

    if not isinstance(supervisor, PositionSupervisor):
        raise RuntimeError("supervisor must be PositionSupervisor (STRICT)")

    if len(OPEN_TRADES) > 1:
        _request_safe_stop(runtime, reason="one_way_mode_open_trades_overflow")
        raise RuntimeError(f"OPEN_TRADES must be <= 1 in one-way mode (STRICT), got={len(OPEN_TRADES)}")

    expected_trade: Optional[Trade] = None
    if len(OPEN_TRADES) == 1:
        t = OPEN_TRADES[0]
        if not isinstance(t, Trade):
            raise RuntimeError(f"OPEN_TRADES[0] must be Trade (STRICT), got={type(t).__name__}")
        expected_trade = t

    try:
        snapshot = run_position_supervisor_once_or_raise(
            supervisor=supervisor,
            expected_trade=expected_trade,
        )
    except PositionSupervisorError as e:
        _request_safe_stop(runtime, reason="position_supervisor_error")
        msg = f"[SAFE_STOP][POSITION_SUPERVISOR] {type(e).__name__}: {e}"
        log(msg)
        _safe_send_tg(f"⛔ POSITION SUPERVISOR 치명 상태: {e}")
        raise RuntimeError(msg) from e

    runtime.extra["last_position_supervisor_snapshot"] = snapshot

    if runtime.safe_stop_requested:
        _request_safe_stop(runtime, reason="position_supervisor_requested_safe_stop")


def _reconciliation_cycle_wrapper(
    now_ts: float,
    runtime: EngineLoopRuntime,
    reconcile_engine: Any,
) -> None:
    _ = _require_float(now_ts, "now_ts", min_value=0.0)

    if reconcile_engine is None:
        raise RuntimeError("reconcile_engine is required (STRICT)")
    if not hasattr(reconcile_engine, "run_if_due"):
        raise RuntimeError("reconcile_engine.run_if_due missing (STRICT)")

    run_if_due = getattr(reconcile_engine, "run_if_due")
    if not callable(run_if_due):
        raise RuntimeError("reconcile_engine.run_if_due must be callable (STRICT)")

    try:
        result = run_if_due(now_ts=now_ts)
    except Exception as e:
        _request_safe_stop(runtime, reason="reconciliation_cycle_error")
        msg = f"[SAFE_STOP][RECONCILIATION] {type(e).__name__}: {e}"
        log(msg)
        _safe_send_tg(f"⛔ RECONCILIATION 치명 상태: {e}")
        raise RuntimeError(msg) from e

    if result is not None:
        runtime.extra["last_reconcile_result"] = result

    if runtime.safe_stop_requested:
        _request_safe_stop(runtime, reason="reconciliation_requested_safe_stop")


def _entry_risk_execution_cycle_wrapper(
    now_ts: float,
    runtime: EngineLoopRuntime,
    entry_cycle_fn: Callable[[float, EngineLoopRuntime], None],
    risk_cycle_fn: Callable[[float, EngineLoopRuntime], None],
    execution_cycle_fn: Callable[[float, EngineLoopRuntime], None],
    position_supervisor_cycle_fn: Callable[[float, EngineLoopRuntime], None],
) -> None:
    entry_cycle_fn(now_ts, runtime)
    if runtime.safe_stop_requested:
        _request_safe_stop(runtime, reason="entry_cycle_requested_safe_stop")
        return

    risk_cycle_fn(now_ts, runtime)
    if runtime.safe_stop_requested:
        _request_safe_stop(runtime, reason="risk_cycle_requested_safe_stop")
        return

    execution_cycle_fn(now_ts, runtime)
    if runtime.safe_stop_requested:
        _request_safe_stop(runtime, reason="execution_cycle_requested_safe_stop")
        return

    if OPEN_TRADES:
        position_supervisor_cycle_fn(now_ts, runtime)
        if runtime.safe_stop_requested:
            _request_safe_stop(runtime, reason="post_execution_position_supervisor_requested_safe_stop")


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
        _request_safe_stop(runtime, reason="open_position_cycle_requested_safe_stop")

    return handled


def _idle_safe_stop_fn(now_ts: float, runtime: EngineLoopRuntime) -> bool:
    global _HALT_NOTICE_SENT

    _ = _require_float(now_ts, "now_ts", min_value=0.0)

    if OPEN_TRADES:
        return False

    if not runtime.safe_stop_requested:
        return False

    if not _HALT_NOTICE_SENT:
        _HALT_NOTICE_SENT = True
        _safe_send_tg("🛑 포지션 0 확인. 자동매매를 종료합니다.")
    return True


def main() -> None:
    global RUNNING
    global _ENGINE_RUNTIME
    global OPEN_TRADES, TRADER_STATE
    global LAST_CLOSE_TS, LAST_EXCHANGE_SYNC_TS, LAST_EXIT_CANDLE_TS_1M
    global CONSEC_LOSSES, _BALANCE_CONSEC_FAILS, LAST_BALANCE_LOG_TS, LAST_FILL_CHECK_TS
    global SIGTERM_REQUESTED_AT, SIGTERM_DEADLINE_TS, _SIGTERM_NOTICE_SENT, _HALT_NOTICE_SENT
    global _AUTO_REPORTER

    RUNNING = True
    _ENGINE_RUNTIME = None

    OPEN_TRADES = []
    TRADER_STATE = TraderState()

    LAST_CLOSE_TS = 0.0
    LAST_EXCHANGE_SYNC_TS = 0.0
    LAST_EXIT_CANDLE_TS_1M = None

    CONSEC_LOSSES = 0
    _BALANCE_CONSEC_FAILS = 0
    LAST_BALANCE_LOG_TS = 0.0
    LAST_FILL_CHECK_TS = 0.0

    SIGTERM_REQUESTED_AT = None
    SIGTERM_DEADLINE_TS = None
    _SIGTERM_NOTICE_SENT = False
    _HALT_NOTICE_SENT = False

    CLOSED_TRADES_CACHE.clear()
    _AUTO_REPORTER = None
    _invalidate_equity_cache()

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

        reconcile_confirm_n = _require_int(
            _require_attr_value(SET, "reconcile_confirm_n", "settings"),
            "settings.reconcile_confirm_n",
            min_value=1,
        )
        hard_consecutive_losses_limit = _require_int(
            _require_attr_value(SET, "hard_consecutive_losses_limit", "settings"),
            "settings.hard_consecutive_losses_limit",
            min_value=0,
        )
        _ = hard_consecutive_losses_limit

        from sync.reconcile_engine import ReconcileConfig, ReconcileEngine

        reconcile_engine = ReconcileEngine(
            ReconcileConfig(
                symbol=str(SET.symbol),
                interval_sec=_require_int(
                    _require_attr_value(SET, "reconcile_interval_sec", "settings"),
                    "settings.reconcile_interval_sec",
                    min_value=1,
                ),
                desync_confirm_n=int(reconcile_confirm_n),
            ),
            fetch_exchange_position=lambda symbol=str(SET.symbol): _fetch_exchange_position_snapshot(symbol),
            get_local_position=lambda symbol=str(SET.symbol): _get_local_position_snapshot(symbol),
            fetch_exchange_open_orders=lambda symbol=str(SET.symbol): _fetch_exchange_open_orders_snapshot(symbol),
            on_desync=lambda result: _on_reconcile_desync(result),
        )

        position_supervisor = build_position_supervisor_or_raise(
            settings=SET,
            symbol=str(SET.symbol),
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
            safe_stop_requested=False,
            halted=False,
            sigterm_deadline_ts=SIGTERM_DEADLINE_TS,
        )
        _ENGINE_RUNTIME = runtime

        start_watchdog(
            settings=SET,
            symbol=str(SET.symbol),
            on_fatal=_on_watchdog_fatal,
        )
        watchdog_started = True

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
            position_supervisor_cycle_fn=lambda now_ts, rt: _position_supervisor_cycle_wrapper(
                now_ts,
                rt,
                position_supervisor,
            ),
            reconciliation_cycle_fn=lambda now_ts, rt: _reconciliation_cycle_wrapper(
                now_ts,
                rt,
                reconcile_engine,
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
                lambda _now_ts, _rt: _position_supervisor_cycle_wrapper(_now_ts, _rt, position_supervisor),
            ),
            idle_safe_stop_fn=_idle_safe_stop_fn,
            stop_flag_getter=_stop_flag_getter,
        )

    except EngineLoopHalted:
        return
    except Exception as e:
        if _ENGINE_RUNTIME is not None:
            _request_safe_stop(_ENGINE_RUNTIME, reason="fatal_exception")

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
        RUNNING = False
        _ENGINE_RUNTIME = None
        _AUTO_REPORTER = None
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
        if "symbol" not in r:
            raise RuntimeError("positionRisk.symbol missing (STRICT)")
        if str(r["symbol"]).upper().strip() != sym:
            continue
        if "positionAmt" not in r:
            raise RuntimeError("positionRisk.positionAmt missing (STRICT)")
        amt = _require_float(r["positionAmt"], "positionRisk.positionAmt")
        if abs(amt) > 1e-12:
            live.append(r)

    if not live:
        return {"symbol": sym, "positionAmt": "0", "entryPrice": "0"}

    dirs: set[str] = set()
    pos_sides: set[str] = set()
    total_signed_amt = 0.0
    weighted_entry_sum = 0.0

    for idx, r in enumerate(live):
        amt = _require_float(r["positionAmt"], f"positionRisk[{idx}].positionAmt")
        if "entryPrice" not in r:
            raise RuntimeError("positionRisk.entryPrice missing (STRICT)")
        entry = _require_float(r["entryPrice"], f"positionRisk[{idx}].entryPrice", min_value=0.0)
        if entry <= 0:
            raise RuntimeError("positionRisk.entryPrice must be > 0 (STRICT)")

        ps_raw = str(r["positionSide"]).upper().strip() if "positionSide" in r and r["positionSide"] is not None else "BOTH"
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
    if not isinstance(t, Trade):
        raise RuntimeError(f"OPEN_TRADES[0] must be Trade (STRICT), got={type(t).__name__}")

    trade_symbol = _require_attr_value(t, "symbol", "trade")
    if str(trade_symbol).upper().strip() != sym:
        raise RuntimeError(f"local trade symbol mismatch (STRICT): trade={trade_symbol} expected={sym}")

    trade_side = _require_attr_value(t, "side", "trade")
    side = str(trade_side).upper().strip()
    if side in ("LONG", "BUY"):
        sign = 1.0
    elif side in ("SHORT", "SELL"):
        sign = -1.0
    else:
        raise RuntimeError(f"invalid trade.side (STRICT): {trade_side!r}")

    remaining_qty_raw = _require_attr_value(t, "remaining_qty", "trade")
    qty = _require_float(remaining_qty_raw, "trade.remaining_qty", min_value=0.0)
    if qty <= 0:
        raise RuntimeError("trade.remaining_qty must be > 0 (STRICT)")

    if hasattr(t, "entry_price"):
        entry_price_raw = getattr(t, "entry_price")
    elif hasattr(t, "entry"):
        entry_price_raw = getattr(t, "entry")
    else:
        raise RuntimeError("trade.entry_price/entry missing (STRICT)")

    if entry_price_raw is None:
        raise RuntimeError("trade.entry_price/entry missing (STRICT)")

    ep = _require_float(entry_price_raw, "trade.entry_price", min_value=0.0)
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
    if result is None:
        raise RuntimeError("reconcile result is None (STRICT)")
    if not hasattr(result, "desync_confirmed"):
        raise RuntimeError("result.desync_confirmed missing (STRICT)")
    desync_confirmed = getattr(result, "desync_confirmed")
    if not isinstance(desync_confirmed, bool):
        raise RuntimeError("result.desync_confirmed must be bool (STRICT)")
    if not desync_confirmed:
        raise RuntimeError("on_desync called but desync_confirmed is False (STRICT)")

    _request_safe_stop_via_runtime_or_sigterm(reason="reconcile_desync_confirmed")

    symbol = _normalize_symbol_strict(_require_attr_value(result, "symbol", "result"), name="result.symbol")

    issues = _require_attr_value(result, "issues", "result")
    if not isinstance(issues, (list, tuple)):
        raise RuntimeError("result.issues must be list/tuple (STRICT)")

    msg = f"⛔ DESYNC 확정: {symbol} issues={len(issues)} (신규 진입 차단, 종료)"
    log(msg)
    for it in issues:
        if it is None:
            raise RuntimeError("result.issues contains None (STRICT)")
        code = _require_attr_value(it, "code", "issue")
        message = _require_attr_value(it, "message", "issue")
        details = _require_attr_value(it, "details", "issue")
        log(f"[DESYNC] {code} | {message} | {details}")
    _safe_send_tg(msg)

    force_close_on_desync = _require_bool(
        _require_attr_value(SET, "force_close_on_desync", "settings"),
        "settings.force_close_on_desync",
    )
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
    main()