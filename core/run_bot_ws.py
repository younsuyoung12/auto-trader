# core/run_bot_ws.py
"""
============================================================
FILE: core/run_bot_ws.py
ROLE:
- Binance USDT-M Futures WebSocket 기반 메인 엔진 루프
- structured health / reconcile / risk / execution 계층을 연결해
  실제 자동매매 런타임을 운영한다

CORE RESPONSIBILITIES:
- market/account websocket 부팅 및 준비 확인
- structured data health 상태 소비(OK/WARNING/FAIL)
- entry candidate → risk physics → execution engine 오케스트레이션
- protection guard / reconcile / fill check / safe stop 처리
- WS/Execution latency 및 invariant/drift 감시
- WS candle(open/closed) 상태를 저장 계층까지 정합하게 전달

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- 시장데이터 의사결정은 WS 버퍼 기준
- REST는 부팅 백필 / 계정·주문 상태 조회에만 사용
- data_health_monitor의 WARNING 은 관측 상태이며 FAIL 과 동일 취급 금지
- WS transport 실패만 치명 liveness failure 로 취급
- settings는 SETTINGS 단일 객체만 사용
- 숨은 기본값(getattr(..., default)) 금지
- 비치명 엔트리 거절(None 반환)은 명시적 SKIP 처리
- 치명 오류는 SAFE_STOP + 예외 전파
- candle 저장 경로는 is_closed 계약을 절대 유실하면 안 된다
- authoritative 5m entry gate 는 치명 build 실패 전에 선점하지 않는다

CHANGE HISTORY:
- 2026-03-11:
  1) FIX(ROOT-CAUSE): md-store 경로를 ws_get_klines_with_volume_and_closed() 기반으로 변경
  2) FIX(ROOT-CAUSE): 같은 ts의 open candle 갱신/close 전환도 저장 대상으로 재선정
  3) FIX(CONTRACT): validate_kline_series_strict()는 legacy 6-tuple로 검증하고 저장은 7-tuple(is_closed 포함)로 전달
  4) FIX(TRADE-GRADE): md-store의 마지막 저장 상태를 interval별로 추적해 중복/누락/동일 ts 갱신 누락 제거
  5) FIX(ROOT-CAUSE): entry signal gate claim 시점을 entry_pipeline 완료 후로 이동
     - _build_entry_market_data() 치명 실패 시 현재 5m candle 이 영구 소모되지 않도록 수정
     - 전략상 no-signal / explicit SKIP 은 기존대로 현재 candle 을 1회 평가로 소모
  6) ADD(STRICT): 이미 claim된 authoritative 5m candle 은 expensive entry build 단계 재진입 전에 fast-path 로 차단
  7) ADD(OBSERVABILITY): entry pipeline build 실패를 stage-localized fatal error 문맥으로 승격
  8) FIX(CONTRACT): typing import에 Callable 추가
     - _build_entry_market_data_stage_or_raise() 타입 힌트와 import 선언 불일치 제거
- 2026-03-10:
  1) FIX(ROOT-CAUSE): data_health_monitor 3단계 상태(OK/WARNING/FAIL)를 run_bot_ws 에서 직접 소비
  2) FIX(ROOT-CAUSE): _ws_liveness_guard_or_raise() 가 market_feed warning 을 치명 장애로 승격하지 않도록 수정
  3) FIX(SSOT): settings.load_settings() 재호출 제거, SETTINGS 단일 객체 사용
  4) FIX(STRICT): market ws ready timeout 계산의 getattr default 제거
  5) FIX(OBSERVABILITY): WARNING health reason 포맷 helper 추가
============================================================
"""

from __future__ import annotations

import datetime
import hashlib
import math
import signal
import threading
import time
import traceback
from collections import deque
from typing import Any, Callable, Dict, List, Optional, Tuple

from sqlalchemy import text

from settings import SETTINGS
from infra.telelog import log, send_tg
from infra.async_worker import start_worker as start_async_worker, submit as submit_async

from execution.exchange_api import (
    fetch_open_orders,
    fetch_open_positions,
    get_available_usdt,
    get_balance_detail,
    set_leverage_and_mode,
)
from execution.order_executor import close_all_positions_market
from execution.execution_engine import ExecutionEngine

from state.db_core import get_session
from state.sync_exchange import sync_open_trades_from_exchange
from state.trader_state import Trade, TraderState, check_closes, build_close_summary_strict
from state.exit_engine import maybe_exit_with_gpt

from events.signals_logger import log_signal

from infra.bot_workers import start_telegram_command_thread
from strategy.signal_analysis_worker import start_signal_analysis_thread

from infra.market_data_ws import (
    backfill_klines_from_rest,
    get_health_snapshot as ws_get_health_snapshot,
    get_klines_with_volume as ws_get_klines_with_volume,
    get_klines_with_volume_and_closed as ws_get_klines_with_volume_and_closed,
    get_orderbook as ws_get_orderbook,
    get_ws_status as ws_get_ws_status,
    start_ws_loop,
)
from infra.account_ws import (
    get_account_protection_orders_snapshot,
    get_account_ws_status,
    start_account_ws_loop,
)
from infra.market_data_store import save_candles_bulk_from_ws, save_orderbook_from_ws
from infra.market_data_rest import fetch_klines_rest, KlineRestError

import infra.data_health_monitor as data_health_monitor
from infra.data_health_monitor import start_health_monitor

from strategy.regime_engine import RegimeEngine
from strategy.account_state_builder import AccountStateBuilder, AccountStateNotReadyError
from execution.risk_physics_engine import RiskPhysicsEngine, RiskPhysicsPolicy
from strategy.signal import Signal
from core.entry_pipeline import _build_entry_market_data, _decide_entry_candidate_strict

from strategy.ev_heatmap_engine import EvHeatmapEngine, HeatmapKey

from sync.reconcile_engine import ReconcileConfig, ReconcileEngine, ReconcileResult

from infra.data_integrity_guard import (
    DataIntegrityError,
    validate_entry_market_data_bundle_strict,
    validate_kline_series_strict,
    validate_orderbook_strict,
)
from execution.invariant_guard import (
    InvariantViolation,
    SignalInvariantInputs,
    validate_signal_invariants_strict,
)
from infra.drift_detector import (
    DriftDetectedError,
    DriftDetector,
    DriftDetectorConfig,
    DriftSnapshot,
)

SET = SETTINGS
RUNNING: bool = True

ENTRY_REQUIRED_TFS: tuple[str, ...] = ("1m", "5m", "15m", "1h", "4h")
ENTRY_REQUIRED_KLINES_MIN: Dict[str, int] = {
    "1m": 120,
    "5m": 200,
    "15m": 200,
    "1h": 200,
    "4h": 200,
}

ENTRY_REQUIRED_RUNTIME_SETTINGS: tuple[str, ...] = (
    "ws_enabled",
    "ws_subscribe_tfs",
    "ws_backfill_tfs",
    "ws_backfill_limit",
    "api_key",
    "api_secret",
    "leverage",
    "isolated",
    "allow_start_without_leverage_setup",
    "signal_analysis_interval_sec",
    "position_resync_sec",
    "poll_fills_sec",
    "max_signal_latency_ms",
    "max_exec_latency_ms",
    "drift_allocation_abs_jump",
    "drift_allocation_spike_ratio",
    "drift_multiplier_abs_jump",
    "drift_micro_abs_jump",
    "drift_stable_regime_steps",
    "async_worker_threads",
    "async_worker_queue_size",
    "sigterm_grace_sec",
    "reconcile_confirm_n",
    "hard_consecutive_losses_limit",
    "reconcile_interval_sec",
    "force_close_on_desync",
    "test_dry_run",
    "test_fake_available_usdt",
    "symbol",
    "ws_max_kline_delay_sec",
    "ws_market_event_max_delay_sec",
)

_LAST_ENTRY_BLOCK_TG_TS: float = 0.0
_LAST_ENTRY_BLOCK_KEY: str = ""

_LAST_ERROR_TG_TS: float = 0.0
_LAST_ERROR_TG_KEY: str = ""

SIGTERM_REQUESTED_AT: Optional[float] = None
SIGTERM_DEADLINE_TS: Optional[float] = None
_SIGTERM_NOTICE_SENT: bool = False
_SIGTERM_DEADLINE_HANDLED: bool = False

OPEN_TRADES: List[Trade] = []
TRADER_STATE: TraderState = TraderState()
LAST_CLOSE_TS: float = 0.0
CONSEC_LOSSES: int = 0
SAFE_STOP_REQUESTED: bool = False
LAST_EXCHANGE_SYNC_TS: float = 0.0

SIGNAL_ANALYSIS_INTERVAL_SEC: int = int(SET.signal_analysis_interval_sec)
LAST_EXIT_CANDLE_TS_1M: Optional[int] = None
LAST_ENTRY_GPT_CALL_TS: float = 0.0

# 같은 authoritative 5m 캔들(openTime)에서는 진입 평가를 한 번만 수행한다.
# - SIGNAL: chosen_signal / signal_source / extra 를 freeze
# - NO_SIGNAL: no-signal 결정도 freeze (같은 캔들 동안 재계산 금지)
# - last_price 는 freeze 하지 않는다(현재가 반영 유지)
LAST_ENTRY_EVAL_SIGNAL_TS_MS: Optional[int] = None
ENTRY_GATE_RUNTIME_STATE_SCOPE: str = "ENTRY_EVAL_SIGNAL_GATE"
ENTRY_GATE_RUNTIME_STATE_TABLE: str = "bt_engine_runtime_state"

_BALANCE_CONSEC_FAILS: int = 0
_EQUITY_CONSEC_FAILS: int = 0
_WS_LIVENESS_CONSEC_FAILS: int = 0
_ACCOUNT_WS_CONSEC_FAILS: int = 0

_BALANCE_FAIL_HARDSTOP_N: int = 3
_EQUITY_FAIL_HARDSTOP_N: int = 3
_WS_LIVENESS_FAIL_HARDSTOP_N: int = 3
_ACCOUNT_WS_FAIL_HARDSTOP_N: int = 3
_ACCOUNT_WS_READY_TIMEOUT_SEC: float = 20.0

ENGINE_LOOP_TICK_SEC: float = 0.20
ENGINE_PROTECTION_GUARD_INTERVAL_SEC: float = 1.00
ENGINE_ENTRY_OB_MIN_INTERVAL_SEC: float = 0.35
ENGINE_ENTRY_FORCE_INTERVAL_SEC: float = 1.00
ENGINE_EQUITY_CACHE_TTL_SEC: float = 1.00

_EQUITY_CACHE_VALUE: Optional[float] = None
_EQUITY_CACHE_TS: float = 0.0

WsStoreCandleRow = Tuple[int, float, float, float, float, float, bool]
LegacyStoreCandleRow = Tuple[int, float, float, float, float, float]


def _require_runtime_setting_exists(name: str) -> Any:
    if not hasattr(SET, name):
        raise RuntimeError(f"settings.{name} is required (STRICT)")
    return getattr(SET, name)


def _verify_runtime_settings_contract_or_raise() -> None:
    for name in ENTRY_REQUIRED_RUNTIME_SETTINGS:
        _require_runtime_setting_exists(name)


def _as_float(v: Any, name: str, *, min_value: Optional[float] = None, max_value: Optional[float] = None) -> float:
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
    if max_value is not None and x > max_value:
        raise RuntimeError(f"{name} must be <= {max_value}")
    return float(x)


def _require_int_ms(v: Any, name: str) -> int:
    if isinstance(v, bool):
        raise RuntimeError(f"{name} must be int ms (bool not allowed)")
    try:
        iv = int(v)
    except Exception as e:
        raise RuntimeError(f"{name} must be int ms (STRICT): {e}") from e
    if iv <= 0:
        raise RuntimeError(f"{name} must be > 0 (STRICT)")
    return iv


def _require_bool(v: Any, name: str) -> bool:
    if not isinstance(v, bool):
        raise RuntimeError(f"{name} must be bool (STRICT)")
    return bool(v)


def _normalize_symbol_strict(symbol: Any, *, name: str = "symbol") -> str:
    s = str(symbol or "").replace("-", "").replace("/", "").upper().strip()
    if not s:
        raise RuntimeError(f"{name} is empty (STRICT)")
    return s


def _join_reason_list_strict(v: Any, name: str) -> str:
    if not isinstance(v, list):
        raise RuntimeError(f"{name} must be list (STRICT)")
    cleaned = [str(x).strip() for x in v if str(x).strip()]
    return " | ".join(cleaned)


def _normalize_health_level_strict(v: Any, name: str) -> str:
    s = str(v or "").strip().upper()
    if s not in ("OK", "WARNING", "FAIL"):
        raise RuntimeError(f"{name} must be OK/WARNING/FAIL (STRICT), got={v!r}")
    return s


def _format_data_health_snapshot_reason(snapshot: Dict[str, Any], fallback_reason: str) -> str:
    if not isinstance(snapshot, dict):
        fb = str(fallback_reason or "").strip()
        if fb:
            return fb
        raise RuntimeError("data health snapshot must be dict (STRICT)")

    reasons: List[str] = []

    fail_reason = str(snapshot.get("fail_reason") or "").strip()
    if fail_reason:
        reasons.append(fail_reason)

    ws = snapshot.get("ws")
    if isinstance(ws, dict):
        ws_reasons = ws.get("overall_reasons")
        if isinstance(ws_reasons, list):
            joined = _join_reason_list_strict(ws_reasons, "health_snapshot.ws.overall_reasons")
            if joined:
                reasons.append(f"ws={joined}")

    feature = snapshot.get("feature")
    if isinstance(feature, dict):
        if not bool(feature.get("ok", False)):
            missing_tfs = feature.get("missing_tfs")
            if isinstance(missing_tfs, list) and missing_tfs:
                reasons.append(f"feature_missing_tfs={missing_tfs}")
            field = feature.get("field")
            if field is not None and str(field).strip():
                reasons.append(f"feature_field={field}")
            err = feature.get("error_type")
            if err is not None and str(err).strip():
                reasons.append(f"feature_error_type={err}")

    if not reasons:
        fb = str(fallback_reason or "").strip()
        if fb:
            return fb
        raise RuntimeError("health fail reason missing (STRICT)")

    unique: List[str] = []
    seen: set[str] = set()
    for r in reasons:
        if r not in seen:
            seen.add(r)
            unique.append(r)
    return " | ".join(unique)


def _format_data_health_warning_reason(snapshot: Dict[str, Any], fallback_warning: str) -> str:
    if not isinstance(snapshot, dict):
        fb = str(fallback_warning or "").strip()
        if fb:
            return fb
        raise RuntimeError("data health snapshot must be dict (STRICT)")

    reasons: List[str] = []

    warning_reason = str(snapshot.get("warning_reason") or "").strip()
    if warning_reason:
        reasons.append(warning_reason)

    ws = snapshot.get("ws")
    if isinstance(ws, dict):
        ws_warnings = ws.get("overall_warnings")
        if isinstance(ws_warnings, list):
            joined = _join_reason_list_strict(ws_warnings, "health_snapshot.ws.overall_warnings")
            if joined:
                reasons.append(f"ws={joined}")

    if not reasons:
        fb = str(fallback_warning or "").strip()
        if fb:
            return fb
        raise RuntimeError("health warning reason missing (STRICT)")

    unique: List[str] = []
    seen: set[str] = set()
    for r in reasons:
        if r not in seen:
            seen.add(r)
            unique.append(r)
    return " | ".join(unique)


def _get_data_health_state_or_raise() -> Tuple[str, str, str, Dict[str, Any]]:
    level_state = data_health_monitor.get_health_level_state()
    if not isinstance(level_state, dict):
        raise RuntimeError("data_health_monitor.get_health_level_state() must return dict (STRICT)")

    level = _normalize_health_level_strict(level_state.get("level"), "health_level_state.level")
    ok = level_state.get("ok")
    if not isinstance(ok, bool):
        raise RuntimeError("health_level_state.ok must be bool (STRICT)")
    has_warning = level_state.get("has_warning")
    if not isinstance(has_warning, bool):
        raise RuntimeError("health_level_state.has_warning must be bool (STRICT)")

    fail_reason = level_state.get("fail_reason")
    if not isinstance(fail_reason, str):
        raise RuntimeError("health_level_state.fail_reason must be str (STRICT)")
    warning_reason = level_state.get("warning_reason")
    if not isinstance(warning_reason, str):
        raise RuntimeError("health_level_state.warning_reason must be str (STRICT)")

    if level == "FAIL" and ok:
        raise RuntimeError("health level FAIL but ok=True (STRICT)")
    if level == "WARNING" and (not ok or not has_warning):
        raise RuntimeError("health level WARNING must satisfy ok=True and has_warning=True (STRICT)")
    if level == "OK" and (not ok or has_warning):
        raise RuntimeError("health level OK must satisfy ok=True and has_warning=False (STRICT)")

    snapshot = data_health_monitor.get_last_health_snapshot()
    if not isinstance(snapshot, dict):
        raise RuntimeError("data_health_monitor.get_last_health_snapshot() must return dict (STRICT)")

    return level, fail_reason, warning_reason, snapshot


def _entry_gate_lock_key(symbol: str) -> int:
    sym = _normalize_symbol_strict(symbol, name="entry_gate.symbol")
    raw = hashlib.sha1(f"{ENTRY_GATE_RUNTIME_STATE_SCOPE}:{sym}".encode("utf-8")).digest()[:8]
    key = int.from_bytes(raw, byteorder="big", signed=False) & 0x7FFFFFFFFFFFFFFF
    if key <= 0:
        raise RuntimeError("entry gate advisory lock key invalid (STRICT)")
    return key


def _ensure_entry_gate_runtime_state_table_or_raise() -> None:
    ddl = text(
        f"""
        CREATE TABLE IF NOT EXISTS {ENTRY_GATE_RUNTIME_STATE_TABLE} (
            scope TEXT NOT NULL,
            symbol TEXT NOT NULL,
            last_entry_eval_signal_ts_ms BIGINT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (scope, symbol)
        )
        """
    )

    with get_session() as session:
        session.execute(ddl)
        session.commit()


def _load_persisted_entry_signal_ts_or_raise(symbol: str) -> Optional[int]:
    sym = _normalize_symbol_strict(symbol, name="entry_gate.symbol")
    q = text(
        f"""
        SELECT last_entry_eval_signal_ts_ms
        FROM {ENTRY_GATE_RUNTIME_STATE_TABLE}
        WHERE scope = :scope
          AND symbol = :symbol
        """
    )

    with get_session() as session:
        row = session.execute(
            q,
            {
                "scope": ENTRY_GATE_RUNTIME_STATE_SCOPE,
                "symbol": sym,
            },
        ).fetchone()

    if row is None:
        return None

    persisted = _require_int_ms(row[0], f"{ENTRY_GATE_RUNTIME_STATE_TABLE}.last_entry_eval_signal_ts_ms")
    return int(persisted)


def _bootstrap_entry_signal_gate_or_raise(symbol: str) -> None:
    global LAST_ENTRY_EVAL_SIGNAL_TS_MS

    _ensure_entry_gate_runtime_state_table_or_raise()
    persisted = _load_persisted_entry_signal_ts_or_raise(symbol)
    LAST_ENTRY_EVAL_SIGNAL_TS_MS = persisted
    if persisted is None:
        log(f"[ENTRY_GATE][BOOT] no persisted signal gate: symbol={_normalize_symbol_strict(symbol)}")
        return

    log(
        "[ENTRY_GATE][BOOT] persisted signal gate loaded: "
        f"symbol={_normalize_symbol_strict(symbol)} last_entry_eval_signal_ts_ms={persisted}"
    )


def _is_current_entry_signal_gate_already_claimed_or_raise(symbol: str, signal_ts_ms: Any) -> bool:
    global LAST_ENTRY_EVAL_SIGNAL_TS_MS

    _normalize_symbol_strict(symbol, name="entry_gate.symbol")
    current_ts = _require_int_ms(signal_ts_ms, "entry_gate.current_signal_ts_ms")
    prev_mem_ts = LAST_ENTRY_EVAL_SIGNAL_TS_MS

    if prev_mem_ts is None:
        return False
    if current_ts < prev_mem_ts:
        raise RuntimeError(
            f"entry signal ts rollback detected vs memory (STRICT): prev={prev_mem_ts} now={current_ts}"
        )
    return bool(current_ts == prev_mem_ts)


def _invalidate_equity_cache() -> None:
    global _EQUITY_CACHE_VALUE, _EQUITY_CACHE_TS
    _EQUITY_CACHE_VALUE = None
    _EQUITY_CACHE_TS = 0.0


def _get_latest_ws_kline_ts_optional(symbol: str, interval: str) -> Optional[int]:
    buf = ws_get_klines_with_volume(symbol, interval, limit=1)
    if not isinstance(buf, list):
        raise RuntimeError(f"ws_get_klines_with_volume returned non-list (STRICT): interval={interval}")
    if not buf:
        return None

    row = buf[-1]
    if not isinstance(row, (list, tuple)) or not row:
        raise RuntimeError(f"ws kline row invalid (STRICT): interval={interval}")
    return _require_int_ms(row[0], f"ws[{interval}].openTime")


def _get_latest_ws_5m_signal_gate_ts_or_raise(symbol: str) -> int:
    latest_5m_ts = _get_latest_ws_kline_ts_optional(symbol, "5m")
    if latest_5m_ts is None:
        raise RuntimeError("authoritative ws 5m latest ts is missing (STRICT)")
    return int(latest_5m_ts)


def _get_orderbook_marker_ts_optional(symbol: str) -> Optional[int]:
    ob = ws_get_orderbook(symbol, limit=1)
    if ob is None:
        return None
    if not isinstance(ob, dict):
        raise RuntimeError("ws_get_orderbook returned non-dict (STRICT)")

    ts_val = ob.get("ts")
    if ts_val is None:
        return None
    return _require_int_ms(ts_val, "orderbook.ts")


def _should_run_entry_cycle(
    symbol: str,
    now_ts: float,
    last_eval_wall_ts: float,
    last_eval_1m_ts: Optional[int],
    last_eval_orderbook_ts: Optional[int],
) -> Tuple[bool, Optional[int], Optional[int]]:
    latest_1m_ts = _get_latest_ws_kline_ts_optional(symbol, "1m")
    latest_orderbook_ts = _get_orderbook_marker_ts_optional(symbol)

    if latest_1m_ts is not None and last_eval_1m_ts is not None and latest_1m_ts < last_eval_1m_ts:
        raise RuntimeError(
            f"entry loop 1m ts rollback detected (STRICT): prev={last_eval_1m_ts} now={latest_1m_ts}"
        )

    if latest_orderbook_ts is not None and last_eval_orderbook_ts is not None and latest_orderbook_ts < last_eval_orderbook_ts:
        raise RuntimeError(
            f"entry loop orderbook ts rollback detected (STRICT): prev={last_eval_orderbook_ts} now={latest_orderbook_ts}"
        )

    if last_eval_wall_ts <= 0.0:
        return True, latest_1m_ts, latest_orderbook_ts

    if latest_1m_ts is not None and (last_eval_1m_ts is None or latest_1m_ts > last_eval_1m_ts):
        return True, latest_1m_ts, latest_orderbook_ts

    if latest_orderbook_ts is not None and (last_eval_orderbook_ts is None or latest_orderbook_ts > last_eval_orderbook_ts):
        if (now_ts - last_eval_wall_ts) >= ENGINE_ENTRY_OB_MIN_INTERVAL_SEC:
            return True, latest_1m_ts, latest_orderbook_ts

    if (now_ts - last_eval_wall_ts) >= ENGINE_ENTRY_FORCE_INTERVAL_SEC:
        return True, latest_1m_ts, latest_orderbook_ts

    return False, latest_1m_ts, latest_orderbook_ts


def _claim_entry_signal_ts_or_skip(symbol: str, signal_ts_ms: Any) -> bool:
    """
    동일 authoritative 5m candle ts 에서는 진입 평가를 한 번만 수행한다.
    - DB 영속 체크포인트를 단일 진실원으로 사용한다.
    - 과거 ts가 오면 rollback으로 간주하고 즉시 예외.
    - 동일 symbol 다중 프로세스 기동에서도 advisory xact lock + row lock 으로 중복 claim을 차단한다.
    """
    global LAST_ENTRY_EVAL_SIGNAL_TS_MS

    sym = _normalize_symbol_strict(symbol, name="entry_gate.symbol")
    current_ts = _require_int_ms(signal_ts_ms, "market_data.signal_ts_ms")
    prev_mem_ts = LAST_ENTRY_EVAL_SIGNAL_TS_MS

    if prev_mem_ts is not None and current_ts < prev_mem_ts:
        raise RuntimeError(
            f"entry signal ts rollback detected vs memory (STRICT): prev={prev_mem_ts} now={current_ts}"
        )

    q_select = text(
        f"""
        SELECT last_entry_eval_signal_ts_ms
        FROM {ENTRY_GATE_RUNTIME_STATE_TABLE}
        WHERE scope = :scope
          AND symbol = :symbol
        FOR UPDATE
        """
    )
    q_insert = text(
        f"""
        INSERT INTO {ENTRY_GATE_RUNTIME_STATE_TABLE} (scope, symbol, last_entry_eval_signal_ts_ms, updated_at)
        VALUES (:scope, :symbol, :signal_ts_ms, NOW())
        """
    )
    q_update = text(
        f"""
        UPDATE {ENTRY_GATE_RUNTIME_STATE_TABLE}
        SET last_entry_eval_signal_ts_ms = :signal_ts_ms,
            updated_at = NOW()
        WHERE scope = :scope
          AND symbol = :symbol
        """
    )

    with get_session() as session:
        session.execute(
            text("SELECT pg_advisory_xact_lock(:lock_key)"),
            {"lock_key": _entry_gate_lock_key(sym)},
        )
        row = session.execute(
            q_select,
            {
                "scope": ENTRY_GATE_RUNTIME_STATE_SCOPE,
                "symbol": sym,
            },
        ).fetchone()

        if row is None:
            session.execute(
                q_insert,
                {
                    "scope": ENTRY_GATE_RUNTIME_STATE_SCOPE,
                    "symbol": sym,
                    "signal_ts_ms": int(current_ts),
                },
            )
            session.commit()
            LAST_ENTRY_EVAL_SIGNAL_TS_MS = int(current_ts)
            return True

        prev_db_ts = _require_int_ms(
            row[0],
            f"{ENTRY_GATE_RUNTIME_STATE_TABLE}.last_entry_eval_signal_ts_ms",
        )

        if current_ts < prev_db_ts:
            raise RuntimeError(
                f"entry signal ts rollback detected vs db (STRICT): prev={prev_db_ts} now={current_ts}"
            )

        if current_ts == prev_db_ts:
            LAST_ENTRY_EVAL_SIGNAL_TS_MS = int(prev_db_ts)
            session.commit()
            return False

        session.execute(
            q_update,
            {
                "scope": ENTRY_GATE_RUNTIME_STATE_SCOPE,
                "symbol": sym,
                "signal_ts_ms": int(current_ts),
            },
        )
        session.commit()

    LAST_ENTRY_EVAL_SIGNAL_TS_MS = int(current_ts)
    return True


def _safe_send_tg(msg: str) -> None:
    try:
        ok = submit_async(send_tg, msg, critical=False, label="send_tg")
        if not ok:
            log(f"[TG][DROP] async queue full: {msg}")
    except Exception as e:
        log(f"[TG] async submit error: {type(e).__name__}: {e} | msg={msg}")


def _parse_tfs(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        items = [x.strip() for x in value.split(",")]
        return [x for x in items if x]
    if isinstance(value, (list, tuple, set)):
        out: List[str] = []
        for v in value:
            s = str(v).strip()
            if s:
                out.append(s)
        return out
    return []


def _verify_required_tfs_or_die(name: str, configured_tfs: List[str], required_tfs: tuple[str, ...]) -> None:
    norm = {str(x).strip().lower() for x in configured_tfs if str(x).strip()}
    missing = [tf for tf in required_tfs if tf.lower() not in norm]
    if missing:
        msg = f"⛔ 설정 오류: {name} 에 필수 TF 누락: {missing}. 필수={list(required_tfs)}"
        log(msg)
        _safe_send_tg(msg)
        raise RuntimeError(msg)


def _verify_ws_boot_configuration_or_die() -> None:
    ws_subscribe_tfs = _parse_tfs(SET.ws_subscribe_tfs)
    _verify_required_tfs_or_die("ws_subscribe_tfs", ws_subscribe_tfs, ENTRY_REQUIRED_TFS)

    ws_backfill_tfs = _parse_tfs(SET.ws_backfill_tfs)
    _verify_required_tfs_or_die("ws_backfill_tfs", ws_backfill_tfs, ENTRY_REQUIRED_TFS)

    backfill_limit = int(SET.ws_backfill_limit)
    if backfill_limit <= 0:
        raise RuntimeError("settings.ws_backfill_limit must be > 0 (STRICT)")


def _maybe_send_entry_block_tg(key: str, msg: str, cooldown_sec: int = 60) -> None:
    global _LAST_ENTRY_BLOCK_TG_TS, _LAST_ENTRY_BLOCK_KEY
    now = time.time()
    if key == _LAST_ENTRY_BLOCK_KEY and (now - _LAST_ENTRY_BLOCK_TG_TS) < cooldown_sec:
        log(f"[SKIP_TG_SUPPRESS] {msg}")
        return
    _LAST_ENTRY_BLOCK_KEY = key
    _LAST_ENTRY_BLOCK_TG_TS = now
    _safe_send_tg(msg)


def _maybe_send_error_tg(key: str, msg: str, cooldown_sec: int = 60) -> None:
    global _LAST_ERROR_TG_TS, _LAST_ERROR_TG_KEY
    now = time.time()
    if key == _LAST_ERROR_TG_KEY and (now - _LAST_ERROR_TG_TS) < cooldown_sec:
        log(f"[SKIP_TG_SUPPRESS][ERROR] {msg}")
        return
    _LAST_ERROR_TG_KEY = key
    _LAST_ERROR_TG_TS = now
    _safe_send_tg(msg)


def interruptible_sleep(total_sec: float, tick: float = 1.0) -> None:
    if total_sec is None:
        return
    total = float(total_sec)
    if total <= 0:
        return

    tick_f = float(tick) if tick is not None else 1.0
    if tick_f <= 0:
        tick_f = 1.0

    end_ts = time.time() + total
    while True:
        if not RUNNING:
            return
        if SAFE_STOP_REQUESTED:
            return
        now = time.time()
        if SIGTERM_DEADLINE_TS is not None and now >= SIGTERM_DEADLINE_TS:
            return
        remain = end_ts - now
        if remain <= 0:
            return
        time.sleep(min(tick_f, remain))


def _normalize_direction_for_events_strict(v: Any) -> str:
    s = str(v or "").upper().strip()
    if s in ("LONG", "SHORT"):
        return s
    if s == "BUY":
        return "LONG"
    if s == "SELL":
        return "SHORT"
    raise RuntimeError(f"invalid trade side for events: {v!r}")


_SIGNAL_EXECUTION_PRICE_ALIASES: tuple[str, ...] = (
    "entry_price_hint",
    "entry_price",
    "price",
    "mark_price",
    "last_price",
    "close",
)


def _extract_first_positive_price_from_mapping_optional(mapping: Any, *, source_name: str) -> tuple[Optional[float], Optional[str]]:
    if mapping is None:
        return None, None
    if not isinstance(mapping, dict):
        raise RuntimeError(f"{source_name} must be dict when provided (STRICT)")

    for alias in _SIGNAL_EXECUTION_PRICE_ALIASES:
        if alias not in mapping:
            continue
        raw = mapping.get(alias)
        if raw is None:
            continue
        price = _as_float(raw, f"{source_name}.{alias}", min_value=0.0)
        if price <= 0.0:
            raise RuntimeError(f"{source_name}.{alias} must be > 0 (STRICT)")
        return float(price), f"{source_name}.{alias}"

    return None, None


def _resolve_entry_price_hint_for_signal_or_raise(
    market_data: Dict[str, Any],
    cand_meta: Optional[Dict[str, Any]],
) -> tuple[float, str]:
    if not isinstance(market_data, dict):
        raise RuntimeError("market_data must be dict for signal price resolution (STRICT)")

    market_features = market_data.get("market_features")
    search_spaces: list[tuple[str, Any]] = [
        ("market_data", market_data),
        ("market_data.market_features", market_features),
        ("cand.meta", cand_meta),
    ]

    for source_name, source_mapping in search_spaces:
        price, path = _extract_first_positive_price_from_mapping_optional(
            source_mapping,
            source_name=source_name,
        )
        if price is not None and path is not None:
            return float(price), str(path)

    raise RuntimeError(
        "entry price contract missing for Signal/ExecutionEngine "
        f"(required aliases={_SIGNAL_EXECUTION_PRICE_ALIASES}) (STRICT)"
    )


def _purge_signal_execution_price_aliases_inplace(meta: Dict[str, Any]) -> None:
    if not isinstance(meta, dict):
        raise RuntimeError("signal.meta must be dict for price alias purge (STRICT)")

    for alias in _SIGNAL_EXECUTION_PRICE_ALIASES:
        if alias in meta:
            del meta[alias]


def _wait_account_ws_ready_or_raise(timeout_sec: float) -> None:
    timeout_f = float(timeout_sec)
    if not math.isfinite(timeout_f) or timeout_f <= 0:
        raise RuntimeError("account ws ready timeout must be finite > 0 (STRICT)")

    deadline = time.time() + timeout_f
    while True:
        st = get_account_ws_status()
        if bool(st.get("running")) and bool(st.get("connected")) and bool(st.get("listen_key_active")):
            log("[ACCOUNT_WS] ready")
            return

        if time.time() >= deadline:
            raise RuntimeError(
                "account ws not ready within deadline (STRICT): "
                f"running={st.get('running')} connected={st.get('connected')} listen_key_active={st.get('listen_key_active')}"
            )
        time.sleep(0.5)


def _wait_market_ws_ready_or_raise(symbol: str, timeout_sec: float) -> None:
    timeout_f = float(timeout_sec)
    if not math.isfinite(timeout_f) or timeout_f <= 0:
        raise RuntimeError("market ws ready timeout must be finite > 0 (STRICT)")

    sym = str(symbol).replace("-", "").replace("/", "").upper().strip()
    if not sym:
        raise RuntimeError("symbol is empty for market ws ready wait (STRICT)")

    deadline = time.time() + timeout_f
    last_progress_log_ts = 0.0

    while True:
        snap = ws_get_health_snapshot(sym)
        if not isinstance(snap, dict):
            raise RuntimeError("ws_get_health_snapshot returned non-dict (STRICT)")

        missing: List[str] = []

        ws_status = snap.get("ws")
        if not isinstance(ws_status, dict):
            raise RuntimeError("market ws snapshot.ws must be dict (STRICT)")
        if not _require_bool(ws_status.get("ok"), "market_ws_snapshot.ws.ok"):
            reasons = ws_status.get("reasons")
            if isinstance(reasons, list) and reasons:
                missing.append(f"ws:{_join_reason_list_strict(reasons, 'market_ws_snapshot.ws.reasons')}")
            else:
                missing.append("ws:not_ok")

        kline_map = snap.get("klines")
        if not isinstance(kline_map, dict):
            raise RuntimeError("market ws snapshot.klines must be dict (STRICT)")

        for iv in ENTRY_REQUIRED_TFS:
            st = kline_map.get(iv)
            if not isinstance(st, dict):
                missing.append(f"kline:{iv}:missing_status")
                continue
            ok = st.get("ok")
            if not isinstance(ok, bool):
                raise RuntimeError(f"market ws snapshot.klines[{iv}].ok must be bool (STRICT)")
            if not ok:
                reasons = st.get("reasons")
                if isinstance(reasons, list) and reasons:
                    missing.append(f"kline:{iv}:{_join_reason_list_strict(reasons, f'kline[{iv}].reasons')}")
                else:
                    missing.append(f"kline:{iv}:not_ok")

        ob = snap.get("orderbook")
        if not isinstance(ob, dict):
            raise RuntimeError("market ws snapshot.orderbook must be dict (STRICT)")
        ob_ok = ob.get("ok")
        if not isinstance(ob_ok, bool):
            raise RuntimeError("market ws snapshot.orderbook.ok must be bool (STRICT)")
        if not ob_ok:
            reasons = ob.get("reasons")
            if isinstance(reasons, list) and reasons:
                missing.append(f"orderbook:{_join_reason_list_strict(reasons, 'orderbook.reasons')}")
            else:
                missing.append("orderbook:not_ok")

        if not missing:
            log(f"[BOOT] market ws ready: symbol={sym} requirements={ENTRY_REQUIRED_TFS}")
            return

        now = time.time()
        if now >= deadline:
            raise RuntimeError(
                f"market ws not ready within deadline (STRICT): symbol={sym} missing={missing}"
            )

        if (now - last_progress_log_ts) >= 5.0:
            last_progress_log_ts = now
            log(f"[BOOT] waiting market ws ready: symbol={sym} missing={missing}")

        interruptible_sleep(0.25, tick=0.25)


def _account_ws_connected_guard_or_raise() -> None:
    global _ACCOUNT_WS_CONSEC_FAILS, SAFE_STOP_REQUESTED

    st = get_account_ws_status()
    ok = bool(st.get("running")) and bool(st.get("connected")) and bool(st.get("listen_key_active"))

    if ok:
        if _ACCOUNT_WS_CONSEC_FAILS != 0:
            log(f"[ACCOUNT_WS][RECOVER] consecutive={_ACCOUNT_WS_CONSEC_FAILS} -> 0")
        _ACCOUNT_WS_CONSEC_FAILS = 0
        return

    _ACCOUNT_WS_CONSEC_FAILS += 1
    log(
        "[ACCOUNT_WS][FAIL] "
        f"running={st.get('running')} connected={st.get('connected')} "
        f"listen_key_active={st.get('listen_key_active')} "
        f"consecutive={_ACCOUNT_WS_CONSEC_FAILS}/{_ACCOUNT_WS_FAIL_HARDSTOP_N}"
    )

    if _ACCOUNT_WS_CONSEC_FAILS >= _ACCOUNT_WS_FAIL_HARDSTOP_N:
        SAFE_STOP_REQUESTED = True
        msg = (
            "[SAFE_STOP][ACCOUNT_WS] account websocket disconnected or invalid "
            f"consecutive={_ACCOUNT_WS_CONSEC_FAILS}"
        )
        log(msg)
        _maybe_send_error_tg("ACCOUNT_WS", msg, cooldown_sec=60)
        raise RuntimeError(msg)


def _normalize_position_side_guard(v: Any, name: str) -> str:
    s = str(v or "").upper().strip()
    if s not in ("BOTH", "LONG", "SHORT"):
        raise RuntimeError(f"{name} must be BOTH/LONG/SHORT (STRICT), got={s!r}")
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
    sym = str(symbol).replace("-", "").replace("/", "").upper().strip()
    rows = fetch_open_positions(sym)
    if not isinstance(rows, list):
        raise RuntimeError("fetch_open_positions returned non-list (STRICT)")

    for r in rows:
        if not isinstance(r, dict):
            raise RuntimeError("fetch_open_positions contains non-dict row (STRICT)")
        if str(r.get("symbol") or "").upper().strip() != sym:
            continue
        if "positionAmt" not in r:
            raise RuntimeError("positionRisk.positionAmt missing (STRICT)")
        try:
            amt = float(r["positionAmt"])
        except Exception as e:
            raise RuntimeError(f"positionAmt parse failed (STRICT): {e}") from e
        if abs(amt) > 1e-12:
            return True
    return False


def _is_rest_protection_order_row_strict(order: Dict[str, Any], *, symbol: str, expected_position_side: str) -> bool:
    if not isinstance(order, dict):
        raise RuntimeError("open order row must be dict (STRICT)")

    sym = str(symbol).replace("-", "").replace("/", "").upper().strip()
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
        raise RuntimeError("get_account_protection_orders_snapshot returned non-list (STRICT)")

    wanted = str(order_id).strip()
    if not wanted:
        raise RuntimeError("order_id is empty (STRICT)")

    for row in rows:
        if not isinstance(row, dict):
            raise RuntimeError("account protection order row must be dict (STRICT)")
        oid = str(row.get("order_id") or "").strip()
        if oid != wanted:
            continue

        row_ps = str(row.get("position_side") or "").upper().strip()
        if not row_ps:
            raise RuntimeError("account protection order missing position_side (STRICT)")
        if not _position_side_compatible(expected_position_side, row_ps):
            raise RuntimeError(
                f"account protection order positionSide mismatch (STRICT): order_id={wanted} row_ps={row_ps} expected={expected_position_side}"
            )
        return True

    return False


def _has_rest_protection_order_strict(symbol: str, order_id: str, expected_position_side: str) -> bool:
    sym = str(symbol).replace("-", "").replace("/", "").upper().strip()
    wanted = str(order_id).strip()
    if not wanted:
        raise RuntimeError("order_id is empty (STRICT)")

    orders = fetch_open_orders(sym)
    if not isinstance(orders, list):
        raise RuntimeError("fetch_open_orders returned non-list (STRICT)")

    for row in orders:
        if not isinstance(row, dict):
            raise RuntimeError("fetch_open_orders contains non-dict row (STRICT)")
        oid = row.get("orderId")
        if oid is None:
            raise RuntimeError("openOrders row missing orderId (STRICT)")
        if str(oid).strip() != wanted:
            continue

        if not _is_rest_protection_order_row_strict(row, symbol=sym, expected_position_side=expected_position_side):
            raise RuntimeError(
                f"open order exists but is not valid protection order (STRICT): order_id={wanted}"
            )
        return True

    return False


def _protection_orders_guard_or_raise() -> None:
    global SAFE_STOP_REQUESTED

    if not OPEN_TRADES:
        return

    if len(OPEN_TRADES) != 1:
        raise RuntimeError(f"OPEN_TRADES must be <= 1 in one-way mode (STRICT), got={len(OPEN_TRADES)}")

    trade = OPEN_TRADES[0]
    symbol = str(trade.symbol).replace("-", "").replace("/", "").upper().strip()
    if not symbol:
        raise RuntimeError("trade.symbol is empty (STRICT)")

    if not _exchange_position_is_open_strict(symbol):
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
        SAFE_STOP_REQUESTED = True
        msg = f"[SAFE_STOP][PROTECTION_MISSING] trade missing protection ids: {missing_fields} symbol={symbol}"
        log(msg)
        _maybe_send_error_tg("PROTECTION_MISSING", msg, cooldown_sec=60)
        if bool(SET.force_close_on_desync):
            try:
                n = close_all_positions_market(symbol)
                log(f"[PROTECTION_MISSING] force close submitted count={n}")
                _safe_send_tg(f"🧯 보호주문 누락 강제정리 제출: {symbol} count={n}")
            except Exception as e:
                log(f"[PROTECTION_MISSING] force close failed: {type(e).__name__}: {e}")
                _safe_send_tg(f"❌ 보호주문 누락 강제정리 실패: {e}")
                raise RuntimeError(f"protection order force close failed: {type(e).__name__}: {e}") from e
        raise RuntimeError(msg)

    tp_ok = _has_account_ws_protection_order_strict(symbol, tp_order_id, expected_position_side)
    sl_ok = _has_account_ws_protection_order_strict(symbol, sl_order_id, expected_position_side)

    if not tp_ok:
        tp_ok = _has_rest_protection_order_strict(symbol, tp_order_id, expected_position_side)
    if not sl_ok:
        sl_ok = _has_rest_protection_order_strict(symbol, sl_order_id, expected_position_side)

    if tp_ok and sl_ok:
        return

    SAFE_STOP_REQUESTED = True
    missing_runtime: List[str] = []
    if not tp_ok:
        missing_runtime.append(f"TP({tp_order_id})")
    if not sl_ok:
        missing_runtime.append(f"SL({sl_order_id})")

    msg = f"[SAFE_STOP][PROTECTION_ORDERS_MISSING] symbol={symbol} missing={missing_runtime}"
    log(msg)
    _maybe_send_error_tg("PROTECTION_ORDERS_MISSING", msg, cooldown_sec=60)

    if bool(SET.force_close_on_desync):
        try:
            n = close_all_positions_market(symbol)
            log(f"[PROTECTION_ORDERS_MISSING] force close submitted count={n}")
            _safe_send_tg(f"🧯 보호주문 미존재 강제정리 제출: {symbol} count={n}")
        except Exception as e:
            log(f"[PROTECTION_ORDERS_MISSING] force close failed: {type(e).__name__}: {e}")
            _safe_send_tg(f"❌ 보호주문 미존재 강제정리 실패: {e}")
            raise RuntimeError(f"protection order force close failed: {type(e).__name__}: {e}") from e

    raise RuntimeError(msg)


def _ws_liveness_guard_or_raise(symbol: str, now_ts: float) -> None:
    """
    STRICT:
    - transport failure만 치명적으로 본다.
    - market_feed warning은 관측 정보이며 SAFE_STOP 사유가 아니다.
    """
    global _WS_LIVENESS_CONSEC_FAILS, SAFE_STOP_REQUESTED

    _ = now_ts
    ws_status = ws_get_ws_status(symbol)
    if not isinstance(ws_status, dict):
        raise RuntimeError("ws_get_ws_status returned non-dict (STRICT)")

    transport_ok = ws_status.get("transport_ok")
    market_feed_ok = ws_status.get("market_feed_ok")
    if not isinstance(transport_ok, bool):
        raise RuntimeError("ws_status.transport_ok must be bool (STRICT)")
    if not isinstance(market_feed_ok, bool):
        raise RuntimeError("ws_status.market_feed_ok must be bool (STRICT)")

    if transport_ok:
        if _WS_LIVENESS_CONSEC_FAILS != 0:
            log(f"[WS_LIVENESS][RECOVER] consecutive={_WS_LIVENESS_CONSEC_FAILS} -> 0")
        _WS_LIVENESS_CONSEC_FAILS = 0

        if not market_feed_ok:
            warnings = ws_status.get("warnings")
            if not isinstance(warnings, list):
                raise RuntimeError("ws_status.warnings must be list (STRICT)")
            joined = _join_reason_list_strict(warnings, "ws_status.warnings")
            if joined:
                log(f"[WS_LIVENESS][WARN] {joined}")
        return

    reasons = ws_status.get("reasons")
    if not isinstance(reasons, list):
        raise RuntimeError("ws_status.reasons must be list (STRICT)")
    joined = _join_reason_list_strict(reasons, "ws_status.reasons")

    _WS_LIVENESS_CONSEC_FAILS += 1
    log(
        f"[WS_LIVENESS][FAIL] {joined} "
        f"consecutive={_WS_LIVENESS_CONSEC_FAILS}/{_WS_LIVENESS_FAIL_HARDSTOP_N}"
    )

    if _WS_LIVENESS_CONSEC_FAILS >= _WS_LIVENESS_FAIL_HARDSTOP_N:
        SAFE_STOP_REQUESTED = True
        msg = (
            "[SAFE_STOP][WS_LIVENESS] transport failure confirmed "
            f"consecutive={_WS_LIVENESS_CONSEC_FAILS} reasons={joined}"
        )
        log(msg)
        _maybe_send_error_tg("WS_LIVENESS", msg, cooldown_sec=60)
        raise RuntimeError(msg)


def _get_equity_current_usdt_strict() -> float:
    if bool(SET.test_dry_run):
        fake = float(SET.test_fake_available_usdt)
        if not math.isfinite(fake) or fake <= 0.0:
            raise RuntimeError("test_fake_available_usdt invalid (STRICT)")
        return float(fake)

    row = get_balance_detail("USDT")
    if not isinstance(row, dict):
        raise RuntimeError("get_balance_detail('USDT') returned non-dict")

    def _must_float(key: str) -> float:
        if key not in row:
            raise RuntimeError(f"balance detail missing key: {key}")
        try:
            v = float(row[key])
        except Exception as e:
            raise RuntimeError(f"balance detail parse failed: {key} ({e})") from e
        if not math.isfinite(v):
            raise RuntimeError(f"balance detail not finite: {key}={v}")
        return v

    available = _must_float("availableBalance")
    cross_unpnl = _must_float("crossUnPnl")

    wallet = None
    if "balance" in row:
        wallet = _must_float("balance")
    elif "walletBalance" in row:
        wallet = _must_float("walletBalance")

    cross_wallet = None
    if "crossWalletBalance" in row:
        cross_wallet = _must_float("crossWalletBalance")

    if wallet is not None:
        eq = wallet + cross_unpnl
        if eq > 0:
            return float(eq)

    if cross_wallet is not None:
        eq = cross_wallet + cross_unpnl
        if eq > 0:
            return float(eq)

    if available > 0:
        return float(available)

    raise RuntimeError("equity_current_usdt invalid: 0.0")


def _get_equity_current_usdt_cached_strict(now_ts: float) -> float:
    global _EQUITY_CACHE_VALUE, _EQUITY_CACHE_TS

    now_f = float(now_ts)
    if not math.isfinite(now_f) or now_f <= 0.0:
        raise RuntimeError("now_ts must be finite > 0 (STRICT)")

    cached_value = _EQUITY_CACHE_VALUE
    cached_ts = _EQUITY_CACHE_TS
    if cached_value is not None:
        age = now_f - float(cached_ts)
        if age < 0:
            raise RuntimeError(f"equity cache ts rollback detected (STRICT): cache_ts={cached_ts} now_ts={now_f}")
        if age <= ENGINE_EQUITY_CACHE_TTL_SEC:
            return float(cached_value)

    fresh = _get_equity_current_usdt_strict()
    _EQUITY_CACHE_VALUE = float(fresh)
    _EQUITY_CACHE_TS = now_f
    return float(fresh)


def _load_equity_peak_bootstrap(symbol: str) -> Optional[float]:
    sym = str(symbol).replace("-", "").replace("/", "").upper().strip()
    if not sym:
        raise RuntimeError("symbol is empty (STRICT)")

    q = text(
        """
        SELECT MAX(equity_peak_usdt)
        FROM bt_trade_snapshots
        WHERE symbol = :symbol
          AND equity_peak_usdt IS NOT NULL
        """
    )

    with get_session() as session:
        v = session.execute(q, {"symbol": sym}).scalar()

    if v is None:
        return None

    peak = float(v)
    if not math.isfinite(peak) or peak <= 0:
        raise RuntimeError(f"persisted equity_peak_usdt invalid (STRICT): {peak}")
    return float(peak)


def _load_closed_trades_bootstrap(symbol: str, limit: int = 50) -> list[dict[str, Any]]:
    sym = str(symbol).replace("-", "").replace("/", "").upper().strip()
    if not sym:
        raise RuntimeError("symbol is empty (STRICT)")

    lim = int(limit)
    if lim <= 0:
        raise RuntimeError("limit must be > 0 (STRICT)")

    q = text(
        """
        SELECT id, exit_ts, pnl_usdt, tp_pct, sl_pct
        FROM bt_trades
        WHERE symbol = :symbol
          AND exit_ts IS NOT NULL
          AND pnl_usdt IS NOT NULL
        ORDER BY exit_ts DESC
        LIMIT :limit
        """
    )

    rows: list[dict[str, Any]] = []
    with get_session() as session:
        result = session.execute(q, {"symbol": sym, "limit": lim}).fetchall()

    for (trade_id, exit_ts, pnl_usdt, tp_pct, sl_pct) in result:
        if not isinstance(trade_id, int) and not (isinstance(trade_id, (str, float)) and str(trade_id).strip()):
            raise RuntimeError("trade_id invalid (STRICT)")

        if not isinstance(exit_ts, datetime.datetime):
            raise RuntimeError(f"exit_ts must be datetime (STRICT), got={type(exit_ts).__name__}")
        if exit_ts.tzinfo is None or exit_ts.tzinfo.utcoffset(exit_ts) is None:
            raise RuntimeError("exit_ts must be tz-aware (STRICT)")

        pnl_f = float(pnl_usdt)
        if not math.isfinite(pnl_f):
            raise RuntimeError("pnl_usdt must be finite (STRICT)")

        rows.append(
            {
                "id": int(trade_id),
                "exit_ts": exit_ts,
                "pnl_usdt": pnl_f,
                "tp_pct": None if tp_pct is None else float(tp_pct),
                "sl_pct": None if sl_pct is None else float(sl_pct),
            }
        )
    return rows


def _backfill_ws_kline_history(symbol: str) -> None:
    intervals = _parse_tfs(SET.ws_backfill_tfs)
    _verify_required_tfs_or_die("ws_backfill_tfs", intervals, ENTRY_REQUIRED_TFS)

    configured_limit = int(SET.ws_backfill_limit)

    for iv in intervals:
        min_needed = ENTRY_REQUIRED_KLINES_MIN.get(iv, 1)
        fetch_limit = max(configured_limit, min_needed)
        log(
            f"[BOOT] REST backfill start: symbol={symbol} interval={iv} "
            f"configured_limit={configured_limit} fetch_limit={fetch_limit} need={min_needed}"
        )

        try:
            rest_rows = fetch_klines_rest(symbol, iv, limit=fetch_limit)
        except KlineRestError as e:
            raise RuntimeError(f"REST backfill failed: symbol={symbol} interval={iv} err={e}") from e

        if not rest_rows:
            raise RuntimeError(f"REST backfill returned 0 rows: symbol={symbol} interval={iv}")

        backfill_klines_from_rest(symbol, iv, rest_rows)

        buf = ws_get_klines_with_volume(symbol, iv, limit=max(300, min_needed))
        if not isinstance(buf, list) or len(buf) < min_needed:
            raise RuntimeError(
                f"WS buffer verify failed: {symbol} {iv} need={min_needed} got={len(buf) if isinstance(buf, list) else 'N/A'}"
            )


def _coerce_ws_store_candle_row_strict(row: Any, *, interval: str, idx: int) -> WsStoreCandleRow:
    if not isinstance(row, (list, tuple)):
        raise RuntimeError(
            f"[MD-STORE] ws candle row must be list/tuple (STRICT): interval={interval} idx={idx} type={type(row).__name__}"
        )
    if len(row) != 7:
        raise RuntimeError(
            f"[MD-STORE] ws candle row must be 7-tuple(ts,o,h,l,c,v,is_closed) (STRICT): interval={interval} idx={idx} len={len(row)}"
        )

    ts_ms = _require_int_ms(row[0], f"md_store.ws[{interval}][{idx}].ts_ms")
    o = _as_float(row[1], f"md_store.ws[{interval}][{idx}].open", min_value=0.0)
    h = _as_float(row[2], f"md_store.ws[{interval}][{idx}].high", min_value=0.0)
    l = _as_float(row[3], f"md_store.ws[{interval}][{idx}].low", min_value=0.0)
    c = _as_float(row[4], f"md_store.ws[{interval}][{idx}].close", min_value=0.0)
    v = _as_float(row[5], f"md_store.ws[{interval}][{idx}].volume", min_value=0.0)
    is_closed = _require_bool(row[6], f"md_store.ws[{interval}][{idx}].is_closed")

    if o <= 0.0 or h <= 0.0 or l <= 0.0 or c <= 0.0:
        raise RuntimeError(f"[MD-STORE] ws candle OHLC must be > 0 (STRICT): interval={interval} idx={idx}")
    if h < l:
        raise RuntimeError(f"[MD-STORE] ws candle high < low (STRICT): interval={interval} idx={idx}")
    if h < o or h < c:
        raise RuntimeError(f"[MD-STORE] ws candle high < open/close (STRICT): interval={interval} idx={idx}")
    if l > o or l > c:
        raise RuntimeError(f"[MD-STORE] ws candle low > open/close (STRICT): interval={interval} idx={idx}")

    return (int(ts_ms), float(o), float(h), float(l), float(c), float(v), bool(is_closed))


def _legacy_kline_rows_for_validator(rows: List[WsStoreCandleRow]) -> List[LegacyStoreCandleRow]:
    return [(ts, o, h, l, c, v) for (ts, o, h, l, c, v, _is_closed) in rows]


def _select_ws_rows_to_persist_strict(
    rows_raw: List[Any],
    *,
    interval: str,
    last_persisted_row: Optional[WsStoreCandleRow],
) -> List[WsStoreCandleRow]:
    typed_rows: List[WsStoreCandleRow] = [
        _coerce_ws_store_candle_row_strict(row, interval=interval, idx=idx)
        for idx, row in enumerate(rows_raw)
    ]
    if not typed_rows:
        return []

    for idx in range(1, len(typed_rows)):
        prev_ts = int(typed_rows[idx - 1][0])
        curr_ts = int(typed_rows[idx][0])
        if curr_ts <= prev_ts:
            raise RuntimeError(
                f"[MD-STORE] ws candle buffer must be strictly increasing by ts (STRICT): "
                f"interval={interval} prev_ts={prev_ts} curr_ts={curr_ts} idx={idx}"
            )

    if last_persisted_row is None:
        return typed_rows

    last_ts = int(last_persisted_row[0])
    selected: List[WsStoreCandleRow] = [row for row in typed_rows if int(row[0]) > last_ts]

    latest_row = typed_rows[-1]
    if int(latest_row[0]) == last_ts and latest_row != last_persisted_row:
        if selected and int(selected[-1][0]) == int(latest_row[0]):
            selected[-1] = latest_row
        else:
            selected.append(latest_row)

    return selected


def _start_market_data_store_thread() -> None:
    symbol = SET.symbol
    flush_sec = float(SET.md_store_flush_sec)
    ob_interval_sec = float(SET.ob_store_interval_sec)
    store_tfs = _parse_tfs(SET.md_store_tfs)

    last_persisted_candle_row: Dict[str, Optional[WsStoreCandleRow]] = {iv: None for iv in store_tfs}
    last_ob_ts: float = 0.0
    last_ob_missing_log_ts: float = 0.0

    def _loop() -> None:
        nonlocal last_ob_ts, last_ob_missing_log_ts
        global SAFE_STOP_REQUESTED

        try:
            log(f"[MD-STORE] loop started: flush_sec={flush_sec}, ob_interval_sec={ob_interval_sec}, store_tfs={store_tfs}")
            while RUNNING:
                now = time.time()

                candles_to_save: List[Dict[str, Any]] = []
                latest_persisted_candidate_by_iv: Dict[str, WsStoreCandleRow] = {}

                for iv in store_tfs:
                    buf = ws_get_klines_with_volume_and_closed(symbol, iv, limit=500)
                    if buf is None:
                        raise RuntimeError(
                            f"[MD-STORE] ws_get_klines_with_volume_and_closed returned None (STRICT) interval={iv}"
                        )
                    if not isinstance(buf, list):
                        raise RuntimeError(
                            f"[MD-STORE] kline buffer invalid type (STRICT) interval={iv} type={type(buf).__name__}"
                        )
                    if not buf:
                        continue

                    new_rows = _select_ws_rows_to_persist_strict(
                        buf,
                        interval=iv,
                        last_persisted_row=last_persisted_candle_row.get(iv),
                    )
                    if not new_rows:
                        continue

                    try:
                        validate_kline_series_strict(
                            _legacy_kline_rows_for_validator(new_rows),
                            name=f"md_store.ws_kline[{iv}]",
                            min_len=1,
                        )
                    except DataIntegrityError as e:
                        raise RuntimeError(f"[MD-STORE] kline integrity fail (STRICT): {e}") from e

                    for ts_ms, o, h, l, c, v, is_closed in new_rows:
                        candles_to_save.append(
                            {
                                "symbol": symbol,
                                "interval": iv,
                                "ts_ms": int(ts_ms),
                                "open": float(o),
                                "high": float(h),
                                "low": float(l),
                                "close": float(c),
                                "volume": float(v),
                                "quote_volume": None,
                                "source": "ws",
                                "is_closed": bool(is_closed),
                            }
                        )

                    latest_persisted_candidate_by_iv[iv] = new_rows[-1]

                if candles_to_save:
                    save_candles_bulk_from_ws(candles_to_save)
                    for iv, latest_row in latest_persisted_candidate_by_iv.items():
                        last_persisted_candle_row[iv] = latest_row

                if now - last_ob_ts >= ob_interval_sec:
                    ob = ws_get_orderbook(symbol, limit=5)
                    if not isinstance(ob, dict) or not ob:
                        if now - last_ob_missing_log_ts >= 60:
                            last_ob_missing_log_ts = now
                            log(f"[MD-STORE][WARN] orderbook missing: symbol={symbol}")
                        time.sleep(flush_sec)
                        continue

                    if not ob.get("bids") or not ob.get("asks"):
                        if now - last_ob_missing_log_ts >= 60:
                            last_ob_missing_log_ts = now
                            log(f"[MD-STORE][WARN] orderbook empty bids/asks: symbol={symbol}")
                        time.sleep(flush_sec)
                        continue

                    if "exchTs" in ob and ob.get("exchTs") is not None:
                        ts_ms = _require_int_ms(ob.get("exchTs"), "orderbook.exchTs")
                    elif "ts" in ob and ob.get("ts") is not None:
                        ts_ms = _require_int_ms(ob.get("ts"), "orderbook.ts")
                    else:
                        raise RuntimeError("[MD-STORE] orderbook missing exchTs/ts (STRICT)")

                    try:
                        validate_orderbook_strict(ob, symbol=str(symbol), require_ts=True)
                    except DataIntegrityError as e:
                        raise RuntimeError(f"[MD-STORE] orderbook integrity fail (STRICT): {e}") from e

                    save_orderbook_from_ws(symbol=symbol, ts_ms=int(ts_ms), bids=ob["bids"], asks=ob["asks"])
                    last_ob_ts = now

                time.sleep(flush_sec)

        except Exception as e:
            SAFE_STOP_REQUESTED = True
            msg = f"⛔ [MD-STORE][FATAL] {type(e).__name__}: {e}"
            log(msg)
            _safe_send_tg(msg)
            raise

    threading.Thread(target=_loop, name="md-store-loop", daemon=True).start()
    log("[MD-STORE] background store thread started")


def _fetch_exchange_position_snapshot(symbol: str) -> Dict[str, Any]:
    sym = str(symbol).replace("-", "").replace("/", "").upper().strip()
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
        amt = _as_float(r.get("positionAmt"), f"positionRisk.positionAmt")
        if abs(amt) > 1e-12:
            live.append(r)

    if not live:
        return {"symbol": sym, "positionAmt": "0", "entryPrice": "0"}

    dirs: set[str] = set()
    pos_sides: set[str] = set()
    total_signed_amt = 0.0
    weighted_entry_sum = 0.0

    for idx, r in enumerate(live):
        amt = _as_float(r.get("positionAmt"), f"positionRisk[{idx}].positionAmt")
        entry = _as_float(r.get("entryPrice"), f"positionRisk[{idx}].entryPrice", min_value=0.0)
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
    sym = str(symbol).replace("-", "").replace("/", "").upper().strip()
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

    qty = float(getattr(t, "remaining_qty", None))
    if qty <= 0 or not math.isfinite(qty):
        raise RuntimeError("trade.remaining_qty must be finite > 0 (STRICT)")

    ep = float(getattr(t, "entry_price", None))
    if ep <= 0 or not math.isfinite(ep):
        raise RuntimeError("trade.entry_price must be finite > 0 (STRICT)")

    return {"symbol": sym, "position_amt": str(sign * qty), "entry_price": str(ep)}


def _fetch_exchange_open_orders_snapshot(symbol: str) -> List[Dict[str, Any]]:
    sym = str(symbol).replace("-", "").replace("/", "").upper().strip()
    orders = fetch_open_orders(sym)
    if not isinstance(orders, list):
        raise RuntimeError("fetch_open_orders returned non-list (STRICT)")
    return orders


def _resolve_closed_trade_pnl_total_strict(trade: Trade, summary: Dict[str, Any]) -> float:
    if not isinstance(summary, dict):
        raise RuntimeError("closed summary must be dict (STRICT)")
    summary_pnl = summary.get("pnl")
    if summary_pnl is None:
        raise RuntimeError("close summary missing pnl (STRICT)")
    summary_pnl_f = _as_float(summary_pnl, "summary.pnl")

    trade_realized = getattr(trade, "realized_pnl_usdt", None)
    if trade_realized is None:
        return float(summary_pnl_f)

    trade_realized_f = _as_float(trade_realized, "trade.realized_pnl_usdt")
    return float(trade_realized_f)


def _resolve_closed_trade_exit_ts_dt_strict(trade: Trade, summary: Dict[str, Any]) -> datetime.datetime:
    if not isinstance(summary, dict):
        raise RuntimeError("closed summary must be dict (STRICT)")

    close_ts = getattr(trade, "close_ts", None)
    if close_ts is not None:
        close_ts_f = _as_float(close_ts, "trade.close_ts", min_value=0.0)
        if close_ts_f > 0:
            dt = datetime.datetime.fromtimestamp(close_ts_f, tz=datetime.timezone.utc)
            if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
                raise RuntimeError("resolved close datetime must be tz-aware (STRICT)")
            return dt

    close_time_ms = summary.get("close_time")
    if close_time_ms is None:
        raise RuntimeError("close summary missing close_time (STRICT)")
    close_time_ms_i = _require_int_ms(close_time_ms, "summary.close_time")
    dt2 = datetime.datetime.fromtimestamp(close_time_ms_i / 1000.0, tz=datetime.timezone.utc)
    if dt2.tzinfo is None or dt2.tzinfo.utcoffset(dt2) is None:
        raise RuntimeError("resolved close datetime from close_time must be tz-aware (STRICT)")
    return dt2


def _on_reconcile_desync(result: ReconcileResult) -> None:
    global SAFE_STOP_REQUESTED
    SAFE_STOP_REQUESTED = True

    if not bool(getattr(result, "desync_confirmed", True)):
        raise RuntimeError("on_desync called but desync_confirmed is False (STRICT)")

    msg = f"⛔ DESYNC 확정: {result.symbol} issues={len(result.issues)} (신규 진입 차단, 종료)"
    log(msg)
    for it in result.issues:
        log(f"[DESYNC] {it.code} | {it.message} | {it.details}")
    _maybe_send_error_tg("DESYNC_CONFIRMED", msg, cooldown_sec=60)

    if bool(SET.force_close_on_desync):
        try:
            n = close_all_positions_market(result.symbol)
            log(f"[DESYNC] force close submitted count={n}")
            _safe_send_tg(f"🧯 DESYNC 강제정리 제출: {result.symbol} count={n}")
        except Exception as e:
            log(f"[DESYNC] force close failed: {type(e).__name__}: {e}")
            _safe_send_tg(f"❌ DESYNC 강제정리 실패: {e}")
            raise RuntimeError(f"DESYNC force close failed: {type(e).__name__}: {e}") from e

    raise RuntimeError("DESYNC confirmed (STRICT): engine must stop")


def _sigterm(*_: Any) -> None:
    global SAFE_STOP_REQUESTED, SIGTERM_REQUESTED_AT, SIGTERM_DEADLINE_TS, _SIGTERM_NOTICE_SENT
    SAFE_STOP_REQUESTED = True
    now = time.time()

    if SIGTERM_REQUESTED_AT is None:
        SIGTERM_REQUESTED_AT = now
        grace = float(SET.sigterm_grace_sec)
        if grace <= 0:
            raise RuntimeError("settings.sigterm_grace_sec must be > 0 (STRICT)")
        SIGTERM_DEADLINE_TS = now + grace

        msg = f"🧯 SIGTERM 수신: 신규 진입 중단, 포지션 정리 후 종료 시도 (grace={int(grace)}s)"
        log(msg)
        if not _SIGTERM_NOTICE_SENT:
            _SIGTERM_NOTICE_SENT = True
            _safe_send_tg(msg)


signal.signal(signal.SIGTERM, _sigterm)


def _on_safe_stop() -> None:
    global SAFE_STOP_REQUESTED
    SAFE_STOP_REQUESTED = True
    _safe_send_tg("🛑 텔레그램 '종료' 요청: 포지션 정리 후 종료합니다.")


def _build_entry_market_data_stage_or_raise(
    settings: Any,
    last_close_ts: float,
    *,
    notify_entry_block_fn: Callable[[str, str, int], None],
    log_fn: Callable[[str], None],
) -> Optional[Dict[str, Any]]:
    try:
        return _build_entry_market_data(
            settings,
            last_close_ts,
            notify_entry_block_fn=notify_entry_block_fn,
            log_fn=log_fn,
        )
    except Exception as e:
        raise RuntimeError(
            f"entry_pipeline market_data build failed (STRICT): {type(e).__name__}: {e}"
        ) from e


def main() -> None:
    global RUNNING, OPEN_TRADES, LAST_CLOSE_TS, CONSEC_LOSSES, SAFE_STOP_REQUESTED, LAST_EXCHANGE_SYNC_TS
    global LAST_EXIT_CANDLE_TS_1M, LAST_ENTRY_GPT_CALL_TS, _SIGTERM_DEADLINE_HANDLED
    global _BALANCE_CONSEC_FAILS, _EQUITY_CONSEC_FAILS, LAST_ENTRY_EVAL_SIGNAL_TS_MS

    _verify_runtime_settings_contract_or_raise()

    start_async_worker(
        num_threads=int(SET.async_worker_threads),
        max_queue_size=int(SET.async_worker_queue_size),
        thread_name_prefix="async-io",
    )

    _verify_ws_boot_configuration_or_die()
    _bootstrap_entry_signal_gate_or_raise(SET.symbol)

    if bool(SET.ws_enabled):
        _backfill_ws_kline_history(SET.symbol)
        start_ws_loop(SET.symbol)
        _start_market_data_store_thread()

        market_ws_ready_timeout_sec = max(
            30.0,
            float(SET.ws_max_kline_delay_sec),
            float(SET.ws_market_event_max_delay_sec),
        )
        _wait_market_ws_ready_or_raise(SET.symbol, market_ws_ready_timeout_sec)

        start_health_monitor()

        last_1m = ws_get_klines_with_volume(SET.symbol, "1m", limit=1)
        if last_1m:
            LAST_EXIT_CANDLE_TS_1M = int(last_1m[0][0])

    if not SET.api_key or not SET.api_secret:
        msg = "❗ API 자격정보가 설정되어 있지 않습니다. 설정 후 재시작해 주세요."
        log(msg)
        _safe_send_tg(msg)
        return

    try:
        set_leverage_and_mode(SET.symbol, int(SET.leverage), bool(SET.isolated))
    except Exception as e:
        allow = bool(SET.allow_start_without_leverage_setup)
        msg = f"❗ 레버리지/마진 설정 실패: {e}"
        log(msg)
        if allow:
            _safe_send_tg(msg + "\n⚠ allow_start_without_leverage_setup=True 이므로 계속 진행합니다.")
        else:
            _safe_send_tg(msg + "\n⛔ 기본 정책에 따라 중단합니다.")
            return

    try:
        start_account_ws_loop()
        _wait_account_ws_ready_or_raise(_ACCOUNT_WS_READY_TIMEOUT_SEC)
    except Exception as e:
        msg = f"❗ Account WS 시작 실패: {type(e).__name__}: {e}"
        log(msg)
        _safe_send_tg(msg)
        return

    _safe_send_tg("✅ Binance USDT-M Futures 자동매매(WS 버전)를 시작합니다.")

    start_telegram_command_thread(on_stop_command=_on_safe_stop)
    start_signal_analysis_thread(interval_sec=SIGNAL_ANALYSIS_INTERVAL_SEC)

    log(
        "[BOOT] embedded market_researcher autostart disabled in run_bot_ws "
        "(STRICT separation: trading engine does not implicitly launch external analysis worker)"
    )

    OPEN_TRADES, _ = sync_open_trades_from_exchange(SET.symbol, replace=True, current_trades=OPEN_TRADES)
    _invalidate_equity_cache()
    LAST_EXCHANGE_SYNC_TS = time.time()

    confirm_n_i = int(SET.reconcile_confirm_n)
    if confirm_n_i < 1:
        raise RuntimeError("settings.reconcile_confirm_n must be >= 1 (STRICT)")

    hard_consec_limit_i = int(SET.hard_consecutive_losses_limit)
    if hard_consec_limit_i < 0:
        raise RuntimeError("settings.hard_consecutive_losses_limit must be >= 0 (STRICT)")

    reconcile_engine = ReconcileEngine(
        ReconcileConfig(
            symbol=str(SET.symbol),
            interval_sec=int(SET.reconcile_interval_sec),
            desync_confirm_n=int(confirm_n_i),
        ),
        fetch_exchange_position=_fetch_exchange_position_snapshot,
        get_local_position=_get_local_position_snapshot,
        fetch_exchange_open_orders=_fetch_exchange_open_orders_snapshot,
        on_desync=_on_reconcile_desync,
    )

    entry_exec_engine = ExecutionEngine(SET)
    regime_engine = RegimeEngine(window_size=200, percentile_min_history=60)
    ev_heatmap = EvHeatmapEngine(window_size=50, min_samples=20)

    drift_detector = DriftDetector(
        DriftDetectorConfig(
            allocation_abs_jump=float(SET.drift_allocation_abs_jump),
            allocation_spike_ratio=float(SET.drift_allocation_spike_ratio),
            multiplier_abs_jump=float(SET.drift_multiplier_abs_jump),
            micro_abs_jump=float(SET.drift_micro_abs_jump),
            stable_regime_steps=int(SET.drift_stable_regime_steps),
        )
    )

    persisted_peak = _load_equity_peak_bootstrap(SET.symbol)
    if persisted_peak is not None:
        log(f"[BOOT] persisted equity_peak_usdt loaded: {persisted_peak:.4f}")

    account_builder = AccountStateBuilder(
        win_rate_window=20,
        min_trades_for_win_rate=5,
        initial_equity_peak_usdt=persisted_peak,
    )

    risk_policy = RiskPhysicsPolicy(max_allocation=1.0)
    risk_physics = RiskPhysicsEngine(policy=risk_policy)

    CLOSED_TRADES_CACHE = deque(maxlen=50)
    boot_rows = _load_closed_trades_bootstrap(SET.symbol, limit=50)
    for r in reversed(boot_rows):
        CLOSED_TRADES_CACHE.appendleft(r)

    position_resync_sec = float(SET.position_resync_sec)
    last_fill_check: float = 0.0
    last_balance_log: float = 0.0
    last_protection_guard_ts: float = 0.0
    last_entry_cycle_wall_ts: float = 0.0
    last_entry_cycle_1m_ts: Optional[int] = None
    last_entry_cycle_orderbook_ts: Optional[int] = None

    max_signal_latency_ms = float(SET.max_signal_latency_ms)
    max_exec_latency_ms = float(SET.max_exec_latency_ms)

    while RUNNING:
        try:
            now = time.time()

            if bool(SET.ws_enabled):
                _ws_liveness_guard_or_raise(SET.symbol, now)

            _account_ws_connected_guard_or_raise()

            if OPEN_TRADES and (now - last_protection_guard_ts) >= ENGINE_PROTECTION_GUARD_INTERVAL_SEC:
                _protection_orders_guard_or_raise()
                last_protection_guard_ts = now

            if SIGTERM_DEADLINE_TS is not None and now >= SIGTERM_DEADLINE_TS and not _SIGTERM_DEADLINE_HANDLED:
                _SIGTERM_DEADLINE_HANDLED = True
                log("[SIGTERM] grace deadline reached. force close attempt starts.")
                _safe_send_tg("🧯 SIGTERM grace 경과: 강제 정리 시도합니다.")
                try:
                    n = close_all_positions_market(SET.symbol)
                    log(f"[SIGTERM] force close submitted count={n}")
                    _safe_send_tg(f"🧯 SIGTERM 강제 정리 제출: count={n}")
                except Exception as e:
                    log(f"[SIGTERM] force close failed: {type(e).__name__}: {e}")
                    _safe_send_tg(f"❌ SIGTERM 강제 정리 실패: {e}")
                    SAFE_STOP_REQUESTED = True
                    raise

            reconcile_engine.run_if_due(now_ts=now)

            if now - LAST_EXCHANGE_SYNC_TS >= position_resync_sec:
                OPEN_TRADES, _ = sync_open_trades_from_exchange(SET.symbol, replace=True, current_trades=OPEN_TRADES)
                _invalidate_equity_cache()
                LAST_EXCHANGE_SYNC_TS = now

            if now - last_balance_log >= 60:
                try:
                    get_available_usdt()
                    _BALANCE_CONSEC_FAILS = 0
                except Exception as e:
                    _BALANCE_CONSEC_FAILS += 1
                    msg = f"[WARN][BALANCE_FAIL] {type(e).__name__}: {e} consecutive={_BALANCE_CONSEC_FAILS}/{_BALANCE_FAIL_HARDSTOP_N}"
                    log(msg)
                    _maybe_send_error_tg("BALANCE_FAIL", msg, cooldown_sec=60)
                    if _BALANCE_CONSEC_FAILS >= _BALANCE_FAIL_HARDSTOP_N:
                        SAFE_STOP_REQUESTED = True
                        raise RuntimeError("balance check failed consecutively (STRICT)") from e
                finally:
                    last_balance_log = now

            if now - last_fill_check >= float(SET.poll_fills_sec):
                OPEN_TRADES, closed_list = check_closes(OPEN_TRADES, TRADER_STATE)
                if closed_list:
                    _invalidate_equity_cache()
                for closed in closed_list:
                    t: Trade = closed["trade"]
                    reason: str = closed["reason"]
                    summary: Any = closed.get("summary")

                    if not isinstance(summary, dict) or not summary:
                        reason2, summary2 = build_close_summary_strict(t)
                        if not isinstance(summary2, dict) or not summary2:
                            raise RuntimeError("close summary requery returned invalid (STRICT)")
                        reason = str(reason2 or reason)
                        summary = summary2

                    if "pnl" not in summary:
                        raise RuntimeError("close summary missing pnl (STRICT)")

                    if "avg_price" not in summary or summary["avg_price"] is None:
                        raise RuntimeError("close summary missing avg_price (STRICT)")
                    if "qty" not in summary or summary["qty"] is None:
                        raise RuntimeError("close summary missing qty (STRICT)")

                    pnl_total = _resolve_closed_trade_pnl_total_strict(t, summary)
                    avg_price = float(summary["avg_price"])
                    qty = float(summary["qty"])

                    LAST_CLOSE_TS = now
                    CONSEC_LOSSES = (CONSEC_LOSSES + 1) if pnl_total < 0 else 0

                    trade_id = getattr(t, "id", None)
                    if not isinstance(trade_id, int) or trade_id <= 0:
                        raise RuntimeError("closed trade missing valid trade.id (DB record mismatch)")

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

                    if hard_consec_limit_i > 0 and CONSEC_LOSSES >= hard_consec_limit_i:
                        SAFE_STOP_REQUESTED = True
                        msg = (
                            "🛑 [SAFE_STOP][CONSEC_LOSS] 연속 손실 한도 도달: "
                            f"consecutive_losses={CONSEC_LOSSES} limit={hard_consec_limit_i} "
                            "(신규 진입 중단, 엔진 종료)"
                        )
                        log(msg)
                        _maybe_send_error_tg("CONSEC_LOSS_HARDSTOP", msg, cooldown_sec=60)
                        return

                last_fill_check = now

            if OPEN_TRADES:
                last_1m = ws_get_klines_with_volume(SET.symbol, "1m", limit=1)
                if last_1m:
                    last_ts_ms = int(last_1m[0][0])
                    if LAST_EXIT_CANDLE_TS_1M is None:
                        LAST_EXIT_CANDLE_TS_1M = last_ts_ms
                    else:
                        if last_ts_ms < LAST_EXIT_CANDLE_TS_1M:
                            SAFE_STOP_REQUESTED = True
                            msg = f"[SAFE_STOP][TS_ROLLBACK] 1m ts rollback: prev={LAST_EXIT_CANDLE_TS_1M} now={last_ts_ms}"
                            log(msg)
                            _maybe_send_error_tg("TS_ROLLBACK", msg, cooldown_sec=60)
                            raise RuntimeError(msg)
                        if last_ts_ms > LAST_EXIT_CANDLE_TS_1M:
                            for t in list(OPEN_TRADES):
                                if maybe_exit_with_gpt(t, SET, scenario="RUNTIME_EXIT_CHECK"):
                                    OPEN_TRADES = [ot for ot in OPEN_TRADES if ot is not t]
                                    LAST_CLOSE_TS = now
                                    _invalidate_equity_cache()
                            LAST_EXIT_CANDLE_TS_1M = last_ts_ms
                interruptible_sleep(ENGINE_LOOP_TICK_SEC, tick=ENGINE_LOOP_TICK_SEC)
                continue

            if SAFE_STOP_REQUESTED and not OPEN_TRADES:
                _safe_send_tg("🛑 포지션 0 확인. 자동매매를 종료합니다.")
                return

            entry_cooldown_sec = float(SET.entry_cooldown_sec)
            if now - LAST_ENTRY_GPT_CALL_TS < entry_cooldown_sec:
                interruptible_sleep(ENGINE_LOOP_TICK_SEC, tick=ENGINE_LOOP_TICK_SEC)
                continue

            should_run_entry_cycle, latest_1m_ts, latest_orderbook_ts = _should_run_entry_cycle(
                SET.symbol,
                now,
                last_entry_cycle_wall_ts,
                last_entry_cycle_1m_ts,
                last_entry_cycle_orderbook_ts,
            )
            if not should_run_entry_cycle:
                interruptible_sleep(ENGINE_LOOP_TICK_SEC, tick=ENGINE_LOOP_TICK_SEC)
                continue

            try:
                equity_current_usdt = _get_equity_current_usdt_cached_strict(now)
                _EQUITY_CONSEC_FAILS = 0
            except Exception as e:
                _EQUITY_CONSEC_FAILS += 1
                msg = f"[SKIP][EQUITY_INVALID] {type(e).__name__}: {e} consecutive={_EQUITY_CONSEC_FAILS}/{_EQUITY_FAIL_HARDSTOP_N}"
                log(msg)
                _maybe_send_entry_block_tg("EQUITY_INVALID", msg, cooldown_sec=300)

                if _EQUITY_CONSEC_FAILS >= _EQUITY_FAIL_HARDSTOP_N:
                    SAFE_STOP_REQUESTED = True
                    raise RuntimeError("equity fetch failed consecutively (STRICT)") from e

                interruptible_sleep(60)
                continue

            health_level, health_fail_reason, health_warning_reason, health_snapshot = _get_data_health_state_or_raise()
            if health_level == "FAIL":
                reason_text = _format_data_health_snapshot_reason(health_snapshot, health_fail_reason)
                msg = f"[SKIP][DATA_HEALTH_FAIL] {reason_text}"
                log(msg)
                _maybe_send_entry_block_tg("DATA_HEALTH_FAIL", msg, cooldown_sec=60)
                interruptible_sleep(5)
                continue

            if health_level == "WARNING":
                warning_text = _format_data_health_warning_reason(health_snapshot, health_warning_reason)
                log(f"[WARN][DATA_HEALTH_WARNING] {warning_text}")

            authoritative_5m_gate_ts = _get_latest_ws_5m_signal_gate_ts_or_raise(SET.symbol)
            if _is_current_entry_signal_gate_already_claimed_or_raise(SET.symbol, authoritative_5m_gate_ts):
                last_entry_cycle_wall_ts = now
                last_entry_cycle_1m_ts = latest_1m_ts
                last_entry_cycle_orderbook_ts = latest_orderbook_ts
                interruptible_sleep(ENGINE_LOOP_TICK_SEC, tick=ENGINE_LOOP_TICK_SEC)
                continue

            market_data = _build_entry_market_data_stage_or_raise(
                SET,
                LAST_CLOSE_TS,
                notify_entry_block_fn=_maybe_send_entry_block_tg,
                log_fn=log,
            )

            last_entry_cycle_wall_ts = now
            last_entry_cycle_1m_ts = latest_1m_ts
            last_entry_cycle_orderbook_ts = latest_orderbook_ts

            if market_data is not None:
                signal_ts_ms = _require_int_ms(market_data.get("signal_ts_ms"), "market_data.signal_ts_ms")
                if signal_ts_ms != authoritative_5m_gate_ts:
                    raise RuntimeError(
                        "market_data.signal_ts_ms != authoritative_5m_gate_ts (STRICT): "
                        f"signal_ts_ms={signal_ts_ms} gate_ts={authoritative_5m_gate_ts}"
                    )

            if not _claim_entry_signal_ts_or_skip(SET.symbol, authoritative_5m_gate_ts):
                interruptible_sleep(ENGINE_LOOP_TICK_SEC, tick=ENGINE_LOOP_TICK_SEC)
                continue

            if market_data is None:
                interruptible_sleep(ENGINE_LOOP_TICK_SEC, tick=ENGINE_LOOP_TICK_SEC)
                continue

            try:
                validate_entry_market_data_bundle_strict(market_data)
            except DataIntegrityError as e:
                SAFE_STOP_REQUESTED = True
                msg = f"[SAFE_STOP][DATA_INTEGRITY] {e}"
                log(msg)
                _maybe_send_error_tg("DATA_INTEGRITY", msg, cooldown_sec=60)
                raise RuntimeError(msg) from e

            mf = market_data.get("market_features")
            eng = (mf or {}).get("engine_scores") if isinstance(mf, dict) else None
            total = (eng or {}).get("total") if isinstance(eng, dict) else None
            if not isinstance(total, dict) or "score" not in total:
                raise RuntimeError("engine_scores.total.score missing (STRICT)")

            regime_score = float(total["score"])
            regime_engine.update(regime_score)
            regime_decision = regime_engine.decide(regime_score)
            if float(regime_decision.allocation) <= 0.0:
                msg = f"[SKIP][REGIME_NO_TRADE] score={float(regime_decision.score):.1f} band={regime_decision.band}"
                log(msg)
                _maybe_send_entry_block_tg("REGIME_NO_TRADE", msg, cooldown_sec=60)
                interruptible_sleep(ENGINE_LOOP_TICK_SEC, tick=ENGINE_LOOP_TICK_SEC)
                continue

            account_state = account_builder.build(
                symbol=market_data["symbol"],
                current_equity_usdt=float(equity_current_usdt),
                closed_trades=list(CLOSED_TRADES_CACHE),
                persisted_equity_peak_usdt=persisted_peak,
            )
            persisted_peak = float(max(float(persisted_peak or 0.0), float(account_state.equity_peak_usdt)))

            t0 = time.perf_counter()
            cand = _decide_entry_candidate_strict(market_data, SET)
            dt_ms = (time.perf_counter() - t0) * 1000.0
            if dt_ms > max_signal_latency_ms:
                SAFE_STOP_REQUESTED = True
                msg = f"[SAFE_STOP][LATENCY_SIGNAL] decide_ms={dt_ms:.1f} > {max_signal_latency_ms:.1f}"
                log(msg)
                _maybe_send_error_tg("LATENCY_SIGNAL", msg, cooldown_sec=60)
                raise RuntimeError(msg)

            action = str(cand.action).upper().strip()
            if not action:
                raise RuntimeError("cand.action missing (STRICT)")

            if action != "ENTER":
                msg = f"[SKIP][CANDIDATE] {cand.reason}"
                log(msg)
                _maybe_send_entry_block_tg("CANDIDATE_SKIP", msg, cooldown_sec=60)
                interruptible_sleep(ENGINE_LOOP_TICK_SEC, tick=ENGINE_LOOP_TICK_SEC)
                continue

            LAST_ENTRY_GPT_CALL_TS = now

            micro = (mf or {}).get("microstructure") if isinstance(mf, dict) else None
            if not isinstance(micro, dict):
                raise RuntimeError("market_features.microstructure missing (STRICT)")
            if "micro_score_risk" not in micro:
                raise RuntimeError("microstructure.micro_score_risk missing (STRICT)")
            micro_score_risk = float(micro["micro_score_risk"])
            if not math.isfinite(micro_score_risk) or micro_score_risk < 0 or micro_score_risk > 100:
                raise RuntimeError(f"micro_score_risk out of range (STRICT): {micro_score_risk}")

            hk: HeatmapKey = ev_heatmap.build_key(
                regime_band=str(regime_decision.band),
                micro_score_risk=float(micro_score_risk),
                engine_total_score=float(regime_score),
            )
            cell = ev_heatmap.get_cell_status(hk)

            rp = risk_physics.decide(
                regime_allocation=float(regime_decision.allocation),
                dd_pct=float(account_state.dd_pct),
                consecutive_losses=int(account_state.consecutive_losses),
                tp_pct=float(cand.tp_pct),
                sl_pct=float(cand.sl_pct),
                micro_score_risk=float(micro_score_risk),
                heatmap_status=str(cell.status),
                heatmap_ev=cell.ev,
                heatmap_n=int(cell.n),
            )
            if rp.action_override == "SKIP":
                msg = f"[SKIP][RISK_PHYSICS] {rp.reason}"
                log(msg)
                _maybe_send_entry_block_tg("RISK_PHYSICS_SKIP", msg, cooldown_sec=60)
                interruptible_sleep(ENGINE_LOOP_TICK_SEC, tick=ENGINE_LOOP_TICK_SEC)
                continue

            try:
                drift_detector.update_and_check(
                    DriftSnapshot(
                        symbol=str(market_data["symbol"]),
                        allocation_ratio=float(rp.effective_risk_pct),
                        risk_multiplier=float(rp.auto_risk_multiplier),
                        regime_band=str(regime_decision.band),
                        micro_score_risk=float(micro_score_risk),
                    )
                )
            except DriftDetectedError as e:
                SAFE_STOP_REQUESTED = True
                msg = f"[SAFE_STOP][DRIFT] {e}"
                log(msg)
                _maybe_send_error_tg("DRIFT", msg, cooldown_sec=60)
                raise RuntimeError(msg) from e

            try:
                validate_signal_invariants_strict(
                    SignalInvariantInputs(
                        symbol=str(market_data["symbol"]),
                        direction=str(cand.direction),
                        risk_pct=float(rp.effective_risk_pct),
                        tp_pct=float(cand.tp_pct),
                        sl_pct=float(cand.sl_pct),
                        dd_pct=float(account_state.dd_pct),
                        micro_score_risk=float(micro_score_risk),
                        final_risk_multiplier=None,
                        equity_current_usdt=float(account_state.equity_current_usdt),
                        equity_peak_usdt=float(account_state.equity_peak_usdt),
                    )
                )
            except InvariantViolation as e:
                SAFE_STOP_REQUESTED = True
                msg = f"[SAFE_STOP][INVARIANT] {e}"
                log(msg)
                _maybe_send_error_tg("INVARIANT", msg, cooldown_sec=60)
                raise RuntimeError(msg) from e

            entry_price_hint, entry_price_source = _resolve_entry_price_hint_for_signal_or_raise(
                market_data,
                cand.meta if isinstance(cand.meta, dict) else None,
            )

            meta2 = dict(cand.meta or {})
            _purge_signal_execution_price_aliases_inplace(meta2)
            meta2.update(
                {
                    "entry_price_hint": float(entry_price_hint),
                    "entry_price_source": str(entry_price_source),
                    "equity_current_usdt": float(account_state.equity_current_usdt),
                    "equity_peak_usdt": float(account_state.equity_peak_usdt),
                    "dd_pct": float(account_state.dd_pct),
                    "consecutive_losses": int(account_state.consecutive_losses),
                    "risk_physics_reason": str(rp.reason),
                    "dynamic_allocation_ratio": float(rp.effective_risk_pct),
                    "dynamic_risk_pct": float(rp.effective_risk_pct),
                    "regime_score": float(regime_decision.score),
                    "regime_band": str(regime_decision.band),
                    "regime_allocation": float(regime_decision.allocation),
                    "micro_score_risk": float(micro_score_risk),
                    "ev_cell_key": f"{hk.regime_band}|{hk.distortion_bucket}|{hk.score_bucket}",
                    "ev_cell_status": str(cell.status),
                    "ev_cell_ev": cell.ev,
                    "ev_cell_n": int(cell.n),
                    "auto_blocked": bool(rp.auto_blocked),
                    "auto_risk_multiplier": float(rp.auto_risk_multiplier),
                    "heatmap_status": rp.heatmap_status,
                    "heatmap_ev": rp.heatmap_ev,
                    "heatmap_n": rp.heatmap_n,
                }
            )

            signal_final = Signal(
                action=action,
                direction=str(cand.direction),
                tp_pct=float(cand.tp_pct),
                sl_pct=float(cand.sl_pct),
                risk_pct=float(rp.effective_risk_pct),
                reason=str(cand.reason),
                guard_adjustments=dict(cand.guard_adjustments or {}),
                meta=meta2,
            )

            t1 = time.perf_counter()
            trade = entry_exec_engine.execute(signal_final)
            dt2_ms = (time.perf_counter() - t1) * 1000.0
            if dt2_ms > max_exec_latency_ms:
                SAFE_STOP_REQUESTED = True
                msg = f"[SAFE_STOP][LATENCY_EXEC] exec_ms={dt2_ms:.1f} > {max_exec_latency_ms:.1f}"
                log(msg)
                _maybe_send_error_tg("LATENCY_EXEC", msg, cooldown_sec=60)
                raise RuntimeError(msg)

            if trade is None:
                msg = (
                    "[SKIP][ENTRY_REJECTED_NONFATAL] "
                    f"symbol={SET.symbol} direction={cand.direction} reason=nonfatal_entry_rejection"
                )
                log(msg)
                _maybe_send_entry_block_tg("ENTRY_REJECTED_NONFATAL", msg, cooldown_sec=60)
                interruptible_sleep(ENGINE_LOOP_TICK_SEC, tick=ENGINE_LOOP_TICK_SEC)
                continue

            if not isinstance(trade, Trade):
                raise RuntimeError(
                    f"ExecutionEngine returned invalid type (STRICT): {type(trade).__name__}"
                )

            OPEN_TRADES.append(trade)
            _invalidate_equity_cache()

            interruptible_sleep(ENGINE_LOOP_TICK_SEC, tick=ENGINE_LOOP_TICK_SEC)

        except AccountStateNotReadyError as e:
            msg = f"[SKIP][ACCOUNT_NOT_READY] {e}"
            log(msg)
            _maybe_send_entry_block_tg("ACCOUNT_NOT_READY", msg, cooldown_sec=60)
            interruptible_sleep(ENGINE_LOOP_TICK_SEC, tick=ENGINE_LOOP_TICK_SEC)

        except Exception as e:
            SAFE_STOP_REQUESTED = True

            tb = traceback.format_exc()
            log(f"ERROR(FATAL): {e}\n{tb}")

            core = f"{type(e).__name__}:{str(e)[:200]}"
            key = hashlib.sha1(core.encode("utf-8", errors="ignore")).hexdigest()[:12]
            _maybe_send_error_tg(key, f"❌ 치명 오류: {e}", cooldown_sec=60)

            log_signal(
                event="ERROR",
                symbol=SET.symbol,
                strategy_type="UNKNOWN",
                direction="CLOSE",
                reason=str(e),
            )

            raise

    return


if __name__ == "__main__":
    from core.run_bot_preflight import run_preflight

    run_preflight(preflight_only=False)