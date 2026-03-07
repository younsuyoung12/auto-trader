# core/run_bot_ws.py
"""
============================================================
FILE: core/run_bot_ws.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
============================================================

run_bot_ws.py – Binance USDT-M Futures WebSocket 메인 루프

핵심 원칙 (STRICT · NO-FALLBACK)
- 시장데이터(캔들/오더북) 의사결정은 WS 버퍼 데이터만 사용한다.
- REST는 (a) 부팅 WS 버퍼 백필, (b) 계정/주문 상태 조회에만 사용한다.
- 폴백(REST 런타임 백필/더미 값/임의 보정/None→0 치환) 절대 금지.
- 데이터가 없거나 손상되면 즉시 예외 또는 명시적 SKIP 처리한다.
- 텔레그램/비핵심 I/O가 메인 루프를 블로킹하면 안 된다.

중요 (TRADE-GRADE)
- DB 접근 단일화: state/db_core(get_session) 경유. psycopg2 직접 연결 금지.
- DB DSN 폴백 금지: TRADER_DB_URL 단일 소스(검증/정규화는 db_core가 수행).
- 비핵심 루프라도 “조용한 실패” 금지: 치명 오류는 SAFE_STOP + 예외 전파.

변경 이력
------------------------------------------------------------
- 2026-03-07 (TRADE-GRADE PATCH):
  1) 5m 캔들 단위 진입 게이트 추가
     - 동일 signal_ts_ms(사실상 동일 5m 캔들)에서 중복 진입 평가 금지
     - LONG/SHORT flip(같은 캔들 내 방향 뒤집힘) 근본 차단
  2) signal_ts_ms rollback 방어 추가
     - 이전에 처리한 캔들보다 과거 ts가 오면 즉시 예외
  3) 기존 기능 삭제 없음
     - entry_pipeline / execution_engine / risk / drift / reconcile / SAFE_STOP 흐름 유지

- 2026-03-06 (TRADE-GRADE):
  1) Drift Detector 설정을 settings.py SSOT로 연결
     - drift_allocation_abs_jump
     - drift_allocation_spike_ratio
     - drift_multiplier_abs_jump
     - drift_micro_abs_jump
     - drift_stable_regime_steps
  2) run_bot_ws 주요 운영값을 settings.py SSOT로 직접 사용
     - ws_klines_stale_sec
     - reconcile_confirm_n
     - max_signal_latency_ms
     - max_exec_latency_ms
     - position_resync_sec
     - poll_fills_sec
     - entry_cooldown_sec
  3) 기본값 fallback/log 최소화:
     - settings에 존재하는 값은 코드 내부 기본값으로 덮지 않음

- 2026-03-06 (PATCH):
  1) 엔진 런타임 경로에서 entry_flow 직접 호출 제거
  2) unified_features → execution_engine 연결을 유지하면서
     execution_engine이 요구하는 decision meta(entry_score/trend_strength/spread/orderbook_imbalance)를
     run_bot_ws에서 엄격 추출해 주입
  3) 기존 자동매매 구조(WS → unified_features → risk_physics → execution_engine) 복구
  4) 기존 주문/리스크/드리프트/리컨실/로그/SAFE_STOP 로직 유지

- 2026-03-05 (TRADE-GRADE):
  1) FIX: entry market_data candles_5m/candles_5m_raw 정규화
     - get_trading_signal이 Candle 객체 / 5튜플 / 6튜플을 혼합 반환할 수 있으므로,
       _build_entry_market_data에서 항상 6튜플(OHLCV)로 통일 후 STRICT 검증 수행

- 2026-03-06:
  1) account websocket(USER DATA STREAM) 시작/준비 단계 추가
  2) account websocket 연결 상태 가드 추가
  3) 엔진 부팅 시 market ws + account ws 이중 websocket 구조로 정합 강화

- 2026-03-06:
  1) 보호주문 감시 가드 추가
     - OPEN_TRADES 보유 중 TP/SL 보호주문 존재 여부를 account_ws + REST로 교차 검증
  2) 보호주문 누락 시 SAFE_STOP 강화
     - 보호주문 미존재 시 즉시 SAFE_STOP
     - force_close_on_desync=True 이면 강제청산 제출 후 종료
  3) 보호주문/포지션 상태 오판 방지
     - 실제 거래소 포지션이 열려 있는 경우에만 보호주문 누락을 치명으로 판정
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
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from settings import load_settings
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
    get_klines_with_volume as ws_get_klines_with_volume,
    get_orderbook as ws_get_orderbook,
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
from analysis.market_researcher import MarketResearcher
from core.entry_pipeline import EntryCandidate, _build_entry_market_data, _decide_entry_candidate_strict

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

SET = load_settings()
START_TS: float = time.time()
RUNNING: bool = True

ENTRY_REQUIRED_TFS: tuple[str, ...] = ("1m", "5m", "15m", "1h", "4h")
ENTRY_REQUIRED_KLINES_MIN: Dict[str, int] = {"1m": 60, "5m": 60, "15m": 60, "1h": 60, "4h": 60}

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

# TRADE-GRADE PATCH:
# 같은 signal_ts_ms(실질적으로 동일 5m 캔들)에서는 진입 평가를 한 번만 수행한다.
LAST_ENTRY_EVAL_SIGNAL_TS_MS: Optional[int] = None

_BALANCE_CONSEC_FAILS: int = 0
_EQUITY_CONSEC_FAILS: int = 0
_WS_LIVENESS_CONSEC_FAILS: int = 0
_ACCOUNT_WS_CONSEC_FAILS: int = 0

_BALANCE_FAIL_HARDSTOP_N: int = 3
_EQUITY_FAIL_HARDSTOP_N: int = 3
_WS_LIVENESS_FAIL_HARDSTOP_N: int = 3
_ACCOUNT_WS_FAIL_HARDSTOP_N: int = 3
_ACCOUNT_WS_READY_TIMEOUT_SEC: float = 20.0


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


def _claim_entry_signal_ts_or_skip(signal_ts_ms: Any) -> bool:
    """
    TRADE-GRADE PATCH:
    - 동일 signal_ts_ms(동일 5m 캔들 판단)에서는 진입 평가를 한 번만 수행한다.
    - 과거 ts가 오면 rollback으로 간주하고 즉시 예외.
    """
    global LAST_ENTRY_EVAL_SIGNAL_TS_MS

    current_ts = _require_int_ms(signal_ts_ms, "market_data.signal_ts_ms")
    prev_ts = LAST_ENTRY_EVAL_SIGNAL_TS_MS

    if prev_ts is None:
        LAST_ENTRY_EVAL_SIGNAL_TS_MS = current_ts
        return True

    if current_ts < prev_ts:
        raise RuntimeError(
            f"entry signal ts rollback detected (STRICT): prev={prev_ts} now={current_ts}"
        )

    if current_ts == prev_ts:
        return False

    LAST_ENTRY_EVAL_SIGNAL_TS_MS = current_ts
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

    # 실제 거래소 포지션이 열려 있을 때만 보호주문 누락을 치명으로 본다.
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
    global _WS_LIVENESS_CONSEC_FAILS, SAFE_STOP_REQUESTED

    stale_sec_f = float(SET.ws_klines_stale_sec)
    if stale_sec_f <= 30:
        raise RuntimeError("settings.ws_klines_stale_sec must be > 30 sec (STRICT)")

    buf = ws_get_klines_with_volume(symbol, "1m", limit=1)
    if not isinstance(buf, list) or not buf:
        _WS_LIVENESS_CONSEC_FAILS += 1
        log(f"[WS_LIVENESS][FAIL] 1m kline buffer missing/empty consecutive={_WS_LIVENESS_CONSEC_FAILS}/{_WS_LIVENESS_FAIL_HARDSTOP_N}")
    else:
        ts_ms = buf[0][0]
        t_ms = _require_int_ms(ts_ms, "ws.1m.openTime")
        now_ms = int(float(now_ts) * 1000.0)
        age_ms = now_ms - int(t_ms)
        if age_ms < 0:
            _WS_LIVENESS_CONSEC_FAILS += 1
            log(f"[WS_LIVENESS][FAIL] future kline ts detected age_ms={age_ms} consecutive={_WS_LIVENESS_CONSEC_FAILS}/{_WS_LIVENESS_FAIL_HARDSTOP_N}")
        elif age_ms > int(stale_sec_f * 1000.0):
            _WS_LIVENESS_CONSEC_FAILS += 1
            log(f"[WS_LIVENESS][FAIL] stale kline age_ms={age_ms} (> {int(stale_sec_f*1000)}ms) consecutive={_WS_LIVENESS_CONSEC_FAILS}/{_WS_LIVENESS_FAIL_HARDSTOP_N}")
        else:
            if _WS_LIVENESS_CONSEC_FAILS != 0:
                log(f"[WS_LIVENESS][RECOVER] consecutive={_WS_LIVENESS_CONSEC_FAILS} -> 0")
            _WS_LIVENESS_CONSEC_FAILS = 0

    if _WS_LIVENESS_CONSEC_FAILS >= _WS_LIVENESS_FAIL_HARDSTOP_N:
        SAFE_STOP_REQUESTED = True
        msg = f"[SAFE_STOP][WS_LIVENESS] stale/missing WS 1m data confirmed consecutive={_WS_LIVENESS_CONSEC_FAILS}"
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

    limit = int(SET.ws_backfill_limit)

    for iv in intervals:
        min_needed = ENTRY_REQUIRED_KLINES_MIN.get(iv, 1)
        log(f"[BOOT] REST backfill start: symbol={symbol} interval={iv} limit={limit}")

        try:
            rest_rows = fetch_klines_rest(symbol, iv, limit=limit)
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


def _start_market_data_store_thread() -> None:
    symbol = SET.symbol
    flush_sec = float(SET.md_store_flush_sec)
    ob_interval_sec = float(SET.ob_store_interval_sec)
    store_tfs = _parse_tfs(SET.md_store_tfs)

    last_candle_ts: Dict[str, int] = {iv: 0 for iv in store_tfs}
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
                for iv in store_tfs:
                    buf = ws_get_klines_with_volume(symbol, iv, limit=500)
                    if buf is None:
                        raise RuntimeError(f"[MD-STORE] ws_get_klines_with_volume returned None (STRICT) interval={iv}")
                    if not isinstance(buf, list):
                        raise RuntimeError(f"[MD-STORE] kline buffer invalid type (STRICT) interval={iv} type={type(buf).__name__}")
                    if not buf:
                        continue

                    newest_ts = last_candle_ts.get(iv, 0)
                    new_rows = [row for row in buf if row[0] > newest_ts]
                    if not new_rows:
                        continue

                    try:
                        validate_kline_series_strict(new_rows, name=f"md_store.ws_kline[{iv}]", min_len=1)
                    except DataIntegrityError as e:
                        raise RuntimeError(f"[MD-STORE] kline integrity fail (STRICT): {e}") from e

                    for ts_ms, o, h, l, c, v in new_rows:
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
                            }
                        )
                    last_candle_ts[iv] = int(new_rows[-1][0])

                if candles_to_save:
                    save_candles_bulk_from_ws(candles_to_save)

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
            continue
        if str(r.get("symbol") or "").upper().strip() != sym:
            continue
        if "positionAmt" not in r:
            raise RuntimeError("positionRisk.positionAmt missing (STRICT)")
        try:
            amt = float(r["positionAmt"])
        except Exception as e:
            raise RuntimeError(f"positionAmt parse failed (STRICT): {e}") from e
        if abs(amt) > 1e-12:
            live.append(r)

    if not live:
        return {"symbol": sym, "positionAmt": "0", "entryPrice": "0"}

    if len(live) != 1:
        raise RuntimeError(f"ambiguous exchange positions (STRICT): count={len(live)}")

    r0 = live[0]
    if "entryPrice" not in r0:
        raise RuntimeError("positionRisk.entryPrice missing (STRICT)")
    return {"symbol": sym, "positionAmt": str(r0["positionAmt"]), "entryPrice": str(r0["entryPrice"])}


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


def main() -> None:
    global RUNNING, OPEN_TRADES, LAST_CLOSE_TS, CONSEC_LOSSES, SAFE_STOP_REQUESTED, LAST_EXCHANGE_SYNC_TS
    global LAST_EXIT_CANDLE_TS_1M, LAST_ENTRY_GPT_CALL_TS, _SIGTERM_DEADLINE_HANDLED
    global _BALANCE_CONSEC_FAILS, _EQUITY_CONSEC_FAILS

    start_async_worker(
        num_threads=int(SET.async_worker_threads),
        max_queue_size=int(SET.async_worker_queue_size),
        thread_name_prefix="async-io",
    )

    _verify_ws_boot_configuration_or_die()

    if bool(SET.ws_enabled):
        _backfill_ws_kline_history(SET.symbol)
        start_ws_loop(SET.symbol)
        _start_market_data_store_thread()

        log("[BOOT] warmup: waiting 10s before enabling data health gate...")
        interruptible_sleep(10)
        start_health_monitor()
        interruptible_sleep(2)

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

    # -----------------------------------------
    # Market Research Worker (external market)
    # -----------------------------------------
    try:
        researcher = MarketResearcher()

        def _research_loop():
            researcher.run_forever()

        threading.Thread(
            target=_research_loop,
            name="market-researcher",
            daemon=True,
        ).start()

        log("[BOOT] market_researcher thread started")

    except Exception as e:
        log(f"[BOOT][FATAL] market_researcher start failed: {e}")
        raise

    OPEN_TRADES, _ = sync_open_trades_from_exchange(SET.symbol, replace=True, current_trades=OPEN_TRADES)
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

    max_signal_latency_ms = float(SET.max_signal_latency_ms)
    max_exec_latency_ms = float(SET.max_exec_latency_ms)

    while RUNNING:
        try:
            now = time.time()

            if bool(SET.ws_enabled):
                _ws_liveness_guard_or_raise(SET.symbol, now)

            _account_ws_connected_guard_or_raise()

            if OPEN_TRADES:
                _protection_orders_guard_or_raise()

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

                    pnl = float(summary["pnl"])
                    avg_price = float(summary["avg_price"])
                    qty = float(summary["qty"])

                    LAST_CLOSE_TS = now
                    CONSEC_LOSSES = (CONSEC_LOSSES + 1) if pnl < 0 else 0

                    trade_id = getattr(t, "id", None)
                    if not isinstance(trade_id, int) or trade_id <= 0:
                        raise RuntimeError("closed trade missing valid trade.id (DB record mismatch)")

                    exit_ts_dt = datetime.datetime.fromtimestamp(float(now), tz=datetime.timezone.utc)
                    tp_pct_v = getattr(t, "tp_pct", None)
                    sl_pct_v = getattr(t, "sl_pct", None)

                    CLOSED_TRADES_CACHE.appendleft(
                        {
                            "id": int(trade_id),
                            "exit_ts": exit_ts_dt,
                            "pnl_usdt": float(pnl),
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
                        pnl=pnl,
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
                            LAST_EXIT_CANDLE_TS_1M = last_ts_ms
                interruptible_sleep(1)
                continue

            if SAFE_STOP_REQUESTED and not OPEN_TRADES:
                _safe_send_tg("🛑 포지션 0 확인. 자동매매를 종료합니다.")
                return

            entry_cooldown_sec = float(SET.entry_cooldown_sec)
            if now - LAST_ENTRY_GPT_CALL_TS < entry_cooldown_sec:
                interruptible_sleep(1)
                continue

            try:
                equity_current_usdt = _get_equity_current_usdt_strict()
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

            if not bool(data_health_monitor.HEALTH_OK):
                msg = f"[SKIP][DATA_HEALTH_FAIL] {data_health_monitor.LAST_FAIL_REASON}"
                log(msg)
                _maybe_send_entry_block_tg("DATA_HEALTH_FAIL", msg, cooldown_sec=60)
                interruptible_sleep(5)
                continue

            market_data = _build_entry_market_data(
                SET,
                LAST_CLOSE_TS,
                notify_entry_block_fn=_maybe_send_entry_block_tg,
                log_fn=log,
            )
            if market_data is None:
                interruptible_sleep(1)
                continue

            try:
                validate_entry_market_data_bundle_strict(market_data)
            except DataIntegrityError as e:
                SAFE_STOP_REQUESTED = True
                msg = f"[SAFE_STOP][DATA_INTEGRITY] {e}"
                log(msg)
                _maybe_send_error_tg("DATA_INTEGRITY", msg, cooldown_sec=60)
                raise RuntimeError(msg) from e

            # =========================================================
            # TRADE-GRADE PATCH:
            # 동일 5m 캔들(signal_ts_ms 동일)에서는 진입 평가를 한 번만 수행한다.
            # =========================================================
            if not _claim_entry_signal_ts_or_skip(market_data.get("signal_ts_ms")):
                interruptible_sleep(1)
                continue

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
                interruptible_sleep(5)
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

            LAST_ENTRY_GPT_CALL_TS = now

            if str(cand.action).upper().strip() != "ENTER":
                msg = f"[SKIP][CANDIDATE] {cand.reason}"
                log(msg)
                _maybe_send_entry_block_tg("CANDIDATE_SKIP", msg, cooldown_sec=60)
                interruptible_sleep(5)
                continue

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
                interruptible_sleep(5)
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

            meta2 = dict(cand.meta or {})
            meta2.update(
                {
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
                action="ENTER",
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

            if trade:
                OPEN_TRADES.append(trade)

            interruptible_sleep(1)

        except AccountStateNotReadyError as e:
            msg = f"[SKIP][ACCOUNT_NOT_READY] {e}"
            log(msg)
            _maybe_send_entry_block_tg("ACCOUNT_NOT_READY", msg, cooldown_sec=60)
            interruptible_sleep(10)

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