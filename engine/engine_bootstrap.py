"""
============================================================
FILE: engine/engine_bootstrap.py
ROLE:
- Binance USDT-M Futures trading engine bootstrap SSOT
- runtime startup / ws bootstrap / initial sync 전용 계층

CORE RESPONSIBILITIES:
- settings runtime contract 검증
- ws/account ws 부팅 및 준비 확인
- REST backfill + market data store thread 시작
- entry gate bootstrap snapshot 복구
- 초기 open trade sync / closed trade bootstrap / equity peak bootstrap
- bootstrap 결과를 dataclass 로 상위 엔진에 전달

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- bootstrap failure 는 즉시 예외 처리
- runtime 은 invalid settings / invalid ws state 로 시작하면 안 된다
- settings 는 SETTINGS 단일 객체만 사용한다
- 부팅 단계는 실행 루프와 분리되어야 한다
- 원본 엔진 로직을 임의 단순화하지 않는다

CHANGE HISTORY:
- 2026-03-12:
  1) ADD(ROOT-CAUSE): run_bot_ws 의 bootstrap 책임을 독립 파일로 1차 분리
  2) ADD(STRUCTURE): bootstrap 결과를 EngineBootstrapArtifacts dataclass 로 반환
  3) KEEP(STRICT): 기존 ws/account ws/backfill/initial sync 검증 계약 유지
============================================================
"""

from __future__ import annotations

import datetime
import math
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from sqlalchemy import text

from settings import SETTINGS
from infra.telelog import log, send_tg
from infra.async_worker import start_worker as start_async_worker, submit as submit_async
from infra.bot_workers import start_telegram_command_thread
from strategy.signal_analysis_worker import start_signal_analysis_thread

from execution.exchange_api import (
    get_balance_detail,
    set_leverage_and_mode,
)
from state.db_core import get_session
from state.sync_exchange import sync_open_trades_from_exchange
from state.trader_state import Trade

from infra.market_data_ws import (
    backfill_klines_from_rest,
    get_health_snapshot as ws_get_health_snapshot,
    get_klines_with_volume as ws_get_klines_with_volume,
    get_klines_with_volume_and_closed as ws_get_klines_with_volume_and_closed,
    get_orderbook as ws_get_orderbook,
    start_ws_loop,
)
from infra.account_ws import (
    get_account_ws_status,
    start_account_ws_loop,
)
from infra.market_data_store import save_candles_bulk_from_ws, save_orderbook_from_ws
from infra.market_data_rest import fetch_klines_rest, KlineRestError
from infra.data_health_monitor import start_health_monitor
from infra.data_integrity_guard import (
    DataIntegrityError,
    validate_kline_series_strict,
    validate_orderbook_strict,
)

SET = SETTINGS

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

ENTRY_GATE_RUNTIME_STATE_SCOPE: str = "ENTRY_EVAL_SIGNAL_GATE"
ENTRY_GATE_RUNTIME_STATE_TABLE: str = "bt_engine_runtime_state"
ACCOUNT_WS_READY_TIMEOUT_SEC: float = 20.0

WsStoreCandleRow = Tuple[int, float, float, float, float, float, bool]
LegacyStoreCandleRow = Tuple[int, float, float, float, float, float]


@dataclass(frozen=True)
class EngineBootstrapArtifacts:
    open_trades: List[Trade]
    last_exchange_sync_ts: float
    last_exit_candle_ts_1m: Optional[int]
    last_entry_eval_signal_ts_ms: Optional[int]
    persisted_equity_peak_usdt: Optional[float]
    closed_trade_rows: List[Dict[str, Any]]


def _require_runtime_setting_exists(settings: Any, name: str) -> Any:
    if not hasattr(settings, name):
        raise RuntimeError(f"settings.{name} is required (STRICT)")
    return getattr(settings, name)


def _verify_runtime_settings_contract_or_raise(settings: Any) -> None:
    for name in ENTRY_REQUIRED_RUNTIME_SETTINGS:
        _require_runtime_setting_exists(settings, name)


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


def _verify_ws_boot_configuration_or_die(settings: Any) -> None:
    ws_subscribe_tfs = _parse_tfs(settings.ws_subscribe_tfs)
    _verify_required_tfs_or_die("ws_subscribe_tfs", ws_subscribe_tfs, ENTRY_REQUIRED_TFS)

    ws_backfill_tfs = _parse_tfs(settings.ws_backfill_tfs)
    _verify_required_tfs_or_die("ws_backfill_tfs", ws_backfill_tfs, ENTRY_REQUIRED_TFS)

    backfill_limit = int(settings.ws_backfill_limit)
    if backfill_limit <= 0:
        raise RuntimeError("settings.ws_backfill_limit must be > 0 (STRICT)")


def _safe_send_tg(msg: str) -> None:
    try:
        ok = submit_async(send_tg, msg, critical=False, label="send_tg")
        if not ok:
            log(f"[TG][DROP] async queue full: {msg}")
    except Exception as e:
        log(f"[TG] async submit error: {type(e).__name__}: {e} | msg={msg}")


def _entry_gate_lock_key(symbol: str) -> int:
    import hashlib

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


def _bootstrap_entry_signal_gate_or_raise(symbol: str) -> Optional[int]:
    _ensure_entry_gate_runtime_state_table_or_raise()
    persisted = _load_persisted_entry_signal_ts_or_raise(symbol)
    if persisted is None:
        log(f"[ENTRY_GATE][BOOT] no persisted signal gate: symbol={_normalize_symbol_strict(symbol)}")
        return None

    log(
        "[ENTRY_GATE][BOOT] persisted signal gate loaded: "
        f"symbol={_normalize_symbol_strict(symbol)} last_entry_eval_signal_ts_ms={persisted}"
    )
    return int(persisted)


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

        time.sleep(0.25)


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


def _load_closed_trades_bootstrap(symbol: str, limit: int = 50) -> List[Dict[str, Any]]:
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

    rows: List[Dict[str, Any]] = []
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


def _backfill_ws_kline_history(settings: Any) -> None:
    symbol = settings.symbol
    intervals = _parse_tfs(settings.ws_backfill_tfs)
    _verify_required_tfs_or_die("ws_backfill_tfs", intervals, ENTRY_REQUIRED_TFS)

    configured_limit = int(settings.ws_backfill_limit)

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


def _start_market_data_store_thread(settings: Any, *, stop_flag_getter: Callable[[], bool]) -> None:
    symbol = settings.symbol
    flush_sec = float(settings.md_store_flush_sec)
    ob_interval_sec = float(settings.ob_store_interval_sec)
    store_tfs = _parse_tfs(settings.md_store_tfs)

    last_persisted_candle_row: Dict[str, Optional[WsStoreCandleRow]] = {iv: None for iv in store_tfs}
    last_ob_ts: float = 0.0
    last_ob_missing_log_ts: float = 0.0

    def _loop() -> None:
        nonlocal last_ob_ts, last_ob_missing_log_ts

        try:
            log(f"[MD-STORE] loop started: flush_sec={flush_sec}, ob_interval_sec={ob_interval_sec}, store_tfs={store_tfs}")
            while True:
                if stop_flag_getter():
                    return

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
            msg = f"⛔ [MD-STORE][FATAL] {type(e).__name__}: {e}"
            log(msg)
            _safe_send_tg(msg)
            raise

    threading.Thread(target=_loop, name="md-store-loop", daemon=True).start()
    log("[MD-STORE] background store thread started")


def _read_last_exit_candle_ts_1m_optional(symbol: str) -> Optional[int]:
    last_1m = ws_get_klines_with_volume(symbol, "1m", limit=1)
    if last_1m:
        return int(last_1m[0][0])
    return None


def bootstrap_engine_runtime_or_raise(
    settings: Any,
    *,
    on_safe_stop: Callable[[], None],
    stop_flag_getter: Callable[[], bool],
) -> EngineBootstrapArtifacts:
    _verify_runtime_settings_contract_or_raise(settings)

    start_async_worker(
        num_threads=int(settings.async_worker_threads),
        max_queue_size=int(settings.async_worker_queue_size),
        thread_name_prefix="async-io",
    )

    _verify_ws_boot_configuration_or_die(settings)
    last_entry_eval_signal_ts_ms = _bootstrap_entry_signal_gate_or_raise(settings.symbol)

    last_exit_candle_ts_1m: Optional[int] = None

    if bool(settings.ws_enabled):
        _backfill_ws_kline_history(settings)
        start_ws_loop(settings.symbol)
        _start_market_data_store_thread(settings, stop_flag_getter=stop_flag_getter)

        market_ws_ready_timeout_sec = max(
            30.0,
            float(settings.ws_max_kline_delay_sec),
            float(settings.ws_market_event_max_delay_sec),
        )
        _wait_market_ws_ready_or_raise(settings.symbol, market_ws_ready_timeout_sec)

        start_health_monitor()
        last_exit_candle_ts_1m = _read_last_exit_candle_ts_1m_optional(settings.symbol)

    if not settings.api_key or not settings.api_secret:
        msg = "❗ API 자격정보가 설정되어 있지 않습니다. 설정 후 재시작해 주세요."
        log(msg)
        _safe_send_tg(msg)
        raise RuntimeError("api credentials missing (STRICT)")

    try:
        set_leverage_and_mode(settings.symbol, int(settings.leverage), bool(settings.isolated))
    except Exception as e:
        allow = bool(settings.allow_start_without_leverage_setup)
        msg = f"❗ 레버리지/마진 설정 실패: {e}"
        log(msg)
        if allow:
            _safe_send_tg(msg + "\n⚠ allow_start_without_leverage_setup=True 이므로 계속 진행합니다.")
        else:
            _safe_send_tg(msg + "\n⛔ 기본 정책에 따라 중단합니다.")
            raise RuntimeError("leverage/margin setup failed (STRICT)") from e

    try:
        start_account_ws_loop()
        _wait_account_ws_ready_or_raise(ACCOUNT_WS_READY_TIMEOUT_SEC)
    except Exception as e:
        msg = f"❗ Account WS 시작 실패: {type(e).__name__}: {e}"
        log(msg)
        _safe_send_tg(msg)
        raise RuntimeError("account ws start failed (STRICT)") from e

    _safe_send_tg("✅ Binance USDT-M Futures 자동매매(WS 버전)를 시작합니다.")

    start_telegram_command_thread(on_stop_command=on_safe_stop)
    start_signal_analysis_thread(interval_sec=int(settings.signal_analysis_interval_sec))

    log(
        "[BOOT] embedded market_researcher autostart disabled in bootstrap "
        "(STRICT separation: trading engine does not implicitly launch external analysis worker)"
    )

    open_trades, _ = sync_open_trades_from_exchange(settings.symbol, replace=True, current_trades=[])
    last_exchange_sync_ts = time.time()

    persisted_peak = _load_equity_peak_bootstrap(settings.symbol)
    if persisted_peak is not None:
        log(f"[BOOT] persisted equity_peak_usdt loaded: {persisted_peak:.4f}")

    closed_trade_rows = _load_closed_trades_bootstrap(settings.symbol, limit=50)

    return EngineBootstrapArtifacts(
        open_trades=open_trades,
        last_exchange_sync_ts=float(last_exchange_sync_ts),
        last_exit_candle_ts_1m=last_exit_candle_ts_1m,
        last_entry_eval_signal_ts_ms=last_entry_eval_signal_ts_ms,
        persisted_equity_peak_usdt=persisted_peak,
        closed_trade_rows=closed_trade_rows,
    )