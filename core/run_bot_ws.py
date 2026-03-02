# core/run_bot_ws.py
"""
run_bot_ws.py – Binance USDT-M Futures WebSocket 메인 루프
============================================================

핵심 원칙 (STRICT · NO-FALLBACK)
- 시장데이터(캔들/오더북) 의사결정은 WS 버퍼 데이터만 사용한다.
- REST는 (a) 부팅 WS 버퍼 백필, (b) 계정/주문 상태 조회에만 사용한다.
- 폴백(REST 런타임 백필/더미 값/임의 보정/None→0 치환) 절대 금지.
- 데이터가 없거나 손상되면 즉시 예외 또는 명시적 SKIP 처리한다.

PATCH NOTES — 2026-03-02 (FINAL)
- WS 부팅/백필/헬스 루프 정합:
  - 부팅: REST 백필 → WS 시작 → (10초 워밍업) → health monitor 시작
- DB 부트스트랩 DSN 정합:
  - psycopg2는 postgresql+psycopg2:// 를 못 먹음 → postgresql:// 로 정규화
- DD 영속화:
  - bt_trade_snapshots에서 MAX(equity_peak_usdt) 로드 → AccountStateBuilder seed
- GPTStrategy STRICT 통과:
  - market_data["account_state"] 필수 주입
- 잔액 0 운영 모드:
  - equity_current_usdt=0이면 ENTRY는 명시적 SKIP + 60초 sleep (스팸 방지)
- signals_logger strict(side) 정합:
  - ERROR 로그는 direction="CLOSE" 강제
  - CLOSE 로그는 BUY/SELL → LONG/SHORT 정규화
- CLOSED_TRADES_CACHE 정렬 STRICT:
  - 최신→과거 내림차순 유지(appendleft)
"""

from __future__ import annotations

import csv
import datetime
import hashlib
import math
import os
import signal
import threading
import time
import traceback
from collections import deque
from typing import Any, Dict, List, Optional, Tuple

from settings import KST, load_settings
from infra.telelog import log, send_tg

from execution.exchange_api import (
    get_available_usdt,
    get_balance_detail,
    set_leverage_and_mode,
)
from execution.order_executor import close_all_positions_market
from state.sync_exchange import sync_open_trades_from_exchange

from state.trader_state import Trade, TraderState, check_closes, build_close_summary_strict
from state.exit_engine import maybe_exit_with_gpt

from events.signals_logger import log_signal

from infra.bot_workers import start_telegram_command_thread
from strategy.signal_analysis_worker import start_signal_analysis_thread

from infra.market_data_ws import (
    backfill_klines_from_rest,
    get_health_snapshot,
    get_kline_buffer_status as ws_get_kline_buffer_status,
    get_klines_with_volume as ws_get_klines_with_volume,
    get_orderbook as ws_get_orderbook,
    start_ws_loop,
)
from infra.market_data_store import save_candles_bulk_from_ws, save_orderbook_from_ws
from infra.market_data_rest import fetch_klines_rest, KlineRestError

import infra.data_health_monitor as data_health_monitor
from infra.data_health_monitor import start_health_monitor

from infra.market_features_ws import get_trading_signal, FeatureBuildError
from strategy.unified_features_builder import build_unified_features, UnifiedFeaturesError
from strategy.gpt_strategy import GPTStrategy
from execution.execution_engine import ExecutionEngine

from strategy.regime_engine import RegimeEngine
from strategy.account_state_builder import AccountStateBuilder, AccountStateNotReadyError
from execution.risk_physics_engine import RiskPhysicsEngine, RiskPhysicsPolicy
from strategy.signal import Signal


# ─────────────────────────────
# 전역 상태
# ─────────────────────────────
SET = load_settings()
START_TS: float = time.time()
RUNNING: bool = True

KRW_RATE: float = float(getattr(SET, "krw_per_usdt", 1400.0) or 1400.0)

ENTRY_REQUIRED_TFS: tuple[str, ...] = ("1m", "5m", "15m", "1h", "4h")
ENTRY_REQUIRED_KLINES_MIN: Dict[str, int] = {"1m": 20, "5m": 20, "15m": 20, "1h": 60, "4h": 60}

_LAST_ENTRY_BLOCK_TG_TS: float = 0.0
_LAST_ENTRY_BLOCK_KEY: str = ""

_LAST_ERROR_TG_TS: float = 0.0
_LAST_ERROR_TG_KEY: str = ""

SIGTERM_REQUESTED_AT: Optional[float] = None
SIGTERM_DEADLINE_TS: Optional[float] = None
_SIGTERM_NOTICE_SENT: bool = False
_SIGTERM_DEADLINE_HANDLED: bool = False
_SIGTERM_FORCE_CLOSE_ATTEMPTED: bool = False

METRICS: Dict[str, Any] = {
    "start_ts": START_TS,
    "last_loop_ts": START_TS,
    "open_trades": 0,
    "consec_losses": 0,
    "kline_failures": 0,
}

OPEN_TRADES: List[Trade] = []
TRADER_STATE: TraderState = TraderState()
LAST_CLOSE_TS: float = 0.0
CONSEC_LOSSES: int = 0
SAFE_STOP_REQUESTED: bool = False
LAST_EXCHANGE_SYNC_TS: float = 0.0

LAST_STATUS_TG_TS: float = 0.0
STATUS_TG_INTERVAL_SEC: int = int(getattr(SET, "unrealized_notify_sec", 1800) or 1800)

SIGNAL_ANALYSIS_INTERVAL_SEC: int = int(getattr(SET, "signal_analysis_interval_sec", 60) or 60)

DATA_HEALTH_TG_INTERVAL_SEC: int = int(getattr(SET, "data_health_notify_sec", 900) or 900)
LAST_DATA_HEALTH_TG_TS: float = 0.0

LAST_REST_BACKFILL_AT: float = 0.0
LAST_EXIT_CANDLE_TS_1M: Optional[int] = None
LAST_ENTRY_GPT_CALL_TS: float = 0.0


# ─────────────────────────────
# 기본 유틸
# ─────────────────────────────
def _safe_send_tg(msg: str) -> None:
    try:
        send_tg(msg)
    except Exception as e:
        log(f"[TG] send_tg error: {e}")


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
    ws_subscribe_tfs = _parse_tfs(getattr(SET, "ws_subscribe_tfs", None))
    _verify_required_tfs_or_die("ws_subscribe_tfs", ws_subscribe_tfs, ENTRY_REQUIRED_TFS)

    ws_backfill_tfs = _parse_tfs(getattr(SET, "ws_backfill_tfs", None))
    if not ws_backfill_tfs:
        ws_backfill_tfs = list(ENTRY_REQUIRED_TFS)
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


def _normalize_direction_for_events(v: Any) -> str:
    s = str(v or "").upper().strip()
    if s in ("LONG", "SHORT", "CLOSE"):
        return s
    if s == "BUY":
        return "LONG"
    if s == "SELL":
        return "SHORT"
    return "CLOSE"


# ─────────────────────────────
# WS 준비 체크
# ─────────────────────────────
def _validate_orderbook_for_entry(symbol: str) -> Optional[str]:
    ob = ws_get_orderbook(symbol, limit=5)
    if not isinstance(ob, dict):
        return "orderbook missing (ws_get_orderbook returned non-dict/None)"
    bids = ob.get("bids")
    asks = ob.get("asks")
    if not isinstance(bids, list) or not bids:
        return "orderbook bids empty"
    if not isinstance(asks, list) or not asks:
        return "orderbook asks empty"

    best_bid = ob.get("bestBid")
    best_ask = ob.get("bestAsk")

    try:
        bb = float(bids[0][0]) if best_bid is None else float(best_bid)
        ba = float(asks[0][0]) if best_ask is None else float(best_ask)
    except Exception:
        return "orderbook bestBid/bestAsk invalid"

    if bb <= 0 or ba <= 0:
        return f"orderbook best prices invalid (bestBid={bb}, bestAsk={ba})"
    if ba <= bb:
        return f"orderbook crossed (bestAsk={ba} <= bestBid={bb})"
    return None


def _validate_klines_for_entry(symbol: str) -> Optional[str]:
    for iv, min_len in ENTRY_REQUIRED_KLINES_MIN.items():
        buf = ws_get_klines_with_volume(symbol, iv, limit=min_len)
        if not isinstance(buf, list):
            return f"kline buffer invalid type for {iv}"
        if len(buf) < min_len:
            return f"kline buffer 부족: {iv} need={min_len} got={len(buf)}"
    return None


def _validate_ws_entry_prereqs(symbol: str) -> Optional[str]:
    r = _validate_orderbook_for_entry(symbol)
    if r:
        return r
    r = _validate_klines_for_entry(symbol)
    if r:
        return r
    return None


# ─────────────────────────────
# equity / DB bootstrap
# ─────────────────────────────
def _get_equity_current_usdt_strict() -> float:
    """
    STRICT:
    - /fapi/v2/balance USDT row 기반 equity 확정
    - 우선순위:
      1) balance(or walletBalance) + crossUnPnl
      2) crossWalletBalance + crossUnPnl
      3) availableBalance
    """
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


def _dsn_strict() -> str:
    """
    STRICT:
    - psycopg2.connect()가 받을 수 있는 DSN만 허용
    - SQLAlchemy URL(postgresql+psycopg2://, postgresql+asyncpg://)은 postgresql://로 정규화
    """
    dsn = os.getenv("TRADER_DB_URL") or os.getenv("DATABASE_URL")
    if not dsn or not str(dsn).strip():
        raise RuntimeError("TRADER_DB_URL or DATABASE_URL is required")
    s = str(dsn).strip()

    if s.startswith("postgresql+psycopg2://"):
        s = "postgresql://" + s[len("postgresql+psycopg2://") :]
    if s.startswith("postgresql+asyncpg://"):
        s = "postgresql://" + s[len("postgresql+asyncpg://") :]

    return s


def _load_equity_peak_bootstrap(symbol: str) -> Optional[float]:
    dsn = _dsn_strict()
    try:
        import psycopg2
    except Exception as e:
        raise RuntimeError("psycopg2 is required for equity_peak bootstrap") from e

    q = """
        SELECT MAX(equity_peak_usdt)
        FROM bt_trade_snapshots
        WHERE symbol = %s
          AND equity_peak_usdt IS NOT NULL
    """
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        cur.execute(q, (symbol,))
        row = cur.fetchone()
        if not row:
            return None
        v = row[0]
        if v is None:
            return None
        peak = float(v)
        if peak <= 0:
            raise RuntimeError(f"persisted equity_peak_usdt invalid: {peak}")
        return float(peak)
    finally:
        try:
            if cur is not None:
                cur.close()
        except Exception:
            pass
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def _load_closed_trades_bootstrap(symbol: str, limit: int = 50) -> list[dict[str, Any]]:
    dsn = _dsn_strict()
    try:
        import psycopg2
    except Exception as e:
        raise RuntimeError("psycopg2 is required for account_state bootstrap") from e

    q = """
        SELECT id, exit_ts, pnl_usdt, tp_pct, sl_pct
        FROM bt_trades
        WHERE symbol = %s
          AND exit_ts IS NOT NULL
          AND pnl_usdt IS NOT NULL
        ORDER BY exit_ts DESC
        LIMIT %s
    """
    rows: list[dict[str, Any]] = []
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        cur.execute(q, (symbol, int(limit)))
        for (trade_id, exit_ts, pnl_usdt, tp_pct, sl_pct) in cur.fetchall():
            rows.append(
                {
                    "id": int(trade_id),
                    "exit_ts": exit_ts,
                    "pnl_usdt": float(pnl_usdt),
                    "tp_pct": None if tp_pct is None else float(tp_pct),
                    "sl_pct": None if sl_pct is None else float(sl_pct),
                }
            )
        return rows
    finally:
        try:
            if cur is not None:
                cur.close()
        except Exception:
            pass
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


# ─────────────────────────────
# WS 부트스트랩/스토어
# ─────────────────────────────
def _backfill_ws_kline_history(symbol: str) -> None:
    global LAST_REST_BACKFILL_AT

    intervals = _parse_tfs(getattr(SET, "ws_backfill_tfs", None)) or list(ENTRY_REQUIRED_TFS)
    _verify_required_tfs_or_die("ws_backfill_tfs", intervals, ENTRY_REQUIRED_TFS)

    limit = int(getattr(SET, "ws_backfill_limit", 120) or 120)

    for iv in intervals:
        min_needed = ENTRY_REQUIRED_KLINES_MIN.get(iv, 1)
        log(f"[BOOT] REST backfill start: symbol={symbol} interval={iv} limit={limit}")

        try:
            rest_rows = fetch_klines_rest(symbol, iv, limit=limit)
        except KlineRestError as e:
            METRICS["kline_failures"] = METRICS.get("kline_failures", 0) + 1
            raise RuntimeError(f"REST backfill failed: symbol={symbol} interval={iv} err={e}") from e

        if not rest_rows:
            METRICS["kline_failures"] = METRICS.get("kline_failures", 0) + 1
            raise RuntimeError(f"REST backfill returned 0 rows: symbol={symbol} interval={iv}")

        backfill_klines_from_rest(symbol, iv, rest_rows)

        buf = ws_get_klines_with_volume(symbol, iv, limit=max(60, min_needed))
        if not isinstance(buf, list) or len(buf) < min_needed:
            raise RuntimeError(
                f"WS buffer verify failed: {symbol} {iv} need={min_needed} got={len(buf) if isinstance(buf, list) else 'N/A'}"
            )

        LAST_REST_BACKFILL_AT = time.time()


def _start_market_data_store_thread() -> None:
    symbol = SET.symbol
    flush_sec = float(getattr(SET, "md_store_flush_sec", 5) or 5)
    ob_interval_sec = float(getattr(SET, "ob_store_interval_sec", 5) or 5)
    store_tfs = _parse_tfs(getattr(SET, "md_store_tfs", None)) or ["1m", "5m", "15m"]

    last_candle_ts: Dict[str, int] = {iv: 0 for iv in store_tfs}
    last_ob_ts: float = 0.0

    def _loop() -> None:
        nonlocal last_ob_ts
        log(f"[MD-STORE] loop started: flush_sec={flush_sec}, ob_interval_sec={ob_interval_sec}, store_tfs={store_tfs}")
        while RUNNING:
            try:
                now = time.time()
                candles_to_save: List[Dict[str, Any]] = []

                for iv in store_tfs:
                    buf = ws_get_klines_with_volume(symbol, iv, limit=500)
                    if not buf:
                        continue
                    newest_ts = last_candle_ts.get(iv, 0)
                    new_rows = [row for row in buf if row[0] > newest_ts]
                    if not new_rows:
                        continue

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
                    if ob and ob.get("bids") and ob.get("asks"):
                        ts_ms = int(ob.get("exchTs") or ob.get("ts") or int(now * 1000))
                        save_orderbook_from_ws(symbol=symbol, ts_ms=ts_ms, bids=ob["bids"], asks=ob["asks"])
                        last_ob_ts = now

            except Exception as e:
                log(f"[MD-STORE] loop error: {e}")

            time.sleep(flush_sec)

    threading.Thread(target=_loop, name="md-store-loop", daemon=True).start()
    log("[MD-STORE] background store thread started")


# ─────────────────────────────
# ENTRY market_data builder
# ─────────────────────────────
def _build_entry_market_data(settings: Any, last_close_ts: float) -> Optional[Dict[str, Any]]:
    signal_ctx = get_trading_signal(settings=settings, last_close_ts=last_close_ts)
    if signal_ctx is None:
        return None
    if not isinstance(signal_ctx, (tuple, list)) or len(signal_ctx) != 7:
        raise RuntimeError("get_trading_signal returned invalid tuple format")

    chosen_signal, signal_source, latest_ts, candles_5m, candles_5m_raw, last_price, extra = signal_ctx

    symbol = str(getattr(settings, "symbol", "")).strip()
    if not symbol:
        raise RuntimeError("settings.symbol is required")

    direction = str(chosen_signal).upper().strip()
    if direction not in ("LONG", "SHORT"):
        raise RuntimeError(f"invalid chosen_signal: {chosen_signal!r}")

    signal_source_s = str(signal_source).strip()
    if not signal_source_s:
        raise RuntimeError("signal_source is empty")

    ts_ms = int(latest_ts)
    if ts_ms <= 0:
        raise RuntimeError(f"invalid latest_ts: {latest_ts!r}")

    prereq_reason = _validate_ws_entry_prereqs(symbol)
    if prereq_reason:
        msg = f"[SKIP][WS_NOT_READY] entry blocked: {symbol} ({prereq_reason})"
        log(msg)
        _maybe_send_entry_block_tg(f"WS_NOT_READY:{symbol}:{prereq_reason}", msg, cooldown_sec=60)
        return None

    try:
        market_features = build_unified_features(symbol)
    except (UnifiedFeaturesError, FeatureBuildError) as e:
        msg = f"[SKIP][FEATURE_BUILD_FAIL] entry blocked: {symbol} ({e})"
        log(msg)
        _maybe_send_entry_block_tg(f"FEATURE_BUILD_FAIL:{symbol}:{type(e).__name__}", msg, cooldown_sec=60)
        return None

    if extra is not None and not isinstance(extra, dict):
        raise RuntimeError("extra must be dict or None")

    return {
        "symbol": symbol,
        "direction": direction,
        "signal_source": signal_source_s,
        "regime": signal_source_s,
        "signal_ts_ms": ts_ms,
        "candles_5m": candles_5m,
        "candles_5m_raw": candles_5m_raw,
        "last_price": float(last_price),
        "extra": extra,
        "market_features": market_features,
    }


# ─────────────────────────────
# SIGTERM / SAFE STOP
# ─────────────────────────────
def _sigterm(*_: Any) -> None:
    global SAFE_STOP_REQUESTED, SIGTERM_REQUESTED_AT, SIGTERM_DEADLINE_TS, _SIGTERM_NOTICE_SENT
    SAFE_STOP_REQUESTED = True
    now = time.time()

    if SIGTERM_REQUESTED_AT is None:
        SIGTERM_REQUESTED_AT = now
        grace = float(getattr(SET, "sigterm_grace_sec", 30.0) or 30.0)
        if grace <= 0:
            grace = 30.0
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


# ─────────────────────────────
# main
# ─────────────────────────────
def main() -> None:
    global RUNNING, OPEN_TRADES, LAST_CLOSE_TS, CONSEC_LOSSES, SAFE_STOP_REQUESTED, LAST_EXCHANGE_SYNC_TS
    global LAST_STATUS_TG_TS, LAST_DATA_HEALTH_TG_TS, LAST_EXIT_CANDLE_TS_1M, LAST_ENTRY_GPT_CALL_TS
    global SIGTERM_DEADLINE_TS, _SIGTERM_DEADLINE_HANDLED, _SIGTERM_FORCE_CLOSE_ATTEMPTED

    _verify_ws_boot_configuration_or_die()

    # WS 부팅(REST 백필 → WS 시작 → (10초 워밍업) → health monitor 시작)
    if bool(getattr(SET, "ws_enabled", True)):
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

    # leverage/margin
    try:
        set_leverage_and_mode(SET.symbol, int(getattr(SET, "leverage", 1) or 1), bool(getattr(SET, "isolated", True)))
    except Exception as e:
        allow = bool(getattr(SET, "allow_start_without_leverage_setup", False))
        msg = f"❗ 레버리지/마진 설정 실패: {e}"
        log(msg)
        if allow:
            _safe_send_tg(msg + "\n⚠ allow_start_without_leverage_setup=True 이므로 계속 진행합니다.")
        else:
            _safe_send_tg(msg + "\n⛔ 기본 정책에 따라 중단합니다.")
            return

    _safe_send_tg("✅ Binance USDT-M Futures 자동매매(WS 버전)를 시작합니다.")

    start_telegram_command_thread(on_stop_command=_on_safe_stop)
    start_signal_analysis_thread(interval_sec=SIGNAL_ANALYSIS_INTERVAL_SEC)

    # exchange sync
    OPEN_TRADES, _ = sync_open_trades_from_exchange(SET.symbol, replace=True, current_trades=OPEN_TRADES)
    LAST_EXCHANGE_SYNC_TS = time.time()

    entry_strategy = GPTStrategy(SET)
    entry_exec_engine = ExecutionEngine(SET)
    regime_engine = RegimeEngine(window_size=200, percentile_min_history=60)

    # persisted peak
    persisted_peak: Optional[float] = None
    try:
        persisted_peak = _load_equity_peak_bootstrap(SET.symbol)
        if persisted_peak is not None:
            log(f"[BOOT] persisted equity_peak_usdt loaded: {persisted_peak:.4f}")
    except Exception as e:
        log(f"[WARN] equity_peak bootstrap failed: {type(e).__name__}: {e}")
        persisted_peak = None

    account_builder = AccountStateBuilder(
        win_rate_window=20,
        min_trades_for_win_rate=5,
        initial_equity_peak_usdt=persisted_peak,
    )

    risk_policy = RiskPhysicsPolicy(max_allocation=1.0)
    risk_physics = RiskPhysicsEngine(policy=risk_policy)

    CLOSED_TRADES_CACHE = deque(maxlen=50)
    try:
        boot_rows = _load_closed_trades_bootstrap(SET.symbol, limit=50)  # DESC
        for r in reversed(boot_rows):
            CLOSED_TRADES_CACHE.appendleft(r)
    except Exception as e:
        log(f"[WARN] closed_trades bootstrap failed: {type(e).__name__}: {e}")
        CLOSED_TRADES_CACHE.clear()

    position_resync_sec = float(getattr(SET, "position_resync_sec", 20) or 20)
    last_fill_check: float = 0.0
    last_balance_log: float = 0.0

    while RUNNING:
        try:
            now = time.time()

            # exchange sync
            if now - LAST_EXCHANGE_SYNC_TS >= position_resync_sec:
                OPEN_TRADES, _ = sync_open_trades_from_exchange(SET.symbol, replace=True, current_trades=OPEN_TRADES)
                LAST_EXCHANGE_SYNC_TS = now

            # balance ping
            if now - last_balance_log >= 60:
                get_available_usdt()
                last_balance_log = now

            # close check
            if now - last_fill_check >= float(getattr(SET, "poll_fills_sec", 3.0) or 3.0):
                OPEN_TRADES, closed_list = check_closes(OPEN_TRADES, TRADER_STATE)
                for closed in closed_list:
                    t: Trade = closed["trade"]
                    reason: str = closed["reason"]
                    summary: Any = closed.get("summary")

                    if not isinstance(summary, dict) or not summary:
                        reason2, summary2 = build_close_summary_strict(t)
                        if not isinstance(summary2, dict) or not summary2:
                            raise RuntimeError("close summary requery returned invalid")
                        reason = str(reason2 or reason)
                        summary = summary2

                    pnl = float(summary["pnl"])
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
                        direction=_normalize_direction_for_events(t.side),
                        price=float(summary.get("avg_price") or 0.0),
                        qty=float(summary.get("qty") or 0.0),
                        reason=reason,
                        pnl=pnl,
                    )

                last_fill_check = now

            # open trades → exit checks
            if OPEN_TRADES:
                last_1m = ws_get_klines_with_volume(SET.symbol, "1m", limit=1)
                if last_1m:
                    last_ts_ms = int(last_1m[0][0])
                    if LAST_EXIT_CANDLE_TS_1M is None:
                        LAST_EXIT_CANDLE_TS_1M = last_ts_ms
                    elif last_ts_ms > LAST_EXIT_CANDLE_TS_1M:
                        for t in list(OPEN_TRADES):
                            if maybe_exit_with_gpt(t, SET, scenario="RUNTIME_EXIT_CHECK"):
                                OPEN_TRADES = [ot for ot in OPEN_TRADES if ot is not t]
                                LAST_CLOSE_TS = now
                        LAST_EXIT_CANDLE_TS_1M = last_ts_ms
                interruptible_sleep(1)
                continue

            # safe stop
            if SAFE_STOP_REQUESTED and not OPEN_TRADES:
                _safe_send_tg("🛑 포지션 0 확인. 자동매매를 종료합니다.")
                return

            # entry cooldown
            entry_cooldown_sec = float(getattr(SET, "entry_cooldown_sec", 20) or 20)
            if now - LAST_ENTRY_GPT_CALL_TS < entry_cooldown_sec:
                interruptible_sleep(1)
                continue

            # ✅ 잔액 0이면: 엔트리 전체를 60초 쉬고 재확인(스팸 제거)
            try:
                equity_current_usdt = _get_equity_current_usdt_strict()
            except Exception as e:
                msg = f"[SKIP][EQUITY_INVALID] {e} (balance likely 0)"
                log(msg)
                _maybe_send_entry_block_tg("EQUITY_INVALID", msg, cooldown_sec=300)  # 5분 1회
                interruptible_sleep(60)  # 60초 쉬고 재확인
                continue

            # data health gate (잔액이 있을 때만 의미 있음)
            if not bool(data_health_monitor.HEALTH_OK):
                msg = f"[SKIP][DATA_HEALTH_FAIL] {data_health_monitor.LAST_FAIL_REASON}"
                log(msg)
                _maybe_send_entry_block_tg("DATA_HEALTH_FAIL", msg, cooldown_sec=60)
                interruptible_sleep(5)
                continue

            market_data = _build_entry_market_data(SET, LAST_CLOSE_TS)
            if market_data is None:
                interruptible_sleep(1)
                continue

            # Regime decision
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

            # Account state + persisted_peak 유지
            account_state = account_builder.build(
                symbol=market_data["symbol"],
                current_equity_usdt=float(equity_current_usdt),
                closed_trades=list(CLOSED_TRADES_CACHE),
                persisted_equity_peak_usdt=persisted_peak,
            )
            persisted_peak = (
                float(account_state.equity_peak_usdt)
                if persisted_peak is None
                else float(max(float(persisted_peak), float(account_state.equity_peak_usdt)))
            )

            market_data["account_state"] = {
                "dd_pct": float(account_state.dd_pct),
                "consecutive_losses": int(account_state.consecutive_losses),
                "recent_win_rate": float(account_state.recent_win_rate),
                "recent_trades_count": int(account_state.recent_trades_count),
                "equity_current_usdt": float(account_state.equity_current_usdt),
                "equity_peak_usdt": float(account_state.equity_peak_usdt),
                "recent_planned_rr_avg": account_state.recent_planned_rr_avg,
            }

            signal_obj = entry_strategy.decide(market_data)
            LAST_ENTRY_GPT_CALL_TS = now

            if str(signal_obj.action).upper().strip() != "ENTER":
                msg = f"[SKIP][GPT] {signal_obj.reason}"
                log(msg)
                _maybe_send_entry_block_tg("GPT_SKIP", msg, cooldown_sec=60)
                interruptible_sleep(5)
                continue

            rp = risk_physics.decide(
                regime_allocation=float(regime_decision.allocation),
                dd_pct=float(account_state.dd_pct),
                consecutive_losses=int(account_state.consecutive_losses),
                tp_pct=float(signal_obj.tp_pct),
                sl_pct=float(signal_obj.sl_pct),
            )
            if rp.action_override == "SKIP":
                msg = f"[SKIP][RISK_PHYSICS] {rp.reason}"
                log(msg)
                _maybe_send_entry_block_tg("RISK_PHYSICS_SKIP", msg, cooldown_sec=60)
                interruptible_sleep(5)
                continue

            meta2 = dict(signal_obj.meta or {})
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
                }
            )

            signal_final = Signal(
                action="ENTER",
                direction=signal_obj.direction,
                tp_pct=float(signal_obj.tp_pct),
                sl_pct=float(signal_obj.sl_pct),
                risk_pct=float(rp.effective_risk_pct),
                reason=str(signal_obj.reason),
                guard_adjustments=dict(signal_obj.guard_adjustments or {}),
                meta=meta2,
            )

            trade = entry_exec_engine.execute(signal_final)
            if trade:
                OPEN_TRADES.append(trade)

            interruptible_sleep(1)

        except AccountStateNotReadyError as e:
            msg = f"[SKIP][ACCOUNT_NOT_READY] {e}"
            log(msg)
            _maybe_send_entry_block_tg("ACCOUNT_NOT_READY", msg, cooldown_sec=60)
            interruptible_sleep(10)

        except Exception as e:
            tb = traceback.format_exc()
            log(f"ERROR: {e}\n{tb}")

            core = f"{type(e).__name__}:{str(e)[:200]}"
            key = hashlib.sha1(core.encode("utf-8", errors="ignore")).hexdigest()[:12]
            _maybe_send_error_tg(key, f"❌ 오류: {e}", cooldown_sec=60)

            # signals_logger strict(side) 방지
            log_signal(
                event="ERROR",
                symbol=SET.symbol,
                strategy_type="UNKNOWN",
                direction="CLOSE",
                reason=str(e),
            )
            interruptible_sleep(5)

    return


if __name__ == "__main__":
    main()