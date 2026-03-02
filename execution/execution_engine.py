# execution/execution_engine.py
"""
========================================================
FILE: execution/execution_engine.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================

역할
- strategy.Signal을 받아 “실행”만 한다.
- 가드(수동/볼륨/점프/스프레드/하드리스크) → 주문 실행 → DB 영속화 → 알림
- 폴백 금지: 누락/불일치/모호성은 즉시 예외

핵심 원칙
- 예외 삼키기 금지(단, 상태 기록 목적의 catch 후 즉시 재-raise는 허용)
- “SKIP”은 정상 흐름(리스크/가드에 의해 실행하지 않는 결정)으로 처리한다.
- “ENTER”는 필요한 메타/필드가 1개라도 없으면 즉시 중단한다.
- 동시 실행(레이스) 금지: execute()는 전역 단일 락으로 직렬화한다.
- 알림(텔레그램)은 메인 실행을 블로킹하면 안 된다(비동기 위임). 실패해도 실행 흐름을 망치지 않는다.

PATCH NOTES — 2026-03-02 (UPGRADE)
- Telegram I/O 비동기화:
  - infra/async_worker.submit()로 send_tg / send_skip_tg를 위임(메인 실행 블로킹 제거)
- bt_trade_snapshots에 DD 영속화 저장(STRICT):
  - meta.equity_current_usdt / meta.equity_peak_usdt / meta.dd_pct 를 STRICT로 요구
  - record_trade_snapshot에 위 3개 값을 저장
- deterministic entry_client_order_id 생성/주입:
  - 동일 signal 재시도 시 동일 newClientOrderId 생성 → 중복 진입 차단
- bt_trades 실행/복구 필드 영속화(STRICT):
  - entry_order_id / tp_order_id / sl_order_id
  - exchange_position_side / remaining_qty / realized_pnl_usdt
  - reconciliation_status / last_synced_at
  - 필수 값 누락 시 즉시 예외(폴백 금지)
- order_state 최소 FSM 도입:
  - OrderState를 내부 전이로 기록(디버깅/추적용)

PATCH NOTES — 2026-03-02 (FIX)
- Pylance "Trade is not defined" 해결:
  - from state.trader_state import Trade 추가
========================================================
"""

from __future__ import annotations

import logging
import math
import os
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Dict, Optional, Tuple

from events.signals_logger import log_event, log_signal, log_skip_event
from execution.exchange_api import get_available_usdt, get_balance_detail
from execution.order_executor import open_position_with_tp_sl
from execution.risk_manager import hard_risk_guard_check
from infra.telelog import send_skip_tg, send_tg
from infra.async_worker import submit as submit_async
from risk.entry_guards_ws import (
    check_manual_position_guard,
    check_price_jump_guard,
    check_spread_guard,
    check_volume_guard,
)
from strategy.signal import Signal
from state.db_writer import (
    close_latest_open_trade_returning_id,
    record_trade_exit_snapshot,
    record_trade_open_returning_id,
    record_trade_snapshot,
)
from state.trader_state import Trade  # ✅ FIX: Trade 타입 import
from execution.exceptions import OrderFailed, StateViolation
from execution.order_state import OrderState

logger = logging.getLogger(__name__)

# ============================================================
# Global single execution lock (STRICT)
# - 동시 실행(레이스)로 인한 중복 주문/중복 DB 기록을 구조적으로 차단한다.
# - acquire 실패 시 즉시 예외(StateViolation). (폴백/조용한 SKIP 금지)
# ============================================================
_EXECUTION_LOCK: Lock = Lock()


def _env_bool(name: str, default: str = "0") -> bool:
    v = os.getenv(name, default)
    return str(v).strip().lower() in {"1", "true", "t", "yes", "y", "on"}


TEST_FORCE_ENTER: bool = _env_bool("TEST_FORCE_ENTER", "0")
TEST_BYPASS_GUARDS: bool = _env_bool("TEST_BYPASS_GUARDS", "0")
TEST_DRY_RUN: bool = _env_bool("TEST_DRY_RUN", "0")

_TEST_FAKE_AVAILABLE_USDT_RAW = os.getenv("TEST_FAKE_AVAILABLE_USDT", "0")
TEST_FAKE_AVAILABLE_USDT: float = float(_TEST_FAKE_AVAILABLE_USDT_RAW) if _TEST_FAKE_AVAILABLE_USDT_RAW else 0.0

if TEST_BYPASS_GUARDS and not TEST_DRY_RUN:
    raise RuntimeError("TEST_BYPASS_GUARDS is only allowed with TEST_DRY_RUN=1")

if any([TEST_FORCE_ENTER, TEST_BYPASS_GUARDS, TEST_DRY_RUN, TEST_FAKE_AVAILABLE_USDT > 0]):
    logger.warning(
        "[TEST MODE ENABLED] FORCE_ENTER=%s BYPASS_GUARDS=%s DRY_RUN=%s FAKE_USDT=%s",
        TEST_FORCE_ENTER,
        TEST_BYPASS_GUARDS,
        TEST_DRY_RUN,
        TEST_FAKE_AVAILABLE_USDT,
    )


def _submit_tg_nonblocking(func, msg: str, *, label: str) -> None:
    """
    STRICT:
    - 텔레그램 전송은 매매 실행을 블로킹하면 안 된다.
    - 실패해도 매매 흐름을 깨지 않는다(알림은 비핵심 I/O).
    """
    try:
        ok = submit_async(func, msg, critical=False, label=label)
        if not ok:
            logger.warning("[TG][DROP] queue full label=%s", label)
    except Exception as e:
        logger.warning("[TG][SUBMIT_FAIL] label=%s err=%s", label, f"{type(e).__name__}:{e}")


def _as_float(
    value: Any,
    name: str,
    *,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a number (bool not allowed)")
    try:
        v = float(value)
    except Exception as e:
        raise ValueError(f"{name} must be a number") from e

    if not math.isfinite(v):
        raise ValueError(f"{name} must be finite")

    if min_value is not None and v < min_value:
        raise ValueError(f"{name} must be >= {min_value}")
    if max_value is not None and v > max_value:
        raise ValueError(f"{name} must be <= {max_value}")

    return v


def _opt_float_strict(value: Any, name: str) -> Optional[float]:
    if value is None:
        return None
    return float(_as_float(value, name))


def _meta_pick_first(meta: Dict[str, Any], keys: Tuple[str, ...]) -> Any:
    """
    STRICT:
    - 0/False 같은 값이 '없음'으로 오해되지 않도록, 키 존재 여부로만 선택한다.
    """
    for k in keys:
        if k in meta:
            return meta[k]
    return None


def _require_meta_equity(meta: Dict[str, Any]) -> tuple[float, float, float]:
    if "equity_current_usdt" not in meta:
        raise OrderFailed("signal.meta['equity_current_usdt'] is required (STRICT)")
    if "equity_peak_usdt" not in meta:
        raise OrderFailed("signal.meta['equity_peak_usdt'] is required (STRICT)")
    if "dd_pct" not in meta:
        raise OrderFailed("signal.meta['dd_pct'] is required (STRICT)")

    eq_cur = _as_float(meta.get("equity_current_usdt"), "meta.equity_current_usdt", min_value=0.0)
    eq_peak = _as_float(meta.get("equity_peak_usdt"), "meta.equity_peak_usdt", min_value=0.0)
    dd = _as_float(meta.get("dd_pct"), "meta.dd_pct", min_value=0.0, max_value=100.0)

    if eq_cur <= 0 or eq_peak <= 0:
        raise OrderFailed("meta equity values must be > 0 (STRICT)")
    return float(eq_cur), float(eq_peak), float(dd)


def _deterministic_entry_client_order_id(symbol: str, direction: str, signal_ts_ms: int) -> str:
    sym = str(symbol).upper().strip()
    if not sym:
        raise ValueError("symbol is empty")
    dir_u = str(direction).upper().strip()
    if dir_u not in ("LONG", "SHORT"):
        raise ValueError("direction must be LONG/SHORT")
    if not isinstance(signal_ts_ms, int) or signal_ts_ms <= 0:
        raise ValueError("signal_ts_ms must be int > 0")

    d = "L" if dir_u == "LONG" else "S"
    sym8 = sym[:8]
    cid = f"ent-{sym8}-{d}-{signal_ts_ms}"
    if len(cid) > 36:
        cid = cid[:36]

    try:
        cid.encode("ascii")
    except UnicodeEncodeError as e:
        raise ValueError("generated client order id must be ASCII") from e

    if not cid or len(cid) > 36:
        raise ValueError("generated client order id invalid")

    return cid


def _require_trade_exec_fields(
    trade: Any,
    *,
    soft_mode: bool,
) -> Tuple[str, Optional[str], Optional[str], str, float, float]:
    entry_order_id = getattr(trade, "entry_order_id", None)
    tp_order_id = getattr(trade, "tp_order_id", None)
    sl_order_id = getattr(trade, "sl_order_id", None)

    if not isinstance(entry_order_id, str) or not entry_order_id.strip():
        raise OrderFailed("trade.entry_order_id is required (STRICT)")

    if tp_order_id is not None and (not isinstance(tp_order_id, str) or not tp_order_id.strip()):
        raise OrderFailed("trade.tp_order_id must be str or None (STRICT)")

    if sl_order_id is not None and (not isinstance(sl_order_id, str) or not sl_order_id.strip()):
        raise OrderFailed("trade.sl_order_id must be str or None (STRICT)")

    if not bool(soft_mode):
        if not isinstance(sl_order_id, str) or not sl_order_id.strip():
            raise OrderFailed("soft_mode=False but trade.sl_order_id is missing (STRICT)")

    exchange_position_side = getattr(trade, "exchange_position_side", None)
    if not isinstance(exchange_position_side, str) or not exchange_position_side.strip():
        raise OrderFailed("trade.exchange_position_side is required (STRICT)")
    exchange_position_side = exchange_position_side.strip().upper()

    remaining_qty = getattr(trade, "remaining_qty", None)
    if remaining_qty is None:
        raise OrderFailed("trade.remaining_qty is required (STRICT)")
    remaining_qty_f = _as_float(remaining_qty, "trade.remaining_qty", min_value=0.0)

    realized_pnl_usdt = getattr(trade, "realized_pnl_usdt", None)
    if realized_pnl_usdt is None:
        raise OrderFailed("trade.realized_pnl_usdt is required (STRICT)")
    realized_pnl_usdt_f = _as_float(realized_pnl_usdt, "trade.realized_pnl_usdt")

    return (
        entry_order_id.strip(),
        (tp_order_id.strip() if isinstance(tp_order_id, str) else None),
        (sl_order_id.strip() if isinstance(sl_order_id, str) else None),
        exchange_position_side,
        float(remaining_qty_f),
        float(realized_pnl_usdt_f),
    )


class _SettingsView:
    __slots__ = ("_base", "_overrides")

    def __init__(self, base: Any, overrides: Dict[str, float]):
        object.__setattr__(self, "_base", base)
        object.__setattr__(self, "_overrides", overrides)

    def __getattr__(self, name: str) -> Any:
        overrides = object.__getattribute__(self, "_overrides")
        if name in overrides:
            return overrides[name]
        base = object.__getattribute__(self, "_base")
        return getattr(base, name)

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("SettingsView is read-only")


def _build_guard_settings_view(settings: Any, guard_adjustments: Dict[str, float]) -> _SettingsView:
    if guard_adjustments is None:
        raise OrderFailed("guard_adjustments must not be None (STRICT)")
    if not isinstance(guard_adjustments, dict):
        raise OrderFailed("guard_adjustments must be dict (STRICT)")

    overrides: Dict[str, float] = {}
    for k, v in guard_adjustments.items():
        if not isinstance(k, str) or not k.strip():
            raise ValueError("guard_adjustments key must be non-empty str")
        if not hasattr(settings, k):
            raise OrderFailed(f"guard_adjustments contains unknown settings key: {k}")
        overrides[k] = _as_float(v, f"guard_adjustments.{k}", min_value=0.0)

    return _SettingsView(settings, overrides)


def _resolve_guard_adjustments(signal: Signal) -> Dict[str, float]:
    ga = getattr(signal, "guard_adjustments", None)
    if ga is None:
        return {}
    if not isinstance(ga, dict):
        raise OrderFailed("signal.guard_adjustments must be dict or None (STRICT)")
    return dict(ga)


def _resolve_dynamic_allocation(meta: Dict[str, Any]) -> Optional[float]:
    v = meta.get("dynamic_allocation_ratio")
    if v is None:
        v = meta.get("dynamic_risk_pct")
    if v is None:
        return None
    return _as_float(v, "meta.dynamic_allocation_ratio", min_value=0.0, max_value=1.0)


class ExecutionEngine:
    def __init__(self, settings: Any):
        self.settings = settings
        required = ["symbol", "leverage", "slippage_block_pct", "slippage_stop_engine"]
        missing = [k for k in required if not hasattr(settings, k)]
        if missing:
            raise ValueError(f"settings missing required fields: {missing}")

    def execute(self, signal: Signal) -> Optional[Trade]:
        if not isinstance(signal, Signal):
            raise ValueError("signal must be Signal")

        action = str(signal.action).upper().strip()
        if action not in ("ENTER", "SKIP"):
            raise ValueError(f"signal.action invalid: {signal.action!r}")

        if TEST_FORCE_ENTER:
            action = "ENTER"

        if not _EXECUTION_LOCK.acquire(blocking=False):
            raise StateViolation("Concurrent execute() call detected (STRICT)")

        state: OrderState = OrderState.CREATED

        try:
            meta = signal.meta
            if not isinstance(meta, dict):
                raise OrderFailed("signal.meta must be dict (STRICT)")

            symbol = meta.get("symbol")
            if not isinstance(symbol, str) or not symbol.strip():
                raise OrderFailed("signal.meta['symbol'] is required (STRICT)")
            symbol = symbol.strip().upper()

            regime = meta.get("regime")
            if not isinstance(regime, str) or not regime.strip():
                raise OrderFailed("signal.meta['regime'] is required (STRICT)")
            regime = regime.strip()

            signal_source = meta.get("signal_source")
            if not isinstance(signal_source, str) or not signal_source.strip():
                raise OrderFailed("signal.meta['signal_source'] is required (STRICT)")
            signal_source = signal_source.strip()

            signal_ts_ms = meta.get("signal_ts_ms")
            if not isinstance(signal_ts_ms, int) or signal_ts_ms <= 0:
                raise OrderFailed("signal.meta['signal_ts_ms'] must be int > 0 (STRICT)")

            direction = str(signal.direction).upper().strip()
            if direction not in ("LONG", "SHORT"):
                raise ValueError("signal.direction must be LONG or SHORT")

            if action == "SKIP":
                msg = f"[SKIP] {symbol} {direction} reason={signal.reason}"
                logger.info(msg)
                log_skip_event(
                    symbol=symbol,
                    regime=regime,
                    source="execution_engine",
                    side=direction,
                    reason=str(signal.reason) or "skip",
                    extra={"signal_source": signal_source, "ts_ms": signal_ts_ms},
                )
                _submit_tg_nonblocking(send_skip_tg, msg, label="send_skip_tg")
                return None

            last_price = _as_float(meta.get("last_price"), "meta.last_price", min_value=0.0)
            if last_price <= 0:
                raise OrderFailed("meta.last_price must be > 0 (STRICT)")

            candles_5m = meta.get("candles_5m")
            candles_5m_raw = meta.get("candles_5m_raw")
            extra = meta.get("extra")
            if extra is not None and not isinstance(extra, dict):
                raise OrderFailed("meta.extra must be dict or None (STRICT)")

            dyn_alloc = _resolve_dynamic_allocation(meta)
            if dyn_alloc is not None:
                sig_alloc = _as_float(signal.risk_pct, "signal.risk_pct(allocation_ratio)", min_value=0.0, max_value=1.0)
                if abs(dyn_alloc - sig_alloc) > 1e-12:
                    raise OrderFailed(f"dynamic_allocation mismatch: meta={dyn_alloc} != signal={sig_alloc} (STRICT)")

            manual_ok = check_manual_position_guard(get_balance_detail_func=get_balance_detail, symbol=symbol, latest_ts=float(signal_ts_ms))
            if not manual_ok:
                msg = f"[SKIP] manual_guard_blocked: {symbol}"
                logger.warning(msg)
                log_skip_event(symbol=symbol, regime=regime, source="execution_engine.guard.manual", side=direction, reason="manual_guard_blocked", extra={"ts_ms": signal_ts_ms})
                _submit_tg_nonblocking(send_skip_tg, msg, label="send_skip_tg")
                return None

            avail_usdt = float(TEST_FAKE_AVAILABLE_USDT) if TEST_FAKE_AVAILABLE_USDT > 0 else _as_float(get_available_usdt(), "available_usdt", min_value=0.0)
            if avail_usdt <= 0:
                msg = f"[SKIP] available_usdt<=0: {symbol}"
                logger.warning(msg)
                log_skip_event(symbol=symbol, regime=regime, source="execution_engine.balance", side=direction, reason="balance_zero_or_invalid", extra={"available_usdt": avail_usdt, "ts_ms": signal_ts_ms})
                _submit_tg_nonblocking(send_skip_tg, msg, label="send_skip_tg")
                return None

            best_bid: Optional[float] = None
            best_ask: Optional[float] = None

            guard_adjustments = _resolve_guard_adjustments(signal)
            guard_settings = _build_guard_settings_view(self.settings, guard_adjustments)

            if candles_5m_raw is None:
                raise OrderFailed("meta.candles_5m_raw is required for volume guard (STRICT)")
            vol_ok = check_volume_guard(settings=guard_settings, candles_5m_raw=candles_5m_raw, latest_ts=float(signal_ts_ms), signal_source=signal_source, direction=direction)
            if not vol_ok:
                msg = f"[SKIP] volume_guard_blocked: {symbol}"
                logger.info(msg)
                log_skip_event(symbol=symbol, regime=regime, source="execution_engine.guard.volume", side=direction, reason="volume_guard_blocked", extra={"ts_ms": signal_ts_ms})
                _submit_tg_nonblocking(send_skip_tg, msg, label="send_skip_tg")
                if not TEST_BYPASS_GUARDS:
                    return None

            if candles_5m is None:
                raise OrderFailed("meta.candles_5m is required for price jump guard (STRICT)")
            price_ok = check_price_jump_guard(settings=guard_settings, candles_5m=candles_5m, latest_ts=float(signal_ts_ms), signal_source=signal_source, direction=direction)
            if not price_ok:
                msg = f"[SKIP] price_jump_guard_blocked: {symbol}"
                logger.info(msg)
                log_skip_event(symbol=symbol, regime=regime, source="execution_engine.guard.price_jump", side=direction, reason="price_jump_guard_blocked", extra={"ts_ms": signal_ts_ms})
                _submit_tg_nonblocking(send_skip_tg, msg, label="send_skip_tg")
                if not TEST_BYPASS_GUARDS:
                    return None

            spread_ok, best_bid, best_ask = check_spread_guard(settings=guard_settings, symbol=symbol, latest_ts=float(signal_ts_ms), signal_source=signal_source, direction=direction)
            if not spread_ok:
                msg = f"[SKIP] spread_guard_blocked: {symbol}"
                logger.info(msg)
                log_skip_event(symbol=symbol, regime=regime, source="execution_engine.guard.spread", side=direction, reason="spread_guard_blocked", extra={"ts_ms": signal_ts_ms, "best_bid": best_bid, "best_ask": best_ask})
                _submit_tg_nonblocking(send_skip_tg, msg, label="send_skip_tg")
                if not TEST_BYPASS_GUARDS:
                    return None

            entry_price_hint = float(last_price)
            price_for_qty = float(last_price)
            if best_bid is not None and best_ask is not None:
                bb = float(best_bid)
                ba = float(best_ask)
                if bb > 0 and ba > 0 and ba > bb:
                    mid = (bb + ba) / 2.0
                    if mid > 0:
                        price_for_qty = mid
                    entry_price_hint = ba if direction == "LONG" else bb

            if price_for_qty <= 0:
                raise OrderFailed("price_for_qty must be > 0 (STRICT)")

            spread_pct_snapshot: Optional[float] = None
            if best_bid is not None and best_ask is not None:
                bb = float(best_bid)
                ba = float(best_ask)
                if bb > 0 and ba > 0 and ba > bb:
                    mid = (bb + ba) / 2.0
                    if mid > 0:
                        spread_pct_snapshot = (ba - bb) / mid

            slippage_block_pct = _as_float(getattr(self.settings, "slippage_block_pct"), "settings.slippage_block_pct", min_value=0.0)
            slippage_stop_engine = bool(getattr(self.settings, "slippage_stop_engine"))

            if slippage_block_pct > 0:
                slippage_pct = abs(entry_price_hint - last_price) / last_price
                if slippage_pct > slippage_block_pct:
                    msg = f"[SKIP] {symbol} {direction} slippage_block slippage_pct={slippage_pct}"
                    logger.warning(msg)
                    log_skip_event(symbol=symbol, regime=regime, source="execution_engine.slippage", side=direction, reason="slippage_block", extra={"slippage_pct": slippage_pct, "slippage_block_pct": slippage_block_pct, "ts_ms": signal_ts_ms})
                    _submit_tg_nonblocking(send_skip_tg, msg, label="send_skip_tg")
                    if slippage_stop_engine:
                        raise OrderFailed("slippage_block triggered and slippage_stop_engine=True (STRICT)")
                    if not TEST_BYPASS_GUARDS:
                        return None

            leverage = _as_float(getattr(self.settings, "leverage"), "settings.leverage", min_value=1.0)
            allocation_ratio = _as_float(signal.risk_pct, "signal.risk_pct(allocation_ratio)", min_value=0.0, max_value=1.0)
            tp_pct = _as_float(signal.tp_pct, "signal.tp_pct", min_value=0.0)
            sl_pct = _as_float(signal.sl_pct, "signal.sl_pct", min_value=0.0)

            if allocation_ratio <= 0:
                raise OrderFailed("ENTER requires allocation_ratio(signal.risk_pct) > 0 (STRICT)")
            if tp_pct <= 0:
                raise OrderFailed("ENTER requires signal.tp_pct > 0 (STRICT)")
            if sl_pct <= 0:
                raise OrderFailed("ENTER requires signal.sl_pct > 0 (STRICT)")

            if leverage > 1.0 and allocation_ratio >= 0.8:
                raise OrderFailed(f"allocation_mode violation: leverage({leverage})>1 with large allocation_ratio({allocation_ratio}) (STRICT)")

            notional = avail_usdt * allocation_ratio * leverage
            if notional <= 0:
                raise OrderFailed("notional must be > 0 (STRICT)")

            qty_raw = notional / price_for_qty
            if qty_raw <= 0:
                raise OrderFailed("qty_raw must be > 0 (STRICT)")

            ok, guard_reason, guard_extra = hard_risk_guard_check(
                self.settings,
                symbol=symbol,
                side=direction,
                entry_price=float(entry_price_hint),
                notional=float(notional),
                available_usdt=float(avail_usdt),
            )
            if not ok:
                msg = f"[SKIP] hard_risk_guard_blocked: {guard_reason}"
                logger.warning("%s extra=%s", msg, guard_extra)
                log_skip_event(symbol=symbol, regime=regime, source="execution_engine.risk_guard", side=direction, reason=str(guard_reason), extra=guard_extra)
                _submit_tg_nonblocking(send_skip_tg, f"{msg} {symbol} {direction}", label="send_skip_tg")
                if not TEST_BYPASS_GUARDS:
                    return None

            side_open = "BUY" if direction == "LONG" else "SELL"

            soft_mode = False
            sl_floor_ratio: Optional[float] = None
            if isinstance(extra, dict):
                soft_mode = bool(extra.get("soft_mode") or extra.get("soft") or False)
                if extra.get("sl_floor_ratio") is not None:
                    sl_floor_ratio = _as_float(extra.get("sl_floor_ratio"), "extra.sl_floor_ratio", min_value=0.0)

            log_event(
                "on_entry_submitted",
                symbol=symbol,
                regime=regime,
                source="execution_engine",
                side=direction,
                price=float(entry_price_hint),
                qty=float(qty_raw),
                leverage=float(leverage),
                tp_pct=float(tp_pct),
                sl_pct=float(sl_pct),
                risk_pct=float(allocation_ratio),
                reason="entry_submit",
                extra_json={"signal_source": signal_source, "gpt_reason": str(signal.reason)[:200]},
            )

            log_signal(
                event="ENTRY_SIGNAL",
                symbol=symbol,
                regime=regime,
                source=str(signal_source),
                side=direction,
                price=float(entry_price_hint),
                qty=float(qty_raw),
                leverage=float(leverage),
                tp_pct=float(tp_pct),
                sl_pct=float(sl_pct),
                risk_pct=float(allocation_ratio),
                reason=str(signal.reason) or "enter",
                extra_json={
                    "candle_ts": int(signal_ts_ms),
                    "available_usdt": float(avail_usdt),
                    "notional": float(notional),
                    "regime_score": meta.get("regime_score"),
                    "regime_band": meta.get("regime_band"),
                    "regime_allocation": meta.get("regime_allocation"),
                    "dd_pct": meta.get("dd_pct"),
                    "consecutive_losses": meta.get("consecutive_losses"),
                    "risk_physics_reason": meta.get("risk_physics_reason"),
                    "dynamic_allocation_ratio": dyn_alloc,
                },
            )

            eq_cur_usdt, eq_peak_usdt, dd_pct_v = _require_meta_equity(meta)
            entry_client_order_id = _deterministic_entry_client_order_id(symbol=symbol, direction=direction, signal_ts_ms=signal_ts_ms)

            state = OrderState.SUBMITTED

            entry_score_val = _meta_pick_first(meta, ("entry_score", "entryScore"))
            engine_total_val = _meta_pick_first(meta, ("engine_total", "engine_total_score", "engine_total_score_v2"))

            if TEST_DRY_RUN:
                state = OrderState.FILLED
                entry_price = float(entry_price_hint)
                entry_qty = float(qty_raw)
                entry_ts_dt = datetime.fromtimestamp(float(signal_ts_ms) / 1000.0, tz=timezone.utc)

                log_event(
                    "on_entry_filled",
                    symbol=symbol,
                    regime=regime,
                    source="execution_engine",
                    side=direction,
                    price=float(entry_price),
                    qty=float(entry_qty),
                    leverage=float(leverage),
                    tp_pct=float(tp_pct),
                    sl_pct=float(sl_pct),
                    risk_pct=float(allocation_ratio),
                    reason="entry_filled_simulated",
                    extra_json={"test_dry_run": True},
                    is_test=True,
                )

                trade_id = record_trade_open_returning_id(
                    symbol=symbol,
                    side=str(side_open),
                    qty=float(entry_qty),
                    entry_price=float(entry_price),
                    entry_ts=entry_ts_dt,
                    is_auto=True,
                    regime_at_entry=str(regime),
                    strategy=str(signal_source),
                    entry_score=_opt_float_strict(entry_score_val, "meta.entry_score"),
                    trend_score_at_entry=None,
                    range_score_at_entry=None,
                    leverage=float(leverage),
                    risk_pct=float(allocation_ratio),
                    tp_pct=float(tp_pct),
                    sl_pct=float(sl_pct),
                    note="TEST_DRY_RUN",
                    entry_order_id=None,
                    tp_order_id=None,
                    sl_order_id=None,
                    exchange_position_side="BOTH",
                    remaining_qty=float(entry_qty),
                    realized_pnl_usdt=0.0,
                    reconciliation_status="TEST_DRY_RUN",
                    last_synced_at=entry_ts_dt,
                )

                record_trade_snapshot(
                    trade_id=int(trade_id),
                    symbol=symbol,
                    entry_ts=entry_ts_dt,
                    direction=str(direction),
                    signal_source=str(signal_source),
                    regime=str(regime),
                    entry_score=_opt_float_strict(entry_score_val, "meta.entry_score"),
                    engine_total=_opt_float_strict(engine_total_val, "meta.engine_total"),
                    trend_strength=None,
                    atr_pct=None,
                    volume_zscore=None,
                    depth_ratio=None,
                    spread_pct=spread_pct_snapshot,
                    last_price=float(last_price),
                    risk_pct=float(allocation_ratio),
                    tp_pct=float(tp_pct),
                    sl_pct=float(sl_pct),
                    gpt_action="ENTER",
                    gpt_reason=str(signal.reason or ""),
                    equity_current_usdt=float(eq_cur_usdt),
                    equity_peak_usdt=float(eq_peak_usdt),
                    dd_pct=float(dd_pct_v),
                )

                closed_trade_id = close_latest_open_trade_returning_id(
                    symbol=symbol,
                    side=str(side_open),
                    exit_price=float(entry_price),
                    exit_ts=entry_ts_dt,
                    pnl_usdt=0.0,
                    close_reason="TEST_DRY_RUN",
                    regime_at_exit=str(regime),
                    pnl_pct_futures=0.0,
                    pnl_pct_spot_ref=None,
                    note="TEST_DRY_RUN closed immediately",
                )

                record_trade_exit_snapshot(
                    trade_id=int(closed_trade_id),
                    symbol=symbol,
                    close_ts=entry_ts_dt,
                    close_price=float(entry_price),
                    pnl=0.0,
                    close_reason="TEST_DRY_RUN",
                    exit_atr_pct=None,
                    exit_trend_strength=None,
                    exit_volume_zscore=None,
                    exit_depth_ratio=None,
                )

                _submit_tg_nonblocking(send_tg, f"[TEST_DRY_RUN] DB recorded + closed: {symbol} {direction} trade_id={trade_id}", label="send_tg")
                return None

            trade = open_position_with_tp_sl(
                settings=self.settings,
                symbol=symbol,
                side_open=side_open,
                qty=float(qty_raw),
                entry_price_hint=float(entry_price_hint),
                tp_pct=float(tp_pct),
                sl_pct=float(sl_pct),
                source=str(signal_source),
                soft_mode=bool(soft_mode),
                sl_floor_ratio=sl_floor_ratio,
                entry_client_order_id=str(entry_client_order_id),
            )
            if trade is None:
                raise OrderFailed("open_position_with_tp_sl returned None (STRICT)")

            state = OrderState.FILLED
            try:
                setattr(trade, "order_state", state.value)
            except Exception:
                pass

            entry_order_id, tp_order_id, sl_order_id, pos_side, remaining_qty, realized_pnl_usdt = _require_trade_exec_fields(trade, soft_mode=bool(soft_mode))

            entry_price = getattr(trade, "entry", getattr(trade, "entry_price", None))
            if entry_price is None:
                raise OrderFailed("trade.entry/entry_price is required (STRICT)")
            entry_price = _as_float(entry_price, "trade.entry_price", min_value=0.0)

            entry_qty = getattr(trade, "qty", None)
            if entry_qty is None:
                raise OrderFailed("trade.qty is required (STRICT)")
            entry_qty = _as_float(entry_qty, "trade.qty", min_value=0.0)

            entry_ts_dt = getattr(trade, "entry_ts", None)
            if not isinstance(entry_ts_dt, datetime) or entry_ts_dt.tzinfo is None or entry_ts_dt.tzinfo.utcoffset(entry_ts_dt) is None:
                raise OrderFailed("trade.entry_ts (tz-aware datetime) is required (STRICT)")

            log_event(
                "on_entry_filled",
                symbol=symbol,
                regime=regime,
                source="execution_engine",
                side=direction,
                price=float(entry_price),
                qty=float(entry_qty),
                leverage=float(leverage),
                tp_pct=float(tp_pct),
                sl_pct=float(sl_pct),
                risk_pct=float(allocation_ratio),
                reason="entry_filled",
                extra_json={"signal_source": signal_source, "entry_client_order_id": entry_client_order_id, "entry_order_id": entry_order_id},
            )

            now_sync = datetime.now(timezone.utc)

            trade_id = record_trade_open_returning_id(
                symbol=symbol,
                side=str(side_open),
                qty=float(entry_qty),
                entry_price=float(entry_price),
                entry_ts=entry_ts_dt,
                is_auto=True,
                regime_at_entry=str(regime),
                strategy=str(signal_source),
                entry_score=_opt_float_strict(entry_score_val, "meta.entry_score"),
                trend_score_at_entry=None,
                range_score_at_entry=None,
                leverage=float(leverage),
                risk_pct=float(allocation_ratio),
                tp_pct=float(tp_pct),
                sl_pct=float(sl_pct),
                note=f"gpt_reason={str(signal.reason or '')[:180]}",
                entry_order_id=str(entry_order_id),
                tp_order_id=(str(tp_order_id) if tp_order_id is not None else None),
                sl_order_id=(str(sl_order_id) if sl_order_id is not None else None),
                exchange_position_side=str(pos_side),
                remaining_qty=float(remaining_qty),
                realized_pnl_usdt=float(realized_pnl_usdt),
                reconciliation_status="OK",
                last_synced_at=now_sync,
            )

            try:
                setattr(trade, "id", int(trade_id))
            except Exception:
                pass

            record_trade_snapshot(
                trade_id=int(trade_id),
                symbol=symbol,
                entry_ts=entry_ts_dt,
                direction=str(direction),
                signal_source=str(signal_source),
                regime=str(regime),
                entry_score=_opt_float_strict(entry_score_val, "meta.entry_score"),
                engine_total=_opt_float_strict(engine_total_val, "meta.engine_total"),
                trend_strength=None,
                atr_pct=None,
                volume_zscore=None,
                depth_ratio=None,
                spread_pct=spread_pct_snapshot,
                last_price=float(last_price),
                risk_pct=float(allocation_ratio),
                tp_pct=float(tp_pct),
                sl_pct=float(sl_pct),
                gpt_action="ENTER",
                gpt_reason=str(signal.reason or ""),
                equity_current_usdt=float(eq_cur_usdt),
                equity_peak_usdt=float(eq_peak_usdt),
                dd_pct=float(dd_pct_v),
            )

            _submit_tg_nonblocking(
                send_tg,
                f"[ENTRY][FILLED] {symbol} {direction} "
                f"alloc={allocation_ratio*100:.1f}% tp={tp_pct*100:.2f}% sl={sl_pct*100:.2f}% "
                f"price={float(entry_price):.4f} qty={float(entry_qty):.6f} src={signal_source}",
                label="send_tg",
            )

            return trade

        finally:
            _EXECUTION_LOCK.release()