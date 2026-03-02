"""
========================================================
FILE: state/trader_state.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================

역할
- in-memory Trade 상태와 거래소 포지션 상태를 동기화한다.
- “청산/부분청산”을 거래소 확정 데이터로만 판정하고, DB에 영속화한다.

핵심 원칙 (STRICT)
- 예외를 삼키지 않는다.
- 폴백 금지:
  - 누락/모호/정합 실패 시 즉시 예외
  - “첫 번째 row 선택”, “0이면 qty로 대체”, “시간 없으면 임의로 보정” 같은 추정 금지
- 민감정보(API키/토큰/DB비번 등) 로그/출력 금지

PATCH NOTES — 2026-03-02 (PATCH)
- (상태머신 연동) Trade.lifecycle_state 추가 및 check_closes에서 상태 검증/전이 적용
  - ENTERED 상태만 close-check 허용
  - 완전 청산 감지 시 SYNC_SET_CLOSED 전이
- (NO-FALLBACK 강화)
  - Trade.remaining_qty / last_synced_at / exchange_position_side 필수화(0/None 자동 보정 제거)
  - positionRisk row 선택 시 “첫 row 폴백” 제거(유효 후보 없으면 즉시 예외)
  - partial close reconciliation window 생성 시 start_ms 폴백 제거(유효하지 않으면 즉시 예외)
- (9점대 핵심) 청산 요약 orderId 확정(STRICT)
  - TP/SL orderId가 있으면 FILLED + userTrades(orderId)로 요약
  - 그 외 케이스는 userTrades를 orderId로 그룹핑하여 “유일” 후보만 허용
========================================================
"""

from __future__ import annotations

import math
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from execution.exchange_api import fetch_open_positions, req
from execution.exceptions import OrderFailed, StateViolation, SyncMismatch
from execution.state_machine import TradeEvent, TradeLifecycleState, apply_event, get_state
from infra.telelog import log, send_tg
from settings import load_settings
from state.db_writer import close_latest_open_trade_returning_id, record_trade_exit_snapshot

SET = load_settings()


# ─────────────────────────────────────────────────────────────
# Strict helpers
# ─────────────────────────────────────────────────────────────
def _require_nonempty_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        raise OrderFailed(f"{name} is required (STRICT)")
    return s


def _require_float(v: Any, name: str) -> float:
    if v is None:
        raise OrderFailed(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        raise OrderFailed(f"{name} must be numeric (bool not allowed) (STRICT)")
    try:
        f = float(v)
    except Exception as e:
        raise OrderFailed(f"{name} must be numeric: {e} (STRICT)") from e
    if not math.isfinite(f):
        raise OrderFailed(f"{name} must be finite (STRICT)")
    return float(f)


def _get_env_required_float(name: str) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        raise OrderFailed(f"required env var missing: {name} (STRICT)")
    try:
        v = float(raw)
    except Exception as e:
        raise OrderFailed(f"invalid env var {name}: {e} (STRICT)") from e
    if not math.isfinite(v):
        raise OrderFailed(f"invalid env var {name}: not finite (STRICT)")
    return float(v)


def _epoch_ms_to_dt_utc(ms: int) -> datetime:
    if not isinstance(ms, int) or ms <= 0:
        raise OrderFailed("epoch ms must be int > 0 (STRICT)")
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def _dt_to_epoch_ms_utc(dt: datetime) -> int:
    if not isinstance(dt, datetime):
        raise OrderFailed("dt must be datetime (STRICT)")
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        raise OrderFailed("dt must be timezone-aware (STRICT)")
    ms = int(dt.timestamp() * 1000)
    if ms <= 0:
        raise OrderFailed("dt timestamp invalid (STRICT)")
    return ms


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ─────────────────────────────────────────────────────────────
# Trade (in-memory)
# ─────────────────────────────────────────────────────────────
@dataclass
class Trade:
    symbol: str
    side: str  # BUY/SELL 또는 LONG/SHORT
    qty: float
    entry_price: float
    leverage: int
    source: str = "GPT"

    tp_price: float = 0.0
    sl_price: float = 0.0
    ts: float = 0.0

    entry_order_id: Optional[str] = None
    tp_order_id: Optional[str] = None
    sl_order_id: Optional[str] = None
    close_order_id: Optional[str] = None

    client_entry_id: Optional[str] = None
    id: int = 0

    # legacy alias
    entry: float = 0.0

    # lifecycle (상태머신)
    lifecycle_state: TradeLifecycleState = TradeLifecycleState.ENTERED
    last_state_change_reason: str = ""
    last_state_change_at: Optional[datetime] = None

    # close state
    is_open: bool = True
    close_reason: str = ""
    close_ts: float = 0.0

    # 운영형 실행/복구 필드 (STRICT: 기본값 자동 보정 금지)
    exchange_position_side: str = ""  # BOTH / LONG / SHORT (필수)
    remaining_qty: float = 0.0        # 필수(>0)
    realized_pnl_usdt: float = 0.0    # 필수(초기 0 가능, 음수 가능)
    reconciliation_status: str = "INIT"
    last_synced_at: Optional[datetime] = None  # 필수(tz-aware)

    def __post_init__(self) -> None:
        self.symbol = _require_nonempty_str(self.symbol, "trade.symbol").upper()

        if self.qty <= 0 or not math.isfinite(float(self.qty)):
            raise OrderFailed("trade.qty must be finite > 0 (STRICT)")
        if self.entry_price <= 0 or not math.isfinite(float(self.entry_price)):
            raise OrderFailed("trade.entry_price must be finite > 0 (STRICT)")
        if int(self.leverage) < 1:
            raise OrderFailed("trade.leverage must be >= 1 (STRICT)")

        # legacy alias sync (허용)
        if self.entry_price > 0 and self.entry == 0:
            self.entry = float(self.entry_price)
        if self.entry > 0 and self.entry_price == 0:
            self.entry_price = float(self.entry)

        ps = str(self.exchange_position_side or "").upper().strip()
        if ps not in ("BOTH", "LONG", "SHORT"):
            raise OrderFailed(f"trade.exchange_position_side must be BOTH/LONG/SHORT (STRICT), got={ps!r}")
        self.exchange_position_side = ps

        if self.remaining_qty <= 0 or not math.isfinite(float(self.remaining_qty)):
            raise OrderFailed("trade.remaining_qty must be finite > 0 (STRICT)")
        if float(self.remaining_qty) - float(self.qty) > 1e-12:
            raise OrderFailed("trade.remaining_qty must be <= trade.qty (STRICT)")

        if not math.isfinite(float(self.realized_pnl_usdt)):
            raise OrderFailed("trade.realized_pnl_usdt must be finite (STRICT)")

        if self.last_synced_at is None:
            raise OrderFailed("trade.last_synced_at is required (STRICT)")
        if self.last_synced_at.tzinfo is None or self.last_synced_at.tzinfo.utcoffset(self.last_synced_at) is None:
            raise OrderFailed("trade.last_synced_at must be timezone-aware (STRICT)")

        # 상태머신: 열린 트레이드는 ENTERED여야 한다.
        st = get_state(self)
        if self.is_open and st != TradeLifecycleState.ENTERED:
            raise StateViolation(f"open Trade must be ENTERED, got {st.value}")

    def calculate_pnl(self, current_price: float) -> float:
        cp = _require_float(current_price, "current_price")
        side_u = str(self.side).upper().strip()
        if side_u in ("BUY", "LONG"):
            return (cp - float(self.entry_price)) * float(self.qty)
        if side_u in ("SELL", "SHORT"):
            return (float(self.entry_price) - cp) * float(self.qty)
        raise OrderFailed(f"invalid trade.side: {side_u!r} (STRICT)")

    def calculate_pnl_pct(self, current_price: float) -> float:
        cp = _require_float(current_price, "current_price")
        ep = float(self.entry_price)
        if ep <= 0:
            raise OrderFailed("entry_price must be > 0 (STRICT)")
        side_u = str(self.side).upper().strip()
        if side_u in ("BUY", "LONG"):
            return (cp - ep) / ep * 100.0
        if side_u in ("SELL", "SHORT"):
            return (ep - cp) / ep * 100.0
        raise OrderFailed(f"invalid trade.side: {side_u!r} (STRICT)")


class TraderState:
    def __init__(self) -> None:
        self.tp_sl_reset_failures: int = 0

    def record_tp_sl_reset_failure(self) -> None:
        self.tp_sl_reset_failures += 1

    def reset_tp_sl_failures(self) -> None:
        self.tp_sl_reset_failures = 0

    def should_stop_bot(self) -> bool:
        max_fail = int(getattr(SET, "max_tp_sl_reset_failures", 999999))
        if self.tp_sl_reset_failures >= max_fail:
            send_tg(
                "🚫 TP/SL 재설정 실패가 누적되었습니다.\n"
                f"- 실패 횟수: {self.tp_sl_reset_failures}\n"
                f"- 허용 한도: {max_fail}\n"
                "STRICT 모드: 운영자가 즉시 확인하세요."
            )
            return True
        return False


# ─────────────────────────────────────────────────────────────
# Slippage (legacy 유지)
# ─────────────────────────────────────────────────────────────
def evaluate_slippage(
    expected_entry: float,
    actual_entry: float,
    tick_size: Optional[float] = None,
    max_ticks: Optional[int] = None,
    max_notional_pct: Optional[float] = None,
) -> Tuple[bool, str]:
    exp = _require_float(expected_entry, "expected_entry")
    act = _require_float(actual_entry, "actual_entry")

    if tick_size is None:
        raise OrderFailed("tick_size is required (symbol filter) (STRICT)")
    tick = _require_float(tick_size, "tick_size")
    if tick <= 0:
        raise OrderFailed("tick_size must be > 0 (STRICT)")

    if max_ticks is None:
        mt = getattr(SET, "slippage_max_ticks", None)
        max_ticks = int(mt) if mt is not None else int(_get_env_required_float("SLIPPAGE_MAX_TICKS"))

    if max_notional_pct is None:
        mnp = getattr(SET, "slippage_max_notional_pct", None)
        max_notional_pct = float(mnp) if mnp is not None else float(_get_env_required_float("SLIPPAGE_MAX_NOTIONAL_PCT"))

    if max_ticks <= 0:
        raise OrderFailed("max_ticks must be > 0 (STRICT)")
    if max_notional_pct <= 0:
        raise OrderFailed("max_notional_pct must be > 0 (STRICT)")

    diff = abs(act - exp)
    ticks = int(diff / tick)

    if exp <= 0:
        raise OrderFailed("expected_entry must be > 0 (STRICT)")

    notional_pct = (diff / exp) * 100.0

    if ticks > max_ticks:
        return False, f"slippage too large: ticks={ticks} > max_ticks={max_ticks}"
    if notional_pct > max_notional_pct:
        return False, (
            f"slippage too large: notional_pct={notional_pct:.4f}% "
            f"> max_notional_pct={max_notional_pct:.4f}%"
        )
    return True, "OK"


def check_entry_slippage_once(trade: Trade, entry_price: float) -> bool:
    from execution.order_executor import close_position_market, get_symbol_filters

    sym = _require_nonempty_str(trade.symbol, "trade.symbol").upper()
    expected_entry = _require_float(entry_price, "entry_price")

    filters = get_symbol_filters(sym)

    tick_size_val: Any = None
    if hasattr(filters, "tick_size"):
        tick_size_val = getattr(filters, "tick_size")
    elif isinstance(filters, dict):
        tick_size_val = filters.get("tickSize") or filters.get("tick_size")

    if tick_size_val is None:
        raise OrderFailed(f"tick_size not found in symbol filters: symbol={sym} (STRICT)")

    tick_size = _require_float(tick_size_val, "tick_size")
    if tick_size <= 0:
        raise OrderFailed(f"tick_size must be > 0: symbol={sym}, value={tick_size} (STRICT)")

    max_ticks = getattr(SET, "slippage_max_ticks", None)
    max_ticks = int(max_ticks) if max_ticks is not None else int(_get_env_required_float("SLIPPAGE_MAX_TICKS"))

    max_notional_pct = getattr(SET, "slippage_max_notional_pct", None)
    max_notional_pct = float(max_notional_pct) if max_notional_pct is not None else float(_get_env_required_float("SLIPPAGE_MAX_NOTIONAL_PCT"))

    stop_on_heavy = bool(int(os.getenv("STOP_ON_HEAVY_SLIPPAGE", "0")))

    positions = fetch_open_positions(sym)
    if not isinstance(positions, list):
        raise OrderFailed("fetch_open_positions returned non-list (STRICT)")

    pos = None
    for p in positions:
        if str(p.get("symbol", "")).upper() == sym.upper():
            pos = p
            break
    if pos is None:
        raise OrderFailed(f"open position not found for slippage check: symbol={sym} (STRICT)")

    actual_entry_raw = pos.get("entryPrice") or pos.get("avgEntryPrice") or pos.get("entry_price")
    actual_entry = _require_float(actual_entry_raw, "actual_entry_price")

    ok, reason = evaluate_slippage(
        expected_entry=expected_entry,
        actual_entry=actual_entry,
        tick_size=float(tick_size),
        max_ticks=max_ticks,
        max_notional_pct=max_notional_pct,
    )
    if ok:
        return True

    msg = (
        "⚠️ 진입 슬리피지 경고\n"
        f"- 심볼: {sym}\n"
        f"- 기대 진입가: {expected_entry}\n"
        f"- 실제 진입가: {actual_entry}\n"
        f"- 사유: {reason}"
    )
    send_tg(msg)
    log(f"[SLIPPAGE] {msg}")

    if stop_on_heavy:
        side_u = str(trade.side).upper().strip()
        if side_u == "LONG":
            side_u = "BUY"
        elif side_u == "SHORT":
            side_u = "SELL"
        send_tg("🛑 설정(STOP_ON_HEAVY_SLIPPAGE=1)에 따라 즉시 시장가로 포지션을 정리합니다.")
        close_position_market(symbol=sym, side=side_u, qty=float(trade.qty))

    return False


# ─────────────────────────────────────────────────────────────
# Close summary (STRICT)
# ─────────────────────────────────────────────────────────────
class CloseSummaryError(SyncMismatch):
    """거래소 확정값으로 청산 요약을 만들 수 없을 때 발생."""


def _float_strict(v: Any, name: str) -> float:
    if v is None:
        raise CloseSummaryError(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        raise CloseSummaryError(f"{name} must be numeric (bool not allowed) (STRICT)")
    try:
        f = float(v)
    except Exception as e:
        raise CloseSummaryError(f"{name} must be numeric: {e} (STRICT)") from e
    if not math.isfinite(f):
        raise CloseSummaryError(f"{name} must be finite (STRICT)")
    return float(f)


def _int_strict(v: Any, name: str) -> int:
    if v is None:
        raise CloseSummaryError(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        raise CloseSummaryError(f"{name} must be int (bool not allowed) (STRICT)")
    try:
        return int(v)
    except Exception as e:
        raise CloseSummaryError(f"{name} must be int: {e} (STRICT)") from e


def _fetch_position_risk_rows_strict(symbol: str) -> List[Dict[str, Any]]:
    sym = _require_nonempty_str(symbol, "symbol").upper()
    data = req("GET", "/fapi/v2/positionRisk", params={"symbol": sym}, private=True)
    if not isinstance(data, list):
        raise CloseSummaryError("positionRisk response must be list (STRICT)")
    out: List[Dict[str, Any]] = [r for r in data if isinstance(r, dict) and str(r.get("symbol") or "").upper() == sym]
    if not out:
        raise CloseSummaryError("positionRisk returned empty (STRICT)")
    return out


def _trade_direction_strict(trade: Trade) -> str:
    side = str(trade.side or "").upper().strip()
    if side in ("BUY", "LONG"):
        return "LONG"
    if side in ("SELL", "SHORT"):
        return "SHORT"
    raise CloseSummaryError(f"invalid trade.side: {side!r} (STRICT)")


def _trade_entry_side_db(trade: Trade) -> str:
    side = str(trade.side or "").upper().strip()
    if side in ("BUY", "SELL"):
        return side
    if side == "LONG":
        return "BUY"
    if side == "SHORT":
        return "SELL"
    raise CloseSummaryError(f"invalid trade.side: {side!r} (STRICT)")


def _close_side_for_trade(trade: Trade) -> str:
    side_u = str(trade.side or "").upper().strip()
    if side_u in ("BUY", "LONG"):
        return "SELL"
    if side_u in ("SELL", "SHORT"):
        return "BUY"
    raise CloseSummaryError(f"invalid trade.side: {side_u!r} (STRICT)")


def _pick_position_row_for_trade(rows: List[Dict[str, Any]], trade: Trade) -> Dict[str, Any]:
    if not rows:
        raise CloseSummaryError("positionRisk rows empty (STRICT)")

    wanted = _trade_direction_strict(trade)

    matches = [r for r in rows if str(r.get("positionSide") or "").upper() == wanted]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise CloseSummaryError("multiple positionRisk rows match positionSide (ambiguous) (STRICT)")

    both = [r for r in rows if str(r.get("positionSide") or "").upper() in ("BOTH", "")]
    if len(both) == 1:
        return both[0]
    if len(both) > 1:
        raise CloseSummaryError("multiple positionRisk rows for BOTH (ambiguous) (STRICT)")

    raise CloseSummaryError("cannot select positionRisk row for trade (STRICT)")


def _fetch_order_strict(symbol: str, order_id: str) -> Dict[str, Any]:
    sym = _require_nonempty_str(symbol, "symbol").upper()
    oid = _int_strict(order_id, "order_id")
    data = req("GET", "/fapi/v1/order", params={"symbol": sym, "orderId": oid}, private=True)
    if not isinstance(data, dict):
        raise CloseSummaryError("order response must be dict (STRICT)")
    return data


def _fetch_user_trades_strict(symbol: str, *, start_ms: int, end_ms: int, limit: int = 1000) -> List[Dict[str, Any]]:
    sym = _require_nonempty_str(symbol, "symbol").upper()
    if start_ms <= 0 or end_ms <= 0 or end_ms < start_ms:
        raise CloseSummaryError("invalid userTrades time range (STRICT)")
    if not isinstance(limit, int) or limit <= 0 or limit > 1000:
        raise CloseSummaryError("userTrades limit must be int in [1, 1000] (STRICT)")

    data = req(
        "GET",
        "/fapi/v1/userTrades",
        params={"symbol": sym, "startTime": int(start_ms), "endTime": int(end_ms), "limit": int(limit)},
        private=True,
    )
    if not isinstance(data, list):
        raise CloseSummaryError("userTrades response must be list (STRICT)")
    return [r for r in data if isinstance(r, dict)]


def _summarize_user_trades_for_order(trades: List[Dict[str, Any]], *, order_id: int) -> Dict[str, Any]:
    qty_sum = 0.0
    px_qty_sum = 0.0
    realized_sum = 0.0
    fee_usdt_sum = 0.0
    last_time_ms: Optional[int] = None

    for r in trades:
        oid = r.get("orderId")
        if oid is None:
            continue
        if int(oid) != int(order_id):
            continue

        qty = _float_strict(r.get("qty"), "userTrade.qty")
        price = _float_strict(r.get("price"), "userTrade.price")
        realized = _float_strict(r.get("realizedPnl"), "userTrade.realizedPnl")
        commission = _float_strict(r.get("commission"), "userTrade.commission")
        commission_asset = str(r.get("commissionAsset") or "").upper()

        t_ms = r.get("time")
        t_ms_i = int(t_ms) if t_ms is not None else None

        if qty <= 0 or price <= 0:
            raise CloseSummaryError("userTrade.qty/price must be > 0 (STRICT)")

        qty_sum += qty
        px_qty_sum += price * qty
        realized_sum += realized
        if commission_asset == "USDT":
            fee_usdt_sum += commission

        if t_ms_i is not None:
            if last_time_ms is None or t_ms_i > last_time_ms:
                last_time_ms = t_ms_i

    if qty_sum <= 0:
        raise CloseSummaryError("no fills for orderId (STRICT)")

    avg_price = px_qty_sum / qty_sum
    if avg_price <= 0 or not math.isfinite(avg_price):
        raise CloseSummaryError("avg_price invalid (STRICT)")

    pnl_net = realized_sum - fee_usdt_sum
    out: Dict[str, Any] = {
        "qty": float(qty_sum),
        "avg_price": float(avg_price),
        "pnl": float(pnl_net),
        "fee": float(fee_usdt_sum),
        "realizedPnl": float(realized_sum),
    }
    if last_time_ms is not None:
        out["close_time"] = int(last_time_ms)
    return out


def _group_user_trades_by_order_id(trades: List[Dict[str, Any]], *, side: str) -> Dict[int, List[Dict[str, Any]]]:
    side_u = str(side).upper().strip()
    out: Dict[int, List[Dict[str, Any]]] = {}
    for r in trades:
        if str(r.get("side") or "").upper().strip() != side_u:
            continue
        oid = r.get("orderId")
        if oid is None:
            continue
        out.setdefault(int(oid), []).append(r)
    return out


def _infer_unique_close_order_id_strict(
    trade: Trade,
    trades_window: List[Dict[str, Any]],
    *,
    expected_qty: float,
    tolerance_ratio: float,
) -> Tuple[int, Dict[str, Any]]:
    close_side = _close_side_for_trade(trade)
    groups = _group_user_trades_by_order_id(trades_window, side=close_side)

    if expected_qty <= 0 or not math.isfinite(expected_qty):
        raise CloseSummaryError("expected_qty invalid (STRICT)")

    if tolerance_ratio <= 0 or not math.isfinite(tolerance_ratio):
        raise CloseSummaryError("tolerance_ratio must be finite > 0 (STRICT)")

    tol = abs(expected_qty) * float(tolerance_ratio)
    if tol <= 0:
        raise CloseSummaryError("tolerance invalid (STRICT)")

    candidates: List[Tuple[int, Dict[str, Any]]] = []
    for oid in groups.keys():
        summary = _summarize_user_trades_for_order(trades_window, order_id=int(oid))
        qty = _float_strict(summary.get("qty"), "summary.qty")
        if abs(qty - expected_qty) <= tol:
            candidates.append((int(oid), summary))

    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) == 0:
        raise CloseSummaryError("cannot infer close orderId uniquely (no candidates) (STRICT)")
    raise CloseSummaryError("cannot infer close orderId uniquely (multiple candidates) (STRICT)")


def build_close_summary_strict(
    trade: Trade,
    *,
    now_ms: Optional[int] = None,
    lookback_sec: int = 6 * 3600,
) -> Tuple[str, Dict[str, Any]]:
    sym = _require_nonempty_str(trade.symbol, "trade.symbol").upper()
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    if now_ms <= 0:
        raise CloseSummaryError("now_ms invalid (STRICT)")
    if lookback_sec <= 0:
        raise CloseSummaryError("lookback_sec must be > 0 (STRICT)")

    tp_id = str(trade.tp_order_id or "").strip()
    sl_id = str(trade.sl_order_id or "").strip()
    close_id = str(trade.close_order_id or "").strip()

    filled: List[Tuple[str, Dict[str, Any]]] = []
    for oid in (tp_id, sl_id):
        if not oid:
            continue
        o = _fetch_order_strict(sym, oid)
        status = str(o.get("status") or "").upper()
        if status == "FILLED":
            filled.append((oid, o))

    if len(filled) == 1:
        close_oid, close_order = filled[0]
        reason = "TP" if close_oid == tp_id else "SL"

        close_time_ms = None
        for k in ("updateTime", "time", "transactTime"):
            if close_order.get(k) is not None:
                close_time_ms = int(close_order.get(k))
                break
        if close_time_ms is None or close_time_ms <= 0:
            # 거래소가 시간을 주지 않으면 “감지 시각(now_ms)”를 기록한다(추정값이 아니라 관측 시각).
            close_time_ms = now_ms

        win_ms = 2 * 3600 * 1000
        start_ms = max(1, close_time_ms - win_ms)
        end_ms = close_time_ms + win_ms
        user_trades = _fetch_user_trades_strict(sym, start_ms=start_ms, end_ms=end_ms)
        summary = _summarize_user_trades_for_order(user_trades, order_id=_int_strict(close_oid, "close_order_id"))
        return reason, summary

    if len(filled) > 1:
        raise CloseSummaryError("multiple close orders FILLED (ambiguous) (STRICT)")

    if close_id:
        o = _fetch_order_strict(sym, close_id)
        status = str(o.get("status") or "").upper()
        if status != "FILLED":
            raise CloseSummaryError(f"close_order_id present but not FILLED (status={status}) (STRICT)")

        close_time_ms = None
        for k in ("updateTime", "time", "transactTime"):
            if o.get(k) is not None:
                close_time_ms = int(o.get(k))
                break
        if close_time_ms is None or close_time_ms <= 0:
            close_time_ms = now_ms

        win_ms = 2 * 3600 * 1000
        start_ms = max(1, close_time_ms - win_ms)
        end_ms = close_time_ms + win_ms
        user_trades = _fetch_user_trades_strict(sym, start_ms=start_ms, end_ms=end_ms)
        summary = _summarize_user_trades_for_order(user_trades, order_id=_int_strict(close_id, "close_order_id"))
        return "MANUAL", summary

    expected_qty = _float_strict(trade.qty, "trade.qty")
    start_ms = max(1, now_ms - int(lookback_sec * 1000))
    end_ms = now_ms
    user_trades = _fetch_user_trades_strict(sym, start_ms=start_ms, end_ms=end_ms)

    oid, summary = _infer_unique_close_order_id_strict(
        trade,
        user_trades,
        expected_qty=float(expected_qty),
        tolerance_ratio=0.01,
    )
    trade.close_order_id = str(oid)
    return "MANUAL", summary


def _calc_pnl_pct_futures(trade: Trade, pnl_usdt: float) -> Optional[float]:
    entry_price = float(trade.entry_price)
    qty = float(trade.qty)
    if entry_price <= 0 or qty <= 0:
        return None
    notional = abs(entry_price * qty)
    if notional <= 0:
        return None
    return float(pnl_usdt) / notional * 100.0


def _persist_close_to_db_strict(*, trade: Trade, reason: str, summary: Dict[str, Any]) -> int:
    sym = _require_nonempty_str(trade.symbol, "trade.symbol").upper()
    entry_side_db = _trade_entry_side_db(trade)

    if not isinstance(summary, dict):
        raise CloseSummaryError("summary must be dict (STRICT)")
    for k in ("avg_price", "pnl"):
        if k not in summary:
            raise CloseSummaryError(f"summary missing required key: {k} (STRICT)")

    exit_price = _float_strict(summary.get("avg_price"), "summary.avg_price")
    pnl_usdt = _float_strict(summary.get("pnl"), "summary.pnl")
    if exit_price <= 0:
        raise CloseSummaryError("summary.avg_price must be > 0 (STRICT)")

    close_time_ms = summary.get("close_time")
    if close_time_ms is not None:
        exit_ts_dt = _epoch_ms_to_dt_utc(_int_strict(close_time_ms, "summary.close_time"))
    else:
        exit_ts_dt = _utc_now()

    close_reason = _require_nonempty_str(reason, "reason")[:32]
    pnl_pct_futures = _calc_pnl_pct_futures(trade, pnl_usdt)

    trade_id = close_latest_open_trade_returning_id(
        symbol=sym,
        side=entry_side_db,
        exit_price=float(exit_price),
        exit_ts=exit_ts_dt,
        pnl_usdt=float(pnl_usdt),
        close_reason=str(close_reason),
        regime_at_exit=None,
        pnl_pct_futures=pnl_pct_futures,
        pnl_pct_spot_ref=None,
        note=None,
    )

    record_trade_exit_snapshot(
        trade_id=int(trade_id),
        symbol=sym,
        close_ts=exit_ts_dt,
        close_price=float(exit_price),
        pnl=float(pnl_usdt),
        close_reason=str(close_reason),
        exit_atr_pct=None,
        exit_trend_strength=None,
        exit_volume_zscore=None,
        exit_depth_ratio=None,
    )

    return int(trade_id)


def _strict_reconcile_partial_close(
    trade: Trade,
    *,
    symbol: str,
    delta_qty: float,
    start_ms: int,
    end_ms: int,
) -> float:
    if delta_qty <= 0 or not math.isfinite(delta_qty):
        raise CloseSummaryError("delta_qty invalid (STRICT)")

    trades_window = _fetch_user_trades_strict(symbol, start_ms=start_ms, end_ms=end_ms)
    close_side = _close_side_for_trade(trade)

    qty_sum = 0.0
    realized_sum = 0.0
    fee_usdt_sum = 0.0

    for r in trades_window:
        if str(r.get("side") or "").upper().strip() != close_side:
            continue

        qty = _float_strict(r.get("qty"), "userTrade.qty")
        realized = _float_strict(r.get("realizedPnl"), "userTrade.realizedPnl")
        commission = _float_strict(r.get("commission"), "userTrade.commission")
        commission_asset = str(r.get("commissionAsset") or "").upper()

        if qty <= 0:
            raise CloseSummaryError("userTrade.qty must be > 0 (STRICT)")

        qty_sum += qty
        realized_sum += realized
        if commission_asset == "USDT":
            fee_usdt_sum += commission

    tol = max(delta_qty * 0.01, 1e-12)
    if abs(qty_sum - delta_qty) > tol:
        raise CloseSummaryError(f"partial close qty mismatch: expected_delta={delta_qty}, got={qty_sum} (STRICT)")

    pnl_delta = realized_sum - fee_usdt_sum
    if not math.isfinite(pnl_delta):
        raise CloseSummaryError("partial pnl_delta not finite (STRICT)")
    return float(pnl_delta)


# ─────────────────────────────────────────────────────────────
# Main close checker
# ─────────────────────────────────────────────────────────────
def check_closes(open_trades: List[Trade], trader_state: TraderState) -> Tuple[List[Trade], List[Dict[str, Any]]]:
    _ = trader_state

    if not open_trades:
        return [], []

    if len(open_trades) != 1:
        raise StateViolation("STRICT: multiple open_trades is not supported (single-trade only)")

    t = open_trades[0]

    # 상태머신: ENTERED만 허용
    if get_state(t) != TradeLifecycleState.ENTERED:
        raise StateViolation(f"check_closes requires ENTERED state, got {get_state(t).value}")

    symbol = _require_nonempty_str(t.symbol, "open_trades[0].symbol").upper()

    pos_rows = _fetch_position_risk_rows_strict(symbol)
    row = _pick_position_row_for_trade(pos_rows, t)

    pos_qty = _float_strict(row.get("positionAmt"), "positionRisk.positionAmt")
    abs_qty = abs(pos_qty)

    eps = 1e-12

    prev_remaining = _require_float(t.remaining_qty, "trade.remaining_qty")
    if prev_remaining <= 0:
        raise CloseSummaryError("trade.remaining_qty must be > 0 (STRICT)")

    # 부분청산: 포지션이 남아있는데 수량이 줄었다
    if abs_qty > eps and abs_qty < prev_remaining - eps:
        delta = prev_remaining - abs_qty

        if t.last_synced_at is None:
            raise CloseSummaryError("trade.last_synced_at is required for partial reconciliation (STRICT)")
        start_ms = _dt_to_epoch_ms_utc(t.last_synced_at) - 2000
        now_dt = _utc_now()
        end_ms = _dt_to_epoch_ms_utc(now_dt) + 2000
        if start_ms <= 0:
            raise CloseSummaryError("partial reconciliation start_ms invalid (STRICT)")

        pnl_delta = _strict_reconcile_partial_close(
            t,
            symbol=symbol,
            delta_qty=float(delta),
            start_ms=int(start_ms),
            end_ms=int(end_ms),
        )

        t.realized_pnl_usdt = float(_require_float(t.realized_pnl_usdt, "trade.realized_pnl_usdt") + pnl_delta)
        t.remaining_qty = float(abs_qty)
        t.last_synced_at = now_dt
        t.reconciliation_status = "PARTIAL_OK"

        send_tg(
            f"[PARTIAL_CLOSE] {symbol} side={t.side} "
            f"delta={delta:.6f} remaining={t.remaining_qty:.6f} pnl_delta={pnl_delta:.4f} pnl_total={t.realized_pnl_usdt:.4f}"
        )

        return [t], []

    # 완전 청산: positionAmt = 0
    if abs_qty < eps:
        reason, summary = build_close_summary_strict(t)

        trade_id = _persist_close_to_db_strict(trade=t, reason=str(reason), summary=summary)
        if t.id <= 0:
            t.id = int(trade_id)

        # 상태 전이 (SYNC 기반 확정)
        apply_event(t, TradeEvent.SYNC_SET_CLOSED, reason="exchange_position_zero")

        t.is_open = False
        t.close_reason = str(reason)
        t.close_ts = _utc_now().timestamp()
        t.remaining_qty = 0.0
        t.reconciliation_status = "CLOSED_OK"
        t.last_synced_at = _utc_now()

        return [], [{"trade": t, "reason": reason, "summary": summary}]

    # 변화 없음
    return [t], []


__all__ = [
    "Trade",
    "TraderState",
    "evaluate_slippage",
    "check_entry_slippage_once",
    "build_close_summary_strict",
    "check_closes",
]