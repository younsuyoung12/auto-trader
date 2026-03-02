"""
========================================================
FILE: state/trader_state.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
설계 원칙:
- in-memory 트레이드 상태(Trade)와 거래소 포지션 상태를 동기화한다.
- 예외를 삼키지 않는다. 데이터/거래소 응답 이상은 즉시 예외로 전파한다.
- 폴백 금지: WS 가격/포지션 정보가 없으면 임의 추정/조용한 return을 하지 않는다.
- 민감정보(API키/토큰/DB비번 등)는 출력/로그에 포함하지 않는다.

PATCH NOTES — 2026-03-02
--------------------------------------------------------
- (9점대 핵심) 청산 요약을 orderId 기반으로 확정(STRICT)
  - TP/SL orderId가 있으면: /fapi/v1/order status=FILLED + userTrades(orderId)로 요약
  - TP/SL이 없거나 비정상 케이스(수동/강제청산)인 경우:
    * userTrades를 orderId 단위로 그룹핑하여 "유일하게" trade.qty를 만족하는 orderId만 허용
    * 후보가 0개/2개 이상이면 즉시 예외 (추정/폴백 금지)
- (9점대 기반) 부분청산(partial close) 감지 및 in-memory 상태 업데이트(STRICT)
  - positionAmt 감소(delta_qty)를 userTrades(시간창)로 검증하여
    delta_qty 정합 시에만 remaining_qty / realized_pnl_usdt 업데이트
  - 정합 실패 시 즉시 예외 (폴백 금지)
- Trade dataclass에 운영형 실행/복구 필드 확장(레거시 호환 유지)
  - tp/sl/entry order ids, exchange_position_side, remaining_qty, realized_pnl_usdt,
    reconciliation_status, last_synced_at, close_order_id
========================================================
"""

from __future__ import annotations

import os
import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from execution.exchange_api import fetch_open_positions, req
from infra.market_data_ws import get_klines_with_volume
from infra.telelog import log, send_tg
from settings import load_settings

# ✅ DB close + exit snapshot (DB 스키마: exit_ts/exit_price/pnl_usdt)
from state.db_writer import (
    close_latest_open_trade_returning_id,
    record_trade_exit_snapshot,
)

SET = load_settings()

# ─────────────────────────────────────────────────────────────
# Basic strict helpers
# ─────────────────────────────────────────────────────────────
def _require_nonempty_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        raise RuntimeError(f"{name} is required")
    return s


def _require_float(v: Any, name: str) -> float:
    if v is None:
        raise RuntimeError(f"{name} is required")
    if isinstance(v, bool):
        raise RuntimeError(f"{name} must be numeric (bool not allowed)")
    try:
        f = float(v)
    except Exception as e:
        raise RuntimeError(f"{name} must be numeric: {e}") from e
    if not math.isfinite(f):
        raise RuntimeError(f"{name} must be finite")
    return f


def _get_env_required_float(name: str) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        raise RuntimeError(f"required env var missing: {name}")
    try:
        return float(raw)
    except Exception as e:
        raise RuntimeError(f"invalid env var {name}: {e}") from e


def _epoch_ms_to_dt_utc(ms: int) -> datetime:
    if not isinstance(ms, int) or ms <= 0:
        raise RuntimeError("epoch ms must be int > 0")
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def _dt_to_epoch_ms_utc(dt: datetime) -> int:
    if not isinstance(dt, datetime):
        raise RuntimeError("dt must be datetime")
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        raise RuntimeError("dt must be timezone-aware")
    ms = int(dt.timestamp() * 1000)
    if ms <= 0:
        raise RuntimeError("dt timestamp invalid")
    return ms


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _get_last_ws_price_strict(symbol: str) -> float:
    sym = _require_nonempty_str(symbol, "symbol")
    kl = get_klines_with_volume(sym, "1m", limit=1)
    if not kl:
        raise RuntimeError(f"no 1m candle in WS buffer: symbol={sym}")
    try:
        close_price = float(kl[0][4])  # [ts, o, h, l, c, v]
    except Exception as e:
        raise RuntimeError(f"invalid 1m candle format: {e}") from e
    if close_price <= 0:
        raise RuntimeError(f"invalid 1m close price: {close_price}")
    return close_price


# ─────────────────────────────────────────────────────────────
# Trade dataclass (in-memory)
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

    # 주문 ID (운영형: DB에도 저장됨)
    entry_order_id: Optional[str] = None
    tp_order_id: Optional[str] = None
    sl_order_id: Optional[str] = None

    # 수동/강제 청산의 close order 추적용(있으면 사용)
    close_order_id: Optional[str] = None

    # 클라이언트 ID (선택)
    client_entry_id: Optional[str] = None

    # DB trade_id (있으면 사용 가능)
    id: int = 0

    # legacy alias
    entry: float = 0.0

    # 상태 (in-memory)
    is_open: bool = True
    close_reason: str = ""
    close_ts: float = 0.0

    # 운영형 실행/복구 필드(부분청산/재시작 정합용)
    exchange_position_side: str = "BOTH"
    remaining_qty: float = 0.0
    realized_pnl_usdt: float = 0.0
    reconciliation_status: str = "INIT"
    last_synced_at: Optional[datetime] = None

    def __post_init__(self) -> None:
        if self.entry_price > 0 and self.entry == 0:
            self.entry = self.entry_price
        if self.entry > 0 and self.entry_price == 0:
            self.entry_price = self.entry

        if self.qty <= 0:
            raise RuntimeError("trade.qty must be > 0 (STRICT)")
        if self.entry_price <= 0:
            raise RuntimeError("trade.entry_price must be > 0 (STRICT)")
        if int(self.leverage) < 1:
            raise RuntimeError("trade.leverage must be >= 1 (STRICT)")

        # remaining_qty는 기본적으로 qty로 초기화
        if self.remaining_qty == 0.0:
            self.remaining_qty = float(self.qty)

        if self.last_synced_at is None:
            self.last_synced_at = _utc_now()

        ps = str(self.exchange_position_side or "").upper().strip() or "BOTH"
        if ps not in ("BOTH", "LONG", "SHORT"):
            raise RuntimeError(f"invalid exchange_position_side: {ps!r}")
        self.exchange_position_side = ps

    def calculate_pnl(self, current_price: float) -> float:
        cp = float(current_price)
        if self.entry_price <= 0:
            raise RuntimeError("entry_price must be > 0 to calculate pnl")
        side_u = str(self.side).upper()
        if side_u in ("BUY", "LONG"):
            return (cp - self.entry_price) * float(self.qty)
        return (self.entry_price - cp) * float(self.qty)

    def calculate_pnl_pct(self, current_price: float) -> float:
        cp = float(current_price)
        if self.entry_price <= 0:
            raise RuntimeError("entry_price must be > 0 to calculate pnl_pct")
        side_u = str(self.side).upper()
        if side_u in ("BUY", "LONG"):
            return (cp - self.entry_price) / self.entry_price * 100.0
        return (self.entry_price - cp) / self.entry_price * 100.0


class TraderState:
    """TP/SL 재설정 실패 횟수 등 런타임 상태 관리."""

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
# Slippage checks (legacy 유지)
# ─────────────────────────────────────────────────────────────
def evaluate_slippage(
    expected_entry: float,
    actual_entry: float,
    tick_size: Optional[float] = None,
    max_ticks: Optional[int] = None,
    max_notional_pct: Optional[float] = None,
) -> Tuple[bool, str]:
    """진입 슬리피지 평가."""
    exp = _require_float(expected_entry, "expected_entry")
    act = _require_float(actual_entry, "actual_entry")

    if tick_size is None:
        raise RuntimeError("tick_size is required (symbol filter)")
    tick = _require_float(tick_size, "tick_size")
    if tick <= 0:
        raise RuntimeError("tick_size must be > 0")

    if max_ticks is None:
        mt = getattr(SET, "slippage_max_ticks", None)
        max_ticks = int(mt) if mt is not None else int(_get_env_required_float("SLIPPAGE_MAX_TICKS"))

    if max_notional_pct is None:
        mnp = getattr(SET, "slippage_max_notional_pct", None)
        max_notional_pct = float(mnp) if mnp is not None else float(_get_env_required_float("SLIPPAGE_MAX_NOTIONAL_PCT"))

    if max_ticks <= 0:
        raise RuntimeError("max_ticks must be > 0")
    if max_notional_pct <= 0:
        raise RuntimeError("max_notional_pct must be > 0")

    diff = abs(act - exp)
    ticks = int(diff / tick)

    if exp <= 0:
        raise RuntimeError("expected_entry must be > 0")

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
    """진입 직후 1회 슬리피지 체크."""
    from execution.order_executor import close_position_market, get_symbol_filters

    sym = _require_nonempty_str(trade.symbol, "trade.symbol")
    expected_entry = _require_float(entry_price, "entry_price")

    filters = get_symbol_filters(sym)

    tick_size_val: Any = None
    if hasattr(filters, "tick_size"):
        tick_size_val = getattr(filters, "tick_size")
    elif isinstance(filters, dict):
        tick_size_val = filters.get("tickSize")
        if tick_size_val is None:
            tick_size_val = filters.get("tick_size")

    if tick_size_val is None:
        raise RuntimeError(f"tick_size not found in symbol filters: symbol={sym}")

    tick_size = _require_float(tick_size_val, "tick_size")
    if tick_size <= 0:
        raise RuntimeError(f"tick_size must be > 0: symbol={sym}, value={tick_size}")

    max_ticks = getattr(SET, "slippage_max_ticks", None)
    if max_ticks is None:
        max_ticks = int(_get_env_required_float("SLIPPAGE_MAX_TICKS"))
    else:
        max_ticks = int(max_ticks)

    max_notional_pct = getattr(SET, "slippage_max_notional_pct", None)
    if max_notional_pct is None:
        max_notional_pct = float(_get_env_required_float("SLIPPAGE_MAX_NOTIONAL_PCT"))
    else:
        max_notional_pct = float(max_notional_pct)

    stop_on_heavy = bool(int(os.getenv("STOP_ON_HEAVY_SLIPPAGE", "0")))

    positions = fetch_open_positions(sym)
    if not isinstance(positions, list):
        raise RuntimeError("fetch_open_positions returned non-list")

    pos = None
    for p in positions:
        if str(p.get("symbol", "")).upper() == sym.upper():
            pos = p
            break

    if pos is None:
        raise RuntimeError(f"open position not found for slippage check: symbol={sym}")

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
# Close summary (STRICT · NO-FALLBACK)
# ─────────────────────────────────────────────────────────────
class CloseSummaryError(RuntimeError):
    """거래소 확정값으로 청산 요약을 만들 수 없을 때 발생."""


def _float_strict(v: Any, name: str) -> float:
    if v is None:
        raise CloseSummaryError(f"{name} is required")
    if isinstance(v, bool):
        raise CloseSummaryError(f"{name} must be numeric (bool not allowed)")
    try:
        f = float(v)
    except Exception as e:
        raise CloseSummaryError(f"{name} must be numeric: {e}") from e
    if not math.isfinite(f):
        raise CloseSummaryError(f"{name} must be finite")
    return f


def _int_strict(v: Any, name: str) -> int:
    if v is None:
        raise CloseSummaryError(f"{name} is required")
    if isinstance(v, bool):
        raise CloseSummaryError(f"{name} must be int (bool not allowed)")
    try:
        return int(v)
    except Exception as e:
        raise CloseSummaryError(f"{name} must be int: {e}") from e


def _fetch_position_risk_rows_strict(symbol: str) -> List[Dict[str, Any]]:
    sym = _require_nonempty_str(symbol, "symbol").upper()
    data = req("GET", "/fapi/v2/positionRisk", params={"symbol": sym}, private=True)
    if not isinstance(data, list):
        raise CloseSummaryError("positionRisk response must be list")
    out: List[Dict[str, Any]] = []
    for r in data:
        if isinstance(r, dict) and str(r.get("symbol") or "").upper() == sym:
            out.append(r)
    if not out:
        raise CloseSummaryError("positionRisk returned empty")
    return out


def _trade_direction_strict(trade: Trade) -> str:
    side = str(trade.side or "").upper().strip()
    if side in ("BUY", "LONG"):
        return "LONG"
    if side in ("SELL", "SHORT"):
        return "SHORT"
    raise CloseSummaryError(f"invalid trade.side: {side!r}")


def _trade_entry_side_db(trade: Trade) -> str:
    """bt_trades.side는 BUY/SELL로 저장."""
    side = str(trade.side or "").upper().strip()
    if side in ("BUY", "SELL"):
        return side
    if side == "LONG":
        return "BUY"
    if side == "SHORT":
        return "SELL"
    raise CloseSummaryError(f"invalid trade.side: {side!r}")


def _close_side_for_trade(trade: Trade) -> str:
    side_u = str(trade.side or "").upper().strip()
    if side_u in ("BUY", "LONG"):
        return "SELL"
    if side_u in ("SELL", "SHORT"):
        return "BUY"
    raise CloseSummaryError(f"invalid trade.side: {side_u!r}")


def _pick_position_row_for_trade(rows: List[Dict[str, Any]], trade: Trade) -> Dict[str, Any]:
    wanted = _trade_direction_strict(trade)
    for r in rows:
        ps = str(r.get("positionSide") or "").upper()
        if ps == wanted:
            return r
    for r in rows:
        ps = str(r.get("positionSide") or "").upper()
        if ps in ("BOTH", ""):
            return r
    if not rows:
        raise CloseSummaryError("positionRisk rows empty")
    return rows[0]


def _fetch_order_strict(symbol: str, order_id: str) -> Dict[str, Any]:
    sym = _require_nonempty_str(symbol, "symbol").upper()
    oid = _int_strict(order_id, "order_id")
    data = req("GET", "/fapi/v1/order", params={"symbol": sym, "orderId": oid}, private=True)
    if not isinstance(data, dict):
        raise CloseSummaryError("order response must be dict")
    return data


def _fetch_user_trades_strict(symbol: str, *, start_ms: int, end_ms: int, limit: int = 1000) -> List[Dict[str, Any]]:
    sym = _require_nonempty_str(symbol, "symbol").upper()
    if start_ms <= 0 or end_ms <= 0 or end_ms < start_ms:
        raise CloseSummaryError("invalid userTrades time range")
    if not isinstance(limit, int) or limit <= 0 or limit > 1000:
        raise CloseSummaryError("userTrades limit must be int in [1, 1000]")

    data = req(
        "GET",
        "/fapi/v1/userTrades",
        params={"symbol": sym, "startTime": int(start_ms), "endTime": int(end_ms), "limit": int(limit)},
        private=True,
    )
    if not isinstance(data, list):
        raise CloseSummaryError("userTrades response must be list")
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
        try:
            if int(oid) != int(order_id):
                continue
        except Exception:
            continue

        qty = _float_strict(r.get("qty"), "userTrade.qty")
        price = _float_strict(r.get("price"), "userTrade.price")
        realized = _float_strict(r.get("realizedPnl"), "userTrade.realizedPnl")
        commission = _float_strict(r.get("commission"), "userTrade.commission")
        commission_asset = str(r.get("commissionAsset") or "").upper()

        t_ms = None
        try:
            t_ms = int(r.get("time")) if r.get("time") is not None else None
        except Exception:
            t_ms = None

        if qty <= 0 or price <= 0:
            raise CloseSummaryError("userTrade.qty/price must be > 0")

        qty_sum += qty
        px_qty_sum += price * qty
        realized_sum += realized
        if commission_asset == "USDT":
            fee_usdt_sum += commission

        if t_ms is not None:
            if last_time_ms is None or t_ms > last_time_ms:
                last_time_ms = t_ms

    if qty_sum <= 0:
        raise CloseSummaryError("no fills for orderId")

    avg_price = px_qty_sum / qty_sum
    if avg_price <= 0 or not math.isfinite(avg_price):
        raise CloseSummaryError("avg_price invalid")

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
        try:
            oi = int(oid)
        except Exception:
            raise CloseSummaryError("userTrade.orderId must be int")
        out.setdefault(oi, []).append(r)
    return out


def _infer_unique_close_order_id_strict(
    trade: Trade,
    trades_window: List[Dict[str, Any]],
    *,
    expected_qty: float,
    tolerance_ratio: float,
) -> Tuple[int, Dict[str, Any]]:
    """
    STRICT:
    - userTrades를 orderId 단위로 그룹핑한 뒤,
      "expected_qty를 유일하게 만족하는" orderId만 허용한다.
    - 후보 0개/2개 이상이면 즉시 예외 (추정/폴백 금지)
    """
    close_side = _close_side_for_trade(trade)
    groups = _group_user_trades_by_order_id(trades_window, side=close_side)

    if expected_qty <= 0 or not math.isfinite(expected_qty):
        raise CloseSummaryError("expected_qty invalid")

    tol = abs(expected_qty) * float(tolerance_ratio)
    if tol <= 0:
        tol = abs(expected_qty) * 0.01  # 최소 1% (단, ratio=0인 경우를 방지)
    if tol <= 0:
        raise CloseSummaryError("tolerance invalid")

    candidates: List[Tuple[int, Dict[str, Any]]] = []
    for oid, rows in groups.items():
        # orderId 단위 요약
        summary = _summarize_user_trades_for_order(trades_window, order_id=int(oid))
        qty = _float_strict(summary.get("qty"), "summary.qty")
        if abs(qty - expected_qty) <= tol:
            candidates.append((int(oid), summary))

    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) == 0:
        raise CloseSummaryError("cannot infer close orderId uniquely (no candidates)")
    raise CloseSummaryError("cannot infer close orderId uniquely (multiple candidates)")


def build_close_summary_strict(
    trade: Trade,
    *,
    now_ms: Optional[int] = None,
    lookback_sec: int = 6 * 3600,
) -> Tuple[str, Dict[str, Any]]:
    """
    STRICT:
    - TP/SL 또는 close_order_id 기반으로 청산을 확정한다.
    - TP/SL이 없고 close_order_id도 없을 경우:
      userTrades를 orderId 단위로 그룹핑하여 trade.qty를 유일하게 만족하는 orderId만 허용한다.
    - 유일성/정합성 실패 시 즉시 예외 (폴백 금지)
    """
    sym = _require_nonempty_str(trade.symbol, "trade.symbol").upper()
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    if now_ms <= 0:
        raise CloseSummaryError("now_ms invalid")

    tp_id = str(trade.tp_order_id or "").strip()
    sl_id = str(trade.sl_order_id or "").strip()
    close_id = str(trade.close_order_id or "").strip()

    # 1) TP/SL FILLED 체크
    filled: List[Tuple[str, Dict[str, Any]]] = []
    for oid in (tp_id, sl_id):
        if not oid:
            continue
        o = _fetch_order_strict(sym, oid)
        status = str(o.get("status") or "").upper()
        if status == "FILLED":
            filled.append((oid, o))

    close_oid: Optional[str] = None
    close_order: Optional[Dict[str, Any]] = None
    if len(filled) == 1:
        close_oid, close_order = filled[0]
        reason = "TP" if close_oid == tp_id else "SL"
        # order updateTime 근처로 userTrades(orderId) 집계
        close_time_ms = None
        for k in ("updateTime", "time", "transactTime"):
            if close_order.get(k) is not None:
                try:
                    close_time_ms = int(close_order.get(k))
                    break
                except Exception:
                    close_time_ms = None
        if close_time_ms is None:
            close_time_ms = now_ms

        win_ms = 2 * 3600 * 1000
        start_ms = max(1, close_time_ms - win_ms)
        end_ms = close_time_ms + win_ms
        user_trades = _fetch_user_trades_strict(sym, start_ms=start_ms, end_ms=end_ms)
        summary = _summarize_user_trades_for_order(user_trades, order_id=_int_strict(close_oid, "close_order_id"))
        return reason, summary

    if len(filled) > 1:
        raise CloseSummaryError("multiple close orders FILLED (ambiguous)")

    # 2) close_order_id가 있으면 그것으로 확정
    if close_id:
        o = _fetch_order_strict(sym, close_id)
        status = str(o.get("status") or "").upper()
        if status != "FILLED":
            raise CloseSummaryError(f"close_order_id present but not FILLED (status={status})")

        close_time_ms = None
        for k in ("updateTime", "time", "transactTime"):
            if o.get(k) is not None:
                try:
                    close_time_ms = int(o.get(k))
                    break
                except Exception:
                    close_time_ms = None
        if close_time_ms is None:
            close_time_ms = now_ms

        win_ms = 2 * 3600 * 1000
        start_ms = max(1, close_time_ms - win_ms)
        end_ms = close_time_ms + win_ms
        user_trades = _fetch_user_trades_strict(sym, start_ms=start_ms, end_ms=end_ms)
        summary = _summarize_user_trades_for_order(user_trades, order_id=_int_strict(close_id, "close_order_id"))
        return "MANUAL", summary

    # 3) 마지막 수단: userTrades를 orderId로 그룹핑하여 "유일" 후보만 허용
    expected_qty = _float_strict(trade.qty, "trade.qty")
    if expected_qty <= 0:
        raise CloseSummaryError("trade.qty must be > 0")

    start_ms = max(1, now_ms - int(lookback_sec * 1000))
    end_ms = now_ms
    user_trades = _fetch_user_trades_strict(sym, start_ms=start_ms, end_ms=end_ms)

    oid, summary = _infer_unique_close_order_id_strict(
        trade,
        user_trades,
        expected_qty=float(expected_qty),
        tolerance_ratio=0.01,  # 1% 허용
    )
    # close_order_id를 메모리에 기록(호출부에서 DB에 반영할지는 별도)
    trade.close_order_id = str(oid)
    return "MANUAL", summary


def _calc_pnl_pct_futures(trade: Trade, pnl_usdt: float) -> Optional[float]:
    try:
        entry_price = float(trade.entry_price)
        qty = float(trade.qty)
        if entry_price <= 0 or qty <= 0:
            return None
        notional = abs(entry_price * qty)
        if notional <= 0:
            return None
        return float(pnl_usdt) / notional * 100.0
    except Exception:
        return None


def _persist_close_to_db_strict(*, trade: Trade, reason: str, summary: Dict[str, Any]) -> int:
    sym = _require_nonempty_str(trade.symbol, "trade.symbol").upper()
    entry_side_db = _trade_entry_side_db(trade)  # BUY/SELL

    if not isinstance(summary, dict):
        raise CloseSummaryError("summary must be dict")
    for k in ("avg_price", "pnl"):
        if k not in summary:
            raise CloseSummaryError(f"summary missing required key: {k}")

    exit_price = _float_strict(summary.get("avg_price"), "summary.avg_price")
    pnl_usdt = _float_strict(summary.get("pnl"), "summary.pnl")
    if exit_price <= 0:
        raise CloseSummaryError("summary.avg_price must be > 0")

    close_time_ms = summary.get("close_time")
    if close_time_ms is not None:
        exit_ts_dt = _epoch_ms_to_dt_utc(_int_strict(close_time_ms, "summary.close_time"))
    else:
        exit_ts_dt = _utc_now()

    close_reason = _require_nonempty_str(reason, "reason")[:32]
    pnl_pct_futures = _calc_pnl_pct_futures(trade, pnl_usdt)

    # STRICT: 단일 심볼 단일 오픈 트레이드 전제에서만 안전
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


# ─────────────────────────────────────────────────────────────
# Partial close reconciliation (STRICT)
# ─────────────────────────────────────────────────────────────
def _strict_reconcile_partial_close(
    trade: Trade,
    *,
    symbol: str,
    delta_qty: float,
    start_ms: int,
    end_ms: int,
) -> float:
    """
    STRICT:
    - delta_qty(감소한 수량)와 userTrades(반대 사이드 체결)의 합이 1% 이내로 일치해야 한다.
    - 일치하면 해당 구간의 realizedPnl - fee(USDT) 를 반환한다.
    - 정합 실패 시 즉시 예외(폴백 금지)
    """
    if delta_qty <= 0 or not math.isfinite(delta_qty):
        raise CloseSummaryError("delta_qty invalid")

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
            raise CloseSummaryError("userTrade.qty must be > 0")

        qty_sum += qty
        realized_sum += realized
        if commission_asset == "USDT":
            fee_usdt_sum += commission

    tol = max(delta_qty * 0.01, 1e-12)  # 1% 허용
    if abs(qty_sum - delta_qty) > tol:
        raise CloseSummaryError(f"partial close qty mismatch: expected_delta={delta_qty}, got={qty_sum}")

    pnl_delta = realized_sum - fee_usdt_sum
    if not math.isfinite(pnl_delta):
        raise CloseSummaryError("partial pnl_delta not finite")
    return float(pnl_delta)


# ─────────────────────────────────────────────────────────────
# Main close checker
# ─────────────────────────────────────────────────────────────
def check_closes(open_trades: List[Trade], trader_state: TraderState) -> Tuple[List[Trade], List[Dict[str, Any]]]:
    _ = trader_state

    if not open_trades:
        return [], []

    # STRICT: DB close_latest_open_trade_returning_id 사용 중이면,
    # 다중 트레이드(같은 심볼)에서는 닫을 대상이 뒤섞일 수 있다 → 즉시 금지.
    if len(open_trades) != 1:
        raise RuntimeError("STRICT: multiple open_trades is not supported (single-trade only)")

    t = open_trades[0]
    symbol = _require_nonempty_str(t.symbol, "open_trades[0].symbol").upper()

    pos_rows = _fetch_position_risk_rows_strict(symbol)
    row = _pick_position_row_for_trade(pos_rows, t)
    pos_qty = _float_strict(row.get("positionAmt"), "positionRisk.positionAmt")

    abs_qty = abs(pos_qty)
    remaining: List[Trade] = []
    closed: List[Dict[str, Any]] = []

    # 부분청산 감지: remaining_qty 감소(포지션이 아직 남아있을 때)
    prev_remaining = _require_float(t.remaining_qty, "trade.remaining_qty")
    if prev_remaining <= 0:
        prev_remaining = float(t.qty)

    # 포지션이 열려 있는데 수량이 감소한 경우(부분청산)
    eps = 1e-12
    if abs_qty > eps and abs_qty < prev_remaining - eps:
        delta = prev_remaining - abs_qty

        # STRICT: userTrades로 delta 정합 검증
        now_dt = _utc_now()
        now_ms = _dt_to_epoch_ms_utc(now_dt)

        # last_synced_at 기준으로 구간 생성(±2초 완충). 완충은 폴백이 아니라 "관측 지연" 흡수용.
        if t.last_synced_at is None:
            raise CloseSummaryError("trade.last_synced_at is required for partial reconciliation (STRICT)")
        start_ms = _dt_to_epoch_ms_utc(t.last_synced_at) - 2000
        end_ms = now_ms + 2000
        if start_ms <= 0:
            start_ms = max(1, now_ms - 10_000)

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

        remaining.append(t)
        return remaining, closed

    # 완전 청산 감지
    if abs_qty < eps:
        reason, summary = build_close_summary_strict(t)

        if not isinstance(summary, dict):
            raise CloseSummaryError("summary must be dict")
        for k in ("qty", "avg_price", "pnl"):
            if k not in summary:
                raise CloseSummaryError(f"summary missing required key: {k}")

        trade_id = _persist_close_to_db_strict(trade=t, reason=str(reason), summary=summary)
        if t.id <= 0:
            t.id = int(trade_id)

        t.is_open = False
        t.close_reason = str(reason)
        t.close_ts = _utc_now().timestamp()
        t.remaining_qty = 0.0
        t.reconciliation_status = "CLOSED_OK"
        t.last_synced_at = _utc_now()

        closed.append({"trade": t, "reason": reason, "summary": summary})
        return [], closed

    # 변화 없음(열려있음)
    remaining.append(t)
    return remaining, closed


__all__ = [
    "Trade",
    "TraderState",
    "evaluate_slippage",
    "check_entry_slippage_once",
    "build_close_summary_strict",
    "check_closes",
]