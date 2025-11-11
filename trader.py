"""
trader.py
포지션 진입/TP·SL 설정/유지/체결 확인을 담당하는 모듈.

2025-11-12 8차 수정 반영 + exchange_api 7차 시그니처 맞춤
- exchange_api 가 positionSide 시도를 내부에서 하도록 바뀌었으므로
  여기서는 position_side 인자를 넘기지 않는다.
- 일부 계정에서 수량을 크게 보내면 바로 Insufficient margin 이 나므로
  심볼별 최소 단위로 한 번 더 내리는 보정을 추가했다.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from telelog import log, send_tg
from exchange_api import (
    place_market,
    place_conditional,
    wait_filled,
    get_order,
    summarize_fills,
    close_position_market,
)

# 심볼별 최소 수량 단위 (봇에서 주로 BTC-USDT 하나만 쓴다고 해서 이렇게 둠)
_MIN_QTY_STEP = {
    "BTC-USDT": 0.001,
}


# ─────────────────────────────
# 작은 유틸: 수량을 거래 가능한 최소 단위로 맞추기
# ─────────────────────────────
def _normalize_qty(symbol: str, q: float) -> float:
    """
    너무 큰 수량을 보내서 증거금 부족 나지 않도록 최소단위로 깎는다.
    예: BTC-USDT → 0.001
    """
    if q is None or q <= 0:
        # 최소한 1단위는 보내게 한다
        step = _MIN_QTY_STEP.get(symbol, 0.001)
        return float(f"{step:.6f}")

    step = _MIN_QTY_STEP.get(symbol, 0.001)
    if step <= 0:
        step = 0.001

    units = int(q / step)
    qty = units * step
    if qty <= 0:
        qty = step

    # 거래소가 소수 3~6자리까지만 받는 경우가 많으니 깔끔하게
    return float(f"{qty:.6f}")


# ─────────────────────────────
# 데이터 구조
# ─────────────────────────────
@dataclass
class Trade:
    symbol: str
    side: str
    qty: float
    entry: float
    entry_order_id: Optional[str] = None
    tp_order_id: Optional[str] = None
    sl_order_id: Optional[str] = None
    tp_price: Optional[float] = None
    sl_price: Optional[float] = None
    source: str = "UNKNOWN"


@dataclass
class TraderState:
    tp_sl_retry_fails: int = 0
    max_tp_sl_retry_fails: int = 3

    def reset_tp_sl_fails(self) -> None:
        self.tp_sl_retry_fails = 0

    def inc_tp_sl_fails(self) -> None:
        self.tp_sl_retry_fails += 1

    def should_stop_bot(self) -> bool:
        return self.tp_sl_retry_fails >= self.max_tp_sl_retry_fails


# ─────────────────────────────
# 유틸: TP/SL 가격 계산
# ─────────────────────────────
def compute_tp_sl_prices(
    side_open: str,
    entry: float,
    tp_pct: float,
    sl_pct: float,
    precision: int = 2,
) -> Tuple[float, float]:
    if entry <= 0:
        entry = 0.0

    if side_open == "BUY":
        tp_price = round(entry * (1 + tp_pct), precision)
        sl_price = round(entry * (1 - sl_pct), precision)
    else:
        tp_price = round(entry * (1 - tp_pct), precision)
        sl_price = round(entry * (1 + sl_pct), precision)
    return tp_price, sl_price


# ─────────────────────────────
# 포지션 진입 + TP/SL 깔기
# ─────────────────────────────
def open_position_with_tp_sl(
    *,
    settings: Any,
    symbol: str,
    side_open: str,
    qty: float,
    entry_price_hint: float,
    tp_pct: float,
    sl_pct: float,
    source: str = "UNKNOWN",
    soft_mode: bool = False,
    sl_floor_ratio: Optional[float] = None,
) -> Optional[Trade]:
    # 1) 수량 보정 먼저
    norm_qty = _normalize_qty(symbol, qty)

    # 2) 시장가 진입 (position_side 안 넘김)
    try:
        resp = place_market(symbol, side_open, norm_qty)
    except Exception as e:
        send_tg(f"[ENTRY][{source}] ❌ 시장가 진입 실패: {e}")
        return None

    data = resp.get("data") or resp
    entry_order_id = (
        data.get("orderId")
        or data.get("id")
        or data.get("orderID")
    )
    if not entry_order_id and isinstance(data, dict):
        order_obj = data.get("order")
        if isinstance(order_obj, dict):
            entry_order_id = (
                order_obj.get("orderId")
                or order_obj.get("orderID")
                or order_obj.get("id")
            )

    if not entry_order_id:
        send_tg("[ENTRY] ⚠️ 시장가 진입 응답에 orderId 가 없어 포지션을 건너뜁니다.")
        return None

    # 3) FILLED 확인
    filled = wait_filled(symbol, entry_order_id, timeout=5)
    if not filled:
        send_tg("[ENTRY] ⚠️ 시장가 주문이 제한 시간 내 FILLED 되지 않아 포지션을 건너뜁니다.")
        return None

    filled_qty = float(
        filled.get("quantity")
        or filled.get("executedQty")
        or norm_qty
    )
    if filled_qty <= 0:
        send_tg("[ENTRY] ⚠️ 시장가 체결 수량이 0입니다. 포지션을 건너뜁니다.")
        return None

    entry_price = float(
        filled.get("avgPrice")
        or entry_price_hint
    )

    # 4) 슬리피지 가드
    max_slip_pct = getattr(settings, "max_entry_slippage_pct", 0.0)
    if max_slip_pct and entry_price_hint and entry_price_hint > 0:
        slip_pct = abs(entry_price - entry_price_hint) / entry_price_hint
        if slip_pct > max_slip_pct:
            close_side = "SELL" if side_open == "BUY" else "BUY"
            try:
                close_position_market(symbol, close_side, _normalize_qty(symbol, filled_qty))
            except Exception as e:
                send_tg(
                    f"[ENTRY][{source}] ❗ 슬리피지 {slip_pct:.5f} > {max_slip_pct:.5f} 라서 닫으려 했으나 실패: {e}"
                )
            else:
                send_tg(
                    f"[ENTRY][{source}] ❌ 슬리피지 {slip_pct:.5f} > {max_slip_pct:.5f} → 포지션 취소"
                )
            return None

    # 4.5) TP/SL 퍼센트 보정
    if getattr(settings, "use_margin_based_tp_sl", False):
        lev = getattr(settings, "leverage", 1) or 1
        fut_tp_margin_pct = getattr(settings, "fut_tp_margin_pct", 0.0)
        fut_sl_margin_pct = getattr(settings, "fut_sl_margin_pct", 0.0)
        m_tp = (fut_tp_margin_pct / 100.0) / lev if lev > 0 else 0.0
        m_sl = (fut_sl_margin_pct / 100.0) / lev if lev > 0 else 0.0
        tp_pct = max(tp_pct, m_tp)
        sl_pct = max(sl_pct, m_sl)

    min_tp_pct = getattr(settings, "min_tp_pct", 0.0)
    min_sl_pct = getattr(settings, "min_sl_pct", 0.0)
    if min_tp_pct > 0:
        tp_pct = max(tp_pct, min_tp_pct)
    if min_sl_pct > 0:
        sl_pct = max(sl_pct, min_sl_pct)

    if soft_mode:
        tp_min = getattr(settings, "range_tp_min", tp_pct)
        soft_factor = getattr(settings, "range_soft_tp_factor", 1.0)
        tp_soft_cap = tp_min * soft_factor
        tp_pct = min(tp_pct, tp_soft_cap)

    eff_sl_floor_ratio = sl_floor_ratio or getattr(settings, "range_short_sl_floor_ratio", 0.0)
    if side_open == "SELL" and eff_sl_floor_ratio > 0 and tp_pct > 0:
        min_short_sl = tp_pct * eff_sl_floor_ratio
        if sl_pct < min_short_sl:
            sl_pct = min_short_sl

    # 5) TP/SL 가격
    tp_price, sl_price = compute_tp_sl_prices(
        side_open=side_open,
        entry=entry_price,
        tp_pct=tp_pct,
        sl_pct=sl_pct,
        precision=2,
    )
    close_side = "SELL" if side_open == "BUY" else "BUY"

    # 6) TP/SL 실제 주문
    try:
        tp_resp = place_conditional(
            symbol,
            close_side,
            _normalize_qty(symbol, filled_qty),
            tp_price,
            "TAKE_PROFIT_MARKET",
        )
        sl_resp = place_conditional(
            symbol,
            close_side,
            _normalize_qty(symbol, filled_qty),
            sl_price,
            "STOP_MARKET",
        )
    except Exception as e:
        send_tg(f"[ENTRY][{source}] ❌ TP/SL 예약 실패: {e}, 포지션을 즉시 닫습니다.")
        close_position_market(symbol, close_side, _normalize_qty(symbol, filled_qty))
        return None

    def _norm_id(r: Dict[str, Any]) -> Optional[str]:
        d = r.get("data") or r
        oid = d.get("orderId") or d.get("id") or d.get("orderID")
        if oid:
            return str(oid)
        if isinstance(d, dict):
            order_obj = d.get("order")
            if isinstance(order_obj, dict):
                return str(
                    order_obj.get("orderId")
                    or order_obj.get("orderID")
                    or order_obj.get("id")
                    or ""
                ) or None
        return None

    trade = Trade(
        symbol=symbol,
        side=side_open,
        qty=filled_qty,
        entry=entry_price,
        entry_order_id=entry_order_id,
        tp_order_id=_norm_id(tp_resp),
        sl_order_id=_norm_id(sl_resp),
        tp_price=tp_price,
        sl_price=sl_price,
        source=source,
    )
    return trade


# ─────────────────────────────
# TP/SL 유지
# ─────────────────────────────
def ensure_tp_sl_for_trade(trade: Trade, state: TraderState) -> bool:
    if trade.source == "SYNC":
        return True

    symbol = trade.symbol
    side_open = trade.side
    close_side = "SELL" if side_open == "BUY" else "BUY"
    qty_norm = _normalize_qty(symbol, trade.qty)

    need_tp = False
    need_sl = False

    if trade.tp_order_id:
        try:
            o = get_order(symbol, trade.tp_order_id)
            d = o.get("data") or o
            st = d.get("status") or d.get("orderStatus")
            if st in ("CANCELED", "REJECTED", "EXPIRED"):
                need_tp = True
        except Exception as e:
            log(f"[ensure_tp_sl] TP check error: {e}")
            need_tp = True
    else:
        need_tp = True

    if trade.sl_order_id:
        try:
            o = get_order(symbol, trade.sl_order_id)
            d = o.get("data") or o
            st = d.get("status") or d.get("orderStatus")
            if st in ("CANCELED", "REJECTED", "EXPIRED"):
                need_sl = True
        except Exception as e:
            log(f"[ensure_tp_sl] SL check error: {e}")
            need_sl = True
    else:
        need_sl = True

    ok = True

    if need_tp:
        try:
            tp_r = place_conditional(
                symbol,
                close_side,
                qty_norm,
                trade.tp_price,
                "TAKE_PROFIT_MARKET",
            )
            d = tp_r.get("data") or tp_r
            trade.tp_order_id = (
                d.get("orderId")
                or d.get("id")
                or d.get("orderID")
                or (d.get("order") or {}).get("orderId")
            )
            send_tg(f"🔄 TP 재설정: {symbol} {trade.tp_price}")
        except Exception as e:
            send_tg(f"❗ TP 재설정 실패: {e}")
            ok = False

    if need_sl:
        try:
            sl_r = place_conditional(
                symbol,
                close_side,
                qty_norm,
                trade.sl_price,
                "STOP_MARKET",
            )
            d = sl_r.get("data") or sl_r
            trade.sl_order_id = (
                d.get("orderId")
                or d.get("id")
                or d.get("orderID")
                or (d.get("order") or {}).get("orderId")
            )
            send_tg(f"🔄 SL 재설정: {symbol} {trade.sl_price}")
        except Exception as e:
            send_tg(f"❗ SL 재설정 실패: {e}")
            ok = False

    if ok:
        state.reset_tp_sl_fails()
    else:
        state.inc_tp_sl_fails()
        log(f"[ensure_tp_sl] reapply failed count={state.tp_sl_retry_fails}")

    return ok


# ─────────────────────────────
# 열린 포지션들의 TP/SL 체결 여부 확인
# ─────────────────────────────
def check_closes(
    open_trades: List[Trade],
    state: TraderState,
) -> Tuple[List[Trade], List[Dict[str, Any]]]:
    if not open_trades:
        return [], []

    still_open: List[Trade] = []
    closed_results: List[Dict[str, Any]] = []

    for t in open_trades:
        if t.source == "SYNC":
            still_open.append(t)
            continue

        symbol = t.symbol
        tp_id = t.tp_order_id
        sl_id = t.sl_order_id
        closed = False

        if tp_id:
            try:
                o = get_order(symbol, tp_id)
                d = o.get("data") or o
                st = d.get("status") or d.get("orderStatus")
                if st == "FILLED":
                    summary = summarize_fills(symbol, tp_id)
                    closed_results.append({"trade": t, "reason": "TP", "summary": summary})
                    closed = True
            except Exception as e:
                log(f"check_closes TP error: {e}")

        if (not closed) and sl_id:
            try:
                o = get_order(symbol, sl_id)
                d = o.get("data") or o
                st = d.get("status") or d.get("orderStatus")
                if st == "FILLED":
                    summary = summarize_fills(symbol, sl_id)
                    closed_results.append({"trade": t, "reason": "SL", "summary": summary})
                    closed = True
            except Exception as e:
                log(f"check_closes SL error: {e}")

        if not closed:
            ok = ensure_tp_sl_for_trade(t, state)
            if not ok:
                close_position_market(symbol, t.side, _normalize_qty(symbol, t.qty))
                closed_results.append({"trade": t, "reason": "FORCE_CLOSE", "summary": None})
            else:
                still_open.append(t)

    return still_open, closed_results


__all__ = [
    "Trade",
    "TraderState",
    "compute_tp_sl_prices",
    "open_position_with_tp_sl",
    "ensure_tp_sl_for_trade",
    "check_closes",
]
