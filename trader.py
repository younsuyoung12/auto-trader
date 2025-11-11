"""
trader.py
포지션 진입/TP·SL 설정/유지/체결 확인을 담당하는 모듈.

2025-11-12 수정 내용
----------------------------------------------------
1) BingX가 시장가 주문 응답에서 바로 FILLED/체결수량/평단을 주는 계정이 있어서,
   그 경우에는 굳이 wait_filled(...) 를 다시 안 돌고 그 값으로 바로 TP/SL 을 깔도록 했습니다.
2) 슬리피지가 설정값보다 크면 바로 반대방향 시장가로 닫고 포지션을 버리도록 했습니다.
3) TP/SL 이 취소/만료된 경우 다시 깔아주는 로직(ensure_tp_sl_for_trade)을 유지했습니다.
4) ❗중요: 진입할 때는 수량을 정수로 깎지 않고 그대로 exchange_api.place_market(...) 에 넘깁니다.
   (exchange_api 가 심볼별 step 으로 알아서 자르도록 하기 위함)
   TP/SL 주문이나 강제청산에서는 기존처럼 정수 계약으로 맞춰서 보냅니다.
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


# ─────────────────────────────
# 작은 유틸: 수량을 계약 단위 정수로 맞추기
# (TP/SL 이나 강제 정리할 때 쓰고, 진입할 때는 쓰지 않는다!)
# ─────────────────────────────
def _to_contract_qty(q: float) -> int:
    if q is None:
        return 1
    iq = int(round(q))
    return iq if iq >= 1 else 1


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
    # 1) 시장가 진입
    #    ❗여기서는 정수로 깎지 않고 그대로 넘긴다.
    try:
        resp = place_market(symbol, side_open, qty)
    except Exception as e:
        send_tg(f"[ENTRY][{source}] ❌ 시장가 진입 실패: {e}")
        return None

    data = resp.get("data") or resp

    # 시장가 주문의 orderId 추출
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

    # 1.5) 응답에서 바로 FILLED 가 왔는지 먼저 본다
    immediate_filled = False
    filled_qty: Optional[float] = None
    entry_price: Optional[float] = None

    status = data.get("status") or data.get("orderStatus")
    if status in ("FILLED", "PARTIALLY_FILLED"):
        immediate_filled = True
    # 어떤 계정은 status 대신 executedQty / avgPrice 만 주기도 함
    if data.get("executedQty") or data.get("avgPrice"):
        immediate_filled = True

    if immediate_filled:
        filled_qty = float(
            data.get("executedQty")
            or data.get("quantity")
            or qty
        )
        entry_price = float(
            data.get("avgPrice")
            or entry_price_hint
        )
    else:
        # 2) 기존처럼 일정 시간 동안 FILLED 될 때까지 기다린다
        filled = wait_filled(symbol, entry_order_id, timeout=5)
        if not filled:
            send_tg("[ENTRY] ⚠️ 시장가 주문이 제한 시간 내 FILLED 되지 않아 포지션을 건너뜁니다.")
            return None

        filled_qty = float(
            filled.get("quantity")
            or filled.get("executedQty")
            or qty
        )
        entry_price = float(
            filled.get("avgPrice")
            or entry_price_hint
        )

    if filled_qty <= 0:
        send_tg("[ENTRY] ⚠️ 시장가 체결 수량이 0입니다. 포지션을 건너뜁니다.")
        return None

    # 3) 슬리피지 가드
    max_slip_pct = getattr(settings, "max_entry_slippage_pct", 0.0)
    if max_slip_pct and entry_price_hint and entry_price_hint > 0:
        slip_pct = abs(entry_price - entry_price_hint) / entry_price_hint
        if slip_pct > max_slip_pct:
            close_side = "SELL" if side_open == "BUY" else "BUY"
            try:
                # 슬리피지 클 때는 바로 닫는다. 여기서는 계약 정수로.
                close_position_market(symbol, close_side, _to_contract_qty(filled_qty))
            except Exception as e:
                send_tg(
                    f"[ENTRY][{source}] ❗ 슬리피지 {slip_pct:.5f} > {max_slip_pct:.5f} 라서 닫으려 했으나 실패: {e}"
                )
            else:
                send_tg(
                    f"[ENTRY][{source}] ❌ 슬리피지 {slip_pct:.5f} > {max_slip_pct:.5f} → 포지션 취소"
                )
            return None

    # 3.5) TP/SL 퍼센트 보정
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

    # soft 시그널이면 TP 살짝만
    if soft_mode:
        tp_min = getattr(settings, "range_tp_min", tp_pct)
        soft_factor = getattr(settings, "range_soft_tp_factor", 1.0)
        tp_soft_cap = tp_min * soft_factor
        tp_pct = min(tp_pct, tp_soft_cap)

    # 숏일 때 SL 바닥 보정
    eff_sl_floor_ratio = sl_floor_ratio or getattr(settings, "range_short_sl_floor_ratio", 0.0)
    if side_open == "SELL" and eff_sl_floor_ratio > 0 and tp_pct > 0:
        min_short_sl = tp_pct * eff_sl_floor_ratio
        if sl_pct < min_short_sl:
            sl_pct = min_short_sl

    # 4) TP/SL 실제 가격 계산
    tp_price, sl_price = compute_tp_sl_prices(
        side_open=side_open,
        entry=entry_price,
        tp_pct=tp_pct,
        sl_pct=sl_pct,
        precision=2,
    )
    close_side = "SELL" if side_open == "BUY" else "BUY"

    # 5) TP/SL 주문 깔기
    try:
        tp_resp = place_conditional(
            symbol,
            close_side,
            _to_contract_qty(filled_qty),  # 여기서는 계약 단위
            tp_price,
            "TAKE_PROFIT_MARKET",
        )
        sl_resp = place_conditional(
            symbol,
            close_side,
            _to_contract_qty(filled_qty),
            sl_price,
            "STOP_MARKET",
        )
    except Exception as e:
        send_tg(f"[ENTRY][{source}] ❌ TP/SL 예약 실패: {e}, 포지션을 즉시 닫습니다.")
        close_position_market(symbol, close_side, _to_contract_qty(filled_qty))
        return None

    # 주문 응답에서 id 통일해서 뽑는 헬퍼
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
    # 거래소에서 동기화해온 포지션이면 건드리지 않는다.
    if trade.source == "SYNC":
        return True

    symbol = trade.symbol
    side_open = trade.side
    qty = trade.qty
    close_side = "SELL" if side_open == "BUY" else "BUY"

    need_tp = False
    need_sl = False

    # TP 살아있는지 확인
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

    # SL 살아있는지 확인
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

    # 필요하면 TP 다시 깔기
    if need_tp:
        try:
            tp_r = place_conditional(
                symbol,
                close_side,
                _to_contract_qty(qty),
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

    # 필요하면 SL 다시 깔기
    if need_sl:
        try:
            sl_r = place_conditional(
                symbol,
                close_side,
                _to_contract_qty(qty),
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
        # 거래소에서 동기화한 포지션이면 여기서 닫지 않는다.
        if t.source == "SYNC":
            still_open.append(t)
            continue

        symbol = t.symbol
        tp_id = t.tp_order_id
        sl_id = t.sl_order_id
        closed = False

        # TP 체결 확인
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

        # SL 체결 확인
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

        # 둘 다 안 닫혔으면 TP/SL 이 살아있는지 다시 보장
        if not closed:
            ok = ensure_tp_sl_for_trade(t, state)
            if not ok:
                # TP/SL 을 어떻게 해도 못 깔면 강제청산
                close_position_market(symbol, t.side, _to_contract_qty(t.qty))
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
