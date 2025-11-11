# trader.py
# 포지션 진입/TP·SL 설정/유지/체결 확인을 담당하는 모듈.
#
# 2025-11-12 추가 수정 (1차)
# ----------------------------------------------------
# - BingX 원웨이 계정에서 실제 체결 수량이 0.005 같은 소수인데
#   이 모듈이 수량을 int(…)로 올려서 1.0으로 보내는 문제가 있었다.
# - 그 때문에 TP/SL 넣을 때 "The order size must be less than the available amount of 0.005 BTC"
#   (110424) 에러가 났고, 강제 정리도 reduceOnly+1.0 이라 또 막혔다.
# - 그래서 여기서는 "정수로 강제"하지 않고, 소수점을 유지해서 exchange_api 에 넘기도록 수정했다.
# - exchange_api 가 이미 심볼별 step 으로 0.005, 0.001 이런 걸 깎아주므로 여기서는 그대로 보내는 게 맞다.
#
# 2025-11-12 추가 수정 (2차)
# ----------------------------------------------------
# - 슬리피지 가드로 포지션을 즉시 닫을 때,
#   그리고 TP/SL 예약이 실패해서 포지션을 닫을 때
#   close_position_market(...) 에 “열었던 방향(side_open)” 을 넘기도록 수정했다.
# - exchange_api.close_position_market(symbol, side_open, qty)는
#   내부에서 반대쪽으로 바꿔서 닫는 구조이기 때문에
#   여기서 미리 반대 방향을 넘기면 방향이 한 번 더 뒤집혀 잘못된 주문이 나갈 수 있다.
# - 따라서 이 모듈에서는 “처음 연 방향”만 넘기도록 통일했다.

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


# 기존에는 int 로 올렸는데, 이제는 소수 그대로 보낸다.
# exchange_api 쪽에서 심볼 step 에 맞게 다시 깎아주므로 여기서는 가볍게만 정리.
def _to_contract_qty(q: float) -> float:
    if q is None or q <= 0:
        return 0.001  # 최소 안전치
    # 너무 긴 소수만 잘라준다 (BTC-USDT 는 0.001 step 기준)
    return float(f"{q:.3f}")


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
    try:
        # 체결 수량은 소수 그대로 전달
        resp = place_market(symbol, side_open, _to_contract_qty(qty))
    except Exception as e:
        send_tg(f"[ENTRY][{source}] ❌ 시장가 진입 실패: {e}")
        return None

    data = resp.get("data") or resp

    # 주문 id 추출
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

    # 1.5) 여기서 바로 FILLED 가 왔는지 먼저 본다
    immediate_filled = False
    filled_qty = None
    entry_price = None

    status = data.get("status") or data.get("orderStatus")
    if status in ("FILLED", "PARTIALLY_FILLED"):
        immediate_filled = True
    if data.get("executedQty") or data.get("avgPrice"):
        # 어떤 계정은 status 안 주고 체결수량/평단만 주기도 함
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
        # 2) FILLED 확인 (기존 방식)
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
            # ❗ 여기서는 '열었던 방향'을 넘겨야 exchange_api 가 반대 주문을 보내며 닫는다.
            try:
                close_position_market(
                    symbol,
                    side_open,  # ← 수정됨: close_side 가 아니라 side_open
                    _to_contract_qty(filled_qty),
                )
            except Exception as e:
                send_tg(
                    f"[ENTRY][{source}] ❗ 슬리피지 {slip_pct:.5f} > {max_slip_pct:.5f} 라서 닫으려 했으나 실패: {e}"
                )
            else:
                send_tg(
                    f"[ENTRY][{source}] ❌ 슬리피지 {slip_pct:.5f} > {max_slip_pct:.5f} → 포지션 취소"
                )
            return None

    # 3.5) TP/SL 퍼센트 보정 (기존 내용 그대로)
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

    # 시그널이 soft 면 tp 조금만
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

    # 4) TP/SL 가격 계산
    tp_price, sl_price = compute_tp_sl_prices(
        side_open=side_open,
        entry=entry_price,
        tp_pct=tp_pct,
        sl_pct=sl_pct,
        precision=2,
    )
    close_side = "SELL" if side_open == "BUY" else "BUY"

    # 5) TP/SL 실제 주문
    try:
        # 체결된 소수 수량으로 넣는다 (0.005 등)
        real_qty = _to_contract_qty(filled_qty)
        tp_resp = place_conditional(
            symbol,
            close_side,
            real_qty,
            tp_price,
            "TAKE_PROFIT_MARKET",
        )
        sl_resp = place_conditional(
            symbol,
            close_side,
            real_qty,
            sl_price,
            "STOP_MARKET",
        )
    except Exception as e:
        send_tg(f"[ENTRY][{source}] ❌ TP/SL 예약 실패: {e}, 포지션을 즉시 닫습니다.")
        # ❗ 여기서도 열었던 방향을 넘겨야 한다.
        close_position_market(
            symbol,
            side_open,  # ← 수정됨: close_side 아님
            _to_contract_qty(filled_qty),
        )
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


def ensure_tp_sl_for_trade(trade: Trade, state: TraderState) -> bool:
    # 거래소에서 동기화해온 포지션은 그냥 놔둔다
    if trade.source == "SYNC":
        return True

    symbol = trade.symbol
    side_open = trade.side
    qty = trade.qty
    close_side = "SELL" if side_open == "BUY" else "BUY"

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

    # 여기서도 무조건 소수 수량으로
    real_qty = _to_contract_qty(qty)

    if need_tp:
        try:
            tp_r = place_conditional(
                symbol,
                close_side,
                real_qty,
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
                real_qty,
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
                # TP/SL 재설정이 계속 실패하면 강제로 닫는다.
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
