"""
sync_exchange.py
역할
----------------------------------------------------
거래소에 실제로 열려 있는 포지션/주문을 읽어서
내부 Trade 리스트와 맞춘다.

특징
----------------------------------------------------
- 포지션 조회가 실패하면 기존 리스트를 비우지 않는다.
- “정상 조회 + 실제 포지션 0”일 때만 내부 리스트를 비운다.
- run_bot.py 에서 주기적으로 호출해서 OPEN_TRADES 를 최신화한다.

사용 예
----------------------------------------------------
OPEN_TRADES, _ = sync_open_trades_from_exchange(symbol, True, OPEN_TRADES)
"""

from __future__ import annotations

from typing import List, Optional, Tuple

from telelog import log, send_tg
from exchange_api import fetch_open_positions, fetch_open_orders
from trader import Trade

# 마지막으로 동기화했을 때의 포지션 개수 (텔레그램 중복 알림 방지용)
_LAST_SYNCED_EXCHANGE_POS_COUNT: int = 0


def sync_open_trades_from_exchange(
    symbol: str,
    replace: bool = True,
    current_trades: Optional[List[Trade]] = None,
) -> Tuple[List[Trade], int]:
    """
    거래소에서 포지션/주문을 읽어서 Trade 리스트를 만든다.
    current_trades 를 주면 그걸 기반으로 대체하거나 유지한다.
    리턴: (새로운 Trade 리스트, 거래소에서 본 포지션 개수)
    """
    global _LAST_SYNCED_EXCHANGE_POS_COUNT

    if current_trades is None:
        current_trades = []

    # 1) 포지션 조회
    try:
        positions = fetch_open_positions(symbol)
    except Exception as e:
        log(f"[SYNC] fetch_open_positions error -> keep existing trades: {e}")
        # 조회 실패 시 기존 리스트를 그대로 돌려준다.
        return current_trades, _LAST_SYNCED_EXCHANGE_POS_COUNT

    # 2) 주문 조회
    try:
        orders = fetch_open_orders(symbol)
    except Exception as e:
        log(f"[SYNC] fetch_open_orders error: {e}")
        orders = []

    # 3) 실제 포지션이 없으면 비움
    if not positions:
        log("[SYNC] 열려 있는 포지션이 없습니다.")
        if replace:
            current_trades = []
            _LAST_SYNCED_EXCHANGE_POS_COUNT = 0
        return current_trades, _LAST_SYNCED_EXCHANGE_POS_COUNT

    # 4) 포지션이 있으면 다시 만든다
    if replace:
        current_trades = []

    for pos in positions:
        raw_amt = pos.get("positionAmt") or pos.get("quantity") or pos.get("size") or 0.0
        try:
            raw_amt_f = float(raw_amt)
        except (TypeError, ValueError):
            raw_amt_f = 0.0

        if raw_amt_f == 0.0:
            continue

        # 방향 변환
        pos_side_raw = (pos.get("positionSide") or "").upper()
        if pos_side_raw in ("LONG", "BOTH"):
            side_open = "BUY"
        elif pos_side_raw == "SHORT":
            side_open = "SELL"
        else:
            side_open = "BUY" if raw_amt_f > 0 else "SELL"

        qty = abs(raw_amt_f)
        entry_price = float(pos.get("entryPrice") or pos.get("avgPrice") or 0.0)

        # TP/SL 주문 찾기
        tp_id = None
        sl_id = None
        tp_price = None
        sl_price = None
        for o in orders:
            o_type = (o.get("type") or o.get("orderType") or "").upper()
            oid = o.get("orderId") or o.get("id")
            trig = o.get("triggerPrice") or o.get("stopPrice")
            if "TAKE" in o_type:
                tp_id = oid
                tp_price = float(trig) if trig else None
            if "STOP" in o_type:
                sl_id = oid
                sl_price = float(trig) if trig else None

        current_trades.append(
            Trade(
                symbol=symbol,
                side=side_open,
                qty=qty,
                entry=entry_price,
                entry_order_id=None,
                tp_order_id=tp_id,
                sl_order_id=sl_id,
                tp_price=tp_price,
                sl_price=sl_price,
                source="SYNC",
            )
        )

    # 5) 개수 변화가 있으면 한 번만 알림
    if len(current_trades) != _LAST_SYNCED_EXCHANGE_POS_COUNT:
        send_tg(f"🔁 거래소 포지션 {len(current_trades)}건 동기화했습니다. (중복 진입 방지)")
        _LAST_SYNCED_EXCHANGE_POS_COUNT = len(current_trades)

    return current_trades, _LAST_SYNCED_EXCHANGE_POS_COUNT
