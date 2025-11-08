"""trader.py
포지션 진입/TP/SL 설정/유지/체결확인까지 담당하는 모듈.

이 모듈은 '저수준 API 호출' 은 exchange_api.py 에 맡기고,
여기서는 그 API 들을 조합해서 실제로 우리가 쓰는 포맷의 트레이드(dict)를 만들고 유지한다.

원래 bot.py 안에 있던 아래 책임을 이쪽으로 옮겼다:
- 시장가 진입 후 FILLED 될 때까지 짧게 대기
- 진입하자마자 TP/SL 두 개 깔기
- 열린 포지션에 대해 TP/SL 이 취소/만료되면 다시 걸기
- TP/SL 체결 여부를 주기적으로 확인해서 닫힌 포지션 목록을 돌려주기

run_bot.py 에서는 이 모듈의 함수들을 사용해서 OPEN_TRADES 리스트를 관리하면 된다.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from settings import BotSettings, load_settings
from telelog import log, send_tg
from exchange_api import (
    place_market,
    place_conditional,
    wait_filled,
    get_order,
    summarize_fills,
    close_position_market,
)

# 설정은 한 번만 읽어두되, 필요한 곳에서는 인자로도 받을 수 있게 한다.
SET = load_settings()


# ─────────────────────────────
# 데이터 구조
# ─────────────────────────────@dataclass
class Trade:
    """우리 봇이 내부에서 쓰는 포지션 표현.

    원래 긴 dict 를 쓰던 것을 dataclass 로 가독성 있게 만든 것이다.
    dict 로 쓰고 싶으면 trade.__dict__ 로 꺼내서 써도 된다.
    """

    symbol: str
    side: str  # "BUY" 또는 "SELL"
    qty: float
    entry: float
    entry_order_id: Optional[str] = None
    tp_order_id: Optional[str] = None
    sl_order_id: Optional[str] = None
    tp_price: Optional[float] = None
    sl_price: Optional[float] = None
    source: str = "UNKNOWN"  # "TREND" / "RANGE" / "SYNC" 등


@dataclass
class TraderState:
    """TP/SL 재설정 실패 횟수 등, 트레이더 레벨에서 유지해야 하는 상태.

    run_bot.py 에서 이걸 하나 만들어서 들고 다니면 된다.
    """

    tp_sl_retry_fails: int = 0
    max_tp_sl_retry_fails: int = 3  # 3번 연속 실패하면 봇 중단 신호를 줌

    def reset_tp_sl_fails(self) -> None:
        self.tp_sl_retry_fails = 0

    def inc_tp_sl_fails(self) -> None:
        self.tp_sl_retry_fails += 1

    def should_stop_bot(self) -> bool:
        return self.tp_sl_retry_fails >= self.max_tp_sl_retry_fails


# ─────────────────────────────
# 유틸: TP/SL 가격 계산
# ─────────────────────────────def compute_tp_sl_prices(side_open: str, entry: float, tp_pct: float, sl_pct: float, precision: int = 2) -> Tuple[float, float]:
    """진입 방향과 퍼센트로 TP/SL 실제 가격을 계산한다.

    - 롱이면 TP = entry * (1 + tp_pct), SL = entry * (1 - sl_pct)
    - 숏이면 TP = entry * (1 - tp_pct), SL = entry * (1 + sl_pct)
    - 거래소 특성상 소수 둘째자리까지 쓰던 걸 따라 precision=2 기본으로 한다.
    """
    if side_open == "BUY":
        tp_price = round(entry * (1 + tp_pct), precision)
        sl_price = round(entry * (1 - sl_pct), precision)
    else:  # SELL
        tp_price = round(entry * (1 - tp_pct), precision)
        sl_price = round(entry * (1 + sl_pct), precision)
    return tp_price, sl_price


# ─────────────────────────────
# 포지션 진입 + TP/SL 깔기
# ─────────────────────────────def open_position_with_tp_sl(
    settings: BotSettings,
    symbol: str,
    side_open: str,
    qty: float,
    entry_price_hint: float,
    tp_pct: float,
    sl_pct: float,
    source: str = "UNKNOWN",
) -> Optional[Trade]:
    """시장가로 진입하고, 체결을 확인한 뒤, TP/SL 을 두 개 다 건다.

    성공하면 Trade 객체를 리턴하고, 실패하면 None 을 리턴한다.
    - place_market 실패 → None
    - wait_filled 타임아웃 → None
    - TP/SL 주문 실패 → 시장가로 닫고 None
    """
    # 1) 시장가 진입
    try:
        res = place_market(symbol, side_open, qty)
    except Exception as e:
        send_tg(f"[ENTRY][{source}] ❌ 시장가 진입 실패: {e}")
        return None

    # 주문 ID 추출 (응답 구조가 계정마다 달라서 안전하게 꺼냄)
    data = res.get("data") or res
    order_id = data.get("orderId") or data.get("id") or data.get("orderID")

    # 2) 체결 대기 (5초 기본)
    filled = None
    if order_id:
        filled = wait_filled(symbol, order_id, timeout=5)
    else:
        # order_id 가 없으면 어차피 TP/SL 을 못 건다.
        send_tg("[ENTRY] ⚠️ 시장가 진입 응답에 orderId 가 없습니다. 진입을 취소합니다.")
        return None

    if not filled:
        # 체결이 안 됐으면 여기서 포기
        send_tg("[ENTRY] ⚠️ 시장가 주문이 제한 시간 내 FILLED 되지 않아 건너뜁니다.")
        return None

    # 실제 체결 수량/가격
    filled_qty = float(filled.get("quantity") or filled.get("executedQty") or qty)
    if filled_qty <= 0:
        send_tg("[ENTRY] ⚠️ 시장가 체결 수량이 0입니다. 진입을 취소합니다.")
        return None
    # 체결 가격이 응답에 없으면 힌트를 사용
    entry_price = float(filled.get("avgPrice") or entry_price_hint)

    # 3) TP/SL 가격 계산
    tp_price, sl_price = compute_tp_sl_prices(side_open, entry_price, tp_pct, sl_pct)
    close_side = "SELL" if side_open == "BUY" else "BUY"

    # 4) TP/SL 주문 두 개 깔기
    try:
        tp_res = place_conditional(symbol, close_side, filled_qty, tp_price, "TAKE_PROFIT_MARKET")
        sl_res = place_conditional(symbol, close_side, filled_qty, sl_price, "STOP_MARKET")
    except Exception as e:
        # TP/SL 중 하나라도 실패하면 포지션을 강제로 닫는다.
        send_tg(f"[ENTRY][{source}] ❌ TP/SL 예약 실패: {e}, 포지션을 즉시 닫습니다.")
        close_position_market(symbol, side_open, filled_qty)
        return None

    # 주문 ID 정규화
    def _norm_order_id(resp: Dict[str, Any]) -> Optional[str]:
        d = resp.get("data") or resp
        return d.get("orderId") or d.get("id") or d.get("orderID")

    trade = Trade(
        symbol=symbol,
        side=side_open,
        qty=filled_qty,
        entry=entry_price,
        entry_order_id=order_id,
        tp_order_id=_norm_order_id(tp_res),
        sl_order_id=_norm_order_id(sl_res),
        tp_price=tp_price,
        sl_price=sl_price,
        source=source,
    )
    return trade


# ─────────────────────────────
# 열린 포지션 TP/SL 상태 보정
# ─────────────────────────────def ensure_tp_sl_for_trade(trade: Trade, state: TraderState) -> bool:
    """포지션은 살아 있는데 TP/SL 주문이 사라졌을 때 다시 건다.

    - TP 또는 SL 이 없으면 해당 주문만 다시 건다.
    - 둘 중 하나라도 다시 거는 데 실패하면 False 를 리턴한다.
    - 연속 실패 횟수는 state 에 누적한다.
    - 연속 실패가 max 를 넘으면 run_bot 이 봇을 중단하도록 할 수 있다.
    """
    symbol = trade.symbol
    side_open = trade.side
    qty = trade.qty
    close_side = "SELL" if side_open == "BUY" else "BUY"

    need_tp = False
    need_sl = False

    # TP 체크
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

    # SL 체크
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
    # 필요하면 다시 건다.
    if need_tp:
        try:
            tp_res = place_conditional(symbol, close_side, qty, trade.tp_price, "TAKE_PROFIT_MARKET")
            d = tp_res.get("data") or tp_res
            trade.tp_order_id = d.get("orderId") or d.get("id") or d.get("orderID")
            send_tg(f"🔄 TP 재설정: {symbol} {trade.tp_price}")
        except Exception as e:
            send_tg(f"❗ TP 재설정 실패: {e}")
            ok = False

    if need_sl:
        try:
            sl_res = place_conditional(symbol, close_side, qty, trade.sl_price, "STOP_MARKET")
            d = sl_res.get("data") or sl_res
            trade.sl_order_id = d.get("orderId") or d.get("id") or d.get("orderID")
            send_tg(f"🔄 SL 재설정: {symbol} {trade.sl_price}")
        except Exception as e:
            send_tg(f"❗ SL 재설정 실패: {e}")
            ok = False

    # 결과에 따라 실패 카운트 조정
    if ok:
        state.reset_tp_sl_fails()
    else:
        state.inc_tp_sl_fails()
        log(f"[ensure_tp_sl] reapply failed count={state.tp_sl_retry_fails}")

    return ok


# ─────────────────────────────
# 열린 포지션의 TP/SL 체결 확인
# ─────────────────────────────def check_closes(open_trades: List[Trade], state: TraderState) -> Tuple[List[Trade], List[Dict[str, Any]]]:
    """열려 있는 포지션들에 대해 TP/SL 체결 여부를 확인하고 결과를 리턴한다.

    반환값:
        (still_open, closed_results)

    - still_open: 여전히 열려 있는 Trade 들
    - closed_results: [{"trade": Trade, "reason": "TP"/"SL", "summary": {...}} ...]
      여기서 summary 는 summarize_fills 의 결과이며, 없으면 None 일 수 있다.
    """
    if not open_trades:
        return [], []

    still_open: List[Trade] = []
    closed_results: List[Dict[str, Any]] = []

    for t in open_trades:
        symbol = t.symbol
        tp_id = t.tp_order_id
        sl_id = t.sl_order_id
        closed = False

        # 1) TP 체크
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

        # 2) SL 체크 (TP 안 됐을 때만)
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

        # 3) 아직 안 닫혔다면 TP/SL 이 둘 다 살아 있는지 확인해서 필요하면 다시 건다.
        if not closed:
            ok = ensure_tp_sl_for_trade(t, state)
            if not ok:
                # 재설정조차 실패 → 안전하게 포지션 시장가로 닫아버리고 닫힌 것으로 처리
                close_position_market(symbol, t.side, t.qty)
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
