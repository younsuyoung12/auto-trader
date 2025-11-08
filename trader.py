"""trader.py
포지션 진입/TP·SL 설정/유지/체결 확인을 담당하는 모듈.

이 모듈은 저수준 거래소 요청은 exchange_api.py 에 맡기고,
여기서는 그 API 들을 조합해서 우리가 쓰는 형태의 포지션(Trade)을 만들고 관리한다.

run_bot.py 에서는 이 모듈에서 제공하는 아래 네 가지만 가져다 쓰면 된다.
- Trade (dataclass)
- TraderState (TP/SL 재설정 실패 횟수 관리)
- open_position_with_tp_sl(...)  → 진입 + TP/SL 두 개 예약
- check_closes(...)              → 열린 포지션들의 TP/SL 체결 여부 확인
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
# 데이터 구조
# ─────────────────────────────
@dataclass
class Trade:
    """열려 있는 우리 봇 포지션 1건을 표현하는 구조체.

    원래는 dict 로 다뤘지만 가독성을 위해 dataclass 로 고정했다.
    run_bot.py 에서는 Trade 인스턴스를 리스트로만 관리하면 된다.
    """

    symbol: str               # 예: "BTC-USDT"
    side: str                 # "BUY" 또는 "SELL" (진입 방향)
    qty: float                # 진입 수량
    entry: float              # 실제 진입가
    entry_order_id: Optional[str] = None  # 진입 주문 ID (시장가)
    tp_order_id: Optional[str] = None     # 예약된 TP 주문 ID
    sl_order_id: Optional[str] = None     # 예약된 SL 주문 ID
    tp_price: Optional[float] = None      # TP 가격
    sl_price: Optional[float] = None      # SL 가격
    source: str = "UNKNOWN"              # 이 포지션이 어떤 전략에서 열렸는지 ("TREND" / "RANGE" / "SYNC" ...)


@dataclass
class TraderState:
    """트레이더 레벨에서 유지해야 하는 런타임 상태.

    - TP/SL 을 다시 걸어야 할 때마다 실패할 수 있는데, 이게 연속으로 여러 번 실패하면
      전체 봇을 멈추도록 run_bot.py 쪽에 시그널을 주어야 한다.
    - 그 카운트를 여기서 관리한다.
    """

    tp_sl_retry_fails: int = 0           # 현재까지 연속 실패 횟수
    max_tp_sl_retry_fails: int = 3       # 이 횟수를 넘으면 봇 중단 시그널

    def reset_tp_sl_fails(self) -> None:
        """TP/SL 재설정에 성공했을 때 카운트를 0으로 되돌린다."""
        self.tp_sl_retry_fails = 0

    def inc_tp_sl_fails(self) -> None:
        """TP/SL 재설정이 실패했을 때 1 증가시킨다."""
        self.tp_sl_retry_fails += 1

    def should_stop_bot(self) -> bool:
        """연속 실패 횟수가 한계를 넘었는지 여부."""
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
    """진입 방향과 퍼센트로 실제 TP/SL 가격을 계산한다.

    - 롱이면 TP = entry * (1 + tp_pct), SL = entry * (1 - sl_pct)
    - 숏이면 TP = entry * (1 - tp_pct), SL = entry * (1 + sl_pct)
    - 거래소가 소숫점 2자리까지 지원하는 경우가 많아서 기본을 2자리로 맞춰 둔다.
    """
    if side_open == "BUY":
        tp_price = round(entry * (1 + tp_pct), precision)
        sl_price = round(entry * (1 - sl_pct), precision)
    else:  # side_open == "SELL"
        tp_price = round(entry * (1 - tp_pct), precision)
        sl_price = round(entry * (1 + sl_pct), precision)
    return tp_price, sl_price


# ─────────────────────────────
# 포지션 진입 + TP/SL 깔기
# ─────────────────────────────
def open_position_with_tp_sl(
    *,
    settings: Any,              # run_bot.py 에서 load_settings() 한 객체를 그대로 받는다 (여기선 tp/sl 퍼센트만 실제로 사용)
    symbol: str,
    side_open: str,
    qty: float,
    entry_price_hint: float,
    tp_pct: float,
    sl_pct: float,
    source: str = "UNKNOWN",
) -> Optional[Trade]:
    """시장가로 진입하고 곧바로 TP/SL 조건부 주문을 두 개 다 거는 고수준 함수.

    성공하면 Trade 를 리턴하고, 실패하면 None 을 리턴한다.
    실패 케이스는 다음과 같다.
    - 시장가 주문 자체가 실패한 경우
    - 시장가가 시간 내에 FILLED 되지 않은 경우
    - TP/SL 중 하나라도 주문이 실패한 경우 (이때는 시장가로 강제 청산한다)
    """
    # 1) 시장가로 진입
    try:
        resp = place_market(symbol, side_open, qty)
    except Exception as e:  # 네트워크/거래소 오류
        send_tg(f"[ENTRY][{source}] ❌ 시장가 진입 실패: {e}")
        return None

    data = resp.get("data") or resp
    entry_order_id = (
        data.get("orderId")
        or data.get("id")
        or data.get("orderID")
    )
    if not entry_order_id:
        # 주문 ID 가 없으면 TP/SL 을 걸 수가 없다.
        send_tg("[ENTRY] ⚠️ 시장가 진입 응답에 orderId 가 없어 포지션을 건너뜁니다.")
        return None

    # 2) 진입 체결 대기 (기본 5초)
    filled = wait_filled(symbol, entry_order_id, timeout=5)
    if not filled:
        # 시간 내에 체결이 안 되면 여기서 포기
        send_tg("[ENTRY] ⚠️ 시장가 주문이 제한 시간 내 FILLED 되지 않아 포지션을 건너뜁니다.")
        return None

    # 체결 수량/가격 추출 (없으면 힌트를 사용)
    filled_qty = float(
        filled.get("quantity")
        or filled.get("executedQty")
        or qty
    )
    if filled_qty <= 0:
        send_tg("[ENTRY] ⚠️ 시장가 체결 수량이 0입니다. 포지션을 건너뜁니다.")
        return None

    entry_price = float(
        filled.get("avgPrice")
        or entry_price_hint
    )

    # 3) TP/SL 실제 가격 계산
    tp_price, sl_price = compute_tp_sl_prices(
        side_open=side_open,
        entry=entry_price,
        tp_pct=tp_pct,
        sl_pct=sl_pct,
        precision=2,
    )
    close_side = "SELL" if side_open == "BUY" else "BUY"

    # 4) TP/SL 두 개 주문
    try:
        tp_resp = place_conditional(
            symbol,
            close_side,
            filled_qty,
            tp_price,
            "TAKE_PROFIT_MARKET",
        )
        sl_resp = place_conditional(
            symbol,
            close_side,
            filled_qty,
            sl_price,
            "STOP_MARKET",
        )
    except Exception as e:
        # 한쪽이라도 실패하면 포지션을 바로 닫는다.
        send_tg(f"[ENTRY][{source}] ❌ TP/SL 예약 실패: {e}, 포지션을 즉시 닫습니다.")
        close_position_market(symbol, side_open, filled_qty)
        return None

    # 주문 ID 정규화
    def _norm_id(r: Dict[str, Any]) -> Optional[str]:
        d = r.get("data") or r
        return d.get("orderId") or d.get("id") or d.get("orderID")

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
# TP/SL 유지 (사라졌으면 다시 걸기)
# ─────────────────────────────
def ensure_tp_sl_for_trade(trade: Trade, state: TraderState) -> bool:
    """포지션은 살아 있는데 TP/SL 주문이 없는 경우 다시 건다.

    - TP 또는 SL 중 하나만 없어졌으면 그쪽만 다시 건다.
    - 다시 거는 것까지 실패하면 False 를 리턴하고, state 에 실패 횟수를 올린다.
    - run_bot.py 에서는 이 False 를 보면 시장가로 포지션을 정리해버릴 수 있다.
    """
    symbol = trade.symbol
    side_open = trade.side
    qty = trade.qty
    close_side = "SELL" if side_open == "BUY" else "BUY"

    need_tp = False
    need_sl = False

    # TP 상태 확인
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

    # SL 상태 확인
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

    # 필요하면 TP 다시 건다
    if need_tp:
        try:
            tp_r = place_conditional(
                symbol,
                close_side,
                qty,
                trade.tp_price,
                "TAKE_PROFIT_MARKET",
            )
            d = tp_r.get("data") or tp_r
            trade.tp_order_id = d.get("orderId") or d.get("id") or d.get("orderID")
            send_tg(f"🔄 TP 재설정: {symbol} {trade.tp_price}")
        except Exception as e:
            send_tg(f"❗ TP 재설정 실패: {e}")
            ok = False

    # 필요하면 SL 다시 건다
    if need_sl:
        try:
            sl_r = place_conditional(
                symbol,
                close_side,
                qty,
                trade.sl_price,
                "STOP_MARKET",
            )
            d = sl_r.get("data") or sl_r
            trade.sl_order_id = d.get("orderId") or d.get("id") or d.get("orderID")
            send_tg(f"🔄 SL 재설정: {symbol} {trade.sl_price}")
        except Exception as e:
            send_tg(f"❗ SL 재설정 실패: {e}")
            ok = False

    # 결과에 따라 상태 업데이트
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
    """열린 포지션 목록을 받아서 TP/SL 체결 여부를 확인하고 결과를 돌려준다.

    반환값은 (still_open, closed_list) 이다.
    - still_open: 아직 열려 있는 포지션들
    - closed_list: 이번에 닫힌 포지션 정보 목록. 각 원소는
        {
            "trade": Trade 인스턴스,
            "reason": "TP" | "SL" | "FORCE_CLOSE",
            "summary": summarize_fills(...) 결과 또는 None,
        }
      형태이다.
    run_bot.py 는 이 closed_list 를 받아서 텔레그램으로 알리고, PnL 을 계산한다.
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

        # 2) SL 체크 (TP 안 된 경우에만)
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

        # 3) 아직 안 닫혔으면 TP/SL 이 둘 다 살아 있는지 다시 확인해서 필요시 재설정한다.
        if not closed:
            ok = ensure_tp_sl_for_trade(t, state)
            if not ok:
                # 재설정조차 실패 → 포지션을 강제 시장가 청산하고 닫힌 것으로 처리
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
