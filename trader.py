"""
trader.py
포지션 진입/TP·SL 설정/유지/체결 확인을 담당하는 모듈.

2025-11-09 1차 수정
- settings 에서 추가된 max_entry_slippage_pct 값을 사용해서
  "실제 체결가(entry_price)가 run_bot 이 넘겨준 진입 힌트(entry_price_hint)보다
  너무 나쁘면(=슬리피지 초과) 바로 시장가로 닫고 포지션을 포기"하는 가드를 추가했다.
  → 선물에서 TP/SL 이 아주 짧을 때(예: 0.05% ~ 0.2%) 슬리피지로 바로 손실 나는 걸 막기 위함.
- 진입 자체는 성공했어도 슬리피지가 과하면 TP/SL 을 안 깔고 None 을 리턴하도록 했다.
  (짧은 TP 전략 보호용. 상위 run_bot.py 는 None 을 받으면 "이번 진입은 실패"로만 기록하면 된다.)

2025-11-09 2차 설명 보강
- 지금 구조에서는 "레버리지 몇 배냐", "선물 마진 기준으로 몇 % 먹겠다"를
  run_bot.py 에서 이미 계산해서 tp_pct / sl_pct 로 넘겨준다.
  이 모듈(trader.py)은 그 퍼센트를 "그대로 가격에 적용"만 한다.
  즉, 여기서는 선물이냐 현물이냐를 다시 판단하지 않는다.
  → 우리는 선물 기준 TP/SL 을 run_bot.py 에서 만들어서 넘긴다. (이미 그렇게 바꿔놨음)
- 그래서 아래 compute_tp_sl_prices(...) / open_position_with_tp_sl(...) 는
  "넘어온 퍼센트를 원시 가격(entry)에 곱해서 TP/SL 가격을 만든다"는 역할만 남겨두었다.

2025-11-09 3차 수정 (수동/동기화 포지션 보호)
- run_bot.py 가 거래소에서 그대로 가져와서 source="SYNC" 로 넣어준 포지션은
  봇이 TP/SL 을 다시 깔거나, 강제 청산하거나, 체결 확인 대상으로 삼지 않도록 했다.
- 즉, 사람이 수동으로 연 포지션(=거래소에 이미 있던 것)은 봇이 익절/손절을 건드리지 않고
  그대로 둔다. 봇이 직접 연 것(source가 TREND/RANGE/UNKNOWN 등)만 관리한다.
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

    run_bot.py 에서는 Trade 인스턴스를 리스트로만 관리하면 된다.
    """

    symbol: str               # 예: "BTC-USDT"
    side: str                 # "BUY" 또는 "SELL" (진입 방향)
    qty: float                # 진입 수량
    entry: float              # 실제 진입가 (거래소 체결가)
    entry_order_id: Optional[str] = None  # 진입 주문 ID (시장가)
    tp_order_id: Optional[str] = None     # 예약된 TP 주문 ID
    sl_order_id: Optional[str] = None     # 예약된 SL 주문 ID
    tp_price: Optional[float] = None      # TP 가격
    sl_price: Optional[float] = None      # SL 가격
    source: str = "UNKNOWN"               # 이 포지션이 어떤 전략에서 열렸는지 ("TREND" / "RANGE" / "SYNC" ...)


@dataclass
class TraderState:
    """트레이더 레벨에서 유지해야 하는 런타임 상태.

    - TP/SL 을 다시 걸어야 할 때마다 실패할 수 있는데, 이게 연속으로 여러 번 실패하면
      전체 봇을 멈추도록 run_bot.py 쪽에 시그널을 주어야 한다.
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

    ⚠️ 여기서 tp_pct, sl_pct 는 이미 run_bot.py 에서
       "선물 마진 기준 → 실제 가격변동률"로 변환되어 들어온 값이라고 가정한다.

    - 롱이면 TP = entry * (1 + tp_pct), SL = entry * (1 - sl_pct)
    - 숏이면 TP = entry * (1 - tp_pct), SL = entry * (1 + sl_pct)
    - 거래소가 소숫점 2자리까지 지원하는 경우가 많아서 기본을 2자리로 맞춰 둔다.
    """
    if entry <= 0:
        # 방어
        entry = 0.0

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
    settings: Any,              # run_bot.py 에서 load_settings() 한 객체를 그대로 받는다
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
    - (2025-11-09 추가) 실제 체결가가 힌트보다 설정값 이상으로 나쁘게 체결된 경우
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
        send_tg("[ENTRY] ⚠️ 시장가 진입 응답에 orderId 가 없어 포지션을 건너뜁니다.")
        return None

    # 2) 진입 체결 대기
    filled = wait_filled(symbol, entry_order_id, timeout=5)
    if not filled:
        send_tg("[ENTRY] ⚠️ 시장가 주문이 제한 시간 내 FILLED 되지 않아 포지션을 건너뜁니다.")
        return None

    # 체결 수량/가격 추출
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

    # ───────── 슬리피지 가드 ─────────
    max_slip_pct = getattr(settings, "max_entry_slippage_pct", 0.0)
    if max_slip_pct and entry_price_hint and entry_price_hint > 0:
        slip_pct = abs(entry_price - entry_price_hint) / entry_price_hint
        if slip_pct > max_slip_pct:
            # 진입은 됐지만 가격이 너무 나쁘니 바로 닫는다
            try:
                close_position_market(symbol, side_open, filled_qty)
            except Exception as e:
                send_tg(
                    f"[ENTRY][{source}] ❗ 슬리피지 {slip_pct:.5f} > {max_slip_pct:.5f} 라서 닫으려 했으나 실패: {e}"
                )
            else:
                send_tg(
                    f"[ENTRY][{source}] ❌ 슬리피지 {slip_pct:.5f} > {max_slip_pct:.5f} → 포지션 취소"
                )
            return None

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
        send_tg(f"[ENTRY][{source}] ❌ TP/SL 예약 실패: {e}, 포지션을 즉시 닫습니다.")
        close_position_market(symbol, close_side, filled_qty)
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

    ⚠️ 단, source == "SYNC" 인 포지션은 사람이/외부에서 연 걸로 보고 손대지 않는다.
    """
    # 사람이 수동으로 열어서 run_bot 이 SYNC 한 포지션이라면 그대로 둔다.
    if trade.source == "SYNC":
        return True

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

    ⚠️ 여기서도 source == "SYNC" 인 포지션은
       "사람이 거래소에서 직접 열어둔 것"으로 보고 건드리지 않는다.
       → TP/SL 확인 안 하고, 다시 깔지도 않고, 강제청산도 하지 않는다.
    """
    if not open_trades:
        return [], []

    still_open: List[Trade] = []
    closed_results: List[Dict[str, Any]] = []

    for t in open_trades:
        # 사람이/외부에서 열어서 run_bot 이 동기화만 한 포지션
        if t.source == "SYNC":
            still_open.append(t)
            continue

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

        # 2) SL 체크
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

        # 3) TP/SL 재설정 혹은 강제청산
        if not closed:
            ok = ensure_tp_sl_for_trade(t, state)
            if not ok:
                # 여기까지 왔다는 건 봇이 연 포지션이므로 강제 정리 가능
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
