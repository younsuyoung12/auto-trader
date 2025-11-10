"""
trader.py
포지션 진입/TP·SL 설정/유지/체결 확인을 담당하는 모듈.

2025-11-12 7차 수정
----------------------------------------------------
(요청한 “들어가자마자 손절 빈도 줄이기”를 실행 레벨에서 보완)

이전까지는 전략(strategies_*.py)에서 TP/SL 퍼센트를 예쁘게 계산해 와도,
실제 거래 시점(entry)에서
- 슬리피지가 조금 있었거나
- 전략이 박스라서 SL을 아주 좁게 줬거나
하면 선물 변동성에 그대로 맞고 나가는 일이 있었다.

그래서 여기(trader.py)에서 한 번 더 안전장치를 둔다.

1) 설정에 있는 최소 TP/SL 하한을 실제 주문 직전에 강제 반영
   - settings.min_tp_pct, settings.min_sl_pct 가 있으면
     전략이 0.004 같은 걸 줘도 최소값(예: 0.005)까지는 끌어올린다.
   - “너무 좁은” SL로 들어가는 걸 막기 위한 최종 방어선이다.

2) 방향별 SL 바닥값을 한 번 더 적용
   - settings.range_short_sl_floor_ratio (예: 0.75)를 trader 레벨에서도 받아서
     숏(SELL) 진입일 때
        sl_pct = max(sl_pct, tp_pct * range_short_sl_floor_ratio)
     를 마지막으로 한 번 더 해준다.
   - 이렇게 하면 전략이 잘못 좁게 준 경우에도
     “위로 한 번만 털려도 SL”을 줄일 수 있다.
   - run_bot 이 이 값을 안 줘도 settings 에서 읽어오므로 호환성은 그대로다.

3) soft 모드 시 TP 살짝만 낮추기
   - open_position_with_tp_sl(...) 에 soft_mode: bool 파라미터를 추가했다.
   - soft_mode=True 로 들어오면
       tp_pct = min(tp_pct, settings.range_tp_min * settings.range_soft_tp_factor)
     로 살짝만 눌러서 “오늘은 애매하니까 빨리 먹고 나오자” 형태를 만들 수 있다.
   - run_bot 이 이 파라미터를 넘기지 않으면 기존과 동일하게 동작한다.

4) 선물(마진) 기준 TP/SL 옵션을 여기서도 받아서 최종 퍼센트로 변환
   - settings.use_margin_based_tp_sl 이 켜져 있으면
     fut_tp_margin_pct, fut_sl_margin_pct 를
       (마진% / 100) / 레버리지
     로 바꾼 값을 기존 tp_pct/sl_pct 와 비교해 더 큰 쪽을 쓴다.
   - 원래는 run_bot 쪽에서 해도 되지만, 여기서도 한 번 더 보정하면
     호출부가 단순해진다.

이렇게 하면 전략/가드에서 한 번, 실제 주문 직전에서 한 번 더 좁은 SL을 걸러서
“들어가자마자 잘리는” 패턴을 줄일 수 있다.

----------------------------------------------------
2025-11-11 6차 수정  ← 이전 버전 설명
(단방향 exchange_api 와 맞춰 TP/SL 을 체결 이후에만 거는 구조, 슬리피지 초과시 즉시 청산 등)
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
    """
    우리 봇이 열어 둔 포지션 1건을 표현하는 구조체.
    run_bot.py 에서는 이걸 리스트로만 관리하면 된다.
    """

    symbol: str               # 예: "BTC-USDT"
    side: str                 # "BUY" 또는 "SELL" (진입 방향)
    qty: float                # 실제 진입 수량
    entry: float              # 실제 진입가 (거래소 체결가)
    entry_order_id: Optional[str] = None  # 시장가 진입 주문 ID
    tp_order_id: Optional[str] = None     # 예약된 TP 주문 ID
    sl_order_id: Optional[str] = None     # 예약된 SL 주문 ID
    tp_price: Optional[float] = None      # TP 가격
    sl_price: Optional[float] = None      # SL 가격
    source: str = "UNKNOWN"               # "TREND" / "RANGE" / "SYNC" 등, 어디서 열었는지 구분


@dataclass
class TraderState:
    """
    트레이더 레벨에서 유지해야 하는 런타임 상태.
    TP/SL 재설정이 계속 실패하면 봇을 멈추기 위해 카운터를 둔다.
    """

    tp_sl_retry_fails: int = 0
    max_tp_sl_retry_fails: int = 3

    def reset_tp_sl_fails(self) -> None:
        """TP/SL 을 정상적으로 다시 걸었을 때 카운터 리셋"""
        self.tp_sl_retry_fails = 0

    def inc_tp_sl_fails(self) -> None:
        """TP/SL 다시 걸기가 실패했을 때 카운터 +1"""
        self.tp_sl_retry_fails += 1

    def should_stop_bot(self) -> bool:
        """연속 실패 횟수가 한도를 넘었는지 확인"""
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
    """
    진입 방향(BUY/SELL)과 퍼센트로 실제 TP/SL 가격을 계산한다.

    - 롱(BUY): TP = entry * (1 + tp_pct), SL = entry * (1 - sl_pct)
    - 숏(SELL): TP = entry * (1 - tp_pct), SL = entry * (1 + sl_pct)
    """
    if entry <= 0:
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
    settings: Any,              # run_bot.py 에서 load_settings() 해 온 객체
    symbol: str,
    side_open: str,
    qty: float,
    entry_price_hint: float,    # run_bot 이 당시 봉에서 계산한 “대략 이 가격쯤 진입” 힌트
    tp_pct: float,
    sl_pct: float,
    source: str = "UNKNOWN",
    soft_mode: bool = False,    # 박스가 soft 허용으로 온 경우 run_bot 이 True 로 넘겨줄 수 있음
    sl_floor_ratio: Optional[float] = None,  # 외부에서 강제로 SL 바닥비율을 주고 싶을 때
) -> Optional[Trade]:
    """
    1) 시장가로 진입하고
    2) 실제로 FILLED 되었는지 확인한 다음
    3) 그 가격으로 TP/SL 을 두 개 다 거는 고수준 함수.

    ⚠️ 아주 중요한 순서:
      (a) place_market(...) 으로 먼저 주문을 “보낸다”.
      (b) wait_filled(...) 로 거래소가 실제 체결해줬는지 “확인한다”.
      (c) 그 다음에만 place_conditional(...) 로 TP 와 SL 을 “각각” 건다.
      → 중간에 하나라도 실패하면 포지션을 열어두지 않고 종료(return None).
    """
    # 1) 시장가 진입 시도
    try:
        resp = place_market(symbol, side_open, qty)
    except Exception as e:
        send_tg(f"[ENTRY][{source}] ❌ 시장가 진입 실패: {e}")
        return None

    # ── 시장가 응답 파싱 ─────────────────────────────────
    data = resp.get("data") or resp
    entry_order_id = (
        data.get("orderId")
        or data.get("id")
        or data.get("orderID")
    )
    if not entry_order_id and isinstance(data, dict):
        # 중첩 케이스
        order_obj = data.get("order")
        if isinstance(order_obj, dict):
            entry_order_id = (
                order_obj.get("orderId")
                or order_obj.get("orderID")
                or order_obj.get("id")
            )

    if not entry_order_id:
        # 여기서 멈추면 TP/SL 도 절대 안 건다.
        send_tg("[ENTRY] ⚠️ 시장가 진입 응답에 orderId 가 없어 포지션을 건너뜁니다.")
        return None

    # 2) 진입 체결(FILLED) 대기
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

    # 3) 슬리피지 가드
    max_slip_pct = getattr(settings, "max_entry_slippage_pct", 0.0)
    if max_slip_pct and entry_price_hint and entry_price_hint > 0:
        slip_pct = abs(entry_price - entry_price_hint) / entry_price_hint
        if slip_pct > max_slip_pct:
            # 진입은 됐는데 가격이 너무 나쁘다 → 바로 시장가로 닫기
            close_side = "SELL" if side_open == "BUY" else "BUY"
            try:
                close_position_market(symbol, close_side, filled_qty)
            except Exception as e:
                send_tg(
                    f"[ENTRY][{source}] ❗ 슬리피지 {slip_pct:.5f} > {max_slip_pct:.5f} 라서 닫으려 했으나 실패: {e}"
                )
            else:
                send_tg(
                    f"[ENTRY][{source}] ❌ 슬리피지 {slip_pct:.5f} > {max_slip_pct:.5f} → 포지션 취소"
                )
            return None

    # ─────────────────────────────────────────────
    # 3.5) 여기서부터는 “진짜로 열기로 한” 포지션에 대해
    #      TP/SL 퍼센트를 한 번 더 안전하게 보정한다.
    # ─────────────────────────────────────────────

    # (a) 선물(마진) 기준 TP/SL 이 켜져 있으면 우선 반영
    if getattr(settings, "use_margin_based_tp_sl", False):
        lev = getattr(settings, "leverage", 1) or 1
        fut_tp_margin_pct = getattr(settings, "fut_tp_margin_pct", 0.0)
        fut_sl_margin_pct = getattr(settings, "fut_sl_margin_pct", 0.0)
        # (마진% / 100) / 레버리지 → 가격 퍼센트
        m_tp = (fut_tp_margin_pct / 100.0) / lev if lev > 0 else 0.0
        m_sl = (fut_sl_margin_pct / 100.0) / lev if lev > 0 else 0.0
        tp_pct = max(tp_pct, m_tp)
        sl_pct = max(sl_pct, m_sl)

    # (b) 설정에서 최소 TP/SL 이 정해져 있으면 그보다 좁게는 안 쓰도록 한다.
    min_tp_pct = getattr(settings, "min_tp_pct", 0.0)
    min_sl_pct = getattr(settings, "min_sl_pct", 0.0)
    if min_tp_pct > 0:
        tp_pct = max(tp_pct, min_tp_pct)
    if min_sl_pct > 0:
        sl_pct = max(sl_pct, min_sl_pct)

    # (c) soft 모드면 TP 를 너무 멀리 두지 않도록 살짝만 제한
    if soft_mode:
        tp_min = getattr(settings, "range_tp_min", tp_pct)
        soft_factor = getattr(settings, "range_soft_tp_factor", 1.0)
        # 예: tp_min=0.0035, factor=1.2 → 0.0042 이상은 안 쓰게
        tp_soft_cap = tp_min * soft_factor
        tp_pct = min(tp_pct, tp_soft_cap)

    # (d) 숏일 때는 SL 바닥 비율을 한 번 더 적용 (들어가자마자 손절 방지)
    #     외부에서 sl_floor_ratio 를 넘겨주면 그걸 우선하고, 없으면 settings 값을 쓴다.
    eff_sl_floor_ratio = sl_floor_ratio or getattr(settings, "range_short_sl_floor_ratio", 0.0)
    if side_open == "SELL" and eff_sl_floor_ratio > 0 and tp_pct > 0:
        min_short_sl = tp_pct * eff_sl_floor_ratio
        if sl_pct < min_short_sl:
            sl_pct = min_short_sl

    # 4) TP/SL 가격 계산 (이 시점에는 entry_price 가 확정되어 있음)
    tp_price, sl_price = compute_tp_sl_prices(
        side_open=side_open,
        entry=entry_price,
        tp_pct=tp_pct,
        sl_pct=sl_pct,
        precision=2,
    )
    close_side = "SELL" if side_open == "BUY" else "BUY"

    # 5) TP/SL 두 개 주문 실제 전송
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
        # 한쪽이라도 실패하면 포지션을 열어두면 안 된다.
        send_tg(f"[ENTRY][{source}] ❌ TP/SL 예약 실패: {e}, 포지션을 즉시 닫습니다.")
        close_position_market(symbol, close_side, filled_qty)
        return None

    # ───── TP/SL 응답에서도 중첩 구조를 확인하는 헬퍼 ─────
    def _norm_id(r: Dict[str, Any]) -> Optional[str]:
        """
        BingX 가
          { "data": { "orderId": ... } }
        도 주고,
          { "data": { "order": { "orderId": ... } } }
        도 주기 때문에 두 경우를 모두 처리한다.
        """
        d = r.get("data") or r
        # 1) 평평한 구조
        oid = d.get("orderId") or d.get("id") or d.get("orderID")
        if oid:
            return str(oid)
        # 2) 중첩 구조
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

    # 여기까지 오면 포지션 1건이 완성
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
    """
    포지션은 살아 있는데 TP/SL 주문이 없는 경우 다시 건다.

    ⚠️ source == "SYNC" 인 건 손대지 않는다. (사람이 연 포지션)
    """
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
    """
    열린 포지션 리스트를 받아서,
    TP/SL 이 체결된 애들은 closed_results 로 빼고,
    나머지는 still_open 에 남겨서 run_bot.py 에 돌려준다.
    """
    if not open_trades:
        return [], []

    still_open: List[Trade] = []
    closed_results: List[Dict[str, Any]] = []

    for t in open_trades:
        # 사람이/외부에서 열어준 포지션은 그대로 둔다.
        if t.source == "SYNC":
            still_open.append(t)
            continue

        symbol = t.symbol
        tp_id = t.tp_order_id
        sl_id = t.sl_order_id
        closed = False

        # 1) TP 체결 여부
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

        # 2) SL 체결 여부
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

        # 3) TP/SL 다시 걸기 또는 강제청산
        if not closed:
            ok = ensure_tp_sl_for_trade(t, state)
            if not ok:
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
