"""
entry_flow.py
역할
----------------------------------------------------
시그널 받기 → 각종 가드 → 주문 열기 → 로그/캔들 스냅샷을
한 함수로 묶어서 run_bot.py 에서 한 줄로 호출할 수 있게 만든다.

반환값
----------------------------------------------------
try_open_new_position(...) -> (Trade | None, float)
- Trade 인스턴스가 있으면 실제로 진입에 성공한 것
- 두 번째 값은 run_bot 이 잠깐 쉬어야 할 시간(sec)

2025-11-12: run_bot.py 에 있던 동일 로직을 그대로 옮김.
"""

from __future__ import annotations

import time
from typing import Any, Optional, Tuple

from telelog import send_tg, log
from signal_flow import get_trading_signal
from entry_guards import (
    check_manual_position_guard,
    check_volume_guard,
    check_price_jump_guard,
    check_spread_guard,
)
from exchange_api import (
    get_available_usdt,
    get_balance_detail,
)
from trader import open_position_with_tp_sl, Trade
from signals_logger import log_signal, log_candle_snapshot


def try_open_new_position(
    settings: Any,
    last_trend_close_ts: float,
    last_range_close_ts: float,
) -> Tuple[Optional[Trade], float]:
    """
    시그널을 받아서 조건이 되면 실제로 포지션을 연다.
    아무 것도 못 열면 (None, sleep_sec) 을 돌려준다.
    """
    # (1) 시그널 받기
    signal_ctx = get_trading_signal(
        settings=settings,
        last_trend_close_ts=last_trend_close_ts,
        last_range_close_ts=last_range_close_ts,
    )
    if signal_ctx is None:
        return None, 1.0

    (
        chosen_signal,     # "LONG"/"SHORT"
        signal_source,     # "TREND"/"RANGE"
        latest_ts,
        candles_3m,
        candles_3m_raw,
        last_price,
        extra,
    ) = signal_ctx

    # 시그널 시점 캔들 스냅샷 남기기
    try:
        if candles_3m_raw and len(candles_3m_raw[-1]) >= 6:
            raw_c = candles_3m_raw[-1]
            c_ts = raw_c[0]
            o = raw_c[1]
            h = raw_c[2]
            l = raw_c[3]
            c = raw_c[4]
            v = raw_c[5]
        else:
            base_c = candles_3m[-1]
            c_ts = base_c[0]
            o = base_c[1]
            h = base_c[2]
            l = base_c[3]
            c = base_c[4]
            v = 0.0

        log_candle_snapshot(
            symbol=settings.symbol,
            tf=settings.interval,
            candle_ts=c_ts,
            open_=o,
            high=h,
            low=l,
            close=c,
            volume=v,
            strategy_type=signal_source,
            direction="LONG" if chosen_signal == "LONG" else "SHORT",
            extra="signal_eval=1",
        )
    except Exception as e:
        log(f"[SIGNAL_SNAPSHOT] log failed: {e}")

    # (2) 진입 전 가드
    manual_ok = check_manual_position_guard(
        get_balance_detail_func=get_balance_detail,
        symbol=settings.symbol,
        latest_ts=latest_ts,
    )
    if not manual_ok:
        return None, 2.0

    vol_ok = check_volume_guard(
        settings=settings,
        candles_3m_raw=candles_3m_raw,
        latest_ts=latest_ts,
        signal_source=signal_source,
        direction=chosen_signal,
    )
    if not vol_ok:
        return None, 1.0

    price_ok = check_price_jump_guard(
        settings=settings,
        candles_3m=candles_3m,
        latest_ts=latest_ts,
        signal_source=signal_source,
        direction=chosen_signal,
    )
    if not price_ok:
        return None, 1.0

    spread_ok, best_bid, best_ask = check_spread_guard(
        settings=settings,
        symbol=settings.symbol,
        latest_ts=latest_ts,
        signal_source=signal_source,
        direction=chosen_signal,
    )
    if not spread_ok:
        return None, 1.0

    # (3) 잔고 확인
    avail = get_available_usdt()
    if avail <= 0:
        send_tg("[BALANCE_SKIP] 가용 선물 잔고 0 → 진입 안함")
        log_signal(
            event="SKIP",
            symbol=settings.symbol,
            strategy_type=signal_source,
            direction=chosen_signal,
            reason="balance_zero",
            candle_ts=latest_ts,
        )
        return None, 3.0

    # (4) 주문 수량 산출
    effective_risk_pct = extra.get("effective_risk_pct", settings.risk_pct) if extra else settings.risk_pct
    notional = avail * effective_risk_pct * settings.leverage
    if notional < settings.min_notional_usdt:
        send_tg(f"⚠️ 주문 금액이 너무 작습니다: {notional:.2f} USDT")
        log_signal(
            event="SKIP",
            symbol=settings.symbol,
            strategy_type=signal_source,
            direction=chosen_signal,
            reason="notional_too_small",
            candle_ts=latest_ts,
        )
        return None, 3.0

    notional = min(notional, settings.max_notional_usdt)
    qty = round(notional / last_price, 6)
    side_open = "BUY" if chosen_signal == "LONG" else "SELL"

    # (5) TP/SL 결정
    tp_pct = extra.get("tp_pct") if extra else None
    sl_pct = extra.get("sl_pct") if extra else None
    if tp_pct is None or sl_pct is None:
        if signal_source == "RANGE":
            tp_pct = settings.range_tp_pct
            sl_pct = settings.range_sl_pct
        else:
            tp_pct = settings.tp_pct
            sl_pct = settings.sl_pct

    # (6) 호가 기반 진입가 힌트
    entry_price_hint = last_price
    if settings.use_orderbook_entry_hint and best_bid and best_ask:
        entry_price_hint = best_ask if side_open == "BUY" else best_bid

    # (7) trader 에 넘길 보강 플래그
    soft_mode_flag = False
    sl_floor_ratio = None
    if extra:
        soft_mode_flag = bool(
            extra.get("soft_mode")
            or extra.get("soft")
            or extra.get("range_soft")
        )
        sl_floor_ratio = extra.get("sl_floor_ratio")

    # (8) 최종 진입
    trade = open_position_with_tp_sl(
        settings=settings,
        symbol=settings.symbol,
        side_open=side_open,
        qty=qty,
        entry_price_hint=entry_price_hint,
        tp_pct=tp_pct,
        sl_pct=sl_pct,
        source=signal_source,
        soft_mode=soft_mode_flag,
        sl_floor_ratio=sl_floor_ratio,
    )
    if trade is None:
        log_signal(
            event="SKIP",
            symbol=settings.symbol,
            strategy_type=signal_source,
            direction=chosen_signal,
            reason="entry_failed",
            candle_ts=latest_ts,
        )
        return None, 2.0

    # (9) 알림 / 로그
    side_ko = "롱" if trade.side == "BUY" else "숏"
    strat_ko = "추세" if signal_source == "TREND" else "박스"
    if getattr(settings, "notify_on_entry", True):
        send_tg(
            f"[ENTRY] {strat_ko} {side_ko} 진입\n"
            f"- 심볼: {trade.symbol}\n"
            f"- 진입가: {trade.entry:.2f}\n"
            f"- 수량: {trade.qty}\n"
            f"- TP: {trade.tp_price} / SL: {trade.sl_price}"
        )

    log_signal(
        event="ENTRY_OPENED",
        symbol=trade.symbol,
        strategy_type=trade.source,
        direction=trade.side,
        price=trade.entry,
        qty=trade.qty,
        tp_price=trade.tp_price,
        sl_price=trade.sl_price,
        candle_ts=latest_ts,
    )

    # 성공적으로 진입했으니 메인 루프 기본 쿨다운을 settings 쪽에서 하도록  반환
    return trade, float(settings.cooldown_sec)
