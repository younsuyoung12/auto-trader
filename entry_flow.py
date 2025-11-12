"""
entry_flow.py (WS 거래량 우선 버전)
----------------------------------------------------
역할
- 시그널 받기 → 각종 가드 → 주문 열기 → 로그/캔들 스냅샷을 한 함수로 묶어서
  run_bot.py / run_bot_ws.py 에서 한 줄로 호출할 수 있게 한다.
- 2025-11-13 보정: 웹소켓으로 받은 1m 캔들을 우선으로 써서 거래량 가드를 돌리고,
  스냅샷도 그걸 기준으로 남기도록 했다. 웹소켓 버퍼가 비어 있으면 예전 3m 데이터로 대체한다.
- 2025-11-14 보강: entry_guards_ws 가 추가로 depth 쏠림 / mark-last 괴리 / 세션별 스프레드·점프
  를 보게 됐으므로, 이 파일에서는 그걸 그대로 호출만 해 주면 된다. (이미 import 되어 있음)
  별도 로직은 필요 없고, 웹소켓 1m 캔들을 먼저 가져오는 순서만 유지하면 된다.

반환값
----------------------------------------------------
try_open_new_position(...) -> (Trade | None, float)
- Trade 인스턴스가 있으면 실제로 진입에 성공한 것
- 두 번째 값은 run_bot 이 잠깐 쉬어야 할 시간(sec)
"""

from __future__ import annotations

import time
from typing import Any, Optional, Tuple, List

from telelog import send_tg, log
from signal_flow_ws import get_trading_signal
from entry_guards_ws import (
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

# ✅ 웹소켓 시세 버퍼에서 거래량까지 있는 캔들을 가져오기
#    없으면 나중에 fallback 한다.
try:
    from market_data_ws import get_klines_with_volume as ws_get_klines_with_volume
except ImportError:
    ws_get_klines_with_volume = None  # 패키지 없으면 그냥 None 처리


def _pick_latest_candle(
    ws_candles: Optional[List[tuple]],
    signal_candles_raw: Optional[list],
    signal_candles: Optional[list],
) -> Tuple[int, float, float, float, float, float]:
    """
    캔들 스냅샷에 쓸 (ts, o, h, l, c, v) 하나를 고른다.
    1) 웹소켓 1m가 있으면 그걸 최우선
    2) 시그널이 내려준 raw 캔들이 있으면 그걸 다음
    3) 그마저 없으면 시그널이 내려준 (o,h,l,c)만 있는 캔들에 v=0 붙여서 쓴다.
    """
    # 1) 웹소켓 캔들 우선
    if ws_candles:
        last = ws_candles[-1]
        # ws_candles 는 (ts, o, h, l, c, v) 형태라고 가정
        if len(last) >= 6:
            return last  # 그대로 리턴

    # 2) 시그널이 준 raw 캔들
    if signal_candles_raw and len(signal_candles_raw[-1]) >= 6:
        raw_c = signal_candles_raw[-1]
        c_ts = raw_c[0]
        o = raw_c[1]
        h = raw_c[2]
        l = raw_c[3]
        c = raw_c[4]
        v = raw_c[5]
        return c_ts, o, h, l, c, v

    # 3) 시그널이 준 기본 캔들
    if signal_candles and len(signal_candles[-1]) >= 5:
        base_c = signal_candles[-1]
        c_ts = base_c[0]
        o = base_c[1]
        h = base_c[2]
        l = base_c[3]
        c = base_c[4]
        v = 0.0
        return c_ts, o, h, l, c, v

    # 4) 진짜 아무 것도 없으면 대충 현재시각으로 빈 캔들
    now_ts = int(time.time() * 1000)
    return now_ts, 0.0, 0.0, 0.0, 0.0, 0.0


def try_open_new_position(
    settings: Any,
    last_trend_close_ts: float,
    last_range_close_ts: float,
) -> Tuple[Optional[Trade], float]:
    """
    시그널을 받아서 조건이 되면 실제로 포지션을 연다.
    아무 것도 못 열면 (None, sleep_sec) 을 돌려준다.
    """
    # (1) 시그널 받기 (WS 버전)
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
        candles_5m,        # 이름은 그대로지만 WS 신호기는 5m 기준으로 내려줄 것
        candles_5m_raw,
        last_price,
        extra,
    ) = signal_ctx

    # ✅ (1-1) 웹소켓에서 1m 캔들을 한 번 땡겨본다. 없으면 None.
    ws_candles_1m = None
    if ws_get_klines_with_volume is not None:
        try:
            ws_candles_1m = ws_get_klines_with_volume(settings.symbol, "1m", 40)
        except Exception as e:
            log(f"[ENTRY][WS] get 1m candles failed: {e}")
            ws_candles_1m = None

    # (1-2) 시그널 시점 캔들 스냅샷 남기기 (WS 1m가 있으면 그걸로)
    try:
        c_ts, o, h, l, c, v = _pick_latest_candle(
            ws_candles=ws_candles_1m,
            signal_candles_raw=candles_5m_raw,
            signal_candles=candles_5m,
        )
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

    # ✅ (2-1) 거래량 가드는 웹소켓 1m 캔들이 있으면 그걸로 돌리고,
    #          없으면 신호기가 준 5m raw 를 그대로 넘겨준다.
    vol_source = ws_candles_1m if ws_candles_1m else candles_5m_raw
    vol_ok = check_volume_guard(
        settings=settings,
        candles_5m_raw=vol_source,
        latest_ts=latest_ts,
        signal_source=signal_source,
        direction=chosen_signal,
    )
    if not vol_ok:
        return None, 1.0

    price_ok = check_price_jump_guard(
        settings=settings,
        candles_5m=candles_5m,
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
