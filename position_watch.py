"""
position_watch.py
역할
----------------------------------------------------
1) 박스(RANGE) 포지션 조기익절 / 조기청산
   - settings.range_early_tp_enabled, settings.range_early_tp_pct
   - settings.range_early_exit_enabled, settings.range_early_exit_loss_pct
   - 캔들을 가져올 때마다 시세 스냅샷을 candles-YYYY-MM-DD.csv 로 남긴다.

2) 박스 → 추세 전환 감지
   - 지금 포지션이 RANGE
   - 새로 본 시그널이 TREND
   - 방향이 동일
   - 15m 추세도 동일
   - 진입가 대비 일정 이익 이상
   이면 시장가로 닫고 로그/텔레그램을 남긴다.

주의
----------------------------------------------------
- 이 모듈은 "열려 있는 포지션을 어떻게 처리할지"만 담당한다.
- 메인 루프(run_bot.py)는 이 모듈의 함수만 호출해서 포지션을 닫을지 말지 결정한다.
"""

from __future__ import annotations

from typing import Any, Optional

from telelog import log, send_tg
from market_data import get_klines
from exchange_api import close_position_market
from signal_flow import get_trading_signal
from strategies_trend import decide_trend_15m
from signals_logger import log_signal, log_candle_snapshot
from trader import Trade


def _get_15m_trend_dir(symbol: str) -> str:
    """
    15m 캔들을 가져와서 대략적인 방향만 문자열로 돌려준다.
    RANGE → TREND 전환 시 필터로 쓴다.
    """
    try:
        candles_15m = get_klines(symbol, "15m", 120)
        if not candles_15m:
            return ""
        trimmed = [c[:5] for c in candles_15m]
        trend_dir = decide_trend_15m(trimmed)
        return trend_dir or ""
    except Exception as e:
        log(f"[15m DIR] error: {e}")
        return ""


def maybe_early_exit_range(trade: Trade, settings: Any) -> bool:
    """
    박스(RANGE) 포지션에 대한 조기익절/조기청산을 수행한다.
    실제로 포지션을 닫으면 True, 아무 것도 안 하면 False.
    캔들을 조회한 시점의 시세는 log_candle_snapshot 으로 남긴다.
    """
    # 박스 포지션이 아니면 패스
    if trade.source != "RANGE":
        return False

    # 최신 캔들 2~3개만 가져오면 충분
    try:
        candles = get_klines(trade.symbol, settings.interval, 3)
    except Exception as e:
        log(f"[EARLY_EXIT] kline fetch error: {e}")
        return False

    if not candles:
        return False

    latest_candle = candles[-1]
    candle_ts = latest_candle[0]
    o = latest_candle[1]
    h = latest_candle[2]
    l = latest_candle[3]
    c = latest_candle[4]
    vol = latest_candle[5] if len(latest_candle) > 5 else 0.0

    # 조회한 캔들을 그대로 저장
    try:
        log_candle_snapshot(
            symbol=trade.symbol,
            tf=settings.interval,
            candle_ts=candle_ts,
            open_=o,
            high=h,
            low=l,
            close=c,
            volume=vol,
            strategy_type="RANGE",
            direction=trade.side,
            extra="early_exit_check=1",
        )
    except Exception as e:
        log(f"[EARLY_EXIT] candle snapshot log failed: {e}")

    last_close = c

    # 1) 조기익절
    if getattr(settings, "range_early_tp_enabled", False):
        early_tp_pct = getattr(settings, "range_early_tp_pct", 0.0025)

        if trade.side == "BUY" and last_close > trade.entry:
            gain_pct = (last_close - trade.entry) / trade.entry
            if gain_pct >= early_tp_pct:
                try:
                    close_position_market(trade.symbol, trade.side, trade.qty)
                    pnl = (last_close - trade.entry) * trade.qty
                    send_tg(
                        f"⚠️ 박스 조기익절 실행: {trade.symbol} {trade.side} "
                        f"진입={trade.entry:.2f} 현재={last_close:.2f} "
                        f"수익={(gain_pct * 100):.2f}%"
                    )
                    log_signal(
                        event="CLOSE",
                        symbol=trade.symbol,
                        strategy_type="RANGE",
                        direction=trade.side,
                        price=last_close,
                        qty=trade.qty,
                        reason="range_early_tp_runtime",
                        pnl=pnl,
                    )
                    return True
                except Exception as e:
                    log(f"[EARLY_TP] close failed: {e}")

        elif trade.side == "SELL" and last_close < trade.entry:
            gain_pct = (trade.entry - last_close) / trade.entry
            if gain_pct >= early_tp_pct:
                try:
                    close_position_market(trade.symbol, trade.side, trade.qty)
                    pnl = (trade.entry - last_close) * trade.qty
                    send_tg(
                        f"⚠️ 박스 조기익절 실행: {trade.symbol} {trade.side} "
                        f"진입={trade.entry:.2f} 현재={last_close:.2f} "
                        f"수익={(gain_pct * 100):.2f}%"
                    )
                    log_signal(
                        event="CLOSE",
                        symbol=trade.symbol,
                        strategy_type="RANGE",
                        direction=trade.side,
                        price=last_close,
                        qty=trade.qty,
                        reason="range_early_tp_runtime",
                        pnl=pnl,
                    )
                    return True
                except Exception as e:
                    log(f"[EARLY_TP] close failed: {e}")

    # 2) 조기청산(역행 컷)
    if not getattr(settings, "range_early_exit_enabled", False):
        return False

    trigger_pct = getattr(settings, "range_early_exit_loss_pct", 0.003)  # 0.3%

    if trade.side == "BUY":
        if last_close >= trade.entry:
            return False
        loss_pct = (trade.entry - last_close) / trade.entry
    else:  # SELL
        if last_close <= trade.entry:
            return False
        loss_pct = (last_close - trade.entry) / trade.entry

    if loss_pct < trigger_pct:
        return False

    try:
        close_position_market(trade.symbol, trade.side, trade.qty)
        if trade.side == "BUY":
            pnl = (last_close - trade.entry) * trade.qty
        else:
            pnl = (trade.entry - last_close) * trade.qty

        send_tg(
            f"⚠️ 박스 조기청산 실행: {trade.symbol} {trade.side} "
            f"진입={trade.entry:.2f} 현재={last_close:.2f} "
            f"역행={(loss_pct * 100):.2f}%"
        )
        log_signal(
            event="CLOSE",
            symbol=trade.symbol,
            strategy_type="RANGE",
            direction=trade.side,
            price=last_close,
            qty=trade.qty,
            reason="range_early_exit_runtime",
            pnl=pnl,
        )
        return True
    except Exception as e:
        log(f"[EARLY_EXIT] close failed: {e}")
        return False


def maybe_upgrade_range_to_trend(
    trade: Trade,
    settings: Any,
    last_trend_close_ts: float,
    last_range_close_ts: float,
) -> bool:
    """
    RANGE 포지션을 들고 있는 상태에서 다시 시그널을 봤더니
    TREND 로 강하게 나올 때 수익권에서 정리하도록 한다.
    실제로 닫으면 True.
    """
    if trade.source != "RANGE":
        return False

    # 최신 시그널 다시 확인
    try:
        sig_ctx = get_trading_signal(
            settings=settings,
            last_trend_close_ts=last_trend_close_ts,
            last_range_close_ts=last_range_close_ts,
        )
    except Exception as e:
        log(f"[R2T] re-signal fetch error: {e}")
        return False

    if sig_ctx is None:
        return False

    (
        new_signal_dir,
        new_signal_source,
        _latest_ts,
        _candles_3m,
        _candles_3m_raw,
        last_price,
        _extra,
    ) = sig_ctx

    # 추세가 아니면 전환 안 함
    if new_signal_source != "TREND":
        return False

    # 방향 일치 여부
    if trade.side == "BUY" and new_signal_dir != "LONG":
        return False
    if trade.side == "SELL" and new_signal_dir != "SHORT":
        return False

    # 15m 추세도 같은 방향일 때만
    trend15 = _get_15m_trend_dir(trade.symbol)
    if trade.side == "BUY" and trend15 != "LONG":
        return False
    if trade.side == "SELL" and trend15 != "SHORT":
        return False

    # 진입가 대비 최소 이익
    upgrade_thresh = getattr(settings, "range_to_trend_min_gain_pct", 0.002)  # 0.2%
    if trade.side == "BUY":
        if last_price <= trade.entry:
            return False
        gain_pct = (last_price - trade.entry) / trade.entry
        if gain_pct < upgrade_thresh:
            return False
    else:
        if last_price >= trade.entry:
            return False
        gain_pct = (trade.entry - last_price) / trade.entry
        if gain_pct < upgrade_thresh:
            return False

    # 시장가로 닫는다
    try:
        close_position_market(trade.symbol, trade.side, trade.qty)
        if trade.side == "BUY":
            pnl = (last_price - trade.entry) * trade.qty
        else:
            pnl = (trade.entry - last_price) * trade.qty

        send_tg(
            f"⚠️ 박스→추세 전환 감지: {trade.symbol} {trade.side} 수익권에서 정리 "
            f"(현재가={last_price:.2f}, 진입={trade.entry:.2f})"
        )
        log_signal(
            event="CLOSE",
            symbol=trade.symbol,
            strategy_type="RANGE",
            direction=trade.side,
            price=last_price,
            qty=trade.qty,
            reason="range_to_trend_upgrade",
            pnl=pnl,
        )
        return True
    except Exception as e:
        log(f"[R2T] close failed: {e}")
        return False
