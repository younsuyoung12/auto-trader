"""
position_watch.py
열려 있는 포지션 모니터링/후처리 모듈

2025-11-12 변경 요약
----------------------------------------------------
1) 박스(RANGE) 포지션 조기익절 / 조기청산 실행
   - settings.range_early_tp_enabled, settings.range_early_tp_pct
     → 박스로 들어가고 조금이라도 이익 나면 TP 오기 전에 먼저 익절
   - settings.range_early_exit_enabled, settings.range_early_exit_loss_pct
     → 박스로 들어가자마자 반대로 세게 밀리면 거래소 TP/SL 안 기다리고 먼저 손절
   - 체크할 때마다 캔들 스냅샷을 signals_logger.log_candle_snapshot(...) 으로 남김

2) 박스 → 추세 전환 감지
   - 지금 포지션이 RANGE 이고
   - 새로 본 실시간 시그널이 TREND 이고
   - 방향이 같고 (롱↔LONG, 숏↔SHORT)
   - 15m 추세도 같은 방향이고
   - 진입가 대비 일정 이익 이상이면
     → "박스를 추세로 이어가려던 상황" 으로 보고 이득일 때 시장가로 닫아서 확정
   - 이유/가격/수량을 텔레그램 + signals_logger 로 남김

3) 역할 분리
   - 이 파일은 “이미 열려 있는 포지션을 지금 닫을지 말지”만 판단한다.
   - 메인 루프(run_bot.py)는 이 함수들을 호출만 하고,
     실제로 닫혔는지 여부(True/False)만 보고 내부 OPEN_TRADES 리스트를 정리한다.

4) ✅ 추세(TREND) 포지션 조기익절 / 조기청산 추가
   - settings.trend_early_tp_enabled, settings.trend_early_tp_pct
     → 추세로 들어갔는데 초반에 원하는 만큼 이익이 나면 거래소 TP 전에 먼저 익절
   - settings.trend_early_exit_enabled, settings.trend_early_exit_loss_pct
     → 추세로 들어갔는데 생각보다 빨리 옆으로 가거나 역행하면 SL 기다리지 않고 먼저 정리
   - 박스와 동일하게 캔들 스냅샷 남김

5) ✅ 추세(TREND) 포지션 ‘횡보 감지’ 조기청산 추가
   - settings.trend_sideways_enabled = True 일 때만 동작
   - 최근 N개 캔들(high-low)이 아주 좁고
   - 진입가 대비 수익/손실이 거의 0 근처일 때
     → “추세가 죽었다”고 보고 시장가로 먼저 닫는다

6) ✅ 실시간 확인용 로그 추가
   - 각 조기처리 함수가 캔들을 가져오기 직전에 무엇을 감시하는지 로그로 남겨
     “매수 후에도 계속 캔들 가져오는지”를 확인할 수 있게 함

7) ✅ 진입 후 1분봉 보조 체크 추가
   - 추세 포지션일 때 기본 주기(예: 3m)로는 아직 횡보가 확실하지 않아도
     1m 캔들 여러 개가 진입가 근처에서 아주 좁게 나오면 “초기 횡보”로 보고 자른다.

8) ✅ 진입 후 ‘정반대’ 시그널(TREND/RANGE) 감지 시 즉시 청산
   - 현재가 TREND 롱인데 새 시그널이 TREND 숏이면 닫고
   - 현재가 TREND 숏인데 새 시그널이 TREND 롱이면 닫고
   - 현재가 RANGE 롱/숏인데 새 시그널이 반대 방향(TREND이든 RANGE이든)이면 닫는다
   - run_bot.py 에서는 maybe_force_close_on_opposite_signal(...) 이름으로만 호출하면 된다.
"""

from __future__ import annotations

from typing import Any

from telelog import log, send_tg
from market_data import get_klines
from exchange_api import close_position_market
from signal_flow import get_trading_signal
from strategies_trend import decide_trend_15m
from signals_logger import log_signal, log_candle_snapshot
from trader import Trade


# ─────────────────────────────────────────
# 15m 추세 헬퍼
# ─────────────────────────────────────────
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


def _is_price_near_entry(trade: Trade, last_price: float, max_abs_pnl_pct: float) -> bool:
    """
    진입가 대비 현 시세가 거의 안 움직였는지 확인.
    """
    if trade.entry <= 0 or last_price <= 0:
        return False
    pnl_pct = (last_price - trade.entry) / trade.entry
    if trade.side == "SELL":
        pnl_pct = -pnl_pct
    return abs(pnl_pct) <= max_abs_pnl_pct


def _all_candles_narrow(candles, need_bars: int, max_range_pct: float) -> bool:
    """
    최근 need_bars 개 캔들이 전부 얇은(rng_pct <= max_range_pct)지 확인.
    """
    if not candles or len(candles) < need_bars:
        return False
    for c in candles[-need_bars:]:
        high = float(c[2])
        low = float(c[3])
        close = float(c[4])
        if close <= 0:
            return False
        rng_pct = (high - low) / close
        if rng_pct > max_range_pct:
            return False
    return True


# ─────────────────────────────────────────
# 1) 박스 조기익절 / 조기청산
# ─────────────────────────────────────────
def maybe_early_exit_range(trade: Trade, settings: Any) -> bool:
    """
    박스(RANGE) 포지션에 대한 조기익절/조기청산을 수행한다.
    실제로 포지션을 닫으면 True, 아무 것도 안 하면 False.
    캔들을 조회한 시점의 시세는 log_candle_snapshot 으로 남긴다.
    """
    if trade.source != "RANGE":
        return False

    # 실시간 확인용 로그
    log(f"[PW] RANGE watch symbol={trade.symbol} tf={settings.interval}")

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

    # 캔들 스냅샷
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

    # 2) 조기청산
    if not getattr(settings, "range_early_exit_enabled", False):
        return False

    trigger_pct = getattr(settings, "range_early_exit_loss_pct", 0.003)

    if trade.side == "BUY":
        if last_close >= trade.entry:
            return False
        loss_pct = (trade.entry - last_close) / trade.entry
    else:
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


# ─────────────────────────────────────────
# 2) 박스 → 추세 전환 감지
# ─────────────────────────────────────────
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

    # 실시간 확인용 로그
    log(f"[PW] RANGE→TREND recheck symbol={trade.symbol}")

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

    if new_signal_source != "TREND":
        return False

    if trade.side == "BUY" and new_signal_dir != "LONG":
        return False
    if trade.side == "SELL" and new_signal_dir != "SHORT":
        return False

    trend15 = _get_15m_trend_dir(trade.symbol)
    if trade.side == "BUY" and trend15 != "LONG":
        return False
    if trade.side == "SELL" and trend15 != "SHORT":
        return False

    upgrade_thresh = getattr(settings, "range_to_trend_min_gain_pct", 0.002)
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


# ─────────────────────────────────────────
# 3) 추세(TREND) 포지션 조기익절 / 조기청산
# ─────────────────────────────────────────
def maybe_early_exit_trend(trade: Trade, settings: Any) -> bool:
    """
    추세(TREND) 포지션인데 진입 직후에 생각보다 강하게 안 가거나
    바로 역행할 때 TP/SL 보다 먼저 정리하는 로직.
    """
    if trade.source != "TREND":
        return False

    # 실시간 확인용 로그
    log(f"[PW] TREND watch symbol={trade.symbol} tf={settings.interval}")

    try:
        candles = get_klines(trade.symbol, settings.interval, 3)
    except Exception as e:
        log(f"[TREND_EARLY] kline fetch error: {e}")
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

    # 캔들 스냅샷
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
            strategy_type="TREND",
            direction=trade.side,
            extra="trend_early_check=1",
        )
    except Exception as e:
        log(f"[TREND_EARLY] candle snapshot log failed: {e}")

    last_close = c

    # 1) 추세 조기익절
    if getattr(settings, "trend_early_tp_enabled", False):
        trend_early_tp_pct = getattr(settings, "trend_early_tp_pct", 0.0025)
        if trade.side == "BUY" and last_close > trade.entry:
            gain_pct = (last_close - trade.entry) / trade.entry
            if gain_pct >= trend_early_tp_pct:
                try:
                    close_position_market(trade.symbol, trade.side, trade.qty)
                    pnl = (last_close - trade.entry) * trade.qty
                    send_tg(
                        f"⚠️ 추세 조기익절 실행: {trade.symbol} {trade.side} "
                        f"진입={trade.entry:.2f} 현재={last_close:.2f} "
                        f"수익={(gain_pct * 100):.2f}%"
                    )
                    log_signal(
                        event="CLOSE",
                        symbol=trade.symbol,
                        strategy_type="TREND",
                        direction=trade.side,
                        price=last_close,
                        qty=trade.qty,
                        reason="trend_early_tp_runtime",
                        pnl=pnl,
                    )
                    return True
                except Exception as e:
                    log(f"[TREND_EARLY_TP] close failed: {e}")

        elif trade.side == "SELL" and last_close < trade.entry:
            gain_pct = (trade.entry - last_close) / trade.entry
            if gain_pct >= trend_early_tp_pct:
                try:
                    close_position_market(trade.symbol, trade.side, trade.qty)
                    pnl = (trade.entry - last_close) * trade.qty
                    send_tg(
                        f"⚠️ 추세 조기익절 실행: {trade.symbol} {trade.side} "
                        f"진입={trade.entry:.2f} 현재={last_close:.2f} "
                        f"수익={(gain_pct * 100):.2f}%"
                    )
                    log_signal(
                        event="CLOSE",
                        symbol=trade.symbol,
                        strategy_type="TREND",
                        direction=trade.side,
                        price=last_close,
                        qty=trade.qty,
                        reason="trend_early_tp_runtime",
                        pnl=pnl,
                    )
                    return True
                except Exception as e:
                    log(f"[TREND_EARLY_TP] close failed: {e}")

    # 2) 추세 조기청산
    if not getattr(settings, "trend_early_exit_enabled", False):
        return False

    trend_trigger_pct = getattr(settings, "trend_early_exit_loss_pct", 0.003)

    if trade.side == "BUY":
        if last_close >= trade.entry:
            return False
        loss_pct = (trade.entry - last_close) / trade.entry
    else:
        if last_close <= trade.entry:
            return False
        loss_pct = (last_close - trade.entry) / trade.entry

    if loss_pct < trend_trigger_pct:
        return False

    try:
        close_position_market(trade.symbol, trade.side, trade.qty)
        if trade.side == "BUY":
            pnl = (last_close - trade.entry) * trade.qty
        else:
            pnl = (trade.entry - last_close) * trade.qty

        send_tg(
            f"⚠️ 추세 조기청산 실행: {trade.symbol} {trade.side} "
            f"진입={trade.entry:.2f} 현재={last_close:.2f} "
            f"역행={(loss_pct * 100):.2f}%"
        )
        log_signal(
            event="CLOSE",
            symbol=trade.symbol,
            strategy_type="TREND",
            direction=trade.side,
            price=last_close,
            qty=trade.qty,
            reason="trend_early_exit_runtime",
            pnl=pnl,
        )
        return True
    except Exception as e:
        log(f"[TREND_EARLY_EXIT] close failed: {e}")
        return False


# ─────────────────────────────────────────
# 4) 추세(TREND) 포지션 ‘횡보 감지’ 조기청산 (+ 1m 보조)
# ─────────────────────────────────────────
def maybe_sideways_exit_trend(trade: Trade, settings: Any) -> bool:
    """
    추세 진입을 했는데 가격이 진입가 주변에서 좁은 캔들만 연속으로 나오면
    '추세가 죽었다'고 보고 먼저 닫는다.
    기본 주기(예: 3m)로 먼저 보고,
    설정이 허용되면 1m 캔들로도 같은 조건을 한 번 더 본다.
    """
    if trade.source != "TREND":
        return False

    enabled = getattr(settings, "trend_sideways_enabled", True)
    if not enabled:
        return False

    need_bars = getattr(settings, "trend_sideways_need_bars", 3)
    max_range_pct = getattr(settings, "trend_sideways_range_pct", 0.0008)
    max_abs_pnl_pct = getattr(settings, "trend_sideways_max_pnl_pct", 0.0015)

    # 실시간 확인용 로그
    log(
        f"[PW] TREND sideways check symbol={trade.symbol} tf={settings.interval} "
        f"bars={need_bars} max_rng={max_range_pct} max_pnl={max_abs_pnl_pct}"
    )

    # ① 기본 주기로 확인
    try:
        candles = get_klines(trade.symbol, settings.interval, need_bars)
    except Exception as e:
        log(f"[TREND_SIDEWAYS] kline fetch error: {e}")
        return False

    if not candles or len(candles) < need_bars:
        return False

    last_close = float(candles[-1][4])
    if last_close <= 0 or trade.entry <= 0:
        return False
    pnl_pct = (last_close - trade.entry) / trade.entry
    if trade.side == "SELL":
        pnl_pct = -pnl_pct

    # 기본 주기에서 이미 어느 정도 움직였으면 여기서는 안 자른다.
    if abs(pnl_pct) > max_abs_pnl_pct:
        return False

    # 기본 주기가 전부 좁은지
    base_narrow = _all_candles_narrow(candles, need_bars, max_range_pct)

    # ② 1분봉 보조 확인 (옵션, 기본값 True)
    use_1m = getattr(settings, "trend_sideways_use_1m", True)
    narrow_1m = False
    if use_1m:
        one_min_need = getattr(settings, "trend_sideways_1m_need_bars", 3)
        one_min_max_rng = getattr(settings, "trend_sideways_1m_range_pct", 0.0006)
        log(
            f"[PW] TREND sideways 1m check symbol={trade.symbol} bars={one_min_need} "
            f"max_rng={one_min_max_rng}"
        )
        try:
            candles_1m = get_klines(trade.symbol, "1m", one_min_need)
            if candles_1m and len(candles_1m) >= one_min_need:
                last_1m_close = float(candles_1m[-1][4])
                # 진입가 근처인지 다시 확인
                if _is_price_near_entry(trade, last_1m_close, max_abs_pnl_pct):
                    narrow_1m = _all_candles_narrow(candles_1m, one_min_need, one_min_max_rng)
        except Exception as e:
            log(f"[TREND_SIDEWAYS 1m] kline fetch error: {e}")

    # 기본 주기 OR 1m 둘 중 하나라도 “진입가 근처에서 납작”이면 닫는다.
    if not base_narrow and not narrow_1m:
        return False

    # 스냅샷 남기기
    try:
        latest = candles[-1]
        log_candle_snapshot(
            symbol=trade.symbol,
            tf=settings.interval,
            candle_ts=latest[0],
            open_=latest[1],
            high=latest[2],
            low=latest[3],
            close=latest[4],
            volume=latest[5] if len(latest) > 5 else 0.0,
            strategy_type="TREND",
            direction=trade.side,
            extra="trend_sideways_detected=1",
        )
    except Exception as e:
        log(f"[TREND_SIDEWAYS] candle snapshot log failed: {e}")

    # 닫기
    try:
        close_position_market(trade.symbol, trade.side, trade.qty)
        pnl = (
            (last_close - trade.entry) * trade.qty
            if trade.side == "BUY"
            else (trade.entry - last_close) * trade.qty
        )

        send_tg(
            f"⚠️ 추세 포지션 횡보 감지 → 조기청산: {trade.symbol} {trade.side} "
            f"진입={trade.entry:.2f} 현재={last_close:.2f} "
            f"pnl≈{(pnl_pct * 100):.3f}%"
        )
        log_signal(
            event="CLOSE",
            symbol=trade.symbol,
            strategy_type="TREND",
            direction=trade.side,
            price=last_close,
            qty=trade.qty,
            reason="trend_sideways_exit",
            pnl=pnl,
        )
        return True
    except Exception as e:
        log(f"[TREND_SIDEWAYS] close failed: {e}")
        return False


# ─────────────────────────────────────────
# 5) 진입 후 “반대 시그널” 감지 시 강제 청산
# ─────────────────────────────────────────
def maybe_close_on_opposite_signal(
    trade: Trade,
    settings: Any,
    last_trend_close_ts: float,
    last_range_close_ts: float,
) -> bool:
    """
    - 지금 들고 있는 포지션이 TREND 인데 새 시그널이 반대 TREND 로 나오면 닫는다.
    - 지금 들고 있는 포지션이 RANGE 인데 새 시그널이 반대 방향(TREND 이든 RANGE 이든)이면 닫는다.
    - '반대' 판단은 매수(BUY) ↔ LONG, 매도(SELL) ↔ SHORT 로 본다.
    """
    try:
        sig_ctx = get_trading_signal(
            settings=settings,
            last_trend_close_ts=last_trend_close_ts,
            last_range_close_ts=last_range_close_ts,
        )
    except Exception as e:
        log(f"[OPP] re-signal fetch error: {e}")
        return False

    if sig_ctx is None:
        return False

    new_dir, new_src, _ts, _c3, _c3raw, last_price, _extra = sig_ctx

    # 현재 포지션 방향 → 시그널 방향 매핑
    def _is_opposite(trade_side: str, sig_dir: str) -> bool:
        if trade_side == "BUY" and sig_dir == "SHORT":
            return True
        if trade_side == "SELL" and sig_dir == "LONG":
            return True
        return False

    # (1) TREND 포지션인데 새 TREND 가 정반대일 때
    if trade.source == "TREND" and new_src == "TREND":
        if _is_opposite(trade.side, new_dir):
            log(f"[OPP] TREND opposite detected → close {trade.symbol}")
            try:
                close_position_market(trade.symbol, trade.side, trade.qty)
                pnl = (
                    (last_price - trade.entry) * trade.qty
                    if trade.side == "BUY"
                    else (trade.entry - last_price) * trade.qty
                )
                send_tg(
                    f"⚠️ 추세 포지션 반대 TREND 시그널 감지 → 즉시 청산: {trade.symbol} "
                    f"{trade.side}→{new_dir} 현재={last_price:.2f}"
                )
                log_signal(
                    event="CLOSE",
                    symbol=trade.symbol,
                    strategy_type="TREND",
                    direction=trade.side,
                    price=last_price,
                    qty=trade.qty,
                    reason="trend_opposite_signal_close",
                    pnl=pnl,
                )
                return True
            except Exception as e:
                log(f"[OPP] close failed: {e}")
                return False

    # (2) RANGE 포지션인데 새 시그널이 정반대 방향이면 (RANGE이든 TREND이든) 닫는다
    if trade.source == "RANGE":
        if _is_opposite(trade.side, new_dir):
            log(f"[OPP] RANGE opposite detected → close {trade.symbol}")
            try:
                close_position_market(trade.symbol, trade.side, trade.qty)
                pnl = (
                    (last_price - trade.entry) * trade.qty
                    if trade.side == "BUY"
                    else (trade.entry - last_price) * trade.qty
                )
                send_tg(
                    f"⚠️ 박스 포지션 반대 시그널 감지 → 즉시 청산: {trade.symbol} "
                    f"{trade.side}→{new_dir} 현재={last_price:.2f}"
                )
                log_signal(
                    event="CLOSE",
                    symbol=trade.symbol,
                    strategy_type="RANGE",
                    direction=trade.side,
                    price=last_price,
                    qty=trade.qty,
                    reason="range_opposite_signal_close",
                    pnl=pnl,
                )
                return True
            except Exception as e:
                log(f"[OPP] close failed: {e}")
                return False

    return False


# run_bot.py 에서 쓸 이름으로 한 번 더 감싸준다.
# (거기서는 maybe_force_close_on_opposite_signal(...) 으로 호출)
def maybe_force_close_on_opposite_signal(
    trade: Trade,
    settings: Any,
    last_trend_close_ts: float,
    last_range_close_ts: float,
) -> bool:
    return maybe_close_on_opposite_signal(
        trade, settings, last_trend_close_ts, last_range_close_ts
    )
