"""
signal_flow.py
====================================================
시그널 결정 전담 모듈.
run_bot.py 에서 한 줄로 호출해서 시그널/캔들 세트를 받아가게 한다.

2025-11-10 추가 ①:
- 기존에는 추세(TREND)가 안 나왔을 때 CSV(signals_logger)에만 남았고 콘솔에는 안 보였다.
- 운영 중에 “왜 맨날 RANGE만 나오냐”를 바로 보이게 하려고,
  추세가 막힌 3가지 케이스에도 log(...) 를 추가해서 콘솔에 찍히도록 변경했다.
  (1) 3m 추세 신호 없음
  (2) 15m 방향 판단 불가
  (3) 3m/15m 방향 불일치

2025-11-10 추가 ② (RANGE 완화):
- should_block_range_today(...)가 True를 주더라도,
  그 캔들에 대해서는 한 번은 decide_signal_range(...)를 시도해본다.
- 이때 나온 RANGE 신호는 signals_logger에 reason="range_relaxed_entry"로 남긴다.
- 이렇게 해서 “추세 없음 → RANGE 차단”으로 신호가 0건이 되는 상황을 줄인다.

2025-11-10 추가 ③ (운영 편의):
- settings 에 range_ignore_daily_block 이라는 플래그가 있으면
  하루 박스장 차단을 강제로 무시하도록 했다. (기본값은 False 로 간주)
- 완화 RANGE 까지 시도했는데도 신호가 없으면 굳이
  "[SKIP] range_blocked_today" 를 또 찍지 않고 조용히 None 반환하게 해서
  로그 스팸을 줄였다.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Tuple

from telelog import log, send_skip_tg
from signals_logger import log_signal
from market_data import get_klines, get_klines_with_volume
from indicators import calc_atr
from strategies_trend import (
    decide_signal_3m_trend,
    decide_trend_15m,
    confirm_1m_direction,
)
from strategies_range import (
    decide_signal_range,
    should_block_range_today,
)


def get_trading_signal(
    *,
    settings: Any,
    last_trend_close_ts: float,
    last_range_close_ts: float,
) -> Optional[Tuple[str, str, int, List[Any], List[Any], float, Dict[str, Any]]]:
    """시그널을 하나 결정해서 관련 캔들/부가정보와 함께 넘긴다."""
    symbol = settings.symbol

    # 1) 3m 캔들 (거래량 포함) 가져오기
    try:
        candles_3m_raw = get_klines_with_volume(symbol, settings.interval, 120)
    except Exception as e:
        log(f"[KLINE ERROR] 3m fetch failed: {e}")
        return None

    if not candles_3m_raw or len(candles_3m_raw) < 50:
        return None

    # 전략에서 쓰는 형태로 변환 (ts,o,h,l,c)
    candles_3m = [c[:5] for c in candles_3m_raw]
    latest_3m_ts = candles_3m[-1][0]
    last_price = candles_3m[-1][4]

    # 2) 캔들 지연 체크
    now_ms = int(time.time() * 1000)
    if now_ms - latest_3m_ts > settings.max_kline_delay_sec * 1000:
        send_skip_tg("[SKIP] 3m_kline_delayed: 최근 3m 캔들이 지연되었습니다.")
        log_signal(
            event="SKIP",
            symbol=symbol,
            strategy_type="UNKNOWN",
            reason="3m_kline_delayed",
            candle_ts=latest_3m_ts,
        )
        return None

    # ─────────────────────────────
    # 3) 엄격 추세 먼저
    # ─────────────────────────────
    chosen_signal: Optional[str] = None
    signal_source: Optional[str] = None
    extra: Dict[str, Any] = {}
    candles_15m: Optional[List[Any]] = None

    if settings.enable_trend:
        # 15m 가져오기
        candles_15m = get_klines(symbol, "15m", 120)
        if candles_15m:
            log(
                f"[DATA] 15m klines ok: symbol={symbol} count={len(candles_15m)} last_close={candles_15m[-1][4]}"
            )

        # 3m 추세 판단
        sig_3m = decide_signal_3m_trend(
            candles_3m,
            settings.rsi_overbought,
            settings.rsi_oversold,
        )
        trend_15m_val = decide_trend_15m(candles_15m) if candles_15m else None

        # 추세 실패 사유: CSV + 콘솔
        if not sig_3m:
            log_signal(
                event="SKIP",
                symbol=symbol,
                strategy_type="TREND",
                reason="trend_3m_no_signal",
                candle_ts=latest_3m_ts,
            )
            log("[TREND_SKIP] 3m 추세 신호 없음")
        elif not trend_15m_val:
            log_signal(
                event="SKIP",
                symbol=symbol,
                strategy_type="TREND",
                reason="trend_15m_unknown",
                candle_ts=latest_3m_ts,
            )
            log("[TREND_SKIP] 15m 방향 판단 불가")
        elif sig_3m != trend_15m_val:
            log_signal(
                event="SKIP",
                symbol=symbol,
                strategy_type="TREND",
                reason="trend_3m_15m_mismatch",
                candle_ts=latest_3m_ts,
            )
            log("[TREND_SKIP] 3m/15m 방향 불일치")

        # 추세 진입
        if sig_3m and trend_15m_val and sig_3m == trend_15m_val:
            if (time.time() - last_trend_close_ts) >= settings.cooldown_after_close_trend:
                chosen_signal = sig_3m
                signal_source = "TREND"
                # 1분봉 확인 옵션
                if settings.enable_1m_confirm:
                    candles_1m = get_klines(symbol, "1m", 20)
                    if not confirm_1m_direction(candles_1m, chosen_signal):
                        send_skip_tg("[SKIP] 1m_confirm_mismatch: 1분봉이 방향 불일치")
                        log_signal(
                            event="SKIP",
                            symbol=symbol,
                            strategy_type="TREND",
                            direction=chosen_signal,
                            reason="1m_confirm_mismatch",
                            candle_ts=latest_3m_ts,
                            trend_15m=trend_15m_val,
                        )
                        chosen_signal = None
                        signal_source = None
            else:
                send_skip_tg("[SKIP] trend_cooldown: 직전 TREND 포지션 대기중")
                log_signal(
                    event="SKIP",
                    symbol=symbol,
                    strategy_type="TREND",
                    direction=sig_3m,
                    reason="trend_cooldown",
                    candle_ts=latest_3m_ts,
                    trend_15m=trend_15m_val,
                )

    # ─────────────────────────────
    # 4) 추세가 안 됐을 때 RANGE 시도
    # ─────────────────────────────
    if (not chosen_signal) and settings.enable_range:
        if not candles_15m:
            candles_15m = get_klines(symbol, "15m", 120)

        if (time.time() - last_range_close_ts) >= settings.cooldown_after_close_range:
            # 새 플래그: 하루 박스장 차단을 무시하고 싶으면 settings.range_ignore_daily_block = True
            ignore_daily_block = bool(getattr(settings, "range_ignore_daily_block", False))

            blocked_now = should_block_range_today(candles_3m, candles_15m)
            if ignore_daily_block:
                blocked_now = False

            if not blocked_now:
                # 원래대로 RANGE
                r_sig = decide_signal_range(candles_3m)
                if r_sig:
                    ok_range = True
                    if settings.enable_1m_confirm:
                        candles_1m = get_klines(symbol, "1m", 20)
                        if not confirm_1m_direction(candles_1m, r_sig):
                            ok_range = False
                    if ok_range:
                        chosen_signal = r_sig
                        signal_source = "RANGE"
                    else:
                        send_skip_tg("[SKIP] 1m_confirm_mismatch_range")
                        log_signal(
                            event="SKIP",
                            symbol=symbol,
                            strategy_type="RANGE",
                            direction=r_sig,
                            reason="1m_confirm_mismatch_range",
                            candle_ts=latest_3m_ts,
                        )
            else:
                # 완화: 막혀도 한 번은 RANGE 형태로 본다
                log("[INFO] range blocked for this candle, trying relaxed RANGE")
                r_sig = decide_signal_range(candles_3m)
                if r_sig:
                    ok_range = True
                    if settings.enable_1m_confirm:
                        candles_1m = get_klines(symbol, "1m", 20)
                        if not confirm_1m_direction(candles_1m, r_sig):
                            ok_range = False
                    if ok_range:
                        chosen_signal = r_sig
                        signal_source = "RANGE"
                        log_signal(
                            event="ENTRY_SIGNAL",
                            symbol=symbol,
                            strategy_type="RANGE",
                            direction=r_sig,
                            reason="range_relaxed_entry",
                            candle_ts=latest_3m_ts,
                        )
                    else:
                        # 예전에는 여기서도 range_blocked_today 를 찍었는데,
                        # 완화 시도까지 했으면 조용히 넘기는 게 운영상 덜 시끄럽다.
                        return None
                else:
                    # 진짜로 박스 신호가 없으면 그냥 조용히 끝낸다.
                    return None
        else:
            send_skip_tg("[SKIP] range_cooldown: 직전 RANGE 포지션 대기중")
            log_signal(
                event="SKIP",
                symbol=symbol,
                strategy_type="RANGE",
                reason="range_cooldown",
                candle_ts=latest_3m_ts,
            )

    # ─────────────────────────────
    # 5) 여기까지 해도 시그널이 없으면 종료
    # ─────────────────────────────
    if not chosen_signal or not signal_source:
        return None

    # ─────────────────────────────
    # 6) ATR 기반 리스크 정보
    # ─────────────────────────────
    if settings.use_atr:
        atr_fast = calc_atr(candles_3m, settings.atr_len)
        atr_slow = calc_atr(candles_3m, max(settings.atr_len * 2, settings.atr_len + 10))
        extra["atr_fast"] = atr_fast
        extra["atr_slow"] = atr_slow
        if (
            atr_fast
            and atr_slow
            and atr_slow > 0
            and atr_fast > atr_slow * settings.atr_risk_high_mult
        ):
            extra["effective_risk_pct"] = settings.risk_pct * settings.atr_risk_reduction

    return (
        chosen_signal,
        signal_source,
        latest_3m_ts,
        candles_3m,
        candles_3m_raw,
        last_price,
        extra,
    )
