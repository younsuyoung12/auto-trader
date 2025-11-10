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

★ 2025-11-10 추가 ③ (TREND 완화):
- 지금 시장처럼 15m 방향은 있는데 3m 추세가 애매해서 계속
  "[TREND_SKIP] 3m 추세 신호 없음" 이 찍히는 경우,
  3m가 없고 15m만 있으면 그걸 ‘소프트 추세’로 받아들이도록 완화했다.
- 즉,
    3m 없음 AND 15m 있음 AND 쿨다운 끝
  이면 TREND 로 진입을 허용한다.
- 이렇게 하면 “왜 추세 안 나오지?” 하는 구간이 훨씬 줄어든다.
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
    # 3) 추세 먼저 본다
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

        # 3m / 15m 방향
        sig_3m = decide_signal_3m_trend(
            candles_3m,
            settings.rsi_overbought,
            settings.rsi_oversold,
        )
        trend_15m_val = decide_trend_15m(candles_15m) if candles_15m else None

        # 기본 진단 로그
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

        # ───── 여기서부터 완화 로직 추가 ─────
        # 3m 는 없지만 15m 는 있는 경우 → 15m 만이라도 따라가자
        soft_trend_dir: Optional[str] = None
        if (not sig_3m) and trend_15m_val:
            soft_trend_dir = trend_15m_val
            log("[TREND_SOFT] 3m 없음 → 15m 방향만으로 추세 진입 시도")

        # 추세 진입 조건
        if (sig_3m and trend_15m_val and sig_3m == trend_15m_val) or soft_trend_dir:
            # 쿨다운 체크
            if (time.time() - last_trend_close_ts) >= settings.cooldown_after_close_trend:
                # 실제로 쓸 방향 고르기
                final_dir = sig_3m if (sig_3m and trend_15m_val and sig_3m == trend_15m_val) else soft_trend_dir
                chosen_signal = final_dir
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
                    direction=sig_3m or soft_trend_dir or "",
                    reason="trend_cooldown",
                    candle_ts=latest_3m_ts,
                    trend_15m=trend_15m_val,
                )

    # ─────────────────────────────
    # 4) 추세가 안 됐을 때 RANGE 시도 (이건 네가 올린 최신 로직 유지)
    # ─────────────────────────────
    if (not chosen_signal) and settings.enable_range:
        if not candles_15m:
            candles_15m = get_klines(symbol, "15m", 120)

        if (time.time() - last_range_close_ts) >= settings.cooldown_after_close_range:
            blocked_now = should_block_range_today(candles_3m, candles_15m)

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
                        send_skip_tg("[SKIP] range_blocked_today: 박스장 조건 불리")
                        log_signal(
                            event="SKIP",
                            symbol=symbol,
                            strategy_type="RANGE",
                            reason="range_blocked_today",
                            candle_ts=latest_3m_ts,
                        )
                else:
                    send_skip_tg("[SKIP] range_blocked_today: 박스장 조건 불리")
                    log_signal(
                        event="SKIP",
                        symbol=symbol,
                        strategy_type="RANGE",
                        reason="range_blocked_today",
                        candle_ts=latest_3m_ts,
                    )
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
    # 6) ATR 기반 리스크 정보 (기존 그대로)
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
