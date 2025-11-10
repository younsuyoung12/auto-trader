"""
signal_flow.py
====================================================
시그널 결정 전담 모듈.
run_bot.py 에서 한 줄로 호출해서 시그널/캔들 세트를 받아가게 한다.

역할:
1) 3분봉(거래량 포함), 15분봉, (필요 시) 1분봉 캔들 가져오기
2) 엄격 TRND → RANGE → 느슨 TRND 순서로 진입 방향 결정
3) 왜 안 됐는지 signals_logger 에 남기기 (연구/관찰용)
4) 진입에 필요한 부가정보(tp/sl 퍼센트, atr 값, 리스크 비율)도 같이 넘기기

반환값 형식:
    tuple(
        chosen_signal: str,        # "LONG" | "SHORT"
        signal_source: str,        # "TREND" | "RANGE"
        latest_ts: int,           # 마지막 3m 캔들 타임스탬프(ms)
        candles_3m: list,         # (ts,o,h,l,c) 형태로 자른 3분봉 리스트
        candles_3m_raw: list,     # (ts,o,h,l,c,vol) 형태 원본 3분봉 리스트
        last_price: float,        # 진입 기준 가격 (3m 마지막 종가)
        extra: dict,              # tp_pct, sl_pct, effective_risk_pct 등 부가정보
    )
시그널이 없으면 None 반환.
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

    # 3) 엄격 추세 먼저
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

        # 왜 안됐는지 남김
        if not sig_3m:
            log_signal(
                event="SKIP",
                symbol=symbol,
                strategy_type="TREND",
                reason="trend_3m_no_signal",
                candle_ts=latest_3m_ts,
            )
        elif not trend_15m_val:
            log_signal(
                event="SKIP",
                symbol=symbol,
                strategy_type="TREND",
                reason="trend_15m_unknown",
                candle_ts=latest_3m_ts,
            )
        elif sig_3m != trend_15m_val:
            log_signal(
                event="SKIP",
                symbol=symbol,
                strategy_type="TREND",
                reason="trend_3m_15m_mismatch",
                candle_ts=latest_3m_ts,
            )

        # 엄격 추세가 조건을 만족하면 여기서 끝
        if sig_3m and trend_15m_val and sig_3m == trend_15m_val:
            # 전략별 쿨다운
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
                # 추세 기본 TP/SL 은 여기서 굳이 넣지 않고 run_bot 에서 없으면 settings 값 사용
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

    # 4) 추세가 안 됐을 때만 RANGE 시도
    if (not chosen_signal) and settings.enable_range:
        if not candles_15m:
            candles_15m = get_klines(symbol, "15m", 120)
        # RANGE 쿨다운
        if (time.time() - last_range_close_ts) >= settings.cooldown_after_close_range:
            blocked_now = should_block_range_today(candles_3m, candles_15m)
            if not blocked_now:
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
                        # 박스장은 기본적으로 settings.range_tp_pct / range_sl_pct 를 사용할 거라 extra 는 비워둠
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
                # 박스가 이 캔들에서만 불리하면 느슨한 추세로 한 번 더 본다
                log("[INFO] range blocked for this candle, trying loose trend fallback")
                loose_sig = decide_signal_3m_trend(
                    candles_3m,
                    settings.rsi_overbought + 5,
                    settings.rsi_oversold - 5,
                )
                loose_trend_15m = decide_trend_15m(candles_15m) if candles_15m else None
                if loose_sig and loose_trend_15m and loose_sig == loose_trend_15m:
                    chosen_signal = loose_sig
                    signal_source = "TREND"
                    log_signal(
                        event="ENTRY_SIGNAL",
                        symbol=symbol,
                        strategy_type="TREND",
                        direction=loose_sig,
                        reason="range_blocked_fallback_to_loose_trend",
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

    # 5) 여기까지 해도 시그널이 없으면 종료
    if not chosen_signal or not signal_source:
        return None

    # 6) ATR 기반 리스크가 필요한 경우 extra 에 넣어준다 (run_bot 이 사용해도 되고 무시해도 됨)
    if settings.use_atr:
        atr_fast = calc_atr(candles_3m, settings.atr_len)
        atr_slow = calc_atr(candles_3m, max(settings.atr_len * 2, settings.atr_len + 10))
        extra["atr_fast"] = atr_fast
        extra["atr_slow"] = atr_slow
        if atr_fast and atr_slow and atr_slow > 0 and atr_fast > atr_slow * settings.atr_risk_high_mult:
            # 변동성이 높으면 리스크 비율을 줄이도록 힌트 제공
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
