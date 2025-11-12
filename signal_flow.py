"""
signal_flow.py
====================================================
시그널 결정 전담 모듈.
run_bot.py 에서 한 줄로 호출해서 시그널/캔들 세트를 받아가게 한다.

2025-11-12 추가/변경
----------------------------------------------------
1) 3m/15m/1m 캔들 가져올 때마다 로그를 남기도록 보강
   - Render 콘솔에서 "지금 어떤 타임프레임을 몇 개 가져갔는지"를 실시간으로 확인할 수 있게 함
   - 진입 전에도 3m/15m/1m 캔들이 실제로 요청되는지 확인용

2) 추세(TREND) 시도 → 1분봉 확인까지 전부 로그로 남김
   - 3m 추세 없음 / 15m 판단 불가 / 3m-15m 불일치 / 1m 확인 불일치
     전부 log_signal + log(...) 로 남겨서 원인 추적 가능하게 함

3) 박스(RANGE) 시도 시에도 15m 캔들 보정, 1m 확인 시점 로그 남김
   - 박스가 soft block 인 경우에도 어느 타이밍에 1m를 다시 가져가는지 볼 수 있음

기존 설명
----------------------------------------------------
- 시그널을 하나 결정해서 관련 캔들/부가정보와 함께 넘긴다.
- TREND → RANGE 순으로 기회를 본다.
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

# RANGE 모듈은 최신/구버전 둘 다 대응
try:
    from strategies_range import (
        decide_signal_range,
        should_block_range_today,
        compute_range_params,
        should_block_range_today_with_level,
    )
except ImportError:
    from strategies_range import (
        decide_signal_range,
        should_block_range_today,
    )

    compute_range_params = None  # type: ignore
    should_block_range_today_with_level = None  # type: ignore


def get_trading_signal(
    *,
    settings: Any,
    last_trend_close_ts: float,
    last_range_close_ts: float,
) -> Optional[Tuple[str, str, int, List[Any], List[Any], float, Dict[str, Any]]]:
    """시그널을 하나 결정해서 관련 캔들/부가정보와 함께 넘긴다."""
    symbol = settings.symbol

    # 1) 3m 캔들 (거래량 포함) 가져오기
    log(f"[SIGNAL] fetch 3m candles for {symbol} interval={settings.interval} limit=120")
    try:
        candles_3m_raw = get_klines_with_volume(symbol, settings.interval, 120)
    except Exception as e:
        log(f"[KLINE ERROR] 3m fetch failed: {e}")
        return None

    if not candles_3m_raw or len(candles_3m_raw) < 50:
        log("[SIGNAL] 3m candles not enough (<50) → skip signal")
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

    chosen_signal: Optional[str] = None
    signal_source: Optional[str] = None
    extra: Dict[str, Any] = {}
    candles_15m: Optional[List[Any]] = None

    # ─────────────────────────────
    # 3) 추세 먼저 본다
    # ─────────────────────────────
    if settings.enable_trend:
        # 15m 가져오기
        log(f"[SIGNAL] fetch 15m candles for {symbol} limit=120 (trend)")
        candles_15m = get_klines(symbol, "15m", 120)
        trend_15m_val = decide_trend_15m(candles_15m) if candles_15m else None
        if trend_15m_val:
            extra["trend_15m"] = trend_15m_val

        # 3m / 15m 방향
        sig_3m = decide_signal_3m_trend(
            candles_3m,
            settings.rsi_overbought,
            settings.rsi_oversold,
        )

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

        # 완화 로직
        soft_trend_dir: Optional[str] = None
        if (not sig_3m) and trend_15m_val:
            soft_trend_dir = trend_15m_val
            log("[TREND_SOFT] 3m 없음 → 15m 방향만으로 추세 진입 시도")

        # 추세 진입 조건
        if (sig_3m and trend_15m_val and sig_3m == trend_15m_val) or soft_trend_dir:
            # 쿨다운 체크
            if (time.time() - last_trend_close_ts) >= settings.cooldown_after_close_trend:
                final_dir = sig_3m if (sig_3m and trend_15m_val and sig_3m == trend_15m_val) else soft_trend_dir
                chosen_signal = final_dir
                signal_source = "TREND"

                # 1분봉 확인 옵션
                if settings.enable_1m_confirm:
                    log(f"[SIGNAL] fetch 1m candles for {symbol} (trend confirm) limit=20")
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
    else:
        trend_15m_val = None

    # ─────────────────────────────
    # 4) 추세가 안 됐을 때 RANGE 시도
    # ─────────────────────────────
    if (not chosen_signal) and settings.enable_range:
        if not candles_15m:
            log(f"[SIGNAL] fetch 15m candles for {symbol} limit=120 (range fallback)")
            candles_15m = get_klines(symbol, "15m", 120)

        if (time.time() - last_range_close_ts) >= settings.cooldown_after_close_range:
            if should_block_range_today_with_level is not None:
                blocked_now, block_reason = should_block_range_today_with_level(
                    candles_3m,
                    candles_15m,
                    settings,
                )
            else:
                blocked_now = should_block_range_today(candles_3m, candles_15m)
                block_reason = ""

            if not blocked_now:
                r_sig = decide_signal_range(candles_3m)
                if r_sig:
                    ok_range = True
                    if settings.enable_1m_confirm:
                        log(f"[SIGNAL] fetch 1m candles for {symbol} (range confirm) limit=20")
                        candles_1m = get_klines(symbol, "1m", 20)
                        if not confirm_1m_direction(candles_1m, r_sig):
                            ok_range = False
                    if ok_range:
                        chosen_signal = r_sig
                        signal_source = "RANGE"
                        if compute_range_params is not None:
                            params = compute_range_params(r_sig, candles_3m, settings)
                            extra["tp_pct"] = params["tp_pct"]
                            extra["sl_pct"] = params["sl_pct"]
                        if trend_15m_val:
                            extra["trend_15m"] = trend_15m_val
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
            elif block_reason.startswith("soft_"):
                log("[RANGE_SOFT] blocked but allowed with softened TP/SL")
                r_sig = decide_signal_range(candles_3m)
                if r_sig:
                    ok_range = True
                    if settings.enable_1m_confirm:
                        log(f"[SIGNAL] fetch 1m candles for {symbol} (range soft confirm) limit=20")
                        candles_1m = get_klines(symbol, "1m", 20)
                        if not confirm_1m_direction(candles_1m, r_sig):
                            ok_range = False
                    if ok_range:
                        chosen_signal = r_sig
                        signal_source = "RANGE"
                        base_tp = settings.range_tp_pct
                        base_sl = settings.range_sl_pct
                        if compute_range_params is not None:
                            params = compute_range_params(r_sig, candles_3m, settings)
                            base_tp = params["tp_pct"]
                            base_sl = params["sl_pct"]
                        extra["tp_pct"] = base_tp * 0.7
                        extra["sl_pct"] = base_sl * 0.7
                        if trend_15m_val:
                            extra["trend_15m"] = trend_15m_val

                        log_signal(
                            event="ENTRY_SIGNAL",
                            symbol=symbol,
                            strategy_type="RANGE",
                            direction=r_sig,
                            reason=f"range_soft_{block_reason}",
                            candle_ts=latest_3m_ts,
                        )
                    else:
                        send_skip_tg("[SKIP] 1m_confirm_mismatch_range_soft")
                        log_signal(
                            event="SKIP",
                            symbol=symbol,
                            strategy_type="RANGE",
                            direction=r_sig,
                            reason="1m_confirm_mismatch_range_soft",
                            candle_ts=latest_3m_ts,
                        )
                else:
                    send_skip_tg("[SKIP] range_soft_but_no_box")
                    log_signal(
                        event="SKIP",
                        symbol=symbol,
                        strategy_type="RANGE",
                        reason="range_soft_but_no_box",
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

    # 6) ATR 기반 리스크 정보
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
