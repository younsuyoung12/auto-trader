"""
signal_flow.py
====================================================
시그널 결정 전담 모듈.
run_bot.py 에서 한 줄로 호출해서 시그널/캔들 세트를 받아가게 한다.

2025-11-12 추가
----------------------------------------------------
(요청 아이디어 반영: 박스 단계화, 방향별 TP/SL 전달)
1) strategies_range.py 에서 만든
     - should_block_range_today_with_level(...)
     - compute_range_params(...)
   를 실제로 여기서 사용하도록 변경했다.
   → 이제 RANGE 신호가 나올 때 extra["tp_pct"], extra["sl_pct"] 에
     방향별로 계산된 값을 실어준다.
   → run_bot.py 는 원래 extra 에 tp/sl 이 있으면 그걸 우선 쓰므로
     박스에서 롱/숏을 다르게 먹는 게 실제 주문에 반영된다.

2) range_strict_level 이 1 이라서 "soft" 로 허용된 경우,
   TP/SL 을 살짝 낮춰서 보내도록 했다.
   → 너무 애매한 날에도 박스가 아예 죽지 않고, 대신 목표를 얕게 간다.
   → soft 인지는 should_block_range_today_with_level(...) 이 주는 reason
     ("soft_atr"/"soft_ema") 로 판별한다.

3) 추세 15m 방향을 extra["trend_15m"] 에 넣어서
   나중에 텔레그램 상태 메시지가 이 값을 써도 되게 했다.

※ run_bot.py 의 시그니처는 그대로 둔다.
※ strategies_range.py 가 옛 버전이라 새 함수가 없으면
   기존 동작(should_block_range_today, decide_signal_range)으로만 돈다.

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
    # 구버전 호환: 새 함수가 없으면 None 으로 두고 아래에서 분기
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
        trend_15m_val = decide_trend_15m(candles_15m) if candles_15m else None
        # 15m 방향은 extra 에 실어줘서 나중에 텔레그램에서 써먹을 수 있게 한다.
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

        # ───── 완화 로직 ─────
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
        # 추세를 시도했으므로 여기서 끝나면 아래 RANGE 로 넘어간다.
    else:
        trend_15m_val = None  # enable_trend=False 인 경우

    # ─────────────────────────────
    # 4) 추세가 안 됐을 때 RANGE 시도
    #    (이제는 단계화/방향별 TP/SL 이 실제로 extra 에 들어간다)
    # ─────────────────────────────
    if (not chosen_signal) and settings.enable_range:
        if not candles_15m:
            candles_15m = get_klines(symbol, "15m", 120)

        if (time.time() - last_range_close_ts) >= settings.cooldown_after_close_range:
            # 새 버전이 있으면 그걸 쓰고, 없으면 기존 함수 그대로
            if should_block_range_today_with_level is not None:
                blocked_now, block_reason = should_block_range_today_with_level(
                    candles_3m,
                    candles_15m,
                    settings,
                )
            else:
                blocked_now = should_block_range_today(candles_3m, candles_15m)
                block_reason = ""  # 구버전에서는 reason 없음

            # 1) 완전히 허용되는 날
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

                        # 방향별 TP/SL 계산해서 넣어준다
                        if compute_range_params is not None:
                            params = compute_range_params(r_sig, candles_3m, settings)
                            extra["tp_pct"] = params["tp_pct"]
                            extra["sl_pct"] = params["sl_pct"]
                        # 15m 방향도 같이 실어준다
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

            # 2) soft 로 허용된 날 (block_reason 이 soft_ 로 시작)
            elif block_reason.startswith("soft_"):
                log("[RANGE_SOFT] blocked but allowed with softened TP/SL")
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

                        # 기본 방향별 TP/SL
                        base_tp = settings.range_tp_pct
                        base_sl = settings.range_sl_pct
                        if compute_range_params is not None:
                            params = compute_range_params(r_sig, candles_3m, settings)
                            base_tp = params["tp_pct"]
                            base_sl = params["sl_pct"]

                        # soft 인 날은 살짝 낮춰서 보낸다 (예: 0.7배)
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
                    # soft 였지만 아예 박스가 안 보이면 그냥 스킵
                    send_skip_tg("[SKIP] range_soft_but_no_box")
                    log_signal(
                        event="SKIP",
                        symbol=symbol,
                        strategy_type="RANGE",
                        reason="range_soft_but_no_box",
                        candle_ts=latest_3m_ts,
                    )

            # 3) 진짜로 막힌 날
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
