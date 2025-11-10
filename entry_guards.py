"""
entry_guards.py
====================================================
진입 직전에 하는 각종 가드(guard) 모듈.
run_bot.py → 이 모듈의 함수를 순서대로 호출해서
"이 캔들은 건너뛰자"를 결정한다.

담당하는 가드:
1) 수동 포지션/usedMargin 가드
2) 거래량 가드 (3m 마지막 캔들이 직전 평균의 일정 비율 미만이면 스킵)
3) 가격 점프 가드 (직전 3m 대비 급등/급락 시 스킵)
4) 스프레드/호가 가드 (orderbook 으로 bid/ask 격차 확인)

2025-11-12 보완
----------------------------------------------------
선물 변동성 때문에 "닫힐 때는 멀리, 들어갈 때는 너무 앞"인 캔들을 걸러내기 위해
기존 가격 점프 가드에 '현재 캔들 자체 변동성' 체크를 추가했다.

- 기존에는 직전 캔들의 종가(prev_close) 대비 이번 종가(last_close)의 변동만 봤다.
- 그런데 선물에서는 같은 캔들 안에서 high/low가 크게 나왔다가 종가는 제자리인
  경우가 있어서 이럴 때 바로 진입하면 다음 틱에서 SL이 맞는 일이 많다.
- 그래서 아래를 추가했다:

  1) 이번 캔들의 high-low 범위를 종가로 나눈 값(range_pct)을 구한다.
  2) 이 값이 max_price_jump_pct의 1.8배를 넘으면 이 캔들은 스킵한다.
     → 예) max_price_jump_pct=0.003 (=0.3%) 라면
            캔들 하나가 0.54% 이상 흔들렸으면 위험 캔들로 본다.

이렇게 하면 “같은 3분봉 안에서 위아래를 다 찍고 내려온 캔들”을 안 들어가게 돼서
‘들어가자마자 손절’ 빈도를 줄일 수 있다.

기존 run_bot 의 호출 순서를 깨지 않기 위해 함수 시그니처와 이름은 그대로 두고,
내부에 캔들 변동성 체크만 추가했다.
"""

from __future__ import annotations

from typing import Any, Callable, List, Optional, Tuple

from telelog import send_skip_tg, log
from signals_logger import log_signal
from market_data import get_orderbook


# ─────────────────────────────
# 1. 수동 포지션 / usedMargin 가드
# ─────────────────────────────
def check_manual_position_guard(
    *,
    get_balance_detail_func: Callable[[], Any],
    symbol: str,
    latest_ts: int,
) -> bool:
    """포지션 API가 안 되는 계정에서 사람이 잡은 포지션이 있으면 신규 진입을 막는다."""
    try:
        bal_detail = get_balance_detail_func()
        used_margin = float(bal_detail.get("usedMargin") or 0.0)
    except Exception as e:
        log(f"[MANUAL_GUARD] balance detail error: {e}")
        log_signal(
            event="SKIP",
            symbol=symbol,
            strategy_type="UNKNOWN",
            reason="manual_guard_balance_error",
            extra=str(e),
            candle_ts=latest_ts,
        )
        # 에러 났으면 이 캔들은 그냥 건너뛰는 게 안전하다
        return False

    if used_margin > 0:
        send_skip_tg("[SKIP] manual_position_detected: usedMargin>0 → 신규 진입 건너뜀")
        log_signal(
            event="SKIP",
            symbol=symbol,
            strategy_type="UNKNOWN",
            reason="manual_position_detected",
            candle_ts=latest_ts,
            extra=f"usedMargin={used_margin}",
        )
        return False

    return True


# ─────────────────────────────
# 2. 거래량 가드
# ─────────────────────────────
def check_volume_guard(
    *,
    settings: Any,
    candles_3m_raw: List[Any],
    latest_ts: int,
    signal_source: str,
    direction: str,
) -> bool:
    """3m 마지막 캔들의 거래량이 최근 평균의 min_entry_volume_ratio 미만이면 스킵"""
    min_vol_ratio = getattr(settings, "min_entry_volume_ratio", 0.3)
    try:
        last_vol = float(candles_3m_raw[-1][5])
        vols_20 = [float(c[5]) for c in candles_3m_raw[-20:]]
        avg_vol_20 = sum(vols_20) / len(vols_20)
    except Exception:
        # 거래소가 거래량 안 주는 경우도 있으니 통과시킨다
        return True

    if avg_vol_20 > 0 and last_vol < avg_vol_20 * min_vol_ratio:
        send_skip_tg("[SKIP] volume_too_low_for_entry")
        log_signal(
            event="SKIP",
            symbol=settings.symbol,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="volume_too_low_for_entry",
            candle_ts=latest_ts,
            extra=f"last_vol={last_vol}, avg_vol_20={avg_vol_20}, ratio={min_vol_ratio}",
        )
        return False
    return True


# ─────────────────────────────
# 3. 가격 점프 + 캔들 변동성 가드
# ─────────────────────────────
def check_price_jump_guard(
    *,
    settings: Any,
    candles_3m: List[Any],
    latest_ts: int,
    signal_source: str,
    direction: str,
) -> bool:
    """직전 3m 대비 가격이 너무 튀거나, 현재 캔들 자체가 너무 흔들렸으면 그 캔들만 스킵"""
    if len(candles_3m) < 2:
        return True

    last_price = candles_3m[-1][4]
    prev_price = candles_3m[-2][4]
    last_high = candles_3m[-1][2]
    last_low = candles_3m[-1][3]

    if prev_price <= 0 or last_price <= 0:
        return True

    # 3-1) 직전 종가 대비 점프
    move_pct = abs(last_price - prev_price) / prev_price
    if move_pct > settings.max_price_jump_pct:
        send_skip_tg(
            f"[SKIP] price_jump_guard: {move_pct:.4f} > {settings.max_price_jump_pct:.4f}"
        )
        log_signal(
            event="SKIP",
            symbol=settings.symbol,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="price_jump_guard",
            candle_ts=latest_ts,
            extra=f"move_pct={move_pct:.6f}",
        )
        return False

    # 3-2) 같은 캔들 안에서의 변동성 체크 (선물에서 흔들다 내려온 캔들 방지)
    # high-low 를 종가로 나눠서 퍼센트로 본다.
    if last_low > 0:
        range_pct = (last_high - last_low) / last_price
    else:
        range_pct = 0.0

    # 기준: 직전 점프 허용치의 1.8배 이상이면 위험 캔들로 본다.
    # 예) max_price_jump_pct=0.003 → range_pct>0.0054 이면 스킵
    candle_vol_limit = settings.max_price_jump_pct * 1.8
    if range_pct > candle_vol_limit:
        send_skip_tg(
            f"[SKIP] candle_volatility_guard: {range_pct:.4f} > {candle_vol_limit:.4f}"
        )
        log_signal(
            event="SKIP",
            symbol=settings.symbol,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="candle_volatility_guard",
            candle_ts=latest_ts,
            extra=f"range_pct={range_pct:.6f}",
        )
        return False

    return True


# ─────────────────────────────
# 4. 스프레드/호가 가드
# ─────────────────────────────
def check_spread_guard(
    *,
    settings: Any,
    symbol: str,
    latest_ts: int,
    signal_source: str,
    direction: str,
) -> Tuple[bool, Optional[float], Optional[float]]:
    """호가 스프레드가 너무 벌어져 있으면 스킵. best_bid/best_ask 도 같이 반환한다."""
    orderbook = get_orderbook(symbol, 5)
    if not orderbook:
        return True, None, None

    bids = orderbook.get("bids") or []
    asks = orderbook.get("asks") or []
    if not bids or not asks:
        return True, None, None

    try:
        best_bid = float(bids[0][0] if isinstance(bids[0], list) else bids[0].get("price"))
        best_ask = float(asks[0][0] if isinstance(asks[0], list) else asks[0].get("price"))
    except Exception as e:
        log(f"[SPREAD PARSE ERROR] {e}")
        return True, None, None

    if best_bid <= 0:
        return True, best_bid, best_ask

    spread_pct = (best_ask - best_bid) / best_bid
    if spread_pct > settings.max_spread_pct:
        send_skip_tg(
            f"[SKIP] spread_guard: {spread_pct:.5f} > {settings.max_spread_pct:.5f}"
        )
        log_signal(
            event="SKIP",
            symbol=symbol,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="spread_guard",
            candle_ts=latest_ts,
            spread_pct=spread_pct,
        )
        return False, best_bid, best_ask

    return True, best_bid, best_ask
