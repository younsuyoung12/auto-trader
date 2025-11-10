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

각 가드는 True/False 로만 답하고, 내부에서 signals_logger 에 SKIP 을 남긴다.
이렇게 하면 run_bot.py 는 흐름만 보고 각각의 이유를 몰라도 된다.
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
# 3. 가격 점프 가드
# ─────────────────────────────
def check_price_jump_guard(
    *,
    settings: Any,
    candles_3m: List[Any],
    latest_ts: int,
    signal_source: str,
    direction: str,
) -> bool:
    """직전 3m 대비 가격이 너무 튀면 그 캔들만 스킵"""
    if len(candles_3m) < 2:
        return True
    last_price = candles_3m[-1][4]
    prev_price = candles_3m[-2][4]
    if prev_price <= 0:
        return True
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
