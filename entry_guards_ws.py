from __future__ import annotations

import datetime
import time
from typing import Any, Callable, List, Optional, Tuple

from telelog import send_skip_tg, log
from signals_logger import log_signal
from market_data_ws import get_orderbook  # WS 버퍼에서 호가 읽기


# ─────────────────────────────
# 세션(시간대) 유틸
# ─────────────────────────────
def _get_session_multipliers(settings: Any) -> Tuple[float, float]:
    """
    현재 UTC 시각을 기반으로 어느 세션인지 대략 나누고,
    스프레드/점프 허용치에 곱할 배수를 돌려준다.
    """
    now_utc = datetime.datetime.utcnow()
    h = now_utc.hour

    if 0 <= h < 7:  # 아시아
        spread_mult = float(getattr(settings, "session_spread_mult_asia", 1.0))
        jump_mult = float(getattr(settings, "session_jump_mult_asia", 1.0))
    elif 7 <= h < 13:  # 유럽
        spread_mult = float(getattr(settings, "session_spread_mult_eu", 1.1))
        jump_mult = float(getattr(settings, "session_jump_mult_eu", 1.1))
    else:  # 미국
        spread_mult = float(getattr(settings, "session_spread_mult_us", 1.2))
        jump_mult = float(getattr(settings, "session_jump_mult_us", 1.2))

    return spread_mult, jump_mult


# ─────────────────────────────
# 1. 수동 포지션 / usedMargin 가드
# ─────────────────────────────
def check_manual_position_guard(
    *,
    get_balance_detail_func: Callable[[], Any],
    symbol: str,
    latest_ts: int,
) -> bool:
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
# 2. 거래량 가드 (5m 기준)
# ─────────────────────────────
def check_volume_guard(
    *,
    settings: Any,
    candles_5m_raw: List[Any],
    latest_ts: int,
    signal_source: str,
    direction: str,
) -> bool:
    sym = getattr(settings, "symbol", "UNKNOWN")
    log(f"[GUARD] (WS) volume len={len(candles_5m_raw)} symbol={sym}")
    min_vol_ratio = float(getattr(settings, "min_entry_volume_ratio", 0.3))
    try:
        last_vol = float(candles_5m_raw[-1][5])
        vols_20 = [float(c[5]) for c in candles_5m_raw[-20:]]
        avg_vol_20 = sum(vols_20) / len(vols_20) if vols_20 else 0.0
    except Exception:
        return True

    if avg_vol_20 <= 0:
        return True

    ratio = last_vol / avg_vol_20

    if ratio < min_vol_ratio:
        send_skip_tg("[SKIP] volume_too_low_for_entry")
        log_signal(
            event="SKIP",
            symbol=sym,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="volume_too_low_for_entry",
            candle_ts=latest_ts,
            extra=(
                f"last_vol={last_vol}, avg_vol_20={avg_vol_20}, "
                f"ratio={ratio:.4f}, threshold={min_vol_ratio:.4f}"
            ),
        )
        return False
    return True


# ─────────────────────────────
# 3. 가격 점프 + 캔들 변동성 가드 (5m 기준, 세션 배수 적용)
# ─────────────────────────────
def check_price_jump_guard(
    *,
    settings: Any,
    candles_5m: List[Any],
    latest_ts: int,
    signal_source: str,
    direction: str,
) -> bool:
    sym = getattr(settings, "symbol", "UNKNOWN")
    log(f"[GUARD] (WS) price_jump len={len(candles_5m)} symbol={sym}")
    if len(candles_5m) < 2:
        return True

    # 세션별 jump 배수
    _, jump_mult = _get_session_multipliers(settings)
    base_jump = float(getattr(settings, "max_price_jump_pct", 0.003))
    max_jump_pct = base_jump * jump_mult

    last_price = candles_5m[-1][4]
    prev_price = candles_5m[-2][4]
    last_high = candles_5m[-1][2]
    last_low = candles_5m[-1][3]

    if prev_price <= 0 or last_price <= 0:
        return True

    move_pct = abs(last_price - prev_price) / prev_price
    if move_pct > max_jump_pct:
        send_skip_tg(
            f"[SKIP] price_jump_guard: {move_pct:.4f} > {max_jump_pct:.4f} (sess_mult={jump_mult})"
        )
        log_signal(
            event="SKIP",
            symbol=sym,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="price_jump_guard",
            candle_ts=latest_ts,
            extra=f"move_pct={move_pct:.6f}, sess_mult={jump_mult}",
        )
        return False

    if last_low > 0:
        range_pct = (last_high - last_low) / last_price
    else:
        range_pct = 0.0

    candle_vol_limit = max_jump_pct * 1.8
    if range_pct > candle_vol_limit:
        send_skip_tg(
            f"[SKIP] candle_volatility_guard: {range_pct:.4f} > {candle_vol_limit:.4f}"
        )
        log_signal(
            event="SKIP",
            symbol=sym,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="candle_volatility_guard",
            candle_ts=latest_ts,
            extra=f"range_pct={range_pct:.6f}, sess_mult={jump_mult}",
        )
        return False

    return True


# ─────────────────────────────
# (보조) depth 한쪽 쏠림 체크
# ─────────────────────────────
def _is_depth_imbalanced(
    settings: Any,
    orderbook: dict,
) -> bool:
    enabled = bool(getattr(settings, "depth_imbalance_enabled", True))
    if not enabled:
        return False

    min_notional = float(getattr(settings, "depth_imbalance_min_notional", 10.0))
    min_ratio = float(getattr(settings, "depth_imbalance_min_ratio", 2.0))

    bids = orderbook.get("bids") or []
    asks = orderbook.get("asks") or []
    if not bids or not asks:
        return False

    def _side_notional(rows):
        total = 0.0
        for r in rows[:5]:
            if isinstance(r, list):
                price = float(r[0])
                qty = float(r[1])
            else:
                price = float(r.get("price"))
                qty = float(r.get("qty") or r.get("quantity") or r.get("size") or 0.0)
            total += price * qty
        return total

    bid_notional = _side_notional(bids)
    ask_notional = _side_notional(asks)

    if bid_notional < min_notional and ask_notional < min_notional:
        return False

    bigger = max(bid_notional, ask_notional)
    smaller = min(bid_notional, ask_notional)
    if smaller == 0:
        return True

    ratio = bigger / smaller
    return ratio >= min_ratio


# ─────────────────────────────
# (보조) mark/last 괴리 체크
# ─────────────────────────────
def _is_price_deviation_large(
    settings: Any,
    orderbook: dict,
    best_bid: float,
    best_ask: float,
) -> bool:
    enabled = bool(getattr(settings, "price_deviation_guard_enabled", True))
    if not enabled:
        return False

    max_pct = float(getattr(settings, "price_deviation_max_pct", 0.0015))
    mark_price = orderbook.get("markPrice")
    last_price = orderbook.get("lastPrice")
    if mark_price is None and last_price is None:
        return False

    mid = None
    if best_bid and best_ask:
        mid = (best_bid + best_ask) / 2.0

    if mark_price is not None and mid:
        try:
            mark_price_f = float(mark_price)
            dev = abs(mark_price_f - mid) / mid
            if dev > max_pct:
                return True
        except Exception:
            pass

    if last_price is not None and mid:
        try:
            last_price_f = float(last_price)
            dev = abs(last_price_f - mid) / mid
            if dev > max_pct:
                return True
        except Exception:
            pass

    return False


# ─────────────────────────────
# 4. 스프레드/호가 가드 (WS depth) + depth 쏠림 + mark/last 괴리
# ─────────────────────────────
def check_spread_guard(
    *,
    settings: Any,
    symbol: str,
    latest_ts: int,
    signal_source: str,
    direction: str,
) -> Tuple[bool, Optional[float], Optional[float]]:
    orderbook = get_orderbook(symbol, 5)
    log(f"[GUARD] (WS) spread symbol={symbol} ob_ok={bool(orderbook)}")
    if not orderbook:
        return True, None, None

    # D) 오더북 신선도(지연) 가드
    now_ms = int(time.time() * 1000)
    ob_ts = int(orderbook.get("ts") or 0)
    ob_age_ms = now_ms - ob_ts if ob_ts > 0 else -1
    max_ob_age = int(getattr(settings, "max_orderbook_age_ms", 3000))
    if ob_age_ms >= 0 and ob_age_ms > max_ob_age:
        send_skip_tg("[SKIP] orderbook_stale")
        log_signal(
            event="SKIP",
            symbol=symbol,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="orderbook_stale",
            candle_ts=latest_ts,
            extra=f"ob_age_ms={ob_age_ms}, max={max_ob_age}",
        )
        return False, None, None

    bids = orderbook.get("bids") or []
    asks = orderbook.get("asks") or []
    if not bids or not asks:
        return True, None, None

    try:
        best_bid = float(bids[0][0] if isinstance(bids[0], list) else bids[0].get("price"))
        best_ask = float(asks[0][0] if isinstance(asks[0], list) else asks[0].get("price"))
        bid_qty = float(
            bids[0][1]
            if isinstance(bids[0], list)
            else bids[0].get("qty") or bids[0].get("quantity") or bids[0].get("size") or 0.0
        )
        ask_qty = float(
            asks[0][1]
            if isinstance(asks[0], list)
            else asks[0].get("qty") or asks[0].get("quantity") or asks[0].get("size") or 0.0
        )
    except Exception as e:
        log(f"[SPREAD PARSE ERROR] {e}")
        return True, None, None

    # E) 비정상 BBO 가드
    if not best_bid or not best_ask or best_bid >= best_ask:
        send_skip_tg("[SKIP] bbo_crossed_or_invalid")
        log_signal(
            event="SKIP",
            symbol=symbol,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="bbo_crossed_or_invalid",
            candle_ts=latest_ts,
            extra=f"best_bid={best_bid}, best_ask={best_ask}",
        )
        return False, best_bid, best_ask

    # F) 최상위 호가 명목가 최소치 가드 (옵션)
    min_bbo_notional = float(getattr(settings, "min_bbo_notional_usdt", 0.0))
    top_bid_notional = best_bid * bid_qty
    top_ask_notional = best_ask * ask_qty

    if min_bbo_notional > 0:
        if top_bid_notional < min_bbo_notional or top_ask_notional < min_bbo_notional:
            send_skip_tg("[SKIP] bbo_notional_too_small")
            log_signal(
                event="SKIP",
                symbol=symbol,
                strategy_type=signal_source or "UNKNOWN",
                direction=direction,
                reason="bbo_notional_too_small",
                candle_ts=latest_ts,
                extra=(
                    f"bid_notional={top_bid_notional:.2f}, "
                    f"ask_notional={top_ask_notional:.2f}, min={min_bbo_notional}"
                ),
            )
            return False, best_bid, best_ask

    spread_mult, _ = _get_session_multipliers(settings)
    base_spread = float(getattr(settings, "max_spread_pct", 0.0008))
    max_spread_pct = base_spread * spread_mult

    spread_abs = best_ask - best_bid
    spread_pct = spread_abs / best_bid if best_bid > 0 else 0.0

    # G) 스프레드 임계 (퍼센트/절대 동시 지원)
    max_spread_abs = float(getattr(settings, "max_spread_abs", 0.0))
    too_wide_by_pct = spread_pct > max_spread_pct
    too_wide_by_abs = max_spread_abs > 0 and spread_abs > max_spread_abs
    if too_wide_by_pct or too_wide_by_abs:
        send_skip_tg(
            "[SKIP] spread_guard: "
            f"pct={spread_pct:.5f} (limit={max_spread_pct:.5f}), "
            f"abs={spread_abs:.2f}"
            f"{' (limit='+str(max_spread_abs)+')' if max_spread_abs > 0 else ''}"
        )
        log_signal(
            event="SKIP",
            symbol=symbol,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="spread_guard",
            candle_ts=latest_ts,
            spread_pct=spread_pct,
            extra=(
                f"ob_age_ms={ob_age_ms}, spread_abs={spread_abs:.2f}, "
                f"sess_mult={spread_mult}, "
                f"top_bid_notional={top_bid_notional:.2f}, "
                f"top_ask_notional={top_ask_notional:.2f}"
            ),
        )
        return False, best_bid, best_ask

    # A) 한쪽 쏠림
    if _is_depth_imbalanced(settings, orderbook):
        send_skip_tg("[SKIP] depth_imbalance_guard: one side dominates 5-level depth")
        log_signal(
            event="SKIP",
            symbol=symbol,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="depth_imbalance_guard",
            candle_ts=latest_ts,
            extra=f"ob_age_ms={ob_age_ms}",
        )
        return False, best_bid, best_ask

    # B) mark/last 괴리
    if _is_price_deviation_large(settings, orderbook, best_bid, best_ask):
        send_skip_tg("[SKIP] price_deviation_guard: mark/last deviated from mid")
        log_signal(
            event="SKIP",
            symbol=symbol,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="price_deviation_guard",
            candle_ts=latest_ts,
            extra=f"ob_age_ms={ob_age_ms}",
        )
        return False, best_bid, best_ask

    return True, best_bid, best_ask


__all__ = [
    "check_manual_position_guard",
    "check_volume_guard",
    "check_price_jump_guard",
    "check_spread_guard",
]
