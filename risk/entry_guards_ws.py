# entry_guards_ws.py
"""
entry_guards_ws.py (Binance USDT-M Futures 전용 하드 리스크 가드 모듈)
====================================================================

역할
-----------------------------------------------------
Binance USDT-M Futures WebSocket 버퍼 데이터를 기반으로
진입 직전 시장 상태가 정상인지 판단하는 하드 리스크 가드 레이어.

이 모듈은 전략과 무관하며,
아래 조건 중 하나라도 FAIL 시 무조건 진입을 SKIP 한다.

가드 목록
-----------------------------------------------------
1) 기존 포지션 존재 가드 (used margin 기반)
2) 거래량 이상 가드 (최근 vs 평균)
3) 5m 캔들 지연/누락 가드
4) 5m 가격 점프/과도한 변동성 가드
5) 스프레드/오더북 이상 가드
6) depth 한쪽 쏠림 가드
7) markPrice / lastPrice 괴리 가드

STRICT 정책
-----------------------------------------------------
- 데이터 부족/파싱 실패/비정상 값은 모두 차단 처리.
- 보정/추정/폴백 금지.
- 이 파일은 "가드 판단"만 수행하며 주문 실행은 포함하지 않는다.

주의: manual_position_guard는 Binance /fapi/v2/balance 응답 구조에 100% 정합해야 한다.
- 사용 필드: availableBalance, crossWalletBalance, crossUnPnl
- unrealizedProfit 사용 금지
"""

from __future__ import annotations

import datetime
import logging
import math
from typing import Any, Callable, Dict, List, Optional, Tuple

from infra.telelog import send_skip_tg
from events.signals_logger import log_signal
from infra.market_data_ws import get_orderbook  # Binance WS 버퍼에서 depth 조회 (폴백 없음)

logger = logging.getLogger(__name__)

LOG_PREFIX = "[ENTRY_GUARD_BINANCE]"


def _now_ms_utc() -> int:
    """현재 UTC 시각(ms)."""
    return int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)


def _log(msg: str, *, level: int = logging.INFO) -> None:
    """모듈 로그 prefix 통일."""
    logger.log(level, "%s %s", LOG_PREFIX, msg)


def _send_skip(message: str) -> None:
    """SKIP 텔레그램 메시지 전송 (prefix 포함)."""
    try:
        send_skip_tg(f"{LOG_PREFIX} {message}")
    except Exception as e:  # pragma: no cover
        _log(f"send_skip_tg_failed error={e}", level=logging.ERROR)


# ─────────────────────────────
# 세션(시간대) 유틸
# ─────────────────────────────


def _get_session_multipliers(settings: Any) -> Tuple[float, float]:
    """UTC 기준 세션을 구분해 스프레드/점프 허용치 배수를 반환한다.

    - 아시아: 00~07시
    - 유럽: 07~13시
    - 미국: 13~24시
    """
    now_utc = datetime.datetime.now(datetime.timezone.utc)
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
# 1. 기존 포지션 / used margin 가드 (Binance /fapi/v2/balance 기반)
# ─────────────────────────────


def check_manual_position_guard(
    *,
    get_balance_detail_func: Callable[[], Any],
    symbol: str,
    latest_ts: int | float,
) -> bool:
    """Binance /fapi/v2/balance 응답 구조에 기반해 used_margin으로 신규 진입 차단.

    사용 필드 (반드시 존재해야 함):
    - availableBalance
    - crossWalletBalance
    - crossUnPnl

    계산식:
    - equity     = crossWalletBalance + crossUnPnl  (참고용)
    - used_margin = max(crossWalletBalance - availableBalance, 0.0)

    STRICT:
    - bal_detail가 dict 아니면 RuntimeError
    - 필수 키 누락 시 RuntimeError
    - float 변환 실패/NaN/inf 시 RuntimeError
    - used_margin > 0 → 신규 진입 차단(False)
    """

    ts_int = int(latest_ts)

    bal_detail = get_balance_detail_func()
    if not isinstance(bal_detail, dict):
        raise RuntimeError(
            f"balance detail must be dict, got {type(bal_detail).__name__}"
        )

    required = ("availableBalance", "crossWalletBalance", "crossUnPnl")
    missing = [k for k in required if k not in bal_detail]
    if missing:
        raise RuntimeError(f"balance detail missing keys: {missing}")

    try:
        available_balance = float(bal_detail["availableBalance"])
        cross_wallet_balance = float(bal_detail["crossWalletBalance"])
        cross_un_pnl = float(bal_detail["crossUnPnl"])
    except Exception as e:
        raise RuntimeError(f"balance detail float parse failed: {e}") from e

    for name, val in (
        ("availableBalance", available_balance),
        ("crossWalletBalance", cross_wallet_balance),
        ("crossUnPnl", cross_un_pnl),
    ):
        if not math.isfinite(val):
            raise RuntimeError(f"balance detail not finite: {name}={val}")

    equity = cross_wallet_balance + cross_un_pnl
    used_margin = max(cross_wallet_balance - available_balance, 0.0)

    if used_margin > 0.0:
        _log(
            "manual_position_detected "
            f"symbol={symbol} "
            f"availableBalance={available_balance:.8f} "
            f"crossWalletBalance={cross_wallet_balance:.8f} "
            f"crossUnPnl={cross_un_pnl:.8f} "
            f"equity={equity:.8f} "
            f"used_margin={used_margin:.8f}",
            level=logging.WARNING,
        )
        _send_skip("[SKIP] manual_position_detected: used_margin>0 → 신규 진입 차단")
        log_signal(
            event="on_risk_guard_trigger",
            symbol=symbol,
            strategy_type="UNKNOWN",
            reason="manual_position_detected",
            candle_ts=ts_int,
            extra=(
                f"availableBalance={available_balance}, "
                f"crossWalletBalance={cross_wallet_balance}, "
                f"crossUnPnl={cross_un_pnl}, "
                f"equity={equity}, used_margin={used_margin}"
            ),
        )
        return False

    return True


# ─────────────────────────────
# 2. 거래량 가드 (WS kline raw 기준)
# ─────────────────────────────


def check_volume_guard(
    *,
    settings: Any,
    candles_5m_raw: List[Any],
    latest_ts: int,
    signal_source: str,
    direction: str,
) -> bool:
    """최근 거래량이 직전 20개 평균 대비 지나치게 작으면 진입 차단.

    WS kline 포맷 가정:
    - (ts, open, high, low, close, volume)

    STRICT:
    - 샘플 부족 / 파싱 실패 / 기준 평균<=0 → 모두 차단.
    """
    sym = getattr(settings, "symbol", "UNKNOWN")
    _log(f"volume_guard_check symbol={sym} len={len(candles_5m_raw)}")

    min_vol_ratio = float(getattr(settings, "min_entry_volume_ratio", 0.3))

    if len(candles_5m_raw) < 20:
        _log(f"volume_guard_insufficient_samples symbol={sym} len={len(candles_5m_raw)}", level=logging.WARNING)
        _send_skip("[SKIP] volume_guard_insufficient_samples: len<20")
        log_signal(
            event="SKIP",
            symbol=sym,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="volume_guard_insufficient_samples",
            candle_ts=latest_ts,
            extra=f"len={len(candles_5m_raw)}",
        )
        return False

    try:
        last_vol = float(candles_5m_raw[-1][5])
        vols_20 = [float(c[5]) for c in candles_5m_raw[-20:]]
        avg_vol_20 = sum(vols_20) / len(vols_20)
    except Exception as e:
        _log(f"volume_guard_parse_error symbol={sym} error={e}", level=logging.WARNING)
        _send_skip("[SKIP] volume_guard_parse_error: volume parsing failed")
        log_signal(
            event="SKIP",
            symbol=sym,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="volume_guard_parse_error",
            candle_ts=latest_ts,
            extra=str(e),
        )
        return False

    if avg_vol_20 <= 0:
        _log(
            f"volume_guard_no_reference symbol={sym} last_vol={last_vol} avg_vol_20={avg_vol_20}",
            level=logging.WARNING,
        )
        _send_skip("[SKIP] volume_guard_no_reference: avg_vol_20<=0")
        log_signal(
            event="SKIP",
            symbol=sym,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="volume_guard_no_reference",
            candle_ts=latest_ts,
            extra=f"last_vol={last_vol}, avg_vol_20={avg_vol_20}",
        )
        return False

    ratio = last_vol / avg_vol_20
    if ratio < min_vol_ratio:
        _log(
            f"volume_too_low_for_entry symbol={sym} ratio={ratio:.6f} threshold={min_vol_ratio:.6f}",
            level=logging.WARNING,
        )
        _send_skip("[SKIP] volume_too_low_for_entry")
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
# 3. 5m 지연 가드 (WS 1m/5m ts 기반)
# ─────────────────────────────


def check_5m_delay_guard(
    *,
    settings: Any,
    candles_1m: List[Any],
    candles_5m: List[Any],
    latest_ts: int,
    signal_source: str,
    direction: str,
) -> bool:
    """1m/5m 마지막 캔들 ts를 비교해 실제 5m 지연/누락을 감지한다.

    조건:
    - 5m 캔들 부재 → 차단
    - 5m ts 파싱 실패 → 차단
    - 1m 대비 5m 누락이 2개 이상 추정 → 차단
    - now - last_5m_ts > max_5m_delay_ms → 차단
    """
    sym = getattr(settings, "symbol", "UNKNOWN")
    _log(
        f"5m_delay_guard_check symbol={sym} len_1m={len(candles_1m)} len_5m={len(candles_5m)}"
    )

    if not candles_5m:
        _log(f"5m_delay_no_5m_candles symbol={sym}", level=logging.WARNING)
        _send_skip("[SKIP] 5m_delay_no_5m_candles")
        log_signal(
            event="SKIP",
            symbol=sym,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="5m_delay_no_5m_candles",
            candle_ts=latest_ts,
            extra=f"len_1m={len(candles_1m)}, len_5m={len(candles_5m)}",
        )
        return False

    try:
        last_5m_ts = int(candles_5m[-1][0])
    except Exception as e:
        _log(f"5m_delay_ts_parse_error symbol={sym} target=last_5m_ts error={e}", level=logging.WARNING)
        _send_skip("[SKIP] 5m_delay_ts_parse_error: last_5m_ts")
        log_signal(
            event="SKIP",
            symbol=sym,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="5m_delay_ts_parse_error",
            candle_ts=latest_ts,
            extra=str(e),
        )
        return False

    last_1m_ts: Optional[int] = None
    if candles_1m:
        try:
            last_1m_ts = int(candles_1m[-1][0])
        except Exception:
            last_1m_ts = None

    now_ms = _now_ms_utc()

    default_max_delay_ms = 2 * 5 * 60 * 1000  # 10분
    max_delay_ms = int(getattr(settings, "max_5m_delay_ms", default_max_delay_ms))

    missing_5m_count = 0
    if last_1m_ts is not None and last_1m_ts > last_5m_ts:
        five_min_ms = 5 * 60 * 1000
        missing_5m_count = max(0, (last_1m_ts - last_5m_ts) // five_min_ms)

    delayed_by_gap = missing_5m_count >= 2
    delayed_by_time = (now_ms - last_5m_ts) > max_delay_ms

    _log(
        "5m_delay_detail "
        f"symbol={sym} last_1m_ts={last_1m_ts} last_5m_ts={last_5m_ts} "
        f"missing_5m_count={missing_5m_count} now_gap_ms={now_ms - last_5m_ts} "
        f"max_delay_ms={max_delay_ms} delayed_by_gap={delayed_by_gap} delayed_by_time={delayed_by_time}"
    )

    if delayed_by_gap or delayed_by_time:
        reason = "5m_kline_delayed"
        extra = (
            f"missing_5m_count={missing_5m_count}, "
            f"now_gap_ms={now_ms - last_5m_ts}, max_delay_ms={max_delay_ms}, "
            f"last_1m_ts={last_1m_ts}, last_5m_ts={last_5m_ts}"
        )
        _log(f"{reason} symbol={sym} {extra}", level=logging.WARNING)
        _send_skip(f"[SKIP] {reason}: {extra}")
        log_signal(
            event="SKIP",
            symbol=sym,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason=reason,
            candle_ts=latest_ts,
            extra=extra,
        )
        return False

    return True


# ─────────────────────────────
# 4. 가격 점프 + 캔들 변동성 가드 (5m 기준, 세션 배수 적용)
# ─────────────────────────────


def check_price_jump_guard(
    *,
    settings: Any,
    candles_5m: List[Any],
    latest_ts: int,
    signal_source: str,
    direction: str,
) -> bool:
    """직전 5m 캔들 기준 급격한 가격 점프/변동성을 제한한다.

    기준:
    - prev_close 대비 last_close 이동률
    - last candle range(high-low) / last_close
    """
    sym = getattr(settings, "symbol", "UNKNOWN")
    _log(f"price_jump_guard_check symbol={sym} len_5m={len(candles_5m)}")

    if len(candles_5m) < 2:
        _log(f"price_jump_insufficient_candles symbol={sym} len_5m={len(candles_5m)}", level=logging.WARNING)
        _send_skip("[SKIP] price_jump_insufficient_candles: 5m<2")
        log_signal(
            event="SKIP",
            symbol=sym,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="price_jump_insufficient_candles",
            candle_ts=latest_ts,
            extra=f"len_5m={len(candles_5m)}",
        )
        return False

    _, jump_mult = _get_session_multipliers(settings)
    base_jump = float(getattr(settings, "max_price_jump_pct", 0.003))
    max_jump_pct = base_jump * jump_mult

    try:
        last_price = float(candles_5m[-1][4])
        prev_price = float(candles_5m[-2][4])
        last_high = float(candles_5m[-1][2])
        last_low = float(candles_5m[-1][3])
    except Exception as e:
        _log(f"price_jump_parse_error symbol={sym} error={e}", level=logging.WARNING)
        _send_skip("[SKIP] price_jump_parse_error")
        log_signal(
            event="SKIP",
            symbol=sym,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="price_jump_parse_error",
            candle_ts=latest_ts,
            extra=str(e),
        )
        return False

    if prev_price <= 0 or last_price <= 0:
        _log(
            f"price_jump_invalid_price symbol={sym} prev_price={prev_price} last_price={last_price}",
            level=logging.WARNING,
        )
        _send_skip("[SKIP] price_jump_invalid_price: prev/last<=0")
        log_signal(
            event="SKIP",
            symbol=sym,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="price_jump_invalid_price",
            candle_ts=latest_ts,
            extra=f"prev_price={prev_price}, last_price={last_price}",
        )
        return False

    move_pct = abs(last_price - prev_price) / prev_price
    if move_pct > max_jump_pct:
        _log(
            f"price_jump_guard symbol={sym} move_pct={move_pct:.6f} limit={max_jump_pct:.6f} sess_mult={jump_mult}",
            level=logging.WARNING,
        )
        _send_skip(
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

    range_pct = (last_high - last_low) / last_price if last_low > 0 else 0.0
    candle_vol_limit = max_jump_pct * 1.8

    if range_pct > candle_vol_limit:
        _log(
            f"candle_volatility_guard symbol={sym} range_pct={range_pct:.6f} limit={candle_vol_limit:.6f}",
            level=logging.WARNING,
        )
        _send_skip(f"[SKIP] candle_volatility_guard: {range_pct:.4f} > {candle_vol_limit:.4f}")
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


def _is_depth_imbalanced(settings: Any, orderbook: Dict[str, Any]) -> bool:
    """5레벨 depth에서 매수/매도 한쪽 명목가 쏠림이 과도한지 확인."""
    enabled = bool(getattr(settings, "depth_imbalance_enabled", True))
    if not enabled:
        return False

    min_notional = float(getattr(settings, "depth_imbalance_min_notional", 10.0))
    min_ratio = float(getattr(settings, "depth_imbalance_min_ratio", 2.0))

    bids = orderbook.get("bids") or []
    asks = orderbook.get("asks") or []
    if not bids or not asks:
        return False

    def _side_notional(rows: List[Any]) -> float:
        total = 0.0
        for row in rows[:5]:
            if isinstance(row, list) and len(row) >= 2:
                price = float(row[0])
                qty = float(row[1])
            elif isinstance(row, tuple) and len(row) >= 2:
                price = float(row[0])
                qty = float(row[1])
            elif isinstance(row, dict):
                price = float(row["price"])
                qty = float(row["qty"])
            else:
                raise ValueError(f"unsupported orderbook row type: {type(row).__name__}")
            total += price * qty
        return total

    try:
        bid_notional = _side_notional(bids)
        ask_notional = _side_notional(asks)
    except Exception:
        return False

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
    orderbook: Dict[str, Any],
    best_bid: float,
    best_ask: float,
) -> bool:
    """markPrice / lastPrice 가 mid 대비 과도하게 이탈했는지 확인."""
    enabled = bool(getattr(settings, "price_deviation_guard_enabled", True))
    if not enabled:
        return False

    max_pct = float(getattr(settings, "price_deviation_max_pct", 0.0015))
    mark_price = orderbook.get("markPrice")
    last_price = orderbook.get("lastPrice")

    if mark_price is None and last_price is None:
        return False

    if best_bid <= 0 or best_ask <= 0:
        return False

    mid = (best_bid + best_ask) / 2.0
    if mid <= 0:
        return False

    if mark_price is not None:
        try:
            mark_price_f = float(mark_price)
            if abs(mark_price_f - mid) / mid > max_pct:
                return True
        except Exception:
            return True

    if last_price is not None:
        try:
            last_price_f = float(last_price)
            if abs(last_price_f - mid) / mid > max_pct:
                return True
        except Exception:
            return True

    return False


# ─────────────────────────────
# 5. 스프레드/호가 가드 (WS depth) + depth 쏠림 + mark/last 괴리
# ─────────────────────────────


def check_spread_guard(
    *,
    settings: Any,
    symbol: str,
    latest_ts: int,
    signal_source: str,
    direction: str,
) -> Tuple[bool, Optional[float], Optional[float]]:
    """Binance WS depth5 기반 스프레드/호가 가드."""
    orderbook = get_orderbook(symbol, 5)
    _log(f"spread_guard_check symbol={symbol} ob_ok={bool(orderbook)}")

    if not orderbook:
        _log(f"orderbook_unavailable symbol={symbol}", level=logging.WARNING)
        _send_skip("[SKIP] orderbook_unavailable: WS depth empty")
        log_signal(
            event="SKIP",
            symbol=symbol,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="orderbook_unavailable",
            candle_ts=latest_ts,
        )
        return False, None, None

    now_ms = _now_ms_utc()
    ob_ts = int(orderbook.get("ts") or 0)
    ob_age_ms = now_ms - ob_ts if ob_ts > 0 else -1
    max_ob_age = int(getattr(settings, "max_orderbook_age_ms", 3000))

    if ob_age_ms >= 0 and ob_age_ms > max_ob_age:
        _log(f"orderbook_stale symbol={symbol} ob_age_ms={ob_age_ms} max_ob_age={max_ob_age}", level=logging.WARNING)
        _send_skip("[SKIP] orderbook_stale")
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
        _log(f"orderbook_bbo_missing symbol={symbol}", level=logging.WARNING)
        _send_skip("[SKIP] orderbook_bbo_missing: no bids/asks")
        log_signal(
            event="SKIP",
            symbol=symbol,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="orderbook_bbo_missing",
            candle_ts=latest_ts,
        )
        return False, None, None

    try:
        bid0 = bids[0]
        ask0 = asks[0]

        if isinstance(bid0, (list, tuple)) and len(bid0) >= 2:
            best_bid = float(bid0[0])
            bid_qty = float(bid0[1])
        elif isinstance(bid0, dict):
            best_bid = float(bid0["price"])
            bid_qty = float(bid0["qty"])
        else:
            raise ValueError(f"unsupported bid row type: {type(bid0).__name__}")

        if isinstance(ask0, (list, tuple)) and len(ask0) >= 2:
            best_ask = float(ask0[0])
            ask_qty = float(ask0[1])
        elif isinstance(ask0, dict):
            best_ask = float(ask0["price"])
            ask_qty = float(ask0["qty"])
        else:
            raise ValueError(f"unsupported ask row type: {type(ask0).__name__}")
    except Exception as e:
        _log(f"spread_guard_parse_error symbol={symbol} error={e}", level=logging.WARNING)
        _send_skip("[SKIP] spread_guard_parse_error: BBO parse failed")
        log_signal(
            event="SKIP",
            symbol=symbol,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="spread_guard_parse_error",
            candle_ts=latest_ts,
            extra=str(e),
        )
        return False, None, None

    if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
        _log(f"bbo_crossed_or_invalid symbol={symbol} best_bid={best_bid} best_ask={best_ask}", level=logging.WARNING)
        _send_skip("[SKIP] bbo_crossed_or_invalid")
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

    min_bbo_notional = float(getattr(settings, "min_bbo_notional_usdt", 0.0))
    top_bid_notional = best_bid * bid_qty
    top_ask_notional = best_ask * ask_qty

    if min_bbo_notional > 0:
        if top_bid_notional < min_bbo_notional or top_ask_notional < min_bbo_notional:
            _log(
                f"bbo_notional_too_small symbol={symbol} "
                f"bid_notional={top_bid_notional:.4f} ask_notional={top_ask_notional:.4f} "
                f"min={min_bbo_notional:.4f}",
                level=logging.WARNING,
            )
            _send_skip("[SKIP] bbo_notional_too_small")
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

    max_spread_abs = float(getattr(settings, "max_spread_abs", 0.0))
    too_wide_by_pct = spread_pct > max_spread_pct
    too_wide_by_abs = max_spread_abs > 0 and spread_abs > max_spread_abs

    if too_wide_by_pct or too_wide_by_abs:
        _log(
            f"spread_guard symbol={symbol} pct={spread_pct:.8f} pct_limit={max_spread_pct:.8f} "
            f"abs={spread_abs:.8f} abs_limit={max_spread_abs:.8f} sess_mult={spread_mult}",
            level=logging.WARNING,
        )
        _send_skip(
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
                f"sess_mult={spread_mult}, top_bid_notional={top_bid_notional:.2f}, "
                f"top_ask_notional={top_ask_notional:.2f}"
            ),
        )
        return False, best_bid, best_ask

    if _is_depth_imbalanced(settings, orderbook):
        _log(f"depth_imbalance_guard symbol={symbol} ob_age_ms={ob_age_ms}", level=logging.WARNING)
        _send_skip("[SKIP] depth_imbalance_guard: one side dominates 5-level depth")
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

    if _is_price_deviation_large(settings, orderbook, best_bid, best_ask):
        _log(f"price_deviation_guard symbol={symbol} ob_age_ms={ob_age_ms}", level=logging.WARNING)
        _send_skip("[SKIP] price_deviation_guard: mark/last deviated from mid")
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
    "check_5m_delay_guard",
    "check_price_jump_guard",
    "check_spread_guard",
]