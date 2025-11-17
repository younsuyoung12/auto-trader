"""
entry_guards_ws.py
=====================================================
엔트리 직전 각종 가드를 묶어 놓은 모듈 (WS 버퍼 전용).

PATCH NOTES — 2025-11-17 (WS 원천 데이터 강제 + 폴백 제거, GPT 트레이더와 정합성 점검)
----------------------------------------------------
1) 모든 가드는 WS/실제 데이터가 이상하거나 부족할 때
   "통과(True)"가 아니라 **SKIP(거래 건너뛰기)** 로 동작하도록 유지.
   - 거래량 가드: 볼륨 파싱 에러 / 평균값 0 / 샘플 부족 시 진입 금지.
   - 가격 점프 가드: 5m 캔들 2개 미만 / 가격 0 이하 시 진입 금지.
   - 스프레드/호가 가드: 오더북 없음 / BBO 파싱 에러 / BBO 비정상 시 진입 금지.
2) entry_flow.try_open_new_position(...) 에서 WS 1m 캔들을 우선 넘기도록 변경된
   구조와 맞춰, 거래량 가드 docstring 을 1m/5m 겸용으로 정리.
3) gpt_trader.GuardBounds 와 필드명을 맞추기 위해, depth/스프레드/점프 관련
   설정 키를 재점검 (min_entry_volume_ratio / max_spread_pct /
   max_price_jump_pct / depth_imbalance_min_ratio / depth_imbalance_min_notional).
4) 각 SKIP 상황에서 telelog + signals_logger.log_signal 로 reason/extra 를 남겨
   이후 signals_logger 의 events CSV (log_skip_event) 와 함께 분석에 사용할 수 있도록
   코멘트 보강.
5) 5m_kline_delayed 판정 로직을 완화.
   - 단순히 마지막 5m ts 기준으로 now-ts 가 일정 시간(X ms)을 넘으면 무조건
     딜레이로 보지 않고,
   - WS 1m/5m 캔들의 ts 를 함께 사용해 "실제로 5m 캔들이 한두 개 이상 비어 있거나
     누락된 경우"에만 딜레이로 간주.
   - 시간 임계값도 5m 캔들 1~2개 정도 지연까지는 허용하도록 상향 조정
     (settings.max_5m_delay_ms 기반).

이 모듈 자체는 "가드 판단"만 담당하며, 최종 SKIP 이벤트 CSV 기록은
entry_flow 쪽에서 log_skip_event(...) 를 호출해 완성된다.
"""

from __future__ import annotations

import datetime
from typing import Any, Callable, List, Optional, Tuple

from telelog import send_skip_tg, log
from signals_logger import log_signal
from market_data_ws import get_orderbook  # WS 버퍼에서 호가 읽기 (폴백 없음)


# ─────────────────────────────
# 세션(시간대) 유틸
# ─────────────────────────────


def _get_session_multipliers(settings: Any) -> Tuple[float, float]:
    """현재 UTC 시각을 기반으로 어느 세션인지 대략 나누고,
    스프레드/점프 허용치에 곱할 배수를 돌려준다.

    - 아시아: 00~07시
    - 유럽: 07~13시
    - 미국: 13~24시
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
    """usedMargin>0 이면 수동/기존 포지션이 있다고 보고 신규 진입을 막는다.

    - 잔고 조회 에러 자체도 리스크로 보고 진입을 SKIP 한다.
    - 실제 SKIP 이벤트 CSV 기록은 entry_flow 에서 log_skip_event(...) 로 수행.
    """
    try:
        bal_detail = get_balance_detail_func()
        used_margin = float(bal_detail.get("usedMargin") or 0.0)
    except Exception as e:  # 잔고 조회 자체가 실패하면 바로 SKIP
        log(f"[MANUAL_GUARD] balance detail error: {e}")
        send_skip_tg("[SKIP] manual_guard_balance_error: 잔고 상세 조회 실패")
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
        # 이미 다른 포지션(수동/타 전략)이 열려 있다고 보고 새 진입을 막는다.
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
# 2. 거래량 가드 (WS 1m 또는 5m raw 기준)
# ─────────────────────────────


def check_volume_guard(
    *,
    settings: Any,
    candles_5m_raw: List[Any],
    latest_ts: int,
    signal_source: str,
    direction: str,
) -> bool:
    """최근 거래량이 직전 20개 평균 대비 너무 작으면 진입을 막는다.

    - entry_flow 에서 WS 1m 버퍼가 있으면 1m 캔들 리스트를, 없으면 5m raw 를 넘긴다.
      · 포맷 가정: [ts, open, high, low, close, volume]
    - 볼륨 파싱 실패 / 샘플 부족 / 평균 0 인 경우도 모두 SKIP (폴백 금지).
    """
    sym = getattr(settings, "symbol", "UNKNOWN")
    log(f"[GUARD] (WS) volume len={len(candles_5m_raw)} symbol={sym}")

    # gpt_trader.GuardBounds.min_entry_volume_ratio 와 동일 키
    min_vol_ratio = float(getattr(settings, "min_entry_volume_ratio", 0.3))

    # 샘플이 너무 적으면 거래량 가드 자체를 수행하지 않고 진입을 막는다.
    if len(candles_5m_raw) < 20:
        send_skip_tg("[SKIP] volume_guard_insufficient_samples: len<20")
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
        avg_vol_20 = sum(vols_20) / len(vols_20) if vols_20 else 0.0
    except Exception as e:
        # 볼륨 파싱 에러 → 진입 금지 (폴백 없음)
        send_skip_tg("[SKIP] volume_guard_parse_error: volume parsing failed")
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
        # 기준 평균이 0 이면 거래량 상태가 비정상 → 진입 금지
        send_skip_tg("[SKIP] volume_guard_no_reference: avg_vol_20<=0")
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
# 3. 5m 딜레이 가드 (WS 1m/5m ts 기반)
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
    """5m_kline_delayed 완화 버전.

    - 예전 버전: 마지막 5m 캔들 ts 기준으로 now - ts > X(ms)이면 바로 딜레이로 간주하고 SKIP.
    - 이번 버전:
        · WS 1m/5m 캔들의 ts 를 함께 사용해,
        · 5m 캔들이 실제로 한두 개 이상 비어 있거나
        · 1m 캔들 ts 흐름 대비 5m 집계가 명확히 따라오지 못하는 경우에만 delayed 로 본다.
    - 단순 시간 기준 X(ms)도 5m 캔들 1~2개 정도 지연까지 허용하도록 상향
      (settings.max_5m_delay_ms, 기본 10분).
    """
    sym = getattr(settings, "symbol", "UNKNOWN")
    log(f"[GUARD] (WS) 5m_delay symbol={sym} len_1m={len(candles_1m)} len_5m={len(candles_5m)}")

    # 5m 캔들이 아예 없으면 신뢰 불가 → 보수적으로 SKIP
    if not candles_5m:
        send_skip_tg("[SKIP] 5m_delay_no_5m_candles")
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
        send_skip_tg("[SKIP] 5m_delay_ts_parse_error: last_5m_ts")
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

    now_ms = int(datetime.datetime.utcnow().timestamp() * 1000)

    # 기본값: 5m * 2 = 10분까지는 허용
    default_max_delay_ms = 2 * 5 * 60 * 1000
    max_delay_ms = int(getattr(settings, "max_5m_delay_ms", default_max_delay_ms))

    # 1m/5m ts 차이로 "비어 있는 5m 캔들 개수"를 대략 추정
    missing_5m_count = 0
    if last_1m_ts is not None and last_1m_ts > last_5m_ts:
        five_min_ms = 5 * 60 * 1000
        missing_5m_count = max(0, (last_1m_ts - last_5m_ts) // five_min_ms)

    delayed_by_gap = missing_5m_count >= 2  # "한두 개 이상" → 2개 이상이면 지연으로 본다.
    delayed_by_time = (now_ms - last_5m_ts) > max_delay_ms

    if delayed_by_gap or delayed_by_time:
        reason = "5m_kline_delayed"
        extra = (
            f"missing_5m_count={missing_5m_count}, "
            f"now_gap_ms={now_ms - last_5m_ts}, max_delay_ms={max_delay_ms}, "
            f"last_1m_ts={last_1m_ts}, last_5m_ts={last_5m_ts}"
        )
        send_skip_tg(f"[SKIP] {reason}: {extra}")
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
    """직전 5m 종가/고가/저가를 기준으로 급격한 점프/변동성을 제한한다.

    - 5m 캔들이 2개 미만이면 기준이 없어 진입을 막는다.
    - 가격이 0 이하 같은 비정상 값도 진입 금지.
    - gpt_trader.GuardBounds.max_price_jump_pct 와 동일 필드 사용.
    """
    sym = getattr(settings, "symbol", "UNKNOWN")
    log(f"[GUARD] (WS) price_jump len={len(candles_5m)} symbol={sym}")

    if len(candles_5m) < 2:
        send_skip_tg("[SKIP] price_jump_insufficient_candles: 5m<2")
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

    # 세션별 jump 배수
    _, jump_mult = _get_session_multipliers(settings)
    base_jump = float(getattr(settings, "max_price_jump_pct", 0.003))
    max_jump_pct = base_jump * jump_mult

    last_price = float(candles_5m[-1][4])
    prev_price = float(candles_5m[-2][4])
    last_high = float(candles_5m[-1][2])
    last_low = float(candles_5m[-1][3])

    if prev_price <= 0 or last_price <= 0:
        send_skip_tg("[SKIP] price_jump_invalid_price: prev/last<=0")
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

    # 캔들 자체의 몸통/꼬리 크기 제한 (jump_pct * 1.8 배)
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
    """5레벨 depth 에서 한쪽(매수/매도) 명목가가 너무 큰지 확인한다.

    - gpt_trader.GuardBounds.depth_imbalance_min_notional / depth_imbalance_min_ratio
      와 이름을 맞춰 두었다.
    """
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
    """markPrice/lastPrice 가 mid 에서 과도하게 벗어나 있는지 확인한다."""
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
    """WS depth5 기반 스프레드/호가 가드.

    - 오더북 없음 / BBO 파싱 에러 / BBO 비정상 / 너무 넓은 스프레드 → 전부 SKIP.
    - depth 한쪽 쏠림 / mark·last 괴리도 진입 금지.
    - 성공 시 True + (best_bid, best_ask)를 반환.
    - gpt_trader.GuardBounds.max_spread_pct 와 동일 필드 사용.
    """
    orderbook = get_orderbook(symbol, 5)
    log(f"[GUARD] (WS) spread symbol={symbol} ob_ok={bool(orderbook)}")

    if not orderbook:
        send_skip_tg("[SKIP] orderbook_unavailable: WS depth empty")
        log_signal(
            event="SKIP",
            symbol=symbol,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="orderbook_unavailable",
            candle_ts=latest_ts,
        )
        return False, None, None

    # D) 오더북 신선도(지연) 가드
    now_ms = int(datetime.datetime.utcnow().timestamp() * 1000)
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
        send_skip_tg("[SKIP] orderbook_bbo_missing: no bids/asks")
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
        send_skip_tg("[SKIP] spread_guard_parse_error: BBO parse failed")
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
    "check_5m_delay_guard",
    "check_price_jump_guard",
    "check_spread_guard",
]
