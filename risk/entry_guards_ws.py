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
- settings 누락/오염은 즉시 예외.
- 이 파일은 "가드 판단"만 수행하며 주문 실행은 포함하지 않는다.

주의: manual_position_guard는 Binance /fapi/v2/balance 응답 구조에 100% 정합해야 한다.
- 사용 필드: availableBalance, crossWalletBalance, crossUnPnl
- unrealizedProfit 사용 금지

변경 이력
-----------------------------------------------------
- 2026-03-11:
  1) FIX(STRICT): settings getattr(..., default) 기반 숨은 fallback 제거
  2) FIX(ROOT-CAUSE): depth/mark/last 보조 가드의 파싱 실패 fail-open 제거 → 차단 처리
  3) FIX(ROOT-CAUSE): orderbook.ts 누락/비정상 시 stale pass-through 제거 → 차단 처리
  4) FIX(ROOT-CAUSE): 1m ts 파싱 실패 시 None 무시 제거 → 차단 처리
  5) FIX(STRICT): BBO/orderbook/price/volume 유한성 검증 강화
- 2026-03-06:
  1) FIX(TRADE-GRADE): signals_logger STRICT 정책 준수
     - log_signal 호출에서 extra(str) 전달 금지
     - extra_json(dict)로 통일하여 bt_events.extra_json(JSONB) 계약 준수
     - 가드 로직/판정/리턴 흐름은 변경하지 않음
"""

from __future__ import annotations

import datetime
import logging
import math
from typing import Any, Callable, Dict, List, Optional, Tuple

from infra.telelog import send_skip_tg
from events.signals_logger import log_signal
from infra.market_data_ws import get_orderbook

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


def _require_finite_float(value: Any, name: str) -> float:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if isinstance(value, bool):
        raise RuntimeError(f"{name} must be numeric, bool is not allowed (STRICT)")
    try:
        out = float(value)
    except Exception as e:
        raise RuntimeError(f"{name} float parse failed: {e}") from e
    if not math.isfinite(out):
        raise RuntimeError(f"{name} must be finite (STRICT)")
    return out


def _require_nonnegative_finite_float(value: Any, name: str) -> float:
    out = _require_finite_float(value, name)
    if out < 0.0:
        raise RuntimeError(f"{name} must be >= 0 (STRICT)")
    return out


def _require_positive_finite_float(value: Any, name: str) -> float:
    out = _require_finite_float(value, name)
    if out <= 0.0:
        raise RuntimeError(f"{name} must be > 0 (STRICT)")
    return out


def _require_int(value: Any, name: str) -> int:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if isinstance(value, bool):
        raise RuntimeError(f"{name} must be int, bool is not allowed (STRICT)")
    try:
        out = int(value)
    except Exception as e:
        raise RuntimeError(f"{name} int parse failed: {e}") from e
    return out


def _require_positive_int(value: Any, name: str) -> int:
    out = _require_int(value, name)
    if out <= 0:
        raise RuntimeError(f"{name} must be > 0 (STRICT)")
    return out


def _require_setting_present(settings: Any, name: str) -> Any:
    if settings is None:
        raise RuntimeError("settings is required (STRICT)")
    if not hasattr(settings, name):
        raise RuntimeError(f"settings.{name} missing (STRICT)")
    return getattr(settings, name)


def _require_setting_float(
    settings: Any,
    name: str,
    *,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> float:
    value = _require_finite_float(_require_setting_present(settings, name), f"settings.{name}")
    if min_value is not None and value < min_value:
        raise RuntimeError(f"settings.{name} must be >= {min_value} (STRICT)")
    if max_value is not None and value > max_value:
        raise RuntimeError(f"settings.{name} must be <= {max_value} (STRICT)")
    return value


def _require_setting_int(
    settings: Any,
    name: str,
    *,
    min_value: Optional[int] = None,
) -> int:
    value = _require_int(_require_setting_present(settings, name), f"settings.{name}")
    if min_value is not None and value < min_value:
        raise RuntimeError(f"settings.{name} must be >= {min_value} (STRICT)")
    return value


def _require_setting_bool(settings: Any, name: str) -> bool:
    value = _require_setting_present(settings, name)
    if not isinstance(value, bool):
        raise RuntimeError(f"settings.{name} must be bool (STRICT)")
    return value


def _parse_bbo_row_strict(row: Any, row_name: str) -> Tuple[float, float]:
    if isinstance(row, (list, tuple)):
        if len(row) < 2:
            raise RuntimeError(f"{row_name} must have len>=2 (STRICT)")
        price = _require_positive_finite_float(row[0], f"{row_name}.price")
        qty = _require_nonnegative_finite_float(row[1], f"{row_name}.qty")
        return price, qty

    if isinstance(row, dict):
        price = _require_positive_finite_float(row.get("price"), f"{row_name}.price")
        qty = _require_nonnegative_finite_float(row.get("qty"), f"{row_name}.qty")
        return price, qty

    raise RuntimeError(f"{row_name} unsupported type: {type(row).__name__} (STRICT)")


def _depth_side_notional_strict(rows: List[Any], side_name: str) -> float:
    if not isinstance(rows, list) or not rows:
        raise RuntimeError(f"{side_name} rows missing/empty (STRICT)")
    total = 0.0
    for idx, row in enumerate(rows[:5]):
        price, qty = _parse_bbo_row_strict(row, f"{side_name}[{idx}]")
        total += price * qty
    if not math.isfinite(total) or total < 0.0:
        raise RuntimeError(f"{side_name} total notional invalid (STRICT)")
    return total


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
        spread_mult = _require_setting_float(settings, "session_spread_mult_asia", min_value=0.0)
        jump_mult = _require_setting_float(settings, "session_jump_mult_asia", min_value=0.0)
    elif 7 <= h < 13:  # 유럽
        spread_mult = _require_setting_float(settings, "session_spread_mult_eu", min_value=0.0)
        jump_mult = _require_setting_float(settings, "session_jump_mult_eu", min_value=0.0)
    else:  # 미국
        spread_mult = _require_setting_float(settings, "session_spread_mult_us", min_value=0.0)
        jump_mult = _require_setting_float(settings, "session_jump_mult_us", min_value=0.0)

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
    if not callable(get_balance_detail_func):
        raise RuntimeError("get_balance_detail_func must be callable (STRICT)")

    ts_int = int(latest_ts)

    bal_detail = get_balance_detail_func()
    if not isinstance(bal_detail, dict):
        raise RuntimeError(
            f"balance detail must be dict, got {type(bal_detail).__name__} (STRICT)"
        )

    required = ("availableBalance", "crossWalletBalance", "crossUnPnl")
    missing = [k for k in required if k not in bal_detail]
    if missing:
        raise RuntimeError(f"balance detail missing keys: {missing} (STRICT)")

    available_balance = _require_nonnegative_finite_float(
        bal_detail["availableBalance"],
        "balance.availableBalance",
    )
    cross_wallet_balance = _require_nonnegative_finite_float(
        bal_detail["crossWalletBalance"],
        "balance.crossWalletBalance",
    )
    cross_un_pnl = _require_finite_float(
        bal_detail["crossUnPnl"],
        "balance.crossUnPnl",
    )

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
            direction="CLOSE",
            reason="manual_position_detected",
            candle_ts=ts_int,
            extra_json={
                "availableBalance": float(available_balance),
                "crossWalletBalance": float(cross_wallet_balance),
                "crossUnPnl": float(cross_un_pnl),
                "equity": float(equity),
                "used_margin": float(used_margin),
            },
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
    sym = _require_setting_present(settings, "symbol")
    _log(f"volume_guard_check symbol={sym} len={len(candles_5m_raw)}")

    min_vol_ratio = _require_setting_float(settings, "min_entry_volume_ratio", min_value=0.0, max_value=1.0)

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
            extra_json={"len": int(len(candles_5m_raw))},
        )
        return False

    try:
        last_vol = _require_nonnegative_finite_float(candles_5m_raw[-1][5], "candles_5m_raw[-1].volume")
        vols_20 = [
            _require_nonnegative_finite_float(c[5], f"candles_5m_raw[{idx}].volume")
            for idx, c in enumerate(candles_5m_raw[-20:])
        ]
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
            extra_json={"error": str(e)},
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
            extra_json={"last_vol": float(last_vol), "avg_vol_20": float(avg_vol_20)},
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
            extra_json={
                "last_vol": float(last_vol),
                "avg_vol_20": float(avg_vol_20),
                "ratio": float(ratio),
                "threshold": float(min_vol_ratio),
            },
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
    - 1m ts 파싱 실패 → 차단
    - 1m 대비 5m 누락이 2개 이상 추정 → 차단
    - now - last_5m_ts > max_5m_delay_ms → 차단
    """
    sym = _require_setting_present(settings, "symbol")
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
            extra_json={"len_1m": int(len(candles_1m)), "len_5m": int(len(candles_5m))},
        )
        return False

    try:
        last_5m_ts = _require_positive_int(candles_5m[-1][0], "candles_5m[-1].ts")
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
            extra_json={"target": "last_5m_ts", "error": str(e)},
        )
        return False

    last_1m_ts: Optional[int] = None
    if candles_1m:
        try:
            last_1m_ts = _require_positive_int(candles_1m[-1][0], "candles_1m[-1].ts")
        except Exception as e:
            _log(f"5m_delay_ts_parse_error symbol={sym} target=last_1m_ts error={e}", level=logging.WARNING)
            _send_skip("[SKIP] 5m_delay_ts_parse_error: last_1m_ts")
            log_signal(
                event="SKIP",
                symbol=sym,
                strategy_type=signal_source or "UNKNOWN",
                direction=direction,
                reason="5m_delay_ts_parse_error",
                candle_ts=latest_ts,
                extra_json={"target": "last_1m_ts", "error": str(e)},
            )
            return False

    now_ms = _now_ms_utc()
    max_delay_ms = _require_setting_int(settings, "max_5m_delay_ms", min_value=1)

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
            extra_json={
                "missing_5m_count": int(missing_5m_count),
                "now_gap_ms": int(now_ms - last_5m_ts),
                "max_delay_ms": int(max_delay_ms),
                "last_1m_ts": None if last_1m_ts is None else int(last_1m_ts),
                "last_5m_ts": int(last_5m_ts),
                "delayed_by_gap": bool(delayed_by_gap),
                "delayed_by_time": bool(delayed_by_time),
            },
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
    sym = _require_setting_present(settings, "symbol")
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
            extra_json={"len_5m": int(len(candles_5m))},
        )
        return False

    _, jump_mult = _get_session_multipliers(settings)
    base_jump = _require_setting_float(settings, "max_price_jump_pct", min_value=0.0)
    max_jump_pct = base_jump * jump_mult

    try:
        last_price = _require_positive_finite_float(candles_5m[-1][4], "candles_5m[-1].close")
        prev_price = _require_positive_finite_float(candles_5m[-2][4], "candles_5m[-2].close")
        last_high = _require_positive_finite_float(candles_5m[-1][2], "candles_5m[-1].high")
        last_low = _require_positive_finite_float(candles_5m[-1][3], "candles_5m[-1].low")
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
            extra_json={"error": str(e)},
        )
        return False

    if last_high < last_low:
        _log(
            f"price_jump_invalid_range symbol={sym} last_high={last_high} last_low={last_low}",
            level=logging.WARNING,
        )
        _send_skip("[SKIP] price_jump_invalid_range: high<low")
        log_signal(
            event="SKIP",
            symbol=sym,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="price_jump_invalid_range",
            candle_ts=latest_ts,
            extra_json={"last_high": float(last_high), "last_low": float(last_low)},
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
            extra_json={"move_pct": float(move_pct), "limit": float(max_jump_pct), "sess_mult": float(jump_mult)},
        )
        return False

    range_pct = (last_high - last_low) / last_price
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
            extra_json={"range_pct": float(range_pct), "limit": float(candle_vol_limit), "sess_mult": float(jump_mult)},
        )
        return False

    return True


# ─────────────────────────────
# (보조) depth 한쪽 쏠림 체크
# ─────────────────────────────


def _is_depth_imbalanced(settings: Any, orderbook: Dict[str, Any]) -> bool:
    """5레벨 depth에서 매수/매도 한쪽 명목가 쏠림이 과도한지 확인."""
    enabled = _require_setting_bool(settings, "depth_imbalance_enabled")
    if not enabled:
        return False

    min_notional = _require_setting_float(settings, "depth_imbalance_min_notional", min_value=0.0)
    min_ratio = _require_setting_float(settings, "depth_imbalance_min_ratio", min_value=1.0)

    bids = orderbook.get("bids")
    asks = orderbook.get("asks")
    if not isinstance(bids, list) or not isinstance(asks, list):
        raise RuntimeError("orderbook.bids/asks must be list (STRICT)")
    if not bids or not asks:
        raise RuntimeError("orderbook.bids/asks empty (STRICT)")

    bid_notional = _depth_side_notional_strict(bids, "bids")
    ask_notional = _depth_side_notional_strict(asks, "asks")

    if bid_notional < min_notional and ask_notional < min_notional:
        return False

    bigger = max(bid_notional, ask_notional)
    smaller = min(bid_notional, ask_notional)
    if smaller == 0.0:
        return True

    ratio = bigger / smaller
    if not math.isfinite(ratio):
        raise RuntimeError("depth imbalance ratio invalid (STRICT)")
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
    enabled = _require_setting_bool(settings, "price_deviation_guard_enabled")
    if not enabled:
        return False

    max_pct = _require_setting_float(settings, "price_deviation_max_pct", min_value=0.0)
    mark_price = orderbook.get("markPrice")
    last_price = orderbook.get("lastPrice")

    if mark_price is None and last_price is None:
        raise RuntimeError("markPrice and lastPrice both missing (STRICT)")

    if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
        raise RuntimeError("best bid/ask invalid for price deviation check (STRICT)")

    mid = (best_bid + best_ask) / 2.0
    if mid <= 0:
        raise RuntimeError("mid price invalid (STRICT)")

    if mark_price is not None:
        mark_price_f = _require_positive_finite_float(mark_price, "orderbook.markPrice")
        if abs(mark_price_f - mid) / mid > max_pct:
            return True

    if last_price is not None:
        last_price_f = _require_positive_finite_float(last_price, "orderbook.lastPrice")
        if abs(last_price_f - mid) / mid > max_pct:
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

    if not isinstance(orderbook, dict) or not orderbook:
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

    try:
        ob_ts = _require_positive_int(orderbook.get("ts"), "orderbook.ts")
    except Exception as e:
        _log(f"orderbook_ts_invalid symbol={symbol} error={e}", level=logging.WARNING)
        _send_skip("[SKIP] orderbook_ts_invalid")
        log_signal(
            event="SKIP",
            symbol=symbol,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="orderbook_ts_invalid",
            candle_ts=latest_ts,
            extra_json={"error": str(e)},
        )
        return False, None, None

    now_ms = _now_ms_utc()
    ob_age_ms = now_ms - ob_ts
    max_ob_age = _require_setting_int(settings, "max_orderbook_age_ms", min_value=1)

    if ob_age_ms < 0:
        _log(f"orderbook_ts_future symbol={symbol} ob_ts={ob_ts} now_ms={now_ms}", level=logging.WARNING)
        _send_skip("[SKIP] orderbook_ts_future")
        log_signal(
            event="SKIP",
            symbol=symbol,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="orderbook_ts_future",
            candle_ts=latest_ts,
            extra_json={"ob_ts": int(ob_ts), "now_ms": int(now_ms)},
        )
        return False, None, None

    if ob_age_ms > max_ob_age:
        _log(f"orderbook_stale symbol={symbol} ob_age_ms={ob_age_ms} max_ob_age={max_ob_age}", level=logging.WARNING)
        _send_skip("[SKIP] orderbook_stale")
        log_signal(
            event="SKIP",
            symbol=symbol,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="orderbook_stale",
            candle_ts=latest_ts,
            extra_json={"ob_age_ms": int(ob_age_ms), "max_ob_age": int(max_ob_age)},
        )
        return False, None, None

    bids = orderbook.get("bids")
    asks = orderbook.get("asks")
    if not isinstance(bids, list) or not isinstance(asks, list) or not bids or not asks:
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
        best_bid, bid_qty = _parse_bbo_row_strict(bids[0], "bids[0]")
        best_ask, ask_qty = _parse_bbo_row_strict(asks[0], "asks[0]")
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
            extra_json={"error": str(e)},
        )
        return False, None, None

    if best_bid >= best_ask:
        _log(f"bbo_crossed_or_invalid symbol={symbol} best_bid={best_bid} best_ask={best_ask}", level=logging.WARNING)
        _send_skip("[SKIP] bbo_crossed_or_invalid")
        log_signal(
            event="SKIP",
            symbol=symbol,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="bbo_crossed_or_invalid",
            candle_ts=latest_ts,
            extra_json={"best_bid": float(best_bid), "best_ask": float(best_ask)},
        )
        return False, best_bid, best_ask

    min_bbo_notional = _require_setting_float(settings, "min_bbo_notional_usdt", min_value=0.0)
    top_bid_notional = best_bid * bid_qty
    top_ask_notional = best_ask * ask_qty

    if min_bbo_notional > 0.0:
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
                extra_json={
                    "bid_notional": float(top_bid_notional),
                    "ask_notional": float(top_ask_notional),
                    "min_bbo_notional": float(min_bbo_notional),
                },
            )
            return False, best_bid, best_ask

    spread_mult, _ = _get_session_multipliers(settings)
    base_spread = _require_setting_float(settings, "max_spread_pct", min_value=0.0)
    max_spread_pct = base_spread * spread_mult
    max_spread_abs = _require_setting_float(settings, "max_spread_abs", min_value=0.0)

    spread_abs = best_ask - best_bid
    spread_pct = spread_abs / best_bid

    too_wide_by_pct = spread_pct > max_spread_pct
    too_wide_by_abs = max_spread_abs > 0.0 and spread_abs > max_spread_abs

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
            f"{' (limit=' + str(max_spread_abs) + ')' if max_spread_abs > 0 else ''}"
        )
        log_signal(
            event="SKIP",
            symbol=symbol,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="spread_guard",
            candle_ts=latest_ts,
            spread_pct=spread_pct,
            extra_json={
                "ob_age_ms": int(ob_age_ms),
                "spread_abs": float(spread_abs),
                "spread_pct_limit": float(max_spread_pct),
                "spread_abs_limit": float(max_spread_abs),
                "sess_mult": float(spread_mult),
                "top_bid_notional": float(top_bid_notional),
                "top_ask_notional": float(top_ask_notional),
            },
        )
        return False, best_bid, best_ask

    try:
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
                extra_json={"ob_age_ms": int(ob_age_ms)},
            )
            return False, best_bid, best_ask
    except Exception as e:
        _log(f"depth_imbalance_parse_error symbol={symbol} error={e}", level=logging.WARNING)
        _send_skip("[SKIP] depth_imbalance_parse_error")
        log_signal(
            event="SKIP",
            symbol=symbol,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="depth_imbalance_parse_error",
            candle_ts=latest_ts,
            extra_json={"error": str(e), "ob_age_ms": int(ob_age_ms)},
        )
        return False, best_bid, best_ask

    try:
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
                extra_json={"ob_age_ms": int(ob_age_ms)},
            )
            return False, best_bid, best_ask
    except Exception as e:
        _log(f"price_deviation_parse_error symbol={symbol} error={e}", level=logging.WARNING)
        _send_skip("[SKIP] price_deviation_parse_error")
        log_signal(
            event="SKIP",
            symbol=symbol,
            strategy_type=signal_source or "UNKNOWN",
            direction=direction,
            reason="price_deviation_parse_error",
            candle_ts=latest_ts,
            extra_json={"error": str(e), "ob_age_ms": int(ob_age_ms)},
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