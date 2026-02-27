# entry_engine.py
"""Binance USDT-M Futures Entry Engine

설계 원칙
- Binance USDT-M Futures 기준만 사용한다.
- 수량/틱 라운딩은 entry 단계에서 절대 하지 않는다. (order_executor에 위임)
- 잘못된 설정/입력은 즉시 예외(ValueError/RuntimeError).
- 폴백(REST 백필/더미 점수/임의 보정 등) 로직을 두지 않는다.
- 운영 환경을 기준으로 logging을 사용하고, 민감정보는 로그/출력하지 않는다.
"""

from __future__ import annotations

import logging
import math
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple


logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# imports (패키지 구조/플랫 구조 모두 지원)
# ─────────────────────────────────────────────────────────────────────────────

try:
    from settings_ws import load_settings
except ImportError as e:  # pragma: no cover
    raise RuntimeError("settings_ws.load_settings import failed") from e

try:
    from infra.data_health_monitor import HEALTH_OK, LAST_FAIL_REASON
except ImportError:  # pragma: no cover
    from data_health_monitor import HEALTH_OK, LAST_FAIL_REASON  # type: ignore

try:
    from infra.telelog import send_tg, send_skip_tg
except ImportError:  # pragma: no cover
    from telelog import send_tg, send_skip_tg  # type: ignore

try:
    from risk.entry_guards_ws import (
        check_manual_position_guard,
        check_price_jump_guard,
        check_spread_guard,
        check_volume_guard,
    )
except ImportError:  # pragma: no cover
    from entry_guards_ws import (  # type: ignore
        check_manual_position_guard,
        check_price_jump_guard,
        check_spread_guard,
        check_volume_guard,
    )

try:
    from execution.exchange_api import get_available_usdt, get_balance_detail
except ImportError:  # pragma: no cover
    from exchange_api import get_available_usdt, get_balance_detail  # type: ignore

# liquidation guard용 (없으면 RuntimeError로 차단)
try:
    from execution.exchange_api import get_position
except ImportError:  # pragma: no cover
    try:
        from exchange_api import get_position  # type: ignore
    except ImportError:  # pragma: no cover
        get_position = None  # type: ignore

try:
    from execution.order_executor import open_position_with_tp_sl
except ImportError:  # pragma: no cover
    from order_executor import open_position_with_tp_sl  # type: ignore

try:
    from events.signals_logger import (
        log_candle_snapshot,
        log_event,
        log_gpt_entry_event,
        log_signal,
        log_skip_event,
    )
except ImportError:  # pragma: no cover
    from signals_logger import (  # type: ignore
        log_candle_snapshot,
        log_event,
        log_gpt_entry_event,
        log_signal,
        log_skip_event,
    )

try:
    from strategy.gpt_trader import decide_entry_with_gpt_trader
except ImportError:  # pragma: no cover
    from gpt_trader import decide_entry_with_gpt_trader  # type: ignore

try:
    from infra.market_features_ws import get_trading_signal
except ImportError:  # pragma: no cover
    from market_features_ws import get_trading_signal  # type: ignore

try:
    from state.db_core import SessionLocal
    from state.db_models import Trade as TradeORM
    from sqlalchemy import func, select
except ImportError:  # pragma: no cover
    from db_core import SessionLocal  # type: ignore
    from db_models import Trade as TradeORM  # type: ignore
    from sqlalchemy import func, select  # type: ignore

try:
    from state.trader_state import Trade
except ImportError:  # pragma: no cover
    from trader import Trade  # type: ignore


# ─────────────────────────────────────────────────────────────────────────────
# settings load
# ─────────────────────────────────────────────────────────────────────────────

SET = load_settings()


# ─────────────────────────────────────────────────────────────────────────────
# try_open_new_position()
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_GPT_ENTRY_MODEL = "gpt-4o-mini"
KST = timezone(timedelta(hours=9))

# try_open_new_position 반환용 기본 슬립(초)
_SLEEP_DEFAULT = 1.0
_SLEEP_GUARD = 2.0
_SLEEP_BALANCE = 5.0
_SLEEP_GPT_ERROR = 3.0
_SLEEP_GPT_SKIP = 3.0
_SLEEP_RISK_GUARD = 5.0
_SLEEP_POST_ENTRY = 5.0


def try_open_new_position(
    settings: Any,
    last_close_ts: float,
) -> Tuple[Optional[Trade], float]:
    """WS 시그널 → 가드 → GPT → 하드 리스크 가드 → 주문 실행.

    Returns:
        (Trade, sleep_sec) if entry opened, else (None, sleep_sec).

    STRICT:
    - 수량/틱 라운딩은 하지 않는다.
    - 잘못된 설정/입력은 예외.
    - 폴백 로직 없음.
    """

    try:
        _validate_settings_contract(settings)

        # 0) 데이터 헬스 FAIL 시 신규 진입 차단
        if not HEALTH_OK:
            msg = f"[SKIP][DATA_HEALTH_FAIL] entry blocked: {LAST_FAIL_REASON}"
            _safe_send_skip(msg)
            logger.warning(msg)
            log_event(
                "on_risk_guard_trigger",
                symbol=str(settings.symbol),
                regime="NO_TRADE",
                source="entry_engine.data_health",
                side="",
                reason="data_health_fail",
                extra_json={"detail": str(LAST_FAIL_REASON)},
            )
            return None, _SLEEP_DEFAULT

        symbol = str(settings.symbol)

        # 1) WS 시그널 받기
        signal_ctx = get_trading_signal(settings=settings, last_close_ts=last_close_ts)
        if signal_ctx is None:
            msg = f"[SKIP] no_signal: symbol={symbol}"
            _safe_send_skip(msg)
            logger.info(msg)
            log_skip_event(
                symbol=symbol,
                regime="NO_TRADE",
                source="entry_engine.signal",
                side=None,
                reason="no_signal_from_ws_features",
                extra=None,
            )
            return None, _SLEEP_DEFAULT

        (
            chosen_signal,
            signal_source,
            latest_ts,
            candles_5m,
            candles_5m_raw,
            last_price,
            extra,
        ) = signal_ctx

        chosen_signal = str(chosen_signal).upper().strip()
        if chosen_signal not in ("LONG", "SHORT"):
            raise ValueError(f"invalid chosen_signal: {chosen_signal!r}")

        signal_source_s = (
            str(signal_source) if isinstance(signal_source, str) else "GENERIC"
        )
        regime_label = signal_source_s or "GENERIC"

        # 이벤트: 시그널 후보
        try:
            log_event(
                "on_signal_candidate",
                symbol=symbol,
                regime=regime_label,
                source="entry_engine.signal",
                side=chosen_signal,
                price=float(last_price) if _is_finite_pos(last_price) else None,
                reason="signal_candidate",
                extra_json={"latest_ts": int(latest_ts)},
            )
        except Exception as e:  # pragma: no cover
            logger.debug("log_event(on_signal_candidate) failed: %s", e)

        # 캔들 스냅샷
        try:
            c_ts, o, h, l, c, v = _pick_latest_candle(
                signal_candles_raw=candles_5m_raw,
                signal_candles=candles_5m,
            )
            log_candle_snapshot(
                symbol=symbol,
                tf="5m",
                candle_ts=c_ts,
                open_=o,
                high=h,
                low=l,
                close=c,
                volume=v,
                strategy_type=signal_source_s,
                direction=chosen_signal,
                extra="signal_eval=1",
            )
        except Exception as e:  # pragma: no cover
            logger.debug("log_candle_snapshot failed: %s", e)

        # 2) 수동/외부 가드 (GPT 이전)
        try:
            manual_ok = check_manual_position_guard(
                get_balance_detail_func=get_balance_detail,
                symbol=symbol,
                latest_ts=float(latest_ts),
            )
        except Exception as e:
            raise RuntimeError(f"manual_position_guard failed: {e}") from e

        if not manual_ok:
            msg = "[SKIP] manual_guard_blocked"
            _safe_send_skip(msg)
            logger.info(msg)
            log_skip_event(
                symbol=symbol,
                regime=regime_label,
                source="entry_engine.guard.manual",
                side=chosen_signal,
                reason="manual_guard_blocked",
                extra={"latest_ts": int(latest_ts)},
            )
            return None, _SLEEP_GUARD

        # 3) 가용 잔고 확인
        avail_usdt = float(get_available_usdt())
        if not math.isfinite(avail_usdt) or avail_usdt <= 0:
            msg = f"[SKIP] available_usdt<=0: available_usdt={avail_usdt}"
            _safe_send_skip(msg)
            logger.warning(msg)
            log_skip_event(
                symbol=symbol,
                regime=regime_label,
                source="entry_engine.balance",
                side=chosen_signal,
                reason="balance_zero_or_invalid",
                extra={"available_usdt": avail_usdt},
            )
            return None, _SLEEP_BALANCE

        # 4) 기본 주문 파라미터
        leverage = _as_float(getattr(settings, "leverage"), "leverage", min_value=1.0)
        base_risk_pct = _as_float(
            getattr(settings, "risk_pct"), "risk_pct", min_value=0.0
        )
        if base_risk_pct <= 0 or base_risk_pct > 1.0:
            raise ValueError(f"risk_pct out of range (0,1]: {base_risk_pct}")

        base_tp_pct = _as_float(getattr(settings, "tp_pct"), "tp_pct", min_value=0.0)
        base_sl_pct = _as_float(getattr(settings, "sl_pct"), "sl_pct", min_value=0.0)
        if base_tp_pct <= 0 or base_tp_pct > 1.0:
            raise ValueError(f"tp_pct out of range (0,1]: {base_tp_pct}")
        if base_sl_pct <= 0 or base_sl_pct > 1.0:
            raise ValueError(f"sl_pct out of range (0,1]: {base_sl_pct}")

        # extra 우선 적용
        tp_pct = base_tp_pct
        sl_pct = base_sl_pct
        effective_risk_pct = base_risk_pct

        if isinstance(extra, dict):
            if extra.get("tp_pct") is not None:
                tp_pct = _as_float(extra.get("tp_pct"), "extra.tp_pct", min_value=0.0)
            if extra.get("sl_pct") is not None:
                sl_pct = _as_float(extra.get("sl_pct"), "extra.sl_pct", min_value=0.0)
            if extra.get("effective_risk_pct") is not None:
                effective_risk_pct = _as_float(
                    extra.get("effective_risk_pct"),
                    "extra.effective_risk_pct",
                    min_value=0.0,
                )

        if effective_risk_pct <= 0 or effective_risk_pct > 1.0:
            raise ValueError(
                f"effective_risk_pct out of range (0,1]: {effective_risk_pct}"
            )

        # 5) GPT 진입 의사결정
        gpt_model_for_log = os.getenv("GPT_ENTRY_MODEL", DEFAULT_GPT_ENTRY_MODEL)
        pre_entry_score = _compute_pre_entry_score(extra)

        guard_snapshot = _build_guard_snapshot(settings)
        extra_for_gpt = dict(extra) if isinstance(extra, dict) else None

        try:
            gpt_result = decide_entry_with_gpt_trader(
                settings,
                symbol=symbol,
                signal_source=signal_source_s,
                direction=chosen_signal,
                last_price=float(last_price),
                entry_score=pre_entry_score,
                base_risk_pct=float(effective_risk_pct),
                base_tp_pct=float(tp_pct),
                base_sl_pct=float(sl_pct),
                extra=extra_for_gpt,
                guard_snapshot=guard_snapshot or None,
            )
        except Exception as e:
            msg = f"[GPT_ENTRY][ERROR] gpt_trader exception -> skip: {e}"
            _safe_send_skip(msg)
            logger.error(msg)
            log_skip_event(
                symbol=symbol,
                regime=regime_label,
                source="entry_engine.gpt",
                side=chosen_signal,
                reason="gpt_trader_exception",
                extra={"error": str(e)},
            )
            return None, _SLEEP_GPT_ERROR

        final_action = str(gpt_result.get("final_action", "SKIP")).upper()
        gpt_action = str(gpt_result.get("gpt_action", "")).upper()
        gpt_reason = gpt_result.get("reason")
        gpt_reason_s = str(gpt_reason) if isinstance(gpt_reason, str) else ""

        if final_action != "ENTER":
            try:
                log_event(
                    "on_gpt_reject",
                    symbol=symbol,
                    regime=regime_label,
                    source="entry_engine.gpt",
                    side=chosen_signal,
                    price=float(last_price) if _is_finite_pos(last_price) else None,
                    reason=gpt_reason_s or f"gpt_action={gpt_action or 'NONE'}",
                    extra_json={"gpt_result": _safe_json(gpt_result)},
                )
            except Exception as e:  # pragma: no cover
                logger.debug("log_event(on_gpt_reject) failed: %s", e)

            msg = f"[GPT_ENTRY][SKIP] {gpt_reason_s or 'gpt_reject'}"
            _safe_send_skip(msg)
            logger.info(msg)
            log_skip_event(
                symbol=symbol,
                regime=regime_label,
                source="entry_engine.gpt",
                side=chosen_signal,
                reason=f"gpt_reject_{gpt_action or 'NONE'}",
                extra={"gpt_result": _safe_json(gpt_result)},
            )
            return None, _SLEEP_GPT_SKIP

        try:
            log_event(
                "on_gpt_approve",
                symbol=symbol,
                regime=regime_label,
                source="entry_engine.gpt",
                side=chosen_signal,
                price=float(last_price) if _is_finite_pos(last_price) else None,
                reason=gpt_reason_s or "gpt_approved",
                extra_json={"gpt_result": _safe_json(gpt_result)},
            )
        except Exception as e:  # pragma: no cover
            logger.debug("log_event(on_gpt_approve) failed: %s", e)

        effective_risk_pct = _as_float(
            gpt_result.get("effective_risk_pct", effective_risk_pct),
            "gpt.effective_risk_pct",
            min_value=0.0,
        )
        tp_pct = _as_float(
            gpt_result.get("tp_pct", tp_pct), "gpt.tp_pct", min_value=0.0
        )
        sl_pct = _as_float(
            gpt_result.get("sl_pct", sl_pct), "gpt.sl_pct", min_value=0.0
        )

        if effective_risk_pct <= 0 or effective_risk_pct > 1.0:
            raise ValueError(
                f"gpt effective_risk_pct out of range (0,1]: {effective_risk_pct}"
            )

        guard_adjustments: Dict[str, float] = {}
        raw_ga = gpt_result.get("guard_adjustments")
        if isinstance(raw_ga, dict):
            for k, v in raw_ga.items():
                try:
                    guard_adjustments[str(k)] = float(v)
                except Exception:
                    continue

        _safe_send_tg(
            f"[GPT_ENTRY][ENTER] {symbol} {chosen_signal} source={signal_source_s} "
            f"risk={effective_risk_pct*100:.2f}% tp={tp_pct*100:.2f}% sl={sl_pct*100:.2f}% "
            f"model={gpt_model_for_log} reason={gpt_reason_s or 'n/a'}"
        )

        log_gpt_entry_event(
            symbol=symbol,
            regime=regime_label,
            side=chosen_signal,
            action=gpt_action,
            tp_pct=tp_pct,
            sl_pct=sl_pct,
            risk_pct=effective_risk_pct,
            gpt_json={
                "model": gpt_model_for_log,
                "reason": gpt_reason_s,
                "decision": _safe_json(gpt_result.get("raw") or {}),
                "guard_adjustments": guard_adjustments,
            },
        )

        best_bid: Optional[float] = None
        best_ask: Optional[float] = None

        original_guard_values: Dict[str, Any] = {}
        try:
            for key, val in guard_adjustments.items():
                if hasattr(settings, key):
                    original_guard_values[key] = getattr(settings, key)
                    try:
                        setattr(settings, key, val)
                    except Exception:
                        pass

            vol_ok = check_volume_guard(
                settings=settings,
                candles_5m_raw=candles_5m_raw,
                latest_ts=float(latest_ts),
                signal_source=signal_source_s,
                direction=chosen_signal,
            )
            if not vol_ok:
                msg = "[SKIP] volume_guard_blocked"
                _safe_send_skip(msg)
                logger.info(msg)
                log_skip_event(
                    symbol=symbol,
                    regime=regime_label,
                    source="entry_engine.guard.volume",
                    side=chosen_signal,
                    reason="volume_guard_blocked",
                    extra={"latest_ts": int(latest_ts)},
                )
                return None, _SLEEP_DEFAULT

            price_ok = check_price_jump_guard(
                settings=settings,
                candles_5m=candles_5m,
                latest_ts=float(latest_ts),
                signal_source=signal_source_s,
                direction=chosen_signal,
            )
            if not price_ok:
                msg = "[SKIP] price_jump_guard_blocked"
                _safe_send_skip(msg)
                logger.info(msg)
                log_skip_event(
                    symbol=symbol,
                    regime=regime_label,
                    source="entry_engine.guard.price_jump",
                    side=chosen_signal,
                    reason="price_jump_guard_blocked",
                    extra={"latest_ts": int(latest_ts)},
                )
                return None, _SLEEP_DEFAULT

            spread_ok, best_bid, best_ask = check_spread_guard(
                settings=settings,
                symbol=symbol,
                latest_ts=float(latest_ts),
                signal_source=signal_source_s,
                direction=chosen_signal,
            )
            if not spread_ok:
                msg = "[SKIP] spread_guard_blocked"
                _safe_send_skip(msg)
                logger.info(msg)
                log_skip_event(
                    symbol=symbol,
                    regime=regime_label,
                    source="entry_engine.guard.spread",
                    side=chosen_signal,
                    reason="spread_guard_blocked",
                    extra={
                        "latest_ts": int(latest_ts),
                        "best_bid": best_bid,
                        "best_ask": best_ask,
                    },
                )
                return None, _SLEEP_DEFAULT
        finally:
            for key, val in original_guard_values.items():
                try:
                    setattr(settings, key, val)
                except Exception:
                    pass

        last_price_f = _as_float(last_price, "last_price", min_value=0.0)
        if last_price_f <= 0:
            raise ValueError(f"invalid last_price: {last_price_f}")

        price_for_qty = last_price_f
        entry_price_hint = last_price_f

        if (
            best_bid is not None
            and best_ask is not None
            and best_bid > 0
            and best_ask > 0
        ):
            mid = (float(best_bid) + float(best_ask)) / 2.0
            if mid > 0:
                price_for_qty = mid
            entry_price_hint = (
                float(best_ask) if chosen_signal == "LONG" else float(best_bid)
            )

        slippage_block_pct = _as_float(
            getattr(settings, "slippage_block_pct"),
            "slippage_block_pct",
            min_value=0.0,
        )
        slippage_stop_engine = bool(getattr(settings, "slippage_stop_engine"))

        if slippage_block_pct > 0:
            slippage_pct = abs(entry_price_hint - last_price_f) / last_price_f
            if slippage_pct > slippage_block_pct:
                reason = (
                    f"slippage_block: slippage_pct={slippage_pct*100:.3f}% "
                    f"> block={slippage_block_pct*100:.3f}%"
                )
                _safe_send_skip(f"[SKIP] {reason}")
                logger.warning("[SLIPPAGE_BLOCK] %s", reason)
                try:
                    log_event(
                        "on_slippage_block",
                        symbol=symbol,
                        regime=regime_label,
                        source="entry_engine.slippage",
                        side=chosen_signal,
                        price=entry_price_hint,
                        reason="slippage_block",
                        extra_json={
                            "slippage_pct": slippage_pct,
                            "slippage_block_pct": slippage_block_pct,
                            "last_price": last_price_f,
                            "entry_price_hint": entry_price_hint,
                        },
                    )
                except Exception as e:  # pragma: no cover
                    logger.debug("log_event(on_slippage_block) failed: %s", e)

                log_skip_event(
                    symbol=symbol,
                    regime=regime_label,
                    source="entry_engine.slippage",
                    side=chosen_signal,
                    reason="slippage_block",
                    extra={
                        "slippage_pct": slippage_pct,
                        "slippage_block_pct": slippage_block_pct,
                    },
                )

                if slippage_stop_engine:
                    raise RuntimeError(
                        "slippage_block triggered and slippage_stop_engine=True"
                    )

                return None, _SLEEP_GPT_SKIP

        notional = avail_usdt * float(effective_risk_pct) * float(leverage)
        if not math.isfinite(notional) or notional <= 0:
            raise ValueError(f"invalid notional: {notional}")

        if not math.isfinite(price_for_qty) or price_for_qty <= 0:
            raise ValueError(f"invalid price_for_qty: {price_for_qty}")

        qty_raw = notional / price_for_qty
        if not math.isfinite(qty_raw) or qty_raw <= 0:
            raise ValueError(f"invalid qty_raw: {qty_raw}")

        ok, guard_reason, guard_extra = hard_risk_guard_check(
            settings,
            symbol=symbol,
            side=chosen_signal,
            entry_price=float(entry_price_hint),
            notional=float(notional),
            available_usdt=float(avail_usdt),
        )
        if not ok:
            msg = f"[RISK_GUARD] blocked: {guard_reason}"
            _safe_send_skip(msg)
            logger.warning("%s extra=%s", msg, guard_extra)
            try:
                log_event(
                    "on_risk_guard_trigger",
                    symbol=symbol,
                    regime=regime_label,
                    source="entry_engine.risk_guard",
                    side=chosen_signal,
                    price=float(entry_price_hint),
                    reason=guard_reason,
                    extra_json=guard_extra,
                )
            except Exception as e:  # pragma: no cover
                logger.debug("log_event(on_risk_guard_trigger) failed: %s", e)

            log_skip_event(
                symbol=symbol,
                regime=regime_label,
                source="entry_engine.risk_guard",
                side=chosen_signal,
                reason=guard_reason,
                extra=guard_extra,
            )
            return None, _SLEEP_RISK_GUARD

        side_open = "BUY" if chosen_signal == "LONG" else "SELL"

        soft_mode = False
        sl_floor_ratio: Optional[float] = None
        if isinstance(extra, dict):
            soft_mode = bool(extra.get("soft_mode") or extra.get("soft") or False)
            if extra.get("sl_floor_ratio") is not None:
                try:
                    sl_floor_ratio = float(extra.get("sl_floor_ratio"))
                except Exception:
                    sl_floor_ratio = None

        trade = execute_entry(
            settings,
            symbol=symbol,
            side_open=side_open,
            qty=qty_raw,
            entry_price_hint=float(entry_price_hint),
            tp_pct=float(tp_pct),
            sl_pct=float(sl_pct),
            source=signal_source_s,
            regime=regime_label,
            signal_ts_ms=int(latest_ts),
            risk_pct=float(effective_risk_pct),
            leverage=float(leverage),
            soft_mode=soft_mode,
            sl_floor_ratio=sl_floor_ratio,
        )

        return trade, _SLEEP_POST_ENTRY

    except (ValueError, RuntimeError):
        raise
    except Exception as e:
        raise RuntimeError(f"unexpected entry_engine error: {e}") from e


# ─────────────────────────────────────────────────────────────────────────────
# hard_risk_guard_check()
# ─────────────────────────────────────────────────────────────────────────────


def hard_risk_guard_check(
    settings: Any,
    *,
    symbol: str,
    side: str,
    entry_price: float,
    notional: float,
    available_usdt: float,
) -> Tuple[bool, str, Dict[str, Any]]:
    """문서 기준 하드 리스크 가드.

    Checks:
    - 일일 손실 한도
    - 연속 손실 한도
    - 계좌 대비 포지션 가치 상한
    - 청산거리 최소 % (현재 포지션이 있는 경우에만 liquidationPrice 기반 체크)

    Returns:
        (ok, reason, extra)
    """

    _validate_hard_risk_settings(settings)

    if not math.isfinite(entry_price) or entry_price <= 0:
        raise ValueError(f"invalid entry_price: {entry_price}")
    if not math.isfinite(notional) or notional <= 0:
        raise ValueError(f"invalid notional: {notional}")
    if not math.isfinite(available_usdt) or available_usdt <= 0:
        raise ValueError(f"invalid available_usdt: {available_usdt}")

    extra: Dict[str, Any] = {
        "symbol": symbol,
        "side": side,
        "entry_price": entry_price,
        "notional": notional,
        "available_usdt": available_usdt,
    }

    # [2-1] 일일 손실 한도
    daily_limit = float(getattr(settings, "hard_daily_loss_limit_usdt"))
    if daily_limit > 0:
        pnl_today = _get_today_realized_pnl_usdt(symbol)
        extra["pnl_today_usdt"] = pnl_today
        extra["hard_daily_loss_limit_usdt"] = daily_limit
        if pnl_today <= -daily_limit:
            return False, "hard_daily_loss_limit_exceeded", extra

    # [2-2] 연속 손실 한도
    consec_limit = int(getattr(settings, "hard_consecutive_losses_limit"))
    if consec_limit > 0:
        consec_losses = _get_consecutive_losses(symbol)
        extra["consecutive_losses"] = consec_losses
        extra["hard_consecutive_losses_limit"] = consec_limit
        if consec_losses >= consec_limit:
            return False, "hard_consecutive_losses_limit_exceeded", extra

    # [2-3] 계좌 대비 포지션 가치 상한
    pct_cap = float(getattr(settings, "hard_position_value_pct_cap"))
    # 0이면 "포지션 금지"로 동작 (명시적)
    max_notional = available_usdt * (pct_cap / 100.0)
    extra["hard_position_value_pct_cap"] = pct_cap
    extra["max_notional_by_cap"] = max_notional
    if notional > max_notional:
        return False, "hard_position_value_pct_cap_exceeded", extra

    # [2-4] 청산거리 최소 %
    liq_min_pct = float(getattr(settings, "hard_liquidation_distance_pct_min"))
    if liq_min_pct > 0:
        if get_position is None:
            raise RuntimeError("exchange_api.get_position is required for liquidation guard")

        try:
            pos = get_position(symbol)  # type: ignore[misc]
        except Exception as e:
            raise RuntimeError(f"get_position failed: {e}") from e
        if not isinstance(pos, dict):
            raise RuntimeError("get_position() returned non-dict")

        # Binance positionRisk 필드 기반
        pos_amt = _safe_float_from_dict(pos, "positionAmt")
        liq_price = _safe_float_from_dict(pos, "liquidationPrice")

        extra["positionAmt"] = pos_amt
        extra["liquidationPrice"] = liq_price
        extra["hard_liquidation_distance_pct_min"] = liq_min_pct

        # 포지션이 없는 경우(0)에는 liquidation guard를 적용할 수 없다.
        # 신규 진입 자체는 허용한다.
        if pos_amt is not None and abs(pos_amt) > 0:
            if liq_price is None or liq_price <= 0:
                raise RuntimeError("liquidationPrice unavailable while positionAmt != 0")

            dist_pct = abs(entry_price - liq_price) / entry_price * 100.0
            extra["liquidation_distance_pct"] = dist_pct
            if dist_pct < liq_min_pct:
                return False, "hard_liquidation_distance_too_close", extra

    return True, "OK", extra


# ─────────────────────────────────────────────────────────────────────────────
# execute_entry()
# ─────────────────────────────────────────────────────────────────────────────


def execute_entry(
    settings: Any,
    *,
    symbol: str,
    side_open: str,
    qty: float,
    entry_price_hint: float,
    tp_pct: float,
    sl_pct: float,
    source: str,
    regime: str,
    signal_ts_ms: int,
    risk_pct: float,
    leverage: float,
    soft_mode: bool,
    sl_floor_ratio: Optional[float],
) -> Trade:
    """order_executor에 주문 실행을 위임한다.

    - on_entry_submitted / on_entry_filled 이벤트를 남긴다.
    - 체결 후 bt_trades에 INSERT 하고 trade.db_id로 연결한다.

    Raises:
        RuntimeError/ValueError
    """

    side_open_u = str(side_open).upper()
    if side_open_u not in ("BUY", "SELL"):
        raise ValueError(f"invalid side_open: {side_open}")

    if not math.isfinite(qty) or qty <= 0:
        raise ValueError(f"invalid qty: {qty}")

    if not math.isfinite(entry_price_hint) or entry_price_hint <= 0:
        raise ValueError(f"invalid entry_price_hint: {entry_price_hint}")

    # 이벤트: 제출
    try:
        log_event(
            "on_entry_submitted",
            symbol=symbol,
            regime=regime,
            source="entry_engine.execute_entry",
            side=side_open_u,
            price=entry_price_hint,
            qty=qty,
            leverage=leverage,
            tp_pct=tp_pct,
            sl_pct=sl_pct,
            risk_pct=risk_pct,
            reason="entry_submit",
            extra_json={"source": source},
        )
    except Exception as e:  # pragma: no cover
        logger.debug("log_event(on_entry_submitted) failed: %s", e)

    # signals CSV (분석용)
    try:
        log_signal(
            event="ENTRY_SIGNAL",
            symbol=symbol,
            strategy_type=source,
            direction="LONG" if side_open_u == "BUY" else "SHORT",
            price=float(entry_price_hint),
            qty=float(qty),
            reason="gpt_approved",
            candle_ts=int(signal_ts_ms),
            available_usdt=float(get_available_usdt()),
            notional=float(get_available_usdt()) * float(risk_pct) * float(leverage),
        )
    except Exception as e:  # pragma: no cover
        logger.debug("log_signal(ENTRY_SIGNAL) failed: %s", e)

    # 실제 주문 실행
    try:
        trade = open_position_with_tp_sl(
            settings=settings,
            symbol=symbol,
            side_open=side_open_u,
            qty=float(qty),
            entry_price_hint=float(entry_price_hint),
            tp_pct=float(tp_pct),
            sl_pct=float(sl_pct),
            source=str(source),
            soft_mode=bool(soft_mode),
            sl_floor_ratio=sl_floor_ratio,
        )
    except Exception as e:
        raise RuntimeError(f"open_position_with_tp_sl failed: {e}") from e

    if trade is None:
        raise RuntimeError("open_position_with_tp_sl returned None")

    # 이벤트: 체결(엔트리 완료)
    try:
        log_event(
            "on_entry_filled",
            symbol=symbol,
            regime=regime,
            source="entry_engine.execute_entry",
            side=side_open_u,
            price=float(getattr(trade, "entry", getattr(trade, "entry_price", 0.0))),
            qty=float(getattr(trade, "qty", qty)),
            leverage=float(getattr(trade, "leverage", leverage)),
            tp_pct=tp_pct,
            sl_pct=sl_pct,
            risk_pct=risk_pct,
            reason="entry_filled",
            extra_json={"source": source},
        )
    except Exception as e:  # pragma: no cover
        logger.debug("log_event(on_entry_filled) failed: %s", e)

    # bt_trades INSERT
    try:
        _insert_trade_row(
            trade=trade,
            symbol=symbol,
            ts_ms=int(signal_ts_ms),
            regime_at_entry=str(regime),
            strategy=str(source),
            risk_pct=float(risk_pct),
            tp_pct=float(tp_pct),
            sl_pct=float(sl_pct),
            leverage=float(getattr(trade, "leverage", leverage)),
        )
    except Exception as e:  # pragma: no cover
        # DB 기록 실패는 실거래 자체를 되돌릴 수 없으므로, 예외를 올려 운영자가 즉시 인지하게 한다.
        raise RuntimeError(f"bt_trades insert failed: {e}") from e

    return trade


# ─────────────────────────────────────────────────────────────────────────────
# utility functions
# ─────────────────────────────────────────────────────────────────────────────


def _safe_send_tg(text: str) -> None:
    try:
        send_tg(text)
    except Exception as e:  # pragma: no cover
        logger.debug("send_tg failed: %s", e)


def _safe_send_skip(text: str) -> None:
    try:
        send_skip_tg(text)
    except Exception as e:  # pragma: no cover
        logger.debug("send_skip_tg failed: %s", e)


def _validate_settings_contract(settings: Any) -> None:
    """entry_engine이 기대하는 최소 Settings 필드 계약을 검증한다."""

    required_fields = [
        "symbol",
        "leverage",
        "risk_pct",
        "tp_pct",
        "sl_pct",
        "hard_daily_loss_limit_usdt",
        "hard_consecutive_losses_limit",
        "hard_position_value_pct_cap",
        "hard_liquidation_distance_pct_min",
        "slippage_block_pct",
        "slippage_stop_engine",
    ]
    missing = [f for f in required_fields if not hasattr(settings, f)]
    if missing:
        raise ValueError(f"settings missing required fields: {missing}")


def _validate_hard_risk_settings(settings: Any) -> None:
    daily = _as_float(
        getattr(settings, "hard_daily_loss_limit_usdt"),
        "hard_daily_loss_limit_usdt",
        min_value=0.0,
    )
    consec = int(getattr(settings, "hard_consecutive_losses_limit"))
    pct_cap = _as_float(
        getattr(settings, "hard_position_value_pct_cap"),
        "hard_position_value_pct_cap",
        min_value=0.0,
    )
    liq = _as_float(
        getattr(settings, "hard_liquidation_distance_pct_min"),
        "hard_liquidation_distance_pct_min",
        min_value=0.0,
    )

    if consec < 0:
        raise ValueError("hard_consecutive_losses_limit must be >= 0")
    if not (0.0 <= pct_cap <= 100.0):
        raise ValueError("hard_position_value_pct_cap must be in [0,100]")
    if not (0.0 <= liq <= 100.0):
        raise ValueError("hard_liquidation_distance_pct_min must be in [0,100]")
    if daily < 0:
        raise ValueError("hard_daily_loss_limit_usdt must be >= 0")


def _build_guard_snapshot(settings: Any) -> Dict[str, float]:
    snap: Dict[str, float] = {}
    for key in [
        "min_entry_volume_ratio",
        "max_spread_pct",
        "max_price_jump_pct",
        "depth_imbalance_min_ratio",
        "depth_imbalance_min_notional",
    ]:
        if hasattr(settings, key):
            val = getattr(settings, key)
            if isinstance(val, (int, float)) and math.isfinite(float(val)):
                snap[key] = float(val)
    return snap


def _compute_pre_entry_score(extra: Any) -> Optional[float]:
    """EntryScore(0~100) 프리뷰. 필요한 값이 없으면 None.

    STRICT:
    - 값을 추정하지 않는다.
    """
    if not isinstance(extra, dict):
        return None

    raw = extra.get("signal_score")
    if raw is None:
        raw = extra.get("candidate_score")

    if not isinstance(raw, (int, float)):
        return None

    if not math.isfinite(float(raw)):
        return None

    # 기존 엔진 관행: 0~10 스케일 점수면 0~100으로 변환
    v = float(raw)
    if 0.0 <= v <= 10.0:
        return v * 10.0

    # 이미 0~100이면 그대로
    if 0.0 <= v <= 100.0:
        return v

    return None


def _pick_latest_candle(
    *,
    signal_candles_raw: Optional[List[List[float]]],
    signal_candles: Optional[List[List[float]]],
) -> Tuple[int, float, float, float, float, float]:
    """캔들 스냅샷용 (ts, o, h, l, c, v) 선택.

    우선순위:
    1) signal_candles_raw (거래량 포함)
    2) signal_candles (거래량 없으면 v=0)
    3) 더미(현재시각)
    """

    if signal_candles_raw and len(signal_candles_raw[-1]) >= 6:
        r = signal_candles_raw[-1]
        return int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])

    if signal_candles and len(signal_candles[-1]) >= 5:
        c = signal_candles[-1]
        return int(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4]), 0.0

    now_ms = int(time.time() * 1000)
    return now_ms, 0.0, 0.0, 0.0, 0.0, 0.0


def _as_float(
    value: Any,
    name: str,
    *,
    min_value: Optional[float] = None,
) -> float:
    """값을 float로 안전 변환. 실패 시 ValueError."""

    try:
        if isinstance(value, bool):
            raise TypeError("bool is not allowed")
        v = float(value)
    except Exception as e:
        raise ValueError(f"{name} must be a number") from e

    if not math.isfinite(v):
        raise ValueError(f"{name} must be finite")

    if min_value is not None and v < min_value:
        raise ValueError(f"{name} must be >= {min_value}")

    return v


def _is_finite_pos(value: Any) -> bool:
    try:
        v = float(value)
    except Exception:
        return False
    return math.isfinite(v) and v > 0


def _safe_float_from_dict(d: Dict[str, Any], key: str) -> Optional[float]:
    if key not in d:
        return None
    val = d.get(key)
    if val is None:
        return None
    try:
        f = float(val)
    except Exception:
        return None
    if not math.isfinite(f):
        return None
    return f


def _safe_json(obj: Any) -> Any:
    """이벤트 로그에 넣기 위해 dict/list/기본형만 유지."""
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    if isinstance(obj, dict):
        return {str(k): _safe_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_safe_json(v) for v in obj]
    return str(obj)


def _kst_day_start_utc() -> datetime:
    now_kst = datetime.now(tz=KST)
    start_kst = datetime(now_kst.year, now_kst.month, now_kst.day, tzinfo=KST)
    return start_kst.astimezone(timezone.utc)


def _get_today_realized_pnl_usdt(symbol: str) -> float:
    """오늘(KST) 기준 실현 PnL 합계를 반환.

    - bt_trades.exit_ts가 오늘(KST) 날짜에 속하는 레코드만 합산
    - pnl_usdt가 NULL인 레코드는 제외
    """

    start_utc = _kst_day_start_utc()
    end_utc = start_utc + timedelta(days=1)

    try:
        session = SessionLocal()
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"DB SessionLocal create failed: {e}") from e

    try:
        stmt = (
            select(func.coalesce(func.sum(TradeORM.pnl_usdt), 0))
            .where(TradeORM.symbol == symbol)
            .where(TradeORM.exit_ts.isnot(None))
            .where(TradeORM.pnl_usdt.isnot(None))
            .where(TradeORM.exit_ts >= start_utc)
            .where(TradeORM.exit_ts < end_utc)
        )
        result = session.execute(stmt).scalar_one()
        try:
            return float(result)
        except Exception:
            return 0.0
    except Exception as e:
        raise RuntimeError(f"daily pnl query failed: {e}") from e
    finally:
        session.close()


def _get_consecutive_losses(symbol: str, *, lookback: int = 50) -> int:
    """최근 종료 트레이드 기준 연속 손실 횟수."""

    if lookback <= 0:
        raise ValueError("lookback must be > 0")

    try:
        session = SessionLocal()
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"DB SessionLocal create failed: {e}") from e

    try:
        stmt = (
            select(TradeORM.pnl_usdt)
            .where(TradeORM.symbol == symbol)
            .where(TradeORM.exit_ts.isnot(None))
            .where(TradeORM.pnl_usdt.isnot(None))
            .order_by(TradeORM.exit_ts.desc())
            .limit(int(lookback))
        )
        rows = session.execute(stmt).all()
        cnt = 0
        for (pnl_val,) in rows:
            try:
                pnl = float(pnl_val)
            except Exception:
                break
            if pnl < 0:
                cnt += 1
            else:
                break
        return cnt
    except Exception as e:
        raise RuntimeError(f"consecutive losses query failed: {e}") from e
    finally:
        session.close()


def _insert_trade_row(
    *,
    trade: Trade,
    symbol: str,
    ts_ms: int,
    regime_at_entry: str,
    strategy: str,
    risk_pct: float,
    tp_pct: float,
    sl_pct: float,
    leverage: float,
) -> None:
    """bt_trades INSERT 후 trade.db_id로 연결."""

    entry_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

    # Trade 객체에서 값 추출 (구현체별 호환)
    side = str(getattr(trade, "side", "")).upper()
    entry_price = getattr(trade, "entry", None)
    if entry_price is None:
        entry_price = getattr(trade, "entry_price", None)

    qty = getattr(trade, "qty", None)

    entry_price_f = _as_float(entry_price, "trade.entry_price", min_value=0.0)
    qty_f = _as_float(qty, "trade.qty", min_value=0.0)

    orm = TradeORM(
        symbol=str(symbol),
        side=side,
        entry_ts=entry_dt,
        entry_price=entry_price_f,
        qty=qty_f,
        is_auto=True,
        regime_at_entry=str(regime_at_entry),
        strategy=str(strategy),
        leverage=float(leverage),
        risk_pct=float(risk_pct),
        tp_pct=float(tp_pct),
        sl_pct=float(sl_pct),
        note="",
        created_at=entry_dt,
        updated_at=entry_dt,
    )

    try:
        session = SessionLocal()
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"DB SessionLocal create failed: {e}") from e

    try:
        session.add(orm)
        session.commit()
        trade_id = getattr(orm, "id", None)
        if trade_id is not None:
            try:
                setattr(trade, "db_id", int(trade_id))
            except Exception:
                pass
    except Exception as e:
        session.rollback()
        raise RuntimeError(f"bt_trades insert failed: {e}") from e
    finally:
        session.close()


__all__ = [
    "try_open_new_position",
    "hard_risk_guard_check",
    "execute_entry",
]