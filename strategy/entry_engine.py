# ========================================================
# FILE: strategy/entry_engine.py
# STRICT · NO-FALLBACK · PRODUCTION MODE
# ========================================================
# 역할
# - WS 시그널 → (필수 데이터 스냅샷) → Supervisor 감사(GPT) → 가드 → 하드리스크 → 주문 실행
#
# 핵심 원칙
# - 결정권은 수학 엔진(규칙)이다. GPT는 감사/해설만 수행한다(결정/사이징/TP·SL 금지).
# - 데이터 누락/불일치/모호성은 즉시 예외 또는 명시적 SKIP 처리한다(추정/0치환 금지).
# - 설정(settings) runtime mutate 금지.
# - 민감정보(키/시크릿/주문ID 등) 로그/텔레그램/이벤트에 출력 금지.
#
# 변경 이력
# --------------------------------------------------------
# - 2026-03-03:
#   1) GPT 진입 판단(gpt_trader) 제거 준비: gpt_supervisor(감사/해설)로 교체
#   2) unified_features_builder(마이크로구조 포함) 스냅샷을 Supervisor 입력으로 사용
#   3) settings 변경(guard_adjustments) 방식 제거: settings mutate 금지
#   4) import fallback(플랫 구조 지원) 제거: 패키지 구조 고정(STRICT)
# ========================================================

from __future__ import annotations

import logging
import math
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from settings import load_settings
from infra.data_health_monitor import HEALTH_OK, LAST_FAIL_REASON
from infra.telelog import send_skip_tg, send_tg

from infra.market_features_ws import get_trading_signal
from strategy.unified_features_builder import UnifiedFeaturesError, build_unified_features
from strategy.gpt_supervisor import GptSupervisorError, run_gpt_supervisor

from risk.entry_guards_ws import (
    check_manual_position_guard,
    check_price_jump_guard,
    check_spread_guard,
    check_volume_guard,
)

from execution.exchange_api import get_available_usdt, get_balance_detail, get_position
from execution.order_executor import open_position_with_tp_sl

from events.signals_logger import (
    log_candle_snapshot,
    log_event,
    log_gpt_entry_event,
    log_signal,
    log_skip_event,
)

from state.db_core import SessionLocal
from state.db_models import Trade as TradeORM
from sqlalchemy import func, select

from state.trader_state import Trade

logger = logging.getLogger(__name__)
SET = load_settings()

KST = timezone(timedelta(hours=9))

_SLEEP_DEFAULT = 1.0
_SLEEP_GUARD = 2.0
_SLEEP_BALANCE = 5.0
_SLEEP_GPT_ERROR = 3.0
_SLEEP_GPT_SKIP = 3.0
_SLEEP_RISK_GUARD = 5.0
_SLEEP_POST_ENTRY = 5.0


def try_open_new_position(settings: Any, last_close_ts: float) -> Tuple[Optional[Trade], float]:
    """
    WS 시그널 → Supervisor 감사(GPT) → 가드 → 하드 리스크 가드 → 주문 실행.

    Returns:
        (Trade, sleep_sec) if entry opened, else (None, sleep_sec).

    STRICT:
    - 수량/틱 라운딩은 하지 않는다(order_executor에 위임).
    - 잘못된 설정/입력은 예외.
    - 데이터 부족/감사 실패 시 '명시적 SKIP' (추정/폴백 진입 금지).
    """
    _validate_settings_contract(settings)

    # 0) 데이터 헬스 FAIL 시 신규 진입 차단(명시적 SKIP)
    if not HEALTH_OK:
        msg = f"[SKIP][DATA_HEALTH_FAIL] entry blocked: {str(LAST_FAIL_REASON)}"
        _safe_send_skip(msg)
        logger.warning(msg)
        _safe_log_event(
            "on_risk_guard_trigger",
            symbol=str(settings.symbol),
            regime="NO_TRADE",
            source="entry_engine.data_health",
            side="",
            reason="data_health_fail",
            extra_json={"detail": str(LAST_FAIL_REASON)},
        )
        return None, _SLEEP_DEFAULT

    symbol = _require_nonempty_str(str(getattr(settings, "symbol", "")).strip(), "settings.symbol")

    # 1) WS 시그널 받기
    signal_ctx = get_trading_signal(settings=settings, last_close_ts=last_close_ts, symbol=symbol)
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

    chosen_signal_u = _require_nonempty_str(str(chosen_signal).upper().strip(), "chosen_signal")
    if chosen_signal_u not in ("LONG", "SHORT"):
        raise ValueError(f"invalid chosen_signal: {chosen_signal_u!r}")

    signal_source_s = _require_nonempty_str(str(signal_source).strip(), "signal_source")
    regime_label = signal_source_s

    last_price_f = _as_float(last_price, "last_price", min_value=0.0)
    if last_price_f <= 0:
        raise ValueError(f"invalid last_price: {last_price_f}")

    # 이벤트: 시그널 후보
    _safe_log_event(
        "on_signal_candidate",
        symbol=symbol,
        regime=regime_label,
        source="entry_engine.signal",
        side=chosen_signal_u,
        price=last_price_f,
        reason="signal_candidate",
        extra_json={"latest_ts": int(latest_ts)},
    )

    # 캔들 스냅샷 (STRICT: 없으면 스냅샷 생략이 아니라 예외로 처리하지는 않음. 스냅샷은 부가)
    _safe_log_candle_snapshot(symbol, signal_source_s, chosen_signal_u, latest_ts, candles_5m, candles_5m_raw)

    # 2) 수동/외부 가드 (Supervisor 이전)
    manual_ok = check_manual_position_guard(
        get_balance_detail_func=get_balance_detail,
        symbol=symbol,
        latest_ts=float(latest_ts),
    )
    if not manual_ok:
        msg = "[SKIP] manual_guard_blocked"
        _safe_send_skip(msg)
        logger.info(msg)
        log_skip_event(
            symbol=symbol,
            regime=regime_label,
            source="entry_engine.guard.manual",
            side=chosen_signal_u,
            reason="manual_guard_blocked",
            extra={"latest_ts": int(latest_ts)},
        )
        return None, _SLEEP_GUARD

    # 3) 가용 잔고 확인
    avail_usdt = _as_float(get_available_usdt(), "available_usdt", min_value=0.0)
    if avail_usdt <= 0:
        msg = f"[SKIP] available_usdt<=0: available_usdt={avail_usdt}"
        _safe_send_skip(msg)
        logger.warning(msg)
        log_skip_event(
            symbol=symbol,
            regime=regime_label,
            source="entry_engine.balance",
            side=chosen_signal_u,
            reason="balance_zero_or_invalid",
            extra={"available_usdt": avail_usdt},
        )
        return None, _SLEEP_BALANCE

    # 4) 기본 주문 파라미터(Quant pre)
    leverage = _as_float(getattr(settings, "leverage"), "leverage", min_value=1.0)
    base_risk_pct = _as_float(getattr(settings, "risk_pct"), "risk_pct", min_value=0.0)
    base_tp_pct = _as_float(getattr(settings, "tp_pct"), "tp_pct", min_value=0.0)
    base_sl_pct = _as_float(getattr(settings, "sl_pct"), "sl_pct", min_value=0.0)

    _require_ratio_0_1(base_risk_pct, "risk_pct")
    _require_ratio_0_1(base_tp_pct, "tp_pct")
    _require_ratio_0_1(base_sl_pct, "sl_pct")

    tp_pct = base_tp_pct
    sl_pct = base_sl_pct
    effective_risk_pct = base_risk_pct

    if isinstance(extra, dict):
        if extra.get("tp_pct") is not None:
            tp_pct = _as_float(extra.get("tp_pct"), "extra.tp_pct", min_value=0.0)
        if extra.get("sl_pct") is not None:
            sl_pct = _as_float(extra.get("sl_pct"), "extra.sl_pct", min_value=0.0)
        if extra.get("effective_risk_pct") is not None:
            effective_risk_pct = _as_float(extra.get("effective_risk_pct"), "extra.effective_risk_pct", min_value=0.0)

    _require_ratio_0_1(effective_risk_pct, "effective_risk_pct")
    _require_ratio_0_1(tp_pct, "tp_pct")
    _require_ratio_0_1(sl_pct, "sl_pct")

    pre_entry_score = _compute_pre_entry_score(extra)

    # 5) unified_features 스냅샷 생성(마이크로구조 포함) — 실패 시 ENTER 금지(명시적 SKIP)
    try:
        unified = build_unified_features(symbol)
    except UnifiedFeaturesError as e:
        msg = f"[SKIP][UNIFIED_FAIL] unified_features unavailable: {e}"
        _safe_send_skip(msg)
        logger.warning(msg)
        log_skip_event(
            symbol=symbol,
            regime=regime_label,
            source="entry_engine.unified",
            side=chosen_signal_u,
            reason="unified_features_fail",
            extra={"error": str(e)},
        )
        return None, _SLEEP_DEFAULT

    # 6) GPT Supervisor(감사/해설) — 실패 시 ENTER 금지(명시적 SKIP)
    decision_id = f"{int(time.time()*1000)}-{uuid.uuid4().hex[:12]}"
    quant_decision_pre: Dict[str, Any] = {
        "symbol": symbol,
        "direction": chosen_signal_u,
        "signal_source": signal_source_s,
        "entry_score": pre_entry_score,
        "risk_pct": float(effective_risk_pct),
        "tp_pct": float(tp_pct),
        "sl_pct": float(sl_pct),
        "leverage": float(leverage),
        "last_price": float(last_price_f),
        "latest_ts": int(latest_ts),
    }
    quant_constraints: Dict[str, Any] = {
        "risk_pct_range": [0.0, 1.0],
        "tp_pct_range": [0.0, 1.0],
        "sl_pct_range": [0.0, 1.0],
        "leverage_min": 1.0,
        "slippage_block_pct": float(getattr(settings, "slippage_block_pct")),
    }

    try:
        sup = run_gpt_supervisor(
            decision_id=decision_id,
            event_type="ENTER_CANDIDATE",
            unified_features=unified,
            quant_decision_pre=quant_decision_pre,
            quant_constraints=quant_constraints,
            recent_messages=None,
        )
    except GptSupervisorError as e:
        msg = f"[SKIP][SUPERVISOR_FAIL] supervisor error: {e}"
        _safe_send_skip(msg)
        logger.warning(msg)
        log_skip_event(
            symbol=symbol,
            regime=regime_label,
            source="entry_engine.supervisor",
            side=chosen_signal_u,
            reason="gpt_supervisor_error",
            extra={"error": str(e)},
        )
        return None, _SLEEP_GPT_ERROR

    # Supervisor 결과 기록(결정권 없음)
    _safe_log_event(
        "on_gpt_audit",
        symbol=symbol,
        regime=regime_label,
        source="entry_engine.supervisor",
        side=chosen_signal_u,
        price=last_price_f,
        reason="gpt_audit",
        extra_json={
            "decision_id": decision_id,
            "severity": sup.auditor.severity,
            "tags": list(sup.auditor.tags),
            "confidence_penalty": sup.auditor.confidence_penalty,
            "suggested_risk_multiplier": sup.auditor.suggested_risk_multiplier,
            "rationale_short": sup.auditor.rationale_short,
            "narration": (sup.narration.message if sup.narration else None),
        },
    )

    if sup.narration is not None:
        _safe_send_tg(f"[SUPERVISOR] {sup.narration.title}: {sup.narration.message}")

    # 결정 반영 규칙(명시 룰): severity>=2는 신규 진입 차단
    if sup.auditor.severity >= 2:
        msg = f"[SKIP][AUDIT_BLOCK] severity={sup.auditor.severity} tags={','.join(sup.auditor.tags)}"
        _safe_send_skip(msg)
        logger.info(msg)
        log_skip_event(
            symbol=symbol,
            regime=regime_label,
            source="entry_engine.supervisor",
            side=chosen_signal_u,
            reason="audit_block",
            extra={
                "decision_id": decision_id,
                "severity": sup.auditor.severity,
                "tags": list(sup.auditor.tags),
            },
        )
        return None, _SLEEP_GPT_SKIP

    # 리스크 페널티(명시 룰): min(confidence_penalty, suggested_risk_multiplier)로 risk_pct 축소
    mul = float(min(sup.auditor.confidence_penalty, sup.auditor.suggested_risk_multiplier))
    if not math.isfinite(mul) or mul < 0.0 or mul > 1.0:
        raise RuntimeError(f"invalid supervisor multipliers: {mul}")

    effective_risk_pct = float(effective_risk_pct) * mul
    _require_ratio_0_1(effective_risk_pct, "effective_risk_pct_after_supervisor")
    if effective_risk_pct <= 0.0:
        msg = f"[SKIP][AUDIT_RISK_ZERO] risk reduced to 0 by supervisor mul={mul}"
        _safe_send_skip(msg)
        logger.info(msg)
        log_skip_event(
            symbol=symbol,
            regime=regime_label,
            source="entry_engine.supervisor",
            side=chosen_signal_u,
            reason="audit_risk_zero",
            extra={"decision_id": decision_id, "mul": mul},
        )
        return None, _SLEEP_GPT_SKIP

    # 7) 가드(진입 직전)
    vol_ok = check_volume_guard(
        settings=settings,
        candles_5m_raw=candles_5m_raw,
        latest_ts=float(latest_ts),
        signal_source=signal_source_s,
        direction=chosen_signal_u,
    )
    if not vol_ok:
        msg = "[SKIP] volume_guard_blocked"
        _safe_send_skip(msg)
        logger.info(msg)
        log_skip_event(
            symbol=symbol,
            regime=regime_label,
            source="entry_engine.guard.volume",
            side=chosen_signal_u,
            reason="volume_guard_blocked",
            extra={"latest_ts": int(latest_ts)},
        )
        return None, _SLEEP_DEFAULT

    price_ok = check_price_jump_guard(
        settings=settings,
        candles_5m=candles_5m,
        latest_ts=float(latest_ts),
        signal_source=signal_source_s,
        direction=chosen_signal_u,
    )
    if not price_ok:
        msg = "[SKIP] price_jump_guard_blocked"
        _safe_send_skip(msg)
        logger.info(msg)
        log_skip_event(
            symbol=symbol,
            regime=regime_label,
            source="entry_engine.guard.price_jump",
            side=chosen_signal_u,
            reason="price_jump_guard_blocked",
            extra={"latest_ts": int(latest_ts)},
        )
        return None, _SLEEP_DEFAULT

    spread_ok, best_bid, best_ask = check_spread_guard(
        settings=settings,
        symbol=symbol,
        latest_ts=float(latest_ts),
        signal_source=signal_source_s,
        direction=chosen_signal_u,
    )
    if not spread_ok:
        msg = "[SKIP] spread_guard_blocked"
        _safe_send_skip(msg)
        logger.info(msg)
        log_skip_event(
            symbol=symbol,
            regime=regime_label,
            source="entry_engine.guard.spread",
            side=chosen_signal_u,
            reason="spread_guard_blocked",
            extra={"latest_ts": int(latest_ts), "best_bid": best_bid, "best_ask": best_ask},
        )
        return None, _SLEEP_DEFAULT

    # 8) 슬리피지 차단(명시 룰)
    entry_price_hint = _entry_price_hint(chosen_signal_u, last_price_f, best_bid, best_ask)
    slippage_block_pct = _as_float(getattr(settings, "slippage_block_pct"), "slippage_block_pct", min_value=0.0)
    slippage_stop_engine = bool(getattr(settings, "slippage_stop_engine"))

    if slippage_block_pct > 0.0:
        slippage_pct = abs(entry_price_hint - last_price_f) / last_price_f
        if slippage_pct > slippage_block_pct:
            reason = (
                f"slippage_block: slippage_pct={slippage_pct*100:.3f}% "
                f"> block={slippage_block_pct*100:.3f}%"
            )
            _safe_send_skip(f"[SKIP] {reason}")
            logger.warning("[SLIPPAGE_BLOCK] %s", reason)
            _safe_log_event(
                "on_slippage_block",
                symbol=symbol,
                regime=regime_label,
                source="entry_engine.slippage",
                side=chosen_signal_u,
                price=float(entry_price_hint),
                reason="slippage_block",
                extra_json={
                    "slippage_pct": slippage_pct,
                    "slippage_block_pct": slippage_block_pct,
                    "last_price": last_price_f,
                    "entry_price_hint": entry_price_hint,
                },
            )
            log_skip_event(
                symbol=symbol,
                regime=regime_label,
                source="entry_engine.slippage",
                side=chosen_signal_u,
                reason="slippage_block",
                extra={"slippage_pct": slippage_pct, "slippage_block_pct": slippage_block_pct},
            )
            if slippage_stop_engine:
                raise RuntimeError("slippage_block triggered and slippage_stop_engine=True")
            return None, _SLEEP_GPT_SKIP

    # 9) 수량 산출(라운딩 금지)
    notional = avail_usdt * float(effective_risk_pct) * float(leverage)
    if not math.isfinite(notional) or notional <= 0:
        raise ValueError(f"invalid notional: {notional}")

    price_for_qty = _price_for_qty(last_price_f, best_bid, best_ask)
    if not math.isfinite(price_for_qty) or price_for_qty <= 0:
        raise ValueError(f"invalid price_for_qty: {price_for_qty}")

    qty_raw = notional / price_for_qty
    if not math.isfinite(qty_raw) or qty_raw <= 0:
        raise ValueError(f"invalid qty_raw: {qty_raw}")

    # 10) 하드 리스크 가드
    ok, guard_reason, guard_extra = hard_risk_guard_check(
        settings,
        symbol=symbol,
        side=chosen_signal_u,
        entry_price=float(entry_price_hint),
        notional=float(notional),
        available_usdt=float(avail_usdt),
    )
    if not ok:
        msg = f"[RISK_GUARD] blocked: {guard_reason}"
        _safe_send_skip(msg)
        logger.warning("%s extra=%s", msg, guard_extra)
        _safe_log_event(
            "on_risk_guard_trigger",
            symbol=symbol,
            regime=regime_label,
            source="entry_engine.risk_guard",
            side=chosen_signal_u,
            price=float(entry_price_hint),
            reason=guard_reason,
            extra_json=guard_extra,
        )
        log_skip_event(
            symbol=symbol,
            regime=regime_label,
            source="entry_engine.risk_guard",
            side=chosen_signal_u,
            reason=guard_reason,
            extra=guard_extra,
        )
        return None, _SLEEP_RISK_GUARD

    # 11) 주문 실행
    side_open = "BUY" if chosen_signal_u == "LONG" else "SELL"
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
        soft_mode=False,
        sl_floor_ratio=None,
    )

    # Supervisor 결정ID를 이벤트로 남김(분석/감사용)
    try:
        log_gpt_entry_event(
            symbol=symbol,
            regime=regime_label,
            side=chosen_signal_u,
            action="AUDIT_ONLY",
            tp_pct=float(tp_pct),
            sl_pct=float(sl_pct),
            risk_pct=float(effective_risk_pct),
            gpt_json={
                "decision_id": decision_id,
                "severity": sup.auditor.severity,
                "tags": list(sup.auditor.tags),
                "confidence_penalty": sup.auditor.confidence_penalty,
                "suggested_risk_multiplier": sup.auditor.suggested_risk_multiplier,
                "rationale_short": sup.auditor.rationale_short,
            },
        )
    except Exception as e:  # pragma: no cover
        logger.debug("log_gpt_entry_event failed: %s", e)

    return trade, _SLEEP_POST_ENTRY


def hard_risk_guard_check(
    settings: Any,
    *,
    symbol: str,
    side: str,
    entry_price: float,
    notional: float,
    available_usdt: float,
) -> Tuple[bool, str, Dict[str, Any]]:
    """문서 기준 하드 리스크 가드."""
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
    max_notional = available_usdt * (pct_cap / 100.0)
    extra["hard_position_value_pct_cap"] = pct_cap
    extra["max_notional_by_cap"] = max_notional
    if notional > max_notional:
        return False, "hard_position_value_pct_cap_exceeded", extra

    # [2-4] 청산거리 최소 %
    liq_min_pct = float(getattr(settings, "hard_liquidation_distance_pct_min"))
    if liq_min_pct > 0:
        pos = get_position(symbol)
        if not isinstance(pos, dict):
            raise RuntimeError("get_position() returned non-dict")

        pos_amt = _safe_float_from_dict(pos, "positionAmt")
        liq_price = _safe_float_from_dict(pos, "liquidationPrice")

        extra["positionAmt"] = pos_amt
        extra["liquidationPrice"] = liq_price
        extra["hard_liquidation_distance_pct_min"] = liq_min_pct

        if pos_amt is not None and abs(pos_amt) > 0:
            if liq_price is None or liq_price <= 0:
                raise RuntimeError("liquidationPrice unavailable while positionAmt != 0")

            dist_pct = abs(entry_price - liq_price) / entry_price * 100.0
            extra["liquidation_distance_pct"] = dist_pct
            if dist_pct < liq_min_pct:
                return False, "hard_liquidation_distance_too_close", extra

    return True, "OK", extra


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
    """order_executor에 주문 실행을 위임한다."""
    side_open_u = _require_nonempty_str(str(side_open).upper().strip(), "side_open")
    if side_open_u not in ("BUY", "SELL"):
        raise ValueError(f"invalid side_open: {side_open_u}")

    if not math.isfinite(qty) or qty <= 0:
        raise ValueError(f"invalid qty: {qty}")
    if not math.isfinite(entry_price_hint) or entry_price_hint <= 0:
        raise ValueError(f"invalid entry_price_hint: {entry_price_hint}")

    _safe_log_event(
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

    try:
        log_signal(
            event="ENTRY_SIGNAL",
            symbol=symbol,
            strategy_type=source,
            direction="LONG" if side_open_u == "BUY" else "SHORT",
            price=float(entry_price_hint),
            qty=float(qty),
            reason="approved_by_rules_with_audit",
            candle_ts=int(signal_ts_ms),
            available_usdt=float(get_available_usdt()),
            notional=float(get_available_usdt()) * float(risk_pct) * float(leverage),
        )
    except Exception as e:  # pragma: no cover
        logger.debug("log_signal(ENTRY_SIGNAL) failed: %s", e)

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

    _safe_log_event(
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

    return trade


# ─────────────────────────────────────────────────────────────────────────────
# Utilities (STRICT)
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


def _safe_log_event(
    event_type: str,
    *,
    symbol: str,
    regime: str,
    source: str,
    side: str,
    reason: str,
    price: Optional[float] = None,
    extra_json: Optional[Dict[str, Any]] = None,
) -> None:
    try:
        log_event(
            event_type,
            symbol=symbol,
            regime=regime,
            source=source,
            side=side,
            price=price,
            reason=reason,
            extra_json=extra_json,
        )
    except Exception as e:  # pragma: no cover
        logger.debug("log_event(%s) failed: %s", event_type, e)


def _safe_log_candle_snapshot(
    symbol: str,
    signal_source_s: str,
    chosen_signal_u: str,
    latest_ts: int,
    candles_5m: Any,
    candles_5m_raw: Any,
) -> None:
    try:
        if isinstance(candles_5m_raw, list) and candles_5m_raw and len(candles_5m_raw[-1]) >= 6:
            r = candles_5m_raw[-1]
            c_ts, o, h, l, c, v = int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])
        elif isinstance(candles_5m, list) and candles_5m and len(candles_5m[-1]) >= 5:
            c0 = candles_5m[-1]
            c_ts, o, h, l, c, v = int(c0[0]), float(c0[1]), float(c0[2]), float(c0[3]), float(c0[4]), 0.0
        else:
            return

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
            direction=chosen_signal_u,
            extra="signal_eval=1",
        )
    except Exception as e:  # pragma: no cover
        logger.debug("log_candle_snapshot failed: %s", e)


def _validate_settings_contract(settings: Any) -> None:
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
    daily = _as_float(getattr(settings, "hard_daily_loss_limit_usdt"), "hard_daily_loss_limit_usdt", min_value=0.0)
    consec = int(getattr(settings, "hard_consecutive_losses_limit"))
    pct_cap = _as_float(getattr(settings, "hard_position_value_pct_cap"), "hard_position_value_pct_cap", min_value=0.0)
    liq = _as_float(getattr(settings, "hard_liquidation_distance_pct_min"), "hard_liquidation_distance_pct_min", min_value=0.0)

    if consec < 0:
        raise ValueError("hard_consecutive_losses_limit must be >= 0")
    if not (0.0 <= pct_cap <= 100.0):
        raise ValueError("hard_position_value_pct_cap must be in [0,100]")
    if not (0.0 <= liq <= 100.0):
        raise ValueError("hard_liquidation_distance_pct_min must be in [0,100]")
    if daily < 0:
        raise ValueError("hard_daily_loss_limit_usdt must be >= 0")


def _compute_pre_entry_score(extra: Any) -> Optional[float]:
    if not isinstance(extra, dict):
        return None
    raw = extra.get("signal_score")
    if raw is None:
        raw = extra.get("candidate_score")
    if not isinstance(raw, (int, float)) or not math.isfinite(float(raw)):
        return None
    v = float(raw)
    if 0.0 <= v <= 10.0:
        return v * 10.0
    if 0.0 <= v <= 100.0:
        return v
    return None


def _as_float(value: Any, name: str, *, min_value: Optional[float] = None) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be numeric (bool not allowed)")
    try:
        v = float(value)
    except Exception as e:
        raise ValueError(f"{name} must be a number") from e
    if not math.isfinite(v):
        raise ValueError(f"{name} must be finite")
    if min_value is not None and v < min_value:
        raise ValueError(f"{name} must be >= {min_value}")
    return v


def _require_ratio_0_1(v: float, name: str) -> None:
    if not math.isfinite(v) or v <= 0.0 or v > 1.0:
        raise ValueError(f"{name} out of range (0,1]: {v}")


def _require_nonempty_str(v: str, name: str) -> str:
    s = str(v).strip()
    if not s:
        raise ValueError(f"{name} is empty")
    return s


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


def _entry_price_hint(direction: str, last_price: float, best_bid: Optional[float], best_ask: Optional[float]) -> float:
    if best_bid is not None and best_ask is not None and best_bid > 0 and best_ask > 0:
        return float(best_ask) if direction == "LONG" else float(best_bid)
    return float(last_price)


def _price_for_qty(last_price: float, best_bid: Optional[float], best_ask: Optional[float]) -> float:
    if best_bid is not None and best_ask is not None and best_bid > 0 and best_ask > 0:
        mid = (float(best_bid) + float(best_ask)) / 2.0
        if mid > 0:
            return mid
    return float(last_price)


def _kst_day_start_utc() -> datetime:
    now_kst = datetime.now(tz=KST)
    start_kst = datetime(now_kst.year, now_kst.month, now_kst.day, tzinfo=KST)
    return start_kst.astimezone(timezone.utc)


def _get_today_realized_pnl_usdt(symbol: str) -> float:
    start_utc = _kst_day_start_utc()
    end_utc = start_utc + timedelta(days=1)

    session = SessionLocal()
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
        return float(result)
    except Exception as e:
        raise RuntimeError(f"daily pnl query failed: {e}") from e
    finally:
        session.close()


def _get_consecutive_losses(symbol: str, *, lookback: int = 50) -> int:
    if not isinstance(lookback, int) or lookback <= 0:
        raise ValueError("lookback must be int > 0")

    session = SessionLocal()
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
            pnl = float(pnl_val)
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
    entry_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

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

    session = SessionLocal()
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


__all__ = ["try_open_new_position", "hard_risk_guard_check", "execute_entry"]