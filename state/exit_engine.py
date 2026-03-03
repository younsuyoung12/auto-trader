# state/exit_engine.py
from __future__ import annotations

"""
========================================================
FILE: state/exit_engine.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
역할
- Binance WS 데이터 기반으로 현재 포지션(Trade)의 EXIT/HOLD를 평가한다.
- 최종 청산 결정은 규칙 기반(Quant)으로만 수행한다.
- GPT는 "감사/해설"만 수행한다(결정 금지). 실패해도 거래 흐름을 깨지 않는다.

근본 해결(필수)
- 삭제된 strategy.gpt_decider_entry 의존 제거
- DB 업데이트는 state.db_writer(SSOT)로 통일
- 텔레그램은 async_worker로 위임(메인 루프 블로킹 금지)

변경 이력
--------------------------------------------------------
- 2026-03-03 (TRADE-GRADE, FIX):
  1) gpt_decider_entry 제거 → gpt_supervisor(감사/해설 전용)로 교체
  2) EXIT 결정은 RUNTIME 규칙만 사용(STRICT)
  3) SessionLocal/직접 ORM 업데이트 제거 → db_writer로 통일
========================================================
"""

import math
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from infra.telelog import log, send_tg
from infra.async_worker import submit as submit_async
from infra.market_data_ws import (
    get_klines as ws_get_klines,
    get_klines_with_volume as ws_get_klines_with_vol,
)
from execution.order_executor import close_position_market
from events.signals_logger import log_signal, log_candle_snapshot, log_skip_event, log_event
from state.trader_state import Trade
from state.db_writer import close_latest_open_trade_returning_id, record_trade_exit_snapshot
from strategy.indicators import build_regime_features_from_candles


# (옵션) GPT Supervisor: 감사/해설 전용
from strategy.gpt_supervisor import run_gpt_supervisor, GptSupervisorError

# (옵션) unified features: supervisor 입력용(없으면 해설 생략)
from strategy.unified_features_builder import build_unified_features, UnifiedFeaturesError

LOG_PW = "[PW_BINANCE]"
LOG_EXIT = "[PW_BINANCE][EXIT]"

EXIT_CHECK_INTERVAL_SEC: float = 60.0

# supervisor 쿨다운(포지션별)
_EXIT_SUP_LAST_CALL_TS: Dict[str, float] = {}
_RECENT_SUP_MSGS: List[str] = []  # repetition 방지용(최근 메시지)


def _submit_tg(msg: str) -> None:
    """
    STRICT:
    - 텔레그램은 비핵심 I/O → async_worker로 위임
    """
    try:
        ok = submit_async(send_tg, msg, critical=False, label="send_tg")
        if not ok:
            log(f"{LOG_EXIT}[TG][DROP] queue full: {msg}")
    except Exception as e:
        log(f"{LOG_EXIT}[TG][SUBMIT_FAIL] {type(e).__name__}: {e} | msg={msg}")


def _as_float(v: Any, name: str, *, min_value: Optional[float] = None) -> float:
    if isinstance(v, bool):
        raise RuntimeError(f"{name} must be numeric (bool not allowed)")
    try:
        x = float(v)
    except Exception as e:
        raise RuntimeError(f"{name} must be numeric: {e}") from e
    if not math.isfinite(x):
        raise RuntimeError(f"{name} must be finite")
    if min_value is not None and x < min_value:
        raise RuntimeError(f"{name} must be >= {min_value}")
    return float(x)


def _open_side_strict(side: Any) -> str:
    s = str(side or "").upper().strip()
    if s in ("BUY", "SELL"):
        return s
    if s == "LONG":
        return "BUY"
    if s == "SHORT":
        return "SELL"
    raise RuntimeError(f"invalid trade.side (STRICT): {side!r}")


def _dir_strict(side: Any) -> str:
    s = str(side or "").upper().strip()
    if s in ("LONG", "SHORT"):
        return s
    if s == "BUY":
        return "LONG"
    if s == "SELL":
        return "SHORT"
    raise RuntimeError(f"invalid direction (STRICT): {side!r}")


def _pnl_usdt(open_side: str, entry: float, last: float, qty: float) -> float:
    if entry <= 0 or last <= 0 or qty <= 0:
        raise RuntimeError("invalid entry/last/qty for pnl (STRICT)")
    if open_side == "BUY":
        return (last - entry) * qty
    if open_side == "SELL":
        return (entry - last) * qty
    raise RuntimeError(f"invalid open_side: {open_side!r}")


def _pnl_pct(pnl: float, entry: float, qty: float) -> float:
    base = entry * qty
    if base <= 0:
        raise RuntimeError("invalid base notional for pnl_pct (STRICT)")
    return float(pnl / base)


def _get_last_candle_with_volume(symbol: str) -> Optional[Tuple[int, float, float, float, float, float]]:
    """
    STRICT:
    - WS 1m 우선, 없으면 5m
    """
    try:
        candles = ws_get_klines_with_vol(symbol, "1m", 1)
        if not candles:
            candles = ws_get_klines_with_vol(symbol, "5m", 1)
    except Exception as e:
        log(f"{LOG_EXIT} kline fetch error symbol={symbol}: {e}")
        return None

    if not candles:
        return None

    row = candles[-1]
    if len(row) < 5:
        return None

    try:
        ts = int(row[0])
        o = float(row[1])
        h = float(row[2])
        l = float(row[3])
        c = float(row[4])
        v = float(row[5]) if len(row) >= 6 else 0.0
    except (TypeError, ValueError):
        return None

    if ts <= 0 or o <= 0 or h <= 0 or l <= 0 or c <= 0:
        return None
    return ts, o, h, l, c, v


def _get_15m_trend_dir(symbol: str) -> str:
    """
    STRICT:
    - 데이터 부족/오류는 "" 반환(명시적 HOLD 판단에 사용)
    """
    try:
        candles_15m = ws_get_klines(symbol, "15m", 120)
        if not candles_15m:
            return ""
        last = candles_15m[-1]
        o = float(last[1])
        c = float(last[4])
        if o <= 0 or c <= 0:
            return ""
        if c > o:
            return "LONG"
        if c < o:
            return "SHORT"
        return ""
    except Exception as e:
        log(f"{LOG_PW}[TREND15] ws_get_klines error symbol={symbol}: {e}")
        return ""


def _is_opposite(trade_side: Any, trend_dir: str) -> bool:
    side = _dir_strict(trade_side)
    td = str(trend_dir or "").upper().strip()
    if not td:
        return False
    return (side == "LONG" and td == "SHORT") or (side == "SHORT" and td == "LONG")


def _runtime_thresholds(settings: Any) -> Dict[str, float]:
    """
    정책 기본값(명시):
    - opposite + profit >= +0.2% -> 익절
    - opposite + loss   <= -0.5% -> 손절
    - hard_stop_loss_pct (옵션): -X%면 즉시 청산
    """
    op_profit = float(getattr(settings, "exit_opposite_profit_take_pct", 0.002) or 0.002)
    op_loss = float(getattr(settings, "exit_opposite_loss_cut_pct", 0.005) or 0.005)
    hard_stop = float(getattr(settings, "exit_hard_stop_loss_pct", 0.0) or 0.0)

    if op_profit <= 0 or op_loss <= 0:
        raise RuntimeError("exit thresholds must be positive (STRICT)")
    if hard_stop < 0:
        raise RuntimeError("exit_hard_stop_loss_pct must be >= 0 (STRICT)")

    return {"op_profit": op_profit, "op_loss": op_loss, "hard_stop": hard_stop}


def _exit_supervisor_enabled(settings: Any) -> bool:
    """
    기본 OFF.
    - 켜려면:
      1) settings.exit_supervisor_enabled=True 또는
      2) ENV EXIT_SUPERVISOR_ENABLED=1
    """
    env = os.getenv("EXIT_SUPERVISOR_ENABLED", "0").strip().lower()
    if env in ("1", "true", "t", "yes", "y", "on"):
        return True
    return bool(getattr(settings, "exit_supervisor_enabled", False))


def _trade_key(trade: Trade) -> str:
    tid = getattr(trade, "id", None)
    if isinstance(tid, int) and tid > 0:
        return f"id:{tid}"
    entry = float(getattr(trade, "entry", 0.0) or 0.0)
    qty = float(getattr(trade, "qty", 0.0) or 0.0)
    return f"{trade.symbol}:{trade.side}:{entry:.8f}:{qty:.8f}"


def _run_exit_supervisor_best_effort(
    *,
    trade: Trade,
    settings: Any,
    scenario: str,
    candle_ts_ms: int,
    last_price: float,
    pnl_pct_val: float,
    trend15_dir: str,
    opposite: bool,
    thresholds: Dict[str, float],
) -> None:
    """
    GPT Supervisor는 '감사/해설'만 한다.
    실패해도 거래 흐름을 깨지 않는다(옵션).
    """
    if not _exit_supervisor_enabled(settings):
        return

    key = _trade_key(trade)
    now = time.time()
    cooldown = float(getattr(settings, "exit_supervisor_cooldown_sec", 180.0) or 180.0)
    last = _EXIT_SUP_LAST_CALL_TS.get(key)
    if last is not None and (now - last) < cooldown:
        return

    # unified_features는 STRICT 빌더이므로 준비 안 됐으면 예외 → 여기서는 해설 생략
    try:
        unified = build_unified_features(trade.symbol)
    except (UnifiedFeaturesError, Exception) as e:
        log(f"{LOG_EXIT}[SUP] unified_features build failed -> narration skip: {type(e).__name__}:{e}")
        return

    decision_id = f"exit-{trade.symbol}-{candle_ts_ms}"
    quant_pre = {
        "scenario": scenario,
        "symbol": trade.symbol,
        "side": str(trade.side),
        "entry_price": float(getattr(trade, "entry", 0.0) or 0.0),
        "qty": float(getattr(trade, "qty", 0.0) or 0.0),
        "last_price": float(last_price),
        "pnl_pct": float(pnl_pct_val),
        "trend15_dir": trend15_dir,
        "opposite": bool(opposite),
        "thresholds": dict(thresholds),
        "note": "EXIT narration/audit only. No decision allowed.",
    }
    constraints = {
        "no_trade_decision": True,
        "no_enter_exit_hold": True,
        "no_position_sizing": True,
        "no_tp_sl": True,
        "max_sentences": 2,
        "language": "ko",
    }

    try:
        res = run_gpt_supervisor(
            decision_id=decision_id,
            event_type="exit_audit",
            unified_features=unified,
            quant_decision_pre=quant_pre,
            quant_constraints=constraints,
            recent_messages=list(_RECENT_SUP_MSGS[-10:]),
        )
    except GptSupervisorError as e:
        log_event(
            "on_exit_supervisor_fail",
            symbol=trade.symbol,
            regime=str(getattr(trade, "source", "") or "UNKNOWN"),
            source="exit_engine.supervisor",
            side=_dir_strict(trade.side),
            price=float(last_price),
            reason="supervisor_failed",
            extra_json={"error": str(e)[:240]},
        )
        return

    _EXIT_SUP_LAST_CALL_TS[key] = now

    # events 기록(감사)
    log_event(
        "on_exit_supervisor",
        symbol=trade.symbol,
        regime=str(getattr(trade, "source", "") or "UNKNOWN"),
        source="exit_engine.supervisor",
        side=_dir_strict(trade.side),
        price=float(last_price),
        reason="exit_supervisor_audit",
        extra_json={
            "decision_id": res.decision_id,
            "severity": int(res.auditor.severity),
            "tags": list(res.auditor.tags),
            "confidence_penalty": float(res.auditor.confidence_penalty),
            "suggested_risk_multiplier": float(res.auditor.suggested_risk_multiplier),
            "rationale_short": res.auditor.rationale_short,
            "narration": None if res.narration is None else {"title": res.narration.title, "message": res.narration.message, "tone": res.narration.tone},
        },
    )

    if res.narration is not None:
        msg = f"📘 [EXIT 해설] {res.narration.title}\n{res.narration.message}"
        _RECENT_SUP_MSGS.append(res.narration.message)
        _submit_tg(msg)


def _close_and_record_strict(
    *,
    trade: Trade,
    close_price: float,
    candle_ts_ms: int,
    reason: str,
    regime_label: str,
) -> None:
    """
    STRICT:
    - 시장가 청산 → bt_trades close → bt_trade_exit_snapshots 기록
    """
    symbol = str(getattr(trade, "symbol", "")).strip()
    if not symbol:
        raise RuntimeError("trade.symbol missing (STRICT)")

    open_side = _open_side_strict(getattr(trade, "side", ""))
    entry = _as_float(getattr(trade, "entry", None), "trade.entry", min_value=0.0)
    qty = _as_float(getattr(trade, "qty", None), "trade.qty", min_value=0.0)
    close_price_f = _as_float(close_price, "close_price", min_value=0.0)

    if entry <= 0 or qty <= 0 or close_price_f <= 0:
        raise RuntimeError("entry/qty/close_price must be > 0 (STRICT)")

    pnl = _pnl_usdt(open_side=open_side, entry=entry, last=close_price_f, qty=qty)
    pnl_pct_val = _pnl_pct(pnl=pnl, entry=entry, qty=qty)

    # 1) 주문 실행
    close_position_market(symbol, open_side, float(qty))

    # 2) DB close + exit snapshot
    exit_dt = datetime.fromtimestamp(int(candle_ts_ms) / 1000.0, tz=timezone.utc)
    closed_trade_id = close_latest_open_trade_returning_id(
        symbol=symbol,
        side=open_side,
        exit_price=float(close_price_f),
        exit_ts=exit_dt,
        pnl_usdt=float(pnl),
        close_reason=str(reason)[:32],
        regime_at_exit=str(regime_label)[:16] if regime_label else None,
        pnl_pct_futures=float(pnl_pct_val),
        pnl_pct_spot_ref=None,
        note=f"exit_engine:{reason}",
    )

    record_trade_exit_snapshot(
        trade_id=int(closed_trade_id),
        symbol=symbol,
        close_ts=exit_dt,
        close_price=float(close_price_f),
        pnl=float(pnl),
        close_reason=str(reason)[:200],
        exit_atr_pct=None,
        exit_trend_strength=None,
        exit_volume_zscore=None,
        exit_depth_ratio=None,
    )


def maybe_exit_with_gpt(  # 이름은 호출부 호환을 위해 유지
    trade: Trade,
    settings: Any,
    *,
    scenario: str = "GENERIC_EXIT_CHECK",
) -> bool:
    """
    RUNTIME(규칙 기반) EXIT/HOLD 판단 후, 필요 시 청산 실행.

    반환:
      True  -> 실제 청산 실행
      False -> HOLD
    """
    regime_label = getattr(trade, "source", "") or "UNKNOWN"

    qty = _as_float(getattr(trade, "qty", None), "trade.qty", min_value=0.0)
    if qty <= 0:
        log(f"{LOG_EXIT} invalid qty<=0 → HOLD (symbol={trade.symbol})")
        return False

    last_candle = _get_last_candle_with_volume(trade.symbol)
    if last_candle is None:
        log(f"{LOG_EXIT} no recent WS candle → HOLD (symbol={trade.symbol})")
        return False

    candle_ts, o, h, l, c, v = last_candle
    entry = _as_float(getattr(trade, "entry", None), "trade.entry", min_value=0.0)
    if entry <= 0 or c <= 0:
        log(f"{LOG_EXIT} invalid entry/last_close → HOLD (symbol={trade.symbol})")
        return False

    # candle snapshot (best-effort)
    try:
        log_candle_snapshot(
            symbol=trade.symbol,
            tf="1m",
            candle_ts=int(candle_ts),
            open_=o,
            high=h,
            low=l,
            close=c,
            volume=v,
            strategy_type=regime_label,
            direction=getattr(trade, "side", ""),
            extra=f"exit_check=1;scenario={scenario};market=binance_futures_ws",
        )
    except Exception as e:
        log(f"{LOG_EXIT} candle snapshot log failed symbol={trade.symbol}: {e}")

    open_side = _open_side_strict(getattr(trade, "side", ""))
    pnl = _pnl_usdt(open_side=open_side, entry=float(entry), last=float(c), qty=float(qty))
    pnl_pct_val = _pnl_pct(pnl=pnl, entry=float(entry), qty=float(qty))

    trend15_dir = _get_15m_trend_dir(trade.symbol)
    opposite = _is_opposite(getattr(trade, "side", ""), trend15_dir) if trend15_dir else False

    th = _runtime_thresholds(settings)

    # ─────────────────────────────────────────
    # RUNTIME EXIT RULES (DECISION = Quant Only)
    # ─────────────────────────────────────────
    if th["hard_stop"] > 0.0 and pnl_pct_val <= -float(th["hard_stop"]):
        reason = "runtime_hard_stop_loss"
        _submit_tg(f"🧯 [EXIT][HARD_STOP] {trade.symbol} {open_side} pnl={pnl:.4f}USDT ({pnl_pct_val*100:.2f}%)")
        log_signal(
            event="CLOSE",
            symbol=trade.symbol,
            strategy_type=regime_label,
            direction=getattr(trade, "side", ""),
            price=float(c),
            qty=float(qty),
            reason=reason,
            pnl=float(pnl),
        )
        _close_and_record_strict(
            trade=trade,
            close_price=float(c),
            candle_ts_ms=int(candle_ts),
            reason=reason,
            regime_label=regime_label,
        )
        return True

    if opposite and pnl_pct_val >= float(th["op_profit"]):
        reason = "runtime_opposite_profit_take"
        _submit_tg(f"✅ [EXIT][OPPOSITE_TAKE] {trade.symbol} {open_side} pnl={pnl:.4f}USDT ({pnl_pct_val*100:.2f}%)")
        log_signal(
            event="CLOSE",
            symbol=trade.symbol,
            strategy_type=regime_label,
            direction=getattr(trade, "side", ""),
            price=float(c),
            qty=float(qty),
            reason=reason,
            pnl=float(pnl),
        )
        _close_and_record_strict(
            trade=trade,
            close_price=float(c),
            candle_ts_ms=int(candle_ts),
            reason=reason,
            regime_label=regime_label,
        )
        return True

    if opposite and pnl_pct_val <= -float(th["op_loss"]):
        reason = "runtime_opposite_loss_cut"
        _submit_tg(f"⚠️ [EXIT][OPPOSITE_CUT] {trade.symbol} {open_side} pnl={pnl:.4f}USDT ({pnl_pct_val*100:.2f}%)")
        log_signal(
            event="CLOSE",
            symbol=trade.symbol,
            strategy_type=regime_label,
            direction=getattr(trade, "side", ""),
            price=float(c),
            qty=float(qty),
            reason=reason,
            pnl=float(pnl),
        )
        _close_and_record_strict(
            trade=trade,
            close_price=float(c),
            candle_ts_ms=int(candle_ts),
            reason=reason,
            regime_label=regime_label,
        )
        return True

    # ─────────────────────────────────────────
    # HOLD (explicit) + optional supervisor narration/audit
    # ─────────────────────────────────────────
    try:
        log_skip_event(
            symbol=trade.symbol,
            regime=regime_label,
            source="exit_engine.maybe_exit_with_gpt",
            side=getattr(trade, "side", ""),
            reason="runtime_hold",
            extra={
                "scenario": scenario,
                "pnl_usdt": float(pnl),
                "pnl_pct": float(pnl_pct_val),
                "trend15_dir": trend15_dir,
                "opposite": int(opposite),
                "thresholds": th,
            },
        )
    except Exception as e:
        log(f"{LOG_EXIT}[HOLD][SKIP_LOG] failed symbol={trade.symbol}: {e}")

    _run_exit_supervisor_best_effort(
        trade=trade,
        settings=settings,
        scenario=scenario,
        candle_ts_ms=int(candle_ts),
        last_price=float(c),
        pnl_pct_val=float(pnl_pct_val),
        trend15_dir=trend15_dir,
        opposite=bool(opposite),
        thresholds=th,
    )
    return False


__all__ = [
    "maybe_exit_with_gpt",
    "EXIT_CHECK_INTERVAL_SEC",
]