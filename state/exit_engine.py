# state/exit_engine.py
from __future__ import annotations

"""
========================================================
FILE: state/exit_engine.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================
역할
- Binance WS 데이터 기반으로 현재 포지션(Trade)의 EXIT/HOLD를 평가한다.
- 최종 청산 결정은 규칙 기반(Quant)으로만 수행한다.
- GPT는 "감사/해설"만 수행한다(결정 금지). 실패해도 거래 흐름을 깨지 않는다(옵션).

절대 원칙 (TRADE-GRADE)
- 폴백 금지: None→0/기본값 주입/조용한 continue 금지
- 환경 변수 직접 접근 금지: os.getenv 사용 금지 → settings.py(SSOT)만 사용
- 데이터 불충분/미준비는 명시적 HOLD로 처리하되, “불량 데이터(형식/범위 오류)”는 예외
- DB 업데이트는 state.db_writer(SSOT)로 통일
- 텔레그램은 async_worker로 위임(메인 루프 블로킹 금지)

변경 이력
--------------------------------------------------------
- 2026-03-03 (TRADE-GRADE, FIX):
  1) ENV 직접 접근 제거:
     - EXIT_SUPERVISOR_ENABLED os.getenv 제거 → settings.exit_supervisor_enabled만 사용
  2) 폴백 제거:
     - _trade_key / supervisor 입력에서 entry/qty 0.0 폴백 제거
     - candle volume 0.0 폴백 제거 (필드 누락이면 candle 자체를 “없음”으로 간주)
  3) 상태/필드 검증 강화:
     - trade.qty / trade.entry 누락/비정상은 즉시 예외(상태 오염)
  4) supervisor는 best-effort 유지:
     - key/필드 불충분하면 supervisor 해설만 스킵(거래 결정에 영향 없음)
========================================================
"""

import math
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from infra.async_worker import submit as submit_async
from infra.market_data_ws import (
    get_klines as ws_get_klines,
    get_klines_with_volume as ws_get_klines_with_vol,
)
from infra.telelog import log, send_tg
from execution.order_executor import close_position_market
from events.signals_logger import log_event, log_signal, log_skip_event, log_candle_snapshot
from state.db_writer import close_latest_open_trade_returning_id, record_trade_exit_snapshot
from state.trader_state import Trade

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
    STRICT(데이터):
    - WS 1m 우선, 없으면 5m (둘 다 WS이며 '데이터 대체'가 아닌 TF fallback)
    - candle row 형식이 불량하면 None(데이터 미준비)로 처리하여 HOLD 유도
    - volume 누락 시 0.0 폴백 금지 → None(미준비) 처리
    """
    try:
        candles = ws_get_klines_with_vol(symbol, "1m", 1)
        if not candles:
            candles = ws_get_klines_with_vol(symbol, "5m", 1)
    except Exception as e:
        log(f"{LOG_EXIT} kline fetch error symbol={symbol}: {type(e).__name__}:{e}")
        return None

    if not candles:
        return None

    row = candles[-1]
    if not isinstance(row, (list, tuple)) or len(row) < 6:
        return None

    try:
        ts = int(row[0])
        o = float(row[1])
        h = float(row[2])
        l = float(row[3])
        c = float(row[4])
        v = float(row[5])
    except (TypeError, ValueError):
        return None

    if ts <= 0 or o <= 0 or h <= 0 or l <= 0 or c <= 0:
        return None
    if v < 0 or not math.isfinite(v):
        return None
    return ts, o, h, l, c, v


def _get_15m_trend_dir(symbol: str) -> str:
    """
    BEST-EFFORT:
    - 데이터 부족/오류는 "" 반환(명시적 HOLD 판단에 사용: opposite=false)
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
        log(f"{LOG_PW}[TREND15] ws_get_klines error symbol={symbol}: {type(e).__name__}:{e}")
        return ""


def _is_opposite(trade_side: Any, trend_dir: str) -> bool:
    side = _dir_strict(trade_side)
    td = str(trend_dir or "").upper().strip()
    if not td:
        return False
    return (side == "LONG" and td == "SHORT") or (side == "SHORT" and td == "LONG")


def _runtime_thresholds(settings: Any) -> Dict[str, float]:
    """
    STRICT:
    - settings에 명시된 값만 사용한다.
    - 값이 없으면 예외(설정 누락은 운영 오류).
    """
    if not hasattr(settings, "exit_opposite_profit_take_pct"):
        raise RuntimeError("settings.exit_opposite_profit_take_pct missing (STRICT)")
    if not hasattr(settings, "exit_opposite_loss_cut_pct"):
        raise RuntimeError("settings.exit_opposite_loss_cut_pct missing (STRICT)")
    if not hasattr(settings, "exit_hard_stop_loss_pct"):
        raise RuntimeError("settings.exit_hard_stop_loss_pct missing (STRICT)")

    op_profit = _as_float(getattr(settings, "exit_opposite_profit_take_pct"), "settings.exit_opposite_profit_take_pct", min_value=0.0)
    op_loss = _as_float(getattr(settings, "exit_opposite_loss_cut_pct"), "settings.exit_opposite_loss_cut_pct", min_value=0.0)
    hard_stop = _as_float(getattr(settings, "exit_hard_stop_loss_pct"), "settings.exit_hard_stop_loss_pct", min_value=0.0)

    if op_profit <= 0 or op_loss <= 0:
        raise RuntimeError("exit thresholds must be positive (STRICT)")
    return {"op_profit": float(op_profit), "op_loss": float(op_loss), "hard_stop": float(hard_stop)}


def _exit_supervisor_enabled(settings: Any) -> bool:
    """
    STRICT:
    - ENV 직접 접근 금지
    - settings.exit_supervisor_enabled만 사용
    - 설정 키가 없으면 False(옵션 기능 기본 OFF)
    """
    return bool(getattr(settings, "exit_supervisor_enabled", False))


def _trade_key_strict(trade: Trade) -> str:
    """
    STRICT:
    - id가 있으면 id 기반.
    - id가 없으면 entry/qty를 반드시 요구(0 폴백 금지).
    """
    tid = getattr(trade, "id", None)
    if isinstance(tid, int) and tid > 0:
        return f"id:{tid}"

    sym = str(getattr(trade, "symbol", "")).strip()
    if not sym:
        raise RuntimeError("trade.symbol missing (STRICT)")

    side = str(getattr(trade, "side", "")).strip()
    if not side:
        raise RuntimeError("trade.side missing (STRICT)")

    entry = _as_float(getattr(trade, "entry", None), "trade.entry", min_value=0.0)
    qty = _as_float(getattr(trade, "qty", None), "trade.qty", min_value=0.0)
    if entry <= 0 or qty <= 0:
        raise RuntimeError("trade.entry/qty must be > 0 (STRICT)")

    return f"{sym}:{side}:{entry:.8f}:{qty:.8f}"


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

    try:
        key = _trade_key_strict(trade)
    except Exception as e:
        log(f"{LOG_EXIT}[SUP] trade key build failed -> narration skip: {type(e).__name__}:{e}")
        return

    now = time.time()
    cooldown = float(getattr(settings, "exit_supervisor_cooldown_sec", 180.0) or 180.0)
    last = _EXIT_SUP_LAST_CALL_TS.get(key)
    if last is not None and (now - last) < cooldown:
        return

    try:
        unified = build_unified_features(trade.symbol)
    except (UnifiedFeaturesError, Exception) as e:
        log(f"{LOG_EXIT}[SUP] unified_features build failed -> narration skip: {type(e).__name__}:{e}")
        return

    # STRICT: supervisor 입력에 0/None 폴백 금지
    entry_price = _as_float(getattr(trade, "entry", None), "trade.entry", min_value=0.0)
    qty = _as_float(getattr(trade, "qty", None), "trade.qty", min_value=0.0)
    if entry_price <= 0 or qty <= 0:
        log(f"{LOG_EXIT}[SUP] invalid entry/qty -> narration skip")
        return

    decision_id = f"exit-{trade.symbol}-{int(candle_ts_ms)}"
    quant_pre = {
        "scenario": scenario,
        "symbol": trade.symbol,
        "side": str(trade.side),
        "entry_price": float(entry_price),
        "qty": float(qty),
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
            "narration": None
            if res.narration is None
            else {"title": res.narration.title, "message": res.narration.message, "tone": res.narration.tone},
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

    close_position_market(symbol, open_side, float(qty))

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

    # STRICT: trade 상태(필수 필드) 오염은 즉시 예외
    qty = _as_float(getattr(trade, "qty", None), "trade.qty", min_value=0.0)
    entry = _as_float(getattr(trade, "entry", None), "trade.entry", min_value=0.0)
    if qty <= 0 or entry <= 0:
        raise RuntimeError("trade.qty/trade.entry must be > 0 (STRICT)")

    last_candle = _get_last_candle_with_volume(trade.symbol)
    if last_candle is None:
        # 명시적 HOLD: 데이터 미준비(폴백 없음)
        try:
            log_skip_event(
                symbol=trade.symbol,
                regime=regime_label,
                source="exit_engine",
                side=getattr(trade, "side", ""),
                reason="ws_candle_not_ready",
                extra={"scenario": scenario},
            )
        except Exception as e:
            log(f"{LOG_EXIT}[HOLD][SKIP_LOG] failed symbol={trade.symbol}: {e}")
        log(f"{LOG_EXIT} no recent WS candle → HOLD (symbol={trade.symbol})")
        return False

    candle_ts, o, h, l, c, v = last_candle
    if c <= 0:
        raise RuntimeError("last_close must be > 0 (STRICT)")

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