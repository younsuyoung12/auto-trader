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
- 2026-03-13:
  1) FIX(CONTRACT): canonical runtime 함수 maybe_exit_quant_strict() 추가, maybe_exit_with_gpt()는 호환 wrapper로 유지
  2) FIX(STRICT): trade.source / settings.exit_supervisor_enabled / settings.exit_supervisor_cooldown_sec 필수 계약화
  3) FIX(STRICT): UNKNOWN / 기본 쿨다운 값 등 숨은 fallback 제거
  4) FIX(BOUNDARY): supervisor는 감사/해설 전용, 청산 결정 경로와 명확히 분리
- 2026-03-11:
  1) FIX(ROOT-CAUSE): 현재 포지션 손익/청산 수량을 trade.qty가 아니라 trade.remaining_qty 기준으로 수정
  2) FIX(ROOT-CAUSE): 최종 청산 DB 기록 손익을 trade.realized_pnl_usdt + 남은 수량 청산 손익으로 집계
  3) FIX(STRICT): trade.remaining_qty 계약 검증 추가
  4) FIX(CONTRACT): supervisor 입력 qty를 현재 남은 포지션 기준으로 정리
- 2026-03-05 (TRADE-GRADE):
  1) 능동형 익절(Trend Extension / Hybrid Exit) 추가:
     - TP 근접(arm) 구간에서 추세 강하면 TP 주문을 취소하고 HOLD로 전환
     - 추세 약화 시(또는 보호 바닥선 이탈 시) 시장가 청산으로 이익 확정
  2) 손절(SL) 로직은 절대 변경하지 않는다.
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
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from infra.async_worker import submit as submit_async
from infra.market_data_ws import (
    get_klines as ws_get_klines,
    get_klines_with_volume as ws_get_klines_with_vol,
)
from infra.telelog import log, send_tg
from execution.order_executor import cancel_order_safe, close_position_market
from events.signals_logger import (
    log_candle_snapshot,
    log_event,
    log_signal,
    log_skip_event,
)
from state.db_writer import (
    close_latest_open_trade_returning_id,
    record_trade_exit_snapshot,
)
from state.trader_state import Trade

# (옵션) GPT Supervisor: 감사/해설 전용
from strategy.gpt_supervisor import GptSupervisorError, run_gpt_supervisor

# (옵션) unified features: supervisor 입력용(없으면 해설 생략)
from strategy.unified_features_builder import UnifiedFeaturesError, build_unified_features


LOG_PW = "[PW_BINANCE]"
LOG_EXIT = "[PW_BINANCE][EXIT]"

EXIT_CHECK_INTERVAL_SEC: float = 10.0

# supervisor 쿨다운(포지션별)
_EXIT_SUP_LAST_CALL_TS: Dict[str, float] = {}
_RECENT_SUP_MSGS: List[str] = []  # repetition 방지용(최근 메시지)

# ─────────────────────────────────────────────
# Trend Extension (능동형 익절) — TRADE-GRADE
# ─────────────────────────────────────────────
# NOTE:
# - TP는 거래소 조건부 주문으로 설치되어 있으므로, "익절 미루기"를 하려면 TP 주문을 취소해야 한다.
# - exit_engine은 1m cadence로 동작하므로, TP 정확 도달 시점에 취소하면 늦을 수 있다.
#   따라서 TP의 일정 비율(arm ratio) 도달 시점에 "추세 강함"이면 취소+HOLD로 전환한다.
#
# 손절(SL)은 절대 변경하지 않는다.
_TREND_EXT_REGIME_SCORE_TH: float = 65.0
_TREND_EXT_MICRO_RISK_MAX: float = 40.0

# TP 근접 시점(arm). 0.6% TP라면 0.54% 도달 시점부터 개입 가능.
_TREND_EXT_ARM_RATIO: float = 0.90

# TP 취소 후 이익 보호 바닥선: TP의 33% (0.6% TP라면 약 +0.198%)
_TREND_EXT_FLOOR_RATIO: float = 0.33

# 상태: trade_key -> state
_TREND_EXT_STATE: Dict[str, Dict[str, Any]] = {}
_TREND_EXT_LAST_NOTICE_TS: Dict[str, float] = {}
_TREND_EXT_NOTICE_COOLDOWN_SEC: float = 120.0

# cancel not found codes (-2011/-2013) detection
_CODE_RE = re.compile(r"code=([-]?\d+)")


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


def _as_bool(v: Any, name: str) -> bool:
    if not isinstance(v, bool):
        raise RuntimeError(f"{name} must be bool (STRICT)")
    return bool(v)


def _require_nonempty_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        raise RuntimeError(f"{name} must not be empty (STRICT)")
    return s


def _require_setting_bool(settings: Any, name: str) -> bool:
    if settings is None:
        raise RuntimeError("settings is required (STRICT)")
    if not hasattr(settings, name):
        raise RuntimeError(f"settings.{name} missing (STRICT)")
    return _as_bool(getattr(settings, name), f"settings.{name}")


def _require_setting_float(settings: Any, name: str, *, min_value: Optional[float] = None) -> float:
    if settings is None:
        raise RuntimeError("settings is required (STRICT)")
    if not hasattr(settings, name):
        raise RuntimeError(f"settings.{name} missing (STRICT)")
    return _as_float(getattr(settings, name), f"settings.{name}", min_value=min_value)


def _trade_symbol_strict(trade: Trade) -> str:
    sym = _require_nonempty_str(getattr(trade, "symbol", None), "trade.symbol").replace("-", "").replace("/", "").upper()
    return sym


def _trade_source_strict(trade: Trade) -> str:
    return _require_nonempty_str(getattr(trade, "source", None), "trade.source")


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


def _trade_direction_strict(trade: Trade) -> str:
    return _dir_strict(getattr(trade, "side", None))


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


def _remaining_qty_strict(trade: Trade) -> float:
    qty_total = _as_float(getattr(trade, "qty", None), "trade.qty", min_value=0.0)
    remaining_qty = _as_float(getattr(trade, "remaining_qty", None), "trade.remaining_qty", min_value=0.0)

    if qty_total <= 0:
        raise RuntimeError("trade.qty must be > 0 (STRICT)")
    if remaining_qty <= 0:
        raise RuntimeError("trade.remaining_qty must be > 0 (STRICT)")
    if remaining_qty - qty_total > 1e-12:
        raise RuntimeError("trade.remaining_qty must be <= trade.qty (STRICT)")

    return float(remaining_qty)


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

    op_profit = _as_float(
        getattr(settings, "exit_opposite_profit_take_pct"),
        "settings.exit_opposite_profit_take_pct",
        min_value=0.0,
    )
    op_loss = _as_float(
        getattr(settings, "exit_opposite_loss_cut_pct"),
        "settings.exit_opposite_loss_cut_pct",
        min_value=0.0,
    )
    hard_stop = _as_float(
        getattr(settings, "exit_hard_stop_loss_pct"),
        "settings.exit_hard_stop_loss_pct",
        min_value=0.0,
    )

    if op_profit <= 0 or op_loss <= 0:
        raise RuntimeError("exit thresholds must be positive (STRICT)")
    return {"op_profit": float(op_profit), "op_loss": float(op_loss), "hard_stop": float(hard_stop)}


def _exit_supervisor_enabled_strict(settings: Any) -> bool:
    """
    STRICT:
    - settings.exit_supervisor_enabled는 반드시 명시되어야 한다.
    - 옵션 기능은 caller가 명시적으로 False를 넣어 꺼야 한다.
    """
    return _require_setting_bool(settings, "exit_supervisor_enabled")


def _exit_supervisor_cooldown_sec_strict(settings: Any) -> float:
    return _require_setting_float(settings, "exit_supervisor_cooldown_sec", min_value=0.0)


def _trade_key_strict(trade: Trade) -> str:
    """
    STRICT:
    - id가 있으면 id 기반.
    - id가 없으면 entry/qty를 반드시 요구(0 폴백 금지).
    """
    tid = getattr(trade, "id", None)
    if isinstance(tid, int) and tid > 0:
        return f"id:{tid}"

    sym = _trade_symbol_strict(trade)
    side = _require_nonempty_str(getattr(trade, "side", None), "trade.side")

    entry = _as_float(getattr(trade, "entry", None), "trade.entry", min_value=0.0)
    qty = _as_float(getattr(trade, "qty", None), "trade.qty", min_value=0.0)
    if entry <= 0 or qty <= 0:
        raise RuntimeError("trade.entry/qty must be > 0 (STRICT)")

    return f"{sym}:{side}:{entry:.8f}:{qty:.8f}"


def _extract_code(err: Exception) -> Optional[int]:
    m = _CODE_RE.search(str(err))
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _is_order_not_found(err: Exception) -> bool:
    code = _extract_code(err)
    if code in (-2011, -2013):
        return True
    s = str(err).lower()
    if "order does not exist" in s:
        return True
    return False


def _tp_pct_strict(settings: Any, trade: Trade) -> float:
    """
    STRICT:
    - 기본 tp_pct는 settings.tp_pct (SSOT)
    - trade.tp_pct가 있으면(포지션별) 우선 적용 가능(값 검증 필수)
    """
    if not hasattr(settings, "tp_pct"):
        raise RuntimeError("settings.tp_pct missing (STRICT)")
    tp = _as_float(getattr(settings, "tp_pct"), "settings.tp_pct", min_value=0.0)
    if tp <= 0:
        raise RuntimeError("settings.tp_pct must be > 0 (STRICT)")

    tp_trade = getattr(trade, "tp_pct", None)
    if tp_trade is None:
        return float(tp)

    tp2 = _as_float(tp_trade, "trade.tp_pct", min_value=0.0)
    if tp2 <= 0:
        raise RuntimeError("trade.tp_pct must be > 0 (STRICT)")
    return float(tp2)


def _sl_pct_strict(settings: Any) -> float:
    """
    STRICT:
    - 손절(SL)은 settings.sl_pct만 사용(절대 변경 금지)
    """
    if not hasattr(settings, "sl_pct"):
        raise RuntimeError("settings.sl_pct missing (STRICT)")
    sl = _as_float(getattr(settings, "sl_pct"), "settings.sl_pct", min_value=0.0)
    if sl <= 0:
        raise RuntimeError("settings.sl_pct must be > 0 (STRICT)")
    return float(sl)


def _meta_scores_best_effort(trade: Trade) -> Optional[Tuple[float, float]]:
    """
    TRADE-GRADE:
    - meta 누락은 '데이터 미준비'로 간주하여 None 반환(명시적 HOLD 유도).
    - 형식/범위 오류는 예외(불량 데이터).
    """
    meta = getattr(trade, "meta", None)
    if meta is None:
        return None
    if not isinstance(meta, dict):
        raise RuntimeError("trade.meta must be dict (STRICT)")

    if "regime_score" not in meta or "micro_score_risk" not in meta:
        return None

    regime_score = _as_float(meta.get("regime_score"), "meta.regime_score")
    micro_score_risk = _as_float(meta.get("micro_score_risk"), "meta.micro_score_risk")

    if not (0.0 <= regime_score <= 100.0):
        raise RuntimeError(f"meta.regime_score out of range (STRICT): {regime_score}")
    if not (0.0 <= micro_score_risk <= 100.0):
        raise RuntimeError(f"meta.micro_score_risk out of range (STRICT): {micro_score_risk}")

    return float(regime_score), float(micro_score_risk)


def _trend_strong_from_scores(regime_score: float, micro_score_risk: float) -> bool:
    return (float(regime_score) > _TREND_EXT_REGIME_SCORE_TH) and (float(micro_score_risk) < _TREND_EXT_MICRO_RISK_MAX)


def _cancel_attached_orders_best_effort(*, trade: Trade, settings: Any) -> None:
    """
    BEST-EFFORT:
    - 포지션 청산 시 남아있는 TP/SL 주문이 이후에 오동작하지 않도록 취소 시도.
    - 실패해도 거래 흐름(청산)을 깨지 않는다.
    """
    sym = _trade_symbol_strict(trade)

    for name in ("tp_order_id", "sl_order_id"):
        oid = getattr(trade, name, None)
        if oid is None:
            continue
        s = str(oid).strip()
        if not s:
            continue
        try:
            cancel_order_safe(sym, s, settings=settings)
        except Exception as e:
            if _is_order_not_found(e):
                continue
            log(f"{LOG_EXIT}[CANCEL][WARN] cancel {name} failed: {type(e).__name__}:{e}")


def _cancel_tp_for_trend_extension_strict(*, trade: Trade, settings: Any) -> bool:
    """
    STRICT(결정 영향):
    - TP 확장을 위해서는 TP 주문 취소가 필요하다.
    - tp_order_id가 없으면 확장 불가 -> False 반환.
    - 취소 실패(미정의 오류) 시 확장 금지 -> False 반환(조용한 진행 금지, 로그/이벤트 기록).
    """
    sym = _trade_symbol_strict(trade)
    regime_label = _trade_source_strict(trade)
    side_dir = _trade_direction_strict(trade)

    tp_oid = getattr(trade, "tp_order_id", None)
    if tp_oid is None or not str(tp_oid).strip():
        return False

    try:
        cancel_order_safe(sym, str(tp_oid).strip(), settings=settings)
        log_event(
            "trend_ext_tp_cancelled",
            symbol=sym,
            regime=regime_label,
            source="exit_engine.trend_extension",
            side=side_dir,
            price=None,
            reason="tp_cancelled_for_trend_extension",
            extra_json={"tp_order_id": str(tp_oid).strip()},
        )
        return True
    except Exception as e:
        if _is_order_not_found(e):
            return False

        log_event(
            "trend_ext_tp_cancel_fail",
            symbol=sym,
            regime=regime_label,
            source="exit_engine.trend_extension",
            side=side_dir,
            price=None,
            reason="tp_cancel_failed",
            extra_json={"err": f"{type(e).__name__}:{str(e)[:200]}", "tp_order_id": str(tp_oid).strip()},
        )
        log(f"{LOG_EXIT}[TREND_EXT][WARN] TP cancel failed (symbol={sym}): {type(e).__name__}:{e}")
        return False


def _trend_ext_notice_once(key: str, msg: str) -> None:
    now = time.time()
    last = _TREND_EXT_LAST_NOTICE_TS.get(key)
    if last is not None and (now - last) < _TREND_EXT_NOTICE_COOLDOWN_SEC:
        return
    _TREND_EXT_LAST_NOTICE_TS[key] = now
    _submit_tg(msg)


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
    if not _exit_supervisor_enabled_strict(settings):
        return

    try:
        key = _trade_key_strict(trade)
    except Exception as e:
        log(f"{LOG_EXIT}[SUP] trade key build failed -> narration skip: {type(e).__name__}:{e}")
        return

    now = time.time()
    cooldown = _exit_supervisor_cooldown_sec_strict(settings)
    last = _EXIT_SUP_LAST_CALL_TS.get(key)
    if last is not None and (now - last) < cooldown:
        return

    try:
        unified = build_unified_features(_trade_symbol_strict(trade))
    except (UnifiedFeaturesError, Exception) as e:
        log(f"{LOG_EXIT}[SUP] unified_features build failed -> narration skip: {type(e).__name__}:{e}")
        return

    entry_price = _as_float(getattr(trade, "entry", None), "trade.entry", min_value=0.0)
    qty_total = _as_float(getattr(trade, "qty", None), "trade.qty", min_value=0.0)
    remaining_qty = _remaining_qty_strict(trade)
    realized_pnl_usdt = _as_float(getattr(trade, "realized_pnl_usdt", None), "trade.realized_pnl_usdt")

    if entry_price <= 0 or qty_total <= 0 or remaining_qty <= 0:
        log(f"{LOG_EXIT}[SUP] invalid entry/qty/remaining_qty -> narration skip")
        return

    symbol = _trade_symbol_strict(trade)
    side_dir = _trade_direction_strict(trade)
    regime_label = _trade_source_strict(trade)

    decision_id = f"exit-{symbol}-{int(candle_ts_ms)}"
    quant_pre = {
        "scenario": scenario,
        "symbol": symbol,
        "side": str(getattr(trade, "side", None)),
        "entry_price": float(entry_price),
        "qty_total": float(qty_total),
        "remaining_qty": float(remaining_qty),
        "realized_pnl_usdt": float(realized_pnl_usdt),
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
            symbol=symbol,
            regime=regime_label,
            source="exit_engine.supervisor",
            side=side_dir,
            price=float(last_price),
            reason="supervisor_failed",
            extra_json={"error": str(e)[:240]},
        )
        return

    _EXIT_SUP_LAST_CALL_TS[key] = now

    log_event(
        "on_exit_supervisor",
        symbol=symbol,
        regime=regime_label,
        source="exit_engine.supervisor",
        side=side_dir,
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
    settings: Any,
    close_price: float,
    candle_ts_ms: int,
    reason: str,
    regime_label: str,
) -> None:
    """
    STRICT:
    - (best-effort) TP/SL 등 잔존 주문 취소
    - 시장가 청산 → bt_trades close → bt_trade_exit_snapshots 기록
    - 부분청산 누적 손익이 있으면 최종 손익에 반드시 합산
    """
    symbol = _trade_symbol_strict(trade)
    open_side = _open_side_strict(getattr(trade, "side", None))
    entry = _as_float(getattr(trade, "entry", None), "trade.entry", min_value=0.0)
    qty_total = _as_float(getattr(trade, "qty", None), "trade.qty", min_value=0.0)
    remaining_qty = _remaining_qty_strict(trade)
    realized_pnl_usdt = _as_float(getattr(trade, "realized_pnl_usdt", None), "trade.realized_pnl_usdt")
    close_price_f = _as_float(close_price, "close_price", min_value=0.0)

    if entry <= 0 or qty_total <= 0 or remaining_qty <= 0 or close_price_f <= 0:
        raise RuntimeError("entry/qty_total/remaining_qty/close_price must be > 0 (STRICT)")

    pnl_remaining = _pnl_usdt(open_side=open_side, entry=entry, last=close_price_f, qty=remaining_qty)
    total_pnl = float(realized_pnl_usdt + pnl_remaining)
    pnl_pct_val = _pnl_pct(pnl=total_pnl, entry=entry, qty=qty_total)

    _cancel_attached_orders_best_effort(trade=trade, settings=settings)

    close_position_market(symbol, open_side, float(remaining_qty))

    exit_dt = datetime.fromtimestamp(int(candle_ts_ms) / 1000.0, tz=timezone.utc)
    closed_trade_id = close_latest_open_trade_returning_id(
        symbol=symbol,
        side=open_side,
        exit_price=float(close_price_f),
        exit_ts=exit_dt,
        pnl_usdt=float(total_pnl),
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
        pnl=float(total_pnl),
        close_reason=str(reason)[:200],
        exit_atr_pct=None,
        exit_trend_strength=None,
        exit_volume_zscore=None,
        exit_depth_ratio=None,
    )


def maybe_exit_quant_strict(
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
    regime_label = _trade_source_strict(trade)
    symbol = _trade_symbol_strict(trade)
    open_side = _open_side_strict(getattr(trade, "side", None))
    side_dir = _trade_direction_strict(trade)

    qty_total = _as_float(getattr(trade, "qty", None), "trade.qty", min_value=0.0)
    remaining_qty = _remaining_qty_strict(trade)
    entry = _as_float(getattr(trade, "entry", None), "trade.entry", min_value=0.0)
    if qty_total <= 0 or remaining_qty <= 0 or entry <= 0:
        raise RuntimeError("trade.qty/trade.remaining_qty/trade.entry must be > 0 (STRICT)")

    last_candle = _get_last_candle_with_volume(symbol)
    if last_candle is None:
        try:
            log_skip_event(
                symbol=symbol,
                regime=regime_label,
                source="exit_engine",
                side=side_dir,
                reason="ws_candle_not_ready",
                extra={"scenario": scenario},
            )
        except Exception as e:
            log(f"{LOG_EXIT}[HOLD][SKIP_LOG] failed symbol={symbol}: {e}")
        log(f"{LOG_EXIT} no recent WS candle → HOLD (symbol={symbol})")
        return False

    candle_ts, o, h, l, c, v = last_candle
    if c <= 0:
        raise RuntimeError("last_close must be > 0 (STRICT)")

    try:
        log_candle_snapshot(
            symbol=symbol,
            tf="1m",
            candle_ts=int(candle_ts),
            open_=o,
            high=h,
            low=l,
            close=c,
            volume=v,
            strategy_type=regime_label,
            direction=side_dir,
            extra=f"exit_check=1;scenario={scenario};market=binance_futures_ws",
        )
    except Exception as e:
        log(f"{LOG_EXIT} candle snapshot log failed symbol={symbol}: {e}")

    pnl = _pnl_usdt(open_side=open_side, entry=float(entry), last=float(c), qty=float(remaining_qty))
    pnl_pct_val = _pnl_pct(pnl=pnl, entry=float(entry), qty=float(qty_total))

    trend15_dir = _get_15m_trend_dir(symbol)
    opposite = _is_opposite(getattr(trade, "side", None), trend15_dir) if trend15_dir else False

    th = _runtime_thresholds(settings)

    if th["hard_stop"] > 0.0 and pnl_pct_val <= -float(th["hard_stop"]):
        reason = "runtime_hard_stop_loss"
        _submit_tg(f"🧯 [EXIT][HARD_STOP] {symbol} {open_side} pnl={pnl:.4f}USDT ({pnl_pct_val*100:.2f}%)")
        log_signal(
            event="CLOSE",
            symbol=symbol,
            strategy_type=regime_label,
            direction=side_dir,
            price=float(c),
            qty=float(remaining_qty),
            reason=reason,
            pnl=float(pnl),
        )
        _close_and_record_strict(
            trade=trade,
            settings=settings,
            close_price=float(c),
            candle_ts_ms=int(candle_ts),
            reason=reason,
            regime_label=regime_label,
        )
        try:
            k = _trade_key_strict(trade)
            _TREND_EXT_STATE.pop(k, None)
        except Exception:
            pass
        return True

    if opposite and pnl_pct_val >= float(th["op_profit"]):
        reason = "runtime_opposite_profit_take"
        _submit_tg(f"✅ [EXIT][OPPOSITE_TAKE] {symbol} {open_side} pnl={pnl:.4f}USDT ({pnl_pct_val*100:.2f}%)")
        log_signal(
            event="CLOSE",
            symbol=symbol,
            strategy_type=regime_label,
            direction=side_dir,
            price=float(c),
            qty=float(remaining_qty),
            reason=reason,
            pnl=float(pnl),
        )
        _close_and_record_strict(
            trade=trade,
            settings=settings,
            close_price=float(c),
            candle_ts_ms=int(candle_ts),
            reason=reason,
            regime_label=regime_label,
        )
        try:
            k = _trade_key_strict(trade)
            _TREND_EXT_STATE.pop(k, None)
        except Exception:
            pass
        return True

    if opposite and pnl_pct_val <= -float(th["op_loss"]):
        reason = "runtime_opposite_loss_cut"
        _submit_tg(f"⚠️ [EXIT][OPPOSITE_CUT] {symbol} {open_side} pnl={pnl:.4f}USDT ({pnl_pct_val*100:.2f}%)")
        log_signal(
            event="CLOSE",
            symbol=symbol,
            strategy_type=regime_label,
            direction=side_dir,
            price=float(c),
            qty=float(remaining_qty),
            reason=reason,
            pnl=float(pnl),
        )
        _close_and_record_strict(
            trade=trade,
            settings=settings,
            close_price=float(c),
            candle_ts_ms=int(candle_ts),
            reason=reason,
            regime_label=regime_label,
        )
        try:
            k = _trade_key_strict(trade)
            _TREND_EXT_STATE.pop(k, None)
        except Exception:
            pass
        return True

    sl_pct = _sl_pct_strict(settings)
    if pnl_pct_val <= -float(sl_pct):
        reason = "runtime_sl_fixed"
        _submit_tg(f"❌ [EXIT][SL] {symbol} {open_side} pnl={pnl:.4f}USDT ({pnl_pct_val*100:.2f}%)")
        log_signal(
            event="CLOSE",
            symbol=symbol,
            strategy_type=regime_label,
            direction=side_dir,
            price=float(c),
            qty=float(remaining_qty),
            reason=reason,
            pnl=float(pnl),
        )
        _close_and_record_strict(
            trade=trade,
            settings=settings,
            close_price=float(c),
            candle_ts_ms=int(candle_ts),
            reason=reason,
            regime_label=regime_label,
        )
        try:
            k = _trade_key_strict(trade)
            _TREND_EXT_STATE.pop(k, None)
        except Exception:
            pass
        return True

    tp_pct = _tp_pct_strict(settings, trade)
    key = _trade_key_strict(trade)

    st = _TREND_EXT_STATE.get(key)
    if isinstance(st, dict) and st.get("active") is True:
        floor_pct = _as_float(st.get("floor_pct"), "trend_ext.floor_pct", min_value=0.0)
        max_seen = _as_float(st.get("max_pnl_pct"), "trend_ext.max_pnl_pct", min_value=0.0)

        if pnl_pct_val > max_seen and math.isfinite(pnl_pct_val):
            st["max_pnl_pct"] = float(pnl_pct_val)

        scores = _meta_scores_best_effort(trade)

        if pnl_pct_val <= floor_pct:
            reason = "trend_ext_floor_exit"
            _submit_tg(f"💰 [EXIT][TREND_EXT_FLOOR] {symbol} pnl={pnl:.4f}USDT ({pnl_pct_val*100:.2f}%)")
            log_signal(
                event="CLOSE",
                symbol=symbol,
                strategy_type=regime_label,
                direction=side_dir,
                price=float(c),
                qty=float(remaining_qty),
                reason=reason,
                pnl=float(pnl),
            )
            _close_and_record_strict(
                trade=trade,
                settings=settings,
                close_price=float(c),
                candle_ts_ms=int(candle_ts),
                reason=reason,
                regime_label=regime_label,
            )
            _TREND_EXT_STATE.pop(key, None)
            return True

        if pnl_pct_val >= tp_pct and scores is not None:
            rs, msr = scores
            if not _trend_strong_from_scores(rs, msr):
                reason = "trend_ext_exit_on_weakness"
                _submit_tg(f"💰 [EXIT][TREND_EXT_WEAK] {symbol} pnl={pnl:.4f}USDT ({pnl_pct_val*100:.2f}%)")
                log_signal(
                    event="CLOSE",
                    symbol=symbol,
                    strategy_type=regime_label,
                    direction=side_dir,
                    price=float(c),
                    qty=float(remaining_qty),
                    reason=reason,
                    pnl=float(pnl),
                )
                _close_and_record_strict(
                    trade=trade,
                    settings=settings,
                    close_price=float(c),
                    candle_ts_ms=int(candle_ts),
                    reason=reason,
                    regime_label=regime_label,
                )
                _TREND_EXT_STATE.pop(key, None)
                return True

        try:
            log_skip_event(
                symbol=symbol,
                regime=regime_label,
                source="exit_engine.trend_extension",
                side=side_dir,
                reason="trend_ext_hold",
                extra={
                    "scenario": scenario,
                    "pnl_usdt": float(pnl),
                    "pnl_pct": float(pnl_pct_val),
                    "max_pnl_pct": float(st.get("max_pnl_pct")),
                    "floor_pct": float(floor_pct),
                    "remaining_qty": float(remaining_qty),
                },
            )
        except Exception as e:
            log(f"{LOG_EXIT}[HOLD][TREND_EXT][SKIP_LOG] failed symbol={symbol}: {e}")
        return False

    arm_pct = float(tp_pct) * float(_TREND_EXT_ARM_RATIO)
    scores2 = _meta_scores_best_effort(trade)
    if (scores2 is not None) and (pnl_pct_val >= arm_pct):
        rs2, msr2 = scores2
        if _trend_strong_from_scores(rs2, msr2):
            cancelled = _cancel_tp_for_trend_extension_strict(trade=trade, settings=settings)
            if cancelled:
                floor_pct = float(tp_pct) * float(_TREND_EXT_FLOOR_RATIO)
                _TREND_EXT_STATE[key] = {
                    "active": True,
                    "started_ts": time.time(),
                    "tp_pct": float(tp_pct),
                    "arm_pct": float(arm_pct),
                    "floor_pct": float(floor_pct),
                    "max_pnl_pct": float(max(0.0, pnl_pct_val)),
                }
                _trend_ext_notice_once(
                    key,
                    f"🟦 [HOLD][TREND_EXT] {symbol} 추세 강함 감지 → TP 취소 후 홀딩 "
                    f"(pnl={pnl_pct_val*100:.2f}%, floor≈{floor_pct*100:.2f}%, remaining_qty={remaining_qty:.6f})",
                )
                return False
            log(f"{LOG_EXIT}[TREND_EXT] arm met but tp cancel failed/not-possible -> no extension (symbol={symbol})")

    try:
        log_skip_event(
            symbol=symbol,
            regime=regime_label,
            source="exit_engine.quant",
            side=side_dir,
            reason="runtime_hold",
            extra={
                "scenario": scenario,
                "pnl_usdt": float(pnl),
                "pnl_pct": float(pnl_pct_val),
                "trend15_dir": trend15_dir,
                "opposite": int(opposite),
                "thresholds": th,
                "remaining_qty": float(remaining_qty),
            },
        )
    except Exception as e:
        log(f"{LOG_EXIT}[HOLD][SKIP_LOG] failed symbol={symbol}: {e}")

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


def maybe_exit_with_gpt(
    trade: Trade,
    settings: Any,
    *,
    scenario: str = "GENERIC_EXIT_CHECK",
) -> bool:
    """
    호환 wrapper.
    실제 청산 판단은 maybe_exit_quant_strict()가 수행한다.
    """
    return maybe_exit_quant_strict(
        trade=trade,
        settings=settings,
        scenario=scenario,
    )


__all__ = [
    "maybe_exit_quant_strict",
    "maybe_exit_with_gpt",
    "EXIT_CHECK_INTERVAL_SEC",
]