"""
========================================================
FILE: execution/execution_engine.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할
- strategy.Signal을 받아 “실행”만 한다.
- 가드(수동/볼륨/점프/스프레드/하드리스크) → 주문 실행 → DB 영속화 → 알림
- 폴백 금지: 누락/불일치/모호성은 즉시 예외

핵심 원칙
- 예외 삼키기 금지(단, 관측/알림 목적의 예외는 별도 이벤트로 기록 후 흐름에 영향 X)
- “SKIP”은 정상 흐름(리스크/가드에 의해 실행하지 않는 결정)으로 처리한다.
- “ENTER”는 필요한 메타/필드가 1개라도 없으면 즉시 중단한다.
- 동시 실행(레이스) 금지: execute()는 전역 단일 락으로 직렬화한다.
- 알림(텔레그램)은 메인 실행을 블로킹하면 안 된다(비동기 위임). 실패해도 실행 흐름을 망치지 않는다.

PATCH NOTES — 2026-03-09 (TRADE-GRADE)
- 실행 계약 정합(ROOT-CAUSE)
  - order_executor.open_position_with_tp_sl() 성공 반환 Trade 는
    반드시 reconciliation_status="PROTECTION_VERIFIED" 상태여야 한다.
  - execution_engine 는 과도기 상태(ENTRY_FILLED)를 허용하지 않는다.
  - order_executor 가 검증 완료한 last_synced_at 을 우선 보존해 DB 영속화와 이벤트 기록에 사용한다.
- 실행 레이어 상태 모델 정리
  - _ALLOWED_RECONCILIATION_STATUSES 를 PROTECTION_VERIFIED 단일 상태로 축소
  - 보호주문 검증이 끝나지 않은 Trade 는 실행 성공으로 취급하지 않음
- 기존 기능 삭제 없음

PATCH NOTES — 2026-03-07 (TRADE-GRADE)
- POSITION 이벤트 기록 보강(STRICT):
  - 실제 진입 성공 후 bt_events 에 event_type="POSITION" 이벤트를 추가 기록
  - TEST_DRY_RUN 경로도 is_test=True POSITION 이벤트를 기록
  - 기존 주문/DB/알림/리턴 흐름은 변경하지 않음
  - 대시보드 /api/position/current 의 STRICT 조회 입력을 충족하도록 최소 이벤트만 추가

PATCH NOTES — 2026-03-06 (TRADE-GRADE)
- DECISION 이벤트 연결(STRICT):
  - NO_ENTRY / ENTRY 의사결정 이벤트를 bt_events 에 기록
  - 대시보드 "왜 진입 안하는지 / 왜 진입했는지" 패널용 payload 엄격 생성
  - 필수 결정 지표(entry_score / trend_strength / spread / orderbook_imbalance) 누락 시 즉시 예외

PATCH NOTES — 2026-03-04 (TRADE-GRADE)
- Invariant Guard 연결(STRICT):
  - ENTER 흐름에서 meta/signal 수학 무결성(0..1/finite/필수키)을 execute() 초기에 검증
  - notional/qty/slippage/spread 등 실행 수학값을 hard_risk_guard_check 이전에 재검증
  - 위반 시 폴백 없이 즉시 OrderFailed 로 중단

PATCH NOTES — 2026-03-03 (TRADE-GRADE)
- ENV 직접 접근 제거(SSOT 준수):
  - os.getenv 기반 TEST_* 플래그 제거
  - settings 객체의 test_* 필드로만 테스트 모드 제어(미존재 시 안전 기본값 False/0.0)
- 운영 사고 방지:
  - test_force_enter / test_bypass_guards / test_fake_available_usdt 는 test_dry_run=1에서만 허용(아니면 즉시 예외)
- 예외 삼키기 제거:
  - trade.order_state / trade.id setattr 실패를 조용히 무시하지 않음(실패 시 예외 전파)
- 폴백 제거:
  - quant_final_decision 기본값 "ENTER" 강제 주입 제거(존재할 때만 기록)

PATCH NOTES — 2026-03-03 (TRADE-GRADE)
- bt_trade_snapshots 확장 컬럼 저장 연결:
  - meta에서 Decision/Micro/EV/AutoBlock 값을 읽어 record_trade_snapshot에 전달
  - exec_expected_price/exec_filled_avg_price/exec_slippage_pct 즉시 저장(대기 불필요)
- Execution Quality 이벤트 기록 비동기화:
  - 체결 후 t+1s/t+3s/t+5s markPrice 조회는 async_worker로 위임(메인 execute 블로킹 제거)
  - bt_events에 on_execution_quality / on_execution_quality_fail 기록

PATCH NOTES — 2026-03-02 (UPGRADE)
- Telegram I/O 비동기화:
  - infra/async_worker.submit()로 send_tg / send_skip_tg를 위임(메인 실행 블로킹 제거)
- bt_trade_snapshots에 DD 영속화 저장(STRICT):
  - meta.equity_current_usdt / meta.equity_peak_usdt / meta.dd_pct 를 STRICT로 요구
- deterministic entry_client_order_id 생성/주입:
  - 동일 signal 재시도 시 동일 newClientOrderId 생성 → 중복 진입 차단
- bt_trades 실행/복구 필드 영속화(STRICT):
  - entry_order_id / tp_order_id / sl_order_id
  - exchange_position_side / remaining_qty / realized_pnl_usdt
  - reconciliation_status / last_synced_at

PATCH NOTES — 2026-03-02 (FIX)
- Pylance "Trade is not defined" 해결:
  - from state.trader_state import Trade 추가

변경 이력
--------------------------------------------------------
- 2026-03-09:
  1) FIX(ROOT-CAUSE): order_executor 와 reconciliation_status 계약 단일화
     - real trade 성공 상태는 PROTECTION_VERIFIED만 허용
  2) FIX(STRICT): order_executor 가 기록한 last_synced_at(tz-aware)을 보존 사용
  3) 기존 주문/DB/알림/리턴 기능 삭제 없음
- 2026-03-07:
  1) POSITION 이벤트 기록 누락 수정
     - 실제 체결/TEST_DRY_RUN 양쪽 경로에 POSITION 이벤트 기록 추가
  2) 기존 주문/DB/알림/리턴 기능 변경 없음
- 2026-03-06:
  1) 보호주문 검증 연동 강화
     - order_executor 의 TP/SL 실가시성 검증 결과를 execution_engine 에서 필수 필드로 재검증
  2) TP 주문 필수화
     - ENTER 성공 후 trade.tp_order_id 누락 시 즉시 예외
  3) reconciliation_status 승격
     - 보호주문 검증 완료 상태를 PROTECTION_VERIFIED 로 영속화
  4) 보호주문 검증 완료 이벤트 추가
     - PROTECTION_VERIFIED 이벤트를 bt_events 에 기록
========================================================
"""

from __future__ import annotations

import logging
import math
import time
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

from events.signals_logger import log_event, log_signal, log_skip_event
from execution.exchange_api import get_available_usdt, get_balance_detail, req
from execution.execution_quality_engine import ExecutionQualityError, build_execution_quality_snapshot_obj
from execution.exceptions import OrderFailed, StateViolation
from execution.order_executor import (
    ProtectionOrderVerificationError,
    open_position_with_tp_sl,
)
from execution.order_state import OrderState
from execution.risk_manager import hard_risk_guard_check
from infra.async_worker import submit as submit_async
from infra.telelog import send_skip_tg, send_tg
from risk.entry_guards_ws import (
    check_manual_position_guard,
    check_price_jump_guard,
    check_spread_guard,
    check_volume_guard,
)
from strategy.signal import Signal
from state.db_writer import (
    close_latest_open_trade_returning_id,
    record_trade_exit_snapshot,
    record_trade_open_returning_id,
    record_trade_snapshot,
)
from state.trader_state import Trade

from execution.invariant_guard import (
    ExecutionInvariantInputs,
    InvariantViolation,
    SignalInvariantInputs,
    validate_execution_invariants_strict,
    validate_signal_from_meta_strict,
    validate_signal_invariants_strict,
)

logger = logging.getLogger(__name__)

# ============================================================
# Global single execution lock (STRICT)
# ============================================================
_EXECUTION_LOCK: Lock = Lock()

_ALLOWED_DECISION_ACTIONS = ("ENTRY", "NO_ENTRY", "HOLD", "EXIT")
_ALLOWED_RECONCILIATION_STATUSES = ("PROTECTION_VERIFIED",)


def _as_bool_strict(v: Any, name: str) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    if s in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise ValueError(f"{name} must be bool-like (got {v!r})")


def _as_float(
    value: Any,
    name: str,
    *,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a number (bool not allowed)")
    try:
        v = float(value)
    except Exception as e:
        raise ValueError(f"{name} must be a number") from e

    if not math.isfinite(v):
        raise ValueError(f"{name} must be finite")

    if min_value is not None and v < min_value:
        raise ValueError(f"{name} must be >= {min_value}")
    if max_value is not None and v > max_value:
        raise ValueError(f"{name} must be <= {max_value}")

    return v


def _opt_float_strict(value: Any, name: str) -> Optional[float]:
    if value is None:
        return None
    return float(_as_float(value, name))


def _require_nonempty_str(value: Any, name: str) -> str:
    if value is None:
        raise OrderFailed(f"{name} is required (STRICT)")
    if not isinstance(value, str):
        raise OrderFailed(f"{name} must be str (STRICT)")
    s = value.strip()
    if not s:
        raise OrderFailed(f"{name} must not be empty (STRICT)")
    return s


def _require_str_list(values: Any, name: str) -> List[str]:
    if values is None:
        raise OrderFailed(f"{name} is required (STRICT)")
    if not isinstance(values, list):
        raise OrderFailed(f"{name} must be list[str] (STRICT)")
    if not values:
        raise OrderFailed(f"{name} must not be empty (STRICT)")

    out: List[str] = []
    for idx, item in enumerate(values):
        out.append(_require_nonempty_str(item, f"{name}[{idx}]"))
    return out


def _require_tz_aware_datetime(value: Any, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise OrderFailed(f"{name} must be datetime (STRICT)")
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise OrderFailed(f"{name} must be timezone-aware datetime (STRICT)")
    return value


def _normalize_decision_action(action: str) -> str:
    a = _require_nonempty_str(action, "decision.action").upper()
    if a not in _ALLOWED_DECISION_ACTIONS:
        raise OrderFailed(f"unsupported decision action (STRICT): {a}")
    return a


def _meta_pick_first(meta: Dict[str, Any], keys: Tuple[str, ...]) -> Any:
    """
    STRICT:
    - 0/False 같은 값이 '없음'으로 오해되지 않도록, 키 존재 여부로만 선택한다.
    """
    for k in keys:
        if k in meta:
            return meta[k]
    return None


def _require_meta_numeric_from_keys(
    meta: Dict[str, Any],
    keys: Tuple[str, ...],
    name: str,
    *,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> float:
    raw = _meta_pick_first(meta, keys)
    if raw is None:
        raise OrderFailed(f"{name} is required (STRICT)")
    return float(_as_float(raw, name, min_value=min_value, max_value=max_value))


def _optional_meta_numeric_from_keys(
    meta: Dict[str, Any],
    keys: Tuple[str, ...],
    name: str,
    *,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> Optional[float]:
    raw = _meta_pick_first(meta, keys)
    if raw is None:
        return None
    return float(_as_float(raw, name, min_value=min_value, max_value=max_value))


def _calculate_exit_prices_strict(
    *,
    direction: str,
    entry_price: float,
    tp_pct: float,
    sl_pct: float,
) -> Tuple[float, float]:
    if entry_price <= 0:
        raise OrderFailed("entry_price must be > 0 (STRICT)")
    if tp_pct <= 0:
        raise OrderFailed("tp_pct must be > 0 (STRICT)")
    if sl_pct <= 0:
        raise OrderFailed("sl_pct must be > 0 (STRICT)")

    d = _require_nonempty_str(direction, "direction").upper()
    if d == "LONG":
        target_price = entry_price * (1.0 + tp_pct)
        stop_price = entry_price * (1.0 - sl_pct)
    elif d == "SHORT":
        target_price = entry_price * (1.0 - tp_pct)
        stop_price = entry_price * (1.0 + sl_pct)
    else:
        raise OrderFailed("direction must be LONG or SHORT (STRICT)")

    if target_price <= 0 or stop_price <= 0:
        raise OrderFailed("calculated target/stop price invalid (STRICT)")
    return float(target_price), float(stop_price)


def _build_decision_payload_strict(
    *,
    meta: Dict[str, Any],
    action: str,
    summary: str,
    reasons: List[str],
    signal_source: str,
    current_price: float,
    direction: str,
    spread_snapshot: Optional[float],
    position_qty: Optional[float],
    entry_price: Optional[float] = None,
    tp_pct: Optional[float] = None,
    sl_pct: Optional[float] = None,
) -> Dict[str, Any]:
    normalized_action = _normalize_decision_action(action)
    summary_s = _require_nonempty_str(summary, "decision.summary")
    reasons_l = _require_str_list(reasons, "decision.reasons")
    signal_source_s = _require_nonempty_str(signal_source, "decision.signal_source")

    current_price_f = float(_as_float(current_price, "decision.current_price", min_value=0.0))
    if current_price_f <= 0:
        raise OrderFailed("decision.current_price must be > 0 (STRICT)")

    entry_score = _require_meta_numeric_from_keys(
        meta,
        ("entry_score", "entryScore", "engine_total", "engine_total_score", "engine_total_score_v2"),
        "decision.entry_score",
    )
    trend_strength = _require_meta_numeric_from_keys(
        meta,
        ("trend_strength", "trend_score", "trendScore", "trend_strength_1m"),
        "decision.trend_strength",
    )
    orderbook_imbalance = _require_meta_numeric_from_keys(
        meta,
        ("orderbook_imbalance", "depth_ratio", "depth_imbalance", "orderbookImbalance"),
        "decision.orderbook_imbalance",
    )

    if spread_snapshot is not None:
        spread = float(_as_float(spread_snapshot, "decision.spread_snapshot", min_value=0.0))
    else:
        spread = _require_meta_numeric_from_keys(
            meta,
            ("spread", "spread_pct", "current_spread_pct"),
            "decision.spread",
            min_value=0.0,
        )

    threshold = _optional_meta_numeric_from_keys(
        meta,
        ("entry_score_threshold", "decision_threshold", "entry_threshold"),
        "decision.threshold",
    )
    exit_score = _optional_meta_numeric_from_keys(
        meta,
        ("exit_score", "exitScore"),
        "decision.exit_score",
    )

    regime = _meta_pick_first(meta, ("regime",))
    regime_s = _require_nonempty_str(regime, "meta.regime") if regime is not None else None

    payload: Dict[str, Any] = {
        "action": normalized_action,
        "summary": summary_s,
        "reasons": reasons_l,
        "entry_score": float(entry_score),
        "trend_strength": float(trend_strength),
        "spread": float(spread),
        "orderbook_imbalance": float(orderbook_imbalance),
        "current_price": float(current_price_f),
        "signal_source": signal_source_s,
        "side": _require_nonempty_str(direction, "direction").upper(),
        "regime": regime_s,
    }

    if threshold is not None:
        payload["threshold"] = float(threshold)
    if exit_score is not None:
        payload["exit_score"] = float(exit_score)
    if position_qty is not None:
        payload["position_qty"] = float(_as_float(position_qty, "decision.position_qty", min_value=0.0))
    if entry_price is not None:
        entry_price_f = float(_as_float(entry_price, "decision.entry_price", min_value=0.0))
        if entry_price_f <= 0:
            raise OrderFailed("decision.entry_price must be > 0 (STRICT)")
        payload["entry_price"] = entry_price_f

        if tp_pct is not None and sl_pct is not None:
            target_price, stop_price = _calculate_exit_prices_strict(
                direction=direction,
                entry_price=entry_price_f,
                tp_pct=float(_as_float(tp_pct, "decision.tp_pct", min_value=0.0)),
                sl_pct=float(_as_float(sl_pct, "decision.sl_pct", min_value=0.0)),
            )
            payload["target_price"] = target_price
            payload["stop_price"] = stop_price

    return payload


def _emit_decision_event_strict(
    *,
    meta: Dict[str, Any],
    symbol: str,
    regime: str,
    direction: str,
    signal_source: str,
    action: str,
    reason_code: str,
    summary: str,
    reasons: List[str],
    current_price: float,
    spread_snapshot: Optional[float],
    position_qty: Optional[float],
    entry_price: Optional[float] = None,
    tp_pct: Optional[float] = None,
    sl_pct: Optional[float] = None,
) -> None:
    reason_code_s = _require_nonempty_str(reason_code, "decision.reason_code")

    payload = _build_decision_payload_strict(
        meta=meta,
        action=action,
        summary=summary,
        reasons=reasons,
        signal_source=signal_source,
        current_price=current_price,
        direction=direction,
        spread_snapshot=spread_snapshot,
        position_qty=position_qty,
        entry_price=entry_price,
        tp_pct=tp_pct,
        sl_pct=sl_pct,
    )

    log_event(
        "DECISION",
        symbol=symbol,
        regime=regime,
        source="execution_engine.decision",
        side=direction,
        price=float(current_price),
        qty=(float(position_qty) if position_qty is not None else None),
        reason=reason_code_s,
        extra_json=payload,
    )


def _emit_position_event_strict(
    *,
    symbol: str,
    regime: str,
    direction: str,
    signal_source: str,
    entry_price: float,
    position_qty: float,
    leverage: float,
    tp_pct: float,
    sl_pct: float,
    allocation_ratio: float,
    trade_id: Optional[int],
    entry_order_id: Optional[str],
    tp_order_id: Optional[str],
    sl_order_id: Optional[str],
    exchange_position_side: str,
    remaining_qty: float,
    realized_pnl_usdt: float,
    reconciliation_status: str,
    entry_ts: datetime,
    entry_client_order_id: str,
    reason: str,
    is_test: bool = False,
) -> None:
    symbol_s = _require_nonempty_str(symbol, "position.symbol").upper()
    regime_s = _require_nonempty_str(regime, "position.regime")
    direction_s = _require_nonempty_str(direction, "position.direction").upper()
    signal_source_s = _require_nonempty_str(signal_source, "position.signal_source")
    exchange_position_side_s = _require_nonempty_str(
        exchange_position_side,
        "position.exchange_position_side",
    ).upper()
    reconciliation_status_s = _require_nonempty_str(
        reconciliation_status,
        "position.reconciliation_status",
    ).upper()
    reason_s = _require_nonempty_str(reason, "position.reason")
    entry_client_order_id_s = _require_nonempty_str(
        entry_client_order_id,
        "position.entry_client_order_id",
    )

    if direction_s not in ("LONG", "SHORT"):
        raise OrderFailed("position.direction must be LONG or SHORT (STRICT)")
    if exchange_position_side_s not in ("BOTH", "LONG", "SHORT"):
        raise OrderFailed("position.exchange_position_side must be BOTH/LONG/SHORT (STRICT)")
    if not isinstance(entry_ts, datetime) or entry_ts.tzinfo is None or entry_ts.tzinfo.utcoffset(entry_ts) is None:
        raise OrderFailed("position.entry_ts must be tz-aware datetime (STRICT)")

    entry_price_f = float(_as_float(entry_price, "position.entry_price", min_value=0.0))
    position_qty_f = float(_as_float(position_qty, "position.position_qty", min_value=0.0))
    leverage_f = float(_as_float(leverage, "position.leverage", min_value=1.0))
    tp_pct_f = float(_as_float(tp_pct, "position.tp_pct", min_value=0.0, max_value=1.0))
    sl_pct_f = float(_as_float(sl_pct, "position.sl_pct", min_value=0.0, max_value=1.0))
    allocation_ratio_f = float(
        _as_float(
            allocation_ratio,
            "position.allocation_ratio",
            min_value=0.0,
            max_value=1.0,
        )
    )
    remaining_qty_f = float(_as_float(remaining_qty, "position.remaining_qty", min_value=0.0))
    realized_pnl_usdt_f = float(_as_float(realized_pnl_usdt, "position.realized_pnl_usdt"))

    if entry_price_f <= 0:
        raise OrderFailed("position.entry_price must be > 0 (STRICT)")
    if position_qty_f <= 0:
        raise OrderFailed("position.position_qty must be > 0 (STRICT)")
    if remaining_qty_f <= 0:
        raise OrderFailed("position.remaining_qty must be > 0 (STRICT)")

    extra_json: Dict[str, Any] = {
        "state": "OPEN",
        "signal_source": signal_source_s,
        "trade_id": (int(trade_id) if trade_id is not None else None),
        "entry_order_id": (entry_order_id.strip() if isinstance(entry_order_id, str) and entry_order_id.strip() else None),
        "tp_order_id": (tp_order_id.strip() if isinstance(tp_order_id, str) and tp_order_id.strip() else None),
        "sl_order_id": (sl_order_id.strip() if isinstance(sl_order_id, str) and sl_order_id.strip() else None),
        "entry_client_order_id": entry_client_order_id_s,
        "exchange_position_side": exchange_position_side_s,
        "remaining_qty": remaining_qty_f,
        "realized_pnl_usdt": realized_pnl_usdt_f,
        "reconciliation_status": reconciliation_status_s,
        "entry_ts": entry_ts.astimezone(timezone.utc).isoformat(),
    }

    log_event(
        "POSITION",
        symbol=symbol_s,
        regime=regime_s,
        source="execution_engine.position",
        side=direction_s,
        price=entry_price_f,
        qty=position_qty_f,
        leverage=leverage_f,
        tp_pct=tp_pct_f,
        sl_pct=sl_pct_f,
        risk_pct=allocation_ratio_f,
        reason=reason_s,
        extra_json=extra_json,
        is_test=bool(is_test),
    )


def _require_meta_equity(meta: Dict[str, Any]) -> tuple[float, float, float]:
    if "equity_current_usdt" not in meta:
        raise OrderFailed("signal.meta['equity_current_usdt'] is required (STRICT)")
    if "equity_peak_usdt" not in meta:
        raise OrderFailed("signal.meta['equity_peak_usdt'] is required (STRICT)")
    if "dd_pct" not in meta:
        raise OrderFailed("signal.meta['dd_pct'] is required (STRICT)")

    eq_cur = _as_float(meta.get("equity_current_usdt"), "meta.equity_current_usdt", min_value=0.0)
    eq_peak = _as_float(meta.get("equity_peak_usdt"), "meta.equity_peak_usdt", min_value=0.0)
    dd = _as_float(meta.get("dd_pct"), "meta.dd_pct", min_value=0.0, max_value=100.0)

    if eq_cur <= 0 or eq_peak <= 0:
        raise OrderFailed("meta equity values must be > 0 (STRICT)")
    return float(eq_cur), float(eq_peak), float(dd)


def _deterministic_entry_client_order_id(symbol: str, direction: str, signal_ts_ms: int) -> str:
    sym = str(symbol).upper().strip()
    if not sym:
        raise ValueError("symbol is empty")
    dir_u = str(direction).upper().strip()
    if dir_u not in ("LONG", "SHORT"):
        raise ValueError("direction must be LONG/SHORT")
    if not isinstance(signal_ts_ms, int) or signal_ts_ms <= 0:
        raise ValueError("signal_ts_ms must be int > 0")

    d = "L" if dir_u == "LONG" else "S"
    sym8 = sym[:8]
    cid = f"ent-{sym8}-{d}-{signal_ts_ms}"
    if len(cid) > 36:
        cid = cid[:36]

    try:
        cid.encode("ascii")
    except UnicodeEncodeError as e:
        raise ValueError("generated client order id must be ASCII") from e

    if not cid or len(cid) > 36:
        raise ValueError("generated client order id invalid")

    return cid


def _submit_tg_nonblocking(func, msg: str, *, label: str) -> None:
    """
    STRICT:
    - 텔레그램 전송은 매매 실행을 블로킹하면 안 된다.
    - 실패해도 매매 흐름을 깨지 않는다(알림은 비핵심 I/O).
    """
    try:
        ok = submit_async(func, msg, critical=False, label=label)
        if not ok:
            logger.warning("[TG][DROP] queue full label=%s", label)
    except Exception as e:
        logger.warning("[TG][SUBMIT_FAIL] label=%s err=%s", label, f"{type(e).__name__}:{e}")


def _submit_task_nonblocking(fn, *args: Any, label: str) -> None:
    """
    STRICT:
    - 비핵심 작업(관측/리포팅)은 async_worker로 위임한다.
    """
    try:
        ok = submit_async(fn, *args, critical=False, label=label)
        if not ok:
            logger.warning("[ASYNC][DROP] queue full label=%s", label)
    except Exception as e:
        logger.warning("[ASYNC][SUBMIT_FAIL] label=%s err=%s", label, f"{type(e).__name__}:{e}")


def _fetch_mark_price_strict(symbol: str) -> float:
    """
    STRICT: /fapi/v1/premiumIndex markPrice 조회
    """
    sym = str(symbol).replace("-", "").replace("/", "").upper().strip()
    if not sym:
        raise OrderFailed("symbol is empty (STRICT)")

    data = req("GET", "/fapi/v1/premiumIndex", {"symbol": sym}, private=False)
    if not isinstance(data, dict):
        raise OrderFailed("premiumIndex response must be dict (STRICT)")
    if "markPrice" not in data:
        raise OrderFailed("premiumIndex.markPrice missing (STRICT)")

    px = _as_float(data.get("markPrice"), "premiumIndex.markPrice", min_value=0.0)
    if px <= 0:
        raise OrderFailed("premiumIndex.markPrice must be > 0 (STRICT)")
    return float(px)


def _collect_post_prices_strict(symbol: str) -> Dict[str, float]:
    """
    STRICT: 체결 후 마크가격을 t+1s/t+3s/t+5s 시점에 각각 조회한다.
    - 이 함수는 async_worker에서만 실행하도록 설계한다(메인 execute 블로킹 금지).
    """
    time.sleep(1.0)
    p1 = _fetch_mark_price_strict(symbol)
    time.sleep(2.0)
    p3 = _fetch_mark_price_strict(symbol)
    time.sleep(2.0)
    p5 = _fetch_mark_price_strict(symbol)
    return {"t+1s": p1, "t+3s": p3, "t+5s": p5}


def _exec_quality_task(symbol: str, direction: str, regime: str, expected_price: float, filled_avg_price: float) -> None:
    """
    비동기 실행 품질 스냅샷 이벤트 기록.
    - 실패해도 거래 흐름은 깨지지 않는다(관측용).
    """
    try:
        post_prices = _collect_post_prices_strict(symbol)
        side = "BUY" if direction == "LONG" else "SELL"
        snap = build_execution_quality_snapshot_obj(
            symbol=symbol,
            side=side,
            expected_price=float(expected_price),
            filled_avg_price=float(filled_avg_price),
            post_prices=post_prices,
        )
        log_event(
            "on_execution_quality",
            symbol=symbol,
            regime=regime,
            source="execution_engine.exec_quality",
            side=direction,
            price=float(filled_avg_price),
            reason="execution_quality_snapshot",
            extra_json={
                "expected_price": snap.expected_price,
                "filled_avg_price": snap.filled_avg_price,
                "slippage_pct": snap.slippage_pct,
                "adverse_move_pct": snap.adverse_move_pct,
                "post_prices": dict(snap.post_prices),
                "execution_score": snap.execution_score,
                "meta": dict(snap.meta),
            },
        )
    except (ExecutionQualityError, OrderFailed, ValueError, RuntimeError) as e:
        log_event(
            "on_execution_quality_fail",
            symbol=symbol,
            regime=regime,
            source="execution_engine.exec_quality",
            side=direction,
            price=float(filled_avg_price) if math.isfinite(float(filled_avg_price)) else None,
            reason="execution_quality_failed",
            extra_json={"error": str(e)[:240]},
        )


def _require_trade_exec_fields(
    trade: Any,
    *,
    soft_mode: bool,
) -> Tuple[str, str, Optional[str], str, float, float, str]:
    entry_order_id = getattr(trade, "entry_order_id", None)
    tp_order_id = getattr(trade, "tp_order_id", None)
    sl_order_id = getattr(trade, "sl_order_id", None)

    if not isinstance(entry_order_id, str) or not entry_order_id.strip():
        raise OrderFailed("trade.entry_order_id is required (STRICT)")

    if not isinstance(tp_order_id, str) or not tp_order_id.strip():
        raise OrderFailed("trade.tp_order_id is required (STRICT)")

    if sl_order_id is not None and (not isinstance(sl_order_id, str) or not sl_order_id.strip()):
        raise OrderFailed("trade.sl_order_id must be str or None (STRICT)")

    if not bool(soft_mode):
        if not isinstance(sl_order_id, str) or not sl_order_id.strip():
            raise OrderFailed("soft_mode=False but trade.sl_order_id is missing (STRICT)")

    exchange_position_side = getattr(trade, "exchange_position_side", None)
    if not isinstance(exchange_position_side, str) or not exchange_position_side.strip():
        raise OrderFailed("trade.exchange_position_side is required (STRICT)")
    exchange_position_side = exchange_position_side.strip().upper()
    if exchange_position_side not in ("BOTH", "LONG", "SHORT"):
        raise OrderFailed("trade.exchange_position_side must be BOTH/LONG/SHORT (STRICT)")

    remaining_qty = getattr(trade, "remaining_qty", None)
    if remaining_qty is None:
        raise OrderFailed("trade.remaining_qty is required (STRICT)")
    remaining_qty_f = _as_float(remaining_qty, "trade.remaining_qty", min_value=0.0)
    if remaining_qty_f <= 0:
        raise OrderFailed("trade.remaining_qty must be > 0 (STRICT)")

    realized_pnl_usdt = getattr(trade, "realized_pnl_usdt", None)
    if realized_pnl_usdt is None:
        raise OrderFailed("trade.realized_pnl_usdt is required (STRICT)")
    realized_pnl_usdt_f = _as_float(realized_pnl_usdt, "trade.realized_pnl_usdt")

    reconciliation_status = getattr(trade, "reconciliation_status", None)
    if not isinstance(reconciliation_status, str) or not reconciliation_status.strip():
        raise OrderFailed("trade.reconciliation_status is required (STRICT)")
    reconciliation_status = reconciliation_status.strip().upper()
    if reconciliation_status not in _ALLOWED_RECONCILIATION_STATUSES:
        raise OrderFailed(
            f"trade.reconciliation_status unexpected (STRICT): {reconciliation_status!r}"
        )

    return (
        entry_order_id.strip(),
        tp_order_id.strip(),
        (sl_order_id.strip() if isinstance(sl_order_id, str) else None),
        exchange_position_side,
        float(remaining_qty_f),
        float(realized_pnl_usdt_f),
        reconciliation_status,
    )


class _SettingsView:
    __slots__ = ("_base", "_overrides")

    def __init__(self, base: Any, overrides: Dict[str, float]):
        object.__setattr__(self, "_base", base)
        object.__setattr__(self, "_overrides", overrides)

    def __getattr__(self, name: str) -> Any:
        overrides = object.__getattribute__(self, "_overrides")
        if name in overrides:
            return overrides[name]
        base = object.__getattribute__(self, "_base")
        return getattr(base, name)

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("SettingsView is read-only")


def _build_guard_settings_view(settings: Any, guard_adjustments: Dict[str, float]) -> _SettingsView:
    if guard_adjustments is None:
        raise OrderFailed("guard_adjustments must not be None (STRICT)")
    if not isinstance(guard_adjustments, dict):
        raise OrderFailed("guard_adjustments must be dict (STRICT)")

    overrides: Dict[str, float] = {}
    for k, v in guard_adjustments.items():
        if not isinstance(k, str) or not k.strip():
            raise ValueError("guard_adjustments key must be non-empty str")
        if not hasattr(settings, k):
            raise OrderFailed(f"guard_adjustments contains unknown settings key: {k}")
        overrides[k] = _as_float(v, f"guard_adjustments.{k}", min_value=0.0)

    return _SettingsView(settings, overrides)


def _resolve_guard_adjustments(signal: Signal) -> Dict[str, float]:
    ga = getattr(signal, "guard_adjustments", None)
    if ga is None:
        return {}
    if not isinstance(ga, dict):
        raise OrderFailed("signal.guard_adjustments must be dict or None (STRICT)")
    return dict(ga)


def _resolve_dynamic_allocation(meta: Dict[str, Any]) -> Optional[float]:
    v = meta.get("dynamic_allocation_ratio")
    if v is None:
        v = meta.get("dynamic_risk_pct")
    if v is None:
        return None
    return _as_float(v, "meta.dynamic_allocation_ratio", min_value=0.0, max_value=1.0)


def _meta_optional(meta: Dict[str, Any], key: str) -> Any:
    return meta.get(key) if key in meta else None


def _meta_optional_dict(meta: Dict[str, Any], key: str) -> Optional[dict]:
    if key not in meta:
        return None
    v = meta.get(key)
    if v is None:
        return None
    if not isinstance(v, dict):
        raise OrderFailed(f"meta.{key} must be dict when provided (STRICT)")
    return v


def _meta_optional_list(meta: Dict[str, Any], key: str) -> Optional[list]:
    if key not in meta:
        return None
    v = meta.get(key)
    if v is None:
        return None
    if not isinstance(v, list):
        raise OrderFailed(f"meta.{key} must be list when provided (STRICT)")
    return v


def _meta_optional_str(meta: Dict[str, Any], key: str) -> Optional[str]:
    if key not in meta:
        return None
    v = meta.get(key)
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        raise OrderFailed(f"meta.{key} must be non-empty when provided (STRICT)")
    return s


def _snapshot_kwargs_from_meta(meta: Dict[str, Any], *, entry_price_hint: float, filled_entry_price: float) -> Dict[str, Any]:
    """
    record_trade_snapshot의 확장 필드(TRADE-GRADE) 입력을 meta에서 추출한다.
    - 값이 '존재하면' STRICT 타입/범위 검증 후 전달
    - 존재하지 않으면 None(미기록)
    """
    decision_id = _meta_optional_str(meta, "decision_id")
    quant_decision_pre = _meta_optional_dict(meta, "quant_decision_pre")
    quant_constraints = _meta_optional_dict(meta, "quant_constraints")
    quant_final_decision = _meta_optional_str(meta, "quant_final_decision")

    gpt_severity = _meta_optional(meta, "gpt_severity")
    if gpt_severity is not None:
        gpt_severity = int(_as_float(gpt_severity, "meta.gpt_severity", min_value=0.0, max_value=3.0))
    gpt_tags = _meta_optional_list(meta, "gpt_tags")
    gpt_confidence_penalty = _meta_optional(meta, "gpt_confidence_penalty")
    if gpt_confidence_penalty is not None:
        gpt_confidence_penalty = _as_float(meta.get("gpt_confidence_penalty"), "meta.gpt_confidence_penalty", min_value=0.0, max_value=1.0)
    gpt_suggested_risk_multiplier = _meta_optional(meta, "gpt_suggested_risk_multiplier")
    if gpt_suggested_risk_multiplier is not None:
        gpt_suggested_risk_multiplier = _as_float(
            gpt_suggested_risk_multiplier,
            "meta.gpt_suggested_risk_multiplier",
            min_value=0.0,
            max_value=1.0,
        )
    gpt_rationale_short = _meta_optional_str(meta, "gpt_rationale_short")

    micro_funding_rate = _meta_optional(meta, "micro_funding_rate")
    if micro_funding_rate is not None:
        micro_funding_rate = _as_float(micro_funding_rate, "meta.micro_funding_rate")
    micro_funding_z = _meta_optional(meta, "micro_funding_z")
    if micro_funding_z is not None:
        micro_funding_z = _as_float(micro_funding_z, "meta.micro_funding_z")
    micro_open_interest = _meta_optional(meta, "micro_open_interest")
    if micro_open_interest is not None:
        micro_open_interest = _as_float(micro_open_interest, "meta.micro_open_interest", min_value=0.0)
    micro_oi_z = _meta_optional(meta, "micro_oi_z")
    if micro_oi_z is not None:
        micro_oi_z = _as_float(micro_oi_z, "meta.micro_oi_z")
    micro_long_short_ratio = _meta_optional(meta, "micro_long_short_ratio")
    if micro_long_short_ratio is not None:
        micro_long_short_ratio = _as_float(micro_long_short_ratio, "meta.micro_long_short_ratio", min_value=0.0)
    micro_lsr_z = _meta_optional(meta, "micro_lsr_z")
    if micro_lsr_z is not None:
        micro_lsr_z = _as_float(micro_lsr_z, "meta.micro_lsr_z")
    micro_distortion_index = _meta_optional(meta, "micro_distortion_index")
    if micro_distortion_index is not None:
        micro_distortion_index = _as_float(micro_distortion_index, "meta.micro_distortion_index")
    micro_score_risk = _meta_optional(meta, "micro_score_risk")
    if micro_score_risk is not None:
        micro_score_risk = _as_float(micro_score_risk, "meta.micro_score_risk", min_value=0.0, max_value=100.0)

    ev_cell_key = _meta_optional_str(meta, "ev_cell_key")
    ev_cell_ev = _meta_optional(meta, "ev_cell_ev")
    if ev_cell_ev is not None:
        ev_cell_ev = _as_float(ev_cell_ev, "meta.ev_cell_ev")
    ev_cell_n = _meta_optional(meta, "ev_cell_n")
    if ev_cell_n is not None:
        ev_cell_n = int(_as_float(ev_cell_n, "meta.ev_cell_n", min_value=0.0))
    ev_cell_status = _meta_optional_str(meta, "ev_cell_status")

    auto_blocked = meta.get("auto_blocked") if "auto_blocked" in meta else None
    if auto_blocked is not None:
        auto_blocked = bool(auto_blocked)
    auto_risk_multiplier = _meta_optional(meta, "auto_risk_multiplier")
    if auto_risk_multiplier is not None:
        auto_risk_multiplier = _as_float(auto_risk_multiplier, "meta.auto_risk_multiplier", min_value=0.0, max_value=1.0)
    auto_block_reasons = _meta_optional_dict(meta, "auto_block_reasons")

    if entry_price_hint <= 0:
        raise OrderFailed("entry_price_hint must be > 0 for exec snapshot (STRICT)")
    slip_pct = abs(float(filled_entry_price) - float(entry_price_hint)) / float(entry_price_hint) * 100.0
    if not math.isfinite(slip_pct) or slip_pct < 0.0:
        raise OrderFailed(f"computed slippage_pct invalid (STRICT): {slip_pct}")

    return {
        "decision_id": decision_id,
        "quant_decision_pre": quant_decision_pre,
        "quant_constraints": quant_constraints,
        "quant_final_decision": quant_final_decision,
        "gpt_severity": gpt_severity,
        "gpt_tags": gpt_tags,
        "gpt_confidence_penalty": gpt_confidence_penalty,
        "gpt_suggested_risk_multiplier": gpt_suggested_risk_multiplier,
        "gpt_rationale_short": gpt_rationale_short,
        "micro_funding_rate": micro_funding_rate,
        "micro_funding_z": micro_funding_z,
        "micro_open_interest": micro_open_interest,
        "micro_oi_z": micro_oi_z,
        "micro_long_short_ratio": micro_long_short_ratio,
        "micro_lsr_z": micro_lsr_z,
        "micro_distortion_index": micro_distortion_index,
        "micro_score_risk": micro_score_risk,
        "exec_expected_price": float(entry_price_hint),
        "exec_filled_avg_price": float(filled_entry_price),
        "exec_slippage_pct": float(slip_pct),
        "exec_adverse_move_pct": None,
        "exec_score": None,
        "exec_post_prices": None,
        "ev_cell_key": ev_cell_key,
        "ev_cell_ev": ev_cell_ev,
        "ev_cell_n": ev_cell_n,
        "ev_cell_status": ev_cell_status,
        "auto_blocked": auto_blocked,
        "auto_risk_multiplier": auto_risk_multiplier,
        "auto_block_reasons": auto_block_reasons,
    }


class ExecutionEngine:
    def __init__(self, settings: Any):
        self.settings = settings

        required = ["symbol", "leverage", "slippage_block_pct", "slippage_stop_engine", "require_deterministic_client_order_id"]
        missing = [k for k in required if not hasattr(settings, k)]
        if missing:
            raise ValueError(f"settings missing required fields: {missing}")

        if bool(getattr(settings, "require_deterministic_client_order_id")) is not True:
            raise ValueError("settings.require_deterministic_client_order_id must be True (TRADE-GRADE)")

        self.test_force_enter: bool = _as_bool_strict(getattr(settings, "test_force_enter", False), "settings.test_force_enter")
        self.test_bypass_guards: bool = _as_bool_strict(getattr(settings, "test_bypass_guards", False), "settings.test_bypass_guards")
        self.test_dry_run: bool = _as_bool_strict(getattr(settings, "test_dry_run", False), "settings.test_dry_run")

        fake_usdt_raw = getattr(settings, "test_fake_available_usdt", 0.0)
        self.test_fake_available_usdt: float = float(_as_float(fake_usdt_raw, "settings.test_fake_available_usdt", min_value=0.0))

        if self.test_bypass_guards and not self.test_dry_run:
            raise RuntimeError("test_bypass_guards is only allowed with test_dry_run=True (STRICT)")
        if self.test_force_enter and not self.test_dry_run:
            raise RuntimeError("test_force_enter is only allowed with test_dry_run=True (STRICT)")
        if self.test_fake_available_usdt > 0 and not self.test_dry_run:
            raise RuntimeError("test_fake_available_usdt is only allowed with test_dry_run=True (STRICT)")

        if any([self.test_force_enter, self.test_bypass_guards, self.test_dry_run, self.test_fake_available_usdt > 0]):
            logger.warning(
                "[TEST MODE ENABLED] FORCE_ENTER=%s BYPASS_GUARDS=%s DRY_RUN=%s FAKE_USDT=%s",
                self.test_force_enter,
                self.test_bypass_guards,
                self.test_dry_run,
                self.test_fake_available_usdt,
            )

    def execute(self, signal: Signal) -> Optional[Trade]:
        if not isinstance(signal, Signal):
            raise ValueError("signal must be Signal")

        action = str(signal.action).upper().strip()
        if action not in ("ENTER", "SKIP"):
            raise ValueError(f"signal.action invalid: {signal.action!r}")

        if self.test_force_enter:
            action = "ENTER"

        if not _EXECUTION_LOCK.acquire(blocking=False):
            raise StateViolation("Concurrent execute() call detected (STRICT)")

        state: OrderState = OrderState.CREATED

        try:
            meta = signal.meta
            if not isinstance(meta, dict):
                raise OrderFailed("signal.meta must be dict (STRICT)")

            symbol = meta.get("symbol")
            if not isinstance(symbol, str) or not symbol.strip():
                raise OrderFailed("signal.meta['symbol'] is required (STRICT)")
            symbol = symbol.strip().upper()

            regime = meta.get("regime")
            if not isinstance(regime, str) or not regime.strip():
                raise OrderFailed("signal.meta['regime'] is required (STRICT)")
            regime = regime.strip()

            signal_source = meta.get("signal_source")
            if not isinstance(signal_source, str) or not signal_source.strip():
                raise OrderFailed("signal.meta['signal_source'] is required (STRICT)")
            signal_source = signal_source.strip()

            signal_ts_ms = meta.get("signal_ts_ms")
            if not isinstance(signal_ts_ms, int) or signal_ts_ms <= 0:
                raise OrderFailed("signal.meta['signal_ts_ms'] must be int > 0 (STRICT)")

            direction = str(signal.direction).upper().strip()
            if direction not in ("LONG", "SHORT"):
                raise ValueError("signal.direction must be LONG or SHORT")

            last_price = _as_float(meta.get("last_price"), "meta.last_price", min_value=0.0)
            if last_price <= 0:
                raise OrderFailed("meta.last_price must be > 0 (STRICT)")

            if action == "SKIP":
                _emit_decision_event_strict(
                    meta=meta,
                    symbol=symbol,
                    regime=regime,
                    direction=direction,
                    signal_source=signal_source,
                    action="NO_ENTRY",
                    reason_code=str(signal.reason or "signal_skip"),
                    summary="전략 레이어에서 진입하지 않기로 결정",
                    reasons=[str(signal.reason or "signal_skip")],
                    current_price=float(last_price),
                    spread_snapshot=None,
                    position_qty=None,
                )

                msg = f"[SKIP] {symbol} {direction} reason={signal.reason}"
                logger.info(msg)
                log_skip_event(
                    symbol=symbol,
                    regime=regime,
                    source="execution_engine",
                    side=direction,
                    reason=str(signal.reason) or "skip",
                    extra={"signal_source": signal_source, "ts_ms": signal_ts_ms},
                )
                _submit_tg_nonblocking(send_skip_tg, msg, label="send_skip_tg")
                return None

            try:
                validate_signal_from_meta_strict(meta)
                validate_signal_invariants_strict(
                    SignalInvariantInputs(
                        symbol=str(symbol),
                        direction=str(direction),
                        risk_pct=float(_as_float(signal.risk_pct, "signal.risk_pct", min_value=0.0, max_value=1.0)),
                        tp_pct=float(_as_float(signal.tp_pct, "signal.tp_pct", min_value=0.0, max_value=1.0)),
                        sl_pct=float(_as_float(signal.sl_pct, "signal.sl_pct", min_value=0.0, max_value=1.0)),
                        dd_pct=(float(meta["dd_pct"]) if "dd_pct" in meta and meta.get("dd_pct") is not None else None),
                        micro_score_risk=(float(meta["micro_score_risk"]) if "micro_score_risk" in meta and meta.get("micro_score_risk") is not None else None),
                        final_risk_multiplier=None,
                        equity_current_usdt=(float(meta["equity_current_usdt"]) if "equity_current_usdt" in meta and meta.get("equity_current_usdt") is not None else None),
                        equity_peak_usdt=(float(meta["equity_peak_usdt"]) if "equity_peak_usdt" in meta and meta.get("equity_peak_usdt") is not None else None),
                    )
                )
            except (InvariantViolation, ValueError) as e:
                raise OrderFailed(f"invariant violation (STRICT): {e}") from e

            candles_5m = meta.get("candles_5m")
            candles_5m_raw = meta.get("candles_5m_raw")
            extra = meta.get("extra")
            if extra is not None and not isinstance(extra, dict):
                raise OrderFailed("meta.extra must be dict or None (STRICT)")

            dyn_alloc = _resolve_dynamic_allocation(meta)
            if dyn_alloc is not None:
                sig_alloc = _as_float(signal.risk_pct, "signal.risk_pct(allocation_ratio)", min_value=0.0, max_value=1.0)
                if abs(dyn_alloc - sig_alloc) > 1e-12:
                    raise OrderFailed(f"dynamic_allocation mismatch: meta={dyn_alloc} != signal={sig_alloc} (STRICT)")

            manual_ok = check_manual_position_guard(
                get_balance_detail_func=get_balance_detail,
                symbol=symbol,
                latest_ts=float(signal_ts_ms),
            )
            if not manual_ok:
                _emit_decision_event_strict(
                    meta=meta,
                    symbol=symbol,
                    regime=regime,
                    direction=direction,
                    signal_source=signal_source,
                    action="NO_ENTRY",
                    reason_code="manual_guard_blocked",
                    summary="수동 포지션 가드로 진입 차단",
                    reasons=["manual_guard_blocked"],
                    current_price=float(last_price),
                    spread_snapshot=None,
                    position_qty=None,
                )

                msg = f"[SKIP] manual_guard_blocked: {symbol}"
                logger.warning(msg)
                log_skip_event(
                    symbol=symbol,
                    regime=regime,
                    source="execution_engine.guard.manual",
                    side=direction,
                    reason="manual_guard_blocked",
                    extra={"ts_ms": signal_ts_ms},
                )
                _submit_tg_nonblocking(send_skip_tg, msg, label="send_skip_tg")
                return None

            avail_usdt = (
                float(self.test_fake_available_usdt)
                if self.test_fake_available_usdt > 0
                else _as_float(get_available_usdt(), "available_usdt", min_value=0.0)
            )
            if avail_usdt <= 0:
                msg = f"[SKIP] available_usdt<=0: {symbol}"
                logger.warning(msg)
                log_skip_event(
                    symbol=symbol,
                    regime=regime,
                    source="execution_engine.balance",
                    side=direction,
                    reason="balance_zero_or_invalid",
                    extra={"available_usdt": avail_usdt, "ts_ms": signal_ts_ms},
                )
                _submit_tg_nonblocking(send_skip_tg, msg, label="send_skip_tg")
                return None

            best_bid: Optional[float] = None
            best_ask: Optional[float] = None

            guard_adjustments = _resolve_guard_adjustments(signal)
            guard_settings = _build_guard_settings_view(self.settings, guard_adjustments)

            if candles_5m_raw is None:
                raise OrderFailed("meta.candles_5m_raw is required for volume guard (STRICT)")
            vol_ok = check_volume_guard(
                settings=guard_settings,
                candles_5m_raw=candles_5m_raw,
                latest_ts=float(signal_ts_ms),
                signal_source=signal_source,
                direction=direction,
            )
            if not vol_ok:
                _emit_decision_event_strict(
                    meta=meta,
                    symbol=symbol,
                    regime=regime,
                    direction=direction,
                    signal_source=signal_source,
                    action="NO_ENTRY",
                    reason_code="volume_guard_blocked",
                    summary="거래량 가드로 진입 차단",
                    reasons=["volume_guard_blocked"],
                    current_price=float(last_price),
                    spread_snapshot=None,
                    position_qty=None,
                )

                msg = f"[SKIP] volume_guard_blocked: {symbol}"
                logger.info(msg)
                log_skip_event(
                    symbol=symbol,
                    regime=regime,
                    source="execution_engine.guard.volume",
                    side=direction,
                    reason="volume_guard_blocked",
                    extra={"ts_ms": signal_ts_ms},
                )
                _submit_tg_nonblocking(send_skip_tg, msg, label="send_skip_tg")
                if not self.test_bypass_guards:
                    return None

            if candles_5m is None:
                raise OrderFailed("meta.candles_5m is required for price jump guard (STRICT)")
            price_ok = check_price_jump_guard(
                settings=guard_settings,
                candles_5m=candles_5m,
                latest_ts=float(signal_ts_ms),
                signal_source=signal_source,
                direction=direction,
            )
            if not price_ok:
                _emit_decision_event_strict(
                    meta=meta,
                    symbol=symbol,
                    regime=regime,
                    direction=direction,
                    signal_source=signal_source,
                    action="NO_ENTRY",
                    reason_code="price_jump_guard_blocked",
                    summary="가격 점프 가드로 진입 차단",
                    reasons=["price_jump_guard_blocked"],
                    current_price=float(last_price),
                    spread_snapshot=None,
                    position_qty=None,
                )

                msg = f"[SKIP] price_jump_guard_blocked: {symbol}"
                logger.info(msg)
                log_skip_event(
                    symbol=symbol,
                    regime=regime,
                    source="execution_engine.guard.price_jump",
                    side=direction,
                    reason="price_jump_guard_blocked",
                    extra={"ts_ms": signal_ts_ms},
                )
                _submit_tg_nonblocking(send_skip_tg, msg, label="send_skip_tg")
                if not self.test_bypass_guards:
                    return None

            spread_ok, best_bid, best_ask = check_spread_guard(
                settings=guard_settings,
                symbol=symbol,
                latest_ts=float(signal_ts_ms),
                signal_source=signal_source,
                direction=direction,
            )
            spread_pct_snapshot: Optional[float] = None
            if best_bid is not None and best_ask is not None:
                bb_tmp = float(best_bid)
                ba_tmp = float(best_ask)
                if bb_tmp > 0 and ba_tmp > 0 and ba_tmp > bb_tmp:
                    mid_tmp = (bb_tmp + ba_tmp) / 2.0
                    if mid_tmp > 0:
                        spread_pct_snapshot = (ba_tmp - bb_tmp) / mid_tmp

            if not spread_ok:
                _emit_decision_event_strict(
                    meta=meta,
                    symbol=symbol,
                    regime=regime,
                    direction=direction,
                    signal_source=signal_source,
                    action="NO_ENTRY",
                    reason_code="spread_guard_blocked",
                    summary="스프레드 가드로 진입 차단",
                    reasons=["spread_guard_blocked"],
                    current_price=float(last_price),
                    spread_snapshot=spread_pct_snapshot,
                    position_qty=None,
                )

                msg = f"[SKIP] spread_guard_blocked: {symbol}"
                logger.info(msg)
                log_skip_event(
                    symbol=symbol,
                    regime=regime,
                    source="execution_engine.guard.spread",
                    side=direction,
                    reason="spread_guard_blocked",
                    extra={"ts_ms": signal_ts_ms, "best_bid": best_bid, "best_ask": best_ask},
                )
                _submit_tg_nonblocking(send_skip_tg, msg, label="send_skip_tg")
                if not self.test_bypass_guards:
                    return None

            entry_price_hint = float(last_price)
            price_for_qty = float(last_price)
            if best_bid is not None and best_ask is not None:
                bb = float(best_bid)
                ba = float(best_ask)
                if bb > 0 and ba > 0 and ba > bb:
                    mid = (bb + ba) / 2.0
                    if mid > 0:
                        price_for_qty = mid
                    entry_price_hint = ba if direction == "LONG" else bb

            if price_for_qty <= 0:
                raise OrderFailed("price_for_qty must be > 0 (STRICT)")

            slippage_pct_for_invariant = abs(float(entry_price_hint) - float(last_price)) / float(last_price)

            slippage_block_pct = _as_float(getattr(self.settings, "slippage_block_pct"), "settings.slippage_block_pct", min_value=0.0)
            slippage_stop_engine = bool(getattr(self.settings, "slippage_stop_engine"))

            if slippage_block_pct > 0:
                slippage_pct = slippage_pct_for_invariant
                if slippage_pct > slippage_block_pct:
                    _emit_decision_event_strict(
                        meta=meta,
                        symbol=symbol,
                        regime=regime,
                        direction=direction,
                        signal_source=signal_source,
                        action="NO_ENTRY",
                        reason_code="slippage_block",
                        summary="슬리피지 기준 초과로 진입 차단",
                        reasons=["slippage_block"],
                        current_price=float(last_price),
                        spread_snapshot=spread_pct_snapshot,
                        position_qty=None,
                    )

                    msg = f"[SKIP] {symbol} {direction} slippage_block slippage_pct={slippage_pct}"
                    logger.warning(msg)
                    log_skip_event(
                        symbol=symbol,
                        regime=regime,
                        source="execution_engine.slippage",
                        side=direction,
                        reason="slippage_block",
                        extra={"slippage_pct": slippage_pct, "slippage_block_pct": slippage_block_pct, "ts_ms": signal_ts_ms},
                    )
                    _submit_tg_nonblocking(send_skip_tg, msg, label="send_skip_tg")
                    if slippage_stop_engine:
                        raise OrderFailed("slippage_block triggered and slippage_stop_engine=True (STRICT)")
                    if not self.test_bypass_guards:
                        return None

            leverage = _as_float(getattr(self.settings, "leverage"), "settings.leverage", min_value=1.0)
            allocation_ratio = _as_float(signal.risk_pct, "signal.risk_pct(allocation_ratio)", min_value=0.0, max_value=1.0)
            tp_pct = _as_float(signal.tp_pct, "signal.tp_pct", min_value=0.0, max_value=1.0)
            sl_pct = _as_float(signal.sl_pct, "signal.sl_pct", min_value=0.0, max_value=1.0)

            if allocation_ratio <= 0:
                raise OrderFailed("ENTER requires allocation_ratio(signal.risk_pct) > 0 (STRICT)")
            if tp_pct <= 0:
                raise OrderFailed("ENTER requires signal.tp_pct > 0 (STRICT)")
            if sl_pct <= 0:
                raise OrderFailed("ENTER requires signal.sl_pct > 0 (STRICT)")

            if leverage > 1.0 and allocation_ratio >= 0.8:
                raise OrderFailed(
                    f"allocation_mode violation: leverage({leverage})>1 with large allocation_ratio({allocation_ratio}) (STRICT)"
                )

            notional = avail_usdt * allocation_ratio * leverage
            if notional <= 0:
                raise OrderFailed("notional must be > 0 (STRICT)")

            qty_raw = notional / price_for_qty
            if qty_raw <= 0:
                raise OrderFailed("qty_raw must be > 0 (STRICT)")

            try:
                validate_execution_invariants_strict(
                    ExecutionInvariantInputs(
                        symbol=str(symbol),
                        direction=str(direction),
                        leverage=float(leverage),
                        available_usdt=float(avail_usdt),
                        notional=float(notional),
                        price_for_qty=float(price_for_qty),
                        qty_raw=float(qty_raw),
                        entry_price_hint=float(entry_price_hint),
                        last_price=float(last_price),
                        slippage_pct=float(slippage_pct_for_invariant),
                        spread_pct_snapshot=(float(spread_pct_snapshot) if spread_pct_snapshot is not None else None),
                    )
                )
            except InvariantViolation as e:
                raise OrderFailed(f"execution invariant violation (STRICT): {e}") from e

            ok, guard_reason, guard_extra = hard_risk_guard_check(
                self.settings,
                symbol=symbol,
                side=direction,
                entry_price=float(entry_price_hint),
                notional=float(notional),
                available_usdt=float(avail_usdt),
            )
            if not ok:
                _emit_decision_event_strict(
                    meta=meta,
                    symbol=symbol,
                    regime=regime,
                    direction=direction,
                    signal_source=signal_source,
                    action="NO_ENTRY",
                    reason_code=str(guard_reason),
                    summary="하드 리스크 가드로 진입 차단",
                    reasons=[str(guard_reason)],
                    current_price=float(last_price),
                    spread_snapshot=spread_pct_snapshot,
                    position_qty=None,
                )

                msg = f"[SKIP] hard_risk_guard_blocked: {guard_reason}"
                logger.warning("%s extra=%s", msg, guard_extra)
                log_skip_event(
                    symbol=symbol,
                    regime=regime,
                    source="execution_engine.risk_guard",
                    side=direction,
                    reason=str(guard_reason),
                    extra=guard_extra,
                )
                _submit_tg_nonblocking(send_skip_tg, f"{msg} {symbol} {direction}", label="send_skip_tg")
                if not self.test_bypass_guards:
                    return None

            side_open = "BUY" if direction == "LONG" else "SELL"

            soft_mode = False
            sl_floor_ratio: Optional[float] = None
            if isinstance(extra, dict):
                soft_mode = bool(extra.get("soft_mode") or extra.get("soft") or False)
                if extra.get("sl_floor_ratio") is not None:
                    sl_floor_ratio = _as_float(extra.get("sl_floor_ratio"), "extra.sl_floor_ratio", min_value=0.0)

            _emit_decision_event_strict(
                meta=meta,
                symbol=symbol,
                regime=regime,
                direction=direction,
                signal_source=signal_source,
                action="ENTRY",
                reason_code="entry_submit",
                summary="모든 가드 통과로 진입 실행",
                reasons=["guards_passed", "entry_submit"],
                current_price=float(entry_price_hint),
                spread_snapshot=spread_pct_snapshot,
                position_qty=float(qty_raw),
                entry_price=float(entry_price_hint),
                tp_pct=float(tp_pct),
                sl_pct=float(sl_pct),
            )

            log_event(
                "on_entry_submitted",
                symbol=symbol,
                regime=regime,
                source="execution_engine",
                side=direction,
                price=float(entry_price_hint),
                qty=float(qty_raw),
                leverage=float(leverage),
                tp_pct=float(tp_pct),
                sl_pct=float(sl_pct),
                risk_pct=float(allocation_ratio),
                reason="entry_submit",
                extra_json={"signal_source": signal_source, "reason": str(signal.reason)[:200]},
            )

            log_signal(
                event="ENTRY_SIGNAL",
                symbol=symbol,
                regime=regime,
                source=str(signal_source),
                side=direction,
                price=float(entry_price_hint),
                qty=float(qty_raw),
                leverage=float(leverage),
                tp_pct=float(tp_pct),
                sl_pct=float(sl_pct),
                risk_pct=float(allocation_ratio),
                reason=str(signal.reason) or "enter",
                extra_json={
                    "candle_ts": int(signal_ts_ms),
                    "available_usdt": float(avail_usdt),
                    "notional": float(notional),
                    "regime_score": meta.get("regime_score"),
                    "regime_band": meta.get("regime_band"),
                    "regime_allocation": meta.get("regime_allocation"),
                    "dd_pct": meta.get("dd_pct"),
                    "consecutive_losses": meta.get("consecutive_losses"),
                    "risk_physics_reason": meta.get("risk_physics_reason"),
                    "dynamic_allocation_ratio": dyn_alloc,
                },
            )

            eq_cur_usdt, eq_peak_usdt, dd_pct_v = _require_meta_equity(meta)
            entry_client_order_id = _deterministic_entry_client_order_id(symbol=symbol, direction=direction, signal_ts_ms=signal_ts_ms)

            state = OrderState.SUBMITTED

            entry_score_val = _meta_pick_first(meta, ("entry_score", "entryScore"))
            engine_total_val = _meta_pick_first(meta, ("engine_total", "engine_total_score", "engine_total_score_v2"))

            if self.test_dry_run:
                state = OrderState.FILLED
                entry_price_sim = float(entry_price_hint)
                entry_qty_sim = float(qty_raw)
                entry_ts_dt_sim = datetime.fromtimestamp(float(signal_ts_ms) / 1000.0, tz=timezone.utc)

                log_event(
                    "on_entry_filled",
                    symbol=symbol,
                    regime=regime,
                    source="execution_engine",
                    side=direction,
                    price=float(entry_price_sim),
                    qty=float(entry_qty_sim),
                    leverage=float(leverage),
                    tp_pct=float(tp_pct),
                    sl_pct=float(sl_pct),
                    risk_pct=float(allocation_ratio),
                    reason="entry_filled_simulated",
                    extra_json={"test_dry_run": True},
                    is_test=True,
                )

                trade_id = record_trade_open_returning_id(
                    symbol=symbol,
                    side=str(side_open),
                    qty=float(entry_qty_sim),
                    entry_price=float(entry_price_sim),
                    entry_ts=entry_ts_dt_sim,
                    is_auto=True,
                    regime_at_entry=str(regime),
                    strategy=str(signal_source),
                    entry_score=_opt_float_strict(entry_score_val, "meta.entry_score"),
                    trend_score_at_entry=None,
                    range_score_at_entry=None,
                    leverage=float(leverage),
                    risk_pct=float(allocation_ratio),
                    tp_pct=float(tp_pct),
                    sl_pct=float(sl_pct),
                    note="TEST_DRY_RUN",
                    entry_order_id=None,
                    tp_order_id=None,
                    sl_order_id=None,
                    exchange_position_side="BOTH",
                    remaining_qty=float(entry_qty_sim),
                    realized_pnl_usdt=0.0,
                    reconciliation_status="TEST_DRY_RUN",
                    last_synced_at=entry_ts_dt_sim,
                )

                _emit_position_event_strict(
                    symbol=symbol,
                    regime=regime,
                    direction=direction,
                    signal_source=signal_source,
                    entry_price=float(entry_price_sim),
                    position_qty=float(entry_qty_sim),
                    leverage=float(leverage),
                    tp_pct=float(tp_pct),
                    sl_pct=float(sl_pct),
                    allocation_ratio=float(allocation_ratio),
                    trade_id=int(trade_id),
                    entry_order_id=None,
                    tp_order_id=None,
                    sl_order_id=None,
                    exchange_position_side="BOTH",
                    remaining_qty=float(entry_qty_sim),
                    realized_pnl_usdt=0.0,
                    reconciliation_status="TEST_DRY_RUN",
                    entry_ts=entry_ts_dt_sim,
                    entry_client_order_id=str(entry_client_order_id),
                    reason="position_opened_simulated",
                    is_test=True,
                )

                snap_kwargs = _snapshot_kwargs_from_meta(meta, entry_price_hint=float(entry_price_hint), filled_entry_price=float(entry_price_sim))

                record_trade_snapshot(
                    trade_id=int(trade_id),
                    symbol=symbol,
                    entry_ts=entry_ts_dt_sim,
                    direction=str(direction),
                    signal_source=str(signal_source),
                    regime=str(regime),
                    entry_score=_opt_float_strict(entry_score_val, "meta.entry_score"),
                    engine_total=_opt_float_strict(engine_total_val, "meta.engine_total"),
                    trend_strength=None,
                    atr_pct=None,
                    volume_zscore=None,
                    depth_ratio=None,
                    spread_pct=spread_pct_snapshot,
                    last_price=float(last_price),
                    risk_pct=float(allocation_ratio),
                    tp_pct=float(tp_pct),
                    sl_pct=float(sl_pct),
                    gpt_action="ENTER",
                    gpt_reason=str(signal.reason or ""),
                    equity_current_usdt=float(eq_cur_usdt),
                    equity_peak_usdt=float(eq_peak_usdt),
                    dd_pct=float(dd_pct_v),
                    **snap_kwargs,
                )

                closed_trade_id = close_latest_open_trade_returning_id(
                    symbol=symbol,
                    side=str(side_open),
                    exit_price=float(entry_price_sim),
                    exit_ts=entry_ts_dt_sim,
                    pnl_usdt=0.0,
                    close_reason="TEST_DRY_RUN",
                    regime_at_exit=str(regime),
                    pnl_pct_futures=0.0,
                    pnl_pct_spot_ref=None,
                    note="TEST_DRY_RUN closed immediately",
                )

                record_trade_exit_snapshot(
                    trade_id=int(closed_trade_id),
                    symbol=symbol,
                    close_ts=entry_ts_dt_sim,
                    close_price=float(entry_price_sim),
                    pnl=0.0,
                    close_reason="TEST_DRY_RUN",
                    exit_atr_pct=None,
                    exit_trend_strength=None,
                    exit_volume_zscore=None,
                    exit_depth_ratio=None,
                )

                _submit_tg_nonblocking(send_tg, f"[TEST_DRY_RUN] DB recorded + closed: {symbol} {direction} trade_id={trade_id}", label="send_tg")
                return None

            try:
                trade = open_position_with_tp_sl(
                    settings=self.settings,
                    symbol=symbol,
                    side_open=side_open,
                    qty=float(qty_raw),
                    entry_price_hint=float(entry_price_hint),
                    tp_pct=float(tp_pct),
                    sl_pct=float(sl_pct),
                    source=str(signal_source),
                    soft_mode=bool(soft_mode),
                    sl_floor_ratio=sl_floor_ratio,
                    entry_client_order_id=str(entry_client_order_id),
                )
            except ProtectionOrderVerificationError as e:
                log_event(
                    "ERROR",
                    symbol=symbol,
                    regime=regime,
                    source="execution_engine",
                    side=direction,
                    price=float(entry_price_hint),
                    qty=float(qty_raw),
                    leverage=float(leverage),
                    tp_pct=float(tp_pct),
                    sl_pct=float(sl_pct),
                    risk_pct=float(allocation_ratio),
                    reason="protection_order_verification_failed",
                    extra_json={
                        "signal_source": signal_source,
                        "entry_client_order_id": entry_client_order_id,
                        "error": str(e)[:240],
                    },
                )
                raise
            if trade is None:
                raise OrderFailed("open_position_with_tp_sl returned None (STRICT)")

            state = OrderState.FILLED
            setattr(trade, "order_state", state.value)

            (
                entry_order_id,
                tp_order_id,
                sl_order_id,
                pos_side,
                remaining_qty,
                realized_pnl_usdt,
                reconciliation_status,
            ) = _require_trade_exec_fields(trade, soft_mode=bool(soft_mode))

            entry_price = getattr(trade, "entry", getattr(trade, "entry_price", None))
            if entry_price is None:
                raise OrderFailed("trade.entry/entry_price is required (STRICT)")
            entry_price = _as_float(entry_price, "trade.entry_price", min_value=0.0)

            entry_qty = getattr(trade, "qty", None)
            if entry_qty is None:
                raise OrderFailed("trade.qty is required (STRICT)")
            entry_qty = _as_float(entry_qty, "trade.qty", min_value=0.0)

            entry_ts_dt = getattr(trade, "entry_ts", None)
            if not isinstance(entry_ts_dt, datetime) or entry_ts_dt.tzinfo is None or entry_ts_dt.tzinfo.utcoffset(entry_ts_dt) is None:
                raise OrderFailed("trade.entry_ts (tz-aware datetime) is required (STRICT)")

            if reconciliation_status != "PROTECTION_VERIFIED":
                raise OrderFailed(
                    f"trade.reconciliation_status must be PROTECTION_VERIFIED after ENTRY (STRICT), got={reconciliation_status!r}"
                )

            log_event(
                "PROTECTION_VERIFIED",
                symbol=symbol,
                regime=regime,
                source="execution_engine",
                side=direction,
                price=float(entry_price),
                qty=float(entry_qty),
                leverage=float(leverage),
                tp_pct=float(tp_pct),
                sl_pct=float(sl_pct),
                risk_pct=float(allocation_ratio),
                reason="protection_orders_verified",
                extra_json={
                    "signal_source": signal_source,
                    "entry_order_id": entry_order_id,
                    "tp_order_id": tp_order_id,
                    "sl_order_id": sl_order_id,
                    "entry_client_order_id": entry_client_order_id,
                    "exchange_position_side": pos_side,
                },
            )

            log_event(
                "on_entry_filled",
                symbol=symbol,
                regime=regime,
                source="execution_engine",
                side=direction,
                price=float(entry_price),
                qty=float(entry_qty),
                leverage=float(leverage),
                tp_pct=float(tp_pct),
                sl_pct=float(sl_pct),
                risk_pct=float(allocation_ratio),
                reason="entry_filled",
                extra_json={"signal_source": signal_source, "entry_client_order_id": entry_client_order_id, "entry_order_id": entry_order_id},
            )

            trade_last_synced_at = _require_tz_aware_datetime(
                getattr(trade, "last_synced_at", None),
                "trade.last_synced_at",
            )

            trade_id = record_trade_open_returning_id(
                symbol=symbol,
                side=str(side_open),
                qty=float(entry_qty),
                entry_price=float(entry_price),
                entry_ts=entry_ts_dt,
                is_auto=True,
                regime_at_entry=str(regime),
                strategy=str(signal_source),
                entry_score=_opt_float_strict(entry_score_val, "meta.entry_score"),
                trend_score_at_entry=None,
                range_score_at_entry=None,
                leverage=float(leverage),
                risk_pct=float(allocation_ratio),
                tp_pct=float(tp_pct),
                sl_pct=float(sl_pct),
                note=f"reason={str(signal.reason or '')[:180]}",
                entry_order_id=str(entry_order_id),
                tp_order_id=str(tp_order_id),
                sl_order_id=(str(sl_order_id) if sl_order_id is not None else None),
                exchange_position_side=str(pos_side),
                remaining_qty=float(remaining_qty),
                realized_pnl_usdt=float(realized_pnl_usdt),
                reconciliation_status="PROTECTION_VERIFIED",
                last_synced_at=trade_last_synced_at,
            )

            setattr(trade, "id", int(trade_id))
            setattr(trade, "reconciliation_status", "PROTECTION_VERIFIED")
            setattr(trade, "last_synced_at", trade_last_synced_at)

            _emit_position_event_strict(
                symbol=symbol,
                regime=regime,
                direction=direction,
                signal_source=signal_source,
                entry_price=float(entry_price),
                position_qty=float(entry_qty),
                leverage=float(leverage),
                tp_pct=float(tp_pct),
                sl_pct=float(sl_pct),
                allocation_ratio=float(allocation_ratio),
                trade_id=int(trade_id),
                entry_order_id=str(entry_order_id),
                tp_order_id=str(tp_order_id),
                sl_order_id=(str(sl_order_id) if sl_order_id is not None else None),
                exchange_position_side=str(pos_side),
                remaining_qty=float(remaining_qty),
                realized_pnl_usdt=float(realized_pnl_usdt),
                reconciliation_status="PROTECTION_VERIFIED",
                entry_ts=entry_ts_dt,
                entry_client_order_id=str(entry_client_order_id),
                reason="position_opened",
                is_test=False,
            )

            snap_kwargs = _snapshot_kwargs_from_meta(meta, entry_price_hint=float(entry_price_hint), filled_entry_price=float(entry_price))

            record_trade_snapshot(
                trade_id=int(trade_id),
                symbol=symbol,
                entry_ts=entry_ts_dt,
                direction=str(direction),
                signal_source=str(signal_source),
                regime=str(regime),
                entry_score=_opt_float_strict(entry_score_val, "meta.entry_score"),
                engine_total=_opt_float_strict(engine_total_val, "meta.engine_total"),
                trend_strength=None,
                atr_pct=None,
                volume_zscore=None,
                depth_ratio=None,
                spread_pct=spread_pct_snapshot,
                last_price=float(last_price),
                risk_pct=float(allocation_ratio),
                tp_pct=float(tp_pct),
                sl_pct=float(sl_pct),
                gpt_action="ENTER",
                gpt_reason=str(signal.reason or ""),
                equity_current_usdt=float(eq_cur_usdt),
                equity_peak_usdt=float(eq_peak_usdt),
                dd_pct=float(dd_pct_v),
                **snap_kwargs,
            )

            _submit_task_nonblocking(
                _exec_quality_task,
                str(symbol),
                str(direction),
                str(regime),
                float(entry_price_hint),
                float(entry_price),
                label="exec_quality",
            )

            _submit_tg_nonblocking(
                send_tg,
                f"[ENTRY][FILLED] {symbol} {direction} "
                f"alloc={allocation_ratio*100:.1f}% tp={tp_pct*100:.2f}% sl={sl_pct*100:.2f}% "
                f"price={float(entry_price):.4f} qty={float(entry_qty):.6f} src={signal_source}",
                label="send_tg",
            )

            return trade

        finally:
            _EXECUTION_LOCK.release()