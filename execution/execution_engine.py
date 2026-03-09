# execution/execution_engine.py
"""
========================================================
FILE: execution/execution_engine.py
AUTO-TRADER — ENTRY EXECUTION ORCHESTRATOR
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

핵심 변경 요약
- ROOT-CAUSE 복구: run_bot_ws.py 가 요구하는 ExecutionEngine 클래스 복원
- SIGNAL 계약 정합 복구: Signal(action/direction/tp_pct/sl_pct/risk_pct/meta) 입력을 엄격 해석
- 수량 계산 복구: qty 직접 입력 또는 risk_pct × capital × leverage / entry_price 방식 지원
- 기존 기능 삭제 없음: 실제 주문 실행 SSOT 는 order_executor.open_position_with_tp_sl() 유지

코드 정리 내용
- execution_engine 내부 중복 주문 실행/검증 로직 제거
- order_executor 공개 계약 재노출(re-export) 유지
- signal/meta 2계층 정규화 추가
- action 을 side 로 오인하던 잘못된 해석 제거
- 사용 안 하는 중복 구현/죽은 로직 제거

변경 이력
--------------------------------------------------------
- 2026-03-10:
  1) FIX(ROOT-CAUSE): ExecutionEngine 클래스 복구
     - __init__(settings)
     - execute(signal) -> Trade
  2) FIX(CONTRACT): run_bot_ws Signal 구조(action/direction/risk_pct/meta)와 정합 복구
  3) FIX(SYNTAX): entry_client_order_id 대입 구문 오타 수정
  4) FIX(SIZING): qty 누락 시 risk_pct 기반 수량 계산 복구
  5) CLEANUP: execution_engine 내부 중복 실행 로직 제거, order_executor 단일 SSOT 유지

- 2026-03-09:
  1) ROOT-CAUSE 확인
     - current HEAD 에서 ExecutionEngine 클래스 누락으로 ImportError 발생
     - run_bot_ws.py 의 import / instantiate 계약 불일치 확인
========================================================
"""

from __future__ import annotations

import logging
import math
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime
from typing import Any, Callable, Mapping, Optional

from execution.order_executor import (
    OrderExecutionError,
    OrderFillTimeoutError,
    PartialFillError,
    PositionVerificationError,
    ProtectionOrderVerificationError,
    SymbolFilters,
    cancel_order_safe,
    close_all_positions_market,
    close_position_market,
    ensure_trading_settings,
    get_symbol_filters,
    open_position_with_tp_sl,
    place_conditional,
    place_limit,
    place_market,
    set_tp_sl,
)
from settings import load_settings
from state.trader_state import Trade

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class EntryExecutionRequest:
    symbol: str
    side_open: str
    qty: float
    entry_price_hint: float
    tp_pct: float
    sl_pct: float
    source: str
    soft_mode: bool
    sl_floor_ratio: Optional[float]
    available_usdt: Optional[float]
    entry_client_order_id: Optional[str]
    risk_pct: Optional[float]


def _signal_to_mapping_strict(signal: Any) -> dict[str, Any]:
    if signal is None:
        raise OrderExecutionError("signal is None (STRICT)")

    if isinstance(signal, Mapping):
        data = dict(signal)
        if not data:
            raise OrderExecutionError("signal mapping is empty (STRICT)")
        return data

    model_dump = getattr(signal, "model_dump", None)
    if callable(model_dump):
        data = model_dump()
        if not isinstance(data, dict) or not data:
            raise OrderExecutionError("signal.model_dump() returned empty/non-dict (STRICT)")
        return data

    if is_dataclass(signal):
        data = asdict(signal)
        if not isinstance(data, dict) or not data:
            raise OrderExecutionError("dataclass signal converted to empty/non-dict (STRICT)")
        return data

    if hasattr(signal, "__dict__"):
        data = {
            str(k): v
            for k, v in vars(signal).items()
            if not str(k).startswith("_")
        }
        if not isinstance(data, dict) or not data:
            raise OrderExecutionError("signal.__dict__ is empty/invalid (STRICT)")
        return data

    raise OrderExecutionError(f"unsupported signal type for execution: {type(signal).__name__}")


def _meta_to_mapping_strict(signal_map: Mapping[str, Any]) -> dict[str, Any]:
    if "meta" not in signal_map or signal_map["meta"] is None:
        return {}

    meta = signal_map["meta"]
    if not isinstance(meta, Mapping):
        raise OrderExecutionError("signal.meta must be mapping when present (STRICT)")
    return dict(meta)


def _normalize_symbol_strict(value: Any) -> str:
    s = str(value).replace("-", "").replace("/", "").upper().strip()
    if not s:
        raise OrderExecutionError("symbol is empty (STRICT)")
    return s


def _normalize_open_side_strict(value: Any) -> str:
    s = str(value).upper().strip()
    if s in {"BUY", "LONG", "OPEN_LONG", "ENTER_LONG", "GO_LONG"}:
        return "BUY"
    if s in {"SELL", "SHORT", "OPEN_SHORT", "ENTER_SHORT", "GO_SHORT"}:
        return "SELL"
    raise OrderExecutionError(f"invalid open side for execution: {value!r}")


def _normalize_action_strict(value: Any) -> str:
    s = str(value).upper().strip()
    if not s:
        raise OrderExecutionError("action is empty (STRICT)")
    return s


def _normalize_positive_float_strict(value: Any, *, field_name: str) -> float:
    try:
        f = float(value)
    except Exception as e:
        raise OrderExecutionError(f"{field_name} must be numeric (STRICT)") from e
    if not math.isfinite(f):
        raise OrderExecutionError(f"{field_name} must be finite (STRICT)")
    if f <= 0.0:
        raise OrderExecutionError(f"{field_name} must be > 0 (STRICT)")
    return f


def _normalize_nonnegative_float_strict(value: Any, *, field_name: str) -> float:
    try:
        f = float(value)
    except Exception as e:
        raise OrderExecutionError(f"{field_name} must be numeric (STRICT)") from e
    if not math.isfinite(f):
        raise OrderExecutionError(f"{field_name} must be finite (STRICT)")
    if f < 0.0:
        raise OrderExecutionError(f"{field_name} must be >= 0 (STRICT)")
    return f


def _normalize_ratio_float_strict(value: Any, *, field_name: str) -> float:
    f = _normalize_positive_float_strict(value, field_name=field_name)
    if f > 1.0:
        raise OrderExecutionError(f"{field_name} must be <= 1.0 (STRICT)")
    return f


def _normalize_optional_positive_float_strict(value: Any, *, field_name: str) -> Optional[float]:
    if value is None:
        return None
    return _normalize_positive_float_strict(value, field_name=field_name)


def _normalize_source_strict(value: Any) -> str:
    s = str(value).strip()
    if not s:
        raise OrderExecutionError("source is empty (STRICT)")
    return s


def _normalize_optional_bool_strict(value: Any, *, field_name: str) -> Optional[bool]:
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    s = str(value).strip().lower()
    if s in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "f", "no", "n", "off"}:
        return False

    raise OrderExecutionError(f"{field_name} must be bool-convertible (STRICT)")


def _normalize_optional_client_order_id_strict(value: Any) -> Optional[str]:
    if value is None:
        return None

    cid = str(value).strip()
    if not cid:
        raise OrderExecutionError("entry_client_order_id is empty (STRICT)")
    if len(cid) > 36:
        raise OrderExecutionError("entry_client_order_id exceeds 36 chars (STRICT)")
    try:
        cid.encode("ascii")
    except UnicodeEncodeError as e:
        raise OrderExecutionError("entry_client_order_id must be ASCII (STRICT)") from e
    return cid


def _extract_normalized_value_strict(
    *,
    search_spaces: tuple[tuple[str, Mapping[str, Any]], ...],
    field_name: str,
    aliases: tuple[str, ...],
    normalizer: Callable[[Any], Any],
    required: bool,
) -> Any:
    found: list[tuple[str, Any]] = []

    for space_name, space in search_spaces:
        for alias in aliases:
            if alias not in space:
                continue
            raw = space[alias]
            if raw is None:
                continue
            normalized = normalizer(raw)
            found.append((f"{space_name}.{alias}", normalized))

    if not found:
        if required:
            raise OrderExecutionError(
                f"signal missing required field '{field_name}' "
                f"(aliases={aliases}) (STRICT)"
            )
        return None

    first_path, first_value = found[0]
    for path, value in found[1:]:
        if value != first_value:
            raise OrderExecutionError(
                f"signal field conflict for '{field_name}' "
                f"({first_path}={first_value!r}, {path}={value!r}) (STRICT)"
            )
    return first_value


def _extract_symbol_with_settings_scope_strict(
    *,
    search_spaces: tuple[tuple[str, Mapping[str, Any]], ...],
    settings: Any,
) -> str:
    symbol = _extract_normalized_value_strict(
        search_spaces=search_spaces,
        field_name="symbol",
        aliases=("symbol",),
        normalizer=_normalize_symbol_strict,
        required=False,
    )
    if symbol is not None:
        return symbol

    settings_symbol = getattr(settings, "symbol", None)
    if settings_symbol is None:
        raise OrderExecutionError("signal.symbol missing and settings.symbol missing (STRICT)")
    return _normalize_symbol_strict(settings_symbol)


def _extract_action_if_present_or_raise(
    *,
    search_spaces: tuple[tuple[str, Mapping[str, Any]], ...],
) -> None:
    action = _extract_normalized_value_strict(
        search_spaces=search_spaces,
        field_name="action",
        aliases=("action",),
        normalizer=_normalize_action_strict,
        required=False,
    )
    if action is None:
        return

    if action != "ENTER":
        raise OrderExecutionError(f"ExecutionEngine only accepts ENTER action (STRICT): got={action!r}")


def _compute_qty_from_risk_strict(
    *,
    capital_usdt: float,
    risk_pct: float,
    entry_price_hint: float,
    settings: Any,
) -> float:
    leverage_raw = getattr(settings, "leverage", None)
    if leverage_raw is None:
        raise OrderExecutionError("settings.leverage missing for qty sizing (STRICT)")

    try:
        leverage = float(leverage_raw)
    except Exception as e:
        raise OrderExecutionError("settings.leverage must be numeric (STRICT)") from e

    if not math.isfinite(leverage) or leverage <= 0.0:
        raise OrderExecutionError("settings.leverage must be finite > 0 (STRICT)")

    notional_usdt = capital_usdt * risk_pct * leverage
    if not math.isfinite(notional_usdt) or notional_usdt <= 0.0:
        raise OrderExecutionError("computed notional_usdt invalid (STRICT)")

    qty = notional_usdt / entry_price_hint
    if not math.isfinite(qty) or qty <= 0.0:
        raise OrderExecutionError("computed qty invalid (STRICT)")

    return float(qty)


def _normalize_entry_request_strict(signal: Any, settings: Any) -> EntryExecutionRequest:
    signal_map = _signal_to_mapping_strict(signal)
    meta_map = _meta_to_mapping_strict(signal_map)
    search_spaces = (("signal", signal_map), ("signal.meta", meta_map))

    _extract_action_if_present_or_raise(search_spaces=search_spaces)

    symbol = _extract_symbol_with_settings_scope_strict(
        search_spaces=search_spaces,
        settings=settings,
    )

    side_open = _extract_normalized_value_strict(
        search_spaces=search_spaces,
        field_name="side_open",
        aliases=("open_side", "side", "signal_side", "direction"),
        normalizer=_normalize_open_side_strict,
        required=True,
    )

    entry_price_hint = _extract_normalized_value_strict(
        search_spaces=search_spaces,
        field_name="entry_price_hint",
        aliases=("entry_price_hint", "entry_price", "price", "mark_price", "last_price", "close"),
        normalizer=lambda v: _normalize_positive_float_strict(v, field_name="entry_price_hint"),
        required=True,
    )

    tp_pct = _extract_normalized_value_strict(
        search_spaces=search_spaces,
        field_name="tp_pct",
        aliases=("tp_pct", "take_profit_pct", "target_pct"),
        normalizer=lambda v: _normalize_nonnegative_float_strict(v, field_name="tp_pct"),
        required=True,
    )

    sl_pct = _extract_normalized_value_strict(
        search_spaces=search_spaces,
        field_name="sl_pct",
        aliases=("sl_pct", "stop_loss_pct", "stop_pct"),
        normalizer=lambda v: _normalize_nonnegative_float_strict(v, field_name="sl_pct"),
        required=True,
    )

    source = _extract_normalized_value_strict(
        search_spaces=search_spaces,
        field_name="source",
        aliases=("source", "regime", "signal_source", "strategy_source", "entry_source", "strategy_type", "reason"),
        normalizer=_normalize_source_strict,
        required=True,
    )

    soft_mode_raw = _extract_normalized_value_strict(
        search_spaces=search_spaces,
        field_name="soft_mode",
        aliases=("soft_mode",),
        normalizer=lambda v: _normalize_optional_bool_strict(v, field_name="soft_mode"),
        required=False,
    )
    soft_mode = bool(soft_mode_raw) if soft_mode_raw is not None else False

    sl_floor_ratio = _extract_normalized_value_strict(
        search_spaces=search_spaces,
        field_name="sl_floor_ratio",
        aliases=("sl_floor_ratio",),
        normalizer=lambda v: _normalize_optional_positive_float_strict(v, field_name="sl_floor_ratio"),
        required=False,
    )

    capital_usdt = _extract_normalized_value_strict(
        search_spaces=search_spaces,
        field_name="available_usdt",
        aliases=("available_usdt", "equity_current_usdt", "current_equity_usdt"),
        normalizer=lambda v: _normalize_optional_positive_float_strict(v, field_name="available_usdt"),
        required=False,
    )

    entry_client_order_id = _extract_normalized_value_strict(
        search_spaces=search_spaces,
        field_name="entry_client_order_id",
        aliases=("entry_client_order_id", "client_order_id", "client_entry_id"),
        normalizer=_normalize_optional_client_order_id_strict,
        required=False,
    )

    explicit_qty = _extract_normalized_value_strict(
        search_spaces=search_spaces,
        field_name="qty",
        aliases=("qty", "quantity", "final_qty", "order_qty"),
        normalizer=lambda v: _normalize_positive_float_strict(v, field_name="qty"),
        required=False,
    )

    risk_pct = _extract_normalized_value_strict(
        search_spaces=search_spaces,
        field_name="risk_pct",
        aliases=("risk_pct", "allocation_ratio", "dynamic_allocation_ratio", "dynamic_risk_pct"),
        normalizer=lambda v: _normalize_ratio_float_strict(v, field_name="risk_pct"),
        required=False,
    )

    if explicit_qty is not None:
        qty = explicit_qty
    else:
        if risk_pct is None:
            raise OrderExecutionError("qty missing and risk_pct missing (STRICT)")
        if capital_usdt is None:
            raise OrderExecutionError(
                "qty missing and capital_usdt source missing "
                "(required aliases: available_usdt/equity_current_usdt/current_equity_usdt) (STRICT)"
            )
        qty = _compute_qty_from_risk_strict(
            capital_usdt=float(capital_usdt),
            risk_pct=float(risk_pct),
            entry_price_hint=float(entry_price_hint),
            settings=settings,
        )

    if not soft_mode and sl_pct <= 0.0:
        raise OrderExecutionError("sl_pct must be > 0 when soft_mode=False (STRICT)")

    return EntryExecutionRequest(
        symbol=symbol,
        side_open=side_open,
        qty=float(qty),
        entry_price_hint=float(entry_price_hint),
        tp_pct=float(tp_pct),
        sl_pct=float(sl_pct),
        source=source,
        soft_mode=soft_mode,
        sl_floor_ratio=sl_floor_ratio,
        available_usdt=None if capital_usdt is None else float(capital_usdt),
        entry_client_order_id=entry_client_order_id,
        risk_pct=None if risk_pct is None else float(risk_pct),
    )


def _validate_trade_contract_strict(trade: Trade, req: EntryExecutionRequest) -> None:
    if not isinstance(trade, Trade):
        raise OrderExecutionError(f"execute() returned non-Trade object: {type(trade).__name__} (STRICT)")

    trade_symbol = _normalize_symbol_strict(getattr(trade, "symbol", None))
    if trade_symbol != req.symbol:
        raise OrderExecutionError(
            f"Trade.symbol mismatch (got={trade_symbol}, expected={req.symbol}) (STRICT)"
        )

    trade_side = _normalize_open_side_strict(getattr(trade, "side", None))
    if trade_side != req.side_open:
        raise OrderExecutionError(
            f"Trade.side mismatch (got={trade_side}, expected={req.side_open}) (STRICT)"
        )

    try:
        trade_qty = float(getattr(trade, "qty"))
    except Exception as e:
        raise OrderExecutionError("Trade.qty missing/invalid (STRICT)") from e
    if not math.isfinite(trade_qty) or trade_qty <= 0.0:
        raise OrderExecutionError("Trade.qty must be finite > 0 (STRICT)")

    entry_price_raw = getattr(trade, "entry_price", None)
    if entry_price_raw is None:
        entry_price_raw = getattr(trade, "entry", None)
    try:
        entry_price = float(entry_price_raw)
    except Exception as e:
        raise OrderExecutionError("Trade.entry_price/entry missing or invalid (STRICT)") from e
    if not math.isfinite(entry_price) or entry_price <= 0.0:
        raise OrderExecutionError("Trade.entry_price/entry must be finite > 0 (STRICT)")

    entry_order_id = getattr(trade, "entry_order_id", None)
    if entry_order_id is None or not str(entry_order_id).strip():
        raise OrderExecutionError("Trade.entry_order_id missing (STRICT)")

    tp_order_id = getattr(trade, "tp_order_id", None)
    if tp_order_id is None or not str(tp_order_id).strip():
        raise OrderExecutionError("Trade.tp_order_id missing (STRICT)")

    if not req.soft_mode:
        sl_order_id = getattr(trade, "sl_order_id", None)
        if sl_order_id is None or not str(sl_order_id).strip():
            raise OrderExecutionError("Trade.sl_order_id missing while soft_mode=False (STRICT)")

    entry_ts = getattr(trade, "entry_ts", None)
    if not isinstance(entry_ts, datetime):
        raise OrderExecutionError("Trade.entry_ts must be datetime (STRICT)")
    if entry_ts.tzinfo is None or entry_ts.utcoffset() is None:
        raise OrderExecutionError("Trade.entry_ts must be timezone-aware (STRICT)")

    reconciliation_status = str(getattr(trade, "reconciliation_status", "") or "").upper().strip()
    if reconciliation_status != "PROTECTION_VERIFIED":
        raise OrderExecutionError(
            f"Trade.reconciliation_status mismatch "
            f"(got={reconciliation_status!r}, expected='PROTECTION_VERIFIED') (STRICT)"
        )

    if req.entry_client_order_id is not None:
        client_entry_id = getattr(trade, "client_entry_id", None)
        if str(client_entry_id or "").strip() != req.entry_client_order_id:
            raise OrderExecutionError(
                f"Trade.client_entry_id mismatch "
                f"(got={client_entry_id!r}, expected={req.entry_client_order_id!r}) (STRICT)"
            )


class ExecutionEngine:
    """
    Entry execution orchestrator.

    역할
    - run_bot_ws 가 기대하는 클래스 기반 execute(signal) 계약을 제공한다.
    - Signal(action/direction/tp_pct/sl_pct/risk_pct/meta) 입력을 엄격하게 정규화한다.
    - qty 직접 입력 또는 risk_pct 기반 수량 계산 후 order_executor.open_position_with_tp_sl() 를 호출한다.
    - 주문 실행 / 체결 검증 / 보호주문 검증의 실제 SSOT 는 order_executor 이다.

    절대 원칙
    - signal 누락/충돌/모호성은 즉시 예외
    - 임의 기본값/추정/폴백 금지
    - Trade 반환 계약은 PROTECTION_VERIFIED 까지 검증
    """

    def __init__(self, settings: Optional[Any] = None) -> None:
        self._settings = settings if settings is not None else load_settings()
        if self._settings is None:
            raise OrderExecutionError("settings resolution failed in ExecutionEngine (STRICT)")

    @property
    def settings(self) -> Any:
        return self._settings

    def execute(self, signal: Any) -> Trade:
        req = _normalize_entry_request_strict(signal, self._settings)

        logger.info(
            "ExecutionEngine.execute start "
            "(symbol=%s side=%s qty=%s entry_price_hint=%s tp_pct=%s sl_pct=%s risk_pct=%s source=%s soft_mode=%s)",
            req.symbol,
            req.side_open,
            req.qty,
            req.entry_price_hint,
            req.tp_pct,
            req.sl_pct,
            req.risk_pct,
            req.source,
            req.soft_mode,
        )

        trade = open_position_with_tp_sl(
            self._settings,
            symbol=req.symbol,
            side_open=req.side_open,
            qty=req.qty,
            entry_price_hint=req.entry_price_hint,
            tp_pct=req.tp_pct,
            sl_pct=req.sl_pct,
            source=req.source,
            soft_mode=req.soft_mode,
            sl_floor_ratio=req.sl_floor_ratio,
            available_usdt=req.available_usdt,
            entry_client_order_id=req.entry_client_order_id,
        )

        if trade is None:
            raise OrderExecutionError("open_position_with_tp_sl returned None (STRICT)")

        _validate_trade_contract_strict(trade, req)

        logger.info(
            "ExecutionEngine.execute success "
            "(symbol=%s side=%s qty=%s entry=%s entry_order_id=%s)",
            getattr(trade, "symbol", None),
            getattr(trade, "side", None),
            getattr(trade, "qty", None),
            getattr(trade, "entry_price", getattr(trade, "entry", None)),
            getattr(trade, "entry_order_id", None),
        )
        return trade


__all__ = [
    "ExecutionEngine",
    "EntryExecutionRequest",
    "OrderExecutionError",
    "OrderFillTimeoutError",
    "PartialFillError",
    "PositionVerificationError",
    "ProtectionOrderVerificationError",
    "SymbolFilters",
    "open_position_with_tp_sl",
    "place_market",
    "place_limit",
    "place_conditional",
    "cancel_order_safe",
    "set_tp_sl",
    "close_position_market",
    "close_all_positions_market",
    "get_symbol_filters",
    "ensure_trading_settings",
]