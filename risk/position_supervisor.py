"""
========================================================
FILE: risk/position_supervisor.py
ROLE:
- 진입 후 포지션 안전 상태를 감시하는 Position Supervisor
- 거래소 실제 포지션 / 보호주문(TP/SL) / 로컬 Trade 정합성을 strict 하게 검증한다
- 보호주문 누락 / 수량 desync / 방향 불일치 / liquidation 위험을 SAFE_STOP 급 상태로 승격한다

CORE RESPONSIBILITIES:
- exchange authoritative position snapshot strict 조회
- open protective orders strict 조회 및 TP/SL visibility 검증
- Trade.remaining_qty / Trade.side / Trade.entry_price 와 exchange state 정합 검증
- liquidation distance strict 검증
- soft_mode 여부를 Trade 계약에서 해석
- monitoring / engine loop 에서 사용할 supervisor snapshot 제공

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- position supervisor 는 신규 주문 실행을 하지 않는다
- 보호주문/포지션 이상 감지 시 즉시 예외
- source / symbol / side / qty / price 의미를 임의 보정하지 않는다
- exchange/local/db 불일치 상태에서 정상 진행 금지
- print 금지, logging만 사용
- dict.get(key, default) / getattr(..., default) 금지
- 추정 복구 금지, 명시적 상태 검증만 수행
========================================================

CHANGE HISTORY:
- 2026-03-14
  1) 신규 생성: Position Supervisor Layer 도입
  2) FEAT(STATE): exchange position ↔ local Trade strict 정합 검증 추가
  3) FEAT(PROTECTION): TP/SL 보호주문 visibility / uniqueness 검증 추가
  4) FEAT(RISK): liquidation distance minimum guard 검증 추가
  5) FEAT(API): PositionSupervisor / build_position_supervisor_or_raise / run_position_supervisor_once_or_raise 추가
========================================================
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from common.exceptions_strict import (
    StrictConfigError,
    StrictDataError,
    StrictStateError,
)
from common.strict_validators import (
    StrictValidationError,
    normalize_symbol as _sv_normalize_symbol,
    require_float as _sv_require_float,
    require_nonempty_str as _sv_require_nonempty_str,
)
from execution.exchange_api import fetch_open_orders, fetch_open_positions
from execution.order_executor import SymbolFilters, get_symbol_filters
from state.trader_state import Trade

logger = logging.getLogger(__name__)


class PositionSupervisorError(RuntimeError):
    """Base error for position supervisor."""


class PositionSupervisorConfigError(StrictConfigError, PositionSupervisorError):
    """Settings/config contract violation."""


class PositionSupervisorContractError(StrictDataError, PositionSupervisorError):
    """Input / payload contract violation."""


class PositionSupervisorStateError(StrictStateError, PositionSupervisorError):
    """Exchange/local runtime state mismatch."""


class PositionProtectionMissingError(PositionSupervisorStateError):
    """Protection order missing or invalid."""


class PositionDesyncError(PositionSupervisorStateError):
    """Exchange position and local Trade are inconsistent."""


class PositionLiquidationRiskError(PositionSupervisorStateError):
    """Liquidation distance is below configured threshold."""


@dataclass(frozen=True, slots=True)
class ProtectionOrdersSnapshot:
    symbol: str
    position_side: str
    close_side: str
    tp_order_id: str
    sl_order_id: Optional[str]
    tp_client_order_id: Optional[str]
    sl_client_order_id: Optional[str]
    visible_order_ids: Tuple[str, ...]
    soft_mode: bool


@dataclass(frozen=True, slots=True)
class PositionSupervisorSnapshot:
    symbol: str
    checked_at_ts: float
    has_position: bool
    position_side: Optional[str]
    exchange_position_side: Optional[str]
    abs_qty: Optional[float]
    entry_price: Optional[float]
    liquidation_price: Optional[float]
    liquidation_distance_pct: Optional[float]
    expected_remaining_qty: Optional[float]
    expected_trade_side: Optional[str]
    expected_entry_price: Optional[float]
    reconciliation_status: Optional[str]
    protection: Optional[ProtectionOrdersSnapshot]


@dataclass(frozen=True, slots=True)
class PositionSupervisorConfig:
    symbol: str
    hard_liquidation_distance_pct_min: float

    def validate_or_raise(self) -> None:
        try:
            normalized_symbol = _sv_normalize_symbol(self.symbol, name="config.symbol")
        except StrictValidationError as exc:
            raise PositionSupervisorConfigError(str(exc)) from exc

        try:
            liq_min = _sv_require_float(
                self.hard_liquidation_distance_pct_min,
                "config.hard_liquidation_distance_pct_min",
                min_value=0.0,
            )
        except StrictValidationError as exc:
            raise PositionSupervisorConfigError(str(exc)) from exc

        if not normalized_symbol:
            raise PositionSupervisorConfigError("config.symbol is empty (STRICT)")
        if liq_min < 0.0:
            raise PositionSupervisorConfigError("config.hard_liquidation_distance_pct_min must be >= 0 (STRICT)")


def _raise_contract_from_strict(exc: StrictValidationError) -> PositionSupervisorContractError:
    return PositionSupervisorContractError(str(exc))


def _normalize_symbol_strict(value: Any, *, name: str) -> str:
    try:
        return _sv_normalize_symbol(value, name=name)
    except StrictValidationError as exc:
        raise _raise_contract_from_strict(exc) from exc


def _require_nonempty_str_strict(value: Any, *, name: str) -> str:
    try:
        return _sv_require_nonempty_str(value, name)
    except StrictValidationError as exc:
        raise _raise_contract_from_strict(exc) from exc


def _require_nonnegative_float_strict(value: Any, *, name: str) -> float:
    try:
        return _sv_require_float(value, name, min_value=0.0)
    except StrictValidationError as exc:
        raise _raise_contract_from_strict(exc) from exc


def _require_positive_float_strict(value: Any, *, name: str) -> float:
    f = _require_nonnegative_float_strict(value, name=name)
    if f <= 0.0:
        raise PositionSupervisorContractError(f"{name} must be > 0 (STRICT)")
    return f


def _require_attr_exists_strict(obj: Any, attr_name: str, owner_name: str) -> Any:
    if obj is None:
        raise PositionSupervisorContractError(f"{owner_name} is None (STRICT)")
    if not hasattr(obj, attr_name):
        raise PositionSupervisorContractError(f"{owner_name}.{attr_name} missing (STRICT)")
    return getattr(obj, attr_name)


def _require_attr_value_strict(obj: Any, attr_name: str, owner_name: str) -> Any:
    value = _require_attr_exists_strict(obj, attr_name, owner_name)
    if value is None:
        raise PositionSupervisorContractError(f"{owner_name}.{attr_name} missing (STRICT)")
    return value


def _require_trade_strict(expected_trade: Any) -> Trade:
    if not isinstance(expected_trade, Trade):
        raise PositionSupervisorContractError(
            f"expected_trade must be Trade or None (STRICT), got={type(expected_trade).__name__}"
        )
    return expected_trade


def _normalize_trade_side_to_position_side_strict(value: Any, *, name: str) -> str:
    s = str(value).upper().strip()
    if s in {"BUY", "LONG"}:
        return "LONG"
    if s in {"SELL", "SHORT"}:
        return "SHORT"
    raise PositionSupervisorContractError(f"{name} must be BUY/SELL/LONG/SHORT (STRICT), got={value!r}")


def _normalize_position_side_strict(value: Any, *, name: str) -> str:
    s = str(value).upper().strip()
    if s in {"BOTH", "LONG", "SHORT"}:
        return s
    raise PositionSupervisorContractError(f"{name} invalid positionSide (STRICT): {value!r}")


def _normalize_close_side_from_position_side_strict(position_side: str) -> str:
    ps = _normalize_position_side_strict(position_side, name="position_side")
    if ps == "LONG":
        return "SELL"
    if ps == "SHORT":
        return "BUY"
    raise PositionSupervisorContractError("position_side BOTH cannot derive close_side (STRICT)")


def _decimal_abs(value: Decimal) -> Decimal:
    return value if value >= 0 else -value


def _require_decimal_strict(value: Any, *, name: str) -> Decimal:
    try:
        dec = Decimal(str(value))
    except Exception as exc:
        raise PositionSupervisorContractError(f"{name} not decimal-convertible (STRICT)") from exc
    return dec


def _extract_trade_entry_price_strict(trade: Trade) -> float:
    if hasattr(trade, "entry_price"):
        raw = getattr(trade, "entry_price")
    elif hasattr(trade, "entry"):
        raw = getattr(trade, "entry")
    else:
        raise PositionSupervisorContractError("trade.entry_price/entry missing (STRICT)")

    if raw is None:
        raise PositionSupervisorContractError("trade.entry_price/entry missing (STRICT)")

    try:
        entry_price = float(raw)
    except Exception as exc:
        raise PositionSupervisorContractError("trade.entry_price/entry invalid (STRICT)") from exc

    if not math.isfinite(entry_price) or entry_price <= 0.0:
        raise PositionSupervisorContractError("trade.entry_price/entry must be finite > 0 (STRICT)")
    return entry_price


def _extract_trade_remaining_qty_strict(trade: Trade) -> float:
    if hasattr(trade, "remaining_qty"):
        raw = getattr(trade, "remaining_qty")
        if raw is not None:
            qty = _require_positive_float_strict(raw, name="trade.remaining_qty")
            return qty

    qty_raw = _require_attr_value_strict(trade, "qty", "trade")
    return _require_positive_float_strict(qty_raw, name="trade.qty")


def _extract_trade_soft_mode_strict(trade: Trade) -> bool:
    if not hasattr(trade, "sl_order_id"):
        raise PositionSupervisorContractError("trade.sl_order_id missing (STRICT)")
    sl_order_id = getattr(trade, "sl_order_id")
    return sl_order_id is None


def _is_protection_order_type_strict(order_type: str) -> bool:
    t = str(order_type).upper().strip()
    return t in {"STOP_MARKET", "TAKE_PROFIT_MARKET", "STOP", "TAKE_PROFIT"}


def _is_tp_order_type_strict(order_type: str) -> bool:
    t = str(order_type).upper().strip()
    return t in {"TAKE_PROFIT_MARKET", "TAKE_PROFIT"}


def _is_sl_order_type_strict(order_type: str) -> bool:
    t = str(order_type).upper().strip()
    return t in {"STOP_MARKET", "STOP"}


def _require_exchange_positions_rows_strict(rows: Any, *, symbol: str) -> List[Dict[str, Any]]:
    if not isinstance(rows, list):
        raise PositionSupervisorStateError("fetch_open_positions returned non-list (STRICT)")

    out: List[Dict[str, Any]] = []
    sym = _normalize_symbol_strict(symbol, name="symbol")

    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            raise PositionSupervisorStateError(f"fetch_open_positions[{idx}] must be dict (STRICT)")
        if "symbol" not in row:
            raise PositionSupervisorStateError(f"fetch_open_positions[{idx}].symbol missing (STRICT)")
        row_symbol = _normalize_symbol_strict(row["symbol"], name=f"fetch_open_positions[{idx}].symbol")
        if row_symbol != sym:
            continue
        out.append(row)

    return out


def _extract_live_one_way_position_strict(
    *,
    symbol: str,
    filters: SymbolFilters,
) -> Tuple[Optional[str], Optional[str], Optional[Decimal], Optional[float], Optional[float]]:
    rows = _require_exchange_positions_rows_strict(fetch_open_positions(symbol), symbol=symbol)
    zero_tol = filters.market_step / Decimal("2")

    live_rows: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows):
        if "positionAmt" not in row:
            raise PositionSupervisorStateError(f"position[{idx}].positionAmt missing (STRICT)")
        amt = _require_decimal_strict(row["positionAmt"], name=f"position[{idx}].positionAmt")
        if _decimal_abs(amt) > zero_tol:
            live_rows.append(row)

    if not live_rows:
        return None, None, None, None, None

    if len(live_rows) > 1:
        raise PositionDesyncError(
            f"multiple live exchange positions detected for one-way symbol={_normalize_symbol_strict(symbol, name='symbol')} (STRICT)"
        )

    row = live_rows[0]

    if "positionAmt" not in row:
        raise PositionSupervisorStateError("position.positionAmt missing (STRICT)")
    amt = _require_decimal_strict(row["positionAmt"], name="position.positionAmt")

    if amt > 0:
        position_side = "LONG"
    elif amt < 0:
        position_side = "SHORT"
    else:
        raise PositionSupervisorStateError("position.positionAmt is zero unexpectedly (STRICT)")

    exchange_position_side = "BOTH"
    if "positionSide" in row:
        exchange_position_side = _normalize_position_side_strict(row["positionSide"], name="position.positionSide")

    if "entryPrice" not in row:
        raise PositionSupervisorStateError("position.entryPrice missing (STRICT)")
    entry_price = _require_positive_float_strict(row["entryPrice"], name="position.entryPrice")

    liquidation_price: Optional[float] = None
    if "liquidationPrice" in row and row["liquidationPrice"] is not None:
        liq_price_raw = _require_nonnegative_float_strict(row["liquidationPrice"], name="position.liquidationPrice")
        if liq_price_raw > 0.0:
            liquidation_price = liq_price_raw

    return position_side, exchange_position_side, _decimal_abs(amt), entry_price, liquidation_price


def _require_open_orders_rows_strict(rows: Any, *, symbol: str) -> List[Dict[str, Any]]:
    if not isinstance(rows, list):
        raise PositionSupervisorStateError("fetch_open_orders returned non-list (STRICT)")

    out: List[Dict[str, Any]] = []
    sym = _normalize_symbol_strict(symbol, name="symbol")

    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            raise PositionSupervisorStateError(f"fetch_open_orders[{idx}] must be dict (STRICT)")
        if "symbol" not in row:
            raise PositionSupervisorStateError(f"fetch_open_orders[{idx}].symbol missing (STRICT)")
        row_symbol = _normalize_symbol_strict(row["symbol"], name=f"fetch_open_orders[{idx}].symbol")
        if row_symbol != sym:
            continue
        out.append(row)

    return out


def _extract_protection_orders_snapshot_strict(
    *,
    symbol: str,
    position_side: str,
    expected_trade: Trade,
) -> ProtectionOrdersSnapshot:
    rows = _require_open_orders_rows_strict(fetch_open_orders(symbol), symbol=symbol)
    close_side = _normalize_close_side_from_position_side_strict(position_side)
    soft_mode = _extract_trade_soft_mode_strict(expected_trade)

    expected_tp_order_id = _require_attr_value_strict(expected_trade, "tp_order_id", "trade")
    expected_tp_order_id_str = _require_nonempty_str_strict(expected_tp_order_id, name="trade.tp_order_id")

    expected_sl_order_id_str: Optional[str] = None
    if not soft_mode:
        expected_sl_order_id = _require_attr_value_strict(expected_trade, "sl_order_id", "trade")
        expected_sl_order_id_str = _require_nonempty_str_strict(expected_sl_order_id, name="trade.sl_order_id")

    expected_tp_client_order_id: Optional[str] = None
    if hasattr(expected_trade, "client_tp_id"):
        raw = getattr(expected_trade, "client_tp_id")
        if raw is not None:
            expected_tp_client_order_id = _require_nonempty_str_strict(raw, name="trade.client_tp_id")

    expected_sl_client_order_id: Optional[str] = None
    if hasattr(expected_trade, "client_sl_id"):
        raw = getattr(expected_trade, "client_sl_id")
        if raw is not None:
            expected_sl_client_order_id = _require_nonempty_str_strict(raw, name="trade.client_sl_id")

    tp_rows: List[Dict[str, Any]] = []
    sl_rows: List[Dict[str, Any]] = []

    for idx, row in enumerate(rows):
        if "side" not in row:
            raise PositionProtectionMissingError(f"open_order[{idx}].side missing (STRICT)")
        row_side = _normalize_trade_side_to_position_side_strict(row["side"], name=f"open_order[{idx}].side")
        if row_side == position_side:
            continue

        if "positionSide" in row:
            row_position_side = _normalize_position_side_strict(row["positionSide"], name=f"open_order[{idx}].positionSide")
            if row_position_side not in {"BOTH", position_side}:
                continue

        if "type" not in row:
            raise PositionProtectionMissingError(f"open_order[{idx}].type missing (STRICT)")
        order_type = _require_nonempty_str_strict(row["type"], name=f"open_order[{idx}].type")
        if not _is_protection_order_type_strict(order_type):
            continue

        if "reduceOnly" in row and not bool(row["reduceOnly"]) and not bool(row["closePosition"]) if "closePosition" in row else True:
            # 보호주문이 아니면 제외
            continue

        if "orderId" not in row:
            raise PositionProtectionMissingError(f"open_order[{idx}].orderId missing (STRICT)")
        order_id = _require_nonempty_str_strict(row["orderId"], name=f"open_order[{idx}].orderId")

        if _is_tp_order_type_strict(order_type):
            tp_rows.append(row)
        elif _is_sl_order_type_strict(order_type):
            sl_rows.append(row)
        else:
            raise PositionProtectionMissingError(f"unexpected protection order type: {order_type!r} (STRICT)")

        _ = order_id

    if len(tp_rows) != 1:
        raise PositionProtectionMissingError(
            f"TP visibility violation (STRICT): expected exactly 1 TP open order, got={len(tp_rows)}"
        )

    if not soft_mode and len(sl_rows) != 1:
        raise PositionProtectionMissingError(
            f"SL visibility violation (STRICT): expected exactly 1 SL open order, got={len(sl_rows)}"
        )

    if soft_mode and len(sl_rows) > 0:
        raise PositionProtectionMissingError(
            "soft_mode trade must not keep visible SL order (STRICT)"
        )

    tp_row = tp_rows[0]
    if "orderId" not in tp_row:
        raise PositionProtectionMissingError("tp_row.orderId missing (STRICT)")
    tp_order_id = _require_nonempty_str_strict(tp_row["orderId"], name="tp_row.orderId")

    if tp_order_id != expected_tp_order_id_str:
        raise PositionProtectionMissingError(
            f"TP orderId mismatch (STRICT): visible={tp_order_id} expected={expected_tp_order_id_str}"
        )

    visible_order_ids: List[str] = [tp_order_id]

    sl_order_id: Optional[str] = None
    if not soft_mode:
        sl_row = sl_rows[0]
        if "orderId" not in sl_row:
            raise PositionProtectionMissingError("sl_row.orderId missing (STRICT)")
        sl_order_id = _require_nonempty_str_strict(sl_row["orderId"], name="sl_row.orderId")

        if expected_sl_order_id_str is None:
            raise PositionProtectionMissingError("expected SL order id missing while soft_mode=False (STRICT)")

        if sl_order_id != expected_sl_order_id_str:
            raise PositionProtectionMissingError(
                f"SL orderId mismatch (STRICT): visible={sl_order_id} expected={expected_sl_order_id_str}"
            )

        if sl_order_id == tp_order_id:
            raise PositionProtectionMissingError("TP/SL orderId must be distinct (STRICT)")

        visible_order_ids.append(sl_order_id)

    return ProtectionOrdersSnapshot(
        symbol=_normalize_symbol_strict(symbol, name="symbol"),
        position_side=position_side,
        close_side=close_side,
        tp_order_id=tp_order_id,
        sl_order_id=sl_order_id,
        tp_client_order_id=expected_tp_client_order_id,
        sl_client_order_id=expected_sl_client_order_id,
        visible_order_ids=tuple(visible_order_ids),
        soft_mode=soft_mode,
    )


def _compute_liquidation_distance_pct_strict(
    *,
    entry_price: float,
    liquidation_price: Optional[float],
) -> Optional[float]:
    if liquidation_price is None:
        return None
    if liquidation_price <= 0.0:
        raise PositionLiquidationRiskError("liquidation_price must be > 0 when present (STRICT)")
    if entry_price <= 0.0:
        raise PositionLiquidationRiskError("entry_price must be > 0 (STRICT)")

    dist_pct = abs(entry_price - liquidation_price) / entry_price * 100.0
    if not math.isfinite(dist_pct):
        raise PositionLiquidationRiskError("liquidation distance is non-finite (STRICT)")
    return float(dist_pct)


def _validate_expected_trade_vs_exchange_strict(
    *,
    expected_trade: Trade,
    exchange_position_side: str,
    exchange_abs_qty: Decimal,
    exchange_entry_price: float,
    filters: SymbolFilters,
) -> Tuple[float, float, str]:
    trade = _require_trade_strict(expected_trade)

    trade_symbol = _normalize_symbol_strict(_require_attr_value_strict(trade, "symbol", "trade"), name="trade.symbol")
    if trade_symbol != filters.symbol:
        raise PositionDesyncError(
            f"trade.symbol mismatch vs supervisor symbol (STRICT): trade={trade_symbol} supervisor={filters.symbol}"
        )

    expected_side = _normalize_trade_side_to_position_side_strict(
        _require_attr_value_strict(trade, "side", "trade"),
        name="trade.side",
    )
    if expected_side != exchange_position_side:
        raise PositionDesyncError(
            f"position side desync (STRICT): trade={expected_side} exchange={exchange_position_side}"
        )

    expected_remaining_qty = _extract_trade_remaining_qty_strict(trade)
    exchange_abs_qty_f = float(exchange_abs_qty)
    qty_tol = float(filters.market_step / Decimal("2"))
    qty_diff = abs(exchange_abs_qty_f - expected_remaining_qty)
    if qty_diff > qty_tol:
        raise PositionDesyncError(
            f"position qty desync (STRICT): exchange={exchange_abs_qty_f} trade={expected_remaining_qty} tol={qty_tol}"
        )

    expected_entry_price = _extract_trade_entry_price_strict(trade)
    price_tol = float(filters.tick_size * Decimal("2"))
    price_diff = abs(exchange_entry_price - expected_entry_price)
    if price_diff > price_tol:
        raise PositionDesyncError(
            f"entry price desync (STRICT): exchange={exchange_entry_price} trade={expected_entry_price} tol={price_tol}"
        )

    reconciliation_status = _require_nonempty_str_strict(
        _require_attr_value_strict(trade, "reconciliation_status", "trade"),
        name="trade.reconciliation_status",
    ).upper()
    if reconciliation_status != "PROTECTION_VERIFIED":
        raise PositionDesyncError(
            f"trade.reconciliation_status must be PROTECTION_VERIFIED for supervised open trade (STRICT), got={reconciliation_status!r}"
        )

    return expected_remaining_qty, expected_entry_price, reconciliation_status


class PositionSupervisor:
    """
    진입 후 포지션 안전 감시 전용 레이어.

    사용 원칙
    - exchange authoritative state 를 진실값으로 사용
    - local Trade 는 그 상태와 strict match 되어야 한다
    - mismatch / protection missing / liquidation risk 는 SAFE_STOP 급 예외로 승격
    """

    def __init__(self, config: PositionSupervisorConfig) -> None:
        if not isinstance(config, PositionSupervisorConfig):
            raise PositionSupervisorConfigError("config must be PositionSupervisorConfig (STRICT)")
        config.validate_or_raise()
        self._config = config

    @property
    def config(self) -> PositionSupervisorConfig:
        return self._config

    def inspect_or_raise(self, *, expected_trade: Optional[Trade]) -> PositionSupervisorSnapshot:
        checked_at_ts = time.time()
        symbol = self._config.symbol
        filters = get_symbol_filters(symbol)
        if not isinstance(filters, SymbolFilters):
            raise PositionSupervisorContractError("get_symbol_filters must return SymbolFilters (STRICT)")

        position_side, exchange_position_side, abs_qty_dec, entry_price, liquidation_price = _extract_live_one_way_position_strict(
            symbol=symbol,
            filters=filters,
        )

        if abs_qty_dec is None:
            if expected_trade is not None:
                trade = _require_trade_strict(expected_trade)
                trade_symbol = _normalize_symbol_strict(
                    _require_attr_value_strict(trade, "symbol", "trade"),
                    name="trade.symbol",
                )
                raise PositionDesyncError(
                    f"local trade exists but exchange has no live position (STRICT): symbol={trade_symbol}"
                )

            return PositionSupervisorSnapshot(
                symbol=symbol,
                checked_at_ts=checked_at_ts,
                has_position=False,
                position_side=None,
                exchange_position_side=None,
                abs_qty=None,
                entry_price=None,
                liquidation_price=None,
                liquidation_distance_pct=None,
                expected_remaining_qty=None,
                expected_trade_side=None,
                expected_entry_price=None,
                reconciliation_status=None,
                protection=None,
            )

        if position_side is None or entry_price is None:
            raise PositionSupervisorStateError("live position contract incomplete (STRICT)")

        if expected_trade is None:
            raise PositionDesyncError(
                f"exchange has live position but local expected_trade is None (STRICT): symbol={symbol} side={position_side}"
            )

        expected_remaining_qty, expected_entry_price, reconciliation_status = _validate_expected_trade_vs_exchange_strict(
            expected_trade=expected_trade,
            exchange_position_side=position_side,
            exchange_abs_qty=abs_qty_dec,
            exchange_entry_price=entry_price,
            filters=filters,
        )

        protection_snapshot = _extract_protection_orders_snapshot_strict(
            symbol=symbol,
            position_side=position_side,
            expected_trade=expected_trade,
        )

        liquidation_distance_pct = _compute_liquidation_distance_pct_strict(
            entry_price=entry_price,
            liquidation_price=liquidation_price,
        )

        if liquidation_distance_pct is not None:
            min_required = self._config.hard_liquidation_distance_pct_min
            if liquidation_distance_pct < min_required:
                raise PositionLiquidationRiskError(
                    f"liquidation distance below configured minimum (STRICT): "
                    f"got={liquidation_distance_pct:.6f} required={min_required:.6f}"
                )

        snapshot = PositionSupervisorSnapshot(
            symbol=symbol,
            checked_at_ts=checked_at_ts,
            has_position=True,
            position_side=position_side,
            exchange_position_side=exchange_position_side,
            abs_qty=float(abs_qty_dec),
            entry_price=float(entry_price),
            liquidation_price=liquidation_price,
            liquidation_distance_pct=liquidation_distance_pct,
            expected_remaining_qty=float(expected_remaining_qty),
            expected_trade_side=position_side,
            expected_entry_price=float(expected_entry_price),
            reconciliation_status=reconciliation_status,
            protection=protection_snapshot,
        )

        logger.info(
            "PositionSupervisor OK "
            "(symbol=%s side=%s qty=%s entry_price=%s liq_dist_pct=%s tp=%s sl=%s soft_mode=%s)",
            snapshot.symbol,
            snapshot.position_side,
            snapshot.abs_qty,
            snapshot.entry_price,
            snapshot.liquidation_distance_pct,
            snapshot.protection.tp_order_id if snapshot.protection is not None else None,
            snapshot.protection.sl_order_id if snapshot.protection is not None else None,
            snapshot.protection.soft_mode if snapshot.protection is not None else None,
        )
        return snapshot


def build_position_supervisor_or_raise(*, settings: Any, symbol: str) -> PositionSupervisor:
    if settings is None:
        raise PositionSupervisorConfigError("settings is required (STRICT)")
    if not hasattr(settings, "hard_liquidation_distance_pct_min"):
        raise PositionSupervisorConfigError("settings.hard_liquidation_distance_pct_min missing (STRICT)")

    config = PositionSupervisorConfig(
        symbol=_normalize_symbol_strict(symbol, name="symbol"),
        hard_liquidation_distance_pct_min=_require_nonnegative_float_strict(
            getattr(settings, "hard_liquidation_distance_pct_min"),
            name="settings.hard_liquidation_distance_pct_min",
        ),
    )
    config.validate_or_raise()
    return PositionSupervisor(config)


def run_position_supervisor_once_or_raise(
    *,
    supervisor: PositionSupervisor,
    expected_trade: Optional[Trade],
) -> PositionSupervisorSnapshot:
    if not isinstance(supervisor, PositionSupervisor):
        raise PositionSupervisorContractError("supervisor must be PositionSupervisor (STRICT)")
    return supervisor.inspect_or_raise(expected_trade=expected_trade)


__all__ = [
    "PositionSupervisorError",
    "PositionSupervisorConfigError",
    "PositionSupervisorContractError",
    "PositionSupervisorStateError",
    "PositionProtectionMissingError",
    "PositionDesyncError",
    "PositionLiquidationRiskError",
    "ProtectionOrdersSnapshot",
    "PositionSupervisorSnapshot",
    "PositionSupervisorConfig",
    "PositionSupervisor",
    "build_position_supervisor_or_raise",
    "run_position_supervisor_once_or_raise",
]