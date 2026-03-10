"""
========================================================
FILE: execution/execution_engine.py
ROLE:
- 엔트리 실행 요청을 최종 정규화하고 주문 실행 SSOT(order_executor)로 위임한다
- 실행 직전 계약(symbol/action/source/qty/tp/sl/client_order_id)을 엄격 검증한다
- Trade 반환 계약을 검증해 상위 엔진에 안전한 실행 결과만 전달한다

CORE RESPONSIBILITIES:
- Signal / mapping / dataclass 입력 정규화
- deterministic client_order_id 강제 생성 및 검증
- entry execution request strict contract 생성
- order_executor.open_position_with_tp_sl() 호출
- Trade 반환 계약 검증

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- 실행 계층은 계약 누락을 임의 보정하지 않는다
- symbol 누락 시 settings.symbol 로 폴백하지 않는다
- action 은 반드시 ENTER 여야 하며 누락 시 즉시 예외
- source 는 출처 필드만 허용하며 regime/reason 을 대체값으로 사용하지 않는다
- tp_pct/sl_pct/risk_pct 는 ratio 계약(0,1] 을 강제한다
- 주문 실행 / 체결 검증 / 보호주문 검증의 실제 SSOT 는 order_executor 이다

CHANGE HISTORY:
- 2026-03-10:
  1) FIX(ROOT-CAUSE): signal.symbol 누락 시 settings.symbol 로 진행하던 숨은 fallback 제거
  2) FIX(ROOT-CAUSE): action 필수화 및 ENTER 외 실행 금지
  3) FIX(CONTRACT): source alias 에서 regime/reason 제거, 실제 출처 필드만 허용
  4) FIX(STRICT): tp_pct/sl_pct 를 ratio(0,1] 계약으로 강화
  5) CLEANUP: 상단 문서 구조를 ROLE / CORE RESPONSIBILITIES / IMPORTANT POLICY 형식으로 정리
- 2026-03-09:
  1) FIX(ROOT-CAUSE): ExecutionEngine 클래스 복구
  2) FIX(CONTRACT): run_bot_ws Signal 구조(action/direction/risk_pct/meta)와 정합 복구
  3) FIX(SIZING): qty 누락 시 risk_pct 기반 수량 계산 복구
========================================================
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
from dataclasses import asdict, dataclass, is_dataclass, replace
from datetime import date, datetime
from decimal import Decimal
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
    if s != "ENTER":
        raise OrderExecutionError(f"ExecutionEngine only accepts ENTER action (STRICT): got={s!r}")
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


def _extract_symbol_strict(
    *,
    search_spaces: tuple[tuple[str, Mapping[str, Any]], ...],
    settings: Any,
) -> str:
    symbol = _extract_normalized_value_strict(
        search_spaces=search_spaces,
        field_name="symbol",
        aliases=("symbol",),
        normalizer=_normalize_symbol_strict,
        required=True,
    )
    settings_symbol = getattr(settings, "symbol", None)
    if settings_symbol is None:
        raise OrderExecutionError("settings.symbol missing (STRICT)")
    normalized_settings_symbol = _normalize_symbol_strict(settings_symbol)
    if symbol != normalized_settings_symbol:
        raise OrderExecutionError(
            f"signal.symbol mismatch vs settings.symbol "
            f"(signal={symbol}, settings={normalized_settings_symbol}) (STRICT)"
        )
    return symbol


def _extract_action_required_or_raise(
    *,
    search_spaces: tuple[tuple[str, Mapping[str, Any]], ...],
) -> str:
    return _extract_normalized_value_strict(
        search_spaces=search_spaces,
        field_name="action",
        aliases=("action",),
        normalizer=_normalize_action_strict,
        required=True,
    )


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


def _canonicalize_for_hash_strict(
    value: Any,
    *,
    path: str,
    seen: set[int],
) -> Any:
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        if not math.isfinite(value):
            raise OrderExecutionError(f"{path} contains non-finite float (STRICT)")
        return format(value, ".15g")

    if isinstance(value, Decimal):
        return str(value)

    if isinstance(value, str):
        return value

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, Mapping):
        obj_id = id(value)
        if obj_id in seen:
            raise OrderExecutionError(f"{path} contains cyclic mapping reference (STRICT)")
        seen.add(obj_id)
        try:
            out: dict[str, Any] = {}
            for k in sorted(value.keys(), key=lambda item: str(item)):
                out[str(k)] = _canonicalize_for_hash_strict(
                    value[k],
                    path=f"{path}.{k}",
                    seen=seen,
                )
            return out
        finally:
            seen.remove(obj_id)

    if isinstance(value, (list, tuple)):
        obj_id = id(value)
        if obj_id in seen:
            raise OrderExecutionError(f"{path} contains cyclic sequence reference (STRICT)")
        seen.add(obj_id)
        try:
            return [
                _canonicalize_for_hash_strict(v, path=f"{path}[{idx}]", seen=seen)
                for idx, v in enumerate(value)
            ]
        finally:
            seen.remove(obj_id)

    if isinstance(value, set):
        obj_id = id(value)
        if obj_id in seen:
            raise OrderExecutionError(f"{path} contains cyclic set reference (STRICT)")
        seen.add(obj_id)
        try:
            normalized_items = [
                _canonicalize_for_hash_strict(v, path=f"{path}[set_item]", seen=seen)
                for v in value
            ]
            return sorted(
                normalized_items,
                key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":"), ensure_ascii=True),
            )
        finally:
            seen.remove(obj_id)

    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        dumped = model_dump()
        return _canonicalize_for_hash_strict(dumped, path=path, seen=seen)

    if is_dataclass(value):
        dumped = asdict(value)
        return _canonicalize_for_hash_strict(dumped, path=path, seen=seen)

    if hasattr(value, "__dict__"):
        public_dict = {
            str(k): v
            for k, v in vars(value).items()
            if not str(k).startswith("_")
        }
        if not public_dict:
            raise OrderExecutionError(f"{path} object has no public fields for deterministic hash (STRICT)")
        return _canonicalize_for_hash_strict(public_dict, path=path, seen=seen)

    raise OrderExecutionError(
        f"{path} contains unsupported type for deterministic client_order_id: "
        f"{type(value).__name__} (STRICT)"
    )


def _build_deterministic_client_order_id_strict(
    *,
    signal: Any,
    req: EntryExecutionRequest,
) -> str:
    signal_map = _signal_to_mapping_strict(signal)
    signal_payload = _canonicalize_for_hash_strict(signal_map, path="signal", seen=set())

    request_payload = {
        "symbol": req.symbol,
        "side_open": req.side_open,
        "qty": format(req.qty, ".15g"),
        "entry_price_hint": format(req.entry_price_hint, ".15g"),
        "tp_pct": format(req.tp_pct, ".15g"),
        "sl_pct": format(req.sl_pct, ".15g"),
        "source": req.source,
        "soft_mode": req.soft_mode,
        "sl_floor_ratio": None if req.sl_floor_ratio is None else format(req.sl_floor_ratio, ".15g"),
        "available_usdt": None if req.available_usdt is None else format(req.available_usdt, ".15g"),
        "risk_pct": None if req.risk_pct is None else format(req.risk_pct, ".15g"),
    }

    payload = {
        "signal": signal_payload,
        "request": request_payload,
    }

    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest().upper()
    client_order_id = f"AT{digest[:34]}"

    return _normalize_optional_client_order_id_strict(client_order_id)  # type: ignore[return-value]


def _settings_require_deterministic_client_order_id_strict(settings: Any) -> bool:
    raw = getattr(settings, "require_deterministic_client_order_id", None)
    normalized = _normalize_optional_bool_strict(
        raw,
        field_name="settings.require_deterministic_client_order_id",
    )
    return bool(normalized) if normalized is not None else False


def _enforce_or_generate_client_order_id_strict(
    *,
    signal: Any,
    req: EntryExecutionRequest,
    settings: Any,
) -> EntryExecutionRequest:
    if req.entry_client_order_id is not None:
        return req

    if not _settings_require_deterministic_client_order_id_strict(settings):
        return req

    generated = _build_deterministic_client_order_id_strict(signal=signal, req=req)

    logger.info(
        "ExecutionEngine generated deterministic entry_client_order_id "
        "(symbol=%s side=%s cid=%s)",
        req.symbol,
        req.side_open,
        generated,
    )

    return replace(req, entry_client_order_id=generated)


def _normalize_entry_request_strict(signal: Any, settings: Any) -> EntryExecutionRequest:
    signal_map = _signal_to_mapping_strict(signal)
    meta_map = _meta_to_mapping_strict(signal_map)
    search_spaces = (("signal", signal_map), ("signal.meta", meta_map))

    _extract_action_required_or_raise(search_spaces=search_spaces)

    symbol = _extract_symbol_strict(
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
        normalizer=lambda v: _normalize_ratio_float_strict(v, field_name="tp_pct"),
        required=True,
    )

    sl_pct = _extract_normalized_value_strict(
        search_spaces=search_spaces,
        field_name="sl_pct",
        aliases=("sl_pct", "stop_loss_pct", "stop_pct"),
        normalizer=lambda v: _normalize_ratio_float_strict(v, field_name="sl_pct"),
        required=True,
    )

    source = _extract_normalized_value_strict(
        search_spaces=search_spaces,
        field_name="source",
        aliases=("source", "signal_source", "strategy_source", "entry_source", "strategy_type"),
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
        req = _enforce_or_generate_client_order_id_strict(
            signal=signal,
            req=req,
            settings=self._settings,
        )

        logger.info(
            "ExecutionEngine.execute start "
            "(symbol=%s side=%s qty=%s entry_price_hint=%s tp_pct=%s sl_pct=%s risk_pct=%s "
            "source=%s soft_mode=%s entry_client_order_id=%s)",
            req.symbol,
            req.side_open,
            req.qty,
            req.entry_price_hint,
            req.tp_pct,
            req.sl_pct,
            req.risk_pct,
            req.source,
            req.soft_mode,
            req.entry_client_order_id,
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
            "(symbol=%s side=%s qty=%s entry=%s entry_order_id=%s client_entry_id=%s)",
            getattr(trade, "symbol", None),
            getattr(trade, "side", None),
            getattr(trade, "qty", None),
            getattr(trade, "entry_price", getattr(trade, "entry", None)),
            getattr(trade, "entry_order_id", None),
            getattr(trade, "client_entry_id", None),
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