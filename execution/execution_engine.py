"""
========================================================
FILE: execution/execution_engine.py
ROLE:
- 엔트리 실행 요청을 최종 정규화하고 주문 실행 SSOT(order_executor)로 위임한다
- 실행 직전 계약(symbol/action/source/qty/tp/sl/client_order_id)을 엄격 검증한다
- Trade 반환 계약을 검증해 상위 엔진에 안전한 실행 결과만 전달한다
- 명시적으로 식별 가능한 거래소 비치명 주문 거절은 "트레이드 스킵"으로 정규화한다
- 실행 성공 후 State Layer(DB insert)와 Execution Quality snapshot 생성을 수행한다

CORE RESPONSIBILITIES:
- Signal / mapping / dataclass 입력 정규화
- deterministic client_order_id 강제 생성 및 검증
- entry execution request strict contract 생성
- order_executor.open_position_with_tp_sl() 호출
- Trade 반환 계약 검증
- bt_trades INSERT 및 trade.db_id 연결
- execution quality snapshot 생성 및 trade 객체에 부착
- 명시적 비치명 주문 거절(-2019) 감지 및 None 반환
- 공통 STRICT 검증 프리미티브(common.strict_validators)와 정합 유지

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- 실행 계층은 계약 누락을 임의 보정하지 않는다
- symbol 누락 시 settings.symbol 로 폴백하지 않는다
- action 은 반드시 ENTER 여야 하며 누락 시 즉시 예외
- source 는 출처 필드만 허용하며 regime/reason 을 대체값으로 사용하지 않는다
- regime 는 source 와 분리된 독립 계약으로 강제한다
- tp_pct/sl_pct/risk_pct 는 ratio 계약(0,1] 을 강제한다
- 주문 실행 / 체결 검증 / 보호주문 검증의 실제 SSOT 는 order_executor 이다
- 단, 거래소가 명시적으로 "주문 자체를 거절"한 비치명 사유는 시스템 치명 오류로 승격하지 않는다
- 현재 비치명 주문 거절로 허용하는 케이스는 Binance code=-2019 (Margin is insufficient) 뿐이다
- state_writer / execution_quality_engine 연결 실패는 즉시 예외 처리한다

CHANGE HISTORY:
- 2026-03-14:
  1) FIX(ARCH): common.strict_validators 공통 STRICT 검증 프리미티브를 Execution Layer에 연결
  2) FIX(CONSISTENCY): symbol/float/bool/mapping 검증을 프로젝트 공통 계약과 정합화
  3) FIX(SAFETY): StrictValidationError 를 OrderExecutionError 로 승격해 기존 호출자 계약 유지
  4) FIX(STRICT): qty/tp/sl/risk/client_order_id 정규화 정책은 유지하되 primitive validation 중복 제거
  5) FIX(ROOT-CAUSE): risk_pct 누락 시 tp_pct 대입 fallback 제거, 즉시 예외 처리
  6) FIX(ROOT-CAUSE): regime 누락 시 source 대입 fallback 제거, regime 필수 계약으로 승격
  7) FIX(ROOT-CAUSE): require_deterministic_client_order_id 누락/None fallback 제거, settings 필수 bool 계약으로 승격
  8) FIX(STRICT): Trade 필수 필드 접근에서 getattr(..., default) 제거 및 명시적 계약 검증 추가
  9) FIX(STATE): state_writer 이후 trade.db_id 필수 반영 검증 추가
- 2026-03-13:
  1) FEAT(STATE): execution.state_writer.insert_trade_row() 정식 연결
  2) FEAT(QUALITY): execution.execution_quality_engine execution snapshot 생성/부착 추가
  3) FEAT(FSM): execution.order_state.OrderState 기반 lifecycle 상태 기록 추가
  4) FIX(STRICT): trade.meta dict strict 정규화 및 execution snapshot 저장 추가
  5) FIX(STRICT): state persistence / quality snapshot 실패 시 즉시 예외 처리
- 2026-03-11:
  1) FIX(STRICT): qty sizing 시 settings.max_leverage 기본값 fallback 제거
  2) FIX(STRICT): qty sizing 시 settings.execution_price_slippage_guard 기본값 fallback 제거
  3) FEAT(RISK): settings.max_risk_pct 상한 검증 추가
  4) FIX(CONTRACT): qty 와 risk_pct 동시 입력 시 즉시 예외 처리
  5) FIX(CONTRACT): entry_price_hint alias 우선순위를 mark_price 중심으로 정렬
- 2026-03-10:
  1) FIX(ROOT-CAUSE): signal.symbol 누락 시 settings.symbol 로 진행하던 숨은 fallback 제거
  2) FIX(ROOT-CAUSE): action 필수화 및 ENTER 외 실행 금지
  3) FIX(CONTRACT): source alias 에서 regime/reason 제거, 실제 출처 필드만 허용
  4) FIX(STRICT): tp_pct/sl_pct 를 ratio(0,1] 계약으로 강화
  5) CLEANUP: 상단 문서 구조를 ROLE / CORE RESPONSIBILITIES / IMPORTANT POLICY 형식으로 정리
  6) FIX(ROOT-CAUSE): Binance code=-2019 (Margin is insufficient) 를 비치명 엔트리 거절로 정규화
  7) FIX(CONTRACT): execute() 반환 계약을 Optional[Trade] 로 확장하여 "스킵된 트레이드"를 명시 표현
  8) FIX(LOG): 비치명 주문 거절 시 예외 전파 대신 구조화 경고 로그를 남기고 None 반환
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
import re
from dataclasses import asdict, dataclass, is_dataclass, replace
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Mapping, Optional

from common.strict_validators import (
    StrictValidationError,
    normalize_symbol as _sv_normalize_symbol,
    require_bool as _sv_require_bool,
    require_float as _sv_require_float,
    require_mapping as _sv_require_mapping,
    require_nonempty_str as _sv_require_nonempty_str,
)
from execution.execution_quality_engine import (
    ExecutionQualityError,
    build_execution_quality_snapshot,
)
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
from execution.order_state import OrderState
from execution.state_writer import StateWriterError, insert_trade_row
from settings import load_settings
from state.trader_state import Trade

logger = logging.getLogger(__name__)

_NONFATAL_BINANCE_ENTRY_REJECTION_CODES: frozenset[int] = frozenset({-2019})


@dataclass(frozen=True, slots=True)
class EntryExecutionRequest:
    symbol: str
    side_open: str
    qty: float
    entry_price_hint: float
    tp_pct: float
    sl_pct: float
    source: str
    regime: str
    soft_mode: bool
    sl_floor_ratio: Optional[float]
    available_usdt: Optional[float]
    entry_client_order_id: Optional[str]
    risk_pct: float


def _raise_exec_from_strict(where: str, exc: StrictValidationError) -> OrderExecutionError:
    return OrderExecutionError(f"{where}: {exc}")


def _require_attr_exists_strict(obj: Any, attr_name: str, owner_name: str) -> Any:
    if obj is None:
        raise OrderExecutionError(f"{owner_name} is None (STRICT)")
    if not hasattr(obj, attr_name):
        raise OrderExecutionError(f"{owner_name}.{attr_name} missing (STRICT)")
    return getattr(obj, attr_name)


def _require_attr_value_strict(obj: Any, attr_name: str, owner_name: str) -> Any:
    value = _require_attr_exists_strict(obj, attr_name, owner_name)
    if value is None:
        raise OrderExecutionError(f"{owner_name}.{attr_name} missing (STRICT)")
    return value


def _signal_to_mapping_strict(signal: Any) -> dict[str, Any]:
    if signal is None:
        raise OrderExecutionError("signal is None (STRICT)")

    if isinstance(signal, Mapping):
        try:
            mapping_obj = _sv_require_mapping(signal, "signal", non_empty=True)
        except StrictValidationError as exc:
            raise _raise_exec_from_strict("signal", exc) from exc
        return dict(mapping_obj)

    if hasattr(signal, "model_dump"):
        model_dump = getattr(signal, "model_dump")
        if callable(model_dump):
            data = model_dump()
            try:
                mapping_obj = _sv_require_mapping(data, "signal.model_dump()", non_empty=True)
            except StrictValidationError as exc:
                raise _raise_exec_from_strict("signal.model_dump()", exc) from exc
            return dict(mapping_obj)

    if is_dataclass(signal):
        data = asdict(signal)
        try:
            mapping_obj = _sv_require_mapping(data, "signal.dataclass", non_empty=True)
        except StrictValidationError as exc:
            raise _raise_exec_from_strict("signal.dataclass", exc) from exc
        return dict(mapping_obj)

    if hasattr(signal, "__dict__"):
        data = {
            str(k): v
            for k, v in vars(signal).items()
            if not str(k).startswith("_")
        }
        try:
            mapping_obj = _sv_require_mapping(data, "signal.__dict__", non_empty=True)
        except StrictValidationError as exc:
            raise _raise_exec_from_strict("signal.__dict__", exc) from exc
        return dict(mapping_obj)

    raise OrderExecutionError(f"unsupported signal type for execution: {type(signal).__name__}")


def _meta_to_mapping_strict(signal_map: Mapping[str, Any]) -> dict[str, Any]:
    if "meta" not in signal_map:
        return {}
    meta = signal_map["meta"]
    if meta is None:
        return {}
    try:
        mapping_obj = _sv_require_mapping(meta, "signal.meta", non_empty=False)
    except StrictValidationError as exc:
        raise _raise_exec_from_strict("signal.meta", exc) from exc
    return dict(mapping_obj)


def _normalize_symbol_strict(value: Any) -> str:
    try:
        return _sv_normalize_symbol(value, name="symbol")
    except StrictValidationError as exc:
        raise _raise_exec_from_strict("symbol", exc) from exc


def _normalize_open_side_strict(value: Any) -> str:
    s = str(value).upper().strip()
    if s in {"BUY", "LONG", "OPEN_LONG", "ENTER_LONG", "GO_LONG"}:
        return "BUY"
    if s in {"SELL", "SHORT", "OPEN_SHORT", "ENTER_SHORT", "GO_SHORT"}:
        return "SELL"
    raise OrderExecutionError(f"invalid open side for execution: {value!r}")


def _normalize_action_strict(value: Any) -> str:
    try:
        s = _sv_require_nonempty_str(value, "action").upper().strip()
    except StrictValidationError as exc:
        raise _raise_exec_from_strict("action", exc) from exc
    if s != "ENTER":
        raise OrderExecutionError(f"ExecutionEngine only accepts ENTER action (STRICT): got={s!r}")
    return s


def _normalize_positive_float_strict(value: Any, *, field_name: str) -> float:
    try:
        f = _sv_require_float(value, field_name, min_value=0.0)
    except StrictValidationError as exc:
        raise _raise_exec_from_strict(field_name, exc) from exc
    if f <= 0.0:
        raise OrderExecutionError(f"{field_name} must be > 0 (STRICT)")
    return f


def _normalize_nonnegative_float_strict(value: Any, *, field_name: str) -> float:
    try:
        return _sv_require_float(value, field_name, min_value=0.0)
    except StrictValidationError as exc:
        raise _raise_exec_from_strict(field_name, exc) from exc


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
    try:
        return _sv_require_nonempty_str(value, "source")
    except StrictValidationError as exc:
        raise _raise_exec_from_strict("source", exc) from exc


def _normalize_regime_strict(value: Any) -> str:
    try:
        return _sv_require_nonempty_str(value, "regime")
    except StrictValidationError as exc:
        raise _raise_exec_from_strict("regime", exc) from exc


def _normalize_bool_strict(value: Any, *, field_name: str) -> bool:
    try:
        return _sv_require_bool(value, field_name)
    except StrictValidationError as exc:
        raise _raise_exec_from_strict(field_name, exc) from exc


def _normalize_optional_bool_strict(value: Any, *, field_name: str) -> Optional[bool]:
    if value is None:
        return None
    return _normalize_bool_strict(value, field_name=field_name)


def _normalize_optional_client_order_id_strict(value: Any) -> Optional[str]:
    if value is None:
        return None

    try:
        cid = _sv_require_nonempty_str(value, "entry_client_order_id")
    except StrictValidationError as exc:
        raise _raise_exec_from_strict("entry_client_order_id", exc) from exc

    if len(cid) > 36:
        raise OrderExecutionError("entry_client_order_id exceeds 36 chars (STRICT)")
    try:
        cid.encode("ascii")
    except UnicodeEncodeError as exc:
        raise OrderExecutionError("entry_client_order_id must be ASCII (STRICT)") from exc
    return cid


def _require_settings_value_strict(settings: Any, field_name: str) -> Any:
    if settings is None:
        raise OrderExecutionError("settings is required (STRICT)")
    if not hasattr(settings, field_name):
        raise OrderExecutionError(f"settings.{field_name} missing (STRICT)")
    value = getattr(settings, field_name)
    if value is None:
        raise OrderExecutionError(f"settings.{field_name} missing (STRICT)")
    return value


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
    settings_symbol = _require_settings_value_strict(settings, "symbol")
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


def _extract_required_regime_strict(
    *,
    search_spaces: tuple[tuple[str, Mapping[str, Any]], ...],
) -> str:
    return _extract_normalized_value_strict(
        search_spaces=search_spaces,
        field_name="regime",
        aliases=("regime", "market_regime", "regime_at_entry"),
        normalizer=_normalize_regime_strict,
        required=True,
    )


def _compute_qty_from_risk_strict(
    *,
    capital_usdt: float,
    risk_pct: float,
    entry_price_hint: float,
    settings: Any,
) -> float:
    normalized_capital_usdt = _normalize_positive_float_strict(
        capital_usdt,
        field_name="capital_usdt",
    )
    normalized_risk_pct = _normalize_ratio_float_strict(
        risk_pct,
        field_name="risk_pct",
    )
    normalized_entry_price_hint = _normalize_positive_float_strict(
        entry_price_hint,
        field_name="entry_price_hint",
    )

    leverage = _normalize_positive_float_strict(
        _require_settings_value_strict(settings, "leverage"),
        field_name="settings.leverage",
    )
    max_leverage = _normalize_positive_float_strict(
        _require_settings_value_strict(settings, "max_leverage"),
        field_name="settings.max_leverage",
    )
    if leverage > max_leverage:
        raise OrderExecutionError(
            f"leverage exceeds max_leverage (STRICT): {leverage} > {max_leverage}"
        )

    max_risk_pct = _normalize_ratio_float_strict(
        _require_settings_value_strict(settings, "max_risk_pct"),
        field_name="settings.max_risk_pct",
    )
    if normalized_risk_pct > max_risk_pct:
        raise OrderExecutionError(
            f"risk_pct exceeds max_risk_pct (STRICT): {normalized_risk_pct} > {max_risk_pct}"
        )

    slippage_guard = _normalize_nonnegative_float_strict(
        _require_settings_value_strict(settings, "execution_price_slippage_guard"),
        field_name="settings.execution_price_slippage_guard",
    )
    if slippage_guard > 0.1:
        raise OrderExecutionError("settings.execution_price_slippage_guard too large (STRICT)")

    notional_usdt = normalized_capital_usdt * normalized_risk_pct * leverage
    if not math.isfinite(notional_usdt) or notional_usdt <= 0.0:
        raise OrderExecutionError("computed notional_usdt invalid (STRICT)")

    effective_price = normalized_entry_price_hint * (1.0 + slippage_guard)
    if not math.isfinite(effective_price) or effective_price <= 0.0:
        raise OrderExecutionError("computed effective_price invalid (STRICT)")

    qty = notional_usdt / effective_price
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

    if hasattr(value, "model_dump"):
        model_dump = getattr(value, "model_dump")
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
        "regime": req.regime,
        "soft_mode": req.soft_mode,
        "sl_floor_ratio": None if req.sl_floor_ratio is None else format(req.sl_floor_ratio, ".15g"),
        "available_usdt": None if req.available_usdt is None else format(req.available_usdt, ".15g"),
        "risk_pct": format(req.risk_pct, ".15g"),
    }

    payload = {
        "signal": signal_payload,
        "request": request_payload,
    }

    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest().upper()
    client_order_id = f"AT{digest[:34]}"

    normalized = _normalize_optional_client_order_id_strict(client_order_id)
    if normalized is None:
        raise OrderExecutionError("deterministic client_order_id generation failed (STRICT)")
    return normalized


def _settings_require_deterministic_client_order_id_strict(settings: Any) -> bool:
    raw = _require_settings_value_strict(settings, "require_deterministic_client_order_id")
    return _normalize_bool_strict(
        raw,
        field_name="settings.require_deterministic_client_order_id",
    )


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


def _iter_exception_chain(exc: BaseException) -> list[BaseException]:
    out: list[BaseException] = []
    seen: set[int] = set()
    current: Optional[BaseException] = exc

    while current is not None:
        obj_id = id(current)
        if obj_id in seen:
            break
        seen.add(obj_id)
        out.append(current)

        if current.__cause__ is not None:
            current = current.__cause__
            continue

        if current.__context__ is not None and not current.__suppress_context__:
            current = current.__context__
            continue

        current = None

    return out


def _extract_binance_error_code_from_text(text: str) -> Optional[int]:
    match = re.search(r"code\s*=\s*(-?\d+)", text)
    if match is None:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def _is_nonfatal_entry_submission_rejection(exc: BaseException) -> bool:
    for part in _iter_exception_chain(exc):
        text = str(part)
        code = _extract_binance_error_code_from_text(text)
        if code in _NONFATAL_BINANCE_ENTRY_REJECTION_CODES:
            return True
        if "Margin is insufficient" in text:
            return True
    return False


def _format_exception_chain_for_log(exc: BaseException) -> str:
    parts: list[str] = []
    for part in _iter_exception_chain(exc):
        rendered = f"{type(part).__name__}: {part}"
        if rendered not in parts:
            parts.append(rendered)
    return " | ".join(parts)


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
        aliases=("entry_price_hint", "mark_price", "last_price", "price", "entry_price", "close"),
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

    regime = _extract_required_regime_strict(
        search_spaces=search_spaces,
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

    if explicit_qty is not None and risk_pct is not None:
        raise OrderExecutionError("qty and risk_pct cannot both be provided (STRICT)")

    if explicit_qty is not None:
        qty = explicit_qty
        if risk_pct is None:
            raise OrderExecutionError("explicit qty path still requires risk_pct contract (STRICT)")
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
        regime=regime,
        soft_mode=soft_mode,
        sl_floor_ratio=sl_floor_ratio,
        available_usdt=None if capital_usdt is None else float(capital_usdt),
        entry_client_order_id=entry_client_order_id,
        risk_pct=float(risk_pct),
    )


def _extract_trade_entry_price_strict(trade: Trade) -> float:
    if hasattr(trade, "entry_price"):
        raw = getattr(trade, "entry_price")
    elif hasattr(trade, "entry"):
        raw = getattr(trade, "entry")
    else:
        raise OrderExecutionError("Trade.entry_price/entry missing (STRICT)")

    if raw is None:
        raise OrderExecutionError("Trade.entry_price/entry missing (STRICT)")

    try:
        entry_price = float(raw)
    except Exception as exc:
        raise OrderExecutionError("Trade.entry_price/entry missing or invalid (STRICT)") from exc

    if not math.isfinite(entry_price) or entry_price <= 0.0:
        raise OrderExecutionError("Trade.entry_price/entry must be finite > 0 (STRICT)")
    return entry_price


def _validate_trade_contract_strict(trade: Trade, req: EntryExecutionRequest) -> None:
    if not isinstance(trade, Trade):
        raise OrderExecutionError(f"execute() returned non-Trade object: {type(trade).__name__} (STRICT)")

    trade_symbol = _normalize_symbol_strict(_require_attr_value_strict(trade, "symbol", "trade"))
    if trade_symbol != req.symbol:
        raise OrderExecutionError(
            f"Trade.symbol mismatch (got={trade_symbol}, expected={req.symbol}) (STRICT)"
        )

    trade_side = _normalize_open_side_strict(_require_attr_value_strict(trade, "side", "trade"))
    if trade_side != req.side_open:
        raise OrderExecutionError(
            f"Trade.side mismatch (got={trade_side}, expected={req.side_open}) (STRICT)"
        )

    try:
        trade_qty = float(_require_attr_value_strict(trade, "qty", "trade"))
    except Exception as exc:
        raise OrderExecutionError("Trade.qty missing/invalid (STRICT)") from exc
    if not math.isfinite(trade_qty) or trade_qty <= 0.0:
        raise OrderExecutionError("Trade.qty must be finite > 0 (STRICT)")

    _extract_trade_entry_price_strict(trade)

    entry_order_id = _require_attr_value_strict(trade, "entry_order_id", "trade")
    if not str(entry_order_id).strip():
        raise OrderExecutionError("Trade.entry_order_id missing (STRICT)")

    tp_order_id = _require_attr_value_strict(trade, "tp_order_id", "trade")
    if not str(tp_order_id).strip():
        raise OrderExecutionError("Trade.tp_order_id missing (STRICT)")

    if not req.soft_mode:
        sl_order_id = _require_attr_value_strict(trade, "sl_order_id", "trade")
        if not str(sl_order_id).strip():
            raise OrderExecutionError("Trade.sl_order_id missing while soft_mode=False (STRICT)")

    entry_ts = _require_attr_value_strict(trade, "entry_ts", "trade")
    if not isinstance(entry_ts, datetime):
        raise OrderExecutionError("Trade.entry_ts must be datetime (STRICT)")
    if entry_ts.tzinfo is None or entry_ts.utcoffset() is None:
        raise OrderExecutionError("Trade.entry_ts must be timezone-aware (STRICT)")

    reconciliation_status = str(_require_attr_value_strict(trade, "reconciliation_status", "trade")).upper().strip()
    if reconciliation_status != "PROTECTION_VERIFIED":
        raise OrderExecutionError(
            f"Trade.reconciliation_status mismatch "
            f"(got={reconciliation_status!r}, expected='PROTECTION_VERIFIED') (STRICT)"
        )

    if req.entry_client_order_id is not None:
        client_entry_id = _require_attr_value_strict(trade, "client_entry_id", "trade")
        if str(client_entry_id).strip() != req.entry_client_order_id:
            raise OrderExecutionError(
                f"Trade.client_entry_id mismatch "
                f"(got={client_entry_id!r}, expected={req.entry_client_order_id!r}) (STRICT)"
            )


def _extract_required_regime_from_signal_strict(signal: Any) -> str:
    signal_map = _signal_to_mapping_strict(signal)
    meta_map = _meta_to_mapping_strict(signal_map)
    search_spaces = (("signal", signal_map), ("signal.meta", meta_map))
    return _extract_required_regime_strict(search_spaces=search_spaces)


def _trade_entry_ts_ms_strict(trade: Trade) -> int:
    entry_ts = _require_attr_value_strict(trade, "entry_ts", "trade")
    if not isinstance(entry_ts, datetime):
        raise OrderExecutionError("Trade.entry_ts must be datetime (STRICT)")
    if entry_ts.tzinfo is None or entry_ts.utcoffset() is None:
        raise OrderExecutionError("Trade.entry_ts must be timezone-aware (STRICT)")
    ts_ms = int(entry_ts.astimezone(timezone.utc).timestamp() * 1000)
    if ts_ms <= 0:
        raise OrderExecutionError("Trade.entry_ts converted ts_ms invalid (STRICT)")
    return ts_ms


def _ensure_trade_meta_dict_strict(trade: Trade) -> dict[str, Any]:
    meta = _require_attr_value_strict(trade, "meta", "trade")
    if not isinstance(meta, dict):
        raise OrderExecutionError("Trade.meta must be dict when present (STRICT)")
    return meta


def _ensure_trade_db_id_strict(trade: Trade) -> int:
    raw = _require_attr_value_strict(trade, "db_id", "trade")
    if isinstance(raw, bool):
        raise OrderExecutionError("trade.db_id must be int (STRICT)")
    try:
        db_id = int(raw)
    except Exception as exc:
        raise OrderExecutionError("trade.db_id must be int (STRICT)") from exc
    if db_id <= 0:
        raise OrderExecutionError("trade.db_id must be > 0 (STRICT)")
    return db_id


def _set_trade_attr_strict(trade: Trade, attr_name: str, value: Any) -> None:
    try:
        setattr(trade, attr_name, value)
    except Exception as exc:
        raise OrderExecutionError(f"failed to set trade.{attr_name} (STRICT)") from exc


def _set_trade_order_state_strict(trade: Trade, state: OrderState) -> None:
    if not isinstance(state, OrderState):
        raise OrderExecutionError("state must be OrderState (STRICT)")
    _set_trade_attr_strict(trade, "order_state", state.value)


def _persist_trade_strict(
    *,
    trade: Trade,
    req: EntryExecutionRequest,
    settings: Any,
    signal: Any,
) -> None:
    leverage = _normalize_positive_float_strict(
        _require_settings_value_strict(settings, "leverage"),
        field_name="settings.leverage",
    )

    regime_at_entry = _extract_required_regime_from_signal_strict(signal)
    strategy = req.source
    ts_ms = _trade_entry_ts_ms_strict(trade)

    if req.risk_pct is None:
        raise OrderExecutionError("req.risk_pct missing for state persistence (STRICT)")

    try:
        insert_trade_row(
            trade=trade,
            symbol=req.symbol,
            ts_ms=ts_ms,
            regime_at_entry=regime_at_entry,
            strategy=strategy,
            risk_pct=req.risk_pct,
            tp_pct=req.tp_pct,
            sl_pct=req.sl_pct,
            leverage=leverage,
        )
    except StateWriterError as exc:
        raise OrderExecutionError(f"state_writer insert failed (STRICT): {exc}") from exc
    except Exception as exc:
        raise OrderExecutionError(f"state_writer unexpected failure (STRICT): {exc}") from exc

    _ensure_trade_db_id_strict(trade)


def _extract_post_prices_optional_strict(signal: Any) -> Optional[dict[str, float]]:
    signal_map = _signal_to_mapping_strict(signal)
    meta_map = _meta_to_mapping_strict(signal_map)
    search_spaces = (("signal", signal_map), ("signal.meta", meta_map))

    post_prices = _extract_normalized_value_strict(
        search_spaces=search_spaces,
        field_name="execution_post_prices",
        aliases=("execution_post_prices", "execution_quality_post_prices", "post_prices"),
        normalizer=lambda v: v,
        required=False,
    )
    if post_prices is None:
        return None

    if not isinstance(post_prices, Mapping):
        raise OrderExecutionError("execution_post_prices must be mapping (STRICT)")

    required_keys = ("t+1s", "t+3s", "t+5s")
    out: dict[str, float] = {}
    for key in required_keys:
        if key not in post_prices:
            raise OrderExecutionError(f"execution_post_prices missing key: {key} (STRICT)")
        out[key] = _normalize_positive_float_strict(post_prices[key], field_name=f"execution_post_prices[{key}]")

    return out


def _attach_execution_quality_snapshot_if_available_strict(
    *,
    trade: Trade,
    req: EntryExecutionRequest,
    signal: Any,
) -> None:
    post_prices = _extract_post_prices_optional_strict(signal)
    if post_prices is None:
        return

    filled_avg_price = _extract_trade_entry_price_strict(trade)

    try:
        snapshot = build_execution_quality_snapshot(
            symbol=req.symbol,
            side=req.side_open,
            expected_price=req.entry_price_hint,
            filled_avg_price=filled_avg_price,
            post_prices=post_prices,
        )
    except ExecutionQualityError as exc:
        raise OrderExecutionError(f"execution quality build failed (STRICT): {exc}") from exc
    except Exception as exc:
        raise OrderExecutionError(f"execution quality unexpected failure (STRICT): {exc}") from exc

    meta = _ensure_trade_meta_dict_strict(trade)
    if "execution_quality" in meta:
        raise OrderExecutionError("trade.meta.execution_quality already exists (STRICT)")
    meta["execution_quality"] = snapshot

    _set_trade_attr_strict(trade, "execution_quality_status", "ATTACHED")


class ExecutionEngine:
    """
    Entry execution orchestrator.

    역할
    - run_bot_ws 가 기대하는 클래스 기반 execute(signal) 계약을 제공한다.
    - Signal(action/direction/tp_pct/sl_pct/risk_pct/meta) 입력을 엄격하게 정규화한다.
    - qty 직접 입력 또는 risk_pct 기반 수량 계산 후 order_executor.open_position_with_tp_sl() 를 호출한다.
    - 주문 실행 / 체결 검증 / 보호주문 검증의 실제 SSOT 는 order_executor 이다.
    - 명시적으로 식별 가능한 거래소 비치명 주문 거절은 None 으로 반환한다.

    절대 원칙
    - signal 누락/충돌/모호성은 즉시 예외
    - 임의 기본값/추정/폴백 금지
    - Trade 반환 계약은 PROTECTION_VERIFIED 까지 검증
    - 단, 현재 허용된 명시적 비치명 주문 거절(code=-2019)은 시스템 치명 오류로 승격하지 않는다
    """

    def __init__(self, settings: Optional[Any] = None) -> None:
        self._settings = settings if settings is not None else load_settings()
        if self._settings is None:
            raise OrderExecutionError("settings resolution failed in ExecutionEngine (STRICT)")

    @property
    def settings(self) -> Any:
        return self._settings

    def execute(self, signal: Any) -> Optional[Trade]:
        req = _normalize_entry_request_strict(signal, self._settings)
        req = _enforce_or_generate_client_order_id_strict(
            signal=signal,
            req=req,
            settings=self._settings,
        )

        logger.info(
            "ExecutionEngine.execute start "
            "(symbol=%s side=%s qty=%s entry_price_hint=%s tp_pct=%s sl_pct=%s risk_pct=%s "
            "source=%s regime=%s soft_mode=%s entry_client_order_id=%s)",
            req.symbol,
            req.side_open,
            req.qty,
            req.entry_price_hint,
            req.tp_pct,
            req.sl_pct,
            req.risk_pct,
            req.source,
            req.regime,
            req.soft_mode,
            req.entry_client_order_id,
        )

        try:
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
        except OrderExecutionError as exc:
            if _is_nonfatal_entry_submission_rejection(exc):
                logger.warning(
                    "ExecutionEngine.execute skipped non-fatal entry rejection "
                    "(symbol=%s side=%s qty=%s source=%s reason=%s)",
                    req.symbol,
                    req.side_open,
                    req.qty,
                    req.source,
                    _format_exception_chain_for_log(exc),
                )
                return None
            raise

        if trade is None:
            raise OrderExecutionError("open_position_with_tp_sl returned None (STRICT)")

        _validate_trade_contract_strict(trade, req)
        _set_trade_order_state_strict(trade, OrderState.FILLED)
        _persist_trade_strict(
            trade=trade,
            req=req,
            settings=self._settings,
            signal=signal,
        )
        _attach_execution_quality_snapshot_if_available_strict(
            trade=trade,
            req=req,
            signal=signal,
        )

        entry_price_for_log = _extract_trade_entry_price_strict(trade)
        entry_order_id_for_log = _require_attr_value_strict(trade, "entry_order_id", "trade")
        client_entry_id_for_log = _require_attr_value_strict(trade, "client_entry_id", "trade")
        db_id_for_log = _ensure_trade_db_id_strict(trade)
        order_state_for_log = _require_attr_value_strict(trade, "order_state", "trade")
        trade_symbol_for_log = _require_attr_value_strict(trade, "symbol", "trade")
        trade_side_for_log = _require_attr_value_strict(trade, "side", "trade")
        trade_qty_for_log = _require_attr_value_strict(trade, "qty", "trade")

        logger.info(
            "ExecutionEngine.execute success "
            "(symbol=%s side=%s qty=%s entry=%s entry_order_id=%s client_entry_id=%s db_id=%s order_state=%s)",
            trade_symbol_for_log,
            trade_side_for_log,
            trade_qty_for_log,
            entry_price_for_log,
            entry_order_id_for_log,
            client_entry_id_for_log,
            db_id_for_log,
            order_state_for_log,
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