# events/event_bus.py
"""
========================================================
FILE: events/event_bus.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
목적:
- 엔진 내부에서 발생하는 이벤트(진입/청산/리스크/슬리피지 등)를 단일 경로로 발행(publish)
- 구독자(subscriber)가 동기 방식으로 처리(로그/해설/GPT/유튜브 채팅 등)

핵심 원칙(STRICT):
- 폴백 금지: 필요한 데이터가 없거나 구독자 오류가 발생하면 즉시 예외 발생
- 데이터 없으면 Render 서버에서 에러가 보이게 -> 예외를 삼키지 않는다
- 민감정보는 이벤트 payload에 넣지 않는다
- 공통 STRICT 예외 계층을 사용한다
- subscriber registry는 thread-safe 하게 관리한다

변경 이력
--------------------------------------------------------
- 2026-03-13:
  1) FIX(EXCEPTION): common.exceptions_strict 공통 예외 계층 적용
  2) FEAT(CONTRACT): Event dataclass 자체 계약 검증 추가
  3) FIX(CONCURRENCY): subscriber registry에 RLock 적용
  4) FIX(STRICT): duplicate subscribe / invalid unsubscribe를 상태 오류로 승격
  5) FIX(OBSERVABILITY): subscriber 실패 시 handler 정보 포함 예외 전파
- 초기 구현:
  1) publish/subscribe 기반 EventBus 제공
========================================================
"""

from __future__ import annotations

import math
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from common.exceptions_strict import StrictDataError, StrictStateError


EventHandler = Callable[["Event"], None]


class EventBusError(RuntimeError):
    """EventBus base error."""


class EventBusContractError(StrictDataError):
    """event payload / event object contract violation."""


class EventBusStateError(StrictStateError):
    """event bus subscriber registry / publish state violation."""


def _require_nonempty_str(v: Any, name: str) -> str:
    if not isinstance(v, str):
        raise EventBusContractError(f"{name} must be str (STRICT), got={type(v).__name__}")
    s = v.strip()
    if not s:
        raise EventBusContractError(f"{name} is required (STRICT)")
    return s


def _require_positive_int(v: Any, name: str) -> int:
    if isinstance(v, bool):
        raise EventBusContractError(f"{name} must be int (STRICT), bool not allowed")
    try:
        iv = int(v)
    except Exception as exc:
        raise EventBusContractError(f"{name} must be int (STRICT): {exc}") from exc
    if iv <= 0:
        raise EventBusContractError(f"{name} must be > 0 (STRICT)")
    return iv


def _require_finite_optional_float(v: Any, name: str) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, bool):
        raise EventBusContractError(f"{name} must be numeric (STRICT), bool not allowed")
    try:
        fv = float(v)
    except Exception as exc:
        raise EventBusContractError(f"{name} must be numeric (STRICT): {exc}") from exc
    if not math.isfinite(fv):
        raise EventBusContractError(f"{name} must be finite (STRICT)")
    return float(fv)


def _require_payload_dict(v: Any, name: str) -> Dict[str, Any]:
    if not isinstance(v, dict):
        raise EventBusContractError(f"{name} must be dict (STRICT), got={type(v).__name__}")
    return dict(v)


def _normalize_side(v: Any, name: str) -> str:
    s = _require_nonempty_str(v, name).upper()
    if s not in {"LONG", "SHORT", "CLOSE"}:
        raise EventBusContractError(f"{name} must be LONG/SHORT/CLOSE (STRICT)")
    return s


def _normalize_handler_name(handler: EventHandler) -> str:
    name = getattr(handler, "__name__", None)
    if isinstance(name, str) and name.strip():
        return name.strip()
    return repr(handler)


@dataclass(frozen=True)
class Event:
    """Immutable event record."""

    event_id: str
    event_type: str
    ts_ms: int
    payload: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_id", _require_nonempty_str(self.event_id, "event.event_id"))
        object.__setattr__(self, "event_type", _require_nonempty_str(self.event_type, "event.event_type"))
        object.__setattr__(self, "ts_ms", _require_positive_int(self.ts_ms, "event.ts_ms"))
        object.__setattr__(self, "payload", _require_payload_dict(self.payload, "event.payload"))


# event_type -> handlers
_SUBSCRIBERS: Dict[str, List[EventHandler]] = {}
_SUBSCRIBERS_LOCK = threading.RLock()


def _now_ms() -> int:
    return int(time.time() * 1000)


def subscribe(event_type: str, handler: EventHandler) -> None:
    """Register a handler for an event type."""
    et = _require_nonempty_str(event_type, "event_type")
    if not callable(handler):
        raise EventBusContractError("handler must be callable (STRICT)")

    with _SUBSCRIBERS_LOCK:
        handlers = _SUBSCRIBERS.setdefault(et, [])
        if handler in handlers:
            raise EventBusStateError(
                f"duplicate subscriber registration not allowed (STRICT): event_type={et} handler={_normalize_handler_name(handler)}"
            )
        handlers.append(handler)


def unsubscribe(event_type: str, handler: EventHandler) -> None:
    """Unregister a handler for an event type."""
    et = _require_nonempty_str(event_type, "event_type")
    if not callable(handler):
        raise EventBusContractError("handler must be callable (STRICT)")

    with _SUBSCRIBERS_LOCK:
        if et not in _SUBSCRIBERS:
            raise EventBusStateError(f"event_type not registered (STRICT): {et}")

        handlers = _SUBSCRIBERS[et]
        if handler not in handlers:
            raise EventBusStateError(
                f"handler not subscribed (STRICT): event_type={et} handler={_normalize_handler_name(handler)}"
            )

        handlers.remove(handler)
        if not handlers:
            del _SUBSCRIBERS[et]


def clear_subscribers(event_type: Optional[str] = None) -> None:
    """Clear subscribers. If event_type is None, clear all."""
    with _SUBSCRIBERS_LOCK:
        if event_type is None:
            _SUBSCRIBERS.clear()
            return

        et = _require_nonempty_str(event_type, "event_type")
        if et not in _SUBSCRIBERS:
            raise EventBusStateError(f"event_type not registered (STRICT): {et}")
        del _SUBSCRIBERS[et]


def publish_event(event_type: str, **payload: Any) -> Event:
    """Publish an event and synchronously execute subscribers.

    STRICT:
    - event_type must be provided
    - payload must include minimal required fields for known event types
    - if any subscriber raises, publish_event raises
    """
    et = _require_nonempty_str(event_type, "event_type")

    ev = Event(
        event_id=str(uuid.uuid4()),
        event_type=et,
        ts_ms=_now_ms(),
        payload=dict(payload),
    )

    validate_event(ev)

    with _SUBSCRIBERS_LOCK:
        handlers = list(_SUBSCRIBERS.get(et, []))

    for h in handlers:
        try:
            h(ev)
        except Exception as exc:
            raise EventBusStateError(
                f"subscriber failed (STRICT): event_type={et} handler={_normalize_handler_name(h)}"
            ) from exc

    return ev


def validate_event(ev: Event) -> None:
    """Strict validation for event payload."""
    if not isinstance(ev, Event):
        raise EventBusContractError("ev must be Event (STRICT)")

    _require_nonempty_str(ev.event_id, "event.event_id")
    _require_nonempty_str(ev.event_type, "event.event_type")
    _require_positive_int(ev.ts_ms, "event.ts_ms")
    payload = _require_payload_dict(ev.payload, "event.payload")

    needs_symbol = ev.event_type in {
        "on_signal_candidate",
        "on_gpt_approve",
        "on_gpt_reject",
        "on_entry_submitted",
        "on_entry_filled",
        "on_exit_submitted",
        "on_exit_filled",
        "on_slippage_block",
        "on_risk_guard_trigger",
        "on_tp_sl_reset_failed",
        "on_tp_sl_reset_recovered",
        "on_exchange_sync_error",
        "on_exchange_sync_recovered",
        "on_hold_update",
    }

    if needs_symbol:
        _require_nonempty_str(payload.get("symbol"), f"{ev.event_type}.payload.symbol")

    needs_side = ev.event_type in {
        "on_entry_submitted",
        "on_entry_filled",
        "on_exit_submitted",
        "on_exit_filled",
    }
    if needs_side:
        _normalize_side(payload.get("side"), f"{ev.event_type}.payload.side")

    if ev.event_type in {"on_entry_filled", "on_exit_filled"}:
        price = _require_finite_optional_float(payload.get("price"), f"{ev.event_type}.payload.price")
        if price is None:
            raise EventBusContractError(f"{ev.event_type}: payload.price missing (STRICT)")
        if price <= 0.0:
            raise EventBusContractError(f"{ev.event_type}: payload.price must be > 0 (STRICT)")

    if ev.event_type in {
        "on_entry_filled",
        "on_exit_filled",
        "on_slippage_block",
        "on_risk_guard_trigger",
        "on_hold_update",
    }:
        _require_nonempty_str(payload.get("reason"), f"{ev.event_type}.payload.reason")


__all__ = [
    "Event",
    "EventHandler",
    "EventBusError",
    "EventBusContractError",
    "EventBusStateError",
    "subscribe",
    "unsubscribe",
    "clear_subscribers",
    "publish_event",
    "validate_event",
]