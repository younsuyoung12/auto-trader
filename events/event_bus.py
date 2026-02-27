# events/event_bus.py
# =============================================================================
# AUTO-TRADER - Event Bus (STRICT / NO-FALLBACK)
# -----------------------------------------------------------------------------
# 목적:
# - 엔진 내부에서 발생하는 이벤트(진입/청산/리스크/슬리피지 등)를 단일 경로로 발행(publish)
# - 구독자(subscriber)가 동기/비동기 방식으로 처리(로그/해설/GPT/유튜브 채팅 등)
#
# 핵심 원칙(STRICT):
# - 폴백 금지: 필요한 데이터가 없거나 구독자 오류가 발생하면 즉시 예외 발생
# - "데이터 없으면 Render 서버에서 에러가 보이게" -> 예외를 삼키지 않는다.
# - 민감정보는 이벤트에 넣지 않는다(키/시크릿 등)
#
# 사용 예:
#   from events.event_bus import publish_event, subscribe
#   subscribe("on_entry_filled", handler_fn)
#   publish_event("on_entry_filled", symbol="BTCUSDT", side="LONG", price=..., ...)
# =============================================================================

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


EventHandler = Callable[["Event"], None]


@dataclass(frozen=True)
class Event:
    """Immutable event record."""

    event_id: str
    event_type: str
    ts_ms: int
    payload: Dict[str, Any] = field(default_factory=dict)


# event_type -> handlers
_SUBSCRIBERS: Dict[str, List[EventHandler]] = {}


class EventBusError(RuntimeError):
    """Raised when event bus usage violates strict requirements."""


def _now_ms() -> int:
    return int(time.time() * 1000)


def subscribe(event_type: str, handler: EventHandler) -> None:
    """Register a handler for an event type."""
    et = str(event_type or "").strip()
    if not et:
        raise ValueError("event_type is empty")
    if not callable(handler):
        raise ValueError("handler must be callable")

    _SUBSCRIBERS.setdefault(et, []).append(handler)


def unsubscribe(event_type: str, handler: EventHandler) -> None:
    """Unregister a handler for an event type."""
    et = str(event_type or "").strip()
    if not et:
        raise ValueError("event_type is empty")
    handlers = _SUBSCRIBERS.get(et) or []
    if handler in handlers:
        handlers.remove(handler)


def clear_subscribers(event_type: Optional[str] = None) -> None:
    """Clear subscribers. If event_type is None, clear all."""
    if event_type is None:
        _SUBSCRIBERS.clear()
        return
    et = str(event_type or "").strip()
    if not et:
        raise ValueError("event_type is empty")
    _SUBSCRIBERS.pop(et, None)


def publish_event(event_type: str, **payload: Any) -> Event:
    """Publish an event and synchronously execute subscribers.

    STRICT:
    - event_type must be provided
    - payload must include minimal required fields for known event types (validated in validate_event)
    - if any subscriber raises, publish_event raises (no swallow)
    """
    et = str(event_type or "").strip()
    if not et:
        raise ValueError("event_type is empty")

    ev = Event(
        event_id=str(uuid.uuid4()),
        event_type=et,
        ts_ms=_now_ms(),
        payload=dict(payload or {}),
    )

    validate_event(ev)  # strict validation (raises)

    handlers = _SUBSCRIBERS.get(et, [])
    # STRICT: no fallback. If there are no handlers, that's allowed (engine may only log/store)
    for h in list(handlers):
        # Do not swallow exceptions. Let them crash and surface on Render logs.
        h(ev)

    return ev


def validate_event(ev: Event) -> None:
    """Strict validation for event payload.

    - If essential fields are missing, raise EventBusError.
    - Keep it minimal but enforce correctness for broadcasting/analysis.
    """
    if not isinstance(ev, Event):
        raise ValueError("ev must be an Event")

    if not ev.event_id or not ev.event_type:
        raise EventBusError("event_id/event_type missing")

    if not isinstance(ev.payload, dict):
        raise EventBusError("payload must be dict")

    # Common essentials (recommended across events)
    # symbol often needed for commentary & display
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
        sym = ev.payload.get("symbol")
        if not sym or not isinstance(sym, str):
            raise EventBusError(f"{ev.event_type}: payload.symbol missing")

    # Trade side required for entry/exit related events
    needs_side = ev.event_type in {"on_entry_submitted", "on_entry_filled", "on_exit_submitted", "on_exit_filled"}
    if needs_side:
        side = ev.payload.get("side")
        if side not in {"LONG", "SHORT", "CLOSE"}:
            raise EventBusError(f"{ev.event_type}: payload.side must be LONG/SHORT/CLOSE")

    # Price required for fills
    if ev.event_type in {"on_entry_filled", "on_exit_filled"}:
        price = ev.payload.get("price")
        if price is None:
            raise EventBusError(f"{ev.event_type}: payload.price missing")
        try:
            float(price)
        except Exception:
            raise EventBusError(f"{ev.event_type}: payload.price not numeric")

    # Reason text is strongly recommended for commentary
    if ev.event_type in {"on_entry_filled", "on_exit_filled", "on_slippage_block", "on_risk_guard_trigger", "on_hold_update"}:
        reason = ev.payload.get("reason")
        if not reason or not isinstance(reason, str):
            raise EventBusError(f"{ev.event_type}: payload.reason missing (required for commentary)")


__all__ = [
    "Event",
    "EventHandler",
    "EventBusError",
    "subscribe",
    "unsubscribe",
    "clear_subscribers",
    "publish_event",
    "validate_event",
]