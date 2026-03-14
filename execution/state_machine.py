# ============================================================
# execution/state_machine.py
# STRICT · NO-FALLBACK · TRADE-GRADE MODE
# ============================================================
# 역할:
#   - Trade(포지션/거래) 생명주기 상태 전이를 단일 지점에서 통제한다.
#
# 핵심 규칙:
#   - 상태 변경은 반드시 이 모듈의 transition/apply_event 를 통해서만 수행한다.
#   - 허용되지 않는 전이는 즉시 예외(StateViolation)로 중단한다. (폴백 금지)
#   - Trade 객체에 lifecycle_state 필드가 없거나 타입이 틀리면 즉시 예외. (폴백 금지)
#   - 보호주문 검증 / desync / recovery 상태를 명시적으로 표현한다.
#
# CHANGELOG
# 2026-03-14
# - FIX(ARCH): 기관형 운영 상태(PROTECTION_PENDING / PROTECTION_VERIFIED / DESYNC / RECOVERY) 추가
# - FIX(CONTRACT): recovery / sync 복구 이벤트를 명시 enum 으로 추가
# - FIX(STATE): EXIT 요청 가능 상태를 ENTERED 뿐 아니라 PROTECTION_PENDING / PROTECTION_VERIFIED 로 확장
# - FIX(TIMESTAMP): last_state_change_at 를 None 으로 덮지 않고, changed_at 주입 시에만 갱신하도록 수정
# - FIX(STRICT): 상태 판정 helper(is_open_like / is_recoverable) 추가
# 2026-03-02
# - 상태 머신 신규 도입 (암묵적 상태 변경 제거용)
# - 상태 전이 규칙을 단일 매핑으로 고정
# ============================================================

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional, Protocol, runtime_checkable

from dataclasses import dataclass


class StateViolation(RuntimeError):
    """엔진 상태 전이 규칙 위반(폴백 금지: 즉시 중단)."""


class TradeLifecycleState(str, Enum):
    IDLE = "IDLE"

    ENTER_PENDING = "ENTER_PENDING"
    ENTERED = "ENTERED"

    PROTECTION_PENDING = "PROTECTION_PENDING"
    PROTECTION_VERIFIED = "PROTECTION_VERIFIED"

    EXIT_PENDING = "EXIT_PENDING"
    CLOSED = "CLOSED"

    DESYNC = "DESYNC"
    RECOVERY = "RECOVERY"

    ERROR = "ERROR"


class TradeEvent(str, Enum):
    REQUEST_ENTER = "REQUEST_ENTER"
    ENTER_ORDER_SUBMITTED = "ENTER_ORDER_SUBMITTED"
    ENTER_FILLED = "ENTER_FILLED"

    PROTECTION_ORDERS_SUBMITTED = "PROTECTION_ORDERS_SUBMITTED"
    PROTECTION_VERIFIED = "PROTECTION_VERIFIED"

    REQUEST_EXIT = "REQUEST_EXIT"
    EXIT_ORDER_SUBMITTED = "EXIT_ORDER_SUBMITTED"
    EXIT_FILLED = "EXIT_FILLED"

    SYNC_SET_ENTERED = "SYNC_SET_ENTERED"
    SYNC_SET_PROTECTION_VERIFIED = "SYNC_SET_PROTECTION_VERIFIED"
    SYNC_SET_CLOSED = "SYNC_SET_CLOSED"

    DETECT_DESYNC = "DETECT_DESYNC"
    START_RECOVERY = "START_RECOVERY"
    RECOVERY_RESTORED_ENTERED = "RECOVERY_RESTORED_ENTERED"
    RECOVERY_RESTORED_PROTECTION_VERIFIED = "RECOVERY_RESTORED_PROTECTION_VERIFIED"

    SET_ERROR = "SET_ERROR"
    RESET_TO_IDLE = "RESET_TO_IDLE"


@runtime_checkable
class SupportsLifecycleState(Protocol):
    lifecycle_state: TradeLifecycleState


@dataclass(frozen=True)
class Transition:
    from_state: TradeLifecycleState
    event: TradeEvent
    to_state: TradeLifecycleState


_TRANSITIONS: dict[tuple[TradeLifecycleState, TradeEvent], TradeLifecycleState] = {
    # Entry lifecycle
    (TradeLifecycleState.IDLE, TradeEvent.REQUEST_ENTER): TradeLifecycleState.ENTER_PENDING,
    (TradeLifecycleState.ENTER_PENDING, TradeEvent.ENTER_ORDER_SUBMITTED): TradeLifecycleState.ENTER_PENDING,
    (TradeLifecycleState.ENTER_PENDING, TradeEvent.ENTER_FILLED): TradeLifecycleState.ENTERED,

    # Protection lifecycle
    (TradeLifecycleState.ENTERED, TradeEvent.PROTECTION_ORDERS_SUBMITTED): TradeLifecycleState.PROTECTION_PENDING,
    (TradeLifecycleState.PROTECTION_PENDING, TradeEvent.PROTECTION_VERIFIED): TradeLifecycleState.PROTECTION_VERIFIED,

    # Sync-driven open-state restore
    (TradeLifecycleState.ENTER_PENDING, TradeEvent.SYNC_SET_ENTERED): TradeLifecycleState.ENTERED,
    (TradeLifecycleState.ENTER_PENDING, TradeEvent.SYNC_SET_PROTECTION_VERIFIED): TradeLifecycleState.PROTECTION_VERIFIED,
    (TradeLifecycleState.ENTERED, TradeEvent.SYNC_SET_PROTECTION_VERIFIED): TradeLifecycleState.PROTECTION_VERIFIED,
    (TradeLifecycleState.PROTECTION_PENDING, TradeEvent.SYNC_SET_PROTECTION_VERIFIED): TradeLifecycleState.PROTECTION_VERIFIED,

    # Exit lifecycle
    (TradeLifecycleState.ENTERED, TradeEvent.REQUEST_EXIT): TradeLifecycleState.EXIT_PENDING,
    (TradeLifecycleState.PROTECTION_PENDING, TradeEvent.REQUEST_EXIT): TradeLifecycleState.EXIT_PENDING,
    (TradeLifecycleState.PROTECTION_VERIFIED, TradeEvent.REQUEST_EXIT): TradeLifecycleState.EXIT_PENDING,

    (TradeLifecycleState.EXIT_PENDING, TradeEvent.EXIT_ORDER_SUBMITTED): TradeLifecycleState.EXIT_PENDING,
    (TradeLifecycleState.EXIT_PENDING, TradeEvent.EXIT_FILLED): TradeLifecycleState.CLOSED,

    # Sync-driven close restore
    (TradeLifecycleState.ENTERED, TradeEvent.SYNC_SET_CLOSED): TradeLifecycleState.CLOSED,
    (TradeLifecycleState.PROTECTION_PENDING, TradeEvent.SYNC_SET_CLOSED): TradeLifecycleState.CLOSED,
    (TradeLifecycleState.PROTECTION_VERIFIED, TradeEvent.SYNC_SET_CLOSED): TradeLifecycleState.CLOSED,
    (TradeLifecycleState.EXIT_PENDING, TradeEvent.SYNC_SET_CLOSED): TradeLifecycleState.CLOSED,
    (TradeLifecycleState.DESYNC, TradeEvent.SYNC_SET_CLOSED): TradeLifecycleState.CLOSED,
    (TradeLifecycleState.RECOVERY, TradeEvent.SYNC_SET_CLOSED): TradeLifecycleState.CLOSED,

    # Desync / recovery lifecycle
    (TradeLifecycleState.ENTERED, TradeEvent.DETECT_DESYNC): TradeLifecycleState.DESYNC,
    (TradeLifecycleState.PROTECTION_PENDING, TradeEvent.DETECT_DESYNC): TradeLifecycleState.DESYNC,
    (TradeLifecycleState.PROTECTION_VERIFIED, TradeEvent.DETECT_DESYNC): TradeLifecycleState.DESYNC,
    (TradeLifecycleState.EXIT_PENDING, TradeEvent.DETECT_DESYNC): TradeLifecycleState.DESYNC,

    (TradeLifecycleState.DESYNC, TradeEvent.START_RECOVERY): TradeLifecycleState.RECOVERY,
    (TradeLifecycleState.RECOVERY, TradeEvent.RECOVERY_RESTORED_ENTERED): TradeLifecycleState.ENTERED,
    (TradeLifecycleState.RECOVERY, TradeEvent.RECOVERY_RESTORED_PROTECTION_VERIFIED): TradeLifecycleState.PROTECTION_VERIFIED,

    # Reset
    (TradeLifecycleState.CLOSED, TradeEvent.RESET_TO_IDLE): TradeLifecycleState.IDLE,
    (TradeLifecycleState.ERROR, TradeEvent.RESET_TO_IDLE): TradeLifecycleState.IDLE,
}

_TERMINAL_STATES: frozenset[TradeLifecycleState] = frozenset(
    {TradeLifecycleState.CLOSED, TradeLifecycleState.ERROR}
)

_OPEN_LIKE_STATES: frozenset[TradeLifecycleState] = frozenset(
    {
        TradeLifecycleState.ENTERED,
        TradeLifecycleState.PROTECTION_PENDING,
        TradeLifecycleState.PROTECTION_VERIFIED,
        TradeLifecycleState.EXIT_PENDING,
        TradeLifecycleState.DESYNC,
        TradeLifecycleState.RECOVERY,
    }
)

_RECOVERABLE_STATES: frozenset[TradeLifecycleState] = frozenset(
    {
        TradeLifecycleState.DESYNC,
        TradeLifecycleState.RECOVERY,
    }
)


def _require_tz_aware_datetime(v: Any, name: str) -> datetime:
    if not isinstance(v, datetime):
        raise StateViolation(f"{name} must be datetime (STRICT)")
    if v.tzinfo is None or v.tzinfo.utcoffset(v) is None:
        raise StateViolation(f"{name} must be timezone-aware datetime (STRICT)")
    return v


def _require_reason(reason: str) -> str:
    if not isinstance(reason, str) or not reason.strip():
        raise StateViolation("reason must be a non-empty string (STRICT · NO-FALLBACK).")
    return reason.strip()


def get_state(trade: SupportsLifecycleState) -> TradeLifecycleState:
    if not hasattr(trade, "lifecycle_state"):
        raise StateViolation("Trade.lifecycle_state is missing (STRICT · NO-FALLBACK).")
    state = getattr(trade, "lifecycle_state")
    if not isinstance(state, TradeLifecycleState):
        raise StateViolation(
            f"Trade.lifecycle_state must be TradeLifecycleState, got {type(state).__name__}."
        )
    return state


def is_terminal(state: TradeLifecycleState) -> bool:
    return state in _TERMINAL_STATES


def is_open_like(state: TradeLifecycleState) -> bool:
    return state in _OPEN_LIKE_STATES


def is_recoverable(state: TradeLifecycleState) -> bool:
    return state in _RECOVERABLE_STATES


def can_apply(state: TradeLifecycleState, event: TradeEvent) -> bool:
    if event == TradeEvent.SET_ERROR:
        return True
    return (state, event) in _TRANSITIONS


def assert_can_apply(trade: SupportsLifecycleState, event: TradeEvent) -> None:
    state = get_state(trade)
    if not can_apply(state, event):
        raise StateViolation(f"Invalid transition: state={state.value} event={event.value}")


def transition(
    trade: SupportsLifecycleState,
    event: TradeEvent,
    *,
    reason: str,
    changed_at: Optional[datetime] = None,
) -> Transition:
    reason_s = _require_reason(reason)

    from_state = get_state(trade)

    if event == TradeEvent.SET_ERROR:
        to_state = TradeLifecycleState.ERROR
    else:
        key = (from_state, event)
        if key not in _TRANSITIONS:
            raise StateViolation(f"Invalid transition: state={from_state.value} event={event.value}")
        to_state = _TRANSITIONS[key]

    _set_trade_state(trade, to_state, reason=reason_s, changed_at=changed_at)

    return Transition(from_state=from_state, event=event, to_state=to_state)


def apply_event(
    trade: SupportsLifecycleState,
    event: TradeEvent,
    *,
    reason: str,
    changed_at: Optional[datetime] = None,
) -> TradeLifecycleState:
    t = transition(trade, event, reason=reason, changed_at=changed_at)
    return t.to_state


def assert_can_request_enter(trade: SupportsLifecycleState) -> None:
    state = get_state(trade)
    if state != TradeLifecycleState.IDLE:
        raise StateViolation(f"ENTER not allowed: current_state={state.value}")


def assert_can_request_exit(trade: SupportsLifecycleState) -> None:
    state = get_state(trade)
    if state not in (
        TradeLifecycleState.ENTERED,
        TradeLifecycleState.PROTECTION_PENDING,
        TradeLifecycleState.PROTECTION_VERIFIED,
    ):
        raise StateViolation(f"EXIT not allowed: current_state={state.value}")


def _set_trade_state(
    trade: Any,
    new_state: TradeLifecycleState,
    *,
    reason: str,
    changed_at: Optional[datetime] = None,
) -> None:
    if not hasattr(trade, "lifecycle_state"):
        raise StateViolation("Trade.lifecycle_state is missing (STRICT · NO-FALLBACK).")
    if not isinstance(new_state, TradeLifecycleState):
        raise StateViolation(f"new_state must be TradeLifecycleState, got {type(new_state).__name__}.")

    setattr(trade, "lifecycle_state", new_state)

    if hasattr(trade, "last_state_change_reason"):
        setattr(trade, "last_state_change_reason", reason)

    if changed_at is not None:
        ts = _require_tz_aware_datetime(changed_at, "changed_at")
        if hasattr(trade, "last_state_change_at"):
            setattr(trade, "last_state_change_at", ts)