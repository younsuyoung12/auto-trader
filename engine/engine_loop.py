# engine/engine_loop.py
"""
============================================================
FILE: engine/engine_loop.py
ROLE:
- trading engine main loop orchestrator
- bootstrap 이후 각 cycle(entry / exit / monitoring)를 deterministic 하게 실행한다

CORE RESPONSIBILITIES:
- monitoring cycle 실행
- open-position cycle 실행
- idle entry cycle 실행
- SAFE_STOP / HALTED 전이 처리
- tick cadence 유지
- loop runtime state를 단일 객체로 관리

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- engine loop 는 orchestration 만 담당한다
- signal / risk / execution / state 세부 구현을 직접 가지지 않는다
- cycle failure 는 즉시 예외 전파한다
- silent continue / hidden fallback 금지
- runtime state rollback 금지
============================================================
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Protocol


class EngineLoopError(RuntimeError):
    """Base error for engine loop."""


class EngineLoopContractError(EngineLoopError):
    """Raised when engine loop contract is violated."""


class EngineLoopHalted(EngineLoopError):
    """Raised when engine loop transitions to HALTED."""


class MonitoringCycleFn(Protocol):
    def __call__(self, now_ts: float, runtime: "EngineLoopRuntime") -> None: ...


class OpenPositionCycleFn(Protocol):
    def __call__(self, now_ts: float, runtime: "EngineLoopRuntime") -> bool: ...


class EntryCycleFn(Protocol):
    def __call__(self, now_ts: float, runtime: "EngineLoopRuntime") -> None: ...


class IdleSafeStopFn(Protocol):
    def __call__(self, now_ts: float, runtime: "EngineLoopRuntime") -> bool: ...


class StopFlagGetterFn(Protocol):
    def __call__(self) -> bool: ...


class SleepFn(Protocol):
    def __call__(self, seconds: float) -> None: ...


class NowFn(Protocol):
    def __call__(self) -> float: ...


def _require_bool(v: Any, name: str) -> bool:
    if not isinstance(v, bool):
        raise EngineLoopContractError(f"{name} must be bool (STRICT)")
    return bool(v)


def _require_float(v: Any, name: str, *, min_value: Optional[float] = None) -> float:
    if isinstance(v, bool):
        raise EngineLoopContractError(f"{name} must be numeric (bool not allowed)")
    try:
        x = float(v)
    except Exception as e:
        raise EngineLoopContractError(f"{name} must be numeric (STRICT): {e}") from e
    if not math.isfinite(x):
        raise EngineLoopContractError(f"{name} must be finite (STRICT)")
    if min_value is not None and x < min_value:
        raise EngineLoopContractError(f"{name} must be >= {min_value} (STRICT)")
    return float(x)


def _require_optional_positive_int(v: Any, name: str) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, bool):
        raise EngineLoopContractError(f"{name} must be int or None (bool not allowed)")
    try:
        iv = int(v)
    except Exception as e:
        raise EngineLoopContractError(f"{name} must be int or None (STRICT): {e}") from e
    if iv <= 0:
        raise EngineLoopContractError(f"{name} must be > 0 when provided (STRICT)")
    return int(iv)


@dataclass(frozen=True)
class EngineLoopConfig:
    tick_sec: float
    open_position_tick_sec: float
    idle_tick_sec: float
    safe_stop_tick_sec: float

    def validate_or_raise(self) -> None:
        _require_float(self.tick_sec, "config.tick_sec", min_value=0.001)
        _require_float(self.open_position_tick_sec, "config.open_position_tick_sec", min_value=0.001)
        _require_float(self.idle_tick_sec, "config.idle_tick_sec", min_value=0.001)
        _require_float(self.safe_stop_tick_sec, "config.safe_stop_tick_sec", min_value=0.001)


@dataclass
class EngineLoopRuntime:
    running: bool = True
    safe_stop_requested: bool = False
    halted: bool = False
    sigterm_deadline_ts: Optional[float] = None

    last_tick_ts: float = 0.0
    last_monitoring_ts: float = 0.0
    last_open_position_cycle_ts: float = 0.0
    last_idle_entry_cycle_ts: float = 0.0
    last_safe_stop_check_ts: float = 0.0

    loop_iteration: int = 0
    extra: Dict[str, Any] = field(default_factory=dict)

    def validate_or_raise(self) -> None:
        _require_bool(self.running, "runtime.running")
        _require_bool(self.safe_stop_requested, "runtime.safe_stop_requested")
        _require_bool(self.halted, "runtime.halted")

        if self.halted and self.running:
            raise EngineLoopContractError("runtime cannot be halted and running simultaneously (STRICT)")

        if self.sigterm_deadline_ts is not None:
            _require_float(self.sigterm_deadline_ts, "runtime.sigterm_deadline_ts", min_value=0.0)

        _require_float(self.last_tick_ts, "runtime.last_tick_ts", min_value=0.0)
        _require_float(self.last_monitoring_ts, "runtime.last_monitoring_ts", min_value=0.0)
        _require_float(self.last_open_position_cycle_ts, "runtime.last_open_position_cycle_ts", min_value=0.0)
        _require_float(self.last_idle_entry_cycle_ts, "runtime.last_idle_entry_cycle_ts", min_value=0.0)
        _require_float(self.last_safe_stop_check_ts, "runtime.last_safe_stop_check_ts", min_value=0.0)

        if not isinstance(self.loop_iteration, int):
            raise EngineLoopContractError("runtime.loop_iteration must be int (STRICT)")
        if self.loop_iteration < 0:
            raise EngineLoopContractError("runtime.loop_iteration must be >= 0 (STRICT)")

        if not isinstance(self.extra, dict):
            raise EngineLoopContractError("runtime.extra must be dict (STRICT)")

    def request_safe_stop(self) -> None:
        self.safe_stop_requested = True

    def halt(self) -> None:
        self.running = False
        self.halted = True


def _interruptible_sleep_or_raise(
    total_sec: float,
    *,
    stop_flag_getter: StopFlagGetterFn,
    runtime: EngineLoopRuntime,
    sleep_fn: SleepFn,
    now_fn: NowFn,
    tick_sec: float,
) -> None:
    total = _require_float(total_sec, "sleep.total_sec", min_value=0.0)
    tick = _require_float(tick_sec, "sleep.tick_sec", min_value=0.001)

    if total <= 0.0:
        return

    end_ts = now_fn() + total
    while True:
        if stop_flag_getter():
            return

        current_now = now_fn()
        if runtime.sigterm_deadline_ts is not None:
            deadline = _require_float(runtime.sigterm_deadline_ts, "runtime.sigterm_deadline_ts", min_value=0.0)
            if current_now >= deadline:
                return

        remain = end_ts - current_now
        if remain <= 0.0:
            return

        sleep_fn(min(tick, remain))


def run_engine_loop_or_raise(
    *,
    config: EngineLoopConfig,
    runtime: EngineLoopRuntime,
    monitoring_cycle_fn: MonitoringCycleFn,
    open_position_cycle_fn: OpenPositionCycleFn,
    entry_cycle_fn: EntryCycleFn,
    idle_safe_stop_fn: IdleSafeStopFn,
    stop_flag_getter: StopFlagGetterFn,
    sleep_fn: SleepFn = time.sleep,
    now_fn: NowFn = time.time,
) -> None:
    """
    STRICT engine loop orchestration.

    loop order
    1) monitoring
    2) open-position cycle
    3) idle safe-stop resolution
    4) idle entry cycle
    """

    if monitoring_cycle_fn is None:
        raise EngineLoopContractError("monitoring_cycle_fn is required (STRICT)")
    if open_position_cycle_fn is None:
        raise EngineLoopContractError("open_position_cycle_fn is required (STRICT)")
    if entry_cycle_fn is None:
        raise EngineLoopContractError("entry_cycle_fn is required (STRICT)")
    if idle_safe_stop_fn is None:
        raise EngineLoopContractError("idle_safe_stop_fn is required (STRICT)")
    if stop_flag_getter is None:
        raise EngineLoopContractError("stop_flag_getter is required (STRICT)")

    config.validate_or_raise()
    runtime.validate_or_raise()

    if runtime.halted:
        raise EngineLoopHalted("engine loop cannot start from HALTED state (STRICT)")

    while runtime.running:
        now_ts = _require_float(now_fn(), "now_ts", min_value=0.0)

        if runtime.last_tick_ts > 0.0 and now_ts < runtime.last_tick_ts:
            raise EngineLoopContractError(
                f"engine loop clock rollback detected (STRICT): prev={runtime.last_tick_ts} now={now_ts}"
            )

        runtime.loop_iteration += 1
        runtime.last_tick_ts = now_ts

        if stop_flag_getter():
            runtime.request_safe_stop()

        monitoring_cycle_fn(now_ts, runtime)
        runtime.last_monitoring_ts = now_ts

        handled_open_position = open_position_cycle_fn(now_ts, runtime)
        if not isinstance(handled_open_position, bool):
            raise EngineLoopContractError("open_position_cycle_fn must return bool (STRICT)")

        if handled_open_position:
            runtime.last_open_position_cycle_ts = now_ts
            _interruptible_sleep_or_raise(
                config.open_position_tick_sec,
                stop_flag_getter=stop_flag_getter,
                runtime=runtime,
                sleep_fn=sleep_fn,
                now_fn=now_fn,
                tick_sec=config.tick_sec,
            )
            continue

        if runtime.safe_stop_requested:
            runtime.last_safe_stop_check_ts = now_ts
            should_halt = idle_safe_stop_fn(now_ts, runtime)
            if not isinstance(should_halt, bool):
                raise EngineLoopContractError("idle_safe_stop_fn must return bool (STRICT)")
            if should_halt:
                runtime.halt()
                raise EngineLoopHalted("engine loop halted after SAFE_STOP resolution")
            _interruptible_sleep_or_raise(
                config.safe_stop_tick_sec,
                stop_flag_getter=stop_flag_getter,
                runtime=runtime,
                sleep_fn=sleep_fn,
                now_fn=now_fn,
                tick_sec=config.tick_sec,
            )
            continue

        entry_cycle_fn(now_ts, runtime)
        runtime.last_idle_entry_cycle_ts = now_ts

        _interruptible_sleep_or_raise(
            config.idle_tick_sec,
            stop_flag_getter=stop_flag_getter,
            runtime=runtime,
            sleep_fn=sleep_fn,
            now_fn=now_fn,
            tick_sec=config.tick_sec,
        )


__all__ = [
    "EngineLoopError",
    "EngineLoopContractError",
    "EngineLoopHalted",
    "EngineLoopConfig",
    "EngineLoopRuntime",
    "run_engine_loop_or_raise",
]