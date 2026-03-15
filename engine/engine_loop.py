# engine/engine_loop.py
"""
============================================================
FILE: engine/engine_loop.py
ROLE:
- trading engine main loop orchestrator
- bootstrap 이후 각 cycle(monitoring / position-supervisor / reconciliation / open-position / idle entry / safe-stop resolution)를
  deterministic 하게 실행한다
- 기관형 엔진 상태기계(BOOTING / RUNNING / SAFE_STOP / RECOVERY / HALTED)를
  단일 runtime 객체로 관리한다

CORE RESPONSIBILITIES:
- monitoring cycle 실행
- position supervisor cycle 실행
- reconciliation cycle 실행
- open-position cycle 실행
- idle entry cycle 실행
- SAFE_STOP / RECOVERY / HALTED 전이 처리
- tick cadence 유지
- loop runtime state를 단일 객체로 관리
- 상태 전이 사유(reason)와 시각(ts)을 strict 하게 보존
- execution 직후 position supervisor / reconciliation 이 같은 루프 안에서 실행되도록 보장

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- engine loop 는 orchestration 만 담당한다
- signal / risk / execution / state 세부 구현을 직접 가지지 않는다
- cycle failure 는 즉시 예외 전파한다
- silent continue / hidden fallback 금지
- runtime state rollback 금지
- BOOTING / RUNNING / SAFE_STOP / RECOVERY / HALTED 상태 계약 위반 금지
- SAFE_STOP 상태에서 신규 진입 실행 금지
- HALTED 전이 시 즉시 loop 종료
============================================================

CHANGE HISTORY:
- 2026-03-15
  1) FIX(ROOT-CAUSE): cycle 계층이 runtime.safe_stop_requested / runtime.halted 만 직접 세팅해도
     engine_loop 가 상태기계로 즉시 동기화하도록 _sync_runtime_state_from_flags_or_raise 추가
  2) FIX(CONTRACT): 각 cycle 직후 bool flag ↔ engine_state 불일치로 validate_or_raise 가 즉시 깨지던 문제 수정
  3) FIX(HALT): halted flag direct-set 도 HALTED 상태로 즉시 승격되도록 동기화 추가
- 2026-03-14
  1) FEAT(ARCH): position_supervisor_cycle_fn / reconciliation_cycle_fn 정식 orchestration 슬롯 추가
  2) FEAT(ORDER): monitoring → position_supervisor → open_position → reconciliation → safe-stop / entry 순서로 loop 재구성
  3) FIX(ROOT-CAUSE): open_position handled 이후 safe_stop_requested=True 면 즉시 SAFE_STOP 분기하도록 수정
  4) FIX(OPERABILITY): last_position_supervisor_ts / last_reconciliation_ts 추적 추가
  5) FIX(HALT): 각 cycle 이후 HALTED 상태 즉시 감지/종료 추가
- 2026-03-13
  1) FIX(STATE-MACHINE): BOOTING / RUNNING / SAFE_STOP / RECOVERY / HALTED 명시 상태기계 도입
  2) FIX(OPERABILITY): safe_stop_reason / halt_reason / recovery_reason / last_state_transition_ts 추적 추가
  3) FIX(CONTRACT): runtime boolean 플래그와 engine_state 정합성 strict 검증 추가
  4) FIX(LOOP): SAFE_STOP 해소 후 RUNNING 복귀 계약을 명시화
  5) FIX(STRICT): loop flow 를 sleep_sec 기반으로 재구성하여 hidden continue 성격 제거
============================================================
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Protocol


ENGINE_STATE_BOOTING = "BOOTING"
ENGINE_STATE_RUNNING = "RUNNING"
ENGINE_STATE_SAFE_STOP = "SAFE_STOP"
ENGINE_STATE_RECOVERY = "RECOVERY"
ENGINE_STATE_HALTED = "HALTED"

_ALLOWED_ENGINE_STATES = {
    ENGINE_STATE_BOOTING,
    ENGINE_STATE_RUNNING,
    ENGINE_STATE_SAFE_STOP,
    ENGINE_STATE_RECOVERY,
    ENGINE_STATE_HALTED,
}


class EngineLoopError(RuntimeError):
    """Base error for engine loop."""


class EngineLoopContractError(EngineLoopError):
    """Raised when engine loop contract is violated."""


class EngineLoopHalted(EngineLoopError):
    """Raised when engine loop transitions to HALTED."""


class MonitoringCycleFn(Protocol):
    def __call__(self, now_ts: float, runtime: "EngineLoopRuntime") -> None: ...


class PositionSupervisorCycleFn(Protocol):
    def __call__(self, now_ts: float, runtime: "EngineLoopRuntime") -> None: ...


class ReconciliationCycleFn(Protocol):
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
        raise EngineLoopContractError(f"{name} must be numeric (bool not allowed) (STRICT)")
    try:
        x = float(v)
    except Exception as e:
        raise EngineLoopContractError(f"{name} must be numeric (STRICT): {e}") from e
    if not math.isfinite(x):
        raise EngineLoopContractError(f"{name} must be finite (STRICT)")
    if min_value is not None and x < min_value:
        raise EngineLoopContractError(f"{name} must be >= {min_value} (STRICT)")
    return float(x)


def _require_nonempty_str(v: Any, name: str) -> str:
    if v is None:
        raise EngineLoopContractError(f"{name} is required (STRICT)")
    s = str(v).strip()
    if not s:
        raise EngineLoopContractError(f"{name} is empty (STRICT)")
    return s


def _resolve_safe_stop_reason_strict(reason: Optional[str]) -> str:
    if reason is None:
        return "SAFE_STOP_REQUESTED"
    return _require_nonempty_str(reason, "safe_stop_reason")


def _resolve_halt_reason_strict(reason: Optional[str], fallback: Optional[str] = None) -> str:
    if reason is not None:
        return _require_nonempty_str(reason, "halt_reason")
    if fallback is not None:
        return _require_nonempty_str(fallback, "halt_reason_fallback")
    return "HALTED"


def _normalize_engine_state(v: Any, name: str) -> str:
    s = _require_nonempty_str(v, name).upper().strip()
    if s not in _ALLOWED_ENGINE_STATES:
        raise EngineLoopContractError(
            f"{name} invalid (STRICT): {s!r} allowed={sorted(_ALLOWED_ENGINE_STATES)}"
        )
    return s


def _resolve_transition_ts_or_raise(now_ts: Optional[float], runtime: "EngineLoopRuntime") -> float:
    if now_ts is not None:
        return _require_float(now_ts, "transition.now_ts", min_value=0.0)
    if runtime.last_tick_ts > 0.0:
        return _require_float(runtime.last_tick_ts, "runtime.last_tick_ts", min_value=0.0)
    return 0.0


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
    engine_state: str = ENGINE_STATE_BOOTING

    running: bool = True
    safe_stop_requested: bool = False
    halted: bool = False

    sigterm_deadline_ts: Optional[float] = None

    safe_stop_reason: Optional[str] = None
    recovery_reason: Optional[str] = None
    halt_reason: Optional[str] = None

    boot_completed_ts: float = 0.0
    safe_stop_requested_ts: float = 0.0
    recovery_requested_ts: float = 0.0
    halted_ts: float = 0.0
    last_state_transition_ts: float = 0.0

    last_tick_ts: float = 0.0
    last_monitoring_ts: float = 0.0
    last_position_supervisor_ts: float = 0.0
    last_reconciliation_ts: float = 0.0
    last_open_position_cycle_ts: float = 0.0
    last_idle_entry_cycle_ts: float = 0.0
    last_safe_stop_check_ts: float = 0.0

    loop_iteration: int = 0
    extra: Dict[str, Any] = field(default_factory=dict)

    def validate_or_raise(self) -> None:
        self.engine_state = _normalize_engine_state(self.engine_state, "runtime.engine_state")

        _require_bool(self.running, "runtime.running")
        _require_bool(self.safe_stop_requested, "runtime.safe_stop_requested")
        _require_bool(self.halted, "runtime.halted")

        if self.sigterm_deadline_ts is not None:
            _require_float(self.sigterm_deadline_ts, "runtime.sigterm_deadline_ts", min_value=0.0)

        _require_float(self.boot_completed_ts, "runtime.boot_completed_ts", min_value=0.0)
        _require_float(self.safe_stop_requested_ts, "runtime.safe_stop_requested_ts", min_value=0.0)
        _require_float(self.recovery_requested_ts, "runtime.recovery_requested_ts", min_value=0.0)
        _require_float(self.halted_ts, "runtime.halted_ts", min_value=0.0)
        _require_float(self.last_state_transition_ts, "runtime.last_state_transition_ts", min_value=0.0)

        _require_float(self.last_tick_ts, "runtime.last_tick_ts", min_value=0.0)
        _require_float(self.last_monitoring_ts, "runtime.last_monitoring_ts", min_value=0.0)
        _require_float(self.last_position_supervisor_ts, "runtime.last_position_supervisor_ts", min_value=0.0)
        _require_float(self.last_reconciliation_ts, "runtime.last_reconciliation_ts", min_value=0.0)
        _require_float(self.last_open_position_cycle_ts, "runtime.last_open_position_cycle_ts", min_value=0.0)
        _require_float(self.last_idle_entry_cycle_ts, "runtime.last_idle_entry_cycle_ts", min_value=0.0)
        _require_float(self.last_safe_stop_check_ts, "runtime.last_safe_stop_check_ts", min_value=0.0)

        if not isinstance(self.loop_iteration, int):
            raise EngineLoopContractError("runtime.loop_iteration must be int (STRICT)")
        if self.loop_iteration < 0:
            raise EngineLoopContractError("runtime.loop_iteration must be >= 0 (STRICT)")

        if not isinstance(self.extra, dict):
            raise EngineLoopContractError("runtime.extra must be dict (STRICT)")

        if self.safe_stop_reason is not None:
            _require_nonempty_str(self.safe_stop_reason, "runtime.safe_stop_reason")
        if self.recovery_reason is not None:
            _require_nonempty_str(self.recovery_reason, "runtime.recovery_reason")
        if self.halt_reason is not None:
            _require_nonempty_str(self.halt_reason, "runtime.halt_reason")

        if self.engine_state == ENGINE_STATE_BOOTING:
            if not self.running:
                raise EngineLoopContractError("BOOTING runtime must be running=True (STRICT)")
            if self.halted:
                raise EngineLoopContractError("BOOTING runtime cannot be halted=True (STRICT)")
            if self.safe_stop_requested:
                raise EngineLoopContractError("BOOTING runtime cannot have safe_stop_requested=True (STRICT)")

        elif self.engine_state == ENGINE_STATE_RUNNING:
            if not self.running:
                raise EngineLoopContractError("RUNNING runtime must be running=True (STRICT)")
            if self.halted:
                raise EngineLoopContractError("RUNNING runtime cannot be halted=True (STRICT)")
            if self.safe_stop_requested:
                raise EngineLoopContractError("RUNNING runtime cannot have safe_stop_requested=True (STRICT)")

        elif self.engine_state == ENGINE_STATE_SAFE_STOP:
            if not self.running:
                raise EngineLoopContractError("SAFE_STOP runtime must be running=True (STRICT)")
            if self.halted:
                raise EngineLoopContractError("SAFE_STOP runtime cannot be halted=True (STRICT)")
            if not self.safe_stop_requested:
                raise EngineLoopContractError("SAFE_STOP runtime must have safe_stop_requested=True (STRICT)")
            if self.safe_stop_reason is None:
                raise EngineLoopContractError("SAFE_STOP runtime must have safe_stop_reason (STRICT)")
            if self.safe_stop_requested_ts <= 0.0:
                raise EngineLoopContractError("SAFE_STOP runtime must have safe_stop_requested_ts > 0 (STRICT)")

        elif self.engine_state == ENGINE_STATE_RECOVERY:
            if not self.running:
                raise EngineLoopContractError("RECOVERY runtime must be running=True (STRICT)")
            if self.halted:
                raise EngineLoopContractError("RECOVERY runtime cannot be halted=True (STRICT)")
            if not self.safe_stop_requested:
                raise EngineLoopContractError("RECOVERY runtime must have safe_stop_requested=True (STRICT)")
            if self.safe_stop_reason is None:
                raise EngineLoopContractError("RECOVERY runtime must preserve safe_stop_reason (STRICT)")
            if self.recovery_reason is None:
                raise EngineLoopContractError("RECOVERY runtime must have recovery_reason (STRICT)")
            if self.recovery_requested_ts <= 0.0:
                raise EngineLoopContractError("RECOVERY runtime must have recovery_requested_ts > 0 (STRICT)")

        elif self.engine_state == ENGINE_STATE_HALTED:
            if self.running:
                raise EngineLoopContractError("HALTED runtime must be running=False (STRICT)")
            if not self.halted:
                raise EngineLoopContractError("HALTED runtime must have halted=True (STRICT)")
            if self.halt_reason is None:
                raise EngineLoopContractError("HALTED runtime must have halt_reason (STRICT)")
            if self.halted_ts <= 0.0:
                raise EngineLoopContractError("HALTED runtime must have halted_ts > 0 (STRICT)")

    def _set_transition_ts_or_raise(self, now_ts: Optional[float]) -> float:
        ts = _resolve_transition_ts_or_raise(now_ts, self)
        self.last_state_transition_ts = ts
        return ts

    def mark_boot_completed(self, *, reason: str, now_ts: Optional[float] = None) -> None:
        if self.engine_state != ENGINE_STATE_BOOTING:
            raise EngineLoopContractError(
                f"boot completion allowed only from BOOTING (STRICT), current={self.engine_state}"
            )
        _require_nonempty_str(reason, "reason")
        ts = self._set_transition_ts_or_raise(now_ts)
        self.engine_state = ENGINE_STATE_RUNNING
        self.running = True
        self.safe_stop_requested = False
        self.halted = False
        self.safe_stop_reason = None
        self.recovery_reason = None
        self.halt_reason = None
        self.boot_completed_ts = ts

    def request_safe_stop(self, reason: Optional[str] = None, *, now_ts: Optional[float] = None) -> None:
        if self.engine_state == ENGINE_STATE_HALTED:
            raise EngineLoopContractError("cannot request SAFE_STOP from HALTED state (STRICT)")

        normalized_reason = _resolve_safe_stop_reason_strict(reason)
        ts = self._set_transition_ts_or_raise(now_ts)

        if not self.safe_stop_requested:
            self.safe_stop_requested_ts = ts

        self.safe_stop_requested = True
        self.engine_state = ENGINE_STATE_SAFE_STOP
        self.running = True
        self.halted = False

        if self.safe_stop_reason is None:
            self.safe_stop_reason = normalized_reason

    def request_recovery(self, reason: str, *, now_ts: Optional[float] = None) -> None:
        if self.engine_state != ENGINE_STATE_SAFE_STOP:
            raise EngineLoopContractError(
                f"RECOVERY request allowed only from SAFE_STOP (STRICT), current={self.engine_state}"
            )
        _require_nonempty_str(reason, "reason")
        if self.safe_stop_reason is None:
            raise EngineLoopContractError("SAFE_STOP reason must exist before RECOVERY (STRICT)")

        ts = self._set_transition_ts_or_raise(now_ts)
        self.engine_state = ENGINE_STATE_RECOVERY
        self.running = True
        self.halted = False
        self.safe_stop_requested = True
        self.recovery_reason = reason
        self.recovery_requested_ts = ts

    def clear_safe_stop_and_resume(self, reason: str, *, now_ts: Optional[float] = None) -> None:
        if self.engine_state not in (ENGINE_STATE_SAFE_STOP, ENGINE_STATE_RECOVERY):
            raise EngineLoopContractError(
                f"resume allowed only from SAFE_STOP/RECOVERY (STRICT), current={self.engine_state}"
            )
        _require_nonempty_str(reason, "reason")
        ts = self._set_transition_ts_or_raise(now_ts)

        self.engine_state = ENGINE_STATE_RUNNING
        self.running = True
        self.halted = False
        self.safe_stop_requested = False

        self.safe_stop_reason = None
        self.recovery_reason = None
        self.safe_stop_requested_ts = 0.0
        self.recovery_requested_ts = 0.0

        self.extra["last_resume_reason"] = reason
        self.extra["last_resume_ts"] = ts

    def halt(self, reason: Optional[str] = None, *, now_ts: Optional[float] = None) -> None:
        normalized_reason = _resolve_halt_reason_strict(reason)
        ts = self._set_transition_ts_or_raise(now_ts)
        self.engine_state = ENGINE_STATE_HALTED
        self.running = False
        self.halted = True
        self.halt_reason = normalized_reason
        self.halted_ts = ts


def _sync_runtime_state_from_flags_or_raise(runtime: EngineLoopRuntime, *, now_ts: float) -> None:
    _require_bool(runtime.running, "runtime.running")
    _require_bool(runtime.safe_stop_requested, "runtime.safe_stop_requested")
    _require_bool(runtime.halted, "runtime.halted")
    runtime.engine_state = _normalize_engine_state(runtime.engine_state, "runtime.engine_state")
    ts = _require_float(now_ts, "sync.now_ts", min_value=0.0)

    if runtime.halted and runtime.engine_state != ENGINE_STATE_HALTED:
        runtime.halt(
            _resolve_halt_reason_strict(runtime.halt_reason, runtime.safe_stop_reason),
            now_ts=ts,
        )

    if runtime.engine_state == ENGINE_STATE_HALTED:
        runtime.validate_or_raise()
        return

    if runtime.safe_stop_requested and runtime.engine_state in (ENGINE_STATE_BOOTING, ENGINE_STATE_RUNNING):
        runtime.request_safe_stop(
            _resolve_safe_stop_reason_strict(runtime.safe_stop_reason),
            now_ts=ts,
        )

    runtime.validate_or_raise()


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

    end_ts = _require_float(now_fn(), "sleep.start_now_ts", min_value=0.0) + total

    sleeping = True
    while sleeping:
        stop_requested = stop_flag_getter()
        if not isinstance(stop_requested, bool):
            raise EngineLoopContractError("stop_flag_getter must return bool (STRICT)")
        if stop_requested:
            stop_now_ts = _require_float(now_fn(), "sleep.stop_now_ts", min_value=0.0)
            runtime.request_safe_stop("STOP_FLAG", now_ts=stop_now_ts)
            _sync_runtime_state_from_flags_or_raise(runtime, now_ts=stop_now_ts)
            sleeping = False
        else:
            current_now = _require_float(now_fn(), "sleep.current_now", min_value=0.0)

            if runtime.sigterm_deadline_ts is not None:
                deadline = _require_float(runtime.sigterm_deadline_ts, "runtime.sigterm_deadline_ts", min_value=0.0)
                if current_now >= deadline:
                    sleeping = False
                else:
                    remain = end_ts - current_now
                    if remain <= 0.0:
                        sleeping = False
                    else:
                        sleep_fn(min(tick, remain))
            else:
                remain = end_ts - current_now
                if remain <= 0.0:
                    sleeping = False
                else:
                    sleep_fn(min(tick, remain))


def _raise_if_halted_or_stopped_or_raise(runtime: EngineLoopRuntime, *, now_ts: float) -> None:
    _sync_runtime_state_from_flags_or_raise(runtime, now_ts=now_ts)
    if runtime.engine_state == ENGINE_STATE_HALTED or runtime.halted:
        raise EngineLoopHalted(f"engine loop halted: reason={runtime.halt_reason}")


def _run_safe_stop_resolution_or_raise(
    *,
    now_ts: float,
    runtime: EngineLoopRuntime,
    idle_safe_stop_fn: IdleSafeStopFn,
    resume_sleep_sec: float,
    safe_stop_sleep_sec: float,
) -> float:
    _sync_runtime_state_from_flags_or_raise(runtime, now_ts=now_ts)

    if not runtime.safe_stop_requested:
        raise EngineLoopContractError("safe-stop resolution called without safe_stop_requested=True (STRICT)")

    if runtime.engine_state not in (ENGINE_STATE_SAFE_STOP, ENGINE_STATE_RECOVERY):
        raise EngineLoopContractError(
            f"safe-stop resolution requires SAFE_STOP/RECOVERY (STRICT), current={runtime.engine_state}"
        )

    runtime.last_safe_stop_check_ts = now_ts
    should_halt = idle_safe_stop_fn(now_ts, runtime)
    if not isinstance(should_halt, bool):
        raise EngineLoopContractError("idle_safe_stop_fn must return bool (STRICT)")

    if should_halt:
        halt_reason = _resolve_halt_reason_strict(runtime.safe_stop_reason, "SAFE_STOP_RESOLUTION_REQUESTED_HALT")
        runtime.halt(halt_reason, now_ts=now_ts)
        raise EngineLoopHalted(
            f"engine loop halted after SAFE_STOP resolution: reason={runtime.halt_reason}"
        )

    _sync_runtime_state_from_flags_or_raise(runtime, now_ts=now_ts)

    if runtime.safe_stop_requested:
        if runtime.engine_state not in (ENGINE_STATE_SAFE_STOP, ENGINE_STATE_RECOVERY):
            raise EngineLoopContractError(
                "post safe-stop resolution state must be SAFE_STOP/RECOVERY when request still active (STRICT)"
            )
        return _require_float(safe_stop_sleep_sec, "safe_stop_sleep_sec", min_value=0.001)

    if runtime.engine_state != ENGINE_STATE_RUNNING:
        raise EngineLoopContractError(
            "safe_stop cleared but engine_state is not RUNNING (STRICT)"
        )

    return _require_float(resume_sleep_sec, "resume_sleep_sec", min_value=0.001)


def run_engine_loop_or_raise(
    *,
    config: EngineLoopConfig,
    runtime: EngineLoopRuntime,
    monitoring_cycle_fn: MonitoringCycleFn,
    position_supervisor_cycle_fn: PositionSupervisorCycleFn,
    reconciliation_cycle_fn: ReconciliationCycleFn,
    open_position_cycle_fn: OpenPositionCycleFn,
    entry_cycle_fn: EntryCycleFn,
    idle_safe_stop_fn: IdleSafeStopFn,
    stop_flag_getter: StopFlagGetterFn,
    sleep_fn: SleepFn = time.sleep,
    now_fn: NowFn = time.time,
) -> None:
    """
    STRICT institution-grade engine loop orchestration.

    loop order
    1) monitoring
    2) position supervisor
    3) open-position cycle
    4) reconciliation
    5) safe-stop resolution
    6) idle entry cycle
    7) reconciliation
    """

    if monitoring_cycle_fn is None:
        raise EngineLoopContractError("monitoring_cycle_fn is required (STRICT)")
    if position_supervisor_cycle_fn is None:
        raise EngineLoopContractError("position_supervisor_cycle_fn is required (STRICT)")
    if reconciliation_cycle_fn is None:
        raise EngineLoopContractError("reconciliation_cycle_fn is required (STRICT)")
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

    if runtime.engine_state == ENGINE_STATE_HALTED:
        raise EngineLoopHalted("engine loop cannot start from HALTED state (STRICT)")

    if runtime.engine_state not in (ENGINE_STATE_BOOTING, ENGINE_STATE_RUNNING):
        raise EngineLoopContractError(
            f"engine loop can start only from BOOTING/RUNNING (STRICT), current={runtime.engine_state}"
        )

    start_now_ts = _require_float(now_fn(), "loop.start_now_ts", min_value=0.0)
    if runtime.engine_state == ENGINE_STATE_BOOTING:
        runtime.mark_boot_completed(reason="BOOTSTRAP_COMPLETE", now_ts=start_now_ts)

    while runtime.running:
        now_ts = _require_float(now_fn(), "now_ts", min_value=0.0)

        if runtime.last_tick_ts > 0.0 and now_ts < runtime.last_tick_ts:
            raise EngineLoopContractError(
                f"engine loop clock rollback detected (STRICT): prev={runtime.last_tick_ts} now={now_ts}"
            )

        runtime.loop_iteration += 1
        runtime.last_tick_ts = now_ts

        stop_requested = stop_flag_getter()
        if not isinstance(stop_requested, bool):
            raise EngineLoopContractError("stop_flag_getter must return bool (STRICT)")
        if stop_requested:
            runtime.request_safe_stop("STOP_FLAG", now_ts=now_ts)

        _sync_runtime_state_from_flags_or_raise(runtime, now_ts=now_ts)

        monitoring_cycle_fn(now_ts, runtime)
        runtime.last_monitoring_ts = now_ts
        _raise_if_halted_or_stopped_or_raise(runtime, now_ts=now_ts)

        position_supervisor_cycle_fn(now_ts, runtime)
        runtime.last_position_supervisor_ts = now_ts
        _raise_if_halted_or_stopped_or_raise(runtime, now_ts=now_ts)

        handled_open_position = open_position_cycle_fn(now_ts, runtime)
        if not isinstance(handled_open_position, bool):
            raise EngineLoopContractError("open_position_cycle_fn must return bool (STRICT)")
        if handled_open_position:
            runtime.last_open_position_cycle_ts = now_ts
        _raise_if_halted_or_stopped_or_raise(runtime, now_ts=now_ts)

        if handled_open_position:
            reconciliation_cycle_fn(now_ts, runtime)
            runtime.last_reconciliation_ts = now_ts
            _raise_if_halted_or_stopped_or_raise(runtime, now_ts=now_ts)

            if runtime.safe_stop_requested:
                sleep_sec = _run_safe_stop_resolution_or_raise(
                    now_ts=now_ts,
                    runtime=runtime,
                    idle_safe_stop_fn=idle_safe_stop_fn,
                    resume_sleep_sec=config.open_position_tick_sec,
                    safe_stop_sleep_sec=config.safe_stop_tick_sec,
                )
            else:
                sleep_sec = _require_float(config.open_position_tick_sec, "config.open_position_tick_sec", min_value=0.001)

        else:
            if runtime.safe_stop_requested:
                sleep_sec = _run_safe_stop_resolution_or_raise(
                    now_ts=now_ts,
                    runtime=runtime,
                    idle_safe_stop_fn=idle_safe_stop_fn,
                    resume_sleep_sec=config.idle_tick_sec,
                    safe_stop_sleep_sec=config.safe_stop_tick_sec,
                )
            else:
                if runtime.engine_state != ENGINE_STATE_RUNNING:
                    raise EngineLoopContractError(
                        f"entry cycle requires RUNNING state (STRICT), current={runtime.engine_state}"
                    )

                entry_cycle_fn(now_ts, runtime)
                runtime.last_idle_entry_cycle_ts = now_ts
                _raise_if_halted_or_stopped_or_raise(runtime, now_ts=now_ts)

                reconciliation_cycle_fn(now_ts, runtime)
                runtime.last_reconciliation_ts = now_ts
                _raise_if_halted_or_stopped_or_raise(runtime, now_ts=now_ts)

                if runtime.safe_stop_requested:
                    sleep_sec = _run_safe_stop_resolution_or_raise(
                        now_ts=now_ts,
                        runtime=runtime,
                        idle_safe_stop_fn=idle_safe_stop_fn,
                        resume_sleep_sec=config.idle_tick_sec,
                        safe_stop_sleep_sec=config.safe_stop_tick_sec,
                    )
                else:
                    sleep_sec = _require_float(config.idle_tick_sec, "config.idle_tick_sec", min_value=0.001)

        runtime.validate_or_raise()

        _interruptible_sleep_or_raise(
            sleep_sec,
            stop_flag_getter=stop_flag_getter,
            runtime=runtime,
            sleep_fn=sleep_fn,
            now_fn=now_fn,
            tick_sec=config.tick_sec,
        )

    runtime.validate_or_raise()

    if runtime.halted:
        raise EngineLoopHalted(f"engine loop halted: reason={runtime.halt_reason}")

    raise EngineLoopContractError("engine loop exited with running=False without HALTED state (STRICT)")


__all__ = [
    "ENGINE_STATE_BOOTING",
    "ENGINE_STATE_RUNNING",
    "ENGINE_STATE_SAFE_STOP",
    "ENGINE_STATE_RECOVERY",
    "ENGINE_STATE_HALTED",
    "EngineLoopError",
    "EngineLoopContractError",
    "EngineLoopHalted",
    "EngineLoopConfig",
    "EngineLoopRuntime",
    "run_engine_loop_or_raise",
]