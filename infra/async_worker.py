from __future__ import annotations

"""
========================================================
FILE: infra/async_worker.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================

역할
--------------------------------------------------------
- 메인 트레이딩 루프에서 발생하는 외부 I/O 작업(DB 기록, Telegram 알림, metrics push 등)을
  블로킹 없이 백그라운드 워커로 위임한다.
- 매매 엔진은 submit()만 호출하고 즉시 리턴해야 한다.

핵심 원칙 (STRICT)
--------------------------------------------------------
1) submit()은 기본적으로 절대 block 하지 않는다.
2) 작업 실패는 조용히 삼키지 않는다:
   - 예외는 반드시 로깅하고 실패 카운트에 반영한다.
3) 비중요 작업(예: 알림)은 명시적 DROP(SKIP) 가능 (critical=False).
4) 중요 작업(예: 상태/거래 기록 등)은 큐 적체 등 문제 시 즉시 예외로 fail-fast (critical=True).
5) 공통 STRICT 예외 계층을 사용한다.
6) worker start/stop/config 상태는 thread-safe 하게 관리한다.

PATCH NOTES
--------------------------------------------------------
- 2026-03-13:
  1) FIX(EXCEPTION): common.exceptions_strict 공통 예외 계층 적용
  2) FIX(STATE): start_worker 재호출 시 설정 불일치면 즉시 예외
  3) FIX(CONCURRENCY): worker loop에 queue 참조를 직접 주입하여 stop 시점 경쟁 상태 완화
  4) FIX(OBSERVABILITY): get_stats()에 started/config/queue_size 포함
  5) FIX(SHUTDOWN): stop_worker() 시 queued task drain 후 종료하도록 명시
- 2026-03-02:
  1) Initial implementation: thread-based async task queue, non-blocking submit,
     explicit DROP policy for non-critical tasks, stats counters.
========================================================
"""

import logging
import math
import queue
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from common.exceptions_strict import StrictConfigError, StrictDataError, StrictStateError

logger = logging.getLogger(__name__)


class AsyncWorkerError(RuntimeError):
    """async_worker base error."""


class AsyncWorkerConfigError(StrictConfigError):
    """async_worker 설정 계약 위반."""


class AsyncWorkerContractError(StrictDataError):
    """async_worker 입력/task 계약 위반."""


class AsyncWorkerStateError(StrictStateError):
    """async_worker 내부 상태 위반."""


def _require_nonempty_str(v: Any, name: str) -> str:
    if not isinstance(v, str):
        raise AsyncWorkerContractError(f"{name} must be str (STRICT), got={type(v).__name__}")
    s = v.strip()
    if not s:
        raise AsyncWorkerContractError(f"{name} must not be empty (STRICT)")
    return s


def _require_positive_int(v: Any, name: str, *, error_cls=AsyncWorkerContractError) -> int:
    if isinstance(v, bool):
        raise error_cls(f"{name} must be int (STRICT), bool not allowed")
    try:
        iv = int(v)
    except Exception as exc:
        raise error_cls(f"{name} must be int (STRICT): {exc}") from exc
    if iv <= 0:
        raise error_cls(f"{name} must be > 0 (STRICT)")
    return iv


def _require_positive_float(v: Any, name: str, *, error_cls=AsyncWorkerContractError) -> float:
    if isinstance(v, bool):
        raise error_cls(f"{name} must be float (STRICT), bool not allowed")
    try:
        fv = float(v)
    except Exception as exc:
        raise error_cls(f"{name} must be float (STRICT): {exc}") from exc
    if not math.isfinite(fv):
        raise error_cls(f"{name} must be finite (STRICT)")
    if fv <= 0.0:
        raise error_cls(f"{name} must be > 0 (STRICT)")
    return float(fv)


def _require_bool(v: Any, name: str, *, error_cls=AsyncWorkerContractError) -> bool:
    if not isinstance(v, bool):
        raise error_cls(f"{name} must be bool (STRICT)")
    return bool(v)


@dataclass(frozen=True, slots=True)
class AsyncWorkerConfig:
    num_threads: int
    max_queue_size: int
    thread_name_prefix: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "num_threads",
            _require_positive_int(self.num_threads, "cfg.num_threads", error_cls=AsyncWorkerConfigError),
        )
        object.__setattr__(
            self,
            "max_queue_size",
            _require_positive_int(self.max_queue_size, "cfg.max_queue_size", error_cls=AsyncWorkerConfigError),
        )
        object.__setattr__(
            self,
            "thread_name_prefix",
            _require_nonempty_str(self.thread_name_prefix, "cfg.thread_name_prefix"),
        )


@dataclass(frozen=True, slots=True)
class _Task:
    task_id: str
    func: Callable[..., Any]
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]
    label: str
    created_ts: float

    def __post_init__(self) -> None:
        object.__setattr__(self, "task_id", _require_nonempty_str(self.task_id, "task.task_id"))
        if not callable(self.func):
            raise AsyncWorkerContractError("task.func must be callable (STRICT)")
        if not isinstance(self.args, tuple):
            raise AsyncWorkerContractError("task.args must be tuple (STRICT)")
        if not isinstance(self.kwargs, dict):
            raise AsyncWorkerContractError("task.kwargs must be dict (STRICT)")
        object.__setattr__(self, "label", _require_nonempty_str(self.label, "task.label"))
        object.__setattr__(self, "created_ts", _require_positive_float(self.created_ts, "task.created_ts"))


# Internal state (module-level singleton)
_lock = threading.RLock()
_started = False
_stop_event = threading.Event()
_q: Optional["queue.Queue[_Task]"] = None
_threads: List[threading.Thread] = []
_config: Optional[AsyncWorkerConfig] = None

# Stats
_stat_lock = threading.Lock()
_enqueued = 0
_dropped = 0
_succeeded = 0
_failed = 0


def start_worker(
    *,
    num_threads: int = 1,
    max_queue_size: int = 2000,
    thread_name_prefix: str = "async-worker",
) -> None:
    """
    워커 스레드를 시작한다.

    - 이미 시작된 상태에서 동일 설정이면 no-op
    - 이미 시작된 상태에서 다른 설정이면 즉시 예외
    """
    global _started, _q, _threads, _config

    cfg = AsyncWorkerConfig(
        num_threads=num_threads,
        max_queue_size=max_queue_size,
        thread_name_prefix=thread_name_prefix,
    )

    with _lock:
        if _started:
            if _config != cfg:
                raise AsyncWorkerStateError(
                    "Async worker already started with different config (STRICT)"
                )
            return

        _stop_event.clear()
        qref: "queue.Queue[_Task]" = queue.Queue(maxsize=cfg.max_queue_size)
        threads: List[threading.Thread] = []

        try:
            for i in range(cfg.num_threads):
                t = threading.Thread(
                    target=_worker_loop,
                    args=(qref,),
                    name=f"{cfg.thread_name_prefix}-{i + 1}",
                    daemon=True,
                )
                t.start()
                threads.append(t)
        except Exception as exc:
            _stop_event.set()
            for t in threads:
                try:
                    t.join(timeout=1.0)
                except Exception:
                    pass
            raise AsyncWorkerStateError("worker thread start failed (STRICT)") from exc

        _q = qref
        _threads = threads
        _config = cfg
        _started = True

    logger.info(
        "[ASYNC_WORKER] started num_threads=%d max_queue_size=%d thread_name_prefix=%s",
        cfg.num_threads,
        cfg.max_queue_size,
        cfg.thread_name_prefix,
    )


def stop_worker(*, join_timeout_sec: float = 2.0) -> None:
    """
    워커를 중지한다. (테스트/종료용)
    - stop 신호 후 큐를 가능한 범위까지 drain 하고 종료한다.
    """
    global _started, _q, _threads, _config

    timeout = _require_positive_float(join_timeout_sec, "join_timeout_sec", error_cls=AsyncWorkerConfigError)

    with _lock:
        if not _started:
            return
        _stop_event.set()
        threads = list(_threads)

    for t in threads:
        t.join(timeout=timeout)

    with _lock:
        _started = False
        _q = None
        _threads = []
        _config = None

    logger.info("[ASYNC_WORKER] stopped")


def submit(
    func: Callable[..., Any],
    *args: Any,
    critical: bool = False,
    label: str = "",
    **kwargs: Any,
) -> bool:
    """
    작업을 비동기로 큐에 넣는다. (기본: non-blocking)

    반환:
    - True: 정상 enqueue
    - False: (critical=False 일 때만) 큐가 가득 차서 명시적으로 DROP(SKIP)

    예외:
    - 워커 미시작: AsyncWorkerStateError
    - critical=True + 큐 포화: AsyncWorkerStateError
    - func가 callable이 아님: AsyncWorkerContractError
    """
    if not callable(func):
        raise AsyncWorkerContractError("func must be callable (STRICT)")
    _require_bool(critical, "critical")

    label_value = label.strip() if isinstance(label, str) else ""
    if not label_value:
        func_name = getattr(func, "__name__", None)
        label_value = func_name.strip() if isinstance(func_name, str) and func_name.strip() else "task"

    with _lock:
        if not _started or _q is None or _config is None:
            raise AsyncWorkerStateError("Async worker is not started. Call start_worker() first. (STRICT)")
        qref = _q

    task = _Task(
        task_id=uuid.uuid4().hex,
        func=func,
        args=args,
        kwargs=kwargs,
        label=label_value,
        created_ts=time.time(),
    )

    try:
        qref.put(task, block=False)
    except queue.Full:
        _inc_dropped()
        if critical:
            raise AsyncWorkerStateError(
                f"Async queue is full. critical task rejected label={task.label} (STRICT)"
            )
        logger.warning(
            "[ASYNC_WORKER] drop (queue full) critical=%s label=%s",
            critical,
            task.label,
        )
        return False

    _inc_enqueued()
    return True


def get_stats() -> Dict[str, Any]:
    """운영 모니터링용 카운터 + 상태."""
    with _lock:
        started = _started
        cfg = _config
        qref = _q

    queue_size = 0
    if qref is not None:
        try:
            queue_size = qref.qsize()
        except Exception:
            queue_size = -1

    with _stat_lock:
        return {
            "started": started,
            "config": None
            if cfg is None
            else {
                "num_threads": cfg.num_threads,
                "max_queue_size": cfg.max_queue_size,
                "thread_name_prefix": cfg.thread_name_prefix,
            },
            "queue_size": queue_size,
            "enqueued": _enqueued,
            "dropped": _dropped,
            "succeeded": _succeeded,
            "failed": _failed,
        }


def _worker_loop(qref: "queue.Queue[_Task]") -> None:
    while True:
        if _stop_event.is_set():
            try:
                task = qref.get(block=False)
            except queue.Empty:
                return
        else:
            try:
                task = qref.get(timeout=0.2)
            except queue.Empty:
                continue

        try:
            task.func(*task.args, **task.kwargs)
        except Exception:
            _inc_failed()
            logger.exception(
                "[ASYNC_WORKER] task failed label=%s task_id=%s",
                task.label,
                task.task_id,
            )
        else:
            _inc_succeeded()
        finally:
            try:
                qref.task_done()
            except Exception:
                logger.exception("[ASYNC_WORKER] queue.task_done failed")


def _inc_enqueued() -> None:
    global _enqueued
    with _stat_lock:
        _enqueued += 1


def _inc_dropped() -> None:
    global _dropped
    with _stat_lock:
        _dropped += 1


def _inc_succeeded() -> None:
    global _succeeded
    with _stat_lock:
        _succeeded += 1


def _inc_failed() -> None:
    global _failed
    with _stat_lock:
        _failed += 1


__all__ = [
    "AsyncWorkerError",
    "AsyncWorkerConfigError",
    "AsyncWorkerContractError",
    "AsyncWorkerStateError",
    "AsyncWorkerConfig",
    "start_worker",
    "stop_worker",
    "submit",
    "get_stats",
]