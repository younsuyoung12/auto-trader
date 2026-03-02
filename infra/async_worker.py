# infra/async_worker.py
"""
========================================================
infra/async_worker.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================

역할
--------------------------------------------------------
- 메인 트레이딩 루프에서 발생하는 외부 I/O 작업(DB 기록, Telegram 알림, metrics push 등)을
  **블로킹 없이** 백그라운드 워커로 위임한다.
- 매매 엔진은 submit()만 호출하고 즉시 리턴해야 한다.

핵심 원칙 (STRICT)
--------------------------------------------------------
1) submit()은 기본적으로 **절대 block 하지 않는다** (큐가 꽉 차면 즉시 처리).
2) 작업 실패는 **조용히 삼키지 않는다**:
   - 예외는 반드시 로깅하고 실패 카운트에 반영한다.
3) 비중요 작업(예: 알림)은 **명시적 DROP(SKIP)** 가능 (critical=False).
4) 중요 작업(예: 상태/거래 기록 등)은 큐 적체 등 문제 시 **즉시 예외로 fail-fast** (critical=True).

PATCH NOTES — 2026-03-02
--------------------------------------------------------
- Initial implementation: thread-based async task queue, non-blocking submit,
  explicit DROP policy for non-critical tasks, stats counters.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _Task:
    task_id: str
    func: Callable[..., Any]
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]
    label: str
    created_ts: float


# Internal state (module-level singleton)
_lock = threading.Lock()
_started = False
_stop_event = threading.Event()
_q: Optional["queue.Queue[_Task]"] = None
_threads: List[threading.Thread] = []

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
    워커 스레드를 시작한다. (idempotent)

    - num_threads: 워커 스레드 개수
    - max_queue_size: 큐 최대 적재량 (초과 시 submit()은 즉시 DROP 또는 fail-fast)
    """
    if num_threads < 1:
        raise ValueError("num_threads must be >= 1")
    if max_queue_size < 1:
        raise ValueError("max_queue_size must be >= 1")

    global _started, _q, _threads

    with _lock:
        if _started:
            return

        _stop_event.clear()
        _q = queue.Queue(maxsize=max_queue_size)
        _threads = []

        for i in range(num_threads):
            t = threading.Thread(
                target=_worker_loop,
                name=f"{thread_name_prefix}-{i+1}",
                daemon=True,
            )
            t.start()
            _threads.append(t)

        _started = True
        logger.info(
            "[ASYNC_WORKER] started num_threads=%d max_queue_size=%d",
            num_threads,
            max_queue_size,
        )


def stop_worker(*, join_timeout_sec: float = 2.0) -> None:
    """
    워커를 중지한다. (테스트/종료용)
    프로덕션 런타임에서 굳이 호출할 필요는 없다.
    """
    global _started, _q, _threads

    with _lock:
        if not _started:
            return
        _stop_event.set()

    # threads are daemon; best-effort join
    for t in list(_threads):
        t.join(timeout=join_timeout_sec)

    with _lock:
        _started = False
        _q = None
        _threads = []

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
    - 워커 미시작: RuntimeError
    - critical=True + 큐 포화: RuntimeError (fail-fast)
    - func가 callable이 아님: TypeError
    """
    if not callable(func):
        raise TypeError("func must be callable")

    with _lock:
        if not _started or _q is None:
            raise RuntimeError("Async worker is not started. Call start_worker() first.")
        q = _q

    task = _Task(
        task_id=uuid.uuid4().hex,
        func=func,
        args=args,
        kwargs=kwargs,
        label=label or getattr(func, "__name__", "task"),
        created_ts=time.time(),
    )

    try:
        # block=False: 엔진 발목 잡지 않음
        q.put(task, block=False)
    except queue.Full:
        _inc_dropped()
        if critical:
            raise RuntimeError(
                f"Async queue is full. critical task rejected label={task.label}"
            )
        logger.warning(
            "[ASYNC_WORKER] drop (queue full) critical=%s label=%s",
            critical,
            task.label,
        )
        return False

    _inc_enqueued()
    return True


def get_stats() -> Dict[str, int]:
    """운영 모니터링용 카운터."""
    with _stat_lock:
        return {
            "enqueued": _enqueued,
            "dropped": _dropped,
            "succeeded": _succeeded,
            "failed": _failed,
        }


def _worker_loop() -> None:
    assert _q is not None  # start_worker 보장

    while not _stop_event.is_set():
        try:
            task = _q.get(timeout=0.2)  # type: ignore[union-attr]
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
                _q.task_done()  # type: ignore[union-attr]
            except Exception:
                # 여기서 예외가 나면 queue 내부 상태가 깨진 것이므로 로그 후 계속.
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