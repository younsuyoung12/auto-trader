# events/commentary_queue.py
"""
========================================================
FILE: events/commentary_queue.py
AUTO-TRADER - Commentary Queue
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

목적:
- GPT 해설 결과를 "최근 N개" 유지한다.
- YouTube 채팅/OBS 오버레이 전송 전에 중복/스팸을 강하게 제한한다.
- 데이터 없으면 즉시 예외 발생(폴백 금지).

역할:
- 저장/중복방지/레이트리밋만 담당한다.
- 실제 GPT 호출은 commentary_engine.py에서 수행한다.

절대 원칙:
- STRICT · NO-FALLBACK
- 필수 item 필드 누락 시 즉시 예외
- queue state / dedup state / send rate state 는 thread-safe 하게 관리한다
- 조용한 continue / 빈 문자열 허용 / 임의 기본값 주입 금지

변경 이력:
- 2026-03-13:
  1) FIX(EXCEPTION): common.exceptions_strict 공통 예외 계층 적용
  2) FEAT(CONTRACT): CommentaryItem.__post_init__ 추가로 필드 계약 검증 강화
  3) FIX(CONCURRENCY): queue / dedup / last_sent state 에 RLock 적용
  4) FIX(STRICT): dedup map pruning 및 text/event contract 검증 강화
  5) FIX(ARCH): global singleton 초기값을 명시 상수로 고정
========================================================
"""

from __future__ import annotations

import math
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, List, Optional, Tuple

from common.exceptions_strict import StrictDataError, StrictStateError


COMMENTARY_QUEUE_MAX_ITEMS: int = 50
COMMENTARY_QUEUE_MIN_INTERVAL_SEC: float = 30.0
COMMENTARY_QUEUE_DEDUP_WINDOW_SEC: float = 300.0


class CommentaryQueueContractError(StrictDataError):
    """commentary queue 입력/아이템 계약 위반."""


class CommentaryQueueStateError(StrictStateError):
    """commentary queue 내부 상태 위반."""


def _require_nonempty_str(v: Any, name: str) -> str:
    if not isinstance(v, str):
        raise CommentaryQueueContractError(f"{name} must be str (STRICT), got={type(v).__name__}")
    s = v.strip()
    if not s:
        raise CommentaryQueueContractError(f"{name} must not be empty (STRICT)")
    return s


def _require_positive_int(v: Any, name: str) -> int:
    if isinstance(v, bool):
        raise CommentaryQueueContractError(f"{name} must be int (STRICT), bool not allowed")
    try:
        iv = int(v)
    except Exception as exc:
        raise CommentaryQueueContractError(f"{name} must be int (STRICT): {exc}") from exc
    if iv <= 0:
        raise CommentaryQueueContractError(f"{name} must be > 0 (STRICT)")
    return iv


def _require_positive_float(v: Any, name: str) -> float:
    if isinstance(v, bool):
        raise CommentaryQueueContractError(f"{name} must be float (STRICT), bool not allowed")
    try:
        fv = float(v)
    except Exception as exc:
        raise CommentaryQueueContractError(f"{name} must be float (STRICT): {exc}") from exc
    if not math.isfinite(fv):
        raise CommentaryQueueContractError(f"{name} must be finite (STRICT)")
    if fv <= 0.0:
        raise CommentaryQueueContractError(f"{name} must be > 0 (STRICT)")
    return fv


def _require_dict(v: Any, name: str) -> Dict[str, Any]:
    if not isinstance(v, dict):
        raise CommentaryQueueContractError(f"{name} must be dict (STRICT), got={type(v).__name__}")
    return dict(v)


@dataclass(frozen=True, slots=True)
class CommentaryItem:
    ts_ms: int
    event_id: str
    event_type: str
    symbol: str
    text: str
    meta: Dict[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "ts_ms", _require_positive_int(self.ts_ms, "CommentaryItem.ts_ms"))
        object.__setattr__(self, "event_id", _require_nonempty_str(self.event_id, "CommentaryItem.event_id"))
        object.__setattr__(self, "event_type", _require_nonempty_str(self.event_type, "CommentaryItem.event_type"))
        object.__setattr__(self, "symbol", _require_nonempty_str(self.symbol, "CommentaryItem.symbol"))
        object.__setattr__(self, "text", _require_nonempty_str(self.text, "CommentaryItem.text"))
        object.__setattr__(self, "meta", _require_dict(self.meta, "CommentaryItem.meta"))


class CommentaryQueue:
    """In-memory queue with strict dedup + rate limit."""

    def __init__(
        self,
        *,
        max_items: int,
        min_interval_sec: float,
        dedup_window_sec: float,
    ) -> None:
        self._max_items = _require_positive_int(max_items, "max_items")
        self._min_interval_sec = _require_positive_float(min_interval_sec, "min_interval_sec")
        self._dedup_window_sec = _require_positive_float(dedup_window_sec, "dedup_window_sec")

        self._items: Deque[CommentaryItem] = deque(maxlen=self._max_items)
        self._last_sent_at: float = 0.0
        self._dedup_map: Dict[str, float] = {}
        self._lock = threading.RLock()

    def push(self, item: CommentaryItem) -> None:
        """Store commentary item. STRICT validation, no fallback."""
        if not isinstance(item, CommentaryItem):
            raise CommentaryQueueContractError(
                f"item must be CommentaryItem (STRICT), got={type(item).__name__}"
            )

        with self._lock:
            self._items.append(item)

    def list_items(self, limit: int = 50) -> List[CommentaryItem]:
        lim = _require_positive_int(limit, "limit")
        with self._lock:
            items = list(self._items)
        return items[-lim:]

    def latest(self) -> CommentaryItem:
        with self._lock:
            if not self._items:
                raise CommentaryQueueStateError("no commentary items (STRICT)")
            return self._items[-1]

    def can_send_now(self) -> bool:
        """Rate-limit gate. STRICT: no fallback."""
        now = time.time()
        with self._lock:
            if self._last_sent_at < 0:
                raise CommentaryQueueStateError("last_sent_at must be >= 0 (STRICT)")
            if self._last_sent_at == 0.0:
                return True
            return (now - self._last_sent_at) >= self._min_interval_sec

    def mark_sent(self) -> None:
        """Mark that we sent a message now."""
        now = time.time()
        with self._lock:
            self._last_sent_at = now

    def is_duplicate(self, text: str) -> bool:
        """Deduplicate commentary text within a time window."""
        t = _require_nonempty_str(text, "text")
        now = time.time()

        with self._lock:
            self._prune_dedup_map_locked(now)
            last = self._dedup_map.get(t)
            if last is None:
                return False
            if last <= 0.0 or not math.isfinite(last):
                raise CommentaryQueueStateError("dedup_map contains invalid timestamp (STRICT)")
            return (now - last) <= self._dedup_window_sec

    def mark_seen(self, text: str) -> None:
        t = _require_nonempty_str(text, "text")
        now = time.time()
        with self._lock:
            self._prune_dedup_map_locked(now)
            self._dedup_map[t] = now

    def make_send_decision(self, text: str) -> Tuple[bool, str]:
        """Return (ok_to_send, reason). STRICT: no fallback."""
        t = _require_nonempty_str(text, "text")

        if self.is_duplicate(t):
            return (False, "duplicate_within_window")

        if not self.can_send_now():
            return (False, "rate_limited")

        return (True, "ok")

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "max_items": self._max_items,
                "min_interval_sec": self._min_interval_sec,
                "dedup_window_sec": self._dedup_window_sec,
                "queued_items": len(self._items),
                "last_sent_at": self._last_sent_at,
                "dedup_items": len(self._dedup_map),
            }

    def _prune_dedup_map_locked(self, now: float) -> None:
        if not math.isfinite(now) or now <= 0.0:
            raise CommentaryQueueStateError("invalid current time for dedup prune (STRICT)")

        stale_keys: List[str] = []
        for key, ts in self._dedup_map.items():
            if not math.isfinite(ts) or ts <= 0.0:
                raise CommentaryQueueStateError("dedup_map contains invalid timestamp (STRICT)")
            if (now - ts) > self._dedup_window_sec:
                stale_keys.append(key)

        for key in stale_keys:
            del self._dedup_map[key]


# Global singleton (explicit policy constants, not runtime fallback)
GLOBAL_COMMENTARY_QUEUE = CommentaryQueue(
    max_items=COMMENTARY_QUEUE_MAX_ITEMS,
    min_interval_sec=COMMENTARY_QUEUE_MIN_INTERVAL_SEC,
    dedup_window_sec=COMMENTARY_QUEUE_DEDUP_WINDOW_SEC,
)


__all__ = [
    "CommentaryQueueContractError",
    "CommentaryQueueStateError",
    "CommentaryItem",
    "CommentaryQueue",
    "GLOBAL_COMMENTARY_QUEUE",
]