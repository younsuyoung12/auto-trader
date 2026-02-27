# events/commentary_queue.py
# =============================================================================
# AUTO-TRADER - Commentary Queue (STRICT / NO-FALLBACK)
# -----------------------------------------------------------------------------
# 목적:
# - GPT 해설 결과를 "최근 N개" 유지
# - YouTube 채팅/OBS 오버레이 전송 전에 중복/스팸을 강하게 제한
# - 데이터 없으면 예외 발생(폴백 금지)
#
# 주의:
# - 이 모듈은 저장/중복방지/레이트리밋만 담당
# - 실제 GPT 호출은 commentary_engine.py에서 수행
# =============================================================================

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Deque, Dict, List, Optional, Tuple
from collections import deque


class CommentaryQueueError(RuntimeError):
    """Raised when strict queue requirements fail."""


@dataclass(frozen=True)
class CommentaryItem:
    ts_ms: int
    event_id: str
    event_type: str
    symbol: str
    text: str
    meta: Dict[str, Any]


class CommentaryQueue:
    """In-memory queue with strict dedup + rate limit."""

    def __init__(
        self,
        *,
        max_items: int = 50,
        min_interval_sec: float = 30.0,   # 최소 채팅 간격 (기본 30초)
        dedup_window_sec: float = 300.0,  # 동일 텍스트 중복 방지 윈도우 (기본 5분)
    ) -> None:
        if max_items <= 0:
            raise ValueError("max_items must be positive")
        if min_interval_sec <= 0:
            raise ValueError("min_interval_sec must be positive")
        if dedup_window_sec <= 0:
            raise ValueError("dedup_window_sec must be positive")

        self._max_items = int(max_items)
        self._min_interval_sec = float(min_interval_sec)
        self._dedup_window_sec = float(dedup_window_sec)

        self._items: Deque[CommentaryItem] = deque(maxlen=self._max_items)
        self._last_sent_at: float = 0.0  # epoch seconds

        # text -> last_seen_epoch_sec
        self._dedup_map: Dict[str, float] = {}

    def push(self, item: CommentaryItem) -> None:
        """Store commentary item. STRICT validation, no fallback."""
        if not isinstance(item, CommentaryItem):
            raise ValueError("item must be CommentaryItem")

        if not item.event_id or not item.event_type or not item.symbol:
            raise CommentaryQueueError("item missing required fields")

        if not item.text or not isinstance(item.text, str):
            raise CommentaryQueueError("item.text missing")

        self._items.append(item)

    def list_items(self, limit: int = 50) -> List[CommentaryItem]:
        if limit <= 0:
            raise ValueError("limit must be positive")
        items = list(self._items)
        return items[-limit:]

    def latest(self) -> CommentaryItem:
        if not self._items:
            raise CommentaryQueueError("no commentary items")
        return self._items[-1]

    def can_send_now(self) -> bool:
        """Rate-limit gate. STRICT: no fallback."""
        now = time.time()
        if self._last_sent_at <= 0:
            return True
        return (now - self._last_sent_at) >= self._min_interval_sec

    def mark_sent(self) -> None:
        """Mark that we sent a message now."""
        self._last_sent_at = time.time()

    def is_duplicate(self, text: str) -> bool:
        """Deduplicate commentary text within a time window."""
        t = str(text or "").strip()
        if not t:
            raise CommentaryQueueError("text is empty")

        now = time.time()
        last = self._dedup_map.get(t)
        if last is None:
            return False
        return (now - last) <= self._dedup_window_sec

    def mark_seen(self, text: str) -> None:
        t = str(text or "").strip()
        if not t:
            raise CommentaryQueueError("text is empty")
        self._dedup_map[t] = time.time()

    def make_send_decision(self, text: str) -> Tuple[bool, str]:
        """Return (ok_to_send, reason). STRICT: no fallback."""
        t = str(text or "").strip()
        if not t:
            raise CommentaryQueueError("text is empty")

        if self.is_duplicate(t):
            return (False, "duplicate_within_window")

        if not self.can_send_now():
            return (False, "rate_limited")

        return (True, "ok")


# Global singleton (simple)
GLOBAL_COMMENTARY_QUEUE = CommentaryQueue()


__all__ = [
    "CommentaryQueueError",
    "CommentaryItem",
    "CommentaryQueue",
    "GLOBAL_COMMENTARY_QUEUE",
]