# api/broadcast_router.py
"""
========================================================
FILE: api/broadcast_router.py
AUTO-TRADER - Broadcast API
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

목적:
- Render에서 엔진 상태/이벤트/해설 텍스트를 외부(OBS 브라우저 소스 등)에 제공한다.

정책(STRICT):
- 데이터가 없으면 200으로 빈값 반환하지 않는다.
- 필수 commentary가 없으면 즉시 예외(500)로 올린다.
- 폴백 금지
- 응답 payload는 strict contract를 따른다.

구현:
- FastAPI Router 형태로 제공한다.
- 상위 앱(main/app)에서 include_router 하여 사용한다.
========================================================
"""

from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter

from common.exceptions_strict import StrictDataError, StrictStateError
from events.commentary_queue import CommentaryItem, GLOBAL_COMMENTARY_QUEUE

router = APIRouter(prefix="/broadcast", tags=["broadcast"])


class BroadcastRouterContractError(StrictDataError):
    """Broadcast API 입력/응답 계약 위반."""


class BroadcastRouterStateError(StrictStateError):
    """Broadcast API queue 상태 위반."""


def _require_positive_int(v: Any, name: str) -> int:
    if isinstance(v, bool):
        raise BroadcastRouterContractError(f"{name} must be int (STRICT), bool not allowed")
    try:
        iv = int(v)
    except Exception as exc:
        raise BroadcastRouterContractError(f"{name} must be int (STRICT): {exc}") from exc
    if iv <= 0:
        raise BroadcastRouterContractError(f"{name} must be > 0 (STRICT)")
    return iv


def _require_nonempty_str(v: Any, name: str) -> str:
    if not isinstance(v, str):
        raise BroadcastRouterContractError(f"{name} must be str (STRICT), got={type(v).__name__}")
    s = v.strip()
    if not s:
        raise BroadcastRouterContractError(f"{name} must not be empty (STRICT)")
    return s


def _require_dict(v: Any, name: str) -> Dict[str, Any]:
    if not isinstance(v, dict):
        raise BroadcastRouterContractError(f"{name} must be dict (STRICT), got={type(v).__name__}")
    return dict(v)


def _require_commentary_item(v: Any, name: str) -> CommentaryItem:
    if not isinstance(v, CommentaryItem):
        raise BroadcastRouterStateError(f"{name} must be CommentaryItem (STRICT), got={type(v).__name__}")
    return v


def _serialize_commentary_item(item: CommentaryItem) -> Dict[str, Any]:
    it = _require_commentary_item(item, "commentary_item")
    ts_ms = _require_positive_int(it.ts_ms, "commentary_item.ts_ms")
    event_id = _require_nonempty_str(it.event_id, "commentary_item.event_id")
    event_type = _require_nonempty_str(it.event_type, "commentary_item.event_type")
    symbol = _require_nonempty_str(it.symbol, "commentary_item.symbol")
    text = _require_nonempty_str(it.text, "commentary_item.text")
    meta = _require_dict(it.meta, "commentary_item.meta")

    return {
        "ts_ms": ts_ms,
        "event_id": event_id,
        "event_type": event_type,
        "symbol": symbol,
        "text": text,
        "meta": meta,
    }


@router.get("/health")
def health() -> Dict[str, Any]:
    return {"ok": True}


@router.get("/commentary/latest")
def latest_commentary() -> Dict[str, Any]:
    item = GLOBAL_COMMENTARY_QUEUE.latest()
    return _serialize_commentary_item(item)


@router.get("/commentary")
def list_commentary(limit: int = 20) -> Dict[str, Any]:
    lim = _require_positive_int(limit, "limit")
    if lim > 200:
        raise BroadcastRouterContractError("limit must be <= 200 (STRICT)")

    items = GLOBAL_COMMENTARY_QUEUE.list_items(limit=lim)
    if not isinstance(items, list):
        raise BroadcastRouterStateError("GLOBAL_COMMENTARY_QUEUE.list_items must return list (STRICT)")
    if not items:
        raise BroadcastRouterStateError("commentary_items is empty (STRICT)")

    serialized: List[Dict[str, Any]] = [_serialize_commentary_item(it) for it in items]
    return {
        "count": len(serialized),
        "items": serialized,
    }


@router.get("/overlay")
def overlay_payload() -> Dict[str, Any]:
    """
    OBS 오버레이용 최소 페이로드.

    STRICT:
    - commentary가 없으면 500
    """
    item = GLOBAL_COMMENTARY_QUEUE.latest()
    data = _serialize_commentary_item(item)

    return {
        "ok": True,
        "ts_ms": data["ts_ms"],
        "symbol": data["symbol"],
        "text": data["text"],
    }


__all__ = [
    "BroadcastRouterContractError",
    "BroadcastRouterStateError",
    "router",
]