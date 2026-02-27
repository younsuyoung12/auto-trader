# api/broadcast_router.py
# =============================================================================
# AUTO-TRADER - Broadcast API (STRICT / NO-FALLBACK)
# -----------------------------------------------------------------------------
# 목적:
# - Render에서 엔진 상태/이벤트/해설 텍스트를 외부(OBS 브라우저 소스 등)에 제공
#
# 정책(STRICT):
# - 데이터가 없으면 200으로 빈값 반환하지 않는다 -> 즉시 예외(500)로 Render 로그에 남긴다.
# - 폴백 금지
#
# 구현:
# - FastAPI Router 형태로 제공한다.
# - 상위 앱(main/app)에서 include_router 하여 사용:
#     from fastapi import FastAPI
#     from api.broadcast_router import router as broadcast_router
#     app = FastAPI()
#     app.include_router(broadcast_router)
# =============================================================================

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter

from events.commentary_queue import GLOBAL_COMMENTARY_QUEUE, CommentaryQueueError

router = APIRouter(prefix="/broadcast", tags=["broadcast"])


def _require_nonempty(v: Any, name: str) -> Any:
    if v is None:
        raise RuntimeError(f"{name} is None")
    if isinstance(v, (list, dict, str)) and len(v) == 0:  # type: ignore[arg-type]
        raise RuntimeError(f"{name} is empty")
    return v


@router.get("/health")
def health() -> Dict[str, Any]:
    # STRICT: just return ok if module loads
    return {"ok": True}


@router.get("/commentary/latest")
def latest_commentary() -> Dict[str, Any]:
    # STRICT: if none exists, raise -> Render logs show error
    item = GLOBAL_COMMENTARY_QUEUE.latest()
    return {
        "ts_ms": item.ts_ms,
        "event_id": item.event_id,
        "event_type": item.event_type,
        "symbol": item.symbol,
        "text": item.text,
        "meta": item.meta,
    }


@router.get("/commentary")
def list_commentary(limit: int = 20) -> Dict[str, Any]:
    items = GLOBAL_COMMENTARY_QUEUE.list_items(limit=limit)
    _require_nonempty(items, "commentary_items")
    return {
        "count": len(items),
        "items": [
            {
                "ts_ms": it.ts_ms,
                "event_id": it.event_id,
                "event_type": it.event_type,
                "symbol": it.symbol,
                "text": it.text,
                "meta": it.meta,
            }
            for it in items
        ],
    }


@router.get("/overlay")
def overlay_payload() -> Dict[str, Any]:
    """OBS 오버레이용 최소 페이로드.

    STRICT:
    - commentary가 없으면 500
    """
    item = GLOBAL_COMMENTARY_QUEUE.latest()
    return {
        "ok": True,
        "ts_ms": item.ts_ms,
        "symbol": item.symbol,
        "text": item.text,
    }


__all__ = ["router"]