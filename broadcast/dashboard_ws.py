"""
========================================================
FILE: broadcast/dashboard_ws.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할
--------------------------------------------------------
- 기관급 대시보드용 WebSocket 허브.
- 서버 내부에서 발생한 실시간 이벤트를 브라우저로 push 한다.
- 지원 스트림:
  - engine_status
  - decision
  - trade
  - error
  - position
  - watchdog
- 브라우저 polling 없이 WebSocket 단일 채널로 전달한다.

절대 원칙 (STRICT · NO-FALLBACK)
--------------------------------------------------------
- 허용되지 않은 event_type 즉시 예외.
- payload는 dict 여야 하며, None/비정상 구조 즉시 예외.
- ts_ms는 양의 정수여야 하며, 비정상 값 즉시 예외.
- 연결 종료/전송 실패는 조용히 무시하지 않고 연결 상태를 정리한다.
- 민감정보(키/토큰/DB URL)는 절대 로그/메시지에 포함하지 않는다.

변경 이력
--------------------------------------------------------
- 2026-03-06:
  1) 신규 생성: Dashboard WebSocket Hub 추가
  2) 실시간 스트림(engine_status/decision/trade/error/position/watchdog) 지원
  3) 최신 이벤트 스냅샷 전송 및 클라이언트 연결 관리 추가
========================================================
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Final, List, Optional, Tuple

from fastapi import WebSocket
from starlette.websockets import WebSocketDisconnect, WebSocketState

logger = logging.getLogger(__name__)

_ALLOWED_EVENT_TYPES: Final[Tuple[str, ...]] = (
    "engine_status",
    "decision",
    "trade",
    "error",
    "position",
    "watchdog",
)

_ALLOWED_CLIENT_OPS: Final[Tuple[str, ...]] = (
    "ping",
    "snapshot",
)

_WS_ROUTE_PATH: Final[str] = "/ws/dashboard"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _require_positive_int(value: Any, name: str) -> int:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if isinstance(value, bool):
        raise RuntimeError(f"{name} must be int, bool is not allowed (STRICT)")
    try:
        ivalue = int(value)
    except Exception as exc:  # pragma: no cover - strict type guard
        raise RuntimeError(f"{name} must be int (STRICT): {exc}") from exc
    if ivalue <= 0:
        raise RuntimeError(f"{name} must be > 0 (STRICT)")
    return ivalue


def _require_dict(value: Any, name: str) -> Dict[str, Any]:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if not isinstance(value, dict):
        raise RuntimeError(f"{name} must be dict (STRICT), got={type(value).__name__}")
    return dict(value)


def _require_event_type(event_type: Any) -> str:
    if event_type is None:
        raise RuntimeError("event_type is required (STRICT)")
    if not isinstance(event_type, str):
        raise RuntimeError(f"event_type must be str (STRICT), got={type(event_type).__name__}")
    normalized = event_type.strip()
    if not normalized:
        raise RuntimeError("event_type must not be empty (STRICT)")
    if normalized not in _ALLOWED_EVENT_TYPES:
        raise RuntimeError(f"unsupported event_type (STRICT): {normalized!r}")
    return normalized


def _json_dumps_strict(data: Dict[str, Any]) -> str:
    try:
        return json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    except Exception as exc:
        raise RuntimeError(f"websocket payload json serialization failed (STRICT): {exc}") from exc


@dataclass(frozen=True, slots=True)
class DashboardEnvelope:
    seq: int
    type: str
    ts_ms: int
    payload: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "seq": int(self.seq),
            "type": self.type,
            "ts_ms": int(self.ts_ms),
            "payload": dict(self.payload),
        }


@dataclass(slots=True)
class _ClientConnection:
    client_id: str
    websocket: WebSocket
    connected_at_ms: int


class DashboardWebSocketHub:
    """
    대시보드용 단일 WebSocket 허브.
    - 서버 내부 publish() 호출로 모든 클라이언트에 브로드캐스트
    - 새 클라이언트 연결 시 최신 이벤트 스냅샷 전송
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._clients: Dict[str, _ClientConnection] = {}
        self._latest_by_type: Dict[str, DashboardEnvelope] = {}
        self._seq: int = 0

    @property
    def route_path(self) -> str:
        return _WS_ROUTE_PATH

    async def client_count(self) -> int:
        async with self._lock:
            return len(self._clients)

    async def connect(self, websocket: WebSocket) -> str:
        if websocket is None:
            raise RuntimeError("websocket is required (STRICT)")

        await websocket.accept()

        client_id = uuid.uuid4().hex
        conn = _ClientConnection(
            client_id=client_id,
            websocket=websocket,
            connected_at_ms=_now_ms(),
        )

        async with self._lock:
            if client_id in self._clients:
                raise RuntimeError(f"duplicate websocket client_id detected (STRICT): {client_id}")
            self._clients[client_id] = conn

        await self._send_system_message(
            websocket=websocket,
            event="connected",
            payload={
                "client_id": client_id,
                "route": self.route_path,
                "allowed_streams": list(_ALLOWED_EVENT_TYPES),
            },
        )
        await self.send_snapshot(client_id=client_id)

        logger.info("[DASHBOARD_WS] connected client_id=%s total=%d", client_id, await self.client_count())
        return client_id

    async def disconnect(self, client_id: str) -> None:
        if not isinstance(client_id, str) or not client_id.strip():
            raise RuntimeError("client_id must be non-empty str (STRICT)")

        conn: Optional[_ClientConnection]
        async with self._lock:
            conn = self._clients.pop(client_id, None)

        if conn is None:
            return

        ws = conn.websocket
        if ws.client_state != WebSocketState.DISCONNECTED:
            try:
                await ws.close()
            except RuntimeError:
                # 이미 닫힌 소켓 상태 정리
                pass

        logger.info("[DASHBOARD_WS] disconnected client_id=%s", client_id)

    async def publish(
        self,
        *,
        event_type: str,
        payload: Dict[str, Any],
        ts_ms: Optional[int] = None,
    ) -> DashboardEnvelope:
        normalized_type = _require_event_type(event_type)
        normalized_payload = _require_dict(payload, "payload")
        event_ts_ms = _require_positive_int(ts_ms if ts_ms is not None else _now_ms(), "ts_ms")

        envelope = DashboardEnvelope(
            seq=self._next_seq(),
            type=normalized_type,
            ts_ms=event_ts_ms,
            payload=normalized_payload,
        )

        async with self._lock:
            self._latest_by_type[normalized_type] = envelope
            clients = list(self._clients.values())

        if not clients:
            return envelope

        message = _json_dumps_strict(envelope.to_dict())
        failed_client_ids: List[str] = []

        for conn in clients:
            try:
                await conn.websocket.send_text(message)
            except (WebSocketDisconnect, RuntimeError, OSError) as exc:
                logger.warning(
                    "[DASHBOARD_WS] send failed client_id=%s event_type=%s error=%s",
                    conn.client_id,
                    normalized_type,
                    type(exc).__name__,
                )
                failed_client_ids.append(conn.client_id)

        if failed_client_ids:
            await self._remove_failed_clients(tuple(failed_client_ids))

        return envelope

    async def publish_engine_status(self, payload: Dict[str, Any], ts_ms: Optional[int] = None) -> DashboardEnvelope:
        return await self.publish(event_type="engine_status", payload=payload, ts_ms=ts_ms)

    async def publish_decision(self, payload: Dict[str, Any], ts_ms: Optional[int] = None) -> DashboardEnvelope:
        return await self.publish(event_type="decision", payload=payload, ts_ms=ts_ms)

    async def publish_trade(self, payload: Dict[str, Any], ts_ms: Optional[int] = None) -> DashboardEnvelope:
        return await self.publish(event_type="trade", payload=payload, ts_ms=ts_ms)

    async def publish_error(self, payload: Dict[str, Any], ts_ms: Optional[int] = None) -> DashboardEnvelope:
        return await self.publish(event_type="error", payload=payload, ts_ms=ts_ms)

    async def publish_position(self, payload: Dict[str, Any], ts_ms: Optional[int] = None) -> DashboardEnvelope:
        return await self.publish(event_type="position", payload=payload, ts_ms=ts_ms)

    async def publish_watchdog(self, payload: Dict[str, Any], ts_ms: Optional[int] = None) -> DashboardEnvelope:
        return await self.publish(event_type="watchdog", payload=payload, ts_ms=ts_ms)

    async def send_snapshot(self, *, client_id: str) -> None:
        if not isinstance(client_id, str) or not client_id.strip():
            raise RuntimeError("client_id must be non-empty str (STRICT)")

        async with self._lock:
            conn = self._clients.get(client_id)
            if conn is None:
                raise RuntimeError(f"unknown websocket client_id (STRICT): {client_id}")
            snapshots = [env.to_dict() for _, env in sorted(self._latest_by_type.items(), key=lambda x: x[0])]

        await self._send_system_message(
            websocket=conn.websocket,
            event="snapshot",
            payload={
                "items": snapshots,
                "count": len(snapshots),
            },
        )

    async def handle_client(self, websocket: WebSocket) -> None:
        client_id = await self.connect(websocket)
        try:
            while True:
                raw = await websocket.receive_text()
                message = self._parse_client_message(raw)

                op = message["op"]
                if op == "ping":
                    await self._send_system_message(
                        websocket=websocket,
                        event="pong",
                        payload={"ts_ms": _now_ms()},
                    )
                    continue

                if op == "snapshot":
                    await self.send_snapshot(client_id=client_id)
                    continue

                raise RuntimeError(f"unsupported client op (STRICT): {op!r}")

        except WebSocketDisconnect:
            logger.info("[DASHBOARD_WS] client disconnected by peer client_id=%s", client_id)
        finally:
            await self.disconnect(client_id)

    async def close_all(self) -> None:
        async with self._lock:
            client_ids = tuple(self._clients.keys())

        for client_id in client_ids:
            await self.disconnect(client_id)

    def latest_envelopes(self) -> List[Dict[str, Any]]:
        """
        서버 내부 디버깅/상태 점검용.
        동기 접근이 필요할 때만 사용한다.
        """
        return [env.to_dict() for _, env in sorted(self._latest_by_type.items(), key=lambda x: x[0])]

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    @staticmethod
    def _parse_client_message(raw: str) -> Dict[str, Any]:
        if raw is None:
            raise RuntimeError("client websocket message is required (STRICT)")
        if not isinstance(raw, str):
            raise RuntimeError(f"client websocket message must be str (STRICT), got={type(raw).__name__}")
        if not raw.strip():
            raise RuntimeError("client websocket message must not be empty (STRICT)")

        try:
            data = json.loads(raw)
        except Exception as exc:
            raise RuntimeError(f"client websocket message must be valid JSON (STRICT): {exc}") from exc

        if not isinstance(data, dict):
            raise RuntimeError("client websocket JSON root must be object (STRICT)")

        op = data.get("op")
        if not isinstance(op, str) or not op.strip():
            raise RuntimeError("client websocket op is required (STRICT)")

        normalized_op = op.strip()
        if normalized_op not in _ALLOWED_CLIENT_OPS:
            raise RuntimeError(f"unsupported client websocket op (STRICT): {normalized_op!r}")

        return {"op": normalized_op}

    async def _remove_failed_clients(self, client_ids: Tuple[str, ...]) -> None:
        if not client_ids:
            return

        removed: List[_ClientConnection] = []
        async with self._lock:
            for client_id in client_ids:
                conn = self._clients.pop(client_id, None)
                if conn is not None:
                    removed.append(conn)

        for conn in removed:
            ws = conn.websocket
            if ws.client_state != WebSocketState.DISCONNECTED:
                try:
                    await ws.close()
                except RuntimeError:
                    pass

            logger.info("[DASHBOARD_WS] removed failed client_id=%s", conn.client_id)

    @staticmethod
    async def _send_system_message(
        *,
        websocket: WebSocket,
        event: str,
        payload: Dict[str, Any],
    ) -> None:
        if websocket is None:
            raise RuntimeError("websocket is required (STRICT)")
        if not isinstance(event, str) or not event.strip():
            raise RuntimeError("system event must be non-empty str (STRICT)")
        normalized_payload = _require_dict(payload, "payload")

        message = {
            "type": "system",
            "event": event.strip(),
            "ts_ms": _now_ms(),
            "payload": normalized_payload,
        }
        await websocket.send_text(_json_dumps_strict(message))


_HUB: Optional[DashboardWebSocketHub] = None


def get_dashboard_ws_hub() -> DashboardWebSocketHub:
    global _HUB
    if _HUB is None:
        _HUB = DashboardWebSocketHub()
    return _HUB


async def dashboard_ws_endpoint(websocket: WebSocket) -> None:
    hub = get_dashboard_ws_hub()
    await hub.handle_client(websocket)


async def publish_dashboard_event(
    *,
    event_type: str,
    payload: Dict[str, Any],
    ts_ms: Optional[int] = None,
) -> DashboardEnvelope:
    hub = get_dashboard_ws_hub()
    return await hub.publish(event_type=event_type, payload=payload, ts_ms=ts_ms)


async def publish_engine_status(payload: Dict[str, Any], ts_ms: Optional[int] = None) -> DashboardEnvelope:
    hub = get_dashboard_ws_hub()
    return await hub.publish_engine_status(payload=payload, ts_ms=ts_ms)


async def publish_decision(payload: Dict[str, Any], ts_ms: Optional[int] = None) -> DashboardEnvelope:
    hub = get_dashboard_ws_hub()
    return await hub.publish_decision(payload=payload, ts_ms=ts_ms)


async def publish_trade(payload: Dict[str, Any], ts_ms: Optional[int] = None) -> DashboardEnvelope:
    hub = get_dashboard_ws_hub()
    return await hub.publish_trade(payload=payload, ts_ms=ts_ms)


async def publish_error(payload: Dict[str, Any], ts_ms: Optional[int] = None) -> DashboardEnvelope:
    hub = get_dashboard_ws_hub()
    return await hub.publish_error(payload=payload, ts_ms=ts_ms)


async def publish_position(payload: Dict[str, Any], ts_ms: Optional[int] = None) -> DashboardEnvelope:
    hub = get_dashboard_ws_hub()
    return await hub.publish_position(payload=payload, ts_ms=ts_ms)


async def publish_watchdog(payload: Dict[str, Any], ts_ms: Optional[int] = None) -> DashboardEnvelope:
    hub = get_dashboard_ws_hub()
    return await hub.publish_watchdog(payload=payload, ts_ms=ts_ms)


__all__ = [
    "DashboardEnvelope",
    "DashboardWebSocketHub",
    "dashboard_ws_endpoint",
    "get_dashboard_ws_hub",
    "publish_dashboard_event",
    "publish_engine_status",
    "publish_decision",
    "publish_trade",
    "publish_error",
    "publish_position",
    "publish_watchdog",
]