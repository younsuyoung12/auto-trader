# integrations/youtube_chat_sender.py
# =============================================================================
# AUTO-TRADER - YouTube Live Chat Sender (STRICT / NO-FALLBACK)
# -----------------------------------------------------------------------------
# 목적:
# - 이벤트 기반으로 생성된 해설(Commentary)을 YouTube Live Chat에 자동 전송
#
# 중요:
# - 이 모듈은 "전송"만 담당한다.
# - 인증/토큰 파일(token.json) 또는 OAuth 설정이 없으면 즉시 예외(폴백 금지)
#
# 구현:
# - google-api-python-client 사용 (requirements.txt에 필요)
# - 환경변수:
#     YOUTUBE_CLIENT_SECRETS=credentials.json (기본값)
#     YOUTUBE_TOKEN_FILE=token.json (기본값)
#     YOUTUBE_LIVE_CHAT_ID=... (선택; 없으면 active live broadcast에서 조회)
#
# STRICT:
# - 필요한 파일/값이 없으면 즉시 예외
# - API 실패 시 예외 그대로 전파 -> Render 로그에 에러 표시
# =============================================================================

from __future__ import annotations

import os
from typing import Any, Dict, Optional

from googleapiclient.discovery import build  # type: ignore
from googleapiclient.errors import HttpError  # type: ignore
from google.oauth2.credentials import Credentials  # type: ignore


class YouTubeChatError(RuntimeError):
    """Raised when YouTube chat sending fails (strict)."""


def _require_file(path: str) -> str:
    p = str(path or "").strip()
    if not p:
        raise YouTubeChatError("file path empty")
    if not os.path.exists(p):
        raise YouTubeChatError(f"required file missing: {p}")
    return p


def _load_credentials_from_token(token_file: str) -> Credentials:
    tf = _require_file(token_file)
    try:
        creds = Credentials.from_authorized_user_file(tf, scopes=["https://www.googleapis.com/auth/youtube.force-ssl"])
    except Exception as e:
        raise YouTubeChatError(f"token load failed: {e.__class__.__name__}") from e
    if not creds or not creds.valid:
        # STRICT: do not attempt refresh here (could be done, but that is a form of fallback flow)
        raise YouTubeChatError("token invalid/expired (refresh not attempted in strict mode)")
    return creds


def _build_youtube_client() -> Any:
    token_file = os.getenv("YOUTUBE_TOKEN_FILE", "").strip() or "token.json"
    creds = _load_credentials_from_token(token_file)
    try:
        yt = build("youtube", "v3", credentials=creds)
    except Exception as e:
        raise YouTubeChatError(f"youtube client build failed: {e.__class__.__name__}") from e
    return yt


def _get_live_chat_id(yt: Any) -> str:
    """Get liveChatId.

    STRICT:
    - If env YOUTUBE_LIVE_CHAT_ID is set, use it.
    - Otherwise, attempt to resolve from active live broadcast.
    - If none found, raise.
    """
    env_id = os.getenv("YOUTUBE_LIVE_CHAT_ID", "").strip()
    if env_id:
        return env_id

    try:
        resp = yt.liveBroadcasts().list(part="snippet", broadcastStatus="active", mine=True).execute()
    except HttpError as e:
        raise YouTubeChatError(f"liveBroadcasts.list failed: {e}") from e
    except Exception as e:
        raise YouTubeChatError(f"liveBroadcasts.list failed: {e.__class__.__name__}") from e

    items = resp.get("items") if isinstance(resp, dict) else None
    if not items or not isinstance(items, list):
        raise YouTubeChatError("no active live broadcasts found (cannot resolve liveChatId)")

    snippet = items[0].get("snippet") if isinstance(items[0], dict) else None
    if not isinstance(snippet, dict):
        raise YouTubeChatError("live broadcast snippet missing")

    chat_id = snippet.get("liveChatId")
    if not chat_id or not isinstance(chat_id, str):
        raise YouTubeChatError("liveChatId missing in active broadcast")
    return chat_id


def send_chat_message(text: str) -> Dict[str, Any]:
    """Send a message to YouTube live chat. STRICT: no fallback."""
    msg = str(text or "").strip()
    if not msg:
        raise YouTubeChatError("text is empty")

    yt = _build_youtube_client()
    live_chat_id = _get_live_chat_id(yt)

    body = {
        "snippet": {
            "liveChatId": live_chat_id,
            "type": "textMessageEvent",
            "textMessageDetails": {"messageText": msg},
        }
    }

    try:
        resp = yt.liveChatMessages().insert(part="snippet", body=body).execute()
    except HttpError as e:
        raise YouTubeChatError(f"liveChatMessages.insert failed: {e}") from e
    except Exception as e:
        raise YouTubeChatError(f"liveChatMessages.insert failed: {e.__class__.__name__}") from e

    if not isinstance(resp, dict):
        raise YouTubeChatError("unexpected response type from insert")
    return resp


__all__ = [
    "YouTubeChatError",
    "send_chat_message",
]