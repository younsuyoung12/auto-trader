"""
========================================================
FILE: integrations/youtube_chat_sender.py
AUTO-TRADER - YouTube Live Chat Sender
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

목적:
- 이벤트 기반으로 생성된 해설(Commentary)을 YouTube Live Chat에 자동 전송한다.

중요:
- 이 모듈은 "전송"만 담당한다.
- 인증/토큰 파일 또는 liveChatId 계약이 없으면 즉시 예외(폴백 금지).
- settings.py(SSOT) 외 환경변수 직접 접근 금지.

구현:
- google-api-python-client 사용
- settings(SSOT)에서 다음 계약을 사용한다.
  - youtube_token_file: 인증된 OAuth token json 파일 경로 (필수)
  - youtube_live_chat_id: 직접 지정된 liveChatId (선택)
  - request_timeout_sec: 외부 API 호출 timeout (필수)

STRICT:
- 필요한 파일/값이 없으면 즉시 예외
- API 실패 시 예외 그대로 전파
- 기본값/환경변수/조용한 우회 금지
========================================================
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

from google.oauth2.credentials import Credentials  # type: ignore
from googleapiclient.discovery import build  # type: ignore
from googleapiclient.errors import HttpError  # type: ignore

from common.exceptions_strict import (
    StrictConfigError,
    StrictDataError,
    StrictExternalError,
)
from settings import load_settings


class YouTubeChatConfigError(StrictConfigError):
    """YouTube chat sender 설정 계약 위반."""


class YouTubeChatContractError(StrictDataError):
    """YouTube chat sender 입력/응답 계약 위반."""


class YouTubeChatExternalError(StrictExternalError):
    """YouTube API 외부 호출 실패."""


def _require_nonempty_str(v: Any, name: str) -> str:
    if not isinstance(v, str):
        raise YouTubeChatContractError(f"{name} must be str (STRICT), got={type(v).__name__}")
    s = v.strip()
    if not s:
        raise YouTubeChatContractError(f"{name} must not be empty (STRICT)")
    return s


def _require_positive_int(v: Any, name: str) -> int:
    if isinstance(v, bool):
        raise YouTubeChatContractError(f"{name} must be int (STRICT), bool not allowed")
    try:
        iv = int(v)
    except Exception as exc:
        raise YouTubeChatContractError(f"{name} must be int (STRICT): {exc}") from exc
    if iv <= 0:
        raise YouTubeChatContractError(f"{name} must be > 0 (STRICT)")
    return iv


def _require_dict(v: Any, name: str) -> Dict[str, Any]:
    if not isinstance(v, dict):
        raise YouTubeChatContractError(f"{name} must be dict (STRICT), got={type(v).__name__}")
    return dict(v)


def _resolve_settings(settings: Optional[Any]) -> Any:
    st = settings if settings is not None else load_settings()
    if st is None:
        raise YouTubeChatConfigError("settings resolution failed (STRICT)")
    return st


def _require_setting_str(settings: Any, name: str) -> str:
    if not hasattr(settings, name):
        raise YouTubeChatConfigError(f"settings.{name} missing (STRICT)")
    return _require_nonempty_str(getattr(settings, name), f"settings.{name}")


def _require_setting_int(settings: Any, name: str) -> int:
    if not hasattr(settings, name):
        raise YouTubeChatConfigError(f"settings.{name} missing (STRICT)")
    return _require_positive_int(getattr(settings, name), f"settings.{name}")


def _optional_setting_str(settings: Any, name: str) -> Optional[str]:
    if not hasattr(settings, name):
        return None
    value = getattr(settings, name)
    if value is None:
        return None
    return _require_nonempty_str(value, f"settings.{name}")


def _require_file(path: str, name: str) -> str:
    p = _require_nonempty_str(path, name)
    if not os.path.exists(p):
        raise YouTubeChatConfigError(f"required file missing: {p}")
    if not os.path.isfile(p):
        raise YouTubeChatConfigError(f"path is not a file: {p}")
    return p


def _load_credentials_from_token(*, settings: Any) -> Credentials:
    token_file = _require_setting_str(settings, "youtube_token_file")
    tf = _require_file(token_file, "settings.youtube_token_file")
    try:
        creds = Credentials.from_authorized_user_file(
            tf,
            scopes=["https://www.googleapis.com/auth/youtube.force-ssl"],
        )
    except Exception as e:
        raise YouTubeChatConfigError(f"token load failed (STRICT): {type(e).__name__}") from e

    if creds is None:
        raise YouTubeChatConfigError("token credentials missing (STRICT)")
    if not getattr(creds, "valid", False):
        raise YouTubeChatConfigError("token invalid/expired (refresh not attempted in strict mode)")
    return creds


def _build_youtube_client(*, settings: Any) -> Any:
    creds = _load_credentials_from_token(settings=settings)
    try:
        yt = build("youtube", "v3", credentials=creds)
    except Exception as e:
        raise YouTubeChatExternalError(f"youtube client build failed: {type(e).__name__}") from e
    return yt


def _get_live_chat_id(*, yt: Any, settings: Any) -> str:
    """
    STRICT:
    - settings.youtube_live_chat_id 가 있으면 사용한다.
    - 없으면 active live broadcast에서 liveChatId를 조회한다.
    - 둘 다 안 되면 예외.
    """
    configured_chat_id = _optional_setting_str(settings, "youtube_live_chat_id")
    if configured_chat_id is not None:
        return configured_chat_id

    try:
        resp = yt.liveBroadcasts().list(
            part="snippet",
            broadcastStatus="active",
            mine=True,
        ).execute()
    except HttpError as e:
        raise YouTubeChatExternalError(f"liveBroadcasts.list failed: {e}") from e
    except Exception as e:
        raise YouTubeChatExternalError(f"liveBroadcasts.list failed: {type(e).__name__}") from e

    resp_dict = _require_dict(resp, "liveBroadcasts.list.response")
    items = resp_dict.get("items")
    if not isinstance(items, list) or not items:
        raise YouTubeChatExternalError("no active live broadcasts found (cannot resolve liveChatId)")

    first = items[0]
    if not isinstance(first, dict):
        raise YouTubeChatContractError("liveBroadcasts.list.items[0] must be dict (STRICT)")

    snippet = first.get("snippet")
    if not isinstance(snippet, dict):
        raise YouTubeChatContractError("live broadcast snippet missing (STRICT)")

    live_chat_id = snippet.get("liveChatId")
    return _require_nonempty_str(live_chat_id, "live broadcast snippet.liveChatId")


def send_chat_message(
    text: str,
    *,
    settings: Optional[Any] = None,
) -> Dict[str, Any]:
    """Send a message to YouTube live chat. STRICT: no fallback."""
    msg = _require_nonempty_str(text, "text")
    st = _resolve_settings(settings)

    yt = _build_youtube_client(settings=st)
    live_chat_id = _get_live_chat_id(yt=yt, settings=st)
    timeout_sec = _require_setting_int(st, "request_timeout_sec")
    if timeout_sec <= 0:
        raise YouTubeChatConfigError("settings.request_timeout_sec must be > 0 (STRICT)")

    body = {
        "snippet": {
            "liveChatId": live_chat_id,
            "type": "textMessageEvent",
            "textMessageDetails": {"messageText": msg},
        }
    }

    try:
        resp = yt.liveChatMessages().insert(part="snippet", body=body).execute(num_retries=0)
    except HttpError as e:
        raise YouTubeChatExternalError(f"liveChatMessages.insert failed: {e}") from e
    except Exception as e:
        raise YouTubeChatExternalError(f"liveChatMessages.insert failed: {type(e).__name__}") from e

    resp_dict = _require_dict(resp, "liveChatMessages.insert.response")
    if "id" not in resp_dict:
        raise YouTubeChatContractError("liveChatMessages.insert.response.id missing (STRICT)")
    _require_nonempty_str(resp_dict["id"], "liveChatMessages.insert.response.id")

    return resp_dict


__all__ = [
    "YouTubeChatConfigError",
    "YouTubeChatContractError",
    "YouTubeChatExternalError",
    "send_chat_message",
]