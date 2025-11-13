# drive_uploader.py
"""
구글 드라이브에 파일을 업로드(덮어쓰기)하는 모듈 - OAuth 토큰 버전

이 버전은 더 이상 서비스계정 JSON을 쓰지 않는다.
로컬에서 oauth_init.py로 얻은 token.json 내용을
환경변수 GOOGLE_OAUTH_TOKEN_JSON 에 그대로 담아두면
Render 같은 서버에서도 그 토큰으로 업로드할 수 있다.

필수 환경변수
- GOOGLE_OAUTH_TOKEN_JSON : token.json 내용 전체 (한 줄로 넣어도 됨)
- GOOGLE_DRIVE_FOLDER_ID   : 업로드할 드라이브 폴더 ID
"""

from __future__ import annotations

import os
import json
from typing import Optional

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

# 우리가 OAuth에서 요청했던 스코프 그대로
_SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.file",
]


def _get_service():
    """
    환경변수에서 토큰(JSON)과 폴더ID 읽어서 드라이브 서비스 객체를 만든다.
    """
    token_str = os.getenv("GOOGLE_OAUTH_TOKEN_JSON")
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")

    if not token_str:
        raise RuntimeError("GOOGLE_OAUTH_TOKEN_JSON 이 비어있습니다. token.json 내용을 넣어주세요.")
    if not folder_id:
        raise RuntimeError("GOOGLE_DRIVE_FOLDER_ID 가 비어있습니다.")

    # token.json 내용이 곧 authorized_user_info 형식이다.
    token_info = json.loads(token_str)

    # refresh_token, client_id, client_secret 이 들어있으면
    # Credentials 가 알아서 갱신도 해준다.
    creds = Credentials.from_authorized_user_info(token_info, scopes=_SCOPES)

    service = build("drive", "v3", credentials=creds)
    return service, folder_id


def _find_file_id(service, folder_id: str, filename: str) -> Optional[str]:
    """
    폴더 안에서 이름이 filename 인 파일 ID 찾아오기.
    없으면 None.
    """
    q = f"'{folder_id}' in parents and name = '{filename}' and trashed = false"
    resp = (
        service.files()
        .list(q=q, spaces="drive", fields="files(id, name)", pageSize=1)
        .execute()
    )
    files = resp.get("files", [])
    if files:
        return files[0]["id"]
    return None


def upload_to_drive(local_path: str, remote_filename: str) -> None:
    """
    local_path 파일을 해당 폴더에 remote_filename 으로 올린다.
    - 있으면 update()
    - 없으면 create()
    """
    if not os.path.exists(local_path):
        raise FileNotFoundError(local_path)

    service, folder_id = _get_service()

    media = MediaFileUpload(local_path, resumable=True)

    file_id = _find_file_id(service, folder_id, remote_filename)

    try:
        if file_id:
            # 기존 파일 덮어쓰기
            service.files().update(fileId=file_id, media_body=media).execute()
            print(f"[DRIVE] updated existing file: {remote_filename}")
        else:
            # 새로 만들기
            file_meta = {
                "name": remote_filename,
                "parents": [folder_id],
            }
            service.files().create(
                body=file_meta,
                media_body=media,
                fields="id",
            ).execute()
            print(f"[DRIVE] created new file: {remote_filename}")

    except HttpError as e:
        # 업로드 실패해도 봇이 죽으면 안 되니까 여기서만 잡는다.
        print(f"[DRIVE] upload failed: {e}")