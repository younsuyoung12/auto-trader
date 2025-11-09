# drive_uploader.py
"""
구글 드라이브에 오늘 csv 같은 파일을 올리는 모듈 (OAuth 사용자 계정 버전).

이전 버전과 다른 점
- 더 이상 서비스계정 JSON을 안 쓴다.
- 대신 oauth_init.py 로 받아온 token.json 내용을
  환경변수 GOOGLE_OAUTH_TOKEN_JSON 에 넣어두고 그걸 읽는다.
- 이 토큰은 네 구글 계정으로 발급받은 거라 개인 드라이브에
  그대로 만들고/덮어쓸 수 있다. (권한만 체크하면 403 안 남)

필수 환경변수
- GOOGLE_OAUTH_TOKEN_JSON : oauth_init.py 로 생성한 token.json 전체 문자열
- GOOGLE_DRIVE_FOLDER_ID   : 업로드할 드라이브 폴더 ID

동작 순서
1. 토큰과 폴더 ID 를 환경변수에서 읽는다.
2. 폴더 안에 같은 이름의 파일이 있는지 먼저 찾는다.
3. 있으면 update()로 덮어쓴다.
4. 없으면 create()로 새로 만든다.
"""

from __future__ import annotations

import os
import json
from typing import Optional

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

# 우리가 권한 요청했던 스코프 그대로 써줍니다.
_SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.file",
]


def _get_service():
    """환경변수에서 토큰을 읽어 Drive 서비스 객체를 만든다."""
    token_json_str = os.getenv("GOOGLE_OAUTH_TOKEN_JSON")
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")

    if not token_json_str:
        raise RuntimeError("GOOGLE_OAUTH_TOKEN_JSON 이 비어있습니다. (token.json 내용 붙여넣기)")
    if not folder_id:
        raise RuntimeError("GOOGLE_DRIVE_FOLDER_ID 이 비어있습니다.")

    token_info = json.loads(token_json_str)
    # token.json 안에는 access_token, refresh_token, client_id, client_secret 이 다 들어있어서
    # 아래 한 줄이면 Credentials 가 복원된다.
    creds = Credentials.from_authorized_user_info(token_info, scopes=_SCOPES)

    service = build("drive", "v3", credentials=creds)
    return service, folder_id


def _find_file_id(service, folder_id: str, filename: str) -> Optional[str]:
    """폴더 안에서 같은 이름의 파일이 있는지 찾아서 ID를 리턴."""
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
    local_path 에 있는 파일을 remote_filename 이라는 이름으로 올린다.
    같은 이름이 있으면 덮어쓰고, 없으면 새로 만든다.
    """
    if not os.path.exists(local_path):
        raise FileNotFoundError(local_path)

    service, folder_id = _get_service()

    media = MediaFileUpload(local_path, resumable=True)

    # 폴더에 같은 이름 있는지 먼저 확인
    file_id = _find_file_id(service, folder_id, remote_filename)

    try:
        if file_id:
            # 이미 있으니 덮어쓰기
            service.files().update(fileId=file_id, media_body=media).execute()
            print(f"[DRIVE] updated existing file: {remote_filename}")
        else:
            # 없으면 새로 만들기
            file_metadata = {
                "name": remote_filename,
                "parents": [folder_id],
            }
            service.files().create(
                body=file_metadata,
                media_body=media,
                fields="id",
            ).execute()
            print(f"[DRIVE] created new file: {remote_filename}")

    except HttpError as e:
        # 실패해도 봇은 돌아야 하니까 여기서만 잡고 끝
        print(f"[DRIVE] upload failed: {e}")
