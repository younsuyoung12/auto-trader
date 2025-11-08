# drive_uploader.py
"""
구글 드라이브에 파일을 업로드(정확히는 '덮어쓰기')하는 모듈.

동작 방식
1. 환경변수에서 서비스계정 JSON, 폴더 ID를 읽는다.
2. 폴더 안에 같은 이름의 파일이 있는지 먼저 찾는다.
3. 있으면 그 파일을 update()로 '덮어쓴다' → 개인 드라이브에서도 에러 안 남.
4. 없으면 create()도 시도는 해보지만, 개인 드라이브 + 서비스계정이면 403 날 수 있으니
   그땐 그냥 로그만 찍고 지나간다.

필수 환경변수
- GOOGLE_SERVICE_ACCOUNT_JSON : 서비스 계정 JSON 문자열
- GOOGLE_DRIVE_FOLDER_ID      : 업로드할 드라이브 폴더 ID
"""

from __future__ import annotations

import os
import json
from typing import Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError


# 드라이브 스코프
_SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.file",
]


def _get_service():
    sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
    if not sa_json:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON 이 비어있습니다.")
    if not folder_id:
        raise RuntimeError("GOOGLE_DRIVE_FOLDER_ID 이 비어있습니다.")

    info = json.loads(sa_json)
    creds = service_account.Credentials.from_service_account_info(info, scopes=_SCOPES)
    service = build("drive", "v3", credentials=creds)
    return service, folder_id


def _find_file_id(service, folder_id: str, filename: str) -> Optional[str]:
    """
    폴더 안에서 이름이 filename 인 파일의 ID를 찾아서 돌려준다.
    없으면 None.
    """
    q = (
        f"'{folder_id}' in parents and name = '{filename}' and trashed = false"
    )
    resp = service.files().list(
        q=q,
        spaces="drive",
        fields="files(id, name)",
        pageSize=1,
    ).execute()
    files = resp.get("files", [])
    if files:
        return files[0]["id"]
    return None


def upload_to_drive(local_path: str, remote_filename: str) -> None:
    """
    local_path 에 있는 파일을 구글 드라이브 폴더에 remote_filename 이름으로 덮어쓴다.
    - 같은 이름이 이미 있으면 update()
    - 없으면 create() 시도 (개인 드라이브 + 서비스계정이면 이건 403 날 수 있음)
    """
    if not os.path.exists(local_path):
        raise FileNotFoundError(local_path)

    service, folder_id = _get_service()

    media = MediaFileUpload(local_path, resumable=True)

    # 1) 같은 이름 파일이 있는지 먼저 본다
    file_id = _find_file_id(service, folder_id, remote_filename)

    try:
        if file_id:
            # 이미 있으니까 덮어쓰기
            service.files().update(
                fileId=file_id,
                media_body=media,
            ).execute()
            print(f"[DRIVE] updated existing file: {remote_filename}")
        else:
            # 없으면 새로 만들기 (이건 개인드라이브+서비스계정이면 403 날 수 있음)
            file_metadata = {
                "name": remote_filename,
                "parents": [folder_id],
            }
            service.files().create(
                body=file_metadata,
                media_body=media,
                fields="id",
            ).execute()
            print(f"[DRIVE] created new file (first time): {remote_filename}")

    except HttpError as e:
        # 403 같이 오는 경우 여기서만 잡고, 봇은 계속 돌게 한다.
        print(f"[DRIVE] upload failed: {e}")
