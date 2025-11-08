"""
drive_uploader.py
구글 서비스 계정(JSON 문자열)과 폴더 ID를 환경변수로 받아서
지정된 구글 드라이브 폴더에 파일을 업로드하는 유틸 모듈.

환경변수:
- GOOGLE_SERVICE_ACCOUNT_JSON : 서비스 계정 JSON 전체 (한 줄로 넣은 것)
- GOOGLE_DRIVE_FOLDER_ID      : 업로드할 드라이브 폴더 ID

사용 예:
    from drive_uploader import upload_to_drive
    upload_to_drive("/tmp/bot.log", "bot.log")
"""

import os
import json
from typing import Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# 환경변수에서 서비스 계정/폴더 정보 읽기
SERVICE_ACCOUNT_JSON_STR = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")


def _get_drive_service():
    """
    구글 드라이브 API 클라이언트를 만든다.
    환경변수가 비어 있으면 예외를 던진다.
    """
    if not SERVICE_ACCOUNT_JSON_STR:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON 이 설정돼 있지 않습니다.")
    if not DRIVE_FOLDER_ID:
        raise RuntimeError("GOOGLE_DRIVE_FOLDER_ID 가 설정돼 있지 않습니다.")

    info = json.loads(SERVICE_ACCOUNT_JSON_STR)
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/drive.file"],
    )
    service = build("drive", "v3", credentials=creds)
    return service


def upload_to_drive(local_path: str, target_name: Optional[str] = None) -> str:
    """
    로컬 파일을 구글 드라이브 지정 폴더에 업로드한다.
    :param local_path: 업로드할 로컬 파일 경로
    :param target_name: 드라이브에 저장할 이름 (None이면 로컬 파일명 그대로)
    :return: 업로드된 파일의 구글 드라이브 파일 ID
    """
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"로컬 파일을 찾을 수 없습니다: {local_path}")

    service = _get_drive_service()

    if target_name is None:
        target_name = os.path.basename(local_path)

    file_metadata = {
        "name": target_name,
        "parents": [DRIVE_FOLDER_ID],
    }
    media = MediaFileUpload(local_path, resumable=True)

    created = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id",
    ).execute()

    return created.get("id")
