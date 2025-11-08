"""
drive_uploader.py
구글 드라이브에 파일을 업로드하는 유틸 모듈.

전제 조건:
1) 서비스 계정 JSON을 환경변수 GOOGLE_SERVICE_ACCOUNT_JSON 으로 넣어둔다.
   - JSON 전체를 그대로 문자열로 넣는다. (Render 대시보드에 붙여넣기)
2) 업로드할 폴더의 ID를 GOOGLE_DRIVE_FOLDER_ID 로 넣어둔다.
   - 구글 드라이브에서 폴더 열면 URL이
     https://drive.google.com/drive/folders/XXXXX 이런 식인데
     그 XXXXX 부분이 폴더 ID다.
3) 그 폴더를 서비스 계정 이메일과 "편집자"로 공유해둔다.

이 모듈은 "업로드하는 기능"만 한다.
시그널을 실시간으로 기록하는 건 run_bot.py 같은 메인 코드에서
CSV 로 쓰고 나서 upload_to_drive(...) 를 호출해야 한다.
"""

from __future__ import annotations

import os
import json
from typing import Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# 환경변수에서 읽어온다.
# Render 대시보드에 그대로 넣어둔 값을 읽는 것.
SERVICE_ACCOUNT_JSON_STR = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")

# 매번 만들지 않도록 모듈 안에서 캐시
_DRIVE_SERVICE = None


def _get_drive_service():
    """
    구글 드라이브 service 객체 하나를 만들어서 재사용한다.
    필요한 환경변수가 없으면 바로 예외를 던진다.
    """
    global _DRIVE_SERVICE

    if _DRIVE_SERVICE is not None:
        return _DRIVE_SERVICE

    if not SERVICE_ACCOUNT_JSON_STR:
        # 여기서 예외가 나면 run_bot.py 에서 잡아서 텔레그램 보내도 됨
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON 가 설정돼 있지 않습니다.")
    if not DRIVE_FOLDER_ID:
        raise RuntimeError("GOOGLE_DRIVE_FOLDER_ID 가 설정돼 있지 않습니다.")

    # 환경변수에 JSON 전체를 문자열로 넣었으니 파싱
    info = json.loads(SERVICE_ACCOUNT_JSON_STR)

    # 드라이브에 파일 업로드할 수 있는 scope
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/drive.file"],
    )

    # v3 드라이브 클라이언트 생성
    _DRIVE_SERVICE = build("drive", "v3", credentials=creds)
    return _DRIVE_SERVICE


def upload_to_drive(local_path: str, target_name: Optional[str] = None) -> str:
    """
    로컬에 있는 파일을 지정된 구글 드라이브 폴더에 업로드한다.

    :param local_path: 업로드할 로컬 파일 경로 (예: "signals.csv")
    :param target_name: 드라이브에 보일 이름 (None이면 로컬 파일명과 동일)
    :return: 업로드된 파일의 드라이브 파일 ID
    """
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"로컬 파일을 찾을 수 없습니다: {local_path}")

    service = _get_drive_service()

    if target_name is None:
        target_name = os.path.basename(local_path)

    # 어떤 폴더에, 어떤 이름으로 올릴지 메타데이터 작성
    file_metadata = {
        "name": target_name,
        "parents": [DRIVE_FOLDER_ID],
    }

    # 실제 파일 본문
    media = MediaFileUpload(local_path, resumable=True)

    # 파일 생성
    created = (
        service.files()
        .create(body=file_metadata, media_body=media, fields="id")
        .execute()
    )

    file_id = created.get("id")
    return file_id
