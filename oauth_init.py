# oauth_init.py
"""
한 번만 실행해서 사람 계정으로 구글 드라이브 권한을 받아 token.json 을 만드는 스크립트.
같은 폴더에 'credentials.json' 이 있어야 한다.
"""

from __future__ import annotations

import os.path
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# 우리가 쓸 드라이브 권한
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.file",
]

def main() -> None:
    creds = None

    # 이미 토큰이 있으면 재사용
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    # 없거나 만료됐으면 다시 인증
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # 여기서 브라우저 열려서 구글 로그인하고 허용 눌러주면 됨
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0)

        # 받아온 토큰 저장
        with open("token.json", "w", encoding="utf-8") as token:
            token.write(creds.to_json())

    print("✅ token.json 생성/갱신 완료")

if __name__ == "__main__":
    main()
