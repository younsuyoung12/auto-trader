from __future__ import annotations

import json
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow

# Drive 업로드에 쓰는 권한 범위
SCOPES = ["https://www.googleapis.com/auth/drive.file"]


def main() -> None:
    """
    기존 만료된 토큰을 refresh 하지 말고,
    브라우저에서 다시 로그인해서 새로운 token.json 을 발급받는 스크립트.
    """
    cred_file = Path("credentials.json")
    if not cred_file.exists():
        raise FileNotFoundError(
            "credentials.json 파일이 없습니다. "
            "GCP 콘솔에서 OAuth 클라이언트 비밀키를 내려받아 "
            "프로젝트 루트에 credentials.json 이름으로 둬야 합니다."
        )

    flow = InstalledAppFlow.from_client_secrets_file(
        str(cred_file),
        scopes=SCOPES,
    )

    # 브라우저 열려서 구글 계정 로그인/허용
    creds = flow.run_local_server(port=0)

    # 1) token.json 파일로 저장
    token_path = Path("token.json")
    token_json = creds.to_json()

    token_path.write_text(token_json, encoding="utf-8")
    print(f"[OK] 새 token.json 저장 완료 → {token_path}")

    # 2) Render ENV 에 넣기 쉽게 그대로 출력도 해 준다
    print("\n===== 아래 JSON 전체를 Render 환경 변수에 붙여넣으면 됨 =====\n")
    print(token_json)


if __name__ == "__main__":
    main()
