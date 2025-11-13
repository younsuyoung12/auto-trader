# init_db_once.py
"""
BingX Auto Trader - DB 테이블 1회 생성 스크립트

Render Background Worker에서 한 번만 실행해서
db_models.py에 정의된 테이블들을 전부 생성한다.
"""

from db_core import Base, engine  # TRADER_DB_URL 로 연결된 engine 사용
import db_models  # noqa: F401  # 모델들을 Base에 등록하기 위해 import 만 해둔다.


def init_db() -> None:
    """모든 ORM 모델에 대해 CREATE TABLE 실행."""
    print("[INIT_DB] Creating all tables...", flush=True)
    Base.metadata.create_all(bind=engine)
    print("[INIT_DB] Done.", flush=True)


if __name__ == "__main__":
    init_db()
