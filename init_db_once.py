"""
init_db_once.py
====================================================
BingX Auto Trader - DB 테이블 1회 생성 스크립트.

Render Background Worker에서 한 번만 실행해서
db_models.py에 정의된 테이블들을 전부 생성한다.

2025-11-15 안내 보강
----------------------------------------------------
1) Render 쪽 start command 를 바꾸지 않고도 로컬에서 직접 실행할 수 있다는
   사용 예를 주석에 명시했다.
2) 기능 변경은 없고, init_db() 시그니처/동작은 그대로 유지한다.

로컬 실행 예 (PowerShell):
    $env:TRADER_DB_URL = "postgresql+psycopg2://user:pass@host:port/dbname"
    python init_db_once.py

Render Background Worker 에서는 TRADER_DB_URL 을 Internal Database URL 로
맞춰둔 뒤, 필요한 경우에만 이 스크립트를 실행하면 된다.
"""

from state.db_core import Base, engine  # TRADER_DB_URL 로 연결된 engine 사용
import state.db_models as db_models  # noqa: F401  # 모델들을 Base에 등록하기 위해 import 만 해둔다.


def init_db() -> None:
    """모든 ORM 모델에 대해 CREATE TABLE 실행.

    - Base.metadata.create_all(...) 을 호출하므로, db_models 에 정의된
      모든 테이블이 존재하지 않을 경우 생성된다.
    - 이미 존재하는 테이블에 대해서는 아무 작업도 하지 않는다.
    """

    print("[INIT_DB] Creating all tables...", flush=True)
    Base.metadata.create_all(bind=engine)
    print("[INIT_DB] Done.", flush=True)


if __name__ == "__main__":
    # 직접 실행된 경우에만 테이블 생성.
    init_db()
