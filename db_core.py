# db_core.py
# BingX Auto Trader - Postgres 공용 연결 모듈
#
# - TRADER_DB_URL 환경변수에서 접속 정보 읽음
# - SQLAlchemy 엔진 / 세션팩토리 / Base 제공
# - 다른 모듈에서는 여기 것만 import 해서 사용
"""
PATCH NOTES — 2025-11-15
----------------------------------------------------
1) 모듈 상단에 변경 이력(PATCH NOTES) 추가.
2) __all__ 정의 추가로 외부에서 export 할 심벌 명시.
3) init_db() 주석 보강 (운영/로컬에서 어떻게 쓰는지 안내).

※ 기능적인 변경은 없고, 리팩터링/주석 정리 수준만 반영되어 있습니다.
"""

import os
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# ─────────────────────────────
# 환경변수에서 DB URL 읽기
# ─────────────────────────────
DB_URL = os.getenv("TRADER_DB_URL")

if not DB_URL:
    # Render Background Worker / 로컬 모두 공통으로 사용하는 경고 메시지
    raise RuntimeError(
        "TRADER_DB_URL 환경변수가 설정되어 있지 않습니다. "
        "Render Background Worker 환경설정 또는 로컬 환경변수에 "
        "Internal Database URL(또는 동일한 Postgres URL)을 TRADER_DB_URL로 등록하세요."
    )

# ─────────────────────────────
# SQLAlchemy 엔진 / 세션팩토리
# ─────────────────────────────
# 필요하면 ENV로 echo, pool_size 같은 옵션을 추가로 뺄 수 있다.
engine = create_engine(
    DB_URL,
    pool_pre_ping=True,   # 죽은 커넥션 자동 감지
    future=True,          # 최신 SQLAlchemy 스타일
)

SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    future=True,
)

# ─────────────────────────────
# Base 모델
# ─────────────────────────────
Base = declarative_base()


# ─────────────────────────────
# 세션 헬퍼
# ─────────────────────────────
@contextmanager
def get_session():
    """DB 세션 컨텍스트 헬퍼.

    사용 예:
        from db_core import get_session

        with get_session() as session:
            session.query(...)

    - with 블록이 끝날 때 자동으로 commit / rollback / close 처리.
    - 예외 발생 시 rollback 후 예외를 다시 올린다.
    """

    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db() -> None:
    """(옵션) 테이블 자동 생성용 초기화 함수.

    실제 실행은 별도 스크립트나 관리용 코드에서 호출하면 된다.

    예)
        # 로컬에서 직접 테이블 생성
        # (사전에 TRADER_DB_URL 환경변수만 맞게 세팅되어 있어야 함)
        from db_core import init_db
        init_db()

    Render 환경에서는 `init_db_once.py`를 통해 한 번만 실행하는 것을 추천.
    """

    # 순환 import 피하려고 함수 내부에서 import
    import db_models  # noqa: F401  # 모델 정의만 필요

    Base.metadata.create_all(bind=engine)


__all__ = [
    "DB_URL",
    "engine",
    "SessionLocal",
    "Base",
    "get_session",
    "init_db",
]
