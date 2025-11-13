# db_core.py
# BingX Auto Trader - Postgres 공용 연결 모듈
#
# - TRADER_DB_URL 환경변수에서 접속 정보 읽음
# - SQLAlchemy 엔진 / 세션팩토리 / Base 제공
# - 다른 모듈에서는 여기 것만 import 해서 사용

import os
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# ─────────────────────────────
# 환경변수에서 DB URL 읽기
# ─────────────────────────────
DB_URL = os.getenv("TRADER_DB_URL")

if not DB_URL:
    raise RuntimeError(
        "TRADER_DB_URL 환경변수가 설정되어 있지 않습니다. "
        "Render Background Worker 환경설정에서 Internal Database URL을 TRADER_DB_URL로 등록하세요."
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
    """
    DB 세션 컨텍스트 헬퍼.

    사용 예:
        from db_core import get_session

        with get_session() as session:
            session.query(...)
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


def init_db():
    """
    (옵션) 테이블 자동 생성용 초기화 함수.
    실제 실행은 별도 스크립트나 관리용 코드에서 호출하면 된다.
    """
    # 순환 import 피하려고 함수 내부에서 import
    import db_models  # noqa: F401  # 모델 정의만 필요

    Base.metadata.create_all(bind=engine)
