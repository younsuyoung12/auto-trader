"""
========================================================
state/db_core.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
설계 원칙:
- Render Postgres를 단일 DB 소스로 사용한다.
- 접속 정보는 TRADER_DB_URL 환경변수 1개만 사용한다.
- sqlite 등 다른 DB 및 폴백을 허용하지 않는다.
- import 시점에 스키마 변경(create_all)을 수행하지 않는다.
- DB 오류는 반드시 예외로 전파한다.

변경 이력
--------------------------------------------------------
- 2026-03-01:
  1) 로컬 테스트 편의: python-dotenv로 .env 로딩 추가(override=False, env 우선)
  2) 기존 STRICT 정책(미설정 즉시 예외 / sqlite 금지 / Postgres 스킴만 허용) 유지
========================================================
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

# ✅ .env 로드 (환경변수 우선, 덮어쓰기 금지)
try:
    from dotenv import load_dotenv
except Exception as e:
    raise RuntimeError(f"python-dotenv import failed: {e}") from None

load_dotenv(override=False)

DB_URL = os.getenv("TRADER_DB_URL")

if not DB_URL or not str(DB_URL).strip():
    raise RuntimeError(
        "TRADER_DB_URL 환경변수가 설정되어 있지 않습니다. "
        ".env 또는 Render 환경변수에 TRADER_DB_URL을 설정하세요."
    )

_url = str(DB_URL).strip()
_url_l = _url.lower()

# sqlite 등 금지
if _url_l.startswith("sqlite:"):
    raise RuntimeError("TRADER_DB_URL이 sqlite 로 설정되어 있습니다. Render Postgres만 허용합니다.")

# SQLAlchemy 호환을 위해 postgres:// 는 postgresql:// 로 정규화한다.
# (폴백이 아니라 스킴 alias 정리)
if _url_l.startswith("postgres://"):
    _url = "postgresql://" + _url[len("postgres://") :]
    _url_l = _url.lower()

# Render Postgres URL 스킴 허용 범위
_ALLOWED_PREFIXES = (
    "postgresql://",
    "postgresql+psycopg2://",
    "postgresql+psycopg://",
)

if not _url_l.startswith(_ALLOWED_PREFIXES):
    raise RuntimeError(
        "TRADER_DB_URL이 Postgres URL 형식이 아닙니다. "
        "허용 스킴: postgresql://, postgresql+psycopg2://, postgresql+psycopg:// (postgres:// 는 자동 변환)"
    )

# SQLAlchemy Engine / Session (스키마 변경은 여기서 하지 않는다)
engine = create_engine(
    _url,
    pool_pre_ping=True,
    future=True,
)

SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    future=True,
)

Base = declarative_base()


@contextmanager
def get_session() -> Iterator[Session]:
    """DB 세션 컨텍스트 헬퍼.

    STRICT:
    - 예외 발생 시 rollback 후 예외를 다시 올린다.
    - 정상 종료 시 commit 한다.
    """
    session: Session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


__all__ = [
    "DB_URL",
    "engine",
    "SessionLocal",
    "Base",
    "get_session",
]