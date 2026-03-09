"""
========================================================
FILE: state/db_core.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

핵심 변경 요약
- DB_URL을 정규화된 Postgres URL로 단일화
- 세션/커밋 계약을 더 명확하게 정리
- 상단 변경 이력 최근 2일 기준으로 정리

코드 정리 내용
- 원시 env 값과 정규화 URL 혼용 제거
- 세션 팩토리/엔진 생성부 정리
- 설명 주석 정리

설계 원칙:
- Render Postgres를 단일 DB 소스로 사용한다.
- 접속 정보는 TRADER_DB_URL 환경변수 1개만 사용한다.
- sqlite 등 다른 DB 및 폴백을 허용하지 않는다.
- import 시점에 스키마 변경(create_all)을 수행하지 않는다.
- DB 오류는 반드시 예외로 전파한다.
- 민감정보(DB URL 포함)는 로그/출력 금지.

변경 이력
--------------------------------------------------------
- 2026-03-09
  1) FIX(STRICT): DB_URL을 정규화된 Postgres URL로 단일화
  2) FIX(CONTRACT): get_session 세션/커밋 계약을 명확화
  3) CLEANUP: 원시 env 값과 정규화 URL 혼용 제거
  4) CLEANUP: 변경 이력 최근 2일 기준 정리

- 2026-03-08
  1) SQLAlchemy pool 설정 명시화
  2) .env 옵션 로딩 / Postgres 스킴 정규화 / sqlite 금지 유지
========================================================
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

_POOL_SIZE = 5
_MAX_OVERFLOW = 5
_POOL_TIMEOUT_SEC = 30
_POOL_RECYCLE_SEC = 1800


def _load_dotenv_trader_db_url_if_needed() -> None:
    """옵션: 로컬 .env에서 TRADER_DB_URL만 로드한다.

    STRICT:
    - 이미 환경변수에 TRADER_DB_URL이 있으면 덮어쓰지 않는다.
    - .env가 없으면 아무것도 하지 않는다.
    - 파싱 실패/형식 오류는 즉시 예외로 올린다.
    """
    if os.getenv("TRADER_DB_URL"):
        return

    env_path = Path(".env")
    if not env_path.is_file():
        return

    text = env_path.read_text(encoding="utf-8")
    found: Optional[str] = None

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if line.startswith("export "):
            line = line[len("export ") :].strip()

        if "=" not in line:
            raise RuntimeError("Invalid .env line (missing '=')")

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if not key:
            raise RuntimeError("Invalid .env line (empty key)")

        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]

        if key == "TRADER_DB_URL":
            normalized = value.strip()
            if not normalized:
                raise RuntimeError("TRADER_DB_URL in .env is empty")
            found = normalized
            break

    if found is not None:
        os.environ["TRADER_DB_URL"] = found


def _require_normalized_postgres_db_url_or_raise() -> str:
    raw = os.getenv("TRADER_DB_URL")
    if raw is None or not str(raw).strip():
        raise RuntimeError(
            "TRADER_DB_URL 환경변수가 설정되어 있지 않습니다. "
            "환경변수 또는 로컬 .env에 TRADER_DB_URL을 설정하세요."
        )

    url = str(raw).strip()
    url_lower = url.lower()

    if url_lower.startswith("sqlite:"):
        raise RuntimeError("TRADER_DB_URL이 sqlite 로 설정되어 있습니다. Render Postgres만 허용합니다.")

    if url_lower.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://") :]
        url_lower = url.lower()

    allowed_prefixes = (
        "postgresql://",
        "postgresql+psycopg2://",
        "postgresql+psycopg://",
    )
    if not url_lower.startswith(allowed_prefixes):
        raise RuntimeError(
            "TRADER_DB_URL이 Postgres URL 형식이 아닙니다. "
            "허용 스킴: postgresql://, postgresql+psycopg2://, postgresql+psycopg:// "
            "(postgres:// 는 postgresql:// 로 자동 정규화)"
        )

    return url


# Optional local .env support (no external dependency)
_load_dotenv_trader_db_url_if_needed()

# 공개 상수는 항상 정규화된 URL만 노출한다.
DB_URL: str = _require_normalized_postgres_db_url_or_raise()

engine: Engine = create_engine(
    DB_URL,
    pool_size=_POOL_SIZE,
    max_overflow=_MAX_OVERFLOW,
    pool_timeout=_POOL_TIMEOUT_SEC,
    pool_recycle=_POOL_RECYCLE_SEC,
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
    - 정상 종료 시 commit 한다.
    - 예외 발생 시 rollback 후 예외를 다시 전파한다.
    - 세션은 항상 close 한다.
    """
    session = SessionLocal()
    if not isinstance(session, Session):
        raise RuntimeError("SessionLocal() must return sqlalchemy.orm.Session (STRICT)")

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