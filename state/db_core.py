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
- 민감정보(DB URL 포함)는 로그/출력 금지.

변경 이력
--------------------------------------------------------
- 2026-03-01:
  1) 로컬 테스트 편의: python-dotenv로 .env 로딩 추가(override=False, env 우선)
  2) 기존 STRICT 정책(미설정 즉시 예외 / sqlite 금지 / Postgres 스킴만 허용) 유지

- 2026-03-03 (PATCH)
  1) python-dotenv 의존성 제거(외부 의존성/배포 리스크 제거)
  2) 내장 .env 로더 추가(옵션):
     - .env 파일이 존재할 때만 TRADER_DB_URL을 읽는다.
     - 이미 환경변수에 TRADER_DB_URL이 있으면 덮어쓰지 않는다(override 금지).
  3) STRICT 정책 유지:
     - TRADER_DB_URL 미설정 시 즉시 예외
     - sqlite 금지
     - Postgres 스킴만 허용(postgres:// 는 postgresql:// 로 정규화)

- 2026-03-06 (TRADE-GRADE)
  1) SQLAlchemy connection pool 명시화:
     - pool_size=5
     - max_overflow=5
     - pool_timeout=30
     - pool_recycle=1800
     - pool_pre_ping=True
  2) 운영 안정성 강화:
     - DB 연결 재사용을 통해 불필요한 connect/disconnect 빈도 완화
     - 죽은 연결 감지 및 재생성 안정화
========================================================
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker


def _load_dotenv_trader_db_url_if_needed() -> None:
    """옵션: 로컬 .env에서 TRADER_DB_URL만 로드한다.

    STRICT:
    - 이미 환경변수에 TRADER_DB_URL이 있으면 덮어쓰지 않는다.
    - .env가 없으면 아무것도 하지 않는다(폴백 아님, 단순 미사용).
    - 파싱 실패/형식 오류는 즉시 예외(조용히 무시 금지).
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

        # allow "export KEY=VALUE"
        if line.startswith("export "):
            line = line[len("export ") :].strip()

        if "=" not in line:
            raise RuntimeError("Invalid .env line (missing '=')")

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if not key:
            raise RuntimeError("Invalid .env line (empty key)")

        # strip optional quotes
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]

        if key == "TRADER_DB_URL":
            if not value.strip():
                raise RuntimeError("TRADER_DB_URL in .env is empty")
            found = value.strip()
            break

    if found is not None:
        os.environ["TRADER_DB_URL"] = found


# Optional local .env support (no external dependency)
_load_dotenv_trader_db_url_if_needed()

DB_URL = os.getenv("TRADER_DB_URL")

if not DB_URL or not str(DB_URL).strip():
    raise RuntimeError(
        "TRADER_DB_URL 환경변수가 설정되어 있지 않습니다. "
        "환경변수 또는 로컬 .env에 TRADER_DB_URL을 설정하세요."
    )

_url = str(DB_URL).strip()
_url_l = _url.lower()

# sqlite 등 금지
if _url_l.startswith("sqlite:"):
    raise RuntimeError("TRADER_DB_URL이 sqlite 로 설정되어 있습니다. Render Postgres만 허용합니다.")

# SQLAlchemy 호환: postgres:// → postgresql:// 정규화 (폴백 아님, alias 정리)
if _url_l.startswith("postgres://"):
    _url = "postgresql://" + _url[len("postgres://") :]
    _url_l = _url.lower()

_ALLOWED_PREFIXES = (
    "postgresql://",
    "postgresql+psycopg2://",
    "postgresql+psycopg://",
)

if not _url_l.startswith(_ALLOWED_PREFIXES):
    raise RuntimeError(
        "TRADER_DB_URL이 Postgres URL 형식이 아닙니다. "
        "허용 스킴: postgresql://, postgresql+psycopg2://, postgresql+psycopg:// "
        "(postgres:// 는 postgresql:// 로 자동 정규화)"
    )

# SQLAlchemy Engine / Session (스키마 변경은 여기서 하지 않는다)
# TRADE-GRADE: Render/AWS 운영 안정성을 위해 pool 설정을 명시한다.
engine = create_engine(
    _url,
    pool_size=5,
    max_overflow=5,
    pool_timeout=30,
    pool_recycle=1800,
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