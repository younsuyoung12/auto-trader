"""
========================================================
FILE: state/db_core.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

핵심 변경 요약
- DB URL 해석 책임을 settings.py 로 이동
- state/db_core.py 내부 os.getenv / os.environ / .env 재로딩 제거
- 세션/커밋 계약을 strict하게 유지
- 상단 변경 이력 최근 2일 기준으로 정리

코드 정리 내용
- DB URL 직접 env 접근 제거
- settings 모듈 기반 DB URL 해석 단일화
- 세션 팩토리/엔진 생성부 정리
- 설명 주석 정리

설계 원칙:
- Render Postgres를 단일 DB 소스로 사용한다.
- DB URL 원천은 settings.py 만 사용한다.
- sqlite 등 다른 DB 및 폴백을 허용하지 않는다.
- import 시점에 스키마 변경(create_all)을 수행하지 않는다.
- DB 오류는 반드시 예외로 전파한다.
- 민감정보(DB URL 포함)는 로그/출력 금지.

변경 이력
--------------------------------------------------------
- 2026-03-15
  1) FIX(SSOT): os.getenv / os.environ / .env 직접 재로딩 제거
  2) FIX(ARCH): DB URL 해석 책임을 settings.py 로 단일화
  3) FIX(CONTRACT): settings.SETTINGS.trader_db_url 우선, 미정의 시 settings._as_str("TRADER_DB_URL") bridge 사용
  4) CLEANUP: DB URL 정규화 / 엔진 생성 경로 정리
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

from contextlib import contextmanager
from importlib import import_module
from typing import Any, Iterator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

_POOL_SIZE = 5
_MAX_OVERFLOW = 5
_POOL_TIMEOUT_SEC = 30
_POOL_RECYCLE_SEC = 1800


def _normalize_postgres_db_url_or_raise(raw_url: str) -> str:
    url = str(raw_url).strip()
    if url == "":
        raise RuntimeError("DB URL is empty (STRICT)")

    url_lower = url.lower()

    if url_lower.startswith("sqlite:"):
        raise RuntimeError("sqlite is not allowed. Render Postgres only (STRICT)")

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
            "DB URL must be Postgres URL (STRICT). "
            "allowed schemes: postgresql://, postgresql+psycopg2://, postgresql+psycopg://"
        )

    return url


def _resolve_db_url_from_settings_or_raise() -> str:
    """
    DB URL은 settings.py 를 통해서만 읽는다.

    우선순위:
    1) settings.SETTINGS.trader_db_url
    2) settings._as_str('TRADER_DB_URL', '')  ← settings.py 내부 bridge
       (현재 Settings dataclass에 trader_db_url 필드가 없을 수 있으므로,
        db_core.py에서 직접 env 접근하지 않고 settings.py를 통해서만 읽는다.)
    """
    try:
        settings_mod = import_module("settings")
    except Exception as e:
        raise RuntimeError(f"settings import failed (STRICT): {type(e).__name__}: {e}") from e

    if not hasattr(settings_mod, "SETTINGS"):
        raise RuntimeError("settings.SETTINGS missing (STRICT)")

    settings_obj = getattr(settings_mod, "SETTINGS")
    if settings_obj is None:
        raise RuntimeError("settings.SETTINGS is None (STRICT)")

    # 1) explicit settings field
    if hasattr(settings_obj, "trader_db_url"):
        value = getattr(settings_obj, "trader_db_url")
        if value is None:
            raise RuntimeError("settings.trader_db_url is None (STRICT)")
        normalized = _normalize_postgres_db_url_or_raise(str(value))
        return normalized

    # 2) bridge via settings.py helper only
    helper = getattr(settings_mod, "_as_str", None)
    if helper is None or not callable(helper):
        raise RuntimeError(
            "settings.trader_db_url missing and settings._as_str unavailable (STRICT)"
        )

    try:
        raw = helper("TRADER_DB_URL", "")
    except Exception as e:
        raise RuntimeError(f"settings._as_str(TRADER_DB_URL) failed (STRICT): {e}") from e

    if not isinstance(raw, str) or raw.strip() == "":
        raise RuntimeError(
            "TRADER_DB_URL is not configured in settings.py source path (STRICT)"
        )

    return _normalize_postgres_db_url_or_raise(raw)


# 공개 상수는 항상 정규화된 URL만 노출한다.
DB_URL: str = _resolve_db_url_from_settings_or_raise()

engine: Engine = create_engine(
    DB_URL,
    pool_size=_POOL_SIZE,
    max_overflow=_MAX_OVERFLOW,
    pool_timeout=_POOL_TIMEOUT_SEC,
    pool_recycle=_POOL_RECYCLE_SEC,
    pool_pre_ping=True,
    connect_args={
        "application_name": "auto_trader",
    },
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
    session: Any = SessionLocal()
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