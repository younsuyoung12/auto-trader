"""
========================================================
FILE: broadcast/dashboard_db.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

핵심 변경 요약
- dashboard DB 세션 경로를 state/db_core.py 단일 경로로 통일
- os.getenv / create_engine / sessionmaker 직접 사용 제거
- 대시보드 DB 접근도 SSOT + 단일 DB 코어 규약에 맞게 정렬

코드 정리 내용
- 미사용 os / create_engine / sessionmaker 제거
- 별도 engine / pool 설정 중복 제거
- get_db 역할만 남기고 단일 세션 의존성으로 정리

변경 이력
--------------------------------------------------------
- 2026-03-09
  1) FIX(ROOT-CAUSE): dashboard 전용 별도 engine/session 생성 제거
  2) FIX(STRICT): DB 접근을 state/db_core.get_session 단일 경로로 통일
  3) CLEANUP: os.getenv / pool 옵션 / 중복 세션 팩토리 제거

- 2026-03-08
  1) Render 안정성 목적 pool 옵션 적용 이력 존재
========================================================
"""

from __future__ import annotations

from typing import Generator

from sqlalchemy.orm import Session

from state.db_core import get_session

import logging

logger = logging.getLogger(__name__)

def get_db() -> Generator[Session, None, None]:
    try:
        with get_session() as db:
            if not isinstance(db, Session):
                raise RuntimeError(
                    "state.db_core.get_session() must yield sqlalchemy.orm.Session (STRICT)"
                )
            yield db
    except Exception:
        logger.exception("dashboard_db.get_db session failure")
        raise


