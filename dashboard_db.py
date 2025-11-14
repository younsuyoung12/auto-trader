# dashboard_db.py
# ====================================================
# BingX Auto Trader - Dashboard DB 세션 유틸
# ----------------------------------------------------
# - TRADER_DB_URL 환경변수로 연결
# - 실패/오타/권한 문제 발생 시 폴백 없이 바로 예외 발생
#   → 대시보드 전체가 500을 내고, Render 로그에 에러가 남도록 설계
# ====================================================

from __future__ import annotations

import os
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session


TRADER_DB_URL = os.getenv("TRADER_DB_URL")

if not TRADER_DB_URL:
    # 폴백 금지: DB URL 이 없으면 아예 서비스 시작을 막는다.
    raise RuntimeError(
        "TRADER_DB_URL 환경변수가 설정되어 있지 않습니다. "
        "대시보드 서버를 실행하기 전에 Render Web Service 환경설정에 "
        "Internal Database URL(TRADER_DB_URL)을 등록하세요."
    )

# 읽기 전용이지만, SQLAlchemy 쪽에는 일반 엔진으로 붙는다.
engine = create_engine(
    TRADER_DB_URL,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
