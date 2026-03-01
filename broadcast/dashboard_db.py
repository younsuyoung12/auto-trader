# broadcast/dashboard_db.py
# ====================================================
# Binance Auto Trader - Dashboard DB 세션 유틸
# ====================================================
# 변경 이력
# ----------------------------------------------------
# - 2026-03-01:
#   1) Render 안정성: pool_size/max_overflow/pool_recycle 옵션 추가
# ====================================================

from __future__ import annotations

import os
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

TRADER_DB_URL = os.getenv("TRADER_DB_URL")
if not TRADER_DB_URL:
    raise RuntimeError("TRADER_DB_URL 환경변수가 설정되어 있지 않습니다.")

pool_size = int(os.getenv("DB_POOL_SIZE", "5"))
max_overflow = int(os.getenv("DB_MAX_OVERFLOW", "10"))
pool_recycle = int(os.getenv("DB_POOL_RECYCLE_SEC", "1800"))

engine = create_engine(
    TRADER_DB_URL,
    pool_pre_ping=True,
    pool_size=pool_size,
    max_overflow=max_overflow,
    pool_recycle=pool_recycle,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()