# db_core.py
# BingX Auto Trader - Postgres 공용 연결 모듈

import os
import sys
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# 🔍 여기서 바로 환경변수 있는지 찍어본다
print(
    "[DEBUG] TRADER_DB_URL in env?:",
    bool(os.getenv("TRADER_DB_URL")),
    file=sys.stderr,
)

# ─────────────────────────────
# 환경변수에서 DB URL 읽기
# ─────────────────────────────
DB_URL = os.getenv("TRADER_DB_URL")

if not DB_URL:
    raise RuntimeError(
        "TRADER_DB_URL 환경변수가 설정되어 있지 않습니다. "
        "Render Background Worker 환경설정에서 Internal Database URL을 TRADER_DB_URL로 등록하세요."
    )
