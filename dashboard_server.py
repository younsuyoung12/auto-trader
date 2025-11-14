from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text

from dashboard_db import get_db  # 이미 있는 파일

app = FastAPI()


@app.get("/healthz")
def health_check(db: Session = Depends(get_db)):
    # DB 연결 확인용 쿼리
    db.execute(text("SELECT 1"))
    return {"status": "ok"}
