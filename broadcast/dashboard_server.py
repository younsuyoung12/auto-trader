# dashboard_server.py
# ====================================================
# BingX Auto Trader - Dashboard Web App + API
# ----------------------------------------------------
# - FastAPI 기반 읽기 전용 대시보드 서버
# - JSON API + 반응형 HTML 대시보드 페이지 제공
# ====================================================

from __future__ import annotations

from typing import Any, Dict, List

from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import text

from broadcast.dashboard_db import get_db
from broadcast.dashboard_metrics import (
    get_summary,
    get_daily_pnl,
    get_recent_trades,
    get_recent_entry_scores,
    build_entry_score_hist,
)

app = FastAPI(title="BingX Auto Trader Dashboard")

# 템플릿 설정 (프로젝트 루트에 templates/ 디렉토리)
templates = Jinja2Templates(directory="templates")

# CORS: 추후 외부 프론트엔드에서 API만 따로 쓸 수도 있으니 열어 둠
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────────
# HTML 대시보드 페이지
# ─────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard_page(request: Request) -> HTMLResponse:
    """
    메인 대시보드 페이지 (반응형, 요약 카드 + 그래프 + 거래 테이블).
    실제 데이터는 JS에서 /api/... 를 호출해서 채운다.
    """
    return templates.TemplateResponse("dashboard.html", {"request": request})


# ─────────────────────────────────────────────
# 헬스 체크
# ─────────────────────────────────────────────
@app.get("/healthz")
def health_check(db: Session = Depends(get_db)) -> Dict[str, Any]:
    db.execute(text("SELECT 1"))
    return {"status": "ok"}


# ─────────────────────────────────────────────
# 요약 카드용 API
# ─────────────────────────────────────────────
@app.get("/api/summary")
def api_summary(db: Session = Depends(get_db)) -> Dict[str, Any]:
    """
    상단 요약 카드용:
    - 총 거래 횟수
    - 이긴 거래 / 진 거래 / 본전
    - 총 수익(USDT)
    - 승률(%)
    - 거래당 평균 수익(USDT)
    """
    summary = get_summary(db)
    return summary


# ─────────────────────────────────────────────
# 일별 손익 그래프용 API
# ─────────────────────────────────────────────
@app.get("/api/daily-pnl")
def api_daily_pnl(days: int = 30, db: Session = Depends(get_db)) -> Dict[str, Any]:
    items = get_daily_pnl(db, days=days)
    return {
        "days": days,
        "items": items,
    }


# ─────────────────────────────────────────────
# 최근 트레이드 리스트
# ─────────────────────────────────────────────
def _map_source_label(source: str | None) -> str:
    if not source:
        return "알 수 없음"
    s = source.upper()
    if "MANUAL" in s:
        return "수동"
    return "자동"


def _map_regime_label(source: str | None) -> str:
    if not source:
        return "기타"
    s = source.upper()
    if "RANGE" in s:
        return "박스장"
    if "TREND" in s:
        return "추세장"
    if "HYBRID" in s:
        return "혼합"
    return "기타"


def _map_side_label(side: str | None) -> str:
    if not side:
        return ""
    s = side.upper()
    if s == "LONG":
        return "롱"
    if s == "SHORT":
        return "숏"
    return s


def _map_close_reason_label(reason: str | None) -> str:
    if not reason:
        return "기타"

    r = reason.lower()

    if "tp" in r and "early" not in r:
        return "익절"
    if "sl" in r and "early" not in r:
        return "손절"
    if "early" in r and "tp" in r:
        return "조기 익절"
    if "early" in r and "sl" in r:
        return "조기 손절"
    if "reverse" in r or "signal" in r:
        return "반대 신호 청산"
    if "manual" in r:
        return "수동 청산"

    return reason


@app.get("/api/trades/recent")
def api_recent_trades(
    limit: int = 50,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    최근 트레이드 리스트.
    - 한 행 = 한 거래
    - 프런트에서 테이블로 보여주기 좋게 한글 레이블을 추가해서 반환.
    """
    trades: List[Dict[str, Any]] = get_recent_trades(db, limit=limit)

    enriched: List[Dict[str, Any]] = []
    for t in trades:
        pnl = float(t.get("pnl_usdt") or 0.0)
        source = t.get("source")
        side = t.get("side")
        close_reason = t.get("close_reason")

        item = dict(t)
        item.update(
            {
                "trade_type": _map_source_label(source),        # 자동 / 수동
                "regime_label": _map_regime_label(source),     # 박스장 / 추세장 / 혼합 / 기타
                "side_label": _map_side_label(side),           # 롱 / 숏
                "close_reason_label": _map_close_reason_label(close_reason),
                "is_profit": pnl > 0,
                "is_loss": pnl < 0,
                "is_breakeven": pnl == 0,
            }
        )
        enriched.append(item)

    return {
        "limit": limit,
        "items": enriched,
    }


# ─────────────────────────────────────────────
# 최근 EntryScore + 히스토그램
# ─────────────────────────────────────────────
@app.get("/api/entry-scores/recent")
def api_recent_entry_scores(
    limit: int = 300,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    scores = get_recent_entry_scores(db, limit=limit)
    labels, counts = build_entry_score_hist(scores)

    return {
        "limit": limit,
        "items": scores,
        "hist_labels": labels,
        "hist_counts": counts,
    }
