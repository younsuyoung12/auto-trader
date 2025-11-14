# dashboard_server.py
# ====================================================
# BingX Auto Trader - Dashboard Web API
# ----------------------------------------------------
# - FastAPI 기반 읽기 전용 대시보드 서버
# - DB 세션: dashboard_db.get_db()
# - 통계/지표: dashboard_metrics 모듈 사용
# - 폴백/예외 숨기기 없음: DB/쿼리 실패 시 500 그대로 노출
# ====================================================

from __future__ import annotations

from typing import Any, Dict, List

from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text

from dashboard_db import get_db
from dashboard_metrics import (
    get_summary,
    get_daily_pnl,
    get_recent_trades,
    get_recent_entry_scores,
    build_entry_score_hist,
)

app = FastAPI(title="BingX Auto Trader Dashboard API")

# CORS: 나중에 프런트엔드(React/Next 등) 붙이기 쉽게 전체 허용
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # 필요하면 도메인 제한하면 됨
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────────
# 헬스 체크
# ─────────────────────────────────────────────
@app.get("/healthz")
def health_check(db: Session = Depends(get_db)) -> Dict[str, Any]:
    # DB 연결 확인용 쿼리
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
    """
    최근 N일 일별 손익.
    - 기본 30일
    - 프런트에서는 이 데이터를 그래프로 그리면 됨.
    """
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
    # MANUAL 이 들어가면 수동, 나머지는 자동으로 본다.
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

    # 대표적인 패턴만 매핑. 나머지는 원문 그대로 노출해도 됨.
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

        item = dict(t)  # 원본 필드 유지
        item.update(
            {
                # 한글 레이블
                "trade_type": _map_source_label(source),          # 자동 / 수동
                "regime_label": _map_regime_label(source),       # 박스장 / 추세장 / 혼합 / 기타
                "side_label": _map_side_label(side),             # 롱 / 숏
                "close_reason_label": _map_close_reason_label(close_reason),
                # 편의용 플래그
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
    """
    최근 EntryScore 목록 + 점수 분포(히스토그램).
    - 점수 리스트: items
    - 히스토그램: hist_labels, hist_counts
    """
    scores = get_recent_entry_scores(db, limit=limit)
    labels, counts = build_entry_score_hist(scores)

    return {
        "limit": limit,
        "items": scores,
        "hist_labels": labels,
        "hist_counts": counts,
    }
