# broadcast/dashboard_server.py
# ====================================================
# Dashboard Server (Trades + Events)
# ====================================================
# 변경 이력
# ----------------------------------------------------
# - 2026-03-01:
#   1) recent trades 라벨 매핑을 is_auto/strategy 기준으로 수정
#   2) EntryScore API에 include_test 옵션 추가
#   3) bt_events 기반 이벤트 분석 API 추가:
#      - /api/events/skip-reasons
#      - /api/events/skip-hourly
#      - /api/events/recent
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
    events_skip_reason_top,
    events_skip_hourly,
    events_recent,
)

app = FastAPI(title="Binance Auto Trader Dashboard")
templates = Jinja2Templates(directory="templates")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", response_class=HTMLResponse)
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.get("/healthz")
def health_check(db: Session = Depends(get_db)) -> Dict[str, Any]:
    db.execute(text("SELECT 1"))
    return {"status": "ok"}


@app.get("/api/summary")
def api_summary(db: Session = Depends(get_db)) -> Dict[str, Any]:
    return get_summary(db)


@app.get("/api/daily-pnl")
def api_daily_pnl(days: int = 30, db: Session = Depends(get_db)) -> Dict[str, Any]:
    return {"days": days, "items": get_daily_pnl(db, days=days)}


def _map_trade_type(is_auto: bool) -> str:
    return "자동" if is_auto else "수동"


def _map_regime_label(strategy: str | None) -> str:
    if not strategy:
        return "기타"
    s = strategy.upper()
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
    if s in ("LONG", "BUY"):
        return "롱"
    if s in ("SHORT", "SELL"):
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
    if "manual" in r:
        return "수동 청산"
    return reason


@app.get("/api/trades/recent")
def api_recent_trades(limit: int = 50, db: Session = Depends(get_db)) -> Dict[str, Any]:
    trades: List[Dict[str, Any]] = get_recent_trades(db, limit=limit)

    enriched: List[Dict[str, Any]] = []
    for t in trades:
        pnl = float(t.get("pnl_usdt") or 0.0)
        is_auto = bool(t.get("is_auto"))
        strategy = t.get("strategy")
        side = t.get("side")
        close_reason = t.get("close_reason")

        item = dict(t)
        item.update(
            {
                "trade_type": _map_trade_type(is_auto),
                "regime_label": _map_regime_label(strategy),
                "side_label": _map_side_label(side),
                "close_reason_label": _map_close_reason_label(close_reason),
                "is_profit": pnl > 0,
                "is_loss": pnl < 0,
                "is_breakeven": pnl == 0,
            }
        )
        enriched.append(item)

    return {"limit": limit, "items": enriched}


@app.get("/api/entry-scores/recent")
def api_recent_entry_scores(limit: int = 300, include_test: bool = False, db: Session = Depends(get_db)) -> Dict[str, Any]:
    scores = get_recent_entry_scores(db, limit=limit, include_test=include_test)
    labels, counts = build_entry_score_hist(scores)
    return {"limit": limit, "include_test": include_test, "items": scores, "hist_labels": labels, "hist_counts": counts}


# -----------------------------
# bt_events 분석 API
# -----------------------------
@app.get("/api/events/skip-reasons")
def api_skip_reasons(days: int = 7, limit: int = 15, db: Session = Depends(get_db)) -> Dict[str, Any]:
    return {"days": days, "limit": limit, "items": events_skip_reason_top(db, days=days, limit=limit)}


@app.get("/api/events/skip-hourly")
def api_skip_hourly(days: int = 7, db: Session = Depends(get_db)) -> Dict[str, Any]:
    return {"days": days, "items": events_skip_hourly(db, days=days)}


@app.get("/api/events/recent")
def api_events_recent(limit: int = 200, event_type: str | None = None, db: Session = Depends(get_db)) -> Dict[str, Any]:
    return {"limit": limit, "event_type": event_type, "items": events_recent(db, limit=limit, event_type=event_type)}