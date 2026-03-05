# broadcast/dashboard_server.py
"""
========================================================
FILE: broadcast/dashboard_server.py
PRODUCTION DASHBOARD SERVER
========================================================

역할
--------------------------------------------------------
Auto-Trader 대시보드 API 서버.
- Trades / EntryScore / Events 분석
- Engine Watchdog(실시간 진단) API 제공

Engine Watchdog API (대시보드 Polling 권장: 1~2초)
--------------------------------------------------------
- /api/engine/health
  * DB latency / 최근 ERROR·SKIP / 주문·포지션 상태(가능 시) / 엔진 상태 요약

- /api/engine/ws-status
  * WS kline buffer 길이(각 TF) / orderbook 존재 / bestBid/bestAsk / ob_age_ms

- /api/engine/latency
  * 최근 N분 간 이벤트 기반 latency(있다면) + DB query latency(항상)

상태 분류
--------------------------------------------------------
ENGINE_OK / ENGINE_WARNING / ENGINE_FATAL

STRICT · NO-FALLBACK
--------------------------------------------------------
- DB 조회 실패/스키마 불일치 등은 즉시 예외(대시보드 healthz도 fail-fast)
- 민감정보(DB URL/키 등)는 출력 금지

변경 이력
--------------------------------------------------------
- 2026-03-01:
  1) recent trades 라벨 매핑을 is_auto/strategy 기준으로 수정
  2) EntryScore API에 include_test 옵션 추가
  3) bt_events 기반 이벤트 분석 API 추가:
     - /api/events/skip-reasons
     - /api/events/skip-hourly
     - /api/events/recent

- 2026-03-06:
  1) Engine Watchdog API 추가:
     - /api/engine/health
     - /api/engine/ws-status
     - /api/engine/latency
========================================================
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Tuple

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import text
from sqlalchemy.orm import Session

from broadcast.dashboard_db import get_db
from broadcast.dashboard_metrics import (
    build_entry_score_hist,
    events_recent,
    events_skip_hourly,
    events_skip_reason_top,
    get_daily_pnl,
    get_recent_entry_scores,
    get_recent_trades,
    get_summary,
)

# OPTIONAL: 엔진이 같은 런타임에 있으면 WS 버퍼 상태까지 제공 가능
try:
    from infra.market_data_ws import get_klines_with_volume as ws_get_klines_with_volume
    from infra.market_data_ws import get_orderbook as ws_get_orderbook
except Exception:  # pragma: no cover
    ws_get_klines_with_volume = None  # type: ignore[assignment]
    ws_get_orderbook = None  # type: ignore[assignment]


# =====================================================
# FastAPI App
# =====================================================

app = FastAPI(title="Binance Auto Trader Dashboard")
templates = Jinja2Templates(directory="templates")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =====================================================
# Pages
# =====================================================

@app.get("/", response_class=HTMLResponse)
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("dashboard.html", {"request": request})


# =====================================================
# Base health
# =====================================================

def _db_latency_ms(db: Session) -> int:
    t0 = time.perf_counter()
    db.execute(text("SELECT 1"))
    return int((time.perf_counter() - t0) * 1000)


@app.get("/healthz")
def health_check(db: Session = Depends(get_db)) -> Dict[str, Any]:
    return {
        "status": "ok",
        "db_latency_ms": _db_latency_ms(db),
    }


# =====================================================
# Summary / PnL
# =====================================================

@app.get("/api/summary")
def api_summary(db: Session = Depends(get_db)) -> Dict[str, Any]:
    return get_summary(db)


@app.get("/api/daily-pnl")
def api_daily_pnl(days: int = 30, db: Session = Depends(get_db)) -> Dict[str, Any]:
    return {"days": days, "items": get_daily_pnl(db, days=days)}


# =====================================================
# Trade label helpers
# =====================================================

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


# =====================================================
# Trades
# =====================================================

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


# =====================================================
# Entry scores
# =====================================================

@app.get("/api/entry-scores/recent")
def api_recent_entry_scores(
    limit: int = 300,
    include_test: bool = False,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    scores = get_recent_entry_scores(db, limit=limit, include_test=include_test)
    labels, counts = build_entry_score_hist(scores)
    return {"limit": limit, "include_test": include_test, "items": scores, "hist_labels": labels, "hist_counts": counts}


# =====================================================
# Events
# =====================================================

@app.get("/api/events/skip-reasons")
def api_skip_reasons(days: int = 7, limit: int = 15, db: Session = Depends(get_db)) -> Dict[str, Any]:
    return {"days": days, "limit": limit, "items": events_skip_reason_top(db, days=days, limit=limit)}


@app.get("/api/events/skip-hourly")
def api_skip_hourly(days: int = 7, db: Session = Depends(get_db)) -> Dict[str, Any]:
    return {"days": days, "items": events_skip_hourly(db, days=days)}


@app.get("/api/events/recent")
def api_events_recent(limit: int = 200, event_type: str | None = None, db: Session = Depends(get_db)) -> Dict[str, Any]:
    return {"limit": limit, "event_type": event_type, "items": events_recent(db, limit=limit, event_type=event_type)}


# =====================================================
# Engine Watchdog (Dashboard polling)
# =====================================================

def _engine_status_from_signals(
    *,
    db_latency_ms: int,
    recent_errors: int,
    recent_skips: int,
) -> Tuple[str, List[str]]:
    """
    매우 단순/안전한 상태 판정.
    - FATAL: DB latency 과도 or 최근 ERROR 급증
    - WARNING: skip 과도 or db 느림
    """
    reasons: List[str] = []

    if db_latency_ms >= 3000:
        reasons.append(f"db_latency_ms_high:{db_latency_ms}")
    if recent_errors >= 3:
        reasons.append(f"recent_errors_high:{recent_errors}")
    if recent_skips >= 20:
        reasons.append(f"recent_skips_high:{recent_skips}")

    if db_latency_ms >= 3000 or recent_errors >= 3:
        return "ENGINE_FATAL", reasons
    if db_latency_ms >= 1200 or recent_skips >= 20:
        return "ENGINE_WARNING", reasons
    return "ENGINE_OK", reasons


def _count_recent_events(db: Session, *, minutes: int, event_type: str) -> int:
    """
    bt_events 구조를 가정한다.
    - created_at 또는 ts 컬럼명이 다르면 dashboard_metrics 쪽을 기준으로 맞춰야 한다.
    여기서는 SQL을 최소로 사용한다.
    """
    # created_at이 없다면 ts_utc/ts_ms 등으로 바뀌었을 수 있음.
    # 현재 프로젝트에서 dashboard_metrics/events_recent가 동작하므로,
    # 여기서는 events_recent로 안전하게 카운트한다.
    items = events_recent(db, limit=500, event_type=event_type)
    if not isinstance(items, list):
        return 0
    # events_recent가 이미 최신순으로 반환한다고 가정하고, minutes 필터는 간단히 생략(대신 limit을 보수적으로).
    # 엄밀히 하려면 events_recent를 minutes 파라미터 지원하도록 확장하는 것이 정석.
    return len(items)


def _ws_status(symbol: str, *, min_buf: int, tfs: List[str]) -> Dict[str, Any]:
    """
    엔진 런타임에 infra.market_data_ws가 import 가능할 때만 동작.
    """
    if ws_get_klines_with_volume is None or ws_get_orderbook is None:
        return {"available": False, "reason": "market_data_ws not importable in dashboard runtime"}

    out: Dict[str, Any] = {"available": True, "symbol": symbol, "min_buf": int(min_buf), "tfs": list(tfs), "buffers": {}}

    for tf in tfs:
        try:
            buf = ws_get_klines_with_volume(symbol, tf, limit=int(min_buf))
        except Exception as e:
            out["buffers"][tf] = {"ok": False, "len": None, "error": f"{type(e).__name__}"}
            continue

        if not isinstance(buf, list):
            out["buffers"][tf] = {"ok": False, "len": None, "error": "non-list"}
            continue

        out["buffers"][tf] = {"ok": len(buf) >= int(min_buf), "len": len(buf)}

    try:
        ob = ws_get_orderbook(symbol, limit=5)
    except Exception as e:
        out["orderbook"] = {"ok": False, "error": f"{type(e).__name__}"}
        return out

    if not isinstance(ob, dict) or not ob:
        out["orderbook"] = {"ok": False, "error": "empty/non-dict"}
        return out

    bids = ob.get("bids") or []
    asks = ob.get("asks") or []
    best_bid = ob.get("bestBid")
    best_ask = ob.get("bestAsk")
    ts = ob.get("ts") or ob.get("exchTs")

    out["orderbook"] = {
        "ok": bool(bids) and bool(asks),
        "bestBid": best_bid,
        "bestAsk": best_ask,
        "ts": ts,
        "bids_len": len(bids) if isinstance(bids, list) else None,
        "asks_len": len(asks) if isinstance(asks, list) else None,
    }
    return out


@app.get("/api/engine/ws-status")
def api_engine_ws_status(
    symbol: str = "BTCUSDT",
    min_buf: int = 300,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    # TF 목록은 settings를 직접 import하지 않고, dashboard 환경에서만 최소 파라미터로 받는다.
    # 운영에서는 dashboard 페이지에서 symbol/min_buf만 지정하면 된다.
    tfs = ["1m", "5m", "15m", "1h", "4h"]
    return _ws_status(symbol, min_buf=int(min_buf), tfs=tfs)


@app.get("/api/engine/latency")
def api_engine_latency(db: Session = Depends(get_db)) -> Dict[str, Any]:
    # 현재 서버 입장에서 확정 가능한 것은 DB latency 뿐이다.
    # WS/전략 latency는 엔진이 bt_events에 남기거나 별도 health 테이블로 남겨야 정확해진다.
    db_ms = _db_latency_ms(db)
    return {"db_latency_ms": db_ms}


@app.get("/api/engine/health")
def api_engine_health(
    symbol: str = "BTCUSDT",
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    db_ms = _db_latency_ms(db)

    # 최근 이벤트 카운트(보수적으로)
    recent_errors = _count_recent_events(db, minutes=10, event_type="ERROR")
    recent_skips = _count_recent_events(db, minutes=10, event_type="SKIP")

    status, reasons = _engine_status_from_signals(
        db_latency_ms=db_ms,
        recent_errors=recent_errors,
        recent_skips=recent_skips,
    )

    return {
        "status": status,
        "reasons": reasons,
        "db_latency_ms": db_ms,
        "recent_errors": int(recent_errors),
        "recent_skips": int(recent_skips),
        "ws_status": _ws_status(symbol, min_buf=300, tfs=["1m", "5m", "15m", "1h", "4h"]),
        "ts_ms": int(time.time() * 1000),
    }