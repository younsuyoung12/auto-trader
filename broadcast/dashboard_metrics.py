# broadcast/dashboard_metrics.py
# ====================================================
# Dashboard Metrics (Trades + Events)
# ====================================================
# 변경 이력
# ----------------------------------------------------
# - 2026-03-01:
#   1) v_trades_prod / v_trade_analytics 기반으로 지표 통일
#   2) bt_events 기반 SKIP 분석(사유 TOP, 시간대별, 최근 이벤트) 추가
# ====================================================

from __future__ import annotations

from datetime import datetime, date
from typing import Any, Dict, List, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session


def _to_iso(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, datetime):
        return x.isoformat()
    return str(x)


# -----------------------------
# 기존 대시보드 기본 지표
# -----------------------------
def get_daily_pnl(db: Session, days: int = 30) -> List[Dict[str, Any]]:
    sql = text(
        """
        SELECT
            (exit_ts AT TIME ZONE 'Asia/Seoul')::date AS trade_date,
            SUM(pnl_usdt) AS pnl_usdt,
            COUNT(*) AS trade_count
        FROM v_trades_prod
        WHERE exit_ts IS NOT NULL
          AND exit_ts >= now() - (:days * INTERVAL '1 day')
        GROUP BY trade_date
        ORDER BY trade_date
        """
    )
    rows = db.execute(sql, {"days": days}).mappings().all()
    out: List[Dict[str, Any]] = []
    for r in rows:
        d: date = r["trade_date"]
        out.append({"date": d.isoformat(), "pnl_usdt": float(r["pnl_usdt"] or 0.0), "trade_count": int(r["trade_count"] or 0)})
    return out


def get_summary(db: Session) -> Dict[str, Any]:
    sql = text(
        """
        SELECT
            COUNT(*) AS total_trades,
            SUM(CASE WHEN pnl_usdt > 0 THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN pnl_usdt < 0 THEN 1 ELSE 0 END) AS losses,
            SUM(CASE WHEN pnl_usdt = 0 THEN 1 ELSE 0 END) AS breakevens,
            SUM(pnl_usdt) AS total_pnl_usdt
        FROM v_trades_prod
        WHERE exit_ts IS NOT NULL
        """
    )
    r = db.execute(sql).mappings().one_or_none()
    total = int(r["total_trades"] or 0) if r else 0
    wins = int(r["wins"] or 0) if r else 0
    losses = int(r["losses"] or 0) if r else 0
    bes = int(r["breakevens"] or 0) if r else 0
    total_pnl = float(r["total_pnl_usdt"] or 0.0) if r else 0.0

    win_rate = (wins / total * 100.0) if total > 0 else 0.0
    avg_pnl = (total_pnl / total) if total > 0 else 0.0

    return {
        "total_trades": total,
        "wins": wins,
        "losses": losses,
        "breakevens": bes,
        "total_pnl_usdt": total_pnl,
        "win_rate_pct": win_rate,
        "avg_pnl_usdt": avg_pnl,
    }


def get_recent_trades(db: Session, limit: int = 50) -> List[Dict[str, Any]]:
    sql = text(
        """
        SELECT
            id, symbol, side, is_auto, strategy,
            entry_ts, exit_ts, entry_price, exit_price,
            pnl_usdt, close_reason
        FROM v_trades_prod
        WHERE exit_ts IS NOT NULL
        ORDER BY id DESC
        LIMIT :limit
        """
    )
    rows = db.execute(sql, {"limit": limit}).mappings().all()
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": int(r["id"]),
                "symbol": r["symbol"],
                "side": r["side"],
                "is_auto": bool(r["is_auto"]),
                "strategy": r["strategy"],
                "entry_ts": _to_iso(r["entry_ts"]),
                "exit_ts": _to_iso(r["exit_ts"]),
                "entry_price": float(r["entry_price"] or 0.0),
                "exit_price": float(r["exit_price"] or 0.0) if r["exit_price"] is not None else None,
                "pnl_usdt": float(r["pnl_usdt"] or 0.0),
                "close_reason": r["close_reason"],
            }
        )
    return out


def get_recent_entry_scores(db: Session, limit: int = 300, include_test: bool = False) -> List[Dict[str, Any]]:
    if include_test:
        sql = text(
            """
            SELECT
              trade_id AS id,
              entry_ts AS ts,
              symbol,
              direction AS side,
              COALESCE(signal_source, '') AS signal_type,
              COALESCE(regime, '') AS regime_at_entry,
              entry_score::double precision AS entry_score
            FROM bt_trade_snapshots
            WHERE entry_score IS NOT NULL
            ORDER BY entry_ts DESC
            LIMIT :limit
            """
        )
        rows = db.execute(sql, {"limit": limit}).mappings().all()
    else:
        sql = text(
            """
            SELECT
              trade_id AS id,
              entry_ts AS ts,
              symbol,
              CASE WHEN side='BUY' THEN 'LONG' WHEN side='SELL' THEN 'SHORT' ELSE side END AS side,
              COALESCE(strategy, '') AS signal_type,
              COALESCE(regime_at_entry, '') AS regime_at_entry,
              entry_score::double precision AS entry_score
            FROM v_trade_analytics
            WHERE entry_score IS NOT NULL
            ORDER BY entry_ts DESC
            LIMIT :limit
            """
        )
        rows = db.execute(sql, {"limit": limit}).mappings().all()

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": int(r["id"]),
                "ts": _to_iso(r["ts"]),
                "symbol": r["symbol"],
                "side": r["side"],
                "signal_type": r["signal_type"],
                "regime_at_entry": r["regime_at_entry"],
                "entry_score": float(r["entry_score"] or 0.0),
            }
        )
    return out


def build_entry_score_hist(scores: List[Dict[str, Any]], step: float = 0.5) -> Tuple[List[str], List[int]]:
    buckets: Dict[float, int] = {}
    for s in scores:
        val = float(s["entry_score"])
        b = round(val / step) * step
        buckets[b] = buckets.get(b, 0) + 1

    items = sorted(buckets.items(), key=lambda x: x[0])
    labels = [f"{k:.1f}" for k, _ in items]
    counts = [v for _, v in items]
    return labels, counts


# -----------------------------
# 이벤트 분석(완전)
# -----------------------------
def events_skip_reason_top(db: Session, days: int = 7, limit: int = 15) -> List[Dict[str, Any]]:
    sql = text(
        """
        SELECT reason, COUNT(*) AS n
        FROM bt_events
        WHERE event_type='SKIP'
          AND is_test=FALSE
          AND ts_utc >= now() - (:days * interval '1 day')
        GROUP BY reason
        ORDER BY n DESC
        LIMIT :limit
        """
    )
    rows = db.execute(sql, {"days": days, "limit": limit}).mappings().all()
    return [{"reason": r["reason"], "n": int(r["n"])} for r in rows]


def events_skip_hourly(db: Session, days: int = 7) -> List[Dict[str, Any]]:
    sql = text(
        """
        SELECT ts_kst_hour AS hour_kst, COUNT(*) AS n
        FROM bt_events
        WHERE event_type='SKIP'
          AND is_test=FALSE
          AND ts_utc >= now() - (:days * interval '1 day')
        GROUP BY ts_kst_hour
        ORDER BY ts_kst_hour
        """
    )
    rows = db.execute(sql, {"days": days}).mappings().all()
    return [{"hour_kst": int(r["hour_kst"]), "n": int(r["n"])} for r in rows]


def events_recent(db: Session, limit: int = 200, event_type: str | None = None) -> List[Dict[str, Any]]:
    if event_type:
        sql = text(
            """
            SELECT id, ts_utc, event_type, symbol, regime, source, side, reason, extra_json
            FROM bt_events
            WHERE is_test=FALSE
              AND event_type=:event_type
            ORDER BY id DESC
            LIMIT :limit
            """
        )
        rows = db.execute(sql, {"limit": limit, "event_type": event_type}).mappings().all()
    else:
        sql = text(
            """
            SELECT id, ts_utc, event_type, symbol, regime, source, side, reason, extra_json
            FROM bt_events
            WHERE is_test=FALSE
            ORDER BY id DESC
            LIMIT :limit
            """
        )
        rows = db.execute(sql, {"limit": limit}).mappings().all()

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": int(r["id"]),
                "ts_utc": _to_iso(r["ts_utc"]),
                "event_type": r["event_type"],
                "symbol": r["symbol"],
                "regime": r["regime"],
                "source": r["source"],
                "side": r["side"],
                "reason": r["reason"],
                "extra_json": r["extra_json"],
            }
        )
    return out