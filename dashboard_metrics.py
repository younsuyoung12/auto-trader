# dashboard_metrics.py
# ====================================================
# BingX Auto Trader - Dashboard Metrics
# ----------------------------------------------------
# bt_trades / bt_entry_scores 를 읽어서
# 1) 일별 손익
# 2) 전체 요약(승률, 총 PnL 등)
# 3) 최근 EntryScore 목록 + 히스토그램
# 을 계산하는 모듈.
#
# ⚠ 폴백 금지 원칙:
#    - 테이블/컬럼이 없거나 쿼리 실패 시 예외를 그대로 올린다.
#    - 호출부(FastAPI 핸들러)에서 500 으로 응답하고, Render 로그에 에러를 남긴다.
# ====================================================

from __future__ import annotations

from datetime import datetime, date
from typing import Any, Dict, List, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session


def get_daily_pnl(db: Session, days: int = 30) -> List[Dict[str, Any]]:
    """
    최근 N일 동안의 일별 손익 집계.
    - 기준: exit_ts 가 있는 트레이드만 합산.
    - 날짜 기준: KST 기준으로 일자 그룹핑.
    """
    sql = text(
        """
        SELECT
            (entry_ts AT TIME ZONE 'Asia/Seoul')::date AS trade_date,
            SUM(pnl_usdt) AS pnl_usdt,
            COUNT(*) AS trade_count
        FROM bt_trades
        WHERE exit_ts IS NOT NULL
          AND entry_ts >= now() - (:days * INTERVAL '1 day')
        GROUP BY trade_date
        ORDER BY trade_date
        """
    )

    rows = db.execute(sql, {"days": days}).fetchall()
    result: List[Dict[str, Any]] = []

    for r in rows:
        d: date = r["trade_date"]
        pnl_usdt = float(r["pnl_usdt"] or 0.0)
        trade_count = int(r["trade_count"] or 0)
        result.append(
            {
                "date": d.isoformat(),      # "YYYY-MM-DD"
                "pnl_usdt": pnl_usdt,
                "trade_count": trade_count,
            }
        )
    return result


def get_summary(db: Session) -> Dict[str, Any]:
    """
    전체 트레이드 요약:
    - 총 트레이드 수
    - 승/패/BE 개수
    - 총 PnL(USDT)
    - 평균 PnL/트레이드
    - 승률(%)
    """
    sql = text(
        """
        SELECT
            COUNT(*) AS total_trades,
            SUM(CASE WHEN pnl_usdt > 0 THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN pnl_usdt < 0 THEN 1 ELSE 0 END) AS losses,
            SUM(CASE WHEN pnl_usdt = 0 THEN 1 ELSE 0 END) AS breakevens,
            SUM(pnl_usdt) AS total_pnl_usdt
        FROM bt_trades
        WHERE exit_ts IS NOT NULL
        """
    )
    row = db.execute(sql).fetchone()
    if row is None:
        raise RuntimeError("bt_trades 에 데이터가 없습니다.")

    total_trades = int(row["total_trades"] or 0)
    wins = int(row["wins"] or 0)
    losses = int(row["losses"] or 0)
    breakevens = int(row["breakevens"] or 0)
    total_pnl_usdt = float(row["total_pnl_usdt"] or 0.0)

    win_rate = (wins / total_trades * 100.0) if total_trades > 0 else 0.0
    avg_pnl_usdt = (total_pnl_usdt / total_trades) if total_trades > 0 else 0.0

    return {
        "total_trades": total_trades,
        "wins": wins,
        "losses": losses,
        "breakevens": breakevens,
        "total_pnl_usdt": total_pnl_usdt,
        "win_rate_pct": win_rate,
        "avg_pnl_usdt": avg_pnl_usdt,
    }


def get_recent_trades(db: Session, limit: int = 50) -> List[Dict[str, Any]]:
    """
    최근 트레이드 리스트 (기본 50건).
    - 대시보드 하단 테이블용.
    """
    sql = text(
        """
        SELECT
            id,
            symbol,
            side,
            source,
            entry_ts,
            exit_ts,
            pnl_usdt,
            close_reason
        FROM bt_trades
        WHERE exit_ts IS NOT NULL
        ORDER BY id DESC
        LIMIT :limit
        """
    )
    rows = db.execute(sql, {"limit": limit}).fetchall()
    result: List[Dict[str, Any]] = []
    for r in rows:
        result.append(
            {
                "id": int(r["id"]),
                "symbol": r["symbol"],
                "side": r["side"],
                "source": r["source"],
                "entry_ts": _to_iso(r["entry_ts"]),
                "exit_ts": _to_iso(r["exit_ts"]),
                "pnl_usdt": float(r["pnl_usdt"] or 0.0),
                "close_reason": r["close_reason"],
            }
        )
    return result


def get_recent_entry_scores(db: Session, limit: int = 300) -> List[Dict[str, Any]]:
    """
    최근 EntryScore 목록 (기본 300건).
    - score 분포/히스토그램 + 최근 개별 진입 품질 확인용.
    """
    sql = text(
        """
        SELECT
            id,
            ts,
            symbol,
            side,
            signal_type,
            regime_at_entry,
            entry_score
        FROM bt_entry_scores
        ORDER BY ts DESC
        LIMIT :limit
        """
    )
    rows = db.execute(sql, {"limit": limit}).fetchall()
    result: List[Dict[str, Any]] = []
    for r in rows:
        result.append(
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
    return result


def build_entry_score_hist(
    scores: List[Dict[str, Any]],
    step: float = 0.5,
) -> Tuple[List[str], List[int]]:
    """
    EntryScore 리스트를 받아서 간단한 히스토그램용 (라벨, 카운트) 를 만든다.
    - step 기본: 0.5 점 간격.
    """
    buckets: Dict[float, int] = {}
    for s in scores:
        val = float(s["entry_score"])
        # step 단위로 버킷팅
        bucket = round(val / step) * step
        buckets[bucket] = buckets.get(bucket, 0) + 1

    items = sorted(buckets.items(), key=lambda x: x[0])
    labels = [f"{k:.1f}" for k, _ in items]
    counts = [v for _, v in items]
    return labels, counts


def _to_iso(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, datetime):
        return x.isoformat()
    return str(x)
