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
#
# 2025-11-15 변경 사항
# ----------------------------------------------------
# 1) SQLAlchemy Row 접근 방식 수정
#    - .fetchone() / .fetchall() → .mappings().one() / .mappings().all()
#      로 변경해 row["col"] 형태의 이름 기반 접근이 TypeError 없이 동작하도록 통일.
# 2) bt_trades 실제 스키마에 맞게 최근 트레이드 쿼리 수정
#    - 존재하지 않는 source 컬럼 제거.
#    - is_auto, strategy, entry_price, exit_price 를 포함해
#      대시보드에서 "자동/수동", "장 타입", "진입/청산가"를 바로 표현할 수 있도록 반환 필드 정리.
# 3) bt_trades 가 비어 있는 경우에도 summary 조회가 500 이 아닌
#    0/0/0 과 0.0 으로 안전하게 동작하도록 방어 로직 추가.
# ====================================================

from __future__ import annotations

from datetime import datetime, date
from typing import Any, Dict, List, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session


def get_daily_pnl(db: Session, days: int = 30) -> List[Dict[str, Any]]:
    """최근 N일 동안의 일별 손익 집계.

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

    # .mappings() 를 사용해 이름 기반(row["col"]) 접근이 가능하도록 통일
    rows = db.execute(sql, {"days": days}).mappings().all()
    result: List[Dict[str, Any]] = []

    for r in rows:
        d: date = r["trade_date"]
        pnl_usdt = float(r["pnl_usdt"] or 0.0)
        trade_count = int(r["trade_count"] or 0)
        result.append(
            {
                "date": d.isoformat(),  # "YYYY-MM-DD"
                "pnl_usdt": pnl_usdt,
                "trade_count": trade_count,
            }
        )
    return result


def get_summary(db: Session) -> Dict[str, Any]:
    """전체 트레이드 요약.

    - 총 트레이드 수
    - 승/패/BE 개수
    - 총 PnL(USDT)
    - 평균 PnL/트레이드
    - 승률(%)

    bt_trades 에 데이터가 0건이어도 COUNT(*) 는 항상 1행을 반환하므로,
    이 함수는 0/0/0 기준으로 안전하게 동작한다.
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

    # .mappings().one() 으로 이름 기반 접근과 단일 행 보장을 동시에 처리
    row_mapping = db.execute(sql).mappings().one_or_none()

    if row_mapping is None:
        # 이 경우는 사실상 발생하지 않지만, 방어적으로 0 기준으로 반환
        total_trades = 0
        wins = 0
        losses = 0
        breakevens = 0
        total_pnl_usdt = 0.0
    else:
        total_trades = int(row_mapping["total_trades"] or 0)
        wins = int(row_mapping["wins"] or 0)
        losses = int(row_mapping["losses"] or 0)
        breakevens = int(row_mapping["breakevens"] or 0)
        total_pnl_usdt = float(row_mapping["total_pnl_usdt"] or 0.0)

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
    """최근 트레이드 리스트 (기본 50건).

    - 대시보드 하단 테이블용.
    - bt_trades 실제 스키마에 맞게 SELECT 필드를 정리.
      * 자동/수동: is_auto
      * 장 타입(박스/추세): strategy
      * 진입/청산가: entry_price / exit_price
    """
    sql = text(
        """
        SELECT
            id,
            symbol,
            side,
            is_auto,
            strategy,
            entry_ts,
            exit_ts,
            entry_price,
            exit_price,
            pnl_usdt,
            close_reason
        FROM bt_trades
        WHERE exit_ts IS NOT NULL
        ORDER BY id DESC
        LIMIT :limit
        """
    )

    rows = db.execute(sql, {"limit": limit}).mappings().all()
    result: List[Dict[str, Any]] = []

    for r in rows:
        result.append(
            {
                "id": int(r["id"]),
                "symbol": r["symbol"],
                "side": r["side"],
                # 프론트에서 "자동/수동" 표시용
                "is_auto": bool(r["is_auto"]),
                # 프론트에서 "박스장/추세장" 표시용 (예: RANGE / TREND)
                "strategy": r["strategy"],
                "entry_ts": _to_iso(r["entry_ts"]),
                "exit_ts": _to_iso(r["exit_ts"]),
                "entry_price": float(r["entry_price"] or 0.0),
                # exit_price 는 청산 전 NULL 일 수 있으므로 None 허용
                "exit_price": float(r["exit_price"] or 0.0) if r["exit_price"] is not None else None,
                "pnl_usdt": float(r["pnl_usdt"] or 0.0),
                "close_reason": r["close_reason"],
            }
        )
    return result


def get_recent_entry_scores(db: Session, limit: int = 300) -> List[Dict[str, Any]]:
    """최근 EntryScore 목록 (기본 300건).

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

    rows = db.execute(sql, {"limit": limit}).mappings().all()
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
    """EntryScore 리스트로 히스토그램용 (라벨, 카운트) 생성.

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
    """datetime 을 ISO 문자열로 변환.

    - None 이면 빈 문자열 반환.
    - datetime 이 아니면 str() 로 변환.
    """
    if x is None:
        return ""
    if isinstance(x, datetime):
        return x.isoformat()
    return str(x)
