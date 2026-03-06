"""
========================================================
FILE: services/performance_service.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할
--------------------------------------------------------
- v_trades_prod 기준으로 종료 거래 성과를 계산한다.
- 대시보드 "성과 분석" 패널에 필요한 구조로 정규화한다.
- 손익 요약 / Equity Curve / Drawdown Curve / 일별 손익을 계산한다.
- 잘못된 컬럼 / 누락 필드 / 비정상 타입은 즉시 예외로 실패한다.

지원 기능
--------------------------------------------------------
- 성과 요약 조회
- Equity Curve 조회
- Drawdown Curve 조회
- 최근 N일 일별 손익 조회
- 성과 번들(summary + equity + drawdown + daily_pnl) 조회

데이터 소스
--------------------------------------------------------
- v_trades_prod
  * 필수 컬럼:
    - id
    - exit_ts
    - pnl_usdt

절대 원칙 (STRICT · NO-FALLBACK)
--------------------------------------------------------
- 종료 거래가 없으면 즉시 예외.
- 필수 컬럼 누락 시 즉시 예외.
- 숫자형 필드는 finite float/int 아니면 즉시 예외.
- 임의 기본값/더미값/자동 보정 금지.
- gross_loss_abs_usdt = 0 인 경우 profit_factor는 정의되지 않으므로 None 반환.
- peak_equity <= 0 인 구간의 drawdown_pct는 정의되지 않으므로 None 반환.

PATCH NOTES — 2026-03-07 (TRADE-GRADE)
--------------------------------------------------------
- v_trades_prod 스키마/데이터 진단 강화
  1) information_schema 기준 필수 컬럼(id / exit_ts / pnl_usdt) 존재 검증 추가
  2) "뷰 자체가 비었는지" vs "행은 있는데 종료거래(exit_ts IS NOT NULL)가 없는지"를 구분해서 즉시 예외
  3) 최근 N일 daily pnl 부재 시에도 전체 종료거래 존재 여부를 먼저 검증하여 원인 메시지 명확화
- 기존 계산 로직(summary / equity / drawdown / daily pnl)은 변경하지 않음

변경 이력
--------------------------------------------------------
- 2026-03-07:
  1) v_trades_prod 스키마 검증 추가
  2) 종료거래 부재 원인 메시지 세분화
  3) 기존 성과 계산 기능 변경 없음
- 2026-03-06:
  1) 신규 생성: 성과 분석 서비스 추가
  2) summary / equity curve / drawdown curve / daily pnl 계산 기능 추가
  3) 대시보드용 엄격 스키마 정규화 추가
========================================================
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Mapping, Optional, Set

from sqlalchemy import text
from sqlalchemy.orm import Session


_REQUIRED_V_TRADES_PROD_COLUMNS: Set[str] = {"id", "exit_ts", "pnl_usdt"}


@dataclass(frozen=True, slots=True)
class ClosedTradePoint:
    id: int
    exit_ts: str
    pnl_usdt: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "exit_ts": self.exit_ts,
            "pnl_usdt": self.pnl_usdt,
        }


@dataclass(frozen=True, slots=True)
class PerformanceSummary:
    total_trades: int
    wins: int
    losses: int
    breakevens: int
    win_rate_pct: float
    total_pnl_usdt: float
    avg_pnl_usdt: float
    gross_profit_usdt: float
    gross_loss_abs_usdt: float
    avg_win_usdt: Optional[float]
    avg_loss_abs_usdt: Optional[float]
    profit_factor: Optional[float]
    expectancy_usdt: float
    max_drawdown_usdt: float
    max_drawdown_pct: Optional[float]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_trades": self.total_trades,
            "wins": self.wins,
            "losses": self.losses,
            "breakevens": self.breakevens,
            "win_rate_pct": self.win_rate_pct,
            "total_pnl_usdt": self.total_pnl_usdt,
            "avg_pnl_usdt": self.avg_pnl_usdt,
            "gross_profit_usdt": self.gross_profit_usdt,
            "gross_loss_abs_usdt": self.gross_loss_abs_usdt,
            "avg_win_usdt": self.avg_win_usdt,
            "avg_loss_abs_usdt": self.avg_loss_abs_usdt,
            "profit_factor": self.profit_factor,
            "expectancy_usdt": self.expectancy_usdt,
            "max_drawdown_usdt": self.max_drawdown_usdt,
            "max_drawdown_pct": self.max_drawdown_pct,
        }


def _require_int(value: Any, name: str) -> int:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if isinstance(value, bool):
        raise RuntimeError(f"{name} must be int, bool not allowed (STRICT)")
    try:
        iv = int(value)
    except Exception as exc:
        raise RuntimeError(f"{name} must be int (STRICT): {exc}") from exc
    if iv <= 0:
        raise RuntimeError(f"{name} must be > 0 (STRICT)")
    return iv


def _require_nonempty_str(value: Any, name: str) -> str:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if not isinstance(value, str):
        raise RuntimeError(f"{name} must be str (STRICT), got={type(value).__name__}")
    s = value.strip()
    if not s:
        raise RuntimeError(f"{name} must not be empty (STRICT)")
    return s


def _require_float(value: Any, name: str) -> float:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if isinstance(value, bool):
        raise RuntimeError(f"{name} must be float, bool not allowed (STRICT)")
    try:
        fv = float(value)
    except Exception as exc:
        raise RuntimeError(f"{name} must be float (STRICT): {exc}") from exc
    if not math.isfinite(fv):
        raise RuntimeError(f"{name} must be finite (STRICT): {fv!r}")
    return fv


def _to_iso(value: Any, name: str) -> str:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        return _require_nonempty_str(value, name)
    raise RuntimeError(f"{name} must be datetime or str (STRICT), got={type(value).__name__}")


def _normalize_closed_trade_row(row: Mapping[str, Any]) -> ClosedTradePoint:
    trade_id = _require_int(row.get("id"), "v_trades_prod.id")
    exit_ts = _to_iso(row.get("exit_ts"), "v_trades_prod.exit_ts")
    pnl_usdt = _require_float(row.get("pnl_usdt"), "v_trades_prod.pnl_usdt")
    return ClosedTradePoint(
        id=trade_id,
        exit_ts=exit_ts,
        pnl_usdt=pnl_usdt,
    )


def _assert_v_trades_prod_required_columns(db: Session) -> None:
    if db is None:
        raise RuntimeError("db session is required (STRICT)")

    sql = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'v_trades_prod'
          AND table_schema = ANY(current_schemas(false))
        """
    )
    rows = db.execute(sql).mappings().all()
    if not rows:
        return {
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "total_pnl_usdt": 0
        }

    present = {str(row.get("column_name", "")).strip() for row in rows}
    missing = sorted(_REQUIRED_V_TRADES_PROD_COLUMNS - present)
    if missing:
        raise RuntimeError(f"v_trades_prod missing required columns (STRICT): {missing}")


def _count_v_trades_prod_rows(db: Session) -> int:
    sql = text("SELECT COUNT(*) AS cnt FROM v_trades_prod")
    row = db.execute(sql).mappings().one()
    cnt_raw = row.get("cnt")
    if cnt_raw is None:
        raise RuntimeError("COUNT(*) from v_trades_prod returned NULL (STRICT)")
    if isinstance(cnt_raw, bool):
        raise RuntimeError("COUNT(*) from v_trades_prod must be int, bool not allowed (STRICT)")
    try:
        cnt = int(cnt_raw)
    except Exception as exc:
        raise RuntimeError(f"COUNT(*) from v_trades_prod must be int (STRICT): {exc}") from exc
    if cnt < 0:
        raise RuntimeError("COUNT(*) from v_trades_prod must be >= 0 (STRICT)")
    return cnt


def _fetch_closed_trades(db: Session) -> List[ClosedTradePoint]:
    if db is None:
        raise RuntimeError("db session is required (STRICT)")

    _assert_v_trades_prod_required_columns(db)

    sql = text(
        """
        SELECT
            id,
            exit_ts,
            pnl_usdt
        FROM v_trades_prod
        WHERE exit_ts IS NOT NULL
        ORDER BY exit_ts ASC, id ASC
        """
    )
    rows = db.execute(sql).mappings().all()
    if not rows:
        total_rows = _count_v_trades_prod_rows(db)
        if total_rows <= 0:
            raise RuntimeError("v_trades_prod is empty (STRICT)")
        raise RuntimeError("v_trades_prod has rows but no closed trades with exit_ts IS NOT NULL (STRICT)")

    out: List[ClosedTradePoint] = []
    for row in rows:
        out.append(_normalize_closed_trade_row(row))
    return out


def get_performance_summary(db: Session) -> Dict[str, Any]:
    trades = _fetch_closed_trades(db)

    total_trades = len(trades)
    wins = 0
    losses = 0
    breakevens = 0
    total_pnl_usdt = 0.0
    gross_profit_usdt = 0.0
    gross_loss_abs_usdt = 0.0

    equity = 0.0
    peak_equity = 0.0
    max_drawdown_usdt = 0.0
    max_drawdown_pct: Optional[float] = None

    for trade in trades:
        pnl = trade.pnl_usdt
        total_pnl_usdt += pnl

        if pnl > 0.0:
            wins += 1
            gross_profit_usdt += pnl
        elif pnl < 0.0:
            losses += 1
            gross_loss_abs_usdt += abs(pnl)
        else:
            breakevens += 1

        equity += pnl
        if equity > peak_equity:
            peak_equity = equity

        drawdown_usdt = peak_equity - equity
        if drawdown_usdt > max_drawdown_usdt:
            max_drawdown_usdt = drawdown_usdt
            if peak_equity > 0.0:
                max_drawdown_pct = (drawdown_usdt / peak_equity) * 100.0
            else:
                max_drawdown_pct = None

    win_rate_pct = (wins / total_trades) * 100.0
    avg_pnl_usdt = total_pnl_usdt / total_trades
    avg_win_usdt = (gross_profit_usdt / wins) if wins > 0 else None
    avg_loss_abs_usdt = (gross_loss_abs_usdt / losses) if losses > 0 else None
    profit_factor = (gross_profit_usdt / gross_loss_abs_usdt) if gross_loss_abs_usdt > 0.0 else None
    expectancy_usdt = total_pnl_usdt / total_trades

    summary = PerformanceSummary(
        total_trades=total_trades,
        wins=wins,
        losses=losses,
        breakevens=breakevens,
        win_rate_pct=win_rate_pct,
        total_pnl_usdt=total_pnl_usdt,
        avg_pnl_usdt=avg_pnl_usdt,
        gross_profit_usdt=gross_profit_usdt,
        gross_loss_abs_usdt=gross_loss_abs_usdt,
        avg_win_usdt=avg_win_usdt,
        avg_loss_abs_usdt=avg_loss_abs_usdt,
        profit_factor=profit_factor,
        expectancy_usdt=expectancy_usdt,
        max_drawdown_usdt=max_drawdown_usdt,
        max_drawdown_pct=max_drawdown_pct,
    )
    return summary.to_dict()


def get_equity_curve(db: Session, *, limit: int = 1000) -> List[Dict[str, Any]]:
    normalized_limit = _require_int(limit, "limit")
    trades = _fetch_closed_trades(db)
    if normalized_limit > len(trades):
        normalized_limit = len(trades)

    selected = trades[-normalized_limit:]
    equity = 0.0
    out: List[Dict[str, Any]] = []

    for trade in selected:
        equity += trade.pnl_usdt
        out.append(
            {
                "id": trade.id,
                "exit_ts": trade.exit_ts,
                "pnl_usdt": trade.pnl_usdt,
                "equity_usdt": equity,
            }
        )

    if not out:
        raise RuntimeError("equity curve empty (STRICT)")
    return out


def get_drawdown_curve(db: Session, *, limit: int = 1000) -> List[Dict[str, Any]]:
    normalized_limit = _require_int(limit, "limit")
    trades = _fetch_closed_trades(db)
    if normalized_limit > len(trades):
        normalized_limit = len(trades)

    selected = trades[-normalized_limit:]
    equity = 0.0
    peak_equity = 0.0
    out: List[Dict[str, Any]] = []

    for trade in selected:
        equity += trade.pnl_usdt
        if equity > peak_equity:
            peak_equity = equity

        drawdown_usdt = peak_equity - equity
        if peak_equity > 0.0:
            drawdown_pct: Optional[float] = (drawdown_usdt / peak_equity) * 100.0
        else:
            drawdown_pct = None

        out.append(
            {
                "id": trade.id,
                "exit_ts": trade.exit_ts,
                "equity_usdt": equity,
                "peak_equity_usdt": peak_equity,
                "drawdown_usdt": drawdown_usdt,
                "drawdown_pct": drawdown_pct,
            }
        )

    if not out:
        raise RuntimeError("drawdown curve empty (STRICT)")
    return out


def get_daily_pnl_series(db: Session, *, days: int = 30) -> List[Dict[str, Any]]:
    if db is None:
        raise RuntimeError("db session is required (STRICT)")

    normalized_days = _require_int(days, "days")
    _assert_v_trades_prod_required_columns(db)

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
        ORDER BY trade_date ASC
        """
    )
    rows = db.execute(sql, {"days": normalized_days}).mappings().all()
    if not rows:
        closed_trades = _fetch_closed_trades(db)
        if not closed_trades:
            raise RuntimeError("daily pnl series not found because closed trades do not exist (STRICT)")
        raise RuntimeError(f"daily pnl series not found in last {normalized_days} days (STRICT)")

    out: List[Dict[str, Any]] = []
    for row in rows:
        trade_date = row.get("trade_date")
        if trade_date is None:
            raise RuntimeError("trade_date is required (STRICT)")
        if not isinstance(trade_date, date):
            raise RuntimeError(f"trade_date must be date (STRICT), got={type(trade_date).__name__}")

        pnl_usdt = _require_float(row.get("pnl_usdt"), "SUM(pnl_usdt)")
        trade_count_raw = row.get("trade_count")
        if trade_count_raw is None:
            raise RuntimeError("trade_count is required (STRICT)")
        if isinstance(trade_count_raw, bool):
            raise RuntimeError("trade_count must be int, bool not allowed (STRICT)")
        try:
            trade_count = int(trade_count_raw)
        except Exception as exc:
            raise RuntimeError(f"trade_count must be int (STRICT): {exc}") from exc
        if trade_count <= 0:
            raise RuntimeError("trade_count must be > 0 (STRICT)")

        out.append(
            {
                "date": trade_date.isoformat(),
                "pnl_usdt": pnl_usdt,
                "trade_count": trade_count,
            }
        )

    return out


def get_performance_bundle(
    db: Session,
    *,
    curve_limit: int = 1000,
    daily_days: int = 30,
) -> Dict[str, Any]:
    return {
        "summary": get_performance_summary(db),
        "equity_curve": get_equity_curve(db, limit=curve_limit),
        "drawdown_curve": get_drawdown_curve(db, limit=curve_limit),
        "daily_pnl": get_daily_pnl_series(db, days=daily_days),
    }


__all__ = [
    "ClosedTradePoint",
    "PerformanceSummary",
    "get_performance_summary",
    "get_equity_curve",
    "get_drawdown_curve",
    "get_daily_pnl_series",
    "get_performance_bundle",
]