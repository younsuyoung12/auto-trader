"""
========================================================
FILE: broadcast/dashboard_metrics.py
ROLE:
- 대시보드 조회 계층에서 거래/성과/이벤트 지표를 읽어
  프론트 표시용 구조로 정규화한다.
- 초기 무데이터 상태와 실제 데이터 이상을 구분한다.

CORE RESPONSIBILITIES:
- v_trades_prod 기반 기본 성과 요약/최근 거래/일별 손익 제공
- v_trade_analytics / bt_trade_snapshots 기반 최근 entry score 제공
- bt_events 기반 SKIP/최근 이벤트 분석 제공
- 조회 계층에서 무데이터는 정상 상태로 표현하고, 데이터 이상은 즉시 예외 처리

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · PRODUCTION / TRADE-GRADE
- None → "" / 0 / [] 자동 보정 금지
- 무데이터는 INIT 상태 또는 빈 결과로 명시적으로 반환
- 행이 존재하는데 필수 컬럼/타입/스키마가 잘못되면 즉시 예외
- COALESCE / or 0 / or "" 식의 원인 은닉 금지

CHANGE HISTORY:
- 2026-03-11
  1) FIX(ROOT-CAUSE): get_summary()가 INIT 상태에서 NULL aggregate 검증 전에 즉시 INIT 요약을 반환하도록 수정
  2) FIX(STRICT): get_recent_trades()의 is_auto bool(...) 강제 변환 제거, 실제 bool만 허용
  3) FIX(ROOT-CAUSE): 조회 계층의 None→0/"" 보정 제거
  4) FIX(ARCH): 무데이터 상태를 INIT / 빈 결과로 명시화
  5) FIX(STRICT): 행 존재 시 컬럼/타입 검증 강화
  6) FIX(SQL): entry score 조회에서 COALESCE 제거
========================================================
"""

from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any, Dict, List, Mapping, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session


_DATA_STATE_READY = "READY"
_DATA_STATE_INIT_NO_CLOSED_TRADES = "INIT_NO_CLOSED_TRADES"


def _require_db(db: Session) -> Session:
    if db is None:
        raise RuntimeError("db session is required (STRICT)")
    return db


def _require_positive_int(value: Any, name: str) -> int:
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


def _require_nonnegative_int(value: Any, name: str) -> int:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if isinstance(value, bool):
        raise RuntimeError(f"{name} must be int, bool not allowed (STRICT)")
    try:
        iv = int(value)
    except Exception as exc:
        raise RuntimeError(f"{name} must be int (STRICT): {exc}") from exc
    if iv < 0:
        raise RuntimeError(f"{name} must be >= 0 (STRICT)")
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


def _optional_nonempty_str(value: Any, name: str) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError(f"{name} must be str or null (STRICT), got={type(value).__name__}")
    s = value.strip()
    if not s:
        raise RuntimeError(f"{name} must not be blank when provided (STRICT)")
    return s


def _require_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise RuntimeError(f"{name} must be bool (STRICT), got={type(value).__name__}")
    return value


def _require_finite_float(value: Any, name: str) -> float:
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


def _require_date(value: Any, name: str) -> date:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if not isinstance(value, date):
        raise RuntimeError(f"{name} must be date (STRICT), got={type(value).__name__}")
    return value


def _require_iso_datetime(value: Any, name: str) -> str:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        return _require_nonempty_str(value, name)
    raise RuntimeError(f"{name} must be datetime or str (STRICT), got={type(value).__name__}")


def _optional_iso_datetime(value: Any, name: str) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        return _require_nonempty_str(value, name)
    raise RuntimeError(f"{name} must be datetime or str or null (STRICT), got={type(value).__name__}")


def _optional_mapping(value: Any, name: str) -> Optional[Dict[str, Any]]:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise RuntimeError(f"{name} must be mapping/dict or null (STRICT), got={type(value).__name__}")
    return dict(value)


def _count_closed_trades(db: Session) -> int:
    _require_db(db)
    sql = text(
        """
        SELECT COUNT(*) AS cnt
        FROM v_trades_prod
        WHERE exit_ts IS NOT NULL
        """
    )
    row = db.execute(sql).mappings().one()
    return _require_nonnegative_int(row.get("cnt"), "v_trades_prod.closed_trade_count")


def _build_empty_summary() -> Dict[str, Any]:
    return {
        "total_trades": 0,
        "wins": 0,
        "losses": 0,
        "breakevens": 0,
        "total_pnl_usdt": 0.0,
        "win_rate_pct": 0.0,
        "avg_pnl_usdt": 0.0,
        "data_state": _DATA_STATE_INIT_NO_CLOSED_TRADES,
        "has_closed_trades": False,
    }


# -----------------------------
# 기존 대시보드 기본 지표
# -----------------------------
def get_daily_pnl(db: Session, days: int = 30) -> List[Dict[str, Any]]:
    _require_db(db)
    normalized_days = _require_positive_int(days, "days")

    if _count_closed_trades(db) == 0:
        return []

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
    rows = db.execute(sql, {"days": normalized_days}).mappings().all()

    if not rows:
        return []

    out: List[Dict[str, Any]] = []
    for idx, r in enumerate(rows):
        trade_date = _require_date(r.get("trade_date"), f"daily_pnl[{idx}].trade_date")
        pnl_usdt = _require_finite_float(r.get("pnl_usdt"), f"daily_pnl[{idx}].pnl_usdt")
        trade_count = _require_positive_int(r.get("trade_count"), f"daily_pnl[{idx}].trade_count")

        out.append(
            {
                "date": trade_date.isoformat(),
                "pnl_usdt": pnl_usdt,
                "trade_count": trade_count,
            }
        )
    return out


def get_summary(db: Session) -> Dict[str, Any]:
    _require_db(db)

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
    if r is None:
        raise RuntimeError("summary aggregate row not found (STRICT)")

    total = _require_nonnegative_int(r.get("total_trades"), "summary.total_trades")
    if total == 0:
        return _build_empty_summary()

    wins = _require_nonnegative_int(r.get("wins"), "summary.wins")
    losses = _require_nonnegative_int(r.get("losses"), "summary.losses")
    breakevens = _require_nonnegative_int(r.get("breakevens"), "summary.breakevens")
    total_pnl = _require_finite_float(r.get("total_pnl_usdt"), "summary.total_pnl_usdt")

    if wins + losses + breakevens != total:
        raise RuntimeError(
            "summary counts mismatch (STRICT): wins + losses + breakevens must equal total_trades"
        )

    win_rate = (wins / total) * 100.0
    avg_pnl = total_pnl / total

    return {
        "total_trades": total,
        "wins": wins,
        "losses": losses,
        "breakevens": breakevens,
        "total_pnl_usdt": total_pnl,
        "win_rate_pct": win_rate,
        "avg_pnl_usdt": avg_pnl,
        "data_state": _DATA_STATE_READY,
        "has_closed_trades": True,
    }


def get_recent_trades(db: Session, limit: int = 50) -> List[Dict[str, Any]]:
    _require_db(db)
    normalized_limit = _require_positive_int(limit, "limit")

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
    rows = db.execute(sql, {"limit": normalized_limit}).mappings().all()
    if not rows:
        return []

    out: List[Dict[str, Any]] = []
    for idx, r in enumerate(rows):
        out.append(
            {
                "id": _require_positive_int(r.get("id"), f"recent_trades[{idx}].id"),
                "symbol": _require_nonempty_str(r.get("symbol"), f"recent_trades[{idx}].symbol"),
                "side": _require_nonempty_str(r.get("side"), f"recent_trades[{idx}].side"),
                "is_auto": _require_bool(r.get("is_auto"), f"recent_trades[{idx}].is_auto"),
                "strategy": _optional_nonempty_str(r.get("strategy"), f"recent_trades[{idx}].strategy"),
                "entry_ts": _optional_iso_datetime(r.get("entry_ts"), f"recent_trades[{idx}].entry_ts"),
                "exit_ts": _require_iso_datetime(r.get("exit_ts"), f"recent_trades[{idx}].exit_ts"),
                "entry_price": _require_finite_float(r.get("entry_price"), f"recent_trades[{idx}].entry_price"),
                "exit_price": (
                    None
                    if r.get("exit_price") is None
                    else _require_finite_float(r.get("exit_price"), f"recent_trades[{idx}].exit_price")
                ),
                "pnl_usdt": _require_finite_float(r.get("pnl_usdt"), f"recent_trades[{idx}].pnl_usdt"),
                "close_reason": _optional_nonempty_str(r.get("close_reason"), f"recent_trades[{idx}].close_reason"),
            }
        )
    return out


def get_recent_entry_scores(db: Session, limit: int = 300, include_test: bool = False) -> List[Dict[str, Any]]:
    _require_db(db)
    normalized_limit = _require_positive_int(limit, "limit")

    if include_test:
        sql = text(
            """
            SELECT
              trade_id AS id,
              entry_ts AS ts,
              symbol,
              direction AS side,
              signal_source AS signal_type,
              regime AS regime_at_entry,
              entry_score::double precision AS entry_score
            FROM bt_trade_snapshots
            WHERE entry_score IS NOT NULL
            ORDER BY entry_ts DESC
            LIMIT :limit
            """
        )
        rows = db.execute(sql, {"limit": normalized_limit}).mappings().all()
    else:
        sql = text(
            """
            SELECT
              trade_id AS id,
              entry_ts AS ts,
              symbol,
              CASE WHEN side='BUY' THEN 'LONG' WHEN side='SELL' THEN 'SHORT' ELSE side END AS side,
              strategy AS signal_type,
              regime_at_entry,
              entry_score::double precision AS entry_score
            FROM v_trade_analytics
            WHERE entry_score IS NOT NULL
            ORDER BY entry_ts DESC
            LIMIT :limit
            """
        )
        rows = db.execute(sql, {"limit": normalized_limit}).mappings().all()

    if not rows:
        return []

    out: List[Dict[str, Any]] = []
    for idx, r in enumerate(rows):
        out.append(
            {
                "id": _require_positive_int(r.get("id"), f"entry_scores[{idx}].id"),
                "ts": _require_iso_datetime(r.get("ts"), f"entry_scores[{idx}].ts"),
                "symbol": _require_nonempty_str(r.get("symbol"), f"entry_scores[{idx}].symbol"),
                "side": _require_nonempty_str(r.get("side"), f"entry_scores[{idx}].side"),
                "signal_type": _optional_nonempty_str(r.get("signal_type"), f"entry_scores[{idx}].signal_type"),
                "regime_at_entry": _optional_nonempty_str(
                    r.get("regime_at_entry"),
                    f"entry_scores[{idx}].regime_at_entry",
                ),
                "entry_score": _require_finite_float(r.get("entry_score"), f"entry_scores[{idx}].entry_score"),
            }
        )
    return out


def build_entry_score_hist(scores: List[Dict[str, Any]], step: float = 0.5) -> Tuple[List[str], List[int]]:
    if isinstance(step, bool):
        raise RuntimeError("step must be float, bool not allowed (STRICT)")
    try:
        normalized_step = float(step)
    except Exception as exc:
        raise RuntimeError(f"step must be float (STRICT): {exc}") from exc
    if not math.isfinite(normalized_step) or normalized_step <= 0.0:
        raise RuntimeError(f"step must be finite and > 0 (STRICT): {normalized_step!r}")

    buckets: Dict[float, int] = {}
    for idx, s in enumerate(scores):
        if not isinstance(s, Mapping):
            raise RuntimeError(f"scores[{idx}] must be mapping/dict (STRICT)")
        val = _require_finite_float(s.get("entry_score"), f"scores[{idx}].entry_score")
        bucket = round(val / normalized_step) * normalized_step
        buckets[bucket] = buckets.get(bucket, 0) + 1

    items = sorted(buckets.items(), key=lambda x: x[0])
    labels = [f"{k:.1f}" for k, _ in items]
    counts = [v for _, v in items]
    return labels, counts


# -----------------------------
# 이벤트 분석(완전)
# -----------------------------
def events_skip_reason_top(db: Session, days: int = 7, limit: int = 15) -> List[Dict[str, Any]]:
    _require_db(db)
    normalized_days = _require_positive_int(days, "days")
    normalized_limit = _require_positive_int(limit, "limit")

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
    rows = db.execute(sql, {"days": normalized_days, "limit": normalized_limit}).mappings().all()
    if not rows:
        return []

    out: List[Dict[str, Any]] = []
    for idx, r in enumerate(rows):
        out.append(
            {
                "reason": _require_nonempty_str(r.get("reason"), f"skip_reason_top[{idx}].reason"),
                "n": _require_positive_int(r.get("n"), f"skip_reason_top[{idx}].n"),
            }
        )
    return out


def events_skip_hourly(db: Session, days: int = 7) -> List[Dict[str, Any]]:
    _require_db(db)
    normalized_days = _require_positive_int(days, "days")

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
    rows = db.execute(sql, {"days": normalized_days}).mappings().all()
    if not rows:
        return []

    out: List[Dict[str, Any]] = []
    for idx, r in enumerate(rows):
        hour_kst = _require_nonnegative_int(r.get("hour_kst"), f"skip_hourly[{idx}].hour_kst")
        if hour_kst > 23:
            raise RuntimeError(f"skip_hourly[{idx}].hour_kst must be <= 23 (STRICT)")
        out.append(
            {
                "hour_kst": hour_kst,
                "n": _require_positive_int(r.get("n"), f"skip_hourly[{idx}].n"),
            }
        )
    return out


def events_recent(db: Session, limit: int = 200, event_type: str | None = None) -> List[Dict[str, Any]]:
    _require_db(db)
    normalized_limit = _require_positive_int(limit, "limit")
    normalized_event_type = None if event_type is None else _require_nonempty_str(event_type, "event_type")

    if normalized_event_type:
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
        rows = db.execute(sql, {"limit": normalized_limit, "event_type": normalized_event_type}).mappings().all()
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
        rows = db.execute(sql, {"limit": normalized_limit}).mappings().all()

    if not rows:
        return []

    out: List[Dict[str, Any]] = []
    for idx, r in enumerate(rows):
        out.append(
            {
                "id": _require_positive_int(r.get("id"), f"events_recent[{idx}].id"),
                "ts_utc": _require_iso_datetime(r.get("ts_utc"), f"events_recent[{idx}].ts_utc"),
                "event_type": _require_nonempty_str(r.get("event_type"), f"events_recent[{idx}].event_type"),
                "symbol": _require_nonempty_str(r.get("symbol"), f"events_recent[{idx}].symbol"),
                "regime": _optional_nonempty_str(r.get("regime"), f"events_recent[{idx}].regime"),
                "source": _optional_nonempty_str(r.get("source"), f"events_recent[{idx}].source"),
                "side": _optional_nonempty_str(r.get("side"), f"events_recent[{idx}].side"),
                "reason": _optional_nonempty_str(r.get("reason"), f"events_recent[{idx}].reason"),
                "extra_json": _optional_mapping(r.get("extra_json"), f"events_recent[{idx}].extra_json"),
            }
        )
    return out