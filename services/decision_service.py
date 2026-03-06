"""
========================================================
FILE: services/decision_service.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할
--------------------------------------------------------
- bt_events 테이블의 DECISION 이벤트를 조회한다.
- 대시보드 "의사결정 이유" 패널에 필요한 구조로 정규화한다.
- 잘못된 event payload / 누락 필드 / 비정상 타입은 즉시 예외로 실패한다.

지원 기능
--------------------------------------------------------
- 최신 DECISION 1건 조회
- 최근 DECISION 목록 조회

DECISION extra_json 필수 스키마
--------------------------------------------------------
{
  "action": "ENTRY" | "NO_ENTRY" | "HOLD" | "EXIT",
  "summary": "현재 판단 요약",
  "reasons": ["사유1", "사유2", ...],
  "entry_score": 1.82,
  "trend_strength": 0.64,
  "spread": 1.25,
  "orderbook_imbalance": 0.41
}

DECISION extra_json 선택 스키마
--------------------------------------------------------
{
  "threshold": 1.80,
  "exit_score": 0.42,
  "current_price": 62450.5,
  "entry_price": 62300.0,
  "target_price": 63000.0,
  "stop_price": 62000.0,
  "pnl_pct": 0.84,
  "position_qty": 0.01,
  "signal_source": "ws_signal_candidate"
}

절대 원칙 (STRICT · NO-FALLBACK)
--------------------------------------------------------
- DECISION 이벤트가 없으면 즉시 예외.
- extra_json이 dict가 아니면 즉시 예외.
- 필수 키 누락 시 즉시 예외.
- 숫자형 필드는 finite float/int 아니면 즉시 예외.
- 임의 기본값/더미값/자동 보정 금지.

변경 이력
--------------------------------------------------------
- 2026-03-06:
  1) 신규 생성: DECISION 이벤트 조회/검증 서비스 추가
  2) 최신 DECISION / 최근 DECISION 목록 조회 기능 추가
  3) 대시보드용 엄격 스키마 정규화 추가
========================================================
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence

from sqlalchemy import text
from sqlalchemy.orm import Session


_ALLOWED_ACTIONS = ("ENTRY", "NO_ENTRY", "HOLD", "EXIT")


@dataclass(frozen=True, slots=True)
class DecisionRecord:
    id: int
    ts_utc: str
    symbol: str
    action: str
    reason_code: str
    summary: str
    reasons: List[str]
    entry_score: float
    trend_strength: float
    spread: float
    orderbook_imbalance: float
    regime: Optional[str]
    side: Optional[str]
    threshold: Optional[float]
    exit_score: Optional[float]
    current_price: Optional[float]
    entry_price: Optional[float]
    target_price: Optional[float]
    stop_price: Optional[float]
    pnl_pct: Optional[float]
    position_qty: Optional[float]
    signal_source: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "ts_utc": self.ts_utc,
            "symbol": self.symbol,
            "action": self.action,
            "reason_code": self.reason_code,
            "summary": self.summary,
            "reasons": list(self.reasons),
            "entry_score": self.entry_score,
            "trend_strength": self.trend_strength,
            "spread": self.spread,
            "orderbook_imbalance": self.orderbook_imbalance,
            "regime": self.regime,
            "side": self.side,
            "threshold": self.threshold,
            "exit_score": self.exit_score,
            "current_price": self.current_price,
            "entry_price": self.entry_price,
            "target_price": self.target_price,
            "stop_price": self.stop_price,
            "pnl_pct": self.pnl_pct,
            "position_qty": self.position_qty,
            "signal_source": self.signal_source,
        }


def _to_iso(value: Any, name: str) -> str:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        s = value.strip()
        if not s:
            raise RuntimeError(f"{name} must not be empty (STRICT)")
        return s
    raise RuntimeError(f"{name} must be datetime or str (STRICT), got={type(value).__name__}")


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


def _optional_nonempty_str(value: Any, name: str) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError(f"{name} must be str or null (STRICT), got={type(value).__name__}")
    s = value.strip()
    if not s:
        raise RuntimeError(f"{name} must not be blank when provided (STRICT)")
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


def _optional_float(value: Any, name: str) -> Optional[float]:
    if value is None:
        return None
    return _require_float(value, name)


def _require_str_list(value: Any, name: str) -> List[str]:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if not isinstance(value, list):
        raise RuntimeError(f"{name} must be list[str] (STRICT), got={type(value).__name__}")
    if not value:
        raise RuntimeError(f"{name} must not be empty (STRICT)")

    out: List[str] = []
    for idx, item in enumerate(value):
        out.append(_require_nonempty_str(item, f"{name}[{idx}]"))
    return out


def _require_mapping(value: Any, name: str) -> Mapping[str, Any]:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if not isinstance(value, dict):
        raise RuntimeError(f"{name} must be dict (STRICT), got={type(value).__name__}")
    return value


def _normalize_action(value: Any) -> str:
    action = _require_nonempty_str(value, "extra_json.action").upper()
    if action not in _ALLOWED_ACTIONS:
        raise RuntimeError(f"unsupported DECISION action (STRICT): {action!r}")
    return action


def _normalize_row(row: Mapping[str, Any]) -> DecisionRecord:
    row_id = _require_int(row.get("id"), "bt_events.id")
    ts_utc = _to_iso(row.get("ts_utc"), "bt_events.ts_utc")
    symbol = _require_nonempty_str(row.get("symbol"), "bt_events.symbol")
    reason_code = _require_nonempty_str(row.get("reason"), "bt_events.reason")

    extra = _require_mapping(row.get("extra_json"), "bt_events.extra_json")

    action = _normalize_action(extra.get("action"))
    summary = _require_nonempty_str(extra.get("summary"), "extra_json.summary")
    reasons = _require_str_list(extra.get("reasons"), "extra_json.reasons")
    entry_score = _require_float(extra.get("entry_score"), "extra_json.entry_score")
    trend_strength = _require_float(extra.get("trend_strength"), "extra_json.trend_strength")
    spread = _require_float(extra.get("spread"), "extra_json.spread")
    orderbook_imbalance = _require_float(extra.get("orderbook_imbalance"), "extra_json.orderbook_imbalance")

    regime = _optional_nonempty_str(row.get("regime"), "bt_events.regime")
    side = _optional_nonempty_str(row.get("side"), "bt_events.side")

    threshold = _optional_float(extra.get("threshold"), "extra_json.threshold")
    exit_score = _optional_float(extra.get("exit_score"), "extra_json.exit_score")
    current_price = _optional_float(extra.get("current_price"), "extra_json.current_price")
    entry_price = _optional_float(extra.get("entry_price"), "extra_json.entry_price")
    target_price = _optional_float(extra.get("target_price"), "extra_json.target_price")
    stop_price = _optional_float(extra.get("stop_price"), "extra_json.stop_price")
    pnl_pct = _optional_float(extra.get("pnl_pct"), "extra_json.pnl_pct")
    position_qty = _optional_float(extra.get("position_qty"), "extra_json.position_qty")
    signal_source = _optional_nonempty_str(extra.get("signal_source"), "extra_json.signal_source")

    return DecisionRecord(
        id=row_id,
        ts_utc=ts_utc,
        symbol=symbol,
        action=action,
        reason_code=reason_code,
        summary=summary,
        reasons=reasons,
        entry_score=entry_score,
        trend_strength=trend_strength,
        spread=spread,
        orderbook_imbalance=orderbook_imbalance,
        regime=regime,
        side=side,
        threshold=threshold,
        exit_score=exit_score,
        current_price=current_price,
        entry_price=entry_price,
        target_price=target_price,
        stop_price=stop_price,
        pnl_pct=pnl_pct,
        position_qty=position_qty,
        signal_source=signal_source,
    )


def _fetch_decision_rows(db: Session, *, limit: int, include_test: bool) -> Sequence[Mapping[str, Any]]:
    if db is None:
        raise RuntimeError("db session is required (STRICT)")
    sql = text(
        """
        SELECT
            id,
            ts_utc,
            symbol,
            regime,
            side,
            reason,
            extra_json
        FROM bt_events
        WHERE event_type = 'DECISION'
          AND (:include_test = TRUE OR is_test = FALSE)
        ORDER BY id DESC
        LIMIT :limit
        """
    )
    rows = db.execute(
        sql,
        {
            "limit": _require_int(limit, "limit"),
            "include_test": bool(include_test),
        },
    ).mappings().all()

    if not rows:
        raise RuntimeError("DECISION event not found (STRICT)")
    return rows


def get_latest_decision(db: Session, *, include_test: bool = False) -> Dict[str, Any]:
    rows = _fetch_decision_rows(db, limit=1, include_test=include_test)
    record = _normalize_row(rows[0])
    return record.to_dict()


def get_recent_decisions(
    db: Session,
    *,
    limit: int,
    include_test: bool = False,
) -> List[Dict[str, Any]]:
    rows = _fetch_decision_rows(db, limit=limit, include_test=include_test)
    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append(_normalize_row(row).to_dict())
    return out


__all__ = [
    "DecisionRecord",
    "get_latest_decision",
    "get_recent_decisions",
]