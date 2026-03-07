"""
========================================================
FILE: services/position_service.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할
--------------------------------------------------------
- bt_events 테이블의 POSITION 이벤트를 조회한다.
- 대시보드 "현재 포지션" 패널에 필요한 구조로 정규화한다.
- 잘못된 event payload / 누락 필드 / 비정상 타입은 즉시 예외로 실패한다.
- POSITION 이벤트가 없거나 현재 OPEN 포지션이 없을 때는
  대시보드 가용성을 위해 "NO_POSITION" 상태를 반환한다.

지원 기능
--------------------------------------------------------
- 최신 POSITION 1건 조회
- 최근 POSITION 목록 조회
- 현재 OPEN 포지션 1건 조회
- POSITION 이벤트가 없을 때:
  * latest  -> NO_POSITION payload 반환
  * recent  -> 빈 리스트 반환
  * current -> NO_POSITION payload 반환

POSITION extra_json 필수 스키마
--------------------------------------------------------
{
  "status": "OPEN" | "FLAT" | "CLOSED",
  "entry_price": 62300.0,
  "current_price": 62450.5,
  "pnl_usdt": 12.34,
  "pnl_pct": 0.84,
  "leverage": 3.0,
  "quantity": 0.01
}

POSITION extra_json 선택 스키마
--------------------------------------------------------
{
  "notional_usdt": 624.5,
  "liquidation_price": 58000.0,
  "entry_ts": "2026-03-06T06:35:43+00:00",
  "updated_ts_ms": 1772740500000,
  "strategy": "TREND",
  "reason": "holding",
  "mark_price": 62451.0
}

절대 원칙 (STRICT · NO-FALLBACK)
--------------------------------------------------------
- extra_json이 dict가 아니면 즉시 예외.
- 필수 키 누락 시 즉시 예외.
- 숫자형 필드는 finite float/int 아니면 즉시 예외.
- 임의 기본값/더미값/자동 보정 금지.
- 단, "POSITION 이벤트 없음" / "현재 OPEN 포지션 없음"은
  대시보드 상태 조회 맥락에서 비정상 데이터가 아니라 정상 상태이므로
  예외 대신 NO_POSITION을 반환한다.

변경 이력
--------------------------------------------------------
- 2026-03-07:
  1) POSITION 이벤트 없음 / OPEN 포지션 없음 처리 변경
     - 기존: 즉시 예외(RuntimeError)
     - 변경: NO_POSITION payload 또는 빈 리스트 반환
  2) 대시보드 상태 조회 안정성 강화
     - /api/position/current 에서 포지션 부재를 정상 상태로 처리
  3) STRICT 정책 유지
     - 이벤트 payload 손상 / 필수 키 누락 / 비정상 타입은 여전히 즉시 예외
- 2026-03-06:
  1) 신규 생성: POSITION 이벤트 조회/검증 서비스 추가
  2) 최신 POSITION / 최근 POSITION / 현재 OPEN 포지션 조회 기능 추가
  3) 대시보드용 엄격 스키마 정규화 추가
========================================================
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session


_ALLOWED_POSITION_STATUS: Tuple[str, ...] = ("OPEN", "FLAT", "CLOSED")
_NO_POSITION_STATUS = "NO_POSITION"


@dataclass(frozen=True, slots=True)
class PositionRecord:
    id: int
    ts_utc: str
    symbol: str
    side: str
    status: str
    entry_price: float
    current_price: float
    pnl_usdt: float
    pnl_pct: float
    leverage: float
    quantity: float
    notional_usdt: Optional[float]
    liquidation_price: Optional[float]
    entry_ts: Optional[str]
    updated_ts_ms: Optional[int]
    strategy: Optional[str]
    regime: Optional[str]
    reason: Optional[str]
    mark_price: Optional[float]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "ts_utc": self.ts_utc,
            "symbol": self.symbol,
            "side": self.side,
            "status": self.status,
            "entry_price": self.entry_price,
            "current_price": self.current_price,
            "pnl_usdt": self.pnl_usdt,
            "pnl_pct": self.pnl_pct,
            "leverage": self.leverage,
            "quantity": self.quantity,
            "notional_usdt": self.notional_usdt,
            "liquidation_price": self.liquidation_price,
            "entry_ts": self.entry_ts,
            "updated_ts_ms": self.updated_ts_ms,
            "strategy": self.strategy,
            "regime": self.regime,
            "reason": self.reason,
            "mark_price": self.mark_price,
        }


def _build_no_position_payload(
    *,
    symbol: Optional[str] = None,
    regime: Optional[str] = None,
    ts_utc: Optional[str] = None,
    strategy: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "id": None,
        "ts_utc": ts_utc,
        "symbol": symbol,
        "side": None,
        "status": _NO_POSITION_STATUS,
        "entry_price": None,
        "current_price": None,
        "pnl_usdt": 0.0,
        "pnl_pct": 0.0,
        "leverage": 0.0,
        "quantity": 0.0,
        "notional_usdt": 0.0,
        "liquidation_price": None,
        "entry_ts": None,
        "updated_ts_ms": None,
        "strategy": strategy,
        "regime": regime,
        "reason": "NO_POSITION",
        "mark_price": None,
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


def _optional_int(value: Any, name: str) -> Optional[int]:
    if value is None:
        return None
    return _require_int(value, name)


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


def _optional_iso(value: Any, name: str) -> Optional[str]:
    if value is None:
        return None
    return _to_iso(value, name)


def _require_mapping(value: Any, name: str) -> Mapping[str, Any]:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if not isinstance(value, dict):
        raise RuntimeError(f"{name} must be dict (STRICT), got={type(value).__name__}")
    return value


def _normalize_status(value: Any) -> str:
    status = _require_nonempty_str(value, "extra_json.status").upper()
    if status not in _ALLOWED_POSITION_STATUS:
        raise RuntimeError(f"unsupported POSITION status (STRICT): {status!r}")
    return status


def _normalize_row(row: Mapping[str, Any]) -> PositionRecord:
    row_id = _require_int(row.get("id"), "bt_events.id")
    ts_utc = _to_iso(row.get("ts_utc"), "bt_events.ts_utc")
    symbol = _require_nonempty_str(row.get("symbol"), "bt_events.symbol")
    side = _require_nonempty_str(row.get("side"), "bt_events.side")
    regime = _optional_nonempty_str(row.get("regime"), "bt_events.regime")
    extra = _require_mapping(row.get("extra_json"), "bt_events.extra_json")

    status = _normalize_status(extra.get("status"))
    entry_price = _require_float(extra.get("entry_price"), "extra_json.entry_price")
    current_price = _require_float(extra.get("current_price"), "extra_json.current_price")
    pnl_usdt = _require_float(extra.get("pnl_usdt"), "extra_json.pnl_usdt")
    pnl_pct = _require_float(extra.get("pnl_pct"), "extra_json.pnl_pct")
    leverage = _require_float(extra.get("leverage"), "extra_json.leverage")
    quantity = _require_float(extra.get("quantity"), "extra_json.quantity")

    notional_usdt = _optional_float(extra.get("notional_usdt"), "extra_json.notional_usdt")
    liquidation_price = _optional_float(extra.get("liquidation_price"), "extra_json.liquidation_price")
    entry_ts = _optional_iso(extra.get("entry_ts"), "extra_json.entry_ts")
    updated_ts_ms = _optional_int(extra.get("updated_ts_ms"), "extra_json.updated_ts_ms")
    strategy = _optional_nonempty_str(extra.get("strategy"), "extra_json.strategy")
    reason = _optional_nonempty_str(extra.get("reason"), "extra_json.reason")
    mark_price = _optional_float(extra.get("mark_price"), "extra_json.mark_price")

    return PositionRecord(
        id=row_id,
        ts_utc=ts_utc,
        symbol=symbol,
        side=side,
        status=status,
        entry_price=entry_price,
        current_price=current_price,
        pnl_usdt=pnl_usdt,
        pnl_pct=pnl_pct,
        leverage=leverage,
        quantity=quantity,
        notional_usdt=notional_usdt,
        liquidation_price=liquidation_price,
        entry_ts=entry_ts,
        updated_ts_ms=updated_ts_ms,
        strategy=strategy,
        regime=regime,
        reason=reason,
        mark_price=mark_price,
    )


def _fetch_position_rows(
    db: Session,
    *,
    limit: int,
    include_test: bool,
) -> Sequence[Mapping[str, Any]]:
    if db is None:
        raise RuntimeError("db session is required (STRICT)")

    sql = text(
        """
        SELECT
            id,
            ts_utc,
            symbol,
            side,
            regime,
            extra_json
        FROM bt_events
        WHERE event_type = 'POSITION'
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

    return rows


def get_latest_position(db: Session, *, include_test: bool = False) -> Dict[str, Any]:
    rows = _fetch_position_rows(db, limit=1, include_test=include_test)
    if not rows:
        return _build_no_position_payload()
    record = _normalize_row(rows[0])
    return record.to_dict()


def get_recent_positions(
    db: Session,
    *,
    limit: int,
    include_test: bool = False,
) -> List[Dict[str, Any]]:
    rows = _fetch_position_rows(db, limit=limit, include_test=include_test)
    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append(_normalize_row(row).to_dict())
    return out


def get_open_position(db: Session, *, include_test: bool = False) -> Dict[str, Any]:
    rows = _fetch_position_rows(db, limit=100, include_test=include_test)

    if not rows:
        return _build_no_position_payload()

    latest_record = _normalize_row(rows[0])

    for row in rows:
        record = _normalize_row(row)
        if record.status == "OPEN":
            return record.to_dict()

    return _build_no_position_payload(
        symbol=latest_record.symbol,
        regime=latest_record.regime,
        ts_utc=latest_record.ts_utc,
        strategy=latest_record.strategy,
    )


__all__ = [
    "PositionRecord",
    "get_latest_position",
    "get_recent_positions",
    "get_open_position",
]