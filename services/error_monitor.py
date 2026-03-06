"""
========================================================
FILE: services/error_monitor.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할
--------------------------------------------------------
- bt_events 테이블의 ERROR / WATCHDOG 이벤트를 조회한다.
- 대시보드 "실시간 에러" 패널에 필요한 구조로 정규화한다.
- 잘못된 event payload / 누락 필드 / 비정상 타입은 즉시 예외로 실패한다.

지원 기능
--------------------------------------------------------
- 최신 ERROR / WATCHDOG 1건 조회
- 최근 ERROR / WATCHDOG 목록 조회
- event_type별 최근 건수 조회

허용 event_type
--------------------------------------------------------
- ERROR
- WATCHDOG

절대 원칙 (STRICT · NO-FALLBACK)
--------------------------------------------------------
- "이벤트가 없음" 자체는 정상 상태로 취급한다.
  - latest: None
  - recent list: []
  - counts: {"ERROR": 0, "WATCHDOG": 0}
- extra_json은 dict 또는 null 만 허용한다.
- 필수 컬럼 누락 시 즉시 예외.
- 숫자형 필드는 finite float/int 아니면 즉시 예외.
- 임의 기본값/더미값/자동 보정 금지.

변경 이력
--------------------------------------------------------
- 2026-03-06:
  1) 신규 생성: ERROR / WATCHDOG 이벤트 조회 서비스 추가
  2) 최신 이벤트 / 최근 이벤트 목록 / 최근 건수 조회 기능 추가
  3) 대시보드용 엄격 스키마 정규화 추가
- 2026-03-06:
  1) "이벤트 없음"을 정상 상태로 분리
     - get_latest_error_event() -> None 허용
     - get_recent_error_events() -> [] 반환
     - get_recent_error_counts() -> zero-count dict 반환
  2) 선택 문자열(regime/source/side) null-equivalent 정규화 추가
     - null 또는 blank string -> None
     - 비문자열 타입은 즉시 예외
  3) 대시보드 500 원인이던 과잉 STRICT 제거
     - 데이터 부재와 데이터 손상을 구분
========================================================
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session


_ALLOWED_EVENT_TYPES: Tuple[str, ...] = ("ERROR", "WATCHDOG")


@dataclass(frozen=True, slots=True)
class ErrorEventRecord:
    id: int
    ts_utc: str
    event_type: str
    symbol: str
    regime: Optional[str]
    source: Optional[str]
    side: Optional[str]
    reason: str
    extra_json: Optional[Dict[str, Any]]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "ts_utc": self.ts_utc,
            "event_type": self.event_type,
            "symbol": self.symbol,
            "regime": self.regime,
            "source": self.source,
            "side": self.side,
            "reason": self.reason,
            "extra_json": None if self.extra_json is None else dict(self.extra_json),
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


def _optional_nonempty_str(value: Any, name: str) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError(f"{name} must be str or null (STRICT), got={type(value).__name__}")
    s = value.strip()
    if not s:
        return None
    return s


def _to_iso(value: Any, name: str) -> str:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if isinstance(value, datetime):
        if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
            raise RuntimeError(f"{name} must be timezone-aware datetime (STRICT)")
        return value.isoformat()
    if isinstance(value, str):
        s = value.strip()
        if not s:
            raise RuntimeError(f"{name} must not be empty (STRICT)")
        return s
    raise RuntimeError(f"{name} must be datetime or str (STRICT), got={type(value).__name__}")


def _require_mapping_or_none(value: Any, name: str) -> Optional[Dict[str, Any]]:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise RuntimeError(f"{name} must be dict or null (STRICT), got={type(value).__name__}")
    return dict(value)


def _require_event_type(value: Any) -> str:
    event_type = _require_nonempty_str(value, "bt_events.event_type").upper()
    if event_type not in _ALLOWED_EVENT_TYPES:
        raise RuntimeError(f"unsupported event_type (STRICT): {event_type!r}")
    return event_type


def _require_limit(value: Any) -> int:
    limit = _require_int(value, "limit")
    if limit > 1000:
        raise RuntimeError("limit must be <= 1000 (STRICT)")
    return limit


def _require_minutes(value: Any) -> int:
    minutes = _require_int(value, "minutes")
    if minutes > 10080:
        raise RuntimeError("minutes must be <= 10080 (STRICT)")
    return minutes


def _normalize_row(row: Mapping[str, Any]) -> ErrorEventRecord:
    row_id = _require_int(row.get("id"), "bt_events.id")
    ts_utc = _to_iso(row.get("ts_utc"), "bt_events.ts_utc")
    event_type = _require_event_type(row.get("event_type"))
    symbol = _require_nonempty_str(row.get("symbol"), "bt_events.symbol")
    regime = _optional_nonempty_str(row.get("regime"), "bt_events.regime")
    source = _optional_nonempty_str(row.get("source"), "bt_events.source")
    side = _optional_nonempty_str(row.get("side"), "bt_events.side")
    reason = _require_nonempty_str(row.get("reason"), "bt_events.reason")
    extra_json = _require_mapping_or_none(row.get("extra_json"), "bt_events.extra_json")

    return ErrorEventRecord(
        id=row_id,
        ts_utc=ts_utc,
        event_type=event_type,
        symbol=symbol,
        regime=regime,
        source=source,
        side=side,
        reason=reason,
        extra_json=extra_json,
    )


def _normalize_event_type_filter(event_type: Optional[str]) -> Optional[str]:
    if event_type is None:
        return None
    normalized = _require_nonempty_str(event_type, "event_type").upper()
    if normalized not in _ALLOWED_EVENT_TYPES:
        raise RuntimeError(f"unsupported event_type filter (STRICT): {normalized!r}")
    return normalized


def _fetch_event_rows(
    db: Session,
    *,
    limit: int,
    include_test: bool,
    event_type: Optional[str],
) -> Sequence[Mapping[str, Any]]:
    if db is None:
        raise RuntimeError("db session is required (STRICT)")

    normalized_limit = _require_limit(limit)
    normalized_event_type = _normalize_event_type_filter(event_type)

    if normalized_event_type is None:
        sql = text(
            """
            SELECT
                id,
                ts_utc,
                event_type,
                symbol,
                regime,
                source,
                side,
                reason,
                extra_json
            FROM bt_events
            WHERE event_type IN ('ERROR', 'WATCHDOG')
              AND (:include_test = TRUE OR is_test = FALSE)
            ORDER BY id DESC
            LIMIT :limit
            """
        )
        params = {
            "limit": normalized_limit,
            "include_test": bool(include_test),
        }
    else:
        sql = text(
            """
            SELECT
                id,
                ts_utc,
                event_type,
                symbol,
                regime,
                source,
                side,
                reason,
                extra_json
            FROM bt_events
            WHERE event_type = :event_type
              AND (:include_test = TRUE OR is_test = FALSE)
            ORDER BY id DESC
            LIMIT :limit
            """
        )
        params = {
            "limit": normalized_limit,
            "include_test": bool(include_test),
            "event_type": normalized_event_type,
        }

    return db.execute(sql, params).mappings().all()


def get_latest_error_event(
    db: Session,
    *,
    event_type: Optional[str] = None,
    include_test: bool = False,
) -> Optional[Dict[str, Any]]:
    rows = _fetch_event_rows(db, limit=1, include_test=include_test, event_type=event_type)
    if not rows:
        return None
    return _normalize_row(rows[0]).to_dict()


def get_recent_error_events(
    db: Session,
    *,
    limit: int,
    event_type: Optional[str] = None,
    include_test: bool = False,
) -> List[Dict[str, Any]]:
    rows = _fetch_event_rows(db, limit=limit, include_test=include_test, event_type=event_type)
    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append(_normalize_row(row).to_dict())
    return out


def get_recent_error_counts(
    db: Session,
    *,
    minutes: int,
    include_test: bool = False,
) -> Dict[str, int]:
    if db is None:
        raise RuntimeError("db session is required (STRICT)")

    normalized_minutes = _require_minutes(minutes)
    sql = text(
        """
        SELECT
            event_type,
            COUNT(*) AS n
        FROM bt_events
        WHERE event_type IN ('ERROR', 'WATCHDOG')
          AND ts_utc >= now() - (:minutes * INTERVAL '1 minute')
          AND (:include_test = TRUE OR is_test = FALSE)
        GROUP BY event_type
        ORDER BY event_type
        """
    )
    rows = db.execute(
        sql,
        {
            "minutes": normalized_minutes,
            "include_test": bool(include_test),
        },
    ).mappings().all()

    out: Dict[str, int] = {
        "ERROR": 0,
        "WATCHDOG": 0,
    }

    for row in rows:
        event_type = _require_event_type(row.get("event_type"))
        n_raw = row.get("n")
        if n_raw is None:
            raise RuntimeError("COUNT(*) result is required (STRICT)")
        if isinstance(n_raw, bool):
            raise RuntimeError("COUNT(*) result must be int, bool not allowed (STRICT)")
        try:
            n = int(n_raw)
        except Exception as exc:
            raise RuntimeError(f"COUNT(*) result must be int (STRICT): {exc}") from exc
        if n < 0:
            raise RuntimeError("COUNT(*) result must be >= 0 (STRICT)")
        out[event_type] = n

    return out


__all__ = [
    "ErrorEventRecord",
    "get_latest_error_event",
    "get_recent_error_events",
    "get_recent_error_counts",
]