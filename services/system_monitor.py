"""
========================================================
FILE: services/system_monitor.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할
--------------------------------------------------------
- 현재 대시보드 서버 프로세스 상태를 수집한다.
- DB latency(SELECT 1 왕복)를 측정한다.
- bt_events 기준 최근 WATCHDOG / ERROR 상태를 조회한다.
- 대시보드 "엔진 상태" 패널에 필요한 구조로 정규화한다.
- 잘못된 컬럼 / 누락 필드 / 비정상 타입은 즉시 예외로 실패한다.

지원 기능
--------------------------------------------------------
- 시스템 상태 스냅샷 조회
- 최근 WATCHDOG 이벤트 조회
- 최근 ERROR 건수 조회

데이터 소스
--------------------------------------------------------
- 로컬 프로세스: psutil
- DB: bt_events
- DB latency: SELECT 1

절대 원칙 (STRICT · NO-FALLBACK)
--------------------------------------------------------
- psutil 미설치 시 즉시 예외.
- WATCHDOG 이벤트가 없으면 즉시 예외.
- extra_json은 dict 여야 한다.
- 필수 컬럼 누락 시 즉시 예외.
- 숫자형 필드는 finite float/int 아니면 즉시 예외.
- 임의 기본값/더미값/자동 보정 금지.

변경 이력
--------------------------------------------------------
- 2026-03-06:
  1) 신규 생성: 시스템 모니터 서비스 추가
  2) 프로세스/메모리/CPU/DB latency/WATCHDOG 상태 조회 기능 추가
  3) 최근 ERROR 건수 및 최신 WATCHDOG 상세 조회 기능 추가
========================================================
"""

from __future__ import annotations

import math
import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Mapping, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

try:
    import psutil
except Exception as exc:  # pragma: no cover
    psutil = None  # type: ignore[assignment]
    _PSUTIL_IMPORT_ERROR = exc
else:
    _PSUTIL_IMPORT_ERROR = None


@dataclass(frozen=True, slots=True)
class WatchdogSnapshot:
    id: int
    ts_utc: str
    symbol: str
    reason: str
    regime: Optional[str]
    source: Optional[str]
    side: Optional[str]
    extra_json: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "ts_utc": self.ts_utc,
            "symbol": self.symbol,
            "reason": self.reason,
            "regime": self.regime,
            "source": self.source,
            "side": self.side,
            "extra_json": dict(self.extra_json),
        }


@dataclass(frozen=True, slots=True)
class SystemStatusSnapshot:
    ts_ms: int
    pid: int
    process_name: str
    process_uptime_sec: float
    cpu_percent: float
    memory_rss_bytes: int
    memory_percent: float
    thread_count: int
    db_latency_ms: int
    recent_error_count: int
    latest_watchdog: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ts_ms": self.ts_ms,
            "pid": self.pid,
            "process_name": self.process_name,
            "process_uptime_sec": self.process_uptime_sec,
            "cpu_percent": self.cpu_percent,
            "memory_rss_bytes": self.memory_rss_bytes,
            "memory_percent": self.memory_percent,
            "thread_count": self.thread_count,
            "db_latency_ms": self.db_latency_ms,
            "recent_error_count": self.recent_error_count,
            "latest_watchdog": dict(self.latest_watchdog),
        }


def _require_psutil() -> Any:
    if psutil is None:
        raise RuntimeError(f"psutil is required (STRICT): {_PSUTIL_IMPORT_ERROR}")
    return psutil


def _require_int(value: Any, name: str) -> int:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if isinstance(value, bool):
        raise RuntimeError(f"{name} must be int, bool not allowed (STRICT)")
    try:
        iv = int(value)
    except Exception as exc:
        raise RuntimeError(f"{name} must be int (STRICT): {exc}") from exc
    return iv


def _require_positive_int(value: Any, name: str) -> int:
    iv = _require_int(value, name)
    if iv <= 0:
        raise RuntimeError(f"{name} must be > 0 (STRICT)")
    return iv


def _require_nonnegative_int(value: Any, name: str) -> int:
    iv = _require_int(value, name)
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


def _require_nonnegative_float(value: Any, name: str) -> float:
    fv = _require_float(value, name)
    if fv < 0.0:
        raise RuntimeError(f"{name} must be >= 0 (STRICT)")
    return fv


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


def _require_mapping(value: Any, name: str) -> Dict[str, Any]:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if not isinstance(value, dict):
        raise RuntimeError(f"{name} must be dict (STRICT), got={type(value).__name__}")
    return dict(value)


def _db_latency_ms(db: Session) -> int:
    if db is None:
        raise RuntimeError("db session is required (STRICT)")

    t0 = time.perf_counter()
    v = db.execute(text("SELECT 1")).scalar()
    if v != 1:
        raise RuntimeError("SELECT 1 failed (STRICT)")
    latency_ms = int((time.perf_counter() - t0) * 1000.0)
    if latency_ms < 0:
        raise RuntimeError("db latency must be >= 0 (STRICT)")
    return latency_ms


def _normalize_watchdog_row(row: Mapping[str, Any]) -> WatchdogSnapshot:
    row_id = _require_positive_int(row.get("id"), "bt_events.id")
    ts_utc = _to_iso(row.get("ts_utc"), "bt_events.ts_utc")
    symbol = _require_nonempty_str(row.get("symbol"), "bt_events.symbol")
    reason = _require_nonempty_str(row.get("reason"), "bt_events.reason")
    regime = _optional_nonempty_str(row.get("regime"), "bt_events.regime")
    source = _optional_nonempty_str(row.get("source"), "bt_events.source")
    side = _optional_nonempty_str(row.get("side"), "bt_events.side")
    extra_json = _require_mapping(row.get("extra_json"), "bt_events.extra_json")

    return WatchdogSnapshot(
        id=row_id,
        ts_utc=ts_utc,
        symbol=symbol,
        reason=reason,
        regime=regime,
        source=source,
        side=side,
        extra_json=extra_json,
    )


def get_latest_watchdog_snapshot(db: Session, *, include_test: bool = False) -> Dict[str, Any]:
    if db is None:
        raise RuntimeError("db session is required (STRICT)")

    sql = text(
        """
        SELECT
            id,
            ts_utc,
            symbol,
            regime,
            source,
            side,
            reason,
            extra_json
        FROM bt_events
        WHERE event_type = 'WATCHDOG'
          AND (:include_test = TRUE OR is_test = FALSE)
        ORDER BY id DESC
        LIMIT 1
        """
    )
    row = db.execute(sql, {"include_test": bool(include_test)}).mappings().one_or_none()
    if row is None:
        raise RuntimeError("WATCHDOG event not found (STRICT)")

    return _normalize_watchdog_row(row).to_dict()


def get_recent_error_count(
    db: Session,
    *,
    minutes: int = 10,
    include_test: bool = False,
) -> int:
    if db is None:
        raise RuntimeError("db session is required (STRICT)")

    normalized_minutes = _require_positive_int(minutes, "minutes")

    sql = text(
        """
        SELECT COUNT(*) AS n
        FROM bt_events
        WHERE event_type = 'ERROR'
          AND ts_utc >= now() - (:minutes * INTERVAL '1 minute')
          AND (:include_test = TRUE OR is_test = FALSE)
        """
    )
    row = db.execute(
        sql,
        {
            "minutes": normalized_minutes,
            "include_test": bool(include_test),
        },
    ).mappings().one_or_none()

    if row is None:
        raise RuntimeError("ERROR count row not found (STRICT)")

    return _require_nonnegative_int(row.get("n"), "COUNT(*)")


def get_system_status_snapshot(
    db: Session,
    *,
    include_test: bool = False,
    error_window_minutes: int = 10,
) -> Dict[str, Any]:
    if db is None:
        raise RuntimeError("db session is required (STRICT)")

    psutil_mod = _require_psutil()
    process = psutil_mod.Process(os.getpid())

    now_ts = time.time()
    now_ms = int(now_ts * 1000)
    if now_ms <= 0:
        raise RuntimeError("ts_ms must be > 0 (STRICT)")

    process_name = _require_nonempty_str(process.name(), "process.name")
    pid = _require_positive_int(process.pid, "process.pid")
    create_time = _require_nonnegative_float(process.create_time(), "process.create_time")
    process_uptime_sec = now_ts - create_time
    if process_uptime_sec < 0.0:
        raise RuntimeError("process_uptime_sec must be >= 0 (STRICT)")

    cpu_percent = _require_nonnegative_float(process.cpu_percent(interval=None), "process.cpu_percent")
    memory_info = process.memory_info()
    memory_rss_bytes = _require_nonnegative_int(getattr(memory_info, "rss", None), "process.memory_info.rss")
    memory_percent = _require_nonnegative_float(process.memory_percent(), "process.memory_percent")
    thread_count = _require_positive_int(process.num_threads(), "process.num_threads")

    db_latency_ms = _db_latency_ms(db)
    recent_error_count = get_recent_error_count(
        db,
        minutes=error_window_minutes,
        include_test=include_test,
    )
    latest_watchdog = get_latest_watchdog_snapshot(db, include_test=include_test)

    snapshot = SystemStatusSnapshot(
        ts_ms=now_ms,
        pid=pid,
        process_name=process_name,
        process_uptime_sec=process_uptime_sec,
        cpu_percent=cpu_percent,
        memory_rss_bytes=memory_rss_bytes,
        memory_percent=memory_percent,
        thread_count=thread_count,
        db_latency_ms=db_latency_ms,
        recent_error_count=recent_error_count,
        latest_watchdog=latest_watchdog,
    )
    return snapshot.to_dict()


__all__ = [
    "WatchdogSnapshot",
    "SystemStatusSnapshot",
    "get_latest_watchdog_snapshot",
    "get_recent_error_count",
    "get_system_status_snapshot",
]