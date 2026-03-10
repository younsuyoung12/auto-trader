"""
========================================================
FILE: services/system_monitor.py
ROLE:
- 대시보드/관측 계층에서 프로세스 상태, DB latency,
  최근 WATCHDOG / ERROR 상태를 수집·정규화한다.
- 엔진 실행 초기(아직 WATCHDOG 미발행) 상태를 명시적 INIT로 표현한다.

CORE RESPONSIBILITIES:
- 현재 대시보드 서버 프로세스 상태 수집
- DB latency(SELECT 1 왕복) 측정
- bt_events 기준 최근 WATCHDOG / ERROR 상태 조회
- 대시보드 "엔진 상태" 패널용 시스템 상태 스냅샷 정규화
- 스키마/타입/필수 필드 불일치 즉시 예외 처리

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- psutil 미설치 시 즉시 예외
- WATCHDOG 이벤트 미존재는 대시보드 관측 계층에서 명시적 INIT 상태로 반환
- WATCHDOG 이벤트가 존재하는데 필드/타입/스키마가 불일치하면 즉시 예외
- extra_json은 Mapping(dict 계열) 이어야 하며, 이벤트 존재 시 null 금지
- 숫자형 필드는 finite float/int 아니면 즉시 예외
- 임의 기본값/자동 보정/오류 은닉 금지

CHANGE HISTORY:
- 2026-03-11
  1) FIX(ROOT-CAUSE): WATCHDOG 미존재를 RuntimeError가 아닌 명시적 INIT 상태로 모델링
  2) FIX(ARCH): 엔진 STRICT와 대시보드 관측 계층의 초기 상태를 분리
  3) FIX(CONTRACT): WATCHDOG snapshot에 status 필드(OK / INIT) 추가
  4) FIX(STRICT): 이벤트가 존재하는 경우 필드/타입 검증은 기존처럼 fail-fast 유지
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


_WATCHDOG_STATUS_OK = "OK"
_WATCHDOG_STATUS_INIT = "INIT"
_WATCHDOG_INIT_REASON_NO_EVENT = "no_watchdog_event"


@dataclass(frozen=True, slots=True)
class WatchdogSnapshot:
    status: str
    reason: str
    ts_utc: Optional[str]
    symbol: Optional[str]
    regime: Optional[str]
    source: Optional[str]
    side: Optional[str]
    extra_json: Dict[str, Any]
    id: Optional[int]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "reason": self.reason,
            "ts_utc": self.ts_utc,
            "symbol": self.symbol,
            "regime": self.regime,
            "source": self.source,
            "side": self.side,
            "extra_json": dict(self.extra_json),
            "id": self.id,
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
    if not isinstance(value, Mapping):
        raise RuntimeError(f"{name} must be mapping/dict (STRICT), got={type(value).__name__}")
    return dict(value)


def _require_watchdog_status(value: Any, name: str) -> str:
    status = _require_nonempty_str(value, name)
    if status not in {_WATCHDOG_STATUS_OK, _WATCHDOG_STATUS_INIT}:
        raise RuntimeError(f"{name} invalid watchdog status (STRICT): {status!r}")
    return status


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


def _build_init_watchdog_snapshot() -> WatchdogSnapshot:
    return WatchdogSnapshot(
        status=_WATCHDOG_STATUS_INIT,
        reason=_WATCHDOG_INIT_REASON_NO_EVENT,
        ts_utc=None,
        symbol=None,
        regime=None,
        source=None,
        side=None,
        extra_json={},
        id=None,
    )


def _normalize_watchdog_row(row: Mapping[str, Any]) -> WatchdogSnapshot:
    row_id = _require_positive_int(row.get("id"), "bt_events.id")
    ts_utc = _to_iso(row.get("ts_utc"), "bt_events.ts_utc")
    symbol = _require_nonempty_str(row.get("symbol"), "bt_events.symbol")
    reason = _require_nonempty_str(row.get("reason"), "bt_events.reason")
    regime = _optional_nonempty_str(row.get("regime"), "bt_events.regime")
    source = _optional_nonempty_str(row.get("source"), "bt_events.source")
    side = _optional_nonempty_str(row.get("side"), "bt_events.side")
    extra_json = _require_mapping(row.get("extra_json"), "bt_events.extra_json")

    snapshot = WatchdogSnapshot(
        status=_WATCHDOG_STATUS_OK,
        reason=reason,
        ts_utc=ts_utc,
        symbol=symbol,
        regime=regime,
        source=source,
        side=side,
        extra_json=extra_json,
        id=row_id,
    )
    _require_watchdog_status(snapshot.status, "watchdog.status")
    return snapshot


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
        return _build_init_watchdog_snapshot().to_dict()

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
    _require_watchdog_status(latest_watchdog.get("status"), "latest_watchdog.status")
    _require_nonempty_str(latest_watchdog.get("reason"), "latest_watchdog.reason")

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