"""
========================================================
FILE: test_dashboard_rootcause.py
STRICT · NO-FALLBACK · TRADE-GRADE DIAGNOSTIC
========================================================
역할:
- 대시보드 500 오류의 "근본 원인"을 정확히 분리 진단한다.
- 읽기 서비스 오류인지, DB 원본 데이터 부재/오염인지, 쓰기 경로 누락인지 구분한다.
- 실제 프로젝트 파일/라인 기준으로 예외 발생 지점을 출력한다.
- POSITION / WATCHDOG / 성과계열 데이터의 존재 여부를 DB 원본으로 직접 확인한다.
- 로컬 코드베이스를 스캔하여 bt_events 직접 INSERT / record_event_db 호출 / POSITION/WATCHDOG 관련 파일을 찾는다.

절대 원칙:
- 폴백 금지
- 예외 삼키지 않되, 진단은 계속 진행
- 민감정보 출력 금지
- 기본 동작은 READ-ONLY
- --write-probe 옵션을 줬을 때만 canary 이벤트를 실제 DB에 기록한다

실행 예:
    python test_dashboard_rootcause.py
    python test_dashboard_rootcause.py --write-probe
========================================================
"""

from __future__ import annotations

import argparse
import importlib
import inspect
import re
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from sqlalchemy import text

from state.db_core import get_session


PROJECT_ROOT = Path(__file__).resolve().parent
SKIP_DIR_NAMES = {
    ".git",
    ".venv",
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "node_modules",
    "dist",
    "build",
}


@dataclass(frozen=True)
class ServiceTarget:
    module_name: str
    func_name: str
    kwargs: dict[str, Any]


@dataclass(frozen=True)
class ScanPattern:
    name: str
    regex: re.Pattern[str]
    max_hits: int = 20


SERVICE_TARGETS: tuple[ServiceTarget, ...] = (
    ServiceTarget(
        module_name="services.position_service",
        func_name="get_open_position",
        kwargs={"include_test": False},
    ),
    ServiceTarget(
        module_name="services.performance_service",
        func_name="get_performance_summary",
        kwargs={},
    ),
    ServiceTarget(
        module_name="services.performance_service",
        func_name="get_equity_curve",
        kwargs={"limit": 20},
    ),
    ServiceTarget(
        module_name="services.performance_service",
        func_name="get_daily_pnl_series",
        kwargs={"days": 7},
    ),
    ServiceTarget(
        module_name="services.system_monitor",
        func_name="get_latest_watchdog_snapshot",
        kwargs={"include_test": False},
    ),
)

SCAN_PATTERNS: tuple[ScanPattern, ...] = (
    ScanPattern(
        name="STRICT read-side error string: POSITION not found",
        regex=re.compile(r"POSITION event not found \(STRICT\)"),
        max_hits=10,
    ),
    ScanPattern(
        name="STRICT read-side error string: closed trade not found",
        regex=re.compile(r"closed trade not found in v_trades_prod \(STRICT\)"),
        max_hits=10,
    ),
    ScanPattern(
        name="STRICT read-side error string: daily pnl series not found",
        regex=re.compile(r"daily pnl series not found \(STRICT\)"),
        max_hits=10,
    ),
    ScanPattern(
        name="STRICT read-side error string: blank watchdog source",
        regex=re.compile(r"bt_events\.source must not be blank when provided \(STRICT\)"),
        max_hits=10,
    ),
    ScanPattern(
        name="event_store 호출 지점",
        regex=re.compile(r"\brecord_event_db\s*\("),
        max_hits=50,
    ),
    ScanPattern(
        name="bt_events 직접 INSERT 지점",
        regex=re.compile(r"INSERT\s+INTO\s+bt_events", re.IGNORECASE),
        max_hits=50,
    ),
    ScanPattern(
        name="POSITION 문자열 포함",
        regex=re.compile(r"""['"]POSITION['"]"""),
        max_hits=50,
    ),
    ScanPattern(
        name="WATCHDOG 문자열 포함",
        regex=re.compile(r"""['"]WATCHDOG['"]"""),
        max_hits=50,
    ),
    ScanPattern(
        name="빈 source 할당 의심",
        regex=re.compile(r"""\bsource\s*=\s*['"]\s*['"]"""),
        max_hits=50,
    ),
)


def _print_header(title: str) -> None:
    print()
    print("=" * 90)
    print(title)
    print("=" * 90)


def _format_value(value: Any, max_len: int = 220) -> str:
    s = repr(value)
    if len(s) > max_len:
        return s[: max_len - 3] + "..."
    return s


def _safe_mapping_row(row: Any) -> dict[str, Any]:
    try:
        return dict(row._mapping)
    except Exception:
        try:
            return dict(row)
        except Exception:
            return {"value": repr(row)}


def _normalize_rows(rows: Iterable[Any], limit: int = 10) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for idx, row in enumerate(rows):
        if idx >= limit:
            break
        out.append(_safe_mapping_row(row))
    return out


def _project_rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except Exception:
        return str(path)


def _extract_project_frame(exc: BaseException) -> Optional[str]:
    tb = exc.__traceback__
    if tb is None:
        return None

    extracted = traceback.extract_tb(tb)
    for frame in reversed(extracted):
        frame_path = Path(frame.filename)
        try:
            frame_path.resolve().relative_to(PROJECT_ROOT.resolve())
        except Exception:
            continue
        return f"{_project_rel(frame_path)}:{frame.lineno} in {frame.name}"
    return None


def _import_optional(module_name: str) -> tuple[Optional[Any], Optional[str]]:
    try:
        return importlib.import_module(module_name), None
    except Exception as exc:
        msg = f"{type(exc).__name__}: {exc}"
        origin = _extract_project_frame(exc)
        if origin:
            msg = f"{msg} @ {origin}"
        return None, msg


def _call_service_target(target: ServiceTarget) -> None:
    module, import_err = _import_optional(target.module_name)
    if module is None:
        print(f"[IMPORT FAIL] {target.module_name}: {import_err}")
        return

    func = getattr(module, target.func_name, None)
    if func is None:
        print(f"[MISSING FUNC] {target.module_name}.{target.func_name}")
        return

    call_kwargs: dict[str, Any] = {}
    sig = inspect.signature(func)
    for name in sig.parameters:
        if name == "db":
            continue
        if name in target.kwargs:
            call_kwargs[name] = target.kwargs[name]

    with get_session() as db:
        try:
            result = func(db, **call_kwargs)
            print(f"[OK] {target.module_name}.{target.func_name}")
            print(f"  result={_format_value(result)}")
        except Exception as exc:
            origin = _extract_project_frame(exc)
            print(f"[FAIL] {target.module_name}.{target.func_name}")
            print(f"  error={type(exc).__name__}: {exc}")
            if origin:
                print(f"  origin={origin}")


def run_service_probes() -> None:
    _print_header("1. READ-SIDE SERVICE PROBES")
    for target in SERVICE_TARGETS:
        _call_service_target(target)


def _query_scalar(sql: str, params: Optional[dict[str, Any]] = None) -> Any:
    with get_session() as db:
        return db.execute(text(sql), params or {}).scalar()


def _query_rows(sql: str, params: Optional[dict[str, Any]] = None, limit: int = 10) -> list[dict[str, Any]]:
    with get_session() as db:
        rows = db.execute(text(sql), params or {}).fetchall()
    return _normalize_rows(rows, limit=limit)


def run_db_probes() -> None:
    _print_header("2. DB RAW PROBES")

    probes: tuple[tuple[str, str, Optional[dict[str, Any]]], ...] = (
        (
            "bt_events event_type 집계",
            """
            SELECT event_type, COUNT(*) AS cnt
            FROM bt_events
            GROUP BY event_type
            ORDER BY event_type
            """,
            None,
        ),
        (
            "POSITION 최근 10건",
            """
            SELECT id, event_type, symbol, source, side, reason, ts_utc
            FROM bt_events
            WHERE event_type = 'POSITION'
            ORDER BY id DESC
            LIMIT 10
            """,
            None,
        ),
        (
            "WATCHDOG 최근 10건",
            """
            SELECT id, event_type, symbol, source, side, reason, ts_utc
            FROM bt_events
            WHERE event_type = 'WATCHDOG'
            ORDER BY id DESC
            LIMIT 10
            """,
            None,
        ),
        (
            "빈 source WATCHDOG 개수",
            """
            SELECT COUNT(*) AS cnt
            FROM bt_events
            WHERE event_type = 'WATCHDOG'
              AND source IS NOT NULL
              AND BTRIM(source) = ''
            """,
            None,
        ),
        (
            "v_trades_prod 전체 개수",
            """
            SELECT COUNT(*) AS cnt
            FROM v_trades_prod
            """,
            None,
        ),
        (
            "v_trades_prod 최근 10건",
            """
            SELECT *
            FROM v_trades_prod
            ORDER BY COALESCE(closed_at, opened_at) DESC NULLS LAST
            LIMIT 10
            """,
            None,
        ),
    )

    for title, sql, params in probes:
        print()
        print(f"[DB] {title}")
        try:
            rows = _query_rows(sql, params=params, limit=10)
            if not rows:
                print("  rows=[]")
            else:
                for row in rows:
                    print(f"  {row}")
        except Exception as exc:
            origin = _extract_project_frame(exc)
            print(f"  FAIL: {type(exc).__name__}: {exc}")
            if origin:
                print(f"  origin={origin}")

    print()
    print("[DB] 핵심 단건 체크")
    scalar_checks: tuple[tuple[str, str], ...] = (
        (
            "POSITION row count",
            "SELECT COUNT(*) FROM bt_events WHERE event_type = 'POSITION'",
        ),
        (
            "WATCHDOG blank source count",
            """
            SELECT COUNT(*)
            FROM bt_events
            WHERE event_type = 'WATCHDOG'
              AND source IS NOT NULL
              AND BTRIM(source) = ''
            """,
        ),
        (
            "closed trade count",
            "SELECT COUNT(*) FROM v_trades_prod",
        ),
    )
    for label, sql in scalar_checks:
        try:
            value = _query_scalar(sql)
            print(f"  {label}: {value}")
        except Exception as exc:
            print(f"  {label}: FAIL {type(exc).__name__}: {exc}")


def _iter_python_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*.py"):
        if any(part in SKIP_DIR_NAMES for part in path.parts):
            continue
        yield path


def run_static_code_scan() -> None:
    _print_header("3. STATIC CODE SCAN")

    for pattern in SCAN_PATTERNS:
        print()
        print(f"[SCAN] {pattern.name}")
        hit_count = 0

        for py_file in _iter_python_files(PROJECT_ROOT):
            try:
                content = py_file.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                try:
                    content = py_file.read_text(encoding="utf-8-sig")
                except Exception:
                    continue
            except Exception:
                continue

            lines = content.splitlines()
            for idx, line in enumerate(lines, start=1):
                if pattern.regex.search(line):
                    rel = _project_rel(py_file)
                    snippet = line.strip()
                    print(f"  {rel}:{idx}: {snippet}")
                    hit_count += 1
                    if hit_count >= pattern.max_hits:
                        break
            if hit_count >= pattern.max_hits:
                break

        if hit_count == 0:
            print("  no hits")


def run_event_store_probe(write_probe: bool) -> None:
    _print_header("4. EVENT STORE PROBE")

    module, import_err = _import_optional("events.event_store")
    if module is None:
        print(f"[IMPORT FAIL] events.event_store: {import_err}")
        return

    record_fn = getattr(module, "record_event_db", None)
    if record_fn is None or not callable(record_fn):
        print("[MISSING FUNC] events.event_store.record_event_db")
        return

    print("[OK] events.event_store.record_event_db import success")

    if not write_probe:
        print("write probe skipped (default read-only mode)")
        print("실제 INSERT 확인이 필요하면 --write-probe 로 실행")
        return

    print("write probe enabled")

    now_utc = datetime.now(timezone.utc)
    canary_reason = "ROOTCAUSE_PROBE_CANARY_DO_NOT_USE"

    payloads: tuple[dict[str, Any], ...] = (
        {
            "ts_utc": now_utc,
            "event_type": "WATCHDOG",
            "symbol": "BTCUSDT",
            "regime": "PROBE",
            "source": "",
            "side": "CLOSE",
            "reason": canary_reason,
            "extra_json": {"probe": "watchdog_blank_source"},
            "is_test": True,
        },
        {
            "ts_utc": now_utc,
            "event_type": "POSITION",
            "symbol": "BTCUSDT",
            "regime": "PROBE",
            "source": "probe",
            "side": "LONG",
            "price": 1.0,
            "qty": 0.001,
            "reason": canary_reason,
            "extra_json": {"probe": "position_insert"},
            "is_test": True,
        },
    )

    for idx, payload in enumerate(payloads, start=1):
        try:
            record_fn(**payload)
            print(f"[WRITE OK] payload#{idx} event_type={payload['event_type']}")
        except Exception as exc:
            origin = _extract_project_frame(exc)
            print(f"[WRITE FAIL] payload#{idx} event_type={payload['event_type']}")
            print(f"  error={type(exc).__name__}: {exc}")
            if origin:
                print(f"  origin={origin}")

    print()
    print("[VERIFY] canary rows")
    try:
        rows = _query_rows(
            """
            SELECT id, event_type, symbol, source, side, reason, is_test, ts_utc
            FROM bt_events
            WHERE reason = :reason
            ORDER BY id DESC
            LIMIT 20
            """,
            params={"reason": canary_reason},
            limit=20,
        )
        if not rows:
            print("  rows=[]")
        else:
            for row in rows:
                print(f"  {row}")
    except Exception as exc:
        print(f"  FAIL: {type(exc).__name__}: {exc}")


def run_summary() -> None:
    _print_header("5. FINAL SUMMARY")

    try:
        position_count = int(_query_scalar("SELECT COUNT(*) FROM bt_events WHERE event_type = 'POSITION'") or 0)
    except Exception:
        position_count = -1

    try:
        blank_watchdog_source_count = int(
            _query_scalar(
                """
                SELECT COUNT(*)
                FROM bt_events
                WHERE event_type = 'WATCHDOG'
                  AND source IS NOT NULL
                  AND BTRIM(source) = ''
                """
            )
            or 0
        )
    except Exception:
        blank_watchdog_source_count = -1

    try:
        closed_trade_count = int(_query_scalar("SELECT COUNT(*) FROM v_trades_prod") or 0)
    except Exception:
        closed_trade_count = -1

    print(f"POSITION rows: {position_count}")
    print(f"WATCHDOG blank source rows: {blank_watchdog_source_count}")
    print(f"closed trades in v_trades_prod: {closed_trade_count}")
    print()
    print("판정 기준:")
    print("- POSITION rows == 0 이면 position_service 쪽 500은 읽기 버그가 아니라 쓰기 누락 가능성이 높음")
    print("- WATCHDOG blank source rows > 0 이면 system_monitor 쪽 500은 읽기 버그가 아니라 DB 오염/잘못된 쓰기 경로 가능성이 높음")
    print("- closed trades == 0 이면 performance 계열 500은 성과 계산 로직 문제가 아니라 데이터 부재 가능성이 높음")
    print("- static scan 에서 'INSERT INTO bt_events' 가 event_store.py 외 다른 파일에도 나오면 우회 쓰기 경로를 의심")


def main() -> int:
    parser = argparse.ArgumentParser(description="Dashboard root cause diagnostic")
    parser.add_argument(
        "--write-probe",
        action="store_true",
        help="canary 이벤트를 실제 DB에 기록해 event_store 쓰기 경로를 검증",
    )
    args = parser.parse_args()

    print("ROOT:", PROJECT_ROOT)
    print("PYTHON:", sys.executable)

    run_service_probes()
    run_db_probes()
    run_static_code_scan()
    run_event_store_probe(write_probe=args.write_probe)
    run_summary()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())