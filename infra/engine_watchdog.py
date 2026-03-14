"""
========================================================
FILE: infra/engine_watchdog.py
ROLE:
- 실시간 런타임 진단(Watchdog) 스레드.
- WS / orderbook / DB / rollback 상태를 주기적으로 검사하고,
  결과를 bt_events(event_type="WATCHDOG")에 기록한다.
- market_data_ws 의 health snapshot을 진실값으로 사용하고,
  watchdog 는 진단/기록/콜백 책임만 가진다.

CORE RESPONSIBILITIES:
- market_data_ws health snapshot(strict) 검사 및 이벤트 기록
- orderbook integrity 추가 검증
- kline timestamp rollback 검증
- DB lag / watchdog loop latency 관측
- FAIL/WARNING/OK 상태를 구분하여 bt_events 기록
- 치명 상태 시 on_fatal 콜백 호출

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- settings.py 외 환경변수 직접 접근 금지
- market_data_ws 와 다른 stale 정책을 중복 구현하지 않는다
- transport / payload / warning 판단은 market_data_ws health snapshot을 따른다
- watchdog 는 warning 과 fail 을 구분한다
- 조용한 continue / 예외 삼키기 금지
- 이벤트 기록 실패는 즉시 예외 전파
- thread start 실패 시 started 상태 rollback 보장
- orderbook reconnect / resync / bootstrapping 은 recoverable window 로 취급해야 한다
- watchdog 는 orderbook recovery 판단을 자체 재구현하지 않고
  health snapshot.orderbook.recovery_context 계약을 그대로 따른다

CHANGE HISTORY:
- 2026-03-15:
  1) FIX(ROOT-CAUSE): orderbook snapshot 부재를 즉시 FAIL로 승격하던 문제 수정
  2) FEAT(RECOVERY): health snapshot.orderbook.recovery_context 기반으로 recoverable orderbook 상태를 WARNING으로 분류
  3) FIX(ARCH): watchdog 의 별도 ws/orderbook recovery 재판정 제거 → health snapshot 단일 진실값으로 통일
  4) FIX(OPERABILITY): recoverable orderbook recovery window 에서는 on_fatal 콜백 호출 금지
  5) FEAT(OBSERVABILITY): orderbook_guard_level / orderbook_guard_warning_reason /
     orderbook_recovery_context / orderbook_recovery_context_reason detail 추가
- 2026-03-13:
  1) FIX(STRICT): ws health snapshot parsing 에서 dict.get(..., default) 제거
  2) FIX(OBSERVABILITY): ws_min_kline_buffer 실제 관측 반영(limit=min_buf)
  3) FEAT(WARNING): kline buffer depth 부족 시 warning_kline_buffer_short 분류 추가
  4) FIX(CONTRACT): snapshot/ws/klines/orderbook 구조 계약 검증 강화
  5) FIX(STATE): stop_watchdog() 시 last snapshot 정리 추가
- 2026-03-10:
  1) FIX(ROOT-CAUSE): market_data_ws 와 다른 stale/orderbook 판정 기준 제거
  2) FIX(ARCH): get_health_snapshot() 를 진실값으로 사용하도록 재설계
  3) ADD(OBSERVABILITY): WARNING/FAIL/OK level 및 ws snapshot summary 를 WatchdogSnapshot 에 저장
  4) FIX(STRICT): 숨은 기본값 제거, settings 누락 시 즉시 예외
  5) FIX(CONCURRENCY): watchdog thread start 실패 rollback 추가
  6) FIX(POLICY): WARNING 상태는 이벤트 기록만 하고 on_fatal 호출 금지
- 2026-03-07:
  1) WATCHDOG 이벤트 source 공란 저장 문제 수정
     - _safe_event() 에서 source="engine_watchdog" 명시
========================================================
"""

from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple

from sqlalchemy import text

from infra.telelog import log
from state.db_core import get_session
from events.signals_logger import log_event

from infra.market_data_ws import (
    get_health_snapshot,
    get_klines_with_volume as ws_get_klines_with_volume,
    get_orderbook as ws_get_orderbook,
)
from infra.data_integrity_guard import DataIntegrityError, validate_orderbook_strict


# =============================================================================
# Types
# =============================================================================
WatchdogLevel = Literal["OK", "WARNING", "FAIL"]


@dataclass(frozen=True, slots=True)
class WatchdogSnapshot:
    level: WatchdogLevel
    ok: bool
    has_warning: bool
    checked_at_ms: int

    # WS / market data snapshot summary
    ws_transport_ok: bool
    ws_overall_ok: bool
    ws_overall_reasons: List[str]
    ws_overall_warnings: List[str]

    # WS kline
    kline_len: Dict[str, int]
    kline_last_ts_ms: Dict[str, int]
    kline_age_ms: Dict[str, int]

    # orderbook
    orderbook_ok: bool
    orderbook_ts_ms: Optional[int]
    orderbook_age_ms: Optional[int]

    # DB
    db_ok: bool
    db_ping_ms: Optional[int]

    # latency
    loop_ms: int

    # reason
    fail_reason: Optional[str]
    warning_reason: Optional[str]
    detail: Dict[str, Any]


# =============================================================================
# Globals
# =============================================================================
_WATCHDOG_THREAD: Optional[threading.Thread] = None
_WATCHDOG_STOP: Optional[threading.Event] = None
_LAST_SNAPSHOT: Optional[WatchdogSnapshot] = None

_LAST_STATUS_KEY: str = ""
_LAST_STATUS_TS: float = 0.0
_STATE_LOCK = threading.RLock()


# =============================================================================
# Strict helpers
# =============================================================================
def _now_ms() -> int:
    return int(time.time() * 1000)


def _finite_float(v: Any, name: str) -> float:
    if v is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        raise RuntimeError(f"{name} must be numeric (bool not allowed) (STRICT)")
    try:
        x = float(v)
    except Exception as e:
        raise RuntimeError(f"{name} must be numeric (STRICT): {e}") from e
    if not math.isfinite(x):
        raise RuntimeError(f"{name} must be finite (STRICT): {x!r}")
    return float(x)


def _positive_int(v: Any, name: str) -> int:
    if v is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        raise RuntimeError(f"{name} must be int (bool not allowed) (STRICT)")
    try:
        i = int(v)
    except Exception as e:
        raise RuntimeError(f"{name} must be int (STRICT): {e}") from e
    if i <= 0:
        raise RuntimeError(f"{name} must be > 0 (STRICT)")
    return int(i)


def _require_bool(v: Any, name: str) -> bool:
    if not isinstance(v, bool):
        raise RuntimeError(f"{name} must be bool (STRICT)")
    return bool(v)


def _require_dict(v: Any, name: str) -> Dict[str, Any]:
    if not isinstance(v, dict):
        raise RuntimeError(f"{name} must be dict (STRICT)")
    return v


def _require_list(v: Any, name: str) -> List[Any]:
    if not isinstance(v, list):
        raise RuntimeError(f"{name} must be list (STRICT)")
    return v


def _require_key(d: Dict[str, Any], key: str, owner: str) -> Any:
    if key not in d:
        raise RuntimeError(f"{owner}.{key} is required (STRICT)")
    return d[key]


def _require_setting(settings: Any, attr_name: str) -> Any:
    if not hasattr(settings, attr_name):
        raise RuntimeError(f"settings.{attr_name} is required (STRICT)")
    return getattr(settings, attr_name)


def _require_setting_positive_int(settings: Any, attr_name: str) -> int:
    return _positive_int(_require_setting(settings, attr_name), f"settings.{attr_name}")


def _require_setting_positive_float(settings: Any, attr_name: str) -> float:
    value = _finite_float(_require_setting(settings, attr_name), f"settings.{attr_name}")
    if value <= 0:
        raise RuntimeError(f"settings.{attr_name} must be > 0 (STRICT)")
    return value


def _require_setting_tfs(settings: Any, attr_name: str) -> Tuple[str, ...]:
    raw = _require_setting(settings, attr_name)
    if not isinstance(raw, (list, tuple)) or not raw:
        raise RuntimeError(f"settings.{attr_name} must be non-empty list/tuple (STRICT)")
    out = tuple(str(x).strip() for x in raw if str(x).strip())
    if not out:
        raise RuntimeError(f"settings.{attr_name} normalized empty (STRICT)")
    return out


def _normalize_symbol_strict(symbol: Any, name: str) -> str:
    s = str(symbol or "").replace("-", "").replace("/", "").upper().strip()
    if not s:
        raise RuntimeError(f"{name} is required (STRICT)")
    return s


def _safe_event(symbol: str, reason: str, extra: Dict[str, Any]) -> None:
    """
    STRICT:
    - 이벤트 기록 실패는 감추지 않는다.
    """
    if not isinstance(extra, dict):
        raise RuntimeError("watchdog event extra must be dict (STRICT)")
    log_event(
        event_type="WATCHDOG",
        symbol=str(symbol),
        source="engine_watchdog",
        side="CLOSE",
        reason=str(reason),
        extra_json=dict(extra),
    )


def _should_emit(key: str, *, min_interval_sec: int) -> bool:
    global _LAST_STATUS_KEY, _LAST_STATUS_TS

    now = time.time()
    with _STATE_LOCK:
        if key != _LAST_STATUS_KEY:
            _LAST_STATUS_KEY = key
            _LAST_STATUS_TS = now
            return True
        if (now - _LAST_STATUS_TS) >= float(min_interval_sec):
            _LAST_STATUS_TS = now
            return True
        return False


def _normalize_reason_list(values: Any, name: str) -> List[str]:
    rows = _require_list(values, name)
    return [str(x).strip() for x in rows if str(x).strip()]


def _extract_health_snapshot_strict(snapshot: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    ws = _require_dict(_require_key(snapshot, "ws", "health_snapshot"), "health_snapshot.ws")
    orderbook = _require_dict(
        _require_key(snapshot, "orderbook", "health_snapshot"),
        "health_snapshot.orderbook",
    )
    klines = _require_dict(
        _require_key(snapshot, "klines", "health_snapshot"),
        "health_snapshot.klines",
    )
    return ws, orderbook, klines


def _extract_orderbook_recovery_state_strict(orderbook_snapshot: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    recovery_context = _require_bool(
        _require_key(orderbook_snapshot, "recovery_context", "health_snapshot.orderbook"),
        "health_snapshot.orderbook.recovery_context",
    )

    recovery_context_reason_raw = orderbook_snapshot.get("recovery_context_reason")
    recovery_context_reason: Optional[str] = None
    if recovery_context_reason_raw is not None:
        recovery_context_reason = str(recovery_context_reason_raw).strip()
        if not recovery_context_reason:
            raise RuntimeError(
                "health_snapshot.orderbook.recovery_context_reason must be non-empty when present (STRICT)"
            )

    return recovery_context, recovery_context_reason


# =============================================================================
# Core checks
# =============================================================================
def _check_kline_rollback_strict(
    *,
    symbol: str,
    tfs: Tuple[str, ...],
    min_buf: int,
    last_seen: Dict[str, int],
) -> Tuple[Dict[str, int], Dict[str, int], Dict[str, int], List[str]]:
    """
    rollback + buffer depth 전용 검사.
    stale 판단은 market_data_ws.get_health_snapshot()를 진실값으로 사용한다.
    """
    now_ms = _now_ms()
    lens: Dict[str, int] = {}
    last_ts: Dict[str, int] = {}
    ages: Dict[str, int] = {}
    short_tfs: List[str] = []

    for tf in tfs:
        buf = ws_get_klines_with_volume(symbol, tf, limit=min_buf)
        if not isinstance(buf, list):
            raise RuntimeError(f"ws kline buffer invalid type (STRICT): tf={tf} type={type(buf).__name__}")

        lens[tf] = len(buf)

        if not buf:
            last_ts[tf] = 0
            ages[tf] = -1
            short_tfs.append(tf)
            continue

        if len(buf) < min_buf:
            short_tfs.append(tf)

        row = buf[-1]
        if not isinstance(row, (list, tuple)) or len(row) < 1:
            raise RuntimeError(f"ws kline row invalid shape (STRICT): tf={tf}")

        ts_ms = _positive_int(row[0], f"ws_kline[{tf}].openTime")
        prev = int(last_seen.get(tf, 0))
        if prev > 0 and ts_ms < prev:
            raise RuntimeError(f"kline rollback detected (STRICT): tf={tf} prev={prev} now={ts_ms}")

        last_seen[tf] = int(ts_ms)
        last_ts[tf] = int(ts_ms)
        ages[tf] = int(now_ms - ts_ms)

    return lens, last_ts, ages, short_tfs


def _check_orderbook_integrity_strict(
    *,
    symbol: str,
    orderbook_snapshot: Dict[str, Any],
) -> Tuple[WatchdogLevel, Optional[str], Optional[str], Optional[int], Optional[int], Optional[str]]:
    """
    orderbook integrity 전용 검사.
    stale/transport/recovery 판단은 health snapshot.orderbook 계약을 진실값으로 사용한다.

    returns:
        level, fail_reason, warning_reason, ts_ms, age_ms, recovery_context_reason
    """
    if not isinstance(orderbook_snapshot, dict):
        raise RuntimeError("orderbook_snapshot must be dict (STRICT)")

    recovery_ok, recovery_context_reason = _extract_orderbook_recovery_state_strict(orderbook_snapshot)

    ob = ws_get_orderbook(symbol, limit=5)
    if not isinstance(ob, dict) or not ob:
        if recovery_ok:
            return "WARNING", None, "warning_orderbook_recovery", None, None, recovery_context_reason
        return "FAIL", "orderbook_integrity_fail", None, None, None, None

    ts_ms: Optional[int] = None
    if "exchTs" in ob and ob["exchTs"] is not None:
        ts_ms = _positive_int(ob["exchTs"], "orderbook.exchTs")
    elif "ts" in ob and ob["ts"] is not None:
        ts_ms = _positive_int(ob["ts"], "orderbook.ts")

    try:
        validate_orderbook_strict(ob, symbol=str(symbol), require_ts=bool(ts_ms))
    except DataIntegrityError:
        if recovery_ok:
            return "WARNING", None, "warning_orderbook_recovery", ts_ms, None, recovery_context_reason
        return "FAIL", "orderbook_integrity_fail", None, ts_ms, None, None

    age_ms: Optional[int] = None
    if ts_ms is not None:
        age_ms = _now_ms() - int(ts_ms)

    return "OK", None, None, ts_ms, age_ms, recovery_context_reason


def _check_db_ping_strict(*, max_ping_ms: int) -> Tuple[bool, int]:
    t0 = time.perf_counter()
    with get_session() as session:
        v = session.execute(text("SELECT 1")).scalar()
        if v != 1:
            raise RuntimeError("DB SELECT 1 failed (STRICT)")
    dt = int((time.perf_counter() - t0) * 1000.0)
    if dt < 0:
        dt = 0
    if dt > int(max_ping_ms):
        return False, dt
    return True, dt


def _classify_ws_fail_reason(snapshot: Dict[str, Any]) -> str:
    ws, orderbook, kline_map = _extract_health_snapshot_strict(snapshot)

    transport_ok = _require_bool(
        _require_key(ws, "transport_ok", "health_snapshot.ws"),
        "health_snapshot.ws.transport_ok",
    )
    if not transport_ok:
        return "ws_transport_fail"

    for _, st in kline_map.items():
        st_dict = _require_dict(st, "health_snapshot.klines.item")
        ok = _require_bool(
            _require_key(st_dict, "ok", "health_snapshot.klines.item"),
            "health_snapshot.klines.item.ok",
        )
        if not ok:
            return "ws_kline_stale"

    ob_ok = _require_bool(
        _require_key(orderbook, "ok", "health_snapshot.orderbook"),
        "health_snapshot.orderbook.ok",
    )
    if not ob_ok:
        payload_reasons = _require_key(orderbook, "payload_reasons", "health_snapshot.orderbook")
        if isinstance(payload_reasons, list) and payload_reasons:
            return "orderbook_integrity_fail"
        return "ws_orderbook_stale"

    return "ws_health_fail"


def _check_market_data_health_strict(
    *,
    symbol: str,
) -> Tuple[WatchdogLevel, Optional[str], Optional[str], Dict[str, Any]]:
    """
    market_data_ws health snapshot을 진실값으로 사용한다.

    returns:
        level, fail_reason, warning_reason, snapshot
    """
    snapshot = get_health_snapshot(symbol)
    if not isinstance(snapshot, dict):
        raise RuntimeError("get_health_snapshot returned non-dict (STRICT)")

    overall_ok = _require_bool(
        _require_key(snapshot, "overall_ok", "health_snapshot"),
        "health_snapshot.overall_ok",
    )
    has_warning = _require_bool(
        _require_key(snapshot, "has_warning", "health_snapshot"),
        "health_snapshot.has_warning",
    )

    overall_warnings = _normalize_reason_list(
        _require_key(snapshot, "overall_warnings", "health_snapshot"),
        "health_snapshot.overall_warnings",
    )

    if not overall_ok:
        fail_reason = _classify_ws_fail_reason(snapshot)
        return "FAIL", fail_reason, None, snapshot

    if has_warning:
        if not overall_warnings:
            raise RuntimeError("health snapshot has_warning=True but overall_warnings empty (STRICT)")
        return "WARNING", None, "warning_market_feed", snapshot

    return "OK", None, None, snapshot


# =============================================================================
# Public API
# =============================================================================
def get_last_watchdog_snapshot() -> Optional[WatchdogSnapshot]:
    return _LAST_SNAPSHOT


def stop_watchdog() -> None:
    global _WATCHDOG_STOP, _WATCHDOG_THREAD, _LAST_SNAPSHOT

    stop_evt = _WATCHDOG_STOP
    th = _WATCHDOG_THREAD

    if stop_evt is not None:
        stop_evt.set()

    if th is not None and th.is_alive():
        th.join(timeout=2.0)

    _WATCHDOG_STOP = None
    _WATCHDOG_THREAD = None
    _LAST_SNAPSHOT = None


def start_watchdog(
    *,
    settings: Any,
    symbol: Optional[str] = None,
    on_fatal: Optional[Callable[[str, Dict[str, Any]], None]] = None,
) -> None:
    """
    settings: settings.SETTINGS 또는 load_settings() 결과(SSOT)
    on_fatal: 치명 감지 시 엔진 SAFE_STOP 등을 트리거하기 위한 콜백
              signature: on_fatal(reason: str, detail: dict)

    STRICT:
    - 이미 실행 중이면 예외
    - 숨은 기본값 금지
    """
    global _WATCHDOG_THREAD, _WATCHDOG_STOP

    if _WATCHDOG_THREAD is not None and _WATCHDOG_THREAD.is_alive():
        raise RuntimeError("engine_watchdog already running (STRICT)")

    if settings is None:
        raise RuntimeError("settings is required (STRICT)")

    if symbol is None:
        sym = _normalize_symbol_strict(_require_setting(settings, "symbol"), "settings.symbol")
    else:
        sym = _normalize_symbol_strict(symbol, "symbol")

    interval_sec = _require_setting_positive_float(settings, "engine_watchdog_interval_sec")
    min_buf = _require_setting_positive_int(settings, "ws_min_kline_buffer")
    max_db_ping_ms = _require_setting_positive_int(settings, "engine_watchdog_max_db_ping_ms")
    emit_min_sec = _require_setting_positive_int(settings, "engine_watchdog_emit_min_sec")
    tfs = _require_setting_tfs(settings, "ws_required_tfs")

    stop_evt = threading.Event()
    _WATCHDOG_STOP = stop_evt

    last_seen: Dict[str, int] = {}

    def _loop() -> None:
        global _LAST_SNAPSHOT

        try:
            log(
                f"[WATCHDOG] started symbol={sym} interval_sec={interval_sec} "
                f"tfs={list(tfs)} min_buf={min_buf}"
            )

            while not stop_evt.is_set():
                t_loop0 = time.perf_counter()
                checked_ms = _now_ms()

                ws_level, ws_fail_reason, ws_warning_reason, ws_snapshot = _check_market_data_health_strict(
                    symbol=sym
                )
                ws_dict, orderbook_snapshot, _ = _extract_health_snapshot_strict(ws_snapshot)

                ws_overall_warnings = _normalize_reason_list(
                    _require_key(ws_snapshot, "overall_warnings", "health_snapshot"),
                    "health_snapshot.overall_warnings",
                )

                lens, last_ts, ages, short_tfs = _check_kline_rollback_strict(
                    symbol=sym,
                    tfs=tfs,
                    min_buf=min_buf,
                    last_seen=last_seen,
                )

                (
                    orderbook_guard_level,
                    orderbook_guard_fail_reason,
                    orderbook_guard_warning_reason,
                    ob_ts,
                    ob_age,
                    orderbook_recovery_context_reason,
                ) = _check_orderbook_integrity_strict(
                    symbol=sym,
                    orderbook_snapshot=orderbook_snapshot,
                )

                db_ok, db_ms = _check_db_ping_strict(max_ping_ms=max_db_ping_ms)

                loop_ms = int((time.perf_counter() - t_loop0) * 1000.0)
                if loop_ms < 0:
                    loop_ms = 0

                orderbook_recovery_context = _require_bool(
                    _require_key(orderbook_snapshot, "recovery_context", "health_snapshot.orderbook"),
                    "health_snapshot.orderbook.recovery_context",
                )

                detail: Dict[str, Any] = {
                    "watchdog_interval_sec": float(interval_sec),
                    "ws_min_kline_buffer": int(min_buf),
                    "ws_required_tfs": list(tfs),
                    "kline_len": dict(lens),
                    "kline_last_ts_ms": dict(last_ts),
                    "kline_age_ms": dict(ages),
                    "kline_short_tfs": list(short_tfs),
                    "orderbook_integrity_ok": bool(orderbook_guard_level == "OK"),
                    "orderbook_guard_level": str(orderbook_guard_level),
                    "orderbook_guard_fail_reason": orderbook_guard_fail_reason,
                    "orderbook_guard_warning_reason": orderbook_guard_warning_reason,
                    "orderbook_recovery_context": bool(orderbook_recovery_context),
                    "orderbook_recovery_context_reason": orderbook_recovery_context_reason,
                    "orderbook_ts_ms": ob_ts,
                    "orderbook_age_ms": ob_age,
                    "db_ok": bool(db_ok),
                    "db_ping_ms": int(db_ms),
                    "max_db_ping_ms": int(max_db_ping_ms),
                    "watchdog_loop_ms": int(loop_ms),
                    "ws_level": str(ws_level),
                    "ws_fail_reason": ws_fail_reason,
                    "ws_warning_reason": ws_warning_reason,
                    "ws_overall_ok": _require_bool(
                        _require_key(ws_snapshot, "overall_ok", "health_snapshot"),
                        "health_snapshot.overall_ok",
                    ),
                    "ws_has_warning": _require_bool(
                        _require_key(ws_snapshot, "has_warning", "health_snapshot"),
                        "health_snapshot.has_warning",
                    ),
                    "ws_overall_reasons": _normalize_reason_list(
                        _require_key(ws_snapshot, "overall_reasons", "health_snapshot"),
                        "health_snapshot.overall_reasons",
                    ),
                    "ws_overall_warnings": list(ws_overall_warnings),
                }

                level: WatchdogLevel = "OK"
                fail_reason: Optional[str] = None
                warning_reason: Optional[str] = None

                if ws_level == "FAIL":
                    level = "FAIL"
                    fail_reason = ws_fail_reason
                elif orderbook_guard_level == "FAIL":
                    level = "FAIL"
                    fail_reason = orderbook_guard_fail_reason
                elif not db_ok:
                    level = "FAIL"
                    fail_reason = "db_lag"
                elif orderbook_guard_level == "WARNING":
                    level = "WARNING"
                    warning_reason = orderbook_guard_warning_reason
                elif ws_level == "WARNING":
                    level = "WARNING"
                    warning_reason = ws_warning_reason
                elif short_tfs:
                    level = "WARNING"
                    warning_reason = "warning_kline_buffer_short"
                else:
                    level = "OK"

                ws_transport_ok = _require_bool(
                    _require_key(ws_dict, "transport_ok", "health_snapshot.ws"),
                    "health_snapshot.ws.transport_ok",
                )
                ws_overall_ok = _require_bool(
                    _require_key(ws_snapshot, "overall_ok", "health_snapshot"),
                    "health_snapshot.overall_ok",
                )
                ws_overall_reasons = _normalize_reason_list(
                    _require_key(ws_snapshot, "overall_reasons", "health_snapshot"),
                    "health_snapshot.overall_reasons",
                )

                snap = WatchdogSnapshot(
                    level=level,
                    ok=level != "FAIL",
                    has_warning=level == "WARNING",
                    checked_at_ms=int(checked_ms),
                    ws_transport_ok=ws_transport_ok,
                    ws_overall_ok=ws_overall_ok,
                    ws_overall_reasons=ws_overall_reasons,
                    ws_overall_warnings=list(ws_overall_warnings),
                    kline_len=dict(lens),
                    kline_last_ts_ms=dict(last_ts),
                    kline_age_ms=dict(ages),
                    orderbook_ok=bool(orderbook_guard_level == "OK"),
                    orderbook_ts_ms=ob_ts,
                    orderbook_age_ms=ob_age,
                    db_ok=bool(db_ok),
                    db_ping_ms=int(db_ms),
                    loop_ms=int(loop_ms),
                    fail_reason=fail_reason,
                    warning_reason=warning_reason,
                    detail=dict(detail),
                )
                _LAST_SNAPSHOT = snap

                reason = "ok"
                if level == "FAIL":
                    reason = str(fail_reason)
                elif level == "WARNING":
                    reason = str(warning_reason)

                key = (
                    f"{level}|reason={reason}|"
                    f"ws_fail={ws_fail_reason}|ws_warn={ws_warning_reason}|"
                    f"ob_level={orderbook_guard_level}|ob_fail={orderbook_guard_fail_reason}|"
                    f"ob_warn={orderbook_guard_warning_reason}|"
                    f"ob_recovery={int(orderbook_recovery_context)}|"
                    f"db={int(db_ok)}|short_tfs={','.join(short_tfs)}"
                )

                if _should_emit(key, min_interval_sec=emit_min_sec):
                    _safe_event(sym, reason, detail)

                if level == "FAIL" and on_fatal is not None:
                    on_fatal(str(fail_reason), dict(detail))

                stop_evt.wait(timeout=float(interval_sec))

        except Exception as e:
            msg = f"[WATCHDOG][FATAL] {type(e).__name__}: {str(e)[:240]}"
            log(msg)
            try:
                _safe_event(
                    sym,
                    "watchdog_internal_error",
                    {"error_type": type(e).__name__, "error": str(e)[:500]},
                )
            except Exception as e2:
                log(f"[WATCHDOG][FATAL][EVENT_WRITE_FAIL] {type(e2).__name__}: {e2}")
            if on_fatal is not None:
                on_fatal("watchdog_internal_error", {"error_type": type(e).__name__, "error": str(e)})
            raise

    th = threading.Thread(target=_loop, name=f"engine-watchdog-{sym}", daemon=True)
    _WATCHDOG_THREAD = th

    try:
        th.start()
    except Exception:
        _WATCHDOG_THREAD = None
        _WATCHDOG_STOP = None
        raise


__all__ = [
    "WatchdogLevel",
    "WatchdogSnapshot",
    "start_watchdog",
    "stop_watchdog",
    "get_last_watchdog_snapshot",
]