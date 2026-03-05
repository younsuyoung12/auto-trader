"""
========================================================
FILE: infra/engine_watchdog.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할
- 실시간 런타임 “진단(Watchdog)” 스레드.
- 아래 항목을 주기적으로 검사하고, 결과를 DB(bt_events)에 기록하여 대시보드에서 조회 가능하게 만든다.

검사 항목
1) WS delay (kline / orderbook staleness)
2) orderbook integrity (ask>bid, bids/asks shape 등)
3) kline rollback (timestamp 역전)
4) latency (watchdog 루프 처리시간)
5) DB lag (SELECT 1 왕복 지연)

출력(저장)
- bt_events 테이블에 event_type="WATCHDOG" 로 기록한다.
- reason 예시:
  - ok
  - ws_kline_stale
  - ws_orderbook_stale
  - kline_rollback
  - orderbook_integrity_fail
  - db_lag
  - watchdog_internal_error

STRICT 정책
- 조용한 continue / 예외 삼키기 금지.
- 단, Watchdog는 “진단 모듈”이므로:
  - 이상 감지 시: DB 이벤트 기록 + (옵션) on_fatal 콜백 호출(엔진 SAFE_STOP 트리거용).
  - 모듈 자체 오류 시: watchdog_internal_error 기록 후 on_fatal 호출.

변경 이력
--------------------------------------------------------
- 2026-03-06:
  1) 신규 생성: 런타임 엔진 진단(Watchdog) 스레드 추가
  2) bt_events 로 저장하여 대시보드에서 “지속 감시” 가능하게 설계
========================================================
"""

from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple

from sqlalchemy import text

from infra.telelog import log
from state.db_core import get_session
from events.signals_logger import log_event

from infra.market_data_ws import (
    get_klines_with_volume as ws_get_klines_with_volume,
    get_orderbook as ws_get_orderbook,
)

from infra.data_integrity_guard import DataIntegrityError, validate_orderbook_strict


# =============================================================================
# Types
# =============================================================================
@dataclass(frozen=True, slots=True)
class WatchdogSnapshot:
    ok: bool
    checked_at_ms: int

    # WS
    kline_len: Dict[str, int]
    kline_last_ts_ms: Dict[str, int]
    kline_age_ms: Dict[str, int]

    orderbook_ok: bool
    orderbook_ts_ms: Optional[int]
    orderbook_age_ms: Optional[int]

    # DB
    db_ok: bool
    db_ping_ms: Optional[int]

    # latency
    loop_ms: int

    # reason if not ok
    fail_reason: Optional[str]
    fail_detail: Optional[Dict[str, Any]]


# =============================================================================
# Globals
# =============================================================================
_WATCHDOG_THREAD: Optional[threading.Thread] = None
_WATCHDOG_STOP: Optional[threading.Event] = None
_LAST_SNAPSHOT: Optional[WatchdogSnapshot] = None

_LAST_STATUS_KEY: str = ""
_LAST_STATUS_TS: float = 0.0


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


def _interval_ms(tf: str) -> int:
    s = str(tf).strip().lower()
    if s == "1m":
        return 60_000
    if s == "5m":
        return 300_000
    if s == "15m":
        return 900_000
    if s == "1h":
        return 3_600_000
    if s == "4h":
        return 14_400_000
    raise RuntimeError(f"unsupported tf (STRICT): {tf!r}")


def _safe_event(symbol: str, reason: str, extra: Dict[str, Any]) -> None:
    """
    STRICT:
    - 이벤트 기록 실패는 감추지 않는다.
    - 다만 Watchdog 내부에서 예외가 연쇄 폭발하면 진단 자체가 불능이 되므로,
      여기서는 log_event 실패 시 즉시 예외를 올린다(상위에서 watchdog_internal_error 처리).
    """
    log_event(
        event_type="WATCHDOG",
        symbol=str(symbol),
        side="CLOSE",   # ← 추가 (STRICT 요구)
        reason=str(reason),
        extra_json=dict(extra),
    )


def _should_emit(key: str, *, min_interval_sec: int) -> bool:
    global _LAST_STATUS_KEY, _LAST_STATUS_TS
    now = time.time()
    if key != _LAST_STATUS_KEY:
        _LAST_STATUS_KEY = key
        _LAST_STATUS_TS = now
        return True
    if (now - _LAST_STATUS_TS) >= float(min_interval_sec):
        _LAST_STATUS_TS = now
        return True
    return False


# =============================================================================
# Core checks
# =============================================================================
def _check_ws_klines_strict(
    *,
    symbol: str,
    tfs: Tuple[str, ...],
    min_buf: int,
    max_delay_sec: float,
    last_seen: Dict[str, int],
) -> Tuple[Dict[str, int], Dict[str, int], Dict[str, int]]:
    """
    Returns:
      lens, last_ts_ms, age_ms
    """
    now_ms = _now_ms()
    lens: Dict[str, int] = {}
    last_ts: Dict[str, int] = {}
    ages: Dict[str, int] = {}

    delay_ms = int(float(max_delay_sec) * 1000.0)
    if delay_ms <= 0:
        raise RuntimeError("max_delay_sec must be > 0 (STRICT)")

    for tf in tfs:
        buf = ws_get_klines_with_volume(symbol, tf, limit=min_buf)
        if not isinstance(buf, list):
            raise RuntimeError(f"ws kline buffer invalid type (STRICT): tf={tf} type={type(buf).__name__}")
        if len(buf) < min_buf:
            # len 부족 자체가 “WS delay / not ready”에 해당
            lens[tf] = int(len(buf))
            last_ts[tf] = int(buf[-1][0]) if buf else 0
            ages[tf] = int(now_ms - last_ts[tf]) if last_ts[tf] > 0 else -1
            continue

        ts_ms = _positive_int(buf[-1][0], f"ws_kline[{tf}].openTime")
        lens[tf] = int(len(buf))
        last_ts[tf] = int(ts_ms)
        ages[tf] = int(now_ms - ts_ms)

        prev = int(last_seen.get(tf, 0))
        if prev > 0 and ts_ms < prev:
            raise RuntimeError(f"kline rollback detected (STRICT): tf={tf} prev={prev} now={ts_ms}")
        last_seen[tf] = int(ts_ms)

        # staleness: (interval_ms + delay_ms)
        allowed = _interval_ms(tf) + delay_ms
        if ages[tf] < 0:
            raise RuntimeError(f"kline ts in future (STRICT): tf={tf} age_ms={ages[tf]}")
        if ages[tf] > allowed:
            # stale 자체는 “엔진 차단 사유”로 기록
            # (여기서 즉시 예외로 올려도 되지만, reason을 명확히 남기기 위해 상위에서 처리)
            pass

    return lens, last_ts, ages


def _check_orderbook_strict(
    *,
    symbol: str,
    max_age_ms: int,
) -> Tuple[bool, Optional[int], Optional[int]]:
    ob = ws_get_orderbook(symbol, limit=5)
    if not isinstance(ob, dict) or not ob:
        return False, None, None

    # ts 추출(STRICT)
    ts_ms: Optional[int] = None
    if ob.get("exchTs") is not None:
        ts_ms = _positive_int(ob.get("exchTs"), "orderbook.exchTs")
    elif ob.get("ts") is not None:
        ts_ms = _positive_int(ob.get("ts"), "orderbook.ts")

    # 무결성 검사
    try:
        # require_ts: ts_ms가 있으면 True로 강제, 없으면 False로라도 구조검증
        validate_orderbook_strict(ob, symbol=str(symbol), require_ts=bool(ts_ms))
    except DataIntegrityError:
        return False, ts_ms, None

    if ts_ms is None:
        # ts 자체가 없으면 “지연 측정 불가”이므로 fail로 취급
        return False, None, None

    age = _now_ms() - int(ts_ms)
    if age < -2000:
        return False, int(ts_ms), int(age)
    if age > int(max_age_ms):
        return False, int(ts_ms), int(age)

    return True, int(ts_ms), int(age)


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


# =============================================================================
# Public API
# =============================================================================
def get_last_watchdog_snapshot() -> Optional[WatchdogSnapshot]:
    return _LAST_SNAPSHOT


def stop_watchdog() -> None:
    global _WATCHDOG_STOP, _WATCHDOG_THREAD
    if _WATCHDOG_STOP is not None:
        _WATCHDOG_STOP.set()
    _WATCHDOG_STOP = None
    _WATCHDOG_THREAD = None


def start_watchdog(
    *,
    settings: Any,
    symbol: Optional[str] = None,
    on_fatal: Optional[Callable[[str, Dict[str, Any]], None]] = None,
) -> None:
    """
    settings: settings.load_settings() 결과(SSOT)
    on_fatal: 치명 감지 시 엔진 SAFE_STOP 등을 트리거하기 위한 콜백
              signature: on_fatal(reason: str, detail: dict)

    STRICT:
    - 이미 실행 중이면 예외.
    """
    global _WATCHDOG_THREAD, _WATCHDOG_STOP

    if _WATCHDOG_THREAD is not None and _WATCHDOG_THREAD.is_alive():
        raise RuntimeError("engine_watchdog already running (STRICT)")

    sym = str(symbol or getattr(settings, "symbol", "")).replace("-", "").replace("/", "").upper().strip()
    if not sym:
        raise RuntimeError("symbol is required (STRICT)")

    # 설정값(SSOT) — 기본값 사용 시 반드시 로그로 가시화
    interval_sec = getattr(settings, "engine_watchdog_interval_sec", None)
    if interval_sec is None:
        interval_sec = 5.0
        log(f"[WATCHDOG][DEFAULT] settings.engine_watchdog_interval_sec missing -> using {interval_sec}s")
    interval_sec_f = _finite_float(interval_sec, "settings.engine_watchdog_interval_sec")
    if interval_sec_f <= 0:
        raise RuntimeError("engine_watchdog_interval_sec must be > 0 (STRICT)")

    min_buf = getattr(settings, "ws_min_kline_buffer", None)
    if min_buf is None:
        raise RuntimeError("settings.ws_min_kline_buffer missing (STRICT)")
    min_buf_i = _positive_int(min_buf, "settings.ws_min_kline_buffer")

    # kline delay 허용치(sec)
    max_kline_delay_sec = getattr(settings, "ws_max_kline_delay_sec", None)
    if max_kline_delay_sec is None:
        max_kline_delay_sec = 120.0
        log(f"[WATCHDOG][DEFAULT] settings.ws_max_kline_delay_sec missing -> using {max_kline_delay_sec}s")
    max_kline_delay_sec_f = _finite_float(max_kline_delay_sec, "settings.ws_max_kline_delay_sec")
    if max_kline_delay_sec_f <= 0:
        raise RuntimeError("ws_max_kline_delay_sec must be > 0 (STRICT)")

    # orderbook age(ms)
    max_ob_age_ms = getattr(settings, "max_orderbook_age_ms", None)
    if max_ob_age_ms is None:
        max_ob_age_ms = 3000
        log(f"[WATCHDOG][DEFAULT] settings.max_orderbook_age_ms missing -> using {max_ob_age_ms}ms")
    max_ob_age_ms_i = _positive_int(max_ob_age_ms, "settings.max_orderbook_age_ms")

    # DB ping(ms)
    max_db_ping_ms = getattr(settings, "engine_watchdog_max_db_ping_ms", None)
    if max_db_ping_ms is None:
        max_db_ping_ms = 1500
        log(f"[WATCHDOG][DEFAULT] settings.engine_watchdog_max_db_ping_ms missing -> using {max_db_ping_ms}ms")
    max_db_ping_ms_i = _positive_int(max_db_ping_ms, "settings.engine_watchdog_max_db_ping_ms")

    # 이벤트 저장 최소 간격(같은 상태 반복 기록 제한)
    emit_min_sec = getattr(settings, "engine_watchdog_emit_min_sec", None)
    if emit_min_sec is None:
        emit_min_sec = 30
        log(f"[WATCHDOG][DEFAULT] settings.engine_watchdog_emit_min_sec missing -> using {emit_min_sec}s")
    emit_min_sec_i = _positive_int(emit_min_sec, "settings.engine_watchdog_emit_min_sec")

    # 필수 TF
    tfs_raw = getattr(settings, "ws_subscribe_tfs", None)
    if not isinstance(tfs_raw, (list, tuple)) or not tfs_raw:
        raise RuntimeError("settings.ws_subscribe_tfs must be list-like and non-empty (STRICT)")
    tfs = tuple(str(x).strip() for x in tfs_raw if str(x).strip())
    if not tfs:
        raise RuntimeError("settings.ws_subscribe_tfs normalized empty (STRICT)")

    stop_evt = threading.Event()
    _WATCHDOG_STOP = stop_evt

    last_seen: Dict[str, int] = {}

    def _loop() -> None:
        global _LAST_SNAPSHOT

        try:
            log(f"[WATCHDOG] started symbol={sym} interval_sec={interval_sec_f} tfs={list(tfs)} min_buf={min_buf_i}")
            while not stop_evt.is_set():
                t_loop0 = time.perf_counter()
                checked_ms = _now_ms()

                # 1) WS klines
                lens, last_ts, ages = _check_ws_klines_strict(
                    symbol=sym,
                    tfs=tfs,
                    min_buf=min_buf_i,
                    max_delay_sec=max_kline_delay_sec_f,
                    last_seen=last_seen,
                )

                # staleness 판단: interval_ms + delay
                delay_ms = int(max_kline_delay_sec_f * 1000.0)
                stale_tfs = []
                for tf in tfs:
                    if lens.get(tf, 0) < min_buf_i:
                        stale_tfs.append(tf)
                        continue
                    age = int(ages.get(tf, -1))
                    allowed = _interval_ms(tf) + delay_ms
                    if age < 0 or age > allowed:
                        stale_tfs.append(tf)

                # 2) Orderbook
                ob_ok, ob_ts, ob_age = _check_orderbook_strict(symbol=sym, max_age_ms=max_ob_age_ms_i)

                # 3) DB ping
                db_ok, db_ms = _check_db_ping_strict(max_ping_ms=max_db_ping_ms_i)

                loop_ms = int((time.perf_counter() - t_loop0) * 1000.0)
                if loop_ms < 0:
                    loop_ms = 0

                # 상태 결정
                ok = (not stale_tfs) and ob_ok and db_ok
                reason = None
                detail: Dict[str, Any] = {
                    "ws_min_kline_buffer": int(min_buf_i),
                    "ws_max_kline_delay_sec": float(max_kline_delay_sec_f),
                    "kline_len": dict(lens),
                    "kline_last_ts_ms": dict(last_ts),
                    "kline_age_ms": dict(ages),
                    "stale_tfs": list(stale_tfs),
                    "orderbook_ok": bool(ob_ok),
                    "orderbook_ts_ms": ob_ts,
                    "orderbook_age_ms": ob_age,
                    "max_orderbook_age_ms": int(max_ob_age_ms_i),
                    "db_ok": bool(db_ok),
                    "db_ping_ms": int(db_ms),
                    "max_db_ping_ms": int(max_db_ping_ms_i),
                    "watchdog_loop_ms": int(loop_ms),
                }

                if ok:
                    reason = "ok"
                else:
                    if stale_tfs:
                        reason = "ws_kline_stale"
                    elif not ob_ok:
                        reason = "ws_orderbook_stale"  # 포함: ts 없음/무결성 실패/age 초과
                    elif not db_ok:
                        reason = "db_lag"
                    else:
                        reason = "unknown_fail"

                snap = WatchdogSnapshot(
                    ok=bool(ok),
                    checked_at_ms=int(checked_ms),
                    kline_len=dict(lens),
                    kline_last_ts_ms=dict(last_ts),
                    kline_age_ms=dict(ages),
                    orderbook_ok=bool(ob_ok),
                    orderbook_ts_ms=ob_ts,
                    orderbook_age_ms=ob_age,
                    db_ok=bool(db_ok),
                    db_ping_ms=int(db_ms),
                    loop_ms=int(loop_ms),
                    fail_reason=None if ok else str(reason),
                    fail_detail=None if ok else dict(detail),
                )
                _LAST_SNAPSHOT = snap

                # DB 이벤트 기록(상태 변동 또는 일정 간격)
                key = f"{reason}|stale={','.join(stale_tfs)}|ob={int(ob_ok)}|db={int(db_ok)}"
                if _should_emit(key, min_interval_sec=emit_min_sec_i):
                    _safe_event(sym, reason, detail)

                # 치명 조건(옵션): rollback 같은 건 위에서 예외로 터진다.
                # 여기서는 on_fatal은 “상태 기반”으로도 호출할 수 있게 한다.
                # (원하면 엔진에서 SAFE_STOP 기준을 reason별로 다르게 둘 수 있다)
                if (not ok) and on_fatal is not None:
                    # 즉시 STOP을 원할지 여부는 엔진 정책(on_fatal)에서 결정.
                    on_fatal(str(reason), dict(detail))

                # sleep
                stop_evt.wait(timeout=float(interval_sec_f))

        except Exception as e:
            # watchdog 자체 오류는 치명(진단기 불능) → DB 기록 + on_fatal
            msg = f"[WATCHDOG][FATAL] {type(e).__name__}: {str(e)[:240]}"
            log(msg)
            try:
                _safe_event(
                    sym,
                    "watchdog_internal_error",
                    {"error_type": type(e).__name__, "error": str(e)[:500]},
                )
            except Exception as e2:
                # 이벤트 기록조차 실패하면, 마지막 수단으로 로그만 남긴다(여기서 더 진행 불가).
                log(f"[WATCHDOG][FATAL][EVENT_WRITE_FAIL] {type(e2).__name__}: {e2}")
            if on_fatal is not None:
                on_fatal("watchdog_internal_error", {"error_type": type(e).__name__, "error": str(e)})
            raise

    th = threading.Thread(target=_loop, name=f"engine-watchdog-{sym}", daemon=True)
    _WATCHDOG_THREAD = th
    th.start()


__all__ = [
    "WatchdogSnapshot",
    "start_watchdog",
    "stop_watchdog",
    "get_last_watchdog_snapshot",
]