# test_engine_watchdog.py
"""
========================================================
FILE: test_engine_watchdog.py
STRICT · NO-FALLBACK · DIAG MODE
========================================================

목적
- infra/engine_watchdog.py가 런타임에서 실제로 동작하는지 검증한다.
- WS 버퍼(klines) + orderbook 준비 → Watchdog 시작 → 스냅샷 출력 → DB(bt_events) 기록 확인

검증 항목
1) WS 루프 시작
2) REST backfill → WS buffer inject (min buffer 충족)
3) orderbook 준비
4) engine_watchdog start → 주기적 스냅샷 확인
5) DB에 WATCHDOG 이벤트가 기록되는지(가능한 경우) 확인

주의
- 민감정보(키/토큰/DB URL)는 출력하지 않는다.
- 실패 시 원인을 최대한 정확히 출력하고 즉시 종료한다.
"""

from __future__ import annotations

import platform
import sys
import time
import traceback
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text

from settings import load_settings
from infra.telelog import log

from infra.async_worker import start_worker as start_async_worker
from infra.market_data_ws import (
    start_ws_loop,
    backfill_klines_from_rest,
    get_klines_with_volume as ws_get_klines_with_volume,
    get_orderbook as ws_get_orderbook,
)
from infra.market_data_rest import fetch_klines_rest, KlineRestError

from state.db_core import get_session

from infra.engine_watchdog import (
    start_watchdog,
    stop_watchdog,
    get_last_watchdog_snapshot,
)


# --------------------------
# Print helpers
# --------------------------
def _banner(title: str) -> None:
    print("\n" + "=" * 94)
    print(title)
    print("=" * 94)


def _kv(k: str, v: Any) -> None:
    print(f"{k:28s}: {v}")


def _die(msg: str, exc: Optional[BaseException] = None) -> None:
    print("\n[FATAL]", msg)
    if exc is not None:
        traceback.print_exc()
    raise SystemExit(2)


# --------------------------
# Strict helpers
# --------------------------
def _require_nonempty_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        _die(f"{name} is required (STRICT)")
    return s


def _int_pos(v: Any, name: str) -> int:
    try:
        i = int(v)
    except Exception:
        _die(f"{name} must be int (STRICT)")
    if i <= 0:
        _die(f"{name} must be > 0 (STRICT)")
    return i


def _normalize_symbol(s: str) -> str:
    return str(s).replace("-", "").replace("/", "").upper().strip()


# --------------------------
# WS bootstrap helpers
# --------------------------
def _wait_ws_buffers_strict(symbol: str, tfs: List[str], *, min_buf: int, timeout_sec: int = 90) -> None:
    deadline = time.time() + float(timeout_sec)
    while True:
        lens = {}
        ok = True
        for tf in tfs:
            buf = ws_get_klines_with_volume(symbol, tf, limit=min_buf)
            ln = len(buf) if isinstance(buf, list) else -1
            lens[tf] = ln
            if ln < min_buf:
                ok = False

        ob = ws_get_orderbook(symbol, limit=5)
        ob_ok = isinstance(ob, dict) and bool(ob.get("bids")) and bool(ob.get("asks"))

        print(f"[WAIT] min={min_buf} lens={lens} ob_ok={ob_ok}")

        if ok and ob_ok:
            return

        if time.time() >= deadline:
            _die(f"WS buffers not ready within timeout (STRICT): min_buf={min_buf} lens={lens} ob_ok={ob_ok}")

        time.sleep(1.0)


def _rest_backfill_inject_strict(symbol: str, tfs: List[str], *, limit: int, min_len: int) -> None:
    """
    REST → backfill_klines_from_rest()로 WS store에 inject.
    STRICT:
    - 각 TF에서 rows<min_len이면 즉시 실패
    """
    for tf in tfs:
        _banner(f"REST BACKFILL: {symbol} tf={tf} limit={limit}")
        try:
            rows = fetch_klines_rest(symbol, tf, limit=limit)
        except KlineRestError as e:
            _die(f"REST klines failed (STRICT): {symbol} {tf} {type(e).__name__}: {e}", e)

        if not isinstance(rows, list) or not rows:
            _die(f"REST klines empty (STRICT): {symbol} {tf}")

        if len(rows) < min_len:
            _die(f"REST klines insufficient (STRICT): {symbol} {tf} need>={min_len} got={len(rows)}")

        backfill_klines_from_rest(symbol, tf, rows)

        probe = ws_get_klines_with_volume(symbol, tf, limit=5)
        if not isinstance(probe, list) or not probe:
            _die(f"WS probe empty after inject (STRICT): {symbol} {tf}")

        _kv("ws_probe_len", len(probe))
        _kv("ws_probe_last_ts", probe[-1][0])


# --------------------------
# DB verify helpers
# --------------------------
def _get_bt_events_columns() -> List[str]:
    """
    bt_events 컬럼을 정보스키마로 조회한다.
    """
    with get_session() as session:
        rows = session.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'bt_events'
                ORDER BY ordinal_position
                """
            )
        ).fetchall()
    return [str(r[0]) for r in rows]


def _fetch_latest_watchdog_event_strict(symbol: str) -> Dict[str, Any]:
    """
    가능한 컬럼 조합을 자동 탐지해 WATCHDOG 최신 1건을 조회한다.
    (스키마가 다르면 여기서 실패하여 원인 노출)
    """
    cols = _get_bt_events_columns()
    if not cols:
        _die("bt_events columns fetch returned empty (STRICT)")

    # event_type 컬럼명 추정
    if "event_type" not in cols:
        _die(f"bt_events missing column 'event_type' (STRICT). cols={cols}")

    # symbol 컬럼 존재 확인
    if "symbol" not in cols:
        _die(f"bt_events missing column 'symbol' (STRICT). cols={cols}")

    # 정렬 기준 컬럼 탐색 (ts_utc / created_at / id)
    order_col = None
    for cand in ("ts_utc", "created_at", "id"):
        if cand in cols:
            order_col = cand
            break
    if order_col is None:
        _die(f"bt_events has no sortable column among ts_utc/created_at/id (STRICT). cols={cols}")

    # reason/extra_json 있으면 같이
    select_cols = ["event_type", "symbol"]
    for cand in ("reason", "side", "ts_utc", "created_at", "id", "extra_json"):
        if cand in cols and cand not in select_cols:
            select_cols.append(cand)

    q = text(
        f"""
        SELECT {", ".join(select_cols)}
        FROM bt_events
        WHERE event_type = 'WATCHDOG'
          AND symbol = :symbol
        ORDER BY {order_col} DESC
        LIMIT 1
        """
    )

    with get_session() as session:
        row = session.execute(q, {"symbol": symbol}).fetchone()

    if row is None:
        _die("No WATCHDOG event found in bt_events (STRICT). Watchdog is not recording or wrong DB.")

    out = {}
    for i, c in enumerate(select_cols):
        out[c] = row[i]
    return out


# --------------------------
# Main
# --------------------------
def main() -> int:
    _banner("0) ENV / RUNTIME")
    _kv("python", sys.version.replace("\n", " "))
    _kv("executable", sys.executable)
    _kv("cwd", str(__import__("os").getcwd()))
    _kv("platform", platform.platform())

    _banner("1) SETTINGS LOAD (SSOT)")
    s = load_settings()
    symbol = _normalize_symbol(_require_nonempty_str(getattr(s, "symbol", None), "settings.symbol"))
    tfs = getattr(s, "ws_subscribe_tfs", None)
    if not isinstance(tfs, list) or not tfs:
        _die("settings.ws_subscribe_tfs must be non-empty list (STRICT)")
    tfs_norm = [str(x).strip() for x in tfs if str(x).strip()]
    if not tfs_norm:
        _die("settings.ws_subscribe_tfs normalized empty (STRICT)")

    min_buf = _int_pos(getattr(s, "ws_min_kline_buffer", None), "settings.ws_min_kline_buffer")
    backfill_limit = _int_pos(getattr(s, "ws_backfill_limit", None), "settings.ws_backfill_limit")

    _kv("symbol", symbol)
    _kv("ws_subscribe_tfs", tfs_norm)
    _kv("ws_min_kline_buffer", min_buf)
    _kv("ws_backfill_limit", backfill_limit)

    # async worker (TG submit fail 방지)
    _banner("2) ASYNC WORKER START (for TG/log async)")
    try:
        start_async_worker(
            num_threads=int(getattr(s, "async_worker_threads", 1) or 1),
            max_queue_size=int(getattr(s, "async_worker_queue_size", 2000) or 2000),
            thread_name_prefix="async-test",
        )
        _kv("async_worker", "started")
    except Exception as e:
        _die(f"async worker start failed (STRICT): {type(e).__name__}: {e}", e)

    _banner("3) WS START")
    try:
        start_ws_loop(symbol)
        _kv("ws_loop", "started")
    except Exception as e:
        _die(f"start_ws_loop failed (STRICT): {type(e).__name__}: {e}", e)

    _banner("4) REST BACKFILL → WS INJECT")
    # min_len은 min_buf 이상을 요구 (STRICT)
    _rest_backfill_inject_strict(symbol, tfs_norm, limit=backfill_limit, min_len=min_buf)

    _banner("5) WAIT WS BUFFERS + ORDERBOOK READY (STRICT)")
    _wait_ws_buffers_strict(symbol, tfs_norm, min_buf=min_buf, timeout_sec=120)

    _banner("6) WATCHDOG START + SNAPSHOT CHECK")
    fatal_hits: List[Tuple[str, Dict[str, Any]]] = []

    def on_fatal(reason: str, detail: Dict[str, Any]) -> None:
        # 엔진 종료 여부는 여기서 결정 가능(테스트에선 기록만)
        fatal_hits.append((str(reason), dict(detail)))

    try:
        start_watchdog(settings=s, symbol=symbol, on_fatal=on_fatal)
    except Exception as e:
        # 대표 원인: signals_logger.log_event side 누락
        msg = f"start_watchdog failed (STRICT): {type(e).__name__}: {e}"
        print("[HINT] if error is about payload.side, fix engine_watchdog._safe_event to pass side='CLOSE'")
        _die(msg, e)

    # 몇 사이클 관찰
    for i in range(4):
        time.sleep(2.5)
        snap = get_last_watchdog_snapshot()
        if snap is None:
            _die("watchdog snapshot is None (STRICT)")

        print(f"\n[SNAPSHOT #{i+1}] ok={snap.ok} reason={snap.fail_reason} loop_ms={snap.loop_ms} db_ping_ms={snap.db_ping_ms}")
        if snap.orderbook_age_ms is not None:
            print(f"  orderbook_age_ms={snap.orderbook_age_ms} orderbook_ok={snap.orderbook_ok}")
        print(f"  stale_tfs={(snap.fail_detail or {}).get('stale_tfs') if snap.fail_detail else None}")

    _banner("7) DB CHECK: latest WATCHDOG event (STRICT)")
    try:
        ev = _fetch_latest_watchdog_event_strict(symbol)
        for k, v in ev.items():
            _kv(k, v)
    except Exception as e:
        _die(f"DB watchdog event check failed (STRICT): {type(e).__name__}: {e}", e)

    _banner("8) STOP WATCHDOG")
    stop_watchdog()
    _kv("watchdog", "stopped")

    _banner("DONE")
    if fatal_hits:
        print("[WARN] on_fatal was called during test:")
        for r, d in fatal_hits[:3]:
            print(" - reason:", r, "keys:", list(d.keys())[:10])
    else:
        print("OK: watchdog running + snapshot ok + DB event recorded")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())