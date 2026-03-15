# -*- coding: utf-8 -*-
# debug_market_data_ws_probe.py
"""
========================================================
FILE: debug_market_data_ws_probe.py
ROLE:
- market_data_ws 계층의 정지 지점을 단계별로 식별하는 진단 스크립트
- production 코드 수정 없이 import / settings / WS URL / REST snapshot / raw WS / on_open / façade 상태를 분리 점검한다

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- 진단 결과를 단계별 PASS/FAIL로 즉시 출력한다
- 실패 지점과 예외 타입/메시지를 그대로 노출한다
- production 런타임 상태를 임의 보정하지 않는다

CHANGE HISTORY:
- 2026-03-15:
  1) ADD(ROOT-CAUSE): market_data_ws 정지 지점 식별용 단계별 디버그 스크립트 추가
  2) ADD(TEST): import / URL / REST depth snapshot / raw WS open / first message / _on_open / façade polling 분리 검증
========================================================
"""

from __future__ import annotations

import argparse
import json
import threading
import time
import traceback
from typing import Any, Dict, Optional

import websocket


class ProbeFailure(RuntimeError):
    """Raised when a probe stage fails."""


def _print_stage(stage: str, status: str, detail: str = "") -> None:
    msg = f"[{status}] {stage}"
    if detail:
        msg += f" :: {detail}"
    print(msg, flush=True)


def _format_exception(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


def _run_stage(stage: str, fn):
    started = time.time()
    try:
        result = fn()
        elapsed = time.time() - started
        detail = f"elapsed={elapsed:.2f}s"
        if result is not None:
            detail += f" result={result}"
        _print_stage(stage, "PASS", detail)
        return result
    except Exception as exc:
        elapsed = time.time() - started
        _print_stage(stage, "FAIL", f"elapsed={elapsed:.2f}s error={_format_exception(exc)}")
        traceback.print_exc()
        raise


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--raw-ws-timeout-sec", type=float, default=10.0)
    parser.add_argument("--on-open-timeout-sec", type=float, default=15.0)
    parser.add_argument("--facade-poll-sec", type=float, default=15.0)
    args = parser.parse_args()

    symbol = str(args.symbol or "").upper().strip()
    if not symbol:
        raise ProbeFailure("symbol required")

    md = _run_stage("import market_data_ws façade", lambda: __import__("infra.market_data_ws", fromlist=["*"]))
    shared = _run_stage(
        "import market_data_ws_shared",
        lambda: __import__("infra.market_data_ws_shared", fromlist=["*"]),
    )
    _run_stage(
        "import market_data_ws_orderbook",
        lambda: __import__("infra.market_data_ws_orderbook", fromlist=["*"]),
    )

    ws_url = _run_stage("build ws url", lambda: shared._build_ws_url(symbol))
    _print_stage("ws url", "INFO", ws_url)

    depth_snapshot = _run_stage(
        "rest depth snapshot",
        lambda: shared._rest_fetch_depth_snapshot_strict(symbol, shared._ORDERBOOK_SNAPSHOT_LIMIT),
    )
    _print_stage(
        "rest depth snapshot summary",
        "INFO",
        f"lastUpdateId={depth_snapshot.get('lastUpdateId')} bids={len(depth_snapshot.get('bids') or [])} asks={len(depth_snapshot.get('asks') or [])}",
    )

    def probe_raw_ws_first_message() -> Dict[str, Any]:
        bucket: Dict[str, Any] = {
            "opened": False,
            "first_message": None,
            "error": None,
            "closed": None,
        }
        open_event = threading.Event()
        msg_event = threading.Event()
        done_event = threading.Event()

        def on_open(ws_app):
            bucket["opened"] = True
            open_event.set()

        def on_message(ws_app, message):
            if bucket["first_message"] is None:
                bucket["first_message"] = message
                msg_event.set()
                try:
                    ws_app.close()
                finally:
                    done_event.set()

        def on_error(ws_app, error):
            bucket["error"] = _format_exception(error) if isinstance(error, BaseException) else str(error)
            done_event.set()

        def on_close(ws_app, code, msg):
            bucket["closed"] = {"code": code, "msg": msg}
            done_event.set()

        ws_app = websocket.WebSocketApp(
            ws_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )

        th = threading.Thread(target=lambda: ws_app.run_forever(ping_interval=20), daemon=True)
        th.start()

        if not open_event.wait(timeout=args.raw_ws_timeout_sec):
            raise ProbeFailure("raw ws open timeout")
        if not msg_event.wait(timeout=args.raw_ws_timeout_sec):
            raise ProbeFailure(f"raw ws first message timeout closed={bucket['closed']} error={bucket['error']}")
        done_event.wait(timeout=2.0)

        raw = bucket["first_message"]
        if isinstance(raw, (bytes, bytearray)):
            raw = bytes(raw).decode("utf-8")
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise ProbeFailure(f"raw ws first message root invalid: {type(parsed).__name__}")
        if "stream" not in parsed or "data" not in parsed:
            raise ProbeFailure("raw ws first message missing stream/data")
        return {
            "stream": parsed.get("stream"),
            "data_keys": sorted(list((parsed.get("data") or {}).keys()))[:20],
        }

    _run_stage("raw ws open + first message", probe_raw_ws_first_message)

    class DummyWs:
        def __init__(self) -> None:
            self.closed_contexts = []

        def close(self) -> None:
            self.closed_contexts.append("close_called")

    def probe_on_open_thread() -> str:
        result: Dict[str, Any] = {"error": None, "finished": False}
        dummy = DummyWs()

        def runner() -> None:
            try:
                md._on_open(symbol, dummy)
                result["finished"] = True
            except Exception as exc:
                result["error"] = _format_exception(exc)
                result["finished"] = True

        th = threading.Thread(target=runner, daemon=True)
        th.start()
        th.join(timeout=args.on_open_timeout_sec)
        if th.is_alive():
            raise ProbeFailure(f"_on_open timeout>{args.on_open_timeout_sec}s (deadlock/blocking suspected)")
        if result["error"] is not None:
            raise ProbeFailure(f"_on_open failed: {result['error']}")
        return "finished"

    _run_stage("direct _on_open thread probe", probe_on_open_thread)

    def probe_facade_status_poll() -> str:
        md.start_ws_loop(symbol)
        deadline = time.time() + args.facade_poll_sec
        opened_seen = False
        orderbook_seen = False
        last_status: Optional[Dict[str, Any]] = None
        last_ob: Optional[Dict[str, Any]] = None

        while time.time() < deadline:
            last_status = md.get_ws_status(symbol)
            last_ob = md.get_orderbook_buffer_status(symbol)
            state = str(last_status.get("state") or "")
            if state == "OPEN":
                opened_seen = True
            if bool(last_ob.get("has_orderbook", False)):
                orderbook_seen = True
            if opened_seen and orderbook_seen:
                return f"state={state} has_orderbook=True"
            time.sleep(0.5)

        raise ProbeFailure(
            f"façade polling timeout status={last_status} orderbook={last_ob}"
        )

    _run_stage("façade start + status polling", probe_facade_status_poll)

    _print_stage("SUMMARY", "PASS", "all diagnostic stages passed")


if __name__ == "__main__":
    main()