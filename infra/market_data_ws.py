# -*- coding: utf-8 -*-
# infra/market_data_ws.py
"""
========================================================
FILE: infra/market_data_ws.py
ROLE:
- Binance USDT-M Futures multiplex WebSocket 수신기 public façade
- 기존 공개 API(import 경로)를 유지하면서 내부 구현을 transport / kline / orderbook / health 로 분리한다

CHANGE HISTORY:
- 2026-03-15:
  1) FIX(RECOVERY): reconnect / close / transport exception 시 last-good orderbook snapshot 즉시 삭제 제거
  2) FIX(OPERABILITY): recoverable WS 재연결 동안 stale snapshot을 health/monitoring이 관측할 수 있도록 유지
  3) FIX(ARCH): stale orderbook cleanup 책임을 close path가 아닌 next on_open bootstrap path 로 일원화
- 2026-03-14:
  1) REFACTOR(ROOT-CAUSE): WS transport / kline / orderbook / health 책임을 내부 모듈로 분리
  2) FIX(CONTRACT): orderbook stream contract 를 depth5@100ms 에서 diff-depth @depth@100ms 로 교정
  3) KEEP(COMPAT): 기존 public API(import path = infra.market_data_ws)는 유지
  4) FIX(ROOT-CAUSE): _handle_single_msg 손상 코드 제거 및 multiplex stream symbol 검증 기준을 stream path 기반으로 교정
  5) FIX(ROOT-CAUSE): _on_open 에서 _orderbook_lock 재진입 deadlock 제거
========================================================
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any, Dict

from .market_data_ws_health import (
    get_dashboard_ws_telemetry_snapshot,
    get_health_snapshot,
    is_data_healthy,
)
from .market_data_ws_kline import (
    _push_kline,
    backfill_klines_from_rest,
    bootstrap_klines_from_rest_strict,
    get_kline_buffer_status,
    get_klines,
    get_klines_with_volume,
    get_klines_with_volume_and_closed,
    get_last_kline_delay_ms,
    get_last_kline_ts,
    preload_klines,
)
from .market_data_ws_orderbook import (
    _bootstrap_orderbook_from_rest_snapshot_strict,
    _make_orderbook_bootstrap_placeholder_state,
    _push_orderbook,
    get_orderbook,
    get_orderbook_buffer_status,
)
from .market_data_ws_shared import (
    WS_BACKFILL_INTERVALS,
    WSProtocolError,
    _WS_CIRCUIT_BREAKER_COOLDOWN_SEC,
    _build_stream_names,
    _build_ws_url,
    _clear_orderbook_runtime_state,
    _close_ws_with_log,
    _get_ws_circuit_breaker_snapshot,
    _mark_ws_closed,
    _mark_ws_error,
    _mark_ws_open,
    _mark_ws_pong,
    _normalize_interval,
    _normalize_symbol,
    _now_ms,
    _orderbook_book_state,
    _orderbook_lock,
    _record_ws_circuit_breaker_failure,
    _register_ws_app,
    _reset_ws_circuit_breaker_on_open,
    _safe_dump_for_log,
    _start_ws_lock,
    _started_ws_symbols,
    _truncate_text,
    _unregister_ws_app,
    _ws_connection_open,
    _ws_state_lock,
    get_ws_status,
    log,
    websocket,
)


def _decode_msg(raw: Any) -> Any:
    if isinstance(raw, (bytes, bytearray)):
        txt = bytes(raw).decode("utf-8")
        return json.loads(txt)
    if isinstance(raw, str):
        return json.loads(raw)
    raise RuntimeError(f"unsupported ws message type: {type(raw)}")


def _extract_stream_symbol_strict(stream: str) -> str:
    s = str(stream or "").strip().lower()
    if not s or "@" not in s:
        raise WSProtocolError(f"invalid stream name (STRICT): {stream!r}")
    stream_symbol = _normalize_symbol(s.split("@", 1)[0])
    if not stream_symbol:
        raise WSProtocolError(f"stream symbol parse failed (STRICT): {stream!r}")
    return stream_symbol


def _handle_single_msg(expected_symbol: str, data: Any) -> None:
    if not isinstance(data, dict):
        raise WSProtocolError("message item must be dict (STRICT)")

    stream = data.get("stream")
    payload = data.get("data")
    if not isinstance(stream, str) or not isinstance(payload, dict):
        raise WSProtocolError("multiplex message missing stream/data (STRICT)")

    sym = _normalize_symbol(expected_symbol)
    if not sym:
        raise WSProtocolError("expected_symbol normalized to empty")

    stream_symbol = _extract_stream_symbol_strict(stream)
    if stream_symbol != sym:
        raise WSProtocolError(
            f"unexpected stream symbol (STRICT): expected={sym} got={stream_symbol} stream={stream}"
        )

    if "@kline_" in stream:
        k = payload.get("k")
        if not isinstance(k, dict):
            raise WSProtocolError(
                f"kline stream missing 'k': stream={stream} payload={_safe_dump_for_log(payload)}"
            )

        interval = str(k.get("i") or "").strip()
        if not interval:
            interval = stream.split("@kline_")[-1]

        interval = _normalize_interval(interval)
        if not interval:
            raise WSProtocolError(
                f"kline interval parse failed: stream={stream} payload={_safe_dump_for_log(payload)}"
            )

        payload_symbol_raw = k.get("s")
        if payload_symbol_raw is None:
            payload_symbol_raw = payload.get("s")
        if payload_symbol_raw is not None:
            payload_symbol = _normalize_symbol(str(payload_symbol_raw))
            if payload_symbol != sym:
                raise WSProtocolError(
                    f"unexpected kline payload symbol (STRICT): expected={sym} got={payload_symbol}"
                )

        _push_kline(sym, interval, k)
        return

    if "@depth" in stream:
        payload_symbol_raw = payload.get("s")
        if payload_symbol_raw is not None:
            payload_symbol = _normalize_symbol(str(payload_symbol_raw))
            if payload_symbol != sym:
                raise WSProtocolError(
                    f"unexpected depth payload symbol (STRICT): expected={sym} got={payload_symbol}"
                )

        _push_orderbook(sym, payload)
        return

    raise WSProtocolError(f"unknown stream type (STRICT): {stream}")


def _on_message(symbol: str, ws: websocket.WebSocketApp, message: Any) -> None:
    try:
        data = _decode_msg(message)
    except Exception as e:
        _record_ws_circuit_breaker_failure(symbol, reason=f"decode_error:{type(e).__name__}")
        log(f"[MD_BINANCE_WS] decode error: {type(e).__name__}: {e}")
        _mark_ws_error(symbol, f"decode_error:{type(e).__name__}:{e}")
        _close_ws_with_log(ws, context=f"decode_error symbol={_normalize_symbol(symbol)}")
        return

    try:
        if isinstance(data, list):
            for item in data:
                _handle_single_msg(symbol, item)
        else:
            _handle_single_msg(symbol, data)
    except WSProtocolError as e:
        _record_ws_circuit_breaker_failure(symbol, reason=f"protocol_error:{e}")
        _mark_ws_error(symbol, f"protocol_error:{e}")
        log(f"[MD_BINANCE_WS] protocol error: {e}")
        _close_ws_with_log(ws, context=f"protocol_error symbol={_normalize_symbol(symbol)}")
    except Exception as e:
        _record_ws_circuit_breaker_failure(symbol, reason=f"fatal_message_handling_error:{type(e).__name__}")
        _mark_ws_error(symbol, f"fatal_message_handling_error:{type(e).__name__}:{e}")
        log(f"[MD_BINANCE_WS] fatal message handling error: {type(e).__name__}: {e}")
        _close_ws_with_log(ws, context=f"fatal_message_handling_error symbol={_normalize_symbol(symbol)}")


def _on_error(symbol: str, ws: websocket.WebSocketApp, error: Any) -> None:
    _ = ws
    _record_ws_circuit_breaker_failure(symbol, reason=f"ws_error:{type(error).__name__}")
    _mark_ws_error(symbol, error)
    log(f"[MD_BINANCE_WS] error: {error}")


def _on_close(symbol: str, ws: websocket.WebSocketApp, code: Any, msg: Any) -> None:
    _ = ws
    sym = _normalize_symbol(symbol)

    # IMPORTANT:
    # close/reconnect path 에서는 last-good orderbook snapshot 을 유지한다.
    # 즉시 삭제하면 watchdog / feature layer 가 recoverable reconnect 를
    # fatal no-orderbook 상태로 오인한다.
    _mark_ws_closed(sym, _now_ms(), code=code, msg=msg)
    log(f"[MD_BINANCE_WS] closed: {code} {msg}")


def _on_open(symbol: str, ws: websocket.WebSocketApp) -> None:
    _ = ws
    streams = _build_stream_names(symbol)
    sym = _normalize_symbol(symbol)

    # IMPORTANT:
    # stale orderbook cleanup 는 next on_open bootstrap path 에서만 수행한다.
    # close path 에서 last-good snapshot 을 지우지 않고, 새 세션 open 시점에 authoritative refresh 한다.
    _clear_orderbook_runtime_state(sym)

    with _orderbook_lock:
        _orderbook_book_state[sym] = _make_orderbook_bootstrap_placeholder_state()

    try:
        _bootstrap_orderbook_from_rest_snapshot_strict(sym)
        _mark_ws_open(sym, _now_ms())
        _reset_ws_circuit_breaker_on_open(sym)
        log(f"[MD_BINANCE_WS] opened: symbol={sym} streams={streams}")
    except Exception as e:
        _record_ws_circuit_breaker_failure(sym, reason=f"orderbook_snapshot_bootstrap:{type(e).__name__}")
        _mark_ws_error(sym, f"orderbook_snapshot_bootstrap:{type(e).__name__}:{e}")
        log(f"[MD_BINANCE_WS] on_open bootstrap failed: {type(e).__name__}: {e}")
        _close_ws_with_log(ws, context=f"on_open_bootstrap_failed symbol={sym}")


def _on_pong(symbol: str, ws: websocket.WebSocketApp, data: Any) -> None:
    _ = ws
    _ = data
    _mark_ws_pong(symbol, _now_ms())


def start_ws_loop(symbol: str) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise RuntimeError("symbol is required")

    with _start_ws_lock:
        if sym in _started_ws_symbols:
            log(f"[MD_BINANCE_WS] already started for {sym} → skip duplicate start")
            return

    bootstrap_counts = bootstrap_klines_from_rest_strict(sym, intervals=WS_BACKFILL_INTERVALS)
    url = _build_ws_url(sym)

    def _runner() -> None:
        retry_wait = 1.0
        while True:
            cb = _get_ws_circuit_breaker_snapshot(sym)
            if cb["open"]:
                open_until_ts = cb["open_until_ts"]
                sleep_sec = _WS_CIRCUIT_BREAKER_COOLDOWN_SEC
                if open_until_ts is not None:
                    sleep_sec = max(0.1, (int(open_until_ts) - _now_ms()) / 1000.0)
                log(
                    f"[MD_BINANCE_WS][CB_WAIT] symbol={sym} "
                    f"sleep_sec={sleep_sec:.1f} reason={_truncate_text(cb['last_reason'])}"
                )
                time.sleep(sleep_sec)
                continue

            session_dur = 0.0
            try:
                log(f"[MD_BINANCE_WS] connecting ... url={url}")
                ws = websocket.WebSocketApp(
                    url,
                    on_open=lambda ws_: _on_open(sym, ws_),
                    on_message=lambda ws_, message: _on_message(sym, ws_, message),
                    on_error=lambda ws_, error: _on_error(sym, ws_, error),
                    on_close=lambda ws_, code, msg: _on_close(sym, ws_, code, msg),
                    on_pong=lambda ws_, data: _on_pong(sym, ws_, data),
                )
                _register_ws_app(sym, ws)
                try:
                    start_ts = time.time()
                    ws.run_forever(ping_interval=20)
                    session_dur = time.time() - start_ts
                finally:
                    _unregister_ws_app(sym, ws)

                with _ws_state_lock:
                    still_open = bool(_ws_connection_open.get(sym, False))
                if still_open:
                    _mark_ws_closed(sym, _now_ms(), code="RUN_FOREVER_RETURN", msg="run_forever returned")

                if session_dur <= 60.0:
                    _record_ws_circuit_breaker_failure(sym, reason="run_forever_return_short_session")

                log("[MD_BINANCE_WS] WS disconnected → retrying ...")

            except Exception as e:
                _record_ws_circuit_breaker_failure(sym, reason=f"run_forever_exception:{type(e).__name__}")
                _mark_ws_error(sym, f"run_forever_exception:{type(e).__name__}:{e}")
                _mark_ws_closed(sym, _now_ms(), code="RUN_FOREVER_EXCEPTION", msg=str(e))
                log(f"[MD_BINANCE_WS] run_forever exception: {type(e).__name__}: {e}")

            retry_wait = 1.0 if session_dur > 60.0 else min(retry_wait * 2.0, 10.0)
            log(f"[MD_BINANCE_WS] reconnecting after {retry_wait:.1f}s ...")
            time.sleep(retry_wait)

    th = threading.Thread(target=_runner, name=f"md-binance-ws-{sym}", daemon=True)

    with _start_ws_lock:
        if sym in _started_ws_symbols:
            log(f"[MD_BINANCE_WS] already started for {sym} → skip duplicate start")
            return
        _started_ws_symbols[sym] = th

    try:
        th.start()
    except Exception:
        with _start_ws_lock:
            _started_ws_symbols.pop(sym, None)
        raise

    log(
        f"[MD_BINANCE_WS] background ws started for {sym} "
        f"bootstrap={bootstrap_counts} backfill_intervals={WS_BACKFILL_INTERVALS}"
    )


def get_market_snapshot(symbol: str) -> Dict[str, Any]:
    sym = _normalize_symbol(symbol)
    from .market_data_ws_shared import _kline_buffers, _kline_lock

    with _kline_lock, _orderbook_lock:
        snapshot: Dict[str, Any] = {"symbol": sym, "orderbook": None, "klines": {}}
        ob = get_orderbook(sym, limit=5)
        if ob:
            snapshot["orderbook"] = dict(ob)

        kl_map: Dict[str, Any] = {}
        for (s, iv), rows in _kline_buffers.items():
            if s == sym:
                kl_map[iv] = [(ts, o, h, l, c, v) for (ts, o, h, l, c, v, _is_closed) in rows]
        snapshot["klines"] = kl_map
        return snapshot


__all__ = [
    "start_ws_loop",
    "preload_klines",
    "backfill_klines_from_rest",
    "bootstrap_klines_from_rest_strict",
    "get_klines",
    "get_klines_with_volume",
    "get_klines_with_volume_and_closed",
    "get_last_kline_ts",
    "get_last_kline_delay_ms",
    "get_kline_buffer_status",
    "get_orderbook_buffer_status",
    "get_orderbook",
    "get_ws_status",
    "get_dashboard_ws_telemetry_snapshot",
    "get_health_snapshot",
    "is_data_healthy",
    "get_market_snapshot",
]