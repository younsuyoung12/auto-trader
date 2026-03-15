# -*- coding: utf-8 -*-
# infra/market_data_ws_health.py
"""
========================================================
FILE: infra/market_data_ws_health.py
ROLE:
- market_data_ws 분리 리팩터링 health / telemetry 전용 계층
- transport / payload / feed activity 를 분리 판단하고 dashboard telemetry 를 제공한다

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- orderbook reconnect / bootstrap / resync 는 recoverable window 로 취급해야 한다
- transport fail 과 payload fail 과 feed-activity warning 을 구분한다
- self-heal 가능한 orderbook recovery 상태를 payload FAIL 로 승격하지 않는다

CHANGE HISTORY:
- 2026-03-15:
  1) FIX(ROOT-CAUSE): orderbook reconnect / bootstrapping / resync 상태를 payload FAIL 대신 warning 으로 재분류
  2) FEAT(RECOVERY): ws_state / bootstrapping / resync_reason / startup_no_orderbook_yet 기반 recoverable orderbook context 추가
  3) FIX(OPERABILITY): recoverable context 에서는 feed gap reconnect trigger 를 중복 발동하지 않도록 수정
  4) FEAT(OBSERVABILITY): health snapshot / dashboard telemetry 에 orderbook resync fields 노출
========================================================
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Tuple

from . import market_data_ws_shared as shared
from .market_data_ws_kline import get_kline_buffer_status
from .market_data_ws_orderbook import get_orderbook_buffer_status
from .market_data_ws_shared import (
    HEALTH_REQUIRED_INTERVALS,
    KLINE_MAX_DELAY_SEC,
    MARKET_EVENT_MAX_DELAY_SEC,
    _dedupe_keep_order,
    _derive_dashboard_connection_status,
    _health_fail_lock,
    _maybe_force_reconnect_on_orderbook_feed_gap,
    _ms_to_sec_optional,
    _normalize_required_intervals_for_dashboard,
    _normalize_symbol,
    _now_ms,
    _orderbook_buffers,
    _orderbook_last_recv_ts,
    _orderbook_lock,
    _require_positive_int,
    get_ws_status,
    log,
)


def _compute_kline_health(symbol: str, interval: str) -> Dict[str, Any]:
    status = get_kline_buffer_status(symbol, interval)
    buffer_len = status["buffer_len"]
    delay_ms = status["delay_ms"]

    ok = True
    reasons: List[str] = []

    min_buf = shared._min_buffer_for_interval(interval)
    if buffer_len < min_buf:
        ok = False
        reasons.append(f"buffer_len<{min_buf} (got={buffer_len})")

    if delay_ms is None:
        ok = False
        reasons.append("no_recv_ts")
    else:
        delay_sec = delay_ms / 1000.0
        if delay_sec > KLINE_MAX_DELAY_SEC:
            ok = False
            reasons.append(f"delay_sec>{KLINE_MAX_DELAY_SEC} (got={delay_sec:.1f})")

    status["ok"] = ok
    status["reasons"] = reasons
    status["min_buffer_required"] = min_buf
    return status


def _is_recoverable_orderbook_context(
    *,
    sym: str,
    ws_status: Dict[str, Any],
    ob_status: Dict[str, Any],
    now_ms: int,
) -> Tuple[bool, Optional[str]]:
    ws_state = str(ws_status.get("state") or "").upper().strip()
    if ws_state in ("OPENING", "RECONNECTING"):
        return True, f"ws_state={ws_state}"

    bootstrapping_raw = ob_status.get("bootstrapping")
    if bootstrapping_raw is not None:
        bootstrapping = bool(bootstrapping_raw)
        if bootstrapping:
            return True, "orderbook_bootstrapping"

    resync_reason_raw = ob_status.get("resync_reason")
    if resync_reason_raw is not None:
        resync_reason = str(resync_reason_raw).strip()
        if not resync_reason:
            raise RuntimeError("orderbook_buffer_status.resync_reason empty (STRICT)")
        return True, f"orderbook_resync:{resync_reason}"

    last_resync_ts_raw = ob_status.get("last_resync_ts")
    if last_resync_ts_raw is not None:
        last_resync_ts = int(last_resync_ts_raw)
        if last_resync_ts <= 0:
            raise RuntimeError("orderbook_buffer_status.last_resync_ts must be > 0 (STRICT)")
        age_sec = max(0.0, (now_ms - last_resync_ts) / 1000.0)
        if age_sec <= MARKET_EVENT_MAX_DELAY_SEC:
            return True, f"recent_resync_age_sec={age_sec:.1f}"

    last_open_ts_raw = ws_status.get("last_open_ts")
    if ws_state == "OPEN" and last_open_ts_raw is not None:
        last_open_ts = int(last_open_ts_raw)
        if last_open_ts <= 0:
            raise RuntimeError("ws_status.last_open_ts must be > 0 (STRICT)")
        open_age_sec = max(0.0, (now_ms - last_open_ts) / 1000.0)
        if open_age_sec <= MARKET_EVENT_MAX_DELAY_SEC:
            return True, f"open_startup_age_sec={open_age_sec:.1f}"

    ws_warning_items = list(ws_status.get("warnings") or [])
    for idx, item in enumerate(ws_warning_items):
        s = str(item).strip()
        if not s:
            raise RuntimeError(f"ws_status.warnings[{idx}] empty (STRICT)")
        if "startup_no_orderbook_yet" in s:
            return True, "warning_startup_no_orderbook_yet"

    return False, None


def _compute_orderbook_health(symbol: str) -> Dict[str, Any]:
    sym = _normalize_symbol(symbol)
    now_ms = _now_ms()
    ws_status = get_ws_status(sym)
    ob_status = get_orderbook_buffer_status(sym)

    with _orderbook_lock:
        ob = _orderbook_buffers.get(sym)
        orderbook_last_recv_ts = _orderbook_last_recv_ts.get(sym)

    recovery_ok, recovery_context_reason = _is_recoverable_orderbook_context(
        sym=sym,
        ws_status=ws_status,
        ob_status=ob_status,
        now_ms=now_ms,
    )

    transport_reasons: List[str] = list(ws_status.get("transport_reasons") or [])
    payload_reasons: List[str] = []
    warning_reasons: List[str] = []

    def _append_orderbook_payload_issue(issue: str) -> None:
        issue_s = str(issue or "").strip()
        if not issue_s:
            raise RuntimeError("orderbook payload issue is empty (STRICT)")
        if recovery_ok:
            warning_reasons.append(f"feed:recoverable_{issue_s}")
        else:
            payload_reasons.append(issue_s)

    result: Dict[str, Any] = {
        "symbol": sym,
        "has_orderbook": ob is not None,
        "transport_ok": bool(ws_status.get("transport_ok", False)),
        "market_feed_ok": True,
        "feed_activity_warning": False,
        "payload_ok": False,
        "transport_reasons": transport_reasons,
        "payload_reasons": payload_reasons,
        "warnings": warning_reasons,
        "orderbook_update_delay_ms": None,
        "orderbook_recv_delay_ms": None,
        "market_event_delay_ms": ws_status.get("market_event_delay_ms"),
        "last_ws_message_ts": ws_status.get("last_message_ts"),
        "last_orderbook_recv_ts": orderbook_last_recv_ts,
        "last_pong_ts": ws_status.get("last_pong_ts"),
        "last_update_id": None,
        "orderbook_source": ob_status.get("source"),
        "stream_aligned": ob_status.get("stream_aligned"),
        "snapshot_last_update_id": ob_status.get("snapshot_last_update_id"),
        "bootstrapping": ob_status.get("bootstrapping"),
        "resync_reason": ob_status.get("resync_reason"),
        "last_resync_ts": ob_status.get("last_resync_ts"),
        "recovery_context": recovery_ok,
        "recovery_context_reason": recovery_context_reason,
        "circuit_breaker_open": bool(ws_status.get("circuit_breaker_open", False)),
        "ok": False,
        "reasons": [],
    }

    ws_state = str(ws_status.get("state") or "").upper().strip()
    last_open_ts = ws_status.get("last_open_ts")

    if ob is None:
        if recovery_ok:
            warning_reasons.append(
                f"feed:orderbook_recovery_no_snapshot ({recovery_context_reason})"
                if recovery_context_reason is not None
                else "feed:orderbook_recovery_no_snapshot"
            )
        else:
            if ws_state in ("OPEN", "OPENING", "RECONNECTING"):
                if last_open_ts is not None:
                    open_age_sec = max(0, now_ms - int(last_open_ts)) / 1000.0
                    if open_age_sec <= MARKET_EVENT_MAX_DELAY_SEC:
                        warning_reasons.append("feed:startup_no_orderbook_yet")
                    else:
                        payload_reasons.append("payload:no_orderbook")
                        _maybe_force_reconnect_on_orderbook_feed_gap(
                            sym,
                            ws_status=ws_status,
                            reason=f"orderbook_missing_after_open>{MARKET_EVENT_MAX_DELAY_SEC}s",
                        )
                else:
                    warning_reasons.append("feed:opening_no_orderbook_yet")
            else:
                payload_reasons.append("payload:no_orderbook")
    else:
        ts = ob.get("ts")
        if ts is None:
            _append_orderbook_payload_issue("payload:no_ts")
        else:
            result["orderbook_update_delay_ms"] = max(0, now_ms - int(ts))

        bids = ob.get("bids") or []
        asks = ob.get("asks") or []
        if not bids:
            _append_orderbook_payload_issue("payload:empty_bids")
        if not asks:
            _append_orderbook_payload_issue("payload:empty_asks")

        best_bid = ob.get("bestBid")
        best_ask = ob.get("bestAsk")
        if best_bid is None or best_ask is None:
            _append_orderbook_payload_issue("payload:no_best_prices")
        else:
            try:
                bb = float(best_bid)
                ba = float(best_ask)
                if ba <= bb:
                    _append_orderbook_payload_issue("payload:crossed_book(bestAsk<=bestBid)")
            except Exception:
                _append_orderbook_payload_issue("payload:invalid_best_prices")

        last_update_id = ob.get("lastUpdateId")
        if last_update_id is None:
            _append_orderbook_payload_issue("payload:no_last_update_id")
        else:
            try:
                parsed_update_id = int(last_update_id)
            except Exception:
                _append_orderbook_payload_issue("payload:invalid_last_update_id")
            else:
                if parsed_update_id <= 0:
                    _append_orderbook_payload_issue("payload:non_positive_last_update_id")
                result["last_update_id"] = parsed_update_id

        stream_aligned = ob_status.get("stream_aligned")
        source = ob_status.get("source")
        if source == "ws_diff_depth" and stream_aligned is not True:
            _append_orderbook_payload_issue("payload:ws_diff_depth_not_aligned")

    if orderbook_last_recv_ts is None:
        if ob is not None:
            if recovery_ok:
                warning_reasons.append(
                    f"feed:no_orderbook_recv_ts_recovering ({recovery_context_reason})"
                    if recovery_context_reason is not None
                    else "feed:no_orderbook_recv_ts_recovering"
                )
            else:
                warning_reasons.append("feed:no_orderbook_recv_ts")
    else:
        recv_delay_ms = max(0, now_ms - int(orderbook_last_recv_ts))
        result["orderbook_recv_delay_ms"] = recv_delay_ms
        recv_delay_sec = recv_delay_ms / 1000.0
        if recv_delay_sec > MARKET_EVENT_MAX_DELAY_SEC:
            if recovery_ok:
                warning_reasons.append(
                    f"feed:orderbook_recv_delay_recovering>{MARKET_EVENT_MAX_DELAY_SEC} "
                    f"(got={recv_delay_sec:.1f}, reason={recovery_context_reason})"
                )
            else:
                warning_reasons.append(
                    f"feed:orderbook_recv_delay_sec>{MARKET_EVENT_MAX_DELAY_SEC} (got={recv_delay_sec:.1f})"
                )

                if recv_delay_sec > MARKET_EVENT_MAX_DELAY_SEC:
                    if recovery_ok:
                        warning_reasons.append(
                            f"feed:orderbook_recv_delay_recovering>{MARKET_EVENT_MAX_DELAY_SEC} "
                            f"(got={recv_delay_sec:.1f}, reason={recovery_context_reason})"
                        )
                else:
                    warning_reasons.append(
                        f"feed:orderbook_recv_delay_sec>{MARKET_EVENT_MAX_DELAY_SEC} (got={recv_delay_sec:.1f})"
                    )           

                    # reconnect 는 2배 delay 이후에만 수행
                    if recv_delay_sec > (MARKET_EVENT_MAX_DELAY_SEC * 2):
                        _maybe_force_reconnect_on_orderbook_feed_gap(
                            sym,
                            ws_status=ws_status,
                            reason=f"orderbook_feed_stale>{MARKET_EVENT_MAX_DELAY_SEC}s recv_delay={recv_delay_sec:.1f}s",
                        )
                _maybe_force_reconnect_on_orderbook_feed_gap(
                    sym,
                    ws_status=ws_status,
                    reason=f"orderbook_feed_stale>{MARKET_EVENT_MAX_DELAY_SEC}s recv_delay={recv_delay_sec:.1f}s",
                )

    result["payload_ok"] = len(payload_reasons) == 0
    result["market_feed_ok"] = len(warning_reasons) == 0
    result["feed_activity_warning"] = len(warning_reasons) > 0
    result["reasons"] = transport_reasons + payload_reasons
    result["ok"] = bool(result["transport_ok"]) and bool(result["payload_ok"])
    return result


def _maybe_log_health_fail(snapshot: Dict[str, Any]) -> None:
    if snapshot.get("overall_ok", True):
        return

    parts: List[str] = []
    ws_status = snapshot.get("ws") or {}
    if not ws_status.get("ok", False):
        parts.append(f"ws:{'|'.join(ws_status.get('reasons') or [])}")

    for iv, st in (snapshot.get("klines") or {}).items():
        if not st.get("ok", False):
            parts.append(f"kline:{iv}:{'|'.join(st.get('reasons') or [])}")

    ob = snapshot.get("orderbook") or {}
    if not ob.get("ok", False):
        parts.append(f"ob:{'|'.join(ob.get('reasons') or [])}")

    key = ";".join(parts)[:600]
    now = time.time()

    with _health_fail_lock:
        if key == shared._last_health_fail_key and (now - shared._last_health_fail_log_ts) < shared._HEALTH_FAIL_LOG_SUPPRESS_SEC:
            return
        shared._last_health_fail_key = key
        shared._last_health_fail_log_ts = now

    log(f"[MD_BINANCE_WS HEALTH_FAIL] {snapshot.get('symbol')} reasons={key}")


def get_health_snapshot(symbol: str) -> Dict[str, Any]:
    sym = _normalize_symbol(symbol)
    snapshot: Dict[str, Any] = {
        "symbol": sym,
        "overall_ok": True,
        "overall_kline_ok": True,
        "overall_orderbook_ok": True,
        "overall_transport_ok": True,
        "transport_ok": True,
        "market_feed_ok": True,
        "ws": {},
        "klines": {},
        "orderbook": {},
        "overall_reasons": [],
        "overall_warnings": [],
        "checked_at_ms": _now_ms(),
        "required_intervals": list(HEALTH_REQUIRED_INTERVALS),
        "ws_required_intervals": list(shared.REQUIRED_INTERVALS),
        "feature_required_intervals": list(shared.FEATURE_REQUIRED_INTERVALS),
        "circuit_breaker_open": False,
    }

    overall_ok = True
    overall_kline_ok = True
    overall_orderbook_ok = True
    overall_transport_ok = True
    overall_market_feed_ok = True
    overall_reasons: List[str] = []
    overall_warnings: List[str] = []

    ws_status = get_ws_status(sym)
    snapshot["ws"] = ws_status
    snapshot["circuit_breaker_open"] = bool(ws_status.get("circuit_breaker_open", False))

    if not ws_status.get("transport_ok", False):
        overall_ok = False
        overall_transport_ok = False
        overall_reasons.append(f"ws:{'|'.join(ws_status.get('reasons') or [])}")

    ws_warning_items = list(ws_status.get("warnings") or [])
    if ws_warning_items:
        overall_market_feed_ok = False
        overall_warnings.extend([f"ws:{w}" for w in ws_warning_items])

    kline_map: Dict[str, Any] = {}
    for iv in HEALTH_REQUIRED_INTERVALS:
        k_status = _compute_kline_health(sym, iv)
        kline_map[iv] = k_status
        if not k_status.get("ok", False):
            overall_ok = False
            overall_kline_ok = False
            overall_reasons.append(f"kline:{iv}:{'|'.join(k_status.get('reasons') or [])}")

    ob_status = _compute_orderbook_health(sym)
    snapshot["orderbook"] = ob_status
    if not ob_status.get("ok", False):
        overall_ok = False
        overall_orderbook_ok = False
        overall_reasons.append(f"orderbook:{'|'.join(ob_status.get('reasons') or [])}")

    ob_warning_items = list(ob_status.get("warnings") or [])
    if ob_warning_items:
        overall_market_feed_ok = False
        overall_warnings.extend([f"orderbook:{w}" for w in ob_warning_items])

    snapshot["klines"] = kline_map
    snapshot["overall_ok"] = bool(overall_ok)
    snapshot["overall_kline_ok"] = bool(overall_kline_ok)
    snapshot["overall_orderbook_ok"] = bool(overall_orderbook_ok)
    snapshot["overall_transport_ok"] = bool(overall_transport_ok)
    snapshot["transport_ok"] = bool(overall_transport_ok)
    snapshot["market_feed_ok"] = bool(overall_market_feed_ok)
    snapshot["overall_reasons"] = _dedupe_keep_order(overall_reasons)
    snapshot["overall_warnings"] = _dedupe_keep_order(overall_warnings)
    snapshot["has_warning"] = len(snapshot["overall_warnings"]) > 0

    _maybe_log_health_fail(snapshot)
    return snapshot


def is_data_healthy(symbol: str) -> bool:
    return bool(get_health_snapshot(symbol).get("overall_ok", False))


def get_dashboard_ws_telemetry_snapshot(symbol: str, *, intervals: Optional[List[str]] = None) -> Dict[str, Any]:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise RuntimeError("symbol is required (STRICT)")

    normalized_intervals = _normalize_required_intervals_for_dashboard(intervals)
    now_ms = _now_ms()

    ws_status = get_ws_status(sym)
    orderbook_status = get_orderbook_buffer_status(sym)

    kline_latency_sec_by_tf: Dict[str, Optional[float]] = {}
    kline_last_ts_ms_by_tf: Dict[str, Optional[int]] = {}
    kline_recv_ts_ms_by_tf: Dict[str, Optional[int]] = {}

    for iv in normalized_intervals:
        k_status = get_kline_buffer_status(sym, iv)
        kline_latency_sec_by_tf[iv] = _ms_to_sec_optional(k_status.get("delay_ms"), f"kline[{iv}].delay_ms")
        last_ts = k_status.get("last_ts")
        if last_ts is not None:
            last_ts = _require_positive_int(last_ts, f"kline[{iv}].last_ts")
        last_recv_ts = k_status.get("last_recv_ts")
        if last_recv_ts is not None:
            last_recv_ts = _require_positive_int(last_recv_ts, f"kline[{iv}].last_recv_ts")
        kline_last_ts_ms_by_tf[iv] = last_ts
        kline_recv_ts_ms_by_tf[iv] = last_recv_ts

    last_ws_message_ts_ms = ws_status.get("last_message_ts")
    if last_ws_message_ts_ms is not None:
        last_ws_message_ts_ms = _require_positive_int(last_ws_message_ts_ms, "ws_status.last_message_ts")

    last_ws_message_latency_sec = _ms_to_sec_optional(ws_status.get("market_event_delay_ms"), "ws_status.market_event_delay_ms")
    orderbook_latency_sec = _ms_to_sec_optional(orderbook_status.get("payload_delay_ms"), "orderbook.payload_delay_ms")

    data_freshness_sec: Optional[float]
    if last_ws_message_latency_sec is not None:
        data_freshness_sec = last_ws_message_latency_sec
    else:
        candidate_values = [v for v in kline_latency_sec_by_tf.values() if v is not None]
        if orderbook_latency_sec is not None:
            candidate_values.append(orderbook_latency_sec)
        data_freshness_sec = min(candidate_values) if candidate_values else None

    return {
        "symbol": sym,
        "source": "ws",
        "checked_at_ms": now_ms,
        "connection_status": _derive_dashboard_connection_status(sym),
        "transport_ok": bool(ws_status.get("transport_ok", False)),
        "market_feed_ok": bool(ws_status.get("market_feed_ok", False)),
        "warnings": list(ws_status.get("warnings") or []),
        "reasons": list(ws_status.get("reasons") or []),
        "circuit_breaker_open": bool(ws_status.get("circuit_breaker_open", False)),
        "circuit_breaker_open_until_ts": ws_status.get("circuit_breaker_open_until_ts"),
        "circuit_breaker_last_reason": ws_status.get("circuit_breaker_last_reason"),
        "last_ws_message_ts_ms": last_ws_message_ts_ms,
        "last_ws_message_latency_sec": last_ws_message_latency_sec,
        "data_freshness_sec": data_freshness_sec,
        "kline_latency_sec_by_tf": kline_latency_sec_by_tf,
        "kline_last_ts_ms_by_tf": kline_last_ts_ms_by_tf,
        "kline_recv_ts_ms_by_tf": kline_recv_ts_ms_by_tf,
        "orderbook_latency_sec": orderbook_latency_sec,
        "orderbook_payload_ts_ms": orderbook_status.get("payload_ts"),
        "orderbook_recv_ts_ms": orderbook_status.get("last_recv_ts"),
        "orderbook_source": orderbook_status.get("source"),
        "orderbook_stream_aligned": orderbook_status.get("stream_aligned"),
        "orderbook_snapshot_last_update_id": orderbook_status.get("snapshot_last_update_id"),
        "orderbook_bootstrapping": orderbook_status.get("bootstrapping"),
        "orderbook_resync_reason": orderbook_status.get("resync_reason"),
        "orderbook_last_resync_ts": orderbook_status.get("last_resync_ts"),
    }


__all__ = [
    "get_health_snapshot",
    "is_data_healthy",
    "get_dashboard_ws_telemetry_snapshot",
]