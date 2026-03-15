# -*- coding: utf-8 -*-
# tests_tradegrade/debug_orderbook_alignment_probe.py
"""
========================================================
FILE: tests_tradegrade/debug_orderbook_alignment_probe.py
ROLE:
- Binance WS orderbook/kline 정렬 상태를 실시간 관측하는 디버그 프로브
- production 코드 수정 없이 WS transport / kline / orderbook recovery 상태를 추적한다
- orderbook false resync / replay ignored / kline repair 발생 패턴을 수집한다

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE DEBUG
- 운영 코드 직접 수정 없이 관측만 수행한다
- 프로브는 public API 우선 사용, 필요한 경우 log tap 으로 보조 계수만 수집한다
- 진단 실패를 숨기지 않고 즉시 예외를 노출한다
- 민감정보 출력 금지

CHANGE HISTORY:
- 2026-03-15
  1) ADD(INIT): start_ws_loop + ws/health/kline/orderbook 상태 1초 요약 출력
  2) ADD(OBSERVABILITY): orderbook replay ignored / resync enter / kline repair 카운터 수집
  3) ADD(STATE): orderbook resync_reason / stream_aligned / last_update_id 변화 JSON 이벤트 출력
  4) ADD(SAFETY): monkey patch log 원복 처리 및 KeyboardInterrupt 정상 종료 처리
========================================================
"""

from __future__ import annotations

import inspect
import json
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

import infra.market_data_ws as market_data_ws
import infra.market_data_ws_kline as market_data_ws_kline
import infra.market_data_ws_orderbook as market_data_ws_orderbook
import infra.market_data_ws_shared as market_data_ws_shared


SYMBOL: str = "BTCUSDT"
SUMMARY_INTERVAL_SEC: float = 1.0
PROBE_DURATION_SEC: float = 180.0


@dataclass
class ProbeCounters:
    orderbook_replay_ignored: int = 0
    orderbook_resync_enter: int = 0
    orderbook_bootstrap_done: int = 0
    kline_repairs: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            "orderbook_replay_ignored": int(self.orderbook_replay_ignored),
            "orderbook_resync_enter": int(self.orderbook_resync_enter),
            "orderbook_bootstrap_done": int(self.orderbook_bootstrap_done),
            "kline_repairs": int(self.kline_repairs),
        }


class ProbeLogTap:
    """
    여러 모듈의 log 함수를 감싸서 디버그 카운터를 수집한다.
    production 동작은 유지하고, 메시지를 파싱해서 보조 지표만 만든다.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._installed = False
        self._originals: Dict[tuple[str, str], Callable[[str], Any]] = {}
        self.counters = ProbeCounters()

    def install(self) -> None:
        if self._installed:
            raise RuntimeError("ProbeLogTap already installed (STRICT)")

        self._patch_module_log(market_data_ws, "infra.market_data_ws")
        self._patch_module_log(market_data_ws_kline, "infra.market_data_ws_kline")
        self._patch_module_log(market_data_ws_orderbook, "infra.market_data_ws_orderbook")
        self._patch_module_log(market_data_ws_shared, "infra.market_data_ws_shared")
        self._installed = True

    def restore(self) -> None:
        if not self._installed:
            return

        for (module_name, attr_name), original in list(self._originals.items()):
            module = self._resolve_module_by_name(module_name)
            setattr(module, attr_name, original)

        self._originals.clear()
        self._installed = False

    def _resolve_module_by_name(self, module_name: str) -> Any:
        if module_name == "infra.market_data_ws":
            return market_data_ws
        if module_name == "infra.market_data_ws_kline":
            return market_data_ws_kline
        if module_name == "infra.market_data_ws_orderbook":
            return market_data_ws_orderbook
        if module_name == "infra.market_data_ws_shared":
            return market_data_ws_shared
        raise RuntimeError(f"unknown module_name (STRICT): {module_name}")

    def _patch_module_log(self, module: Any, module_name: str) -> None:
        if not hasattr(module, "log"):
            raise RuntimeError(f"{module_name}.log missing (STRICT)")

        original = getattr(module, "log")
        if not callable(original):
            raise RuntimeError(f"{module_name}.log must be callable (STRICT)")

        self._originals[(module_name, "log")] = original

        def _wrapped(msg: str) -> Any:
            text = str(msg)
            self._consume_message(text)
            return original(text)

        setattr(module, "log", _wrapped)

    def _consume_message(self, text: str) -> None:
        with self._lock:
            if "orderbook replay/rollback ignored" in text:
                self.counters.orderbook_replay_ignored += 1

            if "[MD_BINANCE_WS][ORDERBOOK][RESYNC_TRIGGER]" in text:
                self.counters.orderbook_resync_enter += 1

            if "[MD_BINANCE_WS] orderbook snapshot bootstrapped" in text:
                self.counters.orderbook_bootstrap_done += 1

            if "[WS_RECOVERY] ws_kline[" in text and "gap repaired from REST" in text:
                self.counters.kline_repairs += 1


def _utc_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _utc_iso_from_ms(ts_ms: Optional[int]) -> Optional[str]:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat(timespec="milliseconds")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _format_age_ms(age_ms: Optional[int]) -> str:
    if age_ms is None:
        return "-"
    return f"{age_ms / 1000.0:.1f}s"


def _to_bool(v: Any) -> Optional[bool]:
    if isinstance(v, bool):
        return v
    return None


def _pick_first(mapping: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None


def _call_maybe_symbol(fn: Callable[..., Any], symbol: str) -> Any:
    sig = inspect.signature(fn)
    params = list(sig.parameters.values())

    if len(params) == 0:
        return fn()

    required_positional = [
        p
        for p in params
        if p.default is inspect._empty
        and p.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
    ]

    if len(required_positional) == 0:
        return fn()

    return fn(symbol)


def _get_ws_status_strict(symbol: str) -> Dict[str, Any]:
    raw = _call_maybe_symbol(market_data_ws.get_ws_status, symbol)
    if not isinstance(raw, dict):
        raise RuntimeError(f"get_ws_status() must return dict (STRICT), got={type(raw).__name__}")
    return raw


def _get_health_snapshot_strict(symbol: str) -> Dict[str, Any]:
    raw = _call_maybe_symbol(market_data_ws.get_health_snapshot, symbol)
    if not isinstance(raw, dict):
        raise RuntimeError(f"get_health_snapshot() must return dict (STRICT), got={type(raw).__name__}")
    return raw


def _derive_ws_state(ws_status: Dict[str, Any]) -> str:
    raw = _pick_first(ws_status, "state", "ws_state", "connection_state", "status", "level")
    if isinstance(raw, str) and raw.strip():
        return raw.strip().upper()
    return "UNKNOWN"


def _derive_transport_ok(ws_status: Dict[str, Any], ws_state: str) -> bool:
    explicit = _pick_first(ws_status, "tx_ok", "transport_ok", "socket_ok", "connected")
    explicit_b = _to_bool(explicit)
    if explicit_b is not None:
        return explicit_b

    return ws_state not in {"ERROR", "CLOSED", "FATAL"}


def _derive_health_ok(health_snapshot: Dict[str, Any]) -> bool:
    explicit = _pick_first(health_snapshot, "ok", "health_ok")
    explicit_b = _to_bool(explicit)
    if explicit_b is not None:
        return explicit_b

    ws_block = health_snapshot.get("ws")
    if isinstance(ws_block, dict):
        nested = _pick_first(ws_block, "overall_ok", "ok")
        nested_b = _to_bool(nested)
        if nested_b is not None:
            return nested_b

    return False


def _derive_has_warning(health_snapshot: Dict[str, Any]) -> bool:
    explicit = _pick_first(health_snapshot, "has_warning", "warn", "warning")
    explicit_b = _to_bool(explicit)
    if explicit_b is not None:
        return explicit_b

    ws_block = health_snapshot.get("ws")
    if isinstance(ws_block, dict):
        nested = _pick_first(ws_block, "has_warning")
        nested_b = _to_bool(nested)
        if nested_b is not None:
            return nested_b

        warnings = _pick_first(ws_block, "overall_warnings", "warnings")
        if isinstance(warnings, list):
            return len(warnings) > 0

    return False


def _derive_feed_ok(
    ws_status: Dict[str, Any],
    health_snapshot: Dict[str, Any],
    kline_1m: Dict[str, Any],
    kline_5m: Dict[str, Any],
    orderbook: Dict[str, Any],
) -> bool:
    explicit = _pick_first(
        ws_status,
        "feed_ok",
        "market_feed_ok",
    )
    explicit_b = _to_bool(explicit)
    if explicit_b is not None:
        return explicit_b

    explicit_h = _pick_first(
        health_snapshot,
        "feed_ok",
        "market_feed_ok",
        "ws_overall_ok",
    )
    explicit_h_b = _to_bool(explicit_h)
    if explicit_h_b is not None:
        return explicit_h_b

    ws_block = health_snapshot.get("ws")
    if isinstance(ws_block, dict):
        nested = _pick_first(ws_block, "overall_ok", "feed_ok", "market_feed_ok")
        nested_b = _to_bool(nested)
        if nested_b is not None:
            return nested_b

    has_orderbook = bool(orderbook.get("has_orderbook", False))
    k1_last = kline_1m.get("last_ts")
    k5_last = kline_5m.get("last_ts")
    return has_orderbook and (k1_last is not None) and (k5_last is not None)


def _safe_kline_status(symbol: str, interval: str) -> Dict[str, Any]:
    raw = market_data_ws.get_kline_buffer_status(symbol, interval)
    if not isinstance(raw, dict):
        raise RuntimeError(
            f"get_kline_buffer_status({symbol}, {interval}) must return dict (STRICT)"
        )
    return raw


def _safe_orderbook_status(symbol: str) -> Dict[str, Any]:
    raw = market_data_ws.get_orderbook_buffer_status(symbol)
    if not isinstance(raw, dict):
        raise RuntimeError(f"get_orderbook_buffer_status({symbol}) must return dict (STRICT)")
    return raw


def _emit_json(kind: str, payload: Dict[str, Any]) -> None:
    envelope = {
        "ts_ms": _now_ms(),
        "ts_utc": _utc_iso_now(),
        "kind": kind,
    }
    envelope.update(payload)
    print(json.dumps(envelope, ensure_ascii=False))


class StateEdgeEmitter:
    """
    상태 변화가 생길 때만 JSON 이벤트를 뿌린다.
    """

    def __init__(self) -> None:
        self._prev_resync_ts: Optional[int] = None
        self._prev_resync_reason: Optional[str] = None
        self._prev_stream_aligned: Optional[bool] = None
        self._prev_last_update_id: Optional[int] = None
        self._prev_bootstrapping: Optional[bool] = None

    def maybe_emit(self, *, symbol: str, orderbook_status: Dict[str, Any]) -> None:
        resync_ts = orderbook_status.get("last_resync_ts")
        resync_reason = orderbook_status.get("resync_reason")
        stream_aligned = orderbook_status.get("stream_aligned")
        last_update_id = orderbook_status.get("last_update_id")
        bootstrapping = orderbook_status.get("bootstrapping")

        changed = (
            resync_ts != self._prev_resync_ts
            or resync_reason != self._prev_resync_reason
            or stream_aligned != self._prev_stream_aligned
            or last_update_id != self._prev_last_update_id
            or bootstrapping != self._prev_bootstrapping
        )

        if changed:
            _emit_json(
                "probe_orderbook_state",
                {
                    "symbol": symbol,
                    "state": {
                        "stream_aligned": stream_aligned,
                        "bootstrapping": bootstrapping,
                        "last_update_id": last_update_id,
                        "payload_ts": orderbook_status.get("payload_ts"),
                        "payload_ts_utc": _utc_iso_from_ms(orderbook_status.get("payload_ts")),
                        "last_recv_ts": orderbook_status.get("last_recv_ts"),
                        "last_recv_ts_utc": _utc_iso_from_ms(orderbook_status.get("last_recv_ts")),
                        "last_resync_ts": resync_ts,
                        "last_resync_ts_utc": _utc_iso_from_ms(resync_ts),
                        "resync_reason": resync_reason,
                        "snapshot_last_update_id": orderbook_status.get("snapshot_last_update_id"),
                        "last_local_apply_ts": orderbook_status.get("last_local_apply_ts"),
                        "last_local_apply_ts_utc": _utc_iso_from_ms(orderbook_status.get("last_local_apply_ts")),
                    },
                },
            )

        self._prev_resync_ts = resync_ts
        self._prev_resync_reason = resync_reason
        self._prev_stream_aligned = stream_aligned
        self._prev_last_update_id = last_update_id
        self._prev_bootstrapping = bootstrapping


def _print_summary(
    *,
    symbol: str,
    counters: ProbeCounters,
    ws_status: Dict[str, Any],
    health_snapshot: Dict[str, Any],
    kline_1m: Dict[str, Any],
    kline_5m: Dict[str, Any],
    orderbook: Dict[str, Any],
) -> None:
    ws_state = _derive_ws_state(ws_status)
    tx_ok = _derive_transport_ok(ws_status, ws_state)
    health_ok = _derive_health_ok(health_snapshot)
    has_warning = _derive_has_warning(health_snapshot)
    feed_ok = _derive_feed_ok(ws_status, health_snapshot, kline_1m, kline_5m, orderbook)

    one_m_last = kline_1m.get("last_ts")
    one_m_age_ms = kline_1m.get("delay_ms")
    five_m_last = kline_5m.get("last_ts")
    five_m_age_ms = kline_5m.get("delay_ms")

    ob_last_u = orderbook.get("last_update_id")
    ob_payload_age_ms = orderbook.get("payload_delay_ms")
    ob_recv_age_ms = orderbook.get("recv_delay_ms")
    ob_aligned = orderbook.get("stream_aligned")
    ob_bootstrapping = orderbook.get("bootstrapping")
    ob_resync_reason = orderbook.get("resync_reason")

    print(
        "[SUMMARY] "
        f"t={_utc_iso_now()} "
        f"ws={ws_state} "
        f"tx_ok={tx_ok} "
        f"feed_ok={feed_ok} "
        f"health_ok={health_ok} "
        f"warn={has_warning} "
        f"1m_last={one_m_last} "
        f"1m_age={_format_age_ms(one_m_age_ms)} "
        f"5m_last={five_m_last} "
        f"5m_age={_format_age_ms(five_m_age_ms)} "
        f"ob_last_u={ob_last_u} "
        f"ob_payload_age={_format_age_ms(ob_payload_age_ms)} "
        f"ob_recv_age={_format_age_ms(ob_recv_age_ms)} "
        f"aligned={ob_aligned} "
        f"boot={ob_bootstrapping} "
        f"resync={'Y' if ob_resync_reason else 'N'} "
        f"replay={counters.orderbook_replay_ignored} "
        f"resync_enter={counters.orderbook_resync_enter} "
        f"kline_repairs={counters.kline_repairs}"
    )


def main() -> None:
    tap = ProbeLogTap()
    emitter = StateEdgeEmitter()

    tap.install()
    try:
        market_data_ws.start_ws_loop(SYMBOL)

        started_at = time.time()
        while True:
            elapsed = time.time() - started_at
            if elapsed > PROBE_DURATION_SEC:
                _emit_json(
                    "probe_done",
                    {
                        "symbol": SYMBOL,
                        "elapsed_sec": round(elapsed, 3),
                        "counters": tap.counters.to_dict(),
                    },
                )
                return

            ws_status = _get_ws_status_strict(SYMBOL)
            health_snapshot = _get_health_snapshot_strict(SYMBOL)
            kline_1m = _safe_kline_status(SYMBOL, "1m")
            kline_5m = _safe_kline_status(SYMBOL, "5m")
            orderbook = _safe_orderbook_status(SYMBOL)

            emitter.maybe_emit(symbol=SYMBOL, orderbook_status=orderbook)
            _print_summary(
                symbol=SYMBOL,
                counters=tap.counters,
                ws_status=ws_status,
                health_snapshot=health_snapshot,
                kline_1m=kline_1m,
                kline_5m=kline_5m,
                orderbook=orderbook,
            )

            time.sleep(SUMMARY_INTERVAL_SEC)

    except KeyboardInterrupt:
        _emit_json(
            "probe_interrupted",
            {
                "symbol": SYMBOL,
                "counters": tap.counters.to_dict(),
            },
        )
        raise
    finally:
        tap.restore()


if __name__ == "__main__":
    main()