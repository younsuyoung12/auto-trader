# -*- coding: utf-8 -*-
# infra/market_data_ws_shared.py
"""
========================================================
FILE: infra/market_data_ws_shared.py
ROLE:
- market_data_ws 분리 리팩터링 공용 계약 / 설정 / 상태 저장소
- WS transport / kline / orderbook / health 계층이 공유하는 단일 런타임 상태를 제공한다
- settings.py SSOT 검증, 공용 유틸, 공용 lock/state, ws status/circuit-breaker 공용 API를 제공한다

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- 환경변수 직접 접근 금지, settings.py(SSOT)만 사용
- 공용 상태는 이 파일에서만 선언한다
- 하위 모듈은 공용 상태를 직접 임의 생성하지 않는다

CHANGE HISTORY:
- 2026-03-15:
  1) KEEP(CONTRACT): orderbook stream contract 는 diff-depth parser(U/u/pu)와 일치하도록
     @depth@100ms 를 유지한다
  2) NOTE(GUARD): parser/storage contract 동시 교체 없이 @depth5@100ms 로 변경 금지
========================================================
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any, Dict, List, Mapping, Optional, Tuple

import requests
import websocket  # pip install websocket-client

from infra.telelog import log
from settings import SETTINGS

SET = SETTINGS


class WSProtocolError(RuntimeError):
    """Binance multiplex payload protocol violation (STRICT)."""


KlineRow = Tuple[int, float, float, float, float, float, bool]
LegacyKlineRowWithVolume = Tuple[int, float, float, float, float, float]
LegacyKlineRow = Tuple[int, float, float, float, float]

_DASHBOARD_WS_STATUS_CONNECTED = "CONNECTED"
_DASHBOARD_WS_STATUS_RECONNECTING = "RECONNECTING"
_DASHBOARD_WS_STATUS_DISCONNECTED = "DISCONNECTED"

_ORDERBOOK_SNAPSHOT_LIMIT = 1000
_ORDERBOOK_TOP_N = 5
_ORDERBOOK_PENDING_EVENT_MAX = 5000


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_dump_for_log(obj: Any, max_len: int = 2000) -> str:
    try:
        s = json.dumps(obj, ensure_ascii=False, default=str)
    except Exception:
        s = str(obj)
    if len(s) > max_len:
        return s[:max_len] + f"... (truncated, total_len={len(s)})"
    return s


def _normalize_symbol(sym: str) -> str:
    s = (sym or "").upper().strip()
    if not s:
        return ""
    return s.replace("-", "").replace("/", "").replace("_", "")


def _normalize_interval(iv: Any) -> str:
    s = str(iv or "").strip()
    if not s:
        return ""
    if s.endswith("M"):
        return s
    return s.lower()


def _to_stream_symbol(sym: str) -> str:
    return _normalize_symbol(sym).lower()


def _dedupe_keep_order(items: List[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for item in items:
        s = str(item).strip()
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _normalize_interval_list_from_settings(raw: Any, *, name: str) -> List[str]:
    if not isinstance(raw, list) or not raw:
        raise RuntimeError(f"settings.{name} must be non-empty list[str] (STRICT)")

    out: List[str] = []
    seen: set[str] = set()
    for idx, item in enumerate(raw):
        iv = _normalize_interval(item)
        if not iv:
            raise RuntimeError(f"settings.{name}[{idx}] invalid/empty interval (STRICT)")
        if iv in seen:
            raise RuntimeError(f"settings.{name} contains duplicate interval={iv!r} (STRICT)")
        seen.add(iv)
        out.append(iv)
    return out


def _require_bool(v: Any, name: str) -> bool:
    if not isinstance(v, bool):
        raise WSProtocolError(f"{name} must be bool")
    return v


def _require_int_ms(v: Any, name: str) -> int:
    if isinstance(v, bool):
        raise WSProtocolError(f"{name} must be int ms (bool not allowed)")
    try:
        iv = int(v)
    except Exception as e:
        raise WSProtocolError(f"{name} must be int ms: {e}") from e
    if iv <= 0:
        raise WSProtocolError(f"{name} must be > 0")
    return iv


def _require_float(v: Any, name: str, *, allow_zero: bool = False) -> float:
    if v is None:
        raise WSProtocolError(f"{name} missing")
    if isinstance(v, bool):
        raise WSProtocolError(f"{name} must be float (bool not allowed)")
    try:
        fv = float(v)
    except Exception as e:
        raise WSProtocolError(f"{name} must be float: {e}") from e
    if not (fv == fv) or fv in (float("inf"), float("-inf")):
        raise WSProtocolError(f"{name} must be finite")
    if allow_zero:
        if fv < 0:
            raise WSProtocolError(f"{name} must be >= 0")
    else:
        if fv <= 0:
            raise WSProtocolError(f"{name} must be > 0")
    return float(fv)


def _require_positive_int(v: Any, name: str) -> int:
    if isinstance(v, bool):
        raise RuntimeError(f"{name} must be int (bool not allowed)")
    try:
        iv = int(v)
    except Exception as e:
        raise RuntimeError(f"{name} must be int: {e}") from e
    if iv <= 0:
        raise RuntimeError(f"{name} must be > 0")
    return iv


def _require_nonnegative_int(v: Any, name: str) -> int:
    if isinstance(v, bool):
        raise RuntimeError(f"{name} must be int (bool not allowed)")
    try:
        iv = int(v)
    except Exception as e:
        raise RuntimeError(f"{name} must be int: {e}") from e
    if iv < 0:
        raise RuntimeError(f"{name} must be >= 0")
    return iv


def _require_positive_float(v: Any, name: str) -> float:
    if isinstance(v, bool):
        raise RuntimeError(f"{name} must be float (bool not allowed)")
    try:
        fv = float(v)
    except Exception as e:
        raise RuntimeError(f"{name} must be float: {e}") from e
    if not (fv == fv) or fv in (float("inf"), float("-inf")):
        raise RuntimeError(f"{name} must be finite")
    if fv <= 0:
        raise RuntimeError(f"{name} must be > 0")
    return float(fv)


def _require_interval_min_buffer_mapping(v: Any, name: str) -> Dict[str, int]:
    if not isinstance(v, Mapping):
        raise RuntimeError(f"{name} must be mapping[str,int] (STRICT)")
    out: Dict[str, int] = {}
    for raw_iv, raw_min in v.items():
        iv = _normalize_interval(raw_iv)
        if not iv:
            raise RuntimeError(f"{name} contains empty/invalid interval key (STRICT)")
        out[iv] = _require_positive_int(raw_min, f"{name}[{iv}]")
    return out


def _truncate_text(v: Any, max_len: int = 300) -> str:
    s = str(v)
    if len(s) <= max_len:
        return s
    return s[:max_len] + f"... (truncated, total_len={len(s)})"


def _extract_orderbook_event_ts_ms_strict(payload: Dict[str, Any]) -> int:
    raw_event_ts = payload.get("E")
    if raw_event_ts is None:
        raw_event_ts = payload.get("T")
    if raw_event_ts is None:
        raise WSProtocolError("orderbook missing exchange event time (E/T) (STRICT)")
    return _require_int_ms(raw_event_ts, "orderbook.event_ts")


def _ms_to_sec_optional(v: Optional[int], name: str) -> Optional[float]:
    if v is None:
        return None
    iv = _require_nonnegative_int(v, name)
    return iv / 1000.0


def _normalize_required_intervals_for_dashboard(intervals: Optional[List[str]]) -> List[str]:
    if intervals is None:
        return list(HEALTH_REQUIRED_INTERVALS)

    if not isinstance(intervals, list):
        raise RuntimeError("intervals must be list[str] or None (STRICT)")

    normalized: List[str] = []
    for idx, raw in enumerate(intervals):
        iv = _normalize_interval(raw)
        if not iv:
            raise RuntimeError(f"intervals[{idx}] invalid/empty (STRICT)")
        normalized.append(iv)

    if not normalized:
        raise RuntimeError("intervals must not be empty (STRICT)")
    return normalized


def _interval_to_ms_strict(interval: str) -> int:
    iv = _normalize_interval(interval)
    if not iv:
        raise RuntimeError("interval is required (STRICT)")

    unit = iv[-1]
    value_raw = iv[:-1]
    if not value_raw.isdigit():
        raise RuntimeError(f"interval format invalid (STRICT): {interval!r}")
    n = int(value_raw)
    if n <= 0:
        raise RuntimeError(f"interval value must be > 0 (STRICT): {interval!r}")

    if unit == "m":
        return n * 60_000
    if unit == "h":
        return n * 3_600_000
    if unit == "d":
        return n * 86_400_000
    if unit == "w":
        return n * 604_800_000
    if unit == "M":
        raise RuntimeError(f"monthly interval rollover unsupported in WS strict path: {interval!r}")

    raise RuntimeError(f"unsupported interval unit (STRICT): {interval!r}")


def _extract_quote_volume_from_rest_row_optional(row: Any, idx: int) -> Optional[float]:
    if isinstance(row, (list, tuple)):
        if len(row) > 7 and row[7] is not None:
            return _require_float(row[7], f"rest_klines[{idx}].quote_volume", allow_zero=True)
        return None

    if isinstance(row, dict):
        raw = row.get("q")
        if raw is None:
            raw = row.get("quoteVolume")
        if raw is None:
            raw = row.get("quote_volume")
        if raw is None:
            return None
        return _require_float(raw, f"rest_klines[{idx}].quote_volume", allow_zero=True)

    return None


def _derive_rest_kline_closed_strict(row: Any, idx: int, now_ms: int) -> bool:
    if isinstance(row, dict):
        explicit_closed = row.get("x")
        if explicit_closed is not None:
            return _require_bool(explicit_closed, f"rest_klines[{idx}].x")

        close_time_raw = row.get("T")
        if close_time_raw is None:
            close_time_raw = row.get("closeTime")
        if close_time_raw is None:
            close_time_raw = row.get("close_time")
        if close_time_raw is None:
            raise RuntimeError(f"rest_klines[{idx}] missing close time for closed-state derivation (STRICT)")

        close_time_ms = _require_int_ms(close_time_raw, f"rest_klines[{idx}].close_time")
        return bool(now_ms > close_time_ms)

    if isinstance(row, (list, tuple)):
        if len(row) < 7:
            raise RuntimeError(f"rest_klines[{idx}] missing close time (need len>=7) (STRICT)")
        close_time_ms = _require_int_ms(row[6], f"rest_klines[{idx}].close_time")
        return bool(now_ms > close_time_ms)

    raise RuntimeError(f"rest_klines[{idx}] invalid type for closed-state derivation (STRICT): {type(row).__name__}")


WS_INTERVALS: List[str] = _normalize_interval_list_from_settings(SET.ws_subscribe_tfs, name="ws_subscribe_tfs")
REQUIRED_INTERVALS: List[str] = _normalize_interval_list_from_settings(SET.ws_required_tfs, name="ws_required_tfs")
WS_BACKFILL_INTERVALS: List[str] = _normalize_interval_list_from_settings(SET.ws_backfill_tfs, name="ws_backfill_tfs")
FEATURE_REQUIRED_INTERVALS: List[str] = _normalize_interval_list_from_settings(
    SET.features_required_tfs,
    name="features_required_tfs",
)

_missing_required = [iv for iv in REQUIRED_INTERVALS if iv not in WS_INTERVALS]
if _missing_required:
    raise RuntimeError(
        "ws_required_tfs contains intervals not present in ws_subscribe_tfs. "
        f"missing={_missing_required}. Fix your settings to subscribe required intervals."
    )

_missing_backfill = [iv for iv in WS_BACKFILL_INTERVALS if iv not in WS_INTERVALS]
if _missing_backfill:
    raise RuntimeError(
        "ws_backfill_tfs contains intervals not present in ws_subscribe_tfs. "
        f"missing={_missing_backfill}. Fix your settings to backfill only subscribed intervals."
    )

_missing_feature = [iv for iv in FEATURE_REQUIRED_INTERVALS if iv not in WS_INTERVALS]
if _missing_feature:
    raise RuntimeError(
        "features_required_tfs contains intervals not present in ws_subscribe_tfs. "
        f"missing={_missing_feature}. Fix your settings to subscribe feature-required intervals."
    )

HEALTH_REQUIRED_INTERVALS: List[str] = _dedupe_keep_order(REQUIRED_INTERVALS + FEATURE_REQUIRED_INTERVALS)
if not HEALTH_REQUIRED_INTERVALS:
    raise RuntimeError("health required intervals empty (STRICT)")

_WS_COMBINED_BASE = str(SET.ws_combined_base or "").strip()
if not _WS_COMBINED_BASE:
    raise RuntimeError("settings.ws_combined_base is required (STRICT)")

if "?streams=" not in _WS_COMBINED_BASE:
    if _WS_COMBINED_BASE.endswith("/stream"):
        _WS_COMBINED_BASE = _WS_COMBINED_BASE + "?streams="
    else:
        raise RuntimeError(f"settings.ws_combined_base must contain '?streams=' (STRICT): {_WS_COMBINED_BASE!r}")

if not _WS_COMBINED_BASE.endswith("streams="):
    raise RuntimeError(f"settings.ws_combined_base must end with 'streams=' (STRICT): {_WS_COMBINED_BASE!r}")

_WS_REST_BASE = str(SET.binance_futures_rest_base or "").strip().rstrip("/")
if not _WS_REST_BASE:
    raise RuntimeError("settings.binance_futures_rest_base is required (STRICT)")
if not (_WS_REST_BASE.startswith("http://") or _WS_REST_BASE.startswith("https://")):
    raise RuntimeError(f"settings.binance_futures_rest_base invalid (STRICT): {_WS_REST_BASE!r}")

_WS_BOOTSTRAP_REST_ENABLED: bool = bool(SET.ws_bootstrap_rest_enabled)
_WS_REST_TIMEOUT_SEC: float = _require_positive_float(SET.ws_rest_timeout_sec, "ws_rest_timeout_sec")
_WS_REST_KLINES_LIMIT_CAP: int = 1500

KLINE_MIN_BUFFER: int = _require_positive_int(SET.ws_min_kline_buffer, "ws_min_kline_buffer")
KLINE_MAX_DELAY_SEC: float = _require_positive_float(SET.ws_max_kline_delay_sec, "ws_max_kline_delay_sec")
MARKET_EVENT_MAX_DELAY_SEC: float = _require_positive_float(
    SET.ws_market_event_max_delay_sec,
    "ws_market_event_max_delay_sec",
)
PONG_MAX_DELAY_SEC: float = _require_positive_float(SET.ws_pong_max_delay_sec, "ws_pong_max_delay_sec")
PONG_STARTUP_GRACE_SEC: float = _require_positive_float(
    SET.ws_pong_startup_grace_sec,
    "ws_pong_startup_grace_sec",
)
_WS_RECONNECT_GRACE_SEC: float = max(5.0, MARKET_EVENT_MAX_DELAY_SEC)

_WS_CIRCUIT_BREAKER_CONFIRM_N: int = 3
_WS_CIRCUIT_BREAKER_COOLDOWN_SEC: float = max(_WS_RECONNECT_GRACE_SEC, MARKET_EVENT_MAX_DELAY_SEC)

_LONG_TF_MIN_BUFFER_DEFAULT: int = _require_positive_int(
    SET.ws_min_kline_buffer_long_tf,
    "ws_min_kline_buffer_long_tf",
)

_BUILTIN_STRICT_MIN_BUFFER_BY_INTERVAL: Dict[str, int] = {
    "5m": 200,
    "15m": 200,
    "1h": 200,
    "4h": 200,
}

_SETTINGS_INTERVAL_MIN_BUFFER_BY_INTERVAL: Dict[str, int] = _require_interval_min_buffer_mapping(
    SET.ws_min_kline_buffer_by_interval,
    "ws_min_kline_buffer_by_interval",
)

_EFFECTIVE_INTERVAL_MIN_BUFFER_BY_INTERVAL: Dict[str, int] = dict(_BUILTIN_STRICT_MIN_BUFFER_BY_INTERVAL)
for _iv, _min_buf in _SETTINGS_INTERVAL_MIN_BUFFER_BY_INTERVAL.items():
    builtin_floor = _BUILTIN_STRICT_MIN_BUFFER_BY_INTERVAL.get(_iv)
    if builtin_floor is not None and _min_buf < builtin_floor:
        raise RuntimeError(
            f"ws_min_kline_buffer_by_interval[{_iv}]={_min_buf} violates strict downstream requirement "
            f"(need>={builtin_floor})"
        )
    prev = _EFFECTIVE_INTERVAL_MIN_BUFFER_BY_INTERVAL.get(_iv)
    _EFFECTIVE_INTERVAL_MIN_BUFFER_BY_INTERVAL[_iv] = max(_min_buf, prev or 0)

_HEALTH_FAIL_LOG_SUPPRESS_SEC: float = _require_positive_float(
    SET.ws_health_fail_log_suppress_sec,
    "ws_health_fail_log_suppress_sec",
)
_last_health_fail_log_ts: float = 0.0
_last_health_fail_key: str = ""
_health_fail_lock = threading.Lock()

_kline_buffers: Dict[Tuple[str, str], List[KlineRow]] = {}
_kline_last_recv_ts: Dict[Tuple[str, str], int] = {}
_kline_last_quote_volume: Dict[Tuple[str, str], Tuple[int, float]] = {}

_orderbook_buffers: Dict[str, Dict[str, Any]] = {}
_orderbook_last_update_id: Dict[str, int] = {}
_orderbook_last_recv_ts: Dict[str, int] = {}
_orderbook_book_state: Dict[str, Dict[str, Any]] = {}

_ws_connection_open: Dict[str, bool] = {}
_ws_last_open_ts: Dict[str, int] = {}
_ws_last_close_ts: Dict[str, int] = {}
_ws_last_close_text: Dict[str, str] = {}
_ws_last_message_ts: Dict[str, int] = {}
_ws_last_pong_ts: Dict[str, int] = {}
_ws_last_error_text: Dict[str, str] = {}
_ws_current_app: Dict[str, websocket.WebSocketApp] = {}
_ws_reconnect_request_ts: Dict[str, int] = {}
_ws_reconnect_request_reason: Dict[str, str] = {}
_ws_state_lock = threading.Lock()

_ws_cb_consecutive_failures: Dict[str, int] = {}
_ws_cb_open_until_ts: Dict[str, int] = {}
_ws_cb_last_reason: Dict[str, str] = {}
_ws_cb_trip_count: Dict[str, int] = {}
_ws_cb_lock = threading.Lock()

_kline_lock = threading.Lock()
_orderbook_lock = threading.Lock()

_max_required_buffer = max(
    [KLINE_MIN_BUFFER, _LONG_TF_MIN_BUFFER_DEFAULT, *_EFFECTIVE_INTERVAL_MIN_BUFFER_BY_INTERVAL.values()]
)
MAX_KLINES = max(500, _max_required_buffer)

_NO_BUF_LOG_SUPPRESS_SEC: float = _require_positive_float(
    SET.ws_no_buffer_log_suppress_sec,
    "ws_no_buffer_log_suppress_sec",
)
_no_buf_log_last: Dict[Tuple[str, str], float] = {}
_no_buf_log_lock = threading.Lock()

_started_ws_symbols: Dict[str, threading.Thread] = {}
_start_ws_lock = threading.Lock()

_rest_session = requests.Session()
_rest_session.headers.update({"User-Agent": "auto-trader/market-data-ws-bootstrap"})


def _rest_fetch_klines_strict(symbol: str, interval: str, limit: int) -> List[Any]:
    sym = _normalize_symbol(symbol)
    iv = _normalize_interval(interval)
    lim = _require_positive_int(limit, "rest_klines.limit")

    if lim > _WS_REST_KLINES_LIMIT_CAP:
        raise RuntimeError(
            f"rest klines limit exceeds cap (STRICT): interval={iv} limit={lim} cap={_WS_REST_KLINES_LIMIT_CAP}"
        )

    url = f"{_WS_REST_BASE}/fapi/v1/klines"
    try:
        resp = _rest_session.get(
            url,
            params={"symbol": sym, "interval": iv, "limit": lim},
            timeout=_WS_REST_TIMEOUT_SEC,
        )
    except Exception as e:
        raise RuntimeError(f"REST klines request failed (STRICT): symbol={sym} interval={iv} limit={lim}") from e

    if resp.status_code != 200:
        raise RuntimeError(
            f"REST klines HTTP {resp.status_code} (STRICT): symbol={sym} interval={iv} limit={lim} body={resp.text[:500]!r}"
        )

    try:
        payload = resp.json()
    except Exception as e:
        raise RuntimeError(f"REST klines response is not valid JSON (STRICT): symbol={sym} interval={iv}") from e

    if not isinstance(payload, list):
        raise RuntimeError(
            f"REST klines response root must be list (STRICT): symbol={sym} interval={iv} got={type(payload).__name__}"
        )
    if not payload:
        raise RuntimeError(f"REST klines empty (STRICT): symbol={sym} interval={iv} limit={lim}")
    if len(payload) < lim:
        raise RuntimeError(
            f"REST klines insufficient rows (STRICT): symbol={sym} interval={iv} need={lim} got={len(payload)}"
        )
    return payload


def _rest_fetch_depth_snapshot_strict(symbol: str, limit: int) -> Dict[str, Any]:
    sym = _normalize_symbol(symbol)
    lim = _require_positive_int(limit, "rest_depth.limit")
    if lim > _ORDERBOOK_SNAPSHOT_LIMIT:
        raise RuntimeError(
            f"REST depth limit exceeds cap (STRICT): symbol={sym} limit={lim} cap={_ORDERBOOK_SNAPSHOT_LIMIT}"
        )

    url = f"{_WS_REST_BASE}/fapi/v1/depth"
    try:
        resp = _rest_session.get(
            url,
            params={"symbol": sym, "limit": lim},
            timeout=_WS_REST_TIMEOUT_SEC,
        )
    except Exception as e:
        raise RuntimeError(f"REST depth request failed (STRICT): symbol={sym} limit={lim}") from e

    if resp.status_code != 200:
        raise RuntimeError(
            f"REST depth HTTP {resp.status_code} (STRICT): symbol={sym} limit={lim} body={resp.text[:500]!r}"
        )

    try:
        payload = resp.json()
    except Exception as e:
        raise RuntimeError(f"REST depth response is not valid JSON (STRICT): symbol={sym}") from e

    if not isinstance(payload, dict):
        raise RuntimeError(
            f"REST depth response root must be dict (STRICT): symbol={sym} got={type(payload).__name__}"
        )

    if "lastUpdateId" not in payload:
        raise RuntimeError("REST depth snapshot missing lastUpdateId (STRICT)")
    if "bids" not in payload or "asks" not in payload:
        raise RuntimeError("REST depth snapshot missing bids/asks (STRICT)")
    return payload


def _build_stream_names(symbol: str) -> List[str]:
    s = _to_stream_symbol(symbol)
    if not s:
        raise RuntimeError("symbol is required to build WS streams (STRICT)")

    streams: List[str] = []
    for iv in WS_INTERVALS:
        streams.append(f"{s}@kline_{iv}")

    # IMPORTANT:
    # current orderbook parser/storage contract is diff-depth(U/u/pu) based.
    # do not change this to depth5@100ms unless parser + state machine + storage contract are replaced together.
    streams.append(f"{s}@depth@100ms")
    return streams


def _build_ws_url(symbol: str) -> str:
    streams = _build_stream_names(symbol)
    return f"{_WS_COMBINED_BASE}{'/'.join(streams)}"


def _mark_ws_open(symbol: str, opened_at_ms: int) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise RuntimeError("symbol is required for ws open state")
    with _ws_state_lock:
        _ws_connection_open[sym] = True
        _ws_last_open_ts[sym] = int(opened_at_ms)
        _ws_last_message_ts.pop(sym, None)
        _ws_last_pong_ts.pop(sym, None)
        _ws_last_error_text.pop(sym, None)
        _ws_reconnect_request_ts.pop(sym, None)
        _ws_reconnect_request_reason.pop(sym, None)


def _mark_ws_closed(symbol: str, closed_at_ms: int, *, code: Any, msg: Any) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise RuntimeError("symbol is required for ws close state")
    with _ws_state_lock:
        _ws_connection_open[sym] = False
        _ws_last_close_ts[sym] = int(closed_at_ms)
        _ws_last_close_text[sym] = f"code={code!r} msg={_truncate_text(msg)}"


def _mark_ws_message(symbol: str, received_at_ms: int) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise RuntimeError("symbol is required for ws message state")
    with _ws_state_lock:
        _ws_last_message_ts[sym] = int(received_at_ms)


def _mark_ws_pong(symbol: str, received_at_ms: int) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise RuntimeError("symbol is required for ws pong state")
    with _ws_state_lock:
        _ws_last_pong_ts[sym] = int(received_at_ms)


def _mark_ws_error(symbol: str, error: Any) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise RuntimeError("symbol is required for ws error state")
    msg = _truncate_text(error)
    with _ws_state_lock:
        _ws_last_error_text[sym] = msg


def _clear_orderbook_runtime_state(symbol: str) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise RuntimeError("symbol is required for orderbook runtime clear")
    with _orderbook_lock:
        _orderbook_last_update_id.pop(sym, None)
        _orderbook_buffers.pop(sym, None)
        _orderbook_last_recv_ts.pop(sym, None)
        _orderbook_book_state.pop(sym, None)


def _register_ws_app(symbol: str, ws: websocket.WebSocketApp) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise RuntimeError("symbol is required for ws app register")
    with _ws_state_lock:
        _ws_current_app[sym] = ws


def _unregister_ws_app(symbol: str, ws: websocket.WebSocketApp) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise RuntimeError("symbol is required for ws app unregister")
    with _ws_state_lock:
        current = _ws_current_app.get(sym)
        if current is ws:
            _ws_current_app.pop(sym, None)


def _get_ws_circuit_breaker_snapshot(symbol: str) -> Dict[str, Any]:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise RuntimeError("symbol is required for circuit breaker snapshot")
    now_ms = _now_ms()
    with _ws_cb_lock:
        open_until_ts = _ws_cb_open_until_ts.get(sym)
        is_open = open_until_ts is not None and now_ms < int(open_until_ts)
        return {
            "open": bool(is_open),
            "open_until_ts": int(open_until_ts) if open_until_ts is not None else None,
            "last_reason": _ws_cb_last_reason.get(sym),
            "consecutive_failures": int(_ws_cb_consecutive_failures.get(sym, 0)),
            "trip_count": int(_ws_cb_trip_count.get(sym, 0)),
        }


def _reset_ws_circuit_breaker_on_open(symbol: str) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise RuntimeError("symbol is required for circuit breaker reset")
    with _ws_cb_lock:
        prev_count = int(_ws_cb_consecutive_failures.get(sym, 0))
        prev_open_until = _ws_cb_open_until_ts.get(sym)
        if prev_count != 0 or prev_open_until is not None:
            log(
                f"[MD_BINANCE_WS][CB] recovered symbol={sym} "
                f"consecutive_failures={prev_count} open_until_ts={prev_open_until}"
            )
        _ws_cb_consecutive_failures[sym] = 0
        _ws_cb_open_until_ts.pop(sym, None)
        _ws_cb_last_reason.pop(sym, None)


def _record_ws_circuit_breaker_failure(symbol: str, *, reason: str) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise RuntimeError("symbol is required for circuit breaker failure")
    rsn = str(reason or "").strip()
    if not rsn:
        raise RuntimeError("circuit breaker failure reason is required (STRICT)")

    now_ms = _now_ms()
    should_trip = False
    ws_app: Optional[websocket.WebSocketApp] = None

    with _ws_cb_lock:
        open_until_ts = _ws_cb_open_until_ts.get(sym)
        if open_until_ts is not None and now_ms < int(open_until_ts):
            _ws_cb_last_reason[sym] = rsn
            return

        current = int(_ws_cb_consecutive_failures.get(sym, 0)) + 1
        _ws_cb_consecutive_failures[sym] = current
        _ws_cb_last_reason[sym] = rsn

        if current >= _WS_CIRCUIT_BREAKER_CONFIRM_N:
            should_trip = True
            _ws_cb_open_until_ts[sym] = now_ms + int(_WS_CIRCUIT_BREAKER_COOLDOWN_SEC * 1000)
            _ws_cb_trip_count[sym] = int(_ws_cb_trip_count.get(sym, 0)) + 1
            _ws_cb_consecutive_failures[sym] = 0

    if not should_trip:
        log(
            f"[MD_BINANCE_WS][CB] failure recorded symbol={sym} "
            f"count<{_WS_CIRCUIT_BREAKER_CONFIRM_N} reason={rsn}"
        )
        return

    _mark_ws_error(sym, f"circuit_breaker_open:{rsn}")
    _clear_orderbook_runtime_state(sym)

    with _ws_state_lock:
        ws_app = _ws_current_app.get(sym)

    log(
        f"[MD_BINANCE_WS][CB_OPEN] symbol={sym} "
        f"cooldown_sec={_WS_CIRCUIT_BREAKER_COOLDOWN_SEC:.1f} reason={rsn}"
    )

    if ws_app is not None:
        _close_ws_with_log(ws_app, context=f"circuit_breaker_open symbol={sym} reason={rsn}")


def _request_ws_reconnect_strict(symbol: str, *, reason: str) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise RuntimeError("symbol is required for forced reconnect request")
    rsn = str(reason or "").strip()
    if not rsn:
        raise RuntimeError("forced reconnect reason is required (STRICT)")

    now_ms = _now_ms()
    ws_app: Optional[websocket.WebSocketApp]

    with _ws_state_lock:
        last_requested_ts = _ws_reconnect_request_ts.get(sym)
        if last_requested_ts is not None:
            if (now_ms - int(last_requested_ts)) < int(_WS_RECONNECT_GRACE_SEC * 1000):
                return

        _ws_reconnect_request_ts[sym] = now_ms
        _ws_reconnect_request_reason[sym] = rsn
        ws_app = _ws_current_app.get(sym)

    _mark_ws_error(sym, f"forced_reconnect_requested:{rsn}")
    _clear_orderbook_runtime_state(sym)
    log(f"[MD_BINANCE_WS] forced reconnect requested: symbol={sym} reason={rsn}")

    if ws_app is not None:
        _close_ws_with_log(ws_app, context=f"forced_reconnect symbol={sym} reason={rsn}")


def _maybe_force_reconnect_on_orderbook_feed_gap(symbol: str, *, ws_status: Dict[str, Any], reason: str) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise RuntimeError("symbol is required for orderbook feed gap reconnect")

    connection_open = bool(ws_status.get("connection_open", False))
    if not connection_open:
        return

    state = str(ws_status.get("state") or "").upper().strip()
    if state != "OPEN":
        return

    _record_ws_circuit_breaker_failure(sym, reason=f"orderbook_feed_gap:{reason}")
    _request_ws_reconnect_strict(sym, reason=reason)


def _close_ws_with_log(ws: websocket.WebSocketApp, *, context: str) -> None:
    try:
        ws.close()
    except Exception as e:
        log(f"[MD_BINANCE_WS] close error after {context}: {type(e).__name__}: {e}")


def _min_buffer_for_interval(iv: str) -> int:
    s = _normalize_interval(iv)
    if not s:
        return max(1, KLINE_MIN_BUFFER)

    strict_min = _EFFECTIVE_INTERVAL_MIN_BUFFER_BY_INTERVAL.get(s)

    if s.endswith("d") or s.endswith("w") or s.endswith("M"):
        base = max(1, _LONG_TF_MIN_BUFFER_DEFAULT)
        if strict_min is not None:
            return max(base, strict_min)
        return base

    base = max(1, KLINE_MIN_BUFFER)
    if strict_min is not None:
        return max(base, strict_min)
    return base


def get_ws_status(symbol: str) -> Dict[str, Any]:
    sym = _normalize_symbol(symbol)
    now_ms = _now_ms()

    with _ws_state_lock:
        connection_open = bool(_ws_connection_open.get(sym, False))
        last_open_ts = _ws_last_open_ts.get(sym)
        last_close_ts = _ws_last_close_ts.get(sym)
        last_close_text = _ws_last_close_text.get(sym)
        last_message_ts = _ws_last_message_ts.get(sym)
        last_pong_ts = _ws_last_pong_ts.get(sym)
        last_error = _ws_last_error_text.get(sym)
        reconnect_request_ts = _ws_reconnect_request_ts.get(sym)
        reconnect_request_reason = _ws_reconnect_request_reason.get(sym)

    cb = _get_ws_circuit_breaker_snapshot(sym)

    with _start_ws_lock:
        runner = _started_ws_symbols.get(sym)
        runner_alive = bool(runner is not None and runner.is_alive())

    if cb["open"]:
        state = "CLOSED"
    elif connection_open:
        state = "OPEN"
    elif runner_alive and last_open_ts is None:
        state = "OPENING"
    elif runner_alive:
        state = "RECONNECTING"
    else:
        state = "CLOSED"

    market_event_delay_ms = None
    if last_message_ts is not None:
        market_event_delay_ms = max(0, now_ms - int(last_message_ts))

    pong_delay_ms = None
    if last_pong_ts is not None:
        pong_delay_ms = max(0, now_ms - int(last_pong_ts))

    transport_reasons: List[str] = []
    market_feed_reasons: List[str] = []
    transport_observations: List[str] = []

    if cb["open"]:
        open_until_ts = cb["open_until_ts"]
        remaining_sec = None
        if open_until_ts is not None:
            remaining_sec = max(0.0, (int(open_until_ts) - now_ms) / 1000.0)
        transport_reasons.append(
            f"circuit_breaker_open remaining_sec={remaining_sec:.1f} reason={_truncate_text(cb['last_reason'])}"
            if remaining_sec is not None
            else f"circuit_breaker_open reason={_truncate_text(cb['last_reason'])}"
        )

    if not connection_open and not cb["open"]:
        if state == "OPENING":
            transport_observations.append("opening_in_progress")
        elif state == "RECONNECTING":
            if last_close_ts is None:
                transport_observations.append("reconnecting_without_last_close_ts")
            else:
                closed_age_sec = max(0, now_ms - int(last_close_ts)) / 1000.0
                if closed_age_sec <= _WS_RECONNECT_GRACE_SEC:
                    transport_observations.append(
                        f"reconnecting_grace_sec<={_WS_RECONNECT_GRACE_SEC} (got={closed_age_sec:.1f})"
                    )
                else:
                    transport_reasons.append("connection_not_open")
        else:
            transport_reasons.append("connection_not_open")

    if connection_open and not cb["open"]:
        if last_open_ts is None:
            transport_reasons.append("no_open_ts")
        elif last_pong_ts is None:
            open_age_sec = max(0, now_ms - int(last_open_ts)) / 1000.0
            if open_age_sec > PONG_STARTUP_GRACE_SEC:
                transport_observations.append(
                    f"pong_not_observed_after_open>{PONG_STARTUP_GRACE_SEC} (got={open_age_sec:.1f})"
                )
        else:
            pong_delay_sec = pong_delay_ms / 1000.0
            if pong_delay_sec > PONG_MAX_DELAY_SEC:
                transport_observations.append(
                    f"pong_delay_sec>{PONG_MAX_DELAY_SEC} (got={pong_delay_sec:.1f})"
                )

    if reconnect_request_ts is not None:
        reconnect_age_sec = max(0, now_ms - int(reconnect_request_ts)) / 1000.0
        transport_observations.append(
            f"forced_reconnect_requested age_sec={reconnect_age_sec:.1f} reason={_truncate_text(reconnect_request_reason)}"
        )

    if last_message_ts is None:
        if state in ("OPENING", "RECONNECTING"):
            market_feed_reasons.append("startup_no_market_event_yet")
        elif not cb["open"]:
            market_feed_reasons.append("no_market_event")
    else:
        market_event_delay_sec = market_event_delay_ms / 1000.0
        if market_event_delay_sec > MARKET_EVENT_MAX_DELAY_SEC:
            market_feed_reasons.append(
                f"market_event_delay_sec>{MARKET_EVENT_MAX_DELAY_SEC} (got={market_event_delay_sec:.1f})"
            )

    transport_reason_items = [f"transport:{r}" for r in transport_reasons]
    warning_items = [f"market_feed:{r}" for r in market_feed_reasons]
    observation_items = [f"transport_observation:{r}" for r in transport_observations]

    return {
        "symbol": sym,
        "state": state,
        "connection_open": connection_open,
        "last_open_ts": last_open_ts,
        "last_close_ts": last_close_ts,
        "last_close_text": last_close_text,
        "last_message_ts": last_message_ts,
        "market_event_delay_ms": market_event_delay_ms,
        "last_pong_ts": last_pong_ts,
        "pong_delay_ms": pong_delay_ms,
        "last_error": last_error,
        "reconnect_requested": reconnect_request_ts is not None,
        "reconnect_request_ts": reconnect_request_ts,
        "reconnect_request_reason": reconnect_request_reason,
        "circuit_breaker_open": bool(cb["open"]),
        "circuit_breaker_open_until_ts": cb["open_until_ts"],
        "circuit_breaker_last_reason": cb["last_reason"],
        "circuit_breaker_consecutive_failures": int(cb["consecutive_failures"]),
        "circuit_breaker_trip_count": int(cb["trip_count"]),
        "transport_ok": len(transport_reasons) == 0,
        "market_feed_ok": len(market_feed_reasons) == 0,
        "ok": len(transport_reasons) == 0,
        "reasons": transport_reason_items,
        "warnings": warning_items,
        "transport_reasons": transport_reason_items,
        "market_feed_reasons": warning_items,
        "transport_observations": observation_items,
    }


def _derive_dashboard_connection_status(symbol: str) -> str:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise RuntimeError("symbol is required for dashboard connection status")

    ws_status = get_ws_status(sym)
    state = str(ws_status.get("state") or "").upper().strip()

    if state == "OPEN":
        return _DASHBOARD_WS_STATUS_CONNECTED
    if state in ("OPENING", "RECONNECTING"):
        return _DASHBOARD_WS_STATUS_RECONNECTING
    return _DASHBOARD_WS_STATUS_DISCONNECTED


__all__ = [
    "SET",
    "WSProtocolError",
    "KlineRow",
    "LegacyKlineRowWithVolume",
    "LegacyKlineRow",
    "HEALTH_REQUIRED_INTERVALS",
    "REQUIRED_INTERVALS",
    "WS_INTERVALS",
    "WS_BACKFILL_INTERVALS",
    "FEATURE_REQUIRED_INTERVALS",
    "KLINE_MIN_BUFFER",
    "KLINE_MAX_DELAY_SEC",
    "MARKET_EVENT_MAX_DELAY_SEC",
    "PONG_MAX_DELAY_SEC",
    "PONG_STARTUP_GRACE_SEC",
    "MAX_KLINES",
    "_ORDERBOOK_SNAPSHOT_LIMIT",
    "_ORDERBOOK_TOP_N",
    "_ORDERBOOK_PENDING_EVENT_MAX",
    "_WS_CIRCUIT_BREAKER_COOLDOWN_SEC",
    "_WS_RECONNECT_GRACE_SEC",
    "_health_fail_lock",
    "_last_health_fail_key",
    "_last_health_fail_log_ts",
    "_kline_buffers",
    "_kline_last_recv_ts",
    "_kline_last_quote_volume",
    "_orderbook_buffers",
    "_orderbook_last_update_id",
    "_orderbook_last_recv_ts",
    "_orderbook_book_state",
    "_ws_connection_open",
    "_ws_last_open_ts",
    "_ws_last_close_ts",
    "_ws_last_close_text",
    "_ws_last_message_ts",
    "_ws_last_pong_ts",
    "_ws_last_error_text",
    "_ws_current_app",
    "_ws_reconnect_request_ts",
    "_ws_reconnect_request_reason",
    "_ws_state_lock",
    "_ws_cb_consecutive_failures",
    "_ws_cb_open_until_ts",
    "_ws_cb_last_reason",
    "_ws_cb_trip_count",
    "_ws_cb_lock",
    "_kline_lock",
    "_orderbook_lock",
    "_no_buf_log_last",
    "_no_buf_log_lock",
    "_started_ws_symbols",
    "_start_ws_lock",
    "_now_ms",
    "_safe_dump_for_log",
    "_normalize_symbol",
    "_normalize_interval",
    "_to_stream_symbol",
    "_dedupe_keep_order",
    "_normalize_interval_list_from_settings",
    "_require_bool",
    "_derive_rest_kline_closed_strict",
    "_require_int_ms",
    "_require_float",
    "_require_positive_int",
    "_require_nonnegative_int",
    "_require_positive_float",
    "_require_interval_min_buffer_mapping",
    "_truncate_text",
    "_extract_orderbook_event_ts_ms_strict",
    "_ms_to_sec_optional",
    "_normalize_required_intervals_for_dashboard",
    "_interval_to_ms_strict",
    "_extract_quote_volume_from_rest_row_optional",
    "_rest_fetch_klines_strict",
    "_rest_fetch_depth_snapshot_strict",
    "_build_stream_names",
    "_build_ws_url",
    "_mark_ws_open",
    "_mark_ws_closed",
    "_mark_ws_message",
    "_mark_ws_pong",
    "_mark_ws_error",
    "_clear_orderbook_runtime_state",
    "_register_ws_app",
    "_unregister_ws_app",
    "_get_ws_circuit_breaker_snapshot",
    "_reset_ws_circuit_breaker_on_open",
    "_record_ws_circuit_breaker_failure",
    "_request_ws_reconnect_strict",
    "_maybe_force_reconnect_on_orderbook_feed_gap",
    "_close_ws_with_log",
    "_min_buffer_for_interval",
    "get_ws_status",
    "_derive_dashboard_connection_status",
    "log",
    "websocket",
]