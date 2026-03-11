# infra/market_data_ws.py
"""
========================================================
FILE: infra/market_data_ws.py
ROLE:
- Binance USDT-M Futures multiplex WebSocket 수신기
- 멀티 타임프레임 kline + depth5 orderbook을 메모리 버퍼에 유지
- WS primary market-data를 bt_candles / bt_orderbook_snapshots 에 지속 기록한다
- 상위 레이어에 getter / atomic snapshot / health snapshot / dashboard telemetry snapshot 을 제공한다

CORE RESPONSIBILITIES:
- WS 수신 데이터의 STRICT 검증 및 메모리 버퍼링
- WS kline / depth5 orderbook의 DB 영속화(STRICT, fail-fast)
- 부팅 시 REST bootstrap으로 필수 캔들 preload
- WS 세션 상태 / market-data freshness / orderbook payload 무결성 관측성 제공
- dashboard 가 직접 사용할 수 있는 WS telemetry snapshot 제공
- strategy / execution / exit 책임과 완전 분리

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- 데이터 누락/손상/계약 불일치는 즉시 예외
- 더미 값 생성, silent continue, 예외 삼키기 금지
- health 판단은 transport / payload / feed activity 를 분리한다
- orderbook payload 무결성과 feed inactivity warning을 혼동하지 않는다
- 환경변수 직접 접근 금지, settings.py(SSOT)만 사용
- orderbook spreadPct/spread_pct 계약은 ratio(0..1) 기준으로 통일한다
- WS primary feed 의 DB 저장 실패는 치명적 오류로 간주하며 세션을 종료/재연결한다
- orderbook DB timestamp 는 반드시 거래소 이벤트 시각(E/T)을 사용한다
- 계좌/포지션 데이터는 이 파일 책임이 아니다. dashboard telemetry 는 시장데이터 전용으로만 제공한다

CHANGE HISTORY:
- 2026-03-11:
  1) FIX(ROOT-CAUSE): Binance multiplex WS 에서 pong callback 부재를 transport failure 로 오판하던 문제 제거
  2) FIX(TRADE-GRADE): WS transport health 는 connection_open 기준으로만 판단하고 market-data activity 는 별도 warning 으로 유지
  3) FIX(OBSERVABILITY): pong 시각은 참고용 메타데이터로만 보존하고 SAFE_STOP 근거에서 제외
  4) FIX(ROOT-CAUSE): orderbook DB/storage ts 를 local receive time 이 아닌 exchange event time(E/T)으로 교정
  5) FIX(TRADE-GRADE): orderbook 는 event ts / recv ts 를 분리해 DB 정합성과 health 관측성을 동시에 유지
  6) FIX(ROOT-CAUSE): WS kline / orderbook DB persistence path 추가
  7) FIX(ROOT-CAUSE): kline close flag(k.x) 를 DB is_closed 계약까지 end-to-end 전달
  8) FIX(TRADE-GRADE): DB 저장 실패를 fatal message handling error 로 승격, WS 세션 종료/재연결
  9) FIX(ROOT-CAUSE): kline buffer contract now preserves is_closed state end-to-end
  10) FIX(ROOT-CAUSE): WS open candle updates are stored instead of being discarded until close
  11) FIX(STRICT): REST bootstrap derives current-candle closed state from explicit close_time only
  12) ADD(API): get_klines_with_volume_and_closed() for downstream persistence path
  13) FIX(CONTRACT): legacy public getters keep 5/6-tuple compatibility while internal buffer uses 7-tuple
  14) ADD(API): get_dashboard_ws_telemetry_snapshot() 추가
  15) ADD(CONTRACT): dashboard 용 connection_status / data_freshness_sec / kline_latency_sec_by_tf / orderbook_latency_sec / last_ws_message_latency_sec 제공
  16) FIX(SSOT): REST bootstrap 대상 interval 을 ws_backfill_tfs 기준으로 정렬
  17) FIX(CONTRACT): health snapshot required_intervals 를 ws_required_tfs ∪ features_required_tfs 로 통일
  18) FIX(CONTRACT): health snapshot top-level transport_ok / market_feed_ok 명시적 노출 추가
  19) FIX(STRICT): internal MAX_KLINES capacity 를 strict required min buffer 와 정합화
- 2026-03-10:
  1) FIX(ROOT-CAUSE): ws_status.ok 를 transport 상태만 의미하도록 정리
  2) FIX(ROOT-CAUSE): orderbook health 에서 feed inactivity warning 과 payload fail 분리
  3) ADD(OBSERVABILITY): orderbook 전용 last recv ts / recv_delay 상태 추적 추가
  4) ADD(API): get_orderbook_buffer_status() 공개
  5) FIX(ARCH): get_health_snapshot() 가 warning 때문에 overall FAIL 되지 않도록 정리
  6) FIX(CONTRACT): orderbook spreadPct 를 percent가 아닌 ratio(0..1)로 통일
========================================================
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any, Dict, List, Mapping, Optional, Tuple

import requests
import websocket  # pip install websocket-client

from infra.market_data_store import save_candle_from_ws, save_orderbook_from_ws
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


# ─────────────────────────────────────────────
# Settings / SSOT
# ─────────────────────────────────────────────
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

_orderbook_buffers: Dict[str, Dict[str, Any]] = {}
_orderbook_last_update_id: Dict[str, int] = {}
_orderbook_last_recv_ts: Dict[str, int] = {}

_ws_connection_open: Dict[str, bool] = {}
_ws_last_open_ts: Dict[str, int] = {}
_ws_last_close_ts: Dict[str, int] = {}
_ws_last_close_text: Dict[str, str] = {}
_ws_last_message_ts: Dict[str, int] = {}
_ws_last_pong_ts: Dict[str, int] = {}
_ws_last_error_text: Dict[str, str] = {}
_ws_state_lock = threading.Lock()

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


def get_ws_status(symbol: str) -> Dict[str, Any]:
    """
    WS session status.

    ok:
    - transport 상태만 의미한다.
    - Binance multiplex WS 에서는 pong callback 부재를 transport failure 로 간주하지 않는다.
    - market_feed inactivity는 warning 으로 분리한다.
    """
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

    market_event_delay_ms = None
    if last_message_ts is not None:
        market_event_delay_ms = max(0, now_ms - int(last_message_ts))

    pong_delay_ms = None
    if last_pong_ts is not None:
        pong_delay_ms = max(0, now_ms - int(last_pong_ts))

    transport_reasons: List[str] = []
    market_feed_reasons: List[str] = []
    transport_observations: List[str] = []

    if not connection_open:
        transport_reasons.append("connection_not_open")

    if connection_open:
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

    if last_message_ts is None:
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
        "connection_open": connection_open,
        "last_open_ts": last_open_ts,
        "last_close_ts": last_close_ts,
        "last_close_text": last_close_text,
        "last_message_ts": last_message_ts,
        "market_event_delay_ms": market_event_delay_ms,
        "last_pong_ts": last_pong_ts,
        "pong_delay_ms": pong_delay_ms,
        "last_error": last_error,
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
    transport_ok = bool(ws_status.get("transport_ok", False))
    if transport_ok:
        return _DASHBOARD_WS_STATUS_CONNECTED

    with _start_ws_lock:
        runner = _started_ws_symbols.get(sym)

    if runner is not None and runner.is_alive():
        return _DASHBOARD_WS_STATUS_RECONNECTING

    return _DASHBOARD_WS_STATUS_DISCONNECTED


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


def _build_stream_names(symbol: str) -> List[str]:
    s = _to_stream_symbol(symbol)
    if not s:
        raise RuntimeError("symbol is required to build WS streams")
    streams: List[str] = [f"{s}@kline_{iv}" for iv in WS_INTERVALS]
    streams.append(f"{s}@depth5@100ms")
    return streams


def _build_ws_url(symbol: str) -> str:
    streams = _build_stream_names(symbol)
    return f"{_WS_COMBINED_BASE}{'/'.join(streams)}"


def _decode_msg(raw: Any) -> Any:
    if isinstance(raw, (bytes, bytearray)):
        txt = bytes(raw).decode("utf-8")
        return json.loads(txt)
    if isinstance(raw, str):
        return json.loads(raw)
    raise RuntimeError(f"unsupported ws message type: {type(raw)}")


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


def bootstrap_klines_from_rest_strict(symbol: str, intervals: Optional[List[str]] = None) -> Dict[str, int]:
    """
    TRADE-GRADE startup bootstrap (explicit preload, not runtime fallback)

    - 운영 시작 전에 필요한 캔들을 REST로 먼저 채운다.
    - interval별 required buffer 길이만큼 정확히 받아오지 못하면 즉시 실패한다.
    - bootstrap 성공 후에는 WS만으로 갱신한다.
    """
    if not _WS_BOOTSTRAP_REST_ENABLED:
        raise RuntimeError("ws_bootstrap_rest_enabled is False. TRADE-GRADE startup requires explicit bootstrap.")

    sym = _normalize_symbol(symbol)
    if not sym:
        raise RuntimeError("symbol is required for REST bootstrap")

    targets = list(intervals or WS_BACKFILL_INTERVALS)
    targets = [_normalize_interval(x) for x in targets if _normalize_interval(x)]
    if not targets:
        raise RuntimeError("REST bootstrap intervals empty (STRICT)")

    loaded: Dict[str, int] = {}
    for iv in targets:
        need = _min_buffer_for_interval(iv)
        payload = _rest_fetch_klines_strict(sym, iv, need)
        backfill_klines_from_rest(sym, iv, payload)
        loaded[iv] = len(payload)

    return loaded


def _require_kline_progression_strict(prev: KlineRow, new_row: KlineRow, *, context: str) -> None:
    prev_ts, prev_o, prev_h, prev_l, _prev_c, prev_v, prev_closed = prev
    new_ts, new_o, new_h, new_l, _new_c, new_v, new_closed = new_row

    if new_ts != prev_ts:
        raise WSProtocolError(f"{context} ts mismatch during same-candle update (STRICT)")
    if prev_closed and not new_closed:
        raise WSProtocolError(f"{context} closed candle cannot reopen (STRICT)")
    if abs(new_o - prev_o) > 1e-12:
        raise WSProtocolError(f"{context} open price changed within same candle (STRICT)")
    if new_h + 1e-12 < prev_h:
        raise WSProtocolError(f"{context} high decreased within same candle (STRICT)")
    if new_l - 1e-12 > prev_l:
        raise WSProtocolError(f"{context} low increased within same candle (STRICT)")
    if new_v + 1e-12 < prev_v:
        raise WSProtocolError(f"{context} volume decreased within same candle (STRICT)")


def _validate_next_kline_row_against_buffer_strict(buf: List[KlineRow], row: KlineRow, *, context: str) -> None:
    ts = int(row[0])
    if not buf:
        return

    last = buf[-1]
    last_ts = int(last[0])

    if ts < last_ts:
        raise WSProtocolError(f"{context} kline timestamp rollback (STRICT): new_ts={ts} last_ts={last_ts}")

    if ts == last_ts:
        _require_kline_progression_strict(last, row, context=context)
        return

    if bool(last[6]) is False:
        raise WSProtocolError(
            f"{context} new candle arrived before previous candle closed (STRICT): prev_ts={last_ts} new_ts={ts}"
        )


def _append_or_update_kline_row_strict(buf: List[KlineRow], row: KlineRow, *, context: str) -> None:
    ts = int(row[0])
    if not buf:
        buf.append(row)
        return

    last = buf[-1]
    last_ts = int(last[0])

    if ts < last_ts:
        raise WSProtocolError(f"{context} kline timestamp rollback (STRICT): new_ts={ts} last_ts={last_ts}")

    if ts == last_ts:
        _require_kline_progression_strict(last, row, context=context)
        buf[-1] = row
        return

    if bool(last[6]) is False:
        raise WSProtocolError(
            f"{context} new candle arrived before previous candle closed (STRICT): prev_ts={last_ts} new_ts={ts}"
        )

    buf.append(row)
    if len(buf) > MAX_KLINES:
        del buf[0 : len(buf) - MAX_KLINES]


def _persist_ws_kline_strict(
    *,
    symbol: str,
    interval: str,
    ts_ms: int,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: float,
    quote_volume: float,
    is_closed: bool,
) -> None:
    save_candle_from_ws(
        symbol=symbol,
        interval=interval,
        ts_ms=ts_ms,
        open_=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
        quote_volume=quote_volume,
        source="ws",
        is_closed=is_closed,
    )


def _push_kline(symbol: str, interval: str, kline_obj: Dict[str, Any]) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise WSProtocolError("empty symbol in kline push")

    iv = _normalize_interval(interval)
    if not iv:
        raise WSProtocolError("empty interval in kline push")

    key = (sym, iv)

    ts = _require_int_ms(kline_obj.get("t"), "kline.t")
    o = _require_float(kline_obj.get("o"), "kline.o")
    h = _require_float(kline_obj.get("h"), "kline.h")
    l = _require_float(kline_obj.get("l"), "kline.l")
    c = _require_float(kline_obj.get("c"), "kline.c")
    v = _require_float(kline_obj.get("v"), "kline.v", allow_zero=True)
    q = _require_float(kline_obj.get("q"), "kline.q", allow_zero=True)
    is_closed = _require_bool(kline_obj.get("x"), "kline.x")

    row: KlineRow = (ts, o, h, l, c, v, is_closed)

    now_ms = _now_ms()
    _mark_ws_message(sym, now_ms)

    with _kline_lock:
        buf = _kline_buffers.setdefault(key, [])
        _validate_next_kline_row_against_buffer_strict(buf, row, context=f"ws_kline[{sym}:{iv}]")

    _persist_ws_kline_strict(
        symbol=sym,
        interval=iv,
        ts_ms=ts,
        open_=o,
        high=h,
        low=l,
        close=c,
        volume=v,
        quote_volume=q,
        is_closed=is_closed,
    )

    with _kline_lock:
        buf = _kline_buffers.setdefault(key, [])
        _append_or_update_kline_row_strict(buf, row, context=f"ws_kline[{sym}:{iv}]")
        _kline_last_recv_ts[key] = now_ms


def _normalize_depth_side_strict(side_val: Any, *, name: str) -> List[List[float]]:
    if side_val is None:
        raise WSProtocolError(f"{name} missing")
    if not isinstance(side_val, (list, tuple)):
        raise WSProtocolError(f"{name} must be list/tuple")

    out: List[List[float]] = []
    for i, row in enumerate(side_val):
        if isinstance(row, (list, tuple)) and len(row) >= 2:
            p = _require_float(row[0], f"{name}[{i}].price")
            q = _require_float(row[1], f"{name}[{i}].qty")
            out.append([p, q])
            continue
        if isinstance(row, dict):
            p = _require_float(row.get("price"), f"{name}[{i}].price")
            q = _require_float(row.get("qty"), f"{name}[{i}].qty")
            out.append([p, q])
            continue
        raise WSProtocolError(f"{name}[{i}] invalid level type: {type(row).__name__}")

    if not out:
        raise WSProtocolError(f"{name} empty after parse (STRICT)")
    return out


def _compute_best_prices_strict(bids: List[List[float]], asks: List[List[float]]) -> Tuple[float, float]:
    if not bids or not asks:
        raise WSProtocolError("bids/asks empty (STRICT)")
    best_bid = max(float(r[0]) for r in bids)
    best_ask = min(float(r[0]) for r in asks)
    if best_bid <= 0 or best_ask <= 0:
        raise WSProtocolError("best prices invalid (STRICT)")
    if best_ask <= best_bid:
        raise WSProtocolError("crossed book (bestAsk<=bestBid) (STRICT)")
    return float(best_bid), float(best_ask)


def _compute_spread_pct(best_bid: float, best_ask: float) -> float:
    """
    STRICT CONTRACT:
    - orderbook spreadPct/spread_pct 는 엔진 전체에서 ratio(0..1) 기준이다.
    - 예: 0.0005 == 0.05%
    """
    if best_bid <= 0 or best_ask <= 0:
        raise WSProtocolError("best prices invalid for spread (STRICT)")
    if best_ask <= best_bid:
        raise WSProtocolError("crossed book for spread (STRICT)")
    mid = (best_bid + best_ask) / 2.0
    if mid <= 0:
        raise WSProtocolError("mid invalid (STRICT)")
    return (best_ask - best_bid) / mid


def _persist_ws_orderbook_snapshot_strict(
    *,
    symbol: str,
    event_ts_ms: int,
    bids: List[List[float]],
    asks: List[List[float]],
) -> None:
    save_orderbook_from_ws(
        symbol=symbol,
        ts_ms=event_ts_ms,
        bids=bids,
        asks=asks,
    )


def _push_orderbook(symbol: str, payload: Dict[str, Any]) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise WSProtocolError("empty symbol in orderbook push")

    raw_update_id = payload.get("u") if payload.get("u") is not None else payload.get("lastUpdateId")
    if raw_update_id is None:
        raise WSProtocolError("orderbook missing update id (u/lastUpdateId) (STRICT)")
    try:
        update_id = int(raw_update_id)
    except Exception as e:
        raise WSProtocolError(f"orderbook update id must be int (STRICT): {e}") from e
    if update_id <= 0:
        raise WSProtocolError("orderbook update id must be > 0 (STRICT)")

    with _orderbook_lock:
        prev_update_id = _orderbook_last_update_id.get(sym)

    if prev_update_id is not None and update_id <= int(prev_update_id):
        raise WSProtocolError(
            f"orderbook outdated packet (STRICT): prev={int(prev_update_id)} new={int(update_id)}"
        )

    raw_bids = payload.get("b") if payload.get("b") is not None else payload.get("bids")
    raw_asks = payload.get("a") if payload.get("a") is not None else payload.get("asks")

    normalized_bids = _normalize_depth_side_strict(raw_bids, name="orderbook.bids")
    normalized_asks = _normalize_depth_side_strict(raw_asks, name="orderbook.asks")

    event_ts_ms = _extract_orderbook_event_ts_ms_strict(payload)
    now_ms = _now_ms()
    _mark_ws_message(sym, now_ms)

    best_bid, best_ask = _compute_best_prices_strict(normalized_bids, normalized_asks)
    spread_pct = _compute_spread_pct(best_bid, best_ask)

    ob: Dict[str, Any] = {
        "bids": normalized_bids,
        "asks": normalized_asks,
        "ts": event_ts_ms,
        "bestBid": best_bid,
        "bestAsk": best_ask,
        "spreadPct": spread_pct,
        "lastUpdateId": int(update_id),
        "exchTs": event_ts_ms,
    }

    _persist_ws_orderbook_snapshot_strict(
        symbol=sym,
        event_ts_ms=event_ts_ms,
        bids=normalized_bids,
        asks=normalized_asks,
    )

    with _orderbook_lock:
        prev2 = _orderbook_last_update_id.get(sym)
        if prev2 is not None and update_id <= int(prev2):
            raise WSProtocolError(
                f"orderbook outdated packet (STRICT, race): prev={int(prev2)} new={int(update_id)}"
            )

        _orderbook_buffers[sym] = ob
        _orderbook_last_update_id[sym] = int(update_id)
        _orderbook_last_recv_ts[sym] = now_ms


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

    if "@kline_" in stream:
        k = payload.get("k")
        if not isinstance(k, dict):
            raise WSProtocolError(f"kline stream missing 'k': stream={stream} payload={_safe_dump_for_log(payload)}")

        interval = str(k.get("i") or "").strip()
        if not interval:
            interval = stream.split("@kline_")[-1]
        interval = _normalize_interval(interval)
        if not interval:
            raise WSProtocolError(f"kline interval parse failed: stream={stream} payload={_safe_dump_for_log(payload)}")

        payload_symbol = _normalize_symbol(str(payload.get("s") or sym))
        if payload_symbol and payload_symbol != sym:
            raise WSProtocolError(f"unexpected symbol in stream (expected={sym}, got={payload_symbol})")

        _push_kline(sym, interval, k)
        return

    if "@depth" in stream:
        payload_symbol = _normalize_symbol(str(payload.get("s") or sym))
        if payload_symbol and payload_symbol != sym:
            raise WSProtocolError(f"unexpected symbol in depth stream (expected={sym}, got={payload_symbol})")
        _push_orderbook(sym, payload)
        return

    raise WSProtocolError(f"unknown stream type (STRICT): {stream}")


def _on_message(symbol: str, ws: websocket.WebSocketApp, message: Any) -> None:
    try:
        data = _decode_msg(message)
    except Exception as e:
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
        _mark_ws_error(symbol, f"protocol_error:{e}")
        log(f"[MD_BINANCE_WS] protocol error: {e}")
        _close_ws_with_log(ws, context=f"protocol_error symbol={_normalize_symbol(symbol)}")
    except Exception as e:
        _mark_ws_error(symbol, f"fatal_message_handling_error:{type(e).__name__}:{e}")
        log(f"[MD_BINANCE_WS] fatal message handling error: {type(e).__name__}: {e}")
        _close_ws_with_log(ws, context=f"fatal_message_handling_error symbol={_normalize_symbol(symbol)}")


def _on_error(symbol: str, ws: websocket.WebSocketApp, error: Any) -> None:
    _ = ws
    _mark_ws_error(symbol, error)
    log(f"[MD_BINANCE_WS] error: {error}")


def _on_close(symbol: str, ws: websocket.WebSocketApp, code: Any, msg: Any) -> None:
    _ = ws
    _mark_ws_closed(symbol, _now_ms(), code=code, msg=msg)
    log(f"[MD_BINANCE_WS] closed: {code} {msg}")


def _on_open(symbol: str, ws: websocket.WebSocketApp) -> None:
    _ = ws
    streams = _build_stream_names(symbol)
    sym = _normalize_symbol(symbol)

    with _orderbook_lock:
        _orderbook_last_update_id.pop(sym, None)
        _orderbook_buffers.pop(sym, None)
        _orderbook_last_recv_ts.pop(sym, None)

    _mark_ws_open(sym, _now_ms())
    log(f"[MD_BINANCE_WS] opened: symbol={sym} streams={streams}")


def _on_pong(symbol: str, ws: websocket.WebSocketApp, data: Any) -> None:
    _ = ws
    _ = data
    _mark_ws_pong(symbol, _now_ms())


def preload_klines(symbol: str, interval: str, rows: List[KlineRow]) -> None:
    sym = _normalize_symbol(symbol)
    iv = _normalize_interval(interval)
    if not sym:
        raise ValueError("symbol is required")
    if not iv:
        raise ValueError("interval is required")

    cleaned: List[KlineRow] = []
    last_ts: Optional[int] = None
    for i, r in enumerate(rows):
        if not isinstance(r, (list, tuple)) or len(r) != 7:
            raise RuntimeError(f"preload row[{i}] must be 7-tuple(ts,o,h,l,c,v,is_closed) (STRICT)")
        ts = _require_int_ms(r[0], f"preload[{i}].ts")
        o = _require_float(r[1], f"preload[{i}].o")
        h = _require_float(r[2], f"preload[{i}].h")
        l = _require_float(r[3], f"preload[{i}].l")
        c = _require_float(r[4], f"preload[{i}].c")
        v = _require_float(r[5], f"preload[{i}].v", allow_zero=True)
        is_closed = _require_bool(r[6], f"preload[{i}].is_closed")
        if last_ts is not None and ts <= last_ts:
            raise RuntimeError(
                f"preload rows must be strictly increasing by ts (STRICT): prev_ts={last_ts} now_ts={ts} idx={i}"
            )
        cleaned.append((ts, o, h, l, c, v, is_closed))
        last_ts = ts

    key = (sym, iv)
    with _kline_lock:
        trimmed = list(cleaned[-MAX_KLINES:])
        _kline_buffers[key] = trimmed
        if trimmed:
            _kline_last_recv_ts[key] = _now_ms()


def backfill_klines_from_rest(symbol: str, interval: str, rest_klines: List[Any]) -> None:
    """
    STRICT:
    - REST 입력이 불량이면 조용히 건너뛰지 않는다(부팅 무결성).
    - REST close_time 또는 explicit x 로만 closed 상태를 결정한다.
    """
    sym = _normalize_symbol(symbol)
    iv = _normalize_interval(interval)
    if not sym:
        raise ValueError("symbol is required")
    if not iv:
        raise ValueError("interval is required")

    if not isinstance(rest_klines, list):
        raise RuntimeError("rest_klines must be list (STRICT)")
    if not rest_klines:
        raise RuntimeError("rest_klines empty (STRICT)")

    now_ms = _now_ms()
    converted: List[KlineRow] = []
    for idx, row in enumerate(rest_klines):
        if isinstance(row, (list, tuple)):
            if len(row) < 7:
                raise RuntimeError(f"rest_klines[{idx}] invalid len<7 (STRICT)")
            ts = _require_int_ms(row[0], f"rest_klines[{idx}].ts")
            o = _require_float(row[1], f"rest_klines[{idx}].o")
            h = _require_float(row[2], f"rest_klines[{idx}].h")
            l = _require_float(row[3], f"rest_klines[{idx}].l")
            c = _require_float(row[4], f"rest_klines[{idx}].c")
            v = _require_float(row[5], f"rest_klines[{idx}].v", allow_zero=True)
            is_closed = _derive_rest_kline_closed_strict(row, idx, now_ms)
            converted.append((ts, o, h, l, c, v, is_closed))
            continue

        if isinstance(row, dict):
            ts = _require_int_ms(row.get("t") or row.get("openTime") or row.get("T"), f"rest_klines[{idx}].ts")
            o = _require_float(row.get("o") or row.get("open"), f"rest_klines[{idx}].o")
            h = _require_float(row.get("h") or row.get("high"), f"rest_klines[{idx}].h")
            l = _require_float(row.get("l") or row.get("low"), f"rest_klines[{idx}].l")
            c = _require_float(row.get("c") or row.get("close"), f"rest_klines[{idx}].c")
            v = _require_float(row.get("v") or row.get("volume"), f"rest_klines[{idx}].v", allow_zero=True)
            is_closed = _derive_rest_kline_closed_strict(row, idx, now_ms)
            converted.append((ts, o, h, l, c, v, is_closed))
            continue

        raise RuntimeError(f"rest_klines[{idx}] invalid type (STRICT): {type(row).__name__}")

    converted.sort(key=lambda x: x[0])
    preload_klines(sym, iv, converted)


def _log_no_buffer_once(symbol: str, interval: str, requested: int) -> None:
    if not SET.ws_log_enabled:
        return

    now = time.time()
    key = (_normalize_symbol(symbol), _normalize_interval(interval))
    with _no_buf_log_lock:
        last = _no_buf_log_last.get(key, 0.0)
        if (now - last) < _NO_BUF_LOG_SUPPRESS_SEC:
            return
        _no_buf_log_last[key] = now

    log(f"[MD_BINANCE_WS KLINES] no kline buffer for {symbol} {interval} (requested={requested})")


def get_klines_with_volume_and_closed(symbol: str, interval: str, limit: int = 300) -> List[KlineRow]:
    if limit <= 0:
        raise ValueError("limit must be > 0")

    sym = _normalize_symbol(symbol)
    iv = _normalize_interval(interval)
    key = (sym, iv)

    with _kline_lock:
        buf = _kline_buffers.get(key, [])
        if not buf:
            _log_no_buffer_once(sym, iv, limit)
            return []
        return list(buf[-limit:])


def get_klines_with_volume(symbol: str, interval: str, limit: int = 300) -> List[LegacyKlineRowWithVolume]:
    rows = get_klines_with_volume_and_closed(symbol, interval, limit=limit)
    return [(ts, o, h, l, c, v) for (ts, o, h, l, c, v, _is_closed) in rows]


def get_klines(symbol: str, interval: str, limit: Optional[int] = None) -> List[LegacyKlineRow]:
    if limit is None:
        normalized_limit = max(300, _min_buffer_for_interval(interval))
    else:
        if limit <= 0:
            raise ValueError("limit must be > 0")
        normalized_limit = limit
    rows = get_klines_with_volume(symbol, interval, limit=normalized_limit)
    return [(ts, o, h, l, c) for (ts, o, h, l, c, v) in rows]


def get_last_kline_ts(symbol: str, interval: str) -> Optional[int]:
    sym = _normalize_symbol(symbol)
    iv = _normalize_interval(interval)
    key = (sym, iv)
    with _kline_lock:
        buf = _kline_buffers.get(key, [])
        if not buf:
            return None
        return int(buf[-1][0])


def get_last_kline_delay_ms(symbol: str, interval: str) -> Optional[int]:
    sym = _normalize_symbol(symbol)
    iv = _normalize_interval(interval)
    key = (sym, iv)
    now = _now_ms()
    with _kline_lock:
        recv_ts = _kline_last_recv_ts.get(key)
    if recv_ts is None:
        return None
    return max(0, now - int(recv_ts))


def get_kline_buffer_status(symbol: str, interval: str) -> Dict[str, Any]:
    sym = _normalize_symbol(symbol)
    iv = _normalize_interval(interval)
    key = (sym, iv)

    with _kline_lock:
        buf = _kline_buffers.get(key, [])
        last_ts = buf[-1][0] if buf else None
        last_recv = _kline_last_recv_ts.get(key)

    delay_ms = None
    if last_recv is not None:
        delay_ms = max(0, _now_ms() - int(last_recv))

    return {
        "symbol": sym,
        "interval": iv,
        "buffer_len": len(buf),
        "last_ts": last_ts,
        "last_recv_ts": last_recv,
        "delay_ms": delay_ms,
    }


def get_orderbook(symbol: str, limit: int = 5) -> Optional[Dict[str, Any]]:
    if limit <= 0:
        raise ValueError("limit must be > 0")

    sym = _normalize_symbol(symbol)
    with _orderbook_lock:
        ob = _orderbook_buffers.get(sym)
        if not ob:
            return None

        bids = ob.get("bids") or []
        asks = ob.get("asks") or []
        if not bids or not asks:
            return None
        result = dict(ob)

    result["bids"] = list((result.get("bids") or [])[:limit])
    result["asks"] = list((result.get("asks") or [])[:limit])
    return result


def get_orderbook_buffer_status(symbol: str) -> Dict[str, Any]:
    sym = _normalize_symbol(symbol)
    now_ms = _now_ms()

    with _orderbook_lock:
        ob = _orderbook_buffers.get(sym)
        last_recv_ts = _orderbook_last_recv_ts.get(sym)

    payload_ts = None
    last_update_id = None
    has_orderbook = ob is not None
    if ob is not None:
        payload_ts = ob.get("ts")
        last_update_id = ob.get("lastUpdateId")

    recv_delay_ms = None
    if last_recv_ts is not None:
        recv_delay_ms = max(0, now_ms - int(last_recv_ts))

    payload_delay_ms = None
    if payload_ts is not None:
        payload_delay_ms = max(0, now_ms - int(payload_ts))

    return {
        "symbol": sym,
        "has_orderbook": has_orderbook,
        "last_recv_ts": last_recv_ts,
        "recv_delay_ms": recv_delay_ms,
        "payload_ts": payload_ts,
        "payload_delay_ms": payload_delay_ms,
        "last_update_id": last_update_id,
    }


def _compute_kline_health(symbol: str, interval: str) -> Dict[str, Any]:
    status = get_kline_buffer_status(symbol, interval)
    buffer_len = status["buffer_len"]
    delay_ms = status["delay_ms"]

    ok = True
    reasons: List[str] = []

    min_buf = _min_buffer_for_interval(interval)
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


def _compute_orderbook_health(symbol: str) -> Dict[str, Any]:
    """
    Orderbook health는 payload integrity와 WS transport만 FAIL 조건으로 본다.

    feed inactivity는 warning 으로만 다룬다.
    """
    sym = _normalize_symbol(symbol)
    now_ms = _now_ms()
    ws_status = get_ws_status(sym)

    with _orderbook_lock:
        ob = _orderbook_buffers.get(sym)
        orderbook_last_recv_ts = _orderbook_last_recv_ts.get(sym)

    transport_reasons: List[str] = list(ws_status.get("transport_reasons") or [])
    payload_reasons: List[str] = []
    warning_reasons: List[str] = []

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
        "ok": False,
        "reasons": [],
    }

    if ob is None:
        payload_reasons.append("payload:no_orderbook")
    else:
        ts = ob.get("ts")
        if ts is None:
            payload_reasons.append("payload:no_ts")
        else:
            result["orderbook_update_delay_ms"] = max(0, now_ms - int(ts))

        bids = ob.get("bids") or []
        asks = ob.get("asks") or []

        if not bids:
            payload_reasons.append("payload:empty_bids")
        if not asks:
            payload_reasons.append("payload:empty_asks")

        best_bid = ob.get("bestBid")
        best_ask = ob.get("bestAsk")
        if best_bid is None or best_ask is None:
            payload_reasons.append("payload:no_best_prices")
        else:
            try:
                bb = float(best_bid)
                ba = float(best_ask)
                if ba <= bb:
                    payload_reasons.append("payload:crossed_book(bestAsk<=bestBid)")
            except Exception:
                payload_reasons.append("payload:invalid_best_prices")

        last_update_id = ob.get("lastUpdateId")
        if last_update_id is None:
            payload_reasons.append("payload:no_last_update_id")
        else:
            try:
                parsed_update_id = int(last_update_id)
            except Exception:
                payload_reasons.append("payload:invalid_last_update_id")
            else:
                if parsed_update_id <= 0:
                    payload_reasons.append("payload:non_positive_last_update_id")
                result["last_update_id"] = parsed_update_id

    if orderbook_last_recv_ts is None:
        if ob is not None:
            warning_reasons.append("feed:no_orderbook_recv_ts")
    else:
        recv_delay_ms = max(0, now_ms - int(orderbook_last_recv_ts))
        result["orderbook_recv_delay_ms"] = recv_delay_ms
        recv_delay_sec = recv_delay_ms / 1000.0
        if recv_delay_sec > MARKET_EVENT_MAX_DELAY_SEC:
            warning_reasons.append(
                f"feed:orderbook_recv_delay_sec>{MARKET_EVENT_MAX_DELAY_SEC} (got={recv_delay_sec:.1f})"
            )

    result["payload_ok"] = len(payload_reasons) == 0
    result["market_feed_ok"] = len(warning_reasons) == 0
    result["feed_activity_warning"] = len(warning_reasons) > 0
    result["reasons"] = transport_reasons + payload_reasons
    result["ok"] = bool(result["transport_ok"]) and bool(result["payload_ok"])
    return result


def _maybe_log_health_fail(snapshot: Dict[str, Any]) -> None:
    global _last_health_fail_log_ts, _last_health_fail_key

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
        if key == _last_health_fail_key and (now - _last_health_fail_log_ts) < _HEALTH_FAIL_LOG_SUPPRESS_SEC:
            return
        _last_health_fail_key = key
        _last_health_fail_log_ts = now

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
        "ws_required_intervals": list(REQUIRED_INTERVALS),
        "feature_required_intervals": list(FEATURE_REQUIRED_INTERVALS),
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


def get_dashboard_ws_telemetry_snapshot(
    symbol: str,
    *,
    intervals: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Dashboard 전용 WS telemetry snapshot.

    목적:
    - 대시보드가 buffer len 같은 간접 지표가 아니라
      connection_status / data_freshness / kline latency / orderbook latency 를 직접 표시하게 한다.
    - 시장데이터 레이어 책임만 제공한다. 계좌/포지션 정보는 여기서 다루지 않는다.
    """
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

    last_ws_message_latency_sec = _ms_to_sec_optional(
        ws_status.get("market_event_delay_ms"),
        "ws_status.market_event_delay_ms",
    )

    orderbook_latency_sec = _ms_to_sec_optional(
        orderbook_status.get("payload_delay_ms"),
        "orderbook.payload_delay_ms",
    )

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
        "last_ws_message_ts_ms": last_ws_message_ts_ms,
        "last_ws_message_latency_sec": last_ws_message_latency_sec,
        "data_freshness_sec": data_freshness_sec,
        "kline_latency_sec_by_tf": kline_latency_sec_by_tf,
        "kline_last_ts_ms_by_tf": kline_last_ts_ms_by_tf,
        "kline_recv_ts_ms_by_tf": kline_recv_ts_ms_by_tf,
        "orderbook_latency_sec": orderbook_latency_sec,
        "orderbook_payload_ts_ms": orderbook_status.get("payload_ts"),
        "orderbook_recv_ts_ms": orderbook_status.get("last_recv_ts"),
    }


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

                start_ts = time.time()
                ws.run_forever(
                    ping_interval=25,
                    ping_timeout=30,
                )
                session_dur = time.time() - start_ts

                with _ws_state_lock:
                    still_open = bool(_ws_connection_open.get(sym, False))
                if still_open:
                    _mark_ws_closed(sym, _now_ms(), code="RUN_FOREVER_RETURN", msg="run_forever returned")

                log("[MD_BINANCE_WS] WS disconnected → retrying ...")

            except Exception as e:
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
    """
    Atomic Market Snapshot (STRICT)

    목적:
    - strategy/feature builder가 kline + orderbook을 동일 시점으로 읽도록 한다.
    - 개별 getter 조합 호출 시 발생 가능한 non-atomic read를 방지한다.

    반환:
    {
        "symbol": str,
        "orderbook": dict | None,
        "klines": { "<interval>": [ (ts,o,h,l,c,v), ... ], ... }
    }

    주의:
    - public snapshot 계약은 기존 호환성을 위해 6-tuple을 유지한다.
    - closed state가 필요한 저장 경로는 get_klines_with_volume_and_closed()를 사용해야 한다.
    """
    sym = _normalize_symbol(symbol)

    with _kline_lock, _orderbook_lock:
        snapshot: Dict[str, Any] = {"symbol": sym, "orderbook": None, "klines": {}}

        ob = _orderbook_buffers.get(sym)
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