# infra/market_data_ws.py
"""
========================================================
FILE: infra/market_data_ws.py
ROLE:
- Binance USDT-M Futures multiplex WebSocket 수신기
- 멀티 타임프레임 kline + diff-depth orderbook을 메모리 버퍼에 유지
- WS primary market-data를 bt_candles / bt_orderbook_snapshots 에 지속 기록한다
- 상위 레이어에 getter / atomic snapshot / health snapshot / dashboard telemetry snapshot 을 제공한다

CORE RESPONSIBILITIES:
- WS 수신 데이터의 STRICT 검증 및 메모리 버퍼링
- WS kline / orderbook DB 영속화(STRICT, fail-fast)
- 부팅 시 REST bootstrap으로 필수 캔들 preload
- orderbook 은 REST snapshot + diff-depth stream 으로 정합성 있게 유지
- WS 세션 상태 / market-data freshness / orderbook payload 무결성 관측성 제공
- dashboard 가 직접 사용할 수 있는 WS telemetry snapshot 제공
- strategy / execution / exit 책임과 완전 분리
- 반복 WS 장애를 circuit breaker 상태로 승격해 운영 불가 상태를 상위로 명시 노출한다

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
- 반복 transport/protocol/feed 장애는 circuit breaker 로 누적/승격해야 하며, 단발 장애와 구분되어야 한다

CHANGE HISTORY:
- 2026-03-14:
  1) FIX(ROOT-CAUSE): partial depth5 의존 제거, REST snapshot + diff-depth(@depth@100ms) 정합 모델로 교체
  2) FEAT(ORDERBOOK-STATE): snapshot_last_update_id / stream_aligned / bids_map / asks_map 기반 로컬 북 상태 추가
  3) FIX(STRICT): diff-depth first bridge(U/u) / pu chain / gap detection strict 검증 추가
  4) FIX(RECOVERY): on_open 시 orderbook REST snapshot bootstrap 실패를 protocol 장애로 승격하고 즉시 세션 종료
  5) FIX(OBSERVABILITY): orderbook source(rest_bootstrap / ws_diff_depth) 및 breaker 사유 노출 보강
  6) FIX(TRADE-GRADE): on_open bootstrap 도중 유입되는 diff-depth 이벤트를 pending queue 로 버퍼링한 뒤
     snapshot 이후 strict bridge/chain 규칙으로 순차 반영
  7) FIX(RECOVERY): bootstrap placeholder state 추가로 snapshot 준비 전 diff-depth 도착 시 즉시 protocol fail 되지 않도록 정합 처리
  8) FEAT(CIRCUIT-BREAKER): 반복 WS transport/protocol/feed-stale 장애를 연속 실패로 누적하고 breaker open 상태를 추가
  9) FEAT(CIRCUIT-BREAKER): run_forever 반복 종료 / protocol_error / decode_error / orderbook feed gap 을 circuit breaker failure source 로 승격
  10) FEAT(OBSERVABILITY): ws_status / health_snapshot / dashboard telemetry 에 circuit breaker 상태/사유/trip_count 노출 추가
  11) FIX(RECOVERY): circuit breaker open 중에는 즉시 재접속 루프를 멈추고 cool-down 이후 재시도하도록 수정
  12) FIX(STRICT): successful on_open 시 circuit breaker 연속 실패 카운터를 명시적으로 reset
- 2026-03-13:
  1) FIX(ROOT-CAUSE): ws_status.state 를 OPEN / OPENING / RECONNECTING / CLOSED 계약으로 명시
  2) FIX(ROOT-CAUSE): reconnect grace 동안 connection_not_open 을 즉시 transport fail 로 승격하지 않도록 수정
  3) FIX(ROOT-CAUSE): 새 candle 도착 시 직전 open candle 정상 rollover close 확정 로직 추가
  4) FIX(ROOT-CAUSE): same-candle rollback anomaly(high/low/volume/quote_volume)가 버퍼 overwrite 로 반영되지 않도록 수정
  5) FIX(OPERABILITY): orderbook 초기 수신 전 짧은 startup 구간은 warning 으로 처리
  6) FIX(OPERABILITY): websocket-client ping_timeout 제거로 ping/pong timeout 강제종료 완화
  7) FIX(CONTRACT): quote_volume rollback 추적 상태 추가
  8) FIX(ROOT-CAUSE): 최신 버퍼보다 과거 ts kline packet 은 stale packet 으로 간주하고 무시
  9) FIX(ROOT-CAUSE): 누락된 _derive_rest_kline_closed_strict 복원 및 REST closed-state 계약 일관화
  10) FIX(ROOT-CAUSE): orderbook feed inactivity 가 stale threshold를 초과하면 WS 세션 강제 재연결 요청
  11) FIX(RECOVERY): forced reconnect / ws close 시 stale orderbook runtime state 즉시 제거
  12) FIX(OBSERVABILITY): ws_status 에 reconnect_requested / reconnect_request_reason 노출 추가
  13) FIX(RECOVERY): 현재 활성 WebSocketApp 레지스트리 추가로 health path 에서 안전한 close/reconnect 가능화
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
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

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


def _make_orderbook_bootstrap_placeholder_state() -> Dict[str, Any]:
    return {
        "ready": False,
        "bootstrapping": True,
        "stream_aligned": False,
        "snapshot_last_update_id": None,
        "last_update_id": None,
        "bids_map": {},
        "asks_map": {},
        "last_snapshot_recv_ts": None,
        "pending_events": [],
    }


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


def _classify_same_candle_update_strict(
    prev: KlineRow,
    new_row: KlineRow,
    *,
    prev_quote_volume: Optional[float],
    new_quote_volume: float,
    context: str,
) -> bool:
    prev_ts, prev_o, prev_h, prev_l, _prev_c, prev_v, prev_closed = prev
    new_ts, new_o, new_h, new_l, _new_c, new_v, new_closed = new_row

    if new_ts != prev_ts:
        raise WSProtocolError(f"{context} ts mismatch during same-candle update (STRICT)")

    if prev_closed and not new_closed:
        raise WSProtocolError(f"{context} closed candle cannot reopen (STRICT)")

    if abs(new_o - prev_o) > 1e-12:
        raise WSProtocolError(f"{context} open price changed within same candle (STRICT)")

    if new_h + 1e-12 < prev_h:
        log(f"[WS_ANOMALY] {context} high rollback prev={prev_h} new={new_h}")
        return False

    if new_l - 1e-12 > prev_l:
        log(f"[WS_ANOMALY] {context} low rollback prev={prev_l} new={new_l}")
        return False

    if new_v + 1e-12 < prev_v:
        log(f"[WS_ANOMALY] {context} volume rollback prev={prev_v} new={new_v}")
        return False

    if prev_quote_volume is not None and new_quote_volume + 1e-12 < prev_quote_volume:
        log(f"[WS_ANOMALY] {context} quote_volume rollback prev={prev_quote_volume} new={new_quote_volume}")
        return False

    return True


def _plan_kline_update_strict(
    buf: List[KlineRow],
    row: KlineRow,
    *,
    interval: str,
    quote_volume: float,
    prev_quote_volume: Optional[float],
    context: str,
) -> Tuple[bool, Optional[KlineRow]]:
    _ = quote_volume
    ts = int(row[0])
    if not buf:
        return True, None

    last = buf[-1]
    last_ts = int(last[0])

    if ts < last_ts:
        log(f"[WS_INFO] {context} stale kline ignored new_ts={ts} last_ts={last_ts}")
        return False, None

    if ts == last_ts:
        should_apply = _classify_same_candle_update_strict(
            last,
            row,
            prev_quote_volume=prev_quote_volume,
            new_quote_volume=quote_volume,
            context=context,
        )
        return should_apply, None

    if bool(last[6]) is False:
        interval_ms = _interval_to_ms_strict(interval)
        delta_ms = ts - last_ts
        if delta_ms != interval_ms:
            raise WSProtocolError(
                f"{context} new candle arrived before previous candle closed with invalid rollover delta (STRICT): "
                f"prev_ts={last_ts} new_ts={ts} delta_ms={delta_ms} expected_ms={interval_ms}"
            )

        sealed_prev: KlineRow = (
            int(last[0]),
            float(last[1]),
            float(last[2]),
            float(last[3]),
            float(last[4]),
            float(last[5]),
            True,
        )
        return True, sealed_prev

    return True, None


def _commit_kline_update_strict(
    buf: List[KlineRow],
    row: KlineRow,
    *,
    key: Tuple[str, str],
    quote_volume: float,
    should_apply: bool,
    sealed_prev: Optional[KlineRow],
    context: str,
) -> None:
    if not should_apply:
        return

    ts = int(row[0])

    if not buf:
        buf.append(row)
        _kline_last_quote_volume[key] = (ts, float(quote_volume))
        return

    last = buf[-1]
    last_ts = int(last[0])

    if ts < last_ts:
        raise WSProtocolError(f"{context} kline timestamp rollback on commit (STRICT): new_ts={ts} last_ts={last_ts}")

    if ts == last_ts:
        buf[-1] = row
        _kline_last_quote_volume[key] = (ts, float(quote_volume))
        return

    if sealed_prev is not None:
        if int(buf[-1][0]) != int(sealed_prev[0]):
            raise WSProtocolError(f"{context} sealed prev ts mismatch on commit (STRICT)")
        buf[-1] = sealed_prev
        log(f"[WS_RECOVERY] {context} previous open candle sealed on rollover prev_ts={sealed_prev[0]} new_ts={ts}")

    buf.append(row)
    if len(buf) > MAX_KLINES:
        del buf[0 : len(buf) - MAX_KLINES]
    _kline_last_quote_volume[key] = (ts, float(quote_volume))


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
        raise RuntimeError("symbol is required to build WS streams (STRICT)")

    streams: List[str] = []

    for iv in WS_INTERVALS:
        streams.append(f"{s}@kline_{iv}")

    # TRADE-GRADE CONTRACT
    # engine orderbook pipeline requires depth5 snapshot stream
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


def bootstrap_klines_from_rest_strict(symbol: str, intervals: Optional[List[str]] = None) -> Dict[str, int]:
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
        prev_q_entry = _kline_last_quote_volume.get(key)
        prev_quote_volume: Optional[float] = None
        if prev_q_entry is not None and int(prev_q_entry[0]) == int(buf[-1][0] if buf else ts):
            prev_quote_volume = float(prev_q_entry[1])

        should_apply, sealed_prev = _plan_kline_update_strict(
            buf,
            row,
            interval=iv,
            quote_volume=q,
            prev_quote_volume=prev_quote_volume,
            context=f"ws_kline[{sym}:{iv}]",
        )

    if sealed_prev is not None:
        sealed_quote_volume = prev_quote_volume
        if sealed_quote_volume is None:
            raise WSProtocolError(
                f"ws_kline[{sym}:{iv}] rollover close missing previous quote_volume tracking (STRICT)"
            )

        _persist_ws_kline_strict(
            symbol=sym,
            interval=iv,
            ts_ms=int(sealed_prev[0]),
            open_=float(sealed_prev[1]),
            high=float(sealed_prev[2]),
            low=float(sealed_prev[3]),
            close=float(sealed_prev[4]),
            volume=float(sealed_prev[5]),
            quote_volume=float(sealed_quote_volume),
            is_closed=True,
        )

    if should_apply:
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
        _commit_kline_update_strict(
            buf,
            row,
            key=key,
            quote_volume=q,
            should_apply=should_apply,
            sealed_prev=sealed_prev,
            context=f"ws_kline[{sym}:{iv}]",
        )
        if should_apply:
            _kline_last_recv_ts[key] = now_ms


def _normalize_depth_side_strict(
    side_val: Any,
    *,
    name: str,
    allow_zero_qty: bool = False,
    allow_empty: bool = False,
) -> List[List[float]]:
    if side_val is None:
        raise WSProtocolError(f"{name} missing")
    if not isinstance(side_val, (list, tuple)):
        raise WSProtocolError(f"{name} must be list/tuple")

    out: List[List[float]] = []
    for i, row in enumerate(side_val):
        if isinstance(row, (list, tuple)) and len(row) >= 2:
            p = _require_float(row[0], f"{name}[{i}].price")
            q = _require_float(row[1], f"{name}[{i}].qty", allow_zero=allow_zero_qty)
            out.append([p, q])
            continue
        if isinstance(row, dict):
            p = _require_float(row.get("price"), f"{name}[{i}].price")
            q = _require_float(row.get("qty"), f"{name}[{i}].qty", allow_zero=allow_zero_qty)
            out.append([p, q])
            continue
        raise WSProtocolError(f"{name}[{i}] invalid level type: {type(row).__name__}")

    if not out and not allow_empty:
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


def _build_orderbook_price_map_strict(levels: List[List[float]], *, name: str) -> Dict[float, float]:
    price_map: Dict[float, float] = {}
    for idx, row in enumerate(levels):
        if not isinstance(row, list) or len(row) != 2:
            raise WSProtocolError(f"{name}[{idx}] must be [price, qty] (STRICT)")
        price = _require_float(row[0], f"{name}[{idx}].price")
        qty = _require_float(row[1], f"{name}[{idx}].qty")
        if qty <= 0.0:
            raise WSProtocolError(f"{name}[{idx}].qty must be > 0 for snapshot/book state (STRICT)")
        price_map[price] = qty
    if not price_map:
        raise WSProtocolError(f"{name} produced empty price_map (STRICT)")
    return price_map


def _apply_depth_updates_to_price_map_strict(
    price_map: Dict[float, float],
    updates: List[List[float]],
    *,
    name: str,
) -> None:
    if not isinstance(price_map, dict):
        raise WSProtocolError(f"{name} price_map must be dict (STRICT)")

    for idx, row in enumerate(updates):
        if not isinstance(row, list) or len(row) != 2:
            raise WSProtocolError(f"{name}[{idx}] must be [price, qty] (STRICT)")
        price = _require_float(row[0], f"{name}[{idx}].price")
        qty = _require_float(row[1], f"{name}[{idx}].qty", allow_zero=True)
        if qty == 0.0:
            price_map.pop(price, None)
        else:
            price_map[price] = qty


def _materialize_orderbook_side_top_n_strict(
    price_map: Dict[float, float],
    *,
    name: str,
    descending: bool,
    limit: int,
) -> List[List[float]]:
    if not isinstance(price_map, dict):
        raise WSProtocolError(f"{name} price_map must be dict (STRICT)")
    if limit <= 0:
        raise WSProtocolError(f"{name} limit must be > 0 (STRICT)")
    if not price_map:
        raise WSProtocolError(f"{name} empty (STRICT)")

    ordered = sorted(price_map.items(), key=lambda kv: kv[0], reverse=descending)
    out: List[List[float]] = []
    for price, qty in ordered[:limit]:
        if price <= 0.0:
            raise WSProtocolError(f"{name} contains non-positive price (STRICT)")
        if qty <= 0.0:
            raise WSProtocolError(f"{name} contains non-positive qty (STRICT)")
        out.append([float(price), float(qty)])

    if not out:
        raise WSProtocolError(f"{name} top-n materialization empty (STRICT)")
    return out


def _materialize_orderbook_top_levels_strict(
    bids_map: Dict[float, float],
    asks_map: Dict[float, float],
) -> Tuple[List[List[float]], List[List[float]], float, float, float]:
    bids_top = _materialize_orderbook_side_top_n_strict(
        bids_map,
        name="orderbook.bids_map",
        descending=True,
        limit=_ORDERBOOK_TOP_N,
    )
    asks_top = _materialize_orderbook_side_top_n_strict(
        asks_map,
        name="orderbook.asks_map",
        descending=False,
        limit=_ORDERBOOK_TOP_N,
    )
    best_bid, best_ask = _compute_best_prices_strict(bids_top, asks_top)
    spread_pct = _compute_spread_pct(best_bid, best_ask)
    return bids_top, asks_top, best_bid, best_ask, spread_pct


def _normalize_orderbook_diff_event_strict(payload: Dict[str, Any]) -> Dict[str, Any]:
    first_update_id = _require_positive_int(payload.get("U"), "orderbook.U")
    final_update_id = _require_positive_int(payload.get("u"), "orderbook.u")
    if final_update_id < first_update_id:
        raise WSProtocolError(
            f"orderbook update id range invalid (STRICT): U={first_update_id} u={final_update_id}"
        )

    prev_final_update_id: Optional[int] = None
    raw_prev_final_update_id = payload.get("pu")
    if raw_prev_final_update_id is not None:
        prev_final_update_id = _require_positive_int(raw_prev_final_update_id, "orderbook.pu")

    raw_bids = payload.get("b") if payload.get("b") is not None else payload.get("bids")
    raw_asks = payload.get("a") if payload.get("a") is not None else payload.get("asks")

    normalized_bids = _normalize_depth_side_strict(
        raw_bids,
        name="orderbook.bids",
        allow_zero_qty=True,
        allow_empty=True,
    )
    normalized_asks = _normalize_depth_side_strict(
        raw_asks,
        name="orderbook.asks",
        allow_zero_qty=True,
        allow_empty=True,
    )

    return {
        "U": int(first_update_id),
        "u": int(final_update_id),
        "pu": prev_final_update_id,
        "bids": normalized_bids,
        "asks": normalized_asks,
        "event_ts_ms": _extract_orderbook_event_ts_ms_strict(payload),
    }


def _queue_orderbook_pending_event_locked(symbol: str, state: Dict[str, Any], event: Dict[str, Any]) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise WSProtocolError("symbol is required for pending diff-depth queue (STRICT)")
    if not isinstance(state, dict):
        raise WSProtocolError("orderbook state must be dict for pending queue (STRICT)")
    pending = state.get("pending_events")
    if not isinstance(pending, list):
        raise WSProtocolError("orderbook_state.pending_events must be list (STRICT)")
    pending.append(dict(event))
    if len(pending) > _ORDERBOOK_PENDING_EVENT_MAX:
        raise WSProtocolError(
            f"pending diff-depth queue overflow (STRICT): symbol={sym} size={len(pending)} "
            f"cap={_ORDERBOOK_PENDING_EVENT_MAX}"
        )


def _build_orderbook_state_from_snapshot_strict(
    symbol: str,
    snapshot_payload: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise WSProtocolError("symbol is required for snapshot state build (STRICT)")

    last_update_id = _require_positive_int(snapshot_payload.get("lastUpdateId"), "depth_snapshot.lastUpdateId")

    bids = _normalize_depth_side_strict(
        snapshot_payload.get("bids"),
        name="depth_snapshot.bids",
        allow_zero_qty=False,
        allow_empty=False,
    )
    asks = _normalize_depth_side_strict(
        snapshot_payload.get("asks"),
        name="depth_snapshot.asks",
        allow_zero_qty=False,
        allow_empty=False,
    )

    bids_map = _build_orderbook_price_map_strict(bids, name="depth_snapshot.bids")
    asks_map = _build_orderbook_price_map_strict(asks, name="depth_snapshot.asks")
    bids_top, asks_top, best_bid, best_ask, spread_pct = _materialize_orderbook_top_levels_strict(
        bids_map,
        asks_map,
    )

    now_ms = _now_ms()

    state = {
        "ready": True,
        "bootstrapping": True,
        "stream_aligned": False,
        "snapshot_last_update_id": int(last_update_id),
        "last_update_id": int(last_update_id),
        "bids_map": bids_map,
        "asks_map": asks_map,
        "last_snapshot_recv_ts": now_ms,
        "pending_events": [],
    }
    snapshot = {
        "bids": bids_top,
        "asks": asks_top,
        "ts": now_ms,
        "bestBid": best_bid,
        "bestAsk": best_ask,
        "spreadPct": spread_pct,
        "lastUpdateId": int(last_update_id),
        "exchTs": None,
        "source": "rest_bootstrap",
        "streamAligned": False,
        "snapshotLastUpdateId": int(last_update_id),
    }
    return state, snapshot


def _advance_orderbook_state_with_event_strict(
    symbol: str,
    state: Dict[str, Any],
    event: Dict[str, Any],
) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise WSProtocolError("symbol is required for orderbook diff advance (STRICT)")
    if not isinstance(state, dict):
        raise WSProtocolError("orderbook state must be dict (STRICT)")
    if not isinstance(event, dict):
        raise WSProtocolError("orderbook diff event must be dict (STRICT)")

    ready = bool(state.get("ready", False))
    if not ready:
        raise WSProtocolError("orderbook state not ready for diff advance (STRICT)")

    current_last_update_id = _require_positive_int(
        state.get("last_update_id"),
        "orderbook_state.last_update_id",
    )
    snapshot_last_update_id = _require_positive_int(
        state.get("snapshot_last_update_id"),
        "orderbook_state.snapshot_last_update_id",
    )
    stream_aligned = bool(state.get("stream_aligned", False))

    first_update_id = _require_positive_int(event.get("U"), "orderbook_event.U")
    final_update_id = _require_positive_int(event.get("u"), "orderbook_event.u")
    if final_update_id <= current_last_update_id:
        log(
            f"[WS_INFO] orderbook replay/rollback ignored "
            f"symbol={sym} prev={current_last_update_id} new={final_update_id}"
        )
        return None

    prev_final_update_id = event.get("pu")
    if prev_final_update_id is not None:
        prev_final_update_id = _require_positive_int(prev_final_update_id, "orderbook_event.pu")

    bridge_update_id = current_last_update_id + 1
    if not stream_aligned:
        if first_update_id > bridge_update_id or final_update_id < bridge_update_id:
            raise WSProtocolError(
                "first diff depth event does not bridge REST snapshot (STRICT): "
                f"symbol={sym} snapshot_last_update_id={snapshot_last_update_id} "
                f"state_last_update_id={current_last_update_id} U={first_update_id} u={final_update_id}"
            )
    else:
        if prev_final_update_id is not None and prev_final_update_id != current_last_update_id:
            raise WSProtocolError(
                "diff depth chain broken by pu mismatch (STRICT): "
                f"symbol={sym} expected_pu={current_last_update_id} got_pu={prev_final_update_id}"
            )
        if first_update_id > bridge_update_id:
            raise WSProtocolError(
                "diff depth gap detected (STRICT): "
                f"symbol={sym} expected_U<={bridge_update_id} got_U={first_update_id}"
            )

    bids_map_raw = state.get("bids_map")
    asks_map_raw = state.get("asks_map")
    if not isinstance(bids_map_raw, dict) or not isinstance(asks_map_raw, dict):
        raise WSProtocolError("orderbook state bids_map/asks_map missing (STRICT)")

    next_bids_map = dict(bids_map_raw)
    next_asks_map = dict(asks_map_raw)

    _apply_depth_updates_to_price_map_strict(next_bids_map, list(event.get("bids") or []), name="orderbook.bids")
    _apply_depth_updates_to_price_map_strict(next_asks_map, list(event.get("asks") or []), name="orderbook.asks")

    bids_top, asks_top, best_bid, best_ask, spread_pct = _materialize_orderbook_top_levels_strict(
        next_bids_map,
        next_asks_map,
    )

    next_state = {
        "ready": True,
        "bootstrapping": bool(state.get("bootstrapping", False)),
        "stream_aligned": True,
        "snapshot_last_update_id": int(snapshot_last_update_id),
        "last_update_id": int(final_update_id),
        "bids_map": next_bids_map,
        "asks_map": next_asks_map,
        "last_snapshot_recv_ts": state.get("last_snapshot_recv_ts"),
        "pending_events": list(state.get("pending_events") or []),
    }
    snapshot = {
        "bids": bids_top,
        "asks": asks_top,
        "ts": int(event["event_ts_ms"]),
        "bestBid": best_bid,
        "bestAsk": best_ask,
        "spreadPct": spread_pct,
        "lastUpdateId": int(final_update_id),
        "exchTs": int(event["event_ts_ms"]),
        "source": "ws_diff_depth",
        "streamAligned": True,
        "snapshotLastUpdateId": int(snapshot_last_update_id),
    }
    return next_state, snapshot


def _bootstrap_orderbook_from_rest_snapshot_strict(symbol: str) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise RuntimeError("symbol is required for REST depth bootstrap")

    snapshot_payload = _rest_fetch_depth_snapshot_strict(sym, _ORDERBOOK_SNAPSHOT_LIMIT)
    state_from_snapshot, snapshot_view = _build_orderbook_state_from_snapshot_strict(sym, snapshot_payload)

    with _orderbook_lock:
        placeholder = _orderbook_book_state.get(sym)
        if not isinstance(placeholder, dict) or not bool(placeholder.get("bootstrapping", False)):
            raise WSProtocolError("orderbook bootstrap placeholder missing (STRICT)")

        pending_events_raw = placeholder.get("pending_events")
        if not isinstance(pending_events_raw, list):
            raise WSProtocolError("orderbook bootstrap placeholder pending_events invalid (STRICT)")

        state = dict(state_from_snapshot)
        state["pending_events"] = list(pending_events_raw)

        _orderbook_book_state[sym] = state
        _orderbook_buffers[sym] = dict(snapshot_view)
        _orderbook_last_update_id[sym] = int(snapshot_view["lastUpdateId"])
        _orderbook_last_recv_ts[sym] = int(state["last_snapshot_recv_ts"])

        last_ws_snapshot: Optional[Dict[str, Any]] = None
        while True:
            pending = state.get("pending_events")
            if not isinstance(pending, list):
                raise WSProtocolError("orderbook_state.pending_events must remain list (STRICT)")
            if not pending:
                break

            event = pending.pop(0)
            transition = _advance_orderbook_state_with_event_strict(sym, state, event)
            if transition is None:
                continue

            next_state, ws_snapshot = transition
            _persist_ws_orderbook_snapshot_strict(
                symbol=sym,
                event_ts_ms=int(ws_snapshot["exchTs"]),
                bids=list(ws_snapshot["bids"]),
                asks=list(ws_snapshot["asks"]),
            )
            state = next_state
            _orderbook_book_state[sym] = state
            _orderbook_buffers[sym] = dict(ws_snapshot)
            _orderbook_last_update_id[sym] = int(ws_snapshot["lastUpdateId"])
            _orderbook_last_recv_ts[sym] = _now_ms()
            last_ws_snapshot = ws_snapshot

        state["bootstrapping"] = False
        _orderbook_book_state[sym] = state

        if last_ws_snapshot is not None:
            _orderbook_buffers[sym] = dict(last_ws_snapshot)
            _orderbook_last_update_id[sym] = int(last_ws_snapshot["lastUpdateId"])
        else:
            snapshot_view["streamAligned"] = False
            _orderbook_buffers[sym] = dict(snapshot_view)
            _orderbook_last_update_id[sym] = int(snapshot_view["lastUpdateId"])

    if last_ws_snapshot is None:
        log(
            f"[MD_BINANCE_WS] orderbook snapshot bootstrapped: "
            f"symbol={sym} lastUpdateId={snapshot_view['lastUpdateId']} "
            f"bestBid={snapshot_view['bestBid']} bestAsk={snapshot_view['bestAsk']}"
        )
    else:
        log(
            f"[MD_BINANCE_WS] orderbook snapshot bootstrapped+aligned: "
            f"symbol={sym} snapshot_lastUpdateId={snapshot_view['lastUpdateId']} "
            f"final_lastUpdateId={last_ws_snapshot['lastUpdateId']}"
        )


def _push_orderbook(symbol: str, payload: Dict[str, Any]) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise WSProtocolError("empty symbol in orderbook push")

    event = _normalize_orderbook_diff_event_strict(payload)
    now_ms = _now_ms()
    _mark_ws_message(sym, now_ms)

    with _orderbook_lock:
        state = _orderbook_book_state.get(sym)
        if not isinstance(state, dict):
            raise WSProtocolError("orderbook state missing/not dict (STRICT)")

        if bool(state.get("bootstrapping", False)):
            _queue_orderbook_pending_event_locked(sym, state, event)
            return

        if not bool(state.get("ready", False)):
            raise WSProtocolError("orderbook state not bootstrapped from REST snapshot (STRICT)")

        transition = _advance_orderbook_state_with_event_strict(sym, state, event)
        if transition is None:
            return

        next_state, ob_snapshot = transition
        _persist_ws_orderbook_snapshot_strict(
            symbol=sym,
            event_ts_ms=int(ob_snapshot["exchTs"]),
            bids=list(ob_snapshot["bids"]),
            asks=list(ob_snapshot["asks"]),
        )

        _orderbook_book_state[sym] = next_state
        _orderbook_buffers[sym] = ob_snapshot
        _orderbook_last_update_id[sym] = int(ob_snapshot["lastUpdateId"])
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
    _clear_orderbook_runtime_state(sym)
    _mark_ws_closed(sym, _now_ms(), code=code, msg=msg)
    log(f"[MD_BINANCE_WS] closed: {code} {msg}")


def _on_open(symbol: str, ws: websocket.WebSocketApp) -> None:
    _ = ws
    streams = _build_stream_names(symbol)
    sym = _normalize_symbol(symbol)

    with _orderbook_lock:
        _clear_orderbook_runtime_state(sym)
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
    quote_volume_by_ts: Dict[int, float] = {}

    for idx, row in enumerate(rest_klines):
        quote_volume_optional = _extract_quote_volume_from_rest_row_optional(row, idx)

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
            if quote_volume_optional is not None:
                quote_volume_by_ts[ts] = quote_volume_optional
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
            if quote_volume_optional is not None:
                quote_volume_by_ts[ts] = quote_volume_optional
            continue

        raise RuntimeError(f"rest_klines[{idx}] invalid type (STRICT): {type(row).__name__}")

    converted.sort(key=lambda x: x[0])
    preload_klines(sym, iv, converted)

    key = (sym, iv)
    latest_ts = int(converted[-1][0])
    latest_quote_volume = quote_volume_by_ts.get(latest_ts)

    with _kline_lock:
        if latest_quote_volume is not None:
            _kline_last_quote_volume[key] = (latest_ts, float(latest_quote_volume))
        else:
            _kline_last_quote_volume.pop(key, None)


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
        state = _orderbook_book_state.get(sym)

    payload_ts = None
    last_update_id = None
    has_orderbook = ob is not None
    orderbook_source = None
    if ob is not None:
        payload_ts = ob.get("ts")
        last_update_id = ob.get("lastUpdateId")
        orderbook_source = ob.get("source")

    recv_delay_ms = None
    if last_recv_ts is not None:
        recv_delay_ms = max(0, now_ms - int(last_recv_ts))

    payload_delay_ms = None
    if payload_ts is not None:
        payload_delay_ms = max(0, now_ms - int(payload_ts))

    stream_aligned = None
    snapshot_last_update_id = None
    bootstrapping = None
    if isinstance(state, dict):
        if "stream_aligned" in state:
            stream_aligned = bool(state.get("stream_aligned"))
        if state.get("snapshot_last_update_id") is not None:
            snapshot_last_update_id = int(state["snapshot_last_update_id"])
        if "bootstrapping" in state:
            bootstrapping = bool(state.get("bootstrapping"))

    return {
        "symbol": sym,
        "has_orderbook": has_orderbook,
        "last_recv_ts": last_recv_ts,
        "recv_delay_ms": recv_delay_ms,
        "payload_ts": payload_ts,
        "payload_delay_ms": payload_delay_ms,
        "last_update_id": last_update_id,
        "source": orderbook_source,
        "stream_aligned": stream_aligned,
        "snapshot_last_update_id": snapshot_last_update_id,
        "bootstrapping": bootstrapping,
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
    sym = _normalize_symbol(symbol)
    now_ms = _now_ms()
    ws_status = get_ws_status(sym)
    ob_status = get_orderbook_buffer_status(sym)

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
        "orderbook_source": ob_status.get("source"),
        "stream_aligned": ob_status.get("stream_aligned"),
        "snapshot_last_update_id": ob_status.get("snapshot_last_update_id"),
        "bootstrapping": ob_status.get("bootstrapping"),
        "circuit_breaker_open": bool(ws_status.get("circuit_breaker_open", False)),
        "ok": False,
        "reasons": [],
    }

    ws_state = str(ws_status.get("state") or "").upper().strip()
    last_open_ts = ws_status.get("last_open_ts")

    if ob is None:
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

        stream_aligned = ob_status.get("stream_aligned")
        source = ob_status.get("source")
        if source == "ws_diff_depth" and stream_aligned is not True:
            payload_reasons.append("payload:ws_diff_depth_not_aligned")

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


def get_dashboard_ws_telemetry_snapshot(
    symbol: str,
    *,
    intervals: Optional[List[str]] = None,
) -> Dict[str, Any]:
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
                    ws.run_forever(
                        ping_interval=20,
                    )
                    session_dur = time.time() - start_ts
                finally:
                    _unregister_ws_app(sym, ws)

                with _ws_state_lock:
                    still_open = bool(_ws_connection_open.get(sym, False))
                if still_open:
                    _clear_orderbook_runtime_state(sym)
                    _mark_ws_closed(sym, _now_ms(), code="RUN_FOREVER_RETURN", msg="run_forever returned")

                if session_dur <= 60.0:
                    _record_ws_circuit_breaker_failure(sym, reason="run_forever_return_short_session")

                log("[MD_BINANCE_WS] WS disconnected → retrying ...")

            except Exception as e:
                _record_ws_circuit_breaker_failure(sym, reason=f"run_forever_exception:{type(e).__name__}")
                _mark_ws_error(sym, f"run_forever_exception:{type(e).__name__}:{e}")
                _clear_orderbook_runtime_state(sym)
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