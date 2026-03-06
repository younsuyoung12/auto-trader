"""
========================================================
FILE: infra/account_ws.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================
역할:
- Binance USDⓈ-M Futures USER DATA STREAM 을 통해
  ACCOUNT_UPDATE / ORDER_TRADE_UPDATE / TRADE_LITE 이벤트를 수신한다.
- listenKey 생성/유지/종료를 관리한다.
- 계정/포지션/주문 상태를 메모리 스냅샷으로 유지한다.
- 상위 레이어(run_bot_ws, sync_exchange, dashboard)에 getter / health snapshot만 제공한다.

핵심 원칙 (STRICT · NO-FALLBACK):
- 잘못된 listenKey 응답 / 비정상 WS payload / 필수 필드 누락은 즉시 예외.
- payload 보정/추정/더미값 생성 금지.
- 환경변수 직접 접근 금지: settings.py(SSOT)만 사용한다.
- 민감정보(API key/secret/listenKey 전체값)는 로그/예외에 직접 출력하지 않는다.
- 텔레그램/주문/전략 로직 포함 금지.

변경 이력
--------------------------------------------------------
- 2026-03-06:
  1) 신규 생성: Binance USER DATA STREAM 전용 account websocket 모듈 추가
  2) listenKey 생성/keepalive/종료 + WS reconnect 루프 구현
  3) ACCOUNT_UPDATE / ORDER_TRADE_UPDATE / TRADE_LITE 엄격 검증 및 메모리 스냅샷 저장
  4) 상태 조회(get_account_ws_status / get_account_positions_snapshot / get_account_orders_snapshot) 추가
- 2026-03-06:
  1) 보호주문 스냅샷 조회 추가
     - get_account_open_orders_snapshot()
     - get_account_protection_orders_snapshot()
     - get_account_order_snapshot()
     - get_account_order_snapshot_by_client_order_id()
  2) ORDER_TRADE_UPDATE 상태 정규화 강화
     - open / terminal status 구분
     - 보호주문 판별용 reduce_only / close_position / order_type / position_side 유지
  3) run_bot_ws / sync_exchange 보호주문 검증 연동 준비
========================================================
"""

from __future__ import annotations

import json
import math
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

import requests
import websocket  # pip install websocket-client

from infra.telelog import log
from settings import load_settings

SET = load_settings()

# ─────────────────────────────────────────────────────────────
# Module policy constants (explicit, not env fallback)
# ─────────────────────────────────────────────────────────────
_LISTENKEY_KEEPALIVE_SEC: float = 30.0 * 60.0
_RECONNECT_MIN_SEC: float = 1.0
_RECONNECT_MAX_SEC: float = 10.0
_STALE_FAIL_SEC: float = 90.0
_REQUEST_TIMEOUT: Tuple[float, float] = (3.5, 10.0)

_OPEN_ORDER_STATUSES = ("NEW", "PARTIALLY_FILLED")
_TERMINAL_ORDER_STATUSES = ("FILLED", "CANCELED", "EXPIRED", "REJECTED", "EXPIRED_IN_MATCH")
_PROTECTION_ORDER_TYPES = ("TAKE_PROFIT_MARKET", "TAKE_PROFIT", "STOP_MARKET", "STOP")


# ─────────────────────────────────────────────────────────────
# Exceptions
# ─────────────────────────────────────────────────────────────
class AccountWsError(RuntimeError):
    """Base error for account user-data websocket."""


class AccountWsProtocolError(AccountWsError):
    """Raised when USER DATA STREAM payload violates strict schema."""


class AccountWsStateError(AccountWsError):
    """Raised when account websocket internal state is invalid."""


# ─────────────────────────────────────────────────────────────
# Settings / base endpoints (STRICT)
# ─────────────────────────────────────────────────────────────
def _require_setting_str(name: str) -> str:
    try:
        v = getattr(SET, name)
    except AttributeError as e:
        raise AccountWsStateError(f"settings.{name} is missing (STRICT)") from e
    s = str(v).strip()
    if not s:
        raise AccountWsStateError(f"settings.{name} is empty (STRICT)")
    return s


def _derive_account_ws_base_strict() -> str:
    combined = _require_setting_str("ws_combined_base")
    parts = urlsplit(combined)
    if parts.scheme not in ("ws", "wss"):
        raise AccountWsStateError("settings.ws_combined_base must be ws/wss URL (STRICT)")
    if not parts.netloc:
        raise AccountWsStateError("settings.ws_combined_base missing netloc (STRICT)")
    return f"{parts.scheme}://{parts.netloc}"


_BASE_URL: str = _require_setting_str("binance_futures_base")
_API_KEY: str = _require_setting_str("api_key")
_ACCOUNT_WS_BASE: str = _derive_account_ws_base_strict()

_SESSION: requests.Session = requests.Session()
_SESSION.headers.update(
    {
        "Accept": "application/json",
        "User-Agent": "auto-trader/binance-usdtm-accountws",
        "X-MBX-APIKEY": _API_KEY,
    }
)


# ─────────────────────────────────────────────────────────────
# In-memory state
# ─────────────────────────────────────────────────────────────
_STATE_LOCK = threading.Lock()
_THREAD_LOCK = threading.Lock()

_THREAD: Optional[threading.Thread] = None
_STOP_EVENT: Optional[threading.Event] = None
_CURRENT_WS: Optional[websocket.WebSocketApp] = None

_STATUS: Dict[str, Any] = {
    "running": False,
    "connected": False,
    "listen_key_active": False,
    "listen_key_hint": None,
    "last_event_time_ms": None,
    "last_event_type": None,
    "last_recv_local_ms": None,
    "last_error": None,
    "last_disconnect": None,
    "started_at_ms": None,
    "updated_at_ms": None,
}

# asset -> {"asset","wallet_balance","cross_wallet_balance","balance_change","event_time_ms","transaction_time_ms"}
_BALANCES: Dict[str, Dict[str, Any]] = {}

# (symbol, position_side) -> {"symbol","position_side","position_amt","entry_price","unrealized_pnl","margin_type","event_time_ms","transaction_time_ms"}
_POSITIONS: Dict[Tuple[str, str], Dict[str, Any]] = {}

# order_id -> normalized latest order update dict
_ORDER_UPDATES: Dict[str, Dict[str, Any]] = {}


# ─────────────────────────────────────────────────────────────
# Strict helpers
# ─────────────────────────────────────────────────────────────
def _now_ms() -> int:
    return int(time.time() * 1000)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_dump_for_log(obj: Any, max_len: int = 1200) -> str:
    try:
        s = json.dumps(obj, ensure_ascii=False, default=str)
    except Exception:
        s = str(obj)
    if len(s) > max_len:
        return s[:max_len] + f"...(truncated,total_len={len(s)})"
    return s


def _listen_key_hint(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    if len(s) <= 8:
        return "***"
    return f"{s[:4]}...{s[-4:]}"


def _set_status(**kwargs: Any) -> None:
    with _STATE_LOCK:
        _STATUS.update(kwargs)
        _STATUS["updated_at_ms"] = _now_ms()


def _clear_runtime_state_for_reconnect() -> None:
    with _STATE_LOCK:
        _STATUS["connected"] = False
        _STATUS["listen_key_active"] = False
        _STATUS["listen_key_hint"] = None
        _STATUS["last_disconnect"] = None


def _normalize_symbol(v: Any, *, allow_empty: bool = False) -> str:
    s = str(v or "").replace("-", "").replace("/", "").upper().strip()
    if not s and not allow_empty:
        raise AccountWsProtocolError("symbol is empty (STRICT)")
    return s


def _require_nonempty_str(v: Any, name: str) -> str:
    if not isinstance(v, str):
        raise AccountWsProtocolError(f"{name} must be str (STRICT), got={type(v).__name__}")
    s = v.strip()
    if not s:
        raise AccountWsProtocolError(f"{name} is empty (STRICT)")
    return s


def _require_int(v: Any, name: str) -> int:
    if v is None:
        raise AccountWsProtocolError(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        raise AccountWsProtocolError(f"{name} must be int (bool not allowed) (STRICT)")
    try:
        i = int(v)
    except Exception as e:
        raise AccountWsProtocolError(f"{name} must be int: {e} (STRICT)") from e
    if i <= 0:
        raise AccountWsProtocolError(f"{name} must be > 0 (STRICT)")
    return i


def _require_float(v: Any, name: str, *, allow_zero: bool = True) -> float:
    if v is None:
        raise AccountWsProtocolError(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        raise AccountWsProtocolError(f"{name} must be numeric (bool not allowed) (STRICT)")
    try:
        f = float(v)
    except Exception as e:
        raise AccountWsProtocolError(f"{name} must be numeric: {e} (STRICT)") from e
    if not math.isfinite(f):
        raise AccountWsProtocolError(f"{name} must be finite (STRICT)")
    if allow_zero:
        return float(f)
    if f <= 0:
        raise AccountWsProtocolError(f"{name} must be > 0 (STRICT)")
    return float(f)


def _require_any_finite_float(v: Any, name: str) -> float:
    if v is None:
        raise AccountWsProtocolError(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        raise AccountWsProtocolError(f"{name} must be numeric (bool not allowed) (STRICT)")
    try:
        f = float(v)
    except Exception as e:
        raise AccountWsProtocolError(f"{name} must be numeric: {e} (STRICT)") from e
    if not math.isfinite(f):
        raise AccountWsProtocolError(f"{name} must be finite (STRICT)")
    return float(f)


def _require_position_side(v: Any, name: str) -> str:
    s = _require_nonempty_str(v, name).upper()
    if s not in ("BOTH", "LONG", "SHORT"):
        raise AccountWsProtocolError(f"{name} invalid positionSide (STRICT): {s!r}")
    return s


def _require_side(v: Any, name: str) -> str:
    s = _require_nonempty_str(v, name).upper()
    if s not in ("BUY", "SELL"):
        raise AccountWsProtocolError(f"{name} invalid side (STRICT): {s!r}")
    return s


def _require_order_type(v: Any, name: str) -> str:
    return _require_nonempty_str(v, name).upper()


def _require_order_status(v: Any, name: str) -> str:
    s = _require_nonempty_str(v, name).upper()
    all_allowed = set(_OPEN_ORDER_STATUSES) | set(_TERMINAL_ORDER_STATUSES)
    if s not in all_allowed:
        raise AccountWsProtocolError(f"{name} invalid order status (STRICT): {s!r}")
    return s


def _require_execution_type(v: Any, name: str) -> str:
    return _require_nonempty_str(v, name).upper()


def _require_dict(v: Any, name: str) -> Dict[str, Any]:
    if not isinstance(v, dict):
        raise AccountWsProtocolError(f"{name} must be dict (STRICT), got={type(v).__name__}")
    return v


def _require_list(v: Any, name: str) -> List[Any]:
    if not isinstance(v, list):
        raise AccountWsProtocolError(f"{name} must be list (STRICT), got={type(v).__name__}")
    return v


def _is_open_order_status(status: str) -> bool:
    return status in _OPEN_ORDER_STATUSES


def _is_terminal_order_status(status: str) -> bool:
    return status in _TERMINAL_ORDER_STATUSES


def _is_protection_order_snapshot(row: Dict[str, Any]) -> bool:
    if not isinstance(row, dict):
        raise AccountWsStateError("order snapshot must be dict (STRICT)")
    order_type = str(row.get("order_type") or "").upper().strip()
    if order_type not in _PROTECTION_ORDER_TYPES:
        return False
    reduce_only = bool(row.get("reduce_only", False))
    close_position = bool(row.get("close_position", False))
    return bool(reduce_only or close_position)


# ─────────────────────────────────────────────────────────────
# listenKey REST
# ─────────────────────────────────────────────────────────────
def _request_listen_key_strict(method: str, *, listen_key: Optional[str] = None) -> Dict[str, Any]:
    m = str(method).upper().strip()
    if m not in ("POST", "PUT", "DELETE"):
        raise AccountWsStateError(f"unsupported listenKey method (STRICT): {m!r}")

    url = f"{_BASE_URL}/fapi/v1/listenKey"
    params: Dict[str, Any] = {}
    if listen_key is not None:
        lk = str(listen_key).strip()
        if not lk:
            raise AccountWsStateError("listen_key is empty (STRICT)")
        params["listenKey"] = lk

    try:
        resp = _SESSION.request(m, url, params=params or None, timeout=_REQUEST_TIMEOUT)
    except requests.RequestException as e:
        raise AccountWsStateError(f"{m} /fapi/v1/listenKey request failed: {e.__class__.__name__}") from e

    try:
        data = resp.json()
    except Exception as e:
        raise AccountWsStateError(f"{m} /fapi/v1/listenKey invalid json: {e.__class__.__name__}") from e

    if resp.status_code != 200:
        if isinstance(data, dict):
            code = data.get("code")
            msg = data.get("msg")
            raise AccountWsStateError(f"{m} /fapi/v1/listenKey failed: http={resp.status_code} code={code} msg={msg}")
        raise AccountWsStateError(f"{m} /fapi/v1/listenKey failed: http={resp.status_code}")

    if not isinstance(data, dict):
        raise AccountWsStateError(f"{m} /fapi/v1/listenKey returned non-dict (STRICT)")

    return data


def _create_listen_key_strict() -> str:
    data = _request_listen_key_strict("POST")
    listen_key = data.get("listenKey")
    if not isinstance(listen_key, str) or not listen_key.strip():
        raise AccountWsStateError("listenKey missing/empty in POST response (STRICT)")
    return listen_key.strip()


def _keepalive_listen_key_strict(listen_key: str) -> None:
    data = _request_listen_key_strict("PUT", listen_key=listen_key)
    if "listenKey" in data:
        lk = data.get("listenKey")
        if not isinstance(lk, str) or not lk.strip():
            raise AccountWsStateError("listenKey invalid in PUT response (STRICT)")


def _close_listen_key_strict(listen_key: str) -> None:
    data = _request_listen_key_strict("DELETE", listen_key=listen_key)
    if not isinstance(data, dict):
        raise AccountWsStateError("DELETE /fapi/v1/listenKey returned invalid payload (STRICT)")


# ─────────────────────────────────────────────────────────────
# Payload handlers
# ─────────────────────────────────────────────────────────────
def _handle_account_update_strict(payload: Dict[str, Any]) -> None:
    event_time_ms = _require_int(payload.get("E"), "ACCOUNT_UPDATE.E")
    tx_time_ms = _require_int(payload.get("T"), "ACCOUNT_UPDATE.T")
    account = _require_dict(payload.get("a"), "ACCOUNT_UPDATE.a")
    _require_nonempty_str(account.get("m"), "ACCOUNT_UPDATE.a.m")

    balances = _require_list(account.get("B"), "ACCOUNT_UPDATE.a.B")
    positions = _require_list(account.get("P"), "ACCOUNT_UPDATE.a.P")

    bal_updates: Dict[str, Dict[str, Any]] = {}
    for i, row in enumerate(balances):
        item = _require_dict(row, f"ACCOUNT_UPDATE.a.B[{i}]")
        asset = _require_nonempty_str(item.get("a"), f"ACCOUNT_UPDATE.a.B[{i}].a").upper()
        wallet_balance = _require_float(item.get("wb"), f"ACCOUNT_UPDATE.a.B[{i}].wb")
        cross_wallet_balance = _require_float(item.get("cw"), f"ACCOUNT_UPDATE.a.B[{i}].cw")
        balance_change = _require_any_finite_float(item.get("bc"), f"ACCOUNT_UPDATE.a.B[{i}].bc")

        bal_updates[asset] = {
            "asset": asset,
            "wallet_balance": float(wallet_balance),
            "cross_wallet_balance": float(cross_wallet_balance),
            "balance_change": float(balance_change),
            "event_time_ms": int(event_time_ms),
            "transaction_time_ms": int(tx_time_ms),
        }

    pos_updates: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for i, row in enumerate(positions):
        item = _require_dict(row, f"ACCOUNT_UPDATE.a.P[{i}]")
        symbol = _normalize_symbol(item.get("s"))
        position_side = _require_position_side(item.get("ps"), f"ACCOUNT_UPDATE.a.P[{i}].ps")
        position_amt = _require_any_finite_float(item.get("pa"), f"ACCOUNT_UPDATE.a.P[{i}].pa")
        entry_price = _require_float(item.get("ep"), f"ACCOUNT_UPDATE.a.P[{i}].ep")
        unrealized_pnl = _require_any_finite_float(item.get("up"), f"ACCOUNT_UPDATE.a.P[{i}].up")
        margin_type = _require_nonempty_str(item.get("mt"), f"ACCOUNT_UPDATE.a.P[{i}].mt").upper()
        if margin_type not in ("ISOLATED", "CROSSED", "CROSS"):
            raise AccountWsProtocolError(f"ACCOUNT_UPDATE.a.P[{i}].mt invalid (STRICT): {margin_type!r}")

        if abs(position_amt) > 1e-12 and entry_price <= 0:
            raise AccountWsProtocolError(f"ACCOUNT_UPDATE.a.P[{i}].ep must be > 0 for live position (STRICT)")

        pos_updates[(symbol, position_side)] = {
            "symbol": symbol,
            "position_side": position_side,
            "position_amt": float(position_amt),
            "entry_price": float(entry_price),
            "unrealized_pnl": float(unrealized_pnl),
            "margin_type": "CROSSED" if margin_type == "CROSS" else margin_type,
            "event_time_ms": int(event_time_ms),
            "transaction_time_ms": int(tx_time_ms),
        }

    with _STATE_LOCK:
        _BALANCES.update(bal_updates)
        _POSITIONS.update(pos_updates)
        _STATUS["last_event_time_ms"] = int(event_time_ms)
        _STATUS["last_event_type"] = "ACCOUNT_UPDATE"
        _STATUS["last_recv_local_ms"] = _now_ms()


def _normalize_order_update_strict(payload: Dict[str, Any]) -> Dict[str, Any]:
    event_time_ms = _require_int(payload.get("E"), "ORDER_TRADE_UPDATE.E")
    tx_time_ms = _require_int(payload.get("T"), "ORDER_TRADE_UPDATE.T")
    order = _require_dict(payload.get("o"), "ORDER_TRADE_UPDATE.o")

    symbol = _normalize_symbol(order.get("s"))
    side = _require_side(order.get("S"), "ORDER_TRADE_UPDATE.o.S")
    order_type = _require_order_type(order.get("o"), "ORDER_TRADE_UPDATE.o.o")
    time_in_force = _require_nonempty_str(order.get("f"), "ORDER_TRADE_UPDATE.o.f").upper()
    orig_qty = _require_float(order.get("q"), "ORDER_TRADE_UPDATE.o.q", allow_zero=False)
    orig_price = _require_float(order.get("p"), "ORDER_TRADE_UPDATE.o.p")
    avg_price = _require_float(order.get("ap"), "ORDER_TRADE_UPDATE.o.ap")
    stop_price = _require_float(order.get("sp"), "ORDER_TRADE_UPDATE.o.sp")
    execution_type = _require_execution_type(order.get("x"), "ORDER_TRADE_UPDATE.o.x")
    order_status = _require_order_status(order.get("X"), "ORDER_TRADE_UPDATE.o.X")
    order_id = _require_int(order.get("i"), "ORDER_TRADE_UPDATE.o.i")
    last_filled_qty = _require_float(order.get("l"), "ORDER_TRADE_UPDATE.o.l")
    cumulative_filled_qty = _require_float(order.get("z"), "ORDER_TRADE_UPDATE.o.z")
    last_filled_price = _require_float(order.get("L"), "ORDER_TRADE_UPDATE.o.L")
    commission = _require_any_finite_float(order.get("n"), "ORDER_TRADE_UPDATE.o.n")
    client_order_id = _require_nonempty_str(order.get("c"), "ORDER_TRADE_UPDATE.o.c")
    position_side = _require_position_side(order.get("ps"), "ORDER_TRADE_UPDATE.o.ps")
    realized_pnl = _require_any_finite_float(order.get("rp"), "ORDER_TRADE_UPDATE.o.rp")
    reduce_only = bool(order.get("R", False))
    close_position = bool(order.get("cp", False))

    return {
        "event_type": "ORDER_TRADE_UPDATE",
        "event_time_ms": int(event_time_ms),
        "transaction_time_ms": int(tx_time_ms),
        "symbol": symbol,
        "side": side,
        "order_type": order_type,
        "time_in_force": time_in_force,
        "orig_qty": float(orig_qty),
        "orig_price": float(orig_price),
        "avg_price": float(avg_price),
        "stop_price": float(stop_price),
        "execution_type": execution_type,
        "order_status": order_status,
        "order_id": str(order_id),
        "last_filled_qty": float(last_filled_qty),
        "cumulative_filled_qty": float(cumulative_filled_qty),
        "last_filled_price": float(last_filled_price),
        "commission": float(commission),
        "client_order_id": client_order_id,
        "position_side": position_side,
        "realized_pnl": float(realized_pnl),
        "reduce_only": bool(reduce_only),
        "close_position": bool(close_position),
        "is_open_order": bool(_is_open_order_status(order_status)),
        "is_terminal_order": bool(_is_terminal_order_status(order_status)),
        "is_protection_order": bool(
            order_type in _PROTECTION_ORDER_TYPES and (reduce_only or close_position)
        ),
        "payload": dict(order),
    }


def _handle_order_trade_update_strict(payload: Dict[str, Any]) -> None:
    normalized = _normalize_order_update_strict(payload)
    with _STATE_LOCK:
        _ORDER_UPDATES[str(normalized["order_id"])] = normalized
        _STATUS["last_event_time_ms"] = int(normalized["event_time_ms"])
        _STATUS["last_event_type"] = "ORDER_TRADE_UPDATE"
        _STATUS["last_recv_local_ms"] = _now_ms()


def _handle_trade_lite_strict(payload: Dict[str, Any]) -> None:
    event_time_ms = _require_int(payload.get("E"), "TRADE_LITE.E")
    tx_time_ms = _require_int(payload.get("T"), "TRADE_LITE.T")
    symbol = _normalize_symbol(payload.get("s"))
    side = _require_side(payload.get("S"), "TRADE_LITE.S")
    order_id = _require_int(payload.get("i"), "TRADE_LITE.i")
    qty = _require_float(payload.get("q"), "TRADE_LITE.q", allow_zero=False)
    price = _require_float(payload.get("p"), "TRADE_LITE.p", allow_zero=False)
    client_order_id = _require_nonempty_str(payload.get("c"), "TRADE_LITE.c")
    position_side = _require_position_side(payload.get("ps"), "TRADE_LITE.ps")

    normalized = {
        "event_type": "TRADE_LITE",
        "event_time_ms": int(event_time_ms),
        "transaction_time_ms": int(tx_time_ms),
        "symbol": symbol,
        "side": side,
        "order_id": str(order_id),
        "last_filled_qty": float(qty),
        "last_filled_price": float(price),
        "client_order_id": client_order_id,
        "position_side": position_side,
        "payload": dict(payload),
    }

    with _STATE_LOCK:
        existing = _ORDER_UPDATES.get(str(order_id))
        if existing is not None:
            merged = dict(existing)
            merged.update(normalized)
            _ORDER_UPDATES[str(order_id)] = merged
        else:
            _ORDER_UPDATES[str(order_id)] = normalized
        _STATUS["last_event_time_ms"] = int(event_time_ms)
        _STATUS["last_event_type"] = "TRADE_LITE"
        _STATUS["last_recv_local_ms"] = _now_ms()


def _handle_listen_key_expired_strict(payload: Dict[str, Any]) -> None:
    event_time_ms = _require_int(payload.get("E"), "listenKeyExpired.E")
    lk = payload.get("listenKey")
    if lk is not None and (not isinstance(lk, str) or not lk.strip()):
        raise AccountWsProtocolError("listenKeyExpired.listenKey invalid (STRICT)")

    with _STATE_LOCK:
        _STATUS["listen_key_active"] = False
        _STATUS["last_event_time_ms"] = int(event_time_ms)
        _STATUS["last_event_type"] = "listenKeyExpired"
        _STATUS["last_recv_local_ms"] = _now_ms()
        _STATUS["last_error"] = "listenKeyExpired"


def _handle_message_strict(data: Dict[str, Any]) -> None:
    event_type = _require_nonempty_str(data.get("e"), "payload.e")

    if event_type == "ACCOUNT_UPDATE":
        _handle_account_update_strict(data)
        return

    if event_type == "ORDER_TRADE_UPDATE":
        _handle_order_trade_update_strict(data)
        return

    if event_type == "TRADE_LITE":
        _handle_trade_lite_strict(data)
        return

    if event_type == "listenKeyExpired":
        _handle_listen_key_expired_strict(data)
        raise AccountWsProtocolError("listenKey expired (STRICT)")

    raise AccountWsProtocolError(f"unsupported user data event type (STRICT): {event_type!r}")


# ─────────────────────────────────────────────────────────────
# WebSocket callbacks
# ─────────────────────────────────────────────────────────────
def _on_open(ws: websocket.WebSocketApp) -> None:
    _ = ws
    _set_status(connected=True, last_error=None, last_disconnect=None)
    log("[ACCOUNT_WS] opened")


def _on_error(ws: websocket.WebSocketApp, error: Any) -> None:
    _ = ws
    msg = f"{type(error).__name__}: {error}" if error is not None else "unknown"
    _set_status(last_error=msg)
    log(f"[ACCOUNT_WS] error: {msg}")


def _on_close(ws: websocket.WebSocketApp, code: Any, msg: Any) -> None:
    _ = ws
    desc = f"code={code} msg={msg}"
    _set_status(connected=False, last_disconnect=desc)
    log(f"[ACCOUNT_WS] closed: {desc}")


def _on_message(ws: websocket.WebSocketApp, message: Any) -> None:
    try:
        if isinstance(message, (bytes, bytearray)):
            raw = bytes(message).decode("utf-8")
        elif isinstance(message, str):
            raw = message
        else:
            raise AccountWsProtocolError(f"unsupported ws message type (STRICT): {type(message).__name__}")

        data = json.loads(raw)
    except Exception as e:
        log(f"[ACCOUNT_WS] decode error: {type(e).__name__}: {e}")
        _set_status(last_error=f"decode_error:{type(e).__name__}")
        try:
            ws.close()
        finally:
            return

    try:
        if not isinstance(data, dict):
            raise AccountWsProtocolError("user data payload must be dict (STRICT)")
        _handle_message_strict(data)
    except Exception as e:
        log(f"[ACCOUNT_WS] protocol error: {type(e).__name__}: {e} payload={_safe_dump_for_log(data)}")
        _set_status(last_error=f"{type(e).__name__}:{e}")
        try:
            ws.close()
        finally:
            return


# ─────────────────────────────────────────────────────────────
# Keepalive / runner
# ─────────────────────────────────────────────────────────────
def _keepalive_loop(stop_event: threading.Event, listen_key: str) -> None:
    while not stop_event.wait(_LISTENKEY_KEEPALIVE_SEC):
        try:
            _keepalive_listen_key_strict(listen_key)
            _set_status(listen_key_active=True, listen_key_hint=_listen_key_hint(listen_key))
            log("[ACCOUNT_WS] listenKey keepalive OK")
        except Exception as e:
            _set_status(last_error=f"listenkey_keepalive:{type(e).__name__}:{e}", listen_key_active=False)
            log(f"[ACCOUNT_WS] listenKey keepalive failed: {type(e).__name__}: {e}")
            return


def _run_account_ws_loop(stop_event: threading.Event) -> None:
    retry_wait = _RECONNECT_MIN_SEC

    _set_status(running=True, connected=False, listen_key_active=False, started_at_ms=_now_ms())

    while not stop_event.is_set():
        listen_key: Optional[str] = None
        ws: Optional[websocket.WebSocketApp] = None
        keepalive_stop = threading.Event()
        keepalive_thread: Optional[threading.Thread] = None

        try:
            listen_key = _create_listen_key_strict()
            _set_status(listen_key_active=True, listen_key_hint=_listen_key_hint(listen_key), last_error=None)

            ws_url = f"{_ACCOUNT_WS_BASE}/ws/{listen_key}"
            log(f"[ACCOUNT_WS] connecting ... url={_ACCOUNT_WS_BASE}/ws/<listenKey>")

            ws = websocket.WebSocketApp(
                ws_url,
                on_open=_on_open,
                on_message=_on_message,
                on_error=_on_error,
                on_close=_on_close,
            )

            with _THREAD_LOCK:
                global _CURRENT_WS
                _CURRENT_WS = ws

            keepalive_thread = threading.Thread(
                target=_keepalive_loop,
                args=(keepalive_stop, listen_key),
                name="account-ws-keepalive",
                daemon=True,
            )
            keepalive_thread.start()

            started = time.time()
            ws.run_forever(ping_interval=20, ping_timeout=10)
            session_dur = time.time() - started

            if stop_event.is_set():
                break

            retry_wait = _RECONNECT_MIN_SEC if session_dur > 60.0 else min(retry_wait * 2.0, _RECONNECT_MAX_SEC)
            log(f"[ACCOUNT_WS] disconnected → reconnect after {retry_wait:.1f}s")

        except Exception as e:
            _set_status(last_error=f"{type(e).__name__}:{e}", connected=False, listen_key_active=False)
            log(f"[ACCOUNT_WS] runner error: {type(e).__name__}: {e}")
            retry_wait = min(retry_wait * 2.0, _RECONNECT_MAX_SEC)

        finally:
            keepalive_stop.set()

            if keepalive_thread is not None:
                keepalive_thread.join(timeout=1.0)

            if listen_key is not None:
                try:
                    _close_listen_key_strict(listen_key)
                    log("[ACCOUNT_WS] listenKey closed")
                except Exception as e:
                    log(f"[ACCOUNT_WS] listenKey close failed: {type(e).__name__}: {e}")

            with _THREAD_LOCK:
                _CURRENT_WS = None

            _clear_runtime_state_for_reconnect()

        if not stop_event.is_set():
            stop_event.wait(retry_wait)

    _set_status(running=False, connected=False, listen_key_active=False)
    log("[ACCOUNT_WS] runner stopped")


# ─────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────
def start_account_ws_loop() -> None:
    global _THREAD, _STOP_EVENT

    with _THREAD_LOCK:
        if _THREAD is not None and _THREAD.is_alive():
            raise AccountWsStateError("account websocket already running (STRICT)")

        stop_event = threading.Event()
        thread = threading.Thread(
            target=_run_account_ws_loop,
            args=(stop_event,),
            name="account-ws-runner",
            daemon=True,
        )
        _STOP_EVENT = stop_event
        _THREAD = thread
        thread.start()

    log("[ACCOUNT_WS] background runner started")


def stop_account_ws_loop() -> None:
    with _THREAD_LOCK:
        stop_event = _STOP_EVENT
        ws = _CURRENT_WS
        thread = _THREAD

    if stop_event is None or thread is None:
        raise AccountWsStateError("account websocket not started (STRICT)")

    stop_event.set()

    if ws is not None:
        try:
            ws.close()
        except Exception as e:
            log(f"[ACCOUNT_WS] ws.close failed during stop: {type(e).__name__}: {e}")

    thread.join(timeout=5.0)

    with _THREAD_LOCK:
        global _THREAD, _STOP_EVENT, _CURRENT_WS
        _THREAD = None
        _STOP_EVENT = None
        _CURRENT_WS = None

    _set_status(running=False, connected=False, listen_key_active=False)
    log("[ACCOUNT_WS] stopped")


def get_account_ws_status() -> Dict[str, Any]:
    with _STATE_LOCK:
        out = dict(_STATUS)

    last_recv = out.get("last_recv_local_ms")
    if isinstance(last_recv, int) and last_recv > 0:
        out["stale_ms"] = max(0, _now_ms() - last_recv)
    else:
        out["stale_ms"] = None
    return out


def is_account_ws_healthy() -> bool:
    st = get_account_ws_status()

    if not bool(st.get("running")):
        return False
    if not bool(st.get("connected")):
        return False
    if not bool(st.get("listen_key_active")):
        return False

    last_recv = st.get("last_recv_local_ms")
    if not isinstance(last_recv, int) or last_recv <= 0:
        return False

    stale_ms = max(0, _now_ms() - last_recv)
    if stale_ms > int(_STALE_FAIL_SEC * 1000.0):
        return False

    return True


def get_account_balances_snapshot() -> List[Dict[str, Any]]:
    with _STATE_LOCK:
        rows = [dict(v) for _, v in sorted(_BALANCES.items(), key=lambda kv: kv[0])]
    return rows


def get_account_positions_snapshot(symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    sym_filter = None if symbol is None else _normalize_symbol(symbol)
    with _STATE_LOCK:
        rows = [dict(v) for _, v in _POSITIONS.items()]

    if sym_filter is not None:
        rows = [r for r in rows if _normalize_symbol(r.get("symbol")) == sym_filter]

    rows.sort(key=lambda r: (str(r.get("symbol")), str(r.get("position_side"))))
    return rows


def get_latest_position_for_symbol(symbol: str) -> Optional[Dict[str, Any]]:
    sym = _normalize_symbol(symbol)
    rows = get_account_positions_snapshot(sym)
    if not rows:
        return None

    if len(rows) == 1:
        return rows[0]

    live = [r for r in rows if abs(float(r["position_amt"])) > 1e-12]
    if not live:
        return None

    dirs = set()
    for r in live:
        amt = float(r["position_amt"])
        ps = str(r["position_side"]).upper()
        if ps in ("LONG", "SHORT"):
            dirs.add(ps)
        else:
            dirs.add("LONG" if amt > 0 else "SHORT")

    if len(dirs) != 1:
        raise AccountWsStateError(f"ambiguous live positions in account ws snapshot (STRICT): symbol={sym}")

    return live[0]


def get_account_orders_snapshot(symbol: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    if not isinstance(limit, int) or limit <= 0:
        raise AccountWsStateError("limit must be int > 0 (STRICT)")
    if limit > 1000:
        raise AccountWsStateError("limit must be <= 1000 (STRICT)")

    sym_filter = None if symbol is None else _normalize_symbol(symbol)
    with _STATE_LOCK:
        rows = [dict(v) for _, v in _ORDER_UPDATES.items()]

    if sym_filter is not None:
        rows = [r for r in rows if _normalize_symbol(r.get("symbol")) == sym_filter]

    rows.sort(key=lambda r: int(r.get("event_time_ms", 0)), reverse=True)
    return rows[:limit]


def get_account_open_orders_snapshot(symbol: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    rows = get_account_orders_snapshot(symbol=symbol, limit=limit)
    out: List[Dict[str, Any]] = []
    for row in rows:
        status = str(row.get("order_status") or "").upper().strip()
        if not status:
            continue
        if _is_open_order_status(status):
            out.append(row)
    return out


def get_account_order_snapshot(order_id: str) -> Optional[Dict[str, Any]]:
    oid = str(order_id).strip()
    if not oid:
        raise AccountWsStateError("order_id is empty (STRICT)")
    with _STATE_LOCK:
        row = _ORDER_UPDATES.get(oid)
        if row is None:
            return None
        return dict(row)


def get_account_order_snapshot_by_client_order_id(
    client_order_id: str,
    *,
    symbol: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    cid = str(client_order_id).strip()
    if not cid:
        raise AccountWsStateError("client_order_id is empty (STRICT)")

    sym_filter = None if symbol is None else _normalize_symbol(symbol)

    with _STATE_LOCK:
        rows = [dict(v) for _, v in _ORDER_UPDATES.items()]

    for row in rows:
        row_cid = str(row.get("client_order_id") or "").strip()
        if row_cid != cid:
            continue
        if sym_filter is not None and _normalize_symbol(row.get("symbol")) != sym_filter:
            continue
        return row

    return None


def get_account_protection_orders_snapshot(
    symbol: str,
    *,
    position_side: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    sym = _normalize_symbol(symbol)
    ps_filter = None
    if position_side is not None:
        ps_filter = _require_position_side(position_side, "position_side")

    rows = get_account_open_orders_snapshot(symbol=sym, limit=limit)

    out: List[Dict[str, Any]] = []
    for row in rows:
        if not _is_protection_order_snapshot(row):
            continue
        row_ps = str(row.get("position_side") or "").upper().strip()
        if ps_filter is not None and row_ps != ps_filter:
            continue
        out.append(row)

    out.sort(key=lambda r: int(r.get("event_time_ms", 0)), reverse=True)
    return out


__all__ = [
    "AccountWsError",
    "AccountWsProtocolError",
    "AccountWsStateError",
    "start_account_ws_loop",
    "stop_account_ws_loop",
    "get_account_ws_status",
    "is_account_ws_healthy",
    "get_account_balances_snapshot",
    "get_account_positions_snapshot",
    "get_latest_position_for_symbol",
    "get_account_orders_snapshot",
    "get_account_open_orders_snapshot",
    "get_account_order_snapshot",
    "get_account_order_snapshot_by_client_order_id",
    "get_account_protection_orders_snapshot",
]