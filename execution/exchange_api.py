# exchange_api.py
# =============================================================================
# Design principles (Binance USDT-M Futures REST Adapter)
# -----------------------------------------------------------------------------
# - Binance USDT-M Futures (/fapi/*) REST endpoints only.
# - Transport/Query layer only: signing, HTTP requests, account/position/order
#   조회 및 레버리지/마진 모드 설정만 제공한다.
# - 주문 실행(진입/청산/TP/SL 생성/슬리피지 계산)은 order_executor.py가 담당한다.
# - Fallback 금지: 오류 발생 시 즉시 예외(RuntimeError/ValueError).
# - 민감 정보(API key/secret/signature)는 로그/예외 메시지에 포함하지 않는다.
# =============================================================================
# One-way mode only.
# Hedge mode is NOT supported.
# positionSide must always be BOTH in order layer.
# =============================================================================
#
# =============================================================================
# 변경 이력
# -----------------------------------------------------------------------------
# 2026-03-02 (PATCH)
# 1) retry 정책 STRICT 통일
#    - execution.retry_policy.execute_with_retry 를 필수 import로 강제
#    - "없으면 조용히 no-retry" 폴백 제거 (ImportError로 즉시 실패)
# 2) Settings 단일 소스 정합
#    - BASE_URL / timeout / recvWindow 를 settings 값으로 우선 적용
#      (binance_futures_base / request_timeout_sec / recv_window_ms)
# 3) 불필요 코드 정리
#    - retry fallback placeholder 제거
#
# 2026-03-01
# - set_leverage_and_mode()를 상태 검증 기반 구조로 전면 수정.
# - 기존: 무조건 set 시도 → 이미 설정된 경우(-4046 등)도 실패 처리.
# - 변경: 현재 leverage/marginType 조회 후 다른 경우에만 set 호출.
# - "이미 설정됨"은 정상 상태로 간주.
# - req()의 STRICT 예외 정책은 유지 (HTTP 코드 완화하지 않음).
# =============================================================================

from __future__ import annotations

import hashlib
import hmac
import logging
import time
from typing import Any, Dict, List, Mapping, Optional, Tuple
from urllib.parse import urlencode

import requests

from execution.retry_policy import execute_with_retry  # STRICT (no fallback)
from settings import load_settings

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Global settings / session
# -----------------------------------------------------------------------------
SET = load_settings()

BASE_URL: str = str(getattr(SET, "binance_futures_base", "") or "").strip() or "https://fapi.binance.com"
DEFAULT_TIMEOUT_SEC: int = int(getattr(SET, "request_timeout_sec", 10) or 10)
RECV_WINDOW_MS: int = int(getattr(SET, "recv_window_ms", 5000) or 5000)

_API_KEY: str = str(getattr(SET, "api_key", "") or "")
_API_SECRET: str = str(getattr(SET, "api_secret", "") or "")

_SESSION: requests.Session = requests.Session()
_SESSION.headers.update(
    {
        "Accept": "application/json",
        "User-Agent": "auto-trader/binance-usdtm",
    }
)

# Server time offset in milliseconds: local_ms + offset_ms = server_ms
_SERVER_TIME_OFFSET_MS: int = 0
_TIME_SYNCED: bool = False
_LAST_TIME_SYNC_AT: float = 0.0
_TIME_SYNC_TTL_SEC: int = 60  # resync at most once per minute (safe for -1021)


# -----------------------------------------------------------------------------
# Internal helpers
# -----------------------------------------------------------------------------
def _normalize_symbol(symbol: str) -> str:
    """Normalize symbol to Binance format (e.g., BTCUSDT)."""
    s = str(symbol).replace("-", "").replace("/", "").upper().strip()
    if not s:
        raise ValueError("symbol is empty")
    return s


def _normalize_margin_type(value: Any) -> str:
    """Normalize marginType from Binance positionRisk to 'ISOLATED' or 'CROSSED'."""
    v = str(value or "").strip().upper()
    if not v:
        raise RuntimeError("positionRisk.marginType is missing/empty")

    # Binance positionRisk marginType commonly: 'isolated' or 'cross'
    if v in {"ISOLATED"}:
        return "ISOLATED"
    if v in {"CROSS", "CROSSED"}:
        return "CROSSED"

    raise RuntimeError(f"positionRisk.marginType unexpected value: {value!r}")


def _local_ts_ms() -> int:
    """Local timestamp in milliseconds."""
    return int(time.time() * 1000)


def _ts_ms() -> int:
    """Timestamp in milliseconds adjusted by server time offset."""
    return _local_ts_ms() + _SERVER_TIME_OFFSET_MS


def _ensure_credentials() -> None:
    """Ensure API key/secret exist."""
    if not _API_KEY or not _API_SECRET:
        raise RuntimeError("BINANCE API key/secret missing (settings.api_key/api_secret)")


def _coerce_params(params: Optional[Mapping[str, Any]]) -> List[Tuple[str, str]]:
    """Convert params to a sorted list of (key, value) strings for query signing."""
    items: List[Tuple[str, str]] = []
    if not params:
        return items

    for k, v in params.items():
        if v is None:
            continue

        key = str(k)
        if isinstance(v, bool):
            val = "true" if v else "false"
        else:
            val = str(v)

        items.append((key, val))

    # Binance signature uses the raw query string; make it stable by sorting keys.
    items.sort(key=lambda kv: kv[0])
    return items


def _encode_query(params: Optional[Mapping[str, Any]]) -> str:
    """URL-encode parameters deterministically."""
    items = _coerce_params(params)
    if not items:
        return ""
    return urlencode(items, doseq=True)


def _sign(query_string: str) -> str:
    """HMAC SHA256 signature for the given query string."""
    _ensure_credentials()
    return hmac.new(
        _API_SECRET.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def sync_server_time() -> int:
    """Sync Binance server time and return offset in milliseconds.

    Calls GET /fapi/v1/time, computes server-local offset using midpoint
    to reduce RTT skew.

    Raises:
        RuntimeError: network/HTTP/JSON errors.
    """
    global _SERVER_TIME_OFFSET_MS, _TIME_SYNCED, _LAST_TIME_SYNC_AT

    # midpoint method to reduce RTT skew
    t0 = time.time()
    data = req("GET", "/fapi/v1/time", private=False)
    t1 = time.time()

    if not isinstance(data, dict) or "serverTime" not in data:
        raise RuntimeError("GET /fapi/v1/time -> unexpected response shape")

    try:
        server_ms = int(data["serverTime"])
    except (TypeError, ValueError):
        raise RuntimeError("GET /fapi/v1/time -> invalid serverTime")

    local_mid_ms = int(((t0 + t1) / 2.0) * 1000)
    offset_ms = server_ms - local_mid_ms

    _SERVER_TIME_OFFSET_MS = offset_ms
    _TIME_SYNCED = True
    _LAST_TIME_SYNC_AT = time.time()

    logger.info("Binance server time synced (offset_ms=%s)", offset_ms)
    return offset_ms


def sync_server_time_offset() -> int:
    """Backward-compatible alias for sync_server_time()."""
    return sync_server_time()


def _ensure_time_sync(force: bool = False) -> None:
    """Ensure server time is synced (raises on failure)."""
    global _TIME_SYNCED
    if force or (not _TIME_SYNCED) or (time.time() - _LAST_TIME_SYNC_AT > _TIME_SYNC_TTL_SEC):
        sync_server_time()
        if not _TIME_SYNCED:
            raise RuntimeError("server time sync failed")


def req(
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,  # compatibility placeholder (unused)
    *,
    private: bool = True,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
) -> Any:
    """Common REST request function with public/private support.

    Public:
        - No signature, no API key required.

    Private:
        - Adds timestamp/recvWindow, signs query, sends X-MBX-APIKEY header.

    Args:
        method: HTTP method (GET/POST/DELETE/PUT).
        path: REST path (must start with /fapi/).
        params: Query parameters (Binance expects form-style params).
        body: Unused. Kept for compatibility with older call sites.
        private: True for signed endpoints.
        timeout_sec: Request timeout in seconds.

    Returns:
        Parsed JSON (dict/list).

    Raises:
        ValueError: invalid inputs.
        RuntimeError: network/HTTP/JSON/Binance error codes.
    """
    _ = body  # explicitly unused (kept for signature compatibility)

    m = str(method).upper().strip()
    if m not in {"GET", "POST", "PUT", "DELETE"}:
        raise ValueError(f"unsupported http method: {method!r}")

    p = str(path).strip()
    if not p.startswith("/"):
        p = "/" + p

    # STRICT: Binance USDT-M Futures only
    if not p.startswith("/fapi/"):
        raise ValueError(f"only /fapi/* endpoints are allowed: {p}")

    if timeout_sec <= 0:
        raise ValueError("timeout_sec must be positive")

    q_params: Dict[str, Any] = dict(params or {})

    headers: Dict[str, str] = {}
    if private:
        _ensure_credentials()
        _ensure_time_sync()

        q_params.setdefault("recvWindow", RECV_WINDOW_MS)
        q_params["timestamp"] = _ts_ms()

        query = _encode_query(q_params)
        sig = _sign(query)
        url = f"{BASE_URL}{p}?{query}&signature={sig}"
        headers["X-MBX-APIKEY"] = _API_KEY
    else:
        query = _encode_query(q_params)
        url = f"{BASE_URL}{p}" + (f"?{query}" if query else "")

    def _do() -> requests.Response:
        return _SESSION.request(m, url, headers=headers or None, timeout=timeout_sec)

    try:
        resp = execute_with_retry(_do)
    except requests.RequestException as e:
        raise RuntimeError(f"{m} {p} -> request failed: {e.__class__.__name__}") from None

    if resp.status_code != 200:
        # Sanitize: do not include URL (contains signature)
        try:
            err = resp.json()
        except Exception:
            err = {"message": (resp.text or "").strip()[:300]}

        if isinstance(err, dict):
            code = err.get("code")
            msg = err.get("msg") or err.get("message")
            raise RuntimeError(f"{m} {p} -> HTTP {resp.status_code}, code={code}, msg={msg}")
        raise RuntimeError(f"{m} {p} -> HTTP {resp.status_code}")

    try:
        data = resp.json()
    except Exception as e:
        raise RuntimeError(f"{m} {p} -> invalid json: {e.__class__.__name__}") from None

    # Some endpoints may return 200 with error-style payload
    if isinstance(data, dict) and isinstance(data.get("code"), int) and int(data["code"]) < 0:
        raise RuntimeError(f"{m} {p} -> binance code={data.get('code')}, msg={data.get('msg')}")

    return data


def assert_one_way_mode() -> None:
    """
    Ensure account is in One-way mode.
    Raises RuntimeError if account is in Hedge mode.
    """
    data = req("GET", "/fapi/v1/positionSide/dual", private=True)

    if not isinstance(data, dict):
        raise RuntimeError("positionSide/dual unexpected response")

    if data.get("dualSidePosition") is True:
        raise RuntimeError("Account is in HEDGE mode. Switch Binance Futures to ONE-WAY mode.")


# -----------------------------------------------------------------------------
# Public endpoints
# -----------------------------------------------------------------------------
def get_exchange_info(symbol: Optional[str] = None) -> Dict[str, Any]:
    """Get futures exchange info (symbol filters, tick/step, etc.)."""
    params: Dict[str, Any] = {}
    if symbol:
        params["symbol"] = _normalize_symbol(symbol)

    data = req("GET", "/fapi/v1/exchangeInfo", params or None, private=False)

    if not isinstance(data, dict):
        raise RuntimeError("GET /fapi/v1/exchangeInfo -> unexpected response shape")
    return data


# -----------------------------------------------------------------------------
# Private endpoints (account/positions/orders/settings)
# -----------------------------------------------------------------------------
def get_balance() -> List[Dict[str, Any]]:
    """Get futures account balance (list of assets)."""
    data = req("GET", "/fapi/v2/balance", {}, private=True)
    if not isinstance(data, list):
        raise RuntimeError("GET /fapi/v2/balance -> unexpected response shape")
    out: List[Dict[str, Any]] = []
    for row in data:
        if isinstance(row, dict):
            out.append(row)
    return out


def get_balance_detail(asset: str = "USDT") -> Dict[str, Any]:
    """Get balance row for a specific asset (default: USDT)."""
    asset_u = str(asset).upper().strip()
    if not asset_u:
        raise ValueError("asset is empty")

    rows = get_balance()
    for row in rows:
        if str(row.get("asset", "")).upper() == asset_u:
            return row
    raise RuntimeError(f"balance row not found for asset={asset_u}")


def get_available_usdt() -> float:
    """Get available USDT balance as float (availableBalance)."""
    row = get_balance_detail("USDT")
    try:
        return float(row.get("availableBalance"))
    except (TypeError, ValueError):
        raise RuntimeError("USDT availableBalance parse failed")


def _fetch_position_risk_raw(symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    """Fetch raw positionRisk list (may include zero positions)."""
    params: Dict[str, Any] = {}
    if symbol:
        params["symbol"] = _normalize_symbol(symbol)

    data = req("GET", "/fapi/v2/positionRisk", params or None, private=True)
    if isinstance(data, dict):
        if "raw" in data and isinstance(data["raw"], list):
            data = data["raw"]

    if not isinstance(data, list):
        raise RuntimeError("GET /fapi/v2/positionRisk -> unexpected response shape")

    out: List[Dict[str, Any]] = []
    for row in data:
        if isinstance(row, dict):
            out.append(row)
    return out


def fetch_open_positions(symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    """Fetch open positions (positionAmt != 0) for a symbol or all symbols."""
    rows = _fetch_position_risk_raw(symbol)
    open_rows: List[Dict[str, Any]] = []
    for row in rows:
        try:
            amt = float(row.get("positionAmt", 0.0) or 0.0)
        except (TypeError, ValueError):
            raise RuntimeError("positionAmt parse failed")
        if amt != 0.0:
            open_rows.append(row)
    return open_rows


def get_position(symbol: str) -> Dict[str, Any]:
    """Get positionRisk for a specific symbol."""
    s = _normalize_symbol(symbol)
    rows = _fetch_position_risk_raw(s)
    if not rows:
        raise RuntimeError(f"position not found for symbol={s}")

    # One-way mode only: Binance returns a single row per symbol.
    return rows[0]


def get_open_orders(symbol: str) -> List[Dict[str, Any]]:
    """Get open orders for a symbol."""
    s = _normalize_symbol(symbol)
    data = req("GET", "/fapi/v1/openOrders", {"symbol": s}, private=True)
    if not isinstance(data, list):
        raise RuntimeError("GET /fapi/v1/openOrders -> unexpected response shape")
    out: List[Dict[str, Any]] = []
    for row in data:
        if isinstance(row, dict):
            out.append(row)
    return out


def fetch_open_orders(symbol: str) -> List[Dict[str, Any]]:
    """Backward-compatible alias for get_open_orders()."""
    return get_open_orders(symbol)


def cancel_order(symbol: str, order_id: int | str) -> Dict[str, Any]:
    """Cancel an order by orderId."""
    s = _normalize_symbol(symbol)
    try:
        oid = int(order_id)
    except (TypeError, ValueError):
        raise ValueError(f"invalid order_id: {order_id!r}")

    data = req("DELETE", "/fapi/v1/order", {"symbol": s, "orderId": oid}, private=True)
    if not isinstance(data, dict):
        raise RuntimeError("DELETE /fapi/v1/order -> unexpected response shape")
    return data


def set_leverage(symbol: str, leverage: int) -> Dict[str, Any]:
    """Set leverage for a symbol."""
    s = _normalize_symbol(symbol)
    if not isinstance(leverage, int):
        raise ValueError("leverage must be int")
    if leverage <= 0:
        raise ValueError("leverage must be positive")

    data = req("POST", "/fapi/v1/leverage", {"symbol": s, "leverage": leverage}, private=True)
    if not isinstance(data, dict):
        raise RuntimeError("POST /fapi/v1/leverage -> unexpected response shape")
    return data


def set_margin_mode(symbol: str, mode: str) -> Dict[str, Any]:
    """Set margin mode for a symbol (ISOLATED or CROSSED)."""
    s = _normalize_symbol(symbol)
    mode_u = str(mode).upper().strip()

    if mode_u in {"CROSS", "CROSSED"}:
        mode_u = "CROSSED"
    if mode_u not in {"ISOLATED", "CROSSED"}:
        raise ValueError("mode must be 'ISOLATED' or 'CROSSED'")

    data = req("POST", "/fapi/v1/marginType", {"symbol": s, "marginType": mode_u}, private=True)
    if not isinstance(data, dict):
        raise RuntimeError("POST /fapi/v1/marginType -> unexpected response shape")
    return data


def set_leverage_and_mode(symbol: str, leverage: int, isolated: bool = True) -> None:
    """Backward-compatible helper to set leverage and margin mode (state-verified)."""
    if not isinstance(leverage, int):
        raise ValueError("leverage must be int")
    if leverage <= 0:
        raise ValueError("leverage must be positive")

    s = _normalize_symbol(symbol)
    current = get_position(s)

    if "leverage" not in current:
        raise RuntimeError("positionRisk.leverage is missing")
    try:
        current_leverage = int(current["leverage"])
    except (TypeError, ValueError):
        raise RuntimeError(f"positionRisk.leverage parse failed: {current.get('leverage')!r}")

    current_margin = _normalize_margin_type(current.get("marginType"))

    target_leverage = leverage
    target_margin = "ISOLATED" if isolated else "CROSSED"

    if current_leverage != target_leverage:
        set_leverage(s, target_leverage)
    else:
        logger.info("leverage already set: %s", target_leverage)

    if current_margin != target_margin:
        set_margin_mode(s, target_margin)
    else:
        logger.info("marginType already set: %s", target_margin)


__all__ = [
    "req",
    "sync_server_time",
    "sync_server_time_offset",
    "assert_one_way_mode",
    "get_exchange_info",
    "get_balance",
    "get_balance_detail",
    "get_available_usdt",
    "fetch_open_positions",
    "get_position",
    "get_open_orders",
    "fetch_open_orders",
    "cancel_order",
    "set_leverage",
    "set_margin_mode",
    "set_leverage_and_mode",
]