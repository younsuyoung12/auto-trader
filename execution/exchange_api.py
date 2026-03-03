"""
========================================================
FILE: execution/exchange_api.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
Design principles (Binance USDT-M Futures REST Adapter)
--------------------------------------------------------
- Binance Futures endpoints only:
  - Trading/Account: /fapi/*
  - Public market data (microstructure): /futures/data/*
- Transport/Query layer only: signing, HTTP requests, account/position/order 조회 및
  레버리지/마진 모드 설정만 제공한다.
- 주문 실행(진입/청산/TP/SL 생성/슬리피지 계산)은 order_executor.py가 담당한다.
- Fallback 금지: 오류/누락/비정상 응답은 즉시 예외(RuntimeError/ValueError).
- 민감 정보(API key/secret/signature)는 로그/예외 메시지에 포함하지 않는다.

One-way mode only.
Hedge mode is NOT supported.
positionSide must always be BOTH in order layer.

PATCH NOTES — 2026-03-03 (TRADE-GRADE)
--------------------------------------------------------
- Microstructure 데이터 계층 확장(STRICT, no fallback)
  - /fapi/* 외에 /futures/data/* (public) 엔드포인트를 허용한다.
  - Funding/OI/Long-Short Ratio 계열 조회 함수 추가:
    * get_premium_index(symbol)                         : /fapi/v1/premiumIndex (public)
    * get_current_funding_rate(symbol)                  : premiumIndex.lastFundingRate (float)
    * get_funding_rate_history(symbol, ...)             : /fapi/v1/fundingRate (public)
    * get_open_interest(symbol)                         : /fapi/v1/openInterest (public)
    * get_open_interest_hist(symbol, period, ...)       : /futures/data/openInterestHist (public)
    * get_global_long_short_account_ratio(symbol, ...)  : /futures/data/globalLongShortAccountRatio (public)
    * get_top_long_short_account_ratio(symbol, ...)     : /futures/data/topLongShortAccountRatio (public)
    * get_top_long_short_position_ratio(symbol, ...)    : /futures/data/topLongShortPositionRatio (public)
- STRICT 강화
  - settings 값(기본 URL/timeout/recvWindow) 누락/비정상 시 즉시 실패(Fail-Fast)

PATCH NOTES — 2026-03-02 (PATCH)
--------------------------------------------------------
- (9점대 기반) 주문/체결/복구를 위한 조회 함수 추가(STRICT, no fallback)
  - get_order(symbol, order_id): /fapi/v1/order by orderId
  - get_order_by_client_id(symbol, client_order_id): /fapi/v1/order by origClientOrderId
  - get_user_trades(symbol, order_id=None, start_time_ms=None, end_time_ms=None, limit=1000)
========================================================
"""

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

# -----------------------------------------------------------------------------#
# Global settings / session (STRICT: fail-fast)
# -----------------------------------------------------------------------------#
SET = load_settings()


def _require_setting_str(name: str) -> str:
    try:
        v = getattr(SET, name)
    except AttributeError as e:
        raise RuntimeError(f"settings.{name} is missing (STRICT)") from e
    s = str(v).strip()
    if not s:
        raise RuntimeError(f"settings.{name} is empty (STRICT)")
    return s


def _require_setting_int(name: str) -> int:
    try:
        v = getattr(SET, name)
    except AttributeError as e:
        raise RuntimeError(f"settings.{name} is missing (STRICT)") from e
    try:
        iv = int(v)
    except Exception as e:
        raise RuntimeError(f"settings.{name} must be int (got={v!r})") from e
    if iv <= 0:
        raise RuntimeError(f"settings.{name} must be positive (got={iv})")
    return iv


BASE_URL: str = _require_setting_str("binance_futures_base")
DEFAULT_TIMEOUT_SEC: int = _require_setting_int("request_timeout_sec")
RECV_WINDOW_MS: int = _require_setting_int("recv_window_ms")

_API_KEY: str = _require_setting_str("api_key")
_API_SECRET: str = _require_setting_str("api_secret")

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

# Allowed endpoints
_ALLOWED_PUBLIC_PREFIXES: Tuple[str, ...] = ("/fapi/", "/futures/data/")
_ALLOWED_PRIVATE_PREFIX: str = "/fapi/"


# -----------------------------------------------------------------------------#
# Internal helpers (STRICT)
# -----------------------------------------------------------------------------#
def _normalize_symbol(symbol: str) -> str:
    """Normalize symbol to Binance format (e.g., BTCUSDT)."""
    s = str(symbol).replace("-", "").replace("/", "").upper().strip()
    if not s:
        raise ValueError("symbol is empty")
    return s


def _normalize_margin_type(value: Any) -> str:
    """Normalize marginType from Binance positionRisk to 'ISOLATED' or 'CROSSED'."""
    v = str(value).strip().upper()
    if not v:
        raise RuntimeError("positionRisk.marginType is missing/empty")

    if v == "ISOLATED":
        return "ISOLATED"
    if v in {"CROSS", "CROSSED"}:
        return "CROSSED"

    raise RuntimeError(f"positionRisk.marginType unexpected value: {value!r}")


def _local_ts_ms() -> int:
    return int(time.time() * 1000)


def _ts_ms() -> int:
    return _local_ts_ms() + _SERVER_TIME_OFFSET_MS


def _coerce_params(params: Optional[Mapping[str, Any]]) -> List[Tuple[str, str]]:
    items: List[Tuple[str, str]] = []
    if params is None:
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

    items.sort(key=lambda kv: kv[0])
    return items


def _encode_query(params: Optional[Mapping[str, Any]]) -> str:
    items = _coerce_params(params)
    if not items:
        return ""
    return urlencode(items, doseq=True)


def _sign(query_string: str) -> str:
    return hmac.new(
        _API_SECRET.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _require_int(name: str, v: Any) -> int:
    try:
        iv = int(v)
    except Exception as e:
        raise ValueError(f"{name} must be int (got={v!r})") from e
    return iv


def _require_float(name: str, v: Any) -> float:
    try:
        fv = float(v)
    except Exception as e:
        raise ValueError(f"{name} must be float (got={v!r})") from e
    if fv != fv or fv in (float("inf"), float("-inf")):
        raise ValueError(f"{name} must be finite (got={fv})")
    return fv


def _validate_ms_range(start_ms: Optional[int], end_ms: Optional[int]) -> None:
    if start_ms is not None:
        if not isinstance(start_ms, int) or start_ms <= 0:
            raise ValueError("start_time_ms must be int > 0")
    if end_ms is not None:
        if not isinstance(end_ms, int) or end_ms <= 0:
            raise ValueError("end_time_ms must be int > 0")
    if start_ms is not None and end_ms is not None and end_ms < start_ms:
        raise ValueError("end_time_ms must be >= start_time_ms")


def _validate_period(period: str) -> str:
    p = str(period).strip()
    if not p:
        raise ValueError("period is empty")
    # Binance futures data endpoints accepted values (common)
    allowed = {"5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"}
    if p not in allowed:
        raise ValueError(f"period must be one of {sorted(allowed)} (got={p!r})")
    return p


def sync_server_time() -> int:
    """Sync Binance server time and return offset in milliseconds.

    Calls GET /fapi/v1/time, computes server-local offset using midpoint
    to reduce RTT skew.
    """
    global _SERVER_TIME_OFFSET_MS, _TIME_SYNCED, _LAST_TIME_SYNC_AT

    t0 = time.time()
    data = req("GET", "/fapi/v1/time", private=False)
    t1 = time.time()

    if not isinstance(data, dict) or "serverTime" not in data:
        raise RuntimeError("GET /fapi/v1/time -> unexpected response shape")

    server_ms = _require_int("serverTime", data["serverTime"])
    local_mid_ms = int(((t0 + t1) / 2.0) * 1000)
    offset_ms = server_ms - local_mid_ms

    _SERVER_TIME_OFFSET_MS = offset_ms
    _TIME_SYNCED = True
    _LAST_TIME_SYNC_AT = time.time()

    logger.info("Binance server time synced (offset_ms=%s)", offset_ms)
    return offset_ms


def sync_server_time_offset() -> int:
    return sync_server_time()


def _ensure_time_sync(force: bool = False) -> None:
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

    STRICT endpoint policy:
        - private=True: only /fapi/*
        - private=False: /fapi/* and /futures/data/* allowed
    """
    _ = body  # explicitly unused

    m = str(method).upper().strip()
    if m not in {"GET", "POST", "PUT", "DELETE"}:
        raise ValueError(f"unsupported http method: {method!r}")

    p = str(path).strip()
    if not p.startswith("/"):
        p = "/" + p

    if private:
        if not p.startswith(_ALLOWED_PRIVATE_PREFIX):
            raise ValueError(f"private endpoints must be /fapi/* (got={p})")
    else:
        if not any(p.startswith(pref) for pref in _ALLOWED_PUBLIC_PREFIXES):
            raise ValueError(f"public endpoints must be /fapi/* or /futures/data/* (got={p})")

    if not isinstance(timeout_sec, int) or timeout_sec <= 0:
        raise ValueError("timeout_sec must be positive int")

    q_params: Dict[str, Any] = dict(params or {})
    headers: Dict[str, str] = {}

    if private:
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
        # Sanitize: do not include URL (contains signature for private)
        raise RuntimeError(f"{m} {p} -> request failed: {e.__class__.__name__}") from None

    if resp.status_code != 200:
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

    if isinstance(data, dict) and isinstance(data.get("code"), int) and int(data["code"]) < 0:
        raise RuntimeError(f"{m} {p} -> binance code={data.get('code')}, msg={data.get('msg')}")

    return data


def assert_one_way_mode() -> None:
    """Ensure account is in One-way mode. Raises RuntimeError if account is in Hedge mode."""
    data = req("GET", "/fapi/v1/positionSide/dual", private=True)
    if not isinstance(data, dict):
        raise RuntimeError("positionSide/dual unexpected response")
    if data.get("dualSidePosition") is True:
        raise RuntimeError("Account is in HEDGE mode. Switch Binance Futures to ONE-WAY mode.")


# -----------------------------------------------------------------------------#
# Public endpoints (exchange / microstructure)
# -----------------------------------------------------------------------------#
def get_exchange_info(symbol: Optional[str] = None) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    if symbol is not None:
        params["symbol"] = _normalize_symbol(symbol)

    data = req("GET", "/fapi/v1/exchangeInfo", params or None, private=False)
    if not isinstance(data, dict):
        raise RuntimeError("GET /fapi/v1/exchangeInfo -> unexpected response shape")
    return data


def get_premium_index(symbol: str) -> Dict[str, Any]:
    """Public: premium index (mark/index + funding info) via /fapi/v1/premiumIndex."""
    s = _normalize_symbol(symbol)
    data = req("GET", "/fapi/v1/premiumIndex", {"symbol": s}, private=False)
    if not isinstance(data, dict) or not data:
        raise RuntimeError("GET /fapi/v1/premiumIndex -> unexpected response shape")
    return data


def get_current_funding_rate(symbol: str) -> float:
    """Public: current funding rate from premiumIndex.lastFundingRate."""
    d = get_premium_index(symbol)
    if "lastFundingRate" not in d:
        raise RuntimeError("premiumIndex.lastFundingRate missing")
    return _require_float("lastFundingRate", d["lastFundingRate"])


def get_funding_rate_history(
    symbol: str,
    *,
    start_time_ms: Optional[int] = None,
    end_time_ms: Optional[int] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """Public: funding rate history via /fapi/v1/fundingRate."""
    s = _normalize_symbol(symbol)
    if not isinstance(limit, int) or limit <= 0 or limit > 1000:
        raise ValueError("limit must be int in [1, 1000]")
    _validate_ms_range(start_time_ms, end_time_ms)

    params: Dict[str, Any] = {"symbol": s, "limit": limit}
    if start_time_ms is not None:
        params["startTime"] = start_time_ms
    if end_time_ms is not None:
        params["endTime"] = end_time_ms

    data = req("GET", "/fapi/v1/fundingRate", params, private=False)
    if not isinstance(data, list):
        raise RuntimeError("GET /fapi/v1/fundingRate -> unexpected response shape")
    out: List[Dict[str, Any]] = []
    for i, row in enumerate(data):
        if not isinstance(row, dict):
            raise RuntimeError(f"fundingRate[{i}] is not dict (STRICT)")
        out.append(row)
    return out


def get_open_interest(symbol: str) -> float:
    """Public: current open interest via /fapi/v1/openInterest."""
    s = _normalize_symbol(symbol)
    data = req("GET", "/fapi/v1/openInterest", {"symbol": s}, private=False)
    if not isinstance(data, dict) or "openInterest" not in data:
        raise RuntimeError("GET /fapi/v1/openInterest -> unexpected response shape")
    return _require_float("openInterest", data["openInterest"])


def get_open_interest_hist(
    symbol: str,
    *,
    period: str,
    start_time_ms: Optional[int] = None,
    end_time_ms: Optional[int] = None,
    limit: int = 30,
) -> List[Dict[str, Any]]:
    """Public: open interest history via /futures/data/openInterestHist."""
    s = _normalize_symbol(symbol)
    p = _validate_period(period)
    if not isinstance(limit, int) or limit <= 0 or limit > 500:
        raise ValueError("limit must be int in [1, 500]")
    _validate_ms_range(start_time_ms, end_time_ms)

    params: Dict[str, Any] = {"symbol": s, "period": p, "limit": limit}
    if start_time_ms is not None:
        params["startTime"] = start_time_ms
    if end_time_ms is not None:
        params["endTime"] = end_time_ms

    data = req("GET", "/futures/data/openInterestHist", params, private=False)
    if not isinstance(data, list):
        raise RuntimeError("GET /futures/data/openInterestHist -> unexpected response shape")
    out: List[Dict[str, Any]] = []
    for i, row in enumerate(data):
        if not isinstance(row, dict):
            raise RuntimeError(f"openInterestHist[{i}] is not dict (STRICT)")
        out.append(row)
    return out


def get_global_long_short_account_ratio(
    symbol: str,
    *,
    period: str,
    start_time_ms: Optional[int] = None,
    end_time_ms: Optional[int] = None,
    limit: int = 30,
) -> List[Dict[str, Any]]:
    """Public: global long/short account ratio via /futures/data/globalLongShortAccountRatio."""
    s = _normalize_symbol(symbol)
    p = _validate_period(period)
    if not isinstance(limit, int) or limit <= 0 or limit > 500:
        raise ValueError("limit must be int in [1, 500]")
    _validate_ms_range(start_time_ms, end_time_ms)

    params: Dict[str, Any] = {"symbol": s, "period": p, "limit": limit}
    if start_time_ms is not None:
        params["startTime"] = start_time_ms
    if end_time_ms is not None:
        params["endTime"] = end_time_ms

    data = req("GET", "/futures/data/globalLongShortAccountRatio", params, private=False)
    if not isinstance(data, list):
        raise RuntimeError("GET /futures/data/globalLongShortAccountRatio -> unexpected response shape")
    out: List[Dict[str, Any]] = []
    for i, row in enumerate(data):
        if not isinstance(row, dict):
            raise RuntimeError(f"globalLongShortAccountRatio[{i}] is not dict (STRICT)")
        out.append(row)
    return out


def get_top_long_short_account_ratio(
    symbol: str,
    *,
    period: str,
    start_time_ms: Optional[int] = None,
    end_time_ms: Optional[int] = None,
    limit: int = 30,
) -> List[Dict[str, Any]]:
    """Public: top long/short account ratio via /futures/data/topLongShortAccountRatio."""
    s = _normalize_symbol(symbol)
    p = _validate_period(period)
    if not isinstance(limit, int) or limit <= 0 or limit > 500:
        raise ValueError("limit must be int in [1, 500]")
    _validate_ms_range(start_time_ms, end_time_ms)

    params: Dict[str, Any] = {"symbol": s, "period": p, "limit": limit}
    if start_time_ms is not None:
        params["startTime"] = start_time_ms
    if end_time_ms is not None:
        params["endTime"] = end_time_ms

    data = req("GET", "/futures/data/topLongShortAccountRatio", params, private=False)
    if not isinstance(data, list):
        raise RuntimeError("GET /futures/data/topLongShortAccountRatio -> unexpected response shape")
    out: List[Dict[str, Any]] = []
    for i, row in enumerate(data):
        if not isinstance(row, dict):
            raise RuntimeError(f"topLongShortAccountRatio[{i}] is not dict (STRICT)")
        out.append(row)
    return out


def get_top_long_short_position_ratio(
    symbol: str,
    *,
    period: str,
    start_time_ms: Optional[int] = None,
    end_time_ms: Optional[int] = None,
    limit: int = 30,
) -> List[Dict[str, Any]]:
    """Public: top long/short position ratio via /futures/data/topLongShortPositionRatio."""
    s = _normalize_symbol(symbol)
    p = _validate_period(period)
    if not isinstance(limit, int) or limit <= 0 or limit > 500:
        raise ValueError("limit must be int in [1, 500]")
    _validate_ms_range(start_time_ms, end_time_ms)

    params: Dict[str, Any] = {"symbol": s, "period": p, "limit": limit}
    if start_time_ms is not None:
        params["startTime"] = start_time_ms
    if end_time_ms is not None:
        params["endTime"] = end_time_ms

    data = req("GET", "/futures/data/topLongShortPositionRatio", params, private=False)
    if not isinstance(data, list):
        raise RuntimeError("GET /futures/data/topLongShortPositionRatio -> unexpected response shape")
    out: List[Dict[str, Any]] = []
    for i, row in enumerate(data):
        if not isinstance(row, dict):
            raise RuntimeError(f"topLongShortPositionRatio[{i}] is not dict (STRICT)")
        out.append(row)
    return out


# -----------------------------------------------------------------------------#
# Private endpoints (account/positions/orders/settings)
# -----------------------------------------------------------------------------#
def get_balance() -> List[Dict[str, Any]]:
    data = req("GET", "/fapi/v2/balance", {}, private=True)
    if not isinstance(data, list):
        raise RuntimeError("GET /fapi/v2/balance -> unexpected response shape")
    out: List[Dict[str, Any]] = []
    for i, row in enumerate(data):
        if not isinstance(row, dict):
            raise RuntimeError(f"balance[{i}] is not dict (STRICT)")
        out.append(row)
    return out


def get_balance_detail(asset: str = "USDT") -> Dict[str, Any]:
    asset_u = str(asset).upper().strip()
    if not asset_u:
        raise ValueError("asset is empty")

    rows = get_balance()
    for row in rows:
        if str(row.get("asset", "")).upper() == asset_u:
            return row
    raise RuntimeError(f"balance row not found for asset={asset_u}")


def get_available_usdt() -> float:
    row = get_balance_detail("USDT")
    if "availableBalance" not in row:
        raise RuntimeError("USDT availableBalance missing")
    return _require_float("availableBalance", row["availableBalance"])


def _fetch_position_risk_raw(symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if symbol is not None:
        params["symbol"] = _normalize_symbol(symbol)

    data = req("GET", "/fapi/v2/positionRisk", params or None, private=True)
    if isinstance(data, dict) and "raw" in data:
        raw = data.get("raw")
        if not isinstance(raw, list):
            raise RuntimeError("positionRisk.raw is not list (STRICT)")
        data = raw

    if not isinstance(data, list):
        raise RuntimeError("GET /fapi/v2/positionRisk -> unexpected response shape")

    out: List[Dict[str, Any]] = []
    for i, row in enumerate(data):
        if not isinstance(row, dict):
            raise RuntimeError(f"positionRisk[{i}] is not dict (STRICT)")
        out.append(row)
    return out


def fetch_open_positions(symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    rows = _fetch_position_risk_raw(symbol)
    open_rows: List[Dict[str, Any]] = []
    for row in rows:
        if "positionAmt" not in row:
            raise RuntimeError("positionAmt missing in positionRisk row (STRICT)")
        amt = _require_float("positionAmt", row["positionAmt"])
        if amt != 0.0:
            open_rows.append(row)
    return open_rows


def get_position(symbol: str) -> Dict[str, Any]:
    s = _normalize_symbol(symbol)
    rows = _fetch_position_risk_raw(s)
    if not rows:
        raise RuntimeError(f"position not found for symbol={s}")
    return rows[0]


def get_open_orders(symbol: str) -> List[Dict[str, Any]]:
    s = _normalize_symbol(symbol)
    data = req("GET", "/fapi/v1/openOrders", {"symbol": s}, private=True)
    if not isinstance(data, list):
        raise RuntimeError("GET /fapi/v1/openOrders -> unexpected response shape")
    out: List[Dict[str, Any]] = []
    for i, row in enumerate(data):
        if not isinstance(row, dict):
            raise RuntimeError(f"openOrders[{i}] is not dict (STRICT)")
        out.append(row)
    return out


def fetch_open_orders(symbol: str) -> List[Dict[str, Any]]:
    return get_open_orders(symbol)


def get_order(symbol: str, order_id: int | str) -> Dict[str, Any]:
    s = _normalize_symbol(symbol)
    oid = _require_int("order_id", order_id)

    data = req("GET", "/fapi/v1/order", {"symbol": s, "orderId": oid}, private=True)
    if not isinstance(data, dict):
        raise RuntimeError("GET /fapi/v1/order -> unexpected response shape")
    return data


def get_order_by_client_id(symbol: str, client_order_id: str) -> Dict[str, Any]:
    s = _normalize_symbol(symbol)
    cid = str(client_order_id).strip()
    if not cid:
        raise ValueError("client_order_id is empty")

    data = req("GET", "/fapi/v1/order", {"symbol": s, "origClientOrderId": cid}, private=True)
    if not isinstance(data, dict):
        raise RuntimeError("GET /fapi/v1/order(by client id) -> unexpected response shape")
    return data


def get_user_trades(
    symbol: str,
    *,
    order_id: Optional[int | str] = None,
    start_time_ms: Optional[int] = None,
    end_time_ms: Optional[int] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    s = _normalize_symbol(symbol)
    if not isinstance(limit, int) or limit <= 0 or limit > 1000:
        raise ValueError("limit must be int in [1, 1000]")

    params: Dict[str, Any] = {"symbol": s, "limit": limit}

    if order_id is not None:
        params["orderId"] = _require_int("order_id", order_id)

    _validate_ms_range(start_time_ms, end_time_ms)
    if start_time_ms is not None:
        params["startTime"] = start_time_ms
    if end_time_ms is not None:
        params["endTime"] = end_time_ms

    data = req("GET", "/fapi/v1/userTrades", params, private=True)
    if not isinstance(data, list):
        raise RuntimeError("GET /fapi/v1/userTrades -> unexpected response shape")
    out: List[Dict[str, Any]] = []
    for i, row in enumerate(data):
        if not isinstance(row, dict):
            raise RuntimeError(f"userTrades[{i}] is not dict (STRICT)")
        out.append(row)
    return out


def cancel_order(symbol: str, order_id: int | str) -> Dict[str, Any]:
    s = _normalize_symbol(symbol)
    oid = _require_int("order_id", order_id)

    data = req("DELETE", "/fapi/v1/order", {"symbol": s, "orderId": oid}, private=True)
    if not isinstance(data, dict):
        raise RuntimeError("DELETE /fapi/v1/order -> unexpected response shape")
    return data


def set_leverage(symbol: str, leverage: int) -> Dict[str, Any]:
    s = _normalize_symbol(symbol)
    if not isinstance(leverage, int) or leverage <= 0:
        raise ValueError("leverage must be positive int")

    data = req("POST", "/fapi/v1/leverage", {"symbol": s, "leverage": leverage}, private=True)
    if not isinstance(data, dict):
        raise RuntimeError("POST /fapi/v1/leverage -> unexpected response shape")
    return data


def set_margin_mode(symbol: str, mode: str) -> Dict[str, Any]:
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
    if not isinstance(leverage, int) or leverage <= 0:
        raise ValueError("leverage must be positive int")

    s = _normalize_symbol(symbol)
    current = get_position(s)

    if "leverage" not in current:
        raise RuntimeError("positionRisk.leverage is missing")
    current_leverage = _require_int("positionRisk.leverage", current["leverage"])
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
    # core
    "req",
    "sync_server_time",
    "sync_server_time_offset",
    "assert_one_way_mode",
    # exchange
    "get_exchange_info",
    # microstructure (public)
    "get_premium_index",
    "get_current_funding_rate",
    "get_funding_rate_history",
    "get_open_interest",
    "get_open_interest_hist",
    "get_global_long_short_account_ratio",
    "get_top_long_short_account_ratio",
    "get_top_long_short_position_ratio",
    # account/positions/orders
    "get_balance",
    "get_balance_detail",
    "get_available_usdt",
    "fetch_open_positions",
    "get_position",
    "get_open_orders",
    "fetch_open_orders",
    "get_order",
    "get_order_by_client_id",
    "get_user_trades",
    "cancel_order",
    # settings
    "set_leverage",
    "set_margin_mode",
    "set_leverage_and_mode",
]