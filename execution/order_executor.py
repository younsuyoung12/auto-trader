"""
========================================================
FILE: execution/order_executor.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
Binance USDT-M Futures - Order Execution Layer (Production)

핵심:
- REST endpoints: /fapi/*
- Symbol filters enforced via /fapi/v1/exchangeInfo (tick/step/minQty)
- Idempotency via newClientOrderId (openOrders + order lookup + in-process lock)
- Timestamp error (-1021) recovery: sync_server_time() + single retry
- No print(); logging only

PATCH NOTES — 2026-03-02
--------------------------------------------------------
- TP/SL 주문 생성 시 orderId를 반환/상위로 전달(set_tp_sl -> (tp_order_id, sl_order_id))
- Entry/TP/SL에 결정적(deterministic) clientOrderId를 주입할 수 있도록 확장
  - entry_client_order_id(kw-only) 지원
  - settings.require_deterministic_client_order_id=True 인 경우 미지정 시 즉시 예외(폴백 금지)
- Trade 객체 생성 시(레거시 호환) Trade 시그니처에 존재하는 필드만 주입(호환성 유지)
  - entry_order_id/tp_order_id/sl_order_id/exchange_position_side/remaining_qty/realized_pnl_usdt 등
- STRICT 유지: 누락/불일치/실패 시 즉시 예외 또는 명시적 실패 처리(추정/폴백 금지)
========================================================
"""

from __future__ import annotations

import inspect
import logging
import math
import re
import threading
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Any, Dict, Optional, Tuple

from execution.exchange_api import (
    fetch_open_positions,
    get_exchange_info,
    get_open_orders,
    req,
    set_leverage,
    set_margin_mode,
    sync_server_time,
)
from execution.retry_policy import execute_with_retry  # type: ignore
from events.signals_logger import log_event
from settings import load_settings
from state.trader_state import Trade

# Decimal precision for money/qty calculations
getcontext().prec = 28

logger = logging.getLogger(__name__)

SET = load_settings()

# Cache TTLs
_FILTER_CACHE_TTL_SEC: int = 1800  # 30 minutes
_IDEMPOTENCY_CACHE_TTL_SEC: int = 6 * 3600  # 6 hours
_IDEMPOTENCY_INFLIGHT_WAIT_SEC: float = 2.0  # wait window for concurrent same clientOrderId


# ---------------------------------------------------------------------------
# Event side normalization (EventBus strict: LONG/SHORT/CLOSE)
# ---------------------------------------------------------------------------
def _event_side_from_open_side(open_side: str) -> str:
    s = str(open_side).upper().strip()
    if s == "BUY":
        return "LONG"
    if s == "SELL":
        return "SHORT"
    return "CLOSE"


def _event_side_close() -> str:
    return "CLOSE"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class SymbolFilters:
    """Parsed trading filters for a single symbol (Binance USDT-M Futures)."""

    symbol: str  # normalized, e.g., BTCUSDT
    tick_size: Decimal

    lot_step: Decimal
    lot_min_qty: Decimal

    market_step: Decimal
    market_min_qty: Decimal


# ---------------------------------------------------------------------------
# Caches / Locks
# ---------------------------------------------------------------------------
_FILTER_CACHE: Dict[str, Tuple[float, SymbolFilters]] = {}
_FILTER_LOCK = threading.Lock()

_SETTINGS_APPLIED: set[str] = set()
_SETTINGS_LOCK = threading.Lock()

# ClientOrderId concurrency control + recent cache
_CLIENT_ID_CACHE: Dict[str, float] = {}
_CLIENT_ID_INFLIGHT: set[str] = set()
_CLIENT_ID_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# Helpers: normalization / parsing / formatting
# ---------------------------------------------------------------------------
def _normalize_symbol(symbol: str) -> str:
    """Normalize symbol to Binance format (e.g., BTCUSDT)."""
    s = str(symbol).replace("-", "").replace("/", "").upper().strip()
    if not s:
        raise ValueError("symbol is empty")
    return s


def _normalize_side(side: str) -> str:
    """Normalize side to BUY/SELL (accepts LONG/SHORT)."""
    s = str(side).upper().strip()
    if s in {"BUY", "LONG"}:
        return "BUY"
    if s in {"SELL", "SHORT"}:
        return "SELL"
    raise ValueError(f"invalid side: {side!r}")


def _normalize_position_side(position_side: Optional[str]) -> str:
    """Normalize positionSide for Binance Futures."""
    if position_side is None:
        return "BOTH"
    ps = str(position_side).upper().strip()
    if ps in {"BOTH", "LONG", "SHORT"}:
        return ps
    raise ValueError(f"invalid position_side: {position_side!r}")


def _to_decimal(x: Any, *, name: str) -> Decimal:
    """Convert input to Decimal using str() to reduce float artifacts."""
    try:
        d = Decimal(str(x))
    except Exception:
        raise ValueError(f"invalid decimal for {name}") from None
    return d


def _d_to_str(d: Decimal) -> str:
    """Convert Decimal to plain string without scientific notation."""
    s = format(d, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    if s in {"", "-0"}:
        return "0"
    return s


def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    """Floor(value) to a given step using Decimal arithmetic."""
    if step <= 0:
        raise ValueError("step must be > 0")
    if value <= 0:
        raise ValueError("value must be > 0")
    units = (value / step).to_integral_value(rounding=ROUND_DOWN)
    return units * step


def _prune_client_id_cache(now: float) -> None:
    """Remove old entries from the in-process clientOrderId cache."""
    cutoff = now - _IDEMPOTENCY_CACHE_TTL_SEC
    dead = [k for k, ts in _CLIENT_ID_CACHE.items() if ts < cutoff]
    for k in dead:
        _CLIENT_ID_CACHE.pop(k, None)


def _validate_client_order_id(client_order_id: str) -> str:
    """Validate Binance newClientOrderId constraints (ASCII, 1..36 chars)."""
    cid = str(client_order_id).strip()
    if not cid:
        raise ValueError("client_order_id is empty")
    if len(cid) > 36:
        raise ValueError("client_order_id exceeds 36 chars")
    try:
        cid.encode("ascii")
    except UnicodeEncodeError:
        raise ValueError("client_order_id must be ASCII") from None
    return cid


def _make_client_order_id(prefix: str = "at") -> str:
    """Generate a safe client order id within Binance length limits."""
    base = f"{prefix}-{uuid.uuid4().hex}"  # 3 + 32 = 35 chars
    return base[:36]


def _make_child_client_order_id(parent: str, suffix: str) -> str:
    """
    Deterministic-ish child id derived from parent id.

    Constraints:
    - ASCII
    - <= 36 chars
    - No fallback: parent must be valid clientOrderId.
    """
    p = _validate_client_order_id(parent)
    sfx = str(suffix).strip().lower()
    if not sfx:
        raise ValueError("suffix is empty")
    # Reserve: "-" + suffix (up to 6 chars typical). Truncate parent to fit 36.
    tail = f"-{sfx}"
    if len(tail) >= 36:
        raise ValueError("suffix too long for client order id")
    head_max = 36 - len(tail)
    return f"{p[:head_max]}{tail}"


def _require_positive_float(v: Any, name: str) -> float:
    try:
        f = float(v)
    except Exception as e:
        raise ValueError(f"{name} must be a number") from e
    if not math.isfinite(f):
        raise ValueError(f"{name} must be finite")
    if f <= 0:
        raise ValueError(f"{name} must be > 0")
    return f


def _get_allocation_ratio_for_log(settings: Any) -> Optional[float]:
    """
    로그/분석용 allocation_ratio를 가져온다.

    - 이 파일에서는 포지션 사이징을 계산하지 않는다(상위 레이어 책임).
    - settings.allocation_ratio 우선, 없으면 settings.risk_pct(legacy alias) 사용.
    """
    v = getattr(settings, "allocation_ratio", None)
    if v is None:
        v = getattr(settings, "risk_pct", None)
    if v is None:
        return None
    try:
        f = float(v)
    except Exception:
        return None
    if not math.isfinite(f):
        return None
    return f


def _trade_ctor_supported_fields() -> set[str]:
    """
    레거시/버전 차이를 안전하게 흡수하기 위한 호환용.

    STRICT는 "데이터"에 적용한다.
    여기서는 Trade dataclass/ctor의 시그니처 차이로 인해 런타임 크래시가 나는 것을 방지한다.
    """
    try:
        # dataclass이면 __dataclass_fields__가 가장 정확
        f = getattr(Trade, "__dataclass_fields__", None)
        if isinstance(f, dict) and f:
            return set(f.keys())
    except Exception:
        pass

    try:
        sig = inspect.signature(Trade)  # type: ignore[arg-type]
        return {p.name for p in sig.parameters.values() if p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)}
    except Exception:
        # 최후: 비정상 타입이면 그냥 빈 집합(Trade 생성 시 키워드 최소화)
        return set()


def _make_trade_strict(**kwargs: Any) -> Trade:
    """
    Trade 객체 생성(호환).

    - Trade ctor에 존재하는 필드만 전달한다.
    - 값 자체의 유효성(0 대입/추정 등)은 호출부에서 이미 STRICT로 보장해야 한다.
    """
    allowed = _trade_ctor_supported_fields()
    if not allowed:
        # Trade가 예상과 다르면 그대로 전달 (에러는 상위로, 폴백 금지)
        return Trade(**kwargs)  # type: ignore[arg-type]
    filtered = {k: v for k, v in kwargs.items() if k in allowed}
    return Trade(**filtered)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Binance error helpers
# ---------------------------------------------------------------------------
_CODE_RE = re.compile(r"code=([-]?\d+)")


def _extract_code(err: Exception) -> Optional[int]:
    """Extract Binance error code from exception string when available."""
    m = _CODE_RE.search(str(err))
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _is_timestamp_error(err: Exception) -> bool:
    """Detect Binance timestamp/recvWindow errors (-1021)."""
    s = str(err).lower()
    code = _extract_code(err)
    if code == -1021:
        return True
    if "-1021" in s:
        return True
    if "timestamp" in s and (
        "recvwindow" in s or "ahead of the server" in s or "behind the server" in s
    ):
        return True
    return False


def _is_order_not_found(err: Exception) -> bool:
    """Detect order-not-found when querying by origClientOrderId."""
    s = str(err).lower()
    code = _extract_code(err)
    if code in {-2013, -2011}:
        return True
    if "order does not exist" in s:
        return True
    return False


def _is_no_need_change_margin(err: Exception) -> bool:
    """Detect idempotent response when setting margin mode."""
    s = str(err).lower()
    code = _extract_code(err)
    if code == -4046:
        return True
    if "no need to change margin type" in s:
        return True
    return False


def _is_no_need_change_leverage(err: Exception) -> bool:
    """Detect idempotent response when setting leverage."""
    s = str(err).lower()
    code = _extract_code(err)
    if code in {-4047, -4048}:
        return True
    if "no need to change leverage" in s or "leverage not modified" in s or "not modified" in s:
        return True
    return False


def _call_with_time_sync_retry(fn) -> Any:
    """Execute fn; on timestamp error (-1021), sync server time and retry once."""
    try:
        return execute_with_retry(fn)
    except Exception as e:
        if _is_timestamp_error(e):
            logger.warning("timestamp error detected; syncing server time and retrying once")
            sync_server_time()  # raises on failure
            return execute_with_retry(fn)
        raise


# ---------------------------------------------------------------------------
# Symbol filters: exchangeInfo cache
# ---------------------------------------------------------------------------
def _parse_symbol_filters(exchange_info: Dict[str, Any], symbol: str) -> SymbolFilters:
    """Parse tick/step/minQty filters from /fapi/v1/exchangeInfo response."""
    symbols = exchange_info.get("symbols")
    if not isinstance(symbols, list):
        raise RuntimeError("exchangeInfo missing 'symbols' list")

    sym_info = None
    for item in symbols:
        if isinstance(item, dict) and str(item.get("symbol", "")).upper() == symbol:
            sym_info = item
            break

    if not isinstance(sym_info, dict):
        raise RuntimeError(f"exchangeInfo does not contain symbol={symbol}")

    filters = sym_info.get("filters")
    if not isinstance(filters, list):
        raise RuntimeError(f"exchangeInfo symbol={symbol} missing filters")

    tick: Optional[Decimal] = None
    lot_step: Optional[Decimal] = None
    lot_min: Optional[Decimal] = None
    mkt_step: Optional[Decimal] = None
    mkt_min: Optional[Decimal] = None

    for f in filters:
        if not isinstance(f, dict):
            continue
        ftype = str(f.get("filterType", "")).upper().strip()

        if ftype == "PRICE_FILTER":
            tick = _to_decimal(f.get("tickSize"), name="tickSize")
        elif ftype == "LOT_SIZE":
            lot_step = _to_decimal(f.get("stepSize"), name="stepSize(LOT_SIZE)")
            lot_min = _to_decimal(f.get("minQty"), name="minQty(LOT_SIZE)")
        elif ftype == "MARKET_LOT_SIZE":
            mkt_step = _to_decimal(f.get("stepSize"), name="stepSize(MARKET_LOT_SIZE)")
            mkt_min = _to_decimal(f.get("minQty"), name="minQty(MARKET_LOT_SIZE)")

    if tick is None or tick <= 0:
        raise RuntimeError(f"missing/invalid PRICE_FILTER.tickSize for symbol={symbol}")

    if lot_step is None or lot_min is None:
        if mkt_step is None or mkt_min is None:
            raise RuntimeError(f"missing LOT_SIZE and MARKET_LOT_SIZE for symbol={symbol}")
        lot_step = mkt_step
        lot_min = mkt_min

    if mkt_step is None or mkt_min is None:
        mkt_step = lot_step
        mkt_min = lot_min

    if lot_step <= 0 or lot_min <= 0 or mkt_step <= 0 or mkt_min <= 0:
        raise RuntimeError(f"invalid step/minQty in exchangeInfo for symbol={symbol}")

    return SymbolFilters(
        symbol=symbol,
        tick_size=tick,
        lot_step=lot_step,
        lot_min_qty=lot_min,
        market_step=mkt_step,
        market_min_qty=mkt_min,
    )


def get_symbol_filters(symbol: str) -> SymbolFilters:
    """Get symbol filters with in-memory cache + TTL."""
    sym = _normalize_symbol(symbol)
    now = time.time()

    with _FILTER_LOCK:
        cached = _FILTER_CACHE.get(sym)
        if cached:
            ts, filt = cached
            if now - ts <= _FILTER_CACHE_TTL_SEC:
                return filt

    info = get_exchange_info(sym)
    if not isinstance(info, dict):
        raise RuntimeError("get_exchange_info returned non-dict")
    filt = _parse_symbol_filters(info, sym)

    with _FILTER_LOCK:
        _FILTER_CACHE[sym] = (now, filt)
    return filt


# ---------------------------------------------------------------------------
# Trading settings enforcement (margin mode / leverage)
# ---------------------------------------------------------------------------
def _require_margin_mode(settings: Any) -> str:
    mm = getattr(settings, "margin_mode", None)
    if not isinstance(mm, str) or not mm.strip():
        raise ValueError("settings.margin_mode is required")
    mm_u = mm.strip().upper()
    if mm_u == "CROSS":
        mm_u = "CROSSED"
    if mm_u not in {"ISOLATED", "CROSSED"}:
        raise ValueError(f"invalid margin_mode: {mm!r}")
    return mm_u


def _require_leverage(settings: Any) -> int:
    lev = getattr(settings, "leverage", None)
    if lev is None:
        raise ValueError("settings.leverage is required")
    try:
        iv = int(float(lev))
    except Exception:
        raise ValueError(f"invalid leverage: {lev!r}") from None
    if iv < 1:
        raise ValueError("leverage must be >= 1")
    return iv


def _require_timeout_sec(settings: Any) -> int:
    v = getattr(settings, "request_timeout_sec", None)
    if v is None:
        raise ValueError("settings.request_timeout_sec is required")
    try:
        iv = int(float(v))
    except Exception:
        raise ValueError("invalid request_timeout_sec") from None
    if iv < 1:
        raise ValueError("request_timeout_sec must be >= 1")
    return iv


def _require_recv_window_ms(settings: Any) -> int:
    v = getattr(settings, "recv_window_ms", None)
    if v is None:
        raise ValueError("settings.recv_window_ms is required")
    try:
        iv = int(float(v))
    except Exception:
        raise ValueError("invalid recv_window_ms") from None
    if iv < 1:
        raise ValueError("recv_window_ms must be >= 1")
    return iv


def ensure_trading_settings(symbol: str, settings: Optional[Any] = None) -> None:
    sym = _normalize_symbol(symbol)
    st = settings if settings is not None else SET
    margin_mode = _require_margin_mode(st)
    leverage = _require_leverage(st)

    with _SETTINGS_LOCK:
        if sym in _SETTINGS_APPLIED:
            return

    try:
        set_margin_mode(sym, margin_mode)
    except Exception as e:
        if not _is_no_need_change_margin(e):
            raise RuntimeError(f"set_margin_mode failed: {e}") from None

    try:
        set_leverage(sym, leverage)
    except Exception as e:
        if not _is_no_need_change_leverage(e):
            raise RuntimeError(f"set_leverage failed: {e}") from None

    with _SETTINGS_LOCK:
        _SETTINGS_APPLIED.add(sym)

    logger.info("trading settings ensured (symbol=%s margin_mode=%s leverage=%s)", sym, margin_mode, leverage)


# ---------------------------------------------------------------------------
# Idempotency helpers
# ---------------------------------------------------------------------------
def _find_open_order_by_client_id(symbol: str, client_order_id: str) -> Optional[Dict[str, Any]]:
    orders = get_open_orders(symbol)
    if not isinstance(orders, list):
        raise RuntimeError("get_open_orders returned non-list")
    for o in orders:
        if not isinstance(o, dict):
            continue
        if str(o.get("clientOrderId", "")).strip() == client_order_id:
            return o
    return None


def _get_order_by_client_id(symbol: str, client_order_id: str, timeout_sec: int) -> Optional[Dict[str, Any]]:
    params = {"symbol": symbol, "origClientOrderId": client_order_id}
    try:
        data = _call_with_time_sync_retry(
            lambda: req("GET", "/fapi/v1/order", params, private=True, timeout_sec=timeout_sec)
        )
    except Exception as e:
        if _is_order_not_found(e):
            return None
        raise
    if not isinstance(data, dict):
        raise RuntimeError("GET /fapi/v1/order returned non-dict")
    return data


def _wait_for_inflight_or_existing(symbol: str, client_order_id: str, timeout_sec: int) -> Optional[Dict[str, Any]]:
    deadline = time.time() + _IDEMPOTENCY_INFLIGHT_WAIT_SEC
    while True:
        with _CLIENT_ID_LOCK:
            inflight = client_order_id in _CLIENT_ID_INFLIGHT

        existing = _find_open_order_by_client_id(symbol, client_order_id)
        if existing is not None:
            return existing

        existing2 = _get_order_by_client_id(symbol, client_order_id, timeout_sec)
        if existing2 is not None:
            return existing2

        if not inflight:
            return None

        if time.time() >= deadline:
            raise RuntimeError("idempotency conflict: order is in-flight but not visible")
        time.sleep(0.05)


# ---------------------------------------------------------------------------
# Core order placement
# ---------------------------------------------------------------------------
def _round_qty_and_price(
    *,
    filters: SymbolFilters,
    order_type: str,
    raw_qty: Any,
    raw_price: Optional[Any],
    raw_stop_price: Optional[Any],
) -> Tuple[Decimal, Optional[Decimal], Optional[Decimal]]:
    type_u = order_type.upper().strip()
    is_market_like = type_u == "MARKET" or type_u.endswith("_MARKET")

    step = filters.market_step if is_market_like else filters.lot_step
    min_qty = filters.market_min_qty if is_market_like else filters.lot_min_qty

    qty = _to_decimal(raw_qty, name="quantity")
    if qty <= 0:
        raise ValueError("quantity must be > 0")

    qty_n = _floor_to_step(qty, step)
    if qty_n <= 0:
        raise ValueError("quantity rounded to 0")
    if qty_n < min_qty:
        raise ValueError(f"quantity {qty_n} < minQty {min_qty}")

    price_n: Optional[Decimal] = None
    if raw_price is not None:
        price = _to_decimal(raw_price, name="price")
        if price <= 0:
            raise ValueError("price must be > 0")
        price_n = _floor_to_step(price, filters.tick_size)
        if price_n <= 0:
            raise ValueError("price rounded to 0")

    stop_n: Optional[Decimal] = None
    if raw_stop_price is not None:
        sp = _to_decimal(raw_stop_price, name="stopPrice")
        if sp <= 0:
            raise ValueError("stopPrice must be > 0")
        stop_n = _floor_to_step(sp, filters.tick_size)
        if stop_n <= 0:
            raise ValueError("stopPrice rounded to 0")

    return qty_n, price_n, stop_n


def _place_order(
    *,
    settings: Optional[Any],
    symbol: str,
    side: str,
    order_type: str,
    quantity: Any,
    price: Optional[Any] = None,
    stop_price: Optional[Any] = None,
    time_in_force: Optional[str] = None,
    reduce_only: Optional[bool] = None,
    position_side: Optional[str] = None,
    close_position: Optional[bool] = None,
    client_order_id: Optional[str] = None,
) -> Dict[str, Any]:
    st = settings if settings is not None else SET

    sym = _normalize_symbol(symbol)
    side_u = _normalize_side(side)
    type_u = str(order_type).upper().strip()
    if not type_u:
        raise ValueError("order_type is empty")

    tif_u: Optional[str] = None
    if time_in_force is not None:
        tif_u = str(time_in_force).upper().strip() or None

    pos_side_u = _normalize_position_side(position_side)

    timeout_sec = _require_timeout_sec(st)
    recv_window_ms = _require_recv_window_ms(st)

    cid = _validate_client_order_id(client_order_id) if client_order_id else _make_client_order_id()

    acquired = False
    with _CLIENT_ID_LOCK:
        now = time.time()
        _prune_client_id_cache(now)
        if cid not in _CLIENT_ID_INFLIGHT:
            _CLIENT_ID_INFLIGHT.add(cid)
            _CLIENT_ID_CACHE[cid] = now
            acquired = True

    if not acquired:
        existing_wait = _wait_for_inflight_or_existing(sym, cid, timeout_sec)
        if existing_wait is not None:
            logger.info(
                "idempotency hit (inflight wait): returning existing order (symbol=%s clientOrderId=%s orderId=%s)",
                sym,
                cid,
                existing_wait.get("orderId"),
            )
            return existing_wait

        with _CLIENT_ID_LOCK:
            now = time.time()
            _prune_client_id_cache(now)
            if cid in _CLIENT_ID_INFLIGHT:
                raise RuntimeError("idempotency conflict: could not acquire inflight lock")
            _CLIENT_ID_INFLIGHT.add(cid)
            _CLIENT_ID_CACHE[cid] = now

    try:
        ensure_trading_settings(sym, st)

        filt = get_symbol_filters(sym)
        qty_n, price_n, stop_n = _round_qty_and_price(
            filters=filt,
            order_type=type_u,
            raw_qty=quantity,
            raw_price=price,
            raw_stop_price=stop_price,
        )

        existing = _find_open_order_by_client_id(sym, cid)
        if existing is not None:
            logger.info(
                "idempotency hit: returning existing open order (symbol=%s clientOrderId=%s orderId=%s)",
                sym,
                cid,
                existing.get("orderId"),
            )
            return existing

        existing2 = _get_order_by_client_id(sym, cid, timeout_sec)
        if existing2 is not None:
            logger.info(
                "idempotency hit: returning existing order (symbol=%s clientOrderId=%s orderId=%s status=%s)",
                sym,
                cid,
                existing2.get("orderId"),
                existing2.get("status"),
            )
            return existing2

        params: Dict[str, Any] = {
            "symbol": sym,
            "side": side_u,
            "type": type_u,
            "quantity": _d_to_str(qty_n),
            "newClientOrderId": cid,
            "recvWindow": recv_window_ms,
            "newOrderRespType": "RESULT",
        }

        if pos_side_u:
            params["positionSide"] = pos_side_u

        if reduce_only is not None:
            params["reduceOnly"] = bool(reduce_only)

        if close_position is not None:
            params["closePosition"] = bool(close_position)

        if price_n is not None:
            params["price"] = _d_to_str(price_n)
        if stop_n is not None:
            params["stopPrice"] = _d_to_str(stop_n)

        if type_u in {"LIMIT", "STOP", "TAKE_PROFIT"}:
            params["timeInForce"] = tif_u or "GTC"
            if "price" not in params:
                raise ValueError(f"price is required for order_type={type_u}")

        if type_u in {"STOP", "TAKE_PROFIT"} and "stopPrice" not in params:
            raise ValueError(f"stopPrice is required for order_type={type_u}")

        if type_u in {"STOP_MARKET", "TAKE_PROFIT_MARKET"} and "stopPrice" not in params:
            raise ValueError(f"stopPrice is required for order_type={type_u}")

        logger.info(
            "submit order (symbol=%s side=%s type=%s qty=%s price=%s stopPrice=%s reduceOnly=%s positionSide=%s clientOrderId=%s)",
            sym,
            side_u,
            type_u,
            _d_to_str(qty_n),
            params.get("price"),
            params.get("stopPrice"),
            params.get("reduceOnly"),
            params.get("positionSide"),
            cid,
        )

        def _do():
            return req("POST", "/fapi/v1/order", params, private=True, timeout_sec=timeout_sec)

        data = _call_with_time_sync_retry(_do)
        if not isinstance(data, dict):
            raise RuntimeError("POST /fapi/v1/order returned non-dict")

        logger.info(
            "order accepted (symbol=%s orderId=%s status=%s clientOrderId=%s)",
            sym,
            data.get("orderId"),
            data.get("status"),
            data.get("clientOrderId"),
        )
        return data
    finally:
        with _CLIENT_ID_LOCK:
            _CLIENT_ID_INFLIGHT.discard(cid)


def place_market(
    symbol: str,
    side: str,
    qty: float,
    *,
    settings: Optional[Any] = None,
    reduce_only: bool = False,
    position_side: Optional[str] = "BOTH",
    client_order_id: Optional[str] = None,
) -> Dict[str, Any]:
    return _place_order(
        settings=settings,
        symbol=symbol,
        side=side,
        order_type="MARKET",
        quantity=qty,
        reduce_only=reduce_only,
        position_side=position_side,
        client_order_id=client_order_id,
    )


def place_limit(
    symbol: str,
    side: str,
    qty: float,
    price: float,
    *,
    settings: Optional[Any] = None,
    time_in_force: str = "GTC",
    reduce_only: bool = False,
    position_side: Optional[str] = "BOTH",
    client_order_id: Optional[str] = None,
) -> Dict[str, Any]:
    return _place_order(
        settings=settings,
        symbol=symbol,
        side=side,
        order_type="LIMIT",
        quantity=qty,
        price=price,
        time_in_force=time_in_force,
        reduce_only=reduce_only,
        position_side=position_side,
        client_order_id=client_order_id,
    )


def place_conditional(
    symbol: str,
    side: str,
    qty: float,
    trigger_price: float,
    order_type: str,
    *,
    settings: Optional[Any] = None,
    reduce_only: bool = True,
    position_side: Optional[str] = "BOTH",
    client_order_id: Optional[str] = None,
) -> Dict[str, Any]:
    type_u = str(order_type).upper().strip()
    return _place_order(
        settings=settings,
        symbol=symbol,
        side=side,
        order_type=type_u,
        quantity=qty,
        stop_price=trigger_price,
        reduce_only=reduce_only,
        position_side=position_side,
        client_order_id=client_order_id,
    )


def cancel_order_safe(
    symbol: str,
    order_id: int | str,
    *,
    settings: Optional[Any] = None,
) -> Dict[str, Any]:
    st = settings if settings is not None else SET
    sym = _normalize_symbol(symbol)
    timeout_sec = _require_timeout_sec(st)

    params = {"symbol": sym, "orderId": order_id}

    def _do():
        return req("DELETE", "/fapi/v1/order", params, private=True, timeout_sec=timeout_sec)

    data = _call_with_time_sync_retry(_do)
    if not isinstance(data, dict):
        raise RuntimeError("DELETE /fapi/v1/order returned non-dict")
    return data


def set_tp_sl(
    *,
    symbol: str,
    side_open: str,
    qty: float,
    tp_price: float,
    sl_price: float,
    soft_mode: bool = False,
    sl_floor_ratio: Optional[float] = None,
    settings: Optional[Any] = None,
    tp_client_order_id: Optional[str] = None,
    sl_client_order_id: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """
    STRICT:
    - TP/SL 주문을 생성하고, 생성된 orderId를 반환한다.
    - soft_mode=True이면 SL 주문은 생성하지 않는다.
    - 폴백(룩백/추정) 금지: 반환은 거래소 응답(orderId) 기반.
    """
    _ = sl_floor_ratio

    sym = _normalize_symbol(symbol)
    open_side = _normalize_side(side_open)
    close_side = "SELL" if open_side == "BUY" else "BUY"

    tp_order_id: Optional[str] = None
    sl_order_id: Optional[str] = None

    if tp_price and tp_price > 0:
        tp_resp = place_conditional(
            symbol=sym,
            side=close_side,
            qty=qty,
            trigger_price=float(tp_price),
            order_type="TAKE_PROFIT_MARKET",
            settings=settings,
            reduce_only=True,
            position_side="BOTH",
            client_order_id=tp_client_order_id,
        )
        oid = tp_resp.get("orderId") if isinstance(tp_resp, dict) else None
        if oid is None:
            raise RuntimeError("TP order response missing orderId")
        tp_order_id = str(oid)

    if soft_mode:
        return tp_order_id, None

    if sl_price and sl_price > 0:
        sl_resp = place_conditional(
            symbol=sym,
            side=close_side,
            qty=qty,
            trigger_price=float(sl_price),
            order_type="STOP_MARKET",
            settings=settings,
            reduce_only=True,
            position_side="BOTH",
            client_order_id=sl_client_order_id,
        )
        oid = sl_resp.get("orderId") if isinstance(sl_resp, dict) else None
        if oid is None:
            raise RuntimeError("SL order response missing orderId")
        sl_order_id = str(oid)

    return tp_order_id, sl_order_id


def _fetch_filled_entry_price_strict(
    *,
    symbol: str,
    order_id: Any,
    settings: Any,
    max_wait_sec: float = 2.0,
) -> float:
    """
    STRICT:
    - entry_price는 실제 체결 값으로만 확정한다.
    - 0.0 대입/폴백 금지.
    - /fapi/v1/order로 FILLED 확인 후 avgPrice 또는 (cummulativeQuoteQty/executedQty)로 계산.
    """
    sym = _normalize_symbol(symbol)
    timeout_sec = _require_timeout_sec(settings)

    oid = order_id
    if oid is None:
        raise RuntimeError("entry order_id missing")

    deadline = time.time() + float(max_wait_sec)

    while True:
        od = _call_with_time_sync_retry(
            lambda: req(
                "GET",
                "/fapi/v1/order",
                {"symbol": sym, "orderId": oid},
                private=True,
                timeout_sec=timeout_sec,
            )
        )
        if not isinstance(od, dict):
            raise RuntimeError("GET /fapi/v1/order returned non-dict")

        status = str(od.get("status") or "").upper()
        if status == "FILLED":
            ap = od.get("avgPrice")
            try:
                apf = float(ap) if ap is not None else 0.0
            except Exception:
                apf = 0.0

            if apf > 0:
                return apf

            cq = od.get("cummulativeQuoteQty")
            eq = od.get("executedQty")
            if cq is None or eq is None:
                raise RuntimeError("FILLED order missing cummulativeQuoteQty/executedQty")

            cqf = float(cq)
            eqf = float(eq)
            if eqf <= 0 or cqf <= 0:
                raise RuntimeError("FILLED order has non-positive executedQty/cummulativeQuoteQty")

            price = cqf / eqf
            if price <= 0:
                raise RuntimeError("computed entry price <= 0")
            return float(price)

        if time.time() >= deadline:
            raise RuntimeError(f"entry order not FILLED within {max_wait_sec}s (status={status})")
        time.sleep(0.2)


def open_position_with_tp_sl(
    settings: Any,
    symbol: str,
    side_open: str,
    qty: float,
    entry_price_hint: float,
    tp_pct: float,
    sl_pct: float,
    source: str,
    soft_mode: bool = False,
    sl_floor_ratio: Optional[float] = None,
    *,
    available_usdt: Optional[float] = None,
    entry_client_order_id: Optional[str] = None,
) -> Optional["Trade"]:
    """
    STRICT:
    - 입력/설정 값 비정상 시 즉시 예외(폴백 금지)
    - entry_price는 실제 체결 데이터로만 확정
    - soft_mode=False 인데 TP/SL 설정 실패 시 포지션 즉시 강제 청산 후 실패 처리
    - qty는 반드시 > 0 (auto-sizing 제거: 상위 레이어에서 수량 산출)
    - 결정적 clientOrderId 강제 모드(settings.require_deterministic_client_order_id=True)에서는
      entry_client_order_id 미지정 시 즉시 예외(폴백 금지)
    """
    _ = available_usdt  # (호환용) auto-sizing 제거로 더 이상 사용하지 않음

    sym = _normalize_symbol(symbol)
    open_side = _normalize_side(side_open)

    # leverage는 STRICT로 강제 (실제 적용은 ensure_trading_settings에서 수행)
    lev = _require_leverage(settings)

    final_qty = _require_positive_float(qty, "qty")

    tp_pct_f = float(tp_pct)
    sl_pct_f = float(sl_pct)
    if tp_pct_f < 0 or sl_pct_f < 0:
        raise ValueError("tp_pct and sl_pct must be >= 0")

    # SL은 soft_mode=False면 반드시 켠다(안전)
    if not bool(soft_mode) and sl_pct_f <= 0:
        raise ValueError("sl_pct must be > 0 when soft_mode=False")

    eph = float(entry_price_hint)
    if not math.isfinite(eph):
        raise ValueError("entry_price_hint must be finite")

    require_det = bool(getattr(settings, "require_deterministic_client_order_id", False))
    if entry_client_order_id is not None:
        entry_cid = _validate_client_order_id(entry_client_order_id)
    else:
        if require_det:
            raise ValueError("entry_client_order_id is required when settings.require_deterministic_client_order_id=True")
        entry_cid = _make_client_order_id(prefix="ent")

    # TP/SL clientOrderId (가능하면 entry_cid 기반으로 결정적으로 파생)
    tp_cid = _make_child_client_order_id(entry_cid, "tp")
    sl_cid = _make_child_client_order_id(entry_cid, "sl")

    try:
        entry_resp = place_market(
            symbol=sym,
            side=open_side,
            qty=float(final_qty),
            settings=settings,
            reduce_only=False,
            position_side="BOTH",
            client_order_id=entry_cid,
        )
    except Exception as e:
        logger.error(
            "entry order failed (symbol=%s side=%s qty=%s source=%s err=%s)",
            sym,
            open_side,
            final_qty,
            source,
            str(e),
        )
        log_event(
            event_type="ERROR",
            symbol=sym,
            regime=source,
            side=_event_side_from_open_side(open_side),
            reason=str(e) or "entry_order_failed",
            extra_json={"open_side": open_side, "qty": float(final_qty), "clientOrderId": entry_cid},
        )
        return None

    # entry_order_id STRICT 확보
    order_id = entry_resp.get("orderId") if isinstance(entry_resp, dict) else None
    if order_id is None:
        # RESULT 타입인데 orderId가 없으면 비정상. 폴백 금지.
        raise RuntimeError("entry response missing orderId (STRICT)")
    entry_order_id = str(order_id)

    # entry_price STRICT 확정
    try:
        entry_price = _fetch_filled_entry_price_strict(
            symbol=sym,
            order_id=entry_order_id,
            settings=settings,
            max_wait_sec=float(getattr(settings, "entry_fill_wait_sec", 2.0) or 2.0),
        )
    except Exception as e:
        logger.error("entry price resolve failed (symbol=%s err=%s)", sym, str(e))
        log_event(
            event_type="ERROR",
            symbol=sym,
            regime=source,
            side=_event_side_from_open_side(open_side),
            reason="entry_price_unavailable",
            extra_json={"err": str(e), "clientOrderId": entry_cid, "entry_order_id": entry_order_id},
        )
        # 포지션이 이미 열렸을 가능성이 있으므로 안전하게 강제 청산 시도
        try:
            close_position_market(sym, open_side, float(final_qty))
        except Exception as e2:
            logger.critical("force close after entry_price failure also failed (symbol=%s err=%s)", sym, str(e2))
        return None

    if entry_price <= 0:
        raise RuntimeError("entry_price resolved <= 0 (STRICT violation)")

    # 슬리피지 가드(옵션)
    max_slip = getattr(settings, "max_entry_slippage_pct", None)
    if max_slip is not None:
        try:
            max_slip_f = float(max_slip)
        except Exception as e:
            raise ValueError("settings.max_entry_slippage_pct must be a number") from e
        if max_slip_f < 0:
            raise ValueError("settings.max_entry_slippage_pct must be >= 0")
        if max_slip_f > 0:
            if eph <= 0:
                raise ValueError("entry_price_hint must be > 0 when max_entry_slippage_pct is enabled")
            slip_pct = abs(entry_price - eph) / eph * 100.0
            if slip_pct > max_slip_f:
                logger.warning(
                    "slippage guard triggered (symbol=%s hint=%s fill=%s slip_pct=%.4f limit=%.4f) -> force close",
                    sym,
                    eph,
                    entry_price,
                    slip_pct,
                    max_slip_f,
                )
                log_event(
                    event_type="ERROR",
                    symbol=sym,
                    regime=source,
                    side=_event_side_from_open_side(open_side),
                    reason="slippage_guard_force_close",
                    extra_json={
                        "entry_price_hint": float(eph),
                        "entry_price": float(entry_price),
                        "slippage_pct": float(slip_pct),
                        "max_entry_slippage_pct": float(max_slip_f),
                        "qty": float(final_qty),
                        "entry_order_id": entry_order_id,
                        "clientOrderId": entry_cid,
                    },
                )
                try:
                    close_position_market(sym, open_side, float(final_qty))
                except Exception as e2:
                    logger.critical("slippage guard force close failed (symbol=%s err=%s)", sym, str(e2))
                return None

    # ENTRY 이벤트
    log_event(
        event_type="ENTRY",
        symbol=sym,
        regime=source,
        side=_event_side_from_open_side(open_side),  # LONG/SHORT
        price=float(entry_price),
        qty=float(final_qty),
        leverage=int(float(lev)),
        tp_pct=float(tp_pct_f),
        sl_pct=float(sl_pct_f),
        # NOTE: 전환 기간 호환. 계산에는 사용 금지(로깅용).
        risk_pct=_get_allocation_ratio_for_log(settings),
        reason="MARKET_ENTRY_FILLED",
        extra_json={"open_side": open_side, "clientOrderId": entry_cid, "entry_order_id": entry_order_id},
    )

    # TP/SL 가격 계산
    if open_side == "BUY":
        tp_price = entry_price * (1.0 + tp_pct_f)
        sl_price = entry_price * (1.0 - sl_pct_f)
    else:
        tp_price = entry_price * (1.0 - tp_pct_f)
        sl_price = entry_price * (1.0 + sl_pct_f)

    if isinstance(sl_floor_ratio, (int, float)) and float(sl_floor_ratio) > 0:
        floor_r = float(sl_floor_ratio)
        if open_side == "BUY":
            floor_price = entry_price * (1.0 - floor_r)
            if sl_price > floor_price:
                sl_price = floor_price
        else:
            floor_price = entry_price * (1.0 + floor_r)
            if sl_price < floor_price:
                sl_price = floor_price

    # TP/SL 설정 (STRICT) + orderId 반환
    try:
        tp_order_id, sl_order_id = set_tp_sl(
            symbol=sym,
            side_open=open_side,
            qty=float(final_qty),
            tp_price=float(tp_price),
            sl_price=float(sl_price),
            soft_mode=bool(soft_mode),
            sl_floor_ratio=sl_floor_ratio,
            settings=settings,
            tp_client_order_id=tp_cid,
            sl_client_order_id=sl_cid,
        )
    except Exception as e:
        logger.error("set_tp_sl failed (symbol=%s err=%s soft_mode=%s)", sym, str(e), bool(soft_mode))
        log_event(
            event_type="ERROR",
            symbol=sym,
            regime=source,
            side=_event_side_from_open_side(open_side),
            reason="tp_sl_set_failed",
            extra_json={"err": str(e), "soft_mode": bool(soft_mode), "qty": float(final_qty), "entry_order_id": entry_order_id},
        )

        if bool(soft_mode):
            return None

        try:
            close_position_market(sym, open_side, float(final_qty))
        except Exception as e2:
            logger.critical("force close after TP/SL failure also failed (symbol=%s err=%s)", sym, str(e2))
        return None

    # soft_mode=False면 SL은 반드시 있어야 한다(안전). 없으면 즉시 실패.
    if not bool(soft_mode) and (sl_order_id is None or not str(sl_order_id).strip()):
        raise RuntimeError("soft_mode=False but SL order_id is missing (STRICT)")

    log_event(
        event_type="TP_SL_SET",
        symbol=sym,
        regime=source,
        side=_event_side_from_open_side(open_side),  # LONG/SHORT
        price=float(entry_price),
        qty=float(final_qty),
        leverage=int(float(lev)),
        tp_pct=float(tp_pct_f),
        sl_pct=float(sl_pct_f),
        # NOTE: 전환 기간 호환. 계산에는 사용 금지(로깅용).
        risk_pct=_get_allocation_ratio_for_log(settings),
        reason="TP_SL_CONFIGURED",
        extra_json={
            "soft_mode": bool(soft_mode),
            "tp_order_id": tp_order_id,
            "sl_order_id": sl_order_id,
            "tp_clientOrderId": tp_cid,
            "sl_clientOrderId": sl_cid,
            "entry_order_id": entry_order_id,
            "entry_clientOrderId": entry_cid,
        },
    )

    # Trade 생성(호환)
    return _make_trade_strict(
        symbol=sym,
        side=open_side,
        qty=float(final_qty),
        entry_price=float(entry_price),
        leverage=int(float(lev)),
        source=str(source or "MARKET"),
        tp_price=float(tp_price) if tp_price > 0 else 0.0,
        sl_price=float(sl_price) if sl_price > 0 else 0.0,
        entry_order_id=entry_order_id,
        tp_order_id=str(tp_order_id) if tp_order_id is not None else None,
        sl_order_id=str(sl_order_id) if sl_order_id is not None else None,
        exchange_position_side="BOTH",
        remaining_qty=float(final_qty),
        realized_pnl_usdt=0.0,
    )


def close_position_market(symbol: str, side_open: str, qty: float) -> None:
    sym = _normalize_symbol(symbol)
    open_side = _normalize_side(side_open)
    close_side = "SELL" if open_side == "BUY" else "BUY"

    cid = _make_client_order_id(prefix="cls")
    resp = place_market(
        symbol=sym,
        side=close_side,
        qty=float(qty),
        settings=SET,
        reduce_only=True,
        position_side="BOTH",
        client_order_id=cid,
    )

    log_event(
        event_type="EXIT",
        symbol=sym,
        regime="MANUAL_CLOSE",
        side=_event_side_close(),  # CLOSE
        price=(resp.get("avgPrice") if isinstance(resp, dict) else "") or "",
        qty=float(qty),
        leverage=getattr(SET, "leverage", None),
        reason="MARKET_CLOSE_EXECUTED",
        extra_json={"close_side": close_side, "clientOrderId": cid, "orderId": resp.get("orderId") if isinstance(resp, dict) else None},
    )

    logger.info(
        "close market submitted (symbol=%s close_side=%s qty=%s orderId=%s clientOrderId=%s)",
        sym,
        close_side,
        qty,
        resp.get("orderId") if isinstance(resp, dict) else None,
        resp.get("clientOrderId") if isinstance(resp, dict) else None,
    )


def close_all_positions_market(symbol: str) -> int:
    """Reduce-Only 시장가로 해당 심볼의 모든 포지션을 정리한다.

    STRICT:
    - 포지션 응답에 필수 필드 누락/비정상 시 즉시 예외 (폴백 금지)
    """
    sym = _normalize_symbol(symbol)

    positions = fetch_open_positions(sym)
    if not isinstance(positions, list):
        raise RuntimeError("fetch_open_positions returned non-list")
    if not positions:
        return 0

    submitted = 0

    for p in positions:
        if not isinstance(p, dict):
            raise RuntimeError("position item is not a dict")
        if str(p.get("symbol") or "").upper() != sym:
            continue

        if "positionAmt" not in p:
            raise RuntimeError("position missing positionAmt")
        try:
            amt = float(p.get("positionAmt"))
        except Exception as e:
            raise RuntimeError(f"positionAmt parse failed: {e}") from e

        if abs(amt) < 1e-12:
            continue

        if "positionSide" not in p:
            raise RuntimeError("position missing positionSide")
        pos_side = str(p.get("positionSide") or "").upper().strip()
        if pos_side not in ("BOTH", "LONG", "SHORT"):
            raise RuntimeError(f"invalid positionSide: {pos_side!r}")

        # 포지션 방향 판정(STRICT)
        if pos_side in ("LONG", "SHORT"):
            direction = pos_side
        else:
            direction = "LONG" if amt > 0 else "SHORT"

        close_side = "SELL" if direction == "LONG" else "BUY"
        qty2 = abs(amt)

        client_id = _make_client_order_id(prefix="sigterm")
        logger.warning(
            "[FORCE_CLOSE] submit reduce-only market close: symbol=%s direction=%s qty=%s positionSide=%s",
            sym,
            direction,
            qty2,
            pos_side,
        )

        resp = place_market(
            symbol=sym,
            side=close_side,
            qty=float(qty2),
            settings=SET,
            reduce_only=True,
            position_side=pos_side,
            client_order_id=client_id,
        )

        log_event(
            event_type="EXIT",
            symbol=sym,
            regime="SIGTERM_FORCE_CLOSE",
            side=_event_side_close(),  # CLOSE
            price=(resp.get("avgPrice") if isinstance(resp, dict) else "") or "",
            qty=float(qty2),
            leverage=getattr(SET, "leverage", None),
            reason="SIGTERM_DEADLINE_FORCE_CLOSE",
            extra_json={"direction": direction, "close_side": close_side, "positionSide": pos_side, "orderId": resp.get("orderId") if isinstance(resp, dict) else None},
        )
        submitted += 1

    return submitted


__all__ = [
    "open_position_with_tp_sl",
    "place_market",
    "place_limit",
    "place_conditional",
    "cancel_order_safe",
    "set_tp_sl",
    "close_position_market",
    "get_symbol_filters",
    "ensure_trading_settings",
    "close_all_positions_market",
]