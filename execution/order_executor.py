"""
========================================================
FILE: execution/order_executor.py
ROLE:
- Binance USDT-M Futures 실제 주문 실행 SSOT
- 엔트리/익절/손절/청산 주문을 거래소 계약에 맞게 실행하고 검증한다
- 체결/포지션/보호주문 가시성을 strict하게 확인한 뒤에만 결과를 상위 계층에 반환한다

CORE RESPONSIBILITIES:
- symbol filter / tick / step / minQty 강제
- idempotency(newClientOrderId) 보장
- ENTRY/EXIT MARKET 체결 STRICT 검증
- 포지션 반영(positionAmt) STRICT 검증
- 청산 전 보호주문 정리 / 청산 후 포지션 0 STRICT 검증
- TP/SL 보호주문 visibility STRICT 검증
- Trade 반환 계약(strict fields / reconciliation_status) 보장

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- 누락/불일치/실패 시 즉시 예외
- source/symbol/action 계약을 임의 보정하지 않는다
- 체결 확인 없는 성공 처리 금지
- 무보호 포지션 상태를 정상으로 취급하지 않는다
- 청산 후 잔존 보호주문/잔존 포지션을 정상으로 취급하지 않는다
- print 금지, logging만 사용

CHANGE HISTORY:
- 2026-03-11:
  1) FIX(CONTRACT): open_position_with_tp_sl() 반환 Trade에 entry_ts 포함
  2) FIX(CONTRACT): 체결 주문 응답에서 실제 체결 시각(updateTime/transactTime/time) strict 추출 추가
  3) FIX(COMPAT): Trade 생성 시 entry_price와 함께 entry 필드도 같이 전달
  4) FIX(STRICT): Trade ctor 미지원 필드를 조용히 버리지 않고 즉시 예외 처리
  5) FIX(ROOT-CAUSE): TP/SL/로그/Trade.qty/remaining_qty 기준을 expectedQty가 아니라 executedQty로 통일
  6) FIX(STRICT): exchangeInfo LOT_SIZE / MARKET_LOT_SIZE 상호 대체 제거
  7) FIX(SSOT): close_position_market / close_all_positions_market 에 settings 주입 경로 추가
  8) FIX(STRICT): 포지션 0 판정을 매직넘버가 아니라 symbol market_step tolerance 기준으로 변경
  9) FIX(BUG): _verify_position_closed_strict() 내부 미정의 상수 _POSITION_CLOSE_POLL_SEC 사용을 _POSITION_CLOSE_VERIFY_POLL_SEC 로 정정
- 2026-03-10:
  1) FIX(ROOT-CAUSE): 청산 전 기존 보호주문(cancel) + 청산 후 포지션 0 검증 추가
  2) FIX(ROOT-CAUSE): ensure_trading_settings 캐시를 symbol 단독이 아니라 (margin_mode, leverage) 기준으로 정합화
  3) FIX(STRICT): idempotency 재사용 시 terminal status 주문을 성공으로 취급하지 않고 즉시 예외
  4) FIX(STRICT): FILLED 대기 중 CANCELED/REJECTED/EXPIRED는 즉시 실패 처리
  5) FIX(SSOT): settings.SETTINGS 단일 객체 사용
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
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Any, Dict, List, Optional, Tuple

from execution.exchange_api import (
    fetch_open_positions,
    get_exchange_info,
    get_open_orders,
    get_order,
    get_order_by_client_id,
    get_position,
    req,
    set_leverage,
    set_margin_mode,
    sync_server_time,
)
from execution.retry_policy import execute_with_retry  # type: ignore
from events.signals_logger import log_event
from settings import SETTINGS
from state.trader_state import Trade

# Decimal precision for money/qty calculations
getcontext().prec = 28

logger = logging.getLogger(__name__)

SET = SETTINGS

# Cache TTLs
_FILTER_CACHE_TTL_SEC: int = 1800  # 30 minutes
_IDEMPOTENCY_CACHE_TTL_SEC: int = 6 * 3600  # 6 hours
_IDEMPOTENCY_INFLIGHT_WAIT_SEC: float = 2.0  # wait window for concurrent same clientOrderId

# Protection order verification
_PROTECTION_VERIFY_WAIT_SEC: float = 3.0
_PROTECTION_VERIFY_POLL_SEC: float = 0.2
_PROTECTION_CANCEL_WAIT_SEC: float = 3.0
_PROTECTION_CANCEL_POLL_SEC: float = 0.2

# Position close verification
_POSITION_CLOSE_VERIFY_WAIT_SEC: float = 2.0
_POSITION_CLOSE_VERIFY_POLL_SEC: float = 0.2

# Existing order status handling
_TERMINAL_ORDER_STATUSES: set[str] = {
    "CANCELED",
    "REJECTED",
    "EXPIRED",
    "EXPIRED_IN_MATCH",
}
_ACTIVE_OR_FILLED_ORDER_STATUSES: set[str] = {
    "NEW",
    "PARTIALLY_FILLED",
    "FILLED",
    "PENDING_CANCEL",
}


# ---------------------------------------------------------------------------
# Exceptions (TRADE-GRADE)
# ---------------------------------------------------------------------------
class OrderExecutionError(RuntimeError):
    """Raised when order execution/verification fails (STRICT)."""


class OrderFillTimeoutError(OrderExecutionError):
    """Raised when an order is not FILLED within deadline (STRICT)."""


class PartialFillError(OrderExecutionError):
    """Raised when executedQty mismatches expectedQty (STRICT)."""


class PositionVerificationError(OrderExecutionError):
    """Raised when exchange position does not reflect expected execution (STRICT)."""


class ProtectionOrderVerificationError(OrderExecutionError):
    """Raised when TP/SL protection orders are missing or invalid (STRICT)."""


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

    symbol: str
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

_SETTINGS_APPLIED: Dict[str, Tuple[str, int]] = {}
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


def _require_nonempty_source(source: Any) -> str:
    s = str(source or "").strip()
    if not s:
        raise ValueError("source is empty (STRICT)")
    return s


def _require_ratio_float(value: Any, name: str, *, allow_zero: bool) -> float:
    try:
        f = float(value)
    except Exception as e:
        raise ValueError(f"{name} must be numeric (STRICT)") from e
    if not math.isfinite(f):
        raise ValueError(f"{name} must be finite (STRICT)")
    if allow_zero:
        if f < 0.0:
            raise ValueError(f"{name} must be >= 0 (STRICT)")
    else:
        if f <= 0.0:
            raise ValueError(f"{name} must be > 0 (STRICT)")
    if f > 1.0:
        raise ValueError(f"{name} must be <= 1.0 (STRICT)")
    return float(f)


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
    base = f"{prefix}-{uuid.uuid4().hex}"
    return base[:36]


def _make_child_client_order_id(parent: str, suffix: str) -> str:
    """
    Deterministic child id derived from parent id.

    Constraints:
    - ASCII
    - <= 36 chars
    - No fallback: parent must be valid clientOrderId.
    """
    p = _validate_client_order_id(parent)
    sfx = str(suffix).strip().lower()
    if not sfx:
        raise ValueError("suffix is empty")
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
    - allocation_ratio가 명시되어 있으면 strict하게 검증한다.
    - 속성이 아예 없으면 None을 반환한다.
    """
    if not hasattr(settings, "allocation_ratio"):
        return None

    v = getattr(settings, "allocation_ratio")
    if v is None:
        return None

    try:
        f = float(v)
    except Exception as e:
        raise ValueError("settings.allocation_ratio must be numeric (STRICT)") from e

    if not math.isfinite(f):
        raise ValueError("settings.allocation_ratio must be finite (STRICT)")
    if f < 0.0:
        raise ValueError("settings.allocation_ratio must be >= 0 (STRICT)")

    return float(f)


def _trade_ctor_supported_fields() -> set[str]:
    """
    STRICT:
    - Trade ctor 시그니처를 탐지하지 못하면 즉시 예외
    - 허용되지 않은 필드를 조용히 버리지 않는다
    """
    f = getattr(Trade, "__dataclass_fields__", None)
    if isinstance(f, dict) and f:
        return set(f.keys())

    try:
        sig = inspect.signature(Trade)  # type: ignore[arg-type]
        names = {
            p.name
            for p in sig.parameters.values()
            if p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)
        }
        if not names:
            raise RuntimeError("Trade signature has no parameters (unexpected)")
        return names
    except Exception as e:
        raise RuntimeError("unable to introspect Trade ctor fields (STRICT)") from e


def _make_trade_strict(**kwargs: Any) -> Trade:
    """
    Trade 객체 생성(STRICT).

    - Trade ctor에 없는 필드를 전달하면 즉시 예외
    - ctor 필드 탐지 실패 시 즉시 예외
    - 누락된 필수 인자는 Trade ctor/Trade.__post_init__ 에서 즉시 예외
    """
    allowed = _trade_ctor_supported_fields()
    unexpected = sorted(set(kwargs.keys()) - allowed)
    if unexpected:
        raise OrderExecutionError(
            f"unexpected Trade ctor fields (STRICT): {unexpected}"
        )
    try:
        return Trade(**kwargs)  # type: ignore[arg-type]
    except Exception as e:
        raise OrderExecutionError(f"Trade construction failed (STRICT): {e}") from e


def _ts_ms_to_datetime_utc_strict(v: Any, *, name: str) -> datetime:
    try:
        iv = int(v)
    except Exception as e:
        raise OrderExecutionError(f"{name} must be int-like epoch ms (STRICT)") from e
    if iv <= 0:
        raise OrderExecutionError(f"{name} must be > 0 epoch ms (STRICT)")
    return datetime.fromtimestamp(iv / 1000.0, tz=timezone.utc)


def _resolve_order_fill_datetime_strict(order: Dict[str, Any]) -> datetime:
    """
    Binance order payload에서 체결 시각을 strict 추출한다.

    우선순위:
    - updateTime
    - transactTime
    - time
    """
    if not isinstance(order, dict):
        raise OrderExecutionError("order must be dict for fill timestamp resolution (STRICT)")

    for key in ("updateTime", "transactTime", "time"):
        if key in order and order.get(key) is not None:
            return _ts_ms_to_datetime_utc_strict(order.get(key), name=f"order.{key}")

    raise OrderExecutionError("filled order missing updateTime/transactTime/time (STRICT)")


# ---------------------------------------------------------------------------
# Binance error helpers
# ---------------------------------------------------------------------------
_CODE_RE = re.compile(r"code=([-]?\d+)")


def _extract_code(err: Exception) -> Optional[int]:
    m = _CODE_RE.search(str(err))
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _is_timestamp_error(err: Exception) -> bool:
    s = str(err).lower()
    code = _extract_code(err)
    if code == -1021:
        return True
    if "-1021" in s:
        return True
    if "timestamp" in s and ("recvwindow" in s or "ahead of the server" in s or "behind the server" in s):
        return True
    return False


def _is_order_not_found(err: Exception) -> bool:
    s = str(err).lower()
    code = _extract_code(err)
    if code in {-2013, -2011}:
        return True
    if "order does not exist" in s:
        return True
    return False


def _is_no_need_change_margin(err: Exception) -> bool:
    s = str(err).lower()
    code = _extract_code(err)
    if code == -4046:
        return True
    if "no need to change margin type" in s:
        return True
    return False


def _is_no_need_change_leverage(err: Exception) -> bool:
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
            sync_server_time()
            return execute_with_retry(fn)
        raise


# ---------------------------------------------------------------------------
# Symbol filters: exchangeInfo cache
# ---------------------------------------------------------------------------
def _parse_symbol_filters(exchange_info: Dict[str, Any], symbol: str) -> SymbolFilters:
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
            raise RuntimeError("exchangeInfo.filters contains non-dict (STRICT)")
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
        raise RuntimeError(f"missing LOT_SIZE for symbol={symbol} (STRICT)")
    if mkt_step is None or mkt_min is None:
        raise RuntimeError(f"missing MARKET_LOT_SIZE for symbol={symbol} (STRICT)")
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


def _require_entry_fill_wait_sec(settings: Any) -> float:
    v = getattr(settings, "entry_fill_wait_sec", None)
    if v is None:
        raise ValueError("settings.entry_fill_wait_sec is required")
    return _require_positive_float(v, "settings.entry_fill_wait_sec")


def _require_position_reflect_wait_sec(settings: Any) -> float:
    v = getattr(settings, "position_reflect_wait_sec", None)
    if v is None:
        raise ValueError("settings.position_reflect_wait_sec is required")
    return _require_positive_float(v, "settings.position_reflect_wait_sec")


def _require_exit_fill_wait_sec(settings: Any) -> float:
    v = getattr(settings, "exit_fill_wait_sec", None)
    if v is None:
        raise ValueError("settings.exit_fill_wait_sec is required")
    return _require_positive_float(v, "settings.exit_fill_wait_sec")


def _require_deterministic_client_order_id_flag(settings: Any) -> bool:
    v = getattr(settings, "require_deterministic_client_order_id", None)
    if not isinstance(v, bool):
        raise ValueError("settings.require_deterministic_client_order_id must be bool (STRICT)")
    return v


def _get_optional_max_entry_slippage_pct(settings: Any) -> Optional[float]:
    if not hasattr(settings, "max_entry_slippage_pct"):
        return None
    v = getattr(settings, "max_entry_slippage_pct")
    if v is None:
        return None
    try:
        f = float(v)
    except Exception as e:
        raise ValueError("settings.max_entry_slippage_pct must be a number") from e
    if not math.isfinite(f):
        raise ValueError("settings.max_entry_slippage_pct must be finite")
    if f < 0:
        raise ValueError("settings.max_entry_slippage_pct must be >= 0")
    return float(f)


def ensure_trading_settings(symbol: str, settings: Optional[Any] = None) -> None:
    sym = _normalize_symbol(symbol)
    st = settings if settings is not None else SET
    margin_mode = _require_margin_mode(st)
    leverage = _require_leverage(st)
    desired = (margin_mode, leverage)

    with _SETTINGS_LOCK:
        cached = _SETTINGS_APPLIED.get(sym)
        if cached == desired:
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
        _SETTINGS_APPLIED[sym] = desired

    logger.info(
        "trading settings ensured (symbol=%s margin_mode=%s leverage=%s)",
        sym,
        margin_mode,
        leverage,
    )


# ---------------------------------------------------------------------------
# Idempotency helpers
# ---------------------------------------------------------------------------
def _find_open_order_by_client_id(symbol: str, client_order_id: str) -> Optional[Dict[str, Any]]:
    orders = get_open_orders(symbol)
    if not isinstance(orders, list):
        raise RuntimeError("get_open_orders returned non-list")
    for o in orders:
        if not isinstance(o, dict):
            raise RuntimeError("openOrders contains non-dict (STRICT)")
        if str(o.get("clientOrderId", "")).strip() == client_order_id:
            return o
    return None


def _find_open_order_by_id(symbol: str, order_id: str) -> Optional[Dict[str, Any]]:
    orders = get_open_orders(symbol)
    if not isinstance(orders, list):
        raise RuntimeError("get_open_orders returned non-list")
    for o in orders:
        if not isinstance(o, dict):
            raise RuntimeError("openOrders contains non-dict (STRICT)")
        oid = o.get("orderId")
        if oid is None:
            raise RuntimeError("openOrders row missing orderId (STRICT)")
        if str(oid).strip() == str(order_id).strip():
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


def _validate_existing_idempotent_order_strict(
    symbol: str,
    client_order_id: str,
    order: Dict[str, Any],
) -> Dict[str, Any]:
    if not isinstance(order, dict):
        raise RuntimeError("existing order must be dict (STRICT)")

    status = str(order.get("status") or "").upper().strip()
    if not status:
        raise OrderExecutionError(
            f"idempotency order missing status (symbol={symbol} clientOrderId={client_order_id})"
        )

    if status in _TERMINAL_ORDER_STATUSES:
        raise OrderExecutionError(
            f"idempotency conflict with terminal order "
            f"(symbol={symbol} clientOrderId={client_order_id} status={status})"
        )

    if status not in _ACTIVE_OR_FILLED_ORDER_STATUSES:
        raise OrderExecutionError(
            f"idempotency conflict with unexpected order status "
            f"(symbol={symbol} clientOrderId={client_order_id} status={status})"
        )

    return order


def _wait_for_inflight_or_existing(
    symbol: str,
    client_order_id: str,
    timeout_sec: int,
) -> Optional[Dict[str, Any]]:
    deadline = time.time() + _IDEMPOTENCY_INFLIGHT_WAIT_SEC
    while True:
        with _CLIENT_ID_LOCK:
            inflight = client_order_id in _CLIENT_ID_INFLIGHT

        existing = _find_open_order_by_client_id(symbol, client_order_id)
        if existing is not None:
            return _validate_existing_idempotent_order_strict(symbol, client_order_id, existing)

        existing2 = _get_order_by_client_id(symbol, client_order_id, timeout_sec)
        if existing2 is not None:
            return _validate_existing_idempotent_order_strict(symbol, client_order_id, existing2)

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
                "idempotency hit (inflight wait): returning existing order (symbol=%s clientOrderId=%s orderId=%s status=%s)",
                sym,
                cid,
                existing_wait.get("orderId"),
                existing_wait.get("status"),
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
            existing = _validate_existing_idempotent_order_strict(sym, cid, existing)
            logger.info(
                "idempotency hit: returning existing open order (symbol=%s clientOrderId=%s orderId=%s status=%s)",
                sym,
                cid,
                existing.get("orderId"),
                existing.get("status"),
            )
            return existing

        existing2 = _get_order_by_client_id(sym, cid, timeout_sec)
        if existing2 is not None:
            existing2 = _validate_existing_idempotent_order_strict(sym, cid, existing2)
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
            "positionSide": pos_side_u,
        }

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


# ---------------------------------------------------------------------------
# Fill / Position verification (TRADE-GRADE)
# ---------------------------------------------------------------------------
def _decimal_abs(x: Decimal) -> Decimal:
    return x if x >= 0 else -x


def _require_decimal(v: Any, *, name: str) -> Decimal:
    try:
        d = Decimal(str(v))
    except Exception as e:
        raise OrderExecutionError(f"{name} not decimal-convertible") from e
    return d


def _require_order_field_strict(od: Dict[str, Any], key: str) -> Any:
    if key not in od:
        raise OrderExecutionError(f"order missing required field: {key}")
    v = od[key]
    if v is None:
        raise OrderExecutionError(f"order field {key} is None")
    return v


def _compute_avg_price_strict(order: Dict[str, Any]) -> float:
    ap = order.get("avgPrice")
    if ap is not None:
        try:
            apf = float(ap)
        except Exception:
            apf = 0.0
        if apf > 0 and math.isfinite(apf):
            return float(apf)

    cq = _require_order_field_strict(order, "cummulativeQuoteQty")
    eq = _require_order_field_strict(order, "executedQty")
    cqf = float(cq)
    eqf = float(eq)
    if not math.isfinite(cqf) or not math.isfinite(eqf) or cqf <= 0 or eqf <= 0:
        raise OrderExecutionError("FILLED order has non-positive executedQty/cummulativeQuoteQty")
    px = cqf / eqf
    if not math.isfinite(px) or px <= 0:
        raise OrderExecutionError("computed avg price invalid")
    return float(px)


def _wait_order_filled_strict(
    *,
    symbol: str,
    order_id: str,
    settings: Any,
    expected_qty: Decimal,
    qty_step: Decimal,
    max_wait_sec: float,
) -> Tuple[Dict[str, Any], Decimal, float]:
    """
    STRICT:
    - deadline 내 FILLED 아니면 예외
    - executedQty/avgPrice 무결성 검증
    - executedQty mismatch 시 예외
    - terminal status는 즉시 예외
    """
    sym = _normalize_symbol(symbol)
    timeout_sec = _require_timeout_sec(settings)

    if not str(order_id).strip():
        raise OrderExecutionError("order_id is empty (STRICT)")
    if expected_qty <= 0:
        raise OrderExecutionError("expected_qty must be > 0 (STRICT)")
    if qty_step <= 0:
        raise OrderExecutionError("qty_step must be > 0 (STRICT)")

    deadline = time.time() + float(max_wait_sec)
    last_status = "UNKNOWN"

    while True:
        od = _call_with_time_sync_retry(
            lambda: req(
                "GET",
                "/fapi/v1/order",
                {"symbol": sym, "orderId": order_id},
                private=True,
                timeout_sec=timeout_sec,
            )
        )
        if not isinstance(od, dict):
            raise OrderExecutionError("GET /fapi/v1/order returned non-dict")

        status = str(od.get("status") or "").upper().strip()
        last_status = status

        if status in _TERMINAL_ORDER_STATUSES:
            raise OrderExecutionError(
                f"order entered terminal status before FILLED (status={status})"
            )

        if status == "FILLED":
            exec_qty = _require_decimal(
                _require_order_field_strict(od, "executedQty"),
                name="order.executedQty",
            )
            if exec_qty <= 0:
                raise OrderExecutionError("FILLED but executedQty <= 0 (STRICT)")

            diff = _decimal_abs(exec_qty - expected_qty)
            tol = qty_step / Decimal("2")
            if diff > tol:
                raise PartialFillError(
                    f"executedQty mismatch (expected={_d_to_str(expected_qty)}, got={_d_to_str(exec_qty)}, step={_d_to_str(qty_step)})"
                )

            avg_price = _compute_avg_price_strict(od)
            return od, exec_qty, avg_price

        if time.time() >= deadline:
            raise OrderFillTimeoutError(
                f"order not FILLED within {max_wait_sec}s (status={last_status})"
            )

        time.sleep(0.2)


def _verify_position_reflects_execution_strict(
    *,
    symbol: str,
    open_side: str,
    executed_qty: Decimal,
    qty_step: Decimal,
    settings: Any,
    max_wait_sec: float,
) -> Decimal:
    """
    STRICT:
    - 거래소 포지션이 체결 방향/수량을 반영하는지 확인한다.
    - 불일치 시 예외.
    """
    _ = settings
    sym = _normalize_symbol(symbol)
    side_u = _normalize_side(open_side)
    if executed_qty <= 0:
        raise PositionVerificationError("executed_qty must be > 0 (STRICT)")
    if qty_step <= 0:
        raise PositionVerificationError("qty_step must be > 0 (STRICT)")

    deadline = time.time() + float(max_wait_sec)
    last_amt: Optional[Decimal] = None

    while True:
        pos = get_position(sym)
        if not isinstance(pos, dict):
            raise PositionVerificationError("get_position returned non-dict (STRICT)")

        if "positionAmt" not in pos:
            raise PositionVerificationError("position missing positionAmt (STRICT)")

        amt = _require_decimal(pos.get("positionAmt"), name="position.positionAmt")
        last_amt = amt

        if side_u == "BUY":
            if amt > 0:
                if _decimal_abs(amt) + qty_step < executed_qty:
                    raise PositionVerificationError(
                        f"positionAmt smaller than executedQty (amt={_d_to_str(_decimal_abs(amt))}, exec={_d_to_str(executed_qty)})"
                    )
                return amt
        else:
            if amt < 0:
                if _decimal_abs(amt) + qty_step < executed_qty:
                    raise PositionVerificationError(
                        f"positionAmt smaller than executedQty (amt={_d_to_str(_decimal_abs(amt))}, exec={_d_to_str(executed_qty)})"
                    )
                return amt

        if time.time() >= deadline:
            raise PositionVerificationError(
                f"position not reflecting execution within {max_wait_sec}s (last_positionAmt={_d_to_str(last_amt) if last_amt is not None else 'None'})"
            )
        time.sleep(0.2)


def _normalize_order_status_strict(v: Any) -> str:
    s = str(v or "").upper().strip()
    if not s:
        raise ProtectionOrderVerificationError("order.status is missing (STRICT)")
    return s


def _normalize_order_position_side_from_row(order: Dict[str, Any]) -> str:
    return _normalize_position_side(order.get("positionSide"))


def _is_protection_order_row(order: Dict[str, Any]) -> bool:
    if not isinstance(order, dict):
        raise ProtectionOrderVerificationError("order must be dict (STRICT)")
    type_u = str(order.get("type") or order.get("origType") or "").upper().strip()
    reduce_only = bool(order.get("reduceOnly", False))
    close_position = bool(order.get("closePosition", False))
    is_protection_type = type_u in {"STOP_MARKET", "TAKE_PROFIT_MARKET"}
    return reduce_only or close_position or is_protection_type


def _list_open_protection_orders_strict(
    *,
    symbol: str,
    close_side: Optional[str],
    position_side: Optional[str],
) -> List[Dict[str, Any]]:
    sym = _normalize_symbol(symbol)
    pos_side_n = _normalize_position_side(position_side) if position_side is not None else None
    side_n = _normalize_side(close_side) if close_side is not None else None

    orders = get_open_orders(sym)
    if not isinstance(orders, list):
        raise ProtectionOrderVerificationError("get_open_orders returned non-list (STRICT)")

    out: List[Dict[str, Any]] = []
    for row in orders:
        if not isinstance(row, dict):
            raise ProtectionOrderVerificationError("open order row must be dict (STRICT)")

        row_symbol = _normalize_symbol(str(row.get("symbol") or ""))
        if row_symbol != sym:
            continue

        if not _is_protection_order_row(row):
            continue

        row_side = _normalize_side(str(row.get("side") or ""))
        row_pos_side = _normalize_order_position_side_from_row(row)

        if side_n is not None and row_side != side_n:
            continue
        if pos_side_n is not None and row_pos_side != pos_side_n:
            continue

        order_id = row.get("orderId")
        if order_id is None or not str(order_id).strip():
            raise ProtectionOrderVerificationError("protection order missing orderId (STRICT)")
        out.append(row)

    return out


def _wait_protection_orders_absent_strict(
    *,
    symbol: str,
    close_side: Optional[str],
    position_side: Optional[str],
) -> None:
    deadline = time.time() + _PROTECTION_CANCEL_WAIT_SEC

    while True:
        remaining = _list_open_protection_orders_strict(
            symbol=symbol,
            close_side=close_side,
            position_side=position_side,
        )
        if not remaining:
            return

        if time.time() >= deadline:
            ids = [str(r.get("orderId")) for r in remaining]
            raise ProtectionOrderVerificationError(
                f"protection orders still visible after cancel deadline (orderIds={ids})"
            )

        time.sleep(_PROTECTION_CANCEL_POLL_SEC)


def _cancel_open_protection_orders_strict(
    *,
    symbol: str,
    close_side: Optional[str],
    position_side: Optional[str],
    settings: Any,
) -> List[str]:
    rows = _list_open_protection_orders_strict(
        symbol=symbol,
        close_side=close_side,
        position_side=position_side,
    )

    canceled_ids: List[str] = []
    for row in rows:
        order_id = row.get("orderId")
        if order_id is None or not str(order_id).strip():
            raise ProtectionOrderVerificationError("protection order missing orderId during cancel (STRICT)")
        cancel_order_safe(symbol, str(order_id), settings=settings)
        canceled_ids.append(str(order_id))

    if canceled_ids:
        _wait_protection_orders_absent_strict(
            symbol=symbol,
            close_side=close_side,
            position_side=position_side,
        )

    return canceled_ids


def _fetch_order_visibility_snapshot_strict(
    *,
    symbol: str,
    order_id: Optional[str],
    client_order_id: Optional[str],
    timeout_sec: int,
) -> Optional[Dict[str, Any]]:
    sym = _normalize_symbol(symbol)

    if order_id is not None and str(order_id).strip():
        open_row = _find_open_order_by_id(sym, str(order_id))
        if open_row is not None:
            return open_row

        try:
            row = get_order(sym, str(order_id))
        except Exception as e:
            if not _is_order_not_found(e):
                raise
        else:
            if not isinstance(row, dict):
                raise ProtectionOrderVerificationError("get_order returned non-dict (STRICT)")
            return row

    if client_order_id is not None and str(client_order_id).strip():
        open_row_by_client = _find_open_order_by_client_id(sym, str(client_order_id))
        if open_row_by_client is not None:
            return open_row_by_client

        try:
            row2 = get_order_by_client_id(sym, str(client_order_id))
        except Exception as e:
            if not _is_order_not_found(e):
                raise
        else:
            if not isinstance(row2, dict):
                raise ProtectionOrderVerificationError("get_order_by_client_id returned non-dict (STRICT)")
            return row2

    _ = timeout_sec
    return None


def _validate_protection_order_shape_strict(
    *,
    order: Dict[str, Any],
    symbol: str,
    expected_side: str,
    expected_type: str,
    expected_position_side: str,
    expected_client_order_id: Optional[str],
) -> str:
    if not isinstance(order, dict):
        raise ProtectionOrderVerificationError("protection order must be dict (STRICT)")

    order_symbol = _normalize_symbol(order.get("symbol", ""))
    if order_symbol != _normalize_symbol(symbol):
        raise ProtectionOrderVerificationError(
            f"protection order symbol mismatch (STRICT): got={order_symbol} expected={symbol}"
        )

    order_id = order.get("orderId")
    if order_id is None or not str(order_id).strip():
        raise ProtectionOrderVerificationError("protection order missing orderId (STRICT)")

    side = str(order.get("side") or "").upper().strip()
    if side != _normalize_side(expected_side):
        raise ProtectionOrderVerificationError(
            f"protection order side mismatch (STRICT): got={side} expected={_normalize_side(expected_side)}"
        )

    order_type = str(order.get("type") or "").upper().strip()
    if order_type != str(expected_type).upper().strip():
        raise ProtectionOrderVerificationError(
            f"protection order type mismatch (STRICT): got={order_type} expected={str(expected_type).upper().strip()}"
        )

    status = _normalize_order_status_strict(order.get("status"))
    if status in {"CANCELED", "EXPIRED", "REJECTED"}:
        raise ProtectionOrderVerificationError(
            f"protection order terminal status (STRICT): orderId={order_id} status={status}"
        )
    if status not in {"NEW", "PARTIALLY_FILLED"}:
        raise ProtectionOrderVerificationError(
            f"unexpected protection order status (STRICT): orderId={order_id} status={status}"
        )

    position_side = str(order.get("positionSide") or "").upper().strip() or "BOTH"
    if position_side != _normalize_position_side(expected_position_side):
        raise ProtectionOrderVerificationError(
            f"protection order positionSide mismatch (STRICT): got={position_side} expected={_normalize_position_side(expected_position_side)}"
        )

    reduce_only = bool(order.get("reduceOnly", False))
    close_position = bool(order.get("closePosition", False))
    if not reduce_only and not close_position:
        raise ProtectionOrderVerificationError("protection order must be reduceOnly or closePosition (STRICT)")

    if expected_client_order_id is not None:
        cid = str(order.get("clientOrderId") or "").strip()
        if cid != str(expected_client_order_id).strip():
            raise ProtectionOrderVerificationError(
                f"protection order clientOrderId mismatch (STRICT): got={cid!r} expected={str(expected_client_order_id).strip()!r}"
            )

    return str(order_id).strip()


def _wait_protection_orders_visible_strict(
    *,
    symbol: str,
    close_side: str,
    position_side: str,
    tp_order_id: Optional[str],
    sl_order_id: Optional[str],
    tp_client_order_id: Optional[str],
    sl_client_order_id: Optional[str],
    soft_mode: bool,
    settings: Any,
) -> Tuple[Optional[str], Optional[str]]:
    timeout_sec = _require_timeout_sec(settings)
    deadline = time.time() + min(float(timeout_sec), _PROTECTION_VERIFY_WAIT_SEC)

    tp_verified_id: Optional[str] = None
    sl_verified_id: Optional[str] = None

    while True:
        if tp_order_id is not None:
            tp_row = _fetch_order_visibility_snapshot_strict(
                symbol=symbol,
                order_id=tp_order_id,
                client_order_id=tp_client_order_id,
                timeout_sec=timeout_sec,
            )
            if tp_row is not None:
                tp_verified_id = _validate_protection_order_shape_strict(
                    order=tp_row,
                    symbol=symbol,
                    expected_side=close_side,
                    expected_type="TAKE_PROFIT_MARKET",
                    expected_position_side=position_side,
                    expected_client_order_id=tp_client_order_id,
                )

        if not soft_mode and sl_order_id is not None:
            sl_row = _fetch_order_visibility_snapshot_strict(
                symbol=symbol,
                order_id=sl_order_id,
                client_order_id=sl_client_order_id,
                timeout_sec=timeout_sec,
            )
            if sl_row is not None:
                sl_verified_id = _validate_protection_order_shape_strict(
                    order=sl_row,
                    symbol=symbol,
                    expected_side=close_side,
                    expected_type="STOP_MARKET",
                    expected_position_side=position_side,
                    expected_client_order_id=sl_client_order_id,
                )

        tp_ok = (tp_order_id is None) or (tp_verified_id is not None)
        sl_ok = bool(soft_mode) or (sl_order_id is not None and sl_verified_id is not None)

        if tp_ok and sl_ok:
            return tp_verified_id, (None if soft_mode else sl_verified_id)

        if time.time() >= deadline:
            raise ProtectionOrderVerificationError(
                "protection orders not visible within deadline "
                f"(tp_visible={tp_verified_id is not None}, sl_visible={sl_verified_id is not None}, soft_mode={bool(soft_mode)})"
            )

        time.sleep(_PROTECTION_VERIFY_POLL_SEC)


def _verify_position_closed_strict(
    *,
    symbol: str,
    position_side: str,
    qty_step: Decimal,
    settings: Any,
) -> None:
    _ = settings
    if qty_step <= 0:
        raise PositionVerificationError("qty_step must be > 0 (STRICT)")

    sym = _normalize_symbol(symbol)
    pos_side_u = _normalize_position_side(position_side)
    deadline = time.time() + _POSITION_CLOSE_VERIFY_WAIT_SEC
    tol = qty_step / Decimal("2")

    while True:
        rows = fetch_open_positions(sym)
        if not isinstance(rows, list):
            raise PositionVerificationError("fetch_open_positions returned non-list (STRICT)")

        still_open = False
        last_amt: Optional[Decimal] = None

        for row in rows:
            if not isinstance(row, dict):
                raise PositionVerificationError("open position row must be dict (STRICT)")

            row_symbol = _normalize_symbol(str(row.get("symbol") or ""))
            if row_symbol != sym:
                continue

            row_pos_side = _normalize_position_side(row.get("positionSide"))
            if row_pos_side != pos_side_u:
                continue

            amt = _require_decimal(row.get("positionAmt"), name="position.positionAmt")
            last_amt = amt
            if _decimal_abs(amt) > tol:
                still_open = True
                break

        if not still_open:
            return

        if time.time() >= deadline:
            raise PositionVerificationError(
                f"position not closed within {_POSITION_CLOSE_VERIFY_WAIT_SEC}s "
                f"(positionSide={pos_side_u}, last_positionAmt={_d_to_str(last_amt) if last_amt is not None else 'None'})"
            )

        time.sleep(_POSITION_CLOSE_VERIFY_POLL_SEC)


# ---------------------------------------------------------------------------
# TP/SL
# ---------------------------------------------------------------------------
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
    _ = sl_floor_ratio

    sym = _normalize_symbol(symbol)
    open_side = _normalize_side(side_open)
    close_side = "SELL" if open_side == "BUY" else "BUY"

    qty_f = _require_positive_float(qty, "qty")
    tp_f = _require_positive_float(tp_price, "tp_price")
    if not soft_mode:
        _require_positive_float(sl_price, "sl_price")

    tp_order_id: Optional[str] = None
    sl_order_id: Optional[str] = None

    if tp_f > 0:
        tp_resp = place_conditional(
            symbol=sym,
            side=close_side,
            qty=qty_f,
            trigger_price=float(tp_f),
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
            qty=qty_f,
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
) -> "Trade":
    _ = available_usdt

    sym = _normalize_symbol(symbol)
    open_side = _normalize_side(side_open)
    source_str = _require_nonempty_source(source)

    lev = _require_leverage(settings)
    final_qty_raw = _require_positive_float(qty, "qty")

    tp_pct_f = _require_ratio_float(tp_pct, "tp_pct", allow_zero=False)
    sl_pct_f = _require_ratio_float(sl_pct, "sl_pct", allow_zero=bool(soft_mode))

    if not bool(soft_mode) and sl_pct_f <= 0.0:
        raise ValueError("sl_pct must be > 0 when soft_mode=False")

    eph = float(entry_price_hint)
    if not math.isfinite(eph) or eph <= 0.0:
        raise ValueError("entry_price_hint must be finite > 0")

    require_det = _require_deterministic_client_order_id_flag(settings)
    if entry_client_order_id is not None:
        entry_cid = _validate_client_order_id(entry_client_order_id)
    else:
        if require_det:
            raise ValueError("entry_client_order_id is required when settings.require_deterministic_client_order_id=True")
        entry_cid = _make_client_order_id(prefix="ent")

    tp_cid = _make_child_client_order_id(entry_cid, "tp")
    sl_cid = _make_child_client_order_id(entry_cid, "sl")

    # 1) 주문 전 라운딩으로 예상 수량 확정
    ensure_trading_settings(sym, settings)
    filt = get_symbol_filters(sym)
    expected_qty_dec, _, _ = _round_qty_and_price(
        filters=filt,
        order_type="MARKET",
        raw_qty=final_qty_raw,
        raw_price=None,
        raw_stop_price=None,
    )
    expected_qty_f = float(expected_qty_dec)

    # 2) ENTRY 제출
    try:
        entry_resp = place_market(
            symbol=sym,
            side=open_side,
            qty=expected_qty_f,
            settings=settings,
            reduce_only=False,
            position_side="BOTH",
            client_order_id=entry_cid,
        )
    except Exception as e:
        logger.exception(
            "entry order failed (symbol=%s side=%s qty=%s source=%s)",
            sym,
            open_side,
            expected_qty_f,
            source_str,
        )
        log_event(
            event_type="ERROR",
            symbol=sym,
            regime=source_str,
            side=_event_side_from_open_side(open_side),
            reason=str(e) or "entry_order_failed",
            extra_json={
                "open_side": open_side,
                "qty": expected_qty_f,
                "clientOrderId": entry_cid,
            },
        )
        raise OrderExecutionError("entry order submission failed") from e

    order_id = entry_resp.get("orderId") if isinstance(entry_resp, dict) else None
    if order_id is None:
        raise OrderExecutionError("entry response missing orderId (STRICT)")
    entry_order_id = str(order_id)

    # 3) ENTRY 체결 검증(FILLED + executedQty + avgPrice)
    od, exec_qty_dec, entry_price = _wait_order_filled_strict(
        symbol=sym,
        order_id=entry_order_id,
        settings=settings,
        expected_qty=expected_qty_dec,
        qty_step=filt.market_step,
        max_wait_sec=_require_entry_fill_wait_sec(settings),
    )
    executed_qty_f = float(exec_qty_dec)

    entry_ts = _resolve_order_fill_datetime_strict(od)

    if entry_price <= 0 or not math.isfinite(entry_price):
        raise OrderExecutionError("entry_price resolved invalid (<=0 or non-finite)")

    # 4) 포지션 반영 확인
    _verify_position_reflects_execution_strict(
        symbol=sym,
        open_side=open_side,
        executed_qty=exec_qty_dec,
        qty_step=filt.market_step,
        settings=settings,
        max_wait_sec=_require_position_reflect_wait_sec(settings),
    )

    # 5) 슬리피지 진단
    slip_pct = abs(entry_price - eph) / eph * 100.0
    logger.info(
        "entry slippage diag (symbol=%s hint=%s fill=%s slip_pct=%.4f clientOrderId=%s orderId=%s)",
        sym,
        eph,
        entry_price,
        slip_pct,
        entry_cid,
        entry_order_id,
    )

    max_slip = _get_optional_max_entry_slippage_pct(settings)
    if max_slip is not None and max_slip > 0 and slip_pct > max_slip:
        logger.warning(
            "slippage guard triggered (symbol=%s hint=%s fill=%s slip_pct=%.4f limit=%.4f) -> force close",
            sym,
            eph,
            entry_price,
            slip_pct,
            max_slip,
        )
        log_event(
            event_type="ERROR",
            symbol=sym,
            regime=source_str,
            side=_event_side_from_open_side(open_side),
            reason="slippage_guard_force_close",
            extra_json={
                "entry_price_hint": float(eph),
                "entry_price": float(entry_price),
                "slippage_pct": float(slip_pct),
                "max_entry_slippage_pct": float(max_slip),
                "qty": executed_qty_f,
                "entry_order_id": entry_order_id,
                "clientOrderId": entry_cid,
            },
        )
        try:
            close_position_market(
                sym,
                open_side,
                executed_qty_f,
                settings=settings,
            )
        except Exception as e2:
            logger.exception("slippage guard force close failed (symbol=%s)", sym)
            raise OrderExecutionError("slippage guard close failed") from e2
        raise OrderExecutionError("slippage guard triggered")

    # ENTRY 이벤트 기록
    log_event(
        event_type="ENTRY",
        symbol=sym,
        regime=source_str,
        side=_event_side_from_open_side(open_side),
        price=float(entry_price),
        qty=executed_qty_f,
        leverage=int(float(lev)),
        tp_pct=float(tp_pct_f),
        sl_pct=float(sl_pct_f),
        risk_pct=_get_allocation_ratio_for_log(settings),
        reason="MARKET_ENTRY_FILLED",
        extra_json={
            "open_side": open_side,
            "clientOrderId": entry_cid,
            "entry_order_id": entry_order_id,
        },
    )

    # 6) TP/SL 가격 계산
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

    # 7) TP/SL 설치 (실제 체결 수량 기준)
    close_side = "SELL" if open_side == "BUY" else "BUY"

    try:
        tp_order_id, sl_order_id = set_tp_sl(
            symbol=sym,
            side_open=open_side,
            qty=executed_qty_f,
            tp_price=float(tp_price),
            sl_price=float(sl_price),
            soft_mode=bool(soft_mode),
            sl_floor_ratio=sl_floor_ratio,
            settings=settings,
            tp_client_order_id=tp_cid,
            sl_client_order_id=sl_cid,
        )
    except Exception as e:
        logger.exception("set_tp_sl failed (symbol=%s soft_mode=%s)", sym, bool(soft_mode))
        log_event(
            event_type="ERROR",
            symbol=sym,
            regime=source_str,
            side=_event_side_from_open_side(open_side),
            reason="tp_sl_set_failed",
            extra_json={
                "err": str(e),
                "soft_mode": bool(soft_mode),
                "qty": executed_qty_f,
                "entry_order_id": entry_order_id,
                "clientOrderId": entry_cid,
            },
        )

        try:
            close_all_positions_market(sym, settings=settings)
        except Exception as e2:
            logger.exception("force close after TP/SL failure also failed (symbol=%s)", sym)
            raise OrderExecutionError("force close after TP/SL failure failed") from e2

        raise OrderExecutionError("tp/sl set failed") from e

    if tp_order_id is None or not str(tp_order_id).strip():
        raise OrderExecutionError("TP order_id is missing after set_tp_sl (STRICT)")
    if not bool(soft_mode) and (sl_order_id is None or not str(sl_order_id).strip()):
        raise OrderExecutionError("soft_mode=False but SL order_id is missing (STRICT)")

    # 8) 보호주문 실제 존재 검증
    try:
        tp_verified_id, sl_verified_id = _wait_protection_orders_visible_strict(
            symbol=sym,
            close_side=close_side,
            position_side="BOTH",
            tp_order_id=str(tp_order_id),
            sl_order_id=(str(sl_order_id) if sl_order_id is not None else None),
            tp_client_order_id=tp_cid,
            sl_client_order_id=(sl_cid if sl_order_id is not None else None),
            soft_mode=bool(soft_mode),
            settings=settings,
        )
    except Exception as e:
        logger.exception("protection order verification failed (symbol=%s soft_mode=%s)", sym, bool(soft_mode))
        log_event(
            event_type="ERROR",
            symbol=sym,
            regime=source_str,
            side=_event_side_from_open_side(open_side),
            reason="protection_order_verification_failed",
            extra_json={
                "err": str(e),
                "soft_mode": bool(soft_mode),
                "qty": executed_qty_f,
                "entry_order_id": entry_order_id,
                "tp_order_id": tp_order_id,
                "sl_order_id": sl_order_id,
                "tp_clientOrderId": tp_cid,
                "sl_clientOrderId": (sl_cid if sl_order_id is not None else None),
                "entry_clientOrderId": entry_cid,
            },
        )

        try:
            close_all_positions_market(sym, settings=settings)
        except Exception as e2:
            logger.exception("force close after protection verification failure also failed (symbol=%s)", sym)
            raise OrderExecutionError("force close after protection verification failure failed") from e2

        raise ProtectionOrderVerificationError("protection order verification failed") from e

    tp_order_id = tp_verified_id
    sl_order_id = sl_verified_id

    log_event(
        event_type="TP_SL_SET",
        symbol=sym,
        regime=source_str,
        side=_event_side_from_open_side(open_side),
        price=float(entry_price),
        qty=executed_qty_f,
        leverage=int(float(lev)),
        tp_pct=float(tp_pct_f),
        sl_pct=float(sl_pct_f),
        risk_pct=_get_allocation_ratio_for_log(settings),
        reason="TP_SL_CONFIGURED",
        extra_json={
            "soft_mode": bool(soft_mode),
            "tp_order_id": tp_order_id,
            "sl_order_id": sl_order_id,
            "tp_clientOrderId": tp_cid,
            "sl_clientOrderId": (sl_cid if sl_order_id is not None else None),
            "entry_order_id": entry_order_id,
            "entry_clientOrderId": entry_cid,
        },
    )

    now_sync = datetime.now(timezone.utc)

    return _make_trade_strict(
        symbol=sym,
        side=open_side,
        qty=executed_qty_f,
        entry=float(entry_price),
        entry_price=float(entry_price),
        entry_ts=entry_ts,
        leverage=int(float(lev)),
        source=source_str,
        tp_price=float(tp_price) if tp_price > 0 else 0.0,
        sl_price=float(sl_price) if sl_price > 0 else 0.0,
        entry_order_id=entry_order_id,
        tp_order_id=str(tp_order_id) if tp_order_id is not None else None,
        sl_order_id=str(sl_order_id) if sl_order_id is not None else None,
        client_entry_id=str(entry_cid),
        exchange_position_side="BOTH",
        remaining_qty=executed_qty_f,
        realized_pnl_usdt=0.0,
        reconciliation_status="PROTECTION_VERIFIED",
        last_synced_at=now_sync,
    )


def close_position_market(
    symbol: str,
    side_open: str,
    qty: float,
    *,
    settings: Optional[Any] = None,
) -> None:
    st = settings if settings is not None else SET
    sym = _normalize_symbol(symbol)
    open_side = _normalize_side(side_open)
    close_side = "SELL" if open_side == "BUY" else "BUY"

    ensure_trading_settings(sym, st)
    filt = get_symbol_filters(sym)

    canceled_ids = _cancel_open_protection_orders_strict(
        symbol=sym,
        close_side=close_side,
        position_side="BOTH",
        settings=st,
    )
    if canceled_ids:
        logger.info(
            "canceled protection orders before close (symbol=%s close_side=%s orderIds=%s)",
            sym,
            close_side,
            canceled_ids,
        )

    expected_qty_dec, _, _ = _round_qty_and_price(
        filters=filt,
        order_type="MARKET",
        raw_qty=_require_positive_float(qty, "qty"),
        raw_price=None,
        raw_stop_price=None,
    )
    expected_qty_f = float(expected_qty_dec)

    cid = _make_client_order_id(prefix="cls")
    resp = place_market(
        symbol=sym,
        side=close_side,
        qty=expected_qty_f,
        settings=st,
        reduce_only=True,
        position_side="BOTH",
        client_order_id=cid,
    )

    order_id = resp.get("orderId") if isinstance(resp, dict) else None
    if order_id is None:
        raise OrderExecutionError("close response missing orderId (STRICT)")

    _, _, avg_px = _wait_order_filled_strict(
        symbol=sym,
        order_id=str(order_id),
        settings=st,
        expected_qty=expected_qty_dec,
        qty_step=filt.market_step,
        max_wait_sec=_require_exit_fill_wait_sec(st),
    )

    _verify_position_closed_strict(
        symbol=sym,
        position_side="BOTH",
        qty_step=filt.market_step,
        settings=st,
    )

    log_event(
        event_type="EXIT",
        symbol=sym,
        regime="MANUAL_CLOSE",
        side=_event_side_close(),
        price=float(avg_px),
        qty=expected_qty_f,
        leverage=_require_leverage(st),
        reason="MARKET_CLOSE_FILLED",
        extra_json={
            "close_side": close_side,
            "clientOrderId": cid,
            "orderId": str(order_id),
            "canceled_protection_order_ids": canceled_ids,
        },
    )

    logger.info(
        "close market filled and verified (symbol=%s close_side=%s qty=%s orderId=%s clientOrderId=%s)",
        sym,
        close_side,
        expected_qty_f,
        order_id,
        cid,
    )


def close_all_positions_market(
    symbol: str,
    *,
    settings: Optional[Any] = None,
) -> int:
    st = settings if settings is not None else SET
    sym = _normalize_symbol(symbol)

    positions = fetch_open_positions(sym)
    if not isinstance(positions, list):
        raise RuntimeError("fetch_open_positions returned non-list")
    if not positions:
        return 0

    ensure_trading_settings(sym, st)
    filt = get_symbol_filters(sym)
    submitted = 0
    zero_tol = filt.market_step / Decimal("2")

    for p in positions:
        if not isinstance(p, dict):
            raise RuntimeError("position item is not a dict")
        if str(p.get("symbol") or "").upper() != sym:
            continue

        if "positionAmt" not in p:
            raise RuntimeError("position missing positionAmt")
        amt_dec = _require_decimal(p.get("positionAmt"), name="position.positionAmt")
        abs_amt_dec = _decimal_abs(amt_dec)

        if abs_amt_dec <= zero_tol:
            continue

        if "positionSide" not in p:
            raise RuntimeError("position missing positionSide")
        pos_side = str(p.get("positionSide") or "").upper().strip()
        if pos_side not in ("BOTH", "LONG", "SHORT"):
            raise RuntimeError(f"invalid positionSide: {pos_side!r}")

        if pos_side in ("LONG", "SHORT"):
            direction = pos_side
        else:
            direction = "LONG" if amt_dec > 0 else "SHORT"

        close_side = "SELL" if direction == "LONG" else "BUY"

        canceled_ids = _cancel_open_protection_orders_strict(
            symbol=sym,
            close_side=close_side,
            position_side=pos_side,
            settings=st,
        )
        if canceled_ids:
            logger.info(
                "canceled protection orders before force close (symbol=%s positionSide=%s close_side=%s orderIds=%s)",
                sym,
                pos_side,
                close_side,
                canceled_ids,
            )

        expected_qty_dec, _, _ = _round_qty_and_price(
            filters=filt,
            order_type="MARKET",
            raw_qty=float(abs_amt_dec),
            raw_price=None,
            raw_stop_price=None,
        )
        expected_qty_f = float(expected_qty_dec)

        client_id = _make_client_order_id(prefix="sigterm")
        logger.warning(
            "[FORCE_CLOSE] submit reduce-only market close: symbol=%s direction=%s qty=%s positionSide=%s",
            sym,
            direction,
            expected_qty_f,
            pos_side,
        )

        resp = place_market(
            symbol=sym,
            side=close_side,
            qty=expected_qty_f,
            settings=st,
            reduce_only=True,
            position_side=pos_side,
            client_order_id=client_id,
        )

        order_id = resp.get("orderId") if isinstance(resp, dict) else None
        if order_id is None:
            raise OrderExecutionError("force close response missing orderId (STRICT)")

        _, _, avg_px = _wait_order_filled_strict(
            symbol=sym,
            order_id=str(order_id),
            settings=st,
            expected_qty=expected_qty_dec,
            qty_step=filt.market_step,
            max_wait_sec=_require_exit_fill_wait_sec(st),
        )

        _verify_position_closed_strict(
            symbol=sym,
            position_side=pos_side,
            qty_step=filt.market_step,
            settings=st,
        )

        log_event(
            event_type="EXIT",
            symbol=sym,
            regime="SIGTERM_FORCE_CLOSE",
            side=_event_side_close(),
            price=float(avg_px),
            qty=expected_qty_f,
            leverage=_require_leverage(st),
            reason="SIGTERM_DEADLINE_FORCE_CLOSE_FILLED",
            extra_json={
                "direction": direction,
                "close_side": close_side,
                "positionSide": pos_side,
                "orderId": str(order_id),
                "clientOrderId": client_id,
                "canceled_protection_order_ids": canceled_ids,
            },
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