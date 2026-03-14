"""
========================================================
FILE: infra/data_integrity_guard.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할
- WS/REST에서 들어오는 “원시 데이터” 무결성 검증(Integrity Guard).
- 캔들(Kline) / 오더북(Orderbook) 구조·값 범위·NaN/inf·시간 역전(rollback)·미래 시각을 STRICT로 차단한다.
- 이 모듈은 "검증"만 수행한다. (로그/알림/SAFE_STOP 결정은 호출자 책임)

CORE RESPONSIBILITIES
- 원시 시세 데이터 필수 필드/형태 검증
- timestamp rollback / future timestamp 차단
- 가격/수량/스프레드 관계식 검증
- entry market data bundle 계약 검증
- 공통 STRICT 검증 프리미티브(common.strict_validators)와 정합 유지

IMPORTANT POLICY
- STRICT · NO-FALLBACK · TRADE-GRADE
- 폴백 금지(None→0 등 금지)
- 데이터 누락/불일치/비정상 값 발견 시 즉시 예외
- 예외 삼키기 금지
- 환경변수 직접 접근 금지(settings.py 불필요: 순수 검증 모듈)
- DB 접근 금지(순수 함수/순수 가드)

변경 이력
--------------------------------------------------------
- 2026-03-14:
  1) FIX(ARCH): 공통 STRICT 검증 프리미티브를 common.strict_validators로 단일화
  2) FIX(CONSISTENCY): 문자열/정수ms/실수/심볼 정규화 규칙을 프로젝트 공통 계약과 정합화
  3) FIX(SAFETY): common.strict_validators 예외를 DataIntegrityError로 승격해 호출자 계약 유지
- 2026-03-04:
  1) 신규 생성: Kline/Orderbook 원시 데이터 무결성 STRICT 검증 추가
  2) 시간 역전(timestamp rollback) 즉시 차단 + 미래 timestamp 차단
  3) 숫자 finite/범위/관계식(high>=low, ask>bid 등) 강제
========================================================
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Sequence, Tuple

from common.strict_validators import (
    StrictValidationError,
    normalize_symbol as _sv_normalize_symbol,
    require_float as _sv_require_float,
    require_int_ms as _sv_require_int_ms,
    require_list as _sv_require_list,
    require_nonempty_str as _sv_require_nonempty_str,
)


class DataIntegrityError(RuntimeError):
    """원시 데이터 무결성 위반(STRICT)."""


# =============================================================================
# Core strict parsers
# =============================================================================
def _now_ms() -> int:
    return int(time.time() * 1000)


def _wrap_validation_error(where: str, exc: StrictValidationError) -> DataIntegrityError:
    return DataIntegrityError(f"{where}: {exc}")


def _require_nonempty_str(v: Any, name: str) -> str:
    try:
        return _sv_require_nonempty_str(v, name)
    except StrictValidationError as e:
        raise _wrap_validation_error(name, e) from e


def _require_int_ms(v: Any, name: str) -> int:
    try:
        return _sv_require_int_ms(v, name)
    except StrictValidationError as e:
        raise _wrap_validation_error(name, e) from e


def _require_float(v: Any, name: str) -> float:
    try:
        return _sv_require_float(v, name)
    except StrictValidationError as e:
        raise _wrap_validation_error(name, e) from e


def _require_float_min(v: Any, name: str, *, min_value: float) -> float:
    try:
        return _sv_require_float(v, name, min_value=min_value)
    except StrictValidationError as e:
        raise _wrap_validation_error(name, e) from e


def _require_list(v: Any, name: str, *, non_empty: bool = False) -> list[Any]:
    try:
        return _sv_require_list(v, name, non_empty=non_empty)
    except StrictValidationError as e:
        raise _wrap_validation_error(name, e) from e


def _normalize_symbol(symbol: Any, *, name: str = "symbol") -> str:
    try:
        return _sv_normalize_symbol(symbol, name=name)
    except StrictValidationError as e:
        raise _wrap_validation_error(name, e) from e


# =============================================================================
# Timestamp monotonic guard (rollback protection)
# =============================================================================
@dataclass
class MonotonicTsGuard:
    """
    STRICT:
    - 동일 key(stream)에서 timestamp rollback(ts 감소) 발생 시 즉시 예외.
    - 미래 timestamp는 allowed_future_ms 초과 시 즉시 예외.
    """

    allowed_future_ms: int = 5_000

    def __post_init__(self) -> None:
        if not isinstance(self.allowed_future_ms, int) or self.allowed_future_ms < 0:
            raise DataIntegrityError("allowed_future_ms must be int >= 0 (STRICT)")
        self._last: Dict[str, int] = {}

    def update(self, key: str, ts_ms: int, *, now_ms: Optional[int] = None) -> None:
        k = _require_nonempty_str(key, "key")
        ts = _require_int_ms(ts_ms, "ts_ms")
        nowv = _now_ms() if now_ms is None else _require_int_ms(now_ms, "now_ms")

        if ts > nowv + int(self.allowed_future_ms):
            raise DataIntegrityError(
                f"future timestamp detected (STRICT): key={k} ts_ms={ts} now_ms={nowv} allowed_future_ms={self.allowed_future_ms}"
            )

        prev = self._last.get(k)
        if prev is not None and ts < prev:
            raise DataIntegrityError(f"timestamp rollback detected (STRICT): key={k} prev={prev} now={ts}")
        self._last[k] = ts


# =============================================================================
# Kline validation
# =============================================================================
def validate_kline_row_strict(
    row: Sequence[Any],
    *,
    name: str,
    idx: int,
    now_ms: Optional[int] = None,
    allowed_future_ms: int = 5_000,
) -> int:
    """
    row format expected:
      [ts_ms, open, high, low, close, volume, ...]
    STRICT:
    - row shape/ts/ohlcv finite & relations enforced
    - returns parsed ts_ms
    """
    if not isinstance(row, (list, tuple)):
        raise DataIntegrityError(f"{name}[{idx}] must be list/tuple (STRICT)")
    if len(row) < 6:
        raise DataIntegrityError(f"{name}[{idx}] invalid kline row shape len<{6} (STRICT)")

    ts = _require_int_ms(row[0], f"{name}[{idx}].ts_ms")
    nowv = _now_ms() if now_ms is None else _require_int_ms(now_ms, "now_ms")
    if ts > nowv + int(allowed_future_ms):
        raise DataIntegrityError(f"{name}[{idx}] future ts_ms (STRICT): ts={ts} now={nowv}")

    o = _require_float(row[1], f"{name}[{idx}].open")
    h = _require_float(row[2], f"{name}[{idx}].high")
    l = _require_float(row[3], f"{name}[{idx}].low")
    c = _require_float(row[4], f"{name}[{idx}].close")
    v = _require_float(row[5], f"{name}[{idx}].volume")

    if h < l:
        raise DataIntegrityError(f"{name}[{idx}] high < low (STRICT)")
    if c < l or c > h:
        raise DataIntegrityError(f"{name}[{idx}] close out of range [low,high] (STRICT)")
    if v < 0:
        raise DataIntegrityError(f"{name}[{idx}] volume < 0 (STRICT)")
    if o <= 0 or h <= 0 or l <= 0 or c <= 0:
        raise DataIntegrityError(f"{name}[{idx}] price must be > 0 (STRICT)")

    return ts


def validate_kline_series_strict(
    rows: Sequence[Sequence[Any]],
    *,
    name: str,
    min_len: int,
    allow_equal_ts: bool = True,
    now_ms: Optional[int] = None,
    allowed_future_ms: int = 5_000,
) -> None:
    """
    STRICT:
    - list 존재/길이 충족
    - timestamp rollback 금지
    - 각 row 무결성 검증
    """
    if not isinstance(rows, (list, tuple)):
        raise DataIntegrityError(f"{name} must be list/tuple (STRICT)")
    if len(rows) < int(min_len):
        raise DataIntegrityError(f"{name} insufficient rows (STRICT): need>={min_len} got={len(rows)}")

    last_ts: Optional[int] = None
    for i, row in enumerate(rows):
        ts = validate_kline_row_strict(
            row,
            name=name,
            idx=i,
            now_ms=now_ms,
            allowed_future_ms=allowed_future_ms,
        )
        if last_ts is not None:
            if allow_equal_ts:
                if ts < last_ts:
                    raise DataIntegrityError(f"{name} timestamp rollback detected (STRICT): {last_ts} -> {ts}")
            else:
                if ts <= last_ts:
                    raise DataIntegrityError(f"{name} timestamp not strictly increasing (STRICT): {last_ts} -> {ts}")
        last_ts = ts


# =============================================================================
# Orderbook validation
# =============================================================================
def _require_price_qty_pair(v: Any, name: str) -> Tuple[float, float]:
    if not isinstance(v, (list, tuple)) or len(v) < 2:
        raise DataIntegrityError(f"{name} must be [price, qty] (STRICT)")
    px = _require_float_min(v[0], f"{name}.price", min_value=0.0)
    qty = _require_float_min(v[1], f"{name}.qty", min_value=0.0)
    if px <= 0:
        raise DataIntegrityError(f"{name}.price must be > 0 (STRICT)")
    if qty <= 0:
        raise DataIntegrityError(f"{name}.qty must be > 0 (STRICT)")
    return float(px), float(qty)


def _pick_best_bid_ask_strict(ob: Dict[str, Any]) -> Tuple[float, float]:
    bids = _require_list(ob.get("bids"), "orderbook.bids", non_empty=True)
    asks = _require_list(ob.get("asks"), "orderbook.asks", non_empty=True)

    best_bid_raw = ob.get("bestBid")
    best_ask_raw = ob.get("bestAsk")

    if best_bid_raw is not None:
        bb = _require_float_min(best_bid_raw, "orderbook.bestBid", min_value=0.0)
    else:
        bb, _ = _require_price_qty_pair(bids[0], "orderbook.bids[0]")

    if best_ask_raw is not None:
        ba = _require_float_min(best_ask_raw, "orderbook.bestAsk", min_value=0.0)
    else:
        ba, _ = _require_price_qty_pair(asks[0], "orderbook.asks[0]")

    if bb <= 0 or ba <= 0:
        raise DataIntegrityError("orderbook best prices must be > 0 (STRICT)")
    if ba <= bb:
        raise DataIntegrityError(f"orderbook crossed (STRICT): bestAsk={ba} <= bestBid={bb}")
    return float(bb), float(ba)


def extract_orderbook_ts_ms_strict(ob: Dict[str, Any], *, name: str = "orderbook") -> int:
    """
    STRICT:
    - ts key는 exchTs 또는 ts 중 하나가 있어야 한다.
    - 폴백(now_ms) 금지.
    """
    if not isinstance(ob, dict):
        raise DataIntegrityError(f"{name} must be dict (STRICT)")
    if "exchTs" in ob and ob.get("exchTs") is not None:
        return _require_int_ms(ob.get("exchTs"), f"{name}.exchTs")
    if "ts" in ob and ob.get("ts") is not None:
        return _require_int_ms(ob.get("ts"), f"{name}.ts")
    raise DataIntegrityError(f"{name} missing exchTs/ts (STRICT)")


def validate_orderbook_strict(
    ob: Any,
    *,
    symbol: str,
    name: str = "orderbook",
    now_ms: Optional[int] = None,
    allowed_future_ms: int = 5_000,
    require_ts: bool = True,
) -> Tuple[float, float]:
    """
    STRICT:
    - ob dict shape
    - bids/asks 존재 + 각 레벨 price/qty > 0 & finite
    - bestAsk > bestBid
    - (옵션) ts 존재 + 미래 ts 차단
    returns: (best_bid, best_ask)
    """
    sym = _normalize_symbol(symbol, name="symbol")

    if not isinstance(ob, dict) or not ob:
        raise DataIntegrityError(f"{name} must be non-empty dict (STRICT) symbol={sym}")

    if require_ts:
        ts_ms = extract_orderbook_ts_ms_strict(ob, name=name)
        nowv = _now_ms() if now_ms is None else _require_int_ms(now_ms, "now_ms")
        if ts_ms > nowv + int(allowed_future_ms):
            raise DataIntegrityError(f"{name} future ts_ms (STRICT): ts={ts_ms} now={nowv} symbol={sym}")

    bids = _require_list(ob.get("bids"), f"{name}.bids", non_empty=True)
    asks = _require_list(ob.get("asks"), f"{name}.asks", non_empty=True)

    top_n = 5
    for i in range(min(top_n, len(bids))):
        _require_price_qty_pair(bids[i], f"{name}.bids[{i}]")
    for i in range(min(top_n, len(asks))):
        _require_price_qty_pair(asks[i], f"{name}.asks[{i}]")

    bb, ba = _pick_best_bid_ask_strict(ob)
    spread = ba - bb
    if not math.isfinite(spread) or spread <= 0:
        raise DataIntegrityError(f"{name} spread invalid (STRICT): {spread} symbol={sym}")

    return float(bb), float(ba)


# =============================================================================
# Bundle validators (used by run_bot_ws / preflight)
# =============================================================================
def validate_entry_market_data_bundle_strict(market_data: Dict[str, Any]) -> None:
    """
    STRICT:
    - run_bot_ws._build_entry_market_data 결과 같은 bundle을 검증할 때 사용.
    - 이 함수는 "원시/준원시" 관점(형태/필수키/finite/롤백 가능성)만 검증한다.
      (전략 판단/리스크/실행은 다른 레이어 책임)
    """
    if not isinstance(market_data, dict) or not market_data:
        raise DataIntegrityError("market_data must be non-empty dict (STRICT)")

    symbol = _normalize_symbol(market_data.get("symbol"), name="market_data.symbol")
    _require_nonempty_str(market_data.get("direction"), "market_data.direction")
    _require_nonempty_str(market_data.get("signal_source"), "market_data.signal_source")

    ts_ms = _require_int_ms(market_data.get("signal_ts_ms"), "market_data.signal_ts_ms")
    nowv = _now_ms()
    if ts_ms > nowv + 5_000:
        raise DataIntegrityError(f"market_data.signal_ts_ms future (STRICT): {ts_ms} now={nowv}")

    last_price = _require_float_min(market_data.get("last_price"), "market_data.last_price", min_value=0.0)
    if last_price <= 0:
        raise DataIntegrityError("market_data.last_price must be > 0 (STRICT)")

    c5 = market_data.get("candles_5m")
    c5r = market_data.get("candles_5m_raw")
    if c5 is None or c5r is None:
        raise DataIntegrityError("market_data candles_5m/candles_5m_raw required (STRICT)")

    validate_kline_series_strict(c5, name="market_data.candles_5m", min_len=20)
    validate_kline_series_strict(c5r, name="market_data.candles_5m_raw", min_len=20)

    extra = market_data.get("extra")
    if extra is not None and not isinstance(extra, dict):
        raise DataIntegrityError("market_data.extra must be dict or None (STRICT)")

    mf = market_data.get("market_features")
    if mf is not None and not isinstance(mf, dict):
        raise DataIntegrityError("market_data.market_features must be dict when provided (STRICT)")

    _ = symbol


__all__ = [
    "DataIntegrityError",
    "MonotonicTsGuard",
    "validate_kline_row_strict",
    "validate_kline_series_strict",
    "extract_orderbook_ts_ms_strict",
    "validate_orderbook_strict",
    "validate_entry_market_data_bundle_strict",
]