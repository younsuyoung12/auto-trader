# infra/market_data_store.py
"""
====================================================
FILE: infra/market_data_store.py
ROLE:
- WS 버퍼에서 추출한 캔들/호가를 bt_candles, bt_orderbook_snapshots 테이블에 기록한다.
- 캔들: save_candles_bulk_from_ws(...) 로 다수의 K라인을 한 번에 INSERT.
- 오더북: save_orderbook_from_ws(...) 로 depth5 스냅샷과 요약 지표
  (best bid/ask, spread, depth imbalance)를 INSERT.

CORE RESPONSIBILITIES:
- 캔들/오더북 입력 도메인 무결성 STRICT 검증
- 배치 입력 내부 중복/역순 검증
- DB 자연키 기반 idempotent write 보장
- 동일 키 기존 row와 내용 불일치 시 즉시 예외
- DB 오류 전파

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- 데이터 누락/손상/형식 오류는 즉시 예외(0/now 등 폴백 금지)
- DB 오류는 예외로 전파(로그만 남기고 계속 진행 금지)
- 부분 성공(행 단위 continue/skip) 금지: 배치 입력이 하나라도 깨지면 전체 실패
- 단, 이미 저장된 동일 스냅샷의 재저장은 허용(no-op)하며, 같은 키 내용 불일치는 즉시 예외
- 저장 계층은 재시작에도 idempotent 해야 한다

CHANGE HISTORY:
- 2026-03-10:
  1) FIX(ROOT-CAUSE): 캔들/오더북 자연키 기반 idempotent write 추가
  2) FIX(ROOT-CAUSE): 재시작 후 동일 캔들/오더북 중복 INSERT 방지
  3) FIX(STRICT): OHLC/volume 도메인 검증 강화 (price>0, volume>=0, high/low 관계 검증)
  4) FIX(STRICT): 배치 입력 내부 duplicate / timestamp 역순 검증 추가
  5) FIX(STRICT): 같은 키 기존 row와 내용 불일치 시 즉시 예외
- 2026-03-03:
  1) 폴백 제거:
     - ts_ms None/0 → now(UTC) 사용 제거 (즉시 예외)
     - 잘못된 bids/asks 행은 continue/skip 제거 (즉시 예외)
     - best/spread 계산 실패 시 return(무시) 제거 (즉시 예외)
  2) DB 오류 전파:
     - SQLAlchemyError를 로그만 남기고 삼키는 동작 제거 → 예외 전파
  3) 트랜잭션 무결성:
     - 배치 저장 시 입력 전체를 먼저 검증 후 INSERT
     - 한 행이라도 불량이면 전체 실패(부분 커밋 금지)
====================================================
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from sqlalchemy.exc import SQLAlchemyError

from infra.telelog import log
from state.db_core import get_session
from state.db_models import Candle, OrderbookSnapshot


# ─────────────────────────────────────────────
# STRICT utilities
# ─────────────────────────────────────────────


class MarketDataStoreError(RuntimeError):
    """STRICT: market_data_store input/DB integrity error."""


def _require_nonempty_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        raise MarketDataStoreError(f"{name} is required (STRICT)")
    return s


def _normalize_symbol_strict(v: Any, name: str) -> str:
    s = _require_nonempty_str(v, name)
    out = s.replace("-", "").replace("/", "").upper().strip()
    if not out:
        raise MarketDataStoreError(f"{name} normalized empty (STRICT)")
    return out


def _normalize_interval_strict(v: Any, name: str) -> str:
    s = _require_nonempty_str(v, name)
    if s.endswith("M"):
        return s
    return s.lower()


def _require_int_ms(v: Any, name: str) -> int:
    if isinstance(v, bool):
        raise MarketDataStoreError(f"{name} must be int ms (bool not allowed)")
    try:
        iv = int(v)
    except Exception as e:
        raise MarketDataStoreError(f"{name} must be int ms (STRICT)") from e
    if iv <= 0:
        raise MarketDataStoreError(f"{name} must be > 0 (STRICT)")
    return iv


def _require_float(v: Any, name: str) -> float:
    if isinstance(v, bool):
        raise MarketDataStoreError(f"{name} must be float (bool not allowed)")
    try:
        fv = float(v)
    except Exception as e:
        raise MarketDataStoreError(f"{name} must be float (STRICT)") from e
    if fv != fv:  # NaN
        raise MarketDataStoreError(f"{name} must be finite (NaN) (STRICT)")
    if fv in (float("inf"), float("-inf")):
        raise MarketDataStoreError(f"{name} must be finite (INF) (STRICT)")
    return fv


def _require_positive_float(v: Any, name: str) -> float:
    fv = _require_float(v, name)
    if fv <= 0.0:
        raise MarketDataStoreError(f"{name} must be > 0 (STRICT)")
    return fv


def _require_nonnegative_float(v: Any, name: str) -> float:
    fv = _require_float(v, name)
    if fv < 0.0:
        raise MarketDataStoreError(f"{name} must be >= 0 (STRICT)")
    return fv


def _to_utc_dt_from_ms_strict(ts_ms: Any) -> datetime:
    """STRICT: epoch ms -> timezone-aware UTC datetime (ts_ms must be int > 0)."""
    ms = _require_int_ms(ts_ms, "ts_ms")
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def _validate_ohlc_strict(
    *,
    open_: float,
    high: float,
    low: float,
    close: float,
    name_prefix: str,
) -> None:
    if high < low:
        raise MarketDataStoreError(f"{name_prefix}.high must be >= low (STRICT)")
    if high < open_ or high < close:
        raise MarketDataStoreError(f"{name_prefix}.high must be >= open/close (STRICT)")
    if low > open_ or low > close:
        raise MarketDataStoreError(f"{name_prefix}.low must be <= open/close (STRICT)")


def _normalize_l2_levels_strict(
    levels: Sequence[Sequence[float]],
    *,
    name: str,
) -> List[Tuple[float, float]]:
    """
    STRICT:
    - levels must be a non-empty sequence of [price, qty]
    - each price/qty must be finite float
    - price > 0, qty >= 0
    - price levels must be unique
    """
    if not isinstance(levels, (list, tuple)) or not levels:
        raise MarketDataStoreError(f"{name} must be non-empty list/tuple (STRICT)")

    out: List[Tuple[float, float]] = []
    seen_prices: set[float] = set()

    for i, row in enumerate(levels):
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            raise MarketDataStoreError(f"{name}[{i}] must be [price, qty] (STRICT)")
        p = _require_positive_float(row[0], f"{name}[{i}].price")
        q = _require_nonnegative_float(row[1], f"{name}[{i}].qty")
        if p in seen_prices:
            raise MarketDataStoreError(f"{name}[{i}].price duplicated within snapshot (STRICT)")
        seen_prices.add(p)
        out.append((p, q))

    if not out:
        raise MarketDataStoreError(f"{name} normalized empty (STRICT)")
    return out


def _validate_book_ordering_strict(
    bids: List[Tuple[float, float]],
    asks: List[Tuple[float, float]],
) -> None:
    for i in range(1, len(bids)):
        if bids[i - 1][0] <= bids[i][0]:
            raise MarketDataStoreError("bids must be strictly descending by price (STRICT)")
    for i in range(1, len(asks)):
        if asks[i - 1][0] >= asks[i][0]:
            raise MarketDataStoreError("asks must be strictly ascending by price (STRICT)")


def _compute_best_and_spread_strict(
    bids: List[Tuple[float, float]],
    asks: List[Tuple[float, float]],
) -> Tuple[float, float, float]:
    """
    STRICT:
    - bids/asks must already be normalized and ordered
    - best_bid = bids[0].price, best_ask = asks[0].price
    - require best_ask > best_bid
    """
    _validate_book_ordering_strict(bids, asks)

    best_bid = float(bids[0][0])
    best_ask = float(asks[0][0])
    if best_ask <= best_bid:
        raise MarketDataStoreError(f"orderbook crossed/invalid (best_ask={best_ask} <= best_bid={best_bid}) (STRICT)")
    spread = best_ask - best_bid
    if spread <= 0:
        raise MarketDataStoreError("spread must be > 0 (STRICT)")
    return best_bid, best_ask, spread


def _compute_depth_imbalance_strict(
    bids: List[Tuple[float, float]],
    asks: List[Tuple[float, float]],
) -> float:
    """
    STRICT depth imbalance:
    (bid_notional - ask_notional) / (bid_notional + ask_notional)

    - 분모가 0이면 예외(폴백 금지)
    """
    bid_notional = 0.0
    for p, q in bids:
        bid_notional += p * q

    ask_notional = 0.0
    for p, q in asks:
        ask_notional += p * q

    total = bid_notional + ask_notional
    if total <= 0:
        raise MarketDataStoreError("depth imbalance denominator must be > 0 (STRICT)")

    imbalance = (bid_notional - ask_notional) / total
    if imbalance > 1.0:
        imbalance = 1.0
    if imbalance < -1.0:
        imbalance = -1.0
    return float(imbalance)


def _levels_to_json_strict(
    levels: List[Tuple[float, float]],
) -> List[Dict[str, float]]:
    """STRICT: normalized (price, qty) -> [{"price": p, "qty": q}, ...]."""
    return [{"price": float(p), "qty": float(q)} for (p, q) in levels]


def _float_equal_strict(a: Optional[float], b: Optional[float], *, tol: float = 1e-12) -> bool:
    if a is None and b is None:
        return True
    if (a is None) != (b is None):
        return False
    assert a is not None and b is not None
    return abs(float(a) - float(b)) <= tol


def _require_same_candle_content_strict(existing: Candle, incoming: Dict[str, Any]) -> None:
    existing_symbol = _normalize_symbol_strict(existing.symbol, "existing.symbol")
    incoming_symbol = _normalize_symbol_strict(incoming["symbol"], "incoming.symbol")
    if existing_symbol != incoming_symbol:
        raise MarketDataStoreError("existing candle symbol mismatch (STRICT)")

    existing_tf = _normalize_interval_strict(existing.timeframe, "existing.timeframe")
    incoming_tf = _normalize_interval_strict(incoming["interval"], "incoming.interval")
    if existing_tf != incoming_tf:
        raise MarketDataStoreError("existing candle timeframe mismatch (STRICT)")

    if existing.ts != incoming["ts"]:
        raise MarketDataStoreError("existing candle ts mismatch (STRICT)")

    existing_source = _require_nonempty_str(existing.source, "existing.source")
    incoming_source = _require_nonempty_str(incoming["source"], "incoming.source")
    if existing_source != incoming_source:
        raise MarketDataStoreError("existing candle source mismatch (STRICT)")

    pairs = (
        ("open", float(existing.open), float(incoming["open"])),
        ("high", float(existing.high), float(incoming["high"])),
        ("low", float(existing.low), float(incoming["low"])),
        ("close", float(existing.close), float(incoming["close"])),
    )
    for name, a, b in pairs:
        if not _float_equal_strict(a, b):
            raise MarketDataStoreError(f"existing candle content mismatch: {name} (STRICT)")

    existing_vol = None if existing.volume is None else float(existing.volume)
    incoming_vol = incoming["volume"]
    if not _float_equal_strict(existing_vol, incoming_vol):
        raise MarketDataStoreError("existing candle content mismatch: volume (STRICT)")

    existing_qv = None if existing.quote_volume is None else float(existing.quote_volume)
    incoming_qv = incoming["quote_volume"]
    if not _float_equal_strict(existing_qv, incoming_qv):
        raise MarketDataStoreError("existing candle content mismatch: quote_volume (STRICT)")


def _require_same_orderbook_content_strict(existing: OrderbookSnapshot, incoming: Dict[str, Any]) -> None:
    existing_symbol = _normalize_symbol_strict(existing.symbol, "existing.symbol")
    incoming_symbol = _normalize_symbol_strict(incoming["symbol"], "incoming.symbol")
    if existing_symbol != incoming_symbol:
        raise MarketDataStoreError("existing orderbook symbol mismatch (STRICT)")

    if existing.ts != incoming["ts"]:
        raise MarketDataStoreError("existing orderbook ts mismatch (STRICT)")

    scalar_pairs = (
        ("best_bid", float(existing.best_bid), float(incoming["best_bid"])),
        ("best_ask", float(existing.best_ask), float(incoming["best_ask"])),
        ("spread", float(existing.spread), float(incoming["spread"])),
        ("depth_imbalance", float(existing.depth_imbalance), float(incoming["depth_imbalance"])),
    )
    for name, a, b in scalar_pairs:
        if not _float_equal_strict(a, b):
            raise MarketDataStoreError(f"existing orderbook content mismatch: {name} (STRICT)")

    existing_bids = list(existing.bids_raw or [])
    existing_asks = list(existing.asks_raw or [])
    if existing_bids != incoming["bids_raw"]:
        raise MarketDataStoreError("existing orderbook content mismatch: bids_raw (STRICT)")
    if existing_asks != incoming["asks_raw"]:
        raise MarketDataStoreError("existing orderbook content mismatch: asks_raw (STRICT)")


def _normalize_candle_row_strict(row: Dict[str, Any], *, idx: str) -> Dict[str, Any]:
    if not isinstance(row, dict):
        raise MarketDataStoreError(f"{idx} must be dict (STRICT)")

    sym = _normalize_symbol_strict(row.get("symbol"), f"{idx}.symbol")
    tf = _normalize_interval_strict(row.get("interval"), f"{idx}.interval")
    src = _require_nonempty_str(row.get("source"), f"{idx}.source")
    ts_dt = _to_utc_dt_from_ms_strict(row.get("ts_ms"))

    c_open = _require_positive_float(row.get("open"), f"{idx}.open")
    c_high = _require_positive_float(row.get("high"), f"{idx}.high")
    c_low = _require_positive_float(row.get("low"), f"{idx}.low")
    c_close = _require_positive_float(row.get("close"), f"{idx}.close")
    _validate_ohlc_strict(
        open_=c_open,
        high=c_high,
        low=c_low,
        close=c_close,
        name_prefix=idx,
    )

    vol: Optional[float] = None
    if row.get("volume") is not None:
        vol = _require_nonnegative_float(row.get("volume"), f"{idx}.volume")

    qv: Optional[float] = None
    if row.get("quote_volume") is not None:
        qv = _require_nonnegative_float(row.get("quote_volume"), f"{idx}.quote_volume")

    return {
        "symbol": sym,
        "interval": tf,
        "source": src,
        "ts": ts_dt,
        "open": c_open,
        "high": c_high,
        "low": c_low,
        "close": c_close,
        "volume": vol,
        "quote_volume": qv,
    }


def _validate_candle_batch_integrity_strict(rows: List[Dict[str, Any]]) -> None:
    seen_natural_keys: set[Tuple[str, str, datetime, str]] = set()
    per_bucket_last_ts: Dict[Tuple[str, str, str], datetime] = {}

    for idx, row in enumerate(rows):
        natural_key = (row["symbol"], row["interval"], row["ts"], row["source"])
        if natural_key in seen_natural_keys:
            raise MarketDataStoreError(
                f"duplicate candle natural key in batch (STRICT): "
                f"symbol={row['symbol']} interval={row['interval']} ts={row['ts'].isoformat()} source={row['source']}"
            )
        seen_natural_keys.add(natural_key)

        bucket = (row["symbol"], row["interval"], row["source"])
        prev_ts = per_bucket_last_ts.get(bucket)
        if prev_ts is not None and row["ts"] <= prev_ts:
            raise MarketDataStoreError(
                f"candle batch ts must be strictly increasing within bucket (STRICT): "
                f"bucket={bucket} prev_ts={prev_ts.isoformat()} now_ts={row['ts'].isoformat()} idx={idx}"
            )
        per_bucket_last_ts[bucket] = row["ts"]


# ─────────────────────────────────────────────
# Candle 저장
# ─────────────────────────────────────────────


def save_candle_from_ws(
    *,
    symbol: str,
    interval: str,
    ts_ms: int,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: Optional[float] = None,
    quote_volume: Optional[float] = None,
    source: str,
) -> None:
    """
    STRICT 단일 캔들 INSERT.

    - ts_ms는 반드시 int>0
    - symbol/interval/source는 반드시 non-empty
    - DB 자연키 기준 idempotent write
    - DB 오류는 예외 전파
    """
    save_candles_bulk_from_ws(
        [
            {
                "symbol": symbol,
                "interval": interval,
                "ts_ms": ts_ms,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "quote_volume": quote_volume,
                "source": source,
            }
        ]
    )


def save_candles_bulk_from_ws(
    candles: Iterable[Dict[str, Any]],
) -> None:
    """
    STRICT bulk INSERT.

    기대 입력 포맷:
        {
          "symbol": "...",
          "interval": "...",
          "ts_ms": 1700000000000,
          "open": ...,
          "high": ...,
          "low": ...,
          "close": ...,
          "volume": ... (optional),
          "quote_volume": ... (optional),
          "source": "...",  # REQUIRED (STRICT)
        }

    STRICT:
    - 한 행이라도 누락/불량이면 전체 실패(부분 commit 금지)
    - 배치 내부 duplicate / 역순 금지
    - DB 자연키(symbol, timeframe, ts, source) 기준 idempotent write
    - 같은 자연키 기존 row와 내용 불일치 시 즉시 예외
    - DB 오류는 예외 전파
    """
    rows_raw = list(candles)
    if not rows_raw:
        return

    rows = [_normalize_candle_row_strict(row, idx=f"candles[{idx}]") for idx, row in enumerate(rows_raw)]
    _validate_candle_batch_integrity_strict(rows)

    try:
        with get_session() as session:
            existing_rows = (
                session.query(Candle)
                .filter(
                    tuple_(Candle.symbol, Candle.timeframe, Candle.ts, Candle.source).in_(
                        [(r["symbol"], r["interval"], r["ts"], r["source"]) for r in rows]
                    )
                )
                .all()
            )

            existing_by_key: Dict[Tuple[str, str, datetime, str], Candle] = {}
            for row in existing_rows:
                key = (
                    _normalize_symbol_strict(row.symbol, "existing.symbol"),
                    _normalize_interval_strict(row.timeframe, "existing.timeframe"),
                    row.ts,
                    _require_nonempty_str(row.source, "existing.source"),
                )
                if key in existing_by_key:
                    raise MarketDataStoreError(f"duplicate existing candle natural key in DB (STRICT): {key}")
                existing_by_key[key] = row

            to_insert: List[Candle] = []
            for row in rows:
                key = (row["symbol"], row["interval"], row["ts"], row["source"])
                existing = existing_by_key.get(key)
                if existing is not None:
                    _require_same_candle_content_strict(existing, row)
                    continue

                to_insert.append(
                    Candle(
                        symbol=row["symbol"],
                        timeframe=row["interval"],
                        ts=row["ts"],
                        open=row["open"],
                        high=row["high"],
                        low=row["low"],
                        close=row["close"],
                        volume=row["volume"],
                        quote_volume=row["quote_volume"],
                        source=row["source"],
                    )
                )

            if to_insert:
                session.add_all(to_insert)
    except SQLAlchemyError as e:
        log(f"[MD-STORE][FAIL] save_candles_bulk_from_ws db_error={type(e).__name__}")
        raise
    except Exception as e:
        log(f"[MD-STORE][FAIL] save_candles_bulk_from_ws error={type(e).__name__}:{str(e)[:200]}")
        raise


# SQLAlchemy tuple_ import 위치 고정(ORM query helper)
from sqlalchemy import tuple_  # noqa: E402


# ─────────────────────────────────────────────
# Orderbook(depth5) 저장
# ─────────────────────────────────────────────


def save_orderbook_from_ws(
    *,
    symbol: str,
    ts_ms: int,
    bids: Sequence[Sequence[float]],
    asks: Sequence[Sequence[float]],
) -> None:
    """
    STRICT depth5 스냅샷 INSERT.

    - bids/asks 형식/값이 불량하면 즉시 예외(부분 무시/스킵 금지)
    - DB 자연키(symbol, ts) 기준 idempotent write
    - 같은 자연키 기존 row와 내용 불일치 시 즉시 예외
    - DB 오류는 예외 전파
    """
    sym = _normalize_symbol_strict(symbol, "symbol")
    ts_dt = _to_utc_dt_from_ms_strict(ts_ms)

    bids_n = _normalize_l2_levels_strict(bids, name="bids")
    asks_n = _normalize_l2_levels_strict(asks, name="asks")

    best_bid, best_ask, spread = _compute_best_and_spread_strict(bids_n, asks_n)
    depth_imb = _compute_depth_imbalance_strict(bids_n, asks_n)

    bids_json = _levels_to_json_strict(bids_n)
    asks_json = _levels_to_json_strict(asks_n)

    incoming = {
        "symbol": sym,
        "ts": ts_dt,
        "best_bid": float(best_bid),
        "best_ask": float(best_ask),
        "spread": float(spread),
        "depth_imbalance": float(depth_imb),
        "bids_raw": bids_json,
        "asks_raw": asks_json,
    }

    try:
        with get_session() as session:
            existing = (
                session.query(OrderbookSnapshot)
                .filter(OrderbookSnapshot.symbol == sym)
                .filter(OrderbookSnapshot.ts == ts_dt)
                .one_or_none()
            )

            if existing is not None:
                _require_same_orderbook_content_strict(existing, incoming)
                return

            ob = OrderbookSnapshot(
                symbol=sym,
                ts=ts_dt,
                best_bid=float(best_bid),
                best_ask=float(best_ask),
                spread=float(spread),
                depth_imbalance=float(depth_imb),
                bids_raw=bids_json,
                asks_raw=asks_json,
            )
            session.add(ob)
    except SQLAlchemyError as e:
        log(f"[MD-STORE][FAIL] save_orderbook_from_ws db_error={type(e).__name__}")
        raise
    except Exception as e:
        log(f"[MD-STORE][FAIL] save_orderbook_from_ws error={type(e).__name__}:{str(e)[:200]}")
        raise


__all__ = [
    "MarketDataStoreError",
    "save_candle_from_ws",
    "save_candles_bulk_from_ws",
    "save_orderbook_from_ws",
]