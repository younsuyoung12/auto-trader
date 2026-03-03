# infra/market_data_store.py
"""
====================================================
FILE: infra/market_data_store.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
====================================================

역할
----------------------------------------------------
- WS 버퍼에서 추출한 캔들/호가를 bt_candles, bt_orderbook_snapshots 테이블에 기록한다.
- 캔들: save_candles_bulk_from_ws(...) 로 다수의 K라인을 한 번에 INSERT.
- 오더북: save_orderbook_from_ws(...) 로 depth5 스냅샷과 요약 지표
  (best bid/ask, spread, depth imbalance)를 INSERT.

절대 원칙 (STRICT · NO-FALLBACK)
----------------------------------------------------
- 데이터 누락/손상/형식 오류는 즉시 예외(0/now 등 폴백 금지).
- DB 오류는 예외로 전파(로그만 남기고 계속 진행 금지).
- 부분 성공(행 단위 continue/skip) 금지: 배치 입력이 하나라도 깨지면 전체 실패.

변경 이력
----------------------------------------------------
- 2025-11-19:
  - WS 멀티 TF / 헬스 체계와 정합
  - 로그 포맷 [MD-STORE] 통일
  - "시세 로그 DB" 역할로 한정
- 2026-03-03 (TRADE-GRADE):
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


def _to_utc_dt_from_ms_strict(ts_ms: Any) -> datetime:
    """STRICT: epoch ms -> timezone-aware UTC datetime (ts_ms must be int > 0)."""
    ms = _require_int_ms(ts_ms, "ts_ms")
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def _normalize_l2_levels_strict(
    levels: Sequence[Sequence[float]],
    *,
    name: str,
) -> List[Tuple[float, float]]:
    """
    STRICT:
    - levels must be a non-empty sequence of [price, qty]
    - each price/qty must be finite float
    - price > 0, qty >= 0 (qty==0은 허용 가능하지만 보통 의미 없음)
    """
    if not isinstance(levels, (list, tuple)) or not levels:
        raise MarketDataStoreError(f"{name} must be non-empty list/tuple (STRICT)")

    out: List[Tuple[float, float]] = []
    for i, row in enumerate(levels):
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            raise MarketDataStoreError(f"{name}[{i}] must be [price, qty] (STRICT)")
        p = _require_float(row[0], f"{name}[{i}].price")
        q = _require_float(row[1], f"{name}[{i}].qty")
        if p <= 0:
            raise MarketDataStoreError(f"{name}[{i}].price must be > 0 (STRICT)")
        if q < 0:
            raise MarketDataStoreError(f"{name}[{i}].qty must be >= 0 (STRICT)")
        out.append((p, q))

    if not out:
        raise MarketDataStoreError(f"{name} normalized empty (STRICT)")
    return out


def _compute_best_and_spread_strict(
    bids: List[Tuple[float, float]],
    asks: List[Tuple[float, float]],
) -> Tuple[float, float, float]:
    """
    STRICT:
    - bids/asks must already be normalized
    - best_bid = bids[0].price, best_ask = asks[0].price
    - require best_ask > best_bid
    """
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
    # 수치적 안정 범위 클램프(연산 결과 안정화, 폴백 아님)
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
    - DB 오류는 예외 전파
    """
    sym = _require_nonempty_str(symbol, "symbol")
    tf = _require_nonempty_str(interval, "interval")
    src = _require_nonempty_str(source, "source")

    ts_dt = _to_utc_dt_from_ms_strict(ts_ms)

    c_open = _require_float(open_, "open")
    c_high = _require_float(high, "high")
    c_low = _require_float(low, "low")
    c_close = _require_float(close, "close")

    vol: Optional[float] = None
    if volume is not None:
        vol = _require_float(volume, "volume")
    qv: Optional[float] = None
    if quote_volume is not None:
        qv = _require_float(quote_volume, "quote_volume")

    try:
        with get_session() as session:
            candle = Candle(
                symbol=sym,
                timeframe=tf,
                ts=ts_dt,
                open=c_open,
                high=c_high,
                low=c_low,
                close=c_close,
                volume=vol,
                quote_volume=qv,
                source=src,
            )
            session.add(candle)
    except SQLAlchemyError as e:
        log(f"[MD-STORE][FAIL] save_candle_from_ws db_error={type(e).__name__}")
        raise
    except Exception as e:
        log(f"[MD-STORE][FAIL] save_candle_from_ws error={type(e).__name__}:{str(e)[:200]}")
        raise


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
    - DB 오류는 예외 전파
    """
    rows = list(candles)
    if not rows:
        return

    normalized: List[Candle] = []
    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            raise MarketDataStoreError(f"candles[{idx}] must be dict (STRICT)")

        sym = _require_nonempty_str(row.get("symbol"), f"candles[{idx}].symbol")
        tf = _require_nonempty_str(row.get("interval"), f"candles[{idx}].interval")
        src = _require_nonempty_str(row.get("source"), f"candles[{idx}].source")
        ts_dt = _to_utc_dt_from_ms_strict(row.get("ts_ms"))

        c_open = _require_float(row.get("open"), f"candles[{idx}].open")
        c_high = _require_float(row.get("high"), f"candles[{idx}].high")
        c_low = _require_float(row.get("low"), f"candles[{idx}].low")
        c_close = _require_float(row.get("close"), f"candles[{idx}].close")

        vol: Optional[float] = None
        if row.get("volume") is not None:
            vol = _require_float(row.get("volume"), f"candles[{idx}].volume")

        qv: Optional[float] = None
        if row.get("quote_volume") is not None:
            qv = _require_float(row.get("quote_volume"), f"candles[{idx}].quote_volume")

        normalized.append(
            Candle(
                symbol=sym,
                timeframe=tf,
                ts=ts_dt,
                open=c_open,
                high=c_high,
                low=c_low,
                close=c_close,
                volume=vol,
                quote_volume=qv,
                source=src,
            )
        )

    try:
        with get_session() as session:
            session.add_all(normalized)
    except SQLAlchemyError as e:
        log(f"[MD-STORE][FAIL] save_candles_bulk_from_ws db_error={type(e).__name__}")
        raise
    except Exception as e:
        log(f"[MD-STORE][FAIL] save_candles_bulk_from_ws error={type(e).__name__}:{str(e)[:200]}")
        raise


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
    - DB 오류는 예외 전파
    """
    sym = _require_nonempty_str(symbol, "symbol")
    ts_dt = _to_utc_dt_from_ms_strict(ts_ms)

    bids_n = _normalize_l2_levels_strict(bids, name="bids")
    asks_n = _normalize_l2_levels_strict(asks, name="asks")

    best_bid, best_ask, spread = _compute_best_and_spread_strict(bids_n, asks_n)
    depth_imb = _compute_depth_imbalance_strict(bids_n, asks_n)

    bids_json = _levels_to_json_strict(bids_n)
    asks_json = _levels_to_json_strict(asks_n)

    try:
        with get_session() as session:
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