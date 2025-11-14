"""
market_data_store.py
====================================================
BingX Auto Trader - WebSocket 시세 → Postgres 저장 모듈.

[변경 요약 / 2025-11-14 최초 작성]
----------------------------------------------------
1) 단일 캔들 저장(save_candle_from_ws) 구현
   - WS epoch ms → UTC datetime 변환 후 bt_candles INSERT
2) 여러 캔들 bulk 저장(save_candles_bulk_from_ws) 구현
3) 오더북(depth5) 저장(save_orderbook_from_ws) 구현
   - best_bid / best_ask / spread / depth_imbalance 계산 후 저장
4) SessionLocal() 사용 → commit/rollback/close 보장
5) WS 실시간 매매 성능에 영향 없도록 예외는 로그만 남기고 흐름 유지

[역할]
----------------------------------------------------
- market_data_ws / run_bot_ws / entry_guards_ws 에서 넘겨준
  1m·5m·15m 캔들 + depth5 오더북을
  Postgres 테이블(bt_candles, bt_orderbook_snapshots)에 기록.
- 실시간 진입·청산 로직은 기존 웹소켓 메모리 버퍼 기준 그대로 사용.
- 이 모듈은 분석/대시보드/백테스트용 기록(Log DB) 역할만 담당.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from sqlalchemy.exc import SQLAlchemyError

from db_core import SessionLocal
from db_models import Candle, OrderbookSnapshot
from telelog import log

# ─────────────────────────────────────────────
# 내부 유틸리티 함수
# ─────────────────────────────────────────────

def _to_utc_dt_from_ms(ts_ms: Optional[int]) -> datetime:
    """epoch ms(밀리초)를 UTC timezone-aware DateTime 으로 변환."""
    if not ts_ms:
        return datetime.now(timezone.utc)
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)


def _compute_best_and_spread(
    bids: Sequence[Sequence[float]],
    asks: Sequence[Sequence[float]],
) -> Optional[Tuple[float, float, float]]:
    """bids/asks 로부터 (best_bid, best_ask, spread) 계산."""
    if not bids or not asks:
        return None
    try:
        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
        spread = best_ask - best_bid
        return best_bid, best_ask, spread
    except Exception:
        return None


def _compute_depth_imbalance(
    bids: Sequence[Sequence[float]],
    asks: Sequence[Sequence[float]],
) -> Optional[float]:
    """단순 depth 비대칭성 지표 계산.
    (bid_notional - ask_notional) / (bid_notional + ask_notional)
    """
    try:
        bid_notional = 0.0
        for row in bids:
            if len(row) < 2:
                continue
            p, q = float(row[0]), float(row[1])
            bid_notional += p * q

        ask_notional = 0.0
        for row in asks:
            if len(row) < 2:
                continue
            p, q = float(row[0]), float(row[1])
            ask_notional += p * q

        total = bid_notional + ask_notional
        if total <= 0:
            return None

        imbalance = (bid_notional - ask_notional) / total
        return max(min(imbalance, 1.0), -1.0)
    except Exception:
        return None


def _bids_asks_to_json(
    bids: Sequence[Sequence[float]],
    asks: Sequence[Sequence[float]],
) -> Tuple[List[Dict[str, float]], List[Dict[str, float]]]:
    """bids/asks 를 JSON 변환.
    [{"price": p, "qty": q}, ...] 형태.
    """
    bids_json: List[Dict[str, float]] = []
    asks_json: List[Dict[str, float]] = []

    for row in bids:
        if len(row) < 2:
            continue
        try:
            price, qty = float(row[0]), float(row[1])
            bids_json.append({"price": price, "qty": qty})
        except Exception:
            continue

    for row in asks:
        if len(row) < 2:
            continue
        try:
            price, qty = float(row[0]), float(row[1])
            asks_json.append({"price": price, "qty": qty})
        except Exception:
            continue

    return bids_json, asks_json


# ─────────────────────────────────────────────
# 공개 함수: 캔들 저장
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
    source: str = "ws",
) -> None:
    """WS 단일 캔들 → bt_candles INSERT"""
    ts_dt = _to_utc_dt_from_ms(ts_ms)

    session = SessionLocal()
    try:
        candle = Candle(
            symbol=symbol,
            timeframe=interval,
            ts=ts_dt,
            open=open_,
            high=high,
            low=low,
            close=close,
            volume=volume,
            quote_volume=quote_volume,
            source=source,
        )
        session.add(candle)
        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        log(f"[MD-STORE] save_candle_from_ws error: {e}")
    finally:
        session.close()


def save_candles_bulk_from_ws(
    candles: Iterable[Dict[str, Any]],
    *,
    default_source: str = "ws",
) -> None:
    """여러 캔들을 bulk INSERT"""
    session = SessionLocal()
    try:
        for row in candles:
            try:
                ts_dt = _to_utc_dt_from_ms(int(row.get("ts_ms") or 0))
                candle = Candle(
                    symbol=str(row["symbol"]),
                    timeframe=str(row["interval"]),
                    ts=ts_dt,
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=float(row["volume"]) if row.get("volume") is not None else None,
                    quote_volume=float(row["quote_volume"]) if row.get("quote_volume") is not None else None,
                    source=str(row.get("source") or default_source),
                )
                session.add(candle)
            except Exception as inner_e:
                log(f"[MD-STORE] save_candles_bulk_from_ws skip one row: {inner_e}")
                continue
        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        log(f"[MD-STORE] save_candles_bulk_from_ws error: {e}")
    finally:
        session.close()


# ─────────────────────────────────────────────
# 공개 함수: 오더북(depth5) 저장
# ─────────────────────────────────────────────

def save_orderbook_from_ws(
    *,
    symbol: str,
    ts_ms: int,
    bids: Sequence[Sequence[float]],
    asks: Sequence[Sequence[float]],
) -> None:
    """WS depth5 → bt_orderbook_snapshots INSERT"""
    ts_dt = _to_utc_dt_from_ms(ts_ms)

    best = _compute_best_and_spread(bids, asks)
    if best is None:
        return
    best_bid, best_ask, spread = best

    depth_imb = _compute_depth_imbalance(bids, asks)
    bids_json, asks_json = _bids_asks_to_json(bids, asks)

    session = SessionLocal()
    try:
        ob = OrderbookSnapshot(
            symbol=symbol,
            ts=ts_dt,
            best_bid=best_bid,
            best_ask=best_ask,
            spread=spread,
            depth_imbalance=depth_imb,
            bids_raw=bids_json,
            asks_raw=asks_json,
        )
        session.add(ob)
        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        log(f"[MD-STORE] save_orderbook_from_ws error: {e}")
    finally:
        session.close()
__all__ = [
    "save_candle_from_ws",
    "save_candles_bulk_from_ws",
    "save_orderbook_from_ws",
]
