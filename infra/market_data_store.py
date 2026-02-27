"""market_data_store.py
====================================================
Auto Trader - WebSocket 시세 → Postgres 저장 모듈.

2025-11-19 변경 사항 (WS 멀티 TF / 헬스 체계와 정합)
----------------------------------------------------
1) market_data_ws / run_bot_ws 에서 넘겨주는 멀티 타임프레임 K라인
   및 depth5 오더북 포맷에 맞춰 주석과 책임 범위를 정리했다.
2) 예외 처리와 로그 포맷을 [MD-STORE] 접두어로 통일하고, 불필요한
   설명/코드를 정리해 실제 서비스에서 그대로 사용할 수 있도록 했다.
3) 이 모듈의 역할을 "시세 로그 DB" 로 한정하고, 실시간 매매 경로에는
   영향을 주지 않도록 명확히 했다.

역할
----------------------------------------------------
- WS 버퍼에서 추출한 캔들/호가를 bt_candles, bt_orderbook_snapshots 테이블에 기록한다.
- 캔들: save_candles_bulk_from_ws(...) 로 다수의 K라인을 한 번에 INSERT.
- 오더북: save_orderbook_from_ws(...) 로 depth5 스냅샷과 요약 지표
  (best bid/ask, spread, depth imbalance)를 INSERT.
- DB 오류는 로그만 남기고 항상 호출측(run_bot_ws 등)의 흐름을 막지 않는다.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from sqlalchemy.exc import SQLAlchemyError

from state.db_core import SessionLocal
from state.db_models import Candle, OrderbookSnapshot
from infra.telelog import log


# ─────────────────────────────────────────────
# 내부 유틸리티 함수
# ─────────────────────────────────────────────


def _to_utc_dt_from_ms(ts_ms: Optional[int]) -> datetime:
    """epoch ms(밀리초)를 UTC timezone-aware datetime 으로 변환.

    ts_ms 가 None/0 이면 현재 시각(UTC)을 사용해 대략적인 기록만 남긴다.
    """

    if not ts_ms:
        return datetime.now(timezone.utc)
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)


def _compute_best_and_spread(
    bids: Sequence[Sequence[float]],
    asks: Sequence[Sequence[float]],
) -> Optional[Tuple[float, float, float]]:
    """bids/asks 로부터 (best_bid, best_ask, spread) 계산.

    - bids/asks 는 [[price, qty], ...] 형식이라고 가정한다.
    - 유효하지 않은 경우(None, 빈 리스트 등)는 None 을 반환한다.
    """

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

    - bid_notional = Σ(price * qty) over bids
    - ask_notional = Σ(price * qty) over asks
    - 분모가 0 이면 None 반환.
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
        # 수치적 안전성을 위해 -1.0~1.0 범위로 클램프
        return max(min(imbalance, 1.0), -1.0)
    except Exception:
        return None


def _bids_asks_to_json(
    bids: Sequence[Sequence[float]],
    asks: Sequence[Sequence[float]],
) -> Tuple[List[Dict[str, float]], List[Dict[str, float]]]:
    """bids/asks 를 ORM 이 JSON 컬럼으로 저장하기 좋은 구조로 변환한다.

    결과 형식: [{"price": p, "qty": q}, ...]
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
    """WS 단일 캔들을 bt_candles 에 한 건 INSERT 한다.

    - 실시간 단건 저장이 필요할 때 사용할 수 있으나,
      run_bot_ws 에서는 save_candles_bulk_from_ws 를 통한 bulk 저장을 권장한다.
    """

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
    """여러 캔들을 bt_candles 에 bulk INSERT 한다.

    기대 입력 포맷 (run_bot_ws._start_market_data_store_thread 와 동일):
        {
          "symbol": "BTC-USDT",
          "interval": "1m",   # 또는 "5m","15m" 등
          "ts_ms": 1700000000000,
          "open": 12345.0,
          "high": 12360.0,
          "low": 12340.0,
          "close": 12355.0,
          "volume": 10.5,
          "quote_volume": 12345.67,   # 선택
          "source": "ws",             # 선택
        }
    """

    session = SessionLocal()
    try:
        for row in candles:
            try:
                ts_ms_val = int(row.get("ts_ms") or 0)
                ts_dt = _to_utc_dt_from_ms(ts_ms_val)

                candle = Candle(
                    symbol=str(row["symbol"]),
                    timeframe=str(row["interval"]),
                    ts=ts_dt,
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=float(row["volume"]) if row.get("volume") is not None else None,
                    quote_volume=(
                        float(row["quote_volume"]) if row.get("quote_volume") is not None else None
                    ),
                    source=str(row.get("source") or default_source),
                )
                session.add(candle)
            except Exception as inner_e:
                # 특정 행 문제는 전체 INSERT 를 막지 않도록 스킵
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
    """WS depth5 스냅샷을 bt_orderbook_snapshots 에 INSERT 한다.

    - bids/asks 는 market_data_ws.get_orderbook(...) 이 돌려주는 형태와 동일하게
      [[price, qty], ...] 구조를 기대한다.
    - bestBid / bestAsk / spread / depth_imbalance 를 함께 계산해 저장한다.
    """

    ts_dt = _to_utc_dt_from_ms(ts_ms)

    best = _compute_best_and_spread(bids, asks)
    if best is None:
        # 호가가 비어 있으면 기록할 수 있는 정보가 없으므로 스킵
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
