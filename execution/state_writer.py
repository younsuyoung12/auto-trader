"""
========================================================
execution/state_writer.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
설계 원칙:
- bt_trades INSERT 및 trade.db_id 연결만 담당한다.
- DB 세션 생성 실패/commit 실패는 즉시 RuntimeError.
- 폴백/예외 삼키기 절대 금지.
========================================================
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Optional

from state.db_core import SessionLocal
from state.db_models import Trade as TradeORM
from state.trader_state import Trade


def _as_float(value: Any, name: str, *, min_value: Optional[float] = None) -> float:
    try:
        if isinstance(value, bool):
            raise TypeError("bool is not allowed")
        v = float(value)
    except Exception as e:
        raise ValueError(f"{name} must be a number") from e

    if not math.isfinite(v):
        raise ValueError(f"{name} must be finite")

    if min_value is not None and v < min_value:
        raise ValueError(f"{name} must be >= {min_value}")

    return v


def insert_trade_row(
    *,
    trade: Trade,
    symbol: str,
    ts_ms: int,
    regime_at_entry: str,
    strategy: str,
    risk_pct: float,
    tp_pct: float,
    sl_pct: float,
    leverage: float,
) -> None:
    """
    bt_trades INSERT 후 trade.db_id로 연결.
    """
    if not isinstance(ts_ms, int) or ts_ms <= 0:
        raise ValueError("ts_ms must be int > 0")

    entry_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

    side = str(getattr(trade, "side", "")).upper()
    if side not in ("BUY", "SELL", "LONG", "SHORT"):
        # 기존 구현체 호환: BUY/SELL 또는 LONG/SHORT 가능
        raise ValueError(f"trade.side invalid: {side!r}")

    entry_price = getattr(trade, "entry", None)
    if entry_price is None:
        entry_price = getattr(trade, "entry_price", None)

    qty = getattr(trade, "qty", None)

    entry_price_f = _as_float(entry_price, "trade.entry_price", min_value=0.0)
    qty_f = _as_float(qty, "trade.qty", min_value=0.0)

    orm = TradeORM(
        symbol=str(symbol),
        side=side,
        entry_ts=entry_dt,
        entry_price=entry_price_f,
        qty=qty_f,
        is_auto=True,
        regime_at_entry=str(regime_at_entry),
        strategy=str(strategy),
        leverage=float(leverage),
        risk_pct=float(risk_pct),
        tp_pct=float(tp_pct),
        sl_pct=float(sl_pct),
        note="",
        created_at=entry_dt,
        updated_at=entry_dt,
    )

    try:
        session = SessionLocal()
    except Exception as e:
        raise RuntimeError(f"DB SessionLocal create failed: {e}") from e

    try:
        session.add(orm)
        session.commit()

        trade_id = getattr(orm, "id", None)
        if trade_id is None:
            raise RuntimeError("bt_trades insert succeeded but orm.id is None")

        # trade 객체에 db_id 연결
        setattr(trade, "db_id", int(trade_id))

    except Exception as e:
        session.rollback()
        raise RuntimeError(f"bt_trades insert failed: {e}") from e
    finally:
        session.close()