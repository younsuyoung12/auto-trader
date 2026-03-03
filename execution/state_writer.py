# execution/state_writer.py
"""
========================================================
FILE: execution/state_writer.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================
설계 원칙
- bt_trades INSERT 및 trade.db_id 연결만 담당한다.
- DB 세션 생성 실패/commit 실패는 즉시 예외.
- 폴백/예외 삼키기 절대 금지.
- DB 접근은 state.db_core.get_session() 단일 경로로 통일한다(SessionLocal 직접 사용 금지).
- 민감정보(DB URL 등)는 예외/로그에 포함하지 않는다.

변경 이력
--------------------------------------------------------
- 2026-03-03 (TRADE-GRADE):
  1) SessionLocal 직접 사용 제거 → db_core.get_session()로 통일
  2) 입력 검증 강화(STRICT):
     - ts_ms/symbol/side/entry/qty/leverage/risk_pct/tp_pct/sl_pct 필수 및 범위 검증
     - LONG/SHORT 입력은 BUY/SELL로 정규화(저장 일관성)
     - entry vs entry_price 불일치 시 예외(상태 오염 방지)
  3) trade.db_id 설정 실패를 숨기지 않음(즉시 예외)
========================================================
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy.exc import SQLAlchemyError

from state.db_core import get_session
from state.db_models import Trade as TradeORM
from state.trader_state import Trade


class StateWriterError(RuntimeError):
    """STRICT: state_writer failure."""


def _require_nonempty_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        raise StateWriterError(f"{name} is required (STRICT)")
    return s


def _require_int_ms(v: Any, name: str) -> int:
    if v is None:
        raise StateWriterError(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        raise StateWriterError(f"{name} must be int ms (bool not allowed) (STRICT)")
    try:
        iv = int(v)
    except Exception as e:
        raise StateWriterError(f"{name} must be int ms (STRICT)") from e
    if iv <= 0:
        raise StateWriterError(f"{name} must be > 0 (STRICT)")
    return iv


def _require_float(value: Any, name: str, *, min_value: Optional[float] = None, max_value: Optional[float] = None) -> float:
    if value is None:
        raise StateWriterError(f"{name} is required (STRICT)")
    if isinstance(value, bool):
        raise StateWriterError(f"{name} must be a number (bool not allowed) (STRICT)")
    try:
        v = float(value)
    except Exception as e:
        raise StateWriterError(f"{name} must be a number (STRICT)") from e

    if not math.isfinite(v):
        raise StateWriterError(f"{name} must be finite (STRICT)")

    if min_value is not None and v < min_value:
        raise StateWriterError(f"{name} must be >= {min_value} (STRICT)")
    if max_value is not None and v > max_value:
        raise StateWriterError(f"{name} must be <= {max_value} (STRICT)")

    return float(v)


def _normalize_side_for_db(side: Any) -> str:
    s = _require_nonempty_str(side, "trade.side").upper()
    if s in ("BUY", "SELL"):
        return s
    if s == "LONG":
        return "BUY"
    if s == "SHORT":
        return "SELL"
    raise StateWriterError(f"trade.side invalid (STRICT): {s!r}")


def _resolve_entry_price_strict(trade: Trade) -> float:
    """
    STRICT:
    - trade.entry 와 trade.entry_price 중 하나는 반드시 존재/유효해야 한다.
    - 둘 다 유효하면 값이 일치해야 한다(오염 방지).
    """
    e1 = getattr(trade, "entry", None)
    e2 = getattr(trade, "entry_price", None)

    v1 = None if e1 is None else _require_float(e1, "trade.entry", min_value=0.0)
    v2 = None if e2 is None else _require_float(e2, "trade.entry_price", min_value=0.0)

    if v1 is None and v2 is None:
        raise StateWriterError("trade.entry/trade.entry_price missing (STRICT)")

    if v1 is not None and v1 <= 0:
        raise StateWriterError("trade.entry must be > 0 (STRICT)")
    if v2 is not None and v2 <= 0:
        raise StateWriterError("trade.entry_price must be > 0 (STRICT)")

    if v1 is not None and v2 is not None:
        if abs(float(v1) - float(v2)) > 1e-12:
            raise StateWriterError(f"trade.entry and trade.entry_price mismatch (STRICT): {v1} != {v2}")
        return float(v1)

    return float(v1 if v1 is not None else v2)


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

    STRICT:
    - 입력 누락/불량 즉시 예외
    - DB 실패 즉시 예외
    """
    if not isinstance(trade, Trade):
        raise StateWriterError("trade must be Trade (STRICT)")

    ts_ms_i = _require_int_ms(ts_ms, "ts_ms")
    entry_dt = datetime.fromtimestamp(ts_ms_i / 1000.0, tz=timezone.utc)

    sym = _require_nonempty_str(symbol, "symbol").upper().replace("-", "").replace("/", "").replace("_", "")
    if any(ch.isspace() for ch in sym):
        raise StateWriterError("symbol contains whitespace (STRICT)")

    side_db = _normalize_side_for_db(getattr(trade, "side", None))

    entry_price_f = _resolve_entry_price_strict(trade)
    qty_f = _require_float(getattr(trade, "qty", None), "trade.qty", min_value=0.0)
    if qty_f <= 0:
        raise StateWriterError("trade.qty must be > 0 (STRICT)")

    lev_f = _require_float(leverage, "leverage", min_value=1.0)
    rp = _require_float(risk_pct, "risk_pct", min_value=0.0, max_value=1.0)
    tp = _require_float(tp_pct, "tp_pct", min_value=0.0, max_value=1.0)
    sl = _require_float(sl_pct, "sl_pct", min_value=0.0, max_value=1.0)

    if rp <= 0:
        raise StateWriterError("risk_pct must be > 0 for ENTER (STRICT)")
    if tp <= 0:
        raise StateWriterError("tp_pct must be > 0 (STRICT)")
    if sl <= 0:
        raise StateWriterError("sl_pct must be > 0 (STRICT)")

    reg = _require_nonempty_str(regime_at_entry, "regime_at_entry")
    strat = _require_nonempty_str(strategy, "strategy")

    orm = TradeORM(
        symbol=str(sym),
        side=str(side_db),
        entry_ts=entry_dt,
        entry_price=float(entry_price_f),
        qty=float(qty_f),
        is_auto=True,
        regime_at_entry=str(reg),
        strategy=str(strat),
        leverage=float(lev_f),
        risk_pct=float(rp),
        tp_pct=float(tp),
        sl_pct=float(sl),
        note="",
        created_at=entry_dt,
        updated_at=entry_dt,
    )

    try:
        with get_session() as session:
            session.add(orm)
            session.flush()  # id 확보(STRICT)
            trade_id = getattr(orm, "id", None)
            if trade_id is None:
                raise StateWriterError("bt_trades insert succeeded but orm.id is None (STRICT)")
            # trade 객체에 db_id 연결 (실패 숨김 금지)
            setattr(trade, "db_id", int(trade_id))
    except SQLAlchemyError as e:
        # 민감정보 금지: DB URL/SQL 전체 출력 금지
        raise RuntimeError(f"bt_trades insert failed (SQLAlchemyError): {type(e).__name__}") from e
    except Exception:
        raise