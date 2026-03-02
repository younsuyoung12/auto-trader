"""
========================================================
state/db_writer.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
설계 원칙:
- Render Postgres(TRADER_DB_URL)만 사용한다.
- DB 쓰기 실패 시 예외를 반드시 전파한다.
- 폴백/조용한 return 금지.
- 입력값은 fail-fast 검증한다.

변경 이력
--------------------------------------------------------
- 2026-03-01: bt_trades 실제 스키마 정합
  - open: entry_ts/entry_price/qty/is_auto/... 기록, trade_id 반환
  - close: exit_ts/exit_price/pnl_usdt/... 업데이트, trade_id 반환

- 2026-03-02 (PATCH)
  - bt_trade_snapshots에 DD 영속화 필드 저장 지원:
    * equity_current_usdt
    * equity_peak_usdt
    * dd_pct
  - STRICT:
    - 값이 전달되면 반드시 finite/범위 검증
    - ORM 모델에 컬럼 매핑이 없으면 즉시 예외(조용히 무시 금지)
========================================================
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import desc

from state.db_core import get_session
from state.db_models import (
    ExternalEvent,
    FundingRate,
    TradeExitSnapshot,
    TradeORM,
    TradeSnapshot,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _require_nonempty_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        raise ValueError(f"{name} is required")
    return s


def _require_tzaware_dt(v: Any, name: str) -> datetime:
    if not isinstance(v, datetime):
        raise ValueError(f"{name} must be datetime")
    if v.tzinfo is None or v.tzinfo.utcoffset(v) is None:
        raise ValueError(f"{name} must be timezone-aware (tzinfo required)")
    return v


def _require_number(v: Any, name: str) -> float:
    if v is None:
        raise ValueError(f"{name} is required")
    try:
        x = float(v)
    except Exception as e:
        raise ValueError(f"{name} must be numeric: {e}") from e
    if not math.isfinite(x):
        raise ValueError(f"{name} must be finite")
    return x


def _require_positive(v: Any, name: str) -> float:
    x = _require_number(v, name)
    if x <= 0:
        raise ValueError(f"{name} must be > 0")
    return x


def _require_positive_int(v: Any, name: str) -> int:
    if v is None:
        raise ValueError(f"{name} is required")
    try:
        iv = int(v)
    except Exception as e:
        raise ValueError(f"{name} must be int: {e}") from e
    if iv <= 0:
        raise ValueError(f"{name} must be > 0")
    return iv


def _opt_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        x = float(v)
    except Exception as e:
        raise ValueError(f"optional float invalid: {e}") from e
    if not math.isfinite(x):
        raise ValueError("optional float must be finite")
    return x


def _opt_float_min(v: Any, name: str, *, min_value: float) -> Optional[float]:
    if v is None:
        return None
    x = _require_number(v, name)
    if x < min_value:
        raise ValueError(f"{name} must be >= {min_value}")
    return float(x)


def _opt_float_range(v: Any, name: str, *, min_value: float, max_value: float) -> Optional[float]:
    if v is None:
        return None
    x = _require_number(v, name)
    if x < min_value or x > max_value:
        raise ValueError(f"{name} must be within {min_value}..{max_value}")
    return float(x)


# ─────────────────────────────────────────────
# Trades (bt_trades)
# ─────────────────────────────────────────────
def record_trade_open_returning_id(
    *,
    symbol: str,
    side: str,  # LONG/SHORT 권장
    qty: float,
    entry_price: float,
    entry_ts: datetime,
    is_auto: bool = True,
    regime_at_entry: Optional[str] = None,
    strategy: Optional[str] = None,
    entry_score: Optional[float] = None,
    trend_score_at_entry: Optional[float] = None,
    range_score_at_entry: Optional[float] = None,
    leverage: Optional[float] = None,
    risk_pct: Optional[float] = None,
    tp_pct: Optional[float] = None,
    sl_pct: Optional[float] = None,
    note: Optional[str] = None,
) -> int:
    sym = _require_nonempty_str(symbol, "symbol").upper()
    sd = _require_nonempty_str(side, "side").upper()
    q = _require_positive(qty, "qty")
    ep = _require_positive(entry_price, "entry_price")
    ets = _require_tzaware_dt(entry_ts, "entry_ts")

    now = _utc_now()

    with get_session() as session:
        row = TradeORM(
            symbol=sym,
            side=sd,
            entry_ts=ets,
            entry_price=ep,
            qty=q,
            is_auto=bool(is_auto),
            regime_at_entry=(str(regime_at_entry).strip() if regime_at_entry else None),
            strategy=(str(strategy).strip() if strategy else None),
            entry_score=_opt_float(entry_score),
            trend_score_at_entry=_opt_float(trend_score_at_entry),
            range_score_at_entry=_opt_float(range_score_at_entry),
            leverage=_opt_float(leverage),
            risk_pct=_opt_float(risk_pct),
            tp_pct=_opt_float(tp_pct),
            sl_pct=_opt_float(sl_pct),
            note=(str(note).strip()[:255] if note else None),
            created_at=now,
            updated_at=now,
        )
        session.add(row)
        session.flush()
        if not getattr(row, "id", None):
            raise RuntimeError("failed to obtain trade_id after flush")
        return int(row.id)


def close_latest_open_trade_returning_id(
    *,
    symbol: str,
    side: str,
    exit_price: float,
    exit_ts: datetime,
    pnl_usdt: float,
    close_reason: str,
    regime_at_exit: Optional[str] = None,
    pnl_pct_futures: Optional[float] = None,
    pnl_pct_spot_ref: Optional[float] = None,
    note: Optional[str] = None,
) -> int:
    sym = _require_nonempty_str(symbol, "symbol").upper()
    sd = _require_nonempty_str(side, "side").upper()
    xp = _require_positive(exit_price, "exit_price")
    xts = _require_tzaware_dt(exit_ts, "exit_ts")
    pnl_val = _require_number(pnl_usdt, "pnl_usdt")
    reason = _require_nonempty_str(close_reason, "close_reason")

    now = _utc_now()

    with get_session() as session:
        q = (
            session.query(TradeORM)
            .filter(
                TradeORM.symbol == sym,
                TradeORM.side == sd,
                TradeORM.exit_ts.is_(None),
            )
            .order_by(desc(TradeORM.entry_ts))
        )
        trade = q.first()
        if trade is None:
            raise RuntimeError(f"no open trade to close: symbol={sym}, side={sd}")

        trade.exit_price = float(xp)
        trade.exit_ts = xts
        trade.pnl_usdt = float(pnl_val)
        trade.close_reason = str(reason)[:32]

        if regime_at_exit:
            trade.regime_at_exit = str(regime_at_exit).strip()[:16]
        if pnl_pct_futures is not None:
            trade.pnl_pct_futures = float(pnl_pct_futures)
        if pnl_pct_spot_ref is not None:
            trade.pnl_pct_spot_ref = float(pnl_pct_spot_ref)

        if note:
            trade.note = str(note).strip()[:255]

        trade.updated_at = now

        session.flush()
        return int(trade.id)


# ─────────────────────────────────────────────
# Snapshots (bt_trade_snapshots / bt_trade_exit_snapshots)
# ─────────────────────────────────────────────
def record_trade_snapshot(
    *,
    trade_id: int,
    symbol: str,
    entry_ts: datetime,
    direction: str,
    signal_source: Optional[str] = None,
    regime: Optional[str] = None,
    entry_score: Optional[float] = None,
    engine_total: Optional[float] = None,
    trend_strength: Optional[float] = None,
    atr_pct: Optional[float] = None,
    volume_zscore: Optional[float] = None,
    depth_ratio: Optional[float] = None,
    spread_pct: Optional[float] = None,
    hour_kst: Optional[int] = None,
    weekday_kst: Optional[int] = None,
    last_price: Optional[float] = None,
    risk_pct: Optional[float] = None,
    tp_pct: Optional[float] = None,
    sl_pct: Optional[float] = None,
    gpt_action: Optional[str] = None,
    gpt_reason: Optional[str] = None,
    # 2026-03-02: DD 영속화 필드
    equity_current_usdt: Optional[float] = None,
    equity_peak_usdt: Optional[float] = None,
    dd_pct: Optional[float] = None,
) -> None:
    tid = _require_positive_int(trade_id, "trade_id")
    sym = _require_nonempty_str(symbol, "symbol").upper()
    ets = _require_tzaware_dt(entry_ts, "entry_ts")
    dirn = _require_nonempty_str(direction, "direction").upper()

    eq_cur = _opt_float_min(equity_current_usdt, "equity_current_usdt", min_value=0.0)
    eq_peak = _opt_float_min(equity_peak_usdt, "equity_peak_usdt", min_value=0.0)
    dd_v = _opt_float_range(dd_pct, "dd_pct", min_value=0.0, max_value=100.0)

    with get_session() as session:
        t = session.query(TradeORM).filter(TradeORM.id == tid).first()
        if t is None:
            raise RuntimeError(f"trade not found: trade_id={tid}")

        existed = session.query(TradeSnapshot).filter(TradeSnapshot.trade_id == tid).first()
        if existed is not None:
            raise RuntimeError(f"trade snapshot already exists: trade_id={tid}")

        row = TradeSnapshot(
            trade_id=tid,
            symbol=sym,
            entry_ts=ets,
            direction=dirn,
            signal_source=(str(signal_source).strip() if signal_source else None),
            regime=(str(regime).strip() if regime else None),
            entry_score=_opt_float(entry_score),
            engine_total=_opt_float(engine_total),
            trend_strength=_opt_float(trend_strength),
            atr_pct=_opt_float(atr_pct),
            volume_zscore=_opt_float(volume_zscore),
            depth_ratio=_opt_float(depth_ratio),
            spread_pct=_opt_float(spread_pct),
            hour_kst=int(hour_kst) if hour_kst is not None else None,
            weekday_kst=int(weekday_kst) if weekday_kst is not None else None,
            last_price=float(last_price) if last_price is not None else None,
            risk_pct=_opt_float(risk_pct),
            tp_pct=_opt_float(tp_pct),
            sl_pct=_opt_float(sl_pct),
            gpt_action=(str(gpt_action).strip() if gpt_action else None),
            gpt_reason=(str(gpt_reason).strip() if gpt_reason else None),
            created_at=_utc_now(),
        )

        # STRICT: 모델 매핑 누락이면 즉시 예외
        if eq_cur is not None:
            if not hasattr(row, "equity_current_usdt"):
                raise RuntimeError("TradeSnapshot model missing equity_current_usdt mapping (db_models mismatch)")
            setattr(row, "equity_current_usdt", float(eq_cur))

        if eq_peak is not None:
            if not hasattr(row, "equity_peak_usdt"):
                raise RuntimeError("TradeSnapshot model missing equity_peak_usdt mapping (db_models mismatch)")
            setattr(row, "equity_peak_usdt", float(eq_peak))

        if dd_v is not None:
            if not hasattr(row, "dd_pct"):
                raise RuntimeError("TradeSnapshot model missing dd_pct mapping (db_models mismatch)")
            setattr(row, "dd_pct", float(dd_v))

        session.add(row)


def record_trade_exit_snapshot(
    *,
    trade_id: int,
    symbol: str,
    close_ts: datetime,
    close_price: float,
    pnl: Optional[float] = None,
    close_reason: Optional[str] = None,
    exit_atr_pct: Optional[float] = None,
    exit_trend_strength: Optional[float] = None,
    exit_volume_zscore: Optional[float] = None,
    exit_depth_ratio: Optional[float] = None,
) -> None:
    tid = _require_positive_int(trade_id, "trade_id")
    sym = _require_nonempty_str(symbol, "symbol").upper()
    cts = _require_tzaware_dt(close_ts, "close_ts")
    cp = _require_positive(close_price, "close_price")

    with get_session() as session:
        t = session.query(TradeORM).filter(TradeORM.id == tid).first()
        if t is None:
            raise RuntimeError(f"trade not found: trade_id={tid}")

        existed = session.query(TradeExitSnapshot).filter(TradeExitSnapshot.trade_id == tid).first()
        if existed is not None:
            raise RuntimeError(f"trade exit snapshot already exists: trade_id={tid}")

        row = TradeExitSnapshot(
            trade_id=tid,
            symbol=sym,
            close_ts=cts,
            close_price=float(cp),
            pnl=float(pnl) if pnl is not None else None,
            close_reason=(str(close_reason).strip() if close_reason else None),
            exit_atr_pct=_opt_float(exit_atr_pct),
            exit_trend_strength=_opt_float(exit_trend_strength),
            exit_volume_zscore=_opt_float(exit_volume_zscore),
            exit_depth_ratio=_opt_float(exit_depth_ratio),
            created_at=_utc_now(),
        )
        session.add(row)


# ─────────────────────────────────────────────
# Funding / Events
# ─────────────────────────────────────────────
def record_funding_rate(
    *,
    symbol: str,
    ts: datetime,
    rate: float,
    mark_price: Optional[float] = None,
) -> None:
    sym = _require_nonempty_str(symbol, "symbol").upper()
    ts_dt = _require_tzaware_dt(ts, "ts")
    rate_val = _require_number(rate, "rate")
    mp = float(mark_price) if mark_price is not None else None

    with get_session() as session:
        fr = FundingRate(
            symbol=sym,
            ts=ts_dt,
            rate=rate_val,
            mark_price=mp,
            created_at=_utc_now(),
        )
        session.add(fr)


def record_external_event(
    *,
    event_ts: datetime,
    event_type: str,
    symbol: str,
    impact: str,
    details: Optional[dict[str, Any]] = None,
) -> None:
    ets = _require_tzaware_dt(event_ts, "event_ts")
    etype = _require_nonempty_str(event_type, "event_type")
    sym = _require_nonempty_str(symbol, "symbol").upper()
    imp = _require_nonempty_str(impact, "impact")

    if details is not None and not isinstance(details, dict):
        raise ValueError("details must be dict or None")

    with get_session() as session:
        ev = ExternalEvent(
            event_ts=ets,
            event_type=etype,
            symbol=sym,
            impact=imp,
            details=details,
            created_at=_utc_now(),
        )
        session.add(ev)


__all__ = [
    "record_trade_open_returning_id",
    "close_latest_open_trade_returning_id",
    "record_trade_snapshot",
    "record_trade_exit_snapshot",
    "record_funding_rate",
    "record_external_event",
]