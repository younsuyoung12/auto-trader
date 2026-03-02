"""
========================================================
FILE: state/db_models.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
설계 원칙:
- Base는 state.db_core.Base 단일 소스를 사용한다.
- 테이블명/컬럼명은 DB 스키마(bt_*)와 완전히 동일하게 유지한다.
- timezone-aware(TIMESTAMPTZ) 컬럼은 timezone=True 로 유지한다.
- JSON 컬럼은 Postgres JSON/JSONB 타입을 그대로 사용한다.
- import 시점에 스키마 생성/변경을 수행하지 않는다.
- 레거시 호환: 일부 모듈이 `Trade` ORM 심벌을 기대하므로 `Trade = TradeORM` alias를 제공한다.
- 폴백 금지: 누락/불일치/모호성은 추정으로 메우지 않는다(즉시 예외/중단).

PATCH NOTES — 2026-03-03 (PATCH)
--------------------------------------------------------
- bt_trade_snapshots(TradeSnapshot) ORM 정합 수정:
  - equity_current_usdt, equity_peak_usdt, dd_pct 컬럼 추가
  - 실제 DB(bt_trade_snapshots) 스키마와 1:1 정합 확보
- bt_trades(TradeORM) 안정성 보강:
  - updated_at onupdate=_utc_now 추가(ORM 레벨 자동 갱신)

PATCH NOTES — 2026-03-02
--------------------------------------------------------
- bt_trades: 운영형 실행/복구를 위한 컬럼 추가(ORM 정합)
  - entry_order_id, tp_order_id, sl_order_id
  - exchange_position_side
  - remaining_qty, realized_pnl_usdt
  - reconciliation_status, last_synced_at
- 기존 설계 원칙/STRICT 정책 유지 (폴백/추정 금지)
========================================================
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSON, JSONB

from state.db_core import Base


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ─────────────────────────────────────────────
# bt_candles
# ─────────────────────────────────────────────
class Candle(Base):
    __tablename__ = "bt_candles"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(32), nullable=False, index=True)
    timeframe = Column(String(8), nullable=False, index=True)
    ts = Column(DateTime(timezone=True), nullable=False, index=True)

    open = Column(Numeric(24, 8), nullable=False)
    high = Column(Numeric(24, 8), nullable=False)
    low = Column(Numeric(24, 8), nullable=False)
    close = Column(Numeric(24, 8), nullable=False)

    volume = Column(Numeric(24, 8), nullable=True)
    quote_volume = Column(Numeric(24, 8), nullable=True)

    source = Column(String(20), nullable=False, default="ws")
    created_at = Column(DateTime(timezone=True), nullable=False, default=_utc_now)

    __table_args__ = (
        Index("ix_bt_candles_symbol_tf_ts", "symbol", "timeframe", "ts", unique=True),
    )


# ─────────────────────────────────────────────
# bt_orderbook_snapshots
# ─────────────────────────────────────────────
class OrderbookSnapshot(Base):
    __tablename__ = "bt_orderbook_snapshots"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(32), nullable=False, index=True)
    ts = Column(DateTime(timezone=True), nullable=False, index=True)

    best_bid = Column(Numeric(24, 8), nullable=False)
    best_ask = Column(Numeric(24, 8), nullable=False)
    spread = Column(Numeric(24, 8), nullable=False)
    depth_imbalance = Column(Numeric(24, 8), nullable=True)

    bids_raw = Column(JSONB, nullable=True)
    asks_raw = Column(JSONB, nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, default=_utc_now)

    __table_args__ = (
        Index("ix_bt_ob_symbol_ts", "symbol", "ts"),
    )


# ─────────────────────────────────────────────
# bt_indicators
# ─────────────────────────────────────────────
class Indicator(Base):
    __tablename__ = "bt_indicators"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(32), nullable=False, index=True)
    timeframe = Column(String(8), nullable=False, index=True)
    ts = Column(DateTime(timezone=True), nullable=False, index=True)

    rsi = Column(Numeric(24, 8), nullable=True)
    ema_fast = Column(Numeric(24, 8), nullable=True)
    ema_slow = Column(Numeric(24, 8), nullable=True)
    macd = Column(Numeric(24, 8), nullable=True)
    macd_signal = Column(Numeric(24, 8), nullable=True)
    macd_hist = Column(Numeric(24, 8), nullable=True)
    atr = Column(Numeric(24, 8), nullable=True)
    bb_upper = Column(Numeric(24, 8), nullable=True)
    bb_middle = Column(Numeric(24, 8), nullable=True)
    bb_lower = Column(Numeric(24, 8), nullable=True)

    extra = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=_utc_now)

    __table_args__ = (
        Index("ix_bt_indicators_symbol_tf_ts", "symbol", "timeframe", "ts", unique=True),
    )


# ─────────────────────────────────────────────
# bt_regime_scores
# ─────────────────────────────────────────────
class RegimeScore(Base):
    __tablename__ = "bt_regime_scores"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(32), nullable=False, index=True)
    ts = Column(DateTime(timezone=True), nullable=False, index=True)

    trend_score = Column(Float, nullable=True)
    range_score = Column(Float, nullable=True)
    chop_score = Column(Float, nullable=True)
    event_risk_score = Column(Float, nullable=True)
    funding_bias_score = Column(Float, nullable=True)

    final_regime = Column(String(16), nullable=False)
    details_json = Column(JSON, nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, default=_utc_now)

    timeframe = Column(String(8), nullable=True, index=True)
    regime = Column(String, nullable=True)
    score = Column(Float, nullable=True)
    details = Column(JSONB, nullable=True)

    __table_args__ = (
        Index("ix_bt_regime_symbol_tf_ts", "symbol", "timeframe", "ts"),
    )


# ─────────────────────────────────────────────
# bt_entry_scores
# (주의: 실제 DB는 trade_id FK가 존재함. 여기 포함)
# ─────────────────────────────────────────────
class EntryScore(Base):
    __tablename__ = "bt_entry_scores"

    id = Column(Integer, primary_key=True, index=True)

    trade_id = Column(Integer, ForeignKey("bt_trades.id"), nullable=True, index=True)

    symbol = Column(String(32), nullable=False, index=True)
    timeframe = Column(String(8), nullable=False, index=True)
    ts = Column(DateTime(timezone=True), nullable=False, index=True)

    direction = Column(String(10), nullable=False)  # LONG / SHORT
    score = Column(Numeric(24, 8), nullable=False)

    features = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=_utc_now)

    __table_args__ = (
        Index("ix_bt_entry_symbol_tf_ts_dir", "symbol", "timeframe", "ts", "direction", unique=True),
    )


# ─────────────────────────────────────────────
# bt_trades (실제 DB 스키마 1:1 정합)
# ─────────────────────────────────────────────
class TradeORM(Base):
    __tablename__ = "bt_trades"

    id = Column(Integer, primary_key=True, index=True)

    symbol = Column(String(32), nullable=False, index=True)
    side = Column(String(8), nullable=False)  # LONG/SHORT 또는 BUY/SELL (운영에서는 LONG/SHORT 권장)

    entry_ts = Column(DateTime(timezone=True), nullable=False)
    exit_ts = Column(DateTime(timezone=True), nullable=True)

    entry_price = Column(Numeric(24, 8), nullable=False)
    exit_price = Column(Numeric(24, 8), nullable=True)

    qty = Column(Numeric(24, 8), nullable=False)

    pnl_usdt = Column(Numeric(24, 8), nullable=True)
    pnl_pct_futures = Column(Float, nullable=True)
    pnl_pct_spot_ref = Column(Float, nullable=True)

    is_auto = Column(Boolean, nullable=False)

    regime_at_entry = Column(String(16), nullable=True)
    regime_at_exit = Column(String(16), nullable=True)

    entry_score = Column(Float, nullable=True)
    trend_score_at_entry = Column(Float, nullable=True)
    range_score_at_entry = Column(Float, nullable=True)

    strategy = Column(String(16), nullable=True)
    close_reason = Column(String(32), nullable=True)

    leverage = Column(Float, nullable=True)
    risk_pct = Column(Float, nullable=True)
    tp_pct = Column(Float, nullable=True)
    sl_pct = Column(Float, nullable=True)

    note = Column(String(255), nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, default=_utc_now)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=_utc_now, onupdate=_utc_now)

    # ─────────────────────────────────────────
    # Execution / Reconciliation Fields (운영형)
    # ─────────────────────────────────────────
    entry_order_id = Column(String(64), nullable=True)
    tp_order_id = Column(String(64), nullable=True)
    sl_order_id = Column(String(64), nullable=True)

    exchange_position_side = Column(String(16), nullable=True)  # e.g. BOTH / LONG / SHORT (거래소 정책에 따름)
    remaining_qty = Column(Numeric(24, 8), nullable=True)
    realized_pnl_usdt = Column(Numeric(24, 8), nullable=True)

    reconciliation_status = Column(String(32), nullable=True)  # e.g. OK / MISMATCH / RECOVERED / ERROR
    last_synced_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_bt_trades_symbol_entry_ts", "symbol", "entry_ts"),
    )


Trade = TradeORM


# ─────────────────────────────────────────────
# bt_trade_snapshots (진입 스냅샷) - 우리가 생성한 테이블
# ─────────────────────────────────────────────
class TradeSnapshot(Base):
    __tablename__ = "bt_trade_snapshots"

    id = Column(Integer, primary_key=True, index=True)
    trade_id = Column(Integer, ForeignKey("bt_trades.id", ondelete="CASCADE"), nullable=False)

    symbol = Column(String(32), nullable=False, index=True)
    entry_ts = Column(DateTime(timezone=True), nullable=False, index=True)
    direction = Column(String(10), nullable=False)

    signal_source = Column(String(32), nullable=True)
    regime = Column(String(32), nullable=True)

    entry_score = Column(Float, nullable=True)
    engine_total = Column(Float, nullable=True)

    trend_strength = Column(Float, nullable=True)
    atr_pct = Column(Float, nullable=True)
    volume_zscore = Column(Float, nullable=True)
    depth_ratio = Column(Float, nullable=True)
    spread_pct = Column(Float, nullable=True)

    hour_kst = Column(Integer, nullable=True)
    weekday_kst = Column(Integer, nullable=True)

    last_price = Column(Numeric(24, 8), nullable=True)
    risk_pct = Column(Float, nullable=True)
    tp_pct = Column(Float, nullable=True)
    sl_pct = Column(Float, nullable=True)

    gpt_action = Column(String(16), nullable=True)
    gpt_reason = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, default=_utc_now)

    # ─────────────────────────────────────────
    # Equity / DD (DB 스키마 정합)
    # ─────────────────────────────────────────
    equity_current_usdt = Column(Float, nullable=True)
    equity_peak_usdt = Column(Float, nullable=True)
    dd_pct = Column(Float, nullable=True)

    __table_args__ = (
        Index("ux_bt_trade_snapshots_tradeid", "trade_id", unique=True),
        Index("ix_bt_trade_snapshots_symbol_entryts", "symbol", "entry_ts"),
    )


# ─────────────────────────────────────────────
# bt_trade_exit_snapshots (청산 스냅샷) - 우리가 생성한 테이블
# ─────────────────────────────────────────────
class TradeExitSnapshot(Base):
    __tablename__ = "bt_trade_exit_snapshots"

    id = Column(Integer, primary_key=True, index=True)
    trade_id = Column(Integer, ForeignKey("bt_trades.id", ondelete="CASCADE"), nullable=False)

    symbol = Column(String(32), nullable=False)
    close_ts = Column(DateTime(timezone=True), nullable=False)
    close_price = Column(Numeric(24, 8), nullable=False)

    pnl = Column(Numeric(24, 8), nullable=True)
    close_reason = Column(String(200), nullable=True)

    exit_atr_pct = Column(Float, nullable=True)
    exit_trend_strength = Column(Float, nullable=True)
    exit_volume_zscore = Column(Float, nullable=True)
    exit_depth_ratio = Column(Float, nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, default=_utc_now)

    __table_args__ = (
        Index("ux_bt_trade_exit_tradeid", "trade_id", unique=True),
        Index("ix_bt_trade_exit_symbol_ts", "symbol", "close_ts"),
    )


# ─────────────────────────────────────────────
# bt_funding_rates
# ─────────────────────────────────────────────
class FundingRate(Base):
    __tablename__ = "bt_funding_rates"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(32), nullable=False, index=True)
    ts = Column(DateTime(timezone=True), nullable=False, index=True)

    rate = Column(Numeric(24, 8), nullable=False)
    mark_price = Column(Numeric(24, 8), nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, default=_utc_now)

    __table_args__ = (
        Index("ix_bt_funding_symbol_ts", "symbol", "ts", unique=True),
    )


# ─────────────────────────────────────────────
# bt_external_events
# ─────────────────────────────────────────────
class ExternalEvent(Base):
    __tablename__ = "bt_external_events"

    id = Column(Integer, primary_key=True, index=True)

    event_ts = Column(DateTime(timezone=True), nullable=False, index=True)
    event_type = Column(String(40), nullable=False, index=True)
    symbol = Column(String(32), nullable=False, index=True)

    impact = Column(String(20), nullable=False)
    details = Column(JSONB, nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, default=_utc_now)

    __table_args__ = (
        Index("ix_bt_ext_event_ts_type", "event_ts", "event_type"),
    )


__all__ = [
    "Candle",
    "OrderbookSnapshot",
    "Indicator",
    "RegimeScore",
    "EntryScore",
    "TradeORM",
    "Trade",
    "TradeSnapshot",
    "TradeExitSnapshot",
    "FundingRate",
    "ExternalEvent",
]