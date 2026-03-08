"""
========================================================
FILE: state/db_models.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================
설계 원칙:
- Base는 state.db_core.Base 단일 소스를 사용한다.
- 테이블명/컬럼명은 DB 스키마(bt_*)와 완전히 동일하게 유지한다.
- timezone-aware(TIMESTAMPTZ) 컬럼은 timezone=True 로 유지한다.
- JSON 컬럼은 Postgres JSON/JSONB 타입을 그대로 사용한다.
- import 시점에 스키마 생성/변경을 수행하지 않는다.
- 레거시 호환: 일부 모듈이 `Trade` ORM 심벌을 기대하므로 `Trade = TradeORM` alias를 제공한다.
- 폴백 금지: 누락/불일치/모호성은 추정으로 메우지 않는다(즉시 예외/중단).
- AI Trading Intelligence System용 분석 테이블/컬럼은 모두 additive 변경만 수행한다.
- 구성 불변 규칙: 런타임 중 전략 설정 변경을 전제로 하는 스키마는 두지 않는다.
- 데이터 무결성 규칙: 핵심 식별자/가격/수량/시간 정합성은 저장 레이어와 스키마 레벨에서 함께 강제한다.

변경 이력
--------------------------------------------------------
- 2026-03-08:
  1) Volume Profile / OrderFlow / Options 구조 피처 영속 컬럼 추가
  2) 대상:
     - bt_trade_snapshots
     - market_features
     - trade_context_snapshots
  3) 추가 컬럼 예:
     - poc_price / value_area_low / value_area_high / poc_distance_bps / price_location
     - cvd / delta_ratio_pct / aggression_bias / divergence / orderflow_price_change_pct
     - put_call_oi_ratio / put_call_volume_ratio / options_bias
  4) 기존 테이블/컬럼 삭제 없음
  5) nullable additive 확장만 수행

- 2026-03-07:
  1) AI Trading Intelligence System 지원용 market_features 테이블 추가
  2) trade_context_snapshots 테이블 추가
  3) bt_trades 호환 분석 컬럼 추가
     - status
     - opened_ts_ms
     - closed_ts_ms
     - realized_pnl
     - exit_reason
  4) bt_trade_snapshots 분석 컬럼 추가
     - ts_ms
     - spread_bps
     - pattern_score
     - market_regime
     - orderbook_imbalance
     - volatility_score
- 2026-03-04:
  1) 모듈 docstring 위치 정합화(문서/정적분석 인식 보장): docstring을 파일 최상단으로 이동
  2) 헤더 표기 TRADE-GRADE로 정합(정책 문서와 일치)
  3) 로직/스키마/컬럼 정의는 변경하지 않음(동작 동일)
- 2026-03-03 (TRADE-GRADE):
  1) bt_trade_snapshots(TradeSnapshot) 분석형 필드 확장(운영/감사/재현 목적)
  2) decision_id UNIQUE 인덱스 추가(다중 NULL 허용: Postgres 특성)
- 2026-03-03 (PATCH):
  1) bt_trade_snapshots ORM 정합 수정(equity_current_usdt/equity_peak_usdt/dd_pct)
  2) bt_trades updated_at onupdate=_utc_now 추가
- 2026-03-02:
  1) bt_trades 실행/복구 컬럼 추가(ORM 정합)
========================================================
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
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
# bt_trades (실제 DB 스키마 1:1 정합 + 분석 호환 컬럼)
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

    exchange_position_side = Column(String(16), nullable=True)  # BOTH/LONG/SHORT
    remaining_qty = Column(Numeric(24, 8), nullable=True)
    realized_pnl_usdt = Column(Numeric(24, 8), nullable=True)

    reconciliation_status = Column(String(32), nullable=True)  # OK / MISMATCH / RECOVERED / ERROR
    last_synced_at = Column(DateTime(timezone=True), nullable=True)

    # ─────────────────────────────────────────
    # Analysis Compatibility Fields (AI Trading Intelligence)
    # ─────────────────────────────────────────
    status = Column(String(32), nullable=True, index=True)
    opened_ts_ms = Column(BigInteger, nullable=True, index=True)
    closed_ts_ms = Column(BigInteger, nullable=True, index=True)
    realized_pnl = Column(Numeric(24, 8), nullable=True)
    exit_reason = Column(String(64), nullable=True)

    __table_args__ = (
        Index("ix_bt_trades_symbol_entry_ts", "symbol", "entry_ts"),
        Index("ix_bt_trades_symbol_closed_ts_ms", "symbol", "closed_ts_ms"),
        CheckConstraint("qty > 0", name="ck_bt_trades_qty_gt_zero"),
    )


Trade = TradeORM


# ─────────────────────────────────────────────
# bt_trade_snapshots (진입 스냅샷 + 분석 호환 컬럼)
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
    # Equity / DD (정합)
    # ─────────────────────────────────────────
    equity_current_usdt = Column(Float, nullable=True)
    equity_peak_usdt = Column(Float, nullable=True)
    dd_pct = Column(Float, nullable=True)

    # ─────────────────────────────────────────
    # Decision Reconciliation (TRADE-GRADE)
    # ─────────────────────────────────────────
    decision_id = Column(String(64), nullable=True, index=True)
    quant_decision_pre = Column(JSONB, nullable=True)
    quant_constraints = Column(JSONB, nullable=True)
    quant_final_decision = Column(String(16), nullable=True)

    gpt_severity = Column(Integer, nullable=True)
    gpt_tags = Column(JSONB, nullable=True)
    gpt_confidence_penalty = Column(Float, nullable=True)
    gpt_suggested_risk_multiplier = Column(Float, nullable=True)
    gpt_rationale_short = Column(Text, nullable=True)

    # ─────────────────────────────────────────
    # Microstructure (TRADE-GRADE)
    # ─────────────────────────────────────────
    micro_funding_rate = Column(Float, nullable=True)
    micro_funding_z = Column(Float, nullable=True)

    micro_open_interest = Column(Float, nullable=True)
    micro_oi_z = Column(Float, nullable=True)

    micro_long_short_ratio = Column(Float, nullable=True)
    micro_lsr_z = Column(Float, nullable=True)

    micro_distortion_index = Column(Float, nullable=True)
    micro_score_risk = Column(Float, nullable=True)

    # ─────────────────────────────────────────
    # Execution Quality (TRADE-GRADE)
    # ─────────────────────────────────────────
    exec_expected_price = Column(Float, nullable=True)
    exec_filled_avg_price = Column(Float, nullable=True)
    exec_slippage_pct = Column(Float, nullable=True)
    exec_adverse_move_pct = Column(Float, nullable=True)
    exec_score = Column(Float, nullable=True)
    exec_post_prices = Column(JSONB, nullable=True)

    # ─────────────────────────────────────────
    # EV Heatmap / AutoBlock (TRADE-GRADE)
    # ─────────────────────────────────────────
    ev_cell_key = Column(String(96), nullable=True)
    ev_cell_ev = Column(Float, nullable=True)
    ev_cell_n = Column(Integer, nullable=True)
    ev_cell_status = Column(String(16), nullable=True)

    auto_blocked = Column(Boolean, nullable=True)
    auto_risk_multiplier = Column(Float, nullable=True)
    auto_block_reasons = Column(JSONB, nullable=True)

    # ─────────────────────────────────────────
    # Analysis Compatibility Fields (AI Trading Intelligence)
    # ─────────────────────────────────────────
    ts_ms = Column(BigInteger, nullable=True, index=True)
    spread_bps = Column(Numeric(24, 8), nullable=True)
    pattern_score = Column(Numeric(24, 8), nullable=True)
    market_regime = Column(String(32), nullable=True)
    orderbook_imbalance = Column(Numeric(24, 8), nullable=True)
    volatility_score = Column(Numeric(24, 8), nullable=True)

    # Volume Profile
    vp_poc_price = Column(Numeric(24, 8), nullable=True)
    vp_value_area_low = Column(Numeric(24, 8), nullable=True)
    vp_value_area_high = Column(Numeric(24, 8), nullable=True)
    vp_poc_distance_bps = Column(Numeric(24, 8), nullable=True)
    vp_price_location = Column(String(32), nullable=True)

    # Order Flow / CVD
    of_cvd = Column(Numeric(36, 8), nullable=True)
    of_delta_ratio_pct = Column(Numeric(24, 8), nullable=True)
    of_aggression_bias = Column(String(32), nullable=True)
    of_divergence = Column(String(32), nullable=True)
    of_price_change_pct = Column(Numeric(24, 8), nullable=True)

    # Options
    opt_put_call_oi_ratio = Column(Numeric(24, 8), nullable=True)
    opt_put_call_volume_ratio = Column(Numeric(24, 8), nullable=True)
    opt_options_bias = Column(String(32), nullable=True)

    __table_args__ = (
        Index("ux_bt_trade_snapshots_tradeid", "trade_id", unique=True),
        Index("ux_bt_trade_snapshots_decision_id", "decision_id", unique=True),
        Index("ix_bt_trade_snapshots_symbol_entryts", "symbol", "entry_ts"),
        Index("ix_bt_trade_snapshots_symbol_ts_ms", "symbol", "ts_ms"),
    )


# ─────────────────────────────────────────────
# bt_trade_exit_snapshots (청산 스냅샷)
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
# market_features
# AI Trading Intelligence 내부 시장 분석용 피처 저장소
# ─────────────────────────────────────────────
class MarketFeature(Base):
    __tablename__ = "market_features"

    id = Column(Integer, primary_key=True, index=True)

    ts_ms = Column(BigInteger, nullable=False, index=True)
    symbol = Column(String(32), nullable=False, index=True)
    timeframe = Column(String(8), nullable=False, index=True)

    close_price = Column(Numeric(24, 8), nullable=False)
    spread_bps = Column(Numeric(24, 8), nullable=False)
    orderbook_imbalance = Column(Numeric(24, 8), nullable=False)

    pattern_score = Column(Numeric(24, 8), nullable=False)
    volatility_score = Column(Numeric(24, 8), nullable=False)
    trend_score = Column(Numeric(24, 8), nullable=False)
    liquidity_score = Column(Numeric(24, 8), nullable=False)

    market_regime = Column(String(32), nullable=False)

    # Volume Profile
    poc_price = Column(Numeric(24, 8), nullable=True)
    value_area_low = Column(Numeric(24, 8), nullable=True)
    value_area_high = Column(Numeric(24, 8), nullable=True)
    poc_distance_bps = Column(Numeric(24, 8), nullable=True)
    price_location = Column(String(32), nullable=True)

    # Order Flow / CVD
    cvd = Column(Numeric(36, 8), nullable=True)
    delta_ratio_pct = Column(Numeric(24, 8), nullable=True)
    aggression_bias = Column(String(32), nullable=True)
    divergence = Column(String(32), nullable=True)
    orderflow_price_change_pct = Column(Numeric(24, 8), nullable=True)

    # Options
    put_call_oi_ratio = Column(Numeric(24, 8), nullable=True)
    put_call_volume_ratio = Column(Numeric(24, 8), nullable=True)
    options_bias = Column(String(32), nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, default=_utc_now)

    __table_args__ = (
        Index("ux_market_features_symbol_tf_tsms", "symbol", "timeframe", "ts_ms", unique=True),
        CheckConstraint("ts_ms > 0", name="ck_market_features_ts_ms_gt_zero"),
        CheckConstraint("close_price > 0", name="ck_market_features_close_price_gt_zero"),
        CheckConstraint("spread_bps >= 0", name="ck_market_features_spread_bps_ge_zero"),
        CheckConstraint("(poc_price IS NULL) OR (poc_price > 0)", name="ck_market_features_poc_price_gt_zero_when_present"),
        CheckConstraint(
            "((value_area_low IS NULL) AND (value_area_high IS NULL)) OR "
            "((value_area_low IS NOT NULL) AND (value_area_high IS NOT NULL) AND (value_area_high >= value_area_low))",
            name="ck_market_features_value_area_bounds_when_present",
        ),
        CheckConstraint(
            "(put_call_oi_ratio IS NULL) OR (put_call_oi_ratio > 0)",
            name="ck_market_features_put_call_oi_ratio_gt_zero_when_present",
        ),
        CheckConstraint(
            "(put_call_volume_ratio IS NULL) OR (put_call_volume_ratio > 0)",
            name="ck_market_features_put_call_volume_ratio_gt_zero_when_present",
        ),
    )


# ─────────────────────────────────────────────
# trade_context_snapshots
# 거래 시점 컨텍스트 스냅샷
# ─────────────────────────────────────────────
class TradeContextSnapshot(Base):
    __tablename__ = "trade_context_snapshots"

    id = Column(Integer, primary_key=True, index=True)

    trade_id = Column(Integer, ForeignKey("bt_trades.id", ondelete="CASCADE"), nullable=True, index=True)

    ts_ms = Column(BigInteger, nullable=False, index=True)
    symbol = Column(String(32), nullable=False, index=True)

    price = Column(Numeric(24, 8), nullable=False)
    spread_bps = Column(Numeric(24, 8), nullable=False)
    pattern_score = Column(Numeric(24, 8), nullable=False)
    market_regime = Column(String(32), nullable=False)
    orderbook_imbalance = Column(Numeric(24, 8), nullable=False)

    funding_rate = Column(Numeric(24, 12), nullable=True)
    open_interest = Column(Numeric(36, 8), nullable=True)
    long_short_ratio = Column(Numeric(24, 8), nullable=True)

    # Volume Profile
    poc_price = Column(Numeric(24, 8), nullable=True)
    value_area_low = Column(Numeric(24, 8), nullable=True)
    value_area_high = Column(Numeric(24, 8), nullable=True)
    poc_distance_bps = Column(Numeric(24, 8), nullable=True)
    price_location = Column(String(32), nullable=True)

    # Order Flow / CVD
    cvd = Column(Numeric(36, 8), nullable=True)
    delta_ratio_pct = Column(Numeric(24, 8), nullable=True)
    aggression_bias = Column(String(32), nullable=True)
    divergence = Column(String(32), nullable=True)
    orderflow_price_change_pct = Column(Numeric(24, 8), nullable=True)

    # Options
    put_call_oi_ratio = Column(Numeric(24, 8), nullable=True)
    put_call_volume_ratio = Column(Numeric(24, 8), nullable=True)
    options_bias = Column(String(32), nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, default=_utc_now)

    __table_args__ = (
        Index("ix_trade_context_symbol_tsms", "symbol", "ts_ms"),
        Index("ix_trade_context_trade_id_tsms", "trade_id", "ts_ms"),
        CheckConstraint("ts_ms > 0", name="ck_trade_context_ts_ms_gt_zero"),
        CheckConstraint("price > 0", name="ck_trade_context_price_gt_zero"),
        CheckConstraint("spread_bps >= 0", name="ck_trade_context_spread_bps_ge_zero"),
        CheckConstraint(
            "(long_short_ratio IS NULL) OR (long_short_ratio > 0)",
            name="ck_trade_context_long_short_ratio_gt_zero_when_present",
        ),
        CheckConstraint(
            "(poc_price IS NULL) OR (poc_price > 0)",
            name="ck_trade_context_poc_price_gt_zero_when_present",
        ),
        CheckConstraint(
            "((value_area_low IS NULL) AND (value_area_high IS NULL)) OR "
            "((value_area_low IS NOT NULL) AND (value_area_high IS NOT NULL) AND (value_area_high >= value_area_low))",
            name="ck_trade_context_value_area_bounds_when_present",
        ),
        CheckConstraint(
            "(put_call_oi_ratio IS NULL) OR (put_call_oi_ratio > 0)",
            name="ck_trade_context_put_call_oi_ratio_gt_zero_when_present",
        ),
        CheckConstraint(
            "(put_call_volume_ratio IS NULL) OR (put_call_volume_ratio > 0)",
            name="ck_trade_context_put_call_volume_ratio_gt_zero_when_present",
        ),
    )


# ─────────────────────────────────────────────
# bt_funding_rates
# ─────────────────────────────────────────────
class FundingRate(Base):
    __tablename__ = "bt_funding_rates"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(32), nullable=False, index=True)
    ts = Column(DateTime(timezone=True), nullable=False, index=True)

    rate = Column("funding_rate", Numeric(24, 8), nullable=False)

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
    "MarketFeature",
    "TradeContextSnapshot",
    "FundingRate",
    "ExternalEvent",
]