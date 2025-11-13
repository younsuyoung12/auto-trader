# db_models.py
# BingX Auto Trader - Postgres ORM 모델 정의
#
# - 모든 모델은 db_core.Base 를 상속
# - 테이블 이름은 bt_ 프리픽스로 통일
# - 스키마를 분리하고 싶으면 __table_args__ 에 schema 지정해서 사용

from datetime import datetime

from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    Float,
    Boolean,
    Numeric,
    JSON,
    Index,
    ForeignKey,
)
from sqlalchemy.orm import relationship

from db_core import Base

# 숫자 정밀도 참고:
# - price, qty 등: Numeric(24, 8) 정도면 충분 (필요시 조정 가능)
NUMERIC_24_8 = Numeric(24, 8)


# ─────────────────────────────
# 캔들 (1m / 5m / 15m ...)
# ─────────────────────────────
class Candle(Base):
    __tablename__ = "bt_candles"
    # __table_args__ = {"schema": "bingx_trader"}  # 스키마 쓰려면 주석 해제 후 DB에 schema 생성

    id = Column(Integer, primary_key=True, autoincrement=True)

    symbol = Column(String(32), nullable=False)        # 예: BTC-USDT
    timeframe = Column(String(8), nullable=False)      # 예: 1m / 5m / 15m
    ts = Column(DateTime(timezone=True), nullable=False)  # 캔들 종가 시각 (UTC 추천)

    open = Column(NUMERIC_24_8, nullable=False)
    high = Column(NUMERIC_24_8, nullable=False)
    low = Column(NUMERIC_24_8, nullable=False)
    close = Column(NUMERIC_24_8, nullable=False)

    volume = Column(NUMERIC_24_8, nullable=True)       # 계약수량 기준 거래량
    quote_volume = Column(NUMERIC_24_8, nullable=True) # USDT 기준 거래대금 (있으면)

    source = Column(String(16), nullable=True)         # ws / rest 등
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("ix_bt_candles_symbol_tf_ts", "symbol", "timeframe", "ts", unique=False),
    )


# ─────────────────────────────
# 오더북 스냅샷 (depth5 기준)
# ─────────────────────────────
class OrderbookSnapshot(Base):
    __tablename__ = "bt_orderbook_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)

    symbol = Column(String(32), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)

    best_bid = Column(NUMERIC_24_8, nullable=False)
    best_ask = Column(NUMERIC_24_8, nullable=False)
    spread = Column(NUMERIC_24_8, nullable=False)

    # BBO 기준 호가 비대칭성 (예: (bid_notional - ask_notional) / (bid+ask))
    depth_imbalance = Column(Float, nullable=True)

    # raw depth5 (Postgres JSONB로 저장)
    bids_raw = Column(JSON, nullable=True)  # [{price, qty}, ...]
    asks_raw = Column(JSON, nullable=True)

    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("ix_bt_orderbook_symbol_ts", "symbol", "ts", unique=False),
    )


# ─────────────────────────────
# 지표 (인디케이터) 테이블
# ─────────────────────────────
class Indicator(Base):
    __tablename__ = "bt_indicators"

    id = Column(Integer, primary_key=True, autoincrement=True)

    symbol = Column(String(32), nullable=False)
    timeframe = Column(String(8), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)

    # 추세 관련
    ema_fast = Column(NUMERIC_24_8, nullable=True)
    ema_slow = Column(NUMERIC_24_8, nullable=True)
    ema_dist = Column(Float, nullable=True)       # (ema_fast - ema_slow) / close
    ema_slope = Column(Float, nullable=True)      # (ema_fast_now - ema_fast_prev) / close

    # 변동성 (ATR)
    atr = Column(NUMERIC_24_8, nullable=True)
    atr_pct = Column(Float, nullable=True)        # ATR / close
    volatility_score = Column(Float, nullable=True)

    # 거래량
    volume_ma = Column(NUMERIC_24_8, nullable=True)
    volume_ratio = Column(Float, nullable=True)   # volume / volume_ma
    volume_z = Column(Float, nullable=True)       # z-score

    # 박스(레인지)
    range_top = Column(NUMERIC_24_8, nullable=True)
    range_bottom = Column(NUMERIC_24_8, nullable=True)
    range_height = Column(Float, nullable=True)   # (range_top - range_bottom) / close
    range_flag = Column(Boolean, default=False, nullable=False)

    extra_json = Column(JSON, nullable=True)      # 필요 시 확장용

    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("ix_bt_indicators_symbol_tf_ts", "symbol", "timeframe", "ts", unique=False),
    )


# ─────────────────────────────
# Regime 점수 (TREND / RANGE / NO_TRADE)
# ─────────────────────────────
class RegimeScore(Base):
    __tablename__ = "bt_regime_scores"

    id = Column(Integer, primary_key=True, autoincrement=True)

    symbol = Column(String(32), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)

    trend_score = Column(Float, nullable=True)
    range_score = Column(Float, nullable=True)
    chop_score = Column(Float, nullable=True)

    event_risk_score = Column(Float, nullable=True)
    funding_bias_score = Column(Float, nullable=True)

    # TREND / RANGE / NO_TRADE
    final_regime = Column(String(16), nullable=False)

    details_json = Column(JSON, nullable=True)    # score 계산에 사용된 원천 지표/설명 등

    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("ix_bt_regime_symbol_ts", "symbol", "ts", unique=False),
    )


# ─────────────────────────────
# Entry Score (진입 점수)
# ─────────────────────────────
class EntryScore(Base):
    __tablename__ = "bt_entry_scores"

    id = Column(Integer, primary_key=True, autoincrement=True)

    symbol = Column(String(32), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)

    # BUY / SELL
    side = Column(String(8), nullable=False)

    # TREND / RANGE / HYBRID 등
    signal_type = Column(String(16), nullable=True)

    # Regime 기준
    regime_at_entry = Column(String(16), nullable=True)  # TREND / RANGE / NO_TRADE

    entry_score = Column(Float, nullable=False)
    components_json = Column(JSON, nullable=True)  # {"trend": ..., "volume": ..., "funding": ...}

    # 실제로 이 점수가 진입에 사용됐는지 여부
    used_for_entry = Column(Boolean, default=False, nullable=False)

    # 해당 score로 만들어진 트레이드 id (있으면 연결)
    trade_id = Column(Integer, ForeignKey("bt_trades.id"), nullable=True)

    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    trade = relationship("Trade", back_populates="entry_scores")

    __table_args__ = (
        Index("ix_bt_entry_scores_symbol_ts", "symbol", "ts", unique=False),
    )


# ─────────────────────────────
# 트레이드 (자동 + 수동 통합)
# ─────────────────────────────
class Trade(Base):
    __tablename__ = "bt_trades"

    id = Column(Integer, primary_key=True, autoincrement=True)

    symbol = Column(String(32), nullable=False)

    # BUY / SELL
    side = Column(String(8), nullable=False)

    # 진입/청산 시각
    entry_ts = Column(DateTime(timezone=True), nullable=False)
    exit_ts = Column(DateTime(timezone=True), nullable=True)

    # 진입/청산 가격
    entry_price = Column(NUMERIC_24_8, nullable=False)
    exit_price = Column(NUMERIC_24_8, nullable=True)

    qty = Column(NUMERIC_24_8, nullable=False)   # 계약 수량

    # PnL
    pnl_usdt = Column(NUMERIC_24_8, nullable=True)
    pnl_pct_futures = Column(Float, nullable=True)
    pnl_pct_spot_ref = Column(Float, nullable=True)

    # 메타정보
    is_auto = Column(Boolean, default=True, nullable=False)  # 자동/수동
    regime_at_entry = Column(String(16), nullable=True)
    regime_at_exit = Column(String(16), nullable=True)

    # 점수 스냅샷
    entry_score = Column(Float, nullable=True)
    trend_score_at_entry = Column(Float, nullable=True)
    range_score_at_entry = Column(Float, nullable=True)

    # 전략 / 마감 이유
    strategy = Column(String(16), nullable=True)     # TREND / RANGE 등
    close_reason = Column(String(32), nullable=True) # TP / SL / EARLY_TP / EARLY_SL / MANUAL_CLOSE / FORCE_CLOSE 등

    # 추가 정보
    leverage = Column(Float, nullable=True)
    risk_pct = Column(Float, nullable=True)
    tp_pct = Column(Float, nullable=True)
    sl_pct = Column(Float, nullable=True)

    note = Column(String(255), nullable=True)

    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    updated_at = Column(
        DateTime(timezone=True),
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    entry_scores = relationship("EntryScore", back_populates="trade")

    __table_args__ = (
        Index("ix_bt_trades_symbol_entry_ts", "symbol", "entry_ts", unique=False),
    )


# ─────────────────────────────
# 펀딩비
# ─────────────────────────────
class FundingRate(Base):
    __tablename__ = "bt_funding_rates"

    id = Column(Integer, primary_key=True, autoincrement=True)

    symbol = Column(String(32), nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False)

    funding_rate = Column(Float, nullable=False)  # 예: 0.00025 (0.025%)

    raw_json = Column(JSON, nullable=True)

    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("ix_bt_funding_symbol_ts", "symbol", "ts", unique=False),
    )


# ─────────────────────────────
# 외부 이벤트 (FOMC, CPI 등)
# ─────────────────────────────
class ExternalEvent(Base):
    __tablename__ = "bt_external_events"

    id = Column(Integer, primary_key=True, autoincrement=True)

    event_id = Column(String(64), nullable=False)  # 외부 API 기준 ID
    event_name = Column(String(128), nullable=False)

    symbol = Column(String(32), nullable=True)     # BTC-USDT, ALL 등

    start_time = Column(DateTime(timezone=True), nullable=False)
    end_time = Column(DateTime(timezone=True), nullable=True)

    impact_level = Column(String(16), nullable=True)  # HIGH / MEDIUM / LOW 등
    source = Column(String(64), nullable=True)        # 예: "forexfactory", "custom"

    raw_json = Column(JSON, nullable=True)

    created_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("ix_bt_events_start_time", "start_time", unique=False),
    )
