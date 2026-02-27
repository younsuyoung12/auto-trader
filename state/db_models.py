# db_models.py
# BingX Auto Trader - Postgres ORM 모델 정의
#
# - 모든 모델은 db_core.Base 를 상속
# - 테이블 이름은 bt_ 프리픽스로 통일
# - 스키마를 분리하고 싶으면 __table_args__ 에 schema 지정해서 사용
"""
db_models.py
=====================================================
BingX Auto Trader - Postgres ORM 모델 정의 모듈.

2025-11-14 변경 요약
----------------------------------------------------
1) EntryScore / Trade 구조 정리
   - bt_entry_scores 테이블에 진입 시점 점수/컴포넌트 저장.
   - bt_trades 와 1:N 관계를 명시(Trade.entry_scores).
2) Regime/Entry 스냅샷 설계 문서화
   - RegimeScore.final_regime 에 매크로 레짐 상태 저장.
   - Trade.entry_score / EntryScore.entry_score 로
     "그때 그 시점 점수"를 별도 보존.
   - EntryScore.components_json 으로 세부 컴포넌트(ema_gap, width_ratio,
     atr_fast/slow, soft_reason, arbitration_label 등)를 JSON 으로 기록.
3) 대시보드/분석 대비 인덱스 정책 정리
   - symbol + ts 조합 인덱스로 시계열 조회 최적화.

2025-11-15 검토/보완
----------------------------------------------------
1) 각 모델에 한국어 주석을 추가해 저장 의도를 더 명확히 했다.
2) __all__ 정의로 외부에서 가져다 쓸 수 있는 심벌 목록을 명시했다.
3) 기능적인 변경은 하지 않고, 문서화/가독성만 향상했다.

2025-11-15 RegimeScore 보완
----------------------------------------------------
1) timeframe 컬럼 추가 (예: "5m", "15m").
2) symbol + timeframe + ts 인덱스 추가로 시계열 레짐 조회 최적화.
"""

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

from state.db_core import Base

# 숫자 정밀도 참고:
# - price, qty 등: Numeric(24, 8) 정도면 충분 (필요시 조정 가능)
NUMERIC_24_8 = Numeric(24, 8)


# ─────────────────────────────
# 캔들 (1m / 5m / 15m ...)
# ─────────────────────────────
class Candle(Base):
    """시세 캔들(분봉/시간봉 등)을 저장하는 테이블.

    - timeframe 에 1m / 5m / 15m / 1h 등을 구분해서 넣을 수 있다.
    - volume/quote_volume 은 선택적이며, 소스에 따라 None 일 수 있다.
    """

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
    """호가창(depth5) 스냅샷 테이블.

    - best_bid / best_ask / spread 를 빠르게 조회하기 위한 구조.
    - bids_raw / asks_raw 에 depth5 전체를 JSON 으로 넣어둔다.
    """

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
    """EMA, ATR, 거래량 관련 지표 등을 저장하는 테이블.

    - 캔들과 1:1 매핑되게 ts/timeframe 을 맞춰 저장하는 것을 권장.
    - extra_json 으로 실험적인 지표를 쉽게 확장할 수 있다.
    """

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
    """시장 레짐(TREND / RANGE / NO_TRADE 등)에 대한 점수를 저장.

    - 시그널 계산 전 단계에서 "지금은 어떤 장인가"를 기록하는 용도.
    - timeframe 으로 5m, 15m 등 분석 기준 타임프레임을 구분한다.
    """

    __tablename__ = "bt_regime_scores"

    id = Column(Integer, primary_key=True, autoincrement=True)

    symbol = Column(String(32), nullable=False)
    timeframe = Column(String(8), nullable=True)  # 예: "5m", "15m" (기존 데이터 호환 위해 nullable)
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
        Index("ix_bt_regime_symbol_tf_ts", "symbol", "timeframe", "ts", unique=False),
    )


# ─────────────────────────────
# Entry Score (진입 점수)
# ─────────────────────────────
class EntryScore(Base):
    """진입 의사결정에 사용된 점수를 저장하는 테이블.

    - 한 번의 진입 시 여러 score 버전을 남길 수 있음 (used_for_entry 플래그로 구분).
    - components_json 예:
      {
        "trend_gap_ratio": 0.0042,
        "range_width_ratio": 0.0021,
        "atr_fast": 120.5,
        "atr_slow": 80.3,
        "arbitration_label": "HYBRID",
        "notes": ["5m/15m 정합 ok", "1m_confirm_pass"],
      }
    """

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
    components_json = Column(JSON, nullable=True)  # {"trend": ..., "volume": ..., "funding": ..., ...}

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
    """실제 체결된 트레이드(자동 + 수동)를 저장하는 테이블.

    - 자동 매매의 경우 is_auto=True, strategy (TREND/RANGE 등) 으로 전략 구분.
    - EntryScore 와의 관계(entry_scores)를 통해 진입 시점 점수 히스토리와 연결.
    """

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
    """펀딩비 이력을 저장하는 테이블."""

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
    """FOMC, CPI 같은 외부 이벤트 정보를 저장하는 테이블."""

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


__all__ = [
    "NUMERIC_24_8",
    "Candle",
    "OrderbookSnapshot",
    "Indicator",
    "RegimeScore",
    "EntryScore",
    "Trade",
    "FundingRate",
    "ExternalEvent",
]
