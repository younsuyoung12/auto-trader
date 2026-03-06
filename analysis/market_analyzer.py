"""
========================================================
FILE: analysis/market_analyzer.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할:
- 내부 DB에 저장된 market_features / trade_context_snapshots를 기반으로
  현재 시장 상태를 분석한다.
- 이 모듈은 "내부 시장 분석(DB 기반)"만 담당한다.
- 주문 실행 / 포지션 변경 / TP·SL 수정은 절대 수행하지 않는다.

절대 원칙:
- STRICT · NO-FALLBACK
- settings.py(SSOT) 외 환경변수 직접 접근 금지
- state/db_core.py 외 DB 직접 연결 금지
- print() 금지 / logging 사용
- 데이터 누락/손상/모호성 발생 시 즉시 예외
- 민감정보 로그 금지

전제 스키마:
- market_features
- trade_context_snapshots

변경 이력:
2026-03-07
- 신규 생성
- DB 기반 내부 시장 분석기 추가
========================================================
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Mapping, Optional, Sequence

from sqlalchemy import text

from settings import SETTINGS
from state.db_core import get_session

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MarketFeaturePoint:
    ts_ms: int
    symbol: str
    timeframe: str
    close_price: Decimal
    spread_bps: Decimal
    orderbook_imbalance: Decimal
    pattern_score: Decimal
    volatility_score: Decimal
    trend_score: Decimal
    market_regime: str
    liquidity_score: Decimal


@dataclass(frozen=True)
class TradeContextPoint:
    ts_ms: int
    symbol: str
    price: Decimal
    spread_bps: Decimal
    pattern_score: Decimal
    market_regime: str
    orderbook_imbalance: Decimal
    funding_rate: Optional[Decimal]
    open_interest: Optional[Decimal]
    long_short_ratio: Optional[Decimal]


@dataclass(frozen=True)
class InternalMarketSummary:
    symbol: str
    timeframe: str
    as_of_ms: int
    latest_close_price: Decimal
    price_change_pct: Decimal
    avg_spread_bps: Decimal
    latest_spread_bps: Decimal
    avg_orderbook_imbalance: Decimal
    latest_orderbook_imbalance: Decimal
    avg_pattern_score: Decimal
    latest_pattern_score: Decimal
    latest_volatility_score: Decimal
    latest_trend_score: Decimal
    latest_liquidity_score: Decimal
    market_regime: str
    trend: str
    volatility: str
    liquidity: str
    funding_rate: Optional[Decimal]
    open_interest: Optional[Decimal]
    long_short_ratio: Optional[Decimal]
    entry_blockers: List[str]
    analyst_summary_ko: str
    dashboard_payload: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class MarketAnalyzer:
    """
    내부 DB 기반 시장 상태 분석기.

    대상:
    - market_features
    - trade_context_snapshots

    출력:
    - 현재 내부 시장 상태 요약
    - 진입 차단 요인(entry blockers)
    - 대시보드/LLM용 payload
    """

    def __init__(self) -> None:
        self._symbol = self._require_str_setting("ANALYST_MARKET_SYMBOL")
        self._timeframe = self._require_str_setting("ANALYST_DB_MARKET_TIMEFRAME")
        self._lookback_limit = self._require_int_setting("ANALYST_DB_MARKET_LOOKBACK_LIMIT")
        self._pattern_entry_threshold = self._require_decimal_setting("ANALYST_PATTERN_ENTRY_THRESHOLD")
        self._spread_guard_bps = self._require_decimal_setting("ANALYST_SPREAD_GUARD_BPS")
        self._imbalance_guard_abs = self._require_decimal_setting("ANALYST_IMBALANCE_GUARD_ABS")

        if self._lookback_limit < 5:
            raise RuntimeError("ANALYST_DB_MARKET_LOOKBACK_LIMIT must be >= 5")
        if self._pattern_entry_threshold <= Decimal("0"):
            raise RuntimeError("ANALYST_PATTERN_ENTRY_THRESHOLD must be > 0")
        if self._spread_guard_bps <= Decimal("0"):
            raise RuntimeError("ANALYST_SPREAD_GUARD_BPS must be > 0")
        if self._imbalance_guard_abs <= Decimal("0"):
            raise RuntimeError("ANALYST_IMBALANCE_GUARD_ABS must be > 0")

    # ========================================================
    # Public API
    # ========================================================

    def run(self) -> InternalMarketSummary:
        market_features = self._load_market_features_or_raise(
            symbol=self._symbol,
            timeframe=self._timeframe,
            limit=self._lookback_limit,
        )
        latest_context = self._load_latest_trade_context_or_raise(symbol=self._symbol)
        summary = self._build_summary(
            market_features=market_features,
            latest_context=latest_context,
        )
        logger.info(
            "Internal market analysis completed: symbol=%s timeframe=%s regime=%s trend=%s",
            summary.symbol,
            summary.timeframe,
            summary.market_regime,
            summary.trend,
        )
        return summary

    def build_prompt_context(self) -> Dict[str, Any]:
        return self.run().dashboard_payload

    # ========================================================
    # DB Load
    # ========================================================

    def _load_market_features_or_raise(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
    ) -> List[MarketFeaturePoint]:
        sql = text(
            """
            SELECT
                ts_ms,
                symbol,
                timeframe,
                close_price,
                spread_bps,
                orderbook_imbalance,
                pattern_score,
                volatility_score,
                trend_score,
                market_regime,
                liquidity_score
            FROM market_features
            WHERE symbol = :symbol
              AND timeframe = :timeframe
            ORDER BY ts_ms DESC
            LIMIT :limit
            """
        )

        with get_session() as session:
            rows = session.execute(
                sql,
                {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "limit": limit,
                },
            ).mappings().all()

        if not rows:
            raise RuntimeError(
                f"No market_features rows found for symbol={symbol}, timeframe={timeframe}"
            )

        points = [self._parse_market_feature_row(row) for row in rows]
        points_sorted = sorted(points, key=lambda x: x.ts_ms)

        if len(points_sorted) < 5:
            raise RuntimeError(
                f"Insufficient market_features rows: required>=5, got={len(points_sorted)}"
            )

        self._validate_time_order(points_sorted)
        return points_sorted

    def _load_latest_trade_context_or_raise(self, symbol: str) -> TradeContextPoint:
        sql = text(
            """
            SELECT
                ts_ms,
                symbol,
                price,
                spread_bps,
                pattern_score,
                market_regime,
                orderbook_imbalance,
                funding_rate,
                open_interest,
                long_short_ratio
            FROM trade_context_snapshots
            WHERE symbol = :symbol
            ORDER BY ts_ms DESC
            LIMIT 1
            """
        )

        with get_session() as session:
            row = session.execute(sql, {"symbol": symbol}).mappings().first()

        if row is None:
            raise RuntimeError(f"No trade_context_snapshots row found for symbol={symbol}")

        return self._parse_trade_context_row(row)

    # ========================================================
    # Core Analysis
    # ========================================================

    def _build_summary(
        self,
        market_features: Sequence[MarketFeaturePoint],
        latest_context: TradeContextPoint,
    ) -> InternalMarketSummary:
        latest_feature = market_features[-1]
        first_feature = market_features[0]

        if latest_feature.symbol != latest_context.symbol:
            raise RuntimeError("Symbol mismatch between market_features and trade_context_snapshots")

        latest_close_price = latest_feature.close_price
        if latest_close_price <= Decimal("0"):
            raise RuntimeError("latest_close_price must be > 0")

        price_change_pct = self._calc_price_change_pct(
            first_price=first_feature.close_price,
            last_price=latest_feature.close_price,
        )
        avg_spread_bps = self._avg_decimal([x.spread_bps for x in market_features], "spread_bps")
        latest_spread_bps = latest_feature.spread_bps

        avg_orderbook_imbalance = self._avg_decimal(
            [x.orderbook_imbalance for x in market_features],
            "orderbook_imbalance",
        )
        latest_orderbook_imbalance = latest_feature.orderbook_imbalance

        avg_pattern_score = self._avg_decimal(
            [x.pattern_score for x in market_features],
            "pattern_score",
        )
        latest_pattern_score = latest_feature.pattern_score

        latest_volatility_score = latest_feature.volatility_score
        latest_trend_score = latest_feature.trend_score
        latest_liquidity_score = latest_feature.liquidity_score
        market_regime = latest_feature.market_regime

        trend = self._classify_trend(latest_trend_score)
        volatility = self._classify_volatility(latest_volatility_score)
        liquidity = self._classify_liquidity(latest_liquidity_score)

        entry_blockers = self._build_entry_blockers(
            latest_pattern_score=latest_pattern_score,
            latest_spread_bps=latest_spread_bps,
            latest_orderbook_imbalance=latest_orderbook_imbalance,
        )

        analyst_summary_ko = self._build_korean_summary(
            symbol=latest_feature.symbol,
            timeframe=latest_feature.timeframe,
            latest_close_price=latest_close_price,
            price_change_pct=price_change_pct,
            trend=trend,
            volatility=volatility,
            liquidity=liquidity,
            market_regime=market_regime,
            avg_spread_bps=avg_spread_bps,
            latest_spread_bps=latest_spread_bps,
            avg_pattern_score=avg_pattern_score,
            latest_pattern_score=latest_pattern_score,
            avg_orderbook_imbalance=avg_orderbook_imbalance,
            latest_orderbook_imbalance=latest_orderbook_imbalance,
            entry_blockers=entry_blockers,
            funding_rate=latest_context.funding_rate,
            open_interest=latest_context.open_interest,
            long_short_ratio=latest_context.long_short_ratio,
        )

        dashboard_payload = self._build_dashboard_payload(
            symbol=latest_feature.symbol,
            timeframe=latest_feature.timeframe,
            as_of_ms=latest_feature.ts_ms,
            latest_close_price=latest_close_price,
            price_change_pct=price_change_pct,
            avg_spread_bps=avg_spread_bps,
            latest_spread_bps=latest_spread_bps,
            avg_orderbook_imbalance=avg_orderbook_imbalance,
            latest_orderbook_imbalance=latest_orderbook_imbalance,
            avg_pattern_score=avg_pattern_score,
            latest_pattern_score=latest_pattern_score,
            latest_volatility_score=latest_volatility_score,
            latest_trend_score=latest_trend_score,
            latest_liquidity_score=latest_liquidity_score,
            market_regime=market_regime,
            trend=trend,
            volatility=volatility,
            liquidity=liquidity,
            entry_blockers=entry_blockers,
            funding_rate=latest_context.funding_rate,
            open_interest=latest_context.open_interest,
            long_short_ratio=latest_context.long_short_ratio,
            analyst_summary_ko=analyst_summary_ko,
        )

        return InternalMarketSummary(
            symbol=latest_feature.symbol,
            timeframe=latest_feature.timeframe,
            as_of_ms=latest_feature.ts_ms,
            latest_close_price=latest_close_price,
            price_change_pct=price_change_pct,
            avg_spread_bps=avg_spread_bps,
            latest_spread_bps=latest_spread_bps,
            avg_orderbook_imbalance=avg_orderbook_imbalance,
            latest_orderbook_imbalance=latest_orderbook_imbalance,
            avg_pattern_score=avg_pattern_score,
            latest_pattern_score=latest_pattern_score,
            latest_volatility_score=latest_volatility_score,
            latest_trend_score=latest_trend_score,
            latest_liquidity_score=latest_liquidity_score,
            market_regime=market_regime,
            trend=trend,
            volatility=volatility,
            liquidity=liquidity,
            funding_rate=latest_context.funding_rate,
            open_interest=latest_context.open_interest,
            long_short_ratio=latest_context.long_short_ratio,
            entry_blockers=entry_blockers,
            analyst_summary_ko=analyst_summary_ko,
            dashboard_payload=dashboard_payload,
        )

    # ========================================================
    # Parsing
    # ========================================================

    def _parse_market_feature_row(self, row: Mapping[str, Any]) -> MarketFeaturePoint:
        return MarketFeaturePoint(
            ts_ms=self._parse_int(row, "ts_ms"),
            symbol=self._parse_str(row, "symbol"),
            timeframe=self._parse_str(row, "timeframe"),
            close_price=self._parse_decimal(row, "close_price"),
            spread_bps=self._parse_decimal(row, "spread_bps"),
            orderbook_imbalance=self._parse_decimal(row, "orderbook_imbalance"),
            pattern_score=self._parse_decimal(row, "pattern_score"),
            volatility_score=self._parse_decimal(row, "volatility_score"),
            trend_score=self._parse_decimal(row, "trend_score"),
            market_regime=self._parse_str(row, "market_regime"),
            liquidity_score=self._parse_decimal(row, "liquidity_score"),
        )

    def _parse_trade_context_row(self, row: Mapping[str, Any]) -> TradeContextPoint:
        return TradeContextPoint(
            ts_ms=self._parse_int(row, "ts_ms"),
            symbol=self._parse_str(row, "symbol"),
            price=self._parse_decimal(row, "price"),
            spread_bps=self._parse_decimal(row, "spread_bps"),
            pattern_score=self._parse_decimal(row, "pattern_score"),
            market_regime=self._parse_str(row, "market_regime"),
            orderbook_imbalance=self._parse_decimal(row, "orderbook_imbalance"),
            funding_rate=self._parse_optional_decimal(row, "funding_rate"),
            open_interest=self._parse_optional_decimal(row, "open_interest"),
            long_short_ratio=self._parse_optional_decimal(row, "long_short_ratio"),
        )

    # ========================================================
    # Classification
    # ========================================================

    def _classify_trend(self, trend_score: Decimal) -> str:
        if trend_score >= Decimal("0.70"):
            return "strong_uptrend"
        if trend_score >= Decimal("0.20"):
            return "weak_uptrend"
        if trend_score <= Decimal("-0.70"):
            return "strong_downtrend"
        if trend_score <= Decimal("-0.20"):
            return "weak_downtrend"
        return "sideways"

    def _classify_volatility(self, volatility_score: Decimal) -> str:
        if volatility_score >= Decimal("0.75"):
            return "high"
        if volatility_score >= Decimal("0.35"):
            return "medium"
        return "low"

    def _classify_liquidity(self, liquidity_score: Decimal) -> str:
        if liquidity_score >= Decimal("0.75"):
            return "high"
        if liquidity_score >= Decimal("0.35"):
            return "normal"
        return "thin"

    def _build_entry_blockers(
        self,
        latest_pattern_score: Decimal,
        latest_spread_bps: Decimal,
        latest_orderbook_imbalance: Decimal,
    ) -> List[str]:
        blockers: List[str] = []

        if latest_pattern_score < self._pattern_entry_threshold:
            blockers.append(
                "pattern_score_below_threshold"
            )

        if latest_spread_bps > self._spread_guard_bps:
            blockers.append(
                "spread_guard_blocked"
            )

        if abs(latest_orderbook_imbalance) > self._imbalance_guard_abs:
            blockers.append(
                "depth_imbalance_guard"
            )

        if not blockers:
            blockers.append("none")

        return blockers

    # ========================================================
    # Presentation
    # ========================================================

    def _build_korean_summary(
        self,
        symbol: str,
        timeframe: str,
        latest_close_price: Decimal,
        price_change_pct: Decimal,
        trend: str,
        volatility: str,
        liquidity: str,
        market_regime: str,
        avg_spread_bps: Decimal,
        latest_spread_bps: Decimal,
        avg_pattern_score: Decimal,
        latest_pattern_score: Decimal,
        avg_orderbook_imbalance: Decimal,
        latest_orderbook_imbalance: Decimal,
        entry_blockers: Sequence[str],
        funding_rate: Optional[Decimal],
        open_interest: Optional[Decimal],
        long_short_ratio: Optional[Decimal],
    ) -> str:
        parts: List[str] = []

        parts.append(
            f"{symbol} {timeframe} 기준 내부 시장 분석 결과, 현재 종가는 {self._fmt_decimal(latest_close_price, 2)} 이고 "
            f"최근 구간 가격 변화율은 {self._fmt_decimal(price_change_pct, 2)}% 입니다."
        )
        parts.append(
            f"추세는 {self._ko_trend(trend)}, 변동성은 {self._ko_volatility(volatility)}, "
            f"유동성은 {self._ko_liquidity(liquidity)}로 분류되며 현재 레짐은 {market_regime} 입니다."
        )
        parts.append(
            f"최근 평균 스프레드는 {self._fmt_decimal(avg_spread_bps, 2)}bps, "
            f"최신 스프레드는 {self._fmt_decimal(latest_spread_bps, 2)}bps 입니다."
        )
        parts.append(
            f"최근 평균 패턴 점수는 {self._fmt_decimal(avg_pattern_score, 4)}, "
            f"최신 패턴 점수는 {self._fmt_decimal(latest_pattern_score, 4)} 입니다."
        )
        parts.append(
            f"최근 평균 오더북 불균형은 {self._fmt_decimal(avg_orderbook_imbalance, 4)}, "
            f"최신 오더북 불균형은 {self._fmt_decimal(latest_orderbook_imbalance, 4)} 입니다."
        )

        if funding_rate is not None:
            parts.append(f"최신 펀딩비는 {self._fmt_decimal(funding_rate, 8)} 입니다.")
        if open_interest is not None:
            parts.append(f"최신 오픈이자는 {self._fmt_decimal(open_interest, 4)} 입니다.")
        if long_short_ratio is not None:
            parts.append(f"최신 롱숏 비율은 {self._fmt_decimal(long_short_ratio, 4)} 입니다.")

        if len(entry_blockers) == 1 and entry_blockers[0] == "none":
            parts.append("현재 기준으로 명시적인 진입 차단 요인은 확인되지 않았습니다.")
        else:
            parts.append(f"현재 진입 차단 요인은 {', '.join(entry_blockers)} 입니다.")

        return " ".join(parts)

    def _build_dashboard_payload(
        self,
        symbol: str,
        timeframe: str,
        as_of_ms: int,
        latest_close_price: Decimal,
        price_change_pct: Decimal,
        avg_spread_bps: Decimal,
        latest_spread_bps: Decimal,
        avg_orderbook_imbalance: Decimal,
        latest_orderbook_imbalance: Decimal,
        avg_pattern_score: Decimal,
        latest_pattern_score: Decimal,
        latest_volatility_score: Decimal,
        latest_trend_score: Decimal,
        latest_liquidity_score: Decimal,
        market_regime: str,
        trend: str,
        volatility: str,
        liquidity: str,
        entry_blockers: Sequence[str],
        funding_rate: Optional[Decimal],
        open_interest: Optional[Decimal],
        long_short_ratio: Optional[Decimal],
        analyst_summary_ko: str,
    ) -> Dict[str, Any]:
        derivatives: Dict[str, Any] = {
            "funding_rate": self._fmt_decimal_optional(funding_rate, 8),
            "open_interest": self._fmt_decimal_optional(open_interest, 4),
            "long_short_ratio": self._fmt_decimal_optional(long_short_ratio, 4),
        }

        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "as_of_ms": as_of_ms,
            "market_structure": {
                "market_regime": market_regime,
                "trend": trend,
                "volatility": volatility,
                "liquidity": liquidity,
            },
            "price_context": {
                "latest_close_price": self._fmt_decimal(latest_close_price, 2),
                "price_change_pct": self._fmt_decimal(price_change_pct, 4),
            },
            "microstructure": {
                "avg_spread_bps": self._fmt_decimal(avg_spread_bps, 4),
                "latest_spread_bps": self._fmt_decimal(latest_spread_bps, 4),
                "avg_orderbook_imbalance": self._fmt_decimal(avg_orderbook_imbalance, 6),
                "latest_orderbook_imbalance": self._fmt_decimal(latest_orderbook_imbalance, 6),
            },
            "pattern_context": {
                "avg_pattern_score": self._fmt_decimal(avg_pattern_score, 6),
                "latest_pattern_score": self._fmt_decimal(latest_pattern_score, 6),
            },
            "scores": {
                "latest_volatility_score": self._fmt_decimal(latest_volatility_score, 6),
                "latest_trend_score": self._fmt_decimal(latest_trend_score, 6),
                "latest_liquidity_score": self._fmt_decimal(latest_liquidity_score, 6),
            },
            "derivatives": derivatives,
            "entry_blockers": list(entry_blockers),
            "analyst_summary_ko": analyst_summary_ko,
        }

    # ========================================================
    # Validation / Math helpers
    # ========================================================

    def _validate_time_order(self, rows: Sequence[MarketFeaturePoint]) -> None:
        prev: Optional[int] = None
        for row in rows:
            if prev is not None and row.ts_ms <= prev:
                raise RuntimeError("market_features ts_ms must be strictly increasing")
            prev = row.ts_ms

    def _calc_price_change_pct(self, first_price: Decimal, last_price: Decimal) -> Decimal:
        if first_price <= Decimal("0") or last_price <= Decimal("0"):
            raise RuntimeError("Prices must be > 0 for price change calculation")
        return ((last_price - first_price) / first_price) * Decimal("100")

    def _avg_decimal(self, values: Sequence[Decimal], field_name: str) -> Decimal:
        if not values:
            raise RuntimeError(f"Cannot calculate average for empty field: {field_name}")
        total = sum(values, Decimal("0"))
        return total / Decimal(len(values))

    # ========================================================
    # Parsing helpers
    # ========================================================

    def _parse_str(self, row: Mapping[str, Any], key: str) -> str:
        value = row.get(key)
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"Missing or invalid string field: {key}")
        return value

    def _parse_int(self, row: Mapping[str, Any], key: str) -> int:
        if key not in row:
            raise RuntimeError(f"Missing required int field: {key}")
        value = row[key]
        if isinstance(value, bool):
            raise RuntimeError(f"Invalid int field type for {key}: bool")
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Invalid int field: {key}") from exc

    def _parse_decimal(self, row: Mapping[str, Any], key: str) -> Decimal:
        if key not in row:
            raise RuntimeError(f"Missing required decimal field: {key}")
        return self._to_decimal(row[key], key)

    def _parse_optional_decimal(self, row: Mapping[str, Any], key: str) -> Optional[Decimal]:
        if key not in row or row[key] in (None, ""):
            return None
        return self._to_decimal(row[key], key)

    def _to_decimal(self, value: Any, field_name: str) -> Decimal:
        if isinstance(value, bool):
            raise RuntimeError(f"Invalid decimal field type for {field_name}: bool")
        try:
            dec = Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise RuntimeError(f"Invalid decimal field for {field_name}") from exc
        if not dec.is_finite():
            raise RuntimeError(f"Non-finite decimal field for {field_name}")
        return dec

    # ========================================================
    # Settings helpers
    # ========================================================

    def _require_str_setting(self, name: str) -> str:
        value = getattr(SETTINGS, name, None)
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"Missing or invalid required setting: {name}")
        return value

    def _require_int_setting(self, name: str) -> int:
        value = getattr(SETTINGS, name, None)
        if isinstance(value, bool):
            raise RuntimeError(f"Invalid bool value for integer setting: {name}")
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Missing or invalid required int setting: {name}") from exc
        return parsed

    def _require_decimal_setting(self, name: str) -> Decimal:
        value = getattr(SETTINGS, name, None)
        if value is None:
            raise RuntimeError(f"Missing required decimal setting: {name}")
        return self._to_decimal(value, name)

    # ========================================================
    # Formatting / KO mapping
    # ========================================================

    def _fmt_decimal(self, value: Decimal, scale: int) -> str:
        if scale < 0:
            raise RuntimeError("scale must be >= 0")
        return f"{value:.{scale}f}"

    def _fmt_decimal_optional(self, value: Optional[Decimal], scale: int) -> Optional[str]:
        if value is None:
            return None
        return self._fmt_decimal(value, scale)

    def _ko_trend(self, value: str) -> str:
        mapping = {
            "strong_uptrend": "강한 상승 추세",
            "weak_uptrend": "약한 상승 추세",
            "strong_downtrend": "강한 하락 추세",
            "weak_downtrend": "약한 하락 추세",
            "sideways": "횡보",
        }
        return self._map_or_raise(mapping, value, "trend")

    def _ko_volatility(self, value: str) -> str:
        mapping = {
            "high": "높음",
            "medium": "중간",
            "low": "낮음",
        }
        return self._map_or_raise(mapping, value, "volatility")

    def _ko_liquidity(self, value: str) -> str:
        mapping = {
            "high": "높음",
            "normal": "보통",
            "thin": "얇음",
        }
        return self._map_or_raise(mapping, value, "liquidity")

    def _map_or_raise(self, mapping: Dict[str, str], value: str, field_name: str) -> str:
        if value not in mapping:
            raise RuntimeError(f"Unexpected {field_name} value: {value}")
        return mapping[value]