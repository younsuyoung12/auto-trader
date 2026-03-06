"""
========================================================
FILE: analysis/market_researcher.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할:
- Binance USDⓈ-M Futures 공개 시장 데이터를 해석해 외부 시장 분석 리포트를 생성한다.
- 내부 DB 분석과 분리된 "외부 시장 분석(Market Research)" 레이어를 담당한다.
- 주문/포지션/리스크 실행 로직에는 관여하지 않는다.

절대 원칙:
- STRICT · NO-FALLBACK
- settings.py(SSOT) 외 환경변수 직접 접근 금지
- print() 금지 / logging 사용
- 데이터 누락/손상/모호성 발생 시 즉시 예외
- 민감정보 로그 금지
- Binance 공개 시장 데이터만 사용

변경 이력:
2026-03-07
- 신규 생성
- Binance 외부 시장 구조 분석기 추가
========================================================
"""

from __future__ import annotations

import logging
import math
from dataclasses import asdict, dataclass
from decimal import Decimal
from statistics import pstdev
from typing import Any, Dict, List, Sequence

from analysis.binance_market_fetcher import (
    BinanceForceOrder,
    BinanceKline,
    BinanceMarketFetcher,
    BinanceMarketSnapshot,
    BinanceRatioPoint,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MarketResearchReport:
    symbol: str
    as_of_ms: int
    price: Decimal
    mark_price: Decimal
    index_price: Decimal
    spread_bps: Decimal
    orderbook_imbalance: Decimal
    realized_volatility_pct: Decimal
    price_change_pct: Decimal
    support_price: Decimal
    resistance_price: Decimal
    funding_rate: Decimal
    funding_bias: str
    open_interest: Decimal
    open_interest_change_pct: Decimal
    open_interest_trend: str
    global_long_short_ratio: Decimal
    top_long_short_ratio: Decimal
    taker_long_short_ratio: Decimal
    crowding_bias: str
    long_liquidation_notional: Decimal
    short_liquidation_notional: Decimal
    liquidation_pressure: str
    trend: str
    volatility: str
    liquidity: str
    market_regime: str
    conviction: str
    key_signals: List[str]
    analyst_summary_ko: str
    dashboard_payload: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class MarketResearcher:
    """
    Binance 공개 선물 데이터를 기반으로 외부 시장 구조 분석 수행.

    출력:
    - 정량 지표
    - 구조적 분류(trend / volatility / liquidity / regime)
    - 대시보드/LLM 연동용 payload
    - 한국어 요약
    """

    def __init__(self) -> None:
        self._fetcher = BinanceMarketFetcher()

    # ========================================================
    # Public API
    # ========================================================

    def run(self) -> MarketResearchReport:
        snapshot = self._fetcher.fetch_market_snapshot()
        report = self._build_report(snapshot)
        logger.info(
            "Market research completed: symbol=%s trend=%s regime=%s conviction=%s",
            report.symbol,
            report.trend,
            report.market_regime,
            report.conviction,
        )
        return report

    def build_prompt_context(self) -> Dict[str, Any]:
        report = self.run()
        return report.dashboard_payload

    # ========================================================
    # Core analysis
    # ========================================================

    def _build_report(self, snapshot: BinanceMarketSnapshot) -> MarketResearchReport:
        klines = snapshot.klines
        if len(klines) < 5:
            raise RuntimeError("At least 5 klines are required for market research")

        price = klines[-1].close_price
        if price <= Decimal("0"):
            raise RuntimeError("Last close price must be > 0")

        mark_price = snapshot.premium_index.mark_price
        index_price = snapshot.premium_index.index_price
        spread_bps = snapshot.book_ticker.spread_bps
        orderbook_imbalance = self._calc_orderbook_imbalance(snapshot.depth.bids, snapshot.depth.asks)

        realized_volatility_pct = self._calc_realized_volatility_pct(klines)
        price_change_pct = self._calc_price_change_pct(klines)
        support_price, resistance_price = self._calc_support_resistance(klines)

        funding_rate = snapshot.premium_index.last_funding_rate
        funding_bias = self._classify_funding_bias(funding_rate)

        open_interest = snapshot.open_interest.open_interest
        open_interest_change_pct = self._calc_open_interest_change_pct(snapshot)
        open_interest_trend = self._classify_open_interest_trend(open_interest_change_pct)

        global_ratio = self._latest_ratio(snapshot.global_long_short_account_ratio)
        top_ratio = self._latest_ratio(snapshot.top_long_short_position_ratio)
        taker_ratio = self._latest_ratio(snapshot.taker_long_short_ratio)
        crowding_bias = self._classify_crowding_bias(global_ratio, top_ratio, taker_ratio)

        long_liq, short_liq = self._calc_liquidation_notional(snapshot.force_orders)
        liquidation_pressure = self._classify_liquidation_pressure(long_liq, short_liq)

        trend = self._classify_trend(
            price_change_pct=price_change_pct,
            funding_rate=funding_rate,
            open_interest_change_pct=open_interest_change_pct,
        )
        volatility = self._classify_volatility(realized_volatility_pct)
        liquidity = self._classify_liquidity(spread_bps)
        market_regime = self._classify_market_regime(
            trend=trend,
            volatility=volatility,
            open_interest_trend=open_interest_trend,
            crowding_bias=crowding_bias,
            liquidation_pressure=liquidation_pressure,
            price=price,
            support=support_price,
            resistance=resistance_price,
        )
        conviction = self._classify_conviction(
            trend=trend,
            volatility=volatility,
            liquidity=liquidity,
            market_regime=market_regime,
            open_interest_trend=open_interest_trend,
        )

        key_signals = self._build_key_signals(
            trend=trend,
            volatility=volatility,
            liquidity=liquidity,
            market_regime=market_regime,
            funding_bias=funding_bias,
            open_interest_trend=open_interest_trend,
            crowding_bias=crowding_bias,
            liquidation_pressure=liquidation_pressure,
            spread_bps=spread_bps,
            orderbook_imbalance=orderbook_imbalance,
            support=support_price,
            resistance=resistance_price,
            price=price,
        )

        analyst_summary_ko = self._build_korean_summary(
            symbol=snapshot.symbol,
            price=price,
            trend=trend,
            volatility=volatility,
            liquidity=liquidity,
            market_regime=market_regime,
            funding_bias=funding_bias,
            open_interest_trend=open_interest_trend,
            crowding_bias=crowding_bias,
            liquidation_pressure=liquidation_pressure,
            support=support_price,
            resistance=resistance_price,
            price_change_pct=price_change_pct,
            realized_volatility_pct=realized_volatility_pct,
            spread_bps=spread_bps,
        )

        dashboard_payload = self._build_dashboard_payload(
            snapshot=snapshot,
            price=price,
            mark_price=mark_price,
            index_price=index_price,
            spread_bps=spread_bps,
            orderbook_imbalance=orderbook_imbalance,
            realized_volatility_pct=realized_volatility_pct,
            price_change_pct=price_change_pct,
            support_price=support_price,
            resistance_price=resistance_price,
            funding_rate=funding_rate,
            funding_bias=funding_bias,
            open_interest=open_interest,
            open_interest_change_pct=open_interest_change_pct,
            open_interest_trend=open_interest_trend,
            global_ratio=global_ratio,
            top_ratio=top_ratio,
            taker_ratio=taker_ratio,
            crowding_bias=crowding_bias,
            long_liq=long_liq,
            short_liq=short_liq,
            liquidation_pressure=liquidation_pressure,
            trend=trend,
            volatility=volatility,
            liquidity=liquidity,
            market_regime=market_regime,
            conviction=conviction,
            key_signals=key_signals,
            analyst_summary_ko=analyst_summary_ko,
        )

        return MarketResearchReport(
            symbol=snapshot.symbol,
            as_of_ms=snapshot.server_time_ms,
            price=price,
            mark_price=mark_price,
            index_price=index_price,
            spread_bps=spread_bps,
            orderbook_imbalance=orderbook_imbalance,
            realized_volatility_pct=realized_volatility_pct,
            price_change_pct=price_change_pct,
            support_price=support_price,
            resistance_price=resistance_price,
            funding_rate=funding_rate,
            funding_bias=funding_bias,
            open_interest=open_interest,
            open_interest_change_pct=open_interest_change_pct,
            open_interest_trend=open_interest_trend,
            global_long_short_ratio=global_ratio,
            top_long_short_ratio=top_ratio,
            taker_long_short_ratio=taker_ratio,
            crowding_bias=crowding_bias,
            long_liquidation_notional=long_liq,
            short_liquidation_notional=short_liq,
            liquidation_pressure=liquidation_pressure,
            trend=trend,
            volatility=volatility,
            liquidity=liquidity,
            market_regime=market_regime,
            conviction=conviction,
            key_signals=key_signals,
            analyst_summary_ko=analyst_summary_ko,
            dashboard_payload=dashboard_payload,
        )

    # ========================================================
    # Quant calculations
    # ========================================================

    def _calc_orderbook_imbalance(self, bids: Sequence[Any], asks: Sequence[Any]) -> Decimal:
        if not bids:
            raise RuntimeError("Depth bids is empty")
        if not asks:
            raise RuntimeError("Depth asks is empty")

        bid_qty = sum((level.qty for level in bids), Decimal("0"))
        ask_qty = sum((level.qty for level in asks), Decimal("0"))
        total = bid_qty + ask_qty
        if total <= Decimal("0"):
            raise RuntimeError("Orderbook total quantity must be > 0")
        return (bid_qty - ask_qty) / total

    def _calc_realized_volatility_pct(self, klines: Sequence[BinanceKline]) -> Decimal:
        closes = [float(k.close_price) for k in klines]
        if len(closes) < 5:
            raise RuntimeError("At least 5 close prices are required for volatility")

        returns: List[float] = []
        for prev, curr in zip(closes[:-1], closes[1:]):
            if prev <= 0.0 or curr <= 0.0:
                raise RuntimeError("Close prices must be > 0 for volatility calculation")
            returns.append(math.log(curr / prev))

        if len(returns) < 2:
            raise RuntimeError("At least 2 returns are required for volatility")
        sigma = pstdev(returns)
        return Decimal(str(sigma * 100.0))

    def _calc_price_change_pct(self, klines: Sequence[BinanceKline]) -> Decimal:
        first = klines[0].open_price
        last = klines[-1].close_price
        if first <= Decimal("0") or last <= Decimal("0"):
            raise RuntimeError("Kline prices must be > 0 for price change calculation")
        return ((last - first) / first) * Decimal("100")

    def _calc_support_resistance(self, klines: Sequence[BinanceKline]) -> tuple[Decimal, Decimal]:
        lows = [k.low_price for k in klines]
        highs = [k.high_price for k in klines]
        if not lows or not highs:
            raise RuntimeError("Klines are required for support/resistance")
        support = min(lows)
        resistance = max(highs)
        if support <= Decimal("0") or resistance <= Decimal("0"):
            raise RuntimeError("Support/resistance must be > 0")
        if resistance < support:
            raise RuntimeError("Resistance must be >= support")
        return support, resistance

    def _calc_open_interest_change_pct(self, snapshot: BinanceMarketSnapshot) -> Decimal:
        hist = snapshot.open_interest_history
        if len(hist) < 2:
            raise RuntimeError("At least 2 open interest history points are required")

        first = hist[0].sum_open_interest
        last = hist[-1].sum_open_interest
        if first <= Decimal("0") or last <= Decimal("0"):
            raise RuntimeError("Open interest history values must be > 0")
        return ((last - first) / first) * Decimal("100")

    def _latest_ratio(self, items: Sequence[BinanceRatioPoint]) -> Decimal:
        if not items:
            raise RuntimeError("Ratio series is empty")
        ratio = items[-1].long_short_ratio
        if ratio <= Decimal("0"):
            raise RuntimeError("Latest long/short ratio must be > 0")
        return ratio

    def _calc_liquidation_notional(self, force_orders: Sequence[BinanceForceOrder]) -> tuple[Decimal, Decimal]:
        long_liq = Decimal("0")
        short_liq = Decimal("0")

        for item in force_orders:
            notional = item.average_price * item.filled_quantity
            if notional < Decimal("0"):
                raise RuntimeError("Force order notional must be >= 0")

            side = item.side.upper().strip()
            if side == "SELL":
                long_liq += notional
            elif side == "BUY":
                short_liq += notional
            else:
                raise RuntimeError(f"Unexpected force order side: {item.side}")

        return long_liq, short_liq

    # ========================================================
    # Classification rules
    # ========================================================

    def _classify_funding_bias(self, funding_rate: Decimal) -> str:
        if funding_rate >= Decimal("0.0008"):
            return "strong_long_bias"
        if funding_rate >= Decimal("0.0001"):
            return "mild_long_bias"
        if funding_rate <= Decimal("-0.0008"):
            return "strong_short_bias"
        if funding_rate <= Decimal("-0.0001"):
            return "mild_short_bias"
        return "neutral"

    def _classify_open_interest_trend(self, oi_change_pct: Decimal) -> str:
        if oi_change_pct >= Decimal("5.0"):
            return "rising_strong"
        if oi_change_pct >= Decimal("1.0"):
            return "rising"
        if oi_change_pct <= Decimal("-5.0"):
            return "falling_strong"
        if oi_change_pct <= Decimal("-1.0"):
            return "falling"
        return "flat"

    def _classify_crowding_bias(
        self,
        global_ratio: Decimal,
        top_ratio: Decimal,
        taker_ratio: Decimal,
    ) -> str:
        bullish_votes = 0
        bearish_votes = 0

        for ratio in (global_ratio, top_ratio, taker_ratio):
            if ratio >= Decimal("1.20"):
                bullish_votes += 1
            elif ratio <= Decimal("0.83"):
                bearish_votes += 1

        if bullish_votes >= 2:
            return "long_crowded"
        if bearish_votes >= 2:
            return "short_crowded"
        return "balanced"

    def _classify_liquidation_pressure(self, long_liq: Decimal, short_liq: Decimal) -> str:
        total = long_liq + short_liq
        if total <= Decimal("0"):
            return "none"

        long_share = long_liq / total
        short_share = short_liq / total

        if long_share >= Decimal("0.70"):
            return "long_flush"
        if short_share >= Decimal("0.70"):
            return "short_squeeze"
        return "mixed"

    def _classify_trend(
        self,
        price_change_pct: Decimal,
        funding_rate: Decimal,
        open_interest_change_pct: Decimal,
    ) -> str:
        bullish_confirm = funding_rate > Decimal("0") and open_interest_change_pct > Decimal("0")
        bearish_confirm = funding_rate < Decimal("0") and open_interest_change_pct > Decimal("0")

        if price_change_pct >= Decimal("2.0") and bullish_confirm:
            return "strong_uptrend"
        if price_change_pct >= Decimal("0.6"):
            return "weak_uptrend"
        if price_change_pct <= Decimal("-2.0") and bearish_confirm:
            return "strong_downtrend"
        if price_change_pct <= Decimal("-0.6"):
            return "weak_downtrend"
        return "sideways"

    def _classify_volatility(self, realized_volatility_pct: Decimal) -> str:
        if realized_volatility_pct >= Decimal("2.2"):
            return "high"
        if realized_volatility_pct >= Decimal("0.9"):
            return "medium"
        return "low"

    def _classify_liquidity(self, spread_bps: Decimal) -> str:
        if spread_bps <= Decimal("1.5"):
            return "high"
        if spread_bps <= Decimal("4.0"):
            return "normal"
        return "thin"

    def _classify_market_regime(
        self,
        trend: str,
        volatility: str,
        open_interest_trend: str,
        crowding_bias: str,
        liquidation_pressure: str,
        price: Decimal,
        support: Decimal,
        resistance: Decimal,
    ) -> str:
        if price <= Decimal("0") or support <= Decimal("0") or resistance <= Decimal("0"):
            raise RuntimeError("Price/support/resistance must be > 0")
        if resistance < support:
            raise RuntimeError("Resistance must be >= support")

        range_span_pct = ((resistance - support) / price) * Decimal("100")

        if trend in {"strong_uptrend", "strong_downtrend"} and open_interest_trend in {"rising", "rising_strong"}:
            return "trend_expansion"
        if liquidation_pressure in {"long_flush", "short_squeeze"} and volatility == "high":
            return "squeeze"
        if range_span_pct <= Decimal("3.0") and trend == "sideways":
            return "range"
        if crowding_bias != "balanced" and volatility != "low":
            return "crowded_directional"
        return "transitional"

    def _classify_conviction(
        self,
        trend: str,
        volatility: str,
        liquidity: str,
        market_regime: str,
        open_interest_trend: str,
    ) -> str:
        strong_conditions = 0

        if trend in {"strong_uptrend", "strong_downtrend"}:
            strong_conditions += 1
        if volatility in {"medium", "high"}:
            strong_conditions += 1
        if liquidity in {"high", "normal"}:
            strong_conditions += 1
        if market_regime in {"trend_expansion", "squeeze", "crowded_directional"}:
            strong_conditions += 1
        if open_interest_trend in {"rising", "rising_strong"}:
            strong_conditions += 1

        if strong_conditions >= 4:
            return "high"
        if strong_conditions >= 2:
            return "medium"
        return "low"

    # ========================================================
    # Presentation builders
    # ========================================================

    def _build_key_signals(
        self,
        trend: str,
        volatility: str,
        liquidity: str,
        market_regime: str,
        funding_bias: str,
        open_interest_trend: str,
        crowding_bias: str,
        liquidation_pressure: str,
        spread_bps: Decimal,
        orderbook_imbalance: Decimal,
        support: Decimal,
        resistance: Decimal,
        price: Decimal,
    ) -> List[str]:
        signals: List[str] = []

        signals.append(f"trend={trend}")
        signals.append(f"volatility={volatility}")
        signals.append(f"liquidity={liquidity}")
        signals.append(f"regime={market_regime}")
        signals.append(f"funding_bias={funding_bias}")
        signals.append(f"oi_trend={open_interest_trend}")
        signals.append(f"crowding={crowding_bias}")
        signals.append(f"liquidation_pressure={liquidation_pressure}")
        signals.append(f"spread_bps={self._fmt_decimal(spread_bps, 2)}")
        signals.append(f"orderbook_imbalance={self._fmt_decimal(orderbook_imbalance, 4)}")
        signals.append(f"support={self._fmt_decimal(support, 2)}")
        signals.append(f"resistance={self._fmt_decimal(resistance, 2)}")
        signals.append(f"last_price={self._fmt_decimal(price, 2)}")

        return signals

    def _build_korean_summary(
        self,
        symbol: str,
        price: Decimal,
        trend: str,
        volatility: str,
        liquidity: str,
        market_regime: str,
        funding_bias: str,
        open_interest_trend: str,
        crowding_bias: str,
        liquidation_pressure: str,
        support: Decimal,
        resistance: Decimal,
        price_change_pct: Decimal,
        realized_volatility_pct: Decimal,
        spread_bps: Decimal,
    ) -> str:
        parts: List[str] = []

        parts.append(
            f"{symbol} 외부 시장 분석 결과, 현재 가격은 {self._fmt_decimal(price, 2)} 수준이며 "
            f"단기 변화율은 {self._fmt_decimal(price_change_pct, 2)}% 입니다."
        )
        parts.append(
            f"추세는 {self._ko_trend(trend)}, 변동성은 {self._ko_volatility(volatility)}, "
            f"유동성은 {self._ko_liquidity(liquidity)}로 분류됩니다."
        )
        parts.append(
            f"현재 시장 레짐은 {self._ko_regime(market_regime)}이며, "
            f"펀딩 구조는 {self._ko_funding_bias(funding_bias)}, "
            f"오픈이자 흐름은 {self._ko_oi_trend(open_interest_trend)} 입니다."
        )
        parts.append(
            f"포지션 쏠림은 {self._ko_crowding(crowding_bias)}, "
            f"청산 압력은 {self._ko_liquidation(liquidation_pressure)}로 해석됩니다."
        )
        parts.append(
            f"단기 지지 구간은 {self._fmt_decimal(support, 2)}, "
            f"저항 구간은 {self._fmt_decimal(resistance, 2)} 입니다."
        )
        parts.append(
            f"실현 변동성은 {self._fmt_decimal(realized_volatility_pct, 2)}%, "
            f"호가 스프레드는 {self._fmt_decimal(spread_bps, 2)} bps 수준입니다."
        )

        return " ".join(parts)

    def _build_dashboard_payload(
        self,
        snapshot: BinanceMarketSnapshot,
        price: Decimal,
        mark_price: Decimal,
        index_price: Decimal,
        spread_bps: Decimal,
        orderbook_imbalance: Decimal,
        realized_volatility_pct: Decimal,
        price_change_pct: Decimal,
        support_price: Decimal,
        resistance_price: Decimal,
        funding_rate: Decimal,
        funding_bias: str,
        open_interest: Decimal,
        open_interest_change_pct: Decimal,
        open_interest_trend: str,
        global_ratio: Decimal,
        top_ratio: Decimal,
        taker_ratio: Decimal,
        crowding_bias: str,
        long_liq: Decimal,
        short_liq: Decimal,
        liquidation_pressure: str,
        trend: str,
        volatility: str,
        liquidity: str,
        market_regime: str,
        conviction: str,
        key_signals: List[str],
        analyst_summary_ko: str,
    ) -> Dict[str, Any]:
        return {
            "symbol": snapshot.symbol,
            "as_of_ms": snapshot.server_time_ms,
            "server_drift_ms": snapshot.drift_ms,
            "market_structure": {
                "trend": trend,
                "volatility": volatility,
                "liquidity": liquidity,
                "market_regime": market_regime,
                "conviction": conviction,
            },
            "price_context": {
                "last_price": self._fmt_decimal(price, 2),
                "mark_price": self._fmt_decimal(mark_price, 2),
                "index_price": self._fmt_decimal(index_price, 2),
                "price_change_pct": self._fmt_decimal(price_change_pct, 4),
                "support_price": self._fmt_decimal(support_price, 2),
                "resistance_price": self._fmt_decimal(resistance_price, 2),
            },
            "microstructure": {
                "spread_bps": self._fmt_decimal(spread_bps, 4),
                "orderbook_imbalance": self._fmt_decimal(orderbook_imbalance, 6),
            },
            "derivatives": {
                "funding_rate": self._fmt_decimal(funding_rate, 8),
                "funding_bias": funding_bias,
                "open_interest": self._fmt_decimal(open_interest, 6),
                "open_interest_change_pct": self._fmt_decimal(open_interest_change_pct, 4),
                "open_interest_trend": open_interest_trend,
                "global_long_short_ratio": self._fmt_decimal(global_ratio, 4),
                "top_long_short_ratio": self._fmt_decimal(top_ratio, 4),
                "taker_long_short_ratio": self._fmt_decimal(taker_ratio, 4),
                "crowding_bias": crowding_bias,
            },
            "liquidation": {
                "long_liquidation_notional": self._fmt_decimal(long_liq, 2),
                "short_liquidation_notional": self._fmt_decimal(short_liq, 2),
                "liquidation_pressure": liquidation_pressure,
            },
            "risk_metrics": {
                "realized_volatility_pct": self._fmt_decimal(realized_volatility_pct, 4),
            },
            "key_signals": key_signals,
            "analyst_summary_ko": analyst_summary_ko,
            "raw_meta": {
                "kline_count": len(snapshot.klines),
                "funding_count": len(snapshot.funding_rates),
                "oi_hist_count": len(snapshot.open_interest_history),
                "global_ratio_count": len(snapshot.global_long_short_account_ratio),
                "top_ratio_count": len(snapshot.top_long_short_position_ratio),
                "taker_ratio_count": len(snapshot.taker_long_short_ratio),
                "force_order_count": len(snapshot.force_orders),
            },
        }

    # ========================================================
    # KO mappers
    # ========================================================

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
            "high": "매우 좋음",
            "normal": "보통",
            "thin": "얇음",
        }
        return self._map_or_raise(mapping, value, "liquidity")

    def _ko_regime(self, value: str) -> str:
        mapping = {
            "trend_expansion": "추세 확장 구간",
            "squeeze": "스퀴즈 구간",
            "range": "박스권 구간",
            "crowded_directional": "포지션 쏠림 방향성 구간",
            "transitional": "전환 구간",
        }
        return self._map_or_raise(mapping, value, "market_regime")

    def _ko_funding_bias(self, value: str) -> str:
        mapping = {
            "strong_long_bias": "강한 롱 우위",
            "mild_long_bias": "약한 롱 우위",
            "neutral": "중립",
            "mild_short_bias": "약한 숏 우위",
            "strong_short_bias": "강한 숏 우위",
        }
        return self._map_or_raise(mapping, value, "funding_bias")

    def _ko_oi_trend(self, value: str) -> str:
        mapping = {
            "rising_strong": "강한 증가",
            "rising": "증가",
            "flat": "정체",
            "falling": "감소",
            "falling_strong": "강한 감소",
        }
        return self._map_or_raise(mapping, value, "open_interest_trend")

    def _ko_crowding(self, value: str) -> str:
        mapping = {
            "long_crowded": "롱 과밀",
            "short_crowded": "숏 과밀",
            "balanced": "균형",
        }
        return self._map_or_raise(mapping, value, "crowding_bias")

    def _ko_liquidation(self, value: str) -> str:
        mapping = {
            "long_flush": "롱 청산 우세",
            "short_squeeze": "숏 청산 우세",
            "mixed": "혼합 청산",
            "none": "청산 압력 미미",
        }
        return self._map_or_raise(mapping, value, "liquidation_pressure")

    # ========================================================
    # Utility
    # ========================================================

    def _fmt_decimal(self, value: Decimal, scale: int) -> str:
        if scale < 0:
            raise RuntimeError("scale must be >= 0")
        return f"{value:.{scale}f}"

    def _map_or_raise(self, mapping: Dict[str, str], value: str, field_name: str) -> str:
        if value not in mapping:
            raise RuntimeError(f"Unexpected {field_name} value: {value}")
        return mapping[value]