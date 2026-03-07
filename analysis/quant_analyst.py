"""
========================================================
FILE: analysis/quant_analyst.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할:
- 내부 DB 기반 시장 분석 + 거래 분석 + 외부 Binance 시장 분석을 통합한다.
- GPT Analyst Engine에 분석 컨텍스트를 전달해 최종 분석/원인/해결책을 생성한다.
- 대시보드 검색창의 단일 오케스트레이션 레이어로 동작한다.
- 주문 실행 / 포지션 변경 / TP·SL 수정은 절대 수행하지 않는다.

절대 원칙:
- STRICT · NO-FALLBACK
- settings.py(SSOT) 외 환경변수 직접 접근 금지
- print() 금지 / logging 사용
- 데이터 누락/손상/모호성 발생 시 즉시 예외
- 민감정보 로그 금지
- 예외 삼키기 금지
- 분석 실패 시 즉시 예외 전파

변경 이력:
2026-03-07
- 신규 생성
- 내부/외부 시장 분석 + 거래 분석 + GPT 분석 오케스트레이터 추가
- market-analysis 전용 순수 외부시장 분석 경로 추가
- 입력 사용 검증을 실제 제공된 분석 컨텍스트 기준으로 강화
- 대시보드 payload 생성 로직을 분석 범위별(optional card) 구조로 확장
========================================================
"""

from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

from analysis.gpt_analyst_engine import GptAnalystEngine, GptAnalystResult
from analysis.market_analyzer import InternalMarketSummary, MarketAnalyzer
from analysis.market_researcher import MarketResearchReport, MarketResearcher
from analysis.trade_analyzer import TradeAnalyzer, TradeAnalyzerSummary
from settings import SETTINGS

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class QuantAnalystResponse:
    symbol: str
    question: str
    generated_ts_ms: int
    internal_market_summary: Optional[Dict[str, Any]]
    trade_summary: Optional[Dict[str, Any]]
    external_market_summary: Optional[Dict[str, Any]]
    gpt_result: Dict[str, Any]
    dashboard_payload: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class QuantAnalyst:
    """
    AI Quant Analyst 통합 서비스.

    구성:
    - MarketAnalyzer      : 내부 DB 기반 시장 상태 분석
    - TradeAnalyzer       : 최근 거래/이벤트/스냅샷 기반 거래 분석
    - MarketResearcher    : Binance 외부 시장 구조 분석
    - GptAnalystEngine    : 한국어 최종 분석 / 원인 / 해결책 생성
    """

    def __init__(self) -> None:
        self._symbol = self._require_str_setting("ANALYST_MARKET_SYMBOL")
        self._include_external_market_default = self._require_bool_setting(
            "ANALYST_INCLUDE_EXTERNAL_MARKET"
        )

        self._market_analyzer = MarketAnalyzer()
        self._trade_analyzer = TradeAnalyzer()
        self._market_researcher = MarketResearcher()
        self._gpt_engine = GptAnalystEngine()

    # ========================================================
    # Public API
    # ========================================================

    def analyze(
        self,
        *,
        question: str,
        include_external_market: Optional[bool] = None,
    ) -> QuantAnalystResponse:
        include_external = self._resolve_include_external_market(include_external_market)
        return self._analyze_core(
            question=question,
            include_internal_market=True,
            include_trade_analysis=True,
            include_external_market=include_external,
        )

    def analyze_for_dashboard(
        self,
        *,
        question: str,
        include_external_market: Optional[bool] = None,
    ) -> Dict[str, Any]:
        return self.analyze(
            question=question,
            include_external_market=include_external_market,
        ).dashboard_payload

    def analyze_market_only(
        self,
        *,
        question: str,
    ) -> QuantAnalystResponse:
        return self._analyze_core(
            question=question,
            include_internal_market=False,
            include_trade_analysis=False,
            include_external_market=True,
        )

    def analyze_market_only_for_dashboard(
        self,
        *,
        question: str,
    ) -> Dict[str, Any]:
        return self.analyze_market_only(question=question).dashboard_payload

    # ========================================================
    # Core orchestration
    # ========================================================

    def _analyze_core(
        self,
        *,
        question: str,
        include_internal_market: bool,
        include_trade_analysis: bool,
        include_external_market: bool,
    ) -> QuantAnalystResponse:
        normalized_question = self._normalize_question(question)
        self._validate_analysis_scope(
            include_internal_market=include_internal_market,
            include_trade_analysis=include_trade_analysis,
            include_external_market=include_external_market,
        )

        internal_market_summary_obj: Optional[InternalMarketSummary] = None
        trade_summary_obj: Optional[TradeAnalyzerSummary] = None
        external_market_summary_obj: Optional[MarketResearchReport] = None

        if include_internal_market:
            internal_market_summary_obj = self._market_analyzer.run()

        if include_trade_analysis:
            trade_summary_obj = self._trade_analyzer.run()

        if include_external_market:
            external_market_summary_obj = self._market_researcher.run()

        internal_market_summary = (
            internal_market_summary_obj.to_dict()
            if internal_market_summary_obj is not None
            else None
        )
        trade_summary = trade_summary_obj.to_dict() if trade_summary_obj is not None else None
        external_market_summary = (
            external_market_summary_obj.to_dict()
            if external_market_summary_obj is not None
            else None
        )

        gpt_result_obj = self._gpt_engine.analyze(
            question=normalized_question,
            internal_market_summary=internal_market_summary,
            trade_summary=trade_summary,
            external_market_summary=external_market_summary,
        )

        self._validate_used_inputs(
            gpt_result=gpt_result_obj,
            internal_market_summary=internal_market_summary,
            trade_summary=trade_summary,
            external_market_summary=external_market_summary,
        )

        generated_ts_ms = self._now_ms()
        dashboard_payload = self._build_dashboard_payload(
            question=normalized_question,
            generated_ts_ms=generated_ts_ms,
            internal_market_summary_obj=internal_market_summary_obj,
            trade_summary_obj=trade_summary_obj,
            external_market_summary_obj=external_market_summary_obj,
            gpt_result_obj=gpt_result_obj,
        )

        result = QuantAnalystResponse(
            symbol=self._symbol,
            question=normalized_question,
            generated_ts_ms=generated_ts_ms,
            internal_market_summary=internal_market_summary,
            trade_summary=trade_summary,
            external_market_summary=external_market_summary,
            gpt_result=gpt_result_obj.to_dict(),
            dashboard_payload=dashboard_payload,
        )

        logger.info(
            (
                "Quant analysis completed: symbol=%s "
                "internal_market=%s trade_analysis=%s external_market=%s "
                "scope=%s confidence=%.3f"
            ),
            self._symbol,
            include_internal_market,
            include_trade_analysis,
            include_external_market,
            gpt_result_obj.scope,
            gpt_result_obj.confidence,
        )
        return result

    # ========================================================
    # Validation
    # ========================================================

    def _validate_used_inputs(
        self,
        *,
        gpt_result: GptAnalystResult,
        internal_market_summary: Optional[Dict[str, Any]],
        trade_summary: Optional[Dict[str, Any]],
        external_market_summary: Optional[Dict[str, Any]],
    ) -> None:
        used_inputs = set(gpt_result.used_inputs)

        provided_inputs = self._build_provided_inputs_set(
            internal_market_summary=internal_market_summary,
            trade_summary=trade_summary,
            external_market_summary=external_market_summary,
        )

        missing_required = provided_inputs - used_inputs
        if gpt_result.scope != "out_of_scope" and missing_required:
            raise RuntimeError(
                f"GPT result omitted required inputs: missing={sorted(missing_required)}"
            )

        if internal_market_summary is None and "internal_market_summary" in used_inputs:
            raise RuntimeError(
                "GPT result claims internal_market_summary usage but internal data was not provided"
            )

        if trade_summary is None and "trade_summary" in used_inputs:
            raise RuntimeError(
                "GPT result claims trade_summary usage but trade data was not provided"
            )

        if external_market_summary is None and "external_market_summary" in used_inputs:
            raise RuntimeError(
                "GPT result claims external_market_summary usage but external data was not provided"
            )

        unexpected = used_inputs - provided_inputs
        if unexpected:
            raise RuntimeError(
                f"GPT result contains unexpected used_inputs: {sorted(unexpected)}"
            )

    def _validate_analysis_scope(
        self,
        *,
        include_internal_market: bool,
        include_trade_analysis: bool,
        include_external_market: bool,
    ) -> None:
        for name, value in (
            ("include_internal_market", include_internal_market),
            ("include_trade_analysis", include_trade_analysis),
            ("include_external_market", include_external_market),
        ):
            if not isinstance(value, bool):
                raise RuntimeError(f"{name} must be bool")

        if not (
            include_internal_market
            or include_trade_analysis
            or include_external_market
        ):
            raise RuntimeError("At least one analysis input must be enabled")

    def _build_provided_inputs_set(
        self,
        *,
        internal_market_summary: Optional[Dict[str, Any]],
        trade_summary: Optional[Dict[str, Any]],
        external_market_summary: Optional[Dict[str, Any]],
    ) -> set[str]:
        provided_inputs: set[str] = set()

        if internal_market_summary is not None:
            provided_inputs.add("internal_market_summary")

        if trade_summary is not None:
            provided_inputs.add("trade_summary")

        if external_market_summary is not None:
            provided_inputs.add("external_market_summary")

        return provided_inputs

    # ========================================================
    # Dashboard payload
    # ========================================================

    def _build_dashboard_payload(
        self,
        *,
        question: str,
        generated_ts_ms: int,
        internal_market_summary_obj: Optional[InternalMarketSummary],
        trade_summary_obj: Optional[TradeAnalyzerSummary],
        external_market_summary_obj: Optional[MarketResearchReport],
        gpt_result_obj: GptAnalystResult,
    ) -> Dict[str, Any]:
        as_of_candidates: List[int] = []

        if internal_market_summary_obj is not None:
            as_of_candidates.append(internal_market_summary_obj.as_of_ms)

        if trade_summary_obj is not None:
            as_of_candidates.append(trade_summary_obj.as_of_ts_ms)

        if external_market_summary_obj is not None:
            as_of_candidates.append(external_market_summary_obj.as_of_ms)

        if not as_of_candidates:
            raise RuntimeError("as_of_candidates must not be empty")

        analysis_as_of_ts_ms = max(as_of_candidates)

        market_cards: Dict[str, Any] = {}

        if internal_market_summary_obj is not None:
            market_cards["internal_market"] = {
                "symbol": internal_market_summary_obj.symbol,
                "timeframe": internal_market_summary_obj.timeframe,
                "market_regime": internal_market_summary_obj.market_regime,
                "trend": internal_market_summary_obj.trend,
                "volatility": internal_market_summary_obj.volatility,
                "liquidity": internal_market_summary_obj.liquidity,
                "latest_close_price": self._fmt_decimal(
                    internal_market_summary_obj.latest_close_price, 2
                ),
                "price_change_pct": self._fmt_decimal(
                    internal_market_summary_obj.price_change_pct, 4
                ),
                "avg_spread_bps": self._fmt_decimal(
                    internal_market_summary_obj.avg_spread_bps, 4
                ),
                "latest_spread_bps": self._fmt_decimal(
                    internal_market_summary_obj.latest_spread_bps, 4
                ),
                "avg_pattern_score": self._fmt_decimal(
                    internal_market_summary_obj.avg_pattern_score, 6
                ),
                "latest_pattern_score": self._fmt_decimal(
                    internal_market_summary_obj.latest_pattern_score, 6
                ),
                "entry_blockers": list(internal_market_summary_obj.entry_blockers),
                "summary_ko": internal_market_summary_obj.analyst_summary_ko,
            }

        if trade_summary_obj is not None:
            market_cards["trade_performance"] = {
                "symbol": trade_summary_obj.symbol,
                "total_trades": trade_summary_obj.total_trades,
                "wins": trade_summary_obj.wins,
                "losses": trade_summary_obj.losses,
                "breakeven": trade_summary_obj.breakeven,
                "win_rate_pct": self._fmt_decimal(trade_summary_obj.win_rate_pct, 4),
                "total_pnl": self._fmt_decimal(trade_summary_obj.total_pnl, 4),
                "avg_pnl": self._fmt_decimal(trade_summary_obj.avg_pnl, 4),
                "avg_win_pnl": self._fmt_decimal(trade_summary_obj.avg_win_pnl, 4),
                "avg_loss_pnl": self._fmt_decimal(trade_summary_obj.avg_loss_pnl, 4),
                "entry_failure_reasons": list(trade_summary_obj.entry_failure_reasons),
                "loss_causes": list(trade_summary_obj.loss_causes),
                "recent_trade_briefs": list(trade_summary_obj.recent_trade_briefs),
                "summary_ko": trade_summary_obj.analyst_summary_ko,
            }

        if external_market_summary_obj is not None:
            market_cards["external_market"] = {
                "symbol": external_market_summary_obj.symbol,
                "trend": external_market_summary_obj.trend,
                "volatility": external_market_summary_obj.volatility,
                "liquidity": external_market_summary_obj.liquidity,
                "market_regime": external_market_summary_obj.market_regime,
                "conviction": external_market_summary_obj.conviction,
                "price": self._fmt_decimal(external_market_summary_obj.price, 2),
                "mark_price": self._fmt_decimal(external_market_summary_obj.mark_price, 2),
                "index_price": self._fmt_decimal(external_market_summary_obj.index_price, 2),
                "spread_bps": self._fmt_decimal(external_market_summary_obj.spread_bps, 4),
                "funding_rate": self._fmt_decimal(external_market_summary_obj.funding_rate, 8),
                "open_interest": self._fmt_decimal(external_market_summary_obj.open_interest, 6),
                "open_interest_change_pct": self._fmt_decimal(
                    external_market_summary_obj.open_interest_change_pct, 4
                ),
                "global_long_short_ratio": self._fmt_decimal(
                    external_market_summary_obj.global_long_short_ratio, 4
                ),
                "top_long_short_ratio": self._fmt_decimal(
                    external_market_summary_obj.top_long_short_ratio, 4
                ),
                "taker_long_short_ratio": self._fmt_decimal(
                    external_market_summary_obj.taker_long_short_ratio, 4
                ),
                "crowding_bias": external_market_summary_obj.crowding_bias,
                "liquidation_pressure": external_market_summary_obj.liquidation_pressure,
                "support_price": self._fmt_decimal(
                    external_market_summary_obj.support_price, 2
                ),
                "resistance_price": self._fmt_decimal(
                    external_market_summary_obj.resistance_price, 2
                ),
                "key_signals": list(external_market_summary_obj.key_signals),
                "summary_ko": external_market_summary_obj.analyst_summary_ko,
            }

        if not market_cards:
            raise RuntimeError("market_cards must not be empty")

        return {
            "symbol": self._symbol,
            "question": question,
            "generated_ts_ms": generated_ts_ms,
            "analysis_as_of_ts_ms": analysis_as_of_ts_ms,
            "scope": gpt_result_obj.scope,
            "confidence": gpt_result_obj.confidence,
            "answer_ko": gpt_result_obj.answer_ko,
            "root_causes": list(gpt_result_obj.root_causes),
            "recommendations": list(gpt_result_obj.recommendations),
            "used_inputs": list(gpt_result_obj.used_inputs),
            "market_cards": market_cards,
        }

    # ========================================================
    # Settings helpers
    # ========================================================

    def _require_str_setting(self, name: str) -> str:
        value = getattr(SETTINGS, name, None)
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"Missing or invalid required setting: {name}")
        return value.strip()

    def _require_bool_setting(self, name: str) -> bool:
        value = getattr(SETTINGS, name, None)

        if isinstance(value, bool):
            return value

        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "y", "on"}:
                return True
            if normalized in {"0", "false", "no", "n", "off"}:
                return False

        raise RuntimeError(f"Missing or invalid required bool setting: {name}")

    def _resolve_include_external_market(
        self,
        include_external_market: Optional[bool],
    ) -> bool:
        include_external = (
            self._include_external_market_default
            if include_external_market is None
            else include_external_market
        )
        if not isinstance(include_external, bool):
            raise RuntimeError("include_external_market must be bool when provided")
        return include_external

    # ========================================================
    # Misc helpers
    # ========================================================

    def _normalize_question(self, question: str) -> str:
        if not isinstance(question, str) or not question.strip():
            raise RuntimeError("question must be a non-empty string")
        return question.strip()

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    def _fmt_decimal(self, value: Any, scale: int) -> str:
        if scale < 0:
            raise RuntimeError("scale must be >= 0")
        return f"{value:.{scale}f}"


__all__ = [
    "QuantAnalyst",
    "QuantAnalystResponse",
]