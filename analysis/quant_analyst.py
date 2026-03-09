"""
========================================================
FILE: analysis/quant_analyst.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

핵심 변경 요약
- FIX(INTEGRITY): dashboard market_cards별 필수 키/심볼 정합성 검증 추가
- FIX(PAYLOAD): confidence / root_causes / recommendations / used_inputs 요소 타입 검증 강화
- FIX(RESPONSE): 최종 응답의 internal/trade/external summary symbol 무결성 검증 추가
- market-only / full-analysis scope 계약 검증 강화 유지
- 내부/거래/외부 분석 결과 symbol 정합성 검증 유지

코드 정리 내용
- dashboard payload 카드 검증 로직 분리
- summary mapping symbol 계약 검증 분리
- 문자열 리스트 검증 공통화
- 기존 오케스트레이션/GPT 호출 구조는 삭제 없이 유지

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
2026-03-09
1) FIX(INTEGRITY): dashboard market_cards별 필수 키/심볼 정합성 검증 추가
2) FIX(PAYLOAD): confidence / root_causes / recommendations / used_inputs 요소 타입 검증 강화
3) FIX(RESPONSE): 최종 응답의 internal/trade/external summary symbol 무결성 검증 추가
4) FIX(STRICT): market-only / full-analysis scope 계약 검증 강화
5) FIX(INTEGRITY): internal/trade/external summary symbol 정합성 검증 추가
6) FIX(PAYLOAD): dashboard payload / 최종 응답 타임스탬프 및 카드 구조 검증 강화
7) CLEANUP: scope별 검증 로직 분리

2026-03-08
1) MarketResearchReport 확장 필드(volume profile / orderflow / options)를 dashboard payload에 반영
2) external_market card 에 POC / Value Area / CVD / delta ratio / options bias 핵심 필드 추가
3) external_market card 에 volume_profile_summary / orderflow_summary / options_summary nested payload 추가
4) 기존 내부시장/거래분석/GPT 오케스트레이션 로직은 삭제 없이 유지
========================================================
"""

from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Mapping, Optional

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
            self._validate_summary_symbol_contract_or_raise(
                obj=internal_market_summary_obj,
                field_name="internal_market_summary_obj",
            )

        if include_trade_analysis:
            trade_summary_obj = self._trade_analyzer.run()
            self._validate_summary_symbol_contract_or_raise(
                obj=trade_summary_obj,
                field_name="trade_summary_obj",
            )

        if include_external_market:
            external_market_summary_obj = self._market_researcher.run()
            self._validate_summary_symbol_contract_or_raise(
                obj=external_market_summary_obj,
                field_name="external_market_summary_obj",
            )

        internal_market_summary = self._object_to_non_empty_mapping_or_none(
            obj=internal_market_summary_obj,
            field_name="internal_market_summary",
        )
        trade_summary = self._object_to_non_empty_mapping_or_none(
            obj=trade_summary_obj,
            field_name="trade_summary",
        )
        external_market_summary = self._object_to_non_empty_mapping_or_none(
            obj=external_market_summary_obj,
            field_name="external_market_summary",
        )

        self._validate_scope_input_contract_or_raise(
            include_internal_market=include_internal_market,
            include_trade_analysis=include_trade_analysis,
            include_external_market=include_external_market,
            internal_market_summary=internal_market_summary,
            trade_summary=trade_summary,
            external_market_summary=external_market_summary,
        )

        gpt_result_obj = self._dispatch_gpt_analysis_or_raise(
            question=normalized_question,
            include_internal_market=include_internal_market,
            include_trade_analysis=include_trade_analysis,
            include_external_market=include_external_market,
            internal_market_summary=internal_market_summary,
            trade_summary=trade_summary,
            external_market_summary=external_market_summary,
        )
        self._validate_gpt_result_or_raise(gpt_result_obj)

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
        self._validate_dashboard_payload_or_raise(dashboard_payload)

        gpt_result_dict = self._gpt_result_to_non_empty_mapping_or_raise(gpt_result_obj)

        result = QuantAnalystResponse(
            symbol=self._symbol,
            question=normalized_question,
            generated_ts_ms=generated_ts_ms,
            internal_market_summary=internal_market_summary,
            trade_summary=trade_summary,
            external_market_summary=external_market_summary,
            gpt_result=gpt_result_dict,
            dashboard_payload=dashboard_payload,
        )
        self._validate_response_or_raise(result)

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

    def _dispatch_gpt_analysis_or_raise(
        self,
        *,
        question: str,
        include_internal_market: bool,
        include_trade_analysis: bool,
        include_external_market: bool,
        internal_market_summary: Optional[Dict[str, Any]],
        trade_summary: Optional[Dict[str, Any]],
        external_market_summary: Optional[Dict[str, Any]],
    ) -> GptAnalystResult:
        self._validate_scope_input_contract_or_raise(
            include_internal_market=include_internal_market,
            include_trade_analysis=include_trade_analysis,
            include_external_market=include_external_market,
            internal_market_summary=internal_market_summary,
            trade_summary=trade_summary,
            external_market_summary=external_market_summary,
        )

        is_market_only_scope = (
            include_internal_market is False
            and include_trade_analysis is False
            and include_external_market is True
        )

        if is_market_only_scope:
            self._validate_market_only_scope_contract_or_raise(
                internal_market_summary=internal_market_summary,
                trade_summary=trade_summary,
                external_market_summary=external_market_summary,
            )
            return self._dispatch_market_only_gpt_analysis_or_raise(
                question=question,
                external_market_summary=external_market_summary,
            )

        self._validate_general_scope_contract_or_raise(
            include_internal_market=include_internal_market,
            include_trade_analysis=include_trade_analysis,
            include_external_market=include_external_market,
            internal_market_summary=internal_market_summary,
            trade_summary=trade_summary,
            external_market_summary=external_market_summary,
        )
        return self._dispatch_generic_gpt_analysis_or_raise(
            question=question,
            internal_market_summary=internal_market_summary,
            trade_summary=trade_summary,
            external_market_summary=external_market_summary,
        )

    def _dispatch_market_only_gpt_analysis_or_raise(
        self,
        *,
        question: str,
        external_market_summary: Optional[Dict[str, Any]],
    ) -> GptAnalystResult:
        if external_market_summary is None:
            raise RuntimeError(
                "external_market_summary is required for market-only GPT analysis"
            )

        candidates = (
            "analyze_market_only",
            "analyze_external_market_only",
        )

        for method_name in candidates:
            method = getattr(self._gpt_engine, method_name, None)
            if method is None:
                continue
            if not callable(method):
                raise RuntimeError(
                    f"GptAnalystEngine.{method_name} exists but is not callable"
                )
            result = method(
                question=question,
                external_market_summary=external_market_summary,
            )
            if not isinstance(result, GptAnalystResult):
                raise RuntimeError(
                    f"GptAnalystEngine.{method_name} must return GptAnalystResult"
                )
            return result

        raise RuntimeError(
            "GptAnalystEngine does not support market-only scope. "
            "Required: analyze_market_only(...) or analyze_external_market_only(...)."
        )

    def _dispatch_generic_gpt_analysis_or_raise(
        self,
        *,
        question: str,
        internal_market_summary: Optional[Dict[str, Any]],
        trade_summary: Optional[Dict[str, Any]],
        external_market_summary: Optional[Dict[str, Any]],
    ) -> GptAnalystResult:
        analyze_method = getattr(self._gpt_engine, "analyze", None)
        if analyze_method is None or not callable(analyze_method):
            raise RuntimeError("GptAnalystEngine.analyze must exist and be callable")

        result = analyze_method(
            question=question,
            internal_market_summary=internal_market_summary,
            trade_summary=trade_summary,
            external_market_summary=external_market_summary,
        )
        if not isinstance(result, GptAnalystResult):
            raise RuntimeError("GptAnalystEngine.analyze must return GptAnalystResult")
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

    def _validate_scope_input_contract_or_raise(
        self,
        *,
        include_internal_market: bool,
        include_trade_analysis: bool,
        include_external_market: bool,
        internal_market_summary: Optional[Dict[str, Any]],
        trade_summary: Optional[Dict[str, Any]],
        external_market_summary: Optional[Dict[str, Any]],
    ) -> None:
        if include_internal_market and internal_market_summary is None:
            raise RuntimeError(
                "include_internal_market=True but internal_market_summary is missing"
            )
        if (not include_internal_market) and internal_market_summary is not None:
            raise RuntimeError(
                "include_internal_market=False but internal_market_summary was provided"
            )

        if include_trade_analysis and trade_summary is None:
            raise RuntimeError(
                "include_trade_analysis=True but trade_summary is missing"
            )
        if (not include_trade_analysis) and trade_summary is not None:
            raise RuntimeError(
                "include_trade_analysis=False but trade_summary was provided"
            )

        if include_external_market and external_market_summary is None:
            raise RuntimeError(
                "include_external_market=True but external_market_summary is missing"
            )
        if (not include_external_market) and external_market_summary is not None:
            raise RuntimeError(
                "include_external_market=False but external_market_summary was provided"
            )

    def _validate_market_only_scope_contract_or_raise(
        self,
        *,
        internal_market_summary: Optional[Dict[str, Any]],
        trade_summary: Optional[Dict[str, Any]],
        external_market_summary: Optional[Dict[str, Any]],
    ) -> None:
        if internal_market_summary is not None:
            raise RuntimeError("market-only scope must not include internal_market_summary")
        if trade_summary is not None:
            raise RuntimeError("market-only scope must not include trade_summary")
        if external_market_summary is None:
            raise RuntimeError("market-only scope requires external_market_summary")

    def _validate_general_scope_contract_or_raise(
        self,
        *,
        include_internal_market: bool,
        include_trade_analysis: bool,
        include_external_market: bool,
        internal_market_summary: Optional[Dict[str, Any]],
        trade_summary: Optional[Dict[str, Any]],
        external_market_summary: Optional[Dict[str, Any]],
    ) -> None:
        if include_internal_market and internal_market_summary is None:
            raise RuntimeError("general scope internal_market_summary missing")
        if include_trade_analysis and trade_summary is None:
            raise RuntimeError("general scope trade_summary missing")
        if include_external_market and external_market_summary is None:
            raise RuntimeError("general scope external_market_summary missing")

    def _validate_summary_symbol_contract_or_raise(
        self,
        *,
        obj: Any,
        field_name: str,
    ) -> None:
        if obj is None:
            raise RuntimeError(f"{field_name} is missing (STRICT)")

        symbol_value = getattr(obj, "symbol", None)
        if not isinstance(symbol_value, str) or not symbol_value.strip():
            raise RuntimeError(f"{field_name}.symbol must be a non-empty string")

        normalized_symbol = self._normalize_symbol_contract_or_raise(
            symbol_value,
            f"{field_name}.symbol",
        )
        if normalized_symbol != self._symbol:
            raise RuntimeError(
                f"{field_name}.symbol mismatch: expected={self._symbol} got={normalized_symbol}"
            )

    def _validate_mapping_symbol_contract_or_raise(
        self,
        *,
        data: Mapping[str, Any],
        field_name: str,
    ) -> None:
        if "symbol" not in data:
            raise RuntimeError(f"{field_name}.symbol is missing")
        symbol_value = data["symbol"]
        if not isinstance(symbol_value, str) or not symbol_value.strip():
            raise RuntimeError(f"{field_name}.symbol must be a non-empty string")

        normalized_symbol = self._normalize_symbol_contract_or_raise(
            symbol_value,
            f"{field_name}.symbol",
        )
        if normalized_symbol != self._symbol:
            raise RuntimeError(
                f"{field_name}.symbol mismatch: expected={self._symbol} got={normalized_symbol}"
            )

    def _validate_gpt_result_or_raise(self, gpt_result: GptAnalystResult) -> None:
        if not isinstance(gpt_result.scope, str) or not gpt_result.scope.strip():
            raise RuntimeError("gpt_result.scope must be a non-empty string")
        if isinstance(gpt_result.confidence, bool):
            raise RuntimeError("gpt_result.confidence invalid bool type")
        try:
            confidence = float(gpt_result.confidence)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("gpt_result.confidence must be float-like") from exc
        if confidence < 0.0 or confidence > 1.0:
            raise RuntimeError("gpt_result.confidence must be between 0 and 1")
        if not isinstance(gpt_result.answer_ko, str) or not gpt_result.answer_ko.strip():
            raise RuntimeError("gpt_result.answer_ko must be a non-empty string")
        if not isinstance(gpt_result.root_causes, list):
            raise RuntimeError("gpt_result.root_causes must be a list")
        if not isinstance(gpt_result.recommendations, list):
            raise RuntimeError("gpt_result.recommendations must be a list")
        if not isinstance(gpt_result.used_inputs, list):
            raise RuntimeError("gpt_result.used_inputs must be a list")

        self._validate_string_list_or_raise(
            gpt_result.root_causes,
            "gpt_result.root_causes",
        )
        self._validate_string_list_or_raise(
            gpt_result.recommendations,
            "gpt_result.recommendations",
        )
        self._validate_string_list_or_raise(
            gpt_result.used_inputs,
            "gpt_result.used_inputs",
        )

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

    def _object_to_non_empty_mapping_or_none(
        self,
        *,
        obj: Any,
        field_name: str,
    ) -> Optional[Dict[str, Any]]:
        if obj is None:
            return None

        to_dict_method = getattr(obj, "to_dict", None)
        if to_dict_method is None or not callable(to_dict_method):
            raise RuntimeError(f"{field_name} object must provide callable to_dict()")

        data = to_dict_method()
        if not isinstance(data, Mapping) or not data:
            raise RuntimeError(f"{field_name} must be a non-empty mapping")

        normalized = dict(data)
        self._validate_mapping_symbol_contract_or_raise(
            data=normalized,
            field_name=field_name,
        )
        return normalized

    def _gpt_result_to_non_empty_mapping_or_raise(
        self,
        gpt_result_obj: GptAnalystResult,
    ) -> Dict[str, Any]:
        to_dict_method = getattr(gpt_result_obj, "to_dict", None)
        if to_dict_method is None or not callable(to_dict_method):
            raise RuntimeError("GptAnalystResult must provide callable to_dict()")

        data = to_dict_method()
        if not isinstance(data, Mapping) or not data:
            raise RuntimeError("GptAnalystResult.to_dict() must return a non-empty mapping")
        return dict(data)

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
                "poc_price": self._fmt_decimal(external_market_summary_obj.poc_price, 2),
                "value_area_low": self._fmt_decimal(external_market_summary_obj.value_area_low, 2),
                "value_area_high": self._fmt_decimal(external_market_summary_obj.value_area_high, 2),
                "poc_distance_bps": self._fmt_decimal(external_market_summary_obj.poc_distance_bps, 4),
                "price_location": external_market_summary_obj.price_location,
                "cvd": self._fmt_decimal(external_market_summary_obj.cvd, 6),
                "delta_ratio_pct": self._fmt_decimal(external_market_summary_obj.delta_ratio_pct, 4),
                "aggression_bias": external_market_summary_obj.aggression_bias,
                "cvd_trend": external_market_summary_obj.cvd_trend,
                "divergence": external_market_summary_obj.divergence,
                "put_call_oi_ratio": self._fmt_decimal(
                    external_market_summary_obj.put_call_oi_ratio, 4
                ),
                "put_call_volume_ratio": self._fmt_decimal(
                    external_market_summary_obj.put_call_volume_ratio, 4
                ),
                "options_bias": external_market_summary_obj.options_bias,
                "key_signals": list(external_market_summary_obj.key_signals),
                "volume_profile_summary": dict(external_market_summary_obj.volume_profile_summary),
                "orderflow_summary": dict(external_market_summary_obj.orderflow_summary),
                "options_summary": dict(external_market_summary_obj.options_summary),
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

    def _validate_dashboard_payload_or_raise(self, payload: Dict[str, Any]) -> None:
        if not isinstance(payload, dict) or not payload:
            raise RuntimeError("dashboard_payload must be a non-empty mapping")

        required_keys = (
            "symbol",
            "question",
            "generated_ts_ms",
            "analysis_as_of_ts_ms",
            "scope",
            "confidence",
            "answer_ko",
            "root_causes",
            "recommendations",
            "used_inputs",
            "market_cards",
        )
        for key in required_keys:
            if key not in payload:
                raise RuntimeError(f"dashboard_payload missing required key: {key}")

        if not isinstance(payload["symbol"], str) or not payload["symbol"].strip():
            raise RuntimeError("dashboard_payload.symbol must be a non-empty string")
        normalized_symbol = self._normalize_symbol_contract_or_raise(
            payload["symbol"],
            "dashboard_payload.symbol",
        )
        if normalized_symbol != self._symbol:
            raise RuntimeError(
                f"dashboard_payload.symbol mismatch: expected={self._symbol} got={payload['symbol']}"
            )
        if not isinstance(payload["question"], str) or not payload["question"].strip():
            raise RuntimeError("dashboard_payload.question must be a non-empty string")
        if not isinstance(payload["scope"], str) or not payload["scope"].strip():
            raise RuntimeError("dashboard_payload.scope must be a non-empty string")
        if not isinstance(payload["answer_ko"], str) or not payload["answer_ko"].strip():
            raise RuntimeError("dashboard_payload.answer_ko must be a non-empty string")
        if not isinstance(payload["root_causes"], list):
            raise RuntimeError("dashboard_payload.root_causes must be a list")
        if not isinstance(payload["recommendations"], list):
            raise RuntimeError("dashboard_payload.recommendations must be a list")
        if not isinstance(payload["used_inputs"], list):
            raise RuntimeError("dashboard_payload.used_inputs must be a list")
        if not isinstance(payload["market_cards"], dict) or not payload["market_cards"]:
            raise RuntimeError("dashboard_payload.market_cards must be a non-empty mapping")

        self._validate_string_list_or_raise(
            payload["root_causes"],
            "dashboard_payload.root_causes",
        )
        self._validate_string_list_or_raise(
            payload["recommendations"],
            "dashboard_payload.recommendations",
        )
        self._validate_string_list_or_raise(
            payload["used_inputs"],
            "dashboard_payload.used_inputs",
        )

        confidence = payload["confidence"]
        if isinstance(confidence, bool):
            raise RuntimeError("dashboard_payload.confidence must not be bool")
        try:
            confidence_f = float(confidence)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("dashboard_payload.confidence must be float-like") from exc
        if confidence_f < 0.0 or confidence_f > 1.0:
            raise RuntimeError("dashboard_payload.confidence must be between 0 and 1")

        generated_ts_ms = payload["generated_ts_ms"]
        analysis_as_of_ts_ms = payload["analysis_as_of_ts_ms"]
        if isinstance(generated_ts_ms, bool) or isinstance(analysis_as_of_ts_ms, bool):
            raise RuntimeError("dashboard payload timestamps must not be bool")
        try:
            generated_ts_ms_i = int(generated_ts_ms)
            analysis_as_of_ts_ms_i = int(analysis_as_of_ts_ms)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("dashboard payload timestamps must be int-like") from exc
        if generated_ts_ms_i <= 0 or analysis_as_of_ts_ms_i <= 0:
            raise RuntimeError("dashboard payload timestamps must be > 0")
        if generated_ts_ms_i < analysis_as_of_ts_ms_i:
            raise RuntimeError(
                f"generated_ts_ms must be >= analysis_as_of_ts_ms: generated={generated_ts_ms_i} as_of={analysis_as_of_ts_ms_i}"
            )

        self._validate_market_cards_or_raise(
            market_cards=payload["market_cards"],
            root_symbol=self._symbol,
        )

    def _validate_market_cards_or_raise(
        self,
        *,
        market_cards: Mapping[str, Any],
        root_symbol: str,
    ) -> None:
        if not isinstance(market_cards, Mapping) or not market_cards:
            raise RuntimeError("market_cards must be a non-empty mapping")

        for card_name, card in market_cards.items():
            if not isinstance(card_name, str) or not card_name.strip():
                raise RuntimeError("market_cards contains invalid card name")
            if not isinstance(card, Mapping) or not card:
                raise RuntimeError(f"market_cards.{card_name} must be a non-empty mapping")

            required_by_card = {
                "internal_market": (
                    "symbol",
                    "timeframe",
                    "market_regime",
                    "trend",
                    "volatility",
                    "liquidity",
                    "latest_close_price",
                    "price_change_pct",
                    "avg_spread_bps",
                    "latest_spread_bps",
                    "avg_pattern_score",
                    "latest_pattern_score",
                    "entry_blockers",
                    "summary_ko",
                ),
                "trade_performance": (
                    "symbol",
                    "total_trades",
                    "wins",
                    "losses",
                    "breakeven",
                    "win_rate_pct",
                    "total_pnl",
                    "avg_pnl",
                    "avg_win_pnl",
                    "avg_loss_pnl",
                    "entry_failure_reasons",
                    "loss_causes",
                    "recent_trade_briefs",
                    "summary_ko",
                ),
                "external_market": (
                    "symbol",
                    "trend",
                    "volatility",
                    "liquidity",
                    "market_regime",
                    "conviction",
                    "price",
                    "mark_price",
                    "index_price",
                    "spread_bps",
                    "funding_rate",
                    "open_interest",
                    "open_interest_change_pct",
                    "global_long_short_ratio",
                    "top_long_short_ratio",
                    "taker_long_short_ratio",
                    "crowding_bias",
                    "liquidation_pressure",
                    "support_price",
                    "resistance_price",
                    "poc_price",
                    "value_area_low",
                    "value_area_high",
                    "poc_distance_bps",
                    "price_location",
                    "cvd",
                    "delta_ratio_pct",
                    "aggression_bias",
                    "cvd_trend",
                    "divergence",
                    "put_call_oi_ratio",
                    "put_call_volume_ratio",
                    "options_bias",
                    "key_signals",
                    "volume_profile_summary",
                    "orderflow_summary",
                    "options_summary",
                    "summary_ko",
                ),
            }

            if card_name not in required_by_card:
                raise RuntimeError(f"Unexpected market_cards card: {card_name}")

            for required_key in required_by_card[card_name]:
                if required_key not in card:
                    raise RuntimeError(
                        f"market_cards.{card_name} missing required key: {required_key}"
                    )

            symbol_value = card["symbol"]
            if not isinstance(symbol_value, str) or not symbol_value.strip():
                raise RuntimeError(f"market_cards.{card_name}.symbol must be a non-empty string")

            normalized_card_symbol = self._normalize_symbol_contract_or_raise(
                symbol_value,
                f"market_cards.{card_name}.symbol",
            )
            if normalized_card_symbol != root_symbol:
                raise RuntimeError(
                    f"market_cards.{card_name}.symbol mismatch: expected={root_symbol} got={normalized_card_symbol}"
                )

            summary_ko = card["summary_ko"]
            if not isinstance(summary_ko, str) or not summary_ko.strip():
                raise RuntimeError(f"market_cards.{card_name}.summary_ko must be a non-empty string")

            if card_name == "internal_market":
                self._validate_string_list_or_raise(
                    card["entry_blockers"],
                    "market_cards.internal_market.entry_blockers",
                )

            if card_name == "trade_performance":
                self._validate_string_list_or_raise(
                    card["entry_failure_reasons"],
                    "market_cards.trade_performance.entry_failure_reasons",
                )
                self._validate_string_list_or_raise(
                    card["loss_causes"],
                    "market_cards.trade_performance.loss_causes",
                )
                self._validate_string_list_or_raise(
                    card["recent_trade_briefs"],
                    "market_cards.trade_performance.recent_trade_briefs",
                )

            if card_name == "external_market":
                self._validate_string_list_or_raise(
                    card["key_signals"],
                    "market_cards.external_market.key_signals",
                )
                for nested_key in (
                    "volume_profile_summary",
                    "orderflow_summary",
                    "options_summary",
                ):
                    nested_value = card[nested_key]
                    if not isinstance(nested_value, Mapping) or not nested_value:
                        raise RuntimeError(
                            f"market_cards.external_market.{nested_key} must be a non-empty mapping"
                        )

    def _validate_response_or_raise(self, result: QuantAnalystResponse) -> None:
        data = result.to_dict()
        if not isinstance(data, dict) or not data:
            raise RuntimeError("QuantAnalystResponse.to_dict() must return a non-empty mapping")
        if not isinstance(result.symbol, str) or not result.symbol.strip():
            raise RuntimeError("QuantAnalystResponse.symbol must be a non-empty string")

        normalized_symbol = self._normalize_symbol_contract_or_raise(
            result.symbol,
            "QuantAnalystResponse.symbol",
        )
        if normalized_symbol != self._symbol:
            raise RuntimeError(
                f"QuantAnalystResponse.symbol mismatch: expected={self._symbol} got={result.symbol}"
            )
        if not isinstance(result.question, str) or not result.question.strip():
            raise RuntimeError("QuantAnalystResponse.question must be a non-empty string")
        if result.generated_ts_ms <= 0:
            raise RuntimeError("QuantAnalystResponse.generated_ts_ms must be > 0")
        if not isinstance(result.gpt_result, dict) or not result.gpt_result:
            raise RuntimeError("QuantAnalystResponse.gpt_result must be a non-empty mapping")

        if result.internal_market_summary is not None:
            self._validate_mapping_symbol_contract_or_raise(
                data=result.internal_market_summary,
                field_name="QuantAnalystResponse.internal_market_summary",
            )
        if result.trade_summary is not None:
            self._validate_mapping_symbol_contract_or_raise(
                data=result.trade_summary,
                field_name="QuantAnalystResponse.trade_summary",
            )
        if result.external_market_summary is not None:
            self._validate_mapping_symbol_contract_or_raise(
                data=result.external_market_summary,
                field_name="QuantAnalystResponse.external_market_summary",
            )

        self._validate_dashboard_payload_or_raise(result.dashboard_payload)

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

    def _normalize_symbol_contract_or_raise(
        self,
        symbol: str,
        field_name: str,
    ) -> str:
        if not isinstance(symbol, str) or not symbol.strip():
            raise RuntimeError(f"{field_name} must be a non-empty string")
        normalized = symbol.strip().replace("-", "").replace("/", "").upper()
        if not normalized:
            raise RuntimeError(f"{field_name} normalized symbol must not be empty")
        return normalized

    def _validate_string_list_or_raise(
        self,
        value: Any,
        field_name: str,
    ) -> None:
        if not isinstance(value, list):
            raise RuntimeError(f"{field_name} must be a list")
        for idx, item in enumerate(value):
            if not isinstance(item, str) or not item.strip():
                raise RuntimeError(f"{field_name}[{idx}] must be a non-empty string")

    def _now_ms(self) -> int:
        ts_ms = int(time.time() * 1000)
        if ts_ms <= 0:
            raise RuntimeError("generated timestamp must be > 0")
        return ts_ms

    def _fmt_decimal(self, value: Any, scale: int) -> str:
        if scale < 0:
            raise RuntimeError("scale must be >= 0")
        return f"{value:.{scale}f}"


__all__ = [
    "QuantAnalyst",
    "QuantAnalystResponse",
]