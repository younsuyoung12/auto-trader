"""
========================================================
FILE: analysis/gpt_analyst_engine.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

ROLE:
- AI Quant Analyst / AI Market Analyst용 OpenAI 호출 전용 엔진
- 내부 DB 분석 결과와 외부 시장 분석 결과를 받아
  한국어 분석/원인/권고 JSON 1개를 생성한다
- 주문 실행 / 포지션 변경 / TP·SL 수정은 절대 수행하지 않는다

CORE RESPONSIBILITIES:
- OpenAI 요청 payload 계약 검증
- OpenAI Responses API 호출
- 응답에서 JSON object 1개만 STRICT 추출
- scope / used_inputs / answer_ko / list fields 무결성 검증
- market-only / full-analysis 입력 계약 분리 유지

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- settings.py(SSOT) 외 환경변수 직접 접근 금지
- print() 금지 / logging 사용
- 응답 누락/손상/모호성 발생 시 즉시 예외
- 민감정보 로그 금지
- OpenAI Responses API 사용
- 응답은 반드시 JSON object 1개여야 한다
- structured output 후보가 여러 개거나 text 후보가 여러 개면 모호성으로 즉시 예외
- settings alias 충돌 시 즉시 예외

CHANGE HISTORY:
- 2026-03-10:
  1) FIX(SSOT): settings canonical/legacy alias를 명시 계약으로 통합하고 충돌 시 즉시 예외
  2) FIX(STRICT): structured output 후보가 2개 이상이면 모호성으로 즉시 예외
  3) FIX(STRICT): output_text 후보가 2개 이상이고 내용이 다르면 즉시 예외
  4) FIX(CONTRACT): OpenAI 응답은 순수 JSON object 1개 또는 fenced JSON 1개만 허용
  5) FIX(INTEGRITY): used_inputs / answer_ko / root_causes / recommendations 계약 검증 강화
- 2026-03-09:
  1) FIX(CONTRACT): payload 상단 키 / 섹션 symbol 계약 검증 강화
  2) FIX(STRICT): OpenAI 응답은 순수 JSON object 1개 또는 fenced JSON 1개만 허용
  3) FIX(INTEGRITY): answer_ko 문장 수 / root_causes / recommendations 개수 계약 검증 강화
========================================================
"""

from __future__ import annotations

import json
import logging
import math
import re
from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from settings import SETTINGS

logger = logging.getLogger(__name__)

_ALLOWED_SCOPE = {
    "quant_analysis",
    "market_analysis",
    "mixed",
    "out_of_scope",
}

_ALLOWED_USED_INPUTS = {
    "internal_market_summary",
    "trade_summary",
    "external_market_summary",
}

_ALLOWED_TOP_LEVEL_KEYS = {
    "question",
    "internal_market_summary",
    "trade_summary",
    "external_market_summary",
}

_OUT_OF_SCOPE_MESSAGE = "이 질문은 트레이딩 시스템 범위를 벗어납니다."

# settings canonical / compatibility aliases
_SETTING_ALIASES: Dict[str, Tuple[str, ...]] = {
    "market_symbol": ("analyst_market_symbol", "ANALYST_MARKET_SYMBOL"),
    "openai_api_key": ("openai_api_key", "OPENAI_API_KEY"),
    "openai_model": ("analyst_openai_model", "ANALYST_OPENAI_MODEL"),
    "openai_timeout_sec": ("analyst_openai_timeout_sec", "ANALYST_OPENAI_TIMEOUT_SEC"),
    "openai_max_output_tokens": ("analyst_openai_max_output_tokens", "ANALYST_OPENAI_MAX_OUTPUT_TOKENS"),
    "openai_reasoning_effort": ("analyst_openai_reasoning_effort", "ANALYST_OPENAI_REASONING_EFFORT"),
}

_SYSTEM_INSTRUCTIONS = """
You are a professional Bitcoin quantitative analyst for an internal trading intelligence system.

Strict rules:
1. You are an analyst only. Never place orders, never modify positions, never modify TP/SL.
2. Use ONLY the provided data payload. Do not invent facts.
3. If the question is outside trading system analysis or Bitcoin futures market analysis, return out_of_scope.
4. Output MUST be exactly one JSON object.
5. All output text must be in Korean.
6. Be concrete, direct, and operationally useful.
7. When evidence is insufficient or missing, you must fail by returning a strict explanation only from the provided data. Do not guess.
8. Recommendations must be diagnostic or strategic suggestions only, never execution commands.
9. Keep the output compact to fit within a strict token budget.
10. answer_ko must be concise: maximum 2 short sentences.
11. root_causes must contain 1 to 4 short items for non-out_of_scope.
12. recommendations must contain 1 to 4 short items for non-out_of_scope.
13. Each root_causes/recommendations item must be short and direct, not an essay.
14. NEVER include a section name in used_inputs if that section is absent from the payload.
15. If only one analysis section exists in the payload, used_inputs MUST contain only that section.
16. used_inputs must exactly reflect the sections actually used, and every used section must exist in the payload.

Required JSON schema:
{
  "scope": "quant_analysis" | "market_analysis" | "mixed" | "out_of_scope",
  "answer_ko": "string",
  "root_causes": ["string", ...],
  "recommendations": ["string", ...],
  "confidence": 0.0,
  "used_inputs": ["internal_market_summary" | "trade_summary" | "external_market_summary", ...]
}

Additional rules:
- For out_of_scope:
  - answer_ko must be exactly "이 질문은 트레이딩 시스템 범위를 벗어납니다."
  - root_causes must be []
  - recommendations must be []
  - used_inputs may be []
- confidence must be within [0,1]
- used_inputs must only contain payload sections that were actually used
- Do not output markdown fences.
- Output only the JSON object.
""".strip()


@dataclass(frozen=True)
class GptAnalystResult:
    scope: str
    answer_ko: str
    root_causes: List[str]
    recommendations: List[str]
    confidence: float
    used_inputs: List[str]
    raw_response_text: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class GptAnalystEngine:
    """
    OpenAI Responses API 기반 분석 엔진.
    """

    def __init__(self) -> None:
        self._symbol = self._normalize_symbol_or_raise(
            self._require_str_setting("market_symbol"),
            "market_symbol",
        )
        self._api_key = self._require_str_setting("openai_api_key")
        self._model = self._require_str_setting("openai_model")
        self._timeout_sec = self._require_float_setting("openai_timeout_sec")
        self._max_output_tokens = self._require_int_setting("openai_max_output_tokens")
        self._reasoning_effort = self._optional_str_setting("openai_reasoning_effort")

        if self._timeout_sec <= 0.0:
            raise RuntimeError("openai_timeout_sec must be > 0")
        if self._max_output_tokens <= 0:
            raise RuntimeError("openai_max_output_tokens must be > 0")

        self._client = self._make_client()

    # ========================================================
    # Public API
    # ========================================================

    def analyze(
        self,
        *,
        question: str,
        internal_market_summary: Mapping[str, Any],
        trade_summary: Mapping[str, Any],
        external_market_summary: Optional[Mapping[str, Any]] = None,
    ) -> GptAnalystResult:
        payload = self._build_payload(
            question=question,
            internal_market_summary=internal_market_summary,
            trade_summary=trade_summary,
            external_market_summary=external_market_summary,
        )
        return self._request_analysis_or_raise(payload)

    def analyze_market_only(
        self,
        *,
        question: str,
        external_market_summary: Mapping[str, Any],
    ) -> GptAnalystResult:
        payload = self._build_market_only_payload(
            question=question,
            external_market_summary=external_market_summary,
        )
        return self._request_analysis_or_raise(payload)

    def analyze_external_market_only(
        self,
        *,
        question: str,
        external_market_summary: Mapping[str, Any],
    ) -> GptAnalystResult:
        return self.analyze_market_only(
            question=question,
            external_market_summary=external_market_summary,
        )

    # ========================================================
    # Build request payload
    # ========================================================

    def _build_payload(
        self,
        *,
        question: str,
        internal_market_summary: Mapping[str, Any],
        trade_summary: Mapping[str, Any],
        external_market_summary: Optional[Mapping[str, Any]],
    ) -> Dict[str, Any]:
        normalized_question = self._require_nonempty_str(question, "question")
        self._require_non_empty_mapping(internal_market_summary, "internal_market_summary")
        self._require_non_empty_mapping(trade_summary, "trade_summary")
        if external_market_summary is not None:
            self._require_non_empty_mapping(external_market_summary, "external_market_summary")

        payload: Dict[str, Any] = {
            "question": normalized_question,
            "internal_market_summary": dict(internal_market_summary),
            "trade_summary": dict(trade_summary),
        }

        if external_market_summary is not None:
            payload["external_market_summary"] = dict(external_market_summary)

        self._validate_payload_contract_or_raise(
            payload,
            expected_sections={"internal_market_summary", "trade_summary"},
            allow_optional_external=True,
        )
        return payload

    def _build_market_only_payload(
        self,
        *,
        question: str,
        external_market_summary: Mapping[str, Any],
    ) -> Dict[str, Any]:
        normalized_question = self._require_nonempty_str(question, "question")
        self._require_non_empty_mapping(external_market_summary, "external_market_summary")

        payload = {
            "question": normalized_question,
            "external_market_summary": dict(external_market_summary),
        }

        self._validate_payload_contract_or_raise(
            payload,
            expected_sections={"external_market_summary"},
            allow_optional_external=False,
        )
        return payload

    def _validate_payload_contract_or_raise(
        self,
        payload: Mapping[str, Any],
        *,
        expected_sections: set[str],
        allow_optional_external: bool,
    ) -> None:
        if not isinstance(payload, Mapping) or not payload:
            raise RuntimeError("payload must be a non-empty mapping")

        self._validate_payload_top_level_keys_or_raise(payload)

        question = payload.get("question")
        self._require_nonempty_str(question, "payload.question")

        available_sections = set(self._extract_available_input_sections(payload))
        if not allow_optional_external:
            if available_sections != expected_sections:
                raise RuntimeError(
                    f"payload sections mismatch: expected={sorted(expected_sections)} actual={sorted(available_sections)}"
                )
            self._validate_payload_section_symbols_or_raise(payload, available_sections)
            return

        required_sections = set(expected_sections)
        if not required_sections.issubset(available_sections):
            raise RuntimeError(
                f"payload missing required sections: required={sorted(required_sections)} actual={sorted(available_sections)}"
            )

        unexpected_sections = available_sections - _ALLOWED_USED_INPUTS
        if unexpected_sections:
            raise RuntimeError(
                f"payload contains unexpected analysis sections: {sorted(unexpected_sections)}"
            )

        self._validate_payload_section_symbols_or_raise(payload, available_sections)

    def _validate_payload_top_level_keys_or_raise(self, payload: Mapping[str, Any]) -> None:
        actual_keys = set(payload.keys())
        if "question" not in actual_keys:
            raise RuntimeError("payload missing required key: question")

        unexpected_keys = actual_keys - _ALLOWED_TOP_LEVEL_KEYS
        if unexpected_keys:
            raise RuntimeError(f"payload contains unexpected top-level keys: {sorted(unexpected_keys)}")

    def _validate_payload_section_symbols_or_raise(
        self,
        payload: Mapping[str, Any],
        sections: set[str],
    ) -> None:
        for section_name in sorted(sections):
            section_value = payload.get(section_name)
            if not isinstance(section_value, Mapping) or not section_value:
                raise RuntimeError(f"{section_name} must be a non-empty mapping")

            symbol_value = section_value.get("symbol")
            if not isinstance(symbol_value, str) or not symbol_value.strip():
                raise RuntimeError(f"{section_name}.symbol must be a non-empty string")

            normalized_symbol = self._normalize_symbol_or_raise(
                symbol_value,
                f"{section_name}.symbol",
            )
            if normalized_symbol != self._symbol:
                raise RuntimeError(
                    f"{section_name}.symbol mismatch: expected={self._symbol} got={normalized_symbol}"
                )

    # ========================================================
    # OpenAI client / response handling
    # ========================================================

    def _make_client(self) -> Any:
        try:
            from openai import OpenAI  # type: ignore
        except Exception as exc:
            raise RuntimeError(f"OpenAI client import failed: {exc.__class__.__name__}") from exc

        try:
            return OpenAI(
                api_key=self._api_key,
                timeout=self._timeout_sec,
            )
        except Exception as exc:
            raise RuntimeError(f"OpenAI client initialization failed: {exc.__class__.__name__}") from exc

    def _request_analysis_or_raise(self, payload: Mapping[str, Any]) -> GptAnalystResult:
        if not isinstance(payload, Mapping) or not payload:
            raise RuntimeError("payload must be a non-empty mapping")

        json_safe_payload = self._json_safe_or_raise(payload, field_name="payload")
        self._validate_json_safe_payload_or_raise(json_safe_payload)

        input_text = json.dumps(json_safe_payload, ensure_ascii=False, separators=(",", ":"))
        request_summary = self._build_request_summary(json_safe_payload, input_text)
        runtime_instructions = self._build_runtime_instructions(json_safe_payload)

        logger.info(
            "OpenAI request prepared: model=%s max_output_tokens=%s payload_chars=%s question_chars=%s sections=%s",
            self._model,
            self._max_output_tokens,
            request_summary["payload_chars"],
            request_summary["question_chars"],
            json.dumps(request_summary["sections"], ensure_ascii=False, separators=(",", ":")),
        )

        request_args: Dict[str, Any] = {
            "model": self._model,
            "instructions": runtime_instructions,
            "input": input_text,
            "max_output_tokens": self._max_output_tokens,
            "store": False,
            "text": {
                "format": self._response_json_schema(),
            },
        }

        if self._reasoning_effort is not None:
            request_args["reasoning"] = {"effort": self._reasoning_effort}

        try:
            response = self._client.responses.create(**request_args)
        except Exception as exc:
            error_name = exc.__class__.__name__
            error_message = str(exc)

            logger.error(
                "OpenAI Responses API request failed: error_type=%s model=%s timeout_sec=%.3f payload_chars=%s",
                error_name,
                self._model,
                self._timeout_sec,
                request_summary["payload_chars"],
            )

            if error_name == "APITimeoutError":
                raise RuntimeError(
                    f"OpenAI Responses API request timed out: model={self._model}, timeout_sec={self._timeout_sec}, "
                    f"payload_chars={request_summary['payload_chars']}"
                ) from exc

            if error_name == "BadRequestError":
                raise RuntimeError(
                    f"OpenAI Responses API bad request: model={self._model}, detail={error_message}"
                ) from exc

            raise RuntimeError(
                f"OpenAI Responses API request failed: model={self._model}, error_type={error_name}"
            ) from exc

        self._ensure_response_completed_or_raise(response, request_summary=request_summary)

        parsed_obj = self._extract_structured_output_dict_or_raise(response)
        if parsed_obj is not None:
            raw_text = json.dumps(parsed_obj, ensure_ascii=False, separators=(",", ":"))
            logger.info(
                "OpenAI structured output extracted: model=%s raw_text_chars=%s usage=%s",
                self._model,
                len(raw_text),
                json.dumps(self._extract_usage_summary(response), ensure_ascii=False, separators=(",", ":")),
            )
            result = self._parse_result_strict(raw_text)
            self._validate_result_used_inputs_against_payload(result, json_safe_payload)
            return result

        raw_text = self._extract_output_text_or_raise(response)
        logger.info(
            "OpenAI text output extracted: model=%s raw_text_chars=%s usage=%s",
            self._model,
            len(raw_text),
            json.dumps(self._extract_usage_summary(response), ensure_ascii=False, separators=(",", ":")),
        )
        result = self._parse_result_strict(raw_text)
        self._validate_result_used_inputs_against_payload(result, json_safe_payload)
        return result

    def _response_json_schema(self) -> Dict[str, Any]:
        return {
            "type": "json_schema",
            "name": "gpt_analyst_result",
            "strict": True,
            "schema": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "scope": {
                        "type": "string",
                        "enum": sorted(_ALLOWED_SCOPE),
                    },
                    "answer_ko": {
                        "type": "string",
                    },
                    "root_causes": {
                        "type": "array",
                        "items": {
                            "type": "string",
                        },
                    },
                    "recommendations": {
                        "type": "array",
                        "items": {
                            "type": "string",
                        },
                    },
                    "confidence": {
                        "type": "number",
                    },
                    "used_inputs": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": sorted(_ALLOWED_USED_INPUTS),
                        },
                    },
                },
                "required": [
                    "scope",
                    "answer_ko",
                    "root_causes",
                    "recommendations",
                    "confidence",
                    "used_inputs",
                ],
            },
        }

    def _build_runtime_instructions(self, payload: Mapping[str, Any]) -> str:
        available_inputs = self._extract_available_input_sections(payload)
        absent_inputs = [name for name in sorted(_ALLOWED_USED_INPUTS) if name not in available_inputs]

        lines: List[str] = [_SYSTEM_INSTRUCTIONS, "", "Runtime payload availability:"]
        lines.append(f"- available_inputs: {json.dumps(available_inputs, ensure_ascii=False)}")
        lines.append(f"- absent_inputs: {json.dumps(absent_inputs, ensure_ascii=False)}")

        if len(available_inputs) == 1:
            lines.append(
                f'- Since only one analysis section is present, used_inputs MUST be exactly {json.dumps(available_inputs, ensure_ascii=False)}.'
            )
        elif available_inputs:
            lines.append(
                f'- used_inputs MUST be a subset of {json.dumps(available_inputs, ensure_ascii=False)} and MUST NOT contain any absent input.'
            )
        else:
            lines.append("- No analysis sections are available. used_inputs MUST be [].")

        return "\n".join(lines)

    def _extract_available_input_sections(self, payload: Mapping[str, Any]) -> List[str]:
        available: List[str] = []
        for key in sorted(_ALLOWED_USED_INPUTS):
            value = payload.get(key)
            if isinstance(value, Mapping) and value:
                available.append(key)
        return available

    def _validate_result_used_inputs_against_payload(
        self,
        result: GptAnalystResult,
        payload: Mapping[str, Any],
    ) -> None:
        available_inputs = set(self._extract_available_input_sections(payload))
        invalid_claims = [item for item in result.used_inputs if item not in available_inputs]

        if invalid_claims:
            raise RuntimeError(
                "GPT result claims unavailable used_inputs: "
                f"invalid={sorted(invalid_claims)}, available={sorted(available_inputs)}"
            )

        if result.scope == "out_of_scope":
            if result.used_inputs and not set(result.used_inputs).issubset(available_inputs):
                raise RuntimeError("out_of_scope used_inputs must be empty or valid subset of available inputs")
            return

        if len(available_inputs) == 1:
            only_available = next(iter(available_inputs))
            if result.used_inputs != [only_available]:
                raise RuntimeError(
                    "GPT result used_inputs mismatch for single-input payload: "
                    f"expected={[only_available]}, actual={result.used_inputs}"
                )

    def _ensure_response_completed_or_raise(
        self,
        response: Any,
        *,
        request_summary: Mapping[str, Any],
    ) -> None:
        status = self._safe_get_attr(response, "status")
        if status in (None, "", "completed"):
            return

        refusal_text = self._extract_refusal_text(response)
        incomplete_reason = self._extract_incomplete_reason(response)
        usage_summary = self._extract_usage_summary(response)

        logger.error(
            "OpenAI response not completed: model=%s status=%s incomplete_reason=%s refusal_present=%s usage=%s request=%s",
            self._model,
            status,
            incomplete_reason or "",
            bool(refusal_text),
            json.dumps(usage_summary, ensure_ascii=False, separators=(",", ":")),
            json.dumps(dict(request_summary), ensure_ascii=False, separators=(",", ":")),
        )

        if refusal_text:
            raise RuntimeError(f"OpenAI response refused request: {refusal_text}")

        if status == "incomplete":
            detail = incomplete_reason or "unknown_reason"
            raise RuntimeError(
                "OpenAI response incomplete: "
                f"reason={detail}, model={self._model}, max_output_tokens={self._max_output_tokens}, "
                f"payload_chars={request_summary['payload_chars']}, question_chars={request_summary['question_chars']}, "
                f"usage={json.dumps(usage_summary, ensure_ascii=False, separators=(',', ':'))}"
            )

        raise RuntimeError(f"OpenAI response not completed: status={status}")

    def _extract_structured_output_dict_or_raise(self, response: Any) -> Optional[Dict[str, Any]]:
        dumped = self._response_to_python(response)

        candidates: List[Dict[str, Any]] = []

        output_parsed = self._safe_get_any(
            dumped,
            [
                ("output_parsed",),
            ],
        )
        if output_parsed is not None:
            parsed = self._normalize_parsed_output(output_parsed)
            if parsed is not None:
                candidates.append(parsed)

        output_items = self._safe_get_any(
            dumped,
            [
                ("output",),
            ],
        )
        if isinstance(output_items, list):
            for item in output_items:
                if not isinstance(item, Mapping):
                    continue

                item_type = item.get("type")
                if item_type != "message":
                    continue

                content = item.get("content")
                if not isinstance(content, list):
                    continue

                for content_item in content:
                    if not isinstance(content_item, Mapping):
                        continue

                    if "parsed" in content_item:
                        parsed = self._normalize_parsed_output(content_item.get("parsed"))
                        if parsed is not None:
                            candidates.append(parsed)

                    if "json" in content_item:
                        parsed = self._normalize_parsed_output(content_item.get("json"))
                        if parsed is not None:
                            candidates.append(parsed)

        if not candidates:
            return None

        unique_candidates = self._dedupe_mapping_candidates_strict(candidates, field_name="structured_output_candidates")
        if len(unique_candidates) != 1:
            raise RuntimeError("OpenAI structured output must contain exactly one JSON object candidate")

        return unique_candidates[0]

    def _normalize_parsed_output(self, value: Any) -> Optional[Dict[str, Any]]:
        if value is None:
            return None

        normalized = self._response_to_python(value)

        if isinstance(normalized, str):
            text = normalized.strip()
            if text.startswith("{") and text.endswith("}"):
                try:
                    normalized = json.loads(text)
                except json.JSONDecodeError:
                    return None

        if isinstance(normalized, Mapping):
            normalized_dict = dict(normalized)
            if normalized_dict:
                return normalized_dict

        return None

    def _extract_output_text_or_raise(self, response: Any) -> str:
        """
        STRICT parser for OpenAI Responses API output.

        우선순위:
        1) response.output_text
        2) response.output[].content[] 중 type=output_text 의 text/value

        STRICT:
        - 후보가 여러 개면 내용이 완전히 동일한 경우만 허용
        - 서로 다른 후보가 2개 이상이면 즉시 예외
        """

        output_text_direct = self._safe_get_attr(response, "output_text")
        candidates: List[str] = []
        if isinstance(output_text_direct, str) and output_text_direct.strip():
            candidates.append(output_text_direct.strip())

        dumped = self._response_to_python(response)

        output_items = self._safe_get_any(
            dumped,
            [
                ("output",),
            ],
        )
        if isinstance(output_items, list):
            for item in output_items:
                if not isinstance(item, Mapping):
                    continue

                item_type = item.get("type")
                if item_type != "message":
                    continue

                content = item.get("content")
                if not isinstance(content, list):
                    continue

                for content_item in content:
                    if not isinstance(content_item, Mapping):
                        continue

                    content_type = content_item.get("type")
                    if content_type == "refusal":
                        refusal_text = self._first_nonempty_string(
                            [
                                content_item.get("refusal"),
                                content_item.get("text"),
                                content_item.get("value"),
                            ]
                        )
                        if refusal_text is not None:
                            raise RuntimeError(f"OpenAI response refused request: {refusal_text}")

                    if content_type != "output_text":
                        continue

                    candidate = self._first_nonempty_string(
                        [
                            content_item.get("text"),
                            content_item.get("value"),
                        ]
                    )
                    if candidate is not None:
                        candidates.append(candidate)

        unique_candidates = self._dedupe_string_candidates_strict(candidates, field_name="output_text_candidates")
        if not unique_candidates:
            raise RuntimeError("OpenAI response contained no text content")
        if len(unique_candidates) != 1:
            raise RuntimeError("OpenAI text output must contain exactly one unique text candidate")

        return unique_candidates[0]

    def _response_to_python(self, response: Any) -> Any:
        if response is None:
            return None

        if isinstance(response, (str, int, float, bool)):
            return response

        if isinstance(response, Mapping):
            return dict(response)

        if isinstance(response, list):
            return [self._response_to_python(x) for x in response]

        if isinstance(response, tuple):
            return [self._response_to_python(x) for x in response]

        model_dump = getattr(response, "model_dump", None)
        if callable(model_dump):
            try:
                return model_dump(mode="python")
            except TypeError:
                return model_dump()

        to_dict = getattr(response, "to_dict", None)
        if callable(to_dict):
            return to_dict()

        if hasattr(response, "__dict__"):
            return dict(response.__dict__)

        return response

    def _extract_refusal_text(self, response: Any) -> Optional[str]:
        dumped = self._response_to_python(response)

        direct_refusal = self._safe_get_any(
            dumped,
            [
                ("refusal",),
            ],
        )
        if isinstance(direct_refusal, str) and direct_refusal.strip():
            return direct_refusal.strip()

        output_items = self._safe_get_any(
            dumped,
            [
                ("output",),
            ],
        )
        if not isinstance(output_items, list):
            return None

        for item in output_items:
            if not isinstance(item, Mapping):
                continue
            content = item.get("content")
            if not isinstance(content, list):
                continue
            for content_item in content:
                if not isinstance(content_item, Mapping):
                    continue
                if content_item.get("type") != "refusal":
                    continue
                refusal_text = self._first_nonempty_string(
                    [
                        content_item.get("refusal"),
                        content_item.get("text"),
                        content_item.get("value"),
                    ]
                )
                if refusal_text is not None:
                    return refusal_text

        return None

    def _extract_incomplete_reason(self, response: Any) -> Optional[str]:
        dumped = self._response_to_python(response)

        incomplete_details = self._safe_get_any(
            dumped,
            [
                ("incomplete_details", "reason"),
                ("incomplete_details",),
            ],
        )

        if isinstance(incomplete_details, str) and incomplete_details.strip():
            return incomplete_details.strip()

        if isinstance(incomplete_details, Mapping):
            reason = incomplete_details.get("reason")
            if isinstance(reason, str) and reason.strip():
                return reason.strip()
            return json.dumps(incomplete_details, ensure_ascii=False, separators=(",", ":"))

        return None

    def _extract_usage_summary(self, response: Any) -> Dict[str, Any]:
        dumped = self._response_to_python(response)
        usage = self._safe_get_any(
            dumped,
            [
                ("usage",),
            ],
        )
        if not isinstance(usage, Mapping):
            return {}

        input_tokens = usage.get("input_tokens")
        output_tokens = usage.get("output_tokens")
        total_tokens = usage.get("total_tokens")

        reasoning_tokens = None
        output_token_details = usage.get("output_tokens_details")
        if isinstance(output_token_details, Mapping):
            reasoning_tokens = output_token_details.get("reasoning_tokens")

        summary: Dict[str, Any] = {}
        if input_tokens is not None:
            summary["input_tokens"] = input_tokens
        if output_tokens is not None:
            summary["output_tokens"] = output_tokens
        if total_tokens is not None:
            summary["total_tokens"] = total_tokens
        if reasoning_tokens is not None:
            summary["reasoning_tokens"] = reasoning_tokens
        return summary

    def _build_request_summary(self, payload: Mapping[str, Any], input_text: str) -> Dict[str, Any]:
        sections: Dict[str, Any] = {}

        question = payload.get("question")
        question_chars = len(question) if isinstance(question, str) else 0

        for key in ("internal_market_summary", "trade_summary", "external_market_summary"):
            value = payload.get(key)
            if isinstance(value, Mapping):
                sections[key] = {
                    "key_count": len(value),
                    "keys_preview": list(value.keys())[:10],
                }

        return {
            "question_chars": question_chars,
            "payload_chars": len(input_text),
            "section_count": len(sections),
            "sections": sections,
            "max_output_tokens": self._max_output_tokens,
            "reasoning_effort": self._reasoning_effort,
        }

    def _safe_get_attr(self, value: Any, name: str) -> Any:
        try:
            return getattr(value, name, None)
        except Exception:
            return None

    def _safe_get_any(self, node: Any, paths: List[tuple[str, ...]]) -> Any:
        for path in paths:
            current = node
            ok = True
            for key in path:
                current = self._response_to_python(current)

                if isinstance(current, Mapping):
                    if key not in current:
                        ok = False
                        break
                    current = current[key]
                    continue

                try:
                    current = getattr(current, key)
                except Exception:
                    ok = False
                    break

            if ok:
                return current

        return None

    def _first_nonempty_string(self, values: List[Any]) -> Optional[str]:
        for value in values:
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    def _dedupe_string_candidates_strict(self, values: Sequence[str], field_name: str) -> List[str]:
        unique: List[str] = []
        seen: set[str] = set()
        for value in values:
            if not isinstance(value, str):
                raise RuntimeError(f"{field_name} must contain strings only (STRICT)")
            text = value.strip()
            if not text:
                continue
            if text not in seen:
                seen.add(text)
                unique.append(text)
        return unique

    def _dedupe_mapping_candidates_strict(self, values: Sequence[Mapping[str, Any]], field_name: str) -> List[Dict[str, Any]]:
        unique: List[Dict[str, Any]] = []
        seen: set[str] = set()

        for value in values:
            if not isinstance(value, Mapping):
                raise RuntimeError(f"{field_name} must contain mappings only (STRICT)")
            normalized = dict(value)
            fingerprint = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            if fingerprint not in seen:
                seen.add(fingerprint)
                unique.append(normalized)

        return unique

    # ========================================================
    # Strict output parsing
    # ========================================================

    def _parse_result_strict(self, raw_text: str) -> GptAnalystResult:
        if not isinstance(raw_text, str) or not raw_text.strip():
            raise RuntimeError("GPT returned empty content")

        json_text = self._extract_single_json_object(raw_text)
        try:
            obj = json.loads(json_text)
        except json.JSONDecodeError as exc:
            snippet = self._json_error_snippet(json_text, exc.pos)
            logger.error(
                "GPT returned invalid JSON: model=%s pos=%s lineno=%s colno=%s snippet=%s",
                self._model,
                exc.pos,
                exc.lineno,
                exc.colno,
                snippet,
            )
            raise RuntimeError("GPT returned invalid JSON") from exc

        if not isinstance(obj, dict):
            raise RuntimeError("GPT output must be a JSON object")

        required_keys = {
            "scope",
            "answer_ko",
            "root_causes",
            "recommendations",
            "confidence",
            "used_inputs",
        }
        actual_keys = set(obj.keys())
        if actual_keys != required_keys:
            raise RuntimeError(
                f"GPT output keys mismatch: expected={sorted(required_keys)}, actual={sorted(actual_keys)}"
            )

        scope = self._require_scope(obj["scope"])
        answer_ko = self._require_nonempty_str(obj["answer_ko"], "answer_ko")
        root_causes = self._require_string_list(obj["root_causes"], "root_causes")
        recommendations = self._require_string_list(obj["recommendations"], "recommendations")
        confidence = self._require_confidence(obj["confidence"])
        used_inputs = self._require_used_inputs(
            obj["used_inputs"],
            allow_empty=(scope == "out_of_scope"),
        )

        if scope == "out_of_scope":
            if answer_ko != _OUT_OF_SCOPE_MESSAGE:
                raise RuntimeError("out_of_scope answer_ko must match the fixed message exactly")
            if root_causes:
                raise RuntimeError("out_of_scope root_causes must be empty")
            if recommendations:
                raise RuntimeError("out_of_scope recommendations must be empty")

        if scope != "out_of_scope":
            if not root_causes:
                raise RuntimeError("Non-out_of_scope response must include at least one root cause")
            if not recommendations:
                raise RuntimeError("Non-out_of_scope response must include at least one recommendation")
            if not used_inputs:
                raise RuntimeError("Non-out_of_scope response must include at least one used input")

        result = GptAnalystResult(
            scope=scope,
            answer_ko=answer_ko,
            root_causes=root_causes,
            recommendations=recommendations,
            confidence=confidence,
            used_inputs=used_inputs,
            raw_response_text=raw_text,
        )
        self._validate_result_integrity_or_raise(result)
        return result

    def _validate_result_integrity_or_raise(self, result: GptAnalystResult) -> None:
        if result.scope not in _ALLOWED_SCOPE:
            raise RuntimeError(f"Unexpected result.scope: {result.scope}")
        if not isinstance(result.raw_response_text, str) or not result.raw_response_text.strip():
            raise RuntimeError("raw_response_text must be a non-empty string")

        sentence_count = self._count_sentences(result.answer_ko)
        if sentence_count <= 0:
            raise RuntimeError("answer_ko must contain at least one sentence")
        if sentence_count > 2:
            raise RuntimeError("answer_ko must contain at most 2 sentences")

        if result.scope == "out_of_scope":
            if result.answer_ko != _OUT_OF_SCOPE_MESSAGE:
                raise RuntimeError("out_of_scope answer_ko mismatch")
            if result.root_causes:
                raise RuntimeError("out_of_scope root_causes must be empty")
            if result.recommendations:
                raise RuntimeError("out_of_scope recommendations must be empty")
            return

        if not (1 <= len(result.root_causes) <= 4):
            raise RuntimeError("root_causes must contain 1 to 4 items for non-out_of_scope")
        if not (1 <= len(result.recommendations) <= 4):
            raise RuntimeError("recommendations must contain 1 to 4 items for non-out_of_scope")

        if len(set(result.root_causes)) != len(result.root_causes):
            raise RuntimeError("root_causes must not contain duplicates")
        if len(set(result.recommendations)) != len(result.recommendations):
            raise RuntimeError("recommendations must not contain duplicates")

    def _extract_single_json_object(self, raw_text: str) -> str:
        text = raw_text.strip()

        fenced_match = re.fullmatch(r"```(?:json)?\s*(\{.*\})\s*```", text, flags=re.DOTALL)
        if fenced_match is not None:
            candidate = fenced_match.group(1).strip()
            self._ensure_single_top_level_json_object(candidate)
            return candidate

        if text.startswith("{") and text.endswith("}"):
            self._ensure_single_top_level_json_object(text)
            return text

        raise RuntimeError("GPT output must be exactly one JSON object without extra text")

    def _ensure_single_top_level_json_object(self, candidate: str) -> None:
        candidate = candidate.strip()
        if not candidate.startswith("{") or not candidate.endswith("}"):
            raise RuntimeError("GPT output must contain exactly one top-level JSON object")

        depth = 0
        in_string = False
        escape = False

        for i, ch in enumerate(candidate):
            if in_string:
                if escape:
                    escape = False
                    continue
                if ch == "\\":
                    escape = True
                    continue
                if ch == '"':
                    in_string = False
                continue

            if ch == '"':
                in_string = True
                continue

            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0 and i != len(candidate) - 1:
                    trailer = candidate[i + 1 :].strip()
                    if trailer:
                        raise RuntimeError("GPT output contains extra text after JSON object")
                if depth < 0:
                    raise RuntimeError("GPT output has invalid JSON brace structure")

        if depth != 0 or in_string:
            raise RuntimeError("GPT output has incomplete JSON structure")

    def _json_error_snippet(self, json_text: str, pos: int, radius: int = 120) -> str:
        if not isinstance(json_text, str):
            return ""
        start = max(0, pos - radius)
        end = min(len(json_text), pos + radius)
        snippet = json_text[start:end]
        return snippet.replace("\n", "\\n").replace("\r", "\\r")

    # ========================================================
    # Payload normalization
    # ========================================================

    def _json_safe_or_raise(self, value: Any, field_name: str) -> Any:
        if value is None:
            return None

        if isinstance(value, (str, int, float, bool)):
            if isinstance(value, float) and not math.isfinite(value):
                raise RuntimeError(f"Non-finite float found in {field_name}")
            return value

        if isinstance(value, Decimal):
            if not value.is_finite():
                raise RuntimeError(f"Non-finite Decimal found in {field_name}")
            return str(value)

        if isinstance(value, Mapping):
            normalized: Dict[str, Any] = {}
            for k, v in value.items():
                if not isinstance(k, str) or not k.strip():
                    raise RuntimeError(f"Invalid mapping key in {field_name}")
                normalized[k] = self._json_safe_or_raise(v, f"{field_name}.{k}")
            return normalized

        if isinstance(value, list):
            return [
                self._json_safe_or_raise(item, f"{field_name}[{idx}]")
                for idx, item in enumerate(value)
            ]

        if isinstance(value, tuple):
            return [
                self._json_safe_or_raise(item, f"{field_name}[{idx}]")
                for idx, item in enumerate(value)
            ]

        raise RuntimeError(
            f"Unsupported payload type for JSON serialization in {field_name}: {value.__class__.__name__}"
        )

    def _validate_json_safe_payload_or_raise(self, payload: Mapping[str, Any]) -> None:
        if not isinstance(payload, Mapping) or not payload:
            raise RuntimeError("json_safe payload must be a non-empty mapping")

        self._validate_payload_top_level_keys_or_raise(payload)
        self._require_nonempty_str(payload.get("question"), "payload.question")

        available_sections = self._extract_available_input_sections(payload)
        if not available_sections:
            raise RuntimeError("json_safe payload must include at least one analysis section")

        self._validate_payload_section_symbols_or_raise(payload, set(available_sections))

    # ========================================================
    # Field validators
    # ========================================================

    def _require_scope(self, value: Any) -> str:
        if not isinstance(value, str) or value not in _ALLOWED_SCOPE:
            raise RuntimeError(f"Invalid scope: {value!r}")
        return value

    def _require_nonempty_str(self, value: Any, field_name: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"{field_name} must be a non-empty string")
        return value.strip()

    def _require_non_empty_mapping(self, value: Any, field_name: str) -> None:
        if not isinstance(value, Mapping) or not value:
            raise RuntimeError(f"{field_name} must be a non-empty mapping")

    def _require_string_list(self, value: Any, field_name: str) -> List[str]:
        if not isinstance(value, list):
            raise RuntimeError(f"{field_name} must be a list")
        result: List[str] = []
        for idx, item in enumerate(value):
            if not isinstance(item, str) or not item.strip():
                raise RuntimeError(f"{field_name}[{idx}] must be a non-empty string")
            result.append(item.strip())
        return result

    def _require_confidence(self, value: Any) -> float:
        if isinstance(value, bool):
            raise RuntimeError("confidence must be numeric, not bool")
        try:
            confidence = float(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("confidence must be numeric") from exc

        if not math.isfinite(confidence):
            raise RuntimeError("confidence must be finite")
        if confidence < 0.0 or confidence > 1.0:
            raise RuntimeError("confidence must be within [0,1]")
        return confidence

    def _require_used_inputs(self, value: Any, allow_empty: bool) -> List[str]:
        if not isinstance(value, list):
            raise RuntimeError("used_inputs must be a list")
        if not value and not allow_empty:
            raise RuntimeError("used_inputs must be a non-empty list")

        result: List[str] = []
        seen = set()

        for idx, item in enumerate(value):
            if not isinstance(item, str) or item not in _ALLOWED_USED_INPUTS:
                raise RuntimeError(f"used_inputs[{idx}] is invalid: {item!r}")
            if item in seen:
                raise RuntimeError(f"used_inputs contains duplicate item: {item}")
            seen.add(item)
            result.append(item)

        return result

    # ========================================================
    # Settings helpers
    # ========================================================

    def _get_setting_alias_values_or_raise(self, logical_name: str) -> List[Tuple[str, Any]]:
        if logical_name not in _SETTING_ALIASES:
            raise RuntimeError(f"Unknown setting logical name: {logical_name}")

        aliases = _SETTING_ALIASES[logical_name]
        found: List[Tuple[str, Any]] = []
        for alias in aliases:
            if hasattr(SETTINGS, alias):
                found.append((alias, getattr(SETTINGS, alias)))
        if not found:
            raise RuntimeError(f"Missing required setting aliases for {logical_name}: {aliases}")
        return found

    def _require_single_setting_value_or_raise(self, logical_name: str) -> Any:
        found = self._get_setting_alias_values_or_raise(logical_name)

        normalized_pairs: List[Tuple[str, Any]] = []
        for alias, value in found:
            if value is None:
                continue
            normalized_pairs.append((alias, value))

        if not normalized_pairs:
            aliases = _SETTING_ALIASES[logical_name]
            raise RuntimeError(f"All candidate settings are None for {logical_name}: {aliases}")

        first_value = normalized_pairs[0][1]
        for alias, value in normalized_pairs[1:]:
            if value != first_value:
                raise RuntimeError(
                    f"Conflicting setting aliases for {logical_name}: "
                    f"{[(a, v) for a, v in normalized_pairs]}"
                )

        return first_value

    def _require_str_setting(self, logical_name: str) -> str:
        value = self._require_single_setting_value_or_raise(logical_name)
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"Missing or invalid required string setting: {logical_name}")
        return value.strip()

    def _optional_str_setting(self, logical_name: str) -> Optional[str]:
        try:
            value = self._require_single_setting_value_or_raise(logical_name)
        except RuntimeError:
            return None

        if value is None:
            return None
        if not isinstance(value, str):
            raise RuntimeError(f"Invalid optional string setting: {logical_name}")
        value_norm = value.strip()
        if not value_norm:
            return None
        return value_norm

    def _require_int_setting(self, logical_name: str) -> int:
        value = self._require_single_setting_value_or_raise(logical_name)
        if isinstance(value, bool):
            raise RuntimeError(f"Invalid bool value for integer setting: {logical_name}")
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Missing or invalid required int setting: {logical_name}") from exc
        return parsed

    def _require_float_setting(self, logical_name: str) -> float:
        value = self._require_single_setting_value_or_raise(logical_name)
        if isinstance(value, bool):
            raise RuntimeError(f"Invalid bool value for float setting: {logical_name}")
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Missing or invalid required float setting: {logical_name}") from exc
        if not math.isfinite(parsed):
            raise RuntimeError(f"{logical_name} must be finite")
        return parsed

    # ========================================================
    # Misc helpers
    # ========================================================

    def _normalize_symbol_or_raise(self, value: str, field_name: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"{field_name} must be a non-empty string")
        normalized = value.strip().replace("-", "").replace("/", "").upper()
        if not normalized:
            raise RuntimeError(f"{field_name} normalized symbol must not be empty")
        return normalized

    def _count_sentences(self, text: str) -> int:
        normalized = self._require_nonempty_str(text, "answer_ko")
        parts = re.split(r"[.!?]+", normalized)
        count = sum(1 for part in parts if part.strip())
        if count == 0:
            return 1
        return count


__all__ = [
    "GptAnalystEngine",
    "GptAnalystResult",
]