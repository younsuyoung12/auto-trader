"""
========================================================
FILE: analysis/gpt_analyst_engine.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할:
- AI Quant Analyst / AI Market Analyst용 OpenAI 호출 전용 엔진.
- 내부 DB 분석 결과와 외부 시장 분석 결과를 받아
  한국어 분석/원인/해결책 JSON 1개를 생성한다.
- 주문 실행 / 포지션 변경 / TP·SL 수정은 절대 수행하지 않는다.

절대 원칙:
- STRICT · NO-FALLBACK
- settings.py(SSOT) 외 환경변수 직접 접근 금지
- print() 금지 / logging 사용
- 응답 누락/손상/모호성 발생 시 즉시 예외
- 민감정보 로그 금지
- OpenAI Responses API 사용
- 응답은 반드시 JSON object 1개여야 한다

변경 이력:
2026-03-07
- 신규 생성
- Responses API 기반 GPT 분석 엔진 추가
========================================================
"""

from __future__ import annotations

import json
import logging
import math
import re
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence

from settings import SETTINGS

logger = logging.getLogger(__name__)

_ALLOWED_SCOPE = {
    "quant_analysis",
    "market_analysis",
    "mixed",
    "out_of_scope",
}

_OUT_OF_SCOPE_MESSAGE = "이 질문은 트레이딩 시스템 범위를 벗어납니다."

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
- confidence must be within [0,1]
- used_inputs must only contain payload sections that were actually used
- Do not wrap JSON in markdown unless unavoidable. If you do, it still must contain exactly one JSON object.
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
        self._api_key = self._require_str_setting("OPENAI_API_KEY")
        self._model = self._require_str_setting("ANALYST_OPENAI_MODEL")
        self._timeout_sec = self._require_float_setting("ANALYST_OPENAI_TIMEOUT_SEC")
        self._max_output_tokens = self._require_int_setting("ANALYST_OPENAI_MAX_OUTPUT_TOKENS")
        self._temperature = self._require_float_setting("ANALYST_OPENAI_TEMPERATURE")
        self._reasoning_effort = self._optional_str_setting("ANALYST_OPENAI_REASONING_EFFORT")

        if self._timeout_sec <= 0.0:
            raise RuntimeError("ANALYST_OPENAI_TIMEOUT_SEC must be > 0")
        if self._max_output_tokens <= 0:
            raise RuntimeError("ANALYST_OPENAI_MAX_OUTPUT_TOKENS must be > 0")
        if not math.isfinite(self._temperature) or self._temperature < 0.0 or self._temperature > 2.0:
            raise RuntimeError("ANALYST_OPENAI_TEMPERATURE must be within [0,2]")

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

        request_args: Dict[str, Any] = {
            "model": self._model,
            "instructions": _SYSTEM_INSTRUCTIONS,
            "input": json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            "max_output_tokens": self._max_output_tokens,
            "temperature": self._temperature,
            "store": False,
        }

        if self._reasoning_effort is not None:
            request_args["reasoning"] = {"effort": self._reasoning_effort}

        try:
            response = self._client.responses.create(**request_args)
        except Exception as exc:
            logger.error(
                "OpenAI Responses API request failed: error_type=%s model=%s",
                exc.__class__.__name__,
                self._model,
            )
            raise RuntimeError("OpenAI Responses API request failed") from exc

        raw_text = self._extract_output_text_or_raise(response)
        return self._parse_result_strict(raw_text)

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
        if not isinstance(question, str) or not question.strip():
            raise RuntimeError("question must be a non-empty string")
        if not isinstance(internal_market_summary, Mapping) or not internal_market_summary:
            raise RuntimeError("internal_market_summary must be a non-empty mapping")
        if not isinstance(trade_summary, Mapping) or not trade_summary:
            raise RuntimeError("trade_summary must be a non-empty mapping")
        if external_market_summary is not None and (
            not isinstance(external_market_summary, Mapping) or not external_market_summary
        ):
            raise RuntimeError("external_market_summary must be a non-empty mapping when provided")

        payload: Dict[str, Any] = {
            "question": question.strip(),
            "internal_market_summary": dict(internal_market_summary),
            "trade_summary": dict(trade_summary),
        }

        if external_market_summary is not None:
            payload["external_market_summary"] = dict(external_market_summary)

        return payload

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

    def _extract_output_text_or_raise(self, response: Any) -> str:
        raw_text = getattr(response, "output_text", None)
        if not isinstance(raw_text, str) or not raw_text.strip():
            raise RuntimeError("OpenAI response missing output_text")
        return raw_text.strip()

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
        used_inputs = self._require_used_inputs(obj["used_inputs"])

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

        return GptAnalystResult(
            scope=scope,
            answer_ko=answer_ko,
            root_causes=root_causes,
            recommendations=recommendations,
            confidence=confidence,
            used_inputs=used_inputs,
            raw_response_text=raw_text,
        )

    def _extract_single_json_object(self, raw_text: str) -> str:
        text = raw_text.strip()

        fenced_match = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, flags=re.DOTALL)
        if fenced_match is not None:
            candidate = fenced_match.group(1).strip()
            self._ensure_single_top_level_json_object(candidate)
            return candidate

        brace_start = text.find("{")
        brace_end = text.rfind("}")
        if brace_start == -1 or brace_end == -1 or brace_end <= brace_start:
            raise RuntimeError("GPT output does not contain a JSON object")

        candidate = text[brace_start : brace_end + 1].strip()
        self._ensure_single_top_level_json_object(candidate)
        return candidate

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

    def _require_used_inputs(self, value: Any) -> List[str]:
        if not isinstance(value, list) or not value:
            raise RuntimeError("used_inputs must be a non-empty list")

        allowed_inputs = {
            "internal_market_summary",
            "trade_summary",
            "external_market_summary",
        }

        result: List[str] = []
        seen = set()

        for idx, item in enumerate(value):
            if not isinstance(item, str) or item not in allowed_inputs:
                raise RuntimeError(f"used_inputs[{idx}] is invalid: {item!r}")
            if item in seen:
                raise RuntimeError(f"used_inputs contains duplicate item: {item}")
            seen.add(item)
            result.append(item)

        return result

    # ========================================================
    # Settings helpers
    # ========================================================

    def _require_str_setting(self, name: str) -> str:
        value = getattr(SETTINGS, name, None)
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"Missing or invalid required setting: {name}")
        return value.strip()

    def _optional_str_setting(self, name: str) -> Optional[str]:
        value = getattr(SETTINGS, name, None)
        if value is None:
            return None
        if not isinstance(value, str):
            raise RuntimeError(f"Invalid optional string setting: {name}")
        value_norm = value.strip()
        if not value_norm:
            return None
        return value_norm

    def _require_int_setting(self, name: str) -> int:
        value = getattr(SETTINGS, name, None)
        if isinstance(value, bool):
            raise RuntimeError(f"Invalid bool value for integer setting: {name}")
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Missing or invalid required int setting: {name}") from exc
        return parsed

    def _require_float_setting(self, name: str) -> float:
        value = getattr(SETTINGS, name, None)
        if isinstance(value, bool):
            raise RuntimeError(f"Invalid bool value for float setting: {name}")
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Missing or invalid required float setting: {name}") from exc
        return parsed


__all__ = [
    "GptAnalystEngine",
    "GptAnalystResult",
]