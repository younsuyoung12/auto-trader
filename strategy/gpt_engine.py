"""
========================================================
FILE: strategy/gpt_engine.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================
역할
- OpenAI 호출(Transport) + latency 측정 + 응답 텍스트 추출 + JSON object 1개 추출을 담당한다.
- "전략 판단"이나 "매매 결정"은 하지 않는다. (상위 레이어가 책임)
- 상위 모듈(gpt_supervisor 등)은 이 모듈을 통해서만 OpenAI를 호출한다(단일 진입점).

절대 원칙 (STRICT · NO-FALLBACK)
- OPENAI_API_KEY 없으면 즉시 예외.
- 응답이 비어있거나 JSON 파싱 불가하면 즉시 예외.
- timeout/latency 예산 초과 시 즉시 예외.
- 민감정보(키/프롬프트/시그니처/주문ID 등)는 로그/예외 메시지에 포함하지 않는다.
- 환경 변수 직접 접근 금지: os.getenv 사용 금지 → settings.py(SSOT)만 사용.

변경 이력
--------------------------------------------------------
- 2026-03-03:
  1) ENV 직접 접근 제거: DEFAULT_* 를 os.getenv로 읽던 구조 제거 → settings 기반으로 통합
  2) 설정 SSOT 강제: openai_api_key/openai_model/openai_* 누락 시 즉시 예외
  3) timeout/latency budget을 요청 timeout으로도 강제(가능한 범위에서)
- 2026-03-04:
  1) docstring 위치 정상화(__future__ import보다 앞) — 모듈 문서/정적 분석 정합
  2) JSON 추출 로직 강화(raw_decode 기반) — 문자열 내부 braces로 인한 오탐/파손 제거
  3) OpenAI client 생성 레이스 방지(락) — 동시 호출 안정성 강화
  4) user_payload JSON 직렬화 STRICT(allow_nan=False) — NaN/Inf 즉시 예외
  5) 로그 실패가 본 예외를 가리지 않게 처리 — 예외 전파 보장
- 2026-03-05:
  1) OpenAI Chat Completions API → Responses API 전환 (GPT-5 계열 호환 / Logs→Responses 기록 보장)
  2) 토큰 파라미터: max_* → max_output_tokens 로 전환
  3) store=True 사용(Responses 로그 저장)
  4) 응답 텍스트 추출을 output_text 단일 의존에서 제거:
     - output_text는 SDK convenience(없거나 empty 가능)
     - output 배열의 message.content[*].type=="output_text"의 text를 STRICT 추출
  5) 기존 STRICT 정책 유지(예산/예외/민감정보 로그 금지/JSON 추출)
- 2026-03-14:
  1) FIX(ROOT-CAUSE): call_chat_json()을 Responses API JSON mode(text.format=json_object)로 전환
  2) FIX(STRICT): JSON 호출은 "정확히 JSON object 1개만" 반환하도록 transport-level contract 강화
  3) FIX(OPERABILITY): incomplete_details.reason=max_output_tokens 를 설정/예산 오류로 명확히 surface
  4) KEEP(STRICT): partial/incomplete 응답 허용 없이 즉시 예외 유지
========================================================
"""

from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from threading import Lock
from typing import Any, Dict, Optional

from openai import OpenAI

from settings import load_settings

try:
    from infra.telelog import log
except Exception as e:  # pragma: no cover
    raise RuntimeError("infra.telelog import failed (STRICT · NO-FALLBACK · TRADE-GRADE MODE)") from e


class GptEngineError(RuntimeError):
    """OpenAI 호출/파싱/정책 위반 오류."""


SET = load_settings()


def _safe_log(msg: str) -> None:
    """
    STRICT:
    - 로깅 실패가 본 예외 흐름을 가리면 안 된다.
    """
    try:
        log(msg)
    except Exception:
        return


def _fail(stage: str, reason: str, exc: Optional[BaseException] = None) -> None:
    # STRICT: 프롬프트/키/민감정보를 절대 로그에 포함하지 않는다.
    msg = f"[GPT-ENGINE] {stage} 실패: {reason}"
    _safe_log(msg)
    if exc is None:
        raise GptEngineError(msg)
    raise GptEngineError(msg) from exc


def _require_nonempty_str(stage: str, v: Any, name: str) -> str:
    if v is None:
        _fail(stage, f"{name} is None")
    s = str(v).strip()
    if not s:
        _fail(stage, f"{name} is empty")
    return s


def _require_int(stage: str, v: Any, name: str) -> int:
    if v is None:
        _fail(stage, f"{name} is None")
    if isinstance(v, bool):
        _fail(stage, f"{name} must be int (bool not allowed)")
    try:
        iv = int(v)
    except Exception as e:
        _fail(stage, f"{name} must be int", e)
    return iv


def _require_float(stage: str, v: Any, name: str) -> float:
    if v is None:
        _fail(stage, f"{name} is None")
    if isinstance(v, bool):
        _fail(stage, f"{name} must be float (bool not allowed)")
    try:
        fv = float(v)
    except Exception as e:
        _fail(stage, f"{name} must be float", e)
    if not math.isfinite(fv):
        _fail(stage, f"{name} must be finite")
    return float(fv)


def _settings_openai_api_key() -> str:
    if not hasattr(SET, "openai_api_key"):
        raise GptEngineError("settings.openai_api_key missing (STRICT)")
    return _require_nonempty_str("config", getattr(SET, "openai_api_key"), "settings.openai_api_key")


def _settings_model() -> str:
    if not hasattr(SET, "openai_model"):
        raise GptEngineError("settings.openai_model missing (STRICT)")
    return _require_nonempty_str("config", getattr(SET, "openai_model"), "settings.openai_model")


def _settings_max_tokens() -> int:
    if not hasattr(SET, "openai_max_tokens"):
        raise GptEngineError("settings.openai_max_tokens missing (STRICT)")
    mt = _require_int("config", getattr(SET, "openai_max_tokens"), "settings.openai_max_tokens")
    if mt <= 0 or mt > 4096:
        _fail("config", "openai_max_tokens out of range (1..4096)")
    return int(mt)


def _settings_temperature() -> float:
    if not hasattr(SET, "openai_temperature"):
        raise GptEngineError("settings.openai_temperature missing (STRICT)")
    t = _require_float("config", getattr(SET, "openai_temperature"), "settings.openai_temperature")
    if t < 0.0 or t > 2.0:
        _fail("config", "openai_temperature out of range [0,2]")
    return float(t)


def _settings_max_latency_sec() -> float:
    if not hasattr(SET, "openai_max_latency_sec"):
        raise GptEngineError("settings.openai_max_latency_sec missing (STRICT)")
    b = _require_float("config", getattr(SET, "openai_max_latency_sec"), "settings.openai_max_latency_sec")
    if b <= 0:
        _fail("config", "openai_max_latency_sec must be > 0")
    return float(b)


@dataclass(frozen=True, slots=True)
class GptRawResponse:
    model: str
    latency_sec: float
    text: str


@dataclass(frozen=True, slots=True)
class GptJsonResponse:
    model: str
    latency_sec: float
    text: str
    obj: Dict[str, Any]


_CLIENT: Optional[OpenAI] = None
_CLIENT_LOCK: Lock = Lock()


def _get_client() -> OpenAI:
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT

    with _CLIENT_LOCK:
        if _CLIENT is not None:
            return _CLIENT
        api_key = _settings_openai_api_key()
        _CLIENT = OpenAI(api_key=api_key)
        return _CLIENT


_JSON_DECODER = json.JSONDecoder()


def _extract_first_json_object(text: str) -> Dict[str, Any]:
    """
    STRICT:
    - 일반 텍스트 응답 안에 JSON object가 섞여 있을 수 있는 경로용.
    - 첫 JSON object만 추출한다.
    """
    s = str(text).strip()
    if not s:
        _fail("parse", "empty response text")

    if s.startswith("{") and s.endswith("}"):
        try:
            obj = json.loads(s)
        except Exception as e:
            _fail("parse", "json.loads failed (full text)", e)
        if not isinstance(obj, dict):
            _fail("parse", f"json root must be object (got={type(obj).__name__})")
        return obj

    idx = 0
    while True:
        start = s.find("{", idx)
        if start < 0:
            _fail("parse", "no JSON object found")
        try:
            obj, _end = _JSON_DECODER.raw_decode(s, start)
        except Exception:
            idx = start + 1
            continue
        if not isinstance(obj, dict):
            _fail("parse", f"json root must be object (got={type(obj).__name__})")
        return obj


def _extract_exact_json_object_strict(text: str) -> Dict[str, Any]:
    """
    STRICT:
    - JSON mode 경로 전용.
    - 응답 전체가 정확히 JSON object 1개여야 한다.
    - prefix/suffix/commentary/markdown fence 허용 금지.
    """
    s = str(text).strip()
    if not s:
        _fail("parse", "empty response text")

    try:
        obj, end_idx = _JSON_DECODER.raw_decode(s)
    except Exception as e:
        _fail("parse", "exact json object decode failed", e)

    trailing = s[end_idx:].strip()
    if trailing:
        _fail("parse", "json response contains trailing non-json text (STRICT)")

    if not isinstance(obj, dict):
        _fail("parse", f"json root must be object (got={type(obj).__name__})")

    return obj


def _extract_response_text_strict(resp: Any) -> str:
    """
    STRICT:
    - output_text는 SDK convenience이며 empty일 수 있다.
    - 따라서 output 배열의 message.content[*].type=="output_text"의 text를 정식으로 추출한다.
    """
    t = getattr(resp, "output_text", None)
    if isinstance(t, str) and t.strip():
        return t.strip()

    output = getattr(resp, "output", None)
    if isinstance(output, list):
        for item in output:
            content_list = getattr(item, "content", None)
            if not isinstance(content_list, list):
                continue

            for c in content_list:
                ctype = getattr(c, "type", None)
                if ctype == "refusal":
                    refusal = getattr(c, "refusal", None)
                    r = str(refusal).strip() if refusal is not None else ""
                    _fail("openai", f"refusal: {r[:120] if r else 'refused'}")

                if ctype != "output_text":
                    continue

                text = getattr(c, "text", None)
                if isinstance(text, str) and text.strip():
                    return text.strip()

    status = getattr(resp, "status", None)
    if isinstance(status, str) and status == "incomplete":
        details = getattr(resp, "incomplete_details", None)
        reason = getattr(details, "reason", None) if details is not None else None
        r = str(reason).strip() if reason is not None else ""
        if r == "max_output_tokens":
            _fail(
                "openai",
                "response incomplete: max_output_tokens "
                "(increase settings.openai_max_tokens or reduce response budget)",
            )
        _fail("openai", f"response incomplete: {r or 'unknown'}")

    _fail("openai", "response text missing/empty")


def _build_json_only_system_prompt(system_prompt: str) -> str:
    """
    STRICT JSON MODE PROMPT

    OpenAI Responses API 규칙:
    text.format = {"type": "json_object"} 사용 시
    입력 메시지 안에 반드시 'json' 단어가 포함되어야 한다.
 
    이 함수는 항상 JSON contract를 명시적으로 강제한다.
    """
    sp = _require_nonempty_str("input", system_prompt, "system_prompt")

    return (
        sp
        + "\n\n"
        + "OUTPUT CONTRACT (STRICT): "
        + "Return exactly one valid json object. "
        + "The response must be json. "   
        + "Do not wrap it in markdown. "
        + "Do not add commentary, explanation, prefix, or suffix."
        + "Only output JSON."
    )


def _responses_create_strict(
    *,
    model: str,
    instructions: str,
    input_text: str,
    max_output_tokens: int,
    max_latency_sec: float,
    text_format: Dict[str, Any],
) -> GptRawResponse:
    m = _require_nonempty_str("input", model, "model")
    inst = _require_nonempty_str("input", instructions, "instructions")
    inp = _require_nonempty_str("input", input_text, "input_text")
    mt = _require_int("input", max_output_tokens, "max_output_tokens")
    if mt <= 0 or mt > 4096:
        _fail("input", "max_output_tokens out of allowed range (1..4096)")
    budget = _require_float("input", max_latency_sec, "max_latency_sec")
    if budget <= 0:
        _fail("input", "max_latency_sec must be > 0")
    if not isinstance(text_format, dict) or not text_format:
        _fail("input", "text_format must be non-empty dict")

    base_client = _get_client()

    t0 = time.time()
    try:
        client = base_client.with_options(timeout=float(budget))
        if not hasattr(client, "responses"):
            _fail("openai", "client.with_options() lost 'responses' namespace (STRICT)")

        resp = client.responses.create(
            model=m,
            instructions=inst,
            input=inp,
            max_output_tokens=int(mt),
            text={"format": text_format},
            store=True,
        )
    except Exception as e:
        _fail("openai", f"request failed: {e.__class__.__name__}", e)

    dt = time.time() - t0
    if dt > budget:
        _fail("openai", f"latency budget exceeded: {dt:.2f}s > {budget:.2f}s")

    content = _extract_response_text_strict(resp)
    if not isinstance(content, str) or not content.strip():
        _fail("openai", "empty content")

    return GptRawResponse(model=m, latency_sec=float(dt), text=content.strip())


def call_chat(
    *,
    system_prompt: str,
    user_content: str,
    model: Optional[str] = None,
    max_tokens: Optional[int] = None,
    temperature: Optional[float] = None,
    max_latency_sec: Optional[float] = None,
) -> GptRawResponse:
    """
    OpenAI Responses 호출(텍스트만 반환) — STRICT
    """
    m = _require_nonempty_str("input", (model or _settings_model()), "model")
    mt = _require_int("input", (max_tokens if max_tokens is not None else _settings_max_tokens()), "max_tokens")
    if mt <= 0 or mt > 4096:
        _fail("input", "max_tokens out of allowed range (1..4096)")

    temp = _require_float("input", (temperature if temperature is not None else _settings_temperature()), "temperature")
    if temp < 0.0 or temp > 2.0:
        _fail("input", "temperature out of range [0,2]")
    _ = temp

    budget = _require_float(
        "input",
        (max_latency_sec if max_latency_sec is not None else _settings_max_latency_sec()),
        "max_latency_sec",
    )
    if budget <= 0:
        _fail("input", "max_latency_sec must be > 0")

    sp = _require_nonempty_str("input", system_prompt, "system_prompt")
    uc = _require_nonempty_str("input", user_content, "user_content")

    return _responses_create_strict(
        model=m,
        instructions=sp,
        input_text=uc,
        max_output_tokens=int(mt),
        max_latency_sec=float(budget),
        text_format={"type": "text"},
    )


def call_chat_json(
    *,
    system_prompt: str,
    user_payload: Dict[str, Any],
    model: Optional[str] = None,
    max_tokens: Optional[int] = None,
    temperature: Optional[float] = None,
    max_latency_sec: Optional[float] = None,
) -> GptJsonResponse:
    """
    OpenAI 호출 후 JSON object 1개를 추출해 반환 — STRICT
    """
    if not isinstance(user_payload, dict) or not user_payload:
        _fail("input", "user_payload must be non-empty dict")

    try:
        user_content = json.dumps(user_payload, ensure_ascii=False, allow_nan=False)
    except Exception as e:
        _fail("input", "user_payload must be JSON-serializable (strict)", e)

    m = _require_nonempty_str("input", (model or _settings_model()), "model")
    mt = _require_int("input", (max_tokens if max_tokens is not None else _settings_max_tokens()), "max_tokens")
    if mt <= 0 or mt > 4096:
        _fail("input", "max_tokens out of allowed range (1..4096)")

    temp = _require_float("input", (temperature if temperature is not None else _settings_temperature()), "temperature")
    if temp < 0.0 or temp > 2.0:
        _fail("input", "temperature out of range [0,2]")
    _ = temp

    budget = _require_float(
        "input",
        (max_latency_sec if max_latency_sec is not None else _settings_max_latency_sec()),
        "max_latency_sec",
    )
    if budget <= 0:
        _fail("input", "max_latency_sec must be > 0")

    raw = _responses_create_strict(
        model=m,
        instructions=_build_json_only_system_prompt(system_prompt),
        input_text=user_content,
        max_output_tokens=int(mt),
        max_latency_sec=float(budget),
        text_format={"type": "json_object"},
    )

    obj = _extract_exact_json_object_strict(raw.text)
    return GptJsonResponse(model=raw.model, latency_sec=raw.latency_sec, text=raw.text, obj=obj)


__all__ = [
    "GptEngineError",
    "GptRawResponse",
    "GptJsonResponse",
    "call_chat",
    "call_chat_json",
]