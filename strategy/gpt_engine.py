from __future__ import annotations

"""
========================================================
FILE: strategy/_gpt_engine.py
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
========================================================
"""

import json
import math
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from openai import OpenAI

from settings import load_settings

try:
    from infra.telelog import log
except Exception as e:  # pragma: no cover
    raise RuntimeError("infra.telelog import failed (STRICT · NO-FALLBACK · TRADE-GRADE MODE)") from e


# -----------------------------------------------------------------------------
# Exceptions (STRICT)
# -----------------------------------------------------------------------------
class GptEngineError(RuntimeError):
    """OpenAI 호출/파싱/정책 위반 오류."""


# -----------------------------------------------------------------------------
# Settings (SSOT)
# -----------------------------------------------------------------------------
SET = load_settings()


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
        _fail("config", f"openai_max_tokens out of range (1..4096)")
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


# -----------------------------------------------------------------------------
# Data structures
# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
# OpenAI client (STRICT)
# -----------------------------------------------------------------------------
_CLIENT: Optional[OpenAI] = None


def _get_client() -> OpenAI:
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT

    api_key = _settings_openai_api_key()
    _CLIENT = OpenAI(api_key=api_key)
    return _CLIENT


# -----------------------------------------------------------------------------
# Helpers (STRICT)
# -----------------------------------------------------------------------------
def _fail(stage: str, reason: str, exc: Optional[BaseException] = None) -> None:
    # STRICT: 프롬프트/키/민감정보를 절대 로그에 포함하지 않는다.
    msg = f"[GPT-ENGINE] {stage} 실패: {reason}"
    log(msg)
    if exc is None:
        raise GptEngineError(msg)
    raise GptEngineError(msg) from exc


def _extract_first_json_object(text: str) -> Dict[str, Any]:
    """
    STRICT: 응답에서 첫 JSON object를 추출한다.
    - 코드블록/앞뒤 텍스트가 있어도 첫 {...} 블록을 찾아 파싱한다.
    - JSON root는 반드시 object(dict)여야 한다.
    """
    s = str(text).strip()
    if not s:
        _fail("parse", "empty response text")

    # Fast path: whole text JSON
    if s.startswith("{") and s.endswith("}"):
        try:
            obj = json.loads(s)
        except Exception as e:
            _fail("parse", "json.loads failed (full text)", e)
        if not isinstance(obj, dict):
            _fail("parse", f"json root must be object (got={type(obj).__name__})")
        return obj

    start = s.find("{")
    if start < 0:
        _fail("parse", "no '{' found")

    depth = 0
    for i in range(start, len(s)):
        ch = s[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                chunk = s[start : i + 1]
                try:
                    obj = json.loads(chunk)
                except Exception as e:
                    _fail("parse", "json.loads failed (chunk)", e)
                if not isinstance(obj, dict):
                    _fail("parse", f"json root must be object (got={type(obj).__name__})")
                return obj

    _fail("parse", "unterminated json object")
    raise AssertionError("unreachable")  # pragma: no cover


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
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
    OpenAI Chat 호출(텍스트만 반환) — STRICT
    - caller는 user_content에 민감정보를 포함하지 않아야 한다.
    """
    m = _require_nonempty_str("input", (model or _settings_model()), "model")
    mt = _require_int("input", (max_tokens if max_tokens is not None else _settings_max_tokens()), "max_tokens")
    if mt <= 0 or mt > 4096:
        _fail("input", "max_tokens out of allowed range (1..4096)")

    temp = _require_float("input", (temperature if temperature is not None else _settings_temperature()), "temperature")
    if temp < 0.0 or temp > 2.0:
        _fail("input", "temperature out of range [0,2]")

    budget = _require_float("input", (max_latency_sec if max_latency_sec is not None else _settings_max_latency_sec()), "max_latency_sec")
    if budget <= 0:
        _fail("input", "max_latency_sec must be > 0")

    sp = _require_nonempty_str("input", system_prompt, "system_prompt")
    uc = _require_nonempty_str("input", user_content, "user_content")

    client = _get_client()

    t0 = time.time()
    try:
        # STRICT: timeout도 budget으로 강제 (라이브러리가 지원하지 않으면 즉시 실패(=정책 위반))
        resp = client.chat.completions.create(
            model=m,
            messages=[
                {"role": "system", "content": sp},
                {"role": "user", "content": uc},
            ],
            temperature=float(temp),
            max_tokens=int(mt),
            timeout=float(budget),
        )
    except Exception as e:
        # sanitize: 프롬프트/키/URL 등 민감/대용량 정보는 포함 금지
        _fail("openai", f"request failed: {e.__class__.__name__}", e)

    dt = time.time() - t0
    if dt > budget:
        _fail("openai", f"latency budget exceeded: {dt:.2f}s > {budget:.2f}s")

    try:
        content = resp.choices[0].message.content  # type: ignore[index]
    except Exception as e:
        _fail("openai", "response content missing", e)

    if not isinstance(content, str) or not content.strip():
        _fail("openai", "empty content")

    return GptRawResponse(model=m, latency_sec=float(dt), text=content.strip())


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
    OpenAI Chat 호출 후 JSON object 1개를 추출해 반환 — STRICT
    """
    if not isinstance(user_payload, dict) or not user_payload:
        _fail("input", "user_payload must be non-empty dict")

    user_content = json.dumps(user_payload, ensure_ascii=False)
    raw = call_chat(
        system_prompt=system_prompt,
        user_content=user_content,
        model=model,
        max_tokens=max_tokens,
        temperature=temperature,
        max_latency_sec=max_latency_sec,
    )
    obj = _extract_first_json_object(raw.text)
    return GptJsonResponse(model=raw.model, latency_sec=raw.latency_sec, text=raw.text, obj=obj)


__all__ = [
    "GptEngineError",
    "GptRawResponse",
    "GptJsonResponse",
    "call_chat",
    "call_chat_json",
]