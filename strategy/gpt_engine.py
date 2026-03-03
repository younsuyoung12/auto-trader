from __future__ import annotations

"""
========================================================
FILE: strategy/_gpt_engine.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
역할
- OpenAI 호출(Transport) + latency 측정 + 응답 텍스트 추출 + JSON object 1개 추출을 담당한다.
- "전략 판단"이나 "매매 결정"은 하지 않는다. (상위 레이어가 책임)
- 상위 모듈(gpt_supervisor 등)은 이 모듈을 통해서만 OpenAI를 호출한다(단일 진입점).

절대 원칙 (STRICT · NO-FALLBACK)
- OPENAI_API_KEY 없으면 즉시 예외.
- 응답이 비어있거나 JSON 파싱 불가하면 즉시 예외.
- timeout/latency 예산 초과 시 즉시 예외.
- 민감정보(키/시그니처/주문ID 등)는 로그/예외 메시지에 포함하지 않는다.

변경 이력
--------------------------------------------------------
- 2026-03-03:
  1) 신규 생성: OpenAI 호출 전용 엔진(단일 진입점) + JSON 1개 추출 + latency guard
========================================================
"""

import json
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from openai import OpenAI

try:
    from infra.telelog import log
except Exception as e:  # pragma: no cover
    raise RuntimeError("infra.telelog import failed (STRICT · NO-FALLBACK · PRODUCTION MODE)") from e


# -----------------------------------------------------------------------------
# Exceptions (STRICT)
# -----------------------------------------------------------------------------
class GptEngineError(RuntimeError):
    """OpenAI 호출/파싱/정책 위반 오류."""


# -----------------------------------------------------------------------------
# Config (STRICT)
# -----------------------------------------------------------------------------
DEFAULT_MODEL: str = os.getenv("OPENAI_SUPERVISOR_MODEL", os.getenv("OPENAI_TRADER_MODEL", "gpt-4o-mini")).strip()
DEFAULT_MAX_TOKENS: int = int(os.getenv("OPENAI_SUPERVISOR_MAX_TOKENS", os.getenv("OPENAI_TRADER_MAX_TOKENS", "512")))
DEFAULT_TEMPERATURE: float = float(os.getenv("OPENAI_SUPERVISOR_TEMPERATURE", "0.2"))
DEFAULT_MAX_LATENCY_SEC: float = float(os.getenv("OPENAI_SUPERVISOR_MAX_LATENCY", os.getenv("OPENAI_TRADER_MAX_LATENCY", "12")))


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

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise GptEngineError("OPENAI_API_KEY missing (STRICT)")
    _CLIENT = OpenAI(api_key=api_key)
    return _CLIENT


# -----------------------------------------------------------------------------
# Helpers (STRICT)
# -----------------------------------------------------------------------------
def _fail(stage: str, reason: str, exc: Optional[BaseException] = None) -> None:
    msg = f"[GPT-ENGINE] {stage} 실패: {reason}"
    log(msg)
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
    try:
        iv = int(v)
    except Exception as e:
        _fail(stage, f"{name} must be int (got={v!r})", e)
    return iv


def _require_float(stage: str, v: Any, name: str) -> float:
    try:
        fv = float(v)
    except Exception as e:
        _fail(stage, f"{name} must be float (got={v!r})", e)
    if not math.isfinite(fv):
        _fail(stage, f"{name} must be finite (got={fv})")
    return fv


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
            _fail("parse", f"json root must be object (got={type(obj)})")
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
                    _fail("parse", f"json root must be object (got={type(obj)})")
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
    m = _require_nonempty_str("input", (model or DEFAULT_MODEL), "model")
    mt = _require_int("input", (max_tokens if max_tokens is not None else DEFAULT_MAX_TOKENS), "max_tokens")
    if mt <= 0 or mt > 4096:
        _fail("input", f"max_tokens out of allowed range (1..4096) (got={mt})")

    temp = _require_float("input", (temperature if temperature is not None else DEFAULT_TEMPERATURE), "temperature")
    if temp < 0.0 or temp > 2.0:
        _fail("input", f"temperature out of range [0,2] (got={temp})")

    budget = _require_float("input", (max_latency_sec if max_latency_sec is not None else DEFAULT_MAX_LATENCY_SEC), "max_latency_sec")
    if budget <= 0:
        _fail("input", f"max_latency_sec must be > 0 (got={budget})")

    sp = _require_nonempty_str("input", system_prompt, "system_prompt")
    uc = _require_nonempty_str("input", user_content, "user_content")

    client = _get_client()

    t0 = time.time()
    try:
        resp = client.chat.completions.create(
            model=m,
            messages=[
                {"role": "system", "content": sp},
                {"role": "user", "content": uc},
            ],
            temperature=temp,
            max_tokens=mt,
        )
    except Exception as e:
        # sanitize: do not include prompts or URL; only error class
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