# events/commentary_engine.py
"""
========================================================
FILE: events/commentary_engine.py
AUTO-TRADER - Commentary Engine
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

목적:
- 이벤트(EventBus) 발생 시, 엔진의 근거(reason/indicators/리스크태그)를 기반으로
  GPT 해설 텍스트를 생성한다.
- 생성된 해설은 CommentaryQueue에 저장한다.
- 데이터가 부족하면 즉시 예외를 발생시킨다.

절대 원칙:
- STRICT · NO-FALLBACK
- settings.py(SSOT) 외 환경변수 직접 접근 금지
- print() 금지 / logging 사용
- 필수 입력/설정/응답 누락 시 즉시 예외
- OpenAI 호출 실패는 즉시 예외
- 시장 판단/매매 결정 금지, 해설만 수행

변경 이력:
- 2026-03-13:
  1) FIX(SSOT): os.getenv 제거, load_settings() 기반 설정 사용으로 변경
  2) FIX(EXCEPTION): common.exceptions_strict 공통 예외 계층 적용
  3) FIX(CONTRACT): Event payload / OpenAI response strict 검증 강화
  4) FIX(ARCH): settings 주입 가능 구조로 변경
- 2026-03-07:
  1) 초기 구현
========================================================
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, Optional, Set

import requests

from common.exceptions_strict import (
    StrictConfigError,
    StrictDataError,
    StrictExternalError,
)
from events.commentary_queue import CommentaryItem, GLOBAL_COMMENTARY_QUEUE
from events.event_bus import Event, subscribe
from settings import load_settings

logger = logging.getLogger(__name__)

_TARGET_EVENT_TYPES: Set[str] = {
    "on_entry_filled",
    "on_exit_filled",
    "on_slippage_block",
    "on_risk_guard_trigger",
    "on_tp_sl_reset_failed",
    "on_exchange_sync_error",
    "on_hold_update",
}


class CommentaryConfigError(StrictConfigError):
    """commentary engine 설정 계약 위반."""


class CommentaryContractError(StrictDataError):
    """event payload / commentary item / OpenAI response 계약 위반."""


class CommentaryExternalError(StrictExternalError):
    """OpenAI 외부 호출 실패."""


def _now_ms() -> int:
    return int(time.time() * 1000)


def _require_str(v: Any, name: str) -> str:
    if not isinstance(v, str):
        raise CommentaryContractError(f"{name} must be str (STRICT), got={type(v).__name__}")
    s = v.strip()
    if not s:
        raise CommentaryContractError(f"{name} must not be empty (STRICT)")
    return s


def _require_dict(v: Any, name: str) -> Dict[str, Any]:
    if not isinstance(v, dict):
        raise CommentaryContractError(f"{name} must be dict (STRICT), got={type(v).__name__}")
    if not v:
        raise CommentaryContractError(f"{name} must not be empty (STRICT)")
    return dict(v)


def _require_positive_int(v: Any, name: str) -> int:
    if isinstance(v, bool):
        raise CommentaryContractError(f"{name} must be int (STRICT), bool not allowed")
    try:
        iv = int(v)
    except Exception as exc:
        raise CommentaryContractError(f"{name} must be int (STRICT): {exc}") from exc
    if iv <= 0:
        raise CommentaryContractError(f"{name} must be > 0 (STRICT)")
    return iv


def _resolve_settings(settings: Optional[Any]) -> Any:
    st = settings if settings is not None else load_settings()
    if st is None:
        raise CommentaryConfigError("settings resolution failed (STRICT)")
    return st


def _require_setting_str(settings: Any, name: str) -> str:
    if not hasattr(settings, name):
        raise CommentaryConfigError(f"settings.{name} missing (STRICT)")
    return _require_str(getattr(settings, name), f"settings.{name}")


def _require_setting_int(settings: Any, name: str) -> int:
    if not hasattr(settings, name):
        raise CommentaryConfigError(f"settings.{name} missing (STRICT)")
    return _require_positive_int(getattr(settings, name), f"settings.{name}")


def _build_prompt(ev: Event) -> Dict[str, Any]:
    """Build a strict prompt payload. Raises if required info missing."""
    if not isinstance(ev, Event):
        raise CommentaryContractError(f"ev must be Event (STRICT), got={type(ev).__name__}")

    payload = _require_dict(ev.payload, "event.payload")

    symbol = _require_str(payload.get("symbol"), "payload.symbol")
    reason = _require_str(payload.get("reason"), "payload.reason")
    event_type = _require_str(ev.event_type, "event.event_type")
    event_id = _require_str(ev.event_id, "event.event_id")
    ts_ms = _require_positive_int(ev.ts_ms, "event.ts_ms")

    side = str(payload.get("side") or "").strip()
    price = payload.get("price")
    confidence = payload.get("confidence")
    risk_tags = payload.get("risk_tags")
    indicators = payload.get("indicators")
    timeframe = payload.get("timeframe")

    system_msg = (
        "너는 자동매매 엔진의 해설자다. "
        "시장 예측이나 투자 조언을 하지 않는다. "
        "오직 제공된 엔진 이벤트와 근거를 한국어로 간결하고 명확하게 설명한다. "
        "확정적 표현(무조건/확실히/반드시/수익 보장)을 금지한다. "
        "마지막 문장은 항상 '투자 판단과 책임은 본인에게 있습니다.'로 끝낸다."
    )

    user_obj = {
        "event_id": event_id,
        "event_type": event_type,
        "symbol": symbol,
        "side": side,
        "price": price,
        "reason": reason,
        "confidence": confidence,
        "timeframe": timeframe,
        "risk_tags": risk_tags,
        "indicators": indicators,
        "ts_ms": ts_ms,
    }

    user_msg = (
        "다음 JSON은 자동매매 엔진 이벤트다. "
        "이 이벤트가 의미하는 바를 2~4문장으로 설명하라. "
        "1) 왜 그런 행동(진입/유지/청산/차단)을 했는지 "
        "2) 현재 리스크/주의점 1개 "
        "3) 마지막 문장은 고정 문구로 끝낼 것\n\n"
        f"{json.dumps(user_obj, ensure_ascii=False)}"
    )

    return {"system": system_msg, "user": user_msg, "symbol": symbol}


def _call_openai_chat(
    *,
    settings: Any,
    system: str,
    user: str,
) -> str:
    """Call OpenAI Chat Completions via HTTP. STRICT."""
    api_key = _require_setting_str(settings, "openai_api_key")
    model = _require_setting_str(settings, "openai_model")
    base_url = _require_setting_str(settings, "openai_base_url")
    timeout_sec = _require_setting_int(settings, "request_timeout_sec")

    url = f"{base_url.rstrip('/')}/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    body = {
        "model": model,
        "temperature": 0.4,
        "messages": [
            {"role": "system", "content": _require_str(system, "system")},
            {"role": "user", "content": _require_str(user, "user")},
        ],
    }

    try:
        resp = requests.post(url, headers=headers, json=body, timeout=timeout_sec)
    except Exception as e:
        raise CommentaryExternalError(f"OpenAI request failed (STRICT): {type(e).__name__}") from e

    if resp.status_code != 200:
        txt = (resp.text or "")[:500]
        raise CommentaryExternalError(f"OpenAI HTTP {resp.status_code}: {txt}")

    try:
        data = resp.json()
    except Exception as e:
        raise CommentaryExternalError(f"OpenAI invalid JSON (STRICT): {type(e).__name__}") from e

    if not isinstance(data, dict):
        raise CommentaryContractError("OpenAI response must be dict (STRICT)")

    try:
        content = data["choices"][0]["message"]["content"]
    except Exception as e:
        raise CommentaryContractError("OpenAI response missing choices[0].message.content (STRICT)") from e

    out = _require_str(content, "openai.content")
    return out


def generate_commentary_for_event(
    ev: Event,
    *,
    settings: Optional[Any] = None,
) -> CommentaryItem:
    """Generate commentary for a single event and store it in queue."""
    st = _resolve_settings(settings)
    prompt = _build_prompt(ev)
    text = _call_openai_chat(
        settings=st,
        system=prompt["system"],
        user=prompt["user"],
    )

    model = _require_setting_str(st, "openai_model")

    item = CommentaryItem(
        ts_ms=_now_ms(),
        event_id=_require_str(ev.event_id, "event.event_id"),
        event_type=_require_str(ev.event_type, "event.event_type"),
        symbol=_require_str(prompt["symbol"], "prompt.symbol"),
        text=text,
        meta={"source": "gpt", "model": model},
    )
    GLOBAL_COMMENTARY_QUEUE.push(item)
    return item


def handle_event(ev: Event) -> None:
    """Default subscriber hook: generates commentary for selected events only."""
    if not isinstance(ev, Event):
        raise CommentaryContractError(f"ev must be Event (STRICT), got={type(ev).__name__}")

    event_type = _require_str(ev.event_type, "event.event_type")
    if event_type not in _TARGET_EVENT_TYPES:
        return

    # STRICT: 실패 시 예외 전파
    generate_commentary_for_event(ev)


def init_commentary_engine() -> None:
    """Register event handlers. Call this once on startup."""
    for et in sorted(_TARGET_EVENT_TYPES):
        subscribe(et, handle_event)


__all__ = [
    "CommentaryConfigError",
    "CommentaryContractError",
    "CommentaryExternalError",
    "init_commentary_engine",
    "generate_commentary_for_event",
    "handle_event",
]