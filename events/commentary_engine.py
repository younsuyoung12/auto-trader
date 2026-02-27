# events/commentary_engine.py
# =============================================================================
# AUTO-TRADER - Commentary Engine (STRICT / NO-FALLBACK)
# -----------------------------------------------------------------------------
# 목적:
# - 이벤트(EventBus) 발생 시, 엔진의 "근거(reason/indicators/리스크태그)"를 기반으로
#   GPT 해설 텍스트를 생성한다.
# - 생성된 해설은 CommentaryQueue에 저장한다.
# - 데이터가 부족하면 즉시 예외 -> Render 로그에 에러가 그대로 보이도록 한다.
#
# 구현 범위:
# - OpenAI 호출부는 "환경변수 기반"으로 동작한다.
#   (OPENAI_API_KEY, OPENAI_MODEL)
# - 폴백 금지: 키/모델/필수 입력이 없으면 즉시 예외
#
# NOTE:
# - 이 엔진은 "시장 판단"을 하지 않는다.
# - 오직 "엔진이 이미 결정한 이유"를 설명한다.
# =============================================================================

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Optional

import requests

from events.event_bus import Event, subscribe
from events.commentary_queue import CommentaryItem, GLOBAL_COMMENTARY_QUEUE


class CommentaryEngineError(RuntimeError):
    """Raised when commentary generation fails (strict)."""


def _now_ms() -> int:
    return int(time.time() * 1000)


def _require_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        raise CommentaryEngineError(f"missing required field: {name}")
    return s


def _require_dict(v: Any, name: str) -> Dict[str, Any]:
    if not isinstance(v, dict):
        raise CommentaryEngineError(f"{name} must be dict")
    return v


def _openai_api_key() -> str:
    key = os.getenv("OPENAI_API_KEY", "").strip()
    if not key:
        raise CommentaryEngineError("OPENAI_API_KEY missing")
    return key


def _openai_model() -> str:
    model = os.getenv("OPENAI_MODEL", "").strip() or "gpt-4o-mini"
    # 모델명 폴백 자체는 괜찮지만 "키가 없으면"은 폴백 금지로 예외
    return model


def _openai_base_url() -> str:
    return os.getenv("OPENAI_BASE_URL", "").strip() or "https://api.openai.com/v1"


def _build_prompt(ev: Event) -> Dict[str, Any]:
    """Build a strict prompt payload. Raises if required info missing."""
    payload = _require_dict(ev.payload, "event.payload")

    symbol = _require_str(payload.get("symbol"), "payload.symbol")
    reason = _require_str(payload.get("reason"), "payload.reason")

    # optional but recommended
    side = str(payload.get("side") or "").strip()
    price = payload.get("price", None)
    confidence = payload.get("confidence", None)
    risk_tags = payload.get("risk_tags", None)
    indicators = payload.get("indicators", None)
    timeframe = payload.get("timeframe", None)

    system_msg = (
        "너는 자동매매 엔진의 '해설자'다. "
        "너는 시장을 예측하거나 투자 조언을 하지 않는다. "
        "오직 제공된 엔진 이벤트/근거를 한국어로 간결하고 명확하게 설명한다. "
        "확정적 표현(무조건/확실히/반드시/수익 보장)을 금지한다. "
        "마지막 문장은 항상 '투자 판단과 책임은 본인에게 있습니다.'로 끝낸다."
    )

    user_obj = {
        "event_type": ev.event_type,
        "symbol": symbol,
        "side": side,
        "price": price,
        "reason": reason,
        "confidence": confidence,
        "timeframe": timeframe,
        "risk_tags": risk_tags,
        "indicators": indicators,
        "ts_ms": ev.ts_ms,
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


def _call_openai_chat(system: str, user: str) -> str:
    """Call OpenAI Chat Completions via HTTP. STRICT."""
    key = _openai_api_key()
    base_url = _openai_base_url()
    model = _openai_model()

    url = f"{base_url}/chat/completions"
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }

    body = {
        "model": model,
        "temperature": 0.4,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    }

    try:
        resp = requests.post(url, headers=headers, json=body, timeout=20)
    except Exception as e:
        raise CommentaryEngineError(f"OpenAI request failed: {e.__class__.__name__}") from e

    if resp.status_code != 200:
        # STRICT: no fallback. Raise with non-sensitive info.
        txt = (resp.text or "")[:500]
        raise CommentaryEngineError(f"OpenAI HTTP {resp.status_code}: {txt}")

    try:
        data = resp.json()
    except Exception as e:
        raise CommentaryEngineError(f"OpenAI invalid JSON: {e.__class__.__name__}") from e

    try:
        content = data["choices"][0]["message"]["content"]
    except Exception:
        raise CommentaryEngineError("OpenAI response missing choices[0].message.content")

    out = str(content or "").strip()
    if not out:
        raise CommentaryEngineError("OpenAI returned empty content")
    return out


def generate_commentary_for_event(ev: Event) -> CommentaryItem:
    """Generate commentary for a single event and store it in queue."""
    prompt = _build_prompt(ev)
    text = _call_openai_chat(prompt["system"], prompt["user"])

    item = CommentaryItem(
        ts_ms=_now_ms(),
        event_id=ev.event_id,
        event_type=ev.event_type,
        symbol=str(prompt["symbol"]),
        text=text,
        meta={"source": "gpt", "model": _openai_model()},
    )
    GLOBAL_COMMENTARY_QUEUE.push(item)
    return item


def handle_event(ev: Event) -> None:
    """Default subscriber hook: generates commentary for selected events only."""
    # STRICT: if event is one of these, we require reason/symbol etc (validated earlier).
    target_types = {
        "on_entry_filled",
        "on_exit_filled",
        "on_slippage_block",
        "on_risk_guard_trigger",
        "on_tp_sl_reset_failed",
        "on_exchange_sync_error",
        "on_hold_update",
    }
    if ev.event_type not in target_types:
        return

    # No swallow; if it fails, crash and show error in Render logs.
    generate_commentary_for_event(ev)


def init_commentary_engine() -> None:
    """Register event handlers. Call this once on startup."""
    for et in [
        "on_entry_filled",
        "on_exit_filled",
        "on_slippage_block",
        "on_risk_guard_trigger",
        "on_tp_sl_reset_failed",
        "on_exchange_sync_error",
        "on_hold_update",
    ]:
        subscribe(et, handle_event)


__all__ = [
    "CommentaryEngineError",
    "init_commentary_engine",
    "generate_commentary_for_event",
    "handle_event",
]