"""
 gpt_trader.py

 역할
 ----------------------------------------------------
 - BingX Auto Trader에서 GPT-5.1 트레이더 모델에 실제 요청을 보내는 모듈.
 - gpt_decider / entry_flow_ws / market_features_ws 등에서 이 모듈의
   ask_entry_decision(...) 함수만 import 해서 사용한다.

 수정 내용 (2025-11-18~2025-11-20)
 ----------------------------------------------------
 1) OPENAI_TRADER_MODEL / OPENAI_TRADER_MAX_TOKENS / OPENAI_TRADER_MAX_LATENCY
    환경변수로 모델명·토큰 수·최대 대기 시간을 조정할 수 있게 함.
 2) OpenAI Python 신규 클라이언트(responses API)와 구버전(ChatCompletion) 둘 다
    동작하도록 분기 처리.
 3) 응답 텍스트 추출 헬퍼(_extract_text_from_response) 추가로 라이브러리 버전에
    상관없이 최대한 안정적으로 텍스트를 파싱.
 4) 에러/지연 시간/원문 텍스트를 모두 반환해 상위 레이어에서 로깅 및
    디버깅에 활용할 수 있도록 구조 통일.
 5) (2025-11-20) ask_entry_decision(...) 시그니처에
        market_features: dict | None = None
    파라미터를 추가.
    - market_features 내용은 그대로 프롬프트 JSON에 포함.
    - *args / **kwargs 를 그대로 받아 기존 호출부와 하위 호환 유지.
"""

from __future__ import annotations

import os
import time
import json
from typing import Any, Dict, Optional, Tuple

from telelog import log, send_tg

# ──────────────────────────────────────────────────────────────────────────────
# OpenAI 클라이언트 초기화 (신규/구버전 모두 지원)
# ──────────────────────────────────────────────────────────────────────────────
try:
    # New style OpenAI client (>=1.0)
    from openai import OpenAI  # type: ignore

    _OPENAI_CLIENT: Any = OpenAI()
    _USE_RESPONSES_API = True
except Exception:  # pragma: no cover - fallback for legacy library
    try:
        import openai  # type: ignore

        _OPENAI_CLIENT = openai
        _USE_RESPONSES_API = False
    except Exception:
        _OPENAI_CLIENT = None
        _USE_RESPONSES_API = False


OPENAI_TRADER_MODEL = (
    os.getenv("OPENAI_TRADER_MODEL")
    or os.getenv("GPT_ENTRY_MODEL")
    or "gpt-5.1-mini"
)
OPENAI_TRADER_MAX_TOKENS = int(os.getenv("OPENAI_TRADER_MAX_TOKENS", "192"))
OPENAI_TRADER_MAX_LATENCY = float(os.getenv("OPENAI_TRADER_MAX_LATENCY", "8.0"))


# ──────────────────────────────────────────────────────────────────────────────
# 유틸 함수들
# ──────────────────────────────────────────────────────────────────────────────


def _safe_to_json(obj: Any, depth: int = 0, max_depth: int = 4) -> Any:
    """settings, dataclass 등을 JSON 직렬화 가능한 형태로 최대한 변환한다."""
    if depth > max_depth:
        return repr(obj)

    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj

    if isinstance(obj, dict):
        return {str(k): _safe_to_json(v, depth + 1, max_depth) for k, v in obj.items()}

    if isinstance(obj, (list, tuple, set)):
        return [_safe_to_json(v, depth + 1, max_depth) for v in obj]

    if hasattr(obj, "as_dict") and callable(getattr(obj, "as_dict")):
        try:
            return _safe_to_json(obj.as_dict(), depth + 1, max_depth)
        except Exception:
            pass

    if hasattr(obj, "__dict__"):
        try:
            return _safe_to_json(obj.__dict__, depth + 1, max_depth)
        except Exception:
            pass

    return repr(obj)


def _extract_text_from_response(resp: Any) -> str:
    """OpenAI Responses/ChatCompletion/Completion 응답에서 텍스트를 최대한 안전하게 뽑는다."""
    if resp is None:
        return ""

    # 1) Responses API (신규)
    try:
        output = getattr(resp, "output", None)
        if output:
            content0 = output[0].content[0]
            text_obj = getattr(content0, "text", None)
            if isinstance(text_obj, str):
                return text_obj
            if text_obj is not None:
                val = getattr(text_obj, "value", None)
                if isinstance(val, str):
                    return val
            # dict 형태인 경우 (직접 접근)
            if isinstance(content0, dict):
                t = content0.get("text")
                if isinstance(t, dict) and "value" in t:
                    return str(t["value"])
                if isinstance(t, str):
                    return t
    except Exception:
        pass

    # 2) dict 형태 Responses
    if isinstance(resp, dict):
        try:
            if "output" in resp:
                content0 = resp["output"][0]["content"][0]
                t = content0.get("text")
                if isinstance(t, dict) and "value" in t:
                    return str(t["value"])
                if isinstance(t, str):
                    return t
        except Exception:
            pass

    # 3) ChatCompletion/Completion 스타일
    try:
        choices = getattr(resp, "choices", None)
        if choices:
            ch0 = choices[0]
            msg = getattr(ch0, "message", None)
            if msg is not None and getattr(msg, "content", None):
                return str(msg.content)
            text_attr = getattr(ch0, "text", None)
            if text_attr:
                return str(text_attr)
    except Exception:
        pass

    if isinstance(resp, dict) and "choices" in resp:
        ch0 = resp["choices"][0]
        if isinstance(ch0, dict):
            msg = ch0.get("message")
            if isinstance(msg, dict) and "content" in msg:
                return str(msg["content"])
            if "text" in ch0:
                return str(ch0["text"])

    # 마지막 폴백
    try:
        return str(resp)
    except Exception:
        return ""


def _call_openai_trader(prompt: str) -> Tuple[bool, str, float, Optional[Exception]]:
    """실제 OpenAI API 를 호출하고 (성공여부, 텍스트, 지연, 에러)를 반환."""
    if _OPENAI_CLIENT is None:
        err = RuntimeError("openai 라이브러리가 설치되어 있지 않습니다.")
        log(f"[GPT_TRADER] OpenAI 클라이언트 초기화 실패: {err}")
        return False, "", 0.0, err

    start = time.time()
    try:
        if _USE_RESPONSES_API:
            resp = _OPENAI_CLIENT.responses.create(
                model=OPENAI_TRADER_MODEL,
                input=prompt,
                max_output_tokens=OPENAI_TRADER_MAX_TOKENS,
            )
        else:
            # 구버전 호환 (ChatCompletion 우선)
            resp = _OPENAI_CLIENT.ChatCompletion.create(
                model=OPENAI_TRADER_MODEL,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=OPENAI_TRADER_MAX_TOKENS,
            )
        latency = time.time() - start
        text = _extract_text_from_response(resp).strip()
        return True, text, latency, None
    except Exception as e:  # pragma: no cover - 네트워크 오류 등
        latency = time.time() - start
        log(f"[GPT_TRADER] OpenAI 호출 오류: {e}")
        return False, "", latency, e


# ──────────────────────────────────────────────────────────────────────────────
# 외부에서 사용하는 메인 엔트리 포인트
# ──────────────────────────────────────────────────────────────────────────────


def ask_entry_decision(
    *args: Any,
    market_features: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """진입 여부/리스크/TP·SL 을 GPT 에게 물어보는 상위 헬퍼.

    - 기존 호출부와 하위호환을 위해 *args / **kwargs 를 그대로 받아서
      모두 JSON 컨텍스트로 넘긴다.
    - 새로 추가된 market_features 인자는 선택적이며, prompt 에 함께 포함된다.

    반환 형식 예시:
    {
        "status": "OK" | "ERROR" | "PARSE_ERROR",
        "raw": "<GPT 원문>",
        "latency": 1.23,
        "take_trade": true/false,
        "side": "BUY" | "SELL" | null,
        "effective_risk_pct": 0.02,
        "tp_pct": 0.01,
        "sl_pct": 0.02,
        "score": 37.5,
        "reason": "..."
    }
    """
    # 1) 컨텍스트 정리
    context: Dict[str, Any] = {
        "args": _safe_to_json(args),
        "kwargs": _safe_to_json(kwargs),
    }
    if market_features is not None:
        context["market_features"] = _safe_to_json(market_features)

    try:
        context_json = json.dumps(context, ensure_ascii=False)
    except Exception as e:
        log(f"[GPT_TRADER] 컨텍스트 JSON 직렬화 실패: {e}")
        return {
            "status": "ERROR",
            "raw": "",
            "latency": 0.0,
            "take_trade": False,
            "side": None,
            "effective_risk_pct": 0.0,
            "tp_pct": 0.0,
            "sl_pct": 0.0,
            "score": 0.0,
            "reason": f"컨텍스트 직렬화 실패: {e}",
        }

    # 2) 프롬프트 구성
    prompt = f"""
당신은 비트코인 선물 자동매매용 GPT 트레이더입니다.

아래 JSON 은 자동매매 엔진이 계산한 현재 시장 정보입니다.
- 최근 캔들/호가 데이터
- 기본 진입 시그널과 방향
- 리스크/TP/SL 기본값 및 가드 결과
- 저변동성 여부, 세션 정보 등 (market_features 포함)

이 정보를 바탕으로, '지금 이 시점에 새로운 포지션을 여는 것이 합리적인지' 판단하고,
가능하다면 방향/리스크/TP/SL 을 제안하세요.

입력 JSON:
{context_json}

출력은 아래 스키마를 따르는 **순수 JSON 문자열**만 출력해야 합니다.

스키마:
{{
  "take_trade": true | false,
  "side": "BUY" | "SELL" | null,
  "effective_risk_pct": 0.0,
  "tp_pct": 0.0,
  "sl_pct": 0.0,
  "score": 0.0,
  "reason": "설명 문자열"
}}

규칙:
- take_trade 가 false 인 경우 side 는 null 이거나 "BUY"/"SELL" 이더라도 실제로는 진입하지 않는다.
- 모든 비율(effective_risk_pct, tp_pct, sl_pct)은 0.0 ~ 0.5 사이 소수(예: 0.01 = 1%)로 작성한다.
- JSON 바깥에 다른 텍스트를 절대 출력하지 말라.
"""

    ok, text, latency, err = _call_openai_trader(prompt)

    if not ok or not text:
        reason = f"OpenAI 호출 실패: {err}" if err else "OpenAI 응답 없음"
        send_tg(f"⚠️ GPT 진입 판단 호출 실패: {reason}")
        return {
            "status": "ERROR",
            "raw": text,
            "latency": latency,
            "take_trade": False,
            "side": None,
            "effective_risk_pct": 0.0,
            "tp_pct": 0.0,
            "sl_pct": 0.0,
            "score": 0.0,
            "reason": reason,
        }

    # 3) JSON 파싱
    try:
        parsed = json.loads(text)
        if not isinstance(parsed, dict):
            raise ValueError("JSON 최상위가 객체(dict)가 아닙니다.")
    except Exception as e:
        log(f"[GPT_TRADER] 응답 JSON 파싱 실패: {e} raw={text!r}")
        send_tg(f"⚠️ GPT 응답 파싱 실패: {e}")
        return {
            "status": "PARSE_ERROR",
            "raw": text,
            "latency": latency,
            "take_trade": False,
            "side": None,
            "effective_risk_pct": 0.0,
            "tp_pct": 0.0,
            "sl_pct": 0.0,
            "score": 0.0,
            "reason": f"응답 JSON 파싱 실패: {e}",
        }

    # 4) 필드 추출 및 기본값 보정
    def _as_float(v: Any, default: float) -> float:
        try:
            return float(v)
        except (TypeError, ValueError):
            return default

    take_trade = bool(parsed.get("take_trade", False))
    side = parsed.get("side")
    if side not in ("BUY", "SELL"):
        side = None

    eff_risk = _as_float(parsed.get("effective_risk_pct"), 0.0)
    tp_pct = _as_float(parsed.get("tp_pct"), 0.0)
    sl_pct = _as_float(parsed.get("sl_pct"), 0.0)
    score = _as_float(parsed.get("score"), 0.0)
    reason_text = str(parsed.get("reason", "") or "")

    # 5) settings 에 따른 상한/하한 클램프 (있으면)
    settings = kwargs.get("settings")
    if settings is not None:
        try:
            max_risk = getattr(settings, "gpt_max_risk_pct", getattr(settings, "risk_pct", 0.03))
            max_tp = getattr(settings, "gpt_max_tp_pct", getattr(settings, "tp_pct", 0.10))
            min_tp = getattr(settings, "gpt_min_tp_pct", 0.0)
            max_sl = getattr(settings, "gpt_max_sl_pct", getattr(settings, "sl_pct", 0.02))
            min_sl = getattr(settings, "gpt_min_sl_pct", 0.0)

            if max_risk > 0:
                eff_risk = max(0.0, min(eff_risk, max_risk))
            if max_tp > 0:
                tp_pct = max(min_tp, min(tp_pct, max_tp))
            if max_sl > 0:
                sl_pct = max(min_sl, min(sl_pct, max_sl))
        except Exception as e:
            log(f"[GPT_TRADER] settings 기반 클램프 중 오류: {e}")

    return {
        "status": "OK",
        "raw": text,
        "latency": latency,
        "take_trade": take_trade,
        "side": side,
        "effective_risk_pct": eff_risk,
        "tp_pct": tp_pct,
        "sl_pct": sl_pct,
        "score": score,
        "reason": reason_text,
    }


__all__ = ["ask_entry_decision"]
