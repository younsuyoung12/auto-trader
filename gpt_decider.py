# gpt_decider.py
# ====================================================
# 역할
# ----------------------------------------------------
# - BingX Auto Trader에서 진입/청산/전략 중재 의사결정을 GPT-5.1에 위임하는 어댑터.
# - entry_flow.py, signal_flow_ws.py, position_watch_ws.py 등에서 이 모듈만 import 해서 사용.
#
# 2025-11-17 변경 사항 (타입힌트 정리 + GPT 응답 파싱 보강 + Pylance 문법 오류 대응 + 구버전 openai 호환 + 로깅 강화)
# ----------------------------------------------------
# 1) Pylance reportInvalidTypeForm 대응
#    - Dict[str, Any] | None → Optional[Dict[str, Any]] 로 전부 변경.
#    - 그 외 Union 연산자(|) 사용을 모두 typing.Optional/typing.Union 기반으로 정리.
# 2) GPT 응답 텍스트 추출 안정화
#    - _extract_text_from_response(resp: Any) 헬퍼 추가.
#    - resp.output_text, resp.output[0].content[0].text / .text.value 등을 순서대로 시도.
#    - 어떤 포맷이 와도 최대한 JSON 문자열을 뽑아내고, 실패 시 str(resp) 로 폴백.
# 3) OpenAI 클라이언트 생성시 OPENAI_API_BASE 지원
#    - OPENAI_API_BASE 가 설정되어 있으면 base_url 로 사용.
# 4) GPT 오류 시 텔레그램 알림 문구 한글로 직관적으로 정리
#    - 진입/청산 판단 실패 시: "기존 규칙으로만 판단" / "포지션 정리/유지" 식으로 안내.
# 5) Pylance가 한글 docstring 을 코드로 잘못 인식하는 문제 해결
#    - _extract_text_from_response 의 한글 설명을 주석(#) 기반으로 변경.
# 6) openai-python 구버전 호환
#    - Responses.create(...) 가 response_format 인자를 지원하지 않는 경우, TypeError 를 캐치하여
#      response_format 없이 재호출하는 폴백 로직 추가.
# 7) GPT 호출 로깅 강화
#    - _safe_log(...) 헬퍼를 추가하여 GPT 진입/청산/중재 호출 및 결과를 Render 로그에서 확인 가능.

from __future__ import annotations

import os
import json
import time
from typing import Any, Dict, Optional, Tuple

from openai import OpenAI

_client: Optional[OpenAI] = None


# ─────────────────────────────────────────
# 공통 유틸 헬퍼
# ─────────────────────────────────────────


def _safe_log(msg: str) -> None:
    """telelog.log 를 사용할 수 있으면 사용하고, 실패해도 조용히 무시한다."""
    try:
        from telelog import log  # 지연 import 로 순환 의존 방지

        log(msg)
    except Exception:
        # 로깅 실패로 인해 트레이딩 로직이 죽지 않도록 무시
        pass


# ─────────────────────────────────────────
# 공통 GPT 호출 헬퍼
# ─────────────────────────────────────────


def _get_client() -> OpenAI:
    """전역 OpenAI 클라이언트 인스턴스 생성/재사용."""
    global _client
    if _client is None:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY 환경변수가 없습니다.")

        base_url = os.getenv("OPENAI_API_BASE")
        if base_url:
            _client = OpenAI(api_key=api_key, base_url=base_url)
        else:
            _client = OpenAI(api_key=api_key)
    return _client


def _extract_text_from_response(resp: Any) -> str:
    """OpenAI Responses API 응답 객체에서 텍스트를 최대한 안정적으로 추출."""
    # 1) output_text 필드가 있으면 우선 사용
    text_attr = getattr(resp, "output_text", None)
    if isinstance(text_attr, str) and text_attr.strip():
        return text_attr

    # 2) output[0].content[0].text(.value) 계열 시도
    try:
        output = getattr(resp, "output", None)
        if output:
            block = output[0]
            content = getattr(block, "content", None)
            if content:
                first = content[0]

                # 새 포맷: first.text.value
                txt_obj = getattr(first, "text", None)
                if isinstance(txt_obj, str):
                    # 구 포맷: text 가 바로 문자열
                    return txt_obj
                if txt_obj is not None:
                    val = getattr(txt_obj, "value", None)
                    if isinstance(val, str):
                        return val

                # 혹시 text 필드가 바로 문자열인 구버전 포맷
                direct = getattr(first, "text", None)
                if isinstance(direct, str):
                    return direct
    except Exception:
        # 응답 포맷이 예상과 다르더라도 여기서는 조용히 폴백으로 넘긴다.
        pass

    # 3) 마지막 폴백
    return str(resp)


def _call_gpt_json(
    model: str,
    prompt: str,
    *,
    timeout_sec: float = 8.0,
    purpose: Optional[str] = None,
) -> Dict[str, Any]:
    """GPT-5.1 을 호출해서 **반드시 JSON**만 받는 헬퍼.

    - 실패/타임아웃/JSON 파싱 에러 시 예외를 던진다.
      → safe 래퍼 층에서 잡고 폴백 처리.
    - purpose 는 호출 용도 구분용 태그(현재는 로깅·라우팅용 확장 포인트).
    """
    client = _get_client()

    t0 = time.time()
    _safe_log(f"[GPT_CALL] start purpose={purpose or '-'} model={model}")

    try:
        try:
            # 최신 openai-python (Responses API + response_format 지원)
            resp = client.responses.create(
                model=model,
                input=prompt,
                response_format={"type": "json_object"},
                timeout=timeout_sec,
            )
        except TypeError as e:
            # 구버전 openai-python: response_format 인자 미지원
            if "response_format" not in str(e):
                raise
            _safe_log("[GPT_CALL] response_format not supported, retrying without it")
            resp = client.responses.create(
                model=model,
                input=prompt,
                timeout=timeout_sec,
            )

        text = _extract_text_from_response(resp)
        elapsed = time.time() - t0
        _safe_log(
            f"[GPT_CALL] done purpose={purpose or '-'} elapsed={elapsed:.2f}s text_len={len(text)}"
        )
    except Exception as e:  # pragma: no cover - GPT 오류는 호출측에서 처리
        _safe_log(f"[GPT_CALL][ERROR] purpose={purpose or '-'} error={e}")
        raise

    if elapsed > timeout_sec:
        raise TimeoutError(f"GPT 응답 지연: {elapsed:.2f}s > {timeout_sec:.2f}s")

    try:
        data: Dict[str, Any] = json.loads(text)
    except json.JSONDecodeError as e:
        raise ValueError(f"GPT JSON 파싱 실패: {e} / raw={text[:200]!r}")

    return data


# ─────────────────────────────────────────
# 1) 진입 의사결정
# ─────────────────────────────────────────


def ask_entry_decision(
    *,
    symbol: str,
    signal_source: str,  # "TREND" / "RANGE" / "HYBRID"
    chosen_signal: str,  # "LONG" / "SHORT"
    last_price: float,
    entry_score: Optional[float],
    effective_risk_pct: float,
    leverage: float,
    tp_pct: float,
    sl_pct: float,
    extra: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """GPT-5.1 에게 '이 진입을 할지 말지, 비율을 수정할지' 물어본다.

    반환 예시(JSON):
    {
      "action": "ENTER",          # ENTER / SKIP / ADJUST
      "reason": "짧은 요약 한줄",
      "tp_pct": 0.02,             # 선택 (없으면 기존 값 사용)
      "sl_pct": 0.01,             # 선택
      "effective_risk_pct": 0.01  # 선택
    }
    """
    model = os.getenv("GPT_ENTRY_MODEL", "gpt-5.1")

    # extra 는 너무 길 수 있으니 핵심만 잘라서 보낸다.
    trimmed_extra: Dict[str, Any] = {}
    if isinstance(extra, dict):
        for k in [
            "signal_score",
            "candidate_score",
            "atr_fast",
            "atr_slow",
            "regime_level",
        ]:
            if k in extra:
                trimmed_extra[k] = extra[k]

    prompt = f"""
당신은 BTC-USDT 선물 자동매매 봇의 진입 리스크 컨트롤러입니다.
아래 정보를 보고 이 진입을 허용할지 결정하세요.

- 심볼: {symbol}
- 시그널 소스: {signal_source}   (TREND=추세, RANGE=박스, HYBRID=혼합)
- 방향: {chosen_signal}          (LONG=매수, SHORT=매도)
- 현재가: {last_price}
- EntryScore(0~100): {entry_score}
- 레버리지: {leverage}
- 현재 설정 리스크 비율: {effective_risk_pct}
- 현재 TP%: {tp_pct}, SL%: {sl_pct}
- 추가 지표(extra 요약): {json.dumps(trimmed_extra, ensure_ascii=False)}

규칙:
1) 답변은 반드시 JSON 하나만 출력하세요. 자연어 설명 금지.
2) action 값:
   - "ENTER"  : 그대로 진입
   - "SKIP"   : 이번 진입은 건너뛰기
   - "ADJUST" : 진입은 하되 TP/SL/리스크를 조정
3) 수치를 수정하고 싶을 때만 해당 필드를 포함합니다.
   - tp_pct / sl_pct / effective_risk_pct 는 0보다 커야 합니다.
   - 리스크 비율은 0.001 ~ 0.03 (0.1% ~ 3%) 범위로 맞추세요.

JSON 스키마 예시는 다음과 같습니다 (그대로 쓰지 말고 값만 채워서 보내세요):

{{
  "action": "ENTER",
  "reason": "여기에 한국어 한 줄 요약",
  "tp_pct": 0.02,
  "sl_pct": 0.01,
  "effective_risk_pct": 0.01
}}
    """.strip()

    return _call_gpt_json(model=model, prompt=prompt, purpose="entry_decision")


def ask_entry_decision_safe(
    *,
    symbol: str,
    signal_source: str,
    chosen_signal: str,
    last_price: float,
    entry_score: Optional[float],
    effective_risk_pct: float,
    leverage: float,
    tp_pct: float,
    sl_pct: float,
    extra: Optional[Dict[str, Any]],
) -> Tuple[str, Dict[str, Any]]:
    """진입 의사결정용 안전 래퍼.

    - GPT 호출에 실패하면 ("ENTER", {}) 를 돌려서
      → 기존 Python 로직만 사용하도록 한다.
    - 정상 응답이면 (action, result_json) 반환.
    """
    try:
        data = ask_entry_decision(
            symbol=symbol,
            signal_source=signal_source,
            chosen_signal=chosen_signal,
            last_price=last_price,
            entry_score=entry_score,
            effective_risk_pct=effective_risk_pct,
            leverage=leverage,
            tp_pct=tp_pct,
            sl_pct=sl_pct,
            extra=extra,
        )
        action = str(data.get("action", "ENTER")).upper()
        if action not in {"ENTER", "SKIP", "ADJUST"}:
            action = "ENTER"

        # 성공 시 Render 로그에 요약 남김
        try:
            new_tp = data.get("tp_pct", tp_pct)
            new_sl = data.get("sl_pct", sl_pct)
            new_risk = data.get("effective_risk_pct", effective_risk_pct)
            _safe_log(
                f"[GPT_ENTRY] action={action} tp={new_tp} sl={new_sl} risk={new_risk} symbol={symbol} src={signal_source} dir={chosen_signal}"
            )
        except Exception:
            pass

        return action, data
    except Exception as e:  # pragma: no cover - GPT 오류 폴백
        from telelog import log, send_tg  # 순환 import 방지용 지연 import

        log(f"[GPT_ENTRY] 호출 실패, 기존 로직 사용: {e}")
        try:
            send_tg("⚠️ GPT 진입 판단 호출에 실패했습니다. 이번 진입은 기존 규칙으로만 판단합니다.")
        except Exception:
            pass
        return "ENTER", {}


# ─────────────────────────────────────────
# 2) 청산(포지션 종료) 의사결정
# ─────────────────────────────────────────


def _make_exit_prompt(payload: Dict[str, Any]) -> str:
    """EXIT(청산) 전용 프롬프트를 구성한다.

    payload 예시:
    {
      "symbol": "BTC-USDT",
      "regime": "TREND" 또는 "RANGE",
      "side": "BUY"/"SELL"/"LONG"/"SHORT",
      "scenario": "RANGE_EARLY_TP" 등,
      "last_price": 95000.0,
      "entry_price": 94000.0,
      "leverage": 10,
      "pnl_pct": 0.0105,
      "extra": { ... }  # 캔들 스냅샷, 임계값, 볼륨 정보 등
    }
    """

    return (
        "당신은 BTC-USDT 선물 자동매매 봇의 '포지션 종료 리스크 컨트롤러'입니다.\n"
        "아래 JSON 상태를 보고, 지금 포지션을 청산할지(HARD CLOSE) 보유할지(HOLD) 결정하세요.\n\n"
        "규칙:\n"
        "1) 반드시 JSON 하나만 출력합니다.\n"
        "2) 필드:\n"
        "   - action: 'CLOSE' 또는 'HOLD' 혹은 'KEEP' 중 하나만 사용.\n"
        "   - reason: 한국어 한 줄 요약 (예: '박스 조기익절, 목표 수익 도달').\n"
        "   - comment: 사람이 읽을 수 있는 짧은 설명.\n"
        "3) 손익이 크지 않거나 잡음 수준이면 HOLD 를 우선 고려합니다.\n"
        "4) 손실 확대 가능성이 크거나, 전략 규칙상 포지션 유지 의미가 약해 보이면 과감하게 CLOSE 를 선택합니다.\n\n"
        "현재 포지션 상태 JSON:\n" + json.dumps(payload, ensure_ascii=False, indent=2) + "\n\n"
        "반드시 아래 예시와 같은 스키마로만 응답하세요(값만 바꿔서 사용):\n\n"
        "{\n"
        "  \"action\": \"CLOSE\",\n"
        "  \"reason\": \"여기에 한국어 한 줄 요약\",\n"
        "  \"comment\": \"사람이 이해하기 쉬운 짧은 설명\"\n"
        "}\n"
    )


def ask_exit_decision(
    *,
    symbol: str,
    side: str,  # "BUY" / "SELL" / "LONG" / "SHORT"
    scenario: str,  # "RANGE_EARLY_TP" / "TREND_EARLY_EXIT" / "OPPOSITE_SIGNAL" 등
    last_price: float,
    entry_price: float,
    leverage: Optional[float],
    extra: Optional[Dict[str, Any]],
    regime: Optional[str] = None,
    source: Optional[str] = None,
) -> Dict[str, Any]:
    """GPT-5.1 에게 '이 포지션을 지금 청산할지 말지'를 묻는다.

    - regime/source: 전략 종류 (TREND/RANGE/HYBRID 등). 둘 중 하나만 넘겨도 됨.
    - side:   포지션 방향
    - scenario: 호출 위치/의도 구분용 태그 (ex: "RANGE_EARLY_TP")
    - extra:  캔들/거래량/임계값/시그널 등 추가 컨텍스트
    """
    model = os.getenv("GPT_EXIT_MODEL", "gpt-5.1")

    # side 정규화
    s = side.upper()
    if s == "BUY":
        norm_side = "LONG"
    elif s == "SELL":
        norm_side = "SHORT"
    else:
        norm_side = s

    # regime/source 정규화 (둘 중 하나만 넘어와도 됨)
    regime_val = (source or regime or "").upper() if (source or regime) else ""

    # PnL %
    pnl_pct: Optional[float]
    try:
        if entry_price > 0 and last_price > 0:
            raw = (last_price - entry_price) / entry_price
            if norm_side == "SHORT":
                raw = -raw
            pnl_pct = raw
        else:
            pnl_pct = None
    except Exception:
        pnl_pct = None

    payload: Dict[str, Any] = {
        "symbol": symbol,
        "regime": regime_val,
        "side": norm_side,
        "scenario": scenario,
        "entry_price": entry_price,
        "last_price": last_price,
        "leverage": leverage,
        "pnl_pct": pnl_pct,
        "extra": extra or {},
    }

    prompt = _make_exit_prompt(payload)
    return _call_gpt_json(model=model, prompt=prompt, purpose="exit_decision")


def ask_exit_decision_safe(
    *,
    symbol: str,
    side: str,
    scenario: str,
    last_price: float,
    entry_price: float,
    leverage: Optional[float],
    extra: Optional[Dict[str, Any]],
    fallback_action: str = "CLOSE",
    regime: Optional[str] = None,
    source: Optional[str] = None,
) -> Tuple[str, Dict[str, Any]]:
    """청산(포지션 종료) 의사결정용 안전 래퍼.

    - 정상 응답: (action, json) 을 돌려준다.
      * action 은 "CLOSE" 또는 "HOLD" 로 정규화된다.
      * GPT 가 "KEEP" 을 보내면 내부적으로 "HOLD" 로 취급.
    - 비정상 상황(GPT 오류/타임아웃/JSON 형식 오류 등):
      * (fallback_action, {"error": "..."}) 를 반환.
      * fallback_action 기본값은 "CLOSE" 이지만, 호출측에서 "HOLD" 로 바꿀 수 있다.
    - regime/source 두 인자 중 하나만 넘겨도 된다.
    """
    # fallback_action 정규화
    fa = str(fallback_action).upper()
    if fa not in {"CLOSE", "HOLD"}:
        fa = "CLOSE"

    try:
        data = ask_exit_decision(
            symbol=symbol,
            side=side,
            scenario=scenario,
            last_price=last_price,
            entry_price=entry_price,
            leverage=leverage,
            extra=extra,
            regime=regime,
            source=source,
        )
        raw_action = str(data.get("action", fa)).upper()

        if raw_action in {"HOLD", "KEEP"}:
            action = "HOLD"
        elif raw_action == "CLOSE":
            action = "CLOSE"
        else:
            action = fa

        # 성공 시 Render 로그에 요약 남김
        try:
            _safe_log(
                f"[GPT_EXIT] scenario={scenario} action={action} symbol={symbol} side={side} reason={data.get('reason')}"
            )
        except Exception:
            pass

        return action, data
    except Exception as e:  # pragma: no cover - GPT 오류 폴백
        from telelog import log, send_tg  # 순환 import 방지용 지연 import

        msg = f"[GPT_EXIT] 호출 실패, fallback={fa}: {e}"
        log(msg)
        try:
            if fa == "CLOSE":
                human = "포지션을 바로 정리합니다."
            else:
                human = "포지션을 그대로 유지합니다."
            send_tg(f"⚠️ GPT 청산 판단 호출에 실패했습니다. {human}")
        except Exception:
            pass

        return fa, {"error": str(e), "fallback_action": fa}


# ─────────────────────────────────────────
# 3) TREND/RANGE 전략 중재 의사결정 (보조 레이어)
# ─────────────────────────────────────────


def ask_signal_arbitration(
    *,
    symbol: str,
    last_price: float,
    trend_candidate: Optional[Dict[str, Any]],
    range_candidate: Optional[Dict[str, Any]],
    model: Optional[str] = None,
) -> Dict[str, Any]:
    """TREND/RANGE 후보 둘 다 애매해서 _arbitrate(...) 결과가 None 인 경우,
    GPT-5에게 한 번 더 물어보는 보조 중재기.

    입력:
        - symbol: 예) "BTC-USDT"
        - last_price: 현재가
        - trend_candidate: {"kind","side","score","tp_pct","sl_pct","reasons","block_reason"} 또는 None
        - range_candidate: 동일 구조 또는 None

    반환:
        {
          "action": "ENTER_TREND" | "ENTER_RANGE" | "SKIP",
          "reason": "자연어 설명",
          "raw":   GPT 전체 응답(JSON Object)
        }

    ⚠ 실패 / 이상 응답 시 ValueError 를 발생시킨다.
      호출부(signal_flow_ws.get_trading_signal)에서 잡아서 no-entry 처리한다.
    """
    from telelog import log  # 로깅만 사용 (순환 import 방지용 지연 import)

    if trend_candidate is None and range_candidate is None:
        raise ValueError("ask_signal_arbitration: no candidates")

    use_model = model or os.getenv("GPT_ENTRY_MODEL", "gpt-5.1")

    # 프롬프트에 넣을 간단한 구조만 남긴다(안전/길이 제한용).
    def _trim_cand(c: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not c:
            return {}
        return {
            "kind": c.get("kind"),
            "side": c.get("side"),
            "score": c.get("score"),
            "tp_pct": c.get("tp_pct"),
            "sl_pct": c.get("sl_pct"),
            "block_reason": c.get("block_reason"),
            "reasons": c.get("reasons"),
        }

    trend_json = _trim_cand(trend_candidate)
    range_json = _trim_cand(range_candidate)

    payload = {
        "symbol": symbol,
        "last_price": float(last_price),
        "trend_candidate": trend_json or None,
        "range_candidate": range_json or None,
    }
    payload_str = json.dumps(payload, ensure_ascii=False)

    prompt = f"""
당신은 BTC-USDT 선물 자동매매 봇의 전략 중재 컨트롤러입니다.
현재 루프에서 TREND / RANGE 전략 후보가 동시에 존재하지만,
내장 중재 규칙으로는 어느 한쪽도 명확히 선택하지 못한 상태입니다.

입력 JSON:
{payload_str}

각 후보 필드 의미:
- kind: "TREND" 또는 "RANGE"
- side: "BUY" / "SELL" (진입 방향)
- score: 전략별 내부 점수 (값이 클수록 해당 전략이 유리)
- tp_pct / sl_pct: 해당 전략이 제안하는 TP / SL 비율 (예: 0.006 → +0.6%)
- block_reason:
    - RANGE 에서 soft_... 로 시작하면 "조건이 나쁘긴 한데 완전 차단은 아님" 의미
- reasons: 사람이 디버깅할 때 보는 간단한 설명 리스트

판단 규칙(요약):
1) 두 후보 모두 위험해 보이거나 정보가 부족하면 "SKIP" 을 선택한다.
2) 한쪽만 존재하면 그 후보를 기준으로 ENTER 여부를 검토한다.
3) 양쪽 모두 존재하면:
   - 리스크가 과도해 보이는 쪽은 피한다.
   - score / TP·SL / block_reason / reasons 를 종합해서
     "조금이라도 더 합리적인" 쪽을 선택한다.
4) 불확실할 때는 보수적으로 "SKIP" 을 선택한다.

반드시 아래 JSON 하나만 출력하라:

{{
  "action": "ENTER_TREND" 또는 "ENTER_RANGE" 또는 "SKIP",
  "reason": "사람이 이해하기 쉬운 한국어 한 줄 설명"
}}
""".strip()

    resp_json = _call_gpt_json(
        model=use_model,
        prompt=prompt,
        purpose="signal_arbitration",
    )

    action_raw = resp_json.get("action", "")
    action = str(action_raw).upper().strip()

    if action not in {"ENTER_TREND", "ENTER_RANGE", "SKIP"}:
        log(f"[GPT_ARB] invalid action from GPT: {action_raw!r}")
        raise ValueError(f"invalid action from GPT arbitration: {action_raw!r}")

    result: Dict[str, Any] = {
        "action": action,
        "reason": resp_json.get("reason"),
        "raw": resp_json,
    }

    # Render 로그에 요약 남김
    try:
        _safe_log(
            f"[GPT_ARB] action={action} symbol={symbol} last_price={last_price} reason={resp_json.get('reason')}"
        )
    except Exception:
        pass

    return result


__all__ = [
    "ask_entry_decision",
    "ask_entry_decision_safe",
    "ask_exit_decision",
    "ask_exit_decision_safe",
    "ask_signal_arbitration",
]
