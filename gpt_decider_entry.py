from __future__ import annotations

import csv
import datetime as dt
import json
import math
import os
import time
from threading import Lock
from typing import Any, Dict, Literal, Optional, Tuple

from openai import OpenAI

try:
    import telelog  # type: ignore
except Exception:  # pragma: no cover
    telelog = None  # type: ignore


"""
2025-12-02 ENTRY/EXIT 안정화 최종본 + GPT-4o-mini 최적화 + gpt_trader/position_watch_ws 정합
============================================================================

이 파일은 진입(ENTRY) / 청산(EXIT)의 GPT 판단 레이어를 담당한다.

이번 최종본에서 정리/수정된 핵심 사항
-----------------------------------
1) ENTRY 응답 스키마를 gpt_trader.decide_entry_with_gpt_trader 와 완전히 정합
   - GPT 응답 필드 (ENTRY):
       action: "ENTER" | "SKIP" | "ADJUST"
       direction: "LONG" | "SHORT" | "PASS"
       tp_pct: float (take-profit 비율, 예: 0.01 = 1%)
       sl_pct: float (stop-loss 비율)
       effective_risk_pct: float (계좌 기준 위험 비율)
       guard_adjustments: dict (옵션, 없으면 {} )
       confidence: float (0~1)
       reason: str (한국어, 왜 ENTER/SKIP/ADJUST 했는지 1~2문장)
       note: str (추가 설명)
       raw_response: str (원문 텍스트)
   - gpt_trader 는 위 JSON 에서 특히
       action / tp_pct / sl_pct / effective_risk_pct / guard_adjustments / reason /
       _meta.latency_sec 를 사용한다.

2) GPT-4o-mini 기준 ENTRY 안정화
   - response_format={"type": "json_object"} 고정
   - temperature=0.0 (결정적·재현 가능한 판단)
   - 프롬프트를 JSON 스키마/제약/예시 위주로 단순·명확하게 재작성
   - NaN/Infinity/None 을 GPT에 넘기지 않도록 _sanitize_for_gpt 로 사전 정리

3) ENTRY 응답 정규화/검증 로직 강화
   - 잘못된 action/direction 은 WARNING 로그 후 안전한 기본값으로 강제
   - tp_pct/sl_pct/effective_risk_pct 는 절대 범위 + base_* 대비 비율을 검증,
     범위를 벗어나면 예외 대신 클램핑 + WARNING 로그만 남김
   - tv_pct 는 tp_pct 의 alias 로만 유지 (하위호환용)
   - guard_adjustments 가 dict 가 아니면 빈 dict 로 정규화
   - ENTRY 쪽 fatal validator 제거 (ENTRY 검증 실패로 엔진 전체가 멈추지 않도록 설계)

4) ENTRY 비용 최적화 (로컬 SKIP 레이어)
   - trend_strength / volatility / entry_score 가 약한 구간에서는
     GPT를 호출하지 않고 로컬에서 즉시 SKIP 을 반환한다.
   - 의미 없는 PASS 캔들에 대한 GPT 호출을 대폭 줄인다.

5) EXIT 인터페이스를 position_watch_ws 와 정합
   - ask_exit_decision_safe(...) 시그니처와 반환값을
     position_watch_ws.maybe_exit_with_gpt(...) 에 맞게 수정:
       action, gpt_data = ask_exit_decision_safe(...)
       · action    : "CLOSE" | "HOLD"
       · gpt_data  : GPT 응답 dict 또는 에러 정보 dict
   - GPT EXIT 프롬프트를 단순화:
       action: "CLOSE" | "HOLD"
       close_ratio: 0.0~1.0 (옵션, 기본 1.0; 현재 레이어에서는 전체 청산만 사용)
       new_sl_pct, new_tp_pct: 0.0~0.5 (옵션)
       reason: 한국어 1~2문장 (필수)
       note, raw_response: 선택
   - EXIT 실패 시 fallback_action("HOLD" 또는 "CLOSE") 으로 안전하게 복구.

6) 공통 메타 정보
   - ENTRY / EXIT 모두 내부적으로 레이턴시를 CSV(gpt_latency.csv)에 기록.
   - ENTRY 반환값에는 "_meta" 필드를 포함:
       "_meta": {
           "latency_sec": float,
           "model": str,
           "symbol": str,
           "source": str
       }
"""

# =============================================================================
# 설정 / 상수
# =============================================================================

GPT_MODEL_DEFAULT = os.getenv("OPENAI_TRADER_MODEL", "gpt-4o-mini")

OPENAI_TRADER_MAX_LATENCY = float(os.getenv("OPENAI_TRADER_MAX_LATENCY", "4.0"))
OPENAI_TRADER_MAX_TOKENS = int(os.getenv("OPENAI_TRADER_MAX_TOKENS", "512"))

GPT_LATENCY_CSV = os.getenv("GPT_LATENCY_CSV", "gpt_latency.csv")

TELELOG_CHAT_ID = os.getenv("TELELOG_CHAT_ID", "")
TELELOG_LEVEL = os.getenv("TELELOG_LEVEL", "INFO").upper()

GPT_MAX_RISK_PCT = float(os.getenv("GPT_MAX_RISK_PCT", "0.03"))  # 3% 기본 한도

gpt_entry_call_count = 0  # ENTRY 호출 횟수 카운터

_gpt_latency_lock = Lock()


# =============================================================================
# 공용 유틸
# =============================================================================


def _safe_log(level: str, msg: str) -> None:
    """
    telelog.log(...) 호출을 감싸서, telelog 미설치/에러가 있어도
    gpt_decider 자체 로직에는 영향을 주지 않도록 한다.
    """
    if telelog is None:
        return

    try:
        level = (level or "INFO").upper()
        if level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            level = "INFO"
        telelog.log(level, msg)  # type: ignore
    except Exception:
        # 로깅 실패는 무시
        pass


def _safe_tg(msg: str) -> None:
    """
    텔레그램 알림을 보낼 수 있으면 보내고, 실패해도 예외로 이어지지 않도록 보호.
    """
    if telelog is None:
        return
    if not TELELOG_CHAT_ID:
        return

    try:
        telelog.tg(msg)  # type: ignore
    except Exception:
        pass


def _fatal_log_and_raise(msg: str, *, exc: Optional[Exception] = None) -> None:
    """
    치명적 오류 상황에서 로그를 남기고 RuntimeError로 감싼 뒤 raise.

    ※ ENTRY 경로에서는 더 이상 사용하지 않는다.
       (ENTRY는 검증 실패로 엔진 전체가 중단되지 않도록 설계)
    """
    full_msg = f"[gpt_decider:FATAL] {msg}"
    if exc is not None:
        full_msg += f" | exc={type(exc).__name__}: {exc}"

    _safe_log("ERROR", full_msg)
    _safe_tg(full_msg)

    raise RuntimeError(full_msg) from exc


def _log_gpt_latency_csv(
    *,
    kind: str,  # "ENTRY" / "EXIT" 등
    model: str,
    symbol: Optional[str],
    source: Optional[str],
    direction: Optional[str],
    latency: float,
    success: bool,
    error_type: str = "",
    error_msg: str = "",
) -> None:
    """
    GPT 호출 레이턴시를 CSV 파일에 append.
    파일 쓰기 충돌을 피하기 위해 Lock 사용.
    """
    row = {
        "timestamp": dt.datetime.utcnow().isoformat(),
        "kind": kind,
        "model": model,
        "symbol": symbol or "",
        "source": source or "",
        "direction": direction or "",
        "latency_sec": f"{latency:.4f}",
        "success": "1" if success else "0",
        "error_type": error_type,
        "error_msg": error_msg[:512],
    }

    try:
        with _gpt_latency_lock:
            file_exists = os.path.exists(GPT_LATENCY_CSV)
            with open(GPT_LATENCY_CSV, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=list(row.keys()))
                if not file_exists:
                    writer.writeheader()
                writer.writerow(row)
    except Exception:
        # 로깅 실패는 전체 로직에 영향을 주지 않게 무시
        pass


def _extract_text_from_response(resp: Any) -> str:
    """
    OpenAI Chat Completions / (구) Responses 스타일 응답 객체에서
    텍스트 부분을 최대한 안전하게 추출한다.

    우선순위:
    1) chat.completions 응답: resp.choices[0].message.content
    2) (구) Responses 스타일: resp.output_text
    3) (구) Responses 스타일: resp.output[0].content[0].text.value
    4) 최후: repr(resp) 일부
    """
    # 0) Chat Completions 스타일 (새 API)
    choices = getattr(resp, "choices", None)
    if choices and isinstance(choices, (list, tuple)) and len(choices) > 0:
        first = choices[0]
        message = getattr(first, "message", None)
        content = None
        if isinstance(message, dict):
            content = message.get("content")
        else:
            content = getattr(message, "content", None)
        if isinstance(content, str) and content.strip():
            return content

    # 1) direct text 속성 (구 Responses API)
    text_attr = getattr(resp, "output_text", None)
    if isinstance(text_attr, str) and text_attr.strip():
        return text_attr

    # 2) output -> content -> text.value 경로 (구 Responses API)
    output = getattr(resp, "output", None)
    if output and isinstance(output, (list, tuple)):
        try:
            block = output[0]
            content_list = getattr(block, "content", None)
            if content_list and isinstance(content_list, (list, tuple)) and content_list:
                first = content_list[0]
                txt_obj = getattr(first, "text", None)
                value = getattr(txt_obj, "value", None)
                if isinstance(value, str) and value.strip():
                    return value
                if isinstance(txt_obj, str) and txt_obj.strip():
                    return txt_obj
        except Exception:
            pass

    # 3) 최후의 수단: repr 일부
    return repr(resp)[:2000]


# =============================================================================
# OpenAI 클라이언트 / 공통 JSON 호출
# =============================================================================


def _get_client() -> OpenAI:
    """
    OpenAI 클라이언트를 생성한다.
    """
    api_base = os.getenv("OPENAI_API_BASE", "").strip() or None
    client = OpenAI(base_url=api_base)
    return client


def _call_gpt_json(
    *,
    model: str,
    prompt: str,
    purpose: str = "EXIT",
    timeout_sec: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Chat Completions API를 사용해 JSON 응답을 기대하는 GPT 호출을 공통 처리.

    - response_format={"type": "json_object"} 사용
    - 레이턴시 측정 및 CSV 로깅
    - 단일 dict로 파싱 실패 시 예외
    """
    if timeout_sec is None:
        timeout_sec = OPENAI_TRADER_MAX_LATENCY

    client = _get_client()

    start = time.monotonic()
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
            response_format={"type": "json_object"},
        )
        latency = time.monotonic() - start

        text = _extract_text_from_response(resp)
        try:
            data = json.loads(text)
        except Exception as e:
            _log_gpt_latency_csv(
                kind=purpose.upper(),
                model=model,
                symbol=None,
                source=None,
                direction=None,
                latency=latency,
                success=False,
                error_type=type(e).__name__,
                error_msg=f"JSON parse error: {e}",
            )
            raise

        _log_gpt_latency_csv(
            kind=purpose.upper(),
            model=model,
            symbol=None,
            source=None,
            direction=None,
            latency=latency,
            success=True,
        )
        if not isinstance(data, dict):
            raise ValueError(f"JSON 응답이 dict가 아님: {type(data).__name__}")
        return data

    except Exception as e:
        latency = time.monotonic() - start
        _log_gpt_latency_csv(
            kind=purpose.upper(),
            model=model,
            symbol=None,
            source=None,
            direction=None,
            latency=latency,
            success=False,
            error_type=type(e).__name__,
            error_msg=str(e),
        )
        raise


# =============================================================================
# ENTRY 쪽 GPT 응답 정규화 / 검증
# =============================================================================


def _normalize_and_validate_entry_response(
    resp: Dict[str, Any],
    *,
    base_tv_pct: float,
    base_sl_pct: float,
    base_risk_pct: float,
    symbol: str,
    source: str,
    gpt_max_risk_pct: float,
) -> Dict[str, Any]:
    """
    ENTRY GPT의 JSON 응답을 정규화하고, 위험/타겟/손절/액션 관련 필드를 검증한다.

    gpt_trader.decide_entry_with_gpt_trader 가 기대하는 스키마에 맞춰
    action / tp_pct / sl_pct / effective_risk_pct / guard_adjustments / reason 을 보정한다.
    """

    # ---- action 정규화 ------------------------------------------------------
    raw_action = str(resp.get("action", "") or "").upper().strip()
    if raw_action == "PASS":
        raw_action = "SKIP"

    if raw_action not in ("ENTER", "SKIP", "ADJUST"):
        _safe_log(
            "WARNING",
            f"[ENTRY_VALIDATE] invalid action={raw_action!r} -> SKIP | "
            f"symbol={symbol} source={source}",
        )
        action = "SKIP"
    else:
        action = raw_action

    # ---- direction 정규화 ---------------------------------------------------
    direction = str(resp.get("direction", "") or "").upper().strip()
    if direction not in ("LONG", "SHORT", "PASS"):
        _safe_log(
            "WARNING",
            f"[ENTRY_VALIDATE] invalid direction={direction!r} -> PASS | "
            f"symbol={symbol} source={source}",
        )
        direction = "PASS"

    def _as_float(key: str, default: float = 0.0) -> float:
        v = resp.get(key, default)
        try:
            if v is None:
                return default
            if isinstance(v, (int, float)):
                f = float(v)
                if math.isnan(f) or math.isinf(f):
                    return default
                return f
            f = float(str(v))
            if math.isnan(f) or math.isinf(f):
                return default
            return f
        except Exception:
            _safe_log(
                "WARNING",
                f"[ENTRY_VALIDATE] {key} parse failed, use default={default} | "
                f"symbol={symbol} source={source} raw={v!r}",
            )
        return default

    def _clamp_float(
        key: str,
        v: float,
        min_val: float,
        max_val: float,
        *,
        allow_zero: bool = False,
    ) -> float:
        """
        숫자 범위를 벗어나더라도 예외를 발생시키지 않고,
        [min_val, max_val] 구간으로 클램핑하며 WARNING 로그만 남긴다.
        """
        if math.isnan(v) or math.isinf(v):
            _safe_log(
                "WARNING",
                f"[ENTRY_VALIDATE] {key} is NaN/Inf -> set to {min_val} | "
                f"symbol={symbol} source={source}",
            )
            return min_val

        if not allow_zero and v == 0.0:
            return min_val

        if v < min_val:
            _safe_log(
                "WARNING",
                f"[ENTRY_VALIDATE] {key} below min ({v} < {min_val}) -> clamp | "
                f"symbol={symbol} source={source}",
            )
            return min_val

        if v > max_val:
            _safe_log(
                "WARNING",
                f"[ENTRY_VALIDATE] {key} above max ({v} > {max_val}) -> clamp | "
                f"symbol={symbol} source={source}",
            )
            return max_val

        return v

    # ---- 수익/손절/리스크 정규화 -------------------------------------------

    # tp_pct: 우선 tp_pct 를 보되, 없으면 tv_pct 를 fallback 으로 사용
    tp_pct = _as_float("tp_pct", base_tv_pct)
    if "tp_pct" not in resp and "tv_pct" in resp:
        tp_pct = _as_float("tv_pct", base_tv_pct)

    sl_pct = _as_float("sl_pct", base_sl_pct)
    effective_risk_pct = _as_float("effective_risk_pct", base_risk_pct)

    if action == "SKIP":
        # SKIP 에서는 숫자들이 크게 의미 없으므로 0 으로 정리
        tp_pct = 0.0
        sl_pct = 0.0
        effective_risk_pct = 0.0
    else:
        # ENTER/ADJUST 인데 GPT 가 0/음수 준 경우 → base 값으로 복구
        if tp_pct <= 0.0 and base_tv_pct > 0.0:
            tp_pct = base_tv_pct
        if sl_pct <= 0.0 and base_sl_pct > 0.0:
            sl_pct = base_sl_pct

        # 절대 범위 클램핑
        tp_pct = _clamp_float("tp_pct", tp_pct, 0.0, 0.5, allow_zero=True)
        sl_pct = _clamp_float("sl_pct", sl_pct, 0.0, 0.5, allow_zero=True)

        # base_* 대비 비율 체크 (경고만)
        if base_tv_pct > 0 and tp_pct > 0:
            ratio_tv = tp_pct / base_tv_pct
            if ratio_tv < 0.02 or ratio_tv > 4.0:
                _safe_log(
                    "WARNING",
                    "[ENTRY_VALIDATE] tp_pct/base_tv_pct ratio out of range 0.02~4.0: "
                    f"{ratio_tv:.4f} (tp={tp_pct}, base={base_tv_pct}) | "
                    f"symbol={symbol} source={source}",
                )

        if base_sl_pct > 0 and sl_pct > 0:
            ratio_sl = sl_pct / base_sl_pct
            if ratio_sl < 0.02 or ratio_sl > 4.0:
                _safe_log(
                    "WARNING",
                    "[ENTRY_VALIDATE] sl_pct/base_sl_pct ratio out of range 0.02~4.0: "
                    f"{ratio_sl:.4f} (sl={sl_pct}, base={base_sl_pct}) | "
                    f"symbol={symbol} source={source}",
                )

        # effective_risk_pct 클램핑 (부동소수점 오차 허용)
        if base_risk_pct > 0:
            tol = max(1e-6, gpt_max_risk_pct * 1e-4)
            max_allowed = gpt_max_risk_pct + tol
            min_allowed = 0.0
            effective_risk_pct = _clamp_float(
                "effective_risk_pct",
                effective_risk_pct,
                min_allowed,
                max_allowed,
                allow_zero=True,
            )
            if effective_risk_pct > gpt_max_risk_pct:
                _safe_log(
                    "WARNING",
                    "[ENTRY_VALIDATE] effective_risk_pct slightly above max, "
                    f"clamp to {gpt_max_risk_pct} | "
                    f"symbol={symbol} source={source} raw={effective_risk_pct}",
                )
                effective_risk_pct = gpt_max_risk_pct
        else:
            effective_risk_pct = 0.0

    # ---- confidence / reason / note / guard_adjustments ---------------------

    confidence = _as_float("confidence", 0.5)
    if confidence < 0.0 or confidence > 1.0:
        _safe_log(
            "WARNING",
            f"[ENTRY_VALIDATE] confidence out of 0~1 range: {confidence} -> clamp | "
            f"symbol={symbol} source={source}",
        )
        confidence = max(0.0, min(1.0, confidence))

    note = str(resp.get("note", "") or "").strip()
    reason = str(resp.get("reason", "") or "").strip()
    raw_response = str(resp.get("raw_response", "") or "").strip()

    if not reason:
        if action == "SKIP":
            reason = "GPT 판단 결과, 이번 캔들은 진입 리스크가 높거나 신뢰도가 낮아 스킵합니다."
        elif action in ("ENTER", "ADJUST"):
            reason = "GPT 판단 결과를 바탕으로 이번 캔들에서는 진입을 허용합니다."

    guard_adjustments = resp.get("guard_adjustments")
    if not isinstance(guard_adjustments, dict):
        guard_adjustments = {}

    normalized = {
        "action": action,
        "direction": direction,
        "confidence": confidence,
        "tp_pct": tp_pct,
        "tv_pct": tp_pct,  # 하위호환용 alias
        "sl_pct": sl_pct,
        "effective_risk_pct": effective_risk_pct,
        "guard_adjustments": guard_adjustments,
        "reason": reason,
        "note": note,
        "raw_response": raw_response,
    }
    return normalized


# =============================================================================
# NaN/Inf/None 정규화 유틸 (GPT 입력 직전용)
# =============================================================================


def _sanitize_for_gpt(
    obj: Any,
    *,
    _depth: int = 0,
    _max_depth: int = 8,
) -> Any:
    """
    GPT 호출 직전에 JSON으로 직렬화할 payload에 들어 있는
    NaN / Infinity / None 값을 비교적 단순하게 정규화한다.
    """
    if _depth > _max_depth:
        return obj

    if isinstance(obj, (int, bool)):
        return obj
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return 0.0
        return obj

    if obj is None:
        return 0

    if isinstance(obj, dict):
        return {
            str(k): _sanitize_for_gpt(v, _depth=_depth + 1, _max_depth=_max_depth)
            for k, v in obj.items()
        }

    if isinstance(obj, (list, tuple)):
        return [
            _sanitize_for_gpt(v, _depth=_depth + 1, _max_depth=_max_depth)
            for v in obj
        ]

    return obj


# =============================================================================
# GPT 프롬프트 정의 (ENTRY)
# =============================================================================


_SYSTEM_PROMPT_ENTRY = """
You are an expert trading decision assistant for intraday crypto futures trading.

너는 단기(인트라데이) 비트코인 선물 자동매매 시스템의 "진입 판단 레이어"를 담당한다.
입력으로는 심볼, 기본 TP/SL/리스크, 장세 요약 피처(market_features), 시그널 점수 등이 주어진다.

반드시 아래 JSON 스키마로만, 하나의 JSON 객체를 출력해야 한다.
코드블록(```json 등)이나 설명 텍스트를 붙이면 안 된다.

[핵심 역할]

1) action 결정
   - "ENTER"  : 지금 캔들에서 신규 진입을 허용
   - "ADJUST" : 진입은 허용하되, TP/SL/리스크/가드 조건을 약간 조정
   - "SKIP"   : 이번 캔들은 진입하지 않고 건너뜀

2) direction 결정
   - "LONG"  : 상승 기대 (롱)
   - "SHORT" : 하락 기대 (숏)
   - "PASS"  : 방향을 강하게 주장하기 어려움 (보조 정보용)

3) 수익/손절/위험 제어
   - tp_pct (take profit, 목표 수익률 비율):
       - 대략 0 < tp_pct <= 0.12 (12%) 정도가 자연스러운 범위
       - "천천히 추세를 탄다" 수준이면 0.02~0.06, 강한 모멘텀이면 최대 0.12 정도까지 허용
       - 0.20(20%) 이상은 비현실적이니 절대 쓰지 않는다.
   - sl_pct (stop loss, 손절 비율):
       - 일반적으로 0 < sl_pct <= 0.06 (6%) 안쪽
       - 변동성이 큰 장이라면 0.03~0.06 사이에서 조정
       - sl_pct가 tp_pct보다 지나치게 크면 비대칭 리스크가 되므로 지양
   - effective_risk_pct (실제 계좌 기준 1회 진입 시 허용 위험 비율):
       - 0 < effective_risk_pct <= {gpt_max_risk_pct} (기본 3%)
       - 단일 포지션에서 계좌의 {gpt_max_risk_pct_pct:.1f}%를 절대 초과하지 말 것
       - 방향성이 애매하거나, 근거가 약하면 0.005~0.01 수준으로 보수적으로 줄인다.

4) guard_adjustments (옵션)
   - 진입 가드(거래량, 스프레드, 점프, depth imbalance 등)를 조정하고 싶을 때만 사용.
   - 사용 가능한 예시 키:
       · "min_entry_volume_ratio"
       · "max_spread_pct"
       · "max_price_jump_pct"
       · "depth_imbalance_min_ratio"
       · "depth_imbalance_min_notional"
   - 과도하게 느슨하게 만들지 말고, 기존 값 주변에서 소폭 조정하는 수준으로만 제안한다.

5) reason / note
   - reason: 이번 캔들에서 왜 ENTER/ADJUST/ SKIP 했는지 **한국어**로 1~2문장 요약.
   - note: 추가 설명, 관찰한 패턴/리스크 등을 한국어로 자유롭게 서술.

[응답 JSON 스키마]

필수 필드 (항상 포함해야 함):
- "action": "ENTER" | "SKIP" | "ADJUST"
- "direction": "LONG" | "SHORT" | "PASS"
- "confidence": 0.0 ~ 1.0 (매매 아이디어에 대한 자신감)
- "tp_pct": 0.0 ~ 0.5 (목표 수익률 비율, 예: 0.03 = 3%)
- "sl_pct": 0.0 ~ 0.5 (손절 비율, 예: 0.01 = 1%)
- "effective_risk_pct": 0.0 ~ {gpt_max_risk_pct} (계좌 대비 위험 비율)
- "guard_adjustments": 객체 (없으면 빈 객체 {} 사용)
- "reason": 문자열 (한국어, 왜 ENTER/ADJUST/ SKIP 했는지 1~2문장)
- "note": 문자열 (추가 설명)
- "raw_response": 문자열 (시스템 내부용, 원문 또는 핵심 요약 등 자유롭게)

제약 조건:
- action 이 "ENTER" 또는 "ADJUST" 인 경우:
    - tp_pct > 0, sl_pct > 0, effective_risk_pct > 0 이어야 한다.
    - effective_risk_pct 는 절대 {gpt_max_risk_pct} 를 넘지 않는다.
- action 이 "SKIP" 인 경우:
    - tp_pct / sl_pct / effective_risk_pct 는 0 또는 매우 작은 값으로 둘 수 있지만,
      실제 진입은 하지 않는다는 점을 reason 에 명확히 설명한다.
- NaN / Infinity / null / "NaN" / "Infinity" / "None" 등은 절대 사용하지 않는다.
- 모든 수치는 실수(float)로 응답한다.

JSON 예시 (형식 참고용, 실제 값/코멘트는 상황에 맞게 변경):

{
  "action": "ENTER",
  "direction": "LONG",
  "confidence": 0.78,
  "tp_pct": 0.032,
  "sl_pct": 0.012,
  "effective_risk_pct": 0.018,
  "guard_adjustments": {
    "max_spread_pct": 0.0009
  },
  "reason": "5분봉 추세와 거래량이 롱 방향으로 정렬되어 있고, 단기 조정 이후 재상승 가능성이 높아 보입니다.",
  "note": "과열 구간은 아니지만 변동성이 살아 있어 손절 폭을 다소 여유 있게 잡았습니다.",
  "raw_response": "요약이나 내부용 메모를 자유롭게 적어도 됩니다."
}
"""


_USER_PROMPT_TEMPLATE_ENTRY = """
[거래 정보]

Symbol: {symbol}
Source: {source}
Current Price: {current_price}

Base Parameters:
- base_tv_pct: {base_tv_pct}
- base_sl_pct: {base_sl_pct}
- base_risk_pct: {base_risk_pct}

Entry Context (JSON 직렬화):
{market_features_json}

[요구사항]

위 정보를 바탕으로, 이번 캔들에서 **신규 진입을 할지 말지**를 판단하고
아래 필드를 모두 포함하는 JSON 객체 한 개만 출력하세요.

- action: "ENTER" | "SKIP" | "ADJUST"
- direction: "LONG" | "SHORT" | "PASS"
- confidence: 0.0 ~ 1.0
- tp_pct: 0.0 ~ 0.5
- sl_pct: 0.0 ~ 0.5
- effective_risk_pct: 0.0 ~ {gpt_max_risk_pct}
- guard_adjustments: 객체 (없으면 빈 객체 {{}} )
- reason: string (한국어, 왜 ENTER/ADJUST/ SKIP 했는지 1~2문장)
- note: string (추가 설명, 한국어)
- raw_response: string

주의:
- action 이 "SKIP" 일 때도 reason 은 반드시 한국어로 자세히 작성합니다.
- NaN / Infinity / null / 문자열 "NaN" / "Infinity" / "None" 등은 절대 사용하지 않습니다.
- 반드시 순수 JSON만, 코드블록 없이 응답합니다.
"""


def _parse_json(text: str) -> Dict[str, Any]:
    """
    GPT가 반환한 텍스트에서 JSON을 파싱.
    코드블록(````json ...) 이 섞여 있어도 최대한 dict 하나를 뽑아낸다.
    """
    text = (text or "").strip()
    if not text:
        raise ValueError("GPT 응답이 비어 있습니다.")

    # 1) 코드블록( ``` / ```json ) 제거
    if text.startswith("```"):
        lines = text.splitlines()
        if lines:
            first = lines[0].strip()
            if first.startswith("```"):
                lines = lines[1:]
        while lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()

    # 2) 전체 파싱 시도
    try:
        data = json.loads(text)
        if isinstance(data, dict):
            return data
        raise ValueError(f"JSON 최상위 타입이 dict가 아님: {type(data).__name__}")
    except Exception:
        pass

    # 3) 중괄호 블록만 추출해서 재시도
    try:
        first = text.index("{")
        last = text.rindex("}")
        snippet = text[first : last + 1]
        data = json.loads(snippet)
        if isinstance(data, dict):
            return data
        raise ValueError(
            f"중괄호 추출 후에도 dict가 아님: {type(data).__name__}, snippet={snippet[:200]}"
        )
    except Exception as e:
        raise ValueError(f"GPT JSON 파싱 실패: {e} | text={text[:500]}") from e


# =============================================================================
# ENTRY GPT 호출 메인 함수
# =============================================================================


def _build_entry_payload(
    *,
    symbol: str,
    source: str,
    current_price: float,
    base_tv_pct: float,
    base_sl_pct: float,
    base_risk_pct: float,
    market_features: Dict[str, Any],
    signal_source: Optional[str] = None,
    chosen_signal: Optional[str] = None,
    last_price: Optional[float] = None,
    entry_score: Optional[float] = None,
    base_effective_risk_pct: Optional[float] = None,
) -> Dict[str, Any]:
    """
    ENTRY 결정에 사용할 payload dict를 구성한다.

    - market_features 에서 의미 있는 키만 추려서 포함한다.
    - signal_source / chosen_signal / entry_score / base_effective_risk_pct 등
      gpt_trader 에서 넘어오는 메타 정보를 entry_meta 로 묶어 전달한다.
    """
    extra = dict(market_features) if market_features else {}
    meaningful_keys = [
        "trend_strength",
        "volatility",
        "volume_score",
        "pattern_summary",
        "regime",
        "time_features",
    ]
    extra_filtered: Dict[str, Any] = {}
    for k in meaningful_keys:
        if k in extra:
            extra_filtered[k] = extra[k]

    payload: Dict[str, Any] = {
        "symbol": symbol,
        "source": source,
        "current_price": current_price,
        "base_tv_pct": base_tv_pct,
        "base_sl_pct": base_sl_pct,
        "base_risk_pct": base_risk_pct,
        "market_features": extra_filtered,
    }

    # ENTRY 메타 정보 (시그널 출처, 방향 힌트, 점수 등)
    entry_meta: Dict[str, Any] = {}
    if signal_source:
        entry_meta["signal_source"] = signal_source
    if chosen_signal:
        entry_meta["signal_direction_hint"] = chosen_signal
    if last_price is not None:
        entry_meta["last_price"] = last_price
    if entry_score is not None:
        entry_meta["entry_score"] = entry_score
    if base_effective_risk_pct is not None:
        entry_meta["base_effective_risk_pct"] = base_effective_risk_pct

    if entry_meta:
        payload["entry_meta"] = entry_meta

    # 중요한 피처 몇 개는 top-level 로도 노출
    important_keys = ["trend_strength", "volatility", "regime"]
    if isinstance(extra_filtered, dict):
        for k, v in extra_filtered.items():
            if k in important_keys:
                payload[k] = v

    return payload


def ask_entry_decision(
    *,
    symbol: str,
    source: str,
    current_price: float,
    base_tv_pct: float,
    base_sl_pct: float,
    base_risk_pct: float,
    market_features: Dict[str, Any],
    model: str = GPT_MODEL_DEFAULT,
    gpt_max_risk_pct: Optional[float] = None,
    signal_source: Optional[str] = None,   # 추가 메타 필드
    chosen_signal: Optional[str] = None,   # 추가 메타 필드
    last_price: Optional[float] = None,    # 추가 메타 필드
    entry_score: Optional[float] = None,   # 추가 메타 필드
    effective_risk_pct: Optional[float] = None,  # 추가 메타 필드 (기본 리스크 힌트)
) -> Dict[str, Any]:
    """
    ENTRY 결정을 위해 GPT에게 질의하고, 응답을 정규화/검증하여 반환한다.

    gpt_trader.decide_entry_with_gpt_trader(...) 가 그대로 사용하는 JSON 을 돌려준다.

    반환 형식(정규화 후):
    {
      "action": "ENTER" | "SKIP" | "ADJUST",
      "direction": "LONG" | "SHORT" | "PASS",
      "confidence": float,
      "tp_pct": float,
      "tv_pct": float,      # tp_pct alias
      "sl_pct": float,
      "effective_risk_pct": float,
      "guard_adjustments": dict,
      "reason": str,
      "note": str,
      "raw_response": str,
      "_meta": {
          "latency_sec": float,
          "model": str,
          "symbol": str,
          "source": str,
      }
    }
    """
    global gpt_entry_call_count

    if gpt_max_risk_pct is None:
        gpt_max_risk_pct = GPT_MAX_RISK_PCT

    payload = _build_entry_payload(
        symbol=symbol,
        source=source,
        current_price=current_price,
        base_tv_pct=base_tv_pct,
        base_sl_pct=base_sl_pct,
        base_risk_pct=base_risk_pct,
        market_features=market_features,
        signal_source=signal_source,
        chosen_signal=chosen_signal,
        last_price=last_price,
        entry_score=entry_score,
        base_effective_risk_pct=effective_risk_pct,
    )

    sanitized_payload = _sanitize_for_gpt(payload)
    market_features_json = json.dumps(
        sanitized_payload, ensure_ascii=False, separators=(",", ":")
    )

    # -------------------------------------------------------------------------
    # [ENTRY 비용 최적화 레이어]
    #  - 장세가 너무 약하거나(entry_score 낮음) 의미 없는 캔들은
    #    GPT를 호출하지 않고 로컬에서 즉시 SKIP 처리한다.
    #  - GPT 호출 전에 이 두 조건을 먼저 검사한다.
    # -------------------------------------------------------------------------
    try:
        # trend_strength / volatility 는 _build_entry_payload 에서
        # top-level 로 추가되었으므로 여기서 바로 참조 가능하다.
        raw_trend = sanitized_payload.get("trend_strength", 0)
        raw_vol = sanitized_payload.get("volatility", 0)

        try:
            trend_strength = float(raw_trend or 0.0)
        except Exception:
            trend_strength = 0.0

        try:
            vol_score = float(raw_vol or 0.0)
        except Exception:
            vol_score = 0.0

        # entry_score 는 entry_meta 에 들어 있음
        entry_meta = sanitized_payload.get("entry_meta") or {}
        raw_es = entry_meta.get("entry_score")
        try:
            es_val: Optional[float] = float(raw_es) if raw_es is not None else None
        except Exception:
            es_val = None

        # 1) 추세/변동성 모두 약한 구간 → 로컬 SKIP
        #    - trend_strength < 0.015 AND volatility < 0.004 인 경우 (완전 박스장만 스킵하도록 임계값 완화)
        if trend_strength < 0.015 and vol_score < 0.004:
            reason = (
                "장세 추세와 변동성이 모두 약해 이번 캔들은 진입 후보에서 제외합니다 "
                "(GPT 호출 생략)."
            )
            local_result = {
                "action": "SKIP",
                "direction": "PASS",
                "confidence": 0.1,
                "tp_pct": 0.0,
                "tv_pct": 0.0,
                "sl_pct": 0.0,
                "effective_risk_pct": 0.0,
                "guard_adjustments": {},
                "reason": reason,
                "note": "local_skip_weak_trend_vol",
                "raw_response": "local_skip_weak_trend_vol",
                "_meta": {
                    "latency_sec": 0.0,
                    "model": model,
                    "symbol": symbol,
                    "source": source,
                },
            }
            _safe_log(
                "INFO",
                "[ENTRY_LOCAL_SKIP] weak trend/vol → SKIP | "
                f"symbol={symbol} source={source} "
                f"trend={trend_strength:.4f} vol={vol_score:.4f} entry_score={es_val}",
            )
            return local_result

        # 2) entry_score 가 너무 낮은 구간 → 로컬 SKIP
        #    - entry_score < 15.0 기준 (0~100 스케일 가정, 이전보다 완화)
        if es_val is not None and es_val < 15.0:
            reason = (
                "entry_score가 충분히 높지 않아 이번 캔들은 진입을 시도하지 않습니다 "
                "(GPT 호출 생략)."
            )
            local_result = {
                "action": "SKIP",
                "direction": "PASS",
                "confidence": 0.1,
                "tp_pct": 0.0,
                "tv_pct": 0.0,
                "sl_pct": 0.0,
                "effective_risk_pct": 0.0,
                "guard_adjustments": {},
                "reason": reason,
                "note": "local_skip_low_entry_score",
                "raw_response": "local_skip_low_entry_score",
                "_meta": {
                    "latency_sec": 0.0,
                    "model": model,
                    "symbol": symbol,
                    "source": source,
                },
            }
            _safe_log(
                "INFO",
                "[ENTRY_LOCAL_SKIP] low entry_score → SKIP | "
                f"symbol={symbol} source={source} "
                f"entry_score={es_val} trend={trend_strength:.4f} vol={vol_score:.4f}",
            )
            return local_result
    except Exception as e:
        # 필터 레이어에서 문제 생겨도 ENTRY 전체가 죽지 않도록 한다.
        _safe_log(
            "ERROR",
            f"[ENTRY_LOCAL_SKIP] filter eval failed → fallback to GPT call "
            f"symbol={symbol} source={source} err={type(e).__name__}: {e}",
        )

    # -------------------------------------------------------------------------
    # 위 두 조건을 통과한 경우에만 GPT 호출을 수행한다.
    # -------------------------------------------------------------------------

    system_prompt = _SYSTEM_PROMPT_ENTRY.format(
        gpt_max_risk_pct=gpt_max_risk_pct,
        gpt_max_risk_pct_pct=gpt_max_risk_pct * 100.0,
    )
    user_prompt = _USER_PROMPT_TEMPLATE_ENTRY.format(
        symbol=symbol,
        source=source,
        current_price=current_price,
        base_tv_pct=base_tv_pct,
        base_sl_pct=base_sl_pct,
        base_risk_pct=base_risk_pct,
        market_features_json=market_features_json,
        gpt_max_risk_pct=gpt_max_risk_pct,
    )

    client = _get_client()

    latency = 0.0
    error_type = ""
    error_msg = ""

    start_ts = time.monotonic()
    try:
        _safe_log(
            "DEBUG",
            f"[ENTRY_CALL] model={model} symbol={symbol} source={source} "
            f"base_tv={base_tv_pct} base_sl={base_sl_pct} base_risk={base_risk_pct}",
        )

        gpt_entry_call_count += 1
        gpt_call_id = gpt_entry_call_count

        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
            max_completion_tokens=OPENAI_TRADER_MAX_TOKENS,
            timeout=OPENAI_TRADER_MAX_LATENCY,
            temperature=0.0,
        )

        latency = time.monotonic() - start_ts

        text = _extract_text_from_response(resp)

        try:
            raw_dict = json.loads(text)
        except Exception:
            raw_dict = _parse_json(text)

        if not isinstance(raw_dict, dict):
            raise ValueError(
                f"ENTRY GPT 응답 JSON 최상위 타입이 dict가 아님: {type(raw_dict).__name__}"
            )

        normalized = _normalize_and_validate_entry_response(
            raw_dict,
            base_tv_pct=base_tv_pct,
            base_sl_pct=base_sl_pct,
            base_risk_pct=base_risk_pct,
            symbol=symbol,
            source=source,
            gpt_max_risk_pct=gpt_max_risk_pct,
        )
        normalized["raw_response"] = text
        normalized["_meta"] = {
            "latency_sec": float(latency),
            "model": model,
            "symbol": symbol,
            "source": source,
        }

        _safe_log(
            "INFO",
            f"[ENTRY_OK] model={model} symbol={symbol} source={source} "
            f"action={normalized['action']} dir={normalized['direction']} "
            f"conf={normalized['confidence']:.3f} "
            f"tp={normalized['tp_pct']:.4f} sl={normalized['sl_pct']:.4f} "
            f"risk={normalized['effective_risk_pct']:.4f} latency={latency:.3f}s "
            f"call_id={gpt_call_id}",
        )

        _log_gpt_latency_csv(
            kind="ENTRY",
            model=model,
            symbol=symbol,
            source=source,
            direction=normalized["direction"],
            latency=latency,
            success=True,
        )

        return normalized

    except Exception as e:
        latency = time.monotonic() - start_ts
        error_type = type(e).__name__
        error_msg = str(e)

        _safe_log(
            "ERROR",
            f"[ENTRY_FAIL] model={model} symbol={symbol} source={source} "
            f"latency={latency:.3f}s err={error_type}: {error_msg}",
        )
        _safe_tg(
            f"[ENTRY_FAIL] model={model} symbol={symbol} source={source} "
            f"latency={latency:.3f}s err={error_type}: {error_msg}"
        )

        _log_gpt_latency_csv(
            kind="ENTRY",
            model=model,
            symbol=symbol,
            source=source,
            direction=None,
            latency=latency,
            success=False,
            error_type=error_type,
            error_msg=error_msg,
        )

        raise RuntimeError(
            f"ENTRY GPT 호출 실패: {error_type}: {error_msg}"
        ) from e


# =============================================================================
# EXIT 쪽 프롬프트 / 호출 (position_watch_ws 용)
# =============================================================================

_EXIT_SYSTEM_PROMPT = """
You are an expert EXIT advisor for intraday crypto futures trading (BTC-USDT).

역할:
- 이미 진입한 단일 포지션에 대해 지금 청산(CLOSE)할지, 유지(HOLD)할지 결정을 돕는다.
- Python 레이어(position_watch_ws.py)와 연동되며, 아래 JSON 스키마를 반드시 지켜야 한다.

[출력 JSON 스키마]

필수 필드:
- action: "CLOSE" | "HOLD"
- reason: 한국어 문자열 1~2문장 (EXIT/HOLD 결정 이유)

선택 필드:
- close_ratio: 0.0 ~ 1.0   (부분 청산 비율, 기본값은 1.0; 현재 레이어에서는 전체 청산 위주)
- new_sl_pct: 0.0 ~ 0.5    (손절 재조정, 사용하지 않으면 0.0)
- new_tp_pct: 0.0 ~ 0.5    (익절 재조정, 사용하지 않으면 0.0)
- note: string              (추가 설명)
- raw_response: string      (모델 내부 메모/요약 등 자유 형식)

규칙:
- action 이 "CLOSE" 일 때:
    - close_ratio 를 주지 않으면 1.0 으로 간주 (전체 청산).
- action 이 "HOLD" 일 때:
    - close_ratio 는 0.0 으로 간주 (청산 없음).
- 데이터가 모호할 경우, 과도한 추격/손절을 피하기 위해 보수적으로 HOLD 쪽에 기울되,
  명확한 반전 신호나 리스크 이벤트가 있으면 과감히 CLOSE 를 제안한다.
- NaN / Infinity / null / "NaN" / "Infinity" / "None" 등은 절대 사용하지 않는다.
- 반드시 JSON 한 개만 출력한다.
"""

_EXIT_USER_PROMPT_TEMPLATE = """
[포지션 및 시장 컨텍스트(JSON)]

아래 JSON 은 현재 포지션, 시장 상태, 레짐/지표 요약을 포함한다.

{context_json}

위 정보를 바탕으로, 시스템 프롬프트에서 정의한 EXIT 출력 스키마에 맞춰
단일 JSON 객체만 응답해 주세요.

주의:
- JSON 이외의 텍스트(마크다운, 설명 문장)는 절대 포함하지 마세요.
- action, reason 필드는 반드시 포함해야 합니다.
"""


def _make_exit_prompt(payload: Dict[str, Any]) -> str:
    """
    EXIT 결정용 user 프롬프트 문자열을 만든다.
    payload 는 NaN/Inf/None 이 제거된 상태여야 한다.
    """
    ctx_json = json.dumps(
        _sanitize_for_gpt(payload),
        ensure_ascii=False,
        separators=(",", ":"),
    )
    prompt = _EXIT_USER_PROMPT_TEMPLATE.format(context_json=ctx_json)
    return prompt


def ask_exit_decision(
    *,
    symbol: str,
    source: Optional[str],
    side: str,
    scenario: str,
    last_price: float,
    entry_price: float,
    leverage: float,
    extra: Dict[str, Any],
    model: str = GPT_MODEL_DEFAULT,
) -> Dict[str, Any]:
    """
    EXIT 결정을 위해 GPT에게 질의하고, 응답을 dict로 반환한다.

    position_watch_ws.maybe_exit_with_gpt(...) 에서 사용하는 컨텍스트에 맞춰 설계되었다.

    - symbol, source, side, scenario, entry_price, last_price, leverage 는
      top-level 로 다시 한 번 명시해 주고,
    - extra 에는 position_watch_ws._build_exit_context(...) 에서 만든 gpt_ctx 를 그대로 넣는다.

    반환 예시:
    {
      "action": "CLOSE" | "HOLD",
      "close_ratio": 1.0,
      "new_sl_pct": 0.0,
      "new_tp_pct": 0.0,
      "reason": "...",
      "note": "...",
      "raw_response": "...",
      "_meta": {...}
    }
    """
    now = dt.datetime.utcnow().isoformat()

    ctx: Dict[str, Any] = {}
    if isinstance(extra, dict):
        ctx.update(extra)

    ctx.update(
        {
            "symbol": symbol,
            "source": source or "",
            "side": side,
            "scenario": scenario,
            "entry_price": float(entry_price),
            "last_price": float(last_price),
            "leverage": float(leverage),
            "now_utc": now,
        }
    )

    prompt = _make_exit_prompt(ctx)
    system = _EXIT_SYSTEM_PROMPT

    start_ts = time.monotonic()
    latency = 0.0

    try:
        _safe_log(
            "DEBUG",
            f"[EXIT_CALL] model={model} symbol={symbol} side={side} "
            f"entry={entry_price} cur={last_price} scenario={scenario}",
        )

        data = _call_gpt_json(
            model=model,
            prompt=system + "\n\n" + prompt,
            purpose="EXIT",
        )
        latency = time.monotonic() - start_ts

        # action 정규화
        raw_action = data.get("action")
        action = str(raw_action or "HOLD").upper().strip()
        # 과거 CLOSE_ALL / CLOSE_PARTIAL 등도 CLOSE 로 수렴시켜서 사용
        if action in ("CLOSE_ALL", "CLOSE_PARTIAL"):
            action = "CLOSE"
        if action not in ("CLOSE", "HOLD"):
            _safe_log(
                "WARNING",
                f"[EXIT_VALIDATE] invalid action={raw_action!r} -> HOLD | "
                f"symbol={symbol} scenario={scenario}",
            )
            action = "HOLD"

        def _flt(
            key: str,
            default: float = 0.0,
            *,
            min_val: float,
            max_val: float,
        ) -> float:
            v = data.get(key, default)
            try:
                if v is None:
                    return default
                v = float(v)
            except Exception:
                return default
            if math.isnan(v) or math.isinf(v):
                return default
            return max(min_val, min(max_val, v))

        close_ratio = _flt("close_ratio", 1.0, min_val=0.0, max_val=1.0)
        new_sl_pct = _flt("new_sl_pct", 0.0, min_val=0.0, max_val=0.5)
        new_tp_pct = _flt("new_tp_pct", 0.0, min_val=0.0, max_val=0.5)

        if action == "CLOSE":
            if close_ratio <= 0.0:
                close_ratio = 1.0
        else:  # HOLD
            close_ratio = 0.0

        reason = str(data.get("reason", "") or "").strip()
        note = str(data.get("note", "") or "").strip()
        raw_response = str(data.get("raw_response", "") or "").strip()

        result: Dict[str, Any] = {
            "action": action,
            "close_ratio": close_ratio,
            "new_sl_pct": new_sl_pct,
            "new_tp_pct": new_tp_pct,
            "reason": reason,
            "note": note,
            "raw_response": raw_response,
            "_meta": {
                "model": model,
                "symbol": symbol,
                "source": source or "",
                "side": side,
                "scenario": scenario,
                "latency_sec": latency,
            },
        }

        _safe_log(
            "INFO",
            f"[EXIT_OK] model={model} symbol={symbol} side={side} "
            f"scenario={scenario} action={action} close_ratio={close_ratio:.3f} "
            f"new_sl={new_sl_pct:.4f} new_tp={new_tp_pct:.4f} "
            f"latency={latency:.3f}s",
        )

        return result

    except Exception as e:
        latency = time.monotonic() - start_ts
        _safe_log(
            "ERROR",
            f"[EXIT_FAIL] model={model} symbol={symbol} side={side} "
            f"scenario={scenario} latency={latency:.3f}s err={type(e).__name__}: {e}",
        )
        _safe_tg(
            f"[EXIT_FAIL] model={model} symbol={symbol} side={side} "
            f"scenario={scenario} latency={latency:.3f}s err={type(e).__name__}: {e}"
        )
        raise RuntimeError(f"EXIT GPT 호출 실패: {type(e).__name__}: {e}") from e


def ask_exit_decision_safe(
    *,
    symbol: str,
    source: Optional[str],
    side: str,
    scenario: str,
    last_price: float,
    entry_price: float,
    leverage: float,
    extra: Dict[str, Any],
    model: str = GPT_MODEL_DEFAULT,
    fallback_action: Literal["HOLD", "CLOSE"] = "HOLD",
) -> Tuple[str, Any]:
    """
    ask_exit_decision 을 감싸는 안전 래퍼.

    - GPT 호출/파싱 실패 시에도 예외를 호출측으로 전파하지 않고,
      fallback_action("HOLD" 또는 "CLOSE") 을 action 으로 돌려준다.
    - position_watch_ws.maybe_exit_with_gpt(...) 에서 다음과 같이 사용된다.

        action, gpt_data = ask_exit_decision_safe(...)
        if action != "CLOSE":
            HOLD 처리
        else:
            CLOSE 처리

    반환:
        (action, gpt_data)
        - action: "CLOSE" | "HOLD"
        - gpt_data: GPT 응답 dict 또는 에러 정보 dict
    """
    try:
        data = ask_exit_decision(
            symbol=symbol,
            source=source,
            side=side,
            scenario=scenario,
            last_price=last_price,
            entry_price=entry_price,
            leverage=leverage,
            extra=extra,
            model=model,
        )
        raw_action = data.get("action")
        action = str(raw_action or fallback_action).upper().strip()
        if action not in ("CLOSE", "HOLD"):
            _safe_log(
                "WARNING",
                f"[EXIT_SAFE] invalid action from GPT={raw_action!r} -> {fallback_action} "
                f"| symbol={symbol} scenario={scenario}",
            )
            action = fallback_action
        return action, data
    except Exception as e:
        _safe_log(
            "ERROR",
            f"[EXIT_SAFE_FAIL] symbol={symbol} side={side} scenario={scenario} "
            f"err={type(e).__name__}: {e}",
        )
        _safe_tg(
            f"[EXIT_SAFE_FAIL] GPT EXIT 판단 실패. fallback_action={fallback_action} "
            f"(symbol={symbol}, scenario={scenario}, err={type(e).__name__}: {e})"
        )
        err_data = {
            "error": str(e),
            "error_type": type(e).__name__,
            "symbol": symbol,
            "side": side,
            "scenario": scenario,
        }
        return fallback_action, err_data


__all__ = [
    "ask_entry_decision",
    "ask_exit_decision",
    "ask_exit_decision_safe",
]
