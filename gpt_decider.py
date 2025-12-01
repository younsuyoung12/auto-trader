from __future__ import annotations

import csv
import datetime as dt
import json
import math
import os
import time
from threading import Lock
from typing import Any, Dict, Literal, Optional

from openai import OpenAI

try:
    import telelog  # type: ignore
except Exception:  # pragma: no cover
    telelog = None  # type: ignore


# =============================================================================
# 설정 / 상수
# =============================================================================

GPT_MODEL_DEFAULT = os.getenv("OPENAI_TRADER_MODEL", "gpt-5.1-mini")

OPENAI_TRADER_MAX_LATENCY = float(os.getenv("OPENAI_TRADER_MAX_LATENCY", "4.0"))
OPENAI_TRADER_MAX_TOKENS = int(os.getenv("OPENAI_TRADER_MAX_TOKENS", "192"))

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
    트레이딩 엔진 쪽에서는 이 예외를 잡아서 정상적인 폴백 로직을 수행하면 된다.
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
    OpenAI Responses API 응답 객체에서 텍스트 부분을 최대한 안전하게 추출한다.

    - 우선 resp.output_text 같은 direct 속성이 있으면 그것을 사용
    - 없으면 resp.output[0].content[0].text.value 루트를 시도
    - 그래도 안 되면 repr(resp) 일부를 잘라서라도 반환 (완전 실패보다는 낫다)
    """
    # 1) direct text 속성
    text_attr = getattr(resp, "output_text", None)
    if isinstance(text_attr, str) and text_attr.strip():
        return text_attr

    # 2) output -> content -> text.value 경로
    output = getattr(resp, "output", None)
    if output and isinstance(output, (list, tuple)):
        try:
            # 첫 번째 블록
            block = output[0]
            content = getattr(block, "content", None)
            if content and isinstance(content, (list, tuple)) and content:
                first = content[0]
                txt_obj = getattr(first, "text", None)
                # text.value
                value = getattr(txt_obj, "value", None)
                if isinstance(value, str) and value.strip():
                    return value

                # 혹시 모를 직접 문자열
                if isinstance(txt_obj, str) and txt_obj.strip():
                    return txt_obj
        except Exception:
            pass

    # 3) 최후의 수단: repr 일부
    return repr(resp)[:2000]


# =============================================================================
# GPT JSON 헬퍼 (EXIT용 공통)
# =============================================================================


def _call_gpt_json(
    *,
    model: str,
    prompt: str,
    purpose: str = "EXIT",
    timeout_sec: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Responses API를 사용해 JSON 응답을 기대하는 GPT 호출을 공통 처리.

    - response_format={"type": "json_object"} 사용
    - 레이턴시 측정 및 CSV 로깅
    - 단일 dict로 파싱 실패 시 예외
    """
    if timeout_sec is None:
        timeout_sec = OPENAI_TRADER_MAX_LATENCY

    client = _get_client()

    start = time.monotonic()
    try:
        resp = client.responses.create(
            model=model,
            input=prompt,
            response_format={"type": "json_object"},
            timeout=timeout_sec,
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
    ENTRY GPT의 JSON 응답을 정규화하고, 위험/타겟/손절 관련 필드를 검증한다.

    기대 JSON 스키마(요약):
    {
      "direction": "LONG" | "SHORT" | "PASS",
      "confidence": 0.0~1.0 실수,
      "tv_pct": 0.0~0.5 실수,  # 타겟 수익률
      "sl_pct": 0.0~0.5 실수,  # 손절 수익률
      "effective_risk_pct": 0.0~gpt_max_risk_pct 실수,
      "note": "string",
      "raw_response": "원본 문자열(JSON을 파싱하기 전의 텍스트)"
    }

    - direction이 PASS인 경우 tv/sl/effective_risk_pct는 모두 0으로 강제
    - tv_pct/sl_pct는 base_*와의 비율 제한(0.02 ~ 4.0배) 및 절대값 제한(0~0.5) 적용
    - effective_risk_pct는 0 < v <= gpt_max_risk_pct
    """
    direction = str(resp.get("direction", "PASS")).upper().strip()
    if direction not in ("LONG", "SHORT", "PASS"):
        direction = "PASS"

    def _as_float(key: str, default: float = 0.0) -> float:
        v = resp.get(key, default)
        try:
            if v is None:
                return default
            if isinstance(v, (int, float)):
                if math.isnan(float(v)) or math.isinf(float(v)):
                    return default
                return float(v)
            return float(str(v))
        except Exception:
            return default

    def _check_float(
        key: str,
        v: float,
        min_val: float,
        max_val: float,
        *,
        allow_zero: bool = False,
    ) -> float:
        # NaN/Inf 보호
        if math.isnan(v) or math.isinf(v):
            _fatal_log_and_raise(
                f"[ENTRY_VALIDATE] {key} is NaN/Inf: {v} | symbol={symbol} source={source}"
            )
        # 범위 체크
        if allow_zero:
            if not (min_val <= v <= max_val):
                _fatal_log_and_raise(
                    f"[ENTRY_VALIDATE] {key} out of range {min_val}~{max_val}: {v} | "
                    f"symbol={symbol} source={source}"
                )
        else:
            if not (min_val < v < max_val):
                _fatal_log_and_raise(
                    f"[ENTRY_VALIDATE] {key} out of range ({min_val},{max_val}): {v} | "
                    f"symbol={symbol} source={source}"
                )
        return v

    # direction PASS면 나머지는 0으로
    if direction == "PASS":
        tv_pct = 0.0
        sl_pct = 0.0
        effective_risk_pct = 0.0
    else:
        tv_pct = _as_float("tv_pct", base_tv_pct)
        sl_pct = _as_float("sl_pct", base_sl_pct)
        effective_risk_pct = _as_float("effective_risk_pct", base_risk_pct)

        tv_pct = _check_float("tv_pct", tv_pct, 0.0, 0.5)
        sl_pct = _check_float("sl_pct", sl_pct, 0.0, 0.5)

        if base_tv_pct > 0:
            ratio_tv = tv_pct / base_tv_pct
            if ratio_tv < 0.02 or ratio_tv > 4.0:
                _fatal_log_and_raise(
                    "[ENTRY_VALIDATE] tv_pct/base_tv_pct ratio out of range 0.02~4.0: "
                    f"{ratio_tv:.4f} (tv={tv_pct}, base={base_tv_pct}) | "
                    f"symbol={symbol} source={source}"
                )

        if base_sl_pct > 0:
            ratio_sl = sl_pct / base_sl_pct
            if ratio_sl < 0.02 or ratio_sl > 4.0:
                _fatal_log_and_raise(
                    "[ENTRY_VALIDATE] sl_pct/base_sl_pct ratio out of range 0.02~4.0: "
                    f"{ratio_sl:.4f} (sl={sl_pct}, base={base_sl_pct}) | "
                    f"symbol={symbol} source={source}"
                )

        if base_risk_pct > 0:
            effective_risk_pct = _check_float(
                "effective_risk_pct", effective_risk_pct, 0.0, gpt_max_risk_pct
            )
            if effective_risk_pct > gpt_max_risk_pct + 1e-9:
                _fatal_log_and_raise(
                    "[ENTRY_VALIDATE] effective_risk_pct exceeds GPT max risk limit: "
                    f"{effective_risk_pct} > {gpt_max_risk_pct} | "
                    f"symbol={symbol} source={source}"
                )
        else:
            effective_risk_pct = 0.0

    confidence = _as_float("confidence", 0.5)
    confidence = max(0.0, min(1.0, confidence))

    note = str(resp.get("note", "") or "").strip()
    raw_response = str(resp.get("raw_response", "") or "").strip()

    normalized = {
        "direction": direction,
        "confidence": confidence,
        "tv_pct": tv_pct,
        "sl_pct": sl_pct,
        "effective_risk_pct": effective_risk_pct,
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

    - 숫자:
        - NaN, ±Inf      -> 0.0
        - 그 외 실수/정수 -> 그대로
    - None: 0
    - dict / list / tuple: 재귀 처리 (최대 _max_depth)
    - 그 외 타입: 그대로 둔다 (문자열 등)

    지표 계산 단계에서 '폴백'을 넣지 않고,
    오직 GPT API가 요구하는 직렬화 포맷만 안정화한다는 취지다.
    """
    if _depth > _max_depth:
        return obj  # 너무 깊으면 더 이상 파고들지 않는다

    # 숫자 계열
    if isinstance(obj, (int, bool)):  # bool은 int의 서브클래스지만 그대로 둠
        return obj
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return 0.0
        return obj

    # None -> 0
    if obj is None:
        return 0

    # dict 재귀
    if isinstance(obj, dict):
        return {
            str(k): _sanitize_for_gpt(v, _depth=_depth + 1, _max_depth=_max_depth)
            for k, v in obj.items()
        }

    # list/tuple 재귀
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

[중요 규칙 요약]

1) Direction 결정
   - LONG: 상승 기대 시
   - SHORT: 하락 기대 시
   - PASS: 방향성이 애매하거나 리스크가 과도하면 진입하지 않음

2) 수익/손절/위험 제어
   - tv_pct (take profit, 목표 수익률 비율):
       - 대략 0 < tv_pct <= 0.12 (12%) 정도가 자연스러운 범위
       - "천천히 추세를 탄다" 수준이면 0.02~0.06, 강한 모멘텀이면 최대 0.12 정도까지 허용
       - 극단적으로 20% 이상(0.20)을 넘기지 않는 것이 좋다.
   - sl_pct (stop loss, 손절 비율):
       - 일반적으로 0 < sl_pct <= 0.06 (6%) 안쪽
       - 변동성이 큰 장이라면 0.03~0.06 사이에서 조정
       - sl_pct가 tv_pct보다 지나치게 크면 비대칭 리스크가 되므로 지양
   - effective_risk_pct (실제 계좌 기준 1회 진입 시 허용 위험 비율):
       - 0 < effective_risk_pct <= {gpt_max_risk_pct} (기본 3%)
       - 단일 포지션에서 계좌의 {gpt_max_risk_pct*100:.1f}%를 초과해서는 안 됨
       - 방향성이 애매하거나, 근거가 약하다고 느껴지면 더 낮은 값(예: 0.005~0.01)으로 보수적으로 잡는다.

3) 리스크 관리
   - 리스크가 과도하면 PASS를 택하는 것이 좋다.
   - "지금 안 들어가도 아쉬울 것 없는" 상황에서 무리해서 진입하지 않는다.
   - 높은 변동성 구간(급등/급락 직후, 주요 뉴스 직후 등)이라면
     tv_pct / sl_pct / effective_risk_pct 모두 평소보다 보수적으로 조정한다.

4) JSON 응답 포맷
   - 아래 JSON 스키마에 맞춰 응답한다.
   - 반드시 JSON만, 추가 텍스트 없이 출력한다.
   - NaN, Infinity, null, 문자열 "NaN"/"Infinity"/"None" 등은 절대 사용하지 않는다.
   - 모든 수치는 실수(float)로 응답한다.

[응답 JSON 스키마]

필수 필드:
- direction: "LONG" | "SHORT" | "PASS"
- confidence: 0.0 ~ 1.0 사이의 숫자 (매매 아이디어에 대한 자신감)
- tv_pct: 0.0 ~ 0.5 사이의 숫자 (목표 수익률 비율, 예: 0.03 = 3%)
- sl_pct: 0.0 ~ 0.5 사이의 숫자 (손절 비율, 예: 0.01 = 1%)
- effective_risk_pct: 0.0 ~ {gpt_max_risk_pct} 사이의 숫자 (계좌 대비 위험 비율)
- note: 문자열 (간단한 설명)
- raw_response: 문자열 (필요시 시스템이 사용할 수 있도록, 핵심 요약 등 자유롭게 기입)

제약 조건:
- direction이 "PASS"일 경우:
    - tv_pct = 0.0
    - sl_pct = 0.0
    - effective_risk_pct = 0.0
- direction이 "LONG" 또는 "SHORT"일 경우:
    - 0 < tv_pct <= 0.5
    - 0 < sl_pct <= 0.5
    - 0 < effective_risk_pct <= {gpt_max_risk_pct}
- NaN / Infinity / null / "NaN" / "Infinity" / "None" 등의 값은 절대 사용하지 않는다.

JSON 예시 (단, 실제 응답에서는 한국어 note 사용 가능):

```json
{
  "direction": "LONG",
  "confidence": 0.74,
  "tv_pct": 0.032,
  "sl_pct": 0.012,
  "effective_risk_pct": 0.018,
  "note": "상승 추세가 이어질 가능성이 크지만, 변동성도 있으므로 손절은 다소 타이트하게 설정.",
  "raw_response": "간단한 요약 또는 내부용 메모"
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

Market Features (JSON 직렬화):
{market_features_json}

[요구사항]

위 정보를 바탕으로 이번 진입에 대한 다음 항목을 JSON 형식으로만 응답해 주세요.

- direction: "LONG" | "SHORT" | "PASS"
- confidence: 0.0 ~ 1.0
- tv_pct: 0.0 ~ 0.5
- sl_pct: 0.0 ~ 0.5
- effective_risk_pct: 0.0 ~ {gpt_max_risk_pct}
- note: string
- raw_response: string

주의:
- direction이 "PASS"일 경우 tv_pct, sl_pct, effective_risk_pct는 모두 0.0으로 설정합니다.
- NaN / Infinity / null / 문자열 "NaN" / "Infinity" / "None" 등은 절대 사용하지 않습니다.
- 반드시 JSON만, 추가 설명 텍스트 없이 응답합니다.
"""


# =============================================================================
# OpenAI 클라이언트 헬퍼
# =============================================================================


def _get_client() -> OpenAI:
    """
    OpenAI 클라이언트를 생성한다.
    """
    api_base = os.getenv("OPENAI_API_BASE", "").strip() or None
    client = OpenAI(base_url=api_base)
    return client


def _parse_json(text: str) -> Dict[str, Any]:
    """
    GPT가 반환한 텍스트에서 JSON을 파싱.
    - 일단 전체를 json.loads 시도
    - 실패하면 '{'와 '}' 사이만 잘라내 재시도
    """
    text = (text or "").strip()
    if not text:
        raise ValueError("GPT 응답이 비어 있습니다.")

    try:
        data = json.loads(text)
        if isinstance(data, dict):
            return data
        raise ValueError(f"JSON 최상위 타입이 dict가 아님: {type(data).__name__}")
    except Exception:
        pass

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
) -> Dict[str, Any]:
    """
    ENTRY 결정에 사용할 payload dict를 구성한다.
    이 payload는 GPT 프롬프트에 JSON으로 직렬화되어 전달되며,
    NaN/Inf/None은 _sanitize_for_gpt 단계에서 보정된다.
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

    payload = {
        "symbol": symbol,
        "source": source,
        "current_price": current_price,
        "base_tv_pct": base_tv_pct,
        "base_sl_pct": base_sl_pct,
        "base_risk_pct": base_risk_pct,
        "market_features": extra_filtered,
    }

    if isinstance(market_features, dict):
        for k, v in market_features.items():
            if k not in extra_filtered and k not in payload:
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
) -> Dict[str, Any]:
    """
    ENTRY 결정을 위해 GPT에게 질의하고, 응답을 정규화/검증하여 반환한다.

    Parameters
    ----------
    symbol : str
        종목 심볼 (예: "BTCUSDT").
    source : str
        시그널/전략 이름 또는 출처 태그.
    current_price : float
        현재 가격.
    base_tv_pct : float
        기본 타겟 수익률 비율 (예: 0.03 = 3%).
    base_sl_pct : float
        기본 손절 비율 (예: 0.01 = 1%).
    base_risk_pct : float
        기본 1회 진입 시 계좌 대비 위험 비율.
    market_features : Dict[str, Any]
        마켓 상태를 요약한 피처 딕셔너리.
    model : str, optional
        사용할 GPT 모델 이름. 기본값은 환경변수 OPENAI_TRADER_MODEL 또는 "gpt-5.1-mini".
    gpt_max_risk_pct : float, optional
        GPT가 설정할 수 있는 최대 effective_risk_pct. 기본값은 GPT_MAX_RISK_PCT.

    Returns
    -------
    Dict[str, Any]
        정규화/검증된 ENTRY 결정 정보:
        {
          "direction": "LONG" | "SHORT" | "PASS",
          "confidence": float,
          "tv_pct": float,
          "sl_pct": float,
          "effective_risk_pct": float,
          "note": str,
          "raw_response": str
        }

    Raises
    ------
    RuntimeError
        - OpenAI API 오류 / 네트워크 오류 / JSON 파싱 오류 / 응답 지연 초과 / JSON 스키마 검증 실패 / market_features 이상 시
          치명적 오류로 간주하고 RuntimeError를 발생시킨다.
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
    )

    sanitized_payload = _sanitize_for_gpt(payload)
    market_features_json = json.dumps(
        sanitized_payload, ensure_ascii=False, separators=(",", ":")
    )

    system_prompt = _SYSTEM_PROMPT_ENTRY.format(gpt_max_risk_pct=gpt_max_risk_pct)
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
    success = False

    start_ts = time.monotonic()
    try:
        max_tokens = OPENAI_TRADER_MAX_TOKENS
        max_latency = OPENAI_TRADER_MAX_LATENCY

        _safe_log(
            "DEBUG",
            f"[ENTRY_CALL] model={model} symbol={symbol} source={source} "
            f"base_tv={base_tv_pct} base_sl={base_sl_pct} base_risk={base_risk_pct}",
        )

        gpt_entry_call_count += 1
        gpt_call_id = gpt_entry_call_count

        resp = client.responses.create(
            model=model,
            input=[
                {
                    "role": "system",
                    "content": system_prompt,
                },
                {
                    "role": "user",
                    "content": user_prompt,
                },
            ],
            max_output_tokens=max_tokens,
            timeout=max_latency,
            response_format={"type": "json_object"},
        )
        latency = time.monotonic() - start_ts
        success = True

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

        _safe_log(
            "INFO",
            f"[ENTRY_OK] model={model} symbol={symbol} source={source} "
            f"dir={normalized['direction']} conf={normalized['confidence']:.3f} "
            f"tv={normalized['tv_pct']:.4f} sl={normalized['sl_pct']:.4f} "
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
# EXIT 쪽 프롬프트 / 호출
# =============================================================================

_EXIT_SYSTEM_PROMPT = """
You are an expert trading exit advisor for intraday crypto futures trading.

[역할]
- 이미 진입한 포지션에 대해, 지금 청산(전부 또는 일부)해야 할지, 또는 유지해야 할지에 대해 조언합니다.
- 손절/익절 레벨에 근접했는지, 시장 구조가 바뀌었는지 등을 고려해 결정을 돕습니다.

[입력으로 제공되는 정보]
- symbol, side(LONG/SHORT), entry_price, current_price, entry_time, holding_minutes
- unrealized_pnl_pct (실현되지 않은 손익률, %, 예: 0.03 = +3%)
- max_favorable_excursion_pct (MFE), max_adverse_excursion_pct (MAE)
- regime, volatility, trend 관련 피처 등

[출력 JSON 스키마]

필수 필드:
- action: "CLOSE_ALL" | "CLOSE_PARTIAL" | "HOLD"
- close_ratio: 0.0 ~ 1.0 (CLOSE_PARTIAL일 때 청산 비율, 예: 0.5 = 50%)
- new_sl_pct: 0.0 ~ 0.5 (새로운 손절 라인(진입가 기준), 필요 없으면 기존 유지 의미에서 0.0 사용 가능)
- new_tp_pct: 0.0 ~ 0.5 (새로운 익절 라인(진입가 기준), 필요 없으면 0.0)
- note: string (결정 이유 설명)
- raw_response: string (추가 메모 등)

제약:
- "CLOSE_ALL"인 경우 close_ratio는 1.0
- "HOLD"인 경우 close_ratio는 0.0
- NaN / Infinity / null / "NaN" / "Infinity" / "None" 등은 절대 사용하지 않는다.

JSON 예시:
```json
{
  "action": "CLOSE_PARTIAL",
  "close_ratio": 0.5,
  "new_sl_pct": 0.01,
  "new_tp_pct": 0.04,
  "note": "이미 상당 부분 이익 구간에 진입했으므로 절반 청산 후 나머지는 추세를 따라가도록 설정.",
  "raw_response": "간단한 요약 또는 내부용 메모"
}
"""
_EXIT_USER_PROMPT_TEMPLATE = """
[포지션 정보]

Symbol: {symbol}
Side: {side}
Entry Price: {entry_price}
Current Price: {current_price}
Entry Time (UTC): {entry_time}
Holding Minutes: {holding_minutes}

[포지션 성과]

Unrealized PnL %: {pnl_pct}
Max Favorable Excursion % (MFE): {mfe_pct}
Max Adverse Excursion % (MAE): {mae_pct}

[시장 상태 요약 (JSON 직렬화)]

{extra_json}

[요구사항]

위 정보를 바탕으로, 현재 포지션을 어떻게 처리할지 JSON 형식으로만 응답해 주세요.

- action: "CLOSE_ALL" | "CLOSE_PARTIAL" | "HOLD"
- close_ratio: 0.0 ~ 1.0
- new_sl_pct: 0.0 ~ 0.5
- new_tp_pct: 0.0 ~ 0.5
- note: string
- raw_response: string

주의:
- "CLOSE_ALL"이면 close_ratio=1.0.
- "HOLD"이면 close_ratio=0.0.
- NaN / Infinity / null / "NaN" / "Infinity" / "None" 등의 값은 절대 사용하지 않습니다.
- 반드시 JSON만, 추가 설명 텍스트 없이 응답합니다.
"""


def _make_exit_prompt(payload: Dict[str, Any]) -> str:
    """
    EXIT 결정용 user 프롬프트 문자열을 만든다.
    이 때 payload는 GPT 직전 _sanitize_for_gpt를 거쳐 NaN/Inf/None이 제거된 상태여야 한다.
    """
    extra_json = json.dumps(
        _sanitize_for_gpt(payload),
        ensure_ascii=False,
        indent=2,
    )

    prompt = _EXIT_USER_PROMPT_TEMPLATE.format(
        symbol=payload.get("symbol"),
        side=payload.get("side"),
        entry_price=payload.get("entry_price"),
        current_price=payload.get("current_price"),
        entry_time=payload.get("entry_time"),
        holding_minutes=payload.get("holding_minutes"),
        pnl_pct=payload.get("pnl_pct"),
        mfe_pct=payload.get("mfe_pct"),
        mae_pct=payload.get("mae_pct"),
        extra_json=extra_json,
    )
    return prompt


def ask_exit_decision(
    *,
    symbol: str,
    side: Literal["LONG", "SHORT"],
    entry_price: float,
    current_price: float,
    entry_time: dt.datetime,
    holding_minutes: float,
    mfe_pct: float,
    mae_pct: float,
    extra: Dict[str, Any],
    model: str = GPT_MODEL_DEFAULT,
) -> Dict[str, Any]:
    """
    EXIT 결정을 위해 GPT에게 질의하고, 응답을 dict로 반환한다.
    NaN / Inf / None은 GPT 직전 _sanitize_for_gpt에 의해 제거된다.
    """
    now = dt.datetime.utcnow()
    entry_time_utc = entry_time.astimezone(dt.timezone.utc).replace(tzinfo=None)

    # PnL %
    try:
        if entry_price > 0:
            pnl_pct = (current_price - entry_price) / entry_price
            if side == "SHORT":
                pnl_pct *= -1.0
        else:
            pnl_pct = None
    except Exception:
        pnl_pct = None

    payload: Dict[str, Any] = {
        "symbol": symbol,
        "side": side,
        "entry_price": entry_price,
        "current_price": current_price,
        "entry_time": entry_time_utc.isoformat(),
        "now": now.isoformat(),
        "holding_minutes": holding_minutes,
        "pnl_pct": pnl_pct,
        "mfe_pct": mfe_pct,
        "mae_pct": mae_pct,
        "extra_context": extra or {},
    }

    prompt = _make_exit_prompt(payload)
    system = _EXIT_SYSTEM_PROMPT

    latency = 0.0
    start_ts = time.monotonic()

    try:
        _safe_log(
            "DEBUG",
            f"[EXIT_CALL] model={model} symbol={symbol} side={side} "
            f"entry={entry_price} cur={current_price} pnl={pnl_pct}",
        )

        data = _call_gpt_json(
            model=model,
            prompt=system + "\n\n" + prompt,
            purpose="EXIT",  # CSV kind="EXIT" 으로 기록
        )
        latency = time.monotonic() - start_ts

        action = str(data.get("action", "HOLD")).upper().strip()
        if action not in ("CLOSE_ALL", "CLOSE_PARTIAL", "HOLD"):
            action = "HOLD"

        def _flt(key: str, default: float = 0.0, *, min_val: float, max_val: float) -> float:
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

        close_ratio = _flt("close_ratio", 0.0, min_val=0.0, max_val=1.0)
        new_sl_pct = _flt("new_sl_pct", 0.0, min_val=0.0, max_val=0.5)
        new_tp_pct = _flt("new_tp_pct", 0.0, min_val=0.0, max_val=0.5)

        if action == "CLOSE_ALL":
            close_ratio = 1.0
        elif action == "HOLD":
            close_ratio = 0.0

        note = str(data.get("note", "") or "").strip()
        raw_response = str(data.get("raw_response", "") or "").strip()

        result = {
            "action": action,
            "close_ratio": close_ratio,
            "new_sl_pct": new_sl_pct,
            "new_tp_pct": new_tp_pct,
            "note": note,
            "raw_response": raw_response,
        }

        _safe_log(
            "INFO",
            f"[EXIT_OK] model={model} symbol={symbol} side={side} "
            f"action={action} close_ratio={close_ratio:.3f} "
            f"new_sl={new_sl_pct:.4f} new_tp={new_tp_pct:.4f} "
            f"latency={latency:.3f}s",
        )

        return result

    except Exception as e:
        latency = time.monotonic() - start_ts
        _safe_log(
            "ERROR",
            f"[EXIT_FAIL] model={model} symbol={symbol} side={side} "
            f"latency={latency:.3f}s err={type(e).__name__}: {e}",
        )
        _safe_tg(
            f"[EXIT_FAIL] model={model} symbol={symbol} side={side} "
            f"latency={latency:.3f}s err={type(e).__name__}: {e}"
        )
        raise RuntimeError(f"EXIT GPT 호출 실패: {type(e).__name__}: {e}") from e


def ask_exit_decision_safe(
    *,
    symbol: str,
    side: Literal["LONG", "SHORT"],
    entry_price: float,
    current_price: float,
    entry_time: dt.datetime,
    holding_minutes: float,
    mfe_pct: float,
    mae_pct: float,
    extra: Dict[str, Any],
    model: str = GPT_MODEL_DEFAULT,
    fallback_action: Literal["HOLD", "CLOSE_ALL"] = "HOLD",
) -> Dict[str, Any]:
    """
    ask_exit_decision을 감싸서, GPT 호출 실패 시에도 예외를 바깥으로 전파하지 않고
    안전한 폴백 액션(HOLD 또는 CLOSE_ALL)을 반환한다.

    Parameters
    ----------
    fallback_action : {"HOLD", "CLOSE_ALL"}
        GPT 호출 실패 시 사용할 기본 액션. 기본값은 "HOLD".

    Returns
    -------
    Dict[str, Any]
        (가능하면) GPT 기반 EXIT 결정 결과, 실패 시 fallback_action 기반 결과.
    """
    _ = fallback_action  # 현재는 항상 예외를 외부로 전파하므로 사용하지 않음

    return ask_exit_decision(
        symbol=symbol,
        side=side,
        entry_price=entry_price,
        current_price=current_price,
        entry_time=entry_time,
        holding_minutes=holding_minutes,
        mfe_pct=mfe_pct,
        mae_pct=mae_pct,
        extra=extra,
        model=model,
    )


__all__ = [
    "ask_entry_decision",
    "ask_exit_decision",
    "ask_exit_decision_safe",
]
