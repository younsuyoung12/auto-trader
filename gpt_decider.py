from __future__ import annotations

"""
gpt_decider.py
====================================================
역할
----------------------------------------------------
- BingX Auto Trader에서 진입/청산 의사결정을 GPT-5.1에 위임하는 어댑터(브레인 모듈).
- 엔트리 루프 / 포지션 감시 루프에서 이 모듈만 import 해서 사용한다.

2025-11-20 변경 사항 (시장 상태 기반 프롬프트 고도화 + JSON 템플릿/검증 강화 + 폴백 제거)
----------------------------------------------------
1) 시장 상태(박스/추세/꼬리 패턴 등)를 고려하도록 진입/청산 프롬프트를 전면 재작성.
   - extra 안에 포함될 수 있는 market_state / risk_context / tail_pattern 정보를 활용하도록 안내.
   - RANGE(박스) / TREND(추세) / 혼잡 구간에서 서로 다른 TP/SL·리스크 선호도를 설명.
2) JSON 실패 방지를 위해 entry/exit 프롬프트에 엄격한 JSON 템플릿·규칙을 명시.
   - 코드블록/주석/NaN/Infinity 금지.
   - action, tp_pct, sl_pct, effective_risk_pct 등의 필드 스키마를 예시로 제시.
3) ask_entry_decision 경로에 response_format={"type": "json_object"} 적용
   (구버전 openai-python 에서는 자동 폴백) + 서버측 JSON 검증 로직 추가.
   - action 필수값/허용값 검증 ("ENTER" / "SKIP" / "ADJUST").
   - 숫자 필드(tp_pct/sl_pct/effective_risk_pct) 타입 및 범위 검증.
4) exit 경로도 JSON 응답 검증을 추가하여 action 을 "CLOSE"/"HOLD" 두 값으로 정규화.
5) *_safe 래퍼에서 기존 '자동 폴백(ENTER/CLOSE)' 제거.
   - GPT 오류/JSON 형식 오류 발생 시 telelog.log + Telegram 알림 후 예외를 그대로 전파.
   - 데이터 이상이 있으면 Render 로그·트레이스에서 즉시 확인 가능하도록 설계.

2025-11-19 변경 사항 (TREND/RANGE/ARB 제거 + 브레인 단순화)
----------------------------------------------------
1) TREND/RANGE 전략 중재용 ask_signal_arbitration(...) 및 관련 설명/레거시 제거.
   - 박스장/추세장 구분 대신, 하나의 "시장 상태(market features)"만 GPT에 전달하는 구조로 단순화.
2) 모듈 역할을 "진입/청산 의사결정 전용 브레인"으로 명확히 재정의.
3) SYSTEM 프롬프트와 주석에서 TREND/RANGE/HYBRID 등의 전략 구분 용어 삭제.
   - regime 필드는 단순 태그(예: "MARKET") 정도로만 사용 가능하도록 설명 정리.
4) __all__ 정리: 공개 API를 ask_entry_decision(_safe), ask_exit_decision(_safe) 네 개로 한정.

2025-11-18 변경 사항 (진입 프롬프트/토큰 경량화 + 지연 시간 가드 통합)
----------------------------------------------------
1) ask_entry_decision 경로를 완전히 재구성:
   - SYSTEM 프롬프트는 고정된 짧은 설명만 사용.
   - USER 메시지는 compact JSON(payload) 하나만 전달.
2) gpt_trader.py 에서 경량화한 extra 를 그대로 받아서 JSON payload 로만 넘기므로,
   raw 캔들/시세 리스트가 그대로 프롬프트에 섞이지 않게 함.
3) OPENAI_TRADER_MODEL / OPENAI_TRADER_MAX_TOKENS / OPENAI_TRADER_MAX_LATENCY 환경변수로
   모델명, 출력 토큰 수, 허용 지연시간을 조정 가능하게 함.
4) ask_entry_decision 응답에 _meta.latency_sec 를 추가해서 왕복 지연시간을 로그/CSV에서
   확인할 수 있게 함.
5) _call_gpt_json 의 timeout_sec 가 None 이면 OPENAI_TRADER_MAX_LATENCY 를 기본값으로 사용하여
   EXIT 경로도 동일한 지연시간 설정을 공유하도록 정리.

2025-11-19 변경 사항 (GPT 진입 지연 CSV 로깅)
----------------------------------------------------
1) ask_entry_decision 경로에서 GPT 왕복 지연 시간을
   logs/gpt_latency/gpt_latency-YYYY-MM-DD.csv 로 CSV 기록하는 헬퍼(_log_gpt_latency_csv) 추가.
2) OPENAI_TRADER_SOFT_LATENCY 환경변수로 "소프트 타임아웃" 기준을 분리해,
   CSV에 is_slow 플래그로 함께 남기도록 정리.
"""

import csv
import datetime
import json
import math
import os
import time
from threading import Lock
from typing import Any, Dict, Optional, Tuple

from openai import OpenAI

_client: Optional[OpenAI] = None


# ─────────────────────────────────────────
# 공통 유틸 헬퍼
# ─────────────────────────────────────────


def _safe_log(msg: str) -> None:
    """telelog.log 를 사용할 수 있으면 사용하고, 실패해도 조용히 무시한다."""
    try:
        # 지연 import 로 순환 의존 방지
        from telelog import log

        log(msg)
    except Exception:
        # 로깅 실패로 인해 트레이딩 로직이 죽지 않도록 무시
        pass


def _fatal_log_and_raise(prefix: str, exc: Exception) -> None:
    """치명적인 오류를 Render 로그에 남기고 예외를 그대로 다시 던진다."""
    try:
        from telelog import log

        log(f"{prefix}: {exc}")
    except Exception:
        # telelog 가 실패해도 예외 자체는 그대로 올라가도록 한다.
        pass
    raise exc


# ─────────────────────────────────────────
# GPT 지연 시간 CSV 로거
# ─────────────────────────────────────────

_gpt_latency_lock: Lock = Lock()


def _log_gpt_latency_csv(
    *,
    kind: str,  # "ENTRY" / "EXIT" 등
    model: str,
    symbol: Optional[str],
    source: Optional[str],
    direction: Optional[str],
    latency_sec: Optional[float],
    soft_limit_sec: Optional[float],
    hard_limit_sec: Optional[float],
    success: bool,
    error_message: Optional[str] = None,
) -> None:
    """GPT 왕복 지연 시간을 logs/gpt_latency/*.csv 에 한 줄로 남긴다.

    CSV 스키마:
        ts_kst, kind, model, symbol, source, direction,
        latency_sec, soft_limit_sec, hard_limit_sec,
        success, is_timeout_or_error, is_slow, error

    - 기록 실패 시 트레이딩 로직에는 영향을 주지 않는다.
    """
    try:
        base_dir = os.path.join("logs", "gpt_latency")
        os.makedirs(base_dir, exist_ok=True)

        # KST 기준 현재 시각
        kst_now = datetime.datetime.utcnow() + datetime.timedelta(hours=9)
        date_str = kst_now.strftime("%Y-%m-%d")
        ts_str = kst_now.strftime("%Y-%m-%d %H:%M:%S")

        path = os.path.join(base_dir, f"gpt_latency-{date_str}.csv")

        # 플래그 계산
        is_timeout_or_error = (not success) or (
            latency_sec is not None
            and hard_limit_sec is not None
            and latency_sec > hard_limit_sec
        )
        is_slow = (
            latency_sec is not None
            and soft_limit_sec is not None
            and latency_sec > soft_limit_sec
        )

        row = [
            ts_str,
            kind,
            model or "",
            symbol or "",
            source or "",
            direction or "",
            f"{latency_sec:.3f}" if latency_sec is not None else "",
            f"{soft_limit_sec:.3f}" if soft_limit_sec is not None else "",
            f"{hard_limit_sec:.3f}" if hard_limit_sec is not None else "",
            "1" if success else "0",
            "1" if is_timeout_or_error else "0",
            "1" if is_slow else "0",
            (error_message or "").replace("\n", " ")[:200],
        ]

        header = [
            "ts_kst",
            "kind",
            "model",
            "symbol",
            "source",
            "direction",
            "latency_sec",
            "soft_limit_sec",
            "hard_limit_sec",
            "success",
            "is_timeout_or_error",
            "is_slow",
            "error",
        ]

        with _gpt_latency_lock:
            file_exists = os.path.exists(path)
            with open(path, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(header)
                writer.writerow(row)
    except Exception as e:
        # CSV 기록 실패는 트레이딩에 영향 주지 않도록 로그만 남김
        _safe_log(f"[GPT_LATENCY][ERROR] {e}")


# ─────────────────────────────────────────
# GPT 시스템 프롬프트 (진입용)
# ─────────────────────────────────────────

_SYSTEM_PROMPT_ENTRY = """
당신은 비트코인 선물 자동매매 시스템의 '진입 허용 여부'만 판단하는 작은 에이전트입니다.

입력으로 하나의 JSON 객체를 받습니다. 이 JSON 에는 대략 다음 정보가 들어 있습니다:
- symbol: "BTC-USDT" 등 거래 종목
- regime: 예) "MARKET" · 세션/전략 태그
- direction: "LONG" / "SHORT"
- last_price: 현재 가격
- entry_score: 진입 점수(높을수록 유리한 방향일 가능성)
- effective_risk_pct: 현재 기본 주문 비율
- leverage: 레버리지 배율
- base_tp_pct, base_sl_pct: 기본 TP/SL 비율
- guard_snapshot: 거래량·스프레드·점프·호가 불균형 등 각종 가드 값
- recent_pnl_pct, skip_streak: 최근 손익과 연속 손실/스킵 횟수 등 리스크 컨텍스트
- market_state / tail_pattern / wick 정보(있을 수 있음):
  * market_state.phase: "RANGE" / "TREND" / "CHAOTIC" 등
  * market_state.boxiness_score: 박스 성향(높을수록 박스장)
  * market_state.trend_strength: 추세 강도(양수 롱/음수 숏 우위)
  * market_state.volatility_regime: 저·중·고 변동성
  * market_state.session: "ASIA" / "EU" / "US" 등
  * tail_pattern 또는 wick_* 필드: 윗꼬리/아랫꼬리 강도, 핀바/스파이크 여부 등

당신의 목표는:
- "지금 이 방향으로 진입하는 것이 합리적인지"를,
- 시장 상태(박스/추세/꼬리 패턴)와 최근 리스크 상태를 함께 고려해서 결정하는 것입니다.

1) action 선택
----------------------------------------
action 필드는 반드시 다음 중 하나여야 합니다.
- "ENTER": 지금 조건이면 진입 허용.
- "SKIP": 지금은 진입하지 말 것.
- "ADJUST": 진입은 허용하지만, TP/SL/리스크/가드 값을 소폭 조정.

선택 가이드:
- 강한 추세(TREND) + 꼬리가 추세 방향을 지지 + entry_score 가 충분히 높으면
  → ENTER 또는 ADJUST (TP 약간 확대 가능).
- 박스장(RANGE)·혼잡 구간(boxiness_score 높고 trend_strength 약함)에서는
  → 작은 TP, 타이트한 SL, 낮은 리스크를 선호하며,
    entry_score 가 낮거나 꼬리 패턴이 애매하면 SKIP 을 더 자주 선택.
- 최근 손실이 누적(recent_pnl_pct 음수, skip_streak 크거나 손실 연속)이면
  → 리스크를 줄이거나(SKIP/ADJUST) 과도한 레버리지 진입을 피해야 한다.
- ADJUST 는 방향은 맞다고 보지만,
  → base_tp_pct / base_sl_pct / effective_risk_pct 를 "작게 미세 조정"할 때만 사용한다.
  → 기본값 대비 ±50% 범위를 크게 벗어나지 않는다고 가정하고 설계한다.
  → 최근 손실 구간에서는 ADJUST 보다는 SKIP 을 우선 고려한다.

2) 숫자 필드 (옵션)
----------------------------------------
다음 숫자 필드는 필요할 때만 넣습니다. 없으면 기본값(base_*)을 그대로 사용한다고 가정합니다.
- tp_pct:
  * 0 < tp_pct <= 0.12 범위 권장 (아주 강한 추세라도 0.20 을 넘지 말 것).
  * 강한 RANGE/박스 구간에서는 base_tp_pct 보다 작게 조정하는 것을 우선 고려.
- sl_pct:
  * 0 < sl_pct <= 0.07 범위 권장 (예외 상황에서도 0.15 를 넘지 말 것).
  * 박스/노이즈 구간에서는 SL 을 너무 넓게 두지 말고, 추세가 꺾였다고 판단되면 진입 자체를 SKIP.
- effective_risk_pct:
  * 0 < effective_risk_pct <= 0.03 범위 권장.
  * 기존 값보다 크게 늘리는 것보다, 위험할 때 과감히 줄이거나(SKIP/ADJUST) 유지하는 쪽을 우선.

3) guard_adjustments (옵션)
----------------------------------------
guard_adjustments 는 다음 키 중 필요한 것만 포함하는 객체입니다.
- min_entry_volume_ratio
- max_spread_pct
- max_price_jump_pct
- depth_imbalance_min_ratio
- depth_imbalance_min_notional

규칙:
- guard_snapshot 에 있는 값들을 기준으로 "조금 더 보수적으로" 조정하는 용도로만 사용합니다.
- 갑자기 스프레드를 크게 허용하거나, price_jump 한계를 과도하게 늘리지는 마십시오.

4) reason (필수)
----------------------------------------
- 한국어로 1~2줄 정도의 짧은 설명을 넣습니다.
- 예: "상승 추세 강하고 아래꼬리 강해 롱 진입 허용, TP 약간 확대"

5) 출력 JSON 형식 (템플릿 예시)
----------------------------------------
반드시 아래와 같은 JSON 스키마를 따릅니다. 필요 없는 키는 생략해도 됩니다.

{
  "action": "ENTER",
  "reason": "상승 추세 강하고 아래꼬리 강해 롱 진입 허용, TP 소폭 확대",
  "tp_pct": 0.008,
  "sl_pct": 0.004,
  "effective_risk_pct": 0.02,
  "guard_adjustments": {
    "min_entry_volume_ratio": 0.30,
    "max_spread_pct": 0.0008,
    "max_price_jump_pct": 0.0040,
    "depth_imbalance_min_ratio": 1.8,
    "depth_imbalance_min_notional": 150000
  },
  "_meta": {
    "pattern_note": "상승 추세 + 아래꼬리 강함",
    "market_state": "RANGE 에서 상향 돌파 시도"
  }
}

형식 규칙 (아주 중요):
- 반드시 JSON 객체 하나만 출력하십시오.
- JSON 바깥에는 아무 텍스트도 쓰지 마십시오.
- 코드블록 마크다운(예: ```json) · 주석(//, # 등) · NaN/Infinity 는 절대 사용하지 마십시오.
- 모든 키는 큰따옴표(")로 감싸고, 문자열도 큰따옴표(")를 사용하십시오.
""".strip()


# ─────────────────────────────────────────
# 공통 GPT 클라이언트/호출 헬퍼
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

                txt_obj = getattr(first, "text", None)
                if isinstance(txt_obj, str):
                    return txt_obj
                if txt_obj is not None:
                    val = getattr(txt_obj, "value", None)
                    if isinstance(val, str):
                        return val

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
    timeout_sec: Optional[float] = None,
    purpose: Optional[str] = None,
) -> Dict[str, Any]:
    """GPT-5.1 을 호출해서 **반드시 JSON**만 받는 헬퍼.

    - 실패/타임아웃/JSON 파싱 에러 시 예외를 던진다.
    - purpose 는 호출 용도 구분용 태그(현재는 로깅·라우팅용 확장 포인트).
    """
    client = _get_client()

    if timeout_sec is None:
        try:
            timeout_sec = float(os.getenv("OPENAI_TRADER_MAX_LATENCY", "8.0"))
        except Exception:
            timeout_sec = 8.0

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

    if not isinstance(data, dict):
        raise ValueError(f"GPT JSON 루트 타입이 dict 가 아닙니다: {type(data)!r}")

    return data


# ─────────────────────────────────────────
# 1) 진입 의사결정 (경량 프롬프트 버전)
# ─────────────────────────────────────────


def _build_entry_payload(
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
) -> Dict[str, Any]:
    """GPT에 넘길 compact JSON payload 구성.

    - extra(dict)는 gpt_trader 에서 이미 경량화가 끝난 상태라고 가정.
    - 여기서는 핵심 메타 필드만 얹어서 하나의 JSON 으로 합친다.
    """
    payload: Dict[str, Any] = {}

    if isinstance(extra, Dict) and extra:
        payload.update(extra)

    payload.setdefault("symbol", symbol)
    payload.setdefault("regime", signal_source.upper())
    payload.setdefault("direction", chosen_signal.upper())
    payload.setdefault("last_price", float(last_price))
    payload.setdefault("effective_risk_pct", float(effective_risk_pct))
    payload.setdefault("leverage", float(leverage))
    payload.setdefault("base_tp_pct", float(tp_pct))
    payload.setdefault("base_sl_pct", float(sl_pct))

    if entry_score is not None:
        payload.setdefault("entry_score", float(entry_score))

    return payload


def _normalize_and_validate_entry_response(
    data: Dict[str, Any],
    *,
    base_tp_pct: float,
    base_sl_pct: float,
    base_risk_pct: float,
) -> Dict[str, Any]:
    """GPT 진입 응답 JSON 을 정규화/검증한다.

    - action: "ENTER"/"SKIP"/"ADJUST" 이 아니면 에러.
    - tp_pct/sl_pct/effective_risk_pct: 존재할 경우 타입/범위 검증.
    """
    # action 필수
    action = data.get("action")
    if not isinstance(action, str):
        raise ValueError("GPT entry response 에 'action' 문자열 필드가 없습니다.")
    action_u = action.strip().upper()
    if action_u not in {"ENTER", "SKIP", "ADJUST"}:
        raise ValueError(f"GPT entry response 의 action 값이 잘못되었습니다: {action_u!r}")
    data["action"] = action_u

    # 숫자 필드 검증 (있을 때만)
    def _check_float(
        key: str,
        value: Any,
        min_val: float,
        max_val: float,
    ) -> float:
        try:
            v = float(value)
        except Exception:
            raise ValueError(f"GPT entry response 의 {key} 는 숫자여야 합니다: {value!r}")
        if not math.isfinite(v):
            raise ValueError(f"GPT entry response 의 {key} 가 유한 실수가 아닙니다: {v!r}")
        if not (min_val < v < max_val):
            raise ValueError(
                f"GPT entry response 의 {key}={v:.6f} 가 허용 범위({min_val}~{max_val})를 벗어납니다."
            )
        return v

    if "tp_pct" in data and data.get("tp_pct") is not None:
        # 기본값의 0.25배 ~ 4배 사이, 절대값은 0~0.5 안쪽 정도로 제한
        v = _check_float("tp_pct", data["tp_pct"], 0.0, 0.5)
        if base_tp_pct > 0:
            ratio = v / base_tp_pct
            if not (0.25 <= ratio <= 4.0):
                raise ValueError(
                    f"GPT entry response 의 tp_pct={v:.6f} 가 base_tp_pct 대비 비정상 비율(ratio={ratio:.2f})입니다."
                )
        data["tp_pct"] = v

    if "sl_pct" in data and data.get("sl_pct") is not None:
        v = _check_float("sl_pct", data["sl_pct"], 0.0, 0.5)
        if base_sl_pct > 0:
            ratio = v / base_sl_pct
            if not (0.25 <= ratio <= 4.0):
                raise ValueError(
                    f"GPT entry response 의 sl_pct={v:.6f} 가 base_sl_pct 대비 비정상 비율(ratio={ratio:.2f})입니다."
                )
        data["sl_pct"] = v

    if "effective_risk_pct" in data and data.get("effective_risk_pct") is not None:
        v = _check_float("effective_risk_pct", data["effective_risk_pct"], 0.0, 0.2)
        if base_risk_pct > 0:
            ratio = v / base_risk_pct
            if not (0.25 <= ratio <= 4.0):
                raise ValueError(
                    f"GPT entry response 의 effective_risk_pct={v:.6f} 가 기존 리스크 대비 비정상 비율(ratio={ratio:.2f})입니다."
                )
        # 절대 상한(환경변수)도 한 번 더 체크
        try:
            gpt_max_risk = float(os.getenv("GPT_MAX_RISK_PCT", "0.03"))
        except Exception:
            gpt_max_risk = 0.03
        if v > gpt_max_risk:
            raise ValueError(
                f"GPT entry response 의 effective_risk_pct={v:.6f} 가 GPT_MAX_RISK_PCT={gpt_max_risk:.6f} 를 초과합니다."
            )
        data["effective_risk_pct"] = v

    # guard_adjustments 타입만 간단히 체크
    ga = data.get("guard_adjustments")
    if ga is not None and not isinstance(ga, dict):
        raise ValueError("GPT entry response 의 guard_adjustments 는 객체여야 합니다.")

    return data


def ask_entry_decision(
    *,
    symbol: str,
    signal_source: str,  # 예: "MARKET"
    chosen_signal: str,  # "LONG" / "SHORT"
    last_price: float,
    entry_score: Optional[float],
    effective_risk_pct: float,
    leverage: float,
    tp_pct: float,
    sl_pct: float,
    extra: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """GPT 에게 진입 여부/강도 결정을 compact JSON 기반으로 요청한다.

    반환 형식 (예시):

        {
          "action": "ENTER" | "SKIP" | "ADJUST",
          "reason": "...",
          "tp_pct": 0.006,
          "sl_pct": 0.004,
          "effective_risk_pct": 0.02,
          "guard_adjustments": {
            "min_entry_volume_ratio": 0.25,
            "max_spread_pct": 0.001
          },
          "_meta": {
            "latency_sec": 1.234
          }
        }

    예외:
    - OpenAI API 오류 / 네트워크 오류 / JSON 파싱 오류 / 응답 지연 초과 /
      JSON 스키마 검증 실패 시 RuntimeError 발생.
    """
    client = _get_client()

    model = (
        os.getenv("OPENAI_TRADER_MODEL")
        or os.getenv("GPT_ENTRY_MODEL")
        or "gpt-5.1"
    )
    try:
        max_tokens = int(os.getenv("OPENAI_TRADER_MAX_TOKENS", "192"))
    except Exception:
        max_tokens = 192
    try:
        max_latency = float(os.getenv("OPENAI_TRADER_MAX_LATENCY", "8.0"))
    except Exception:
        max_latency = 8.0
    try:
        soft_latency = float(os.getenv("OPENAI_TRADER_SOFT_LATENCY", str(max_latency)))
    except Exception:
        soft_latency = max_latency

    payload = _build_entry_payload(
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

    user_content = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
    )

    t0 = time.perf_counter()
    try:
        try:
            # 최신 openai-python (Responses API + response_format 지원)
            resp = client.responses.create(
                model=model,
                input=[
                    {"role": "system", "content": _SYSTEM_PROMPT_ENTRY},
                    {"role": "user", "content": user_content},
                ],
                response_format={"type": "json_object"},
                max_output_tokens=max_tokens,
                temperature=0.2,
                timeout=max_latency,
            )
        except TypeError as e:
            # 구버전 openai-python: response_format 인자 미지원
            if "response_format" not in str(e):
                raise
            _safe_log(
                "[GPT_CALL][ENTRY] response_format not supported, retrying without it"
            )
            resp = client.responses.create(
                model=model,
                input=[
                    {"role": "system", "content": _SYSTEM_PROMPT_ENTRY},
                    {"role": "user", "content": user_content},
                ],
                max_output_tokens=max_tokens,
                temperature=0.2,
                timeout=max_latency,
            )
        elapsed = time.perf_counter() - t0
    except Exception as e:
        elapsed = time.perf_counter() - t0
        try:
            _log_gpt_latency_csv(
                kind="ENTRY",
                model=model,
                symbol=symbol,
                source=signal_source,
                direction=chosen_signal,
                latency_sec=elapsed,
                soft_limit_sec=soft_latency,
                hard_limit_sec=max_latency,
                success=False,
                error_message=str(e),
            )
        except Exception:
            pass
        _fatal_log_and_raise("[GPT_ENTRY] 호출 실패", RuntimeError(str(e)))

    # 지연 시간 가드 (하드 타임아웃; soft 기준은 CSV에서 is_slow 플래그로만 사용)
    if elapsed > max_latency:
        try:
            _log_gpt_latency_csv(
                kind="ENTRY",
                model=model,
                symbol=symbol,
                source=signal_source,
                direction=chosen_signal,
                latency_sec=elapsed,
                soft_limit_sec=soft_latency,
                hard_limit_sec=max_latency,
                success=False,
                error_message=f"latency>{max_latency:.2f}s",
            )
        except Exception:
            pass
        _fatal_log_and_raise(
            "[GPT_ENTRY] 응답 지연",
            RuntimeError(f"GPT 응답 지연: {elapsed:.2f}s > {max_latency:.2f}s"),
        )

    # 정상 응답도 CSV 로 남김
    try:
        _log_gpt_latency_csv(
            kind="ENTRY",
            model=model,
            symbol=symbol,
            source=signal_source,
            direction=chosen_signal,
            latency_sec=elapsed,
            soft_limit_sec=soft_latency,
            hard_limit_sec=max_latency,
            success=True,
            error_message=None,
        )
    except Exception:
        pass

    try:
        text = _extract_text_from_response(resp).strip()
    except Exception as e:
        _fatal_log_and_raise("[GPT_ENTRY] 응답 텍스트 추출 실패", RuntimeError(str(e)))

    if not text:
        _fatal_log_and_raise(
            "[GPT_ENTRY] 응답 없음", RuntimeError("GPT 응답이 비어 있습니다.")
        )

    # JSON 파싱 (필요 시 중괄호 부분만 잘라서 재시도)
    def _parse_json(s: str) -> Dict[str, Any]:
        try:
            obj = json.loads(s)
            if not isinstance(obj, Dict):
                raise ValueError("JSON 루트 타입이 dict 가 아닙니다.")
            return obj
        except Exception:
            start = s.find("{")
            end = s.rfind("}")
            if start != -1 and end != -1 and start < end:
                sub = s[start : end + 1]
                obj = json.loads(sub)
                if not isinstance(obj, Dict):
                    raise ValueError("잘라낸 JSON 루트 타입이 dict 가 아닙니다.")
                return obj
            raise

    try:
        data = _parse_json(text)
    except Exception as e:
        preview = text[:200]
        _fatal_log_and_raise(
            "[GPT_ENTRY] JSON 파싱 실패",
            RuntimeError(f"GPT 응답 JSON 파싱 실패: {preview!r} / {e}"),
        )

    # JSON 스키마/값 검증
    try:
        data = _normalize_and_validate_entry_response(
            data,
            base_tp_pct=tp_pct,
            base_sl_pct=sl_pct,
            base_risk_pct=effective_risk_pct,
        )
    except Exception as e:
        _fatal_log_and_raise(
            "[GPT_ENTRY] JSON 검증 실패",
            RuntimeError(f"GPT 진입 응답 검증 실패: {e}"),
        )

    # 메타 정보(지연 시간)를 붙여서 상위에서 참고할 수 있게 한다.
    try:
        meta = data.get("_meta")
        if not isinstance(meta, Dict):
            meta = {}
        meta["latency_sec"] = round(elapsed, 3)
        data["_meta"] = meta
    except Exception:
        # 메타 추가는 필수가 아니므로 실패해도 무시
        pass

    return data


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
    """진입 의사결정용 래퍼.

    - ask_entry_decision 과 동일하되, 실패 시 Render 로그/Telegram 알림을 남기고 예외를 그대로 전파한다.
    - 더 이상 'ENTER' 로의 자동 폴백을 하지 않는다(폴백 금지).
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
        action = str(data.get("action", "")).upper()
        # 여기까지 오면 _normalize_and_validate_entry_response 에서 이미 검증됨
        # 혹시라도 action 이 비정상이면 그대로 에러 처리
        if action not in {"ENTER", "SKIP", "ADJUST"}:
            raise RuntimeError(f"검증 이후에도 action 값이 비정상입니다: {action!r}")

        # 성공 시 Render 로그에 요약 남김
        try:
            new_tp = data.get("tp_pct", tp_pct)
            new_sl = data.get("sl_pct", sl_pct)
            new_risk = data.get("effective_risk_pct", effective_risk_pct)
            latency = None
            meta = data.get("_meta")
            if isinstance(meta, Dict):
                latency = meta.get("latency_sec")
            _safe_log(
                f"[GPT_ENTRY] action={action} tp={new_tp} sl={new_sl} "
                f"risk={new_risk} lat={latency} symbol={symbol} src={signal_source} dir={chosen_signal}"
            )
        except Exception:
            pass

        return action, data
    except Exception as e:  # pragma: no cover - GPT 오류
        try:
            from telelog import log, send_tg  # 순환 import 방지용 지연 import

            log(f"[GPT_ENTRY][ERROR] GPT 진입 판단 호출 실패: {e}")
            try:
                send_tg(
                    "⚠️ GPT 진입 판단 호출에 실패했습니다. "
                    "자동매매 상태와 로그를 확인해 주세요.\n"
                    f"에러: {e}"
                )
            except Exception:
                pass
        except Exception:
            pass
        # 폴백 없이 예외를 그대로 전파
        raise


# ─────────────────────────────────────────
# 2) 청산(포지션 종료) 의사결정
# ─────────────────────────────────────────


def _make_exit_prompt(payload: Dict[str, Any]) -> str:
    """EXIT(청산) 전용 프롬프트를 구성한다.

    payload 예시:
    {
      "symbol": "BTC-USDT",
      "regime": "MARKET" 등 단순 태그,
      "side": "BUY"/"SELL"/"LONG"/"SHORT",
      "scenario": "RANGE_EARLY_TP" 등,
      "last_price": 95000.0,
      "entry_price": 94000.0,
      "leverage": 10,
      "pnl_pct": 0.0105,
      "extra": {
        "market_state": {...},   # RANGE/TREND, boxiness_score, trend_strength, tail_pattern 등
        "thresholds": {...},     # 조기 익절/손절 임계값, 시간 경과 정보 등
        "guards": {...}          # 추가 리스크 플래그 등
      }
    }
    """
    return (
        "당신은 BTC-USDT 선물 자동매매 봇의 '포지션 종료 리스크 컨트롤러'입니다.\n"
        "아래 JSON 상태를 보고, 지금 포지션을 청산할지(CLOSE) 보유할지(HOLD) 결정하세요.\n\n"
        "입력 JSON에는 대략 다음 정보가 포함됩니다.\n"
        "- symbol, regime, side(LONG/SHORT), scenario\n"
        "- entry_price, last_price, leverage, pnl_pct (수익률, 롱 기준 양수=이익, 음수=손실)\n"
        "- extra.market_state: RANGE/TREND 여부, boxiness_score, trend_strength, 꼬리 패턴 등\n"
        "- extra.thresholds: 조기익절/조기손절 기준, 목표 TP/SL, 경과 시간 등\n"
        "- extra.guards / 기타 플래그: 급등락, 변동성 급증, 지원/저항 이탈 여부 등\n\n"
        "리스크 기반 의사결정 가이드:\n"
        "1) pnl_pct 가 충분한 이익 구간이면서\n"
        "   - 박스장(RANGE)이거나, 강한 꼬리 패턴으로 반전 신호가 보이면 → 'CLOSE' 로 이익 확정 선호.\n"
        "   - 강한 추세(TREND) 지속 + 꼬리가 추세 방향을 지지하고, thresholds 상 아직 여유가 있으면\n"
        "     → 'HOLD' 를 고려하되, scenario 가 조기익절(EARLY_TP)라면 보수적으로 판단.\n"
        "2) pnl_pct 가 손실 구간이고, 다음 중 하나라도 만족하면 과감히 'CLOSE' 를 선택합니다.\n"
        "   - 손실이 thresholds 에 정의된 조기손절 한계를 넘었거나, 곧 도달 가능성이 큰 경우\n"
        "   - 시장 상태가 포지션 방향과 정반대(강한 반대 추세, 꼬리 패턴이 일관되게 역방향)인 경우\n"
        "   - 변동성 급증/급락, 지지·저항 명확한 이탈 등 tail risk 가 커진 경우\n"
        "3) pnl_pct 가 아주 작고(근처 노이즈 수준) 시장 상태가 불분명하면\n"
        "   - 불필요한 수수료/노이즈 회피를 위해 'HOLD' 를 우선 고려하되,\n"
        "   - scenario 가 TIME_STOP/만기 관련 등 '정리 필요' 시그널이면 'CLOSE' 를 고려합니다.\n\n"
        "반드시 JSON 하나만 출력합니다.\n"
        "출력 스키마는 다음과 같습니다.\n\n"
        "{\n"
        "  \"action\": \"CLOSE\" 또는 \"HOLD\",\n"
        "  \"reason\": \"여기에 한국어 한 줄 요약\",\n"
        "  \"comment\": \"사람이 이해하기 쉬운 짧은 설명 (선택 사항)\"\n"
        "}\n\n"
        "규칙(아주 중요):\n"
        "- action 은 오직 \"CLOSE\" 또는 \"HOLD\" 중 하나만 사용하십시오.\n"
        "- \"KEEP\" 이라고 표현하고 싶다면 대신 \"HOLD\" 를 사용하십시오.\n"
        "- JSON 바깥에 다른 텍스트를 절대 쓰지 마십시오.\n"
        "- 코드블록 마크다운(예: ```json) 과 주석(//, # 등), NaN/Infinity 는 절대 사용하지 마십시오.\n\n"
        "현재 포지션 상태 JSON:\n"
        + json.dumps(payload, ensure_ascii=False, indent=2)
    )


def _normalize_and_validate_exit_response(data: Dict[str, Any]) -> Dict[str, Any]:
    """GPT 청산 응답 JSON 을 정규화/검증한다.

    - action: 'CLOSE' 또는 'HOLD' 만 허용. 'KEEP' 은 내부적으로 'HOLD' 로 변환.
    - reason/comment: 존재할 경우 문자열 여부만 확인.
    """
    action = data.get("action")
    if not isinstance(action, str):
        raise ValueError("GPT exit response 에 'action' 문자열 필드가 없습니다.")
    action_u = action.strip().upper()
    if action_u == "KEEP":
        action_u = "HOLD"
    if action_u not in {"CLOSE", "HOLD"}:
        raise ValueError(f"GPT exit response 의 action 값이 잘못되었습니다: {action_u!r}")
    data["action"] = action_u

    if "reason" in data and data["reason"] is not None and not isinstance(
        data["reason"], str
    ):
        raise ValueError("GPT exit response 의 reason 은 문자열이어야 합니다.")

    if "comment" in data and data["comment"] is not None and not isinstance(
        data["comment"], str
    ):
        raise ValueError("GPT exit response 의 comment 는 문자열이어야 합니다.")

    return data


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

    - regime/source: 전략 종류 태그. 둘 중 하나만 넘겨도 됨.
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
    try:
        data = _call_gpt_json(
            model=model, prompt=prompt, purpose="exit_decision"
        )
    except Exception as e:
        _fatal_log_and_raise("[GPT_EXIT] 호출/JSON 파싱 실패", RuntimeError(str(e)))

    # JSON 스키마/값 검증
    try:
        data = _normalize_and_validate_exit_response(data)
    except Exception as e:
        _fatal_log_and_raise(
            "[GPT_EXIT] JSON 검증 실패", RuntimeError(f"GPT 청산 응답 검증 실패: {e}")
        )

    return data


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
    """청산(포지션 종료) 의사결정용 래퍼.

    - 정상 응답: (action, json) 을 돌려준다.
      * action 은 항상 "CLOSE" 또는 "HOLD" 로 정규화된다.
    - 비정상 상황(GPT 오류/타임아웃/JSON 형식 오류 등):
      * Render 로그 + Telegram 알림을 남기고 예외를 그대로 전파한다.
    - fallback_action 인자는 더 이상 사용되지 않으며, 향후 제거될 수 있다.
    """
    # 기존 시그니처 유지를 위해 fallback_action 인자를 받지만, 실제 폴백에는 사용하지 않는다.
    _ = fallback_action

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
        raw_action = str(data.get("action", "")).upper()

        if raw_action not in {"CLOSE", "HOLD"}:
            raise RuntimeError(f"검증 이후에도 action 값이 비정상입니다: {raw_action!r}")
        action = raw_action

        # 성공 시 Render 로그에 요약 남김
        try:
            _safe_log(
                f"[GPT_EXIT] scenario={scenario} action={action} "
                f"symbol={symbol} side={side} reason={data.get('reason')}"
            )
        except Exception:
            pass

        return action, data
    except Exception as e:  # pragma: no cover - GPT 오류
        try:
            from telelog import log, send_tg  # 순환 import 방지용 지연 import

            msg = f"[GPT_EXIT][ERROR] GPT 청산 판단 호출 실패: {e}"
            log(msg)
            try:
                send_tg(
                    "⚠️ GPT 청산 판단 호출에 실패했습니다. "
                    "자동매매 상태와 로그를 확인해 주세요.\n"
                    f"에러: {e}"
                )
            except Exception:
                pass
        except Exception:
            pass

        # 폴백 없이 예외를 그대로 전파
        raise


__all__ = [
    "ask_entry_decision",
    "ask_entry_decision_safe",
    "ask_exit_decision",
    "ask_exit_decision_safe",
]
