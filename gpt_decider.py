from __future__ import annotations

"""
gpt_decider.py
====================================================
역할
----------------------------------------------------
- BingX Auto Trader에서 진입/청산 의사결정을 GPT-5.1에 위임하는 브레인 모듈.
- gpt_trader / position_watch_ws 등에서 이 모듈만 import 해서 사용한다.

2025-12-01 변경 사항 (WS market_features + NaN/None 직렬화 보정)
----------------------------------------------------
1) market_features 및 extra 안의 숫자 필드에서 None/NaN/Infinity 값을 GPT 프롬프트 직전에 0으로 단순 치환해,
   "None"/"NaN" 문자열이 프롬프트에 그대로 노출되지 않도록 했다.
2) pattern_score, trend_strength, range_strength, volatility_score, multi_timeframe signals, risk_flags,
   liquidity_event_score 등 주요 지표가 모두 유한한 숫자로만 GPT 입력에 포함되도록 정규화했다.
3) 지표 계산 단계(indicators.py / market_features_ws.py / entry_flow.py)에서의 '폴백 금지' 정책은 그대로 유지하고,
   gpt_decider.py는 오직 직렬화 포맷 안정화만 담당하도록 분리했다.
4) EXIT(청산) 프롬프트에 포함되는 market_features/extra payload 에도 동일한 NaN/None 보정을 적용해,
   청산 판단 시에도 이상값이 들어가지 않도록 했다.

2025-11-21 변경 사항 (Unified Features + 백필 금지 + 레짐 제거)
----------------------------------------------------
1) unified_features_builder.build_unified_features(...) 가 만든 market_features dict 를
   진입/청산 프롬프트의 핵심 입력으로 사용한다.
2) 박스장/추세장 같은 레짐 태그(RANGE/TREND)를 쓰지 않고,
   trend_strength, boxiness_score, pattern_score 등 숫자 기반 피처만 전달받아 판단한다.
3) market_features 가 None 이거나 빈 dict 인 경우 치명적 오류로 간주하고,
   Render 로그 + Telegram 알림을 남긴 뒤 예외를 그대로 전파한다(백필 금지).
4) GPT 오류/JSON 파싱 오류/지연 초과 시에도 폴백 진입·청산을 하지 않고 예외를 전파한다.
5) EXIT(청산) 프롬프트에 소량 익절, 변동성 감소, 반대 신호 발생 등 실시간 대응 규칙을 명확히 서술했다.
6) ask_exit_decision 호출 시 extra.market_features 가 없거나 비어 있으면 치명적 오류로 처리하고,
   EXIT 측 GPT 호출도 gpt_latency CSV 에 기록되도록 했다.

설계 핵심
----------------------------------------------------
- 입력: unified_features_builder 에서 온 market_features(패턴/지표/MTF/유동성 통합),
  guard_snapshot, recent_pnl_pct/skip_streak 등 경량 JSON.
- 출력: 진입용 action/TP/SL/리스크/가드 조정, 청산용 CLOSE/HOLD 결정(JSON).
- GPT 가 정상 JSON 을 주지 않으면 어떤 주문도 실행되지 않는다.
- GPT 왕복 시간을 logs/gpt_latency/*.csv 에 CSV 로 기록한다.

공개 API
----------------------------------------------------
- ask_entry_decision / ask_entry_decision_safe
- ask_exit_decision / ask_exit_decision_safe
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


def _safe_tg(msg: str) -> None:
    """텔레그램 알림을 보낼 수 있으면 보내고, 실패해도 조용히 무시한다."""
    try:
        from telelog import send_tg

        send_tg(msg)
    except Exception:
        pass


def _fatal_log_and_raise(prefix: str, exc: Exception) -> None:
    """치명적인 오류를 Render 로그와 Telegram 에 남기고 예외를 다시 던진다."""
    try:
        from telelog import log

        log(f"{prefix}: {exc}")
        _safe_tg(
            "⚠️ GPT 브레인 모듈 치명적 오류 발생\n"
            f"위치: {prefix}\n"
            f"에러: {exc}"
        )
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


def _sanitize_for_gpt(obj: Any, *, _depth: int = 0, _max_depth: int = 8) -> Any:
    """GPT 프롬프트에 들어가는 payload 에서 None/NaN/Infinity 를 정규화한다.

    - 지표 계산 단계의 값을 '추정해서 보정'하지 않고,
      GPT 가 읽는 JSON 직렬화 포맷을 안정화하는 용도만 담당한다.
    - 규칙:
      * None → 0
      * float 가 NaN/Infinity → 0.0
      * dict/list/tuple 는 재귀적으로 동일 규칙 적용
      * 그 외(str/bool 등)는 그대로 둔다.
    """
    if _depth > _max_depth:
        # 너무 깊은 중첩은 그대로 두되, 상위에서 이미 주요 지표는 정규화된 상태라고 가정한다.
        return obj

    if obj is None:
        return 0

    # float 인 경우 유한 실수만 허용
    if isinstance(obj, float):
        return float(obj) if math.isfinite(obj) else 0.0

    # int/bool 은 그대로 사용
    if isinstance(obj, (int, bool)):
        return obj

    # dict 는 각 값에 재귀 적용
    if isinstance(obj, dict):
        return {
            k: _sanitize_for_gpt(v, _depth=_depth + 1, _max_depth=_max_depth)
            for k, v in obj.items()
        }

    # list/tuple 등 시퀀스는 요소별 재귀 적용
    if isinstance(obj, (list, tuple)):
        return [
            _sanitize_for_gpt(v, _depth=_depth + 1, _max_depth=_max_depth) for v in obj
        ]

    # 문자열/그 외 타입은 그대로 둔다.
    return obj


# ─────────────────────────────────────────
# GPT 시스템 프롬프트 (진입용)
# ─────────────────────────────────────────

_SYSTEM_PROMPT_ENTRY = """
당신은 비트코인 선물 자동매매 시스템의 '진입 허용 여부'만 판단하는 작은 에이전트입니다.

입력으로 하나의 JSON 객체를 받습니다. 이 JSON 에는 대략 다음 정보가 들어 있습니다:
- symbol: "BTC-USDT" 등 거래 종목
- regime: 예) "MARKET" · 세션/전략 태그(단순 문자열)
- direction: "LONG" / "SHORT"
- last_price: 현재 가격
- entry_score: 진입 점수(높을수록 유리한 방향일 가능성)
- effective_risk_pct: 현재 기본 주문 비율
- leverage: 레버리지 배율
- base_tp_pct, base_sl_pct: 기본 TP/SL 비율
- guard_snapshot: 거래량·스프레드·점프·호가 불균형 등 각종 가드 값
- recent_pnl_pct, skip_streak: 최근 손익과 연속 손실/스킵 횟수 등 리스크 컨텍스트
- market_features: unified_features_builder 가 만든 시장 상태·패턴 요약 dict
  * timeframes: 1m/5m/15m 가격·거래량·변동성 피처(숫자 위주)
  * pattern_features: pattern_score, trend_strength, range_strength(또는 boxiness_score), volatility_score, reversal_probability, continuation_probability,
    momentum_score, volume_confirmation, wick_strength, liquidity_event_score 등
  * pattern_summary: 사람이 읽기 쉬운 한 줄 설명
  * risk_flags / liquidity 정보: 멀티타임프레임 신호, 갑작스러운 변동성, 유동성 스윕 등

당신의 목표는:
- "지금 이 방향으로 진입하는 것이 합리적인지"를,
- 시장 피처(market_features)와 최근 리스크 상태를 함께 고려해서 결정하는 것입니다.

1) action 선택
----------------------------------------
action 필드는 반드시 다음 중 하나여야 합니다.
- "ENTER": 지금 조건이면 진입 허용.
- "SKIP": 지금은 진입하지 말 것.
- "ADJUST": 진입은 허용하지만, TP/SL/리스크/가드 값을 소폭 조정.

선택 가이드:
- trend_strength, momentum_score, volume_confirmation 이 모두 포지션 방향을 지지하고
  패턴 관련 점수(pattern_score, continuation_probability 등)가 높다면
  → ENTER 또는 ADJUST 를 고려할 수 있습니다.
- boxiness_score 가 크고 trend_strength 가 작으며, 패턴/유동성 신호가 혼재되어
  방향성이 뚜렷하지 않다면
  → 작은 TP, 타이트한 SL, 낮은 리스크를 선호하고
    entry_score 가 낮거나 패턴 신뢰도가 떨어지면 SKIP 을 더 자주 선택합니다.
- 최근 손실이 누적(recent_pnl_pct 가 크게 음수, skip_streak/연속 손실 횟수 증가)된 상태에서는
  → 리스크를 줄이거나(SKIP/ADJUST) 과도한 레버리지 진입을 피해야 합니다.
- ADJUST 는 방향은 맞다고 보지만,
  → base_tp_pct / base_sl_pct / effective_risk_pct 를 "작게 미세 조정"할 때만 사용합니다.
  → 기본값 대비 ±50% 범위를 크게 벗어나지 않는다고 가정하고 설계합니다.
  → 최근 손실 구간에서는 ADJUST 보다는 SKIP 을 우선 고려합니다.

2) 숫자 필드 (옵션)
----------------------------------------
다음 숫자 필드는 필요할 때만 넣습니다. 없으면 기본값(base_*)을 그대로 사용한다고 가정합니다.
- tp_pct:
  * 0 < tp_pct <= 0.12 범위 권장 (아주 예외적인 경우에도 0.20 을 넘지 말 것).
  * 변동성이 크지 않거나 패턴 확신이 낮을 때는 base_tp_pct 보다 작게 조정하는 것을 우선 고려.
- sl_pct:
  * 0 < sl_pct <= 0.07 범위 권장 (예외 상황에서도 0.15 를 넘지 말 것).
  * 노이즈 구간에서는 SL 을 너무 넓게 두지 말고, 구조가 불리하게 무너졌다고 판단되면
    진입 자체를 SKIP 하는 것을 고려합니다.
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
- guard_snapshot 및 market_features 에서 보이는 위험 신호를 기준으로
  "조금 더 보수적으로" 조정하는 용도로만 사용합니다.
- 갑자기 스프레드를 크게 허용하거나, price_jump 한계를 과도하게 늘리지는 마십시오.

4) reason (필수)
----------------------------------------
- 한국어로 1~2줄 정도의 짧은 설명을 넣습니다.
- 예: "상승 모멘텀과 아래꼬리·거래량이 강해 롱 진입 허용, TP 소폭 확대"

5) 출력 JSON 형식 (템플릿 예시)
----------------------------------------
반드시 아래와 같은 JSON 스키마를 따릅니다. 필요 없는 키는 생략해도 됩니다.

{
  "action": "ENTER",
  "reason": "상승 모멘텀과 아래꼬리·거래량이 강해 롱 진입 허용, TP 소폭 확대",
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
    "pattern_note": "상승 모멘텀 + 아래꼬리 강함",
    "market_comment": "단기 저항 재돌파 시도, 위쪽 유동성에 매물 집중"
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

    elapsed: float = 0.0
    text: str = ""
    kind = (purpose or "OTHER").upper()

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
        # EXIT 등 공통 GPT 호출도 레이턴시 CSV 에 기록
        try:
            _log_gpt_latency_csv(
                kind=kind,
                model=model,
                symbol=None,
                source=None,
                direction=None,
                latency_sec=elapsed,
                soft_limit_sec=timeout_sec,
                hard_limit_sec=timeout_sec,
                success=True,
                error_message=None,
            )
        except Exception:
            pass
    except Exception as e:  # pragma: no cover - GPT 오류는 호출측에서 처리
        elapsed = time.time() - t0
        try:
            _log_gpt_latency_csv(
                kind=kind,
                model=model,
                symbol=None,
                source=None,
                direction=None,
                latency_sec=elapsed,
                soft_limit_sec=timeout_sec,
                hard_limit_sec=timeout_sec,
                success=False,
                error_message=str(e),
            )
        except Exception:
            pass
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
    market_features: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """GPT에 넘길 compact JSON payload 구성.

    - extra(dict)는 gpt_trader 에서 이미 경량화가 끝난 상태라고 가정.
    - market_features(dict)는 unified_features_builder 에서 만든
      시장 상태·패턴/지표/MTF 요약 dict 라고 가정한다.
    """
    payload: Dict[str, Any] = {}

    if isinstance(extra, Dict) and extra:
        payload.update(extra)

    if market_features is not None:
        if not isinstance(market_features, Dict) or not market_features:
            raise RuntimeError(
                "market_features 가 비어 있거나 dict 가 아닙니다. "
                "unified_features_builder 결과를 확인하세요."
            )
        # GPT 가 전체 시장 상태를 한 번에 볼 수 있도록 별도 키로 붙인다.
        payload["market_features"] = market_features
        # 사람이 이해하기 쉬운 한 줄 요약이 있으면 최상위에도 복사
        pattern_summary = market_features.get("pattern_summary")
        if isinstance(pattern_summary, str) and pattern_summary:
            payload.setdefault("pattern_summary", pattern_summary)

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
            if not (0.02 <= ratio <= 4.0):
                raise ValueError(
                    f"GPT entry response 의 tp_pct={v:.6f} 가 base_tp_pct 대비 비정상 비율(ratio={ratio:.2f})입니다."
                )
        data["tp_pct"] = v

    if "sl_pct" in data and data.get("sl_pct") is not None:
        v = _check_float("sl_pct", data["sl_pct"], 0.0, 0.5)
        if base_sl_pct > 0:
            ratio = v / base_sl_pct
            if not (0.02 <= ratio <= 4.0):
                raise ValueError(
                    f"GPT entry response 의 sl_pct={v:.6f} 가 base_sl_pct 대비 비정상 비율(ratio={ratio:.2f})입니다."
                )
        data["sl_pct"] = v

    if "effective_risk_pct" in data and data.get("effective_risk_pct") is not None:
        v = _check_float("effective_risk_pct", data["effective_risk_pct"], 0.0, 0.2)
        if base_risk_pct > 0:
            ratio = v / base_risk_pct
            if not (0.02 <= ratio <= 4.0):
                raise ValueError(
                    "GPT entry response 의 effective_risk_pct="
                    f"{v:.6f} 가 기존 리스크 대비 비정상 비율(ratio={ratio:.2f})입니다."
                )
        # 절대 상한(환경변수)도 한 번 더 체크
        try:
            gpt_max_risk = float(os.getenv("GPT_MAX_RISK_PCT", "0.03"))
        except Exception:
            gpt_max_risk = 0.03
        if v > gpt_max_risk:
            raise ValueError(
                "GPT entry response 의 effective_risk_pct="
                f"{v:.6f} 가 GPT_MAX_RISK_PCT={gpt_max_risk:.6f} 를 초과합니다."
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
    market_features: Optional[Dict[str, Any]],
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
      JSON 스키마 검증 실패 / market_features 이상 시 RuntimeError 발생.
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

    # market_features 는 필수라고 간주 (백필 금지)
    if market_features is None:
        raise RuntimeError(
            "ask_entry_decision 호출 시 market_features 가 None 입니다. "
            "unified_features_builder 파이프라인을 확인하세요."
        )

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
        market_features=market_features,
    )

    # NaN/None/Infinity 방지용으로 payload 를 한 번 정규화해서 GPT 에 전달한다.
    sanitized_payload = _sanitize_for_gpt(payload)

    user_content = json.dumps(
        sanitized_payload,
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
            "[GPT_ENTRY] 응답 없음", RuntimeError("GPT 응답이 비어 있습니다."),
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
    market_features: Optional[Dict[str, Any]],
) -> Tuple[str, Dict[str, Any]]:
    """진입 의사결정용 래퍼.

    - ask_entry_decision 과 동일하되, 실패 시 Render 로그/Telegram 알림을 남기고 예외를 그대로 전파한다.
    - 폴백 진입(ENTER/CLOSE 자동 선택)을 하지 않는다.
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
            market_features=market_features,
        )
        action = str(data.get("action", "")).upper()
        # 여기까지 오면 _normalize_and_validate_entry_response 에서 이미 검증됨
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
                f"risk={new_risk} lat={latency} symbol={symbol} "
                f"src={signal_source} dir={chosen_signal}"
            )
        except Exception:
            pass

        return action, data
    except Exception as e:  # pragma: no cover - GPT 오류
        try:
            from telelog import log  # 순환 import 방지용 지연 import

            log(f"[GPT_ENTRY][ERROR] GPT 진입 판단 호출 실패: {e}")
            _safe_tg(
                "⚠️ GPT 진입 판단 호출에 실패했습니다. "
                "자동매매 상태와 로그를 확인해 주세요.\n"
                f"에러: {e}"
            )
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
      "scenario": "RUNTIME_EXIT_CHECK" / "OPPOSITE_SIGNAL" / "SMALL_PROFIT_PROTECT" 등,
      "last_price": 95000.0,
      "entry_price": 94000.0,
      "leverage": 10,
      "pnl_pct": 0.0105,
      "extra": {
        "market_features": {...},   # unified_features_builder 가 만든 시장/패턴/지표 피처
        "thresholds": {...},       # 조기 익절/손절 임계값, 시간 경과 정보 등
        "guards": {...}            # 추가 리스크 플래그 등
      }
    }
    """
    return (
        "당신은 BTC-USDT 선물 자동매매 봇의 '포지션 종료 전담 리스크 컨트롤러'입니다.\n"
        "아래 JSON 상태를 보고, 지금 포지션을 청산할지(CLOSE) 보유할지(HOLD) 결정하세요.\n\n"
        "입력 JSON에는 대략 다음 정보가 포함됩니다.\n"
        "- symbol: 거래 종목\n"
        "- regime: 단순 태그 (예: 'MARKET')\n"
        "- side: LONG/SHORT (BUY/SELL 포함)\n"
        "- scenario: 호출 의도 (예: 'RUNTIME_EXIT_CHECK', 'OPPOSITE_SIGNAL', 'SMALL_PROFIT_PROTECT', 'VOLATILITY_DROP', 'TIME_STOP' 등)\n"
        "- entry_price, last_price, leverage\n"
        "- pnl_pct: 롱 기준 수익률 (양수=이익, 음수=손실)\n"
        "- extra.market_features: 모멘텀/변동성/패턴 점수, wick 구조, 유동성 이벤트 등\n"
        "- extra.thresholds: 조기익절/조기손절 기준, 목표 TP/SL, 경과 시간, small_profit 범위 등\n"
        "- extra.guards 및 기타 플래그: 급등락, 변동성 급증/급락, 지지·저항 이탈 여부 등\n\n"
        "청산 의사결정 기본 가이드:\n"
        "1) 충분한 이익 구간 (pnl_pct 가 thresholds.small_tp_max 이상 또는 목표 TP 근처)에서는 다음을 고려합니다.\n"
        "   - pattern_features 와 market_features 가 '되돌림 위험'(반전 패턴, 강한 윗꼬리/아랫꼬리, 유동성 스윕) 을 강하게 보여주면 → 이익을 확정하기 위해 'CLOSE' 를 선호합니다.\n"
        "   - 모멘텀/추세 점수가 여전히 포지션 방향을 강하게 지지하고, 변동성이 살아 있으며, thresholds 가 허용하는 경우 → 'HOLD' 를 허용할 수 있습니다.\n\n"
        "2) 소량 이익 구간 (0 < pnl_pct < thresholds.small_tp_max 근처)에서는:\n"
        "   - scenario 가 'SMALL_PROFIT_PROTECT' 이거나, 변동성이 죽고(boxiness 증가, momentum 약화) 방향성이 애매해진 경우 → 작은 이익이라도 지키기 위해 'CLOSE' 쪽으로 기울어야 합니다.\n"
        "   - 모멘텀/유동성이 여전히 포지션 방향을 지지하고, reversal 신호가 약하다면 → 'HOLD' 를 잠시 더 허용할 수 있습니다.\n\n"
        "3) 손실 구간 (pnl_pct < 0)에서는:\n"
        "   - thresholds.max_loss 또는 조기손절 기준에 도달했거나 가까운 경우 → 과감히 'CLOSE' 를 선택합니다.\n"
        "   - market_features 가 모멘텀/패턴/유동성 측면에서 일관되게 포지션 반대 방향을 지지하는 경우 → 손실이 작더라도 'CLOSE' 를 우선합니다.\n"
        "   - 아직 명확한 방향 전환 없이 노이즈 수준이고, thresholds 가 더 허용한다면 → 'HOLD' 도 고려할 수 있습니다.\n\n"
        "4) scenario 별 우선 규칙:\n"
        "   - 'OPPOSITE_SIGNAL': 상위/동일 타임프레임에서 강한 반대 방향 신호가 감지된 상황이면 기본 선택은 'CLOSE' 입니다.\n"
        "   - 'VOLATILITY_DROP' 또는 유사 태그: 급등/급락 이후 변동성이 죽고(박스화), 방향성 에지가 약해졌다면 소량 이익이라도 'CLOSE' 를 선호합니다.\n"
        "   - 'RUNTIME_EXIT_CHECK': 주기적(예: 10초) 점검용이며, 위 1~3번 규칙을 그대로 적용해 합리적인 리스크 관점에서 CLOSE/HOLD 를 선택합니다.\n"
        "   - 'TIME_STOP' / 만기/강제정리 관련 태그: 시간이 충분히 경과했고 더 이상 가져갈 이유가 없으면 손익이 작아도 'CLOSE' 를 고려합니다.\n\n"
        "5) TP/SL 과의 관계:\n"
        "   - 기존 TP/SL 은 '참고 값'일 뿐, 시장 구조와 현재 리스크를 보고 필요하면 그 전에 'CLOSE' 를 선택해야 합니다.\n"
        "   - 손절가(SL)에 도달하기 전이라도 구조가 명확히 깨졌다면 과감히 'CLOSE' 합니다.\n"
        "   - TP 에 근접했지만 되돌림 위험이 크다면 TP 까지 기다리지 말고 'CLOSE' 로 이익을 확정합니다.\n\n"
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
        + json.dumps(
            _sanitize_for_gpt(payload),
            ensure_ascii=False,
            indent=2,
        )
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
    scenario: str,  # "EARLY_TP" / "EARLY_EXIT" / "OPPOSITE_SIGNAL" 등
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
    - scenario: 호출 위치/의도 구분용 태그 (ex: "EARLY_TP")
    - extra:  캔들/거래량/임계값/시장 피처 등 추가 컨텍스트
    """
    model = os.getenv("GPT_EXIT_MODEL", "gpt-5.1-mini")

    # unified_features 기반 market_features 는 EXIT 에서도 필수 (백필 금지)
    if extra is None or not isinstance(extra, dict):
        raise RuntimeError(
            "ask_exit_decision 호출 시 extra 가 None 이거나 dict 가 아닙니다. "
            "position_watch_ws → unified_features_builder 파이프라인을 확인하세요."
        )
    mf = extra.get("market_features")
    if not isinstance(mf, dict) or not mf:
        raise RuntimeError(
            "ask_exit_decision.extra.market_features 가 비어 있습니다. "
            "EXIT 호출 시 unified_features_builder 에서 만든 market_features 를 반드시 포함해야 합니다."
        )

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
            model=model, prompt=prompt, purpose="EXIT_DECISION"
        )
    except Exception as e:
        _fatal_log_and_raise("[GPT_EXIT] 호출/JSON 파싱 실패", RuntimeError(str(e)))

    # JSON 스키마/값 검증
    try:
        data = _normalize_and_validate_exit_response(data)
    except Exception as e:
        _fatal_log_and_raise(
            "[GPT_EXIT] JSON 검증 실패", RuntimeError(f"GPT 청산 응답 검증 실패: {e}"),
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
            from telelog import log  # 순환 import 방지용 지연 import

            msg = f"[GPT_EXIT][ERROR] GPT 청산 판단 호출 실패: {e}"
            log(msg)
            _safe_tg(
                "⚠️ GPT 청산 판단 호출에 실패했습니다. "
                "자동매매 상태와 로그를 확인해 주세요.\n"
                f"에러: {e}"
            )
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
