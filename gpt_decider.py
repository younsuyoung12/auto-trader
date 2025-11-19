from __future__ import annotations

"""
gpt_decider.py
====================================================
역할
----------------------------------------------------
- BingX Auto Trader에서 진입/청산 의사결정을 GPT-5.1에 위임하는 어댑터(브레인 모듈).
- 엔트리 루프 / 포지션 감시 루프에서 이 모듈만 import 해서 사용한다.

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
- symbol, regime(예: "MARKET" 등 단순 태그), direction(LONG/SHORT)
- last_price, entry_score, effective_risk_pct, leverage
- base_tp_pct, base_sl_pct
- guard_snapshot, recent_pnl_pct, skip_streak
- 그 밖에 요약된 지표/리스크 플래그 등이 들어올 수 있습니다.

할 일:
1) action: "ENTER" / "SKIP" / "ADJUST" 중 하나를 고르세요.
   - "ENTER": 지금 조건이면 진입 허용.
   - "SKIP": 지금은 진입하지 말 것.
   - "ADJUST": 진입은 허용하지만, TP/SL/리스크/가드 값을 소폭 조정.

2) 숫자 필드 (옵션):
   - tp_pct: 기본값 근처의 수익 목표 비율 (0보다 크고 0.03~0.12 사이 권장).
   - sl_pct: 기본값 근처의 손절 비율 (0보다 크고 0.01~0.07 사이 권장).
   - effective_risk_pct: 계좌 대비 주문 비율. 0 < effective_risk_pct <= 0.03 정도로 작게 유지.

3) guard_adjustments (옵션):
   - 객체 형태로, 다음 키 중 필요한 것만 포함할 수 있습니다:
     * min_entry_volume_ratio
     * max_spread_pct
     * max_price_jump_pct
     * depth_imbalance_min_ratio
     * depth_imbalance_min_notional
   - 각 값은 현재 guard_snapshot 에서 크게 벗어나지 않는 선에서 소폭 조정합니다.

4) reason:
   - 한국어로 한 줄 정도의 짧은 설명을 남기세요.

형식 규칙:
- 반드시 하나의 JSON 객체만 출력하십시오.
- JSON 바깥에는 아무 텍스트도 쓰지 마십시오.
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
    - OpenAI API 오류 / 네트워크 오류 / JSON 파싱 오류 / 응답 지연 초과 시 RuntimeError 발생.
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
        raise RuntimeError(f"GPT 응답 호출 실패: {e}") from e

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
        raise RuntimeError(f"GPT 응답 지연: {elapsed:.2f}s > {max_latency:.2f}s")

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
        raise RuntimeError(f"GPT 응답 텍스트 추출 실패: {e}") from e

    if not text:
        raise RuntimeError("GPT 응답이 비어 있습니다.")

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
        raise RuntimeError(f"GPT 응답 JSON 파싱 실패: {preview!r}") from e

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
    except Exception as e:  # pragma: no cover - GPT 오류 폴백
        from telelog import log, send_tg  # 순환 import 방지용 지연 import

        log(f"[GPT_ENTRY] 호출 실패, 기존 로직 사용: {e}")
        try:
            send_tg(
                "⚠️ GPT 진입 판단 호출에 실패했습니다. "
                "이번 진입은 기존 규칙으로만 판단합니다."
            )
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
      "regime": "MARKET" 등 단순 태그,
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
        "   - reason: 한국어 한 줄 요약 (예: '조기익절, 목표 수익 도달').\n"
        "   - comment: 사람이 읽을 수 있는 짧은 설명.\n"
        "3) 손익이 크지 않거나 잡음 수준이면 HOLD 를 우선 고려합니다.\n"
        "4) 손실 확대 가능성이 크거나, 전략 규칙상 포지션 유지 의미가 약해 보이면 과감하게 "
        "CLOSE 를 선택합니다.\n\n"
        "현재 포지션 상태 JSON:\n"
        + json.dumps(payload, ensure_ascii=False, indent=2)
        + "\n\n"
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
                f"[GPT_EXIT] scenario={scenario} action={action} "
                f"symbol={symbol} side={side} reason={data.get('reason')}"
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


__all__ = [
    "ask_entry_decision",
    "ask_entry_decision_safe",
    "ask_exit_decision",
    "ask_exit_decision_safe",
]
