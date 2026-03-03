from __future__ import annotations

"""
========================================================
FILE: strategy/gpt_supervisor.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
역할
- GPT를 "중간 브레인(Strategy Supervisor)"로 사용한다.
  1) Logical Auditor: 수학 엔진(Quant) 산출물의 모순/과열/비정상 조합을 감지해 라벨링
  2) Narration: 대중 언어 해설(짧게, 반복 최소화, 확정 표현 금지)
  3) Post-mortem: 청산 후 원인 라벨(확률 손실 vs 전략 오류 vs 실행 오류)

중요 (Decision Reconciliation 계약)
- GPT는 절대 매매 결정을 내리지 않는다. (ENTER/HOLD/EXIT 지시 금지)
- GPT 출력은 오직 "감사 라벨 + 설명 문장 + 제안(페널티/리스크 조정 힌트)" 형태다.
- 최종 결정(ENTER/HOLD/EXIT, 사이징, TP/SL)은 Quant Engine의 규칙으로만 확정한다.

절대 원칙 (STRICT · NO-FALLBACK)
- 입력(unified_features / quant_decision_pre / constraints) 누락/형식 오류 → 즉시 예외
- GPT 응답이 JSON 스키마 위반/금지 표현 포함/금지 키 포함 → 즉시 예외
- 실패 시 "대충 통과" / "기본값 보정" / "None 리턴" 금지
  (Caller가 이 모듈 호출을 optional로 취급하는 것은 허용. 이 파일은 폴백을 만들지 않는다.)

보안
- API key/secret, signature, 주문ID 등 민감정보를 GPT에 전달하지 않는다.
- 로그/예외 메시지에도 민감정보를 포함하지 않는다.

변경 이력
--------------------------------------------------------
- 2026-03-03:
  1) 신규 생성: GPT Supervisor(감사/해설/사후감사) JSON 계약 + STRICT 검증/금지 규칙 적용
  2) 반복 방지: 최근 메시지와 유사도(간단 토큰 Jaccard)로 2회까지 재생성 시도(동일 의미 변주)
========================================================
"""

import json
import math
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple

from openai import OpenAI

from settings import load_settings

try:
    from infra.telelog import log
except Exception as e:  # pragma: no cover
    raise RuntimeError("infra.telelog import failed (STRICT · NO-FALLBACK · PRODUCTION MODE)") from e


SET = load_settings()

# -----------------------------------------------------------------------------
# Policy / constants (STRICT)
# -----------------------------------------------------------------------------
DEFAULT_MODEL = os.getenv("OPENAI_SUPERVISOR_MODEL", os.getenv("OPENAI_TRADER_MODEL", "gpt-4o-mini"))
MAX_LATENCY_SEC = float(os.getenv("OPENAI_SUPERVISOR_MAX_LATENCY", os.getenv("OPENAI_TRADER_MAX_LATENCY", "12")))
MAX_TOKENS = int(os.getenv("OPENAI_SUPERVISOR_MAX_TOKENS", os.getenv("OPENAI_TRADER_MAX_TOKENS", "512")))
TEMPERATURE = float(os.getenv("OPENAI_SUPERVISOR_TEMPERATURE", "0.2"))

# 금지어/금지 패턴(투자 권유/확정 표현/공격 표현 최소 세트)
_PROHIBITED_SUBSTRINGS: Tuple[str, ...] = (
    "무조건",
    "확실",
    "100%",
    "보장",
    "사세요",
    "매수하세요",
    "팔세요",
    "매도하세요",
    "따라하세요",
    "지금 사",
    "지금 팔",
)

# GPT가 "결정"을 직접 내리려는 키 금지
_FORBIDDEN_TOP_KEYS: Tuple[str, ...] = (
    "action",
    "decision",
    "enter",
    "exit",
    "hold",
    "buy",
    "sell",
    "position_size",
    "leverage",
    "tp",
    "sl",
)

_TAG_RE = re.compile(r"^[a-z0-9_]{1,32}$")


class GptSupervisorError(RuntimeError):
    """Supervisor 단계(입력/응답/정책 위반) 오류."""


@dataclass(frozen=True, slots=True)
class Auditor:
    severity: int  # 0~3
    tags: Tuple[str, ...]
    rationale_short: str
    confidence_penalty: float  # 0.0~1.0 (1.0 = penalty 없음), Quant가 규칙으로만 반영
    suggested_risk_multiplier: float  # 0.0~1.0 (단순 제안), Quant가 규칙으로만 반영


@dataclass(frozen=True, slots=True)
class Narration:
    title: str
    message: str
    tone: str  # e.g. calm/neutral/caution


@dataclass(frozen=True, slots=True)
class Postmortem:
    kind: Literal["probabilistic_loss", "strategy_issue", "execution_issue"]
    notes: str


@dataclass(frozen=True, slots=True)
class SupervisorResult:
    decision_id: str
    event_type: str
    auditor: Auditor
    narration: Optional[Narration]
    postmortem: Optional[Postmortem]
    raw_text: str  # 원문(민감정보 없음)


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
        raise GptSupervisorError("OPENAI_API_KEY missing (STRICT)")
    _CLIENT = OpenAI(api_key=api_key)
    return _CLIENT


# -----------------------------------------------------------------------------
# Strict helpers
# -----------------------------------------------------------------------------
def _fail(stage: str, reason: str, exc: Optional[BaseException] = None) -> None:
    msg = f"[GPT-SUPERVISOR] {stage} 실패: {reason}"
    log(msg)
    if exc is None:
        raise GptSupervisorError(msg)
    raise GptSupervisorError(msg) from exc


def _require_nonempty_str(stage: str, v: Any, name: str) -> str:
    if v is None:
        _fail(stage, f"{name} is None")
    s = str(v).strip()
    if not s:
        _fail(stage, f"{name} is empty")
    return s


def _require_dict(stage: str, v: Any, name: str) -> Dict[str, Any]:
    if not isinstance(v, dict):
        _fail(stage, f"{name} must be dict (got={type(v)})")
    if not v:
        _fail(stage, f"{name} is empty")
    return v


def _require_list(stage: str, v: Any, name: str, *, min_len: int = 0) -> List[Any]:
    if not isinstance(v, list):
        _fail(stage, f"{name} must be list (got={type(v)})")
    if len(v) < min_len:
        _fail(stage, f"{name} length<{min_len} (got={len(v)})")
    return v


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


def _require_range(stage: str, v: float, name: str, lo: float, hi: float) -> float:
    if v < lo or v > hi:
        _fail(stage, f"{name} out of range [{lo},{hi}] (got={v})")
    return v


def _extract_first_json_object(text: str) -> Dict[str, Any]:
    """
    STRICT: 응답에서 첫 JSON object를 추출한다.
    - 코드블록/앞뒤 텍스트가 있어도 첫 {...} 블록을 찾아 파싱
    """
    s = text.strip()
    if not s:
        _fail("parse", "empty response text")

    # 빠른 경로: 전체가 JSON
    if s.startswith("{") and s.endswith("}"):
        try:
            obj = json.loads(s)
        except Exception as e:
            _fail("parse", "json.loads failed (full text)", e)
        if not isinstance(obj, dict):
            _fail("parse", f"json root must be object (got={type(obj)})")
        return obj

    # 브레이스 스캔
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


def _contains_prohibited(text: str) -> Optional[str]:
    t = (text or "").strip()
    if not t:
        return None
    for bad in _PROHIBITED_SUBSTRINGS:
        if bad in t:
            return bad
    return None


def _jaccard(a: str, b: str) -> float:
    """
    초간단 유사도(토큰 Jaccard). 외부 의존성 없이 반복 방지용.
    """
    def tok(x: str) -> set[str]:
        x = re.sub(r"[^0-9A-Za-z가-힣\s]", " ", x)
        parts = [p for p in x.lower().split() if p]
        return set(parts)

    sa = tok(a)
    sb = tok(b)
    if not sa and not sb:
        return 1.0
    if not sa or not sb:
        return 0.0
    inter = len(sa & sb)
    uni = len(sa | sb)
    return inter / float(uni)


# -----------------------------------------------------------------------------
# Prompt / payload builder (STRICT)
# -----------------------------------------------------------------------------
def _compact_unified_features(unified: Dict[str, Any]) -> Dict[str, Any]:
    """
    GPT에는 원본 캔들 전체를 던지지 않는다.
    unified_features에서 필요한 최소 요약만 뽑는다(STRICT 키 검증).
    """
    stage = "payload"
    symbol = _require_nonempty_str(stage, unified.get("symbol"), "symbol") if "symbol" in unified else ""
    # unified_features_builder가 symbol을 최상위에 싣지 않는 경우가 있어 timeframes 쪽에서 보완
    if not symbol:
        # STRICT: timeframes 없으면 unified 자체가 잘못
        tfs = _require_dict(stage, unified.get("timeframes"), "timeframes")
        # 1m의 last_close가 있는 경우 사용(심볼은 caller가 별도 전달하는 게 정석)
        _ = tfs  # only validation

    engine_scores = _require_dict(stage, unified.get("engine_scores"), "engine_scores")
    total = _require_dict(stage, engine_scores.get("total"), "engine_scores.total")
    total_score = _require_float(stage, total.get("score"), "engine_scores.total.score")
    _require_range(stage, total_score, "engine_scores.total.score", 0.0, 100.0)

    orderbook = _require_dict(stage, unified.get("orderbook"), "orderbook")
    spread_pct = _require_float(stage, orderbook.get("spread_pct"), "orderbook.spread_pct")
    depth_imbalance = _require_float(stage, orderbook.get("depth_imbalance"), "orderbook.depth_imbalance")

    pattern_summary = _require_dict(stage, unified.get("pattern_summary"), "pattern_summary")
    ps_score = _require_float(stage, pattern_summary.get("pattern_score"), "pattern_summary.pattern_score")
    _require_range(stage, ps_score, "pattern_summary.pattern_score", 0.0, 1.0)

    micro = _require_dict(stage, unified.get("microstructure"), "microstructure")
    di = _require_float(stage, micro.get("distortion_index"), "microstructure.distortion_index")
    micro_risk = _require_float(stage, micro.get("micro_score_risk"), "microstructure.micro_score_risk")
    _require_range(stage, micro_risk, "microstructure.micro_score_risk", 0.0, 100.0)

    return {
        "engine": {
            "engine_total_score": total_score,
            "band_scores": {
                "trend_4h": _require_float(stage, engine_scores.get("trend_4h", {}).get("score"), "engine_scores.trend_4h.score")
                if isinstance(engine_scores.get("trend_4h"), dict) else _fail(stage, "engine_scores.trend_4h missing/invalid"),
                "momentum_1h": _require_float(stage, engine_scores.get("momentum_1h", {}).get("score"), "engine_scores.momentum_1h.score")
                if isinstance(engine_scores.get("momentum_1h"), dict) else _fail(stage, "engine_scores.momentum_1h missing/invalid"),
                "structure_15m": _require_float(stage, engine_scores.get("structure_15m", {}).get("score"), "engine_scores.structure_15m.score")
                if isinstance(engine_scores.get("structure_15m"), dict) else _fail(stage, "engine_scores.structure_15m missing/invalid"),
                "timing_5m": _require_float(stage, engine_scores.get("timing_5m", {}).get("score"), "engine_scores.timing_5m.score")
                if isinstance(engine_scores.get("timing_5m"), dict) else _fail(stage, "engine_scores.timing_5m missing/invalid"),
                "orderbook_micro": _require_float(stage, engine_scores.get("orderbook_micro", {}).get("score"), "engine_scores.orderbook_micro.score")
                if isinstance(engine_scores.get("orderbook_micro"), dict) else _fail(stage, "engine_scores.orderbook_micro missing/invalid"),
            },
        },
        "orderbook": {
            "spread_pct": spread_pct,
            "depth_imbalance": depth_imbalance,
        },
        "pattern": {
            "pattern_score": ps_score,
            "best_pattern": _require_nonempty_str(stage, pattern_summary.get("best_pattern"), "pattern_summary.best_pattern"),
            "best_direction": _require_nonempty_str(stage, pattern_summary.get("best_pattern_direction"), "pattern_summary.best_pattern_direction"),
            "best_timeframe": _require_nonempty_str(stage, pattern_summary.get("best_timeframe"), "pattern_summary.best_timeframe"),
        },
        "microstructure": {
            "distortion_index": di,
            "micro_score_risk": micro_risk,
            "funding_z": _require_float(stage, micro.get("funding_z"), "microstructure.funding_z"),
            "oi_z": _require_float(stage, micro.get("oi_z"), "microstructure.oi_z"),
            "lsr_z": _require_float(stage, micro.get("lsr_z"), "microstructure.lsr_z"),
        },
    }


def _build_system_prompt() -> str:
    return (
        "You are GPT Strategy Supervisor for a crypto futures trading system.\n"
        "You DO NOT make trading decisions. You only audit and explain.\n"
        "Return STRICT JSON only (single JSON object). No markdown.\n\n"
        "Hard rules:\n"
        "- Never output ENTER/HOLD/EXIT/BUY/SELL decisions.\n"
        "- Never output position sizing, leverage, TP/SL values.\n"
        "- No investment advice. No commands like 'buy now'/'sell now'.\n"
        "- No guaranteed language (no '100%', 'sure', 'must').\n"
        "- Keep narration in 1-2 short sentences in Korean, easy words, no jargon.\n\n"
        "Output schema:\n"
        "{\n"
        '  "auditor": {\n'
        '    "severity": 0|1|2|3,\n'
        '    "tags": ["snake_case", ...],\n'
        '    "rationale_short": "short reason",\n'
        '    "confidence_penalty": 0.0..1.0,\n'
        '    "suggested_risk_multiplier": 0.0..1.0\n'
        "  },\n"
        '  "narration": {"title": "string", "message": "string", "tone": "calm|neutral|caution"} | null,\n'
        '  "postmortem": {"kind": "probabilistic_loss|strategy_issue|execution_issue", "notes": "string"} | null\n'
        "}\n"
    )


def _build_user_payload(
    *,
    decision_id: str,
    event_type: str,
    unified_features: Dict[str, Any],
    quant_decision_pre: Dict[str, Any],
    quant_constraints: Dict[str, Any],
    recent_messages: Optional[Sequence[str]],
    force_rephrase: bool,
) -> Dict[str, Any]:
    stage = "payload"
    did = _require_nonempty_str(stage, decision_id, "decision_id")
    et = _require_nonempty_str(stage, event_type, "event_type")
    uf = _require_dict(stage, unified_features, "unified_features")
    qpre = _require_dict(stage, quant_decision_pre, "quant_decision_pre")
    qcon = _require_dict(stage, quant_constraints, "quant_constraints")

    compact = _compact_unified_features(uf)

    recent: List[str] = []
    if recent_messages is not None:
        for i, m in enumerate(recent_messages):
            s = str(m).strip()
            if not s:
                _fail(stage, f"recent_messages[{i}] empty")
            recent.append(s)
        if len(recent) > 50:
            _fail(stage, "recent_messages too many (>50)")

    return {
        "decision_id": did,
        "event_type": et,
        "gpt_audit_only": True,
        "force_rephrase": bool(force_rephrase),
        "recent_messages": recent,
        "quant_decision_pre": qpre,
        "quant_constraints": qcon,
        "snapshot": compact,
        "rules": {
            "no_jargon": True,
            "no_guarantee": True,
            "max_sentences": 2,
            "avoid_terms": ["레버리지", "ATR", "RSI", "MACD", "확실", "무조건", "100%"],
        },
    }


# -----------------------------------------------------------------------------
# Parse / validate output (STRICT)
# -----------------------------------------------------------------------------
def _validate_tags(tags: List[Any]) -> Tuple[str, ...]:
    if not tags:
        _fail("parse", "auditor.tags is empty")
    out: List[str] = []
    for i, t in enumerate(tags):
        s = str(t).strip()
        if not s:
            _fail("parse", f"auditor.tags[{i}] empty")
        if not _TAG_RE.match(s):
            _fail("parse", f"auditor.tags[{i}] invalid format: {s!r}")
        out.append(s)
    if len(out) > 12:
        _fail("parse", "auditor.tags too many (>12)")
    return tuple(out)


def _validate_output(
    *,
    decision_id: str,
    event_type: str,
    raw_text: str,
    obj: Dict[str, Any],
) -> SupervisorResult:
    stage = "parse"

    # top-level forbidden keys
    for k in obj.keys():
        lk = str(k).lower().strip()
        if lk in _FORBIDDEN_TOP_KEYS:
            _fail(stage, f"forbidden top-level key present: {k!r}")

    auditor_raw = _require_dict(stage, obj.get("auditor"), "auditor")
    sev = _require_int(stage, auditor_raw.get("severity"), "auditor.severity")
    if sev not in (0, 1, 2, 3):
        _fail(stage, f"auditor.severity must be 0..3 (got={sev})")

    tags = _validate_tags(_require_list(stage, auditor_raw.get("tags"), "auditor.tags", min_len=1))
    rationale = _require_nonempty_str(stage, auditor_raw.get("rationale_short"), "auditor.rationale_short")
    if len(rationale) > 240:
        _fail(stage, "auditor.rationale_short too long (>240)")

    cp = _require_float(stage, auditor_raw.get("confidence_penalty"), "auditor.confidence_penalty")
    cp = _require_range(stage, cp, "auditor.confidence_penalty", 0.0, 1.0)

    rm = _require_float(stage, auditor_raw.get("suggested_risk_multiplier"), "auditor.suggested_risk_multiplier")
    rm = _require_range(stage, rm, "auditor.suggested_risk_multiplier", 0.0, 1.0)

    narration_val = obj.get("narration")
    narration: Optional[Narration] = None
    if narration_val is not None:
        if narration_val is not None and not isinstance(narration_val, dict):
            _fail(stage, f"narration must be object or null (got={type(narration_val)})")
        if isinstance(narration_val, dict):
            title = _require_nonempty_str(stage, narration_val.get("title"), "narration.title")
            msg = _require_nonempty_str(stage, narration_val.get("message"), "narration.message")
            tone = _require_nonempty_str(stage, narration_val.get("tone"), "narration.tone")
            if len(msg) > 240:
                _fail(stage, "narration.message too long (>240)")
            bad = _contains_prohibited(msg)
            if bad is not None:
                _fail(stage, f"narration contains prohibited phrase: {bad!r}")
            narration = Narration(title=title, message=msg, tone=tone)

    post_val = obj.get("postmortem")
    post: Optional[Postmortem] = None
    if post_val is not None:
        if not isinstance(post_val, dict):
            _fail(stage, f"postmortem must be object or null (got={type(post_val)})")
        kind = _require_nonempty_str(stage, post_val.get("kind"), "postmortem.kind")
        if kind not in ("probabilistic_loss", "strategy_issue", "execution_issue"):
            _fail(stage, f"postmortem.kind invalid: {kind!r}")
        notes = _require_nonempty_str(stage, post_val.get("notes"), "postmortem.notes")
        if len(notes) > 500:
            _fail(stage, "postmortem.notes too long (>500)")
        post = Postmortem(kind=kind, notes=notes)

    auditor = Auditor(
        severity=sev,
        tags=tags,
        rationale_short=rationale,
        confidence_penalty=cp,
        suggested_risk_multiplier=rm,
    )

    return SupervisorResult(
        decision_id=decision_id,
        event_type=event_type,
        auditor=auditor,
        narration=narration,
        postmortem=post,
        raw_text=raw_text,
    )


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
def run_gpt_supervisor(
    *,
    decision_id: str,
    event_type: str,
    unified_features: Dict[str, Any],
    quant_decision_pre: Dict[str, Any],
    quant_constraints: Dict[str, Any],
    recent_messages: Optional[Sequence[str]] = None,
    model: Optional[str] = None,
    timeout_sec: Optional[float] = None,
) -> SupervisorResult:
    """
    STRICT: GPT Supervisor 실행 (감사/해설/사후감사)

    - 실패 시 예외를 던진다(폴백 없음).
    - caller는 "거래 결정은 GPT에 의존하지 않는다" 원칙에 따라
      이 호출 실패를 trade-flow에서 분리해서 처리해야 한다.
    """
    did = _require_nonempty_str("input", decision_id, "decision_id")
    et = _require_nonempty_str("input", event_type, "event_type")
    m = (model or DEFAULT_MODEL).strip()
    if not m:
        _fail("input", "model is empty")

    tsec = float(timeout_sec if timeout_sec is not None else MAX_LATENCY_SEC)
    if not math.isfinite(tsec) or tsec <= 0:
        _fail("input", f"timeout_sec invalid: {timeout_sec!r}")

    system_prompt = _build_system_prompt()
    client = _get_client()

    # 2-pass anti-repeat (STRICT quality rule)
    last_msg = (recent_messages[-1] if recent_messages else None)

    attempt = 0
    last_text = ""
    while attempt < 2:
        force_rephrase = attempt == 1

        payload = _build_user_payload(
            decision_id=did,
            event_type=et,
            unified_features=unified_features,
            quant_decision_pre=quant_decision_pre,
            quant_constraints=quant_constraints,
            recent_messages=recent_messages,
            force_rephrase=force_rephrase,
        )

        t0 = time.time()
        try:
            resp = client.chat.completions.create(
                model=m,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
                temperature=TEMPERATURE,
                max_tokens=MAX_TOKENS,
            )
        except Exception as e:
            _fail("openai", f"request failed: {e.__class__.__name__}", e)
        dt = time.time() - t0
        if dt > tsec:
            _fail("openai", f"latency budget exceeded: {dt:.2f}s > {tsec:.2f}s")

        # Extract text (STRICT)
        try:
            content = resp.choices[0].message.content  # type: ignore[index]
        except Exception as e:
            _fail("openai", "response content missing", e)
        if not isinstance(content, str) or not content.strip():
            _fail("openai", "empty content")

        last_text = content.strip()

        # Parse/validate
        obj = _extract_first_json_object(last_text)
        result = _validate_output(decision_id=did, event_type=et, raw_text=last_text, obj=obj)

        # anti-repeat check only if narration exists
        if last_msg is not None and result.narration is not None:
            sim = _jaccard(last_msg, result.narration.message)
            if sim >= 0.80:
                # too similar: retry with force_rephrase
                attempt += 1
                continue

        return result

    _fail("quality", "narration too similar to recent message even after rephrase")
    raise AssertionError("unreachable")  # pragma: no cover


__all__ = [
    "GptSupervisorError",
    "Auditor",
    "Narration",
    "Postmortem",
    "SupervisorResult",
    "run_gpt_supervisor",
]