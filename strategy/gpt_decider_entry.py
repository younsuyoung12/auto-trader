"""
======================================================
strategy/gpt_decider_entry.py
STRICT · NO-FALLBACK · PRODUCTION MODE
======================================================
역할
- ENTRY/EXIT GPT 판단 레이어.
- ENTRY: 로컬 하드 필터 + Regime(레짐) 점수화 기반 필터 + GPT JSON 판단.
- EXIT : GPT JSON 판단(필수 필드 누락/비정상 시 즉시 예외).

절대 원칙 (STRICT · NO-FALLBACK)
- 데이터 누락/NaN/Infinity/None 을 0으로 “보정”해서 진행하지 않는다.
- GPT 응답이 스키마/범위를 위반하면 “ENTER로 보정”하지 않는다.
  → 명시적 SKIP(ENTRY) 또는 즉시 예외(EXIT).
- 텔레메트리(telelog/CSV) 실패는 의사결정에 영향을 주지 않는다(로깅만 실패).

인프라 전제
- 트레이딩 엔진 실행: AWS
- DB/대시보드: Render
- 이 파일은 네트워크/DB 접근 없이 GPT 의사결정만 담당한다.

======================================================

변경 이력
------------------------------------------------------
2026-03-02
- 고급 버전 “레짐 점수화 시스템” 적용
  - trend_strength / atr_pct(volatility) / range_pct / adx / is_low_volatility 기반 점수 계산
  - 점수 합산으로 TREND / RANGE / HIGH_VOL 3-way 분류
  - 레짐에 따라 ENTRY 로컬 필터 임계값(entry_score_min) 및 base_risk_pct에 risk_multiplier 적용
- STRICT · NO-FALLBACK 강화
  - NaN/Inf/None을 0으로 치환(sanitize)하는 로직 제거 → 발견 즉시 SKIP 또는 예외
  - ENTRY GPT 응답 필드/범위 위반 시 ENTER로 보정 금지 → 명시적 SKIP
  - EXIT 응답 필수 필드 위반 시 HOLD로 보정 금지 → 즉시 예외
  - ask_exit_decision_safe: fallback 제거(실패 시 예외)
- ENTRY 로컬 필터 버그 수정
  - trend_strength/volatility를 payload 최상단에서 읽던 오류 수정(시장 피처 dict에서 추출)

2026-03-02 (hotfix)
- RegimeScorePolicy(slots=True)에서 policy.__dict__ 접근 제거
  → dataclasses.asdict(policy) 사용
- 로컬 개발 편의: .env 자동 로딩 지원 (의존성 없이)
  - OPENAI_API_KEY가 os.environ에 없으면 .env를 읽어 주입
  - 여전히 OPENAI_API_KEY 없으면 즉시 예외(STRICT)
  - AWS 운영은 환경변수만 사용(.env 업로드 금지)
======================================================
"""

from __future__ import annotations

import csv
import datetime as dt
import json
import math
import os
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Literal, Optional, Tuple

from openai import OpenAI

from settings import load_settings

try:
    import infra.telelog as telelog  # type: ignore
except Exception:  # pragma: no cover
    telelog = None  # type: ignore


# =============================================================================
# 설정 / 상수
# =============================================================================

# 기본 모델 (환경변수로 덮어쓰기)
GPT_MODEL_DEFAULT = os.getenv("OPENAI_TRADER_MODEL", "gpt-4o-mini")

OPENAI_TRADER_MAX_LATENCY = float(os.getenv("OPENAI_TRADER_MAX_LATENCY", "12"))
OPENAI_TRADER_MAX_TOKENS = int(os.getenv("OPENAI_TRADER_MAX_TOKENS", "512"))

GPT_LATENCY_CSV = os.getenv("GPT_LATENCY_CSV", "gpt_latency.csv")

TELELOG_CHAT_ID = os.getenv("TELELOG_CHAT_ID", "")
TELELOG_LEVEL = os.getenv("TELELOG_LEVEL", "INFO").upper()

# GPT가 제안할 수 있는 최대 리스크(비율, 예: 0.03 = 3%)
GPT_MAX_RISK_PCT = float(os.getenv("GPT_MAX_RISK_PCT", "0.03"))

# ENTRY GPT 호출 횟수(프로세스 내 카운터)
gpt_entry_call_count = 0

_gpt_latency_lock = Lock()

SET = load_settings()  # settings는 단일 소스(불변 전제)


# =============================================================================
# 공용 유틸 (로깅은 의사결정에 영향 주지 않음)
# =============================================================================
def _safe_log(level: str, msg: str) -> None:
    if telelog is None:
        return
    try:
        level = (level or "INFO").upper()
        if level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            level = "INFO"
        telelog.log(level, msg)  # type: ignore
    except Exception:
        return


def _safe_tg(msg: str) -> None:
    if telelog is None:
        return
    if not TELELOG_CHAT_ID:
        return
    try:
        telelog.tg(msg)  # type: ignore
    except Exception:
        return


def _log_gpt_latency_csv(
    *,
    kind: str,
    model: str,
    symbol: Optional[str],
    source: Optional[str],
    direction: Optional[str],
    latency: float,
    success: bool,
    error_type: str = "",
    error_msg: str = "",
) -> None:
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
        "error_msg": (error_msg or "")[:512],
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
        return


def _extract_text_from_response(resp: Any) -> str:
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

    text_attr = getattr(resp, "output_text", None)
    if isinstance(text_attr, str) and text_attr.strip():
        return text_attr

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

    return repr(resp)[:2000]


# =============================================================================
# .env 로딩 (로컬 개발 편의) + OpenAI Client (STRICT)
# =============================================================================
def _load_dotenv_if_needed() -> None:
    """
    로컬 개발 편의용.
    - OPENAI_API_KEY가 os.environ에 없을 때만 .env를 읽어 주입한다.
    - 이미 환경변수가 있으면 덮어쓰지 않는다.
    - .env가 없으면 아무것도 하지 않는다.
    - 파일 파싱 실패는 즉시 예외(STRICT). (조용히 무시 금지)
    """
    if os.getenv("OPENAI_API_KEY", "").strip():
        return

    # gpt_decider_entry.py는 strategy/ 아래에 있으므로, 프로젝트 루트는 parents[1]
    root = Path(__file__).resolve().parents[1]
    candidates = [
        root / ".env",          # 프로젝트 루트
        Path.cwd() / ".env",    # 현재 작업 디렉토리
    ]

    env_path: Optional[Path] = None
    for p in candidates:
        if p.exists() and p.is_file():
            env_path = p
            break

    if env_path is None:
        return

    try:
        with env_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v
    except Exception as e:
        raise RuntimeError(f".env 로딩 실패: {e}") from e


def _get_client() -> OpenAI:
    """
    STRICT:
    - OPENAI_API_KEY 없으면 즉시 예외.
    - 로컬에서만 .env 로딩을 허용(환경변수 우선, 덮어쓰기 금지).
    """
    _load_dotenv_if_needed()

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError(
            "OPENAI_API_KEY가 없습니다. "
            "로컬은 .env에 OPENAI_API_KEY를 넣고, AWS 운영은 환경변수로 설정하십시오."
        )

    api_base = os.getenv("OPENAI_API_BASE", "").strip() or None
    return OpenAI(api_key=api_key, base_url=api_base)


# =============================================================================
# STRICT JSON/수치 검증 (NO-FALLBACK)
# =============================================================================
def _is_finite_number(x: Any) -> bool:
    try:
        f = float(x)
    except Exception:
        return False
    return math.isfinite(f)


def _req_float(x: Any, name: str, *, min_v: Optional[float] = None, max_v: Optional[float] = None) -> float:
    if isinstance(x, bool):
        raise ValueError(f"{name} must be a number (bool not allowed)")
    try:
        f = float(x)
    except Exception as e:
        raise ValueError(f"{name} must be a number") from e
    if not math.isfinite(f):
        raise ValueError(f"{name} must be finite")
    if min_v is not None and f < min_v:
        raise ValueError(f"{name} must be >= {min_v}")
    if max_v is not None and f > max_v:
        raise ValueError(f"{name} must be <= {max_v}")
    return f


def _req_int(x: Any, name: str, *, min_v: Optional[int] = None, max_v: Optional[int] = None) -> int:
    try:
        iv = int(x)
    except Exception as e:
        raise ValueError(f"{name} must be int") from e
    if min_v is not None and iv < min_v:
        raise ValueError(f"{name} must be >= {min_v}")
    if max_v is not None and iv > max_v:
        raise ValueError(f"{name} must be <= {max_v}")
    return iv


def _assert_no_nan_inf_none(obj: Any, *, path: str = "root", depth: int = 0, max_depth: int = 12) -> None:
    if depth > max_depth:
        raise ValueError(f"payload too deep (>{max_depth}) at {path}")

    if obj is None:
        raise ValueError(f"None is not allowed at {path}")

    if isinstance(obj, bool):
        return

    if isinstance(obj, (int, str)):
        return

    if isinstance(obj, float):
        if not math.isfinite(obj):
            raise ValueError(f"NaN/Inf is not allowed at {path}")
        return

    if isinstance(obj, dict):
        for k, v in obj.items():
            _assert_no_nan_inf_none(v, path=f"{path}.{k}", depth=depth + 1, max_depth=max_depth)
        return

    if isinstance(obj, (list, tuple)):
        for i, v in enumerate(obj):
            _assert_no_nan_inf_none(v, path=f"{path}[{i}]", depth=depth + 1, max_depth=max_depth)
        return

    raise ValueError(f"unsupported type at {path}: {type(obj).__name__}")


# =============================================================================
# 레짐 점수화 시스템 (고급 버전)
# =============================================================================
@dataclass(frozen=True, slots=True)
class RegimeScorePolicy:
    # 변동성(atr_pct) 임계 (atr_pct는 ratio: 0.004 = 0.4%)
    vol_hi_1: float = 0.008  # 0.8%  → 큰 변동성
    vol_hi_2: float = 0.012  # 1.2%  → 매우 큰 변동성
    vol_extreme: float = 0.020  # 2.0% → 극단(즉시 HIGH_VOL)

    # trend_strength 임계 (기존 build_regime_features 기준으로 1.x대가 흔함)
    ts_1: float = 1.2
    ts_2: float = 1.6
    ts_3: float = 2.0

    # ADX 임계
    adx_1: float = 18.0
    adx_2: float = 25.0
    adx_3: float = 35.0

    # range_pct 임계 (ratio)
    range_small_1: float = 0.004  # 0.4%
    range_small_2: float = 0.002  # 0.2%

    # 분류 기준(총점)
    trend_score_min: int = 3
    high_vol_score_max: int = -4  # 이 이하이면 HIGH_VOL

    # 레짐별 로컬 필터/리스크
    base_entry_score_min: float = 15.0
    range_entry_add: float = 10.0
    high_vol_entry_add: float = 20.0

    trend_risk_mult: float = 1.0
    range_risk_mult: float = 0.7
    high_vol_risk_mult: float = 0.5


def _load_regime_policy(settings: Any) -> RegimeScorePolicy:
    p = RegimeScorePolicy()

    def g(name: str, default: Any) -> Any:
        return getattr(settings, name, default)

    pol = RegimeScorePolicy(
        vol_hi_1=float(g("regime_vol_hi_1", p.vol_hi_1)),
        vol_hi_2=float(g("regime_vol_hi_2", p.vol_hi_2)),
        vol_extreme=float(g("regime_vol_extreme", p.vol_extreme)),
        ts_1=float(g("regime_ts_1", p.ts_1)),
        ts_2=float(g("regime_ts_2", p.ts_2)),
        ts_3=float(g("regime_ts_3", p.ts_3)),
        adx_1=float(g("regime_adx_1", p.adx_1)),
        adx_2=float(g("regime_adx_2", p.adx_2)),
        adx_3=float(g("regime_adx_3", p.adx_3)),
        range_small_1=float(g("regime_range_small_1", p.range_small_1)),
        range_small_2=float(g("regime_range_small_2", p.range_small_2)),
        trend_score_min=_req_int(g("regime_trend_score_min", p.trend_score_min), "regime_trend_score_min", min_v=1),
        high_vol_score_max=_req_int(g("regime_high_vol_score_max", p.high_vol_score_max), "regime_high_vol_score_max"),
        base_entry_score_min=float(g("regime_base_entry_score_min", p.base_entry_score_min)),
        range_entry_add=float(g("regime_range_entry_add", p.range_entry_add)),
        high_vol_entry_add=float(g("regime_high_vol_entry_add", p.high_vol_entry_add)),
        trend_risk_mult=float(g("regime_trend_risk_mult", p.trend_risk_mult)),
        range_risk_mult=float(g("regime_range_risk_mult", p.range_risk_mult)),
        high_vol_risk_mult=float(g("regime_high_vol_risk_mult", p.high_vol_risk_mult)),
    )

    for name, v in [
        ("vol_hi_1", pol.vol_hi_1),
        ("vol_hi_2", pol.vol_hi_2),
        ("vol_extreme", pol.vol_extreme),
        ("ts_1", pol.ts_1),
        ("ts_2", pol.ts_2),
        ("ts_3", pol.ts_3),
        ("adx_1", pol.adx_1),
        ("adx_2", pol.adx_2),
        ("adx_3", pol.adx_3),
        ("range_small_1", pol.range_small_1),
        ("range_small_2", pol.range_small_2),
        ("base_entry_score_min", pol.base_entry_score_min),
        ("range_entry_add", pol.range_entry_add),
        ("high_vol_entry_add", pol.high_vol_entry_add),
        ("trend_risk_mult", pol.trend_risk_mult),
        ("range_risk_mult", pol.range_risk_mult),
        ("high_vol_risk_mult", pol.high_vol_risk_mult),
    ]:
        if not math.isfinite(float(v)):
            raise ValueError(f"regime policy {name} must be finite")

    if not (0 < pol.vol_hi_1 < pol.vol_hi_2 < pol.vol_extreme):
        raise ValueError("regime vol thresholds must satisfy 0 < hi1 < hi2 < extreme")
    if not (0 < pol.range_risk_mult <= 1.0 and 0 < pol.high_vol_risk_mult <= 1.0 and pol.trend_risk_mult > 0):
        raise ValueError("regime risk multipliers invalid")
    return pol


def _pick_tf5_container(market_features: Dict[str, Any]) -> Dict[str, Any]:
    for k in ("tf5", "tf_5m", "m5", "five_min"):
        v = market_features.get(k)
        if isinstance(v, dict):
            return v
    return market_features


def _extract_regime_inputs_strict(market_features: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(market_features, dict):
        raise ValueError("market_features must be dict")

    base = market_features
    tf5 = _pick_tf5_container(market_features)

    trend_strength = None
    if "trend_strength" in base:
        trend_strength = base.get("trend_strength")
    elif "trend_strength" in tf5:
        trend_strength = tf5.get("trend_strength")
    else:
        reg = tf5.get("regime")
        if isinstance(reg, dict) and "trend_strength" in reg:
            trend_strength = reg.get("trend_strength")

    atr_pct = None
    for k in ("volatility", "atr_pct"):
        if k in base:
            atr_pct = base.get(k)
            break
    if atr_pct is None:
        for k in ("volatility", "atr_pct"):
            if k in tf5:
                atr_pct = tf5.get(k)
                break

    range_pct = base.get("range_pct") if "range_pct" in base else tf5.get("range_pct")
    adx = base.get("adx") if "adx" in base else tf5.get("adx")
    is_lv = base.get("is_low_volatility") if "is_low_volatility" in base else tf5.get("is_low_volatility")

    return {
        "trend_strength": _req_float(trend_strength, "trend_strength", min_v=0.0),
        "atr_pct": _req_float(atr_pct, "atr_pct", min_v=0.0),
        "range_pct": _req_float(range_pct, "range_pct", min_v=0.0),
        "adx": _req_float(adx, "adx", min_v=0.0),
        "is_low_volatility": _req_int(is_lv, "is_low_volatility", min_v=0, max_v=1),
    }


def _score_and_classify_regime(
    *,
    policy: RegimeScorePolicy,
    current_price: float,
    market_features: Dict[str, Any],
) -> Dict[str, Any]:
    cp = _req_float(current_price, "current_price", min_v=0.0)
    if cp <= 0:
        raise ValueError("current_price must be > 0")

    x = _extract_regime_inputs_strict(market_features)

    ts = x["trend_strength"]
    atr = x["atr_pct"]
    rng = x["range_pct"]
    adx = x["adx"]
    is_lv = x["is_low_volatility"]

    score = 0
    comp: Dict[str, int] = {}

    if atr >= policy.vol_extreme:
        comp["vol_extreme"] = -999
    elif atr >= policy.vol_hi_2:
        comp["vol_hi_2"] = -5
        score += -5
    elif atr >= policy.vol_hi_1:
        comp["vol_hi_1"] = -3
        score += -3
    else:
        comp["vol_ok"] = 0

    if ts >= policy.ts_3:
        comp["ts_3"] = +3
        score += 3
    elif ts >= policy.ts_2:
        comp["ts_2"] = +2
        score += 2
    elif ts >= policy.ts_1:
        comp["ts_1"] = +1
        score += 1
    else:
        comp["ts_weak"] = -1
        score += -1

    if adx >= policy.adx_3:
        comp["adx_3"] = +3
        score += 3
    elif adx >= policy.adx_2:
        comp["adx_2"] = +2
        score += 2
    elif adx < policy.adx_1:
        comp["adx_weak"] = -1
        score += -1
    else:
        comp["adx_mid"] = 0

    if rng <= policy.range_small_2:
        comp["range_tight_2"] = -2
        score += -2
    elif rng <= policy.range_small_1:
        comp["range_tight_1"] = -1
        score += -1
    else:
        comp["range_wide"] = 0

    if is_lv == 1:
        comp["low_vol_flag"] = -1
        score += -1

    b_bonus = 0
    try:
        hi = market_features.get("high_recent")
        lo = market_features.get("low_recent")
        if hi is not None and lo is not None:
            hi_f = _req_float(hi, "high_recent", min_v=0.0)
            lo_f = _req_float(lo, "low_recent", min_v=0.0)
            if cp > hi_f:
                b_bonus = 1
                comp["breakout_up"] = 1
            elif cp < lo_f:
                b_bonus = 1
                comp["breakout_down"] = 1
    except Exception:
        pass
    score += b_bonus

    if atr >= policy.vol_extreme or comp.get("vol_extreme") == -999:
        regime = "HIGH_VOL"
    elif score <= policy.high_vol_score_max:
        regime = "HIGH_VOL"
    elif score >= policy.trend_score_min:
        regime = "TREND"
    else:
        regime = "RANGE"

    if regime == "TREND":
        risk_mult = policy.trend_risk_mult
        entry_min = policy.base_entry_score_min
    elif regime == "HIGH_VOL":
        risk_mult = policy.high_vol_risk_mult
        entry_min = policy.base_entry_score_min + policy.high_vol_entry_add
    else:
        risk_mult = policy.range_risk_mult
        entry_min = policy.base_entry_score_min + policy.range_entry_add

    return {
        "regime": regime,
        "score": int(score),
        "risk_multiplier": float(risk_mult),
        "entry_score_min": float(entry_min),
        "inputs": x,
        "components": comp,
        "policy": asdict(policy),
    }


# =============================================================================
# OpenAI 공통 JSON 호출(Exit)
# =============================================================================
def _call_gpt_json(
    *,
    model: str,
    prompt: str,
    purpose: str = "EXIT",
    timeout_sec: Optional[float] = None,
) -> Dict[str, Any]:
    if timeout_sec is None:
        timeout_sec = OPENAI_TRADER_MAX_LATENCY

    client = _get_client()

    start = time.monotonic()
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            timeout=timeout_sec,
            temperature=0.0,
            max_completion_tokens=OPENAI_TRADER_MAX_TOKENS,
        )
        latency = time.monotonic() - start

        text = _extract_text_from_response(resp)
        data = json.loads(text)

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
# ENTRY GPT 응답 정규화(STRICT)
# =============================================================================
def _entry_invalid_to_skip(
    *,
    symbol: str,
    source: str,
    reason: str,
    note: str,
    raw_response: str = "",
) -> Dict[str, Any]:
    return {
        "action": "SKIP",
        "direction": "PASS",
        "confidence": 0.0,
        "tp_pct": 0.0,
        "tv_pct": 0.0,
        "sl_pct": 0.0,
        "effective_risk_pct": 0.0,
        "guard_adjustments": {},
        "reason": reason,
        "note": note,
        "raw_response": raw_response,
        "_meta": {"symbol": symbol, "source": source},
    }


def _normalize_and_validate_entry_response_strict(
    resp: Dict[str, Any],
    *,
    base_tv_pct: float,
    base_sl_pct: float,
    base_risk_pct: float,
    symbol: str,
    source: str,
    gpt_max_risk_pct: float,
) -> Dict[str, Any]:
    raw_action = str(resp.get("action", "") or "").upper().strip()
    if raw_action == "PASS":
        raw_action = "SKIP"
    if raw_action not in ("ENTER", "SKIP", "ADJUST"):
        return _entry_invalid_to_skip(
            symbol=symbol,
            source=source,
            reason=f"GPT 응답 action이 유효하지 않음({raw_action}).",
            note="invalid_gpt_action",
            raw_response=str(resp)[:500],
        )
    action = raw_action

    direction = str(resp.get("direction", "") or "").upper().strip()
    if direction not in ("LONG", "SHORT", "PASS"):
        direction = "PASS"

    try:
        confidence = float(resp.get("confidence", 0.5))
    except Exception:
        confidence = 0.5
    if not math.isfinite(confidence):
        confidence = 0.5
    confidence = max(0.0, min(1.0, confidence))

    note = str(resp.get("note", "") or "").strip()
    reason = str(resp.get("reason", "") or "").strip()
    raw_response = str(resp.get("raw_response", "") or "").strip()

    if action == "SKIP":
        if not reason:
            reason = "지표 근거 부족 또는 리스크 과다로 SKIP."
        return {
            "action": "SKIP",
            "direction": "PASS",
            "confidence": confidence,
            "tp_pct": 0.0,
            "tv_pct": 0.0,
            "sl_pct": 0.0,
            "effective_risk_pct": 0.0,
            "guard_adjustments": {},
            "reason": reason,
            "note": note,
            "raw_response": raw_response,
        }

    tp_raw = resp.get("tp_pct", None)
    if tp_raw is None and "tv_pct" in resp:
        tp_raw = resp.get("tv_pct")
    sl_raw = resp.get("sl_pct", None)
    rk_raw = resp.get("effective_risk_pct", None)

    try:
        tp_pct = _req_float(tp_raw, "tp_pct", min_v=0.0, max_v=0.5)
        sl_pct = _req_float(sl_raw, "sl_pct", min_v=0.0, max_v=0.5)
        effective_risk_pct = _req_float(rk_raw, "effective_risk_pct", min_v=0.0, max_v=gpt_max_risk_pct)
    except Exception as e:
        return _entry_invalid_to_skip(
            symbol=symbol,
            source=source,
            reason=f"GPT 응답 수치 필드가 유효하지 않음: {type(e).__name__}: {e}",
            note="invalid_gpt_numeric_fields",
            raw_response=str(resp)[:500],
        )

    if tp_pct <= 0 or sl_pct <= 0 or effective_risk_pct <= 0:
        return _entry_invalid_to_skip(
            symbol=symbol,
            source=source,
            reason="GPT 응답(tp/sl/risk)이 0 이하. ENTER/ADJUST 불가.",
            note="invalid_zero_fields",
            raw_response=str(resp)[:500],
        )

    if not reason:
        reason = "지표 근거에 따라 진입을 허용."

    guard_adjustments = resp.get("guard_adjustments")
    if not isinstance(guard_adjustments, dict):
        guard_adjustments = {}

    return {
        "action": action,
        "direction": direction,
        "confidence": confidence,
        "tp_pct": tp_pct,
        "tv_pct": tp_pct,
        "sl_pct": sl_pct,
        "effective_risk_pct": effective_risk_pct,
        "guard_adjustments": guard_adjustments,
        "reason": reason,
        "note": note,
        "raw_response": raw_response,
    }


# =============================================================================
# GPT 프롬프트 (ENTRY)
# =============================================================================
SYSTEM_PROMPT_ENTRY = """
You are an expert intraday crypto futures trading decision assistant.

역할:
- 단기 선물 자동매매 시스템의 '진입 판단 레이어'를 담당한다.
- JSON 컨텍스트를 기반으로 이번 캔들에서 ENTER / ADJUST / SKIP 중 하나를 결정한다.
- 반드시 하나의 JSON 객체만 출력한다.

출력 JSON 스키마:
- 필수:
    - action: "ENTER" | "SKIP" | "ADJUST"
    - direction: "LONG" | "SHORT" | "PASS"
    - confidence: 0.0 ~ 1.0
    - tp_pct: 0.0 ~ 0.5
    - sl_pct: 0.0 ~ 0.5
    - effective_risk_pct: 0.0 ~ {gpt_max_risk_pct}
    - guard_adjustments: 객체 (없으면 빈 객체)
    - reason: 한국어 1~2문장 (지표/리스크 근거)
    - note: string
    - raw_response: string

규칙:
- action 이 "ENTER" 또는 "ADJUST"면 tp_pct/sl_pct/effective_risk_pct 모두 0보다 커야 한다.
- action 이 "SKIP"이면 reason에 지표 기반 근거를 포함한다.
- NaN/Infinity/null/None 과 같은 값은 절대 사용하지 않는다.
- 반드시 순수 JSON 형식으로만 출력한다.

문체 규칙:
- 전문적이고 간결.
- 감정/수사 금지.
- 지표·가격·리스크 근거만 명확히 제시.
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

Regime (score-based):
- regime: {regime}
- regime_score: {regime_score}
- regime_entry_score_min: {regime_entry_score_min}
- regime_risk_multiplier: {regime_risk_multiplier}

Entry Context (JSON):
{market_features_json}

[요청]
위 정보를 바탕으로 이번 캔들에서 신규 진입을 할지 판단하고,
아래 필드를 모두 포함하는 순수 JSON 객체 1개만 출력해라.

필수 필드:
- action: "ENTER" | "SKIP" | "ADJUST"
- direction: "LONG" | "SHORT" | "PASS"
- confidence: 0.0 ~ 1.0
- tp_pct: 0.0 ~ 0.5
- sl_pct: 0.0 ~ 0.5
- effective_risk_pct: 0.0 ~ {gpt_max_risk_pct}
- guard_adjustments: 객체 (없으면 빈 객체 {{}} )
- reason: 한국어 1~2문장
- note: 한국어
- raw_response: string

주의:
- JSON 이외 텍스트 금지.
- NaN/Infinity/null/None 금지.
"""


def _parse_json(text: str) -> Dict[str, Any]:
    text = (text or "").strip()
    if not text:
        raise ValueError("GPT 응답이 비어 있습니다.")

    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].strip().startswith("```"):
            lines = lines[1:]
        while lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()

    try:
        data = json.loads(text)
        if isinstance(data, dict):
            return data
        raise ValueError(f"JSON 최상위 타입이 dict가 아님: {type(data).__name__}")
    except Exception:
        pass

    first = text.find("{")
    last = text.rfind("}")
    if first >= 0 and last > first:
        snippet = text[first : last + 1]
        data = json.loads(snippet)
        if isinstance(data, dict):
            return data
        raise ValueError(f"중괄호 추출 후에도 dict가 아님: {type(data).__name__}")

    raise ValueError(f"GPT JSON 파싱 실패: text={text[:500]}")


# =============================================================================
# ENTRY payload
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
    regime_meta: Dict[str, Any],
    signal_source: Optional[str] = None,
    chosen_signal: Optional[str] = None,
    last_price: Optional[float] = None,
    entry_score: Optional[float] = None,
    base_effective_risk_pct: Optional[float] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "symbol": symbol,
        "source": source,
        "current_price": float(current_price),
        "base_tv_pct": float(base_tv_pct),
        "base_sl_pct": float(base_sl_pct),
        "base_risk_pct": float(base_risk_pct),
        "regime_meta": regime_meta,
        "market_features": market_features,
    }

    entry_meta: Dict[str, Any] = {}
    if signal_source:
        entry_meta["signal_source"] = signal_source
    if chosen_signal:
        entry_meta["signal_direction_hint"] = chosen_signal
    if last_price is not None:
        entry_meta["last_price"] = float(last_price)
    if entry_score is not None:
        entry_meta["entry_score"] = float(entry_score)
    if base_effective_risk_pct is not None:
        entry_meta["base_effective_risk_pct"] = float(base_effective_risk_pct)

    if entry_meta:
        payload["entry_meta"] = entry_meta

    return payload


# =============================================================================
# ENTRY main
# =============================================================================
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
    signal_source: Optional[str] = None,
    chosen_signal: Optional[str] = None,
    last_price: Optional[float] = None,
    entry_score: Optional[float] = None,
    effective_risk_pct: Optional[float] = None,
) -> Dict[str, Any]:
    global gpt_entry_call_count

    if gpt_entry_call_count >= int(getattr(SET, "gpt_daily_call_limit")):
        return _entry_invalid_to_skip(
            symbol=symbol,
            source=source,
            reason="GPT 일일 호출 상한 초과로 ENTRY GPT 호출을 중단.",
            note="daily_gpt_limit_exceeded",
            raw_response="daily_gpt_limit_exceeded",
        )

    if gpt_max_risk_pct is None:
        gpt_max_risk_pct = GPT_MAX_RISK_PCT

    _req_float(current_price, "current_price", min_v=0.0)
    _req_float(base_tv_pct, "base_tv_pct", min_v=0.0, max_v=0.5)
    _req_float(base_sl_pct, "base_sl_pct", min_v=0.0, max_v=0.5)
    _req_float(base_risk_pct, "base_risk_pct", min_v=0.0, max_v=1.0)
    _req_float(gpt_max_risk_pct, "gpt_max_risk_pct", min_v=0.0, max_v=1.0)

    if not isinstance(market_features, dict):
        return _entry_invalid_to_skip(
            symbol=symbol,
            source=source,
            reason="market_features 타입이 dict가 아님. ENTRY 스킵.",
            note="invalid_market_features_type",
            raw_response=str(type(market_features).__name__),
        )

    try:
        pol = _load_regime_policy(SET)
        regime_meta = _score_and_classify_regime(
            policy=pol,
            current_price=float(current_price),
            market_features=market_features,
        )
    except Exception as e:
        _safe_log("ERROR", f"[REGIME_FAIL] symbol={symbol} source={source} err={type(e).__name__}: {e}")
        return _entry_invalid_to_skip(
            symbol=symbol,
            source=source,
            reason="레짐 계산 실패(필수 피처 누락/비정상). ENTRY 스킵.",
            note="regime_calc_failed",
            raw_response=f"{type(e).__name__}: {e}",
        )

    adj_base_risk_pct = float(base_risk_pct) * float(regime_meta["risk_multiplier"])
    if not math.isfinite(adj_base_risk_pct) or adj_base_risk_pct < 0:
        raise ValueError("adjusted base_risk_pct invalid")

    payload = _build_entry_payload(
        symbol=symbol,
        source=source,
        current_price=float(current_price),
        base_tv_pct=float(base_tv_pct),
        base_sl_pct=float(base_sl_pct),
        base_risk_pct=float(adj_base_risk_pct),
        market_features=market_features,
        regime_meta=regime_meta,
        signal_source=signal_source,
        chosen_signal=chosen_signal,
        last_price=last_price,
        entry_score=entry_score,
        base_effective_risk_pct=effective_risk_pct,
    )

    try:
        _assert_no_nan_inf_none(payload)
    except Exception as e:
        _safe_log("ERROR", f"[ENTRY_PAYLOAD_INVALID] {type(e).__name__}: {e}")
        return _entry_invalid_to_skip(
            symbol=symbol,
            source=source,
            reason="ENTRY payload에 None/NaN/Inf 또는 비직렬화 타입이 포함됨. ENTRY 스킵.",
            note="payload_invalid_no_fallback",
            raw_response=f"{type(e).__name__}: {e}",
        )

    es_val: Optional[float] = None
    if entry_score is not None:
        try:
            es_val = float(entry_score)
            if not math.isfinite(es_val):
                es_val = None
        except Exception:
            es_val = None

    entry_score_min = float(regime_meta["entry_score_min"])
    if es_val is not None and es_val < entry_score_min:
        _safe_log(
            "INFO",
            f"[ENTRY_LOCAL_SKIP] entry_score<{entry_score_min:.1f} by regime "
            f"(regime={regime_meta['regime']} score={regime_meta['score']}) "
            f"symbol={symbol} source={source} entry_score={es_val}",
        )
        return {
            "action": "SKIP",
            "direction": "PASS",
            "confidence": 0.1,
            "tp_pct": 0.0,
            "tv_pct": 0.0,
            "sl_pct": 0.0,
            "effective_risk_pct": 0.0,
            "guard_adjustments": {},
            "reason": f"entry_score {es_val:.1f}. 레짐({regime_meta['regime']}) 기준 미달 → SKIP.",
            "note": "local_skip_regime_entry_score_min",
            "raw_response": "local_skip_regime_entry_score_min",
            "_meta": {
                "latency_sec": 0.0,
                "model": model,
                "symbol": symbol,
                "source": source,
                "regime": regime_meta["regime"],
                "regime_score": regime_meta["score"],
            },
        }

    if str(regime_meta["regime"]) == "HIGH_VOL" and float(regime_meta["inputs"]["atr_pct"]) >= float(
        regime_meta["policy"]["vol_extreme"]
    ):
        return {
            "action": "SKIP",
            "direction": "PASS",
            "confidence": 0.1,
            "tp_pct": 0.0,
            "tv_pct": 0.0,
            "sl_pct": 0.0,
            "effective_risk_pct": 0.0,
            "guard_adjustments": {},
            "reason": "atr_pct가 극단 수준. 레짐 HIGH_VOL → SKIP.",
            "note": "local_skip_extreme_volatility",
            "raw_response": "local_skip_extreme_volatility",
            "_meta": {
                "latency_sec": 0.0,
                "model": model,
                "symbol": symbol,
                "source": source,
                "regime": regime_meta["regime"],
                "regime_score": regime_meta["score"],
            },
        }

    market_features_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    system_prompt = SYSTEM_PROMPT_ENTRY.format(gpt_max_risk_pct=gpt_max_risk_pct)
    user_prompt = _USER_PROMPT_TEMPLATE_ENTRY.format(
        symbol=symbol,
        source=source,
        current_price=float(current_price),
        base_tv_pct=float(base_tv_pct),
        base_sl_pct=float(base_sl_pct),
        base_risk_pct=float(adj_base_risk_pct),
        regime=str(regime_meta["regime"]),
        regime_score=int(regime_meta["score"]),
        regime_entry_score_min=float(regime_meta["entry_score_min"]),
        regime_risk_multiplier=float(regime_meta["risk_multiplier"]),
        market_features_json=market_features_json,
        gpt_max_risk_pct=gpt_max_risk_pct,
    )

    client = _get_client()
    start_ts = time.monotonic()
    latency = 0.0

    try:
        _safe_log(
            "DEBUG",
            f"[ENTRY_CALL] model={model} symbol={symbol} source={source} "
            f"regime={regime_meta['regime']} score={regime_meta['score']} "
            f"adj_base_risk={adj_base_risk_pct:.4f}",
        )

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

        gpt_entry_call_count += 1
        gpt_call_id = gpt_entry_call_count

        latency = time.monotonic() - start_ts
        text = _extract_text_from_response(resp)

        try:
            raw_dict = json.loads(text)
        except Exception:
            raw_dict = _parse_json(text)

        if not isinstance(raw_dict, dict):
            return _entry_invalid_to_skip(
                symbol=symbol,
                source=source,
                reason="ENTRY GPT 응답 JSON 최상위 타입이 dict가 아님. SKIP.",
                note="invalid_gpt_json_root",
                raw_response=text[:500],
            )

        normalized = _normalize_and_validate_entry_response_strict(
            raw_dict,
            base_tv_pct=float(base_tv_pct),
            base_sl_pct=float(base_sl_pct),
            base_risk_pct=float(adj_base_risk_pct),
            symbol=symbol,
            source=source,
            gpt_max_risk_pct=float(gpt_max_risk_pct),
        )

        normalized["raw_response"] = text
        normalized["_meta"] = {
            "latency_sec": float(latency),
            "model": model,
            "symbol": symbol,
            "source": source,
            "call_id": gpt_call_id,
            "regime": regime_meta["regime"],
            "regime_score": regime_meta["score"],
            "regime_risk_multiplier": regime_meta["risk_multiplier"],
            "regime_entry_score_min": regime_meta["entry_score_min"],
        }

        _safe_log(
            "INFO",
            f"[ENTRY_OK] model={model} symbol={symbol} source={source} "
            f"regime={regime_meta['regime']} score={regime_meta['score']} "
            f"action={normalized['action']} dir={normalized['direction']} "
            f"conf={normalized['confidence']:.3f} "
            f"tp={float(normalized.get('tp_pct', 0.0)):.4f} sl={float(normalized.get('sl_pct', 0.0)):.4f} "
            f"risk={float(normalized.get('effective_risk_pct', 0.0)):.4f} latency={latency:.3f}s "
            f"call_id={gpt_call_id}",
        )

        _log_gpt_latency_csv(
            kind="ENTRY",
            model=model,
            symbol=symbol,
            source=source,
            direction=str(normalized.get("direction") or ""),
            latency=latency,
            success=True,
        )

        return normalized

    except Exception as e:
        latency = time.monotonic() - start_ts
        _safe_log(
            "ERROR",
            f"[ENTRY_FAIL] model={model} symbol={symbol} source={source} "
            f"latency={latency:.3f}s err={type(e).__name__}: {e}",
        )
        _safe_tg(
            f"[ENTRY_FAIL] model={model} symbol={symbol} source={source} "
            f"latency={latency:.3f}s err={type(e).__name__}: {e}"
        )

        _log_gpt_latency_csv(
            kind="ENTRY",
            model=model,
            symbol=symbol,
            source=source,
            direction=None,
            latency=latency,
            success=False,
            error_type=type(e).__name__,
            error_msg=str(e),
        )

        raise RuntimeError(f"ENTRY GPT 호출 실패: {type(e).__name__}: {e}") from e


# =============================================================================
# EXIT (STRICT)
# =============================================================================
_EXIT_SYSTEM_PROMPT = """
당신은 BTC-USDT 인트라데이 자동매매 시스템의 EXIT 전문 어드바이저이다.

규칙:
- 반드시 하나의 JSON 객체만 출력한다.
- action은 "CLOSE" 또는 "HOLD" 중 하나여야 한다.
- reason은 반드시 포함하며, 포지션 방향 기준으로 지표 근거를 1~3문장으로 작성한다.
- NaN/Infinity/null/None 금지.
"""

_EXIT_USER_PROMPT_TEMPLATE = """
[포지션 및 시장 컨텍스트(JSON)]
{context_json}

위 정보를 바탕으로 단일 JSON 객체만 응답해라.
주의: JSON 이외 텍스트 금지.
"""


def _make_exit_prompt(payload: Dict[str, Any]) -> str:
    _assert_no_nan_inf_none(payload)
    ctx_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return _EXIT_USER_PROMPT_TEMPLATE.format(context_json=ctx_json)


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
            "entry_price": _req_float(entry_price, "entry_price", min_v=0.0),
            "last_price": _req_float(last_price, "last_price", min_v=0.0),
            "leverage": _req_float(leverage, "leverage", min_v=0.0),
            "now_utc": now,
        }
    )

    prompt = _make_exit_prompt(ctx)
    system = _EXIT_SYSTEM_PROMPT

    start_ts = time.monotonic()
    try:
        _safe_log("DEBUG", f"[EXIT_CALL] model={model} symbol={symbol} side={side} scenario={scenario}")

        data = _call_gpt_json(model=model, prompt=system + "\n\n" + prompt, purpose="EXIT")
        latency = time.monotonic() - start_ts

        raw_action = data.get("action")
        action = str(raw_action or "").upper().strip()
        if action not in ("CLOSE", "HOLD"):
            raise ValueError(f"EXIT invalid action: {raw_action!r}")

        reason = str(data.get("reason", "") or "").strip()
        if not reason:
            raise ValueError("EXIT missing reason")

        close_ratio = float(data.get("close_ratio", 1.0 if action == "CLOSE" else 0.0))
        if not math.isfinite(close_ratio) or not (0.0 <= close_ratio <= 1.0):
            raise ValueError("EXIT close_ratio out of range")

        new_sl_pct = float(data.get("new_sl_pct", 0.0))
        new_tp_pct = float(data.get("new_tp_pct", 0.0))
        for name, v in [("new_sl_pct", new_sl_pct), ("new_tp_pct", new_tp_pct)]:
            if not math.isfinite(v) or v < 0.0 or v > 0.5:
                raise ValueError(f"EXIT {name} out of range")

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

        _safe_log("INFO", f"[EXIT_OK] model={model} symbol={symbol} side={side} action={action} latency={latency:.3f}s")
        return result

    except Exception as e:
        latency = time.monotonic() - start_ts
        _safe_log(
            "ERROR",
            f"[EXIT_FAIL] model={model} symbol={symbol} side={side} scenario={scenario} "
            f"latency={latency:.3f}s err={type(e).__name__}: {e}",
        )
        _safe_tg(
            f"[EXIT_FAIL] model={model} symbol={symbol} side={side} scenario={scenario} "
            f"latency={latency:.3f}s err={type(e).__name__}: {e}"
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
    STRICT:
    - fallback_action을 사용하지 않는다(호환용 파라미터만 유지).
    - 성공 시 (action, data) 반환.
    - 실패 시 예외를 그대로 발생시킨다.
    """
    _ = fallback_action
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
    return str(data["action"]), data


__all__ = [
    "ask_entry_decision",
    "ask_exit_decision",
    "ask_exit_decision_safe",
]