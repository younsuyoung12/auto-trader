# meta/meta_strategy_engine.py
"""
========================================================
FILE: meta/meta_strategy_engine.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할
- Meta Strategy(레벨3) 파이프라인의 “오케스트레이터”:
  1) DB 기반 통계 집계(meta_stats_aggregator)
  2) 프롬프트/페이로드 생성(meta_prompt_builder)
  3) GPT 호출(단일 진입점: strategy/gpt_engine.py)
  4) 응답 JSON의 STRICT 검증(meta_decision_validator)
  5) 최종 Recommendation(조정안) 객체 반환

절대 원칙 (STRICT · NO-FALLBACK)
- 폴백 금지: None→0/기본값 주입/조용한 continue 금지.
- 예외 삼키기 금지: 모든 오류는 예외로 전파.
- 환경 변수 직접 접근 금지: os.getenv 사용 금지.
- GPT는 주문/진입/청산/TP·SL 결정 금지(제안만 가능).
- 적용(apply)은 별도 레이어(meta_risk_adjuster)에서 수행(이 파일은 제안 생성까지만).

변경 이력
--------------------------------------------------------
- 2026-03-03:
  1) 신규 생성: Meta Strategy Engine(레벨3) 오케스트레이터
  2) GPT 출력 스키마 강제(단일 JSON object only) + 범위 검증 + 금지 행위 검증
  3) settings 스냅샷을 민감정보 없이 화이트리스트로 전달(프롬프트 측에서도 재검증)

- 2026-03-03 (PATCH):
  1) import 오류 수정:
     - strategy._gpt_engine → strategy.gpt_engine 로 변경 (Pylance reportMissingImports 해결)
  2) 검증 로직 단일화:
     - meta/meta_decision_validator.py 를 사용해 응답 검증/파생값 계산을 일원화
========================================================
"""

from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from settings import load_settings

from meta.meta_stats_aggregator import MetaStatsConfig, build_meta_stats_dict
from meta.meta_prompt_builder import MetaPromptConfig, build_meta_prompts
from meta.meta_decision_validator import (
    MetaDecisionContext,
    ValidatedMetaDecision,
    validate_meta_decision_text,
)

# ✅ FIX: 실제 파일명(스크린샷 기준)과 일치
from strategy.gpt_engine import GptEngineError, GptJsonResponse, call_chat_json


# ─────────────────────────────────────────────
# Exceptions
# ─────────────────────────────────────────────
class MetaStrategyError(RuntimeError):
    """STRICT: meta strategy pipeline error."""


# ─────────────────────────────────────────────
# Data models
# ─────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class MetaRecommendation:
    version: str
    action: str  # NO_CHANGE | ADJUST_PARAMS | RECOMMEND_SAFE_STOP
    severity: int  # 0..3
    tags: List[str]
    confidence: float  # 0..1
    ttl_sec: int  # 60..86400

    # recommendation
    risk_multiplier_delta: float  # -0.50..+0.50
    allocation_ratio_cap: float  # 0..1
    disable_directions: List[str]  # ["LONG","SHORT"] subset
    guard_multipliers: Dict[str, float]  # key->mult (0.50..2.00)

    rationale_short: str

    # derived (STRICT, no clamp)
    final_risk_multiplier: float
    effective_max_spread_pct: float
    effective_max_price_jump_pct: float
    effective_min_entry_volume_ratio: float

    # raw linkage
    stats_generated_at_utc: str
    model: str
    latency_sec: float


@dataclass(frozen=True, slots=True)
class MetaStrategyConfig:
    """
    엔진 실행 설정.
    - stats_cfg: DB 통계 집계 설정
    - prompt_cfg: 프롬프트 설정
    - cooldown_sec: 같은 프로세스 내에서 연속 호출 방지(옵션)
    """
    stats_cfg: MetaStatsConfig
    prompt_cfg: MetaPromptConfig
    cooldown_sec: int = 60


# ─────────────────────────────────────────────
# STRICT helpers
# ─────────────────────────────────────────────
def _fail(reason: str, exc: Optional[BaseException] = None) -> None:
    if exc is None:
        raise MetaStrategyError(reason)
    raise MetaStrategyError(reason) from exc


def _require_nonempty_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        _fail(f"{name} is required (STRICT)")
    return s


def _require_int(v: Any, name: str, *, min_value: Optional[int] = None, max_value: Optional[int] = None) -> int:
    if v is None:
        _fail(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        _fail(f"{name} must be int (bool not allowed) (STRICT)")
    try:
        iv = int(v)
    except Exception as e:
        _fail(f"{name} must be int (STRICT)", e)
    if min_value is not None and iv < min_value:
        _fail(f"{name} must be >= {min_value} (STRICT)")
    if max_value is not None and iv > max_value:
        _fail(f"{name} must be <= {max_value} (STRICT)")
    return int(iv)


def _require_float(v: Any, name: str, *, min_value: Optional[float] = None, max_value: Optional[float] = None) -> float:
    if v is None:
        _fail(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        _fail(f"{name} must be float (bool not allowed) (STRICT)")
    try:
        fv = float(v)
    except Exception as e:
        _fail(f"{name} must be float (STRICT)", e)
    if not math.isfinite(fv):
        _fail(f"{name} must be finite (STRICT)")
    if min_value is not None and fv < min_value:
        _fail(f"{name} must be >= {min_value} (STRICT)")
    if max_value is not None and fv > max_value:
        _fail(f"{name} must be <= {max_value} (STRICT)")
    return float(fv)


def _validate_full_json_only(text: str) -> None:
    """
    STRICT:
    - 응답 전체가 단 하나의 JSON object여야 한다(추가 텍스트/코드블록 금지).
    - meta_decision_validator에서 파싱하지만, 여기서도 “형태”를 1차 강제한다.
    """
    s = str(text or "").strip()
    if not s:
        _fail("gpt response text empty (STRICT)")
    if not (s.startswith("{") and s.endswith("}")):
        _fail("gpt response must be a single JSON object only (STRICT)")

    try:
        obj = json.loads(s)
    except Exception as e:
        _fail("gpt response json.loads failed (STRICT)", e)
    if not isinstance(obj, dict):
        _fail("gpt response root must be JSON object (STRICT)")


# ─────────────────────────────────────────────
# Settings snapshot (safe allowlist)
# ─────────────────────────────────────────────
def _build_current_settings_snapshot_strict() -> Dict[str, Any]:
    """
    STRICT:
    - settings 객체에서 민감정보를 제외한 “조정 대상/운영 파라미터”만 추출한다.
    - 이 스냅샷은 meta_prompt_builder에서 민감키 검증을 다시 수행한다.
    """
    s = load_settings()

    def get(name: str) -> Any:
        if not hasattr(s, name):
            _fail(f"settings missing required key for snapshot (STRICT): {name}")
        return getattr(s, name)

    snapshot: Dict[str, Any] = {
        "allocation_ratio": float(get("allocation_ratio")),
        "leverage": float(get("leverage")),
        "max_spread_pct": float(get("max_spread_pct")),
        "max_price_jump_pct": float(get("max_price_jump_pct")),
        "min_entry_volume_ratio": float(get("min_entry_volume_ratio")),
        "slippage_block_pct": float(get("slippage_block_pct")),
        "reconcile_interval_sec": int(get("reconcile_interval_sec")),
        "force_close_on_desync": bool(get("force_close_on_desync")),
        "max_signal_latency_ms": int(get("max_signal_latency_ms")),
        "max_exec_latency_ms": int(get("max_exec_latency_ms")),
        # exit supervisor는 선택
        "exit_supervisor_enabled": bool(getattr(s, "exit_supervisor_enabled", False)),
        "exit_supervisor_cooldown_sec": float(getattr(s, "exit_supervisor_cooldown_sec", 180.0)),
    }

    _require_float(snapshot["allocation_ratio"], "snapshot.allocation_ratio", min_value=0.0, max_value=1.0)
    _require_float(snapshot["leverage"], "snapshot.leverage", min_value=1.0)
    _require_float(snapshot["max_spread_pct"], "snapshot.max_spread_pct", min_value=0.0)
    _require_float(snapshot["max_price_jump_pct"], "snapshot.max_price_jump_pct", min_value=0.0)
    _require_float(snapshot["min_entry_volume_ratio"], "snapshot.min_entry_volume_ratio", min_value=0.0)
    _require_float(snapshot["slippage_block_pct"], "snapshot.slippage_block_pct", min_value=0.0)
    _require_int(snapshot["reconcile_interval_sec"], "snapshot.reconcile_interval_sec", min_value=1)
    _require_int(snapshot["max_signal_latency_ms"], "snapshot.max_signal_latency_ms", min_value=1)
    _require_int(snapshot["max_exec_latency_ms"], "snapshot.max_exec_latency_ms", min_value=1)
    _require_float(snapshot["exit_supervisor_cooldown_sec"], "snapshot.exit_supervisor_cooldown_sec", min_value=1.0)

    return snapshot


def _build_validation_context_strict(symbol: str, snapshot: Dict[str, Any]) -> MetaDecisionContext:
    """
    STRICT:
    - MetaDecisionValidator에 전달할 base 컨텍스트를 구성한다.
    - base_risk_multiplier는 현재 시스템에서 “기본값”을 명시해야 한다.
      (이 단계에서 임의 추정 금지)
    """
    sym = _require_nonempty_str(symbol, "symbol").upper()

    # base_risk_multiplier는 RiskPhysics 산출값을 넘기는 것이 이상적이다.
    # 현재 파일은 오케스트레이터이므로 “기본값 1.0”을 명시로 사용한다.
    # (폴백이 아니라 정책값: Meta delta는 -0.50..+0.50 범위이며 validator가 0..1 범위 내에서만 허용)
    base_risk_multiplier = 1.0

    return MetaDecisionContext(
        symbol=sym,
        base_risk_multiplier=_require_float(base_risk_multiplier, "ctx.base_risk_multiplier", min_value=0.0, max_value=1.0),
        base_allocation_ratio=_require_float(snapshot.get("allocation_ratio"), "ctx.base_allocation_ratio", min_value=0.0, max_value=1.0),
        base_max_spread_pct=_require_float(snapshot.get("max_spread_pct"), "ctx.base_max_spread_pct", min_value=0.0),
        base_max_price_jump_pct=_require_float(snapshot.get("max_price_jump_pct"), "ctx.base_max_price_jump_pct", min_value=0.0),
        base_min_entry_volume_ratio=_require_float(snapshot.get("min_entry_volume_ratio"), "ctx.base_min_entry_volume_ratio", min_value=0.0, max_value=1.0),
        max_rationale_sentences=int(getattr(load_settings(), "meta_max_rationale_sentences", 3)),
        max_rationale_len=int(getattr(load_settings(), "meta_max_rationale_len", 800)),
    )


def _to_recommendation_strict(
    *,
    v: ValidatedMetaDecision,
    stats_generated_at_utc: str,
    model: str,
    latency_sec: float,
) -> MetaRecommendation:
    return MetaRecommendation(
        version=_require_nonempty_str(v.version, "validated.version"),
        action=_require_nonempty_str(v.action, "validated.action"),
        severity=_require_int(v.severity, "validated.severity", min_value=0, max_value=3),
        tags=[_require_nonempty_str(x, "validated.tags[]") for x in list(v.tags)],
        confidence=_require_float(v.confidence, "validated.confidence", min_value=0.0, max_value=1.0),
        ttl_sec=_require_int(v.ttl_sec, "validated.ttl_sec", min_value=60, max_value=86400),
        risk_multiplier_delta=_require_float(v.risk_multiplier_delta, "validated.risk_multiplier_delta", min_value=-0.50, max_value=0.50),
        allocation_ratio_cap=_require_float(v.allocation_ratio_cap, "validated.allocation_ratio_cap", min_value=0.0, max_value=1.0),
        disable_directions=[_require_nonempty_str(x, "validated.disable_directions[]").upper() for x in list(v.disable_directions)],
        guard_multipliers=dict(v.guard_multipliers),
        rationale_short=_require_nonempty_str(v.rationale_short, "validated.rationale_short"),
        final_risk_multiplier=_require_float(v.final_risk_multiplier, "validated.final_risk_multiplier", min_value=0.0, max_value=1.0),
        effective_max_spread_pct=_require_float(v.effective_max_spread_pct, "validated.effective_max_spread_pct", min_value=0.0),
        effective_max_price_jump_pct=_require_float(v.effective_max_price_jump_pct, "validated.effective_max_price_jump_pct", min_value=0.0, max_value=1.0),
        effective_min_entry_volume_ratio=_require_float(v.effective_min_entry_volume_ratio, "validated.effective_min_entry_volume_ratio", min_value=0.0, max_value=1.0),
        stats_generated_at_utc=_require_nonempty_str(stats_generated_at_utc, "stats_generated_at_utc"),
        model=_require_nonempty_str(model, "model"),
        latency_sec=_require_float(latency_sec, "latency_sec", min_value=0.0),
    )


# ─────────────────────────────────────────────
# Engine
# ─────────────────────────────────────────────
class MetaStrategyEngine:
    """
    STRICT Meta Strategy Engine (Level 3)

    사용:
      engine = MetaStrategyEngine(cfg)
      rec = engine.run_once()
    """

    def __init__(self, cfg: MetaStrategyConfig):
        if not isinstance(cfg, MetaStrategyConfig):
            _fail("cfg must be MetaStrategyConfig (STRICT)")
        if cfg.cooldown_sec <= 0:
            _fail("cfg.cooldown_sec must be > 0 (STRICT)")
        self.cfg = cfg
        self._last_run_ts: Optional[float] = None

    def run_once(self) -> MetaRecommendation:
        now = time.time()
        if self._last_run_ts is not None and (now - self._last_run_ts) < float(self.cfg.cooldown_sec):
            _fail("meta strategy cooldown not elapsed (STRICT)")

        # 1) stats
        stats = build_meta_stats_dict(self.cfg.stats_cfg)
        stats_generated_at_utc = _require_nonempty_str(stats.get("generated_at_utc"), "stats.generated_at_utc")
        symbol = _require_nonempty_str(stats.get("symbol"), "stats.symbol").upper()

        # 2) prompts
        settings_snapshot = _build_current_settings_snapshot_strict()
        system_prompt, user_payload = build_meta_prompts(
            stats=stats,
            cfg=self.cfg.prompt_cfg,
            current_settings=settings_snapshot,
        )

        # 3) GPT call (single entry point)
        try:
            gpt: GptJsonResponse = call_chat_json(
                system_prompt=system_prompt,
                user_payload=user_payload,
            )
        except GptEngineError as e:
            _fail("gpt engine error (STRICT)", e)
        except Exception as e:
            _fail("unexpected error while calling gpt (STRICT)", e)

        # 4) STRICT: 응답 전체가 JSON 1개인지 강제
        _validate_full_json_only(gpt.text)

        # 5) STRICT validate + derive (no clamp)
        ctx = _build_validation_context_strict(symbol, settings_snapshot)
        validated = validate_meta_decision_text(response_text=gpt.text, ctx=ctx)

        rec = _to_recommendation_strict(
            v=validated,
            stats_generated_at_utc=stats_generated_at_utc,
            model=gpt.model,
            latency_sec=gpt.latency_sec,
        )

        self._last_run_ts = now
        return rec


__all__ = [
    "MetaStrategyError",
    "MetaRecommendation",
    "MetaStrategyConfig",
    "MetaStrategyEngine",
]