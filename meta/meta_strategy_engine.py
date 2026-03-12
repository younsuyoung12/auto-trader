"""
========================================================
FILE: meta/meta_strategy_engine.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할
- Meta Strategy(레벨3) 파이프라인의 오케스트레이터:
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
- 2026-03-13:
  1) FIX(EXCEPTION): common.exceptions_strict 공통 예외 계층 적용
  2) FIX(STRICT): settings snapshot / validation context 구성 시 getattr(..., default) 제거
  3) FIX(CONTRACT): GPT 응답 / prompt 반환 / settings 키 계약 검증 강화
  4) FIX(ARCH): MetaStrategyConfig.validate_or_raise() 추가
  5) FIX(STRICT): MetaRecommendation dataclass 자체 검증 추가
- 2026-03-03:
  1) 신규 생성: Meta Strategy Engine(레벨3) 오케스트레이터
  2) GPT 출력 스키마 강제(단일 JSON object only) + 범위 검증 + 금지 행위 검증
  3) settings 스냅샷을 민감정보 없이 화이트리스트로 전달(프롬프트 측에서도 재검증)
- 2026-03-03 (PATCH):
  1) import 오류 수정:
     - strategy._gpt_engine → strategy.gpt_engine 로 변경
  2) 검증 로직 단일화:
     - meta/meta_decision_validator.py 를 사용해 응답 검증/파생값 계산을 일원화
========================================================
"""

from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from common.exceptions_strict import (
    StrictConfigError,
    StrictDataError,
    StrictExternalError,
    StrictStateError,
)
from settings import load_settings

from meta.meta_decision_validator import (
    MetaDecisionContext,
    ValidatedMetaDecision,
    validate_meta_decision_text,
)
from meta.meta_prompt_builder import MetaPromptConfig, build_meta_prompts
from meta.meta_stats_aggregator import MetaStatsConfig, build_meta_stats_dict
from strategy.gpt_engine import GptEngineError, GptJsonResponse, call_chat_json


# ─────────────────────────────────────────────
# Exceptions
# ─────────────────────────────────────────────
class MetaStrategyError(RuntimeError):
    """STRICT meta strategy pipeline base error."""


class MetaStrategyConfigError(StrictConfigError):
    """메타 전략 설정/스냅샷 계약 위반."""


class MetaStrategyContractError(StrictDataError):
    """메타 전략 payload/응답/추천 계약 위반."""


class MetaStrategyExternalError(StrictExternalError):
    """GPT 엔진 / 외부 호출 실패."""


class MetaStrategyStateError(StrictStateError):
    """메타 전략 내부 상태/불변식 위반."""


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

    def __post_init__(self) -> None:
        version = _require_nonempty_str(self.version, "recommendation.version")
        action = _require_nonempty_str(self.action, "recommendation.action")
        if action not in ("NO_CHANGE", "ADJUST_PARAMS", "RECOMMEND_SAFE_STOP"):
            _fail_contract("recommendation.action invalid (STRICT)")

        severity = _require_int(self.severity, "recommendation.severity", min_value=0, max_value=3)
        confidence = _require_float(self.confidence, "recommendation.confidence", min_value=0.0, max_value=1.0)
        ttl_sec = _require_int(self.ttl_sec, "recommendation.ttl_sec", min_value=60, max_value=86400)

        risk_multiplier_delta = _require_float(
            self.risk_multiplier_delta,
            "recommendation.risk_multiplier_delta",
            min_value=-0.50,
            max_value=0.50,
        )
        allocation_ratio_cap = _require_float(
            self.allocation_ratio_cap,
            "recommendation.allocation_ratio_cap",
            min_value=0.0,
            max_value=1.0,
        )
        rationale_short = _require_nonempty_str(self.rationale_short, "recommendation.rationale_short")

        final_risk_multiplier = _require_float(
            self.final_risk_multiplier,
            "recommendation.final_risk_multiplier",
            min_value=0.0,
            max_value=1.0,
        )
        effective_max_spread_pct = _require_float(
            self.effective_max_spread_pct,
            "recommendation.effective_max_spread_pct",
            min_value=0.0,
        )
        effective_max_price_jump_pct = _require_float(
            self.effective_max_price_jump_pct,
            "recommendation.effective_max_price_jump_pct",
            min_value=0.0,
            max_value=1.0,
        )
        effective_min_entry_volume_ratio = _require_float(
            self.effective_min_entry_volume_ratio,
            "recommendation.effective_min_entry_volume_ratio",
            min_value=0.0,
            max_value=1.0,
        )

        if not isinstance(self.tags, list):
            _fail_contract("recommendation.tags must be list (STRICT)")
        if not isinstance(self.disable_directions, list):
            _fail_contract("recommendation.disable_directions must be list (STRICT)")
        if not isinstance(self.guard_multipliers, dict):
            _fail_contract("recommendation.guard_multipliers must be dict (STRICT)")

        normalized_tags = [_require_nonempty_str(x, "recommendation.tags[]") for x in self.tags]
        normalized_dirs = [_require_nonempty_str(x, "recommendation.disable_directions[]").upper() for x in self.disable_directions]
        for d in normalized_dirs:
            if d not in ("LONG", "SHORT"):
                _fail_contract(f"recommendation.disable_directions invalid (STRICT): {d}")

        for k, v in self.guard_multipliers.items():
            _require_nonempty_str(k, "recommendation.guard_multipliers.key")
            _require_float(v, "recommendation.guard_multipliers.value", min_value=0.50, max_value=2.00)

        stats_generated_at_utc = _require_nonempty_str(
            self.stats_generated_at_utc,
            "recommendation.stats_generated_at_utc",
        )
        model = _require_nonempty_str(self.model, "recommendation.model")
        latency_sec = _require_float(self.latency_sec, "recommendation.latency_sec", min_value=0.0)

        object.__setattr__(self, "version", version)
        object.__setattr__(self, "action", action)
        object.__setattr__(self, "severity", severity)
        object.__setattr__(self, "confidence", confidence)
        object.__setattr__(self, "ttl_sec", ttl_sec)
        object.__setattr__(self, "risk_multiplier_delta", risk_multiplier_delta)
        object.__setattr__(self, "allocation_ratio_cap", allocation_ratio_cap)
        object.__setattr__(self, "rationale_short", rationale_short)
        object.__setattr__(self, "final_risk_multiplier", final_risk_multiplier)
        object.__setattr__(self, "effective_max_spread_pct", effective_max_spread_pct)
        object.__setattr__(self, "effective_max_price_jump_pct", effective_max_price_jump_pct)
        object.__setattr__(self, "effective_min_entry_volume_ratio", effective_min_entry_volume_ratio)
        object.__setattr__(self, "tags", normalized_tags)
        object.__setattr__(self, "disable_directions", normalized_dirs)
        object.__setattr__(self, "stats_generated_at_utc", stats_generated_at_utc)
        object.__setattr__(self, "model", model)
        object.__setattr__(self, "latency_sec", latency_sec)


@dataclass(frozen=True, slots=True)
class MetaStrategyConfig:
    """
    엔진 실행 설정.
    - stats_cfg: DB 통계 집계 설정
    - prompt_cfg: 프롬프트 설정
    - cooldown_sec: 같은 프로세스 내에서 연속 호출 방지
    """
    stats_cfg: MetaStatsConfig
    prompt_cfg: MetaPromptConfig
    cooldown_sec: int = 60

    def validate_or_raise(self) -> None:
        if not isinstance(self.stats_cfg, MetaStatsConfig):
            _fail_config("cfg.stats_cfg must be MetaStatsConfig (STRICT)")
        if not isinstance(self.prompt_cfg, MetaPromptConfig):
            _fail_config("cfg.prompt_cfg must be MetaPromptConfig (STRICT)")
        _require_int(self.cooldown_sec, "cfg.cooldown_sec", min_value=1)


# ─────────────────────────────────────────────
# STRICT helpers
# ─────────────────────────────────────────────
def _fail_config(reason: str, exc: Optional[BaseException] = None) -> None:
    if exc is None:
        raise MetaStrategyConfigError(reason)
    raise MetaStrategyConfigError(reason) from exc


def _fail_contract(reason: str, exc: Optional[BaseException] = None) -> None:
    if exc is None:
        raise MetaStrategyContractError(reason)
    raise MetaStrategyContractError(reason) from exc


def _fail_external(reason: str, exc: Optional[BaseException] = None) -> None:
    if exc is None:
        raise MetaStrategyExternalError(reason)
    raise MetaStrategyExternalError(reason) from exc


def _fail_state(reason: str, exc: Optional[BaseException] = None) -> None:
    if exc is None:
        raise MetaStrategyStateError(reason)
    raise MetaStrategyStateError(reason) from exc


def _require_nonempty_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        _fail_contract(f"{name} is required (STRICT)")
    return s


def _require_int(
    v: Any,
    name: str,
    *,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
) -> int:
    if v is None:
        _fail_contract(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        _fail_contract(f"{name} must be int (bool not allowed) (STRICT)")
    try:
        iv = int(v)
    except Exception as e:
        _fail_contract(f"{name} must be int (STRICT)", e)
    if min_value is not None and iv < min_value:
        _fail_contract(f"{name} must be >= {min_value} (STRICT)")
    if max_value is not None and iv > max_value:
        _fail_contract(f"{name} must be <= {max_value} (STRICT)")
    return int(iv)


def _require_float(
    v: Any,
    name: str,
    *,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> float:
    if v is None:
        _fail_contract(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        _fail_contract(f"{name} must be float (bool not allowed) (STRICT)")
    try:
        fv = float(v)
    except Exception as e:
        _fail_contract(f"{name} must be float (STRICT)", e)
    if not math.isfinite(fv):
        _fail_contract(f"{name} must be finite (STRICT)")
    if min_value is not None and fv < min_value:
        _fail_contract(f"{name} must be >= {min_value} (STRICT)")
    if max_value is not None and fv > max_value:
        _fail_contract(f"{name} must be <= {max_value} (STRICT)")
    return float(fv)


def _require_dict(v: Any, name: str) -> Dict[str, Any]:
    if not isinstance(v, dict):
        _fail_contract(f"{name} must be dict (STRICT)")
    if not v:
        _fail_contract(f"{name} must not be empty (STRICT)")
    return dict(v)


def _validate_full_json_only(text: str) -> None:
    """
    STRICT:
    - 응답 전체가 단 하나의 JSON object여야 한다(추가 텍스트/코드블록 금지).
    - meta_decision_validator에서 파싱하지만, 여기서도 “형태”를 1차 강제한다.
    """
    s = str(text or "").strip()
    if not s:
        _fail_contract("gpt response text empty (STRICT)")
    if not (s.startswith("{") and s.endswith("}")):
        _fail_contract("gpt response must be a single JSON object only (STRICT)")

    try:
        obj = json.loads(s)
    except Exception as e:
        _fail_contract("gpt response json.loads failed (STRICT)", e)
    if not isinstance(obj, dict):
        _fail_contract("gpt response root must be JSON object (STRICT)")


def _require_settings_value_strict(settings: Any, name: str) -> Any:
    if settings is None:
        _fail_config("settings resolution failed (STRICT)")
    if not hasattr(settings, name):
        _fail_config(f"settings.{name} missing (STRICT)")
    return getattr(settings, name)


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
    if s is None:
        _fail_config("load_settings() returned None (STRICT)")

    snapshot: Dict[str, Any] = {
        "allocation_ratio": float(_require_settings_value_strict(s, "allocation_ratio")),
        "leverage": float(_require_settings_value_strict(s, "leverage")),
        "max_spread_pct": float(_require_settings_value_strict(s, "max_spread_pct")),
        "max_price_jump_pct": float(_require_settings_value_strict(s, "max_price_jump_pct")),
        "min_entry_volume_ratio": float(_require_settings_value_strict(s, "min_entry_volume_ratio")),
        "slippage_block_pct": float(_require_settings_value_strict(s, "slippage_block_pct")),
        "reconcile_interval_sec": int(_require_settings_value_strict(s, "reconcile_interval_sec")),
        "force_close_on_desync": bool(_require_settings_value_strict(s, "force_close_on_desync")),
        "max_signal_latency_ms": int(_require_settings_value_strict(s, "max_signal_latency_ms")),
        "max_exec_latency_ms": int(_require_settings_value_strict(s, "max_exec_latency_ms")),
        "exit_supervisor_enabled": _require_settings_value_strict(s, "exit_supervisor_enabled"),
        "exit_supervisor_cooldown_sec": _require_settings_value_strict(s, "exit_supervisor_cooldown_sec"),
        "meta_max_rationale_sentences": _require_settings_value_strict(s, "meta_max_rationale_sentences"),
        "meta_max_rationale_len": _require_settings_value_strict(s, "meta_max_rationale_len"),
    }

    _require_float(snapshot["allocation_ratio"], "snapshot.allocation_ratio", min_value=0.0, max_value=1.0)
    _require_float(snapshot["leverage"], "snapshot.leverage", min_value=1.0)
    _require_float(snapshot["max_spread_pct"], "snapshot.max_spread_pct", min_value=0.0)
    _require_float(snapshot["max_price_jump_pct"], "snapshot.max_price_jump_pct", min_value=0.0)
    _require_float(snapshot["min_entry_volume_ratio"], "snapshot.min_entry_volume_ratio", min_value=0.0, max_value=1.0)
    _require_float(snapshot["slippage_block_pct"], "snapshot.slippage_block_pct", min_value=0.0)
    _require_int(snapshot["reconcile_interval_sec"], "snapshot.reconcile_interval_sec", min_value=1)
    if not isinstance(snapshot["force_close_on_desync"], bool):
        _fail_config("snapshot.force_close_on_desync must be bool (STRICT)")
    if not isinstance(snapshot["exit_supervisor_enabled"], bool):
        _fail_config("snapshot.exit_supervisor_enabled must be bool (STRICT)")
    _require_int(snapshot["max_signal_latency_ms"], "snapshot.max_signal_latency_ms", min_value=1)
    _require_int(snapshot["max_exec_latency_ms"], "snapshot.max_exec_latency_ms", min_value=1)
    _require_float(snapshot["exit_supervisor_cooldown_sec"], "snapshot.exit_supervisor_cooldown_sec", min_value=1.0)
    _require_int(snapshot["meta_max_rationale_sentences"], "snapshot.meta_max_rationale_sentences", min_value=1)
    _require_int(snapshot["meta_max_rationale_len"], "snapshot.meta_max_rationale_len", min_value=1)

    return snapshot


def _build_validation_context_strict(symbol: str, snapshot: Dict[str, Any]) -> MetaDecisionContext:
    """
    STRICT:
    - MetaDecisionValidator에 전달할 base 컨텍스트를 구성한다.
    - base_risk_multiplier는 현재 시스템 정책값 1.0을 명시적으로 사용한다.
      (숨은 폴백이 아니라 Meta layer 기준 정책 상수)
    """
    sym = _require_nonempty_str(symbol, "symbol").upper()
    snap = _require_dict(snapshot, "snapshot")

    base_risk_multiplier = 1.0

    return MetaDecisionContext(
        symbol=sym,
        base_risk_multiplier=_require_float(
            base_risk_multiplier,
            "ctx.base_risk_multiplier",
            min_value=0.0,
            max_value=1.0,
        ),
        base_allocation_ratio=_require_float(
            snap.get("allocation_ratio"),
            "ctx.base_allocation_ratio",
            min_value=0.0,
            max_value=1.0,
        ),
        base_max_spread_pct=_require_float(
            snap.get("max_spread_pct"),
            "ctx.base_max_spread_pct",
            min_value=0.0,
        ),
        base_max_price_jump_pct=_require_float(
            snap.get("max_price_jump_pct"),
            "ctx.base_max_price_jump_pct",
            min_value=0.0,
        ),
        base_min_entry_volume_ratio=_require_float(
            snap.get("min_entry_volume_ratio"),
            "ctx.base_min_entry_volume_ratio",
            min_value=0.0,
            max_value=1.0,
        ),
        max_rationale_sentences=_require_int(
            snap.get("meta_max_rationale_sentences"),
            "ctx.max_rationale_sentences",
            min_value=1,
        ),
        max_rationale_len=_require_int(
            snap.get("meta_max_rationale_len"),
            "ctx.max_rationale_len",
            min_value=1,
        ),
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
        risk_multiplier_delta=_require_float(
            v.risk_multiplier_delta,
            "validated.risk_multiplier_delta",
            min_value=-0.50,
            max_value=0.50,
        ),
        allocation_ratio_cap=_require_float(
            v.allocation_ratio_cap,
            "validated.allocation_ratio_cap",
            min_value=0.0,
            max_value=1.0,
        ),
        disable_directions=[_require_nonempty_str(x, "validated.disable_directions[]").upper() for x in list(v.disable_directions)],
        guard_multipliers=dict(v.guard_multipliers),
        rationale_short=_require_nonempty_str(v.rationale_short, "validated.rationale_short"),
        final_risk_multiplier=_require_float(
            v.final_risk_multiplier,
            "validated.final_risk_multiplier",
            min_value=0.0,
            max_value=1.0,
        ),
        effective_max_spread_pct=_require_float(
            v.effective_max_spread_pct,
            "validated.effective_max_spread_pct",
            min_value=0.0,
        ),
        effective_max_price_jump_pct=_require_float(
            v.effective_max_price_jump_pct,
            "validated.effective_max_price_jump_pct",
            min_value=0.0,
            max_value=1.0,
        ),
        effective_min_entry_volume_ratio=_require_float(
            v.effective_min_entry_volume_ratio,
            "validated.effective_min_entry_volume_ratio",
            min_value=0.0,
            max_value=1.0,
        ),
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
            _fail_config("cfg must be MetaStrategyConfig (STRICT)")
        cfg.validate_or_raise()
        self.cfg = cfg
        self._last_run_ts: Optional[float] = None

    def run_once(self) -> MetaRecommendation:
        now = time.time()
        if self._last_run_ts is not None and (now - self._last_run_ts) < float(self.cfg.cooldown_sec):
            _fail_state("meta strategy cooldown not elapsed (STRICT)")

        # 1) stats
        try:
            stats = build_meta_stats_dict(self.cfg.stats_cfg)
        except Exception as e:
            _fail_external("meta stats aggregation failed (STRICT)", e)
        stats_dict = _require_dict(stats, "stats")
        stats_generated_at_utc = _require_nonempty_str(stats_dict.get("generated_at_utc"), "stats.generated_at_utc")
        symbol = _require_nonempty_str(stats_dict.get("symbol"), "stats.symbol").upper()

        # 2) prompts
        settings_snapshot = _build_current_settings_snapshot_strict()
        try:
            prompt_out = build_meta_prompts(
                stats=stats_dict,
                cfg=self.cfg.prompt_cfg,
                current_settings=settings_snapshot,
            )
        except Exception as e:
            _fail_external("meta prompt build failed (STRICT)", e)

        if not isinstance(prompt_out, tuple) or len(prompt_out) != 2:
            _fail_contract("build_meta_prompts must return (system_prompt, user_payload) tuple (STRICT)")
        system_prompt, user_payload = prompt_out
        _require_nonempty_str(system_prompt, "system_prompt")
        _require_nonempty_str(user_payload, "user_payload")

        # 3) GPT call (single entry point)
        try:
            gpt: GptJsonResponse = call_chat_json(
                system_prompt=system_prompt,
                user_payload=user_payload,
            )
        except GptEngineError as e:
            _fail_external("gpt engine error (STRICT)", e)
        except Exception as e:
            _fail_external("unexpected error while calling gpt (STRICT)", e)

        text = _require_nonempty_str(getattr(gpt, "text", None), "gpt.text")
        model = _require_nonempty_str(getattr(gpt, "model", None), "gpt.model")
        latency_sec = _require_float(getattr(gpt, "latency_sec", None), "gpt.latency_sec", min_value=0.0)

        # 4) STRICT: 응답 전체가 JSON 1개인지 강제
        _validate_full_json_only(text)

        # 5) STRICT validate + derive (no clamp)
        ctx = _build_validation_context_strict(symbol, settings_snapshot)
        try:
            validated = validate_meta_decision_text(response_text=text, ctx=ctx)
        except Exception as e:
            _fail_contract("validated meta decision parse/validation failed (STRICT)", e)

        rec = _to_recommendation_strict(
            v=validated,
            stats_generated_at_utc=stats_generated_at_utc,
            model=model,
            latency_sec=latency_sec,
        )

        self._last_run_ts = now
        return rec


__all__ = [
    "MetaStrategyError",
    "MetaStrategyConfigError",
    "MetaStrategyContractError",
    "MetaStrategyExternalError",
    "MetaStrategyStateError",
    "MetaRecommendation",
    "MetaStrategyConfig",
    "MetaStrategyEngine",
]