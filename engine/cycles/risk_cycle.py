# engine/cycles/risk_cycle.py
"""
============================================================
FILE: engine/cycles/risk_cycle.py
ROLE:
- trading engine risk cycle
- entry packet을 받아 risk / allocation / approval 을 수행한다

CORE RESPONSIBILITIES:
- entry packet 소비
- ws hard entry guards 수행
- regime / account state / EV heatmap 계산
- meta strategy recommendation 생성 및 risk overlay 적용
- risk physics 결정
- hard risk guard 승인
- execution layer 로 넘길 finalized signal packet 생성
- STOP / SKIP / ENTER 상태를 명시적으로 분기

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- entry packet 없으면 아무것도 하지 않는다
- hard risk / data integrity / invariant 위반은 즉시 예외 또는 명시적 SKIP 처리
- hidden default / silent continue / 예외 삼키기 금지
- risk cycle 은 주문 실행을 직접 수행하지 않는다
- Meta Strategy(레벨3)는 recommendation 생성만이 아니라 risk overlay로 실제 반영되어야 한다
- Meta recommendation 생성/적용 실패는 조용히 무시하지 않고 즉시 예외 또는 SAFE_STOP 처리한다

CHANGE HISTORY:
- 2026-03-14:
  1) FEAT(META-L3): MetaStrategyEngine + MetaRiskAdjuster 를 runtime risk cycle 에 정식 연결
  2) FEAT(META-L3): active recommendation TTL / refresh state 추가
  3) FEAT(META-L3): meta guard adjustments 를 entry guards 설정 overlay 로 반영
  4) FEAT(META-L3): final allocation_ratio 를 meta overlay 로 재계산하고 audit blob 을 signal.meta 에 기록
  5) FIX(STRICT): meta recommendation 만료/refresh 실패를 명시적 예외로 승격
  6) FIX(STRICT): candidate.guard_adjustments 와 meta guard adjustments 병합 시 충돌을 즉시 차단
- 2026-03-12:
  1) FIX(ROOT-CAUSE): build_risk_cycle_context_or_raise 누락/유실 상태 복구
  2) FIX(ROOT-CAUSE): helper 정의 순서/배치를 정리해 import 안정성 복구
  3) FIX(STRICT): getattr(..., default) 제거, 필수 속성은 즉시 예외 처리
  4) FIX(RISK-FLOW): entry guard block 을 explicit SKIP 으로 처리
============================================================
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, is_dataclass, replace
from typing import Any, Callable, Dict, Iterable, List, Optional

from execution.exchange_api import get_available_usdt, get_balance_detail
from execution.invariant_guard import (
    InvariantViolation,
    SignalInvariantInputs,
    validate_signal_invariants_strict,
)
from execution.risk_manager import hard_risk_guard_check
from execution.risk_physics_engine import RiskPhysicsEngine
from infra.async_worker import submit as submit_async
from infra.market_data_ws import get_klines_with_volume as ws_get_klines_with_volume
from infra.telelog import log, send_tg
from meta.meta_prompt_builder import MetaPromptConfig
from meta.meta_risk_adjuster import (
    MetaApplyConfigError,
    MetaApplyContractError,
    MetaApplyStateError,
    apply_allocation_ratio_from_recommendation,
    build_guard_adjustments_from_recommendation,
    build_meta_l3_audit_blob,
)
from meta.meta_stats_aggregator import MetaStatsConfig
from meta.meta_strategy_engine import (
    MetaRecommendation,
    MetaStrategyConfig,
    MetaStrategyConfigError,
    MetaStrategyContractError,
    MetaStrategyEngine,
    MetaStrategyExternalError,
    MetaStrategyStateError,
)
from risk.entry_guards_ws import (
    check_5m_delay_guard,
    check_manual_position_guard,
    check_price_jump_guard,
    check_spread_guard,
    check_volume_guard,
)
from strategy.account_state_builder import (
    AccountState,
    AccountStateBuilder,
    AccountStateNotReadyError,
)
from strategy.ev_heatmap_engine import EvHeatmapEngine, HeatmapKey
from strategy.regime_engine import RegimeEngine
from strategy.signal import Signal

from engine.cycles.entry_cycle import (
    EntryCyclePacket,
    consume_pending_entry_packet_or_none,
)
from engine.engine_loop import EngineLoopRuntime

PENDING_RISK_PACKET_KEY: str = "pending_risk_packet"


class RiskCycleError(RuntimeError):
    """Base error for risk cycle."""


class RiskCycleContractError(RiskCycleError):
    """Raised when risk cycle contract is violated."""


class RiskCycleSkip(RiskCycleError):
    """Explicit non-fatal skip in risk cycle."""


def _require_bool(v: Any, name: str) -> bool:
    if not isinstance(v, bool):
        raise RiskCycleContractError(f"{name} must be bool (STRICT)")
    return bool(v)


def _require_int(v: Any, name: str, *, min_value: Optional[int] = None) -> int:
    if isinstance(v, bool):
        raise RiskCycleContractError(f"{name} must be int (bool not allowed)")
    try:
        iv = int(v)
    except Exception as e:
        raise RiskCycleContractError(f"{name} must be int (STRICT): {e}") from e
    if min_value is not None and iv < min_value:
        raise RiskCycleContractError(f"{name} must be >= {min_value} (STRICT)")
    return int(iv)


def _require_int_ms(v: Any, name: str) -> int:
    return _require_int(v, name, min_value=1)


def _require_float(v: Any, name: str, *, min_value: Optional[float] = None) -> float:
    if isinstance(v, bool):
        raise RiskCycleContractError(f"{name} must be numeric (bool not allowed)")
    try:
        x = float(v)
    except Exception as e:
        raise RiskCycleContractError(f"{name} must be numeric (STRICT): {e}") from e
    if not math.isfinite(x):
        raise RiskCycleContractError(f"{name} must be finite (STRICT)")
    if min_value is not None and x < min_value:
        raise RiskCycleContractError(f"{name} must be >= {min_value} (STRICT)")
    return float(x)


def _require_optional_positive_float(v: Any, name: str) -> Optional[float]:
    if v is None:
        return None
    return _require_float(v, name, min_value=0.0)


def _require_nonempty_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        raise RiskCycleContractError(f"{name} is empty (STRICT)")
    return s


def _normalize_symbol_strict(symbol: Any, *, name: str = "symbol") -> str:
    s = str(symbol or "").replace("-", "").replace("/", "").upper().strip()
    if not s:
        raise RiskCycleContractError(f"{name} is empty (STRICT)")
    return s


def _require_attr(obj: Any, attr_name: str, owner_name: str) -> Any:
    if obj is None:
        raise RiskCycleContractError(f"{owner_name} is None (STRICT)")
    if not hasattr(obj, attr_name):
        raise RiskCycleContractError(f"{owner_name}.{attr_name} missing (STRICT)")
    return getattr(obj, attr_name)


def _require_dict(v: Any, name: str) -> Dict[str, Any]:
    if not isinstance(v, dict):
        raise RiskCycleContractError(f"{name} must be dict (STRICT)")
    return v


def _require_list(v: Any, name: str) -> List[Any]:
    if not isinstance(v, list):
        raise RiskCycleContractError(f"{name} must be list (STRICT)")
    return v


def _require_dict_key(d: Dict[str, Any], key: str, name: str) -> Any:
    if key not in d:
        raise RiskCycleContractError(f"{name}.{key} missing (STRICT)")
    return d[key]


def _safe_send_tg(msg: str) -> None:
    try:
        ok = submit_async(send_tg, msg, critical=False, label="send_tg")
        if not ok:
            log(f"[TG][DROP] async queue full: {msg}")
    except Exception as e:
        log(f"[TG] async submit error: {type(e).__name__}: {e} | msg={msg}")


@dataclass(frozen=True)
class RiskCycleConfig:
    risk_block_tg_cooldown_sec: int

    def validate_or_raise(self) -> None:
        _require_int(self.risk_block_tg_cooldown_sec, "config.risk_block_tg_cooldown_sec", min_value=1)


@dataclass
class RiskCycleState:
    persisted_equity_peak_usdt: Optional[float] = None
    last_risk_block_tg_ts: float = 0.0
    last_risk_block_key: str = ""
    active_meta_recommendation: Optional[MetaRecommendation] = None
    active_meta_expires_ts: float = 0.0
    last_meta_refresh_ts: float = 0.0

    def validate_or_raise(self) -> None:
        _require_optional_positive_float(self.persisted_equity_peak_usdt, "state.persisted_equity_peak_usdt")
        _require_float(self.last_risk_block_tg_ts, "state.last_risk_block_tg_ts", min_value=0.0)
        _require_float(self.active_meta_expires_ts, "state.active_meta_expires_ts", min_value=0.0)
        _require_float(self.last_meta_refresh_ts, "state.last_meta_refresh_ts", min_value=0.0)

        if not isinstance(self.last_risk_block_key, str):
            raise RiskCycleContractError("state.last_risk_block_key must be str (STRICT)")
        if self.active_meta_recommendation is not None and not isinstance(self.active_meta_recommendation, MetaRecommendation):
            raise RiskCycleContractError("state.active_meta_recommendation must be MetaRecommendation or None (STRICT)")


@dataclass(frozen=True)
class RiskCyclePacket:
    symbol: str
    market_data: Dict[str, Any]
    candidate: Any
    signal_final: Signal
    account_state: AccountState
    regime_decision: Any
    risk_decision: Any
    created_at_ts: float
    available_usdt: float
    entry_price_hint: float
    entry_price_source: str
    hard_risk_reason: str
    hard_risk_extra: Dict[str, Any]


@dataclass(frozen=True)
class RiskCycleContext:
    settings: Any
    symbol: str
    regime_engine: RegimeEngine
    account_builder: AccountStateBuilder
    ev_heatmap: EvHeatmapEngine
    risk_physics: RiskPhysicsEngine
    meta_engine: MetaStrategyEngine
    closed_trades_getter: Callable[[], Iterable[Dict[str, Any]]]
    config: RiskCycleConfig
    state: RiskCycleState

    def validate_or_raise(self) -> None:
        _ = _normalize_symbol_strict(self.symbol, name="context.symbol")

        if self.settings is None:
            raise RiskCycleContractError("context.settings is required (STRICT)")
        if not isinstance(self.regime_engine, RegimeEngine):
            raise RiskCycleContractError("context.regime_engine must be RegimeEngine (STRICT)")
        if not isinstance(self.account_builder, AccountStateBuilder):
            raise RiskCycleContractError("context.account_builder must be AccountStateBuilder (STRICT)")
        if not isinstance(self.ev_heatmap, EvHeatmapEngine):
            raise RiskCycleContractError("context.ev_heatmap must be EvHeatmapEngine (STRICT)")
        if not isinstance(self.risk_physics, RiskPhysicsEngine):
            raise RiskCycleContractError("context.risk_physics must be RiskPhysicsEngine (STRICT)")
        if not isinstance(self.meta_engine, MetaStrategyEngine):
            raise RiskCycleContractError("context.meta_engine must be MetaStrategyEngine (STRICT)")
        if not callable(self.closed_trades_getter):
            raise RiskCycleContractError("context.closed_trades_getter is required (STRICT)")

        self.config.validate_or_raise()
        self.state.validate_or_raise()

        required_setting_names = (
            "symbol",
            "leverage",
            "entry_cooldown_sec",
            "hard_daily_loss_limit_usdt",
            "hard_consecutive_losses_limit",
            "hard_position_value_pct_cap",
            "hard_liquidation_distance_pct_min",
            "test_dry_run",
            "test_fake_available_usdt",
            "meta_lookback_trades",
            "meta_recent_window",
            "meta_baseline_window",
            "meta_min_trades_required",
            "meta_max_recent_trades",
            "meta_language",
            "meta_max_output_sentences",
            "meta_prompt_version",
            "meta_cooldown_sec",
            "meta_max_rationale_sentences",
            "meta_max_rationale_len",
            "allocation_ratio",
            "max_spread_pct",
            "max_price_jump_pct",
            "min_entry_volume_ratio",
        )
        for name in required_setting_names:
            if not hasattr(self.settings, name):
                raise RiskCycleContractError(f"settings.{name} is required (STRICT)")


def _build_meta_strategy_engine_or_raise(settings: Any, symbol: str) -> MetaStrategyEngine:
    if settings is None:
        raise RiskCycleContractError("settings is required for meta engine (STRICT)")

    try:
        stats_cfg = MetaStatsConfig(
            symbol=_normalize_symbol_strict(symbol),
            lookback_trades=_require_int(_require_attr(settings, "meta_lookback_trades", "settings"), "settings.meta_lookback_trades", min_value=1),
            recent_window=_require_int(_require_attr(settings, "meta_recent_window", "settings"), "settings.meta_recent_window", min_value=1),
            baseline_window=_require_int(_require_attr(settings, "meta_baseline_window", "settings"), "settings.meta_baseline_window", min_value=1),
            min_trades_required=_require_int(_require_attr(settings, "meta_min_trades_required", "settings"), "settings.meta_min_trades_required", min_value=1),
            exclude_test_trades=True,
        )
        prompt_cfg = MetaPromptConfig(
            max_recent_trades=_require_int(_require_attr(settings, "meta_max_recent_trades", "settings"), "settings.meta_max_recent_trades", min_value=1),
            language=_require_nonempty_str(_require_attr(settings, "meta_language", "settings"), "settings.meta_language"),
            max_output_sentences=_require_int(_require_attr(settings, "meta_max_output_sentences", "settings"), "settings.meta_max_output_sentences", min_value=1),
            prompt_version=_require_nonempty_str(_require_attr(settings, "meta_prompt_version", "settings"), "settings.meta_prompt_version"),
        )
        cfg = MetaStrategyConfig(
            stats_cfg=stats_cfg,
            prompt_cfg=prompt_cfg,
            cooldown_sec=_require_int(_require_attr(settings, "meta_cooldown_sec", "settings"), "settings.meta_cooldown_sec", min_value=1),
        )
        return MetaStrategyEngine(cfg)
    except (
        MetaStrategyConfigError,
        MetaStrategyContractError,
        MetaStrategyStateError,
        MetaApplyConfigError,
        MetaApplyContractError,
        MetaApplyStateError,
    ) as e:
        raise RiskCycleContractError(f"meta strategy engine build failed (STRICT): {e}") from e
    except Exception as e:
        raise RiskCycleContractError(f"meta strategy engine build failed (STRICT): {e}") from e


def build_risk_cycle_context_or_raise(
    *,
    settings: Any,
    symbol: str,
    regime_engine: RegimeEngine,
    account_builder: AccountStateBuilder,
    ev_heatmap: EvHeatmapEngine,
    risk_physics: RiskPhysicsEngine,
    closed_trades_getter: Callable[[], Iterable[Dict[str, Any]]],
    persisted_equity_peak_usdt: Optional[float],
) -> RiskCycleContext:
    normalized_symbol = _normalize_symbol_strict(symbol)
    meta_engine = _build_meta_strategy_engine_or_raise(settings, normalized_symbol)

    ctx = RiskCycleContext(
        settings=settings,
        symbol=normalized_symbol,
        regime_engine=regime_engine,
        account_builder=account_builder,
        ev_heatmap=ev_heatmap,
        risk_physics=risk_physics,
        meta_engine=meta_engine,
        closed_trades_getter=closed_trades_getter,
        config=RiskCycleConfig(
            risk_block_tg_cooldown_sec=60,
        ),
        state=RiskCycleState(
            persisted_equity_peak_usdt=persisted_equity_peak_usdt,
        ),
    )
    ctx.validate_or_raise()
    return ctx


def build_risk_cycle_fn(ctx: RiskCycleContext) -> Callable[[float, EngineLoopRuntime], None]:
    ctx.validate_or_raise()

    def _fn(now_ts: float, runtime: EngineLoopRuntime) -> None:
        run_risk_cycle_or_raise(now_ts, runtime, ctx)

    return _fn


def _resolve_active_meta_recommendation_or_raise(
    *,
    ctx: RiskCycleContext,
    now_ts: float,
    symbol: str,
) -> MetaRecommendation:
    ctx.validate_or_raise()
    now_f = _require_float(now_ts, "now_ts", min_value=0.0)
    _ = _normalize_symbol_strict(symbol, name="symbol")

    active = ctx.state.active_meta_recommendation
    if active is not None:
        if now_f <= float(ctx.state.active_meta_expires_ts):
            return active

        cooldown_sec = _require_int(
            _require_attr(ctx.settings, "meta_cooldown_sec", "settings"),
            "settings.meta_cooldown_sec",
            min_value=1,
        )
        elapsed = now_f - float(ctx.state.last_meta_refresh_ts)
        if elapsed < float(cooldown_sec):
            raise RiskCycleError(
                f"[SAFE_STOP][META_EXPIRED] recommendation expired before cooldown elapsed "
                f"(elapsed={elapsed:.2f}s cooldown={cooldown_sec}s symbol={symbol})"
            )

    try:
        rec = ctx.meta_engine.run_once()
    except (
        MetaStrategyConfigError,
        MetaStrategyContractError,
        MetaStrategyStateError,
        MetaStrategyExternalError,
    ) as e:
        raise RiskCycleError(f"[SAFE_STOP][META_ENGINE] {e}") from e
    except Exception as e:
        raise RiskCycleError(f"[SAFE_STOP][META_ENGINE] unexpected failure: {e}") from e

    if not isinstance(rec, MetaRecommendation):
        raise RiskCycleContractError("meta_engine.run_once() must return MetaRecommendation (STRICT)")

    ctx.state.active_meta_recommendation = rec
    ctx.state.last_meta_refresh_ts = now_f
    ctx.state.active_meta_expires_ts = now_f + float(_require_int(rec.ttl_sec, "rec.ttl_sec", min_value=1))
    return rec


def _enforce_meta_direction_policy_or_raise(
    *,
    runtime: EngineLoopRuntime,
    rec: MetaRecommendation,
    direction: str,
) -> None:
    _ = _require_nonempty_str(direction, "direction").upper()

    if rec.action == "RECOMMEND_SAFE_STOP":
        runtime.safe_stop_requested = True
        raise RiskCycleError(
            f"[SAFE_STOP][META_L3] action=RECOMMEND_SAFE_STOP rationale={rec.rationale_short}"
        )

    if direction in rec.disable_directions:
        raise RiskCycleSkip(
            f"meta_l3 disabled direction={direction} action={rec.action} rationale={rec.rationale_short}"
        )


def _apply_meta_guard_adjustments_to_settings_or_raise(
    settings: Any,
    guard_adjustments: Dict[str, float],
) -> Any:
    if settings is None:
        raise RiskCycleContractError("settings is required for guard overlay (STRICT)")
    if not isinstance(guard_adjustments, dict):
        raise RiskCycleContractError("guard_adjustments must be dict (STRICT)")
    if not is_dataclass(settings):
        raise RiskCycleContractError("settings must be dataclass instance for guard overlay (STRICT)")

    max_spread_pct = _require_float(
        _require_dict_key(guard_adjustments, "max_spread_pct", "guard_adjustments"),
        "guard_adjustments.max_spread_pct",
        min_value=0.0,
    )
    max_price_jump_pct = _require_float(
        _require_dict_key(guard_adjustments, "max_price_jump_pct", "guard_adjustments"),
        "guard_adjustments.max_price_jump_pct",
        min_value=0.0,
    )
    min_entry_volume_ratio = _require_float(
        _require_dict_key(guard_adjustments, "min_entry_volume_ratio", "guard_adjustments"),
        "guard_adjustments.min_entry_volume_ratio",
        min_value=0.0,
    )

    try:
        return replace(
            settings,
            max_spread_pct=float(max_spread_pct),
            max_price_jump_pct=float(max_price_jump_pct),
            min_entry_volume_ratio=float(min_entry_volume_ratio),
        )
    except Exception as e:
        raise RiskCycleContractError(f"failed to overlay meta guard adjustments into settings (STRICT): {e}") from e


def _merge_guard_adjustments_strict(
    base_adjustments: Dict[str, Any],
    overlay_adjustments: Dict[str, float],
) -> Dict[str, Any]:
    base = _require_dict(base_adjustments, "base_guard_adjustments")
    overlay = _require_dict(overlay_adjustments, "overlay_guard_adjustments")

    out = dict(base)
    for key, value in overlay.items():
        new_value = _require_float(value, f"overlay_guard_adjustments[{key}]", min_value=0.0)
        if key in out:
            old_value = _require_float(out[key], f"base_guard_adjustments[{key}]", min_value=0.0)
            if abs(float(old_value) - float(new_value)) > 1e-12:
                raise RiskCycleContractError(
                    f"guard_adjustments conflict on key={key!r} "
                    f"(base={old_value}, overlay={new_value}) (STRICT)"
                )
        out[key] = float(new_value)
    return out


def run_risk_cycle_or_raise(
    now_ts: float,
    runtime: EngineLoopRuntime,
    ctx: RiskCycleContext,
) -> None:
    ctx.validate_or_raise()
    runtime.validate_or_raise()
    now_f = _require_float(now_ts, "now_ts", min_value=0.0)

    _clear_stale_pending_risk_packet_if_present(runtime)

    entry_packet = consume_pending_entry_packet_or_none(runtime)
    if entry_packet is None:
        return

    _validate_entry_packet_or_raise(entry_packet)

    market_data = _require_dict(entry_packet.market_data, "entry_packet.market_data")
    candidate = entry_packet.candidate

    symbol = _normalize_symbol_strict(_require_dict_key(market_data, "symbol", "market_data"), name="market_data.symbol")
    direction = _require_nonempty_str(_require_attr(candidate, "direction", "candidate"), "candidate.direction").upper()
    if direction not in ("LONG", "SHORT"):
        raise RiskCycleContractError(f"candidate.direction invalid (STRICT): {direction!r}")

    signal_source = _require_nonempty_str(_require_dict_key(market_data, "signal_source", "market_data"), "market_data.signal_source")
    signal_ts_ms = _require_int_ms(_require_dict_key(market_data, "signal_ts_ms", "market_data"), "market_data.signal_ts_ms")

    meta_rec = _resolve_active_meta_recommendation_or_raise(
        ctx=ctx,
        now_ts=now_f,
        symbol=symbol,
    )

    try:
        _enforce_meta_direction_policy_or_raise(
            runtime=runtime,
            rec=meta_rec,
            direction=direction,
        )
    except RiskCycleSkip as e:
        msg = f"[SKIP][META_L3] {e}"
        log(msg)
        _maybe_send_risk_block_tg(ctx, "META_L3_SKIP", msg)
        return

    try:
        meta_guard_adjustments = build_guard_adjustments_from_recommendation(ctx.settings, meta_rec)
    except (
        MetaApplyConfigError,
        MetaApplyContractError,
        MetaApplyStateError,
    ) as e:
        raise RiskCycleError(f"[SAFE_STOP][META_APPLY_GUARDS] {e}") from e
    except Exception as e:
        raise RiskCycleError(f"[SAFE_STOP][META_APPLY_GUARDS] unexpected failure: {e}") from e

    entry_guard_settings = _apply_meta_guard_adjustments_to_settings_or_raise(
        ctx.settings,
        meta_guard_adjustments,
    )

    try:
        _run_entry_guards_or_skip(
            settings=entry_guard_settings,
            symbol=symbol,
            direction=direction,
            signal_source=signal_source,
            signal_ts_ms=signal_ts_ms,
            market_data=market_data,
        )
    except RiskCycleSkip as e:
        msg = f"[SKIP][ENTRY_GUARD] {e}"
        log(msg)
        _maybe_send_risk_block_tg(ctx, f"ENTRY_GUARD:{str(e)}", msg)
        return

    market_features = _require_dict(_require_dict_key(market_data, "market_features", "market_data"), "market_data.market_features")
    engine_scores = _require_dict(_require_dict_key(market_features, "engine_scores", "market_features"), "market_features.engine_scores")
    total = _require_dict(_require_dict_key(engine_scores, "total", "engine_scores"), "engine_scores.total")
    regime_score = _require_float(_require_dict_key(total, "score", "engine_scores.total"), "engine_scores.total.score", min_value=0.0)

    ctx.regime_engine.update(regime_score)
    regime_decision = ctx.regime_engine.decide(regime_score)
    regime_allocation = _require_float(
        _require_attr(regime_decision, "allocation", "regime_decision"),
        "regime_decision.allocation",
        min_value=0.0,
    )
    regime_band = _require_nonempty_str(
        _require_attr(regime_decision, "band", "regime_decision"),
        "regime_decision.band",
    )

    if regime_allocation <= 0.0:
        regime_decision_score = _require_float(_require_attr(regime_decision, "score", "regime_decision"), "regime_decision.score")
        msg = f"[SKIP][REGIME_NO_TRADE] score={regime_decision_score:.1f} band={regime_band}"
        log(msg)
        _maybe_send_risk_block_tg(ctx, "REGIME_NO_TRADE", msg)
        return

    equity_current_usdt = _get_equity_current_usdt_strict(ctx.settings)
    account_state = _build_account_state_or_skip(
        ctx,
        symbol=symbol,
        equity_current_usdt=equity_current_usdt,
    )
    if account_state is None:
        return

    ctx.state.persisted_equity_peak_usdt = float(account_state.equity_peak_usdt)

    micro = _require_dict(_require_dict_key(market_features, "microstructure", "market_features"), "market_features.microstructure")
    micro_score_risk = _require_float(
        _require_dict_key(micro, "micro_score_risk", "microstructure"),
        "microstructure.micro_score_risk",
        min_value=0.0,
    )
    if micro_score_risk > 100.0:
        raise RiskCycleContractError(f"micro_score_risk out of range (STRICT): {micro_score_risk}")

    hk: HeatmapKey = ctx.ev_heatmap.build_key(
        regime_band=str(regime_band),
        micro_score_risk=float(micro_score_risk),
        engine_total_score=float(regime_score),
    )
    cell = ctx.ev_heatmap.get_cell_status(hk)

    tp_pct = _require_float(_require_attr(candidate, "tp_pct", "candidate"), "candidate.tp_pct", min_value=0.0)
    sl_pct = _require_float(_require_attr(candidate, "sl_pct", "candidate"), "candidate.sl_pct", min_value=0.0)
    if tp_pct <= 0.0:
        raise RiskCycleContractError("candidate.tp_pct must be > 0 (STRICT)")
    if sl_pct <= 0.0:
        raise RiskCycleContractError("candidate.sl_pct must be > 0 (STRICT)")

    risk_decision = ctx.risk_physics.decide(
        regime_allocation=float(regime_allocation),
        dd_pct=float(account_state.dd_pct),
        consecutive_losses=int(account_state.consecutive_losses),
        tp_pct=float(tp_pct),
        sl_pct=float(sl_pct),
        micro_score_risk=float(micro_score_risk),
        heatmap_status=str(cell.status),
        heatmap_ev=cell.ev,
        heatmap_n=int(cell.n),
    )

    action_override = _require_nonempty_str(
        _require_attr(risk_decision, "action_override", "risk_decision"),
        "risk_decision.action_override",
    ).upper()

    if action_override == "STOP":
        runtime.safe_stop_requested = True
        msg = f"[SAFE_STOP][RISK_PHYSICS_STOP] {_require_attr(risk_decision, 'reason', 'risk_decision')}"
        log(msg)
        _safe_send_tg(msg)
        raise RiskCycleError(msg)

    if action_override == "SKIP":
        msg = f"[SKIP][RISK_PHYSICS] {_require_attr(risk_decision, 'reason', 'risk_decision')}"
        log(msg)
        _maybe_send_risk_block_tg(ctx, "RISK_PHYSICS_SKIP", msg)
        return

    base_effective_risk_pct = _require_float(
        _require_attr(risk_decision, "effective_risk_pct", "risk_decision"),
        "risk_decision.effective_risk_pct",
        min_value=0.0,
    )
    if base_effective_risk_pct <= 0.0:
        raise RiskCycleContractError("ENTER decision must have effective_risk_pct > 0 (STRICT)")

    try:
        effective_risk_pct = apply_allocation_ratio_from_recommendation(
            base_allocation_ratio=float(base_effective_risk_pct),
            rec=meta_rec,
        )
    except (
        MetaApplyConfigError,
        MetaApplyContractError,
        MetaApplyStateError,
    ) as e:
        raise RiskCycleError(f"[SAFE_STOP][META_APPLY_ALLOC] {e}") from e
    except Exception as e:
        raise RiskCycleError(f"[SAFE_STOP][META_APPLY_ALLOC] unexpected failure: {e}") from e

    if effective_risk_pct <= 0.0:
        msg = f"[SKIP][META_L3_ZERO_ALLOC] action={meta_rec.action} rationale={meta_rec.rationale_short}"
        log(msg)
        _maybe_send_risk_block_tg(ctx, "META_L3_ZERO_ALLOC", msg)
        return

    try:
        meta_audit_blob = build_meta_l3_audit_blob(
            meta_rec,
            base_allocation_ratio=float(base_effective_risk_pct),
            final_allocation_ratio=float(effective_risk_pct),
        )
    except (
        MetaApplyConfigError,
        MetaApplyContractError,
        MetaApplyStateError,
    ) as e:
        raise RiskCycleError(f"[SAFE_STOP][META_AUDIT] {e}") from e
    except Exception as e:
        raise RiskCycleError(f"[SAFE_STOP][META_AUDIT] unexpected failure: {e}") from e

    entry_price_hint, entry_price_source = _resolve_entry_price_hint_for_signal_or_raise(
        market_data,
        _require_attr(candidate, "meta", "candidate"),
    )

    available_usdt = _get_available_usdt_strict()
    leverage = _require_float(_require_attr(ctx.settings, "leverage", "settings"), "settings.leverage", min_value=0.0)
    if leverage <= 0.0:
        raise RiskCycleContractError("settings.leverage must be > 0 (STRICT)")

    incoming_notional = float(available_usdt) * float(effective_risk_pct) * float(leverage)
    if not math.isfinite(incoming_notional) or incoming_notional <= 0.0:
        raise RiskCycleContractError("incoming_notional invalid (STRICT)")

    hard_ok, hard_reason, hard_extra = hard_risk_guard_check(
        ctx.settings,
        symbol=symbol,
        side=direction,
        entry_price=float(entry_price_hint),
        notional=float(incoming_notional),
        available_usdt=float(available_usdt),
    )
    if not isinstance(hard_ok, bool):
        raise RiskCycleContractError("hard_risk_guard_check.ok must be bool (STRICT)")
    hard_reason_s = _require_nonempty_str(hard_reason, "hard_risk_guard_check.reason")
    if not isinstance(hard_extra, dict):
        raise RiskCycleContractError("hard_risk_guard_check.extra must be dict (STRICT)")

    if not hard_ok:
        msg = f"[SKIP][HARD_RISK] {hard_reason_s}"
        log(msg)
        _maybe_send_risk_block_tg(ctx, f"HARD_RISK:{hard_reason_s}", msg)
        return

    try:
        validate_signal_invariants_strict(
            SignalInvariantInputs(
                symbol=str(symbol),
                direction=str(direction),
                risk_pct=float(effective_risk_pct),
                tp_pct=float(tp_pct),
                sl_pct=float(sl_pct),
                dd_pct=float(account_state.dd_pct),
                micro_score_risk=float(micro_score_risk),
                final_risk_multiplier=float(meta_rec.final_risk_multiplier),
                equity_current_usdt=float(account_state.equity_current_usdt),
                equity_peak_usdt=float(account_state.equity_peak_usdt),
            )
        )
    except InvariantViolation as e:
        raise RiskCycleError(f"[SAFE_STOP][INVARIANT] {e}") from e

    raw_meta = _require_dict(_require_attr(candidate, "meta", "candidate"), "candidate.meta")
    candidate_guard_adjustments = _require_dict(
        _require_attr(candidate, "guard_adjustments", "candidate"),
        "candidate.guard_adjustments",
    )
    merged_guard_adjustments = _merge_guard_adjustments_strict(
        candidate_guard_adjustments,
        meta_guard_adjustments,
    )

    meta2 = dict(raw_meta)
    _purge_signal_execution_price_aliases_inplace(meta2)
    meta2.update(
        {
            "entry_price_hint": float(entry_price_hint),
            "entry_price_source": str(entry_price_source),
            "equity_current_usdt": float(account_state.equity_current_usdt),
            "equity_peak_usdt": float(account_state.equity_peak_usdt),
            "dd_pct": float(account_state.dd_pct),
            "consecutive_losses": int(account_state.consecutive_losses),
            "risk_physics_reason": str(_require_attr(risk_decision, "reason", "risk_decision")),
            "dynamic_allocation_ratio": float(effective_risk_pct),
            "dynamic_risk_pct": float(effective_risk_pct),
            "risk_physics_effective_risk_pct": float(base_effective_risk_pct),
            "regime_score": float(_require_attr(regime_decision, "score", "regime_decision")),
            "regime_band": str(regime_band),
            "regime_allocation": float(regime_allocation),
            "micro_score_risk": float(micro_score_risk),
            "ev_cell_key": f"{hk.regime_band}|{hk.distortion_bucket}|{hk.score_bucket}",
            "ev_cell_status": str(cell.status),
            "ev_cell_ev": cell.ev,
            "ev_cell_n": int(cell.n),
            "auto_blocked": bool(_require_attr(risk_decision, "auto_blocked", "risk_decision")),
            "auto_risk_multiplier": float(_require_attr(risk_decision, "auto_risk_multiplier", "risk_decision")),
            "heatmap_status": _require_attr(risk_decision, "heatmap_status", "risk_decision"),
            "heatmap_ev": _require_attr(risk_decision, "heatmap_ev", "risk_decision"),
            "heatmap_n": _require_attr(risk_decision, "heatmap_n", "risk_decision"),
            "hard_risk_reason": str(hard_reason_s),
            "hard_risk_extra": dict(hard_extra),
            "available_usdt": float(available_usdt),
            "planned_incoming_notional": float(incoming_notional),
            "meta_l3_action": str(meta_rec.action),
            "meta_l3_severity": int(meta_rec.severity),
            "meta_l3_tags": list(meta_rec.tags),
            "meta_l3_confidence": float(meta_rec.confidence),
            "meta_l3_ttl_sec": int(meta_rec.ttl_sec),
            "meta_l3_disable_directions": list(meta_rec.disable_directions),
            "meta_l3_guard_multipliers": dict(meta_rec.guard_multipliers),
            "meta_l3_base_allocation_ratio": float(base_effective_risk_pct),
            "meta_l3_final_allocation_ratio": float(effective_risk_pct),
            "meta_l3_audit": dict(meta_audit_blob),
            "meta_l3_guard_adjustments": dict(meta_guard_adjustments),
            "meta_l3_rationale_short": str(meta_rec.rationale_short),
        }
    )

    signal_final = Signal(
        action="ENTER",
        direction=str(direction),
        tp_pct=float(tp_pct),
        sl_pct=float(sl_pct),
        risk_pct=float(effective_risk_pct),
        reason=str(_require_attr(candidate, "reason", "candidate")),
        guard_adjustments=dict(merged_guard_adjustments),
        meta=meta2,
    )

    packet = RiskCyclePacket(
        symbol=symbol,
        market_data=market_data,
        candidate=candidate,
        signal_final=signal_final,
        account_state=account_state,
        regime_decision=regime_decision,
        risk_decision=risk_decision,
        created_at_ts=now_f,
        available_usdt=float(available_usdt),
        entry_price_hint=float(entry_price_hint),
        entry_price_source=str(entry_price_source),
        hard_risk_reason=str(hard_reason_s),
        hard_risk_extra=dict(hard_extra),
    )
    runtime.extra[PENDING_RISK_PACKET_KEY] = packet


def consume_pending_risk_packet_or_none(runtime: EngineLoopRuntime) -> Optional[RiskCyclePacket]:
    runtime.validate_or_raise()
    raw = runtime.extra.pop(PENDING_RISK_PACKET_KEY, None)
    if raw is None:
        return None
    if not isinstance(raw, RiskCyclePacket):
        raise RiskCycleContractError(
            f"runtime.extra[{PENDING_RISK_PACKET_KEY!r}] must be RiskCyclePacket (STRICT)"
        )
    return raw


def peek_pending_risk_packet_or_none(runtime: EngineLoopRuntime) -> Optional[RiskCyclePacket]:
    runtime.validate_or_raise()
    raw = runtime.extra.get(PENDING_RISK_PACKET_KEY)
    if raw is None:
        return None
    if not isinstance(raw, RiskCyclePacket):
        raise RiskCycleContractError(
            f"runtime.extra[{PENDING_RISK_PACKET_KEY!r}] must be RiskCyclePacket (STRICT)"
        )
    return raw


def _clear_stale_pending_risk_packet_if_present(runtime: EngineLoopRuntime) -> None:
    raw = runtime.extra.get(PENDING_RISK_PACKET_KEY)
    if raw is None:
        return
    if not isinstance(raw, RiskCyclePacket):
        raise RiskCycleContractError(
            f"runtime.extra[{PENDING_RISK_PACKET_KEY!r}] must be RiskCyclePacket (STRICT)"
        )


def _validate_entry_packet_or_raise(packet: EntryCyclePacket) -> None:
    if not isinstance(packet, EntryCyclePacket):
        raise RiskCycleContractError("entry packet must be EntryCyclePacket (STRICT)")
    _ = _normalize_symbol_strict(packet.symbol, name="entry_packet.symbol")
    _ = _require_int_ms(packet.authoritative_signal_ts_ms, "entry_packet.authoritative_signal_ts_ms")
    _ = _require_dict(packet.market_data, "entry_packet.market_data")
    _ = _require_float(packet.created_at_ts, "entry_packet.created_at_ts", min_value=0.0)


def _run_entry_guards_or_skip(
    *,
    settings: Any,
    symbol: str,
    direction: str,
    signal_source: str,
    signal_ts_ms: int,
    market_data: Dict[str, Any],
) -> None:
    candles_5m = _require_list(_require_dict_key(market_data, "candles_5m", "market_data"), "market_data.candles_5m")
    candles_5m_raw = _require_list(_require_dict_key(market_data, "candles_5m_raw", "market_data"), "market_data.candles_5m_raw")

    candles_1m = ws_get_klines_with_volume(symbol, "1m", limit=120)
    candles_1m = _require_list(candles_1m, "ws_get_klines_with_volume(1m)")

    ok_manual = check_manual_position_guard(
        get_balance_detail_func=lambda: get_balance_detail("USDT"),
        symbol=symbol,
        latest_ts=int(signal_ts_ms),
    )
    if not isinstance(ok_manual, bool):
        raise RiskCycleContractError("check_manual_position_guard must return bool (STRICT)")
    if not ok_manual:
        raise RiskCycleSkip("manual_position_guard blocked entry")

    ok_volume = check_volume_guard(
        settings=settings,
        candles_5m_raw=candles_5m_raw,
        latest_ts=int(signal_ts_ms),
        signal_source=signal_source,
        direction=direction,
    )
    if not isinstance(ok_volume, bool):
        raise RiskCycleContractError("check_volume_guard must return bool (STRICT)")
    if not ok_volume:
        raise RiskCycleSkip("volume_guard blocked entry")

    ok_delay = check_5m_delay_guard(
        settings=settings,
        candles_1m=candles_1m,
        candles_5m=candles_5m,
        latest_ts=int(signal_ts_ms),
        signal_source=signal_source,
        direction=direction,
    )
    if not isinstance(ok_delay, bool):
        raise RiskCycleContractError("check_5m_delay_guard must return bool (STRICT)")
    if not ok_delay:
        raise RiskCycleSkip("5m_delay_guard blocked entry")

    ok_jump = check_price_jump_guard(
        settings=settings,
        candles_5m=candles_5m,
        latest_ts=int(signal_ts_ms),
        signal_source=signal_source,
        direction=direction,
    )
    if not isinstance(ok_jump, bool):
        raise RiskCycleContractError("check_price_jump_guard must return bool (STRICT)")
    if not ok_jump:
        raise RiskCycleSkip("price_jump_guard blocked entry")

    ok_spread, best_bid, best_ask = check_spread_guard(
        settings=settings,
        symbol=symbol,
        latest_ts=int(signal_ts_ms),
        signal_source=signal_source,
        direction=direction,
    )
    if not isinstance(ok_spread, bool):
        raise RiskCycleContractError("check_spread_guard.ok must be bool (STRICT)")
    if best_bid is not None:
        _ = _require_float(best_bid, "check_spread_guard.best_bid", min_value=0.0)
    if best_ask is not None:
        _ = _require_float(best_ask, "check_spread_guard.best_ask", min_value=0.0)
    if not ok_spread:
        raise RiskCycleSkip("spread_guard blocked entry")


def _get_equity_current_usdt_strict(settings: Any) -> float:
    test_dry_run = _require_bool(_require_attr(settings, "test_dry_run", "settings"), "settings.test_dry_run")
    if test_dry_run:
        fake = _require_float(
            _require_attr(settings, "test_fake_available_usdt", "settings"),
            "settings.test_fake_available_usdt",
            min_value=0.0,
        )
        if fake <= 0.0:
            raise RiskCycleContractError("settings.test_fake_available_usdt must be > 0 (STRICT)")
        return float(fake)

    row = get_balance_detail("USDT")
    row = _require_dict(row, "get_balance_detail('USDT')")

    def _must_float(key: str) -> float:
        if key not in row:
            raise RiskCycleContractError(f"balance detail missing key: {key}")
        return _require_float(row[key], f"balance.{key}")

    available = _must_float("availableBalance")
    cross_unpnl = _must_float("crossUnPnl")

    wallet = None
    if "balance" in row:
        wallet = _must_float("balance")
    elif "walletBalance" in row:
        wallet = _must_float("walletBalance")

    cross_wallet = None
    if "crossWalletBalance" in row:
        cross_wallet = _must_float("crossWalletBalance")

    if wallet is not None:
        eq = wallet + cross_unpnl
        if eq > 0:
            return float(eq)

    if cross_wallet is not None:
        eq = cross_wallet + cross_unpnl
        if eq > 0:
            return float(eq)

    if available > 0:
        return float(available)

    raise RiskCycleContractError("equity_current_usdt invalid: 0.0")


def _get_available_usdt_strict() -> float:
    value = get_available_usdt()
    return _require_float(value, "available_usdt", min_value=0.0)


def _build_account_state_or_skip(
    ctx: RiskCycleContext,
    *,
    symbol: str,
    equity_current_usdt: float,
) -> Optional[AccountState]:
    closed_trades = list(ctx.closed_trades_getter())
    try:
        account_state = ctx.account_builder.build(
            symbol=symbol,
            current_equity_usdt=float(equity_current_usdt),
            closed_trades=closed_trades,
            persisted_equity_peak_usdt=ctx.state.persisted_equity_peak_usdt,
        )
    except AccountStateNotReadyError as e:
        msg = f"[SKIP][ACCOUNT_NOT_READY] {e}"
        log(msg)
        _maybe_send_risk_block_tg(ctx, "ACCOUNT_NOT_READY", msg)
        return None
    return account_state


def _resolve_entry_price_hint_for_signal_or_raise(
    market_data: Dict[str, Any],
    cand_meta: Any,
) -> tuple[float, str]:
    market_data = _require_dict(market_data, "market_data")
    if cand_meta is not None:
        cand_meta = _require_dict(cand_meta, "candidate.meta")

    market_features = market_data.get("market_features")
    if market_features is not None:
        market_features = _require_dict(market_features, "market_data.market_features")

    search_spaces: list[tuple[str, Any]] = [
        ("market_data", market_data),
        ("market_data.market_features", market_features),
        ("cand.meta", cand_meta),
    ]

    aliases = (
        "entry_price_hint",
        "entry_price",
        "price",
        "mark_price",
        "last_price",
        "close",
    )

    for source_name, source_mapping in search_spaces:
        if source_mapping is None:
            continue
        for alias in aliases:
            if alias not in source_mapping:
                continue
            raw = source_mapping[alias]
            if raw is None:
                continue
            price = _require_float(raw, f"{source_name}.{alias}", min_value=0.0)
            if price <= 0.0:
                raise RiskCycleContractError(f"{source_name}.{alias} must be > 0 (STRICT)")
            return float(price), f"{source_name}.{alias}"

    raise RiskCycleContractError(
        f"entry price contract missing for risk_cycle (required aliases={aliases}) (STRICT)"
    )


def _purge_signal_execution_price_aliases_inplace(meta: Dict[str, Any]) -> None:
    meta = _require_dict(meta, "signal.meta")
    for alias in (
        "entry_price_hint",
        "entry_price",
        "price",
        "mark_price",
        "last_price",
        "close",
    ):
        if alias in meta:
            del meta[alias]


def _maybe_send_risk_block_tg(
    ctx: RiskCycleContext,
    key: str,
    msg: str,
) -> None:
    now = time.time()
    if key == ctx.state.last_risk_block_key and (now - ctx.state.last_risk_block_tg_ts) < ctx.config.risk_block_tg_cooldown_sec:
        log(f"[SKIP_TG_SUPPRESS][RISK] {msg}")
        return
    ctx.state.last_risk_block_key = str(key)
    ctx.state.last_risk_block_tg_ts = now
    _safe_send_tg(msg)


__all__ = [
    "RiskCycleError",
    "RiskCycleContractError",
    "RiskCycleSkip",
    "RiskCycleConfig",
    "RiskCycleState",
    "RiskCyclePacket",
    "RiskCycleContext",
    "PENDING_RISK_PACKET_KEY",
    "build_risk_cycle_context_or_raise",
    "build_risk_cycle_fn",
    "run_risk_cycle_or_raise",
    "consume_pending_risk_packet_or_none",
    "peek_pending_risk_packet_or_none",
]