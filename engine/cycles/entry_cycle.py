# engine/cycles/entry_cycle.py
"""
============================================================
FILE: engine/cycles/entry_cycle.py
ROLE:
- trading engine entry cycle
- idle 상태에서 진입 후보 생성 전용 계층
- Feature Layer → Signal Layer 연결을 수행한다

CORE RESPONSIBILITIES:
- entry cadence / cooldown 제어
- data health(OK/WARNING/FAIL) 상태 소비
- authoritative 5m entry gate 사전 차단
- entry market_data build 수행
- upstream engine_scores strict contract 검증
- entry_flow 기반 strategy signal 생성
- gpt_entry_filter 기반 최종 진입 veto 연결
- legacy candidate 와 strategy signal 간 계약 정합성 검증
- 다음 계층(risk_cycle)으로 전달할 pending entry packet 생성
- entry skip / warning / contract violation 을 명시적으로 처리

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- entry cycle 은 market data build / signal build / gpt veto / candidate compatibility 검증만 담당한다
- risk sizing / execution submit 은 이 파일에서 수행하지 않는다
- authoritative 5m entry gate 는 market_data build 성공 후에만 claim 한다
- upstream engine_scores 의 단일 owner 는 unified_features_builder 출력이며 entry cycle 은 재계산하지 않는다
- NO_SIGNAL / explicit SKIP 은 명시적 skip 으로 처리한다
- hidden default / silent continue / 예외 삼키기 금지
- entry_flow / gpt_entry_filter / common.exceptions_strict 를 정식 연결한다
============================================================

CHANGE HISTORY:
- 2026-03-15
  1) FIX(ROOT-CAUSE): data health FAIL / WS recovery 경로를 cadence rollback 검사보다 먼저 실행하도록 run_entry_cycle_or_raise 순서 수정
  2) FIX(STATE): health FAIL 시 entry cycle wall/1m/orderbook marker 를 recovery authoritative baseline 으로 재기준화
  3) FIX(CONTRACT): 정상 RUNNING 상태의 1m/orderbook ts rollback fatal 계약은 유지하고, recovery 경계는 baseline rebase 로 분리
  4) FIX(STRICT): _require_float 에 max_value 계약을 추가해 기존 score 검증 호출과 함수 시그니처를 정합화
  5) FIX(STRICT): 핵심 계약 경로에서 getattr(..., default) / dict.get(..., default) 사용 제거
  6) FIX(ROOT-CAUSE): health_level_state.fail_reason / warning_reason 는 빈 문자열을 허용하고, 실제 reason 조합 단계에서만 non-empty 를 요구하도록 계약 분리
  7) FIX(OPERABILITY): health snapshot fail_reason / warning_reason 키가 존재하지만 빈 문자열인 경우 fatal 처리하지 않고 무시하도록 조정
  8) FIX(ROOT-CAUSE): engine_scores 단일 owner 를 upstream unified_features_builder 로 고정하고 entry cycle 재계산 제거
  9) FIX(ARCH): market_data.market_features.engine_scores 는 구조/범위/entry_score 정합만 검증하고 local score_engine mismatch fatal 제거
  10) ADD(GPT-VETO): strategy.gpt_entry_filter.apply_gpt_entry_filter_strict 정식 연결
  11) ADD(CONTRACT): gpt_entry_* settings 필수 계약 추가
  12) ADD(SAFE_STOP): GPT entry filter 실패 시 ENTRY_GPT_FILTER_ERROR safe stop 요청
  13) FIX(STATE): GPT entry filter 가 실제 호출된 경우 last_entry_gpt_call_ts 즉시 반영
- 2026-03-13
  1) ADD(SIGNAL-LAYER): strategy.entry_flow.build_entry_signal_strict 정식 연결
  2) ADD(CONTRACT): legacy candidate vs strategy signal action/direction strict 정합성 검증 추가
  3) ADD(PACKET): EntryCyclePacket 에 strategy_signal / engine_scores 포함
  4) FIX(STRICT): common.exceptions_strict 기반 entry 예외 계층 연결
  5) FIX(SAFE_STOP): signal/data contract mismatch 시 runtime.request_safe_stop() 사용
============================================================
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from common.exceptions_strict import StrictExternalError, StrictStateError
from infra.telelog import log, send_tg
from infra.async_worker import submit as submit_async
import infra.data_health_monitor as data_health_monitor
from infra.market_data_ws import (
    get_klines_with_volume as ws_get_klines_with_volume,
    get_orderbook as ws_get_orderbook,
)
from infra.data_integrity_guard import (
    DataIntegrityError,
    validate_entry_market_data_bundle_strict,
)
from core.entry_pipeline import _build_entry_market_data, _decide_entry_candidate_strict
from strategy.engine_runtime_state import (
    bootstrap_last_entry_signal_ts_ms,
    claim_entry_signal_ts_or_skip,
)
from strategy.entry_flow import build_entry_signal_strict
from strategy.gpt_entry_filter import (
    GptEntryFilterConfigError,
    GptEntryFilterContractError,
    GptEntryFilterRuntimeError,
    apply_gpt_entry_filter_strict,
)
from strategy.signal import Signal
from engine.engine_loop import ENGINE_STATE_RUNNING, EngineLoopRuntime


PENDING_ENTRY_PACKET_KEY: str = "pending_entry_packet"


class EntryCycleError(StrictExternalError):
    """Base error for entry cycle."""


class EntryCycleContractError(StrictStateError):
    """Raised when entry cycle contract is violated."""


@dataclass(frozen=True)
class EntryCycleConfig:
    engine_entry_ob_min_interval_sec: float
    engine_entry_force_interval_sec: float
    engine_loop_tick_sec: float
    entry_block_tg_cooldown_sec: int
    entry_cooldown_sec: float

    def validate_or_raise(self) -> None:
        _require_float(
            self.engine_entry_ob_min_interval_sec,
            "config.engine_entry_ob_min_interval_sec",
            min_value=0.001,
        )
        _require_float(
            self.engine_entry_force_interval_sec,
            "config.engine_entry_force_interval_sec",
            min_value=0.001,
        )
        _require_float(
            self.engine_loop_tick_sec,
            "config.engine_loop_tick_sec",
            min_value=0.001,
        )
        _require_int(
            self.entry_block_tg_cooldown_sec,
            "config.entry_block_tg_cooldown_sec",
            min_value=1,
        )
        _require_float(self.entry_cooldown_sec, "config.entry_cooldown_sec", min_value=0.0)


@dataclass
class EntryCycleState:
    last_entry_cycle_wall_ts: float = 0.0
    last_entry_cycle_1m_ts: Optional[int] = None
    last_entry_cycle_orderbook_ts: Optional[int] = None
    last_entry_gpt_call_ts: float = 0.0
    last_entry_eval_signal_ts_ms: Optional[int] = None
    entry_gate_bootstrapped: bool = False

    last_entry_block_tg_ts: float = 0.0
    last_entry_block_key: str = ""

    def validate_or_raise(self) -> None:
        _require_float(self.last_entry_cycle_wall_ts, "state.last_entry_cycle_wall_ts", min_value=0.0)
        _require_optional_positive_int(self.last_entry_cycle_1m_ts, "state.last_entry_cycle_1m_ts")
        _require_optional_positive_int(
            self.last_entry_cycle_orderbook_ts,
            "state.last_entry_cycle_orderbook_ts",
        )
        _require_float(self.last_entry_gpt_call_ts, "state.last_entry_gpt_call_ts", min_value=0.0)
        _require_optional_positive_int(
            self.last_entry_eval_signal_ts_ms,
            "state.last_entry_eval_signal_ts_ms",
        )
        _require_bool(self.entry_gate_bootstrapped, "state.entry_gate_bootstrapped")
        _require_float(self.last_entry_block_tg_ts, "state.last_entry_block_tg_ts", min_value=0.0)
        if not isinstance(self.last_entry_block_key, str):
            raise EntryCycleContractError("state.last_entry_block_key must be str (STRICT)")


@dataclass(frozen=True)
class EntryCyclePacket:
    symbol: str
    authoritative_signal_ts_ms: int
    market_data: Dict[str, Any]
    candidate: Any
    strategy_signal: Signal
    engine_scores: Dict[str, Any]
    created_at_ts: float


@dataclass(frozen=True)
class EntryCycleContext:
    settings: Any
    symbol: str
    config: EntryCycleConfig
    state: EntryCycleState
    last_close_ts_getter: Callable[[], float]
    build_entry_market_data_fn: Callable[..., Optional[Dict[str, Any]]] = _build_entry_market_data
    decide_entry_candidate_fn: Callable[[Dict[str, Any], Any], Any] = _decide_entry_candidate_strict
    build_entry_signal_fn: Callable[..., Signal] = build_entry_signal_strict
    apply_gpt_entry_filter_fn: Callable[..., Signal] = apply_gpt_entry_filter_strict

    def validate_or_raise(self) -> None:
        _ = _normalize_symbol_strict(self.symbol, name="context.symbol")
        if self.settings is None:
            raise EntryCycleContractError("context.settings is required (STRICT)")
        self.config.validate_or_raise()
        self.state.validate_or_raise()

        if not callable(self.last_close_ts_getter):
            raise EntryCycleContractError("context.last_close_ts_getter is required (STRICT)")
        if not callable(self.build_entry_market_data_fn):
            raise EntryCycleContractError("context.build_entry_market_data_fn is required (STRICT)")
        if not callable(self.decide_entry_candidate_fn):
            raise EntryCycleContractError("context.decide_entry_candidate_fn is required (STRICT)")
        if not callable(self.build_entry_signal_fn):
            raise EntryCycleContractError("context.build_entry_signal_fn is required (STRICT)")
        if not callable(self.apply_gpt_entry_filter_fn):
            raise EntryCycleContractError("context.apply_gpt_entry_filter_fn is required (STRICT)")

        required_setting_names = (
            "entry_cooldown_sec",
            "engine_loop_tick_sec",
            "symbol",
            "entry_tp_pct",
            "entry_sl_pct",
            "entry_risk_pct",
            "entry_score_threshold",
            "entry_max_spread_pct",
            "entry_min_trend_strength",
            "entry_min_abs_orderbook_imbalance",
            "gpt_entry_enabled",
            "gpt_entry_min_entry_score",
            "gpt_entry_timeout_sec",
            "gpt_entry_max_tokens",
        )
        for name in required_setting_names:
            if not hasattr(self.settings, name):
                raise EntryCycleContractError(f"settings.{name} is required (STRICT)")


def build_entry_cycle_context_or_raise(
    *,
    settings: Any,
    symbol: str,
    last_close_ts_getter: Callable[[], float],
) -> EntryCycleContext:
    ctx = EntryCycleContext(
        settings=settings,
        symbol=_normalize_symbol_strict(symbol),
        config=EntryCycleConfig(
            engine_entry_ob_min_interval_sec=0.35,
            engine_entry_force_interval_sec=1.00,
            engine_loop_tick_sec=float(settings.engine_loop_tick_sec),
            entry_block_tg_cooldown_sec=60,
            entry_cooldown_sec=float(settings.entry_cooldown_sec),
        ),
        state=EntryCycleState(),
        last_close_ts_getter=last_close_ts_getter,
    )
    ctx.validate_or_raise()
    return ctx


def build_entry_cycle_fn(ctx: EntryCycleContext) -> Callable[[float, EngineLoopRuntime], None]:
    ctx.validate_or_raise()

    def _fn(now_ts: float, runtime: EngineLoopRuntime) -> None:
        run_entry_cycle_or_raise(now_ts, runtime, ctx)

    return _fn


def run_entry_cycle_or_raise(
    now_ts: float,
    runtime: EngineLoopRuntime,
    ctx: EntryCycleContext,
) -> None:
    ctx.validate_or_raise()
    runtime.validate_or_raise()
    now_f = _require_float(now_ts, "now_ts", min_value=0.0)

    _clear_stale_pending_packet_if_present(runtime)

    if runtime.safe_stop_requested:
        return

    if runtime.engine_state != ENGINE_STATE_RUNNING:
        raise EntryCycleContractError(
            f"entry cycle requires RUNNING state (STRICT), current={runtime.engine_state}"
        )

    latest_1m_ts, latest_orderbook_ts = _capture_entry_cycle_markers_or_raise(ctx.symbol)

    health_level, health_fail_reason, health_warning_reason, health_snapshot = _get_data_health_state_or_raise()
    if health_level == "FAIL":
        # recoverable 상태는 entry 차단하지 않는다
        if health_snapshot.get("recovery_context"):
            log("[ENTRY][RECOVERY] health FAIL but recovery_context active → skip SAFE_STOP")
        else:
            reason_text = _format_data_health_snapshot_reason(health_snapshot, health_fail_reason)
            _rebase_entry_cycle_markers_on_health_failure(
                ctx,
                now_ts=now_f,
                latest_1m_ts=latest_1m_ts,
                latest_orderbook_ts=latest_orderbook_ts,
                reason_text=reason_text,
            )
            msg = f"[SKIP][DATA_HEALTH_FAIL] {reason_text}"
            log(msg)
            _maybe_send_entry_block_tg(ctx, "DATA_HEALTH_FAIL", msg)
            return

    if health_level == "WARNING":
        warning_text = _format_data_health_warning_reason(health_snapshot, health_warning_reason)
        log(f"[WARN][DATA_HEALTH_WARNING] {warning_text}")

    if _is_entry_cooldown_active(now_f, ctx):
        return

    should_run_entry_cycle = _should_run_entry_cycle(
        now_ts=now_f,
        last_eval_wall_ts=ctx.state.last_entry_cycle_wall_ts,
        last_eval_1m_ts=ctx.state.last_entry_cycle_1m_ts,
        last_eval_orderbook_ts=ctx.state.last_entry_cycle_orderbook_ts,
        latest_1m_ts=latest_1m_ts,
        latest_orderbook_ts=latest_orderbook_ts,
        config=ctx.config,
    )
    if not should_run_entry_cycle:
        return

    authoritative_5m_gate_ts = _get_latest_ws_5m_signal_gate_ts_or_raise(ctx.symbol)
    _bootstrap_entry_gate_state_if_needed(ctx)

    if _is_current_entry_signal_gate_already_claimed_or_raise(ctx, authoritative_5m_gate_ts):
        _update_entry_cycle_markers(
            ctx.state,
            now_ts=now_f,
            latest_1m_ts=latest_1m_ts,
            latest_orderbook_ts=latest_orderbook_ts,
        )
        return

    market_data = _build_entry_market_data_stage_or_raise(
        ctx.settings,
        _get_last_close_ts_or_raise(ctx),
        notify_entry_block_fn=lambda key, msg, cooldown_sec: _maybe_send_entry_block_tg(
            ctx,
            key,
            msg,
            cooldown_sec,
        ),
        log_fn=log,
        build_fn=ctx.build_entry_market_data_fn,
    )

    _update_entry_cycle_markers(
        ctx.state,
        now_ts=now_f,
        latest_1m_ts=latest_1m_ts,
        latest_orderbook_ts=latest_orderbook_ts,
    )

    if market_data is not None:
        signal_ts_ms = _require_int_ms(
            _require_mapping_key(market_data, "signal_ts_ms", "market_data"),
            "market_data.signal_ts_ms",
        )
        if signal_ts_ms != authoritative_5m_gate_ts:
            raise EntryCycleContractError(
                "market_data.signal_ts_ms != authoritative_5m_gate_ts (STRICT): "
                f"signal_ts_ms={signal_ts_ms} gate_ts={authoritative_5m_gate_ts}"
            )

    if not _claim_entry_signal_ts_or_skip_and_update_state(ctx, authoritative_5m_gate_ts):
        return

    if market_data is None:
        return

    try:
        validate_entry_market_data_bundle_strict(market_data)
    except DataIntegrityError as e:
        runtime.request_safe_stop("ENTRY_DATA_INTEGRITY", now_ts=now_f)
        msg = f"[SAFE_STOP][DATA_INTEGRITY] {e}"
        log(msg)
        _safe_send_tg(msg)
        raise EntryCycleError(msg) from e

    engine_scores = _extract_upstream_engine_scores_or_raise(market_data)
    _validate_market_data_entry_score_vs_upstream_engine_scores_or_raise(market_data, engine_scores)

    signal_features = _build_entry_signal_features_from_market_data_or_raise(
        market_data,
        engine_scores=engine_scores,
    )
    strategy_signal = _build_strategy_signal_stage_or_raise(
        signal_features,
        ctx.settings,
        build_fn=ctx.build_entry_signal_fn,
    )
    strategy_signal = _apply_gpt_entry_filter_stage_or_raise(
        signal_features=signal_features,
        strategy_signal=strategy_signal,
        settings=ctx.settings,
        engine_scores=engine_scores,
        apply_fn=ctx.apply_gpt_entry_filter_fn,
        runtime=runtime,
        now_ts=now_f,
    )

    if _did_gpt_entry_filter_invoke_model_or_raise(strategy_signal):
        ctx.state.last_entry_gpt_call_ts = now_f

    signal_action = _require_nonempty_str(strategy_signal.action, "strategy_signal.action").upper()

    if signal_action == "SKIP":
        reason = _require_nonempty_str(strategy_signal.reason, "strategy_signal.reason")
        msg = f"[SKIP][ENTRY_FLOW] {reason}"
        log(msg)
        _maybe_send_entry_block_tg(ctx, "ENTRY_FLOW_SKIP", msg)
        return

    if signal_action != "ENTER":
        runtime.request_safe_stop("ENTRY_SIGNAL_INVALID_ACTION", now_ts=now_f)
        raise EntryCycleContractError(
            f"strategy_signal.action must be ENTER/SKIP (STRICT), got={signal_action!r}"
        )

    candidate = _decide_entry_candidate_stage_or_raise(
        market_data,
        ctx.settings,
        decide_fn=ctx.decide_entry_candidate_fn,
    )

    _validate_candidate_vs_strategy_signal_or_raise(
        candidate=candidate,
        strategy_signal=strategy_signal,
        runtime=runtime,
        now_ts=now_f,
    )

    ctx.state.last_entry_gpt_call_ts = now_f

    packet = EntryCyclePacket(
        symbol=_normalize_symbol_strict(
            _require_mapping_key(market_data, "symbol", "market_data"),
            name="market_data.symbol",
        ),
        authoritative_signal_ts_ms=int(authoritative_5m_gate_ts),
        market_data=market_data,
        candidate=candidate,
        strategy_signal=strategy_signal,
        engine_scores=engine_scores,
        created_at_ts=now_f,
    )
    runtime.extra[PENDING_ENTRY_PACKET_KEY] = packet


def consume_pending_entry_packet_or_none(runtime: EngineLoopRuntime) -> Optional[EntryCyclePacket]:
    runtime.validate_or_raise()
    if PENDING_ENTRY_PACKET_KEY not in runtime.extra:
        return None
    raw = runtime.extra.pop(PENDING_ENTRY_PACKET_KEY)
    if not isinstance(raw, EntryCyclePacket):
        raise EntryCycleContractError(
            f"runtime.extra[{PENDING_ENTRY_PACKET_KEY!r}] must be EntryCyclePacket (STRICT)"
        )
    return raw


def peek_pending_entry_packet_or_none(runtime: EngineLoopRuntime) -> Optional[EntryCyclePacket]:
    runtime.validate_or_raise()
    if PENDING_ENTRY_PACKET_KEY not in runtime.extra:
        return None
    raw = runtime.extra[PENDING_ENTRY_PACKET_KEY]
    if not isinstance(raw, EntryCyclePacket):
        raise EntryCycleContractError(
            f"runtime.extra[{PENDING_ENTRY_PACKET_KEY!r}] must be EntryCyclePacket (STRICT)"
        )
    return raw


def _clear_stale_pending_packet_if_present(runtime: EngineLoopRuntime) -> None:
    if PENDING_ENTRY_PACKET_KEY not in runtime.extra:
        return
    raw = runtime.extra[PENDING_ENTRY_PACKET_KEY]
    if not isinstance(raw, EntryCyclePacket):
        raise EntryCycleContractError(
            f"runtime.extra[{PENDING_ENTRY_PACKET_KEY!r}] must be EntryCyclePacket (STRICT)"
        )


def _is_entry_cooldown_active(now_ts: float, ctx: EntryCycleContext) -> bool:
    if now_ts < ctx.state.last_entry_gpt_call_ts:
        raise EntryCycleContractError(
            f"entry cooldown clock rollback detected (STRICT): prev={ctx.state.last_entry_gpt_call_ts} now={now_ts}"
        )
    return (now_ts - ctx.state.last_entry_gpt_call_ts) < ctx.config.entry_cooldown_sec


def _require_bool(v: Any, name: str) -> bool:
    if not isinstance(v, bool):
        raise EntryCycleContractError(f"{name} must be bool (STRICT)")
    return bool(v)


def _require_float(
    v: Any,
    name: str,
    *,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> float:
    if isinstance(v, bool):
        raise EntryCycleContractError(f"{name} must be numeric (bool not allowed)")
    try:
        x = float(v)
    except Exception as e:
        raise EntryCycleContractError(f"{name} must be numeric (STRICT): {e}") from e
    if not math.isfinite(x):
        raise EntryCycleContractError(f"{name} must be finite (STRICT)")
    if min_value is not None and x < min_value:
        raise EntryCycleContractError(f"{name} must be >= {min_value} (STRICT)")
    if max_value is not None and x > max_value:
        raise EntryCycleContractError(f"{name} must be <= {max_value} (STRICT)")
    return float(x)


def _require_int(v: Any, name: str, *, min_value: Optional[int] = None) -> int:
    if isinstance(v, bool):
        raise EntryCycleContractError(f"{name} must be int (bool not allowed)")
    try:
        iv = int(v)
    except Exception as e:
        raise EntryCycleContractError(f"{name} must be int (STRICT): {e}") from e
    if min_value is not None and iv < min_value:
        raise EntryCycleContractError(f"{name} must be >= {min_value} (STRICT)")
    return int(iv)


def _require_int_ms(v: Any, name: str) -> int:
    return _require_int(v, name, min_value=1)


def _require_nonempty_str(v: Any, name: str) -> str:
    if not isinstance(v, str):
        raise EntryCycleContractError(f"{name} must be str (STRICT)")
    s = v.strip()
    if not s:
        raise EntryCycleContractError(f"{name} is empty (STRICT)")
    return s


def _require_optional_positive_int(v: Any, name: str) -> Optional[int]:
    if v is None:
        return None
    return _require_int(v, name, min_value=1)


def _require_dict(v: Any, name: str) -> Dict[str, Any]:
    if not isinstance(v, dict):
        raise EntryCycleContractError(f"{name} must be dict (STRICT)")
    if not v:
        raise EntryCycleContractError(f"{name} must not be empty (STRICT)")
    return v


def _require_list(v: Any, name: str) -> List[Any]:
    if not isinstance(v, list):
        raise EntryCycleContractError(f"{name} must be list (STRICT)")
    if not v:
        raise EntryCycleContractError(f"{name} must not be empty (STRICT)")
    return v


def _require_mapping_key(d: Dict[str, Any], key: str, ctx_name: str) -> Any:
    if key not in d:
        raise EntryCycleContractError(f"{ctx_name}.{key} is required (STRICT)")
    return d[key]


def _optional_mapping_value(d: Dict[str, Any], key: str) -> Any:
    if key in d:
        return d[key]
    return None


def _require_attr_value(obj: Any, attr_name: str, owner_name: str) -> Any:
    if obj is None:
        raise EntryCycleContractError(f"{owner_name} is None (STRICT)")
    if not hasattr(obj, attr_name):
        raise EntryCycleContractError(f"{owner_name}.{attr_name} is required (STRICT)")
    value = getattr(obj, attr_name)
    if value is None:
        raise EntryCycleContractError(f"{owner_name}.{attr_name} is required (STRICT)")
    return value


def _normalize_symbol_strict(symbol: Any, *, name: str = "symbol") -> str:
    if not isinstance(symbol, str):
        raise EntryCycleContractError(f"{name} must be str (STRICT)")
    s = symbol.replace("-", "").replace("/", "").upper().strip()
    if not s:
        raise EntryCycleContractError(f"{name} is empty (STRICT)")
    return s


def _join_reason_list_strict(v: Any, name: str) -> str:
    if not isinstance(v, list):
        raise EntryCycleContractError(f"{name} must be list (STRICT)")
    cleaned = [str(x).strip() for x in v if str(x).strip()]
    return " | ".join(cleaned)


def _normalize_health_level_strict(v: Any, name: str) -> str:
    if not isinstance(v, str):
        raise EntryCycleContractError(f"{name} must be str (STRICT)")
    s = v.strip().upper()
    if s not in ("OK", "WARNING", "FAIL"):
        raise EntryCycleContractError(f"{name} must be OK/WARNING/FAIL (STRICT), got={v!r}")
    return s


def _safe_send_tg(msg: str) -> None:
    try:
        ok = submit_async(send_tg, msg, critical=False, label="send_tg")
        if not ok:
            log(f"[TG][DROP] async queue full: {msg}")
    except Exception as e:
        log(f"[TG] async submit error: {type(e).__name__}: {e} | msg={msg}")


def _maybe_send_entry_block_tg(
    ctx: EntryCycleContext,
    key: str,
    msg: str,
    cooldown_sec: Optional[int] = None,
) -> None:
    cooldown = ctx.config.entry_block_tg_cooldown_sec if cooldown_sec is None else _require_int(
        cooldown_sec,
        "cooldown_sec",
        min_value=1,
    )
    now = time.time()
    if key == ctx.state.last_entry_block_key and (now - ctx.state.last_entry_block_tg_ts) < cooldown:
        log(f"[SKIP_TG_SUPPRESS] {msg}")
        return
    ctx.state.last_entry_block_key = str(key)
    ctx.state.last_entry_block_tg_ts = now
    _safe_send_tg(msg)


def _format_data_health_snapshot_reason(snapshot: Dict[str, Any], fallback_reason: str) -> str:
    if not isinstance(snapshot, dict):
        raise EntryCycleContractError("data health snapshot must be dict (STRICT)")

    reasons: List[str] = []

    fail_reason_raw = _optional_mapping_value(snapshot, "fail_reason")
    if fail_reason_raw is not None:
        if not isinstance(fail_reason_raw, str):
            raise EntryCycleContractError("health_snapshot.fail_reason must be str (STRICT)")
        fail_reason = fail_reason_raw.strip()
        if fail_reason:
            reasons.append(fail_reason)

    ws_raw = _optional_mapping_value(snapshot, "ws")
    if ws_raw is not None:
        ws = _require_dict(ws_raw, "health_snapshot.ws")
        ws_reasons_raw = _optional_mapping_value(ws, "overall_reasons")
        if ws_reasons_raw is not None:
            joined = _join_reason_list_strict(ws_reasons_raw, "health_snapshot.ws.overall_reasons")
            if joined:
                reasons.append(f"ws={joined}")

    feature_raw = _optional_mapping_value(snapshot, "feature")
    if feature_raw is not None:
        feature = _require_dict(feature_raw, "health_snapshot.feature")
        feature_ok = _require_bool(
            _require_mapping_key(feature, "ok", "health_snapshot.feature"),
            "health_snapshot.feature.ok",
        )
        if not feature_ok:
            missing_tfs_raw = _optional_mapping_value(feature, "missing_tfs")
            if missing_tfs_raw is not None:
                missing_tfs = _require_list(missing_tfs_raw, "health_snapshot.feature.missing_tfs")
                reasons.append(f"feature_missing_tfs={missing_tfs}")

            field_raw = _optional_mapping_value(feature, "field")
            if field_raw is not None:
                if not isinstance(field_raw, str):
                    raise EntryCycleContractError("health_snapshot.feature.field must be str (STRICT)")
                field = field_raw.strip()
                if field:
                    reasons.append(f"feature_field={field}")

            err_raw = _optional_mapping_value(feature, "error_type")
            if err_raw is not None:
                if not isinstance(err_raw, str):
                    raise EntryCycleContractError("health_snapshot.feature.error_type must be str (STRICT)")
                err = err_raw.strip()
                if err:
                    reasons.append(f"feature_error_type={err}")

    if not reasons:
        fb = _require_nonempty_str(fallback_reason, "fallback_reason")
        return fb

    unique: List[str] = []
    seen: set[str] = set()
    for r in reasons:
        if r not in seen:
            seen.add(r)
            unique.append(r)
    return " | ".join(unique)


def _format_data_health_warning_reason(snapshot: Dict[str, Any], fallback_warning: str) -> str:
    if not isinstance(snapshot, dict):
        raise EntryCycleContractError("data health snapshot must be dict (STRICT)")

    reasons: List[str] = []

    warning_reason_raw = _optional_mapping_value(snapshot, "warning_reason")
    if warning_reason_raw is not None:
        if not isinstance(warning_reason_raw, str):
            raise EntryCycleContractError("health_snapshot.warning_reason must be str (STRICT)")
        warning_reason = warning_reason_raw.strip()
        if warning_reason:
            reasons.append(warning_reason)

    ws_raw = _optional_mapping_value(snapshot, "ws")
    if ws_raw is not None:
        ws = _require_dict(ws_raw, "health_snapshot.ws")
        ws_warnings_raw = _optional_mapping_value(ws, "overall_warnings")
        if ws_warnings_raw is not None:
            joined = _join_reason_list_strict(ws_warnings_raw, "health_snapshot.ws.overall_warnings")
            if joined:
                reasons.append(f"ws={joined}")

    if not reasons:
        fb = _require_nonempty_str(fallback_warning, "fallback_warning")
        return fb

    unique: List[str] = []
    seen: set[str] = set()
    for r in reasons:
        if r not in seen:
            seen.add(r)
            unique.append(r)
    return " | ".join(unique)


def _get_data_health_state_or_raise() -> tuple[str, str, str, Dict[str, Any]]:
    level_state = data_health_monitor.get_health_level_state()
    if not isinstance(level_state, dict):
        raise EntryCycleContractError("data_health_monitor.get_health_level_state() must return dict (STRICT)")

    level = _normalize_health_level_strict(
        _require_mapping_key(level_state, "level", "health_level_state"),
        "health_level_state.level",
    )
    ok = _require_bool(
        _require_mapping_key(level_state, "ok", "health_level_state"),
        "health_level_state.ok",
    )
    has_warning = _require_bool(
        _require_mapping_key(level_state, "has_warning", "health_level_state"),
        "health_level_state.has_warning",
    )

    fail_reason_raw = _require_mapping_key(level_state, "fail_reason", "health_level_state")
    if not isinstance(fail_reason_raw, str):
        raise EntryCycleContractError("health_level_state.fail_reason must be str (STRICT)")
    fail_reason = fail_reason_raw.strip()

    warning_reason_raw = _require_mapping_key(level_state, "warning_reason", "health_level_state")
    if not isinstance(warning_reason_raw, str):
        raise EntryCycleContractError("health_level_state.warning_reason must be str (STRICT)")
    warning_reason = warning_reason_raw.strip()

    if level == "FAIL" and ok:
        raise EntryCycleContractError("health level FAIL but ok=True (STRICT)")
    if level == "WARNING" and (not ok or not has_warning):
        raise EntryCycleContractError("health level WARNING must satisfy ok=True and has_warning=True (STRICT)")
    if level == "OK" and (not ok or has_warning):
        raise EntryCycleContractError("health level OK must satisfy ok=True and has_warning=False (STRICT)")

    snapshot = data_health_monitor.get_last_health_snapshot()
    if not isinstance(snapshot, dict):
        raise EntryCycleContractError("data_health_monitor.get_last_health_snapshot() must return dict (STRICT)")

    return level, fail_reason, warning_reason, snapshot


def _get_latest_ws_kline_ts_optional(symbol: str, interval: str) -> Optional[int]:
    buf = ws_get_klines_with_volume(symbol, interval, limit=1)
    if not isinstance(buf, list):
        raise EntryCycleContractError(f"ws_get_klines_with_volume returned non-list (STRICT): interval={interval}")
    if not buf:
        return None

    row = buf[-1]
    if not isinstance(row, (list, tuple)) or not row:
        raise EntryCycleContractError(f"ws kline row invalid (STRICT): interval={interval}")
    return _require_int_ms(row[0], f"ws[{interval}].openTime")


def _get_latest_ws_5m_signal_gate_ts_or_raise(symbol: str) -> int:
    latest_5m_ts = _get_latest_ws_kline_ts_optional(symbol, "5m")
    if latest_5m_ts is None:
        raise EntryCycleContractError("authoritative ws 5m latest ts is missing (STRICT)")
    return int(latest_5m_ts)


def _get_orderbook_marker_ts_optional(symbol: str) -> Optional[int]:
    ob = ws_get_orderbook(symbol, limit=1)
    if ob is None:
        return None
    if not isinstance(ob, dict):
        raise EntryCycleContractError("ws_get_orderbook returned non-dict (STRICT)")

    if "ts" not in ob:
        return None
    return _require_int_ms(ob["ts"], "orderbook.ts")


def _capture_entry_cycle_markers_or_raise(symbol: str) -> tuple[Optional[int], Optional[int]]:
    latest_1m_ts = _get_latest_ws_kline_ts_optional(symbol, "1m")
    latest_orderbook_ts = _get_orderbook_marker_ts_optional(symbol)
    return latest_1m_ts, latest_orderbook_ts


def _update_entry_cycle_markers(
    state: EntryCycleState,
    *,
    now_ts: float,
    latest_1m_ts: Optional[int],
    latest_orderbook_ts: Optional[int],
) -> None:
    state.last_entry_cycle_wall_ts = _require_float(now_ts, "entry_cycle_marker.now_ts", min_value=0.0)
    state.last_entry_cycle_1m_ts = _require_optional_positive_int(
        latest_1m_ts,
        "entry_cycle_marker.latest_1m_ts",
    )
    state.last_entry_cycle_orderbook_ts = _require_optional_positive_int(
        latest_orderbook_ts,
        "entry_cycle_marker.latest_orderbook_ts",
    )


def _rebase_entry_cycle_markers_on_health_failure(
    ctx: EntryCycleContext,
    *,
    now_ts: float,
    latest_1m_ts: Optional[int],
    latest_orderbook_ts: Optional[int],
    reason_text: str,
) -> None:
    _update_entry_cycle_markers(
        ctx.state,
        now_ts=now_ts,
        latest_1m_ts=latest_1m_ts,
        latest_orderbook_ts=latest_orderbook_ts,
    )
    log(
        "[ENTRY_CYCLE][REBASE] health failure baseline updated: "
        f"symbol={ctx.symbol} "
        f"last_entry_cycle_1m_ts={ctx.state.last_entry_cycle_1m_ts} "
        f"last_entry_cycle_orderbook_ts={ctx.state.last_entry_cycle_orderbook_ts} "
        f"reason={reason_text}"
    )


def _should_run_entry_cycle(
    *,
    now_ts: float,
    last_eval_wall_ts: float,
    last_eval_1m_ts: Optional[int],
    last_eval_orderbook_ts: Optional[int],
    latest_1m_ts: Optional[int],
    latest_orderbook_ts: Optional[int],
    config: EntryCycleConfig,
) -> bool:
    if latest_1m_ts is not None and last_eval_1m_ts is not None and latest_1m_ts < last_eval_1m_ts:
        raise EntryCycleContractError(
            f"entry loop 1m ts rollback detected (STRICT): prev={last_eval_1m_ts} now={latest_1m_ts}"
        )

    if (
        latest_orderbook_ts is not None
        and last_eval_orderbook_ts is not None
        and latest_orderbook_ts < last_eval_orderbook_ts
    ):
        raise EntryCycleContractError(
            f"entry loop orderbook ts rollback detected (STRICT): prev={last_eval_orderbook_ts} now={latest_orderbook_ts}"
        )

    if last_eval_wall_ts <= 0.0:
        return True

    if latest_1m_ts is not None and (last_eval_1m_ts is None or latest_1m_ts > last_eval_1m_ts):
        return True

    if latest_orderbook_ts is not None and (
        last_eval_orderbook_ts is None or latest_orderbook_ts > last_eval_orderbook_ts
    ):
        if (now_ts - last_eval_wall_ts) >= config.engine_entry_ob_min_interval_sec:
            return True

    if (now_ts - last_eval_wall_ts) >= config.engine_entry_force_interval_sec:
        return True

    return False


def _bootstrap_entry_gate_state_if_needed(ctx: EntryCycleContext) -> None:
    if ctx.state.entry_gate_bootstrapped:
        return

    persisted = bootstrap_last_entry_signal_ts_ms(ctx.symbol)
    if persisted is not None:
        persisted_i = _require_int_ms(persisted, "bootstrap_last_entry_signal_ts_ms")
        ctx.state.last_entry_eval_signal_ts_ms = int(persisted_i)
        log(
            "[ENTRY_GATE][BOOT] persisted signal gate loaded: "
            f"symbol={ctx.symbol} last_entry_eval_signal_ts_ms={persisted_i}"
        )
    else:
        log(f"[ENTRY_GATE][BOOT] no persisted signal gate: symbol={ctx.symbol}")

    ctx.state.entry_gate_bootstrapped = True


def _is_current_entry_signal_gate_already_claimed_or_raise(ctx: EntryCycleContext, signal_ts_ms: Any) -> bool:
    current_ts = _require_int_ms(signal_ts_ms, "entry_gate.current_signal_ts_ms")
    prev_mem_ts = ctx.state.last_entry_eval_signal_ts_ms

    if prev_mem_ts is None:
        return False
    if current_ts < prev_mem_ts:
        raise EntryCycleContractError(
            f"entry signal ts rollback detected vs memory (STRICT): prev={prev_mem_ts} now={current_ts}"
        )
    return bool(current_ts == prev_mem_ts)


def _claim_entry_signal_ts_or_skip_and_update_state(ctx: EntryCycleContext, signal_ts_ms: Any) -> bool:
    current_ts = _require_int_ms(signal_ts_ms, "market_data.signal_ts_ms")
    ok = claim_entry_signal_ts_or_skip(ctx.symbol, current_ts)
    if not isinstance(ok, bool):
        raise EntryCycleContractError("claim_entry_signal_ts_or_skip must return bool (STRICT)")
    if ok:
        ctx.state.last_entry_eval_signal_ts_ms = int(current_ts)
    return bool(ok)


def _get_last_close_ts_or_raise(ctx: EntryCycleContext) -> float:
    value = ctx.last_close_ts_getter()
    return _require_float(value, "last_close_ts", min_value=0.0)


def _build_entry_market_data_stage_or_raise(
    settings: Any,
    last_close_ts: float,
    *,
    notify_entry_block_fn: Callable[[str, str, int], None],
    log_fn: Callable[[str], None],
    build_fn: Callable[..., Optional[Dict[str, Any]]],
) -> Optional[Dict[str, Any]]:
    try:
        return build_fn(
            settings,
            last_close_ts,
            notify_entry_block_fn=notify_entry_block_fn,
            log_fn=log_fn,
        )
    except Exception as e:
        raise EntryCycleError(
            f"entry_pipeline market_data build failed (STRICT): {type(e).__name__}: {e}"
        ) from e


def _decide_entry_candidate_stage_or_raise(
    market_data: Dict[str, Any],
    settings: Any,
    *,
    decide_fn: Callable[[Dict[str, Any], Any], Any],
) -> Any:
    if not isinstance(market_data, dict):
        raise EntryCycleContractError("market_data must be dict (STRICT)")
    try:
        candidate = decide_fn(market_data, settings)
    except Exception as e:
        raise EntryCycleError(
            f"entry candidate build failed (STRICT): {type(e).__name__}: {e}"
        ) from e

    action = _require_nonempty_str(
        _require_attr_value(candidate, "action", "candidate"),
        "candidate.action",
    ).upper()
    _ = _require_nonempty_str(
        _require_attr_value(candidate, "reason", "candidate"),
        "candidate.reason",
    )
    if action == "ENTER":
        _ = _require_nonempty_str(
            _require_attr_value(candidate, "direction", "candidate"),
            "candidate.direction",
        )
    return candidate


def _extract_upstream_engine_scores_or_raise(market_data: Dict[str, Any]) -> Dict[str, Any]:
    market_data_dict = _require_dict(market_data, "market_data")
    market_features = _require_dict(
        _require_mapping_key(market_data_dict, "market_features", "market_data"),
        "market_data.market_features",
    )
    engine_scores = _require_dict(
        _require_mapping_key(market_features, "engine_scores", "market_features"),
        "market_features.engine_scores",
    )

    total_block = _require_dict(
        _require_mapping_key(engine_scores, "total", "market_features.engine_scores"),
        "market_features.engine_scores.total",
    )
    _ = _require_float(
        _require_mapping_key(total_block, "score", "market_features.engine_scores.total"),
        "market_features.engine_scores.total.score",
        min_value=0.0,
        max_value=100.0,
    )

    if "weights" in total_block and total_block["weights"] is not None:
        weights = _require_dict(
            total_block["weights"],
            "market_features.engine_scores.total.weights",
        )
        total_weight = 0.0
        for weight_name, weight_val in weights.items():
            if not isinstance(weight_name, str) or not weight_name.strip():
                raise EntryCycleContractError("engine_scores.total.weights key must be non-empty str (STRICT)")
            fv = _require_float(
                weight_val,
                f"market_features.engine_scores.total.weights[{weight_name}]",
                min_value=0.0,
                max_value=1.0,
            )
            total_weight += fv
        if abs(total_weight - 1.0) > 1e-9:
            raise EntryCycleContractError(
                f"engine_scores.total.weights sum must be 1.0 (STRICT), got={total_weight}"
            )

    for block_name, block_val in engine_scores.items():
        if not isinstance(block_name, str) or not block_name.strip():
            raise EntryCycleContractError("engine_scores block name must be non-empty str (STRICT)")
        block = _require_dict(block_val, f"market_features.engine_scores.{block_name}")
        _ = _require_float(
            _require_mapping_key(block, "score", f"market_features.engine_scores.{block_name}"),
            f"market_features.engine_scores.{block_name}.score",
            min_value=0.0,
            max_value=100.0,
        )

    return engine_scores


def _validate_market_data_entry_score_vs_upstream_engine_scores_or_raise(
    market_data: Dict[str, Any],
    engine_scores: Dict[str, Any],
) -> None:
    market_data_dict = _require_dict(market_data, "market_data")
    market_data_entry_score = _require_float(
        _require_mapping_key(market_data_dict, "entry_score", "market_data"),
        "market_data.entry_score",
        min_value=0.0,
        max_value=1.0,
    )

    total_block = _require_dict(
        _require_mapping_key(engine_scores, "total", "engine_scores"),
        "engine_scores.total",
    )
    upstream_entry_score = _require_float(
        _require_mapping_key(total_block, "score", "engine_scores.total"),
        "engine_scores.total.score",
        min_value=0.0,
        max_value=100.0,
    ) / 100.0

    if abs(market_data_entry_score - upstream_entry_score) > 1e-9:
        raise EntryCycleContractError(
            "market_data.entry_score mismatch detected vs upstream engine_scores (STRICT): "
            f"market_data.entry_score={market_data_entry_score} "
            f"upstream_entry_score={upstream_entry_score}"
        )


def _build_entry_signal_features_from_market_data_or_raise(
    market_data: Dict[str, Any],
    *,
    engine_scores: Dict[str, Any],
) -> Dict[str, Any]:
    market_data_dict = _require_dict(market_data, "market_data")
    market_features = _require_dict(
        _require_mapping_key(market_data_dict, "market_features", "market_data"),
        "market_data.market_features",
    )

    symbol = _normalize_symbol_strict(
        _require_mapping_key(market_data_dict, "symbol", "market_data"),
        name="market_data.symbol",
    )
    regime = _require_nonempty_str(
        _require_mapping_key(market_data_dict, "regime", "market_data"),
        "market_data.regime",
    )
    signal_source = _require_nonempty_str(
        _require_mapping_key(market_data_dict, "signal_source", "market_data"),
        "market_data.signal_source",
    )
    signal_ts_ms = _require_int_ms(
        _require_mapping_key(market_data_dict, "signal_ts_ms", "market_data"),
        "market_data.signal_ts_ms",
    )
    direction = _require_nonempty_str(
        _require_mapping_key(market_data_dict, "direction", "market_data"),
        "market_data.direction",
    ).upper()
    if direction not in ("LONG", "SHORT"):
        raise EntryCycleContractError(f"market_data.direction invalid (STRICT): {direction!r}")

    last_price = _require_float(
        _require_mapping_key(market_data_dict, "last_price", "market_data"),
        "market_data.last_price",
        min_value=0.0,
    )
    candles_5m = _require_list(
        _require_mapping_key(market_data_dict, "candles_5m", "market_data"),
        "market_data.candles_5m",
    )
    candles_5m_raw = _require_list(
        _require_mapping_key(market_data_dict, "candles_5m_raw", "market_data"),
        "market_data.candles_5m_raw",
    )

    equity_current_usdt = _require_float(
        _require_mapping_key(market_features, "equity_current_usdt", "market_features"),
        "market_features.equity_current_usdt",
        min_value=0.0,
    )
    equity_peak_usdt = _require_float(
        _require_mapping_key(market_features, "equity_peak_usdt", "market_features"),
        "market_features.equity_peak_usdt",
        min_value=0.0,
    )
    dd_pct = _require_float(
        _require_mapping_key(market_features, "dd_pct", "market_features"),
        "market_features.dd_pct",
        min_value=0.0,
        max_value=100.0,
    )

    total_block = _require_dict(
        _require_mapping_key(engine_scores, "total", "engine_scores"),
        "engine_scores.total",
    )
    upstream_entry_score = _require_float(
        _require_mapping_key(total_block, "score", "engine_scores.total"),
        "engine_scores.total.score",
        min_value=0.0,
        max_value=100.0,
    ) / 100.0

    trend_strength = _require_float(
        _require_mapping_key(market_data_dict, "trend_strength", "market_data"),
        "market_data.trend_strength",
    )
    spread = _require_float(
        _require_mapping_key(market_data_dict, "spread", "market_data"),
        "market_data.spread",
        min_value=0.0,
    )
    orderbook_imbalance = _require_float(
        _require_mapping_key(market_data_dict, "orderbook_imbalance", "market_data"),
        "market_data.orderbook_imbalance",
    )

    features: Dict[str, Any] = {
        "symbol": symbol,
        "regime": regime,
        "signal_source": signal_source,
        "signal_ts_ms": int(signal_ts_ms),
        "direction": direction,
        "last_price": float(last_price),
        "candles_5m": list(candles_5m),
        "candles_5m_raw": list(candles_5m_raw),
        "equity_current_usdt": float(equity_current_usdt),
        "equity_peak_usdt": float(equity_peak_usdt),
        "dd_pct": float(dd_pct),
        "entry_score": float(upstream_entry_score),
        "trend_strength": float(trend_strength),
        "spread": float(spread),
        "orderbook_imbalance": float(orderbook_imbalance),
    }

    if "guard_adjustments" in market_features and market_features["guard_adjustments"] is not None:
        features["guard_adjustments"] = _require_dict(
            market_features["guard_adjustments"],
            "market_features.guard_adjustments",
        )

    return features


def _build_strategy_signal_stage_or_raise(
    signal_features: Dict[str, Any],
    settings: Any,
    *,
    build_fn: Callable[..., Signal],
) -> Signal:
    features = _require_dict(signal_features, "signal_features")
    if settings is None:
        raise EntryCycleContractError("settings is required for strategy signal (STRICT)")

    try:
        signal = build_fn(features=features, settings=settings)
    except Exception as e:
        raise EntryCycleError(
            f"entry_flow signal build failed (STRICT): {type(e).__name__}: {e}"
        ) from e

    if not isinstance(signal, Signal):
        raise EntryCycleContractError(
            f"build_entry_signal_fn must return strategy.signal.Signal (STRICT), got={type(signal).__name__}"
        )

    action = _require_nonempty_str(signal.action, "strategy_signal.action").upper()
    direction = _require_nonempty_str(signal.direction, "strategy_signal.direction").upper()
    if action not in ("ENTER", "SKIP"):
        raise EntryCycleContractError(f"strategy_signal.action invalid (STRICT): {action!r}")
    if direction not in ("LONG", "SHORT"):
        raise EntryCycleContractError(f"strategy_signal.direction invalid (STRICT): {direction!r}")

    _ = _require_nonempty_str(signal.reason, "strategy_signal.reason")
    _ = _require_float(signal.risk_pct, "strategy_signal.risk_pct", min_value=0.0)
    _ = _require_float(signal.tp_pct, "strategy_signal.tp_pct", min_value=0.0)
    _ = _require_float(signal.sl_pct, "strategy_signal.sl_pct", min_value=0.0)

    return signal


def _apply_gpt_entry_filter_stage_or_raise(
    *,
    signal_features: Dict[str, Any],
    strategy_signal: Signal,
    settings: Any,
    engine_scores: Dict[str, Any],
    apply_fn: Callable[..., Signal],
    runtime: EngineLoopRuntime,
    now_ts: float,
) -> Signal:
    _ = _require_dict(signal_features, "signal_features")
    _ = _require_dict(engine_scores, "engine_scores")
    if settings is None:
        raise EntryCycleContractError("settings is required for gpt entry filter (STRICT)")
    if not isinstance(strategy_signal, Signal):
        raise EntryCycleContractError(
            f"strategy_signal must be Signal before gpt entry filter (STRICT), got={type(strategy_signal).__name__}"
        )
    if not callable(apply_fn):
        raise EntryCycleContractError("apply_gpt_entry_filter_fn must be callable (STRICT)")

    try:
        filtered_signal = apply_fn(
            features=signal_features,
            strategy_signal=strategy_signal,
            settings=settings,
            engine_scores=engine_scores,
        )
    except (
        GptEntryFilterConfigError,
        GptEntryFilterContractError,
        GptEntryFilterRuntimeError,
    ) as e:
        runtime.request_safe_stop("ENTRY_GPT_FILTER_ERROR", now_ts=now_ts)
        raise EntryCycleError(
            f"gpt entry filter failed (STRICT): {type(e).__name__}: {e}"
        ) from e
    except Exception as e:
        runtime.request_safe_stop("ENTRY_GPT_FILTER_ERROR", now_ts=now_ts)
        raise EntryCycleError(
            f"gpt entry filter unexpected failure (STRICT): {type(e).__name__}: {e}"
        ) from e

    if not isinstance(filtered_signal, Signal):
        raise EntryCycleContractError(
            "apply_gpt_entry_filter_fn must return strategy.signal.Signal (STRICT)"
        )

    action = _require_nonempty_str(filtered_signal.action, "gpt_filtered_signal.action").upper()
    direction = _require_nonempty_str(filtered_signal.direction, "gpt_filtered_signal.direction").upper()
    if action not in ("ENTER", "SKIP"):
        raise EntryCycleContractError(f"gpt_filtered_signal.action invalid (STRICT): {action!r}")
    if direction not in ("LONG", "SHORT"):
        raise EntryCycleContractError(f"gpt_filtered_signal.direction invalid (STRICT): {direction!r}")

    _ = _require_nonempty_str(filtered_signal.reason, "gpt_filtered_signal.reason")
    _ = _require_float(filtered_signal.risk_pct, "gpt_filtered_signal.risk_pct", min_value=0.0)
    _ = _require_float(filtered_signal.tp_pct, "gpt_filtered_signal.tp_pct", min_value=0.0)
    _ = _require_float(filtered_signal.sl_pct, "gpt_filtered_signal.sl_pct", min_value=0.0)

    return filtered_signal


def _did_gpt_entry_filter_invoke_model_or_raise(strategy_signal: Signal) -> bool:
    if not isinstance(strategy_signal, Signal):
        raise EntryCycleContractError("strategy_signal must be Signal (STRICT)")

    meta = strategy_signal.meta
    if not isinstance(meta, dict):
        raise EntryCycleContractError("strategy_signal.meta must be dict (STRICT)")

    if "gpt_entry_filter" not in meta:
        return False

    raw = meta["gpt_entry_filter"]
    info = _require_dict(raw, "strategy_signal.meta.gpt_entry_filter")

    enabled_raw = _optional_mapping_value(info, "enabled")
    if enabled_raw is not None:
        enabled = _require_bool(enabled_raw, "strategy_signal.meta.gpt_entry_filter.enabled")
        if not enabled:
            return False

    status_raw = _optional_mapping_value(info, "status")
    if status_raw is None:
        return True
    if not isinstance(status_raw, str):
        raise EntryCycleContractError("strategy_signal.meta.gpt_entry_filter.status must be str (STRICT)")

    status = status_raw.strip().upper()
    if status in ("DISABLED", "BYPASS"):
        return False
    return True


def _validate_candidate_vs_strategy_signal_or_raise(
    *,
    candidate: Any,
    strategy_signal: Signal,
    runtime: EngineLoopRuntime,
    now_ts: float,
) -> None:
    candidate_action = _require_nonempty_str(
        _require_attr_value(candidate, "action", "candidate"),
        "candidate.action",
    ).upper()
    candidate_reason = _require_nonempty_str(
        _require_attr_value(candidate, "reason", "candidate"),
        "candidate.reason",
    )
    signal_action = _require_nonempty_str(strategy_signal.action, "strategy_signal.action").upper()
    signal_reason = _require_nonempty_str(strategy_signal.reason, "strategy_signal.reason")

    if candidate_action != signal_action:
        runtime.request_safe_stop("ENTRY_SIGNAL_ACTION_MISMATCH", now_ts=now_ts)
        raise EntryCycleContractError(
            "legacy candidate action != strategy signal action (STRICT): "
            f"candidate_action={candidate_action} signal_action={signal_action} "
            f"candidate_reason={candidate_reason} signal_reason={signal_reason}"
        )

    if signal_action != "ENTER":
        runtime.request_safe_stop("ENTRY_SIGNAL_NON_ENTER_CONTRACT", now_ts=now_ts)
        raise EntryCycleContractError(
            f"candidate validation requires ENTER action (STRICT), got={signal_action}"
        )

    candidate_direction = _require_nonempty_str(
        _require_attr_value(candidate, "direction", "candidate"),
        "candidate.direction",
    ).upper()
    signal_direction = _require_nonempty_str(
        strategy_signal.direction,
        "strategy_signal.direction",
    ).upper()

    if candidate_direction != signal_direction:
        runtime.request_safe_stop("ENTRY_SIGNAL_DIRECTION_MISMATCH", now_ts=now_ts)
        raise EntryCycleContractError(
            "legacy candidate direction != strategy signal direction (STRICT): "
            f"candidate_direction={candidate_direction} signal_direction={signal_direction}"
        )


__all__ = [
    "EntryCycleError",
    "EntryCycleContractError",
    "EntryCycleConfig",
    "EntryCycleState",
    "EntryCyclePacket",
    "EntryCycleContext",
    "PENDING_ENTRY_PACKET_KEY",
    "build_entry_cycle_context_or_raise",
    "build_entry_cycle_fn",
    "run_entry_cycle_or_raise",
    "consume_pending_entry_packet_or_none",
    "peek_pending_entry_packet_or_none",
]