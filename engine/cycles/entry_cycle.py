# engine/cycles/entry_cycle.py
"""
============================================================
FILE: engine/cycles/entry_cycle.py
ROLE:
- trading engine entry cycle
- idle 상태에서 진입 후보 생성 전용 계층

CORE RESPONSIBILITIES:
- entry cadence / cooldown 제어
- data health(OK/WARNING/FAIL) 상태 소비
- authoritative 5m entry gate 사전 차단
- entry market_data build 수행
- entry candidate 생성 및 STRICT 검증
- 다음 계층(risk_cycle)으로 전달할 pending entry packet 생성
- entry skip / warning / contract violation 을 명시적으로 처리

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- entry cycle 은 market data build / candidate 생성만 담당한다
- risk sizing / execution submit 은 이 파일에서 수행하지 않는다
- authoritative 5m entry gate 는 market_data build 성공 후에만 claim 한다
- NO_SIGNAL / explicit SKIP 은 명시적 skip 으로 처리한다
- hidden default / silent continue / 예외 삼키기 금지
============================================================
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

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
from engine.engine_loop import EngineLoopRuntime


PENDING_ENTRY_PACKET_KEY: str = "pending_entry_packet"


class EntryCycleError(RuntimeError):
    """Base error for entry cycle."""


class EntryCycleContractError(EntryCycleError):
    """Raised when entry cycle contract is violated."""


@dataclass(frozen=True)
class EntryCycleConfig:
    engine_entry_ob_min_interval_sec: float
    engine_entry_force_interval_sec: float
    engine_loop_tick_sec: float
    entry_block_tg_cooldown_sec: int
    entry_cooldown_sec: float

    def validate_or_raise(self) -> None:
        _require_float(self.engine_entry_ob_min_interval_sec, "config.engine_entry_ob_min_interval_sec", min_value=0.001)
        _require_float(self.engine_entry_force_interval_sec, "config.engine_entry_force_interval_sec", min_value=0.001)
        _require_float(self.engine_loop_tick_sec, "config.engine_loop_tick_sec", min_value=0.001)
        _require_int(self.entry_block_tg_cooldown_sec, "config.entry_block_tg_cooldown_sec", min_value=1)
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
        _require_optional_positive_int(self.last_entry_cycle_orderbook_ts, "state.last_entry_cycle_orderbook_ts")
        _require_float(self.last_entry_gpt_call_ts, "state.last_entry_gpt_call_ts", min_value=0.0)
        _require_optional_positive_int(self.last_entry_eval_signal_ts_ms, "state.last_entry_eval_signal_ts_ms")
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

        required_setting_names = (
            "entry_cooldown_sec",
            "engine_loop_tick_sec",
            "symbol",
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

    if _is_entry_cooldown_active(now_f, ctx):
        return

    should_run_entry_cycle, latest_1m_ts, latest_orderbook_ts = _should_run_entry_cycle(
        ctx.symbol,
        now_f,
        ctx.state.last_entry_cycle_wall_ts,
        ctx.state.last_entry_cycle_1m_ts,
        ctx.state.last_entry_cycle_orderbook_ts,
        ctx.config,
    )
    if not should_run_entry_cycle:
        return

    health_level, health_fail_reason, health_warning_reason, health_snapshot = _get_data_health_state_or_raise()
    if health_level == "FAIL":
        reason_text = _format_data_health_snapshot_reason(health_snapshot, health_fail_reason)
        msg = f"[SKIP][DATA_HEALTH_FAIL] {reason_text}"
        log(msg)
        _maybe_send_entry_block_tg(ctx, "DATA_HEALTH_FAIL", msg)
        return

    if health_level == "WARNING":
        warning_text = _format_data_health_warning_reason(health_snapshot, health_warning_reason)
        log(f"[WARN][DATA_HEALTH_WARNING] {warning_text}")

    authoritative_5m_gate_ts = _get_latest_ws_5m_signal_gate_ts_or_raise(ctx.symbol)
    _bootstrap_entry_gate_state_if_needed(ctx)

    if _is_current_entry_signal_gate_already_claimed_or_raise(ctx, authoritative_5m_gate_ts):
        ctx.state.last_entry_cycle_wall_ts = now_f
        ctx.state.last_entry_cycle_1m_ts = latest_1m_ts
        ctx.state.last_entry_cycle_orderbook_ts = latest_orderbook_ts
        return

    market_data = _build_entry_market_data_stage_or_raise(
        ctx.settings,
        _get_last_close_ts_or_raise(ctx),
        notify_entry_block_fn=lambda key, msg, cooldown_sec: _maybe_send_entry_block_tg(
            ctx, key, msg, cooldown_sec
        ),
        log_fn=log,
        build_fn=ctx.build_entry_market_data_fn,
    )

    ctx.state.last_entry_cycle_wall_ts = now_f
    ctx.state.last_entry_cycle_1m_ts = latest_1m_ts
    ctx.state.last_entry_cycle_orderbook_ts = latest_orderbook_ts

    if market_data is not None:
        signal_ts_ms = _require_int_ms(market_data.get("signal_ts_ms"), "market_data.signal_ts_ms")
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
        runtime.safe_stop_requested = True
        msg = f"[SAFE_STOP][DATA_INTEGRITY] {e}"
        log(msg)
        _safe_send_tg(msg)
        raise EntryCycleError(msg) from e

    candidate = _decide_entry_candidate_stage_or_raise(
        market_data,
        ctx.settings,
        decide_fn=ctx.decide_entry_candidate_fn,
    )

    action = _require_nonempty_str(getattr(candidate, "action", None), "candidate.action").upper()
    if action != "ENTER":
        reason = _require_nonempty_str(getattr(candidate, "reason", None), "candidate.reason")
        msg = f"[SKIP][CANDIDATE] {reason}"
        log(msg)
        _maybe_send_entry_block_tg(ctx, "CANDIDATE_SKIP", msg)
        return

    ctx.state.last_entry_gpt_call_ts = now_f

    packet = EntryCyclePacket(
        symbol=_normalize_symbol_strict(market_data.get("symbol"), name="market_data.symbol"),
        authoritative_signal_ts_ms=int(authoritative_5m_gate_ts),
        market_data=market_data,
        candidate=candidate,
        created_at_ts=now_f,
    )
    runtime.extra[PENDING_ENTRY_PACKET_KEY] = packet


def consume_pending_entry_packet_or_none(runtime: EngineLoopRuntime) -> Optional[EntryCyclePacket]:
    runtime.validate_or_raise()
    raw = runtime.extra.pop(PENDING_ENTRY_PACKET_KEY, None)
    if raw is None:
        return None
    if not isinstance(raw, EntryCyclePacket):
        raise EntryCycleContractError(
            f"runtime.extra[{PENDING_ENTRY_PACKET_KEY!r}] must be EntryCyclePacket (STRICT)"
        )
    return raw


def peek_pending_entry_packet_or_none(runtime: EngineLoopRuntime) -> Optional[EntryCyclePacket]:
    runtime.validate_or_raise()
    raw = runtime.extra.get(PENDING_ENTRY_PACKET_KEY)
    if raw is None:
        return None
    if not isinstance(raw, EntryCyclePacket):
        raise EntryCycleContractError(
            f"runtime.extra[{PENDING_ENTRY_PACKET_KEY!r}] must be EntryCyclePacket (STRICT)"
        )
    return raw


def _clear_stale_pending_packet_if_present(runtime: EngineLoopRuntime) -> None:
    raw = runtime.extra.get(PENDING_ENTRY_PACKET_KEY)
    if raw is None:
        return
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


def _require_float(v: Any, name: str, *, min_value: Optional[float] = None) -> float:
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
    s = str(v or "").strip()
    if not s:
        raise EntryCycleContractError(f"{name} is empty (STRICT)")
    return s


def _require_optional_positive_int(v: Any, name: str) -> Optional[int]:
    if v is None:
        return None
    return _require_int(v, name, min_value=1)


def _normalize_symbol_strict(symbol: Any, *, name: str = "symbol") -> str:
    s = str(symbol or "").replace("-", "").replace("/", "").upper().strip()
    if not s:
        raise EntryCycleContractError(f"{name} is empty (STRICT)")
    return s


def _join_reason_list_strict(v: Any, name: str) -> str:
    if not isinstance(v, list):
        raise EntryCycleContractError(f"{name} must be list (STRICT)")
    cleaned = [str(x).strip() for x in v if str(x).strip()]
    return " | ".join(cleaned)


def _normalize_health_level_strict(v: Any, name: str) -> str:
    s = str(v or "").strip().upper()
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
        fb = str(fallback_reason or "").strip()
        if fb:
            return fb
        raise EntryCycleContractError("data health snapshot must be dict (STRICT)")

    reasons: List[str] = []

    fail_reason = str(snapshot.get("fail_reason") or "").strip()
    if fail_reason:
        reasons.append(fail_reason)

    ws = snapshot.get("ws")
    if isinstance(ws, dict):
        ws_reasons = ws.get("overall_reasons")
        if isinstance(ws_reasons, list):
            joined = _join_reason_list_strict(ws_reasons, "health_snapshot.ws.overall_reasons")
            if joined:
                reasons.append(f"ws={joined}")

    feature = snapshot.get("feature")
    if isinstance(feature, dict):
        if not bool(feature.get("ok", False)):
            missing_tfs = feature.get("missing_tfs")
            if isinstance(missing_tfs, list) and missing_tfs:
                reasons.append(f"feature_missing_tfs={missing_tfs}")
            field = feature.get("field")
            if field is not None and str(field).strip():
                reasons.append(f"feature_field={field}")
            err = feature.get("error_type")
            if err is not None and str(err).strip():
                reasons.append(f"feature_error_type={err}")

    if not reasons:
        fb = str(fallback_reason or "").strip()
        if fb:
            return fb
        raise EntryCycleContractError("health fail reason missing (STRICT)")

    unique: List[str] = []
    seen: set[str] = set()
    for r in reasons:
        if r not in seen:
            seen.add(r)
            unique.append(r)
    return " | ".join(unique)


def _format_data_health_warning_reason(snapshot: Dict[str, Any], fallback_warning: str) -> str:
    if not isinstance(snapshot, dict):
        fb = str(fallback_warning or "").strip()
        if fb:
            return fb
        raise EntryCycleContractError("data health snapshot must be dict (STRICT)")

    reasons: List[str] = []

    warning_reason = str(snapshot.get("warning_reason") or "").strip()
    if warning_reason:
        reasons.append(warning_reason)

    ws = snapshot.get("ws")
    if isinstance(ws, dict):
        ws_warnings = ws.get("overall_warnings")
        if isinstance(ws_warnings, list):
            joined = _join_reason_list_strict(ws_warnings, "health_snapshot.ws.overall_warnings")
            if joined:
                reasons.append(f"ws={joined}")

    if not reasons:
        fb = str(fallback_warning or "").strip()
        if fb:
            return fb
        raise EntryCycleContractError("health warning reason missing (STRICT)")

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

    level = _normalize_health_level_strict(level_state.get("level"), "health_level_state.level")
    ok = level_state.get("ok")
    if not isinstance(ok, bool):
        raise EntryCycleContractError("health_level_state.ok must be bool (STRICT)")
    has_warning = level_state.get("has_warning")
    if not isinstance(has_warning, bool):
        raise EntryCycleContractError("health_level_state.has_warning must be bool (STRICT)")

    fail_reason = level_state.get("fail_reason")
    if not isinstance(fail_reason, str):
        raise EntryCycleContractError("health_level_state.fail_reason must be str (STRICT)")
    warning_reason = level_state.get("warning_reason")
    if not isinstance(warning_reason, str):
        raise EntryCycleContractError("health_level_state.warning_reason must be str (STRICT)")

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

    ts_val = ob.get("ts")
    if ts_val is None:
        return None
    return _require_int_ms(ts_val, "orderbook.ts")


def _should_run_entry_cycle(
    symbol: str,
    now_ts: float,
    last_eval_wall_ts: float,
    last_eval_1m_ts: Optional[int],
    last_eval_orderbook_ts: Optional[int],
    config: EntryCycleConfig,
) -> tuple[bool, Optional[int], Optional[int]]:
    latest_1m_ts = _get_latest_ws_kline_ts_optional(symbol, "1m")
    latest_orderbook_ts = _get_orderbook_marker_ts_optional(symbol)

    if latest_1m_ts is not None and last_eval_1m_ts is not None and latest_1m_ts < last_eval_1m_ts:
        raise EntryCycleContractError(
            f"entry loop 1m ts rollback detected (STRICT): prev={last_eval_1m_ts} now={latest_1m_ts}"
        )

    if latest_orderbook_ts is not None and last_eval_orderbook_ts is not None and latest_orderbook_ts < last_eval_orderbook_ts:
        raise EntryCycleContractError(
            f"entry loop orderbook ts rollback detected (STRICT): prev={last_eval_orderbook_ts} now={latest_orderbook_ts}"
        )

    if last_eval_wall_ts <= 0.0:
        return True, latest_1m_ts, latest_orderbook_ts

    if latest_1m_ts is not None and (last_eval_1m_ts is None or latest_1m_ts > last_eval_1m_ts):
        return True, latest_1m_ts, latest_orderbook_ts

    if latest_orderbook_ts is not None and (last_eval_orderbook_ts is None or latest_orderbook_ts > last_eval_orderbook_ts):
        if (now_ts - last_eval_wall_ts) >= config.engine_entry_ob_min_interval_sec:
            return True, latest_1m_ts, latest_orderbook_ts

    if (now_ts - last_eval_wall_ts) >= config.engine_entry_force_interval_sec:
        return True, latest_1m_ts, latest_orderbook_ts

    return False, latest_1m_ts, latest_orderbook_ts


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

    action = _require_nonempty_str(getattr(candidate, "action", None), "candidate.action").upper()
    _ = _require_nonempty_str(getattr(candidate, "reason", None), "candidate.reason")
    if action == "ENTER":
        _ = _require_nonempty_str(getattr(candidate, "direction", None), "candidate.direction")
    return candidate


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