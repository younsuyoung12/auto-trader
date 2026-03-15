# -*- coding: utf-8 -*-
# engine/cycles/monitoring_cycle.py
"""
============================================================
FILE: engine/cycles/monitoring_cycle.py
ROLE:
- trading engine monitoring cycle
- Monitoring Layer 전용 orchestration 계층
- ws/account/protection/reconciliation/watchdog/drift 감시를 단일 계약으로 수행한다

CORE RESPONSIBILITIES:
- websocket transport liveness 검증
- account websocket 연결 상태 검증
- engine watchdog snapshot 검증
- runtime risk drift snapshot 검증
- protection order 존재 및 정합성 검증
- reconcile engine 주기 실행
- SIGTERM grace deadline 도달 시 강제 정리
- monitoring failure 발생 시 SAFE_STOP 승격

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- monitoring cycle 은 Signal / Risk / Execution 세부 판단을 가지지 않는다
- watchdog start 책임은 BOOT/상위 orchestration 에 있으며 monitoring cycle 은 감시만 수행한다
- drift detector 입력값은 runtime risk snapshot 계약만 사용한다
- contract violation / connectivity failure / protection missing / watchdog failure / drift detected 는 즉시 예외 처리한다
- force close 는 설정에 명시된 경우에만 실행한다
- hidden default / silent continue / 예외 삼키기 금지
- settings 는 SSOT 만 사용한다
- orderbook reconnect / bootstrap / resync 는 recoverable window 로 취급해야 하며
  watchdog FAIL(orderbook_integrity_fail)을 즉시 SAFE_STOP 하지 않는다
- watchdog snapshot.detail 이 이미 recovery 메타를 제공하면 그것을 우선 신뢰한다
- monitoring layer 가 watchdog 판단을 다시 뒤집는 별도 stale 정책을 만들지 않는다

CHANGE HISTORY:
- 2026-03-15:
  1) FEAT(RUNTIME-DRIFT): runtime.extra.last_risk_drift_snapshot 기반 DriftDetector 런타임 연결
  2) ADD(CONTRACT): drift snapshot rollback / stale / schema violation 검증 추가
  3) FIX(RISK): drift detected 시 즉시 SAFE_STOP 승격
  4) FIX(OBSERVABILITY): drift snapshot 미도착/정상화 로그 추가
  5) KEEP(ARCH): monitoring layer 는 risk decision 자체를 만들지 않고 snapshot 감시만 수행
  6) FIX(ROOT-CAUSE): watchdog FAIL(orderbook_integrity_fail) 즉시 SAFE_STOP 제거
  7) FEAT(RECOVERY-GRACE): ws RECONNECTING/OPENING, orderbook bootstrapping/resync 상태에서
     orderbook_integrity_fail 를 grace window 동안 recoverable 로 처리
  8) FIX(OPERABILITY): recovery grace 경과 후에도 orderbook_fail 지속 시에만 SAFE_STOP 승격
  9) FEAT(OBSERVABILITY): recovery grace 시작/유지/해제 로그 추가
  10) FIX(RACE): watchdog snapshot.detail 의 orderbook recovery 메타를 우선 사용하도록 수정
- 2026-03-13:
  1) ADD(MONITORING): infra.engine_watchdog 를 monitoring cycle 에 정식 연결
  2) ADD(CONTRACT): watchdog startup / snapshot freshness / fatal escalation 계약 추가
  3) FIX(STRICT): getattr(..., default) / dict.get(..., default) 기반 은닉 접근 제거
  4) FIX(STATE): watchdog started/fatal/snapshot rollback 상태를 MonitoringCycleState 로 통합
  5) FIX(RISK): watchdog FAIL / stale / internal fatal 발생 시 즉시 SAFE_STOP 승격
  6) FIX(ROOT-CAUSE): monitoring cycle 에서 watchdog 재기동 제거
  7) FIX(ARCH): watchdog 는 BOOT 에서 1회 시작, monitoring cycle 은 snapshot 감시만 수행
============================================================
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Protocol, Tuple

from infra.telelog import log, send_tg
from infra.async_worker import submit as submit_async
from infra.account_ws import (
    get_account_protection_orders_snapshot,
    get_account_ws_status,
)
from infra.engine_watchdog import (
    WatchdogSnapshot,
    get_last_watchdog_snapshot,
    start_watchdog,
)
from infra.market_data_ws import (
    get_orderbook_buffer_status as ws_get_orderbook_buffer_status,
    get_ws_status as ws_get_ws_status,
)
from infra.drift_detector import (
    DriftDetectedError,
    DriftDetector,
    DriftDetectorConfig,
    DriftSnapshot,
)
from execution.exchange_api import (
    fetch_open_orders,
    fetch_open_positions,
)
from engine.engine_loop import EngineLoopRuntime


DEFAULT_WS_LIVENESS_FAIL_HARDSTOP_N: int = 15
DEFAULT_ACCOUNT_WS_FAIL_HARDSTOP_N: int = 3
DEFAULT_PROTECTION_GUARD_INTERVAL_SEC: float = 1.0
DEFAULT_DRIFT_SNAPSHOT_MAX_AGE_SEC: float = 30.0


class MonitoringCycleError(RuntimeError):
    """Base error for monitoring cycle."""


class MonitoringContractError(MonitoringCycleError):
    """Raised when monitoring cycle contract is violated."""


class ForceCloseFn(Protocol):
    def __call__(self, symbol: str) -> int: ...


class StartWatchdogFn(Protocol):
    def __call__(
        self,
        *,
        settings: Any,
        symbol: Optional[str] = None,
        on_fatal: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    ) -> None: ...


class GetWatchdogSnapshotFn(Protocol):
    def __call__(self) -> Optional[WatchdogSnapshot]: ...


def _require_bool(v: Any, name: str) -> bool:
    if not isinstance(v, bool):
        raise MonitoringContractError(f"{name} must be bool (STRICT)")
    return bool(v)


def _require_float(v: Any, name: str, *, min_value: Optional[float] = None) -> float:
    if isinstance(v, bool):
        raise MonitoringContractError(f"{name} must be numeric (bool not allowed) (STRICT)")
    try:
        x = float(v)
    except Exception as e:
        raise MonitoringContractError(f"{name} must be numeric (STRICT): {e}") from e
    if not math.isfinite(x):
        raise MonitoringContractError(f"{name} must be finite (STRICT)")
    if min_value is not None and x < min_value:
        raise MonitoringContractError(f"{name} must be >= {min_value} (STRICT)")
    return float(x)


def _require_int(v: Any, name: str, *, min_value: Optional[int] = None) -> int:
    if isinstance(v, bool):
        raise MonitoringContractError(f"{name} must be int (bool not allowed) (STRICT)")
    try:
        iv = int(v)
    except Exception as e:
        raise MonitoringContractError(f"{name} must be int (STRICT): {e}") from e
    if min_value is not None and iv < min_value:
        raise MonitoringContractError(f"{name} must be >= {min_value} (STRICT)")
    return int(iv)


def _require_nonempty_str(v: Any, name: str) -> str:
    if v is None:
        raise MonitoringContractError(f"{name} is required (STRICT)")
    s = str(v).strip()
    if not s:
        raise MonitoringContractError(f"{name} is empty (STRICT)")
    return s


def _require_dict(v: Any, name: str) -> Dict[str, Any]:
    if not isinstance(v, dict):
        raise MonitoringContractError(f"{name} must be dict (STRICT)")
    return v


def _require_list(v: Any, name: str) -> List[Any]:
    if not isinstance(v, list):
        raise MonitoringContractError(f"{name} must be list (STRICT)")
    return v


def _require_attr(obj: Any, attr_name: str, ctx_name: str) -> Any:
    if obj is None:
        raise MonitoringContractError(f"{ctx_name} is required (STRICT)")
    if not hasattr(obj, attr_name):
        raise MonitoringContractError(f"{ctx_name}.{attr_name} is required (STRICT)")
    return getattr(obj, attr_name)


def _require_setting_attr(settings: Any, attr_name: str) -> Any:
    if settings is None:
        raise MonitoringContractError("settings is required (STRICT)")
    if not hasattr(settings, attr_name):
        raise MonitoringContractError(f"settings.{attr_name} is required (STRICT)")
    return getattr(settings, attr_name)


def _require_dict_key(d: Dict[str, Any], key: str, ctx_name: str) -> Any:
    if key not in d:
        raise MonitoringContractError(f"{ctx_name}.{key} is required (STRICT)")
    return d[key]


def _require_bool_key(d: Dict[str, Any], key: str, ctx_name: str) -> bool:
    return _require_bool(_require_dict_key(d, key, ctx_name), f"{ctx_name}.{key}")


def _require_list_key(d: Dict[str, Any], key: str, ctx_name: str) -> List[Any]:
    return _require_list(_require_dict_key(d, key, ctx_name), f"{ctx_name}.{key}")


def _normalize_symbol_strict(symbol: Any, *, name: str = "symbol") -> str:
    s = _require_nonempty_str(symbol, name).replace("-", "").replace("/", "").upper().strip()
    if not s:
        raise MonitoringContractError(f"{name} normalized empty (STRICT)")
    return s


def _join_reason_list_strict(v: Any, name: str) -> str:
    rows = _require_list(v, name)
    cleaned = [str(x).strip() for x in rows if str(x).strip()]
    return " | ".join(cleaned)


def _reason_list_contains_fragment_strict(v: Any, name: str, fragment: str) -> bool:
    rows = _require_list(v, name)
    frag = _require_nonempty_str(fragment, "fragment")
    for idx, item in enumerate(rows):
        s = _require_nonempty_str(item, f"{name}[{idx}]")
        if frag in s:
            return True
    return False


def _safe_send_tg(msg: str) -> None:
    try:
        ok = submit_async(send_tg, msg, critical=False, label="send_tg")
        if not isinstance(ok, bool):
            raise MonitoringContractError("infra.async_worker.submit must return bool (STRICT)")
        if not ok:
            log(f"[TG][DROP] async queue full: {msg}")
    except Exception as e:
        log(f"[TG] async submit error: {type(e).__name__}: {e} | msg={msg}")


@dataclass(frozen=True)
class MonitoringCycleConfig:
    protection_guard_interval_sec: float
    ws_liveness_fail_hardstop_n: int
    account_ws_fail_hardstop_n: int
    watchdog_startup_grace_sec: float
    watchdog_snapshot_max_age_sec: float
    watchdog_orderbook_recovery_grace_sec: float
    drift_snapshot_max_age_sec: float

    def validate_or_raise(self) -> None:
        _require_float(
            self.protection_guard_interval_sec,
            "config.protection_guard_interval_sec",
            min_value=0.001,
        )
        _require_int(
            self.ws_liveness_fail_hardstop_n,
            "config.ws_liveness_fail_hardstop_n",
            min_value=1,
        )
        _require_int(
            self.account_ws_fail_hardstop_n,
            "config.account_ws_fail_hardstop_n",
            min_value=1,
        )
        _require_float(
            self.watchdog_startup_grace_sec,
            "config.watchdog_startup_grace_sec",
            min_value=0.001,
        )
        _require_float(
            self.watchdog_snapshot_max_age_sec,
            "config.watchdog_snapshot_max_age_sec",
            min_value=0.001,
        )
        _require_float(
            self.watchdog_orderbook_recovery_grace_sec,
            "config.watchdog_orderbook_recovery_grace_sec",
            min_value=0.001,
        )
        _require_float(
            self.drift_snapshot_max_age_sec,
            "config.drift_snapshot_max_age_sec",
            min_value=0.001,
        )


@dataclass
class MonitoringCycleState:
    ws_liveness_consec_fails: int = 0
    account_ws_consec_fails: int = 0
    last_protection_guard_ts: float = 0.0
    sigterm_deadline_handled: bool = False

    watchdog_started: bool = False
    watchdog_start_ts: float = 0.0
    watchdog_last_snapshot_checked_at_ms: int = 0
    watchdog_fatal_reason: Optional[str] = None
    watchdog_fatal_detail: Optional[Dict[str, Any]] = None

    watchdog_orderbook_recovery_grace_started_ts: float = 0.0
    watchdog_orderbook_recovery_last_log_ts: float = 0.0
    watchdog_orderbook_recovery_reason: Optional[str] = None

    drift_last_snapshot_checked_at_ms: int = 0
    drift_snapshot_wait_logged: bool = False

    def validate_or_raise(self) -> None:
        _require_int(self.ws_liveness_consec_fails, "state.ws_liveness_consec_fails", min_value=0)
        _require_int(self.account_ws_consec_fails, "state.account_ws_consec_fails", min_value=0)
        _require_float(self.last_protection_guard_ts, "state.last_protection_guard_ts", min_value=0.0)
        _require_bool(self.sigterm_deadline_handled, "state.sigterm_deadline_handled")

        _require_bool(self.watchdog_started, "state.watchdog_started")
        _require_float(self.watchdog_start_ts, "state.watchdog_start_ts", min_value=0.0)
        _require_int(
            self.watchdog_last_snapshot_checked_at_ms,
            "state.watchdog_last_snapshot_checked_at_ms",
            min_value=0,
        )

        if self.watchdog_fatal_reason is not None:
            _require_nonempty_str(self.watchdog_fatal_reason, "state.watchdog_fatal_reason")
        if self.watchdog_fatal_detail is not None:
            _require_dict(self.watchdog_fatal_detail, "state.watchdog_fatal_detail")

        _require_float(
            self.watchdog_orderbook_recovery_grace_started_ts,
            "state.watchdog_orderbook_recovery_grace_started_ts",
            min_value=0.0,
        )
        _require_float(
            self.watchdog_orderbook_recovery_last_log_ts,
            "state.watchdog_orderbook_recovery_last_log_ts",
            min_value=0.0,
        )
        if self.watchdog_orderbook_recovery_reason is not None:
            _require_nonempty_str(
                self.watchdog_orderbook_recovery_reason,
                "state.watchdog_orderbook_recovery_reason",
            )

        _require_int(
            self.drift_last_snapshot_checked_at_ms,
            "state.drift_last_snapshot_checked_at_ms",
            min_value=0,
        )
        _require_bool(self.drift_snapshot_wait_logged, "state.drift_snapshot_wait_logged")


@dataclass(frozen=True)
class MonitoringCycleContext:
    settings: Any
    symbol: str
    open_trades_ref: List[Any]
    reconcile_engine: Any
    force_close_fn: ForceCloseFn
    watchdog_start_fn: StartWatchdogFn
    watchdog_snapshot_fn: GetWatchdogSnapshotFn
    watchdog_on_fatal_fn: Callable[[str, Dict[str, Any]], None]
    drift_detector: DriftDetector
    config: MonitoringCycleConfig
    state: MonitoringCycleState

    def validate_or_raise(self) -> None:
        _normalize_symbol_strict(self.symbol, name="context.symbol")

        if self.settings is None:
            raise MonitoringContractError("context.settings is required (STRICT)")

        if not isinstance(self.open_trades_ref, list):
            raise MonitoringContractError("context.open_trades_ref must be list (STRICT)")

        if self.reconcile_engine is None:
            raise MonitoringContractError("context.reconcile_engine is required (STRICT)")
        if not hasattr(self.reconcile_engine, "run_if_due"):
            raise MonitoringContractError("context.reconcile_engine.run_if_due is required (STRICT)")
        run_if_due = getattr(self.reconcile_engine, "run_if_due")
        if not callable(run_if_due):
            raise MonitoringContractError("context.reconcile_engine.run_if_due must be callable (STRICT)")

        if not callable(self.force_close_fn):
            raise MonitoringContractError("context.force_close_fn is required (STRICT)")
        if not callable(self.watchdog_start_fn):
            raise MonitoringContractError("context.watchdog_start_fn is required (STRICT)")
        if not callable(self.watchdog_snapshot_fn):
            raise MonitoringContractError("context.watchdog_snapshot_fn is required (STRICT)")
        if not callable(self.watchdog_on_fatal_fn):
            raise MonitoringContractError("context.watchdog_on_fatal_fn is required (STRICT)")

        if not isinstance(self.drift_detector, DriftDetector):
            raise MonitoringContractError("context.drift_detector must be DriftDetector (STRICT)")

        self.config.validate_or_raise()
        self.state.validate_or_raise()

        _require_setting_attr(self.settings, "force_close_on_desync")
        _require_setting_attr(self.settings, "engine_watchdog_interval_sec")
        _require_setting_attr(self.settings, "engine_watchdog_max_db_ping_ms")
        _require_setting_attr(self.settings, "engine_watchdog_emit_min_sec")
        _require_setting_attr(self.settings, "ws_min_kline_buffer")
        _require_setting_attr(self.settings, "ws_required_tfs")
        _require_setting_attr(self.settings, "symbol")


def build_monitoring_cycle_context_or_raise(
    *,
    settings: Any,
    symbol: str,
    open_trades_ref: List[Any],
    reconcile_engine: Any,
    force_close_fn: ForceCloseFn,
) -> MonitoringCycleContext:
    watchdog_interval_sec = _require_float(
        _require_setting_attr(settings, "engine_watchdog_interval_sec"),
        "settings.engine_watchdog_interval_sec",
        min_value=0.001,
    )
    startup_grace_sec = watchdog_interval_sec * 3.0
    snapshot_max_age_sec = watchdog_interval_sec * 3.0
    orderbook_recovery_grace_sec = max(15.0, watchdog_interval_sec * 4.0)
    drift_snapshot_max_age_sec = max(DEFAULT_DRIFT_SNAPSHOT_MAX_AGE_SEC, watchdog_interval_sec * 5.0)

    state = MonitoringCycleState()

    def _on_watchdog_fatal(reason: str, detail: Dict[str, Any]) -> None:
        state.watchdog_fatal_reason = _require_nonempty_str(reason, "watchdog_fatal.reason")
        state.watchdog_fatal_detail = _require_dict(detail, "watchdog_fatal.detail")

    ctx = MonitoringCycleContext(
        settings=settings,
        symbol=_normalize_symbol_strict(symbol),
        open_trades_ref=open_trades_ref,
        reconcile_engine=reconcile_engine,
        force_close_fn=force_close_fn,
        watchdog_start_fn=start_watchdog,
        watchdog_snapshot_fn=get_last_watchdog_snapshot,
        watchdog_on_fatal_fn=_on_watchdog_fatal,
        drift_detector=DriftDetector(DriftDetectorConfig()),
        config=MonitoringCycleConfig(
            protection_guard_interval_sec=DEFAULT_PROTECTION_GUARD_INTERVAL_SEC,
            ws_liveness_fail_hardstop_n=DEFAULT_WS_LIVENESS_FAIL_HARDSTOP_N,
            account_ws_fail_hardstop_n=DEFAULT_ACCOUNT_WS_FAIL_HARDSTOP_N,
            watchdog_startup_grace_sec=startup_grace_sec,
            watchdog_snapshot_max_age_sec=snapshot_max_age_sec,
            watchdog_orderbook_recovery_grace_sec=orderbook_recovery_grace_sec,
            drift_snapshot_max_age_sec=drift_snapshot_max_age_sec,
        ),
        state=state,
    )
    ctx.validate_or_raise()
    return ctx


def _normalize_position_side_guard(v: Any, name: str) -> str:
    s = _require_nonempty_str(v, name).upper().strip()
    if s not in ("BOTH", "LONG", "SHORT"):
        raise MonitoringContractError(f"{name} must be BOTH/LONG/SHORT (STRICT), got={s!r}")
    return s


def _position_side_compatible(expected_position_side: str, order_position_side: str) -> bool:
    exp = _normalize_position_side_guard(expected_position_side, "expected_position_side")
    got = _normalize_position_side_guard(order_position_side, "order_position_side")

    if exp in ("LONG", "SHORT") and got in ("LONG", "SHORT"):
        return got == exp
    if exp == "BOTH":
        return got == "BOTH"
    if exp in ("LONG", "SHORT") and got == "BOTH":
        return True
    return False


def _request_safe_stop_or_raise(runtime: EngineLoopRuntime, reason: str) -> None:
    if runtime is None:
        raise MonitoringContractError("runtime is required (STRICT)")
    _require_nonempty_str(reason, "reason")
    runtime.validate_or_raise()
    runtime.request_safe_stop()


def _reset_watchdog_orderbook_recovery_grace_state(ctx: MonitoringCycleContext) -> None:
    ctx.state.watchdog_orderbook_recovery_grace_started_ts = 0.0
    ctx.state.watchdog_orderbook_recovery_last_log_ts = 0.0
    ctx.state.watchdog_orderbook_recovery_reason = None


def _extract_watchdog_detail_recovery_context_strict(
    snapshot: WatchdogSnapshot,
) -> Tuple[Optional[bool], Optional[str]]:
    detail = snapshot.detail
    if not isinstance(detail, dict):
        raise MonitoringContractError("watchdog_snapshot.detail must be dict (STRICT)")

    if "orderbook_recovery_context" not in detail:
        return None, None

    recovery_context = _require_bool(
        detail["orderbook_recovery_context"],
        "watchdog_snapshot.detail.orderbook_recovery_context",
    )

    recovery_reason_raw = detail.get("orderbook_recovery_context_reason")
    recovery_reason: Optional[str] = None
    if recovery_reason_raw is not None:
        recovery_reason = _require_nonempty_str(
            recovery_reason_raw,
            "watchdog_snapshot.detail.orderbook_recovery_context_reason",
        )
    return recovery_context, recovery_reason


def _is_recoverable_orderbook_fail_context(
    now_ts: float,
    ctx: MonitoringCycleContext,
    snapshot: WatchdogSnapshot,
) -> Tuple[bool, Optional[str]]:
    _require_float(now_ts, "now_ts", min_value=0.0)

    detail_recovery_context, detail_recovery_reason = _extract_watchdog_detail_recovery_context_strict(snapshot)
    if detail_recovery_context is True:
        return True, detail_recovery_reason or "watchdog_detail_recovery_context"
    if detail_recovery_context is False:
        return False, None

    ws_status_any = ws_get_ws_status(ctx.symbol)
    ws_status = _require_dict(ws_status_any, "ws_status")

    orderbook_status_any = ws_get_orderbook_buffer_status(ctx.symbol)
    orderbook_status = _require_dict(orderbook_status_any, "orderbook_buffer_status")

    ws_state = _require_nonempty_str(
        _require_dict_key(ws_status, "state", "ws_status"),
        "ws_status.state",
    ).upper().strip()

    ws_warnings = _require_list_key(ws_status, "warnings", "ws_status")
    has_orderbook = _require_bool_key(orderbook_status, "has_orderbook", "orderbook_buffer_status")

    bootstrapping = False
    if "bootstrapping" in orderbook_status and orderbook_status["bootstrapping"] is not None:
        bootstrapping = _require_bool(
            orderbook_status["bootstrapping"],
            "orderbook_buffer_status.bootstrapping",
        )

    resync_reason: Optional[str] = None
    if "resync_reason" in orderbook_status and orderbook_status["resync_reason"] is not None:
        resync_reason = _require_nonempty_str(
            orderbook_status["resync_reason"],
            "orderbook_buffer_status.resync_reason",
        )

    last_resync_ts_ms: Optional[int] = None
    if "last_resync_ts" in orderbook_status and orderbook_status["last_resync_ts"] is not None:
        last_resync_ts_ms = _require_int(
            orderbook_status["last_resync_ts"],
            "orderbook_buffer_status.last_resync_ts",
            min_value=1,
        )

    startup_warning = _reason_list_contains_fragment_strict(
        ws_warnings,
        "ws_status.warnings",
        "startup_no_orderbook_yet",
    )

    resync_recent = False
    if last_resync_ts_ms is not None:
        last_resync_ts = float(last_resync_ts_ms) / 1000.0
        if (now_ts - last_resync_ts) <= ctx.config.watchdog_orderbook_recovery_grace_sec:
            resync_recent = True

    if ws_state in ("OPENING", "RECONNECTING"):
        return True, f"ws_state={ws_state}"
    if bootstrapping:
        return True, "orderbook_bootstrapping"
    if resync_reason is not None:
        return True, f"orderbook_resync:{resync_reason}"
    if startup_warning:
        return True, "warning_startup_no_orderbook_yet"
    if (not has_orderbook) and resync_recent:
        return True, "orderbook_recent_resync_without_snapshot"

    return False, None


def _maybe_tolerate_watchdog_orderbook_fail_or_raise(
    now_ts: float,
    ctx: MonitoringCycleContext,
    snapshot: WatchdogSnapshot,
    fail_reason: str,
) -> bool:
    _require_float(now_ts, "now_ts", min_value=0.0)
    reason = _require_nonempty_str(fail_reason, "fail_reason")

    if reason != "orderbook_integrity_fail":
        _reset_watchdog_orderbook_recovery_grace_state(ctx)
        return False

    recoverable, recoverable_reason = _is_recoverable_orderbook_fail_context(now_ts, ctx, snapshot)
    if not recoverable:
        _reset_watchdog_orderbook_recovery_grace_state(ctx)
        return False

    if ctx.state.watchdog_orderbook_recovery_grace_started_ts == 0.0:
        ctx.state.watchdog_orderbook_recovery_grace_started_ts = now_ts
        ctx.state.watchdog_orderbook_recovery_last_log_ts = now_ts
        ctx.state.watchdog_orderbook_recovery_reason = reason
        log(
            "[WATCHDOG][RECOVERY_GRACE][START] "
            f"symbol={ctx.symbol} reason={reason} "
            f"context={recoverable_reason} "
            f"grace_sec={ctx.config.watchdog_orderbook_recovery_grace_sec:.3f}"
        )
        return True

    elapsed = now_ts - ctx.state.watchdog_orderbook_recovery_grace_started_ts
    if elapsed <= ctx.config.watchdog_orderbook_recovery_grace_sec:
        last_log_elapsed = now_ts - ctx.state.watchdog_orderbook_recovery_last_log_ts
        if last_log_elapsed >= 5.0:
            ctx.state.watchdog_orderbook_recovery_last_log_ts = now_ts
            log(
                "[WATCHDOG][RECOVERY_GRACE][KEEP] "
                f"symbol={ctx.symbol} reason={reason} context={recoverable_reason} "
                f"elapsed_sec={elapsed:.3f}/{ctx.config.watchdog_orderbook_recovery_grace_sec:.3f}"
            )
        return True

    log(
        "[WATCHDOG][RECOVERY_GRACE][EXPIRED] "
        f"symbol={ctx.symbol} reason={reason} context={recoverable_reason} "
        f"elapsed_sec={elapsed:.3f} grace_sec={ctx.config.watchdog_orderbook_recovery_grace_sec:.3f}"
    )
    _reset_watchdog_orderbook_recovery_grace_state(ctx)
    return False


def _extract_runtime_drift_payload_or_none(runtime: EngineLoopRuntime) -> Optional[Dict[str, Any]]:
    extra_any = _require_attr(runtime, "extra", "runtime")
    extra = _require_dict(extra_any, "runtime.extra")

    if "last_risk_drift_snapshot" not in extra:
        return None

    payload_any = extra["last_risk_drift_snapshot"]
    payload = _require_dict(payload_any, "runtime.extra.last_risk_drift_snapshot")
    return payload


def _build_drift_snapshot_from_payload_or_raise(
    ctx: MonitoringCycleContext,
    payload: Dict[str, Any],
) -> Tuple[DriftSnapshot, int]:
    symbol = _normalize_symbol_strict(
        _require_dict_key(payload, "symbol", "runtime.extra.last_risk_drift_snapshot"),
        name="runtime.extra.last_risk_drift_snapshot.symbol",
    )
    if symbol != ctx.symbol:
        raise MonitoringContractError(
            "runtime drift snapshot symbol mismatch (STRICT): "
            f"snapshot={symbol} context={ctx.symbol}"
        )

    allocation_ratio = _require_float(
        _require_dict_key(payload, "allocation_ratio", "runtime.extra.last_risk_drift_snapshot"),
        "runtime.extra.last_risk_drift_snapshot.allocation_ratio",
        min_value=0.0,
    )
    risk_multiplier = _require_float(
        _require_dict_key(payload, "risk_multiplier", "runtime.extra.last_risk_drift_snapshot"),
        "runtime.extra.last_risk_drift_snapshot.risk_multiplier",
        min_value=0.0,
    )
    regime_band = _require_nonempty_str(
        _require_dict_key(payload, "regime_band", "runtime.extra.last_risk_drift_snapshot"),
        "runtime.extra.last_risk_drift_snapshot.regime_band",
    )
    micro_score_risk = _require_float(
        _require_dict_key(payload, "micro_score_risk", "runtime.extra.last_risk_drift_snapshot"),
        "runtime.extra.last_risk_drift_snapshot.micro_score_risk",
        min_value=0.0,
    )
    as_of_ts_ms = _require_int(
        _require_dict_key(payload, "as_of_ts_ms", "runtime.extra.last_risk_drift_snapshot"),
        "runtime.extra.last_risk_drift_snapshot.as_of_ts_ms",
        min_value=1,
    )

    if micro_score_risk > 100.0:
        raise MonitoringContractError(
            "runtime drift snapshot micro_score_risk must be <= 100 (STRICT)"
        )

    return (
        DriftSnapshot(
            symbol=symbol,
            allocation_ratio=float(allocation_ratio),
            risk_multiplier=float(risk_multiplier),
            regime_band=str(regime_band),
            micro_score_risk=float(micro_score_risk),
        ),
        int(as_of_ts_ms),
    )


def _drift_guard_or_raise(
    now_ts: float,
    runtime: EngineLoopRuntime,
    ctx: MonitoringCycleContext,
) -> None:
    _require_float(now_ts, "now_ts", min_value=0.0)

    payload = _extract_runtime_drift_payload_or_none(runtime)
    if payload is None:
        if not ctx.state.drift_snapshot_wait_logged:
            ctx.state.drift_snapshot_wait_logged = True
            log(
                "[DRIFT][WAIT] "
                f"symbol={ctx.symbol} runtime.extra.last_risk_drift_snapshot unavailable yet"
            )
        return

    ctx.state.drift_snapshot_wait_logged = False

    snapshot, as_of_ts_ms = _build_drift_snapshot_from_payload_or_raise(ctx, payload)

    prev_as_of_ts_ms = ctx.state.drift_last_snapshot_checked_at_ms
    if prev_as_of_ts_ms > 0 and as_of_ts_ms < prev_as_of_ts_ms:
        raise MonitoringContractError(
            "runtime drift snapshot rollback detected (STRICT): "
            f"prev={prev_as_of_ts_ms} now={as_of_ts_ms}"
        )

    now_ms = int(now_ts * 1000.0)
    age_ms = now_ms - as_of_ts_ms
    if age_ms < 0:
        raise MonitoringContractError(
            "runtime drift snapshot future timestamp detected (STRICT): "
            f"now_ms={now_ms} as_of_ts_ms={as_of_ts_ms}"
        )

    max_age_ms = int(ctx.config.drift_snapshot_max_age_sec * 1000.0)
    if age_ms > max_age_ms:
        _request_safe_stop_or_raise(runtime, "DRIFT_SNAPSHOT_STALE")
        msg = (
            "[SAFE_STOP][DRIFT_SNAPSHOT_STALE] "
            f"symbol={ctx.symbol} age_ms={age_ms} max_age_ms={max_age_ms}"
        )
        log(msg)
        _safe_send_tg(msg)
        raise MonitoringCycleError(msg)

    try:
        ctx.drift_detector.update_and_check(snapshot)
    except DriftDetectedError as e:
        _request_safe_stop_or_raise(runtime, "DRIFT_DETECTED")
        msg = f"[SAFE_STOP][DRIFT_DETECTED] symbol={ctx.symbol} reason={e}"
        log(msg)
        _safe_send_tg(msg)
        raise MonitoringCycleError(msg) from e

    ctx.state.drift_last_snapshot_checked_at_ms = as_of_ts_ms


def _ensure_watchdog_started_or_raise(
    now_ts: float,
    ctx: MonitoringCycleContext,
) -> None:
    _require_float(now_ts, "now_ts", min_value=0.0)

    snapshot = ctx.watchdog_snapshot_fn()
    if snapshot is not None:
        if not isinstance(snapshot, WatchdogSnapshot):
            raise MonitoringContractError(
                f"watchdog snapshot must be WatchdogSnapshot (STRICT), got={type(snapshot).__name__}"
            )
        ctx.state.watchdog_started = True
        if ctx.state.watchdog_start_ts == 0.0:
            ctx.state.watchdog_start_ts = now_ts
        return

    if ctx.state.watchdog_start_ts == 0.0:
        ctx.state.watchdog_start_ts = now_ts
        log(
            "[WATCHDOG][WAIT] "
            f"symbol={ctx.symbol} snapshot unavailable yet "
            f"grace_sec={ctx.config.watchdog_startup_grace_sec:.3f}"
        )

    ctx.state.watchdog_started = True


def _watchdog_guard_or_raise(
    now_ts: float,
    runtime: EngineLoopRuntime,
    ctx: MonitoringCycleContext,
) -> None:
    _require_float(now_ts, "now_ts", min_value=0.0)

    _ensure_watchdog_started_or_raise(now_ts, ctx)

    if ctx.state.watchdog_fatal_reason is not None:
        reason = _require_nonempty_str(ctx.state.watchdog_fatal_reason, "state.watchdog_fatal_reason")
        detail = ctx.state.watchdog_fatal_detail
        if detail is None:
            raise MonitoringContractError("state.watchdog_fatal_detail is required when fatal reason exists (STRICT)")
        _request_safe_stop_or_raise(runtime, f"WATCHDOG_FATAL:{reason}")
        msg = f"[SAFE_STOP][WATCHDOG_FATAL] reason={reason} detail={detail}"
        log(msg)
        _safe_send_tg(msg)
        raise MonitoringCycleError(msg)

    snapshot = ctx.watchdog_snapshot_fn()
    if snapshot is None:
        elapsed = now_ts - ctx.state.watchdog_start_ts
        if elapsed <= ctx.config.watchdog_startup_grace_sec:
            return

        _request_safe_stop_or_raise(runtime, "WATCHDOG_SNAPSHOT_MISSING")
        msg = (
            "[SAFE_STOP][WATCHDOG_SNAPSHOT_MISSING] "
            f"symbol={ctx.symbol} elapsed_sec={elapsed:.3f} "
            f"startup_grace_sec={ctx.config.watchdog_startup_grace_sec:.3f}"
        )
        log(msg)
        _safe_send_tg(msg)
        raise MonitoringCycleError(msg)

    if not isinstance(snapshot, WatchdogSnapshot):
        raise MonitoringContractError(
            f"watchdog snapshot must be WatchdogSnapshot (STRICT), got={type(snapshot).__name__}"
        )

    checked_at_ms = _require_int(snapshot.checked_at_ms, "watchdog_snapshot.checked_at_ms", min_value=1)
    prev_checked_at_ms = ctx.state.watchdog_last_snapshot_checked_at_ms
    if prev_checked_at_ms > 0 and checked_at_ms < prev_checked_at_ms:
        raise MonitoringContractError(
            "watchdog snapshot rollback detected (STRICT): "
            f"prev={prev_checked_at_ms} now={checked_at_ms}"
        )

    now_ms = int(now_ts * 1000.0)
    age_ms = now_ms - checked_at_ms
    if age_ms < 0:
        raise MonitoringContractError(
            f"watchdog snapshot future timestamp detected (STRICT): now_ms={now_ms} checked_at_ms={checked_at_ms}"
        )

    ctx.state.watchdog_last_snapshot_checked_at_ms = checked_at_ms

    max_age_ms = int(ctx.config.watchdog_snapshot_max_age_sec * 1000.0)
    if age_ms > max_age_ms:
        _request_safe_stop_or_raise(runtime, "WATCHDOG_SNAPSHOT_STALE")
        msg = (
            "[SAFE_STOP][WATCHDOG_SNAPSHOT_STALE] "
            f"symbol={ctx.symbol} age_ms={age_ms} max_age_ms={max_age_ms}"
        )
        log(msg)
        _safe_send_tg(msg)
        raise MonitoringCycleError(msg)

    level = _require_nonempty_str(snapshot.level, "watchdog_snapshot.level").upper().strip()
    if level not in ("OK", "WARNING", "FAIL"):
        raise MonitoringContractError(f"watchdog_snapshot.level invalid (STRICT): {level}")

    if level == "OK":
        _reset_watchdog_orderbook_recovery_grace_state(ctx)
        return

    if level == "WARNING":
        warning_reason = snapshot.warning_reason
        if warning_reason is not None:
            _require_nonempty_str(warning_reason, "watchdog_snapshot.warning_reason")
            log(
                "[WATCHDOG][WARNING] "
                f"symbol={ctx.symbol} reason={warning_reason} checked_at_ms={checked_at_ms}"
            )
        return

    if level == "FAIL":
        fail_reason = snapshot.fail_reason
        if fail_reason is None:
            raise MonitoringContractError("watchdog_snapshot.fail_reason is required on FAIL (STRICT)")
        reason = _require_nonempty_str(fail_reason, "watchdog_snapshot.fail_reason")

        if _maybe_tolerate_watchdog_orderbook_fail_or_raise(now_ts, ctx, snapshot, reason):
            return

        _request_safe_stop_or_raise(runtime, f"WATCHDOG_FAIL:{reason}")
        msg = (
            "[SAFE_STOP][WATCHDOG_FAIL] "
            f"symbol={ctx.symbol} fail_reason={reason} checked_at_ms={checked_at_ms}"
        )
        log(msg)
        _safe_send_tg(msg)
        raise MonitoringCycleError(msg)


def _ws_liveness_guard_or_raise(
    now_ts: float,
    runtime: EngineLoopRuntime,
    ctx: MonitoringCycleContext,
) -> None:
    _require_float(now_ts, "now_ts", min_value=0.0)

    ws_status_any = ws_get_ws_status(ctx.symbol)
    ws_status = _require_dict(ws_status_any, "ws_status")

    transport_ok = _require_bool_key(ws_status, "transport_ok", "ws_status")
    market_feed_ok = _require_bool_key(ws_status, "market_feed_ok", "ws_status")
    state_raw = _require_dict_key(ws_status, "state", "ws_status")
    state = str(state_raw).upper().strip()

    if transport_ok or state in ("CONNECTING", "RECONNECTING", "OPENING"):
        if ctx.state.ws_liveness_consec_fails != 0:
            log(f"[WS_LIVENESS][RECOVER] consecutive={ctx.state.ws_liveness_consec_fails} -> 0")
        ctx.state.ws_liveness_consec_fails = 0

        if not market_feed_ok:
            warnings = _require_list_key(ws_status, "warnings", "ws_status")
            joined = _join_reason_list_strict(warnings, "ws_status.warnings")
            if joined:
                log(f"[WS_LIVENESS][WARN] {joined}")
        return

    reasons = _require_list_key(ws_status, "reasons", "ws_status")
    joined = _join_reason_list_strict(reasons, "ws_status.reasons")

    ctx.state.ws_liveness_consec_fails += 1
    log(
        f"[WS_LIVENESS][FAIL] {joined} "
        f"consecutive={ctx.state.ws_liveness_consec_fails}/{ctx.config.ws_liveness_fail_hardstop_n}"
    )

    if ctx.state.ws_liveness_consec_fails >= ctx.config.ws_liveness_fail_hardstop_n:
        _request_safe_stop_or_raise(runtime, "WS_LIVENESS")
        msg = (
            "[SAFE_STOP][WS_LIVENESS] transport failure confirmed "
            f"consecutive={ctx.state.ws_liveness_consec_fails} reasons={joined}"
        )
        log(msg)
        _safe_send_tg(msg)
        raise MonitoringCycleError(msg)


def _account_ws_connected_guard_or_raise(
    runtime: EngineLoopRuntime,
    ctx: MonitoringCycleContext,
) -> None:
    st_any = get_account_ws_status()
    st = _require_dict(st_any, "account_ws_status")

    running = _require_bool_key(st, "running", "account_ws_status")
    connected = _require_bool_key(st, "connected", "account_ws_status")
    listen_key_active = _require_bool_key(st, "listen_key_active", "account_ws_status")

    ok = running and connected and listen_key_active

    if ok:
        if ctx.state.account_ws_consec_fails != 0:
            log(f"[ACCOUNT_WS][RECOVER] consecutive={ctx.state.account_ws_consec_fails} -> 0")
        ctx.state.account_ws_consec_fails = 0
        return

    ctx.state.account_ws_consec_fails += 1
    log(
        "[ACCOUNT_WS][FAIL] "
        f"running={running} connected={connected} "
        f"listen_key_active={listen_key_active} "
        f"consecutive={ctx.state.account_ws_consec_fails}/{ctx.config.account_ws_fail_hardstop_n}"
    )

    if ctx.state.account_ws_consec_fails >= ctx.config.account_ws_fail_hardstop_n:
        _request_safe_stop_or_raise(runtime, "ACCOUNT_WS")
        msg = (
            "[SAFE_STOP][ACCOUNT_WS] account websocket disconnected or invalid "
            f"consecutive={ctx.state.account_ws_consec_fails}"
        )
        log(msg)
        _safe_send_tg(msg)
        raise MonitoringCycleError(msg)


def _exchange_position_is_open_strict(symbol: str) -> bool:
    sym = _normalize_symbol_strict(symbol)
    rows_any = fetch_open_positions(sym)
    rows = _require_list(rows_any, "fetch_open_positions")

    for r_any in rows:
        r = _require_dict(r_any, "fetch_open_positions.row")
        row_symbol = _normalize_symbol_strict(
            _require_dict_key(r, "symbol", "fetch_open_positions.row"),
            name="fetch_open_positions.row.symbol",
        )
        if row_symbol != sym:
            continue

        amt = _require_float(
            _require_dict_key(r, "positionAmt", "fetch_open_positions.row"),
            "fetch_open_positions.row.positionAmt",
        )
        if abs(amt) > 1e-12:
            return True
    return False


def _is_rest_protection_order_row_strict(
    order: Dict[str, Any],
    *,
    symbol: str,
    expected_position_side: str,
) -> bool:
    row_symbol = _normalize_symbol_strict(
        _require_dict_key(order, "symbol", "open_order"),
        name="open_order.symbol",
    )
    if row_symbol != _normalize_symbol_strict(symbol, name="symbol"):
        return False

    order_type = _require_nonempty_str(
        _require_dict_key(order, "type", "open_order"),
        "open_order.type",
    ).upper().strip()
    if order_type not in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT", "STOP_MARKET", "STOP"):
        return False

    reduce_only = _require_bool(
        _require_dict_key(order, "reduceOnly", "open_order"),
        "open_order.reduceOnly",
    )
    close_position = _require_bool(
        _require_dict_key(order, "closePosition", "open_order"),
        "open_order.closePosition",
    )
    if not reduce_only and not close_position:
        return False

    row_ps = _normalize_position_side_guard(
        _require_dict_key(order, "positionSide", "open_order"),
        "open_order.positionSide",
    )
    return _position_side_compatible(expected_position_side, row_ps)


def _has_account_ws_protection_order_strict(
    symbol: str,
    order_id: str,
    expected_position_side: str,
) -> bool:
    rows_any = get_account_protection_orders_snapshot(symbol, limit=200)
    rows = _require_list(rows_any, "account_protection_orders_snapshot")

    wanted = _require_nonempty_str(order_id, "order_id")
    for row_any in rows:
        row = _require_dict(row_any, "account_protection_order")
        oid = _require_nonempty_str(
            _require_dict_key(row, "order_id", "account_protection_order"),
            "account_protection_order.order_id",
        )
        if oid != wanted:
            continue

        row_ps = _normalize_position_side_guard(
            _require_dict_key(row, "position_side", "account_protection_order"),
            "account_protection_order.position_side",
        )
        if not _position_side_compatible(expected_position_side, row_ps):
            raise MonitoringContractError(
                "account protection order positionSide mismatch (STRICT): "
                f"order_id={wanted} row_ps={row_ps} expected={expected_position_side}"
            )
        return True

    return False


def _has_rest_protection_order_strict(
    symbol: str,
    order_id: str,
    expected_position_side: str,
) -> bool:
    sym = _normalize_symbol_strict(symbol)
    wanted = _require_nonempty_str(order_id, "order_id")

    orders_any = fetch_open_orders(sym)
    orders = _require_list(orders_any, "fetch_open_orders")

    for row_any in orders:
        row = _require_dict(row_any, "fetch_open_orders.row")
        oid_raw = _require_dict_key(row, "orderId", "fetch_open_orders.row")
        oid = _require_nonempty_str(oid_raw, "fetch_open_orders.row.orderId")
        if oid != wanted:
            continue

        if not _is_rest_protection_order_row_strict(
            row,
            symbol=sym,
            expected_position_side=expected_position_side,
        ):
            raise MonitoringContractError(
                f"open order exists but is not valid protection order (STRICT): order_id={wanted}"
            )
        return True

    return False


def _protection_orders_guard_or_raise(
    now_ts: float,
    runtime: EngineLoopRuntime,
    ctx: MonitoringCycleContext,
) -> None:
    _require_float(now_ts, "now_ts", min_value=0.0)

    if not ctx.open_trades_ref:
        return

    if (now_ts - ctx.state.last_protection_guard_ts) < ctx.config.protection_guard_interval_sec:
        return

    if len(ctx.open_trades_ref) != 1:
        raise MonitoringContractError(
            f"OPEN_TRADES must be <= 1 in one-way mode (STRICT), got={len(ctx.open_trades_ref)}"
        )

    trade = ctx.open_trades_ref[0]
    symbol = _normalize_symbol_strict(_require_attr(trade, "symbol", "trade"), name="trade.symbol")

    if not _exchange_position_is_open_strict(symbol):
        ctx.state.last_protection_guard_ts = now_ts
        return

    expected_position_side = _normalize_position_side_guard(
        _require_attr(trade, "exchange_position_side", "trade"),
        "trade.exchange_position_side",
    )

    tp_order_id_raw = _require_attr(trade, "tp_order_id", "trade")
    sl_order_id_raw = _require_attr(trade, "sl_order_id", "trade")
    tp_order_id = str(tp_order_id_raw).strip() if tp_order_id_raw is not None else ""
    sl_order_id = str(sl_order_id_raw).strip() if sl_order_id_raw is not None else ""

    missing_fields: List[str] = []
    if not tp_order_id:
        missing_fields.append("tp_order_id")
    if not sl_order_id:
        missing_fields.append("sl_order_id")

    force_close_on_desync = _require_bool(
        _require_setting_attr(ctx.settings, "force_close_on_desync"),
        "settings.force_close_on_desync",
    )

    if missing_fields:
        _request_safe_stop_or_raise(runtime, "PROTECTION_MISSING")
        msg = f"[SAFE_STOP][PROTECTION_MISSING] trade missing protection ids: {missing_fields} symbol={symbol}"
        log(msg)
        _safe_send_tg(msg)

        if force_close_on_desync:
            try:
                n = ctx.force_close_fn(symbol)
                _require_int(n, "force_close.count", min_value=0)
                log(f"[PROTECTION_MISSING] force close submitted count={n}")
                _safe_send_tg(f"🧯 보호주문 누락 강제정리 제출: {symbol} count={n}")
            except Exception as e:
                log(f"[PROTECTION_MISSING] force close failed: {type(e).__name__}: {e}")
                _safe_send_tg(f"❌ 보호주문 누락 강제정리 실패: {e}")
                raise MonitoringCycleError(
                    f"protection order force close failed: {type(e).__name__}: {e}"
                ) from e
        raise MonitoringCycleError(msg)

    tp_ok = _has_account_ws_protection_order_strict(symbol, tp_order_id, expected_position_side)
    sl_ok = _has_account_ws_protection_order_strict(symbol, sl_order_id, expected_position_side)

    if not tp_ok:
        tp_ok = _has_rest_protection_order_strict(symbol, tp_order_id, expected_position_side)
    if not sl_ok:
        sl_ok = _has_rest_protection_order_strict(symbol, sl_order_id, expected_position_side)

    if tp_ok and sl_ok:
        ctx.state.last_protection_guard_ts = now_ts
        return

    _request_safe_stop_or_raise(runtime, "PROTECTION_ORDERS_MISSING")
    missing_runtime: List[str] = []
    if not tp_ok:
        missing_runtime.append(f"TP({tp_order_id})")
    if not sl_ok:
        missing_runtime.append(f"SL({sl_order_id})")

    msg = f"[SAFE_STOP][PROTECTION_ORDERS_MISSING] symbol={symbol} missing={missing_runtime}"
    log(msg)
    _safe_send_tg(msg)

    if force_close_on_desync:
        try:
            n = ctx.force_close_fn(symbol)
            _require_int(n, "force_close.count", min_value=0)
            log(f"[PROTECTION_ORDERS_MISSING] force close submitted count={n}")
            _safe_send_tg(f"🧯 보호주문 미존재 강제정리 제출: {symbol} count={n}")
        except Exception as e:
            log(f"[PROTECTION_ORDERS_MISSING] force close failed: {type(e).__name__}: {e}")
            _safe_send_tg(f"❌ 보호주문 미존재 강제정리 실패: {e}")
            raise MonitoringCycleError(
                f"protection order force close failed: {type(e).__name__}: {e}"
            ) from e

    raise MonitoringCycleError(msg)


def _reconcile_or_raise(
    now_ts: float,
    ctx: MonitoringCycleContext,
) -> None:
    _require_float(now_ts, "now_ts", min_value=0.0)
    run_if_due = getattr(ctx.reconcile_engine, "run_if_due")
    run_if_due(now_ts=now_ts)


def _sigterm_deadline_guard_or_raise(
    now_ts: float,
    runtime: EngineLoopRuntime,
    ctx: MonitoringCycleContext,
) -> None:
    _require_float(now_ts, "now_ts", min_value=0.0)

    if runtime.sigterm_deadline_ts is None:
        return
    if ctx.state.sigterm_deadline_handled:
        return
    if now_ts < runtime.sigterm_deadline_ts:
        return

    ctx.state.sigterm_deadline_handled = True
    log("[SIGTERM] grace deadline reached. force close attempt starts.")
    _safe_send_tg("🧯 SIGTERM grace 경과: 강제 정리 시도합니다.")

    try:
        n = ctx.force_close_fn(ctx.symbol)
        _require_int(n, "force_close.count", min_value=0)
        log(f"[SIGTERM] force close submitted count={n}")
        _safe_send_tg(f"🧯 SIGTERM 강제 정리 제출: count={n}")
    except Exception as e:
        log(f"[SIGTERM] force close failed: {type(e).__name__}: {e}")
        _safe_send_tg(f"❌ SIGTERM 강제 정리 실패: {e}")
        _request_safe_stop_or_raise(runtime, "SIGTERM_FORCE_CLOSE_FAIL")
        raise MonitoringCycleError(f"sigterm force close failed: {type(e).__name__}: {e}") from e

    _request_safe_stop_or_raise(runtime, "SIGTERM_DEADLINE")


def run_monitoring_cycle_or_raise(
    now_ts: float,
    runtime: EngineLoopRuntime,
    ctx: MonitoringCycleContext,
) -> None:
    ctx.validate_or_raise()
    runtime.validate_or_raise()
    _require_float(now_ts, "now_ts", min_value=0.0)

    _watchdog_guard_or_raise(now_ts, runtime, ctx)
    _drift_guard_or_raise(now_ts, runtime, ctx)
    _ws_liveness_guard_or_raise(now_ts, runtime, ctx)
    _account_ws_connected_guard_or_raise(runtime, ctx)
    _protection_orders_guard_or_raise(now_ts, runtime, ctx)
    _reconcile_or_raise(now_ts, ctx)
    _sigterm_deadline_guard_or_raise(now_ts, runtime, ctx)


def build_monitoring_cycle_fn(ctx: MonitoringCycleContext) -> Callable[[float, EngineLoopRuntime], None]:
    ctx.validate_or_raise()

    def _fn(now_ts: float, runtime: EngineLoopRuntime) -> None:
        run_monitoring_cycle_or_raise(now_ts, runtime, ctx)

    return _fn


__all__ = [
    "MonitoringCycleError",
    "MonitoringContractError",
    "MonitoringCycleConfig",
    "MonitoringCycleState",
    "MonitoringCycleContext",
    "build_monitoring_cycle_context_or_raise",
    "build_monitoring_cycle_fn",
    "run_monitoring_cycle_or_raise",
]