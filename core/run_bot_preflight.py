"""
========================================================
FILE: core/run_bot_preflight.py
ROLE:
- 트레이딩 엔진 기동 전 인프라 / 데이터 / 실행 계약을 사전 검증한다
- WS / DB / Binance / feature / execution contract 준비 상태를 검증한 뒤
  검증 통과 시 run_bot_ws.main() 으로 handoff 한다
- Meta L3 는 트레이딩 엔진 preflight의 관측 대상이며, 별도 서비스의 readiness와 분리한다

CORE RESPONSIBILITIES:
- settings / DB / Binance private API / WS bootstrap / unified features 검증
- 현재 시장 상태와 독립적인 execution dry-run probe 생성 및 실행 계약 검증
- Meta L3 readiness 관측 및 상태 로그화
- 치명 계약 위반 시 즉시 예외 및 preflight 중단

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- settings 는 SETTINGS 단일 객체만 사용한다
- 데이터/계약/스키마 불일치는 즉시 예외 처리한다
- preflight 는 "인프라/계약 검증"이며 "현재 시장의 진입 가능 여부"와 분리한다
- execution dry-run 은 live no-trade 상태와 무관하게 명시적 probe signal 로 검증한다
- Meta L3 는 별도 전략/분석 계층이며, trading-engine preflight의 필수 시작 게이트가 아니다
- silent fail 금지, readiness 부족은 명시적 로그/결과 note 로 드러낸다
- WS bootstrap / signal timestamp 계약은 runtime(run_bot_ws / entry_pipeline)과 정합해야 한다
- preflight 는 runtime 내부 심볼을 monkey patch 하지 않는다
- handoff 이후 authoritative bootstrap / worker start / runtime orchestration 은 run_bot_ws 가 책임진다

CHANGE HISTORY:
--------------------------------------------------------
- 2026-03-15:
  1) FIX(ARCH): preflight→runtime handoff helper 추가
  2) FIX(CONTRACT): core.run_bot_ws.main callable 계약을 handoff 시점에 strict 검증
  3) FIX(ARCH): run_bot_ws 의 preflight 역참조 제거와 정합되도록 단방향 handoff 유지
  4) FIX(ROOT-CAUSE): WS ready 직후 orderbook recovery/resync 안정화 대기 단계 추가
  5) FIX(BOOT): orderbook snapshot 존재와 orderbook 안정화 상태를 분리 검증하도록 강화
- 2026-03-14:
  1) FIX(ROOT-CAUSE): GPT_PING max_tokens 하드코딩(512) 제거 → settings.openai_max_tokens SSOT 사용
  2) FIX(OPERABILITY): GPT ping JSON 스키마를 최소화하여 max_output_tokens false fail 가능성 축소
  3) FIX(STRICT): GPT ping 응답을 {"ok": true} 단일 object 계약으로 강화
  4) FIX(ARCH): handoff 시 run_bot_ws / engine_bootstrap 내부 심볼 monkey patch 제거
  5) FIX(ARCH): preflight 는 runtime bootstrap ownership 을 주장하지 않고 검증 후 정상 handoff 만 수행
- 2026-03-11:
  1) FIX(ARCH): META_L3 를 preflight 필수 게이트에서 observability 단계로 재정의
  2) FIX(ROOT-CAUSE): live no-trade 상태와 분리된 execution probe signal 도입
  3) FIX(CONTRACT): signal.risk_pct / meta.dynamic_risk_pct / dynamic_allocation_ratio 계약 일치화
  4) FIX(SSOT): settings.load_settings() 재호출 제거, SETTINGS 단일 객체 사용
  5) FIX(SSOT): meta_* / preflight_fake_usdt 를 SETTINGS 정식 필드로 직접 사용
  6) FIX(ROOT-CAUSE): WS bootstrap 최소 버퍼 계약을 runtime 기준과 정합화
     - 1m=120 / 5m=200 / 15m=200 / 1h=200 / 4h=200 으로 상향
     - preflight 통과 후 runtime feature/entry 단계에서 뒤늦게 실패하던 구조 제거
  7) FIX(CONTRACT): REST backfill fetch_limit 을 configured_limit 그대로 쓰지 않고 실제 required min 기준으로 상향
  8) FIX(CONTRACT): preflight signal_ts_ms 를 authoritative 1m 이 아닌 authoritative 5m 기준으로 통일
  9) FIX(STRICT): settings.preflight_ws_wait_sec 를 settings 단계에서 선검증
- 2026-03-10:
  1) FIX(STRICT): PREFLIGHT dry-run Signal의 source 계열 필드 충돌 제거
  2) FIX(STRUCTURE): PREFLIGHT lifecycle 정보와 시장/신호 source 의미를 분리
  3) FIX(ROOT-CAUSE): EXECUTION_DRY_RUN 에서 ExecutionEngine 인스턴스 메서드 오호출 제거
  4) FIX(CONTRACT): dry-run 에서 normalize + deterministic client_order_id 생성 경로 검증
========================================================
"""

from __future__ import annotations

import argparse
import importlib
import math
import os
import socket
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from sqlalchemy import text

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from settings import SETTINGS  # noqa: E402
from infra.telelog import log, send_tg  # noqa: E402

from state.db_core import get_session  # noqa: E402

from execution.exchange_api import (  # noqa: E402
    fetch_open_orders,
    get_balance_detail,
)
from infra.market_data_rest import fetch_klines_rest  # noqa: E402
from infra.market_data_ws import (  # noqa: E402
    backfill_klines_from_rest,
    get_klines_with_volume as ws_get_klines_with_volume,
    get_orderbook as ws_get_orderbook,
    start_ws_loop,
)

from infra.market_features_ws import FeatureBuildError, build_entry_features_ws  # noqa: E402
from strategy.unified_features_builder import UnifiedFeaturesError, build_unified_features  # noqa: E402

from strategy.regime_engine import RegimeEngine  # noqa: E402
from execution.risk_physics_engine import RiskPhysicsEngine, RiskPhysicsPolicy  # noqa: E402
from strategy.ev_heatmap_engine import EvHeatmapEngine, HeatmapKey  # noqa: E402

from infra.drift_detector import DriftDetector, DriftDetectorConfig, DriftSnapshot, DriftDetectedError  # noqa: E402
from execution.invariant_guard import SignalInvariantInputs, validate_signal_invariants_strict, InvariantViolation  # noqa: E402

from execution.execution_engine import (  # noqa: E402
    ExecutionEngine,
    _enforce_or_generate_client_order_id_strict,
    _normalize_entry_request_strict,
)
from strategy.signal import Signal  # noqa: E402

from meta.meta_strategy_engine import MetaStrategyConfig, MetaStrategyEngine  # noqa: E402
from meta.meta_stats_aggregator import MetaStatsConfig, MetaStatsError  # noqa: E402
from meta.meta_prompt_builder import MetaPromptConfig  # noqa: E402

from strategy.gpt_engine import GptEngineError, call_chat_json  # noqa: E402

from infra.data_integrity_guard import (  # noqa: E402
    DataIntegrityError,
    validate_kline_series_strict,
    validate_orderbook_strict,
)

SET = SETTINGS

ENTRY_REQUIRED_TFS: Tuple[str, ...] = ("1m", "5m", "15m", "1h", "4h")
ENTRY_REQUIRED_KLINES_MIN: Dict[str, int] = {
    "1m": 120,
    "5m": 200,
    "15m": 200,
    "1h": 200,
    "4h": 200,
}

PREFLIGHT_SIGNAL_SOURCE = "PREFLIGHT"
PREFLIGHT_REASON = "PREFLIGHT"
PREFLIGHT_REASON_DETAIL = "PREFLIGHT_EXECUTION_PROBE"
PREFLIGHT_STAGE_NAME = "EXECUTION_DRY_RUN"

META_L3_STAGE_NAME = "META_L3_OBSERVABILITY"

PREFLIGHT_ORDERBOOK_STABILIZE_MIN_SEC: float = 3.0
PREFLIGHT_ORDERBOOK_STABILIZE_CONFIRM_N: int = 3
PREFLIGHT_ORDERBOOK_STABILIZE_TIMEOUT_SEC: float = 8.0
PREFLIGHT_ORDERBOOK_STABILIZE_POLL_SEC: float = 0.5


class PreflightError(RuntimeError):
    """Preflight 단계 실패."""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _finite(x: Any, name: str) -> float:
    if x is None:
        raise PreflightError(f"{name} is required (STRICT)")
    if isinstance(x, bool):
        raise PreflightError(f"{name} must be numeric (bool not allowed)")
    try:
        v = float(x)
    except Exception as e:
        raise PreflightError(f"{name} must be numeric: {e}") from e
    if not math.isfinite(v):
        raise PreflightError(f"{name} must be finite (STRICT)")
    return float(v)


def _require_nonempty_str(v: Any, name: str) -> str:
    if v is None:
        raise PreflightError(f"{name} is required (STRICT)")
    s = str(v).strip()
    if not s:
        raise PreflightError(f"{name} is required (STRICT)")
    return s


def _require_bool(v: Any, name: str) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        raise PreflightError(f"{name} is required (STRICT)")
    s = str(v).strip().lower()
    if s in ("1", "true", "t", "yes", "y", "on"):
        return True
    if s in ("0", "false", "f", "no", "n", "off"):
        return False
    raise PreflightError(f"{name} must be bool-like (STRICT), got={v!r}")


def _require_positive_int_from_any(v: Any, name: str) -> int:
    if v is None:
        raise PreflightError(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        raise PreflightError(f"{name} must be int (bool not allowed) (STRICT)")
    try:
        iv = int(v)
    except Exception as e:
        raise PreflightError(f"{name} must be int-convertible (STRICT): {e}") from e
    if iv <= 0:
        raise PreflightError(f"{name} must be > 0 (STRICT)")
    return int(iv)


def _parse_tfs(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        items = [x.strip() for x in value.split(",")]
        return [x for x in items if x]
    if isinstance(value, (list, tuple, set)):
        out: List[str] = []
        for v in value:
            s = str(v).strip()
            if s:
                out.append(s)
        return out
    raise PreflightError(f"tfs must be str or list-like (STRICT), got={type(value).__name__}")


def _verify_required_tfs_or_die(name: str, configured_tfs: List[str], required_tfs: Tuple[str, ...]) -> None:
    norm = {str(x).strip().lower() for x in configured_tfs if str(x).strip()}
    missing = [tf for tf in required_tfs if tf.lower() not in norm]
    if missing:
        raise PreflightError(f"settings.{name} missing required TFs={missing} required={list(required_tfs)} (STRICT)")


def _sanitize_err(e: BaseException) -> str:
    s = str(e).replace("\n", " ").strip()
    if len(s) > 240:
        s = s[:240]
    return f"{type(e).__name__}: {s}"


def _safe_tg_notice(msg: str) -> None:
    try:
        send_tg(msg)
    except Exception as e:
        log(f"[PRE-FLIGHT][TG_WARN] send_tg failed: {_sanitize_err(e)}")


@dataclass(frozen=True, slots=True)
class StageResult:
    name: str
    ok: bool
    elapsed_ms: float
    note: str


@dataclass(frozen=True, slots=True)
class PreflightBoot:
    symbol: str
    last_price: float
    candles_5m: List[List[Any]]
    candles_5m_raw: List[List[Any]]
    signal_ts_ms: int


@dataclass(frozen=True, slots=True)
class PipelineSimulationResult:
    eq_cur: float
    eq_peak: float
    dd_pct: float
    regime_score: float
    regime_band: str
    regime_allocation: float
    micro_score_risk: float
    live_effective_risk_pct: float
    execution_probe_risk_pct: float
    tp_pct: float
    sl_pct: float
    decision_meta: Dict[str, float]
    hk: HeatmapKey
    ev_cell_status: str
    ev_cell_ev: Optional[float]
    ev_cell_n: int


@dataclass(frozen=True, slots=True)
class MetaL3Status:
    ready: bool
    note: str
    final_risk_multiplier: Optional[float]


def _run_stage(name: str, fn: Callable[[], Any]) -> Tuple[StageResult, Any]:
    t0 = time.perf_counter()
    try:
        out = fn()
    except Exception as e:
        dt = (time.perf_counter() - t0) * 1000.0
        note = _sanitize_err(e)
        msg = f"⛔ PRE-FLIGHT FAILED\nStage: {name}\n{note}\nHost: {socket.gethostname()}\nUTC: {_utc_now().isoformat()}"
        log(msg)
        _safe_tg_notice(msg)
        raise
    dt = (time.perf_counter() - t0) * 1000.0
    return StageResult(name=name, ok=True, elapsed_ms=float(dt), note="OK"), out


def _resolve_preflight_fake_available_usdt_strict() -> float:
    value = _finite(SET.preflight_fake_usdt, "settings.preflight_fake_usdt")
    if value <= 0:
        raise PreflightError("settings.preflight_fake_usdt must be > 0 (STRICT)")
    return float(value)


def _stage_settings_strict() -> None:
    symbol = _require_nonempty_str(SET.symbol, "settings.symbol").replace("-", "").replace("/", "").upper().strip()
    if not symbol:
        raise PreflightError("settings.symbol normalized empty (STRICT)")

    _ = _require_nonempty_str(SET.api_key, "settings.api_key")
    _ = _require_nonempty_str(SET.api_secret, "settings.api_secret")

    lev = _finite(SET.leverage, "settings.leverage")
    if lev <= 0:
        raise PreflightError("settings.leverage must be > 0 (STRICT)")

    tp = _finite(SET.tp_pct, "settings.tp_pct")
    sl = _finite(SET.sl_pct, "settings.sl_pct")
    if tp <= 0:
        raise PreflightError("settings.tp_pct must be > 0 (STRICT)")
    if sl <= 0:
        raise PreflightError("settings.sl_pct must be > 0 (STRICT)")

    allocation_ratio = _finite(SET.allocation_ratio, "settings.allocation_ratio")
    risk_pct = _finite(SET.risk_pct, "settings.risk_pct")
    if allocation_ratio <= 0 or allocation_ratio > 1:
        raise PreflightError("settings.allocation_ratio must be within (0,1] (STRICT)")
    if risk_pct <= 0 or risk_pct > 1:
        raise PreflightError("settings.risk_pct must be within (0,1] (STRICT)")
    if abs(allocation_ratio - risk_pct) > 1e-12:
        raise PreflightError("settings.allocation_ratio and settings.risk_pct mismatch (STRICT)")

    ws_enabled = _require_bool(SET.ws_enabled, "settings.ws_enabled")
    if not ws_enabled:
        raise PreflightError("settings.ws_enabled must be True for WS-driven engine (STRICT)")

    ws_subscribe_tfs = _parse_tfs(SET.ws_subscribe_tfs)
    _verify_required_tfs_or_die("ws_subscribe_tfs", ws_subscribe_tfs, ENTRY_REQUIRED_TFS)

    ws_backfill_tfs = _parse_tfs(SET.ws_backfill_tfs)
    if ws_backfill_tfs:
        _verify_required_tfs_or_die("ws_backfill_tfs", ws_backfill_tfs, ENTRY_REQUIRED_TFS)

    _ = _require_nonempty_str(SET.openai_api_key, "settings.openai_api_key")
    _ = _require_nonempty_str(SET.openai_model, "settings.openai_model")
    _ = _finite(SET.openai_max_latency_sec, "settings.openai_max_latency_sec")
    _ = _finite(SET.openai_temperature, "settings.openai_temperature")
    _ = _require_positive_int_from_any(SET.openai_max_tokens, "settings.openai_max_tokens")
    _ = _finite(SET.preflight_fake_usdt, "settings.preflight_fake_usdt")

    preflight_ws_wait_sec = _finite(SET.preflight_ws_wait_sec, "settings.preflight_ws_wait_sec")
    if preflight_ws_wait_sec <= 0:
        raise PreflightError("settings.preflight_ws_wait_sec must be > 0 (STRICT)")


def _stage_db_connectivity_strict() -> None:
    with get_session() as session:
        v = session.execute(text("SELECT 1")).scalar()
        if v != 1:
            raise PreflightError("DB SELECT 1 failed (STRICT)")
        for tname in ("bt_trades", "bt_trade_snapshots", "bt_trade_exit_snapshots"):
            reg = session.execute(text("SELECT to_regclass(:tname)"), {"tname": tname}).scalar()
            if reg is None:
                raise PreflightError(f"DB table missing (STRICT): {tname}")


def _stage_binance_private_api_strict() -> None:
    symbol = str(SET.symbol).replace("-", "").replace("/", "").upper().strip()
    if not symbol:
        raise PreflightError("settings.symbol empty (STRICT)")

    bal = get_balance_detail("USDT")
    if not isinstance(bal, dict) or not bal:
        raise PreflightError("get_balance_detail('USDT') returned non-dict/empty (STRICT)")

    for k in ("availableBalance", "crossUnPnl"):
        if k not in bal:
            raise PreflightError(f"balance detail missing key (STRICT): {k}")

    _ = _finite(bal.get("availableBalance"), "balance.availableBalance")
    _ = _finite(bal.get("crossUnPnl"), "balance.crossUnPnl")

    if bool(SET.test_dry_run) and float(SET.test_fake_available_usdt) > 0.0:
        eq_cur = float(_finite(SET.test_fake_available_usdt, "settings.test_fake_available_usdt"))
        if eq_cur <= 0:
            raise PreflightError("settings.test_fake_available_usdt must be > 0 in test_dry_run (STRICT)")
    else:
        eq_cur = float(_finite(bal.get("availableBalance"), "balance.availableBalance"))
        if eq_cur <= 0:
            raise PreflightError("equity_current_usdt must be > 0 (STRICT)")

    orders = fetch_open_orders(symbol)
    if not isinstance(orders, list):
        raise PreflightError("fetch_open_orders returned non-list (STRICT)")


def _validate_kline_rows_strict(rows: List[List[Any]], *, name: str, min_len: int) -> None:
    if not isinstance(rows, list) or not rows:
        raise PreflightError(f"{name} returned empty/non-list (STRICT)")
    if len(rows) < min_len:
        raise PreflightError(f"{name} insufficient rows (STRICT): need>={min_len} got={len(rows)}")

    last_ts: Optional[int] = None
    for i, r in enumerate(rows):
        if not isinstance(r, list) or len(r) < 6:
            raise PreflightError(f"{name}[{i}] invalid row shape (STRICT)")
        ts = int(r[0])
        if ts <= 0:
            raise PreflightError(f"{name}[{i}] ts<=0 (STRICT)")
        if last_ts is not None and ts < last_ts:
            raise PreflightError(f"{name} timestamp rollback detected (STRICT): {last_ts} -> {ts}")
        last_ts = ts

        _ = _finite(r[1], f"{name}[{i}].open")
        h = _finite(r[2], f"{name}[{i}].high")
        l = _finite(r[3], f"{name}[{i}].low")
        c = _finite(r[4], f"{name}[{i}].close")
        v = _finite(r[5], f"{name}[{i}].volume")
        if h < l:
            raise PreflightError(f"{name}[{i}] high<low (STRICT)")
        if c < l or c > h:
            raise PreflightError(f"{name}[{i}] close out of range (STRICT)")
        if v < 0:
            raise PreflightError(f"{name}[{i}] volume<0 (STRICT)")


def _wait_for_stable_orderbook_or_raise(symbol: str) -> None:
    symbol_norm = _require_nonempty_str(symbol, "symbol").replace("-", "").replace("/", "").upper().strip()
    deadline = time.time() + PREFLIGHT_ORDERBOOK_STABILIZE_TIMEOUT_SEC
    started_at = time.time()
    consecutive_ok = 0

    while True:
        ob = ws_get_orderbook(symbol_norm, limit=5)
        ok_ob = isinstance(ob, dict) and bool(ob.get("bids")) and bool(ob.get("asks"))

        if ok_ob:
            consecutive_ok += 1
        else:
            consecutive_ok = 0

        elapsed = time.time() - started_at
        if (
            elapsed >= PREFLIGHT_ORDERBOOK_STABILIZE_MIN_SEC
            and consecutive_ok >= PREFLIGHT_ORDERBOOK_STABILIZE_CONFIRM_N
        ):
            return

        if time.time() >= deadline:
            raise PreflightError(
                "WS orderbook snapshot did not stabilize after bootstrap "
                f"(STRICT): waited={elapsed:.1f}s confirm_n={consecutive_ok}"
            )

        time.sleep(PREFLIGHT_ORDERBOOK_STABILIZE_POLL_SEC)


def _stage_bootstrap_ws_strict() -> PreflightBoot:
    symbol = str(SET.symbol).replace("-", "").replace("/", "").upper().strip()
    if not symbol:
        raise PreflightError("settings.symbol empty (STRICT)")

    configured_limit = int(SET.ws_backfill_limit)
    if configured_limit <= 0:
        raise PreflightError("settings.ws_backfill_limit must be > 0 (STRICT)")

    for tf in ENTRY_REQUIRED_TFS:
        required_min = ENTRY_REQUIRED_KLINES_MIN[tf]
        fetch_limit = max(configured_limit, required_min)
        rows = fetch_klines_rest(symbol, tf, limit=fetch_limit)
        _validate_kline_rows_strict(rows, name=f"REST_KLINES_{tf}", min_len=required_min)
        backfill_klines_from_rest(symbol, tf, rows)

    start_ws_loop(symbol)

    wait_sec = float(_finite(SET.preflight_ws_wait_sec, "settings.preflight_ws_wait_sec"))
    if wait_sec <= 0:
        raise PreflightError("settings.preflight_ws_wait_sec must be > 0 (STRICT)")
    deadline = time.time() + wait_sec

    while True:
        ob = ws_get_orderbook(symbol, limit=5)
        ok_ob = isinstance(ob, dict) and bool(ob.get("bids")) and bool(ob.get("asks"))

        ok_buf = True
        for iv, need in ENTRY_REQUIRED_KLINES_MIN.items():
            buf = ws_get_klines_with_volume(symbol, iv, limit=need)
            if not isinstance(buf, list) or len(buf) < need:
                ok_buf = False
                break

        if ok_ob and ok_buf:
            break
        if time.time() >= deadline:
            raise PreflightError("WS not ready within deadline (STRICT)")
        time.sleep(0.5)

    _wait_for_stable_orderbook_or_raise(symbol)

    ob = ws_get_orderbook(symbol, limit=5)
    try:
        validate_orderbook_strict(ob, symbol=symbol, require_ts=False)
    except DataIntegrityError as e:
        raise PreflightError(f"WS orderbook integrity fail (STRICT): {e}") from e

    for iv, need in ENTRY_REQUIRED_KLINES_MIN.items():
        buf = ws_get_klines_with_volume(symbol, iv, limit=need)
        try:
            validate_kline_series_strict(buf, name=f"WS_KLINES_{iv}", min_len=need)
        except DataIntegrityError as e:
            raise PreflightError(f"WS kline integrity fail (STRICT): {iv} {e}") from e

    candles_5m = ws_get_klines_with_volume(symbol, "5m", limit=ENTRY_REQUIRED_KLINES_MIN["5m"])
    candles_5m_raw = ws_get_klines_with_volume(symbol, "5m", limit=ENTRY_REQUIRED_KLINES_MIN["5m"])
    if not isinstance(candles_5m, list) or len(candles_5m) < ENTRY_REQUIRED_KLINES_MIN["5m"]:
        raise PreflightError("WS candles_5m insufficient (STRICT)")
    if not isinstance(candles_5m_raw, list) or len(candles_5m_raw) < ENTRY_REQUIRED_KLINES_MIN["5m"]:
        raise PreflightError("WS candles_5m_raw insufficient (STRICT)")

    k5 = ws_get_klines_with_volume(symbol, "5m", limit=1)
    if not isinstance(k5, list) or not k5 or len(k5[0]) < 1:
        raise PreflightError("WS 5m buffer missing for signal_ts_ms (STRICT)")
    signal_ts_ms = int(k5[0][0])
    if signal_ts_ms <= 0:
        raise PreflightError("signal_ts_ms invalid (STRICT)")

    last_price = float(_finite(k5[0][4], "WS_5m.last_close"))
    if last_price <= 0:
        raise PreflightError("last_price must be > 0 (STRICT)")

    return PreflightBoot(
        symbol=symbol,
        last_price=float(last_price),
        candles_5m=candles_5m,
        candles_5m_raw=candles_5m_raw,
        signal_ts_ms=int(signal_ts_ms),
    )


def _stage_unified_features_strict(boot: PreflightBoot) -> Dict[str, Any]:
    try:
        mf = build_entry_features_ws(boot.symbol)
    except FeatureBuildError as e:
        raise PreflightError(f"build_entry_features_ws failed (STRICT): {e}") from e
    if not isinstance(mf, dict) or not mf:
        raise PreflightError("market_features_ws returned empty/invalid (STRICT)")

    try:
        uf = build_unified_features(boot.symbol)
    except (UnifiedFeaturesError, FeatureBuildError) as e:
        raise PreflightError(f"build_unified_features failed (STRICT): {e}") from e

    if not isinstance(uf, dict) or not uf:
        raise PreflightError("unified_features empty/invalid (STRICT)")

    if "engine_scores" not in uf or not isinstance(uf.get("engine_scores"), dict):
        raise PreflightError("unified_features.engine_scores missing/invalid (STRICT)")
    if "microstructure" not in uf or not isinstance(uf.get("microstructure"), dict):
        raise PreflightError("unified_features.microstructure missing/invalid (STRICT)")
    if "orderbook" not in uf or not isinstance(uf.get("orderbook"), dict):
        raise PreflightError("unified_features.orderbook missing/invalid (STRICT)")

    total = uf["engine_scores"].get("total")
    if not isinstance(total, dict) or "score" not in total:
        raise PreflightError("unified_features.engine_scores.total.score missing (STRICT)")
    _ = _finite(total.get("score"), "unified_features.engine_scores.total.score")

    return uf


def _extract_preflight_decision_meta_strict(uf: Dict[str, Any]) -> Dict[str, float]:
    if not isinstance(uf, dict) or not uf:
        raise PreflightError("unified_features is required (STRICT)")

    engine_scores = uf.get("engine_scores")
    if not isinstance(engine_scores, dict) or not engine_scores:
        raise PreflightError("unified_features.engine_scores missing/invalid (STRICT)")

    total = engine_scores.get("total")
    if not isinstance(total, dict) or "score" not in total:
        raise PreflightError("unified_features.engine_scores.total.score missing (STRICT)")

    trend_4h = engine_scores.get("trend_4h")
    if not isinstance(trend_4h, dict) or not trend_4h:
        raise PreflightError("unified_features.engine_scores.trend_4h missing/invalid (STRICT)")

    trend_components = trend_4h.get("components")
    if not isinstance(trend_components, dict) or not trend_components:
        raise PreflightError("unified_features.engine_scores.trend_4h.components missing/invalid (STRICT)")
    if "trend_strength" not in trend_components:
        raise PreflightError("unified_features.engine_scores.trend_4h.components.trend_strength missing (STRICT)")

    orderbook = uf.get("orderbook")
    if not isinstance(orderbook, dict) or not orderbook:
        raise PreflightError("unified_features.orderbook missing/invalid (STRICT)")
    if "spread_pct" not in orderbook:
        raise PreflightError("unified_features.orderbook.spread_pct missing (STRICT)")
    if "depth_imbalance" not in orderbook:
        raise PreflightError("unified_features.orderbook.depth_imbalance missing (STRICT)")

    entry_score_threshold = _finite(SET.entry_score_threshold, "settings.entry_score_threshold")
    total_score_pct = _finite(total.get("score"), "unified_features.engine_scores.total.score")
    trend_strength = _finite(
        trend_components.get("trend_strength"),
        "unified_features.engine_scores.trend_4h.components.trend_strength",
    )
    spread = _finite(orderbook.get("spread_pct"), "unified_features.orderbook.spread_pct")
    orderbook_imbalance = _finite(orderbook.get("depth_imbalance"), "unified_features.orderbook.depth_imbalance")

    if total_score_pct < 0.0 or total_score_pct > 100.0:
        raise PreflightError("unified_features.engine_scores.total.score out of range 0..100 (STRICT)")
    if trend_strength < 0.0 or trend_strength > 1.0:
        raise PreflightError("trend_strength out of range 0..1 (STRICT)")
    if spread < 0.0:
        raise PreflightError("spread must be >= 0 (STRICT)")
    if orderbook_imbalance < -1.0 or orderbook_imbalance > 1.0:
        raise PreflightError("orderbook_imbalance out of range -1..1 (STRICT)")
    if entry_score_threshold < 0.0:
        raise PreflightError("settings.entry_score_threshold must be >= 0 (STRICT)")

    return {
        "entry_score": float(total_score_pct) / 100.0,
        "trend_strength": float(trend_strength),
        "spread": float(spread),
        "orderbook_imbalance": float(orderbook_imbalance),
        "entry_score_threshold": float(entry_score_threshold),
    }


def _build_preflight_signal_meta_strict(
    *,
    boot: PreflightBoot,
    pipeline: PipelineSimulationResult,
    meta_l3_status: MetaL3Status,
) -> Dict[str, Any]:
    execution_probe_risk_pct = _finite(pipeline.execution_probe_risk_pct, "pipeline.execution_probe_risk_pct")
    if execution_probe_risk_pct <= 0:
        raise PreflightError("pipeline.execution_probe_risk_pct must be > 0 (STRICT)")

    live_effective_risk_pct = _finite(pipeline.live_effective_risk_pct, "pipeline.live_effective_risk_pct")
    if live_effective_risk_pct < 0 or live_effective_risk_pct > 1:
        raise PreflightError("pipeline.live_effective_risk_pct out of range 0..1 (STRICT)")

    meta: Dict[str, Any] = {
        "symbol": boot.symbol,
        "source": PREFLIGHT_SIGNAL_SOURCE,
        "regime": PREFLIGHT_SIGNAL_SOURCE,
        "signal_source": PREFLIGHT_SIGNAL_SOURCE,
        "signal_ts_ms": int(boot.signal_ts_ms),
        "last_price": float(boot.last_price),
        "candles_5m": boot.candles_5m,
        "candles_5m_raw": boot.candles_5m_raw,
        "extra": {"preflight": True},
        "preflight_stage": PREFLIGHT_STAGE_NAME,
        "preflight_reason_detail": PREFLIGHT_REASON_DETAIL,
        "equity_current_usdt": float(pipeline.eq_cur),
        "equity_peak_usdt": float(pipeline.eq_peak),
        "dd_pct": float(pipeline.dd_pct),
        "dynamic_allocation_ratio": float(execution_probe_risk_pct),
        "dynamic_risk_pct": float(execution_probe_risk_pct),
        "live_dynamic_allocation_ratio": float(live_effective_risk_pct),
        "live_dynamic_risk_pct": float(live_effective_risk_pct),
        "regime_score": float(pipeline.regime_score),
        "regime_band": str(pipeline.regime_band),
        "regime_allocation": float(pipeline.regime_allocation),
        "micro_score_risk": float(pipeline.micro_score_risk),
        "ev_cell_key": f"{pipeline.hk.regime_band}|{pipeline.hk.distortion_bucket}|{pipeline.hk.score_bucket}",
        "ev_cell_status": str(pipeline.ev_cell_status),
        "ev_cell_ev": pipeline.ev_cell_ev,
        "ev_cell_n": int(pipeline.ev_cell_n),
        "meta_l3_ready": bool(meta_l3_status.ready),
        "meta_l3_note": str(meta_l3_status.note),
        "meta_l3_final_risk_multiplier": meta_l3_status.final_risk_multiplier,
        "entry_score": float(pipeline.decision_meta["entry_score"]),
        "trend_strength": float(pipeline.decision_meta["trend_strength"]),
        "spread": float(pipeline.decision_meta["spread"]),
        "orderbook_imbalance": float(pipeline.decision_meta["orderbook_imbalance"]),
        "entry_score_threshold": float(pipeline.decision_meta["entry_score_threshold"]),
    }

    if str(meta.get("source", "")).strip() != PREFLIGHT_SIGNAL_SOURCE:
        raise PreflightError("preflight meta.source normalization failed (STRICT)")
    if str(meta.get("regime", "")).strip() != PREFLIGHT_SIGNAL_SOURCE:
        raise PreflightError("preflight meta.regime normalization failed (STRICT)")
    if str(meta.get("signal_source", "")).strip() != PREFLIGHT_SIGNAL_SOURCE:
        raise PreflightError("preflight meta.signal_source normalization failed (STRICT)")
    if float(meta["dynamic_allocation_ratio"]) <= 0:
        raise PreflightError("preflight meta.dynamic_allocation_ratio must be > 0 (STRICT)")
    if float(meta["dynamic_risk_pct"]) <= 0:
        raise PreflightError("preflight meta.dynamic_risk_pct must be > 0 (STRICT)")

    return meta


def _build_preflight_signal_strict(
    *,
    tp_pct: float,
    sl_pct: float,
    risk_pct: float,
    meta: Dict[str, Any],
) -> Signal:
    if not isinstance(meta, dict) or not meta:
        raise PreflightError("preflight signal meta missing/invalid (STRICT)")

    if str(meta.get("source", "")).strip() != PREFLIGHT_SIGNAL_SOURCE:
        raise PreflightError("preflight signal meta.source invalid (STRICT)")
    if str(meta.get("regime", "")).strip() != PREFLIGHT_SIGNAL_SOURCE:
        raise PreflightError("preflight signal meta.regime invalid (STRICT)")
    if str(meta.get("signal_source", "")).strip() != PREFLIGHT_SIGNAL_SOURCE:
        raise PreflightError("preflight signal meta.signal_source invalid (STRICT)")
    if str(meta.get("preflight_reason_detail", "")).strip() != PREFLIGHT_REASON_DETAIL:
        raise PreflightError("preflight signal detail missing/invalid (STRICT)")
    if str(meta.get("preflight_stage", "")).strip() != PREFLIGHT_STAGE_NAME:
        raise PreflightError("preflight signal stage missing/invalid (STRICT)")

    risk_pct_f = _finite(risk_pct, "preflight.signal.risk_pct")
    if risk_pct_f <= 0:
        raise PreflightError("preflight signal risk_pct must be > 0 (STRICT)")

    return Signal(
        action="ENTER",
        direction="LONG",
        tp_pct=float(tp_pct),
        sl_pct=float(sl_pct),
        risk_pct=float(risk_pct_f),
        reason=PREFLIGHT_REASON,
        guard_adjustments={},
        meta=meta,
    )


def _stage_pipeline_simulation_strict(boot: PreflightBoot, uf: Dict[str, Any]) -> PipelineSimulationResult:
    total_score = float(_finite(uf["engine_scores"]["total"]["score"], "engine_total_score"))
    micro = uf.get("microstructure")
    if not isinstance(micro, dict) or "micro_score_risk" not in micro:
        raise PreflightError("microstructure.micro_score_risk missing (STRICT)")

    micro_score_risk = float(_finite(micro.get("micro_score_risk"), "micro_score_risk"))
    if micro_score_risk < 0 or micro_score_risk > 100:
        raise PreflightError(f"micro_score_risk out of range (STRICT): {micro_score_risk}")

    regime_engine = RegimeEngine(window_size=200, percentile_min_history=60)
    for _ in range(60):
        regime_engine.update(total_score)
    regime_decision = regime_engine.decide(total_score)

    regime_allocation = float(_finite(regime_decision.allocation, "regime_decision.allocation"))
    if regime_allocation < 0.0 or regime_allocation > 1.0:
        raise PreflightError("regime_decision.allocation out of range 0..1 (STRICT)")

    ev_heatmap = EvHeatmapEngine(window_size=50, min_samples=20)
    hk: HeatmapKey = ev_heatmap.build_key(
        regime_band=str(regime_decision.band),
        micro_score_risk=float(micro_score_risk),
        engine_total_score=float(total_score),
    )
    cell = ev_heatmap.get_cell_status(hk)

    bal = get_balance_detail("USDT")
    if not isinstance(bal, dict) or not bal:
        raise PreflightError("get_balance_detail('USDT') returned invalid (STRICT)")
    if "availableBalance" not in bal or "crossUnPnl" not in bal:
        raise PreflightError("balance detail missing keys (STRICT)")

    if bool(SET.test_dry_run) and float(SET.test_fake_available_usdt) > 0.0:
        eq_cur = float(_finite(SET.test_fake_available_usdt, "settings.test_fake_available_usdt"))
        if eq_cur <= 0:
            raise PreflightError("settings.test_fake_available_usdt must be > 0 in test_dry_run (STRICT)")
    else:
        eq_cur = float(_finite(bal.get("availableBalance"), "balance.availableBalance"))
        if eq_cur <= 0:
            raise PreflightError("equity_current_usdt must be > 0 (STRICT)")

    eq_peak = eq_cur
    with get_session() as session:
        v = session.execute(
            text(
                """
                SELECT MAX(equity_peak_usdt)
                FROM bt_trade_snapshots
                WHERE symbol = :symbol
                  AND equity_peak_usdt IS NOT NULL
                """
            ),
            {"symbol": boot.symbol},
        ).scalar()

    if v is not None:
        peak = float(v)
        if not math.isfinite(peak) or peak <= 0:
            raise PreflightError(f"persisted equity_peak_usdt invalid (STRICT): {peak}")
        eq_peak = float(max(eq_cur, peak))

    dd_pct = (max(0.0, (eq_peak - eq_cur)) / eq_peak) * 100.0
    if not math.isfinite(dd_pct) or dd_pct < 0.0 or dd_pct > 100.0:
        raise PreflightError(f"dd_pct invalid (STRICT): {dd_pct}")

    risk_physics = RiskPhysicsEngine(policy=RiskPhysicsPolicy(max_allocation=1.0))
    tp_pct = float(_finite(SET.tp_pct, "settings.tp_pct"))
    sl_pct = float(_finite(SET.sl_pct, "settings.sl_pct"))
    if tp_pct <= 0 or sl_pct <= 0:
        raise PreflightError("tp/sl must be > 0 (STRICT)")

    rp = risk_physics.decide(
        regime_allocation=float(regime_allocation),
        dd_pct=float(dd_pct),
        consecutive_losses=0,
        tp_pct=float(tp_pct),
        sl_pct=float(sl_pct),
        micro_score_risk=float(micro_score_risk),
        heatmap_status=str(cell.status),
        heatmap_ev=cell.ev,
        heatmap_n=int(cell.n),
    )

    live_effective_risk_pct = float(_finite(rp.effective_risk_pct, "risk_physics.effective_risk_pct"))
    if live_effective_risk_pct < 0.0 or live_effective_risk_pct > 1.0:
        raise PreflightError(f"risk_physics effective_risk_pct out of range (STRICT): {live_effective_risk_pct}")

    execution_probe_risk_pct = float(_finite(SET.allocation_ratio, "settings.allocation_ratio"))
    if execution_probe_risk_pct <= 0.0 or execution_probe_risk_pct > 1.0:
        raise PreflightError("settings.allocation_ratio must be within (0,1] for execution probe (STRICT)")

    if live_effective_risk_pct == 0.0:
        log(
            "[PRE-FLIGHT][PIPELINE_SIMULATION][NO_TRADE] "
            "live_effective_risk_pct=0.0 current market is no-trade. "
            "execution dry-run will validate contract with explicit execution probe risk."
        )

    drift = DriftDetector(DriftDetectorConfig())
    try:
        drift.update_and_check(
            DriftSnapshot(
                symbol=boot.symbol,
                allocation_ratio=float(execution_probe_risk_pct),
                risk_multiplier=float(max(float(rp.auto_risk_multiplier), 1e-6)),
                regime_band=str(regime_decision.band),
                micro_score_risk=float(micro_score_risk),
            )
        )
    except DriftDetectedError as e:
        raise PreflightError(f"drift detected (STRICT): {e}") from e

    try:
        validate_signal_invariants_strict(
            SignalInvariantInputs(
                symbol=boot.symbol,
                direction="LONG",
                risk_pct=float(execution_probe_risk_pct),
                tp_pct=float(tp_pct),
                sl_pct=float(sl_pct),
                dd_pct=float(dd_pct),
                micro_score_risk=float(micro_score_risk),
                final_risk_multiplier=None,
                equity_current_usdt=float(eq_cur),
                equity_peak_usdt=float(eq_peak),
            )
        )
    except InvariantViolation as e:
        raise PreflightError(f"invariant violation (STRICT): {e}") from e

    decision_meta = _extract_preflight_decision_meta_strict(uf)

    return PipelineSimulationResult(
        eq_cur=float(eq_cur),
        eq_peak=float(eq_peak),
        dd_pct=float(dd_pct),
        regime_score=float(regime_decision.score),
        regime_band=str(regime_decision.band),
        regime_allocation=float(regime_allocation),
        micro_score_risk=float(micro_score_risk),
        live_effective_risk_pct=float(live_effective_risk_pct),
        execution_probe_risk_pct=float(execution_probe_risk_pct),
        tp_pct=float(tp_pct),
        sl_pct=float(sl_pct),
        decision_meta=decision_meta,
        hk=hk,
        ev_cell_status=str(cell.status),
        ev_cell_ev=cell.ev,
        ev_cell_n=int(cell.n),
    )


def _stage_gpt_contract_ping_strict() -> None:
    max_tokens = _require_positive_int_from_any(SET.openai_max_tokens, "settings.openai_max_tokens")
    max_latency_sec = float(_finite(SET.openai_max_latency_sec, "settings.openai_max_latency_sec"))

    system_prompt = (
        "Return exactly one valid json object.\n"
        'Schema: {"ok": true}\n'
        "The output must be json.\n"
        "Rules:\n"
        '- key must be exactly "ok"\n'
        "- value must be exactly true\n"
        "- do not include any other keys\n"
        "- do not include markdown or explanation\n"
    )
    payload = {
        "ping": True,
        "request": "json"
    }

    try:
        r = call_chat_json(
            system_prompt=system_prompt,
            user_payload=payload,
            max_tokens=max_tokens,
            temperature=0.0,
            max_latency_sec=max_latency_sec,
        )
    except GptEngineError as e:
        raise PreflightError(f"GPT ping failed (STRICT): {e}") from e

    if not isinstance(r.obj, dict) or not r.obj:
        raise PreflightError("GPT ping returned empty obj (STRICT)")
    if r.obj != {"ok": True}:
        raise PreflightError(f"GPT ping schema mismatch (STRICT): got={r.obj!r}")


def _stage_meta_l3_observability_strict() -> MetaL3Status:
    symbol = str(SET.symbol).replace("-", "").replace("/", "").upper().strip()
    if not symbol:
        raise PreflightError("settings.symbol empty (STRICT)")

    cfg = MetaStrategyConfig(
        stats_cfg=MetaStatsConfig(
            symbol=symbol,
            lookback_trades=int(SET.meta_lookback_trades),
            recent_window=int(SET.meta_recent_window),
            baseline_window=int(SET.meta_baseline_window),
            min_trades_required=int(SET.meta_min_trades_required),
            exclude_test_trades=False,
        ),
        prompt_cfg=MetaPromptConfig(
            max_recent_trades=int(SET.meta_max_recent_trades),
            language=str(SET.meta_language),
            max_output_sentences=int(SET.meta_max_output_sentences),
            prompt_version=str(SET.meta_prompt_version),
        ),
        cooldown_sec=int(SET.meta_cooldown_sec),
    )

    eng = MetaStrategyEngine(cfg)
    try:
        rec = eng.run_once()
    except MetaStatsError as e:
        note = str(e).strip()
        if not note:
            note = "META_L3 not ready (STRICT)"
        log(f"[PRE-FLIGHT][META_L3][NOT_READY] {note}")
        return MetaL3Status(ready=False, note=note, final_risk_multiplier=None)
    except Exception as e:
        raise PreflightError(f"Meta L3 run_once failed (STRICT): {_sanitize_err(e)}") from e

    final_risk_multiplier = float(_finite(rec.final_risk_multiplier, "meta_l3.final_risk_multiplier"))
    if final_risk_multiplier < 0.0 or final_risk_multiplier > 1.0:
        raise PreflightError(f"Meta L3 final_risk_multiplier out of range (STRICT): {final_risk_multiplier}")

    return MetaL3Status(
        ready=True,
        note="READY",
        final_risk_multiplier=float(final_risk_multiplier),
    )


def _build_execution_probe_signal_strict(
    *,
    boot: PreflightBoot,
    pipeline: PipelineSimulationResult,
    meta_l3_status: MetaL3Status,
) -> Signal:
    meta = _build_preflight_signal_meta_strict(
        boot=boot,
        pipeline=pipeline,
        meta_l3_status=meta_l3_status,
    )

    return _build_preflight_signal_strict(
        tp_pct=float(pipeline.tp_pct),
        sl_pct=float(pipeline.sl_pct),
        risk_pct=float(pipeline.execution_probe_risk_pct),
        meta=meta,
    )


def _stage_execution_dry_run_strict(sig: Signal) -> None:
    class _SettingsView:
        __slots__ = ("_base", "_ov")

        def __init__(self, base: Any, ov: Dict[str, Any]):
            object.__setattr__(self, "_base", base)
            object.__setattr__(self, "_ov", ov)

        def __getattr__(self, name: str) -> Any:
            ov = object.__getattribute__(self, "_ov")
            if name in ov:
                return ov[name]
            base = object.__getattribute__(self, "_base")
            return getattr(base, name)

        def __setattr__(self, name: str, value: Any) -> None:
            raise AttributeError("SettingsView is read-only (STRICT)")

    preflight_fake_usdt = _resolve_preflight_fake_available_usdt_strict()

    if not isinstance(sig.meta, dict):
        raise PreflightError("signal.meta must be dict for execution dry-run (STRICT)")
    if _finite(sig.risk_pct, "signal.risk_pct") <= 0:
        raise PreflightError("signal.risk_pct must be > 0 for execution dry-run (STRICT)")
    if _finite(sig.meta.get("dynamic_risk_pct"), "signal.meta.dynamic_risk_pct") <= 0:
        raise PreflightError("signal.meta.dynamic_risk_pct must be > 0 for execution dry-run (STRICT)")
    if _finite(sig.meta.get("dynamic_allocation_ratio"), "signal.meta.dynamic_allocation_ratio") <= 0:
        raise PreflightError("signal.meta.dynamic_allocation_ratio must be > 0 for execution dry-run (STRICT)")

    view = _SettingsView(
        SET,
        {
            "test_dry_run": True,
            "test_bypass_guards": True,
            "test_force_enter": False,
            "test_fake_available_usdt": float(preflight_fake_usdt),
        },
    )

    eng = ExecutionEngine(view)
    req = _normalize_entry_request_strict(sig, eng.settings)
    req = _enforce_or_generate_client_order_id_strict(
        signal=sig,
        req=req,
        settings=eng.settings,
    )

    if req.qty <= 0:
        raise PreflightError("execution sizing invalid (STRICT)")

    if req.entry_price_hint <= 0:
        raise PreflightError("execution entry_price_hint invalid (STRICT)")

    if req.tp_pct <= 0:
        raise PreflightError("execution tp_pct invalid (STRICT)")

    if not req.soft_mode and req.sl_pct <= 0:
        raise PreflightError("execution sl_pct invalid while soft_mode=False (STRICT)")

    if not hasattr(eng.settings, "require_deterministic_client_order_id"):
        raise PreflightError("settings.require_deterministic_client_order_id missing (STRICT)")

    require_deterministic_cid = _require_bool(
        getattr(eng.settings, "require_deterministic_client_order_id"),
        "settings.require_deterministic_client_order_id",
    )

    if require_deterministic_cid and not str(req.entry_client_order_id or "").strip():
        raise PreflightError(
            "deterministic entry_client_order_id missing after dry-run normalization (STRICT)"
        )

    log(
        "[PRE-FLIGHT][EXECUTION_DRY_RUN] validated "
        f"symbol={req.symbol} side={req.side_open} qty={req.qty} "
        f"entry_price_hint={req.entry_price_hint} entry_client_order_id={req.entry_client_order_id}"
    )


def _handoff_to_runtime_or_raise() -> None:
    try:
        runtime_mod = importlib.import_module("core.run_bot_ws")
    except Exception as e:
        raise PreflightError(f"runtime handoff import failed (STRICT): {_sanitize_err(e)}") from e

    runtime_main = getattr(runtime_mod, "main", None)
    if not callable(runtime_main):
        raise PreflightError("core.run_bot_ws.main missing/not-callable (STRICT)")

    log("[PRE-FLIGHT] handoff -> core.run_bot_ws.main()")
    runtime_main()


def run_preflight(*, preflight_only: bool) -> None:
    host = socket.gethostname()
    log(f"[PRE-FLIGHT] start host={host} utc={_utc_now().isoformat()} pid={os.getpid()}")

    results: List[StageResult] = []

    r, _ = _run_stage("SETTINGS", _stage_settings_strict)
    results.append(r)

    r, _ = _run_stage("DB", _stage_db_connectivity_strict)
    results.append(r)

    r, _ = _run_stage("BINANCE_API", _stage_binance_private_api_strict)
    results.append(r)

    r, boot = _run_stage("WS_BOOTSTRAP", _stage_bootstrap_ws_strict)
    results.append(r)
    assert isinstance(boot, PreflightBoot)

    r, uf = _run_stage("UNIFIED_FEATURES", lambda: _stage_unified_features_strict(boot))
    results.append(r)
    assert isinstance(uf, dict)

    r, pipeline = _run_stage("PIPELINE_SIMULATION", lambda: _stage_pipeline_simulation_strict(boot, uf))
    assert isinstance(pipeline, PipelineSimulationResult)
    if float(pipeline.live_effective_risk_pct) == 0.0:
        r = StageResult(
            name=r.name,
            ok=r.ok,
            elapsed_ms=r.elapsed_ms,
            note="NO_TRADE_CURRENT_REGIME",
        )
    results.append(r)

    r, _ = _run_stage("GPT_PING", _stage_gpt_contract_ping_strict)
    results.append(r)

    r, meta_l3_status = _run_stage(META_L3_STAGE_NAME, _stage_meta_l3_observability_strict)
    assert isinstance(meta_l3_status, MetaL3Status)
    if not meta_l3_status.ready:
        r = StageResult(
            name=r.name,
            ok=r.ok,
            elapsed_ms=r.elapsed_ms,
            note=f"NOT_READY: {meta_l3_status.note}",
        )
    results.append(r)

    sig = _build_execution_probe_signal_strict(
        boot=boot,
        pipeline=pipeline,
        meta_l3_status=meta_l3_status,
    )

    r, _ = _run_stage("EXECUTION_DRY_RUN", lambda: _stage_execution_dry_run_strict(sig))
    results.append(r)

    lines = ["✅ PRE-FLIGHT OK", f"Host: {host}", f"Symbol: {boot.symbol}", f"UTC: {_utc_now().isoformat()}"]
    for it in results:
        suffix = f" [{it.note}]" if it.note != "OK" else ""
        lines.append(f"- {it.name}: {it.elapsed_ms:.0f}ms{suffix}")
    msg = "\n".join(lines)
    log(msg)
    _safe_tg_notice(msg)

    if preflight_only:
        log("[PRE-FLIGHT] preflight_only=True -> exit without starting run_bot_ws")
        return

    _handoff_to_runtime_or_raise()


def main() -> None:
    ap = argparse.ArgumentParser(prog="run_bot_preflight", add_help=True)
    ap.add_argument("--preflight-only", action="store_true", help="preflight만 수행하고 run_bot_ws는 시작하지 않음")
    args = ap.parse_args()
    run_preflight(preflight_only=bool(args.preflight_only))


if __name__ == "__main__":
    main()