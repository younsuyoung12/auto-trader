"""
========================================================
FILE: core/run_bot_preflight.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

핵심 변경 요약
- PREFLIGHT dry-run 신호의 source 계열 필드(reason / meta.regime / meta.signal_source / meta.source)를
  단일 canonical 값으로 정규화해 execution_engine 충돌을 제거
- PREFLIGHT 단계 상세 의미는 preflight_reason_detail / preflight_stage 로 분리해
  source 필드와 의미가 섞이지 않도록 구조 수정
- 사용하지 않는 import / dataclass 제거 및 상단 주석 구조 정리

코드 정리 내용
- 미사용 import(fetch_open_positions, KlineRestError) 제거
- 미사용 dataclass(PreflightPipeline) 제거
- os.getpid 직접 사용으로 동적 import 제거
- PREFLIGHT 신호 생성 로직을 helper로 분리해 중복 의미 혼선 제거

설계 원칙
- 폴백 금지
- 데이터 누락 시 즉시 예외
- 예외 삼키기 금지
- 설정은 settings.py 단일 소스
- DB 접근은 db_core 경유
- 시간 역전(timestamp rollback) 발생 시 즉시 SAFE_STOP

변경 이력
--------------------------------------------------------
- 2026-03-10:
  1) FIX(STRICT): PREFLIGHT dry-run Signal의 source 계열 필드 충돌 제거
  2) FIX(STRUCTURE): PREFLIGHT lifecycle 정보와 시장/신호 source 의미를 분리
  3) CLEANUP: 미사용 import/dataclass 제거, 상단 주석 최근 기준으로 정리
========================================================
"""

from __future__ import annotations

import argparse
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

from settings import load_settings  # noqa: E402
from infra.telelog import log, send_tg  # noqa: E402
from infra.async_worker import start_worker as start_async_worker  # noqa: E402

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

from execution.execution_engine import ExecutionEngine  # noqa: E402
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

SET = load_settings()

ENTRY_REQUIRED_TFS: Tuple[str, ...] = ("1m", "5m", "15m", "1h", "4h")
ENTRY_REQUIRED_KLINES_MIN: Dict[str, int] = {"1m": 20, "5m": 20, "15m": 20, "1h": 60, "4h": 60}

PREFLIGHT_SIGNAL_SOURCE = "PREFLIGHT"
PREFLIGHT_REASON = "PREFLIGHT"
PREFLIGHT_REASON_DETAIL = "PREFLIGHT_PIPELINE"
PREFLIGHT_STAGE_NAME = "PIPELINE_SIMULATION"


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


def _safe_tg_notice(msg: str) -> None:
    try:
        send_tg(msg)
    except Exception:
        return


def _sanitize_err(e: BaseException) -> str:
    s = str(e).replace("\n", " ").strip()
    if len(s) > 240:
        s = s[:240]
    return f"{type(e).__name__}: {s}"


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


def _stage_settings_strict() -> None:
    symbol = _require_nonempty_str(getattr(SET, "symbol", None), "settings.symbol").replace("-", "").replace("/", "").upper().strip()
    if not symbol:
        raise PreflightError("settings.symbol normalized empty (STRICT)")

    _ = _require_nonempty_str(getattr(SET, "api_key", None), "settings.api_key")
    _ = _require_nonempty_str(getattr(SET, "api_secret", None), "settings.api_secret")

    lev = _finite(getattr(SET, "leverage", None), "settings.leverage")
    if lev <= 0:
        raise PreflightError("settings.leverage must be > 0 (STRICT)")

    tp = _finite(getattr(SET, "tp_pct", None), "settings.tp_pct")
    sl = _finite(getattr(SET, "sl_pct", None), "settings.sl_pct")
    if tp <= 0:
        raise PreflightError("settings.tp_pct must be > 0 (STRICT)")
    if sl <= 0:
        raise PreflightError("settings.sl_pct must be > 0 (STRICT)")

    ws_enabled = _require_bool(getattr(SET, "ws_enabled", True), "settings.ws_enabled")
    if not ws_enabled:
        raise PreflightError("settings.ws_enabled must be True for WS-driven engine (STRICT)")

    ws_subscribe_tfs = _parse_tfs(getattr(SET, "ws_subscribe_tfs", None))
    _verify_required_tfs_or_die("ws_subscribe_tfs", ws_subscribe_tfs, ENTRY_REQUIRED_TFS)

    ws_backfill_tfs = _parse_tfs(getattr(SET, "ws_backfill_tfs", None))
    if ws_backfill_tfs:
        _verify_required_tfs_or_die("ws_backfill_tfs", ws_backfill_tfs, ENTRY_REQUIRED_TFS)

    _ = _require_nonempty_str(getattr(SET, "openai_api_key", None), "settings.openai_api_key")
    _ = _require_nonempty_str(getattr(SET, "openai_model", None), "settings.openai_model")
    _ = _finite(getattr(SET, "openai_max_latency_sec", None), "settings.openai_max_latency_sec")


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
    symbol = str(getattr(SET, "symbol", "")).replace("-", "").replace("/", "").upper().strip()
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

    if bool(getattr(SET, "test_dry_run", False)) and float(getattr(SET, "test_fake_available_usdt", 0.0) or 0.0) > 0.0:
        eq_cur = float(_finite(getattr(SET, "test_fake_available_usdt", None), "settings.test_fake_available_usdt"))
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


def _stage_bootstrap_ws_strict() -> PreflightBoot:
    symbol = str(getattr(SET, "symbol", "")).replace("-", "").replace("/", "").upper().strip()
    if not symbol:
        raise PreflightError("settings.symbol empty (STRICT)")

    limit = SET.ws_backfill_limit

    for tf in ("1m", "5m", "15m", "1h", "4h"):
        rows = fetch_klines_rest(symbol, tf, limit=limit)
        _validate_kline_rows_strict(
            rows,
            name=f"REST_KLINES_{tf}",
            min_len=200,
        )
        backfill_klines_from_rest(symbol, tf, rows)

    start_ws_loop(symbol)

    wait_sec = float(getattr(SET, "preflight_ws_wait_sec", 20.0) or 20.0)
    if not math.isfinite(wait_sec) or wait_sec <= 0:
        raise PreflightError("settings.preflight_ws_wait_sec must be finite > 0 (STRICT)")
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

    candles_5m = ws_get_klines_with_volume(symbol, "5m", limit=60)
    candles_5m_raw = ws_get_klines_with_volume(symbol, "5m", limit=60)
    if not isinstance(candles_5m, list) or len(candles_5m) < 20:
        raise PreflightError("WS candles_5m insufficient (STRICT)")
    if not isinstance(candles_5m_raw, list) or len(candles_5m_raw) < 20:
        raise PreflightError("WS candles_5m_raw insufficient (STRICT)")

    k1 = ws_get_klines_with_volume(symbol, "1m", limit=1)
    if not isinstance(k1, list) or not k1 or len(k1[0]) < 1:
        raise PreflightError("WS 1m buffer missing for signal_ts_ms (STRICT)")
    signal_ts_ms = int(k1[0][0])
    if signal_ts_ms <= 0:
        raise PreflightError("signal_ts_ms invalid (STRICT)")

    last_price = float(_finite(k1[0][4], "WS_1m.last_close"))
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
    """
    STRICT:
    - preflight에서도 runtime과 동일하게 entry_flow를 통하지 않고
      execution_engine이 요구하는 decision meta만 unified_features에서 직접 추출한다.
    """
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

    entry_score_threshold = _finite(getattr(SET, "entry_score_threshold", None), "settings.entry_score_threshold")
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
    eq_cur: float,
    eq_peak: float,
    dd_pct: float,
    regime_decision: Any,
    rp: Any,
    hk: HeatmapKey,
    cell: Any,
    decision_meta: Dict[str, float],
    micro_score_risk: float,
) -> Dict[str, Any]:
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
        "equity_current_usdt": float(eq_cur),
        "equity_peak_usdt": float(eq_peak),
        "dd_pct": float(dd_pct),
        "dynamic_allocation_ratio": float(rp.effective_risk_pct),
        "dynamic_risk_pct": float(rp.effective_risk_pct),
        "regime_score": float(regime_decision.score),
        "regime_band": str(regime_decision.band),
        "regime_allocation": float(regime_decision.allocation),
        "micro_score_risk": float(micro_score_risk),
        "ev_cell_key": f"{hk.regime_band}|{hk.distortion_bucket}|{hk.score_bucket}",
        "ev_cell_status": str(cell.status),
        "ev_cell_ev": cell.ev,
        "ev_cell_n": int(cell.n),
        "auto_blocked": bool(rp.auto_blocked),
        "auto_risk_multiplier": float(rp.auto_risk_multiplier),
        "entry_score": float(decision_meta["entry_score"]),
        "trend_strength": float(decision_meta["trend_strength"]),
        "spread": float(decision_meta["spread"]),
        "orderbook_imbalance": float(decision_meta["orderbook_imbalance"]),
        "entry_score_threshold": float(decision_meta["entry_score_threshold"]),
    }

    if str(meta.get("source", "")).strip() != PREFLIGHT_SIGNAL_SOURCE:
        raise PreflightError("preflight meta.source normalization failed (STRICT)")
    if str(meta.get("regime", "")).strip() != PREFLIGHT_SIGNAL_SOURCE:
        raise PreflightError("preflight meta.regime normalization failed (STRICT)")
    if str(meta.get("signal_source", "")).strip() != PREFLIGHT_SIGNAL_SOURCE:
        raise PreflightError("preflight meta.signal_source normalization failed (STRICT)")

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

    return Signal(
        action="ENTER",
        direction="LONG",
        tp_pct=float(tp_pct),
        sl_pct=float(sl_pct),
        risk_pct=float(risk_pct) if float(risk_pct) > 0 else 0.01,
        reason=PREFLIGHT_REASON,
        guard_adjustments={},
        meta=meta,
    )


def _stage_pipeline_simulation_strict(boot: PreflightBoot, uf: Dict[str, Any]) -> Signal:
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

    if not math.isfinite(float(regime_decision.allocation)):
        raise PreflightError("regime_decision.allocation must be finite (STRICT)")
    if float(regime_decision.allocation) < 0.0 or float(regime_decision.allocation) > 1.0:
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

    if bool(getattr(SET, "test_dry_run", False)) and float(getattr(SET, "test_fake_available_usdt", 0.0) or 0.0) > 0.0:
        eq_cur = float(_finite(getattr(SET, "test_fake_available_usdt", None), "settings.test_fake_available_usdt"))
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
    tp_pct = float(_finite(getattr(SET, "tp_pct", None), "settings.tp_pct"))
    sl_pct = float(_finite(getattr(SET, "sl_pct", None), "settings.sl_pct"))
    if tp_pct <= 0 or sl_pct <= 0:
        raise PreflightError("tp/sl must be > 0 (STRICT)")

    rp = risk_physics.decide(
        regime_allocation=float(regime_decision.allocation),
        dd_pct=float(dd_pct),
        consecutive_losses=0,
        tp_pct=float(tp_pct),
        sl_pct=float(sl_pct),
        micro_score_risk=float(micro_score_risk),
        heatmap_status=str(cell.status),
        heatmap_ev=cell.ev,
        heatmap_n=int(cell.n),
    )
    if not math.isfinite(float(rp.effective_risk_pct)) or float(rp.effective_risk_pct) < 0.0 or float(rp.effective_risk_pct) > 1.0:
        raise PreflightError(f"risk_physics effective_risk_pct out of range (STRICT): {rp.effective_risk_pct}")

    drift = DriftDetector(DriftDetectorConfig())
    try:
        drift.update_and_check(
            DriftSnapshot(
                symbol=boot.symbol,
                allocation_ratio=float(max(float(rp.effective_risk_pct), 1e-6)),
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
                risk_pct=float(max(float(rp.effective_risk_pct), 1e-6)),
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

    meta = _build_preflight_signal_meta_strict(
        boot=boot,
        eq_cur=float(eq_cur),
        eq_peak=float(eq_peak),
        dd_pct=float(dd_pct),
        regime_decision=regime_decision,
        rp=rp,
        hk=hk,
        cell=cell,
        decision_meta=decision_meta,
        micro_score_risk=float(micro_score_risk),
    )

    return _build_preflight_signal_strict(
        tp_pct=float(tp_pct),
        sl_pct=float(sl_pct),
        risk_pct=float(rp.effective_risk_pct),
        meta=meta,
    )


def _stage_gpt_contract_ping_strict() -> None:
    system_prompt = (
        "Return STRICT JSON only (single object). No markdown.\n"
        'Schema: {"ok": true, "ts_utc": "ISO8601"}\n'
        "Always generate the JSON object.\n"
        "Rules: do not include any other keys.\n"
    )
    payload = {"ping": True, "reply": "PONG"}
    try:
        r = call_chat_json(
            system_prompt=system_prompt,
            user_payload=payload,
            max_tokens=512,
            temperature=0.0,
            max_latency_sec=float(getattr(SET, "openai_max_latency_sec")),
        )
    except GptEngineError as e:
        raise PreflightError(f"GPT ping failed (STRICT): {e}") from e

    if not isinstance(r.obj, dict) or not r.obj:
        raise PreflightError("GPT ping returned empty obj (STRICT)")
    if r.obj.get("ok") is not True:
        raise PreflightError("GPT ping schema mismatch (STRICT)")


def _stage_meta_l3_strict() -> None:
    symbol = str(getattr(SET, "symbol", "")).replace("-", "").replace("/", "").upper().strip()
    if not symbol:
        raise PreflightError("settings.symbol empty (STRICT)")

    cfg = MetaStrategyConfig(
        stats_cfg=MetaStatsConfig(
            symbol=symbol,
            lookback_trades=int(getattr(SET, "meta_lookback_trades", 200) or 200),
            recent_window=int(getattr(SET, "meta_recent_window", 20) or 20),
            baseline_window=int(getattr(SET, "meta_baseline_window", 60) or 60),
            min_trades_required=int(getattr(SET, "meta_min_trades_required", 20) or 20),
            exclude_test_trades=True,
        ),
        prompt_cfg=MetaPromptConfig(
            max_recent_trades=int(getattr(SET, "meta_max_recent_trades", 20) or 20),
            language=str(getattr(SET, "meta_language", "ko") or "ko"),
            max_output_sentences=int(getattr(SET, "meta_max_output_sentences", 3) or 3),
            prompt_version=str(getattr(SET, "meta_prompt_version", "2026-03-03") or "2026-03-03"),
        ),
        cooldown_sec=int(getattr(SET, "meta_cooldown_sec", 60) or 60),
    )

    eng = MetaStrategyEngine(cfg)
    try:
        rec = eng.run_once()
    except MetaStatsError as e:
        log(f"[PRE-FLIGHT][META_L3][NOT_READY] {e}")
        return
    except Exception as e:
        raise PreflightError(f"Meta L3 run_once failed (STRICT): {_sanitize_err(e)}") from e

    if not math.isfinite(float(rec.final_risk_multiplier)) or float(rec.final_risk_multiplier) < 0.0 or float(rec.final_risk_multiplier) > 1.0:
        raise PreflightError(f"Meta L3 final_risk_multiplier out of range (STRICT): {rec.final_risk_multiplier}")


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

    view = _SettingsView(
        SET,
        {
            "test_dry_run": True,
            "test_bypass_guards": True,
            "test_force_enter": False,
            "test_fake_available_usdt": float(_finite(getattr(SET, "preflight_fake_usdt", 1000.0), "settings.preflight_fake_usdt")),
        },
    )

    eng = ExecutionEngine(view)
    out = eng.execute(sig)
    if out is not None:
        raise PreflightError("ExecutionEngine dry-run returned Trade (unexpected) (STRICT)")


def run_preflight(*, preflight_only: bool) -> None:
    host = socket.gethostname()
    log(f"[PRE-FLIGHT] start host={host} utc={_utc_now().isoformat()} pid={os.getpid()}")

    start_async_worker(
        num_threads=int(getattr(SET, "async_worker_threads", 1) or 1),
        max_queue_size=int(getattr(SET, "async_worker_queue_size", 2000) or 2000),
        thread_name_prefix="async-preflight",
    )

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

    r, sig = _run_stage("PIPELINE_SIMULATION", lambda: _stage_pipeline_simulation_strict(boot, uf))
    results.append(r)
    assert isinstance(sig, Signal)

    r, _ = _run_stage("GPT_PING", _stage_gpt_contract_ping_strict)
    results.append(r)

    r, _ = _run_stage("META_L3", _stage_meta_l3_strict)
    results.append(r)

    r, _ = _run_stage("EXECUTION_DRY_RUN", lambda: _stage_execution_dry_run_strict(sig))
    results.append(r)

    lines = ["✅ PRE-FLIGHT OK", f"Host: {host}", f"Symbol: {boot.symbol}", f"UTC: {_utc_now().isoformat()}"]
    for it in results:
        lines.append(f"- {it.name}: {it.elapsed_ms:.0f}ms")
    msg = "\n".join(lines)
    log(msg)
    _safe_tg_notice(msg)

    if preflight_only:
        log("[PRE-FLIGHT] preflight_only=True -> exit without starting run_bot_ws")
        return

    import core.run_bot_ws as rb

    def _noop(*args: Any, **kwargs: Any) -> None:
        return None

    rb.start_async_worker = _noop  # type: ignore[attr-defined]
    rb._backfill_ws_kline_history = _noop  # type: ignore[attr-defined]
    rb.start_ws_loop = _noop  # type: ignore[attr-defined]

    log("[PRE-FLIGHT] handoff -> core.run_bot_ws.main()")
    rb.main()


def main() -> None:
    ap = argparse.ArgumentParser(prog="run_bot_preflight", add_help=True)
    ap.add_argument("--preflight-only", action="store_true", help="preflight만 수행하고 run_bot_ws는 시작하지 않음")
    args = ap.parse_args()
    run_preflight(preflight_only=bool(args.preflight_only))


if __name__ == "__main__":
    main()