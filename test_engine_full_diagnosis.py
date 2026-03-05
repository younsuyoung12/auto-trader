#!/usr/bin/env python3
"""
========================================================
FILE: test_engine_full_diagnosis.py
STRICT · NO-FALLBACK · TRADE-GRADE DIAGNOSTIC
========================================================

목적
- 8개 모듈을 한 번에 검사하여 “정확히 어디서” 실패하는지 즉시 확인한다.

검사 항목(8)
1) SETTINGS (SSOT)
2) WS (start + buffers)
3) REST BACKFILL → WS INJECT
4) HEALTH SNAPSHOT (가능한 경우)
5) UNIFIED FEATURES (STRICT)
6) REGIME ENGINE (sanity)
7) RISK PHYSICS (sanity)
8) EXECUTION_DRY_RUN + DB + OPENAI (실전 구성 검증)

STRICT 정책
- 실패 시 즉시 예외 + traceback 출력
- 민감정보(키/시크릿/DB URL)는 출력 금지(길이/프리픽스만)
- 폴백/임의 보정 금지 (설정/데이터 부족은 FAIL로 명시)

주의
- EXECUTION_DRY_RUN 단계는 DB 기록이 발생할 수 있다(프로젝트 정책상).
- 실제 주문은 test_dry_run=True(override)로 차단되는 전제.
========================================================
"""

from __future__ import annotations

import inspect
import os
import platform
import sys
import time
import traceback
from dataclasses import dataclass
from infra.async_worker import start_worker
from typing import Any, Dict, List, Optional, Tuple


# ─────────────────────────────────────────────────────────────
# Pretty print helpers
# ─────────────────────────────────────────────────────────────
def _banner(title: str) -> None:
    print("\n" + "=" * 94)
    print(title)
    print("=" * 94)


def _kv(k: str, v: Any) -> None:
    print(f"{k:28s}: {v}")


def _safe_prefix(s: str, n: int = 7) -> str:
    s = str(s or "")
    if len(s) <= n:
        return s
    return s[:n] + "..."


def _die(msg: str) -> None:
    raise RuntimeError(msg)


@dataclass(frozen=True, slots=True)
class StageResult:
    name: str
    ok: bool
    elapsed_ms: float


def _run_stage(name: str, fn) -> StageResult:
    t0 = time.perf_counter()
    fn()
    dt = (time.perf_counter() - t0) * 1000.0
    return StageResult(name=name, ok=True, elapsed_ms=float(dt))

start_worker(
    num_threads=1,
    max_queue_size=2000,
    thread_name_prefix="test-async"
)
# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────
def main() -> int:
    results: List[StageResult] = []

    # ------------------------------------------------------------------
    # 0) ENV / RUNTIME
    # ------------------------------------------------------------------
    _banner("0) ENV / RUNTIME")
    _kv("python", sys.version.replace("\n", " "))
    _kv("executable", sys.executable)
    _kv("cwd", os.getcwd())
    _kv("platform", platform.platform())

    # ------------------------------------------------------------------
    # 1) SETTINGS LOAD (SSOT)
    # ------------------------------------------------------------------
    _banner("1) SETTINGS LOAD (SSOT)")

    try:
        from settings import load_settings  # type: ignore
    except Exception as e:
        print("[FATAL] settings import failed:", f"{type(e).__name__}: {e}")
        traceback.print_exc()
        return 2

    try:
        s = load_settings()
    except Exception as e:
        print("[FATAL] load_settings() failed:", f"{type(e).__name__}: {e}")
        traceback.print_exc()
        return 3

    # 핵심만 출력(민감정보 금지)
    _kv("symbol", getattr(s, "symbol", None))
    _kv("ws_subscribe_tfs", getattr(s, "ws_subscribe_tfs", None))
    _kv("ws_min_kline_buffer", getattr(s, "ws_min_kline_buffer", None))
    _kv("ws_backfill_limit", getattr(s, "ws_backfill_limit", None))
    _kv("openai_model", getattr(s, "openai_model", None))
    _kv("openai_api_key_len", len(str(getattr(s, "openai_api_key", "") or "")))
    _kv("openai_api_key_prefix", _safe_prefix(getattr(s, "openai_api_key", "")))

    # STRICT sanity for buffer/limit
    try:
        ws_need = int(getattr(s, "ws_min_kline_buffer"))
        ws_lim = int(getattr(s, "ws_backfill_limit"))
    except Exception:
        return 4

    if ws_need <= 0:
        print("[FATAL] ws_min_kline_buffer must be > 0")
        return 5
    if ws_lim <= 0:
        print("[FATAL] ws_backfill_limit must be > 0")
        return 6
    if ws_lim < ws_need:
        print(f"[FATAL] ws_backfill_limit({ws_lim}) < ws_min_kline_buffer({ws_need}) -> cannot satisfy buffers")
        return 7

    results.append(StageResult("SETTINGS", True, 0.0))

    # ------------------------------------------------------------------
    # 2) WS START
    # ------------------------------------------------------------------
    def stage_ws_start() -> None:
        from infra.market_data_ws import start_ws_loop  # type: ignore

        start_ws_loop(str(getattr(s, "symbol")))

    _banner("2) WS START")
    try:
        r = _run_stage("WS_START", stage_ws_start)
        results.append(r)
        _kv("WS_START", f"OK ({r.elapsed_ms:.0f}ms)")
    except Exception as e:
        print("[FATAL] WS_START failed:", f"{type(e).__name__}: {e}")
        traceback.print_exc()
        return 10

    # ------------------------------------------------------------------
    # 3) REST BACKFILL → WS INJECT
    # ------------------------------------------------------------------
    def stage_rest_backfill_inject() -> None:
        symbol = str(getattr(s, "symbol")).replace("-", "").replace("/", "").upper().strip()
        if not symbol:
            _die("settings.symbol empty (STRICT)")

        # settings 기반으로 통일
        limit = int(getattr(s, "ws_backfill_limit"))
        # 최소 200은 regime/ema 요구(프로젝트 기준)
        need = max(200, int(getattr(s, "ws_min_kline_buffer")))

        from infra.market_data_rest import fetch_klines_rest, KlineRestError  # type: ignore
        from infra.market_data_ws import backfill_klines_from_rest, get_klines_with_volume  # type: ignore

        tfs = ("1m", "5m", "15m", "1h", "4h")

        for tf in tfs:
            _banner(f"REST BACKFILL: {symbol} {tf} limit={limit}")
            try:
                rows = fetch_klines_rest(symbol, tf, limit=limit)
            except KlineRestError as e:
                raise RuntimeError(f"REST klines failed (STRICT): {symbol} {tf} {e}") from e

            if not isinstance(rows, list) or not rows:
                _die(f"REST returned empty (STRICT): {symbol} {tf}")

            # WS store inject
            backfill_klines_from_rest(symbol, tf, rows)

            # probe (짧게)
            probe = get_klines_with_volume(symbol, tf, limit=5)
            _kv("ws_probe_len", len(probe) if isinstance(probe, list) else f"type={type(probe).__name__}")
            if isinstance(probe, list) and probe:
                _kv("ws_probe_last_ts", probe[-1][0])

        # readiness wait: 모든 TF가 need 이상이 될 때까지(타임아웃)
        _banner("WAIT WS BUFFERS + ORDERBOOK READY (STRICT)")
        from infra.market_data_ws import get_klines_with_volume, get_orderbook  # type: ignore

        deadline = time.time() + 60.0  # 고정 60s (진단 스크립트)
        last_report = 0.0

        while True:
            ok = True
            lens: Dict[str, int] = {}
            for tf in tfs:
                buf = get_klines_with_volume(symbol, tf, limit=need)
                n = len(buf) if isinstance(buf, list) else 0
                lens[tf] = n
                if n < need:
                    ok = False

            ob = get_orderbook(symbol, limit=5)
            ob_ok = isinstance(ob, dict) and bool(ob.get("bids")) and bool(ob.get("asks"))

            now = time.time()
            if now - last_report >= 1.0:
                last_report = now
                _kv(
                    "[WAIT] buffers",
                    f"(min={need}) "
                    f"1m={lens['1m']} 5m={lens['5m']} 15m={lens['15m']} 1h={lens['1h']} 4h={lens['4h']} | ob_ok={ob_ok}",
                )

            if ok and ob_ok:
                break

            if now >= deadline:
                _die(f"WS buffers not ready within deadline (STRICT): need={need}, lens={lens}, ob_ok={ob_ok}")

            time.sleep(0.5)

    _banner("3) REST BACKFILL → WS INJECT")
    try:
        r = _run_stage("REST_BACKFILL_INJECT", stage_rest_backfill_inject)
        results.append(r)
        _kv("REST_BACKFILL_INJECT", f"OK ({r.elapsed_ms:.0f}ms)")
    except Exception as e:
        print("[FATAL] REST_BACKFILL_INJECT failed:", f"{type(e).__name__}: {e}")
        traceback.print_exc()
        return 20

    # ------------------------------------------------------------------
    # 4) HEALTH SNAPSHOT (if available)
    # ------------------------------------------------------------------
    def stage_health_snapshot() -> None:
        symbol = str(getattr(s, "symbol")).replace("-", "").replace("/", "").upper().strip()
        try:
            from infra.market_data_ws import get_health_snapshot  # type: ignore
        except Exception:
            print("[SKIP] infra.market_data_ws.get_health_snapshot not available in this build")
            return

        hs = get_health_snapshot(symbol)
        if not isinstance(hs, dict) or not hs:
            _die("health_snapshot returned empty/non-dict (STRICT)")

        _kv("overall_ok", hs.get("overall_ok"))
        _kv("reasons", hs.get("reasons"))

        if hs.get("overall_ok") is not True:
            _die(f"health_snapshot overall_ok=False (STRICT): {hs.get('reasons')}")

    _banner("4) HEALTH SNAPSHOT (if available)")
    try:
        r = _run_stage("HEALTH_SNAPSHOT", stage_health_snapshot)
        results.append(r)
        _kv("HEALTH_SNAPSHOT", f"OK ({r.elapsed_ms:.0f}ms)")
    except Exception as e:
        print("[FATAL] HEALTH_SNAPSHOT failed:", f"{type(e).__name__}: {e}")
        traceback.print_exc()
        return 30

    # ------------------------------------------------------------------
    # 5) UNIFIED FEATURES BUILD (STRICT)
    # ------------------------------------------------------------------
    features: Dict[str, Any] = {}

    def stage_unified_features() -> None:
        nonlocal features
        from strategy.unified_features_builder import build_unified_features  # type: ignore

        sym = str(getattr(s, "symbol")).replace("-", "").replace("/", "").upper().strip()
        features = build_unified_features(sym)
        if not isinstance(features, dict) or not features:
            _die("build_unified_features returned empty/non-dict (STRICT)")

        _kv("features keys", list(features.keys()))
        eng = features.get("engine_scores")
        if not isinstance(eng, dict):
            _die("features.engine_scores missing/invalid (STRICT)")
        total = eng.get("total")
        if not isinstance(total, dict) or "score" not in total:
            _die("features.engine_scores.total.score missing (STRICT)")
        _kv("engine_total_score", total["score"])

    _banner("5) UNIFIED FEATURES BUILD (STRICT)")
    try:
        r = _run_stage("UNIFIED_FEATURES", stage_unified_features)
        results.append(r)
        _kv("UNIFIED_FEATURES", f"OK ({r.elapsed_ms:.0f}ms)")
    except Exception as e:
        print("[FATAL] UNIFIED_FEATURES failed:", f"{type(e).__name__}: {e}")
        traceback.print_exc()
        return 40

    # ------------------------------------------------------------------
    # 6) REGIME ENGINE SANITY
    # ------------------------------------------------------------------
    regime_band: str = ""
    regime_alloc: float = 0.0
    engine_score: float = 0.0

    def stage_regime() -> None:
        nonlocal regime_band, regime_alloc, engine_score

        eng = features["engine_scores"]
        total = eng["total"]
        engine_score = float(total["score"])

        from strategy.regime_engine import RegimeEngine  # type: ignore

        re = RegimeEngine(window_size=200, percentile_min_history=60)
        # preflight와 동일: history 최소 확보
        for _ in range(60):
            re.update(engine_score)
        dec = re.decide(engine_score)

        regime_band = str(dec.band)
        regime_alloc = float(dec.allocation)
        _kv("regime_band", regime_band)
        _kv("regime_allocation", regime_alloc)

        if not (0.0 <= regime_alloc <= 1.0):
            _die("regime_allocation out of range (STRICT)")

    _banner("6) REGIME ENGINE SANITY")
    try:
        r = _run_stage("REGIME", stage_regime)
        results.append(r)
        _kv("REGIME", f"OK ({r.elapsed_ms:.0f}ms)")
    except Exception as e:
        print("[FATAL] REGIME failed:", f"{type(e).__name__}: {e}")
        traceback.print_exc()
        return 50

    # ------------------------------------------------------------------
    # 7) RISK PHYSICS SANITY (heatmap_status는 엔진 방식으로 생성)
    # ------------------------------------------------------------------
    def stage_risk_physics() -> None:
        from execution.risk_physics_engine import RiskPhysicsEngine, RiskPhysicsPolicy  # type: ignore
        from strategy.ev_heatmap_engine import EvHeatmapEngine  # type: ignore

        micro = features.get("microstructure")
        if not isinstance(micro, dict) or "micro_score_risk" not in micro:
            _die("features.microstructure.micro_score_risk missing (STRICT)")
        micro_score_risk = float(micro["micro_score_risk"])

        ev = EvHeatmapEngine(window_size=50, min_samples=20)
        hk = ev.build_key(
            regime_band=str(regime_band),
            micro_score_risk=float(micro_score_risk),
            engine_total_score=float(engine_score),
        )
        cell = ev.get_cell_status(hk)

        heatmap_status = str(cell.status)
        heatmap_ev = cell.ev
        heatmap_n = int(cell.n)

        _kv("heatmap_status", heatmap_status)
        _kv("heatmap_ev", heatmap_ev)
        _kv("heatmap_n", heatmap_n)

        rp = RiskPhysicsEngine(policy=RiskPhysicsPolicy(max_allocation=1.0)).decide(
            regime_allocation=float(regime_alloc),
            dd_pct=0.0,
            consecutive_losses=0,
            tp_pct=float(getattr(s, "tp_pct")),
            sl_pct=float(getattr(s, "sl_pct")),
            micro_score_risk=float(micro_score_risk),
            heatmap_status=heatmap_status,
            heatmap_ev=heatmap_ev,
            heatmap_n=heatmap_n,
        )

        _kv("risk_action_override", getattr(rp, "action_override", None))
        _kv("effective_risk_pct", getattr(rp, "effective_risk_pct", None))
        _kv("auto_risk_multiplier", getattr(rp, "auto_risk_multiplier", None))

    _banner("7) RISK PHYSICS SANITY")
    try:
        r = _run_stage("RISK_PHYSICS", stage_risk_physics)
        results.append(r)
        _kv("RISK_PHYSICS", f"OK ({r.elapsed_ms:.0f}ms)")
    except Exception as e:
        print("[FATAL] RISK_PHYSICS failed:", f"{type(e).__name__}: {e}")
        traceback.print_exc()
        return 60

    # ------------------------------------------------------------------
    # 8) EXECUTION_DRY_RUN + DB + OPENAI
    # ------------------------------------------------------------------
    def stage_db_check() -> None:
        from sqlalchemy import text  # type: ignore
        from state.db_core import get_session  # type: ignore

        with get_session() as session:
            v = session.execute(text("SELECT 1")).scalar()
            if v != 1:
                _die("DB SELECT 1 failed (STRICT)")

            # 핵심 테이블 존재 체크
            for tname in ("bt_trades", "bt_trade_snapshots", "bt_trade_exit_snapshots"):
                reg = session.execute(text("SELECT to_regclass(:tname)"), {"tname": tname}).scalar()
                if reg is None:
                    _die(f"DB table missing (STRICT): {tname}")

        _kv("db_select_1", "OK")
        _kv("db_tables", "OK")

    def stage_execution_dry_run() -> None:
        # ExecutionEngine은 settings.test_dry_run=True에서 주문 대신 DB 기록만 수행하는 전제.
        from execution.execution_engine import ExecutionEngine  # type: ignore
        from strategy.signal import Signal  # type: ignore
        from infra.market_data_ws import get_klines_with_volume  # type: ignore

        sym = str(getattr(s, "symbol")).replace("-", "").replace("/", "").upper().strip()
        k1 = get_klines_with_volume(sym, "1m", limit=1)
        if not isinstance(k1, list) or not k1:
            _die("WS 1m buffer missing for execution dry run (STRICT)")
        signal_ts_ms = int(k1[-1][0])
        last_price = float(k1[-1][4])

        c5 = get_klines_with_volume(sym, "5m", limit=60)
        c5r = get_klines_with_volume(sym, "5m", limit=60)

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
            s,
            {
                "test_dry_run": True,
                "test_bypass_guards": True,
                "test_force_enter": False,
                "test_fake_available_usdt": float(getattr(s, "test_fake_available_usdt", 1000.0) or 1000.0),
            },
        )

        sig = Signal(
            action="ENTER",
            direction="LONG",
            tp_pct=float(getattr(s, "tp_pct")),
            sl_pct=float(getattr(s, "sl_pct")),
            risk_pct=0.01,
            reason="DIAG_EXEC_DRY_RUN",
            guard_adjustments={},
            meta={
                "symbol": sym,
                "regime": "DIAG",
                "signal_source": "DIAG",
                "signal_ts_ms": int(signal_ts_ms),
                "last_price": float(last_price),
                "candles_5m": c5,
                "candles_5m_raw": c5r,
                "extra": {"diagnostic": True},
                "equity_current_usdt": float(getattr(view, "test_fake_available_usdt")),
                "equity_peak_usdt": float(getattr(view, "test_fake_available_usdt")),
                "dd_pct": 0.0,
                "dynamic_allocation_ratio": 0.01,
                "dynamic_risk_pct": 0.01,
            },
        )

        eng = ExecutionEngine(view)
        out = eng.execute(sig)
        # 프로젝트 정책: dry-run은 Trade를 반환하지 않는 설계가 많음. 반환값이 있으면 표시만.
        _kv("execution_return", type(out).__name__ if out is not None else "None")

    def stage_openai_ping() -> None:
        import openai  # type: ignore
        from openai import OpenAI  # type: ignore

        _kv("openai.__version__", getattr(openai, "__version__", "(missing)"))
        _kv("openai.__file__", getattr(openai, "__file__", "(missing)"))
        try:
            _kv("OpenAI source file", inspect.getsourcefile(OpenAI))
        except Exception:
            _kv("OpenAI source file", "(unavailable)")

        api_key = str(getattr(s, "openai_api_key", "") or "").strip()
        model = str(getattr(s, "openai_model", "") or "").strip()
        if not api_key or not model:
            _die("OpenAI settings missing (STRICT)")

        budget = float(getattr(s, "openai_max_latency_sec", 12.0) or 12.0)
        client = OpenAI(api_key=api_key).with_options(timeout=float(budget))

        resp = client.responses.create(
            model=model,
            instructions="Reply with the single word: pong",
            input="ping",
            max_output_tokens=16,
            store=True,
        )

        out_text = getattr(resp, "output_text", None)

        # fallback parsing (SDK 2.x)
        if not out_text:
            try:
                out_text = resp.output[0].content[0].text
            except Exception:
                out_text = None

        if not isinstance(out_text, str) or not out_text.strip():
            raise RuntimeError("OpenAI ping returned empty response (STRICT)")

        _kv("openai_ping", out_text.strip())

    _banner("8) EXECUTION_DRY_RUN + DB + OPENAI")

    try:
        r = _run_stage("DB_CHECK", stage_db_check)
        results.append(r)
        _kv("DB_CHECK", f"OK ({r.elapsed_ms:.0f}ms)")
    except Exception as e:
        print("[FATAL] DB_CHECK failed:", f"{type(e).__name__}: {e}")
        traceback.print_exc()
        return 70

    try:
        r = _run_stage("EXECUTION_DRY_RUN", stage_execution_dry_run)
        results.append(r)
        _kv("EXECUTION_DRY_RUN", f"OK ({r.elapsed_ms:.0f}ms)")
    except Exception as e:
        print("[FATAL] EXECUTION_DRY_RUN failed:", f"{type(e).__name__}: {e}")
        traceback.print_exc()
        return 71

    try:
        r = _run_stage("OPENAI_PING", stage_openai_ping)
        results.append(r)
        _kv("OPENAI_PING", f"OK ({r.elapsed_ms:.0f}ms)")
    except Exception as e:
        print("[FATAL] OPENAI_PING failed:", f"{type(e).__name__}: {e}")
        traceback.print_exc()
        return 72

    # ------------------------------------------------------------------
    # DONE
    # ------------------------------------------------------------------
    _banner("DONE (SUMMARY)")
    for it in results:
        _kv(it.name, f"OK {it.elapsed_ms:.0f}ms")

    print("\nOK: FULL DIAG PASSED")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.")
        raise