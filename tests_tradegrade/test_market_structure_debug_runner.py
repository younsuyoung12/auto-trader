"""
========================================================
FILE: tests_tradegrade/test_market_structure_debug_runner.py
AUTO-TRADER — MARKET STRUCTURE DEBUG RUNNER
STRICT · NO-FALLBACK · TRADE-GRADE TEST MODE
========================================================

역할:
- 이번에 추가/수정한 시장구조 엔진 작업을 단계별로 검증한다.
- 정적 소스 검사, import 검사, DB 스키마 검사, WS live bootstrap 검사,
  unified_features live 검사, entry_pipeline 연동 검사, market_researcher live 검사를 수행한다.
- 실패 시 정확히 어느 stage / 어느 함수 / 어떤 예외인지 JSON 리포트로 출력한다.
- 운영 경로(run_bot_ws -> entry_pipeline -> unified_features_builder) 기준으로 점검한다.

절대 원칙:
- STRICT · NO-FALLBACK
- 테스트 실패를 조용히 삼키지 않는다.
- stage별 예외/traceback을 그대로 기록한다.
- DB/네트워크 live 테스트와 GPT 비용성 테스트를 분리한다.
- GPT/OpenAI 호출은 기본 비활성화한다. (RUN_GPT_TESTS=1 일 때만 수행)
- live WS stage는 반드시 "현재 테스트 프로세스 내부"에서 버퍼를 준비한다.
  다른 프로세스(run_bot_ws)의 메모리 버퍼를 공유한다고 가정하지 않는다.

변경 이력
--------------------------------------------------------
- 2026-03-08:
  1) 신규 생성
  2) 대상 점검:
     - analysis/volume_profile.py
     - analysis/orderflow_cvd.py
     - analysis/options_market_fetcher.py
     - analysis/market_researcher.py
     - analysis/quant_analyst.py
     - strategy/unified_features_builder.py
     - strategy/score_engine.py
     - core/entry_pipeline.py
     - state/db_models.py
  3) stage별 성공/실패 JSON 리포트 출력 추가
  4) DB 실제 컬럼 vs ORM 컬럼 정합 검사 추가
  5) live unified_features / entry_pipeline / market_researcher 검사 추가
  6) GPT 분석 호출은 기본 skip, 환경변수 RUN_GPT_TESTS=1 일 때만 수행

- 2026-03-08 (PATCH):
  1) FIX(TRADE-GRADE): WS live 테스트용 in-process bootstrap 추가
     - Binance public REST klines preload
     - market_data_ws.start_ws_loop() in-process 실행
     - orderbook warmup / health wait
  2) cross-process 메모리 버퍼 공유 가정 제거
  3) unified_features_live / entry_pipeline_live 전 ws_live_bootstrap stage 추가
  4) 1h/4h buffer_len<60 문제를 테스트 러너 자체에서 재현 가능하게 수정
========================================================
"""

from __future__ import annotations

import ast
import importlib
import json
import os
import sys
import threading
import time
import traceback
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TARGET_FILES: List[str] = [
    "analysis/volume_profile.py",
    "analysis/orderflow_cvd.py",
    "analysis/options_market_fetcher.py",
    "analysis/market_researcher.py",
    "analysis/quant_analyst.py",
    "strategy/unified_features_builder.py",
    "strategy/score_engine.py",
    "core/entry_pipeline.py",
    "state/db_models.py",
]

RUN_GPT_TESTS: bool = os.environ.get("RUN_GPT_TESTS", "").strip().lower() in {"1", "true", "yes", "y", "on"}

BINANCE_FAPI_KLINES_URL: str = "https://fapi.binance.com/fapi/v1/klines"
LIVE_REQUIRED_INTERVALS: Dict[str, int] = {"1m": 60, "5m": 60, "15m": 60, "1h": 60, "4h": 60}
WS_BOOTSTRAP_TIMEOUT_SEC: float = 25.0
WS_BOOTSTRAP_POLL_SEC: float = 0.25

_BOOTSTRAP_LOCK = threading.Lock()
_BOOTSTRAPPED_SYMBOLS: set[str] = set()


@dataclass(frozen=True)
class StageSuccess:
    stage: str
    details: Dict[str, Any]


@dataclass(frozen=True)
class StageFailure:
    stage: str
    module: str
    function: str
    error_type: str
    error_message: str
    traceback_text: str


def _json_default(v: Any) -> Any:
    if isinstance(v, Path):
        return str(v)
    if hasattr(v, "isoformat"):
        try:
            return v.isoformat()
        except Exception:
            return str(v)
    return str(v)


def _read_text_or_raise(path_str: str) -> str:
    path = PROJECT_ROOT / path_str
    if not path.exists():
        raise RuntimeError(f"Missing file: {path}")
    return path.read_text(encoding="utf-8")


def _ast_duplicate_top_level_defs_or_raise(path_str: str) -> Dict[str, Any]:
    src = _read_text_or_raise(path_str)
    try:
        tree = ast.parse(src, filename=path_str)
    except SyntaxError as exc:
        raise RuntimeError(
            f"SyntaxError in {path_str}: line={exc.lineno} offset={exc.offset} msg={exc.msg}"
        ) from exc

    seen: Dict[str, List[int]] = {}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            seen.setdefault(node.name, []).append(int(node.lineno))

    duplicates = {name: lines for name, lines in seen.items() if len(lines) > 1}
    if duplicates:
        raise RuntimeError(json.dumps({"file": path_str, "duplicates": duplicates}, ensure_ascii=False))
    return {
        "file": path_str,
        "top_level_defs": len(seen),
    }


def _import_module_or_raise(module_name: str) -> Any:
    try:
        return importlib.import_module(module_name)
    except Exception as exc:
        raise RuntimeError(f"import failed: module={module_name}") from exc


def _require_attr(module: Any, attr_name: str) -> Any:
    if not hasattr(module, attr_name):
        raise RuntimeError(f"missing attr: module={module.__name__} attr={attr_name}")
    return getattr(module, attr_name)


def _get_db_columns_or_raise(table_name: str) -> List[str]:
    from state.db_core import get_session

    sql = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = :table_name
        ORDER BY ordinal_position
        """
    )
    with get_session() as session:
        rows = session.execute(sql, {"table_name": table_name}).fetchall()

    if not rows:
        raise RuntimeError(f"table not found in DB: {table_name}")

    return [str(row[0]) for row in rows]


def _get_model_columns_or_raise(model_cls: Any) -> List[str]:
    table = getattr(model_cls, "__table__", None)
    if table is None:
        raise RuntimeError(f"model has no __table__: {model_cls}")
    return [str(col.name) for col in table.columns]


def _check_schema_alignment_or_raise() -> Dict[str, Any]:
    from state import db_models

    model_map = {
        "market_features": db_models.MarketFeature,
        "trade_context_snapshots": db_models.TradeContextSnapshot,
        "bt_trade_snapshots": db_models.TradeSnapshot,
    }

    results: Dict[str, Any] = {}
    missing_any = False

    for table_name, model_cls in model_map.items():
        db_cols = _get_db_columns_or_raise(table_name)
        model_cols = _get_model_columns_or_raise(model_cls)

        missing_in_db = [c for c in model_cols if c not in db_cols]
        extra_in_db = [c for c in db_cols if c not in model_cols]

        results[table_name] = {
            "db_column_count": len(db_cols),
            "model_column_count": len(model_cols),
            "missing_in_db": missing_in_db,
            "extra_in_db": extra_in_db,
        }

        if missing_in_db:
            missing_any = True

    if missing_any:
        raise RuntimeError(json.dumps(results, ensure_ascii=False))

    return results


def _require_dict(v: Any, name: str) -> Dict[str, Any]:
    if not isinstance(v, dict) or not v:
        raise RuntimeError(f"{name} must be non-empty dict")
    return v


def _require_float(v: Any, name: str, *, min_value: Optional[float] = None, max_value: Optional[float] = None) -> float:
    if isinstance(v, bool):
        raise RuntimeError(f"{name} bool not allowed")
    try:
        x = float(v)
    except Exception as exc:
        raise RuntimeError(f"{name} must be float-like") from exc
    if min_value is not None and x < min_value:
        raise RuntimeError(f"{name} must be >= {min_value}")
    if max_value is not None and x > max_value:
        raise RuntimeError(f"{name} must be <= {max_value}")
    return x


def _require_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        raise RuntimeError(f"{name} must be non-empty string")
    return s


def _normalize_symbol_strict(v: Any, name: str) -> str:
    s = str(v or "").upper().strip().replace("-", "").replace("/", "").replace("_", "")
    if not s:
        raise RuntimeError(f"{name} must be non-empty symbol")
    return s


def _http_get_json_or_raise(url: str, params: Dict[str, Any], *, timeout_sec: float) -> Any:
    query = urllib.parse.urlencode(params)
    full_url = f"{url}?{query}"
    req = urllib.request.Request(
        full_url,
        headers={
            "User-Agent": "auto-trader-debug-runner/1.0",
            "Accept": "application/json",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            status = getattr(resp, "status", None)
            body = resp.read()
    except Exception as exc:
        raise RuntimeError(f"HTTP GET failed: url={full_url} error={exc}") from exc

    if status != 200:
        raise RuntimeError(f"HTTP GET non-200: url={full_url} status={status}")

    try:
        return json.loads(body.decode("utf-8"))
    except Exception as exc:
        raise RuntimeError(f"HTTP response json decode failed: url={full_url}") from exc


def _fetch_public_futures_klines_or_raise(symbol: str, interval: str, limit: int) -> List[Any]:
    sym = _normalize_symbol_strict(symbol, "symbol")
    iv = _require_str(interval, "interval")
    if limit <= 0 or limit > 1500:
        raise RuntimeError(f"limit out of range for klines: {limit}")

    payload = _http_get_json_or_raise(
        BINANCE_FAPI_KLINES_URL,
        {"symbol": sym, "interval": iv, "limit": limit},
        timeout_sec=10.0,
    )
    if not isinstance(payload, list) or not payload:
        raise RuntimeError(f"Binance public klines empty/invalid: symbol={sym} interval={iv} limit={limit}")
    if len(payload) < limit:
        raise RuntimeError(
            f"Binance public klines insufficient rows: symbol={sym} interval={iv} need={limit} got={len(payload)}"
        )
    return payload


def _bootstrap_live_ws_buffers_or_raise() -> Dict[str, Any]:
    from settings import load_settings
    from infra.market_data_ws import (
        backfill_klines_from_rest,
        get_health_snapshot,
        get_market_snapshot,
        start_ws_loop,
    )

    settings = load_settings()
    symbol = _normalize_symbol_strict(getattr(settings, "symbol", ""), "settings.symbol")

    with _BOOTSTRAP_LOCK:
        already_bootstrapped = symbol in _BOOTSTRAPPED_SYMBOLS
        if not already_bootstrapped:
            for interval, min_len in LIVE_REQUIRED_INTERVALS.items():
                rows = _fetch_public_futures_klines_or_raise(symbol, interval, min_len)
                backfill_klines_from_rest(symbol, interval, rows)

            start_ws_loop(symbol)
            _BOOTSTRAPPED_SYMBOLS.add(symbol)

    deadline = time.time() + WS_BOOTSTRAP_TIMEOUT_SEC
    last_state: Dict[str, Any] = {}

    while time.time() < deadline:
        snap = get_market_snapshot(symbol)
        health = get_health_snapshot(symbol)

        klines = snap.get("klines")
        if not isinstance(klines, dict):
            raise RuntimeError("get_market_snapshot.klines invalid")

        kline_lengths = {
            iv: len(klines.get(iv, [])) if isinstance(klines.get(iv), list) else 0
            for iv in LIVE_REQUIRED_INTERVALS.keys()
        }
        missing_kline_intervals = [
            iv for iv, min_len in LIVE_REQUIRED_INTERVALS.items()
            if kline_lengths.get(iv, 0) < min_len
        ]

        orderbook = snap.get("orderbook")
        has_orderbook = isinstance(orderbook, dict) and bool(orderbook.get("bids")) and bool(orderbook.get("asks"))

        last_state = {
            "symbol": symbol,
            "health_overall_ok": bool(health.get("overall_ok", False)),
            "health_required_intervals": list(health.get("required_intervals") or []),
            "health_overall_reasons": list(health.get("overall_reasons") or []),
            "has_orderbook": has_orderbook,
            "kline_lengths": kline_lengths,
            "missing_kline_intervals": missing_kline_intervals,
        }

        if has_orderbook and not missing_kline_intervals and bool(health.get("overall_ok", False)):
            return last_state

        time.sleep(WS_BOOTSTRAP_POLL_SEC)

    raise RuntimeError(
        "ws_live_bootstrap failed: "
        + json.dumps(last_state, ensure_ascii=False)
    )


def _validate_unified_features_payload_or_raise(payload: Dict[str, Any]) -> Dict[str, Any]:
    required_top = [
        "timeframes",
        "orderbook",
        "pattern_features",
        "pattern_summary",
        "engine_scores",
        "microstructure",
        "volume_profile",
        "orderflow_cvd",
        "options_market",
        "entry_score",
    ]
    for key in required_top:
        if key not in payload:
            raise RuntimeError(f"unified_features missing key: {key}")

    engine_scores = _require_dict(payload["engine_scores"], "engine_scores")
    total = _require_dict(engine_scores.get("total"), "engine_scores.total")
    total_score = _require_float(total.get("score"), "engine_scores.total.score", min_value=0.0, max_value=100.0)
    entry_score = _require_float(payload.get("entry_score"), "entry_score", min_value=0.0, max_value=1.0)

    volume_profile = _require_dict(payload["volume_profile"], "volume_profile")
    orderflow_cvd = _require_dict(payload["orderflow_cvd"], "orderflow_cvd")
    options_market = _require_dict(payload["options_market"], "options_market")

    _require_float(volume_profile.get("poc_price"), "volume_profile.poc_price", min_value=0.0)
    _require_str(volume_profile.get("price_location"), "volume_profile.price_location")

    _require_float(orderflow_cvd.get("delta_ratio_pct"), "orderflow_cvd.delta_ratio_pct")
    _require_str(orderflow_cvd.get("aggression_bias"), "orderflow_cvd.aggression_bias")
    _require_str(orderflow_cvd.get("divergence"), "orderflow_cvd.divergence")

    _require_float(options_market.get("put_call_oi_ratio"), "options_market.put_call_oi_ratio", min_value=0.0)
    _require_float(options_market.get("put_call_volume_ratio"), "options_market.put_call_volume_ratio", min_value=0.0)
    _require_str(options_market.get("options_bias"), "options_market.options_bias")

    if abs(entry_score - (total_score / 100.0)) > 1e-9:
        raise RuntimeError(
            f"entry_score mismatch: entry_score={entry_score}, engine_total={total_score / 100.0}"
        )

    return {
        "engine_total_score": total_score,
        "entry_score": entry_score,
        "price_location": str(volume_profile.get("price_location")),
        "aggression_bias": str(orderflow_cvd.get("aggression_bias")),
        "options_bias": str(options_market.get("options_bias")),
    }


def _run_stage(stage_name: str, func: Callable[[], Dict[str, Any]]) -> tuple[Optional[StageSuccess], Optional[StageFailure]]:
    t0 = time.perf_counter()
    try:
        details = func()
        details = dict(details)
        details["duration_ms"] = round((time.perf_counter() - t0) * 1000.0, 2)
        return StageSuccess(stage=stage_name, details=details), None
    except Exception as exc:
        tb = traceback.format_exc()
        failure = StageFailure(
            stage=stage_name,
            module=getattr(func, "__module__", "__main__"),
            function=getattr(func, "__name__", stage_name),
            error_type=type(exc).__name__,
            error_message=str(exc),
            traceback_text=tb,
        )
        return None, failure


# =========================================================
# Stage functions
# =========================================================
def stage_static_source_scan() -> Dict[str, Any]:
    results: List[Dict[str, Any]] = []
    for path_str in TARGET_FILES:
        results.append(_ast_duplicate_top_level_defs_or_raise(path_str))
    return {
        "scanned_file_count": len(results),
        "files": results,
    }


def stage_imports_and_symbols() -> Dict[str, Any]:
    checks = {
        "analysis.volume_profile": ["VolumeProfileEngine", "VolumeProfileReport"],
        "analysis.orderflow_cvd": ["OrderFlowCvdEngine", "OrderFlowCvdReport"],
        "analysis.options_market_fetcher": ["OptionsMarketFetcher", "OptionsMarketSnapshot"],
        "analysis.market_researcher": ["MarketResearcher", "MarketResearchReport"],
        "analysis.quant_analyst": ["QuantAnalyst", "QuantAnalystResponse"],
        "strategy.unified_features_builder": ["build_unified_features", "UnifiedFeaturesError"],
        "strategy.score_engine": ["build_engine_scores", "ScoreEngineError"],
        "core.entry_pipeline": ["EntryCandidate", "_build_entry_market_data", "_decide_entry_candidate_strict"],
        "state.db_models": ["MarketFeature", "TradeContextSnapshot", "TradeSnapshot"],
    }

    imported: Dict[str, List[str]] = {}
    for module_name, attrs in checks.items():
        mod = _import_module_or_raise(module_name)
        imported[module_name] = []
        for attr in attrs:
            _require_attr(mod, attr)
            imported[module_name].append(attr)

    return {
        "imported_module_count": len(imported),
        "modules": imported,
    }


def stage_db_schema_alignment() -> Dict[str, Any]:
    return _check_schema_alignment_or_raise()


def stage_score_engine_contract() -> Dict[str, Any]:
    mod = _import_module_or_raise("strategy.score_engine")
    weights = getattr(mod, "TOTAL_WEIGHTS", None)
    if not isinstance(weights, dict) or not weights:
        raise RuntimeError("strategy.score_engine.TOTAL_WEIGHTS missing/invalid")
    total = float(sum(float(v) for v in weights.values()))
    if abs(total - 1.0) > 1e-9:
        raise RuntimeError(f"strategy.score_engine TOTAL_WEIGHTS sum must be 1.0 (got={total})")
    return {
        "weight_keys": sorted(list(weights.keys())),
        "weight_sum": total,
    }


def stage_ws_live_bootstrap() -> Dict[str, Any]:
    return _bootstrap_live_ws_buffers_or_raise()


def stage_unified_features_live() -> Dict[str, Any]:
    from strategy.unified_features_builder import build_unified_features

    _bootstrap_live_ws_buffers_or_raise()

    t0 = time.perf_counter()
    payload = build_unified_features()
    latency_ms = round((time.perf_counter() - t0) * 1000.0, 2)

    if not isinstance(payload, dict) or not payload:
        raise RuntimeError("build_unified_features returned empty/non-dict")

    summary = _validate_unified_features_payload_or_raise(payload)
    return {
        "latency_ms_live": latency_ms,
        "summary": summary,
        "top_keys": sorted(list(payload.keys())),
    }


def stage_entry_pipeline_live() -> Dict[str, Any]:
    from settings import load_settings
    from strategy.unified_features_builder import build_unified_features
    from core.entry_pipeline import (
        _decide_entry_candidate_strict,
        _evaluate_structural_entry_conflict_strict,
        _extract_runtime_decision_meta_strict,
    )

    _bootstrap_live_ws_buffers_or_raise()

    settings = load_settings()
    features = build_unified_features()
    meta = _extract_runtime_decision_meta_strict(features, settings)

    direction = str(features.get("engine_scores", {}).get("trend_4h", {}).get("direction") or "").upper().strip()
    if direction not in {"LONG", "SHORT"}:
        direction = "LONG"

    signal_ts_ms = int(features.get("orderbook", {}).get("checked_at_ms") or 0)
    if signal_ts_ms <= 0:
        raise RuntimeError("orderbook.checked_at_ms missing/invalid")

    last_price = float(features.get("volume_profile", {}).get("current_price") or 0.0)
    if last_price <= 0.0:
        raise RuntimeError("volume_profile.current_price missing/invalid")

    market_data = {
        "symbol": str(getattr(settings, "symbol", "")).strip(),
        "direction": direction,
        "signal_source": "debug_runner",
        "regime": "debug_runner",
        "signal_ts_ms": signal_ts_ms,
        "candles_5m": [],
        "candles_5m_raw": [],
        "last_price": last_price,
        "extra": {},
        "market_features": features,
        "entry_score": float(meta["entry_score"]),
        "trend_strength": float(meta["trend_strength"]),
        "spread": float(meta["spread"]),
        "orderbook_imbalance": float(meta["orderbook_imbalance"]),
        "entry_score_threshold": float(meta["entry_score_threshold"]),
        "poc_price": float(meta["poc_price"]),
        "value_area_low": float(meta["value_area_low"]),
        "value_area_high": float(meta["value_area_high"]),
        "poc_distance_bps": float(meta["poc_distance_bps"]),
        "price_location": str(meta["price_location"]),
        "delta_ratio_pct": float(meta["delta_ratio_pct"]),
        "aggression_bias": str(meta["aggression_bias"]),
        "divergence": str(meta["divergence"]),
        "cvd": float(meta["cvd"]),
        "orderflow_price_change_pct": float(meta["orderflow_price_change_pct"]),
        "put_call_oi_ratio": float(meta["put_call_oi_ratio"]),
        "put_call_volume_ratio": float(meta["put_call_volume_ratio"]),
        "options_bias": str(meta["options_bias"]),
    }

    block_reason = _evaluate_structural_entry_conflict_strict(market_data)
    candidate = _decide_entry_candidate_strict(market_data, settings)

    return {
        "direction_used": direction,
        "structural_block_reason": block_reason,
        "candidate_action": candidate.action,
        "candidate_reason": candidate.reason,
        "candidate_tp_pct": candidate.tp_pct,
        "candidate_sl_pct": candidate.sl_pct,
        "meta_keys": sorted(list(candidate.meta.keys())),
    }


def stage_market_researcher_live() -> Dict[str, Any]:
    from analysis.market_researcher import MarketResearcher

    t0 = time.perf_counter()
    report = MarketResearcher().run()
    latency_ms = round((time.perf_counter() - t0) * 1000.0, 2)

    to_dict = getattr(report, "to_dict", None)
    if to_dict is None or not callable(to_dict):
        raise RuntimeError("MarketResearchReport.to_dict missing")
    data = to_dict()
    if not isinstance(data, dict) or not data:
        raise RuntimeError("MarketResearchReport.to_dict returned empty/non-dict")

    required = [
        "poc_price",
        "value_area_low",
        "value_area_high",
        "cvd",
        "delta_ratio_pct",
        "put_call_oi_ratio",
        "put_call_volume_ratio",
        "options_bias",
        "volume_profile_summary",
        "orderflow_summary",
        "options_summary",
    ]
    for key in required:
        if key not in data:
            raise RuntimeError(f"MarketResearchReport missing key: {key}")

    return {
        "latency_ms_live": latency_ms,
        "symbol": data["symbol"],
        "market_regime": data["market_regime"],
        "conviction": data["conviction"],
        "options_bias": data["options_bias"],
        "aggression_bias": data["aggression_bias"],
        "price_location": data["price_location"],
    }


def stage_quant_analyst_optional() -> Dict[str, Any]:
    if not RUN_GPT_TESTS:
        return {
            "skipped": True,
            "reason": "RUN_GPT_TESTS not enabled",
        }

    from analysis.quant_analyst import QuantAnalyst

    t0 = time.perf_counter()
    result = QuantAnalyst().analyze_market_only(question="현재 외부 시장 구조를 요약해줘")
    latency_ms = round((time.perf_counter() - t0) * 1000.0, 2)

    payload = result.to_dict()
    if not isinstance(payload, dict) or not payload:
        raise RuntimeError("QuantAnalystResponse.to_dict returned empty/non-dict")

    return {
        "skipped": False,
        "latency_ms_live": latency_ms,
        "scope": payload["gpt_result"]["scope"],
        "used_inputs": payload["gpt_result"]["used_inputs"],
    }


def main() -> None:
    successes: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []

    stages: List[tuple[str, Callable[[], Dict[str, Any]]]] = [
        ("static_source_scan", stage_static_source_scan),
        ("imports_and_symbols", stage_imports_and_symbols),
        ("db_schema_alignment", stage_db_schema_alignment),
        ("score_engine_contract", stage_score_engine_contract),
        ("ws_live_bootstrap", stage_ws_live_bootstrap),
        ("unified_features_live", stage_unified_features_live),
        ("entry_pipeline_live", stage_entry_pipeline_live),
        ("market_researcher_live", stage_market_researcher_live),
        ("quant_analyst_optional", stage_quant_analyst_optional),
    ]

    for stage_name, func in stages:
        ok, fail = _run_stage(stage_name, func)
        if ok is not None:
            successes.append(asdict(ok))
        if fail is not None:
            failures.append(asdict(fail))

    report = {
        "ok": len(failures) == 0,
        "successes": successes,
        "failures": failures,
    }

    print("=" * 72)
    print("MARKET STRUCTURE DEBUG REPORT")
    print("=" * 72)
    print(json.dumps(report, ensure_ascii=False, indent=2, default=_json_default))

    if failures:
        raise SystemExit(1)
    raise SystemExit(0)


if __name__ == "__main__":
    main()