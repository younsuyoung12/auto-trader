"""
========================================================
FILE: tests_tradegrade/test_full_trading_pipeline.py
AUTO-TRADER — FULL TRADING PIPELINE CONTRACT TEST
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

핵심 변경 요약
- run_bot_ws 스타일 파이프라인 계약을 실제 주문 없이 검증하는 테스트 파일 추가
- invariant_guard → Signal 생성 → execution_engine.execute 까지 단계별 검증
- 현재 run_bot_ws 신호 조립이 entry_price_hint/mark_price 없이 실패하는 지점 재현
- mark_price 포함 patched pipeline 이 LONG/SHORT 모두 통과하는지 검증
- 기존 기능 삭제 없음

코드 정리 내용
- 실제 WS/REST/DB/OpenAI 호출 없이 순수 계약 테스트만 수행
- run_bot_ws 의 signal_final 조립 로직을 테스트용으로 엄격 복제
- execution_engine.Trade 는 FakeTrade 로 패치하여 execute 계약만 검증
- stage별 traceback 과 PASS/EXPECTED_FAIL/FAIL 을 즉시 출력

설계 원칙
- 폴백 금지
- 단계별 Fail-Fast
- 실제 주문 금지
- 현재 실패해야 하는 버그는 EXPECTED_FAIL 로 고정
- patched pipeline 은 LONG/SHORT 모두 execute 계약까지 통과해야 함

변경 이력
--------------------------------------------------------
- 2026-03-10:
  1) ADD: full trading pipeline 계약 테스트 파일 신규 작성
  2) ADD: invariant_guard LONG/SHORT 입력 검증
  3) ADD: current run_bot_ws style signal missing price 재현
  4) ADD: patched pipeline with mark_price LONG/SHORT execute 검증
========================================================

실행 예시
- python tests_tradegrade/test_full_trading_pipeline.py
"""

from __future__ import annotations

import argparse
import importlib
import math
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, Dict, Optional


def _find_repo_root_or_raise() -> Path:
    candidates: list[Path] = []

    if "__file__" in globals():
        candidates.append(Path(__file__).resolve())

    candidates.append(Path.cwd().resolve())

    for base in candidates:
        scan_from = base if base.is_dir() else base.parent
        for current in [scan_from, *scan_from.parents]:
            if (
                (current / "settings.py").exists()
                and (current / "core" / "run_bot_ws.py").exists()
                and (current / "execution" / "execution_engine.py").exists()
            ):
                return current

    raise RuntimeError(
        "repo root not found (STRICT): expected settings.py, core/run_bot_ws.py, execution/execution_engine.py"
    )


REPO_ROOT = _find_repo_root_or_raise()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@dataclass(slots=True)
class StageResult:
    name: str
    ok: bool
    expected_failure: bool
    detail: str
    traceback_text: str = ""

    @property
    def status(self) -> str:
        if self.ok and not self.expected_failure:
            return "PASS"
        if self.ok and self.expected_failure:
            return "EXPECTED_FAIL"
        if (not self.ok) and self.expected_failure:
            return "UNEXPECTED_PASS"
        return "FAIL"


@dataclass(slots=True)
class FakeEntryCandidate:
    direction: str
    tp_pct: float
    sl_pct: float
    reason: str
    guard_adjustments: Dict[str, Any]
    meta: Dict[str, Any]


@dataclass(slots=True)
class FakeAccountState:
    equity_current_usdt: float
    equity_peak_usdt: float
    dd_pct: float
    consecutive_losses: int


@dataclass(slots=True)
class FakeRegimeDecision:
    score: float
    band: str
    allocation: float


@dataclass(slots=True)
class FakeHeatmapKey:
    regime_band: str
    distortion_bucket: str
    score_bucket: str


@dataclass(slots=True)
class FakeCell:
    status: str
    ev: float
    n: int


@dataclass(slots=True)
class FakeRiskPhysicsResult:
    effective_risk_pct: float
    reason: str
    auto_blocked: bool
    auto_risk_multiplier: float
    heatmap_status: str
    heatmap_ev: float
    heatmap_n: int


@dataclass(slots=True)
class FakeTrade:
    symbol: str
    side: str
    qty: float
    entry_price: float
    entry_order_id: str
    tp_order_id: str
    sl_order_id: str
    entry_ts: datetime
    reconciliation_status: str
    client_entry_id: Optional[str]


def _banner(title: str) -> None:
    print("\n" + "=" * 88)
    print(title)
    print("=" * 88)


def _pretty(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:.12g}"
    return repr(value)


def _run_stage(
    name: str,
    fn,
    *,
    expect_failure: bool = False,
    expected_message_substring: Optional[str] = None,
) -> StageResult:
    _banner(f"[STAGE] {name}")

    try:
        detail = fn()
        detail_str = str(detail) if detail is not None else "OK"

        if expect_failure:
            print(f"[UNEXPECTED_PASS] {detail_str}")
            return StageResult(
                name=name,
                ok=False,
                expected_failure=True,
                detail=detail_str,
            )

        print(f"[PASS] {detail_str}")
        return StageResult(
            name=name,
            ok=True,
            expected_failure=False,
            detail=detail_str,
        )

    except Exception as e:
        tb = traceback.format_exc()
        print(tb.rstrip())

        if expect_failure:
            if expected_message_substring and expected_message_substring not in str(e):
                return StageResult(
                    name=name,
                    ok=False,
                    expected_failure=True,
                    detail=(
                        "expected failure matched stage type, but message mismatch: "
                        f"wanted substring={expected_message_substring!r}, actual={str(e)!r}"
                    ),
                    traceback_text=tb,
                )

            return StageResult(
                name=name,
                ok=True,
                expected_failure=True,
                detail=f"{type(e).__name__}: {e}",
                traceback_text=tb,
            )

        return StageResult(
            name=name,
            ok=False,
            expected_failure=False,
            detail=f"{type(e).__name__}: {e}",
            traceback_text=tb,
        )


def _import_module_or_raise(module_name: str) -> ModuleType:
    return importlib.import_module(module_name)


def _build_settings_stub(symbol: str, leverage: float) -> Any:
    if not isinstance(symbol, str) or not symbol.strip():
        raise RuntimeError("symbol must be non-empty string (STRICT)")
    if not math.isfinite(float(leverage)) or float(leverage) <= 0.0:
        raise RuntimeError("leverage must be finite > 0 (STRICT)")

    return SimpleNamespace(
        symbol=symbol.strip().upper(),
        leverage=float(leverage),
    )


def _make_candidate(direction: str) -> FakeEntryCandidate:
    direction_u = str(direction).upper().strip()
    if direction_u not in {"LONG", "SHORT"}:
        raise RuntimeError(f"candidate direction invalid (STRICT): {direction!r}")

    return FakeEntryCandidate(
        direction=direction_u,
        tp_pct=0.0125,
        sl_pct=0.0060,
        reason=f"PIPELINE_{direction_u}",
        guard_adjustments={},
        meta={
            "candidate_origin": "test_full_trading_pipeline",
            "direction_snapshot": direction_u,
        },
    )


def _make_account_state(equity_usdt: float) -> FakeAccountState:
    if not math.isfinite(equity_usdt) or equity_usdt <= 0.0:
        raise RuntimeError("equity_usdt invalid (STRICT)")
    return FakeAccountState(
        equity_current_usdt=float(equity_usdt),
        equity_peak_usdt=float(equity_usdt),
        dd_pct=0.0,
        consecutive_losses=0,
    )


def _make_regime_decision() -> FakeRegimeDecision:
    return FakeRegimeDecision(
        score=71.0,
        band="TREND",
        allocation=0.10,
    )


def _make_heatmap_key(regime_band: str) -> FakeHeatmapKey:
    return FakeHeatmapKey(
        regime_band=str(regime_band),
        distortion_bucket="MID",
        score_bucket="A",
    )


def _make_cell() -> FakeCell:
    return FakeCell(
        status="ACTIVE",
        ev=0.37,
        n=128,
    )


def _make_risk_physics_result(risk_pct: float) -> FakeRiskPhysicsResult:
    if not math.isfinite(risk_pct) or risk_pct <= 0.0 or risk_pct > 1.0:
        raise RuntimeError("risk_pct invalid (STRICT)")
    return FakeRiskPhysicsResult(
        effective_risk_pct=float(risk_pct),
        reason="PIPELINE_TEST_RISK_OK",
        auto_blocked=False,
        auto_risk_multiplier=1.0,
        heatmap_status="ACTIVE",
        heatmap_ev=0.37,
        heatmap_n=128,
    )


def _build_market_snapshot(symbol: str, mark_price: float) -> Dict[str, Any]:
    if not math.isfinite(mark_price) or mark_price <= 0.0:
        raise RuntimeError("mark_price invalid (STRICT)")
    return {
        "symbol": str(symbol).upper().strip(),
        "close": float(mark_price),
        "mark_price": float(mark_price),
        "last_price": float(mark_price),
    }


def _build_meta2_run_bot_style(
    *,
    cand: FakeEntryCandidate,
    account_state: FakeAccountState,
    regime_decision: FakeRegimeDecision,
    micro_score_risk: float,
    hk: FakeHeatmapKey,
    cell: FakeCell,
    rp: FakeRiskPhysicsResult,
    extra_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if not math.isfinite(micro_score_risk):
        raise RuntimeError("micro_score_risk must be finite (STRICT)")

    meta2 = dict(cand.meta or {})
    meta2.update(
        {
            "equity_current_usdt": float(account_state.equity_current_usdt),
            "equity_peak_usdt": float(account_state.equity_peak_usdt),
            "dd_pct": float(account_state.dd_pct),
            "consecutive_losses": int(account_state.consecutive_losses),
            "risk_physics_reason": str(rp.reason),
            "dynamic_allocation_ratio": float(rp.effective_risk_pct),
            "dynamic_risk_pct": float(rp.effective_risk_pct),
            "regime_score": float(regime_decision.score),
            "regime_band": str(regime_decision.band),
            "regime_allocation": float(regime_decision.allocation),
            "micro_score_risk": float(micro_score_risk),
            "ev_cell_key": f"{hk.regime_band}|{hk.distortion_bucket}|{hk.score_bucket}",
            "ev_cell_status": str(cell.status),
            "ev_cell_ev": float(cell.ev),
            "ev_cell_n": int(cell.n),
            "auto_blocked": bool(rp.auto_blocked),
            "auto_risk_multiplier": float(rp.auto_risk_multiplier),
            "heatmap_status": str(rp.heatmap_status),
            "heatmap_ev": float(rp.heatmap_ev),
            "heatmap_n": int(rp.heatmap_n),
        }
    )

    if extra_meta:
        meta2.update(dict(extra_meta))

    return meta2


def _make_signal_object_or_raise(
    *,
    action: str,
    cand: FakeEntryCandidate,
    risk_pct: float,
    meta: Dict[str, Any],
) -> Any:
    signal_mod = _import_module_or_raise("strategy.signal")
    signal_cls = getattr(signal_mod, "Signal", None)
    if signal_cls is None:
        raise RuntimeError("strategy.signal.Signal missing (STRICT)")

    return signal_cls(
        action=str(action),
        direction=str(cand.direction),
        tp_pct=float(cand.tp_pct),
        sl_pct=float(cand.sl_pct),
        risk_pct=float(risk_pct),
        reason=str(cand.reason),
        guard_adjustments=dict(cand.guard_adjustments or {}),
        meta=dict(meta),
    )


def _stage_imports() -> str:
    ee = _import_module_or_raise("execution.execution_engine")
    rb = _import_module_or_raise("core.run_bot_ws")
    inv = _import_module_or_raise("execution.invariant_guard")
    sig = _import_module_or_raise("strategy.signal")

    if getattr(ee, "ExecutionEngine", None) is None:
        raise RuntimeError("ExecutionEngine missing after import (STRICT)")
    if getattr(inv, "SignalInvariantInputs", None) is None:
        raise RuntimeError("SignalInvariantInputs missing after import (STRICT)")
    if getattr(inv, "validate_signal_invariants_strict", None) is None:
        raise RuntimeError("validate_signal_invariants_strict missing after import (STRICT)")
    if getattr(sig, "Signal", None) is None:
        raise RuntimeError("Signal missing after import (STRICT)")
    if getattr(rb, "ExecutionEngine", None) is None:
        raise RuntimeError("run_bot_ws ExecutionEngine binding missing (STRICT)")

    return "execution_engine + invariant_guard + signal + run_bot_ws import ok"


def _stage_invariant_guard(
    *,
    symbol: str,
    direction: str,
    risk_pct: float,
    equity_usdt: float,
) -> str:
    inv = _import_module_or_raise("execution.invariant_guard")
    inputs_cls = getattr(inv, "SignalInvariantInputs")
    validate_fn = getattr(inv, "validate_signal_invariants_strict")

    obj = inputs_cls(
        symbol=str(symbol),
        direction=str(direction),
        risk_pct=float(risk_pct),
        tp_pct=0.0125,
        sl_pct=0.0060,
        dd_pct=0.0,
        micro_score_risk=41.0,
        final_risk_multiplier=None,
        equity_current_usdt=float(equity_usdt),
        equity_peak_usdt=float(equity_usdt),
    )
    validate_fn(obj)
    return f"invariant ok: direction={direction}"


def _stage_current_run_bot_signal_fails_missing_price(
    *,
    symbol: str,
    leverage: float,
    mark_price: float,
    equity_usdt: float,
    risk_pct: float,
    direction: str,
) -> str:
    ee = _import_module_or_raise("execution.execution_engine")
    settings_stub = _build_settings_stub(symbol=symbol, leverage=leverage)

    cand = _make_candidate(direction)
    account_state = _make_account_state(equity_usdt)
    regime_decision = _make_regime_decision()
    hk = _make_heatmap_key(regime_decision.band)
    cell = _make_cell()
    rp = _make_risk_physics_result(risk_pct)
    market = _build_market_snapshot(symbol, mark_price)

    meta2 = _build_meta2_run_bot_style(
        cand=cand,
        account_state=account_state,
        regime_decision=regime_decision,
        micro_score_risk=41.0,
        hk=hk,
        cell=cell,
        rp=rp,
        extra_meta={
            "symbol": str(market["symbol"]),
            "pipeline_stage": "current-run-bot-style",
        },
    )

    signal = _make_signal_object_or_raise(
        action="ENTER",
        cand=cand,
        risk_pct=rp.effective_risk_pct,
        meta=meta2,
    )

    ee._normalize_entry_request_strict(signal, settings_stub)
    return "current run_bot_ws style signal unexpectedly normalized"


def _build_fake_trade_or_raise(req: Any, *, client_entry_id: Optional[str]) -> FakeTrade:
    if not math.isfinite(float(req.qty)) or float(req.qty) <= 0.0:
        raise RuntimeError("req.qty invalid while building FakeTrade (STRICT)")
    if not math.isfinite(float(req.entry_price_hint)) or float(req.entry_price_hint) <= 0.0:
        raise RuntimeError("req.entry_price_hint invalid while building FakeTrade (STRICT)")
    if not isinstance(req.symbol, str) or not req.symbol.strip():
        raise RuntimeError("req.symbol invalid while building FakeTrade (STRICT)")
    if not isinstance(req.side_open, str) or not req.side_open.strip():
        raise RuntimeError("req.side_open invalid while building FakeTrade (STRICT)")

    return FakeTrade(
        symbol=str(req.symbol).strip().upper(),
        side=str(req.side_open).strip().upper(),
        qty=float(req.qty),
        entry_price=float(req.entry_price_hint),
        entry_order_id="debug-entry-order-id",
        tp_order_id="debug-tp-order-id",
        sl_order_id="debug-sl-order-id",
        entry_ts=datetime.now(timezone.utc),
        reconciliation_status="PROTECTION_VERIFIED",
        client_entry_id=client_entry_id,
    )


def _stage_patched_pipeline_execute(
    *,
    symbol: str,
    leverage: float,
    mark_price: float,
    equity_usdt: float,
    risk_pct: float,
    direction: str,
) -> str:
    ee = _import_module_or_raise("execution.execution_engine")
    settings_stub = _build_settings_stub(symbol=symbol, leverage=leverage)

    cand = _make_candidate(direction)
    account_state = _make_account_state(equity_usdt)
    regime_decision = _make_regime_decision()
    hk = _make_heatmap_key(regime_decision.band)
    cell = _make_cell()
    rp = _make_risk_physics_result(risk_pct)
    market = _build_market_snapshot(symbol, mark_price)

    extra_meta = {
        "symbol": str(market["symbol"]),
        "mark_price": float(market["mark_price"]),
        "pipeline_stage": "patched-with-mark-price",
        "entry_client_order_id": f"pipeline-{direction.lower()}-cid",
    }

    meta2 = _build_meta2_run_bot_style(
        cand=cand,
        account_state=account_state,
        regime_decision=regime_decision,
        micro_score_risk=41.0,
        hk=hk,
        cell=cell,
        rp=rp,
        extra_meta=extra_meta,
    )

    signal = _make_signal_object_or_raise(
        action="ENTER",
        cand=cand,
        risk_pct=rp.effective_risk_pct,
        meta=meta2,
    )

    req_preview = ee._normalize_entry_request_strict(signal, settings_stub)
    expected_side = "BUY" if direction == "LONG" else "SELL"
    expected_qty = (float(equity_usdt) * float(risk_pct) * float(leverage)) / float(mark_price)

    if req_preview.side_open != expected_side:
        raise RuntimeError(
            f"normalized side mismatch (STRICT): {req_preview.side_open!r} != {expected_side!r}"
        )
    if abs(float(req_preview.qty) - float(expected_qty)) > 1e-12:
        raise RuntimeError(
            f"normalized qty mismatch (STRICT): {req_preview.qty} != {expected_qty}"
        )

    captured: Dict[str, Any] = {}
    original_open = ee.open_position_with_tp_sl
    original_trade_cls = ee.Trade

    try:
        ee.Trade = FakeTrade

        def _stub_open_position_with_tp_sl(
            settings: Any,
            *,
            symbol: str,
            side_open: str,
            qty: float,
            entry_price_hint: float,
            tp_pct: float,
            sl_pct: float,
            source: str,
            soft_mode: bool,
            sl_floor_ratio: Optional[float],
            available_usdt: Optional[float],
            entry_client_order_id: Optional[str],
        ) -> Any:
            captured["settings_symbol"] = getattr(settings, "symbol", None)
            captured["symbol"] = symbol
            captured["side_open"] = side_open
            captured["qty"] = qty
            captured["entry_price_hint"] = entry_price_hint
            captured["source"] = source
            captured["soft_mode"] = soft_mode
            captured["available_usdt"] = available_usdt
            captured["entry_client_order_id"] = entry_client_order_id

            return _build_fake_trade_or_raise(
                req_preview,
                client_entry_id=entry_client_order_id,
            )

        ee.open_position_with_tp_sl = _stub_open_position_with_tp_sl

        engine = ee.ExecutionEngine(settings_stub)
        trade = engine.execute(signal)

        if captured.get("settings_symbol") != symbol:
            raise RuntimeError(
                f"settings.symbol mismatch (STRICT): {captured.get('settings_symbol')!r} != {symbol!r}"
            )
        if captured.get("symbol") != symbol:
            raise RuntimeError(
                f"symbol mismatch (STRICT): {captured.get('symbol')!r} != {symbol!r}"
            )
        if captured.get("side_open") != expected_side:
            raise RuntimeError(
                f"side_open mismatch (STRICT): {captured.get('side_open')!r} != {expected_side!r}"
            )
        if abs(float(captured.get("entry_price_hint")) - float(mark_price)) > 1e-12:
            raise RuntimeError(
                f"entry_price_hint mismatch (STRICT): {captured.get('entry_price_hint')} != {mark_price}"
            )
        if abs(float(captured.get("qty")) - float(expected_qty)) > 1e-12:
            raise RuntimeError(
                f"qty mismatch (STRICT): {captured.get('qty')} != {expected_qty}"
            )
        if getattr(trade, "side", None) != expected_side:
            raise RuntimeError(
                f"Trade.side mismatch (STRICT): {getattr(trade, 'side', None)!r} != {expected_side!r}"
            )
        if getattr(trade, "symbol", None) != symbol:
            raise RuntimeError(
                f"Trade.symbol mismatch (STRICT): {getattr(trade, 'symbol', None)!r} != {symbol!r}"
            )

        return (
            f"patched pipeline ok: direction={direction} side={expected_side} "
            f"qty={_pretty(getattr(trade, 'qty', None))} entry={_pretty(getattr(trade, 'entry_price', None))}"
        )

    finally:
        ee.open_position_with_tp_sl = original_open
        ee.Trade = original_trade_cls


def _print_summary(results: list[StageResult]) -> None:
    _banner("[SUMMARY]")

    for item in results:
        print(f"{item.status:<15} | {item.name:<48} | {item.detail}")

    unexpected = [x for x in results if x.status in {"FAIL", "UNEXPECTED_PASS"}]
    expected = [x for x in results if x.status == "EXPECTED_FAIL"]
    passed = [x for x in results if x.status == "PASS"]

    print("-" * 88)
    print(f"PASS={len(passed)} EXPECTED_FAIL={len(expected)} UNEXPECTED={len(unexpected)}")
    print("[RESULT] PASS" if not unexpected else "[RESULT] FAIL")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="run_bot_ws style full trading pipeline contract test"
    )
    parser.add_argument("--symbol", type=str, default="BTCUSDT")
    parser.add_argument("--leverage", type=float, default=10.0)
    parser.add_argument("--mark-price", type=float, default=65000.0)
    parser.add_argument("--equity-usdt", type=float, default=1000.0)
    parser.add_argument("--risk-pct", type=float, default=0.10)
    args = parser.parse_args()

    symbol = str(args.symbol).replace("-", "").replace("/", "").upper().strip()
    if not symbol:
        raise RuntimeError("symbol is empty (STRICT)")

    _banner("[BOOT]")
    print(f"repo_root={REPO_ROOT}")
    print(f"python={sys.executable}")
    print(f"cwd={Path.cwd().resolve()}")
    print(f"epoch={time.time():.6f}")

    results: list[StageResult] = []

    results.append(_run_stage("import required modules", _stage_imports))
    results.append(
        _run_stage(
            "invariant guard LONG input",
            lambda: _stage_invariant_guard(
                symbol=symbol,
                direction="LONG",
                risk_pct=float(args.risk_pct),
                equity_usdt=float(args.equity_usdt),
            ),
        )
    )
    results.append(
        _run_stage(
            "invariant guard SHORT input",
            lambda: _stage_invariant_guard(
                symbol=symbol,
                direction="SHORT",
                risk_pct=float(args.risk_pct),
                equity_usdt=float(args.equity_usdt),
            ),
        )
    )
    results.append(
        _run_stage(
            "current run_bot_ws signal fails without price",
            lambda: _stage_current_run_bot_signal_fails_missing_price(
                symbol=symbol,
                leverage=float(args.leverage),
                mark_price=float(args.mark_price),
                equity_usdt=float(args.equity_usdt),
                risk_pct=float(args.risk_pct),
                direction="LONG",
            ),
            expect_failure=True,
            expected_message_substring="entry_price_hint",
        )
    )
    results.append(
        _run_stage(
            "patched pipeline LONG execute",
            lambda: _stage_patched_pipeline_execute(
                symbol=symbol,
                leverage=float(args.leverage),
                mark_price=float(args.mark_price),
                equity_usdt=float(args.equity_usdt),
                risk_pct=float(args.risk_pct),
                direction="LONG",
            ),
        )
    )
    results.append(
        _run_stage(
            "patched pipeline SHORT execute",
            lambda: _stage_patched_pipeline_execute(
                symbol=symbol,
                leverage=float(args.leverage),
                mark_price=float(args.mark_price),
                equity_usdt=float(args.equity_usdt),
                risk_pct=float(args.risk_pct),
                direction="SHORT",
            ),
        )
    )

    _print_summary(results)

    unexpected = [x for x in results if x.status in {"FAIL", "UNEXPECTED_PASS"}]
    raise SystemExit(1 if unexpected else 0)


if __name__ == "__main__":
    main()