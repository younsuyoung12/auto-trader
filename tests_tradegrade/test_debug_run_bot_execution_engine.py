"""
========================================================
FILE: tests_tradegrade/test_debug_run_bot_execution_engine.py
AUTO-TRADER — RUN_BOT_WS / EXECUTION_ENGINE DEBUG TEST
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

핵심 변경 요약
- run_bot_ws ↔ execution_engine 계약을 실제 주문 없이 단계별로 검증하는 디버그 테스트 파일 추가
- import / signal 정규화 / execute 계약 검증을 분리해 어느 단계에서 깨지는지 즉시 확인 가능
- 현재 run_bot_ws 스타일 Signal 에서 entry_price_hint 누락 여부를 근본 원인 후보로 재현
- execute 단계 스텁을 실제 Trade 생성 추론 방식에서 제거하고, execution_engine.Trade 를 테스트용 FakeTrade 로 엄격 패치
- 기존 기능 삭제 없음

코드 정리 내용
- 네트워크/실주문 호출 없이 open_position_with_tp_sl 을 스텁으로 대체
- Trade 실구현의 내부 생성자 필드 추론 제거
- execution_engine 내부의 _validate_trade_contract_strict 가 요구하는 필수 계약만 정확히 만족하는 테스트용 Trade 객체 사용
- 예외를 삼키지 않고 stage별 traceback 을 그대로 출력
- repo root 자동 탐지 및 sys.path 부트스트랩 추가

설계 원칙
- 폴백 금지
- 단계별 Fail-Fast
- 실제 주문 금지
- 테스트가 실패하면 어느 계약이 깨졌는지 즉시 출력
- 테스트 스텁이 실제 엔진 계약을 흐리지 않도록 최소 속성만 엄격 제공

변경 이력
--------------------------------------------------------
- 2026-03-10:
  1) FIX(TEST-STUB): Trade dataclass 생성 추론 제거
  2) FIX(TEST-CONTRACT): execution_engine.Trade 를 FakeTrade 로 패치해 execute 단계 검증 안정화
  3) KEEP: entry_price_hint 누락 재현 stage 유지
  4) KEEP: normalize / execute 단계 분리 유지
========================================================

실행 예시
- python tests_tradegrade/test_debug_run_bot_execution_engine.py
- python tests_tradegrade/test_debug_run_bot_execution_engine.py --skip-run-bot-import
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
from typing import Any, Dict, Mapping, Optional


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


def _make_run_bot_style_signal_without_price(*, risk_pct: float, equity_usdt: float) -> Dict[str, Any]:
    if not math.isfinite(risk_pct) or risk_pct <= 0.0 or risk_pct > 1.0:
        raise RuntimeError("risk_pct must be within (0, 1] (STRICT)")
    if not math.isfinite(equity_usdt) or equity_usdt <= 0.0:
        raise RuntimeError("equity_usdt must be finite > 0 (STRICT)")

    return {
        "action": "ENTER",
        "direction": "LONG",
        "tp_pct": 0.0125,
        "sl_pct": 0.0060,
        "risk_pct": float(risk_pct),
        "reason": "DEBUG_ENTRY_FROM_RUN_BOT_WS_STYLE_SIGNAL",
        "guard_adjustments": {},
        "meta": {
            "equity_current_usdt": float(equity_usdt),
            "equity_peak_usdt": float(equity_usdt),
            "dd_pct": 0.0,
            "dynamic_allocation_ratio": float(risk_pct),
            "dynamic_risk_pct": float(risk_pct),
            "regime_score": 71.0,
            "regime_band": "TREND",
            "micro_score_risk": 41.0,
        },
    }


def _make_enriched_signal_with_price(
    *,
    risk_pct: float,
    equity_usdt: float,
    mark_price: float,
    symbol: str,
    client_entry_id: Optional[str],
) -> Dict[str, Any]:
    if not math.isfinite(mark_price) or mark_price <= 0.0:
        raise RuntimeError("mark_price must be finite > 0 (STRICT)")

    signal = _make_run_bot_style_signal_without_price(
        risk_pct=risk_pct,
        equity_usdt=equity_usdt,
    )
    signal["meta"] = dict(signal["meta"])
    signal["meta"]["mark_price"] = float(mark_price)
    signal["meta"]["symbol"] = str(symbol).upper().strip()
    if client_entry_id is not None:
        signal["meta"]["entry_client_order_id"] = str(client_entry_id).strip()
    return signal


def _stage_import_execution_engine() -> str:
    mod = _import_module_or_raise("execution.execution_engine")
    engine_cls = getattr(mod, "ExecutionEngine", None)
    if engine_cls is None:
        raise RuntimeError("ExecutionEngine missing after import (STRICT)")
    return f"import ok: module={mod.__name__} ExecutionEngine={engine_cls.__name__}"


def _stage_import_run_bot_ws() -> str:
    mod = _import_module_or_raise("core.run_bot_ws")
    engine_cls = getattr(mod, "ExecutionEngine", None)
    if engine_cls is None:
        raise RuntimeError("run_bot_ws imported but ExecutionEngine binding missing (STRICT)")
    return f"import ok: module={mod.__name__} ExecutionEngine={engine_cls.__name__}"


def _stage_reproduce_missing_entry_price(
    *,
    symbol: str,
    leverage: float,
    risk_pct: float,
    equity_usdt: float,
) -> str:
    ee = _import_module_or_raise("execution.execution_engine")
    settings_stub = _build_settings_stub(symbol=symbol, leverage=leverage)
    signal = _make_run_bot_style_signal_without_price(
        risk_pct=risk_pct,
        equity_usdt=equity_usdt,
    )

    ee._normalize_entry_request_strict(signal, settings_stub)
    return "normalize unexpectedly succeeded"


def _stage_normalize_enriched_signal(
    *,
    symbol: str,
    leverage: float,
    risk_pct: float,
    equity_usdt: float,
    mark_price: float,
    client_entry_id: Optional[str],
) -> str:
    ee = _import_module_or_raise("execution.execution_engine")
    settings_stub = _build_settings_stub(symbol=symbol, leverage=leverage)
    signal = _make_enriched_signal_with_price(
        risk_pct=risk_pct,
        equity_usdt=equity_usdt,
        mark_price=mark_price,
        symbol=symbol,
        client_entry_id=client_entry_id,
    )

    req = ee._normalize_entry_request_strict(signal, settings_stub)
    expected_qty = (float(equity_usdt) * float(risk_pct) * float(leverage)) / float(mark_price)

    if req.symbol != symbol:
        raise RuntimeError(f"req.symbol mismatch (STRICT): {req.symbol!r} != {symbol!r}")
    if req.side_open != "BUY":
        raise RuntimeError(f"req.side_open mismatch (STRICT): {req.side_open!r} != 'BUY'")
    if abs(float(req.qty) - float(expected_qty)) > 1e-12:
        raise RuntimeError(
            f"qty mismatch (STRICT): normalized={req.qty} expected={expected_qty}"
        )
    if abs(float(req.entry_price_hint) - float(mark_price)) > 1e-12:
        raise RuntimeError(
            f"entry_price_hint mismatch (STRICT): normalized={req.entry_price_hint} expected={mark_price}"
        )
    if client_entry_id is not None and str(req.entry_client_order_id) != str(client_entry_id):
        raise RuntimeError(
            "entry_client_order_id mismatch (STRICT): "
            f"{req.entry_client_order_id!r} != {client_entry_id!r}"
        )

    return (
        f"normalize ok: symbol={req.symbol} side_open={req.side_open} "
        f"qty={req.qty:.12g} entry_price_hint={req.entry_price_hint:.12g}"
    )


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


def _build_fake_trade_or_raise(req: Any, *, include_client_entry_id: bool) -> FakeTrade:
    if not math.isfinite(float(req.qty)) or float(req.qty) <= 0.0:
        raise RuntimeError("req.qty invalid while building FakeTrade (STRICT)")
    if not math.isfinite(float(req.entry_price_hint)) or float(req.entry_price_hint) <= 0.0:
        raise RuntimeError("req.entry_price_hint invalid while building FakeTrade (STRICT)")
    if not isinstance(req.symbol, str) or not req.symbol.strip():
        raise RuntimeError("req.symbol invalid while building FakeTrade (STRICT)")
    if not isinstance(req.side_open, str) or not req.side_open.strip():
        raise RuntimeError("req.side_open invalid while building FakeTrade (STRICT)")

    client_entry_id: Optional[str]
    if include_client_entry_id:
        client_entry_id = req.entry_client_order_id
    else:
        client_entry_id = None

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


def _stage_execute_with_stubbed_open_position(
    *,
    symbol: str,
    leverage: float,
    risk_pct: float,
    equity_usdt: float,
    mark_price: float,
    client_entry_id: Optional[str],
) -> str:
    ee = _import_module_or_raise("execution.execution_engine")
    settings_stub = _build_settings_stub(symbol=symbol, leverage=leverage)
    signal = _make_enriched_signal_with_price(
        risk_pct=risk_pct,
        equity_usdt=equity_usdt,
        mark_price=mark_price,
        symbol=symbol,
        client_entry_id=client_entry_id,
    )

    req_preview = ee._normalize_entry_request_strict(signal, settings_stub)

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
            captured["tp_pct"] = tp_pct
            captured["sl_pct"] = sl_pct
            captured["source"] = source
            captured["soft_mode"] = soft_mode
            captured["sl_floor_ratio"] = sl_floor_ratio
            captured["available_usdt"] = available_usdt
            captured["entry_client_order_id"] = entry_client_order_id

            return _build_fake_trade_or_raise(
                req_preview,
                include_client_entry_id=True,
            )

        ee.open_position_with_tp_sl = _stub_open_position_with_tp_sl

        engine = ee.ExecutionEngine(settings_stub)
        trade = engine.execute(signal)

        if captured.get("settings_symbol") != symbol:
            raise RuntimeError(
                f"stub settings symbol mismatch (STRICT): {captured.get('settings_symbol')!r} != {symbol!r}"
            )
        if captured.get("symbol") != symbol:
            raise RuntimeError(
                f"stub symbol mismatch (STRICT): {captured.get('symbol')!r} != {symbol!r}"
            )
        if captured.get("side_open") != "BUY":
            raise RuntimeError(
                f"stub side_open mismatch (STRICT): {captured.get('side_open')!r} != 'BUY'"
            )
        if abs(float(captured.get("entry_price_hint")) - float(mark_price)) > 1e-12:
            raise RuntimeError(
                "stub entry_price_hint mismatch (STRICT): "
                f"{captured.get('entry_price_hint')} != {mark_price}"
            )
        if abs(float(captured.get("qty")) - float(req_preview.qty)) > 1e-12:
            raise RuntimeError(
                f"stub qty mismatch (STRICT): {captured.get('qty')} != {req_preview.qty}"
            )

        trade_symbol = getattr(trade, "symbol", None)
        trade_side = getattr(trade, "side", None)
        trade_entry = getattr(trade, "entry_price", None)
        trade_qty = getattr(trade, "qty", None)
        trade_client_entry_id = getattr(trade, "client_entry_id", None)

        if trade_symbol != symbol:
            raise RuntimeError(
                f"returned trade symbol mismatch (STRICT): {trade_symbol!r} != {symbol!r}"
            )
        if trade_side != "BUY":
            raise RuntimeError(
                f"returned trade side mismatch (STRICT): {trade_side!r} != 'BUY'"
            )
        if abs(float(trade_entry) - float(mark_price)) > 1e-12:
            raise RuntimeError(
                f"returned trade entry mismatch (STRICT): {trade_entry} != {mark_price}"
            )
        if abs(float(trade_qty) - float(req_preview.qty)) > 1e-12:
            raise RuntimeError(
                f"returned trade qty mismatch (STRICT): {trade_qty} != {req_preview.qty}"
            )
        if str(trade_client_entry_id or "").strip() != str(client_entry_id or "").strip():
            raise RuntimeError(
                "returned trade client_entry_id mismatch (STRICT): "
                f"{trade_client_entry_id!r} != {client_entry_id!r}"
            )

        return (
            f"execute ok: trade_symbol={trade_symbol} trade_side={trade_side} "
            f"trade_entry={_pretty(trade_entry)} qty={_pretty(trade_qty)} "
            f"client_entry_id={trade_client_entry_id!r}"
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

    if unexpected:
        print("[RESULT] FAIL")
    else:
        print("[RESULT] PASS")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="run_bot_ws / execution_engine 계약 디버그 테스트"
    )
    parser.add_argument("--symbol", type=str, default="BTCUSDT")
    parser.add_argument("--leverage", type=float, default=10.0)
    parser.add_argument("--risk-pct", type=float, default=0.10)
    parser.add_argument("--equity-usdt", type=float, default=1000.0)
    parser.add_argument("--mark-price", type=float, default=65000.0)
    parser.add_argument("--client-entry-id", type=str, default="debug-client-entry-id")
    parser.add_argument(
        "--skip-run-bot-import",
        action="store_true",
        help="settings/env 의존 때문에 run_bot_ws import 검사만 건너뛴다",
    )
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

    results.append(
        _run_stage(
            "import execution.execution_engine",
            _stage_import_execution_engine,
        )
    )

    if not bool(args.skip_run_bot_import):
        results.append(
            _run_stage(
                "import core.run_bot_ws",
                _stage_import_run_bot_ws,
            )
        )
    else:
        print("[SKIP] import core.run_bot_ws")

    results.append(
        _run_stage(
            "reproduce current run_bot_ws-style signal failure (missing entry_price_hint)",
            lambda: _stage_reproduce_missing_entry_price(
                symbol=symbol,
                leverage=float(args.leverage),
                risk_pct=float(args.risk_pct),
                equity_usdt=float(args.equity_usdt),
            ),
            expect_failure=True,
            expected_message_substring="entry_price_hint",
        )
    )

    results.append(
        _run_stage(
            "normalize enriched signal with mark_price",
            lambda: _stage_normalize_enriched_signal(
                symbol=symbol,
                leverage=float(args.leverage),
                risk_pct=float(args.risk_pct),
                equity_usdt=float(args.equity_usdt),
                mark_price=float(args.mark_price),
                client_entry_id=str(args.client_entry_id).strip() or None,
            ),
        )
    )

    results.append(
        _run_stage(
            "execute enriched signal with stubbed open_position_with_tp_sl",
            lambda: _stage_execute_with_stubbed_open_position(
                symbol=symbol,
                leverage=float(args.leverage),
                risk_pct=float(args.risk_pct),
                equity_usdt=float(args.equity_usdt),
                mark_price=float(args.mark_price),
                client_entry_id=str(args.client_entry_id).strip() or None,
            ),
        )
    )

    _print_summary(results)

    unexpected = [x for x in results if x.status in {"FAIL", "UNEXPECTED_PASS"}]
    raise SystemExit(1 if unexpected else 0)


if __name__ == "__main__":
    main()