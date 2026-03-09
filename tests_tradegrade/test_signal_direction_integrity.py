"""
========================================================
FILE: tests_tradegrade/test_signal_direction_integrity.py
AUTO-TRADER — SIGNAL DIRECTION INTEGRITY TEST
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

핵심 변경 요약
- build_signal()에 equity_current_usdt 추가
- execution_engine 정규화 시 qty 계산용 capital source 누락 문제 해결
- LONG / SHORT ↔ BUY / SELL 방향 계약 무결성 검증 유지
- signal/meta 방향 충돌 시 즉시 실패 검증 유지
- execute 단계까지 LONG / SHORT 방향 유지 검증 유지

코드 정리 내용
- 테스트용 Signal payload 필수 자본 필드 보강
- 실제 주문 없이 open_position_with_tp_sl 스텁 유지
- execution_engine.Trade → FakeTrade 패치 유지
- stage별 PASS / EXPECTED_FAIL / FAIL 출력 유지

설계 원칙
- STRICT · NO-FALLBACK
- 실제 주문 금지
- 테스트 실패 시 원인 즉시 노출
- 방향 뒤집힘 및 signal/meta 충돌은 즉시 실패

변경 이력
--------------------------------------------------------
- 2026-03-10:
  1) FIX: build_signal()에 equity_current_usdt 추가
  2) FIX: direction matrix / execute LONG / execute SHORT 단계의 capital source 누락 오류 제거
========================================================
"""

from __future__ import annotations

import sys
import time
import traceback
import importlib
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Optional


# -------------------------------------------------------
# repo root
# -------------------------------------------------------

def _find_repo_root_or_raise() -> Path:
    base = Path.cwd().resolve()
    for p in [base, *base.parents]:
        if (
            (p / "core" / "run_bot_ws.py").exists()
            and (p / "execution" / "execution_engine.py").exists()
        ):
            return p
    raise RuntimeError("repo root not found (STRICT)")


REPO_ROOT = _find_repo_root_or_raise()

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


# -------------------------------------------------------
# result
# -------------------------------------------------------

@dataclass(slots=True)
class StageResult:
    name: str
    ok: bool
    expected_failure: bool
    detail: str

    @property
    def status(self) -> str:
        if self.ok and not self.expected_failure:
            return "PASS"
        if self.ok and self.expected_failure:
            return "EXPECTED_FAIL"
        if (not self.ok) and self.expected_failure:
            return "UNEXPECTED_PASS"
        return "FAIL"


# -------------------------------------------------------
# fake trade
# -------------------------------------------------------

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


# -------------------------------------------------------
# util
# -------------------------------------------------------

def banner(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def run_stage(name: str, fn, expect_failure: bool = False) -> StageResult:
    banner(name)

    try:
        r = fn()

        if expect_failure:
            return StageResult(name, False, True, "unexpected pass")

        print("[PASS]", r)
        return StageResult(name, True, False, str(r))

    except Exception as e:
        print(traceback.format_exc())

        if expect_failure:
            return StageResult(name, True, True, str(e))

        return StageResult(name, False, False, str(e))


# -------------------------------------------------------
# helpers
# -------------------------------------------------------

def build_settings(symbol: str, leverage: float):
    return SimpleNamespace(
        symbol=symbol,
        leverage=leverage,
    )


def build_signal(direction: str, symbol: str, price: float):
    equity_current_usdt = 1000.0

    return {
        "action": "ENTER",
        "direction": direction,
        "tp_pct": 0.0125,
        "sl_pct": 0.006,
        "risk_pct": 0.1,
        "reason": "direction_test",
        "guard_adjustments": {},
        "meta": {
            "symbol": symbol,
            "mark_price": price,
            "equity_current_usdt": equity_current_usdt,
            "entry_client_order_id": "direction-test",
        },
    }


# -------------------------------------------------------
# stages
# -------------------------------------------------------

def stage_import():
    ee = importlib.import_module("execution.execution_engine")
    rb = importlib.import_module("core.run_bot_ws")

    if getattr(ee, "ExecutionEngine", None) is None:
        raise RuntimeError("ExecutionEngine missing (STRICT)")
    if rb is None:
        raise RuntimeError("core.run_bot_ws import failed (STRICT)")

    return "imports ok"


def stage_direction_matrix():
    ee = importlib.import_module("execution.execution_engine")

    settings = build_settings("BTCUSDT", 10)

    cases = [
        ("LONG", "BUY"),
        ("SHORT", "SELL"),
        ("BUY", "BUY"),
        ("SELL", "SELL"),
    ]

    result = []

    for raw, expected in cases:
        signal = build_signal(raw, "BTCUSDT", 65000.0)
        req = ee._normalize_entry_request_strict(signal, settings)

        if req.side_open != expected:
            raise RuntimeError(
                f"direction mismatch {raw} -> {req.side_open} expected {expected} (STRICT)"
            )

        result.append(f"{raw}->{req.side_open}")

    return " / ".join(result)


def stage_meta_conflict():
    ee = importlib.import_module("execution.execution_engine")

    settings = build_settings("BTCUSDT", 10)
    signal = build_signal("LONG", "BTCUSDT", 65000.0)

    signal["meta"]["direction"] = "SHORT"

    ee._normalize_entry_request_strict(signal, settings)

    return "unexpected pass"


def stage_execute(direction: str):
    ee = importlib.import_module("execution.execution_engine")

    settings = build_settings("BTCUSDT", 10)
    signal = build_signal(direction, "BTCUSDT", 65000.0)

    req = ee._normalize_entry_request_strict(signal, settings)

    if req.symbol != "BTCUSDT":
        raise RuntimeError(f"normalized symbol mismatch: {req.symbol} (STRICT)")

    original_open = ee.open_position_with_tp_sl
    original_trade = ee.Trade

    try:
        ee.Trade = FakeTrade

        def stub(
            settings,
            *,
            symbol,
            side_open,
            qty,
            entry_price_hint,
            tp_pct,
            sl_pct,
            source,
            soft_mode,
            sl_floor_ratio,
            available_usdt,
            entry_client_order_id,
        ):
            return FakeTrade(
                symbol=symbol,
                side=side_open,
                qty=qty,
                entry_price=entry_price_hint,
                entry_order_id="test",
                tp_order_id="tp",
                sl_order_id="sl",
                entry_ts=datetime.now(timezone.utc),
                reconciliation_status="PROTECTION_VERIFIED",
                client_entry_id=entry_client_order_id,
            )

        ee.open_position_with_tp_sl = stub

        engine = ee.ExecutionEngine(settings)
        trade = engine.execute(signal)

        expected = "BUY" if direction in ("LONG", "BUY") else "SELL"

        if trade.side != expected:
            raise RuntimeError(
                f"side mismatch: trade.side={trade.side} expected={expected} (STRICT)"
            )

        if trade.symbol != "BTCUSDT":
            raise RuntimeError(
                f"symbol mismatch: trade.symbol={trade.symbol} expected=BTCUSDT (STRICT)"
            )

        return f"{direction}->{trade.side}"

    finally:
        ee.open_position_with_tp_sl = original_open
        ee.Trade = original_trade


# -------------------------------------------------------
# summary
# -------------------------------------------------------

def print_summary(results) -> None:
    banner("SUMMARY")

    unexpected = []

    for r in results:
        print(f"{r.status:<15} | {r.name:<35} | {r.detail}")
        if r.status in ("FAIL", "UNEXPECTED_PASS"):
            unexpected.append(r)

    print("-" * 80)

    print(
        f"PASS={sum(x.status == 'PASS' for x in results)} "
        f"EXPECTED_FAIL={sum(x.status == 'EXPECTED_FAIL' for x in results)} "
        f"UNEXPECTED={len(unexpected)}"
    )

    if unexpected:
        print("[RESULT] FAIL")
    else:
        print("[RESULT] PASS")


# -------------------------------------------------------
# main
# -------------------------------------------------------

def main() -> None:
    banner("BOOT")

    print("repo_root=", REPO_ROOT)
    print("python=", sys.executable)
    print("epoch=", time.time())

    results = []

    results.append(run_stage("import modules", stage_import))
    results.append(run_stage("direction matrix", stage_direction_matrix))
    results.append(
        run_stage(
            "signal/meta conflict reject",
            stage_meta_conflict,
            expect_failure=True,
        )
    )
    results.append(
        run_stage(
            "execute LONG",
            lambda: stage_execute("LONG"),
        )
    )
    results.append(
        run_stage(
            "execute SHORT",
            lambda: stage_execute("SHORT"),
        )
    )

    print_summary(results)


if __name__ == "__main__":
    main()