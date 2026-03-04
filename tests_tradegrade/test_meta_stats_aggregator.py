"""
========================================================
FILE: test_meta_stats_aggregator.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

설계 원칙:
- 폴백 금지
- 데이터 누락 시 즉시 예외
- 예외 삼키기 금지
- 설정은 settings.py 단일 소스
- DB 접근은 db_core 경유

변경 이력
--------------------------------------------------------
- 2026-03-04:
  1) test_meta_stats_aggregator.py 대상 단위 테스트 추가
  2) 실패/누락/정책 위반 케이스 포함
  3) 외부 I/O(OpenAI/DB) 없이 동작하도록 monkeypatch 사용
========================================================
"""

from __future__ import annotations

import importlib
import sys
import types
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, List

from tests_tradegrade._test_util import expect_raises_any, expect_raises


# --- Dummy SQLAlchemy-like column helpers (minimal) ---
class _Col:
    def __init__(self, name: str):
        self.name = name

    def __eq__(self, other: Any) -> tuple:
        return ("eq", self.name, other)

    def isnot(self, other: Any) -> tuple:
        return ("isnot", self.name, other)

    def desc(self) -> tuple:
        return ("desc", self.name)

    def in_(self, other: Any) -> tuple:
        return ("in", self.name, other)


# --- Dummy state.db_models ---
class TradeORM:
    symbol = _Col("symbol")
    exit_ts = _Col("exit_ts")
    pnl_usdt = _Col("pnl_usdt")


class TradeSnapshot:
    symbol = _Col("symbol")
    trade_id = _Col("trade_id")


@dataclass
class _TradeRow:
    id: int
    symbol: str
    side: str
    entry_ts: datetime
    exit_ts: datetime
    entry_price: float
    exit_price: float
    qty: float
    pnl_usdt: float
    pnl_pct_futures: float | None = None
    leverage: float | None = None
    risk_pct: float | None = None
    tp_pct: float | None = None
    sl_pct: float | None = None
    strategy: str | None = None
    regime_at_entry: str | None = None
    regime_at_exit: str | None = None
    close_reason: str | None = None
    reconciliation_status: str | None = None
    note: str | None = None


@dataclass
class _SnapRow:
    trade_id: int
    symbol: str
    entry_ts: datetime
    direction: str
    signal_source: str | None = None
    regime: str | None = None
    entry_score: float | None = None
    engine_total: float | None = None
    dd_pct: float | None = None
    hour_kst: int | None = None
    weekday_kst: int | None = None
    micro_score_risk: float | None = None
    ev_cell_key: str | None = None
    ev_cell_ev: float | None = None
    ev_cell_n: int | None = None
    ev_cell_status: str | None = None
    auto_blocked: bool | None = None
    auto_risk_multiplier: float | None = None
    gpt_severity: int | None = None
    gpt_tags: Any | None = None
    gpt_confidence_penalty: float | None = None
    gpt_suggested_risk_multiplier: float | None = None


class _FakeQuery:
    def __init__(self, rows: list[Any]):
        self._rows = rows

    def filter(self, *args: Any, **kwargs: Any) -> "_FakeQuery":
        return self

    def order_by(self, *args: Any, **kwargs: Any) -> "_FakeQuery":
        return self

    def limit(self, *args: Any, **kwargs: Any) -> "_FakeQuery":
        return self

    def all(self) -> list[Any]:
        return list(self._rows)


class _FakeSession:
    def __init__(self, trades: list[Any], snaps: list[Any]):
        self._trades = trades
        self._snaps = snaps
        self._mode = None

    def query(self, model: Any) -> _FakeQuery:
        # model에 따라 rows 선택
        if model.__name__ == "TradeORM":
            return _FakeQuery(self._trades)
        if model.__name__ == "TradeSnapshot":
            return _FakeQuery(self._snaps)
        raise RuntimeError("unknown model in test")

    def __enter__(self) -> "_FakeSession":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def _install_dummy_state_modules(fake_session_factory) -> None:
    # state.db_core
    m_db_core = types.ModuleType("state.db_core")
    def get_session():
        return fake_session_factory()
    m_db_core.get_session = get_session  # type: ignore[attr-defined]

    # state.db_models
    m_db_models = types.ModuleType("state.db_models")
    m_db_models.TradeORM = TradeORM  # type: ignore[attr-defined]
    m_db_models.TradeSnapshot = TradeSnapshot  # type: ignore[attr-defined]

    sys.modules["state.db_core"] = m_db_core
    sys.modules["state.db_models"] = m_db_models


def _import_meta_stats(trades: list[_TradeRow], snaps: list[_SnapRow]):
    def factory():
        return _FakeSession(trades, snaps)
    _install_dummy_state_modules(factory)

    if "meta.meta_stats_aggregator" in sys.modules:
        del sys.modules["meta.meta_stats_aggregator"]
    return importlib.import_module("meta.meta_stats_aggregator")


def _make_data(n: int, *, include_test_trade: bool) -> tuple[list[_TradeRow], list[_SnapRow]]:
    sym = "BTCUSDT"
    now = datetime.now(timezone.utc)
    trades: list[_TradeRow] = []
    snaps: list[_SnapRow] = []

    for i in range(n):
        tid = i + 1
        exit_ts = now - timedelta(minutes=i)
        entry_ts = exit_ts - timedelta(minutes=5)
        pnl = [1.0, -0.5, 0.0, 0.2, -0.1][i % 5]
        rs = "TEST_DRY_RUN" if (include_test_trade and i == 0) else "OK"
        note = "TEST_DRY_RUN" if (include_test_trade and i == 0) else ""
        trades.append(_TradeRow(
            id=tid,
            symbol=sym,
            side="BUY",
            entry_ts=entry_ts,
            exit_ts=exit_ts,
            entry_price=100.0,
            exit_price=101.0,
            qty=0.01,
            pnl_usdt=pnl,
            pnl_pct_futures=None,
            leverage=2.0,
            risk_pct=0.2,
            tp_pct=0.01,
            sl_pct=0.01,
            strategy="S1",
            regime_at_entry="RANGE",
            regime_at_exit="RANGE",
            close_reason="TP",
            reconciliation_status=rs,
            note=note,
        ))
        snaps.append(_SnapRow(
            trade_id=tid,
            symbol=sym,
            entry_ts=entry_ts,
            direction="LONG" if (i % 2 == 0) else "SHORT",
            regime="RANGE",
            entry_score=50.0,
            engine_total=60.0,
            dd_pct=1.0,
            micro_score_risk=10.0,
        ))
    return trades, snaps


def test_build_meta_stats_ok() -> None:
    trades, snaps = _make_data(5, include_test_trade=True)
    m = _import_meta_stats(trades, snaps)

    cfg = m.MetaStatsConfig(symbol="BTCUSDT", lookback_trades=10, recent_window=2, baseline_window=2, min_trades_required=4, exclude_test_trades=True)
    d = m.build_meta_stats_dict(cfg)
    assert d["symbol"] == "BTCUSDT"
    assert int(d["n_trades"]) >= 4
    assert "by_direction" in d and isinstance(d["by_direction"], dict)


def test_missing_snapshot_raises() -> None:
    trades, snaps = _make_data(4, include_test_trade=False)
    snaps = snaps[:-1]  # 마지막 snapshot 제거
    m = _import_meta_stats(trades, snaps)

    cfg = m.MetaStatsConfig(symbol="BTCUSDT", lookback_trades=10, recent_window=2, baseline_window=2, min_trades_required=4, exclude_test_trades=False)
    expect_raises(m.MetaStatsError, lambda: m.build_meta_stats_dict(cfg))


def test_db_failure_propagates() -> None:
    # get_session 자체가 터지면 예외가 삼켜지면 안 된다.
    def factory_raises():
        raise RuntimeError("DB down")

    _install_dummy_state_modules(factory_raises)

    if "meta.meta_stats_aggregator" in sys.modules:
        del sys.modules["meta.meta_stats_aggregator"]
    m = importlib.import_module("meta.meta_stats_aggregator")

    cfg = m.MetaStatsConfig(symbol="BTCUSDT", lookback_trades=10, recent_window=2, baseline_window=2, min_trades_required=4, exclude_test_trades=False)
    expect_raises_any(lambda: m.build_meta_stats_dict(cfg))


if __name__ == "__main__":
    test_build_meta_stats_ok()
    test_missing_snapshot_raises()
    test_db_failure_propagates()
    print("OK")
