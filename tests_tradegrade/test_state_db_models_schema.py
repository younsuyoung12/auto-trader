"""
========================================================
FILE: test_state_db_models_schema.py
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
  1) state/db_models.py ORM 스키마 정합 테스트 추가(DB 연결 없이)
  2) 핵심 테이블/컬럼/타입/인덱스/alias(Trade=TradeORM) 검증
  3) timezone-aware, JSONB, Numeric(24,8), onupdate 등 TRADE-GRADE 요구사항 검증
========================================================
"""

from __future__ import annotations

import importlib
import sys
import types
from typing import Iterable, Type

from sqlalchemy import Boolean, DateTime, Float, Index, Integer, Numeric, String, Text
from sqlalchemy.dialects.postgresql import JSON, JSONB
from sqlalchemy.orm import declarative_base


def _expect(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


def _expect_raises(exc_type: Type[BaseException], fn) -> BaseException:
    try:
        fn()
    except exc_type as e:
        return e
    raise AssertionError(f"expected exception {exc_type.__name__} not raised")


def _install_dummy_db_core() -> None:
    """
    STRICT:
    - 이 테스트는 DB/ENV 없이 ORM 정의만 검사해야 한다.
    - state.db_core.Base 를 테스트용 declarative_base()로 대체 주입한다.
    """
    Base = declarative_base()
    m = types.ModuleType("state.db_core")
    m.Base = Base  # type: ignore[attr-defined]
    sys.modules["state.db_core"] = m


def _import_db_models():
    _install_dummy_db_core()

    if "state.db_models" in sys.modules:
        del sys.modules["state.db_models"]

    return importlib.import_module("state.db_models")


def _col(table, name: str):
    c = table.columns.get(name)
    _expect(c is not None, f"missing column: {table.name}.{name}")
    return c


def _assert_datetime_tz(table, name: str, *, nullable: bool | None = None) -> None:
    c = _col(table, name)
    _expect(isinstance(c.type, DateTime), f"{table.name}.{name} must be DateTime")
    _expect(bool(getattr(c.type, "timezone", False)) is True, f"{table.name}.{name} must be timezone-aware (timezone=True)")
    if nullable is not None:
        _expect(bool(c.nullable) == bool(nullable), f"{table.name}.{name} nullable mismatch: {c.nullable} != {nullable}")


def _assert_integer(table, name: str, *, nullable: bool | None = None) -> None:
    c = _col(table, name)
    _expect(isinstance(c.type, Integer), f"{table.name}.{name} must be Integer")
    if nullable is not None:
        _expect(bool(c.nullable) == bool(nullable), f"{table.name}.{name} nullable mismatch: {c.nullable} != {nullable}")


def _assert_boolean(table, name: str, *, nullable: bool | None = None) -> None:
    c = _col(table, name)
    _expect(isinstance(c.type, Boolean), f"{table.name}.{name} must be Boolean")
    if nullable is not None:
        _expect(bool(c.nullable) == bool(nullable), f"{table.name}.{name} nullable mismatch: {c.nullable} != {nullable}")


def _assert_numeric_24_8(table, name: str, *, nullable: bool | None = None) -> None:
    c = _col(table, name)
    _expect(isinstance(c.type, Numeric), f"{table.name}.{name} must be Numeric")
    _expect(int(c.type.precision or 0) == 24, f"{table.name}.{name} precision mismatch: {c.type.precision} != 24")
    _expect(int(c.type.scale or 0) == 8, f"{table.name}.{name} scale mismatch: {c.type.scale} != 8")
    if nullable is not None:
        _expect(bool(c.nullable) == bool(nullable), f"{table.name}.{name} nullable mismatch: {c.nullable} != {nullable}")


def _assert_float(table, name: str, *, nullable: bool | None = None) -> None:
    c = _col(table, name)
    _expect(isinstance(c.type, Float), f"{table.name}.{name} must be Float")
    if nullable is not None:
        _expect(bool(c.nullable) == bool(nullable), f"{table.name}.{name} nullable mismatch: {c.nullable} != {nullable}")


def _assert_string(table, name: str, *, nullable: bool | None = None) -> None:
    c = _col(table, name)
    _expect(isinstance(c.type, String), f"{table.name}.{name} must be String")
    if nullable is not None:
        _expect(bool(c.nullable) == bool(nullable), f"{table.name}.{name} nullable mismatch: {c.nullable} != {nullable}")


def _assert_text(table, name: str, *, nullable: bool | None = None) -> None:
    c = _col(table, name)
    _expect(isinstance(c.type, Text), f"{table.name}.{name} must be Text")
    if nullable is not None:
        _expect(bool(c.nullable) == bool(nullable), f"{table.name}.{name} nullable mismatch: {c.nullable} != {nullable}")


def _assert_json(table, name: str, *, nullable: bool | None = None) -> None:
    c = _col(table, name)
    _expect(isinstance(c.type, JSON), f"{table.name}.{name} must be JSON")
    if nullable is not None:
        _expect(bool(c.nullable) == bool(nullable), f"{table.name}.{name} nullable mismatch: {c.nullable} != {nullable}")


def _assert_jsonb(table, name: str, *, nullable: bool | None = None) -> None:
    c = _col(table, name)
    _expect(isinstance(c.type, JSONB), f"{table.name}.{name} must be JSONB")
    if nullable is not None:
        _expect(bool(c.nullable) == bool(nullable), f"{table.name}.{name} nullable mismatch: {c.nullable} != {nullable}")


def _assert_index(table, index_name: str, cols: Iterable[str], unique: bool) -> None:
    idx = None
    for i in table.indexes:
        if i.name == index_name:
            idx = i
            break
    _expect(idx is not None, f"missing index: {table.name}.{index_name}")
    _expect(bool(idx.unique) == bool(unique), f"index unique mismatch: {index_name} unique={idx.unique} expected={unique}")

    idx_cols = [c.name for c in idx.columns]
    _expect(list(cols) == idx_cols, f"index columns mismatch: {index_name} {idx_cols} expected={list(cols)}")


def test_alias_and_required_classes() -> None:
    m = _import_db_models()

    _expect(hasattr(m, "TradeORM"), "TradeORM missing")
    _expect(hasattr(m, "Trade"), "Trade alias missing")
    _expect(m.Trade is m.TradeORM, "Trade must be alias of TradeORM (STRICT legacy contract)")

    required = [
        "Candle",
        "OrderbookSnapshot",
        "Indicator",
        "RegimeScore",
        "EntryScore",
        "TradeORM",
        "TradeSnapshot",
        "TradeExitSnapshot",
        "FundingRate",
        "ExternalEvent",
    ]
    for cls in required:
        _expect(hasattr(m, cls), f"{cls} missing")


def test_candle_schema() -> None:
    m = _import_db_models()
    t = m.Candle.__table__
    _expect(t.name == "bt_candles", "Candle.__tablename__ mismatch")

    _assert_integer(t, "id", nullable=False)
    _assert_string(t, "symbol", nullable=False)
    _assert_string(t, "timeframe", nullable=False)
    _assert_datetime_tz(t, "ts", nullable=False)

    _assert_numeric_24_8(t, "open", nullable=False)
    _assert_numeric_24_8(t, "high", nullable=False)
    _assert_numeric_24_8(t, "low", nullable=False)
    _assert_numeric_24_8(t, "close", nullable=False)

    _assert_numeric_24_8(t, "volume", nullable=True)
    _assert_numeric_24_8(t, "quote_volume", nullable=True)

    _assert_string(t, "source", nullable=False)
    _assert_datetime_tz(t, "created_at", nullable=False)

    _assert_index(t, "ix_bt_candles_symbol_tf_ts", ["symbol", "timeframe", "ts"], unique=True)


def test_orderbook_snapshot_schema() -> None:
    m = _import_db_models()
    t = m.OrderbookSnapshot.__table__
    _expect(t.name == "bt_orderbook_snapshots", "OrderbookSnapshot.__tablename__ mismatch")

    _assert_integer(t, "id", nullable=False)
    _assert_string(t, "symbol", nullable=False)
    _assert_datetime_tz(t, "ts", nullable=False)

    _assert_numeric_24_8(t, "best_bid", nullable=False)
    _assert_numeric_24_8(t, "best_ask", nullable=False)
    _assert_numeric_24_8(t, "spread", nullable=False)
    _assert_numeric_24_8(t, "depth_imbalance", nullable=True)

    _assert_jsonb(t, "bids_raw", nullable=True)
    _assert_jsonb(t, "asks_raw", nullable=True)

    _assert_datetime_tz(t, "created_at", nullable=False)
    _assert_index(t, "ix_bt_ob_symbol_ts", ["symbol", "ts"], unique=False)


def test_indicator_schema() -> None:
    m = _import_db_models()
    t = m.Indicator.__table__
    _expect(t.name == "bt_indicators", "Indicator.__tablename__ mismatch")

    _assert_integer(t, "id", nullable=False)
    _assert_string(t, "symbol", nullable=False)
    _assert_string(t, "timeframe", nullable=False)
    _assert_datetime_tz(t, "ts", nullable=False)

    # Numeric(24,8) nullable columns
    for k in [
        "rsi",
        "ema_fast",
        "ema_slow",
        "macd",
        "macd_signal",
        "macd_hist",
        "atr",
        "bb_upper",
        "bb_middle",
        "bb_lower",
    ]:
        _assert_numeric_24_8(t, k, nullable=True)

    _assert_jsonb(t, "extra", nullable=True)
    _assert_datetime_tz(t, "created_at", nullable=False)

    _assert_index(t, "ix_bt_indicators_symbol_tf_ts", ["symbol", "timeframe", "ts"], unique=True)


def test_regime_score_schema() -> None:
    m = _import_db_models()
    t = m.RegimeScore.__table__
    _expect(t.name == "bt_regime_scores", "RegimeScore.__tablename__ mismatch")

    _assert_integer(t, "id", nullable=False)
    _assert_string(t, "symbol", nullable=False)
    _assert_datetime_tz(t, "ts", nullable=False)

    # Float nullable scores
    for k in ["trend_score", "range_score", "chop_score", "event_risk_score", "funding_bias_score"]:
        _assert_float(t, k, nullable=True)

    _assert_string(t, "final_regime", nullable=False)
    _assert_json(t, "details_json", nullable=True)

    _assert_datetime_tz(t, "created_at", nullable=False)

    # extended columns (as defined in the file)
    _assert_string(t, "timeframe", nullable=True)
    _assert_string(t, "regime", nullable=True)
    _assert_float(t, "score", nullable=True)
    _assert_jsonb(t, "details", nullable=True)

    _assert_index(t, "ix_bt_regime_symbol_tf_ts", ["symbol", "timeframe", "ts"], unique=False)


def test_entry_score_schema() -> None:
    m = _import_db_models()
    t = m.EntryScore.__table__
    _expect(t.name == "bt_entry_scores", "EntryScore.__tablename__ mismatch")

    _assert_integer(t, "id", nullable=False)
    _assert_integer(t, "trade_id", nullable=True)

    _assert_string(t, "symbol", nullable=False)
    _assert_string(t, "timeframe", nullable=False)
    _assert_datetime_tz(t, "ts", nullable=False)
    _assert_string(t, "direction", nullable=False)

    _assert_numeric_24_8(t, "score", nullable=False)
    _assert_jsonb(t, "features", nullable=True)

    _assert_datetime_tz(t, "created_at", nullable=False)
    _assert_index(t, "ix_bt_entry_symbol_tf_ts_dir", ["symbol", "timeframe", "ts", "direction"], unique=True)


def test_trade_schema_core_and_exec_fields() -> None:
    m = _import_db_models()
    t = m.TradeORM.__table__
    _expect(t.name == "bt_trades", "TradeORM.__tablename__ mismatch")

    _assert_integer(t, "id", nullable=False)
    _assert_string(t, "symbol", nullable=False)
    _assert_string(t, "side", nullable=False)

    _assert_datetime_tz(t, "entry_ts", nullable=False)
    _assert_datetime_tz(t, "exit_ts", nullable=True)

    _assert_numeric_24_8(t, "entry_price", nullable=False)
    _assert_numeric_24_8(t, "exit_price", nullable=True)
    _assert_numeric_24_8(t, "qty", nullable=False)

    _assert_numeric_24_8(t, "pnl_usdt", nullable=True)
    _assert_float(t, "pnl_pct_futures", nullable=True)
    _assert_float(t, "pnl_pct_spot_ref", nullable=True)

    _assert_boolean(t, "is_auto", nullable=False)

    _assert_string(t, "regime_at_entry", nullable=True)
    _assert_string(t, "regime_at_exit", nullable=True)

    _assert_float(t, "entry_score", nullable=True)
    _assert_float(t, "trend_score_at_entry", nullable=True)
    _assert_float(t, "range_score_at_entry", nullable=True)

    _assert_string(t, "strategy", nullable=True)
    _assert_string(t, "close_reason", nullable=True)

    _assert_float(t, "leverage", nullable=True)
    _assert_float(t, "risk_pct", nullable=True)
    _assert_float(t, "tp_pct", nullable=True)
    _assert_float(t, "sl_pct", nullable=True)

    _assert_string(t, "note", nullable=True)

    _assert_datetime_tz(t, "created_at", nullable=False)
    _assert_datetime_tz(t, "updated_at", nullable=False)

    # updated_at onupdate 존재(STRICT 운영 요구)
    c_updated = _col(t, "updated_at")
    _expect(c_updated.onupdate is not None, "bt_trades.updated_at must have onupdate (TRADE-GRADE)")

    # execution/reconciliation fields
    _assert_string(t, "entry_order_id", nullable=True)
    _assert_string(t, "tp_order_id", nullable=True)
    _assert_string(t, "sl_order_id", nullable=True)

    _assert_string(t, "exchange_position_side", nullable=True)
    _assert_numeric_24_8(t, "remaining_qty", nullable=True)
    _assert_numeric_24_8(t, "realized_pnl_usdt", nullable=True)

    _assert_string(t, "reconciliation_status", nullable=True)
    _assert_datetime_tz(t, "last_synced_at", nullable=True)

    _assert_index(t, "ix_bt_trades_symbol_entry_ts", ["symbol", "entry_ts"], unique=False)


def test_trade_snapshot_schema_and_indexes() -> None:
    m = _import_db_models()
    t = m.TradeSnapshot.__table__
    _expect(t.name == "bt_trade_snapshots", "TradeSnapshot.__tablename__ mismatch")

    _assert_integer(t, "id", nullable=False)
    _assert_integer(t, "trade_id", nullable=False)

    _assert_string(t, "symbol", nullable=False)
    _assert_datetime_tz(t, "entry_ts", nullable=False)
    _assert_string(t, "direction", nullable=False)

    _assert_string(t, "signal_source", nullable=True)
    _assert_string(t, "regime", nullable=True)

    _assert_float(t, "entry_score", nullable=True)
    _assert_float(t, "engine_total", nullable=True)

    _assert_float(t, "trend_strength", nullable=True)
    _assert_float(t, "atr_pct", nullable=True)
    _assert_float(t, "volume_zscore", nullable=True)
    _assert_float(t, "depth_ratio", nullable=True)
    _assert_float(t, "spread_pct", nullable=True)

    _assert_integer(t, "hour_kst", nullable=True)
    _assert_integer(t, "weekday_kst", nullable=True)

    _assert_numeric_24_8(t, "last_price", nullable=True)
    _assert_float(t, "risk_pct", nullable=True)
    _assert_float(t, "tp_pct", nullable=True)
    _assert_float(t, "sl_pct", nullable=True)

    _assert_string(t, "gpt_action", nullable=True)
    _assert_text(t, "gpt_reason", nullable=True)

    _assert_datetime_tz(t, "created_at", nullable=False)

    # equity/dd
    _assert_float(t, "equity_current_usdt", nullable=True)
    _assert_float(t, "equity_peak_usdt", nullable=True)
    _assert_float(t, "dd_pct", nullable=True)

    # decision reconciliation
    _assert_string(t, "decision_id", nullable=True)
    _assert_jsonb(t, "quant_decision_pre", nullable=True)
    _assert_jsonb(t, "quant_constraints", nullable=True)
    _assert_string(t, "quant_final_decision", nullable=True)

    _assert_integer(t, "gpt_severity", nullable=True)
    _assert_jsonb(t, "gpt_tags", nullable=True)
    _assert_float(t, "gpt_confidence_penalty", nullable=True)
    _assert_float(t, "gpt_suggested_risk_multiplier", nullable=True)
    _assert_text(t, "gpt_rationale_short", nullable=True)

    # microstructure
    for k in [
        "micro_funding_rate",
        "micro_funding_z",
        "micro_open_interest",
        "micro_oi_z",
        "micro_long_short_ratio",
        "micro_lsr_z",
        "micro_distortion_index",
        "micro_score_risk",
    ]:
        _assert_float(t, k, nullable=True)

    # execution quality
    for k in [
        "exec_expected_price",
        "exec_filled_avg_price",
        "exec_slippage_pct",
        "exec_adverse_move_pct",
        "exec_score",
    ]:
        _assert_float(t, k, nullable=True)
    _assert_jsonb(t, "exec_post_prices", nullable=True)

    # EV / autoblock
    _assert_string(t, "ev_cell_key", nullable=True)
    _assert_float(t, "ev_cell_ev", nullable=True)
    _assert_integer(t, "ev_cell_n", nullable=True)
    _assert_string(t, "ev_cell_status", nullable=True)

    _assert_boolean(t, "auto_blocked", nullable=True)
    _assert_float(t, "auto_risk_multiplier", nullable=True)
    _assert_jsonb(t, "auto_block_reasons", nullable=True)

    _assert_index(t, "ux_bt_trade_snapshots_tradeid", ["trade_id"], unique=True)
    _assert_index(t, "ux_bt_trade_snapshots_decision_id", ["decision_id"], unique=True)
    _assert_index(t, "ix_bt_trade_snapshots_symbol_entryts", ["symbol", "entry_ts"], unique=False)


def test_trade_exit_snapshot_schema_and_indexes() -> None:
    m = _import_db_models()
    t = m.TradeExitSnapshot.__table__
    _expect(t.name == "bt_trade_exit_snapshots", "TradeExitSnapshot.__tablename__ mismatch")

    _assert_integer(t, "id", nullable=False)
    _assert_integer(t, "trade_id", nullable=False)

    _assert_string(t, "symbol", nullable=False)
    _assert_datetime_tz(t, "close_ts", nullable=False)
    _assert_numeric_24_8(t, "close_price", nullable=False)

    _assert_numeric_24_8(t, "pnl", nullable=True)
    _assert_string(t, "close_reason", nullable=True)

    _assert_float(t, "exit_atr_pct", nullable=True)
    _assert_float(t, "exit_trend_strength", nullable=True)
    _assert_float(t, "exit_volume_zscore", nullable=True)
    _assert_float(t, "exit_depth_ratio", nullable=True)

    _assert_datetime_tz(t, "created_at", nullable=False)

    _assert_index(t, "ux_bt_trade_exit_tradeid", ["trade_id"], unique=True)
    _assert_index(t, "ix_bt_trade_exit_symbol_ts", ["symbol", "close_ts"], unique=False)


def test_funding_rate_schema() -> None:
    m = _import_db_models()
    t = m.FundingRate.__table__
    _expect(t.name == "bt_funding_rates", "FundingRate.__tablename__ mismatch")

    _assert_integer(t, "id", nullable=False)
    _assert_string(t, "symbol", nullable=False)
    _assert_datetime_tz(t, "ts", nullable=False)

    _assert_numeric_24_8(t, "rate", nullable=False)
    _assert_numeric_24_8(t, "mark_price", nullable=True)

    _assert_datetime_tz(t, "created_at", nullable=False)
    _assert_index(t, "ix_bt_funding_symbol_ts", ["symbol", "ts"], unique=True)


def test_external_event_schema() -> None:
    m = _import_db_models()
    t = m.ExternalEvent.__table__
    _expect(t.name == "bt_external_events", "ExternalEvent.__tablename__ mismatch")

    _assert_integer(t, "id", nullable=False)
    _assert_datetime_tz(t, "event_ts", nullable=False)
    _assert_string(t, "event_type", nullable=False)
    _assert_string(t, "symbol", nullable=False)

    _assert_string(t, "impact", nullable=False)
    _assert_jsonb(t, "details", nullable=True)

    _assert_datetime_tz(t, "created_at", nullable=False)
    _assert_index(t, "ix_bt_ext_event_ts_type", ["event_ts", "event_type"], unique=False)


if __name__ == "__main__":
    # 단독 실행 모드: 모든 테스트 실행
    test_alias_and_required_classes()
    test_candle_schema()
    test_orderbook_snapshot_schema()
    test_indicator_schema()
    test_regime_score_schema()
    test_entry_score_schema()
    test_trade_schema_core_and_exec_fields()
    test_trade_snapshot_schema_and_indexes()
    test_trade_exit_snapshot_schema_and_indexes()
    test_funding_rate_schema()
    test_external_event_schema()
    print("OK")