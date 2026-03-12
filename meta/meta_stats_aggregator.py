"""
========================================================
FILE: meta/meta_stats_aggregator.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할
- DB(bt_trades, bt_trade_snapshots)를 기반으로 “메타 전략(레벨3)” 입력용 통계를 산출한다.
- 이 모듈은 "분석/집계"만 한다. 주문/진입/청산/전략 결정은 하지 않는다.

절대 원칙 (STRICT · NO-FALLBACK)
- DB 조회 실패/스키마 불일치/필수 필드 누락 시 즉시 예외.
- None→0, 빈값 대체, 조용한 skip/continue 금지.
- 민감정보(DB URL 등)는 예외 메시지/로그에 포함하지 않는다.
- DB 접근은 state.db_core.get_session() 단일 경로.
- 공통 STRICT 예외 계층을 사용한다.

변경 이력
--------------------------------------------------------
- 2026-03-13:
  1) FIX(EXCEPTION): common.exceptions_strict 공통 예외 계층 적용
  2) FIX(DB): DB 조회/ORM 접근 실패를 MetaStatsDBError로 분리
  3) FIX(STRICT): regime/source/direction/symbol 계약 검증 강화
  4) FIX(NO-FALLBACK): UNKNOWN 대체 제거, 필수 regime 누락 시 즉시 예외
  5) FEAT(CONTRACT): MetaStatsConfig / MetaStatsResult dataclass 자체 검증 추가
- 2026-03-05:
  1) FIX(TRADE-GRADE): DetachedInstanceError 근본 해결
     - ORM 객체(TradeORM/TradeSnapshot)를 Session 종료 이후 접근하지 않도록 수정
     - ORM → MetaTradeRow/MetaSnapshotRow 변환을 반드시 get_session() 내부에서 완료
  2) 잘못된 __dict__.copy() 기반 변환 제거(사용 금지)
- 2026-03-03:
  1) 신규 생성: Meta Strategy(레벨3)용 통계 집계기
  2) bt_trades + bt_trade_snapshots 조인 기반:
     - 최근 N 트레이드 요약, 레짐/방향별 성과, 스코어 버킷, 드리프트 지표 제공
  3) STRICT 보장:
     - 스냅샷 누락/필수값 누락/NaN/INF 즉시 예외
========================================================
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from common.exceptions_strict import (
    StrictConfigError,
    StrictDBError,
    StrictDataError,
    StrictStateError,
)
from state.db_core import get_session
from state.db_models import TradeORM, TradeSnapshot


# ─────────────────────────────────────────────
# Exceptions
# ─────────────────────────────────────────────
class MetaStatsError(RuntimeError):
    """STRICT meta stats aggregation base error."""


class MetaStatsConfigError(StrictConfigError):
    """설정 계약 위반."""


class MetaStatsContractError(StrictDataError):
    """입력/행/집계 payload 계약 위반."""


class MetaStatsDBError(StrictDBError):
    """DB 조회/ORM 접근/세션 처리 실패."""


class MetaStatsStateError(StrictStateError):
    """집계 내부 상태/불변식 위반."""


# ─────────────────────────────────────────────
# Strict helpers
# ─────────────────────────────────────────────
def _fail_config(reason: str, exc: Optional[BaseException] = None) -> None:
    if exc is None:
        raise MetaStatsConfigError(reason)
    raise MetaStatsConfigError(reason) from exc


def _fail_contract(reason: str, exc: Optional[BaseException] = None) -> None:
    if exc is None:
        raise MetaStatsContractError(reason)
    raise MetaStatsContractError(reason) from exc


def _fail_db(reason: str, exc: Optional[BaseException] = None) -> None:
    if exc is None:
        raise MetaStatsDBError(reason)
    raise MetaStatsDBError(reason) from exc


def _fail_state(reason: str, exc: Optional[BaseException] = None) -> None:
    if exc is None:
        raise MetaStatsStateError(reason)
    raise MetaStatsStateError(reason) from exc


def _normalize_symbol_strict(v: Any, name: str) -> str:
    s = str(v or "").replace("-", "").replace("/", "").replace("_", "").upper().strip()
    if not s:
        _fail_contract(f"{name} is required (STRICT)")
    return s


def _require_nonempty_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        _fail_contract(f"{name} is required (STRICT)")
    return s


def _require_optional_nonempty_str(v: Any, name: str) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        _fail_contract(f"{name} must not be empty when provided (STRICT)")
    return s


def _normalize_direction_strict(v: Any, name: str) -> str:
    s = _require_nonempty_str(v, name).upper()
    if s in ("BUY", "LONG"):
        return "LONG"
    if s in ("SELL", "SHORT"):
        return "SHORT"
    _fail_contract(f"{name} invalid direction (STRICT): {s!r}")
    raise AssertionError("unreachable")


def _require_tz_aware_dt(v: Any, name: str) -> datetime:
    if not isinstance(v, datetime):
        _fail_contract(f"{name} must be datetime (STRICT)")
    if v.tzinfo is None or v.tzinfo.utcoffset(v) is None:
        _fail_contract(f"{name} must be tz-aware datetime (STRICT)")
    return v


def _to_float_strict(v: Any, name: str) -> float:
    if v is None:
        _fail_contract(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        _fail_contract(f"{name} must be numeric (bool not allowed) (STRICT)")

    if isinstance(v, Decimal):
        fv = float(v)
    else:
        try:
            fv = float(v)
        except Exception as e:
            _fail_contract(f"{name} must be numeric (STRICT)", e)

    if not math.isfinite(fv):
        _fail_contract(f"{name} must be finite (STRICT): {fv!r}")
    return float(fv)


def _to_int_strict(v: Any, name: str) -> int:
    if v is None:
        _fail_contract(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        _fail_contract(f"{name} must be int (bool not allowed) (STRICT)")
    try:
        iv = int(v)
    except Exception as e:
        _fail_contract(f"{name} must be int (STRICT)", e)
    return int(iv)


def _mean(values: Sequence[float], name: str) -> float:
    if not values:
        _fail_state(f"{name} empty (STRICT)")
    s = float(sum(values))
    n = int(len(values))
    out = s / float(n)
    if not math.isfinite(out):
        _fail_state(f"{name} mean not finite (STRICT)")
    return float(out)


def _pct(numer: float, denom: float, name: str) -> float:
    if denom <= 0 or not math.isfinite(denom):
        _fail_state(f"{name} denom invalid (STRICT)")
    out = (numer / denom) * 100.0
    if not math.isfinite(out):
        _fail_state(f"{name} percent not finite (STRICT)")
    return float(out)


def _bucket_0_100(v: float, *, step: int = 10) -> str:
    if step <= 0:
        _fail_state("bucket step must be > 0 (STRICT)")
    if v < 0 or v > 100 or not math.isfinite(v):
        _fail_contract(f"score out of range 0..100 (STRICT): {v}")
    lo = int(v // step) * step
    hi = min(lo + step, 100)
    if hi == lo:
        hi = min(lo + step, 100)
    return f"{lo:02d}-{hi:02d}"


def _resolve_regime_strict(t: "MetaTradeRow", s: "MetaSnapshotRow") -> str:
    if s.regime is not None:
        return s.regime
    if t.regime_at_entry is not None:
        return t.regime_at_entry
    _fail_contract(f"regime missing for trade_id={t.trade_id} (STRICT)")
    raise AssertionError("unreachable")


# ─────────────────────────────────────────────
# Data models
# ─────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class MetaTradeRow:
    trade_id: int
    symbol: str
    side: str
    entry_ts: datetime
    exit_ts: datetime
    entry_price: float
    exit_price: float
    qty: float
    pnl_usdt: float
    pnl_pct_futures: Optional[float]
    leverage: Optional[float]
    risk_pct: Optional[float]
    tp_pct: Optional[float]
    sl_pct: Optional[float]
    strategy: Optional[str]
    regime_at_entry: Optional[str]
    regime_at_exit: Optional[str]
    close_reason: Optional[str]
    reconciliation_status: Optional[str]
    note: Optional[str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "trade_id", _to_int_strict(self.trade_id, "MetaTradeRow.trade_id"))
        object.__setattr__(self, "symbol", _normalize_symbol_strict(self.symbol, "MetaTradeRow.symbol"))
        object.__setattr__(self, "side", _normalize_direction_strict(self.side, "MetaTradeRow.side"))
        object.__setattr__(self, "entry_ts", _require_tz_aware_dt(self.entry_ts, "MetaTradeRow.entry_ts"))
        object.__setattr__(self, "exit_ts", _require_tz_aware_dt(self.exit_ts, "MetaTradeRow.exit_ts"))
        object.__setattr__(self, "entry_price", _to_float_strict(self.entry_price, "MetaTradeRow.entry_price"))
        object.__setattr__(self, "exit_price", _to_float_strict(self.exit_price, "MetaTradeRow.exit_price"))
        object.__setattr__(self, "qty", _to_float_strict(self.qty, "MetaTradeRow.qty"))
        object.__setattr__(self, "pnl_usdt", _to_float_strict(self.pnl_usdt, "MetaTradeRow.pnl_usdt"))

        if self.trade_id <= 0:
            _fail_contract("MetaTradeRow.trade_id must be > 0 (STRICT)")
        if self.entry_price <= 0 or self.exit_price <= 0 or self.qty <= 0:
            _fail_contract("MetaTradeRow entry_price/exit_price/qty must be > 0 (STRICT)")
        if self.exit_ts < self.entry_ts:
            _fail_contract("MetaTradeRow.exit_ts must be >= entry_ts (STRICT)")

        if self.pnl_pct_futures is not None:
            object.__setattr__(self, "pnl_pct_futures", _to_float_strict(self.pnl_pct_futures, "MetaTradeRow.pnl_pct_futures"))
        if self.leverage is not None:
            object.__setattr__(self, "leverage", _to_float_strict(self.leverage, "MetaTradeRow.leverage"))
        if self.risk_pct is not None:
            object.__setattr__(self, "risk_pct", _to_float_strict(self.risk_pct, "MetaTradeRow.risk_pct"))
        if self.tp_pct is not None:
            object.__setattr__(self, "tp_pct", _to_float_strict(self.tp_pct, "MetaTradeRow.tp_pct"))
        if self.sl_pct is not None:
            object.__setattr__(self, "sl_pct", _to_float_strict(self.sl_pct, "MetaTradeRow.sl_pct"))

        object.__setattr__(self, "strategy", _require_optional_nonempty_str(self.strategy, "MetaTradeRow.strategy"))
        object.__setattr__(self, "regime_at_entry", _require_optional_nonempty_str(self.regime_at_entry, "MetaTradeRow.regime_at_entry"))
        object.__setattr__(self, "regime_at_exit", _require_optional_nonempty_str(self.regime_at_exit, "MetaTradeRow.regime_at_exit"))
        object.__setattr__(self, "close_reason", _require_optional_nonempty_str(self.close_reason, "MetaTradeRow.close_reason"))
        object.__setattr__(self, "reconciliation_status", _require_optional_nonempty_str(self.reconciliation_status, "MetaTradeRow.reconciliation_status"))
        object.__setattr__(self, "note", _require_optional_nonempty_str(self.note, "MetaTradeRow.note"))


@dataclass(frozen=True, slots=True)
class MetaSnapshotRow:
    trade_id: int
    symbol: str
    entry_ts: datetime
    direction: str
    signal_source: Optional[str]
    regime: Optional[str]
    entry_score: Optional[float]
    engine_total: Optional[float]
    dd_pct: Optional[float]
    hour_kst: Optional[int]
    weekday_kst: Optional[int]
    micro_score_risk: Optional[float]
    ev_cell_key: Optional[str]
    ev_cell_ev: Optional[float]
    ev_cell_n: Optional[int]
    ev_cell_status: Optional[str]
    auto_blocked: Optional[bool]
    auto_risk_multiplier: Optional[float]
    gpt_severity: Optional[int]
    gpt_tags: Optional[Any]
    gpt_confidence_penalty: Optional[float]
    gpt_suggested_risk_multiplier: Optional[float]

    def __post_init__(self) -> None:
        object.__setattr__(self, "trade_id", _to_int_strict(self.trade_id, "MetaSnapshotRow.trade_id"))
        object.__setattr__(self, "symbol", _normalize_symbol_strict(self.symbol, "MetaSnapshotRow.symbol"))
        object.__setattr__(self, "entry_ts", _require_tz_aware_dt(self.entry_ts, "MetaSnapshotRow.entry_ts"))
        object.__setattr__(self, "direction", _normalize_direction_strict(self.direction, "MetaSnapshotRow.direction"))

        if self.trade_id <= 0:
            _fail_contract("MetaSnapshotRow.trade_id must be > 0 (STRICT)")

        object.__setattr__(self, "signal_source", _require_optional_nonempty_str(self.signal_source, "MetaSnapshotRow.signal_source"))
        object.__setattr__(self, "regime", _require_optional_nonempty_str(self.regime, "MetaSnapshotRow.regime"))
        object.__setattr__(self, "ev_cell_key", _require_optional_nonempty_str(self.ev_cell_key, "MetaSnapshotRow.ev_cell_key"))
        object.__setattr__(self, "ev_cell_status", _require_optional_nonempty_str(self.ev_cell_status, "MetaSnapshotRow.ev_cell_status"))

        if self.entry_score is not None:
            object.__setattr__(self, "entry_score", _to_float_strict(self.entry_score, "MetaSnapshotRow.entry_score"))
        if self.engine_total is not None:
            object.__setattr__(self, "engine_total", _to_float_strict(self.engine_total, "MetaSnapshotRow.engine_total"))
        if self.dd_pct is not None:
            object.__setattr__(self, "dd_pct", _to_float_strict(self.dd_pct, "MetaSnapshotRow.dd_pct"))
        if self.hour_kst is not None:
            object.__setattr__(self, "hour_kst", _to_int_strict(self.hour_kst, "MetaSnapshotRow.hour_kst"))
        if self.weekday_kst is not None:
            object.__setattr__(self, "weekday_kst", _to_int_strict(self.weekday_kst, "MetaSnapshotRow.weekday_kst"))
        if self.micro_score_risk is not None:
            object.__setattr__(self, "micro_score_risk", _to_float_strict(self.micro_score_risk, "MetaSnapshotRow.micro_score_risk"))
        if self.ev_cell_ev is not None:
            object.__setattr__(self, "ev_cell_ev", _to_float_strict(self.ev_cell_ev, "MetaSnapshotRow.ev_cell_ev"))
        if self.ev_cell_n is not None:
            object.__setattr__(self, "ev_cell_n", _to_int_strict(self.ev_cell_n, "MetaSnapshotRow.ev_cell_n"))
        if self.auto_risk_multiplier is not None:
            object.__setattr__(self, "auto_risk_multiplier", _to_float_strict(self.auto_risk_multiplier, "MetaSnapshotRow.auto_risk_multiplier"))
        if self.gpt_severity is not None:
            object.__setattr__(self, "gpt_severity", _to_int_strict(self.gpt_severity, "MetaSnapshotRow.gpt_severity"))
        if self.gpt_confidence_penalty is not None:
            object.__setattr__(self, "gpt_confidence_penalty", _to_float_strict(self.gpt_confidence_penalty, "MetaSnapshotRow.gpt_confidence_penalty"))
        if self.gpt_suggested_risk_multiplier is not None:
            object.__setattr__(self, "gpt_suggested_risk_multiplier", _to_float_strict(self.gpt_suggested_risk_multiplier, "MetaSnapshotRow.gpt_suggested_risk_multiplier"))
        if self.auto_blocked is not None and not isinstance(self.auto_blocked, bool):
            _fail_contract("MetaSnapshotRow.auto_blocked must be bool when provided (STRICT)")


@dataclass(frozen=True, slots=True)
class MetaStatsConfig:
    symbol: str
    lookback_trades: int = 200
    recent_window: int = 20
    baseline_window: int = 60
    min_trades_required: int = 20
    exclude_test_trades: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _normalize_symbol_strict(self.symbol, "cfg.symbol"))
        object.__setattr__(self, "lookback_trades", _to_int_strict(self.lookback_trades, "cfg.lookback_trades"))
        object.__setattr__(self, "recent_window", _to_int_strict(self.recent_window, "cfg.recent_window"))
        object.__setattr__(self, "baseline_window", _to_int_strict(self.baseline_window, "cfg.baseline_window"))
        object.__setattr__(self, "min_trades_required", _to_int_strict(self.min_trades_required, "cfg.min_trades_required"))
        if not isinstance(self.exclude_test_trades, bool):
            _fail_config("cfg.exclude_test_trades must be bool (STRICT)")

        if self.lookback_trades <= 0:
            _fail_config("cfg.lookback_trades must be > 0 (STRICT)")
        if self.recent_window <= 0:
            _fail_config("cfg.recent_window must be > 0 (STRICT)")
        if self.baseline_window <= 0:
            _fail_config("cfg.baseline_window must be > 0 (STRICT)")
        if self.min_trades_required <= 0:
            _fail_config("cfg.min_trades_required must be > 0 (STRICT)")


@dataclass(frozen=True, slots=True)
class MetaStatsResult:
    symbol: str
    generated_at_utc: datetime

    n_trades: int
    wins: int
    losses: int
    breakevens: int
    win_rate_pct: float
    total_pnl_usdt: float
    avg_pnl_usdt: float
    avg_pnl_pct_futures: Optional[float]

    recent_win_rate_pct: float
    baseline_win_rate_pct: float
    recent_avg_pnl_usdt: float
    baseline_avg_pnl_usdt: float

    by_direction: Dict[str, Dict[str, Any]]
    by_regime: Dict[str, Dict[str, Any]]
    by_engine_total_bucket: Dict[str, Dict[str, Any]]
    by_entry_score_bucket: Dict[str, Dict[str, Any]]

    recent_trades: List[Dict[str, Any]]

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _normalize_symbol_strict(self.symbol, "result.symbol"))
        object.__setattr__(self, "generated_at_utc", _require_tz_aware_dt(self.generated_at_utc, "result.generated_at_utc"))

        for name in ("n_trades", "wins", "losses", "breakevens"):
            object.__setattr__(self, name, _to_int_strict(getattr(self, name), f"result.{name}"))
        if self.n_trades <= 0:
            _fail_state("result.n_trades must be > 0 (STRICT)")
        if (self.wins + self.losses + self.breakevens) != self.n_trades:
            _fail_state("wins+losses+breakevens must equal n_trades (STRICT)")

        object.__setattr__(self, "win_rate_pct", _to_float_strict(self.win_rate_pct, "result.win_rate_pct"))
        object.__setattr__(self, "total_pnl_usdt", _to_float_strict(self.total_pnl_usdt, "result.total_pnl_usdt"))
        object.__setattr__(self, "avg_pnl_usdt", _to_float_strict(self.avg_pnl_usdt, "result.avg_pnl_usdt"))
        object.__setattr__(self, "recent_win_rate_pct", _to_float_strict(self.recent_win_rate_pct, "result.recent_win_rate_pct"))
        object.__setattr__(self, "baseline_win_rate_pct", _to_float_strict(self.baseline_win_rate_pct, "result.baseline_win_rate_pct"))
        object.__setattr__(self, "recent_avg_pnl_usdt", _to_float_strict(self.recent_avg_pnl_usdt, "result.recent_avg_pnl_usdt"))
        object.__setattr__(self, "baseline_avg_pnl_usdt", _to_float_strict(self.baseline_avg_pnl_usdt, "result.baseline_avg_pnl_usdt"))

        if self.avg_pnl_pct_futures is not None:
            object.__setattr__(self, "avg_pnl_pct_futures", _to_float_strict(self.avg_pnl_pct_futures, "result.avg_pnl_pct_futures"))

        for pct_name in ("win_rate_pct", "recent_win_rate_pct", "baseline_win_rate_pct"):
            pct_val = float(getattr(self, pct_name))
            if pct_val < 0.0 or pct_val > 100.0:
                _fail_state(f"result.{pct_name} out of range 0..100 (STRICT)")

        for name in ("by_direction", "by_regime", "by_engine_total_bucket", "by_entry_score_bucket"):
            value = getattr(self, name)
            if not isinstance(value, dict) or not value:
                _fail_state(f"result.{name} must be non-empty dict (STRICT)")

        if not isinstance(self.recent_trades, list) or not self.recent_trades:
            _fail_state("result.recent_trades must be non-empty list (STRICT)")


# ─────────────────────────────────────────────
# Fetch layer (STRICT)
# ─────────────────────────────────────────────
def _is_test_trade_strict(t: MetaTradeRow) -> bool:
    rs = (t.reconciliation_status or "").strip().upper()
    note = (t.note or "").strip().upper()
    if rs == "TEST_DRY_RUN":
        return True
    if note.startswith("TEST"):
        return True
    return False


def _fetch_closed_trades_strict(cfg: MetaStatsConfig) -> List[MetaTradeRow]:
    sym = _normalize_symbol_strict(cfg.symbol, "cfg.symbol")
    if cfg.lookback_trades <= 0:
        _fail_config("cfg.lookback_trades must be > 0 (STRICT)")

    out: List[MetaTradeRow] = []
    try:
        with get_session() as session:
            q = (
                session.query(TradeORM)
                .filter(TradeORM.symbol == sym)
                .filter(TradeORM.exit_ts.isnot(None))
                .filter(TradeORM.pnl_usdt.isnot(None))
                .order_by(TradeORM.exit_ts.desc())
                .limit(int(cfg.lookback_trades))
            )
            rows = q.all()

            if not rows:
                _fail_db(f"no closed trades found for symbol={sym} (STRICT)")

            for r in rows:
                tid = _to_int_strict(getattr(r, "id", None), "bt_trades.id")
                entry_ts = _require_tz_aware_dt(getattr(r, "entry_ts", None), "bt_trades.entry_ts")
                exit_ts = _require_tz_aware_dt(getattr(r, "exit_ts", None), "bt_trades.exit_ts")
                entry_price = _to_float_strict(getattr(r, "entry_price", None), "bt_trades.entry_price")
                exit_price = _to_float_strict(getattr(r, "exit_price", None), "bt_trades.exit_price")
                qty = _to_float_strict(getattr(r, "qty", None), "bt_trades.qty")
                pnl_usdt = _to_float_strict(getattr(r, "pnl_usdt", None), "bt_trades.pnl_usdt")

                pnl_pct_futures = getattr(r, "pnl_pct_futures", None)
                if pnl_pct_futures is not None:
                    pnl_pct_futures = _to_float_strict(pnl_pct_futures, "bt_trades.pnl_pct_futures")

                side = _normalize_direction_strict(getattr(r, "side", None), "bt_trades.side")

                leverage = getattr(r, "leverage", None)
                if leverage is not None:
                    leverage = _to_float_strict(leverage, "bt_trades.leverage")

                risk_pct = getattr(r, "risk_pct", None)
                if risk_pct is not None:
                    risk_pct = _to_float_strict(risk_pct, "bt_trades.risk_pct")

                tp_pct = getattr(r, "tp_pct", None)
                if tp_pct is not None:
                    tp_pct = _to_float_strict(tp_pct, "bt_trades.tp_pct")

                sl_pct = getattr(r, "sl_pct", None)
                if sl_pct is not None:
                    sl_pct = _to_float_strict(sl_pct, "bt_trades.sl_pct")

                out.append(
                    MetaTradeRow(
                        trade_id=int(tid),
                        symbol=sym,
                        side=side,
                        entry_ts=entry_ts,
                        exit_ts=exit_ts,
                        entry_price=float(entry_price),
                        exit_price=float(exit_price),
                        qty=float(qty),
                        pnl_usdt=float(pnl_usdt),
                        pnl_pct_futures=pnl_pct_futures,
                        leverage=leverage,
                        risk_pct=risk_pct,
                        tp_pct=tp_pct,
                        sl_pct=sl_pct,
                        strategy=_require_optional_nonempty_str(getattr(r, "strategy", None), "bt_trades.strategy"),
                        regime_at_entry=_require_optional_nonempty_str(getattr(r, "regime_at_entry", None), "bt_trades.regime_at_entry"),
                        regime_at_exit=_require_optional_nonempty_str(getattr(r, "regime_at_exit", None), "bt_trades.regime_at_exit"),
                        close_reason=_require_optional_nonempty_str(getattr(r, "close_reason", None), "bt_trades.close_reason"),
                        reconciliation_status=_require_optional_nonempty_str(getattr(r, "reconciliation_status", None), "bt_trades.reconciliation_status"),
                        note=_require_optional_nonempty_str(getattr(r, "note", None), "bt_trades.note"),
                    )
                )
    except MetaStatsError:
        raise
    except Exception as e:
        _fail_db("failed while fetching closed trades (STRICT)", e)

    if cfg.exclude_test_trades:
        filtered = [t for t in out if not _is_test_trade_strict(t)]
        if not filtered:
            _fail_state("all trades were filtered as test trades (STRICT)")
        return filtered

    return out


def _fetch_snapshots_by_trade_ids_strict(symbol: str, trade_ids: Sequence[int]) -> Dict[int, MetaSnapshotRow]:
    sym = _normalize_symbol_strict(symbol, "symbol")
    if not trade_ids:
        _fail_contract("trade_ids empty (STRICT)")

    ids = sorted({int(x) for x in trade_ids if int(x) > 0})
    if not ids:
        _fail_contract("trade_ids invalid (STRICT)")

    by_id: Dict[int, MetaSnapshotRow] = {}
    try:
        with get_session() as session:
            q = (
                session.query(TradeSnapshot)
                .filter(TradeSnapshot.symbol == sym)
                .filter(TradeSnapshot.trade_id.in_(ids))
            )
            rows = q.all()

            if not rows:
                _fail_db("no trade snapshots found (STRICT)")

            for r in rows:
                tid = _to_int_strict(getattr(r, "trade_id", None), "bt_trade_snapshots.trade_id")
                entry_ts = _require_tz_aware_dt(getattr(r, "entry_ts", None), "bt_trade_snapshots.entry_ts")
                direction = _normalize_direction_strict(getattr(r, "direction", None), "bt_trade_snapshots.direction")

                def opt_f(val: Any, nm: str) -> Optional[float]:
                    if val is None:
                        return None
                    return _to_float_strict(val, nm)

                def opt_i(val: Any, nm: str) -> Optional[int]:
                    if val is None:
                        return None
                    return _to_int_strict(val, nm)

                auto_blocked_raw = getattr(r, "auto_blocked", None)
                auto_blocked: Optional[bool] = None
                if auto_blocked_raw is not None:
                    if not isinstance(auto_blocked_raw, bool):
                        _fail_contract("bt_trade_snapshots.auto_blocked must be bool when provided (STRICT)")
                    auto_blocked = auto_blocked_raw

                by_id[int(tid)] = MetaSnapshotRow(
                    trade_id=int(tid),
                    symbol=sym,
                    entry_ts=entry_ts,
                    direction=direction,
                    signal_source=_require_optional_nonempty_str(getattr(r, "signal_source", None), "bt_trade_snapshots.signal_source"),
                    regime=_require_optional_nonempty_str(getattr(r, "regime", None), "bt_trade_snapshots.regime"),
                    entry_score=opt_f(getattr(r, "entry_score", None), "bt_trade_snapshots.entry_score"),
                    engine_total=opt_f(getattr(r, "engine_total", None), "bt_trade_snapshots.engine_total"),
                    dd_pct=opt_f(getattr(r, "dd_pct", None), "bt_trade_snapshots.dd_pct"),
                    hour_kst=opt_i(getattr(r, "hour_kst", None), "bt_trade_snapshots.hour_kst"),
                    weekday_kst=opt_i(getattr(r, "weekday_kst", None), "bt_trade_snapshots.weekday_kst"),
                    micro_score_risk=opt_f(getattr(r, "micro_score_risk", None), "bt_trade_snapshots.micro_score_risk"),
                    ev_cell_key=_require_optional_nonempty_str(getattr(r, "ev_cell_key", None), "bt_trade_snapshots.ev_cell_key"),
                    ev_cell_ev=opt_f(getattr(r, "ev_cell_ev", None), "bt_trade_snapshots.ev_cell_ev"),
                    ev_cell_n=opt_i(getattr(r, "ev_cell_n", None), "bt_trade_snapshots.ev_cell_n"),
                    ev_cell_status=_require_optional_nonempty_str(getattr(r, "ev_cell_status", None), "bt_trade_snapshots.ev_cell_status"),
                    auto_blocked=auto_blocked,
                    auto_risk_multiplier=opt_f(getattr(r, "auto_risk_multiplier", None), "bt_trade_snapshots.auto_risk_multiplier"),
                    gpt_severity=opt_i(getattr(r, "gpt_severity", None), "bt_trade_snapshots.gpt_severity"),
                    gpt_tags=getattr(r, "gpt_tags", None),
                    gpt_confidence_penalty=opt_f(getattr(r, "gpt_confidence_penalty", None), "bt_trade_snapshots.gpt_confidence_penalty"),
                    gpt_suggested_risk_multiplier=opt_f(
                        getattr(r, "gpt_suggested_risk_multiplier", None),
                        "bt_trade_snapshots.gpt_suggested_risk_multiplier",
                    ),
                )
    except MetaStatsError:
        raise
    except Exception as e:
        _fail_db("failed while fetching trade snapshots (STRICT)", e)

    missing = [tid for tid in ids if tid not in by_id]
    if missing:
        _fail_state(f"missing trade snapshots for trade_ids (STRICT): {missing[:20]}{'...' if len(missing) > 20 else ''}")

    return by_id


# ─────────────────────────────────────────────
# Aggregation logic (STRICT)
# ─────────────────────────────────────────────
def _win_loss_break_even(pnl: float, eps: float = 1e-12) -> str:
    if not math.isfinite(pnl):
        _fail_contract("pnl not finite (STRICT)")
    if pnl > eps:
        return "WIN"
    if pnl < -eps:
        return "LOSS"
    return "BE"


def _agg_basic(trades: List[MetaTradeRow]) -> Tuple[int, int, int, int, float, float, float, Optional[float]]:
    n = len(trades)
    if n <= 0:
        _fail_state("trades empty (STRICT)")

    wins = 0
    losses = 0
    bes = 0
    pnls: List[float] = []
    pnl_pcts: List[float] = []

    for t in trades:
        pnl = float(t.pnl_usdt)
        pnls.append(pnl)
        if t.pnl_pct_futures is not None:
            pnl_pcts.append(float(t.pnl_pct_futures))
        cls = _win_loss_break_even(pnl)
        if cls == "WIN":
            wins += 1
        elif cls == "LOSS":
            losses += 1
        else:
            bes += 1

    total_pnl = float(sum(pnls))
    avg_pnl = _mean(pnls, "avg_pnl_usdt")
    win_rate = _pct(float(wins), float(n), "win_rate_pct")

    avg_pnl_pct = None
    if pnl_pcts:
        avg_pnl_pct = _mean(pnl_pcts, "avg_pnl_pct_futures")

    return n, wins, losses, bes, win_rate, total_pnl, avg_pnl, avg_pnl_pct


def _group_stats_by_key(
    trades: List[MetaTradeRow],
    snaps: Mapping[int, MetaSnapshotRow],
    *,
    key_name: str,
    key_fn,
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}

    for t in trades:
        if int(t.trade_id) not in snaps:
            _fail_state(f"{key_name} grouping snapshot missing for trade_id={t.trade_id} (STRICT)")
        s = snaps[int(t.trade_id)]
        k = key_fn(t, s)
        key = _require_nonempty_str(k, f"{key_name}.group_key")

        bucket = out.setdefault(
            key,
            {"n": 0, "wins": 0, "losses": 0, "breakevens": 0, "total_pnl_usdt": 0.0, "avg_pnl_usdt": 0.0},
        )
        bucket["n"] += 1
        pnl = float(t.pnl_usdt)
        bucket["total_pnl_usdt"] += pnl

        cls = _win_loss_break_even(pnl)
        if cls == "WIN":
            bucket["wins"] += 1
        elif cls == "LOSS":
            bucket["losses"] += 1
        else:
            bucket["breakevens"] += 1

    for k, b in out.items():
        n = int(b["n"])
        if n <= 0:
            _fail_state(f"{key_name} group has n<=0 (STRICT): {k}")
        b["avg_pnl_usdt"] = float(b["total_pnl_usdt"]) / float(n)
        b["win_rate_pct"] = _pct(float(b["wins"]), float(n), f"{key_name}.win_rate_pct")

    return out


def build_meta_stats(cfg: MetaStatsConfig) -> MetaStatsResult:
    if not isinstance(cfg, MetaStatsConfig):
        _fail_config("cfg must be MetaStatsConfig (STRICT)")

    sym = _normalize_symbol_strict(cfg.symbol, "cfg.symbol")

    trades = _fetch_closed_trades_strict(cfg)
    if len(trades) < int(cfg.min_trades_required):
        _fail_state(f"not enough trades for meta stats (STRICT): n={len(trades)} < min={cfg.min_trades_required}")

    trade_ids = [t.trade_id for t in trades]
    snaps = _fetch_snapshots_by_trade_ids_strict(sym, trade_ids)

    n, wins, losses, bes, win_rate, total_pnl, avg_pnl, avg_pnl_pct = _agg_basic(trades)

    rw = int(cfg.recent_window)
    bw = int(cfg.baseline_window)
    if rw <= 0 or bw <= 0:
        _fail_config("recent_window/baseline_window must be > 0 (STRICT)")
    if n < (rw + bw):
        _fail_state(f"not enough trades for drift windows (STRICT): n={n} need>={rw + bw}")

    recent = trades[:rw]
    baseline = trades[rw : rw + bw]

    _, _, _, _, rw_wr, _, rw_avg, _ = _agg_basic(recent)
    _, _, _, _, bw_wr, _, bw_avg, _ = _agg_basic(baseline)

    by_direction = _group_stats_by_key(
        trades,
        snaps,
        key_name="direction",
        key_fn=lambda t, s: s.direction,
    )

    by_regime = _group_stats_by_key(
        trades,
        snaps,
        key_name="regime",
        key_fn=lambda t, s: _resolve_regime_strict(t, s),
    )

    def engine_bucket(t: MetaTradeRow, s: MetaSnapshotRow) -> str:
        if s.engine_total is None:
            _fail_contract(f"trade snapshot.engine_total missing (STRICT): trade_id={t.trade_id}")
        return _bucket_0_100(float(s.engine_total), step=10)

    def entry_bucket(t: MetaTradeRow, s: MetaSnapshotRow) -> str:
        if s.entry_score is None:
            _fail_contract(f"trade snapshot.entry_score missing (STRICT): trade_id={t.trade_id}")
        return _bucket_0_100(float(s.entry_score), step=10)

    by_engine_total_bucket = _group_stats_by_key(
        trades,
        snaps,
        key_name="engine_total_bucket",
        key_fn=engine_bucket,
    )
    by_entry_score_bucket = _group_stats_by_key(
        trades,
        snaps,
        key_name="entry_score_bucket",
        key_fn=entry_bucket,
    )

    recent_trades_payload: List[Dict[str, Any]] = []
    for t in trades[:rw]:
        s = snaps[int(t.trade_id)]
        recent_trades_payload.append(
            {
                "trade_id": int(t.trade_id),
                "entry_ts": t.entry_ts.isoformat(),
                "exit_ts": t.exit_ts.isoformat(),
                "direction": s.direction,
                "regime": _resolve_regime_strict(t, s),
                "pnl_usdt": float(t.pnl_usdt),
                "pnl_pct_futures": None if t.pnl_pct_futures is None else float(t.pnl_pct_futures),
                "engine_total": None if s.engine_total is None else float(s.engine_total),
                "entry_score": None if s.entry_score is None else float(s.entry_score),
                "dd_pct": None if s.dd_pct is None else float(s.dd_pct),
                "micro_score_risk": None if s.micro_score_risk is None else float(s.micro_score_risk),
                "ev_cell_status": s.ev_cell_status,
                "auto_blocked": s.auto_blocked,
                "auto_risk_multiplier": None if s.auto_risk_multiplier is None else float(s.auto_risk_multiplier),
                "close_reason": t.close_reason,
            }
        )

    return MetaStatsResult(
        symbol=sym,
        generated_at_utc=datetime.now(timezone.utc),
        n_trades=int(n),
        wins=int(wins),
        losses=int(losses),
        breakevens=int(bes),
        win_rate_pct=float(win_rate),
        total_pnl_usdt=float(total_pnl),
        avg_pnl_usdt=float(avg_pnl),
        avg_pnl_pct_futures=avg_pnl_pct,
        recent_win_rate_pct=float(rw_wr),
        baseline_win_rate_pct=float(bw_wr),
        recent_avg_pnl_usdt=float(rw_avg),
        baseline_avg_pnl_usdt=float(bw_avg),
        by_direction=by_direction,
        by_regime=by_regime,
        by_engine_total_bucket=by_engine_total_bucket,
        by_entry_score_bucket=by_entry_score_bucket,
        recent_trades=recent_trades_payload,
    )


def build_meta_stats_dict(cfg: MetaStatsConfig) -> Dict[str, Any]:
    """
    JSON 직렬화용 dict 반환(STRICT).
    """
    r = build_meta_stats(cfg)
    d = asdict(r)
    d["generated_at_utc"] = r.generated_at_utc.isoformat()
    return d


__all__ = [
    "MetaStatsError",
    "MetaStatsConfigError",
    "MetaStatsContractError",
    "MetaStatsDBError",
    "MetaStatsStateError",
    "MetaTradeRow",
    "MetaSnapshotRow",
    "MetaStatsConfig",
    "MetaStatsResult",
    "build_meta_stats",
    "build_meta_stats_dict",
]