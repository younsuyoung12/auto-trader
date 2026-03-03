# meta/meta_stats_aggregator.py
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

변경 이력
--------------------------------------------------------
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
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from state.db_core import get_session
from state.db_models import TradeORM, TradeSnapshot


# ─────────────────────────────────────────────
# Exceptions
# ─────────────────────────────────────────────
class MetaStatsError(RuntimeError):
    """STRICT: meta stats aggregation failure."""


# ─────────────────────────────────────────────
# Strict primitives
# ─────────────────────────────────────────────
def _require_nonempty_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        raise MetaStatsError(f"{name} is required (STRICT)")
    return s


def _require_tz_aware_dt(v: Any, name: str) -> datetime:
    if not isinstance(v, datetime):
        raise MetaStatsError(f"{name} must be datetime (STRICT)")
    if v.tzinfo is None or v.tzinfo.utcoffset(v) is None:
        raise MetaStatsError(f"{name} must be tz-aware datetime (STRICT)")
    return v


def _to_float_strict(v: Any, name: str) -> float:
    if v is None:
        raise MetaStatsError(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        raise MetaStatsError(f"{name} must be numeric (bool not allowed) (STRICT)")

    # SQLAlchemy Numeric can be Decimal
    if isinstance(v, Decimal):
        fv = float(v)
    else:
        try:
            fv = float(v)
        except Exception as e:
            raise MetaStatsError(f"{name} must be numeric (STRICT)") from e

    if not math.isfinite(fv):
        raise MetaStatsError(f"{name} must be finite (STRICT): {fv!r}")
    return float(fv)


def _to_int_strict(v: Any, name: str) -> int:
    if v is None:
        raise MetaStatsError(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        raise MetaStatsError(f"{name} must be int (bool not allowed) (STRICT)")
    try:
        iv = int(v)
    except Exception as e:
        raise MetaStatsError(f"{name} must be int (STRICT)") from e
    return int(iv)


def _mean(values: Sequence[float], name: str) -> float:
    if not values:
        raise MetaStatsError(f"{name} empty (STRICT)")
    s = float(sum(values))
    n = int(len(values))
    out = s / float(n)
    if not math.isfinite(out):
        raise MetaStatsError(f"{name} mean not finite (STRICT)")
    return float(out)


def _pct(numer: float, denom: float, name: str) -> float:
    if denom <= 0 or not math.isfinite(denom):
        raise MetaStatsError(f"{name} denom invalid (STRICT)")
    out = (numer / denom) * 100.0
    if not math.isfinite(out):
        raise MetaStatsError(f"{name} percent not finite (STRICT)")
    return float(out)


def _bucket_0_100(v: float, *, step: int = 10) -> str:
    if step <= 0:
        raise MetaStatsError("bucket step must be > 0 (STRICT)")
    if v < 0 or v > 100 or not math.isfinite(v):
        raise MetaStatsError(f"score out of range 0..100 (STRICT): {v}")
    lo = int(v // step) * step
    hi = min(lo + step, 100)
    if hi == lo:
        hi = min(lo + step, 100)
    return f"{lo:02d}-{hi:02d}"


# ─────────────────────────────────────────────
# Data models
# ─────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class MetaTradeRow:
    trade_id: int
    symbol: str
    side: str  # BUY/SELL or LONG/SHORT (stored as-is)
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


@dataclass(frozen=True, slots=True)
class MetaSnapshotRow:
    trade_id: int
    symbol: str
    entry_ts: datetime
    direction: str  # LONG/SHORT
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


@dataclass(frozen=True, slots=True)
class MetaStatsConfig:
    symbol: str
    lookback_trades: int = 200
    recent_window: int = 20
    baseline_window: int = 60
    min_trades_required: int = 20
    exclude_test_trades: bool = True  # reconciliation_status/note 기반 필터


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

    # Drift (recent vs baseline)
    recent_win_rate_pct: float
    baseline_win_rate_pct: float
    recent_avg_pnl_usdt: float
    baseline_avg_pnl_usdt: float

    # Breakdowns
    by_direction: Dict[str, Dict[str, Any]]
    by_regime: Dict[str, Dict[str, Any]]
    by_engine_total_bucket: Dict[str, Dict[str, Any]]
    by_entry_score_bucket: Dict[str, Dict[str, Any]]

    # Recent trades (compact, deterministic)
    recent_trades: List[Dict[str, Any]]


# ─────────────────────────────────────────────
# Fetch layer (STRICT)
# ─────────────────────────────────────────────
def _is_test_trade_strict(t: MetaTradeRow) -> bool:
    """
    STRICT:
    - 테스트 트레이드 판별은 DB 필드에 근거해야 한다.
    - 현재 프로젝트는 TEST_DRY_RUN 시 reconciliation_status="TEST_DRY_RUN" 또는 note에 TEST 문자열이 들어간다.
    """
    rs = (t.reconciliation_status or "").strip().upper()
    note = (t.note or "").strip().upper()
    if rs == "TEST_DRY_RUN":
        return True
    if note.startswith("TEST"):
        return True
    return False


def _fetch_closed_trades_strict(cfg: MetaStatsConfig) -> List[MetaTradeRow]:
    sym = _require_nonempty_str(cfg.symbol, "cfg.symbol").upper().replace("-", "").replace("/", "").replace("_", "")
    if cfg.lookback_trades <= 0:
        raise MetaStatsError("cfg.lookback_trades must be > 0 (STRICT)")

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
        raise MetaStatsError(f"no closed trades found for symbol={sym} (STRICT)")

    out: List[MetaTradeRow] = []
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

        side = _require_nonempty_str(getattr(r, "side", None), "bt_trades.side").upper()

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
                strategy=(str(getattr(r, "strategy", "")).strip() or None),
                regime_at_entry=(str(getattr(r, "regime_at_entry", "")).strip() or None),
                regime_at_exit=(str(getattr(r, "regime_at_exit", "")).strip() or None),
                close_reason=(str(getattr(r, "close_reason", "")).strip() or None),
                reconciliation_status=(str(getattr(r, "reconciliation_status", "")).strip() or None),
                note=(str(getattr(r, "note", "")).strip() or None),
            )
        )

    # optional exclude test trades
    if cfg.exclude_test_trades:
        out2 = [t for t in out if not _is_test_trade_strict(t)]
        if not out2:
            raise MetaStatsError("all trades were filtered as test trades (STRICT)")
        return out2

    return out


def _fetch_snapshots_by_trade_ids_strict(symbol: str, trade_ids: Sequence[int]) -> Dict[int, MetaSnapshotRow]:
    sym = _require_nonempty_str(symbol, "symbol").upper().replace("-", "").replace("/", "").replace("_", "")
    if not trade_ids:
        raise MetaStatsError("trade_ids empty (STRICT)")

    # de-dup
    ids = sorted({int(x) for x in trade_ids if int(x) > 0})
    if not ids:
        raise MetaStatsError("trade_ids invalid (STRICT)")

    with get_session() as session:
        q = (
            session.query(TradeSnapshot)
            .filter(TradeSnapshot.symbol == sym)
            .filter(TradeSnapshot.trade_id.in_(ids))
        )
        rows = q.all()

    if not rows:
        raise MetaStatsError("no trade snapshots found (STRICT)")

    by_id: Dict[int, MetaSnapshotRow] = {}
    for r in rows:
        tid = _to_int_strict(getattr(r, "trade_id", None), "bt_trade_snapshots.trade_id")
        entry_ts = _require_tz_aware_dt(getattr(r, "entry_ts", None), "bt_trade_snapshots.entry_ts")
        direction = _require_nonempty_str(getattr(r, "direction", None), "bt_trade_snapshots.direction").upper()
        if direction not in ("LONG", "SHORT"):
            raise MetaStatsError(f"bt_trade_snapshots.direction invalid (STRICT): {direction!r}")

        # optional numeric fields: if present must be finite
        def opt_f(val: Any, nm: str) -> Optional[float]:
            if val is None:
                return None
            return _to_float_strict(val, nm)

        def opt_i(val: Any, nm: str) -> Optional[int]:
            if val is None:
                return None
            return _to_int_strict(val, nm)

        by_id[int(tid)] = MetaSnapshotRow(
            trade_id=int(tid),
            symbol=sym,
            entry_ts=entry_ts,
            direction=direction,
            signal_source=(str(getattr(r, "signal_source", "")).strip() or None),
            regime=(str(getattr(r, "regime", "")).strip() or None),
            entry_score=opt_f(getattr(r, "entry_score", None), "bt_trade_snapshots.entry_score"),
            engine_total=opt_f(getattr(r, "engine_total", None), "bt_trade_snapshots.engine_total"),
            dd_pct=opt_f(getattr(r, "dd_pct", None), "bt_trade_snapshots.dd_pct"),
            hour_kst=opt_i(getattr(r, "hour_kst", None), "bt_trade_snapshots.hour_kst"),
            weekday_kst=opt_i(getattr(r, "weekday_kst", None), "bt_trade_snapshots.weekday_kst"),
            micro_score_risk=opt_f(getattr(r, "micro_score_risk", None), "bt_trade_snapshots.micro_score_risk"),
            ev_cell_key=(str(getattr(r, "ev_cell_key", "")).strip() or None),
            ev_cell_ev=opt_f(getattr(r, "ev_cell_ev", None), "bt_trade_snapshots.ev_cell_ev"),
            ev_cell_n=opt_i(getattr(r, "ev_cell_n", None), "bt_trade_snapshots.ev_cell_n"),
            ev_cell_status=(str(getattr(r, "ev_cell_status", "")).strip() or None),
            auto_blocked=(bool(getattr(r, "auto_blocked")) if getattr(r, "auto_blocked", None) is not None else None),
            auto_risk_multiplier=opt_f(getattr(r, "auto_risk_multiplier", None), "bt_trade_snapshots.auto_risk_multiplier"),
            gpt_severity=opt_i(getattr(r, "gpt_severity", None), "bt_trade_snapshots.gpt_severity"),
            gpt_tags=getattr(r, "gpt_tags", None),
            gpt_confidence_penalty=opt_f(getattr(r, "gpt_confidence_penalty", None), "bt_trade_snapshots.gpt_confidence_penalty"),
            gpt_suggested_risk_multiplier=opt_f(getattr(r, "gpt_suggested_risk_multiplier", None), "bt_trade_snapshots.gpt_suggested_risk_multiplier"),
        )

    # STRICT: snapshot 누락 금지
    missing = [tid for tid in ids if tid not in by_id]
    if missing:
        raise MetaStatsError(f"missing trade snapshots for trade_ids (STRICT): {missing[:20]}{'...' if len(missing) > 20 else ''}")

    return by_id


# ─────────────────────────────────────────────
# Aggregation logic (STRICT)
# ─────────────────────────────────────────────
def _win_loss_break_even(pnl: float, eps: float = 1e-12) -> str:
    if not math.isfinite(pnl):
        raise MetaStatsError("pnl not finite (STRICT)")
    if pnl > eps:
        return "WIN"
    if pnl < -eps:
        return "LOSS"
    return "BE"


def _agg_basic(trades: List[MetaTradeRow]) -> Tuple[int, int, int, int, float, float, float, Optional[float]]:
    n = len(trades)
    if n <= 0:
        raise MetaStatsError("trades empty (STRICT)")

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
        s = snaps[int(t.trade_id)]
        k = key_fn(t, s)
        k = str(k or "").strip() or "UNKNOWN"

        bucket = out.setdefault(k, {"n": 0, "wins": 0, "losses": 0, "breakevens": 0, "total_pnl_usdt": 0.0, "avg_pnl_usdt": 0.0})
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

    # finalize averages + winrate
    for k, b in out.items():
        n = int(b["n"])
        if n <= 0:
            raise MetaStatsError(f"{key_name} group has n<=0 (STRICT)")
        b["avg_pnl_usdt"] = float(b["total_pnl_usdt"]) / float(n)
        b["win_rate_pct"] = _pct(float(b["wins"]), float(n), f"{key_name}.win_rate_pct")

    return out


def build_meta_stats(cfg: MetaStatsConfig) -> MetaStatsResult:
    sym = _require_nonempty_str(cfg.symbol, "cfg.symbol").upper().replace("-", "").replace("/", "").replace("_", "")

    trades = _fetch_closed_trades_strict(cfg)
    if len(trades) < int(cfg.min_trades_required):
        raise MetaStatsError(f"not enough trades for meta stats (STRICT): n={len(trades)} < min={cfg.min_trades_required}")

    trade_ids = [t.trade_id for t in trades]
    snaps = _fetch_snapshots_by_trade_ids_strict(sym, trade_ids)

    # basic
    n, wins, losses, bes, win_rate, total_pnl, avg_pnl, avg_pnl_pct = _agg_basic(trades)

    # drift windows
    rw = int(cfg.recent_window)
    bw = int(cfg.baseline_window)
    if rw <= 0 or bw <= 0:
        raise MetaStatsError("recent_window/baseline_window must be > 0 (STRICT)")
    if n < (rw + bw):
        raise MetaStatsError(f"not enough trades for drift windows (STRICT): n={n} need>={rw+bw}")

    recent = trades[:rw]
    baseline = trades[rw : rw + bw]

    _, rw_w, _, _, rw_wr, _, rw_avg, _ = _agg_basic(recent)
    _, bw_w, _, _, bw_wr, _, bw_avg, _ = _agg_basic(baseline)

    # breakdowns
    by_direction = _group_stats_by_key(
        trades,
        snaps,
        key_name="direction",
        key_fn=lambda t, s: s.direction,  # snapshot 기준(롱/숏)
    )

    by_regime = _group_stats_by_key(
        trades,
        snaps,
        key_name="regime",
        key_fn=lambda t, s: (s.regime or t.regime_at_entry or "UNKNOWN"),
    )

    # score buckets (engine_total / entry_score)
    def engine_bucket(t: MetaTradeRow, s: MetaSnapshotRow) -> str:
        if s.engine_total is None:
            raise MetaStatsError("trade snapshot.engine_total missing (STRICT)")
        return _bucket_0_100(float(s.engine_total), step=10)

    def entry_bucket(t: MetaTradeRow, s: MetaSnapshotRow) -> str:
        if s.entry_score is None:
            raise MetaStatsError("trade snapshot.entry_score missing (STRICT)")
        return _bucket_0_100(float(s.entry_score), step=10)

    by_engine_total_bucket = _group_stats_by_key(trades, snaps, key_name="engine_total_bucket", key_fn=engine_bucket)
    by_entry_score_bucket = _group_stats_by_key(trades, snaps, key_name="entry_score_bucket", key_fn=entry_bucket)

    # recent trades compact payload
    recent_trades_payload: List[Dict[str, Any]] = []
    for t in trades[:rw]:
        s = snaps[int(t.trade_id)]
        recent_trades_payload.append(
            {
                "trade_id": int(t.trade_id),
                "entry_ts": t.entry_ts.isoformat(),
                "exit_ts": t.exit_ts.isoformat(),
                "direction": s.direction,
                "regime": s.regime or t.regime_at_entry or "UNKNOWN",
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
    # datetime serialization
    d["generated_at_utc"] = r.generated_at_utc.isoformat()
    return d


__all__ = [
    "MetaStatsError",
    "MetaStatsConfig",
    "MetaStatsResult",
    "build_meta_stats",
    "build_meta_stats_dict",
]