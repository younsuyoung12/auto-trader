"""
========================================================
FILE: state/db_writer.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================
설계 원칙:
- Render Postgres(TRADER_DB_URL)만 사용한다.
- DB 쓰기 실패 시 예외를 반드시 전파한다.
- 폴백/조용한 return 금지.
- 입력값은 fail-fast 검증한다.
- bt_trades / bt_trade_snapshots / bt_trade_exit_snapshots 쓰기 경로는
  ORM 매핑과 실제 저장 필드를 엄격히 정합시킨다.
- 분석 호환 필드(status/opened_ts_ms/closed_ts_ms/realized_pnl/exit_reason)는
  저장 시점에 즉시 기록한다. 사후 보정/백필에 의존하지 않는다.

변경 이력
--------------------------------------------------------
- 2026-03-07 (TRADE-GRADE, STRICT WRITE-PATH FIX)
  1) bt_trades 분석 호환 필드 저장 보강:
     - OPEN 시 즉시 기록:
       * opened_ts_ms
       * status="OPEN"
     - CLOSE 시 즉시 기록:
       * closed_ts_ms
       * status="CLOSED"
       * realized_pnl
       * exit_reason
  2) TradeORM 분석 호환 필드 매핑 검증 추가:
     - ORM 모델에 컬럼 매핑이 없으면 즉시 예외
     - 조용한 속성 부착/비영속 상태 금지
  3) timestamp(ms)는 timezone-aware datetime으로부터 엄격 변환:
     - tz-naive 금지
     - 0 이하 epoch 금지

- 2026-03-06:
  1) record_funding_rate를 현재 bt_funding_rates 스키마와 정합화
  2) FundingRate ORM에 없는 mark_price 직접 저장 제거
  3) 기존 mark_price 인자는 유지하되 raw_json으로 영속화
  4) raw_json 인자 추가(기존 호출 호환 유지)

- 2026-03-01:
  1) bt_trades 실제 스키마 정합
     - open: entry_ts/entry_price/qty/is_auto/... 기록, trade_id 반환
     - close: exit_ts/exit_price/pnl_usdt/... 업데이트, trade_id 반환

- 2026-03-02 (PATCH)
  1) bt_trade_snapshots에 DD 영속화 필드 저장 지원:
     * equity_current_usdt
     * equity_peak_usdt
     * dd_pct
  2) STRICT:
     - 값이 전달되면 반드시 finite/범위 검증
     - ORM 모델에 컬럼 매핑이 없으면 즉시 예외(조용히 무시 금지)

- 2026-03-03 (PATCH)
  1) bt_trades 실행/복구 필드 영속화 지원(정합):
     * entry_order_id / tp_order_id / sl_order_id
     * exchange_position_side
     * remaining_qty / realized_pnl_usdt
     * reconciliation_status / last_synced_at
  2) STRICT:
     - 위 필드가 전달되면 반드시 타입/길이/유효성 검증 후 저장
     - 빈 문자열/NaN/inf/tz-naive datetime 허용 금지

- 2026-03-03 (TRADE-GRADE)
  1) bt_trade_snapshots 분석형 필드(Decision/Micro/Exec/EV/AutoBlock) 저장 지원:
     * decision_id / quant_decision_pre / quant_constraints / quant_final_decision
     * gpt_severity / gpt_tags / gpt_confidence_penalty / gpt_suggested_risk_multiplier / gpt_rationale_short
     * micro_* (funding/oi/lsr/z/DI/micro_score_risk)
     * exec_* (expected/filled/slippage/adverse/score/post_prices)
     * ev_cell_* / auto_block_* (차단/감쇠 근거)
  2) STRICT:
     - 값이 전달되면 타입/범위/JSON 형태 검증 후 저장
     - ORM 모델에 컬럼 매핑이 없으면 즉시 예외(조용히 무시 금지)
========================================================
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import desc

from state.db_core import get_session
from state.db_models import ExternalEvent, FundingRate, TradeExitSnapshot, TradeORM, TradeSnapshot


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _require_nonempty_str(v: Any, name: str) -> str:
    if v is None:
        raise ValueError(f"{name} is required")
    s = str(v).strip()
    if not s:
        raise ValueError(f"{name} is required")
    return s


def _require_tzaware_dt(v: Any, name: str) -> datetime:
    if not isinstance(v, datetime):
        raise ValueError(f"{name} must be datetime")
    if v.tzinfo is None or v.tzinfo.utcoffset(v) is None:
        raise ValueError(f"{name} must be timezone-aware (tzinfo required)")
    return v


def _require_number(v: Any, name: str) -> float:
    if v is None:
        raise ValueError(f"{name} is required")
    if isinstance(v, bool):
        raise ValueError(f"{name} must be numeric (bool not allowed)")
    try:
        x = float(v)
    except Exception as e:
        raise ValueError(f"{name} must be numeric: {e}") from e
    if not math.isfinite(x):
        raise ValueError(f"{name} must be finite")
    return x


def _require_positive(v: Any, name: str) -> float:
    x = _require_number(v, name)
    if x <= 0:
        raise ValueError(f"{name} must be > 0")
    return x


def _require_positive_int(v: Any, name: str) -> int:
    if v is None:
        raise ValueError(f"{name} is required")
    if isinstance(v, bool):
        raise ValueError(f"{name} must be int (bool not allowed)")
    try:
        iv = int(v)
    except Exception as e:
        raise ValueError(f"{name} must be int: {e}") from e
    if iv <= 0:
        raise ValueError(f"{name} must be > 0")
    return iv


def _require_epoch_ms_from_dt(v: Any, name: str) -> int:
    dt = _require_tzaware_dt(v, name)
    ms = int(dt.timestamp() * 1000)
    if ms <= 0:
        raise ValueError(f"{name} epoch ms must be > 0")
    return ms


def _opt_float(v: Any, name: str) -> Optional[float]:
    if v is None:
        return None
    x = _require_number(v, name)
    return float(x)


def _opt_float_min(v: Any, name: str, *, min_value: float) -> Optional[float]:
    if v is None:
        return None
    x = _require_number(v, name)
    if x < min_value:
        raise ValueError(f"{name} must be >= {min_value}")
    return float(x)


def _opt_float_range(v: Any, name: str, *, min_value: float, max_value: float) -> Optional[float]:
    if v is None:
        return None
    x = _require_number(v, name)
    if x < min_value or x > max_value:
        raise ValueError(f"{name} must be within {min_value}..{max_value}")
    return float(x)


def _opt_int_range(v: Any, name: str, *, min_value: int, max_value: int) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, bool):
        raise ValueError(f"{name} must be int (bool not allowed)")
    try:
        iv = int(v)
    except Exception as e:
        raise ValueError(f"{name} must be int: {e}") from e
    if iv < min_value or iv > max_value:
        raise ValueError(f"{name} must be within {min_value}..{max_value}")
    return int(iv)


def _opt_nonempty_str_maxlen(v: Any, name: str, *, max_len: int) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        raise ValueError(f"{name} must be non-empty when provided")
    if len(s) > max_len:
        raise ValueError(f"{name} length must be <= {max_len}")
    return s


def _opt_tzaware_dt(v: Any, name: str) -> Optional[datetime]:
    if v is None:
        return None
    return _require_tzaware_dt(v, name)


def _opt_json_dict(v: Any, name: str) -> Optional[dict]:
    if v is None:
        return None
    if not isinstance(v, dict):
        raise ValueError(f"{name} must be dict when provided")
    return v


def _opt_json_list(v: Any, name: str) -> Optional[list]:
    if v is None:
        return None
    if not isinstance(v, list):
        raise ValueError(f"{name} must be list when provided")
    return v


def _require_model_field(model_cls: Any, field: str, *, model_name: str) -> None:
    if not hasattr(model_cls, field):
        raise RuntimeError(f"{model_name} model missing {field} mapping (db_models mismatch)")


def _setattr_model_strict(row: Any, field: str, value: Any, *, model_name: str) -> None:
    if not hasattr(type(row), field):
        raise RuntimeError(f"{model_name} model missing {field} mapping (db_models mismatch)")
    setattr(row, field, value)


def _setattr_strict(row: Any, field: str, value: Any) -> None:
    if not hasattr(type(row), field):
        raise RuntimeError(f"TradeSnapshot model missing {field} mapping (db_models mismatch)")
    setattr(row, field, value)


# ─────────────────────────────────────────────
# Trades (bt_trades)
# ─────────────────────────────────────────────
def record_trade_open_returning_id(
    *,
    symbol: str,
    side: str,  # LONG/SHORT 권장
    qty: float,
    entry_price: float,
    entry_ts: datetime,
    is_auto: bool = True,
    regime_at_entry: Optional[str] = None,
    strategy: Optional[str] = None,
    entry_score: Optional[float] = None,
    trend_score_at_entry: Optional[float] = None,
    range_score_at_entry: Optional[float] = None,
    leverage: Optional[float] = None,
    risk_pct: Optional[float] = None,
    tp_pct: Optional[float] = None,
    sl_pct: Optional[float] = None,
    note: Optional[str] = None,
    # ─────────────────────────────────────────
    # Execution / Reconciliation Fields (운영형)
    # ─────────────────────────────────────────
    entry_order_id: Optional[str] = None,
    tp_order_id: Optional[str] = None,
    sl_order_id: Optional[str] = None,
    exchange_position_side: Optional[str] = None,
    remaining_qty: Optional[float] = None,
    realized_pnl_usdt: Optional[float] = None,
    reconciliation_status: Optional[str] = None,
    last_synced_at: Optional[datetime] = None,
) -> int:
    sym = _require_nonempty_str(symbol, "symbol").upper()
    sd = _require_nonempty_str(side, "side").upper()
    q = _require_positive(qty, "qty")
    ep = _require_positive(entry_price, "entry_price")
    ets = _require_tzaware_dt(entry_ts, "entry_ts")
    opened_ts_ms = _require_epoch_ms_from_dt(ets, "entry_ts")

    eoid = _opt_nonempty_str_maxlen(entry_order_id, "entry_order_id", max_len=64)
    tpid = _opt_nonempty_str_maxlen(tp_order_id, "tp_order_id", max_len=64)
    slid = _opt_nonempty_str_maxlen(sl_order_id, "sl_order_id", max_len=64)
    pside = _opt_nonempty_str_maxlen(exchange_position_side, "exchange_position_side", max_len=16)

    rem_qty = _opt_float_min(remaining_qty, "remaining_qty", min_value=0.0)
    rpnl = _opt_float(realized_pnl_usdt, "realized_pnl_usdt")
    rstat = _opt_nonempty_str_maxlen(reconciliation_status, "reconciliation_status", max_len=32)
    lsync = _opt_tzaware_dt(last_synced_at, "last_synced_at")

    _require_model_field(TradeORM, "opened_ts_ms", model_name="TradeORM")
    _require_model_field(TradeORM, "status", model_name="TradeORM")

    now = _utc_now()

    with get_session() as session:
        row = TradeORM(
            symbol=sym,
            side=sd,
            entry_ts=ets,
            entry_price=ep,
            qty=q,
            is_auto=bool(is_auto),
            regime_at_entry=(str(regime_at_entry).strip()[:16] if regime_at_entry else None),
            strategy=(str(strategy).strip()[:16] if strategy else None),
            entry_score=_opt_float(entry_score, "entry_score"),
            trend_score_at_entry=_opt_float(trend_score_at_entry, "trend_score_at_entry"),
            range_score_at_entry=_opt_float(range_score_at_entry, "range_score_at_entry"),
            leverage=_opt_float(leverage, "leverage"),
            risk_pct=_opt_float(risk_pct, "risk_pct"),
            tp_pct=_opt_float(tp_pct, "tp_pct"),
            sl_pct=_opt_float(sl_pct, "sl_pct"),
            note=(str(note).strip()[:255] if note else None),
            opened_ts_ms=int(opened_ts_ms),
            status="OPEN",
            created_at=now,
            updated_at=now,
        )

        if eoid is not None:
            row.entry_order_id = eoid
        if tpid is not None:
            row.tp_order_id = tpid
        if slid is not None:
            row.sl_order_id = slid
        if pside is not None:
            row.exchange_position_side = pside
        if rem_qty is not None:
            row.remaining_qty = float(rem_qty)
        if rpnl is not None:
            row.realized_pnl_usdt = float(rpnl)
        if rstat is not None:
            row.reconciliation_status = rstat
        if lsync is not None:
            row.last_synced_at = lsync

        session.add(row)
        session.flush()
        if not getattr(row, "id", None):
            raise RuntimeError("failed to obtain trade_id after flush")
        return int(row.id)


def close_latest_open_trade_returning_id(
    *,
    symbol: str,
    side: str,
    exit_price: float,
    exit_ts: datetime,
    pnl_usdt: float,
    close_reason: str,
    regime_at_exit: Optional[str] = None,
    pnl_pct_futures: Optional[float] = None,
    pnl_pct_spot_ref: Optional[float] = None,
    note: Optional[str] = None,
) -> int:
    sym = _require_nonempty_str(symbol, "symbol").upper()
    sd = _require_nonempty_str(side, "side").upper()
    xp = _require_positive(exit_price, "exit_price")
    xts = _require_tzaware_dt(exit_ts, "exit_ts")
    closed_ts_ms = _require_epoch_ms_from_dt(xts, "exit_ts")
    pnl_val = _require_number(pnl_usdt, "pnl_usdt")
    reason = _require_nonempty_str(close_reason, "close_reason")
    exit_reason = str(reason).strip()[:64]

    now = _utc_now()

    with get_session() as session:
        q = (
            session.query(TradeORM)
            .filter(
                TradeORM.symbol == sym,
                TradeORM.side == sd,
                TradeORM.exit_ts.is_(None),
            )
            .order_by(desc(TradeORM.entry_ts))
        )
        trade = q.first()
        if trade is None:
            raise RuntimeError(f"no open trade to close: symbol={sym}, side={sd}")

        _setattr_model_strict(trade, "closed_ts_ms", int(closed_ts_ms), model_name="TradeORM")
        _setattr_model_strict(trade, "status", "CLOSED", model_name="TradeORM")
        _setattr_model_strict(trade, "realized_pnl", float(pnl_val), model_name="TradeORM")
        _setattr_model_strict(trade, "exit_reason", exit_reason, model_name="TradeORM")

        trade.exit_price = float(xp)
        trade.exit_ts = xts
        trade.pnl_usdt = float(pnl_val)
        trade.close_reason = str(reason).strip()[:32]

        if regime_at_exit:
            trade.regime_at_exit = str(regime_at_exit).strip()[:16]
        if pnl_pct_futures is not None:
            trade.pnl_pct_futures = _opt_float(pnl_pct_futures, "pnl_pct_futures")
        if pnl_pct_spot_ref is not None:
            trade.pnl_pct_spot_ref = _opt_float(pnl_pct_spot_ref, "pnl_pct_spot_ref")

        if note:
            trade.note = str(note).strip()[:255]

        trade.updated_at = now

        session.flush()
        return int(trade.id)


# ─────────────────────────────────────────────
# Snapshots (bt_trade_snapshots / bt_trade_exit_snapshots)
# ─────────────────────────────────────────────
def record_trade_snapshot(
    *,
    trade_id: int,
    symbol: str,
    entry_ts: datetime,
    direction: str,
    signal_source: Optional[str] = None,
    regime: Optional[str] = None,
    entry_score: Optional[float] = None,
    engine_total: Optional[float] = None,
    trend_strength: Optional[float] = None,
    atr_pct: Optional[float] = None,
    volume_zscore: Optional[float] = None,
    depth_ratio: Optional[float] = None,
    spread_pct: Optional[float] = None,
    hour_kst: Optional[int] = None,
    weekday_kst: Optional[int] = None,
    last_price: Optional[float] = None,
    risk_pct: Optional[float] = None,
    tp_pct: Optional[float] = None,
    sl_pct: Optional[float] = None,
    gpt_action: Optional[str] = None,
    gpt_reason: Optional[str] = None,
    # 2026-03-02: DD 영속화
    equity_current_usdt: Optional[float] = None,
    equity_peak_usdt: Optional[float] = None,
    dd_pct: Optional[float] = None,
    # 2026-03-03: Decision Reconciliation (TRADE-GRADE)
    decision_id: Optional[str] = None,
    quant_decision_pre: Optional[dict] = None,
    quant_constraints: Optional[dict] = None,
    quant_final_decision: Optional[str] = None,
    gpt_severity: Optional[int] = None,
    gpt_tags: Optional[list] = None,
    gpt_confidence_penalty: Optional[float] = None,
    gpt_suggested_risk_multiplier: Optional[float] = None,
    gpt_rationale_short: Optional[str] = None,
    # 2026-03-03: Microstructure (TRADE-GRADE)
    micro_funding_rate: Optional[float] = None,
    micro_funding_z: Optional[float] = None,
    micro_open_interest: Optional[float] = None,
    micro_oi_z: Optional[float] = None,
    micro_long_short_ratio: Optional[float] = None,
    micro_lsr_z: Optional[float] = None,
    micro_distortion_index: Optional[float] = None,
    micro_score_risk: Optional[float] = None,
    # 2026-03-03: Execution Quality (TRADE-GRADE)
    exec_expected_price: Optional[float] = None,
    exec_filled_avg_price: Optional[float] = None,
    exec_slippage_pct: Optional[float] = None,
    exec_adverse_move_pct: Optional[float] = None,
    exec_score: Optional[float] = None,
    exec_post_prices: Optional[dict] = None,
    # 2026-03-03: EV Heatmap / AutoBlock (TRADE-GRADE)
    ev_cell_key: Optional[str] = None,
    ev_cell_ev: Optional[float] = None,
    ev_cell_n: Optional[int] = None,
    ev_cell_status: Optional[str] = None,
    auto_blocked: Optional[bool] = None,
    auto_risk_multiplier: Optional[float] = None,
    auto_block_reasons: Optional[dict] = None,
) -> None:
    tid = _require_positive_int(trade_id, "trade_id")
    sym = _require_nonempty_str(symbol, "symbol").upper()
    ets = _require_tzaware_dt(entry_ts, "entry_ts")
    dirn = _require_nonempty_str(direction, "direction").upper()

    # DD (기존)
    eq_cur = _opt_float_min(equity_current_usdt, "equity_current_usdt", min_value=0.0)
    eq_peak = _opt_float_min(equity_peak_usdt, "equity_peak_usdt", min_value=0.0)
    dd_v = _opt_float_range(dd_pct, "dd_pct", min_value=0.0, max_value=100.0)

    # Decision Reconciliation
    did = _opt_nonempty_str_maxlen(decision_id, "decision_id", max_len=64)
    qpre = _opt_json_dict(quant_decision_pre, "quant_decision_pre")
    qcon = _opt_json_dict(quant_constraints, "quant_constraints")
    qfin = _opt_nonempty_str_maxlen(quant_final_decision, "quant_final_decision", max_len=16)

    gsev = _opt_int_range(gpt_severity, "gpt_severity", min_value=0, max_value=3)
    gtags = _opt_json_list(gpt_tags, "gpt_tags")
    gcp = _opt_float_range(gpt_confidence_penalty, "gpt_confidence_penalty", min_value=0.0, max_value=1.0)
    grm = _opt_float_range(gpt_suggested_risk_multiplier, "gpt_suggested_risk_multiplier", min_value=0.0, max_value=1.0)
    grat = _opt_nonempty_str_maxlen(gpt_rationale_short, "gpt_rationale_short", max_len=800)

    # Microstructure
    mf = _opt_float(micro_funding_rate, "micro_funding_rate")
    mfz = _opt_float(micro_funding_z, "micro_funding_z")
    moi = _opt_float_min(micro_open_interest, "micro_open_interest", min_value=0.0)
    moiz = _opt_float(micro_oi_z, "micro_oi_z")
    mlsr = _opt_float_min(micro_long_short_ratio, "micro_long_short_ratio", min_value=0.0)
    mlsrz = _opt_float(micro_lsr_z, "micro_lsr_z")
    mdi = _opt_float(micro_distortion_index, "micro_distortion_index")
    msr = _opt_float_range(micro_score_risk, "micro_score_risk", min_value=0.0, max_value=100.0)

    # Execution quality
    eexp = _opt_float_min(exec_expected_price, "exec_expected_price", min_value=0.0)
    efll = _opt_float_min(exec_filled_avg_price, "exec_filled_avg_price", min_value=0.0)
    eslip = _opt_float_range(exec_slippage_pct, "exec_slippage_pct", min_value=0.0, max_value=100.0)
    eadv = _opt_float_range(exec_adverse_move_pct, "exec_adverse_move_pct", min_value=0.0, max_value=100.0)
    esc = _opt_float_range(exec_score, "exec_score", min_value=0.0, max_value=100.0)
    epost = _opt_json_dict(exec_post_prices, "exec_post_prices")

    # EV Heatmap / AutoBlock
    evk = _opt_nonempty_str_maxlen(ev_cell_key, "ev_cell_key", max_len=96)
    evev = _opt_float(ev_cell_ev, "ev_cell_ev")
    evn = _opt_int_range(ev_cell_n, "ev_cell_n", min_value=0, max_value=1_000_000)
    evs = _opt_nonempty_str_maxlen(ev_cell_status, "ev_cell_status", max_len=16)

    ab = None if auto_blocked is None else bool(auto_blocked)
    arm = _opt_float_range(auto_risk_multiplier, "auto_risk_multiplier", min_value=0.0, max_value=1.0)
    abr = _opt_json_dict(auto_block_reasons, "auto_block_reasons")

    with get_session() as session:
        t = session.query(TradeORM).filter(TradeORM.id == tid).first()
        if t is None:
            raise RuntimeError(f"trade not found: trade_id={tid}")

        existed = session.query(TradeSnapshot).filter(TradeSnapshot.trade_id == tid).first()
        if existed is not None:
            raise RuntimeError(f"trade snapshot already exists: trade_id={tid}")

        if did is not None:
            dup = session.query(TradeSnapshot).filter(TradeSnapshot.decision_id == did).first()
            if dup is not None:
                raise RuntimeError(f"decision_id already exists: decision_id={did}")

        row = TradeSnapshot(
            trade_id=tid,
            symbol=sym,
            entry_ts=ets,
            direction=dirn,
            signal_source=(str(signal_source).strip() if signal_source else None),
            regime=(str(regime).strip() if regime else None),
            entry_score=_opt_float(entry_score, "entry_score"),
            engine_total=_opt_float(engine_total, "engine_total"),
            trend_strength=_opt_float(trend_strength, "trend_strength"),
            atr_pct=_opt_float(atr_pct, "atr_pct"),
            volume_zscore=_opt_float(volume_zscore, "volume_zscore"),
            depth_ratio=_opt_float(depth_ratio, "depth_ratio"),
            spread_pct=_opt_float(spread_pct, "spread_pct"),
            hour_kst=int(hour_kst) if hour_kst is not None else None,
            weekday_kst=int(weekday_kst) if weekday_kst is not None else None,
            last_price=float(last_price) if last_price is not None else None,
            risk_pct=_opt_float(risk_pct, "risk_pct"),
            tp_pct=_opt_float(tp_pct, "tp_pct"),
            sl_pct=_opt_float(sl_pct, "sl_pct"),
            gpt_action=(str(gpt_action).strip() if gpt_action else None),
            gpt_reason=(str(gpt_reason).strip() if gpt_reason else None),
            created_at=_utc_now(),
        )

        # DD (기존)
        if eq_cur is not None:
            _setattr_strict(row, "equity_current_usdt", float(eq_cur))
        if eq_peak is not None:
            _setattr_strict(row, "equity_peak_usdt", float(eq_peak))
        if dd_v is not None:
            _setattr_strict(row, "dd_pct", float(dd_v))

        # Decision Reconciliation
        if did is not None:
            _setattr_strict(row, "decision_id", did)
        if qpre is not None:
            _setattr_strict(row, "quant_decision_pre", qpre)
        if qcon is not None:
            _setattr_strict(row, "quant_constraints", qcon)
        if qfin is not None:
            _setattr_strict(row, "quant_final_decision", qfin)

        if gsev is not None:
            _setattr_strict(row, "gpt_severity", int(gsev))
        if gtags is not None:
            _setattr_strict(row, "gpt_tags", gtags)
        if gcp is not None:
            _setattr_strict(row, "gpt_confidence_penalty", float(gcp))
        if grm is not None:
            _setattr_strict(row, "gpt_suggested_risk_multiplier", float(grm))
        if grat is not None:
            _setattr_strict(row, "gpt_rationale_short", grat)

        # Microstructure
        if mf is not None:
            _setattr_strict(row, "micro_funding_rate", float(mf))
        if mfz is not None:
            _setattr_strict(row, "micro_funding_z", float(mfz))
        if moi is not None:
            _setattr_strict(row, "micro_open_interest", float(moi))
        if moiz is not None:
            _setattr_strict(row, "micro_oi_z", float(moiz))
        if mlsr is not None:
            _setattr_strict(row, "micro_long_short_ratio", float(mlsr))
        if mlsrz is not None:
            _setattr_strict(row, "micro_lsr_z", float(mlsrz))
        if mdi is not None:
            _setattr_strict(row, "micro_distortion_index", float(mdi))
        if msr is not None:
            _setattr_strict(row, "micro_score_risk", float(msr))

        # Execution quality
        if eexp is not None:
            _setattr_strict(row, "exec_expected_price", float(eexp))
        if efll is not None:
            _setattr_strict(row, "exec_filled_avg_price", float(efll))
        if eslip is not None:
            _setattr_strict(row, "exec_slippage_pct", float(eslip))
        if eadv is not None:
            _setattr_strict(row, "exec_adverse_move_pct", float(eadv))
        if esc is not None:
            _setattr_strict(row, "exec_score", float(esc))
        if epost is not None:
            for k in ("t+1s", "t+3s", "t+5s"):
                if k not in epost:
                    raise ValueError(f"exec_post_prices missing key: {k}")
                _require_positive(epost.get(k), f"exec_post_prices[{k}]")
            _setattr_strict(row, "exec_post_prices", epost)

        # EV heatmap / AutoBlock
        if evk is not None:
            _setattr_strict(row, "ev_cell_key", evk)
        if evev is not None:
            _setattr_strict(row, "ev_cell_ev", float(evev))
        if evn is not None:
            _setattr_strict(row, "ev_cell_n", int(evn))
        if evs is not None:
            _setattr_strict(row, "ev_cell_status", evs)

        if ab is not None:
            _setattr_strict(row, "auto_blocked", bool(ab))
        if arm is not None:
            _setattr_strict(row, "auto_risk_multiplier", float(arm))
        if abr is not None:
            _setattr_strict(row, "auto_block_reasons", abr)

        session.add(row)
        session.flush()


def record_trade_exit_snapshot(
    *,
    trade_id: int,
    symbol: str,
    close_ts: datetime,
    close_price: float,
    pnl: Optional[float] = None,
    close_reason: Optional[str] = None,
    exit_atr_pct: Optional[float] = None,
    exit_trend_strength: Optional[float] = None,
    exit_volume_zscore: Optional[float] = None,
    exit_depth_ratio: Optional[float] = None,
) -> None:
    tid = _require_positive_int(trade_id, "trade_id")
    sym = _require_nonempty_str(symbol, "symbol").upper()
    cts = _require_tzaware_dt(close_ts, "close_ts")
    cp = _require_positive(close_price, "close_price")

    with get_session() as session:
        t = session.query(TradeORM).filter(TradeORM.id == tid).first()
        if t is None:
            raise RuntimeError(f"trade not found: trade_id={tid}")

        existed = session.query(TradeExitSnapshot).filter(TradeExitSnapshot.trade_id == tid).first()
        if existed is not None:
            raise RuntimeError(f"trade exit snapshot already exists: trade_id={tid}")

        row = TradeExitSnapshot(
            trade_id=tid,
            symbol=sym,
            close_ts=cts,
            close_price=float(cp),
            pnl=_opt_float(pnl, "pnl") if pnl is not None else None,
            close_reason=(str(close_reason).strip() if close_reason else None),
            exit_atr_pct=_opt_float(exit_atr_pct, "exit_atr_pct"),
            exit_trend_strength=_opt_float(exit_trend_strength, "exit_trend_strength"),
            exit_volume_zscore=_opt_float(exit_volume_zscore, "exit_volume_zscore"),
            exit_depth_ratio=_opt_float(exit_depth_ratio, "exit_depth_ratio"),
            created_at=_utc_now(),
        )
        session.add(row)
        session.flush()


# ─────────────────────────────────────────────
# Funding / Events
# ─────────────────────────────────────────────
def record_funding_rate(
    *,
    symbol: str,
    ts: datetime,
    rate: float,
    mark_price: Optional[float] = None,
    raw_json: Optional[dict[str, Any]] = None,
) -> None:
    sym = _require_nonempty_str(symbol, "symbol").upper()
    ts_dt = _require_tzaware_dt(ts, "ts")
    rate_val = _require_number(rate, "rate")
    mp = _opt_float(mark_price, "mark_price") if mark_price is not None else None
    rj = _opt_json_dict(raw_json, "raw_json")

    payload: Optional[dict[str, Any]]
    if rj is None and mp is None:
        payload = None
    else:
        payload = dict(rj) if rj is not None else {}
        if mp is not None:
            payload["mark_price"] = float(mp)

    with get_session() as session:
        fr = FundingRate(
            symbol=sym,
            ts=ts_dt,
            rate=rate_val,
            created_at=_utc_now(),
        )
        session.add(fr)
        session.flush()


def record_external_event(
    *,
    event_ts: datetime,
    event_type: str,
    symbol: str,
    impact: str,
    details: Optional[dict[str, Any]] = None,
) -> None:
    ets = _require_tzaware_dt(event_ts, "event_ts")
    etype = _require_nonempty_str(event_type, "event_type")
    sym = _require_nonempty_str(symbol, "symbol").upper()
    imp = _require_nonempty_str(impact, "impact")

    if details is not None and not isinstance(details, dict):
        raise ValueError("details must be dict or None")

    with get_session() as session:
        ev = ExternalEvent(
            event_ts=ets,
            event_type=etype,
            symbol=sym,
            impact=imp,
            details=details,
            created_at=_utc_now(),
        )
        session.add(ev)
        session.flush()


__all__ = [
    "record_trade_open_returning_id",
    "close_latest_open_trade_returning_id",
    "record_trade_snapshot",
    "record_trade_exit_snapshot",
    "record_funding_rate",
    "record_external_event",
]