"""
========================================================
FILE: state/sync_exchange.py
ROLE:
- Binance USDT-M Futures 전용 거래소 상태 동기화 SSOT
- 거래소 실제 포지션/미체결 주문과 DB(bt_trades) OPEN 트레이드 정합을 강제한다
- 엔진 재시작 시 거래소 사실값과 메모리 사실값으로 DB 실행 필드와 메모리 Trade를 복구한다

CORE RESPONSIBILITIES:
- 거래소 포지션/오더 snapshot STRICT 수집
- DB OPEN 트레이드 1건 ↔ 거래소 OPEN 포지션 1건 정합 강제
- TP/SL 보호주문 식별 및 실행 필드 DB 반영
- 메모리 Trade 재구성 전 필수 계약 검증
- POSITION 이벤트 기록 및 sync 성공 알림
- DB OPEN trade 부재 시 memory-assisted recovery 수행
- 복구 불가능 상태를 SyncRecoveryRequiredError 로 명시 승격

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · PRODUCTION MODE
- REST 실패/응답 이상/정합 불일치 시 즉시 예외
- DB OPEN 1건 ↔ 거래소 OPEN 포지션 1건 원칙
- Hedge / ambiguous topology / duplicate memory trade 금지
- PROTECTION_VERIFIED 상태는 필수 필드 완비 상태에서만 인정
- 거래소 사실값이 DB/메모리와 다르면 즉시 예외 또는 거래소 사실로 엄격 갱신
- blind recovery 금지: DB OPEN trade 부재 시 current_trades 기반 복구가 가능한 경우에만 복구
- print 금지, logging만 사용

CHANGE HISTORY:
- 2026-03-14:
  1) FIX(ROOT-CAUSE): exchange OPEN position + DB NO OPEN trade 상태에 대한 memory-assisted recovery 경로 추가
  2) ADD(RECOVERY): 복구 불가능 상태를 SyncRecoveryRequiredError 로 명시 승격
  3) ADD(STRICT): recovery 시 mem trade / exchange protection / db persisted fields 계약 검증 추가
  4) FIX(STRICT): recovery 가능한 경우에만 DB OPEN trade 재구성, 불가능하면 blind fallback 없이 즉시 중단
  5) ADD(OPERABILITY): recovery success / recovery required 텔레그램/로그 알림 추가
- 2026-03-10:
  1) FIX(ROOT-CAUSE): DB trade side ↔ exchange pos_dir 방향 정합 검증 추가
  2) FIX(ROOT-CAUSE): replace=False 경로에서 동일 심볼 memory trade 중복 복구 금지
  3) FIX(STRICT): TP/SL 동일 orderId 및 비양수 보호가격 검증 추가
  4) FIX(CONTRACT): PROTECTION_VERIFIED 상태 사전 계약 검증 강화
  5) FIX(STRICT): DB 필수 문자열 필드(strategy/regime/source) 정합 강화
- 2026-03-09:
  1) FIX(ROOT-CAUSE): 재시작 복구 시 qty 원본 유지 + remaining_qty만 실잔량으로 복구
  2) FIX(STRICT): DB 접근을 get_session() SSOT 경유로 통일
  3) FIX(CONTRACT): PROTECTION_VERIFIED 실행 필드 누락 시 즉시 예외
  4) FIX(STRICT): exchange abs_qty > db qty 정합 불일치 즉시 예외
  5) 기존 기능 삭제 없음
========================================================
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from events.signals_logger import log_event
from execution.exchange_api import fetch_open_orders, fetch_open_positions
from infra.telelog import log, send_tg
from state.db_core import get_session
from state.db_models import Trade as TradeORM
from state.trader_state import Trade as MemTrade


class SyncRecoveryRequiredError(RuntimeError):
    """거래소 OPEN 포지션은 존재하지만 현재 보유한 진실원천만으로는 안전 복구가 불가능한 상태."""


# ─────────────────────────────────────────────────────────────
# Strict helpers
# ─────────────────────────────────────────────────────────────
def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _require_nonempty_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        raise RuntimeError(f"{name} is required (STRICT)")
    return s


def _safe_float_strict(v: Any, name: str) -> float:
    if v is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        raise RuntimeError(f"{name} must be numeric (bool not allowed) (STRICT)")
    try:
        f = float(v)
    except Exception as e:
        raise RuntimeError(f"{name} must be numeric: {e} (STRICT)") from e
    if not math.isfinite(f):
        raise RuntimeError(f"{name} must be finite (STRICT)")
    return f


def _safe_positive_int_strict(v: Any, name: str) -> int:
    if v is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        raise RuntimeError(f"{name} must be int (bool not allowed) (STRICT)")
    try:
        i = int(v)
    except Exception as e:
        raise RuntimeError(f"{name} must be int: {e} (STRICT)") from e
    if i <= 0:
        raise RuntimeError(f"{name} must be > 0 (STRICT)")
    return i


def _require_tz_aware_datetime(v: Any, name: str) -> datetime:
    if not isinstance(v, datetime):
        raise RuntimeError(f"{name} must be datetime (STRICT)")
    if v.tzinfo is None or v.tzinfo.utcoffset(v) is None:
        raise RuntimeError(f"{name} must be timezone-aware datetime (STRICT)")
    return v


def _normalize_symbol(symbol: Any) -> str:
    s = str(symbol).replace("-", "").replace("/", "").upper().strip()
    if not s:
        raise RuntimeError("symbol is empty (STRICT)")
    return s


def _position_dir_one_way(position_amt: float) -> str:
    if position_amt > 0:
        return "LONG"
    if position_amt < 0:
        return "SHORT"
    raise RuntimeError("positionAmt is zero (STRICT)")


def _entry_side_from_dir(pos_dir: str) -> str:
    if pos_dir == "LONG":
        return "BUY"
    if pos_dir == "SHORT":
        return "SELL"
    raise RuntimeError(f"invalid pos_dir for entry side (STRICT): {pos_dir!r}")


def _exit_side_from_dir(pos_dir: str) -> str:
    if pos_dir == "LONG":
        return "SELL"
    if pos_dir == "SHORT":
        return "BUY"
    raise RuntimeError(f"invalid pos_dir for exit side (STRICT): {pos_dir!r}")


def _normalize_entry_side_strict(v: Any, name: str) -> str:
    s = str(v or "").upper().strip()
    if s in ("BUY", "LONG"):
        return "BUY"
    if s in ("SELL", "SHORT"):
        return "SELL"
    raise RuntimeError(f"{name} must be BUY/SELL/LONG/SHORT (STRICT), got={v!r}")


def _normalize_position_side_raw(v: Any, *, default_both: bool = True) -> str:
    s = str(v or "").upper().strip()
    if not s:
        return "BOTH" if default_both else ""
    if s not in ("BOTH", "LONG", "SHORT"):
        raise RuntimeError(f"invalid positionSide (STRICT): {s!r}")
    return s


def _is_tp_order(o: Dict[str, Any]) -> bool:
    t = str(o.get("type", "")).upper().strip()
    return t in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT")


def _is_sl_order(o: Dict[str, Any]) -> bool:
    t = str(o.get("type", "")).upper().strip()
    return t in ("STOP_MARKET", "STOP")


def _is_protective_order_strict(o: Dict[str, Any]) -> bool:
    reduce_only = bool(o.get("reduceOnly", False))
    close_position = bool(o.get("closePosition", False))
    return bool(reduce_only or close_position)


def _order_price(o: Dict[str, Any]) -> Optional[float]:
    sp = o.get("stopPrice")
    if sp not in (None, "", "0", "0.0", 0):
        v = _safe_float_strict(sp, "order.stopPrice")
        return v if v > 0 else None

    pr = o.get("price")
    if pr not in (None, "", "0", "0.0", 0):
        v = _safe_float_strict(pr, "order.price")
        return v if v > 0 else None

    return None


def _req_float_from_dict(d: Dict[str, Any], key: str) -> float:
    if key not in d:
        raise RuntimeError(f"exchange position missing required field: {key}")
    val = d.get(key)
    if val is None:
        raise RuntimeError(f"exchange position field is null: {key}")
    try:
        f = float(val)
    except Exception as e:
        raise RuntimeError(f"exchange position field not a number: {key}") from e
    if not math.isfinite(f):
        raise RuntimeError(f"exchange position field not finite: {key}")
    return f


def _req_float_from_dict_multi(d: Dict[str, Any], keys: Tuple[str, ...], *, name: str) -> float:
    for k in keys:
        if k in d and d.get(k) is not None:
            try:
                f = float(d.get(k))
            except Exception as e:
                raise RuntimeError(f"exchange position field not a number: {k}") from e
            if not math.isfinite(f):
                raise RuntimeError(f"exchange position field not finite: {k}")
            return f
    raise RuntimeError(f"exchange position missing required field for {name}: {list(keys)}")


def _notify_sync_ok(symbol: str, *, pos_dir: str, abs_qty: float, tp_id: Optional[str], sl_id: Optional[str]) -> None:
    msg = f"🔁 [SYNC_OK] {symbol} pos={pos_dir} qty={abs_qty:.6f} tp={tp_id} sl={sl_id}"
    try:
        send_tg(msg)
    except Exception as e:
        log(f"[SYNC_OK][TG_FAIL] {type(e).__name__}: {e}")


def _notify_sync_recovered(symbol: str, *, pos_dir: str, abs_qty: float, trade_id: int) -> None:
    msg = f"♻️ [SYNC_RECOVERED] {symbol} pos={pos_dir} qty={abs_qty:.6f} trade_id={trade_id}"
    try:
        send_tg(msg)
    except Exception as e:
        log(f"[SYNC_RECOVERED][TG_FAIL] {type(e).__name__}: {e}")


def _notify_sync_recovery_required(symbol: str, *, reason: str) -> None:
    msg = f"⛔ [SYNC_RECOVERY_REQUIRED] {symbol} reason={reason}"
    try:
        send_tg(msg)
    except Exception as e:
        log(f"[SYNC_RECOVERY_REQUIRED][TG_FAIL] {type(e).__name__}: {e}")


def _set_db_mismatch_and_commit(session, db_trade: TradeORM, *, status: str) -> None:
    status_s = _require_nonempty_str(status, "status")
    db_trade.reconciliation_status = status_s
    db_trade.last_synced_at = _utc_now()
    session.add(db_trade)
    session.commit()


# ─────────────────────────────────────────────────────────────
# Position event / contracts
# ─────────────────────────────────────────────────────────────
def _emit_position_sync_event_strict(
    *,
    symbol: str,
    db_trade: TradeORM,
    pos_dir: str,
    abs_qty: float,
    entry_price: float,
    tp_price: Optional[float],
    sl_price: Optional[float],
    reason: str = "position_synced",
) -> None:
    symbol_s = _normalize_symbol(symbol)
    pos_dir_s = _require_nonempty_str(pos_dir, "pos_dir").upper()
    if pos_dir_s not in ("LONG", "SHORT"):
        raise RuntimeError(f"invalid pos_dir (STRICT): {pos_dir_s!r}")

    if abs_qty <= 0:
        raise RuntimeError("abs_qty must be > 0 (STRICT)")
    if entry_price <= 0:
        raise RuntimeError("entry_price must be > 0 (STRICT)")

    regime = _require_nonempty_str(db_trade.regime_at_entry, "bt_trades.regime_at_entry")
    strategy = _require_nonempty_str(db_trade.strategy, "bt_trades.strategy")
    source = _require_nonempty_str(db_trade.source, "bt_trades.source")
    exchange_position_side = _normalize_position_side_raw(
        db_trade.exchange_position_side,
        default_both=True,
    )
    reconciliation_status = _require_nonempty_str(
        db_trade.reconciliation_status,
        "bt_trades.reconciliation_status",
    ).upper()
    remaining_qty = _safe_float_strict(db_trade.remaining_qty, "bt_trades.remaining_qty")
    if remaining_qty <= 0:
        raise RuntimeError("bt_trades.remaining_qty must be > 0 (STRICT)")

    realized_pnl_usdt = 0.0
    if db_trade.realized_pnl_usdt is not None:
        realized_pnl_usdt = _safe_float_strict(
            db_trade.realized_pnl_usdt,
            "bt_trades.realized_pnl_usdt",
        )

    leverage = _safe_positive_int_strict(db_trade.leverage, "bt_trades.leverage")
    tp_pct = _safe_float_strict(db_trade.tp_pct, "bt_trades.tp_pct")
    sl_pct = _safe_float_strict(db_trade.sl_pct, "bt_trades.sl_pct")
    risk_pct = _safe_float_strict(db_trade.risk_pct, "bt_trades.risk_pct")
    entry_ts = _require_tz_aware_datetime(db_trade.entry_ts, "bt_trades.entry_ts")

    extra_json: Dict[str, Any] = {
        "state": "OPEN",
        "strategy": strategy,
        "source": source,
        "trade_id": int(db_trade.id),
        "entry_order_id": (str(db_trade.entry_order_id) if db_trade.entry_order_id is not None else None),
        "tp_order_id": (str(db_trade.tp_order_id) if db_trade.tp_order_id is not None else None),
        "sl_order_id": (str(db_trade.sl_order_id) if db_trade.sl_order_id is not None else None),
        "exchange_position_side": exchange_position_side,
        "remaining_qty": float(remaining_qty),
        "realized_pnl_usdt": float(realized_pnl_usdt),
        "reconciliation_status": reconciliation_status,
        "entry_ts": entry_ts.astimezone(timezone.utc).isoformat(),
        "tp_price": (float(tp_price) if tp_price is not None else None),
        "sl_price": (float(sl_price) if sl_price is not None else None),
    }

    log_event(
        "POSITION",
        symbol=symbol_s,
        regime=regime,
        source="sync_exchange",
        side=pos_dir_s,
        price=float(entry_price),
        qty=float(abs_qty),
        leverage=float(leverage),
        tp_pct=float(tp_pct),
        sl_pct=float(sl_pct),
        risk_pct=float(risk_pct),
        reason=_require_nonempty_str(reason, "reason"),
        extra_json=extra_json,
    )


def _validate_db_trade_contract_before_sync_strict(db_trade: TradeORM) -> None:
    _normalize_symbol(db_trade.symbol)
    _require_nonempty_str(db_trade.strategy, "bt_trades.strategy")
    _require_nonempty_str(db_trade.regime_at_entry, "bt_trades.regime_at_entry")
    _require_nonempty_str(db_trade.source, "bt_trades.source")
    _normalize_entry_side_strict(db_trade.side, "bt_trades.side")
    _safe_float_strict(db_trade.qty, "bt_trades.qty")
    _safe_positive_int_strict(db_trade.leverage, "bt_trades.leverage")

    status = _require_nonempty_str(
        db_trade.reconciliation_status,
        "bt_trades.reconciliation_status",
    ).upper()

    if status == "PROTECTION_VERIFIED":
        if db_trade.entry_order_id is None or not str(db_trade.entry_order_id).strip():
            raise RuntimeError("PROTECTION_VERIFIED requires entry_order_id (STRICT)")
        if db_trade.tp_order_id is None or not str(db_trade.tp_order_id).strip():
            raise RuntimeError("PROTECTION_VERIFIED requires tp_order_id (STRICT)")
        if db_trade.sl_order_id is None or not str(db_trade.sl_order_id).strip():
            raise RuntimeError("PROTECTION_VERIFIED requires sl_order_id (STRICT)")
        if db_trade.remaining_qty is None:
            raise RuntimeError("PROTECTION_VERIFIED requires remaining_qty (STRICT)")
        _safe_float_strict(db_trade.remaining_qty, "bt_trades.remaining_qty")
        if db_trade.last_synced_at is None:
            raise RuntimeError("PROTECTION_VERIFIED requires last_synced_at (STRICT)")
        _require_tz_aware_datetime(db_trade.last_synced_at, "bt_trades.last_synced_at")


def _ensure_no_duplicate_memory_trade_strict(
    *,
    symbol: str,
    current_trades: List[MemTrade],
) -> None:
    sym = _normalize_symbol(symbol)
    dup_count = 0
    for t in current_trades:
        if not isinstance(t, MemTrade):
            raise RuntimeError("current_trades contains non-MemTrade item (STRICT)")
        if _normalize_symbol(t.symbol) == sym:
            dup_count += 1
    if dup_count > 0:
        raise RuntimeError(
            f"current_trades already contains OPEN trade for symbol={sym} (STRICT, n={dup_count})"
        )


def _assert_db_direction_matches_exchange_strict(
    *,
    db_trade: TradeORM,
    pos_dir: str,
) -> None:
    db_side = _normalize_entry_side_strict(db_trade.side, "bt_trades.side")
    expected_side = _entry_side_from_dir(pos_dir)
    if db_side != expected_side:
        raise RuntimeError(
            f"DB trade side mismatches exchange position direction (STRICT): "
            f"db_side={db_side} exch_expected_side={expected_side}"
        )


# ─────────────────────────────────────────────────────────────
# Memory-assisted recovery helpers
# ─────────────────────────────────────────────────────────────
def _find_single_memory_trade_or_none(
    *,
    symbol: str,
    current_trades: List[MemTrade],
) -> Optional[MemTrade]:
    sym = _normalize_symbol(symbol)
    matches: List[MemTrade] = []

    for t in current_trades:
        if not isinstance(t, MemTrade):
            raise RuntimeError("current_trades contains non-MemTrade item (STRICT)")
        if _normalize_symbol(getattr(t, "symbol", None)) == sym:
            matches.append(t)

    if len(matches) > 1:
        raise RuntimeError(f"multiple memory trades found for symbol={sym} (STRICT, n={len(matches)})")
    if len(matches) == 0:
        return None
    return matches[0]


def _extract_optional_meta_dict_from_mem_trade(mem_trade: MemTrade) -> Optional[Dict[str, Any]]:
    meta = getattr(mem_trade, "meta", None)
    if meta is None:
        return None
    if not isinstance(meta, dict):
        raise RuntimeError("mem_trade.meta must be dict when present (STRICT)")
    return dict(meta)


def _extract_nonempty_str_from_mem_trade_or_meta_strict(
    mem_trade: MemTrade,
    *,
    attr_candidates: Tuple[str, ...],
    meta_candidates: Tuple[str, ...],
    name: str,
) -> str:
    for attr_name in attr_candidates:
        if hasattr(mem_trade, attr_name):
            raw = getattr(mem_trade, attr_name)
            if raw is None:
                continue
            s = str(raw).strip()
            if s:
                return s

    meta = _extract_optional_meta_dict_from_mem_trade(mem_trade)
    if meta is not None:
        for key in meta_candidates:
            if key not in meta or meta[key] is None:
                continue
            s = str(meta[key]).strip()
            if s:
                return s

    raise RuntimeError(f"{name} missing in mem_trade/meta (STRICT)")


def _extract_float_from_mem_trade_or_meta_strict(
    mem_trade: MemTrade,
    *,
    attr_candidates: Tuple[str, ...],
    meta_candidates: Tuple[str, ...],
    name: str,
    min_value: Optional[float] = None,
) -> float:
    for attr_name in attr_candidates:
        if hasattr(mem_trade, attr_name):
            raw = getattr(mem_trade, attr_name)
            if raw is None:
                continue
            value = _safe_float_strict(raw, name)
            if min_value is not None and value < min_value:
                raise RuntimeError(f"{name} must be >= {min_value} (STRICT)")
            return value

    meta = _extract_optional_meta_dict_from_mem_trade(mem_trade)
    if meta is not None:
        for key in meta_candidates:
            if key not in meta or meta[key] is None:
                continue
            value = _safe_float_strict(meta[key], name)
            if min_value is not None and value < min_value:
                raise RuntimeError(f"{name} must be >= {min_value} (STRICT)")
            return value

    raise RuntimeError(f"{name} missing in mem_trade/meta (STRICT)")


def _derive_tp_pct_from_prices_strict(entry_price: float, tp_price: float, pos_dir: str) -> float:
    if entry_price <= 0 or tp_price <= 0:
        raise RuntimeError("entry_price/tp_price must be > 0 (STRICT)")
    if pos_dir == "LONG":
        pct = (tp_price - entry_price) / entry_price
    elif pos_dir == "SHORT":
        pct = (entry_price - tp_price) / entry_price
    else:
        raise RuntimeError(f"invalid pos_dir (STRICT): {pos_dir!r}")

    if not math.isfinite(pct) or pct <= 0:
        raise RuntimeError("derived tp_pct must be finite > 0 (STRICT)")
    return float(pct)


def _derive_sl_pct_from_prices_strict(entry_price: float, sl_price: float, pos_dir: str) -> float:
    if entry_price <= 0 or sl_price <= 0:
        raise RuntimeError("entry_price/sl_price must be > 0 (STRICT)")
    if pos_dir == "LONG":
        pct = (entry_price - sl_price) / entry_price
    elif pos_dir == "SHORT":
        pct = (sl_price - entry_price) / entry_price
    else:
        raise RuntimeError(f"invalid pos_dir (STRICT): {pos_dir!r}")

    if not math.isfinite(pct) or pct <= 0:
        raise RuntimeError("derived sl_pct must be finite > 0 (STRICT)")
    return float(pct)


def _set_model_field_if_exists(obj: Any, field_name: str, value: Any) -> None:
    if hasattr(obj, field_name):
        setattr(obj, field_name, value)


def _recover_db_trade_from_memory_trade_or_raise(
    session,
    *,
    symbol: str,
    mem_trade: MemTrade,
    pos_dir: str,
    abs_qty: float,
    entry_price: float,
    position_side_raw: str,
    tp_id: str,
    tp_price: Optional[float],
    sl_id: str,
    sl_price: Optional[float],
) -> TradeORM:
    sym = _normalize_symbol(symbol)
    if _normalize_symbol(getattr(mem_trade, "symbol", None)) != sym:
        raise RuntimeError("mem_trade.symbol mismatch during recovery (STRICT)")

    mem_side = _normalize_entry_side_strict(getattr(mem_trade, "side", None), "mem_trade.side")
    expected_side = _entry_side_from_dir(pos_dir)
    if mem_side != expected_side:
        raise RuntimeError(
            f"mem_trade.side mismatches exchange position direction (STRICT): mem_side={mem_side} exch_expected={expected_side}"
        )

    mem_qty = _safe_float_strict(getattr(mem_trade, "qty", None), "mem_trade.qty")
    if mem_qty <= 0:
        raise RuntimeError("mem_trade.qty must be > 0 (STRICT)")
    if abs_qty - mem_qty > 1e-12:
        raise RuntimeError(
            f"exchange abs_qty exceeds mem_trade.qty (STRICT): mem_qty={mem_qty} exch_abs_qty={abs_qty}"
        )

    mem_entry_order_id = _require_nonempty_str(getattr(mem_trade, "entry_order_id", None), "mem_trade.entry_order_id")
    mem_tp_order_id = _require_nonempty_str(getattr(mem_trade, "tp_order_id", None), "mem_trade.tp_order_id")
    mem_sl_order_id = _require_nonempty_str(getattr(mem_trade, "sl_order_id", None), "mem_trade.sl_order_id")
    if mem_tp_order_id != tp_id:
        raise RuntimeError(
            f"mem_trade.tp_order_id mismatches exchange tp_id (STRICT): mem={mem_tp_order_id} exch={tp_id}"
        )
    if mem_sl_order_id != sl_id:
        raise RuntimeError(
            f"mem_trade.sl_order_id mismatches exchange sl_id (STRICT): mem={mem_sl_order_id} exch={sl_id}"
        )

    leverage = _safe_positive_int_strict(getattr(mem_trade, "leverage", None), "mem_trade.leverage")
    entry_ts = _require_tz_aware_datetime(getattr(mem_trade, "entry_ts", None), "mem_trade.entry_ts")

    source = _extract_nonempty_str_from_mem_trade_or_meta_strict(
        mem_trade,
        attr_candidates=("source",),
        meta_candidates=("source", "signal_source", "strategy_source", "entry_source", "strategy_type"),
        name="mem_trade.source",
    )
    strategy = _extract_nonempty_str_from_mem_trade_or_meta_strict(
        mem_trade,
        attr_candidates=("strategy",),
        meta_candidates=("strategy", "strategy_type", "source"),
        name="mem_trade.strategy",
    )
    regime_at_entry = _extract_nonempty_str_from_mem_trade_or_meta_strict(
        mem_trade,
        attr_candidates=("regime_at_entry", "regime"),
        meta_candidates=("regime_at_entry", "regime", "market_regime"),
        name="mem_trade.regime_at_entry",
    )

    risk_pct = _extract_float_from_mem_trade_or_meta_strict(
        mem_trade,
        attr_candidates=("risk_pct",),
        meta_candidates=("risk_pct", "allocation_ratio", "dynamic_allocation_ratio", "dynamic_risk_pct"),
        name="mem_trade.risk_pct",
        min_value=0.0,
    )
    if risk_pct <= 0:
        raise RuntimeError("mem_trade.risk_pct must be > 0 (STRICT)")

    if tp_price is not None and tp_price > 0:
        tp_pct = _derive_tp_pct_from_prices_strict(entry_price, tp_price, pos_dir)
    else:
        tp_pct = _extract_float_from_mem_trade_or_meta_strict(
            mem_trade,
            attr_candidates=("tp_pct",),
            meta_candidates=("tp_pct", "take_profit_pct", "target_pct"),
            name="mem_trade.tp_pct",
            min_value=0.0,
        )
        if tp_pct <= 0:
            raise RuntimeError("mem_trade.tp_pct must be > 0 (STRICT)")

    if sl_price is not None and sl_price > 0:
        sl_pct = _derive_sl_pct_from_prices_strict(entry_price, sl_price, pos_dir)
    else:
        sl_pct = _extract_float_from_mem_trade_or_meta_strict(
            mem_trade,
            attr_candidates=("sl_pct",),
            meta_candidates=("sl_pct", "stop_loss_pct", "stop_pct"),
            name="mem_trade.sl_pct",
            min_value=0.0,
        )
        if sl_pct <= 0:
            raise RuntimeError("mem_trade.sl_pct must be > 0 (STRICT)")

    db_trade = TradeORM()
    db_trade.symbol = sym
    db_trade.side = expected_side
    db_trade.qty = float(mem_qty)
    db_trade.leverage = int(leverage)
    db_trade.strategy = strategy
    db_trade.regime_at_entry = regime_at_entry
    db_trade.source = source
    db_trade.entry_ts = entry_ts
    db_trade.entry_order_id = mem_entry_order_id
    db_trade.tp_order_id = mem_tp_order_id
    db_trade.sl_order_id = mem_sl_order_id
    db_trade.exchange_position_side = _normalize_position_side_raw(position_side_raw, default_both=True)
    db_trade.remaining_qty = float(abs_qty)
    db_trade.reconciliation_status = "PROTECTION_VERIFIED"
    db_trade.last_synced_at = _utc_now()
    db_trade.tp_pct = float(tp_pct)
    db_trade.sl_pct = float(sl_pct)
    db_trade.risk_pct = float(risk_pct)

    if hasattr(mem_trade, "realized_pnl_usdt"):
        realized = getattr(mem_trade, "realized_pnl_usdt")
        db_trade.realized_pnl_usdt = None if realized is None else float(_safe_float_strict(realized, "mem_trade.realized_pnl_usdt"))
    else:
        db_trade.realized_pnl_usdt = None

    _set_model_field_if_exists(db_trade, "exit_ts", None)
    _set_model_field_if_exists(db_trade, "entry_price", float(entry_price))
    _set_model_field_if_exists(db_trade, "entry", float(entry_price))
    _set_model_field_if_exists(db_trade, "client_entry_id", getattr(mem_trade, "client_entry_id", None))
    _set_model_field_if_exists(db_trade, "is_test", False)

    session.add(db_trade)
    session.flush()

    if getattr(db_trade, "id", None) is None:
        raise RuntimeError("recovered db trade id missing after flush (STRICT)")

    _validate_db_trade_contract_before_sync_strict(db_trade)
    return db_trade


# ─────────────────────────────────────────────────────────────
# Exchange snapshot (STRICT)
# ─────────────────────────────────────────────────────────────
def _fetch_one_way_position_strict(symbol: str) -> Tuple[str, float, float, str]:
    """
    Returns:
      (pos_dir, abs_qty, entry_price, position_side_raw)

    STRICT:
    - Hedge 모드(동일 심볼에서 LONG/SHORT 동시) 감지 시 즉시 예외
    - position row가 여러 개면 방향/positionSide topology 검증 후 합산
    - open position이 없으면 ("", 0, 0, "") 반환
    """
    sym = _normalize_symbol(symbol)
    rows = fetch_open_positions(sym)
    if not isinstance(rows, list):
        raise RuntimeError("fetch_open_positions returned non-list (STRICT)")

    live: List[Dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            raise RuntimeError("fetch_open_positions contains non-dict row (STRICT)")
        if _normalize_symbol(r.get("symbol", "")) != sym:
            continue
        amt = _safe_float_strict(r.get("positionAmt"), "positionRisk.positionAmt")
        if abs(amt) > 1e-12:
            live.append(r)

    if not live:
        return "", 0.0, 0.0, ""

    dirs: set[str] = set()
    pos_sides: set[str] = set()
    total_signed_amt = 0.0
    weighted_entry_sum = 0.0

    for i, r in enumerate(live):
        amt = _safe_float_strict(r.get("positionAmt"), f"positionRisk[{i}].positionAmt")
        entry = _safe_float_strict(r.get("entryPrice"), f"positionRisk[{i}].entryPrice")
        if entry <= 0:
            raise RuntimeError("positionRisk.entryPrice must be > 0 (STRICT)")

        ps = _normalize_position_side_raw(r.get("positionSide"), default_both=True)
        pos_sides.add(ps)

        row_dir = ps if ps in ("LONG", "SHORT") else _position_dir_one_way(amt)
        dirs.add(row_dir)

        total_signed_amt += amt
        weighted_entry_sum += abs(amt) * entry

    if len(dirs) != 1:
        raise RuntimeError(f"HEDGE mode detected or ambiguous positions (STRICT): dirs={sorted(list(dirs))}")

    if "BOTH" in pos_sides and len(pos_sides) > 1:
        raise RuntimeError(
            f"ambiguous positionSide topology (STRICT): mixed BOTH and directional rows pos_sides={sorted(list(pos_sides))}"
        )

    if abs(total_signed_amt) <= 1e-12:
        raise RuntimeError("aggregated positionAmt became zero unexpectedly (STRICT)")

    abs_qty = abs(float(total_signed_amt))
    if abs_qty <= 0:
        raise RuntimeError("aggregated abs_qty must be > 0 (STRICT)")

    entry_price = weighted_entry_sum / abs_qty
    if not math.isfinite(entry_price) or entry_price <= 0:
        raise RuntimeError("aggregated entry_price invalid (STRICT)")

    pos_dir = _position_dir_one_way(float(total_signed_amt))
    position_side_raw = "BOTH" if pos_sides == {"BOTH"} else list(pos_sides)[0]

    return pos_dir, abs_qty, float(entry_price), position_side_raw


def _pick_unique_tp_sl_orders_strict(
    symbol: str,
    *,
    pos_dir: str,
    position_side_raw: str,
    open_orders: List[Dict[str, Any]],
) -> Tuple[Optional[str], Optional[float], Optional[str], Optional[float]]:
    """
    STRICT:
    - TP 후보가 2개 이상이면 예외
    - SL 후보가 2개 이상이면 예외
    - reduceOnly=True 또는 closePosition=True 보호 주문만 인정
    - TP/SL orderId 동일 금지
    - 보호가격이 존재하면 > 0 이어야 함
    """
    sym = _normalize_symbol(symbol)
    exit_side = _exit_side_from_dir(pos_dir)
    ps_raw = _normalize_position_side_raw(position_side_raw, default_both=True)

    tp_cands: List[Dict[str, Any]] = []
    sl_cands: List[Dict[str, Any]] = []

    for o in open_orders:
        if not isinstance(o, dict):
            raise RuntimeError("open_orders contains non-dict row (STRICT)")

        if _normalize_symbol(o.get("symbol", "")) != sym:
            continue

        o_side = str(o.get("side", "")).upper().strip()
        if o_side != exit_side:
            continue

        o_ps = _normalize_position_side_raw(o.get("positionSide"), default_both=True)
        if ps_raw in ("LONG", "SHORT") and o_ps in ("LONG", "SHORT") and o_ps != ps_raw:
            continue

        if not _is_protective_order_strict(o):
            continue

        if _is_tp_order(o):
            tp_cands.append(o)
        elif _is_sl_order(o):
            sl_cands.append(o)

    if len(tp_cands) > 1:
        raise RuntimeError(f"multiple TP candidates found (STRICT): n={len(tp_cands)}")
    if len(sl_cands) > 1:
        raise RuntimeError(f"multiple SL candidates found (STRICT): n={len(sl_cands)}")

    tp_id: Optional[str] = None
    tp_price: Optional[float] = None
    if len(tp_cands) == 1:
        oid = tp_cands[0].get("orderId")
        if oid is None:
            raise RuntimeError("TP candidate missing orderId (STRICT)")
        tp_id = str(oid)
        tp_price = _order_price(tp_cands[0])
        if tp_price is not None and tp_price <= 0:
            raise RuntimeError("TP price must be > 0 when present (STRICT)")

    sl_id: Optional[str] = None
    sl_price: Optional[float] = None
    if len(sl_cands) == 1:
        oid = sl_cands[0].get("orderId")
        if oid is None:
            raise RuntimeError("SL candidate missing orderId (STRICT)")
        sl_id = str(oid)
        sl_price = _order_price(sl_cands[0])
        if sl_price is not None and sl_price <= 0:
            raise RuntimeError("SL price must be > 0 when present (STRICT)")

    if tp_id is not None and sl_id is not None and tp_id == sl_id:
        raise RuntimeError(f"TP/SL orderId must be distinct (STRICT): orderId={tp_id}")

    return tp_id, tp_price, sl_id, sl_price


# ─────────────────────────────────────────────────────────────
# DB reconciliation (STRICT)
# ─────────────────────────────────────────────────────────────
def _load_db_open_trade_strict(session, symbol: str) -> Optional[TradeORM]:
    sym = _normalize_symbol(symbol)
    rows = (
        session.query(TradeORM)
        .filter(TradeORM.symbol == sym)
        .filter(TradeORM.exit_ts.is_(None))
        .order_by(TradeORM.entry_ts.desc())
        .all()
    )

    if len(rows) == 0:
        return None
    if len(rows) > 1:
        raise RuntimeError(f"multiple OPEN trades in DB for symbol={sym} (STRICT, n={len(rows)})")
    return rows[0]


def _update_trade_exec_fields_strict(
    session,
    *,
    db_trade: TradeORM,
    pos_dir: str,
    abs_qty: float,
    entry_price: float,
    position_side_raw: str,
    tp_id: Optional[str],
    sl_id: Optional[str],
) -> None:
    """
    STRICT update rules:
    - DB에 값이 있는데 거래소와 불일치하면 즉시 예외
    - DB에 값이 없고 거래소에 값이 있으면 거래소 사실로 채운다
    """
    if abs_qty <= 0:
        raise RuntimeError("abs_qty must be > 0 (STRICT)")
    if entry_price <= 0:
        raise RuntimeError("entry_price must be > 0 (STRICT)")
    if pos_dir not in ("LONG", "SHORT"):
        raise RuntimeError(f"invalid pos_dir (STRICT): {pos_dir!r}")

    _assert_db_direction_matches_exchange_strict(db_trade=db_trade, pos_dir=pos_dir)

    db_qty = _safe_float_strict(db_trade.qty, "bt_trades.qty")
    if db_qty <= 0:
        raise RuntimeError("bt_trades.qty must be > 0 (STRICT)")
    if abs_qty - db_qty > 1e-12:
        raise RuntimeError(
            f"exchange remaining abs_qty exceeds original db qty (STRICT): db_qty={db_qty} exch_abs_qty={abs_qty}"
        )

    if db_trade.entry_order_id is None or not str(db_trade.entry_order_id).strip():
        raise RuntimeError("ENTRY orderId is missing in DB (STRICT)")

    db_trade.remaining_qty = float(abs_qty)

    ps = _normalize_position_side_raw(position_side_raw, default_both=True)
    if db_trade.exchange_position_side is not None:
        db_ps = _normalize_position_side_raw(db_trade.exchange_position_side, default_both=True)
        if db_ps != ps:
            raise RuntimeError(
                f"exchange_position_side mismatch (STRICT): db={db_ps} exch={ps}"
            )
    db_trade.exchange_position_side = ps

    if db_trade.tp_order_id is not None and tp_id is not None:
        if str(db_trade.tp_order_id) != str(tp_id):
            raise RuntimeError(f"TP orderId mismatch (STRICT): db={db_trade.tp_order_id} exch={tp_id}")

    if db_trade.sl_order_id is not None and sl_id is not None:
        if str(db_trade.sl_order_id) != str(sl_id):
            raise RuntimeError(f"SL orderId mismatch (STRICT): db={db_trade.sl_order_id} exch={sl_id}")

    if db_trade.tp_order_id is None and tp_id is not None:
        db_trade.tp_order_id = str(tp_id)

    if db_trade.sl_order_id is None and sl_id is not None:
        db_trade.sl_order_id = str(sl_id)

    if db_trade.tp_order_id is None:
        raise RuntimeError("TP orderId is missing (STRICT, protection order required)")
    if db_trade.sl_order_id is None:
        raise RuntimeError("SL orderId is missing (STRICT, protection order required)")
    if str(db_trade.tp_order_id) == str(db_trade.sl_order_id):
        raise RuntimeError("TP/SL orderId must be distinct in DB (STRICT)")

    if db_trade.realized_pnl_usdt is None:
        db_trade.realized_pnl_usdt = 0.0

    db_trade.reconciliation_status = "PROTECTION_VERIFIED"
    db_trade.last_synced_at = _utc_now()
    session.add(db_trade)


def _build_mem_trade_strict(
    *,
    db_trade: TradeORM,
    pos_dir: str,
    abs_qty: float,
    entry_price: float,
    tp_price: Optional[float],
    sl_price: Optional[float],
) -> MemTrade:
    if abs_qty <= 0:
        raise RuntimeError("abs_qty must be > 0 (STRICT)")
    if entry_price <= 0:
        raise RuntimeError("entry_price must be > 0 (STRICT)")

    leverage_i = _safe_positive_int_strict(db_trade.leverage, "bt_trades.leverage")
    exchange_position_side = _normalize_position_side_raw(db_trade.exchange_position_side, default_both=True)

    if db_trade.entry_order_id is None or not str(db_trade.entry_order_id).strip():
        raise RuntimeError("bt_trades.entry_order_id is required after reconciliation (STRICT)")
    if db_trade.tp_order_id is None or not str(db_trade.tp_order_id).strip():
        raise RuntimeError("bt_trades.tp_order_id is required after reconciliation (STRICT)")
    if db_trade.sl_order_id is None or not str(db_trade.sl_order_id).strip():
        raise RuntimeError("bt_trades.sl_order_id is required after reconciliation (STRICT)")

    original_qty = _safe_float_strict(db_trade.qty, "bt_trades.qty")
    if original_qty <= 0:
        raise RuntimeError("bt_trades.qty must be > 0 (STRICT)")

    if db_trade.remaining_qty is None:
        raise RuntimeError("bt_trades.remaining_qty is required after reconciliation (STRICT)")
    remaining_qty = _safe_float_strict(db_trade.remaining_qty, "bt_trades.remaining_qty")
    if remaining_qty <= 0:
        raise RuntimeError("bt_trades.remaining_qty must be > 0 (STRICT)")
    if remaining_qty - original_qty > 1e-12:
        raise RuntimeError(
            f"bt_trades.remaining_qty must be <= bt_trades.qty (STRICT): remaining={remaining_qty} qty={original_qty}"
        )

    last_synced_at = _require_tz_aware_datetime(db_trade.last_synced_at, "bt_trades.last_synced_at")
    reconciliation_status = _require_nonempty_str(db_trade.reconciliation_status, "bt_trades.reconciliation_status").upper()
    if reconciliation_status != "PROTECTION_VERIFIED":
        raise RuntimeError(
            f"bt_trades.reconciliation_status must be PROTECTION_VERIFIED (STRICT), got={reconciliation_status!r}"
        )

    realized_pnl = 0.0
    if db_trade.realized_pnl_usdt is not None:
        realized_pnl = _safe_float_strict(db_trade.realized_pnl_usdt, "bt_trades.realized_pnl_usdt")

    return MemTrade(
        symbol=_normalize_symbol(db_trade.symbol),
        side=_entry_side_from_dir(pos_dir),
        qty=float(original_qty),
        entry_price=float(entry_price),
        leverage=int(leverage_i),
        entry=float(entry_price),
        entry_order_id=str(db_trade.entry_order_id),
        tp_order_id=str(db_trade.tp_order_id),
        sl_order_id=str(db_trade.sl_order_id),
        tp_price=float(tp_price) if tp_price is not None else 0.0,
        sl_price=float(sl_price) if sl_price is not None else 0.0,
        source="SYNC",
        id=int(db_trade.id) if int(db_trade.id) > 0 else 0,
        exchange_position_side=exchange_position_side,
        remaining_qty=float(remaining_qty),
        realized_pnl_usdt=float(realized_pnl),
        reconciliation_status=reconciliation_status,
        last_synced_at=last_synced_at,
    )


# ─────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────
def sync_open_trades_from_exchange(
    symbol: str,
    replace: bool = True,
    current_trades: Optional[List[MemTrade]] = None,
) -> Tuple[List[MemTrade], int]:
    """
    STRICT:
    - REST 실패/응답 이상/정합 불일치 시 즉시 예외
    - DB OPEN 트레이드가 없는데 거래소 포지션이 있으면 current_trades 기반 복구 가능 시에만 복구
    - DB OPEN 트레이드가 있는데 거래소 포지션이 없으면 즉시 예외
    - blind fallback 없이 복구 불가능 상태는 SyncRecoveryRequiredError 로 승격
    - 결과로 반환되는 Trade 리스트는 DB+거래소 정합을 통과한 상태만 포함

    Returns:
      (rebuilt_trades, exchange_pos_count)
    """
    sym = _normalize_symbol(symbol)
    if current_trades is None:
        current_trades = []
    if not isinstance(current_trades, list):
        raise RuntimeError("current_trades must be list (STRICT)")
    if not replace:
        _ensure_no_duplicate_memory_trade_strict(symbol=sym, current_trades=current_trades)

    pos_dir, abs_qty, entry_price, position_side_raw = _fetch_one_way_position_strict(sym)

    orders = fetch_open_orders(sym)
    if not isinstance(orders, list):
        raise RuntimeError("fetch_open_orders returned non-list (STRICT)")
    open_orders = [o for o in orders if isinstance(o, dict)]
    if len(open_orders) != len(orders):
        raise RuntimeError("fetch_open_orders contains non-dict row (STRICT)")

    rebuilt: Optional[MemTrade] = None
    db_open: Optional[TradeORM] = None
    tp_id: Optional[str] = None
    tp_price: Optional[float] = None
    sl_id: Optional[str] = None
    sl_price: Optional[float] = None
    recovered_from_memory = False

    with get_session() as session:
        db_open = _load_db_open_trade_strict(session, sym)

        if abs_qty <= 0:
            if db_open is not None:
                _set_db_mismatch_and_commit(session, db_open, status="MISMATCH_NO_POSITION")
                raise RuntimeError(f"DB has OPEN trade but exchange has NO position (STRICT, symbol={sym})")

            return ([], 0) if replace else (current_trades, 0)

        tp_id, tp_price, sl_id, sl_price = _pick_unique_tp_sl_orders_strict(
            sym,
            pos_dir=pos_dir,
            position_side_raw=position_side_raw,
            open_orders=open_orders,
        )

        if tp_id is None or sl_id is None:
            if db_open is not None:
                _set_db_mismatch_and_commit(session, db_open, status="MISMATCH_PROTECTION_ORDERS")
            raise RuntimeError(
                f"exchange protection orders missing (STRICT, symbol={sym}, tp_id={tp_id}, sl_id={sl_id})"
            )

        if db_open is None:
            mem_trade = _find_single_memory_trade_or_none(symbol=sym, current_trades=current_trades)
            if mem_trade is None:
                _notify_sync_recovery_required(
                    sym,
                    reason="exchange_open_position_but_db_missing_and_no_memory_trade",
                )
                raise SyncRecoveryRequiredError(
                    f"exchange has OPEN position but DB has NO OPEN trade and no memory trade is available "
                    f"(STRICT, symbol={sym})"
                )

            db_open = _recover_db_trade_from_memory_trade_or_raise(
                session,
                symbol=sym,
                mem_trade=mem_trade,
                pos_dir=pos_dir,
                abs_qty=abs_qty,
                entry_price=entry_price,
                position_side_raw=position_side_raw,
                tp_id=tp_id,
                tp_price=tp_price,
                sl_id=sl_id,
                sl_price=sl_price,
            )
            recovered_from_memory = True

        _validate_db_trade_contract_before_sync_strict(db_open)

        _update_trade_exec_fields_strict(
            session,
            db_trade=db_open,
            pos_dir=pos_dir,
            abs_qty=abs_qty,
            entry_price=entry_price,
            position_side_raw=position_side_raw,
            tp_id=tp_id,
            sl_id=sl_id,
        )
        session.flush()

        _emit_position_sync_event_strict(
            symbol=sym,
            db_trade=db_open,
            pos_dir=pos_dir,
            abs_qty=abs_qty,
            entry_price=entry_price,
            tp_price=tp_price,
            sl_price=sl_price,
            reason=("position_recovered_from_memory" if recovered_from_memory else "position_synced"),
        )

        rebuilt = _build_mem_trade_strict(
            db_trade=db_open,
            pos_dir=pos_dir,
            abs_qty=abs_qty,
            entry_price=entry_price,
            tp_price=tp_price,
            sl_price=sl_price,
        )

        session.commit()

    if rebuilt is None:
        raise RuntimeError("rebuilt trade missing after sync (STRICT)")

    out = [rebuilt] if replace else (list(current_trades) + [rebuilt])

    if recovered_from_memory:
        _notify_sync_recovered(sym, pos_dir=pos_dir, abs_qty=abs_qty, trade_id=int(rebuilt.id))
    else:
        _notify_sync_ok(sym, pos_dir=pos_dir, abs_qty=abs_qty, tp_id=tp_id, sl_id=sl_id)

    return out, 1


__all__ = [
    "SyncRecoveryRequiredError",
    "sync_open_trades_from_exchange",
]