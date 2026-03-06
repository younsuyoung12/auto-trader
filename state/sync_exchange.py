"""
========================================================
FILE: state/sync_exchange.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
sync_exchange.py (Binance USDT-M Futures 전용, 거래소 상태 동기화 + DB 정합)

역할
-----------------------------------------------------
- 거래소(Binance USDT-M Futures)의 실제 포지션/미체결 주문 상태를 읽어
  DB(bt_trades) OPEN 트레이드와 정합을 강제한다.
- 엔진 재시작(무감독 24/7) 시:
  - DB OPEN 트레이드 1건 ↔ 거래소 OPEN 포지션 1건(One-way) 정합을 확인하고
  - 필요한 실행 필드(tp/sl orderId 등)를 DB에 “거래소 사실”로 확정 저장한다.
- 폴백 금지:
  - REST 실패, 응답 이상, 정합 불일치(모호/중복/누락) 발생 시 즉시 예외.

전제(운영 모드)
-----------------------------------------------------
- One-way 모드(단일 포지션) 기준으로 설계한다.
  (Hedge 모드 감지 시 즉시 예외)
- bt_trades는 exit_ts IS NULL 인 OPEN 트레이드가 심볼당 최대 1건이어야 한다.
- TP/SL 보호 주문은 reduceOnly=True 또는 closePosition=True +
  (STOP_MARKET / TAKE_PROFIT_MARKET)로 운영한다.

PATCH NOTES — 2026-03-02
--------------------------------------------------------
- execution/sync_exchange.py 위치 금지 → state/sync_exchange.py 로 고정
- (9점대 핵심) DB(bt_trades) ↔ 거래소(포지션/오더) 정합 강제
  - entry_order_id / tp_order_id / sl_order_id
  - exchange_position_side / remaining_qty / realized_pnl_usdt
  - reconciliation_status / last_synced_at
- 모호성 금지:
  - TP 후보 2개 이상, SL 후보 2개 이상 → 즉시 예외
  - DB OPEN 2개 이상 → 즉시 예외
  - Hedge(동일 심볼 LONG+SHORT 동시) → 즉시 예외

변경 이력
--------------------------------------------------------
- 2026-03-06:
  1) 거래소 포지션 집계 로직 강화
     - 동일 방향 다중 row를 live[0]로 선택하지 않고 전체 합산
     - abs(positionAmt) 가중 평균 entryPrice 계산
     - BOTH/LONG/SHORT 혼재 topology는 즉시 예외
  2) TP/SL 보호 주문 판별 강화
     - reduceOnly=True 뿐 아니라 closePosition=True 도 보호 주문으로 인정
  3) 메모리 Trade 재구성 STRICT 강화
     - leverage NULL fallback 제거
     - last_synced_at / remaining_qty / reconciliation_status 필수 정합 강화
- 2026-03-06:
  1) 보호주문 정합 강화
     - TP, SL 둘 다 필수 보호 주문으로 강제
     - 거래소 보호주문 누락 시 즉시 MISMATCH 상태 기록 후 예외
  2) reconciliation_status 정합 변경
     - OK → PROTECTION_VERIFIED 로 승격
  3) DB/메모리 정합 강화
     - tp_order_id / sl_order_id 모두 존재해야 재구성 허용
========================================================
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from execution.exchange_api import fetch_open_orders, fetch_open_positions
from infra.telelog import log, send_tg
from state.db_core import SessionLocal
from state.db_models import Trade as TradeORM
from state.trader_state import Trade as MemTrade


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


def _notify_sync_ok(symbol: str, *, pos_dir: str, abs_qty: float, tp_id: Optional[str], sl_id: Optional[str]) -> None:
    msg = f"🔁 [SYNC_OK] {symbol} pos={pos_dir} qty={abs_qty:.6f} tp={tp_id} sl={sl_id}"
    try:
        send_tg(msg)
    except Exception as e:
        log(f"[SYNC_OK][TG_FAIL] {type(e).__name__}: {e}")


def _set_db_mismatch_and_commit(session, db_trade: TradeORM, *, status: str) -> None:
    status_s = _require_nonempty_str(status, "status")
    db_trade.reconciliation_status = status_s
    db_trade.last_synced_at = _utc_now()
    session.add(db_trade)
    session.commit()


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

    sl_id: Optional[str] = None
    sl_price: Optional[float] = None
    if len(sl_cands) == 1:
        oid = sl_cands[0].get("orderId")
        if oid is None:
            raise RuntimeError("SL candidate missing orderId (STRICT)")
        sl_id = str(oid)
        sl_price = _order_price(sl_cands[0])

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

    db_trade.remaining_qty = float(abs_qty)

    ps = _normalize_position_side_raw(position_side_raw, default_both=True)
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

    if db_trade.tp_order_id is None or not str(db_trade.tp_order_id).strip():
        raise RuntimeError("bt_trades.tp_order_id is required after reconciliation (STRICT)")
    if db_trade.sl_order_id is None or not str(db_trade.sl_order_id).strip():
        raise RuntimeError("bt_trades.sl_order_id is required after reconciliation (STRICT)")

    if db_trade.remaining_qty is None:
        raise RuntimeError("bt_trades.remaining_qty is required after reconciliation (STRICT)")
    remaining_qty = _safe_float_strict(db_trade.remaining_qty, "bt_trades.remaining_qty")
    if remaining_qty <= 0:
        raise RuntimeError("bt_trades.remaining_qty must be > 0 (STRICT)")

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
        qty=float(abs_qty),
        entry_price=float(entry_price),
        leverage=int(leverage_i),
        entry=float(entry_price),
        entry_order_id=str(db_trade.entry_order_id) if db_trade.entry_order_id is not None else None,
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
    - DB OPEN 트레이드가 없는데 거래소 포지션이 있으면 즉시 예외
    - DB OPEN 트레이드가 있는데 거래소 포지션이 없으면 즉시 예외
    - 결과로 반환되는 Trade 리스트는 DB+거래소 정합을 통과한 상태만 포함

    Returns:
      (rebuilt_trades, exchange_pos_count)
    """
    sym = _normalize_symbol(symbol)
    if current_trades is None:
        current_trades = []

    pos_dir, abs_qty, entry_price, position_side_raw = _fetch_one_way_position_strict(sym)

    orders = fetch_open_orders(sym)
    if not isinstance(orders, list):
        raise RuntimeError("fetch_open_orders returned non-list (STRICT)")
    open_orders = [o for o in orders if isinstance(o, dict)]
    if len(open_orders) != len(orders):
        raise RuntimeError("fetch_open_orders contains non-dict row (STRICT)")

    session = SessionLocal()
    try:
        db_open = _load_db_open_trade_strict(session, sym)

        if abs_qty <= 0:
            if db_open is not None:
                _set_db_mismatch_and_commit(session, db_open, status="MISMATCH_NO_POSITION")
                raise RuntimeError(f"DB has OPEN trade but exchange has NO position (STRICT, symbol={sym})")

            return ([], 0) if replace else (current_trades, 0)

        if db_open is None:
            raise RuntimeError(f"exchange has OPEN position but DB has NO OPEN trade (STRICT, symbol={sym})")

        tp_id, tp_price, sl_id, sl_price = _pick_unique_tp_sl_orders_strict(
            sym,
            pos_dir=pos_dir,
            position_side_raw=position_side_raw,
            open_orders=open_orders,
        )

        if tp_id is None or sl_id is None:
            _set_db_mismatch_and_commit(session, db_open, status="MISMATCH_PROTECTION_ORDERS")
            raise RuntimeError(
                f"exchange protection orders missing (STRICT, symbol={sym}, tp_id={tp_id}, sl_id={sl_id})"
            )

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
        session.commit()

        rebuilt = _build_mem_trade_strict(
            db_trade=db_open,
            pos_dir=pos_dir,
            abs_qty=abs_qty,
            entry_price=entry_price,
            tp_price=tp_price,
            sl_price=sl_price,
        )

        out = [rebuilt] if replace else (list(current_trades) + [rebuilt])

        _notify_sync_ok(sym, pos_dir=pos_dir, abs_qty=abs_qty, tp_id=tp_id, sl_id=sl_id)
        return out, 1

    finally:
        session.close()


__all__ = [
    "sync_open_trades_from_exchange",
]