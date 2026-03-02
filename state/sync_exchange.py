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
- TP/SL 보호 주문은 reduceOnly=True + (STOP_MARKET / TAKE_PROFIT_MARKET)로 운영한다.

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
========================================================
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from infra.telelog import log, send_tg
from execution.exchange_api import fetch_open_orders, fetch_open_positions
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
        raise RuntimeError(f"{name} is required")
    return s


def _safe_float_strict(v: Any, name: str) -> float:
    if v is None:
        raise RuntimeError(f"{name} is required")
    if isinstance(v, bool):
        raise RuntimeError(f"{name} must be numeric (bool not allowed)")
    try:
        f = float(v)
    except Exception as e:
        raise RuntimeError(f"{name} must be numeric: {e}") from e
    if not math.isfinite(f):
        raise RuntimeError(f"{name} must be finite")
    return f


def _normalize_symbol(symbol: str) -> str:
    s = str(symbol).replace("-", "").replace("/", "").upper().strip()
    if not s:
        raise RuntimeError("symbol is empty")
    return s


def _position_dir_one_way(position_amt: float) -> str:
    # One-way: sign of positionAmt determines direction
    if position_amt > 0:
        return "LONG"
    if position_amt < 0:
        return "SHORT"
    raise RuntimeError("positionAmt is zero")


def _entry_side_from_dir(pos_dir: str) -> str:
    return "BUY" if pos_dir == "LONG" else "SELL"


def _exit_side_from_dir(pos_dir: str) -> str:
    return "SELL" if pos_dir == "LONG" else "BUY"


def _is_tp_order(o: Dict[str, Any]) -> bool:
    t = str(o.get("type", "")).upper().strip()
    return t in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT")


def _is_sl_order(o: Dict[str, Any]) -> bool:
    t = str(o.get("type", "")).upper().strip()
    return t in ("STOP_MARKET", "STOP")


def _order_price(o: Dict[str, Any]) -> Optional[float]:
    sp = o.get("stopPrice")
    if sp not in (None, "", "0", "0.0", 0):
        try:
            v = float(sp)
            return v if v > 0 else None
        except Exception:
            return None
    pr = o.get("price")
    if pr not in (None, "", "0", "0.0", 0):
        try:
            v = float(pr)
            return v if v > 0 else None
        except Exception:
            return None
    return None


# ─────────────────────────────────────────────────────────────
# Exchange snapshot (STRICT)
# ─────────────────────────────────────────────────────────────
def _fetch_one_way_position_strict(symbol: str) -> Tuple[str, float, float, str]:
    """
    Returns:
      (pos_dir, abs_qty, entry_price, position_side_raw)

    STRICT:
    - Hedge 모드(동일 심볼에서 LONG/SHORT 동시) 감지 시 즉시 예외
    - open position이 없으면 ("", 0, 0, "") 반환 (상위에서 정합 처리)
    """
    sym = _normalize_symbol(symbol)
    rows = fetch_open_positions(sym)
    if not isinstance(rows, list):
        raise RuntimeError("fetch_open_positions returned non-list")

    # 같은 심볼의 positionAmt != 0 row만
    live: List[Dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        if _normalize_symbol(r.get("symbol", "")) != sym:
            continue
        amt = _safe_float_strict(r.get("positionAmt"), "positionRisk.positionAmt")
        if abs(amt) > 1e-12:
            live.append(r)

    if not live:
        return "", 0.0, 0.0, ""

    # Hedge 감지: positionSide LONG/SHORT 2개가 동시에 있거나, 서로 다른 방향 row가 동시에 존재
    dirs: set[str] = set()
    for r in live:
        ps = str(r.get("positionSide", "")).upper().strip()
        amt = _safe_float_strict(r.get("positionAmt"), "positionRisk.positionAmt")
        if ps in ("LONG", "SHORT"):
            dirs.add(ps)
        else:
            dirs.add(_position_dir_one_way(amt))

    if len(dirs) != 1:
        raise RuntimeError(f"HEDGE mode detected or ambiguous positions: dirs={sorted(list(dirs))}")

    r0 = live[0]
    amt0 = _safe_float_strict(r0.get("positionAmt"), "positionRisk.positionAmt")
    pos_dir = list(dirs)[0] if list(dirs)[0] in ("LONG", "SHORT") else _position_dir_one_way(amt0)

    entry_price = _safe_float_strict(r0.get("entryPrice"), "positionRisk.entryPrice")
    if entry_price <= 0:
        raise RuntimeError("positionRisk.entryPrice must be > 0 (STRICT)")

    pos_side_raw = str(r0.get("positionSide", "")).upper().strip() or "BOTH"
    return pos_dir, abs(float(amt0)), float(entry_price), pos_side_raw


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
    - reduceOnly=True 우선이 아니라, "유일성"을 강제한다.
    """
    sym = _normalize_symbol(symbol)
    exit_side = _exit_side_from_dir(pos_dir)
    ps_raw = str(position_side_raw or "").upper().strip() or "BOTH"

    tp_cands: List[Dict[str, Any]] = []
    sl_cands: List[Dict[str, Any]] = []

    for o in open_orders:
        if not isinstance(o, dict):
            continue
        if _normalize_symbol(o.get("symbol", "")) != sym:
            continue

        o_side = str(o.get("side", "")).upper().strip()
        if o_side != exit_side:
            continue

        # Hedge 포지션이면 주문 positionSide도 일치해야 함
        o_ps = str(o.get("positionSide", "")).upper().strip() or "BOTH"
        if ps_raw in ("LONG", "SHORT") and o_ps in ("LONG", "SHORT") and o_ps != ps_raw:
            continue

        if not bool(o.get("reduceOnly", False)):
            # 운영 정책상 reduceOnly 없는 TP/SL은 허용하지 않는다(오동작 리스크).
            continue

        if _is_tp_order(o):
            tp_cands.append(o)
        elif _is_sl_order(o):
            sl_cands.append(o)

    if len(tp_cands) > 1:
        raise RuntimeError(f"multiple TP candidates found (ambiguous): n={len(tp_cands)}")
    if len(sl_cands) > 1:
        raise RuntimeError(f"multiple SL candidates found (ambiguous): n={len(sl_cands)}")

    tp_id = None
    tp_price = None
    if len(tp_cands) == 1:
        oid = tp_cands[0].get("orderId")
        if oid is None:
            raise RuntimeError("TP candidate missing orderId")
        tp_id = str(oid)
        tp_price = _order_price(tp_cands[0])

    sl_id = None
    sl_price = None
    if len(sl_cands) == 1:
        oid = sl_cands[0].get("orderId")
        if oid is None:
            raise RuntimeError("SL candidate missing orderId")
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
        raise RuntimeError(f"STRICT: multiple OPEN trades in DB for symbol={sym} (n={len(rows)})")
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
    - DB에 값이 없고 거래소에 값이 있으면 거래소 사실로 채운다(추정 아님)
    """
    if abs_qty <= 0:
        raise RuntimeError("abs_qty must be > 0")
    if entry_price <= 0:
        raise RuntimeError("entry_price must be > 0")

    # remaining_qty
    db_trade.remaining_qty = float(abs_qty)

    # exchange_position_side
    ps = str(position_side_raw or "").upper().strip() or "BOTH"
    if ps not in ("BOTH", "LONG", "SHORT"):
        raise RuntimeError(f"invalid exchange positionSide: {ps!r}")
    db_trade.exchange_position_side = ps

    # tp/sl order ids: 채우거나, 불일치면 실패
    if db_trade.tp_order_id is not None and tp_id is not None:
        if str(db_trade.tp_order_id) != str(tp_id):
            raise RuntimeError(f"TP orderId mismatch: db={db_trade.tp_order_id} exch={tp_id}")
    if db_trade.sl_order_id is not None and sl_id is not None:
        if str(db_trade.sl_order_id) != str(sl_id):
            raise RuntimeError(f"SL orderId mismatch: db={db_trade.sl_order_id} exch={sl_id}")

    if db_trade.tp_order_id is None and tp_id is not None:
        db_trade.tp_order_id = str(tp_id)
    if db_trade.sl_order_id is None and sl_id is not None:
        db_trade.sl_order_id = str(sl_id)

    # 보호 주문 정책(운영): SL은 반드시 존재해야 한다.
    if db_trade.sl_order_id is None:
        raise RuntimeError("STRICT: SL orderId is missing (protection order required)")

    # realized_pnl_usdt 초기값 보정: NULL이면 0으로 확정(추정 아님: 초기 정의값)
    if db_trade.realized_pnl_usdt is None:
        db_trade.realized_pnl_usdt = 0.0

    db_trade.reconciliation_status = "OK"
    db_trade.last_synced_at = _utc_now()
    session.add(db_trade)


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

    # 1) 거래소 상태 (STRICT)
    pos_dir, abs_qty, entry_price, position_side_raw = _fetch_one_way_position_strict(sym)
    orders = fetch_open_orders(sym)
    if not isinstance(orders, list):
        raise RuntimeError("fetch_open_orders returned non-list")
    orders = [o for o in orders if isinstance(o, dict)]

    # 2) DB 정합 (STRICT)
    session = SessionLocal()
    try:
        db_open = _load_db_open_trade_strict(session, sym)

        if abs_qty <= 0:
            # 거래소 포지션 없음
            if db_open is not None:
                db_open.reconciliation_status = "MISMATCH_NO_POSITION"
                db_open.last_synced_at = _utc_now()
                session.add(db_open)
                session.commit()
                raise RuntimeError(f"STRICT: DB has OPEN trade but exchange has NO position (symbol={sym})")

            # 둘 다 없음 → 정상
            return ([], 0) if replace else (current_trades, 0)

        # 거래소 포지션 있음
        if db_open is None:
            raise RuntimeError(f"STRICT: exchange has OPEN position but DB has NO OPEN trade (symbol={sym})")

        tp_id, tp_price, sl_id, sl_price = _pick_unique_tp_sl_orders_strict(
            sym, pos_dir=pos_dir, position_side_raw=position_side_raw, open_orders=orders
        )

        # DB 실행 필드 업데이트/검증(STRICT)
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

        # 3) 메모리 Trade 재구성 (DB 필드 우선)
        side_open = _entry_side_from_dir(pos_dir)
        rebuilt = MemTrade(
            symbol=sym,
            side=side_open,
            qty=float(abs_qty),
            entry_price=float(entry_price),
            leverage=int(db_open.leverage) if db_open.leverage is not None else int(getattr(db_open, "leverage", 1) or 1),
            entry=float(entry_price),
            entry_order_id=str(db_open.entry_order_id) if db_open.entry_order_id is not None else None,
            tp_order_id=str(db_open.tp_order_id) if db_open.tp_order_id is not None else None,
            sl_order_id=str(db_open.sl_order_id) if db_open.sl_order_id is not None else None,
            tp_price=float(tp_price) if tp_price is not None else 0.0,
            sl_price=float(sl_price) if sl_price is not None else 0.0,
            source="SYNC",
            id=int(db_open.id) if int(db_open.id) > 0 else 0,
            exchange_position_side=str(db_open.exchange_position_side or "BOTH"),
            remaining_qty=float(db_open.remaining_qty) if db_open.remaining_qty is not None else float(abs_qty),
            realized_pnl_usdt=float(db_open.realized_pnl_usdt) if db_open.realized_pnl_usdt is not None else 0.0,
            reconciliation_status=str(db_open.reconciliation_status or "OK"),
            last_synced_at=db_open.last_synced_at,
        )

        # replace 정책 적용
        if replace:
            out = [rebuilt]
        else:
            out = list(current_trades) + [rebuilt]

        # 4) 알림(변화가 있을 때만)
        try:
            send_tg(f"🔁 [SYNC_OK] {sym} pos={pos_dir} qty={abs_qty:.6f} tp={tp_id} sl={sl_id}")
        except Exception:
            pass

        return out, 1

    finally:
        try:
            session.close()
        except Exception:
            pass


__all__ = [
    "sync_open_trades_from_exchange",
]