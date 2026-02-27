# sync_exchange.py
"""
sync_exchange.py (Binance USDT-M Futures 전용, 거래소 상태 동기화 + DB 스냅샷)
=====================================================

역할
-----------------------------------------------------
거래소(Binance USDT-M Futures)에 실제로 열려 있는 포지션/미체결 주문/잔고 상태를 읽어
내부 Trade 리스트와 동기화하고, 동시에 DB 스냅샷 테이블에 기록한다.

변경 내용
-----------------------------------------------------
1) BingX 관련 필드/파싱 로직 전면 삭제
2) Binance USDT-M Futures REST 기준으로 재작성
3) 포지션 조회: fetch_open_positions() (positionRisk 기반)
4) 미체결 주문 조회: fetch_open_orders() (openOrders 기반)
5) 잔고 조회: get_balance_detail() (USDT 행 기준)
6) positionAmt, entryPrice, unrealizedProfit, leverage, positionSide 사용
7) Hedge / One-way 모드 모두 대응 (positionSide + positionAmt 부호)
8) REST 실패 시 기존 Trade 리스트 유지 (절대 추정 금지)
9) DB 스냅샷은 delete+insert 방식 유지
10) Binance 전용 심볼 형식(BTCUSDT) 기준으로 동기화

주의
-----------------------------------------------------
- BingX 전용 필드/JSON 구조 사용 금지
- quantity / size / avgPrice / triggerPrice 등 BingX 의존 파싱 제거
- TP/SL 식별은 Binance order type 기준으로만 판단
- 이 파일은 주문 실행 로직을 포함하지 않음 (order_executor.py 분리)
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from infra.telelog import log, send_tg
from execution.exchange_api import fetch_open_positions, fetch_open_orders, get_balance_detail
from state.trader_state import Trade

# exchange_api의 내부 helper 사용 시도 (실패 시 로컬 fallback)
try:
    from execution.exchange_api import _normalize_symbol as _exchange_normalize_symbol  # type: ignore
except Exception:
    _exchange_normalize_symbol = None  # type: ignore


# DB 세션/ORM (없으면 DB 동기화만 SKIP)
try:  # pragma: no cover
    from state.db_core import SessionLocal  # type: ignore
    from state.db_models import (  # type: ignore
        ExchangePosition as ExchangePositionORM,
        ExchangeOrder as ExchangeOrderORM,
        BalanceSnapshot as BalanceSnapshotORM,
    )
except Exception:
    SessionLocal = None  # type: ignore
    ExchangePositionORM = None  # type: ignore
    ExchangeOrderORM = None  # type: ignore
    BalanceSnapshotORM = None  # type: ignore


# 마지막 동기화된 "거래소 포지션 개수" (텔레그램 중복 알림 방지)
_LAST_SYNCED_EXCHANGE_POS_COUNT: int = 0


# ─────────────────────────────────────────────────────────────
# 내부 유틸
# ─────────────────────────────────────────────────────────────
def _normalize_symbol(symbol: str) -> str:
    if _exchange_normalize_symbol is not None:
        return _exchange_normalize_symbol(symbol)
    return str(symbol).replace("-", "").replace("/", "").upper().strip()


def _safe_float(v: Any, field_name: str) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        raise ValueError(f"{field_name} float 변환 실패: {v!r}")


def _position_direction(position_side: str, position_amt: float) -> str:
    """
    Binance positionRisk 기준 포지션 방향 판정.
    리턴: "LONG" | "SHORT"
    """
    ps = (position_side or "").upper()

    if ps == "LONG":
        return "LONG"
    if ps == "SHORT":
        return "SHORT"

    # One-way (BOTH) 또는 비정상 값은 positionAmt 부호로 판정
    if position_amt > 0:
        return "LONG"
    if position_amt < 0:
        return "SHORT"

    raise ValueError("positionAmt == 0 이거나 방향 판정 불가")


def _trade_side_from_position_dir(pos_dir: str) -> str:
    return "BUY" if pos_dir == "LONG" else "SELL"


def _exit_order_side_for_position_dir(pos_dir: str) -> str:
    # LONG 청산은 SELL, SHORT 청산은 BUY
    return "SELL" if pos_dir == "LONG" else "BUY"


def _is_tp_order_type(order_type: str) -> bool:
    t = (order_type or "").upper()
    return t in ("TAKE_PROFIT", "TAKE_PROFIT_MARKET")


def _is_sl_order_type(order_type: str) -> bool:
    t = (order_type or "").upper()
    return t in ("STOP", "STOP_MARKET")


def _binance_order_price(o: Dict[str, Any]) -> Optional[float]:
    """
    Binance TP/SL 가격 선택:
    - stopPrice 우선
    - 없으면 price
    """
    stop_price = o.get("stopPrice")
    if stop_price not in (None, "", 0, "0", "0.0"):
        try:
            return float(stop_price)
        except (TypeError, ValueError):
            return None

    price = o.get("price")
    if price not in (None, "", 0, "0", "0.0"):
        try:
            return float(price)
        except (TypeError, ValueError):
            return None

    return None


def _extract_usdt_balance_row(bal_raw: Any) -> Dict[str, Any]:
    """
    get_balance_detail() 결과에서 USDT row 추출.
    허용 입력:
    - dict (USDT row)
    - list[dict] (asset == USDT 탐색)
    - dict {"raw": ...} (exchange_api wrapper)
    """
    # 1) dict(USDT row) 형태 우선
    if isinstance(bal_raw, dict):
        asset = str(bal_raw.get("asset", "")).upper()
        if asset == "USDT":
            return bal_raw

        # exchange_api가 {"raw": ...} 형태로 감싼 경우 대응
        if "raw" in bal_raw:
            return _extract_usdt_balance_row(bal_raw.get("raw"))

    # 2) list 형태에서 asset == USDT 탐색
    if isinstance(bal_raw, list):
        for row in bal_raw:
            if not isinstance(row, dict):
                continue
            if str(row.get("asset", "")).upper() == "USDT":
                return row

    raise ValueError("USDT 잔고 row를 찾지 못함")


def _order_matches_position_for_exit(
    order: Dict[str, Any],
    symbol_norm: str,
    pos_dir: str,
) -> bool:
    """
    포지션에 대응 가능한 '청산성 주문'인지 판단.
    - 동일 심볼
    - TP/SL 타입
    - 반대 side
    - reduceOnly=True 우선 (강제는 아니지만 후보 선별에서 우선순위 반영)
    - hedge 모드면 positionSide 일치 확인 (LONG/SHORT)
    """
    if not isinstance(order, dict):
        return False

    if _normalize_symbol(order.get("symbol", "")) != symbol_norm:
        return False

    o_type = str(order.get("type", "")).upper()
    if not (_is_tp_order_type(o_type) or _is_sl_order_type(o_type)):
        return False

    o_side = str(order.get("side", "")).upper()
    expected_exit_side = _exit_order_side_for_position_dir(pos_dir)
    if o_side != expected_exit_side:
        return False

    # Hedge 모드 주문이면 positionSide 일치 필요
    o_pos_side = str(order.get("positionSide", "")).upper()
    if o_pos_side in ("LONG", "SHORT") and o_pos_side != pos_dir:
        return False

    return True


def _pick_tp_sl_for_position(
    symbol_norm: str,
    pos_dir: str,
    orders: List[Dict[str, Any]],
) -> Tuple[Optional[str], Optional[float], Optional[str], Optional[float]]:
    """
    특정 포지션 방향(LONG/SHORT)에 대응하는 TP/SL 주문 1개씩 선택.
    우선순위:
    1) reduceOnly=True
    2) 그 외 매칭 주문
    """
    candidates: List[Dict[str, Any]] = []
    for o in orders:
        if _order_matches_position_for_exit(o, symbol_norm, pos_dir):
            candidates.append(o)

    if not candidates:
        return None, None, None, None

    # reduceOnly 우선 정렬 (True 먼저)
    def _prio(order: Dict[str, Any]) -> int:
        return 0 if bool(order.get("reduceOnly", False)) else 1

    candidates.sort(key=_prio)

    tp_id: Optional[str] = None
    tp_price: Optional[float] = None
    sl_id: Optional[str] = None
    sl_price: Optional[float] = None

    for o in candidates:
        o_type = str(o.get("type", "")).upper()
        oid = o.get("orderId")
        price = _binance_order_price(o)

        if tp_id is None and _is_tp_order_type(o_type):
            tp_id = str(oid) if oid is not None else None
            tp_price = price
            continue

        if sl_id is None and _is_sl_order_type(o_type):
            sl_id = str(oid) if oid is not None else None
            sl_price = price
            continue

        if tp_id is not None and sl_id is not None:
            break

    return tp_id, tp_price, sl_id, sl_price


# ─────────────────────────────────────────────────────────────
# DB 스냅샷 동기화
# ─────────────────────────────────────────────────────────────
def _sync_exchange_state_to_db(
    symbol: str,
    positions: List[Dict[str, Any]],
    orders: List[Dict[str, Any]],
) -> None:
    """
    Binance REST로 받은 잔고/포지션/주문 상태를 DB에 스냅샷으로 기록한다.

    STRICT / NO-FALLBACK:
    - SessionLocal/ORM 미준비면 SKIP (로그만)
    - 잔고 파싱/포지션 저장/주문 저장 중 하나라도 실패 시 전체 rollback
    """
    if (
        SessionLocal is None
        or BalanceSnapshotORM is None
        or ExchangePositionORM is None
        or ExchangeOrderORM is None
    ):
        log("[SYNC_BINANCE][DB] SessionLocal/ORM 없음 → DB 동기화 SKIP")
        return

    symbol_norm = _normalize_symbol(symbol)

    try:
        session = SessionLocal()
    except Exception as e:  # pragma: no cover
        log(f"[SYNC_BINANCE][DB] Session 생성 실패: {e}")
        return

    try:
        # 1) 잔고 스냅샷
        bal_raw = get_balance_detail()
        usdt_row = _extract_usdt_balance_row(bal_raw)

        asset = str(usdt_row.get("asset", "USDT")).upper() or "USDT"
        balance = _safe_float(usdt_row.get("balance", 0.0), "balance")
        available_balance = _safe_float(usdt_row.get("availableBalance", 0.0), "availableBalance")
        cross_wallet_balance = _safe_float(usdt_row.get("crossWalletBalance", 0.0), "crossWalletBalance")
        unrealized_profit = _safe_float(usdt_row.get("unrealizedProfit", 0.0), "unrealizedProfit")

        equity = cross_wallet_balance + unrealized_profit
        available_margin = available_balance
        used_margin = max(equity - available_margin, 0.0)

        bs = BalanceSnapshotORM(  # type: ignore[call-arg]
            asset=asset,
            balance=balance,
            equity=equity,
            available_margin=available_margin,
            used_margin=used_margin,
            raw_json=bal_raw,
        )
        session.add(bs)

        # 2) 포지션 스냅샷 (해당 심볼 전부 교체)
        session.query(ExchangePositionORM).filter(  # type: ignore[attr-defined]
            ExchangePositionORM.symbol == symbol_norm  # type: ignore[attr-defined]
        ).delete(synchronize_session=False)

        for pos in positions:
            if not isinstance(pos, dict):
                raise ValueError(f"포지션 row 형식 오류: {pos!r}")

            raw_amt = _safe_float(pos.get("positionAmt", 0.0), "positionAmt")
            if raw_amt == 0.0:
                continue

            pos_dir = _position_direction(str(pos.get("positionSide", "")), raw_amt)
            qty = abs(raw_amt)
            entry_price = _safe_float(pos.get("entryPrice", 0.0), "entryPrice")
            leverage = _safe_float(pos.get("leverage", 0.0), "leverage")
            unrealized_pnl = _safe_float(pos.get("unrealizedProfit", 0.0), "unrealizedProfit")

            ep = ExchangePositionORM(  # type: ignore[call-arg]
                symbol=symbol_norm,
                side=pos_dir,  # LONG / SHORT
                qty=qty,
                entry_price=entry_price,
                leverage=leverage,
                unrealized_pnl=unrealized_pnl,
                raw_json=pos,
            )
            session.add(ep)

        # 3) 미체결 주문 스냅샷 (해당 심볼 전부 교체)
        session.query(ExchangeOrderORM).filter(  # type: ignore[attr-defined]
            ExchangeOrderORM.symbol == symbol_norm  # type: ignore[attr-defined]
        ).delete(synchronize_session=False)

        for o in orders:
            if not isinstance(o, dict):
                raise ValueError(f"주문 row 형식 오류: {o!r}")

            oid = o.get("orderId")
            o_type = str(o.get("type", "")).upper()
            side = str(o.get("side", "")).upper()

            # 주문 스냅샷의 대표 price는 stopPrice 우선, 없으면 price
            price_val = _binance_order_price(o)
            price = float(price_val) if price_val is not None else 0.0
            qty = _safe_float(o.get("origQty", 0.0), "origQty")

            eo = ExchangeOrderORM(  # type: ignore[call-arg]
                symbol=symbol_norm,
                order_id=str(oid) if oid is not None else None,
                side=side,
                order_type=o_type,
                price=price,
                qty=qty,
                raw_json=o,
            )
            session.add(eo)

        session.commit()
        log(
            f"[SYNC_BINANCE][DB] symbol={symbol_norm} balance/positions/orders 동기화 완료 "
            f"(pos={len(positions)}, open_orders={len(orders)})"
        )

    except Exception as e:  # pragma: no cover
        try:
            session.rollback()
        except Exception:
            pass
        log(f"[SYNC_BINANCE][DB] 동기화 실패 → 롤백: {e}")

    finally:
        try:
            session.close()
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────
# Trade 리스트 동기화
# ─────────────────────────────────────────────────────────────
def sync_open_trades_from_exchange(
    symbol: str,
    replace: bool = True,
    current_trades: Optional[List[Trade]] = None,
) -> Tuple[List[Trade], int]:
    """
    Binance 거래소 포지션/주문을 읽어 Trade 리스트를 동기화한다.

    규칙:
    - REST 실패 시 기존 Trade 리스트 유지 (NO-FALLBACK)
    - DB 동기화 실패는 rollback/log 후 계속 진행
    - positions 비어 있으면 replace=True일 때만 current_trades 비움
    """
    global _LAST_SYNCED_EXCHANGE_POS_COUNT

    symbol_norm = _normalize_symbol(symbol)

    if current_trades is None:
        current_trades = []

    # 1) 포지션 조회 (실패 시 기존 리스트 유지)
    try:
        positions = fetch_open_positions(symbol_norm)
    except Exception as e:
        log(f"[SYNC_BINANCE] fetch_open_positions 실패 → 기존 Trade 유지, DB 동기화 SKIP: {e}")
        return current_trades, _LAST_SYNCED_EXCHANGE_POS_COUNT

    # 2) 주문 조회 (실패 시 기존 리스트 유지)
    try:
        orders = fetch_open_orders(symbol_norm)
    except Exception as e:
        log(f"[SYNC_BINANCE] fetch_open_orders 실패 → 기존 Trade 유지, DB 동기화 SKIP: {e}")
        return current_trades, _LAST_SYNCED_EXCHANGE_POS_COUNT

    # 3) DB 스냅샷 동기화 (실패해도 Trade 재구성은 계속)
    try:
        _sync_exchange_state_to_db(symbol_norm, positions or [], orders or [])
    except Exception as e:  # pragma: no cover
        log(f"[SYNC_BINANCE] DB 스냅샷 동기화 호출 예외(계속 진행): {e}")

    # 4) 실제 포지션이 없으면 메모리 Trade 비움 (replace=True일 때만)
    if not positions:
        log(f"[SYNC_BINANCE] symbol={symbol_norm} 열려 있는 포지션 없음")
        if replace:
            current_trades = []
            _LAST_SYNCED_EXCHANGE_POS_COUNT = 0
        return current_trades, _LAST_SYNCED_EXCHANGE_POS_COUNT

    # 5) 포지션 기반으로 Trade 재구성
    if replace:
        current_trades = []

    rebuilt_trades: List[Trade] = []

    for pos in positions:
        if not isinstance(pos, dict):
            log(f"[SYNC_BINANCE] 포지션 row 형식 오류 → 스킵: {pos!r}")
            continue

        try:
            position_amt = _safe_float(pos.get("positionAmt", 0.0), "positionAmt")
        except Exception as e:
            log(f"[SYNC_BINANCE] positionAmt 파싱 실패 → 스킵: {e} raw={pos}")
            continue

        if position_amt == 0.0:
            continue

        try:
            pos_dir = _position_direction(str(pos.get("positionSide", "")), position_amt)
        except Exception as e:
            log(f"[SYNC_BINANCE] 포지션 방향 판정 실패 → 스킵: {e} raw={pos}")
            continue

        qty = abs(position_amt)

        try:
            entry_price = _safe_float(pos.get("entryPrice", 0.0), "entryPrice")
        except Exception:
            entry_price = 0.0

        try:
            leverage = _safe_float(pos.get("leverage", 0.0), "leverage")
        except Exception:
            leverage = 0.0

        tp_id, tp_price, sl_id, sl_price = _pick_tp_sl_for_position(
            symbol_norm=symbol_norm,
            pos_dir=pos_dir,
            orders=orders or [],
        )

        side_open = _trade_side_from_position_dir(pos_dir)

        rebuilt_trades.append(
            Trade(
                symbol=symbol_norm,
                side=side_open,
                qty=qty,
                entry_price=entry_price,
                leverage=leverage,
                entry=entry_price,
                entry_order_id=None,
                tp_order_id=tp_id,
                sl_order_id=sl_id,
                tp_price=tp_price,
                sl_price=sl_price,
                source="SYNC",
            )
        )

    if replace:
        current_trades = rebuilt_trades
    else:
        current_trades.extend(rebuilt_trades)

    # 6) 거래소 포지션 개수 변화 알림 (중복 알림 방지)
    exchange_pos_count = len(rebuilt_trades)
    if exchange_pos_count != _LAST_SYNCED_EXCHANGE_POS_COUNT:
        try:
            send_tg(
                f"🔁 Binance 거래소 포지션 {exchange_pos_count}건 동기화했습니다. "
                f"(중복 진입 방지)"
            )
        except Exception as e:
            log(f"[SYNC_BINANCE] 텔레그램 알림 실패: {e}")
        _LAST_SYNCED_EXCHANGE_POS_COUNT = exchange_pos_count

    return current_trades, _LAST_SYNCED_EXCHANGE_POS_COUNT