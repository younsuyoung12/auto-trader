"""
sync_exchange.py (거래소 상태 동기화 + DB 스냅샷 버전)
=====================================================
역할
----------------------------------------------------
거래소에 실제로 열려 있는 포지션/주문을 읽어서
내부 Trade 리스트와 맞춘다.

추가 기능 (7단계)
----------------------------------------------------
1) BingX REST 로부터 가져온 잔고·포지션·미체결 주문 상태를
   DB 스냅샷 테이블에 그대로 기록한다.
   - 잔고: get_balance_detail() 결과를 BalanceSnapshot ORM 으로 저장
   - 포지션: fetch_open_positions() 결과를 ExchangePosition ORM 으로 저장
   - 미체결 주문: fetch_open_orders() 결과를 ExchangeOrder ORM 으로 저장
2) "데이터가 온전히 준비되지 않은 경우"에는 절대 폴백으로 추정값을 만들지 않는다.
   - REST 호출 실패, 응답 파싱 실패, DB 세션/ORM 문제 등이 발생하면
     → 모든 DB 쓰기를 롤백하고 [SYNC_DB] 로그만 남긴 뒤, DB 동기화는 건너뛴다.
   - 기존 OPEN_TRADES 리스트는 그대로 유지되므로, 전략 로직이 잘못된
     상태를 기반으로 추가 진입하지 않도록 안전하게 막는다.
3) run_bot.py 에서는 이전과 동일하게
   OPEN_TRADES, _ = sync_open_trades_from_exchange(symbol, True, OPEN_TRADES)
   형태로 호출할 수 있다.

주의사항
----------------------------------------------------
- 아래 DB ORM 클래스 이름(ExchangePosition, ExchangeOrder, BalanceSnapshot)은
  db_models 쪽 정의에 맞게 필요 시 수정해야 한다.
- DB 쪽 문제로 동기화가 실패해도, Trade 리스트 재구성 자체는 계속 수행한다.
  (포지션 동기화 실패 시에는 기존 리스트를 그대로 반환하여 중복 진입을 방지)

사용 예
----------------------------------------------------
OPEN_TRADES, _ = sync_open_trades_from_exchange(symbol, True, OPEN_TRADES)
"""

from __future__ import annotations

from typing import List, Optional, Tuple, Dict, Any

from telelog import log, send_tg
from exchange_api import fetch_open_positions, fetch_open_orders, get_balance_detail
from trader import Trade

# DB 세션/ORM (있으면 사용, 없으면 동기화만 SKIP 하고 로그 남김)
try:  # pragma: no cover - DB 준비 전 환경 방어
    from db_core import SessionLocal  # type: ignore
    from db_models import (
        ExchangePosition as ExchangePositionORM,  # type: ignore
        ExchangeOrder as ExchangeOrderORM,        # type: ignore
        BalanceSnapshot as BalanceSnapshotORM,    # type: ignore
    )
except Exception:
    SessionLocal = None            # type: ignore
    ExchangePositionORM = None     # type: ignore
    ExchangeOrderORM = None        # type: ignore
    BalanceSnapshotORM = None      # type: ignore


# 마지막으로 동기화했을 때의 포지션 개수 (텔레그램 중복 알림 방지용)
_LAST_SYNCED_EXCHANGE_POS_COUNT: int = 0


def _sync_exchange_state_to_db(
    symbol: str,
    positions: List[Dict[str, Any]],
    orders: List[Dict[str, Any]],
) -> None:
    """REST 로 받은 잔고/포지션/주문 상태를 DB 에 스냅샷으로 기록한다.

    ⚠ 폴백/추정 없음.
    - SessionLocal 또는 ORM 이 준비되어 있지 않으면 동기화 자체를 건너뛰고 로그만 남긴다.
    - get_balance_detail / 응답 파싱 / INSERT 중 하나라도 실패하면 전체 롤백.
    """
    if (
        SessionLocal is None
        or BalanceSnapshotORM is None
        or ExchangePositionORM is None
        or ExchangeOrderORM is None
    ):
        log("[SYNC_DB] SessionLocal/ORM 없음 → DB 동기화 SKIP (폴백 없음)")
        return

    try:
        session = SessionLocal()
    except Exception as e:  # pragma: no cover - DB 초기화 실패 방어
        log(f"[SYNC_DB] Session 생성 실패: {e}")
        return

    try:
        # 1) 잔고 스냅샷
        try:
            bal_raw = get_balance_detail()
        except Exception as e:
            log(f"[SYNC_DB] get_balance_detail 실패: {e}")
            raise

        try:
            data = (bal_raw or {}).get("data", {})
            bal = data.get("balance", {})
            asset = bal.get("asset", "USDT")
            balance = float(bal.get("balance", 0.0))
            equity = float(bal.get("equity", 0.0))
            available_margin = float(bal.get("availableMargin", 0.0))
            used_margin = float(bal.get("usedMargin", 0.0))
        except Exception as e:
            log(f"[SYNC_DB] 잔고 응답 파싱 실패: {e}")
            raise

        bs = BalanceSnapshotORM(  # type: ignore[call-arg]
            asset=asset,
            balance=balance,
            equity=equity,
            available_margin=available_margin,
            used_margin=used_margin,
            raw_json=bal_raw,
        )
        session.add(bs)

        # 2) 포지션: 해당 심볼 기준으로 전부 교체(upsert 대신 delete+insert)
        try:
            session.query(ExchangePositionORM).filter(  # type: ignore[attr-defined]
                ExchangePositionORM.symbol == symbol  # type: ignore[attr-defined]
            ).delete(synchronize_session=False)
        except Exception as e:
            log(f"[SYNC_DB] 기존 포지션 삭제 실패: {e}")
            raise

        for pos in positions:
            try:
                raw_amt = pos.get("positionAmt") or pos.get("quantity") or pos.get("size") or 0.0
                qty = abs(float(raw_amt))
                if qty == 0.0:
                    continue

                pos_side_raw = (pos.get("positionSide") or "").upper()
                if pos_side_raw in ("LONG", "BOTH"):
                    side = "LONG"
                elif pos_side_raw == "SHORT":
                    side = "SHORT"
                else:
                    side = "LONG" if float(raw_amt) > 0 else "SHORT"

                entry_price = float(pos.get("entryPrice") or pos.get("avgPrice") or 0.0)
                leverage = float(pos.get("leverage") or 0.0)
                unrealized_pnl = float(pos.get("unrealizedProfit") or 0.0)

                ep = ExchangePositionORM(  # type: ignore[call-arg]
                    symbol=symbol,
                    side=side,
                    qty=qty,
                    entry_price=entry_price,
                    leverage=leverage,
                    unrealized_pnl=unrealized_pnl,
                    raw_json=pos,
                )
                session.add(ep)
            except Exception as e:
                log(f"[SYNC_DB] 포지션 변환/저장 실패: {e} raw={pos}")
                raise

        # 3) 미체결 주문: 해당 심볼 기준으로 전부 교체
        try:
            session.query(ExchangeOrderORM).filter(  # type: ignore[attr-defined]
                ExchangeOrderORM.symbol == symbol  # type: ignore[attr-defined]
            ).delete(synchronize_session=False)
        except Exception as e:
            log(f"[SYNC_DB] 기존 주문 삭제 실패: {e}")
            raise

        for o in orders:
            try:
                oid = o.get("orderId") or o.get("id")
                o_type = (o.get("type") or o.get("orderType") or "").upper()
                side = (o.get("side") or "").upper()
                price = float(o.get("price") or o.get("triggerPrice") or o.get("stopPrice") or 0.0)
                qty = float(o.get("origQty") or o.get("quantity") or o.get("size") or 0.0)

                eo = ExchangeOrderORM(  # type: ignore[call-arg]
                    symbol=symbol,
                    order_id=str(oid) if oid is not None else None,
                    side=side,
                    order_type=o_type,
                    price=price,
                    qty=qty,
                    raw_json=o,
                )
                session.add(eo)
            except Exception as e:
                log(f"[SYNC_DB] 주문 변환/저장 실패: {e} raw={o}")
                raise

        session.commit()
        log(
            f"[SYNC_DB] symbol={symbol} balance/positions/orders 동기화 완료 "
            f"(pos={len(positions)}, open_orders={len(orders)})"
        )

    except Exception as e:  # pragma: no cover - 예기치 못한 DB 에러 보호
        try:
            session.rollback()
        except Exception:
            pass
        log(f"[SYNC_DB] 동기화 실패 → 롤백: {e}")

    finally:
        try:
            session.close()
        except Exception:
            pass


def sync_open_trades_from_exchange(
    symbol: str,
    replace: bool = True,
    current_trades: Optional[List[Trade]] = None,
) -> Tuple[List[Trade], int]:
    """거래소에서 포지션/주문을 읽어서 Trade 리스트를 만든다.

    - current_trades 를 주면 그걸 기반으로 대체하거나 유지한다.
    - REST 조회가 실패하면 기존 리스트를 그대로 돌려주고, DB 동기화도 수행하지 않는다.
      (데이터 없는 상태에서 추정/폴백으로 만드는 동작은 없음)
    - 리턴: (새로운 Trade 리스트, 거래소에서 본 포지션 개수)
    """
    global _LAST_SYNCED_EXCHANGE_POS_COUNT

    if current_trades is None:
        current_trades = []

    # 1) 포지션 조회
    try:
        positions = fetch_open_positions(symbol)
    except Exception as e:
        log(f"[SYNC] fetch_open_positions error → 기존 Trade 유지, DB 동기화 SKIP: {e}")
        # 조회 실패 시 기존 리스트를 그대로 돌려준다.
        return current_trades, _LAST_SYNCED_EXCHANGE_POS_COUNT

    # 2) 주문 조회
    try:
        orders = fetch_open_orders(symbol)
    except Exception as e:
        log(f"[SYNC] fetch_open_orders error → 기존 Trade 유지, DB 동기화 SKIP: {e}")
        return current_trades, _LAST_SYNCED_EXCHANGE_POS_COUNT

    # 3) REST 데이터 기반으로 DB 스냅샷 먼저 시도 (실패해도 Trade 리스트는 계속 처리)
    try:
        _sync_exchange_state_to_db(symbol, positions or [], orders or [])
    except Exception as e:  # pragma: no cover - 방어적
        # _sync_exchange_state_to_db 내부에서 이미 로그/rollback 처리하지만,
        # 여기서도 한 번 더 남겨둔다.
        log(f"[SYNC] _sync_exchange_state_to_db 호출 중 예외: {e}")

    # 4) 실제 포지션이 없으면 메모리 Trade 비움
    if not positions:
        log("[SYNC] 열려 있는 포지션이 없습니다.")
        if replace:
            current_trades = []
            _LAST_SYNCED_EXCHANGE_POS_COUNT = 0
        return current_trades, _LAST_SYNCED_EXCHANGE_POS_COUNT

    # 5) 포지션이 있으면 다시 만든다
    if replace:
        current_trades = []

    for pos in positions:
        raw_amt = pos.get("positionAmt") or pos.get("quantity") or pos.get("size") or 0.0
        try:
            raw_amt_f = float(raw_amt)
        except (TypeError, ValueError):
            log(f"[SYNC] positionAmt 파싱 실패 → 스킵: raw={raw_amt}")
            continue

        if raw_amt_f == 0.0:
            continue

        # 방향 변환
        pos_side_raw = (pos.get("positionSide") or "").upper()
        if pos_side_raw in ("LONG", "BOTH"):
            side_open = "BUY"
        elif pos_side_raw == "SHORT":
            side_open = "SELL"
        else:
            side_open = "BUY" if raw_amt_f > 0 else "SELL"

        qty = abs(raw_amt_f)
        try:
            entry_price = float(pos.get("entryPrice") or pos.get("avgPrice") or 0.0)
        except (TypeError, ValueError):
            entry_price = 0.0

        # TP/SL 주문 찾기
        tp_id = None
        sl_id = None
        tp_price = None
        sl_price = None
        for o in orders:
            o_type = (o.get("type") or o.get("orderType") or "").upper()
            oid = o.get("orderId") or o.get("id")
            trig = o.get("triggerPrice") or o.get("stopPrice")
            if "TAKE" in o_type:
                tp_id = oid
                tp_price = float(trig) if trig else None
            if "STOP" in o_type:
                sl_id = oid
                sl_price = float(trig) if trig else None

        current_trades.append(
            Trade(
                symbol=symbol,
                side=side_open,
                qty=qty,
                entry_price=entry_price,        # ★ 추가됨
                leverage=float(pos.get("leverage") or 0.0),   # ★ 추가됨
                entry=entry_price,
                entry_order_id=None,
                tp_order_id=tp_id,
                sl_order_id=sl_id,
                tp_price=tp_price,
                sl_price=sl_price,
                source="SYNC",
            )
        )

    # 6) 개수 변화가 있으면 한 번만 알림
    if len(current_trades) != _LAST_SYNCED_EXCHANGE_POS_COUNT:
        send_tg(f"🔁 거래소 포지션 {len(current_trades)}건 동기화했습니다. (중복 진입 방지)")
        _LAST_SYNCED_EXCHANGE_POS_COUNT = len(current_trades)

    return current_trades, _LAST_SYNCED_EXCHANGE_POS_COUNT
