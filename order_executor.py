"""order_executor.py

BingX 선물 주문 실행 계층 (Execution Layer)

역할
----------------------------------------------------
- exchange_api.req(...) 를 사용해 **실제 돈이 오가는 주문 실행 로직만** 담당한다.
- 이 모듈은 다음과 같은 고수준 실행 기능을 제공한다:
  · 시장가 / 하이브리드(LIMIT+IOC) 진입(place_market)
  · 진입 + TP/SL 패키지(open_position_with_tp_sl)
  · TP/SL 조건부 주문(place_conditional, set_tp_sl)
  · 포지션 강제 청산(close_position_market)
  · 수량 정규화(_normalize_qty), 슬리피지 검사(_check_entry_slippage_and_maybe_close)

설계 원칙
----------------------------------------------------
1) 이 모듈은 "주문 실행"만 담당하고, REST 호출/서명은 exchange_api.req(...) 에 위임한다.
2) raw 시세/캔들 데이터는 market_data_ws 에서만 가져오며, 어떤 경우에도
   가격/체결 데이터를 보정하거나 인위적으로 생성하지 않는다(STRICT 원칙).
3) 슬리피지 가드는 **틱 단위 검증 + 텔레그램 경고**만 수행하고,
   절대 자동 청산을 하지 않는다. (청산 여부는 상위 레이어 정책에서만 수행)
4) entry_flow / position_watch_ws 는 이 모듈의 public 함수만 사용해
   주문을 실행하며, BingX API 파라미터를 직접 건드리지 않는다.

2025-12-XX 리팩터링
----------------------------------------------------
- 기존 exchange_api.py 에 섞여 있던 주문 실행/슬리피지/TP·SL/강제청산 로직을
  모두 이 파일로 분리해 계층 구조를 명확히 했다.
- 슬리피지 기대가격 계산 시 markPrice 단독이 아니라,
  오더북 bestBid/bestAsk 기반 mid-price 를 우선 사용하도록 개선했다.
"""

from __future__ import annotations

import math
import time
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from decimal import Decimal, ROUND_DOWN, getcontext

from settings_ws import load_settings
from telelog import log, send_tg
from exchange_api import req

if TYPE_CHECKING:
    from trader import Trade

# decimal 연산 정밀도 (수량/가격 계산 오차 방지용)
getcontext().prec = 28

# ─────────────────────────────
# 설정 및 심볼별 step/tick
# ─────────────────────────────
SET = load_settings()

_QTY_STEP: Dict[str, float] = {
    "BTC-USDT": 0.001,
}

_PRICE_TICK: Dict[str, float] = {
    "BTC-USDT": 0.1,  # BingX BTC-USDT 선물 최소 가격 단위
}


# ─────────────────────────────
# 공통 유틸: 수량 정규화 / tick size
# ─────────────────────────────
def _normalize_qty(symbol: str, raw_qty: float) -> float:
    """심볼별 최소 수량단위에 맞춰서 내림 정규화."""
    step = Decimal(str(_QTY_STEP.get(symbol, 0.001) or 0.001))
    q = Decimal(str(raw_qty if raw_qty is not None else 0))
    units = (q / step).to_integral_value(rounding=ROUND_DOWN)
    qty = (units * step) if units > 0 else step
    return float(qty.quantize(step, rounding=ROUND_DOWN))


def _get_tick_size(symbol: str) -> float:
    """심볼별 가격 tick size 를 반환한다. 설정값이 있으면 우선 사용."""
    try:
        s_val = getattr(SET, "price_tick_size", None)
        if isinstance(s_val, (int, float)) and s_val > 0:
            return float(s_val)
    except Exception:
        pass
    return float(_PRICE_TICK.get(symbol, 0.1))


def _is_param_error(exc: Exception) -> bool:
    """주문 포맷을 바꿔서 다시 시도할지 판단."""
    msg = str(exc).lower()
    needles = [
        "109400",
        "110400",
        "invalid parameters",
        "parameter invalid",
        "parameters invalid",
        "parameter error",
        "parameters error",
        "param error",
        "illegal parameter",
    ]
    return any(n in msg for n in needles)


# ─────────────────────────────
# 슬리피지 하드 가드 (틱 기반, 경고 전용)
# ─────────────────────────────
def _check_entry_slippage_and_maybe_close(
    *,
    symbol: str,
    side_req: str,
    qty: float,
    expected_price: float,
    fill_price: float,
) -> bool:
    """진입 직후 슬리피지를 검사하고 경고만 보내며 절대 자동 청산하지 않는다.

    - STRICT 원칙 준수: fill_price/expected_price 를 보정하지 않고,
      오더북과 hint 를 기반으로 "기대 가격이 어느 정도였는가"만 추정해 비교한다.
    - heavy slippage 가 감지되면 텔레그램 경고만 보내고, 포지션은 그대로 둔다.
      (자동 청산은 상위 레이어 정책에서만 수행 가능)
    """
    # 오더북 기반 기대가격 계산 (bestBid/bestAsk mid 우선)
    try:
        from market_data_ws import get_orderbook

        ob = get_orderbook(symbol, limit=5) or {}
        exp: Optional[float] = None

        bids = ob.get("bids") or []
        asks = ob.get("asks") or []
        best_bid = None
        best_ask = None
        try:
            if bids and isinstance(bids[0], (list, tuple)) and len(bids[0]) >= 1:
                best_bid = float(bids[0][0])
            if asks and isinstance(asks[0], (list, tuple)) and len(asks[0]) >= 1:
                best_ask = float(asks[0][0])
        except Exception:
            best_bid = None
            best_ask = None

        if best_bid and best_ask and best_bid > 0 and best_ask > 0:
            exp = (best_bid + best_ask) / 2.0
        else:
            # bid/ask 가 없다면 markPrice / lastPrice / bestBid / bestAsk 순으로 시도
            for key in ("markPrice", "lastPrice", "bestBid", "bestAsk"):
                v = ob.get(key)
                try:
                    f = float(v)
                except (TypeError, ValueError):
                    continue
                if f > 0:
                    exp = f
                    break

        if exp is None:
            exp = float(expected_price)

        fill = float(fill_price)
    except Exception as e:
        log(f"[SLIP GUARD] orderbook read failed for {symbol}: {e}")
        try:
            exp = float(expected_price)
            fill = float(fill_price)
        except Exception:
            # 기대가/체결가 둘 다 이상하면 가드를 건너뛴다.
            return True

    if exp <= 0 or fill <= 0:
        return True

    diff_abs = abs(fill - exp)
    tick_size = _get_tick_size(symbol)
    if tick_size <= 0:
        return True

    diff_ticks = diff_abs / tick_size
    max_ticks = float(getattr(SET, "max_entry_slippage_ticks", 20) or 0.0)

    if diff_ticks > max_ticks:
        pct_100 = (diff_abs / exp) * 100.0
        msg = (
            "⚠️ 진입 슬리피지 경고 (자동청산 없음)\n"
            f"- 심볼: {symbol}\n"
            f"- 방향: {side_req}\n"
            f"- 기대가(mid/mark 기반): {exp:.2f}\n"
            f"- 실제 체결가: {fill:.2f}\n"
            f"- 차이: {diff_abs:.2f} ≒ {diff_ticks:.1f} ticks (tick={tick_size})\n"
            f"- 비율: {pct_100:.3f}%\n"
            f"- 허용 한도: {max_ticks:.1f} ticks\n"
        )
        log(msg)
        try:
            send_tg(msg)
        except Exception:
            pass

    # 자동 청산 절대 없음 → 항상 True
    return True


# ─────────────────────────────
# 주문 재시도 (여러 payload 포맷)
# ─────────────────────────────
def _try_order(payloads: List[Dict[str, Any]]) -> Dict[str, Any]:
    """여러 형태의 payload 를 순서대로 보내서 재시도."""
    last_err: Optional[Exception] = None
    for idx, pay in enumerate(payloads, start=1):
        log(f"[PLACE TRY {idx}] {pay}")
        try:
            resp = req("POST", "/openApi/swap/v2/trade/order", pay)
            log(f"[PLACE RESP {idx}] {resp}")
            return resp
        except Exception as e:
            last_err = e
            log(f"[PLACE ERR {idx}] {e}")
            if not _is_param_error(e):
                break
            time.sleep(0.2)  # 백오프
    if last_err:
        raise last_err
    raise RuntimeError("order failed without explicit error")


# ─────────────────────────────
# 진입 + TP/SL 패키지
# ─────────────────────────────
def open_position_with_tp_sl(
    settings: Any,
    symbol: str,
    side_open: str,
    qty: float,
    entry_price_hint: float,
    tp_pct: float,
    sl_pct: float,
    source: str,
    soft_mode: bool = False,
    sl_floor_ratio: Optional[float] = None,
) -> Optional["Trade"]:
    """
    진입 + TP/SL 설정 엔트리 포인트.
    """
    from trader import Trade  # 지연 import 로 순환 의존성 방지

    side_up = str(side_open).upper()
    if side_up not in ("BUY", "SELL", "LONG", "SHORT"):
        log(f"[OPEN_POS] invalid side_open={side_open!r}")
        return None

    # BUY/SELL 로 정규화
    if side_up in ("LONG", "BUY"):
        side_req = "BUY"
    else:
        side_req = "SELL"

    # 1) 시장가(하이브리드) 주문 실행
    try:
        order_resp = place_market(symbol=symbol, side=side_req, qty=qty)
    except Exception as e:
        log(f"[OPEN_POS] place_market exception: {e}")
        try:
            send_tg(
                f"❗ 진입 주문 실패: {symbol} side={side_req} qty={qty} err={e}"
            )
        except Exception:
            pass
        return None

    if not isinstance(order_resp, Dict):
        log(f"[OPEN_POS] unexpected order response type: {type(order_resp)}")
        return None

    # 1-1) 체결 평균가 추출 (없으면 entry_price_hint 사용)
    raw = order_resp.get("data") or order_resp
    entry_price_val: Optional[float] = None
    if isinstance(raw, dict):
        for key in ("avgPrice", "price", "avg_price", "fillPrice", "executedPrice"):
            v = raw.get(key)
            if v is None:
                continue
            try:
                entry_price_val = float(v)
                break
            except (TypeError, ValueError):
                continue

    if entry_price_val is None or not math.isfinite(entry_price_val) or entry_price_val <= 0:
        entry_price_val = float(entry_price_hint or 0.0)

    if entry_price_val <= 0:
        log(
            f"[OPEN_POS] invalid entry_price (resp/hint invalid) "
            f"symbol={symbol} side={side_req}"
        )
        return None

    entry_price = entry_price_val

    # 1-2) 슬리피지 하드 가드 적용 (틱 기반, 경고 전용)
    try:
        if entry_price_hint and entry_price_hint > 0:
            ok = _check_entry_slippage_and_maybe_close(
                symbol=symbol,
                side_req=side_req,
                qty=qty,
                expected_price=float(entry_price_hint),
                fill_price=entry_price,
            )
            if not ok:
                log(
                    f"[OPEN_POS] heavy slippage detected "
                    f"(expected={entry_price_hint}, fill={entry_price}) → abort Trade"
                )
                return None
    except Exception as e:
        log(f"[OPEN_POS] slippage guard check failed: {e}")

    # 2) TP/SL 가격 계산
    if side_req == "BUY":
        tp_price = entry_price * (1 + tp_pct)
        sl_price = entry_price * (1 - sl_pct)
    else:
        tp_price = entry_price * (1 - tp_pct)
        sl_price = entry_price * (1 + sl_pct)

    # 2-1) SL 바닥 비율(sl_floor_ratio) 적용 (선택적)
    if isinstance(sl_floor_ratio, (int, float)) and sl_floor_ratio > 0:
        try:
            if side_req == "BUY":
                floor_price = entry_price * (1 - float(sl_floor_ratio))
                if sl_price > floor_price:
                    sl_price = floor_price
            else:
                floor_price = entry_price * (1 + float(sl_floor_ratio))
                if sl_price < floor_price:
                    sl_price = floor_price
        except Exception as e:
            log(f"[OPEN_POS] sl_floor_ratio adjust failed: {e}")

    # 3) TP/SL 조건부 주문 설정 (실패하더라도 진입 자체는 유지)
    try:
        set_tp_sl(
            symbol=symbol,
            side_open=side_req,
            qty=qty,
            tp_price=tp_price,
            sl_price=sl_price,
            soft_mode=soft_mode,
            sl_floor_ratio=sl_floor_ratio,
        )
    except Exception as e:
        log(f"[OPEN_POS] set_tp_sl error: {e}")
        try:
            send_tg(
                f"⚠️ TP/SL 설정 실패: {symbol} side={side_req} "
                f"tp={tp_price:.2f}, sl={sl_price:.2f}, err={e}"
            )
        except Exception:
            pass

    # 4) Trade 객체 생성
    try:
        leverage = float(getattr(settings, "leverage", 10.0) or 0.0)
    except Exception:
        leverage = 0.0

    trade = Trade(
        symbol=symbol,
        side=side_req,
        qty=qty,
        entry_price=entry_price,
        leverage=leverage,
        source=source,
        tp_price=tp_price,
        sl_price=sl_price,
    )
    return trade


# ─────────────────────────────
# Market-Limit Hybrid 주문
# ─────────────────────────────
def place_market(symbol: str, side: str, qty: float) -> Dict[str, Any]:
    """
    Market-Limit Hybrid 주문

    - 기본 전략: 오더북 bestBid/bestAsk 기반으로 거의 시장가와 동일한 LIMIT 주문을
      IOC(timeInForce="IOC") 로 전송해, 즉시 체결 또는 즉시 취소만 허용한다.
    - WS 오더북이 비정상인 경우에는 안전하게 일반 MARKET 으로 폴백한다.
    - STRICT 원칙: 실제 체결가는 BingX 가 결정하며, 여기서는 그 어떤 보정도 하지 않는다.
    """
    norm_qty = _normalize_qty(symbol, qty)
    side_u = side.upper()

    # WS 오더북 기반 기준가 산출
    base_price = 0.0
    try:
        from market_data_ws import get_orderbook

        ob = get_orderbook(symbol, limit=5) or {}
        bids = ob.get("bids") or []
        asks = ob.get("asks") or []

        best_bid = None
        best_ask = None
        if bids and isinstance(bids[0], (list, tuple)) and len(bids[0]) >= 1:
            best_bid = float(bids[0][0])
        if asks and isinstance(asks[0], (list, tuple)) and len(asks[0]) >= 1:
            best_ask = float(asks[0][0])

        if side_u == "BUY":
            # 매수 → bestAsk 기준 (없으면 bestBid)
            if best_ask and best_ask > 0:
                base_price = best_ask
            elif best_bid and best_bid > 0:
                base_price = best_bid
        else:
            # 매도 → bestBid 기준 (없으면 bestAsk)
            if best_bid and best_bid > 0:
                base_price = best_bid
            elif best_ask and best_ask > 0:
                base_price = best_ask

        if base_price <= 0:
            # 그래도 못 구하면 markPrice/lastPrice 참고
            for key in ("markPrice", "lastPrice"):
                v = ob.get(key)
                try:
                    f = float(v)
                except (TypeError, ValueError):
                    continue
                if f > 0:
                    base_price = f
                    break
    except Exception as e:
        log(f"[PLACE HYBRID] get_orderbook failed for {symbol}: {e}")
        base_price = 0.0

    if base_price <= 0:
        # WS 가격 실패 → 안전하게 일반 MARKET 사용
        log(f"[PLACE HYBRID] WS price invalid → fallback MARKET")
        payload = {
            "symbol": symbol,
            "side": side_u,
            "positionSide": "BOTH",
            "type": "MARKET",
            "quantity": norm_qty,
            "recvWindow": 5000,
        }
        return _try_order([payload])

    # 허용 슬리피지 비율 (기본 0.05% = 0.0005), 설정값이 있으면 사용
    slip_ratio = float(getattr(SET, "hybrid_limit_offset", 0.0005) or 0.0005)

    if side_u == "BUY":
        limit_price = base_price * (1.0 + slip_ratio)
    else:
        limit_price = base_price * (1.0 - slip_ratio)

    # 가격 소수점 자릿수는 심볼 tick_size 를 기반으로 조정 (대략적인 라운딩)
    tick = _get_tick_size(symbol)
    if tick > 0:
        # tick 의 소수 자릿수 추정
        decimals = max(0, -int(round(math.log10(tick)))) if tick < 1 else 0
        limit_price = round(limit_price, decimals)
    else:
        limit_price = round(limit_price, 2)

    log(
        f"[PLACE HYBRID] symbol={symbol} side={side_u} qty={norm_qty} "
        f"base_price={base_price} limit_price={limit_price}"
    )

    # LIMIT + IOC → 즉시체결 or 취소 (슬리피지 제한)
    payload = {
        "symbol": symbol,
        "side": side_u,
        "positionSide": "BOTH",
        "type": "LIMIT",
        "price": limit_price,
        "quantity": norm_qty,
        "timeInForce": "IOC",
        "recvWindow": 5000,
    }

    try:
        return _try_order([payload])
    except Exception as e:
        # LIMIT 실패 → fallback MARKET
        log(f"[PLACE HYBRID] limit fail → fallback MARKET: {e}")
        payload_mkt = {
            "symbol": symbol,
            "side": side_u,
            "positionSide": "BOTH",
            "type": "MARKET",
            "quantity": norm_qty,
            "recvWindow": 5000,
        }
        return _try_order([payload_mkt])


# ─────────────────────────────
# TP/SL 조건부 주문
# ─────────────────────────────
def place_conditional(
    symbol: str,
    side: str,
    qty: float,
    trigger_price: float,
    order_type: str,
) -> Dict[str, Any]:
    """TP/SL 조건부 주문."""
    norm_qty = _normalize_qty(symbol, qty)

    log(
        f"[PLACE COND] symbol={symbol} side={side} qty={qty}(norm={norm_qty}) "
        f"trigger={trigger_price} type={order_type}"
    )

    working_type = getattr(SET, "conditional_working_type", "MARK_PRICE")

    base_common = {
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "quantity": norm_qty,
        "recvWindow": 5000,
        "workingType": working_type,
    }

    payloads: List[Dict[str, Any]] = [
        {**base_common, "positionSide": "BOTH", "reduceOnly": True, "triggerPrice": trigger_price},
        {**base_common, "positionSide": "BOTH", "reduceOnly": True, "stopPrice": trigger_price},
        {**base_common, "positionSide": "BOTH", "reduceOnly": True, "activationPrice": trigger_price},
        {**base_common, "reduceOnly": True, "triggerPrice": trigger_price},
        {**base_common, "reduceOnly": True, "stopPrice": trigger_price},
        {**base_common, "reduceOnly": True, "activationPrice": trigger_price},
        {**base_common, "triggerPrice": trigger_price},
        {**base_common, "stopPrice": trigger_price},
        {**base_common, "activationPrice": trigger_price},
        {
            **base_common,
            "positionSide": "LONG" if side.upper() == "BUY" else "SHORT",
            "reduceOnly": True,
            "triggerPrice": trigger_price,
        },
    ]

    try:
        return _try_order(payloads)
    except Exception:
        # 수량이 안 맞는 케이스 대비: 최소수량으로 재시도
        min_qty = _normalize_qty(symbol, 0)
        payloads_min = [{**p, "quantity": min_qty} for p in payloads]
        return _try_order(payloads_min)


def set_tp_sl(
    *,
    symbol: str,
    side_open: str,
    qty: float,
    tp_price: float,
    sl_price: float,
    soft_mode: bool = False,
    sl_floor_ratio: Optional[float] = None,
) -> None:
    """BingX 조건부 주문을 이용해 TP/SL 설정."""
    side_up = str(side_open).upper()
    if side_up in ("LONG", "BUY"):
        open_side = "BUY"
    elif side_up in ("SHORT", "SELL"):
        open_side = "SELL"
    else:
        log(f"[SET_TP_SL] invalid side_open={side_open!r}")
        return

    if tp_price <= 0 and sl_price <= 0:
        log(f"[SET_TP_SL] both tp/sl <= 0 → skip (symbol={symbol})")
        return

    close_side = "SELL" if open_side == "BUY" else "BUY"

    # TP
    if tp_price and tp_price > 0:
        try:
            place_conditional(
                symbol=symbol,
                side=close_side,
                qty=qty,
                trigger_price=float(tp_price),
                order_type="TAKE_PROFIT_MARKET",
            )
        except Exception as e:
            log(
                f"[SET_TP_SL] TP 조건부 주문 실패 "
                f"symbol={symbol} side_open={open_side} close_side={close_side} "
                f"tp_price={tp_price}: {e}"
            )

    if soft_mode:
        return

    # SL
    if sl_price and sl_price > 0:
        try:
            place_conditional(
                symbol=symbol,
                side=close_side,
                qty=qty,
                trigger_price=float(sl_price),
                order_type="STOP_MARKET",
            )
        except Exception as e:
            log(
                f"[SET_TP_SL] SL 조건부 주문 실패 "
                f"symbol={symbol} side_open={open_side} close_side={close_side} "
                f"sl_price={sl_price}: {e}"
            )


# ─────────────────────────────
# 강제 청산 (시장가)
# ─────────────────────────────
def close_position_market(symbol: str, side_open: str, qty: float) -> None:
    """포지션을 시장가로 강제 청산."""
    close_side = "SELL" if side_open.upper() == "BUY" else "BUY"
    norm_qty = _normalize_qty(symbol, qty)

    log(
        f"[FORCE CLOSE] symbol={symbol} open_side={side_open} close_side={close_side} "
        f"qty_req={qty} qty_norm={norm_qty}"
    )

    payloads = [
        {
            "symbol": symbol,
            "side": close_side,
            "positionSide": "BOTH",
            "type": "MARKET",
            "quantity": norm_qty,
            "reduceOnly": True,
            "recvWindow": 5000,
        },
        {
            "symbol": symbol,
            "side": close_side,
            "type": "MARKET",
            "quantity": norm_qty,
            "reduceOnly": True,
            "recvWindow": 5000,
        },
        {
            "symbol": symbol,
            "side": close_side,
            "positionSide": "LONG" if close_side == "BUY" else "SHORT",
            "type": "MARKET",
            "quantity": norm_qty,
            "reduceOnly": True,
            "recvWindow": 5000,
        },
    ]
    try:
        _try_order(payloads)
        send_tg(f"⚠️ 포지션을 즉시 시장가로 닫았습니다. 수량={norm_qty}")
        return
    except Exception as e:
        msg = str(e)
        log(f"[FORCE CLOSE ERR] {msg}")
        if "101290" in msg:
            # reduceOnly 관련 오류 → RO 제거 후 재시도
            no_ro_payloads = [
                {
                    "symbol": symbol,
                    "side": close_side,
                    "type": "MARKET",
                    "quantity": norm_qty,
                    "recvWindow": 5000,
                }
            ]
            try:
                _try_order(no_ro_payloads)
                send_tg(f"⚠️ 포지션을 즉시 시장가로 닫았습니다(RO제거). 수량={norm_qty}")
                return
            except Exception as e2:
                log(f"[FORCE CLOSE ERR NO-RO] {e2}")
                send_tg(f"❗ 포지션 강제 정리 실패(RO제거도 실패): {e2}")
                return

        # 기타 케이스는 심볼 최소 수량으로 재시도
        min_qty = _normalize_qty(symbol, 0)
        payloads_min = [{**p, "quantity": min_qty} for p in payloads]
        try:
            _try_order(payloads_min)
            send_tg(f"⚠️ 포지션을 즉시 시장가로 닫았습니다. 수량={min_qty}")
        except Exception as e3:
            log(f"[FORCE CLOSE ERR MIN] {e3}")
            send_tg(f"❗ 포지션 강제 정리 실패: {e3}")


__all__ = [
    "open_position_with_tp_sl",
    "place_market",
    "place_conditional",
    "set_tp_sl",
    "close_position_market",
]
