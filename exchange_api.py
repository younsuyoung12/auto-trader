"""
exchange_api.py
BingX REST API 호출과 관련된 저수준 함수 모음.

이 모듈이 담당하는 것:
- 쿼리 스트링 서명 (HMAC SHA256)
- 공통 요청(req)
- 레버리지/마진 설정
- 잔고 조회, 포지션 조회, 주문 조회
- 시장가/조건부 주문 생성
- 주문 상태 폴링(wait_filled)
- 체결내역 요약(summarize_fills)

2025-11-10 수정
- 일부 계정/원웨이 모드에서 MARKET/조건부 주문에 positionSide 를 넣으면
  bingx code=109400 이 나는 현상이 있었다.
- 그래서 place_market(), place_conditional(), close_position_market() 에
  "1차: positionSide 포함해서 보내기 → 109400 나오면 positionSide 빼고 다시 보내기"
  폴백 로직을 넣었다.
- 이렇게 하면 헤지 모드 계정에서도 되고, 원웨이 계정에서도 같은 코드로 동작한다.

2025-11-09 수정/추가 내용
1) 응답 검증 강화
   - BingX가 HTTP 200을 줘도 JSON 안에 code가 0/None/100400 이 아니면 실패로 보도록 req()를 보강했다.
   - 일부 엔드포인트가 주는 100400은 여전히 예외 안 던지고 넘겨서 상위에서 처리할 수 있게 했다.

2) 주문 응답 로그 추가
   - place_market(), place_conditional() 이 BingX 원본 응답을 log()로 남기게 했다.
   - 진입 직후 orderId가 없다고 나올 때 서버 로그로 원인 파악할 수 있게 하기 위함.

3) 레버리지/마진 설정 예외 완화
   - 어떤 계정은 API로 레버리지/마진을 못 바꿔서 109400(Invalid parameters)이 뜬다.
   - set_leverage_and_mode()에서 LONG/SHORT/marginType 을 각각 시도하되,
     109400인 경우에는 “지원 안 하는 듯” 하고 경고만 남기고 봇은 계속 돌도록 완화했다.
   - 이렇게 하면 run_bot.py 시작할 때마다 에러로 죽지 않는다.

4) 선물 수량 정규화 추가 (중요)
   - BingX 선물 BTC-USDT 등은 보통 quantity step 이 0.001 이라서
     run_bot.py 에서 계산된 0.005904 같은 값을 그대로 보내면 109400 이 발생할 수 있다.
   - place_market(), place_conditional(), close_position_market() 에서 모두
     0.001 단위로 내림(floor) 후 소수 셋째 자리까지로 잘라서 전송하도록 변경했다.
   - 이 변경으로 전략단(run_bot.py / trader.py)에서 약간 지저분한 수량을 내려보내도
     실제 API 호출 시에는 거래소가 받는 단위로 정리된다.

5) 잔고 상세(raw) 조회 헬퍼 추가
   - 일부 계정은 포지션 API가 없어서 run_bot.py 쪽에서 “사람이 들고 있는 포지션이 있는지”를
     잔고의 usedMargin 으로 추정해야 한다.
   - 이를 위해 /openApi/swap/v2/user/balance 의 안쪽 balance dict 를 그대로 꺼내는
     get_balance_detail() 을 추가했다.

6) positionSide 강제 추가 (헤지/원웨이 공통 호환)
   - (2025-11-10 에서 이 부분을 ‘가능하면 추가, 안 되면 제거’ 방식으로 더 유연하게 바꿨다.)
"""

from __future__ import annotations

import time
import hmac
import hashlib
from typing import Any, Dict, List, Optional
import requests

from settings import load_settings
from telelog import log, send_tg

# 설정 읽기 (전역으로 보관)
SET = load_settings()
BASE = SET.bingx_base  # 기본: https://open-api.bingx.com

# ─────────────────────────────
# 심볼별 수량 step (선물 전용)
# ─────────────────────────────
_QTY_STEP: Dict[str, float] = {
    "BTC-USDT": 0.001,
    "BTCUSDT": 0.001,
}


# ─────────────────────────────
# 공통 유틸
# ─────────────────────────────
def _ts_ms() -> int:
    return int(time.time() * 1000)


def sign_query(params: Dict[str, Any], api_secret: str) -> str:
    qs = "&".join(f"{k}={params[k]}" for k in sorted(params.keys()))
    sig = hmac.new(api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
    return qs + "&signature=" + sig


def _headers() -> Dict[str, str]:
    return {
        "X-BX-APIKEY": SET.api_key,
        "Content-Type": "application/json",
    }


def _normalize_qty(symbol: str, raw_qty: float) -> float:
    step = _QTY_STEP.get(symbol, 0.001)
    if step <= 0:
        step = 0.001
    units = int(raw_qty / step)
    qty = units * step
    if qty <= 0:
        qty = step
    return float(f"{qty:.3f}")


def req(
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    params = params or {}
    params["timestamp"] = _ts_ms()
    url = f"{BASE}{path}?{sign_query(params, SET.api_secret)}"
    r = requests.request(method, url, json=body, headers=_headers(), timeout=12)

    if r.status_code != 200:
        raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text}")

    data = r.json()

    if isinstance(data, dict):
        code = data.get("code")
        if code not in (None, 0, "0", 100400):
            # 여기서 바로 예외를 던지기 때문에,
            # 위쪽 함수(place_market 등)에서 109400만 잡아서 폴백하면 된다.
            raise RuntimeError(
                f"{method} {path} -> bingx code={code}, msg={data.get('msg') or data}"
            )

    return data


# 109400 인지 검사하는 작은 헬퍼
def _is_param_error(exc: Exception) -> bool:
    msg = str(exc)
    return "109400" in msg or "Invalid parameters" in msg


# ─────────────────────────────
# 계좌/포지션/주문 조회
# ─────────────────────────────
def get_available_usdt() -> float:
    try:
        res = req("GET", "/openApi/swap/v2/user/balance", {})
        log(f"[BALANCE RAW] {res}")

        data = res.get("data") or res.get("balances") or res

        if isinstance(data, dict) and "balance" in data and isinstance(data["balance"], dict):
            bal = data["balance"]
            cand = (
                bal.get("availableMargin")
                or bal.get("availableBalance")
                or bal.get("balance")
                or bal.get("equity")
                or 0.0
            )
            return float(cand)

        if isinstance(data, list) and data:
            item = data[0]
        else:
            item = data

        if isinstance(item, dict) and "balance" in item and isinstance(item["balance"], dict):
            bal = item["balance"]
            cand = (
                bal.get("availableMargin")
                or bal.get("availableBalance")
                or bal.get("balance")
                or bal.get("equity")
                or 0.0
            )
            return float(cand)

        cand = (
            item.get("availableBalance")
            or item.get("availableMargin")
            or item.get("balance")
            or 0.0
        )
        return float(cand)

    except Exception as e:
        log(f"[BALANCE ERROR] {e}")
        send_tg(f"⚠️ 잔고 조회 실패: {e}")
        return 0.0


def get_balance_detail() -> Dict[str, Any]:
    res = req("GET", "/openApi/swap/v2/user/balance", {})
    log(f"[BALANCE RAW for detail] {res}")

    data = res.get("data") or res.get("balances") or res

    if isinstance(data, dict) and "balance" in data and isinstance(data["balance"], dict):
        return data["balance"]

    if isinstance(data, list) and data:
        first = data[0]
        if isinstance(first, dict) and "balance" in first and isinstance(first["balance"], dict):
            return first["balance"]
        return first

    if isinstance(data, dict):
        return data

    return {"raw": res}


def fetch_open_positions(symbol: str) -> List[Dict[str, Any]]:
    try:
        res = req("GET", "/openApi/swap/v2/trade/positions", {"symbol": symbol})
        if res.get("code") == 100400:
            log("[POSITIONS] this api is not exist -> skip syncing positions")
            return []
        log(f"[POSITIONS RAW] {res}")
        data = res.get("data") or res.get("positions") or []
        if not isinstance(data, list):
            data = [data]
        return data
    except Exception as e:
        log(f"[POSITIONS ERROR] {e}")
        return []


def fetch_open_orders(symbol: str) -> List[Dict[str, Any]]:
    try:
        res = req("GET", "/openApi/swap/v2/trade/openOrders", {"symbol": symbol})
        log(f"[OPEN ORDERS RAW] {res}")
        data = res.get("data") or res.get("orders") or []
        if not isinstance(data, list):
            data = [data]
        return data
    except Exception as e:
        log(f"[OPEN ORDERS ERROR] {e}")
        return []


# ─────────────────────────────
# 레버리지/마진
# ─────────────────────────────
def set_leverage_and_mode(symbol: str, leverage: int, isolated: bool = True) -> None:
    errs: List[str] = []

    for side in ("LONG", "SHORT"):
        try:
            req("POST", "/openApi/swap/v2/trade/leverage", {
                "symbol": symbol,
                "leverage": leverage,
                "side": side,
            })
        except Exception as e:
            msg = str(e)
            if "109400" in msg:
                log(f"[WARN] 레버리지({side})는 API로 설정 불가해 보여서 건너뜁니다: {msg}")
            else:
                errs.append(f"{side}: {msg}")

    try:
        req("POST", "/openApi/swap/v2/trade/marginType", {
            "symbol": symbol,
            "marginType": "ISOLATED" if isolated else "CROSSED",
        })
    except Exception as e:
        msg = str(e)
        if "109400" in msg:
            log(f"[WARN] 마진모드는 API로 설정 불가해 보여서 건너뜁니다: {msg}")
        else:
            errs.append(f"MARGIN: {msg}")

    if errs:
        log(f"[WARN] 레버리지/마진 설정 일부 실패: {', '.join(errs)}")


# ─────────────────────────────
# 주문 전송 (109400 폴백 포함)
# ─────────────────────────────
def place_market(symbol: str, side: str, qty: float) -> Dict[str, Any]:
    """
    시장가 주문 발행.
    1) positionSide 포함해서 보냄
    2) 109400 이면 positionSide 뺀 걸로 다시 보냄
    """
    norm_qty = _normalize_qty(symbol, qty)
    position_side = "LONG" if side.upper() == "BUY" else "SHORT"

    payload = {
        "symbol": symbol,
        "side": side,
        "positionSide": position_side,
        "type": "MARKET",
        "quantity": norm_qty,
        "recvWindow": 5000,
    }
    log(f"[PLACE MARKET REQ] {payload}")
    try:
        resp = req("POST", "/openApi/swap/v2/trade/order", payload)
        log(f"[PLACE MARKET RESP] {resp}")
        return resp
    except Exception as e:
        # 여기서 109400 이면 positionSide 빼고 재시도
        if _is_param_error(e):
            fallback_payload = {
                "symbol": symbol,
                "side": side,
                "type": "MARKET",
                "quantity": norm_qty,
                "recvWindow": 5000,
            }
            log(f"[PLACE MARKET RETRY w/o positionSide] {fallback_payload}")
            resp = req("POST", "/openApi/swap/v2/trade/order", fallback_payload)
            log(f"[PLACE MARKET RESP RETRY] {resp}")
            return resp
        # 다른 에러면 그대로 위로
        raise


def place_conditional(
    symbol: str,
    side: str,
    qty: float,
    trigger_price: float,
    order_type: str,
) -> Dict[str, Any]:
    """
    TP/SL 조건부 주문.
    1) positionSide 포함해서 보냄
    2) 109400 이면 positionSide 빼고 다시 보냄
    """
    norm_qty = _normalize_qty(symbol, qty)
    position_side = "LONG" if side.upper() == "BUY" else "SHORT"
    payload = {
        "symbol": symbol,
        "side": side,
        "positionSide": position_side,
        "type": order_type,
        "quantity": norm_qty,
        "reduceOnly": True,
        "triggerPrice": trigger_price,
        "recvWindow": 5000,
    }
    log(f"[PLACE CONDITIONAL REQ] {payload}")
    try:
        resp = req("POST", "/openApi/swap/v2/trade/order", payload)
        log(f"[PLACE CONDITIONAL RESP] {resp}")
        return resp
    except Exception as e:
        if _is_param_error(e):
            fallback_payload = {
                "symbol": symbol,
                "side": side,
                "type": order_type,
                "quantity": norm_qty,
                "reduceOnly": True,
                "triggerPrice": trigger_price,
                "recvWindow": 5000,
            }
            log(f"[PLACE CONDITIONAL RETRY w/o positionSide] {fallback_payload}")
            resp = req("POST", "/openApi/swap/v2/trade/order", fallback_payload)
            log(f"[PLACE CONDITIONAL RESP RETRY] {resp}")
            return resp
        raise


def cancel_order(symbol: str, order_id: str) -> None:
    try:
        res = req("POST", "/openApi/swap/v2/trade/cancel", {
            "symbol": symbol,
            "orderId": order_id,
        })
        log(f"[CANCEL] order_id={order_id} resp={res}")
    except Exception as e:
        log(f"[CANCEL ERROR] {e}")


def get_order(symbol: str, order_id: str) -> Dict[str, Any]:
    return req("GET", "/openApi/swap/v2/trade/order", {
        "symbol": symbol,
        "orderId": order_id,
    })


def get_fills(symbol: str, order_id: str) -> List[Dict[str, Any]]:
    res = req("GET", "/openApi/swap/v2/trade/allFillOrders", {
        "symbol": symbol,
        "orderId": order_id,
        "limit": 50,
    })
    return res.get("data", []) or []


def summarize_fills(symbol: str, order_id: str) -> Optional[Dict[str, Any]]:
    fills = get_fills(symbol, order_id)
    if not fills:
        return None
    total_qty = 0.0
    total_pnl = 0.0
    notional = 0.0
    last_time = None
    for f in fills:
        q = float(
            f.get("quantity")
            or f.get("qty")
            or f.get("volume")
            or f.get("vol")
            or 0.0
        )
        px = float(f.get("price") or f.get("avgPrice") or 0.0)
        pnl = float(f.get("realizedPnl") or 0.0)
        total_qty += q
        notional += q * px
        total_pnl += pnl
        last_time = f.get("time") or f.get("updateTime") or last_time
    avg_px = notional / total_qty if total_qty else 0.0
    return {
        "qty": total_qty,
        "avg_price": avg_px,
        "pnl": total_pnl,
        "time": last_time,
    }


def wait_filled(symbol: str, order_id: str, timeout: int = 5) -> Optional[Dict[str, Any]]:
    end = time.time() + timeout
    last_status = None
    while time.time() < end:
        try:
            o = get_order(symbol, order_id)
            d = o.get("data") or o
            status = d.get("status") or d.get("orderStatus")
            last_status = status
            if status in ("FILLED", "PARTIALLY_FILLED"):
                return d
        except Exception as e:
            log(f"[wait_filled] error: {e}")
        time.sleep(0.5)
    log(f"[wait_filled] timeout, last_status={last_status}, cancel order")
    cancel_order(symbol, order_id)
    return None


def close_position_market(symbol: str, side_open: str, qty: float) -> None:
    """
    포지션 강제 종료용.
    여기도 계정 따라 positionSide 가 문제될 수 있어서
    109400 이면 빼고 다시 보낸다.
    """
    close_side = "SELL" if side_open == "BUY" else "BUY"
    norm_qty = _normalize_qty(symbol, qty)
    position_side = "SHORT" if side_open.upper() == "BUY" else "LONG"
    payload = {
        "symbol": symbol,
        "side": close_side,
        "positionSide": position_side,
        "type": "MARKET",
        "quantity": norm_qty,
        "reduceOnly": True,
        "recvWindow": 5000,
    }
    log(f"[FORCE CLOSE REQ] {payload}")
    try:
        req("POST", "/openApi/swap/v2/trade/order", payload)
        send_tg(f"⚠️ 포지션을 즉시 시장가로 닫았습니다. 수량={norm_qty}")
    except Exception as e:
        if _is_param_error(e):
            fallback_payload = {
                "symbol": symbol,
                "side": close_side,
                "type": "MARKET",
                "quantity": norm_qty,
                "reduceOnly": True,
                "recvWindow": 5000,
            }
            log(f"[FORCE CLOSE RETRY w/o positionSide] {fallback_payload}")
            req("POST", "/openApi/swap/v2/trade/order", fallback_payload)
            send_tg(f"⚠️ 포지션을 즉시 시장가로 닫았습니다. 수량={norm_qty} (fallback)")
        else:
            send_tg(f"❗ 포지션 강제 정리 실패: {e}")


__all__ = [
    "req",
    "get_available_usdt",
    "get_balance_detail",
    "fetch_open_positions",
    "fetch_open_orders",
    "set_leverage_and_mode",
    "place_market",
    "place_conditional",
    "cancel_order",
    "get_order",
    "get_fills",
    "summarize_fills",
    "wait_filled",
    "close_position_market",
    "sign_query",
]
