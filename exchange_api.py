"""
exchange_api.py
BingX REST API 호출과 관련된 저수준 함수 모음.

2025-11-12 수정 (7차)
----------------------------------------------------
(배경)
- 실제 계정에서 주문을 넣어보니, 단순히 positionSide="BOTH" 로는
  "Invalid parameters" 가 계속 발생했고,
  사용자가 올려준 BingX 예제 파이썬 코드에서는
    positionSide: "LONG"
  처럼 방향을 명시해서 보내고 있었다.
- 따라서 이 계정은 주문마다 포지션 방향을 LONG/SHORT 으로 넣어줘야만
  정상 주문이 되는 형태로 보인다.

(이번 변경)
1) place_market(...) 이 side 에 맞춰서 positionSide 를 LONG/SHORT 으로 자동 설정하게 했다.
   - side == "BUY"  → positionSide="LONG"
   - side == "SELL" → positionSide="SHORT"

2) place_conditional(...) 에서도 TP/SL 주문일 때는 방향을 추론해서
   - TP/SL 이고 side == "SELL" 이면  → 기존 LONG 포지션 닫는다고 보고 positionSide="LONG"
   - TP/SL 이고 side == "BUY"  이면  → 기존 SHORT 포지션 닫는다고 보고 positionSide="SHORT"
   으로 보내게 했다.
   - 필요하면 호출부에서 position_side=... 로 강제로 지정할 수도 있게 파라미터를 추가했다.
     (기존 호출 방식은 그대로 동작)

3) close_position_market(...) 도 원래 포지션 방향을 받아서
   반대 side + 그 포지션의 positionSide 로 보내도록 바꿨다.

4) 레버리지/마진 설정은 계정별 요구 파라미터가 다른 것으로 보이므로
   기존처럼 여러 형태로 시도하되 실패해도 봇이 멈추지 않게 유지했다.
----------------------------------------------------

2025-11-11 수정 (6차)
----------------------------------------------------
(단방향 계정에서 positionSide 를 요구하므로 주문에 positionSide 를 넣는 버전)
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
BASE = SET.bingx_base  # 예: https://open-api.bingx.com

# ─────────────────────────────
# 심볼별 수량 step (선물 전용)
# ─────────────────────────────
_QTY_STEP: Dict[str, float] = {
    "BTC-USDT": 0.001,  # 기본으로 이걸 쓴다
}


# ─────────────────────────────
# 공통 유틸
# ─────────────────────────────
def _ts_ms() -> int:
    """현재 ms 타임스탬프"""
    return int(time.time() * 1000)


def sign_query(params: Dict[str, Any], api_secret: str) -> str:
    """파라미터를 정렬해서 HMAC-SHA256 서명 붙이기"""
    qs = "&".join(f"{k}={params[k]}" for k in sorted(params.keys()))
    sig = hmac.new(api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
    return qs + "&signature=" + sig


def _headers() -> Dict[str, str]:
    """BingX 필수 헤더"""
    return {
        "X-BX-APIKEY": SET.api_key,
        "Content-Type": "application/json",
    }


def _normalize_qty(symbol: str, raw_qty: float) -> float:
    """
    선물 수량을 거래소가 받는 최소 단위로 내린다.
    예: step=0.001, raw=0.005904 → 0.005
    """
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
    """
    BingX REST 요청 공통부.
    - HTTP 200이라도 data["code"] 가 0/None/100400 이 아니면 예외로 본다.
    """
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
            raise RuntimeError(
                f"{method} {path} -> bingx code={code}, msg={data.get('msg') or data}"
            )

    return data


def _is_param_error(exc: Exception) -> bool:
    """109400, Invalid parameters 같은 파라미터 오류인지 판별"""
    msg = str(exc)
    return "109400" in msg or "Invalid parameters" in msg


# ─────────────────────────────
# 계좌/포지션/주문 조회
# ─────────────────────────────
def get_available_usdt() -> float:
    """가용 마진(availableMargin/Balance)을 최대한 평탄화해서 가져온다."""
    try:
        res = req("GET", "/openApi/swap/v2/user/balance", {})
        log(f"[BALANCE RAW] {res}")

        data = res.get("data") or res.get("balances") or res

        # {"data": {"balance": {...}}} 구조
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

        # 리스트 구조
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
    """잔고 전체 구조를 그대로 반환."""
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
    """
    열려 있는 포지션 목록 조회.
    1) /user/positions 먼저
    2) 안 되면 /trade/positions 로 폴백
    """
    # 1차
    try:
        res = req("GET", "/openApi/swap/v2/user/positions", {"symbol": symbol})
        log(f"[POSITIONS RAW user] {res}")
        data = res.get("data") or res.get("positions") or []
        if not isinstance(data, list):
            data = [data]
        return data
    except Exception as e1:
        log(f"[POSITIONS user ERROR] {e1}")

    # 2차
    try:
        res = req("GET", "/openApi/swap/v2/trade/positions", {"symbol": symbol})
        if res.get("code") == 100400:
            log("[POSITIONS] this api is not exist -> skip syncing positions")
            return []
        log(f"[POSITIONS RAW trade] {res}")
        data = res.get("data") or res.get("positions") or []
        if not isinstance(data, list):
            data = [data]
        return data
    except Exception as e2:
        log(f"[POSITIONS trade ERROR] {e2}")
        return []


def fetch_open_orders(symbol: str) -> List[Dict[str, Any]]:
    """걸려 있는 주문 목록 조회"""
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
    """
    계정에서 side/positionSide 를 요구할 수 있으므로 몇 가지 패턴을 다 시도한다.
    실패해도 봇은 계속 돌게 하고, 로그에만 남긴다.
    """
    # 1) side 기반 (LONG / SHORT)
    for side in ("LONG", "SHORT"):
        try:
            req("POST", "/openApi/swap/v2/trade/leverage", {
                "symbol": symbol,
                "side": side,
                "leverage": leverage,
            })
        except Exception as e:
            log(f"[WARN] 레버리지 설정 실패({side}): {e}")

    # 2) positionSide 기반 (LONG / SHORT)
    for ps in ("LONG", "SHORT"):
        try:
            req("POST", "/openApi/swap/v2/trade/leverage", {
                "symbol": symbol,
                "positionSide": ps,
                "leverage": leverage,
            })
        except Exception as e:
            log(f"[WARN] 레버리지 설정 실패(positionSide={ps}): {e}")

    # 3) 마진 모드
    try:
        req("POST", "/openApi/swap/v2/trade/marginType", {
            "symbol": symbol,
            "marginType": "ISOLATED" if isolated else "CROSSED",
        })
    except Exception as e:
        log(f"[WARN] 마진모드 설정 실패: {e}")


# ─────────────────────────────
# 주문 전송
# ─────────────────────────────
def place_market(symbol: str, side: str, qty: float) -> Dict[str, Any]:
    """
    시장가 주문
    - 이 계정은 positionSide 를 실제 LONG/SHORT 으로 지정해야 하므로 side 에 맞춰 넣는다.
    """
    norm_qty = _normalize_qty(symbol, qty)
    side_u = side.upper()
    if side_u == "BUY":
        pos_side = "LONG"
    else:
        pos_side = "SHORT"

    payload = {
        "symbol": symbol,
        "side": side_u,
        "positionSide": pos_side,
        "type": "MARKET",
        "quantity": norm_qty,
        "recvWindow": 5000,
    }
    log(f"[PLACE MARKET REQ] {payload}")
    resp = req("POST", "/openApi/swap/v2/trade/order", payload)
    log(f"[PLACE MARKET RESP] {resp}")
    return resp


def place_conditional(
    symbol: str,
    side: str,
    qty: float,
    trigger_price: float,
    order_type: str,
    position_side: Optional[str] = None,
) -> Dict[str, Any]:
    """
    TP/SL 조건부 주문
    - 이 계정은 positionSide 를 요구하므로 가능한 한 방향을 추론해서 넣는다.
    - 호출부에서 position_side 를 직접 넘기면 그 값을 그대로 사용한다.
    """
    norm_qty = _normalize_qty(symbol, qty)
    side_u = side.upper()
    order_type_u = order_type.upper()

    # position_side 가 안 들어오면 추론
    ps = position_side
    if not ps:
        # TP/SL 이고 side 가 SELL 이면 → LONG 포지션 종료
        if any(k in order_type_u for k in ("TAKE_PROFIT", "STOP")) and side_u == "SELL":
            ps = "LONG"
        # TP/SL 이고 side 가 BUY 이면 → SHORT 포지션 종료
        elif any(k in order_type_u for k in ("TAKE_PROFIT", "STOP")) and side_u == "BUY":
            ps = "SHORT"
        else:
            # 일반적인 경우에는 side 에 따라 맞춰준다
            ps = "LONG" if side_u == "BUY" else "SHORT"

    payload = {
        "symbol": symbol,
        "side": side_u,
        "positionSide": ps,
        "type": order_type_u,
        "quantity": norm_qty,
        "reduceOnly": True,
        "triggerPrice": trigger_price,
        "recvWindow": 5000,
    }
    log(f"[PLACE CONDITIONAL REQ] {payload}")
    resp = req("POST", "/openApi/swap/v2/trade/order", payload)
    log(f"[PLACE CONDITIONAL RESP] {resp}")
    return resp


def cancel_order(symbol: str, order_id: str) -> None:
    """주문 취소"""
    try:
        res = req("POST", "/openApi/swap/v2/trade/cancel", {
            "symbol": symbol,
            "orderId": order_id,
        })
        log(f"[CANCEL] order_id={order_id} resp={res}")
    except Exception as e:
        log(f"[CANCEL ERROR] {e}")


def get_order(symbol: str, order_id: str) -> Dict[str, Any]:
    """주문 상태 조회"""
    return req("GET", "/openApi/swap/v2/trade/order", {
        "symbol": symbol,
        "orderId": order_id,
    })


def get_fills(symbol: str, order_id: str) -> List[Dict[str, Any]]:
    """체결 내역 조회"""
    res = req("GET", "/openApi/swap/v2/trade/allFillOrders", {
        "symbol": symbol,
        "orderId": order_id,
        "limit": 50,
    })
    return res.get("data", []) or []


def summarize_fills(symbol: str, order_id: str) -> Optional[Dict[str, Any]]:
    """여러 체결 건을 평균가 등으로 요약"""
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
    """
    시장가 주문이 실제 FILLED 될 때까지 잠깐 폴링.
    positionSide 는 조회할 때 필요 없으므로 빼고 조회.
    """
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
    강제 시장가 청산
    - 열 때가 BUY(=LONG) 였으면 닫을 때는 SELL + positionSide=LONG
    - 열 때가 SELL(=SHORT) 였으면 닫을 때는 BUY + positionSide=SHORT
    """
    side_open_u = side_open.upper()
    if side_open_u == "BUY":
        close_side = "SELL"
        pos_side = "LONG"
    else:
        close_side = "BUY"
        pos_side = "SHORT"

    norm_qty = _normalize_qty(symbol, qty)
    payload = {
        "symbol": symbol,
        "side": close_side,
        "positionSide": pos_side,
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
