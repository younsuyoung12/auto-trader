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

2025-11-11 수정 (6차)  ← 지금 버전
----------------------------------------------------
(배경)
- 잔고/포지션 같은 GET 은 잘 되는데, 주문/레버리지/마진 같은 POST 가 전부
  109400 Invalid parameters 로 떨어졌다.
- 우리가 보내는 POST 에는 실제 body 가 없는데도 Content-Type: application/json
  을 항상 붙이고 있었고, BingX 예제는 이걸 붙이지 않는다.
- 이게 이 계정/엔드포인트에서 파라미터 전체를 Invalid 로 보는 원인으로
  판단된다.

(변경)
- _headers(...) 를 "바디가 있을 때만" Content-Type 을 붙이는 형태로 바꿨다.
- req(...) 에서 body 가 None 이면 Content-Type 을 안 넣는다.
- 나머지 로직(단방향 주문, positionSide 제거 등)은 2025-11-11 버전 그대로 유지.

2025-11-11 수정 (5차)
----------------------------------------------------
- 앱에서 포지션 모드를 Hedge → One-way 로 바꾼 환경에 맞춰
  주문 전송 시 positionSide 를 전부 제거했다.
- 주문을 한 번만 심플하게 보내도록 했다.

2025-11-10 수정 (3~4차)
----------------------------------------------------
- 포지션 조회를 /user/positions → /trade/positions 순으로 폴백하도록 유지.
- 계정에 따라 positionSide 를 넣어야 하는 케이스가 있어서 한 번 넣었으나,
  지금은 단방향 기준이라 제거됨.

2025-11-10 이전
----------------------------------------------------
- 공통 요청, 시그니처, 주문, 체결조회 기본 뼈대.
----------------------------------------------------
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


def _headers(has_body: bool = False) -> Dict[str, str]:
    """
    BingX 필수 헤더.
    - 기본으로는 X-BX-APIKEY 만 넣는다.
    - 실제 JSON 바디를 보낼 때만 Content-Type 을 붙인다.
      (이걸 항상 넣어두면 파라미터 전체를 Invalid 로 보는 계정이 있었다.)
    """
    h = {
        "X-BX-APIKEY": SET.api_key,
    }
    if has_body:
        h["Content-Type"] = "application/json"
    return h


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
    - 2025-11-11: body 가 있을 때만 Content-Type 을 넣는다.
    """
    params = params or {}
    params["timestamp"] = _ts_ms()
    url = f"{BASE}{path}?{sign_query(params, SET.api_secret)}"

    has_body = body is not None
    r = requests.request(
        method,
        url,
        json=body if has_body else None,
        headers=_headers(has_body),
        timeout=12,
    )

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
    단방향 기준 레버리지/마진 설정.
    일부 계정에서는 여전히 이게 안 될 수 있으므로, 실패해도 에러만 남기고 진행한다.
    """
    # 레버리지
    try:
        req("POST", "/openApi/swap/v2/trade/leverage", {
            "symbol": symbol,
            "leverage": leverage,
        })
    except Exception as e:
        log(f"[WARN] 레버리지 설정 실패(단방향): {e}")

    # 마진 모드
    try:
        req("POST", "/openApi/swap/v2/trade/marginType", {
            "symbol": symbol,
            "marginType": "ISOLATED" if isolated else "CROSSED",
        })
    except Exception as e:
        log(f"[WARN] 마진모드 설정 실패: {e}")


# ─────────────────────────────
# 주문 전송 (단방향 전용)
# ─────────────────────────────
def place_market(symbol: str, side: str, qty: float) -> Dict[str, Any]:
    """
    시장가 주문 (단방향)
    - positionSide 전혀 안 보냄
    - 한 번만 시도
    """
    norm_qty = _normalize_qty(symbol, qty)
    payload = {
        "symbol": symbol,
        "side": side,
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
) -> Dict[str, Any]:
    """
    TP/SL 조건부 주문 (단방향)
    - positionSide 전혀 안 보냄
    - reduceOnly 만 붙임
    """
    norm_qty = _normalize_qty(symbol, qty)
    payload = {
        "symbol": symbol,
        "side": side,
        "type": order_type,
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
    단방향이므로 따로 positionSide 를 보낼 필요는 없다.
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
    강제 시장가 청산 (단방향)
    - 그냥 반대 side 로 MARKET 한 번만 보낸다.
    """
    close_side = "SELL" if side_open.upper() == "BUY" else "BUY"
    norm_qty = _normalize_qty(symbol, qty)
    payload = {
        "symbol": symbol,
        "side": close_side,
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
