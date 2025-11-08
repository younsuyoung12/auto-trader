"""exchange_api.py
BingX REST API 호출과 관련된 저수준 함수 모음.

이 모듈이 담당하는 것:
- 쿼리 스트링 서명 (HMAC SHA256)
- 공통 요청(req)
- 레버리지/마진 설정
- 잔고 조회, 포지션 조회, 주문 조회
- 시장가/조건부 주문 생성
- 주문 상태 폴링(wait_filled)
- 체결내역 요약(summarize_fills)

원래 bot.py 안에 있던 코드를 최대한 그대로 옮겨 왔고,
settings.py, telelog.py 에 있는 유틸을 재사용하도록 했다.

주의:
- 여기서는 "OPEN_TRADES" 같은 봇 내부 상태는 다루지 않는다.
  그 부분은 run_bot.py 쪽에서 이 모듈의 함수들을 호출해서 상태를 구성해야 한다.
- 일부 BingX 엔드포인트는 계정 설정에 따라 없는 경우가 있어(code=100400),
  그런 경우는 상위에서 빈 리스트로 처리하도록 한다.
"""

from __future__ import annotations

import time
import hmac
import hashlib
from typing import Any, Dict, List, Optional, Tuple
import requests

from settings import load_settings
from telelog import log, send_tg

# 설정 읽기 (전역으로 보관)
SET = load_settings()
BASE = SET.bingx_base  # 기본: https://open-api.bingx.com


# ─────────────────────────────
# 서명/헤더 공통부
# ─────────────────────────────
def _ts_ms() -> int:
    """현재 시간을 밀리초 단위 int 로 리턴"""
    return int(time.time() * 1000)


def sign_query(params: Dict[str, Any], api_secret: str) -> str:
    """BingX 가 요구하는 방식으로 쿼리스트링에 서명한다.

    - 파라미터 키를 알파벳 순으로 정렬한 후 "k=v&..." 형태로 만든다.
    - 그 문자열에 대해 HMAC-SHA256(api_secret) 을 수행해서 signature 를 붙인다.
    - 반환값은 "k=v&...&signature=xxxx" 문자열이다.
    """
    qs = "&".join(f"{k}={params[k]}" for k in sorted(params.keys()))
    sig = hmac.new(api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
    return qs + "&signature=" + sig


def _headers() -> Dict[str, str]:
    """BingX 요청에 필요한 기본 헤더 생성"""
    return {
        "X-BX-APIKEY": SET.api_key,
        "Content-Type": "application/json",
    }


# ─────────────────────────────
# 공통 요청 함수
# ─────────────────────────────
def req(method: str, path: str, params: Optional[Dict[str, Any]] = None, body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """BingX REST 요청을 1회 수행하는 기본 함수.

    - 모든 요청에 timestamp 를 추가한다.
    - 실패하면 RuntimeError 를 던지게 해서 상위에서 처리하도록 한다.
    - status_code 가 200 이 아니면 그대로 예외로 올린다.
    """
    params = params or {}
    params["timestamp"] = _ts_ms()
    url = f"{BASE}{path}?{sign_query(params, SET.api_secret)}"
    r = requests.request(method, url, json=body, headers=_headers(), timeout=12)
    if r.status_code != 200:
        raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text}")
    return r.json()


# ─────────────────────────────
# 계좌/포지션/주문 조회
# ─────────────────────────────
def get_available_usdt() -> float:
    """가용 선물 잔고(availableBalance/availableMargin 계열)를 조회한다.

    BingX 가 종종
    {
        "data": {
            "balance": { ... }
        }
    }
    이런 식으로 중첩해서 보내므로 그 케이스를 전부 커버한다.
    실패하면 0.0 을 리턴하고 텔레그램으로 알린다.
    """
    try:
        res = req("GET", "/openApi/swap/v2/user/balance", {})
        log(f"[BALANCE RAW] {res}")

        data = res.get("data") or res.get("balances") or res

        # case 1: {"data": {"balance": {...}}}
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

        # case 2: 리스트로 오는 경우 → 첫 원소 사용
        if isinstance(data, list) and data:
            item = data[0]
        else:
            item = data

        # case 3: item 안에 다시 balance dict 가 있는 경우
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

        # 일반 케이스
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


def fetch_open_positions(symbol: str) -> List[Dict[str, Any]]:
    """실제 거래소에 열려 있는 포지션 목록을 가져온다.

    - 일부 계정/서버에서는 이 API 가 100400 코드를 주면서 "this api is not exist" 라고 한다.
      이 경우에는 빈 리스트를 리턴해서 상위에서 '동기화 불가' 로 처리하도록 한다.
    """
    try:
        res = req("GET", "/openApi/swap/v2/trade/positions", {"symbol": symbol})
        if res.get("code") == 100400:
            log("[POSITIONS] this api is not exist -> skip syncing positions")
            return []
        log(f"[POSITIONS RAW] {res}")
        data = res.get("data") or res.get("positions") or []
        if not isinstance(data, list):  # 단일 오브젝트로 오는 경우
            data = [data]
        return data
    except Exception as e:
        log(f"[POSITIONS ERROR] {e}")
        return []


def fetch_open_orders(symbol: str) -> List[Dict[str, Any]]:
    """해당 심볼에 대해 거래소에 걸려 있는 주문 목록을 가져온다."""
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
# 주문/포지션 관련 액션
# ─────────────────────────────
def set_leverage_and_mode(symbol: str, leverage: int, isolated: bool = True) -> None:
    """레버리지와 마진 모드를 설정한다.

    - 실패하더라도 봇 전체가 멈출 필요는 없으므로 예외를 상위로 올리지 않는다.
    """
    try:
        req("POST", "/openApi/swap/v2/trade/leverage", {
            "symbol": symbol,
            "leverage": leverage,
        })
        req("POST", "/openApi/swap/v2/trade/marginType", {
            "symbol": symbol,
            "marginType": "ISOLATED" if isolated else "CROSSED",
        })
    except Exception as e:
        log(f"[WARN] 레버리지/마진 설정 실패: {e}")


def place_market(symbol: str, side: str, qty: float) -> Dict[str, Any]:
    """시장가 주문을 발행한다."""
    return req("POST", "/openApi/swap/v2/trade/order", {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": qty,
    })


def place_conditional(symbol: str, side: str, qty: float, trigger_price: float, order_type: str) -> Dict[str, Any]:
    """TP/SL 과 같은 조건부 주문을 발행한다."""
    return req("POST", "/openApi/swap/v2/trade/order", {
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "quantity": qty,
        "reduceOnly": True,
        "triggerPrice": trigger_price,
    })


def cancel_order(symbol: str, order_id: str) -> None:
    """주문 ID 기준으로 주문을 취소한다.
    시장가 대기 중 타임아웃이 났을 때 사용."""
    try:
        res = req("POST", "/openApi/swap/v2/trade/cancel", {
            "symbol": symbol,
            "orderId": order_id,
        })
        log(f"[CANCEL] order_id={order_id} resp={res}")
    except Exception as e:
        log(f"[CANCEL ERROR] {e}")


def get_order(symbol: str, order_id: str) -> Dict[str, Any]:
    """주문 상태를 조회한다."""
    return req("GET", "/openApi/swap/v2/trade/order", {
        "symbol": symbol,
        "orderId": order_id,
    })


def get_fills(symbol: str, order_id: str) -> List[Dict[str, Any]]:
    """해당 주문의 체결내역을 조회한다."""
    res = req("GET", "/openApi/swap/v2/trade/allFillOrders", {
        "symbol": symbol,
        "orderId": order_id,
        "limit": 50,
    })
    return res.get("data", []) or []


def summarize_fills(symbol: str, order_id: str) -> Optional[Dict[str, Any]]:
    """체결 내역 여러 건을 한 번에 요약해서 평균가/총수량/실현손익을 계산한다.

    - TP/SL 이 체결된 뒤 run_bot 에서 텔레그램용 메시지를 만들 때 사용한다.
    - 체결이 하나도 없으면 None 리턴.
    """
    fills = get_fills(symbol, order_id)
    if not fills:
        return None
    total_qty = 0.0
    total_pnl = 0.0
    notional = 0.0
    last_time = None
    for f in fills:
        q = float(f.get("quantity") or f.get("qty") or f.get("volume") or f.get("vol") or 0.0)
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
    """시장가 주문이 실제로 FILLED 될 때까지 짧게 폴링한다.

    - 응답에 FILLED 가 안 찍혀 있으면 주문을 취소하고 None 을 리턴한다.
    - Render 같은 환경에서 너무 길게 잡으면 안 되므로 기본 5초.
    """
    end = time.time() + timeout
    last_status = None
    while time.time() < end:
        try:
            o = get_order(symbol, order_id)
            # 주문 응답 구조가 제각각이라 normalize 대신 필수값만 본다.
            d = o.get("data") or o
            status = d.get("status") or d.get("orderStatus")
            last_status = status
            if status in ("FILLED", "PARTIALLY_FILLED"):
                return d
        except Exception as e:
            log(f"[wait_filled] error: {e}")
        time.sleep(0.5)
    # 타임아웃 → 취소 시도
    log(f"[wait_filled] timeout, last_status={last_status}, cancel order")
    cancel_order(symbol, order_id)
    return None


def close_position_market(symbol: str, side_open: str, qty: float) -> None:
    """TP/SL 세팅이 실패했을 때 포지션을 즉시 시장가로 닫을 때 사용."""
    close_side = "SELL" if side_open == "BUY" else "BUY"
    try:
        req("POST", "/openApi/swap/v2/trade/order", {
            "symbol": symbol,
            "side": close_side,
            "type": "MARKET",
            "quantity": qty,
            "reduceOnly": True,
        })
        send_tg(f"⚠️ 포지션을 즉시 시장가로 닫았습니다. 수량={qty}")
    except Exception as e:
        send_tg(f"❗ 포지션 강제 정리 실패: {e}")


__all__ = [
    "req",
    "get_available_usdt",
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
