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

2025-11-09 수정/추가 내용
1) 응답 검증 강화
   - BingX가 HTTP 200을 줘도 JSON 안에 code가 0/None/100400 이 아니면 실패로 보도록 req()를 보강했다.
   - 단, 일부 엔드포인트가 주는 100400은 기존처럼 예외를 던지지 않고 그대로 돌려줘서
     상위(run_bot.py 등)에서 “이 API는 계정에서 미지원”으로 처리할 수 있게 했다.

2) 주문 응답 로그 추가
   - 시장가 주문(place_market)과 조건부 주문(place_conditional)을 넣을 때
     BingX가 돌려준 원본 응답을 log()로 남기도록 했다.
     → 텔레그램에 "시장가 진입 응답에 orderId가 없어 포지션을 건너뜁니다." 가 찍히는 원인을
       서버 로그에서 바로 확인할 수 있게 하기 위함.

3) 레버리지 설정 파라미터 보완 (중요)
   - 일부 계정에서 /openApi/swap/v2/trade/leverage 가 symbol + leverage 만으로 109400(Invalid parameters)를 내서
     side 를 명시적으로 LONG, SHORT 두 번 호출하도록 변경했다.
   - 이렇게 하면 롱/숏 양쪽 레버리지가 동시에 설정되므로 run_bot.py 에서 한 번만 호출해도 된다.
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
# 서명/헤더 공통부
# ─────────────────────────────
def _ts_ms() -> int:
    """현재 시간을 밀리초 단위 int 로 리턴"""
    return int(time.time() * 1000)


def sign_query(params: Dict[str, Any], api_secret: str) -> str:
    """BingX 가 요구하는 방식으로 쿼리스트링에 서명한다."""
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
def req(
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """BingX REST 요청을 1회 수행하는 기본 함수."""
    params = params or {}
    params["timestamp"] = _ts_ms()
    url = f"{BASE}{path}?{sign_query(params, SET.api_secret)}"
    r = requests.request(method, url, json=body, headers=_headers(), timeout=12)

    if r.status_code != 200:
        raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text}")

    data = r.json()

    if isinstance(data, dict):
        code = data.get("code")
        # code 가 0, "0", None, 100400 이면 통과
        if code not in (None, 0, "0", 100400):
            raise RuntimeError(
                f"{method} {path} -> bingx code={code}, msg={data.get('msg') or data}"
            )

    return data


# ─────────────────────────────
# 계좌/포지션/주문 조회
# ─────────────────────────────
def get_available_usdt() -> float:
    """가용 선물 잔고(availableBalance/availableMargin 계열)를 조회한다."""
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
    """실제 거래소에 열려 있는 포지션 목록을 가져온다."""
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

    여기서는 BingX 문서대로 side 를 LONG, SHORT 두 번 호출한다.
    하나라도 성공하면 그냥 넘어가고, 둘 다 실패하면 경고만 찍는다.
    """
    errors: List[str] = []

    # 1) LONG 레버리지
    try:
        req("POST", "/openApi/swap/v2/trade/leverage", {
            "symbol": symbol,
            "leverage": leverage,
            "side": "LONG",
        })
    except Exception as e:
        errors.append(f"LONG: {e}")

    # 2) SHORT 레버리지
    try:
        req("POST", "/openApi/swap/v2/trade/leverage", {
            "symbol": symbol,
            "leverage": leverage,
            "side": "SHORT",
        })
    except Exception as e:
        errors.append(f"SHORT: {e}")

    # 3) 마진 모드
    try:
        req("POST", "/openApi/swap/v2/trade/marginType", {
            "symbol": symbol,
            "marginType": "ISOLATED" if isolated else "CROSSED",
        })
    except Exception as e:
        errors.append(f"MARGIN: {e}")

    if errors:
        # run_bot.py 에서 멈추지 않도록 여기서만 경고
        log(f"[WARN] 레버리지/마진 설정 일부 실패: {', '.join(errors)}")


def place_market(symbol: str, side: str, qty: float) -> Dict[str, Any]:
    """시장가 주문을 발행한다."""
    resp = req("POST", "/openApi/swap/v2/trade/order", {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": qty,
    })
    log(f"[PLACE MARKET] {resp}")
    return resp


def place_conditional(
    symbol: str,
    side: str,
    qty: float,
    trigger_price: float,
    order_type: str,
) -> Dict[str, Any]:
    """TP/SL 과 같은 조건부 주문을 발행한다."""
    resp = req("POST", "/openApi/swap/v2/trade/order", {
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "quantity": qty,
        "reduceOnly": True,
        "triggerPrice": trigger_price,
    })
    log(f"[PLACE CONDITIONAL] {resp}")
    return resp


def cancel_order(symbol: str, order_id: str) -> None:
    """주문 ID 기준으로 주문을 취소한다."""
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
    """체결 내역 여러 건을 한 번에 요약해서 평균가/총수량/실현손익을 계산한다."""
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
    """시장가 주문이 실제로 FILLED 될 때까지 짧게 폴링한다."""
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
