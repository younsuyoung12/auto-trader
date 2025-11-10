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

2025-11-12 수정 (6차)  ← 이번 수정
----------------------------------------------------
(109400 방지/호환성 강화)
1) 서명/전송 프로토콜을 BingX swap v2 권장 방식으로 통일
   - 모든 요청에 timestamp/recvWindow 공통 부착(없으면 기본값 채움).
   - 서명은 URL-인코딩된 정렬 파라미터(qs)에 대해 HMAC-SHA256.
   - GET: URL = BASE + path + "?" + signed_qs
   - POST/DELETE: 헤더 Content-Type=application/x-www-form-urlencoded,
     바디에 signed_qs 그대로 전송(data=...).
   - 헤더는 X-BX-APIKEY 사용.
2) 수량/가격 문자열 포맷 강화
   - 수량: 심볼별 step에 맞춰 내림 정규화 후 "0.005" 같은 고정 소수 문자열.
   - 가격(triggerPrice 등): "105000.12" 같은 소수 문자열(지수표기 방지).
3) 에러 메시지 강화
   - BingX code/msg를 최대한 그대로 노출하여 원인 분석 용이.
4) 레버리지/마진 설정도 동일한 서명/전송 경로 사용.

(주의)
- 단방향(one-way) 기준으로 positionSide 관련 필드는 어디에도 넣지 않음.

2025-11-11 수정 (5차)
----------------------------------------------------
- 주문 관련 함수(place_market, place_conditional, close_position_market)에서
  positionSide 관련 필드 제거 (단방향 전제).
- 레버리지/마진 설정 실패 시에도 진행 (로그만).

2025-11-10 수정 (4차)
----------------------------------------------------
- (양방향 폴백 로직은 현재 단방향이라 제거됨)

2025-11-10 수정 (3차)
----------------------------------------------------
- 포지션 조회 /user/positions → /trade/positions 폴백 유지

2025-11-10 수정 (1~2차)
----------------------------------------------------
- (과거 positionSide 폴백 제거됨)
----------------------------------------------------
"""

from __future__ import annotations

import time
import hmac
import hashlib
import urllib.parse
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


def _headers(is_post_like: bool) -> Dict[str, str]:
    """
    BingX 필수 헤더:
    - API 키는 X-BX-APIKEY
    - POST/DELETE 는 x-www-form-urlencoded 로 보냄
    """
    h = {"X-BX-APIKEY": SET.api_key}
    if is_post_like:
        h["Content-Type"] = "application/x-www-form-urlencoded"
    return h


def _urlencode_sorted(params: Dict[str, Any]) -> str:
    """
    BingX 서명용: key 정렬 후 URL 인코딩된 쿼리스트링 생성.
    값은 문자열로 변환하여 지수표기 방지.
    """
    items = []
    for k in sorted(params.keys()):
        v = params[k]
        # 값 포맷 강제(지수표기 방지)
        if isinstance(v, float):
            # 과도한 자리수 방지: 일반적으로 거래 인수는 2~8자리면 충분
            v = f"{v:.10f}".rstrip("0").rstrip(".")
        else:
            v = str(v)
        items.append((k, v))
    return urllib.parse.urlencode(items, quote_via=urllib.parse.quote)


def _sign(params: Dict[str, Any], api_secret: str) -> str:
    """
    URL-인코딩된 정렬 파라미터 문자열에 대해 HMAC-SHA256 서명 생성 후
    signature 파라미터를 붙인 최종 쿼리스트링 반환.
    """
    qs = _urlencode_sorted(params)
    sig = hmac.new(api_secret.encode("utf-8"), qs.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{qs}&signature={sig}"


def _ensure_common(params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """timestamp/recvWindow 기본값 채워넣기"""
    p = dict(params or {})
    p.setdefault("timestamp", _ts_ms())
    p.setdefault("recvWindow", 5000)
    return p


def req(
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,  # 유지: 인터페이스 호환용(미사용)
) -> Dict[str, Any]:
    """
    BingX REST 요청 공통부.
    - GET: URL에 서명 쿼리
    - POST/DELETE: body=x-www-form-urlencoded 로 서명 쿼리 전송
    - HTTP 200이라도 code != 0/None/100400 은 예외로 본다.
    """
    method = method.upper()
    is_post_like = method in ("POST", "DELETE")
    p = _ensure_common(params)

    signed_qs = _sign(p, SET.api_secret)
    url = f"{BASE}{path}"

    if method == "GET":
        url = f"{url}?{signed_qs}"
        r = requests.get(url, headers=_headers(False), timeout=12)
    elif method == "DELETE":
        r = requests.delete(url, headers=_headers(True), data=signed_qs, timeout=12)
    else:  # POST (기타 메서드는 현재 사용 안 함)
        r = requests.post(url, headers=_headers(True), data=signed_qs, timeout=12)

    # HTTP 에러
    if r.status_code != 200:
        raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text}")

    data = r.json() if r.content else {}

    # BingX code 판단
    if isinstance(data, dict):
        code = data.get("code")
        if code not in (None, 0, "0", 100400):
            msg = data.get("msg") or data
            raise RuntimeError(f"{method} {path} -> bingx code={code}, msg={msg}")
    return data


def _normalize_qty(symbol: str, raw_qty: float) -> str:
    """
    선물 수량을 거래소가 받는 최소 단위로 내린다.
    예: step=0.001, raw=0.005904 → 0.005
    문자열로 반환하여 지수표기 방지.
    """
    step = _QTY_STEP.get(symbol, 0.001)
    if step <= 0:
        step = 0.001
    units = int(raw_qty / step)
    qty = units * step
    if qty <= 0:
        qty = step
    # step 이 0.001 기준이라 소수 3자리가 안전
    return f"{qty:.3f}".rstrip("0").rstrip(".") or "0"


def _fmt_price(p: float, decimals: int = 2) -> str:
    """
    가격 문자열 포맷(지수표기 방지). 기본 2자리.
    심볼에 따라 tick size 다르면 나중에 per-symbol 로 조정 가능.
    """
    return f"{p:.{decimals}f}".rstrip("0").rstrip(".") or "0"


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
    (이 부분은 단방향/양방향과 무관하니까 그대로 둔다.)
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
            "leverage": str(leverage),
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
    qty_str = _normalize_qty(symbol, qty)
    payload = {
        "symbol": symbol,
        "side": side,          # "BUY" or "SELL"
        "type": "MARKET",
        "quantity": qty_str,
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
    qty_str = _normalize_qty(symbol, qty)
    price_str = _fmt_price(trigger_price, decimals=2)
    payload = {
        "symbol": symbol,
        "side": side,
        "type": order_type,        # "TAKE_PROFIT_MARKET" / "STOP_MARKET"
        "quantity": qty_str,
        "reduceOnly": True,
        "triggerPrice": price_str,
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
    qty_str = _normalize_qty(symbol, qty)
    payload = {
        "symbol": symbol,
        "side": close_side,
        "type": "MARKET",
        "quantity": qty_str,
        "reduceOnly": True,
    }
    log(f"[FORCE CLOSE REQ] {payload}")
    try:
        req("POST", "/openApi/swap/v2/trade/order", payload)
        send_tg(f"⚠️ 포지션을 즉시 시장가로 닫았습니다. 수량={qty_str}")
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
    "sign_query",  # (구호환용) 내부적으로는 _sign 사용
]
