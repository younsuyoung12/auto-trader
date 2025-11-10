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

2025-11-10 수정 (4차)
----------------------------------------------------
(핵심 원인)
- BingX 공식 예제는 파라미터를 전부 쿼리스트링에 넣고,
  POST 바디는 비워서 보낸다.
- 우리는 바디가 없어도 Content-Type: application/json 헤더를
  강제로 달아서 보내고 있었기 때문에, 서버가 이를 파라미터 오류
  (code=109400)로 처리했을 가능성이 크다.

(해결)
- req() 에서 POST를 보낼 때 body가 없으면 Content-Type 을 붙이지 않는다.
- body가 있을 때만 json 으로 보낸다.
- 나머지 주문 로직(최소 필드 → positionSide → BOTH)과
  포지션 2단계 조회(user → trade)는 3차 수정 사항을 그대로 유지한다.

2025-11-10 수정 (3차)
----------------------------------------------------
(배경)
- MARKET 주문을 positionSide 포함/미포함/BOTH로 모두 던져도
  code=109400 이 나온 계정이 있었다.
- 계정 모드/필드 수에 민감한 것으로 보고,
  가장 보수적으로 ① 최소 필드만 → ② 그래도 안 되면 positionSide →
  ③ 그래도 안 되면 BOTH 순서로 바꾼다.
- 조건부 주문(place_conditional)과 강제청산(close_position_market)도
  같은 패턴으로 단순화한다.
- 포지션 조회도 /user/positions 를 먼저 쓰고, 안 되면 /trade/positions 로
  폴백하도록 했다.

2025-11-10 수정 (2차)
----------------------------------------------------
- 응답에서 orderType 이 오는 것을 보고, 요청에도 orderType 을 같이 보내도록
  했던 버전. (현재는 최소 필드 우선이므로 필요시만 추가)

2025-11-10 수정 (1차)
----------------------------------------------------
- place_market(), place_conditional(), close_position_market() 에서
  positionSide 넣었다가 109400이면 빼고 다시 보내는 폴백을 넣었다.
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
    "BTC-USDT": 0.001,  # 현재 봇에서 사용할 심볼
}


# ─────────────────────────────
# 공통 유틸
# ─────────────────────────────
def _ts_ms() -> int:
    """현재 ms 타임스탬프 반환"""
    return int(time.time() * 1000)


def sign_query(params: Dict[str, Any], api_secret: str) -> str:
    """
    파라미터를 key 기준으로 정렬해 쿼리스트링을 만든 뒤
    HMAC-SHA256 으로 서명해서 signature=... 를 붙여준다.
    """
    qs = "&".join(f"{k}={params[k]}" for k in sorted(params.keys()))
    sig = hmac.new(api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
    return qs + "&signature=" + sig


def _normalize_qty(symbol: str, raw_qty: float) -> float:
    """
    선물 수량을 거래소 최소단위로 내림.
    예) step=0.001, raw=0.005904 → 0.005 → 0.005 으로 고정
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
    BingX REST 공통 요청.
    - 파라미터는 전부 쿼리스트링에 올리고 서명한다.
    - POST 인데 body 가 없으면 Content-Type 을 붙이지 않는다
      (공식 예제와 동일한 형태).
    - 응답 code 가 0/None/100400 이 아니면 예외로 올린다.
    """
    params = params or {}
    params["timestamp"] = _ts_ms()
    url = f"{BASE}{path}?{sign_query(params, SET.api_secret)}"

    # 필수 헤더: 키만 먼저
    headers = {
        "X-BX-APIKEY": SET.api_key,
    }

    # 메서드에 따라 전송
    if method.upper() == "POST":
        if body is None:
            # 바디 없이 보내는 공식 예제 패턴
            resp = requests.post(url, headers=headers, data="", timeout=12)
        else:
            # 바디가 있을 때만 JSON 으로
            headers["Content-Type"] = "application/json"
            resp = requests.post(url, headers=headers, json=body, timeout=12)
    else:
        resp = requests.request(method, url, headers=headers, timeout=12)

    if resp.status_code != 200:
        raise RuntimeError(f"{method} {path} -> {resp.status_code}: {resp.text}")

    data = resp.json()
    if isinstance(data, dict):
        code = data.get("code")
        # 0 / "0" / None / 100400 은 통과
        if code not in (None, 0, "0", 100400):
            raise RuntimeError(
                f"{method} {path} -> bingx code={code}, msg={data.get('msg') or data}"
            )
    return data


def _is_param_error(exc: Exception) -> bool:
    """109400 / Invalid parameters 문자열이 포함된 예외인지 판별"""
    msg = str(exc)
    return "109400" in msg or "Invalid parameters" in msg


# ─────────────────────────────
# 계좌/포지션/주문 조회
# ─────────────────────────────
def get_available_usdt() -> float:
    """
    사용 가능한 USDT(availableMargin 등)를 최대한 회수해서 float 으로 리턴.
    구조가 계정마다 조금 달라서 케이스별로 풀어줌.
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

        # case 2: list 로 온 경우
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

        # fallback
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
    """
    잔고 구조 전체를 그대로 돌려준다.
    run_bot.py / trader.py 쪽에서 usedMargin 같은 필드를 직접 읽을 때 사용.
    """
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
    일부 계정은 trade/positions 가 막혀 있으므로 user/positions 먼저 시도.
    """
    # 1차: user/positions
    try:
        res = req("GET", "/openApi/swap/v2/user/positions", {"symbol": symbol})
        log(f"[POSITIONS RAW user] {res}")
        data = res.get("data") or res.get("positions") or []
        if not isinstance(data, list):
            data = [data]
        return data
    except Exception as e1:
        log(f"[POSITIONS user ERROR] {e1}")

    # 2차: trade/positions
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
    """열려 있는 주문 목록 조회"""
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
# 레버리지/마진 설정
# ─────────────────────────────
def set_leverage_and_mode(symbol: str, leverage: int, isolated: bool = True) -> None:
    """
    레버리지/마진모드 설정.
    일부 계정은 이 API 자체가 막혀 있어 109400 을 내므로 그때는 경고만.
    """
    errs: List[str] = []

    # LONG/SHORT 각각 시도
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
                log(f"[WARN] 레버리지({side})는 API로 설정 불가해서 건너뜀: {msg}")
            else:
                errs.append(f"{side}: {msg}")

    # 마진 모드
    try:
        req("POST", "/openApi/swap/v2/trade/marginType", {
            "symbol": symbol,
            "marginType": "ISOLATED" if isolated else "CROSSED",
        })
    except Exception as e:
        msg = str(e)
        if "109400" in msg:
            log(f"[WARN] 마진모드는 API로 설정 불가해서 건너뜀: {msg}")
        else:
            errs.append(f"MARGIN: {msg}")

    if errs:
        log(f"[WARN] 레버리지/마진 설정 일부 실패: {', '.join(errs)}")


# ─────────────────────────────
# 주문 전송 (시장가/조건부) - 109400 폴백 포함
# ─────────────────────────────
def place_market(symbol: str, side: str, qty: float) -> Dict[str, Any]:
    """
    시장가 주문.
    1) 최소 필드로 먼저 던진다.
    2) 109400 이면 positionSide=LONG/SHORT 으로 한 번 더.
    3) 그래도 109400 이면 positionSide=BOTH 로 마지막으로.
    """
    norm_qty = _normalize_qty(symbol, qty)

    # 1차: 최소 필드
    payload1 = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": norm_qty,
        "recvWindow": 5000,
    }
    log(f"[PLACE MARKET REQ 1] {payload1}")
    try:
        resp = req("POST", "/openApi/swap/v2/trade/order", payload1)
        log(f"[PLACE MARKET RESP 1] {resp}")
        return resp
    except Exception as e1:
        if not _is_param_error(e1):
            raise
        log(f"[PLACE MARKET] 1차 파라미터 오류 → positionSide 재시도: {e1}")

    # 2차: 헷지/양방향 모드 고려
    pos_side = "LONG" if side.upper() == "BUY" else "SHORT"
    payload2 = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": norm_qty,
        "recvWindow": 5000,
        "positionSide": pos_side,
    }
    log(f"[PLACE MARKET REQ 2] {payload2}")
    try:
        resp = req("POST", "/openApi/swap/v2/trade/order", payload2)
        log(f"[PLACE MARKET RESP 2] {resp}")
        return resp
    except Exception as e2:
        if not _is_param_error(e2):
            raise
        log(f"[PLACE MARKET] 2차 파라미터 오류 → BOTH 재시도: {e2}")

    # 3차: BOTH
    payload3 = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": norm_qty,
        "recvWindow": 5000,
        "positionSide": "BOTH",
    }
    log(f"[PLACE MARKET REQ 3] {payload3}")
    resp = req("POST", "/openApi/swap/v2/trade/order", payload3)
    log(f"[PLACE MARKET RESP 3] {resp}")
    return resp


def place_conditional(
    symbol: str,
    side: str,
    qty: float,
    trigger_price: float,
    order_type: str,
) -> Dict[str, Any]:
    """
    TP/SL 조건부 주문.
    시장가와 동일한 패턴으로 1→2→3 단계로 보낸다.
    """
    norm_qty = _normalize_qty(symbol, qty)

    # 1차: 최소 필드
    payload1 = {
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "quantity": norm_qty,
        "reduceOnly": True,
        "triggerPrice": trigger_price,
        "recvWindow": 5000,
    }
    log(f"[PLACE CONDITIONAL REQ 1] {payload1}")
    try:
        resp = req("POST", "/openApi/swap/v2/trade/order", payload1)
        log(f"[PLACE CONDITIONAL RESP 1] {resp}")
        return resp
    except Exception as e1:
        if not _is_param_error(e1):
            raise
        log(f"[PLACE CONDITIONAL] 1차 파라미터 오류 → positionSide 재시도: {e1}")

    # 2차: positionSide 포함
    pos_side = "LONG" if side.upper() == "BUY" else "SHORT"
    payload2 = {
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "quantity": norm_qty,
        "reduceOnly": True,
        "triggerPrice": trigger_price,
        "recvWindow": 5000,
        "positionSide": pos_side,
    }
    log(f"[PLACE CONDITIONAL REQ 2] {payload2}")
    try:
        resp = req("POST", "/openApi/swap/v2/trade/order", payload2)
        log(f"[PLACE CONDITIONAL RESP 2] {resp}")
        return resp
    except Exception as e2:
        if not _is_param_error(e2):
            raise
        log(f"[PLACE CONDITIONAL] 2차 파라미터 오류 → BOTH 재시도: {e2}")

    # 3차: BOTH
    payload3 = {
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "quantity": norm_qty,
        "reduceOnly": True,
        "triggerPrice": trigger_price,
        "recvWindow": 5000,
        "positionSide": "BOTH",
    }
    log(f"[PLACE CONDITIONAL REQ 3] {payload3}")
    resp = req("POST", "/openApi/swap/v2/trade/order", payload3)
    log(f"[PLACE CONDITIONAL RESP 3] {resp}")
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
    """시장가 주문이 실제 FILLED 될 때까지 잠깐 폴링"""
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
    강제 시장가 청산.
    - 1차: 최소 필드
    - 2차: positionSide 붙여서
    - 3차: BOTH
    """
    close_side = "SELL" if side_open == "BUY" else "BUY"
    norm_qty = _normalize_qty(symbol, qty)

    # 1차
    payload1 = {
        "symbol": symbol,
        "side": close_side,
        "type": "MARKET",
        "quantity": norm_qty,
        "reduceOnly": True,
        "recvWindow": 5000,
    }
    log(f"[FORCE CLOSE REQ 1] {payload1}")
    try:
        req("POST", "/openApi/swap/v2/trade/order", payload1)
        send_tg(f"⚠️ 포지션을 즉시 시장가로 닫았습니다. 수량={norm_qty}")
        return
    except Exception as e1:
        if not _is_param_error(e1):
            send_tg(f"❗ 포지션 강제 정리 실패: {e1}")
            return
        log(f"[FORCE CLOSE] 1차 파라미터 오류 → positionSide 재시도: {e1}")

    # 2차
    position_side = "SHORT" if side_open.upper() == "BUY" else "LONG"
    payload2 = {
        "symbol": symbol,
        "side": close_side,
        "type": "MARKET",
        "quantity": norm_qty,
        "reduceOnly": True,
        "recvWindow": 5000,
        "positionSide": position_side,
    }
    log(f"[FORCE CLOSE REQ 2] {payload2}")
    try:
        req("POST", "/openApi/swap/v2/trade/order", payload2)
        send_tg(f"⚠️ 포지션을 즉시 시장가로 닫았습니다. 수량={norm_qty} (fallback)")
        return
    except Exception as e2:
        if not _is_param_error(e2):
            send_tg(f"❗ 포지션 강제 정리 실패: {e2}")
            return
        log(f"[FORCE CLOSE] 2차도 파라미터 오류 → BOTH 재시도: {e2}")

    # 3차
    payload3 = {
        "symbol": symbol,
        "side": close_side,
        "type": "MARKET",
        "quantity": norm_qty,
        "reduceOnly": True,
        "recvWindow": 5000,
        "positionSide": "BOTH",
    }
    log(f"[FORCE CLOSE REQ 3] {payload3}")
    req("POST", "/openApi/swap/v2/trade/order", payload3)
    send_tg(f"⚠️ 포지션을 즉시 시장가로 닫았습니다. 수량={norm_qty} (fallback BOTH)")


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
