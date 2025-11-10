"""
exchange_api.py
BingX REST API 호출과 관련된 저수준 함수 모음.

2025-11-12 수정 (7차)
----------------------------------------------------
이번 로그 기준으로 여전히 109400(Invalid parameters)이 떨어져서,
"어느 파라미터 조합을 요구하는지"가 계정단에서 확실치 않은 상태다.
그래서 주문/레버리지 설정을 한 모양으로만 보내지 말고,
여러 대표적인 모양을 순서대로 시도해서
하나라도 통과되면 그걸 쓰도록 방어 로직을 추가했다.

변경 포인트
1) req() 에 어떤 파라미터로 호출했는지 로그를 좀 더 남기게 했다.
   - 서명(signature)은 찍지 않는다.
   - 109400 난 요청은 어떤 params였는지 로그에 남겨서 나중에 바로 비교할 수 있게 했다.

2) set_leverage_and_mode(...)
   - 지금까지는 LONG, SHORT 한 번씩만 보냈는데,
     여기에 positionSide 를 쓰는 형태도 추가로 시도한다.
   - 그래도 안 되면 마지막으로 "심볼만" 보내는 최소형도 한 번 던져본다.
   - 어떤 단계에서 실패했는지는 WARN 으로 찍는다.

3) place_market(...)
   - 아래 3가지 모양을 차례대로 시도한다.
     (1) 현행 계정에서 요구했던 형태: side + positionSide(LONG/SHORT) + MARKET + quantity
     (2) side + positionSide=BOTH + MARKET + quantity
     (3) side + MARKET + quantity (positionSide 생략형)
   - quantity 는 먼저 소수(step 단위)로 보냈다가 실패하면, 정수형 문자열(계약 수량식)으로도 한 번 더 보낸다.
     → 일부 계정은 BTC-USDT 선물에서 정수 계약수만 받는 경우가 있어서 이렇게 한다.

4) place_conditional(...), close_position_market(...) 도 (1) → (2) → (3) fallback 을 동일하게 적용한다.

이렇게 해두면 어떤 모양에서 통과되는지를 로그로 바로 볼 수 있으니,
다음 단계에서 "이 계정은 이 모양으로만 보내자"로 좁혀갈 수 있다.

----------------------------------------------------
2025-11-11 수정 (6차)
(원본 설명은 아래 그대로 남겨둠)
----------------------------------------------------
(배경)
- 단방향(one-way)로 바꿨다고 했지만, 실제 계정 로그에서는
  주문 시에 여전히 `positionSide: This field is required.` 가 나왔다.
- 또 레버리지 설정에서도 `side: This field is required.` 라고 하므로,
  이 계정은 양방향/단방향과 상관없이 필드를 명시해줘야 하는 형태다.

(변경)
1) 주문 관련 함수(place_market, place_conditional, close_position_market)에
   다시 positionSide="BOTH" 를 넣었다.
2) 레버리지/마진 설정할 때도 side 를 요구하므로,
   set_leverage_and_mode(...)에서 LONG, SHORT 두 번 호출하도록 바꿨다.
3) 나머지 구조(잔고 조회, 포지션 조회, 주문 조회)는 이전 버전 그대로 둔다.
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
    # 3자리까지만
    return float(f"{qty:.3f}")


def _as_int_qty(raw_qty: float) -> str:
    """
    어떤 계정은 '계약 수량' 형식의 정수만 받기도 하므로
    실패 시에 대비해서 문자열 정수 형태도 만들어둔다.
    """
    if raw_qty <= 0:
        return "1"
    return str(int(round(raw_qty)) or 1)


def req(
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    BingX REST 요청 공통부.
    - HTTP 200이라도 data["code"] 가 0/None/100400 이 아니면 예외로 본다.
    - 요청 파라미터는 로그에 남기되, signature 는 남기지 않는다.
    """
    params = params or {}
    params["timestamp"] = _ts_ms()
    # signature 붙이기
    signed_qs = sign_query(params, SET.api_secret)
    url = f"{BASE}{path}?{signed_qs}"

    # signature 빼고 원래 params 만 찍어둔다
    log(f"[REQ] {method} {path} params={{{{ {', '.join(f'{k}: {params[k]}' for k in sorted(params.keys()) if k != 'signature')} }}}}")

    r = requests.request(method, url, json=body, headers=_headers(), timeout=12)

    if r.status_code != 200:
        raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text}")

    data = r.json()

    if isinstance(data, dict):
        code = data.get("code")
        if code not in (None, 0, "0", 100400):
            # 어떤 파라미터 때문에 막혔는지 나중에 보려고 그대로 남긴다
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
    이 계정은 뭘 넣어도 109400 을 줬으므로,
    symbol+side, symbol+positionSide, symbol-only 3단계로 시도한다.
    실패해도 봇은 계속 돌게 한다.
    """
    # 1단계: side 로 보내기
    for side in ("LONG", "SHORT"):
        try:
            req("POST", "/openApi/swap/v2/trade/leverage", {
                "symbol": symbol,
                "side": side,
                "leverage": leverage,
            })
        except Exception as e:
            log(f"[WARN] 레버리지 설정 실패({side}): {e}")

    # 2단계: positionSide 로도 한 번 던져보기
    for ps in ("LONG", "SHORT"):
        try:
            req("POST", "/openApi/swap/v2/trade/leverage", {
                "symbol": symbol,
                "positionSide": ps,
                "leverage": leverage,
            })
        except Exception as e:
            log(f"[WARN] 레버리지 설정 실패(positionSide={ps}): {e}")

    # 3단계: 심볼만
    try:
        req("POST", "/openApi/swap/v2/trade/leverage", {
            "symbol": symbol,
            "leverage": leverage,
        })
    except Exception as e:
        log(f"[WARN] 레버리지 설정 실패(심볼만): {e}")

    # 마진 모드
    try:
        req("POST", "/openApi/swap/v2/trade/marginType", {
            "symbol": symbol,
            "marginType": "ISOLATED" if isolated else "CROSSED",
        })
    except Exception as e:
        log(f"[WARN] 마진모드 설정 실패: {e}")


# ─────────────────────────────
# 주문 전송 (여러 모양으로 재시도)
# ─────────────────────────────
def _try_order(payloads: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    주어진 payload 리스트를 순서대로 /trade/order 에 던져서
    처음으로 성공하는 응답을 돌려준다.
    전부 실패하면 마지막 예외를 다시 던진다.
    """
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
            # 파라미터 에러면 다음 모양으로 계속 간다
            if not _is_param_error(e):
                break
    if last_err:
        raise last_err
    raise RuntimeError("order failed without explicit error")


def place_market(symbol: str, side: str, qty: float) -> Dict[str, Any]:
    """
    시장가 주문
    - (1) side + positionSide(LONG/SHORT) + MARKET
    - (2) side + positionSide=BOTH + MARKET
    - (3) side + MARKET
    - 실패하면 quantity 를 정수 문자열로 한 번 더 시도
    """
    norm_qty = _normalize_qty(symbol, qty)
    int_qty_str = _as_int_qty(qty)

    # 1차 시도들 (소수 수량)
    payloads = [
        {
            "symbol": symbol,
            "side": side,
            "positionSide": "LONG" if side.upper() == "BUY" else "SHORT",
            "type": "MARKET",
            "quantity": norm_qty,
            "recvWindow": 5000,
        },
        {
            "symbol": symbol,
            "side": side,
            "positionSide": "BOTH",
            "type": "MARKET",
            "quantity": norm_qty,
            "recvWindow": 5000,
        },
        {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": norm_qty,
            "recvWindow": 5000,
        },
    ]

    try:
        return _try_order(payloads)
    except Exception as e:
        # 전부 안 되면 정수 수량으로 다시 시도
        log(f"[PLACE MARKET] float qty fail -> retry with int qty: {int_qty_str}")
        payloads_int = [
            {**p, "quantity": int_qty_str} for p in payloads
        ]
        return _try_order(payloads_int)


def place_conditional(
    symbol: str,
    side: str,
    qty: float,
    trigger_price: float,
    order_type: str,
) -> Dict[str, Any]:
    """
    TP/SL 조건부 주문
    위랑 똑같이 3단계 모양으로 시도한다.
    """
    norm_qty = _normalize_qty(symbol, qty)
    int_qty_str = _as_int_qty(qty)

    payloads = [
        {
            "symbol": symbol,
            "side": side,
            "positionSide": "LONG" if side.upper() == "BUY" else "SHORT",
            "type": order_type,
            "quantity": norm_qty,
            "reduceOnly": True,
            "triggerPrice": trigger_price,
            "recvWindow": 5000,
        },
        {
            "symbol": symbol,
            "side": side,
            "positionSide": "BOTH",
            "type": order_type,
            "quantity": norm_qty,
            "reduceOnly": True,
            "triggerPrice": trigger_price,
            "recvWindow": 5000,
        },
        {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "quantity": norm_qty,
            "reduceOnly": True,
            "triggerPrice": trigger_price,
            "recvWindow": 5000,
        },
    ]
    try:
        return _try_order(payloads)
    except Exception:
        # 정수 수량으로 한 번 더
        payloads_int = [{**p, "quantity": int_qty_str} for p in payloads]
        return _try_order(payloads_int)


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
    - 위에서 만든 order fallback 을 그대로 써서 닫는다.
    """
    close_side = "SELL" if side_open.upper() == "BUY" else "BUY"
    norm_qty = _normalize_qty(symbol, qty)
    int_qty_str = _as_int_qty(qty)

    payloads = [
        {
            "symbol": symbol,
            "side": close_side,
            "positionSide": "LONG" if close_side == "BUY" else "SHORT",
            "type": "MARKET",
            "quantity": norm_qty,
            "reduceOnly": True,
            "recvWindow": 5000,
        },
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
    ]
    try:
        _try_order(payloads)
        send_tg(f"⚠️ 포지션을 즉시 시장가로 닫았습니다. 수량={norm_qty}")
    except Exception:
        # 정수로도 한 번
        payloads_int = [{**p, "quantity": int_qty_str} for p in payloads]
        try:
            _try_order(payloads_int)
            send_tg(f"⚠️ 포지션을 즉시 시장가로 닫았습니다. 수량={int_qty_str}")
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
