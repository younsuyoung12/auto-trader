"""
exchange_api.py
BingX REST API 호출과 관련된 저수준 함수 모음.

2025-11-12 수정 (7차, one-way 계정 전용 순서 보정 + 요청 포맷 정정)
+ 2025-11-12 추가 수정 ①
+ 2025-11-12 추가 수정 ② (TP/SL 다중 포맷 시도, reduceOnly 강제청산 보완)
----------------------------------------------------
- 원웨이 계정이라 positionSide=BOTH 를 먼저 보낸다.
- 이 계정은 로그상 side 가 없으면 109400 을 주므로,
  레버리지 설정은 side → positionSide → 심볼만 순으로 여러 번 시도한다.
- 주문/조회는 문서 예제처럼 "쿼리스트링 + 서명" 형태로 보낸다.
- 일부 계정에서 /trade/cancel 이 100400 을 내므로 wait_filled 에서
  타임아웃 시 무조건 cancel 을 치지 않고, cancel_order 도 100400 은 워닝만 남긴다.
- TP/SL(/trade/order 조건부) 이 110400 을 자주 내서 triggerPrice/stopPrice/
  activationPrice, positionSide 유/무, reduceOnly 유/무 를 여러 형태로 순차 시도한다.
- 강제 시장가 청산이 101290(The Reduce Only order can only decrease...) 이 나오는
  계정이 있어 reduceOnly 제거 재시도를 추가했다.
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
    "BTC-USDT": 0.001,
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
    # 문서 예제와 동일하게 API 키만 보낸다.
    return {
        "X-BX-APIKEY": SET.api_key,
    }


def _normalize_qty(symbol: str, raw_qty: float) -> float:
    step = _QTY_STEP.get(symbol, 0.001)
    if step <= 0:
        step = 0.001
    units = int(raw_qty / step)
    qty = units * step
    if qty <= 0:
        qty = step
    return float(f"{qty:.3f}")  # BingX BTC-USDT는 0.001 단위여서 3째자리까지


def _as_int_qty(raw_qty: float) -> str:
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
    - 모든 파라미터는 쿼리스트링에 넣고 서명한다.
    - 본문은 기본적으로 보내지 않는다(data=None).
    """
    params = params or {}
    params["timestamp"] = _ts_ms()
    signed_qs = sign_query(params, SET.api_secret)
    url = f"{BASE}{path}?{signed_qs}"

    # 보기 쉬운 형태로 남긴다 (signature는 로그에 안 남김)
    log_params = {k: v for k, v in params.items() if k != "signature"}
    log(f"[REQ] {method} {path} params={log_params}")

    r = requests.request(method, url, headers=_headers(), data=body or None, timeout=12)

    if r.status_code != 200:
        raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text}")

    data = r.json()

    if isinstance(data, dict):
        code = data.get("code")
        # 100400 은 "이 API 없음"이라 패스
        if code not in (None, 0, "0", 100400):
            raise RuntimeError(
                f"{method} {path} -> bingx code={code}, msg={data.get('msg') or data}"
            )

    return data


def _is_param_error(exc: Exception) -> bool:
    msg = str(exc)
    return "109400" in msg or "Invalid parameters" in msg or "110400" in msg


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
        res = req("GET", "/openApi/swap/v2/user/positions", {"symbol": symbol})
        log(f"[POSITIONS RAW user] {res}")
        data = res.get("data") or res.get("positions") or []
        if not isinstance(data, list):
            data = [data]
        return data
    except Exception as e1:
        log(f"[POSITIONS user ERROR] {e1}")

    # 백업 엔드포인트
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
    이 계정 로그에서는 'err:side: This field is required.' 가 나왔으므로
    side 있는 포맷을 우선적으로 전부 시도하고,
    그 다음 positionSide, 그 다음 최소 포맷으로 내려간다.
    실패해도 봇은 계속 돌게 한다.
    """
    try:
        req(
            "POST",
            "/openApi/swap/v2/trade/leverage",
            {
                "symbol": symbol,
                "side": "BOTH",
                "leverage": leverage,
            },
        )
        log("[LEV OK] side=BOTH")
    except Exception as e:
        log(f"[LEV FAIL] side=BOTH: {e}")
        # LONG
        try:
            req(
                "POST",
                "/openApi/swap/v2/trade/leverage",
                {
                    "symbol": symbol,
                    "side": "LONG",
                    "leverage": leverage,
                },
            )
            log("[LEV OK] side=LONG")
        except Exception as e2:
            log(f"[LEV FAIL] side=LONG: {e2}")
            # SHORT
            try:
                req(
                    "POST",
                    "/openApi/swap/v2/trade/leverage",
                    {
                        "symbol": symbol,
                        "side": "SHORT",
                        "leverage": leverage,
                    },
                )
                log("[LEV OK] side=SHORT")
            except Exception as e3:
                log(f"[LEV FAIL] side=SHORT: {e3}")
                # positionSide
                try:
                    req(
                        "POST",
                        "/openApi/swap/v2/trade/leverage",
                        {
                            "symbol": symbol,
                            "positionSide": "BOTH",
                            "leverage": leverage,
                        },
                    )
                    log("[LEV OK] positionSide=BOTH")
                except Exception as e4:
                    log(f"[LEV FAIL] positionSide=BOTH: {e4}")
                    # 최소형
                    try:
                        req(
                            "POST",
                            "/openApi/swap/v2/trade/leverage",
                            {
                                "symbol": symbol,
                                "leverage": leverage,
                            },
                        )
                        log("[LEV OK] symbol-only")
                    except Exception as e5:
                        log(f"[LEV FAIL] symbol-only: {e5}")

    # 마진 모드 설정
    try:
        req(
            "POST",
            "/openApi/swap/v2/trade/marginType",
            {
                "symbol": symbol,
                "marginType": "ISOLATED" if isolated else "CROSSED",
            },
        )
    except Exception as e:
        log(f"[WARN] 마진모드 설정 실패: {e}")


# ─────────────────────────────
# 주문 전송 (여러 모양으로 재시도)
# ─────────────────────────────
def _try_order(payloads: List[Dict[str, Any]]) -> Dict[str, Any]:
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
            # 파라미터 문제(109400/110400)일 때만 다음 포맷 시도
            if not _is_param_error(e):
                break
    if last_err:
        raise last_err
    raise RuntimeError("order failed without explicit error")


def place_market(symbol: str, side: str, qty: float) -> Dict[str, Any]:
    """
    시장가 주문
    - (1) side + positionSide=BOTH + MARKET
    - (2) side + MARKET
    - (3) side + positionSide=LONG/SHORT + MARKET
    - 실패하면 quantity 를 정수 문자열로 한 번 더 시도
    """
    norm_qty = _normalize_qty(symbol, qty)
    int_qty_str = _as_int_qty(qty)

    payloads = [
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
        {
            "symbol": symbol,
            "side": side,
            "positionSide": "LONG" if side.upper() == "BUY" else "SHORT",
            "type": "MARKET",
            "quantity": norm_qty,
            "recvWindow": 5000,
        },
    ]

    try:
        return _try_order(payloads)
    except Exception:
        log(f"[PLACE MARKET] float qty fail -> retry with int qty: {int_qty_str}")
        payloads_int = [{**p, "quantity": int_qty_str} for p in payloads]
        return _try_order(payloads_int)


def place_conditional(
    symbol: str,
    side: str,
    qty: float,
    trigger_price: float,
    order_type: str,
) -> Dict[str, Any]:
    """
    TP/SL 조건부 주문.
    이 계정은 triggerPrice / stopPrice / activationPrice / reduceOnly / positionSide
    조합에 따라 110400을 줄 수 있으므로 여러 모양으로 순차 시도한다.
    """
    norm_qty = _normalize_qty(symbol, qty)
    int_qty_str = _as_int_qty(qty)

    base_common = {
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "quantity": norm_qty,
        "recvWindow": 5000,
    }

    payloads: List[Dict[str, Any]] = [
        # 1) positionSide=BOTH + reduceOnly + triggerPrice
        {
            **base_common,
            "positionSide": "BOTH",
            "reduceOnly": True,
            "triggerPrice": trigger_price,
        },
        # 2) positionSide=BOTH + reduceOnly + stopPrice
        {
            **base_common,
            "positionSide": "BOTH",
            "reduceOnly": True,
            "stopPrice": trigger_price,
        },
        # 3) positionSide=BOTH + reduceOnly + activationPrice
        {
            **base_common,
            "positionSide": "BOTH",
            "reduceOnly": True,
            "activationPrice": trigger_price,
        },
        # 4) reduceOnly + triggerPrice
        {
            **base_common,
            "reduceOnly": True,
            "triggerPrice": trigger_price,
        },
        # 5) reduceOnly + stopPrice
        {
            **base_common,
            "reduceOnly": True,
            "stopPrice": trigger_price,
        },
        # 6) reduceOnly + activationPrice
        {
            **base_common,
            "reduceOnly": True,
            "activationPrice": trigger_price,
        },
        # 7) positionSide 없고 reduceOnly도 없는 가장 단순한 형태 (거래소가 칭얼대면 이게 먹힐 때가 있음)
        {
            **base_common,
            "triggerPrice": trigger_price,
        },
        {
            **base_common,
            "stopPrice": trigger_price,
        },
        {
            **base_common,
            "activationPrice": trigger_price,
        },
        # 10) 방향형 positionSide + reduceOnly
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
        # 실패하면 같은 것들을 정수 수량으로 한 번 더
        payloads_int = [{**p, "quantity": int_qty_str} for p in payloads]
        return _try_order(payloads_int)


def cancel_order(symbol: str, order_id: str) -> None:
    """
    일부 계정에서는 이 API 자체가 없어서 100400 을 주므로,
    그 경우에는 에러로 안 보고 워닝만 남긴다.
    """
    try:
        res = req(
            "POST",
            "/openApi/swap/v2/trade/cancel",
            {
                "symbol": symbol,
                "orderId": order_id,
            },
        )
        log(f"[CANCEL] order_id={order_id} resp={res}")
    except Exception as e:
        msg = str(e)
        if "100400" in msg or "this api is not exist" in msg:
            log(f"[CANCEL WARN] cancel api not available on this account: {e}")
        else:
            log(f"[CANCEL ERROR] {e}")


def get_order(symbol: str, order_id: str) -> Dict[str, Any]:
    return req(
        "GET",
        "/openApi/swap/v2/trade/order",
        {
            "symbol": symbol,
            "orderId": order_id,
        },
    )


def get_fills(symbol: str, order_id: str) -> List[Dict[str, Any]]:
    res = req(
        "GET",
        "/openApi/swap/v2/trade/allFillOrders",
        {
            "symbol": symbol,
            "orderId": order_id,
            "limit": 50,
        },
    )
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
    """
    주문이 FILLED 됐는지 기다린다.
    - BingX 가 data 안에 order 키를 한 번 더 싸서 보낼 수 있으므로 그 경우도 펼친다.
    - 타임아웃 되어도 이 계정은 cancel api 가 없을 수 있으니 무조건 cancel 안 친다.
    """
    end = time.time() + timeout
    last_status = None
    while time.time() < end:
        try:
            o = get_order(symbol, order_id)
            d = o.get("data") or o

            # some responses: {"code":0,"data":{"order":{...}}}
            if isinstance(d, dict) and "order" in d and isinstance(d["order"], dict):
                d = d["order"]

            status = (d.get("status") or d.get("orderStatus") or "").upper()
            last_status = status
            if status in ("FILLED", "PARTIALLY_FILLED"):
                return d
        except Exception as e:
            log(f"[wait_filled] error: {e}")
        time.sleep(0.5)
    log(f"[wait_filled] timeout, last_status={last_status}, skip cancel (api may not exist)")
    # 이 계정은 cancel 이 100400 을 주므로 여기서는 시도하지 않는다.
    return None


def close_position_market(symbol: str, side_open: str, qty: float) -> None:
    """
    시장가로 포지션을 닫는다.
    - 기본은 reduceOnly=True 로 닫고
    - 101290 이 나오면 reduceOnly 없이 다시 시도한다.
    """
    close_side = "SELL" if side_open.upper() == "BUY" else "BUY"
    norm_qty = _normalize_qty(symbol, qty)
    int_qty_str = _as_int_qty(qty)

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
        # 네 로그에 나온 케이스: reduceOnly 로는 닫을 포지션이 없다 → reduceOnly 빼고 다시
        if "101290" in msg:
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
                send_tg(f"❗ 포지션 강제 정리 실패(RO제거도 실패): {e2}")
                return

        # 그 외에는 예전처럼 정수 수량으로 한 번 더
        payloads_int = [{**p, "quantity": int_qty_str} for p in payloads]
        try:
            _try_order(payloads_int)
            send_tg(f"⚠️ 포지션을 즉시 시장가로 닫았습니다. 수량={int_qty_str}")
        except Exception as e3:
            send_tg(f"❗ 포지션 강제 정리 실패: {e3}")


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
