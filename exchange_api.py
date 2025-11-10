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
----------------------------------------------------
(배경)
- 어떤 계정은 헤지(양방향) 모드처럼 주문을 보내면 잘 되는데,
  어떤 계정은 원웨이(단방향) 모드라서 주문에 positionSide(LONG/SHORT)가 들어가면
  bingx code=109400 "Invalid parameters" 가 발생했다.
- 우리는 봇 코드(run_bot.py / trader.py) 쪽은 그대로 두고,
  실제로 BingX에 주문을 던지는 이 레이어에서만 "안 되면 한 번 더" 시도하게 만들면
  위쪽 코드를 건드리지 않아도 된다.

(변경 내용)
1) place_market(), place_conditional(), close_position_market() 에 폴백 로직 추가
   - 1차 시도: positionSide 포함해서 보냄
   - 만약 109400 이면: positionSide 를 빼고 똑같이 다시 보냄
   - 이렇게 하면 헤지 계정에서는 첫 번째 시도가 성공하고,
     원웨이 계정에서는 두 번째 시도가 성공해서 공통 코드로 쓸 수 있다.

2) 109400 인지 확인하는 헬퍼 _is_param_error(...) 추가
   - 에러 메시지 안에 "109400" 이나 "Invalid parameters" 가 있으면 그걸로 간주

3) 나머지 구조는 기존 2025-11-09 버전과 동일
   - 수량 0.001 단위 내림
   - 레버리지/마진 설정 실패는 경고만
   - 잔고 상세 조회 헬퍼 그대로 유지
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
    - 상위에서 109400만 따로 잡아서 폴백할 수 있게 한다.
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
        # 0 / "0" / None / 100400 은 통과
        if code not in (None, 0, "0", 100400):
            raise RuntimeError(
                f"{method} {path} -> bingx code={code}, msg={data.get('msg') or data}"
            )

    return data


def _is_param_error(exc: Exception) -> bool:
    """109400, Invalid parameters 같은 파라미터 오류인지 대충 판별"""
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

        # 리스트로 오는 구조
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

        # 마지막 일반 케이스
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
    잔고 전체 구조를 그대로 반환.
    run_bot.py 에서 usedMargin 등 읽어볼 때 사용.
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
    """열려 있는 포지션 목록 조회 (계정에 따라 안 될 수도 있음)"""
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
    레버리지/마진 모드 설정.
    일부 계정에서는 안 되므로 109400이면 경고만 남기고 넘어간다.
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
                log(f"[WARN] 레버리지({side})는 API로 설정 불가해 보여서 건너뜁니다: {msg}")
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
    1) positionSide=LONG/SHORT 포함해보고
    2) 109400 나면 positionSide 없이 다시 전송
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
        if _is_param_error(e):
            # 원웨이 계정일 가능성 → positionSide 빼고 재시도
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
        # 다른 에러면 위로 올려보냄
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
    1) positionSide 포함
    2) 109400 이면 빼고 재시도
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
    여기도 계정 모드 따라 positionSide 가 문제될 수 있으니
    109400 이면 빼고 재시도.
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
