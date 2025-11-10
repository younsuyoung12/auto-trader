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

6) positionSide 강제 추가 (헤지/원웨이 공통 호환) + 109400 폴백
   - 어떤 계정/환경에서는 MARKET, 조건부 주문에 positionSide 가 없으면 109400 이 난다.
   - 반대로 어떤 계정에서는 positionSide 를 주면 109400 이 난다.
   - 그래서 기본은 positionSide 를 붙여서 보내고,
     만약 RuntimeError 안에 109400 이 보이면 같은 주문을 positionSide 없이 한 번 더 보내도록
     place_market(), place_conditional(), close_position_market() 을 보강했다.
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
# 필요 시 여기다 심볼을 더 넣어라.
# ─────────────────────────────
_QTY_STEP: Dict[str, float] = {
    "BTC-USDT": 0.001,
    "BTCUSDT": 0.001,  # 혹시 심볼 포맷이 다르게 들어오는 경우 대비
}


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
# 수량 정규화 (선물)
# ─────────────────────────────
def _normalize_qty(symbol: str, raw_qty: float) -> float:
    """
    선물 주문 수량을 거래소가 받는 단위로 내린다.
    예) 0.005904 → 0.005
    - 기본 step 은 0.001 로 둔다.
    - 0 이하로 내려가면 최소 step 을 보낸다.
    - 마지막엔 소수 3째 자리까지만 유지한다.
    """
    step = _QTY_STEP.get(symbol, 0.001)
    if step <= 0:
        step = 0.001
    units = int(raw_qty / step)
    qty = units * step
    if qty <= 0:
        qty = step
    return float(f"{qty:.3f}")


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
        # 0 / "0" / None / 100400 은 통과
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

        # {"data": {"balance": {...}}} 케이스
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

        # 리스트로 오면 첫 원소 사용
        if isinstance(data, list) and data:
            item = data[0]
        else:
            item = data

        # item 안에 balance 가 또 있는 경우
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


def get_balance_detail() -> Dict[str, Any]:
    """
    잔고 전체 구조를 그대로(혹은 최대한 평평하게) 돌려준다.
    run_bot.py 쪽에서 usedMargin 이나 freezedMargin 으로
    “사람이 이미 포지션 들고 있는지” 추정할 때 쓴다.
    """
    res = req("GET", "/openApi/swap/v2/user/balance", {})
    log(f"[BALANCE RAW for detail] {res}")

    data = res.get("data") or res.get("balances") or res

    # 가장 자주 보는 구조: {"data": {"balance": {...}}}
    if isinstance(data, dict) and "balance" in data and isinstance(data["balance"], dict):
        return data["balance"]

    # 리스트로 올 수도 있음
    if isinstance(data, list) and data:
        first = data[0]
        if isinstance(first, dict) and "balance" in first and isinstance(first["balance"], dict):
            return first["balance"]
        return first

    if isinstance(data, dict):
        return data

    return {"raw": res}


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
    """레버리지와 마진 모드를 설정한다."""
    errs: List[str] = []

    # 1) 레버리지 LONG / SHORT 각각 시도
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

    # 2) 마진 모드
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


def place_market(symbol: str, side: str, qty: float) -> Dict[str, Any]:
    """
    시장가 주문을 발행한다.
    1) 기본적으로 positionSide 를 붙여서 보낸다.
    2) 만약 이 계정이 positionSide 를 싫어해서 109400 이 나면
       positionSide 를 뺀 페이로드로 한 번 더 시도한다.
    """
    norm_qty = _normalize_qty(symbol, qty)
    position_side = "LONG" if side.upper() == "BUY" else "SHORT"
    base_payload = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": norm_qty,
        "recvWindow": 5000,
    }
    payload_with_pos = dict(base_payload, positionSide=position_side)

    log(f"[PLACE MARKET REQ] {payload_with_pos}")
    try:
        resp = req("POST", "/openApi/swap/v2/trade/order", payload_with_pos)
        log(f"[PLACE MARKET RESP] {resp}")
        return resp
    except RuntimeError as e:
        msg = str(e)
        if "109400" in msg:
            # positionSide 빼고 다시 시도
            log("[PLACE MARKET] 109400 → positionSide 제거 후 재시도")
            log(f"[PLACE MARKET REQ2] {base_payload}")
            resp = req("POST", "/openApi/swap/v2/trade/order", base_payload)
            log(f"[PLACE MARKET RESP2] {resp}")
            return resp
        raise


def place_conditional(
    symbol: str,
    side: str,
    qty: float,
    trigger_price: float,
    order_type: str,
) -> Dict[str, Any]:
    """
    TP/SL 과 같은 조건부 주문을 발행한다.
    기본은 positionSide 를 보낸다.
    109400 이면 positionSide 를 빼고 다시 보낸다.
    """
    norm_qty = _normalize_qty(symbol, qty)
    position_side = "LONG" if side.upper() == "BUY" else "SHORT"
    base_payload = {
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "quantity": norm_qty,
        "reduceOnly": True,
        "triggerPrice": trigger_price,
        "recvWindow": 5000,
    }
    payload_with_pos = dict(base_payload, positionSide=position_side)

    log(f"[PLACE CONDITIONAL REQ] {payload_with_pos}")
    try:
        resp = req("POST", "/openApi/swap/v2/trade/order", payload_with_pos)
        log(f"[PLACE CONDITIONAL RESP] {resp}")
        return resp
    except RuntimeError as e:
        msg = str(e)
        if "109400" in msg:
            log("[PLACE CONDITIONAL] 109400 → positionSide 제거 후 재시도")
            log(f"[PLACE CONDITIONAL REQ2] {base_payload}")
            resp = req("POST", "/openApi/swap/v2/trade/order", base_payload)
            log(f"[PLACE CONDITIONAL RESP2] {resp}")
            return resp
        raise


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
    """체결내역 여러 건을 요약해서 평균가/총수량/실현손익을 계산한다."""
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
    """
    TP/SL 세팅이 실패했을 때 포지션을 즉시 시장가로 닫을 때 사용.
    기본은 positionSide 를 붙여서 보내고,
    109400 이면 positionSide 를 빼고 다시 보낸다.
    """
    close_side = "SELL" if side_open == "BUY" else "BUY"
    norm_qty = _normalize_qty(symbol, qty)
    position_side = "SHORT" if side_open.upper() == "BUY" else "LONG"
    base_payload = {
        "symbol": symbol,
        "side": close_side,
        "type": "MARKET",
        "quantity": norm_qty,
        "reduceOnly": True,
        "recvWindow": 5000,
    }
    payload_with_pos = dict(base_payload, positionSide=position_side)

    log(f"[FORCE CLOSE REQ] {payload_with_pos}")
    try:
        req("POST", "/openApi/swap/v2/trade/order", payload_with_pos)
        send_tg(f"⚠️ 포지션을 즉시 시장가로 닫았습니다. 수량={norm_qty}")
    except RuntimeError as e:
        msg = str(e)
        if "109400" in msg:
            log("[FORCE CLOSE] 109400 → positionSide 제거 후 재시도")
            req("POST", "/openApi/swap/v2/trade/order", base_payload)
            send_tg(f"⚠️ 포지션을 즉시 시장가로 닫았습니다(재시도). 수량={norm_qty}")
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
