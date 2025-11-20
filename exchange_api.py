"""
# exchange_api.py

수정 내용 (2025-11-13)
1) _as_int_qty(...) 제거 → 심볼별 최소 수량 step 으로 재시도하도록 변경 (요구 반영)
2) 시장가/조건부/강제청산 재시도 시에도 심볼 step 기반 최소 수량을 사용하도록 통일 (요구 반영)
3) 나머지 로직과 로그 포맷은 유지 (요구 반영)
4) [PATCH] 수량 정규화에 Decimal 기반 내림 적용 → 부동소수점 오차 제거, step 정밀도 보존
5) [PATCH] sign_query 를 urlencode 기반으로 변경, bool 소문자화 → 서명/전송 문자열 일치 강화
6) [PATCH] _is_param_error 메시지 패턴 확장 → 109400/110400 외 변형 문구 방어
7) [PATCH] _try_order 재시도 사이 소폭 백오프(0.2s) 추가 → 레이트리밋/연쇄 오류 완화
8) [PATCH] place_conditional 에 workingType 명시(기본 MARK_PRICE, settings 로 변경 가능)
"""
from __future__ import annotations

import time
import hmac
import hashlib
from typing import Any, Dict, List, Optional
import requests

from settings_ws import load_settings
from telelog import log, send_tg

# ─────────────────────────────
# 설정 읽기 (전역)
# ─────────────────────────────
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
from decimal import Decimal, ROUND_DOWN, getcontext
from urllib.parse import urlencode
getcontext().prec = 28  # [PATCH] 수량/서명 계산 안전 여유
def open_position_with_tp_sl(
    settings,
    symbol: str,
    side_open: str,
    qty: float,
    entry_price_hint: float,
    tp_pct: float,
    sl_pct: float,
    source: str,
    soft_mode: bool = False,
    sl_floor_ratio: float = None,
):
    """
    run_bot_ws → entry_flow → 여기서 호출됨.
    주문 실행 + TP/SL 설정까지 처리하는 핵심 함수.
    """

    from telelog import log, send_tg
    from trader import Trade
    from exchange_api import open_order_market, set_tp_sl

    try:
        # 1) 시장가 주문 실행
        order = open_order_market(
            symbol=symbol,
            side=side_open,
            qty=qty,
        )
        if not order:
            log("[OPEN_POS] open_order_market failed")
            return None

        entry_price = float(order.get("avgPrice") or entry_price_hint or 0)

        # 2) TP/SL 가격 계산
        if side_open == "BUY":
            tp_price = entry_price * (1 + tp_pct)
            sl_price = entry_price * (1 - sl_pct)
        else:
            tp_price = entry_price * (1 - tp_pct)
            sl_price = entry_price * (1 + sl_pct)

        # 3) TP/SL 설정
        set_tp_sl(
            symbol=symbol,
            side=side_open,
            tp_price=tp_price,
            sl_price=sl_price,
        )

        # 4) Trade 객체 반환
        trade = Trade(
            symbol=symbol,
            side=side_open,
            qty=qty,
            entry_price=entry_price,
            leverage=float(getattr(settings, "leverage", 10)),
            source=source,
            tp_price=tp_price,
            sl_price=sl_price,
        )

        return trade

    except Exception as e:
        log(f"[OPEN_POS] exception: {e}")
        return None


def _ts_ms() -> int:
    return int(time.time() * 1000)


# [PATCH] 서명/전송 문자열 일치: urlencode + bool 소문자화
def _to_param_str(v: Any) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def sign_query(params: Dict[str, Any], api_secret: str) -> str:
    """BingX 는 쿼리스트링 정렬 후 서명하는 방식.
    - [PATCH] urlencode 로 인코딩/정렬을 표준화, bool 은 소문자화
    - 서버가 서명 계산에 사용하는 문자열과 동일하게 맞춘다.
    """
    items = [(k, _to_param_str(params[k])) for k in sorted(params.keys())]
    qs = urlencode(items, safe="-_.~")
    sig = hmac.new(api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
    return f"{qs}&signature={sig}"


def _headers() -> Dict[str, str]:
    """문서 예제와 동일: API 키만 헤더에 실어 보낸다."""
    return {
        "X-BX-APIKEY": SET.api_key,
    }


# [PATCH] Decimal 기반 정규화: step 배수로 내림, 최소 step 보장
def _normalize_qty(symbol: str, raw_qty: float) -> float:
    """심볼별 최소 수량단위에 맞춰서 내림 정규화.
    BTC-USDT 는 0.001 단위. 재시도 시에도 이 값을 그대로 쓴다.
    부동소수점 오차를 피하기 위해 Decimal 사용.
    """
    step = Decimal(str(_QTY_STEP.get(symbol, 0.001) or 0.001))
    q = Decimal(str(raw_qty if raw_qty is not None else 0))
    units = (q / step).to_integral_value(rounding=ROUND_DOWN)
    qty = (units * step) if units > 0 else step
    # step 의 지수에 맞춰 고정 소수 처리
    return float(qty.quantize(step, rounding=ROUND_DOWN))


def req(
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """BingX REST 요청 공통부.
    - 파라미터는 전부 쿼리스트링에 넣고 서명해서 보낸다.
    - 일부 API 는 없는 계정에서 100400 을 주는데, 그건 예외로 안 터뜨리고 그냥 리턴.
    """
    params = params or {}
    params["timestamp"] = _ts_ms()
    signed_qs = sign_query(params, SET.api_secret)
    url = f"{BASE}{path}?{signed_qs}"

    # signature 빼고 로그 남기기
    log_params = {k: v for k, v in params.items() if k != "signature"}
    log(f"[REQ] {method} {path} params={log_params}")

    r = requests.request(method, url, headers=_headers(), data=body or None, timeout=12)

    if r.status_code != 200:
        raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text}")

    data = r.json()

    if isinstance(data, dict):
        code = data.get("code")
        # 100400: 이 API 없음 → 워닝 수준으로만 본다
        if code not in (None, 0, "0", 100400):
            raise RuntimeError(
                f"{method} {path} -> bingx code={code}, msg={data.get('msg') or data}"
            )

    return data


# [PATCH] 파라미터 오류 패턴 확장
def _is_param_error(exc: Exception) -> bool:
    """주문 포맷을 바꿔서 다시 시도할지 판단할 때 사용.
    대표 코드/문구의 다양한 변형을 포괄.
    """
    msg = str(exc).lower()
    needles = [
        "109400", "110400",
        "invalid parameters", "parameter invalid", "parameters invalid",
        "parameter error", "parameters error", "param error", "illegal parameter",
    ]
    return any(n in msg for n in needles)


# ─────────────────────────────
# 계좌/포지션/주문 조회
# ─────────────────────────────

def get_available_usdt() -> float:
    """사용 가능한 마진(USDT)을 float 로 돌려준다.
    여러 형태의 응답을 커버하도록 되어 있음.
    """
    try:
        res = req("GET", "/openApi/swap/v2/user/balance", {})
        log(f"[BALANCE RAW] {res}")

        data = res.get("data") or res.get("balances") or res

        # 1) data: { balance: { ... } }
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

        # 2) data: [ { balance: {...} } ]
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

        # 3) 기타 형태
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
    """텔레그램에서 상태 찍을 때 쓰는 상세 밸런스 반환."""
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
    """열려 있는 선물 포지션 조회.
    - 2025-11-12: timestamp is invalid 을 피하려고 recvWindow=5000 추가.
    - 메인 엔드포인트가 안 되면 백업(/trade/positions)도 시도하되, 그 API 가 없으면 그냥 빈 리스트.
    """
    try:
        res = req(
            "GET",
            "/openApi/swap/v2/user/positions",
            {
                "symbol": symbol,
                "recvWindow": 5000,
            },
        )
        log(f"[POSITIONS RAW user] {res}")
        data = res.get("data") or res.get("positions") or []
        if not isinstance(data, list):
            data = [data]
        return data
    except Exception as e1:
        log(f"[POSITIONS user ERROR] {e1}")

    # 백업 엔드포인트
    try:
        res = req(
            "GET",
            "/openApi/swap/v2/trade/positions",
            {
                "symbol": symbol,
                "recvWindow": 5000,
            },
        )
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
    """심볼별 열려 있는 주문 목록."""
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
    """이 계정은 side 없으면 에러를 내는 로그가 있어서
    side → (LONG/SHORT) → positionSide → 최소형 순으로 시도한다.
    실패해도 봇은 계속 돌 수 있게 예외는 내부에서만 처리.
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
    """여러 형태의 payload 를 순서대로 보내서
    파라미터 에러가 아닌 게 터지면 바로 멈추고,
    파라미터 에러면 다음 포맷을 시도한다.
    [PATCH] 재시도 사이 0.2s 백오프 추가.
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
            if not _is_param_error(e):
                break
            time.sleep(0.2)  # [PATCH] 백오프
    if last_err:
        raise last_err
    raise RuntimeError("order failed without explicit error")


def place_market(symbol: str, side: str, qty: float) -> Dict[str, Any]:
    """시장가 주문
    1) side + positionSide=BOTH
    2) side 만
    3) side + 방향 positionSide
    안 되면 동일 포맷으로 심볼 최소수량으로 재시도.
    """
    norm_qty = _normalize_qty(symbol, qty)

    log(f"[PLACE MARKET] symbol={symbol} side={side} qty_req={qty} qty_norm={norm_qty}")

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
        min_qty = _normalize_qty(symbol, 0)
        log(f"[PLACE MARKET] norm qty fail -> retry with min step qty: {min_qty}")
        payloads_min = [{**p, "quantity": min_qty} for p in payloads]
        return _try_order(payloads_min)


def place_conditional(
    symbol: str,
    side: str,
    qty: float,
    trigger_price: float,
    order_type: str,
) -> Dict[str, Any]:
    """TP/SL 조건부 주문.
    triggerPrice / stopPrice / activationPrice, reduceOnly, positionSide 조합을
    여러 개 보내서 110400 을 피한다.
    [PATCH] workingType 명시(SET.conditional_working_type 기본값 사용)
    """
    norm_qty = _normalize_qty(symbol, qty)

    log(
        f"[PLACE COND] symbol={symbol} side={side} qty={qty}(norm={norm_qty}) "
        f"trigger={trigger_price} type={order_type}"
    )

    working_type = getattr(SET, "conditional_working_type", "MARK_PRICE")  # [PATCH]

    base_common = {
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "quantity": norm_qty,
        "recvWindow": 5000,
        "workingType": working_type,  # [PATCH]
    }

    payloads: List[Dict[str, Any]] = [
        {**base_common, "positionSide": "BOTH", "reduceOnly": True, "triggerPrice": trigger_price},
        {**base_common, "positionSide": "BOTH", "reduceOnly": True, "stopPrice": trigger_price},
        {**base_common, "positionSide": "BOTH", "reduceOnly": True, "activationPrice": trigger_price},
        {**base_common, "reduceOnly": True, "triggerPrice": trigger_price},
        {**base_common, "reduceOnly": True, "stopPrice": trigger_price},
        {**base_common, "reduceOnly": True, "activationPrice": trigger_price},
        {**base_common, "triggerPrice": trigger_price},
        {**base_common, "stopPrice": trigger_price},
        {**base_common, "activationPrice": trigger_price},
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
        min_qty = _normalize_qty(symbol, 0)
        payloads_min = [{**p, "quantity": min_qty} for p in payloads]
        return _try_order(payloads_min)


def cancel_order(symbol: str, order_id: str) -> None:
    """일부 계정에서는 이 API 자체가 없어서 100400 을 주므로,
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
    """주문 단건 조회."""
    return req(
        "GET",
        "/openApi/swap/v2/trade/order",
        {
            "symbol": symbol,
            "orderId": order_id,
        },
    )


def get_fills(symbol: str, order_id: str) -> List[Dict[str, Any]]:
    """체결 내역 조회."""
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
    """여러 체결 건을 합쳐서 평균가/수량/PnL 을 구한다."""
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
    """주문이 FILLED 됐는지 최대 timeout 초까지 폴링.
    일부 계정은 cancel 이 안 되므로 타임아웃돼도 cancel 은 시도하지 않는다.
    """
    end = time.time() + timeout
    last_status = None
    while time.time() < end:
        try:
            o = get_order(symbol, order_id)
            d = o.get("data") or o

            # {"code":0,"data":{"order":{...}}} 형태 보정
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
    return None


def close_position_market(symbol: str, side_open: str, qty: float) -> None:
    """포지션을 시장가로 강제 청산.
    1) reduceOnly=True 로 닫아보고
    2) 101290 이면 reduceOnly 없이 다시 닫는다.
    3) 그래도 안 되면 심볼 최소수량으로 다시 닫는다.
    """
    close_side = "SELL" if side_open.upper() == "BUY" else "BUY"
    norm_qty = _normalize_qty(symbol, qty)

    log(
        f"[FORCE CLOSE] symbol={symbol} open_side={side_open} close_side={close_side} "
        f"qty_req={qty} qty_norm={norm_qty}"
    )

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
        log(f"[FORCE CLOSE ERR] {msg}")
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
                log(f"[FORCE CLOSE ERR NO-RO] {e2}")
                send_tg(f"❗ 포지션 강제 정리 실패(RO제거도 실패): {e2}")
                return

        # 기타 케이스는 심볼 최소 수량으로 재시도
        min_qty = _normalize_qty(symbol, 0)
        payloads_min = [{**p, "quantity": min_qty} for p in payloads]
        try:
            _try_order(payloads_min)
            send_tg(f"⚠️ 포지션을 즉시 시장가로 닫았습니다. 수량={min_qty}")
        except Exception as e3:
            log(f"[FORCE CLOSE ERR MIN] {e3}")
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
