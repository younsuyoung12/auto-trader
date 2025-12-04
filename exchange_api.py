"""exchange_api.py

BingX 선물 REST 어댑터 (저수준 Transport / 조회 전용 계층)

역할
----------------------------------------------------
- BingX 선물 REST API를 래핑하는 "통신 레이어"만 담당한다.
- 이 모듈은 서명, HTTP 요청, 계좌/포지션/주문 조회, 레버리지/마진 모드 설정,
  주문 단건 조회/체결 조회/요약, 취소 등 **읽기/설정 계열**만 제공한다.
- 실제 진입/청산 주문 실행, 슬리피지 계산, TP/SL 조건부 주문 등
  "돈이 직접 오가는 실행 로직"은 order_executor.py 로 완전히 분리한다.

설계 원칙
----------------------------------------------------
1) 이 모듈 밖에서는 BingX REST 엔드포인트를 직접 호출하지 않는다.
   - 모든 HTTP 요청은 req(...) 또는 여기 정의된 helper 함수만 사용한다.
2) 주문 실행 로직(시장가/하이브리드/강제청산/TP·SL/슬리피지)은 포함하지 않는다.
   - 해당 로직은 order_executor.py 에서만 구현한다.
3) 예외 처리
   - 조회 계열(get_available_usdt, fetch_open_positions, ...)은
     실패 시 텔레그램/로그를 남기고 안전한 기본값(0.0, [], {})을 반환한다.
   - 주문 단건/체결 조회는 실패 시 예외를 던져 상위 레이어에서 판단하게 할 수 있다.

2025-12-XX 리팩터링
----------------------------------------------------
- 주문 실행/슬리피지/TP·SL/강제청산 관련 함수(open_position_with_tp_sl, place_market,
  place_conditional, set_tp_sl, close_position_market, _try_order, _normalize_qty 등)을
  모두 order_executor.py 로 이동했다.
- 이 파일은 "저수준 REST 래퍼 + 계좌/포지션/주문 조회" 수준만 남도록 단순화했다.
"""

from __future__ import annotations

import time
import hmac
import hashlib
from typing import Any, Dict, List, Optional

import requests
from urllib.parse import urlencode

from settings_ws import load_settings
from telelog import log, send_tg

# ─────────────────────────────
# 설정 읽기 (전역)
# ─────────────────────────────
SET = load_settings()
BASE = SET.bingx_base  # 예: https://open-api.bingx.com


# ─────────────────────────────
# 공통 유틸 / 서명 / 헤더
# ─────────────────────────────
def _ts_ms() -> int:
    """현재 시각(ms)."""
    return int(time.time() * 1000)


def _to_param_str(v: Any) -> str:
    """서명용 파라미터 문자열 변환."""
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def sign_query(params: Dict[str, Any], api_secret: str) -> str:
    """BingX 는 쿼리스트링 정렬 후 서명하는 방식."""
    items = [(k, _to_param_str(params[k])) for k in sorted(params.keys())]
    qs = urlencode(items, safe="-_.~")
    sig = hmac.new(api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
    return f"{qs}&signature={sig}"


def _headers() -> Dict[str, str]:
    """문서 예제와 동일: API 키만 헤더에 실어 보낸다."""
    return {
        "X-BX-APIKEY": SET.api_key,
    }


def req(
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """BingX REST 요청 공통부."""
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


# ─────────────────────────────
# 계좌/포지션/주문 조회
# ─────────────────────────────
def get_available_usdt() -> float:
    """사용 가능한 마진(USDT)을 float 로 돌려준다."""
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
        item = None
        if isinstance(data, list) and data:
            item = data[0]
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
        else:
            item = data if isinstance(data, dict) else None

        # 3) 기타 형태
        if isinstance(item, dict):
            cand = (
                item.get("availableBalance")
                or item.get("availableMargin")
                or item.get("balance")
                or 0.0
            )
            return float(cand)

        return 0.0

    except Exception as e:
        log(f"[BALANCE ERROR] {e}")
        try:
            send_tg(f"⚠️ 잔고 조회 실패: {e}")
        except Exception:
            pass
        return 0.0


def get_balance_detail() -> Dict[str, Any]:
    """텔레그램에서 상태 찍을 때 쓰는 상세 밸런스 반환."""
    res = req("GET", "/openApi/swap/v2/user/balance", {})
    log(f"[BALANCE RAW for detail] {res}")

    data = res.get("data") or res.get("balances") or res

    # 1) data: { balance: {...} }
    if isinstance(data, dict) and "balance" in data and isinstance(data["balance"], dict):
        return data["balance"]

    # 2) data: [ { balance: {...} } ]
    if isinstance(data, list) and data:
        first = data[0]
        if isinstance(first, dict) and "balance" in first and isinstance(first["balance"], dict):
            return first["balance"]
        if isinstance(first, dict):
            return first
        return {"raw": first}

    # 3) 기타 형태
    if isinstance(data, dict):
        return data

    return {"raw": res}


def fetch_open_positions(symbol: str) -> List[Dict[str, Any]]:
    """열려 있는 선물 포지션 조회."""
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
    """레버리지/마진 모드 설정."""
    # 레버리지 설정 (여러 포맷으로 재시도)
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
# 주문 조회/취소/체결
# ─────────────────────────────
def cancel_order(symbol: str, order_id: str) -> None:
    """주문 취소."""
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
    """주문 FILLED 폴링."""
    end = time.time() + timeout
    last_status = None
    while time.time() < end:
        try:
            o = get_order(symbol, order_id)
            d = o.get("data") or o
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


__all__ = [
    "sign_query",
    "req",
    "get_available_usdt",
    "get_balance_detail",
    "fetch_open_positions",
    "fetch_open_orders",
    "set_leverage_and_mode",
    "cancel_order",
    "get_order",
    "get_fills",
    "summarize_fills",
    "wait_filled",
]
