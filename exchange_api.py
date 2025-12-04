"""exchange_api.py

BingX 선물 REST 어댑터 (주문/계좌/포지션 단일 게이트웨이)

역할
----------------------------------------------------
- BingX 선물 REST API를 래핑하는 단일 진입/청산 계층.
- run_bot_ws → entry_flow.try_open_new_position(...) → open_position_with_tp_sl(...) → place_market(...) 흐름으로
  실제 진입 주문과 TP/SL 조건부 주문을 전송한다.
- position_watch_ws.maybe_exit_with_gpt(...) → close_position_market(...) 흐름으로
  열린 포지션을 시장가로 강제 청산한다.
- trader.Trade / TraderState 와 함께 현재 포지션 상태를 추적하고,
  fetch_open_positions(...) / fetch_open_orders(...) 등으로 거래소 상태를 동기화한다.

설계 원칙
----------------------------------------------------
1) "실제 돈이 움직이는 유일한 계층"
   - 이 모듈 밖에서는 requests, 서명, BingX 엔드포인트를 직접 호출하지 않는다.
   - 모든 주문/계좌 조회는 반드시 여기 정의된 public 함수만 사용한다.

2) 수량 정규화
   - 선물 수량은 심볼별 step(_QTY_STEP) 기준으로 _normalize_qty(...) 에서 항상 내림 처리한다.
   - 최소 1 step 이상으로 강제해 '0.00099999' 같은 부동소수점 꼬리를 제거한다.

3) 서명/요청 일관성
   - sign_query(...) 에서 urlencode + HMAC-SHA256 으로 쿼리스트링을 서명한다.
   - req(...) 는 timestamp 를 자동으로 붙이고, BingX code != 0 인 케이스를 RuntimeError 로 올린다
     (100400 같이 "API 미지원" 코드는 워닝만 남기고 통과).

4) 주문 재시도 정책
   - _try_order(...) 는 여러 payload 변형을 순차 시도하면서,
     파라미터 오류(_is_param_error)로 판단되는 경우에만 다음 포맷으로 재시도한다.
   - 재시도 사이 0.2초 백오프를 두어 레이트리밋/연쇄 오류를 완화한다.

5) 예외 처리 방침
   - 계좌/포지션 조회 계열(get_available_usdt, fetch_open_positions, ...)은
     실패 시 텔레그램/로그를 남기고 안전한 기본값(0.0, [])을 반환한다.
   - 주문/TP·SL 설정 계열(open_position_with_tp_sl, place_market, place_conditional, close_position_market)은
     실패 시 예외를 로깅하고 상위 레이어(entry_flow / position_watch_ws)가
     "trade is None" 또는 False 등의 신호로 후속 처리할 수 있도록 설계한다.

2025-12-01 정리/수정 사항
----------------------------------------------------
- open_position_with_tp_sl 이 존재하지 않는 open_order_market 에 의존하던 문제를 수정.
  · 이제 place_market(...) 을 직접 호출해 진입 시장가 주문을 전송한다.
  · 체결 정보가 있으면 avgPrice/price 를 사용하고, 없으면 entry_flow 가 넘겨준 entry_price_hint 를 사용한다.
- set_tp_sl(...) 를 재구현하여 place_conditional(...) 를 통해
  포지션 방향과 반대 side 의 reduceOnly 조건부 TP/SL 주문을 1회씩 전송한다.
- exchange_api 모듈 내부에서 자기 자신을 import 하던 레거시 코드
  (from exchange_api import open_order_market, set_tp_sl)를 제거해 순환 의존성을 없앴다.
- __all__ 에 open_position_with_tp_sl, set_tp_sl 를 추가하여 공개 API 를 명시적으로 정리했다.

2025-12-02 슬리피지 방지 기능 (틱 기반) 추가
----------------------------------------------------
- open_position_with_tp_sl(...) 진입 직후, entry_price_hint 대비 실제 체결가(entry_price)의
  슬리피지를 "틱 수" 기준으로 계산해 하드 가드로 사용한다.
  · tick_size = SET.price_tick_size (없으면 기본 0.1 USD / BTC-USDT) 를 사용해 diff_ticks = |fill-exp| / tick_size 계산.
  · 설정값 SET.max_entry_slippage_ticks (기본 20틱)을 초과하면 텔레그램 경고를 보낸다.
  · SET.auto_close_on_heavy_slippage 가 True 이면 close_position_market(...) 을 즉시 호출해
    방금 연 포지션을 강제 청산하고, open_position_with_tp_sl(...) 은 Trade 대신 None 을 반환한다.
- 퍼센트 기반 슬리피지 대신 "선물 시장의 틱 단위"를 기준으로 가드를 적용한다.
"""
from __future__ import annotations

import time
import hmac
import hashlib
import math
from typing import Any, Dict, List, Optional
import requests
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from trader import Trade

from settings_ws import load_settings
from telelog import log, send_tg

# ─────────────────────────────
# 설정 읽기 (전역)
# ─────────────────────────────
SET = load_settings()
BASE = SET.bingx_base  # 예: https://open-api.bingx.com

# ─────────────────────────────
# 심볼별 수량 step / tick (선물 전용)
# ─────────────────────────────
_QTY_STEP: Dict[str, float] = {
    "BTC-USDT": 0.001,
}

_PRICE_TICK: Dict[str, float] = {
    "BTC-USDT": 0.1,  # BingX BTC-USDT 선물 최소 가격 단위
}

# ─────────────────────────────
# 공통 유틸
# ─────────────────────────────
from decimal import Decimal, ROUND_DOWN, getcontext
from urllib.parse import urlencode

getcontext().prec = 28  # 수량/서명 계산 안전 여유


def _ts_ms() -> int:
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


def _normalize_qty(symbol: str, raw_qty: float) -> float:
    """심볼별 최소 수량단위에 맞춰서 내림 정규화."""
    step = Decimal(str(_QTY_STEP.get(symbol, 0.001) or 0.001))
    q = Decimal(str(raw_qty if raw_qty is not None else 0))
    units = (q / step).to_integral_value(rounding=ROUND_DOWN)
    qty = (units * step) if units > 0 else step
    return float(qty.quantize(step, rounding=ROUND_DOWN))


def _get_tick_size(symbol: str) -> float:
    """심볼별 가격 tick size 를 반환한다. 설정값이 있으면 우선 사용."""
    try:
        s_val = getattr(SET, "price_tick_size", None)
        if isinstance(s_val, (int, float)) and s_val > 0:
            return float(s_val)
    except Exception:
        pass
    return float(_PRICE_TICK.get(symbol, 0.1))


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


def _is_param_error(exc: Exception) -> bool:
    """주문 포맷을 바꿔서 다시 시도할지 판단."""
    msg = str(exc).lower()
    needles = [
        "109400",
        "110400",
        "invalid parameters",
        "parameter invalid",
        "parameters invalid",
        "parameter error",
        "parameters error",
        "param error",
        "illegal parameter",
    ]
    return any(n in msg for n in needles)


# 슬리피지 하드 가드 (틱 기반)
# ─────────────────────────────
def _check_entry_slippage_and_maybe_close(
    *,
    symbol: str,
    side_req: str,
    qty: float,
    expected_price: float,
    fill_price: float,
) -> bool:
    """진입 직후 슬리피지를 검사하고 경고만 보내며 절대 자동 청산하지 않는다."""

    # markPrice 기반 기대가격 계산
    try:
        from market_data_ws import get_orderbook
        ob = get_orderbook(symbol, limit=5)
        mark = ob.get("markPrice")
        if mark:
            exp = float(mark)   # markPrice 사용
        else:
            exp = float(expected_price)
        fill = float(fill_price)
    except Exception:
        exp = float(expected_price)
        fill = float(fill_price)

    if exp <= 0 or fill <= 0:
        return True

    diff_abs = abs(fill - exp)
    tick_size = _get_tick_size(symbol)
    if tick_size <= 0:
        return True

    diff_ticks = diff_abs / tick_size
    max_ticks = float(getattr(SET, "max_entry_slippage_ticks", 20) or 0.0)

    # 슬리피지 초과 시 경고만 보내고 절대 자동청산하지 않음
    if diff_ticks > max_ticks:
        pct_100 = (diff_abs / exp) * 100.0
        msg = (
            "⚠️ 진입 슬리피지 경고 (자동청산 없음)\n"
            f"- 심볼: {symbol}\n"
            f"- 방향: {side_req}\n"
            f"- markPrice 기준 기대가: {exp:.2f}\n"
            f"- 실제 체결가: {fill:.2f}\n"
            f"- 차이: {diff_abs:.2f} ≒ {diff_ticks:.1f} ticks (tick={tick_size})\n"
            f"- 비율: {pct_100:.3f}%\n"
            f"- 허용 한도: {max_ticks:.1f} ticks\n"
        )
        log(msg)
        try:
            send_tg(msg)
        except:
            pass

    # 자동 청산 절대 없음 → 항상 True
    return True
# ─────────────────────────────
# 계좌/포지션/주문 조회 + 진입/청산
# ─────────────────────────────
def open_position_with_tp_sl(
    settings: Any,
    symbol: str,
    side_open: str,
    qty: float,
    entry_price_hint: float,
    tp_pct: float,
    sl_pct: float,
    source: str,
    soft_mode: bool = False,
    sl_floor_ratio: Optional[float] = None,
) -> Optional["Trade"]:
    """
    진입 + TP/SL 설정 엔트리 포인트.
    """
    from trader import Trade  # 지연 import 로 순환 의존성 방지

    side_up = str(side_open).upper()
    if side_up not in ("BUY", "SELL", "LONG", "SHORT"):
        log(f"[OPEN_POS] invalid side_open={side_open!r}")
        return None

    # BUY/SELL 로 정규화
    if side_up in ("LONG", "BUY"):
        side_req = "BUY"
    else:
        side_req = "SELL"

    # 1) 시장가 주문 실행
    try:
        order_resp = place_market(symbol=symbol, side=side_req, qty=qty)
    except Exception as e:
        log(f"[OPEN_POS] place_market exception: {e}")
        try:
            send_tg(
                f"❗ 진입 주문 실패: {symbol} side={side_req} qty={qty} err={e}"
            )
        except Exception:
            pass
        return None

    if not isinstance(order_resp, dict):
        log(f"[OPEN_POS] unexpected order response type: {type(order_resp)}")
        return None

    # 1-1) 체결 평균가 추출 (없으면 entry_price_hint 사용)
    raw = order_resp.get("data") or order_resp
    entry_price_val: Optional[float] = None
    if isinstance(raw, dict):
        for key in ("avgPrice", "price", "avg_price", "fillPrice", "executedPrice"):
            v = raw.get(key)
            if v is None:
                continue
            try:
                entry_price_val = float(v)
                break
            except (TypeError, ValueError):
                continue

    if entry_price_val is None or not math.isfinite(entry_price_val) or entry_price_val <= 0:
        entry_price_val = float(entry_price_hint or 0.0)

    if entry_price_val <= 0:
        log(
            f"[OPEN_POS] invalid entry_price (resp/hint invalid) "
            f"symbol={symbol} side={side_req}"
        )
        return None

    entry_price = entry_price_val

    # 1-2) 슬리피지 하드 가드 적용 (틱 기반)
    try:
        if entry_price_hint and entry_price_hint > 0:
            ok = _check_entry_slippage_and_maybe_close(
                symbol=symbol,
                side_req=side_req,
                qty=qty,
                expected_price=float(entry_price_hint),
                fill_price=entry_price,
            )
            if not ok:
                log(
                    f"[OPEN_POS] heavy slippage detected "
                    f"(expected={entry_price_hint}, fill={entry_price}) → abort Trade"
                )
                return None
    except Exception as e:
        log(f"[OPEN_POS] slippage guard check failed: {e}")

    # 2) TP/SL 가격 계산
    if side_req == "BUY":
        tp_price = entry_price * (1 + tp_pct)
        sl_price = entry_price * (1 - sl_pct)
    else:
        tp_price = entry_price * (1 - tp_pct)
        sl_price = entry_price * (1 + sl_pct)

    # 2-1) SL 바닥 비율(sl_floor_ratio) 적용 (선택적)
    if isinstance(sl_floor_ratio, (int, float)) and sl_floor_ratio > 0:
        try:
            if side_req == "BUY":
                floor_price = entry_price * (1 - float(sl_floor_ratio))
                if sl_price > floor_price:
                    sl_price = floor_price
            else:
                floor_price = entry_price * (1 + float(sl_floor_ratio))
                if sl_price < floor_price:
                    sl_price = floor_price
        except Exception as e:
            log(f"[OPEN_POS] sl_floor_ratio adjust failed: {e}")

    # 3) TP/SL 조건부 주문 설정 (실패하더라도 진입 자체는 유지)
    try:
        set_tp_sl(
            symbol=symbol,
            side_open=side_req,
            qty=qty,
            tp_price=tp_price,
            sl_price=sl_price,
            soft_mode=soft_mode,
            sl_floor_ratio=sl_floor_ratio,
        )
    except Exception as e:
        log(f"[OPEN_POS] set_tp_sl error: {e}")
        try:
            send_tg(
                f"⚠️ TP/SL 설정 실패: {symbol} side={side_req} "
                f"tp={tp_price:.2f}, sl={sl_price:.2f}, err={e}"
            )
        except Exception:
            pass

    # 4) Trade 객체 생성
    try:
        leverage = float(getattr(settings, "leverage", 10.0) or 0.0)
    except Exception:
        leverage = 0.0

    trade = Trade(
        symbol=symbol,
        side=side_req,
        qty=qty,
        entry_price=entry_price,
        leverage=leverage,
        source=source,
        tp_price=tp_price,
        sl_price=sl_price,
    )
    return trade


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
    """여러 형태의 payload 를 순서대로 보내서 재시도."""
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
            time.sleep(0.2)  # 백오프
    if last_err:
        raise last_err
    raise RuntimeError("order failed without explicit error")


def place_market(symbol: str, side: str, qty: float) -> Dict[str, Any]:
    """시장가 주문."""
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
    """TP/SL 조건부 주문."""
    norm_qty = _normalize_qty(symbol, qty)

    log(
        f"[PLACE COND] symbol={symbol} side={side} qty={qty}(norm={norm_qty}) "
        f"trigger={trigger_price} type={order_type}"
    )

    working_type = getattr(SET, "conditional_working_type", "MARK_PRICE")

    base_common = {
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "quantity": norm_qty,
        "recvWindow": 5000,
        "workingType": working_type,
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


def set_tp_sl(
    *,
    symbol: str,
    side_open: str,
    qty: float,
    tp_price: float,
    sl_price: float,
    soft_mode: bool = False,
    sl_floor_ratio: Optional[float] = None,
) -> None:
    """BingX 조건부 주문을 이용해 TP/SL 설정."""
    side_up = str(side_open).upper()
    if side_up in ("LONG", "BUY"):
        open_side = "BUY"
    elif side_up in ("SHORT", "SELL"):
        open_side = "SELL"
    else:
        log(f"[SET_TP_SL] invalid side_open={side_open!r}")
        return

    if tp_price <= 0 and sl_price <= 0:
        log(f"[SET_TP_SL] both tp/sl <= 0 → skip (symbol={symbol})")
        return

    close_side = "SELL" if open_side == "BUY" else "BUY"

    # TP
    if tp_price and tp_price > 0:
        try:
            place_conditional(
                symbol=symbol,
                side=close_side,
                qty=qty,
                trigger_price=float(tp_price),
                order_type="TAKE_PROFIT_MARKET",
            )
        except Exception as e:
            log(
                f"[SET_TP_SL] TP 조건부 주문 실패 "
                f"symbol={symbol} side_open={open_side} close_side={close_side} "
                f"tp_price={tp_price}: {e}"
            )

    if soft_mode:
        return

    # SL
    if sl_price and sl_price > 0:
        try:
            place_conditional(
                symbol=symbol,
                side=close_side,
                qty=qty,
                trigger_price=float(sl_price),
                order_type="STOP_MARKET",
            )
        except Exception as e:
            log(
                f"[SET_TP_SL] SL 조건부 주문 실패 "
                f"symbol={symbol} side_open={open_side} close_side={close_side} "
                f"sl_price={sl_price}: {e}"
            )


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


def close_position_market(symbol: str, side_open: str, qty: float) -> None:
    """포지션을 시장가로 강제 청산."""
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
    "open_position_with_tp_sl",
    "place_market",
    "place_conditional",
    "set_tp_sl",
    "cancel_order",
    "get_order",
    "get_fills",
    "summarize_fills",
    "wait_filled",
    "close_position_market",
    "sign_query",
]
