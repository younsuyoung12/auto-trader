# bot.py
# BingX Auto Trader (Render Background Worker)
# 적용 내용 (90점대 버전):
# - 3분봉 EMA20/50 + RSI 신호
# - 3분봉 신호가 나와도 15분봉 방향과 같을 때만 진입 (MTF 필터)
# - RSI 다이버전스가 신호와 반대면 진입 안 함
# - 횡보(이평이 붙거나 캔들 안 움직이면) 진입 안 함
# - 옵션: 박스장(레인지) 전략 ON/OFF
# - 진입 시 TP/SL 예약 (고정 % 또는 ATR 기반)
# - 포지션 1개만 운용, 청산 전에는 재진입 안 함
# - 3회 연속 손실이면 1시간 휴식
# - 잔고 없으면 진입 안 함
# - 시작 시 거래소 포지션/열린 주문 동기화
# - TP/SL 중 하나만 사라지면 역방향 주문 자동 재설정
# - 진입 주문 체결 타임아웃 시 강제 취소 API 호출
# - 거래소 응답 스키마 파싱을 공통 함수로 분리
# - 간단한 health/metrics HTTP 서버
# - 설정값 검증(RISK_PCT > 1 등) 추가
# - 텔레그램 알림 한국어
# - Render SIGTERM 시 STOP 알림 생략

import os, time, hmac, hashlib, math, signal, random, datetime, threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, List, Tuple, Optional
import requests

# ─────────────────────────────
# 시작 / 종료 제어
# ─────────────────────────────
START_TS = time.time()
MIN_UPTIME_FOR_STOP = int(os.getenv("MIN_UPTIME_FOR_STOP", "5"))
TERMINATED_BY_SIGNAL = False

# ─────────────────────────────
# 환경변수
# ─────────────────────────────
API_KEY    = os.getenv("BINGX_API_KEY", "")
API_SECRET = os.getenv("BINGX_API_SECRET", "")

SYMBOL   = os.getenv("SYMBOL", "BTC-USDT")
INTERVAL = os.getenv("INTERVAL", "3m")      # 메인 타임프레임

# 전략 ON/OFF
ENABLE_TREND = os.getenv("ENABLE_TREND", "1") == "1"   # 3m→15m 추세 전략
ENABLE_RANGE = os.getenv("ENABLE_RANGE", "0") == "1"   # 박스(레인지) 전략
ENABLE_1M_CONFIRM = os.getenv("ENABLE_1M_CONFIRM", "1") == "1"  # 1분봉 컨펌

LEVERAGE = int(os.getenv("LEVERAGE", "10"))
ISOLATED = os.getenv("ISOLATED", "1") == "1"

# 리스크 비율: 가용 선물잔고의 몇 %만 한 번에 쓸지
RISK_PCT = float(os.getenv("RISK_PCT", "0.3"))  # 30% 기본

# 고정 TP/SL
TP_PCT = float(os.getenv("TP_PCT", "0.02"))   # +2%
SL_PCT = float(os.getenv("SL_PCT", "0.02"))   # -2%

# ATR 기반 TP/SL 옵션
USE_ATR       = os.getenv("USE_ATR", "1") == "1"
ATR_LEN       = int(os.getenv("ATR_LEN", "20"))
ATR_TP_MULT   = float(os.getenv("ATR_TP_MULT", "2.0"))
ATR_SL_MULT   = float(os.getenv("ATR_SL_MULT", "1.2"))
MIN_TP_PCT    = float(os.getenv("MIN_TP_PCT", "0.005"))   # 0.5%
MIN_SL_PCT    = float(os.getenv("MIN_SL_PCT", "0.005"))   # 0.5%

COOLDOWN_SEC         = int(os.getenv("COOLDOWN_SEC", "15"))
COOLDOWN_AFTER_CLOSE = int(os.getenv("COOLDOWN_AFTER_CLOSE", "30"))
COOLDOWN_AFTER_3LOSS = int(os.getenv("COOLDOWN_AFTER_3LOSS", "3600"))
POLL_FILLS_SEC       = int(os.getenv("POLL_FILLS_SEC", "2"))

MIN_NOTIONAL_USDT = float(os.getenv("MIN_NOTIONAL_USDT", "5"))
MAX_NOTIONAL_USDT = float(os.getenv("MAX_NOTIONAL_USDT", "999999"))

# 가격이 갑자기 튀었는데 그걸 시장가로 따라가면 손해가 커지니까
MAX_PRICE_JUMP_PCT = float(os.getenv("MAX_PRICE_JUMP_PCT", "0.003"))  # 0.3% 기본

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
BASE = "https://open-api.bingx.com"

# 로그 파일 사용 여부 (Render는 tmp일 수 있음)
LOG_TO_FILE = os.getenv("LOG_TO_FILE", "0") == "1"
LOG_FILE = os.getenv("LOG_FILE", "bot.log")

# 시세 API 연속 실패 허용 횟수
MAX_KLINE_FAILS = int(os.getenv("MAX_KLINE_FAILS", "5"))
KLINE_FAIL_SLEEP = int(os.getenv("KLINE_FAIL_SLEEP", "600"))  # 10분

# 시간대 필터 (UTC 기준: "0-3,12-15" 형식)
TRADING_SESSIONS_UTC = os.getenv("TRADING_SESSIONS_UTC", "0-23")

# Health server
HEALTH_PORT = int(os.getenv("HEALTH_PORT", "0"))

# 상태
OPEN_TRADES: List[Dict[str, Any]] = []
LAST_CLOSE_TS = 0.0
CONSEC_LOSSES = 0
CONSEC_KLINE_FAILS = 0

# RSI 설정
RSI_OVERBOUGHT = int(os.getenv("RSI_OVERBOUGHT", "70"))
RSI_OVERSOLD   = int(os.getenv("RSI_OVERSOLD", "30"))

# metrics
METRICS = {
    "start_ts": START_TS,
    "last_loop_ts": START_TS,
    "open_trades": 0,
    "consec_losses": 0,
    "kline_failures": 0,
}

# ─────────────────────────────
# 공통 유틸 / 로거
# ─────────────────────────────
def log(msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    if LOG_TO_FILE:
        try:
            with open(LOG_FILE, "a") as f:
                f.write(line + "\n")
        except Exception:
            pass

def ts_ms() -> int:
    return int(time.time() * 1000)

def sign_query(params: Dict[str, Any]) -> str:
    qs = "&".join(f"{k}={params[k]}" for k in sorted(params.keys()))
    sig = hmac.new(API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
    return qs + "&signature=" + sig

def headers() -> Dict[str, str]:
    return {"X-BX-APIKEY": API_KEY, "Content-Type": "application/json"}

def req(method: str, path: str, params: Dict[str, Any] | None=None, body: Dict[str, Any] | None=None) -> Dict[str, Any]:
    params = params or {}
    params["timestamp"] = ts_ms()
    url = f"{BASE}{path}?{sign_query(params)}"
    r = requests.request(method, url, json=body, headers=headers(), timeout=12)
    if r.status_code != 200:
        raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text}")
    return r.json()

def send_tg(text: str):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        log("[TG SKIP] " + text)
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=8)
        log("[TG OK] " + text)
    except Exception as e:
        log(f"[TG ERROR] {e} {text}")

# ─────────────────────────────
# 응답 파서 (단일 출처)
# ─────────────────────────────
def normalize_order(resp: Dict[str, Any]) -> Dict[str, Any]:
    d = resp.get("data") or resp
    order_id = d.get("orderId") or d.get("id") or d.get("orderID")
    status   = d.get("status") or d.get("orderStatus")
    avg_px   = d.get("avgPrice") or d.get("price") or d.get("executedPrice") or 0.0
    qty      = d.get("quantity") or d.get("origQty") or d.get("executedQty") or d.get("volume") or 0.0
    trig     = d.get("triggerPrice") or d.get("stopPrice")
    side     = d.get("side")
    return {
        "raw": resp,
        "order_id": order_id,
        "status": status,
        "avg_price": float(avg_px) if avg_px else 0.0,
        "quantity": float(qty) if qty else 0.0,
        "trigger_price": trig,
        "side": side,
    }

def normalize_simple_order_status(resp: Dict[str, Any]) -> str:
    return normalize_order(resp).get("status") or ""

# ─────────────────────────────
# Health / metrics HTTP 서버
# ─────────────────────────────
class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/healthz":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")
        elif self.path == "/metrics":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            uptime = time.time() - METRICS["start_ts"]
            out = [
                f"bot_uptime_seconds {uptime}",
                f"bot_open_trades {METRICS['open_trades']}",
                f"bot_consec_losses {METRICS['consec_losses']}",
                f"bot_kline_failures {METRICS['kline_failures']}",
            ]
            self.wfile.write(("\n".join(out)).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *_):
        return  # quiet

def start_health_server():
    if HEALTH_PORT <= 0:
        return
    def _run():
        httpd = HTTPServer(("0.0.0.0", HEALTH_PORT), _HealthHandler)
        log(f"[HEALTH] server started on 0.0.0.0:{HEALTH_PORT}")
        httpd.serve_forever()
    th = threading.Thread(target=_run, daemon=True)
    th.start()

# ─────────────────────────────
# 시간대 필터 (UTC)
# ─────────────────────────────
def _parse_sessions(spec: str) -> List[Tuple[int, int]]:
    out = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            s, e = part.split("-", 1)
            out.append((int(s), int(e)))
        else:
            h = int(part)
            out.append((h, h))
    return out

SESSIONS = _parse_sessions(TRADING_SESSIONS_UTC)

def in_trading_session_utc() -> bool:
    now_utc = datetime.datetime.utcnow()
    h = now_utc.hour
    for s, e in SESSIONS:
        if s <= h <= e:
            return True
    return False

# ─────────────────────────────
# 잔고 / 포지션 / 주문 조회
# ─────────────────────────────
def get_available_usdt() -> float:
    try:
        res = req("GET", "/openApi/swap/v2/user/balance", {})
        log(f"[BALANCE RAW] {res}")
        data = res.get("data") or res.get("balances") or res
        if isinstance(data, list) and data:
            item = data[0]
        else:
            item = data
        avail = (
            item.get("availableBalance")
            or item.get("availableMargin")
            or item.get("balance")
            or 0.0
        )
        avail = float(avail)
        log(f"[BALANCE] available={avail} USDT")
        return avail
    except Exception as e:
        log(f"[BALANCE ERROR] {e}")
        send_tg(f"⚠️ 잔고 조회 실패: {e}")
        return 0.0

def fetch_open_positions() -> List[Dict[str, Any]]:
    try:
        res = req("GET", "/openApi/swap/v2/trade/positions", {"symbol": SYMBOL})
        log(f"[POSITIONS RAW] {res}")
        data = res.get("data") or res.get("positions") or []
        if not isinstance(data, list):
            data = [data]
        return data
    except Exception as e:
        log(f"[POSITIONS ERROR] {e}")
        return []

def fetch_open_orders() -> List[Dict[str, Any]]:
    try:
        res = req("GET", "/openApi/swap/v2/trade/openOrders", {"symbol": SYMBOL})
        log(f"[OPEN ORDERS RAW] {res}")
        data = res.get("data") or res.get("orders") or []
        if not isinstance(data, list):
            data = [data]
        return data
    except Exception as e:
        log(f"[OPEN ORDERS ERROR] {e}")
        return []

def sync_open_trades_from_exchange():
    global OPEN_TRADES
    positions = fetch_open_positions()
    orders    = fetch_open_orders()
    if not positions:
        log("[SYNC] 열려 있는 포지션이 없습니다.")
        return

    for pos in positions:
        qty = float(pos.get("positionAmt") or pos.get("quantity") or pos.get("size") or 0.0)
        if qty == 0.0:
            continue
        side = "BUY" if (pos.get("positionSide") or pos.get("side") or "").upper() in ("LONG", "BUY") else "SELL"
        entry_price = float(pos.get("entryPrice") or pos.get("avgPrice") or 0.0)
        tp_id = None; sl_id = None; tp_price = None; sl_price = None
        for o in orders:
            o_type = o.get("type") or o.get("orderType")
            oid = o.get("orderId") or o.get("id")
            trig = o.get("triggerPrice") or o.get("stopPrice")
            if o_type and "TAKE" in o_type.upper():
                tp_id = oid; tp_price = trig
            if o_type and "STOP" in o_type.upper():
                sl_id = oid; sl_price = trig
        OPEN_TRADES.append({
            "symbol": SYMBOL,
            "side": "BUY" if side == "BUY" else "SELL",
            "qty": qty,
            "entry": entry_price,
            "entry_order_id": None,
            "tp_order_id": tp_id,
            "sl_order_id": sl_id,
            "tp_price": tp_price,
            "sl_price": sl_price,
        })
    if OPEN_TRADES:
        send_tg(f"🔁 재시작 시 기존 포지션 {len(OPEN_TRADES)}건을 동기화했습니다. (중복 진입 방지)")
    else:
        log("[SYNC] 포지션을 못 가져와서 동기화할 게 없습니다.")

# ─────────────────────────────
# 마켓 데이터
# ─────────────────────────────
def get_klines(symbol: str, interval: str, limit: int = 120) -> List[Tuple[int, float, float, float, float]]:
    resp = requests.get(
        f"{BASE}/openApi/swap/v2/quote/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
        timeout=12,
    )
    try:
        log(f"[KLINES {interval}] {resp.text[:200]}")
    except Exception:
        log(f"[KLINES {interval}] <cannot print>")

    raw = resp.json()
    data = raw.get("data", []) if isinstance(raw, dict) else raw

    out: List[Tuple[int, float, float, float, float]] = []
    for it in data:
        if isinstance(it, dict):
            ts_val = it.get("time") or it.get("openTime") or it.get("t")
            if not ts_val:
                continue
            try:
                ts = int(ts_val)
                o = float(it.get("open")); h = float(it.get("high"))
                l = float(it.get("low"));  c = float(it.get("close"))
            except Exception:
                continue
            out.append((ts, o, h, l, c))
        else:
            try:
                ts = int(it[0])
                o, h, l, c = map(float, it[1:5])
                out.append((ts, o, h, l, c))
            except Exception:
                continue
    out.sort(key=lambda x: x[0])
    return out

# ─────────────────────────────
# ATR 계산
# ─────────────────────────────
def calc_atr(candles: List[Tuple[int, float, float, float, float]], length: int = 14) -> Optional[float]:
    if len(candles) < length + 1:
        return None
    trs: List[float] = []
    for i in range(1, len(candles)):
        _, _, high, low, close = candles[i]
        _, _, prev_high, prev_low, prev_close = candles[i-1]
        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close)
        )
        trs.append(tr)
    if len(trs) < length:
        return None
    atr = sum(trs[-length:]) / length
    return atr

# ─────────────────────────────
# 인디케이터 / 다이버전스
# ─────────────────────────────
def ema(values: List[float], length: int) -> List[float]:
    if len(values) < length:
        return [math.nan] * len(values)
    k = 2 / (length + 1)
    out = [math.nan] * (length - 1)
    ema_prev = sum(values[:length]) / length
    out.append(ema_prev)
    for v in values[length:]:
        ema_prev = v * k + ema_prev * (1 - k)
        out.append(ema_prev)
    return out

def rsi(closes: List[float], length: int = 14) -> List[float]:
    if len(closes) < length + 1:
        return [math.nan] * len(closes)
    gains, losses = [], []
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i - 1]
        gains.append(max(ch, 0.0))
        losses.append(max(-ch, 0.0))
    avg_gain = sum(gains[:length]) / length
    avg_loss = sum(losses[:length]) / length
    rsis = [math.nan] * length
    rs = (avg_gain / avg_loss) if avg_loss > 0 else float("inf")
    rsis.append(100 - 100 / (1 + rs))
    for i in range(length, len(gains)):
        avg_gain = (avg_gain * (length - 1) + gains[i]) / length
        avg_loss = (avg_loss * (length - 1) + losses[i]) / length
        rs = (avg_gain / avg_loss) if avg_loss > 0 else float("inf")
        rsis.append(100 - 100 / (1 + rs))
    return rsis

def _find_last_two_pivot_highs(candles):
    idxs = []
    for i in range(len(candles)-2, 1, -1):
        if candles[i][2] > candles[i-1][2] and candles[i][2] > candles[i+1][2]:
            idxs.append(i)
            if len(idxs) == 2:
                break
    return list(reversed(idxs))

def _find_last_two_pivot_lows(candles):
    idxs = []
    for i in range(len(candles)-2, 1, -1):
        if candles[i][3] < candles[i-1][3] and candles[i][3] < candles[i+1][3]:
            idxs.append(i)
            if len(idxs) == 2:
                break
    return list(reversed(idxs))

def has_bearish_rsi_divergence(candles, rsi_vals) -> bool:
    piv = _find_last_two_pivot_highs(candles)
    if len(piv) < 2:
        return False
    i1, i2 = piv
    price_up = candles[i2][2] > candles[i1][2]
    rsi_down = rsi_vals[i2] < rsi_vals[i1]
    return price_up and rsi_down

def has_bullish_rsi_divergence(candles, rsi_vals) -> bool:
    piv = _find_last_two_pivot_lows(candles)
    if len(piv) < 2:
        return False
    i1, i2 = piv
    price_down = candles[i2][3] < candles[i1][3]
    rsi_up     = rsi_vals[i2] > rsi_vals[i1]
    return price_down and rsi_up

# ─────────────────────────────
# 전략 1: 3m 추세 + 15m 필터
# ─────────────────────────────
def decide_signal_3m_trend(candles: List[Tuple[int,float,float,float,float]]) -> str | None:
    closes = [c[4] for c in candles]
    if len(closes) < 60:
        return None
    e20 = ema(closes, 20)
    e50 = ema(closes, 50)
    r   = rsi(closes, 14)

    e20p, e20n = e20[-2], e20[-1]
    e50p, e50n = e50[-2], e50[-1]
    r_now = r[-1]
    price = closes[-1]

    # 횡보 필터
    spread_ratio = abs(e20n - e50n) / e50n
    if spread_ratio < 0.0005:
        return None
    last = candles[-1]
    last_range_pct = (last[2] - last[3]) / last[3] if last[3] else 0
    if last_range_pct < 0.001:
        return None

    long_sig  = (e20p < e50p) and (e20n > e50n) and (r_now < RSI_OVERBOUGHT)
    short_sig = (e20p > e50p) and (e20n < e50n) and (r_now > RSI_OVERSOLD)

    # 방향 정리
    if long_sig and price < e50n:
        long_sig = False
    if short_sig and price > e50n:
        short_sig = False

    # 다이버전스 필터
    if long_sig:
        if has_bearish_rsi_divergence(candles, r):
            return None
        return "LONG"
    if short_sig:
        if has_bullish_rsi_divergence(candles, r):
            return None
        return "SHORT"
    return None

def decide_trend_15m(candles_15m) -> str | None:
    closes = [c[4] for c in candles_15m]
    if len(closes) < 50:
        return None
    e20 = ema(closes, 20)
    e50 = ema(closes, 50)
    if math.isnan(e20[-1]) or math.isnan(e50[-1]):
        return None
    if e20[-1] > e50[-1]:
        return "LONG"
    if e20[-1] < e50[-1]:
        return "SHORT"
    return None

# ─────────────────────────────
# 전략 2: 박스(레인지) 모드 – 옵션
# ─────────────────────────────
def decide_signal_range(candles) -> str | None:
    if len(candles) < 40:
        return None
    closes = [c[4] for c in candles]
    r = rsi(closes, 14)
    hi = max(c[2] for c in candles[-40:])
    lo = min(c[3] for c in candles[-40:])
    now = closes[-1]
    box_h = hi - lo
    if lo == 0:
        return None
    box_pct = box_h / lo
    if box_pct < 0.0015:  # 0.15%
        return None
    upper_line = lo + box_h * 0.75
    lower_line = lo + box_h * 0.25
    r_now = r[-1]
    if now >= upper_line and r_now > 60:
        return "SHORT"
    if now <= lower_line and r_now < 40:
        return "LONG"
    return None

# ─────────────────────────────
# BingX 거래
# ─────────────────────────────
def set_leverage_and_mode(symbol: str, leverage: int, isolated: bool = True):
    req("POST", "/openApi/swap/v2/trade/leverage", {
        "symbol": symbol,
        "leverage": leverage,
    })
    req("POST", "/openApi/swap/v2/trade/marginType", {
        "symbol": symbol,
        "marginType": "ISOLATED" if isolated else "CROSSED",
    })

def place_market(symbol: str, side: str, qty: float) -> Dict[str, Any]:
    return req("POST", "/openApi/swap/v2/trade/order", {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": qty,
    })

def place_conditional(symbol: str, side: str, qty: float, trigger_price: float, order_type: str) -> Dict[str, Any]:
    # order_type: "TAKE_PROFIT_MARKET" or "STOP_MARKET"
    return req("POST", "/openApi/swap/v2/trade/order", {
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "quantity": qty,
        "reduceOnly": True,
        "triggerPrice": trigger_price,
    })

def place_tp_sl(symbol: str, side_open: str, qty: float, entry: float, tp_pct: float, sl_pct: float):
    close_side = "SELL" if side_open == "BUY" else "BUY"
    tp_price = round(entry * (1 + tp_pct) if side_open == "BUY" else entry * (1 - tp_pct), 2)
    sl_price = round(entry * (1 - sl_pct) if side_open == "BUY" else entry * (1 + sl_pct), 2)

    tp_res = place_conditional(symbol, close_side, qty, tp_price, "TAKE_PROFIT_MARKET")
    sl_res = place_conditional(symbol, close_side, qty, sl_price, "STOP_MARKET")
    return {
        "tp": tp_price,
        "sl": sl_price,
        "tp_order_id": normalize_order(tp_res)["order_id"],
        "sl_order_id": normalize_order(sl_res)["order_id"],
    }

def close_position_market(symbol: str, side_open: str, qty: float):
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

def cancel_order(symbol: str, order_id: str):
    # BingX 실제 취소 경로가 다르면 여기만 바꾸면 됨.
    try:
        res = req("POST", "/openApi/swap/v2/trade/cancel", {
            "symbol": symbol,
            "orderId": order_id,
        })
        log(f"[CANCEL] order_id={order_id} resp={res}")
    except Exception as e:
        log(f"[CANCEL ERROR] {e}")

def get_order(symbol: str, order_id: str) -> Dict[str, Any]:
    return req("GET", "/openApi/swap/v2/trade/order", {
        "symbol": symbol,
        "orderId": order_id,
    })

def get_fills(symbol: str, order_id: str) -> List[Dict[str, Any]]:
    res = req("GET", "/openApi/swap/v2/trade/allFillOrders", {
        "symbol": symbol,
        "orderId": order_id,
        "limit": 50,
    })
    return res.get("data", []) or []

def summarize_fills(symbol: str, order_id: str) -> Dict[str, Any] | None:
    fills = get_fills(symbol, order_id)
    if not fills:
        return None
    total_qty = 0.0
    total_pnl = 0.0
    notional = 0.0
    last_time = None
    for f in fills:
        q = float(f.get("quantity") or f.get("qty") or f.get("volume") or f.get("vol") or 0.0)
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
    end = time.time() + timeout
    last_status = None
    while time.time() < end:
        try:
            o = get_order(symbol, order_id)
            norm = normalize_order(o)
            last_status = norm["status"]
            if last_status in ("FILLED", "PARTIALLY_FILLED"):
                return norm
        except Exception as e:
            log(f"[wait_filled] error: {e}")
        time.sleep(0.5)
    log(f"[wait_filled] timeout, last_status={last_status}, cancel order")
    # 타임아웃 시 주문 취소
    cancel_order(symbol, order_id)
    return None

def ensure_tp_sl_for_trade(t: Dict[str, Any]) -> bool:
    """
    아직 포지션이 열려 있는데 TP 또는 SL이 없거나, 상태가 취소돼 있으면 다시 건다.
    실패하면 False 리턴해서 포지션 강제 정리하도록 한다.
    """
    symbol = t["symbol"]
    side_open = t["side"]
    qty = t["qty"]
    close_side = "SELL" if side_open == "BUY" else "BUY"

    need_tp = False
    need_sl = False

    # TP 확인
    if t.get("tp_order_id"):
        try:
            o = get_order(symbol, t["tp_order_id"])
            st = normalize_order(o)["status"]
            if st in ("CANCELED", "REJECTED", "EXPIRED"):
                need_tp = True
        except Exception as e:
            log(f"[ensure_tp_sl] TP check error: {e}")
            need_tp = True
    else:
        need_tp = True

    # SL 확인
    if t.get("sl_order_id"):
        try:
            o = get_order(symbol, t["sl_order_id"])
            st = normalize_order(o)["status"]
            if st in ("CANCELED", "REJECTED", "EXPIRED"):
                need_sl = True
        except Exception as e:
            log(f"[ensure_tp_sl] SL check error: {e}")
            need_sl = True
    else:
        need_sl = True

    ok = True
    if need_tp:
        try:
            tp_res = place_conditional(symbol, close_side, qty, t["tp_price"], "TAKE_PROFIT_MARKET")
            t["tp_order_id"] = normalize_order(tp_res)["order_id"]
            send_tg(f"🔄 TP 재설정: {symbol} {t['tp_price']}")
        except Exception as e:
            send_tg(f"❗ TP 재설정 실패: {e}")
            ok = False
    if need_sl:
        try:
            sl_res = place_conditional(symbol, close_side, qty, t["sl_price"], "STOP_MARKET")
            t["sl_order_id"] = normalize_order(sl_res)["order_id"]
            send_tg(f"🔄 SL 재설정: {symbol} {t['sl_price']}")
        except Exception as e:
            send_tg(f"❗ SL 재설정 실패: {e}")
            ok = False
    return ok

def check_closes() -> List[Dict[str, Any]]:
    global OPEN_TRADES
    if not OPEN_TRADES:
        return []
    still_open = []
    closed_results: List[Dict[str, Any]] = []
    for t in OPEN_TRADES:
        symbol = t["symbol"]
        tp_id = t.get("tp_order_id")
        sl_id = t.get("sl_order_id")
        closed = False
        # TP 검사
        if tp_id:
            try:
                o = get_order(symbol, tp_id)
                st = normalize_order(o)["status"]
                if st == "FILLED":
                    summary = summarize_fills(symbol, tp_id)
                    closed_results.append({"trade": t, "reason": "TP", "summary": summary})
                    closed = True
            except Exception as e:
                log(f"check_closes TP error: {e}")
        # SL 검사
        if (not closed) and sl_id:
            try:
                o = get_order(symbol, sl_id)
                st = normalize_order(o)["status"]
                if st == "FILLED":
                    summary = summarize_fills(symbol, sl_id)
                    closed_results.append({"trade": t, "reason": "SL", "summary": summary})
                    closed = True
            except Exception as e:
                log(f"check_closes SL error: {e}")
        if not closed:
            # 여기서 TP/SL 한쪽이 없으면 재설정
            ok = ensure_tp_sl_for_trade(t)
            if not ok:
                # 재설정 실패 → 포지션 닫기
                close_position_market(symbol, t["side"], t["qty"])
                # 닫혔다고 보고하지는 않고 넘어간다.
            else:
                still_open.append(t)
    OPEN_TRADES = still_open
    return closed_results

# ─────────────────────────────
# 메인 루프
# ─────────────────────────────
RUNNING = True
def _sigterm(*_):
    global RUNNING, TERMINATED_BY_SIGNAL
    TERMINATED_BY_SIGNAL = True
    RUNNING = False
signal.signal(signal.SIGTERM, _sigterm)

def main():
    global LAST_CLOSE_TS, CONSEC_LOSSES, CONSEC_KLINE_FAILS, OPEN_TRADES
    # 설정값 검증
    if not API_KEY or not API_SECRET:
        raise RuntimeError("BINGX_API_KEY / BINGX_API_SECRET 가 필요합니다.")
    if not (0 < RISK_PCT <= 1):
        raise ValueError("RISK_PCT 는 0 < RISK_PCT <= 1 이어야 합니다.")
    if LEVERAGE <= 0:
        raise ValueError("LEVERAGE 는 0보다 커야 합니다.")
    if TP_PCT <= 0 or SL_PCT <= 0:
        raise ValueError("TP_PCT, SL_PCT 는 0보다 커야 합니다.")

    send_tg("✅ [봇 시작] BingX 자동매매 시작합니다.")
    set_leverage_and_mode(SYMBOL, LEVERAGE, ISOLATED)

    start_health_server()

    # 재시작 시 거래소 상태와 동기화
    sync_open_trades_from_exchange()

    last_reset_day = time.strftime("%Y-%m-%d")
    daily_pnl = 0.0
    last_fill_check = 0.0

    while RUNNING:
        try:
            METRICS["last_loop_ts"] = time.time()
            METRICS["open_trades"] = len(OPEN_TRADES)
            METRICS["consec_losses"] = CONSEC_LOSSES
            METRICS["kline_failures"] = CONSEC_KLINE_FAILS

            today = time.strftime("%Y-%m-%d")
            if today != last_reset_day:
                send_tg(f"📊 일일 정산: PnL {daily_pnl:.2f} USDT, 연속 손실 {CONSEC_LOSSES}")
                daily_pnl = 0.0
                CONSEC_LOSSES = 0
                last_reset_day = today

            now = time.time()
            if now - last_fill_check >= POLL_FILLS_SEC:
                closed_list = check_closes()
                for c in closed_list:
                    t = c["trade"]; reason = c["reason"]; summary = c["summary"] or {}
                    closed_qty = summary.get("qty") or t["qty"]
                    closed_price = summary.get("avg_price") or 0.0
                    pnl = summary.get("pnl")
                    if pnl is None or pnl == 0.0:
                        if reason == "TP":
                            if t["side"] == "BUY":
                                pnl = (t["tp_price"] - t["entry"]) * closed_qty
                            else:
                                pnl = (t["entry"] - t["tp_price"]) * closed_qty
                        else:
                            if t["side"] == "BUY":
                                pnl = (t["sl_price"] - t["entry"]) * closed_qty
                            else:
                                pnl = (t["entry"] - t["sl_price"]) * closed_qty
                    daily_pnl += pnl
                    LAST_CLOSE_TS = now
                    if pnl < 0: CONSEC_LOSSES += 1
                    else:       CONSEC_LOSSES = 0
                    send_tg(
                        f"💰 청산({reason}) {t['symbol']} {t['side']} 수량={closed_qty} "
                        f"가격={closed_price:.2f} PnL={pnl:.2f} USDT (금일 누적 {daily_pnl:.2f})"
                    )
                last_fill_check = now

            # 포지션 있으면 아무것도 안 함
            if OPEN_TRADES:
                time.sleep(1); continue

            # 연속 3손실이면 휴식
            if CONSEC_LOSSES >= 3:
                send_tg("⏸ 연속 3회 손실 감지. 1시간 쉬고 다시 시작합니다.")
                time.sleep(COOLDOWN_AFTER_3LOSS)
                CONSEC_LOSSES = 0
                continue

            # 청산 직후 쿨다운
            if LAST_CLOSE_TS > 0 and (time.time() - LAST_CLOSE_TS) < COOLDOWN_AFTER_CLOSE:
                time.sleep(1); continue

            # 시간대 필터
            if not in_trading_session_utc():
                time.sleep(2); continue

            # 데이터 가져오기
            try:
                candles_3m = get_klines(SYMBOL, INTERVAL, 120)
                CONSEC_KLINE_FAILS = 0
            except Exception as e:
                CONSEC_KLINE_FAILS += 1
                log(f"[KLINE ERROR] {e} (fail={CONSEC_KLINE_FAILS})")
                if CONSEC_KLINE_FAILS >= MAX_KLINE_FAILS:
                    send_tg(f"⚠️ 시세 연속 {CONSEC_KLINE_FAILS}회 실패. {KLINE_FAIL_SLEEP}s 대기.")
                    time.sleep(KLINE_FAIL_SLEEP)
                    CONSEC_KLINE_FAILS = 0
                else:
                    time.sleep(2)
                continue

            if len(candles_3m) < 50:
                time.sleep(1); continue

            # 신호 후보
            chosen_signal = None
            signal_source = None

            # 1) 추세 모드
            if ENABLE_TREND:
                candles_15m = get_klines(SYMBOL, "15m", 120)
                sig_3m = decide_signal_3m_trend(candles_3m)
                trend_15m = decide_trend_15m(candles_15m)
                if sig_3m and trend_15m and sig_3m == trend_15m:
                    chosen_signal = sig_3m
                    signal_source = "TREND"

                    # 1분봉 컨펌
                    if ENABLE_1M_CONFIRM:
                        candles_1m = get_klines(SYMBOL, "1m", 20)
                        if len(candles_1m) >= 2:
                            last_1m = candles_1m[-1]
                            if chosen_signal == "LONG" and last_1m[4] < last_1m[1]:
                                chosen_signal = None
                            elif chosen_signal == "SHORT" and last_1m[4] > last_1m[1]:
                                chosen_signal = None

            # 2) 박스 모드 (추세가 없을 때만)
            if (not chosen_signal) and ENABLE_RANGE:
                sig_r = decide_signal_range(candles_3m)
                if sig_r:
                    chosen_signal = sig_r
                    signal_source = "RANGE"

            if not chosen_signal:
                time.sleep(1); continue

            last_price = candles_3m[-1][4]

            # 슬리피지 가드
            prev_price = candles_3m[-2][4]
            move_pct = abs(last_price - prev_price) / prev_price
            if move_pct > MAX_PRICE_JUMP_PCT:
                log(f"[PRICE GUARD] price jumped {move_pct:.4f}, skip entry")
                time.sleep(1); continue

            # 잔고 확인
            avail = get_available_usdt()
            if avail <= 0:
                send_tg("⚠️ 가용 선물 잔고가 0입니다. 진입을 건너뜁니다.")
                time.sleep(3); continue

            # 리스크 비율 적용
            notional = avail * RISK_PCT * LEVERAGE
            if notional < MIN_NOTIONAL_USDT:
                send_tg(f"⚠️ 계산된 주문 금액이 너무 작습니다: {notional:.2f} USDT")
                time.sleep(3); continue
            notional = min(notional, MAX_NOTIONAL_USDT)

            qty = round(notional / last_price, 6)
            side = "BUY" if chosen_signal == "LONG" else "SELL"

            # ATR 기반 TP/SL 계산
            local_tp_pct = TP_PCT
            local_sl_pct = SL_PCT
            if USE_ATR:
                atr = calc_atr(candles_3m, ATR_LEN)
                if atr and last_price > 0:
                    sl_pct_atr = (atr * ATR_SL_MULT) / last_price
                    tp_pct_atr = (atr * ATR_TP_MULT) / last_price
                    local_sl_pct = max(sl_pct_atr, MIN_SL_PCT)
                    local_tp_pct = max(tp_pct_atr, MIN_TP_PCT)

            send_tg(
                f"🟢 진입 시도: {SYMBOL} 전략={signal_source} 방향={chosen_signal}"
                f" 레버리지={LEVERAGE}x 사용비율={RISK_PCT*100:.0f}% 명목가≈{notional:.2f}USDT 수량={qty}"
            )

            # 실제 진입
            try:
                res = place_market(SYMBOL, side, qty)
            except Exception as e:
                send_tg(f"❌ 시장가 진입 실패: {e}")
                time.sleep(2); continue

            entry = last_price
            entry_order_id = normalize_order(res)["order_id"]
            try:
                entry = float((res.get("data") or {}).get("avgPrice", entry))
            except Exception:
                pass
            if not entry_order_id:
                send_tg("⚠️ 시장가 진입 응답에 orderId가 없습니다. TP/SL을 걸지 않습니다.")
                time.sleep(2); continue

            # 진입 체결 여부 폴링 (타임아웃 시 취소)
            filled_data = wait_filled(SYMBOL, entry_order_id, timeout=5)
            if not filled_data:
                send_tg("⚠️ 시장가 주문이 제한 시간 내 FILLED되지 않아 포지션을 건너뜁니다.")
                time.sleep(2); continue

            filled_qty = float(filled_data.get("quantity") or filled_data.get("executedQty") or qty)
            if filled_qty <= 0:
                send_tg("⚠️ 시장가 주문 체결 수량이 0입니다. TP/SL 미설정.")
                time.sleep(2); continue

            # TP/SL 예약
            try:
                tp_sl = place_tp_sl(SYMBOL, side, filled_qty, entry, local_tp_pct, local_sl_pct)
            except Exception as e:
                send_tg(f"❌ TP/SL 예약 실패: {e}, 포지션을 즉시 닫습니다.")
                close_position_market(SYMBOL, side, filled_qty)
                time.sleep(2); continue

            send_tg(
                f"📌 예약완료: TP={tp_sl['tp']} / SL={tp_sl['sl']} "
                f"(reduceOnly, mode={'ATR' if USE_ATR else 'FIXED'})"
            )

            OPEN_TRADES.append({
                "symbol": SYMBOL,
                "side": side,
                "qty": filled_qty,
                "entry": entry,
                "entry_order_id": entry_order_id,
                "tp_order_id": tp_sl["tp_order_id"],
                "sl_order_id": tp_sl["sl_order_id"],
                "tp_price": tp_sl["tp"],
                "sl_price": tp_sl["sl"],
            })

            time.sleep(COOLDOWN_SEC)

        except Exception as e:
            log(f"ERROR: {e}")
            send_tg(f"❌ 오류 발생: {e}")
            time.sleep(2)

    uptime = time.time() - START_TS
    if (not TERMINATED_BY_SIGNAL) and (uptime >= MIN_UPTIME_FOR_STOP):
        send_tg("🛑 봇이 종료되었습니다.")
    else:
        log(f"[SKIP STOP] sig={TERMINATED_BY_SIGNAL} uptime={uptime:.2f}s")

if __name__ == "__main__":
    main()
