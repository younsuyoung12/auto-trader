# bot.py
# BingX Auto Trader (Render Background Worker)
# - EMA20/EMA50 크로스 + RSI 필터로 방향 결정
# - 진입 즉시 TP/SL 예약 (reduceOnly)
# - 청산 후 쿨다운 → 다시 신호 체크
# - 텔레그램 알림
import os, time, hmac, hashlib, math, signal
from typing import Any, Dict, List, Tuple
import requests

# ─────────────────────────────
# 환경변수
# ─────────────────────────────
API_KEY    = os.getenv("BINGX_API_KEY", "")
API_SECRET = os.getenv("BINGX_API_SECRET", "")

SYMBOL   = os.getenv("SYMBOL", "BTC-USDT")     # 계정에 따라 BTCUSDT로 바꿔야 할 수도 있음
INTERVAL = os.getenv("INTERVAL", "3m")         # 기본 3분봉
LEVERAGE = int(os.getenv("LEVERAGE", "30"))
ISOLATED = os.getenv("ISOLATED", "1") == "1"

TP_PCT   = float(os.getenv("TP_PCT", "0.012"))  # 기본 1.2% 익절
SL_PCT   = float(os.getenv("SL_PCT", "0.006"))  # 기본 0.6% 손절
COOLDOWN_SEC = int(os.getenv("COOLDOWN_SEC", "15"))

POSITION_VALUE_USDT = float(os.getenv("POSITION_VALUE_USDT", "50"))  # 1회 진입 명목가
QTY_FIX = os.getenv("QTY_FIX")  # 있으면 이 수량 그대로 사용

MAX_DAILY_DRAWDOWN_PCT = float(os.getenv("MAX_DAILY_DRAWDOWN_PCT", "3"))
MAX_CONSECUTIVE_LOSSES = int(os.getenv("MAX_CONSECUTIVE_LOSSES", "3"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

BASE = "https://open-api.bingx.com"

# ─────────────────────────────
# 공통 유틸
# ─────────────────────────────
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
    """텔레그램으로 전송하고, Render 로그에도 찍는다."""
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        print("[TG SKIP]", text)
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=8)
        print("[TG OK]", text)
    except Exception as e:
        print("[TG ERROR]", e, text)

# ─────────────────────────────
# 마켓 데이터 (여기 수정)
# ─────────────────────────────
def get_klines(symbol: str, interval: str, limit: int = 120) -> List[Tuple[int, float, float, float, float]]:
    r = requests.get(
        f"{BASE}/openApi/swap/v2/quote/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
        timeout=12,
    )
    raw = r.json()
    data = raw.get("data", []) if isinstance(raw, dict) else raw

    out: List[Tuple[int, float, float, float, float]] = []

    for it in data:
        if isinstance(it, dict):
            # 빙엑스가 딕셔너리로 줄 때
            ts = int(it.get("time") or it.get("openTime") or it.get("t"))
            o = float(it.get("open"))
            h = float(it.get("high"))
            l = float(it.get("low"))
            c = float(it.get("close"))
            out.append((ts, o, h, l, c))
        else:
            # 옛날/다른 포맷: 리스트
            ts = int(it[0])
            o, h, l, c = map(float, it[1:5])
            out.append((ts, o, h, l, c))

    out.sort(key=lambda x: x[0])
    return out

# ─────────────────────────────
# 인디케이터
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

RSI_OVERBOUGHT = int(os.getenv("RSI_OVERBOUGHT", "70"))
RSI_OVERSOLD   = int(os.getenv("RSI_OVERSOLD", "30"))

def decide_signal(candles: List[Tuple[int,float,float,float,float]]) -> str | None:
    """EMA20/EMA50 크로스 + RSI 조건으로 LONG/SHORT 결정"""
    closes = [c[4] for c in candles]
    if len(closes) < 60:
        return None
    e20 = ema(closes, 20)
    e50 = ema(closes, 50)
    r   = rsi(closes, 14)
    e20p, e20n = e20[-2], e20[-1]
    e50p, e50n = e50[-2], e50[-1]
    r_now = r[-1]

    long_sig  = (e20p < e50p) and (e20n > e50n) and (r_now < RSI_OVERBOUGHT)
    short_sig = (e20p > e50p) and (e20n < e50n) and (r_now > RSI_OVERSOLD)

    if long_sig:
        return "LONG"
    if short_sig:
        return "SHORT"
    return None

# ─────────────────────────────
# 계정/주문
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

def place_tp_sl(symbol: str, side_open: str, qty: float, entry: float, tp_pct: float, sl_pct: float):
    close_side = "SELL" if side_open == "BUY" else "BUY"
    tp_price = round(entry * (1 + tp_pct) if side_open == "BUY" else entry * (1 - tp_pct), 2)
    sl_price = round(entry * (1 - sl_pct) if side_open == "BUY" else entry * (1 + sl_pct), 2)

    # 익절
    req("POST", "/openApi/swap/v2/trade/order", {
        "symbol": symbol,
        "side": close_side,
        "type": "TAKE_PROFIT_MARKET",
        "quantity": qty,
        "reduceOnly": True,
        "triggerPrice": tp_price,
    })
    # 손절
    req("POST", "/openApi/swap/v2/trade/order", {
        "symbol": symbol,
        "side": close_side,
        "type": "STOP_MARKET",
        "quantity": qty,
        "reduceOnly": True,
        "triggerPrice": sl_price,
    })
    return {"tp": tp_price, "sl": sl_price}

# ─────────────────────────────
# 메인 루프
# ─────────────────────────────
RUNNING = True
def _sigterm(*_):
    global RUNNING
    RUNNING = False
signal.signal(signal.SIGTERM, _sigterm)

def main():
    assert API_KEY and API_SECRET, "BINGX_API_KEY / BINGX_API_SECRET 필요"
    send_tg("✅ [BOT READY] BingX auto trader started")

    set_leverage_and_mode(SYMBOL, LEVERAGE, ISOLATED)

    last_reset_day = time.strftime("%Y-%m-%d")
    daily_pnl = 0.0
    consec_losses = 0

    while RUNNING:
        try:
            today = time.strftime("%Y-%m-%d")
            if today != last_reset_day:
                send_tg(f"📊 [DAILY] PnL {daily_pnl:.2f} USDT, 연속손실 {consec_losses}")
                daily_pnl = 0.0
                consec_losses = 0
                last_reset_day = today

            candles = get_klines(SYMBOL, INTERVAL, 120)
            sig = decide_signal(candles)
            if not sig:
                time.sleep(1)
                continue

            last_price = candles[-1][4]
            if QTY_FIX:
                qty = float(QTY_FIX)
            else:
                qty = round(POSITION_VALUE_USDT / last_price, 6)

            side = "BUY" if sig == "LONG" else "SELL"
            send_tg(f"🟢 [ENTRY] {SYMBOL} {sig} {LEVERAGE}x qty={qty} @≈{last_price}")

            res = place_market(SYMBOL, side, qty)
            entry = last_price
            try:
                entry = float(res.get("data", {}).get("avgPrice", entry))
            except Exception:
                pass

            tp_sl = place_tp_sl(SYMBOL, side, qty, entry, TP_PCT, SL_PCT)
            send_tg(f"📌 [ORDERS] TP@{tp_sl['tp']} / SL@{tp_sl['sl']} (reduceOnly)")

            time.sleep(COOLDOWN_SEC)

        except Exception as e:
            print("ERROR:", e)
            send_tg(f"❌ [ERROR] {e}")
            time.sleep(2)

    send_tg("🛑 [BOT STOPPED]")

if __name__ == "__main__":
    main()
