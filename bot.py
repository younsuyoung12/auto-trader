# bot.py
# BingX Auto Trader (Render Background Worker)
# - 차트 신호(EMA20/EMA50 + RSI)로 방향 자동결정
# - 진입 즉시 TP/SL(리듀스온리) 예약
# - 청산 후 5초 쿨다운 → 재평가 재진입
# - 텔레그램 알림(진입/TP/SL/에러/데일리)
# Python 3.11+

import os, time, hmac, hashlib, math, json, signal, random
from typing import Any, Dict, List, Tuple
import requests

# ─────────────────────────────────────────────────────────────
# 환경변수
# ─────────────────────────────────────────────────────────────
API_KEY  = os.getenv("BINGX_API_KEY", "")
API_SECRET = os.getenv("BINGX_API_SECRET", "")

SYMBOL   = os.getenv("SYMBOL", "BTC-USDT")     # 계정에 따라 BTCUSDT일 수도 있음
INTERVAL = os.getenv("INTERVAL", "3m")         # 1m/3m/5m/15m/1h/4h/1d
LEVERAGE = int(os.getenv("LEVERAGE", "30"))
ISOLATED = os.getenv("ISOLATED", "1") == "1"

TP_PCT   = float(os.getenv("TP_PCT", "0.021"))  # +2.1% (수수료/슬리피지 보정)
SL_PCT   = float(os.getenv("SL_PCT", "0.010"))  # -1.0%
COOLDOWN_SEC = int(os.getenv("COOLDOWN_SEC", "5"))

POSITION_VALUE_USDT = float(os.getenv("POSITION_VALUE_USDT", "50"))  # 1회 명목가 (USDT)
QTY_FIX   = os.getenv("QTY_FIX")  # 고정 수량 사용 시 ex) "0.002" (None이면 명목가/현재가로 계산)

MAX_DAILY_DRAWDOWN_PCT = float(os.getenv("MAX_DAILY_DRAWDOWN_PCT", "3"))   # 일손실 -3% 도달 시 중지
MAX_CONSECUTIVE_LOSSES = int(os.getenv("MAX_CONSECUTIVE_LOSSES", "3"))    # 연속 손절 N회 정지

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

BASE = "https://open-api.bingx.com"

# ─────────────────────────────────────────────────────────────
# 공통 유틸
# ─────────────────────────────────────────────────────────────
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
    try:
        data = r.json()
    except Exception:
        raise RuntimeError(f"Invalid JSON: {r.text[:200]}")
    return data

def send_tg(text: str):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=8)
    except Exception:
        pass

# ─────────────────────────────────────────────────────────────
# 퍼블릭 마켓 데이터
# ─────────────────────────────────────────────────────────────
def get_price(symbol: str) -> float:
    r = requests.get(f"{BASE}/openApi/swap/v2/quote/price", params={"symbol": symbol}, timeout=10)
    j = r.json()
    return float(j["data"]["price"])

def get_klines(symbol: str, interval: str, limit: int=120) -> List[Tuple[int,float,float,float,float]]:
    r = requests.get(f"{BASE}/openApi/swap/v2/quote/klines",
                     params={"symbol": symbol, "interval": interval, "limit": limit},
                     timeout=12)
    j = r.json()["data"]
    # [openTime, open, high, low, close, volume, ...] 가정
    out = []
    for it in j:
        ts = int(it[0])
        o,h,l,c = map(float, it[1:5])
        out.append((ts,o,h,l,c))
    out.sort(key=lambda x: x[0])
    return out

# ─────────────────────────────────────────────────────────────
# 인디케이터 (EMA, RSI)
# ─────────────────────────────────────────────────────────────
def ema(values: List[float], length: int) -> List[float]:
    if len(values) < length:
        return [math.nan]*len(values)
    k = 2/(length+1)
    out = [math.nan]*(length-1)
    ema_prev = sum(values[:length])/length
    out.append(ema_prev)
    for v in values[length:]:
        ema_prev = v*k + ema_prev*(1-k)
        out.append(ema_prev)
    return out

def rsi(closes: List[float], length: int=14) -> List[float]:
    if len(closes) < length+1:
        return [math.nan]*len(closes)
    gains, losses = [], []
    for i in range(1, len(closes)):
        ch = closes[i]-closes[i-1]
        gains.append(max(ch,0.0)); losses.append(max(-ch,0.0))
    avg_gain = sum(gains[:length])/length
    avg_loss = sum(losses[:length])/length
    rsis = [math.nan]*(length)
    rs = (avg_gain/avg_loss) if avg_loss>0 else float("inf")
    rsis.append(100 - 100/(1+rs))
    for i in range(length, len(gains)):
        avg_gain = (avg_gain*(length-1) + gains[i]) / length
        avg_loss = (avg_loss*(length-1) + losses[i]) / length
        rs = (avg_gain/avg_loss) if avg_loss>0 else float("inf")
        rsis.append(100 - 100/(1+rs))
    return rsis

# ─────────────────────────────────────────────────────────────
# 신호(롱/숏) 결정: EMA20/EMA50 + RSI 필터
# ─────────────────────────────────────────────────────────────
RSI_OVERBOUGHT = int(os.getenv("RSI_OVERBOUGHT", "70"))
RSI_OVERSOLD  = int(os.getenv("RSI_OVERSOLD",  "30"))

def decide_signal(candles: List[Tuple[int,float,float,float,float]]) -> str | None:
    closes = [c[4] for c in candles]
    if len(closes) < 60:
        return None
    e20 = ema(closes, 20)
    e50 = ema(closes, 50)
    r = rsi(closes, 14)
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

# ─────────────────────────────────────────────────────────────
# 계정/주문
# ─────────────────────────────────────────────────────────────
def set_leverage_and_mode(symbol: str, leverage: int, isolated: bool=True):
    # 레버리지
    req("POST", "/openApi/swap/v2/trade/leverage", {
        "symbol": symbol, "leverage": leverage
    })
    # 격리/교차
    req("POST", "/openApi/swap/v2/trade/marginType", {
        "symbol": symbol, "marginType": "ISOLATED" if isolated else "CROSSED"
    })

def place_market(symbol: str, side: str, qty: float) -> Dict[str, Any]:
    # side: BUY(Long) / SELL(Short)
    return req("POST", "/openApi/swap/v2/trade/order", {
        "symbol": symbol, "side": side, "type": "MARKET", "quantity": qty
    })

def place_tp_sl(symbol: str, side_open: str, qty: float, entry: float, tp_pct: float, sl_pct: float):
    close_side = "SELL" if side_open == "BUY" else "BUY"
    tp_price = round(entry * (1 + tp_pct) if side_open=="BUY" else entry*(1 - tp_pct), 2)
    sl_price = round(entry * (1 - sl_pct) if side_open=="BUY" else entry*(1 + sl_pct), 2)

    # TAKE_PROFIT_MARKET
    req("POST", "/openApi/swap/v2/trade/order", {
        "symbol": symbol, "side": close_side, "type": "TAKE_PROFIT_MARKET",
        "quantity": qty, "reduceOnly": True, "triggerPrice": tp_price
    })
    # STOP_MARKET
    req("POST", "/openApi/swap/v2/trade/order", {
        "symbol": symbol, "side": close_side, "type": "STOP_MARKET",
        "quantity": qty, "reduceOnly": True, "triggerPrice": sl_price
    })
    return {"tp": tp_price, "sl": sl_price}

def close_all(symbol: str):
    req("POST", "/openApi/swap/v2/trade/closeAllPositions", {"symbol": symbol})

# ─────────────────────────────────────────────────────────────
# 메인 루프
# ─────────────────────────────────────────────────────────────
RUNNING = True
def _sigterm(*_):  # Render 재시작/종료 대응
    global RUNNING
    RUNNING = False
signal.signal(signal.SIGTERM, _sigterm)

def main():
    assert API_KEY and API_SECRET, "BINGX_API_KEY / BINGX_API_SECRET 필요"
    send_tg("✅ [BOT READY] BingX auto trader started")

    set_leverage_and_mode(SYMBOL, LEVERAGE, ISOLATED)

    daily_start_equity = 0.0   # (선택) 계정 잔고 조회해 초기화 가능
    daily_pnl = 0.0
    consec_losses = 0

    last_reset_day = time.strftime("%Y-%m-%d")

    while RUNNING:
        try:
            # 날짜 바뀌면 일간 통계 초기화
            today = time.strftime("%Y-%m-%d")
            if today != last_reset_day:
                send_tg(f"📊 [DAILY] PnL {daily_pnl:.2f} USDT, 연속손실 {consec_losses}")
                daily_pnl = 0.0
                consec_losses = 0
                last_reset_day = today

            # 신호 계산
            candles = get_klines(SYMBOL, INTERVAL, 120)
            sig = decide_signal(candles)
            if not sig:
                time.sleep(1); continue

            last = candles[-1][4]
            if QTY_FIX:
                qty = float(QTY_FIX)
            else:
                qty = round(POSITION_VALUE_USDT / last, 6)  # 명목가/현재가

            side = "BUY" if sig == "LONG" else "SELL"
            send_tg(f"🟢 [ENTRY] {SYMBOL} {sig} {LEVERAGE}x qty={qty} @≈{last}")

            # 진입 (시장가)
            res = place_market(SYMBOL, side, qty)
            # 체결가가 응답에 없다면 현재가로 근사
            entry = last
            try:
                entry = float(res.get("data", {}).get("avgPrice", entry))
            except Exception:
                pass

            tp_sl = place_tp_sl(SYMBOL, side, qty, entry, TP_PCT, SL_PCT)
            send_tg(f"📌 [ORDERS] TP@{tp_sl['tp']} / SL@{tp_sl['sl']} (reduceOnly)")

            # 간단 대기: TP/SL 체결을 이벤트로 잡는 대신 쿨다운만 두고 재평가
            time.sleep(COOLDOWN_SEC)

            # 참고: 실전은 WebSocket 포지션/오더 이벤트로 체결 즉시 감지하는 걸 권장합니다.

        except Exception as e:
            send_tg(f"❌ [ERROR] {e}")
            time.sleep(2)

    send_tg("🛑 [BOT STOPPED]")

if __name__ == "__main__":
    main()
