# bot.py
# BingX Auto Trader (Render Background Worker)
# - EMA20/EMA50 크로스 + RSI 필터
# - 진입 즉시 TP/SL 예약 (reduceOnly)
# - TP/SL 체결 시 fill 조회해서 텔레그램으로 PnL 보고
# - Render가 SIGTERM 보낸 종료는 텔레그램에 STOP 안 보냄

import os, time, hmac, hashlib, math, signal
from typing import Any, Dict, List, Tuple
import requests

# ─────────────────────────────
# 시작 시각 / STOP 필터
# ─────────────────────────────
START_TS = time.time()
MIN_UPTIME_FOR_STOP = int(os.getenv("MIN_UPTIME_FOR_STOP", "5"))
TERMINATED_BY_SIGNAL = False  # ← SIGTERM 종료 여부

# ─────────────────────────────
# 환경변수
# ─────────────────────────────
API_KEY    = os.getenv("BINGX_API_KEY", "")
API_SECRET = os.getenv("BINGX_API_SECRET", "")

SYMBOL   = os.getenv("SYMBOL", "BTC-USDT")   # 필요하면 BTCUSDT 로 변경
INTERVAL = os.getenv("INTERVAL", "3m")
LEVERAGE = int(os.getenv("LEVERAGE", "30"))
ISOLATED = os.getenv("ISOLATED", "1") == "1"

TP_PCT   = float(os.getenv("TP_PCT", "0.012"))   # +1.2%
SL_PCT   = float(os.getenv("SL_PCT", "0.006"))   # -0.6%
COOLDOWN_SEC   = int(os.getenv("COOLDOWN_SEC", "15"))
POLL_FILLS_SEC = int(os.getenv("POLL_FILLS_SEC", "2"))

POSITION_VALUE_USDT = float(os.getenv("POSITION_VALUE_USDT", "50"))
QTY_FIX = os.getenv("QTY_FIX")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

BASE = "https://open-api.bingx.com"

# TP/SL 추적용
OPEN_TRADES: List[Dict[str, Any]] = []

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
# 마켓 데이터
# ─────────────────────────────
def get_klines(symbol: str, interval: str, limit: int = 120) -> List[Tuple[int, float, float, float, float]]:
    resp = requests.get(
        f"{BASE}/openApi/swap/v2/quote/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
        timeout=12,
    )
    try:
        print("[KLINES RAW]", resp.text[:500])
    except Exception:
        print("[KLINES RAW] <cannot print>")

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
                o = float(it.get("open"))
                h = float(it.get("high"))
                l = float(it.get("low"))
                c = float(it.get("close"))
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
# BingX 거래 관련
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

    tp_res = req("POST", "/openApi/swap/v2/trade/order", {
        "symbol": symbol,
        "side": close_side,
        "type": "TAKE_PROFIT_MARKET",
        "quantity": qty,
        "reduceOnly": True,
        "triggerPrice": tp_price,
    })
    sl_res = req("POST", "/openApi/swap/v2/trade/order", {
        "symbol": symbol,
        "side": close_side,
        "type": "STOP_MARKET",
        "quantity": qty,
        "reduceOnly": True,
        "triggerPrice": sl_price,
    })
    return {
        "tp": tp_price,
        "sl": sl_price,
        "tp_order_id": (tp_res.get("data") or {}).get("orderId"),
        "sl_order_id": (sl_res.get("data") or {}).get("orderId"),
    }

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

def check_closes() -> List[Dict[str, Any]]:
    global OPEN_TRADES
    if not OPEN_TRADES:
        return []
    still_open = []
    closed_results: List[Dict[str, Any]] = []

    for t in OPEN_TRADES:
        symbol = t["symbol"]
        closed = False
        tp_id = t.get("tp_order_id")
        sl_id = t.get("sl_order_id")

        if tp_id:
            try:
                o = get_order(symbol, tp_id)
                status = (o.get("data") or {}).get("status") or o.get("status")
                if status == "FILLED":
                    summary = summarize_fills(symbol, tp_id)
                    closed_results.append({"trade": t, "reason": "TP", "summary": summary})
                    closed = True
            except Exception as e:
                print("check_closes TP error:", e)

        if (not closed) and sl_id:
            try:
                o = get_order(symbol, sl_id)
                status = (o.get("data") or {}).get("status") or o.get("status")
                if status == "FILLED":
                    summary = summarize_fills(symbol, sl_id)
                    closed_results.append({"trade": t, "reason": "SL", "summary": summary})
                    closed = True
            except Exception as e:
                print("check_closes SL error:", e)

        if not closed:
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
    assert API_KEY and API_SECRET, "BINGX_API_KEY / BINGX_API_SECRET 필요"
    send_tg("✅ [BOT READY] BingX auto trader started")

    set_leverage_and_mode(SYMBOL, LEVERAGE, ISOLATED)

    last_reset_day = time.strftime("%Y-%m-%d")
    daily_pnl = 0.0
    consec_losses = 0
    last_fill_check = 0.0

    while RUNNING:
        try:
            # 날짜 바뀌면 데일리 리포트
            today = time.strftime("%Y-%m-%d")
            if today != last_reset_day:
                send_tg(f"📊 [DAILY] PnL {daily_pnl:.2f} USDT, 연속손실 {consec_losses}")
                daily_pnl = 0.0
                consec_losses = 0
                last_reset_day = today

            # TP/SL 체결 확인
            now = time.time()
            if now - last_fill_check >= POLL_FILLS_SEC:
                closed_list = check_closes()
                for c in closed_list:
                    t = c["trade"]
                    reason = c["reason"]
                    summary = c["summary"] or {}
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
                    if pnl < 0:
                        consec_losses += 1
                    else:
                        consec_losses = 0
                    send_tg(
                        f"💰 [CLOSE-{reason}] {t['symbol']} {t['side']} "
                        f"qty={closed_qty} @{closed_price:.2f} "
                        f"PnL={pnl:.2f} USDT (오늘누계 {daily_pnl:.2f})"
                    )
                last_fill_check = now

            # 진입 신호 체크
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
            entry_order_id = (res.get("data") or {}).get("orderId")
            try:
                entry = float((res.get("data") or {}).get("avgPrice", entry))
            except Exception:
                pass

            tp_sl = place_tp_sl(SYMBOL, side, qty, entry, TP_PCT, SL_PCT)
            send_tg(f"📌 [ORDERS] TP@{tp_sl['tp']} / SL@{tp_sl['sl']} (reduceOnly)")

            OPEN_TRADES.append({
                "symbol": SYMBOL,
                "side": side,
                "qty": qty,
                "entry": entry,
                "entry_order_id": entry_order_id,
                "tp_order_id": tp_sl["tp_order_id"],
                "sl_order_id": tp_sl["sl_order_id"],
                "tp_price": tp_sl["tp"],
                "sl_price": tp_sl["sl"],
            })

            time.sleep(COOLDOWN_SEC)

        except Exception as e:
            print("ERROR:", e)
            send_tg(f"❌ [ERROR] {e}")
            time.sleep(2)

    uptime = time.time() - START_TS
    if (not TERMINATED_BY_SIGNAL) and (uptime >= MIN_UPTIME_FOR_STOP):
        send_tg("🛑 [BOT STOPPED]")
    else:
        print(f"[SKIP STOP] sig={TERMINATED_BY_SIGNAL} uptime={uptime:.2f}s")

if __name__ == "__main__":
    main()
