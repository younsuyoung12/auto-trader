# backtest_from_api.py
import requests, math, datetime, time

BASE = "https://open-api.bingx.com"
SYMBOL = "BTC-USDT"

# 설정 (봇이랑 맞춰둠)
RANGE_TP_PCT = 0.006   # 0.6%
RANGE_SL_PCT = 0.004   # 0.4%
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30

def get_klines(symbol: str, interval: str, limit: int = 200):
    r = requests.get(
        f"{BASE}/openApi/swap/v2/quote/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
        timeout=10,
    )
    data = r.json().get("data", [])
    out = []
    for it in data:
        out.append((
            int(it["time"]),
            float(it["open"]),
            float(it["high"]),
            float(it["low"]),
            float(it["close"]),
        ))
    out.sort(key=lambda x: x[0])
    return out

def ema(values, length):
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

def rsi(closes, length=14):
    if len(closes) < length + 1:
        return [math.nan] * len(closes)
    gains, losses = [], []
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i-1]
        gains.append(max(ch, 0.0))
        losses.append(max(-ch, 0.0))
    avg_gain = sum(gains[:length]) / length
    avg_loss = sum(losses[:length]) / length
    rsis = [math.nan] * length
    rs = (avg_gain / avg_loss) if avg_loss > 0 else float("inf")
    rsis.append(100 - 100 / (1 + rs))
    for i in range(length, len(gains)):
        avg_gain = (avg_gain*(length-1) + gains[i]) / length
        avg_loss = (avg_loss*(length-1) + losses[i]) / length
        rs = (avg_gain / avg_loss) if avg_loss > 0 else float("inf")
        rsis.append(100 - 100 / (1 + rs))
    return rsis

# --- 봇이랑 같은 신호들 ---

def decide_signal_3m_trend(candles):
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

    # 이평 너무 붙어있으면 스킵
    spread_ratio = abs(e20n - e50n) / e50n
    if spread_ratio < 0.0005:
        return None

    # 마지막 캔들 변동폭 작으면 스킵
    last = candles[-1]
    last_range_pct = (last[2] - last[3]) / last[3] if last[3] else 0
    if last_range_pct < 0.001:
        return None

    long_sig  = (e20p < e50p) and (e20n > e50n) and (r_now < RSI_OVERBOUGHT)
    short_sig = (e20p > e50p) and (e20n < e50n) and (r_now > RSI_OVERSOLD)

    if long_sig and price < e50n:
        long_sig = False
    if short_sig and price > e50n:
        short_sig = False

    if long_sig:  return "LONG"
    if short_sig: return "SHORT"
    return None

def decide_trend_15m(candles_15m):
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

def decide_signal_range(candles):
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
    if box_pct < 0.0015:
        return None

    upper = lo + box_h * 0.75
    lower = lo + box_h * 0.25
    r_now = r[-1]

    if now >= upper and r_now > 60:
        return "SHORT"
    if now <= lower and r_now < 40:
        return "LONG"
    return None

def main():
    # 오늘 3m, 15m 실제로 긁어오기
    c3 = get_klines(SYMBOL, "3m", 400)   # 대충 하루치
    c15 = get_klines(SYMBOL, "15m", 200)

    # 3m 한 캔들씩 진행하면서 시뮬
    open_pos = None   # {"side": "LONG/SHORT", "entry": float, "tp": float, "sl": float, "src": "RANGE/TREND"}
    trades = []

    for i in range(60, len(c3)):  # 인디케이터 워밍업 구간 넘기기
        now_candle = c3[i]
        now_price = now_candle[4]

        # 1) 포지션 열려있으면 TP/SL 먼저 확인
        if open_pos:
            if open_pos["side"] == "LONG":
                # 위로 가면 TP, 아래로 가면 SL
                high = now_candle[2]; low = now_candle[3]
                if high >= open_pos["tp"]:
                    trades.append(("TP", open_pos))
                    open_pos = None
                elif low <= open_pos["sl"]:
                    trades.append(("SL", open_pos))
                    open_pos = None
            else:  # SHORT
                high = now_candle[2]; low = now_candle[3]
                if low <= open_pos["tp"]:
                    trades.append(("TP", open_pos))
                    open_pos = None
                elif high >= open_pos["sl"]:
                    trades.append(("SL", open_pos))
                    open_pos = None

        # 2) 포지션 없으면 진입 신호 찾기
        if open_pos is None:
            recent_3m = c3[: i+1]
            sig_3m = decide_signal_3m_trend(recent_3m)
            trend_15 = decide_trend_15m(c15)
            chosen = None
            src = None

            if sig_3m and trend_15 and sig_3m == trend_15:
                chosen = sig_3m
                src = "TREND"
            else:
                sig_r = decide_signal_range(recent_3m)
                if sig_r:
                    chosen = sig_r
                    src = "RANGE"

            if chosen:
                if chosen == "LONG":
                    entry = now_price
                    if src == "RANGE":
                        tp = round(entry * (1 + RANGE_TP_PCT), 2)
                        sl = round(entry * (1 - RANGE_SL_PCT), 2)
                    else:
                        tp = round(entry * 1.02, 2)
                        sl = round(entry * 0.98, 2)
                    open_pos = {"side": "LONG", "entry": entry, "tp": tp, "sl": sl, "src": src}
                else:
                    entry = now_price
                    if src == "RANGE":
                        tp = round(entry * (1 - RANGE_TP_PCT), 2)
                        sl = round(entry * (1 + RANGE_SL_PCT), 2)
                    else:
                        tp = round(entry * 0.98, 2)
                        sl = round(entry * 1.02, 2)
                    open_pos = {"side": "SHORT", "entry": entry, "tp": tp, "sl": sl, "src": src}

    # 결과 출력
    print("===== 오늘 실제 캔들로 대략 시뮬 =====")
    print("총 거래 수:", len(trades))
    tp_cnt = sum(1 for t,_ in trades)
    sl_cnt = len(trades) - tp_cnt
    print("TP:", tp_cnt, " / SL:", sl_cnt)

    # 퍼센트 합산 (1회당 명목가 1이라고 가정해서 %만)
    total_pct = 0.0
    for kind, pos in trades:
        if pos["src"] == "RANGE":
            if kind == "TP":
                total_pct += RANGE_TP_PCT
            else:
                total_pct -= RANGE_SL_PCT
        else:
            # 추세는 여기선 고정 2%/-2%로 둠
            if kind == "TP":
                total_pct += 0.02
            else:
                total_pct -= 0.02

    print(f"대략 수익률 합계: {total_pct*100:.2f}%")
    for kind, pos in trades:
        print(kind, pos["src"], pos["side"], pos["entry"], "->", pos["tp"] if kind=="TP" else pos["sl"])

if __name__ == "__main__":
    main()
