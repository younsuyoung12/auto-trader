# backtest_realistic.py
import os, math, requests, datetime
from typing import List, Tuple, Optional

BASE = "https://open-api.bingx.com"
SYMBOL = os.getenv("SYMBOL", "BTC-USDT")

# 봇이랑 똑같이 가져올 설정
ENABLE_TREND      = os.getenv("ENABLE_TREND", "1") == "1"
ENABLE_RANGE      = os.getenv("ENABLE_RANGE", "1") == "1"
ENABLE_1M_CONFIRM = os.getenv("ENABLE_1M_CONFIRM", "1") == "1"

TP_PCT  = float(os.getenv("TP_PCT", "0.02"))
SL_PCT  = float(os.getenv("SL_PCT", "0.02"))
RANGE_TP_PCT = float(os.getenv("RANGE_TP_PCT", "0.006"))
RANGE_SL_PCT = float(os.getenv("RANGE_SL_PCT", "0.004"))

USE_ATR     = os.getenv("USE_ATR", "1") == "1"
ATR_LEN     = int(os.getenv("ATR_LEN", "20"))
ATR_TP_MULT = float(os.getenv("ATR_TP_MULT", "2.0"))
ATR_SL_MULT = float(os.getenv("ATR_SL_MULT", "1.2"))
MIN_TP_PCT  = float(os.getenv("MIN_TP_PCT", "0.005"))
MIN_SL_PCT  = float(os.getenv("MIN_SL_PCT", "0.005"))

COOLDOWN_AFTER_CLOSE = int(os.getenv("COOLDOWN_AFTER_CLOSE", "30"))  # 초
COOLDOWN_AFTER_3LOSS = int(os.getenv("COOLDOWN_AFTER_3LOSS", "3600"))

RSI_OVERBOUGHT = int(os.getenv("RSI_OVERBOUGHT", "70"))
RSI_OVERSOLD   = int(os.getenv("RSI_OVERSOLD", "30"))

TRADING_SESSIONS_UTC = os.getenv("TRADING_SESSIONS_UTC", "0-23")

# 백테스트 때 몇 USDT 들고 있다고 가정할지
BT_CAPITAL = float(os.getenv("BT_CAPITAL", "500"))

def fetch_klines(symbol: str, interval: str, limit: int = 1000):
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

def calc_atr(candles: List[Tuple[int,float,float,float,float]], length: int = 14) -> Optional[float]:
    if len(candles) < length + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        _, _, high, low, close = candles[i]
        _, _, ph, pl, pc = candles[i-1]
        tr = max(high - low, abs(high - pc), abs(low - pc))
        trs.append(tr)
    if len(trs) < length:
        return None
    return sum(trs[-length:]) / length

def _parse_sessions(spec: str):
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

def in_trading_session_utc(ts_ms: int) -> bool:
    dt = datetime.datetime.utcfromtimestamp(ts_ms / 1000)
    h = dt.hour
    for s, e in SESSIONS:
        if s <= h <= e:
            return True
    return False

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

def decide_signal_3m_trend(candles, rsi_overbought=70, rsi_oversold=30):
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

    spread_ratio = abs(e20n - e50n) / e50n
    if spread_ratio < 0.0005:
        return None

    last = candles[-1]
    last_range_pct = (last[2] - last[3]) / last[3] if last[3] else 0
    if last_range_pct < 0.001:
        return None

    long_sig  = (e20p < e50p) and (e20n > e50n) and (r_now < rsi_overbought)
    short_sig = (e20p > e50p) and (e20n < e50n) and (r_now > rsi_oversold)

    if long_sig and price < e50n:
        long_sig = False
    if short_sig and price > e50n:
        short_sig = False

    if long_sig:
        if has_bearish_rsi_divergence(candles, r):
            return None
        return "LONG"
    if short_sig:
        if has_bullish_rsi_divergence(candles, r):
            return None
        return "SHORT"
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
    upper_line = lo + box_h * 0.75
    lower_line = lo + box_h * 0.25
    r_now = r[-1]
    if now >= upper_line and r_now > 60:
        return "SHORT"
    if now <= lower_line and r_now < 40:
        return "LONG"
    return None

def main():
    candles_3m = fetch_klines(SYMBOL, "3m", 400)
    candles_15m = fetch_klines(SYMBOL, "15m", 200)
    candles_1m = fetch_klines(SYMBOL, "1m", 400)

    today_utc = datetime.datetime.utcnow().date()
    day_3m = [c for c in candles_3m if datetime.datetime.utcfromtimestamp(c[0]/1000).date() == today_utc]

    open_trade = None
    last_close_ts = 0  # sec
    consec_losses = 0
    cooldown_until = 0  # sec

    results = []

    for i, c in enumerate(day_3m):
        ts_ms, o, h, l, close = c
        now_sec = ts_ms // 1000

        if not in_trading_session_utc(ts_ms):
            continue

        # 열려있는 포지션 체크 -> TP / SL 맞았는지
        if open_trade is not None:
            side = open_trade["side"]
            tp = open_trade["tp_price"]
            sl = open_trade["sl_price"]
            entry = open_trade["entry"]
            reason = None
            pnl_pct = 0.0

            if side == "BUY":
                hit_sl = l <= sl
                hit_tp = h >= tp
                if hit_sl:
                    pnl_pct = (sl - entry) / entry
                    reason = "SL"
                elif hit_tp:
                    pnl_pct = (tp - entry) / entry
                    reason = "TP"
            else:
                hit_sl = h >= sl
                hit_tp = l <= tp
                if hit_sl:
                    pnl_pct = (entry - sl) / entry
                    reason = "SL"
                elif hit_tp:
                    pnl_pct = (entry - tp) / entry
                    reason = "TP"

            if reason:
                results.append({
                    "time": ts_ms,
                    "reason": reason,
                    "source": open_trade["source"],
                    "side": side,
                    "entry": entry,
                    "exit": tp if reason=="TP" else sl,
                    "pnl_pct": pnl_pct,
                })
                open_trade = None
                last_close_ts = now_sec
                if pnl_pct < 0:
                    consec_losses += 1
                    if consec_losses >= 3:
                        cooldown_until = now_sec + COOLDOWN_AFTER_3LOSS
                else:
                    consec_losses = 0
                continue  # 이 캔들은 청산으로 끝

        # 포지션 없고, 쿨다운이면 스킵
        if open_trade is None:
            if now_sec < cooldown_until:
                continue
            if last_close_ts > 0 and (now_sec - last_close_ts) < COOLDOWN_AFTER_CLOSE:
                continue

        # 여기까지 왔는데 포지션 열려 있으면 스킵
        if open_trade is not None:
            continue

        # 3m 캔들 i 까지로 신호 만들기
        upto_3m = day_3m[:i+1]

        chosen_signal = None
        signal_source = None

        # 추세 모드
        if ENABLE_TREND:
            upto_15m = [x for x in candles_15m if x[0] <= ts_ms]
            if len(upto_15m) >= 50:
                sig_3m = decide_signal_3m_trend(upto_3m, RSI_OVERBOUGHT, RSI_OVERSOLD)
                trend_15 = decide_trend_15m(upto_15m)
                if sig_3m and trend_15 and sig_3m == trend_15:
                    chosen_signal = sig_3m
                    signal_source = "TREND"
                    if ENABLE_1M_CONFIRM:
                        upto_1m = [x for x in candles_1m if x[0] <= ts_ms]
                        if len(upto_1m) >= 2:
                            last_1m = upto_1m[-1]
                            if chosen_signal == "LONG" and last_1m[4] < last_1m[1]:
                                chosen_signal = None
                            elif chosen_signal == "SHORT" and last_1m[4] > last_1m[1]:
                                chosen_signal = None

        # 박스장 모드
        if (not chosen_signal) and ENABLE_RANGE:
            sig_r = decide_signal_range(upto_3m)
            if sig_r:
                chosen_signal = sig_r
                signal_source = "RANGE"

        if not chosen_signal:
            continue

        entry_price = close

        if signal_source == "RANGE":
            tp_pct = RANGE_TP_PCT
            sl_pct = RANGE_SL_PCT
        else:
            tp_pct = TP_PCT
            sl_pct = SL_PCT
            if USE_ATR:
                atr = calc_atr(upto_3m, ATR_LEN)
                if atr and entry_price > 0:
                    sl_pct_atr = (atr * ATR_SL_MULT) / entry_price
                    tp_pct_atr = (atr * ATR_TP_MULT) / entry_price
                    sl_pct = max(sl_pct_atr, MIN_SL_PCT)
                    tp_pct = max(tp_pct_atr, MIN_TP_PCT)

        if chosen_signal == "LONG":
            tp_price = round(entry_price * (1 + tp_pct), 2)
            sl_price = round(entry_price * (1 - sl_pct), 2)
            side = "BUY"
        else:
            tp_price = round(entry_price * (1 - tp_pct), 2)
            sl_price = round(entry_price * (1 + sl_pct), 2)
            side = "SELL"

        open_trade = {
            "side": side,
            "entry": entry_price,
            "tp_price": tp_price,
            "sl_price": sl_price,
            "source": signal_source,
        }

    # 끝나고 결과 출력
    tp_cnt = sum(1 for r in results if r["reason"] == "TP")
    sl_cnt = sum(1 for r in results if r["reason"] == "SL")
    total_pct = sum(r["pnl_pct"] for r in results)
    total_usdt = BT_CAPITAL * total_pct
    krw = total_usdt * 1400  # 대충

    print("===== 오늘 실제 캔들로 시뮬(봇 로직 근접) =====")
    print(f"총 거래 수: {len(results)}")
    print(f"TP: {tp_cnt} / SL: {sl_cnt}")
    print(f"수익률 합계: {total_pct*100:.3f}% (기준자본 {BT_CAPITAL} USDT → {total_usdt:.3f} USDT, 한화 약 {krw:,.0f}원)")
    for r in results:
        dt = datetime.datetime.utcfromtimestamp(r["time"]/1000)
        print(f"{dt} | {r['reason']} | {r['source']} | {r['side']} | entry={r['entry']} -> {r['exit']} | pnl={r['pnl_pct']*100:.3f}%")

if __name__ == "__main__":
    main()
