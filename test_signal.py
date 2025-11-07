# test_signal_tp_sl.py
import requests, math
from typing import List, Tuple, Optional

BASE = "https://open-api.bingx.com"
SYMBOL = "BTC-USDT"

# ===== 인디케이터 =====
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

def get_klines(symbol: str, interval: str, limit: int = 120):
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

# ===== 다이버전스용 보조 =====
def _find_last_two_pivot_highs(candles):
    idxs = []
    for i in range(len(candles) - 2, 1, -1):
        if candles[i][2] > candles[i-1][2] and candles[i][2] > candles[i+1][2]:
            idxs.append(i)
            if len(idxs) == 2:
                break
    return list(reversed(idxs))

def _find_last_two_pivot_lows(candles):
    idxs = []
    for i in range(len(candles) - 2, 1, -1):
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

# ===== 전략 판단 =====
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

    # 이평이 너무 붙어 있으면 횡보
    spread_ratio = abs(e20n - e50n) / e50n
    if spread_ratio < 0.0005:
        return None

    # 마지막 캔들이 너무 안 움직였으면 스킵
    last = candles[-1]
    last_range_pct = (last[2] - last[3]) / last[3] if last[3] else 0
    if last_range_pct < 0.001:
        return None

    long_sig  = (e20p < e50p) and (e20n > e50n) and (r_now < rsi_overbought)
    short_sig = (e20p > e50p) and (e20n < e50n) and (r_now > rsi_oversold)

    # 가격 위치 체크
    if long_sig and price < e50n:
        long_sig = False
    if short_sig and price > e50n:
        short_sig = False

    # 다이버전스 반대면 스킵
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

def calc_atr(candles, length=20):
    if len(candles) < length + 1:
        return None
    trs = []
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
    return sum(trs[-length:]) / length

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
    # 봇과 같은 파라미터
    TP_PCT = 0.02
    SL_PCT = 0.02
    USE_ATR = 1
    ATR_LEN = 20
    ATR_TP_MULT = 2.0
    ATR_SL_MULT = 1.2
    MIN_TP_PCT = 0.005
    MIN_SL_PCT = 0.005
    RANGE_TP_PCT = 0.006
    RANGE_SL_PCT = 0.004

    c3 = get_klines(SYMBOL, "3m", 120)
    c15 = get_klines(SYMBOL, "15m", 120)

    last_price = c3[-1][4]

    sig_3m = decide_signal_3m_trend(c3)
    trend_15 = decide_trend_15m(c15)

    if sig_3m and trend_15 and sig_3m == trend_15:
        # 추세장 신호
        chosen_signal = sig_3m
        signal_source = "TREND"

        # TP/SL 계산
        local_tp_pct = TP_PCT
        local_sl_pct = SL_PCT
        if USE_ATR:
            atr = calc_atr(c3, ATR_LEN)
            if atr and last_price > 0:
                sl_pct_atr = (atr * ATR_SL_MULT) / last_price
                tp_pct_atr = (atr * ATR_TP_MULT) / last_price
                local_sl_pct = max(sl_pct_atr, MIN_SL_PCT)
                local_tp_pct = max(tp_pct_atr, MIN_TP_PCT)
    else:
        # 추세 신호 없으면 박스장 신호 보기
        sig_r = decide_signal_range(c3)
        if sig_r:
            chosen_signal = sig_r
            signal_source = "RANGE"
            local_tp_pct = RANGE_TP_PCT
            local_sl_pct = RANGE_SL_PCT
        else:
            print("지금은 진입 신호 없음")
            return

    # 여기까지 왔으면 진입 신호가 있다는 뜻
    side = "BUY" if chosen_signal == "LONG" else "SELL"

    if side == "BUY":
        tp_price = round(last_price * (1 + local_tp_pct), 2)
        sl_price = round(last_price * (1 - local_sl_pct), 2)
    else:
        tp_price = round(last_price * (1 - local_tp_pct), 2)
        sl_price = round(last_price * (1 + local_sl_pct), 2)

    strategy_kr = "추세장" if signal_source == "TREND" else "박스장"
    direction_kr = "롱" if chosen_signal == "LONG" else "숏"

    print(f"[신호 발견] 전략={strategy_kr}, 방향={direction_kr}")
    print(f"현재가: {last_price}")
    print(f"TP 퍼센트: {local_tp_pct*100:.3f}%, TP 가격: {tp_price}")
    print(f"SL 퍼센트: {local_sl_pct*100:.3f}%, SL 가격: {sl_price}")

if __name__ == "__main__":
    main()
