# backtest_realistic.py
# BingX 실제 캔들로 bot.py랑 거의 같은 로직을
# 최근 N일(기본 5일) 동안 돌려보는 간단 백테스트 버전
#
# 백테스트라서 너무 운영용으로 빡빡한 필터들은 일부 뺐다.
# (호가 스프레드, kline 딜레이, 박스장 하루 N회 손절 시 비활성화 같은 것들)

import os
import math
import requests
import datetime
from typing import List, Tuple, Optional

BASE = "https://open-api.bingx.com"
SYMBOL = os.getenv("SYMBOL", "BTC-USDT")

# 며칠 치를 돌릴지
BT_DAYS = int(os.getenv("BT_DAYS", "5"))

# bot.py랑 최대한 맞춰 두는 파라미터들
ENABLE_TREND      = os.getenv("ENABLE_TREND", "1") == "1"
ENABLE_RANGE      = os.getenv("ENABLE_RANGE", "1") == "1"   # 백테스트에선 기본 ON
ENABLE_1M_CONFIRM = os.getenv("ENABLE_1M_CONFIRM", "1") == "1"

TP_PCT  = float(os.getenv("TP_PCT", "0.02"))
SL_PCT  = float(os.getenv("SL_PCT", "0.02"))

# 박스장 전용 TP/SL
RANGE_TP_PCT = float(os.getenv("RANGE_TP_PCT", "0.006"))  # 0.6%
RANGE_SL_PCT = float(os.getenv("RANGE_SL_PCT", "0.004"))  # 0.4%

# ATR 쪽
USE_ATR     = os.getenv("USE_ATR", "1") == "1"
ATR_LEN     = int(os.getenv("ATR_LEN", "20"))
ATR_TP_MULT = float(os.getenv("ATR_TP_MULT", "2.0"))
ATR_SL_MULT = float(os.getenv("ATR_SL_MULT", "1.2"))
MIN_TP_PCT  = float(os.getenv("MIN_TP_PCT", "0.005"))
MIN_SL_PCT  = float(os.getenv("MIN_SL_PCT", "0.005"))

# bot.py에도 있는 쿨다운들
COOLDOWN_AFTER_CLOSE = int(os.getenv("COOLDOWN_AFTER_CLOSE", "30"))    # 한 포지션 끝나고
COOLDOWN_AFTER_3LOSS = int(os.getenv("COOLDOWN_AFTER_3LOSS", "3600"))  # 3연속 손절 후

RSI_OVERBOUGHT = int(os.getenv("RSI_OVERBOUGHT", "70"))
RSI_OVERSOLD   = int(os.getenv("RSI_OVERSOLD", "30"))

# “수익률 × 이 자본” 으로 USDT 환산만 해보려고
BT_CAPITAL = float(os.getenv("BT_CAPITAL", "500"))

KST = datetime.timezone(datetime.timedelta(hours=9))

# ─────────────────────────────
# 공통: 날짜 구간 캔들 가져오기
# ─────────────────────────────
def fetch_klines_range(
    symbol: str,
    interval: str,
    start_ts: int,
    end_ts: int,
    limit: int = 1000,
) -> List[Tuple[int, float, float, float, float]]:
    """
    start_ts ~ end_ts 사이 BingX 캔들 모두 가져오기
    (bot.py가 쓰는 엔드포인트 그대로)
    """
    out: List[Tuple[int, float, float, float, float]] = []
    cursor = start_ts
    while cursor < end_ts:
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
            "startTime": cursor,
            "endTime": end_ts,
        }
        resp = requests.get(
            f"{BASE}/openApi/swap/v2/quote/klines",
            params=params,
            timeout=10,
        )
        data = resp.json().get("data", [])
        if not data:
            break

        batch: List[Tuple[int, float, float, float, float]] = []
        for it in data:
            ts = int(it["time"])
            if ts >= end_ts:
                continue
            batch.append(
                (
                    ts,
                    float(it["open"]),
                    float(it["high"]),
                    float(it["low"]),
                    float(it["close"]),
                )
            )

        batch.sort(key=lambda x: x[0])
        out.extend(batch)

        if len(batch) < limit:
            break

        # 다음 가져올 시작점
        cursor = batch[-1][0] + 1

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

def calc_atr(candles: List[Tuple[int,float,float,float,float]], length: int = 14) -> Optional[float]:
    if len(candles) < length + 1:
        return None
    trs: List[float] = []
    for i in range(1, len(candles)):
        _, _, high, low, close = candles[i]
        _, _, ph, pl, pc = candles[i - 1]
        tr = max(
            high - low,
            abs(high - pc),
            abs(low - pc),
        )
        trs.append(tr)
    if len(trs) < length:
        return None
    return sum(trs[-length:]) / length

# ─────────────────────────────
# 다이버전스용 피벗
# ─────────────────────────────
def _find_last_two_pivot_highs(candles):
    idxs = []
    for i in range(len(candles) - 2, 1, -1):
        if candles[i][2] > candles[i - 1][2] and candles[i][2] > candles[i + 1][2]:
            idxs.append(i)
            if len(idxs) == 2:
                break
    return list(reversed(idxs))

def _find_last_two_pivot_lows(candles):
    idxs = []
    for i in range(len(candles) - 2, 1, -1):
        if candles[i][3] < candles[i - 1][3] and candles[i][3] < candles[i + 1][3]:
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
    rsi_up = rsi_vals[i2] > rsi_vals[i1]
    return price_down and rsi_up

# ─────────────────────────────
# 신호 판단 (bot.py 버전의 “느슨한” 쪽)
# ─────────────────────────────
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

    # 이평이 너무 붙으면 횡보
    spread_ratio = abs(e20n - e50n) / e50n
    if spread_ratio < 0.0005:
        return None

    # 마지막 캔들 변동폭도 너무 작으면 패스
    last = candles[-1]
    last_range_pct = (last[2] - last[3]) / last[3] if last[3] else 0
    if last_range_pct < 0.001:
        return None

    long_sig  = (e20p < e50p) and (e20n > e50n) and (r_now < rsi_overbought)
    short_sig = (e20p > e50p) and (e20n < e50n) and (r_now > rsi_oversold)

    # 가격이 이평 반대편이면 무효
    if long_sig and price < e50n:
        long_sig = False
    if short_sig and price > e50n:
        short_sig = False

    # RSI 다이버전스 반대면 패스
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
    # 박스가 아예 너무 좁으면 안 한다
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

# ─────────────────────────────
# 하루치 돌리는 함수
# ─────────────────────────────
def run_one_day(day_start_kst, candles_1m, candles_3m, candles_15m):
    day_end_kst = day_start_kst + datetime.timedelta(days=1)
    start_ts = int(day_start_kst.astimezone(datetime.timezone.utc).timestamp() * 1000)
    end_ts   = int(day_end_kst.astimezone(datetime.timezone.utc).timestamp() * 1000)

    # 그날 3m만 “진입 타이밍”으로 쓴다
    day_3m = [c for c in candles_3m if start_ts <= c[0] < end_ts]

    open_trade = None
    last_close_sec = 0
    consec_losses = 0
    cooldown_until = 0
    results = []

    for c in day_3m:
        ts_ms, o, h, l, close = c
        now_sec = ts_ms // 1000

        # 열려 있으면 TP/SL 먼저 체크
        if open_trade is not None:
            side = open_trade["side"]
            tp   = open_trade["tp_price"]
            sl   = open_trade["sl_price"]
            entry = open_trade["entry"]
            reason = None
            pnl_pct = 0.0

            if side == "BUY":
                if l <= sl:
                    pnl_pct = (sl - entry) / entry
                    reason = "SL"
                elif h >= tp:
                    pnl_pct = (tp - entry) / entry
                    reason = "TP"
            else:  # SELL
                if h >= sl:
                    pnl_pct = (entry - sl) / entry
                    reason = "SL"
                elif l <= tp:
                    pnl_pct = (entry - tp) / entry
                    reason = "TP"

            if reason:
                results.append({
                    "time": ts_ms,
                    "reason": reason,
                    "source": open_trade["source"],
                    "side": side,
                    "entry": entry,
                    "exit": tp if reason == "TP" else sl,
                    "pnl_pct": pnl_pct,
                })
                open_trade = None
                last_close_sec = now_sec

                if pnl_pct < 0:
                    consec_losses += 1
                    if consec_losses >= 3:
                        cooldown_until = now_sec + COOLDOWN_AFTER_3LOSS
                else:
                    consec_losses = 0

                continue  # 이 캔들에서는 더 안 봄

        # 포지션 없는데 쿨다운이면 패스
        if open_trade is None:
            if now_sec < cooldown_until:
                continue
            if last_close_sec > 0 and (now_sec - last_close_sec) < COOLDOWN_AFTER_CLOSE:
                continue

        if open_trade is not None:
            continue

        # 이 시점까지의 캔들 잘라오기
        upto_3m  = [x for x in candles_3m  if x[0] <= ts_ms]
        upto_15m = [x for x in candles_15m if x[0] <= ts_ms]
        upto_1m  = [x for x in candles_1m  if x[0] <= ts_ms]

        chosen_signal = None
        signal_source = None

        # 1) 추세 우선
        if ENABLE_TREND and len(upto_15m) >= 50:
            sig_3m = decide_signal_3m_trend(upto_3m, RSI_OVERBOUGHT, RSI_OVERSOLD)
            trend_15 = decide_trend_15m(upto_15m)
            if sig_3m and trend_15 and sig_3m == trend_15:
                chosen_signal = sig_3m
                signal_source = "TREND"
                if ENABLE_1M_CONFIRM and len(upto_1m) >= 2:
                    last_1m = upto_1m[-1]
                    # 롱인데 1분봉이 음봉이면 패스
                    if chosen_signal == "LONG" and last_1m[4] < last_1m[1]:
                        chosen_signal = None
                    # 숏인데 1분봉이 양봉이면 패스
                    elif chosen_signal == "SHORT" and last_1m[4] > last_1m[1]:
                        chosen_signal = None

        # 2) 박스장
        if (not chosen_signal) and ENABLE_RANGE:
            sig_r = decide_signal_range(upto_3m)
            if sig_r:
                chosen_signal = sig_r
                signal_source = "RANGE"

        if not chosen_signal:
            continue

        # 진입가
        entry_price = close

        # TP/SL 계산
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

    return results

# ─────────────────────────────
# 메인
# ─────────────────────────────
def main():
    # 오늘 KST 기준으로 최근 BT_DAYS일
    today_kst = datetime.datetime.now(KST).date()
    first_day_kst = today_kst - datetime.timedelta(days=BT_DAYS)

    # 전체 기간 한 번에 캔들 다 받아 놓고, 하루씩 잘라서 씀
    overall_start_kst = datetime.datetime.combine(
        first_day_kst, datetime.time(0, 0, 0), tzinfo=KST
    )
    overall_end_kst = datetime.datetime.combine(
        today_kst + datetime.timedelta(days=1),
        datetime.time(0, 0, 0),
        tzinfo=KST,
    )

    start_ts = int(overall_start_kst.astimezone(datetime.timezone.utc).timestamp() * 1000)
    end_ts   = int(overall_end_kst.astimezone(datetime.timezone.utc).timestamp() * 1000)

    # 인디케이터 여유분 주려고 조금 더 앞에서부터
    candles_3m = fetch_klines_range(
        SYMBOL, "3m",
        start_ts - 3 * 60 * 1000 * 200,
        end_ts,
        limit=800,
    )
    candles_15m = fetch_klines_range(
        SYMBOL, "15m",
        start_ts - 15 * 60 * 1000 * 200,
        end_ts,
        limit=800,
    )
    candles_1m = fetch_klines_range(
        SYMBOL, "1m",
        start_ts - 60 * 1000 * 200,
        end_ts,
        limit=1000,
    )

    print("===== 최근 {}일 BingX BTC-USDT 시뮬 =====".format(BT_DAYS))
    all_results = []
    for d in range(BT_DAYS):
        day = today_kst - datetime.timedelta(days=BT_DAYS - d)
        day_start_kst = datetime.datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=KST)
        day_results = run_one_day(day_start_kst, candles_1m, candles_3m, candles_15m)

        day_pct = sum(r["pnl_pct"] for r in day_results)
        print(f"\n--- {day.strftime('%Y-%m-%d')} KST ---")
        print(f"거래 수: {len(day_results)} | 수익률 합계: {day_pct*100:.3f}%")
        for r in day_results:
            dt_kst = datetime.datetime.fromtimestamp(r["time"]/1000, tz=datetime.timezone.utc).astimezone(KST)
            print(
                f"{dt_kst} | {r['reason']} | {r['source']} | {r['side']} "
                f"| entry={r['entry']} -> {r['exit']} | pnl={r['pnl_pct']*100:.3f}%"
            )
        all_results.extend(day_results)

    total_pct = sum(r["pnl_pct"] for r in all_results)
    total_usdt = BT_CAPITAL * total_pct
    krw = total_usdt * 1400

    print("\n===== 전체 합계 =====")
    print(f"총 거래 수: {len(all_results)}")
    print(f"총 수익률: {total_pct*100:.3f}% (기준자본 {BT_CAPITAL} USDT → {total_usdt:.3f} USDT, 한화 약 {krw:,.0f}원)")


if __name__ == "__main__":
    main()
