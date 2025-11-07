# bot.py
# BingX Auto Trader (Render Background Worker)
# 이 버전에서 들어있는 것들:
# - 3분봉 EMA20/50 + RSI 신호
# - 3분봉 신호가 나와도 15분봉 방향과 같을 때만 진입 (MTF 필터)
# - RSI 다이버전스가 신호와 반대면 진입 안 함
# - 횡보(이평이 붙거나 캔들 변동폭이 작으면) 진입 안 함
# - 옵션: 박스장(레인지) 전략 ON/OFF
# - 진입 시 TP/SL 예약 (고정 % 또는 ATR 기반)
# - 포지션 1개만 운용, 청산 전에는 재진입 안 함
# - 3회 연속 손실이면 일정 시간 휴식
# - 시작 시 거래소 포지션/열린 주문 동기화
#   ↳ 일부 계정에서 /trade/positions 가 100400 주면 자동으로 건너뜀
# - TP/SL 중 한쪽이 사라지면 다시 걸어줌
# - 진입 주문이 타임아웃 나면 강제 취소
# - /healthz, /metrics 붙일 수 있는 HTTP 서버(옵션)
# - 일정 주기마다 잔고 로그 남김
# - BingX 잔고 응답이 중첩(dict 안에 dict)일 때도 파싱되게 함
# - 환경변수에 한글/공백 들어 있어도 죽지 않도록 완화
# - 📌 (추가) 박스장일 때는 별도 TP/SL 퍼센트 사용
# - 📌 (추가) 텔레그램에 "추세장/박스장" + "롱/숏" 한글로 전송

import os, time, hmac, hashlib, math, signal, random, datetime, threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, List, Tuple, Optional
import requests

# ─────────────────────────────
# 기본 런타임 상태
# ─────────────────────────────
START_TS = time.time()
MIN_UPTIME_FOR_STOP = int(os.getenv("MIN_UPTIME_FOR_STOP", "5"))
TERMINATED_BY_SIGNAL = False

# ─────────────────────────────
# 환경변수 설정
# ─────────────────────────────
API_KEY    = os.getenv("BINGX_API_KEY", "")
API_SECRET = os.getenv("BINGX_API_SECRET", "")
SYMBOL     = os.getenv("SYMBOL", "BTC-USDT")
INTERVAL   = os.getenv("INTERVAL", "3m")  # 메인 타임프레임

# 전략 ON/OFF
ENABLE_TREND       = os.getenv("ENABLE_TREND", "1") == "1"   # 3m → 15m 추세 전략
ENABLE_RANGE       = os.getenv("ENABLE_RANGE", "0") == "1"   # 박스장 전략
ENABLE_1M_CONFIRM  = os.getenv("ENABLE_1M_CONFIRM", "1") == "1"  # 1분봉 마지막 캔들 확인

# 레버리지/마진
LEVERAGE = int(os.getenv("LEVERAGE", "10"))
ISOLATED = os.getenv("ISOLATED", "1") == "1"

# 한 번에 계좌의 몇 %를 쓸지 (가용 선물잔고 기준)
RISK_PCT = float(os.getenv("RISK_PCT", "0.3"))

# 고정 TP/SL 비율 (추세장 기본값)
TP_PCT = float(os.getenv("TP_PCT", "0.02"))   # +2%
SL_PCT = float(os.getenv("SL_PCT", "0.02"))   # -2%

# 📌 박스장 전용 TP/SL 비율 (좀 더 짧게)
RANGE_TP_PCT = float(os.getenv("RANGE_TP_PCT", "0.006"))  # 0.6% 먹기
RANGE_SL_PCT = float(os.getenv("RANGE_SL_PCT", "0.004"))  # 0.4% 손절

# ATR 기반 TP/SL 옵션 (추세장에서만 쓰도록 아래에서 분기)
USE_ATR     = os.getenv("USE_ATR", "1") == "1"
ATR_LEN     = int(os.getenv("ATR_LEN", "20"))
ATR_TP_MULT = float(os.getenv("ATR_TP_MULT", "2.0"))
ATR_SL_MULT = float(os.getenv("ATR_SL_MULT", "1.2"))
# ATR이 너무 작거나 클 때 최소/최대 퍼센트
MIN_TP_PCT  = float(os.getenv("MIN_TP_PCT", "0.005"))   # 0.5%
MIN_SL_PCT  = float(os.getenv("MIN_SL_PCT", "0.005"))   # 0.5%

# 각종 쿨다운
COOLDOWN_SEC         = int(os.getenv("COOLDOWN_SEC", "15"))             # 진입 후 기본 대기
COOLDOWN_AFTER_CLOSE = int(os.getenv("COOLDOWN_AFTER_CLOSE", "30"))     # 청산 후 대기
COOLDOWN_AFTER_3LOSS = int(os.getenv("COOLDOWN_AFTER_3LOSS", "3600"))   # 3연속 손실 후 대기(초)
POLL_FILLS_SEC       = int(os.getenv("POLL_FILLS_SEC", "2"))            # TP/SL 체결 체크 주기

# 주문 금액 최소/최대
MIN_NOTIONAL_USDT = float(os.getenv("MIN_NOTIONAL_USDT", "5"))
MAX_NOTIONAL_USDT = float(os.getenv("MAX_NOTIONAL_USDT", "999999"))

# 갑자기 튄 캔들 따라가지 않기 위한 슬리피지 가드
MAX_PRICE_JUMP_PCT = float(os.getenv("MAX_PRICE_JUMP_PCT", "0.003"))

# 알림/REST
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
BASE = "https://open-api.bingx.com"

# 로그 옵션
LOG_TO_FILE = os.getenv("LOG_TO_FILE", "0") == "1"
LOG_FILE    = os.getenv("LOG_FILE", "bot.log")

# 시세 호출 실패 관련
MAX_KLINE_FAILS  = int(os.getenv("MAX_KLINE_FAILS", "5"))
KLINE_FAIL_SLEEP = int(os.getenv("KLINE_FAIL_SLEEP", "600"))

# UTC 시간대 필터 (예: "0-3,8-12")
TRADING_SESSIONS_UTC = os.getenv("TRADING_SESSIONS_UTC", "0-23")

# health / metrics 서버 포트 (0이면 안 띄움)
HEALTH_PORT = int(os.getenv("HEALTH_PORT", "0"))

# ─────────────────────────────
# 런타임 상태 변수
# ─────────────────────────────
OPEN_TRADES: List[Dict[str, Any]] = []   # 현재 열려있는 우리 봇 포지션 목록
LAST_CLOSE_TS = 0.0                      # 마지막 청산 시간
CONSEC_LOSSES = 0                        # 연속 손실 카운트
CONSEC_KLINE_FAILS = 0                   # 연속 시세 실패 카운트

# RSI 기준
RSI_OVERBOUGHT = int(os.getenv("RSI_OVERBOUGHT", "70"))
RSI_OVERSOLD   = int(os.getenv("RSI_OVERSOLD", "30"))

# metrics 노출용
METRICS = {
    "start_ts": START_TS,
    "last_loop_ts": START_TS,
    "open_trades": 0,
    "consec_losses": 0,
    "kline_failures": 0,
}

# ─────────────────────────────
# 공통 유틸
# ─────────────────────────────
def log(msg: str):
    """콘솔 + 옵션에 따라 파일로 로그 남김"""
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    if LOG_TO_FILE:
        try:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass

def ts_ms() -> int:
    return int(time.time() * 1000)

def sign_query(params: Dict[str, Any]) -> str:
    """BingX 용 쿼리 스트링 싸이닝"""
    qs = "&".join(f"{k}={params[k]}" for k in sorted(params.keys()))
    sig = hmac.new(API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
    return qs + "&signature=" + sig

def headers() -> Dict[str, str]:
    return {"X-BX-APIKEY": API_KEY, "Content-Type": "application/json"}

def req(method: str, path: str, params: Optional[Dict[str, Any]]=None, body: Optional[Dict[str, Any]]=None) -> Dict[str, Any]:
    """BingX REST 호출 공통 함수 (단일 시도 버전)"""
    params = params or {}
    params["timestamp"] = ts_ms()
    url = f"{BASE}{path}?{sign_query(params)}"
    r = requests.request(method, url, json=body, headers=headers(), timeout=12)
    if r.status_code != 200:
        raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text}")
    return r.json()

def send_tg(text: str):
    """텔레그램으로 한국어 알림 보내기"""
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
# 환경변수 정리: 한글/공백 들어가도 죽지 않게
# ─────────────────────────────
def _ensure_ascii_env(val: str, name: str) -> str:
    """
    키에 한글/공백 들어있어서 requests 헤더에서 터지는 걸 방지.
    - 가능하면 ASCII만 남기고
    - 뭐가 지워졌는지 로그만 남기고
    - 여기서 예외는 안 던진다.
    """
    if not val:
        return val
    try:
        val.encode("ascii")
        return val
    except UnicodeEncodeError:
        cleaned = val.encode("ascii", "ignore").decode("ascii").strip()
        log(f"[WARN] {name}에 ASCII 아닌 문자가 있어 제거했습니다. 원래='{val}' -> '{cleaned}'")
        return cleaned

# ─────────────────────────────
# 거래소 응답 파서
# ─────────────────────────────
def normalize_order(resp: Dict[str, Any]) -> Dict[str, Any]:
    """order 응답을 공통 포맷으로 바꿈"""
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
        return  # HTTP 요청 로그는 생략

def start_health_server():
    """0이 아닌 포트가 설정돼 있으면 /healthz, /metrics 띄움"""
    if HEALTH_PORT <= 0:
        return
    def _run():
        httpd = HTTPServer(("0.0.0.0", HEALTH_PORT), _HealthHandler)
        log(f"[HEALTH] server started on 0.0.0.0:{HEALTH_PORT}")
        httpd.serve_forever()
    th = threading.Thread(target=_run, daemon=True)
    th.start()

# ─────────────────────────────
# 시간대 필터
# ─────────────────────────────
def _parse_sessions(spec: str) -> List[Tuple[int, int]]:
    """예: '0-3,8-12' → [(0,3),(8,12)]"""
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
    """현재 UTC 시간이 우리가 지정한 시간대 안에 있는지"""
    now_utc = datetime.datetime.utcnow()
    h = now_utc.hour
    for s, e in SESSIONS:
        if s <= h <= e:
            return True
    return False

# ─────────────────────────────
# 잔고 / 포지션 / 주문
# ─────────────────────────────
def get_available_usdt() -> float:
    """
    가용 선물 잔고 조회 + 로그
    BingX가 종종 {"data": {"balance": {...}}} 이렇게 중첩으로 주기 때문에 그 경우도 처리한다.
    """
    try:
        res = req("GET", "/openApi/swap/v2/user/balance", {})
        log(f"[BALANCE RAW] {res}")

        data = res.get("data") or res.get("balances") or res

        # case 1: {"data": {"balance": {...}}}
        if isinstance(data, dict) and "balance" in data and isinstance(data["balance"], dict):
            bal = data["balance"]
            cand = (
                bal.get("availableMargin")
                or bal.get("availableBalance")
                or bal.get("balance")
                or bal.get("equity")
                or 0.0
            )
            try:
                avail = float(cand)
            except (TypeError, ValueError):
                avail = 0.0
            log(f"[BALANCE] available={avail} USDT (nested balance)")
            return avail

        # case 2: 리스트로 오는 경우
        if isinstance(data, list) and data:
            item = data[0]
        else:
            item = data

        # case 3: item 안에 다시 balance dict 있는 경우
        if isinstance(item, dict) and "balance" in item and isinstance(item["balance"], dict):
            bal = item["balance"]
            cand = (
                bal.get("availableMargin")
                or bal.get("availableBalance")
                or bal.get("balance")
                or bal.get("equity")
                or 0.0
            )
            try:
                avail = float(cand)
            except (TypeError, ValueError):
                avail = 0.0
            log(f"[BALANCE] available={avail} USDT (item.balance)")
            return avail

        # 일반 케이스
        cand = (
            item.get("availableBalance")
            or item.get("availableMargin")
            or item.get("balance")
            or 0.0
        )
        try:
            avail = float(cand)
        except (TypeError, ValueError):
            avail = 0.0

        log(f"[BALANCE] available={avail} USDT")
        return avail

    except Exception as e:
        log(f"[BALANCE ERROR] {e}")
        send_tg(f"⚠️ 잔고 조회 실패: {e}")
        return 0.0

def fetch_open_positions() -> List[Dict[str, Any]]:
    """
    거래소에 실제 열려 있는 포지션 가져오기
    일부 환경에서는 code=100400(this api is not exist) 을 주므로 그땐 그냥 빈 리스트 리턴
    """
    try:
        res = req("GET", "/openApi/swap/v2/trade/positions", {"symbol": SYMBOL})
        if res.get("code") == 100400:
            log("[POSITIONS] this api is not exist -> skip syncing positions")
            return []
        log(f"[POSITIONS RAW] {res}")
        data = res.get("data") or res.get("positions") or []
        if not isinstance(data, list):
            data = [data]
        return data
    except Exception as e:
        log(f"[POSITIONS ERROR] {e}")
        return []

def fetch_open_orders() -> List[Dict[str, Any]]:
    """거래소에 걸려 있는 주문 가져오기"""
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
    """
    봇이 재시작됐을 때,
    거래소에 열려 있는 포지션/주문을 보고 내부 OPEN_TRADES 상태를 맞춤.
    positions API가 미지원이면 그냥 안 맞추고 넘어감.
    """
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
    """
    캔들 가져와서 (ts, open, high, low, close) 리스트로 변환
    """
    resp = requests.get(
        f"{BASE}/openApi/swap/v2/quote/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
        timeout=12,
    )
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
    if out:
        log(f"[KLINES {interval}] ok count={len(out)} last_close={out[-1][4]}")
    else:
        log(f"[KLINES {interval}] empty")
    return out

# ─────────────────────────────
# ATR / 인디케이터
# ─────────────────────────────
def calc_atr(candles: List[Tuple[int, float, float, float, float]], length: int = 14) -> Optional[float]:
    """기본 ATR 계산"""
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

def ema(values: List[float], length: int) -> List[float]:
    """EMA 계산"""
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
    """RSI 계산"""
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
    """다이버전스 체크용 - 최근 두 개 고점 인덱스"""
    idxs = []
    for i in range(len(candles)-2, 1, -1):
        if candles[i][2] > candles[i-1][2] and candles[i][2] > candles[i+1][2]:
            idxs.append(i)
            if len(idxs) == 2:
                break
    return list(reversed(idxs))

def _find_last_two_pivot_lows(candles):
    """다이버전스 체크용 - 최근 두 개 저점 인덱스"""
    idxs = []
    for i in range(len(candles)-2, 1, -1):
        if candles[i][3] < candles[i-1][3] and candles[i][3] < candles[i+1][3]:
            idxs.append(i)
            if len(idxs) == 2:
                break
    return list(reversed(idxs))

def has_bearish_rsi_divergence(candles, rsi_vals) -> bool:
    """가격은 고점 높였는데 RSI는 낮춘 경우"""
    piv = _find_last_two_pivot_highs(candles)
    if len(piv) < 2:
        return False
    i1, i2 = piv
    price_up = candles[i2][2] > candles[i1][2]
    rsi_down = rsi_vals[i2] < rsi_vals[i1]
    return price_up and rsi_down

def has_bullish_rsi_divergence(candles, rsi_vals) -> bool:
    """가격은 저점 낮췄는데 RSI는 높인 경우"""
    piv = _find_last_two_pivot_lows(candles)
    if len(piv) < 2:
        return False
    i1, i2 = piv
    price_down = candles[i2][3] < candles[i1][3]
    rsi_up     = rsi_vals[i2] > rsi_vals[i1]
    return price_down and rsi_up

# ─────────────────────────────
# 전략 1: 3분봉 추세 + 15분봉 필터
# ─────────────────────────────
def decide_signal_3m_trend(candles: List[Tuple[int,float,float,float,float]]) -> Optional[str]:
    """3분봉에서 골든/데드크로스 + RSI + 횡보필터"""
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
    # 이평 간격이 너무 좁으면 횡보로 판단
    spread_ratio = abs(e20n - e50n) / e50n
    if spread_ratio < 0.0005:
        return None
    # 마지막 캔들 변동이 너무 작으면 스킵
    last = candles[-1]
    last_range_pct = (last[2] - last[3]) / last[3] if last[3] else 0
    if last_range_pct < 0.001:
        return None
    # 골든/데드 크로스 + RSI 범위
    long_sig  = (e20p < e50p) and (e20n > e50n) and (r_now < RSI_OVERBOUGHT)
    short_sig = (e20p > e50p) and (e20n < e50n) and (r_now > RSI_OVERSOLD)
    # 가격이 이평선 반대쪽에 있으면 신호 무효
    if long_sig and price < e50n:
        long_sig = False
    if short_sig and price > e50n:
        short_sig = False
    # 다이버전스 반대면 진입 안함
    if long_sig:
        if has_bearish_rsi_divergence(candles, r):
            return None
        return "LONG"
    if short_sig:
        if has_bullish_rsi_divergence(candles, r):
            return None
        return "SHORT"
    return None

def decide_trend_15m(candles_15m) -> Optional[str]:
    """15분봉 큰 방향"""
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
# 전략 2: 박스장 모드
# ─────────────────────────────
def decide_signal_range(candles) -> Optional[str]:
    """
    최근 N개 캔들 안에서 박스가 보이면 상단부에선 숏, 하단부에선 롱
    RSI도 같이 봐서 너무 약하면 진입 안함
    """
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
    # 박스 폭이 너무 좁으면 박스장으로 안 봄
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
# 거래 함수들
# ─────────────────────────────
def set_leverage_and_mode(symbol: str, leverage: int, isolated: bool = True):
    """레버리지 / 격리모드 설정"""
    req("POST", "/openApi/swap/v2/trade/leverage", {
        "symbol": symbol,
        "leverage": leverage,
    })
    req("POST", "/openApi/swap/v2/trade/marginType", {
        "symbol": symbol,
        "marginType": "ISOLATED" if isolated else "CROSSED",
    })

def place_market(symbol: str, side: str, qty: float) -> Dict[str, Any]:
    """시장가 주문"""
    return req("POST", "/openApi/swap/v2/trade/order", {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": qty,
    })

def place_conditional(symbol: str, side: str, qty: float, trigger_price: float, order_type: str) -> Dict[str, Any]:
    """TP/SL 같은 조건부 주문"""
    return req("POST", "/openApi/swap/v2/trade/order", {
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "quantity": qty,
        "reduceOnly": True,
        "triggerPrice": trigger_price,
    })

def place_tp_sl(symbol: str, side_open: str, qty: float, entry: float, tp_pct: float, sl_pct: float):
    """포지션 열자마자 TP/SL 두 개 깔기"""
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
    """TP/SL 설정 실패했을 때 시장가로 정리"""
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
    """진입 주문 타임아웃 났을 때 취소"""
    try:
        res = req("POST", "/openApi/swap/v2/trade/cancel", {
            "symbol": symbol,
            "orderId": order_id,
        })
        log(f"[CANCEL] order_id={order_id} resp={res}")
    except Exception as e:
        log(f"[CANCEL ERROR] {e}")

def get_order(symbol: str, order_id: str) -> Dict[str, Any]:
    """주문 상태 조회"""
    return req("GET", "/openApi/swap/v2/trade/order", {
        "symbol": symbol,
        "orderId": order_id,
    })

def get_fills(symbol: str, order_id: str) -> List[Dict[str, Any]]:
    """해당 주문 체결내역 가져오기"""
    res = req("GET", "/openApi/swap/v2/trade/allFillOrders", {
        "symbol": symbol,
        "orderId": order_id,
        "limit": 50,
    })
    return res.get("data", []) or []

def summarize_fills(symbol: str, order_id: str) -> Optional[Dict[str, Any]]:
    """체결내역으로부터 실제 체결가/수량/실현손익 추려내기"""
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
    """
    시장가 진입 응답이 왔다고 해서 진짜 체결된 건 아니니까
    짧게 몇 초 동안 상태를 폴링해서 FILLED 확인.
    안 되면 주문 취소.
    """
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
    cancel_order(symbol, order_id)
    return None

def ensure_tp_sl_for_trade(t: Dict[str, Any]) -> bool:
    """
    포지션은 살아 있는데 TP나 SL 주문이 취소/만료로 사라졌으면 다시 건다.
    실패하면 False 리턴해서 강제정리하도록 한다.
    """
    symbol = t["symbol"]
    side_open = t["side"]
    qty = t["qty"]
    close_side = "SELL" if side_open == "BUY" else "BUY"
    need_tp = False
    need_sl = False

    # TP 상태 체크
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

    # SL 상태 체크
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
    """
    열린 포지션에 대해 TP/SL이 체결됐는지 확인하고,
    체결되면 결과를 리턴해서 위에서 텔레그램으로 보내도록 함.
    """
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
        # TP 체크
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
        # SL 체크
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
        # TP/SL이 둘 다 살아 있어야 정상, 아니면 다시 건다
        if not closed:
            ok = ensure_tp_sl_for_trade(t)
            if not ok:
                close_position_market(symbol, t["side"], t["qty"])
            else:
                still_open.append(t)
    OPEN_TRADES = still_open
    return closed_results

# ─────────────────────────────
# 메인 루프
# ─────────────────────────────
RUNNING = True

def _sigterm(*_):
    """운영 환경에서 SIGTERM 들어오면 플래그만 바꿔서 자연 종료"""
    global RUNNING, TERMINATED_BY_SIGNAL
    TERMINATED_BY_SIGNAL = True
    RUNNING = False

signal.signal(signal.SIGTERM, _sigterm)

def main():
    global LAST_CLOSE_TS, CONSEC_LOSSES, CONSEC_KLINE_FAILS, OPEN_TRADES, API_KEY, API_SECRET

    # 여기서 한글/공백 제거해서 안전하게 바꿔 둔다.
    API_KEY = _ensure_ascii_env(API_KEY, "BINGX_API_KEY")
    API_SECRET = _ensure_ascii_env(API_SECRET, "BINGX_API_SECRET")

    # 환경변수 필수 검사
    if not API_KEY or not API_SECRET:
        msg = "❗ BINGX_API_KEY 또는 BINGX_API_SECRET 이 비어있습니다. .env 또는 Render 환경변수를 다시 설정하세요."
        log(msg)
        send_tg(msg)
        return

    if not (0 < RISK_PCT <= 1):
        log("RISK_PCT 는 0 < RISK_PCT <= 1 이어야 합니다. 현재 설정이 잘못되었습니다.")
        return
    if LEVERAGE <= 0:
        log("LEVERAGE 는 0보다 커야 합니다.")
        return
    if TP_PCT <= 0 or SL_PCT <= 0:
        log("TP_PCT, SL_PCT 는 0보다 커야 합니다.")
        return

    # 시작 알림
    send_tg("✅ [봇 시작] BingX 자동매매 시작합니다.")

    # 레버리지/마진 세팅
    try:
        set_leverage_and_mode(SYMBOL, LEVERAGE, ISOLATED)
    except Exception as e:
        log(f"[WARN] 레버리지/마진 설정 실패: {e} (계속 진행은 함)")

    # health 서버 시작
    start_health_server()

    # 거래소 상태 동기화 (positions 미지원이면 그냥 패스됨)
    sync_open_trades_from_exchange()

    last_reset_day = time.strftime("%Y-%m-%d")
    daily_pnl = 0.0
    last_fill_check = 0.0
    last_balance_log = 0.0

    while RUNNING:
        try:
            # metrics 갱신
            METRICS["last_loop_ts"] = time.time()
            METRICS["open_trades"] = len(OPEN_TRADES)
            METRICS["consec_losses"] = CONSEC_LOSSES
            METRICS["kline_failures"] = CONSEC_KLINE_FAILS

            now = time.time()

            # 1분마다 잔고 찍기
            if now - last_balance_log >= 60:
                get_available_usdt()
                last_balance_log = now

            # 날짜 바뀌면 일일 정산
            today = time.strftime("%Y-%m-%d")
            if today != last_reset_day:
                send_tg(f"📊 일일 정산: PnL {daily_pnl:.2f} USDT, 연속 손실 {CONSEC_LOSSES}")
                daily_pnl = 0.0
                CONSEC_LOSSES = 0
                last_reset_day = today

            # TP/SL 체결 체크
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

            # 포지션 열려 있으면 새로 안 들어감
            if OPEN_TRADES:
                time.sleep(1)
                continue

            # 연속 손실 휴식
            if CONSEC_LOSSES >= 3:
                send_tg("⏸ 연속 3회 손실 감지. 1시간 쉬고 다시 시작합니다.")
                time.sleep(COOLDOWN_AFTER_3LOSS)
                CONSEC_LOSSES = 0
                continue

            # 방금 청산했으면 잠깐 쉬기
            if LAST_CLOSE_TS > 0 and (time.time() - LAST_CLOSE_TS) < COOLDOWN_AFTER_CLOSE:
                time.sleep(1)
                continue

            # 시간대 필터
            if not in_trading_session_utc():
                time.sleep(2)
                continue

            # 3분봉 가져오기
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
                time.sleep(1)
                continue

            # 진입 방향 결정
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
                    if ENABLE_1M_CONFIRM:
                        candles_1m = get_klines(SYMBOL, "1m", 20)
                        if len(candles_1m) >= 2:
                            last_1m = candles_1m[-1]
                            if chosen_signal == "LONG" and last_1m[4] < last_1m[1]:
                                chosen_signal = None
                            elif chosen_signal == "SHORT" and last_1m[4] > last_1m[1]:
                                chosen_signal = None

            # 2) 박스장 모드 (추세 신호 없을 때만)
            if (not chosen_signal) and ENABLE_RANGE:
                sig_r = decide_signal_range(candles_3m)
                if sig_r:
                    chosen_signal = sig_r
                    signal_source = "RANGE"

            if not chosen_signal:
                time.sleep(1)
                continue

            last_price = candles_3m[-1][4]

            # 슬리피지 가드
            prev_price = candles_3m[-2][4]
            move_pct = abs(last_price - prev_price) / prev_price
            if move_pct > MAX_PRICE_JUMP_PCT:
                log(f"[PRICE GUARD] price jumped {move_pct:.4f}, skip entry")
                time.sleep(1)
                continue

            # 잔고 확인
            avail = get_available_usdt()
            if avail <= 0:
                send_tg("⚠️ 가용 선물 잔고가 0입니다. 진입을 건너뜁니다.")
                time.sleep(3)
                continue

            # 리스크 비율만큼만 사용
            notional = avail * RISK_PCT * LEVERAGE
            if notional < MIN_NOTIONAL_USDT:
                send_tg(f"⚠️ 계산된 주문 금액이 너무 작습니다: {notional:.2f} USDT")
                time.sleep(3)
                continue
            notional = min(notional, MAX_NOTIONAL_USDT)

            qty = round(notional / last_price, 6)
            side = "BUY" if chosen_signal == "LONG" else "SELL"

            # TP/SL 퍼센트 계산
            # 박스장일 때는 RANGE_TP_PCT/RANGE_SL_PCT를 그대로 사용
            # 추세장일 때만 ATR 모드/고정모드 사용
            if signal_source == "RANGE":
                local_tp_pct = RANGE_TP_PCT
                local_sl_pct = RANGE_SL_PCT
            else:
                local_tp_pct = TP_PCT
                local_sl_pct = SL_PCT
                if USE_ATR:
                    atr = calc_atr(candles_3m, ATR_LEN)
                    if atr and last_price > 0:
                        sl_pct_atr = (atr * ATR_SL_MULT) / last_price
                        tp_pct_atr = (atr * ATR_TP_MULT) / last_price
                        local_sl_pct = max(sl_pct_atr, MIN_SL_PCT)
                        local_tp_pct = max(tp_pct_atr, MIN_TP_PCT)

            # 한글로 전략/방향 변환
            strategy_kr = "추세장" if signal_source == "TREND" else "박스장"
            direction_kr = "롱" if chosen_signal == "LONG" else "숏"

            send_tg(
                f"🟢 진입 시도: {SYMBOL} 전략={strategy_kr} 방향={direction_kr}"
                f" 레버리지={LEVERAGE}x 사용비율={RISK_PCT*100:.0f}% 명목가≈{notional:.2f}USDT 수량={qty}"
            )

            # 실제 시장가 진입
            try:
                res = place_market(SYMBOL, side, qty)
            except Exception as e:
                send_tg(f"❌ 시장가 진입 실패: {e}")
                time.sleep(2)
                continue

            entry = last_price
            entry_order_id = normalize_order(res)["order_id"]
            try:
                entry = float((res.get("data") or {}).get("avgPrice", entry))
            except Exception:
                pass
            if not entry_order_id:
                send_tg("⚠️ 시장가 진입 응답에 orderId가 없습니다. TP/SL을 걸지 않습니다.")
                time.sleep(2)
                continue

            # 주문 체결 대기 (타임아웃 시 취소)
            filled_data = wait_filled(SYMBOL, entry_order_id, timeout=5)
            if not filled_data:
                send_tg("⚠️ 시장가 주문이 제한 시간 내 FILLED되지 않아 포지션을 건너뜁니다.")
                time.sleep(2)
                continue

            filled_qty = float(filled_data.get("quantity") or filled_data.get("executedQty") or qty)
            if filled_qty <= 0:
                send_tg("⚠️ 시장가 주문 체결 수량이 0입니다. TP/SL 미설정.")
                time.sleep(2)
                continue

            # TP/SL 예약
            try:
                tp_sl = place_tp_sl(SYMBOL, side, filled_qty, entry, local_tp_pct, local_sl_pct)
            except Exception as e:
                send_tg(f"❌ TP/SL 예약 실패: {e}, 포지션을 즉시 닫습니다.")
                close_position_market(SYMBOL, side, filled_qty)
                time.sleep(2)
                continue

            send_tg(
                f"📌 예약완료: TP={tp_sl['tp']} / SL={tp_sl['sl']} "
                f"(reduceOnly, mode={'ATR' if signal_source != 'RANGE' and USE_ATR else 'FIXED'})"
            )

            # 우리 내부 상태에 포지션 추가
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

            # 진입 후 잠깐 대기
            time.sleep(COOLDOWN_SEC)

        except Exception as e:
            log(f"ERROR: {e}")
            send_tg(f"❌ 오류 발생: {e}")
            time.sleep(2)

    # 여기까지 내려오면 종료
    uptime = time.time() - START_TS
    if (not TERMINATED_BY_SIGNAL) and (uptime >= MIN_UPTIME_FOR_STOP):
        send_tg("🛑 봇이 종료되었습니다.")
    else:
        log(f"[SKIP STOP] sig={TERMINATED_BY_SIGNAL} uptime={uptime:.2f}s")

if __name__ == "__main__":
    main()
