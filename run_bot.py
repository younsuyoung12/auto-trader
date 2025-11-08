"""
run_bot.py
메인 루프 파일.

역할:
- 환경설정 로드
- health/metrics HTTP 서버 (옵션)
- 거래소 포지션/열린 주문 → 내부 상태로 동기화
- 무한 루프에서 시그널 판단 → 가드체크 → 주문 → TP/SL 확인
- 텔레그램 알림 전송
- signals_logger 로 모든 이벤트(SKIP/ENTRY/CLOSE/ERROR) CSV 기록
- (추가) 백그라운드에서 일정 주기로 오늘자 CSV를 구글 드라이브에 올리고,
         너무 오래된 CSV는 Render 디스크에서 지워서 용량이 안 불어나게 함

2025-11-09 1차 수정:
- settings.py에 추가된 use_orderbook_entry_hint 값을 반영해서,
  주문 직전에 받아둔 호가(best bid / best ask)로 진입가 힌트를 만든 뒤
  open_position_with_tp_sl(...)에 넘기도록 변경
- 스프레드 체크 시 가져온 best_bid / best_ask를 이후에도 재사용하도록 변수 유지

2025-11-09 2차 수정 (안전 종료 기능):
- 텔레그램에서 한글로 "종료", "봇종료", "끝나면 종료", "안전종료" 를 보내면
  SAFE_STOP_REQUESTED 플래그를 세팅하는 스레드를 추가
- 메인 루프에서 이 플래그를 보고,
  ① 열려 있는 포지션이 없으면 즉시 종료하고
  ② 열려 있는 포지션이 있으면 새 포지션은 만들지 않고 기다렸다가 비는 순간 종료
"""

from __future__ import annotations

import os
import datetime
import signal
import threading
import time
import json  # ← [추가] 텔레그램 getUpdates 응답 파싱용
import urllib.request  # ← [추가] 텔레그램 폴링용 HTTP 요청
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, List, Optional, Tuple

# 프로젝트 내부 모듈
from settings import load_settings, KST
from telelog import log, send_tg, send_skip_tg
from exchange_api import (
    get_available_usdt,
    fetch_open_positions,
    fetch_open_orders,
    set_leverage_and_mode,
)
from market_data import get_klines, get_orderbook
from indicators import calc_atr
from strategies_trend import (
    decide_signal_3m_trend,
    decide_trend_15m,
    confirm_1m_direction,
)
from strategies_range import (
    decide_signal_range,
    should_block_range_today,
)
from trader import (
    Trade,
    TraderState,
    open_position_with_tp_sl,
    check_closes,
)
from drive_uploader import upload_to_drive
from signals_logger import log_signal


# ─────────────────────────────
# 1. 설정/전역 상태
# ─────────────────────────────

# .env → settings.py → 여기
SET = load_settings()

# 프로세스 시작 시각
START_TS: float = time.time()

# 메인 루프 on/off
RUNNING: bool = True

# SIGTERM 으로 끝났는지
TERMINATED_BY_SIGNAL: bool = False

# 간단 메트릭
METRICS: Dict[str, Any] = {
    "start_ts": START_TS,
    "last_loop_ts": START_TS,
    "open_trades": 0,
    "consec_losses": 0,
    "kline_failures": 0,
}

# 현재 열려 있다고 알고 있는 포지션
OPEN_TRADES: List[Trade] = []

# TP/SL 재설정 실패 카운트 유지용
TRADER_STATE: TraderState = TraderState()

# 최근 청산 시각
LAST_CLOSE_TS: float = 0.0

# 연속 손실
CONSEC_LOSSES: int = 0

# 캔들 조회 실패 연속 카운트
CONSEC_KLINE_FAILS: int = 0

# 박스장 손절 횟수 / 비활성화 여부
RANGE_DAILY_SL: int = 0
RANGE_DISABLED_TODAY: bool = False

# 전략별 최근 청산 시각
LAST_CLOSE_TS_TREND: float = 0.0
LAST_CLOSE_TS_RANGE: float = 0.0

# 같은 3m 캔들 재진입 막는 타임스탬프
LAST_SIGNAL_TS_3M: int = 0

# ← [추가] 텔레그램으로 "종료" 명령이 들어왔는지 체크하는 플래그
SAFE_STOP_REQUESTED: bool = False

# ← [추가] 텔레그램 getUpdates 오프셋
TG_UPDATE_OFFSET: int = 0


# ─────────────────────────────
# 2. health / metrics HTTP 서버
# ─────────────────────────────
class _HealthHandler(BaseHTTPRequestHandler):
    """Render 에서 상태 확인할 때 쓰는 간단 서버"""

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/healthz":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")
            return

        if self.path == "/metrics":
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
            self.wfile.write("\n".join(out).encode())
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, *_: Any) -> None:
        # 기본 HTTP 로그는 감춤
        return


def start_health_server() -> None:
    """환경변수에 포트가 있으면 백그라운드로 health 서버 띄움"""
    if SET.health_port <= 0:
        return

    def _run() -> None:
        httpd = HTTPServer(("0.0.0.0", SET.health_port), _HealthHandler)
        log(f"[HEALTH] server started on 0.0.0.0:{SET.health_port}")
        httpd.serve_forever()

    th = threading.Thread(target=_run, daemon=True)
    th.start()


# ─────────────────────────────
# 3. 드라이브 동기화 백그라운드 스레드
# ─────────────────────────────
def start_drive_sync_thread() -> None:
    """
    logs/signals 안에 오늘자 CSV가 있으면 5분마다 구글 드라이브에 올리고,
    3일 지난 CSV는 로컬(Render 디스크)에서 지우는 스레드.
    → 매매 루프 성능에 영향 거의 없음.
    """
    SYNC_INTERVAL_SEC = 300  # 5분
    KEEP_DAYS = 3  # 3일 지난 건 삭제

    def _worker():
        while True:
            try:
                # 오늘 날짜 기준 파일 경로
                day_str = datetime.datetime.now(KST).strftime("%Y-%m-%d")
                local_dir = os.path.join("logs", "signals")
                local_path = os.path.join(local_dir, f"signals-{day_str}.csv")

                # 있으면 드라이브로 업로드
                if os.path.exists(local_path):
                    upload_to_drive(local_path, f"signals-{day_str}.csv")
                    log("[DRIVE_SYNC] uploaded today's signals csv")

                # 오래된 CSV 정리
                if os.path.exists(local_dir):
                    now_ts = time.time()
                    for fname in os.listdir(local_dir):
                        if not (fname.startswith("signals-") and fname.endswith(".csv")):
                            continue
                        fpath = os.path.join(local_dir, fname)
                        mtime = os.path.getmtime(fpath)
                        if now_ts - mtime > KEEP_DAYS * 86400:
                            try:
                                os.remove(fpath)
                                log(f"[DRIVE_SYNC] old csv removed: {fname}")
                            except OSError:
                                pass

            except Exception as e:
                # 업로드 실패해도 봇은 계속 돌아야 함
                log(f"[DRIVE_SYNC] error: {e}")

            time.sleep(SYNC_INTERVAL_SEC)

    th = threading.Thread(target=_worker, daemon=True)
    th.start()


# ─────────────────────────────
# 3-1. 텔레그램 명령 수신 스레드 (2025-11-09 추가)
# ─────────────────────────────
def start_telegram_command_thread() -> None:
    """텔레그램에서 '종료' 같은 한글 명령을 받아와 SAFE_STOP_REQUESTED 를 켜는 스레드"""
    if not SET.telegram_bot_token or not SET.telegram_chat_id:
        return  # 텔레그램 설정이 없으면 안 돌린다

    def _worker():
        global SAFE_STOP_REQUESTED, TG_UPDATE_OFFSET
        base_url = f"https://api.telegram.org/bot{SET.telegram_bot_token}/getUpdates"
        chat_id_str = str(SET.telegram_chat_id)
        while True:
            try:
                url = f"{base_url}?timeout=30"
                if TG_UPDATE_OFFSET:
                    url += f"&offset={TG_UPDATE_OFFSET}"
                with urllib.request.urlopen(url, timeout=35) as resp:
                    data = json.loads(resp.read().decode("utf-8"))

                for upd in data.get("result", []):
                    # 다음 요청부터는 이 이후로만
                    TG_UPDATE_OFFSET = upd["update_id"] + 1
                    msg = upd.get("message") or upd.get("channel_post")
                    if not msg:
                        continue
                    if str(msg["chat"]["id"]) != chat_id_str:
                        continue
                    text = (msg.get("text") or "").strip()
                    # 한글 종료 명령어 집합
                    if text in ("종료", "봇종료", "끝나면 종료", "안전종료"):
                        SAFE_STOP_REQUESTED = True
                        send_tg("🛑 종료 명령 수신: 포지션 정리 후 종료합니다.")
            except Exception as e:
                log(f"[TG CMD] error: {e}")
                time.sleep(5)

    th = threading.Thread(target=_worker, daemon=True)
    th.start()


# ─────────────────────────────
# 4. 시간대(UTC) 필터
# ─────────────────────────────
def _parse_sessions(spec: str) -> List[Tuple[int, int]]:
    """예: '0-3,8-12' → [(0,3),(8,12)]"""
    out: List[Tuple[int, int]] = []
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


SESSIONS: List[Tuple[int, int]] = _parse_sessions(SET.trading_sessions_utc)


def in_trading_session_utc() -> bool:
    """현재 UTC 시각이 우리가 설정한 거래 가능 시간대인지"""
    now_utc = datetime.datetime.now(datetime.UTC)
    hour = now_utc.hour
    for start_h, end_h in SESSIONS:
        if start_h <= hour <= end_h:
            return True
    return False


# ─────────────────────────────
# 5. SIGTERM 핸들러
# ─────────────────────────────
def _sigterm(*_: Any) -> None:
    """Render 가 SIGTERM 보냈을 때 자연스럽게 종료하려고"""
    global RUNNING, TERMINATED_BY_SIGNAL
    TERMINATED_BY_SIGNAL = True
    RUNNING = False


signal.signal(signal.SIGTERM, _sigterm)


# ─────────────────────────────
# 6. 거래소 → 내부 포지션 동기화
# ─────────────────────────────
def sync_open_trades_from_exchange(symbol: str) -> None:
    """
    봇이 재시작됐을 때 실제 거래소에 열려 있는 포지션을
    우리 OPEN_TRADES 에 그대로 반영해서 중복 진입을 막는다.
    """
    global OPEN_TRADES

    positions = fetch_open_positions(symbol)
    orders = fetch_open_orders(symbol)

    if not positions:
        log("[SYNC] 열려 있는 포지션이 없습니다.")
        return

    for pos in positions:
        qty = float(
            pos.get("positionAmt")
            or pos.get("quantity")
            or pos.get("size")
            or 0.0
        )
        if qty == 0.0:
            continue

        side_raw = (pos.get("positionSide") or pos.get("side") or "").upper()
        side_open = "BUY" if side_raw in ("LONG", "BUY") else "SELL"
        entry_price = float(pos.get("entryPrice") or pos.get("avgPrice") or 0.0)

        tp_id: Optional[str] = None
        sl_id: Optional[str] = None
        tp_price: Optional[float] = None
        sl_price: Optional[float] = None

        # 열려 있는 주문들 중에서 TP/SL 찾아서 붙여줌
        for o in orders:
            o_type = (o.get("type") or o.get("orderType") or "").upper()
            oid = o.get("orderId") or o.get("id")
            trig = o.get("triggerPrice") or o.get("stopPrice")
            if "TAKE" in o_type:
                tp_id = oid
                tp_price = float(trig) if trig else None
            if "STOP" in o_type:
                sl_id = oid
                sl_price = float(trig) if trig else None

        OPEN_TRADES.append(
            Trade(
                symbol=symbol,
                side=side_open,
                qty=qty,
                entry=entry_price,
                entry_order_id=None,
                tp_order_id=tp_id,
                sl_order_id=sl_id,
                tp_price=tp_price,
                sl_price=sl_price,
                source="SYNC",
            )
        )

    if OPEN_TRADES:
        send_tg(f"🔁 재시작 시 기존 포지션 {len(OPEN_TRADES)}건을 동기화했습니다. (중복 진입 방지)")
    else:
        log("[SYNC] 포지션을 못 가져와서 동기화할 게 없습니다.")


# ─────────────────────────────
# 7. 메인 루프
# ─────────────────────────────
def main() -> None:
    global RUNNING
    global OPEN_TRADES, LAST_CLOSE_TS, CONSEC_LOSSES, CONSEC_KLINE_FAILS
    global RANGE_DAILY_SL, RANGE_DISABLED_TODAY
    global LAST_CLOSE_TS_TREND, LAST_CLOSE_TS_RANGE
    global LAST_SIGNAL_TS_3M
    global SAFE_STOP_REQUESTED  # ← [추가] 종료명령 플래그 사용

    # 필수 키 체크
    if not SET.api_key or not SET.api_secret:
        msg = "❗ BINGX_API_KEY 또는 BINGX_API_SECRET 이 비어있습니다. 환경변수를 먼저 설정하세요."
        log(msg)
        send_tg(msg)
        return

    # 시작 로그
    log(
        f"CONFIG: ENABLE_TREND={SET.enable_trend}, "
        f"ENABLE_RANGE={SET.enable_range}, "
        f"ENABLE_1M_CONFIRM={SET.enable_1m_confirm}"
    )
    send_tg("✅ [봇 시작] BingX 자동매매 시작합니다.")

    # 레버리지/마진 설정 (실패해도 멈추지 않음)
    try:
        set_leverage_and_mode(SET.symbol, SET.leverage, SET.isolated)
    except Exception as e:
        log(f"[WARN] 레버리지/마진 설정 실패: {e}")

    # health 서버, 드라이브 동기화 스레드, 텔레그램 명령 스레드 시작
    start_health_server()
    start_drive_sync_thread()
    start_telegram_command_thread()  # ← [추가] 텔레그램에서 종료 명령 수신

    # 거래소 포지션을 우리 내부에 반영
    sync_open_trades_from_exchange(SET.symbol)

    # KST 기준 마지막 일일 보고 날짜
    now_kst = datetime.datetime.now(KST)
    last_report_date_kst = now_kst.strftime("%Y-%m-%d")

    # 일일 PnL
    daily_pnl: float = 0.0

    # 마지막 TP/SL 체결체크 시각
    last_fill_check: float = 0.0

    # 잔고 로그용
    last_balance_log: float = 0.0

    # ───────── 메인 무한 루프 ─────────
    while RUNNING:
        try:
            # 메트릭 최신화
            METRICS["last_loop_ts"] = time.time()
            METRICS["open_trades"] = len(OPEN_TRADES)
            METRICS["consec_losses"] = CONSEC_LOSSES
            METRICS["kline_failures"] = CONSEC_KLINE_FAILS

            now = time.time()

            # 1) 1분마다 잔고 찍기 (Render 로그에서 보려고)
            if now - last_balance_log >= 60:
                get_available_usdt()
                last_balance_log = now

            # 2) KST 자정이면 하루 정산
            now_kst = datetime.datetime.now(KST)
            today_kst = now_kst.strftime("%Y-%m-%d")
            if now_kst.hour == 0 and now_kst.minute < 1:
                if last_report_date_kst != today_kst:
                    send_tg(
                        f"📊 일일 정산(KST): PnL {daily_pnl:.2f} USDT, 연속 손실 {CONSEC_LOSSES}"
                    )
                    daily_pnl = 0.0
                    CONSEC_LOSSES = 0
                    RANGE_DAILY_SL = 0
                    RANGE_DISABLED_TODAY = False
                    last_report_date_kst = today_kst

            # 3) 열려 있는 포지션들 TP/SL 체결됐는지 주기적으로 확인
            if now - last_fill_check >= SET.poll_fills_sec:
                OPEN_TRADES, closed_list = check_closes(OPEN_TRADES, TRADER_STATE)
                for closed in closed_list:
                    t: Trade = closed["trade"]
                    reason: str = closed["reason"]
                    summary: Optional[Dict[str, Any]] = closed.get("summary")

                    closed_qty = summary.get("qty") if summary else t.qty
                    closed_price = summary.get("avg_price") if summary else 0.0
                    pnl = summary.get("pnl") if summary else None

                    # pnl 이 없으면 우리가 계산
                    if pnl is None or pnl == 0.0:
                        if reason == "TP":
                            if t.side == "BUY":
                                pnl = (t.tp_price - t.entry) * closed_qty
                            else:
                                pnl = (t.entry - t.tp_price) * closed_qty
                        else:  # SL or FORCE_CLOSE
                            if t.side == "BUY":
                                pnl = (t.sl_price - t.entry) * closed_qty
                            else:
                                pnl = (t.entry - t.sl_price) * closed_qty

                    daily_pnl += pnl
                    LAST_CLOSE_TS = now

                    # 연속 손실 up/down
                    if pnl < 0:
                        CONSEC_LOSSES += 1
                    else:
                        CONSEC_LOSSES = 0

                    # 전략별 최근 청산 시각
                    if t.source == "TREND":
                        LAST_CLOSE_TS_TREND = now
                    elif t.source == "RANGE":
                        LAST_CLOSE_TS_RANGE = now

                    # 박스장 SL 누적되면 오늘은 박스 전략 off
                    if t.source == "RANGE" and reason == "SL" and pnl < 0:
                        RANGE_DAILY_SL += 1
                        if RANGE_DAILY_SL >= SET.range_max_daily_sl:
                            RANGE_DISABLED_TODAY = True
                            send_tg(
                                f"🛑 [RANGE_OFF] 박스장 손절 {RANGE_DAILY_SL}회 → 오늘은 박스장 전략 비활성화"
                            )

                    # 텔레그램 청산 알림
                    send_tg(
                        f"💰 청산({reason}) {t.symbol} {t.side} 수량={closed_qty} "
                        f"가격={closed_price:.2f} PnL={pnl:.2f} USDT (금일 누적 {daily_pnl:.2f})"
                    )

                    # CSV에도 청산 기록
                    log_signal(
                        event="CLOSE",
                        symbol=t.symbol,
                        strategy_type=t.source,
                        direction=t.side,
                        price=closed_price,
                        qty=closed_qty,
                        tp_price=t.tp_price,
                        sl_price=t.sl_price,
                        reason=reason,
                        pnl=pnl,
                        extra=f"entry_order_id={t.entry_order_id}",
                    )

                last_fill_check = now

                # TP/SL 재설정이 계속 실패하면 봇 멈춤
                if TRADER_STATE.should_stop_bot():
                    send_tg("🛑 TP/SL 재설정이 연속으로 실패해서 봇을 중단합니다.")
                    RUNNING = False
                    break

                # ← [추가] 종료 명령이 있고, 포지션이 이제 0개면 바로 종료
                if SAFE_STOP_REQUESTED and not OPEN_TRADES:
                    send_tg("🛑 종료 명령에 따라 포지션이 없어 즉시 종료합니다.")
                    RUNNING = False
                    break

            # 4) 포지션 열려 있으면 새로 진입하지 않음
            if OPEN_TRADES:
                time.sleep(1)
                continue

            # ← [추가] 포지션이 없고 종료 요청이 켜져 있으면 새 진입 없이 종료
            if SAFE_STOP_REQUESTED:
                send_tg("🛑 종료 명령에 따라 새 진입 없이 종료합니다.")
                RUNNING = False
                break

            # 5) 연속 손실 3회면 잠깐 쉬기
            if CONSEC_LOSSES >= 3:
                send_tg("⛔ 연속 3회 손실 → 1시간 휴식")
                time.sleep(SET.cooldown_after_3loss)
                CONSEC_LOSSES = 0
                continue

            # 6) 청산 직후 공통 쿨다운
            if LAST_CLOSE_TS > 0 and (time.time() - LAST_CLOSE_TS) < SET.cooldown_after_close:
                time.sleep(1)
                continue

            # 7) 트레이딩 세션(UTC) 밖이면 대기
            if not in_trading_session_utc():
                time.sleep(2)
                continue

            # 8) 3분봉 데이터 가져오기
            try:
                candles_3m = get_klines(SET.symbol, SET.interval, 120)
                CONSEC_KLINE_FAILS = 0
                if candles_3m:
                    log(
                        f"[DATA] 3m klines ok: symbol={SET.symbol} count={len(candles_3m)} last_close={candles_3m[-1][4]}"
                    )
            except Exception as e:
                CONSEC_KLINE_FAILS += 1
                log(f"[KLINE ERROR] {e} (fail={CONSEC_KLINE_FAILS})")
                if CONSEC_KLINE_FAILS >= SET.max_kline_fails:
                    send_tg(
                        f"⚠️ 시세 연속 {CONSEC_KLINE_FAILS}회 실패. {SET.kline_fail_sleep}s 대기."
                    )
                    time.sleep(SET.kline_fail_sleep)
                    CONSEC_KLINE_FAILS = 0
                else:
                    time.sleep(2)
                continue

            # 캔들이 너무 적으면 패스
            if len(candles_3m) < 50:
                time.sleep(1)
                continue

            # 최신 캔들이 너무 오래됐으면 패스
            latest_3m_ts = candles_3m[-1][0]
            now_ms = int(time.time() * 1000)
            if now_ms - latest_3m_ts > SET.max_kline_delay_sec * 1000:
                send_skip_tg("[SKIP] 3m_kline_delayed: 최근 3m 캔들이 지연되었습니다.")
                log_signal(
                    event="SKIP",
                    symbol=SET.symbol,
                    strategy_type="UNKNOWN",
                    reason="3m_kline_delayed",
                    candle_ts=latest_3m_ts,
                )
                time.sleep(1)
                continue

            # 같은 3m 캔들에서 이미 진입했으면 패스
            if latest_3m_ts == LAST_SIGNAL_TS_3M:
                send_skip_tg("[SKIP] same_3m_candle: 이미 이 캔들에서 진입했습니다.")
                log_signal(
                    event="SKIP",
                    symbol=SET.symbol,
                    strategy_type="UNKNOWN",
                    reason="same_3m_candle",
                    candle_ts=latest_3m_ts,
                )
                time.sleep(1)
                continue

            # 9) 잔고 확인
            avail = get_available_usdt()
            if avail <= 0:
                send_skip_tg("[BALANCE_SKIP] ⚠️ 가용 선물 잔고가 0입니다. 진입을 건너뜁니다.")
                log_signal(
                    event="SKIP",
                    symbol=SET.symbol,
                    strategy_type="UNKNOWN",
                    reason="balance_zero",
                    candle_ts=latest_3m_ts,
                    available_usdt=0.0,
                )
                time.sleep(3)
                continue

            # ───────── 시그널 판단 ─────────
            chosen_signal: Optional[str] = None  # LONG / SHORT
            signal_source: Optional[str] = None  # TREND / RANGE
            trend_15m_val: Optional[str] = None  # 로깅용

            # 1) 추세 전략
            candles_15m: Optional[List[Any]] = None
            if SET.enable_trend:
                candles_15m = get_klines(SET.symbol, "15m", 120)
                if candles_15m:
                    log(
                        f"[DATA] 15m klines ok: symbol={SET.symbol} count={len(candles_15m)} last_close={candles_15m[-1][4]}"
                    )
                sig_3m = decide_signal_3m_trend(
                    candles_3m,
                    SET.rsi_overbought,
                    SET.rsi_oversold,
                )
                trend_15m_val = decide_trend_15m(candles_15m) if candles_15m else None

                if sig_3m and trend_15m_val and sig_3m == trend_15m_val:
                    if (time.time() - LAST_CLOSE_TS_TREND) >= SET.cooldown_after_close_trend:
                        chosen_signal = sig_3m
                        signal_source = "TREND"
                        # 1분봉 확인 옵션
                        if SET.enable_1m_confirm:
                            candles_1m = get_klines(SET.symbol, "1m", 20)
                            if candles_1m:
                                log(
                                    f"[DATA] 1m klines ok: symbol={SET.symbol} count={len(candles_1m)} last_close={candles_1m[-1][4]}"
                                )
                            if not confirm_1m_direction(candles_1m, chosen_signal):
                                send_skip_tg("[SKIP] 1m_confirm_mismatch: 1분봉이 방향 불일치")
                                log_signal(
                                    event="SKIP",
                                    symbol=SET.symbol,
                                    strategy_type="TREND",
                                    direction=chosen_signal,
                                    reason="1m_confirm_mismatch",
                                    candle_ts=latest_3m_ts,
                                    trend_15m=trend_15m_val,
                                )
                                chosen_signal = None
                                signal_source = None
                    else:
                        send_skip_tg("[SKIP] trend_cooldown: 직전 TREND 포지션 대기중")
                        log_signal(
                            event="SKIP",
                            symbol=SET.symbol,
                            strategy_type="TREND",
                            direction=sig_3m,
                            reason="trend_cooldown",
                            candle_ts=latest_3m_ts,
                            trend_15m=trend_15m_val,
                        )

            # 2) 박스 전략
            if (not chosen_signal) and SET.enable_range and (not RANGE_DISABLED_TODAY):
                if not candles_15m:
                    candles_15m = get_klines(SET.symbol, "15m", 120)
                    if candles_15m:
                        log(
                            f"[DATA] 15m klines ok: symbol={SET.symbol} count={len(candles_15m)} last_close={candles_15m[-1][4]}"
                        )
                if (time.time() - LAST_CLOSE_TS_RANGE) >= SET.cooldown_after_close_range:
                    if not should_block_range_today(candles_3m, candles_15m):
                        r_sig = decide_signal_range(candles_3m)
                        if r_sig:
                            chosen_signal = r_sig
                            signal_source = "RANGE"
                    else:
                        send_skip_tg("[SKIP] range_blocked_today: 박스장 조건 불리")
                        log_signal(
                            event="SKIP",
                            symbol=SET.symbol,
                            strategy_type="RANGE",
                            reason="range_blocked_today",
                            candle_ts=latest_3m_ts,
                        )
                else:
                    send_skip_tg("[SKIP] range_cooldown: 직전 RANGE 포지션 대기중")
                    log_signal(
                        event="SKIP",
                        symbol=SET.symbol,
                        strategy_type="RANGE",
                        reason="range_cooldown",
                        candle_ts=latest_3m_ts,
                    )

            # 두 전략 다 없으면 다음 루프
            if not chosen_signal:
                time.sleep(1)
                continue

            # ───────── 진입 전 가드 ─────────
            last_price = candles_3m[-1][4]
            prev_price = candles_3m[-2][4]
            move_pct = abs(last_price - prev_price) / prev_price
            if move_pct > SET.max_price_jump_pct:
                send_skip_tg(
                    f"[SKIP] price_jump_guard: {move_pct:.4f} > {SET.max_price_jump_pct:.4f}"
                )
                log_signal(
                    event="SKIP",
                    symbol=SET.symbol,
                    strategy_type=signal_source or "UNKNOWN",
                    direction=chosen_signal,
                    reason="price_jump_guard",
                    candle_ts=latest_3m_ts,
                    extra=f"move_pct={move_pct:.6f}",
                )
                time.sleep(1)
                continue

            # 호가 스프레드 체크 (아래에서 진입가 힌트로도 쓰기 위해 best_bid/best_ask 보관)
            spread_pct_logged: Optional[float] = None
            best_bid: Optional[float] = None
            best_ask: Optional[float] = None
            orderbook = get_orderbook(SET.symbol, 5)
            if orderbook:
                bids = orderbook.get("bids") or []
                asks = orderbook.get("asks") or []
                if bids and asks:
                    try:
                        best_bid = float(bids[0][0] if isinstance(bids[0], list) else bids[0].get("price"))
                        best_ask = float(asks[0][0] if isinstance(asks[0], list) else asks[0].get("price"))
                        if best_bid > 0:
                            spread_pct = (best_ask - best_bid) / best_bid
                            spread_pct_logged = spread_pct
                            if spread_pct > SET.max_spread_pct:
                                send_skip_tg(
                                    f"[SKIP] spread_guard: {spread_pct:.5f} > {SET.max_spread_pct:.5f}"
                                )
                                log_signal(
                                    event="SKIP",
                                    symbol=SET.symbol,
                                    strategy_type=signal_source or "UNKNOWN",
                                    direction=chosen_signal,
                                    reason="spread_guard",
                                    candle_ts=latest_3m_ts,
                                    spread_pct=spread_pct,
                                )
                                time.sleep(1)
                                continue
                    except Exception as e:
                        log(f"[SPREAD PARSE ERROR] {e}")

            # ATR 기반 리스크 조절
            effective_risk_pct = SET.risk_pct
            atr_fast_val: Optional[float] = None
            atr_slow_val: Optional[float] = None
            if SET.use_atr:
                atr_fast_val = calc_atr(candles_3m, SET.atr_len)
                atr_slow_val = calc_atr(candles_3m, max(SET.atr_len * 2, SET.atr_len + 10))
                if atr_fast_val and atr_slow_val and atr_slow_val > 0:
                    if atr_fast_val > atr_slow_val * SET.atr_risk_high_mult:
                        effective_risk_pct = SET.risk_pct * SET.atr_risk_reduction
                        log(
                            f"[ATR RISK] fast ATR high → risk {SET.risk_pct} -> {effective_risk_pct}"
                        )

            # 주문 명목가 계산
            notional = avail * effective_risk_pct * SET.leverage
            if notional < SET.min_notional_usdt:
                send_tg(f"⚠️ 계산된 주문 금액이 너무 작습니다: {notional:.2f} USDT")
                log_signal(
                    event="SKIP",
                    symbol=SET.symbol,
                    strategy_type=signal_source or "UNKNOWN",
                    direction=chosen_signal,
                    reason="notional_too_small",
                    candle_ts=latest_3m_ts,
                    available_usdt=avail,
                    notional=notional,
                )
                time.sleep(3)
                continue
            notional = min(notional, SET.max_notional_usdt)

            # 수량
            qty = round(notional / last_price, 6)
            side_open = "BUY" if chosen_signal == "LONG" else "SELL"

            # TP/SL 퍼센트 선택
            if signal_source == "RANGE":
                local_tp_pct = SET.range_tp_pct
                local_sl_pct = SET.range_sl_pct
            else:
                local_tp_pct = SET.tp_pct
                local_sl_pct = SET.sl_pct
                if SET.use_atr:
                    atr_val = calc_atr(candles_3m, SET.atr_len)
                    if atr_val and last_price > 0:
                        sl_pct_atr = (atr_val * SET.atr_sl_mult) / last_price
                        tp_pct_atr = (atr_val * SET.atr_tp_mult) / last_price
                        local_sl_pct = max(sl_pct_atr, SET.min_sl_pct)
                        local_tp_pct = max(tp_pct_atr, SET.min_tp_pct)

            strategy_kr = "추세장" if signal_source == "TREND" else "박스장"
            direction_kr = "롱" if chosen_signal == "LONG" else "숏"

            # 텔레그램 진입 시도 메시지
            send_tg(
                f"[ENTRY][{signal_source}] 🟢 진입 시도: {SET.symbol} 전략={strategy_kr} 방향={direction_kr} "
                f"레버리지={SET.leverage}x 사용비율={effective_risk_pct*100:.0f}% "
                f"명목가≈{notional:.2f}USDT 수량={qty}"
            )

            # 실제 주문 넣기 전에 ENTRY_SIGNAL 기록
            log_signal(
                event="ENTRY_SIGNAL",
                symbol=SET.symbol,
                strategy_type=signal_source or "UNKNOWN",
                direction=chosen_signal,
                price=last_price,
                qty=qty,
                candle_ts=latest_3m_ts,
                trend_15m=trend_15m_val,
                atr_fast=atr_fast_val,
                atr_slow=atr_slow_val,
                spread_pct=spread_pct_logged,
                available_usdt=avail,
                notional=notional,
            )

            # ===== 진입가 힌트 결정 (기존 2025-11-09 수정) =====
            entry_price_hint = last_price
            if SET.use_orderbook_entry_hint and best_bid and best_ask:
                # 살 때는 매도호가, 팔 때는 매수호가 기준으로 TP/SL 계산하도록 힌트를 준다
                if side_open == "BUY":
                    entry_price_hint = best_ask
                else:
                    entry_price_hint = best_bid

            # 진짜 주문 + TP/SL 예약
            trade = open_position_with_tp_sl(
                settings=SET,
                symbol=SET.symbol,
                side_open=side_open,
                qty=qty,
                entry_price_hint=entry_price_hint,
                tp_pct=local_tp_pct,
                sl_pct=local_sl_pct,
                source=signal_source or "UNKNOWN",
            )
            if trade is None:
                # 주문 자체가 실패했다면 SKIP 으로 기록
                log_signal(
                    event="SKIP",
                    symbol=SET.symbol,
                    strategy_type=signal_source or "UNKNOWN",
                    direction=chosen_signal,
                    reason="entry_failed",
                    candle_ts=latest_3m_ts,
                )
                time.sleep(2)
                continue

            # 예약 성공 텔레그램
            send_tg(
                f"[ENTRY][{signal_source}] 📌 예약완료: TP={trade.tp_price} / SL={trade.sl_price}"
            )

            # 실제로 포지션 열린 것 기록
            log_signal(
                event="ENTRY_OPENED",
                symbol=trade.symbol,
                strategy_type=trade.source,
                direction=trade.side,
                price=trade.entry,
                qty=trade.qty,
                tp_price=trade.tp_price,
                sl_price=trade.sl_price,
                candle_ts=latest_3m_ts,
                available_usdt=avail,
                notional=notional,
                extra=f"entry_order_id={trade.entry_order_id}",
            )

            # 내부 상태에 포지션 추가
            OPEN_TRADES.append(trade)

            # 같은 3m 캔들 재진입 방지
            LAST_SIGNAL_TS_3M = latest_3m_ts

            # 진입 후 쿨다운
            time.sleep(SET.cooldown_sec)

        except Exception as e:
            # 최상단 예외 처리
            log(f"ERROR: {e}")
            send_tg(f"❌ 오류 발생: {e}")
            log_signal(
                event="ERROR",
                symbol=SET.symbol,
                strategy_type="UNKNOWN",
                reason=str(e),
            )
            time.sleep(2)

    # 메인 루프 빠져나온 뒤
    uptime = time.time() - START_TS
    if (not TERMINATED_BY_SIGNAL) and (uptime >= SET.min_uptime_for_stop):
        send_tg("🛑 봇이 종료되었습니다.")
    else:
        log(f"[SKIP STOP] sig={TERMINATED_BY_SIGNAL} uptime={uptime:.2f}s")


# 직접 실행일 때만 main() 호출
if __name__ == "__main__":
    main()
