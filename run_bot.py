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
- (옵션) 청산 시 오늘자 signals CSV 를 구글 드라이브에 업로드
"""

from __future__ import annotations

import datetime
import signal
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, List, Optional, Tuple

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

SET = load_settings()
START_TS: float = time.time()
RUNNING: bool = True
TERMINATED_BY_SIGNAL: bool = False

METRICS: Dict[str, Any] = {
    "start_ts": START_TS,
    "last_loop_ts": START_TS,
    "open_trades": 0,
    "consec_losses": 0,
    "kline_failures": 0,
}

OPEN_TRADES: List[Trade] = []
TRADER_STATE: TraderState = TraderState()

LAST_CLOSE_TS: float = 0.0
CONSEC_LOSSES: int = 0
CONSEC_KLINE_FAILS: int = 0
RANGE_DAILY_SL: int = 0
RANGE_DISABLED_TODAY: bool = False
LAST_CLOSE_TS_TREND: float = 0.0
LAST_CLOSE_TS_RANGE: float = 0.0
LAST_SIGNAL_TS_3M: int = 0  # 같은 3분봉 재진입 방지


# ─────────────────────────────
# 2. health / metrics HTTP 서버
# ─────────────────────────────
class _HealthHandler(BaseHTTPRequestHandler):
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
        return


def start_health_server() -> None:
    if SET.health_port <= 0:
        return

    def _run() -> None:
        httpd = HTTPServer(("0.0.0.0", SET.health_port), _HealthHandler)
        log(f"[HEALTH] server started on 0.0.0.0:{SET.health_port}")
        httpd.serve_forever()

    th = threading.Thread(target=_run, daemon=True)
    th.start()


# ─────────────────────────────
# 3. 시간대(UTC) 필터
# ─────────────────────────────
def _parse_sessions(spec: str) -> List[Tuple[int, int]]:
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
    now_utc = datetime.datetime.now(datetime.UTC)
    hour = now_utc.hour
    for start_h, end_h in SESSIONS:
        if start_h <= hour <= end_h:
            return True
    return False


# ─────────────────────────────
# 4. SIGTERM 핸들러
# ─────────────────────────────
def _sigterm(*_: Any) -> None:
    global RUNNING, TERMINATED_BY_SIGNAL
    TERMINATED_BY_SIGNAL = True
    RUNNING = False


signal.signal(signal.SIGTERM, _sigterm)


# ─────────────────────────────
# 5. 거래소 → 내부 포지션 동기화
# ─────────────────────────────
def sync_open_trades_from_exchange(symbol: str) -> None:
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
# 6. 메인 루프
# ─────────────────────────────
def main() -> None:
    global RUNNING
    global OPEN_TRADES, LAST_CLOSE_TS, CONSEC_LOSSES, CONSEC_KLINE_FAILS
    global RANGE_DAILY_SL, RANGE_DISABLED_TODAY
    global LAST_CLOSE_TS_TREND, LAST_CLOSE_TS_RANGE
    global LAST_SIGNAL_TS_3M

    if not SET.api_key or not SET.api_secret:
        msg = "❗ BINGX_API_KEY 또는 BINGX_API_SECRET 이 비어있습니다. 환경변수를 먼저 설정하세요."
        log(msg)
        send_tg(msg)
        return

    log(
        f"CONFIG: ENABLE_TREND={SET.enable_trend}, "
        f"ENABLE_RANGE={SET.enable_range}, "
        f"ENABLE_1M_CONFIRM={SET.enable_1m_confirm}"
    )
    send_tg("✅ [봇 시작] BingX 자동매매 시작합니다.")

    try:
        set_leverage_and_mode(SET.symbol, SET.leverage, SET.isolated)
    except Exception as e:
        log(f"[WARN] 레버리지/마진 설정 실패: {e}")

    start_health_server()
    sync_open_trades_from_exchange(SET.symbol)

    now_kst = datetime.datetime.now(KST)
    last_report_date_kst = now_kst.strftime("%Y-%m-%d")

    daily_pnl: float = 0.0
    last_fill_check: float = 0.0
    last_balance_log: float = 0.0

    while RUNNING:
        try:
            METRICS["last_loop_ts"] = time.time()
            METRICS["open_trades"] = len(OPEN_TRADES)
            METRICS["consec_losses"] = CONSEC_LOSSES
            METRICS["kline_failures"] = CONSEC_KLINE_FAILS

            now = time.time()

            # 1) 1분마다 잔고 찍기
            if now - last_balance_log >= 60:
                get_available_usdt()
                last_balance_log = now

            # 2) 자정 리포트
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

            # 3) 청산 체크
            if now - last_fill_check >= SET.poll_fills_sec:
                OPEN_TRADES, closed_list = check_closes(OPEN_TRADES, TRADER_STATE)
                for closed in closed_list:
                    t: Trade = closed["trade"]
                    reason: str = closed["reason"]
                    summary: Optional[Dict[str, Any]] = closed.get("summary")

                    closed_qty = summary.get("qty") if summary else t.qty
                    closed_price = summary.get("avg_price") if summary else 0.0
                    pnl = summary.get("pnl") if summary else None

                    if pnl is None or pnl == 0.0:
                        if reason == "TP":
                            if t.side == "BUY":
                                pnl = (t.tp_price - t.entry) * closed_qty
                            else:
                                pnl = (t.entry - t.tp_price) * closed_qty
                        else:
                            if t.side == "BUY":
                                pnl = (t.sl_price - t.entry) * closed_qty
                            else:
                                pnl = (t.entry - t.sl_price) * closed_qty

                    daily_pnl += pnl
                    LAST_CLOSE_TS = now

                    if pnl < 0:
                        CONSEC_LOSSES += 1
                    else:
                        CONSEC_LOSSES = 0

                    if t.source == "TREND":
                        LAST_CLOSE_TS_TREND = now
                    elif t.source == "RANGE":
                        LAST_CLOSE_TS_RANGE = now

                    if t.source == "RANGE" and reason == "SL" and pnl < 0:
                        RANGE_DAILY_SL += 1
                        if RANGE_DAILY_SL >= SET.range_max_daily_sl:
                            RANGE_DISABLED_TODAY = True
                            send_tg(
                                f"🛑 [RANGE_OFF] 박스장 손절 {RANGE_DAILY_SL}회 → 오늘은 박스장 전략 비활성화"
                            )

                    send_tg(
                        f"💰 청산({reason}) {t.symbol} {t.side} 수량={closed_qty} "
                        f"가격={closed_price:.2f} PnL={pnl:.2f} USDT (금일 누적 {daily_pnl:.2f})"
                    )

                    # CSV에 청산 기록
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

                    # 오늘자 CSV를 드라이브에 올리기
                    try:
                        day_str = datetime.datetime.now(KST).strftime("%Y-%m-%d")
                        local_csv = f"logs/signals/signals-{day_str}.csv"
                        upload_to_drive(local_csv, f"signals-{day_str}.csv")
                        log("[DRIVE] signals uploaded to Google Drive.")
                    except FileNotFoundError:
                        log("[DRIVE] signals CSV 가 없어서 업로드를 건너뜁니다.")
                    except Exception as e:
                        log(f"[DRIVE] 업로드 실패: {e}")

                last_fill_check = now

                if TRADER_STATE.should_stop_bot():
                    send_tg("🛑 TP/SL 재설정이 연속으로 실패해서 봇을 중단합니다.")
                    RUNNING = False
                    break

            # 포지션 열려 있으면 새 진입 안 함
            if OPEN_TRADES:
                time.sleep(1)
                continue

            # 연속 손실 3회면 쉬기
            if CONSEC_LOSSES >= 3:
                send_tg("⛔ 연속 3회 손실 → 1시간 휴식")
                time.sleep(SET.cooldown_after_3loss)
                CONSEC_LOSSES = 0
                continue

            # 청산 직후 공통 쿨다운
            if LAST_CLOSE_TS > 0 and (time.time() - LAST_CLOSE_TS) < SET.cooldown_after_close:
                time.sleep(1)
                continue

            # 세션 시간 아니면 대기
            if not in_trading_session_utc():
                time.sleep(2)
                continue

            # 3분봉 가져오기
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

            if len(candles_3m) < 50:
                time.sleep(1)
                continue

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

            # 잔고 확인
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
            trend_15m_val: Optional[str] = None  # 로그용

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

            spread_pct_logged: Optional[float] = None
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

            qty = round(notional / last_price, 6)
            side_open = "BUY" if chosen_signal == "LONG" else "SELL"

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

            trade = open_position_with_tp_sl(
                settings=SET,
                symbol=SET.symbol,
                side_open=side_open,
                qty=qty,
                entry_price_hint=last_price,
                tp_pct=local_tp_pct,
                sl_pct=local_sl_pct,
                source=signal_source or "UNKNOWN",
            )
            if trade is None:
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

            OPEN_TRADES.append(trade)
            LAST_SIGNAL_TS_3M = latest_3m_ts
            time.sleep(SET.cooldown_sec)

        except Exception as e:
            log(f"ERROR: {e}")
            send_tg(f"❌ 오류 발생: {e}")
            log_signal(
                event="ERROR",
                symbol=SET.symbol,
                strategy_type="UNKNOWN",
                reason=str(e),
            )
            time.sleep(2)

    uptime = time.time() - START_TS
    if (not TERMINATED_BY_SIGNAL) and (uptime >= SET.min_uptime_for_stop):
        send_tg("🛑 봇이 종료되었습니다.")
    else:
        log(f"[SKIP STOP] sig={TERMINATED_BY_SIGNAL} uptime={uptime:.2f}s")


if __name__ == "__main__":
    main()
