"""run_bot.py
메인 루프 파일.

원래 하나로 뭉쳐 있던 bot.py 내용을 역할별로 나눈 뒤,
여기서 전부 import 해서 실제로 봇을 돌린다.

주요 역할:
- 환경설정 로드
- health/metrics HTTP 서버 (옵션)
- 거래소 포지션/열린 주문 → 내부 상태로 동기화
- 무한 루프에서 진입 조건 판단, 주문, TP/SL 확인
- 텔레그램 알림 전송

이 파일을 Render의 실행 엔트리로 두면 된다.
예: python -u run_bot.py
"""

from __future__ import annotations

import datetime
import signal
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, List, Optional, Tuple

# 우리가 앞에서 나눈 모듈들
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

# ─────────────────────────────
# 1. 설정 및 전역 상태
# ─────────────────────────────

# .env → settings.py → 여기로 로드
SET = load_settings()

# 프로세스 시작 시각 (uptime 계산용)
START_TS: float = time.time()

# 메인 루프 on/off 플래그
RUNNING: bool = True

# SIGTERM 으로 끝났는지 표시
TERMINATED_BY_SIGNAL: bool = False

# 메트릭스 (health 서버에서 노출)
METRICS: Dict[str, Any] = {
    "start_ts": START_TS,
    "last_loop_ts": START_TS,
    "open_trades": 0,
    "consec_losses": 0,
    "kline_failures": 0,
}

# 현재 우리 봇이 알고 있는 열린 포지션 목록
OPEN_TRADES: List[Trade] = []

# TP/SL 재설정 실패 카운트 등 관리
TRADER_STATE: TraderState = TraderState()

# 최근 청산 시각 (공통)
LAST_CLOSE_TS: float = 0.0

# 연속 손실 카운트
CONSEC_LOSSES: int = 0

# 캔들 조회 실패 연속 카운트
CONSEC_KLINE_FAILS: int = 0

# 박스장 일일 손절 횟수
RANGE_DAILY_SL: int = 0

# 오늘 박스장 전략 비활성화 여부
RANGE_DISABLED_TODAY: bool = False

# 전략별(추세/박스) 최근 청산 시각
LAST_CLOSE_TS_TREND: float = 0.0
LAST_CLOSE_TS_RANGE: float = 0.0

# 같은 3m 캔들에서 중복 진입을 막기 위한 타임스탬프
LAST_SIGNAL_TS_3M: int = 0


# ─────────────────────────────
# 2. health / metrics HTTP 서버
# ─────────────────────────────
class _HealthHandler(BaseHTTPRequestHandler):
    """Render 같은 곳에서 상태 확인할 때 쓰는 단순 HTTP 핸들러."""

    def do_GET(self) -> None:  # noqa: N802 (BaseHTTPRequestHandler 규약)
        """GET /healthz, /metrics 에 응답"""
        if self.path == "/healthz":
            # 단순 생존 확인
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")
            return

        if self.path == "/metrics":
            # 프로메테우스 스타일 간단 메트릭
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

        # 그 외 경로는 404
        self.send_response(404)
        self.end_headers()

    def log_message(self, *_: Any) -> None:
        """기본 HTTP 요청 로그는 지워서 콘솔이 지저분해지는 걸 막는다."""
        return


def start_health_server() -> None:
    """환경변수에 포트가 지정돼 있으면 백그라운드로 health 서버를 띄운다."""
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
    """예: '0-3,8-12' → [(0, 3), (8, 12)] 형태로 파싱."""
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


# settings 에서 들어온 문자열을 파싱해 전역으로 들고 있음
SESSIONS: List[Tuple[int, int]] = _parse_sessions(SET.trading_sessions_utc)


def in_trading_session_utc() -> bool:
    """현재 UTC 시간이 우리가 설정한 거래 가능 시간대 안에 있는지 확인."""
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
    """SIGTERM 들어오면 메인 루프가 자연스럽게 빠져나가도록 플래그 세팅."""
    global RUNNING, TERMINATED_BY_SIGNAL
    TERMINATED_BY_SIGNAL = True
    RUNNING = False


# 프로세스에 시그널 핸들러 등록
signal.signal(signal.SIGTERM, _sigterm)


# ─────────────────────────────
# 5. 거래소 → 내부 포지션 동기화
# ─────────────────────────────
def sync_open_trades_from_exchange(symbol: str) -> None:
    """
    봇이 재시작됐을 때 거래소에 이미 열려 있는 포지션/주문을
    현재 봇의 OPEN_TRADES 에 맞춰 넣어 중복 진입을 막는다.
    """
    global OPEN_TRADES

    positions = fetch_open_positions(symbol)
    orders = fetch_open_orders(symbol)

    # 포지션 없으면 끝
    if not positions:
        log("[SYNC] 열려 있는 포지션이 없습니다.")
        return

    for pos in positions:
        # 가능한 여러 케이스를 float 로 통일
        qty = float(
            pos.get("positionAmt")
            or pos.get("quantity")
            or pos.get("size")
            or 0.0
        )
        if qty == 0.0:
            # 수량 0이면 의미 없는 포지션
            continue

        # 포지션 방향 파싱
        side_raw = (pos.get("positionSide") or pos.get("side") or "").upper()
        side_open = "BUY" if side_raw in ("LONG", "BUY") else "SELL"

        # 진입가
        entry_price = float(pos.get("entryPrice") or pos.get("avgPrice") or 0.0)

        # 포지션에 묶여 있는 TP/SL 주문 찾기
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

        # 내부 상태에 추가 (source="SYNC" 로 표기해서 박스장 카운트에서 제외)
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
    """실제로 봇을 실행하는 메인 함수."""
    global RUNNING
    global OPEN_TRADES, LAST_CLOSE_TS, CONSEC_LOSSES, CONSEC_KLINE_FAILS
    global RANGE_DAILY_SL, RANGE_DISABLED_TODAY
    global LAST_CLOSE_TS_TREND, LAST_CLOSE_TS_RANGE
    global LAST_SIGNAL_TS_3M

    # 필수 키가 없으면 바로 종료
    if not SET.api_key or not SET.api_secret:
        msg = "❗ BINGX_API_KEY 또는 BINGX_API_SECRET 이 비어있습니다. 환경변수를 먼저 설정하세요."
        log(msg)
        send_tg(msg)
        return

    # 설정 출력
    log(
        f"CONFIG: ENABLE_TREND={SET.enable_trend}, "
        f"ENABLE_RANGE={SET.enable_range}, "
        f"ENABLE_1M_CONFIRM={SET.enable_1m_confirm}"
    )
    send_tg("✅ [봇 시작] BingX 자동매매 시작합니다.")

    # 레버리지/마진 설정 (실패해도 계속 감)
    try:
        set_leverage_and_mode(SET.symbol, SET.leverage, SET.isolated)
    except Exception as e:
        log(f"[WARN] 레버리지/마진 설정 실패: {e}")

    # health 서버 백그라운드 실행
    start_health_server()

    # 거래소에 있는 포지션을 내부에 동기화
    sync_open_trades_from_exchange(SET.symbol)

    # KST 기준으로 마지막으로 일일결산 보낸 날짜
    now_kst = datetime.datetime.now(KST)
    last_report_date_kst = now_kst.strftime("%Y-%m-%d")

    # 일일 PnL
    daily_pnl: float = 0.0

    # 마지막 TP/SL 체결체크 시각
    last_fill_check: float = 0.0

    # 잔고 로그용
    last_balance_log: float = 0.0

    # 메인 무한 루프
    while RUNNING:
        try:
            # 메트릭스 최신화
            METRICS["last_loop_ts"] = time.time()
            METRICS["open_trades"] = len(OPEN_TRADES)
            METRICS["consec_losses"] = CONSEC_LOSSES
            METRICS["kline_failures"] = CONSEC_KLINE_FAILS

            now = time.time()

            # 1) 1분에 한 번 잔고 찍기
            if now - last_balance_log >= 60:
                get_available_usdt()
                last_balance_log = now

            # 2) KST 자정이면 일일 결산 메시지 보내고 각종 카운트 리셋
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

            # 3) 열린 포지션이 있으면 TP/SL 체결됐는지 주기적으로 확인
            if now - last_fill_check >= SET.poll_fills_sec:
                OPEN_TRADES, closed_list = check_closes(OPEN_TRADES, TRADER_STATE)
                for closed in closed_list:
                    t: Trade = closed["trade"]
                    reason: str = closed["reason"]
                    summary: Optional[Dict[str, Any]] = closed.get("summary")

                    # 체결 내역이 있으면 거기서 수량/가격/pnl을 가져온다.
                    closed_qty = summary.get("qty") if summary else t.qty
                    closed_price = summary.get("avg_price") if summary else 0.0
                    pnl = summary.get("pnl") if summary else None

                    # PnL 이 없으면 진입가와 TP/SL 가격으로 계산
                    if pnl is None or pnl == 0.0:
                        if reason == "TP":
                            if t.side == "BUY":
                                pnl = (t.tp_price - t.entry) * closed_qty
                            else:
                                pnl = (t.entry - t.tp_price) * closed_qty
                        else:  # SL 또는 FORCE_CLOSE
                            if t.side == "BUY":
                                pnl = (t.sl_price - t.entry) * closed_qty
                            else:
                                pnl = (t.entry - t.sl_price) * closed_qty

                    # 일일 PnL 누적
                    daily_pnl += pnl
                    # 전체 최근 청산 시각
                    LAST_CLOSE_TS = now

                    # 연속 손실 카운트 관리
                    if pnl < 0:
                        CONSEC_LOSSES += 1
                    else:
                        CONSEC_LOSSES = 0

                    # 전략별 최근 청산 시각 기록
                    if t.source == "TREND":
                        LAST_CLOSE_TS_TREND = now
                    elif t.source == "RANGE":
                        LAST_CLOSE_TS_RANGE = now

                    # 박스장 전략으로 들어갔다가 SL 나면 일일 카운트 올리고 일정 횟수 넘으면 그날은 비활성화
                    if t.source == "RANGE" and reason == "SL" and pnl < 0:
                        RANGE_DAILY_SL += 1
                        if RANGE_DAILY_SL >= SET.range_max_daily_sl:
                            RANGE_DISABLED_TODAY = True
                            send_tg(
                                f"🛑 [RANGE_OFF] 박스장 손절 {RANGE_DAILY_SL}회 → 오늘은 박스장 전략 비활성화"
                            )

                    # 청산 결과 텔레그램 전송
                    send_tg(
                        f"💰 청산({reason}) {t.symbol} {t.side} 수량={closed_qty} "
                        f"가격={closed_price:.2f} PnL={pnl:.2f} USDT (금일 누적 {daily_pnl:.2f})"
                    )

                # 다음 체크 시간 기록
                last_fill_check = now

                # TP/SL 재설정이 여러 번 실패해서 봇을 멈춰야 하는 경우
                if TRADER_STATE.should_stop_bot():
                    send_tg("🛑 TP/SL 재설정이 연속으로 실패해서 봇을 중단합니다.")
                    RUNNING = False
                    break

            # 4) 포지션이 열려 있으면 새 진입 안 함
            if OPEN_TRADES:
                time.sleep(1)
                continue

            # 5) 연속 손실 3회면 쉬기
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

            # 8) 3분봉 캔들 가져오기
            try:
                candles_3m = get_klines(SET.symbol, SET.interval, 120)
                CONSEC_KLINE_FAILS = 0
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

            # 캔들이 너무 적으면 전략 계산 불가
            if len(candles_3m) < 50:
                time.sleep(1)
                continue

            # 최신 3m 캔들이 너무 오래된 경우 (거래소 응답 지연 등) → 진입 스킵
            latest_3m_ts = candles_3m[-1][0]
            now_ms = int(time.time() * 1000)
            if now_ms - latest_3m_ts > SET.max_kline_delay_sec * 1000:
                send_skip_tg("[SKIP] 3m_kline_delayed: 최근 3m 캔들이 지연되었습니다.")
                time.sleep(1)
                continue

            # 같은 3m 캔들에서 이미 진입했다면 다시 진입하지 않는다.
            if latest_3m_ts == LAST_SIGNAL_TS_3M:
                send_skip_tg("[SKIP] same_3m_candle: 이미 이 캔들에서 진입했습니다.")
                time.sleep(1)
                continue

            # 9) 잔고 확인 (0 이면 1시간 스킵 텔레그램이 날아가게 되어 있음)
            avail = get_available_usdt()
            if avail <= 0:
                send_skip_tg("[BALANCE_SKIP] ⚠️ 가용 선물 잔고가 0입니다. 진입을 건너뜁니다.")
                time.sleep(3)
                continue

            # ───────── 시그널 판단 시작 ─────────
            chosen_signal: Optional[str] = None  # "LONG" / "SHORT"
            signal_source: Optional[str] = None  # "TREND" / "RANGE"

            # 1) 추세 전략 먼저 본다.
            candles_15m: Optional[List[Any]] = None
            if SET.enable_trend:
                candles_15m = get_klines(SET.symbol, "15m", 120)
                sig_3m = decide_signal_3m_trend(
                    candles_3m,
                    SET.rsi_overbought,
                    SET.rsi_oversold,
                )
                trend_15m = decide_trend_15m(candles_15m) if candles_15m else None

                # 3m 신호 있고 15m 방향과 같을 때만
                if sig_3m and trend_15m and sig_3m == trend_15m:
                    # 전략별 쿨다운
                    if (time.time() - LAST_CLOSE_TS_TREND) >= SET.cooldown_after_close_trend:
                        chosen_signal = sig_3m
                        signal_source = "TREND"
                        # 1분봉 확인 옵션
                        if SET.enable_1m_confirm:
                            candles_1m = get_klines(SET.symbol, "1m", 20)
                            if not confirm_1m_direction(candles_1m, chosen_signal):
                                send_skip_tg("[SKIP] 1m_confirm_mismatch: 1분봉이 방향 불일치")
                                chosen_signal = None
                                signal_source = None
                    else:
                        send_skip_tg("[SKIP] trend_cooldown: 직전 TREND 포지션 대기중")

            # 2) 추세 신호가 없을 때만 박스장 전략
            if (not chosen_signal) and SET.enable_range and (not RANGE_DISABLED_TODAY):
                # 15m 없으면 여기서라도 가져온다.
                if not candles_15m:
                    candles_15m = get_klines(SET.symbol, "15m", 120)

                # 박스장 전략 쿨다운
                if (time.time() - LAST_CLOSE_TS_RANGE) >= SET.cooldown_after_close_range:
                    # 시장 상태가 박스장 진입에 안 맞으면 스킵
                    if not should_block_range_today(candles_3m, candles_15m):
                        r_sig = decide_signal_range(candles_3m)
                        if r_sig:
                            chosen_signal = r_sig
                            signal_source = "RANGE"
                    else:
                        send_skip_tg("[SKIP] range_blocked_today: 박스장 조건 불리")
                else:
                    send_skip_tg("[SKIP] range_cooldown: 직전 RANGE 포지션 대기중")

            # 두 전략 모두 진입 조건이 안 나왔으면 다음 루프
            if not chosen_signal:
                time.sleep(1)
                continue

            # ───────── 진입 전에 여러 가드 확인 ─────────
            last_price = candles_3m[-1][4]
            prev_price = candles_3m[-2][4]
            move_pct = abs(last_price - prev_price) / prev_price
            if move_pct > SET.max_price_jump_pct:
                send_skip_tg(
                    f"[SKIP] price_jump_guard: {move_pct:.4f} > {SET.max_price_jump_pct:.4f}"
                )
                time.sleep(1)
                continue

            # 호가 스프레드 확인
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
                            if spread_pct > SET.max_spread_pct:
                                send_skip_tg(
                                    f"[SKIP] spread_guard: {spread_pct:.5f} > {SET.max_spread_pct:.5f}"
                                )
                                time.sleep(1)
                                continue
                    except Exception as e:
                        log(f"[SPREAD PARSE ERROR] {e}")

            # ATR 급등 시 리스크 자동 축소
            effective_risk_pct = SET.risk_pct
            if SET.use_atr:
                atr_fast = calc_atr(candles_3m, SET.atr_len)
                atr_slow = calc_atr(candles_3m, max(SET.atr_len * 2, SET.atr_len + 10))
                if atr_fast and atr_slow and atr_slow > 0:
                    if atr_fast > atr_slow * SET.atr_risk_high_mult:
                        effective_risk_pct = SET.risk_pct * SET.atr_risk_reduction
                        log(
                            f"[ATR RISK] fast ATR high → risk {SET.risk_pct} -> {effective_risk_pct}"
                        )

            # 주문 명목가 계산 (가용잔고 × 리스크 × 레버리지)
            notional = avail * effective_risk_pct * SET.leverage
            if notional < SET.min_notional_usdt:
                send_tg(f"⚠️ 계산된 주문 금액이 너무 작습니다: {notional:.2f} USDT")
                time.sleep(3)
                continue
            # 최대 주문 금액 제한
            notional = min(notional, SET.max_notional_usdt)

            # 수량 계산 (선물 기본 형태: 명목가 / 가격)
            qty = round(notional / last_price, 6)
            side_open = "BUY" if chosen_signal == "LONG" else "SELL"

            # TP/SL 비율 결정
            if signal_source == "RANGE":
                # 박스장은 짧게
                local_tp_pct = SET.range_tp_pct
                local_sl_pct = SET.range_sl_pct
            else:
                # 추세는 기본값 또는 ATR 기반
                local_tp_pct = SET.tp_pct
                local_sl_pct = SET.sl_pct
                if SET.use_atr:
                    atr_val = calc_atr(candles_3m, SET.atr_len)
                    if atr_val and last_price > 0:
                        sl_pct_atr = (atr_val * SET.atr_sl_mult) / last_price
                        tp_pct_atr = (atr_val * SET.atr_tp_mult) / last_price
                        local_sl_pct = max(sl_pct_atr, SET.min_sl_pct)
                        local_tp_pct = max(tp_pct_atr, SET.min_tp_pct)

            # 한글로 전략/방향 표시
            strategy_kr = "추세장" if signal_source == "TREND" else "박스장"
            direction_kr = "롱" if chosen_signal == "LONG" else "숏"

            # 진입 알림
            send_tg(
                f"[ENTRY][{signal_source}] 🟢 진입 시도: {SET.symbol} 전략={strategy_kr} 방향={direction_kr} "
                f"레버리지={SET.leverage}x 사용비율={effective_risk_pct*100:.0f}% "
                f"명목가≈{notional:.2f}USDT 수량={qty}"
            )

            # 실제 진입 + TP/SL 예약
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
                # 진입 실패 시 너무 많이 재시도하지 않도록 잠깐 쉰다.
                time.sleep(2)
                continue

            # 예약 성공 알림
            send_tg(
                f"[ENTRY][{signal_source}] 📌 예약완료: TP={trade.tp_price} / SL={trade.sl_price}"
            )

            # 내부 상태에 추가
            OPEN_TRADES.append(trade)

            # 같은 3m 캔들에서 다시는 안 들어가도록 기록
            LAST_SIGNAL_TS_3M = latest_3m_ts

            # 진입 후 공통 쿨다운
            time.sleep(SET.cooldown_sec)

        except Exception as e:
            # 최상위에서 예외를 모두 잡아 텔레그램으로 알려주고 루프는 계속 돈다.
            log(f"ERROR: {e}")
            send_tg(f"❌ 오류 발생: {e}")
            time.sleep(2)

    # 루프가 끝났다면 여기로 온다.
    uptime = time.time() - START_TS
    if (not TERMINATED_BY_SIGNAL) and (uptime >= SET.min_uptime_for_stop):
        send_tg("🛑 봇이 종료되었습니다.")
    else:
        log(f"[SKIP STOP] sig={TERMINATED_BY_SIGNAL} uptime={uptime:.2f}s")


# 이 파일을 직접 실행했을 때만 main() 실행
if __name__ == "__main__":
    main()
