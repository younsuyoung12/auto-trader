"""
run_bot.py
메인 루프: 시그널 받기 → 진입 → 포지션 모니터링 → 종료/대기

2025-11-12 리팩터링 + 실시간 대응 추가 요약
----------------------------------------------------
1) 주문 후 대응 로직 분리
   - 포지션 체결 후에 할 일(박스 조기익절/역행컷, 박스→추세 업그레이드)을
     run_bot.py 안에서 직접 하지 않고 position_watch.py 로 넘긴다.
   - run_bot.py 는 포지션 리스트만 넘기고, 닫혔다는 결과만 반영한다.

2) 거래소 ↔ 내부 포지션 동기화 분리
   - 거래소에 실제로 열려 있는 포지션과 내부 OPEN_TRADES 를 맞추는 코드를
     sync_exchange.py 로 뺐다.
   - 주기적으로 sync_open_trades_from_exchange(...) 를 호출해서 OPEN_TRADES 를 교체한다.

3) 시그널 → 가드 → 진입 플로우 분리
   - 진입 전에 하던 시그널 계산, 각종 가드(잔고, 스프레드, 점프) 검사, 실제 주문 열기까지를
     entry_flow.py 로 옮겼다.
   - run_bot.py 는 try_open_new_position(...) 에게 “지금 진입해도 돼?”라고 물어보고
     (trade, sleep_sec) 만 받아서 처리한다.

4) 시세 스냅샷 기록 기능 분산
   - 진입할 때 필요한 기록은 entry_flow.py 에서,
   - 포지션을 들고 있는 동안의 대응(박스 조기익절/역행컷, 박스→추세 전환)은
     position_watch.py 에서 각각 처리한다.

5) 박스 포지션 실시간 대응 호출
   - run_bot.py 메인 루프에서 포지션이 살아 있는 경우
     position_watch.maybe_early_exit_range(...) 를 호출해서
     settings 에서 허용한 박스 조기익절 / 조기청산을 즉시 실행한다.

6) 박스 → 추세 업그레이드 호출
   - 같은 위치에서 position_watch.maybe_upgrade_range_to_trend(...) 를 불러서
     “처음엔 RANGE 로 들어갔는데 지금 보니 추세가 열렸다” 상황이면
     그 포지션을 정리하고 추세 쪽으로 재구성할 수 있게 했다.

7) ✅ 추세(TREND) 포지션 실시간 조기 대응 호출
   - position_watch.maybe_early_exit_trend(...) 를 호출해서
     추세로 들어갔는데 바로 횡보/역행이 나올 때 설정값에 따라 먼저 잘라낼 수 있게 했다.

8) ✅ 열린 포지션과 “정반대 방향” 새 시그널이 나오면 즉시 정리
   - position_watch.maybe_force_close_on_opposite_signal(...) 을 호출해서
     현재 들고 있는 방향과 새로 감지한 신호 방향이 반대면 시장가로 먼저 닫는다.
   - 추세 ↔ 추세, 박스 ↔ 추세, 박스 ↔ 박스 모두 같은 흐름으로 처리한다.
"""

from __future__ import annotations

import os
import time
import datetime
import signal
from typing import Any, Dict, List, Optional

from settings import load_settings, KST
from telelog import log, send_tg, send_skip_tg
from exchange_api import (
    get_available_usdt,
    set_leverage_and_mode,
    get_balance_detail,
)
from trader import Trade, TraderState, check_closes
from signals_logger import log_signal

from bot_workers import (
    start_health_server,
    start_drive_sync_thread,
    start_telegram_command_thread,
)
from signal_analysis_worker import start_signal_analysis_thread

# 새로 분리한 모듈들
from sync_exchange import sync_open_trades_from_exchange
from entry_flow import try_open_new_position
from position_watch import (
    maybe_early_exit_range,
    maybe_upgrade_range_to_trend,
    maybe_early_exit_trend,
    # 🔥 반대 방향 새 신호 감지해서 닫는 함수 (position_watch 쪽에 구현돼 있어야 한다)
    maybe_force_close_on_opposite_signal,
)

# ─────────────────────────────
# 전역 상태
# ─────────────────────────────
SET = load_settings()
START_TS: float = time.time()
RUNNING: bool = True
TERMINATED_BY_SIGNAL: bool = False

# 텔레그램에 보여줄 때 환산해서 쓰는 환율
KRW_RATE: float = getattr(SET, "krw_per_usdt", 1400.0)

# 간단 상태값
METRICS: Dict[str, Any] = {
    "start_ts": START_TS,
    "last_loop_ts": START_TS,
    "open_trades": 0,
    "consec_losses": 0,
    "kline_failures": 0,
}

# 현재 들고 있다고 알고 있는 포지션 목록
OPEN_TRADES: List[Trade] = []
# TP/SL 재설정 실패 횟수 등 관리
TRADER_STATE: TraderState = TraderState()
# 가장 최근에 청산이 있었던 시각
LAST_CLOSE_TS: float = 0.0
# 전략별 최근 청산 시각
LAST_CLOSE_TS_TREND: float = 0.0
LAST_CLOSE_TS_RANGE: float = 0.0
# 연속 손실 횟수
CONSEC_LOSSES: int = 0
# 텔레그램에서 넘어온 안전 종료 플래그
SAFE_STOP_REQUESTED: bool = False
# 거래소 포지션을 마지막으로 동기화한 시각
LAST_EXCHANGE_SYNC_TS: float = 0.0
# 포지션이 살아 있을 때 주기 상태 알림용
LAST_STATUS_TG_TS: float = 0.0
STATUS_TG_INTERVAL_SEC: int = getattr(SET, "unrealized_notify_sec", 1800)


# ─────────────────────────────
# STOP_FLAG 유틸
# ─────────────────────────────
def _write_stop_flag() -> None:
    """
    다음 실행을 막기 위한 플래그 파일을 만든다.
    run_bot.py 시작부에서 이 파일이 있으면 바로 종료한다.
    """
    try:
        with open("STOP_FLAG", "w", encoding="utf-8") as f:
            f.write("stop\n")
    except OSError as e:
        log(f"[STOP_FLAG] write failed: {e}")


# ─────────────────────────────
# idle 모드
# ─────────────────────────────
def _enter_idle_forever() -> None:
    """
    봇 로직을 멈추고 프로세스만 살려두는 루프.
    컨테이너/프로세스는 살아 있으니 외부에서 재시작할 수 있다.
    """
    log("[IDLE] safe stop reached, entering idle loop...")
    try:
        send_tg("🟡 봇을 멈춘 상태로 유지합니다. 재시작은 컨테이너/프로세스로 해주세요.")
    except Exception:
        pass
    while True:
        time.sleep(60)


# ─────────────────────────────
# SIGTERM 핸들러
# ─────────────────────────────
def _sigterm(*_: Any) -> None:
    """
    컨테이너에서 SIGTERM 이 들어오면 자연스럽게 메인 루프를 빠져나오도록 한다.
    """
    global RUNNING, TERMINATED_BY_SIGNAL
    TERMINATED_BY_SIGNAL = True
    RUNNING = False


# 실제로 시그널 등록
signal.signal(signal.SIGTERM, _sigterm)


# ─────────────────────────────
# 포지션 상태 텔레그램 알림
# ─────────────────────────────
def _send_open_positions_status(symbol: str, interval_sec: int) -> None:
    """
    포지션이 살아 있을 때 주기적으로 텔레그램으로 상태를 보내는 부분.
    (원래 run_bot.py 에 있던 로직 그대로 가져온 형태)
    """
    from exchange_api import fetch_open_positions
    from market_data import get_klines
    from strategies_trend import decide_trend_15m

    try:
        positions = fetch_open_positions(symbol)
    except Exception as e:
        log(f"[STATUS_TG] fetch_open_positions error: {e}")
        return

    if not positions:
        return

    # 15m 추세 꼬리 붙이기
    try:
        candles_15m = get_klines(symbol, "15m", 120)
        if candles_15m:
            trimmed = [c[:5] for c in candles_15m]
            trend_dir = decide_trend_15m(trimmed)
            if trend_dir == "LONG":
                trend_txt = " (15m 추세: 상승)"
            elif trend_dir == "SHORT":
                trend_txt = " (15m 추세: 하락)"
            else:
                trend_txt = " (15m 추세: 중립)"
        else:
            trend_txt = ""
    except Exception:
        trend_txt = ""

    mins = max(1, interval_sec // 60)
    lines = [f"⏱ 포지션 상태({mins}m){trend_txt}:"]

    for p in positions:
        qty = float(p.get("positionAmt") or p.get("quantity") or p.get("size") or 0.0)
        upnl = float(p.get("unrealizedProfit") or 0.0)
        # 거래소에서 방향이 오면 그걸 우선
        pos_side_raw = (p.get("positionSide") or "").upper()
        if pos_side_raw in ("LONG", "BOTH"):
            side_text = "LONG"
        elif pos_side_raw == "SHORT":
            side_text = "SHORT"
        else:
            # 없으면 수량 부호로 추정
            side_text = "LONG" if qty > 0 else "SHORT"

        upnl_krw = upnl * KRW_RATE
        lines.append(
            f"- {symbol} {side_text} 수량={abs(qty)} 미실현={upnl:.2f} USDT (~{upnl_krw:,.0f} KRW)"
        )

    send_tg("\n".join(lines))


# ─────────────────────────────
# 텔레그램 종료 콜백
# ─────────────────────────────
def _on_safe_stop() -> None:
    """
    텔레그램 스레드에서 “멈춰” 명령이 들어오면 실행되는 콜백.
    실제 종료는 메인 루프에서 SAFE_STOP_REQUESTED 를 보고 처리한다.
    """
    global SAFE_STOP_REQUESTED
    SAFE_STOP_REQUESTED = True
    send_tg("🛑 종료 명령 수신: 포지션 정리 후 종료합니다.")


# ─────────────────────────────
# 메인 루프
# ─────────────────────────────
def main() -> None:
    global RUNNING
    global OPEN_TRADES, LAST_CLOSE_TS, LAST_CLOSE_TS_TREND, LAST_CLOSE_TS_RANGE
    global CONSEC_LOSSES, SAFE_STOP_REQUESTED, LAST_EXCHANGE_SYNC_TS
    global LAST_STATUS_TG_TS

    # 1) STOP_FLAG 있으면 시작 안 함
    if os.path.exists("STOP_FLAG"):
        log("STOP_FLAG detected on startup. exiting without start.")
        send_tg("⛔ 이전 종료 명령이 있어 실행하지 않습니다. (STOP_FLAG 삭제 후 재실행)")
        return

    # 2) API 키 확인
    if not SET.api_key or not SET.api_secret:
        msg = "❗ BINGX_API_KEY / BINGX_API_SECRET 이 비어있습니다. 환경변수 설정 필요."
        log(msg)
        send_tg(msg)
        return

    # 3) 시작 로그
    log(
        f"CONFIG: ENABLE_TREND={SET.enable_trend}, ENABLE_RANGE={SET.enable_range}, ENABLE_1M_CONFIRM={SET.enable_1m_confirm}"
    )
    send_tg("✅ [봇 시작] BingX 선물 자동매매 시작합니다.")

    # 4) 레버리지/마진 모드 설정 (실패해도 계속)
    try:
        set_leverage_and_mode(SET.symbol, SET.leverage, SET.isolated)
    except Exception as e:
        log(f"[WARN] 레버리지/마진 설정 실패: {e}")

    # 5) 보조 스레드 시작
    start_health_server()
    start_drive_sync_thread()
    start_telegram_command_thread(on_stop_command=_on_safe_stop)
    start_signal_analysis_thread(interval_sec=1800)  # ← 30분마다 매매 요약 보내기

    # 6) 최초 포지션 동기화
    OPEN_TRADES, _ = sync_open_trades_from_exchange(
        SET.symbol,
        replace=True,
        current_trades=OPEN_TRADES,
    )
    LAST_EXCHANGE_SYNC_TS = time.time()

    # 7) 일일 정산용 데이터
    now_kst = datetime.datetime.now(KST)
    last_report_date_kst = now_kst.strftime("%Y-%m-%d")
    daily_pnl: float = 0.0

    # 8) 주기 설정
    position_resync_sec = getattr(SET, "position_resync_sec", 20)
    last_fill_check: float = 0.0
    last_balance_log: float = 0.0

    # ─────────────────────────────
    # 메인 무한 루프
    # ─────────────────────────────
    while RUNNING:
        try:
            METRICS["last_loop_ts"] = time.time()
            METRICS["open_trades"] = len(OPEN_TRADES)
            METRICS["consec_losses"] = CONSEC_LOSSES

            now = time.time()

            # (a) 거래소 포지션 주기 동기화
            if now - LAST_EXCHANGE_SYNC_TS >= position_resync_sec:
                OPEN_TRADES, _ = sync_open_trades_from_exchange(
                    SET.symbol,
                    replace=True,
                    current_trades=OPEN_TRADES,
                )
                LAST_EXCHANGE_SYNC_TS = now

            # (b) 1분마다 잔고 찍기
            if now - last_balance_log >= 60:
                get_available_usdt()
                last_balance_log = now

            # (c) 자정 정산
            now_kst = datetime.datetime.now(KST)
            today_kst = now_kst.strftime("%Y-%m-%d")
            if now_kst.hour == 0 and now_kst.minute < 1 and last_report_date_kst != today_kst:
                send_tg(f"📊 일일 정산(KST): PnL {daily_pnl:.2f} USDT, 연속 손실 {CONSEC_LOSSES}")
                daily_pnl = 0.0
                CONSEC_LOSSES = 0
                last_report_date_kst = today_kst

            # (d) 거래소 체결/청산 확인
            if now - last_fill_check >= SET.poll_fills_sec:
                OPEN_TRADES, closed_list = check_closes(OPEN_TRADES, TRADER_STATE)
                for closed in closed_list:
                    t: Trade = closed["trade"]
                    reason: str = closed["reason"]
                    summary: Optional[Dict[str, Any]] = closed.get("summary")

                    closed_qty = summary.get("qty") if summary else t.qty
                    closed_price = summary.get("avg_price") if summary else 0.0
                    pnl = summary.get("pnl") if summary else 0.0

                    daily_pnl += pnl
                    LAST_CLOSE_TS = now

                    # 연속 손실 카운트
                    if pnl < 0:
                        CONSEC_LOSSES += 1
                    else:
                        CONSEC_LOSSES = 0

                    # 전략별 최근 청산 시각
                    if t.source == "TREND":
                        LAST_CLOSE_TS_TREND = now
                    elif t.source == "RANGE":
                        LAST_CLOSE_TS_RANGE = now

                    pnl_krw = pnl * KRW_RATE
                    if getattr(SET, "notify_on_close", True):
                        send_tg(
                            f"💰 청산({reason}) {t.symbol} {t.side} 수량={closed_qty} "
                            f"가격={closed_price:.2f} PnL={pnl:.2f} USDT (~{pnl_krw:,.0f} KRW)"
                        )

                    log_signal(
                        event="CLOSE",
                        symbol=t.symbol,
                        strategy_type=t.source,
                        direction=t.side,
                        price=closed_price,
                        qty=closed_qty,
                        reason=reason,
                        pnl=pnl,
                    )

                last_fill_check = now

                # TP/SL 재설정이 너무 많이 실패하면 봇 중단
                if TRADER_STATE.should_stop_bot():
                    send_tg("🛑 TP/SL 재설정이 연속 실패해서 봇을 중단합니다.")
                    _write_stop_flag()
                    _enter_idle_forever()

                # 텔레그램에서 종료 요청 왔고, 포지션까지 비었으면 종료
                if SAFE_STOP_REQUESTED and not OPEN_TRADES:
                    send_tg("🛑 종료 명령에 따라 포지션이 없어 종료합니다.")
                    _write_stop_flag()
                    _enter_idle_forever()

            # (e) 포지션이 살아있는 경우: 주문 후 대응만 수행
            if OPEN_TRADES:
                for t in list(OPEN_TRADES):
                    # 1) 박스 조기익절 / 조기청산
                    if maybe_early_exit_range(t, SET):
                        OPEN_TRADES = [ot for ot in OPEN_TRADES if ot is not t]
                        LAST_CLOSE_TS = now
                        LAST_CLOSE_TS_RANGE = now
                        continue

                    # 2) 박스 → 추세 업그레이드
                    if maybe_upgrade_range_to_trend(
                        t, SET, LAST_CLOSE_TS_TREND, LAST_CLOSE_TS_RANGE
                    ):
                        OPEN_TRADES = [ot for ot in OPEN_TRADES if ot is not t]
                        LAST_CLOSE_TS = now
                        LAST_CLOSE_TS_RANGE = now
                        continue

                    # 3) ❗ 반대 방향 새 시그널 나오면 즉시 정리
                    if maybe_force_close_on_opposite_signal(
                        t, SET, LAST_CLOSE_TS_TREND, LAST_CLOSE_TS_RANGE
                    ):
                        OPEN_TRADES = [ot for ot in OPEN_TRADES if ot is not t]
                        LAST_CLOSE_TS = now
                        # 어떤 전략에서 왔든 “반대 신호로 닫힌 것”이므로 둘 다 갱신해도 무방
                        LAST_CLOSE_TS_TREND = now
                        LAST_CLOSE_TS_RANGE = now
                        continue

                    # 4) ✅ 추세 포지션 조기 대응 (횡보/역행 컷)
                    if maybe_early_exit_trend(t, SET):
                        OPEN_TRADES = [ot for ot in OPEN_TRADES if ot is not t]
                        LAST_CLOSE_TS = now
                        LAST_CLOSE_TS_TREND = now
                        continue

                # 아직 남은 포지션이 있으면 상태 알림만 하고 다음 루프로
                if OPEN_TRADES:
                    if (
                        getattr(SET, "unrealized_notify_enabled", False)
                        and now - LAST_STATUS_TG_TS >= STATUS_TG_INTERVAL_SEC
                    ):
                        _send_open_positions_status(SET.symbol, STATUS_TG_INTERVAL_SEC)
                        LAST_STATUS_TG_TS = now
                    time.sleep(1)
                    continue

            # (f) 포지션이 없는 상태에서 종료 명령이 들어온 경우
            if SAFE_STOP_REQUESTED:
                send_tg("🛑 종료 명령에 따라 새 진입 없이 종료합니다.")
                _write_stop_flag()
                _enter_idle_forever()

            # (g) 연속 손실 방어
            if CONSEC_LOSSES >= 3:
                send_tg("⛔ 연속 3회 손실 → 휴식 진입")
                time.sleep(SET.cooldown_after_3loss)
                CONSEC_LOSSES = 0
                continue

            # (h) 직후 공통 쿨다운
            if LAST_CLOSE_TS > 0 and (time.time() - LAST_CLOSE_TS) < SET.cooldown_after_close:
                time.sleep(1)
                continue

            # (i) 새 진입 시도
            trade, sleep_sec = try_open_new_position(SET, LAST_CLOSE_TS_TREND, LAST_CLOSE_TS_RANGE)
            if trade:
                OPEN_TRADES.append(trade)
                LAST_STATUS_TG_TS = time.time()

            time.sleep(sleep_sec)

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

    _enter_idle_forever()


if __name__ == "__main__":
    main()
