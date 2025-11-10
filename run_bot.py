"""
run_bot.py (메인 흐름 전용)
====================================================
이 파일은 "봇이 어떻게 돈다"만 담당한다.
세부 기능은 모듈로 분리한다.

2025-11-10 보완 사항
- entry_guards.py 쪽에서 3분 거래량 평균이 아주 낮은 시간대는 통과시키도록 완화했다.
  이 파일은 그 가드를 그대로 호출만 하므로 흐름 설명에 그 점을 명시했다.
- 나머지 구조는 동일하며, 시그널 → 가드 → 주문 순서는 그대로 유지된다.

의존 모듈 (다음 순서로 제공):
1) bot_workers.py
   - start_health_server()
   - start_drive_sync_thread()
   - start_telegram_command_thread(on_stop_command)
2) signal_flow.py
   - get_trading_signal(...)  → 시그널, 캔들 세트, 부가 정보 반환
3) entry_guards.py
   - check_manual_position_guard(...)
   - check_volume_guard(...)
   - check_price_jump_guard(...)
   - check_spread_guard(...)

기존 파일들은 그대로 사용:
- settings.py, exchange_api.py, trader.py, signals_logger.py, telelog.py

메인 루프 구조:
1. STOP_FLAG 있으면 바로 종료
2. 설정 로드 후 health/drive/tg 스레드 시작
3. 시작 시 거래소 포지션 동기화
4. 무한 루프 돌면서
   4-1) TP/SL 체결 확인
   4-2) 종료 플래그 확인
   4-3) 포지션 있으면 다음 루프
   4-4) 시그널 요청 → (완화된) 가드 → 진입 → 로그
5. 안전 종료면 STOP_FLAG 남기고 끝낸다.
"""

from __future__ import annotations

import os
import time
import datetime
import signal
from typing import Any, Dict, List, Optional, Tuple

from settings import load_settings, KST
from telelog import log, send_tg, send_skip_tg
from exchange_api import (
    get_available_usdt,
    fetch_open_positions,
    fetch_open_orders,
    set_leverage_and_mode,
    get_balance_detail,
)
from trader import Trade, TraderState, open_position_with_tp_sl, check_closes
from signals_logger import log_signal

# 분리된 모듈
from bot_workers import (
    start_health_server,
    start_drive_sync_thread,
    start_telegram_command_thread,
)
from signal_flow import get_trading_signal
from entry_guards import (
    check_manual_position_guard,
    check_volume_guard,
    check_price_jump_guard,
    check_spread_guard,
)

# ─────────────────────────────
# 전역 상태
# ─────────────────────────────
SET = load_settings()
START_TS: float = time.time()
RUNNING: bool = True
TERMINATED_BY_SIGNAL: bool = False

# 간단 메트릭
METRICS: Dict[str, Any] = {
    "start_ts": START_TS,
    "last_loop_ts": START_TS,
    "open_trades": 0,
    "consec_losses": 0,
    "kline_failures": 0,
}

# 현재 우리가 알고 있는 포지션
OPEN_TRADES: List[Trade] = []
# TP/SL 재설정 실패 횟수 관리
TRADER_STATE: TraderState = TraderState()
# 최근 청산 시각
LAST_CLOSE_TS: float = 0.0
# 전략별 최근 청산 시각
LAST_CLOSE_TS_TREND: float = 0.0
LAST_CLOSE_TS_RANGE: float = 0.0
# 연속 손실 수
CONSEC_LOSSES: int = 0
# 텔레그램에서 온 종료 플래그
SAFE_STOP_REQUESTED: bool = False
# 거래소 포지션 재동기화 시각
LAST_EXCHANGE_SYNC_TS: float = 0.0


# ─────────────────────────────
# STOP_FLAG 유틸
# ─────────────────────────────
def _write_stop_flag() -> None:
    """재시작 차단용 파일 생성"""
    try:
        with open("STOP_FLAG", "w", encoding="utf-8") as f:
            f.write("stop\n")
    except OSError as e:
        log(f"[STOP_FLAG] write failed: {e}")


# ─────────────────────────────
# SIGTERM 핸들러
# ─────────────────────────────
def _sigterm(*_: Any) -> None:
    """컨테이너에서 SIGTERM 오면 자연스럽게 빠져나오게 한다."""
    global RUNNING, TERMINATED_BY_SIGNAL
    TERMINATED_BY_SIGNAL = True
    RUNNING = False


signal.signal(signal.SIGTERM, _sigterm)


# ─────────────────────────────
# 거래소 → 내부 포지션 동기화 (간단 버전)
# ─────────────────────────────
def sync_open_trades_from_exchange(symbol: str, replace: bool = True) -> None:
    """
    거래소 포지션/열린 주문을 내부 리스트로 옮겨 중복 진입을 막는다.
    시작할 때 1번, 이후에는 주기적으로 호출한다.
    """
    global OPEN_TRADES

    positions = fetch_open_positions(symbol)
    orders = fetch_open_orders(symbol)

    if replace:
        OPEN_TRADES = []

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

        # 열린 주문에서 TP/SL 찾아서 붙인다 (간단 매핑)
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
        send_tg(f"🔁 거래소 포지션 {len(OPEN_TRADES)}건 동기화했습니다. (중복 진입 방지)")


# ─────────────────────────────
# 텔레그램 종료 콜백
# ─────────────────────────────
def _on_safe_stop() -> None:
    """
    텔레그램 명령 스레드에서 호출하는 콜백.
    여기서는 플래그만 세우고 실제 종료는 메인 루프가 판단한다.
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

    # 1) STOP_FLAG 있으면 바로 종료
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

    # 3) 시작 메시지
    log(
        f"CONFIG: ENABLE_TREND={SET.enable_trend}, ENABLE_RANGE={SET.enable_range}, ENABLE_1M_CONFIRM={SET.enable_1m_confirm}"
    )
    send_tg("✅ [봇 시작] BingX 선물 자동매매 시작합니다.")

    # 4) 레버리지/마진 모드 설정 (실패해도 진행)
    try:
        set_leverage_and_mode(SET.symbol, SET.leverage, SET.isolated)
    except Exception as e:
        # 여기서 멈추지 않는 게 중요하다.
        log(f"[WARN] 레버리지/마진 설정 실패: {e}")

    # 5) 보조 스레드 시작
    start_health_server()
    start_drive_sync_thread()
    start_telegram_command_thread(on_stop_command=_on_safe_stop)

    # 6) 거래소 포지션 1회 동기화
    sync_open_trades_from_exchange(SET.symbol, replace=True)
    LAST_EXCHANGE_SYNC_TS = time.time()

    # 일일 정산용
    now_kst = datetime.datetime.now(KST)
    last_report_date_kst = now_kst.strftime("%Y-%m-%d")
    daily_pnl: float = 0.0

    # 재동기화 주기
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

            # 0) 주기적 포지션 재동기화
            if now - LAST_EXCHANGE_SYNC_TS >= position_resync_sec:
                sync_open_trades_from_exchange(SET.symbol, replace=True)
                LAST_EXCHANGE_SYNC_TS = now

            # 1) 1분에 한 번 가용 잔고 찍어두기 (에러 추적용)
            if now - last_balance_log >= 60:
                get_available_usdt()
                last_balance_log = now

            # 2) 자정(KST) 리셋
            now_kst = datetime.datetime.now(KST)
            today_kst = now_kst.strftime("%Y-%m-%d")
            if now_kst.hour == 0 and now_kst.minute < 1 and last_report_date_kst != today_kst:
                send_tg(
                    f"📊 일일 정산(KST): PnL {daily_pnl:.2f} USDT, 연속 손실 {CONSEC_LOSSES}"
                )
                daily_pnl = 0.0
                CONSEC_LOSSES = 0
                last_report_date_kst = today_kst

            # 3) 체결 확인
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

                    if pnl < 0:
                        CONSEC_LOSSES += 1
                    else:
                        CONSEC_LOSSES = 0

                    if t.source == "TREND":
                        LAST_CLOSE_TS_TREND = now
                    elif t.source == "RANGE":
                        LAST_CLOSE_TS_RANGE = now

                    send_tg(
                        f"💰 청산({reason}) {t.symbol} {t.side} 수량={closed_qty} 가격={closed_price:.2f} PnL={pnl:.2f} USDT"
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

                # TP/SL 재설정 실패로 인한 종료
                if TRADER_STATE.should_stop_bot():
                    send_tg("🛑 TP/SL 재설정이 연속 실패해서 봇을 중단합니다.")
                    _write_stop_flag()
                    RUNNING = False
                    break

                # 종료 명령 들어왔고 포지션이 비었다면 여기서 끝
                if SAFE_STOP_REQUESTED and not OPEN_TRADES:
                    send_tg("🛑 종료 명령에 따라 포지션이 없어 종료합니다.")
                    _write_stop_flag()
                    RUNNING = False
                    break

            # 포지션 살아 있으면 신규 진입 안 함
            if OPEN_TRADES:
                time.sleep(1)
                continue

            # 종료 플래그가 있는데 포지션이 없는 경우 → 진입하지 않고 종료
            if SAFE_STOP_REQUESTED:
                send_tg("🛑 종료 명령에 따라 새 진입 없이 종료합니다.")
                _write_stop_flag()
                RUNNING = False
                break

            # 연속 손실 3회면 휴식
            if CONSEC_LOSSES >= 3:
                send_tg("⛔ 연속 3회 손실 → 휴식 진입")
                time.sleep(SET.cooldown_after_3loss)
                CONSEC_LOSSES = 0
                continue

            # 최근 청산 후 쿨다운
            if LAST_CLOSE_TS > 0 and (time.time() - LAST_CLOSE_TS) < SET.cooldown_after_close:
                time.sleep(1)
                continue

            # 4) 시그널 + 캔들 가져오기 (여기서 TREND/RANGE/완화 RANGE가 결정된다)
            signal_ctx = get_trading_signal(
                settings=SET,
                last_trend_close_ts=LAST_CLOSE_TS_TREND,
                last_range_close_ts=LAST_CLOSE_TS_RANGE,
            )
            if signal_ctx is None:
                # 시그널 없음 → 다음 루프
                time.sleep(1)
                continue

            (
                chosen_signal,
                signal_source,
                latest_ts,
                candles_3m,
                candles_3m_raw,
                last_price,
                extra,
            ) = signal_ctx

            # 5) 진입 전 가드 실행
            # 5-1) 수동 포지션/usedMargin 가드
            manual_ok = check_manual_position_guard(
                get_balance_detail_func=get_balance_detail,
                symbol=SET.symbol,
                latest_ts=latest_ts,
            )
            if not manual_ok:
                time.sleep(2)
                continue

            # 5-2) 거래량 가드
            #     entry_guards 에서 평균 거래량이 아주 낮은 시간대는 통과시키도록 이미 완화되어 있음.
            vol_ok = check_volume_guard(
                settings=SET,
                candles_3m_raw=candles_3m_raw,
                latest_ts=latest_ts,
                signal_source=signal_source,
                direction=chosen_signal,
            )
            if not vol_ok:
                time.sleep(1)
                continue

            # 5-3) 가격 점프 가드
            price_ok = check_price_jump_guard(
                settings=SET,
                candles_3m=candles_3m,
                latest_ts=latest_ts,
                signal_source=signal_source,
                direction=chosen_signal,
            )
            if not price_ok:
                time.sleep(1)
                continue

            # 5-4) 스프레드 / 호가 체크
            spread_ok, best_bid, best_ask = check_spread_guard(
                settings=SET,
                symbol=SET.symbol,
                latest_ts=latest_ts,
                signal_source=signal_source,
                direction=chosen_signal,
            )
            if not spread_ok:
                time.sleep(1)
                continue

            # 6) 잔고 확인 및 주문 수량 계산
            avail = get_available_usdt()
            if avail <= 0:
                send_skip_tg("[BALANCE_SKIP] 가용 선물 잔고 0 → 진입 안함")
                log_signal(
                    event="SKIP",
                    symbol=SET.symbol,
                    strategy_type=signal_source,
                    direction=chosen_signal,
                    reason="balance_zero",
                    candle_ts=latest_ts,
                )
                time.sleep(3)
                continue

            # ATR 에서 리스크를 줄여오면 그걸 우선 사용
            effective_risk_pct = extra.get("effective_risk_pct", SET.risk_pct) if extra else SET.risk_pct
            notional = avail * effective_risk_pct * SET.leverage
            if notional < SET.min_notional_usdt:
                send_tg(f"⚠️ 주문 금액이 너무 작습니다: {notional:.2f} USDT")
                log_signal(
                    event="SKIP",
                    symbol=SET.symbol,
                    strategy_type=signal_source,
                    direction=chosen_signal,
                    reason="notional_too_small",
                    candle_ts=latest_ts,
                )
                time.sleep(3)
                continue
            notional = min(notional, SET.max_notional_usdt)
            qty = round(notional / last_price, 6)
            side_open = "BUY" if chosen_signal == "LONG" else "SELL"

            # 7) TP/SL 퍼센트는 시그널 쪽에서 넘겨준 게 있으면 그걸 쓰고, 없으면 settings 기본값
            tp_pct = extra.get("tp_pct") if extra else None
            sl_pct = extra.get("sl_pct") if extra else None
            if tp_pct is None or sl_pct is None:
                if signal_source == "RANGE":
                    tp_pct = SET.range_tp_pct
                    sl_pct = SET.range_sl_pct
                else:
                    tp_pct = SET.tp_pct
                    sl_pct = SET.sl_pct

            # 8) 호가 기반 진입가 힌트
            entry_price_hint = last_price
            if SET.use_orderbook_entry_hint and best_bid and best_ask:
                entry_price_hint = best_ask if side_open == "BUY" else best_bid

            # 9) 최종 진입
            trade = open_position_with_tp_sl(
                settings=SET,
                symbol=SET.symbol,
                side_open=side_open,
                qty=qty,
                entry_price_hint=entry_price_hint,
                tp_pct=tp_pct,
                sl_pct=sl_pct,
                source=signal_source,
            )
            if trade is None:
                log_signal(
                    event="SKIP",
                    symbol=SET.symbol,
                    strategy_type=signal_source,
                    direction=chosen_signal,
                    reason="entry_failed",
                    candle_ts=latest_ts,
                )
                time.sleep(2)
                continue

            send_tg(
                f"[ENTRY][{signal_source}] 예약완료: TP={trade.tp_price} / SL={trade.sl_price} 수량={trade.qty}"
            )
            log_signal(
                event="ENTRY_OPENED",
                symbol=trade.symbol,
                strategy_type=trade.source,
                direction=trade.side,
                price=trade.entry,
                qty=trade.qty,
                tp_price=trade.tp_price,
                sl_price=trade.sl_price,
                candle_ts=latest_ts,
            )
            OPEN_TRADES.append(trade)

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

    # 루프 끝
    uptime = time.time() - START_TS
    if (not TERMINATED_BY_SIGNAL) and (uptime >= SET.min_uptime_for_stop):
        send_tg("🛑 봇이 종료되었습니다.")
    else:
        log(f"[SKIP STOP] sig={TERMINATED_BY_SIGNAL} uptime={uptime:.2f}s")


if __name__ == "__main__":
    main()
