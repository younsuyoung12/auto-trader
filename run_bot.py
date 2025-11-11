"""
run_bot.py (메인 흐름 전용)
====================================================
이 파일은 "봇이 어떻게 돈다"만 담당한다.
세부 기능은 모듈로 분리한다.

2025-11-12 보완 사항 ⑤  ← 이번 수정
----------------------------------------------------
(trader.py 8차 수정과 연동)

1) trader.open_position_with_tp_sl(...) 에서 새로 받은 파라미터를 실제로 넘겨준다.
   - soft_mode: bool
   - sl_floor_ratio: Optional[float]

+ 2025-11-12 추가 보완
----------------------------------------------------
텔레그램에서 종료를 눌렀을 때 프로세스가 완전히 끝나서
호스팅쪽에서 다시 재시작되는 걸 막기 위해,
종료 조건이 충족되면 메인 루프에서 빠져나와도 프로그램은
죽지 않고 idle 상태로 들어간다.
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
from market_data import get_klines
from strategies_trend import decide_trend_15m

# ─────────────────────────────
# 전역 상태
# ─────────────────────────────
SET = load_settings()
START_TS: float = time.time()
RUNNING: bool = True
TERMINATED_BY_SIGNAL: bool = False

# 텔레그램에 뿌릴 때 KRW로도 보여주려고 추가
KRW_RATE: float = getattr(SET, "krw_per_usdt", getattr(SET, "krw_rate", 1400.0))

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
# 직전에 거래소에서 몇 건 동기화했는지 기억 → 같으면 텔레 안 보냄
_LAST_SYNCED_EXCHANGE_POS_COUNT: int = 0
# 열려 있는 동안 주기적으로 상태 알려주기 위한 타임스탬프
LAST_STATUS_TG_TS: float = 0.0
# 주기는 settings 에서 끌어오되, 없으면 1800초(30분)
STATUS_TG_INTERVAL_SEC: int = getattr(SET, "unrealized_notify_sec", 1800)


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
# idle 모드 진입 (1번 방법)
# ─────────────────────────────
def _enter_idle_forever() -> None:
    """
    여기로 들어오면 봇 로직은 멈추고 프로세스만 살아있다.
    Render 같은 데서 '프로세스 죽었네?' 하고 다시 띄우는 걸 막기 위함.
    """
    log("[IDLE] safe stop reached, entering idle loop...")
    # 너무 자주 보내면 안 되니까 한 번만 보낸다.
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
    """컨테이너에서 SIGTERM 오면 자연스럽게 빠져나오게 한다."""
    global RUNNING, TERMINATED_BY_SIGNAL
    TERMINATED_BY_SIGNAL = True
    RUNNING = False


signal.signal(signal.SIGTERM, _sigterm)


# ─────────────────────────────
# 15m 추세 문구 만들기
# ─────────────────────────────
def _get_15m_trend_text(symbol: str) -> str:
    try:
        candles_15m = get_klines(symbol, "15m", 120)
        if not candles_15m:
            return ""
        trimmed = [c[:5] for c in candles_15m]
        trend_dir = decide_trend_15m(trimmed)
        if trend_dir == "LONG":
            return " (15m 추세: 상승)"
        elif trend_dir == "SHORT":
            return " (15m 추세: 하락)"
        else:
            return " (15m 추세: 중립)"
    except Exception as e:
        log(f="[STATUS_TG] 15m trend check error: {e}")
        return ""


# ─────────────────────────────
# 보조: 열려 있는 포지션 상태를 텔레그램으로 던지기
# ─────────────────────────────
def _send_open_positions_status(symbol: str, interval_sec: int) -> None:
    try:
        positions = fetch_open_positions(symbol)
    except Exception as e:
        log(f"[STATUS_TG] fetch_open_positions error: {e}")
        return

    if not positions:
        return

    mins = max(1, interval_sec // 60)
    trend_txt = _get_15m_trend_text(symbol)
    lines = [f"⏱ 포지션 상태({mins}m){trend_txt}:"]
    for p in positions:
        qty = float(
            p.get("positionAmt")
            or p.get("quantity")
            or p.get("size")
            or 0.0
        )
        upnl = float(p.get("unrealizedProfit") or 0.0)
        side_text = "LONG" if qty > 0 else "SHORT"
        upnl_krw = upnl * KRW_RATE
        lines.append(
            f"- {symbol} {side_text} 수량={abs(qty)} 미실현={upnl:.2f} USDT (~{upnl_krw:,.0f} KRW)"
        )
    send_tg("\n".join(lines))


# ─────────────────────────────
# 거래소 → 내부 포지션 동기화 (단방향 대응 버전)
# ─────────────────────────────
def sync_open_trades_from_exchange(symbol: str, replace: bool = True) -> None:
    global OPEN_TRADES, _LAST_SYNCED_EXCHANGE_POS_COUNT

    positions = fetch_open_positions(symbol)
    orders = fetch_open_orders(symbol)

    if replace:
        OPEN_TRADES = []

    if not positions:
        log("[SYNC] 열려 있는 포지션이 없습니다.")
        _LAST_SYNCED_EXCHANGE_POS_COUNT = 0
        return

    for pos in positions:
        raw_amt = pos.get("positionAmt") or pos.get("quantity") or pos.get("size") or 0.0
        try:
            raw_amt_f = float(raw_amt)
        except (TypeError, ValueError):
            raw_amt_f = 0.0

        if raw_amt_f == 0.0:
            continue

        if raw_amt_f > 0:
            side_open = "BUY"
        else:
            side_open = "SELL"

        qty = abs(raw_amt_f)
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

    if len(OPEN_TRADES) != _LAST_SYNCED_EXCHANGE_POS_COUNT:
        send_tg(f"🔁 거래소 포지션 {len(OPEN_TRADES)}건 동기화했습니다. (중복 진입 방지)")
        _LAST_SYNCED_EXCHANGE_POS_COUNT = len(OPEN_TRADES)


# ─────────────────────────────
# 텔레그램 종료 콜백
# ─────────────────────────────
def _on_safe_stop() -> None:
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

            # 1) 1분에 한 번 가용 잔고 찍어두기
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

                # TP/SL 재설정 실패로 인한 종료 → idle 진입
                if TRADER_STATE.should_stop_bot():
                    send_tg("🛑 TP/SL 재설정이 연속 실패해서 봇을 중단합니다.")
                    _write_stop_flag()
                    _enter_idle_forever()

                # 종료 명령 들어왔고 포지션이 비었다면 여기서 끝 → idle 진입
                if SAFE_STOP_REQUESTED and not OPEN_TRADES:
                    send_tg("🛑 종료 명령에 따라 포지션이 없어 종료합니다.")
                    _write_stop_flag()
                    _enter_idle_forever()

            # 포지션 살아 있으면 신규 진입 안 함 + 주기 상태만 던짐
            if OPEN_TRADES:
                if (
                    getattr(SET, "unrealized_notify_enabled", False)
                    and now - LAST_STATUS_TG_TS >= STATUS_TG_INTERVAL_SEC
                ):
                    _send_open_positions_status(SET.symbol, STATUS_TG_INTERVAL_SEC)
                    LAST_STATUS_TG_TS = now
                time.sleep(1)
                continue

            # 종료 플래그가 있는데 포지션이 없는 경우 → 진입하지 않고 종료 → idle
            if SAFE_STOP_REQUESTED:
                send_tg("🛑 종료 명령에 따라 새 진입 없이 종료합니다.")
                _write_stop_flag()
                _enter_idle_forever()

            # 연속 손실 3회면 휴식
            if CONSEC_LOSSES >= 3:
                send_tg("⛔ 연속 3회 손실 → 휴식 진입")
                time.sleep(SET.cooldown_after_3loss)
                CONSEC_LOSSES = 0
                continue

            # 최근 청산 후 공통 쿨다운
            if LAST_CLOSE_TS > 0 and (time.time() - LAST_CLOSE_TS) < SET.cooldown_after_close:
                time.sleep(1)
                continue

            # 4) 시그널 + 캔들 가져오기
            signal_ctx = get_trading_signal(
                settings=SET,
                last_trend_close_ts=LAST_CLOSE_TS_TREND,
                last_range_close_ts=LAST_CLOSE_TS_RANGE,
            )
            if signal_ctx is None:
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

            # 5) 진입 전 가드
            manual_ok = check_manual_position_guard(
                get_balance_detail_func=get_balance_detail,
                symbol=SET.symbol,
                latest_ts=latest_ts,
            )
            if not manual_ok:
                time.sleep(2)
                continue

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

            # 7) TP/SL 퍼센트
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

            # 8.5) trader 에 넘길 보강 플래그 추출
            soft_mode_flag = False
            sl_floor_ratio = None
            if extra:
                soft_mode_flag = bool(
                    extra.get("soft_mode")
                    or extra.get("soft")
                    or extra.get("range_soft")
                )
                sl_floor_ratio = extra.get("sl_floor_ratio")

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
                soft_mode=soft_mode_flag,
                sl_floor_ratio=sl_floor_ratio,
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

            # 여기서 사람이 보기 좋게 한 줄로
            side_ko = "롱" if trade.side == "BUY" else "숏"
            strat_ko = "추세" if signal_source == "TREND" else "박스"
            if getattr(SET, "notify_on_entry", True):
                send_tg(
                    f"[ENTRY] {strat_ko} {side_ko} 진입\n"
                    f"- 심볼: {trade.symbol}\n"
                    f"- 진입가: {trade.entry:.2f}\n"
                    f"- 수량: {trade.qty}\n"
                    f"- TP: {trade.tp_price} / SL: {trade.sl_price}"
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
            LAST_STATUS_TG_TS = time.time()

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
    # 예전엔 여기서 종료했는데, 이제는 idle 로 들어간다.
    _enter_idle_forever()


if __name__ == "__main__":
    main()
