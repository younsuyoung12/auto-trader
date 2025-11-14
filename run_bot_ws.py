"""run_bot_ws.py
=====================================================
웹소켓으로 들어오는 1m / 5m / 15m 캔들을 기준으로 포지션을 열고 감시하는 메인 루프.
이 버전에서는 더 이상 3m 캔들을 사용하지 않는다.

2025-11-13 변경 사항
----------------------------------------------------
1) settings → settings_ws 로 교체해서 기본 주기를 5m 로 사용.
2) 포지션 상태 텔레그램 알림에서 15m 추세를 볼 때 REST 대신 market_data_ws 를 사용.
   (Render 콘솔에서 15m 가 실제로 들어오는지 바로 확인 가능)
3) position_watch 모듈을 WS 버전(position_watch_ws)으로 교체해서
   조기익절/조기청산/반대 시그널 컷이 모두 WS 캔들을 쓰도록 정리.
4) 시작 시점에 현재 WS 설정(ws_enabled, ws_subscribe_tfs)을 로그로 남겨
   Render 환경에서 "WS 모드로 올라왔는지" 확인 가능하게 함.
5) 3m 관련 주석과 interval 언급을 모두 제거하고, 1m/5m/15m 기준으로 다시 작성.
6) 주문/TP/SL 은 기존과 동일하게 REST(exchange_api)로 처리.

2025-11-13 추가 보정
----------------------------------------------------
7) 실제 웹소켓 수신이 돌아가도록 main() 시작 시
   market_data_ws.start_ws_loop(SET.symbol)을 한 번만 호출하도록 추가.

2025-11-14 변경 사항 (시세 DB 저장 + 레짐 워커 연동)
----------------------------------------------------
8) market_data_store 모듈을 사용해 WS 버퍼의 캔들/호가를 Postgres 에 저장하는
   백그라운드 스레드(_start_market_data_store_thread)를 추가.
   - 1m/5m/15m 캔들은 bt_candles 테이블에 적재.
   - depth5 오더북 스냅샷은 bt_orderbook_snapshots 테이블에 적재.
9) candles 는 interval 별 마지막 저장 timestamp(ts_ms)를 기억해서
   이미 저장된 캔들은 다시 INSERT 하지 않도록 방어.
10) 오더북은 일정 간격(기본 5초)마다 한 번만 저장해서 DB I/O 를 과도하게 늘리지 않도록 함.
11) DB 저장 실패(SQLAlchemyError)는 telelog 로만 남기고
    메인 매매 루프/WS 루프에는 영향을 주지 않도록 설계.
12) signal_analysis_worker 를 1분 주기(또는 settings_ws.signal_analysis_interval_sec)로
    백그라운드에서 돌려 bt_candles 를 읽고 bt_regime_scores 에 레짐 점수를 기록.

2025-11-15 변경 사항 (REST 히스토리 백필)
----------------------------------------------------
13) BingX REST /kline 응답을 이용해 5m/15m 히스토리 캔들을
    market_data_ws.backfill_klines_from_rest(...) 으로 WS 버퍼에 미리 채우는
    _backfill_ws_kline_history(...) 헬퍼 추가.
14) main() 시작 시 ws_enabled=True 이면
    _backfill_ws_kline_history(...) 실행 후 start_ws_loop(...) 를 호출하도록 변경.
    → 최초 부팅 직후에도 5m 캔들이 50개 이상 준비되어
      "[SIGNAL] 5m candles not enough (<50) → skip signal" 현상을 줄임.
"""

from __future__ import annotations

import os
import time
import datetime
import signal
import threading
from typing import Any, Dict, List, Optional

from settings_ws import load_settings, KST  # WS 버전 설정
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

# 동기화/진입/포지션 감시 모듈
from sync_exchange import sync_open_trades_from_exchange
from entry_flow import try_open_new_position  # 필요 시 entry_flow_ws 로 교체 가능
from position_watch_ws import (  # WS 버전으로 교체
    maybe_early_exit_range,
    maybe_upgrade_range_to_trend,
    maybe_early_exit_trend,
    maybe_force_close_on_opposite_signal,
)
# 웹소켓 시세 버퍼
from market_data_ws import (
    start_ws_loop,                  # 웹소켓 수신 시작
    get_klines as ws_get_klines,    # 15m 추세 조회용
    get_klines_with_volume as ws_get_klines_with_volume,  # 캔들+거래량(DB 적재용)
    get_orderbook as ws_get_orderbook,  # depth5 오더북(DB 적재용)
    backfill_klines_from_rest,      # REST 히스토리 → WS 버퍼 백필용
)
from market_data_store import (
    save_candles_bulk_from_ws,
    save_orderbook_from_ws,
)
from strategies_trend_ws import decide_trend_15m  # 15m 방향 판단
from market_data_ws import fetch_klines_rest     # BingX REST /kline 헬퍼


# ─────────────────────────────
# 전역 상태 초기화
# ─────────────────────────────
SET = load_settings()
START_TS: float = time.time()
RUNNING: bool = True
TERMINATED_BY_SIGNAL: bool = False

# KRW 환산용 환율 (텔레그램 표시용)
KRW_RATE: float = getattr(SET, "krw_per_usdt", 1400.0)

# 헬스/모니터링용 메트릭
METRICS: Dict[str, Any] = {
    "start_ts": START_TS,
    "last_loop_ts": START_TS,
    "open_trades": 0,
    "consec_losses": 0,
    "kline_failures": 0,
}

# 현재 열린 포지션 목록 (Trade 객체)
OPEN_TRADES: List[Trade] = []
# TP/SL 재설정 실패 횟수 관리용
TRADER_STATE: TraderState = TraderState()
# 최근 청산 시각들
LAST_CLOSE_TS: float = 0.0
LAST_CLOSE_TS_TREND: float = 0.0
LAST_CLOSE_TS_RANGE: float = 0.0
# 연속 손실 횟수
CONSEC_LOSSES: int = 0
# 텔레그램에서 들어오는 안전 종료 플래그
SAFE_STOP_REQUESTED: bool = False
# 거래소 포지션 동기화 주기 체크용
LAST_EXCHANGE_SYNC_TS: float = 0.0
# 포지션 살아 있을 때 상태 알림 주기
LAST_STATUS_TG_TS: float = 0.0
STATUS_TG_INTERVAL_SEC: int = getattr(SET, "unrealized_notify_sec", 1800)

# 레짐 워커 주기 (기본 60초)
SIGNAL_ANALYSIS_INTERVAL_SEC: int = getattr(SET, "signal_analysis_interval_sec", 60)


# ─────────────────────────────
# STOP_FLAG 유틸
# ─────────────────────────────

def _write_stop_flag() -> None:
    """프로세스 재시작 시 즉시 종료시키기 위한 플래그 파일 작성."""
    try:
        with open("STOP_FLAG", "w", encoding="utf-8") as f:
            f.write("stop\n")
    except OSError as e:
        log(f"[STOP_FLAG] write failed: {e}")


# ─────────────────────────────
# idle 모드
# ─────────────────────────────

def _enter_idle_forever() -> None:
    """메인 로직만 멈추고 프로세스는 살아 있게 하는 무한 슬립."""
    log("[IDLE] safe stop reached, entering idle loop...")
    try:
        send_tg("🟡 봇을 멈춘 상태로 유지합니다. 재시작은 컨테이너/프로세스로 해주세요.")
    except Exception:
        pass
    while True:
        time.sleep(60)


# ─────────────────────────────
# SIGTERM 핸들러 등록
# ─────────────────────────────

def _sigterm(*_: Any) -> None:
    """컨테이너 SIGTERM 시 자연스럽게 메인 루프를 빠져나오도록 한다."""
    global RUNNING, TERMINATED_BY_SIGNAL
    TERMINATED_BY_SIGNAL = True
    RUNNING = False


signal.signal(signal.SIGTERM, _sigterm)


# ─────────────────────────────
# 포지션 상태 텔레그램 알림 (WS 버전)
# ─────────────────────────────

def _send_open_positions_status(symbol: str, interval_sec: int) -> None:
    """열린 포지션이 있을 때 현재 미실현 PnL + 15m 방향을 주기적으로 텔레그램으로 보낸다.

    15m 캔들은 웹소켓 버퍼에서 읽는다.
    Render 콘솔에서는 아래 로그로 실제 수신 여부 확인 가능:
      [STATUS_TG] WS 15m count=...
    """
    from exchange_api import fetch_open_positions

    try:
        positions = fetch_open_positions(symbol)
    except Exception as e:
        log(f"[STATUS_TG] fetch_open_positions error: {e}")
        return

    if not positions:
        return

    # 15m 추세 정보 붙이기 (WS)
    try:
        candles_15m = ws_get_klines(symbol, "15m", 120)
        log(f"[STATUS_TG] WS 15m count={len(candles_15m) if candles_15m else 0}")
        if candles_15m:
            trend_dir = decide_trend_15m(candles_15m)
            if trend_dir == "LONG":
                trend_txt = " (15m 추세: 상승)"
            elif trend_dir == "SHORT":
                trend_txt = " (15m 추세: 하락)"
            else:
                trend_txt = " (15m 추세: 중립)"
        else:
            trend_txt = ""
    except Exception as e:
        log(f"[STATUS_TG] WS 15m error: {e}")
        trend_txt = ""

    mins = max(1, interval_sec // 60)
    lines = [f"⏱ 포지션 상태({mins}m){trend_txt}:"]

    for p in positions:
        qty = float(p.get("positionAmt") or p.get("quantity") or p.get("size") or 0.0)
        upnl = float(p.get("unrealizedProfit") or 0.0)
        pos_side_raw = (p.get("positionSide") or "").upper()
        if pos_side_raw in ("LONG", "BOTH"):
            side_text = "LONG"
        elif pos_side_raw == "SHORT":
            side_text = "SHORT"
        else:
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
    """텔레그램 명령으로 안전 종료를 요청받았을 때 플래그만 세팅한다."""
    global SAFE_STOP_REQUESTED
    SAFE_STOP_REQUESTED = True
    send_tg("🛑 종료 명령 수신: 포지션 정리 후 종료합니다.")


# ─────────────────────────────
# WS 시세 초기 REST 백필
# ─────────────────────────────

def _backfill_ws_kline_history(symbol: str) -> None:
    """BingX REST /kline 으로 5m/15m 히스토리 캔들을 받아와 WS 버퍼에 미리 채운다.

    - run_bot_ws.main() 시작 시점에 한 번만 호출.
    - market_data_ws.backfill_klines_from_rest(...) 를 사용해
      5m/15m 버퍼가 최소 수십 개 이상 채워진 상태에서 신호 계산이 시작되도록 한다.
    """
    # 기본은 5m/15m 만 백필, 필요하면 settings_ws.ws_backfill_tfs 로 조정 가능
    intervals = getattr(SET, "ws_backfill_tfs", ["5m", "15m"])
    limit = int(getattr(SET, "ws_backfill_limit", 120))

    for iv in intervals:
        try:
            log(
                f"[BOOT] REST backfill start: symbol={symbol} interval={iv} limit={limit}"
            )
            rest_rows = fetch_klines_rest(symbol, iv, limit=limit)
            if not rest_rows:
                log(
                    f"[BOOT] REST backfill skipped: empty response for {symbol} {iv}"
                )
                continue
            backfill_klines_from_rest(symbol, iv, rest_rows)
        except Exception as e:
            log(f"[BOOT] REST backfill failed for {symbol} {iv}: {e}")


# ─────────────────────────────
# WS 시세 → DB 저장 스레드
# ─────────────────────────────

def _start_market_data_store_thread() -> None:
    """WS 버퍼에서 캔들/호가를 읽어 주기적으로 Postgres 에 저장하는 백그라운드 스레드.

    - 1m/5m/15m 캔들 → bt_candles
    - depth5 오더북 스냅샷 → bt_orderbook_snapshots
    """
    symbol = SET.symbol
    flush_sec = getattr(SET, "md_store_flush_sec", 5)
    ob_interval_sec = getattr(SET, "ob_store_interval_sec", 5)

    # interval 별 마지막 저장 ts_ms (중복 INSERT 방지)
    last_candle_ts: Dict[str, int] = {
        "1m": 0,
        "5m": 0,
        "15m": 0,
    }
    last_ob_ts: float = 0.0

    def _loop() -> None:
        nonlocal last_ob_ts
        log(
            f"[MD-STORE] loop started: flush_sec={flush_sec}, "
            f"ob_interval_sec={ob_interval_sec}"
        )
        while RUNNING:
            try:
                now = time.time()

                # 1) 캔들 적재 (1m/5m/15m)
                candles_to_save: List[Dict[str, Any]] = []

                for iv in ("1m", "5m", "15m"):
                    buf = ws_get_klines_with_volume(symbol, iv, limit=500)
                    if not buf:
                        continue

                    # buf 원소: (ts_ms, o, h, l, c, v)
                    newest_ts = last_candle_ts.get(iv, 0)
                    new_rows = [row for row in buf if row[0] > newest_ts]
                    if not new_rows:
                        continue

                    for ts_ms, o, h, l, c, v in new_rows:
                        candles_to_save.append(
                            {
                                "symbol": symbol,
                                "interval": iv,
                                "ts_ms": int(ts_ms),
                                "open": float(o),
                                "high": float(h),
                                "low": float(l),
                                "close": float(c),
                                "volume": float(v),
                                # quote_volume 은 WS payload 에서 별도로 가져올 수 있을 때만 넣는다.
                                "quote_volume": None,
                                "source": "ws",
                            }
                        )

                    # 가장 최신 ts_ms 로 갱신
                    last_candle_ts[iv] = int(new_rows[-1][0])

                if candles_to_save:
                    save_candles_bulk_from_ws(candles_to_save)

                # 2) 오더북(depth5) 스냅샷 적재 (지나치게 자주 쓰지 않도록 별도 주기 사용)
                if now - last_ob_ts >= ob_interval_sec:
                    ob = ws_get_orderbook(symbol, limit=5)
                    if ob and ob.get("bids") and ob.get("asks"):
                        ts_ms = None
                        # exchTs(거래소 타임스탬프) 우선, 없으면 로컬 ts 사용
                        for k in ("exchTs", "ts"):
                            if ob.get(k) is not None:
                                try:
                                    ts_ms = int(ob[k])
                                    break
                                except Exception:
                                    continue
                        if ts_ms is None:
                            ts_ms = int(now * 1000)

                        save_orderbook_from_ws(
                            symbol=symbol,
                            ts_ms=ts_ms,
                            bids=ob["bids"],
                            asks=ob["asks"],
                        )
                        last_ob_ts = now

            except Exception as e:
                # DB 쪽 문제로 전체 봇이 죽지 않도록 여기서만 잡고 로그만 남긴다.
                log(f"[MD-STORE] loop error: {e}")

            time.sleep(flush_sec)

    th = threading.Thread(target=_loop, name="md-store-loop", daemon=True)
    th.start()
    log("[MD-STORE] background store thread started")


# ─────────────────────────────
# 메인 루프 (WS)
# ─────────────────────────────

def main() -> None:
    global RUNNING
    global OPEN_TRADES, LAST_CLOSE_TS, LAST_CLOSE_TS_TREND, LAST_CLOSE_TS_RANGE
    global CONSEC_LOSSES, SAFE_STOP_REQUESTED, LAST_EXCHANGE_SYNC_TS
    global LAST_STATUS_TG_TS

    # 시작 시 STOP_FLAG 있으면 바로 종료
    if os.path.exists("STOP_FLAG"):
        log("STOP_FLAG detected on startup. exiting without start.")
        send_tg("⛔ 이전 종료 명령이 있어 실행하지 않습니다. (STOP_FLAG 삭제 후 재실행)")
        return

    # WS 설정 로그로 남기기 (Render 확인용)
    log(
        f"[BOOT] WS_ENABLED={getattr(SET, 'ws_enabled', True)} "
        f"WS_TF={getattr(SET, 'ws_subscribe_tfs', ['1m','5m','15m'])} "
        f"INTERVAL={SET.interval}"
    )

    # ★ REST 히스토리 백필 + 웹소켓 시세 수신 시작
    if getattr(SET, "ws_enabled", True):
        _backfill_ws_kline_history(SET.symbol)
        start_ws_loop(SET.symbol)
        _start_market_data_store_thread()

    # 필수 API 키 체크
    if not SET.api_key or not SET.api_secret:
        msg = "❗ BINGX_API_KEY / BINGX_API_SECRET 이 비어있습니다. 환경변수 설정 필요."
        log(msg)
        send_tg(msg)
        return

    # 시작 알림
    log(
        f"CONFIG: ENABLE_TREND={SET.enable_trend}, "
        f"ENABLE_RANGE={SET.enable_range}, ENABLE_1M_CONFIRM={SET.enable_1m_confirm}"
    )
    send_tg("✅ [봇 시작] BingX 선물 자동매매 (WS) 시작합니다.")

    # 레버리지/마진 모드 세팅 (실패해도 계속 감)
    try:
        set_leverage_and_mode(SET.symbol, SET.leverage, SET.isolated)
    except Exception as e:
        log(f"[WARN] 레버리지/마진 설정 실패: {e}")

    # 보조 스레드들 시작 (헬스 서버, 드라이브 동기화, 텔레그램 명령, 레짐 워커)
    start_health_server()
    start_drive_sync_thread()
    start_telegram_command_thread(on_stop_command=_on_safe_stop)
    start_signal_analysis_thread(interval_sec=SIGNAL_ANALYSIS_INTERVAL_SEC)

    # 최초 거래소 포지션 동기화
    OPEN_TRADES, _ = sync_open_trades_from_exchange(
        SET.symbol,
        replace=True,
        current_trades=OPEN_TRADES,
    )
    LAST_EXCHANGE_SYNC_TS = time.time()

    # 일일 PnL 집계를 위한 변수
    now_kst = datetime.datetime.now(KST)
    last_report_date_kst = now_kst.strftime("%Y-%m-%d")
    daily_pnl: float = 0.0

    # 주기 설정
    position_resync_sec = getattr(SET, "position_resync_sec", 20)
    last_fill_check: float = 0.0
    last_balance_log: float = 0.0

    # 메인 루프 시작
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

            # (b) 잔고 주기 로그
            if now - last_balance_log >= 60:
                get_available_usdt()
                last_balance_log = now

            # (c) 자정(KST)마다 일일 정산 알림
            now_kst = datetime.datetime.now(KST)
            today_kst = now_kst.strftime("%Y-%m-%d")
            if now_kst.hour == 0 and now_kst.minute < 1 and last_report_date_kst != today_kst:
                send_tg(
                    f"📊 일일 정산(KST): PnL {daily_pnl:.2f} USDT, 연속 손실 {CONSEC_LOSSES}"
                )
                daily_pnl = 0.0
                CONSEC_LOSSES = 0
                last_report_date_kst = today_kst

            # (d) 거래소 TP/SL 체결 확인 및 내부 포지션 리스트 정리
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

                    # 연속 손실 계산
                    if pnl < 0:
                        CONSEC_LOSSES += 1
                    else:
                        CONSEC_LOSSES = 0

                    # 전략별 마지막 청산 시각 업데이트
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

                # TP/SL 재설정이 계속 실패하면 봇 중단 → idle
                if TRADER_STATE.should_stop_bot():
                    send_tg("🛑 TP/SL 재설정이 연속 실패해서 봇을 중단합니다.")
                    _write_stop_flag()
                    _enter_idle_forever()

                # 안전 종료 요청이 왔고, 이미 포지션이 모두 정리된 상태라면 종료
                if SAFE_STOP_REQUESTED and not OPEN_TRADES:
                    send_tg("🛑 종료 명령에 따라 포지션이 없어 종료합니다.")
                    _write_stop_flag()
                    _enter_idle_forever()

            # (e) 열린 포지션에 대한 실시간 대응 (WS 캔들 기반)
            if OPEN_TRADES:
                for t in list(OPEN_TRADES):
                    if maybe_early_exit_range(t, SET):
                        OPEN_TRADES = [ot for ot in OPEN_TRADES if ot is not t]
                        LAST_CLOSE_TS = now
                        LAST_CLOSE_TS_RANGE = now
                        continue

                    if maybe_upgrade_range_to_trend(
                        t, SET, LAST_CLOSE_TS_TREND, LAST_CLOSE_TS_RANGE
                    ):
                        OPEN_TRADES = [ot for ot in OPEN_TRADES if ot is not t]
                        LAST_CLOSE_TS = now
                        LAST_CLOSE_TS_RANGE = now
                        continue

                    if maybe_force_close_on_opposite_signal(
                        t, SET, LAST_CLOSE_TS_TREND, LAST_CLOSE_TS_RANGE
                    ):
                        OPEN_TRADES = [ot for ot in OPEN_TRADES if ot is not t]
                        LAST_CLOSE_TS = now
                        LAST_CLOSE_TS_TREND = now
                        LAST_CLOSE_TS_RANGE = now
                        continue

                    if maybe_early_exit_trend(t, SET):
                        OPEN_TRADES = [ot for ot in OPEN_TRADES if ot is not t]
                        LAST_CLOSE_TS = now
                        LAST_CLOSE_TS_TREND = now
                        continue

                # 포지션이 여전히 남아 있으면 상태만 알리고 다음 루프로 넘어감
                if OPEN_TRADES:
                    if (
                        getattr(SET, "unrealized_notify_enabled", False)
                        and now - LAST_STATUS_TG_TS >= STATUS_TG_INTERVAL_SEC
                    ):
                        _send_open_positions_status(SET.symbol, STATUS_TG_INTERVAL_SEC)
                        LAST_STATUS_TG_TS = now
                    time.sleep(1)
                    continue

            # (f) 포지션이 없는 상태에서 안전 종료 요청이 들어온 경우 → 즉시 idle
            if SAFE_STOP_REQUESTED:
                send_tg("🛑 종료 명령에 따라 새 진입 없이 종료합니다.")
                _write_stop_flag()
                _enter_idle_forever()

            # (g) 연속 손실 방어 로직
            if CONSEC_LOSSES >= 3:
                send_tg("⛔ 연속 3회 손실 → 휴식 진입")
                time.sleep(SET.cooldown_after_3loss)
                CONSEC_LOSSES = 0
                continue

            # (h) 방금 닫은 직후라면 쿨다운
            if LAST_CLOSE_TS > 0 and (time.time() - LAST_CLOSE_TS) < SET.cooldown_after_close:
                time.sleep(1)
                continue

            # (i) 새 포지션 진입 시도
            trade, sleep_sec = try_open_new_position(
                SET, LAST_CLOSE_TS_TREND, LAST_CLOSE_TS_RANGE
            )
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

    # RUNNING 이 False 로 바뀌어 루프가 끝나면 idle 로 가서 멈춘다
    _enter_idle_forever()


if __name__ == "__main__":
    main()
