"""run_bot_ws.py
=====================================================
웹소켓으로 들어오는 1m / 5m / 15m 캔들을 기준으로 포지션을 열고 감시하는 메인 루프.
이 버전에서는 더 이상 3m 캔들을 사용하지 않는다.

2025-11-19 변경 사항 (EXIT GPT 레이어 완전 연동 + 추세→박스 다운그레이드)
----------------------------------------------------
1) position_watch_ws.py 의 GPT 기반 EXIT 레이어와 완전 연동.
   - 조기익절/조기청산/반대 시그널 컷/박스↔추세 전환은 모두
     gpt_decider.ask_exit_decision_safe(...) 를 한 번 거친 뒤 실행된다.
   - GPT 오류 시에는 fallback_action 으로 기존 Python 로직만으로 동작.
2) 추세→박스 다운그레이드(maybe_downgrade_trend_to_range) 를 메인 루프에 추가.
   - TREND 포지션이 RANGE 환경으로 바뀐 경우, GPT 가 CLOSE 를 승인하면
     포지션을 정리하고 RANGE 전략으로 갈아타도록 한다.
3) EXIT 경로별 마지막 청산 시각 업데이트 정리.
   - RANGE 관련 종료: LAST_CLOSE_TS_RANGE
   - TREND 관련 종료: LAST_CLOSE_TS_TREND
   - 공통 종료 시각: LAST_CLOSE_TS

2025-11-17 변경 사항 (텔레그램 메시지 한글화 + 직관 표현)
----------------------------------------------------
1) 텔레그램으로 나가는 안내 문구를 전부 한국어로 정리하고, 어려운 용어를 줄였다.
   - 봇 시작/종료, 일일 정산, 연속 손실, 오류 안내, STOP_FLAG 경고 등.
2) 포지션 청산 알림에서 reason 코드를 사람이 보기 쉬운 문장으로 변환했다.
   - 예) "tp_hit" → "목표가 도달(익절)", "sl_hit" → "손절가 도달(손절)" 등.
3) 열린 포지션 상태 알림에서 방향을 "LONG/SHORT" 대신 "롱/숏" 으로 표시하고,
   15분 추세도 "상승/하락/횡보" 형태로 안내하도록 변경했다.

2025-11-15 변경 사항 (REST 백필 디버그 강화)
----------------------------------------------------
15) _backfill_ws_kline_history(...) 디버그 로그 강화
    - REST 응답 rows 개수, 첫/마지막 openTime(ts) 를 부팅 로그에 남긴다.
    - backfill_klines_from_rest(...) 호출 후 WS 버퍼 길이(ws_get_klines_with_volume)를
      즉시 확인해서 "REST 백필 → WS 버퍼 적재" 단계가 실제로 수행됐는지 로그로 남긴다.
    - KlineRestError / 기타 예외를 구분해서 로깅하고, 실패 시 METRICS["kline_failures"]++.
16) main() 부팅 직후 WS 버퍼 상태 진단 로그 추가
    - WS 시작 후 2초 정도 대기 뒤 1m/5m/15m 에 대해 WS 버퍼 길이를 로그.
      예: [BOOT] WS buffer status: symbol=BTC-USDT interval=5m len=120
    - REST 백필이 안 먹었는지, WS 실시간 스트림이 안 들어오는지 Render 로그에서 바로 구분 가능.

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
from entry_flow import try_open_new_position  # 진입 쪽 GPT 결정은 entry_flow/gpt_decider 에서 처리
from position_watch_ws import (  # WS 버전(GPT EXIT 레이어)으로 교체
    maybe_early_exit_range,
    maybe_upgrade_range_to_trend,
    maybe_downgrade_trend_to_range,
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
from market_data_rest import fetch_klines_rest, KlineRestError  # BingX REST /kline 헬퍼


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
# 유틸: STOP_FLAG 파일
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
    log("[IDLE] 봇이 중지 상태(idle 모드)로 들어갑니다.")
    try:
        send_tg("🟡 자동매매를 멈추고 대기 상태로 유지합니다. 다시 돌리려면 서버에서 봇을 재시작해 주세요.")
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
# 유틸: 청산 사유(reason) 한글 변환
# ─────────────────────────────


def _translate_close_reason(reason: str) -> str:
    """check_closes 에서 넘어오는 청산 사유를 사람이 보기 쉬운 짧은 문장으로 변환."""
    if not reason:
        return "기타"

    r = reason.lower()
    if "tp" in r or "take" in r:
        return "목표가 도달(익절)"
    if "sl" in r or "stop" in r:
        return "손절가 도달(손절)"
    if "manual" in r:
        return "수동으로 종료"
    if "force" in r or "emergency" in r:
        return "강제로 종료"
    if "opposite" in r:
        return "반대 방향 신호로 종료"
    return reason


# ─────────────────────────────
# 포지션 상태 텔레그램 알림 (WS 버전)
# ─────────────────────────────


def _send_open_positions_status(symbol: str, interval_sec: int) -> None:
    """열린 포지션이 있을 때 현재 미실현 손익 + 15분 방향을 주기적으로 텔레그램으로 보낸다.

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
                trend_txt = " (15분 차트 기준: 상승 흐름)"
            elif trend_dir == "SHORT":
                trend_txt = " (15분 차트 기준: 하락 흐름)"
            else:
                trend_txt = " (15분 차트 기준: 횡보)"
        else:
            trend_txt = ""
    except Exception as e:
        log(f"[STATUS_TG] WS 15m error: {e}")
        trend_txt = ""

    mins = max(1, interval_sec // 60)
    lines = [f"⏱ 현재 열린 포지션 안내 (약 {mins}분 간격){trend_txt}"]

    for p in positions:
        qty = float(p.get("positionAmt") or p.get("quantity") or p.get("size") or 0.0)
        upnl = float(p.get("unrealizedProfit") or 0.0)
        pos_side_raw = (p.get("positionSide") or "").upper()
        if pos_side_raw in ("LONG", "BOTH"):
            side_text = "롱"
        elif pos_side_raw == "SHORT":
            side_text = "숏"
        else:
            side_text = "롱" if qty > 0 else "숏"

        upnl_krw = upnl * KRW_RATE
        lines.append(
            f"- {symbol} {side_text} / 수량 {abs(qty)} / 미실현 손익 {upnl:.2f} USDT (약 {upnl_krw:,.0f}원)"
        )

    send_tg("\n".join(lines))


# ─────────────────────────────
# 텔레그램 종료 콜백
# ─────────────────────────────


def _on_safe_stop() -> None:
    """텔레그램 명령으로 안전 종료를 요청받았을 때 플래그만 세팅한다."""
    global SAFE_STOP_REQUESTED
    SAFE_STOP_REQUESTED = True
    send_tg("🛑 텔레그램에서 '종료' 버튼을 눌렀습니다. 현재 포지션을 모두 정리한 뒤 자동매매를 멈춥니다.")


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
                METRICS["kline_failures"] = METRICS.get("kline_failures", 0) + 1
                continue

            # REST 응답 요약 로그 (첫/마지막 openTime 기준)
            try:
                first_ts = rest_rows[0][0] if isinstance(rest_rows[0], (list, tuple)) and rest_rows[0] else "N/A"
                last_ts = rest_rows[-1][0] if isinstance(rest_rows[-1], (list, tuple)) and rest_rows[-1] else "N/A"
            except Exception:
                first_ts, last_ts = "N/A", "N/A"

            log(
                f"[BOOT] REST backfill fetched: symbol={symbol} interval={iv} "
                f"rows={len(rest_rows)} first_ts={first_ts} last_ts={last_ts}"
            )

            backfill_klines_from_rest(symbol, iv, rest_rows)

            # WS 버퍼에 실제로 들어갔는지 즉시 확인
            try:
                buf = ws_get_klines_with_volume(symbol, iv, limit=5)
                buf_len = len(buf) if buf else 0
                log(
                    f"[BOOT] REST backfill verify: symbol={symbol} interval={iv} "
                    f"ws_buf_len={buf_len}"
                )
            except Exception as ve:
                log(
                    f"[BOOT] REST backfill verify error for {symbol} {iv}: {ve}"
                )

        except KlineRestError as e:
            log(f"[BOOT] REST backfill KlineRestError for {symbol} {iv}: {e}")
            METRICS["kline_failures"] = METRICS.get("kline_failures", 0) + 1
        except Exception as e:
            log(f"[BOOT] REST backfill failed for {symbol} {iv}: {e}")
            METRICS["kline_failures"] = METRICS.get("kline_failures", 0) + 1


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
        send_tg(
            "⛔ 이전에 '종료' 명령을 받아 STOP_FLAG 파일이 남아 있습니다. "
            "파일을 삭제한 뒤 다시 실행해 주세요."
        )
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

        # 부팅 직후 WS 버퍼 상태 진단 (REST 백필 + WS 스트림 확인용)
        try:
            time.sleep(2)
            for iv in ("1m", "5m", "15m"):
                buf = ws_get_klines_with_volume(SET.symbol, iv, limit=5)
                buf_len = len(buf) if buf else 0
                log(
                    f"[BOOT] WS buffer status: symbol={SET.symbol} interval={iv} len={buf_len}"
                )
        except Exception as e:
            log(f"[BOOT] WS buffer status check error: {e}")

    # 필수 API 키 체크
    if not SET.api_key or not SET.api_secret:
        msg = (
            "❗ 선물 API 키가 설정되어 있지 않습니다. "
            "Render 환경변수에 BINGX_API_KEY / BINGX_API_SECRET 값을 확인해 주세요."
        )
        log(msg)
        send_tg(msg)
        return

    # 시작 알림
    log(
        f"CONFIG: ENABLE_TREND={SET.enable_trend}, "
        f"ENABLE_RANGE={SET.enable_range}, ENABLE_1M_CONFIRM={SET.enable_1m_confirm}"
    )
    send_tg("✅ BingX 비트코인 선물 자동매매(WS 버전)를 시작합니다.")

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
                daily_pnl_krw = daily_pnl * KRW_RATE
                send_tg(
                    "📊 하루 정산 (한국시간 기준)\n"
                    f"- 오늘 손익: {daily_pnl:.2f} USDT (약 {daily_pnl_krw:,.0f}원)\n"
                    f"- 연속 손실 횟수: {CONSEC_LOSSES}회"
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
                        reason_ko = _translate_close_reason(reason)
                        side_ko = "롱" if str(t.side).upper() in ("BUY", "LONG") else "숏"
                        send_tg(
                            "💰 포지션 청산 알림\n"
                            f"- 종목: {t.symbol}\n"
                            f"- 방향: {side_ko}\n"
                            f"- 청산 사유: {reason_ko}\n"
                            f"- 수량: {closed_qty}\n"
                            f"- 청산가: {closed_price:.2f}\n"
                            f"- 실현 손익: {pnl:.2f} USDT (약 {pnl_krw:,.0f}원)"
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
                    send_tg(
                        "🛑 손절/익절 주문을 여러 번 다시 걸었는데 계속 실패합니다. "
                        "안전을 위해 자동매매를 중단합니다."
                    )
                    _write_stop_flag()
                    _enter_idle_forever()

                # 안전 종료 요청이 왔고, 이미 포지션이 모두 정리된 상태라면 종료
                if SAFE_STOP_REQUESTED and not OPEN_TRADES:
                    send_tg("🛑 요청하신 대로 포지션을 모두 정리했고, 자동매매를 종료합니다.")
                    _write_stop_flag()
                    _enter_idle_forever()

            # (e) 열린 포지션에 대한 실시간 대응 (WS 캔들 기반 + GPT EXIT 레이어)
            if OPEN_TRADES:
                for t in list(OPEN_TRADES):
                    # 1) RANGE 조기익절 / 조기청산 (GPT 승인 필요)
                    if maybe_early_exit_range(t, SET):
                        OPEN_TRADES = [ot for ot in OPEN_TRADES if ot is not t]
                        LAST_CLOSE_TS = now
                        LAST_CLOSE_TS_RANGE = now
                        continue

                    # 2) 박스→추세 업그레이드 (RANGE → TREND, GPT 승인 필요)
                    if maybe_upgrade_range_to_trend(
                        t, SET, LAST_CLOSE_TS_TREND, LAST_CLOSE_TS_RANGE
                    ):
                        OPEN_TRADES = [ot for ot in OPEN_TRADES if ot is not t]
                        LAST_CLOSE_TS = now
                        LAST_CLOSE_TS_RANGE = now
                        continue

                    # 2.5) 추세→박스 다운그레이드 (TREND → RANGE, GPT 승인 필요)
                    if maybe_downgrade_trend_to_range(
                        t, SET, LAST_CLOSE_TS_TREND, LAST_CLOSE_TS_RANGE
                    ):
                        OPEN_TRADES = [ot for ot in OPEN_TRADES if ot is not t]
                        LAST_CLOSE_TS = now
                        LAST_CLOSE_TS_TREND = now
                        continue

                    # 3) 반대 시그널 강제 청산 (TREND/RANGE 공통, GPT 승인 필요)
                    if maybe_force_close_on_opposite_signal(
                        t, SET, LAST_CLOSE_TS_TREND, LAST_CLOSE_TS_RANGE
                    ):
                        OPEN_TRADES = [ot for ot in OPEN_TRADES if ot is not t]
                        LAST_CLOSE_TS = now
                        # 어떤 레짐이든 반대 시그널이면 둘 다 최근 청산으로 본다
                        LAST_CLOSE_TS_TREND = now
                        LAST_CLOSE_TS_RANGE = now
                        continue

                    # 4) TREND 조기익절 / 조기청산 (GPT 승인 필요)
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
                send_tg("🛑 요청하신 대로 새로운 진입 없이 자동매매를 종료합니다.")
                _write_stop_flag()
                _enter_idle_forever()

            # (g) 연속 손실 방어 로직
            if CONSEC_LOSSES >= 3:
                send_tg(
                    "⛔ 연속으로 3번 손실이 발생했습니다. "
                    "설정된 휴식 시간 동안 새로 진입하지 않고 쉬었다가 다시 시작합니다."
                )
                time.sleep(SET.cooldown_after_3loss)
                CONSEC_LOSSES = 0
                continue

            # (h) 방금 닫은 직후라면 쿨다운
            if LAST_CLOSE_TS > 0 and (time.time() - LAST_CLOSE_TS) < SET.cooldown_after_close:
                time.sleep(1)
                continue

            # (i) 새 포지션 진입 시도
            # - 진입 신호 계산 + 각종 가드 + GPT 진입 의사결정은 entry_flow.py / gpt_decider 가 담당한다.
            trade, sleep_sec = try_open_new_position(
                SET, LAST_CLOSE_TS_TREND, LAST_CLOSE_TS_RANGE
            )
            if trade:
                OPEN_TRADES.append(trade)
                LAST_STATUS_TG_TS = time.time()

            time.sleep(sleep_sec)

        except Exception as e:
            log(f"ERROR: {e}")
            send_tg(
                "❌ 예기치 못한 오류가 발생했습니다.\n"
                f"- 내용: {e}\n"
                "2초 후 자동으로 다시 시도합니다."
            )
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
