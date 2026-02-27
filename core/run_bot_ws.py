# run_bot_ws.py
"""
run_bot_ws.py – Binance USDT-M Futures WebSocket 메인 루프
============================================================

역할
-----------------------------------------------------
- WS: Binance fstream 멀티 타임프레임 kline + depth5 오더북 수신.
- REST: 부팅 시 Binance Futures /fapi/v1/klines 백필 전용(런타임 의사결정에는 사용하지 않음).
- 진입: entry_flow.try_open_new_position → GPT 엔트리 레이어.
- 청산: position_watch_ws.maybe_exit_with_gpt → GPT EXIT 레이어.
- 이 파일은 포지션/쿨다운/헬스체크를 오케스트레이션할 뿐,
  매수·매도·손절·익절 결정 로직은 하위 GPT 모듈에서 관리한다.

핵심 원칙
-----------------------------------------------------
- 실시간 판단은 WS 버퍼 데이터만 사용하고, REST 는 초기 백필에만 사용한다.
- STOP_FLAG 기반 자동 종료/idle 없음. 텔레그램 '종료' 요청 + 포지션 0 일 때만 종료한다.
- 엔트리: entry_cooldown_sec 으로 GPT 엔트리 호출 간격을 제한한다.
- 익절/손절: 완성된 1m WS 캔들 종가가 확정될 때만 GPT EXIT 를 호출한다.
- WS/REST 데이터 헬스·GPT 레이턴시 상태를 주기적으로 텔레그램으로 보고한다.

참고 (설명용)
-----------------------------------------------------
- Binance Futures WS base: wss://fstream.binance.com/ws
- kline stream: <symbol>@kline_<interval>
- depth stream: <symbol>@depth5
- 실제 스트림 연결/파싱은 market_data_ws 모듈이 담당한다.
"""

from __future__ import annotations

import csv
import datetime
import os
import signal
import threading
import time
from typing import Any, Dict, List, Optional

from settings import KST, load_settings  # WS 버전 설정
from infra.telelog import log, send_tg
from execution.exchange_api import (
    get_available_usdt,
    set_leverage_and_mode,
)
from state.trader_state import Trade, TraderState, check_closes
from events.signals_logger import log_signal

from infra.bot_workers import (
    start_drive_sync_thread,
    start_health_server,
    start_telegram_command_thread,
)
from strategy.signal_analysis_worker import start_signal_analysis_thread

# 동기화/진입/포지션 감시 모듈
from execution.sync_exchange import sync_open_trades_from_exchange
from strategy.entry_engine import try_open_new_position  # 진입 쪽 GPT 결정은 entry_flow/gpt_trader 에서 처리
from state.exit_engine import (  # WS 버전 GPT EXIT 어댑터
    maybe_exit_with_gpt,
)

# 웹소켓 시세 버퍼
from infra.market_data_ws import (
    backfill_klines_from_rest,      # REST 히스토리 → WS 버퍼 백필용
    get_health_snapshot,            # WS/오더북 데이터 헬스 스냅샷
    get_klines_with_volume as ws_get_klines_with_volume,  # 캔들+거래량(DB 적재용 및 EXIT 1m 캔들 체크용)
    get_orderbook as ws_get_orderbook,  # depth5 오더북(DB 적재용)
    start_ws_loop,                  # 웹소켓 수신 시작
)
from infra.market_data_store import (
    save_candles_bulk_from_ws,
    save_orderbook_from_ws,
)
from infra.market_data_rest import fetch_klines_rest, KlineRestError  # Binance Futures REST /fapi/v1/klines 헬퍼

# ─────────────────────────────
# 전역 상태 초기화
# ─────────────────────────────
SET = load_settings()
START_TS: float = time.time()
RUNNING: bool = True

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
# 최근 청산 시각
LAST_CLOSE_TS: float = 0.0
# 연속 손실 횟수
CONSEC_LOSSES: int = 0
# 텔레그램에서 들어오는 안전 종료 플래그
SAFE_STOP_REQUESTED: bool = False
# 거래소 포지션 동기화 주기 체크용
LAST_EXCHANGE_SYNC_TS: float = 0.0
# 포지션 살아 있을 때 상태 알림 주기
LAST_STATUS_TG_TS: float = 0.0
STATUS_TG_INTERVAL_SEC: int = getattr(SET, "unrealized_notify_sec", 1800)

# 레짐/시그널 분석 워커 주기 (기본 60초)
SIGNAL_ANALYSIS_INTERVAL_SEC: int = getattr(SET, "signal_analysis_interval_sec", 60)

# 데이터 헬스 텔레그램 리포트 주기 (기본 15분)
DATA_HEALTH_TG_INTERVAL_SEC: int = int(getattr(SET, "data_health_notify_sec", 900))
LAST_DATA_HEALTH_TG_TS: float = 0.0

# 마지막 REST /kline 백필 성공 시각 (epoch seconds)
LAST_REST_BACKFILL_AT: float = 0.0

# GPT EXIT 체크에 사용하는 마지막 1m 캔들 timestamp(ms)
LAST_EXIT_CANDLE_TS_1M: Optional[int] = None

# 마지막 ENTRY GPT 호출 시각 (ENTRY 쿨다운용)
LAST_ENTRY_GPT_CALL_TS: float = 0.0


# ─────────────────────────────────────────────
# GPT Latency Reporter (내장형)
# ─────────────────────────────────────────────

from infra.telelog import send_tg as _send_tg_for_latency, log as _log_for_latency  # 이름 충돌 방지

LATENCY_DIR = os.path.join("logs", "gpt_latency")
REPORT_INTERVAL_SEC = 1800  # 30분


def _read_recent_latency(minutes: int = 30) -> list[dict[str, str]]:
    if not os.path.isdir(LATENCY_DIR):
        return []

    now = datetime.datetime.utcnow() + datetime.timedelta(hours=9)
    cutoff = now - datetime.timedelta(minutes=minutes)
    rec: list[dict[str, str]] = []

    for fname in os.listdir(LATENCY_DIR):
        if not fname.startswith("gpt-latency") and not fname.startswith("gpt_latency"):
            continue
        if not fname.endswith(".csv"):
            continue

        fpath = os.path.join(LATENCY_DIR, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        ts = datetime.datetime.strptime(row["ts_kst"], "%Y-%m-%d %H:%M:%S")
                    except Exception:
                        continue
                    if ts >= cutoff:
                        rec.append(row)
        except Exception as e:
            _log_for_latency(f"[GPT_REPORTER] CSV read error: {e}")

    return rec


def _build_summary(records: list[dict[str, str]]) -> str:
    if not records:
        return "📉 최근 30분 동안 GPT 호출 기록이 없습니다."

    latencies: list[float] = []
    slow_count, err_count = 0, 0

    for r in records:
        if r.get("latency_sec"):
            try:
                latencies.append(float(r["latency_sec"]))
            except Exception:
                pass
        if r.get("is_slow") == "1":
            slow_count += 1
        if r.get("is_timeout_or_error") == "1":
            err_count += 1

    avg_latency = sum(latencies) / len(latencies) if latencies else 0.0
    max_latency = max(latencies) if latencies else 0.0
    success_rate = ((len(records) - err_count) / len(records)) * 100 if records else 0.0

    summary = (
        "📊 *GPT Latency Report — 최근 30분*\n"
        f"• 호출 수: {len(records)}\n"
        f"• 평균 응답 시간: {avg_latency:.2f}초\n"
        f"• 최대 응답 시간: {max_latency:.2f}초\n"
        f"• 느린 응답: {slow_count}건\n"
        f"• 오류/타임아웃: {err_count}건\n"
        f"• 성공률: {success_rate:.1f}%\n"
    )
    return summary


def start_gpt_latency_reporter() -> None:
    def _worker() -> None:
        log("[GPT_REPORTER] started")
        while True:
            try:
                rec = _read_recent_latency(30)
                summary = _build_summary(rec)
                _send_tg_for_latency(summary)
            except Exception as e:
                _log_for_latency(f"[GPT_REPORTER ERROR] {e}")
                try:
                    _send_tg_for_latency(f"[GPT_REPORTER ERROR] {e}")
                except Exception:
                    pass
            time.sleep(REPORT_INTERVAL_SEC)

    t = threading.Thread(target=_worker, daemon=True, name="gpt-latency-reporter")
    t.start()


# ─────────────────────────────
# SIGTERM 핸들러 등록
# ─────────────────────────────


def _sigterm(*_: Any) -> None:
    """컨테이너 SIGTERM 시 자연스럽게 메인 루프를 빠져나오도록 한다."""
    global RUNNING
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
    """열린 포지션이 있을 때 현재 미실현 손익을 주기적으로 텔레그램으로 보낸다.

    - 과거 버전에서는 15m 방향(상승/하락/횡보)을 함께 붙였으나,
      추세/박스 레짐 Python 로직을 모두 제거하면서 단순 PnL 안내만 남긴다.
    """
    from execution.exchange_api import fetch_open_positions

    try:
        positions = fetch_open_positions(symbol)
    except Exception as e:
        log(f"[STATUS_TG] fetch_open_positions error: {e}")
        return

    if not positions:
        return

    mins = max(1, interval_sec // 60)
    lines = [f"⏱ 현재 열린 포지션 안내 (약 {mins}분 간격)"]

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
    """Binance Futures REST /fapi/v1/klines 로 히스토리 캔들을 받아 WS 버퍼에 미리 채운다.

    - main() 시작 시점에 한 번만 호출.
    - 각 타임프레임 버퍼가 충분히 채워진 상태에서 WS 기반 신호 계산이 시작되도록 한다.
    """
    global LAST_REST_BACKFILL_AT

    # 기본은 1m/5m/15m 백필, 필요하면 settings_ws.ws_backfill_tfs 로 조정 가능
    intervals = getattr(SET, "ws_backfill_tfs", ["1m", "5m", "15m"])
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
                try:
                    send_tg(
                        "⚠ REST 백필 결과가 비어 있습니다.\n"
                        f"- 심볼: {symbol}\n"
                        f"- 주기: {iv}\n"
                        "- 사유: 응답 rows=0"
                    )
                except Exception:
                    pass
                continue

            # REST 응답 요약 로그 (첫/마지막 openTime 기준)
            try:
                first_ts = (
                    rest_rows[0][0]
                    if isinstance(rest_rows[0], (list, tuple)) and rest_rows[0]
                    else "N/A"
                )
                last_ts = (
                    rest_rows[-1][0]
                    if isinstance(rest_rows[-1], (list, tuple)) and rest_rows[-1]
                    else "N/A"
                )
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

            LAST_REST_BACKFILL_AT = time.time()

        except KlineRestError as e:
            log(f"[BOOT] REST backfill KlineRestError for {symbol} {iv}: {e}")
            METRICS["kline_failures"] = METRICS.get("kline_failures", 0) + 1
            try:
                send_tg(
                    "⛔ REST 히스토리 백필 중 오류가 발생했습니다.\n"
                    f"- 심볼: {symbol}\n"
                    f"- 주기: {iv}\n"
                    f"- 내용: {e}"
                )
            except Exception:
                pass
        except Exception as e:
            log(f"[BOOT] REST backfill failed for {symbol} {iv}: {e}")
            METRICS["kline_failures"] = METRICS.get("kline_failures", 0) + 1
            try:
                send_tg(
                    "⛔ REST 히스토리 백필 중 예기치 못한 오류가 발생했습니다.\n"
                    f"- 심볼: {symbol}\n"
                    f"- 주기: {iv}\n"
                    f"- 내용: {e}"
                )
            except Exception:
                pass


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
# 데이터 헬스 텔레그램 리포트
# ─────────────────────────────


def _send_data_health_report(symbol: str) -> None:
    """REST 백필 및 WS/오더북 데이터 상태를 1회 텔레그램으로 전송한다."""
    try:
        snap = get_health_snapshot(symbol)
    except Exception as e:
        log(f"[DATA-HEALTH] snapshot error: {e}")
        try:
            send_tg(
                "⚠ 데이터 헬스 스냅샷 조회 중 오류가 발생했습니다.\n"
                f"- 내용: {e}"
            )
        except Exception:
            pass
        return

    overall_ok = bool(snap.get("overall_ok", False))
    klines = snap.get("klines", {}) or {}
    ob_status = snap.get("orderbook", {}) or {}

    # REST 백필 상태 요약
    kline_failures = int(METRICS.get("kline_failures", 0))
    if LAST_REST_BACKFILL_AT > 0:
        last_dt = datetime.datetime.fromtimestamp(LAST_REST_BACKFILL_AT, KST)
        last_rest_str = last_dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        last_rest_str = "한 번도 성공한 적 없음"

    header_emoji = "✅" if overall_ok else "⚠️"
    lines: List[str] = [
        f"{header_emoji} 데이터 수집 상태 점검 (REST/WS)",
        f"- 심볼: {symbol}",
        f"- 마지막 REST 백필 시각: {last_rest_str}",
        f"- REST 백필 실패 누적: {kline_failures}회",
    ]

    # 주요 타임프레임만 요약 (없으면 건너뜀)
    key_intervals = ["1m", "5m", "15m", "1h", "4h", "1d"]
    lines.append("")
    lines.append("📈 K라인 버퍼 상태:")
    for iv in key_intervals:
        st = klines.get(iv)
        if not st:
            continue
        buffer_len = st.get("buffer_len", 0)
        delay_ms = st.get("delay_ms")
        if delay_ms is None:
            delay_str = "N/A"
        else:
            delay_str = f"{delay_ms / 1000.0:.1f}s"
        ok = bool(st.get("ok", False))
        ok_mark = "O" if ok else "X"
        lines.append(f"· {iv}: len={buffer_len}, delay={delay_str}, ok={ok_mark}")

    # 오더북 상태
    if ob_status:
        ob_ok = bool(ob_status.get("ok", False))
        ob_delay_ms = ob_status.get("delay_ms")
        if ob_delay_ms is None:
            ob_delay_str = "N/A"
        else:
            ob_delay_str = f"{ob_delay_ms / 1000.0:.1f}s"
        lines.append("")
        lines.append(
            f"📊 오더북(depth5) 상태: ok={'O' if ob_ok else 'X'}, delay={ob_delay_str}"
        )

    if overall_ok and LAST_REST_BACKFILL_AT > 0:
        lines.append("")
        lines.append("→ GPT 트레이더가 사용할 WS/REST 로우데이터는 현재 정상 상태입니다.")
    else:
        lines.append("")
        lines.append(
            "→ 데이터 이상으로 GPT 판단에 사용할 로우데이터가 부족할 수 있습니다. "
            "로그를 함께 확인해 주세요."
        )

    try:
        send_tg("\n".join(lines))
    except Exception as e:
        log(f"[DATA-HEALTH] send_tg error: {e}")


# ─────────────────────────────
# 메인 루프 (WS)
# ─────────────────────────────


def main() -> None:
    global RUNNING
    global OPEN_TRADES, LAST_CLOSE_TS
    global CONSEC_LOSSES, SAFE_STOP_REQUESTED, LAST_EXCHANGE_SYNC_TS
    global LAST_STATUS_TG_TS, LAST_DATA_HEALTH_TG_TS, LAST_EXIT_CANDLE_TS_1M, LAST_ENTRY_GPT_CALL_TS

    # WS 설정 로그로 남기기
    log(
        f"[BOOT] WS_ENABLED={getattr(SET, 'ws_enabled', True)} "
        f"WS_TF={getattr(SET, 'ws_subscribe_tfs', ['1m','5m','15m'])} "
        f"INTERVAL={SET.interval}"
    )

    # REST 히스토리 백필 + 웹소켓 시세 수신 시작
    if getattr(SET, "ws_enabled", True):
        _backfill_ws_kline_history(SET.symbol)
        start_ws_loop(SET.symbol)
        _start_market_data_store_thread()

        # 데이터 헬스 모니터 시작
        from infra.data_health_monitor import start_health_monitor
        start_health_monitor()

        # 부팅 직후 WS 버퍼 상태 진단
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

        # EXIT 1m 캔들 기준 초기화
        try:
            last_1m = ws_get_klines_with_volume(SET.symbol, "1m", limit=1)
            if last_1m:
                LAST_EXIT_CANDLE_TS_1M = int(last_1m[0][0])
                log(
                    f"[EXIT] INIT last 1m candle ts_ms={LAST_EXIT_CANDLE_TS_1M} "
                    "for candle-close based EXIT checks"
                )
        except Exception as e:
            log(f"[EXIT] failed to init last 1m candle ts: {e}")
            LAST_EXIT_CANDLE_TS_1M = None

    # 필수 API 키 체크
    if not SET.api_key or not SET.api_secret:
        msg = (
            "❗ 선물 API 키가 설정되어 있지 않습니다. "
            "Render 환경변수에 BINANCE_API_KEY / BINANCE_API_SECRET 값을 확인해 주세요."
        )
        log(msg)
        send_tg(msg)
        return

    # 시작 알림 및 주요 설정 로깅
    log(
        f"CONFIG: ENABLE_MARKET={getattr(SET, 'enable_market', True)}, "
        f"ENABLE_1M_CONFIRM={getattr(SET, 'enable_1m_confirm', False)}, "
        f"SYMBOL={SET.symbol}, INTERVAL={SET.interval}, "
        f"LEVERAGE={SET.leverage}, RISK_PCT={SET.risk_pct}"
    )
    send_tg("✅ Binance USDT-M Futures 자동매매(WS 버전)를 시작합니다.")

    # 레버리지/마진 모드 세팅 (실패해도 계속 감)
    try:
        set_leverage_and_mode(SET.symbol, SET.leverage, SET.isolated)
    except Exception as e:
        log(f"[WARN] 레버리지/마진 설정 실패: {e}")

    # 보조 스레드들 시작 (헬스 서버, 드라이브 동기화, 텔레그램 명령, 레짐/지표 워커, GPT 레이턴시)
    start_health_server()
    start_drive_sync_thread()
    start_telegram_command_thread(on_stop_command=_on_safe_stop)
    start_signal_analysis_thread(interval_sec=SIGNAL_ANALYSIS_INTERVAL_SEC)
    start_gpt_latency_reporter()   # GPT 레이턴시 30분 리포트 스레드

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

            # (c.5) 데이터 헬스 텔레그램 리포트 (기본 15분 간격)
            if (
                DATA_HEALTH_TG_INTERVAL_SEC > 0
                and now - LAST_DATA_HEALTH_TG_TS >= DATA_HEALTH_TG_INTERVAL_SEC
            ):
                _send_data_health_report(SET.symbol)
                LAST_DATA_HEALTH_TG_TS = now

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

            # TP/SL 재설정 실패 알림 (자동 중지 기능 비활성화 상태)
            if TRADER_STATE.should_stop_bot():
                send_tg("🚫 TP/SL 재설정 실패 발생 (자동중지 기능 비활성화됨). 계속 진행합니다.")

            # (e) 열린 포지션에 대한 실시간 대응 (1m 캔들 종가 기준 GPT EXIT 레이어)
            if OPEN_TRADES:
                # WS 1m 캔들의 마지막 ts_ms 를 기준으로, 새 캔들이 생겼을 때만 EXIT 판단 수행
                try:
                    last_1m = ws_get_klines_with_volume(SET.symbol, "1m", limit=1)
                except Exception as e:
                    last_1m = []
                    log(f"[EXIT] failed to read 1m kline buffer for EXIT check: {e}")

                if last_1m:
                    last_ts_ms = int(last_1m[0][0])
                    # 초기에는 기준 ts 만 셋업하고, 다음 캔들부터 EXIT 체크
                    if LAST_EXIT_CANDLE_TS_1M is None:
                        LAST_EXIT_CANDLE_TS_1M = last_ts_ms
                    elif last_ts_ms > LAST_EXIT_CANDLE_TS_1M:
                        # 직전 캔들 종가가 확정된 뒤 새 1m 캔들이 시작된 것으로 보고 EXIT 판단 수행
                        for t in list(OPEN_TRADES):
                            # GPT 레이어가 EXIT 여부(HOLD/CLOSE)를 전적으로 결정
                            if maybe_exit_with_gpt(t, SET, scenario="RUNTIME_EXIT_CHECK"):
                                OPEN_TRADES = [ot for ot in OPEN_TRADES if ot is not t]
                                LAST_CLOSE_TS = now
                        LAST_EXIT_CANDLE_TS_1M = last_ts_ms

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

            # (f) 포지션이 없는 상태에서 안전 종료 요청이 들어온 경우 → 메인 루프 종료
            if SAFE_STOP_REQUESTED and not OPEN_TRADES:
                send_tg("🛑 요청하신 대로 포지션을 모두 정리했고, 자동매매를 종료합니다.")
                return

            # (g) 연속 손실 방어 로직
            if CONSEC_LOSSES >= 3:
                send_tg(
                    "⛔ 연속으로 3번 손실이 발생했습니다. "
                    "설정된 휴식 시간 동안 새로 진입하지 않고 쉬었다가 다시 시작합니다."
                )
                # 기존 키가 없을 수 있으므로 호환 fallback 적용
                loss_cooldown_sec = int(
                    getattr(
                        SET,
                        "cooldown_after_3loss",
                        getattr(SET, "cooldown_after_consec_loss_sec", 10800),
                    )
                )
                time.sleep(loss_cooldown_sec)
                CONSEC_LOSSES = 0
                continue

            # (h) 방금 닫은 직후라면 쿨다운
            if LAST_CLOSE_TS > 0 and (time.time() - LAST_CLOSE_TS) < SET.cooldown_after_close:
                time.sleep(1)
                continue

            # (h-1) 안전 종료 요청 상태에서는 EXIT 만 진행하고 새 진입은 막는다
            if SAFE_STOP_REQUESTED:
                time.sleep(1)
                continue

            # (i) 새 포지션 진입 시도 (GPT 전담 엔트리)
            #     - ENTRY 쿨다운: GPT 비용 절감을 위해 최소 entry_cooldown_sec 간격으로만 진입 판단 수행
            entry_cooldown_sec = getattr(SET, "entry_cooldown_sec", 20)
            trade: Optional[Trade] = None
            if now - LAST_ENTRY_GPT_CALL_TS >= entry_cooldown_sec:
                trade, sleep_sec = try_open_new_position(
                    SET,
                    LAST_CLOSE_TS,
                )
                LAST_ENTRY_GPT_CALL_TS = now
                if trade:
                    OPEN_TRADES.append(trade)
                    LAST_STATUS_TG_TS = time.time()

            # 루프 주기는 1초
            time.sleep(1)

        except Exception as e:
            log(f"ERROR: {e}")
            try:
                send_tg(
                    "❌ 예기치 못한 오류가 발생했습니다.\n"
                    f"- 내용: {e}\n"
                    "2초 후 자동으로 다시 시도합니다."
                )
            except Exception:
                pass
            log_signal(
                event="ERROR",
                symbol=SET.symbol,
                strategy_type="UNKNOWN",
                reason=str(e),
            )
            time.sleep(2)

    # RUNNING 이 False 로 바뀌어 루프가 끝나면 여기로 빠져나온다.
    return


if __name__ == "__main__":
    main()