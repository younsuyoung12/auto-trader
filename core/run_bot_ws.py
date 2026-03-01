# core/run_bot_ws.py
"""
run_bot_ws.py – Binance USDT-M Futures WebSocket 메인 루프
============================================================

역할
-----------------------------------------------------
- WS: Binance fstream 멀티 타임프레임 kline + depth5 오더북 수신.
- REST:
  - 시장데이터(캔들/오더북) 런타임 의사결정에는 사용하지 않음(WS only).
  - 단, 계정/주문 상태(포지션/체결/잔고) 조회에는 사용 가능.
  - 부팅 시 WS 버퍼 백필(/fapi/v1/klines) 용도로도 사용.
- 진입(ENTRY): (STRICT)
  get_trading_signal → build_unified_features → Regime/Account Gate → GPTStrategy.decide
  → RiskPhysics(decide) → ExecutionEngine.execute
- 청산(EXIT): maybe_exit_with_gpt → GPT EXIT 레이어.
- 이 파일은 포지션/쿨다운/헬스체크를 오케스트레이션할 뿐,
  매수·매도·손절·익절 “실행” 로직은 execution 레이어에서만 수행한다.

핵심 원칙 (STRICT · NO-FALLBACK)
-----------------------------------------------------
- 시장데이터(캔들/오더북) 의사결정은 WS 버퍼 데이터만 사용한다.
- REST는 (a) 부팅 WS 버퍼 백필, (b) 계정/주문 상태(포지션/체결/잔고) 조회에만 사용한다.
- 폴백(REST 런타임 백필/더미 값/임의 보정/None→0 치환) 절대 금지.
- 데이터가 없거나 손상되면 즉시 예외를 발생시키고 로그에 스택트레이스를 남긴다.
- STOP_FLAG 기반 자동 종료/idle 없음. 텔레그램 '종료' 요청 + 포지션 0 일 때만 종료한다.
- 엔트리: entry_cooldown_sec 으로 GPT 엔트리 호출 간격을 제한한다.
- 익절/손절: 완성된 1m WS 캔들 종가가 확정될 때만 GPT EXIT 를 호출한다.
- WS/REST 데이터 헬스·GPT 레이턴시 상태를 주기적으로 텔레그램으로 보고한다.

PATCH NOTES — 2026-02-28
- ws_subscribe_tfs / ws_backfill_tfs 필수 TF(1m/5m/15m/1h/4h) 부팅 검증 추가.
- 부팅 REST 백필 후 WS 버퍼 적재 검증(1m/5m/15m>=20, 1h/4h>=60) 실패 시 즉시 중단.
- ENTRY 게이트: WS 캔들/오더북 미준비 시 SKIP (unified_features/GPT 호출 금지).
- 헬스 리포트: get_kline_buffer_status로 1h/4h/1d 표시, md-store TF 옵션화.

PATCH NOTES — 2026-03-02 (Trade-grade upgrade)
- RegimeEngine(절대 점수 65/75/85) 기반 allocation 도입: regime_score<65이면 GPT 호출 금지.
- AccountStateBuilder 도입: DD/연속손실/승률 기반으로 GPT 호출 전 차단 가능.
- RiskPhysicsEngine 도입: DD/연속손실/최소RR 기반으로 최종 투입 비율(=risk_pct) 결정.
- 전액 배분형: risk_pct는 "계좌 투입 비율"로 해석(1.0=전액).
- 흐름: Python(Regime/Account) → GPT(전략) → Python(RiskPhysics) → Execution
============================================================
"""

from __future__ import annotations

import csv
import datetime
import hashlib
import os
import signal
import threading
import time
import traceback
from collections import deque
from typing import Any, Dict, List, Optional

from settings import KST, load_settings  # WS 버전 설정
from infra.telelog import log, send_tg
from execution.exchange_api import (
    get_available_usdt,
    set_leverage_and_mode,
)
from execution.order_executor import close_all_positions_market
from state.trader_state import Trade, TraderState, check_closes, build_close_summary_strict
from events.signals_logger import log_signal

from infra.bot_workers import start_telegram_command_thread
from strategy.signal_analysis_worker import start_signal_analysis_thread

# 동기화/포지션 감시 모듈
from execution.sync_exchange import sync_open_trades_from_exchange
from state.exit_engine import maybe_exit_with_gpt  # WS 버전 GPT EXIT 어댑터

# WS 시세 버퍼
from infra.market_data_ws import (
    backfill_klines_from_rest,  # REST 히스토리 → WS 버퍼 백필용 (부팅 전용)
    get_health_snapshot,  # WS/오더북 데이터 헬스 스냅샷 (required_tfs 기준)
    get_kline_buffer_status as ws_get_kline_buffer_status,  # 리포트용(모든 TF 직접 조회)
    get_klines_with_volume as ws_get_klines_with_volume,  # 캔들+거래량
    get_orderbook as ws_get_orderbook,  # depth5 오더북
    start_ws_loop,  # 웹소켓 수신 시작
)
from infra.market_data_store import (
    save_candles_bulk_from_ws,
    save_orderbook_from_ws,
)
from infra.market_data_rest import fetch_klines_rest, KlineRestError  # Binance Futures REST /fapi/v1/klines 헬퍼

# ─────────────────────────────────────────────
# NEW ENTRY FLOW (STRICT · NO-FALLBACK)
# ─────────────────────────────────────────────
import infra.data_health_monitor as data_health_monitor
from infra.market_features_ws import get_trading_signal, FeatureBuildError
from strategy.unified_features_builder import build_unified_features, UnifiedFeaturesError
from strategy.gpt_strategy import GPTStrategy
from execution.execution_engine import ExecutionEngine

# ─────────────────────────────────────────────
# Trade-grade engines (STRICT · NO-FALLBACK)
# ─────────────────────────────────────────────
from strategy.regime_engine import RegimeEngine
from strategy.account_state_builder import AccountStateBuilder, AccountStateNotReadyError
from execution.risk_physics_engine import RiskPhysicsEngine, RiskPhysicsPolicy
from strategy.signal import Signal


# ─────────────────────────────
# 전역 상태 초기화
# ─────────────────────────────
SET = load_settings()
START_TS: float = time.time()
RUNNING: bool = True

# KRW 환산용 환율 (텔레그램 표시용)
KRW_RATE: float = getattr(SET, "krw_per_usdt", 1400.0)

# ENTRY에 필요한 필수 TF/최소 버퍼 개수 (WS 버퍼 기준)
ENTRY_REQUIRED_TFS: tuple[str, ...] = ("1m", "5m", "15m", "1h", "4h")
ENTRY_REQUIRED_KLINES_MIN: Dict[str, int] = {
    "1m": 20,
    "5m": 20,
    "15m": 20,
    "1h": 60,
    "4h": 60,
}

# 엔트리 차단 텔레그램 스팸 방지(동일 사유 연속 전송 제한)
_LAST_ENTRY_BLOCK_TG_TS: float = 0.0
_LAST_ENTRY_BLOCK_KEY: str = ""

# 메인 예외 텔레그램 스팸 방지(동일 예외 key를 cooldown 동안 1회만 전송)
_LAST_ERROR_TG_TS: float = 0.0
_LAST_ERROR_TG_KEY: str = ""

# SIGTERM 처리 상태
SIGTERM_REQUESTED_AT: Optional[float] = None
SIGTERM_DEADLINE_TS: Optional[float] = None
_SIGTERM_NOTICE_SENT: bool = False
_SIGTERM_DEADLINE_HANDLED: bool = False
_SIGTERM_FORCE_CLOSE_ATTEMPTED: bool = False

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


def _safe_send_tg(msg: str) -> None:
    """텔레그램 전송 실패는 시스템 동작 유지 목적에서만 무시한다."""
    try:
        send_tg(msg)
    except Exception as e:
        log(f"[TG] send_tg error: {e}")


def _parse_tfs(value: Any) -> List[str]:
    """settings에서 TF 리스트를 안전하게 파싱한다."""
    if value is None:
        return []
    if isinstance(value, str):
        items = [x.strip() for x in value.split(",")]
        return [x for x in items if x]
    if isinstance(value, (list, tuple, set)):
        out: List[str] = []
        for v in value:
            s = str(v).strip()
            if s:
                out.append(s)
        return out
    return []


def _verify_required_tfs_or_die(name: str, configured_tfs: List[str], required_tfs: tuple[str, ...]) -> None:
    norm = {str(x).strip().lower() for x in configured_tfs if str(x).strip()}
    missing = [tf for tf in required_tfs if tf.lower() not in norm]
    if missing:
        msg = f"⛔ 설정 오류: {name} 에 필수 TF 누락: {missing}. 필수={list(required_tfs)}"
        log(msg)
        _safe_send_tg(msg)
        raise RuntimeError(msg)


def _verify_ws_boot_configuration_or_die() -> None:
    """부팅 시 WS 구독/백필 설정이 ENTRY 요구사항을 만족하는지 강제 검증한다."""
    ws_subscribe_tfs = _parse_tfs(getattr(SET, "ws_subscribe_tfs", None))
    _verify_required_tfs_or_die("ws_subscribe_tfs", ws_subscribe_tfs, ENTRY_REQUIRED_TFS)

    ws_backfill_tfs = _parse_tfs(getattr(SET, "ws_backfill_tfs", None))
    if not ws_backfill_tfs:
        ws_backfill_tfs = list(ENTRY_REQUIRED_TFS)
    _verify_required_tfs_or_die("ws_backfill_tfs", ws_backfill_tfs, ENTRY_REQUIRED_TFS)


def _maybe_send_entry_block_tg(key: str, msg: str, cooldown_sec: int = 60) -> None:
    """ENTRY 차단 사유 텔레그램 스팸을 제한한다(동일 key는 cooldown 동안 1회만 전송)."""
    global _LAST_ENTRY_BLOCK_TG_TS, _LAST_ENTRY_BLOCK_KEY
    now = time.time()
    if key == _LAST_ENTRY_BLOCK_KEY and (now - _LAST_ENTRY_BLOCK_TG_TS) < cooldown_sec:
        log(f"[SKIP_TG_SUPPRESS] {msg}")
        return
    _LAST_ENTRY_BLOCK_KEY = key
    _LAST_ENTRY_BLOCK_TG_TS = now
    _safe_send_tg(msg)


def _maybe_send_error_tg(key: str, msg: str, cooldown_sec: int = 60) -> None:
    """메인 예외 텔레그램 스팸을 제한한다(동일 key는 cooldown 동안 1회만 전송)."""
    global _LAST_ERROR_TG_TS, _LAST_ERROR_TG_KEY
    now = time.time()
    if key == _LAST_ERROR_TG_KEY and (now - _LAST_ERROR_TG_TS) < cooldown_sec:
        log(f"[SKIP_TG_SUPPRESS][ERROR] {msg}")
        return
    _LAST_ERROR_TG_KEY = key
    _LAST_ERROR_TG_TS = now
    _safe_send_tg(msg)


def interruptible_sleep(total_sec: float, tick: float = 1.0) -> None:
    """인터럽트 가능한 슬립."""
    if total_sec is None:
        return
    try:
        total = float(total_sec)
    except Exception:
        raise ValueError("total_sec must be numeric")
    if total <= 0:
        return

    try:
        tick_f = float(tick)
    except Exception:
        tick_f = 1.0
    if tick_f <= 0:
        tick_f = 1.0

    end_ts = time.time() + total
    while True:
        if not RUNNING:
            return
        if SAFE_STOP_REQUESTED:
            return
        now = time.time()
        if SIGTERM_DEADLINE_TS is not None and now >= SIGTERM_DEADLINE_TS:
            return
        remain = end_ts - now
        if remain <= 0:
            return
        time.sleep(min(tick_f, remain))


def _validate_orderbook_for_entry(symbol: str) -> Optional[str]:
    ob = ws_get_orderbook(symbol, limit=5)
    if not isinstance(ob, dict):
        return "orderbook missing (ws_get_orderbook returned non-dict/None)"

    bids = ob.get("bids")
    asks = ob.get("asks")
    if not isinstance(bids, list) or not bids:
        return "orderbook bids empty"
    if not isinstance(asks, list) or not asks:
        return "orderbook asks empty"

    best_bid = ob.get("bestBid")
    best_ask = ob.get("bestAsk")

    try:
        best_bid = float(bids[0][0]) if best_bid is None else float(best_bid)
    except Exception:
        return "orderbook bestBid invalid"

    try:
        best_ask = float(asks[0][0]) if best_ask is None else float(best_ask)
    except Exception:
        return "orderbook bestAsk invalid"

    if best_bid <= 0 or best_ask <= 0:
        return f"orderbook best prices invalid (bestBid={best_bid}, bestAsk={best_ask})"
    if best_ask <= best_bid:
        return f"orderbook crossed (bestAsk={best_ask} <= bestBid={best_bid})"

    return None


def _validate_klines_for_entry(symbol: str) -> Optional[str]:
    for iv, min_len in ENTRY_REQUIRED_KLINES_MIN.items():
        buf = ws_get_klines_with_volume(symbol, iv, limit=min_len)
        if not isinstance(buf, list):
            return f"kline buffer invalid type for {iv}"
        if len(buf) < min_len:
            return f"kline buffer 부족: {iv} need={min_len} got={len(buf)}"
    return None


def _validate_ws_entry_prereqs(symbol: str) -> Optional[str]:
    r = _validate_orderbook_for_entry(symbol)
    if r:
        return r
    r = _validate_klines_for_entry(symbol)
    if r:
        return r
    return None


# ─────────────────────────────────────────────
# AccountState bootstrap (DB read, STRICT)
# ─────────────────────────────────────────────
def _load_closed_trades_bootstrap(symbol: str, limit: int = 50) -> list[dict[str, Any]]:
    """
    STRICT:
    - DATABASE_URL 환경변수 기반으로 bt_trades에서 최근 청산 트레이드 로드.
    - 실패 시 예외(폴백 금지). 단, caller가 예외를 받아 '명시적 SKIP' 처리 가능.
    """
    dsn = os.getenv("DATABASE_URL")
    if not dsn or not str(dsn).strip():
        raise RuntimeError("DATABASE_URL is required for account_state bootstrap")

    try:
        import psycopg2
    except Exception as e:
        raise RuntimeError("psycopg2 is required for account_state bootstrap") from e

    q = """
        SELECT id, exit_ts, pnl_usdt, tp_pct, sl_pct
        FROM bt_trades
        WHERE symbol = %s
          AND exit_ts IS NOT NULL
          AND pnl_usdt IS NOT NULL
        ORDER BY exit_ts DESC
        LIMIT %s
    """

    rows: list[dict[str, Any]] = []
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        cur.execute(q, (symbol, int(limit)))
        for (trade_id, exit_ts, pnl_usdt, tp_pct, sl_pct) in cur.fetchall():
            if exit_ts is None:
                raise RuntimeError("bt_trades.exit_ts is NULL (unexpected)")
            rows.append(
                {
                    "id": int(trade_id),
                    "exit_ts": exit_ts,
                    "pnl_usdt": float(pnl_usdt),
                    "tp_pct": None if tp_pct is None else float(tp_pct),
                    "sl_pct": None if sl_pct is None else float(sl_pct),
                }
            )
        return rows
    finally:
        try:
            if cur is not None:
                cur.close()
        except Exception:
            pass
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


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

    return (
        "📊 *GPT Latency Report — 최근 30분*\n"
        f"• 호출 수: {len(records)}\n"
        f"• 평균 응답 시간: {avg_latency:.2f}초\n"
        f"• 최대 응답 시간: {max_latency:.2f}초\n"
        f"• 느린 응답: {slow_count}건\n"
        f"• 오류/타임아웃: {err_count}건\n"
        f"• 성공률: {success_rate:.1f}%\n"
    )


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
    """SIGTERM 수신 시 안전 종료를 요청한다."""
    global SAFE_STOP_REQUESTED, SIGTERM_REQUESTED_AT, SIGTERM_DEADLINE_TS, _SIGTERM_NOTICE_SENT

    SAFE_STOP_REQUESTED = True
    now = time.time()

    if SIGTERM_REQUESTED_AT is None:
        SIGTERM_REQUESTED_AT = now
        try:
            grace = float(getattr(SET, "sigterm_grace_sec", 30.0) or 30.0)
        except Exception:
            grace = 30.0
        if grace <= 0:
            grace = 30.0
        SIGTERM_DEADLINE_TS = now + grace

        msg = f"🧯 SIGTERM 수신: 신규 진입 중단, 포지션 정리 후 종료 시도 (grace={int(grace)}s)"
        log(msg)
        if not _SIGTERM_NOTICE_SENT:
            _SIGTERM_NOTICE_SENT = True
            _safe_send_tg(msg)


signal.signal(signal.SIGTERM, _sigterm)


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


def _send_open_positions_status(symbol: str, interval_sec: int) -> None:
    """열린 포지션이 있을 때 현재 미실현 손익을 주기적으로 텔레그램으로 보낸다."""
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

    _safe_send_tg("\n".join(lines))


def _on_safe_stop() -> None:
    """텔레그램 명령으로 안전 종료를 요청받았을 때 플래그만 세팅한다."""
    global SAFE_STOP_REQUESTED
    SAFE_STOP_REQUESTED = True
    _safe_send_tg("🛑 텔레그램에서 '종료' 버튼을 눌렀습니다. 현재 포지션을 모두 정리한 뒤 자동매매를 멈춥니다.")


def _build_entry_market_data(settings: Any, last_close_ts: float) -> Optional[Dict[str, Any]]:
    """
    STRICT:
    - get_trading_signal이 None이면 None 반환(정상).
    - 신호가 있는데 WS 필수 데이터가 미준비면 SKIP 처리, GPT 호출 금지.
    - unified_features는 build_unified_features로만 생성한다(실패 시 SKIP).
    - 런타임 REST 폴백은 절대 하지 않는다.
    """
    signal_ctx = get_trading_signal(settings=settings, last_close_ts=last_close_ts)
    if signal_ctx is None:
        return None

    if not isinstance(signal_ctx, (tuple, list)) or len(signal_ctx) != 7:
        raise RuntimeError("get_trading_signal returned invalid tuple format")

    chosen_signal, signal_source, latest_ts, candles_5m, candles_5m_raw, last_price, extra = signal_ctx

    symbol = str(getattr(settings, "symbol", "")).strip()
    if not symbol:
        raise RuntimeError("settings.symbol is required")

    direction = str(chosen_signal).upper().strip()
    if direction not in ("LONG", "SHORT"):
        raise RuntimeError(f"invalid chosen_signal: {chosen_signal!r}")

    signal_source_s = str(signal_source).strip()
    if not signal_source_s:
        raise RuntimeError("signal_source is empty")

    ts_ms = int(latest_ts)
    if ts_ms <= 0:
        raise RuntimeError(f"invalid latest_ts: {latest_ts!r}")

    prereq_reason = _validate_ws_entry_prereqs(symbol)
    if prereq_reason:
        msg = f"[SKIP][WS_NOT_READY] entry blocked: {symbol} ({prereq_reason})"
        log(msg)
        _maybe_send_entry_block_tg(f"WS_NOT_READY:{symbol}:{prereq_reason}", msg, cooldown_sec=60)
        return None

    try:
        market_features = build_unified_features(symbol)
    except (UnifiedFeaturesError, FeatureBuildError) as e:
        msg = f"[SKIP][FEATURE_BUILD_FAIL] entry blocked: {symbol} ({e})"
        log(msg)
        _maybe_send_entry_block_tg(f"FEATURE_BUILD_FAIL:{symbol}:{type(e).__name__}", msg, cooldown_sec=60)
        return None

    if not isinstance(market_features, dict) or not market_features:
        raise RuntimeError("build_unified_features returned empty or non-dict")

    if extra is not None and not isinstance(extra, dict):
        raise RuntimeError("extra must be dict or None")

    return {
        "symbol": symbol,
        "direction": direction,
        "signal_source": signal_source_s,
        "regime": signal_source_s,  # 기존 관행 유지
        "signal_ts_ms": ts_ms,
        "candles_5m": candles_5m,
        "candles_5m_raw": candles_5m_raw,
        "last_price": float(last_price),
        "extra": extra,
        "market_features": market_features,
    }


def _backfill_ws_kline_history(symbol: str) -> None:
    """
    Binance Futures REST /fapi/v1/klines 로 히스토리 캔들을 받아 WS 버퍼에 미리 채운다.
    STRICT: 부팅 전용. 필수 TF 백필 실패/부족 시 부팅 중단.
    """
    global LAST_REST_BACKFILL_AT

    configured_intervals = _parse_tfs(getattr(SET, "ws_backfill_tfs", None))
    intervals = configured_intervals or list(ENTRY_REQUIRED_TFS)
    _verify_required_tfs_or_die("ws_backfill_tfs", intervals, ENTRY_REQUIRED_TFS)

    limit = int(getattr(SET, "ws_backfill_limit", 120))

    for iv in intervals:
        min_needed = ENTRY_REQUIRED_KLINES_MIN.get(iv, 1)

        log(f"[BOOT] REST backfill start: symbol={symbol} interval={iv} limit={limit}")
        try:
            rest_rows = fetch_klines_rest(symbol, iv, limit=limit)
        except KlineRestError as e:
            METRICS["kline_failures"] = METRICS.get("kline_failures", 0) + 1
            msg = (
                "⛔ REST 히스토리 백필 실패(KlineRestError) — 부팅 중단\n"
                f"- 심볼: {symbol}\n"
                f"- 주기: {iv}\n"
                f"- 내용: {e}"
            )
            log(msg)
            _safe_send_tg(msg)
            raise RuntimeError(msg) from e
        except Exception as e:
            METRICS["kline_failures"] = METRICS.get("kline_failures", 0) + 1
            msg = (
                "⛔ REST 히스토리 백필 실패(Unexpected) — 부팅 중단\n"
                f"- 심볼: {symbol}\n"
                f"- 주기: {iv}\n"
                f"- 내용: {e}"
            )
            log(msg)
            _safe_send_tg(msg)
            raise RuntimeError(msg) from e

        if not rest_rows:
            METRICS["kline_failures"] = METRICS.get("kline_failures", 0) + 1
            msg = (
                "⛔ REST 백필 결과가 비어 있습니다 — 부팅 중단\n"
                f"- 심볼: {symbol}\n"
                f"- 주기: {iv}\n"
                "- 사유: 응답 rows=0"
            )
            log(msg)
            _safe_send_tg(msg)
            raise RuntimeError(msg)

        log(f"[BOOT] REST backfill fetched: symbol={symbol} interval={iv} rows={len(rest_rows)}")
        backfill_klines_from_rest(symbol, iv, rest_rows)

        verify_limit = max(60, min_needed)
        buf = ws_get_klines_with_volume(symbol, iv, limit=verify_limit)
        buf_len = len(buf) if isinstance(buf, list) else 0

        log(f"[BOOT] REST backfill verify: symbol={symbol} interval={iv} ws_buf_len={buf_len} (need>={min_needed})")

        if buf_len < min_needed:
            msg = (
                "⛔ WS 버퍼 적재 검증 실패 — 부팅 중단\n"
                f"- 심볼: {symbol}\n"
                f"- 주기: {iv}\n"
                f"- WS 버퍼 len: {buf_len}\n"
                f"- 필요 len: {min_needed}\n"
                f"- 힌트: ws_backfill_tfs / ws_backfill_limit / 네트워크 상태를 확인하세요."
            )
            log(msg)
            _safe_send_tg(msg)
            raise RuntimeError(msg)

        LAST_REST_BACKFILL_AT = time.time()


def _start_market_data_store_thread() -> None:
    """WS 버퍼에서 캔들/호가를 읽어 주기적으로 Postgres 에 저장하는 백그라운드 스레드."""
    symbol = SET.symbol
    flush_sec = getattr(SET, "md_store_flush_sec", 5)
    ob_interval_sec = getattr(SET, "ob_store_interval_sec", 5)

    configured_tfs = _parse_tfs(getattr(SET, "md_store_tfs", None))
    store_tfs = configured_tfs or ["1m", "5m", "15m"]

    last_candle_ts: Dict[str, int] = {iv: 0 for iv in store_tfs}
    last_ob_ts: float = 0.0

    def _loop() -> None:
        nonlocal last_ob_ts
        log(f"[MD-STORE] loop started: flush_sec={flush_sec}, ob_interval_sec={ob_interval_sec}, store_tfs={store_tfs}")
        while RUNNING:
            try:
                now = time.time()
                candles_to_save: List[Dict[str, Any]] = []

                for iv in store_tfs:
                    buf = ws_get_klines_with_volume(symbol, iv, limit=500)
                    if not buf:
                        continue

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
                                "quote_volume": None,
                                "source": "ws",
                            }
                        )
                    last_candle_ts[iv] = int(new_rows[-1][0])

                if candles_to_save:
                    save_candles_bulk_from_ws(candles_to_save)

                if now - last_ob_ts >= ob_interval_sec:
                    ob = ws_get_orderbook(symbol, limit=5)
                    if ob and ob.get("bids") and ob.get("asks"):
                        ts_ms = None
                        for k in ("exchTs", "ts"):
                            if ob.get(k) is not None:
                                try:
                                    ts_ms = int(ob[k])
                                    break
                                except Exception:
                                    continue
                        if ts_ms is None:
                            ts_ms = int(now * 1000)

                        save_orderbook_from_ws(symbol=symbol, ts_ms=ts_ms, bids=ob["bids"], asks=ob["asks"])
                        last_ob_ts = now

            except Exception as e:
                log(f"[MD-STORE] loop error: {e}")

            time.sleep(flush_sec)

    th = threading.Thread(target=_loop, name="md-store-loop", daemon=True)
    th.start()
    log("[MD-STORE] background store thread started")


def _send_data_health_report(symbol: str) -> None:
    """REST 백필 및 WS/오더북 데이터 상태를 1회 텔레그램으로 전송한다."""
    try:
        snap = get_health_snapshot(symbol)
    except Exception as e:
        log(f"[DATA-HEALTH] snapshot error: {e}")
        _safe_send_tg("⚠ 데이터 헬스 스냅샷 조회 중 오류가 발생했습니다.\n" f"- 내용: {e}")
        return

    overall_ok = bool(snap.get("overall_ok", False))
    ob_status = snap.get("orderbook", {}) or {}

    key_intervals = ["1m", "5m", "15m", "1h", "4h", "1d"]

    kline_failures = int(METRICS.get("kline_failures", 0))
    if LAST_REST_BACKFILL_AT > 0:
        last_dt = datetime.datetime.fromtimestamp(LAST_REST_BACKFILL_AT, KST)
        last_rest_str = last_dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        last_rest_str = "한 번도 성공한 적 없음"

    entry_prereq_reason = _validate_ws_entry_prereqs(symbol)
    entry_ready = entry_prereq_reason is None

    header_emoji = "✅" if overall_ok and entry_ready else "⚠️"
    lines: List[str] = [
        f"{header_emoji} 데이터 수집 상태 점검 (REST/WS)",
        f"- 심볼: {symbol}",
        f"- 마지막 REST 백필 시각: {last_rest_str}",
        f"- REST 백필 실패 누적: {kline_failures}회",
        f"- ENTRY WS 준비: {'O' if entry_ready else 'X'}" + ("" if entry_ready else f" ({entry_prereq_reason})"),
        "",
        "📈 K라인 버퍼 상태:",
    ]

    min_buf = int(getattr(SET, "ws_min_kline_buffer", 60))
    max_delay_sec = float(getattr(SET, "ws_max_kline_delay_sec", 120.0))

    for iv in key_intervals:
        st = ws_get_kline_buffer_status(symbol, iv)
        buffer_len = int(st.get("buffer_len") or 0)
        delay_ms = st.get("delay_ms")
        if delay_ms is None:
            delay_str = "N/A"
            ok = False
        else:
            delay_sec = float(delay_ms) / 1000.0
            delay_str = f"{delay_sec:.1f}s"
            ok = buffer_len >= min_buf and delay_sec <= max_delay_sec
        ok_mark = "O" if ok else "X"
        lines.append(f"· {iv}: len={buffer_len}, delay={delay_str}, ok={ok_mark}")

    if ob_status:
        ob_ok = bool(ob_status.get("ok", False))
        ob_delay_ms = ob_status.get("delay_ms")
        ob_delay_str = "N/A" if ob_delay_ms is None else f"{float(ob_delay_ms) / 1000.0:.1f}s"
        lines.append("")
        lines.append(f"📊 오더북(depth5) 상태: ok={'O' if ob_ok else 'X'}, delay={ob_delay_str}")

    lines.append("")
    if overall_ok and entry_ready and LAST_REST_BACKFILL_AT > 0:
        lines.append("→ GPT 트레이더가 사용할 WS 로우데이터는 현재 ENTRY 기준으로 준비되어 있습니다.")
    else:
        lines.append("→ 데이터 이상/부족으로 GPT 판단에 사용할 로우데이터가 부족할 수 있습니다. 로그를 확인해 주세요.")

    _safe_send_tg("\n".join(lines))


def main() -> None:
    global RUNNING
    global OPEN_TRADES, LAST_CLOSE_TS
    global CONSEC_LOSSES, SAFE_STOP_REQUESTED, LAST_EXCHANGE_SYNC_TS
    global LAST_STATUS_TG_TS, LAST_DATA_HEALTH_TG_TS, LAST_EXIT_CANDLE_TS_1M, LAST_ENTRY_GPT_CALL_TS
    global SIGTERM_DEADLINE_TS, _SIGTERM_DEADLINE_HANDLED, _SIGTERM_FORCE_CLOSE_ATTEMPTED

    _verify_ws_boot_configuration_or_die()

    log(
        f"[BOOT] WS_ENABLED={getattr(SET, 'ws_enabled', True)} "
        f"WS_TF={getattr(SET, 'ws_subscribe_tfs', ['1m','5m','15m'])} "
        f"INTERVAL={SET.interval}"
    )

    if getattr(SET, "ws_enabled", True):
        _backfill_ws_kline_history(SET.symbol)
        start_ws_loop(SET.symbol)
        _start_market_data_store_thread()

        from infra.data_health_monitor import start_health_monitor
        start_health_monitor()

        interruptible_sleep(2)
        for iv in ENTRY_REQUIRED_TFS:
            buf = ws_get_klines_with_volume(SET.symbol, iv, limit=5)
            buf_len = len(buf) if isinstance(buf, list) else 0
            log(f"[BOOT] WS buffer status: symbol={SET.symbol} interval={iv} len={buf_len}")

        last_1m = ws_get_klines_with_volume(SET.symbol, "1m", limit=1)
        if last_1m:
            LAST_EXIT_CANDLE_TS_1M = int(last_1m[0][0])
            log(f"[EXIT] INIT last 1m candle ts_ms={LAST_EXIT_CANDLE_TS_1M} for candle-close based EXIT checks")

    if not SET.api_key or not SET.api_secret:
        msg = "❗ API 자격정보가 설정되어 있지 않습니다. 설정 후 재시작해 주세요."
        log(msg)
        _safe_send_tg(msg)
        return

    log(
        f"CONFIG: ENABLE_MARKET={getattr(SET, 'enable_market', True)}, "
        f"ENABLE_1M_CONFIRM={getattr(SET, 'enable_1m_confirm', False)}, "
        f"SYMBOL={SET.symbol}, INTERVAL={SET.interval}, "
        f"LEVERAGE={SET.leverage}, RISK_PCT={getattr(SET, 'risk_pct', None)}"
    )

    try:
        set_leverage_and_mode(SET.symbol, SET.leverage, SET.isolated)
    except Exception as e:
        allow = bool(getattr(SET, "allow_start_without_leverage_setup", False))
        msg = f"❗ 레버리지/마진 설정 실패: {e}"
        log(msg)
        if allow:
            _safe_send_tg(msg + "\n⚠️ allow_start_without_leverage_setup=True 이므로 계속 진행합니다.")
        else:
            _safe_send_tg(msg + "\n⛔ 기본 정책에 따라 중단합니다.")
            return

    _safe_send_tg("✅ Binance USDT-M Futures 자동매매(WS 버전)를 시작합니다.")

    start_telegram_command_thread(on_stop_command=_on_safe_stop)
    start_signal_analysis_thread(interval_sec=SIGNAL_ANALYSIS_INTERVAL_SEC)
    start_gpt_latency_reporter()

    OPEN_TRADES, _ = sync_open_trades_from_exchange(SET.symbol, replace=True, current_trades=OPEN_TRADES)
    LAST_EXCHANGE_SYNC_TS = time.time()

    entry_strategy = GPTStrategy(SET)
    entry_exec_engine = ExecutionEngine(SET)

    regime_engine = RegimeEngine(window_size=200, percentile_min_history=60)
    account_builder = AccountStateBuilder(win_rate_window=20, min_trades_for_win_rate=5)

    # 전액 배분형 정책: 상한은 1.0(전액)
    risk_policy = RiskPhysicsPolicy(max_allocation=1.0)
    risk_physics = RiskPhysicsEngine(policy=risk_policy)

    CLOSED_TRADES_CACHE = deque(maxlen=50)
    try:
        boot_rows = _load_closed_trades_bootstrap(SET.symbol, limit=50)
        for r in boot_rows:
            CLOSED_TRADES_CACHE.append(r)
    except Exception as e:
        log(f"[WARN] account_state bootstrap failed: {type(e).__name__}: {e}")
        CLOSED_TRADES_CACHE.clear()

    now_kst = datetime.datetime.now(KST)
    last_report_date_kst = now_kst.strftime("%Y-%m-%d")
    daily_pnl: float = 0.0

    position_resync_sec = getattr(SET, "position_resync_sec", 20)
    last_fill_check: float = 0.0
    last_balance_log: float = 0.0

    while RUNNING:
        try:
            METRICS["last_loop_ts"] = time.time()
            METRICS["open_trades"] = len(OPEN_TRADES)
            METRICS["consec_losses"] = CONSEC_LOSSES

            now = time.time()

            if SIGTERM_DEADLINE_TS is not None and now >= SIGTERM_DEADLINE_TS:
                if not _SIGTERM_DEADLINE_HANDLED:
                    _SIGTERM_DEADLINE_HANDLED = True
                    log(f"[SIGTERM] grace deadline reached (deadline_ts={SIGTERM_DEADLINE_TS}, now={now})")

                    if OPEN_TRADES and not _SIGTERM_FORCE_CLOSE_ATTEMPTED:
                        _SIGTERM_FORCE_CLOSE_ATTEMPTED = True
                        msg = "⏳ SIGTERM grace 초과: Reduce-Only 시장가 강제 청산을 시도합니다."
                        log(msg)
                        _safe_send_tg(msg)
                        try:
                            submitted = close_all_positions_market(SET.symbol)
                            log(f"[SIGTERM] force close submitted={submitted}")
                        except Exception as e:
                            log(f"[SIGTERM][ERROR] force close failed: {e}")
                            log(traceback.format_exc())

                    try:
                        OPEN_TRADES, _ = sync_open_trades_from_exchange(
                            symbol=SET.symbol,
                            current_trades=OPEN_TRADES,
                            replace=True,
                        )
                        LAST_EXCHANGE_SYNC_TS = time.time()
                    except Exception as e:
                        log(f"[SIGTERM][ERROR] post-force sync failed: {e}")
                        log(traceback.format_exc())

                    if OPEN_TRADES:
                        warn = "⚠️ SIGTERM 종료: 데드라인 초과 후에도 포지션이 남아 있습니다. 로그를 확인하세요."
                        log(warn)
                        _safe_send_tg(warn)

                RUNNING = False
                continue

            if now - LAST_EXCHANGE_SYNC_TS >= position_resync_sec:
                OPEN_TRADES, _ = sync_open_trades_from_exchange(
                    SET.symbol,
                    replace=True,
                    current_trades=OPEN_TRADES,
                )
                LAST_EXCHANGE_SYNC_TS = now

            if now - last_balance_log >= 60:
                get_available_usdt()
                last_balance_log = now

            now_kst = datetime.datetime.now(KST)
            today_kst = now_kst.strftime("%Y-%m-%d")
            if now_kst.hour == 0 and now_kst.minute < 1 and last_report_date_kst != today_kst:
                daily_pnl_krw = daily_pnl * KRW_RATE
                _safe_send_tg(
                    "📊 하루 정산 (한국시간 기준)\n"
                    f"- 오늘 손익: {daily_pnl:.2f} USDT (약 {daily_pnl_krw:,.0f}원)\n"
                    f"- 연속 손실 횟수: {CONSEC_LOSSES}회"
                )
                daily_pnl = 0.0
                CONSEC_LOSSES = 0
                last_report_date_kst = today_kst

            if DATA_HEALTH_TG_INTERVAL_SEC > 0 and now - LAST_DATA_HEALTH_TG_TS >= DATA_HEALTH_TG_INTERVAL_SEC:
                _send_data_health_report(SET.symbol)
                LAST_DATA_HEALTH_TG_TS = now

            if now - last_fill_check >= SET.poll_fills_sec:
                OPEN_TRADES, closed_list = check_closes(OPEN_TRADES, TRADER_STATE)
                for closed in closed_list:
                    t: Trade = closed["trade"]
                    reason: str = closed["reason"]
                    summary: Any = closed.get("summary")

                    if not isinstance(summary, dict) or not summary:
                        log(f"[WARN] close summary missing/invalid -> requery once (symbol={t.symbol} reason={reason})")
                        reason2, summary2 = build_close_summary_strict(t)
                        if not isinstance(summary2, dict) or not summary2:
                            raise RuntimeError("close summary requery returned invalid")
                        if str(reason).upper() in ("", "CLOSED") and str(reason2 or ""):
                            reason = str(reason2)
                        summary = summary2

                    for k in ("qty", "avg_price", "pnl"):
                        if k not in summary:
                            raise RuntimeError(f"close summary missing required key: {k}")

                    closed_qty = float(summary["qty"])
                    closed_price = float(summary["avg_price"])
                    pnl = float(summary["pnl"])

                    if closed_qty <= 0:
                        raise RuntimeError(f"close summary qty invalid: {closed_qty}")
                    if closed_price <= 0:
                        raise RuntimeError(f"close summary avg_price invalid: {closed_price}")

                    daily_pnl += pnl
                    LAST_CLOSE_TS = now

                    if pnl < 0:
                        CONSEC_LOSSES += 1
                    else:
                        CONSEC_LOSSES = 0

                    trade_id = getattr(t, "id", None)
                    if not isinstance(trade_id, int) or trade_id <= 0:
                        raise RuntimeError("closed trade missing valid trade.id (DB record mismatch)")

                    exit_ts_dt = datetime.datetime.fromtimestamp(float(now), tz=datetime.timezone.utc)

                    tp_pct_v = getattr(t, "tp_pct", None)
                    sl_pct_v = getattr(t, "sl_pct", None)

                    CLOSED_TRADES_CACHE.append(
                        {
                            "id": int(trade_id),
                            "exit_ts": exit_ts_dt,
                            "pnl_usdt": float(pnl),
                            "tp_pct": None if tp_pct_v is None else float(tp_pct_v),
                            "sl_pct": None if sl_pct_v is None else float(sl_pct_v),
                        }
                    )

                    pnl_krw = pnl * KRW_RATE
                    if getattr(SET, "notify_on_close", True):
                        reason_ko = _translate_close_reason(reason)
                        side_ko = "롱" if str(t.side).upper() in ("BUY", "LONG") else "숏"
                        _safe_send_tg(
                            "💰 포지션 청산 알림\n"
                            f"- 종목: {t.symbol}\n"
                            f"- 방향: {side_ko}\n"
                            f"- 청산 사유: {reason_ko}\n"
                            f"- 수량: {closed_qty:.4f}\n"
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

            if TRADER_STATE.should_stop_bot():
                _safe_send_tg("🚫 TP/SL 재설정 실패 발생 (자동중지 기능 비활성화됨). 계속 진행합니다.")

            if OPEN_TRADES:
                last_1m = ws_get_klines_with_volume(SET.symbol, "1m", limit=1)
                if last_1m:
                    last_ts_ms = int(last_1m[0][0])
                    if LAST_EXIT_CANDLE_TS_1M is None:
                        LAST_EXIT_CANDLE_TS_1M = last_ts_ms
                    elif last_ts_ms > LAST_EXIT_CANDLE_TS_1M:
                        exit_resync_needed = False
                        for t in list(OPEN_TRADES):
                            if maybe_exit_with_gpt(t, SET, scenario="RUNTIME_EXIT_CHECK"):
                                OPEN_TRADES = [ot for ot in OPEN_TRADES if ot is not t]
                                LAST_CLOSE_TS = now
                                exit_resync_needed = True
                        LAST_EXIT_CANDLE_TS_1M = last_ts_ms

                        if exit_resync_needed:
                            try:
                                OPEN_TRADES, _ = sync_open_trades_from_exchange(
                                    symbol=SET.symbol,
                                    current_trades=OPEN_TRADES,
                                    replace=True,
                                )
                                LAST_EXCHANGE_SYNC_TS = time.time()
                            except Exception as e:
                                log(f"[WARN] post-exit sync failed: {e}")
                                log(traceback.format_exc())

                if OPEN_TRADES:
                    if getattr(SET, "unrealized_notify_enabled", False) and now - LAST_STATUS_TG_TS >= STATUS_TG_INTERVAL_SEC:
                        _send_open_positions_status(SET.symbol, STATUS_TG_INTERVAL_SEC)
                        LAST_STATUS_TG_TS = now
                    interruptible_sleep(1)
                    continue

            if SAFE_STOP_REQUESTED and not OPEN_TRADES:
                if SIGTERM_REQUESTED_AT is None:
                    _safe_send_tg("🛑 요청하신 대로 포지션을 모두 정리했고, 자동매매를 종료합니다.")
                else:
                    log("[SIGTERM] all positions are cleared; exiting")
                return

            if CONSEC_LOSSES >= 3:
                _safe_send_tg(
                    "⛔ 연속으로 3번 손실이 발생했습니다. 설정된 휴식 시간 동안 새로 진입하지 않고 쉬었다가 다시 시작합니다."
                )
                loss_cooldown_sec = int(getattr(SET, "cooldown_after_3loss", getattr(SET, "cooldown_after_consec_loss_sec", 10800)))
                interruptible_sleep(loss_cooldown_sec)
                CONSEC_LOSSES = 0
                continue

            if LAST_CLOSE_TS > 0 and (time.time() - LAST_CLOSE_TS) < SET.cooldown_after_close:
                interruptible_sleep(1)
                continue

            if SAFE_STOP_REQUESTED:
                interruptible_sleep(1)
                continue

            entry_cooldown_sec = getattr(SET, "entry_cooldown_sec", 20)

            if now - LAST_ENTRY_GPT_CALL_TS >= entry_cooldown_sec:
                if not bool(data_health_monitor.HEALTH_OK):
                    msg = f"[SKIP][DATA_HEALTH_FAIL] entry blocked: {data_health_monitor.LAST_FAIL_REASON}"
                    log(msg)
                    _maybe_send_entry_block_tg("DATA_HEALTH_FAIL", msg, cooldown_sec=60)
                    interruptible_sleep(1)
                    continue

                market_data = _build_entry_market_data(SET, LAST_CLOSE_TS)
                if market_data is not None:
                    mf = market_data.get("market_features")
                    if not isinstance(mf, dict) or not mf:
                        raise RuntimeError("market_features missing (STRICT)")

                    eng = mf.get("engine_scores")
                    if not isinstance(eng, dict) or not eng:
                        raise RuntimeError("engine_scores missing (STRICT)")

                    total = eng.get("total")
                    if not isinstance(total, dict) or not total:
                        raise RuntimeError("engine_scores.total missing (STRICT)")
                    if "score" not in total:
                        raise RuntimeError("engine_scores.total.score missing (STRICT)")

                    regime_score = total["score"]
                    regime_engine.update(regime_score)
                    regime_decision = regime_engine.decide(regime_score)

                    if regime_decision.allocation <= 0.0:
                        msg = f"[SKIP][REGIME_NO_TRADE] score={regime_decision.score:.1f} band={regime_decision.band}"
                        log(msg)
                        _maybe_send_entry_block_tg("REGIME_NO_TRADE", msg, cooldown_sec=60)
                        interruptible_sleep(1)
                        continue

                    equity_current_usdt = float(get_available_usdt())
                    try:
                        account_state = account_builder.build(
                            symbol=market_data["symbol"],
                            current_equity_usdt=equity_current_usdt,
                            closed_trades=list(CLOSED_TRADES_CACHE),
                        )
                    except AccountStateNotReadyError as e:
                        msg = f"[SKIP][ACCOUNT_NOT_READY] {e}"
                        log(msg)
                        _maybe_send_entry_block_tg("ACCOUNT_NOT_READY", msg, cooldown_sec=60)
                        interruptible_sleep(1)
                        continue

                    if account_state.dd_pct >= 15.0:
                        msg = f"[SKIP][DD_BLOCK] dd_pct={account_state.dd_pct:.2f}>=15"
                        log(msg)
                        _maybe_send_entry_block_tg("DD_BLOCK", msg, cooldown_sec=60)
                        interruptible_sleep(1)
                        continue

                    if account_state.consecutive_losses >= 3:
                        msg = f"[SKIP][CONSEC_BLOCK] consecutive_losses={account_state.consecutive_losses}>=3"
                        log(msg)
                        _maybe_send_entry_block_tg("CONSEC_BLOCK", msg, cooldown_sec=60)
                        interruptible_sleep(1)
                        continue

                    signal_obj = entry_strategy.decide(market_data)
                    LAST_ENTRY_GPT_CALL_TS = now

                    # 전액 배분형 RiskPhysics 호출( base_risk_pct 제거 )
                    rp = risk_physics.decide(
                        regime_allocation=float(regime_decision.allocation),
                        dd_pct=float(account_state.dd_pct),
                        consecutive_losses=int(account_state.consecutive_losses),
                        tp_pct=float(signal_obj.tp_pct),
                        sl_pct=float(signal_obj.sl_pct),
                    )

                    if rp.action_override == "STOP":
                        SAFE_STOP_REQUESTED = True
                        _safe_send_tg(
                            "⛔ RiskPhysics STOP: DD 임계치 초과로 신규 진입을 중단합니다.\n"
                            f"- dd_pct={account_state.dd_pct:.2f}\n"
                            "포지션이 남아 있으면 정리 후 종료합니다."
                        )
                        interruptible_sleep(1)
                        continue

                    if rp.action_override == "SKIP":
                        msg = f"[SKIP][RISK_PHYSICS] {rp.reason}"
                        log(msg)
                        _maybe_send_entry_block_tg("RISK_PHYSICS_SKIP", msg, cooldown_sec=60)
                        interruptible_sleep(1)
                        continue

                    meta2 = dict(signal_obj.meta or {})
                    meta2.update(
                        {
                            "regime_score": float(regime_decision.score),
                            "regime_band": str(regime_decision.band),
                            "regime_allocation": float(regime_decision.allocation),
                            "dd_pct": float(account_state.dd_pct),
                            "consecutive_losses": int(account_state.consecutive_losses),
                            "risk_physics_reason": str(rp.reason),
                            "dynamic_risk_pct": float(rp.effective_risk_pct),
                        }
                    )

                    signal_final = Signal(
                        action="ENTER",
                        direction=signal_obj.direction,
                        tp_pct=float(signal_obj.tp_pct),
                        sl_pct=float(signal_obj.sl_pct),
                        risk_pct=float(rp.effective_risk_pct),  # 전액 배분형: 1.0=전액
                        reason=str(signal_obj.reason),
                        guard_adjustments=dict(signal_obj.guard_adjustments or {}),
                        meta=meta2,
                    )

                    trade = entry_exec_engine.execute(signal_final)
                    if trade:
                        OPEN_TRADES.append(trade)
                        LAST_STATUS_TG_TS = time.time()

            interruptible_sleep(1)

        except Exception as e:
            tb = traceback.format_exc()
            log(f"ERROR: {e}\n{tb}")

            core = f"{type(e).__name__}:{str(e)[:200]}"
            key = hashlib.sha1(core.encode("utf-8", errors="ignore")).hexdigest()[:12]
            _maybe_send_error_tg(
                key,
                "❌ 예기치 못한 오류가 발생했습니다.\n"
                f"- 내용: {e}\n"
                "2초 후 자동으로 다시 시도합니다.",
                cooldown_sec=60,
            )

            log_signal(
                event="ERROR",
                symbol=SET.symbol,
                strategy_type="UNKNOWN",
                reason=str(e),
            )
            interruptible_sleep(2)

    return


if __name__ == "__main__":
    main()