# infra/data_health_monitor.py
"""
=====================================================
FILE: infra/data_health_monitor.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
=====================================================

역할
-----------------------------------------------------
- WS 캔들/오더북 상태 + market_features_ws.build_entry_features_ws() 로
  실제 GPT가 사용하는 핵심 지표가 모두 정상 생성되는지 검사한다.
- 하나라도 이상(누락/None/NaN/지연/버퍼부족)이면 HEALTH_OK=False.
- run_bot_ws / entry_flow / gpt_trader 가 이 flag를 참고해
  GPT ENTRY 를 자동 차단하게 한다.
- 10분마다 텔레그램으로 완전한 상태 리포트를 전송한다.

사용
-----------------------------------------------------
run_bot_ws.main() 부트 이후:
      from infra.data_health_monitor import start_health_monitor
      start_health_monitor()

entry_flow.py 상단에서:
      from infra.data_health_monitor import HEALTH_OK
      if not HEALTH_OK: SKIP

핵심 원칙 (STRICT · NO-FALLBACK)
-----------------------------------------------------
- 헬스 모니터 내부 오류도 "정상"으로 취급하지 않는다.
- 내부 예외 발생 시 HEALTH_OK=False 로 즉시 전환한다.
- 최초 1회 동기 체크 없이 스레드만 띄우는 경쟁 조건 금지.
- 동일 프로세스 내 중복 monitor thread 실행 금지.
- settings.py 외 환경변수 직접 접근 금지.
- print() 금지 / logging(telelog) 사용.

변경 이력
-----------------------------------------------------
- 2026-03-08 (TRADE-GRADE PATCH)
  1) FIX(ROOT-CAUSE): health monitor 내부 예외 시 stale HEALTH_OK 유지 문제 수정
  2) FIX(BOOT): start_health_monitor() 최초 1회 동기 체크 추가
  3) FIX(CONCURRENCY): 중복 health monitor thread 시작 방지
  4) FIX(VISIBILITY): WS 실패 사유를 overall_reasons 기반으로 상세 기록
=====================================================
"""

from __future__ import annotations

import math
import threading
import time
import traceback
from typing import Any, Dict, Tuple

from infra.telelog import log, send_tg
from infra.market_data_ws import get_health_snapshot
from infra.market_features_ws import build_entry_features_ws, FeatureBuildError
from settings import load_settings


# -----------------------------------------------------
# 전역 상태
# -----------------------------------------------------
HEALTH_OK: bool = True
LAST_FAIL_REASON: str = ""
LAST_FULL_REPORT_TS: float = 0.0

# 보고 주기 (초) — 10분
REPORT_INTERVAL_SEC: int = 600

# 내부 검사 주기 (초)
CHECK_INTERVAL_SEC: int = 5

# 단일 인스턴스 보장
_STATE_LOCK = threading.RLock()
_MONITOR_THREAD: threading.Thread | None = None
_MONITOR_STARTED: bool = False


# -----------------------------------------------------
# 내부 상태 유틸
# -----------------------------------------------------
def _set_health_state(ok: bool, reason: str) -> None:
    global HEALTH_OK, LAST_FAIL_REASON
    with _STATE_LOCK:
        HEALTH_OK = bool(ok)
        LAST_FAIL_REASON = str(reason or "").strip()


def _get_health_state() -> Tuple[bool, str]:
    with _STATE_LOCK:
        return bool(HEALTH_OK), str(LAST_FAIL_REASON)


def _should_send_full_report(now_ts: float) -> bool:
    global LAST_FULL_REPORT_TS
    with _STATE_LOCK:
        if (now_ts - LAST_FULL_REPORT_TS) < REPORT_INTERVAL_SEC:
            return False
        LAST_FULL_REPORT_TS = now_ts
        return True


def _format_ws_fail_reason(snapshot: Dict[str, Any]) -> str:
    if not isinstance(snapshot, dict):
        raise RuntimeError("health snapshot must be dict (STRICT)")

    reasons = snapshot.get("overall_reasons")
    if not isinstance(reasons, list):
        raise RuntimeError("health snapshot overall_reasons must be list (STRICT)")

    cleaned = [str(x).strip() for x in reasons if str(x).strip()]
    if not cleaned:
        raise RuntimeError("health snapshot failed but overall_reasons empty (STRICT)")

    return "WS 데이터 헬스 체크 실패: " + " | ".join(cleaned)


def _is_valid_number(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(float(v))


# -----------------------------------------------------
# 핵심 헬스 체크 함수
# -----------------------------------------------------
def _check_ws_health(symbol: str) -> Tuple[bool, str]:
    """
    WS 필수 타임프레임 버퍼 + 오더북 상태를 검사한다.
    get_health_snapshot()가 strict required intervals 기준으로 검사한다.
    """
    snap = get_health_snapshot(symbol)
    if not isinstance(snap, dict):
        raise RuntimeError("get_health_snapshot returned non-dict (STRICT)")

    overall_ok = snap.get("overall_ok")
    if not isinstance(overall_ok, bool):
        raise RuntimeError("health snapshot overall_ok must be bool (STRICT)")

    if not overall_ok:
        return False, _format_ws_fail_reason(snap)

    return True, ""


def _check_feature_health(symbol: str) -> Tuple[bool, str]:
    """
    build_entry_features_ws() 기반으로 GPT가 실제로 사용하는
    모든 핵심 지표가 정상 생성되는지 검사한다.

    이 함수가 True 를 반환해야 GPT ENTRY/EXIT 가 정상적으로 동작한다고 볼 수 있다.
    """
    try:
        feats = build_entry_features_ws(symbol)
    except FeatureBuildError as e:
        return False, f"피처 생성 실패: {e}"
    except Exception as e:
        return False, f"예상치 못한 피처 생성 오류: {type(e).__name__}: {e}"

    if not isinstance(feats, dict):
        return False, "features dict 구조가 비정상"

    tfs = feats.get("timeframes")
    if not isinstance(tfs, dict):
        return False, "features.timeframes 구조가 비정상"

    tf5 = tfs.get("5m")
    tf15 = tfs.get("15m")

    if not isinstance(tf5, dict) or not isinstance(tf15, dict):
        return False, "필수 타임프레임(5m/15m) 피처 없음"

    regime = tf5.get("regime")
    if not isinstance(regime, dict):
        return False, "5m.regime 구조가 비정상"

    trend = regime.get("trend_strength")
    vol = tf5.get("atr_pct")
    vz = tf5.get("volume_zscore")

    if not _is_valid_number(trend):
        return False, "trend_strength 비정상"

    if not _is_valid_number(vol):
        return False, "volatility(atr_pct) 비정상"

    if not _is_valid_number(vz):
        return False, "volume_zscore 비정상"

    atr_fast = tf5.get("atr")
    atr_slow = tf15.get("atr")

    if not _is_valid_number(atr_fast):
        return False, "ATR_fast 비정상"

    if not _is_valid_number(atr_slow):
        return False, "ATR_slow 비정상"

    return True, ""


# -----------------------------------------------------
# 헬스 검사 → 전역 플래그 갱신
# -----------------------------------------------------
def _update_health(symbol: str) -> None:
    ok_ws, reason_ws = _check_ws_health(symbol)
    if not ok_ws:
        _set_health_state(False, reason_ws)
        return

    ok_feat, reason_feat = _check_feature_health(symbol)
    if not ok_feat:
        _set_health_state(False, reason_feat)
        return

    _set_health_state(True, "")


# -----------------------------------------------------
# 텔레그램 보고
# -----------------------------------------------------
def _send_full_report(symbol: str) -> None:
    """
    10분마다 전체 상태 보고.
    """
    now = time.time()
    if not _should_send_full_report(now):
        return

    ok, fail_reason = _get_health_state()

    if ok:
        msg = (
            f"✅ [DATA HEALTH OK]\n"
            f"- symbol: {symbol}\n"
            f"- WS KLINE/ORDERBOOK 정상\n"
            f"- 지표/패턴/ATR/트렌드 정상\n"
            f"- GPT 사용 가능"
        )
    else:
        msg = (
            f"❌ [DATA HEALTH FAIL]\n"
            f"- symbol: {symbol}\n"
            f"- 이유: {fail_reason}\n"
            f"- GPT ENTRY 자동 차단됨"
        )

    try:
        send_tg(msg)
    except Exception as e:
        log(f"[DATA HEALTH MONITOR] TG send error: {type(e).__name__}: {e}")


# -----------------------------------------------------
# 메인 루프 (백그라운드 스레드)
# -----------------------------------------------------
def _health_loop(symbol: str) -> None:
    log(f"[DATA HEALTH MONITOR] started: symbol={symbol}")

    while True:
        try:
            _update_health(symbol)
            _send_full_report(symbol)
        except Exception as e:
            tb = traceback.format_exc()
            reason = f"health monitor internal error: {type(e).__name__}: {e}"
            _set_health_state(False, reason)
            log(f"[DATA HEALTH MONITOR][FATAL] {reason}\n{tb}")

        time.sleep(CHECK_INTERVAL_SEC)


# -----------------------------------------------------
# 외부에서 호출하는 스타터
# -----------------------------------------------------
def start_health_monitor() -> None:
    """
    run_bot_ws.main() 부트 이후 실행:

        from infra.data_health_monitor import start_health_monitor
        start_health_monitor()
    """
    global _MONITOR_THREAD, _MONITOR_STARTED

    with _STATE_LOCK:
        if _MONITOR_STARTED:
            log("[DATA HEALTH MONITOR] already started -> skip duplicate launch")
            return

    SET = load_settings()
    symbol = str(getattr(SET, "symbol", "") or "").strip()
    if not symbol:
        raise RuntimeError("settings.symbol is required for data health monitor (STRICT)")

    # 최초 1회 동기 체크: 부팅 직후 HEALTH_OK stale true 상태 노출 방지
    try:
        _update_health(symbol)
    except Exception as e:
        tb = traceback.format_exc()
        reason = f"initial health check failed: {type(e).__name__}: {e}"
        _set_health_state(False, reason)
        log(f"[DATA HEALTH MONITOR][BOOT_FAIL] {reason}\n{tb}")
        raise RuntimeError(reason) from e

    th = threading.Thread(
        target=_health_loop,
        args=(symbol,),
        name="data-health-monitor",
        daemon=True,
    )

    with _STATE_LOCK:
        if _MONITOR_STARTED:
            log("[DATA HEALTH MONITOR] already started -> skip duplicate launch")
            return
        _MONITOR_THREAD = th
        _MONITOR_STARTED = True

    th.start()
    ok, fail_reason = _get_health_state()
    if ok:
        log("[DATA HEALTH MONITOR] thread launched with initial state=OK")
    else:
        log(f"[DATA HEALTH MONITOR] thread launched with initial state=FAIL reason={fail_reason}")


# -----------------------------------------------------
# 다른 모듈에서 HEALTH_OK flag만 바로 import해서 사용:
#   from infra.data_health_monitor import HEALTH_OK
# -----------------------------------------------------
__all__ = ["HEALTH_OK", "LAST_FAIL_REASON", "start_health_monitor"]