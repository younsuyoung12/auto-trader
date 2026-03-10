# infra/data_health_monitor.py
"""
=====================================================
FILE: infra/data_health_monitor.py
ROLE:
- WS 캔들/오더북 상태 + entry feature 생성 상태를 종합 점검한다
- run_bot_ws / entry_flow / GPT 진입 차단 계층에 실시간 health 상태를 제공한다
- 텔레그램으로 정기 상태 리포트를 비동기 전송한다

CORE RESPONSIBILITIES:
- WS health snapshot(strict) 검사
- build_entry_features_ws() 기반 feature readiness 검사
- HEALTH_OK / LAST_FAIL_REASON / LAST_HEALTH_SNAPSHOT 상태 관리
- 최초 1회 동기 체크 + 단일 monitor thread 보장
- 상태 전환 시점 / 마지막 성공·실패 시점 추적

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- 내부 예외도 정상으로 취급하지 않는다
- 내부 예외 발생 시 즉시 HEALTH FAIL 전환
- 최초 1회 동기 체크 없이 thread만 띄우는 경쟁 조건 금지
- 동일 프로세스 내 중복 monitor thread 실행 금지
- settings.py 외 환경변수 직접 접근 금지
- print() 금지 / logging(telelog) 사용
- direct import(`from ... import HEALTH_OK`) stale 문제를 막기 위해 proxy 객체를 사용한다

CHANGE HISTORY:
- 2026-03-10:
  1) FIX(ROOT-CAUSE): direct import 된 HEALTH_OK stale 문제를 proxy 기반 상태 노출로 해결
  2) ADD(OBSERVABILITY): LAST_HEALTH_SNAPSHOT / 마지막 상태 전환/성공/실패 시각 추적 추가
  3) FIX(ARCH): WS health snapshot 구조(ws/orderbook/klines)와 feature health를 함께 저장
  4) FIX(NONBLOCKING): 텔레그램 정기 리포트를 백그라운드 전송으로 분리
  5) FIX(STRICT): feature health가 실제 엔진 필수 TF(1m/5m/15m/1h/4h) 계약을 검사하도록 강화
- 2026-03-08:
  1) FIX(ROOT-CAUSE): health monitor 내부 예외 시 stale HEALTH_OK 유지 문제 수정
  2) FIX(BOOT): start_health_monitor() 최초 1회 동기 체크 추가
  3) FIX(CONCURRENCY): 중복 health monitor thread 시작 방지
  4) FIX(VISIBILITY): WS 실패 사유를 overall_reasons 기반으로 상세 기록
=====================================================
"""

from __future__ import annotations

import copy
import math
import threading
import time
import traceback
from typing import Any, Dict, Optional, Tuple

from infra.telelog import log, send_tg
from infra.market_data_ws import get_health_snapshot
from infra.market_features_ws import build_entry_features_ws, FeatureBuildError
from settings import load_settings


# -----------------------------------------------------
# 전역 설정 / 상수
# -----------------------------------------------------
SET = load_settings()

# 보고 주기 (초) — 10분
REPORT_INTERVAL_SEC: int = 600

# 내부 검사 주기 (초)
CHECK_INTERVAL_SEC: int = 5

# 실제 엔진 진입 계약과 맞춘 필수 TF
_REQUIRED_FEATURE_TFS: tuple[str, ...] = ("1m", "5m", "15m", "1h", "4h")


# -----------------------------------------------------
# 내부 raw 상태
# -----------------------------------------------------
_HEALTH_OK_RAW: bool = True
_LAST_FAIL_REASON_RAW: str = ""
_LAST_HEALTH_SNAPSHOT_RAW: Dict[str, Any] = {}
_LAST_CHECK_TS: float = 0.0
_LAST_SUCCESS_TS: float = 0.0
_LAST_FAIL_TS: float = 0.0
_LAST_STATE_CHANGE_TS: float = 0.0
_LAST_FULL_REPORT_TS: float = 0.0

# 단일 인스턴스 보장
_STATE_LOCK = threading.RLock()
_MONITOR_THREAD: threading.Thread | None = None
_MONITOR_STARTED: bool = False


# -----------------------------------------------------
# Proxy objects
# -----------------------------------------------------
class _HealthFlagProxy:
    """
    `from infra.data_health_monitor import HEALTH_OK` 로 direct import 해도
    bool(HEALTH_OK) 시점마다 최신 raw 상태를 읽도록 하는 proxy.
    """

    def __bool__(self) -> bool:
        with _STATE_LOCK:
            return bool(_HEALTH_OK_RAW)

    @property
    def value(self) -> bool:
        return bool(self)

    def __repr__(self) -> str:
        return f"_HealthFlagProxy(value={bool(self)})"

    def __str__(self) -> str:
        return "True" if bool(self) else "False"


class _ReasonProxy:
    """
    direct import 된 LAST_FAIL_REASON 이 stale string 으로 굳는 문제를 막기 위한 proxy.
    문자열 컨텍스트(f-string, str, bool, startswith 등)에서 현재 값을 읽는다.
    """

    def _current(self) -> str:
        with _STATE_LOCK:
            return str(_LAST_FAIL_REASON_RAW)

    @property
    def value(self) -> str:
        return self._current()

    def __str__(self) -> str:
        return self._current()

    def __repr__(self) -> str:
        return repr(self._current())

    def __bool__(self) -> bool:
        return bool(self._current())

    def __len__(self) -> int:
        return len(self._current())

    def __contains__(self, item: object) -> bool:
        return str(item) in self._current()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._current(), name)


# 외부 공개 상태 (proxy)
HEALTH_OK = _HealthFlagProxy()
LAST_FAIL_REASON = _ReasonProxy()


# -----------------------------------------------------
# 내부 상태 유틸
# -----------------------------------------------------
def _build_health_snapshot(
    *,
    symbol: str,
    ok: bool,
    fail_reason: str,
    ws_snapshot: Dict[str, Any],
    feature_status: Dict[str, Any],
    checked_at_ts: float,
) -> Dict[str, Any]:
    if not isinstance(ws_snapshot, dict):
        raise RuntimeError("ws_snapshot must be dict (STRICT)")
    if not isinstance(feature_status, dict):
        raise RuntimeError("feature_status must be dict (STRICT)")

    return {
        "symbol": str(symbol),
        "ok": bool(ok),
        "fail_reason": str(fail_reason or "").strip(),
        "checked_at_ts": float(checked_at_ts),
        "ws": copy.deepcopy(ws_snapshot),
        "feature": copy.deepcopy(feature_status),
    }


def _set_health_state(ok: bool, reason: str, snapshot: Dict[str, Any]) -> None:
    global _HEALTH_OK_RAW, _LAST_FAIL_REASON_RAW, _LAST_HEALTH_SNAPSHOT_RAW
    global _LAST_CHECK_TS, _LAST_SUCCESS_TS, _LAST_FAIL_TS, _LAST_STATE_CHANGE_TS

    if not isinstance(snapshot, dict):
        raise RuntimeError("health snapshot must be dict (STRICT)")

    now = time.time()
    normalized_ok = bool(ok)
    normalized_reason = str(reason or "").strip()

    with _STATE_LOCK:
        prev_ok = bool(_HEALTH_OK_RAW)

        _HEALTH_OK_RAW = normalized_ok
        _LAST_FAIL_REASON_RAW = normalized_reason
        _LAST_HEALTH_SNAPSHOT_RAW = copy.deepcopy(snapshot)
        _LAST_CHECK_TS = now

        if normalized_ok:
            _LAST_SUCCESS_TS = now
        else:
            _LAST_FAIL_TS = now

        if prev_ok != normalized_ok:
            _LAST_STATE_CHANGE_TS = now


def get_health_state() -> Tuple[bool, str]:
    with _STATE_LOCK:
        return bool(_HEALTH_OK_RAW), str(_LAST_FAIL_REASON_RAW)


def is_health_ok() -> bool:
    ok, _ = get_health_state()
    return bool(ok)


def get_last_health_snapshot() -> Dict[str, Any]:
    with _STATE_LOCK:
        return copy.deepcopy(_LAST_HEALTH_SNAPSHOT_RAW)


def get_health_runtime_meta() -> Dict[str, Any]:
    with _STATE_LOCK:
        return {
            "last_check_ts": float(_LAST_CHECK_TS),
            "last_success_ts": float(_LAST_SUCCESS_TS),
            "last_fail_ts": float(_LAST_FAIL_TS),
            "last_state_change_ts": float(_LAST_STATE_CHANGE_TS),
            "monitor_started": bool(_MONITOR_STARTED),
            "monitor_thread_alive": bool(_MONITOR_THREAD.is_alive()) if _MONITOR_THREAD is not None else False,
        }


def _should_send_full_report(now_ts: float) -> bool:
    global _LAST_FULL_REPORT_TS
    with _STATE_LOCK:
        if (now_ts - _LAST_FULL_REPORT_TS) < REPORT_INTERVAL_SEC:
            return False
        _LAST_FULL_REPORT_TS = now_ts
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
def _check_ws_health(symbol: str) -> Tuple[bool, str, Dict[str, Any]]:
    """
    WS 필수 타임프레임 버퍼 + 오더북 + transport 상태를 strict snapshot 기준으로 검사한다.
    """
    snap = get_health_snapshot(symbol)
    if not isinstance(snap, dict):
        raise RuntimeError("get_health_snapshot returned non-dict (STRICT)")

    overall_ok = snap.get("overall_ok")
    if not isinstance(overall_ok, bool):
        raise RuntimeError("health snapshot overall_ok must be bool (STRICT)")

    if not overall_ok:
        return False, _format_ws_fail_reason(snap), snap

    return True, "", snap


def _check_feature_health(symbol: str) -> Tuple[bool, str, Dict[str, Any]]:
    """
    build_entry_features_ws() 기반으로 GPT/ENTRY 엔진이 실제 사용하는
    핵심 피처가 정상 생성되는지 검사한다.
    """
    try:
        feats = build_entry_features_ws(symbol)
    except FeatureBuildError as e:
        return False, f"피처 생성 실패: {e}", {"ok": False, "error_type": "FeatureBuildError"}
    except Exception as e:
        return (
            False,
            f"예상치 못한 피처 생성 오류: {type(e).__name__}: {e}",
            {"ok": False, "error_type": type(e).__name__},
        )

    if not isinstance(feats, dict):
        return False, "features dict 구조가 비정상", {"ok": False, "error_type": "InvalidFeaturesRoot"}

    tfs = feats.get("timeframes")
    if not isinstance(tfs, dict):
        return False, "features.timeframes 구조가 비정상", {"ok": False, "error_type": "InvalidTimeframes"}

    missing_tfs = [tf for tf in _REQUIRED_FEATURE_TFS if not isinstance(tfs.get(tf), dict)]
    if missing_tfs:
        return (
            False,
            f"필수 타임프레임 피처 없음: {missing_tfs}",
            {"ok": False, "missing_tfs": missing_tfs},
        )

    tf5 = tfs["5m"]
    tf15 = tfs["15m"]

    regime = tf5.get("regime")
    if not isinstance(regime, dict):
        return False, "5m.regime 구조가 비정상", {"ok": False, "error_type": "InvalidRegime"}

    trend = regime.get("trend_strength")
    vol = tf5.get("atr_pct")
    vz = tf5.get("volume_zscore")
    atr_fast = tf5.get("atr")
    atr_slow = tf15.get("atr")

    if not _is_valid_number(trend):
        return False, "trend_strength 비정상", {"ok": False, "field": "5m.regime.trend_strength"}
    if not _is_valid_number(vol):
        return False, "volatility(atr_pct) 비정상", {"ok": False, "field": "5m.atr_pct"}
    if not _is_valid_number(vz):
        return False, "volume_zscore 비정상", {"ok": False, "field": "5m.volume_zscore"}
    if not _is_valid_number(atr_fast):
        return False, "ATR_fast 비정상", {"ok": False, "field": "5m.atr"}
    if not _is_valid_number(atr_slow):
        return False, "ATR_slow 비정상", {"ok": False, "field": "15m.atr"}

    details = {
        "ok": True,
        "required_tfs": list(_REQUIRED_FEATURE_TFS),
        "trend_strength": float(trend),
        "atr_pct": float(vol),
        "volume_zscore": float(vz),
        "atr_fast": float(atr_fast),
        "atr_slow": float(atr_slow),
    }
    return True, "", details


# -----------------------------------------------------
# 헬스 검사 → 전역 상태 갱신
# -----------------------------------------------------
def _update_health(symbol: str) -> None:
    now_ts = time.time()

    ok_ws, reason_ws, ws_snapshot = _check_ws_health(symbol)
    if not ok_ws:
        snapshot = _build_health_snapshot(
            symbol=symbol,
            ok=False,
            fail_reason=reason_ws,
            ws_snapshot=ws_snapshot,
            feature_status={"ok": False, "skipped": True, "reason": "ws_health_fail"},
            checked_at_ts=now_ts,
        )
        _set_health_state(False, reason_ws, snapshot)
        return

    ok_feat, reason_feat, feature_status = _check_feature_health(symbol)
    if not ok_feat:
        snapshot = _build_health_snapshot(
            symbol=symbol,
            ok=False,
            fail_reason=reason_feat,
            ws_snapshot=ws_snapshot,
            feature_status=feature_status,
            checked_at_ts=now_ts,
        )
        _set_health_state(False, reason_feat, snapshot)
        return

    snapshot = _build_health_snapshot(
        symbol=symbol,
        ok=True,
        fail_reason="",
        ws_snapshot=ws_snapshot,
        feature_status=feature_status,
        checked_at_ts=now_ts,
    )
    _set_health_state(True, "", snapshot)


# -----------------------------------------------------
# 텔레그램 보고
# -----------------------------------------------------
def _send_tg_in_background(msg: str) -> None:
    def _worker() -> None:
        try:
            send_tg(msg)
        except Exception as e:
            log(f"[DATA HEALTH MONITOR] TG send error: {type(e).__name__}: {e}")

    threading.Thread(target=_worker, name="data-health-tg-report", daemon=True).start()


def _send_full_report(symbol: str) -> None:
    """
    10분마다 전체 상태 보고.
    """
    now = time.time()
    if not _should_send_full_report(now):
        return

    ok, fail_reason = get_health_state()
    snapshot = get_last_health_snapshot()
    runtime_meta = get_health_runtime_meta()

    if ok:
        msg = (
            f"✅ [DATA HEALTH OK]\n"
            f"- symbol: {symbol}\n"
            f"- WS/ORDERBOOK 정상\n"
            f"- feature 생성 정상\n"
            f"- GPT 사용 가능\n"
            f"- last_check_ts: {runtime_meta['last_check_ts']:.0f}"
        )
    else:
        msg = (
            f"❌ [DATA HEALTH FAIL]\n"
            f"- symbol: {symbol}\n"
            f"- 이유: {fail_reason}\n"
            f"- last_check_ts: {runtime_meta['last_check_ts']:.0f}\n"
            f"- GPT ENTRY 자동 차단됨"
        )

        ws = snapshot.get("ws") if isinstance(snapshot, dict) else None
        if isinstance(ws, dict):
            reasons = ws.get("overall_reasons")
            if isinstance(reasons, list) and reasons:
                msg += "\n- ws_reasons: " + " | ".join(str(x) for x in reasons[:5])

    _send_tg_in_background(msg)


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

            snapshot = {
                "symbol": str(symbol),
                "ok": False,
                "fail_reason": reason,
                "checked_at_ts": time.time(),
                "ws": {},
                "feature": {"ok": False, "skipped": True, "reason": "monitor_internal_error"},
            }
            _set_health_state(False, reason, snapshot)
            log(f"[DATA HEALTH MONITOR][FATAL] {reason}\n{tb}")

        time.sleep(CHECK_INTERVAL_SEC)


# -----------------------------------------------------
# 외부에서 호출하는 스타터
# -----------------------------------------------------
def start_health_monitor() -> None:
    """
    run_bot_ws.main() 부트 이후 실행.

    direct import stale 문제를 막기 위해 HEALTH_OK / LAST_FAIL_REASON 는
    proxy 객체로 공개된다. 그래도 가장 바람직한 사용은 아래 중 하나다.

        import infra.data_health_monitor as dhm
        if not bool(dhm.HEALTH_OK): ...

    또는

        ok, reason = get_health_state()
        if not ok: ...
    """
    global _MONITOR_THREAD, _MONITOR_STARTED

    with _STATE_LOCK:
        if _MONITOR_STARTED:
            log("[DATA HEALTH MONITOR] already started -> skip duplicate launch")
            return

    symbol = str(getattr(SET, "symbol", "") or "").strip()
    if not symbol:
        raise RuntimeError("settings.symbol is required for data health monitor (STRICT)")

    # 최초 1회 동기 체크: 부팅 직후 stale true 노출 방지
    try:
        _update_health(symbol)
    except Exception as e:
        tb = traceback.format_exc()
        reason = f"initial health check failed: {type(e).__name__}: {e}"
        snapshot = {
            "symbol": str(symbol),
            "ok": False,
            "fail_reason": reason,
            "checked_at_ts": time.time(),
            "ws": {},
            "feature": {"ok": False, "skipped": True, "reason": "initial_health_check_failed"},
        }
        _set_health_state(False, reason, snapshot)
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

    ok, fail_reason = get_health_state()
    if ok:
        log("[DATA HEALTH MONITOR] thread launched with initial state=OK")
    else:
        log(f"[DATA HEALTH MONITOR] thread launched with initial state=FAIL reason={fail_reason}")


__all__ = [
    "HEALTH_OK",
    "LAST_FAIL_REASON",
    "start_health_monitor",
    "get_health_state",
    "is_health_ok",
    "get_last_health_snapshot",
    "get_health_runtime_meta",
]