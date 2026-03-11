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
- HEALTH LEVEL(OK/WARNING/FAIL) / LAST_FAIL_REASON / LAST_WARNING_REASON / LAST_HEALTH_SNAPSHOT 상태 관리
- 최초 1회 동기 체크 + 단일 monitor thread 보장
- 상태 전환 시점 / 마지막 성공·경고·실패 시점 추적

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- 내부 예외도 정상으로 취급하지 않는다
- 내부 예외 발생 시 즉시 HEALTH FAIL 전환
- 최초 1회 동기 체크 없이 thread만 띄우는 경쟁 조건 금지
- 동일 프로세스 내 중복 monitor thread 실행 금지
- settings.py 외 환경변수 직접 접근 금지
- print() 금지 / logging(telelog) 사용
- direct import 된 stale 상태 문제를 막기 위해 proxy 객체를 사용한다
- WARNING 과 FAIL 을 분리한다
- WARNING 은 관측성 상태이며 FAIL 과 동일하게 엔진 차단으로 취급하지 않는다
- WS snapshot 계약 불일치는 즉시 FAIL 이다

CHANGE HISTORY:
- 2026-03-11:
  1) FIX(ROOT-CAUSE): WS snapshot transport flag extractor 추가
     - overall_transport_ok / transport_ok / ws.transport_ok 허용 위치를 명시적으로 검사
     - 복수 위치 존재 시 값 불일치면 즉시 예외 처리
  2) FIX(SSOT): 필수 feature TF를 settings.features_required_tfs 에서 직접 로드
  3) FIX(SSOT): full report 주기를 settings.data_health_notify_sec 와 정합화
  4) FIX(STRICT): overall_ok=True 이면서 transport_ok=False 인 snapshot 불일치 즉시 예외 처리
  5) FIX(STRICT): health snapshot 핵심 계약(ws dict / ws.ok / overall_reasons / overall_warnings) 검증 강화
- 2026-03-10:
  1) FIX(ROOT-CAUSE): WS health를 OK/WARNING/FAIL 3단계 상태로 분리
  2) FIX(ROOT-CAUSE): market_data_ws warning 정보를 버리지 않고 LAST_WARNING_REASON / HEALTH_WARNING 으로 노출
  3) FIX(ARCH): HEALTH_OK 는 FAIL 에서만 False 가 되도록 정리 (warning 시 stale false 방지)
  4) FIX(STRICT): market_data_ws snapshot 계약(has_warning / overall_warnings / transport_ok) 불일치 시 즉시 예외
  5) FIX(OBSERVABILITY): 마지막 warning 시각 / 상태 레벨 / warning snapshot 노출 추가
  6) FIX(CONCURRENCY): monitor thread start 실패 시 started 상태 rollback 추가
  7) FIX(SSOT): load_settings() 재호출 제거, settings.SETTINGS 단일 객체 사용
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
from typing import Any, Dict, Literal, Tuple

from infra.market_data_ws import get_health_snapshot
from infra.market_features_ws import FeatureBuildError, build_entry_features_ws
from infra.telelog import log, send_tg
from settings import SETTINGS


# -----------------------------------------------------
# 전역 설정 / 상수
# -----------------------------------------------------
SET = SETTINGS

# 보고 주기 (초) — settings SSOT
REPORT_INTERVAL_SEC: int = int(SET.data_health_notify_sec)

# 내부 검사 주기 (초)
CHECK_INTERVAL_SEC: int = 5

# 실제 엔진 진입 계약과 맞춘 필수 TF (settings SSOT)
def _resolve_required_feature_tfs_from_settings() -> tuple[str, ...]:
    raw = getattr(SET, "features_required_tfs", None)
    if not isinstance(raw, list) or not raw:
        raise RuntimeError("settings.features_required_tfs must be non-empty list[str] (STRICT)")

    cleaned: list[str] = []
    seen: set[str] = set()
    for idx, item in enumerate(raw):
        tf = str(item).strip()
        if not tf:
            raise RuntimeError(f"settings.features_required_tfs[{idx}] is empty (STRICT)")
        if tf in seen:
            raise RuntimeError(f"settings.features_required_tfs contains duplicate tf={tf!r} (STRICT)")
        seen.add(tf)
        cleaned.append(tf)

    return tuple(cleaned)


_REQUIRED_FEATURE_TFS: tuple[str, ...] = _resolve_required_feature_tfs_from_settings()

if REPORT_INTERVAL_SEC <= 0:
    raise RuntimeError("settings.data_health_notify_sec must be > 0 (STRICT)")
if CHECK_INTERVAL_SEC <= 0:
    raise RuntimeError("CHECK_INTERVAL_SEC must be > 0 (STRICT)")

HealthLevel = Literal["OK", "WARNING", "FAIL"]


# -----------------------------------------------------
# 내부 raw 상태
# -----------------------------------------------------
_HEALTH_LEVEL_RAW: HealthLevel = "OK"
_HEALTH_OK_RAW: bool = True
_LAST_FAIL_REASON_RAW: str = ""
_LAST_WARNING_REASON_RAW: str = ""
_LAST_HEALTH_SNAPSHOT_RAW: Dict[str, Any] = {}
_LAST_CHECK_TS: float = 0.0
_LAST_SUCCESS_TS: float = 0.0
_LAST_WARNING_TS: float = 0.0
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

    정책:
    - HEALTH_OK 는 "FAIL 이 아닌 상태"를 의미한다.
    - WARNING 은 health degraded 이지만 FAIL 과 동일하게 False 로 취급하지 않는다.
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


class _HealthWarningProxy:
    """
    `from infra.data_health_monitor import HEALTH_WARNING` direct import 용 proxy.
    """

    def __bool__(self) -> bool:
        with _STATE_LOCK:
            return _HEALTH_LEVEL_RAW == "WARNING"

    @property
    def value(self) -> bool:
        return bool(self)

    def __repr__(self) -> str:
        return f"_HealthWarningProxy(value={bool(self)})"

    def __str__(self) -> str:
        return "True" if bool(self) else "False"


class _HealthLevelProxy:
    """
    최신 health level 문자열을 direct import stale 없이 노출한다.
    """

    def _current(self) -> str:
        with _STATE_LOCK:
            return str(_HEALTH_LEVEL_RAW)

    @property
    def value(self) -> str:
        return self._current()

    def __str__(self) -> str:
        return self._current()

    def __repr__(self) -> str:
        return repr(self._current())

    def __bool__(self) -> bool:
        return self._current() != "FAIL"

    def __len__(self) -> int:
        return len(self._current())

    def __contains__(self, item: object) -> bool:
        return str(item) in self._current()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._current(), name)


class _ReasonProxy:
    """
    direct import 된 reason string stale 문제를 막기 위한 proxy.
    문자열 컨텍스트(f-string, str, bool, startswith 등)에서 현재 값을 읽는다.
    """

    def __init__(self, kind: Literal["fail", "warning"]) -> None:
        self._kind = kind

    def _current(self) -> str:
        with _STATE_LOCK:
            if self._kind == "fail":
                return str(_LAST_FAIL_REASON_RAW)
            return str(_LAST_WARNING_REASON_RAW)

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
HEALTH_WARNING = _HealthWarningProxy()
HEALTH_LEVEL = _HealthLevelProxy()
LAST_FAIL_REASON = _ReasonProxy("fail")
LAST_WARNING_REASON = _ReasonProxy("warning")


# -----------------------------------------------------
# 내부 상태 유틸
# -----------------------------------------------------
def _normalize_health_level(level: Any) -> HealthLevel:
    s = str(level or "").strip().upper()
    if s not in ("OK", "WARNING", "FAIL"):
        raise RuntimeError(f"invalid health level (STRICT): {level!r}")
    return s  # type: ignore[return-value]


def _build_health_snapshot(
    *,
    symbol: str,
    level: HealthLevel,
    fail_reason: str,
    warning_reason: str,
    ws_snapshot: Dict[str, Any],
    feature_status: Dict[str, Any],
    checked_at_ts: float,
) -> Dict[str, Any]:
    if not isinstance(ws_snapshot, dict):
        raise RuntimeError("ws_snapshot must be dict (STRICT)")
    if not isinstance(feature_status, dict):
        raise RuntimeError("feature_status must be dict (STRICT)")

    normalized_level = _normalize_health_level(level)
    normalized_fail_reason = str(fail_reason or "").strip()
    normalized_warning_reason = str(warning_reason or "").strip()

    if normalized_level == "OK":
        if normalized_fail_reason:
            raise RuntimeError("OK snapshot must not contain fail_reason (STRICT)")
        if normalized_warning_reason:
            raise RuntimeError("OK snapshot must not contain warning_reason (STRICT)")
    elif normalized_level == "WARNING":
        if normalized_fail_reason:
            raise RuntimeError("WARNING snapshot must not contain fail_reason (STRICT)")
        if not normalized_warning_reason:
            raise RuntimeError("WARNING snapshot requires warning_reason (STRICT)")
    else:
        if not normalized_fail_reason:
            raise RuntimeError("FAIL snapshot requires fail_reason (STRICT)")

    return {
        "symbol": str(symbol),
        "level": normalized_level,
        "ok": normalized_level != "FAIL",
        "has_warning": normalized_level == "WARNING",
        "fail_reason": normalized_fail_reason,
        "warning_reason": normalized_warning_reason,
        "checked_at_ts": float(checked_at_ts),
        "ws": copy.deepcopy(ws_snapshot),
        "feature": copy.deepcopy(feature_status),
    }


def _set_health_state(level: HealthLevel, fail_reason: str, warning_reason: str, snapshot: Dict[str, Any]) -> None:
    global _HEALTH_LEVEL_RAW, _HEALTH_OK_RAW, _LAST_FAIL_REASON_RAW, _LAST_WARNING_REASON_RAW
    global _LAST_HEALTH_SNAPSHOT_RAW
    global _LAST_CHECK_TS, _LAST_SUCCESS_TS, _LAST_WARNING_TS, _LAST_FAIL_TS, _LAST_STATE_CHANGE_TS

    if not isinstance(snapshot, dict):
        raise RuntimeError("health snapshot must be dict (STRICT)")

    normalized_level = _normalize_health_level(level)
    normalized_fail_reason = str(fail_reason or "").strip()
    normalized_warning_reason = str(warning_reason or "").strip()

    if normalized_level == "OK":
        if normalized_fail_reason:
            raise RuntimeError("OK state must not have fail_reason (STRICT)")
        if normalized_warning_reason:
            raise RuntimeError("OK state must not have warning_reason (STRICT)")
    elif normalized_level == "WARNING":
        if normalized_fail_reason:
            raise RuntimeError("WARNING state must not have fail_reason (STRICT)")
        if not normalized_warning_reason:
            raise RuntimeError("WARNING state requires warning_reason (STRICT)")
    else:
        if not normalized_fail_reason:
            raise RuntimeError("FAIL state requires fail_reason (STRICT)")

    now = time.time()

    with _STATE_LOCK:
        prev_level = _HEALTH_LEVEL_RAW

        _HEALTH_LEVEL_RAW = normalized_level
        _HEALTH_OK_RAW = normalized_level != "FAIL"
        _LAST_FAIL_REASON_RAW = normalized_fail_reason if normalized_level == "FAIL" else ""
        _LAST_WARNING_REASON_RAW = normalized_warning_reason if normalized_level == "WARNING" else ""
        _LAST_HEALTH_SNAPSHOT_RAW = copy.deepcopy(snapshot)
        _LAST_CHECK_TS = now

        if normalized_level == "OK":
            _LAST_SUCCESS_TS = now
        elif normalized_level == "WARNING":
            _LAST_WARNING_TS = now
        else:
            _LAST_FAIL_TS = now

        if prev_level != normalized_level:
            _LAST_STATE_CHANGE_TS = now


def get_health_state() -> Tuple[bool, str]:
    """
    하위 호환용 API.
    - bool: FAIL 이 아니면 True
    - str : FAIL 일 때만 fail_reason
    """
    with _STATE_LOCK:
        return bool(_HEALTH_OK_RAW), str(_LAST_FAIL_REASON_RAW)


def get_health_level_state() -> Dict[str, Any]:
    with _STATE_LOCK:
        return {
            "level": str(_HEALTH_LEVEL_RAW),
            "ok": bool(_HEALTH_OK_RAW),
            "has_warning": _HEALTH_LEVEL_RAW == "WARNING",
            "fail_reason": str(_LAST_FAIL_REASON_RAW),
            "warning_reason": str(_LAST_WARNING_REASON_RAW),
        }


def is_health_ok() -> bool:
    ok, _ = get_health_state()
    return bool(ok)


def is_health_warning() -> bool:
    with _STATE_LOCK:
        return _HEALTH_LEVEL_RAW == "WARNING"


def get_last_health_snapshot() -> Dict[str, Any]:
    with _STATE_LOCK:
        return copy.deepcopy(_LAST_HEALTH_SNAPSHOT_RAW)


def get_health_runtime_meta() -> Dict[str, Any]:
    with _STATE_LOCK:
        return {
            "health_level": str(_HEALTH_LEVEL_RAW),
            "last_check_ts": float(_LAST_CHECK_TS),
            "last_success_ts": float(_LAST_SUCCESS_TS),
            "last_warning_ts": float(_LAST_WARNING_TS),
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


def _format_ws_warning_reason(snapshot: Dict[str, Any]) -> str:
    if not isinstance(snapshot, dict):
        raise RuntimeError("health snapshot must be dict (STRICT)")

    warnings = snapshot.get("overall_warnings")
    if not isinstance(warnings, list):
        raise RuntimeError("health snapshot overall_warnings must be list (STRICT)")

    cleaned = [str(x).strip() for x in warnings if str(x).strip()]
    if not cleaned:
        raise RuntimeError("health snapshot warning but overall_warnings empty (STRICT)")

    return "WS 데이터 경고: " + " | ".join(cleaned)


def _is_valid_number(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(float(v))


def _extract_transport_ok_strict(snapshot: Dict[str, Any]) -> bool:
    if not isinstance(snapshot, dict):
        raise RuntimeError("health snapshot must be dict (STRICT)")

    candidates: list[tuple[str, bool]] = []

    if "overall_transport_ok" in snapshot:
        raw = snapshot.get("overall_transport_ok")
        if not isinstance(raw, bool):
            raise RuntimeError("health snapshot overall_transport_ok must be bool (STRICT)")
        candidates.append(("overall_transport_ok", raw))

    if "transport_ok" in snapshot:
        raw = snapshot.get("transport_ok")
        if not isinstance(raw, bool):
            raise RuntimeError("health snapshot transport_ok must be bool (STRICT)")
        candidates.append(("transport_ok", raw))

    ws = snapshot.get("ws")
    if not isinstance(ws, dict):
        raise RuntimeError("health snapshot ws must be dict (STRICT)")

    if "transport_ok" in ws:
        raw = ws.get("transport_ok")
        if not isinstance(raw, bool):
            raise RuntimeError("health snapshot.ws.transport_ok must be bool (STRICT)")
        candidates.append(("ws.transport_ok", raw))

    if not candidates:
        raise RuntimeError("health snapshot transport_ok missing in approved locations (STRICT)")

    values = {value for _, value in candidates}
    if len(values) > 1:
        detail = ", ".join(f"{name}={value}" for name, value in candidates)
        raise RuntimeError(f"health snapshot transport_ok conflict (STRICT): {detail}")

    return candidates[0][1]


def _validate_ws_snapshot_contract_strict(snapshot: Dict[str, Any]) -> Tuple[bool, bool]:
    if not isinstance(snapshot, dict):
        raise RuntimeError("get_health_snapshot returned non-dict (STRICT)")

    overall_ok = snapshot.get("overall_ok")
    if not isinstance(overall_ok, bool):
        raise RuntimeError("health snapshot overall_ok must be bool (STRICT)")

    has_warning = snapshot.get("has_warning")
    if not isinstance(has_warning, bool):
        raise RuntimeError("health snapshot has_warning must be bool (STRICT)")

    overall_reasons = snapshot.get("overall_reasons")
    if not isinstance(overall_reasons, list):
        raise RuntimeError("health snapshot overall_reasons must be list (STRICT)")

    overall_warnings = snapshot.get("overall_warnings")
    if not isinstance(overall_warnings, list):
        raise RuntimeError("health snapshot overall_warnings must be list (STRICT)")

    ws = snapshot.get("ws")
    if not isinstance(ws, dict):
        raise RuntimeError("health snapshot ws must be dict (STRICT)")

    ws_ok = ws.get("ok")
    if not isinstance(ws_ok, bool):
        raise RuntimeError("health snapshot.ws.ok must be bool (STRICT)")

    transport_ok = _extract_transport_ok_strict(snapshot)

    if overall_ok and not transport_ok:
        raise RuntimeError("health snapshot inconsistent: overall_ok=True but transport_ok=False (STRICT)")

    if not overall_ok and not any(str(x).strip() for x in overall_reasons):
        raise RuntimeError("health snapshot FAIL without overall_reasons (STRICT)")

    if has_warning and not any(str(x).strip() for x in overall_warnings):
        raise RuntimeError("health snapshot WARNING without overall_warnings (STRICT)")

    return overall_ok, has_warning


# -----------------------------------------------------
# 핵심 헬스 체크 함수
# -----------------------------------------------------
def _check_ws_health(symbol: str) -> Tuple[HealthLevel, str, str, Dict[str, Any]]:
    """
    WS 필수 타임프레임 버퍼 + 오더북 + transport 상태를 strict snapshot 기준으로 검사한다.

    반환:
    - level: OK / WARNING / FAIL
    - fail_reason
    - warning_reason
    - raw snapshot
    """
    snap = get_health_snapshot(symbol)
    overall_ok, has_warning = _validate_ws_snapshot_contract_strict(snap)

    if not overall_ok:
        return "FAIL", _format_ws_fail_reason(snap), "", snap

    if has_warning:
        return "WARNING", "", _format_ws_warning_reason(snap), snap

    return "OK", "", "", snap


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

    tf5 = tfs.get("5m")
    tf15 = tfs.get("15m")

    if not isinstance(tf5, dict):
        return False, "5m feature 구조가 비정상", {"ok": False, "field": "timeframes.5m"}
    if not isinstance(tf15, dict):
        return False, "15m feature 구조가 비정상", {"ok": False, "field": "timeframes.15m"}

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

    ws_level, ws_fail_reason, ws_warning_reason, ws_snapshot = _check_ws_health(symbol)

    if ws_level == "FAIL":
        snapshot = _build_health_snapshot(
            symbol=symbol,
            level="FAIL",
            fail_reason=ws_fail_reason,
            warning_reason="",
            ws_snapshot=ws_snapshot,
            feature_status={"ok": False, "skipped": True, "reason": "ws_health_fail"},
            checked_at_ts=now_ts,
        )
        _set_health_state("FAIL", ws_fail_reason, "", snapshot)
        return

    ok_feat, reason_feat, feature_status = _check_feature_health(symbol)
    if not ok_feat:
        snapshot = _build_health_snapshot(
            symbol=symbol,
            level="FAIL",
            fail_reason=reason_feat,
            warning_reason="",
            ws_snapshot=ws_snapshot,
            feature_status=feature_status,
            checked_at_ts=now_ts,
        )
        _set_health_state("FAIL", reason_feat, "", snapshot)
        return

    if ws_level == "WARNING":
        snapshot = _build_health_snapshot(
            symbol=symbol,
            level="WARNING",
            fail_reason="",
            warning_reason=ws_warning_reason,
            ws_snapshot=ws_snapshot,
            feature_status=feature_status,
            checked_at_ts=now_ts,
        )
        _set_health_state("WARNING", "", ws_warning_reason, snapshot)
        return

    snapshot = _build_health_snapshot(
        symbol=symbol,
        level="OK",
        fail_reason="",
        warning_reason="",
        ws_snapshot=ws_snapshot,
        feature_status=feature_status,
        checked_at_ts=now_ts,
    )
    _set_health_state("OK", "", "", snapshot)


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
    주기마다 전체 상태 보고.
    """
    now = time.time()
    if not _should_send_full_report(now):
        return

    level_state = get_health_level_state()
    snapshot = get_last_health_snapshot()
    runtime_meta = get_health_runtime_meta()

    level = str(level_state["level"])
    fail_reason = str(level_state["fail_reason"])
    warning_reason = str(level_state["warning_reason"])

    if level == "OK":
        msg = (
            f"✅ [DATA HEALTH OK]\n"
            f"- symbol: {symbol}\n"
            f"- WS/ORDERBOOK 정상\n"
            f"- feature 생성 정상\n"
            f"- GPT 사용 가능\n"
            f"- last_check_ts: {runtime_meta['last_check_ts']:.0f}"
        )
    elif level == "WARNING":
        msg = (
            f"⚠️ [DATA HEALTH WARNING]\n"
            f"- symbol: {symbol}\n"
            f"- 이유: {warning_reason}\n"
            f"- WS transport/payload 정상\n"
            f"- feature 생성 정상\n"
            f"- GPT ENTRY 차단 아님\n"
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
                "level": "FAIL",
                "ok": False,
                "has_warning": False,
                "fail_reason": reason,
                "warning_reason": "",
                "checked_at_ts": time.time(),
                "ws": {},
                "feature": {"ok": False, "skipped": True, "reason": "monitor_internal_error"},
            }
            _set_health_state("FAIL", reason, "", snapshot)
            log(f"[DATA HEALTH MONITOR][FATAL] {reason}\n{tb}")

        time.sleep(CHECK_INTERVAL_SEC)


# -----------------------------------------------------
# 외부에서 호출하는 스타터
# -----------------------------------------------------
def start_health_monitor() -> None:
    """
    run_bot_ws.main() 부트 이후 실행.

    direct import stale 문제를 막기 위해 HEALTH_OK / HEALTH_WARNING / HEALTH_LEVEL /
    LAST_FAIL_REASON / LAST_WARNING_REASON 는 proxy 객체로 공개된다.

    권장 사용:
        import infra.data_health_monitor as dhm

        if not bool(dhm.HEALTH_OK):
            ...
        if bool(dhm.HEALTH_WARNING):
            ...
        if str(dhm.HEALTH_LEVEL) == "WARNING":
            ...
    """
    global _MONITOR_THREAD, _MONITOR_STARTED

    with _STATE_LOCK:
        if _MONITOR_STARTED:
            log("[DATA HEALTH MONITOR] already started -> skip duplicate launch")
            return

    symbol = str(getattr(SET, "symbol", "") or "").strip()
    if not symbol:
        raise RuntimeError("settings.symbol is required for data health monitor (STRICT)")

    # 최초 1회 동기 체크: 부팅 직후 stale 상태 노출 방지
    try:
        _update_health(symbol)
    except Exception as e:
        tb = traceback.format_exc()
        reason = f"initial health check failed: {type(e).__name__}: {e}"
        snapshot = {
            "symbol": str(symbol),
            "level": "FAIL",
            "ok": False,
            "has_warning": False,
            "fail_reason": reason,
            "warning_reason": "",
            "checked_at_ts": time.time(),
            "ws": {},
            "feature": {"ok": False, "skipped": True, "reason": "initial_health_check_failed"},
        }
        _set_health_state("FAIL", reason, "", snapshot)
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

    try:
        th.start()
    except Exception as e:
        with _STATE_LOCK:
            _MONITOR_THREAD = None
            _MONITOR_STARTED = False
        raise RuntimeError(f"failed to start data health monitor thread: {type(e).__name__}: {e}") from e

    level_state = get_health_level_state()
    level = str(level_state["level"])

    if level == "OK":
        log("[DATA HEALTH MONITOR] thread launched with initial state=OK")
    elif level == "WARNING":
        log(
            "[DATA HEALTH MONITOR] thread launched with initial state=WARNING "
            f"reason={level_state['warning_reason']}"
        )
    else:
        log(
            "[DATA HEALTH MONITOR] thread launched with initial state=FAIL "
            f"reason={level_state['fail_reason']}"
        )


__all__ = [
    "HEALTH_OK",
    "HEALTH_WARNING",
    "HEALTH_LEVEL",
    "LAST_FAIL_REASON",
    "LAST_WARNING_REASON",
    "start_health_monitor",
    "get_health_state",
    "get_health_level_state",
    "is_health_ok",
    "is_health_warning",
    "get_last_health_snapshot",
    "get_health_runtime_meta",
]