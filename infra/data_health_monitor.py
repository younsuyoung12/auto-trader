# data_health_monitor.py
# =====================================================
# BingX Auto Trader — Real-time Data Health Monitor
#
# 역할
# -----------------------------------------------------
# - WS 캔들/오더북 상태 + market_features_ws.build_entry_features_ws() 로
#   실제 GPT가 사용하는 핵심 지표가 모두 정상 생성되는지 검사.
# - 하나라도 이상(누락/None/NaN/지연/버퍼부족)이면 HEALTH_OK=False.
# - run_bot_ws / entry_flow / gpt_trader 가 이 flag를 참고해
#   GPT ENTRY 를 자동 차단하게 한다.
# - 10분마다 텔레그램으로 완전한 상태 리포트를 전송한다.
#
# 사용
# -----------------------------------------------------
# run_bot_ws.main() 부트 이후:
#       from data_health_monitor import start_health_monitor
#       start_health_monitor()
#
# entry_flow.py 상단에서:
#       from data_health_monitor import HEALTH_OK
#       if not HEALTH_OK: SKIP
#
# =====================================================

import time
import threading
import math
from typing import Dict, Any

from infra.telelog import log, send_tg
from infra.market_data_ws import get_health_snapshot
from infra.market_features_ws import build_entry_features_ws, FeatureBuildError


# -----------------------------------------------------
# 전역 상태
# -----------------------------------------------------
HEALTH_OK: bool = True
LAST_FAIL_REASON: str = ""
LAST_FULL_REPORT_TS: float = 0.0

# 보고 주기 (초) — 10분
REPORT_INTERVAL_SEC = 600

# 내부 검사 주기 (초)
CHECK_INTERVAL_SEC = 5


# -----------------------------------------------------
# 핵심 헬스 체크 함수
# -----------------------------------------------------
from typing import Tuple
def _check_ws_health(symbol: str) -> Tuple[bool, str]:

    """
    WS 1m/5m/15m 버퍼 + 오더북 상태를 검사한다.
    """
    snap = get_health_snapshot(symbol)

    if not snap.get("overall_kline_ok", True):
        return False, "WS KLINE 데이터 지연 또는 버퍼 부족"

    if not snap.get("overall_orderbook_ok", True):
        return False, "오더북(depth5) 지연 또는 부재"

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
        return False, f"예상치 못한 피처 생성 오류: {e}"

    # 필수 키 확인
    if not isinstance(feats, dict):
        return False, "features dict 구조가 비정상"

    tfs = feats.get("timeframes") or {}
    tf5 = tfs.get("5m")
    tf15 = tfs.get("15m")

    if not tf5 or not tf15:
        return False, "필수 타임프레임(5m/15m) 피처 없음"

    # trend_strength, volatility, volume_zscore 필수
    trend = tf5.get("regime", {}).get("trend_strength")
    vol = tf5.get("atr_pct")
    vz = tf5.get("volume_zscore")

    if not isinstance(trend, (int, float)) or math.isnan(trend):
        return False, "trend_strength 비정상"

    if not isinstance(vol, (int, float)) or math.isnan(vol):
        return False, "volatility(atr_pct) 비정상"

    if not isinstance(vz, (int, float)) or math.isnan(vz):
        return False, "volume_zscore 비정상"

    # ATR fast/slow
    atr_fast = tf5.get("atr")
    atr_slow = tf15.get("atr") if tf15 else None

    if not isinstance(atr_fast, (int, float)) or math.isnan(atr_fast):
        return False, "ATR_fast 비정상"

    if not isinstance(atr_slow, (int, float)) or math.isnan(atr_slow):
        return False, "ATR_slow 비정상"

    return True, ""


# -----------------------------------------------------
# 헬스 검사 → 전역 플래그 갱신
# -----------------------------------------------------
def _update_health(symbol: str):
    global HEALTH_OK, LAST_FAIL_REASON

    ok_ws, reason_ws = _check_ws_health(symbol)
    if not ok_ws:
        HEALTH_OK = False
        LAST_FAIL_REASON = reason_ws
        return

    ok_feat, reason_feat = _check_feature_health(symbol)
    if not ok_feat:
        HEALTH_OK = False
        LAST_FAIL_REASON = reason_feat
        return

    # 모든 검사 통과
    HEALTH_OK = True
    LAST_FAIL_REASON = ""


# -----------------------------------------------------
# 텔레그램 보고
# -----------------------------------------------------
def _send_full_report(symbol: str):
    """
    10분마다 전체 상태 보고.
    """
    global LAST_FULL_REPORT_TS

    now = time.time()
    if now - LAST_FULL_REPORT_TS < REPORT_INTERVAL_SEC:
        return

    LAST_FULL_REPORT_TS = now

    if HEALTH_OK:
        msg = (
            f"✅ [DATA HEALTH OK]\n"
            f"- WS KLINE/ORDERBOOK 정상\n"
            f"- 지표/패턴/ATR/트렌드 정상\n"
            f"- GPT 사용 가능"
        )
    else:
        msg = (
            f"❌ [DATA HEALTH FAIL]\n"
            f"- 이유: {LAST_FAIL_REASON}\n"
            f"- GPT ENTRY 자동 차단됨"
        )

    try:
        send_tg(msg)
    except Exception as e:
        log(f"[HEALTH_MONITOR] TG send error: {e}")


# -----------------------------------------------------
# 메인 루프 (백그라운드 스레드)
# -----------------------------------------------------
def _health_loop(symbol: str):
    log("[DATA HEALTH MONITOR] started")

    while True:
        try:
            _update_health(symbol)
            _send_full_report(symbol)
        except Exception as e:
            log(f"[DATA HEALTH MONITOR] unexpected error: {e}")

        time.sleep(CHECK_INTERVAL_SEC)


# -----------------------------------------------------
# 외부에서 호출하는 스타터
# -----------------------------------------------------
def start_health_monitor():
    """
    run_bot_ws.main() 부트 이후 실행:

        from data_health_monitor import start_health_monitor
        start_health_monitor()
    """
    from settings import load_settings
    SET = load_settings()
    symbol = SET.symbol

    th = threading.Thread(target=_health_loop, args=(symbol,), daemon=True)
    th.start()
    log("[DATA HEALTH MONITOR] thread launched")


# -----------------------------------------------------
# 다른 모듈에서 HEALTH_OK flag만 바로 import해서 사용:
#   from data_health_monitor import HEALTH_OK
# -----------------------------------------------------
__all__ = ["HEALTH_OK", "LAST_FAIL_REASON", "start_health_monitor"]
