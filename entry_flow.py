"""
entry_flow.py (WS 거래량 우선 + arbitration + EntryScore + bt_trades INSERT + EntryScore 0-100/TG/Preview + SKIP 사유 TG + GPT-5 진입 게이트 - gpt_trader 연동 버전)
=====================================================
역할
----------------------------------------------------
- 시그널 받기 → 각종 가드 → GPT-5 승인 → 주문 열기 → 로그/캔들 스냅샷까지 한 함수로 묶어서
  run_bot.py / run_bot_ws.py 에서 한 줄로 호출할 수 있게 한다.
- 웹소켓 기반 시그널(signal_flow_ws.get_trading_signal)을 사용하며,
  TREND/RANGE 동시 평가 + 중재(Arbitration) 결과를 그대로 받아서 처리한다.

2025-11-17 변경 사항 (GPT 장애 시 신규 진입 하드 스톱 + 텔레그램 알림 + 이벤트 CSV 연동 + gpt_trader 연동)
----------------------------------------------------
1) GPT 엔트리 하드 스톱 플래그 추가
   - 모듈 전역 변수 GPT_ENTRY_HARD_DOWN / GPT_ENTRY_HARD_DOWN_NOTIFIED 를 도입.
   - GPT 관련 오류가 하드 상태로 확인되면 GPT_ENTRY_HARD_DOWN=True 로 세팅하여 이후 모든 신규 진입을 전면 차단.
2) try_open_new_position(...) 진입부에서 하드 스톱 플래그를 체크.
   - GPT_ENTRY_HARD_DOWN 이 True 인 상태에서는 시그널/가드/잔고 확인을 수행하지 않고 즉시 SKIP.
   - gpt_error_sleep_sec 만큼 대기 후 (None, sleep_sec) 반환.
3) 첫 하드 스톱 발생 시 텔레그램 알림 전송.
   - GPT 장애가 처음 감지되는 시점에 [GPT_ENTRY][HARD_STOP] 메시지를 한 번만 전송.
   - 이후에는 로그/skip 사유만 남기고 추가 알림은 보내지 않는다(스팸 방지).
4) GPT 예외/호출 실패 처리 로직 수정.
   - gpt_trader.decide_entry_with_gpt_trader(...) 결과에서
     · gpt_action == "ERROR" 이고 raw 가 비어 있으면 → GPT 호출 실패로 간주, 하드 스톱 진입.
     · 그 외의 SKIP 은 단순 GPT 판단에 의한 스킵으로 처리하고 하드 스톱은 걸지 않음.
5) EntryScore.components_json 에 GPT·시그널 메타 정보 추가.
   - gpt_entry.signal 에 extra["direction_raw"], extra["direction_norm"], extra["regime_level"] 를 함께 저장.
   - signal_extra 블록으로 signal_flow_ws.extra 의 핵심 지표/사유를 별도로 남겨 사후 분석에 활용 가능.
6) signals_logger.py 이벤트 CSV 연동.
   - GPT 승인 시 log_gpt_entry_event(...) 호출.
   - GPT 거절(SKIP) 및 각종 가드/잔고/수량/주문 실패 SKIP 경로에서 log_skip_event(...) 호출.
   - GPT 장애 및 하드 스톱 상태에서도 log_skip_event(...) 로 events-YYYY-MM-DD.csv 에 기록.
7) gpt_trader 연동으로 GPT 프롬프트 컨텍스트 강화.
   - entry_flow 에서 직접 gpt_decider.ask_entry_decision(...) 를 호출하지 않고,
     gpt_trader.decide_entry_with_gpt_trader(...) 를 통해 GPT 호출/리스크 클램핑/가드 조정까지 일괄 처리.
   - guard_snapshot(min_entry_volume_ratio, max_spread_pct, max_price_jump_pct, depth_imbalance_min_*)와
     최근 PnL/skip_streak 이 GPT 프롬프트에 함께 전달되어 장 흐름/가드 상태를 보고 리스크를 조정할 수 있게 함.
   - gpt_trader 가 돌려준 guard_adjustments 는 settings 의 해당 필드를 한 루프 동안만 override 해서
     거래량/스프레드/점프 가드에 1회성으로 반영한다.
8) GPT 응답 지연(타임아웃) 처리 분리.
   - gpt_trader 결과에서 status="TIMEOUT" 이거나, gpt_action="ERROR" 이면서 reason 에
     "응답 지연" / "timeout" 이 포함된 경우를 TIMEOUT 으로 간주.
   - 이 경우에는 GPT_ENTRY_HARD_DOWN 을 건드리지 않고, 해당 시그널만 SKIP 처리 후 gpt_skip_sleep_sec 만큼 대기.
   - events CSV 에도 source="entry_flow.gpt_timeout", reason="gpt_timeout" 으로 기록하여
     진짜 장애(hard error)와 응답 지연(timeout)을 구분할 수 있게 했다.

2025-11-16 변경 사항 (GPT-5 진입 게이트 + gpt_decider.py 일원화)
----------------------------------------------------
1) GPT-5 모델을 사용한 NEW_ENTRY_CANDIDATE 의사결정 레이어를 gpt_decider.py 로 분리.
   - 이 파일(entry_flow.py)에서는 openai 를 직접 import / 호출하지 않는다.
   - GPT 호출은 gpt_decider.ask_entry_decision(...) 한 곳에서만 수행. (현재는 gpt_trader 에서 래핑 호출)
2) GPT 응답이 없거나 예외 발생 시 진입 완전 차단(폴백 없음).
   - gpt_decider.ask_entry_decision(...) 예외 → [GPT_ENTRY][SKIP] 텔레그램 전송 후 해당 시그널은 스킵.
   - 기존 "safe" 폴백(그냥 ENTER) 전략은 사용하지 않음.
3) GPT action 이 "ENTER" 또는 "ADJUST" 일 때만 주문 진행.
   - "SKIP" 또는 알 수 없는 action → 진입하지 않고 sleep 후 종료.
4) GPT가 effective_risk_pct / tp_pct / sl_pct 를 제안하면 안전 범위 내에서만 반영.
   - effective_risk_pct ≤ 3%, tp_pct ≤ 10%, sl_pct ≤ 5% 경계 초과분은 무시하고 기존 값 유지.
   - risk 키는 우선 "effective_risk_pct" 를, 없으면 "risk_pct" 를 읽어온다.
5) EntryScore.components_json 에 gpt_entry 블록 추가.
   - {"gpt_entry": {"model": ..., "action": ..., "reason": ..., "decision": {...}}} 형태로 기록.
   - model 값은 환경변수 GPT_ENTRY_MODEL (없으면 "gpt-5.1")을 사용해 로그/메타에 남긴다.
6) trader.open_position_with_tp_sl(...) 시그니처 변경분 반영.
   - settings / entry_price_hint / source / soft_mode / sl_floor_ratio 를 넘기도록 수정.
7) GPT 관련 settings 키와 쿨다운/상한값 정리.
   - gpt_error_sleep_sec, gpt_skip_sleep_sec: GPT 오류/거절 시 대기 시간(sec) 설정.
   - gpt_max_risk_pct, gpt_max_tp_pct, gpt_max_sl_pct: GPT 제안 리스크/TP/SL 상한.
   - post_entry_sleep_sec: 진입 성공 후 짧은 쿨다운 시간.

2025-11-15 변경 사항
----------------------------------------------------
1) 진입 성공 시 bt_trades 테이블에 즉시 INSERT 하는 로직 추가.
   - _create_trade_row_on_entry(...) 헬퍼 함수 신설.
   - entry_ts/entry_price/qty/is_auto/strategy/regime_at_entry/leverage/risk_pct/tp_pct/sl_pct
     및 created_at/updated_at 을 함께 저장.
   - INSERT 시 RETURNING id 를 사용해 생성된 pk 를 받아 trade.db_id 로 심어 둔다.
   - 이후 EntryScore 저장 시 trade_id로 연결될 수 있게 설계.
2) EntryScore 점수를 0~100 범위(0점~100점)로 스케일링.
   - _compute_entry_score_components(...) 에서 기존 0~10 근사 점수를 0~100 점수로 변환.
3) EntryScore DB 저장 시 텔레그램으로 요약 메시지 전송.
   - _save_entry_score_to_db(...) 내에서 send_tg(...) 호출.
   - 예) [ENTRY_SCORE] BTC-USDT 롱(TREND) 점수=83.2/100
4) 진입 직전 EntryScore 프리뷰 텔레그램 전송 헬퍼 추가.
   - _preview_entry_score_and_notify(...) 를 호출하면
     실제 주문 전에 0~100 점수/리스크/레버리지/TP·SL 을 미리 텔레그램으로 확인 가능.
5) try_open_new_position(...) 의 모든 SKIP 경로에서 텔레그램으로 이유 전송.
   - telelog.send_skip_tg(...) 를 사용해 "왜 안 들어갔는지"를 바로 확인 가능.
6) try_open_new_position(...) 이 모든 경로에서 (Trade | None, float) 튜플을 반환하도록 정리.
   - 호출부에서 `trade, sleep_sec = try_open_new_position(...)` 언패킹 시
     cannot unpack non-iterable NoneType object 에러가 나지 않도록 방지.

2025-11-14 변경 사항
----------------------------------------------------
1) signal_flow_ws 의 TREND/RANGE 동시 평가 + 중재(Arbitration)에 맞춰 extra 필드를
   그대로 반영하도록 정리.
   - TP/SL: extra["tp_pct"], extra["sl_pct"] 우선 → 없으면 settings.* 로 폴백.
   - 리스크: extra["effective_risk_pct"] 가 있으면 그 값으로 주문 금액 계산.
   - soft 플래그 / SL 바닥: extra["soft_mode"|"soft"|"range_soft"], extra["sl_floor_ratio"]
     를 open_position_with_tp_sl(...) 에 그대로 전달.
2) 캔들 스냅샷은 WS 1m → 시그널 5m raw → 시그널 5m 기준으로 일관되게 선택하도록
   _pick_latest_candle(...) 주석/설명을 보강.
3) settings 누락으로 인한 크래시를 막기 위해, 리스크/금액/쿨다운 등은 getattr(...)
   패턴으로 기본값을 가지도록 구조 정리.
4) [5단계] 진입 성공 시 EntryScore 를 계산해서 bt_entry_scores 테이블에 저장.
   - _compute_entry_score_components(...) 에서 entry_score / components_json 생성.
   - _save_entry_score_to_db(...) 에서 EntryScore ORM 으로 persisted 기록.
   - ⚠ EntryScore 계산은 "모든 필수 데이터가 온전히 있을 때만" 수행한다.
     · extra 가 dict 가 아니거나,
     · signal_score / candidate_score, atr_fast / atr_slow, risk/레버리지/TP/SL 중
       하나라도 빠지거나 값이 이상하면 → 점수 계산/저장은 하지 않고 SKIP 로그만 남긴다.
     · 추정·폴백으로 점수를 만드는 경우는 없다.

2025-11-13 변경 사항(WS 거래량/가드 통합)
----------------------------------------------------
A) 웹소켓 1m 거래량 우선
   - entry_guards_ws.check_volume_guard 에 넘겨주는 캔들은
     WS 1m 캔들이 있으면 그걸 우선 사용하고, 없을 때만 시그널이 준 5m raw 로 대체.
   - signals_logger.log_candle_snapshot 도 WS 1m 캔들이 있으면 그걸로 찍어 둔다.

B) entry_guards_ws 연동
   - check_manual_position_guard: 외부 On/Off / 강제 휴식 등 수동 가드.
   - check_volume_guard: 거래량/틱 수 등 세부 진입 조건.
   - check_price_jump_guard: 직전 캔들 대비 점프/갭 과도 시 차단.
   - check_spread_guard: WS depth5 기반 스프레드/쏠림 가드 + best_bid/best_ask 제공.

반환값
----------------------------------------------------
try_open_new_position(...) -> (Trade | None, float)
- Trade 인스턴스가 있으면 실제로 진입에 성공한 것.
- 두 번째 값은 run_bot 이 잠깐 쉬어야 할 시간(sec).
"""

from __future__ import annotations

import os
import json
import time
import math
from datetime import datetime, timezone
from typing import Any, Optional, Tuple, List, Dict

from telelog import send_tg, log, send_skip_tg
from signal_flow_ws import get_trading_signal
from entry_guards_ws import (
    check_manual_position_guard,
    check_volume_guard,
    check_price_jump_guard,
    check_spread_guard,
)
from exchange_api import (
    get_available_usdt,
    get_balance_detail,
)
from trader import open_position_with_tp_sl, Trade
from signals_logger import (
    log_signal,
    log_candle_snapshot,
    log_gpt_entry_event,
    log_skip_event,
)
from gpt_trader import decide_entry_with_gpt_trader

# DB 세션/모델 (있으면 사용, 없으면 조용히 패스)
try:
    from db_core import SessionLocal  # type: ignore
    from db_models import EntryScore as EntryScoreORM  # type: ignore
    from sqlalchemy import text as sa_text  # type: ignore
except Exception:  # pragma: no cover - DB가 아직 준비되지 않은 환경 방어
    SessionLocal = None  # type: ignore
    EntryScoreORM = None  # type: ignore
    sa_text = None  # type: ignore

# GPT-5 기본 설정 (entry_flow 에서는 로깅/메타 정보용으로만 사용)
DEFAULT_GPT_ENTRY_MODEL = "gpt-5.1"

# 안전 범위 기본값 (settings 에서 덮어쓸 수 있음)
DEFAULT_GPT_MAX_RISK_PCT = 0.03  # 3%
DEFAULT_GPT_MAX_TP_PCT = 0.10    # 10%
DEFAULT_GPT_MAX_SL_PCT = 0.05    # 5%

# GPT 엔트리 하드 스톱 상태 플래그
# - GPT_ENTRY_HARD_DOWN 이 True 가 되면, 이후 try_open_new_position(...) 호출에서는
#   GPT 를 다시 시도하지 않고 즉시 신규 진입을 전면 차단한다.
# - 하드 스톱이 걸린 상태를 해제하려면 프로세스를 재시작하거나, 별도 관리 코드를 추가해야 한다.
GPT_ENTRY_HARD_DOWN: bool = False
GPT_ENTRY_HARD_DOWN_NOTIFIED: bool = False

# ✅ 웹소켓 시세 버퍼에서 거래량까지 있는 캔들을 가져오기
# - ws_get_klines_with_volume(symbol, interval, limit)
# - 없거나 ImportError 나면 None 으로 두고, 나중에 fallback 한다.
try:
    from market_data_ws import get_klines_with_volume as ws_get_klines_with_volume
except ImportError:  # pragma: no cover - 환경에 따라 없을 수 있음
    ws_get_klines_with_volume = None  # type: ignore[assignment]


def _pick_latest_candle(
    ws_candles: Optional[List[tuple]],
    signal_candles_raw: Optional[List[List[float]]],
    signal_candles: Optional[List[List[float]]],
) -> Tuple[int, float, float, float, float, float]:
    """캔들 스냅샷에 쓸 (ts, o, h, l, c, v) 하나를 고른다.

    우선순위:
        1) 웹소켓 1m 캔들(ws_candles)이 있고 (ts, o, h, l, c, v) 형태면 그대로 사용.
        2) signal_flow_ws 가 내려준 raw 캔들(candles_5m_raw)에 거래량이 있으면 그걸 사용.
        3) 기본 5m 캔들(candles_5m)만 있으면 v=0.0 을 붙여서 사용.
        4) 진짜 아무 것도 없으면 현재 시각 기준 dummy 캔들을 만든다.
    """
    # 1) 웹소켓 캔들(1m) 우선
    if ws_candles:
        last = ws_candles[-1]
        # ws_candles 는 (ts, o, h, l, c, v) 형태라고 가정
        if len(last) >= 6:
            return (
                int(last[0]),
                float(last[1]),
                float(last[2]),
                float(last[3]),
                float(last[4]),
                float(last[5]),
            )

    # 2) 시그널이 준 raw 캔들(주로 5m, 거래량 포함 포맷)
    if signal_candles_raw and len(signal_candles_raw[-1]) >= 6:
        raw_c = signal_candles_raw[-1]
        return (
            int(raw_c[0]),
            float(raw_c[1]),
            float(raw_c[2]),
            float(raw_c[3]),
            float(raw_c[4]),
            float(raw_c[5]),
        )

    # 3) 시그널이 준 기본 캔들(OHLC만 있는 경우)
    if signal_candles and len(signal_candles[-1]) >= 5:
        base_c = signal_candles[-1]
        return (
            int(base_c[0]),
            float(base_c[1]),
            float(base_c[2]),
            float(base_c[3]),
            float(base_c[4]),
            0.0,
        )

    # 4) 진짜 아무 것도 없으면 현재 시각 기준 dummy 캔들
    now_ts = int(time.time() * 1000)
    return now_ts, 0.0, 0.0, 0.0, 0.0, 0.0


def _infer_regime_from_signal(signal_source: str) -> str:
    """signal_source → Regime label 매핑."""
    if signal_source in ("TREND", "RANGE", "NO_TRADE"):
        return signal_source
    if signal_source == "HYBRID":
        return "HYBRID"
    return "UNKNOWN"


def _compute_entry_score_components(
    *,
    signal_source: str,
    chosen_signal: str,
    effective_risk_pct: float,
    leverage: float,
    tp_pct: float,
    sl_pct: float,
    extra: Optional[Dict[str, Any]],
) -> Tuple[float, Dict[str, Any]]:
    """진입 품질을 단일 score + components breakdown 형태로 계산.

    ⚠ 폴백/추정 없음.
    - 필수 값이 하나라도 없거나, NaN/inf/0 등으로 비정상이면 ValueError 를 발생시킨다.
    - 호출부에서 이 예외를 잡아서 EntryScore 저장을 SKIP 하고 로그만 남긴다.

    필수 입력:
    - extra: dict 여야 하고, 다음 키가 필수
      · signal_score 또는 candidate_score (float)
      · atr_fast, atr_slow (float, atr_slow > 0)
    - effective_risk_pct > 0
    - leverage > 0
    - tp_pct > 0, sl_pct > 0

    반환:
    - entry_score: float (0~100 범위 점수)
    - components: {"base": ..., "signal_strength": ..., "risk": ..., "volatility": ..., "entry_score": ...}
    """
    missing: List[str] = []

    # extra dict 필수
    if not isinstance(extra, dict):
        missing.append("extra(dict)")

    # 수치값 기본 검증 유틸
    def _valid_num(name: str, value: Any, *, min_val: float | None = None) -> bool:
        if not isinstance(value, (int, float)):
            missing.append(name)
            return False
        if not math.isfinite(float(value)):
            missing.append(f"{name}_nan")
            return False
        if min_val is not None and float(value) <= min_val:
            missing.append(f"{name}_le_{min_val}")
            return False
        return True

    _valid_num("effective_risk_pct", effective_risk_pct, min_val=0.0)
    _valid_num("leverage", leverage, min_val=0.0)
    _valid_num("tp_pct", tp_pct, min_val=0.0)
    _valid_num("sl_pct", sl_pct, min_val=0.0)

    signal_strength = None
    atr_fast = None
    atr_slow = None

    if isinstance(extra, dict):
        # 시그널 강도 점수 (필수)
        raw_score = extra.get("signal_score")
        if raw_score is None:
            raw_score = extra.get("candidate_score")
        if isinstance(raw_score, (int, float)) and math.isfinite(float(raw_score)):
            signal_strength = float(raw_score)
        else:
            missing.append("signal_score/candidate_score")

        # ATR fast/slow (필수)
        atr_fast = extra.get("atr_fast")
        atr_slow = extra.get("atr_slow")
        if not isinstance(atr_fast, (int, float)) or not math.isfinite(float(atr_fast)):
            missing.append("atr_fast")
        if not isinstance(atr_slow, (int, float)) or not math.isfinite(float(atr_slow)) or float(atr_slow) <= 0:
            missing.append("atr_slow")

    # 하나라도 문제 있으면 점수 계산 SKIP
    if missing:
        raise ValueError("missing/invalid fields for EntryScore: " + ", ".join(sorted(set(missing))))

    components: Dict[str, Any] = {}

    # 1) 기본 점수
    base_score = 1.0
    components["base"] = base_score

    # 2) 시그널 강도 (필수 값, 위에서 검증 완료)
    components["signal_strength"] = signal_strength
    components["chosen_signal"] = chosen_signal
    components["signal_source"] = signal_source

    # 3) 리스크/레버리지 기반 점수
    risk_norm = min(max(float(effective_risk_pct), 0.0), 0.03) / 0.01  # 0~3
    leverage_norm = min(max(float(leverage), 1.0), 50.0) / 10.0        # 0.1~5.0
    risk_score = risk_norm + 0.5 * leverage_norm
    components["risk_pct"] = float(effective_risk_pct)
    components["leverage"] = float(leverage)
    components["risk_score"] = risk_score

    # 4) ATR 기반 변동성 점수 (필수 값, 위에서 검증 완료)
    atr_fast_f = float(atr_fast)  # type: ignore[arg-type]
    atr_slow_f = float(atr_slow)  # type: ignore[arg-type]
    atr_ratio = atr_fast_f / atr_slow_f
    # 0.5~2.0 구간은 0.5~2.0 점수로 그대로 사용 (너무 크면 클램프)
    vol_score = min(max(atr_ratio, 0.5), 2.0)

    components["atr_fast"] = atr_fast_f
    components["atr_slow"] = atr_slow_f
    components["atr_ratio"] = atr_ratio
    components["volatility_score"] = vol_score

    # 5) TP/SL 비율 자체도 참고용으로 남긴다.
    tp_pct_f = float(tp_pct)
    sl_pct_f = float(sl_pct)
    components["tp_pct"] = tp_pct_f
    components["sl_pct"] = sl_pct_f
    components["tp_sl_ratio"] = tp_pct_f / sl_pct_f

    # 6) 최종 entry_score 합성 (내부적으로 0~10 근사 스코어 만든 뒤 0~100 으로 스케일링)
    entry_score_raw = (
        base_score
        + 0.7 * float(signal_strength)
        + 0.5 * risk_score
        + 0.5 * vol_score
    )
    components["entry_score_raw"] = entry_score_raw

    # 0 이하 / 10 이상은 클램프 후 0~100 으로 변환
    entry_score_clamped = min(max(entry_score_raw, 0.0), 10.0)
    entry_score = entry_score_clamped * 10.0
    components["entry_score"] = entry_score

    return entry_score, components


def _preview_entry_score_and_notify(
    *,
    symbol: str,
    signal_source: str,
    chosen_signal: str,
    effective_risk_pct: float,
    leverage: float,
    tp_pct: float,
    sl_pct: float,
    extra: Optional[Dict[str, Any]],
) -> Tuple[Optional[float], Dict[str, Any]]:
    """진입 직전 EntryScore 를 계산해서 텔레그램으로 프리뷰 전송.

    - _compute_entry_score_components(...) 를 호출해서 0~100 점수/컴포넌트 계산.
    - 계산에 필요한 값이 부족하면 ValueError 를 잡고 로그만 남기고 조용히 패스.

    반환:
    - (entry_score or None, components_dict)
      · entry_score 가 None 이면 점수 계산이 스킵된 것.
    """
    try:
        entry_score, components = _compute_entry_score_components(
            signal_source=signal_source,
            chosen_signal=chosen_signal,
            effective_risk_pct=effective_risk_pct,
            leverage=leverage,
            tp_pct=tp_pct,
            sl_pct=sl_pct,
            extra=extra,
        )
    except ValueError as e:
        log(f"[ENTRY_PREVIEW] score compute skipped: {e}")
        return None, {}

    # 텔레그램 프리뷰 메시지
    try:
        side_up = (chosen_signal or "").upper()
        if side_up == "LONG":
            side_ko = "롱"
        elif side_up == "SHORT":
            side_ko = "숏"
        else:
            side_ko = side_up or "?"

        msg = (
            f"[ENTRY_PREVIEW] {symbol} {side_ko}({signal_source}) "
            f"점수={entry_score:.1f}/100 "
            f"리스크={effective_risk_pct * 100:.2f}% "
            f"레버리지={leverage:.1f}x "
            f"TP={tp_pct * 100:.2f}% SL={sl_pct * 100:.2f}%"
        )
        send_tg(msg)
    except Exception as e:
        log(f"[ENTRY_PREVIEW] telegram send failed: {e}")

    return entry_score, components


def _save_entry_score_to_db(
    *,
    symbol: str,
    ts_ms: int,
    side: str,
    signal_type: str,
    regime_at_entry: str,
    entry_score: float,
    components: Dict[str, Any],
    trade: Trade,
    used_for_entry: bool = True,
) -> None:
    """bt_entry_scores 테이블에 EntryScore 레코드를 저장.

    - DB 세션/모델이 준비되지 않은 환경에서는 조용히 패스하고 로그만 남긴다.
    - DB 에러 발생 시 rollback 후 에러 로그를 남긴다.
    - 저장에 성공하면 텔레그램으로도 점수 요약을 전송한다.
    """
    if SessionLocal is None or EntryScoreORM is None:
        log("[ENTRY_SCORE] SessionLocal / EntryScoreORM 없음 → 기록 생략")
        return

    try:
        session = SessionLocal()
    except Exception as e:  # pragma: no cover - DB 초기화 실패 방어
        log(f"[ENTRY_SCORE] Session 생성 실패: {e}")
        return

    trade_db_id = None

    try:
        ts_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

        # Trade 객체에 DB pk 가 있으면 활용 (없어도 무방)
        trade_db_id = getattr(trade, "db_id", None)
        if trade_db_id is None:
            trade_db_id = getattr(trade, "id", None)

        es = EntryScoreORM(
            symbol=symbol,
            ts=ts_dt,
            side=side,
            signal_type=signal_type,
            regime_at_entry=regime_at_entry,
            entry_score=float(entry_score),
            components_json=components,
            used_for_entry=used_for_entry,
            trade_id=trade_db_id,
        )
        session.add(es)
        session.commit()
        log(f"[ENTRY_SCORE] saved: symbol={symbol} ts={ts_dt} score={entry_score:.3f}")

        # ✅ 저장 성공 시 텔레그램으로도 점수 알림 전송
        try:
            side_up = (side or "").upper()
            if side_up == "LONG":
                side_ko = "롱"
            elif side_up == "SHORT":
                side_ko = "숏"
            else:
                side_ko = side_up or "?"

            msg = (
                f"[ENTRY_SCORE] {symbol} {side_ko}({signal_type}) "
                f"점수={entry_score:.1f}/100 "
                f"regime={regime_at_entry} "
            )
            if trade_db_id is not None:
                msg += f"trade_id={trade_db_id}"

            send_tg(msg)
        except Exception as e:
            log(f"[ENTRY_SCORE] telegram send failed: {e}")

    except Exception as e:  # pragma: no cover - DB 에러 방어
        session.rollback()
        log(f"[ENTRY_SCORE] insert failed: {e}")
    finally:
        session.close()


def _create_trade_row_on_entry(
    *,
    trade: Trade,
    symbol: str,
    latest_ts_ms: int,
    regime_at_entry: str,
    signal_source: str,
    effective_risk_pct: float,
    tp_pct: float,
    sl_pct: float,
    is_auto: bool = True,
) -> None:
    """진입 직후 bt_trades 에 1건 INSERT.

    - INSERT 시 RETURNING id 를 사용해 pk 를 받아 trade.db_id 로 심어 둔다.
    - SessionLocal / sa_text 가 없으면 조용히 패스하고 로그만 남긴다.
    - DB 에러 발생 시 rollback 후 에러 로그를 남긴다.
    """
    if SessionLocal is None or sa_text is None:
        log("[BT_TRADES] SessionLocal / sa_text 없음 → INSERT 생략")
        return

    try:
        session = SessionLocal()
    except Exception as e:  # pragma: no cover
        log(f"[BT_TRADES] Session 생성 실패: {e}")
        return

    try:
        entry_dt = datetime.fromtimestamp(latest_ts_ms / 1000.0, tz=timezone.utc)
        created_at = entry_dt
        updated_at = entry_dt

        params = {
            "symbol": symbol,
            "side": trade.side,
            "entry_ts": entry_dt,
            "entry_price": float(trade.entry),
            "qty": float(trade.qty),
            "is_auto": bool(is_auto),
            "regime_at_entry": regime_at_entry,
            "strategy": signal_source,
            "leverage": float(getattr(trade, "leverage", 0.0)),
            "risk_pct": float(effective_risk_pct),
            "tp_pct": float(tp_pct),
            "sl_pct": float(sl_pct),
            "note": "",
            "created_at": created_at,
            "updated_at": updated_at,
        }

        sql = sa_text(
            """
            INSERT INTO bt_trades (
                symbol,
                side,
                entry_ts,
                entry_price,
                qty,
                pnl_usdt,
                pnl_pct_futures,
                pnl_pct_spot_ref,
                is_auto,
                regime_at_entry,
                regime_at_exit,
                entry_score,
                trend_score_at_entry,
                range_score_at_entry,
                strategy,
                close_reason,
                leverage,
                risk_pct,
                tp_pct,
                sl_pct,
                note,
                created_at,
                updated_at
            ) VALUES (
                :symbol,
                :side,
                :entry_ts,
                :entry_price,
                :qty,
                NULL,
                NULL,
                NULL,
                :is_auto,
                :regime_at_entry,
                NULL,
                NULL,
                NULL,
                NULL,
                :strategy,
                NULL,
                :leverage,
                :risk_pct,
                :tp_pct,
                :sl_pct,
                :note,
                :created_at,
                :updated_at
            )
            RETURNING id
            """
        )

        result = session.execute(sql, params)
        new_id = result.scalar_one_or_none()
        session.commit()

        if new_id is not None:
            try:
                setattr(trade, "db_id", int(new_id))
            except Exception:
                pass

        log(
            f"[BT_TRADES] INSERT ok: symbol={symbol} side={trade.side} "
            f"price={trade.entry} qty={trade.qty} id={new_id}"
        )
    except Exception as e:  # pragma: no cover
        session.rollback()
        log(f"[BT_TRADES] INSERT failed: {e}")
    finally:
        session.close()


def try_open_new_position(
    settings: Any,
    last_trend_close_ts: float,
    last_range_close_ts: float,
) -> Tuple[Optional[Trade], float]:
    """시그널을 받아서 조건이 되면 실제로 포지션을 연다.

    아무 것도 못 열면 (None, sleep_sec)을 돌려준다.
    - settings: BotSettings (settings_ws.load_settings 결과)
    - last_trend_close_ts / last_range_close_ts: 최근 전략별 청산 시각(쿨다운용)
    """
    symbol = settings.symbol

    # GPT 엔트리 하드 스톱 상태라면, 신규 진입을 전면 차단한다.
    # - GPT 장애가 한 번이라도 발생해 GPT_ENTRY_HARD_DOWN 이 True 가 되면,
    #   이 함수는 GPT 를 다시 호출하지 않고, 즉시 SKIP + 짧은 대기 후 반환한다.
    global GPT_ENTRY_HARD_DOWN, GPT_ENTRY_HARD_DOWN_NOTIFIED
    if GPT_ENTRY_HARD_DOWN:
        msg = "[GPT_ENTRY][HARD_STOP] GPT 장애 상태 → 신규 진입 스킵"
        log(msg)
        try:
            send_skip_tg(msg)
        except Exception as e:
            log(f"[GPT_ENTRY] send_skip_tg failed under hard stop: {e}")

        # 이벤트 CSV에도 하드 스톱 SKIP 기록
        log_skip_event(
            symbol=symbol,
            regime="UNKNOWN",
            source="entry_flow.gpt_hard_stop",
            side=None,
            reason="gpt_entry_hard_down",
            extra={"note": "GPT hard stop active; new entries blocked"},
        )

        # 첫 시도 시점에만 하드 스톱 상태를 명시적으로 알린다.
        if not GPT_ENTRY_HARD_DOWN_NOTIFIED:
            try:
                send_tg(
                    "[GPT_ENTRY][HARD_STOP] GPT 오류로 인해 신규 진입을 중단한 상태입니다. "
                    "원인을 확인한 뒤 봇을 재시작해 주세요."
                )
            except Exception as e:
                log(f"[GPT_ENTRY] hard stop telegram send failed: {e}")
            GPT_ENTRY_HARD_DOWN_NOTIFIED = True

        sleep_sec = float(getattr(settings, "gpt_error_sleep_sec", 5.0))
        return None, sleep_sec

    # GPT 메타 정보 (로그/EntryScore components 에 기록용)
    gpt_model_for_log = os.getenv("GPT_ENTRY_MODEL", DEFAULT_GPT_ENTRY_MODEL)
    gpt_decision: Dict[str, Any] = {}
    gpt_action: str = ""
    gpt_reason: Optional[str] = None

    # (1) 시그널 받기 (WS + arbitration 버전)
    signal_ctx = get_trading_signal(
        settings=settings,
        last_trend_close_ts=last_trend_close_ts,
        last_range_close_ts=last_range_close_ts,
    )
    if signal_ctx is None:
        # 시그널이 없거나 모든 후보가 중재 단계에서 탈락
        send_skip_tg(f"[SKIP] no_signal_or_arbitration_rejected: symbol={symbol}")
        log_skip_event(
            symbol=symbol,
            regime="NO_TRADE",
            source="entry_flow.signal",
            side=None,
            reason="no_signal_or_arbitration_rejected",
            extra=None,
        )
        return None, 1.0

    (
        chosen_signal,     # "LONG" / "SHORT"
        signal_source,     # "TREND" / "RANGE" / "HYBRID"
        latest_ts,
        candles_5m,        # 기본 5m 캔들(OHLC)
        candles_5m_raw,    # 필요 시 거래량까지 포함된 원본 캔들
        last_price,
        extra,
    ) = signal_ctx

    # (1-1) 웹소켓에서 1m 캔들을 한 번 땡겨본다. 없으면 None.
    ws_candles_1m: Optional[List[tuple]] = None
    if ws_get_klines_with_volume is not None:
        try:
            ws_candles_1m = ws_get_klines_with_volume(symbol, "1m", 40)
        except Exception as e:  # pragma: no cover - 방어적 로깅
            log(f"[ENTRY][WS] get 1m candles failed: {e}")
            ws_candles_1m = None

    # (1-2) 시그널 시점 캔들 스냅샷 남기기 (WS 1m가 있으면 그걸로)
    try:
        c_ts, o, h, l, c, v = _pick_latest_candle(
            ws_candles=ws_candles_1m,
            signal_candles_raw=candles_5m_raw,
            signal_candles=candles_5m,
        )
        log_candle_snapshot(
            symbol=symbol,
            tf=settings.interval,
            candle_ts=c_ts,
            open_=o,
            high=h,
            low=l,
            close=c,
            volume=v,
            strategy_type=signal_source,
            direction="LONG" if chosen_signal == "LONG" else "SHORT",
            extra="signal_eval=1",
        )
    except Exception as e:  # pragma: no cover - 스냅샷 실패는 무시
        log(f"[SIGNAL_SNAPSHOT] log failed: {e}")

    regime_label = _infer_regime_from_signal(signal_source)

    # (2) 진입 전 가드: 수동/외부 가드 (GPT 이전에 항상 체크)
    manual_ok = check_manual_position_guard(
        get_balance_detail_func=get_balance_detail,
        symbol=symbol,
        latest_ts=latest_ts,
    )
    if not manual_ok:
        send_skip_tg("[SKIP] manual_guard_blocked (외부 강제 OFF/휴식)")
        log_skip_event(
            symbol=symbol,
            regime=regime_label,
            source="entry_flow.guard.manual",
            side=chosen_signal,
            reason="manual_guard_blocked",
            extra={"latest_ts": int(latest_ts)},
        )
        return None, 2.0

    # (3) 잔고 확인 (GPT 호출 전에 0잔고는 바로 컷)
    avail = get_available_usdt()
    if avail <= 0:
        send_tg("[BALANCE_SKIP] 가용 선물 잔고 0 → 진입 안함")
        log_signal(
            event="SKIP",
            symbol=symbol,
            strategy_type=signal_source,
            direction=chosen_signal,
            reason="balance_zero",
            candle_ts=latest_ts,
        )
        log_skip_event(
            symbol=symbol,
            regime=regime_label,
            source="entry_flow.balance",
            side=chosen_signal,
            reason="balance_zero",
            extra={"available_usdt": float(avail)},
        )
        return None, 5.0

    # (4) 주문 관련 파라미터 계산 (리스크/레버리지/TP·SL 기본값)
    # settings 에 값이 없으면 안전한 기본값으로.
    leverage = float(getattr(settings, "leverage", 10.0))
    risk_pct = float(getattr(settings, "risk_pct", 0.01))  # 1% 기본

    # TP/SL 퍼센트 (extra 우선)
    tp_pct = float(extra.get("tp_pct")) if isinstance(extra, dict) and extra.get("tp_pct") is not None else float(
        getattr(settings, "tp_pct", 0.02)
    )
    sl_pct = float(extra.get("sl_pct")) if isinstance(extra, dict) and extra.get("sl_pct") is not None else float(
        getattr(settings, "sl_pct", 0.01)
    )

    effective_risk_pct = float(extra.get("effective_risk_pct")) if isinstance(extra, dict) and extra.get(
        "effective_risk_pct"
    ) is not None else risk_pct

    # (4-1) 진입 전 점수 프리뷰 (GPT 프롬프트에도 같이 들어갈 요약 정보)
    preview_score, preview_components = _preview_entry_score_and_notify(
        symbol=symbol,
        signal_source=signal_source,
        chosen_signal=chosen_signal,
        effective_risk_pct=effective_risk_pct,
        leverage=leverage,
        tp_pct=tp_pct,
        sl_pct=sl_pct,
        extra=extra,
    )

    # (4-2) 현재 가드 스냅샷 구성 (GPT 프롬프트 + guard_adjustments 에 사용)
    guard_snapshot: Dict[str, float] = {}
    for key in [
        "min_entry_volume_ratio",
        "max_spread_pct",
        "max_price_jump_pct",
        "depth_imbalance_min_ratio",
        "depth_imbalance_min_notional",
    ]:
        val = getattr(settings, key, None)
        if isinstance(val, (int, float)):
            guard_snapshot[key] = float(val)

    # (4-3) GPT-5 진입 의사결정 (gpt_trader 사용, 폴백 없음)
    try:
        gpt_result = decide_entry_with_gpt_trader(
            settings,
            symbol=symbol,
            signal_source=signal_source,
            direction=chosen_signal,
            last_price=float(last_price),
            entry_score=preview_score,
            base_risk_pct=effective_risk_pct,
            base_tp_pct=tp_pct,
            base_sl_pct=sl_pct,
            extra=extra if isinstance(extra, dict) else None,
            guard_snapshot=guard_snapshot or None,
        )
    except Exception as e:
        # gpt_trader 내부에서도 예외를 처리하지만, 혹시 모를 오류는 하드 스톱으로 격리
        msg = f"[GPT_ENTRY][ERROR] gpt_trader unexpected exception → hard stop: {e}"
        log(msg)
        try:
            send_skip_tg(msg)
        except Exception as te:
            log(f"[GPT_ENTRY] send_skip_tg failed on gpt_trader exception: {te}")

        log_skip_event(
            symbol=symbol,
            regime=regime_label,
            source="entry_flow.gpt_error",
            side=chosen_signal,
            reason="gpt_trader_exception",
            extra={"error": str(e)},
        )

        GPT_ENTRY_HARD_DOWN = True
        if not GPT_ENTRY_HARD_DOWN_NOTIFIED:
            try:
                send_tg(
                    "[GPT_ENTRY][HARD_STOP] GPT 호출 오류가 발생하여 신규 진입을 전면 중단합니다. "
                    "원인을 확인한 뒤 봇을 재시작해 주세요."
                )
            except Exception as te:
                log(f"[GPT_ENTRY] hard stop telegram send failed: {te}")
            GPT_ENTRY_HARD_DOWN_NOTIFIED = True

        sleep_sec = float(getattr(settings, "gpt_error_sleep_sec", 5.0))
        return None, sleep_sec

    gpt_action = str(gpt_result.get("gpt_action", "")).upper()
    final_action = str(gpt_result.get("final_action", "SKIP")).upper()
    raw_decision = gpt_result.get("raw") or {}
    reason_val = gpt_result.get("reason")
    gpt_reason = str(reason_val) if isinstance(reason_val, str) else None
    status_val = gpt_result.get("status")
    gpt_status = str(status_val).upper() if isinstance(status_val, str) else ""
    gpt_decision = raw_decision  # EntryScore 메타용

    # (4-4) GPT 타임아웃(응답 지연) → 이 캔들만 SKIP (하드 스톱 금지)
    is_timeout = False
    if gpt_status == "TIMEOUT":
        is_timeout = True
    elif gpt_action == "ERROR" and gpt_reason:
        reason_lower = gpt_reason.lower()
        if "응답 지연" in gpt_reason or "timeout" in reason_lower or "time out" in reason_lower:
            is_timeout = True

    if is_timeout:
        msg = f"[GPT_ENTRY][TIMEOUT] {gpt_reason or 'gpt_timeout'} → this signal skipped (no hard stop)"
        log(msg)
        try:
            send_skip_tg(msg)
        except Exception as te:
            log(f"[GPT_ENTRY] send_skip_tg failed on timeout: {te}")

        log_skip_event(
            symbol=symbol,
            regime=regime_label,
            source="entry_flow.gpt_timeout",
            side=chosen_signal,
            reason="gpt_timeout",
            extra={"gpt_result": gpt_result},
        )

        sleep_sec = float(
            gpt_result.get(
                "sleep_after_sec",
                getattr(settings, "gpt_skip_sleep_sec", 3.0),
            )
        )
        return None, sleep_sec

    # (4-5) GPT 하드 장애 감지: gpt_action == ERROR 이고 raw 가 비어 있는 경우
    if gpt_action == "ERROR" and not raw_decision:
        msg = "[GPT_ENTRY][ERROR] GPT_TRADER returned ERROR with empty raw → hard stop"
        log(msg)
        try:
            send_skip_tg(msg)
        except Exception as te:
            log(f"[GPT_ENTRY] send_skip_tg failed on hard error: {te}")

        log_skip_event(
            symbol=symbol,
            regime=regime_label,
            source="entry_flow.gpt_error",
            side=chosen_signal,
            reason="gpt_entry_exception",
            extra={"gpt_result": gpt_result},
        )

        GPT_ENTRY_HARD_DOWN = True
        if not GPT_ENTRY_HARD_DOWN_NOTIFIED:
            try:
                send_tg(
                    "[GPT_ENTRY][HARD_STOP] GPT 호출 오류가 발생하여 신규 진입을 전면 중단합니다. "
                    "원인을 확인한 뒤 봇을 재시작해 주세요."
                )
            except Exception as te:
                log(f"[GPT_ENTRY] hard stop telegram send failed: {te}")
            GPT_ENTRY_HARD_DOWN_NOTIFIED = True

        sleep_sec = float(getattr(settings, "gpt_error_sleep_sec", 5.0))
        return None, sleep_sec

    # (4-6) GPT 판단에 따른 일반 SKIP (ENTER/ADJUST 가 아닌 경우, 또는 final_action=SKIP)
    if final_action != "ENTER":
        msg = f"[GPT_ENTRY][SKIP] action={gpt_action or 'NONE'} reason={gpt_reason or 'no_reason'}"
        log(msg)
        send_skip_tg(msg)
        log_skip_event(
            symbol=symbol,
            regime=regime_label,
            source="entry_flow.gpt_decision",
            side=chosen_signal,
            reason=f"gpt_action_{gpt_action or 'NONE'}",
            extra={"gpt_result": gpt_result},
        )
        sleep_sec = float(
            gpt_result.get(
                "sleep_after_sec",
                getattr(settings, "gpt_skip_sleep_sec", 3.0),
            )
        )
        return None, sleep_sec

    # (4-7) GPT 가 승인한 리스크/TP/SL 및 가드 조정값 적용
    effective_risk_pct = float(gpt_result.get("effective_risk_pct", effective_risk_pct))
    tp_pct = float(gpt_result.get("tp_pct", tp_pct))
    sl_pct = float(gpt_result.get("sl_pct", sl_pct))

    guard_adjustments: Dict[str, float] = {}
    raw_ga = gpt_result.get("guard_adjustments") or {}
    if isinstance(raw_ga, dict):
        try:
            guard_adjustments = {k: float(v) for k, v in raw_ga.items()}
        except Exception:
            guard_adjustments = {}

    # GPT 승인/보정 결과 텔레그램 알림 (정보용)
    try:
        side_up = (chosen_signal or "").upper()
        if side_up == "LONG":
            side_ko = "롱"
        elif side_up == "SHORT":
            side_ko = "숏"
        else:
            side_ko = side_up or "?"

        msg = (
            f"[GPT_ENTRY] APPROVED {symbol} {side_ko}({signal_source}) "
            f"risk={effective_risk_pct * 100:.2f}% tp={tp_pct * 100:.2f}% sl={sl_pct * 100:.2f}% "
            f"model={gpt_model_for_log}"
        )
        if gpt_reason:
            msg += f" reason={gpt_reason}"
        if guard_adjustments:
            msg += f" guards={guard_adjustments}"
        send_tg(msg)
    except Exception as e:
        log(f"[GPT_ENTRY] telegram send failed: {e}")

    # GPT 승인 결과를 이벤트 CSV에도 기록
    log_gpt_entry_event(
        symbol=symbol,
        regime=regime_label,
        side=chosen_signal,
        action=gpt_action,
        tp_pct=tp_pct,
        sl_pct=sl_pct,
        risk_pct=effective_risk_pct,
        gpt_json={
            "model": gpt_model_for_log,
            "decision": raw_decision,
            "reason": gpt_reason,
            "entry_score_preview": preview_score,
            "guard_adjustments": guard_adjustments,
        },
    )

    # (5) 거래량/가격 점프/스프레드 가드
    #     - gpt_trader 가 내려준 guard_adjustments 를 settings 에 1회성으로 반영한 뒤 체크
    original_guard_values: Dict[str, Any] = {}
    try:
        # 5-1) guard_adjustments 를 settings 에 주입 (한 루프 동안만)
        for key, val in guard_adjustments.items():
            if hasattr(settings, key):
                original_guard_values.setdefault(key, getattr(settings, key))
                try:
                    setattr(settings, key, val)
                except Exception:
                    pass

        # 5-2) 거래량 가드
        vol_source = ws_candles_1m if ws_candles_1m else candles_5m_raw
        vol_ok = check_volume_guard(
            settings=settings,
            candles_5m_raw=vol_source,
            latest_ts=latest_ts,
            signal_source=signal_source,
            direction=chosen_signal,
        )
        if not vol_ok:
            send_skip_tg("[SKIP] volume_guard: volume_too_low_for_entry")
            log_skip_event(
                symbol=symbol,
                regime=regime_label,
                source="entry_flow.guard.volume",
                side=chosen_signal,
                reason="volume_too_low_for_entry",
                extra={"latest_ts": int(latest_ts)},
            )
            return None, 1.0

        # 5-3) 직전/현재 5m 기준 가격 점프·갭 가드
        price_ok = check_price_jump_guard(
            settings=settings,
            candles_5m=candles_5m,
            latest_ts=latest_ts,
            signal_source=signal_source,
            direction=chosen_signal,
        )
        if not price_ok:
            send_skip_tg("[SKIP] price_jump_guard: recent_price_jump_or_gap")
            log_skip_event(
                symbol=symbol,
                regime=regime_label,
                source="entry_flow.guard.price_jump",
                side=chosen_signal,
                reason="price_jump_guard_recent_price_jump_or_gap",
                extra={"latest_ts": int(latest_ts)},
            )
            return None, 1.0

        # 5-4) 호가 스프레드/쏠림 가드 (WS depth5 활용)
        spread_ok, best_bid, best_ask = check_spread_guard(
            settings=settings,
            symbol=symbol,
            latest_ts=latest_ts,
            signal_source=signal_source,
            direction=chosen_signal,
        )
        if not spread_ok:
            send_skip_tg("[SKIP] spread_guard: depth_imbalance_or_spread_too_wide")
            log_skip_event(
                symbol=symbol,
                regime=regime_label,
                source="entry_flow.guard.spread",
                side=chosen_signal,
                reason="depth_imbalance_or_spread_too_wide",
                extra={"latest_ts": int(latest_ts), "best_bid": best_bid, "best_ask": best_ask},
            )
            return None, 1.0

    finally:
        # 5-5) settings 의 가드 값 복원 (guard_adjustments 는 1회성)
        for key, val in original_guard_values.items():
            try:
                setattr(settings, key, val)
            except Exception:
                pass

    # (6) 실제 주문 수량 계산
    #   - available USDT * effective_risk_pct * leverage / price
    #   - 최소/단위는 settings 에 따라 맞춘다.
    notional = avail * effective_risk_pct * leverage
    qty = notional / max(float(last_price), 1.0)

    # 수량 단위 및 최소 수량 적용
    qty_step = float(getattr(settings, "qty_step", 0.0001))
    min_qty = float(getattr(settings, "min_qty", qty_step))

    def _round_step(x: float, step: float) -> float:
        if step <= 0:
            return x
        return math.floor(x / step) * step

    qty = _round_step(qty, qty_step)
    if qty < min_qty:
        send_skip_tg(
            f"[SKIP] qty_too_small: qty={qty:.8f} < min_qty={min_qty:.8f} (notional={notional:.4f}USDT)"
        )
        log_skip_event(
            symbol=symbol,
            regime=regime_label,
            source="entry_flow.qty",
            side=chosen_signal,
            reason="qty_too_small",
            extra={"qty": qty, "min_qty": min_qty, "notional": notional},
        )
        return None, 5.0

    # (7) 실제 주문 실행
    side = "BUY" if chosen_signal == "LONG" else "SELL"
    regime_at_entry = regime_label

    # RANGE 신호에서 넘어온 soft_mode / SL 바닥 비율(extra)을 open_position_with_tp_sl 에 그대로 전달
    soft_mode = False
    sl_floor_ratio = None
    if isinstance(extra, dict):
        soft_mode = bool(
            extra.get("soft_mode")
            or extra.get("soft")
            or extra.get("range_soft")
            or False
        )
        _sl_floor = extra.get("sl_floor_ratio")
        if isinstance(_sl_floor, (int, float)):
            sl_floor_ratio = float(_sl_floor)

    trade = open_position_with_tp_sl(
        settings=settings,
        symbol=symbol,
        side_open=side,
        qty=qty,
        entry_price_hint=float(last_price),
        tp_pct=tp_pct,
        sl_pct=sl_pct,
        source=signal_source,
        soft_mode=soft_mode,
        sl_floor_ratio=sl_floor_ratio,
    )

    if trade is None:
        send_skip_tg("[SKIP] open_position_failed: trade_object_is_none")
        log_skip_event(
            symbol=symbol,
            regime=regime_at_entry,
            source="entry_flow.open_position",
            side=chosen_signal,
            reason="open_position_failed_trade_none",
            extra={
                "qty": qty,
                "tp_pct": tp_pct,
                "sl_pct": sl_pct,
                "effective_risk_pct": effective_risk_pct,
            },
        )
        return None, 3.0

    # (8) bt_trades INSERT
    _create_trade_row_on_entry(
        trade=trade,
        symbol=symbol,
        latest_ts_ms=int(latest_ts),
        regime_at_entry=regime_at_entry,
        signal_source=signal_source,
        effective_risk_pct=effective_risk_pct,
        tp_pct=tp_pct,
        sl_pct=sl_pct,
        is_auto=True,
    )

    # (9) EntryScore 저장 (GPT 메타 포함)
    entry_score: Optional[float] = None
    components: Dict[str, Any] = {}

    try:
        entry_score, components = _compute_entry_score_components(
            signal_source=signal_source,
            chosen_signal=chosen_signal,
            effective_risk_pct=effective_risk_pct,
            leverage=leverage,
            tp_pct=tp_pct,
            sl_pct=sl_pct,
            extra=extra,
        )
    except ValueError as e:
        log(f"[ENTRY_SCORE] compute skipped on save: {e}")
        entry_score = None
        components = {}

    if entry_score is not None and components:
        # GPT/시그널 메타 정보 components_json 에 주입
        try:
            gpt_info: Dict[str, Any] = {
                "model": gpt_model_for_log,
                "action": gpt_action,
                "reason": gpt_reason,
                "decision": gpt_decision,
            }
            # signal_flow_ws 가 내려준 방향/레짐 정보도 함께 기록
            if isinstance(extra, dict):
                gpt_info["signal"] = {
                    "direction_raw": extra.get("direction_raw"),
                    "direction_norm": extra.get("direction_norm"),
                    "regime_level": extra.get("regime_level"),
                }
            components["gpt_entry"] = gpt_info

            # 시그널 extra 일부도 분석용으로 별도 블록에 보관(핵심 필드만 선별)
            if isinstance(extra, dict):
                signal_extra: Dict[str, Any] = {}
                allowed_keys = [
                    "signal_score",
                    "candidate_score",
                    "regime_level",
                    "gap_ratio_15m",
                    "width_ratio_5m",
                    "atr_fast",
                    "atr_slow",
                    "direction_raw",
                    "direction_norm",
                    "signal_reasons",
                    "block_reason",
                ]
                for k in allowed_keys:
                    if k in extra:
                        signal_extra[k] = extra[k]
                if signal_extra:
                    components["signal_extra"] = signal_extra
        except Exception as e:
            log(f"[ENTRY_SCORE] gpt_entry/signal_extra attach failed: {e}")

        _save_entry_score_to_db(
            symbol=symbol,
            ts_ms=int(latest_ts),
            side=chosen_signal,
            signal_type=signal_source,
            regime_at_entry=regime_at_entry,
            entry_score=entry_score,
            components=components,
            trade=trade,
            used_for_entry=True,
        )

    log(
        f"[ENTRY] opened: symbol={symbol} side={side} qty={qty:.8f} "
        f"lev={leverage} tp={tp_pct*100:.2f}% sl={sl_pct*100:.2f}%"
    )

    # 진입 후에는 짧은 쿨다운
    return trade, float(getattr(settings, "post_entry_sleep_sec", 5.0))
