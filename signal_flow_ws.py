"""
signal_flow_ws.py (simultaneous arbitration)
====================================================
시그널 결정 전담 모듈 (웹소켓 기반, 동시 평가 + 중재 버전).
run_bot.py 에서 한 줄로 호출해서 시그널/캔들 세트를 받아가게 한다.

PATCH NOTES — 2025-11-14
----------------------------------------------------
A) TREND/RANGE **동시 평가 + 중재(Arbitration)** 도입
   - 한 루프마다 TREND/RANGE 후보를 동시에 산출 → 중재 규칙으로 단일 결론 결정
   - 방향 동일 시 HYBRID: 점수 높은 쪽 TP/SL 우선
   - 방향 충돌 시 점수 비교 + 히스테리시스(`settings.arbitration_hysteresis`, 기본 0.25) 적용
B) 후보 점수 정의
   - TREND: 15m EMA20·EMA50 갭 비율(갭/종가) = 추세 강도
   - RANGE: 5m 박스 폭 비율의 역수(1/width_ratio) = 압축 강도
C) 쿨다운/차단을 후보 단계에서 반영
   - TREND: `cooldown_after_close_trend` 경과 전이면 후보 무효
   - RANGE: `should_block_range_today_with_level`(있으면)·`range` 쿨다운을 후보 무효로 처리
D) TP/SL 통일 출력
   - 최종 선택된 전략에 대해 `extra['tp_pct']`, `extra['sl_pct']`를 항상 채워 반환
E) 기존 1m 확인 로직과 메시지 **그대로 유지**
   - 1m 불일치 시 텔레그램: "[SKIP] 1m_confirm_mismatch" / CSV reason: "1m_confirm_mismatch"
   - RANGE 전용 토글: `enable_1m_confirm_range`(없으면 `enable_1m_confirm` 하위호환)
F) 설정 키 미존재 대비 안전장치
   - 코드 전역에서 `getattr(settings, ..., default)` 패턴으로 누락된 ENV/필드를 안전 처리(운영 중 회귀 방지)

PATCH NOTES — 2025-11-13 (이전 패치 유지)
----------------------------------------------------
1) 1분 확인 불일치 알림/사유 **단일 키로 통일**
   - 텔레그램 메시지: "[SKIP] 1m_confirm_mismatch"
   - CSV reason: "1m_confirm_mismatch"
   - 효과: telelog 쿨다운이 문자열별로 쪼개지지 않고 **한 번만** 적용 → 스팸 완화
2) RANGE 전용 스위치 추가(하위호환)
   - `settings.enable_1m_confirm_range` (없으면 기존 `enable_1m_confirm` 값 사용)
3) 관련 위치 주석 보강
   - 변경 이유와 쿨다운 효과, 하위호환 동작 명시
4) RANGE 신호 호출부가 settings 인자 전달
   - `decide_signal_range(candles_5m, settings=settings)` → 상·하단 퍼센타일 ENV 반영

기존 변경사항(요약)
----------------------------------------------------
- 3m 의존 제거 → 1m/5m/15m만 사용
- WS 버퍼 사용, 5m 지연 검사
"""

from __future__ import annotations

import time
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from telelog import log, send_skip_tg
from signals_logger import log_signal
# 웹소켓 버퍼에서 캔들을 읽는다
from market_data_ws import (
    get_klines as ws_get_klines,
    get_klines_with_volume as ws_get_klines_with_volume,
)
from indicators import calc_atr
from strategies_trend_ws import (
    decide_trend_15m,      # 15m 방향("BUY"/"SELL") 판단기
    confirm_1m_direction,  # 1m 확인
)

# RANGE 모듈은 최신/구버전 둘 다 대응
try:
    from strategies_range_ws import (
        decide_signal_range,                    # decide_signal_range(candles_5m, settings=...)
        should_block_range_today,
        compute_range_params,
        should_block_range_today_with_level,
    )
except ImportError:
    from strategies_range_ws import (
        decide_signal_range,
        should_block_range_today,
    )
    compute_range_params = None  # type: ignore
    should_block_range_today_with_level = None  # type: ignore


# ─────────────────────────────
# 보조: RANGE 1분 확인 스위치(하위호환)
# ─────────────────────────────
def _is_range_1m_confirm_enabled(settings: Any) -> bool:
    """RANGE에서 1분 확인을 쓸지 결정한다.
    - settings.enable_1m_confirm_range 가 있으면 그 값을 우선.
    - 없으면 기존 enable_1m_confirm 값을 그대로 따른다(하위호환).
    """
    return bool(getattr(settings, "enable_1m_confirm_range", getattr(settings, "enable_1m_confirm", False)))


# ─────────────────────────────
# 보조: EMA/시리즈 헬퍼
# ─────────────────────────────
@dataclass
class Candidate:
    kind: str                # "TREND" / "RANGE"
    side: str                # "BUY" / "SELL"
    score: float             # 중재 비교용 점수 (0~∞)
    tp_pct: float
    sl_pct: float
    reasons: List[str]


def _ema(values: List[float], period: int) -> Optional[float]:
    if not values or len(values) < period:
        return None
    k = 2.0 / (period + 1)
    ema = values[-period]
    for v in values[-period+1:]:
        ema = v * k + ema * (1 - k)
    return ema


def _close_series(candles: List[List[float]]) -> List[float]:
    # 캔들 포맷: [ts, open, high, low, close, (vol)]
    return [float(c[4]) for c in candles]


def _price(candles: List[List[float]]) -> Optional[float]:
    return float(candles[-1][4]) if candles else None


def _range_width_ratio_5m(candles_5m: List[List[float]], window: int = 12) -> Optional[float]:
    if len(candles_5m) < window:
        return None
    seg = candles_5m[-window:]
    hi = max(float(c[2]) for c in seg)
    lo = min(float(c[3]) for c in seg)
    last = float(seg[-1][4])
    if last <= 0:
        return None
    return (hi - lo) / last


def _decide_signal_5m_trend(candles_5m: List[Tuple[int, float, float, float, float]]) -> Optional[str]:
    """아주 단순한 5m 추세판단.
    - 최근 3개 캔들의 종가가 전부 올라가면 "BUY"
    - 최근 3개 캔들의 종가가 전부 내려가면 "SELL"
    - 아니면 None
    (기존 3m 대체용 임시 로직)
    """
    if len(candles_5m) < 4:
        return None
    closes = [c[4] for c in candles_5m[-4:]]  # 가장 최신이 뒤
    if closes[1] < closes[2] < closes[3]:
        return "BUY"
    if closes[1] > closes[2] > closes[3]:
        return "SELL"
    return None


# ─────────────────────────────
# 후보 생성기: TREND
# ─────────────────────────────
def _trend_candidate(
    *,
    settings: Any,
    candles_5m: List[List[float]],
    candles_15m: List[List[float]],
    latest_5m_ts: int,
    last_trend_close_ts: float,
) -> Optional[Candidate]:
    # 쿨다운
    if (time.time() - last_trend_close_ts) < float(getattr(settings, "cooldown_after_close_trend", 0)):
        send_skip_tg("[SKIP] trend_cooldown: 직전 TREND 포지션 대기중")
        log_signal(event="SKIP", symbol=settings.symbol, strategy_type="TREND", reason="trend_cooldown", candle_ts=latest_5m_ts)
        return None

    closes_15 = _close_series(candles_15m)
    last_15 = closes_15[-1] if closes_15 else None
    if not last_15:
        log("[TREND_SKIP] 15m 방향 판단 불가")
        log_signal(event="SKIP", symbol=settings.symbol, strategy_type="TREND", reason="trend_15m_unknown", candle_ts=latest_5m_ts)
        return None

    # 5m 방향
    sig_5m = _decide_signal_5m_trend(candles_5m)
    trend_15m_val = decide_trend_15m(candles_15m)

    # 5m/15m 정합성 검사(완화: 5m 없음→15m만으로 허용)
    final_dir: Optional[str] = None
    if sig_5m and trend_15m_val and sig_5m == trend_15m_val:
        final_dir = sig_5m
    elif (not sig_5m) and trend_15m_val:
        final_dir = trend_15m_val
        log("[TREND_SOFT] 5m 없음 → 15m 방향만으로 후보 허용")
    else:
        # 불일치/부재 사유 남기기만 하고 후보 무효
        if not sig_5m:
            log_signal(event="SKIP", symbol=settings.symbol, strategy_type="TREND", reason="trend_5m_no_signal", candle_ts=latest_5m_ts)
        elif not trend_15m_val:
            log_signal(event="SKIP", symbol=settings.symbol, strategy_type="TREND", reason="trend_15m_unknown", candle_ts=latest_5m_ts)
        else:
            log_signal(event="SKIP", symbol=settings.symbol, strategy_type="TREND", reason="trend_5m_15m_mismatch", candle_ts=latest_5m_ts)
        return None

    # 1m 확인 (TREND는 전역 enable_1m_confirm 사용)
    if bool(getattr(settings, "enable_1m_confirm", False)):
        log(f"[SIGNAL] (WS) fetch 1m candles for {settings.symbol} (trend confirm) limit=40")
        candles_1m = ws_get_klines(settings.symbol, "1m", 40)
        if not confirm_1m_direction(candles_1m, final_dir):
            send_skip_tg("[SKIP] 1m_confirm_mismatch")
            log_signal(event="SKIP", symbol=settings.symbol, strategy_type="TREND", direction=final_dir, reason="1m_confirm_mismatch", candle_ts=latest_5m_ts)
            return None

    # 점수: 15m EMA 갭 비율
    ema20 = _ema(closes_15, 20)
    ema50 = _ema(closes_15, 50)
    if not ema20 or not ema50 or last_15 <= 0:
        return None
    gap_ratio = abs(ema20 - ema50) / last_15
    thr = float(getattr(settings, "trend_ema_gap_min_ratio_15m", 0.003))
    if gap_ratio < thr:
        return None

    # TP/SL 기본값 (settings 에 없으면 안전 기본값 사용)
    tp_pct = float(getattr(settings, "trend_tp_pct", 0.006))
    sl_pct = float(getattr(settings, "trend_sl_pct", 0.004))

    return Candidate(kind="TREND", side=final_dir, score=gap_ratio, tp_pct=tp_pct, sl_pct=sl_pct,
                     reasons=[f"trend gap_ratio={gap_ratio:.4f} (>= {thr:.4f})"])


# ─────────────────────────────
# 후보 생성기: RANGE
# ─────────────────────────────
def _range_candidate(
    *,
    settings: Any,
    candles_5m: List[List[float]],
    candles_15m: Optional[List[List[float]]],
    latest_5m_ts: int,
    last_range_close_ts: float,
) -> Optional[Candidate]:
    # 쿨다운
    if (time.time() - last_range_close_ts) < float(getattr(settings, "cooldown_after_close_range", 0)):
        send_skip_tg("[SKIP] range_cooldown: 직전 RANGE 포지션 대기중")
        log_signal(event="SKIP", symbol=settings.symbol, strategy_type="RANGE", reason="range_cooldown", candle_ts=latest_5m_ts)
        return None

    # 일/세션 차단 규칙
    blocked_now = False
    block_reason = ""
    try:
        if should_block_range_today_with_level is not None:
            blocked_now, block_reason = should_block_range_today_with_level(candles_5m, candles_15m or [], settings)
        else:
            blocked_now = should_block_range_today(candles_5m, candles_15m or [])
    except Exception as e:
        log(f"[RANGE block error] {e}")

    if blocked_now and not str(block_reason).startswith("soft_"):
        send_skip_tg("[SKIP] range_blocked_today: 박스장 조건 불리")
        log_signal(event="SKIP", symbol=settings.symbol, strategy_type="RANGE", reason="range_blocked_today", candle_ts=latest_5m_ts)
        return None

    # 방향 산출 (settings 연동 필수)
    r_dir = decide_signal_range(candles_5m, settings=settings)
    if not r_dir:
        return None

    # 1m 확인 (RANGE 전용 토글)
    if _is_range_1m_confirm_enabled(settings):
        log(f"[SIGNAL] (WS) fetch 1m candles for {settings.symbol} (range confirm) limit=40")
        candles_1m = ws_get_klines(settings.symbol, "1m", 40)
        if not confirm_1m_direction(candles_1m, r_dir):
            send_skip_tg("[SKIP] 1m_confirm_mismatch")
            log_signal(event="SKIP", symbol=settings.symbol, strategy_type="RANGE", direction=r_dir, reason="1m_confirm_mismatch", candle_ts=latest_5m_ts)
            return None

    # 폭/점수 계산
    width_window = int(getattr(settings, "range_window_5m", 12))
    width_ratio = _range_width_ratio_5m(candles_5m, window=width_window)
    if width_ratio is None:
        return None
    width_thr = float(getattr(settings, "range_max_width_ratio_5m", 0.004))
    if width_ratio > width_thr:
        return None
    score = 1.0 / max(1e-6, width_ratio)

    # TP/SL: 엔진 있으면 우선
    tp_pct = float(getattr(settings, "range_tp_pct", 0.006))
    sl_pct = float(getattr(settings, "range_sl_pct", 0.004))
    try:
        if compute_range_params is not None:
            params = compute_range_params(r_dir, candles_5m, settings) or {}
            tp_pct = float(params.get("tp_pct", tp_pct))
            sl_pct = float(params.get("sl_pct", sl_pct))
    except Exception as e:
        log(f"[RANGE params error] {e}")

    # soft 차단이면 완화 계수 적용(로그용 reason 채움)
    if str(block_reason).startswith("soft_"):
        soft = float(getattr(settings, "range_soft_tp_factor", 0.7))
        tp_pct *= soft
        sl_pct *= soft
        log_signal(event="ENTRY_SIGNAL", symbol=settings.symbol, strategy_type="RANGE", direction=r_dir, reason=f"range_soft_{block_reason}", candle_ts=latest_5m_ts)

    return Candidate(kind="RANGE", side=r_dir, score=score, tp_pct=tp_pct, sl_pct=sl_pct,
                     reasons=[f"range width_ratio={width_ratio:.4f} (<= {width_thr:.4f})"])


# ─────────────────────────────
# 중재기
# ─────────────────────────────
def _arbitrate(tr: Optional[Candidate], rg: Optional[Candidate], hysteresis: float) -> Tuple[Optional[Candidate], Optional[str]]:
    """
    - 둘 다 없음 → (None, None)
    - 한쪽만 유효 → (that, that.kind)
    - 둘 다 유효:
        - 같은 방향 → HYBRID (점수 높은 쪽을 베이스)
        - 반대 방향 → 점수 비교(+히스테리시스)
    """
    if tr is None and rg is None:
        return None, None
    if tr and not rg:
        return tr, "TREND"
    if rg and not tr:
        return rg, "RANGE"

    # 둘 다 있음
    if tr.side == rg.side:
        base = tr if tr.score >= rg.score else rg
        return base, "HYBRID"

    # 방향 충돌 → 히스테리시스 비교
    if tr.score >= rg.score * (1.0 + hysteresis):
        return tr, "TREND"
    if rg.score >= tr.score * (1.0 + hysteresis):
        return rg, "RANGE"
    return None, None  # 차이 미미 → 보류


# ─────────────────────────────
# 공개 API
# ─────────────────────────────
def get_trading_signal(
    *,
    settings: Any,
    last_trend_close_ts: float,
    last_range_close_ts: float,
) -> Optional[Tuple[str, str, int, List[Any], List[Any], float, Dict[str, Any]]]:
    """시그널을 하나 결정해서 관련 캔들/부가정보와 함께 넘긴다.
    이제는 TREND/RANGE 동시 평가 후 중재로 단일 결론을 내린다.
    return: (chosen_signal, signal_source, latest_5m_ts, candles_5m, candles_5m_raw, last_price, extra)
    """
    symbol = settings.symbol

    # 1) 5m 캔들 (거래량 포함)
    log(f"[SIGNAL] (WS) fetch 5m candles for {symbol} limit=120")
    candles_5m_raw = ws_get_klines_with_volume(symbol, "5m", 120)
    if not candles_5m_raw or len(candles_5m_raw) < 50:
        log("[SIGNAL] 5m candles not enough (<50) → skip signal")
        return None
    candles_5m = [c[:5] for c in candles_5m_raw]
    latest_5m_ts = int(candles_5m[-1][0])
    last_price = float(candles_5m[-1][4])

    # 2) 캔들 지연 체크
    now_ms = int(time.time() * 1000)
    if now_ms - latest_5m_ts > int(getattr(settings, "max_kline_delay_sec", 10)) * 1000:
        send_skip_tg("[SKIP] 5m_kline_delayed: 최근 5m 캔들이 지연되었습니다.")
        log_signal(event="SKIP", symbol=symbol, strategy_type="UNKNOWN", reason="5m_kline_delayed", candle_ts=latest_5m_ts)
        return None

    # 3) 15m 캔들(추세/박스 보조)
    log(f"[SIGNAL] (WS) fetch 15m candles for {symbol} limit=120")
    candles_15m = ws_get_klines(symbol, "15m", 120)

    # 4) 후보 동시 산출
    trend_cand = _trend_candidate(settings=settings, candles_5m=candles_5m, candles_15m=candles_15m,
                                  latest_5m_ts=latest_5m_ts, last_trend_close_ts=last_trend_close_ts) if bool(getattr(settings, "enable_trend", True)) else None
    range_cand = _range_candidate(settings=settings, candles_5m=candles_5m, candles_15m=candles_15m,
                                  latest_5m_ts=latest_5m_ts, last_range_close_ts=last_range_close_ts) if bool(getattr(settings, "enable_range", True)) else None

    # 5) 중재
    hys = float(getattr(settings, "arbitration_hysteresis", 0.25))
    chosen, label = _arbitrate(trend_cand, range_cand, hys)
    if not chosen:
        rs: List[str] = []
        if trend_cand: rs.append(f"TREND({trend_cand.side}) s={trend_cand.score:.4f}")
        if range_cand: rs.append(f"RANGE({range_cand.side}) s={range_cand.score:.4f}")
        if not rs: rs = ["no-candidate"]
        log(f"[DECIDE] no-entry (arbitration) {', '.join(rs)}")
        log_signal(event="SKIP", symbol=symbol, strategy_type="UNKNOWN", reason="no_entry_arbitration", candle_ts=latest_5m_ts, extra=", ".join(rs))
        return None

    # 6) HYBRID 시 TP/SL 소스 선택(점수 높은 후보 우선)
    tp_pct = chosen.tp_pct
    sl_pct = chosen.sl_pct
    if label == "HYBRID" and (trend_cand and range_cand):
        if trend_cand.score >= range_cand.score:
            tp_pct, sl_pct = trend_cand.tp_pct, trend_cand.sl_pct
        else:
            tp_pct, sl_pct = range_cand.tp_pct, range_cand.sl_pct

    # 7) ATR 기반 리스크 정보 (5m 기준)
    extra: Dict[str, Any] = {"tp_pct": tp_pct, "sl_pct": sl_pct}
    if bool(getattr(settings, "use_atr", False)):
        atr_len = int(getattr(settings, "atr_len", 14))
        atr_fast = calc_atr(candles_5m, atr_len)
        atr_slow = calc_atr(candles_5m, max(atr_len * 2, atr_len + 10))
        extra["atr_fast"], extra["atr_slow"] = atr_fast, atr_slow
        if (atr_fast and atr_slow and atr_slow > 0 and atr_fast > atr_slow * float(getattr(settings, "atr_risk_high_mult", 1.8))):
            extra["effective_risk_pct"] = float(getattr(settings, "risk_pct", 0.01)) * float(getattr(settings, "atr_risk_reduction", 0.5))

    log(f"[DECIDE] {label} {chosen.side} tp={tp_pct:.4f} sl={sl_pct:.4f} last={last_price}")
    return (
        chosen.side,     # chosen_signal: "BUY" / "SELL"
        label,           # signal_source: "TREND" / "RANGE" / "HYBRID"
        latest_5m_ts,
        candles_5m,
        candles_5m_raw,
        last_price,
        extra,
    )


__all__ = [
    "get_trading_signal",
]
