"""
strategies_range_ws.py
웹소켓으로 받은 5m/15m 캔들을 기준으로 박스장(레인지) 전략을 판단하는 모듈.

2025-11-14 보강
----------------------------------------------------
1) signal_flow_ws 동시 중재(arbitration) 플로우와 인터페이스 정리.
   - decide_signal_range(...)는 방향("LONG" / "SHORT")만 판단하는 순수 엔진으로 유지.
   - TP/SL 계산과 soft 차단은 compute_range_params(...) 및 signal_flow_ws 쪽에서 결합.
2) should_block_range_today_with_level(...)의 반환 reason 규격화.
   - 강한 차단: "atr" / "ema" → RANGE 전략 자체를 막음.
   - 완화 차단: "soft_atr" / "soft_ema" → 신호는 허용하되 TP/SL 보수적으로 조정.
3) settings 접근은 전역적으로 getattr(..., default) 패턴을 사용해 ENV 누락 시 크래시 방지.

2025-11-13 추가 보정 (유지)
----------------------------------------------------
1) settings 연동 강화(하위호환):
   - decide_signal_range(..)에 optional settings 파라미터를 추가(기존 호출은 그대로 동작).
   - 상단/하단 경계는 settings.range_entry_upper_pct / range_entry_lower_pct를 우선 사용.
   - 최소 박스 폭 임계는 settings.range_box_min_width_pct(기본 0.0015 = 0.15%).
2) 로그 개선: 박스 폭 비율, 상·하단 경계값, 현재가를 상세 기록.
3) 안정성 보강: 음수/0 분모 방지, 결측 캔들 방어, NaN 방어.
4) compute_range_params(..):
   - soft_reason 시 TP 상한을 settings.range_soft_tp_factor로 제한(기본 1.2×tp_min).
   - 숏 SL 보정 시 settings.range_short_sl_floor_ratio(기본 0.75)를 사용.
5) should_block_range_today(..): optional settings 지원(ATR/EMA 임계 조정 가능).
   - ATR 압축 기준: fast < slow * settings.range_atr_fast_ratio_limit (기본 0.6)
   - 15m EMA 이격 기준: dist > settings.range_ema_dist_thresh (기본 0.01 = 1%)

기존 2025-11-13 변경 사항(요약)
----------------------------------------------------
1) 기존 버전은 최근 N개 3m 캔들로 박스를 만들었으나, BingX 환경에서 3m가 안정적으로
   오지 않는 문제가 있어 5m 기준으로 전부 변경했다.
   → 모든 함수 시그니처의 `candles_3m` 을 `candles_5m` 으로 교체.
2) 5m 캔들 개수/폭을 Render 콘솔에서 바로 확인할 수 있도록 `telelog.log(...)`를
   진입부에 넣었다.
3) 15m 캔들 역시 웹소켓 버퍼에서 가져오는 전제를 두기 때문에, 15m가 충분히 없으면
   막는 로직만 먼저 찍고 빠지도록 했다.
4) 박스 폭 판단, 상단 80% 숏 / 하단 20% 롱 규칙, 숏일 때 SL을 TP의 75% 이상으로 보정하는
   이전 완화 로직은 그대로 유지했다.
5) 주문/TP/SL 은 기존대로 REST 에서 처리한다.
"""

from __future__ import annotations

import math
from typing import List, Optional, Tuple, Dict, TYPE_CHECKING

from indicators import ema, calc_atr, Candle
from telelog import log

if TYPE_CHECKING:
    from settings_ws import BotSettings

# 타입 별칭: (ts, open, high, low, close)
Candles = List[Candle]


# ─────────────────────────────
# 내부 유틸: 캔들 정제
# ─────────────────────────────
def _clean_candles(candles: Candles) -> Candles:
    """(ts, o, h, l, c[, v]) 형태가 아닌 값/결측을 배제하고 반환한다.

    - 숫자로 캐스팅이 안 되거나
    - NaN / 0 / 음수 close 가 포함된 행은 버린다.
    → RANGE 로직 전체에서 공통으로 사용되는 방어 필터.
    """
    cleaned: Candles = []
    for c in candles or []:
        try:
            ts, o, h, l, cclose = c[0], float(c[1]), float(c[2]), float(c[3]), float(c[4])
            if cclose > 0 and not (
                math.isnan(o) or math.isnan(h) or math.isnan(l) or math.isnan(cclose)
            ):
                cleaned.append((ts, o, h, l, cclose))
        except Exception:
            # 개별 캔들 오류는 전체 전략에 영향을 주지 않게 조용히 스킵
            continue
    return cleaned


# ─────────────────────────────
# 기본 박스장 신호 (5m 기준)
# ─────────────────────────────
def decide_signal_range(
    candles_5m: Candles,
    lookback: int = 40,
    settings: Optional["BotSettings"] = None,
) -> Optional[str]:
    """5분봉 캔들로 박스장 상단/하단을 판단해서 롱/숏을 결정한다.

    반환값:
        "LONG" / "SHORT" / None

    - 최근 `lookback` 개 5m 캔들의 high/low 로 박스 폭을 계산.
    - 박스 폭 비율이 너무 작으면(너무 좁은 박스) RANGE 전략 자체를 스킵.
    - 상단 퍼센타일 이상이면 SHORT, 하단 퍼센타일 이하이면 LONG.
    - 퍼센타일/폭 임계는 settings 로 오버라이드 가능.
    """
    candles_5m = _clean_candles(candles_5m)
    log(f"[RANGE] (WS) decide_signal_range 5m_count={len(candles_5m)} lookback={lookback}")
    if len(candles_5m) < lookback:
        return None

    recent = candles_5m[-lookback:]
    hi = max(c[2] for c in recent)  # high
    lo = min(c[3] for c in recent)  # low
    now_price = float(candles_5m[-1][4])

    if lo <= 0 or math.isnan(now_price):
        return None

    box_h = hi - lo
    box_pct = box_h / lo if lo > 0 else 0.0

    # 최소 박스폭 임계 (기본 0.15%)
    box_min_pct = (
        float(getattr(settings, "range_box_min_width_pct", 0.0015)) if settings else 0.0015
    )
    if box_pct < box_min_pct:
        log(f"[RANGE] (WS) box too narrow pct={box_pct:.6f} < {box_min_pct}")
        return None

    # 상단/하단 퍼센타일 (기본 80% / 20%)
    up_pct = float(getattr(settings, "range_entry_upper_pct", 0.80)) if settings else 0.80
    lo_pct = float(getattr(settings, "range_entry_lower_pct", 0.20)) if settings else 0.20
    # 방어적 클램프 (완전 상단/하단으로 붙는 값 방지)
    up_pct = max(0.5, min(0.99, up_pct))
    lo_pct = max(0.01, min(0.5, lo_pct))

    upper_line = lo + box_h * up_pct
    lower_line = lo + box_h * lo_pct

    log(
        f"[RANGE] (WS) box%={box_pct:.6f} lo={lo:.2f} hi={hi:.2f} now={now_price:.2f} "
        f"upper@{up_pct:.2f}→{upper_line:.2f} lower@{lo_pct:.2f}→{lower_line:.2f}"
    )

    # 상단에 닿으면 숏, 하단에 닿으면 롱 후보로 해석
    if now_price >= upper_line:
        log(f"[RANGE] (WS) SHORT zone hit price={now_price} upper_line={upper_line}")
        return "SHORT"
    if now_price <= lower_line:
        log(f"[RANGE] (WS) LONG zone hit price={now_price} lower_line={lower_line}")
        return "LONG"

    return None


# ─────────────────────────────
# 박스장 TP/SL 계산 보조
# ─────────────────────────────
def compute_range_params(
    direction: str,
    candles_5m: Candles,
    settings: Optional["BotSettings"],
    lookback_for_vol: int = 20,
    soft_reason: Optional[str] = None,
) -> Dict[str, float]:
    """박스 진입 방향에 따라 TP/SL 을 계산해서 돌려주는 보조 유틸.

    반환 예:
        {"tp_pct": 0.0042, "sl_pct": 0.0035}

    - settings 가 있으면 range_tp_* / range_sl_* 로 기본값을 오버라이드.
    - use_range_dynamic_tp 가 켜져 있으면 최근 평균 고저폭을 기준으로 TP 를 동적으로 계산.
    - soft_reason 이 들어오면 TP 상한을 range_soft_tp_factor × tp_min 으로 제한.
    - SHORT 일 때는 SL 이 지나치게 짧지 않도록 range_short_sl_floor_ratio 로 바닥을 깐다.
    """
    candles_5m = _clean_candles(candles_5m)

    # 기본값 (settings 없을 때도 동작하도록)
    base_tp = 0.006
    base_sl = 0.004

    if settings is not None:
        # 방향별 기본 TP/SL
        if direction == "LONG":
            base_tp = float(
                getattr(settings, "range_tp_long_pct", getattr(settings, "range_tp_pct", 0.006))
            )
            base_sl = float(
                getattr(settings, "range_sl_long_pct", getattr(settings, "range_sl_pct", 0.004))
            )
        else:  # SHORT
            base_tp = float(
                getattr(settings, "range_tp_short_pct", getattr(settings, "range_tp_pct", 0.006))
            )
            base_sl = float(
                getattr(settings, "range_sl_short_pct", getattr(settings, "range_sl_pct", 0.004))
            )

        # 동적 TP (최근 평균 고저폭을 비율화)
        if getattr(settings, "use_range_dynamic_tp", False) and len(candles_5m) >= lookback_for_vol:
            recent = candles_5m[-lookback_for_vol:]
            avg_hl = sum((float(c[2]) - float(c[3])) for c in recent) / float(lookback_for_vol)
            last_close = float(candles_5m[-1][4])
            if last_close > 0 and not math.isnan(last_close):
                dyn_tp = avg_hl / last_close
                tp_min = float(getattr(settings, "range_tp_min", 0.0035))
                tp_max = float(getattr(settings, "range_tp_max", 0.0065))
                dyn_tp = max(tp_min, min(tp_max, dyn_tp))
                base_tp = dyn_tp

        # soft 허용이면 좀 더 보수적으로 (tp ≤ tp_min * soft_factor)
        if soft_reason:
            tp_min = float(getattr(settings, "range_tp_min", 0.0035)) if settings else 0.0035
            soft_factor = float(getattr(settings, "range_soft_tp_factor", 1.2)) if settings else 1.2
            base_tp = min(base_tp, tp_min * soft_factor)

    # 숏일 때 SL 하한을 TP의 ratio 로 보정해, 너무 가까운 SL 을 방지
    if direction == "SHORT":
        ratio = float(getattr(settings, "range_short_sl_floor_ratio", 0.75)) if settings else 0.75
        widened_sl = base_tp * float(ratio)
        base_sl = max(base_sl, widened_sl)

    return {"tp_pct": float(base_tp), "sl_pct": float(base_sl)}


# ─────────────────────────────
# 박스장 자체를 오늘은 막을지 판단 (5m/15m)
# ─────────────────────────────
def should_block_range_today(
    candles_5m: Candles,
    candles_15m: Candles,
    settings: Optional["BotSettings"] = None,
) -> bool:
    """5m / 15m 기반 간단 차단.

    1) 5m ATR 이 너무 죽어 있으면 막는다.
    2) 15m EMA 이격이 너무 크면 막는다.
    """
    candles_5m = _clean_candles(candles_5m)
    candles_15m = _clean_candles(candles_15m)

    atr_fast_ratio_limit = (
        float(getattr(settings, "range_atr_fast_ratio_limit", 0.6)) if settings else 0.6
    )
    ema_dist_thresh = float(getattr(settings, "range_ema_dist_thresh", 0.01)) if settings else 0.01

    # ATR 기반: 5m 변동성이 너무 작으면 RANGE 를 막는다.
    atr_fast = calc_atr(candles_5m, 14)
    atr_slow = calc_atr(candles_5m, 40)
    if atr_fast and atr_slow and atr_slow > 0:
        if atr_fast < atr_slow * atr_fast_ratio_limit:
            log(
                f"[RANGE_BLOCK] (WS) ATR compressed: fast={atr_fast:.6f} slow={atr_slow:.6f} "
                f"→ fast < slow*{atr_fast_ratio_limit}"
            )
            return True

    # 15m EMA 기반: 이미 추세장이 너무 강하면 RANGE 를 막는다.
    if candles_15m:
        closes_15 = [float(c[4]) for c in candles_15m]
        if len(closes_15) >= 50:
            e20_15 = ema(closes_15, 20)
            e50_15 = ema(closes_15, 50)
            if not math.isnan(e20_15[-1]) and not math.isnan(e50_15[-1]) and e50_15[-1] != 0:
                dist = abs(e20_15[-1] - e50_15[-1]) / e50_15[-1]
                if dist > ema_dist_thresh:
                    log(
                        f"[RANGE_BLOCK] (WS) 15m EMA distance too wide: dist={dist:.6f} > {ema_dist_thresh}"
                    )
                    return True

    return False


# ─────────────────────────────
# 박스장 차단을 단계적으로 할 수 있는 버전 (5m/15m)
# ─────────────────────────────
def should_block_range_today_with_level(
    candles_5m: Candles,
    candles_15m: Candles,
    settings: Optional["BotSettings"],
) -> Tuple[bool, str]:
    """박스장 차단을 레벨에 따라 다르게 주고 싶을 때 사용한다.

    반환값:
        (blocked: bool, reason: str)

    - level 미지정(settings=None): should_block_range_today 와 동일한 하드 차단.
    - level == 0: ATR/EMA 조건을 만족하면 무조건 차단.
    - level == 1: soft_* reason 으로만 반환(신호는 허용, TP/SL 은 보수적으로).
    - level >= 2: 다시 강하게 차단.
    """
    candles_5m = _clean_candles(candles_5m)
    candles_15m = _clean_candles(candles_15m)

    atr_fast_ratio_limit = (
        float(getattr(settings, "range_atr_fast_ratio_limit", 0.6)) if settings else 0.6
    )
    ema_dist_thresh = float(getattr(settings, "range_ema_dist_thresh", 0.01)) if settings else 0.01

    atr_fast = calc_atr(candles_5m, 14)
    atr_slow = calc_atr(candles_5m, 40)
    atr_block = False
    if atr_fast and atr_slow and atr_slow > 0:
        if atr_fast < atr_slow * atr_fast_ratio_limit:
            atr_block = True

    ema_block = False
    dist = 0.0
    if candles_15m:
        closes_15 = [float(c[4]) for c in candles_15m]
        if len(closes_15) >= 50:
            e20_15 = ema(closes_15, 20)
            e50_15 = ema(closes_15, 50)
            if not math.isnan(e20_15[-1]) and not math.isnan(e50_15[-1]) and e50_15[-1] != 0:
                dist = abs(e20_15[-1] - e50_15[-1]) / e50_15[-1]
                if dist > ema_dist_thresh:
                    ema_block = True

    # settings 없음: 단순 하드 차단 모드
    if settings is None:
        if atr_block:
            log(
                f"[RANGE_BLOCK] (WS) ATR compressed: fast={atr_fast:.6f} slow={atr_slow:.6f}"
            )
            return True, "atr"
        if ema_block:
            log(
                f"[RANGE_BLOCK] (WS) 15m EMA distance too wide: dist={dist:.6f} > {ema_dist_thresh}"
            )
            return True, "ema"
        return False, ""

    level = int(getattr(settings, "range_strict_level", 0))

    # level 0: 강한 차단(ATR/EMA 조건 만족 시 바로 막음)
    if level == 0:
        if atr_block:
            log(
                f"[RANGE_BLOCK] (WS) ATR compressed (level 0): fast={atr_fast:.6f} slow={atr_slow:.6f}"
            )
            return True, "atr"
        if ema_block:
            log(
                f"[RANGE_BLOCK] (WS) 15m EMA distance too wide (level 0): dist={dist:.6f} > {ema_dist_thresh}"
            )
            return True, "ema"
        return False, ""

    # level 1: soft 차단(신호는 허용하되 reason 으로만 표시)
    if level == 1:
        if atr_block:
            log(
                f"[RANGE_SOFT] (WS) ATR compressed but allowed (level 1): fast={atr_fast:.6f} slow={atr_slow:.6f}"
            )
            return False, "soft_atr"
        if ema_block:
            log(
                f"[RANGE_SOFT] (WS) 15m EMA distance wide but allowed (level 1): dist={dist:.6f} > {ema_dist_thresh}"
            )
            return False, "soft_ema"
        return False, ""

    # level 2 이상은 다시 강한 차단
    if atr_block:
        log(
            f"[RANGE_BLOCK] (WS) ATR compressed (level {level}): fast={atr_fast:.6f} slow={atr_slow:.6f}"
        )
        return True, "atr"
    if ema_block:
        log(
            f"[RANGE_BLOCK] (WS) 15m EMA distance too wide (level {level}): dist={dist:.6f} > {ema_dist_thresh}"
        )
        return True, "ema"

    return False, ""


__all__ = [
    "decide_signal_range",
    "should_block_range_today",
    "compute_range_params",
    "should_block_range_today_with_level",
]
