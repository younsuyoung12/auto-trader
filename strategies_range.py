"""strategies_range.py
박스장(레인지) 전략 전용 모듈.

이 모듈이 하는 일:
1. 최근 N개 3분봉에서 박스가 형성돼 있는지 본다.
2. 박스 상단 쪽이면 숏, 하단 쪽이면 롱 신호를 낸다.
3. 너무 좁은 박스는 무시한다.
4. ATR 수축/15분 이격 과다일 때는 '오늘은 박스 안 쓰자' 라고 신호를 낼 수 있게 한다.

2025-11-12 추가 (두 번째 보완)
----------------------------------------------------
1) 진입 구간을 더 끝으로 밀어 상단 80%↑ 숏, 하단 20%↓ 롱으로 조정
   → 들어가자마자 SL 나는 일 줄이기 위함.
2) 숏일 때 SL 을 TP 의 75% 이상으로 보정해서 윗꼬리 한 번에 바로 잘리는 것 완화.
3) soft 허용일 때(= 오늘은 애매하지만 막진 않을 때) TP 를 살짝 보수적으로 만드는 옵션 추가.

2025-11-12 추가 (타입/안전 보강)
----------------------------------------------------
- settings 에서 옵션을 읽을 때, 해당 속성이 없으면 AttributeError 나지 않도록
  getattr(..., getattr(..., 숫자)) 형태로 2단계 기본값을 준다.
  (range_tp_long_pct 가 없고 range_tp_pct 도 없을 때를 대비)
- TYPE_CHECKING 으로만 BotSettings 를 참조해서 런타임 의존성 제거.

2025-11-10 완화
----------------------------------------------------
- 15m EMA 이격 기준을 0.3% → 1% 로 올려서 박스가 너무 쉽게 막히지 않게 함.
- ATR 수축 기준은 0.6 그대로.
- 왜 막혔는지는 log(...) 로 콘솔에 남김.
"""

from __future__ import annotations

import math
from typing import List, Optional, Tuple, Dict, TYPE_CHECKING

from indicators import ema, calc_atr, Candle
from telelog import log  # 왜 막혔는지 콘솔에서 바로 보려고 추가

# 타입체커용: 실제 런타임에서는 안 가져와도 되게
if TYPE_CHECKING:
    from settings import BotSettings

# 타입 별칭
Candles = List[Candle]


# ─────────────────────────────
# 기본 박스장 신호
# ─────────────────────────────
def decide_signal_range(candles_3m: Candles, lookback: int = 40) -> Optional[str]:
    """3분봉 캔들로 박스장 상단/하단을 판단해서 롱/숏을 결정한다.

    반환값:
        "LONG" / "SHORT" / None
    """
    if len(candles_3m) < lookback:
        return None

    # 최근 3m 구간에서 박스 높이 계산
    recent = candles_3m[-lookback:]
    hi = max(c[2] for c in recent)  # high
    lo = min(c[3] for c in recent)  # low
    now_price = candles_3m[-1][4]

    if lo == 0:
        return None

    box_h = hi - lo
    box_pct = box_h / lo

    # 박스 폭이 너무 좁으면 박스로 보지 않는다 (0.15% 미만)
    if box_pct < 0.0015:
        return None

    # ── 변경점: 더 끝에서만 진입하도록 상하단 기준을 80% / 20% 로 조정 ──
    upper_line = lo + box_h * 0.80  # 0.75 → 0.80
    lower_line = lo + box_h * 0.20  # 0.25 → 0.20

    # 상단으로 치우쳐 있으면 숏
    if now_price >= upper_line:
        return "SHORT"
    # 하단으로 치우쳐 있으면 롱
    if now_price <= lower_line:
        return "LONG"

    return None


# ─────────────────────────────
# 박스장 TP/SL 계산 보조
# ─────────────────────────────
def compute_range_params(
    direction: str,
    candles_3m: Candles,
    settings: Optional["BotSettings"],
    lookback_for_vol: int = 20,
    soft_reason: Optional[str] = None,
) -> Dict[str, float]:
    """박스 진입 방향에 따라 TP/SL 을 계산해서 돌려주는 보조 유틸.

    반환 예:
        {"tp_pct": 0.0042, "sl_pct": 0.0035}
    """
    # 기본값 (settings 없음)
    base_tp = 0.006
    base_sl = 0.004

    if settings is not None:
        # 방향별 기본값을 “있으면 쓰고, 없으면 공통값, 그것도 없으면 숫자” 순서로 안전하게 읽는다.
        if direction == "LONG":
            base_tp = getattr(
                settings,
                "range_tp_long_pct",
                getattr(settings, "range_tp_pct", 0.006),
            )
            base_sl = getattr(
                settings,
                "range_sl_long_pct",
                getattr(settings, "range_sl_pct", 0.004),
            )
        else:  # SHORT
            base_tp = getattr(
                settings,
                "range_tp_short_pct",
                getattr(settings, "range_tp_pct", 0.006),
            )
            base_sl = getattr(
                settings,
                "range_sl_short_pct",
                getattr(settings, "range_sl_pct", 0.004),
            )

        # 동적 TP 켜져 있으면 캔들폭으로 조정
        if getattr(settings, "use_range_dynamic_tp", False) and len(candles_3m) >= lookback_for_vol:
            recent = candles_3m[-lookback_for_vol:]
            # 평균 고저폭
            avg_hl = sum(c[2] - c[3] for c in recent) / float(lookback_for_vol)
            last_close = candles_3m[-1][4]
            if last_close > 0:
                dyn_tp = avg_hl / last_close
                # 최소/최대 범위로 클램프
                tp_min = getattr(settings, "range_tp_min", 0.0035)
                tp_max = getattr(settings, "range_tp_max", 0.0065)
                dyn_tp = max(tp_min, min(tp_max, dyn_tp))
                base_tp = dyn_tp

        # soft 허용일 때는 TP를 너무 욕심내지 않게 한 번 더 보수화
        if soft_reason:
            tp_min = getattr(settings, "range_tp_min", 0.0035)
            # 원래 base_tp가 크면 조금만 내리고, 작으면 그대로 둔다.
            base_tp = max(tp_min, min(base_tp, tp_min * 1.2))

    # ── 변경점: 숏일 때는 SL을 TP의 75% 이상으로 보정해서
    #    위로 한 번만 털리는 캔들에서 바로 잘리는 걸 줄인다. ──
    if direction == "SHORT":
        # 예: TP=0.006 → SL 최소 0.0045
        widened_sl = base_tp * 0.75
        base_sl = max(base_sl, widened_sl)

    return {
        "tp_pct": base_tp,
        "sl_pct": base_sl,
    }


# ─────────────────────────────
# 박스장 자체를 오늘은 막을지 판단 (기존과 동일한 시그니처)
# ─────────────────────────────
def should_block_range_today(candles_3m: Candles, candles_15m: Candles) -> bool:
    """박스장 진입 전에 시장 상태를 한 번 더 필터링한다.

    1) ATR 이 너무 죽어 있으면 → True
    2) 15m EMA 이격이 너무 크면 → True
    아니면 False.
    """
    # 1) ATR 수축 체크
    atr_fast = calc_atr(candles_3m, 14)
    atr_slow = calc_atr(candles_3m, 40)
    if atr_fast and atr_slow and atr_slow > 0:
        # 완화 버전: fast < slow * 0.6 일 때만 막는다
        if atr_fast < atr_slow * 0.6:
            log(
                f"[RANGE_BLOCK] ATR compressed: fast={atr_fast:.6f} slow={atr_slow:.6f} → fast < slow*0.6"
            )
            return True

    # 2) 15m 추세 강도 체크
    if candles_15m:
        closes_15 = [c[4] for c in candles_15m]
        if len(closes_15) >= 50:
            e20_15 = ema(closes_15, 20)
            e50_15 = ema(closes_15, 50)
            if not math.isnan(e20_15[-1]) and not math.isnan(e50_15[-1]):
                dist = abs(e20_15[-1] - e50_15[-1]) / e50_15[-1]
                # 최종 완화: dist > 0.01 (1%) 일 때만 막는다
                if dist > 0.01:
                    log(
                        f"[RANGE_BLOCK] 15m EMA distance too wide: dist={dist:.6f} > 0.01"
                    )
                    return True

    # 여기까지 안 걸리면 박스 허용
    return False


# ─────────────────────────────
# 박스장 차단을 단계적으로 할 수 있는 버전
# ─────────────────────────────
def should_block_range_today_with_level(
    candles_3m: Candles,
    candles_15m: Candles,
    settings: Optional["BotSettings"],
) -> Tuple[bool, str]:
    """박스장 차단을 레벨에 따라 다르게 주고 싶을 때 사용.

    반환값:
        (blocked: bool, reason: str)
    """
    # 기존 로직으로 먼저 판정
    atr_fast = calc_atr(candles_3m, 14)
    atr_slow = calc_atr(candles_3m, 40)
    atr_block = False
    if atr_fast and atr_slow and atr_slow > 0:
        if atr_fast < atr_slow * 0.6:
            atr_block = True

    ema_block = False
    dist = 0.0
    if candles_15m:
        closes_15 = [c[4] for c in candles_15m]
        if len(closes_15) >= 50:
            e20_15 = ema(closes_15, 20)
            e50_15 = ema(closes_15, 50)
            if not math.isnan(e20_15[-1]) and not math.isnan(e50_15[-1]):
                dist = abs(e20_15[-1] - e50_15[-1]) / e50_15[-1]
                if dist > 0.01:
                    ema_block = True

    # settings 없으면 기존처럼 동작
    if settings is None:
        if atr_block:
            log(
                f"[RANGE_BLOCK] ATR compressed: fast={atr_fast:.6f} slow={atr_slow:.6f} → fast < slow*0.6"
            )
            return True, "atr"
        if ema_block:
            log(
                f"[RANGE_BLOCK] 15m EMA distance too wide: dist={dist:.6f} > 0.01"
            )
            return True, "ema"
        return False, ""

    level = getattr(settings, "range_strict_level", 0)

    # level 0: 기존과 동일하게 막음
    if level == 0:
        if atr_block:
            log(
                f"[RANGE_BLOCK] ATR compressed (level 0): fast={atr_fast:.6f} slow={atr_slow:.6f}"
            )
            return True, "atr"
        if ema_block:
            log(
                f"[RANGE_BLOCK] 15m EMA distance too wide (level 0): dist={dist:.6f} > 0.01"
            )
            return True, "ema"
        return False, ""

    # level 1: soft block → 막지 않고 호출 측이 TP/SL 을 더 낮춰서 쓸 수 있게 한다
    if level == 1:
        if atr_block:
            log(
                f"[RANGE_SOFT] ATR compressed but allowed (level 1): fast={atr_fast:.6f} slow={atr_slow:.6f}"
            )
            return False, "soft_atr"
        if ema_block:
            log(
                f"[RANGE_SOFT] 15m EMA distance wide but allowed (level 1): dist={dist:.6f} > 0.01"
            )
            return False, "soft_ema"
        return False, ""

    # level 2 이상: 다시 강하게 막음
    if atr_block:
        log(
            f"[RANGE_BLOCK] ATR compressed (level {level}): fast={atr_fast:.6f} slow={atr_slow:.6f}"
        )
        return True, "atr"
    if ema_block:
        log(
            f"[RANGE_BLOCK] 15m EMA distance too wide (level {level}): dist={dist:.6f} > 0.01"
        )
        return True, "ema"

    return False, ""


__all__ = [
    "decide_signal_range",
    "should_block_range_today",
    "compute_range_params",
    "should_block_range_today_with_level",
]
