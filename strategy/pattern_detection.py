from __future__ import annotations

# PATCH NOTES — 2026-03-01
# -----------------------------------------------------
# 1) raw_ohlcv_last20 입력 포맷 변경 대응
#    - 기존: List[{"ts_ms","open","high","low","close","volume"}] (dict 기반)
#    - 신규: List[(ts, o, h, l, c, v)] 또는 List[[ts, o, h, l, c, v]] (list/tuple 기반)
# 2) STRICT 강화
#    - dict 입력은 즉시 PatternError (dict 유지 금지)
#    - (ts,o,h,l,c,v) 길이 6 미만 즉시 PatternError
#    - None→0 치환 금지 / 임의 추정 금지
#    - upstream( market_features_ws ) 출력도 동일 포맷(list/tuple)으로 유지해야 함
# 3) 문서/타입힌트/파서 전면 정합
# -----------------------------------------------------

# PATCH NOTES — 2026-03-05
# -----------------------------------------------------
# 1) FIX(TRADE-GRADE): pattern item direction 값이 None이면 다운스트림(unified_features_builder)
#    STRICT 검증에서 크래시가 발생하므로, 방향성 없는 패턴은 direction="NEUTRAL"로 고정한다.
#    적용 패턴: doji, doji_long_legged, triangle_sym, pennant, volume_climax
# 2) FIX: add_pattern 호출부의 쉼표 누락으로 인한 SyntaxError를 모두 제거한다.
# 3) STRICT: add_pattern()에서 direction이 None/비허용 값이면 PatternError로 즉시 실패한다.
# -----------------------------------------------------


"""
pattern_detection.py (Ultra Version, tuned)
=====================================================
BingX/Binance Auto Trader에서 사용할
"차트 패턴 전용 피처 엔진" 모듈.

역할
-----------------------------------------------------
- WS raw_ohlcv_last20 (STRICT: list/tuple OHLCV)을 기반으로
  캔들 패턴 · 구조 패턴 · 지표 결합 패턴 · 볼륨/유동성 패턴을 정량화한다.
- build_pattern_features(...) 결과를 unified_features_builder / gpt_decider 에서
  그대로 사용한다.
- WS 데이터가 부족/손상된 경우 PatternError를 발생시켜,
  REST 백필이나 임의 추정 없이 그대로 실패시키는 정책을 따른다.

입력 포맷 (STRICT)
-----------------------------------------------------
raw_ohlcv_last20:
    List[Tuple[int, float, float, float, float, float]]
    또는 List[List[int|float, ...]] (길이 6 고정)
    (ts_ms, open, high, low, close, volume)

dict 포맷은 금지한다.
=====================================================
"""

import math
from typing import Any, Dict, List, Optional, Sequence, Tuple


try:
    from infra.telelog import log, send_tg
except Exception:  # 로컬 테스트/단일 모듈 실행 시 대비
    def log(msg: str) -> None:  # type: ignore[override]
        print(msg)

    def send_tg(msg: str) -> None:  # type: ignore[override]
        print(f"[TG-STUB] {msg}")


# ─────────────────────────────────────────────────────
# 패턴 강도 설정 (휴리스틱 값 중앙 관리)
# ─────────────────────────────────────────────────────

DEFAULT_PATTERN_STRENGTHS: Dict[str, float] = {
    # 캔들 패턴
    "bullish_engulfing": 0.8,
    "bearish_engulfing": 0.8,
    "bullish_pinbar": 0.7,
    "bearish_pinbar": 0.7,
    "doji": 0.4,
    "doji_long_legged": 0.5,
    "doji_dragonfly": 0.6,
    "doji_gravestone": 0.6,
    "morning_star": 0.9,
    "evening_star": 0.9,
    # 구조 패턴
    "head_and_shoulders": 0.85,
    "inverse_head_and_shoulders": 0.85,
    "triangle_sym": 0.6,
    "triangle_asc": 0.6,
    "triangle_desc": 0.6,
    "bullish_flag": 0.65,
    "bearish_flag": 0.65,
    "pennant": 0.6,
    "bullish_breakout": 0.7,
    "bearish_breakout": 0.7,
    "bullish_fakeout": 0.6,
    "bearish_fakeout": 0.6,
    # 볼륨/유동성/오더북
    "volume_climax": 0.6,
    "bullish_liquidity_grab": 0.7,
    "bearish_liquidity_grab": 0.7,
    "orderbook_bullish_imbalance": 0.6,
    "orderbook_bearish_imbalance": 0.6,
    # 추세선/지표
    "bullish_trendline_break": 0.6,
    "bearish_trendline_break": 0.6,
    "rsi_bullish_divergence": 0.65,
    "rsi_bearish_divergence": 0.65,
}

try:  # pragma: no cover
    from settings import PatternStrengthSettings  # type: ignore
except Exception:
    PatternStrengthSettings = None  # type: ignore[assignment]


def _load_pattern_strengths() -> Dict[str, float]:
    strengths: Dict[str, float] = dict(DEFAULT_PATTERN_STRENGTHS)
    if PatternStrengthSettings is not None:
        try:
            user_settings = PatternStrengthSettings()  # type: ignore[call-arg]
            user_map = getattr(user_settings, "pattern_strengths", None)
            if isinstance(user_map, dict):
                for k, v in user_map.items():
                    try:
                        strengths[str(k)] = float(v)
                    except Exception:
                        continue
        except Exception as e:
            log(f"[PATTERN] PatternStrengthSettings 로드 실패: {e}")
    return strengths


PATTERN_STRENGTHS: Dict[str, float] = _load_pattern_strengths()


def _strength(key: str, default: float) -> float:
    try:
        return float(PATTERN_STRENGTHS.get(key, default))
    except Exception:
        return default


# ─────────────────────────────────────────────────────
# 에러/유틸
# ─────────────────────────────────────────────────────

class PatternError(RuntimeError):
    """필수 패턴 데이터가 부족하거나 손상된 경우 사용하는 예외."""


def _safe_tg(msg: str) -> None:
    try:
        send_tg(msg)
    except Exception:
        pass


def _is_nan(x: Any) -> bool:
    return isinstance(x, float) and math.isnan(x)


def _row_preview(row: Any, max_len: int = 180) -> str:
    s = str(row)
    if len(s) > max_len:
        return s[:max_len] + f"...(trunc,len={len(s)})"
    return s


def _require_ohlcv(
    raw_ohlcv_last20: Any,
    name: str = "raw_ohlcv_last20",
    min_len: int = 5,
) -> None:
    """
    STRICT:
    - dict 금지
    - 각 row는 list/tuple 이고 길이 6 이상이어야 한다.
    - (ts,o,h,l,c,v) 모두 숫자 파싱 가능해야 한다.
    """

    if raw_ohlcv_last20 is None or not isinstance(raw_ohlcv_last20, list) or len(raw_ohlcv_last20) < min_len:
        msg = (
            f"[패턴오류] {name} 데이터 부족/형식 오류: 필요 {min_len}, "
            f"실제 {0 if raw_ohlcv_last20 is None else (len(raw_ohlcv_last20) if isinstance(raw_ohlcv_last20, list) else 'N/A')}"
        )
        log(msg)
        _safe_tg(msg)
        raise PatternError(msg)

    for idx, row in enumerate(raw_ohlcv_last20):
        if isinstance(row, dict):
            msg = f"[패턴오류] {name}[{idx}] dict 포맷 금지 (got={_row_preview(row)})"
            log(msg)
            _safe_tg(msg)
            raise PatternError(msg)

        if not isinstance(row, (list, tuple)) or len(row) < 6:
            msg = f"[패턴오류] {name}[{idx}] must be (ts,o,h,l,c,v) list/tuple (got={_row_preview(row)})"
            log(msg)
            _safe_tg(msg)
            raise PatternError(msg)

        try:
            ts = int(row[0])
            o = float(row[1])
            h = float(row[2])
            l = float(row[3])
            cl = float(row[4])
            v = float(row[5])
        except Exception as e:
            msg = f"[패턴오류] {name}[{idx}] OHLCV 파싱 실패: {e}"
            log(msg)
            _safe_tg(msg)
            raise PatternError(msg)

        if ts <= 0:
            msg = f"[패턴오류] {name}[{idx}] ts<=0 (ts={ts})"
            log(msg)
            _safe_tg(msg)
            raise PatternError(msg)

        if any(_is_nan(x) for x in (o, h, l, cl, v)):
            msg = f"[패턴오류] {name}[{idx}] NaN 값 포함 (o/h/l/c/v)"
            log(msg)
            _safe_tg(msg)
            raise PatternError(msg)

        if h <= 0 or l <= 0 or cl <= 0 or o <= 0:
            msg = (
                f"[패턴오류] {name}[{idx}] 가격이 0 이하입니다 "
                f"(o={o}, h={h}, l={l}, c={cl})"
            )
            log(msg)
            _safe_tg(msg)
            raise PatternError(msg)


def _require_series(
    series: Optional[List[float]],
    name: str,
    min_len: int = 5,
    strict: bool = False,
) -> bool:
    if series is None or len(series) < min_len:
        if strict:
            msg = (
                f"[패턴오류] {name} 시리즈 길이 부족: 필요 {min_len}, "
                f"실제 {0 if series is None else len(series)}"
            )
            log(msg)
            _safe_tg(msg)
            raise PatternError(msg)
        return False

    for idx, v in enumerate(series):
        if _is_nan(float(v)):
            if strict:
                msg = f"[패턴오류] {name}[{idx}] NaN 값 발견"
                log(msg)
                _safe_tg(msg)
                raise PatternError(msg)
            return False

    return True


def _ohlc_lists(
    raw_ohlcv_last20: List[Sequence[Any]],
) -> Tuple[List[float], List[float], List[float], List[float], List[float]]:
    opens: List[float] = []
    highs: List[float] = []
    lows: List[float] = []
    closes: List[float] = []
    vols: List[float] = []

    for row in raw_ohlcv_last20:
        # _require_ohlcv에서 이미 길이/타입 검증 완료 전제
        opens.append(float(row[1]))
        highs.append(float(row[2]))
        lows.append(float(row[3]))
        closes.append(float(row[4]))
        vols.append(float(row[5]))

    return opens, highs, lows, closes, vols


def _confidence_from_strength(strength: float) -> str:
    if strength >= 0.75:
        return "high"
    if strength >= 0.4:
        return "medium"
    if strength > 0.0:
        return "low"
    return "none"


# ─────────────────────────────────────────────────────
# 1) 캔들 패턴
# ─────────────────────────────────────────────────────

def _detect_engulfing(opens: List[float], closes: List[float]) -> Tuple[int, int]:
    if len(opens) < 2 or len(closes) < 2:
        return 0, 0

    o1, c1 = opens[-2], closes[-2]
    o2, c2 = opens[-1], closes[-1]

    body1 = abs(c1 - o1)
    body2 = abs(c2 - o2)

    bull = int(o1 > c1 and c2 > o2 and body2 > body1 and o2 <= c1 and c2 >= o1)
    bear = int(c1 > o1 and o2 > c2 and body2 > body1 and o2 >= c1 and c2 <= o1)

    return bull, bear


def _detect_pinbar(
    opens: List[float],
    highs: List[float],
    lows: List[float],
    closes: List[float],
) -> Tuple[int, int, float]:
    if not opens:
        return 0, 0, 0.0

    o = opens[-1]
    h = highs[-1]
    l = lows[-1]
    c = closes[-1]

    body = abs(c - o)
    full = max(h, c, o) - min(l, c, o)
    upper = h - max(o, c)
    lower = min(o, c) - l

    bull = int(full > 0 and lower >= body * 2 and body <= full * 0.3 and c > o)
    bear = int(full > 0 and upper >= body * 2 and body <= full * 0.3 and c < o)

    wick_strength = 0.0
    if full > 0:
        wick_strength = max(upper, lower) / full

    return bull, bear, max(0.0, min(1.0, wick_strength))


def _detect_doji(
    opens: List[float],
    highs: List[float],
    lows: List[float],
    closes: List[float],
) -> Dict[str, int]:
    out = {
        "doji": 0,
        "doji_long_legged": 0,
        "doji_dragonfly": 0,
        "doji_gravestone": 0,
    }

    if not opens:
        return out

    o = opens[-1]
    h = highs[-1]
    l = lows[-1]
    c = closes[-1]

    body = abs(c - o)
    full = h - l
    if full <= 0:
        return out

    upper = h - max(o, c)
    lower = min(o, c) - l

    is_doji = body <= full * 0.1
    if not is_doji:
        return out

    out["doji"] = 1

    if upper > full * 0.25 and lower > full * 0.25:
        out["doji_long_legged"] = 1
    if lower > full * 0.3 and upper <= full * 0.1:
        out["doji_dragonfly"] = 1
    if upper > full * 0.3 and lower <= full * 0.1:
        out["doji_gravestone"] = 1

    return out


def _detect_morning_evening_star(
    opens: List[float],
    closes: List[float],
    highs: List[float],
    lows: List[float],
) -> Tuple[int, int]:
    if len(opens) < 3:
        return 0, 0

    o1, o2, o3 = opens[-3], opens[-2], opens[-1]
    c1, c2, c3 = closes[-3], closes[-2], closes[-1]

    body1 = abs(c1 - o1)
    body2 = abs(c2 - o2)
    body3 = abs(c3 - o3)

    cond_morning = (
        c1 < o1
        and body1 > body2 * 1.5
        and c3 > o3
        and body3 > body2 * 1.5
        and c3 >= (o1 + c1) / 2.0
    )

    cond_evening = (
        c1 > o1
        and body1 > body2 * 1.5
        and c3 < o3
        and body3 > body2 * 1.5
        and c3 <= (o1 + c1) / 2.0
    )

    return int(cond_morning), int(cond_evening)


def _detect_head_and_shoulders(
    highs: List[float],
    lows: List[float],
) -> Tuple[int, int]:
    n = len(highs)
    if n < 7:
        return 0, 0

    mid = n // 2
    head = highs[mid]
    left_shoulder = max(highs[:mid])
    right_shoulder = max(highs[mid + 1 :]) if mid + 1 < n else head

    hs = int(head > left_shoulder and head > right_shoulder)

    head_low = lows[mid]
    left_low = min(lows[:mid])
    right_low = min(lows[mid + 1 :]) if mid + 1 < n else head_low

    inv_hs = int(head_low < left_low and head_low < right_low)

    return hs, inv_hs


# ─────────────────────────────────────────────────────
# 2) 구조 패턴
# ─────────────────────────────────────────────────────

def _linreg_slope(xs: List[float], ys: List[float]) -> float:
    n = len(xs)
    if n == 0:
        return 0.0
    sx = sum(xs)
    sy = sum(ys)
    sxx = sum(v * v for v in xs)
    sxy = sum(xs[i] * ys[i] for i in range(n))
    denom = n * sxx - sx * sx
    if denom == 0:
        return 0.0
    return (n * sxy - sx * sy) / denom


def _detect_triangle(highs: List[float], lows: List[float]) -> Tuple[int, int, int]:
    n = len(highs)
    if n < 10:
        return 0, 0, 0

    xs = list(range(n))
    up_slope = _linreg_slope(xs, highs)
    low_slope = _linreg_slope(xs, lows)

    sym = int(up_slope < 0 and low_slope > 0)
    asc = int(abs(up_slope) < 1e-6 and low_slope > 0)
    desc = int(up_slope < 0 and abs(low_slope) < 1e-6)

    return sym, asc, desc


def _detect_flag_pennant(
    closes: List[float],
    highs: List[float],
    lows: List[float],
) -> Tuple[int, int, int, int]:
    n = len(closes)
    if n < 10:
        return 0, 0, 0, 0

    impulse_len = max(3, n // 3)
    flag_len = max(4, n // 3)

    base_idx = n - impulse_len - flag_len
    if base_idx < 0:
        base_idx = 0

    base_price = closes[base_idx]
    last_price = closes[-1]

    if base_price <= 0:
        return 0, 0, 0, 0

    impulse_ret = (last_price - base_price) / base_price

    flag_high = max(highs[-flag_len:])
    flag_low = min(lows[-flag_len:])
    flag_range_pct = (flag_high - flag_low) / last_price if last_price > 0 else 0.0

    strong_move_up = impulse_ret >= 0.01
    strong_move_down = impulse_ret <= -0.01
    tight_flag = flag_range_pct <= 0.005

    bullish_flag = int(strong_move_up and tight_flag)
    bearish_flag = int(strong_move_down and tight_flag)

    pennant = int(tight_flag and abs(impulse_ret) >= 0.015)
    channel_flag = int(tight_flag and abs(impulse_ret) >= 0.008)

    return bullish_flag, bearish_flag, pennant, channel_flag


def _detect_breakout_fakeout(
    highs: List[float],
    lows: List[float],
    closes: List[float],
) -> Tuple[int, int, int, int]:
    n = len(closes)
    if n < 5:
        return 0, 0, 0, 0

    lookback = min(20, n)
    box_high = max(highs[-lookback:-1])
    box_low = min(lows[-lookback:-1])

    last = closes[-1]
    prev = closes[-2]

    eps = 0.0005

    bull_break = int(last > box_high * (1 + eps))
    bear_break = int(last < box_low * (1 - eps))

    bull_fake = int(prev > box_high * (1 + eps) and last <= box_high)
    bear_fake = int(prev < box_low * (1 - eps) and last >= box_low)

    return bull_break, bear_break, bull_fake, bear_fake


def _detect_trendline_break(closes: List[float]) -> Tuple[int, int]:
    n = len(closes)
    if n < 11:
        return 0, 0

    sma_len = 10
    sma = sum(closes[-sma_len - 1 : -1]) / sma_len
    prev = closes[-2]
    last = closes[-1]

    bull_break = int(prev <= sma and last > sma)
    bear_break = int(prev >= sma and last < sma)

    return bull_break, bear_break


# ─────────────────────────────────────────────────────
# 3) 볼륨/유동성/오더북
# ─────────────────────────────────────────────────────

def _detect_volume_climax(vols: List[float]) -> Tuple[int, float]:
    n = len(vols)
    if n < 5:
        return 0, 0.0

    last = vols[-1]
    base = sum(vols[:-1]) / max(1, n - 1)
    if base <= 0:
        return 0, 0.0

    ratio = last / base
    has_climax = int(ratio >= 3.0)

    return has_climax, min(ratio / 5.0, 1.0)


def _detect_liquidity_grab(
    opens: List[float],
    highs: List[float],
    lows: List[float],
    closes: List[float],
) -> Tuple[int, int]:
    if not opens:
        return 0, 0

    o = opens[-1]
    h = highs[-1]
    l = lows[-1]
    c = closes[-1]

    body = abs(c - o)
    full = h - l
    if full <= 0:
        return 0, 0

    upper = h - max(o, c)
    lower = min(o, c) - l

    bull = int(lower > body * 2 and c > o)
    bear = int(upper > body * 2 and c < o)

    return bull, bear


def _detect_orderbook_imbalance(
    orderbook_features: Optional[Dict[str, Any]]
) -> Tuple[int, int, float]:
    if not orderbook_features:
        return 0, 0, 0.0

    imb = orderbook_features.get("depth_imbalance")
    spread_pct = orderbook_features.get("spread_pct")

    try:
        imb_f = float(imb) if imb is not None else 0.0
        spread_f = float(spread_pct) if spread_pct is not None else 0.0
    except Exception:
        return 0, 0, 0.0

    if spread_f > 0.002:
        return 0, 0, 0.0

    bull = int(imb_f >= 0.25)
    bear = int(imb_f <= -0.25)

    strength = min(1.0, abs(imb_f) / 0.6)

    return bull, bear, strength


# ─────────────────────────────────────────────────────
# 4) 지표 결합 패턴
# ─────────────────────────────────────────────────────

def _detect_rsi_divergence(
    closes: List[float],
    rsi_series: Optional[List[float]],
) -> Tuple[int, int]:
    if not rsi_series or len(rsi_series) < 5 or len(closes) < 5:
        return 0, 0

    c1, c2 = closes[-5], closes[-1]
    r1, r2 = rsi_series[-5], rsi_series[-1]

    bull = int(c2 < c1 and r2 > r1)
    bear = int(c2 > c1 and r2 < r1)

    return bull, bear


# ─────────────────────────────────────────────────────
# 5) 합성 스코어
# ─────────────────────────────────────────────────────

def _aggregate_scores(
    flags: Dict[str, int],
    volume_ratio: float,
    wick_strength: float,
    liquidity_strength: float,
) -> Dict[str, float]:
    bull_rev_keys = [
        "bullish_engulfing",
        "bullish_pinbar",
        "morning_star",
        "bullish_liquidity_grab",
        "inverse_head_and_shoulders",
    ]
    bear_rev_keys = [
        "bearish_engulfing",
        "bearish_pinbar",
        "evening_star",
        "bearish_liquidity_grab",
        "head_and_shoulders",
    ]
    bull_cont_keys = [
        "bullish_flag",
        "triangle_sym",
        "triangle_asc",
        "bullish_trendline_break",
    ]
    bear_cont_keys = [
        "bearish_flag",
        "triangle_sym",
        "triangle_desc",
        "bearish_trendline_break",
    ]

    bull_rev = sum(int(flags.get(k, 0)) for k in bull_rev_keys)
    bear_rev = sum(int(flags.get(k, 0)) for k in bear_rev_keys)
    bull_cont = sum(int(flags.get(k, 0)) for k in bull_cont_keys)
    bear_cont = sum(int(flags.get(k, 0)) for k in bear_cont_keys)

    total_signals = bull_rev + bear_rev + bull_cont + bear_cont

    pattern_score = min(1.0, total_signals / 6.0)
    reversal_probability = min(1.0, (bull_rev + bear_rev) / 4.0)
    continuation_probability = min(1.0, (bull_cont + bear_cont) / 4.0)
    momentum_score = min(1.0, max(bull_cont, bear_cont) / 4.0)

    volume_confirmation = max(0.0, min(1.0, volume_ratio))
    wick_strength = max(0.0, min(1.0, wick_strength))
    liquidity_event_score = max(0.0, min(1.0, liquidity_strength))

    return {
        "pattern_score": pattern_score,
        "reversal_probability": reversal_probability,
        "continuation_probability": continuation_probability,
        "momentum_score": momentum_score,
        "volume_confirmation": volume_confirmation,
        "wick_strength": wick_strength,
        "liquidity_event_score": liquidity_event_score,
    }


# ─────────────────────────────────────────────────────
# 6) 외부 진입점
# ─────────────────────────────────────────────────────

def build_pattern_features(
    *,
    raw_ohlcv_last20: List[Sequence[Any]],
    interval: Optional[str] = None,
    indicators: Optional[Dict[str, Any]] = None,
    orderbook_features: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    _require_ohlcv(raw_ohlcv_last20, "raw_ohlcv_last20", min_len=5)

    opens, highs, lows, closes, vols = _ohlc_lists(raw_ohlcv_last20)

    rsi_series: Optional[List[float]] = None
    if indicators is not None:
        cand = indicators.get("rsi_series") or indicators.get("rsi")
        if isinstance(cand, list) and _require_series(cand, "rsi_series", min_len=5, strict=False):
            rsi_series = [float(v) for v in cand]

    bullish_engulfing, bearish_engulfing = _detect_engulfing(opens, closes)
    bullish_pinbar, bearish_pinbar, wick_strength = _detect_pinbar(opens, highs, lows, closes)
    doji_flags = _detect_doji(opens, highs, lows, closes)
    morning_star, evening_star = _detect_morning_evening_star(opens, closes, highs, lows)
    hs, inv_hs = _detect_head_and_shoulders(highs, lows)

    tri_sym, tri_asc, tri_desc = _detect_triangle(highs, lows)
    bullish_flag, bearish_flag, pennant, channel_flag = _detect_flag_pennant(closes, highs, lows)
    bull_break, bear_break, bull_fake, bear_fake = _detect_breakout_fakeout(highs, lows, closes)
    bull_tlb, bear_tlb = _detect_trendline_break(closes)

    vol_climax, vol_ratio = _detect_volume_climax(vols)
    bull_liq, bear_liq = _detect_liquidity_grab(opens, highs, lows, closes)
    ob_bull, ob_bear, ob_strength = _detect_orderbook_imbalance(orderbook_features)

    rsi_bull_div, rsi_bear_div = _detect_rsi_divergence(closes, rsi_series)

    flags: Dict[str, int] = {
        "bullish_engulfing": bullish_engulfing,
        "bearish_engulfing": bearish_engulfing,
        "bullish_pinbar": bullish_pinbar,
        "bearish_pinbar": bearish_pinbar,
        "doji": doji_flags.get("doji", 0),
        "doji_long_legged": doji_flags.get("doji_long_legged", 0),
        "doji_dragonfly": doji_flags.get("doji_dragonfly", 0),
        "doji_gravestone": doji_flags.get("doji_gravestone", 0),
        "morning_star": morning_star,
        "evening_star": evening_star,
        "head_and_shoulders": hs,
        "inverse_head_and_shoulders": inv_hs,
        "triangle_sym": tri_sym,
        "triangle_asc": tri_asc,
        "triangle_desc": tri_desc,
        "bullish_flag": bullish_flag,
        "bearish_flag": bearish_flag,
        "pennant": pennant,
        "channel_flag": channel_flag,
        "bullish_breakout": bull_break,
        "bearish_breakout": bear_break,
        "bullish_fakeout": bull_fake,
        "bearish_fakeout": bear_fake,
        "volume_climax": vol_climax,
        "bullish_liquidity_grab": bull_liq,
        "bearish_liquidity_grab": bear_liq,
        "bullish_trendline_break": bull_tlb,
        "bearish_trendline_break": bear_tlb,
        "rsi_bullish_divergence": rsi_bull_div,
        "rsi_bearish_divergence": rsi_bear_div,
        "orderbook_bullish_imbalance": ob_bull,
        "orderbook_bearish_imbalance": ob_bear,
    }

    scores = _aggregate_scores(
        flags,
        volume_ratio=vol_ratio,
        wick_strength=wick_strength,
        liquidity_strength=max(ob_strength, float(bull_liq or bear_liq)),
    )

    patterns: List[Dict[str, Any]] = []

    def add_pattern(
        *,
        key: str,
        enabled: int,
        direction: Optional[str],
        kind: str,
        base_strength: float,
        extra_boost: float = 0.0,
        explanation: str,
    ) -> None:
        if not enabled:
            return

        if direction is None:
            raise PatternError(f"[패턴오류] pattern.direction must not be None (key={key})")

        d = str(direction).upper().strip()
        if d not in {"BULLISH", "BEARISH", "NEUTRAL"}:
            raise PatternError(f"[패턴오류] pattern.direction invalid (key={key}, direction={direction!r})")

        strength = max(0.0, min(1.0, base_strength + extra_boost))
        conf = _confidence_from_strength(strength)
        if conf == "none":
            return

        patterns.append(
            {
                "pattern": key,
                "direction": d,
                "kind": kind,
                "strength": strength,
                "confidence": conf,
                "explanation": explanation,
            }
        )

    iv_txt = f" ({interval})" if interval else ""

    # 이하 add_pattern(...) 블록은 기존 로직 그대로 유지 (생략 없이 유지)
    # ---- 캔들 패턴
    add_pattern(
        key="bullish_engulfing",
        enabled=bullish_engulfing,
        direction="BULLISH",
        kind="candle",
        base_strength=_strength("bullish_engulfing", 0.8),
        extra_boost=vol_ratio * 0.1,
        explanation=(f"최근 2개 봉 기준 상승 엔골핑 패턴이 감지되었습니다{iv_txt}. 강한 매수 전환 신호로 해석될 수 있습니다."),
    )
    add_pattern(
        key="bearish_engulfing",
        enabled=bearish_engulfing,
        direction="BEARISH",
        kind="candle",
        base_strength=_strength("bearish_engulfing", 0.8),
        extra_boost=vol_ratio * 0.1,
        explanation=(f"최근 2개 봉 기준 하락 엔골핑 패턴이 감지되었습니다{iv_txt}. 강한 매도 전환 신호로 해석될 수 있습니다."),
    )
    add_pattern(
        key="bullish_pinbar",
        enabled=bullish_pinbar,
        direction="BULLISH",
        kind="candle",
        base_strength=_strength("bullish_pinbar", 0.7),
        extra_boost=wick_strength * 0.2,
        explanation=(f"아래 꼬리가 긴 강세 핀바 패턴이 감지되었습니다{iv_txt}. 아래 구간에서 강한 매수 응답이 있었다는 신호입니다."),
    )
    add_pattern(
        key="bearish_pinbar",
        enabled=bearish_pinbar,
        direction="BEARISH",
        kind="candle",
        base_strength=_strength("bearish_pinbar", 0.7),
        extra_boost=wick_strength * 0.2,
        explanation=(f"위 꼬리가 긴 약세 핀바 패턴이 감지되었습니다{iv_txt}. 위 구간에서 강한 매도 응답이 있었다는 신호입니다."),
    )

    if doji_flags.get("doji"):
        add_pattern(
            key="doji",
            enabled=1,
            direction="NEUTRAL",
            kind="candle",
            base_strength=_strength("doji", 0.4),
            explanation=(f"몸통이 매우 작은 도지 캔들이 감지되었습니다{iv_txt}. 방향성 모멘텀이 약하거나 추세 전환 구간일 가능성이 있습니다."),
        )
    if doji_flags.get("doji_long_legged"):
        add_pattern(
            key="doji_long_legged",
            enabled=1,
            direction="NEUTRAL",
            kind="candle",
            base_strength=_strength("doji_long_legged", 0.5),
            explanation=(f"양쪽 꼬리가 긴 롱레그드 도지 패턴이 감지되었습니다{iv_txt}. 매수·매도 세력이 강하게 충돌한 구간일 수 있습니다."),
        )
    if doji_flags.get("doji_dragonfly"):
        add_pattern(
            key="doji_dragonfly",
            enabled=1,
            direction="BULLISH",
            kind="candle",
            base_strength=_strength("doji_dragonfly", 0.6),
            explanation=(f"아래 꼬리가 긴 드래곤플라이 도지 패턴이 감지되었습니다{iv_txt}. 하단 유동성 스윕 후 반등 신호로 해석될 수 있습니다."),
        )
    if doji_flags.get("doji_gravestone"):
        add_pattern(
            key="doji_gravestone",
            enabled=1,
            direction="BEARISH",
            kind="candle",
            base_strength=_strength("doji_gravestone", 0.6),
            explanation=(f"위 꼬리가 긴 그래브스톤 도지 패턴이 감지되었습니다{iv_txt}. 상단 유동성 스윕 후 매도 압력 신호로 해석될 수 있습니다."),
        )

    add_pattern(
        key="morning_star",
        enabled=morning_star,
        direction="BULLISH",
        kind="candle",
        base_strength=_strength("morning_star", 0.9),
        explanation=(f"3개 봉 기준 모닝 스타(강세 반전) 패턴이 감지되었습니다{iv_txt}."),
    )
    add_pattern(
        key="evening_star",
        enabled=evening_star,
        direction="BEARISH",
        kind="candle",
        base_strength=_strength("evening_star", 0.9),
        explanation=(f"3개 봉 기준 이브닝 스타(약세 반전) 패턴이 감지되었습니다{iv_txt}."),
    )

    add_pattern(
        key="head_and_shoulders",
        enabled=hs,
        direction="BEARISH",
        kind="structure",
        base_strength=_strength("head_and_shoulders", 0.85),
        explanation=(f"최근 고점 구조에서 헤드앤숄더 패턴이 감지되었습니다{iv_txt}."),
    )
    add_pattern(
        key="inverse_head_and_shoulders",
        enabled=inv_hs,
        direction="BULLISH",
        kind="structure",
        base_strength=_strength("inverse_head_and_shoulders", 0.85),
        explanation=(f"최근 저점 구조에서 역헤드앤숄더 패턴이 감지되었습니다{iv_txt}."),
    )

    add_pattern(
        key="triangle_sym",
        enabled=tri_sym,
        direction="NEUTRAL",
        kind="structure",
        base_strength=_strength("triangle_sym", 0.6),
        explanation=(f"대칭 삼각수렴 패턴이 감지되었습니다{iv_txt}."),
    )
    add_pattern(
        key="triangle_asc",
        enabled=tri_asc,
        direction="BULLISH",
        kind="structure",
        base_strength=_strength("triangle_asc", 0.6),
        explanation=(f"상승 삼각형 패턴이 감지되었습니다{iv_txt}."),
    )
    add_pattern(
        key="triangle_desc",
        enabled=tri_desc,
        direction="BEARISH",
        kind="structure",
        base_strength=_strength("triangle_desc", 0.6),
        explanation=(f"하락 삼각형 패턴이 감지되었습니다{iv_txt}."),
    )

    add_pattern(
        key="bullish_flag",
        enabled=bullish_flag,
        direction="BULLISH",
        kind="structure",
        base_strength=_strength("bullish_flag", 0.65),
        explanation=(f"강세 플래그 패턴이 감지되었습니다{iv_txt}."),
    )
    add_pattern(
        key="bearish_flag",
        enabled=bearish_flag,
        direction="BEARISH",
        kind="structure",
        base_strength=_strength("bearish_flag", 0.65),
        explanation=(f"약세 플래그 패턴이 감지되었습니다{iv_txt}."),
    )
    add_pattern(
        key="pennant",
        enabled=pennant,
        direction="NEUTRAL",
        kind="structure",
        base_strength=_strength("pennant", 0.6),
        explanation=(f"페넌트 패턴이 감지되었습니다{iv_txt}."),
    )

    add_pattern(
        key="bullish_breakout",
        enabled=bull_break,
        direction="BULLISH",
        kind="structure",
        base_strength=_strength("bullish_breakout", 0.7),
        explanation=(f"강세 브레이크아웃 신호가 감지되었습니다{iv_txt}."),
    )
    add_pattern(
        key="bearish_breakout",
        enabled=bear_break,
        direction="BEARISH",
        kind="structure",
        base_strength=_strength("bearish_breakout", 0.7),
        explanation=(f"약세 브레이크아웃 신호가 감지되었습니다{iv_txt}."),
    )
    add_pattern(
        key="bullish_fakeout",
        enabled=bull_fake,
        direction="BEARISH",
        kind="structure",
        base_strength=_strength("bullish_fakeout", 0.6),
        explanation=(f"강세 페이크아웃 패턴이 감지되었습니다{iv_txt}."),
    )
    add_pattern(
        key="bearish_fakeout",
        enabled=bear_fake,
        direction="BULLISH",
        kind="structure",
        base_strength=_strength("bearish_fakeout", 0.6),
        explanation=(f"약세 페이크아웃 패턴이 감지되었습니다{iv_txt}."),
    )

    add_pattern(
        key="volume_climax",
        enabled=vol_climax,
        direction="NEUTRAL",
        kind="volume",
        base_strength=_strength("volume_climax", 0.6),
        extra_boost=vol_ratio * 0.2,
        explanation=(f"볼륨 클라이맥스 패턴이 감지되었습니다{iv_txt}."),
    )
    add_pattern(
        key="bullish_liquidity_grab",
        enabled=bull_liq,
        direction="BULLISH",
        kind="liquidity",
        base_strength=_strength("bullish_liquidity_grab", 0.7),
        explanation=(f"강세 유동성 그랩 패턴이 감지되었습니다{iv_txt}."),
    )
    add_pattern(
        key="bearish_liquidity_grab",
        enabled=bear_liq,
        direction="BEARISH",
        kind="liquidity",
        base_strength=_strength("bearish_liquidity_grab", 0.7),
        explanation=(f"약세 유동성 그랩 패턴이 감지되었습니다{iv_txt}."),
    )
    add_pattern(
        key="orderbook_bullish_imbalance",
        enabled=ob_bull,
        direction="BULLISH",
        kind="orderbook",
        base_strength=_strength("orderbook_bullish_imbalance", 0.6),
        extra_boost=ob_strength * 0.3,
        explanation=(f"오더북 강세 비대칭 패턴이 감지되었습니다{iv_txt}."),
    )
    add_pattern(
        key="orderbook_bearish_imbalance",
        enabled=ob_bear,
        direction="BEARISH",
        kind="orderbook",
        base_strength=_strength("orderbook_bearish_imbalance", 0.6),
        extra_boost=ob_strength * 0.3,
        explanation=(f"오더북 약세 비대칭 패턴이 감지되었습니다{iv_txt}."),
    )

    add_pattern(
        key="bullish_trendline_break",
        enabled=bull_tlb,
        direction="BULLISH",
        kind="trendline",
        base_strength=_strength("bullish_trendline_break", 0.6),
        explanation=(f"강세 추세선 브레이크 패턴이 감지되었습니다{iv_txt}."),
    )
    add_pattern(
        key="bearish_trendline_break",
        enabled=bear_tlb,
        direction="BEARISH",
        kind="trendline",
        base_strength=_strength("bearish_trendline_break", 0.6),
        explanation=(f"약세 추세선 브레이크 패턴이 감지되었습니다{iv_txt}."),
    )

    add_pattern(
        key="rsi_bullish_divergence",
        enabled=rsi_bull_div,
        direction="BULLISH",
        kind="indicator",
        base_strength=_strength("rsi_bullish_divergence", 0.65),
        explanation=(f"RSI 강세 다이버전스 패턴이 감지되었습니다{iv_txt}."),
    )
    add_pattern(
        key="rsi_bearish_divergence",
        enabled=rsi_bear_div,
        direction="BEARISH",
        kind="indicator",
        base_strength=_strength("rsi_bearish_divergence", 0.65),
        explanation=(f"RSI 약세 다이버전스 패턴이 감지되었습니다{iv_txt}."),
    )

    best_pattern: Optional[Dict[str, Any]] = None
    best_strength = 0.0
    for p in patterns:
        s = float(p.get("strength", 0.0))
        if s > best_strength:
            best_strength = s
            best_pattern = p

    has_bullish_pattern = int(any(p.get("direction") == "BULLISH" for p in patterns))
    has_bearish_pattern = int(any(p.get("direction") == "BEARISH" for p in patterns))

    out: Dict[str, Any] = {}
    out.update(flags)
    out.update(scores)

    out["patterns"] = patterns
    out["best_pattern"] = best_pattern.get("pattern") if best_pattern else None
    out["best_pattern_direction"] = best_pattern.get("direction") if best_pattern else None
    out["best_pattern_confidence"] = best_pattern.get("confidence") if best_pattern else None
    out["has_bullish_pattern"] = has_bullish_pattern
    out["has_bearish_pattern"] = has_bearish_pattern

    return out


__all__ = ["PatternError", "build_pattern_features"]