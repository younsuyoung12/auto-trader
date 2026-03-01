from __future__ import annotations

"""
========================================================
strategy/unified_features_builder.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
설계 원칙:
- WS 기반 market_features_ws + pattern_detection 결과를 결합해 GPT 입력 dict를 생성한다.
- REST 백필/더미값/0 치환/None 반환 금지: 필수 데이터가 없거나 손상되면 즉시 실패한다.
- 필수 타임프레임: 1m / 5m / 15m / 1h / 4h (없으면 즉시 실패).
- 각 타임프레임은 필수 raw OHLCV + indicators dict 를 반드시 포함해야 한다.
- 오더북은 WS depth5 스냅샷(bids/asks)을 포함하며, 비어 있으면 즉시 실패한다.
- pattern_features/summary 는 화이트리스트 기반 compact 형태로 축소(토큰 최적화).
- engine_scores(0~100)는 엔진 1차 정량 점수이며, 최종 해석/결정은 GPT가 수행한다.
========================================================
입력 계약(중요)
- market_features_ws.timeframes[1m|5m|15m].raw_ohlcv_last20:
  (ts_ms, open, high, low, close, volume) 형태의 list/tuple 이어야 한다. dict 금지.
- market_features_ws.timeframes[*].indicators:
  engine_scores 계산에 필요한 scalar(rsi/ema_fast/ema_slow/atr_pct/macd_hist 등)를 포함해야 한다.

"""

import math
from typing import Any, Dict, List, NoReturn, Optional, Tuple

from settings import load_settings

try:
    from infra.telelog import log, send_tg
except Exception as e:  # pragma: no cover
    # STRICT: 로컬 stub 폴백 금지. 운영 환경에서는 반드시 telelog 가 있어야 한다.
    raise RuntimeError("infra.telelog import failed (STRICT · NO-FALLBACK · PRODUCTION MODE)") from e

from infra.market_data_ws import get_orderbook
from infra.market_features_ws import FeatureBuildError, build_entry_features_ws
from strategy.pattern_detection import PatternError, build_pattern_features

SET = load_settings()

# REQUIRED: GPT/엔진 1차 점수 산출을 위해 반드시 필요
REQUIRED_TFS: Tuple[str, ...] = ("1m", "5m", "15m", "1h", "4h")

# PATTERN: Ultra 정책 유지
PATTERN_TFS: Tuple[str, ...] = ("1m", "5m", "15m")

# 토큰 절감: 패턴 리스트 상한
MAX_PATTERNS_PER_TF: int = 4
MAX_SUMMARY_PATTERNS: int = 10

# 패턴 item compact 화이트리스트
_PATTERN_ITEM_KEYS: Tuple[str, ...] = (
    "pattern",
    "direction",
    "confidence",
    "strength",
    "type",
    "kind",
    "summary",
    "label",
    "trigger_price",
    "stop_price",
    "target_price",
)

# engine_scores.total 가중치(고정)
_TOTAL_WEIGHTS: Dict[str, float] = {
    "trend_4h": 0.30,
    "momentum_1h": 0.20,
    "structure_15m": 0.20,
    "timing_5m": 0.20,
    "orderbook_micro": 0.10,
}

# 출력(토큰 최적화)용으로 indicators 에서 유지할 필드
_INDICATOR_KEEP_KEYS: Tuple[str, ...] = (
    "rsi",
    "ema_fast",
    "ema_slow",
    "atr",
    "atr_pct",
    "macd",
    "macd_signal",
    "macd_hist",
)


class UnifiedFeaturesError(RuntimeError):
    """마켓 피처 + 패턴 피처 + 엔진 점수 통합 단계에서 사용하는 예외."""


def _safe_tg(msg: str) -> None:
    """텔레그램 전송 실패만 제한적으로 무시한다(유틸 보호)."""
    try:
        send_tg(msg)
    except Exception:
        pass


def _fail_unified(symbol: str, stage: str, reason: str, exc: Optional[BaseException] = None) -> NoReturn:
    msg = f"[UNIFIED-FEAT][{symbol}] {stage} 실패: {reason}"
    log(msg)
    _safe_tg(msg)
    if exc is None:
        raise UnifiedFeaturesError(msg)
    raise UnifiedFeaturesError(msg) from exc


def _clamp(v: float, lo: float, hi: float) -> float:
    if v < lo:
        return lo
    if v > hi:
        return hi
    return v


def _require_nonempty_str(symbol: str, stage: str, v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        _fail_unified(symbol, stage, f"{name} is required (empty)")
    return s


def _require_dict(symbol: str, stage: str, v: Any, name: str) -> Dict[str, Any]:
    if not isinstance(v, dict):
        _fail_unified(symbol, stage, f"{name} must be dict (got={type(v)})")
    if not v:
        _fail_unified(symbol, stage, f"{name} is empty")
    return v


def _require_list(symbol: str, stage: str, v: Any, name: str, min_len: int) -> List[Any]:
    if not isinstance(v, list):
        _fail_unified(symbol, stage, f"{name} must be list (got={type(v)})")
    if len(v) < min_len:
        _fail_unified(symbol, stage, f"{name} length<{min_len} (got={len(v)})")
    return v


def _require_int(symbol: str, stage: str, v: Any, name: str) -> int:
    try:
        iv = int(v)
    except Exception as e:
        _fail_unified(symbol, stage, f"{name} must be int (got={v!r})", e)
    return iv


def _require_float(symbol: str, stage: str, v: Any, name: str) -> float:
    try:
        fv = float(v)
    except Exception as e:
        _fail_unified(symbol, stage, f"{name} must be float (got={v!r})", e)
    if not math.isfinite(fv):
        _fail_unified(symbol, stage, f"{name} must be finite (got={fv})")
    return fv


def _require_prob_0_1(symbol: str, stage: str, v: Any, name: str) -> float:
    fv = _require_float(symbol, stage, v, name)
    if fv < 0.0 or fv > 1.0:
        _fail_unified(symbol, stage, f"{name} out of range [0,1] (got={fv})")
    return fv


def _parse_ohlcv_rows(
    symbol: str,
    stage: str,
    rows: Any,
    *,
    min_len: int,
    name: str,
) -> List[Tuple[int, float, float, float, float, float]]:
    raw = _require_list(symbol, stage, rows, name, min_len=min_len)
    out: List[Tuple[int, float, float, float, float, float]] = []

    # min_len 개만 사용(토큰/연산 최적화 + 요건 충족)
    sliced = raw[-min_len:]

    for i, row in enumerate(sliced):
        if not isinstance(row, (list, tuple)) or len(row) < 6:
            _fail_unified(symbol, stage, f"{name}[{i}] must be (ts,o,h,l,c,v) list/tuple (got={row!r})")

        ts = _require_int(symbol, stage, row[0], f"{name}[{i}].ts")
        o = _require_float(symbol, stage, row[1], f"{name}[{i}].open")
        h = _require_float(symbol, stage, row[2], f"{name}[{i}].high")
        l = _require_float(symbol, stage, row[3], f"{name}[{i}].low")
        c = _require_float(symbol, stage, row[4], f"{name}[{i}].close")
        v = _require_float(symbol, stage, row[5], f"{name}[{i}].volume")

        if ts <= 0:
            _fail_unified(symbol, stage, f"{name}[{i}] invalid ts (<=0): {ts}")
        if o <= 0 or h <= 0 or l <= 0 or c <= 0:
            _fail_unified(symbol, stage, f"{name}[{i}] invalid OHLC (<=0): o={o},h={h},l={l},c={c}")
        if h < l:
            _fail_unified(symbol, stage, f"{name}[{i}] invalid candle (high<low): h={h}, l={l}")
        if h < max(o, c) or l > min(o, c):
            _fail_unified(symbol, stage, f"{name}[{i}] invalid candle (bounds): o={o},h={h},l={l},c={c}")
        if v < 0:
            _fail_unified(symbol, stage, f"{name}[{i}] invalid volume (<0): v={v}")

        out.append((ts, o, h, l, c, v))

    return out


def _ensure_timeframe_payloads_strict(symbol: str, timeframes: Dict[str, Any]) -> None:
    """
    STRICT 요구사항(A):
    - REQUIRED_TFS 존재
    - 1m/5m/15m: raw_ohlcv_last20 >= 20
    - 1h/4h: raw_ohlcv_last60 >= 60 (없으면 candles 에서 생성)
    - 모든 REQUIRED_TFS: indicators dict 필수
    """
    for tf in REQUIRED_TFS:
        stage = f"timeframes[{tf}]"
        tf_data = timeframes.get(tf)
        if not isinstance(tf_data, dict) or not tf_data:
            _fail_unified(
                symbol,
                stage,
                (
                    f"required timeframe missing/invalid: {tf}. "
                    "settings.ws_subscribe_tfs 에 1h/4h 포함 및 WS 백필 설정을 확인하세요."
                ),
            )

        indicators = tf_data.get("indicators")
        if not isinstance(indicators, dict) or not indicators:
            _fail_unified(symbol, stage, "indicators dict missing/empty (STRICT)")

        if tf in ("1m", "5m", "15m"):
            raw20 = tf_data.get("raw_ohlcv_last20")
            _parse_ohlcv_rows(symbol, stage, raw20, min_len=20, name=f"{tf}.raw_ohlcv_last20")
        elif tf in ("1h", "4h"):
            raw60 = tf_data.get("raw_ohlcv_last60")
            if raw60 is None:
                candles = tf_data.get("candles")
                parsed = _parse_ohlcv_rows(symbol, stage, candles, min_len=60, name=f"{tf}.candles")
                # 생성은 "더미"가 아니라 WS 캔들 데이터의 형식 변환이다.
                tf_data["raw_ohlcv_last60"] = [[ts, o, h, l, c, v] for (ts, o, h, l, c, v) in parsed]
            else:
                _parse_ohlcv_rows(symbol, stage, raw60, min_len=60, name=f"{tf}.raw_ohlcv_last60")


def _get_orderbook_strict(symbol: str, limit: int = 5) -> Dict[str, Any]:
    """
    STRICT 요구사항(A):
    - orderbook dict 존재 필수
    - bids/asks 비어있거나 누락이면 즉시 실패
    """
    stage = "orderbook"
    ob = get_orderbook(symbol, limit=limit)
    if not isinstance(ob, dict) or not ob:
        _fail_unified(symbol, stage, "WS orderbook snapshot missing (get_orderbook returned None/empty)")

    bids = ob.get("bids")
    asks = ob.get("asks")
    if not isinstance(bids, list) or not bids:
        _fail_unified(symbol, stage, "orderbook bids missing/empty")
    if not isinstance(asks, list) or not asks:
        _fail_unified(symbol, stage, "orderbook asks missing/empty")

    # Normalize rows to [price, qty]
    nbids: List[List[float]] = []
    nasks: List[List[float]] = []

    for i, row in enumerate(bids[:limit]):
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            _fail_unified(symbol, stage, f"bids[{i}] invalid row: {row!r}")
        px = _require_float(symbol, stage, row[0], f"bids[{i}].price")
        qty = _require_float(symbol, stage, row[1], f"bids[{i}].qty")
        if px <= 0 or qty < 0:
            _fail_unified(symbol, stage, f"bids[{i}] invalid values: price={px}, qty={qty}")
        nbids.append([px, qty])

    for i, row in enumerate(asks[:limit]):
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            _fail_unified(symbol, stage, f"asks[{i}] invalid row: {row!r}")
        px = _require_float(symbol, stage, row[0], f"asks[{i}].price")
        qty = _require_float(symbol, stage, row[1], f"asks[{i}].qty")
        if px <= 0 or qty < 0:
            _fail_unified(symbol, stage, f"asks[{i}] invalid values: price={px}, qty={qty}")
        nasks.append([px, qty])

    best_bid = max(r[0] for r in nbids)
    best_ask = min(r[0] for r in nasks)
    if best_ask <= best_bid:
        _fail_unified(symbol, stage, f"crossed book: bestAsk({best_ask}) <= bestBid({best_bid})")

    spread = best_ask - best_bid
    mid = (best_ask + best_bid) / 2.0
    if mid <= 0:
        _fail_unified(symbol, stage, f"invalid mid price: {mid}")

    spread_pct = (spread / mid) * 100.0
    if not math.isfinite(spread_pct) or spread_pct < 0:
        _fail_unified(symbol, stage, f"invalid spread_pct: {spread_pct}")

    top_bid_qty = sum(q for _, q in nbids)
    top_ask_qty = sum(q for _, q in nasks)
    qty_total = top_bid_qty + top_ask_qty
    if qty_total <= 0:
        _fail_unified(symbol, stage, "invalid qty_total (<=0) for imbalance computation")

    imbalance_qty_pct = ((top_bid_qty - top_ask_qty) / qty_total) * 100.0
    if not math.isfinite(imbalance_qty_pct):
        _fail_unified(symbol, stage, f"invalid imbalance_qty_pct: {imbalance_qty_pct}")

    bid_notional = sum(px * qty for px, qty in nbids)
    ask_notional = sum(px * qty for px, qty in nasks)
    notional_total = bid_notional + ask_notional
    if notional_total <= 0:
        _fail_unified(symbol, stage, "invalid notional_total (<=0) for depth_imbalance")

    depth_imbalance = (bid_notional - ask_notional) / notional_total  # [-1, +1]
    if not math.isfinite(depth_imbalance):
        _fail_unified(symbol, stage, f"invalid depth_imbalance: {depth_imbalance}")

    checked_at_ms = _require_int(symbol, stage, ob.get("ts"), "checked_at_ms(ts)")
    if checked_at_ms <= 0:
        _fail_unified(symbol, stage, f"invalid checked_at_ms(ts): {checked_at_ms}")

    out: Dict[str, Any] = {
        "checked_at_ms": checked_at_ms,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "spread_pct": spread_pct,
        "top_bid_qty": top_bid_qty,
        "top_ask_qty": top_ask_qty,
        "imbalance_qty_pct": imbalance_qty_pct,
        "depth_imbalance": depth_imbalance,
        "bids": nbids,
        "asks": nasks,
    }

    return out


def _compact_pattern_item(symbol: str, stage: str, p: Dict[str, Any], timeframe: str) -> Dict[str, Any]:
    item: Dict[str, Any] = {}
    for key in _PATTERN_ITEM_KEYS:
        if key in p:
            item[key] = p[key]

    item["timeframe"] = timeframe

    if "strength" not in item:
        _fail_unified(symbol, stage, f"pattern item missing strength (timeframe={timeframe})")

    item["strength"] = _require_float(symbol, stage, item["strength"], "pattern.strength")
    return item


def _build_pattern_bundle_strict(
    symbol: str,
    timeframes: Dict[str, Any],
    orderbook_features: Dict[str, Any],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    """
    STRICT 요구사항(B):
    - PATTERN_TFS 고정
    - score 키 누락/비정상 -> 즉시 실패 (0으로 방어 금지)
    - build_pattern_features 예외 -> UnifiedFeaturesError 로 래핑
    - patterns compact + 상위 N개 유지
    """
    pattern_features: Dict[str, Dict[str, Any]] = {}

    all_patterns: List[Dict[str, Any]] = []
    best_strength = -1.0
    best_pattern_name = "NONE"
    best_pattern_direction = "NONE"
    best_pattern_confidence = "none"
    best_pattern_timeframe = "None"

    # 전역 요약 스코어: max 기준
    global_pattern_score = 0.0
    global_reversal = 0.0
    global_continuation = 0.0
    global_momentum = 0.0
    global_volume_conf = 0.0
    global_wick_strength = 0.0
    global_liquidity_score = 0.0

    has_bullish_pattern = 0
    has_bearish_pattern = 0

    for tf in PATTERN_TFS:
        stage = f"patterns[{tf}]"
        tf_data = _require_dict(symbol, stage, timeframes.get(tf), f"timeframes['{tf}']")

        raw_ohlcv_last20 = tf_data.get("raw_ohlcv_last20")
        # parse/validate only. pattern_detection 입력은 원본(list of list) 사용 가능하지만, 여기서 최소 요건 검증은 필수.
        _parse_ohlcv_rows(symbol, stage, raw_ohlcv_last20, min_len=20, name=f"{tf}.raw_ohlcv_last20")

        indicators = tf_data.get("indicators")
        indicators_dict = _require_dict(symbol, stage, indicators, f"{tf}.indicators")

        try:
            pf_raw = build_pattern_features(
                raw_ohlcv_last20=raw_ohlcv_last20,
                interval=tf,
                indicators=indicators_dict,
                orderbook_features=orderbook_features,
            )
        except PatternError as e:
            _fail_unified(symbol, stage, f"build_pattern_features PatternError: {e}", e)
        except Exception as e:
            _fail_unified(symbol, stage, f"build_pattern_features unexpected error: {e!r}", e)

        if not isinstance(pf_raw, dict) or not pf_raw:
            _fail_unified(symbol, stage, "pattern_detection returned non-dict or empty")

        # ── STRICT: 핵심 스코어 키 검증 ────────────────────────────────────────
        pattern_score = _require_prob_0_1(symbol, stage, pf_raw.get("pattern_score"), "pattern_score")
        reversal_prob = _require_prob_0_1(symbol, stage, pf_raw.get("reversal_probability"), "reversal_probability")
        continuation_prob = _require_prob_0_1(
            symbol, stage, pf_raw.get("continuation_probability"), "continuation_probability"
        )
        momentum_score = _require_prob_0_1(symbol, stage, pf_raw.get("momentum_score"), "momentum_score")
        volume_conf = _require_prob_0_1(symbol, stage, pf_raw.get("volume_confirmation"), "volume_confirmation")
        wick_strength = _require_prob_0_1(symbol, stage, pf_raw.get("wick_strength"), "wick_strength")
        liquidity_event = _require_prob_0_1(symbol, stage, pf_raw.get("liquidity_event_score"), "liquidity_event_score")

        has_bull = _require_int(symbol, stage, pf_raw.get("has_bullish_pattern"), "has_bullish_pattern")
        has_bear = _require_int(symbol, stage, pf_raw.get("has_bearish_pattern"), "has_bearish_pattern")
        if has_bull not in (0, 1) or has_bear not in (0, 1):
            _fail_unified(
                symbol,
                stage,
                f"has_bullish_pattern/has_bearish_pattern must be 0/1 (got={has_bull}/{has_bear})",
            )

        raw_patterns = pf_raw.get("patterns")
        if raw_patterns is None:
            _fail_unified(symbol, stage, "patterns key missing (None)")
        if not isinstance(raw_patterns, list):
            _fail_unified(symbol, stage, f"patterns must be list (got={type(raw_patterns)})")

        # ── compact patterns (화이트리스트 + timeframe + strength float) ───────
        compact_patterns: List[Dict[str, Any]] = []
        for i, p in enumerate(raw_patterns):
            if not isinstance(p, dict):
                _fail_unified(symbol, stage, f"patterns[{i}] must be dict (got={type(p)})")
            item = _compact_pattern_item(symbol, stage, p, tf)
            compact_patterns.append(item)

            strength = float(item["strength"])
            if strength > best_strength:
                best_strength = strength
                best_pattern_name = str(item.get("pattern") or "NONE")
                best_pattern_direction = str(item.get("direction") or "NONE")
                best_pattern_confidence = str(item.get("confidence") or "none")
                best_pattern_timeframe = tf

        # 타임프레임별 상위 N개
        compact_patterns.sort(key=lambda x: float(x.get("strength", 0.0)), reverse=True)
        if MAX_PATTERNS_PER_TF > 0:
            compact_patterns = compact_patterns[:MAX_PATTERNS_PER_TF]

        all_patterns.extend(compact_patterns)

        # 타임프레임별 score 저장
        pattern_features[tf] = {
            "interval": tf,
            "pattern_score": pattern_score,
            "reversal_probability": reversal_prob,
            "continuation_probability": continuation_prob,
            "momentum_score": momentum_score,
            "volume_confirmation": volume_conf,
            "wick_strength": wick_strength,
            "liquidity_event_score": liquidity_event,
            "has_bullish_pattern": has_bull,
            "has_bearish_pattern": has_bear,
        }

        # 전역 max 갱신
        global_pattern_score = max(global_pattern_score, pattern_score)
        global_reversal = max(global_reversal, reversal_prob)
        global_continuation = max(global_continuation, continuation_prob)
        global_momentum = max(global_momentum, momentum_score)
        global_volume_conf = max(global_volume_conf, volume_conf)
        global_wick_strength = max(global_wick_strength, wick_strength)
        global_liquidity_score = max(global_liquidity_score, liquidity_event)
        has_bullish_pattern = max(has_bullish_pattern, has_bull)
        has_bearish_pattern = max(has_bearish_pattern, has_bear)

    # 전역 상위 N개
    all_patterns.sort(key=lambda x: float(x.get("strength", 0.0)), reverse=True)
    if MAX_SUMMARY_PATTERNS > 0:
        all_patterns = all_patterns[:MAX_SUMMARY_PATTERNS]

    pattern_summary: Dict[str, Any] = {
        "patterns": all_patterns,
        "best_pattern": best_pattern_name,
        "best_pattern_direction": best_pattern_direction,
        "best_pattern_confidence": best_pattern_confidence,
        "best_timeframe": best_pattern_timeframe,
        "pattern_score": global_pattern_score,
        "reversal_probability": global_reversal,
        "continuation_probability": global_continuation,
        "momentum_score": global_momentum,
        "volume_confirmation": global_volume_conf,
        "wick_strength": global_wick_strength,
        "liquidity_event_score": global_liquidity_score,
        "has_bullish_pattern": has_bullish_pattern,
        "has_bearish_pattern": has_bearish_pattern,
    }

    return pattern_features, pattern_summary


def _atr_pct_change_from_ohlcv(
    symbol: str,
    stage: str,
    ohlcv: List[Tuple[int, float, float, float, float, float]],
    period: int = 14,
) -> float:
    """
    ATR% 변화(단순 SMA 기반).
    - 최근 ATR% vs 직전 ATR% 비교 (percent 단위)
    """
    if len(ohlcv) < (period * 2 + 1):
        _fail_unified(symbol, stage, f"need >= {period*2+1} candles for ATR change (got={len(ohlcv)})")

    highs = [x[2] for x in ohlcv]
    lows = [x[3] for x in ohlcv]
    closes = [x[4] for x in ohlcv]

    tr: List[float] = []
    for i in range(len(ohlcv)):
        if i == 0:
            tr.append(highs[i] - lows[i])
        else:
            tr.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))

    tr_now = tr[-period:]
    tr_prev = tr[-(period * 2) : -period]

    atr_now = sum(tr_now) / float(period)
    atr_prev = sum(tr_prev) / float(period)
    if atr_prev <= 0:
        _fail_unified(symbol, stage, f"invalid atr_prev (<=0): {atr_prev}")

    close_now = closes[-1]
    close_prev = closes[-period - 1]
    if close_now <= 0 or close_prev <= 0:
        _fail_unified(symbol, stage, f"invalid close for atr_pct_change: now={close_now}, prev={close_prev}")

    atr_pct_now = (atr_now / close_now) * 100.0
    atr_pct_prev = (atr_prev / close_prev) * 100.0
    if not math.isfinite(atr_pct_now) or not math.isfinite(atr_pct_prev) or atr_pct_prev <= 0:
        _fail_unified(symbol, stage, f"invalid atr_pct values: now={atr_pct_now}, prev={atr_pct_prev}")

    change_pct = ((atr_pct_now - atr_pct_prev) / atr_pct_prev) * 100.0
    if not math.isfinite(change_pct):
        _fail_unified(symbol, stage, f"invalid atr_pct_change_pct: {change_pct}")

    return change_pct


def _score_trend_4h(
    symbol: str,
    tf4h: Dict[str, Any],
    ohlcv60: List[Tuple[int, float, float, float, float, float]],
) -> Dict[str, Any]:
    stage = "engine_scores.trend_4h"

    indicators = _require_dict(symbol, stage, tf4h.get("indicators"), "4h.indicators")

    price = _require_float(symbol, stage, tf4h.get("last_close"), "4h.last_close")
    ema_fast = _require_float(symbol, stage, indicators.get("ema_fast"), "4h.indicators.ema_fast")
    ema_slow = _require_float(symbol, stage, indicators.get("ema_slow"), "4h.indicators.ema_slow")

    if ema_slow <= 0:
        _fail_unified(symbol, stage, f"invalid ema_slow (<=0): {ema_slow}")

    price_vs_ema_pct = ((price - ema_slow) / ema_slow) * 100.0
    ema_spread_pct = ((ema_fast - ema_slow) / ema_slow) * 100.0

    closes = [x[4] for x in ohlcv60]
    highs = [x[2] for x in ohlcv60]
    lows = [x[3] for x in ohlcv60]

    slope_pct_60 = ((closes[-1] - closes[0]) / closes[0]) * 100.0

    # HH/HL vs LL/LH 구조 (최근 20봉 vs 직전 20봉)
    last20_high = max(highs[-20:])
    prev20_high = max(highs[-40:-20])
    last20_low = min(lows[-20:])
    prev20_low = min(lows[-40:-20])

    structure_bias = 0
    structure_state = "MIXED"
    if last20_high > prev20_high and last20_low > prev20_low:
        structure_bias = 1
        structure_state = "HH_HL"
    elif last20_high < prev20_high and last20_low < prev20_low:
        structure_bias = -1
        structure_state = "LL_LH"

    # direction 결정(EMA200 + 구조)
    direction = "NEUTRAL"
    if price > ema_slow and ema_fast > ema_slow and structure_bias >= 0:
        direction = "LONG"
    elif price < ema_slow and ema_fast < ema_slow and structure_bias <= 0:
        direction = "SHORT"

    # trend_strength 는 market_features_ws.regime.trend_strength 를 우선 사용(있으면)
    regime = tf4h.get("regime")
    if not isinstance(regime, dict):
        _fail_unified(symbol, stage, "4h.regime missing/invalid")
    trend_strength_val = _require_float(symbol, stage, regime.get("trend_strength"), "4h.regime.trend_strength")

    # 점수(0~100): 방향과 무관하게 '추세 선명도'를 수치화
    comp_price_dist = _clamp(abs(price_vs_ema_pct) * 8.0, 0.0, 30.0)  # 3.75% → 30
    comp_ema_spread = _clamp(abs(ema_spread_pct) * 10.0, 0.0, 20.0)  # 2% → 20
    comp_slope = _clamp(abs(slope_pct_60) * 5.0, 0.0, 20.0)  # 4% → 20
    comp_structure = 10.0 if structure_bias != 0 else 0.0
    comp_regime = _clamp(trend_strength_val * 20.0, 0.0, 20.0)  # 스케일 불명 → 상한 클램프

    score = _clamp(comp_price_dist + comp_ema_spread + comp_slope + comp_structure + comp_regime, 0.0, 100.0)

    return {
        "score": score,
        "direction": direction,
        "components": {
            "price_vs_ema200_pct": price_vs_ema_pct,
            "ema50_vs_ema200_pct": ema_spread_pct,
            "slope_pct_60": slope_pct_60,
            "structure_state": structure_state,
            "trend_strength": trend_strength_val,
            "score_price_dist": comp_price_dist,
            "score_ema_spread": comp_ema_spread,
            "score_slope": comp_slope,
            "score_structure": comp_structure,
            "score_regime": comp_regime,
        },
    }


def _score_momentum_1h(
    symbol: str,
    tf1h: Dict[str, Any],
    ohlcv60: List[Tuple[int, float, float, float, float, float]],
) -> Dict[str, Any]:
    stage = "engine_scores.momentum_1h"

    indicators = _require_dict(symbol, stage, tf1h.get("indicators"), "1h.indicators")

    price = _require_float(symbol, stage, tf1h.get("last_close"), "1h.last_close")
    rsi = _require_float(symbol, stage, indicators.get("rsi"), "1h.indicators.rsi")
    macd_hist = _require_float(symbol, stage, indicators.get("macd_hist"), "1h.indicators.macd_hist")
    atr_pct = _require_float(symbol, stage, indicators.get("atr_pct"), "1h.indicators.atr_pct")

    if price <= 0:
        _fail_unified(symbol, stage, f"invalid price (<=0): {price}")

    # RSI 편차(50 기준) → 모멘텀 강도
    rsi_dev = abs(rsi - 50.0)  # 0~50
    comp_rsi = _clamp(rsi_dev * 1.6, 0.0, 40.0)  # 25 → 40

    # MACD hist 크기(가격 대비) → 모멘텀
    macd_hist_pct = (abs(macd_hist) / price) * 100.0
    comp_macd = _clamp(macd_hist_pct * 300.0, 0.0, 30.0)  # 0.1% → 30

    closes = [x[4] for x in ohlcv60]
    # 최근 10봉 모멘텀(절대값)
    if len(closes) < 10:
        _fail_unified(symbol, stage, f"need >=10 closes for momentum (got={len(closes)})")

    mom_pct_10 = abs((closes[-1] - closes[-10]) / closes[-10]) * 100.0
    comp_price_mom = _clamp(mom_pct_10 * 10.0, 0.0, 20.0)  # 2% → 20

    # ATR% 자체는 환경(움직임 여지)로 사용 (변화는 별도 산출)
    comp_atr_env = _clamp(atr_pct * 50.0, 0.0, 10.0)  # 0.2% → 10

    # ATR% 변화(최근 vs 직전)
    atr_chg_pct = _atr_pct_change_from_ohlcv(symbol, stage, ohlcv60, period=14)
    comp_atr_chg = _clamp(abs(atr_chg_pct) * 0.4, 0.0, 10.0)  # 25% 변화 → 10

    score = _clamp(comp_rsi + comp_macd + comp_price_mom + comp_atr_env + comp_atr_chg, 0.0, 100.0)

    return {
        "score": score,
        "components": {
            "rsi": rsi,
            "rsi_dev": rsi_dev,
            "macd_hist": macd_hist,
            "macd_hist_pct": macd_hist_pct,
            "mom_pct_10": mom_pct_10,
            "atr_pct": atr_pct,
            "atr_pct_change_pct": atr_chg_pct,
            "score_rsi": comp_rsi,
            "score_macd": comp_macd,
            "score_price_mom": comp_price_mom,
            "score_atr_env": comp_atr_env,
            "score_atr_change": comp_atr_chg,
        },
    }


def _score_structure_15m(
    symbol: str,
    tf15m: Dict[str, Any],
    ohlcv20: List[Tuple[int, float, float, float, float, float]],
) -> Dict[str, Any]:
    stage = "engine_scores.structure_15m"

    price = _require_float(symbol, stage, tf15m.get("last_close"), "15m.last_close")
    range_pct = _require_float(symbol, stage, tf15m.get("range_pct"), "15m.range_pct")

    closes = [x[4] for x in ohlcv20]
    highs = [x[2] for x in ohlcv20]
    lows = [x[3] for x in ohlcv20]

    prev_high = max(highs[:-1])
    prev_low = min(lows[:-1])

    breakout_up = price > prev_high
    breakout_down = price < prev_low

    slope_pct_20 = ((closes[-1] - closes[0]) / closes[0]) * 100.0
    slope_abs = abs(slope_pct_20)

    state = "RANGE"
    if breakout_up:
        state = "BREAKOUT_UP"
    elif breakout_down:
        state = "BREAKOUT_DOWN"
    else:
        # range_pct 로 박스/추세 단순 분류
        if range_pct >= 1.2 and slope_abs >= 0.6:
            state = "TRENDING"
        elif range_pct >= 1.2:
            state = "WIDE_RANGE"
        else:
            state = "TIGHT_RANGE"

    comp_breakout = 40.0 if (breakout_up or breakout_down) else 0.0
    comp_slope = _clamp(slope_abs * 12.0, 0.0, 30.0)  # 2.5% → 30
    comp_range = _clamp(range_pct * 10.0, 0.0, 30.0)  # 3% → 30

    score = _clamp(comp_breakout + comp_slope + comp_range, 0.0, 100.0)

    return {
        "score": score,
        "state": state,
        "components": {
            "prev_high": prev_high,
            "prev_low": prev_low,
            "breakout_up": 1 if breakout_up else 0,
            "breakout_down": 1 if breakout_down else 0,
            "slope_pct_20": slope_pct_20,
            "range_pct": range_pct,
            "score_breakout": comp_breakout,
            "score_slope": comp_slope,
            "score_range": comp_range,
        },
    }


def _score_timing_5m(
    symbol: str,
    tf5m: Dict[str, Any],
    ohlcv20: List[Tuple[int, float, float, float, float, float]],
    pattern_summary: Dict[str, Any],
    pattern_features_5m: Dict[str, Any],
) -> Dict[str, Any]:
    stage = "engine_scores.timing_5m"

    indicators = _require_dict(symbol, stage, tf5m.get("indicators"), "5m.indicators")
    atr_pct = _require_float(symbol, stage, indicators.get("atr_pct"), "5m.indicators.atr_pct")

    closes = [x[4] for x in ohlcv20]
    highs = [x[2] for x in ohlcv20]
    lows = [x[3] for x in ohlcv20]

    if len(closes) < 6:
        _fail_unified(symbol, stage, f"need >=6 closes for 5m timing (got={len(closes)})")

    mom_pct_5 = abs((closes[-1] - closes[-6]) / closes[-6]) * 100.0

    # 변동성 확장/수축: 최근 5봉 평균 range vs 이전 5봉 평균 range
    def _avg_range_pct(seg_high: List[float], seg_low: List[float], seg_close: List[float]) -> float:
        if len(seg_high) != len(seg_low) or len(seg_low) != len(seg_close) or not seg_close:
            _fail_unified(symbol, stage, "invalid segment lengths for range_pct")
        vals: List[float] = []
        for h, l, c in zip(seg_high, seg_low, seg_close):
            if c <= 0:
                _fail_unified(symbol, stage, "close<=0 in range segment")
            vals.append(((h - l) / c) * 100.0)
        return sum(vals) / float(len(vals))

    recent_range = _avg_range_pct(highs[-5:], lows[-5:], closes[-5:])
    prev_range = _avg_range_pct(highs[-10:-5], lows[-10:-5], closes[-10:-5])
    if prev_range <= 0:
        _fail_unified(symbol, stage, f"invalid prev_range (<=0): {prev_range}")

    range_ratio = recent_range / prev_range

    # 패턴 스코어(5m + 전역)
    p5_score = _require_prob_0_1(symbol, stage, pattern_features_5m.get("pattern_score"), "5m.pattern_score")
    global_pattern_score = _require_prob_0_1(
        symbol, stage, pattern_summary.get("pattern_score"), "pattern_summary.pattern_score"
    )

    comp_pattern = _clamp(max(p5_score, global_pattern_score) * 40.0, 0.0, 40.0)
    comp_momentum = _clamp(mom_pct_5 * 12.0, 0.0, 25.0)  # 2.1% → 25
    comp_atr_env = _clamp(atr_pct * 80.0, 0.0, 15.0)  # 0.1875% → 15

    # range_ratio 가 1보다 크면 확장(진입 타이밍에 유리한 경우가 많음)
    comp_expansion = 0.0
    if range_ratio >= 1.20:
        comp_expansion = 15.0
    elif range_ratio >= 1.05:
        comp_expansion = 8.0

    score = _clamp(comp_pattern + comp_momentum + comp_atr_env + comp_expansion, 0.0, 100.0)

    return {
        "score": score,
        "components": {
            "mom_pct_5": mom_pct_5,
            "atr_pct": atr_pct,
            "recent_range_pct": recent_range,
            "prev_range_pct": prev_range,
            "range_ratio": range_ratio,
            "pattern_score_5m": p5_score,
            "pattern_score_global": global_pattern_score,
            "score_pattern": comp_pattern,
            "score_momentum": comp_momentum,
            "score_atr_env": comp_atr_env,
            "score_vol_expansion": comp_expansion,
        },
    }


def _score_orderbook_micro(symbol: str, orderbook: Dict[str, Any]) -> Dict[str, Any]:
    stage = "engine_scores.orderbook_micro"

    spread_pct = _require_float(symbol, stage, orderbook.get("spread_pct"), "orderbook.spread_pct")
    imbalance_qty_pct = _require_float(symbol, stage, orderbook.get("imbalance_qty_pct"), "orderbook.imbalance_qty_pct")
    depth_imbalance = _require_float(symbol, stage, orderbook.get("depth_imbalance"), "orderbook.depth_imbalance")

    # spreadPct 낮을수록 좋다.
    spread_score = 0.0
    if spread_pct <= 0.02:
        spread_score = 40.0
    elif spread_pct <= 0.05:
        spread_score = 30.0
    elif spread_pct <= 0.10:
        spread_score = 20.0
    elif spread_pct <= 0.20:
        spread_score = 10.0
    else:
        spread_score = 0.0

    # imbalance(절대값) 클수록 한쪽 우세. 과도하면 불안정할 수 있으나, 상한 클램프.
    imbalance_score = _clamp(abs(imbalance_qty_pct) * 0.6, 0.0, 30.0)  # 50% → 30
    depth_score = _clamp(abs(depth_imbalance) * 30.0, 0.0, 30.0)  # 1.0 → 30

    # 정상 오더북(이미 strict 검증 통과) → 품질 보너스
    quality_score = 10.0

    score = _clamp(spread_score + imbalance_score + depth_score + quality_score, 0.0, 100.0)

    return {
        "score": score,
        "components": {
            "spread_pct": spread_pct,
            "imbalance_qty_pct": imbalance_qty_pct,
            "depth_imbalance": depth_imbalance,
            "score_spread": spread_score,
            "score_imbalance": imbalance_score,
            "score_depth": depth_score,
            "score_quality": quality_score,
        },
    }


def _build_engine_scores_strict(
    symbol: str,
    timeframes: Dict[str, Any],
    pattern_features: Dict[str, Dict[str, Any]],
    pattern_summary: Dict[str, Any],
    orderbook: Dict[str, Any],
) -> Dict[str, Any]:
    """
    STRICT 요구사항(C):
    - 멀티TF 엔진 점수(0~100) 산출
    - total 은 고정 weights 로 합산
    """
    # 4h (trend)
    tf4h = _require_dict(symbol, "engine_scores", timeframes.get("4h"), "timeframes['4h']")
    ohlcv4h = _parse_ohlcv_rows(
        symbol, "engine_scores", tf4h.get("raw_ohlcv_last60"), min_len=60, name="4h.raw_ohlcv_last60"
    )
    trend_4h = _score_trend_4h(symbol, tf4h, ohlcv4h)

    # 1h (momentum)
    tf1h = _require_dict(symbol, "engine_scores", timeframes.get("1h"), "timeframes['1h']")
    ohlcv1h = _parse_ohlcv_rows(
        symbol, "engine_scores", tf1h.get("raw_ohlcv_last60"), min_len=60, name="1h.raw_ohlcv_last60"
    )
    momentum_1h = _score_momentum_1h(symbol, tf1h, ohlcv1h)

    # 15m (structure)
    tf15 = _require_dict(symbol, "engine_scores", timeframes.get("15m"), "timeframes['15m']")
    ohlcv15 = _parse_ohlcv_rows(
        symbol, "engine_scores", tf15.get("raw_ohlcv_last20"), min_len=20, name="15m.raw_ohlcv_last20"
    )
    structure_15m = _score_structure_15m(symbol, tf15, ohlcv15)

    # 5m (timing)
    tf5 = _require_dict(symbol, "engine_scores", timeframes.get("5m"), "timeframes['5m']")
    ohlcv5 = _parse_ohlcv_rows(
        symbol, "engine_scores", tf5.get("raw_ohlcv_last20"), min_len=20, name="5m.raw_ohlcv_last20"
    )
    pf5 = _require_dict(symbol, "engine_scores", pattern_features.get("5m"), "pattern_features['5m']")
    timing_5m = _score_timing_5m(symbol, tf5, ohlcv5, pattern_summary, pf5)

    # orderbook micro
    orderbook_micro = _score_orderbook_micro(symbol, orderbook)

    # total weighted
    w = _TOTAL_WEIGHTS
    total_score = (
        float(trend_4h["score"]) * w["trend_4h"]
        + float(momentum_1h["score"]) * w["momentum_1h"]
        + float(structure_15m["score"]) * w["structure_15m"]
        + float(timing_5m["score"]) * w["timing_5m"]
        + float(orderbook_micro["score"]) * w["orderbook_micro"]
    )
    total_score = _clamp(total_score, 0.0, 100.0)

    return {
        "trend_4h": trend_4h,
        "momentum_1h": momentum_1h,
        "structure_15m": structure_15m,
        "timing_5m": timing_5m,
        "orderbook_micro": orderbook_micro,
        "total": {"score": total_score, "weights": dict(w)},
    }


def _compact_indicators_strict(symbol: str, tf: str, indicators: Dict[str, Any]) -> Dict[str, Any]:
    """
    STRICT:
    - indicators dict 필수 키를 포함해야 한다(없으면 실패)
    - series(대형 리스트)는 출력에서 제거(토큰 최적화)
    """
    stage = f"timeframes[{tf}].indicators"
    out: Dict[str, Any] = {}

    for k in _INDICATOR_KEEP_KEYS:
        if k not in indicators:
            _fail_unified(symbol, stage, f"missing indicator key: {k}")
        out[k] = _require_float(symbol, stage, indicators.get(k), f"indicators.{k}")

    return out


def _compact_timeframes_for_output(symbol: str, timeframes: Dict[str, Any]) -> Dict[str, Any]:
    """
    STRICT 요구사항(D):
    - raw_ohlcv_last20/60 유지
    - 불필요한 대형 배열/디버그 시리즈 제거
    - 출력은 REQUIRED_TFS 중심으로 축소(토큰 최적화)
    """
    out: Dict[str, Any] = {}

    for tf in REQUIRED_TFS:
        stage = f"timeframes[{tf}]"
        tf_data = _require_dict(symbol, stage, timeframes.get(tf), f"timeframes['{tf}']")

        tf_out: Dict[str, Any] = {}

        # 최소 필요 키는 그대로 유지(스칼라/작은 dict)
        for k, v in tf_data.items():
            if k in ("candles",):
                continue  # 중복/대형
            if k.startswith("debug") or k.endswith("_series"):
                continue
            if k == "indicators":
                continue  # 아래에서 별도 compact

            # raw series는 필수만 유지
            if k == "raw_ohlcv_last20" and tf not in ("1m", "5m", "15m"):
                continue
            if k == "raw_ohlcv_last60" and tf not in ("1h", "4h"):
                continue

            # 기타 list/tuple 가 과도하게 크면 제거 (필수 raw 제외)
            if isinstance(v, list) and len(v) > 200 and k not in ("raw_ohlcv_last20", "raw_ohlcv_last60"):
                continue

            # None 값은 출력에서 제거(토큰+해석 안정성)
            if v is None:
                continue

            tf_out[k] = v

        # indicators compact
        indicators = _require_dict(symbol, stage, tf_data.get("indicators"), f"{tf}.indicators")
        tf_out["indicators"] = _compact_indicators_strict(symbol, tf, indicators)

        # raw_ohlcv 필수 유지/정규화(리스트 of list)
        if tf in ("1m", "5m", "15m"):
            raw20 = tf_data.get("raw_ohlcv_last20")
            parsed20 = _parse_ohlcv_rows(symbol, stage, raw20, min_len=20, name=f"{tf}.raw_ohlcv_last20")
            tf_out["raw_ohlcv_last20"] = [[ts, o, h, l, c, v] for (ts, o, h, l, c, v) in parsed20]
        elif tf in ("1h", "4h"):
            raw60 = tf_data.get("raw_ohlcv_last60")
            parsed60 = _parse_ohlcv_rows(symbol, stage, raw60, min_len=60, name=f"{tf}.raw_ohlcv_last60")
            tf_out["raw_ohlcv_last60"] = [[ts, o, h, l, c, v] for (ts, o, h, l, c, v) in parsed60]

        out[tf] = tf_out

    return out


def build_unified_features(symbol: Optional[str] = None) -> Dict[str, Any]:
    """
    STRICT · NO-FALLBACK:
    - base(market_features_ws) + pattern_detection + engine_scores 를 통합한다.
    - 필수 데이터 누락/손상 시 즉시 UnifiedFeaturesError.
    """
    sym = str(symbol or getattr(SET, "symbol", "")).strip()
    if not sym:
        _fail_unified("UNKNOWN", "symbol", "symbol is empty (provide symbol or set settings.symbol)")

    # 1) WS 기반 기본 마켓 피처 생성
    try:
        base = build_entry_features_ws(sym)
    except FeatureBuildError as e:
        _fail_unified(sym, "build_entry_features_ws", f"FeatureBuildError: {e}", e)
    except Exception as e:
        _fail_unified(sym, "build_entry_features_ws", f"unexpected error: {e!r}", e)

    if not isinstance(base, dict) or not base:
        _fail_unified(sym, "build_entry_features_ws", "returned non-dict or empty")

    timeframes = base.get("timeframes")
    if not isinstance(timeframes, dict) or not timeframes:
        _fail_unified(sym, "timeframes", "timeframes missing/invalid")

    # 2) 필수 타임프레임/데이터 STRICT 검증 + (1h/4h raw_ohlcv_last60 생성)
    _ensure_timeframe_payloads_strict(sym, timeframes)

    # 3) 오더북 STRICT 확보 (bids/asks 포함) — base["orderbook"] 대신 WS snapshot 기반으로 재구성
    orderbook = _get_orderbook_strict(sym, limit=5)

    # 4) 패턴 번들 생성 (STRICT)
    pattern_features, pattern_summary = _build_pattern_bundle_strict(sym, timeframes, orderbook)

    # 5) 엔진 1차 멀티TF 점수 (STRICT)
    engine_scores = _build_engine_scores_strict(sym, timeframes, pattern_features, pattern_summary, orderbook)

    # 6) 토큰 최적화: timeframes 축소/정리 (필수 raw + scalar indicators 유지)
    compact_timeframes = _compact_timeframes_for_output(sym, timeframes)

    # 7) 최종 dict 구성
    result: Dict[str, Any] = dict(base)
    result["timeframes"] = compact_timeframes
    result["orderbook"] = orderbook
    result["pattern_features"] = pattern_features
    result["pattern_summary"] = pattern_summary
    result["engine_scores"] = engine_scores

    # (운영 권고) settings/ws_subscribe_tfs 에 반드시 포함:
    #   WS_SUBSCRIBE_TFS="1m,5m,15m,1h,4h"
    # (운영 권고) 부팅 안정화용 WS 백필에도 포함:
    #   ws_backfill_tfs=["1m","5m","15m","1h","4h"]

    return result


__all__ = ["UnifiedFeaturesError", "build_unified_features"]