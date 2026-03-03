# strategy/score_engine.py
"""
========================================================
FILE: strategy/score_engine.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================
역할:
- 엔진 1차 정량 점수(engine_scores)를 계산한다.
- 멀티 타임프레임/오더북/패턴 요약을 0~100 점수와 해석 가능한 컴포넌트로 정리해 제공한다.
- "매매 결정"은 하지 않는다(상위 레이어 책임).

핵심 원칙 (STRICT · NO-FALLBACK):
- REST 호출/백필/외부 폴백 금지.
- 더미값 생성/0 치환/None 반환 금지.
- 필수 입력 누락/손상/타입 불일치 시 즉시 예외(ScoreEngineError).
- 예외 삼키기 금지(조용히 진행 금지). 필요한 경우 명시적으로 raise 한다.
- 점수는 0~100로 클램핑하며, total은 고정 가중치로 합산한다.

입력 전제:
- timeframes: "4h","1h","15m","5m" 키 포함
- pattern_features: {"5m": {...}} 포함
- pattern_summary: {"pattern_score": 0~1} 포함
- orderbook: bids/asks 또는 bestBid/bestAsk/spreadPct 등 포함

가중치(고정):
- trend_4h        0.30
- momentum_1h     0.20
- structure_15m   0.20
- timing_5m       0.20
- orderbook_micro 0.10

변경 이력
--------------------------------------------------------
- 2026-03-03 (TRADE-GRADE):
  1) orderbook 키 정합 강화(STRICT, 폴백 아님):
     - bestBid/bestAsk/spreadPct/depthImbalance/imbalanceQtyPct 등 camelCase/snake_case 모두 허용
     - 존재하는 키만 선택(0/False를 “없음”으로 오해하지 않음)
  2) total weight 무결성 체크 추가(합=1.0 강제)
========================================================
"""

import math
from typing import Any, Dict, List, NoReturn, Optional, Tuple

try:
    from infra.telelog import log
except Exception as e:  # pragma: no cover
    raise RuntimeError("infra.telelog import failed (STRICT · NO-FALLBACK)") from e


TOTAL_WEIGHTS: Dict[str, float] = {
    "trend_4h": 0.30,
    "momentum_1h": 0.20,
    "structure_15m": 0.20,
    "timing_5m": 0.20,
    "orderbook_micro": 0.10,
}


class ScoreEngineError(RuntimeError):
    """엔진 점수 계산(멀티 TF/오더북/패턴) 단계에서 사용하는 예외."""


def _fail(symbol: str, stage: str, reason: str, exc: Optional[BaseException] = None) -> NoReturn:
    msg = f"[SCORE_ENGINE][{symbol}] {stage} 실패: {reason}"
    log(msg)
    if exc is None:
        raise ScoreEngineError(msg)
    raise ScoreEngineError(msg) from exc


def _clamp(v: float, lo: float, hi: float) -> float:
    if v < lo:
        return lo
    if v > hi:
        return hi
    return v


def _pick_first_key(d: Dict[str, Any], keys: Tuple[str, ...]) -> Any:
    """
    STRICT:
    - 0/False를 “없음”으로 취급하지 않기 위해 key 존재 여부로만 선택한다.
    """
    for k in keys:
        if k in d:
            return d[k]
    return None


def _require_dict(symbol: str, stage: str, v: Any, name: str) -> Dict[str, Any]:
    if not isinstance(v, dict):
        _fail(symbol, stage, f"{name} must be dict (got={type(v).__name__})")
    if not v:
        _fail(symbol, stage, f"{name} is empty")
    return v


def _require_list(symbol: str, stage: str, v: Any, name: str, min_len: int) -> List[Any]:
    if not isinstance(v, list):
        _fail(symbol, stage, f"{name} must be list (got={type(v).__name__})")
    if len(v) < min_len:
        _fail(symbol, stage, f"{name} length<{min_len} (got={len(v)})")
    return v


def _require_int(symbol: str, stage: str, v: Any, name: str) -> int:
    if v is None:
        _fail(symbol, stage, f"{name} is None")
    if isinstance(v, bool):
        _fail(symbol, stage, f"{name} must be int (bool not allowed)")
    try:
        iv = int(v)
    except Exception as e:
        _fail(symbol, stage, f"{name} must be int (got={v!r})", e)
    return iv


def _require_float(symbol: str, stage: str, v: Any, name: str) -> float:
    if v is None:
        _fail(symbol, stage, f"{name} is None")
    if isinstance(v, bool):
        _fail(symbol, stage, f"{name} must be float (bool not allowed)")
    try:
        fv = float(v)
    except Exception as e:
        _fail(symbol, stage, f"{name} must be float (got={v!r})", e)
    if not math.isfinite(fv):
        _fail(symbol, stage, f"{name} must be finite (got={fv})")
    return fv


def _require_prob_0_1(symbol: str, stage: str, v: Any, name: str) -> float:
    fv = _require_float(symbol, stage, v, name)
    if fv < 0.0 or fv > 1.0:
        _fail(symbol, stage, f"{name} out of range [0,1] (got={fv})")
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

    sliced = raw[-min_len:]

    for i, row in enumerate(sliced):
        if not isinstance(row, (list, tuple)) or len(row) < 6:
            _fail(symbol, stage, f"{name}[{i}] must be [ts,o,h,l,c,v] (got={row!r})")

        ts = _require_int(symbol, stage, row[0], f"{name}[{i}].ts")
        o = _require_float(symbol, stage, row[1], f"{name}[{i}].open")
        h = _require_float(symbol, stage, row[2], f"{name}[{i}].high")
        l = _require_float(symbol, stage, row[3], f"{name}[{i}].low")
        c = _require_float(symbol, stage, row[4], f"{name}[{i}].close")
        v = _require_float(symbol, stage, row[5], f"{name}[{i}].volume")

        if ts <= 0:
            _fail(symbol, stage, f"{name}[{i}] invalid ts (<=0): {ts}")
        if o <= 0 or h <= 0 or l <= 0 or c <= 0:
            _fail(symbol, stage, f"{name}[{i}] invalid OHLC (<=0): o={o},h={h},l={l},c={c}")
        if h < l:
            _fail(symbol, stage, f"{name}[{i}] invalid candle (high<low): h={h}, l={l}")
        if h < max(o, c) or l > min(o, c):
            _fail(symbol, stage, f"{name}[{i}] invalid candle bounds: o={o},h={h},l={l},c={c}")
        if v < 0:
            _fail(symbol, stage, f"{name}[{i}] invalid volume (<0): v={v}")

        out.append((ts, o, h, l, c, v))

    return out


def _atr_pct_change_from_ohlcv(
    symbol: str,
    stage: str,
    ohlcv: List[Tuple[int, float, float, float, float, float]],
    period: int = 14,
) -> float:
    """
    ATR% 변화(단순 SMA 기반).
    - 최근 ATR% vs 직전 ATR% 변화율(%)
    """
    need = period * 2 + 1
    if len(ohlcv) < need:
        _fail(symbol, stage, f"need >= {need} candles for atr_pct_change (got={len(ohlcv)})")

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
        _fail(symbol, stage, f"invalid atr_prev (<=0): {atr_prev}")

    close_now = closes[-1]
    close_prev = closes[-period - 1]
    if close_now <= 0 or close_prev <= 0:
        _fail(symbol, stage, f"invalid close for atr_pct_change: now={close_now}, prev={close_prev}")

    atr_pct_now = (atr_now / close_now) * 100.0
    atr_pct_prev = (atr_prev / close_prev) * 100.0
    if not math.isfinite(atr_pct_now) or not math.isfinite(atr_pct_prev) or atr_pct_prev <= 0:
        _fail(symbol, stage, f"invalid atr_pct values: now={atr_pct_now}, prev={atr_pct_prev}")

    change_pct = ((atr_pct_now - atr_pct_prev) / atr_pct_prev) * 100.0
    if not math.isfinite(change_pct):
        _fail(symbol, stage, f"invalid atr_pct_change_pct: {change_pct}")

    return change_pct


def _compute_range_pct(
    symbol: str,
    stage: str,
    ohlcv: List[Tuple[int, float, float, float, float, float]],
) -> float:
    highs = [x[2] for x in ohlcv]
    lows = [x[3] for x in ohlcv]
    close = ohlcv[-1][4]
    if close <= 0:
        _fail(symbol, stage, f"invalid close for range_pct: {close}")
    rp = ((max(highs) - min(lows)) / close) * 100.0
    if not math.isfinite(rp) or rp < 0:
        _fail(symbol, stage, f"invalid range_pct: {rp}")
    return rp


def _best_prices_from_orderbook_rows(
    symbol: str,
    stage: str,
    bids: List[Any],
    asks: List[Any],
) -> Tuple[float, float]:
    nbids: List[List[float]] = []
    nasks: List[List[float]] = []

    for i, row in enumerate(bids):
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            _fail(symbol, stage, f"bids[{i}] invalid row: {row!r}")
        px = _require_float(symbol, stage, row[0], f"bids[{i}].price")
        qty = _require_float(symbol, stage, row[1], f"bids[{i}].qty")
        if px <= 0 or qty < 0:
            _fail(symbol, stage, f"bids[{i}] invalid values: price={px}, qty={qty}")
        nbids.append([px, qty])

    for i, row in enumerate(asks):
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            _fail(symbol, stage, f"asks[{i}] invalid row: {row!r}")
        px = _require_float(symbol, stage, row[0], f"asks[{i}].price")
        qty = _require_float(symbol, stage, row[1], f"asks[{i}].qty")
        if px <= 0 or qty < 0:
            _fail(symbol, stage, f"asks[{i}] invalid values: price={px}, qty={qty}")
        nasks.append([px, qty])

    if not nbids or not nasks:
        _fail(symbol, stage, "orderbook bids/asks empty")

    best_bid = max(r[0] for r in nbids)
    best_ask = min(r[0] for r in nasks)
    if best_ask <= best_bid:
        _fail(symbol, stage, f"crossed book: bestAsk({best_ask}) <= bestBid({best_bid})")

    return best_bid, best_ask


def _orderbook_metrics_strict(symbol: str, orderbook: Dict[str, Any]) -> Dict[str, float]:
    """
    STRICT:
    - best_bid/best_ask/spread_pct/depth_imbalance/imbalance_qty_pct는 "존재하면 사용", 없으면 bids/asks로 계산.
    - camelCase/snake_case 모두 허용(키 alias 처리; 폴백/대체값 주입 아님).
    """
    stage = "engine_scores.orderbook_micro"

    best_bid = _pick_first_key(orderbook, ("best_bid", "bestBid"))
    best_ask = _pick_first_key(orderbook, ("best_ask", "bestAsk"))

    if best_bid is None or best_ask is None:
        bids = _pick_first_key(orderbook, ("bids",))
        asks = _pick_first_key(orderbook, ("asks",))
        if not isinstance(bids, list) or not isinstance(asks, list):
            _fail(symbol, stage, "orderbook missing bestBid/bestAsk and bids/asks (cannot compute)")
        bb, ba = _best_prices_from_orderbook_rows(symbol, stage, bids, asks)
        best_bid = bb
        best_ask = ba
    else:
        best_bid = _require_float(symbol, stage, best_bid, "orderbook.best_bid")
        best_ask = _require_float(symbol, stage, best_ask, "orderbook.best_ask")
        if best_bid <= 0 or best_ask <= 0 or best_ask <= best_bid:
            _fail(symbol, stage, f"invalid best prices: best_bid={best_bid}, best_ask={best_ask}")

    spread_pct = _pick_first_key(orderbook, ("spread_pct", "spreadPct"))
    if spread_pct is None:
        mid = (best_bid + best_ask) / 2.0
        if mid <= 0:
            _fail(symbol, stage, f"invalid mid for spread_pct: {mid}")
        spread_pct = ((best_ask - best_bid) / mid) * 100.0
    spread_pct = _require_float(symbol, stage, spread_pct, "orderbook.spread_pct")
    if spread_pct < 0 or not math.isfinite(spread_pct):
        _fail(symbol, stage, f"invalid spread_pct: {spread_pct}")

    depth_imbalance = _pick_first_key(orderbook, ("depth_imbalance", "depthImbalance"))
    if depth_imbalance is None:
        bids = _pick_first_key(orderbook, ("bids",))
        asks = _pick_first_key(orderbook, ("asks",))
        if not isinstance(bids, list) or not isinstance(asks, list):
            _fail(symbol, stage, "orderbook missing depth_imbalance and bids/asks (cannot compute)")
        bid_notional = 0.0
        ask_notional = 0.0
        for i, r in enumerate(bids):
            if not isinstance(r, (list, tuple)) or len(r) < 2:
                _fail(symbol, stage, f"bids[{i}] invalid row: {r!r}")
            px = _require_float(symbol, stage, r[0], f"bids[{i}].price")
            qty = _require_float(symbol, stage, r[1], f"bids[{i}].qty")
            if px <= 0 or qty < 0:
                _fail(symbol, stage, f"bids[{i}] invalid values: price={px}, qty={qty}")
            bid_notional += px * qty
        for i, r in enumerate(asks):
            if not isinstance(r, (list, tuple)) or len(r) < 2:
                _fail(symbol, stage, f"asks[{i}] invalid row: {r!r}")
            px = _require_float(symbol, stage, r[0], f"asks[{i}].price")
            qty = _require_float(symbol, stage, r[1], f"asks[{i}].qty")
            if px <= 0 or qty < 0:
                _fail(symbol, stage, f"asks[{i}] invalid values: price={px}, qty={qty}")
            ask_notional += px * qty
        total = bid_notional + ask_notional
        if total <= 0:
            _fail(symbol, stage, "invalid notional_total (<=0) while computing depth_imbalance")
        depth_imbalance = (bid_notional - ask_notional) / total

    depth_imbalance = _require_float(symbol, stage, depth_imbalance, "orderbook.depth_imbalance")
    if not math.isfinite(depth_imbalance) or depth_imbalance < -1.0 or depth_imbalance > 1.0:
        _fail(symbol, stage, f"invalid depth_imbalance (range/finite): {depth_imbalance}")

    imbalance_qty_pct = _pick_first_key(orderbook, ("imbalance_qty_pct", "imbalanceQtyPct"))
    if imbalance_qty_pct is None:
        bids = _pick_first_key(orderbook, ("bids",))
        asks = _pick_first_key(orderbook, ("asks",))
        if isinstance(bids, list) and isinstance(asks, list) and bids and asks:
            bid_qty = 0.0
            ask_qty = 0.0
            for i, r in enumerate(bids):
                if not isinstance(r, (list, tuple)) or len(r) < 2:
                    _fail(symbol, stage, f"bids[{i}] invalid row: {r!r}")
                qty = _require_float(symbol, stage, r[1], f"bids[{i}].qty")
                if qty < 0:
                    _fail(symbol, stage, f"bids[{i}] invalid qty (<0): {qty}")
                bid_qty += qty
            for i, r in enumerate(asks):
                if not isinstance(r, (list, tuple)) or len(r) < 2:
                    _fail(symbol, stage, f"asks[{i}] invalid row: {r!r}")
                qty = _require_float(symbol, stage, r[1], f"asks[{i}].qty")
                if qty < 0:
                    _fail(symbol, stage, f"asks[{i}] invalid qty (<0): {qty}")
                ask_qty += qty
            total_qty = bid_qty + ask_qty
            if total_qty <= 0:
                _fail(symbol, stage, "invalid total_qty (<=0) while computing imbalance_qty_pct")
            imbalance_qty_pct = ((bid_qty - ask_qty) / total_qty) * 100.0
        else:
            _fail(symbol, stage, "orderbook missing imbalance_qty_pct and bids/asks (cannot compute)")

    imbalance_qty_pct = _require_float(symbol, stage, imbalance_qty_pct, "orderbook.imbalance_qty_pct")
    if not math.isfinite(imbalance_qty_pct):
        _fail(symbol, stage, f"invalid imbalance_qty_pct: {imbalance_qty_pct}")

    return {
        "best_bid": float(best_bid),
        "best_ask": float(best_ask),
        "spread_pct": float(spread_pct),
        "depth_imbalance": float(depth_imbalance),
        "imbalance_qty_pct": float(imbalance_qty_pct),
    }


def score_trend_4h(symbol: str, tf4h: Dict[str, Any]) -> Dict[str, Any]:
    stage = "engine_scores.trend_4h"

    last_close = _require_float(symbol, stage, tf4h.get("last_close"), "4h.last_close")
    indicators = _require_dict(symbol, stage, tf4h.get("indicators"), "4h.indicators")

    ema_fast = _require_float(symbol, stage, indicators.get("ema_fast"), "4h.indicators.ema_fast")
    ema_slow = _require_float(symbol, stage, indicators.get("ema_slow"), "4h.indicators.ema_slow")

    raw60 = tf4h.get("raw_ohlcv_last60")
    ohlcv60 = _parse_ohlcv_rows(symbol, stage, raw60, min_len=60, name="4h.raw_ohlcv_last60")

    if ema_slow <= 0:
        _fail(symbol, stage, f"invalid ema_slow (<=0): {ema_slow}")

    price_vs_ema_pct = ((last_close - ema_slow) / ema_slow) * 100.0
    ema_spread_pct = ((ema_fast - ema_slow) / ema_slow) * 100.0

    closes = [x[4] for x in ohlcv60]
    highs = [x[2] for x in ohlcv60]
    lows = [x[3] for x in ohlcv60]

    slope_pct_60 = ((closes[-1] - closes[0]) / closes[0]) * 100.0

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

    direction = "NEUTRAL"
    if last_close > ema_slow and ema_fast > ema_slow and structure_bias >= 0:
        direction = "LONG"
    elif last_close < ema_slow and ema_fast < ema_slow and structure_bias <= 0:
        direction = "SHORT"

    comp_price_dist = _clamp(abs(price_vs_ema_pct) * 8.0, 0.0, 30.0)
    comp_ema_spread = _clamp(abs(ema_spread_pct) * 10.0, 0.0, 20.0)
    comp_slope = _clamp(abs(slope_pct_60) * 5.0, 0.0, 20.0)
    comp_structure = 10.0 if structure_bias != 0 else 0.0
    comp_align = 20.0 if (ema_fast > ema_slow and last_close > ema_slow) or (ema_fast < ema_slow and last_close < ema_slow) else 0.0

    score = _clamp(comp_price_dist + comp_ema_spread + comp_slope + comp_structure + comp_align, 0.0, 100.0)

    return {
        "score": score,
        "direction": direction,
        "components": {
            "price_vs_ema200_pct": price_vs_ema_pct,
            "ema50_vs_ema200_pct": ema_spread_pct,
            "slope_pct_60": slope_pct_60,
            "structure_state": structure_state,
            "score_price_dist": comp_price_dist,
            "score_ema_spread": comp_ema_spread,
            "score_slope": comp_slope,
            "score_structure": comp_structure,
            "score_alignment": comp_align,
        },
    }


def score_momentum_1h(symbol: str, tf1h: Dict[str, Any]) -> Dict[str, Any]:
    stage = "engine_scores.momentum_1h"

    last_close = _require_float(symbol, stage, tf1h.get("last_close"), "1h.last_close")
    indicators = _require_dict(symbol, stage, tf1h.get("indicators"), "1h.indicators")

    rsi = _require_float(symbol, stage, indicators.get("rsi"), "1h.indicators.rsi")
    macd_hist = _require_float(symbol, stage, indicators.get("macd_hist"), "1h.indicators.macd_hist")
    atr_pct = _require_float(symbol, stage, indicators.get("atr_pct"), "1h.indicators.atr_pct")

    raw60 = tf1h.get("raw_ohlcv_last60")
    ohlcv60 = _parse_ohlcv_rows(symbol, stage, raw60, min_len=60, name="1h.raw_ohlcv_last60")

    if last_close <= 0:
        _fail(symbol, stage, f"invalid last_close (<=0): {last_close}")

    rsi_dev = abs(rsi - 50.0)
    comp_rsi = _clamp(rsi_dev * 1.6, 0.0, 40.0)

    macd_hist_pct = (abs(macd_hist) / last_close) * 100.0
    comp_macd = _clamp(macd_hist_pct * 300.0, 0.0, 30.0)

    closes = [x[4] for x in ohlcv60]
    if len(closes) < 10:
        _fail(symbol, stage, f"need >=10 closes for momentum calc (got={len(closes)})")

    mom_pct_10 = abs((closes[-1] - closes[-10]) / closes[-10]) * 100.0
    comp_price_mom = _clamp(mom_pct_10 * 10.0, 0.0, 20.0)

    comp_atr_env = _clamp(atr_pct * 50.0, 0.0, 10.0)

    atr_chg_pct = _atr_pct_change_from_ohlcv(symbol, stage, ohlcv60, period=14)
    comp_atr_chg = _clamp(abs(atr_chg_pct) * 0.4, 0.0, 10.0)

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


def score_structure_15m(symbol: str, tf15m: Dict[str, Any]) -> Dict[str, Any]:
    stage = "engine_scores.structure_15m"

    last_close = _require_float(symbol, stage, tf15m.get("last_close"), "15m.last_close")
    raw20 = tf15m.get("raw_ohlcv_last20")
    ohlcv20 = _parse_ohlcv_rows(symbol, stage, raw20, min_len=20, name="15m.raw_ohlcv_last20")

    closes = [x[4] for x in ohlcv20]
    highs = [x[2] for x in ohlcv20]
    lows = [x[3] for x in ohlcv20]

    prev_high = max(highs[:-1])
    prev_low = min(lows[:-1])

    breakout_up = last_close > prev_high
    breakout_down = last_close < prev_low

    slope_pct_20 = ((closes[-1] - closes[0]) / closes[0]) * 100.0
    slope_abs = abs(slope_pct_20)

    range_pct = _compute_range_pct(symbol, stage, ohlcv20)

    state = "RANGE"
    if breakout_up:
        state = "BREAKOUT_UP"
    elif breakout_down:
        state = "BREAKOUT_DOWN"
    else:
        if range_pct >= 1.2 and slope_abs >= 0.6:
            state = "TRENDING"
        elif range_pct >= 1.2:
            state = "WIDE_RANGE"
        else:
            state = "TIGHT_RANGE"

    comp_breakout = 40.0 if (breakout_up or breakout_down) else 0.0
    comp_slope = _clamp(slope_abs * 12.0, 0.0, 30.0)
    comp_range = _clamp(range_pct * 10.0, 0.0, 30.0)

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


def score_timing_5m(
    symbol: str,
    tf5m: Dict[str, Any],
    pattern_features_5m: Dict[str, Any],
    pattern_summary: Dict[str, Any],
) -> Dict[str, Any]:
    stage = "engine_scores.timing_5m"

    indicators = _require_dict(symbol, stage, tf5m.get("indicators"), "5m.indicators")
    atr_pct = _require_float(symbol, stage, indicators.get("atr_pct"), "5m.indicators.atr_pct")

    raw20 = tf5m.get("raw_ohlcv_last20")
    ohlcv20 = _parse_ohlcv_rows(symbol, stage, raw20, min_len=20, name="5m.raw_ohlcv_last20")

    closes = [x[4] for x in ohlcv20]
    highs = [x[2] for x in ohlcv20]
    lows = [x[3] for x in ohlcv20]

    if len(closes) < 6:
        _fail(symbol, stage, f"need >=6 closes for 5m timing (got={len(closes)})")

    mom_pct_5 = abs((closes[-1] - closes[-6]) / closes[-6]) * 100.0

    def _avg_range_pct(seg_high: List[float], seg_low: List[float], seg_close: List[float]) -> float:
        if len(seg_high) != len(seg_low) or len(seg_low) != len(seg_close) or not seg_close:
            _fail(symbol, stage, "invalid segment lengths for range_pct")
        vals: List[float] = []
        for h, l, c in zip(seg_high, seg_low, seg_close):
            if c <= 0:
                _fail(symbol, stage, "close<=0 in range segment")
            vals.append(((h - l) / c) * 100.0)
        return sum(vals) / float(len(vals))

    recent_range = _avg_range_pct(highs[-5:], lows[-5:], closes[-5:])
    prev_range = _avg_range_pct(highs[-10:-5], lows[-10:-5], closes[-10:-5])
    if prev_range <= 0:
        _fail(symbol, stage, f"invalid prev_range (<=0): {prev_range}")

    range_ratio = recent_range / prev_range
    if not math.isfinite(range_ratio) or range_ratio <= 0:
        _fail(symbol, stage, f"invalid range_ratio: {range_ratio}")

    p5_score = _require_prob_0_1(symbol, stage, pattern_features_5m.get("pattern_score"), "pattern_features_5m.pattern_score")
    global_pattern_score = _require_prob_0_1(symbol, stage, pattern_summary.get("pattern_score"), "pattern_summary.pattern_score")

    comp_pattern = _clamp(max(p5_score, global_pattern_score) * 40.0, 0.0, 40.0)
    comp_momentum = _clamp(mom_pct_5 * 12.0, 0.0, 25.0)
    comp_atr_env = _clamp(atr_pct * 80.0, 0.0, 15.0)

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


def score_orderbook_micro(symbol: str, orderbook: Dict[str, Any]) -> Dict[str, Any]:
    m = _orderbook_metrics_strict(symbol, orderbook)

    spread_pct = m["spread_pct"]
    imbalance_qty_pct = m["imbalance_qty_pct"]
    depth_imbalance = m["depth_imbalance"]

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

    imbalance_score = _clamp(abs(imbalance_qty_pct) * 0.6, 0.0, 30.0)
    depth_score = _clamp(abs(depth_imbalance) * 30.0, 0.0, 30.0)

    quality_score = 10.0
    score = _clamp(spread_score + imbalance_score + depth_score + quality_score, 0.0, 100.0)

    return {
        "score": score,
        "components": {
            "best_bid": m["best_bid"],
            "best_ask": m["best_ask"],
            "spread_pct": spread_pct,
            "imbalance_qty_pct": imbalance_qty_pct,
            "depth_imbalance": depth_imbalance,
            "score_spread": spread_score,
            "score_imbalance": imbalance_score,
            "score_depth": depth_score,
            "score_quality": quality_score,
        },
    }


def build_engine_scores(
    symbol: str,
    timeframes: Dict[str, Any],
    pattern_features: Dict[str, Dict[str, Any]],
    pattern_summary: Dict[str, Any],
    orderbook: Dict[str, Any],
) -> Dict[str, Any]:
    sym = str(symbol or "").strip()
    if not sym:
        _fail("UNKNOWN", "build_engine_scores", "symbol is empty")

    # TRADE-GRADE: total weight 무결성 체크
    w = TOTAL_WEIGHTS
    wsum = float(sum(w.values()))
    if not math.isfinite(wsum) or abs(wsum - 1.0) > 1e-9:
        _fail(sym, "build_engine_scores", f"TOTAL_WEIGHTS sum must be 1.0 (got={wsum})")

    tfs = _require_dict(sym, "build_engine_scores", timeframes, "timeframes")
    pf = _require_dict(sym, "build_engine_scores", pattern_features, "pattern_features")
    ps = _require_dict(sym, "build_engine_scores", pattern_summary, "pattern_summary")
    ob = _require_dict(sym, "build_engine_scores", orderbook, "orderbook")

    tf4h = _require_dict(sym, "build_engine_scores", tfs.get("4h"), "timeframes['4h']")
    tf1h = _require_dict(sym, "build_engine_scores", tfs.get("1h"), "timeframes['1h']")
    tf15 = _require_dict(sym, "build_engine_scores", tfs.get("15m"), "timeframes['15m']")
    tf5 = _require_dict(sym, "build_engine_scores", tfs.get("5m"), "timeframes['5m']")

    pf5 = _require_dict(sym, "build_engine_scores", pf.get("5m"), "pattern_features['5m']")

    _require_prob_0_1(sym, "build_engine_scores", ps.get("pattern_score"), "pattern_summary.pattern_score")

    trend_4h = score_trend_4h(sym, tf4h)
    momentum_1h = score_momentum_1h(sym, tf1h)
    structure_15m = score_structure_15m(sym, tf15)
    timing_5m = score_timing_5m(sym, tf5, pf5, ps)
    orderbook_micro = score_orderbook_micro(sym, ob)

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


__all__ = [
    "ScoreEngineError",
    "build_engine_scores",
    "score_trend_4h",
    "score_momentum_1h",
    "score_structure_15m",
    "score_timing_5m",
    "score_orderbook_micro",
]