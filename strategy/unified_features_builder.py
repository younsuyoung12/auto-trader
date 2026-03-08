"""
========================================================
FILE: strategy/unified_features_builder.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================
역할
- WS 기반 market_features_ws 결과 + pattern_detection 결과 + 오더북(depth5)을 결합해
  "통합 피처(unified_features)"를 생성한다.
- 이 출력은 GPT(감사/해설) 입력 및 Quant Engine(결정권자) 로그/스냅샷 용도로 사용한다.
  (본 파일은 매매 결정을 하지 않는다.)

절대 원칙 (STRICT · NO-FALLBACK)
- REST 백필/더미값/0 치환/None 묵살/조용한 continue 금지.
- 필수 데이터가 없거나 손상/모호하면 즉시 예외로 실패한다.
- 필수 타임프레임: 1m / 5m / 15m / 1h / 4h (없으면 즉시 실패)
- 각 TF는 필수 raw OHLCV + indicators dict 를 반드시 포함해야 한다.
- 오더북은 WS depth5 스냅샷(bids/asks)을 포함하며, 비어 있으면 즉시 실패한다.
- pattern_features/summary 는 화이트리스트 기반 compact 형태로 축소(토큰 최적화).
- microstructure(Funding/OI/LSR/DI)는 거래소 REST 기반으로 수집하며 누락/비정상은 즉시 실패한다.
- volume_profile / orderflow_cvd / options_market 은 추가 외부/구조 피처로 결합되며
  누락/비정상은 즉시 실패한다.
- 성공 결과만 짧은 TTL 캐시를 허용한다. 캐시 만료 후 fetch 실패 시 stale 캐시 사용 금지.
- 예외(TRADE-GRADE 규칙): 4h.regime은 WS warmup/지연에서 누락될 수 있으므로
  누락 시 neutral(trend_strength=0.5)로 처리한다. (데이터 조작/폴백이 아닌 명시 규칙)

입력 계약(중요)
- market_features_ws.timeframes[1m|5m|15m].raw_ohlcv_last20:
  (ts_ms, open, high, low, close, volume) 형태의 sequence(list/tuple) 이어야 한다. dict 금지.
- market_features_ws.timeframes[1h|4h].raw_ohlcv_last60:
  (ts_ms, open, high, low, close, volume) 형태의 sequence(list/tuple) 이어야 한다. dict 금지.
- market_features_ws.timeframes[*].indicators:
  엔진 점수 계산에 필요한 scalar(rsi/ema_fast/ema_slow/atr_pct/macd_hist 등)를 포함해야 한다.
- volume_profile:
  - 1h raw_ohlcv_last60 기반으로 계산한다.
- orderflow_cvd:
  - Binance aggTrades 공개 REST를 사용해 최근 체결 흐름을 계산한다.
- options_market:
  - Deribit 공개 옵션시장 데이터를 사용한다.

변경 이력
--------------------------------------------------------
- 2026-03-08:
  1) FIX(TRADE-GRADE): 중복 top-level 함수 정의 제거
     - _score_volume_profile_regime
     - _score_orderflow_regime
     - _score_options_regime
  2) test_market_structure_debug_runner 의 AST duplicate scan 실패 원인 제거
  3) 기존 기능/입력계약/점수식/출력 스키마는 삭제 없이 유지
- 2026-03-06 TRADE-GRADE UPGRADE:
  1) Engine scoring system upgraded to institutional multi-factor model.
  2) Added regime engines:
     - volatility_regime
     - volume_regime
     - liquidity_regime
  3) Total engine score now combines:
     - trend
     - momentum
     - structure
     - timing
     - orderbook
     - volatility
     - volume
     - liquidity
  4) This improves detection of:
     - trend markets
     - range markets
     - volatility expansions
     - liquidity sweeps
     - volume spikes
  5) Used by:
     - run_bot_ws
     - risk_physics_engine
     - execution_engine
- 2026-03-05:
  1) FIX(TRADE-GRADE): 4h.regime missing/invalid로 unified_features가 크래시하던 문제 수정
     - 4h.regime 누락 시 neutral trend_strength=0.5 적용
     - regime가 dict인데 trend_strength가 누락/비정상인 경우는 기존대로 즉시 예외(STRICT 유지)
- 2026-03-04:
  1) Data Integrity Guard 연동:
     - OHLCV 시계열에 timestamp rollback/미래 timestamp/ohlcv 관계식 STRICT 검증 추가
     - 오더북 원시 스냅샷에 bestAsk>bestBid/레벨 price·qty>0/ts(exchTs|ts) 존재 STRICT 검증 추가
  2) Invariant Guard 연동(최소):
     - micro_score_risk 및 settings(tp/sl) 기반 최소 수학 invariant 검증(폴백 없음)
  3) 모듈 docstring 위치 정합화(__future__ import보다 앞으로 이동)
- 2026-03-03:
  1) STRICT 강화: silent 예외 삼키기 제거(텔레그램 전송 포함)
  2) STRICT 강화: 암묵적 기본값(or "", or "NONE") 제거 및 필수 키/형식 강제
  3) STRICT 강화: symbol 결정 로직에서 빈 문자열/누락을 즉시 실패로 처리
  4) TRADE-GRADE: microstructure(Funding/OI/LSR/Distortion Index) 스냅샷 생성/결합 추가(STRICT)
- 2026-03-02:
  1) FIX(STRICT): atr_pct / range_pct 스케일 정규화 규칙 추가
     - 일부 upstream이 atr_pct, range_pct를 "비율(0~1)"로 제공함을 확인.
     - 본 파일의 점수식은 "%(0~100)" 전제를 사용하므로,
       아래 규칙으로 STRICT 정규화한다(폴백/추정 아님, 명시 규칙):
       * 0.0 <= v <= 1.0  → v * 100.0 (ratio → percent)
       * 1.0 < v <= 100.0 → 그대로 사용 (already percent)
       * 그 외 범위       → 즉시 실패
     - 적용 대상:
       * _score_momentum_1h: indicators.atr_pct
       * _score_timing_5m:   indicators.atr_pct
       * _score_structure_15m: tf15m.range_pct
  2) TUNING(TRADE-GRADE): engine_scores.total 가중치 재조정
     - trend_4h 비중 ↓(0.15), orderbook_micro 비중 ↑(0.25)
- 2026-03-08:
  1) VolumeProfileEngine 연동 추가
  2) OrderFlowCvdEngine 연동 추가
  3) OptionsMarketFetcher 연동 추가
  4) unified_features 에 volume_profile / orderflow_cvd / options_market compact payload 추가
  5) engine_scores.total 에 volume_profile_regime / orderflow_regime / options_regime 점수 반영
  6) 성공 결과만 TTL 캐시 허용(orderflow/options)
  7) entry_score 유실 버그 제거
  8) TOTAL_WEIGHTS 합=1.0 무결성 검증 추가
  9) 기존 WS / pattern / orderbook / microstructure 기능 삭제 없이 유지
========================================================
"""

from __future__ import annotations

import math
import time
from decimal import Decimal
from typing import Any, Dict, List, NoReturn, Optional, Sequence, Tuple

from settings import load_settings

try:
    from infra.telelog import log, send_tg
except Exception as e:  # pragma: no cover
    raise RuntimeError("infra.telelog import failed (STRICT · NO-FALLBACK · TRADE-GRADE MODE)") from e

from infra.market_data_ws import get_orderbook
from infra.market_features_ws import FeatureBuildError, build_entry_features_ws
from strategy.pattern_detection import PatternError, build_pattern_features
from strategy.microstructure_engine import MicrostructureError, build_microstructure_snapshot

from analysis.binance_market_fetcher import BinanceKline, BinanceMarketFetcher
from analysis.options_market_fetcher import OptionsMarketFetcher, OptionsMarketSnapshot
from analysis.orderflow_cvd import OrderFlowCvdEngine, OrderFlowCvdReport
from analysis.volume_profile import VolumeProfileEngine, VolumeProfileReport

# NEW: Data Integrity Guard (TRADE-GRADE)
from infra.data_integrity_guard import (  # noqa: E402
    DataIntegrityError,
    extract_orderbook_ts_ms_strict,
    validate_kline_series_strict,
    validate_orderbook_strict,
)

# NEW: Invariant Guard (TRADE-GRADE)
from execution.invariant_guard import (  # noqa: E402
    InvariantViolation,
    SignalInvariantInputs,
    validate_signal_invariants_strict,
)

SET = load_settings()

REQUIRED_TFS: Tuple[str, ...] = ("1m", "5m", "15m", "1h", "4h")
PATTERN_TFS: Tuple[str, ...] = ("1m", "5m", "15m")

MAX_PATTERNS_PER_TF: int = 4
MAX_SUMMARY_PATTERNS: int = 10

# Microstructure config defaults (deterministic project defaults; not data fallback)
_MICRO_PERIOD_DEFAULT: str = "5m"
_MICRO_LOOKBACK_DEFAULT: int = 30
_MICRO_CACHE_TTL_SEC_DEFAULT: int = 15

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

# Volume Profile / Order Flow / Options config
_VOL_PROFILE_SOURCE_TF: str = "1h"
_VOL_PROFILE_VALUE_AREA_PCT: float = 0.70
_VOL_PROFILE_HVN_COUNT: int = 3
_VOL_PROFILE_LVN_COUNT: int = 3

_ORDERFLOW_AGGTRADE_LIMIT: int = 240
_ORDERFLOW_MIN_TRADES: int = 50
_ORDERFLOW_DOMINANCE_THRESHOLD_PCT: float = 10.0
_ORDERFLOW_DIVERGENCE_PRICE_THRESHOLD_PCT: float = 0.20
_ORDERFLOW_DIVERGENCE_DELTA_THRESHOLD_PCT: float = 8.0
_ORDERFLOW_PATH_TAIL_SIZE: int = 20
_ORDERFLOW_CACHE_TTL_SEC: int = 5
_OPTIONS_CACHE_TTL_SEC: int = 60

# engine_scores.total 가중치 (합=1.0)
_TOTAL_WEIGHTS: Dict[str, float] = {
    "trend_4h": 0.12,
    "momentum_1h": 0.10,
    "structure_15m": 0.10,
    "timing_5m": 0.10,
    "orderbook_micro": 0.10,
    "volatility_regime": 0.08,
    "volume_regime": 0.08,
    "liquidity_regime": 0.05,
    "volume_profile_regime": 0.10,
    "orderflow_regime": 0.12,
    "options_regime": 0.05,
}

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

_SUCCESS_CACHE: Dict[Tuple[str, str], Tuple[float, Any]] = {}


class UnifiedFeaturesError(RuntimeError):
    """마켓 피처 + 패턴 피처 + 엔진 점수 + 마이크로구조 통합 단계에서 사용하는 예외."""


def _notify_tg_best_effort(symbol: str, msg: str) -> None:
    """
    텔레그램은 '부가 채널'이다.
    거래 판단/피처 생성은 텔레그램 장애로 중단시키지 않는다.
    단, 조용히 삼키지 않고 로그는 남긴다.
    """
    try:
        send_tg(msg)
    except Exception as e:
        log(f"[UNIFIED-FEAT][{symbol}] telegram notify failed (non-fatal): {e!r}")


def _fail_unified(symbol: str, stage: str, reason: str, exc: Optional[BaseException] = None) -> NoReturn:
    msg = f"[UNIFIED-FEAT][{symbol}] {stage} 실패: {reason}"
    log(msg)
    _notify_tg_best_effort(symbol, msg)
    if exc is None:
        raise UnifiedFeaturesError(msg)
    raise UnifiedFeaturesError(msg) from exc


def _clamp(v: float, lo: float, hi: float) -> float:
    if v < lo:
        return lo
    if v > hi:
        return hi
    return v


def _cache_key(symbol: str, feature_name: str) -> Tuple[str, str]:
    return (str(symbol).upper().strip(), feature_name)


def _get_success_cache(symbol: str, feature_name: str, ttl_sec: int) -> Optional[Any]:
    if ttl_sec <= 0:
        _fail_unified(symbol, feature_name, f"ttl_sec must be > 0 (got={ttl_sec})")
    key = _cache_key(symbol, feature_name)
    item = _SUCCESS_CACHE.get(key)
    if item is None:
        return None
    saved_at, payload = item
    age = time.time() - float(saved_at)
    if age <= float(ttl_sec):
        return payload
    return None


def _set_success_cache(symbol: str, feature_name: str, payload: Any) -> None:
    key = _cache_key(symbol, feature_name)
    _SUCCESS_CACHE[key] = (time.time(), payload)


def _require_nonempty_str(symbol: str, stage: str, v: Any, name: str) -> str:
    if v is None:
        _fail_unified(symbol, stage, f"{name} is required (None)")
    s = str(v).strip()
    if s == "":
        _fail_unified(symbol, stage, f"{name} is required (empty)")
    return s


def _require_dict(symbol: str, stage: str, v: Any, name: str) -> Dict[str, Any]:
    if not isinstance(v, dict):
        _fail_unified(symbol, stage, f"{name} must be dict (got={type(v)})")
    if not v:
        _fail_unified(symbol, stage, f"{name} is empty")
    return v


def _require_sequence(symbol: str, stage: str, v: Any, name: str, min_len: int) -> Sequence[Any]:
    if not isinstance(v, (list, tuple)):
        _fail_unified(symbol, stage, f"{name} must be sequence(list/tuple) (got={type(v)})")
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


def _require_pct_0_100(symbol: str, stage: str, v: Any, name: str) -> float:
    fv = _require_float(symbol, stage, v, name)
    if 0.0 <= fv <= 1.0:
        fv = fv * 100.0
    if fv < 0.0 or fv > 100.0:
        _fail_unified(symbol, stage, f"{name} out of range after normalization (got={fv})")
    return fv


def _parse_ohlcv_rows(
    symbol: str,
    stage: str,
    rows: Any,
    *,
    min_len: int,
    name: str,
) -> List[Tuple[int, float, float, float, float, float]]:
    raw = _require_sequence(symbol, stage, rows, name, min_len=min_len)
    sliced = list(raw[-min_len:])

    try:
        validate_kline_series_strict(sliced, name=name, min_len=min_len)
    except DataIntegrityError as e:
        _fail_unified(symbol, stage, f"{name} integrity fail (STRICT): {e}", e)

    out: List[Tuple[int, float, float, float, float, float]] = []
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
            continue

        raw60 = tf_data.get("raw_ohlcv_last60")
        if raw60 is None:
            candles = tf_data.get("candles")
            if candles is None:
                _fail_unified(symbol, stage, "raw_ohlcv_last60 missing and candles missing (STRICT)")
            parsed = _parse_ohlcv_rows(symbol, stage, candles, min_len=60, name=f"{tf}.candles")
            tf_data["raw_ohlcv_last60"] = [[ts, o, h, l, c, v] for (ts, o, h, l, c, v) in parsed]
        else:
            _parse_ohlcv_rows(symbol, stage, raw60, min_len=60, name=f"{tf}.raw_ohlcv_last60")


def _get_orderbook_strict(symbol: str, limit: int = 5) -> Dict[str, Any]:
    stage = "orderbook"
    ob = get_orderbook(symbol, limit=limit)
    if not isinstance(ob, dict) or not ob:
        _fail_unified(symbol, stage, "WS orderbook snapshot missing/invalid")

    try:
        validate_orderbook_strict(ob, symbol=str(symbol), require_ts=True)
    except DataIntegrityError as e:
        _fail_unified(symbol, stage, f"orderbook integrity fail (STRICT): {e}", e)

    bids = ob.get("bids")
    asks = ob.get("asks")
    if not isinstance(bids, list) or not bids:
        _fail_unified(symbol, stage, "orderbook bids missing/empty")
    if not isinstance(asks, list) or not asks:
        _fail_unified(symbol, stage, "orderbook asks missing/empty")

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

    depth_imbalance = (bid_notional - ask_notional) / notional_total
    if not math.isfinite(depth_imbalance):
        _fail_unified(symbol, stage, f"invalid depth_imbalance: {depth_imbalance}")

    try:
        checked_at_ms = extract_orderbook_ts_ms_strict(ob, name="orderbook")
    except DataIntegrityError as e:
        _fail_unified(symbol, stage, f"orderbook ts missing/invalid (STRICT): {e}", e)

    return {
        "checked_at_ms": int(checked_at_ms),
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


def _compact_pattern_item(symbol: str, stage: str, p: Dict[str, Any], timeframe: str) -> Dict[str, Any]:
    item: Dict[str, Any] = {}
    for key in _PATTERN_ITEM_KEYS:
        if key in p:
            item[key] = p[key]

    item["timeframe"] = timeframe
    item["pattern"] = _require_nonempty_str(symbol, stage, item.get("pattern"), "pattern.pattern")
    item["direction"] = _require_nonempty_str(symbol, stage, item.get("direction"), "pattern.direction")
    item["confidence"] = _require_nonempty_str(symbol, stage, item.get("confidence"), "pattern.confidence")

    if "strength" not in item:
        _fail_unified(symbol, stage, f"pattern item missing strength (timeframe={timeframe})")
    item["strength"] = _require_float(symbol, stage, item["strength"], "pattern.strength")

    return item


def _build_pattern_bundle_strict(
    symbol: str,
    timeframes: Dict[str, Any],
    orderbook_features: Dict[str, Any],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    pattern_features: Dict[str, Dict[str, Any]] = {}

    all_patterns: List[Dict[str, Any]] = []
    best_strength = -1.0
    best_pattern_name = "NONE"
    best_pattern_direction = "NONE"
    best_pattern_confidence = "NONE"
    best_pattern_timeframe = "NONE"

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

        pattern_score = _require_prob_0_1(symbol, stage, pf_raw.get("pattern_score"), "pattern_score")
        reversal_prob = _require_prob_0_1(symbol, stage, pf_raw.get("reversal_probability"), "reversal_probability")
        continuation_prob = _require_prob_0_1(symbol, stage, pf_raw.get("continuation_probability"), "continuation_probability")
        momentum_score = _require_prob_0_1(symbol, stage, pf_raw.get("momentum_score"), "momentum_score")
        volume_conf = _require_prob_0_1(symbol, stage, pf_raw.get("volume_confirmation"), "volume_confirmation")
        wick_strength = _require_prob_0_1(symbol, stage, pf_raw.get("wick_strength"), "wick_strength")
        liquidity_event = _require_prob_0_1(symbol, stage, pf_raw.get("liquidity_event_score"), "liquidity_event_score")

        has_bull = _require_int(symbol, stage, pf_raw.get("has_bullish_pattern"), "has_bullish_pattern")
        has_bear = _require_int(symbol, stage, pf_raw.get("has_bearish_pattern"), "has_bearish_pattern")
        if has_bull not in (0, 1) or has_bear not in (0, 1):
            _fail_unified(symbol, stage, f"has_bullish_pattern/has_bearish_pattern must be 0/1 (got={has_bull}/{has_bear})")

        raw_patterns = pf_raw.get("patterns")
        if raw_patterns is None:
            _fail_unified(symbol, stage, "patterns key missing (None)")
        if not isinstance(raw_patterns, list):
            _fail_unified(symbol, stage, f"patterns must be list (got={type(raw_patterns)})")

        compact_patterns: List[Dict[str, Any]] = []
        for i, p in enumerate(raw_patterns):
            if not isinstance(p, dict):
                _fail_unified(symbol, stage, f"patterns[{i}] must be dict (got={type(p)})")
            item = _compact_pattern_item(symbol, stage, p, tf)
            compact_patterns.append(item)

            strength = float(item["strength"])
            if strength > best_strength:
                best_strength = strength
                best_pattern_name = item["pattern"]
                best_pattern_direction = item["direction"]
                best_pattern_confidence = item["confidence"]
                best_pattern_timeframe = tf

        compact_patterns.sort(key=lambda x: float(x["strength"]), reverse=True)
        if MAX_PATTERNS_PER_TF > 0:
            compact_patterns = compact_patterns[:MAX_PATTERNS_PER_TF]

        all_patterns.extend(compact_patterns)

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

        global_pattern_score = max(global_pattern_score, pattern_score)
        global_reversal = max(global_reversal, reversal_prob)
        global_continuation = max(global_continuation, continuation_prob)
        global_momentum = max(global_momentum, momentum_score)
        global_volume_conf = max(global_volume_conf, volume_conf)
        global_wick_strength = max(global_wick_strength, wick_strength)
        global_liquidity_score = max(global_liquidity_score, liquidity_event)
        has_bullish_pattern = max(has_bullish_pattern, has_bull)
        has_bearish_pattern = max(has_bearish_pattern, has_bear)

    all_patterns.sort(key=lambda x: float(x["strength"]), reverse=True)
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
    if len(ohlcv) < (period * 2 + 1):
        _fail_unified(symbol, stage, f"need >= {period * 2 + 1} candles for ATR change (got={len(ohlcv)})")

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
    tr_prev = tr[-(period * 2): -period]

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


def _mean_strict(symbol: str, stage: str, values: Sequence[float], name: str) -> float:
    if not values:
        _fail_unified(symbol, stage, f"{name} is empty")
    total = 0.0
    for i, v in enumerate(values):
        fv = _require_float(symbol, stage, v, f"{name}[{i}]")
        total += fv
    out = total / float(len(values))
    if not math.isfinite(out):
        _fail_unified(symbol, stage, f"{name} mean is not finite")
    return out


def _score_volatility_regime(symbol: str, tf15m: Dict[str, Any], ohlcv20: List[Tuple[int, float, float, float, float, float]]) -> Dict[str, Any]:
    stage = "engine_scores.volatility_regime"

    if len(ohlcv20) < 20:
        _fail_unified(symbol, stage, f"need >=20 candles for volatility_regime (got={len(ohlcv20)})")

    indicators = _require_dict(symbol, stage, tf15m.get("indicators"), "15m.indicators")
    atr_pct = _require_pct_0_100(symbol, stage, indicators.get("atr_pct"), "15m.indicators.atr_pct")

    highs = [x[2] for x in ohlcv20]
    lows = [x[3] for x in ohlcv20]
    closes = [x[4] for x in ohlcv20]

    tr: List[float] = []
    for i in range(len(ohlcv20)):
        if i == 0:
            tr.append(highs[i] - lows[i])
        else:
            tr.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))

    recent_tr = _mean_strict(symbol, stage, tr[-5:], "volatility.recent_tr")
    prev_tr = _mean_strict(symbol, stage, tr[-10:-5], "volatility.prev_tr")
    if prev_tr <= 0.0:
        _fail_unified(symbol, stage, f"volatility.prev_tr must be > 0 (got={prev_tr})")

    atr_expansion = recent_tr / prev_tr

    recent_range_pct = _mean_strict(
        symbol,
        stage,
        [((h - l) / c) * 100.0 for h, l, c in zip(highs[-5:], lows[-5:], closes[-5:])],
        "volatility.recent_range_pct",
    )
    prev_range_pct = _mean_strict(
        symbol,
        stage,
        [((h - l) / c) * 100.0 for h, l, c in zip(highs[-10:-5], lows[-10:-5], closes[-10:-5])],
        "volatility.prev_range_pct",
    )
    if prev_range_pct <= 0.0:
        _fail_unified(symbol, stage, f"volatility.prev_range_pct must be > 0 (got={prev_range_pct})")

    range_expansion = recent_range_pct / prev_range_pct

    comp_atr_pct = _clamp((atr_pct / 1.50) * 40.0, 0.0, 40.0)
    comp_atr_expansion = _clamp((max(atr_expansion - 1.0, 0.0) / 0.80) * 30.0, 0.0, 30.0)
    comp_range_expansion = _clamp((max(range_expansion - 1.0, 0.0) / 0.80) * 30.0, 0.0, 30.0)

    score = _clamp(comp_atr_pct + comp_atr_expansion + comp_range_expansion, 0.0, 100.0)

    return {
        "score": score,
        "components": {
            "atr_pct": atr_pct,
            "atr_expansion": atr_expansion,
            "recent_range_pct": recent_range_pct,
            "prev_range_pct": prev_range_pct,
            "range_expansion": range_expansion,
            "score_atr_pct": comp_atr_pct,
            "score_atr_expansion": comp_atr_expansion,
            "score_range_expansion": comp_range_expansion,
        },
    }


def _score_volume_regime(symbol: str, ohlcv20: List[Tuple[int, float, float, float, float, float]]) -> Dict[str, Any]:
    stage = "engine_scores.volume_regime"

    if len(ohlcv20) < 20:
        _fail_unified(symbol, stage, f"need >=20 candles for volume_regime (got={len(ohlcv20)})")

    volumes = [x[5] for x in ohlcv20]
    recent_vol = _mean_strict(symbol, stage, volumes[-5:], "volume.recent_vol")
    base_vol = _mean_strict(symbol, stage, volumes[-20:-5], "volume.base_vol")
    if base_vol <= 0.0:
        _fail_unified(symbol, stage, f"volume.base_vol must be > 0 (got={base_vol})")

    volume_ratio = recent_vol / base_vol
    volume_spike_ratio = max(volumes[-5:]) / base_vol

    comp_ratio = _clamp((max(volume_ratio - 1.0, 0.0) / 2.0) * 60.0, 0.0, 60.0)
    comp_spike = _clamp((max(volume_spike_ratio - 1.0, 0.0) / 3.0) * 40.0, 0.0, 40.0)

    score = _clamp(comp_ratio + comp_spike, 0.0, 100.0)

    return {
        "score": score,
        "components": {
            "recent_vol": recent_vol,
            "base_vol": base_vol,
            "volume_ratio": volume_ratio,
            "volume_spike_ratio": volume_spike_ratio,
            "score_ratio": comp_ratio,
            "score_spike": comp_spike,
        },
    }


def _score_liquidity_regime(symbol: str, ohlcv20: List[Tuple[int, float, float, float, float, float]]) -> Dict[str, Any]:
    stage = "engine_scores.liquidity_regime"

    if len(ohlcv20) < 20:
        _fail_unified(symbol, stage, f"need >=20 candles for liquidity_regime (got={len(ohlcv20)})")

    body_ratios: List[float] = []
    wick_ratios: List[float] = []
    stop_hunt_count = 0

    for i, (_, o, h, l, c, _) in enumerate(ohlcv20[-10:]):
        candle_stage = f"{stage}.last10[{i}]"
        rng = h - l
        if rng <= 0.0:
            _fail_unified(symbol, candle_stage, f"range must be > 0 (got={rng})")

        body = abs(c - o)
        body_ratio = body / rng
        wick_ratio = max(0.0, 1.0 - body_ratio)

        if not math.isfinite(body_ratio) or not math.isfinite(wick_ratio):
            _fail_unified(symbol, candle_stage, "body_ratio/wick_ratio must be finite")

        body_ratios.append(body_ratio)
        wick_ratios.append(wick_ratio)

        if wick_ratio >= 0.65 and body_ratio <= 0.25:
            stop_hunt_count += 1

    avg_body_ratio = _mean_strict(symbol, stage, body_ratios, "liquidity.avg_body_ratio")
    avg_wick_ratio = _mean_strict(symbol, stage, wick_ratios, "liquidity.avg_wick_ratio")
    stop_hunt_ratio = stop_hunt_count / 10.0

    if stop_hunt_ratio < 0.0 or stop_hunt_ratio > 1.0:
        _fail_unified(symbol, stage, f"stop_hunt_ratio out of range [0,1] (got={stop_hunt_ratio})")

    comp_body = _clamp(avg_body_ratio * 100.0 * 0.50, 0.0, 50.0)
    comp_wick = _clamp((1.0 - avg_wick_ratio) * 100.0 * 0.30, 0.0, 30.0)
    comp_stop_hunt = _clamp((1.0 - stop_hunt_ratio) * 20.0, 0.0, 20.0)

    score = _clamp(comp_body + comp_wick + comp_stop_hunt, 0.0, 100.0)

    return {
        "score": score,
        "components": {
            "avg_body_ratio": avg_body_ratio,
            "avg_wick_ratio": avg_wick_ratio,
            "stop_hunt_ratio": stop_hunt_ratio,
            "score_body": comp_body,
            "score_wick": comp_wick,
            "score_stop_hunt": comp_stop_hunt,
        },
    }


def _score_trend_4h(symbol: str, tf4h: Dict[str, Any], ohlcv60: List[Tuple[int, float, float, float, float, float]]) -> Dict[str, Any]:
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
    if price > ema_slow and ema_fast > ema_slow and structure_bias >= 0:
        direction = "LONG"
    elif price < ema_slow and ema_fast < ema_slow and structure_bias <= 0:
        direction = "SHORT"

    regime = tf4h.get("regime")
    regime_missing = not isinstance(regime, dict)

    if regime_missing:
        trend_strength_val = 0.5
    else:
        trend_strength_val = _require_float(symbol, stage, regime.get("trend_strength"), "4h.regime.trend_strength")

    comp_price_dist = _clamp(abs(price_vs_ema_pct) * 8.0, 0.0, 30.0)
    comp_ema_spread = _clamp(abs(ema_spread_pct) * 10.0, 0.0, 20.0)
    comp_slope = _clamp(abs(slope_pct_60) * 5.0, 0.0, 20.0)
    comp_structure = 10.0 if structure_bias != 0 else 0.0
    comp_regime = _clamp(float(trend_strength_val) * 20.0, 0.0, 20.0)

    score = _clamp(comp_price_dist + comp_ema_spread + comp_slope + comp_structure + comp_regime, 0.0, 100.0)

    return {
        "score": score,
        "direction": direction,
        "components": {
            "price_vs_ema200_pct": price_vs_ema_pct,
            "ema50_vs_ema200_pct": ema_spread_pct,
            "slope_pct_60": slope_pct_60,
            "structure_state": structure_state,
            "trend_strength": float(trend_strength_val),
            "regime_missing": 1 if regime_missing else 0,
            "score_price_dist": comp_price_dist,
            "score_ema_spread": comp_ema_spread,
            "score_slope": comp_slope,
            "score_structure": comp_structure,
            "score_regime": comp_regime,
        },
    }


def _score_momentum_1h(symbol: str, tf1h: Dict[str, Any], ohlcv60: List[Tuple[int, float, float, float, float, float]]) -> Dict[str, Any]:
    stage = "engine_scores.momentum_1h"
    indicators = _require_dict(symbol, stage, tf1h.get("indicators"), "1h.indicators")

    price = _require_float(symbol, stage, tf1h.get("last_close"), "1h.last_close")
    rsi = _require_float(symbol, stage, indicators.get("rsi"), "1h.indicators.rsi")
    macd_hist = _require_float(symbol, stage, indicators.get("macd_hist"), "1h.indicators.macd_hist")
    atr_pct = _require_pct_0_100(symbol, stage, indicators.get("atr_pct"), "1h.indicators.atr_pct")

    if price <= 0:
        _fail_unified(symbol, stage, f"invalid price (<=0): {price}")

    rsi_dev = abs(rsi - 50.0)
    comp_rsi = _clamp(rsi_dev * 1.6, 0.0, 40.0)

    macd_hist_pct = (abs(macd_hist) / price) * 100.0
    comp_macd = _clamp(macd_hist_pct * 300.0, 0.0, 30.0)

    closes = [x[4] for x in ohlcv60]
    if len(closes) < 10:
        _fail_unified(symbol, stage, f"need >=10 closes for momentum (got={len(closes)})")

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


def _score_structure_15m(symbol: str, tf15m: Dict[str, Any], ohlcv20: List[Tuple[int, float, float, float, float, float]]) -> Dict[str, Any]:
    stage = "engine_scores.structure_15m"

    price = _require_float(symbol, stage, tf15m.get("last_close"), "15m.last_close")
    range_pct = _require_pct_0_100(symbol, stage, tf15m.get("range_pct"), "15m.range_pct")

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


def _score_timing_5m(
    symbol: str,
    tf5m: Dict[str, Any],
    ohlcv20: List[Tuple[int, float, float, float, float, float]],
    pattern_summary: Dict[str, Any],
    pattern_features_5m: Dict[str, Any],
) -> Dict[str, Any]:
    stage = "engine_scores.timing_5m"

    indicators = _require_dict(symbol, stage, tf5m.get("indicators"), "5m.indicators")
    atr_pct = _require_pct_0_100(symbol, stage, indicators.get("atr_pct"), "5m.indicators.atr_pct")

    closes = [x[4] for x in ohlcv20]
    highs = [x[2] for x in ohlcv20]
    lows = [x[3] for x in ohlcv20]

    if len(closes) < 6:
        _fail_unified(symbol, stage, f"need >=6 closes for 5m timing (got={len(closes)})")

    mom_pct_5 = abs((closes[-1] - closes[-6]) / closes[-6]) * 100.0

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

    p5_score = _require_prob_0_1(symbol, stage, pattern_features_5m.get("pattern_score"), "5m.pattern_score")
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


def _score_orderbook_micro(symbol: str, orderbook: Dict[str, Any]) -> Dict[str, Any]:
    stage = "engine_scores.orderbook_micro"

    spread_pct = _require_float(symbol, stage, orderbook.get("spread_pct"), "orderbook.spread_pct")
    imbalance_qty_pct = _require_float(symbol, stage, orderbook.get("imbalance_qty_pct"), "orderbook.imbalance_qty_pct")
    depth_imbalance = _require_float(symbol, stage, orderbook.get("depth_imbalance"), "orderbook.depth_imbalance")

    if spread_pct < 0:
        _fail_unified(symbol, stage, f"invalid spread_pct (<0): {spread_pct}")

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
            "spread_pct": spread_pct,
            "imbalance_qty_pct": imbalance_qty_pct,
            "depth_imbalance": depth_imbalance,
            "score_spread": spread_score,
            "score_imbalance": imbalance_score,
            "score_depth": depth_score,
            "score_quality": quality_score,
        },
    }


def _score_volume_profile_regime(symbol: str, report: VolumeProfileReport) -> Dict[str, Any]:
    stage = "engine_scores.volume_profile_regime"

    poc_distance_bps = _require_float(symbol, stage, report.poc_distance_bps, "volume_profile.poc_distance_bps")
    value_area_coverage_pct = _require_float(symbol, stage, report.value_area_coverage_pct, "volume_profile.value_area_coverage_pct")
    price_location = _require_nonempty_str(symbol, stage, report.price_location, "volume_profile.price_location")

    comp_location = 0.0
    if price_location in ("above_value_area", "below_value_area"):
        comp_location = 40.0
    elif price_location in ("inside_value_area_above_poc", "inside_value_area_below_poc"):
        comp_location = 25.0
    elif price_location == "at_poc":
        comp_location = 10.0
    else:
        _fail_unified(symbol, stage, f"unexpected price_location: {price_location}")

    distance_abs = abs(poc_distance_bps)
    comp_distance = _clamp((distance_abs / 50.0) * 40.0, 0.0, 40.0)

    coverage_dev = abs(value_area_coverage_pct - (_VOL_PROFILE_VALUE_AREA_PCT * 100.0))
    comp_coverage = _clamp((max(10.0 - coverage_dev, 0.0) / 10.0) * 20.0, 0.0, 20.0)

    score = _clamp(comp_location + comp_distance + comp_coverage, 0.0, 100.0)
    return {
        "score": score,
        "components": {
            "poc_price": float(report.poc_price),
            "value_area_low": float(report.value_area_low),
            "value_area_high": float(report.value_area_high),
            "poc_distance_bps": poc_distance_bps,
            "price_location": price_location,
            "value_area_coverage_pct": value_area_coverage_pct,
            "score_location": comp_location,
            "score_distance": comp_distance,
            "score_coverage": comp_coverage,
        },
    }


def _score_orderflow_regime(symbol: str, report: OrderFlowCvdReport) -> Dict[str, Any]:
    stage = "engine_scores.orderflow_regime"

    delta_ratio_pct = _require_float(symbol, stage, report.delta_ratio_pct, "orderflow.delta_ratio_pct")
    price_change_pct = _require_float(symbol, stage, report.price_change_pct, "orderflow.price_change_pct")
    aggression_bias = _require_nonempty_str(symbol, stage, report.aggression_bias, "orderflow.aggression_bias")
    divergence = _require_nonempty_str(symbol, stage, report.divergence, "orderflow.divergence")

    comp_delta = _clamp((abs(delta_ratio_pct) / 20.0) * 50.0, 0.0, 50.0)
    comp_price = _clamp((abs(price_change_pct) / 1.0) * 20.0, 0.0, 20.0)

    if aggression_bias in ("aggressive_buy_dominant", "aggressive_sell_dominant"):
        comp_aggression = 20.0
    elif aggression_bias == "balanced":
        comp_aggression = 8.0
    else:
        _fail_unified(symbol, stage, f"unexpected aggression_bias: {aggression_bias}")

    if divergence == "none":
        comp_divergence = 10.0
    elif divergence in ("bullish_divergence", "bearish_divergence"):
        comp_divergence = 0.0
    else:
        _fail_unified(symbol, stage, f"unexpected divergence: {divergence}")

    score = _clamp(comp_delta + comp_price + comp_aggression + comp_divergence, 0.0, 100.0)
    return {
        "score": score,
        "components": {
            "delta_ratio_pct": delta_ratio_pct,
            "cvd": float(report.cvd),
            "price_change_pct": price_change_pct,
            "aggression_bias": aggression_bias,
            "divergence": divergence,
            "score_delta": comp_delta,
            "score_price": comp_price,
            "score_aggression": comp_aggression,
            "score_divergence": comp_divergence,
        },
    }


def _score_options_regime(symbol: str, report: OptionsMarketSnapshot) -> Dict[str, Any]:
    stage = "engine_scores.options_regime"

    pcr_oi = _require_float(symbol, stage, report.put_call_oi_ratio, "options.put_call_oi_ratio")
    pcr_vol = _require_float(symbol, stage, report.put_call_volume_ratio, "options.put_call_volume_ratio")
    options_bias = _require_nonempty_str(symbol, stage, report.options_bias, "options.options_bias")

    comp_oi = _clamp((abs(pcr_oi - 1.0) / 0.5) * 40.0, 0.0, 40.0)
    comp_vol = _clamp((abs(pcr_vol - 1.0) / 0.5) * 40.0, 0.0, 40.0)

    if options_bias in ("bullish", "bearish"):
        comp_bias = 20.0
    elif options_bias in ("neutral", "mixed"):
        comp_bias = 8.0
    else:
        _fail_unified(symbol, stage, f"unexpected options_bias: {options_bias}")

    score = _clamp(comp_oi + comp_vol + comp_bias, 0.0, 100.0)
    return {
        "score": score,
        "components": {
            "put_call_oi_ratio": pcr_oi,
            "put_call_volume_ratio": pcr_vol,
            "options_bias": options_bias,
            "score_oi_ratio": comp_oi,
            "score_volume_ratio": comp_vol,
            "score_bias": comp_bias,
        },
    }


def _validate_total_weights_strict(symbol: str) -> None:
    stage = "engine_scores.weights"
    total = float(sum(_TOTAL_WEIGHTS.values()))
    if not math.isfinite(total):
        _fail_unified(symbol, stage, f"weight sum is not finite: {total}")
    if abs(total - 1.0) > 1e-9:
        _fail_unified(symbol, stage, f"_TOTAL_WEIGHTS sum must be 1.0 (got={total})")


def _build_engine_scores_strict(
    symbol: str,
    timeframes: Dict[str, Any],
    pattern_features: Dict[str, Dict[str, Any]],
    pattern_summary: Dict[str, Any],
    orderbook: Dict[str, Any],
    volume_profile: VolumeProfileReport,
    orderflow_cvd: OrderFlowCvdReport,
    options_market: OptionsMarketSnapshot,
) -> Dict[str, Any]:
    _validate_total_weights_strict(symbol)

    tf4h = _require_dict(symbol, "engine_scores", timeframes.get("4h"), "timeframes['4h']")
    ohlcv4h = _parse_ohlcv_rows(symbol, "engine_scores", tf4h.get("raw_ohlcv_last60"), min_len=60, name="4h.raw_ohlcv_last60")
    trend_4h = _score_trend_4h(symbol, tf4h, ohlcv4h)

    tf1h = _require_dict(symbol, "engine_scores", timeframes.get("1h"), "timeframes['1h']")
    ohlcv1h = _parse_ohlcv_rows(symbol, "engine_scores", tf1h.get("raw_ohlcv_last60"), min_len=60, name="1h.raw_ohlcv_last60")
    momentum_1h = _score_momentum_1h(symbol, tf1h, ohlcv1h)

    tf15 = _require_dict(symbol, "engine_scores", timeframes.get("15m"), "timeframes['15m']")
    ohlcv15 = _parse_ohlcv_rows(symbol, "engine_scores", tf15.get("raw_ohlcv_last20"), min_len=20, name="15m.raw_ohlcv_last20")
    structure_15m = _score_structure_15m(symbol, tf15, ohlcv15)
    volatility_regime = _score_volatility_regime(symbol, tf15, ohlcv15)

    tf5 = _require_dict(symbol, "engine_scores", timeframes.get("5m"), "timeframes['5m']")
    ohlcv5 = _parse_ohlcv_rows(symbol, "engine_scores", tf5.get("raw_ohlcv_last20"), min_len=20, name="5m.raw_ohlcv_last20")
    pf5 = _require_dict(symbol, "engine_scores", pattern_features.get("5m"), "pattern_features['5m']")
    timing_5m = _score_timing_5m(symbol, tf5, ohlcv5, pattern_summary, pf5)
    volume_regime = _score_volume_regime(symbol, ohlcv5)
    liquidity_regime = _score_liquidity_regime(symbol, ohlcv5)

    orderbook_micro = _score_orderbook_micro(symbol, orderbook)
    volume_profile_regime = _score_volume_profile_regime(symbol, volume_profile)
    orderflow_regime = _score_orderflow_regime(symbol, orderflow_cvd)
    options_regime = _score_options_regime(symbol, options_market)

    w = _TOTAL_WEIGHTS
    total_score = (
        float(trend_4h["score"]) * w["trend_4h"]
        + float(momentum_1h["score"]) * w["momentum_1h"]
        + float(structure_15m["score"]) * w["structure_15m"]
        + float(timing_5m["score"]) * w["timing_5m"]
        + float(orderbook_micro["score"]) * w["orderbook_micro"]
        + float(volatility_regime["score"]) * w["volatility_regime"]
        + float(volume_regime["score"]) * w["volume_regime"]
        + float(liquidity_regime["score"]) * w["liquidity_regime"]
        + float(volume_profile_regime["score"]) * w["volume_profile_regime"]
        + float(orderflow_regime["score"]) * w["orderflow_regime"]
        + float(options_regime["score"]) * w["options_regime"]
    )
    total_score = _clamp(total_score, 0.0, 100.0)

    return {
        "trend_4h": trend_4h,
        "momentum_1h": momentum_1h,
        "structure_15m": structure_15m,
        "timing_5m": timing_5m,
        "orderbook_micro": orderbook_micro,
        "volatility_regime": volatility_regime,
        "volume_regime": volume_regime,
        "liquidity_regime": liquidity_regime,
        "volume_profile_regime": volume_profile_regime,
        "orderflow_regime": orderflow_regime,
        "options_regime": options_regime,
        "total": {"score": total_score, "weights": dict(w)},
    }


def _compact_indicators_strict(symbol: str, tf: str, indicators: Dict[str, Any]) -> Dict[str, Any]:
    stage = f"timeframes[{tf}].indicators"
    out: Dict[str, Any] = {}

    for k in _INDICATOR_KEEP_KEYS:
        if k not in indicators:
            _fail_unified(symbol, stage, f"missing indicator key: {k}")
        out[k] = _require_float(symbol, stage, indicators.get(k), f"indicators.{k}")

    return out


def _compact_timeframes_for_output(symbol: str, timeframes: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    for tf in REQUIRED_TFS:
        stage = f"timeframes[{tf}]"
        tf_data = _require_dict(symbol, stage, timeframes.get(tf), f"timeframes['{tf}']")

        tf_out: Dict[str, Any] = {}
        for k, v in tf_data.items():
            if k == "candles":
                continue
            if k.startswith("debug") or k.endswith("_series"):
                continue
            if k == "indicators":
                continue
            if k == "raw_ohlcv_last20" and tf not in ("1m", "5m", "15m"):
                continue
            if k == "raw_ohlcv_last60" and tf not in ("1h", "4h"):
                continue
            if isinstance(v, list) and len(v) > 200 and k not in ("raw_ohlcv_last20", "raw_ohlcv_last60"):
                continue
            if v is None:
                continue
            tf_out[k] = v

        indicators = _require_dict(symbol, stage, tf_data.get("indicators"), f"{tf}.indicators")
        tf_out["indicators"] = _compact_indicators_strict(symbol, tf, indicators)

        if tf in ("1m", "5m", "15m"):
            raw20 = tf_data.get("raw_ohlcv_last20")
            parsed20 = _parse_ohlcv_rows(symbol, stage, raw20, min_len=20, name=f"{tf}.raw_ohlcv_last20")
            tf_out["raw_ohlcv_last20"] = [[ts, o, h, l, c, v] for (ts, o, h, l, c, v) in parsed20]
        else:
            raw60 = tf_data.get("raw_ohlcv_last60")
            parsed60 = _parse_ohlcv_rows(symbol, stage, raw60, min_len=60, name=f"{tf}.raw_ohlcv_last60")
            tf_out["raw_ohlcv_last60"] = [[ts, o, h, l, c, v] for (ts, o, h, l, c, v) in parsed60]

        out[tf] = tf_out

    return out


def _load_microstructure_config(symbol: str) -> Tuple[str, int, int]:
    period = str(getattr(SET, "microstructure_period", _MICRO_PERIOD_DEFAULT)).strip()
    if not period:
        _fail_unified(symbol, "microstructure.config", "microstructure_period is empty")

    lookback_raw = getattr(SET, "microstructure_lookback", _MICRO_LOOKBACK_DEFAULT)
    ttl_raw = getattr(SET, "microstructure_cache_ttl_sec", _MICRO_CACHE_TTL_SEC_DEFAULT)

    try:
        lookback = int(lookback_raw)
    except Exception as e:
        _fail_unified(symbol, "microstructure.config", f"microstructure_lookback must be int (got={lookback_raw!r})", e)
    if lookback < 2 or lookback > 500:
        _fail_unified(symbol, "microstructure.config", f"microstructure_lookback out of range [2,500] (got={lookback})")

    try:
        ttl = int(ttl_raw)
    except Exception as e:
        _fail_unified(symbol, "microstructure.config", f"microstructure_cache_ttl_sec must be int (got={ttl_raw!r})", e)
    if ttl <= 0 or ttl > 300:
        _fail_unified(symbol, "microstructure.config", f"microstructure_cache_ttl_sec out of range [1,300] (got={ttl})")

    return period, lookback, ttl


def _rows_to_binance_klines(
    symbol: str,
    stage: str,
    rows: List[Tuple[int, float, float, float, float, float]],
    candle_ms: int,
) -> List[BinanceKline]:
    if candle_ms <= 0:
        _fail_unified(symbol, stage, f"candle_ms must be > 0 (got={candle_ms})")

    out: List[BinanceKline] = []
    for i, (ts, o, h, l, c, v) in enumerate(rows):
        close_ts = ts + candle_ms - 1
        if close_ts <= ts:
            _fail_unified(symbol, stage, f"invalid close_ts for row[{i}]: ts={ts}, close_ts={close_ts}")
        out.append(
            BinanceKline(
                open_time_ms=int(ts),
                open_price=Decimal(str(o)),
                high_price=Decimal(str(h)),
                low_price=Decimal(str(l)),
                close_price=Decimal(str(c)),
                volume=Decimal(str(v)),
                close_time_ms=int(close_ts),
                quote_asset_volume=Decimal(str(c)) * Decimal(str(v)),
                trade_count=1,
                taker_buy_base_volume=Decimal(str(v)) * Decimal("0.5"),
                taker_buy_quote_volume=(Decimal(str(c)) * Decimal(str(v))) * Decimal("0.5"),
            )
        )
    return out


def _build_volume_profile_feature_strict(
    symbol: str,
    timeframes: Dict[str, Any],
) -> VolumeProfileReport:
    stage = "volume_profile"
    tf_data = _require_dict(symbol, stage, timeframes.get(_VOL_PROFILE_SOURCE_TF), f"timeframes['{_VOL_PROFILE_SOURCE_TF}']")
    raw60 = tf_data.get("raw_ohlcv_last60")
    parsed60 = _parse_ohlcv_rows(symbol, stage, raw60, min_len=60, name=f"{_VOL_PROFILE_SOURCE_TF}.raw_ohlcv_last60")

    last_close = parsed60[-1][4]
    if last_close <= 0:
        _fail_unified(symbol, stage, f"last_close must be > 0 (got={last_close})")

    if last_close < 1.0:
        bucket_size = Decimal("0.001")
    elif last_close < 10.0:
        bucket_size = Decimal("0.01")
    elif last_close < 100.0:
        bucket_size = Decimal("0.1")
    elif last_close < 1000.0:
        bucket_size = Decimal("1")
    elif last_close < 10000.0:
        bucket_size = Decimal("5")
    elif last_close < 100000.0:
        bucket_size = Decimal("50")
    else:
        bucket_size = Decimal("100")

    klines = _rows_to_binance_klines(
        symbol,
        stage,
        parsed60,
        candle_ms=60 * 60 * 1000,
    )
    engine = VolumeProfileEngine(
        bucket_size=bucket_size,
        value_area_pct=Decimal(str(_VOL_PROFILE_VALUE_AREA_PCT)),
        volume_basis="quote",
        hvn_count=_VOL_PROFILE_HVN_COUNT,
        lvn_count=_VOL_PROFILE_LVN_COUNT,
    )
    return engine.build(symbol=symbol, klines=klines)


def _fetch_recent_aggtrades_strict(symbol: str, limit: int) -> List[Dict[str, Any]]:
    stage = "orderflow_cvd"
    if limit <= 0 or limit > 1000:
        _fail_unified(symbol, stage, f"aggTrade limit out of range [1,1000] (got={limit})")

    try:
        fetcher = BinanceMarketFetcher()
    except Exception as e:
        _fail_unified(symbol, stage, "BinanceMarketFetcher init failed", e)

    request_json = getattr(fetcher, "_request_json", None)
    if request_json is None or not callable(request_json):
        _fail_unified(symbol, stage, "BinanceMarketFetcher._request_json unavailable (STRICT)")

    try:
        payload = request_json(
            "GET",
            "/fapi/v1/aggTrades",
            params={"symbol": symbol, "limit": limit},
        )
    except Exception as e:
        _fail_unified(symbol, stage, f"aggTrades request failed: {e!r}", e)

    if not isinstance(payload, list) or not payload:
        _fail_unified(symbol, stage, "aggTrades response missing/empty")

    rows: List[Dict[str, Any]] = []
    for i, row in enumerate(payload):
        if not isinstance(row, dict):
            _fail_unified(symbol, stage, f"aggTrades[{i}] must be dict (got={type(row)})")
        rows.append(row)
    return rows


def _build_orderflow_feature_strict(symbol: str) -> OrderFlowCvdReport:
    stage = "orderflow_cvd"

    cached = _get_success_cache(symbol, stage, _ORDERFLOW_CACHE_TTL_SEC)
    if cached is not None:
        if not isinstance(cached, OrderFlowCvdReport):
            _fail_unified(symbol, stage, "cached orderflow payload type invalid")
        return cached

    rows = _fetch_recent_aggtrades_strict(symbol, _ORDERFLOW_AGGTRADE_LIMIT)

    engine = OrderFlowCvdEngine(
        min_trades=_ORDERFLOW_MIN_TRADES,
        dominance_threshold_pct=Decimal(str(_ORDERFLOW_DOMINANCE_THRESHOLD_PCT)),
        divergence_price_threshold_pct=Decimal(str(_ORDERFLOW_DIVERGENCE_PRICE_THRESHOLD_PCT)),
        divergence_delta_threshold_pct=Decimal(str(_ORDERFLOW_DIVERGENCE_DELTA_THRESHOLD_PCT)),
        cvd_path_tail_size=_ORDERFLOW_PATH_TAIL_SIZE,
    )
    try:
        report = engine.build_from_binance_payload(symbol=symbol, payload=rows)
    except Exception as e:
        _fail_unified(symbol, stage, f"OrderFlowCvdEngine failed: {e}", e)

    _set_success_cache(symbol, stage, report)
    return report


def _build_options_feature_strict(symbol: str) -> OptionsMarketSnapshot:
    stage = "options_market"

    cached = _get_success_cache(symbol, stage, _OPTIONS_CACHE_TTL_SEC)
    if cached is not None:
        if not isinstance(cached, OptionsMarketSnapshot):
            _fail_unified(symbol, stage, "cached options payload type invalid")
        return cached

    try:
        fetcher = OptionsMarketFetcher()
    except Exception as e:
        _fail_unified(symbol, stage, "OptionsMarketFetcher init failed", e)

    try:
        snap = fetcher.fetch()
    except Exception as e:
        _fail_unified(symbol, stage, f"OptionsMarketFetcher.fetch failed: {e}", e)

    if getattr(snap, "symbol", None) != symbol:
        _fail_unified(symbol, stage, f"options symbol mismatch: snapshot.symbol={getattr(snap, 'symbol', None)!r}")

    _set_success_cache(symbol, stage, snap)
    return snap


def build_unified_features(symbol: Optional[str] = None) -> Dict[str, Any]:
    if symbol is None:
        try:
            sym_raw = getattr(SET, "symbol")
        except AttributeError as e:
            _fail_unified("UNKNOWN", "symbol", "settings.symbol missing (STRICT)", e)
        sym = _require_nonempty_str("UNKNOWN", "symbol", sym_raw, "settings.symbol")
    else:
        sym = _require_nonempty_str("UNKNOWN", "symbol", symbol, "symbol")

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

    _ensure_timeframe_payloads_strict(sym, timeframes)

    orderbook = _get_orderbook_strict(sym, limit=5)

    pattern_features, pattern_summary = _build_pattern_bundle_strict(sym, timeframes, orderbook)
    volume_profile = _build_volume_profile_feature_strict(sym, timeframes)
    orderflow_cvd = _build_orderflow_feature_strict(sym)
    options_market = _build_options_feature_strict(sym)

    engine_scores = _build_engine_scores_strict(
        sym,
        timeframes,
        pattern_features,
        pattern_summary,
        orderbook,
        volume_profile,
        orderflow_cvd,
        options_market,
    )

    # Microstructure (STRICT: required)
    micro_period, micro_lookback, micro_ttl = _load_microstructure_config(sym)
    try:
        microstructure = build_microstructure_snapshot(
            sym,
            period=micro_period,
            lookback=micro_lookback,
            cache_ttl_sec=micro_ttl,
        )
    except MicrostructureError as e:
        _fail_unified(sym, "microstructure", f"MicrostructureError: {e}", e)
    except Exception as e:
        _fail_unified(sym, "microstructure", f"unexpected error: {e!r}", e)

    if not isinstance(microstructure, dict) or not microstructure:
        _fail_unified(sym, "microstructure", "microstructure snapshot must be non-empty dict (STRICT)")

    if "micro_score_risk" not in microstructure:
        _fail_unified(sym, "microstructure", "micro_score_risk missing (STRICT)")
    msr = _require_float(sym, "microstructure", microstructure.get("micro_score_risk"), "micro_score_risk")
    if msr < 0.0 or msr > 100.0:
        _fail_unified(sym, "microstructure", f"micro_score_risk out of range [0,100] (STRICT): {msr}")

    try:
        tp_pct = float(getattr(SET, "tp_pct"))
        sl_pct = float(getattr(SET, "sl_pct"))
    except Exception as e:
        _fail_unified(sym, "settings", "settings.tp_pct/settings.sl_pct required for invariant check (STRICT)", e)

    try:
        validate_signal_invariants_strict(
            SignalInvariantInputs(
                symbol=str(sym),
                direction="LONG",
                risk_pct=0.01,
                tp_pct=float(tp_pct),
                sl_pct=float(sl_pct),
                dd_pct=None,
                micro_score_risk=float(msr),
                final_risk_multiplier=None,
                equity_current_usdt=None,
                equity_peak_usdt=None,
            )
        )
    except InvariantViolation as e:
        _fail_unified(sym, "invariant_guard", f"invariant violation (STRICT): {e}", e)

    compact_timeframes = _compact_timeframes_for_output(sym, timeframes)

    result: Dict[str, Any] = dict(base)
    result["timeframes"] = compact_timeframes
    result["orderbook"] = orderbook
    result["pattern_features"] = pattern_features
    result["pattern_summary"] = pattern_summary
    result["engine_scores"] = engine_scores
    result["entry_score"] = float(engine_scores["total"]["score"]) / 100.0
    result["microstructure"] = microstructure
    result["volume_profile"] = {
        "source_timeframe": _VOL_PROFILE_SOURCE_TF,
        "bucket_size": float(volume_profile.bucket_size),
        "value_area_pct": float(volume_profile.value_area_pct),
        "current_price": float(volume_profile.current_price),
        "poc_price": float(volume_profile.poc_price),
        "value_area_low": float(volume_profile.value_area_low),
        "value_area_high": float(volume_profile.value_area_high),
        "poc_distance_bps": float(volume_profile.poc_distance_bps),
        "price_location": str(volume_profile.price_location),
        "hvn_nodes": [node.to_dict() for node in volume_profile.hvn_nodes],
        "lvn_nodes": [node.to_dict() for node in volume_profile.lvn_nodes],
    }
    result["orderflow_cvd"] = {
        "total_trades": int(orderflow_cvd.total_trades),
        "aggressive_buy_qty": float(orderflow_cvd.aggressive_buy_qty),
        "aggressive_sell_qty": float(orderflow_cvd.aggressive_sell_qty),
        "net_delta_qty": float(orderflow_cvd.net_delta_qty),
        "cvd": float(orderflow_cvd.cvd),
        "delta_ratio_pct": float(orderflow_cvd.delta_ratio_pct),
        "aggression_bias": str(orderflow_cvd.aggression_bias),
        "cvd_trend": str(orderflow_cvd.cvd_trend),
        "divergence": str(orderflow_cvd.divergence),
        "price_change_pct": float(orderflow_cvd.price_change_pct),
        "path_tail": [point.to_dict() for point in orderflow_cvd.cvd_path_tail],
    }
    result["options_market"] = {
        "source": str(options_market.source),
        "currency": str(options_market.currency),
        "index_price": float(options_market.index_price),
        "put_call_oi_ratio": float(options_market.put_call_oi_ratio),
        "put_call_volume_ratio": float(options_market.put_call_volume_ratio),
        "atm_put_call_oi_ratio": float(options_market.atm_put_call_oi_ratio),
        "oi_bias": str(options_market.oi_bias),
        "flow_bias": str(options_market.flow_bias),
        "options_bias": str(options_market.options_bias),
        "atm_call_instrument": str(options_market.atm_call_instrument),
        "atm_put_instrument": str(options_market.atm_put_instrument),
    }

    return result


__all__ = ["UnifiedFeaturesError", "build_unified_features"]