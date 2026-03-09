"""
============================================================
FILE: core/entry_pipeline.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
============================================================

역할
- run_bot_ws 엔진의 진입 후보 생성 전용 파이프라인을 분리한다.
- WS 오더북/캔들 준비상태 검증, unified_features 적재, entry candidate 생성만 담당한다.
- 주문 실행 / SAFE_STOP / 텔레그램 전송 자체는 담당하지 않는다.

설계 원칙
- STRICT · NO-FALLBACK
- WS 버퍼 데이터만 사용한다.
- None→0, 조용한 continue, 임의 보정 금지.
- 필수 데이터 누락/손상 시 즉시 예외 또는 명시적 SKIP 처리.
- 기존 run_bot_ws.py 의 로직을 분리만 하고 의미를 바꾸지 않는다.

변경 이력
------------------------------------------------------------
- 2026-03-09:
  1) FIX(ROOT-CAUSE): signal payload 5m ts 와 authoritative WS 5m ts 를 강제 일치 검증
     - get_trading_signal 반환 latest_ts, candles_5m, candles_5m_raw, WS 5m buffer 마지막 ts 불일치 시 즉시 예외
     - signal 생성 시점과 진입 후보 생성 시점의 캔들 기준 불일치를 차단
  2) FIX(TRADE-GRADE): candles_5m / candles_5m_raw 를 authoritative WS 5m OHLCV 로 재구성
     - 5튜플 → volume=0.0 주입 제거
     - 진입 파이프라인 내부 None→0 / 임의 OHLCV 보정 제거
  3) FIX(INSTITUTIONAL): multi-timeframe directional confirmation guard 추가
     - 5m confirmed momentum 필수 정렬
     - 15m / 1h / orderflow / options secondary alignment 기반 반전 히스테리시스 추가
     - 새 5m 캔들마다 LONG↔SHORT 교차 진입되는 whipsaw 완화
  4) ADD(AUDIT): direction stability 진단 메타 추가
     - tf slope pct / directional state / support-opposition count 를 candidate meta 에 기록

- 2026-03-08:
  1) FIX(TRADE-GRADE): duplicate top-level function 제거
     - _require_tp_sl_from_settings_or_extra 중복 정의 제거
  2) static_source_scan(AST duplicate scan) 실패 원인 제거
  3) 기존 시그니처/기능/검증 규칙 삭제 없음

- 2026-03-07:
  1) run_bot_ws.py 에서 진입 파이프라인 로직 분리
  2) EntryCandidate / WS 준비상태 검증 / unified_features 진입 후보 생성 분리

- 2026-03-08:
  1) unified_features 의 volume_profile / orderflow_cvd / options_market 필드 연동 추가
  2) 진입 후보 생성 전 구조 피처 검증 강화
     - volume_profile.price_location / poc_distance_bps
     - orderflow_cvd.delta_ratio_pct / aggression_bias / divergence
     - options_market.options_bias / put_call ratios
  3) 방향 충돌 시 명시적 SKIP 처리 추가
     - LONG:
       * below_value_area
       * aggressive_sell_dominant + 음수 delta
       * bearish_divergence
       * bearish options bias
     - SHORT:
       * above_value_area
       * aggressive_buy_dominant + 양수 delta
       * bullish_divergence
       * bullish options bias
  4) 기존 진입 파이프라인 구조/시그니처/기능 삭제 없음
============================================================
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Sequence

from infra.market_data_ws import (
    get_klines_with_volume as ws_get_klines_with_volume,
    get_orderbook as ws_get_orderbook,
)
from infra.market_features_ws import FeatureBuildError, get_trading_signal
from strategy.unified_features_builder import UnifiedFeaturesError, build_unified_features
from infra.data_integrity_guard import (
    DataIntegrityError,
    validate_entry_market_data_bundle_strict,
    validate_kline_series_strict,
    validate_orderbook_strict,
)

ENTRY_REQUIRED_KLINES_MIN: Dict[str, int] = {"1m": 60, "5m": 60, "15m": 60, "1h": 60, "4h": 60}

ORDERFLOW_BLOCK_DELTA_RATIO_PCT: float = 8.0
OPTIONS_BEARISH_PCR_THRESHOLD: float = 1.20
OPTIONS_BULLISH_PCR_THRESHOLD: float = 0.83

DIRECTION_CONFIRM_MIN_LEN_BY_TF: Dict[str, int] = {
    "5m": 6,
    "15m": 6,
    "1h": 6,
}
DIRECTION_CONFIRM_LOOKBACK_CLOSED_BARS_BY_TF: Dict[str, int] = {
    "5m": 3,
    "15m": 3,
    "1h": 3,
}
DIRECTION_CONFIRM_MIN_SLOPE_PCT_BY_TF: Dict[str, float] = {
    "5m": 0.0004,
    "15m": 0.0008,
    "1h": 0.0015,
}
DIRECTION_CONFIRM_REQUIRED_SECONDARY_SUPPORTS: int = 1
DIRECTION_CONFIRM_MAX_SECONDARY_OPPOSE: int = 1


@dataclass(frozen=True, slots=True)
class EntryCandidate:
    action: str
    direction: str
    tp_pct: float
    sl_pct: float
    reason: str
    meta: Dict[str, Any]
    guard_adjustments: Dict[str, float]


def _as_float(v: Any, name: str, *, min_value: Optional[float] = None, max_value: Optional[float] = None) -> float:
    if isinstance(v, bool):
        raise RuntimeError(f"{name} must be numeric (bool not allowed)")
    try:
        x = float(v)
    except Exception as e:
        raise RuntimeError(f"{name} must be numeric: {e}") from e
    if not math.isfinite(x):
        raise RuntimeError(f"{name} must be finite")
    if min_value is not None and x < min_value:
        raise RuntimeError(f"{name} must be >= {min_value}")
    if max_value is not None and x > max_value:
        raise RuntimeError(f"{name} must be <= {max_value}")
    return float(x)


def _require_int_ms(v: Any, name: str) -> int:
    if isinstance(v, bool):
        raise RuntimeError(f"{name} must be int ms (bool not allowed)")
    try:
        iv = int(v)
    except Exception as e:
        raise RuntimeError(f"{name} must be int ms (STRICT): {e}") from e
    if iv <= 0:
        raise RuntimeError(f"{name} must be > 0 (STRICT)")
    return iv


def _require_nonempty_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        raise RuntimeError(f"{name} must be non-empty string (STRICT)")
    return s


def _normalize_price_location_strict(v: Any) -> str:
    s = _require_nonempty_str(v, "volume_profile.price_location")
    allowed = {
        "above_value_area",
        "below_value_area",
        "inside_value_area_above_poc",
        "inside_value_area_below_poc",
        "at_poc",
    }
    if s not in allowed:
        raise RuntimeError(f"invalid volume_profile.price_location (STRICT): {s!r}")
    return s


def _normalize_aggression_bias_strict(v: Any) -> str:
    s = _require_nonempty_str(v, "orderflow_cvd.aggression_bias")
    allowed = {
        "aggressive_buy_dominant",
        "aggressive_sell_dominant",
        "balanced",
    }
    if s not in allowed:
        raise RuntimeError(f"invalid orderflow_cvd.aggression_bias (STRICT): {s!r}")
    return s


def _normalize_divergence_strict(v: Any) -> str:
    s = _require_nonempty_str(v, "orderflow_cvd.divergence")
    allowed = {
        "bullish_divergence",
        "bearish_divergence",
        "none",
    }
    if s not in allowed:
        raise RuntimeError(f"invalid orderflow_cvd.divergence (STRICT): {s!r}")
    return s


def _normalize_options_bias_strict(v: Any) -> str:
    s = _require_nonempty_str(v, "options_market.options_bias")
    allowed = {
        "bullish",
        "bearish",
        "neutral",
        "mixed",
    }
    if s not in allowed:
        raise RuntimeError(f"invalid options_market.options_bias (STRICT): {s!r}")
    return s


def _require_tp_sl_from_settings_or_extra(settings: Any, extra: Any) -> tuple[float, float]:
    tp = _as_float(getattr(settings, "tp_pct", None), "settings.tp_pct", min_value=0.0, max_value=1.0)
    sl = _as_float(getattr(settings, "sl_pct", None), "settings.sl_pct", min_value=0.0, max_value=1.0)

    if isinstance(extra, dict):
        if extra.get("tp_pct") is not None:
            tp = _as_float(extra.get("tp_pct"), "extra.tp_pct", min_value=0.0, max_value=1.0)
        if extra.get("sl_pct") is not None:
            sl = _as_float(extra.get("sl_pct"), "extra.sl_pct", min_value=0.0, max_value=1.0)

    if tp <= 0.0:
        raise RuntimeError("tp_pct must be > 0 (STRICT)")
    if sl <= 0.0:
        raise RuntimeError("sl_pct must be > 0 (STRICT)")
    return float(tp), float(sl)


def _normalize_ws_ohlcv_row_strict(row: Any, *, name: str, index: int) -> tuple[int, float, float, float, float, float]:
    if not isinstance(row, (list, tuple)):
        raise RuntimeError(f"{name}[{index}] must be list/tuple (STRICT), got {type(row).__name__}")
    if len(row) < 6:
        raise RuntimeError(f"{name}[{index}] must have >=6 fields (STRICT), got len={len(row)}")

    ts = _require_int_ms(row[0], f"{name}[{index}].ts")
    o = _as_float(row[1], f"{name}[{index}].open", min_value=0.0)
    h = _as_float(row[2], f"{name}[{index}].high", min_value=0.0)
    l = _as_float(row[3], f"{name}[{index}].low", min_value=0.0)
    c = _as_float(row[4], f"{name}[{index}].close", min_value=0.0)
    v = _as_float(row[5], f"{name}[{index}].volume", min_value=0.0)

    if min(o, h, l, c) <= 0.0:
        raise RuntimeError(f"{name}[{index}] OHLC must be > 0 (STRICT)")
    if h < max(o, c, l):
        raise RuntimeError(f"{name}[{index}] high is inconsistent (STRICT)")
    if l > min(o, c, h):
        raise RuntimeError(f"{name}[{index}] low is inconsistent (STRICT)")

    return (int(ts), float(o), float(h), float(l), float(c), float(v))


def _normalize_ws_ohlcv_series_strict(arr: Any, *, name: str, min_len: int) -> list[tuple[int, float, float, float, float, float]]:
    if not isinstance(arr, list):
        raise RuntimeError(f"{name} must be list (STRICT)")
    if len(arr) < min_len:
        raise RuntimeError(f"{name} len insufficient (STRICT): need>={min_len} got={len(arr)}")

    fixed = [_normalize_ws_ohlcv_row_strict(it, name=name, index=i) for i, it in enumerate(arr)]
    try:
        validate_kline_series_strict(fixed, name=name, min_len=min_len)
    except DataIntegrityError as e:
        raise RuntimeError(f"{name} integrity fail (STRICT): {e}") from e
    return fixed


def _validate_signal_payload_candle_ts_strict(
    candles: Any,
    *,
    name: str,
    expected_latest_ts_ms: int,
    min_len: int,
    min_width: int,
) -> None:
    if not isinstance(candles, list):
        raise RuntimeError(f"{name} must be list (STRICT)")
    if len(candles) < min_len:
        raise RuntimeError(f"{name} len insufficient (STRICT): need>={min_len} got={len(candles)}")

    last_ts: Optional[int] = None
    prev_ts: Optional[int] = None
    for i, row in enumerate(candles):
        if not isinstance(row, (list, tuple)):
            raise RuntimeError(f"{name}[{i}] must be list/tuple (STRICT)")
        if len(row) < min_width:
            raise RuntimeError(f"{name}[{i}] width insufficient (STRICT): need>={min_width} got={len(row)}")
        ts = _require_int_ms(row[0], f"{name}[{i}].ts")
        if prev_ts is not None and ts <= prev_ts:
            raise RuntimeError(f"{name} ts must be strictly increasing (STRICT): prev={prev_ts} now={ts}")
        prev_ts = ts
        last_ts = ts

    if last_ts is None:
        raise RuntimeError(f"{name} is empty (STRICT)")
    if last_ts != expected_latest_ts_ms:
        raise RuntimeError(
            f"{name} latest ts mismatch (STRICT): expected={expected_latest_ts_ms} got={last_ts}"
        )


def _load_authoritative_ohlcv_series_strict(
    symbol: str,
    interval: str,
    *,
    min_len: int,
    expected_latest_ts_ms: Optional[int] = None,
) -> list[tuple[int, float, float, float, float, float]]:
    raw = ws_get_klines_with_volume(symbol, interval, limit=min_len)
    series = _normalize_ws_ohlcv_series_strict(raw, name=f"ws_kline[{interval}]", min_len=min_len)
    if expected_latest_ts_ms is not None:
        actual_latest_ts = int(series[-1][0])
        if actual_latest_ts != int(expected_latest_ts_ms):
            raise RuntimeError(
                f"ws_kline[{interval}] latest ts mismatch (STRICT): expected={expected_latest_ts_ms} got={actual_latest_ts}"
            )
    return series


def _compute_closed_slope_pct_strict(
    series: Sequence[tuple[int, float, float, float, float, float]],
    *,
    interval: str,
    lookback_closed_bars: int,
) -> float:
    need = int(lookback_closed_bars) + 1
    if len(series) < need + 1:
        raise RuntimeError(
            f"ws_kline[{interval}] insufficient for confirmed slope (STRICT): need>={need + 1} got={len(series)}"
        )

    closed = list(series[-(lookback_closed_bars + 1) : -1])
    if len(closed) != lookback_closed_bars:
        raise RuntimeError(f"ws_kline[{interval}] closed slice invalid (STRICT)")

    start_close = _as_float(closed[0][4], f"ws_kline[{interval}].closed_start_close", min_value=0.0)
    end_close = _as_float(closed[-1][4], f"ws_kline[{interval}].closed_end_close", min_value=0.0)
    if start_close <= 0.0:
        raise RuntimeError(f"ws_kline[{interval}] closed_start_close must be > 0 (STRICT)")
    return float((end_close - start_close) / start_close)


def _classify_directional_state_from_slope_pct_strict(slope_pct: float, *, threshold: float, label: str) -> str:
    sp = _as_float(slope_pct, label)
    th = _as_float(threshold, f"{label}.threshold", min_value=0.0)
    if sp >= th:
        return "LONG"
    if sp <= -th:
        return "SHORT"
    return "NEUTRAL"


def _derive_orderflow_directional_state_strict(
    *,
    delta_ratio_pct: float,
    aggression_bias: str,
    divergence: str,
) -> str:
    delta_ratio = _as_float(delta_ratio_pct, "market_data.delta_ratio_pct")
    bias = _normalize_aggression_bias_strict(aggression_bias)
    div = _normalize_divergence_strict(divergence)

    long_votes = 0
    short_votes = 0

    if bias == "aggressive_buy_dominant":
        long_votes += 1
    elif bias == "aggressive_sell_dominant":
        short_votes += 1

    if delta_ratio >= ORDERFLOW_BLOCK_DELTA_RATIO_PCT:
        long_votes += 1
    elif delta_ratio <= -ORDERFLOW_BLOCK_DELTA_RATIO_PCT:
        short_votes += 1

    if div == "bullish_divergence":
        long_votes += 1
    elif div == "bearish_divergence":
        short_votes += 1

    if long_votes > short_votes:
        return "LONG"
    if short_votes > long_votes:
        return "SHORT"
    return "NEUTRAL"


def _derive_options_directional_state_strict(
    *,
    options_bias: str,
    put_call_oi_ratio: float,
    put_call_volume_ratio: float,
) -> str:
    bias = _normalize_options_bias_strict(options_bias)
    oi_ratio = _as_float(put_call_oi_ratio, "market_data.put_call_oi_ratio", min_value=0.0)
    vol_ratio = _as_float(put_call_volume_ratio, "market_data.put_call_volume_ratio", min_value=0.0)

    if (
        bias == "bullish"
        and oi_ratio <= OPTIONS_BULLISH_PCR_THRESHOLD
        and vol_ratio <= OPTIONS_BULLISH_PCR_THRESHOLD
    ):
        return "LONG"
    if (
        bias == "bearish"
        and oi_ratio >= OPTIONS_BEARISH_PCR_THRESHOLD
        and vol_ratio >= OPTIONS_BEARISH_PCR_THRESHOLD
    ):
        return "SHORT"
    return "NEUTRAL"


def _evaluate_direction_stability_guard_strict(
    *,
    direction: str,
    tf_states: Dict[str, str],
    tf_slopes_pct: Dict[str, float],
    orderflow_state: str,
    options_state: str,
) -> Optional[str]:
    dir_norm = str(direction).upper().strip()
    if dir_norm not in ("LONG", "SHORT"):
        raise RuntimeError(f"invalid direction for stability guard (STRICT): {direction!r}")

    primary_state = str(tf_states.get("5m") or "").upper().strip()
    if primary_state != dir_norm:
        slope = tf_slopes_pct.get("5m")
        raise RuntimeError(
            f"5m confirmed momentum misaligned (STRICT): direction={dir_norm} state={primary_state} slope_pct={slope}"
        )

    support_count = 0
    oppose_count = 0
    for label in ("15m", "1h"):
        state = str(tf_states.get(label) or "").upper().strip()
        if state == dir_norm:
            support_count += 1
        elif state in ("LONG", "SHORT") and state != dir_norm:
            oppose_count += 1

    for label, state in (("orderflow", orderflow_state), ("options", options_state)):
        state_norm = str(state).upper().strip()
        if state_norm == dir_norm:
            support_count += 1
        elif state_norm in ("LONG", "SHORT") and state_norm != dir_norm:
            oppose_count += 1

    if support_count < DIRECTION_CONFIRM_REQUIRED_SECONDARY_SUPPORTS:
        return f"direction_secondary_support_insufficient:{dir_norm}:support={support_count}"
    if oppose_count > DIRECTION_CONFIRM_MAX_SECONDARY_OPPOSE:
        return f"direction_secondary_opposition_excess:{dir_norm}:oppose={oppose_count}"
    if oppose_count > support_count:
        return f"direction_secondary_alignment_negative:{dir_norm}:support={support_count}:oppose={oppose_count}"
    return None


def _extract_runtime_decision_meta_strict(market_features: Dict[str, Any], settings: Any) -> Dict[str, float | str]:
    if not isinstance(market_features, dict) or not market_features:
        raise RuntimeError("market_features is required (STRICT)")

    engine_scores = market_features.get("engine_scores")
    if not isinstance(engine_scores, dict) or not engine_scores:
        raise RuntimeError("market_features.engine_scores is required (STRICT)")

    total = engine_scores.get("total")
    if not isinstance(total, dict) or "score" not in total:
        raise RuntimeError("market_features.engine_scores.total.score is required (STRICT)")

    trend_4h = engine_scores.get("trend_4h")
    if not isinstance(trend_4h, dict) or not trend_4h:
        raise RuntimeError("market_features.engine_scores.trend_4h is required (STRICT)")

    trend_components = trend_4h.get("components")
    if not isinstance(trend_components, dict) or not trend_components:
        raise RuntimeError("market_features.engine_scores.trend_4h.components is required (STRICT)")
    if "trend_strength" not in trend_components:
        raise RuntimeError("market_features.engine_scores.trend_4h.components.trend_strength is required (STRICT)")

    orderbook = market_features.get("orderbook")
    if not isinstance(orderbook, dict) or not orderbook:
        raise RuntimeError("market_features.orderbook is required (STRICT)")
    if "spread_pct" not in orderbook:
        raise RuntimeError("market_features.orderbook.spread_pct is required (STRICT)")
    if "depth_imbalance" not in orderbook:
        raise RuntimeError("market_features.orderbook.depth_imbalance is required (STRICT)")

    volume_profile = market_features.get("volume_profile")
    if not isinstance(volume_profile, dict) or not volume_profile:
        raise RuntimeError("market_features.volume_profile is required (STRICT)")
    if "poc_price" not in volume_profile:
        raise RuntimeError("market_features.volume_profile.poc_price is required (STRICT)")
    if "value_area_low" not in volume_profile:
        raise RuntimeError("market_features.volume_profile.value_area_low is required (STRICT)")
    if "value_area_high" not in volume_profile:
        raise RuntimeError("market_features.volume_profile.value_area_high is required (STRICT)")
    if "poc_distance_bps" not in volume_profile:
        raise RuntimeError("market_features.volume_profile.poc_distance_bps is required (STRICT)")
    if "price_location" not in volume_profile:
        raise RuntimeError("market_features.volume_profile.price_location is required (STRICT)")

    orderflow_cvd = market_features.get("orderflow_cvd")
    if not isinstance(orderflow_cvd, dict) or not orderflow_cvd:
        raise RuntimeError("market_features.orderflow_cvd is required (STRICT)")
    if "delta_ratio_pct" not in orderflow_cvd:
        raise RuntimeError("market_features.orderflow_cvd.delta_ratio_pct is required (STRICT)")
    if "aggression_bias" not in orderflow_cvd:
        raise RuntimeError("market_features.orderflow_cvd.aggression_bias is required (STRICT)")
    if "divergence" not in orderflow_cvd:
        raise RuntimeError("market_features.orderflow_cvd.divergence is required (STRICT)")
    if "cvd" not in orderflow_cvd:
        raise RuntimeError("market_features.orderflow_cvd.cvd is required (STRICT)")
    if "price_change_pct" not in orderflow_cvd:
        raise RuntimeError("market_features.orderflow_cvd.price_change_pct is required (STRICT)")

    options_market = market_features.get("options_market")
    if not isinstance(options_market, dict) or not options_market:
        raise RuntimeError("market_features.options_market is required (STRICT)")
    if "put_call_oi_ratio" not in options_market:
        raise RuntimeError("market_features.options_market.put_call_oi_ratio is required (STRICT)")
    if "put_call_volume_ratio" not in options_market:
        raise RuntimeError("market_features.options_market.put_call_volume_ratio is required (STRICT)")
    if "options_bias" not in options_market:
        raise RuntimeError("market_features.options_market.options_bias is required (STRICT)")

    total_score_pct = _as_float(
        total.get("score"),
        "market_features.engine_scores.total.score",
        min_value=0.0,
        max_value=100.0,
    )
    trend_strength = _as_float(
        trend_components.get("trend_strength"),
        "market_features.engine_scores.trend_4h.components.trend_strength",
        min_value=0.0,
        max_value=1.0,
    )
    spread = _as_float(
        orderbook.get("spread_pct"),
        "market_features.orderbook.spread_pct",
        min_value=0.0,
    )
    orderbook_imbalance = _as_float(
        orderbook.get("depth_imbalance"),
        "market_features.orderbook.depth_imbalance",
        min_value=-1.0,
        max_value=1.0,
    )
    entry_score_threshold = _as_float(
        getattr(settings, "entry_score_threshold", None),
        "settings.entry_score_threshold",
        min_value=0.0,
    )

    poc_price = _as_float(
        volume_profile.get("poc_price"),
        "market_features.volume_profile.poc_price",
        min_value=0.0,
    )
    value_area_low = _as_float(
        volume_profile.get("value_area_low"),
        "market_features.volume_profile.value_area_low",
        min_value=0.0,
    )
    value_area_high = _as_float(
        volume_profile.get("value_area_high"),
        "market_features.volume_profile.value_area_high",
        min_value=0.0,
    )
    if value_area_high < value_area_low:
        raise RuntimeError("market_features.volume_profile.value_area_high < value_area_low (STRICT)")
    poc_distance_bps = _as_float(
        volume_profile.get("poc_distance_bps"),
        "market_features.volume_profile.poc_distance_bps",
    )
    price_location = _normalize_price_location_strict(volume_profile.get("price_location"))

    delta_ratio_pct = _as_float(
        orderflow_cvd.get("delta_ratio_pct"),
        "market_features.orderflow_cvd.delta_ratio_pct",
    )
    aggression_bias = _normalize_aggression_bias_strict(orderflow_cvd.get("aggression_bias"))
    divergence = _normalize_divergence_strict(orderflow_cvd.get("divergence"))
    cvd = _as_float(
        orderflow_cvd.get("cvd"),
        "market_features.orderflow_cvd.cvd",
    )
    orderflow_price_change_pct = _as_float(
        orderflow_cvd.get("price_change_pct"),
        "market_features.orderflow_cvd.price_change_pct",
    )

    put_call_oi_ratio = _as_float(
        options_market.get("put_call_oi_ratio"),
        "market_features.options_market.put_call_oi_ratio",
        min_value=0.0,
    )
    put_call_volume_ratio = _as_float(
        options_market.get("put_call_volume_ratio"),
        "market_features.options_market.put_call_volume_ratio",
        min_value=0.0,
    )
    options_bias = _normalize_options_bias_strict(options_market.get("options_bias"))

    return {
        "entry_score": float(total_score_pct) / 100.0,
        "trend_strength": float(trend_strength),
        "spread": float(spread),
        "orderbook_imbalance": float(orderbook_imbalance),
        "entry_score_threshold": float(entry_score_threshold),
        "poc_price": float(poc_price),
        "value_area_low": float(value_area_low),
        "value_area_high": float(value_area_high),
        "poc_distance_bps": float(poc_distance_bps),
        "price_location": price_location,
        "delta_ratio_pct": float(delta_ratio_pct),
        "aggression_bias": aggression_bias,
        "divergence": divergence,
        "cvd": float(cvd),
        "orderflow_price_change_pct": float(orderflow_price_change_pct),
        "put_call_oi_ratio": float(put_call_oi_ratio),
        "put_call_volume_ratio": float(put_call_volume_ratio),
        "options_bias": options_bias,
    }


def _evaluate_structural_entry_conflict_strict(market_data: Dict[str, Any]) -> Optional[str]:
    direction = str(market_data.get("direction") or "").upper().strip()
    if direction not in ("LONG", "SHORT"):
        raise RuntimeError(f"market_data.direction invalid for structural filter (STRICT): {direction!r}")

    price_location = _normalize_price_location_strict(market_data.get("price_location"))
    delta_ratio_pct = _as_float(
        market_data.get("delta_ratio_pct"),
        "market_data.delta_ratio_pct",
    )
    aggression_bias = _normalize_aggression_bias_strict(market_data.get("aggression_bias"))
    divergence = _normalize_divergence_strict(market_data.get("divergence"))
    options_bias = _normalize_options_bias_strict(market_data.get("options_bias"))
    put_call_oi_ratio = _as_float(
        market_data.get("put_call_oi_ratio"),
        "market_data.put_call_oi_ratio",
        min_value=0.0,
    )
    put_call_volume_ratio = _as_float(
        market_data.get("put_call_volume_ratio"),
        "market_data.put_call_volume_ratio",
        min_value=0.0,
    )

    if direction == "LONG":
        if price_location == "below_value_area":
            return "volume_profile_below_value_area_long_block"
        if aggression_bias == "aggressive_sell_dominant" and delta_ratio_pct <= -ORDERFLOW_BLOCK_DELTA_RATIO_PCT:
            return "orderflow_sell_dominant_long_block"
        if divergence == "bearish_divergence":
            return "bearish_divergence_long_block"
        if (
            options_bias == "bearish"
            and put_call_oi_ratio >= OPTIONS_BEARISH_PCR_THRESHOLD
            and put_call_volume_ratio >= OPTIONS_BEARISH_PCR_THRESHOLD
        ):
            return "bearish_options_long_block"
        return None

    if price_location == "above_value_area":
        return "volume_profile_above_value_area_short_block"
    if aggression_bias == "aggressive_buy_dominant" and delta_ratio_pct >= ORDERFLOW_BLOCK_DELTA_RATIO_PCT:
        return "orderflow_buy_dominant_short_block"
    if divergence == "bullish_divergence":
        return "bullish_divergence_short_block"
    if (
        options_bias == "bullish"
        and put_call_oi_ratio <= OPTIONS_BULLISH_PCR_THRESHOLD
        and put_call_volume_ratio <= OPTIONS_BULLISH_PCR_THRESHOLD
    ):
        return "bullish_options_short_block"
    return None


def _decide_entry_candidate_strict(market_data: Dict[str, Any], settings: Any) -> EntryCandidate:
    if not isinstance(market_data, dict) or not market_data:
        raise RuntimeError("market_data is required (STRICT)")

    symbol = str(market_data.get("symbol") or "").strip()
    if not symbol:
        raise RuntimeError("market_data.symbol is required (STRICT)")

    direction = str(market_data.get("direction") or "").upper().strip()
    if direction not in ("LONG", "SHORT"):
        raise RuntimeError(f"market_data.direction invalid (STRICT): {direction!r}")

    extra = market_data.get("extra")
    tp_pct, sl_pct = _require_tp_sl_from_settings_or_extra(settings, extra)

    if market_data.get("signal_ts_ms") is None:
        raise RuntimeError("market_data.signal_ts_ms is required (STRICT)")
    signal_ts_ms = _require_int_ms(market_data.get("signal_ts_ms"), "market_data.signal_ts_ms")

    if market_data.get("last_price") is None:
        raise RuntimeError("market_data.last_price is required (STRICT)")
    last_price = _as_float(market_data.get("last_price"), "market_data.last_price", min_value=0.0)
    if last_price <= 0:
        raise RuntimeError("market_data.last_price must be > 0 (STRICT)")

    entry_score = _as_float(market_data.get("entry_score"), "market_data.entry_score", min_value=0.0, max_value=1.0)
    trend_strength = _as_float(
        market_data.get("trend_strength"),
        "market_data.trend_strength",
        min_value=0.0,
        max_value=1.0,
    )
    spread = _as_float(market_data.get("spread"), "market_data.spread", min_value=0.0)
    orderbook_imbalance = _as_float(
        market_data.get("orderbook_imbalance"),
        "market_data.orderbook_imbalance",
        min_value=-1.0,
        max_value=1.0,
    )
    entry_score_threshold = _as_float(
        market_data.get("entry_score_threshold"),
        "market_data.entry_score_threshold",
        min_value=0.0,
    )

    price_location = _normalize_price_location_strict(market_data.get("price_location"))
    poc_price = _as_float(market_data.get("poc_price"), "market_data.poc_price", min_value=0.0)
    value_area_low = _as_float(market_data.get("value_area_low"), "market_data.value_area_low", min_value=0.0)
    value_area_high = _as_float(market_data.get("value_area_high"), "market_data.value_area_high", min_value=0.0)
    poc_distance_bps = _as_float(market_data.get("poc_distance_bps"), "market_data.poc_distance_bps")

    delta_ratio_pct = _as_float(market_data.get("delta_ratio_pct"), "market_data.delta_ratio_pct")
    aggression_bias = _normalize_aggression_bias_strict(market_data.get("aggression_bias"))
    divergence = _normalize_divergence_strict(market_data.get("divergence"))
    cvd = _as_float(market_data.get("cvd"), "market_data.cvd")
    orderflow_price_change_pct = _as_float(
        market_data.get("orderflow_price_change_pct"),
        "market_data.orderflow_price_change_pct",
    )

    put_call_oi_ratio = _as_float(
        market_data.get("put_call_oi_ratio"),
        "market_data.put_call_oi_ratio",
        min_value=0.0,
    )
    put_call_volume_ratio = _as_float(
        market_data.get("put_call_volume_ratio"),
        "market_data.put_call_volume_ratio",
        min_value=0.0,
    )
    options_bias = _normalize_options_bias_strict(market_data.get("options_bias"))

    direction_stability_guard_reason = market_data.get("direction_stability_guard_reason")
    if direction_stability_guard_reason is not None:
        reason = _require_nonempty_str(direction_stability_guard_reason, "market_data.direction_stability_guard_reason")
        return EntryCandidate(
            action="SKIP",
            direction=direction,
            tp_pct=float(tp_pct),
            sl_pct=float(sl_pct),
            reason=f"direction_stability_guard:{reason}",
            meta={
                "symbol": symbol,
                "signal_ts_ms": int(signal_ts_ms),
                "direction": direction,
                "direction_stability_guard_reason": reason,
            },
            guard_adjustments={},
        )

    meta = {
        "symbol": symbol,
        "regime": str(market_data.get("regime") or "").strip() or str(market_data.get("signal_source") or "").strip(),
        "signal_source": str(market_data.get("signal_source") or "").strip(),
        "signal_ts_ms": int(signal_ts_ms),
        "last_price": float(last_price),
        "candles_5m": market_data.get("candles_5m"),
        "candles_5m_raw": market_data.get("candles_5m_raw"),
        "extra": extra if isinstance(extra, dict) else None,
        "entry_score": float(entry_score),
        "trend_strength": float(trend_strength),
        "spread": float(spread),
        "orderbook_imbalance": float(orderbook_imbalance),
        "entry_score_threshold": float(entry_score_threshold),
        "poc_price": float(poc_price),
        "value_area_low": float(value_area_low),
        "value_area_high": float(value_area_high),
        "poc_distance_bps": float(poc_distance_bps),
        "price_location": price_location,
        "delta_ratio_pct": float(delta_ratio_pct),
        "aggression_bias": aggression_bias,
        "divergence": divergence,
        "cvd": float(cvd),
        "orderflow_price_change_pct": float(orderflow_price_change_pct),
        "put_call_oi_ratio": float(put_call_oi_ratio),
        "put_call_volume_ratio": float(put_call_volume_ratio),
        "options_bias": options_bias,
        "direction_tf_states": market_data.get("direction_tf_states"),
        "direction_tf_slopes_pct": market_data.get("direction_tf_slopes_pct"),
        "direction_orderflow_state": market_data.get("direction_orderflow_state"),
        "direction_options_state": market_data.get("direction_options_state"),
        "direction_secondary_support_count": market_data.get("direction_secondary_support_count"),
        "direction_secondary_oppose_count": market_data.get("direction_secondary_oppose_count"),
    }

    return EntryCandidate(
        action="ENTER",
        direction=direction,
        tp_pct=float(tp_pct),
        sl_pct=float(sl_pct),
        reason="ws_signal_candidate",
        meta=meta,
        guard_adjustments={},
    )


def _validate_orderbook_for_entry(symbol: str) -> Optional[str]:
    ob = ws_get_orderbook(symbol, limit=5)
    if not isinstance(ob, dict):
        return "orderbook missing (ws_get_orderbook returned non-dict/None)"

    try:
        validate_orderbook_strict(ob, symbol=str(symbol), require_ts=False)
    except DataIntegrityError as e:
        return f"orderbook integrity fail: {e}"

    bids = ob.get("bids")
    asks = ob.get("asks")
    if not isinstance(bids, list) or not bids:
        return "orderbook bids empty"
    if not isinstance(asks, list) or not asks:
        return "orderbook asks empty"

    best_bid = ob.get("bestBid")
    best_ask = ob.get("bestAsk")

    try:
        bb = float(bids[0][0]) if best_bid is None else float(best_bid)
        ba = float(asks[0][0]) if best_ask is None else float(best_ask)
    except Exception:
        return "orderbook bestBid/bestAsk invalid"

    if bb <= 0 or ba <= 0:
        return f"orderbook best prices invalid (bestBid={bb}, bestAsk={ba})"
    if ba <= bb:
        return f"orderbook crossed (bestAsk={ba} <= bestBid={bb})"
    return None


def _validate_klines_for_entry(symbol: str) -> Optional[str]:
    for iv, min_len in ENTRY_REQUIRED_KLINES_MIN.items():
        buf = ws_get_klines_with_volume(symbol, iv, limit=min_len)
        if not isinstance(buf, list):
            return f"kline buffer invalid type for {iv}"
        if len(buf) < min_len:
            return f"kline buffer 부족: {iv} need={min_len} got={len(buf)}"

        try:
            validate_kline_series_strict(buf, name=f"ws_kline[{iv}]", min_len=min_len)
        except DataIntegrityError as e:
            return f"kline integrity fail {iv}: {e}"

    return None


def _validate_ws_entry_prereqs(symbol: str) -> Optional[str]:
    r = _validate_orderbook_for_entry(symbol)
    if r:
        return r
    r = _validate_klines_for_entry(symbol)
    if r:
        return r
    return None


def _build_entry_market_data(
    settings: Any,
    last_close_ts: float,
    *,
    notify_entry_block_fn: Callable[[str, str, int], None],
    log_fn: Callable[[str], None],
) -> Optional[Dict[str, Any]]:
    signal_ctx = get_trading_signal(settings=settings, last_close_ts=last_close_ts)
    if signal_ctx is None:
        return None
    if not isinstance(signal_ctx, (tuple, list)) or len(signal_ctx) != 7:
        raise RuntimeError("get_trading_signal returned invalid tuple format")

    chosen_signal, signal_source, latest_ts, candles_5m, candles_5m_raw, last_price, extra = signal_ctx

    symbol = str(getattr(settings, "symbol", "")).strip()
    if not symbol:
        raise RuntimeError("settings.symbol is required")

    direction = str(chosen_signal).upper().strip()
    if direction not in ("LONG", "SHORT"):
        raise RuntimeError(f"invalid chosen_signal: {chosen_signal!r}")

    signal_source_s = str(signal_source).strip()
    if not signal_source_s:
        raise RuntimeError("signal_source is empty")

    ts_ms = _require_int_ms(latest_ts, "latest_ts")
    prereq_reason = _validate_ws_entry_prereqs(symbol)
    if prereq_reason:
        msg = f"[SKIP][WS_NOT_READY] entry blocked: {symbol} ({prereq_reason})"
        log_fn(msg)
        notify_entry_block_fn(f"WS_NOT_READY:{symbol}:{prereq_reason}", msg, 60)
        return None

    if extra is not None and not isinstance(extra, dict):
        raise RuntimeError("extra must be dict or None")

    _validate_signal_payload_candle_ts_strict(
        candles_5m,
        name="signal.candles_5m",
        expected_latest_ts_ms=ts_ms,
        min_len=ENTRY_REQUIRED_KLINES_MIN["5m"],
        min_width=5,
    )
    _validate_signal_payload_candle_ts_strict(
        candles_5m_raw,
        name="signal.candles_5m_raw",
        expected_latest_ts_ms=ts_ms,
        min_len=ENTRY_REQUIRED_KLINES_MIN["5m"],
        min_width=6,
    )

    authoritative_5m = _load_authoritative_ohlcv_series_strict(
        symbol,
        "5m",
        min_len=max(ENTRY_REQUIRED_KLINES_MIN["5m"], len(candles_5m_raw)),
        expected_latest_ts_ms=ts_ms,
    )
    confirm_15m = _load_authoritative_ohlcv_series_strict(
        symbol,
        "15m",
        min_len=max(ENTRY_REQUIRED_KLINES_MIN["15m"], DIRECTION_CONFIRM_MIN_LEN_BY_TF["15m"]),
    )
    confirm_1h = _load_authoritative_ohlcv_series_strict(
        symbol,
        "1h",
        min_len=max(ENTRY_REQUIRED_KLINES_MIN["1h"], DIRECTION_CONFIRM_MIN_LEN_BY_TF["1h"]),
    )

    lp = _as_float(last_price, "last_price", min_value=0.0)
    if lp <= 0:
        raise RuntimeError("last_price must be > 0 (STRICT)")

    try:
        market_features = build_unified_features(symbol)
    except (UnifiedFeaturesError, FeatureBuildError) as e:
        msg = f"[SKIP][FEATURE_BUILD_FAIL] entry blocked: {symbol} ({e})"
        log_fn(msg)
        notify_entry_block_fn(f"FEATURE_BUILD_FAIL:{symbol}:{type(e).__name__}", msg, 60)
        return None

    decision_meta = _extract_runtime_decision_meta_strict(market_features, settings)

    direction_tf_slopes_pct = {
        "5m": _compute_closed_slope_pct_strict(
            authoritative_5m,
            interval="5m",
            lookback_closed_bars=DIRECTION_CONFIRM_LOOKBACK_CLOSED_BARS_BY_TF["5m"],
        ),
        "15m": _compute_closed_slope_pct_strict(
            confirm_15m,
            interval="15m",
            lookback_closed_bars=DIRECTION_CONFIRM_LOOKBACK_CLOSED_BARS_BY_TF["15m"],
        ),
        "1h": _compute_closed_slope_pct_strict(
            confirm_1h,
            interval="1h",
            lookback_closed_bars=DIRECTION_CONFIRM_LOOKBACK_CLOSED_BARS_BY_TF["1h"],
        ),
    }
    direction_tf_states = {
        tf: _classify_directional_state_from_slope_pct_strict(
            slope_pct,
            threshold=DIRECTION_CONFIRM_MIN_SLOPE_PCT_BY_TF[tf],
            label=f"direction_tf_slopes_pct[{tf}]",
        )
        for tf, slope_pct in direction_tf_slopes_pct.items()
    }

    direction_orderflow_state = _derive_orderflow_directional_state_strict(
        delta_ratio_pct=float(decision_meta["delta_ratio_pct"]),
        aggression_bias=str(decision_meta["aggression_bias"]),
        divergence=str(decision_meta["divergence"]),
    )
    direction_options_state = _derive_options_directional_state_strict(
        options_bias=str(decision_meta["options_bias"]),
        put_call_oi_ratio=float(decision_meta["put_call_oi_ratio"]),
        put_call_volume_ratio=float(decision_meta["put_call_volume_ratio"]),
    )

    out = {
        "symbol": symbol,
        "direction": direction,
        "signal_source": signal_source_s,
        "regime": signal_source_s,
        "signal_ts_ms": int(ts_ms),
        "candles_5m": list(authoritative_5m),
        "candles_5m_raw": list(authoritative_5m),
        "last_price": float(lp),
        "extra": extra,
        "market_features": market_features,
        "entry_score": float(decision_meta["entry_score"]),
        "trend_strength": float(decision_meta["trend_strength"]),
        "spread": float(decision_meta["spread"]),
        "orderbook_imbalance": float(decision_meta["orderbook_imbalance"]),
        "entry_score_threshold": float(decision_meta["entry_score_threshold"]),
        "poc_price": float(decision_meta["poc_price"]),
        "value_area_low": float(decision_meta["value_area_low"]),
        "value_area_high": float(decision_meta["value_area_high"]),
        "poc_distance_bps": float(decision_meta["poc_distance_bps"]),
        "price_location": str(decision_meta["price_location"]),
        "delta_ratio_pct": float(decision_meta["delta_ratio_pct"]),
        "aggression_bias": str(decision_meta["aggression_bias"]),
        "divergence": str(decision_meta["divergence"]),
        "cvd": float(decision_meta["cvd"]),
        "orderflow_price_change_pct": float(decision_meta["orderflow_price_change_pct"]),
        "put_call_oi_ratio": float(decision_meta["put_call_oi_ratio"]),
        "put_call_volume_ratio": float(decision_meta["put_call_volume_ratio"]),
        "options_bias": str(decision_meta["options_bias"]),
        "direction_tf_slopes_pct": direction_tf_slopes_pct,
        "direction_tf_states": direction_tf_states,
        "direction_orderflow_state": direction_orderflow_state,
        "direction_options_state": direction_options_state,
    }

    structural_block_reason = _evaluate_structural_entry_conflict_strict(out)
    if structural_block_reason is not None:
        msg = f"[SKIP][STRUCTURE_FILTER] entry blocked: {symbol} ({direction}) reason={structural_block_reason}"
        log_fn(msg)
        notify_entry_block_fn(f"STRUCTURE_FILTER:{symbol}:{structural_block_reason}", msg, 60)
        return None

    direction_stability_guard_reason = _evaluate_direction_stability_guard_strict(
        direction=direction,
        tf_states=direction_tf_states,
        tf_slopes_pct=direction_tf_slopes_pct,
        orderflow_state=direction_orderflow_state,
        options_state=direction_options_state,
    )
    secondary_support_count = 0
    secondary_oppose_count = 0
    for state in (
        direction_tf_states["15m"],
        direction_tf_states["1h"],
        direction_orderflow_state,
        direction_options_state,
    ):
        state_norm = str(state).upper().strip()
        if state_norm == direction:
            secondary_support_count += 1
        elif state_norm in ("LONG", "SHORT") and state_norm != direction:
            secondary_oppose_count += 1

    out["direction_secondary_support_count"] = int(secondary_support_count)
    out["direction_secondary_oppose_count"] = int(secondary_oppose_count)
    out["direction_stability_guard_reason"] = direction_stability_guard_reason

    if direction_stability_guard_reason is not None:
        msg = (
            f"[SKIP][DIRECTION_STABILITY] entry blocked: {symbol} ({direction}) "
            f"reason={direction_stability_guard_reason} tf_states={direction_tf_states} "
            f"tf_slopes_pct={direction_tf_slopes_pct} orderflow={direction_orderflow_state} "
            f"options={direction_options_state}"
        )
        log_fn(msg)
        notify_entry_block_fn(f"DIRECTION_STABILITY:{symbol}:{direction_stability_guard_reason}", msg, 60)
        return None

    validate_entry_market_data_bundle_strict(out)
    return out


__all__ = [
    "EntryCandidate",
    "_build_entry_market_data",
    "_decide_entry_candidate_strict",
]