"""
=====================================================
FILE: infra/market_features_ws.py
BingX WebSocket 캔들(1m/5m/15m/...)과 depth5 오더북을
GPT-5.1 트레이더용 엔트리 피처로 변환하는 모듈.
STRICT · NO-FALLBACK · TRADE-GRADE MODE
=====================================================

핵심 원칙
-----------------------------------------------------
- WS 순수 데이터만 사용 (REST 백필 · 임의 보정 · 추론 · 폴백 전부 금지)
- 헬스 체크나 지표 계산 중 하나라도 비정상 / NaN / None 이 발견되면
  FeatureBuildError 를 발생시키고 피처 생성을 즉시 중단
- GPT 는 항상 "완전한 피처 셋"만 전달받고, 애매한 데이터로는 절대 진입하지 않음

주요 공개 함수
-----------------------------------------------------
- build_entry_features_ws(symbol) -> Dict[str, Any]
    · timeframes / orderbook / multi_timeframe 를 포함한 전체 WS 피처
- get_trading_signal(settings, last_close_ts, symbol) -> EntrySignal
    · entry_flow 가 바로 사용할 수 있는
      (chosen_signal, signal_source, latest_ts,
       candles_5m, candles_5m_raw, last_price, extra) 튜플

extra 주요 필드
-----------------------------------------------------
- signal_score   : 0.5 ~ 6.0 엔트리 강도 점수
- trend_strength : 5m regime 의 추세 강도 (필수, 없으면 시그널 생성 중단)
- volatility     : 5m ATR pct (필수)
- volume_zscore  : 5m 거래량 z-score (필수)
- majority_trend / depth_imbalance / spread_pct 등은
  GPT 자연어 해석 및 리스크 로그용 메타로 함께 제공된다.

변경 이력
-----------------------------------------------------
- 2026-03-01:
  1) raw_ohlcv_last20 포맷을 dict -> (ts_ms, o, h, l, c, v) tuple 로 변경 (STRICT 호환)
  2) indicators dict 에 scalar 지표(rsi/ema/atr/macd...) 포함 (unified_features_builder 호환)

- 2026-03-09 (TRADE-GRADE PATCH 5):
  1) FIX(ROOT-CAUSE): 동일 5m 캔들에서는 신호 재계산보다 freeze 상태를 우선 적용하는 early gate 추가
     - lightweight 5m/orderbook 확인 후 같은 latest_ts 이면 즉시 기존 결정 반환
     - 같은 캔들 동안 intra-candle LONG/SHORT flip 및 점수 흔들림 차단
  2) FIX(TRADE-GRADE): no-signal(None) 결정도 동일 5m 캔들 동안 freeze
     - 첫 판단이 저변동성/방향미확정으로 스킵이면 같은 캔들에서는 계속 스킵 유지
  3) FIX(STRICT): majority_trend 가 MIXED/NEUTRAL 인 경우 depth_imbalance 로 방향을 직접 결정하지 않음
     - 오더북 쏠림은 리스크/해석용 메타로만 유지
     - 방향 결정은 상위 TF trend → 5m EMA 정렬 순으로만 수행
  4) FIX(STRICT): 방향이 끝까지 확정되지 않으면 LONG 기본값으로 진입하지 않고 None 반환
  5) FIX(BUG): 저변동성 구간 스킵 로그 후 실제로 None 반환
  6) FIX(STRICT): freeze 재사용 시 last_close_ts 는 최신 런타임 값으로 갱신
  7) FIX(CLEANUP): 중복 top-level 함수 정의 제거
  8) FIX(MISSING): _compute_orderbook_features / build_entry_features_ws 누락 복원
=====================================================
"""

from __future__ import annotations

import math
import time
from typing import Any, Dict, List, Optional, Tuple

from settings import load_settings
from infra.telelog import log, send_tg
from infra.market_data_ws import (
    KLINE_MAX_DELAY_SEC,
    KLINE_MIN_BUFFER,
    ORDERBOOK_MAX_DELAY_SEC,
    get_health_snapshot,
    get_klines_with_volume,
    get_last_kline_delay_ms,
    get_orderbook,
)
from strategy.indicators import (
    Candle,
    adx,
    bollinger_bands,
    build_regime_features_from_candles,
    calc_atr,
    ema,
    macd,
    obv,
    rsi,
    stochastic_oscillator,
)

SET = load_settings()

EntrySignal = Tuple[
    str,                 # chosen_signal ("LONG"|"SHORT")
    str,                 # signal_source ("TREND"|"RANGE"|"GENERIC"...)
    int,                 # latest_ts (ms)
    List[List[float]],   # candles_5m (ts, o, h, l, c)
    List[List[float]],   # candles_5m_raw (ts, o, h, l, c, v)
    float,               # last_price
    Dict[str, Any],      # extra (GPT/EntryScore용 메타)
]

REQUIRED_TFS: List[str] = list(
    getattr(SET, "features_required_tfs", ["1m", "5m", "15m", "1h", "4h"])
)
EXTRA_TFS: List[str] = []

TF_CONFIG: Dict[str, Dict[str, int]] = {
    "1m": {"ema_fast": 9, "ema_slow": 21, "rsi": 14, "atr": 14, "range": 50, "vol_ma": 20},
    "5m": {"ema_fast": 20, "ema_slow": 50, "rsi": 14, "atr": 14, "range": 50, "vol_ma": 20},
    "15m": {"ema_fast": 20, "ema_slow": 50, "rsi": 14, "atr": 14, "range": 50, "vol_ma": 20},
    "1h": {"ema_fast": 20, "ema_slow": 50, "rsi": 14, "atr": 14, "range": 50, "vol_ma": 20},
    "4h": {"ema_fast": 20, "ema_slow": 50, "rsi": 14, "atr": 14, "range": 50, "vol_ma": 20},
    "1d": {"ema_fast": 20, "ema_slow": 50, "rsi": 14, "atr": 14, "range": 50, "vol_ma": 20},
}

FEATURE_ERROR_TG_COOLDOWN_SEC: float = float(
    getattr(SET, "features_error_tg_cooldown_sec", 60.0)
)


class FeatureBuildError(RuntimeError):
    """필수 시세 데이터가 부족/지연된 경우 사용하는 예외."""


_LAST_ERROR_SENT: Dict[str, float] = {}

# 동일 5m 캔들(latest_ts) 동안 최초 결정 결과를 그대로 재사용한다.
# - SIGNAL: chosen_signal / signal_source / extra 를 freeze
# - NO_SIGNAL: no-signal 결정도 freeze (같은 캔들 동안 재계산 금지)
# - last_price 는 freeze 하지 않는다(현재가 반영 유지)
_SIGNAL_FREEZE_STATE: Dict[str, Dict[str, Any]] = {}


def _normalize_symbol(symbol: Any) -> str:
    sym = str(symbol).upper().strip()
    if not sym:
        raise FeatureBuildError("symbol is empty (STRICT)")
    return sym


def _now_ms() -> int:
    return int(time.time() * 1000)


def _notify_error_once(key: str, human_msg: str) -> None:
    now = time.time()
    last = _LAST_ERROR_SENT.get(key)
    if last is not None and (now - last) < FEATURE_ERROR_TG_COOLDOWN_SEC:
        log(f"[MKT-FEAT] (suppressed) {human_msg}")
        return

    _LAST_ERROR_SENT[key] = now
    log(f"[MKT-FEAT] {human_msg}")
    try:
        send_tg("❌ [시세 데이터 오류 - GPT 피처 빌더]\n" + human_msg)
    except Exception:
        pass


def _fail(symbol: str, location: str, reason: str) -> None:
    msg = f"[{symbol}] {location}: {reason}"
    key = f"{symbol}|{location}|{reason}"
    _notify_error_once(key, msg)
    raise FeatureBuildError(msg)


def _get_signal_freeze_state(symbol: str) -> Optional[Dict[str, Any]]:
    state = _SIGNAL_FREEZE_STATE.get(_normalize_symbol(symbol))
    if state is None:
        return None
    if not isinstance(state, dict):
        raise FeatureBuildError("signal freeze state must be dict (STRICT)")
    return state


def _set_signal_freeze_state(
    *,
    symbol: str,
    latest_ts: int,
    chosen_signal: Optional[str],
    signal_source: Optional[str],
    extra: Optional[Dict[str, Any]],
    blocked_reason: Optional[str] = None,
) -> None:
    sym = _normalize_symbol(symbol)

    if not isinstance(latest_ts, int) or latest_ts <= 0:
        raise FeatureBuildError("signal freeze latest_ts must be int > 0 (STRICT)")

    if chosen_signal is None:
        if signal_source is not None:
            raise FeatureBuildError(
                "signal freeze signal_source must be None when chosen_signal is None (STRICT)"
            )
        if extra is not None:
            raise FeatureBuildError(
                "signal freeze extra must be None when chosen_signal is None (STRICT)"
            )

        reason = None if blocked_reason is None else str(blocked_reason).strip()
        _SIGNAL_FREEZE_STATE[sym] = {
            "latest_ts": int(latest_ts),
            "chosen_signal": None,
            "signal_source": None,
            "extra": None,
            "blocked_reason": (reason if reason else None),
        }
        return

    sig = str(chosen_signal).upper().strip()
    if sig not in ("LONG", "SHORT"):
        raise FeatureBuildError(
            f"signal freeze chosen_signal invalid (STRICT): {chosen_signal!r}"
        )

    src = str(signal_source).strip() if signal_source is not None else ""
    if not src:
        raise FeatureBuildError("signal freeze signal_source is empty (STRICT)")
    if not isinstance(extra, dict):
        raise FeatureBuildError("signal freeze extra must be dict (STRICT)")

    _SIGNAL_FREEZE_STATE[sym] = {
        "latest_ts": int(latest_ts),
        "chosen_signal": sig,
        "signal_source": src,
        "extra": dict(extra),
        "blocked_reason": None,
    }


def _clone_frozen_extra_for_return(
    frozen_extra: Dict[str, Any],
    *,
    last_close_ts: float,
) -> Dict[str, Any]:
    if not isinstance(frozen_extra, dict):
        raise FeatureBuildError("frozen extra must be dict (STRICT)")
    cloned = dict(frozen_extra)
    cloned["last_close_ts"] = float(last_close_ts)
    return cloned


def _fetch_candles_strict(
    symbol: str,
    interval: str,
    *,
    cfg: Dict[str, int],
    required: bool,
) -> List[Tuple[int, float, float, float, float, float]]:
    ema_fast_len = int(cfg.get("ema_fast", 20))
    ema_slow_len = int(cfg.get("ema_slow", 50))
    rsi_len = int(cfg.get("rsi", 14))
    atr_len = int(cfg.get("atr", 14))
    range_len = int(cfg.get("range", 50))

    need_len = max(
        ema_fast_len,
        ema_slow_len,
        rsi_len + 1,
        atr_len + 1,
        range_len,
        KLINE_MIN_BUFFER,
        atr_len * 2,
    )

    buf = get_klines_with_volume(symbol, interval, limit=max(300, need_len * 2))

    if not buf or len(buf) < need_len:
        reason = (
            f"{interval} 캔들이 부족합니다 "
            f"(필요 {need_len}개 이상, 현재 {0 if not buf else len(buf)}개). "
            "ws_subscribe_tfs / WS 백필 설정을 확인해 주세요."
        )
        if required:
            _fail(symbol, f"kline_{interval}", reason)
        log(f"[MKT-FEAT] optional {reason}")
        return []

    delay_ms = get_last_kline_delay_ms(symbol, interval)
    if delay_ms is None:
        reason = f"{interval} 마지막 캔들 수신 시각 정보를 가져오지 못했습니다."
        if required:
            _fail(symbol, f"kline_{interval}", reason)
        log(f"[MKT-FEAT] optional {reason}")
        return []

    if delay_ms > KLINE_MAX_DELAY_SEC * 1000.0:
        reason = (
            f"{interval} 캔들이 {delay_ms/1000.0:.1f}초 동안 갱신되지 않았습니다 "
            f"(허용 최대 {KLINE_MAX_DELAY_SEC:.1f}초)."
        )
        if required:
            _fail(symbol, f"kline_{interval}", reason)
        log(f"[MKT-FEAT] optional {reason}")
        return []

    return [r for r in buf[-max(300, need_len):] if len(r) >= 6]


def _candles_from_ws_buf(
    buf: List[Tuple[int, float, float, float, float, float]]
) -> List[Tuple[int, float, float, float, float, float]]:
    return buf


def _last_two_valid(vals: List[float]) -> Tuple[Optional[float], Optional[float]]:
    valid = [v for v in vals if not math.isnan(v)]
    if len(valid) < 2:
        return None, None
    return valid[-2], valid[-1]


def _detect_last_cross(
    ema_fast_vals: List[float],
    ema_slow_vals: List[float],
) -> Tuple[str, Optional[int]]:
    n = min(len(ema_fast_vals), len(ema_slow_vals))
    if n == 0:
        return "NONE", None

    last_cross_type: str = "NONE"
    last_cross_idx: Optional[int] = None
    prev_diff: Optional[float] = None

    for i in range(n):
        f = ema_fast_vals[i]
        s = ema_slow_vals[i]
        if math.isnan(f) or math.isnan(s):
            continue
        diff = f - s
        if prev_diff is not None:
            if prev_diff < 0 and diff > 0:
                last_cross_type = "GOLDEN"
                last_cross_idx = i
            elif prev_diff > 0 and diff < 0:
                last_cross_type = "DEAD"
                last_cross_idx = i
        prev_diff = diff

    if last_cross_idx is None:
        return "NONE", None

    bars_ago = n - 1 - last_cross_idx
    return last_cross_type, bars_ago


def _volume_stats(values: List[float], ma_len: int) -> Tuple[float, float, float, float]:
    if not values:
        return math.nan, math.nan, math.nan, math.nan

    last = float(values[-1])

    if len(values) < ma_len:
        window = [float(v) for v in values]
    else:
        window = [float(v) for v in values[-ma_len:]]

    mean = sum(window) / len(window)
    var = sum((v - mean) ** 2 for v in window) / len(window) if len(window) > 1 else 0.0
    std = math.sqrt(var)

    ratio = last / mean if mean > 0 else math.nan
    z = (last - mean) / std if std > 0 else 0.0
    return last, mean, ratio, z


def _normalize_regime_keys(regime: Dict[str, Any]) -> Dict[str, Any]:
    return dict(regime)


def _validate_core_features(
    symbol: str,
    interval: str,
    feats: Dict[str, Any],
) -> None:
    core_keys = [
        "last_close",
        "ema_fast",
        "ema_slow",
        "atr",
        "atr_pct",
        "range_pct",
        "rsi",
        "macd",
        "macd_signal",
        "macd_hist",
        "stoch_k",
        "stoch_d",
        "volume_last",
        "volume_ma",
        "volume_zscore",
    ]

    for key in core_keys:
        v = feats.get(key)
        if not isinstance(v, (int, float)) or math.isnan(float(v)):
            _fail(
                symbol,
                f"features_{interval}",
                f"{interval} 핵심 피처 '{key}' 가 NaN/None 입니다.",
            )

    adx_val = feats.get("adx")
    if adx_val is None or (
        isinstance(adx_val, (int, float)) and math.isnan(float(adx_val))
    ):
        _fail(
            symbol,
            f"features_{interval}",
            f"{interval} 핵심 피처 'adx' 가 NaN/None 입니다.",
        )


def _build_timeframe_features(
    symbol: str,
    interval: str,
    buf: List[Tuple[int, float, float, float, float, float]],
    cfg: Dict[str, int],
) -> Dict[str, Any]:
    closes = [c[4] for c in buf]
    highs = [c[2] for c in buf]
    lows = [c[3] for c in buf]
    vols = [c[5] for c in buf]
    last_close = closes[-1] if closes else math.nan

    ema_fast_len = int(cfg.get("ema_fast", 20))
    ema_slow_len = int(cfg.get("ema_slow", 50))
    rsi_len = int(cfg.get("rsi", 14))
    atr_len = int(cfg.get("atr", 14))
    range_len = int(cfg.get("range", 50))
    vol_ma_len = int(cfg.get("vol_ma", 20))

    candles_for_calc: List[Candle] = _candles_from_ws_buf(buf)

    raw_ohlcv_last20: List[Tuple[int, float, float, float, float, float]] = []
    raw_slice = buf[-20:] if len(buf) >= 20 else buf
    for ts, o, h, l, c, v in raw_slice:
        try:
            raw_ohlcv_last20.append(
                (int(ts), float(o), float(h), float(l), float(c), float(v))
            )
        except Exception as e:
            _fail(symbol, f"raw_ohlcv_{interval}", f"raw_ohlcv_last20 build failed: {e}")

    ema_fast_vals = ema(closes, ema_fast_len)
    ema_slow_vals = ema(closes, ema_slow_len)
    ema_fast_val = ema_fast_vals[-1] if ema_fast_vals else math.nan
    ema_slow_val = ema_slow_vals[-1] if ema_slow_vals else math.nan

    ema_fast_prev, ema_fast_last = _last_two_valid(ema_fast_vals)
    if math.isnan(ema_fast_val) or math.isnan(ema_slow_val) or last_close <= 0:
        ema_dist_pct = math.nan
    else:
        ema_dist_pct = (ema_fast_val - ema_slow_val) / last_close

    if ema_fast_prev is None or ema_fast_last is None or last_close <= 0:
        ema_fast_slope_pct = math.nan
    else:
        ema_fast_slope_pct = (ema_fast_last - ema_fast_prev) / last_close

    ema_slow_prev, ema_slow_last = _last_two_valid(ema_slow_vals)
    if ema_slow_prev is None or ema_slow_last is None or last_close <= 0:
        ema_slow_slope_pct = math.nan
    else:
        ema_slow_slope_pct = (ema_slow_last - ema_slow_prev) / last_close

    def _ret(n_bars: int) -> float:
        if len(closes) <= n_bars:
            return math.nan
        past = closes[-(n_bars + 1)]
        curr = closes[-1]
        if past <= 0:
            return math.nan
        return (curr - past) / past

    ret_1 = _ret(1)
    ret_3 = _ret(3)
    ret_5 = _ret(5)

    atr_val = calc_atr(candles_for_calc[-(atr_len + 1):], length=atr_len)
    atr_pct = (atr_val / last_close) if (atr_val is not None and last_close > 0) else math.nan

    window_for_range = (
        candles_for_calc[-range_len:]
        if len(candles_for_calc) >= range_len
        else candles_for_calc
    )
    if window_for_range:
        high_recent = max(c[2] for c in window_for_range)
        low_recent = min(c[3] for c in window_for_range)
        range_pct = (high_recent - low_recent) / last_close if last_close > 0 else math.nan
    else:
        range_pct = math.nan

    rsi_vals = rsi(closes, rsi_len)
    rsi_last = rsi_vals[-1] if rsi_vals else math.nan

    macd_line, macd_signal, macd_hist = macd(closes)
    macd_last = macd_line[-1] if macd_line else math.nan
    macd_signal_last = macd_signal[-1] if macd_signal else math.nan
    macd_hist_last = macd_hist[-1] if macd_hist else math.nan

    _, bb_upper, bb_lower = bollinger_bands(closes)
    bb_width_pct = math.nan
    bb_pos = math.nan
    if bb_upper and bb_lower:
        u = bb_upper[-1]
        l = bb_lower[-1]
        if (
            not math.isnan(u)
            and not math.isnan(l)
            and (u - l) > 0
            and last_close > 0
        ):
            bb_width_pct = (u - l) / last_close
            bb_pos = (last_close - l) / (u - l)

    stoch_k_vals, stoch_d_vals = stochastic_oscillator(highs, lows, closes)
    stoch_k_last = stoch_k_vals[-1] if stoch_k_vals else math.nan
    stoch_d_last = stoch_d_vals[-1] if stoch_d_vals else math.nan

    adx_val = adx(candles_for_calc, length=atr_len)

    vol_last, vol_ma, vol_ratio, vol_z = _volume_stats(vols, vol_ma_len)
    obv_vals = obv(closes, vols)
    obv_last = obv_vals[-1] if obv_vals else math.nan

    cross_type, cross_bars_ago = _detect_last_cross(ema_fast_vals, ema_slow_vals)

    try:
        if (
            not math.isnan(range_pct)
            and range_pct <= 0.0
            and atr_val is not None
            and atr_val <= 0.0
            and isinstance(vol_ma, (int, float))
            and vol_ma == 0.0
        ):
            reason = (
                f"{interval} 최근 {range_len}개 구간 동안 가격 변동과 평균 거래량이 모두 0에 가깝습니다. "
                "WS 시세가 멈췄거나 비정상일 수 있습니다."
            )
            _fail(symbol, f"flat_{interval}", reason)
    except Exception as e:
        log(f"[MKT-FEAT] flat-market check error interval={interval}: {e}")

    def _flag(condition: bool) -> int:
        return 1 if condition else 0

    rsi_overbought = _flag(rsi_last >= 70.0) if not math.isnan(rsi_last) else 0
    rsi_oversold = _flag(rsi_last <= 30.0) if not math.isnan(rsi_last) else 0
    stoch_overbought = _flag(stoch_k_last >= 80.0) if not math.isnan(stoch_k_last) else 0
    stoch_oversold = _flag(stoch_k_last <= 20.0) if not math.isnan(stoch_k_last) else 0

    if not math.isnan(macd_last) and not math.isnan(macd_signal_last):
        macd_bias = 1 if macd_last > macd_signal_last else -1 if macd_last < macd_signal_last else 0
    else:
        macd_bias = 0

    if adx_val is not None:
        strong_trend_flag = _flag(adx_val >= 25.0)
    else:
        strong_trend_flag = 0

    is_low_volatility = 0
    try:
        if (
            isinstance(atr_pct, (int, float))
            and not math.isnan(atr_pct)
            and isinstance(range_pct, (int, float))
            and not math.isnan(range_pct)
        ):
            low_range_th_local = 0.01
            low_atr_th_local = 0.004
            if atr_pct < low_atr_th_local and range_pct < low_range_th_local:
                is_low_volatility = 1
    except Exception as e:
        log(f"[MKT-FEAT] is_low_volatility 계산 중 예외 interval={interval}: {e}")

    tf_features: Dict[str, Any] = {
        "interval": interval,
        "buffer_len": len(buf),
        "last_close": last_close,
        "prev_close": closes[-2] if len(closes) >= 2 else math.nan,
        "return_1": ret_1,
        "return_3": ret_3,
        "return_5": ret_5,
        "ema_fast": ema_fast_val,
        "ema_slow": ema_slow_val,
        "ema_fast_len": ema_fast_len,
        "ema_slow_len": ema_slow_len,
        "ema_dist_pct": ema_dist_pct,
        "ema_fast_slope_pct": ema_fast_slope_pct,
        "atr": atr_val,
        "atr_pct": atr_pct,
        "range_pct": range_pct,
        "rsi": rsi_last,
        "rsi_len": rsi_len,
        "rsi_overbought": rsi_overbought,
        "rsi_oversold": rsi_oversold,
        "macd": macd_last,
        "macd_signal": macd_signal_last,
        "macd_hist": macd_hist_last,
        "macd_bias": macd_bias,
        "bb_width_pct": bb_width_pct,
        "bb_pos": bb_pos,
        "stoch_k": stoch_k_last,
        "stoch_d": stoch_d_last,
        "stoch_overbought": stoch_overbought,
        "stoch_oversold": stoch_oversold,
        "adx": adx_val,
        "strong_trend_flag": strong_trend_flag,
        "volume_last": vol_last,
        "volume_ma": vol_ma,
        "volume_ratio": vol_ratio,
        "volume_zscore": vol_z,
        "obv": obv_last,
        "cross_type": cross_type,
        "cross_bars_ago": cross_bars_ago,
        "is_low_volatility": is_low_volatility,
    }

    indicators: Dict[str, Any] = {
        "ema_fast": float(ema_fast_val) if isinstance(ema_fast_val, (int, float)) else math.nan,
        "ema_slow": float(ema_slow_val) if isinstance(ema_slow_val, (int, float)) else math.nan,
        "ema_fast_slope_pct": float(ema_fast_slope_pct) if isinstance(ema_fast_slope_pct, (int, float)) else math.nan,
        "ema_slow_slope_pct": float(ema_slow_slope_pct) if isinstance(ema_slow_slope_pct, (int, float)) else math.nan,
        "rsi": float(rsi_last) if isinstance(rsi_last, (int, float)) else math.nan,
        "atr": float(atr_val) if isinstance(atr_val, (int, float)) else math.nan,
        "atr_pct": float(atr_pct) if isinstance(atr_pct, (int, float)) else math.nan,
        "macd": float(macd_last) if isinstance(macd_last, (int, float)) else math.nan,
        "macd_signal": float(macd_signal_last) if isinstance(macd_signal_last, (int, float)) else math.nan,
        "macd_hist": float(macd_hist_last) if isinstance(macd_hist_last, (int, float)) else math.nan,
        "adx": float(adx_val) if isinstance(adx_val, (int, float)) else math.nan,
        "bb_width_pct": float(bb_width_pct) if isinstance(bb_width_pct, (int, float)) else math.nan,
        "bb_pos": float(bb_pos) if isinstance(bb_pos, (int, float)) else math.nan,
        "stoch_k": float(stoch_k_last) if isinstance(stoch_k_last, (int, float)) else math.nan,
        "stoch_d": float(stoch_d_last) if isinstance(stoch_d_last, (int, float)) else math.nan,
        "volume_ratio": float(vol_ratio) if isinstance(vol_ratio, (int, float)) else math.nan,
        "volume_zscore": float(vol_z) if isinstance(vol_z, (int, float)) else math.nan,
    }

    try:
        if rsi_vals:
            rsi_clean = [
                float(v)
                for v in rsi_vals
                if isinstance(v, (int, float)) and math.isfinite(float(v))
            ]
            if rsi_clean:
                indicators["rsi_series"] = rsi_clean
    except Exception:
        pass

    try:
        if macd_hist:
            macd_hist_clean = [
                float(v)
                for v in macd_hist
                if isinstance(v, (int, float)) and math.isfinite(float(v))
            ]
            if macd_hist_clean:
                indicators["macd_hist_series"] = macd_hist_clean
    except Exception:
        pass

    tf_features["indicators"] = indicators

    if interval in ("1m", "5m", "15m"):
        tf_features["raw_ohlcv_last20"] = raw_ohlcv_last20

    if interval in ("5m", "15m", "1h", "4h"):
        try:
            regime = build_regime_features_from_candles(
                candles_for_calc,
                fast_ema_len=ema_fast_len,
                slow_ema_len=ema_slow_len,
                atr_len=atr_len,
                rsi_len=rsi_len,
                range_window=range_len,
            )
            if regime is not None:
                tf_features["regime"] = _normalize_regime_keys(regime)
        except Exception as e:
            log(f"[MKT-FEAT] regime feature 계산 중 예외 interval={interval}: {e}")

    tf_features["candles_raw"] = [
        (int(ts), float(o), float(h), float(l), float(c), float(v))
        for (ts, o, h, l, c, v) in buf
    ]
    tf_features["candles"] = list(tf_features["candles_raw"])
    return tf_features


def _compute_trend_bias(tf_features: Dict[str, Any]) -> int:
    ema_fast_val = float(tf_features.get("ema_fast") or math.nan)
    ema_slow_val = float(tf_features.get("ema_slow") or math.nan)
    if math.isnan(ema_fast_val) or math.isnan(ema_slow_val):
        return 0
    if ema_fast_val > ema_slow_val:
        return 1
    if ema_fast_val < ema_slow_val:
        return -1
    return 0


def _build_multi_timeframe_summary(
    tf_map: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    trend_votes: Dict[str, int] = {}
    for iv, feats in tf_map.items():
        trend_votes[iv] = _compute_trend_bias(feats)

    long_votes = sum(1 for v in trend_votes.values() if v > 0)
    short_votes = sum(1 for v in trend_votes.values() if v < 0)

    if long_votes > 0 and short_votes == 0:
        majority_trend = "LONG"
    elif short_votes > 0 and long_votes == 0:
        majority_trend = "SHORT"
    elif long_votes == 0 and short_votes == 0:
        majority_trend = "NEUTRAL"
    else:
        majority_trend = "MIXED"

    trend_align_long = long_votes >= 3 and short_votes == 0
    trend_align_short = short_votes >= 3 and long_votes == 0

    adx_trend_tfs = 0
    overbought_tfs = 0
    oversold_tfs = 0
    low_vol_tfs = 0

    for feats in tf_map.values():
        adx_val = feats.get("adx")
        if isinstance(adx_val, (int, float)) and not math.isnan(adx_val) and adx_val >= 25.0:
            adx_trend_tfs += 1

        rsi_val = feats.get("rsi")
        stoch_k_val = feats.get("stoch_k")

        if isinstance(rsi_val, (int, float)) and not math.isnan(rsi_val) and rsi_val >= 70.0:
            overbought_tfs += 1
        elif isinstance(stoch_k_val, (int, float)) and not math.isnan(stoch_k_val) and stoch_k_val >= 80.0:
            overbought_tfs += 1

        if isinstance(rsi_val, (int, float)) and not math.isnan(rsi_val) and rsi_val <= 30.0:
            oversold_tfs += 1
        elif isinstance(stoch_k_val, (int, float)) and not math.isnan(stoch_k_val) and stoch_k_val <= 20.0:
            oversold_tfs += 1

        is_lv = feats.get("is_low_volatility")
        if isinstance(is_lv, int) and is_lv == 1:
            low_vol_tfs += 1

    return {
        "trend_votes": trend_votes,
        "long_votes": long_votes,
        "short_votes": short_votes,
        "trend_align_long": trend_align_long,
        "trend_align_short": trend_align_short,
        "majority_trend": majority_trend,
        "adx_trend_tfs": adx_trend_tfs,
        "overbought_tfs": overbought_tfs,
        "oversold_tfs": oversold_tfs,
        "low_vol_tfs": low_vol_tfs,
    }


def _compute_orderbook_features(symbol: str) -> Dict[str, Any]:
    ob = get_orderbook(symbol, limit=5)
    now_ms = _now_ms()

    if not ob or not ob.get("bids") or not ob.get("asks"):
        _fail(
            symbol,
            "orderbook",
            "depth5 오더북 스냅샷을 가져오지 못했습니다. WS 구독에 '@depth5' 가 포함되어 있는지 확인해 주세요.",
        )

    bids = ob.get("bids") or []
    asks = ob.get("asks") or []

    ts_ms: Optional[int] = None
    for key in ("exchTs", "ts"):
        if ob.get(key) is not None:
            try:
                ts_ms = int(ob[key])
                break
            except Exception:
                continue
    if ts_ms is None:
        ts_ms = now_ms

    age_sec = max(0.0, (now_ms - ts_ms) / 1000.0)
    if age_sec > ORDERBOOK_MAX_DELAY_SEC:
        _fail(
            symbol,
            "orderbook",
            f"오더북 스냅샷이 {age_sec:.1f}초 동안 갱신되지 않았습니다 (허용 최대 {ORDERBOOK_MAX_DELAY_SEC:.1f}초).",
        )

    best_bid = float(bids[0][0])
    best_ask = float(asks[0][0])
    spread_abs = best_ask - best_bid
    mid = (best_bid + best_ask) / 2.0 if (best_ask > 0 and best_bid > 0) else math.nan
    spread_pct = (spread_abs / mid) if (mid and mid > 0) else math.nan

    def _depth_notional(side: List[List[float]]) -> float:
        total = 0.0
        for row in side:
            if len(row) < 2:
                continue
            try:
                p = float(row[0])
                q = float(row[1])
                total += p * q
            except Exception:
                continue
        return total

    bid_notional = _depth_notional(bids)
    ask_notional = _depth_notional(asks)
    depth_imbalance: Optional[float]
    if bid_notional + ask_notional <= 0:
        depth_imbalance = None
    else:
        depth_imbalance = (bid_notional - ask_notional) / (bid_notional + ask_notional)
        depth_imbalance = max(min(depth_imbalance, 1.0), -1.0)

    return {
        "ts_ms": ts_ms,
        "age_sec": age_sec,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid_price": mid,
        "spread_abs": spread_abs,
        "spread_pct": spread_pct,
        "bid_notional": bid_notional,
        "ask_notional": ask_notional,
        "depth_imbalance": depth_imbalance,
        "mark_price": float(ob.get("markPrice")) if ob.get("markPrice") is not None else None,
        "last_price": float(ob.get("lastPrice")) if ob.get("lastPrice") is not None else None,
    }


def build_entry_features_ws(
    symbol: Optional[str] = None,
) -> Dict[str, Any]:
    if symbol is None:
        symbol = SET.symbol

    symbol = _normalize_symbol(symbol)
    checked_at_ms = _now_ms()

    try:
        snap = get_health_snapshot(symbol)
        if not snap.get("overall_ok", True):
            _fail(
                symbol,
                "health_snapshot",
                "WS 시세 데이터 헬스 체크 결과 overall_ok=False 입니다. market_data_ws.get_health_snapshot 로그를 확인해 주세요.",
            )
    except FeatureBuildError:
        raise
    except Exception as e:
        log(f"[MKT-FEAT] get_health_snapshot error: {e}")

    timeframes: Dict[str, Dict[str, Any]] = {}

    all_tfs: List[str] = list(dict.fromkeys(REQUIRED_TFS + EXTRA_TFS))
    for iv in all_tfs:
        cfg = TF_CONFIG.get(iv)
        if cfg is None:
            continue

        is_required = iv in REQUIRED_TFS

        buf = _fetch_candles_strict(
            symbol,
            iv,
            cfg=cfg,
            required=is_required,
        )
        if not buf:
            continue

        try:
            tf_features = _build_timeframe_features(symbol, iv, buf, cfg)
            if is_required:
                _validate_core_features(symbol, iv, tf_features)
            timeframes[iv] = tf_features
        except FeatureBuildError:
            raise
        except Exception as e:
            reason = f"{iv} 피처 계산 중 예외가 발생했습니다: {e}"
            if is_required:
                _fail(symbol, f"features_{iv}", reason)
            else:
                log(f"[MKT-FEAT] optional {reason}")

    if not timeframes:
        _fail(symbol, "features", "어떤 타임프레임에서도 피처를 생성하지 못했습니다.")

    ob_features = _compute_orderbook_features(symbol)
    mtf_summary = _build_multi_timeframe_summary(timeframes)

    return {
        "symbol": symbol,
        "checked_at_ms": checked_at_ms,
        "timeframes": timeframes,
        "orderbook": ob_features,
        "multi_timeframe": mtf_summary,
    }


def get_trading_signal(
    *,
    settings: Any,
    last_close_ts: float,
    symbol: Optional[str] = None,
) -> Optional[EntrySignal]:
    if symbol is None:
        symbol = getattr(settings, "symbol", None) or SET.symbol

    symbol = _normalize_symbol(symbol)

    cfg_5m = TF_CONFIG.get(
        "5m",
        {"ema_fast": 20, "ema_slow": 50, "rsi": 14, "atr": 14, "range": 50, "vol_ma": 20},
    )
    try:
        buf_5m = _fetch_candles_strict(
            symbol,
            "5m",
            cfg=cfg_5m,
            required=True,
        )
    except FeatureBuildError as e:
        log(f"[MKT-FEAT] get_trading_signal: 5m fetch FeatureBuildError: {e}")
        return None
    except Exception as e:
        log(f"[MKT-FEAT] get_trading_signal: 5m fetch unexpected error: {e}")
        return None

    if not buf_5m:
        log("[MKT-FEAT] get_trading_signal: 5m buf_5m 이 비어 있습니다.")
        return None

    candles_5m_raw: List[List[float]] = [
        [float(ts), float(o), float(h), float(l), float(c), float(v)]
        for (ts, o, h, l, c, v) in buf_5m
    ]
    candles_5m: List[List[float]] = [
        [float(ts), float(o), float(h), float(l), float(c)]
        for (ts, o, h, l, c, v) in buf_5m
    ]
    latest_ts = int(buf_5m[-1][0])

    try:
        ob_now = _compute_orderbook_features(symbol)
    except FeatureBuildError as e:
        log(f"[MKT-FEAT] get_trading_signal: orderbook FeatureBuildError: {e}")
        return None
    except Exception as e:
        log(f"[MKT-FEAT] get_trading_signal: orderbook unexpected error: {e}")
        return None

    last_price_candidates: List[float] = []
    ob_last = ob_now.get("last_price")
    if isinstance(ob_last, (int, float)):
        last_price_candidates.append(float(ob_last))

    ob_mark = ob_now.get("mark_price")
    if isinstance(ob_mark, (int, float)):
        last_price_candidates.append(float(ob_mark))

    tf5_close_light = buf_5m[-1][4]
    if isinstance(tf5_close_light, (int, float)):
        last_price_candidates.append(float(tf5_close_light))

    if last_price_candidates:
        last_price = float(last_price_candidates[0])
    else:
        last_price = float(buf_5m[-1][4])

    freeze_state = _get_signal_freeze_state(symbol)
    if freeze_state is not None:
        frozen_ts = freeze_state.get("latest_ts")
        if isinstance(frozen_ts, int) and frozen_ts == latest_ts:
            frozen_signal = freeze_state.get("chosen_signal")
            if frozen_signal is None:
                return None

            frozen_source = freeze_state.get("signal_source")
            frozen_extra = freeze_state.get("extra")
            if (
                isinstance(frozen_signal, str)
                and frozen_signal in ("LONG", "SHORT")
                and isinstance(frozen_source, str)
                and frozen_source.strip()
                and isinstance(frozen_extra, dict)
            ):
                return (
                    frozen_signal,
                    frozen_source,
                    latest_ts,
                    candles_5m,
                    candles_5m_raw,
                    float(last_price),
                    _clone_frozen_extra_for_return(
                        frozen_extra,
                        last_close_ts=float(last_close_ts),
                    ),
                )
            raise FeatureBuildError("signal freeze state payload invalid (STRICT)")

    try:
        features = build_entry_features_ws(symbol)
    except FeatureBuildError as e:
        log(f"[MKT-FEAT] get_trading_signal FeatureBuildError: {e}")
        return None
    except Exception as e:
        msg = f"[MKT-FEAT] get_trading_signal unexpected error: {e}"
        log(msg)
        try:
            send_tg("❌ [get_trading_signal 예외]\n" + msg)
        except Exception:
            pass
        return None

    tfs: Dict[str, Dict[str, Any]] = features.get("timeframes", {})
    ob: Dict[str, Any] = features.get("orderbook", {})
    mtf: Dict[str, Any] = features.get("multi_timeframe", {})

    tf5 = tfs.get("5m")
    if not tf5:
        log("[MKT-FEAT] get_trading_signal: 5m timeframes 가 없습니다.")
        _set_signal_freeze_state(
            symbol=symbol,
            latest_ts=latest_ts,
            chosen_signal=None,
            signal_source=None,
            extra=None,
            blocked_reason="missing_5m_features",
        )
        return None

    tf15 = tfs.get("15m")

    try:
        range_pct_5 = tf5.get("range_pct")
        atr_pct_5 = tf5.get("atr_pct")
        is_low_vol_5 = tf5.get("is_low_volatility") == 1

        low_range_th = float(
            getattr(
                settings,
                "low_vol_range_pct_threshold",
                getattr(SET, "low_vol_range_pct_threshold", 0.01),
            )
        )
        low_atr_th = float(
            getattr(
                settings,
                "low_vol_atr_pct_threshold",
                getattr(SET, "low_vol_atr_pct_threshold", 0.004),
            )
        )

        is_low_range = (
            isinstance(range_pct_5, (int, float))
            and not math.isnan(range_pct_5)
            and range_pct_5 < low_range_th
        )
        is_low_atr = (
            isinstance(atr_pct_5, (int, float))
            and not math.isnan(atr_pct_5)
            and atr_pct_5 < low_atr_th
        )

        low_vol_tfs = int(mtf.get("low_vol_tfs") or 0)

        if (is_low_range and is_low_atr) or (is_low_vol_5 and low_vol_tfs >= 2):
            range_str = (
                f"{range_pct_5:.4f}"
                if isinstance(range_pct_5, (int, float)) and not math.isnan(range_pct_5)
                else "nan"
            )
            atr_str = (
                f"{atr_pct_5:.4f}"
                if isinstance(atr_pct_5, (int, float)) and not math.isnan(atr_pct_5)
                else "nan"
            )
            log(
                "[MKT-FEAT] get_trading_signal: 저변동성 구간 스킵 "
                f"(5m range_pct={range_str}, "
                f"atr_pct={atr_str}, "
                f"th=({low_range_th:.4f}, {low_atr_th:.4f}), low_vol_tfs={low_vol_tfs})"
            )
            _set_signal_freeze_state(
                symbol=symbol,
                latest_ts=latest_ts,
                chosen_signal=None,
                signal_source=None,
                extra=None,
                blocked_reason="low_volatility",
            )
            return None
    except Exception as e:
        log(f"[MKT-FEAT] low-volatility filter 계산 중 예외 발생: {e}")

    last_price_candidates = []

    ob_last = ob.get("last_price")
    if isinstance(ob_last, (int, float)):
        last_price_candidates.append(float(ob_last))

    ob_mark = ob.get("mark_price")
    if isinstance(ob_mark, (int, float)):
        last_price_candidates.append(float(ob_mark))

    tf5_close = tf5.get("last_close")
    if isinstance(tf5_close, (int, float)):
        last_price_candidates.append(float(tf5_close))

    if last_price_candidates:
        last_price = float(last_price_candidates[0])
    else:
        last_price = float(buf_5m[-1][4])

    majority_trend = str(mtf.get("majority_trend", "NEUTRAL")).upper()
    depth_imbalance = ob.get("depth_imbalance")

    trend_bias = 0

    if majority_trend == "LONG":
        trend_bias = 1
    elif majority_trend == "SHORT":
        trend_bias = -1

    if trend_bias == 0:
        ema_fast_5 = tf5.get("ema_fast")
        ema_slow_5 = tf5.get("ema_slow")
        if isinstance(ema_fast_5, (int, float)) and isinstance(ema_slow_5, (int, float)):
            if ema_fast_5 > ema_slow_5:
                trend_bias = 1
            elif ema_fast_5 < ema_slow_5:
                trend_bias = -1

    if trend_bias > 0:
        chosen_signal = "LONG"
        direction_num = 1.0
    elif trend_bias < 0:
        chosen_signal = "SHORT"
        direction_num = -1.0
    else:
        log(
            "[MKT-FEAT] get_trading_signal: 방향이 확정되지 않아 시그널 생성을 중단합니다 "
            f"(majority_trend={majority_trend}, depth_imbalance={depth_imbalance})."
        )
        _set_signal_freeze_state(
            symbol=symbol,
            latest_ts=latest_ts,
            chosen_signal=None,
            signal_source=None,
            extra=None,
            blocked_reason="direction_undecided",
        )
        return None

    signal_source = "GENERIC"
    try:
        adx_trend_tfs = int(mtf.get("adx_trend_tfs") or 0)
        overbought_tfs = int(mtf.get("overbought_tfs") or 0)
        oversold_tfs = int(mtf.get("oversold_tfs") or 0)
        strong_trend_flag = tf5.get("strong_trend_flag")

        if adx_trend_tfs >= 2 and strong_trend_flag == 1:
            signal_source = "TREND"
        elif (overbought_tfs + oversold_tfs) >= 1:
            signal_source = "RANGE"
    except Exception:
        pass

    if signal_source == "TREND":
        regime_level = 1.0
    elif signal_source == "RANGE":
        regime_level = 2.0
    else:
        regime_level = 1.5

    atr_fast = tf5.get("atr")
    atr_fast_val = float(atr_fast) if isinstance(atr_fast, (int, float)) else float("nan")

    atr_slow = tf15.get("atr") if tf15 else atr_fast
    atr_slow_val = float(atr_slow) if isinstance(atr_slow, (int, float)) else float("nan")

    signal_score = 0.5
    try:
        vol_z = tf5.get("volume_zscore")
        if signal_source == "TREND":
            signal_score += 1.5
        if majority_trend in ("LONG", "SHORT"):
            signal_score += 1.0
        if isinstance(vol_z, (int, float)):
            signal_score += min(abs(vol_z) * 0.5, 2.0)
    except Exception:
        pass

    signal_score = max(0.5, min(signal_score, 6.0))

    reg5 = tf5.get("regime") or {}
    trend_strength = reg5.get("trend_strength")
    volatility = tf5.get("atr_pct")
    volume_zscore = tf5.get("volume_zscore")

    for name, value in (
        ("trend_strength", trend_strength),
        ("volatility", volatility),
        ("volume_zscore", volume_zscore),
    ):
        if not isinstance(value, (int, float)) or math.isnan(float(value)):
            log(
                f"[MKT-FEAT] get_trading_signal: 핵심 피처 {name} 가 NaN/None 입니다. "
                "엔트리 시그널 생성을 중단합니다."
            )
            _set_signal_freeze_state(
                symbol=symbol,
                latest_ts=latest_ts,
                chosen_signal=None,
                signal_source=None,
                extra=None,
                blocked_reason=f"invalid_{name}",
            )
            return None

    extra: Dict[str, Any] = {
        "signal_score": float(signal_score),
        "atr_fast": atr_fast_val,
        "atr_slow": atr_slow_val,
        "direction_raw": direction_num,
        "direction_norm": direction_num,
        "regime_level": regime_level,
        "trend_strength": float(trend_strength),
        "volatility": float(volatility),
        "volume_zscore": float(volume_zscore),
        "market_features": features,
        "last_close_ts": float(last_close_ts),
    }

    try:
        extra["majority_trend"] = majority_trend
        extra["depth_imbalance"] = depth_imbalance
        extra["spread_pct"] = ob.get("spread_pct")
        extra["volume_zscore_5m"] = tf5.get("volume_zscore")
        extra["strong_trend_flag_5m"] = tf5.get("strong_trend_flag")
    except Exception:
        pass

    _set_signal_freeze_state(
        symbol=symbol,
        latest_ts=latest_ts,
        chosen_signal=chosen_signal,
        signal_source=signal_source,
        extra=extra,
    )

    return (
        chosen_signal,
        signal_source,
        latest_ts,
        candles_5m,
        candles_5m_raw,
        float(last_price),
        extra,
    )


__all__ = [
    "FeatureBuildError",
    "build_entry_features_ws",
    "get_trading_signal",
]