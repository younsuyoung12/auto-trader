from __future__ import annotations

"""
=====================================================
FILE: infra/market_features_ws.py
ROLE:
- WebSocket 기반 캔들/오더북 데이터를 엔트리 피처로 변환한다.
- 상위 엔진(entry / unified_features / exit)이 사용할 수 있는
  STRICT · NO-FALLBACK · TRADE-GRADE 피처를 생성한다.

CORE RESPONSIBILITIES:
- 필수 타임프레임 WS 캔들 로드 및 freshness 검증
- 오더북(depth5) freshness / 무결성 검증
- 타임프레임별 지표 및 multi-timeframe summary 생성
- entry_flow 가 직접 사용할 수 있는 trading signal 생성
- market_data_ws health snapshot 계약을 STRICT 검증하고 warning 정보를 보존한다

IMPORTANT POLICY:
- settings.py 는 단일 설정 소스(SSOT)다.
- 설정 상수/임계값을 market_data_ws 내부 구현 상수에서 가져오지 않는다.
- 데이터 누락 / stale / NaN / Inf / 계약 불일치는 즉시 예외 처리한다.
- orderbook timestamp 누락을 현재 시각으로 대체하지 않는다.
- 비핵심 알림 실패는 로깅하되, 핵심 데이터 실패를 가리지 않는다.
- market_data_ws 의 WARNING 상태는 관측 정보로 보존하되 feature 생성 실패로 오판하지 않는다.
- 방향 결정 계약은 entry_pipeline 과 정합해야 하며, 상위 TF 우선 편향으로
  저주기 confirmed momentum 을 무시하는 구조를 금지한다.
- get_trading_signal() 은 데이터/계약 실패를 no-signal(None)로 숨기지 않는다.
- LONG/SHORT 후보는 동일한 기준으로 동시에 계산해야 하며,
  한 방향을 먼저 확정한 뒤 다른 방향을 제거하는 구조를 금지한다.

CHANGE HISTORY:
- 2026-03-12:
  1) FIX(ROOT-CAUSE): LONG 편향의 직접 원인 제거
     - 기존 trend_bias 단일 방향 선결정 구조 제거
     - LONG / SHORT 후보를 동시에 점수화하고 margin 기반으로 최종 선택
  2) FIX(STRUCTURE): direction selection contract 정합화
     - 5m primary bias / 5m primary momentum 을 최우선으로 사용
     - 15m / 1h 는 support/oppose 보조 근거로 사용
     - 4h 는 선택 우선순위가 아닌 telemetry / 약한 bias 보조치로만 사용
  3) ADD(OBSERVABILITY): extra 에 direction_candidates / long_score / short_score /
     selection_margin / candidate telemetry 추가
  4) FIX(STRICT): 애매한 양방향 동점/저확신 구간은 no-signal 처리
     - arbitrary LONG fallback 금지
  5) KEEP(COMPAT): 기존 EntrySignal 반환 계약 / freeze state / health summary 유지

- 2026-03-11:
  1) FIX(ROOT-CAUSE): direction bias 결정을 entry_pipeline 정합 기준으로 재구성
     - 기존 4h/1h 우선 구조 제거
     - 1h/15m → 15m/5m → 1h/5m → 5m_only 순서로 변경
     - 4h 는 방향 선택이 아닌 관측 메타(telemetry)로만 유지
  2) FIX(TRADE-GRADE): LONG 편향 완화
     - 4h/1h LONG 이 15m/5m SHORT 전환을 덮어써 LONG 생성→차단 반복되던 구조 제거
  3) FIX(ROOT-CAUSE): strict indicators 계약과 정합화
     - OHLCV 버퍼 STRICT 검증 추가
     - 6필드 OHLCV → 5필드 Candle 변환 고정
     - regime/EMA200 기준 최소 buffer 요구량 상향
  4) FIX(STRICT): regime / low-vol / signal_source / signal_score 경로의 silent fail 제거
  5) FIX(STRICT): low-vol threshold 로컬 상수 제거, settings SSOT만 사용
  6) FIX(STRICT): get_trading_signal() 에서 데이터 실패와 no-signal 반환을 분리

- 2026-03-10:
  1) FIX(ROOT-CAUSE): market_data_ws health snapshot 계약(overall_ok / has_warning / overall_warnings)과 정합화
  2) FIX(STRICT): settings getattr default 제거, 필요한 설정은 명시적으로 검증
  3) ADD(OBSERVABILITY): feature 결과에 health summary(has_warning / warnings) 포함
  4) FIX(STRICT): low-vol threshold 숨은 기본값 제거
  5) FIX(STRICT): ws_min_kline_buffer_by_interval 계약 검증 강화
=====================================================
"""

import math
import time
from typing import Any, Dict, List, Optional, Tuple

from settings import SETTINGS
from infra.telelog import log, send_tg
from infra.market_data_ws import (
    get_health_snapshot,
    get_klines_with_volume,
    get_last_kline_delay_ms,
    get_orderbook,
    get_orderbook_buffer_status,
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

SET = SETTINGS


def _require_setting_attr(name: str) -> Any:
    if not hasattr(SET, name):
        raise RuntimeError(f"settings.{name} is required (STRICT)")
    return getattr(SET, name)


def _require_positive_float_setting(name: str) -> float:
    raw = _require_setting_attr(name)
    if isinstance(raw, bool):
        raise RuntimeError(f"settings.{name} must be float, bool is not allowed (STRICT)")
    try:
        value = float(raw)
    except Exception as e:
        raise RuntimeError(f"settings.{name} must be float-convertible (STRICT): {e}") from e
    if not math.isfinite(value) or value <= 0:
        raise RuntimeError(f"settings.{name} must be finite > 0 (STRICT)")
    return value


def _require_positive_int_setting(name: str) -> int:
    raw = _require_setting_attr(name)
    if isinstance(raw, bool):
        raise RuntimeError(f"settings.{name} must be int, bool is not allowed (STRICT)")
    try:
        value = int(raw)
    except Exception as e:
        raise RuntimeError(f"settings.{name} must be int-convertible (STRICT): {e}") from e
    if value <= 0:
        raise RuntimeError(f"settings.{name} must be > 0 (STRICT)")
    return value


def _require_str_list_setting(name: str) -> List[str]:
    raw = _require_setting_attr(name)
    if not isinstance(raw, (list, tuple)):
        raise RuntimeError(f"settings.{name} must be list/tuple[str] (STRICT)")
    out: List[str] = []
    for idx, item in enumerate(raw):
        s = str(item).strip()
        if not s:
            raise RuntimeError(f"settings.{name}[{idx}] must be non-empty string (STRICT)")
        out.append(s)
    if not out:
        raise RuntimeError(f"settings.{name} must not be empty (STRICT)")
    return out


def _require_interval_buffer_mapping_setting(name: str) -> Dict[str, int]:
    raw = _require_setting_attr(name)
    if not isinstance(raw, dict):
        raise RuntimeError(f"settings.{name} must be dict[str,int] (STRICT)")
    out: Dict[str, int] = {}
    for raw_interval, raw_value in raw.items():
        interval = str(raw_interval).strip()
        if not interval:
            raise RuntimeError(f"settings.{name} contains empty interval key (STRICT)")
        if isinstance(raw_value, bool):
            raise RuntimeError(f"settings.{name}[{interval}] must be int, bool is not allowed (STRICT)")
        try:
            iv = int(raw_value)
        except Exception as e:
            raise RuntimeError(f"settings.{name}[{interval}] must be int-convertible (STRICT): {e}") from e
        if iv <= 0:
            raise RuntimeError(f"settings.{name}[{interval}] must be > 0 (STRICT)")
        out[interval] = iv
    return out


def _resolve_runtime_positive_float(settings_obj: Any, attr_name: str) -> float:
    if settings_obj is not None and hasattr(settings_obj, attr_name):
        raw = getattr(settings_obj, attr_name)
    else:
        raw = _require_setting_attr(attr_name)

    if isinstance(raw, bool):
        raise FeatureBuildError(f"{attr_name} must be float, bool is not allowed (STRICT)")
    try:
        value = float(raw)
    except Exception as e:
        raise FeatureBuildError(f"{attr_name} must be float-convertible (STRICT): {e}") from e
    if not math.isfinite(value) or value <= 0:
        raise FeatureBuildError(f"{attr_name} must be finite > 0 (STRICT)")
    return value


EntrySignal = Tuple[
    str,                 # chosen_signal ("LONG"|"SHORT")
    str,                 # signal_source ("TREND"|"RANGE"|"GENERIC"...)
    int,                 # latest_ts (ms)
    List[List[float]],   # candles_5m (ts, o, h, l, c)
    List[List[float]],   # candles_5m_raw (ts, o, h, l, c, v)
    float,               # last_price
    Dict[str, Any],      # extra (GPT/EntryScore용 메타)
]

REQUIRED_TFS: List[str] = _require_str_list_setting("features_required_tfs")
EXTRA_TFS: List[str] = []

TF_CONFIG: Dict[str, Dict[str, int]] = {
    "1m": {"ema_fast": 9, "ema_slow": 21, "rsi": 14, "atr": 14, "range": 50, "vol_ma": 20},
    "5m": {"ema_fast": 20, "ema_slow": 50, "rsi": 14, "atr": 14, "range": 50, "vol_ma": 20},
    "15m": {"ema_fast": 20, "ema_slow": 50, "rsi": 14, "atr": 14, "range": 50, "vol_ma": 20},
    "1h": {"ema_fast": 20, "ema_slow": 50, "rsi": 14, "atr": 14, "range": 50, "vol_ma": 20},
    "4h": {"ema_fast": 20, "ema_slow": 50, "rsi": 14, "atr": 14, "range": 50, "vol_ma": 20},
    "1d": {"ema_fast": 20, "ema_slow": 50, "rsi": 14, "atr": 14, "range": 50, "vol_ma": 20},
}

FEATURE_ERROR_TG_COOLDOWN_SEC: float = _require_positive_float_setting(
    "features_error_tg_cooldown_sec"
)

KLINE_MAX_DELAY_SEC: float = _require_positive_float_setting("ws_max_kline_delay_sec")
ORDERBOOK_MAX_DELAY_SEC: float = _require_positive_float_setting("ws_market_event_max_delay_sec")
DEFAULT_KLINE_MIN_BUFFER: int = _require_positive_int_setting("ws_min_kline_buffer")
KLINE_MIN_BUFFER_BY_INTERVAL: Dict[str, int] = _require_interval_buffer_mapping_setting(
    "ws_min_kline_buffer_by_interval"
)
LOW_VOL_RANGE_PCT_THRESHOLD: float = _require_positive_float_setting("low_vol_range_pct_threshold")
LOW_VOL_ATR_PCT_THRESHOLD: float = _require_positive_float_setting("low_vol_atr_pct_threshold")

REGIME_INTERVALS = {"5m", "15m", "1h", "4h"}


class FeatureBuildError(RuntimeError):
    """필수 시세 데이터가 부족/지연/계약 위반인 경우 사용하는 예외."""


_LAST_ERROR_SENT: Dict[str, float] = {}

_SIGNAL_FREEZE_STATE: Dict[str, Dict[str, Any]] = {}


def _normalize_symbol(symbol: Any) -> str:
    sym = str(symbol).upper().strip()
    if not sym:
        raise FeatureBuildError("symbol is empty (STRICT)")
    return sym


def _now_ms() -> int:
    return int(time.time() * 1000)


def _is_finite_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))


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
    except Exception as e:
        log(f"[MKT-FEAT] telegram notify failed: {e.__class__.__name__}: {e}")


def _fail(symbol: str, location: str, reason: str) -> None:
    msg = f"[{symbol}] {location}: {reason}"
    key = f"{symbol}|{location}|{reason}"
    _notify_error_once(key, msg)
    raise FeatureBuildError(msg)


def _require_finite_float(
    symbol: str,
    location: str,
    name: str,
    value: Any,
    *,
    positive: bool = False,
    non_negative: bool = False,
) -> float:
    if isinstance(value, bool):
        _fail(symbol, location, f"{name} must be float-compatible, bool is not allowed (STRICT)")
    try:
        parsed = float(value)
    except Exception:
        _fail(symbol, location, f"{name} must be float-compatible (STRICT)")
    if not math.isfinite(parsed):
        _fail(symbol, location, f"{name} must be finite (STRICT)")
    if positive and parsed <= 0:
        _fail(symbol, location, f"{name} must be > 0 (STRICT)")
    if non_negative and parsed < 0:
        _fail(symbol, location, f"{name} must be >= 0 (STRICT)")
    return parsed


def _require_int_ms(symbol: str, location: str, name: str, value: Any) -> int:
    if isinstance(value, bool):
        _fail(symbol, location, f"{name} must be int-compatible milliseconds, bool is not allowed (STRICT)")
    try:
        parsed = int(value)
    except Exception:
        _fail(symbol, location, f"{name} must be int-compatible milliseconds (STRICT)")
    if parsed <= 0:
        _fail(symbol, location, f"{name} must be > 0 milliseconds (STRICT)")
    return parsed


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


def _resolve_min_kline_buffer(interval: str, cfg: Dict[str, int]) -> int:
    interval_floor = KLINE_MIN_BUFFER_BY_INTERVAL.get(interval)
    if interval_floor is None:
        interval_floor = DEFAULT_KLINE_MIN_BUFFER

    if interval_floor <= 0:
        raise FeatureBuildError(
            f"ws_min_kline_buffer for interval={interval!r} must be > 0 (STRICT)"
        )

    ema_fast_len = int(cfg.get("ema_fast", 20))
    ema_slow_len = int(cfg.get("ema_slow", 50))
    rsi_len = int(cfg.get("rsi", 14))
    atr_len = int(cfg.get("atr", 14))
    range_len = int(cfg.get("range", 50))
    vol_ma_len = int(cfg.get("vol_ma", 20))

    regime_need = max(
        ema_fast_len,
        ema_slow_len,
        100,
        200,
        rsi_len + 1,
        atr_len + 1,
        2 * atr_len,
        range_len,
    )

    return max(interval_floor, regime_need, vol_ma_len, 6)


def _validate_health_snapshot_contract(symbol: str, snap: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(snap, dict):
        _fail(symbol, "health_snapshot", "get_health_snapshot 결과가 dict 가 아닙니다.")

    if "overall_ok" not in snap:
        _fail(symbol, "health_snapshot", "health snapshot missing overall_ok (STRICT)")
    if not isinstance(snap["overall_ok"], bool):
        _fail(symbol, "health_snapshot", "health snapshot overall_ok must be bool (STRICT)")

    if "has_warning" not in snap:
        _fail(symbol, "health_snapshot", "health snapshot missing has_warning (STRICT)")
    if not isinstance(snap["has_warning"], bool):
        _fail(symbol, "health_snapshot", "health snapshot has_warning must be bool (STRICT)")

    warnings = snap.get("overall_warnings")
    if not isinstance(warnings, list):
        _fail(symbol, "health_snapshot", "health snapshot overall_warnings must be list (STRICT)")

    reasons = snap.get("overall_reasons")
    if not isinstance(reasons, list):
        _fail(symbol, "health_snapshot", "health snapshot overall_reasons must be list (STRICT)")

    cleaned_warnings = [str(x).strip() for x in warnings if str(x).strip()]
    return {
        "overall_ok": bool(snap["overall_ok"]),
        "has_warning": bool(snap["has_warning"]),
        "overall_warnings": cleaned_warnings,
        "overall_reasons": [str(x).strip() for x in reasons if str(x).strip()],
    }


def _validate_ws_ohlcv_buffer_strict(
    symbol: str,
    interval: str,
    rows: Any,
) -> List[Tuple[int, float, float, float, float, float]]:
    location = f"kline_{interval}"

    if not isinstance(rows, list) or not rows:
        _fail(symbol, location, f"{interval} 캔들 버퍼가 비어 있습니다. (STRICT)")

    out: List[Tuple[int, float, float, float, float, float]] = []
    prev_ts: Optional[int] = None

    for idx, row in enumerate(rows):
        if not isinstance(row, (list, tuple)) or len(row) < 6:
            _fail(symbol, location, f"{interval}[{idx}] row malformed: expected len>=6 (STRICT)")

        ts = _require_int_ms(symbol, location, f"{interval}[{idx}].ts_ms", row[0])
        open_px = _require_finite_float(symbol, location, f"{interval}[{idx}].open", row[1], positive=True)
        high_px = _require_finite_float(symbol, location, f"{interval}[{idx}].high", row[2], positive=True)
        low_px = _require_finite_float(symbol, location, f"{interval}[{idx}].low", row[3], positive=True)
        close_px = _require_finite_float(symbol, location, f"{interval}[{idx}].close", row[4], positive=True)
        volume = _require_finite_float(symbol, location, f"{interval}[{idx}].volume", row[5], non_negative=True)

        if high_px < low_px:
            _fail(symbol, location, f"{interval}[{idx}] invalid OHLC: high < low (STRICT)")
        if high_px < max(open_px, close_px):
            _fail(symbol, location, f"{interval}[{idx}] invalid OHLC: high < max(open, close) (STRICT)")
        if low_px > min(open_px, close_px):
            _fail(symbol, location, f"{interval}[{idx}] invalid OHLC: low > min(open, close) (STRICT)")

        if prev_ts is not None and ts <= prev_ts:
            _fail(
                symbol,
                location,
                f"{interval} timestamp must be strictly increasing "
                f"(prev_ts={prev_ts}, ts={ts}, index={idx}) (STRICT)",
            )
        prev_ts = ts
        out.append((ts, open_px, high_px, low_px, close_px, volume))

    return out


def _fetch_candles_strict(
    symbol: str,
    interval: str,
    *,
    cfg: Dict[str, int],
    required: bool,
) -> List[Tuple[int, float, float, float, float, float]]:
    need_len = _resolve_min_kline_buffer(interval, cfg)
    raw_buf = get_klines_with_volume(symbol, interval, limit=max(300, need_len * 2))

    if not raw_buf or len(raw_buf) < need_len:
        reason = (
            f"{interval} 캔들이 부족합니다 "
            f"(필요 {need_len}개 이상, 현재 {0 if not raw_buf else len(raw_buf)}개). "
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

    sliced = raw_buf[-max(300, need_len):]
    validated = _validate_ws_ohlcv_buffer_strict(symbol, interval, sliced)

    if len(validated) < need_len:
        reason = (
            f"{interval} STRICT 검증 후 유효 캔들이 부족합니다 "
            f"(필요 {need_len}개 이상, 현재 {len(validated)}개)."
        )
        if required:
            _fail(symbol, f"kline_{interval}", reason)
        log(f"[MKT-FEAT] optional {reason}")
        return []

    return validated


def _candles_from_ws_buf(
    buf: List[Tuple[int, float, float, float, float, float]]
) -> List[Candle]:
    return [(int(ts), float(o), float(h), float(l), float(c)) for (ts, o, h, l, c, _v) in buf]


def _last_two_finite_or_fail(symbol: str, location: str, vals: List[float], name: str) -> Tuple[float, float]:
    valid = [float(v) for v in vals if math.isfinite(float(v))]
    if len(valid) < 2:
        _fail(symbol, location, f"{name} has <2 finite values (STRICT)")
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
        f = float(ema_fast_vals[i])
        s = float(ema_slow_vals[i])
        if not math.isfinite(f) or not math.isfinite(s):
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
    window = [float(v) for v in (values[-ma_len:] if len(values) >= ma_len else values)]

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
        if not _is_finite_number(v):
            _fail(
                symbol,
                f"features_{interval}",
                f"{interval} 핵심 피처 '{key}' 가 finite number 가 아닙니다.",
            )

    adx_val = feats.get("adx")
    if not _is_finite_number(adx_val):
        _fail(
            symbol,
            f"features_{interval}",
            f"{interval} 핵심 피처 'adx' 가 finite number 가 아닙니다.",
        )


def _validate_regime_features(
    symbol: str,
    interval: str,
    feats: Dict[str, Any],
) -> None:
    regime = feats.get("regime")
    if not isinstance(regime, dict):
        _fail(symbol, f"features_{interval}", f"{interval} regime dict 가 없습니다. (STRICT)")

    required_keys = [
        "trend_strength",
        "range_strength",
        "_regime_hint",
        "_regime_label",
        "ema_200",
        "ema_200_dist_pct",
    ]
    for key in required_keys:
        value = regime.get(key)
        if not _is_finite_number(value):
            _fail(
                symbol,
                f"features_{interval}",
                f"{interval} regime 핵심 피처 '{key}' 가 finite number 가 아닙니다.",
            )


def _build_timeframe_features(
    symbol: str,
    interval: str,
    buf: List[Tuple[int, float, float, float, float, float]],
    cfg: Dict[str, int],
) -> Dict[str, Any]:
    location = f"features_{interval}"
    validated_buf = _validate_ws_ohlcv_buffer_strict(symbol, interval, buf)

    closes = [c[4] for c in validated_buf]
    highs = [c[2] for c in validated_buf]
    lows = [c[3] for c in validated_buf]
    vols = [c[5] for c in validated_buf]

    last_close = _require_finite_float(symbol, location, "last_close", closes[-1], positive=True)

    ema_fast_len = int(cfg.get("ema_fast", 20))
    ema_slow_len = int(cfg.get("ema_slow", 50))
    rsi_len = int(cfg.get("rsi", 14))
    atr_len = int(cfg.get("atr", 14))
    range_len = int(cfg.get("range", 50))
    vol_ma_len = int(cfg.get("vol_ma", 20))

    candles_for_calc: List[Candle] = _candles_from_ws_buf(validated_buf)

    raw_ohlcv_last20: List[Tuple[int, float, float, float, float, float]] = list(
        validated_buf[-20:] if len(validated_buf) >= 20 else validated_buf
    )

    ema_fast_vals = ema(closes, ema_fast_len)
    ema_slow_vals = ema(closes, ema_slow_len)
    ema_fast_val = _require_finite_float(symbol, location, "ema_fast", ema_fast_vals[-1])
    ema_slow_val = _require_finite_float(symbol, location, "ema_slow", ema_slow_vals[-1])

    ema_fast_prev, ema_fast_last = _last_two_finite_or_fail(symbol, location, ema_fast_vals, "ema_fast_vals")
    ema_fast_slope_pct = (ema_fast_last - ema_fast_prev) / last_close
    if not math.isfinite(ema_fast_slope_pct):
        _fail(symbol, location, "ema_fast_slope_pct is not finite (STRICT)")

    ema_slow_prev, ema_slow_last = _last_two_finite_or_fail(symbol, location, ema_slow_vals, "ema_slow_vals")
    ema_slow_slope_pct = (ema_slow_last - ema_slow_prev) / last_close
    if not math.isfinite(ema_slow_slope_pct):
        _fail(symbol, location, "ema_slow_slope_pct is not finite (STRICT)")

    ema_dist_pct = (ema_fast_val - ema_slow_val) / last_close
    if not math.isfinite(ema_dist_pct):
        _fail(symbol, location, "ema_dist_pct is not finite (STRICT)")

    def _ret(n_bars: int) -> float:
        if len(closes) <= n_bars:
            _fail(symbol, location, f"return_{n_bars} requires len>{n_bars} (STRICT)")
        past = float(closes[-(n_bars + 1)])
        curr = float(closes[-1])
        if past <= 0:
            _fail(symbol, location, f"return_{n_bars} past close must be > 0 (STRICT)")
        value = (curr - past) / past
        if not math.isfinite(value):
            _fail(symbol, location, f"return_{n_bars} is not finite (STRICT)")
        return value

    ret_1 = _ret(1)
    ret_3 = _ret(3)
    ret_5 = _ret(5)

    atr_val = calc_atr(candles_for_calc[-(atr_len + 1):], length=atr_len)
    atr_val = _require_finite_float(symbol, location, "atr", atr_val, positive=True)
    atr_pct = atr_val / last_close
    if not math.isfinite(atr_pct) or atr_pct <= 0:
        _fail(symbol, location, "atr_pct must be finite > 0 (STRICT)")

    window_for_range = candles_for_calc[-range_len:] if len(candles_for_calc) >= range_len else candles_for_calc
    if not window_for_range:
        _fail(symbol, location, f"{interval} range window is empty (STRICT)")
    high_recent = max(c[2] for c in window_for_range)
    low_recent = min(c[3] for c in window_for_range)
    range_pct = (high_recent - low_recent) / last_close
    if not math.isfinite(range_pct) or range_pct <= 0:
        _fail(symbol, location, "range_pct must be finite > 0 (STRICT)")

    rsi_vals = rsi(closes, rsi_len)
    rsi_last = _require_finite_float(symbol, location, "rsi", rsi_vals[-1])

    macd_line, macd_signal, macd_hist = macd(closes)
    macd_last = _require_finite_float(symbol, location, "macd", macd_line[-1])
    macd_signal_last = _require_finite_float(symbol, location, "macd_signal", macd_signal[-1])
    macd_hist_last = _require_finite_float(symbol, location, "macd_hist", macd_hist[-1])

    _, bb_upper, bb_lower = bollinger_bands(closes)
    bb_upper_last = _require_finite_float(symbol, location, "bb_upper", bb_upper[-1])
    bb_lower_last = _require_finite_float(symbol, location, "bb_lower", bb_lower[-1])
    if bb_upper_last <= bb_lower_last:
        _fail(symbol, location, "bb_upper must be > bb_lower (STRICT)")
    bb_width_pct = (bb_upper_last - bb_lower_last) / last_close
    bb_pos = (last_close - bb_lower_last) / (bb_upper_last - bb_lower_last)
    if not math.isfinite(bb_width_pct) or not math.isfinite(bb_pos):
        _fail(symbol, location, "bb_width_pct/bb_pos must be finite (STRICT)")

    stoch_k_vals, stoch_d_vals = stochastic_oscillator(highs, lows, closes)
    stoch_k_last = _require_finite_float(symbol, location, "stoch_k", stoch_k_vals[-1])
    stoch_d_last = _require_finite_float(symbol, location, "stoch_d", stoch_d_vals[-1])

    adx_val = adx(candles_for_calc, length=atr_len)
    adx_val = _require_finite_float(symbol, location, "adx", adx_val, non_negative=True)

    vol_last, vol_ma, vol_ratio, vol_z = _volume_stats(vols, vol_ma_len)
    obv_vals = obv(closes, vols)
    obv_last = _require_finite_float(symbol, location, "obv", obv_vals[-1])

    if not _is_finite_number(vol_last):
        _fail(symbol, location, "volume_last must be finite (STRICT)")
    if not _is_finite_number(vol_ma):
        _fail(symbol, location, "volume_ma must be finite (STRICT)")
    if not _is_finite_number(vol_ratio):
        _fail(symbol, location, "volume_ratio must be finite (STRICT)")
    if not _is_finite_number(vol_z):
        _fail(symbol, location, "volume_zscore must be finite (STRICT)")

    cross_type, cross_bars_ago = _detect_last_cross(ema_fast_vals, ema_slow_vals)

    if range_pct <= 0.0 and atr_val <= 0.0 and vol_ma == 0.0:
        reason = (
            f"{interval} 최근 {range_len}개 구간 동안 가격 변동과 평균 거래량이 모두 0에 가깝습니다. "
            "WS 시세가 멈췄거나 비정상일 수 있습니다."
        )
        _fail(symbol, f"flat_{interval}", reason)

    def _flag(condition: bool) -> int:
        return 1 if condition else 0

    rsi_overbought = _flag(rsi_last >= 70.0)
    rsi_oversold = _flag(rsi_last <= 30.0)
    stoch_overbought = _flag(stoch_k_last >= 80.0)
    stoch_oversold = _flag(stoch_k_last <= 20.0)

    if macd_last > macd_signal_last:
        macd_bias = 1
    elif macd_last < macd_signal_last:
        macd_bias = -1
    else:
        macd_bias = 0

    strong_trend_flag = _flag(adx_val >= 25.0)

    is_low_volatility = _flag(
        atr_pct < LOW_VOL_ATR_PCT_THRESHOLD and range_pct < LOW_VOL_RANGE_PCT_THRESHOLD
    )

    tf_features: Dict[str, Any] = {
        "interval": interval,
        "buffer_len": len(validated_buf),
        "last_close": last_close,
        "prev_close": float(closes[-2]) if len(closes) >= 2 else math.nan,
        "return_1": ret_1,
        "return_3": ret_3,
        "return_5": ret_5,
        "ema_fast": ema_fast_val,
        "ema_slow": ema_slow_val,
        "ema_fast_len": ema_fast_len,
        "ema_slow_len": ema_slow_len,
        "ema_dist_pct": ema_dist_pct,
        "ema_fast_slope_pct": ema_fast_slope_pct,
        "ema_slow_slope_pct": ema_slow_slope_pct,
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
        "volume_last": float(vol_last),
        "volume_ma": float(vol_ma),
        "volume_ratio": float(vol_ratio),
        "volume_zscore": float(vol_z),
        "obv": obv_last,
        "cross_type": cross_type,
        "cross_bars_ago": cross_bars_ago,
        "is_low_volatility": is_low_volatility,
    }

    indicators: Dict[str, Any] = {
        "ema_fast": ema_fast_val,
        "ema_slow": ema_slow_val,
        "ema_fast_slope_pct": ema_fast_slope_pct,
        "ema_slow_slope_pct": ema_slow_slope_pct,
        "rsi": rsi_last,
        "atr": atr_val,
        "atr_pct": atr_pct,
        "macd": macd_last,
        "macd_signal": macd_signal_last,
        "macd_hist": macd_hist_last,
        "adx": adx_val,
        "bb_width_pct": bb_width_pct,
        "bb_pos": bb_pos,
        "stoch_k": stoch_k_last,
        "stoch_d": stoch_d_last,
        "volume_ratio": float(vol_ratio),
        "volume_zscore": float(vol_z),
        "rsi_series": [float(v) for v in rsi_vals],
        "macd_hist_series": [float(v) for v in macd_hist],
    }
    tf_features["indicators"] = indicators

    if interval in ("1m", "5m", "15m"):
        tf_features["raw_ohlcv_last20"] = raw_ohlcv_last20

    if interval in REGIME_INTERVALS:
        regime = build_regime_features_from_candles(
            candles_for_calc,
            fast_ema_len=ema_fast_len,
            slow_ema_len=ema_slow_len,
            atr_len=atr_len,
            rsi_len=rsi_len,
            range_window=range_len,
        )
        tf_features["regime"] = _normalize_regime_keys(regime)

    tf_features["candles_raw"] = list(validated_buf)
    tf_features["candles"] = list(candles_for_calc)

    # unified_features_builder STRICT 계약 대응
    if interval in ("1h", "4h"):
        tf_features["raw_ohlcv_last60"] = [
            [ts, o, h, l, c, v]
            for (ts, o, h, l, c, v) in validated_buf[-60:]
        ]
    return tf_features


def _compute_trend_bias(tf_features: Dict[str, Any]) -> int:
    ema_fast_val = tf_features.get("ema_fast")
    ema_slow_val = tf_features.get("ema_slow")
    if not _is_finite_number(ema_fast_val) or not _is_finite_number(ema_slow_val):
        raise FeatureBuildError("trend bias requires finite ema_fast/ema_slow (STRICT)")

    f = float(ema_fast_val)
    s = float(ema_slow_val)
    if f > s:
        return 1
    if f < s:
        return -1
    return 0


def _build_multi_timeframe_summary(
    symbol: str,
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

    for iv, feats in tf_map.items():
        adx_val = feats.get("adx")
        rsi_val = feats.get("rsi")
        stoch_k_val = feats.get("stoch_k")
        is_lv = feats.get("is_low_volatility")

        if not _is_finite_number(adx_val):
            _fail(symbol, f"summary_{iv}", "adx must be finite (STRICT)")
        if not _is_finite_number(rsi_val):
            _fail(symbol, f"summary_{iv}", "rsi must be finite (STRICT)")
        if not _is_finite_number(stoch_k_val):
            _fail(symbol, f"summary_{iv}", "stoch_k must be finite (STRICT)")
        if not isinstance(is_lv, int):
            _fail(symbol, f"summary_{iv}", "is_low_volatility must be int flag (STRICT)")

        adx_num = float(adx_val)
        rsi_num = float(rsi_val)
        stoch_num = float(stoch_k_val)

        if adx_num >= 25.0:
            adx_trend_tfs += 1

        if rsi_num >= 70.0 or stoch_num >= 80.0:
            overbought_tfs += 1

        if rsi_num <= 30.0 or stoch_num <= 20.0:
            oversold_tfs += 1

        if is_lv == 1:
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


def _strict_optional_float(symbol: str, location: str, name: str, value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        parsed = float(value)
    except Exception:
        _fail(symbol, location, f"{name} must be float-compatible when present (STRICT)")
    if not math.isfinite(parsed):
        _fail(symbol, location, f"{name} must be finite when present (STRICT)")
    return parsed


def _compute_orderbook_features(symbol: str) -> Dict[str, Any]:
    ob = get_orderbook(symbol, limit=5)

    # WS 초기 구간에서는 orderbook payload가 아직 도착하지 않을 수 있음
    # STRICT 정책: 일정 시간 이후에도 payload가 없으면 실패
    if ob is None:
        ob_status = get_orderbook_buffer_status(symbol)
        recv_ts = ob_status.get("last_recv_ts")

        if recv_ts is None:
            raise FeatureBuildError(
                f"{symbol} orderbook payload not received yet (startup phase)"
            )
        else:
            _fail(
                symbol,
                "orderbook",
                "orderbook payload missing after WS start (STRICT)"
            )

    if not isinstance(ob, dict):
        _fail(symbol, "orderbook", "오더북 스냅샷 타입이 dict 가 아닙니다.")

    bids = ob.get("bids")
    asks = ob.get("asks")

    if not isinstance(bids, list) or not bids:
        _fail(
            symbol,
            "orderbook",
            "depth5 bids 스냅샷을 가져오지 못했습니다. WS 구독에 '@depth5' 가 포함되어 있는지 확인해 주세요.",
        )
    if not isinstance(asks, list) or not asks:
        _fail(
            symbol,
            "orderbook",
            "depth5 asks 스냅샷을 가져오지 못했습니다. WS 구독에 '@depth5' 가 포함되어 있는지 확인해 주세요.",
        )

    ts_ms: Optional[int] = None
    for key in ("exchTs", "ts"):
        raw_ts = ob.get(key)
        if raw_ts is None:
            continue
        ts_ms = _require_int_ms(symbol, "orderbook", key, raw_ts)
        break

    if ts_ms is None:
        _fail(
            symbol,
            "orderbook",
            "오더북 스냅샷 timestamp(exchTs/ts)가 없습니다. timestamp 누락을 현재 시각으로 대체하지 않습니다.",
        )

    now_ms = _now_ms()
    age_sec = max(0.0, (now_ms - ts_ms) / 1000.0)
    if age_sec > ORDERBOOK_MAX_DELAY_SEC:
        _fail(
            symbol,
            "orderbook",
            f"오더북 스냅샷이 {age_sec:.1f}초 동안 갱신되지 않았습니다 (허용 최대 {ORDERBOOK_MAX_DELAY_SEC:.1f}초).",
        )

    ob_status = get_orderbook_buffer_status(symbol)
    if not isinstance(ob_status, dict):
        _fail(symbol, "orderbook", "get_orderbook_buffer_status 결과가 dict 가 아닙니다. (STRICT)")

    best_bid = _require_finite_float(symbol, "orderbook", "best_bid", bids[0][0], positive=True)
    best_ask = _require_finite_float(symbol, "orderbook", "best_ask", asks[0][0], positive=True)

    if best_ask <= best_bid:
        _fail(symbol, "orderbook", "best_ask must be greater than best_bid (STRICT)")

    spread_abs = best_ask - best_bid
    mid = (best_bid + best_ask) / 2.0
    spread_pct = spread_abs / mid

    def _depth_notional(side_name: str, side: List[List[float]]) -> float:
        total = 0.0
        for idx, row in enumerate(side):
            if not isinstance(row, (list, tuple)) or len(row) < 2:
                _fail(
                    symbol,
                    "orderbook",
                    f"{side_name}[{idx}] depth row malformed (STRICT): expected [price, qty]",
                )
            p = _require_finite_float(symbol, "orderbook", f"{side_name}[{idx}].price", row[0], positive=True)
            q = _require_finite_float(symbol, "orderbook", f"{side_name}[{idx}].qty", row[1], non_negative=True)
            total += p * q
        return total

    bid_notional = _depth_notional("bids", bids)
    ask_notional = _depth_notional("asks", asks)

    if bid_notional + ask_notional <= 0:
        depth_imbalance = None
    else:
        depth_imbalance = (bid_notional - ask_notional) / (bid_notional + ask_notional)
        depth_imbalance = max(min(depth_imbalance, 1.0), -1.0)

    mark_price = _strict_optional_float(symbol, "orderbook", "markPrice", ob.get("markPrice"))
    last_price = _strict_optional_float(symbol, "orderbook", "lastPrice", ob.get("lastPrice"))

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
        "mark_price": mark_price,
        "last_price": last_price,
        "recv_delay_ms": ob_status.get("recv_delay_ms"),
        "payload_delay_ms": ob_status.get("payload_delay_ms"),
        "last_update_id": ob_status.get("last_update_id"),
    }


def _sign(value: float, *, eps: float = 0.0) -> int:
    if not math.isfinite(value):
        raise FeatureBuildError("sign input must be finite (STRICT)")
    if value > eps:
        return 1
    if value < -eps:
        return -1
    return 0


def _build_direction_alignment_meta(
    *,
    tf5: Dict[str, Any],
    tf15: Optional[Dict[str, Any]],
    tf1h: Optional[Dict[str, Any]],
    tf4h: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    b5 = _compute_trend_bias(tf5)
    b15 = _compute_trend_bias(tf15) if tf15 else 0
    b1h = _compute_trend_bias(tf1h) if tf1h else 0
    b4h = _compute_trend_bias(tf4h) if tf4h else 0

    s5 = _sign(float(tf5.get("ema_fast_slope_pct", 0.0)))
    s15 = _sign(float(tf15.get("ema_fast_slope_pct", 0.0))) if tf15 else 0
    s1h = _sign(float(tf1h.get("ema_fast_slope_pct", 0.0))) if tf1h else 0
    s4h = _sign(float(tf4h.get("ema_fast_slope_pct", 0.0))) if tf4h else 0

    return {
        "5m_bias": b5,
        "15m_bias": b15,
        "1h_bias": b1h,
        "4h_bias": b4h,
        "5m_slope_bias": s5,
        "15m_slope_bias": s15,
        "1h_slope_bias": s1h,
        "4h_slope_bias": s4h,
    }


def _score_direction_candidate(
    *,
    symbol: str,
    direction: str,
    tf5: Dict[str, Any],
    tf15: Optional[Dict[str, Any]],
    tf1h: Optional[Dict[str, Any]],
    tf4h: Optional[Dict[str, Any]],
    ob: Dict[str, Any],
    mtf: Dict[str, Any],
    direction_meta: Dict[str, Any],
) -> Dict[str, Any]:
    if direction not in ("LONG", "SHORT"):
        raise FeatureBuildError(f"direction invalid (STRICT): {direction}")

    dir_sign = 1 if direction == "LONG" else -1
    score = 0.0
    support_count = 0
    oppose_count = 0
    reasons: List[str] = []

    def _apply_bias(component_name: str, bias_value: int, support_w: float, oppose_w: float) -> None:
        nonlocal score, support_count, oppose_count
        if bias_value == dir_sign:
            score += support_w
            support_count += 1
            reasons.append(f"{component_name}:support")
        elif bias_value == -dir_sign:
            score -= oppose_w
            oppose_count += 1
            reasons.append(f"{component_name}:oppose")
        else:
            reasons.append(f"{component_name}:neutral")

    # 5m primary bias / momentum 을 최우선
    _apply_bias("bias_5m", int(direction_meta.get("5m_bias", 0)), 2.25, 2.25)
    _apply_bias("slope_5m", int(direction_meta.get("5m_slope_bias", 0)), 2.25, 2.25)

    # 중기 지원
    _apply_bias("bias_15m", int(direction_meta.get("15m_bias", 0)), 1.10, 1.10)
    _apply_bias("slope_15m", int(direction_meta.get("15m_slope_bias", 0)), 0.80, 0.80)
    _apply_bias("bias_1h", int(direction_meta.get("1h_bias", 0)), 0.95, 0.95)
    _apply_bias("slope_1h", int(direction_meta.get("1h_slope_bias", 0)), 0.65, 0.65)

    # 4h 는 telemetry 성격의 약한 bias 보정만 허용
    _apply_bias("bias_4h", int(direction_meta.get("4h_bias", 0)), 0.25, 0.25)

    depth_imbalance_raw = ob.get("depth_imbalance")
    depth_imbalance: Optional[float] = None
    if depth_imbalance_raw is not None:
        depth_imbalance = _require_finite_float(
            symbol,
            "candidate_score",
            "orderbook.depth_imbalance",
            depth_imbalance_raw,
        )
        depth_sign = _sign(depth_imbalance, eps=0.03)
        if depth_sign == dir_sign:
            score += min(abs(depth_imbalance) * 1.25, 0.90)
            reasons.append("orderbook:support")
        elif depth_sign == -dir_sign:
            score -= min(abs(depth_imbalance) * 1.25, 0.90)
            reasons.append("orderbook:oppose")
        else:
            reasons.append("orderbook:neutral")
    else:
        reasons.append("orderbook:missing")

    strong_trend_flag = tf5.get("strong_trend_flag")
    if not isinstance(strong_trend_flag, int):
        _fail(symbol, "candidate_score", "tf5.strong_trend_flag must be int flag (STRICT)")

    adx_trend_tfs_raw = mtf.get("adx_trend_tfs")
    overbought_tfs_raw = mtf.get("overbought_tfs")
    oversold_tfs_raw = mtf.get("oversold_tfs")
    try:
        adx_trend_tfs = int(adx_trend_tfs_raw or 0)
        overbought_tfs = int(overbought_tfs_raw or 0)
        oversold_tfs = int(oversold_tfs_raw or 0)
    except Exception as e:
        raise FeatureBuildError(f"candidate score counters must be int-compatible (STRICT): {e}") from e

    if strong_trend_flag == 1 and support_count >= 2:
        score += 0.55
        reasons.append("trend_strength:support")
    elif strong_trend_flag == 1 and oppose_count >= 2:
        score -= 0.55
        reasons.append("trend_strength:oppose")
    else:
        reasons.append("trend_strength:neutral")

    vol_z = _require_finite_float(
        symbol,
        "candidate_score",
        "tf5.volume_zscore",
        tf5.get("volume_zscore"),
    )
    score += min(abs(vol_z) * 0.35, 0.70)
    reasons.append("volume:activity")

    # 과매수/과매도는 range 계열의 약한 보정만 허용
    if direction == "LONG":
        if oversold_tfs > 0:
            score += min(float(oversold_tfs) * 0.25, 0.50)
            reasons.append("range_reversal:support")
        if overbought_tfs > 0:
            score -= min(float(overbought_tfs) * 0.20, 0.40)
            reasons.append("range_reversal:oppose")
    else:
        if overbought_tfs > 0:
            score += min(float(overbought_tfs) * 0.25, 0.50)
            reasons.append("range_reversal:support")
        if oversold_tfs > 0:
            score -= min(float(oversold_tfs) * 0.20, 0.40)
            reasons.append("range_reversal:oppose")

    primary_bias = int(direction_meta.get("5m_bias", 0))
    primary_slope = int(direction_meta.get("5m_slope_bias", 0))

    if strong_trend_flag == 1 and primary_bias == dir_sign and primary_slope == dir_sign and support_count >= 2:
        signal_source = "TREND"
        regime_level = 1.0
    elif (
        (direction == "LONG" and oversold_tfs > 0)
        or (direction == "SHORT" and overbought_tfs > 0)
    ):
        signal_source = "RANGE"
        regime_level = 2.0
    else:
        signal_source = "GENERIC"
        regime_level = 1.5

    score = max(0.0, min(score, 10.0))

    return {
        "direction": direction,
        "score": float(score),
        "support_count": int(support_count),
        "oppose_count": int(oppose_count),
        "signal_source": signal_source,
        "regime_level": float(regime_level),
        "primary_bias": primary_bias,
        "primary_slope_bias": primary_slope,
        "depth_imbalance": depth_imbalance,
        "reasons": list(reasons),
        "strong_trend_flag": int(strong_trend_flag),
        "adx_trend_tfs": int(adx_trend_tfs),
        "overbought_tfs": int(overbought_tfs),
        "oversold_tfs": int(oversold_tfs),
    }


def build_entry_features_ws(
    symbol: Optional[str] = None,
) -> Dict[str, Any]:
    if symbol is None:
        symbol = SET.symbol

    symbol = _normalize_symbol(symbol)
    checked_at_ms = _now_ms()

    try:
        raw_snap = get_health_snapshot(symbol)
    except FeatureBuildError:
        raise
    except Exception as e:
        _fail(symbol, "health_snapshot", f"get_health_snapshot 예외: {e}")

    health_summary = _validate_health_snapshot_contract(symbol, raw_snap)

    if not health_summary["overall_ok"]:
        _fail(
            symbol,
            "health_snapshot",
            "WS 시세 데이터 헬스 체크 결과 overall_ok=False 입니다. market_data_ws.get_health_snapshot 로그를 확인해 주세요.",
        )

    if health_summary["has_warning"] and health_summary["overall_warnings"]:
        log(
            f"[MKT-FEAT] health warning preserved: symbol={symbol} "
            f"warnings={' | '.join(health_summary['overall_warnings'])}"
        )

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
                if iv in REGIME_INTERVALS:
                    _validate_regime_features(symbol, iv, tf_features)
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
    mtf_summary = _build_multi_timeframe_summary(symbol, timeframes)

    return {
        "symbol": symbol,
        "checked_at_ms": checked_at_ms,
        "health": health_summary,
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

    buf_5m = _fetch_candles_strict(
        symbol,
        "5m",
        cfg=cfg_5m,
        required=True,
    )

    candles_5m_raw: List[List[float]] = [
        [float(ts), float(o), float(h), float(l), float(c), float(v)]
        for (ts, o, h, l, c, v) in buf_5m
    ]
    candles_5m: List[List[float]] = [
        [float(ts), float(o), float(h), float(l), float(c)]
        for (ts, o, h, l, c, _v) in buf_5m
    ]
    latest_ts = int(buf_5m[-1][0])

    ob_now = _compute_orderbook_features(symbol)

    last_price_candidates: List[float] = []
    ob_last_now = ob_now.get("last_price")
    if _is_finite_number(ob_last_now):
        last_price_candidates.append(float(ob_last_now))

    ob_mark_now = ob_now.get("mark_price")
    if _is_finite_number(ob_mark_now):
        last_price_candidates.append(float(ob_mark_now))

    tf5_close_light = buf_5m[-1][4]
    if _is_finite_number(tf5_close_light):
        last_price_candidates.append(float(tf5_close_light))

    if last_price_candidates:
        last_price = float(last_price_candidates[0])
    else:
        last_price = float(buf_5m[-1][4])

    freeze_state = _get_signal_freeze_state(symbol)
    if freeze_state is not None:
        frozen_ts = freeze_state.get("latest_ts")
        if isinstance(frozen_ts, int):
            if frozen_ts > latest_ts:
                raise FeatureBuildError(
                    f"signal freeze ts rollback detected (STRICT): frozen_ts={frozen_ts} latest_ts={latest_ts}"
                )
            if frozen_ts == latest_ts:
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

    features = build_entry_features_ws(symbol)

    tfs: Dict[str, Dict[str, Any]] = features.get("timeframes", {})
    ob: Dict[str, Any] = features.get("orderbook", {})
    mtf: Dict[str, Any] = features.get("multi_timeframe", {})

    tf5 = tfs.get("5m")
    if not isinstance(tf5, dict):
        _fail(symbol, "get_trading_signal", "5m timeframe features 가 없습니다. (STRICT)")

    tf15 = tfs.get("15m")
    tf1h = tfs.get("1h")
    tf4h = tfs.get("4h")

    range_pct_5 = _require_finite_float(symbol, "get_trading_signal", "tf5.range_pct", tf5.get("range_pct"))
    atr_pct_5 = _require_finite_float(symbol, "get_trading_signal", "tf5.atr_pct", tf5.get("atr_pct"))
    is_low_vol_5_raw = tf5.get("is_low_volatility")
    if not isinstance(is_low_vol_5_raw, int):
        _fail(symbol, "get_trading_signal", "tf5.is_low_volatility must be int flag (STRICT)")
    is_low_vol_5 = is_low_vol_5_raw == 1

    low_range_th = _resolve_runtime_positive_float(settings, "low_vol_range_pct_threshold")
    low_atr_th = _resolve_runtime_positive_float(settings, "low_vol_atr_pct_threshold")

    is_low_range = range_pct_5 < low_range_th
    is_low_atr = atr_pct_5 < low_atr_th

    low_vol_tfs_raw = mtf.get("low_vol_tfs")
    if isinstance(low_vol_tfs_raw, bool):
        _fail(symbol, "get_trading_signal", "multi_timeframe.low_vol_tfs must be int (STRICT)")
    try:
        low_vol_tfs = int(low_vol_tfs_raw or 0)
    except Exception:
        _fail(symbol, "get_trading_signal", "multi_timeframe.low_vol_tfs must be int-compatible (STRICT)")

    if (is_low_range and is_low_atr) or (is_low_vol_5 and low_vol_tfs >= 2):
        log(
            "[MKT-FEAT] get_trading_signal: 저변동성 구간 스킵 "
            f"(5m range_pct={range_pct_5:.4f}, "
            f"atr_pct={atr_pct_5:.4f}, "
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

    last_price_candidates = []

    ob_last = ob.get("last_price")
    if _is_finite_number(ob_last):
        last_price_candidates.append(float(ob_last))

    ob_mark = ob.get("mark_price")
    if _is_finite_number(ob_mark):
        last_price_candidates.append(float(ob_mark))

    tf5_close = tf5.get("last_close")
    if _is_finite_number(tf5_close):
        last_price_candidates.append(float(tf5_close))

    if last_price_candidates:
        last_price = float(last_price_candidates[0])
    else:
        last_price = float(buf_5m[-1][4])

    majority_trend = str(mtf.get("majority_trend", "NEUTRAL")).upper()
    direction_meta = _build_direction_alignment_meta(
        tf5=tf5,
        tf15=tf15,
        tf1h=tf1h,
        tf4h=tf4h,
    )

    long_candidate = _score_direction_candidate(
        symbol=symbol,
        direction="LONG",
        tf5=tf5,
        tf15=tf15,
        tf1h=tf1h,
        tf4h=tf4h,
        ob=ob,
        mtf=mtf,
        direction_meta=direction_meta,
    )
    short_candidate = _score_direction_candidate(
        symbol=symbol,
        direction="SHORT",
        tf5=tf5,
        tf15=tf15,
        tf1h=tf1h,
        tf4h=tf4h,
        ob=ob,
        mtf=mtf,
        direction_meta=direction_meta,
    )

    long_score = float(long_candidate["score"])
    short_score = float(short_candidate["score"])
    selection_margin = abs(long_score - short_score)

    min_candidate_score = 1.25
    min_selection_margin = 0.55

    if long_score < min_candidate_score and short_score < min_candidate_score:
        log(
            "[MKT-FEAT] get_trading_signal: 양방향 후보 점수가 모두 부족하여 시그널 생성을 중단합니다 "
            f"(long_score={long_score:.3f}, short_score={short_score:.3f}, "
            f"majority_trend={majority_trend})"
        )
        _set_signal_freeze_state(
            symbol=symbol,
            latest_ts=latest_ts,
            chosen_signal=None,
            signal_source=None,
            extra=None,
            blocked_reason="both_candidates_too_weak",
        )
        return None

    if selection_margin < min_selection_margin:
        log(
            "[MKT-FEAT] get_trading_signal: 양방향 후보 margin 이 부족하여 시그널 생성을 중단합니다 "
            f"(long_score={long_score:.3f}, short_score={short_score:.3f}, "
            f"selection_margin={selection_margin:.3f}, majority_trend={majority_trend})"
        )
        _set_signal_freeze_state(
            symbol=symbol,
            latest_ts=latest_ts,
            chosen_signal=None,
            signal_source=None,
            extra=None,
            blocked_reason="candidate_margin_too_small",
        )
        return None

    if long_score > short_score:
        selected = long_candidate
        chosen_signal = "LONG"
        direction_num = 1.0
    elif short_score > long_score:
        selected = short_candidate
        chosen_signal = "SHORT"
        direction_num = -1.0
    else:
        log(
            "[MKT-FEAT] get_trading_signal: 양방향 후보가 완전 동점이라 시그널 생성을 중단합니다 "
            f"(long_score={long_score:.3f}, short_score={short_score:.3f})"
        )
        _set_signal_freeze_state(
            symbol=symbol,
            latest_ts=latest_ts,
            chosen_signal=None,
            signal_source=None,
            extra=None,
            blocked_reason="candidate_tie",
        )
        return None

    signal_source = str(selected["signal_source"])
    regime_level = float(selected["regime_level"])

    atr_fast_val = _require_finite_float(symbol, "get_trading_signal", "tf5.atr", tf5.get("atr"))
    if tf15 is not None:
        atr_slow_val = _require_finite_float(symbol, "get_trading_signal", "tf15.atr", tf15.get("atr"))
    else:
        atr_slow_val = atr_fast_val

    reg5 = tf5.get("regime")
    if not isinstance(reg5, dict):
        _fail(symbol, "get_trading_signal", "tf5.regime dict 가 없습니다. (STRICT)")

    trend_strength = _require_finite_float(
        symbol,
        "get_trading_signal",
        "tf5.regime.trend_strength",
        reg5.get("trend_strength"),
    )
    volatility = _require_finite_float(symbol, "get_trading_signal", "tf5.atr_pct", tf5.get("atr_pct"))
    volume_zscore = _require_finite_float(symbol, "get_trading_signal", "tf5.volume_zscore", tf5.get("volume_zscore"))

    health_info = features.get("health")
    extra: Dict[str, Any] = {
        "signal_score": float(selected["score"]),
        "long_score": float(long_score),
        "short_score": float(short_score),
        "selection_margin": float(selection_margin),
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
        "majority_trend": majority_trend,
        "depth_imbalance": ob.get("depth_imbalance"),
        "spread_pct": ob.get("spread_pct"),
        "volume_zscore_5m": tf5.get("volume_zscore"),
        "strong_trend_flag_5m": tf5.get("strong_trend_flag"),
        "direction_source": signal_source,
        "high_tf_bias_4h": direction_meta.get("4h_bias"),
        "high_tf_bias_1h": direction_meta.get("1h_bias"),
        "high_tf_bias_15m": direction_meta.get("15m_bias"),
        "bias_5m": direction_meta.get("5m_bias"),
        "slope_bias_5m": direction_meta.get("5m_slope_bias"),
        "slope_bias_15m": direction_meta.get("15m_slope_bias"),
        "slope_bias_1h": direction_meta.get("1h_slope_bias"),
        "support_count": int(selected.get("support_count") or 0),
        "oppose_count": int(selected.get("oppose_count") or 0),
        "health_has_warning": bool(health_info.get("has_warning")) if isinstance(health_info, dict) else False,
        "health_warnings": list(health_info.get("overall_warnings") or []) if isinstance(health_info, dict) else [],
        "direction_candidates": {
            "LONG": dict(long_candidate),
            "SHORT": dict(short_candidate),
        },
    }

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