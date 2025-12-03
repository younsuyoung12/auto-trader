"""market_features_ws.py
=====================================================
BingX WebSocket 로우데이터(멀티 타임프레임 캔들 + depth5 오더북)를
GPT-5.1 트레이더용 피처로 가공하는 모듈.

2025-12-02 변경 사항 (RSI/macd 시리즈 + 패턴 엔진 연동)
----------------------------------------------------
1) _build_timeframe_features(...)에서 rsi(...) 결과 전체를 rsi_series로
   timeframes[iv]["indicators"]["rsi_series"]에 추가했다.
2) 동일하게 MACD 히스토그램 시리즈를 macd_hist_series로
   timeframes[iv]["indicators"]["macd_hist_series"]에 포함했다.
3) NaN/None/비수치 값은 필터링한 뒤 저장해
   pattern_detection.build_pattern_features(...)에서 _require_series(...)
   가 안정적으로 동작하도록 했다.
4) unified_features_builder.build_unified_features(...)
   → pattern_detection.build_pattern_features(...) 호출 시
   indicators 인자를 통해 RSI 다이버전스 패턴이 활성화되도록 했다.

2025-11-20 변경 사항 (엔트리 시그널 빌더 통합 + 저변동성 필터 + 피처 문서화/강화)
----------------------------------------------------
1) get_trading_signal(...) 추가
   - build_entry_features_ws(...) 결과 + WS 5m 캔들 버퍼를 이용해
     엔트리 후보 방향/시그널 메타/캔들 세트를 entry_flow.py 가
     바로 사용할 수 있는 형태로 제공한다.
   - 반환 포맷:
       (chosen_signal, signal_source, latest_ts,
        candles_5m, candles_5m_raw, last_price, extra)
2) EntryScore / GPT 트레이더 연동용 extra 필드 구성
   - extra["signal_score"]      : 0~3 근사 시그널 강도 점수
   - extra["atr_fast"]          : 5m ATR
   - extra["atr_slow"]          : 15m ATR (없으면 5m ATR로 대체)
   - extra["direction_raw"]     : LONG=+1, SHORT=-1
   - extra["direction_norm"]    : 위와 동일
   - extra["regime_level"]      : TREND=1.0, RANGE=2.0, GENERIC=1.5
   - extra["market_features"]   : build_entry_features_ws(...) 전체 dict
3) 시장 레이블 단순화
   - signal_source 는 로그/분석용 레이블로만 사용:
       · 강한 추세 + ADX 다수 → "TREND"
       · 과열/과매도 MTF 다수 → "RANGE"
       · 그 외 → "GENERIC"
4) 저변동성(좁은 박스장) 필터 추가
   - settings.low_vol_range_pct_threshold / low_vol_atr_pct_threshold 기준으로
     5m range_pct, atr_pct 가 모두 너무 작으면 엔트리 자체를 SKIP 처리.
5) indicators.py 에서 계산된 고급 지표/상태 플래그를 정리해
   GPT 프롬프트에서 바로 활용할 수 있도록 key 목록을 문서화.
6) 저변동성/가격 정지 구간에 대한 방어 로직 강화
   - 타임프레임별 is_low_volatility 플래그 추가.
   - 멀티 타임프레임 요약에 low_vol_tfs(저변동성 타임프레임 수) 추가.
   - 극단적으로 가격/거래량이 정지된 구간은 FeatureBuildError 로 취급.

2025-11-19 변경 사항 (2차 - 지표 확장 + GPT 피드 최적화)
----------------------------------------------------
1) indicators.py 에 새로 추가된 고급 지표들을 통합했다.
   - MACD (12/26/9), Bollinger Bands(20, 2σ), ADX(추세 강도), OBV, Stochastic(14,3)
   - 각 타임프레임별로 대표 값과 단순 상태 플래그(과열/과매도, 강한 추세 여부 등)를
     숫자 피처로 만들어 GPT 에 그대로 넘길 수 있게 했다.
2) 멀티 타임프레임 요약(_build_multi_timeframe_summary)을 보강했다.
   - EMA 기반 다수결 추세(majority_trend) 외에,
     ADX 기준 "트렌드가 강한 타임프레임 수", RSI/스토캐스틱 과열·과매도 타임프레임 수를
     한 번에 파악할 수 있는 집계 피처를 추가했다.
3) _fetch_candles_strict(...) 에서 ADX 계산에 필요한 최소 캔들 수(2 * length)를
   need_len 에 포함해, WS 버퍼가 부족하면 바로 FeatureBuildError 를 던지도록 했다.
4) 이 모듈에서는 어떤 경우에도 REST 백필을 호출하지 않으며,
   필수 WS 데이터가 부족/지연되면 FeatureBuildError + 텔레그램 알림만 발생한다.
   (데이터가 애매한 상태에서 임의 보정/추론을 하지 않는다.)
5) 1m/5m/15m 타임프레임에 대해 최근 20개 OHLCV 스냅샷을
   "raw_ohlcv_last20" 키로 함께 내보내, GPT 프롬프트에서
   쐐기, 플래그, 헤드앤숄더 등 차트 패턴을 직접 해석할 수 있도록 했다.
6) 원시 OHLCV 스냅샷 파싱 중 하나라도 예외가 발생하면
   해당 타임프레임 피처 생성을 즉시 중단하고 FeatureBuildError 를 발생시키며,
   텔레그램으로 상세 원인을 전송하도록 변경했다.

2025-11-19 변경 사항 (WS 피처 빌더 1차 도입)
----------------------------------------------------
1) build_entry_features_ws(...) 추가
   - 1m/5m/15m/1h/4h/1d 캔들과 depth5 오더북을 사용해
     추세·모멘텀·변동성·거래량·오더북 관련 핵심 지표를 계산하고
     GPT 컨텍스트로 바로 넘길 수 있는 dict 를 생성한다.
2) 데이터 헬스 검증 강화
   - 필수 타임프레임(기본 1m/5m/15m)에 대해 버퍼 개수/지연을 엄격하게 검사하고,
     하나라도 부족하거나 지연 기준(KLINE_MAX_DELAY_SEC)을 초과하면
     FeatureBuildError 를 발생시키고 텔레그램으로 상세 원인을 알린다.
   - 오더북(depth5)이 비어 있거나 ORDERBOOK_MAX_DELAY_SEC 을 넘게 지연된 경우도
     동일하게 오류로 처리한다.
3) 고전 트레이더들이 많이 참고하는 신호를 포괄하는 피처 세트 제공
   - EMA 추세(20/50 조합), 최근 수익률, RSI(14), ATR(14) 기반 변동성,
     최근 박스 폭(range_pct), 거래량 급증 여부(volume_ratio/z-score),
     골든/데드 크로스, 멀티 타임프레임 추세 정렬(trend alignment) 등을 포함한다.

역할
----------------------------------------------------
- market_data_ws 가 메모리에 보관 중인 WS 캔들/오더북을 읽어
  "지금 시장이 어떤 상태인지"를 GPT-5.1 이 한 번에 이해하기 쉽도록
  구조화된 피처 dict 로 변환한다.
- EMA/RSI/ATR/MACD/볼린저/ADX/OBV/스토캐스틱 등
  유명 트레이더들이 자주 사용하는 지표를 멀티 타임프레임으로 정리하고,
  1m/5m/15m 에 대해서는 최근 20개 OHLCV 원시 캔들 스냅샷도 함께 제공해
  LLM 이 차트 패턴까지 직접 해석할 수 있게 돕는다.
- 이 모듈은 **계산/검증 전용 레이어**로, 매매 판단은 gpt_decider/gpt_trader 쪽에 맡긴다.
- 데이터가 불완전한 상태에서 잘못된 판단을 내리지 않도록,
  필수 데이터가 비정상일 때는 백필/추론 없이 바로 예외를 던지고
  텔레그램 알림으로 사람에게 원인을 명확히 전달하는 것을 최우선으로 한다.


Key Overview
----------------------------------------------------
[표 1] build_entry_features_ws(...) 최상위 키

- symbol: str
- checked_at_ms: int (ms 단위 헬스 체크 시각)
- timeframes: Dict[str, Dict[str, Any]]  # "1m" / "5m" / "15m" / "1h" / "4h" / "1d"
- orderbook: Dict[str, Any]
- multi_timeframe: Dict[str, Any]

[표 2] timeframes[iv] 공통 키 (단일 타임프레임 피처)

- interval: str ("1m"/"5m"/...)
- buffer_len: int (WS 버퍼에 사용한 캔들 개수)
- last_close: float
- prev_close: float
- return_1 / return_3 / return_5: float (최근 1/3/5봉 수익률)
- ema_fast / ema_slow: float
- ema_fast_len / ema_slow_len: int
- ema_dist_pct: float (fast-slow 간격 / 가격)
- ema_fast_slope_pct: float (fast EMA 기울기 / 가격)
- atr: float
- atr_pct: float (ATR / 가격)
- range_pct: float (최근 range_window 고가-저가 / 가격)
- rsi / rsi_len: float/int
- rsi_overbought / rsi_oversold: int (0/1 플래그)
- macd / macd_signal / macd_hist: float
- macd_bias: int (-1/0/1, MACD 방향)
- bb_width_pct / bb_pos: float (볼린저 폭/위치)
- stoch_k / stoch_d: float
- stoch_overbought / stoch_oversold: int
- adx: float | None
- strong_trend_flag: int (ADX 25 이상이면 1)
- volume_last / volume_ma / volume_ratio / volume_zscore: float
- obv: float
- cross_type: str ("GOLDEN"/"DEAD"/"NONE")
- cross_bars_ago: Optional[int]
- is_low_volatility: int (0/1, 저변동성 플래그)
- raw_ohlcv_last20: List[Dict[str, float]]  # interval 이 1m/5m/15m 인 경우에만 존재
- regime: Dict[str, Any]  # interval 이 5m/15m/1h 인 경우에만 존재

[표 3] orderbook 피처 키

- ts_ms: int (오더북 기준 시각)
- age_sec: float (스냅샷 지연 시간)
- best_bid / best_ask / mid_price: float
- spread_abs / spread_pct: float
- bid_notional / ask_notional: float
- depth_imbalance: float in [-1, 1] or None
- mark_price / last_price: float | None

[표 4] multi_timeframe 요약 키

- trend_votes: Dict[str, int]  # 각 TF 의 LONG(+1)/SHORT(-1)/중립(0) 편향
- long_votes / short_votes: int
- trend_align_long / trend_align_short: bool
- majority_trend: str ("LONG"/"SHORT"/"NEUTRAL"/"MIXED")
- adx_trend_tfs: int  # ADX >= 25 인 타임프레임 개수
- overbought_tfs / oversold_tfs: int  # 과열/과매도 TF 수
- low_vol_tfs: int  # is_low_volatility == 1 인 타임프레임 수

[표 5] get_trading_signal(...) extra 키

- signal_score: float (0~3 근사 시그널 강도)
- atr_fast / atr_slow: float
- direction_raw / direction_norm: float (+1 LONG / -1 SHORT)
- regime_level: float (TREND=1.0 / RANGE=2.0 / GENERIC=1.5)
- market_features: Dict[str, Any]  # build_entry_features_ws(...) 전체 반환값
- last_close_ts: float (최근 청산 시각)
- majority_trend: str
- depth_imbalance: float | None
- spread_pct: float | None
- volume_zscore_5m: float | None
- strong_trend_flag_5m: int | None
"""

import math
import time
from typing import Any, Dict, List, Tuple, Optional

from settings_ws import load_settings
from telelog import log, send_tg
from market_data_ws import (
    get_klines_with_volume,
    get_orderbook,
    get_last_kline_delay_ms,
    get_health_snapshot,
    KLINE_MIN_BUFFER,
    KLINE_MAX_DELAY_SEC,
    ORDERBOOK_MAX_DELAY_SEC,
)
from indicators import (
    ema,
    rsi,
    calc_atr,
    macd,
    bollinger_bands,
    obv,
    adx,
    stochastic_oscillator,
    Candle,
    build_regime_features_from_candles,
)

SET = load_settings()

# 반환 타입 alias: get_trading_signal 이 돌려주는 튜플 구조
EntrySignal = Tuple[
    str,                 # chosen_signal ("LONG"|"SHORT")
    str,                 # signal_source ("TREND"/"RANGE"/"GENERIC"...)
    int,                 # latest_ts (ms)
    List[List[float]],   # candles_5m (ts, o, h, l, c)
    List[List[float]],   # candles_5m_raw (ts, o, h, l, c, v)
    float,               # last_price
    Dict[str, Any],      # extra (GPT/EntryScore용 메타)
]

# 필수/옵션 타임프레임
# - REQUIRED_TFS: 이 목록의 타임프레임은 부족/지연 시 곧바로 오류로 본다.
# - EXTRA_TFS: 있으면 피처에 포함하지만, 없어도 오류로 보지 않는다.
REQUIRED_TFS: List[str] = list(
    getattr(SET, "features_required_tfs", ["1m", "5m", "15m"])
)
EXTRA_TFS: List[str] = ["1h", "4h", "1d"]

# 각 타임프레임별 기본 파라미터 (EMA 길이, RSI/ATR 길이 등)
TF_CONFIG: Dict[str, Dict[str, int]] = {
    "1m":  {"ema_fast": 9,  "ema_slow": 21, "rsi": 14, "atr": 14, "range": 50, "vol_ma": 20},
    "5m":  {"ema_fast": 20, "ema_slow": 50, "rsi": 14, "atr": 14, "range": 50, "vol_ma": 20},
    "15m": {"ema_fast": 20, "ema_slow": 50, "rsi": 14, "atr": 14, "range": 50, "vol_ma": 20},
    "1h":  {"ema_fast": 20, "ema_slow": 50, "rsi": 14, "atr": 14, "range": 50, "vol_ma": 20},
    "4h":  {"ema_fast": 20, "ema_slow": 50, "rsi": 14, "atr": 14, "range": 50, "vol_ma": 20},
    "1d":  {"ema_fast": 20, "ema_slow": 50, "rsi": 14, "atr": 14, "range": 50, "vol_ma": 20},
}

# 텔레그램 에러 알림 쿨다운 (초)
FEATURE_ERROR_TG_COOLDOWN_SEC: float = float(
    getattr(SET, "features_error_tg_cooldown_sec", 60.0)
)


class FeatureBuildError(RuntimeError):
    """필수 시세 데이터가 부족/지연된 경우 사용하는 예외."""


# 최근 에러 알림 전송 시각 (같은 원인으로 텔레폭주 방지)
_LAST_ERROR_SENT: Dict[str, float] = {}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _notify_error_once(key: str, human_msg: str) -> None:
    """같은 유형의 에러에 대해 텔레그램 알림을 일정 시간 간격으로만 보낸다."""
    now = time.time()
    last = _LAST_ERROR_SENT.get(key)
    if last is not None and (now - last) < FEATURE_ERROR_TG_COOLDOWN_SEC:
        # 로그에는 남기되 텔레그램은 생략
        log(f"[MKT-FEAT] (suppressed) {human_msg}")
        return

    _LAST_ERROR_SENT[key] = now
    log(f"[MKT-FEAT] {human_msg}")
    try:
        send_tg("❌ [시세 데이터 오류 - GPT 피처 빌더]\n" + human_msg)
    except Exception:
        # 텔레그램 자체 오류는 여기서만 먹고 넘어간다.
        pass


def _fail(symbol: str, location: str, reason: str) -> None:
    """에러 메시지를 텔레그램/로그에 남기고 FeatureBuildError 로 올린다."""
    msg = f"[{symbol}] {location}: {reason}"
    key = f"{symbol}|{location}|{reason}"
    _notify_error_once(key, msg)
    raise FeatureBuildError(msg)


def _fetch_candles_strict(
    symbol: str,
    interval: str,
    *,
    cfg: Dict[str, int],
    required: bool,
) -> List[Tuple[int, float, float, float, float, float]]:
    """WS 버퍼에서 캔들을 가져오되, 필수 타임프레임이면 엄격하게 검사한다.

    - required=True 인 경우:
      · 버퍼 길이 < need_len 이면 즉시 FeatureBuildError
      · get_last_kline_delay_ms(...) > KLINE_MAX_DELAY_SEC 이면 FeatureBuildError
    - required=False 인 경우:
      · 부족/지연이면 빈 리스트를 반환하고, 피처 계산에서 건너뛴다.
    """
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
        atr_len * 2,  # ADX 계산을 위한 최소 캔들 수(대략 2 * length)
    )
    buf = get_klines_with_volume(symbol, interval, limit=need_len * 2)

    if not buf or len(buf) < need_len:
        reason = (
            f"{interval} 캔들이 부족합니다 "
            f"(필요 {need_len}개 이상, 현재 {0 if not buf else len(buf)}개). "
            "ws_subscribe_tfs / WS 백필 설정을 확인해 주세요."
        )
        if required:
            _fail(symbol, f"kline_{interval}", reason)
        else:
            log(f"[MKT-FEAT] optional {reason}")
            return []

    delay_ms = get_last_kline_delay_ms(symbol, interval)
    if delay_ms is None:
        reason = f"{interval} 마지막 캔들 수신 시각 정보를 가져오지 못했습니다."
        if required:
            _fail(symbol, f"kline_{interval}", reason)
        else:
            log(f"[MKT-FEAT] optional {reason}")
            return []

    # KLINE_MAX_DELAY_SEC 초 이상 지연되면 비정상으로 간주
    if delay_ms > KLINE_MAX_DELAY_SEC * 1000.0:
        reason = (
            f"{interval} 캔들이 {delay_ms/1000.0:.1f}초 동안 갱신되지 않았습니다 "
            f"(허용 최대 {KLINE_MAX_DELAY_SEC:.1f}초)."
        )
        if required:
            _fail(symbol, f"kline_{interval}", reason)
        else:
            log(f"[MKT-FEAT] optional {reason}")
            return []

    return list(buf[-need_len:])


def _candles_from_ws_buf(
    buf: List[Tuple[int, float, float, float, float, float]]
) -> List[Candle]:
    """WS 버퍼 포맷(ts, o, h, l, c, v) → indicators.Candle(ts, o, h, l, c)."""
    return [(int(ts), float(o), float(h), float(l), float(c)) for (ts, o, h, l, c, _v) in buf]


def _last_two_valid(vals: List[float]) -> Tuple[Optional[float], Optional[float]]:
    """NaN 을 제외한 값 중 마지막 두 개를 반환."""
    valid = [v for v in vals if not math.isnan(v)]
    if len(valid) < 2:
        return None, None
    return valid[-2], valid[-1]


def _detect_last_cross(
    ema_fast_vals: List[float],
    ema_slow_vals: List[float],
) -> Tuple[str, Optional[int]]:
    """골든/데드 크로스를 단순 탐지한다.

    반환:
      ("GOLDEN"|"DEAD"|"NONE", bars_ago 또는 None)
    """
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
    """거래량 통계 계산 (마지막 값, 이동평균, 비율, z-score).

    반환:
      (last, ma, ratio, zscore)
    """
    if not values:
        return math.nan, math.nan, math.nan, math.nan

    last = float(values[-1])
    n = len(values)

    if n < ma_len:
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
    """TA-Lib indicators.py 정합 체크용: 핵심 키들을 보정/보완한다.

    - atr_pct / range_pct / macd_hist / rsi_last 필드가 누락되어 있으면
      가능한 값에서 alias 를 만들어 준다.
    - 현재 indicators.py 는 이미 위 키들을 생성하지만,
      과거 버전과의 호환을 위한 안전장치로 둔다.
    """
    normalized = dict(regime)

    # rsi_last 가 없고 rsi 만 있다면 alias 생성
    if "rsi_last" not in normalized and "rsi" in normalized:
        v = normalized.get("rsi")
        if isinstance(v, (int, float)):
            normalized["rsi_last"] = float(v)

    # macd_hist 가 없고 macd_hist_last 가 있는 경우 alias
    if "macd_hist" not in normalized and "macd_hist_last" in normalized:
        v = normalized.get("macd_hist_last")
        if isinstance(v, (int, float)):
            normalized["macd_hist"] = float(v)

    # atr_pct 가 없고 atr / last_close 로 계산 가능한 경우 보완
    if "atr_pct" not in normalized:
        atr_val = normalized.get("atr")
        last_close = normalized.get("last_close")
        if (
            isinstance(atr_val, (int, float))
            and isinstance(last_close, (int, float))
            and last_close > 0
        ):
            normalized["atr_pct"] = float(atr_val) / float(last_close)

    # range_pct 가 없고 high_recent/low_recent 로 계산 가능한 경우 보완
    if "range_pct" not in normalized:
        high_recent = normalized.get("high_recent")
        low_recent = normalized.get("low_recent")
        last_close = normalized.get("last_close")
        if (
            isinstance(high_recent, (int, float))
            and isinstance(low_recent, (int, float))
            and isinstance(last_close, (int, float))
            and last_close > 0
        ):
            normalized["range_pct"] = (float(high_recent) - float(low_recent)) / float(
                last_close
            )

    return normalized


def _build_timeframe_features(
    symbol: str,
    interval: str,
    buf: List[Tuple[int, float, float, float, float, float]],
    cfg: Dict[str, int],
) -> Dict[str, Any]:
    """단일 타임프레임에 대한 피처 묶음을 생성한다.

    유명 트레이더들이 자주 참고하는 지표들을 최대한 모아서
    GPT 가 한 번에 읽기 좋은 형태로 정리한다.
    """
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

    # GPT 프롬프트용 원시 OHLCV 스냅샷(최대 20개)
    # - 1m/5m/15m 에 대해서만 raw_ohlcv_last20 키로 내보낸다.
    raw_ohlcv_last20: List[Dict[str, float]] = []
    raw_slice = buf[-20:] if len(buf) >= 20 else buf
    for ts, o, h, l, c, v in raw_slice:
        try:
            raw_ohlcv_last20.append(
                {
                    "ts_ms": int(ts),
                    "open": float(o),
                    "high": float(h),
                    "low": float(l),
                    "close": float(c),
                    "volume": float(v),
                }
            )
        except Exception as e:
            reason = (
                f"{interval} raw OHLCV 스냅샷 파싱 중 예외가 발생했습니다: {e}. "
                "WS 원시 캔들 데이터 형식을 확인해 주세요."
            )
            _fail(symbol, f"raw_ohlcv_{interval}", reason)

    # EMA(추세), 최근 기울기
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

    # 수익률 (최근 1, 3, 5 봉 기준)
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

    # ATR 및 박스 폭
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

    # RSI
    rsi_vals = rsi(closes, rsi_len)
    rsi_last = rsi_vals[-1] if rsi_vals else math.nan

    # MACD / 시그널 / 히스토그램
    macd_line, macd_signal, macd_hist = macd(closes)
    macd_last = macd_line[-1] if macd_line else math.nan
    macd_signal_last = macd_signal[-1] if macd_signal else math.nan
    macd_hist_last = macd_hist[-1] if macd_hist else math.nan

    # 볼린저 밴드 (폭/위치)
    bb_mid, bb_upper, bb_lower = bollinger_bands(closes)
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

    # 스토캐스틱
    stoch_k_vals, stoch_d_vals = stochastic_oscillator(highs, lows, closes)
    stoch_k_last = stoch_k_vals[-1] if stoch_k_vals else math.nan
    stoch_d_last = stoch_d_vals[-1] if stoch_d_vals else math.nan

    # ADX (추세 강도)
    adx_val = adx(candles_for_calc, length=atr_len)

    # 거래량 통계 및 OBV
    vol_last, vol_ma, vol_ratio, vol_z = _volume_stats(vols, vol_ma_len)
    obv_vals = obv(closes, vols)
    obv_last = obv_vals[-1] if obv_vals else math.nan

    # 골든/데드 크로스
    cross_type, cross_bars_ago = _detect_last_cross(ema_fast_vals, ema_slow_vals)

    # 극단적인 가격/거래량 정지 구간 방어
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

    # 단순 상태 플래그 (GPT 가 해석하기 쉬운 0/1/-1 값)
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

    # 저변동성 플래그 (타임프레임 단위)
    is_low_volatility = 0
    try:
        if (
            isinstance(atr_pct, (int, float))
            and not math.isnan(atr_pct)
            and isinstance(range_pct, (int, float))
            and not math.isnan(range_pct)
        ):
            # 이 값들은 get_trading_signal 의 기본 threshold 와 동일하게 맞춘다.
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
        "volume_ratio": vol_ratio,     # 1.0 이상이면 최근 거래량이 평균보다 큼
        "volume_zscore": vol_z,        # 2 이상이면 통계적으로 꽤 큰 스파이크
        "obv": obv_last,
        "cross_type": cross_type,      # "GOLDEN" / "DEAD" / "NONE"
        "cross_bars_ago": cross_bars_ago,
        "is_low_volatility": is_low_volatility,
    }

    # 패턴 엔진용 지표 시리즈 (RSI / MACD 히스토그램 등)
    indicators: Dict[str, Any] = {}
    try:
        if rsi_vals:
            rsi_clean = [
                float(v)
                for v in rsi_vals
                if isinstance(v, (int, float)) and not math.isnan(float(v))
            ]
            if rsi_clean:
                indicators["rsi_series"] = rsi_clean
    except Exception as e:
        log(f"[MKT-FEAT] rsi_series 빌드 중 예외 interval={interval}: {e}")
    try:
        if macd_hist:
            macd_hist_clean = [
                float(v)
                for v in macd_hist
                if isinstance(v, (int, float)) and not math.isnan(float(v))
            ]
            if macd_hist_clean:
                indicators["macd_hist_series"] = macd_hist_clean
    except Exception as e:
        log(f"[MKT-FEAT] macd_hist_series 빌드 중 예외 interval={interval}: {e}")

    if indicators:
        tf_features["indicators"] = indicators

    # GPT 프롬프트에서 직접 차트 패턴을 해석할 수 있게,
    # 1m/5m/15m 에 대해서만 최근 20개 원시 OHLCV 를 함께 실어 보낸다.
    if interval in ("1m", "5m", "15m"):
        tf_features["raw_ohlcv_last20"] = raw_ohlcv_last20

    # 5m / 15m / 1h 등의 타임프레임는 regime 피처도 같이 계산
    if interval in ("5m", "15m", "1h"):
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
                # TA-Lib indicators.py 에서 생성된 regime 키 정합을 한 번 더 보정
                tf_features["regime"] = _normalize_regime_keys(regime)
        except Exception as e:
            log(f"[MKT-FEAT] regime feature 계산 중 예외 interval={interval}: {e}")

    return tf_features


def _compute_trend_bias(tf_features: Dict[str, Any]) -> int:
    """ema_fast / ema_slow 를 기준으로 LONG(1)/SHORT(-1)/중립(0) 편향을 계산."""
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
    """여러 타임프레임 피처를 종합해 간단한 멀티 타임프레임 요약을 만든다."""
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

    # ADX / RSI / Stoch 기반 멀티 타임프레임 요약
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

        # 저변동성 타임프레임 개수 집계
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
        "adx_trend_tfs": adx_trend_tfs,       # ADX 25 이상인 타임프레임 수
        "overbought_tfs": overbought_tfs,     # RSI/스토캐스틱 과열 타임프레임 수
        "oversold_tfs": oversold_tfs,         # RSI/스토캐스틱 과매도 타임프레임 수
        "low_vol_tfs": low_vol_tfs,           # 저변동성 타임프레임 수
    }


def _compute_orderbook_features(symbol: str) -> Dict[str, Any]:
    """depth5 오더북에서 스프레드/비대칭성 등을 계산한다."""
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

    # exchTs(거래소 타임스탬프) 우선, 없으면 ts(로컬) 사용
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

    # 단순 depth 비대칭성: (bid_notional - ask_notional) / (bid_notional + ask_notional)
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
    """WS 로우데이터를 기반으로 GPT-5.1 진입/관리용 피처를 생성한다.

    사용 예시:
        from market_features_ws import build_entry_features_ws

        features = build_entry_features_ws(SET.symbol)
        # gpt_decider.ask_entry_decision_safe(..., market_features=features)

    동작 개요:
        1) market_data_ws.get_health_snapshot(...) 으로 전체 헬스를 한 번 체크.
           - overall_ok=False 이면 상세 사유와 함께 FeatureBuildError.
        2) REQUIRED_TFS (기본 1m/5m/15m)에 대해
           - 최소 버퍼 개수(KLINE_MIN_BUFFER 이상) & 지연(KLINE_MAX_DELAY_SEC 이하) 검증.
        3) EXTRA_TFS (1h/4h/1d)는 있으면 피처를 만들고, 없으면 건너뜀.
        4) depth5 오더북이 비어 있거나 ORDERBOOK_MAX_DELAY_SEC 을 넘게 지연되면 오류.
        5) 위 검증이 모두 통과된 경우에만 timeframes/orderbook/multi_timeframe 으로 구성된 dict 를 반환.

    주의:
        - 이 모듈에서는 REST 백필을 절대로 호출하지 않는다.
        - WS 로우데이터가 비정상일 때는 FeatureBuildError 를 던지고,
          원인은 텔레그램/Render 로그에서 바로 확인할 수 있다.
    """
    if symbol is None:
        symbol = SET.symbol

    checked_at_ms = _now_ms()

    # 1) 전체 헬스 스냅샷 (요약)
    try:
        snap = get_health_snapshot(symbol)
        if not snap.get("overall_ok", True):
            # 세부 사유는 snap["klines"]/ ["orderbook"] 에 이미 담겨 있으므로
            # 여기서는 간략히 상태만 알린다.
            _fail(
                symbol,
                "health_snapshot",
                "WS 시세 데이터 헬스 체크 결과 overall_ok=False 입니다. market_data_ws.get_health_snapshot 로그를 확인해 주세요.",
            )
    except FeatureBuildError:
        # 이미 _fail 에서 처리됨
        raise
    except Exception as e:
        # 헬스 스냅샷을 못 가져왔다고 해서 바로 죽이지는 않고,
        # 아래 개별 타임프레임 검증에서 한 번 더 체크한다.
        log(f"[MKT-FEAT] get_health_snapshot error: {e}")

    timeframes: Dict[str, Dict[str, Any]] = {}

    # 2) 필수/옵션 타임프레임별 피처 계산
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
            # optional 이고 실패한 경우
            continue

        try:
            tf_features = _build_timeframe_features(symbol, iv, buf, cfg)
            timeframes[iv] = tf_features
        except FeatureBuildError:
            raise
        except Exception as e:
            # 피처 계산 중 예외 → 필수 타임프레임이면 오류로 올리기
            reason = f"{iv} 피처 계산 중 예외가 발생했습니다: {e}"
            if is_required:
                _fail(symbol, f"features_{iv}", reason)
            else:
                log(f"[MKT-FEAT] optional {reason}")

    if not timeframes:
        _fail(symbol, "features", "어떤 타임프레임에서도 피처를 생성하지 못했습니다.")

    # 3) 오더북 피처
    ob_features = _compute_orderbook_features(symbol)

    # 4) 멀티 타임프레임 요약
    mtf_summary = _build_multi_timeframe_summary(timeframes)

    return {
        "symbol": symbol,
        "checked_at_ms": checked_at_ms,
        "timeframes": timeframes,
        "orderbook": ob_features,
        "multi_timeframe": mtf_summary,
    }


# ─────────────────────────────────────────────────────
# 엔트리 시그널 빌더: entry_flow.try_open_new_position(...) 에서 사용
# ─────────────────────────────────────────────────────


def get_trading_signal(
    *,
    settings: Any,
    last_close_ts: float,
    symbol: Optional[str] = None,
) -> Optional[EntrySignal]:
    """WS 기반 엔트리 시그널/컨텍스트를 생성한다.

    반환 형식:
        (chosen_signal, signal_source, latest_ts,
         candles_5m, candles_5m_raw, last_price, extra)

    - chosen_signal : "LONG" / "SHORT"
    - signal_source : 로그/DB용 전략 라벨 ("TREND", "RANGE", "GENERIC" 등)
    - latest_ts     : 기준 5m 캔들 타임스탬프(ms)
    - candles_5m    : [[ts, o, h, l, c], ...]
    - candles_5m_raw: [[ts, o, h, l, c, v], ...]
    - last_price    : 현재 기준가(오더북 last/mark/5m 종가 순으로 선택)
    - extra         :
        · signal_score  : 0~3 근사 시그널 강도
        · atr_fast      : 5m ATR
        · atr_slow      : 15m ATR
        · direction_raw : LONG=+1, SHORT=-1
        · direction_norm: 위와 동일
        · regime_level  : TREND=1.0, RANGE=2.0, GENERIC=1.5
        · market_features: build_entry_features_ws(...) 전체 dict
        · last_close_ts : 최근 청산 시각(단일 전략 기준)
    """
    # 심볼 결정: 우선 settings.symbol, 없으면 글로벌 SET.symbol
    if symbol is None:
        symbol = getattr(settings, "symbol", None) or SET.symbol

    # 1) WS 피처 빌더 실행 (필수 데이터/헬스 체크는 여기서 끝낸다)
    try:
        features = build_entry_features_ws(symbol)
    except FeatureBuildError as e:
        log(f"[MKT-FEAT] get_trading_signal FeatureBuildError: {e}")
        # 여기서 텔레그램 알림은 이미 _fail 에서 보냈으므로 추가 전송은 하지 않는다.
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
        return None

    tf15 = tfs.get("15m")
    tf1 = tfs.get("1m")

    # 1.5) 저변동성 필터: 5m range/ATR 모두 너무 작으면 엔트리 스킵
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

        # 멀티 타임프레임에서 저변동성 타임프레임 수
        low_vol_tfs = int(mtf.get("low_vol_tfs") or 0)

        if (is_low_range and is_low_atr) or (is_low_vol_5 and low_vol_tfs >= 2):
            # TA-Lib 기반 range_pct / atr_pct 가 정상 계산되었는지 로그로도 함께 체크
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
            
    except Exception as e:
        log(f"[MKT-FEAT] low-volatility filter 계산 중 예외 발생: {e}")

    # 2) 5m 캔들 버퍼 확보 (가드/스냅샷용) - WS 버퍼 그대로 사용
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

    # 3) 기준 가격(last_price) 선택
    last_price_candidates: List[float] = []

    ob_last = ob.get("last_price")
    if isinstance(ob_last, (int, float)):
        last_price_candidates.append(float(ob_last))

    ob_mark = ob.get("mark_price")
    if isinstance(ob_mark, (int, float)):
        last_price_candidates.append(float(ob_mark))

    tf5_close = tf5.get("last_close")
    if isinstance(tf5_close, (int, float)):
        last_price_candidates.append(float(tf5_close))

    if tf1:
        tf1_close = tf1.get("last_close")
        if isinstance(tf1_close, (int, float)):
            last_price_candidates.append(float(tf1_close))

    if last_price_candidates:
        last_price = last_price_candidates[0]
    else:
        # 최후 보루: 5m 마지막 종가
        last_price = float(buf_5m[-1][4])

    # 4) 방향 편향(chosen_signal) 계산
    majority_trend = str(mtf.get("majority_trend", "NEUTRAL")).upper()
    depth_imbalance = ob.get("depth_imbalance")

    trend_bias = 0  # LONG=+1 / SHORT=-1 / 0=중립

    if majority_trend == "LONG":
        trend_bias = 1
    elif majority_trend == "SHORT":
        trend_bias = -1
    else:
         # 오더북 쏠림이 강할 때만 방향 반영 (과도한 방향 흔들림 방지)
        if isinstance(depth_imbalance, (int, float)) and abs(depth_imbalance) >= 0.20:
            trend_bias = 1 if depth_imbalance > 0 else -1

    # 여전히 0이면 5m EMA 정렬로 최소 방향은 정해준다.
    if trend_bias == 0:
        ema_fast_5 = tf5.get("ema_fast")
        ema_slow_5 = tf5.get("ema_slow")
        if isinstance(ema_fast_5, (int, float)) and isinstance(ema_slow_5, (int, float)):
            if ema_fast_5 > ema_slow_5:
                trend_bias = 1
            elif ema_fast_5 < ema_slow_5:
                trend_bias = -1

    # 완전히 애매한 경우라도 GPT 가 최종 판단하므로 한쪽을 기본값으로 둔다.
    if trend_bias >= 0:
        chosen_signal = "LONG"
        direction_num = 1.0
    else:
        chosen_signal = "SHORT"
        direction_num = -1.0

    # 5) signal_source (레짐 라벨) 결정 - 완전 단순화
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
        # 어떤 이유로든 계산 실패하면 GENERIC 유지
        pass

    # regime_level 숫자 라벨 (EntryScore/GPT 메타용)
    if signal_source == "TREND":
        regime_level = 1.0
    elif signal_source == "RANGE":
        regime_level = 2.0
    else:
        regime_level = 1.5

    # 6) signal_score / ATR fast/slow 계산 (EntryScore용 필수 필드)
    atr_fast = tf5.get("atr")
    if isinstance(atr_fast, (int, float)):
        atr_fast_val = float(atr_fast)
    else:
        atr_fast_val = float("nan")

    if tf15:
        atr_slow = tf15.get("atr")
    else:
        atr_slow = atr_fast

    if isinstance(atr_slow, (int, float)):
        atr_slow_val = float(atr_slow)
    else:
        atr_slow_val = float("nan")

    # ▶ 확장된 시그널 강도 점수 (0.5 ~ 6.0)
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

    # 최종 범위 확장
    signal_score = max(0.5, min(signal_score, 6.0))

    # 7) extra 메타 구성 (GPT/gpt_trader + EntryScore 공용)
    extra: Dict[str, Any] = {
        "signal_score": float(signal_score),
        "atr_fast": atr_fast_val,
        "atr_slow": atr_slow_val,
        "direction_raw": direction_num,
        "direction_norm": direction_num,
        "regime_level": regime_level,
        "market_features": features,
        "last_close_ts": float(last_close_ts),
    }

    # 참고용으로 일부 핵심 값도 함께 넣어 준다.
    try:
        extra["majority_trend"] = majority_trend
        extra["depth_imbalance"] = depth_imbalance
        extra["spread_pct"] = ob.get("spread_pct")
        extra["volume_zscore_5m"] = tf5.get("volume_zscore")
        extra["strong_trend_flag_5m"] = tf5.get("strong_trend_flag")
    except Exception:
        pass

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
