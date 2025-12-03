from __future__ import annotations

"""
unified_features_builder.py
====================================================
BingX Auto Trader - WS 마켓 피처 + 차트 패턴 피처를
GPT-5.1 이 바로 사용할 수 있는 하나의 dict 로 통합하는 모듈.

2025-11-21 Ultra Version (pattern_detection.py + market_features_ws.py 통합)
----------------------------------------------------
1) market_features_ws.build_entry_features_ws(...) 에서 만든
   멀티 타임프레임 피처(timeframes/orderbook/multi_timeframe) 위에
   pattern_detection.build_pattern_features(...) 결과를 얹어,
   gpt_decider/gpt_trader 가 그대로 사용할 수 있는 구조를 제공한다.
2) 1m/5m/15m raw_ohlcv_last20 을 기준으로 엔골핑/핀바/플래그/삼각수렴/
   헤드앤숄더/브레이크아웃/유동성 스윕 등 패턴을 정량화한다.
3) 필수 데이터가 하나라도 부족/손상되면 **백필 없이 즉시 실패**시키고,
   Render 로그 + 텔레그램 알림으로 원인을 남긴다.
4) unified_features 토큰 최적화 (8k~10k → 약 3k~4k 목표)
   - pattern_features 에서는 타임프레임별 핵심 스코어만 유지한다.
   - 상세 패턴 리스트는 pattern_summary.patterns 에서
     "강도(strength)가 높은 상위 N개"만 compact 형태로 제공한다.
   - pattern_detection 이 만들어내는 raw 패턴/시리즈는 GPT 프롬프트에
     그대로 실리지 않도록 설계해 토큰 비용을 절감한다.

역할
----------------------------------------------------
- run_bot_ws → unified_features_builder.build_unified_features(...) → gpt_decider
  흐름에서 "지금 시장 상태 + 차트 패턴 상태"를 한 번에 만들어 주는 브레인 전처리 레이어.
- 이 모듈은 **계산/검증 전용**이며, 실제 주문 실행은 trader.py / gpt_trader.py 가 담당한다.

연결 구조
----------------------------------------------------
- market_features_ws.build_entry_features_ws(symbol)
    → 기본 마켓 피처(EMA/RSI/ATR/볼륨/오더북/멀티 타임프레임 요약 등)
- unified_features_builder.build_unified_features(symbol)
    → 위 결과 + pattern_detection.build_pattern_features(...)
      를 결합해 pattern_features / pattern_summary 를 추가한 dict 반환
- gpt_decider.ask_entry_decision_safe(..., market_features=unified_features)
    → GPT-5.1 이 숫자 피처 + 패턴 요약을 모두 보고 의사결정

백필 금지 정책
----------------------------------------------------
- 이 모듈은 market_features_ws 와 마찬가지로 **REST 백필을 호출하지 않는다.**
- 필수 타임프레임(1m/5m/15m)의 raw_ohlcv_last20 / timeframes 가
  비어 있거나 누락되면 UnifiedFeaturesError 를 발생시키고,
  반드시 Render 로그와 텔레그램으로 동시에 알린다.
- pattern_detection 내부에서도 OHLCV/지표 시리즈에 대해 별도의
  PatternError 검증을 수행하며, 이 예외 역시 통과시키지 않고 곧바로 실패시킨다.

반환 구조 요약 (토큰 최적화 버전)
----------------------------------------------------
build_unified_features(...) → Dict[str, Any] 예시:

{
  "symbol": "BTC-USDT",
  "checked_at_ms": 1763600000000,

  # market_features_ws 에서 가져온 요약 정보 (구조는 기존과 동일하되,
  # 필요에 따라 timeframes 내부의 raw 시리즈는 pattern_detection 이후
  # 별도로 사용하지 않는 한 최소화/제거하는 방향으로 운영할 수 있다.
  "timeframes": { ... },
  "orderbook": { ... },
  "multi_timeframe": { ... },

  # 타임프레임별 패턴 스코어(핵심 숫자 피처만):
  "pattern_features": {
    "1m": {
      "interval": "1m",
      "pattern_score": 0.0,
      "reversal_probability": 0.0,
      "continuation_probability": 0.0,
      "momentum_score": 0.0,
      "volume_confirmation": 0.0,
      "wick_strength": 0.0,
      "liquidity_event_score": 0.0,
      "has_bullish_pattern": 0,
      "has_bearish_pattern": 0,
    },
    "5m": { ... },
    "15m": { ... }
  },

  # 상위 N개 compact 패턴 + 전역 요약 스코어:
  "pattern_summary": {
    "patterns": [
      {
        "pattern": "bullish_engulfing",
        "direction": "BULLISH",
        "confidence": "high",
        "strength": 0.92,
        "timeframe": "5m",
        # (필요 시 요약 텍스트/트리거 가격 정도만 유지)
      },
      ... (전 타임프레임 상위 N개 패턴까지)
    ],

    "best_pattern": "bullish_engulfing",
    "best_pattern_direction": "BULLISH",  # or "BEARISH" / "NONE"
    "best_pattern_confidence": "high",     # or "medium" / "low" / "none"
    "best_timeframe": "1m|5m|15m|None",

    "pattern_score": 0.0~1.0,
    "reversal_probability": 0.0~1.0,
    "continuation_probability": 0.0~1.0,
    "momentum_score": 0.0~1.0,
    "volume_confirmation": 0.0~1.0,
    "wick_strength": 0.0~1.0,
    "liquidity_event_score": 0.0~1.0,

    "has_bullish_pattern": 0|1,
    "has_bearish_pattern": 0|1,
  }
}

GPT 프롬프트에서는 pattern_summary 와 pattern_features 의 숫자 피처를
우선적으로 사용하고, pattern_summary.patterns 에 들어 있는 상위 N개
compact 패턴만 참고하도록 설계하면 토큰을 절반 이하로 줄일 수 있다.
"""

import math
from typing import Any, Dict, List, Optional, Tuple

try:
    # 실제 Render 환경에서는 telelog 를 사용해 텔레그램 알림/로그를 보낸다.
    from telelog import log, send_tg
except Exception:  # 로컬 테스트/단일 모듈 실행 시 대비

    def log(msg: str) -> None:  # type: ignore[override]
        print(msg)

    def send_tg(msg: str) -> None:  # type: ignore[override]
        print(f"[TG-STUB] {msg}")


from settings_ws import load_settings
from market_features_ws import build_entry_features_ws, FeatureBuildError
from pattern_detection import build_pattern_features, PatternError


SET = load_settings()


class UnifiedFeaturesError(RuntimeError):
    """마켓 피처 + 패턴 피처 통합 단계에서 사용하는 예외."""


def _safe_tg(msg: str) -> None:
    """텔레그램 전송 오류가 있어도 통합 빌더 동작은 유지."""
    try:
        send_tg(msg)
    except Exception:
        # 텔레그램 쪽 오류는 피처 빌더 동작에 영향을 주지 않는다.
        pass


def _fail_unified(symbol: str, stage: str, reason: str) -> None:
    """통합 피처 생성 중 치명적 오류 발생 시 호출.

    - 항상 Render 로그 + 텔레그램에 남긴 뒤 UnifiedFeaturesError 를 발생시킨다.
    """
    msg = f"[UNIFIED-FEAT][{symbol}] {stage} 실패: {reason}"
    log(msg)
    _safe_tg(msg)
    raise UnifiedFeaturesError(msg)


PATTERN_TFS: Tuple[str, ...] = ("1m", "5m", "15m")

# 타임프레임별로 얼마나 많은 패턴을 살릴지, 전체 summary 에서 상위 몇 개까지만
# GPT 프롬프트에 태울지에 대한 상한. (토큰 절감 핵심 포인트)
MAX_PATTERNS_PER_TF: int = 4
MAX_SUMMARY_PATTERNS: int = 10

# 패턴 한 개당 어떤 필드만 남길지 화이트리스트.
# (나머지 raw 포인트/내부 디버그 정보는 모두 버려서 토큰을 줄인다.)
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


def _safe_float(value: Any, default: float = 0.0) -> float:
    """float 캐스팅이 실패해도 기본값으로 방어."""
    try:
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    """int 캐스팅이 실패해도 기본값으로 방어."""
    try:
        return int(value)
    except Exception:
        return default


def _compact_pattern_item(p: Dict[str, Any], timeframe: str) -> Dict[str, Any]:
    """pattern_detection 에서 넘어온 raw 패턴 dict 를 compact 형태로 축소.

    - 화이트리스트에 포함된 필드만 남긴다.
    - 항상 timeframe 태그를 붙여서 어떤 타임프레임에서 나온 패턴인지 표시한다.
    """
    item: Dict[str, Any] = {}
    for key in _PATTERN_ITEM_KEYS:
        if key in p:
            item[key] = p[key]
    # timeframes 는 GPT 가 패턴의 스케일을 판단하는 데 중요하므로 항상 유지
    item["timeframe"] = timeframe

    # strength 는 float 로 강제 캐스팅해서 이후 정렬/비교에 사용
    item["strength"] = _safe_float(item.get("strength", 0.0), 0.0)
    return item


def build_unified_features(symbol: Optional[str] = None) -> Dict[str, Any]:
    """WS 마켓 피처 + Ultra 패턴 피처를 통합해 GPT-5.1용 dict 를 생성한다.

    사용 예시:
        from unified_features_builder import build_unified_features
        from settings_ws import load_settings

        SET = load_settings()
        market_features = build_unified_features(SET.symbol)
        # gpt_decider.ask_entry_decision_safe(..., market_features=market_features)
    """
    if symbol is None:
        symbol = getattr(SET, "symbol", "UNKNOWN")

    # 1) 기본 마켓 피처 생성 (market_features_ws)
    try:
        base = build_entry_features_ws(symbol)
    except FeatureBuildError:
        # market_features_ws 쪽에서 이미 텔레그램/로그를 남겼으므로 그대로 전파
        raise
    except Exception as e:  # 예기치 못한 오류
        _fail_unified(symbol, "build_entry_features_ws", f"알 수 없는 예외: {e!r}")

    if not isinstance(base, dict):
        _fail_unified(symbol, "build_entry_features_ws", "반환값이 dict 형식이 아닙니다.")

    timeframes = base.get("timeframes")
    if not isinstance(timeframes, dict) or not timeframes:
        _fail_unified(symbol, "timeframes", "timeframes dict 가 비어 있거나 없습니다.")

    # 2) 각 타임프레임별 패턴 피처 계산 (pattern_detection)
    #    - 여기서는 pattern_detection 이 만들어낸 raw dict 를 받아서
    #      숫자 스코어 + compact 패턴 정보만 남기는 방향으로 토큰을 최적화한다.
    pattern_features: Dict[str, Dict[str, Any]] = {}

    # 전역 요약/베스트 패턴 계산용 변수들
    all_patterns: List[Dict[str, Any]] = []
    best_pattern_name: Optional[str] = None
    best_pattern_direction: Optional[str] = None
    best_pattern_confidence: Optional[str] = None
    best_pattern_timeframe: Optional[str] = None
    best_strength: float = -1.0

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
        tf_data = timeframes.get(tf)
        if not isinstance(tf_data, dict):
            _fail_unified(symbol, f"patterns[{tf}]", f"timeframes['{tf}'] 데이터가 없습니다.")

        raw_ohlcv = tf_data.get("raw_ohlcv_last20")
        if raw_ohlcv is None:
            _fail_unified(symbol, f"patterns[{tf}]", "raw_ohlcv_last20 가 없습니다.")
        if not isinstance(raw_ohlcv, list) or len(raw_ohlcv) == 0:
            _fail_unified(symbol, f"patterns[{tf}]", "raw_ohlcv_last20 리스트가 비어 있습니다.")

        indicators = tf_data.get("indicators")
        if not isinstance(indicators, dict):
            indicators = None

        try:
            pf_raw = build_pattern_features(
                raw_ohlcv_last20=raw_ohlcv,
                interval=tf,
                indicators=indicators,
                orderbook_features=base.get("orderbook"),
            )
        except PatternError as e:
            # pattern_detection 쪽에서 이미 텔레그램/로그를 남겼지만,
            # 통합 빌더 레벨에서도 명시적으로 실패를 남긴다.
            _fail_unified(symbol, f"patterns[{tf}]", f"build_pattern_features 실패: {e}")
        except Exception as e:
            _fail_unified(symbol, f"patterns[{tf}]", f"알 수 없는 예외: {e!r}")

        if not isinstance(pf_raw, dict):
            _fail_unified(symbol, f"patterns[{tf}]", "pattern_features 반환값이 dict 가 아닙니다.")

        # ── 2-1) raw 패턴 리스트를 compact 형태로 변환 + 상위 N개만 유지 ─────────
        raw_patterns = pf_raw.get("patterns") or []
        compact_patterns: List[Dict[str, Any]] = []

        if isinstance(raw_patterns, list):
            for p in raw_patterns:
                if not isinstance(p, dict):
                    continue
                item = _compact_pattern_item(p, tf)
                strength = _safe_float(item.get("strength", 0.0), 0.0)
                item["strength"] = strength
                compact_patterns.append(item)

                # 전역 베스트 패턴 갱신
                if strength > best_strength:
                    best_strength = strength
                    best_pattern_name = str(item.get("pattern") or "") or None
                    best_pattern_direction = str(item.get("direction") or "") or None
                    best_pattern_confidence = str(item.get("confidence") or "") or None
                    best_pattern_timeframe = tf

            # 강도 기준 내림차순 정렬 후, 각 타임프레임별 상위 MAX_PATTERNS_PER_TF 개만 유지
            compact_patterns.sort(key=lambda x: x.get("strength", 0.0), reverse=True)
            if MAX_PATTERNS_PER_TF > 0:
                compact_patterns = compact_patterns[:MAX_PATTERNS_PER_TF]

            all_patterns.extend(compact_patterns)

        # ── 2-2) 타임프레임별 숫자 스코어를 pattern_features 에 압축 저장 ─────────
        compact_pf: Dict[str, Any] = {"interval": tf}

        try:
            local_pattern_score = _safe_float(pf_raw.get("pattern_score", 0.0), 0.0)
            local_reversal = _safe_float(
                pf_raw.get("reversal_probability", pf_raw.get("reversal_score", 0.0)),
                0.0,
            )
            local_continuation = _safe_float(
                pf_raw.get("continuation_probability", pf_raw.get("continuation_score", 0.0)),
                0.0,
            )
            local_momentum = _safe_float(pf_raw.get("momentum_score", 0.0), 0.0)
            local_volume_conf = _safe_float(pf_raw.get("volume_confirmation", 0.0), 0.0)
            local_wick_strength = _safe_float(pf_raw.get("wick_strength", 0.0), 0.0)
            local_liquidity_score = _safe_float(pf_raw.get("liquidity_event_score", 0.0), 0.0)
        except Exception as e:
            # 스코어 파싱 중 문제가 생겨도 통합 빌더가 죽지는 않게 방어
            log(f"[UNIFIED-FEAT][{symbol}] pattern score parse error ({tf}): {e!r}")
            local_pattern_score = 0.0
            local_reversal = 0.0
            local_continuation = 0.0
            local_momentum = 0.0
            local_volume_conf = 0.0
            local_wick_strength = 0.0
            local_liquidity_score = 0.0

        # 타임프레임별 compact 스코어 저장
        compact_pf["pattern_score"] = local_pattern_score
        compact_pf["reversal_probability"] = local_reversal
        compact_pf["continuation_probability"] = local_continuation
        compact_pf["momentum_score"] = local_momentum
        compact_pf["volume_confirmation"] = local_volume_conf
        compact_pf["wick_strength"] = local_wick_strength
        compact_pf["liquidity_event_score"] = local_liquidity_score

        # 전역 max 스코어 갱신
        global_pattern_score = max(global_pattern_score, local_pattern_score)
        global_reversal = max(global_reversal, local_reversal)
        global_continuation = max(global_continuation, local_continuation)
        global_momentum = max(global_momentum, local_momentum)
        global_volume_conf = max(global_volume_conf, local_volume_conf)
        global_wick_strength = max(global_wick_strength, local_wick_strength)
        global_liquidity_score = max(global_liquidity_score, local_liquidity_score)

        # 방향성 플래그 처리 (0/1 로 압축)
        try:
            local_has_bullish = _safe_int(pf_raw.get("has_bullish_pattern", 0), 0)
            local_has_bearish = _safe_int(pf_raw.get("has_bearish_pattern", 0), 0)
        except Exception:
            local_has_bullish = 0
            local_has_bearish = 0

        compact_pf["has_bullish_pattern"] = 1 if local_has_bullish else 0
        compact_pf["has_bearish_pattern"] = 1 if local_has_bearish else 0

        has_bullish_pattern = max(has_bullish_pattern, compact_pf["has_bullish_pattern"])
        has_bearish_pattern = max(has_bearish_pattern, compact_pf["has_bearish_pattern"])

        pattern_features[tf] = compact_pf

    # 3) 타임프레임별 compact 패턴을 한데 모아 전역 summary 구성 ──────────────
    #    - all_patterns 에는 이미 각 타임프레임별 상위 MAX_PATTERNS_PER_TF 개만 들어 있다.
    #    - 여기서 한 번 더 strength 기준으로 정렬 후, 전체 상위 MAX_SUMMARY_PATTERNS 개만 유지.
    if all_patterns:
        all_patterns.sort(key=lambda x: x.get("strength", 0.0), reverse=True)
        if MAX_SUMMARY_PATTERNS > 0:
            all_patterns = all_patterns[:MAX_SUMMARY_PATTERNS]

    # 베스트 패턴이 하나도 없다면 direction/confidence 기본값을 NONE/none 으로 맞춘다.
    best_pattern = best_pattern_name or None
    best_direction = best_pattern_direction or "NONE"
    best_confidence = best_pattern_confidence or "none"
    best_timeframe = best_pattern_timeframe or "None"

    pattern_summary: Dict[str, Any] = {
        "patterns": all_patterns,
        "best_pattern": best_pattern,
        "best_pattern_direction": best_direction,
        "best_pattern_confidence": best_confidence,
        "best_timeframe": best_timeframe,
        # 스코어/확률 요약
        "pattern_score": global_pattern_score,
        "reversal_probability": global_reversal,
        "continuation_probability": global_continuation,
        "momentum_score": global_momentum,
        "volume_confirmation": global_volume_conf,
        "wick_strength": global_wick_strength,
        "liquidity_event_score": global_liquidity_score,
        # 방향성 플래그
        "has_bullish_pattern": has_bullish_pattern,
        "has_bearish_pattern": has_bearish_pattern,
    }

    # 4) 최종 dict 구성: 기존 market_features_ws 결과 위에 패턴 피처를 얹는다.
    #    - base 는 그대로 두되, GPT 프롬프트에서는 pattern_summary 및
    #      pattern_features 의 숫자 스코어를 우선 사용하는 것을 권장한다.
    result = dict(base)
    result["pattern_features"] = pattern_features
    result["pattern_summary"] = pattern_summary

    # 🔧 (추가) GPT ENTRY 필터가 필요로 하는 핵심 값 전달
    tf5 = base.get("timeframes", {}).get("5m", {})
    reg = tf5.get("regime", {})
    result["trend_strength"] = reg.get("trend_strength")
    result["volatility"] = tf5.get("atr_pct")
    return result
