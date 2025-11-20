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

반환 구조 요약
----------------------------------------------------
build_unified_features(...) → Dict[str, Any] 예시:

{
  "symbol": "BTC-USDT",
  "checked_at_ms": 1763600000000,
  "timeframes": { ... },          # market_features_ws 그대로
  "orderbook": { ... },           # market_features_ws 그대로
  "multi_timeframe": { ... },     # market_features_ws 그대로

  "pattern_features": {
    "1m": { ... },                # pattern_detection.build_pattern_features(...)
    "5m": { ... },
    "15m": { ... }
  },

  "pattern_summary": {
    "patterns": [...],            # 타임프레임 태그가 붙은 패턴 리스트
    "best_pattern": "...",
    "best_pattern_direction": "BULLISH|BEARISH|NONE",
    "best_pattern_confidence": "low|medium|high|none",

    "pattern_score": 0.0~1.0,
    "reversal_probability": 0.0~1.0,
    "continuation_probability": 0.0~1.0,
    "momentum_score": 0.0~1.0,
    "volume_confirmation": 0.0~1.0,
    "wick_strength": 0.0~1.0,
    "liquidity_event_score": 0.0~1.0,

    "has_bullish_pattern": 0|1,
    "has_bearish_pattern": 0|1,
    "best_timeframe": "1m|5m|15m|None"
  }
}

gpt_decider/gpt_trader 는 pattern_summary 와 pattern_features 를
프롬프트에 그대로 녹여서 사용할 수 있다.
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
    pattern_features: Dict[str, Dict[str, Any]] = {}

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
            pf = build_pattern_features(
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

        # 타임프레임 메타 정보 추가
        pf = dict(pf)
        pf["interval"] = tf
        pattern_features[tf] = pf

    # 3) 타임프레임별 패턴 스코어를 집계해 요약 패턴 피처 생성
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

    for tf, pf in pattern_features.items():
        # 개별 패턴 JSON 리스트 평탄화 + 타임프레임 태그 부여
        raw_patterns = pf.get("patterns") or []
        if isinstance(raw_patterns, list):
            for p in raw_patterns:
                if not isinstance(p, dict):
                    continue
                item = dict(p)
                item.setdefault("timeframe", tf)

                try:
                    strength = float(item.get("strength", 0.0))
                except Exception:
                    strength = 0.0

                all_patterns.append(item)

                if strength > best_strength:
                    best_strength = strength
                    best_pattern_name = str(item.get("pattern") or "")
                    best_pattern_direction = str(item.get("direction") or "")
                    best_pattern_confidence = str(item.get("confidence") or "")
                    best_pattern_timeframe = tf

        # 스코어는 "가장 강한" 타임프레임 기준 max 값 사용
        try:
            global_pattern_score = max(global_pattern_score, float(pf.get("pattern_score", 0.0)))
            global_reversal = max(
                global_reversal,
                float(pf.get("reversal_probability", pf.get("reversal_score", 0.0))),
            )
            global_continuation = max(
                global_continuation,
                float(pf.get("continuation_probability", pf.get("continuation_score", 0.0))),
            )
            global_momentum = max(global_momentum, float(pf.get("momentum_score", 0.0)))
            global_volume_conf = max(global_volume_conf, float(pf.get("volume_confirmation", 0.0)))
            global_wick_strength = max(global_wick_strength, float(pf.get("wick_strength", 0.0)))
            global_liquidity_score = max(
                global_liquidity_score, float(pf.get("liquidity_event_score", 0.0))
            )
        except Exception as e:
            # 스코어 파싱 중 문제가 생겨도 통합 빌더가 죽지는 않게 방어
            log(f"[UNIFIED-FEAT][{symbol}] pattern score aggregate error ({tf}): {e!r}")

        try:
            has_bullish_pattern = max(
                has_bullish_pattern, int(pf.get("has_bullish_pattern", 0) or 0)
            )
            has_bearish_pattern = max(
                has_bearish_pattern, int(pf.get("has_bearish_pattern", 0) or 0)
            )
        except Exception:
            # 형 변환 에러는 무시
            pass

    pattern_summary: Dict[str, Any] = {
        "patterns": all_patterns,
        "best_pattern": best_pattern_name,
        "best_pattern_direction": best_pattern_direction,
        "best_pattern_confidence": best_pattern_confidence,
        "best_timeframe": best_pattern_timeframe,
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
    result = dict(base)
    result["pattern_features"] = pattern_features
    result["pattern_summary"] = pattern_summary

    return result
