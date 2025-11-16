"""strategies_trend_ws.py
웹소켓으로 받은 5m/15m/1m 캔들을 기준으로 추세장(트렌드) 신호를 판단하는 모듈.

2025-11-16 보강 (GPT 컨텍스트용 요약 빌더 추가)
----------------------------------------------------
1) build_trend_gpt_context(...) 추가
   - 5m/15m/1m 캔들 기반으로 EMA/RSI/박스폭/다이버전스/방향 후보 등을 한 번에 계산해서
     GPT-5 프롬프트에 바로 넣을 수 있는 dict 형태의 컨텍스트를 생성.
   - 이 모듈 안에서는 GPT를 직접 호출하지 않으며, 순수 수치 엔진 역할만 유지.
   - gpt_decider.py / signal_flow_ws.py / position_watch_ws.py 등에서
     extra["trend_ctx"] 로 실어 보내는 용도로만 사용해야 한다.
2) decide_signal_5m_trend / decide_trend_15m / confirm_1m_direction 의 기존 동작은 그대로,
   추가 함수만 도입해서 상위 레이어에서 GPT와의 연계를 쉽게 했다.

2025-11-14 변경 요약
----------------------------------------------------
1) indicators.py 분리 이후 구조 정리
   - EMA/RSI/다이버전스 계산은 indicators.py 공용 함수 사용.
   - Candle 타입 별칭을 indicators.Candle 과 동일하게 유지해서 재사용성 확보.
2) 역할 분리 및 운영 가이드 명시
   - 이 모듈은 "추세 신호 판단"에만 집중한다.
   - DB 적재, 레짐 점수(bt_regime_scores) 저장, 포지션 관리 등은
     signal_flow_ws / signal_analysis_worker / position_watch_ws 가 담당.
3) WS 환경 안정성 강화
   - 캔들 개수 부족, NaN, 0 분모 등의 상황에서는 조용히 None 을 반환하도록 유지.
   - RSI/횡보 임계치는 settings 기반으로만 읽도록 주석을 보강해서
     운영 중 ENV 누락/조정 시에도 크래시 없이 동작하게 정리.

2025-11-13 추가 보정
----------------------------------------------------
A) 안정성/내구성
 - NaN/0 분모 방지, 캔들 개수 부족 시 조기 반환, 인덱스 안전화.
 - 로그에 실제 사용 임계값(EMA 스프레드/마지막 캔들 범위/RSI)을 함께 출력.

B) 설정 연계(하위호환 유지)
 - `decide_signal_5m_trend(...)`에 선택 인자 `settings` 추가(기존 호출 그대로 동작).
   · 있으면 `settings.rsi_overbought/oversold`로 RSI 임계치를 덮어씀.
   · 5m 마지막 캔들 최소 범위 임계치는 `settings.trend_sideways_range_pct`가 있으면
     그 값과 기본값(0.0005) 중 더 큰 값을 사용.
 - 스프레드 임계(`min_ema_spread_ratio`)와 마지막 캔들 범위 임계(`min_last_range_pct`)를
   함수 인자로 노출(기본 0.0005). 필요 시 호출부에서 미세 조정 가능.

C) 동작 논리 변경 없음
 - 5m: EMA20/EMA50 크로스 + RSI 필터 + 소폭 변동/스프레드 축소 시 스킵.
 - 15m: EMA20/EMA50로 큰 방향만 판단.
 - 1m: 최종 역주행 캔들 차단 용도로만 사용(데이터 없으면 허용).
"""

from __future__ import annotations

import math
from typing import List, Optional, Dict, Any

from indicators import (
    ema,
    rsi,
    has_bearish_rsi_divergence,
    has_bullish_rsi_divergence,
    Candle,  # (ts_ms, open, high, low, close)
)
from telelog import log

# 타입 별칭: 가독성을 위해 타임프레임별로 분리해서 사용
FiveMCandles = List[Candle]
FifteenMCandles = List[Candle]
OneMCandles = List[Candle]


# ───────────────────────────────────────────────────────────────
# 5분봉 신호 판단 (기존 3m 대체)
# ───────────────────────────────────────────────────────────────
def decide_signal_5m_trend(
    candles_5m: FiveMCandles,
    rsi_overbought: int = 70,
    rsi_oversold: int = 30,
    *,
    settings: object | None = None,
    min_ema_spread_ratio: float = 0.0005,
    min_last_range_pct: float = 0.0005,
) -> Optional[str]:
    """5분봉만 보고 1차 추세 신호를 만든다.

    반환값: "LONG" / "SHORT" / None

    로직 요약:
      - 캔들 60개 미만이면 계산 안 함(EMA50 필요)
      - EMA20/EMA50 스프레드가 너무 작고, 마지막 봉 변동폭도 작으면 스킵
      - EMA20↗ EMA50 골든크로스 + RSI < 과매수 → LONG
      - EMA20↘ EMA50 데드크로스 + RSI > 과매도 → SHORT
      - 가격이 50EMA 반대편이면 각각 무효
      - 신호와 반대 RSI 다이버전스면 무효

    NOTE
    ----------------------------------------------------
    - 이 함수는 오직 "추세 후보"를 만드는 역할만 한다.
    - 최종 진입 여부는 signal_flow_ws 의 중재/쿨다운/1m 확인에서 결정한다.
    """
    count_5m = len(candles_5m)
    log(f"[TREND] (WS) decide_signal_5m_trend 5m_count={count_5m}")

    # EMA50, RSI 계산을 위해 최소 60개 이상 필요
    if count_5m < 60:
        return None

    closes = [c[4] for c in candles_5m]

    # settings 연계: RSI 임계 및 마지막 캔들 최소 범위 덮어쓰기
    if settings is not None:
        try:
            # RSI 과열/침체 구간을 ENV 로 조정 가능하게 함
            rsi_overbought = int(getattr(settings, "rsi_overbought", rsi_overbought))
            rsi_oversold = int(getattr(settings, "rsi_oversold", rsi_oversold))
            # 횡보 구간 최소 범위: 기본값과 설정값 중 더 큰 값을 사용(보수적)
            cfg_rng = float(getattr(settings, "trend_sideways_range_pct", min_last_range_pct))
            min_last_range_pct = max(min_last_range_pct, cfg_rng)
        except Exception as e:  # pragma: no cover - 방어적 처리
            log(f"[TREND] (WS) settings override failed: {e}")

    # EMA/RSI 계산 (indicators.py 공용 함수 사용)
    e20 = ema(closes, 20)
    e50 = ema(closes, 50)
    r14 = rsi(closes, 14)

    # 끝단 NaN 이 하나라도 있으면 계산 중단
    if any(math.isnan(x) for x in (e20[-1], e20[-2], e50[-1], e50[-2], r14[-1])):
        return None

    e20_prev, e20_now = e20[-2], e20[-1]
    e50_prev, e50_now = e50[-2], e50[-1]
    r_now = r14[-1]
    price_now = closes[-1]

    # 마지막 5m 캔들의 변동폭(고저/저가)
    last = candles_5m[-1]
    lo = last[3]
    hi = last[2]
    if not lo or lo <= 0:
        return None
    last_range_pct = (hi - lo) / lo

    # EMA 스프레드(분모 0 방지): |EMA20-EMA50| / |EMA50|
    denom = abs(e50_now) if e50_now != 0 else 1.0
    spread_ratio = abs(e20_now - e50_now) / denom

    # 작은 변동 + 스프레드 축소 동시 발생 시 스킵
    if spread_ratio < min_ema_spread_ratio and last_range_pct < min_last_range_pct:
        log(
            "[TREND] (WS) skip: small spread & tiny last 5m "
            f"(spread={spread_ratio:.6f} < {min_ema_spread_ratio:.6f}, "
            f"range={last_range_pct:.6f} < {min_last_range_pct:.6f})"
        )
        return None

    # 추가 안전장치: 마지막 봉 자체가 너무 작으면 스킵 (횡보 필터)
    if last_range_pct < min_last_range_pct:
        log(
            f"[TREND] (WS) skip: last 5m range too small "
            f"{last_range_pct:.6f} < {min_last_range_pct:.6f}"
        )
        return None

    # 골든/데드크로스 판정 + RSI 필터
    long_sig = (e20_prev < e50_prev) and (e20_now > e50_now) and (r_now < rsi_overbought)
    short_sig = (e20_prev > e50_prev) and (e20_now < e50_now) and (r_now > rsi_oversold)

    # 가격이 50EMA 반대편에 있으면 신호 무효화
    if long_sig and price_now < e50_now:
        long_sig = False
    if short_sig and price_now > e50_now:
        short_sig = False

    # 다이버전스 검사: 신호와 반대 다이버전스면 무효
    if long_sig:
        if has_bearish_rsi_divergence(candles_5m, r14):
            log("[TREND] (WS) skip: bearish RSI divergence on LONG")
            return None
        return "LONG"

    if short_sig:
        if has_bullish_rsi_divergence(candles_5m, r14):
            log("[TREND] (WS) skip: bullish RSI divergence on SHORT")
            return None
        return "SHORT"

    return None


# ───────────────────────────────────────────────────────────────
# 15분봉 큰 방향 판단 (기존과 동일하나 로그 보강)
# ───────────────────────────────────────────────────────────────
def decide_trend_15m(candles_15m: FifteenMCandles) -> Optional[str]:
    """15분봉 EMA20/EMA50 으로 큰 방향을 본다.

    반환값: "LONG" / "SHORT" / None

    NOTE
    ----------------------------------------------------
    - 이 함수는 "상위 타임프레임 방향"만 본다.
    - 실제 진입 여부는 5m 후보 및 1m 확인, 쿨다운, 레짐 여부에 따라 바뀐다.
    """
    count_15m = len(candles_15m)
    log(f"[TREND] (WS) decide_trend_15m 15m_count={count_15m}")
    if count_15m < 50:
        return None

    closes = [c[4] for c in candles_15m]
    e20 = ema(closes, 20)
    e50 = ema(closes, 50)

    if math.isnan(e20[-1]) or math.isnan(e50[-1]):
        return None

    if e20[-1] > e50[-1]:
        return "LONG"
    if e20[-1] < e50[-1]:
        return "SHORT"
    return None


# ───────────────────────────────────────────────────────────────
# 1분봉 확인 (완화 버전, 로그 추가)
# ───────────────────────────────────────────────────────────────
def confirm_1m_direction(candles_1m: OneMCandles, direction: str) -> bool:
    """1분봉을 ‘역주행 캔들 차단’ 용도로만 사용한다.

    - 데이터가 없으면 막지 않는다(True 반환).
    - 마지막 1분봉이 LONG 방향이면 양봉, SHORT 방향이면 음봉이어야 통과.
    """
    count_1m = len(candles_1m)
    log(f"[TREND] (WS) confirm_1m_direction 1m_count={count_1m} dir={direction}")
    if count_1m < 1:
        return True

    _ts, o, _h, _l, c = candles_1m[-1]

    if direction == "LONG":
        return c >= o
    if direction == "SHORT":
        return c <= o
    return True


# ───────────────────────────────────────────────────────────────
# GPT 연동용: 추세 컨텍스트 요약 빌더
# ───────────────────────────────────────────────────────────────
def build_trend_gpt_context(
    candles_5m: FiveMCandles,
    candles_15m: Optional[FifteenMCandles] = None,
    candles_1m: Optional[OneMCandles] = None,
    *,
    settings: object | None = None,
    min_ema_spread_ratio: float = 0.0005,
    min_last_range_pct: float = 0.0005,
) -> Dict[str, Any]:
    """GPT 프롬프트에서 바로 쓸 수 있는 추세 관련 요약 지표를 계산해서 돌려준다.

    반환 예시(dict):

        {
          "count_5m": 120,
          "count_15m": 120,
          "count_1m": 40,
          "rsi_overbought": 70,
          "rsi_oversold": 30,
          "trend_sideways_range_pct_cfg": 0.0008,
          "min_last_range_pct": 0.0008,
          "min_ema_spread_ratio": 0.0005,

          "ema20_5m": 94500.1,
          "ema50_5m": 94010.3,
          "ema_spread_ratio_5m": 0.0042,
          "last_range_pct_5m": 0.0012,
          "price_now_5m": 94600.5,
          "rsi_14_5m": 62.3,
          "sideways_flag": false,
          "small_spread_flag": false,

          "trend_5m_signal": "LONG",        # decide_signal_5m_trend 기준 최종 신호 (없으면 None)
          "trend_5m_cross_raw": "LONG",     # EMA20/50 크로스+RSI 필터까지만 적용한 신호
          "has_bullish_rsi_div": false,
          "has_bearish_rsi_div": false,

          "trend_15m": "LONG",
          "ema20_15m": 94200.0,
          "ema50_15m": 93000.0,
          "ema_gap_ratio_15m": 0.0123,

          "last_1m_open": 94580.0,
          "last_1m_close": 94620.0,
          "last_1m_body_dir": "UP",
          "last_1m_supports_long": true,
          "last_1m_supports_short": false
        }

    주의:
    - 이 함수는 어떤 진입/청산도 결정하지 않는다.
      → GPT 프롬프트에 넣을 "상태 설명용" 데이터만 제공한다.
    - decide_signal_5m_trend / decide_trend_15m 의 동작은 건드리지 않는다.
    """
    count_5m = len(candles_5m)
    count_15m = len(candles_15m) if candles_15m else 0
    count_1m = len(candles_1m) if candles_1m else 0

    # settings 기반 임계치 덮어쓰기 (decide_signal_5m_trend 와 동일 규칙 유지)
    rsi_overbought = 70
    rsi_oversold = 30
    trend_sideways_cfg = None
    if settings is not None:
        try:
            rsi_overbought = int(getattr(settings, "rsi_overbought", rsi_overbought))
            rsi_oversold = int(getattr(settings, "rsi_oversold", rsi_oversold))
            cfg_rng = float(getattr(settings, "trend_sideways_range_pct", min_last_range_pct))
            trend_sideways_cfg = cfg_rng
            min_last_range_pct = max(min_last_range_pct, cfg_rng)
        except Exception as e:  # pragma: no cover - 방어적 처리
            log(f"[TREND] (WS) build_trend_gpt_context settings override failed: {e}")

    ctx: Dict[str, Any] = {
        "count_5m": count_5m,
        "count_15m": count_15m,
        "count_1m": count_1m,
        "rsi_overbought": rsi_overbought,
        "rsi_oversold": rsi_oversold,
        "trend_sideways_range_pct_cfg": trend_sideways_cfg,
        "min_last_range_pct": min_last_range_pct,
        "min_ema_spread_ratio": min_ema_spread_ratio,
        # 5m 기본 값 (계산 실패 대비 None 으로 채워둔다)
        "ema20_5m": None,
        "ema50_5m": None,
        "ema_spread_ratio_5m": None,
        "last_range_pct_5m": None,
        "price_now_5m": None,
        "rsi_14_5m": None,
        "sideways_flag": None,
        "small_spread_flag": None,
        "trend_5m_signal": None,
        "trend_5m_cross_raw": None,
        "has_bullish_rsi_div": None,
        "has_bearish_rsi_div": None,
        # 15m 기본 값
        "trend_15m": None,
        "ema20_15m": None,
        "ema50_15m": None,
        "ema_gap_ratio_15m": None,
        # 1m 기본 값
        "last_1m_open": None,
        "last_1m_close": None,
        "last_1m_body_dir": None,
        "last_1m_supports_long": None,
        "last_1m_supports_short": None,
    }

    # 5m 기반 상세 상태
    if count_5m >= 60:
        try:
            closes_5m = [c[4] for c in candles_5m]
            e20_5 = ema(closes_5m, 20)
            e50_5 = ema(closes_5m, 50)
            r14_5 = rsi(closes_5m, 14)

            if not any(math.isnan(x) for x in (e20_5[-1], e20_5[-2], e50_5[-1], e50_5[-2], r14_5[-1])):
                e20_prev, e20_now = e20_5[-2], e20_5[-1]
                e50_prev, e50_now = e50_5[-2], e50_5[-1]
                r_now = r14_5[-1]
                price_now = closes_5m[-1]

                last = candles_5m[-1]
                lo = last[3]
                hi = last[2]
                if lo and lo > 0:
                    last_range_pct = (hi - lo) / lo
                else:
                    last_range_pct = None

                denom = abs(e50_now) if e50_now != 0 else 1.0
                spread_ratio = abs(e20_now - e50_now) / denom

                small_spread = spread_ratio < min_ema_spread_ratio
                small_range = last_range_pct is not None and last_range_pct < min_last_range_pct

                ctx.update(
                    {
                        "ema20_5m": float(e20_now),
                        "ema50_5m": float(e50_now),
                        "ema_spread_ratio_5m": float(spread_ratio),
                        "last_range_pct_5m": float(last_range_pct) if last_range_pct is not None else None,
                        "price_now_5m": float(price_now),
                        "rsi_14_5m": float(r_now),
                        "sideways_flag": bool(small_range) if last_range_pct is not None else None,
                        "small_spread_flag": bool(small_spread),
                    }
                )

                # 골든/데드크로스 + RSI 필터까지만 본 "raw" 방향
                cross_long_raw = (e20_prev < e50_prev) and (e20_now > e50_now) and (r_now < rsi_overbought)
                cross_short_raw = (e20_prev > e50_prev) and (e20_now < e50_now) and (r_now > rsi_oversold)
                trend_5m_cross_raw: Optional[str] = None
                if cross_long_raw:
                    trend_5m_cross_raw = "LONG"
                elif cross_short_raw:
                    trend_5m_cross_raw = "SHORT"
                ctx["trend_5m_cross_raw"] = trend_5m_cross_raw

                # 다이버전스 플래그
                bearish_div = has_bearish_rsi_divergence(candles_5m, r14_5)
                bullish_div = has_bullish_rsi_divergence(candles_5m, r14_5)
                ctx["has_bearish_rsi_div"] = bool(bearish_div)
                ctx["has_bullish_rsi_div"] = bool(bullish_div)

                # decide_signal_5m_trend 와 최대한 동일한 규칙으로 최종 신호를 계산
                trend_sig: Optional[str] = None
                if not (small_spread and small_range) and not small_range:
                    long_sig = cross_long_raw
                    short_sig = cross_short_raw

                    # 가격이 50EMA 반대편에 있으면 신호 무효화
                    if long_sig and price_now < e50_now:
                        long_sig = False
                    if short_sig and price_now > e50_now:
                        short_sig = False

                    if long_sig:
                        if bearish_div:
                            long_sig = False
                        else:
                            trend_sig = "LONG"
                    elif short_sig:
                        if bullish_div:
                            short_sig = False
                        else:
                            trend_sig = "SHORT"

                ctx["trend_5m_signal"] = trend_sig

        except Exception as e:  # pragma: no cover - 방어적 처리
            log(f"[TREND] (WS) build_trend_gpt_context 5m-part failed: {e}")

    # 15m 기반 큰 방향/갭 비율
    if candles_15m:
        try:
            closes_15 = [c[4] for c in candles_15m]
            if len(closes_15) >= 50:
                e20_15 = ema(closes_15, 20)
                e50_15 = ema(closes_15, 50)
                if not (math.isnan(e20_15[-1]) or math.isnan(e50_15[-1])):
                    e20_now_15 = e20_15[-1]
                    e50_now_15 = e50_15[-1]
                    denom15 = abs(e50_now_15) if e50_now_15 != 0 else 1.0
                    gap_ratio_15 = abs(e20_now_15 - e50_now_15) / denom15

                    trend_15 = None
                    if e20_now_15 > e50_now_15:
                        trend_15 = "LONG"
                    elif e20_now_15 < e50_now_15:
                        trend_15 = "SHORT"

                    ctx.update(
                        {
                            "trend_15m": trend_15,
                            "ema20_15m": float(e20_now_15),
                            "ema50_15m": float(e50_now_15),
                            "ema_gap_ratio_15m": float(gap_ratio_15),
                        }
                    )
        except Exception as e:  # pragma: no cover - 방어적 처리
            log(f"[TREND] (WS) build_trend_gpt_context 15m-part failed: {e}")

    # 1m 마지막 캔들 방향 요약
    if candles_1m:
        try:
            _ts, o1, _h1, _l1, c1 = candles_1m[-1]
            body_dir = "FLAT"
            if c1 > o1:
                body_dir = "UP"
            elif c1 < o1:
                body_dir = "DOWN"

            ctx.update(
                {
                    "last_1m_open": float(o1),
                    "last_1m_close": float(c1),
                    "last_1m_body_dir": body_dir,
                    "last_1m_supports_long": bool(c1 >= o1),
                    "last_1m_supports_short": bool(c1 <= o1),
                }
            )
        except Exception as e:  # pragma: no cover - 방어적 처리
            log(f"[TREND] (WS) build_trend_gpt_context 1m-part failed: {e}")

    return ctx


__all__ = [
    "decide_signal_5m_trend",
    "decide_trend_15m",
    "confirm_1m_direction",
    "build_trend_gpt_context",
]
