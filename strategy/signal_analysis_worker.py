"""signal_analysis_worker.py
=====================================================
DB(bt_candles)에 쌓인 웹소켓 캔들을 주기적으로 읽어서
지표(EMA/RSI/ATR)를 계산하고 레짐 점수(bt_regime_scores)에 저장하는 워커.

[2026-03-06 TRADE-GRADE UPGRADE]
----------------------------------------------------
1) _compute_regime_from_candles(...) 를 기관형 다중요소 레짐 엔진으로 업그레이드.
2) 신규 레짐 축 추가:
   - Trend regime
   - Volatility regime
   - Volume regime
   - Liquidity regime
   - Momentum regime
3) 단순 EMA/ATR 휴리스틱을 다중요소 가중 점수 방식으로 대체.
4) regime_total_score 를 내부 계산하되, 기존 반환 구조는 유지:
   (trend_score, range_score, chop_score, event_risk_score, final_regime)
5) run_bot_ws / risk_physics_engine / execution_engine 에서 사용하는 레짐 입력 품질 개선.

[2025-11-17 패치 요약]
----------------------------------------------------
1) indicators.build_regime_features_from_candles(...) 를 사용해
   동일 5m 캔들 시퀀스에 대한 레짐/지표 피처 스냅샷(regime_features)을 계산한다.
2) RegimeScore 모델에 features_json 컬럼이 정의된 경우,
   regime_features 전체를 JSON 으로 저장해 대시보드/리포트/GPT 컨텍스트에서
   공용으로 사용할 수 있게 했다.
   - features_json 컬럼이 없으면 값 저장은 건너뛰고 로그만 남긴다(폴백 없음).
3) 기존 레짐 점수(trend/range/chop/event_risk, final_regime) 계산 로직은 유지하고,
   워커의 동작 방식(주기/에러 처리) 역시 변경하지 않았다.

[2025-11-14 변경 요약]
----------------------------------------------------
1) 기존 CSV 분석/엑셀 생성 워커 → DB 기반 레짐/신호 워커로 전환.
2) market_data_store 가 저장한 5m 캔들을 기준으로 TREND / RANGE / CHOP / NEUTRAL 판정.
3) 결과를 RegimeScore(bt_regime_scores)에 INSERT (동일 symbol/timeframe/ts 는 한 번만 저장).
4) run_bot_ws 에서 start_signal_analysis_thread(...) 로 백그라운드 실행.
   (interval_sec 기본값은 60초, 필요 시 설정에서 조정.)
5) 타입 힌트 정리(SessionLocal → Session)로 Pylance reportInvalidTypeForm 경고 제거.

주의
----------------------------------------------------
- 이 워커는 "상황 기록용"이다. 실제 진입 로직은 run_bot_ws / entry_flow 가 담당.
- DB 오류가 나도 메인 봇을 멈추지 않도록 예외는 잡고 로그만 남긴다.
"""

from __future__ import annotations

import json
import math
import threading
import time
from typing import Dict, List, Optional, Sequence, Tuple

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from state.db_core import SessionLocal
from state.db_models import Candle, RegimeScore
from strategy.indicators import ema, rsi, calc_atr, build_regime_features_from_candles
from settings import load_settings
from infra.telelog import log

# 설정 로드 (심볼, 분석 기준 타임프레임/캔들 수 등)
SET = load_settings()
SYMBOL: str = getattr(SET, "symbol", "BTC-USDT")
ANALYSIS_TF: str = getattr(SET, "analysis_timeframe", "5m")
ANALYSIS_CANDLE_COUNT: int = getattr(SET, "analysis_candle_count", 300)


# ─────────────────────────────────────────────
# DB에서 캔들 로드 유틸
# ─────────────────────────────────────────────
def _load_recent_candles(
    session: Session,
    symbol: str,
    timeframe: str,
    limit: int,
) -> List[Candle]:
    """주어진 symbol/timeframe 의 최근 캔들을 과거→현재 순으로 반환."""
    rows: List[Candle] = (
        session.query(Candle)
        .filter(Candle.symbol == symbol, Candle.timeframe == timeframe)
        .order_by(Candle.ts.desc())
        .limit(limit)
        .all()
    )
    rows.reverse()  # 오래된 것부터 정렬
    return rows


# ─────────────────────────────────────────────
# STRICT 헬퍼
# ─────────────────────────────────────────────
def _clamp_0_100(v: float) -> float:
    if not math.isfinite(v):
        raise RuntimeError(f"non-finite score detected: {v}")
    if v < 0.0:
        return 0.0
    if v > 100.0:
        return 100.0
    return float(v)


def _mean_strict(values: Sequence[float], *, name: str) -> float:
    if not values:
        raise RuntimeError(f"{name} is empty")
    total = 0.0
    for i, v in enumerate(values):
        if not math.isfinite(v):
            raise RuntimeError(f"{name}[{i}] is not finite: {v}")
        total += float(v)
    out = total / float(len(values))
    if not math.isfinite(out):
        raise RuntimeError(f"{name} mean is not finite")
    return out


def _last_indicator_value(v: object, *, name: str) -> float:
    if isinstance(v, (list, tuple)):
        if not v:
            raise RuntimeError(f"{name} is empty")
        x = float(v[-1])
    else:
        x = float(v)
    if not math.isfinite(x):
        raise RuntimeError(f"{name} must be finite")
    return float(x)


# ─────────────────────────────────────────────
# 레짐(시장 상태) 계산 로직
# ─────────────────────────────────────────────
def _compute_regime_from_candles(
    candles_5m: Sequence[Candle],
) -> Optional[Tuple[float, float, float, float, str]]:
    """5m 캔들 시퀀스에서 레짐 점수와 레이블을 계산.

    반환: (trend_score, range_score, chop_score, event_risk_score, final_regime)
    - 점수 범위는 0.0~100.0 기준으로 설계.
    - final_regime 은 "TREND" / "RANGE" / "CHOP" / "NEUTRAL" 중 하나.

    ⚠ 레짐 피처(regime_features)는 _analyze_and_store_once() 에서
      build_regime_features_from_candles(...) 를 통해 별도로 계산/저장한다.
    """
    if len(candles_5m) < 30:
        # 데이터가 너무 적으면 계산하지 않음
        return None

    ema_fast_len = int(getattr(SET, "ema_fast_len", 20))
    ema_slow_len = int(getattr(SET, "ema_slow_len", 50))
    atr_len = int(getattr(SET, "atr_len", 14))
    rsi_len = int(getattr(SET, "rsi_len", 14))

    if ema_fast_len <= 0 or ema_slow_len <= 0 or atr_len <= 0 or rsi_len <= 0:
        raise RuntimeError("ema/rsi/atr length settings must be positive")

    min_required = max(30, ema_slow_len + 9, (atr_len * 2) + 1, rsi_len + 1, 40)
    if len(candles_5m) < min_required:
        return None

    opens = [float(c.open) for c in candles_5m]
    highs = [float(c.high) for c in candles_5m]
    lows = [float(c.low) for c in candles_5m]
    closes = [float(c.close) for c in candles_5m]
    volumes = [float(c.volume) for c in candles_5m]

    for i, (o, h, l, c, v) in enumerate(zip(opens, highs, lows, closes, volumes)):
        if not all(math.isfinite(x) for x in (o, h, l, c, v)):
            raise RuntimeError(f"non-finite candle value detected at index={i}")
        if o <= 0.0 or h <= 0.0 or l <= 0.0 or c <= 0.0:
            raise RuntimeError(f"OHLC must be > 0 at index={i}")
        if h < l:
            raise RuntimeError(f"high < low at index={i}")
        if h < max(o, c) or l > min(o, c):
            raise RuntimeError(f"invalid candle bounds at index={i}")
        if v < 0.0:
            raise RuntimeError(f"volume must be >= 0 at index={i}")

    last_close = closes[-1]
    if last_close <= 0.0 or not math.isfinite(last_close):
        raise RuntimeError("last_close must be finite > 0")

    # ─────────────────────────────────────────
    # Trend regime
    # - EMA spread
    # - price slope
    # - structure HH/HL, LL/LH
    # ─────────────────────────────────────────
    ema_fast_list = ema(closes, ema_fast_len)
    ema_slow_list = ema(closes, ema_slow_len)

    if not isinstance(ema_fast_list, list) or not ema_fast_list:
        raise RuntimeError("ema_fast_list missing/empty")
    if not isinstance(ema_slow_list, list) or not ema_slow_list:
        raise RuntimeError("ema_slow_list missing/empty")

    ema_fast = float(ema_fast_list[-1])
    ema_slow = float(ema_slow_list[-1])

    if not math.isfinite(ema_fast) or not math.isfinite(ema_slow):
        raise RuntimeError("ema_fast/ema_slow must be finite")
    if ema_slow <= 0.0:
        raise RuntimeError("ema_slow must be > 0")

    ema_spread_pct = abs(ema_fast - ema_slow) / last_close * 100.0
    slope_pct_10 = abs((closes[-1] - closes[-10]) / closes[-10]) * 100.0
    slope_pct_20 = abs((closes[-1] - closes[-20]) / closes[-20]) * 100.0
    slope_pct = max(slope_pct_10, slope_pct_20)

    last20_high = max(highs[-20:])
    prev20_high = max(highs[-40:-20])
    last20_low = min(lows[-20:])
    prev20_low = min(lows[-40:-20])

    if last20_high > prev20_high and last20_low > prev20_low:
        structure_score = 20.0
    elif last20_high < prev20_high and last20_low < prev20_low:
        structure_score = 20.0
    elif last20_high > prev20_high or last20_low < prev20_low:
        structure_score = 10.0
    else:
        structure_score = 0.0

    trend_score = _clamp_0_100(
        (min(ema_spread_pct / 0.60, 1.0) * 40.0)
        + (min(slope_pct / 1.50, 1.0) * 40.0)
        + structure_score
    )

    # ─────────────────────────────────────────
    # Volatility regime
    # - ATR pct
    # - ATR expansion
    # - range expansion
    # ─────────────────────────────────────────
    atr_input = [
        (
            int(c.ts.timestamp() * 1000),
            float(c.open),
            float(c.high),
            float(c.low),
            float(c.close),
        )
        for c in candles_5m
    ]

    atr_now = float(calc_atr(atr_input[-(atr_len + 1) :]))
    atr_prev = float(calc_atr(atr_input[-((atr_len * 2) + 1) : -atr_len]))

    if not math.isfinite(atr_now) or not math.isfinite(atr_prev):
        raise RuntimeError("ATR must be finite")
    if atr_now <= 0.0 or atr_prev <= 0.0:
        raise RuntimeError("ATR must be > 0")

    atr_pct = (atr_now / last_close) * 100.0
    atr_expansion = atr_now / atr_prev

    recent_range_pct = _mean_strict(
        [((h - l) / c) * 100.0 for h, l, c in zip(highs[-5:], lows[-5:], closes[-5:])],
        name="recent_range_pct",
    )
    prev_range_pct = _mean_strict(
        [((h - l) / c) * 100.0 for h, l, c in zip(highs[-10:-5], lows[-10:-5], closes[-10:-5])],
        name="prev_range_pct",
    )
    if prev_range_pct <= 0.0:
        raise RuntimeError("prev_range_pct must be > 0")

    range_expansion = recent_range_pct / prev_range_pct

    volatility_score = _clamp_0_100(
        (min(atr_pct / 1.20, 1.0) * 40.0)
        + (min(max(atr_expansion - 1.0, 0.0) / 0.80, 1.0) * 30.0)
        + (min(max(range_expansion - 1.0, 0.0) / 0.80, 1.0) * 30.0)
    )

    # ─────────────────────────────────────────
    # Volume regime
    # - 최근 volume / 평균 volume
    # - volume spike
    # ─────────────────────────────────────────
    avg_recent_vol = _mean_strict(volumes[-5:], name="avg_recent_vol")
    avg_base_vol = _mean_strict(volumes[-25:-5], name="avg_base_vol")
    if avg_base_vol <= 0.0:
        raise RuntimeError("avg_base_vol must be > 0")

    volume_ratio = avg_recent_vol / avg_base_vol
    volume_spike = max(volumes[-3:]) / avg_base_vol

    volume_score = _clamp_0_100(
        (min(max(volume_ratio - 1.0, 0.0) / 2.0, 1.0) * 60.0)
        + (min(max(volume_spike - 1.0, 0.0) / 3.0, 1.0) * 40.0)
    )

    # ─────────────────────────────────────────
    # Liquidity regime
    # - high-low wick ratio
    # - candle body ratio
    # - stop hunt 가능성
    # ─────────────────────────────────────────
    body_ratios: List[float] = []
    wick_ratios: List[float] = []
    stop_hunt_count = 0

    for i in range(len(candles_5m) - 10, len(candles_5m)):
        rng = highs[i] - lows[i]
        if rng <= 0.0:
            raise RuntimeError(f"candle range must be > 0 for liquidity at index={i}")

        body = abs(closes[i] - opens[i])
        body_ratio = body / rng
        wick_ratio = max(0.0, 1.0 - body_ratio)

        if not math.isfinite(body_ratio) or not math.isfinite(wick_ratio):
            raise RuntimeError("body_ratio/wick_ratio must be finite")

        body_ratios.append(body_ratio)
        wick_ratios.append(wick_ratio)

        if wick_ratio >= 0.65 and body_ratio <= 0.25:
            stop_hunt_count += 1

    avg_body_ratio = _mean_strict(body_ratios, name="avg_body_ratio")
    avg_wick_ratio = _mean_strict(wick_ratios, name="avg_wick_ratio")
    stop_hunt_ratio = stop_hunt_count / 10.0

    if stop_hunt_ratio < 0.0 or stop_hunt_ratio > 1.0:
        raise RuntimeError("stop_hunt_ratio out of range")

    stop_hunt_score = _clamp_0_100(stop_hunt_ratio * 100.0)
    liquidity_score = _clamp_0_100(
        (avg_body_ratio * 100.0 * 0.50)
        + ((100.0 - (avg_wick_ratio * 100.0)) * 0.25)
        + ((100.0 - stop_hunt_score) * 0.25)
    )

    # ─────────────────────────────────────────
    # Momentum regime
    # - RSI distance
    # - MACD histogram
    # - price momentum
    # ─────────────────────────────────────────
    rsi_series = rsi(closes, rsi_len)
    rsi_last = _last_indicator_value(rsi_series, name="rsi_last")
    if rsi_last < 0.0 or rsi_last > 100.0:
        raise RuntimeError(f"RSI out of range: {rsi_last}")

    ema12 = ema(closes, 12)
    ema26 = ema(closes, 26)
    if not isinstance(ema12, list) or not ema12:
        raise RuntimeError("ema12 missing/empty")
    if not isinstance(ema26, list) or not ema26:
        raise RuntimeError("ema26 missing/empty")
    if len(ema12) != len(ema26):
        raise RuntimeError("ema12/ema26 length mismatch")

    macd_line = [float(a) - float(b) for a, b in zip(ema12, ema26)]
    if not macd_line:
        raise RuntimeError("macd_line missing/empty")

    macd_signal = ema(macd_line, 9)
    if not isinstance(macd_signal, list) or not macd_signal:
        raise RuntimeError("macd_signal missing/empty")

    macd_hist = float(macd_line[-1]) - float(macd_signal[-1])
    if not math.isfinite(macd_hist):
        raise RuntimeError("macd_hist must be finite")

    macd_hist_pct = abs(macd_hist) / last_close * 100.0
    price_mom_pct = abs((closes[-1] - closes[-6]) / closes[-6]) * 100.0

    momentum_score = _clamp_0_100(
        (min(abs(rsi_last - 50.0) / 30.0, 1.0) * 35.0)
        + (min(macd_hist_pct / 0.25, 1.0) * 30.0)
        + (min(price_mom_pct / 1.50, 1.0) * 35.0)
    )

    # ─────────────────────────────────────────
    # Derived range/chop/event risk
    # ─────────────────────────────────────────
    range_score = _clamp_0_100(
        ((100.0 - trend_score) * 0.40)
        + ((100.0 - momentum_score) * 0.25)
        + ((100.0 - volatility_score) * 0.20)
        + (liquidity_score * 0.15)
    )

    chop_score = _clamp_0_100(
        (volatility_score * 0.40)
        + ((100.0 - trend_score) * 0.20)
        + ((100.0 - liquidity_score) * 0.25)
        + (volume_score * 0.15)
    )

    # 외부 뉴스 데이터는 없으므로, 내부 shock/event proxy 로 계산
    event_risk_score = _clamp_0_100(
        (volatility_score * 0.50)
        + ((100.0 - liquidity_score) * 0.30)
        + (volume_score * 0.20)
    )

    regime_total_score = _clamp_0_100(
        (trend_score * 0.30)
        + (volatility_score * 0.20)
        + (volume_score * 0.15)
        + (liquidity_score * 0.15)
        + (momentum_score * 0.20)
    )

    if not math.isfinite(regime_total_score):
        raise RuntimeError("regime_total_score must be finite")

    # 최종 레짐 결정 (요청한 규칙 유지)
    if trend_score >= 60.0:
        final_regime = "TREND"
    elif range_score >= 60.0:
        final_regime = "RANGE"
    elif volatility_score >= 60.0 and trend_score < 40.0:
        final_regime = "CHOP"
    else:
        final_regime = "NEUTRAL"

    return (
        float(trend_score),
        float(range_score),
        float(chop_score),
        float(event_risk_score),
        final_regime,
    )


# ─────────────────────────────────────────────
# 단일 사이클: DB 읽고 RegimeScore 저장
# ─────────────────────────────────────────────
def _analyze_and_store_once() -> None:
    """최근 5m 캔들을 읽어 레짐 점수 및 레짐 피처를 bt_regime_scores 에 한 번 기록."""
    session = SessionLocal()
    try:
        candles_5m = _load_recent_candles(
            session,
            symbol=SYMBOL,
            timeframe=ANALYSIS_TF,
            limit=ANALYSIS_CANDLE_COUNT,
        )
        if not candles_5m:
            log("[SIG-ANALYSIS] no candles in bt_candles yet")
            return

        result = _compute_regime_from_candles(candles_5m)
        if result is None:
            log("[SIG-ANALYSIS] not enough data to compute regime")
            return

        trend_score, range_score, chop_score, event_risk_score, final_regime = result
        ts = candles_5m[-1].ts

        # 동일 ts 에 대해 중복 INSERT 방지
        exists = (
            session.query(RegimeScore)
            .filter(
                RegimeScore.symbol == SYMBOL,
                RegimeScore.timeframe == ANALYSIS_TF,
                RegimeScore.ts == ts,
            )
            .first()
        )
        if exists:
            return

        # DB 캔들을 indicators.Candle 포맷으로 변환하여 레짐 피처 계산
        regime_features: Optional[Dict[str, object]] = None
        try:
            candles_for_ind = [
                (
                    int(c.ts.timestamp() * 1000),
                    float(c.open),
                    float(c.high),
                    float(c.low),
                    float(c.close),
                )
                for c in candles_5m
            ]
            regime_features = build_regime_features_from_candles(candles_for_ind)
        except Exception as e:  # noqa: BLE001
            log(f"[SIG-ANALYSIS] regime_features 계산 실패: {e}")

        row = RegimeScore(
            symbol=SYMBOL,
            timeframe=ANALYSIS_TF,
            ts=ts,
            trend_score=trend_score,
            range_score=range_score,
            chop_score=chop_score,
            event_risk_score=event_risk_score,
            final_regime=final_regime,
        )

        # RegimeScore 에 features_json 컬럼이 있을 때만 JSON 저장 시도
        if regime_features:
            try:
                if hasattr(RegimeScore, "features_json"):
                    # type: ignore[attr-defined]
                    row.features_json = json.dumps(regime_features, ensure_ascii=False)  # noqa: E501
            except Exception as e:  # noqa: BLE001
                log(f"[SIG-ANALYSIS] regime_features 저장 실패: {e}")

        session.add(row)
        session.commit()

        feat_hint = ""
        try:
            if regime_features and isinstance(regime_features, dict):
                hint = regime_features.get("regime_hint")  # type: ignore[index]
                if hint:
                    feat_hint = f" feat={hint}"
        except Exception:
            feat_hint = ""

        log(
            f"[SIG-ANALYSIS] {SYMBOL} {ANALYSIS_TF} ts={ts.isoformat()} "
            f"trend={trend_score:.1f} range={range_score:.1f} "
            f"chop={chop_score:.1f} regime={final_regime}{feat_hint}"
        )

    except SQLAlchemyError as e:
        session.rollback()
        log(f"[SIG-ANALYSIS] DB error: {e}")
    except Exception as e:  # noqa: BLE001
        log(f"[SIG-ANALYSIS] unexpected error: {e}")
    finally:
        session.close()


# ─────────────────────────────────────────────
# 백그라운드 스레드 진입점
# ─────────────────────────────────────────────
def start_signal_analysis_thread(interval_sec: int = 60) -> None:
    """주기적으로 _analyze_and_store_once() 를 호출하는 백그라운드 스레드 시작."""

    def _worker() -> None:
        while True:
            try:
                _analyze_and_store_once()
            except Exception as e:  # noqa: BLE001
                # 어떤 예외도 여기서 잡아서 메인 봇이 죽지 않게 한다.
                log(f"[SIG-ANALYSIS] worker error: {e}")
            time.sleep(interval_sec)

    th = threading.Thread(target=_worker, daemon=True, name="signal-analysis")
    th.start()
    log(
        f"[SIG-ANALYSIS] worker started "
        f"(interval={interval_sec}s, symbol={SYMBOL}, tf={ANALYSIS_TF})"
    )


# ─────────────────────────────────────────────
# 단독 실행용 (__main__)
# ─────────────────────────────────────────────
if __name__ == "__main__":
    # 단독 실행 시에도 동일한 워커를 60초 주기로 돌린다.
    start_signal_analysis_thread(interval_sec=60)
    while True:
        time.sleep(3600)