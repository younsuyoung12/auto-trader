from __future__ import annotations

# indicators_worker.py
# ====================================================
# BingX Auto Trader - Indicators Worker (Full Spec)
# 개발문서 기준 완전 버전(B)
# ====================================================
# 역할
# - bt_candles 에 저장된 1m/5m/15m 캔들을 읽고 모든 지표 계산
# - EMA, ATR, 거래량 통계, 변동성 점수, range_top/bottom, trend_score,
#   range_score, chop_score, final_regime 까지 전부 계산해 DB 에 저장
# - bt_indicators / bt_regime_scores 테이블에 기록
# - run_bot_ws.py 에서 백그라운드 스레드로 실행됨
# ====================================================
#
# 2025-12-03 변경 사항 (TA-Lib indicators.py 호환 패치)
# ----------------------------------------------------
# 1) indicators.py 가 TA-Lib 기반으로 리팩터링되면서
#    atr / compute_volatility_score / compute_volume_stats /
#    detect_range_zones 함수가 제거되었으므로,
#    이 워커에서 더 이상 해당 심볼을 임포트하지 않는다.
# 2) 대신 indicators.build_regime_features_from_candles(...) 를 사용해
#    EMA/ATR/range_pct/range_strength 를 계산하고,
#    거래량 통계/변동성 점수/range_top/bottom 은 워커 내부의
#    순수 수치 함수(_compute_volume_stats, _compute_volatility_score,
#    _compute_range_zones)로 다시 구현했다.
# 3) DB 레벨 의미는 최대한 유지:
#    - ema_fast / ema_slow / ema_dist / ema_slope
#    - atr / atr_pct
#    - volatility_score (ATR% 기반 0~1 정규화 점수)
#    - vol_ma / vol_ratio / volume_z
#    - range_top / range_bottom / range_height / range_strength
#    - trend_score / range_score / chop_score / final_regime
#    를 계속 동일 테이블(BTIndicator/BTRegimeScore)에 저장한다.
# 4) WS / REST 를 다시 호출하거나, 부족한 데이터를 임의 보정하는
#    폴백 로직은 추가하지 않는다. DB bt_candles 가 부족하거나
#    비정상이면 단순히 해당 주기 계산을 건너뛴다.
# ====================================================

import math
import time
from typing import List, Dict, Any, Optional, Tuple

from sqlalchemy import select

from db_core import get_session
from db_models import BTCandle, BTIndicator, BTRegimeScore
from indicators import (
    Candle,
    build_regime_features_from_candles,
)
from settings_ws import load_settings
from telelog import log

SET = load_settings()

# 지표 계산 시 최소 캔들 개수
MIN_CANDLES = 60

# 레짐/통계 계산에 사용하는 기본 파라미터
EMA_FAST_LEN = 20
EMA_SLOW_LEN = 50
ATR_LEN = 14
RANGE_WINDOW = 50
VOLUME_MA_LEN = 20


# -----------------------------------------------------
# 내부 유틸
# -----------------------------------------------------
def _load_candles_for(symbol: str, timeframe: str, limit: int = 200) -> List[BTCandle]:
    """DB 의 bt_candles 에서 최근 limit 개 캔들을 timeframe 기준으로 조회."""
    with get_session() as s:
        q = (
            select(BTCandle)
            .where(
                BTCandle.symbol == symbol,
                BTCandle.timeframe == timeframe,
            )
            .order_by(BTCandle.ts.desc())
            .limit(limit)
        )
        rows = list(s.execute(q).scalars())
        # DB 에서는 최신 → 오래된 순으로 가져오므로
        # 계산 편의를 위해 오래된 → 최신 순으로 뒤집어서 반환한다.
        return list(reversed(rows))


def _bt_candles_to_indicator_candles(candles: List[BTCandle]) -> List[Candle]:
    """BTCandle 리스트 → indicators.Candle 리스트로 변환.

    Candle 타입은 (ts_ms, open, high, low, close) 튜플이다.
    """
    out: List[Candle] = []
    for c in candles:
        try:
            out.append(
                (
                    int(c.ts),
                    float(c.open),
                    float(c.high),
                    float(c.low),
                    float(c.close),
                )
            )
        except Exception as e:
            log(f"[IND] candle cast error ts={getattr(c, 'ts', None)}: {e}")
            # 데이터에 이상이 있으면 전체 계산을 포기하고 상위에서 건너뛰도록 한다.
            return []
    return out


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _compute_volatility_score(atr_pct: float) -> float:
    """ATR% 기반 단순 변동성 점수(0~1)를 계산한다.

    - atr_pct 는 ATR / price 비율 (예: 0.004 = 0.4%) 이다.
    - 0% 일 때 0.0, 1% 이상이면 1.0 으로 포화되도록 선형 스케일링한다.
      (초단기 파생상품 기준, 0.3~1.0% 구간을 '의미 있는 변동성'으로 본다.)
    """
    if not isinstance(atr_pct, (int, float)) or not math.isfinite(atr_pct) or atr_pct <= 0:
        return 0.0
    # 1% (0.01) 를 기준으로 0~1 스케일링
    return _clamp(atr_pct / 0.01, 0.0, 1.0)


def _compute_volume_stats(volumes: List[float], ma_len: int = VOLUME_MA_LEN) -> Tuple[float, float, float]:
    """거래량 통계를 계산한다.

    반환:
        (vol_ma, vol_ratio, volume_z)

        - vol_ma     : 최근 ma_len 구간 평균 거래량
        - vol_ratio  : 최근 봉 거래량 / vol_ma
        - volume_z   : z-score (평균 대비 표준편차 기준 몇 배인지)
    """
    if not volumes:
        return 0.0, 0.0, 0.0

    last = float(volumes[-1])

    if len(volumes) < ma_len:
        window = [float(v) for v in volumes]
    else:
        window = [float(v) for v in volumes[-ma_len:]]

    if not window:
        return 0.0, 0.0, 0.0

    mean = sum(window) / len(window)
    if len(window) > 1:
        var = sum((v - mean) ** 2 for v in window) / len(window)
        std = math.sqrt(var)
    else:
        std = 0.0

    vol_ma = mean
    vol_ratio = last / mean if mean > 0 else 0.0
    volume_z = (last - mean) / std if std > 0 else 0.0

    return vol_ma, vol_ratio, volume_z


def _compute_range_zones(
    candles: List[BTCandle],
    *,
    window: int = RANGE_WINDOW,
) -> Tuple[float, float, float]:
    """최근 window 구간 기준 단순 range_top/bottom/height 를 계산한다.

    - range_top    : 최근 window 개 봉 중 최고가
    - range_bottom : 최근 window 개 봉 중 최저가
    - range_height : (range_top - range_bottom) / 마지막 종가

    detect_range_zones 의 하드 버전을 완전히 재현할 수는 없지만,
    DB 에 저장되는 값의 의미(최근 구간 박스 폭)를 유지하기 위해
    최소한의 일관된 정의를 사용한다.
    """
    if not candles:
        return 0.0, 0.0, 0.0

    if len(candles) < window:
        sub = candles
    else:
        sub = candles[-window:]

    highs = [float(c.high) for c in sub]
    lows = [float(c.low) for c in sub]
    closes = [float(c.close) for c in candles]

    if not highs or not lows or not closes:
        return 0.0, 0.0, 0.0

    range_top = max(highs)
    range_bottom = min(lows)
    last_close = float(closes[-1])

    if last_close <= 0:
        range_height = 0.0
    else:
        range_height = (range_top - range_bottom) / last_close

    return range_top, range_bottom, range_height


# -----------------------------------------------------
# trend_score / range_score / chop_score 계산
# -----------------------------------------------------
def compute_trend_score(
    ema_dist_norm: float,
    ema_slope_norm: float,
    vol_score: float,
    vol_boost: float,
) -> float:
    """개발문서에 정의된 가중 조합형 trend_score."""
    return (
        0.4 * abs(ema_dist_norm)
        + 0.3 * ema_slope_norm
        + 0.2 * vol_score
        + 0.1 * vol_boost
    )


def compute_range_score(
    range_strength: float,
    range_height: float,
    volatility_score: float,
) -> float:
    """range_strength + 보조 요소 조합."""
    return (
        0.5 * range_strength
        + 0.3 * (1 - volatility_score)
        + 0.2 * (1 - abs(range_height))
    )


def compute_chop_score(trend_score: float, range_score: float) -> float:
    """둘 다 애매하면 chop_score 증가."""
    t = abs(trend_score)
    r = abs(range_score)
    return max(0.0, 1.0 - t - r)


# -----------------------------------------------------
# final regime 결정
# -----------------------------------------------------
def decide_final_regime(
    tscore: float,
    rscore: float,
    thr_t: float = 0.6,
    thr_r: float = 0.6,
) -> str:
    if tscore > thr_t and tscore > rscore:
        return "TREND"
    if rscore > thr_r:
        return "RANGE"
    return "NO_TRADE"


# -----------------------------------------------------
# main 계산 로직
# -----------------------------------------------------
def compute_indicators_for_tf(symbol: str, timeframe: str) -> None:
    """
    하나의 타임프레임(예: 5m)에 대해 모든 지표 계산 후 DB 기록.

    - bt_candles 에 최소 MIN_CANDLES 개 이상 존재할 때만 계산한다.
    - DB 시세 데이터에 이상(가격<=0, 캐스팅 오류 등)이 있으면
      해당 타임프레임 계산은 건너뛴다.
    """
    candles = _load_candles_for(symbol, timeframe, limit=200)
    if len(candles) < MIN_CANDLES:
        return

    # indicators.py 에서 사용하는 Candle 포맷으로 변환
    ind_candles = _bt_candles_to_indicator_candles(candles)
    if not ind_candles:
        # 변환 실패 시 계산을 건너뛴다.
        return

    closes = [float(c.close) for c in candles]
    highs = [float(c.high) for c in candles]
    lows = [float(c.low) for c in candles]
    volumes = [float(c.volume or 0.0) for c in candles]

    # 가격이 비정상(<=0)인 캔들이 섞여 있으면 계산을 중단한다.
    if any((h <= 0 or l <= 0 or cl <= 0) for h, l, cl in zip(highs, lows, closes)):
        log(f"[IND] skip {symbol} {timeframe}: non-positive price in bt_candles")
        return

    # ------------------------------------------------------------------
    # 1) indicators.build_regime_features_from_candles 를 통해
    #    EMA/ATR/range_pct/range_strength 등 핵심 피처를 한 번에 계산.
    # ------------------------------------------------------------------
    regime = build_regime_features_from_candles(
        ind_candles,
        fast_ema_len=EMA_FAST_LEN,
        slow_ema_len=EMA_SLOW_LEN,
        atr_len=ATR_LEN,
        rsi_len=14,
        range_window=RANGE_WINDOW,
    )
    if regime is None:
        # 캔들 수 부족 또는 내장 검증 실패
        log(f"[IND] skip {symbol} {timeframe}: regime_features is None")
        return

    last_close = float(regime.get("last_close", closes[-1]))

    ema_fast_val = float(regime.get("ema_fast", math.nan))
    ema_slow_val = float(regime.get("ema_slow", math.nan))
    ema_dist_pct = float(regime.get("ema_dist_pct", math.nan))
    ema_slope_pct = float(regime.get("ema_fast_slope_pct", math.nan))

    atr_val = regime.get("atr")
    atr_val = float(atr_val) if isinstance(atr_val, (int, float)) else math.nan
    atr_pct = float(regime.get("atr_pct", math.nan))
    range_pct = float(regime.get("range_pct", math.nan))
    range_strength = float(regime.get("range_strength", 0.0))

    # regime_features 가 NaN 을 반환한 경우 방어적으로 계산을 중단
    if any(
        math.isnan(x)
        for x in (
            ema_fast_val,
            ema_slow_val,
            ema_dist_pct,
            ema_slope_pct,
            atr_pct,
            range_pct,
        )
    ):
        log(f"[IND] skip {symbol} {timeframe}: NaN in regime_features")
        return

    # ------------------------------------------------------------------
    # 2) 파생 지표 계산 (노말라이즈 / 거래량 / range_top/bottom)
    # ------------------------------------------------------------------
    # EMA 괴리/기울기 노말라이즈
    ema_dist = ema_dist_pct
    ema_dist_norm = _clamp(ema_dist_pct / 0.01, -1.0, 1.0)

    ema_slope = ema_slope_pct
    ema_slope_norm = _clamp(ema_slope_pct / 0.002, -1.0, 1.0)

    # ATR 기반 변동성 점수
    vol_score = _compute_volatility_score(atr_pct)

    # 거래량 통계
    vol_ma, vol_ratio, volume_z = _compute_volume_stats(volumes, ma_len=VOLUME_MA_LEN)

    # 단순 range_top/bottom/height
    range_top, range_bottom, range_height = _compute_range_zones(
        candles,
        window=RANGE_WINDOW,
    )

    # ------------------------------------------------------------------
    # 3) trend_score / range_score / chop_score / final_regime 계산
    # ------------------------------------------------------------------
    trend_score = compute_trend_score(
        ema_dist_norm=ema_dist_norm,
        ema_slope_norm=ema_slope_norm,
        vol_score=vol_score,
        vol_boost=vol_ratio,
    )
    range_score = compute_range_score(
        range_strength=range_strength,
        range_height=range_height,
        volatility_score=vol_score,
    )
    chop_score = compute_chop_score(trend_score, range_score)
    final_regime = decide_final_regime(trend_score, range_score)

    # ------------------------------------------------------------------
    # 4) DB 저장
    # ------------------------------------------------------------------
    with get_session() as s:
        ind = BTIndicator(
            symbol=symbol,
            timeframe=timeframe,
            ts=candles[-1].ts,
            ema_fast=ema_fast_val,
            ema_slow=ema_slow_val,
            ema_dist=ema_dist,
            ema_slope=ema_slope,
            atr=atr_val,
            atr_pct=atr_pct,
            volatility_score=vol_score,
            vol_ma=vol_ma,
            vol_ratio=vol_ratio,
            volume_z=volume_z,
            range_top=range_top,
            range_bottom=range_bottom,
            range_height=range_height,
            range_strength=range_strength,
        )
        s.add(ind)
        s.flush()

        regime_row = BTRegimeScore(
            symbol=symbol,
            ts=candles[-1].ts,
            trend_score=trend_score,
            range_score=range_score,
            chop_score=chop_score,
            final_regime=final_regime,
        )
        s.add(regime_row)

    log(
        "[IND] {sym} {tf} saved regime={reg} t={t:.3f} r={r:.3f}".format(
            sym=symbol,
            tf=timeframe,
            reg=final_regime,
            t=trend_score,
            r=range_score,
        )
    )


# -----------------------------------------------------
# 워커 스레드 실행
# -----------------------------------------------------
def start_indicators_worker(interval_sec: int = 30) -> None:
    """run_bot_ws.py 에서 백그라운드 스레드로 실행될 메인 엔트리."""
    import threading

    def _runner() -> None:
        symbol = SET.symbol
        while True:
            try:
                for tf in ["1m", "5m", "15m"]:
                    compute_indicators_for_tf(symbol, tf)
            except Exception as e:
                log(f"[IND] error: {e}")
            time.sleep(interval_sec)

    th = threading.Thread(target=_runner, name="ind-worker", daemon=True)
    th.start()
    log("[IND] indicators worker started")


__all__ = [
    "start_indicators_worker",
    "compute_indicators_for_tf",
]
