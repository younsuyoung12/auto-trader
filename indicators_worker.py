# indicators_worker.py
# ====================================================
# BingX Auto Trader - Indicators Worker (Full Spec)
# 개발문서 기준 완전 버전(B)
# ====================================================
# 역할
# - bt_candles 에 저장된 1m/5m/15m 캔들을 읽고 모든 지표 계산
# - EMA, ATR, 거래량, 변동성, range_top/bottom, trend_score, range_score,
#   chop_score, final_regime 까지 전부 계산해 DB 에 저장
# - bt_indicators / bt_regime_scores 테이블에 기록
# - run_bot_ws.py 에서 백그라운드 스레드로 실행됨
# ====================================================
# 주석은 모든 함수에 상세하게 달려 있음
# ====================================================

from __future__ import annotations

import time
import datetime
from typing import List, Dict, Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from db_core import get_session
from db_models import BTCandle, BTIndicator, BTRegimeScore
from indicators import (
    ema,
    atr,
    compute_volatility_score,
    compute_volume_stats,
    detect_range_zones,
)
from settings_ws import load_settings
from telelog import log

SET = load_settings()

# 지표 계산 시 최소 캔들 개수
MIN_CANDLES = 60

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
        return list(reversed(rows))  # 오래된 → 최신 순으로 정렬


# -----------------------------------------------------
# trend_score / range_score / chop_score 계산
# -----------------------------------------------------
def compute_trend_score(ema_dist_norm: float, ema_slope_norm: float, vol_score: float, vol_boost: float) -> float:
    """개발문서에 정의된 가중 조합형 trend_score."""
    return (
        0.4 * abs(ema_dist_norm)
        + 0.3 * ema_slope_norm
        + 0.2 * vol_score
        + 0.1 * vol_boost
    )


def compute_range_score(range_strength: float, range_height: float, volatility_score: float) -> float:
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
def decide_final_regime(tscore: float, rscore: float, thr_t: float = 0.6, thr_r: float = 0.6) -> str:
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
    """
    candles = _load_candles_for(symbol, timeframe, limit=200)
    if len(candles) < MIN_CANDLES:
        return

    closes = [float(c.close) for c in candles]
    highs = [float(c.high) for c in candles]
    lows = [float(c.low) for c in candles]
    volumes = [float(c.volume or 0.0) for c in candles]

    # EMA
    ema_fast = ema(closes, 20)
    ema_slow = ema(closes, 50)
    if ema_fast is None or ema_slow is None:
        return

    ema_dist = (ema_fast - ema_slow) / closes[-1]
    ema_dist_norm = max(min(ema_dist / 0.01, 1), -1)

    ema_prev = ema(closes[:-1], 20)
    ema_slope = 0
    if ema_prev is not None:
        ema_slope = (ema_fast - ema_prev) / closes[-1]
    ema_slope_norm = max(min(ema_slope / 0.002, 1), -1)

    # ATR & Volatility
    atr_val = atr(highs, lows, closes)
    atr_pct = atr_val / closes[-1] if atr_val else 0.0
    vol_score = compute_volatility_score(atr_pct)

    # Volume stats
    vol_ma, vol_ratio, volume_z = compute_volume_stats(volumes)

    # Range detection
    range_top, range_bottom, range_height, range_strength = detect_range_zones(candles)

    # Scores
    trend_score = compute_trend_score(ema_dist_norm, ema_slope_norm, vol_score, vol_ratio)
    range_score = compute_range_score(range_strength, range_height, vol_score)
    chop_score = compute_chop_score(trend_score, range_score)
    final_regime = decide_final_regime(trend_score, range_score)

    # DB 저장
    with get_session() as s:
        ind = BTIndicator(
            symbol=symbol,
            timeframe=timeframe,
            ts=candles[-1].ts,
            ema_fast=ema_fast,
            ema_slow=ema_slow,
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

        regime = BTRegimeScore(
            symbol=symbol,
            ts=candles[-1].ts,
            trend_score=trend_score,
            range_score=range_score,
            chop_score=chop_score,
            final_regime=final_regime,
        )
        s.add(regime)

    log(f"[IND] {symbol} {timeframe} saved regime={final_regime} t={trend_score:.3f} r={range_score:.3f}")


# -----------------------------------------------------
# 워커 스레드 실행
# -----------------------------------------------------
def start_indicators_worker(interval_sec: int = 30) -> None:
    """run_bot_ws.py 에서 백그라운드 스레드로 실행될 메인 엔트리."""
    import threading

    def _runner():
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
