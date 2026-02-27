"""signal_analysis_worker.py
=====================================================
DB(bt_candles)에 쌓인 웹소켓 캔들을 주기적으로 읽어서
지표(EMA/RSI/ATR)를 계산하고 레짐 점수(bt_regime_scores)에 저장하는 워커.

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
ANALYSIS_CANDLE_COUNT: int = getattr(SET, "analysis_candle_count", 120)


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

    closes = [float(c.close) for c in candles_5m]
    highs = [float(c.high) for c in candles_5m]
    lows = [float(c.low) for c in candles_5m]

    last_close = closes[-1]
    if last_close <= 0:
        return None

    # EMA 기반 트렌드 강도
    ema_fast_len = getattr(SET, "ema_fast_len", 20)
    ema_slow_len = getattr(SET, "ema_slow_len", 50)

    ema_fast_list = ema(closes, ema_fast_len)
    ema_slow_list = ema(closes, ema_slow_len)
    ema_fast = ema_fast_list[-1]
    ema_slow = ema_slow_list[-1]

    ema_diff = ema_fast - ema_slow
    ema_diff_pct = abs(ema_diff) / last_close  # 이평선 괴리율

    # ATR 기반 변동성 (최근 14개 캔들)
    atr_len = getattr(SET, "atr_len", 14)
    tail = candles_5m[-max(atr_len + 1, 15) :]
    atr_input = [
        (
            int(c.ts.timestamp() * 1000),
            float(c.open),
            float(c.high),
            float(c.low),
            float(c.close),
        )
        for c in tail
    ]
    atr_val = calc_atr(atr_input[-(atr_len + 1) :]) if len(atr_input) >= atr_len + 1 else 0.0
    atr_pct = (atr_val / last_close) if last_close > 0 else 0.0

    # 점수 스케일링 (간단한 휴리스틱)
    # - 이평 괴리율이 0.3% 이상이면 강한 트렌드
    # - 0.1% 이하이면 횡보(레인지) 쪽 가중
    trend_raw = min(ema_diff_pct / 0.003, 1.5)  # 0.3% → 1.0, 그 이상은 1.5까지 상한
    trend_score = max(0.0, min(100.0, trend_raw * 100.0))

    # range_score 는 이평이 붙어 있을수록(ema_diff_pct 작을수록) 높게
    range_raw = max(0.0, (0.0025 - ema_diff_pct) / 0.0025)  # 0~1 사이
    range_score = max(0.0, min(100.0, range_raw * 100.0))

    # CHOP(난조장) 점수: 변동성은 있는데 이평 정렬이 애매한 구간
    chop_raw = max(0.0, min(1.0, atr_pct / 0.004)) * max(0.0, 1.0 - trend_raw)
    chop_score = max(0.0, min(100.0, chop_raw * 100.0))

    # 이벤트 리스크는 아직 별도 데이터가 없으므로 0으로 둔다 (향후 뉴스/펀딩 연계 예정)
    event_risk_score = 0.0

    # 최종 레짐 결정 (단순 규칙)
    if trend_score >= 60 and trend_score >= range_score and trend_score >= chop_score:
        final_regime = "TREND"
    elif range_score >= 60 and range_score >= trend_score and range_score >= chop_score:
        final_regime = "RANGE"
    elif chop_score >= 60:
        final_regime = "CHOP"
    else:
        final_regime = "NEUTRAL"

    return trend_score, range_score, chop_score, event_risk_score, final_regime


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
