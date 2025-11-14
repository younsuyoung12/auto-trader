# db_writer.py
# BingX Auto Trader - 분석용 DB 쓰기 전용 헬퍼
#
# - bt_indicators      : 지표(EMA, ATR, 거래량 등)
# - bt_regime_scores   : TREND / RANGE 레짐 점수
# - bt_trades          : 실제 진입/청산 내역
# - bt_entry_scores    : 그때 사용된 점수/컴포넌트 스냅샷
#
# 다른 모듈에서는 이 파일만 import 해서 사용하면 된다.

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from db_core import get_session
from db_models import Indicator, RegimeScore, Trade, EntryScore


# ─────────────────────────────
# 공통: ms → UTC DateTime 변환
# ─────────────────────────────
def _ms_to_utc(ts_ms: int) -> datetime:
    """
    BingX 캔들/체결 timestamp(ms)를 UTC datetime 으로 변환.

    예)
        ts_ms = 1763142300000  →  2025-11-14 18:45:00+00:00
    """
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)


# ─────────────────────────────
# 1) bt_indicators 저장
# ─────────────────────────────
def save_indicator_row(
    symbol: str,
    timeframe: str,
    ts_ms: int,
    fields: Dict[str, Any],
) -> None:
    """
    지표 한 줄을 bt_indicators 에 저장한다.

    - fields 는 Indicator 모델의 컬럼 이름을 key 로 쓰면 된다.
      예:
        fields = {
          "ema_fast":  ..., "ema_slow": ...,
          "ema_dist":  ...,
          "atr":       ..., "atr_pct": ...,
          "volume_ma": ..., "volume_ratio": ...,
          "range_top": ..., "range_bottom": ..., "range_flag": True,
          "extra_json": {...}
        }

    - 캔들 ts 는 보통 "해당 봉의 마감 시각" ms 단위로 넣으면 된다.
    """
    ts = _ms_to_utc(ts_ms)

    # Indicator 모델에 없는 key 는 자동으로 버린다.
    allowed_keys = {
        "ema_fast",
        "ema_slow",
        "ema_dist",
        "ema_slope",
        "atr",
        "atr_pct",
        "volatility_score",
        "volume_ma",
        "volume_ratio",
        "volume_z",
        "range_top",
        "range_bottom",
        "range_height",
        "range_flag",
        "extra_json",
    }
    data = {k: v for k, v in fields.items() if k in allowed_keys}

    with get_session() as s:
        row = Indicator(
            symbol=symbol,
            timeframe=timeframe,
            ts=ts,
            **data,
        )
        s.add(row)
        # commit 은 get_session() 컨텍스트가 끝날 때 자동


# ─────────────────────────────
# 2) bt_regime_scores 저장
# ─────────────────────────────
def save_regime_score_row(
    symbol: str,
    ts_ms: int,
    *,
    trend_score: Optional[float] = None,
    range_score: Optional[float] = None,
    chop_score: Optional[float] = None,
    event_risk_score: Optional[float] = None,
    funding_bias_score: Optional[float] = None,
    final_regime: str,
    details_json: Optional[Dict[str, Any]] = None,
) -> None:
    """
    TREND / RANGE / NO_TRADE 레짐 점수를 bt_regime_scores 에 저장한다.

    호출 예:
        save_regime_score_row(
            symbol="BTC-USDT",
            ts_ms=last_15m_close_ts_ms,
            trend_score=0.8,
            range_score=0.1,
            chop_score=0.1,
            final_regime="TREND",
            details_json={"reason": "...", "features": {...}},
        )
    """
    ts = _ms_to_utc(ts_ms)

    with get_session() as s:
        row = RegimeScore(
            symbol=symbol,
            ts=ts,
            trend_score=trend_score,
            range_score=range_score,
            chop_score=chop_score,
            event_risk_score=event_risk_score,
            funding_bias_score=funding_bias_score,
            final_regime=final_regime,
            details_json=details_json,
        )
        s.add(row)


# ─────────────────────────────
# 3) 트레이드 + EntryScore 저장
# ─────────────────────────────
def create_trade_with_entry_score(
    *,
    symbol: str,
    side: str,                    # "BUY" / "SELL"
    entry_ts_ms: int,
    entry_price: float,
    qty: float,
    is_auto: bool = True,
    regime_at_entry: Optional[str] = None,   # TREND / RANGE / NO_TRADE 등
    trend_score_at_entry: Optional[float] = None,
    range_score_at_entry: Optional[float] = None,
    strategy: Optional[str] = None,          # "TREND" / "RANGE" 등
    leverage: Optional[float] = None,
    risk_pct: Optional[float] = None,
    tp_pct: Optional[float] = None,
    sl_pct: Optional[float] = None,
    entry_score: Optional[float] = None,
    entry_components: Optional[Dict[str, Any]] = None,
    regime_label: Optional[str] = None,      # RegimeScore.final_regime 과 같은 값 등
) -> int:
    """
    진입이 확정되었을 때 bt_trades + bt_entry_scores 를 같이 생성한다.

    반환값:
        생성된 Trade.id (나중에 디버깅용으로 볼 수 있음)

    호출 예:
        trade_id = create_trade_with_entry_score(
            symbol="BTC-USDT",
            side="BUY",
            entry_ts_ms=entry_ts_ms,
            entry_price=entry_price,
            qty=qty,
            is_auto=True,
            regime_at_entry="TREND",
            trend_score_at_entry=0.8,
            range_score_at_entry=0.1,
            strategy="TREND",
            leverage=10,
            risk_pct=0.3,
            tp_pct=0.02,
            sl_pct=0.006,
            entry_score=72.5,
            entry_components={
                "trend_gap_ratio": 0.0042,
                "range_width_ratio": 0.0015,
                "atr_fast": 120.5,
                "atr_slow": 80.3,
            },
            regime_label="TREND",
        )
    """
    entry_ts = _ms_to_utc(entry_ts_ms)

    with get_session() as s:
        trade = Trade(
            symbol=symbol,
            side=side,
            entry_ts=entry_ts,
            entry_price=entry_price,
            qty=qty,
            is_auto=is_auto,
            regime_at_entry=regime_at_entry,
            entry_score=entry_score,
            trend_score_at_entry=trend_score_at_entry,
            range_score_at_entry=range_score_at_entry,
            strategy=strategy,
            leverage=leverage,
            risk_pct=risk_pct,
            tp_pct=tp_pct,
            sl_pct=sl_pct,
        )
        s.add(trade)
        s.flush()  # trade.id 채우기

        if entry_score is not None:
            es = EntryScore(
                symbol=symbol,
                ts=entry_ts,
                side=side,
                signal_type=strategy,
                regime_at_entry=regime_label or regime_at_entry,
                entry_score=entry_score,
                components_json=entry_components,
                used_for_entry=True,
                trade=trade,
            )
            s.add(es)

        # with get_session() 컨텍스트 끝나면 commit
        trade_id = trade.id

    return trade_id


def close_latest_open_trade(
    *,
    symbol: str,
    side: str,
    exit_ts_ms: int,
    exit_price: float,
    close_reason: str,                    # "TP" / "SL" / "EARLY_TP" / ...
    pnl_usdt: Optional[float] = None,
    pnl_pct_futures: Optional[float] = None,
    pnl_pct_spot_ref: Optional[float] = None,
    regime_at_exit: Optional[str] = None,
) -> None:
    """
    아직 청산되지 않은 가장 최근 트레이드를 찾아서 마감 정보를 채운다.

    - 우리 봇은 포지션 1개만 운용하므로
      symbol + side 로 exit_ts IS NULL 인 마지막 한 개만 잡으면 된다.
    """
    exit_ts = _ms_to_utc(exit_ts_ms)

    with get_session() as s:
        trade: Optional[Trade] = (
            s.query(Trade)
            .filter(
                Trade.symbol == symbol,
                Trade.side == side,
                Trade.exit_ts.is_(None),
            )
            .order_by(Trade.entry_ts.desc())
            .first()
        )

        if not trade:
            # 이상 상황 (이미 닫혀 있거나 DB에 안 들어간 경우)
            # 굳이 예외는 안 터뜨리고 그냥 무시해도 된다.
            return

        trade.exit_ts = exit_ts
        trade.exit_price = exit_price
        trade.close_reason = close_reason
        trade.regime_at_exit = regime_at_exit
        trade.pnl_usdt = pnl_usdt
        trade.pnl_pct_futures = pnl_pct_futures
        trade.pnl_pct_spot_ref = pnl_pct_spot_ref
        # updated_at 은 onupdate 로 자동 변경
