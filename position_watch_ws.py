# position_watch_ws.py
# ====================================================
# 웹소켓으로 받은 1m/5m/15m 캔들을 기준으로 포지션을 감시해서
# 조기익절, 조기청산, 박스→추세 전환, 반대 시그널 강제 청산을 수행하는 모듈.
#
# 2025-11-14 패치 (6단계: trades 테이블 연동 + 폴백 금지)
# ----------------------------------------------------
# A) 포지션 종료 시 bt_trades 테이블과 상태를 묶어서 저장
#    - 조기익절 / 조기손절 / 업그레이드(박스→추세) / 다운그레이드(추세→박스)
#      / 반대 시그널 강제 청산에 대해 Trade ORM 레코드 업데이트 시도
#    - trade.db_id / trade.id / trade.trade_id 중 하나라도 없으면
#      절대 임의 PK 로 추정/생성하지 않고, 로그만 남기고 DB 업데이트를 건너뜀
#    - 종료 가격, 종료 시각(ms), 실현 PnL, PnL% 를 bt_trades 에 반영
#    - close_reason 은 기존 log_signal 에 쓰는 값과 동일 문자열 사용
#    - note 필드에는 runtime_close=<reason> 형태로 힌트 남김
#
# B) 폴백 금지 방침
#    - DB 연동에 필요한 데이터(세션, ORM, trade 식별자, 가격/수량/타임스탬프)가
#      하나라도 빠지면 해당 포지션 종료에 대한 DB 업데이트는 "완전히" 생략
#    - 이때도 포지션 청산(위험 관리) 자체는 그대로 수행
#    - 모든 스킵/에러는 log(...) 로 Render 로그에 남김
#
# C) 기존 2025-11-14 / 2025-11-13 변경사항은 그대로 유지
#    - RANGE/TREND 업/다운그레이드, 1m 확인, volume collapse 완화 트리거 등
#
# 2025-11-14 패치 (이전 내용)
# ----------------------------------------------------
# A) TREND→RANGE 다운그레이드(갈아타기) 로직 추가
#    - `maybe_downgrade_trend_to_range(...)` 공개 함수 추가
#    - 조건: settings.trend_to_range_enable 이고, RANGE 신호가 나오며,
#            PnL이 소폭(±trend_to_range_max_abs_pnl_pct) 범위이거나
#            소정의 최소이익(trend_to_range_min_gain_pct) 이상일 때
#    - RANGE 전용 1m 확인 토글 반영(`enable_1m_confirm_range` → 없으면 `enable_1m_confirm`)
#    - soft block 시 TP/SL 완화(0.7 배) 동일 적용
#    - auto re-enter 옵션(`trend_to_range_auto_reenter`)
#      * 가능한 경우 `trader.open_position_with_tp_sl` 사용 (없으면 스킵하고 로그/알림만)
#
# B) RANGE 전용 1m 확인 토글 보조 헬퍼 추가
#    - `_is_range_1m_confirm_enabled(settings)`
#
# C) "동시 평가+중재"(signal_flow_ws) 출력 표준화 대응
#    - get_trading_signal() 이 방향을 BUY/SELL 로 반환하고 소스는 "TREND"/"RANGE"/"HYBRID" 일 수 있음
#    - 본 모듈에서 방향은 내부 비교 시 LONG/SHORT 로 정규화해 사용
#    - 업그레이드(박스→추세) 판단 시 HYBRID 도 추세 계열로 인정
#    - 반대 시그널 비교(opposite)도 BUY/SELL/LONG/SHORT 혼용 안전 처리
#
# D) 추세 조기청산/익절에서 거래량 급감+납작 구간일 때 트리거 완화
#    - _volume_collapse + _all_candles_narrow 동시 만족 시 loss 트리거를 0.6배로 낮춰 더 기민하게 청산
#    - 감소 비율은 settings.min_entry_volume_ratio (없으면 0.15)
#
# 2025-11-13 변경 사항
# ----------------------------------------------------
# 1) 기존 코드가 3m(=settings.interval) 기준으로 캔들을 가져오던 부분을 모두 5m 기준으로 변경.
# 2) 캔들 소스는 REST 가 아니라 `market_data_ws` 에서 웹소켓으로 수신한 버퍼를 읽는다.
# 3) 15m 추세 확인은 `strategies_trend_ws.decide_trend_15m` 를 사용하도록 변경.
# 4) 재시그널 확인은 `signal_flow_ws.get_trading_signal` 을 사용하도록 변경.
# 5) Render 콘솔에서 실시간으로 5m/15m/1m 캔들이 잘 들어오는지 확인할 수 있도록 각 분기마다 log(...)를 추가했다.
# 6) 주문/TP/SL 은 기존대로 `exchange_api.close_position_market(...)` 를 통해 REST 로 처리한다.
#
# 2025-11-13 추가 보정 (volume 사용)
# ----------------------------------------------------
# 7) market_data_ws 의 get_klines_with_volume(...) 을 사용해서 ws 캔들에 들어있는 거래량도 같이 읽는다.
# 8) log_candle_snapshot(...) 에 실제 volume 을 넣어서 나중에 CSV 로 볼 때 “이게 거래 없는 구간이었는지”를 알 수 있게 했다.
# 9) 추세 횡보 조기청산에서 “가격이 납작 + 거래량도 직전 평균대비 크게 줄었다”면 더 적극적으로 청산하도록 했다.
#    - 이때 감소 비율은 settings.min_entry_volume_ratio 를 재활용한다. (없으면 0.15 로 본다)

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, List, Tuple, Optional

from telelog import log, send_tg
# ⬇️ volume 있는 버전으로 가져온다
from market_data_ws import (
    get_klines as ws_get_klines,
    get_klines_with_volume as ws_get_klines_with_vol,
)
from exchange_api import close_position_market
from signal_flow_ws import get_trading_signal  # WS 기반 시그널 (동시 평가 + 중재 버전)
from strategies_trend_ws import decide_trend_15m, confirm_1m_direction  # WS 기반 15m 추세 + 1m 확인
from signals_logger import log_signal, log_candle_snapshot
from trader import Trade

# RANGE 전략 관련 (구/신 버전 모두 대응)
try:
    from strategies_range_ws import (
        decide_signal_range,
        compute_range_params,
        should_block_range_today_with_level,
    )
except Exception:
    try:
        from strategies_range_ws import decide_signal_range, should_block_range_today_with_level  # type: ignore
        compute_range_params = None  # type: ignore
    except Exception:
        from strategies_range_ws import decide_signal_range  # type: ignore
        compute_range_params = None  # type: ignore
        should_block_range_today_with_level = None  # type: ignore

# 재진입용(있을 때만 사용)
try:
    from trader import open_position_with_tp_sl  # type: ignore
except Exception:
    open_position_with_tp_sl = None  # type: ignore

# DB 연동 (bt_trades)
try:
    from db_core import SessionLocal  # type: ignore
    from db_models import Trade as TradeORM  # type: ignore
except Exception:  # pragma: no cover - DB 미준비 환경 방어
    SessionLocal = None  # type: ignore
    TradeORM = None  # type: ignore


# ─────────────────────────────────────────
# 공통 헬퍼 (방향/트렌드/볼륨 등)
# ─────────────────────────────────────────

def _get_15m_trend_dir(symbol: str) -> str:
    """웹소켓 버퍼에서 15m 캔들을 가져와 대략적인 방향만 문자열로 돌려준다."""
    log(f"[PW] (WS) fetch 15m for trend dir symbol={symbol}")
    try:
        candles_15m = ws_get_klines(symbol, "15m", 120)
        if not candles_15m:
            return ""
        trend_dir = decide_trend_15m(candles_15m)
        return trend_dir or ""
    except Exception as e:
        log(f"[15m DIR WS] error: {e}")
        return ""


def _is_price_near_entry(trade: Trade, last_price: float, max_abs_pnl_pct: float) -> bool:
    if trade.entry <= 0 or last_price <= 0:
        return False
    pnl_pct = (last_price - trade.entry) / trade.entry
    if trade.side == "SELL":
        pnl_pct = -pnl_pct
    return abs(pnl_pct) <= max_abs_pnl_pct


def _all_candles_narrow(candles: List[Tuple], need_bars: int, max_range_pct: float) -> bool:
    """candles 는 (ts,o,h,l,c) 또는 (ts,o,h,l,c,v) 둘 다 허용."""
    if not candles or len(candles) < need_bars:
        return False
    for c in candles[-need_bars:]:
        high = float(c[2])
        low = float(c[3])
        close = float(c[4])
        if close <= 0:
            return False
        rng_pct = (high - low) / close
        if rng_pct > max_range_pct:
            return False
    return True


def _volume_collapse(candles_with_vol: List[Tuple], need_bars: int, ratio: float) -> bool:
    """가장 최근 봉의 거래량이 직전 봉들 평균보다 ratio 배 미만이면 True.

    candles_with_vol: (ts,o,h,l,c,v)
    """
    if not candles_with_vol or len(candles_with_vol) < need_bars:
        return False
    latest_v = float(candles_with_vol[-1][5])
    prev = candles_with_vol[-(need_bars): -1]
    prev_vols = [float(c[5]) for c in prev if len(c) >= 6]
    if not prev_vols:
        return False
    avg_prev = sum(prev_vols) / len(prev_vols)
    if avg_prev <= 0:
        return False
    return latest_v < avg_prev * ratio


def _is_range_1m_confirm_enabled(settings: Any) -> bool:
    """RANGE에서 1분 확인을 사용할지 결정.

    - enable_1m_confirm_range 가 있으면 그 값을 우선
    - 없으면 기존 enable_1m_confirm 값 사용(하위호환)
    """
    return bool(getattr(settings, "enable_1m_confirm_range", getattr(settings, "enable_1m_confirm", False)))


def _norm_dir(d: str) -> str:
    """BUY/SELL/LONG/SHORT 혼용을 LONG/SHORT 로 정규화."""
    if not d:
        return ""
    d = d.upper()
    if d == "BUY":
        return "LONG"
    if d == "SELL":
        return "SHORT"
    if d in {"LONG", "SHORT"}:
        return d
    return ""


# ─────────────────────────────────────────
# DB 연동 헬퍼 (bt_trades 업데이트, 폴백 금지)
# ─────────────────────────────────────────

def _get_trade_db_id(trade: Trade) -> Optional[int]:
    """Trade 객체에서 ORM pk 후보를 가져온다.

    - db_id → id → trade_id 순으로 확인
    - 어떤 것도 없으면 None (이 경우 DB 업데이트는 건너뜀)
    """
    for attr in ("db_id", "id", "trade_id"):
        try:
            val = getattr(trade, attr, None)
        except Exception:
            val = None
        if isinstance(val, int):
            return val
    return None


def _update_trade_close_in_db(
    *,
    trade: Trade,
    close_price: float,
    close_reason: str,
    event_ts_ms: Optional[int],
    pnl: Optional[float],
    cooldown_tag: Optional[str] = None,
) -> None:
    """bt_trades 레코드에 종료 정보 반영.

    폴백 금지 정책:
    - SessionLocal/TradeORM 없음 → 로그만 남기고 종료
    - trade db_id/id 없음 → 로그만 남기고 종료
    - 가격/수량/타임스탬프 누락 → 로그만 남기고 종료 (추정값 사용 금지)
    """
    if SessionLocal is None or TradeORM is None:
        log("[TRADE_DB] SessionLocal/TradeORM 없음 → trades 업데이트 생략 (no fallback)")
        return

    trade_db_id = _get_trade_db_id(trade)
    if trade_db_id is None:
        log("[TRADE_DB] trade 객체에 db_id/id/trade_id 없음 → trades 업데이트 생략 (no fallback)")
        return

    if close_price <= 0 or getattr(trade, "entry", 0.0) <= 0 or getattr(trade, "qty", 0.0) <= 0:
        log(
            f"[TRADE_DB] invalid price/entry/qty for trade_id={trade_db_id} "
            f"entry={getattr(trade, 'entry', None)} close={close_price} qty={getattr(trade, 'qty', None)} → 업데이트 생략"
        )
        return

    if event_ts_ms is None:
        log(f"[TRADE_DB] event_ts_ms 없음 → trade_id={trade_db_id} 업데이트 생략 (no fallback)")
        return

    try:
        session = SessionLocal()
    except Exception as e:  # pragma: no cover - 세션 생성 실패 방어
        log(f"[TRADE_DB] Session 생성 실패 trade_id={trade_db_id}: {e}")
        return

    try:
        orm: Optional[TradeORM] = session.get(TradeORM, trade_db_id)  # type: ignore[arg-type]
        if orm is None:
            log(f"[TRADE_DB] TradeORM 레코드 없음 id={trade_db_id} → 업데이트 생략")
            return

        exit_dt = datetime.fromtimestamp(event_ts_ms / 1000.0, tz=timezone.utc)

        # PnL 계산 (없으면 여기서 계산)
        real_pnl: float
        if isinstance(pnl, (int, float)):
            real_pnl = float(pnl)
        else:
            if trade.side == "BUY":
                real_pnl = (close_price - float(trade.entry)) * float(trade.qty)
            else:
                real_pnl = (float(trade.entry) - close_price) * float(trade.qty)

        base_notional = float(trade.entry) * float(trade.qty)
        pnl_pct = real_pnl / base_notional if base_notional > 0 else None

        orm.exit_ts = exit_dt
        orm.exit_price = close_price
        orm.pnl_usdt = real_pnl
        orm.pnl_pct_futures = pnl_pct
        orm.close_reason = close_reason

        try:
            # regime_at_exit 는 가능하면 원래 source 반영
            if getattr(trade, "source", None):
                orm.regime_at_exit = getattr(trade, "source")
        except Exception:
            pass

        # note 에 runtime close 힌트 추가
        note_bits: List[str] = []
        if orm.note:
            note_bits.append(orm.note)
        note_bits.append(f"runtime_close={close_reason}")
        if cooldown_tag:
            note_bits.append(f"cooldown={cooldown_tag}")
        orm.note = " | ".join(note_bits)

        session.commit()
        log(
            f"[TRADE_DB] updated trade_id={trade_db_id} "
            f"reason={close_reason} pnl={real_pnl:.4f} pnl_pct={(pnl_pct or 0.0)*100:.4f}%"
        )
    except Exception as e:  # pragma: no cover - DB 에러 방어
        session.rollback()
        log(f"[TRADE_DB] update 실패 trade_id={trade_db_id}: {e}")
    finally:
        session.close()


# ─────────────────────────────────────────
# 1) 박스 조기익절 / 조기청산 (5m 기준, volume 기록)
# ─────────────────────────────────────────

def maybe_early_exit_range(trade: Trade, settings: Any) -> bool:
    if trade.source != "RANGE":
        return False

    log(f"[PW] (WS) RANGE watch symbol={trade.symbol} tf=5m")

    try:
        candles = ws_get_klines_with_vol(trade.symbol, "5m", 3)
    except Exception as e:
        log(f"[EARLY_EXIT WS] kline fetch error: {e}")
        return False

    if not candles:
        return False

    latest_candle = candles[-1]
    candle_ts = latest_candle[0]
    o = latest_candle[1]
    h = latest_candle[2]
    l = latest_candle[3]
    c = latest_candle[4]
    v = latest_candle[5] if len(latest_candle) >= 6 else 0.0

    try:
        log_candle_snapshot(
            symbol=trade.symbol,
            tf="5m",
            candle_ts=candle_ts,
            open_=o,
            high=h,
            low=l,
            close=c,
            volume=v,
            strategy_type="RANGE",
            direction=trade.side,
            extra="early_exit_check=1",
        )
    except Exception as e:
        log(f"[EARLY_EXIT WS] candle snapshot log failed: {e}")

    last_close = c

    # RANGE 조기익절
    if getattr(settings, "range_early_tp_enabled", False):
        early_tp_pct = getattr(settings, "range_early_tp_pct", 0.0025)
        if trade.side == "BUY" and last_close > trade.entry:
            gain_pct = (last_close - trade.entry) / trade.entry
            if gain_pct >= early_tp_pct:
                try:
                    close_position_market(trade.symbol, trade.side, trade.qty)
                    pnl = (last_close - trade.entry) * trade.qty
                    send_tg(
                        f"⚠️ (WS) 박스 조기익절 실행: {trade.symbol} {trade.side} "
                        f"진입={trade.entry:.2f} 현재={last_close:.2f} "
                        f"수익={(gain_pct * 100):.2f}%"
                    )
                    reason = "range_early_tp_runtime"
                    log_signal(
                        event="CLOSE",
                        symbol=trade.symbol,
                        strategy_type="RANGE",
                        direction=trade.side,
                        price=last_close,
                        qty=trade.qty,
                        reason=reason,
                        pnl=pnl,
                    )
                    _update_trade_close_in_db(
                        trade=trade,
                        close_price=float(last_close),
                        close_reason=reason,
                        event_ts_ms=int(candle_ts),
                        pnl=float(pnl),
                    )
                    return True
                except Exception as e:
                    log(f"[EARLY_TP WS] close failed: {e}")
        elif trade.side == "SELL" and last_close < trade.entry:
            gain_pct = (trade.entry - last_close) / trade.entry
            if gain_pct >= early_tp_pct:
                try:
                    close_position_market(trade.symbol, trade.side, trade.qty)
                    pnl = (trade.entry - last_close) * trade.qty
                    send_tg(
                        f"⚠️ (WS) 박스 조기익절 실행: {trade.symbol} {trade.side} "
                        f"진입={trade.entry:.2f} 현재={last_close:.2f} "
                        f"수익={(gain_pct * 100):.2f}%"
                    )
                    reason = "range_early_tp_runtime"
                    log_signal(
                        event="CLOSE",
                        symbol=trade.symbol,
                        strategy_type="RANGE",
                        direction=trade.side,
                        price=last_close,
                        qty=trade.qty,
                        reason=reason,
                        pnl=pnl,
                    )
                    _update_trade_close_in_db(
                        trade=trade,
                        close_price=float(last_close),
                        close_reason=reason,
                        event_ts_ms=int(candle_ts),
                        pnl=float(pnl),
                    )
                    return True
                except Exception as e:
                    log(f"[EARLY_TP WS] close failed: {e}")

    # RANGE 조기청산
    if not getattr(settings, "range_early_exit_enabled", False):
        return False

    trigger_pct = getattr(settings, "range_early_exit_loss_pct", 0.003)

    if trade.side == "BUY":
        if last_close >= trade.entry:
            return False
        loss_pct = (trade.entry - last_close) / trade.entry
    else:
        if last_close <= trade.entry:
            return False
        loss_pct = (last_close - trade.entry) / trade.entry

    if loss_pct < trigger_pct:
        return False

    try:
        close_position_market(trade.symbol, trade.side, trade.qty)
        if trade.side == "BUY":
            pnl = (last_close - trade.entry) * trade.qty
        else:
            pnl = (trade.entry - last_close) * trade.qty
        send_tg(
            f"⚠️ (WS) 박스 조기청산 실행: {trade.symbol} {trade.side} "
            f"진입={trade.entry:.2f} 현재={last_close:.2f} "
            f"역행={(loss_pct * 100):.2f}%"
        )
        reason = "range_early_exit_runtime"
        log_signal(
            event="CLOSE",
            symbol=trade.symbol,
            strategy_type="RANGE",
            direction=trade.side,
            price=last_close,
            qty=trade.qty,
            reason=reason,
            pnl=pnl,
        )
        _update_trade_close_in_db(
            trade=trade,
            close_price=float(last_close),
            close_reason=reason,
            event_ts_ms=int(candle_ts),
            pnl=float(pnl),
        )
        return True
    except Exception as e:
        log(f"[EARLY_EXIT WS] close failed: {e}")
        return False


# ─────────────────────────────────────────
# 2) 박스 → 추세 전환
# ─────────────────────────────────────────

def maybe_upgrade_range_to_trend(
    trade: Trade,
    settings: Any,
    last_trend_close_ts: float,
    last_range_close_ts: float,
) -> bool:
    if trade.source != "RANGE":
        return False

    log(f"[PW] (WS) RANGE→TREND recheck symbol={trade.symbol}")

    try:
        sig_ctx = get_trading_signal(
            settings=settings,
            last_trend_close_ts=last_trend_close_ts,
            last_range_close_ts=last_range_close_ts,
        )
    except Exception as e:
        log(f"[R2T WS] re-signal fetch error: {e}")
        return False

    if sig_ctx is None:
        return False

    (
        new_signal_dir,
        new_signal_source,
        latest_ts,
        candles_5m,
        _candles_5m_raw,
        last_price,
        _extra,
    ) = sig_ctx

    # HYBRID(동일 방향)도 추세 계열로 인정
    if new_signal_source not in ("TREND", "HYBRID"):
        return False

    need_dir = _norm_dir(new_signal_dir)
    if trade.side == "BUY" and need_dir != "LONG":
        return False
    if trade.side == "SELL" and need_dir != "SHORT":
        return False

    trend15 = _get_15m_trend_dir(trade.symbol)
    if trade.side == "BUY" and trend15 != "LONG":
        return False
    if trade.side == "SELL" and trend15 != "SHORT":
        return False

    upgrade_thresh = getattr(settings, "range_to_trend_min_gain_pct", 0.002)
    if trade.side == "BUY":
        if last_price <= trade.entry:
            return False
        gain_pct = (last_price - trade.entry) / trade.entry
        if gain_pct < upgrade_thresh:
            return False
    else:
        if last_price >= trade.entry:
            return False
        gain_pct = (trade.entry - last_price) / trade.entry
        if gain_pct < upgrade_thresh:
            return False

    # 캔들 ts (없으면 latest_ts 사용)
    try:
        candle_ts = int(candles_5m[-1][0]) if candles_5m else int(latest_ts)
    except Exception:
        candle_ts = int(latest_ts)

    try:
        close_position_market(trade.symbol, trade.side, trade.qty)
        if trade.side == "BUY":
            pnl = (last_price - trade.entry) * trade.qty
        else:
            pnl = (trade.entry - last_price) * trade.qty
        send_tg(
            f"⚠️ (WS) 박스→추세 전환 감지: {trade.symbol} {trade.side} 수익권에서 정리 "
            f"(현재가={last_price:.2f}, 진입={trade.entry:.2f})"
        )
        reason = "range_to_trend_upgrade"
        log_signal(
            event="CLOSE",
            symbol=trade.symbol,
            strategy_type="RANGE",
            direction=trade.side,
            price=last_price,
            qty=trade.qty,
            reason=reason,
            pnl=pnl,
        )
        _update_trade_close_in_db(
            trade=trade,
            close_price=float(last_price),
            close_reason=reason,
            event_ts_ms=candle_ts,
            pnl=float(pnl),
        )
        return True
    except Exception as e:
        log(f"[R2T WS] close failed: {e}")
        return False


# ─────────────────────────────────────────
# 2.5) 추세 → 박스 다운그레이드 (신규)
# ─────────────────────────────────────────

def maybe_downgrade_trend_to_range(
    trade: Trade,
    settings: Any,
    last_trend_close_ts: float,
    last_range_close_ts: float,
) -> bool:
    """추세 포지션이지만 시장이 박스로 전환됐다고 판단되면 추세 포지션을 정리하고
    RANGE 전략으로 갈아탄다.

    - RANGE 신호가 현재 포지션 방향과 같을 때만 동작(무리한 반전 진입 방지)
    - PnL이 아주 작을 때(±max_abs_pnl) 또는 최소이익 이상일 때만 허용
    - RANGE 전용 1m 확인 토글을 반영
    - soft block 시 TP/SL 0.7배 완화
    - auto re-enter 옵션이 켜져 있고, 재진입 함수가 있으면 즉시 재진입
    """
    if trade.source != "TREND":
        return False

    if not getattr(settings, "trend_to_range_enable", False):
        return False

    symbol = trade.symbol
    log(f"[PW] (WS) TREND→RANGE downgrade check symbol={symbol}")

    # 5m/15m 캔들 확보
    try:
        candles_5m = ws_get_klines(symbol, "5m", 120)
        candles_15m = ws_get_klines(symbol, "15m", 120)
        if not candles_5m:
            return False
    except Exception as e:
        log(f"[T2R WS] kline fetch error: {e}")
        return False

    last_price = float(candles_5m[-1][4])
    candle_ts = int(candles_5m[-1][0])
    if last_price <= 0 or trade.entry <= 0:
        return False

    # 박스 차단 판단
    blocked_now = False
    block_reason = ""
    if 'should_block_range_today_with_level' in globals() and should_block_range_today_with_level:
        try:
            blocked_now, block_reason = should_block_range_today_with_level(candles_5m, candles_15m, settings)
        except Exception:
            blocked_now, block_reason = False, ""
    else:
        try:
            blocked_now, block_reason = False, ""
        except Exception:
            blocked_now, block_reason = False, ""

    if blocked_now and not str(block_reason).startswith("soft_"):
        log("[T2R] range blocked today → skip downgrade")
        return False

    # 박스 신호 (신규 시그니처 지원)
    try:
        try:
            r_sig = decide_signal_range(candles_5m, settings=settings)  # type: ignore[arg-type]
        except TypeError:
            r_sig = decide_signal_range(candles_5m)  # 구버전 호환
    except Exception as e:
        log(f"[T2R WS] decide_signal_range error: {e}")
        return False

    if not r_sig:
        return False

    # 현재 포지션 방향과 동일한지 확인(반전 진입 방지)
    nd = _norm_dir(r_sig)
    if trade.side == "BUY" and nd != "LONG":
        return False
    if trade.side == "SELL" and nd != "SHORT":
        return False

    # RANGE 1m 확인 토글
    if _is_range_1m_confirm_enabled(settings):
        try:
            candles_1m = ws_get_klines(symbol, "1m", 40)
            if not confirm_1m_direction(candles_1m, r_sig):
                log("[T2R] 1m confirm mismatch → skip downgrade")
                return False
        except Exception as e:
            log(f"[T2R WS] 1m confirm error: {e}")
            # 확인 실패 시 보수적으로 스킵
            return False

    # PnL 조건
    pnl_pct = (last_price - trade.entry) / trade.entry
    if trade.side == "SELL":
        pnl_pct = -pnl_pct

    min_gain = float(getattr(settings, "trend_to_range_min_gain_pct", 0.0015))
    max_abs = float(getattr(settings, "trend_to_range_max_abs_pnl_pct", 0.0015))

    near_entry_ok = abs(pnl_pct) <= max_abs
    small_profit_ok = pnl_pct >= min_gain
    if not (near_entry_ok or small_profit_ok):
        return False

    # TP/SL 산출
    tp_pct = float(getattr(settings, "range_tp_pct", 0.006))
    sl_pct = float(getattr(settings, "range_sl_pct", 0.004))

    if 'compute_range_params' in globals() and compute_range_params:
        try:
            params = compute_range_params(r_sig, candles_5m, settings)
            tp_pct = float(params.get("tp_pct", tp_pct))
            sl_pct = float(params.get("sl_pct", sl_pct))
        except Exception as e:
            log(f"[T2R WS] compute_range_params error: {e}")

    if str(block_reason).startswith("soft_"):
        tp_pct *= 0.7
        sl_pct *= 0.7

    # 기존 추세 포지션 정리
    try:
        close_position_market(symbol, trade.side, trade.qty)
        if trade.side == "BUY":
            pnl = (last_price - trade.entry) * trade.qty
        else:
            pnl = (trade.entry - last_price) * trade.qty
        send_tg(
            f"⚠️ (WS) 추세→박스 다운그레이드: {symbol} {trade.side} 정리 후 RANGE 전환 준비 "
            f"(pnl≈{pnl_pct*100:.3f}%, last={last_price:.2f})"
        )
        reason = "trend_to_range_downgrade"
        log_signal(
            event="CLOSE",
            symbol=symbol,
            strategy_type="TREND",
            direction=trade.side,
            price=last_price,
            qty=trade.qty,
            reason=reason,
            pnl=pnl,
        )
        _update_trade_close_in_db(
            trade=trade,
            close_price=float(last_price),
            close_reason=reason,
            event_ts_ms=candle_ts,
            pnl=float(pnl),
        )
    except Exception as e:
        log(f"[T2R WS] close failed: {e}")
        return False

    # 자동 재진입(가능할 때만)
    if getattr(settings, "trend_to_range_auto_reenter", True) and open_position_with_tp_sl:
        try:
            side = "BUY" if nd == "LONG" else "SELL"
            new_qty = trade.qty  # 단순 동일 수량 재사용(필요 시 별도 정책 적용)
            open_position_with_tp_sl(
                symbol=symbol,
                side=side,
                qty=new_qty,
                tp_pct=tp_pct,
                sl_pct=sl_pct,
                source="RANGE",
            )
            send_tg(
                f"✅ (WS) RANGE 재진입 완료: {symbol} {side} tp={tp_pct*100:.2f}% sl={sl_pct*100:.2f}%"
            )
            log_signal(
                event="ENTRY",
                symbol=symbol,
                strategy_type="RANGE",
                direction=side,
                price=last_price,
                qty=new_qty,
                reason="trend_to_range_reenter",
                tp_pct=tp_pct,
                sl_pct=sl_pct,
            )
        except Exception as e:
            log(f"[T2R WS] auto reenter failed: {e}")
            send_tg("⚠️ (WS) RANGE 자동 재진입 실패: 함수 미존재/오류")

    return True


# ─────────────────────────────────────────
# 3) 추세 포지션 조기익절 / 조기청산 (5m 기준, volume 기록)
# ─────────────────────────────────────────

def maybe_early_exit_trend(trade: Trade, settings: Any) -> bool:
    if trade.source != "TREND":
        return False

    log(f"[PW] (WS) TREND watch symbol={trade.symbol} tf=5m")

    try:
        candles = ws_get_klines_with_vol(trade.symbol, "5m", 3)
    except Exception as e:
        log(f"[TREND_EARLY WS] kline fetch error: {e}")
        return False

    if not candles:
        return False

    latest_candle = candles[-1]
    candle_ts = latest_candle[0]
    o = latest_candle[1]
    h = latest_candle[2]
    l = latest_candle[3]
    c = latest_candle[4]
    v = latest_candle[5] if len(latest_candle) >= 6 else 0.0

    try:
        log_candle_snapshot(
            symbol=trade.symbol,
            tf="5m",
            candle_ts=candle_ts,
            open_=o,
            high=h,
            low=l,
            close=c,
            volume=v,
            strategy_type="TREND",
            direction=trade.side,
            extra="trend_early_check=1",
        )
    except Exception as e:
        log(f"[TREND_EARLY WS] candle snapshot log failed: {e}")

    last_close = c

    # 추세 조기익절
    if getattr(settings, "trend_early_tp_enabled", False):
        trend_early_tp_pct = getattr(settings, "trend_early_tp_pct", 0.0025)
        if trade.side == "BUY" and last_close > trade.entry:
            gain_pct = (last_close - trade.entry) / trade.entry
            if gain_pct >= trend_early_tp_pct:
                try:
                    close_position_market(trade.symbol, trade.side, trade.qty)
                    pnl = (last_close - trade.entry) * trade.qty
                    send_tg(
                        f"⚠️ (WS) 추세 조기익절 실행: {trade.symbol} {trade.side} "
                        f"진입={trade.entry:.2f} 현재={last_close:.2f} "
                        f"수익={(gain_pct * 100):.2f}%"
                    )
                    reason = "trend_early_tp_runtime"
                    log_signal(
                        event="CLOSE",
                        symbol=trade.symbol,
                        strategy_type="TREND",
                        direction=trade.side,
                        price=last_close,
                        qty=trade.qty,
                        reason=reason,
                        pnl=pnl,
                    )
                    _update_trade_close_in_db(
                        trade=trade,
                        close_price=float(last_close),
                        close_reason=reason,
                        event_ts_ms=int(candle_ts),
                        pnl=float(pnl),
                    )
                    return True
                except Exception as e:
                    log(f"[TREND_EARLY_TP WS] close failed: {e}")
        elif trade.side == "SELL" and last_close < trade.entry:
            gain_pct = (trade.entry - last_close) / trade.entry
            if gain_pct >= trend_early_tp_pct:
                try:
                    close_position_market(trade.symbol, trade.side, trade.qty)
                    pnl = (trade.entry - last_close) * trade.qty
                    send_tg(
                        f"⚠️ (WS) 추세 조기익절 실행: {trade.symbol} {trade.side} "
                        f"진입={trade.entry:.2f} 현재={last_close:.2f} "
                        f"수익={(gain_pct * 100):.2f}%"
                    )
                    reason = "trend_early_tp_runtime"
                    log_signal(
                        event="CLOSE",
                        symbol=trade.symbol,
                        strategy_type="TREND",
                        direction=trade.side,
                        price=last_close,
                        qty=trade.qty,
                        reason=reason,
                        pnl=pnl,
                    )
                    _update_trade_close_in_db(
                        trade=trade,
                        close_price=float(last_close),
                        close_reason=reason,
                        event_ts_ms=int(candle_ts),
                        pnl=float(pnl),
                    )
                    return True
                except Exception as e:
                    log(f"[TREND_EARLY_TP WS] close failed: {e}")

    # 추세 조기청산
    if not getattr(settings, "trend_early_exit_enabled", False):
        return False

    trend_trigger_pct = getattr(settings, "trend_early_exit_loss_pct", 0.003)

    # 납작 + 거래량 급감 시 더 적극적으로: 트리거 0.6배
    eff_trig = trend_trigger_pct
    try:
        need_bars = int(getattr(settings, "trend_sideways_need_bars", 3))
        max_rng = float(getattr(settings, "trend_sideways_range_pct", 0.0008))
        vol_ratio = float(getattr(settings, "min_entry_volume_ratio", 0.15))
        if _all_candles_narrow(candles, need_bars, max_rng) and _volume_collapse(candles, need_bars, vol_ratio):
            eff_trig = trend_trigger_pct * 0.6
    except Exception:
        pass

    if trade.side == "BUY":
        if last_close >= trade.entry:
            return False
        loss_pct = (trade.entry - last_close) / trade.entry
    else:
        if last_close <= trade.entry:
            return False
        loss_pct = (last_close - trade.entry) / trade.entry

    if loss_pct < eff_trig:
        return False

    try:
        close_position_market(trade.symbol, trade.side, trade.qty)
        if trade.side == "BUY":
            pnl = (last_close - trade.entry) * trade.qty
        else:
            pnl = (trade.entry - last_close) * trade.qty
        send_tg(
            f"⚠️ (WS) 추세 조기청산 실행: {trade.symbol} {trade.side} "
            f"진입={trade.entry:.2f} 현재={last_close:.2f} "
            f"역행={(loss_pct * 100):.2f}% (eff_trig={eff_trig*100:.2f}%)"
        )
        reason = "trend_early_exit_runtime"
        log_signal(
            event="CLOSE",
            symbol=trade.symbol,
            strategy_type="TREND",
            direction=trade.side,
            price=last_close,
            qty=trade.qty,
            reason=reason,
            pnl=pnl,
        )
        _update_trade_close_in_db(
            trade=trade,
            close_price=float(last_close),
            close_reason=reason,
            event_ts_ms=int(candle_ts),
            pnl=float(pnl),
        )
        return True
    except Exception as e:
        log(f"[TREND_EARLY_EXIT WS] close failed: {e}")
        return False


# ─────────────────────────────────────────
# 5) 반대 시그널 감지 시 강제 청산
# ─────────────────────────────────────────

def maybe_close_on_opposite_signal(
    trade: Trade,
    settings: Any,
    last_trend_close_ts: float,
    last_range_close_ts: float,
) -> bool:
    try:
        sig_ctx = get_trading_signal(
            settings=settings,
            last_trend_close_ts=last_trend_close_ts,
            last_range_close_ts=last_range_close_ts,
        )
    except Exception as e:
        log(f"[OPP WS] re-signal fetch error: {e}")
        return False

    if sig_ctx is None:
        return False

    new_dir, new_src, ts_ms, _c5, _c5raw, last_price, _extra = sig_ctx

    def _is_opposite(trade_side: str, sig_dir: str) -> bool:
        nd = _norm_dir(sig_dir)
        if trade_side == "BUY" and nd == "SHORT":
            return True
        if trade_side == "SELL" and nd == "LONG":
            return True
        return False

    # TREND 포지션: TREND/HYBRID 반대 시그널 시 강제 청산
    if trade.source == "TREND" and new_src in ("TREND", "HYBRID"):
        if _is_opposite(trade.side, new_dir):
            log(f"[OPP WS] TREND opposite detected → close {trade.symbol}")
            try:
                close_position_market(trade.symbol, trade.side, trade.qty)
                if trade.side == "BUY":
                    pnl = (last_price - trade.entry) * trade.qty
                else:
                    pnl = (trade.entry - last_price) * trade.qty
                send_tg(
                    f"⚠️ (WS) 추세 포지션 반대 TREND/HYBRID 시그널 감지 → 즉시 청산: {trade.symbol} "
                    f"{trade.side}→{new_dir} 현재={last_price:.2f}"
                )
                reason = "trend_opposite_signal_close"
                log_signal(
                    event="CLOSE",
                    symbol=trade.symbol,
                    strategy_type="TREND",
                    direction=trade.side,
                    price=last_price,
                    qty=trade.qty,
                    reason=reason,
                    pnl=pnl,
                )
                _update_trade_close_in_db(
                    trade=trade,
                    close_price=float(last_price),
                    close_reason=reason,
                    event_ts_ms=int(ts_ms),
                    pnl=float(pnl),
                )
                return True
            except Exception as e:
                log(f"[OPP WS] close failed: {e}")
                return False

    # RANGE 포지션: 어떤 소스든 반대 방향이면 강제 청산
    if trade.source == "RANGE":
        if _is_opposite(trade.side, new_dir):
            log(f"[OPP WS] RANGE opposite detected → close {trade.symbol}")
            try:
                close_position_market(trade.symbol, trade.side, trade.qty)
                if trade.side == "BUY":
                    pnl = (last_price - trade.entry) * trade.qty
                else:
                    pnl = (trade.entry - last_price) * trade.qty
                send_tg(
                    f"⚠️ (WS) 박스 포지션 반대 시그널 감지 → 즉시 청산: {trade.symbol} "
                    f"{trade.side}→{new_dir} 현재={last_price:.2f}"
                )
                reason = "range_opposite_signal_close"
                log_signal(
                    event="CLOSE",
                    symbol=trade.symbol,
                    strategy_type="RANGE",
                    direction=trade.side,
                    price=last_price,
                    qty=trade.qty,
                    reason=reason,
                    pnl=pnl,
                )
                _update_trade_close_in_db(
                    trade=trade,
                    close_price=float(last_price),
                    close_reason=reason,
                    event_ts_ms=int(ts_ms),
                    pnl=float(pnl),
                )
                return True
            except Exception as e:
                log(f"[OPP WS] close failed: {e}")
                return False

    return False


def maybe_force_close_on_opposite_signal(
    trade: Trade,
    settings: Any,
    last_trend_close_ts: float,
    last_range_close_ts: float,
) -> bool:
    """run_bot 이 사용하는 공개 함수 이름은 그대로 둔다."""
    return maybe_close_on_opposite_signal(
        trade, settings, last_trend_close_ts, last_range_close_ts
    )


__all__ = [
    "maybe_early_exit_range",
    "maybe_upgrade_range_to_trend",
    "maybe_downgrade_trend_to_range",  # 신규
    "maybe_early_exit_trend",
    "maybe_close_on_opposite_signal",
    "maybe_force_close_on_opposite_signal",
]
