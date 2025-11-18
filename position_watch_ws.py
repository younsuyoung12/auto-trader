# position_watch_ws.py
# ====================================================
# 웹소켓으로 받은 1m/5m/15m 캔들을 기준으로 포지션을 감시해서
# 조기익절, 조기청산, 박스→추세 전환, 추세→박스 다운그레이드,
# 반대 시그널 강제 청산을 수행하는 모듈.
#
# 2025-11-19 패치 (EXIT 레이어 안정성 보강 + 레짐 피처 공통화)
# ----------------------------------------------------
# - 모든 EXIT 경로에서 trade.qty <= 0 인 포지션은 즉시 스킵하도록 방어 로직 추가.
#   · 실수로 qty=0 인 Trade 객체가 들어와도 거래소 API 에 잘못된 청산 주문을 보내지 않도록 차단.
# - TREND 조기청산(sideways+거래량 급감) 트리거 계산 시 캔들 수가 부족해도
#   안전하게 동작하도록 조건을 보강(len(candles) 체크 추가).
# - log_skip_event 의 source 문자열을 실제 함수명 기준으로 맞춰 로깅 가독성 개선.
# - EXIT GPT 컨텍스트의 regime_features 는 indicators.build_regime_features_from_candles(...)
#   를 재사용하여 entry_flow.py 와 포맷을 통일.
#
# 2025-11-17 패치 (EXIT GPT 컨텍스트 + 이벤트/스킵 CSV 로깅 강화)
# ----------------------------------------------------
# - ask_exit_decision_safe(...) 로 넘기는 extra(=GPT 컨텍스트)에
#   "regime_features" 필드를 공통으로 추가했다.
#   · 5m 웹소켓 캔들 기반으로 EMA/ATR/RSI 및 TREND/RANGE/CHOP 점수 계산
#   · GPT 가 "지금 시장 상태"를 같이 보고 CLOSE/HOLD 를 판단할 수 있도록 함.
#   · 레짐 계산 실패 시에는 regime_features 는 비어 있는 dict 로 들어가며,
#     기존 동작에는 영향을 주지 않는다.
# - signals_logger.log_gpt_exit_event / log_skip_event 연동.
#   · GPT 가 EXIT 시 action="CLOSE" 를 반환하여 실제 청산이 실행된 경우:
#       → log_gpt_exit_event(...) 로 events CSV 에 EXIT 이벤트 기록
#   · GPT 가 action="HOLD" 를 반환하여 청산을 막은 경우:
#       → log_skip_event(...) 로 "gpt_exit_hold_*" 사유와 컨텍스트 기록
#   · RANGE/TREND 조기익절/조기청산, 박스↔추세 전환, 반대 시그널 강제청산
#     모든 GPT EXIT 경로에 적용.
#
# 2025-11-16 패치 (GPT-5.1 런타임 EXIT 의사결정 + gpt_decider 연동 완료)
# ----------------------------------------------------
# - 이 모듈의 "조기익절/조기청산/박스↔추세 전환/반대 시그널 강제 청산"은
#   이제 모두 gpt_decider.ask_exit_decision_safe(...) 를 통해 GPT-5.1 의사결정을 한 번 거친 뒤
#   CLOSE/HOLD 를 결정한다.
#   · entry_flow.py 의 진입은 이미 ask_entry_decision_safe(...) 를 사용하고 있음.
#   · 이 파일에서는 GPT/OpenAI 를 직접 호출하지 않고, gpt_decider 모듈만 import 한다.
# - GPT 호출이 실패하거나 예외가 발생하면,
#   ask_exit_decision_safe(...)가 fallback_action="CLOSE" 로 동작해서
#   → 기존 Python 로직 그대로(조기익절/조기청산/전환/강제청산) 수행한다.
#   (진입 쪽과 동일하게 "GPT 추가 레이어"가 실패해도 기본 리스크 가드는 유지되는 구조)
# - GPT 는 "추가 브레이크" 역할을 한다.
#   · Python 조건이 모두 만족해서 조기 종료 후보가 되더라도,
#     GPT 가 action="HOLD" 를 반환하면 실제 청산/전환은 하지 않는다.
#   · action="CLOSE" 일 때만 close_position_market(...) 을 호출한다.
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
# 2025-11-13 변경 사항 (요약)
# ----------------------------------------------------
# - 3m REST 캔들이 아니라, WS 기반 5m/15m/1m 버퍼를 사용.
# - 추세 판단은 strategies_trend_ws.decide_trend_15m 사용.
# - 재시그널은 signal_flow_ws.get_trading_signal 사용.
# - 각 단계에서 log(...) 를 통해 Render 로그로 상태 확인.
# - 주문/TP/SL 은 exchange_api.close_position_market(...) 으로 처리.
#
# ⚠ 중요
# ----------------------------------------------------
# - 이 파일은 "런타임 보조 EXIT 레이어" 이다.
#   → 거래소 TP/SL, 전역 손실 한도(연속 손실, 일일 손실) 등 하드 리스크 가드는
#     다른 모듈에서 이미 지키고 있다는 전제.
# - GPT 의 판단(action="HOLD"/"CLOSE")도 "추가 레이어"일 뿐이고,
#   GPT 가 죽어도 이 레이어가 fallback_action 으로 원래 동작을 그대로 수행하게 설계했다.
# - 따라서 실서비스 시에는
#   · gpt_decider 의 OPENAI_API_KEY, GPT_ENTRY_MODEL/GPT_EXIT_MODEL 등만 올바르게 세팅하면 된다.

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional

from telelog import log, send_tg
# ⬇️ volume 있는 버전으로 가져온다
from market_data_ws import (
    get_klines as ws_get_klines,
    get_klines_with_volume as ws_get_klines_with_vol,
)
from exchange_api import close_position_market
from signal_flow_ws import get_trading_signal  # WS 기반 시그널 (동시 평가 + 중재 버전)
from strategies_trend_ws import decide_trend_15m, confirm_1m_direction  # WS 기반 15m 추세 + 1m 확인
from signals_logger import (
    log_signal,
    log_candle_snapshot,
    log_gpt_exit_event,
    log_skip_event,
)
from trader import Trade
from gpt_decider import ask_exit_decision_safe  # ✅ EXIT 쪽 GPT 의사결정은 이 함수만 사용
from indicators import build_regime_features_from_candles  # ✅ 레짐 피처 공통 함수 재사용

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


def _build_runtime_regime_features_for_gpt(symbol: str, settings: Any) -> Dict[str, Any]:
    """5m 웹소켓 캔들 기반 레짐/지표 스냅샷 생성.

    - indicators.build_regime_features_from_candles(...) 를 재사용해서
      entry_flow.py 의 regime_features_5m 와 일관된 포맷을 만든다.
    - 필요 시 15m 방향(trend15_dir)을 덧붙인다.
    - 실패 시에는 빈 dict 반환 (GPT 컨텍스트에서 무시 가능).
    """
    try:
        candles_5m = ws_get_klines(symbol, "5m", 200)
    except Exception as e:
        log(f"[PW][REGIME_FEAT] ws_get_klines error symbol={symbol}: {e}")
        return {}

    if not candles_5m or len(candles_5m) < 30:
        return {}

    # build_regime_features_from_candles 는 (ts, o, h, l, c) 리스트를 기대
    ohlc_rows: List[Tuple[int, float, float, float, float]] = []
    for row in candles_5m:
        if len(row) < 5:
            continue
        try:
            ts_i = int(row[0])
            o_i = float(row[1])
            h_i = float(row[2])
            l_i = float(row[3])
            c_i = float(row[4])
        except (TypeError, ValueError):
            continue
        ohlc_rows.append((ts_i, o_i, h_i, l_i, c_i))

    if len(ohlc_rows) < 20:
        return {}

    try:
        base_features = build_regime_features_from_candles(ohlc_rows)
    except Exception as e:
        log(f"[PW][REGIME_FEAT] build_regime_features_from_candles error symbol={symbol}: {e}")
        return {}

    if not isinstance(base_features, dict) or not base_features:
        return {}

    features: Dict[str, Any] = dict(base_features)

    # 메타 필드 보강
    features.setdefault("version", "ws_runtime_v2")
    features.setdefault("symbol", symbol)
    features.setdefault("timeframe", "5m")

    # 15m 방향 추가 (있으면)
    try:
        trend15_dir = _get_15m_trend_dir(symbol)
        if trend15_dir:
            features["trend15_dir"] = trend15_dir
    except Exception as e:
        log(f"[PW][REGIME_FEAT] trend15_dir error symbol={symbol}: {e}")

    return features


def _build_exit_context_base(
    *,
    trade: Trade,
    settings: Any,
    scenario: str,
    last_price: float,
    candle_ts_ms: Optional[int] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """GPT에 넘기는 공통 EXIT 컨텍스트 생성.

    - trade, settings 에서 필요한 최소 정보만 요약해서 보낸다.
    - extra 로 시나리오별 세부 지표(gain_pct, loss_pct, upgrade_thresh 등)를 합친다.
    - 2025-11-17: 5m/websocket 기반 레짐/지표 스냅샷(regime_features)도 함께 포함.
    """
    ctx: Dict[str, Any] = {
        "scenario": scenario,
        "symbol": trade.symbol,
        "source": getattr(trade, "source", ""),
        "side": trade.side,
        "entry_price": float(getattr(trade, "entry", 0.0) or 0.0),
        "qty": float(getattr(trade, "qty", 0.0) or 0.0),
        "last_price": float(last_price),
        "leverage": float(getattr(settings, "leverage", 0.0) or 0.0),
        "event_ts_ms": int(candle_ts_ms) if candle_ts_ms is not None else None,
    }
    entry = ctx["entry_price"]
    if entry > 0 and last_price > 0:
        pnl_pct = (last_price - entry) / entry
        if trade.side == "SELL":
            pnl_pct = -pnl_pct
        ctx["pnl_pct"] = pnl_pct
    else:
        ctx["pnl_pct"] = None

    if extra:
        ctx.update(extra)

    # ✅ 레짐/지표 스냅샷 추가 (실패해도 기존 동작에는 영향 없음)
    try:
        regime_features = _build_runtime_regime_features_for_gpt(symbol=trade.symbol, settings=settings)
        if regime_features:
            ctx["regime_features"] = regime_features
    except Exception as e:  # pragma: no cover - 방어적 로그
        log(f"[PW][REGIME_FEAT] context build error symbol={trade.symbol}: {e}")

    return ctx


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

    ⚠ GPT-5.1 연동 시에도:
    - GPT 결정(CLOSE/HOLD)은 위에서 미리 끝내고,
    - 실제 DB 업데이트는 이 함수만 통해 일관되게 반영한다.
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
# 1) 박스 조기익절 / 조기청산 (5m 기준, volume 기록 + GPT 확인)
# ─────────────────────────────────────────


def maybe_early_exit_range(trade: Trade, settings: Any) -> bool:
    """RANGE 포지션에 대한 조기익절/조기청산 후보를 판단하고,
    GPT-5.1 에게 EXIT 여부를 한 번 묻는다.

    - Python 조건이 false 이면 GPT 는 아예 호출하지 않는다.
    - Python 조건이 true 이면 ask_exit_decision_safe(...) 를 호출해서
      action="CLOSE" 인 경우에만 실제 청산을 실행한다.
    """
    if trade.source != "RANGE":
        return False

    regime_label = trade.source or "RANGE"
    qty = float(getattr(trade, "qty", 0.0) or 0.0)
    if qty <= 0:
        log(f"[EARLY_EXIT WS] invalid qty<=0 for RANGE trade → skip (symbol={trade.symbol})")
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
    candle_ts = int(latest_candle[0])
    o = float(latest_candle[1])
    h = float(latest_candle[2])
    l = float(latest_candle[3])
    c = float(latest_candle[4])
    v = float(latest_candle[5]) if len(latest_candle) >= 6 else 0.0

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
    entry = float(trade.entry)

    # RANGE 조기익절 (수익권에서 아주 빨리 정리)
    if getattr(settings, "range_early_tp_enabled", False) and entry > 0:
        early_tp_pct = float(getattr(settings, "range_early_tp_pct", 0.0025))
        if trade.side == "BUY" and last_close > entry:
            gain_pct = (last_close - entry) / entry
        elif trade.side == "SELL" and last_close < entry:
            gain_pct = (entry - last_close) / entry
        else:
            gain_pct = 0.0

        if gain_pct >= early_tp_pct:
            # ✅ GPT에 먼저 물어본다
            ctx_extra = {
                "check_type": "range_early_tp",
                "gain_pct": gain_pct,
                "threshold_pct": early_tp_pct,
                "candle_5m": {"open": o, "high": h, "low": l, "close": last_close, "volume": v},
            }
            gpt_ctx = _build_exit_context_base(
                trade=trade,
                settings=settings,
                scenario="RANGE_EARLY_TP",
                last_price=last_close,
                candle_ts_ms=candle_ts,
                extra=ctx_extra,
            )
            action, gpt_data = ask_exit_decision_safe(
                symbol=trade.symbol,
                source=trade.source,
                side=trade.side,
                scenario="RANGE_EARLY_TP",
                last_price=last_close,
                entry_price=entry,
                leverage=float(getattr(settings, "leverage", 0.0) or 0.0),
                extra=gpt_ctx,
                fallback_action="CLOSE",
            )
            if action != "CLOSE":
                log(f"[GPT_EXIT][RANGE_TP] HOLD 결정 → 조기익절 스킵 (pnl≈{gain_pct*100:.3f}%)")
                # GPT 가 조기익절을 막은 경우 SKIP 이벤트 로깅
                try:
                    log_skip_event(
                        symbol=trade.symbol,
                        regime=regime_label,
                        source="position_watch_ws.maybe_early_exit_range",
                        side=trade.side,
                        reason="gpt_exit_hold_range_early_tp",
                        extra={
                            "gain_pct": gain_pct,
                            "threshold_pct": early_tp_pct,
                            "gpt_action": action,
                            "gpt_data": gpt_data,
                        },
                    )
                except Exception as e:
                    log(f"[GPT_EXIT_LOG][RANGE_EARLY_TP][SKIP] failed: {e}")
                return False

            try:
                close_position_market(trade.symbol, trade.side, qty)
                pnl = (last_close - entry) * qty if trade.side == "BUY" else (entry - last_close) * qty
                send_tg(
                    f"⚠️ (WS) 박스 조기익절 실행(GPT 승인): {trade.symbol} {trade.side} "
                    f"진입={entry:.2f} 현재={last_close:.2f} "
                    f"수익={(gain_pct * 100):.2f}%"
                )
                reason = "range_early_tp_runtime"
                log_signal(
                    event="CLOSE",
                    symbol=trade.symbol,
                    strategy_type="RANGE",
                    direction=trade.side,
                    price=last_close,
                    qty=qty,
                    reason=reason,
                    pnl=pnl,
                )
                # GPT EXIT 이벤트 CSV 기록
                try:
                    pnl_pct_signed = gain_pct  # 수익이므로 양수
                    log_gpt_exit_event(
                        symbol=trade.symbol,
                        regime=regime_label,
                        side=trade.side,
                        scenario="RANGE_EARLY_TP",
                        action="CLOSE",
                        pnl_pct=pnl_pct_signed,
                        extra={
                            "gain_pct": gain_pct,
                            "threshold_pct": early_tp_pct,
                            "gpt_data": gpt_data,
                            "exit_ctx": gpt_ctx,
                        },
                    )
                except Exception as e:
                    log(f"[GPT_EXIT_LOG][RANGE_EARLY_TP] failed: {e}")

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
                return False

    # RANGE 조기청산 (손실이 빠르게 커질 때)
    if not getattr(settings, "range_early_exit_enabled", False):
        return False

    trigger_pct = float(getattr(settings, "range_early_exit_loss_pct", 0.003))
    if entry <= 0:
        return False

    if trade.side == "BUY":
        if last_close >= entry:
            return False
        loss_pct = (entry - last_close) / entry
    else:
        if last_close <= entry:
            return False
        loss_pct = (last_close - entry) / entry

    if loss_pct < trigger_pct:
        return False

    # ✅ GPT에 EXIT 여부 질의
    ctx_extra = {
        "check_type": "range_early_exit",
        "loss_pct": loss_pct,
        "threshold_pct": trigger_pct,
        "candle_5m": {"open": o, "high": h, "low": l, "close": last_close, "volume": v},
    }
    gpt_ctx = _build_exit_context_base(
        trade=trade,
        settings=settings,
        scenario="RANGE_EARLY_EXIT",
        last_price=last_close,
        candle_ts_ms=candle_ts,
        extra=ctx_extra,
    )
    action, gpt_data = ask_exit_decision_safe(
        symbol=trade.symbol,
        source=trade.source,
        side=trade.side,
        scenario="RANGE_EARLY_EXIT",
        last_price=last_close,
        entry_price=entry,
        leverage=float(getattr(settings, "leverage", 0.0) or 0.0),
        extra=gpt_ctx,
        fallback_action="CLOSE",
    )
    if action != "CLOSE":
        log(f"[GPT_EXIT][RANGE_EXIT] HOLD 결정 → 조기청산 스킵 (loss≈{loss_pct*100:.3f}%)")
        # GPT 가 조기청산을 막은 경우 SKIP 이벤트 로깅
        try:
            log_skip_event(
                symbol=trade.symbol,
                regime=regime_label,
                source="position_watch_ws.maybe_early_exit_range",
                side=trade.side,
                reason="gpt_exit_hold_range_early_exit",
                extra={
                    "loss_pct": loss_pct,
                    "threshold_pct": trigger_pct,
                    "gpt_action": action,
                    "gpt_data": gpt_data,
                },
            )
        except Exception as e:
            log(f"[GPT_EXIT_LOG][RANGE_EARLY_EXIT][SKIP] failed: {e}")
        return False

    try:
        close_position_market(trade.symbol, trade.side, qty)
        pnl = (last_close - entry) * qty if trade.side == "BUY" else (entry - last_close) * qty
        send_tg(
            f"⚠️ (WS) 박스 조기청산 실행(GPT 승인): {trade.symbol} {trade.side} "
            f"진입={entry:.2f} 현재={last_close:.2f} "
            f"역행={(loss_pct * 100):.2f}%"
        )
        reason = "range_early_exit_runtime"
        log_signal(
            event="CLOSE",
            symbol=trade.symbol,
            strategy_type="RANGE",
            direction=trade.side,
            price=last_close,
            qty=qty,
            reason=reason,
            pnl=pnl,
        )
        # GPT EXIT 이벤트 CSV 기록 (손실은 음수 부호)
        try:
            pnl_pct_signed = -loss_pct
            log_gpt_exit_event(
                symbol=trade.symbol,
                regime=regime_label,
                side=trade.side,
                scenario="RANGE_EARLY_EXIT",
                action="CLOSE",
                pnl_pct=pnl_pct_signed,
                extra={
                    "loss_pct": loss_pct,
                    "threshold_pct": trigger_pct,
                    "gpt_data": gpt_data,
                    "exit_ctx": gpt_ctx,
                },
            )
        except Exception as e:
            log(f"[GPT_EXIT_LOG][RANGE_EARLY_EXIT] failed: {e}")

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
# 2) 박스 → 추세 전환 (GPT 확인 포함)
# ─────────────────────────────────────────


def maybe_upgrade_range_to_trend(
    trade: Trade,
    settings: Any,
    last_trend_close_ts: float,
    last_range_close_ts: float,
) -> bool:
    """RANGE 포지션인데 시장이 TREND/HYBRID 쪽으로 강하게 기울었을 때,
    수익권에서 정리하는 로직.

    - Python 조건(동일 방향 TREND/HYBRID + 15m 동조 + 최소 수익) 만족 시
      GPT-5.1 에게 "range_to_trend_upgrade" 시나리오로 EXIT 여부를 물어본다.
    """
    if trade.source != "RANGE":
        return False

    regime_label = trade.source or "RANGE"
    qty = float(getattr(trade, "qty", 0.0) or 0.0)
    if qty <= 0:
        log(f"[R2T WS] invalid qty<=0 for RANGE trade → skip (symbol={trade.symbol})")
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
        extra,
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

    entry = float(trade.entry)
    if entry <= 0 or last_price <= 0:
        return False

    upgrade_thresh = float(getattr(settings, "range_to_trend_min_gain_pct", 0.002))
    if trade.side == "BUY":
        if last_price <= entry:
            return False
        gain_pct = (last_price - entry) / entry
    else:
        if last_price >= entry:
            return False
        gain_pct = (entry - last_price) / entry

    if gain_pct < upgrade_thresh:
        return False

    # 캔들 ts (없으면 latest_ts 사용)
    try:
        candle_ts = int(candles_5m[-1][0]) if candles_5m else int(latest_ts)
    except Exception:
        candle_ts = int(latest_ts)

    # ✅ GPT에 RANGE→TREND 업그레이드 여부 질의
    ctx_extra = {
        "check_type": "range_to_trend_upgrade",
        "gain_pct": gain_pct,
        "min_gain_pct": upgrade_thresh,
        "trend15_dir": trend15,
        "signal_source": new_signal_source,
        "signal_dir": new_signal_dir,
        "extra_from_signal": extra or {},
    }
    gpt_ctx = _build_exit_context_base(
        trade=trade,
        settings=settings,
        scenario="RANGE_TO_TREND_UPGRADE",
        last_price=float(last_price),
        candle_ts_ms=candle_ts,
        extra=ctx_extra,
    )
    action, gpt_data = ask_exit_decision_safe(
        symbol=trade.symbol,
        source=trade.source,
        side=trade.side,
        scenario="RANGE_TO_TREND_UPGRADE",
        last_price=float(last_price),
        entry_price=entry,
        leverage=float(getattr(settings, "leverage", 0.0) or 0.0),
        extra=gpt_ctx,
        fallback_action="CLOSE",
    )
    if action != "CLOSE":
        log(f"[GPT_EXIT][R2T] HOLD 결정 → RANGE→TREND 업그레이드 스킵 (pnl≈{gain_pct*100:.3f}%)")
        # GPT 가 업그레이드를 막은 경우 SKIP 이벤트 로깅
        try:
            log_skip_event(
                symbol=trade.symbol,
                regime=regime_label,
                source="position_watch_ws.maybe_upgrade_range_to_trend",
                side=trade.side,
                reason="gpt_exit_hold_range_to_trend_upgrade",
                extra={
                    "gain_pct": gain_pct,
                    "min_gain_pct": upgrade_thresh,
                    "trend15_dir": trend15,
                    "new_signal_source": new_signal_source,
                    "new_signal_dir": new_signal_dir,
                    "gpt_action": action,
                    "gpt_data": gpt_data,
                },
            )
        except Exception as e:
            log(f"[GPT_EXIT_LOG][R2T][SKIP] failed: {e}")
        return False

    try:
        close_position_market(trade.symbol, trade.side, qty)
        if trade.side == "BUY":
            pnl = (last_price - entry) * qty
        else:
            pnl = (entry - last_price) * qty
        send_tg(
            f"⚠️ (WS) 박스→추세 전환 감지(GPT 승인): {trade.symbol} {trade.side} 수익권에서 정리 "
            f"(현재가={last_price:.2f}, 진입={entry:.2f})"
        )
        reason = "range_to_trend_upgrade"
        log_signal(
            event="CLOSE",
            symbol=trade.symbol,
            strategy_type="RANGE",
            direction=trade.side,
            price=last_price,
            qty=qty,
            reason=reason,
            pnl=pnl,
        )
        # GPT EXIT 이벤트 CSV 기록
        try:
            pnl_pct_signed = gain_pct  # 수익권에서 정리
            log_gpt_exit_event(
                symbol=trade.symbol,
                regime=regime_label,
                side=trade.side,
                scenario="RANGE_TO_TREND_UPGRADE",
                action="CLOSE",
                pnl_pct=pnl_pct_signed,
                extra={
                    "gain_pct": gain_pct,
                    "min_gain_pct": upgrade_thresh,
                    "trend15_dir": trend15,
                    "new_signal_source": new_signal_source,
                    "new_signal_dir": new_signal_dir,
                    "gpt_data": gpt_data,
                    "exit_ctx": gpt_ctx,
                },
            )
        except Exception as e:
            log(f"[GPT_EXIT_LOG][R2T] failed: {e}")

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
# 2.5) 추세 → 박스 다운그레이드 (신규 + GPT 확인)
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
    - EXIT 자체는 GPT-5.1 의 승인(action=CLOSE)을 받은 경우에만 수행.
    """
    if trade.source != "TREND":
        return False

    if not getattr(settings, "trend_to_range_enable", False):
        return False

    symbol = trade.symbol
    regime_label = trade.source or "TREND"
    qty = float(getattr(trade, "qty", 0.0) or 0.0)
    if qty <= 0:
        log(f"[T2R WS] invalid qty<=0 for TREND trade → skip (symbol={symbol})")
        return False

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
    entry = float(trade.entry)
    if last_price <= 0 or entry <= 0:
        return False

    # 박스 차단 판단
    blocked_now = False
    block_reason = ""
    if "should_block_range_today_with_level" in globals() and should_block_range_today_with_level:
        try:
            blocked_now, block_reason = should_block_range_today_with_level(candles_5m, candles_15m, settings)
        except Exception:
            blocked_now, block_reason = False, ""
    else:
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
    pnl_pct = (last_price - entry) / entry
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

    if "compute_range_params" in globals() and compute_range_params:
        try:
            params = compute_range_params(r_sig, candles_5m, settings)
            tp_pct = float(params.get("tp_pct", tp_pct))
            sl_pct = float(params.get("sl_pct", sl_pct))
        except Exception as e:
            log(f"[T2R WS] compute_range_params error: {e}")

    if str(block_reason).startswith("soft_"):
        tp_pct *= 0.7
        sl_pct *= 0.7

    # ✅ GPT에 TREND→RANGE 다운그레이드 여부 질의
    ctx_extra = {
        "check_type": "trend_to_range_downgrade",
        "pnl_pct": pnl_pct,
        "min_gain_pct": min_gain,
        "max_abs_pnl_pct": max_abs,
        "r_sig": r_sig,
        "block_reason": block_reason,
        "tp_pct": tp_pct,
        "sl_pct": sl_pct,
    }
    gpt_ctx = _build_exit_context_base(
        trade=trade,
        settings=settings,
        scenario="TREND_TO_RANGE_DOWNGRADE",
        last_price=last_price,
        candle_ts_ms=candle_ts,
        extra=ctx_extra,
    )
    action, gpt_data = ask_exit_decision_safe(
        symbol=trade.symbol,
        source=trade.source,
        side=trade.side,
        scenario="TREND_TO_RANGE_DOWNGRADE",
        last_price=last_price,
        entry_price=entry,
        leverage=float(getattr(settings, "leverage", 0.0) or 0.0),
        extra=gpt_ctx,
        fallback_action="CLOSE",
    )
    if action != "CLOSE":
        log(f"[GPT_EXIT][T2R] HOLD 결정 → TREND→RANGE 다운그레이드 스킵 (pnl≈{pnl_pct*100:.3f}%)")
        # GPT 가 다운그레이드를 막은 경우 SKIP 이벤트 로깅
        try:
            log_skip_event(
                symbol=trade.symbol,
                regime=regime_label,
                source="position_watch_ws.maybe_downgrade_trend_to_range",
                side=trade.side,
                reason="gpt_exit_hold_trend_to_range_downgrade",
                extra={
                    "pnl_pct": pnl_pct,
                    "min_gain_pct": min_gain,
                    "max_abs_pnl_pct": max_abs,
                    "r_sig": r_sig,
                    "block_reason": block_reason,
                    "tp_pct": tp_pct,
                    "sl_pct": sl_pct,
                    "gpt_action": action,
                    "gpt_data": gpt_data,
                },
            )
        except Exception as e:
            log(f"[GPT_EXIT_LOG][T2R][SKIP] failed: {e}")
        return False

    # 기존 추세 포지션 정리
    try:
        close_position_market(symbol, trade.side, qty)
        if trade.side == "BUY":
            pnl = (last_price - entry) * qty
        else:
            pnl = (entry - last_price) * qty
        send_tg(
            f"⚠️ (WS) 추세→박스 다운그레이드(GPT 승인): {symbol} {trade.side} 정리 후 RANGE 전환 준비 "
            f"(pnl≈{pnl_pct*100:.3f}%, last={last_price:.2f})"
        )
        reason = "trend_to_range_downgrade"
        log_signal(
            event="CLOSE",
            symbol=symbol,
            strategy_type="TREND",
            direction=trade.side,
            price=last_price,
            qty=qty,
            reason=reason,
            pnl=pnl,
        )
        # GPT EXIT 이벤트 CSV 기록
        try:
            log_gpt_exit_event(
                symbol=trade.symbol,
                regime=regime_label,
                side=trade.side,
                scenario="TREND_TO_RANGE_DOWNGRADE",
                action="CLOSE",
                pnl_pct=pnl_pct,
                extra={
                    "min_gain_pct": min_gain,
                    "max_abs_pnl_pct": max_abs,
                    "r_sig": r_sig,
                    "block_reason": block_reason,
                    "tp_pct": tp_pct,
                    "sl_pct": sl_pct,
                    "gpt_data": gpt_data,
                    "exit_ctx": gpt_ctx,
                },
            )
        except Exception as e:
            log(f"[GPT_EXIT_LOG][T2R] failed: {e}")

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
            new_qty = qty  # 단순 동일 수량 재사용(필요 시 별도 정책 적용)
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
# 3) 추세 포지션 조기익절 / 조기청산 (5m 기준, volume 기록 + GPT 확인)
# ─────────────────────────────────────────


def maybe_early_exit_trend(trade: Trade, settings: Any) -> bool:
    """TREND 포지션에 대한 조기익절/조기청산 후보를 판단하고,
    GPT-5.1 에게 EXIT 여부를 한 번 묻는다.
    """
    if trade.source != "TREND":
        return False

    regime_label = trade.source or "TREND"
    qty = float(getattr(trade, "qty", 0.0) or 0.0)
    if qty <= 0:
        log(f"[TREND_EARLY WS] invalid qty<=0 for TREND trade → skip (symbol={trade.symbol})")
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
    candle_ts = int(latest_candle[0])
    o = float(latest_candle[1])
    h = float(latest_candle[2])
    l = float(latest_candle[3])
    c = float(latest_candle[4])
    v = float(latest_candle[5]) if len(latest_candle) >= 6 else 0.0

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
    entry = float(trade.entry)

    # 추세 조기익절
    if getattr(settings, "trend_early_tp_enabled", False) and entry > 0:
        trend_early_tp_pct = float(getattr(settings, "trend_early_tp_pct", 0.0025))
        if trade.side == "BUY" and last_close > entry:
            gain_pct = (last_close - entry) / entry
        elif trade.side == "SELL" and last_close < entry:
            gain_pct = (entry - last_close) / entry
        else:
            gain_pct = 0.0

        if gain_pct >= trend_early_tp_pct:
            ctx_extra = {
                "check_type": "trend_early_tp",
                "gain_pct": gain_pct,
                "threshold_pct": trend_early_tp_pct,
                "candle_5m": {"open": o, "high": h, "low": l, "close": last_close, "volume": v},
            }
            gpt_ctx = _build_exit_context_base(
                trade=trade,
                settings=settings,
                scenario="TREND_EARLY_TP",
                last_price=last_close,
                candle_ts_ms=candle_ts,
                extra=ctx_extra,
            )
            action, gpt_data = ask_exit_decision_safe(
                symbol=trade.symbol,
                source=trade.source,
                side=trade.side,
                scenario="TREND_EARLY_TP",
                last_price=last_close,
                entry_price=entry,
                leverage=float(getattr(settings, "leverage", 0.0) or 0.0),
                extra=gpt_ctx,
                fallback_action="CLOSE",
            )
            if action != "CLOSE":
                log(f"[GPT_EXIT][TREND_TP] HOLD 결정 → 추세 조기익절 스킵 (pnl≈{gain_pct*100:.3f}%)")
                # GPT 가 조기익절을 막은 경우 SKIP 이벤트 로깅
                try:
                    log_skip_event(
                        symbol=trade.symbol,
                        regime=regime_label,
                        source="position_watch_ws.maybe_early_exit_trend",
                        side=trade.side,
                        reason="gpt_exit_hold_trend_early_tp",
                        extra={
                            "gain_pct": gain_pct,
                            "threshold_pct": trend_early_tp_pct,
                            "gpt_action": action,
                            "gpt_data": gpt_data,
                        },
                    )
                except Exception as e:
                    log(f"[GPT_EXIT_LOG][TREND_EARLY_TP][SKIP] failed: {e}")
                return False

            try:
                close_position_market(trade.symbol, trade.side, qty)
                pnl = (last_close - entry) * qty if trade.side == "BUY" else (entry - last_close) * qty
                send_tg(
                    f"⚠️ (WS) 추세 조기익절 실행(GPT 승인): {trade.symbol} {trade.side} "
                    f"진입={entry:.2f} 현재={last_close:.2f} "
                    f"수익={(gain_pct * 100):.2f}%"
                )
                reason = "trend_early_tp_runtime"
                log_signal(
                    event="CLOSE",
                    symbol=trade.symbol,
                    strategy_type="TREND",
                    direction=trade.side,
                    price=last_close,
                    qty=qty,
                    reason=reason,
                    pnl=pnl,
                )
                # GPT EXIT 이벤트 CSV 기록
                try:
                    pnl_pct_signed = gain_pct
                    log_gpt_exit_event(
                        symbol=trade.symbol,
                        regime=regime_label,
                        side=trade.side,
                        scenario="TREND_EARLY_TP",
                        action="CLOSE",
                        pnl_pct=pnl_pct_signed,
                        extra={
                            "gain_pct": gain_pct,
                            "threshold_pct": trend_early_tp_pct,
                            "gpt_data": gpt_data,
                            "exit_ctx": gpt_ctx,
                        },
                    )
                except Exception as e:
                    log(f"[GPT_EXIT_LOG][TREND_EARLY_TP] failed: {e}")

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
                return False

    # 추세 조기청산
    if not getattr(settings, "trend_early_exit_enabled", False):
        return False

    trend_trigger_pct = float(getattr(settings, "trend_early_exit_loss_pct", 0.003))

    # 납작 + 거래량 급감 시 더 적극적으로: 트리거 0.6배
    eff_trig = trend_trigger_pct
    try:
        need_bars = int(getattr(settings, "trend_sideways_need_bars", 3))
        max_rng = float(getattr(settings, "trend_sideways_range_pct", 0.0008))
        vol_ratio = float(getattr(settings, "min_entry_volume_ratio", 0.15))
        if (
            len(candles) >= max(need_bars, 2)
            and _all_candles_narrow(candles, need_bars, max_rng)
            and _volume_collapse(candles, need_bars, vol_ratio)
        ):
            eff_trig = trend_trigger_pct * 0.6
    except Exception:
        pass

    if entry <= 0:
        return False

    if trade.side == "BUY":
        if last_close >= entry:
            return False
        loss_pct = (entry - last_close) / entry
    else:
        if last_close <= entry:
            return False
        loss_pct = (last_close - entry) / entry

    if loss_pct < eff_trig:
        return False

    ctx_extra = {
        "check_type": "trend_early_exit",
        "loss_pct": loss_pct,
        "trigger_pct": trend_trigger_pct,
        "effective_trigger_pct": eff_trig,
        "candle_5m": {"open": o, "high": h, "low": l, "close": last_close, "volume": v},
    }
    gpt_ctx = _build_exit_context_base(
        trade=trade,
        settings=settings,
        scenario="TREND_EARLY_EXIT",
        last_price=last_close,
        candle_ts_ms=candle_ts,
        extra=ctx_extra,
    )
    action, gpt_data = ask_exit_decision_safe(
        symbol=trade.symbol,
        source=trade.source,
        side=trade.side,
        scenario="TREND_EARLY_EXIT",
        last_price=last_close,
        entry_price=entry,
        leverage=float(getattr(settings, "leverage", 0.0) or 0.0),
        extra=gpt_ctx,
        fallback_action="CLOSE",
    )
    if action != "CLOSE":
        log(
            f"[GPT_EXIT][TREND_EXIT] HOLD 결정 → 추세 조기청산 스킵 "
            f"(loss≈{loss_pct*100:.3f}%, eff_trig={eff_trig*100:.3f}%)"
        )
        # GPT 가 조기청산을 막은 경우 SKIP 이벤트 로깅
        try:
            log_skip_event(
                symbol=trade.symbol,
                regime=regime_label,
                source="position_watch_ws.maybe_early_exit_trend",
                side=trade.side,
                reason="gpt_exit_hold_trend_early_exit",
                extra={
                    "loss_pct": loss_pct,
                    "trigger_pct": trend_trigger_pct,
                    "effective_trigger_pct": eff_trig,
                    "gpt_action": action,
                    "gpt_data": gpt_data,
                },
            )
        except Exception as e:
            log(f"[GPT_EXIT_LOG][TREND_EARLY_EXIT][SKIP] failed: {e}")
        return False

    try:
        close_position_market(trade.symbol, trade.side, qty)
        pnl = (last_close - entry) * qty if trade.side == "BUY" else (entry - last_close) * qty
        send_tg(
            f"⚠️ (WS) 추세 조기청산 실행(GPT 승인): {trade.symbol} {trade.side} "
            f"진입={entry:.2f} 현재={last_close:.2f} "
            f"역행={(loss_pct * 100):.2f}% (eff_trig={eff_trig*100:.2f}%)"
        )
        reason = "trend_early_exit_runtime"
        log_signal(
            event="CLOSE",
            symbol=trade.symbol,
            strategy_type="TREND",
            direction=trade.side,
            price=last_close,
            qty=qty,
            reason=reason,
            pnl=pnl,
        )
        # GPT EXIT 이벤트 CSV 기록 (손실은 음수 부호)
        try:
            pnl_pct_signed = -loss_pct
            log_gpt_exit_event(
                symbol=trade.symbol,
                regime=regime_label,
                side=trade.side,
                scenario="TREND_EARLY_EXIT",
                action="CLOSE",
                pnl_pct=pnl_pct_signed,
                extra={
                    "loss_pct": loss_pct,
                    "trigger_pct": trend_trigger_pct,
                    "effective_trigger_pct": eff_trig,
                    "gpt_data": gpt_data,
                    "exit_ctx": gpt_ctx,
                },
            )
        except Exception as e:
            log(f"[GPT_EXIT_LOG][TREND_EARLY_EXIT] failed: {e}")

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
# 5) 반대 시그널 감지 시 강제 청산 (GPT 확인 포함)
# ─────────────────────────────────────────


def maybe_close_on_opposite_signal(
    trade: Trade,
    settings: Any,
    last_trend_close_ts: float,
    last_range_close_ts: float,
) -> bool:
    """동일 심볼에서 반대 방향 TREND/RANGE/HYBRID 시그널이 나왔을 때 강제 청산하는 로직.

    - TREND 포지션: TREND/HYBRID 반대 시그널일 때만 후보.
    - RANGE 포지션: 어떤 소스든 반대 방향이면 후보.
    - 후보가 되면 GPT-5.1 에게 "opposite_signal_close" 시나리오로 EXIT 여부를 묻는다.
    """
    regime_label = trade.source or "UNKNOWN"
    qty = float(getattr(trade, "qty", 0.0) or 0.0)
    if qty <= 0:
        log(f"[OPP WS] invalid qty<=0 for trade → skip (symbol={trade.symbol})")
        return False

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

    new_dir, new_src, ts_ms, candles_5m, _c5raw, last_price, extra = sig_ctx
    entry = float(trade.entry)

    def _is_opposite(trade_side: str, sig_dir: str) -> bool:
        nd = _norm_dir(sig_dir)
        if trade_side == "BUY" and nd == "SHORT":
            return True
        if trade_side == "SELL" and nd == "LONG":
            return True
        return False

    scenario = None

    # TREND 포지션: TREND/HYBRID 반대 시그널 시 강제 청산 후보
    if trade.source == "TREND" and new_src in ("TREND", "HYBRID"):
        if _is_opposite(trade.side, new_dir):
            scenario = "TREND_OPPOSITE_SIGNAL"

    # RANGE 포지션: 어떤 소스든 반대 방향이면 강제 청산 후보
    if trade.source == "RANGE":
        if _is_opposite(trade.side, new_dir):
            scenario = "RANGE_OPPOSITE_SIGNAL"

    if not scenario:
        return False

    # ✅ GPT 에 EXIT 여부 질의
    ctx_extra = {
        "check_type": "opposite_signal_close",
        "new_signal_dir": new_dir,
        "new_signal_source": new_src,
        "extra_from_signal": extra or {},
    }
    gpt_ctx = _build_exit_context_base(
        trade=trade,
        settings=settings,
        scenario=scenario,
        last_price=float(last_price),
        candle_ts_ms=int(ts_ms),
        extra=ctx_extra,
    )
    action, gpt_data = ask_exit_decision_safe(
        symbol=trade.symbol,
        source=trade.source,
        side=trade.side,
        scenario=scenario,
        last_price=float(last_price),
        entry_price=entry,
        leverage=float(getattr(settings, "leverage", 0.0) or 0.0),
        extra=gpt_ctx,
        fallback_action="CLOSE",
    )
    if action != "CLOSE":
        log(f"[GPT_EXIT][OPP] HOLD 결정 → 반대 시그널 강제 청산 스킵 (scenario={scenario})")
        # GPT 가 강제청산을 막은 경우 SKIP 이벤트 로깅
        try:
            log_skip_event(
                symbol=trade.symbol,
                regime=regime_label,
                source="position_watch_ws.maybe_close_on_opposite_signal",
                side=trade.side,
                reason="gpt_exit_hold_opposite_signal",
                extra={
                    "scenario": scenario,
                    "new_signal_dir": new_dir,
                    "new_signal_source": new_src,
                    "gpt_action": action,
                    "gpt_data": gpt_data,
                },
            )
        except Exception as e:
            log(f"[GPT_EXIT_LOG][OPP][SKIP] failed: {e}")
        return False

    # 실제 강제 청산 실행
    try:
        close_position_market(trade.symbol, trade.side, qty)
        if trade.side == "BUY":
            pnl = (last_price - entry) * qty
        else:
            pnl = (entry - last_price) * qty

        # PnL% (DB 계산과 동일한 방식)
        base_notional = entry * qty if entry > 0 and qty > 0 else 0.0
        pnl_pct_signed = (pnl / base_notional) if base_notional > 0 else None

        if trade.source == "TREND":
            send_tg(
                f"⚠️ (WS) 추세 포지션 반대 TREND/HYBRID 시그널 감지(GPT 승인) → 즉시 청산: {trade.symbol} "
                f"{trade.side}→{new_dir} 현재={last_price:.2f}"
            )
            reason = "trend_opposite_signal_close"
            strategy_type = "TREND"
        else:
            send_tg(
                f"⚠️ (WS) 박스 포지션 반대 시그널 감지(GPT 승인) → 즉시 청산: {trade.symbol} "
                f"{trade.side}→{new_dir} 현재={last_price:.2f}"
            )
            reason = "range_opposite_signal_close"
            strategy_type = "RANGE"

        log_signal(
            event="CLOSE",
            symbol=trade.symbol,
            strategy_type=strategy_type,
            direction=trade.side,
            price=last_price,
            qty=qty,
            reason=reason,
            pnl=pnl,
        )
        # GPT EXIT 이벤트 CSV 기록
        try:
            log_gpt_exit_event(
                symbol=trade.symbol,
                regime=regime_label,
                side=trade.side,
                scenario=scenario,
                action="CLOSE",
                pnl_pct=pnl_pct_signed,
                extra={
                    "new_signal_dir": new_dir,
                    "new_signal_source": new_src,
                    "gpt_data": gpt_data,
                    "exit_ctx": gpt_ctx,
                },
            )
        except Exception as e:
            log(f"[GPT_EXIT_LOG][OPP] failed: {e}")

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
    "maybe_downgrade_trend_to_range",
    "maybe_early_exit_trend",
    "maybe_close_on_opposite_signal",
    "maybe_force_close_on_opposite_signal",
]
