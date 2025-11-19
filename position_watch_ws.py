# position_watch_ws.py
# ====================================================
# 역할
# ----------------------------------------------------
# - 웹소켓 캔들/호가에서 현재 시장 상태 요약 피처를 만들고,
#   열린 포지션(Trade)에 대해 GPT-5.1(gpt_decider)을 통해
#   EXIT 여부(HOLD/CLOSE)만 판단한 뒤, 실제 청산/로그/DB 업데이트만 수행하는 얇은 레이어.
#
# 2025-11-19 패치 (v2: 전면 GPT EXIT 레이어화)
# ----------------------------------------------------
# - 기존 RANGE/TREND 조기익절/조기청산, 박스↔추세 전환, 반대 시그널 강제 청산 등
#   모든 Python 규칙 기반 EXIT 로직을 제거했다.
# - position_watch_ws.py 는 더 이상 "전략/레짐 판단"을 하지 않고,
#   · WS 캔들에서 last_price 와 레짐 피처(regime_features_5m)만 계산,
#   · gpt_decider.ask_exit_decision_safe(...) 를 호출하여
#     GPT-5.1 이 포지션 단위로 HOLD/CLOSE 를 결정,
#   · action="CLOSE" 일 때만 거래소 청산 + CSV/DB/Telegram 후처리만 담당한다.
# - GPT 호출 실패 시 fallback_action="HOLD" 로 동작하여
#   추가적인 런타임 EXIT 레이어만 비활성화되고,
#   거래소 TP/SL / 전역 손실 한도 등 하드 리스크 가드는 그대로 유지된다는 전제.
#
# 사용 방법 (run_bot_ws 예시)
# ----------------------------------------------------
#   from position_watch_ws import maybe_exit_with_gpt
#
#   for trade in open_trades:
#       maybe_exit_with_gpt(trade, settings)
#
# - 이 파일은 "EXIT 어댑터" 이고, 실제 판단 로직은 gpt_decider.py 의
#   ask_exit_decision_safe / GPT 프롬프트에서 관리한다.

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional

from telelog import log, send_tg
from market_data_ws import (
    get_klines as ws_get_klines,
    get_klines_with_volume as ws_get_klines_with_vol,
)
from exchange_api import close_position_market
from signals_logger import (
    log_signal,
    log_candle_snapshot,
    log_gpt_exit_event,
    log_skip_event,
)
from trader import Trade
from gpt_decider import ask_exit_decision_safe
from indicators import build_regime_features_from_candles

# DB 연동 (bt_trades)
try:
    from db_core import SessionLocal  # type: ignore
    from db_models import Trade as TradeORM  # type: ignore
except Exception:  # pragma: no cover - DB 미준비 환경 방어
    SessionLocal = None  # type: ignore
    TradeORM = None  # type: ignore


# ─────────────────────────────────────────
# 내부 헬퍼: 방향/레짐/DB 등
# ─────────────────────────────────────────


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


def _get_15m_trend_dir(symbol: str) -> str:
    """웹소켓 버퍼에서 15m 캔들을 가져와 대략적인 방향만 문자열로 돌려준다.

    - build_regime_features_from_candles 와 함께 GPT 컨텍스트에 참고용으로만 넣는다.
    """
    log(f"[PW] (WS) fetch 15m for trend dir symbol={symbol}")
    try:
        candles_15m = ws_get_klines(symbol, "15m", 120)
        if not candles_15m:
            return ""
        # 간단히 마지막 15m 캔들의 방향만 본다.
        last = candles_15m[-1]
        try:
            o = float(last[1])
            c = float(last[4])
        except (TypeError, ValueError):
            return ""
        if o <= 0 or c <= 0:
            return ""
        if c > o:
            return "LONG"
        if c < o:
            return "SHORT"
        return ""
    except Exception as e:
        log(f"[15m DIR WS] error: {e}")
        return ""


def _build_runtime_regime_features_for_gpt(symbol: str) -> Dict[str, Any]:
    """5m 웹소켓 캔들 기반 레짐/지표 스냅샷 생성.

    - indicators.build_regime_features_from_candles(...) 를 재사용해서
      entry_flow.py 와 동일한 포맷의 regime_features_5m 을 만든다.
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


def _get_last_candle_with_volume(symbol: str) -> Optional[Tuple[int, float, float, float, float, float]]:
    """1m → 5m 순서로 가장 최근 캔들 한 개를 가져온다.

    반환: (ts, open, high, low, close, volume) 또는 None
    """
    try:
        candles = ws_get_klines_with_vol(symbol, "1m", 1)
        if not candles:
            candles = ws_get_klines_with_vol(symbol, "5m", 1)
    except Exception as e:
        log(f"[PW][EXIT] kline fetch error symbol={symbol}: {e}")
        return None

    if not candles:
        return None

    row = candles[-1]
    if len(row) < 5:
        return None

    ts = int(row[0])
    o = float(row[1])
    h = float(row[2])
    l = float(row[3])
    c = float(row[4])
    v = float(row[5]) if len(row) >= 6 else 0.0
    return ts, o, h, l, c, v


def _build_exit_context(
    *,
    trade: Trade,
    settings: Any,
    scenario: str,
    last_price: float,
    candle_ts_ms: Optional[int],
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """GPT에 넘기는 공통 EXIT 컨텍스트 생성.

    - trade, settings 에서 필요한 최소 정보만 요약해서 보낸다.
    - extra 로 시나리오별 세부 정보를 합친다.
    - 5m/websocket 기반 레짐/지표 스냅샷(regime_features)도 함께 포함.
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

    # 레짐/지표 스냅샷 추가 (실패해도 기존 동작에는 영향 없음)
    try:
        regime_features = _build_runtime_regime_features_for_gpt(symbol=trade.symbol)
        if regime_features:
            ctx["regime_features"] = regime_features
    except Exception as e:  # pragma: no cover - 방어적 로그
        log(f"[PW][REGIME_FEAT] context build error symbol={trade.symbol}: {e}")

    return ctx


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
# 공개 함수: GPT 기반 단일 EXIT 체크
# ─────────────────────────────────────────


def maybe_exit_with_gpt(
    trade: Trade,
    settings: Any,
    *,
    scenario: str = "GENERIC_EXIT_CHECK",
) -> bool:
    """현재 포지션에 대해 GPT-5.1 에 EXIT 여부를 묻고, 필요 시 청산한다.

    반환값:
        True  → 이 함수에서 실제 청산을 실행한 경우
        False → 청산을 실행하지 않은 경우(HOLD 또는 데이터 부족/오류)
    """
    regime_label = getattr(trade, "source", "") or "UNKNOWN"
    qty = float(getattr(trade, "qty", 0.0) or 0.0)
    if qty <= 0:
        log(f"[PW][EXIT] invalid qty<=0 for trade → skip (symbol={trade.symbol})")
        return False

    last_candle = _get_last_candle_with_volume(trade.symbol)
    if last_candle is None:
        log(f"[PW][EXIT] no recent candle for symbol={trade.symbol} → skip")
        return False

    candle_ts, o, h, l, c, v = last_candle
    entry = float(getattr(trade, "entry", 0.0) or 0.0)
    if c <= 0 or entry <= 0:
        log(
            f"[PW][EXIT] invalid price data symbol={trade.symbol} "
            f"entry={entry} last_close={c} → skip"
        )
        return False

    # 캔들 스냅샷 로그 (분석용)
    try:
        log_candle_snapshot(
            symbol=trade.symbol,
            tf="1m",
            candle_ts=int(candle_ts),
            open_=o,
            high=h,
            low=l,
            close=c,
            volume=v,
            strategy_type=regime_label,
            direction=trade.side,
            extra=f"gpt_exit_check=1;scenario={scenario}",
        )
    except Exception as e:
        log(f"[PW][EXIT] candle snapshot log failed: {e}")

    # GPT EXIT 컨텍스트 구성
    ctx_extra = {
        "check_type": "generic_exit",
        "timeframe": "1m",
        "candle_1m": {"open": o, "high": h, "low": l, "close": c, "volume": v},
    }
    gpt_ctx = _build_exit_context(
        trade=trade,
        settings=settings,
        scenario=scenario,
        last_price=c,
        candle_ts_ms=int(candle_ts),
        extra=ctx_extra,
    )

    # GPT 에 EXIT 여부 질의
    action, gpt_data = ask_exit_decision_safe(
        symbol=trade.symbol,
        source=getattr(trade, "source", None),
        side=trade.side,
        scenario=scenario,
        last_price=c,
        entry_price=entry,
        leverage=float(getattr(settings, "leverage", 0.0) or 0.0),
        extra=gpt_ctx,
        fallback_action="HOLD",  # GPT 오류/타임아웃 시 추가 EXIT 레이어만 비활성화
    )

    if action != "CLOSE":
        # HOLD or 기타 → 청산하지 않음
        try:
            log_skip_event(
                symbol=trade.symbol,
                regime=regime_label,
                source="position_watch_ws.maybe_exit_with_gpt",
                side=trade.side,
                reason="gpt_exit_hold",
                extra={
                    "scenario": scenario,
                    "gpt_action": action,
                    "gpt_data": gpt_data,
                    "exit_ctx": gpt_ctx,
                },
            )
        except Exception as e:
            log(f"[GPT_EXIT_LOG][HOLD][SKIP] failed: {e}")

        pnl_pct = gpt_ctx.get("pnl_pct")
        pnl_pct_str = f"{(pnl_pct or 0.0)*100:.3f}%" if pnl_pct is not None else "n/a"
        log(
            f"[GPT_EXIT] HOLD 결정 → 포지션 유지 "
            f"(symbol={trade.symbol}, side={trade.side}, scenario={scenario}, pnl≈{pnl_pct_str})"
        )
        return False

    # action == "CLOSE" → 실제 청산 실행
    try:
        close_position_market(trade.symbol, trade.side, qty)
        if trade.side == "BUY":
            pnl = (c - entry) * qty
        else:
            pnl = (entry - c) * qty

        # PnL% (DB 계산과 동일한 방식)
        base_notional = entry * qty if entry > 0 and qty > 0 else 0.0
        pnl_pct_signed = (pnl / base_notional) if base_notional > 0 else None

        reason = f"gpt_exit_{scenario.lower()}"
        send_tg(
            f"⚠️ (WS) GPT EXIT 실행: {trade.symbol} {trade.side} "
            f"scenario={scenario} 진입={entry:.2f} 현재={c:.2f} "
            f"pnl={pnl:.4f}USDT ({(pnl_pct_signed or 0.0)*100:.2f}%)"
        )
        log_signal(
            event="CLOSE",
            symbol=trade.symbol,
            strategy_type=regime_label,
            direction=trade.side,
            price=c,
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
                    "gpt_data": gpt_data,
                    "exit_ctx": gpt_ctx,
                },
            )
        except Exception as e:
            log(f"[GPT_EXIT_LOG][CLOSE] failed: {e}")

        _update_trade_close_in_db(
            trade=trade,
            close_price=float(c),
            close_reason=reason,
            event_ts_ms=int(candle_ts),
            pnl=float(pnl),
        )

        return True
    except Exception as e:
        log(f"[PW][EXIT] close failed symbol={trade.symbol}: {e}")
        return False


__all__ = [
    "maybe_exit_with_gpt",
]
