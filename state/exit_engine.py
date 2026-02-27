# exit_engine.py
"""
exit_engine.py (Binance USDT-M Futures 전용 WS EXIT 오케스트레이터)
=======================================================================

역할
-----------------------------------------------------
Binance USDT-M Futures WebSocket 데이터 기반으로
현재 포지션(Trade)에 대한 EXIT / HOLD 판단을 수행하는 레이어.

핵심 구조
-----------------------------------------------------
1) Binance WS 1m/5m/15m 캔들 + 오더북 기반 데이터 사용
2) TA 기반 레짐/지표 생성 후 EXIT 컨텍스트 구성
3) RUNTIME 강제 EXIT 규칙 (반대 추세 + 수익/손실 기준)
4) GPT EXIT 판단 (fallback_action="HOLD")
5) 부분청산(close_ratio) 지원
6) 강한 추세에서 CLOSE → HOLD 오버라이드
7) EXIT GPT 호출 쿨다운 적용
8) 실제 데이터 기반 DB 업데이트 (폴백 금지)

주의
-----------------------------------------------------
- BingX 관련 코드/필드/주석 완전 제거
- REST 기반 EXIT 강제 로직 금지
- Binance WebSocket 기반 데이터만 사용
- 이 파일은 주문 실행(close_position_market)만 호출하고,
  주문 생성 로직은 포함하지 않는다.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional
import math
import time

from infra.telelog import log, send_tg
from infra.market_data_ws import (
    get_klines as ws_get_klines,
    get_klines_with_volume as ws_get_klines_with_vol,
)
from execution.order_executor import close_position_market
from events.signals_logger import (
    log_signal,
    log_candle_snapshot,
    log_gpt_exit_event,
    log_skip_event,
)
from state.trader_state import Trade
from strategy.gpt_decider_entry import ask_exit_decision_safe
from strategy.indicators import build_regime_features_from_candles

# Binance WS EXIT 오케스트레이터 로그 prefix
LOG_PW = "[PW_BINANCE]"
LOG_GPT = "[PW_BINANCE][GPT_EXIT]"
LOG_DB = "[PW_BINANCE][TRADE_DB]"

# 이 모듈에서 권장하는 EXIT 체크 주기 (초 단위)
EXIT_CHECK_INTERVAL_SEC: float = 60.0

# EXIT GPT 호출 쿨다운 관리용 (포지션별 마지막 호출 시각)
_EXIT_GPT_LAST_CALL_TS: Dict[str, float] = {}

# DB 연동 (bt_trades)
try:
    from state.db_core import SessionLocal  # type: ignore
    from state.db_models import Trade as TradeORM  # type: ignore
except Exception:  # pragma: no cover - DB 미준비 환경 방어
    SessionLocal = None  # type: ignore
    TradeORM = None  # type: ignore


# ─────────────────────────────────────────
# 내부 헬퍼: 레짐/지표 정합 + DB 등
# ─────────────────────────────────────────


def _normalize_regime_keys(regime: Dict[str, Any]) -> Dict[str, Any]:
    """TA 기반 regime dict 의 핵심 키를 정규화한다."""
    normalized = dict(regime)

    # rsi_last alias
    if "rsi_last" not in normalized and "rsi" in normalized:
        v = normalized.get("rsi")
        if isinstance(v, (int, float)) and math.isfinite(float(v)):
            normalized["rsi_last"] = float(v)

    # macd_hist alias
    if "macd_hist" not in normalized and "macd_hist_last" in normalized:
        v = normalized.get("macd_hist_last")
        if isinstance(v, (int, float)) and math.isfinite(float(v)):
            normalized["macd_hist"] = float(v)

    # atr_pct 보완
    if "atr_pct" not in normalized:
        atr_val = normalized.get("atr")
        last_close = normalized.get("last_close")
        if (
            isinstance(atr_val, (int, float))
            and math.isfinite(float(atr_val))
            and isinstance(last_close, (int, float))
            and math.isfinite(float(last_close))
            and float(last_close) > 0
        ):
            normalized["atr_pct"] = float(atr_val) / float(last_close)

    # range_pct 보완
    if "range_pct" not in normalized:
        high_recent = normalized.get("high_recent")
        low_recent = normalized.get("low_recent")
        last_close = normalized.get("last_close")
        if (
            isinstance(high_recent, (int, float))
            and math.isfinite(float(high_recent))
            and isinstance(low_recent, (int, float))
            and math.isfinite(float(low_recent))
            and isinstance(last_close, (int, float))
            and math.isfinite(float(last_close))
            and float(last_close) > 0
        ):
            normalized["range_pct"] = (
                float(high_recent) - float(low_recent)
            ) / float(last_close)

    return normalized


def _safe_num_or_none(v: Any) -> Optional[float]:
    """유한 실수이면 float, 아니면 None."""
    if isinstance(v, (int, float)):
        f = float(v)
        if math.isfinite(f):
            return f
    return None


def _build_market_features_from_regime(regime_features: Dict[str, Any]) -> Dict[str, Any]:
    """EXIT 용 market_features dict 를 regime_features 기반으로 구성."""
    if not isinstance(regime_features, dict) or not regime_features:
        return {}

    reg = _normalize_regime_keys(regime_features)

    pattern: Dict[str, Any] = {}

    def _copy_num(src_key: str, dst_key: Optional[str] = None) -> None:
        val = _safe_num_or_none(reg.get(src_key))
        if val is not None:
            pattern[dst_key or src_key] = val

    # 핵심 패턴/지표
    _copy_num("pattern_score")
    _copy_num("trend_strength")
    _copy_num("range_strength")
    _copy_num("boxiness")
    _copy_num("volatility_score")
    _copy_num("reversal_probability")
    _copy_num("continuation_probability")

    # EXIT 에서 직접 보는 지표들
    _copy_num("atr_pct")
    _copy_num("stoch_k")
    if "stoch_k" not in pattern:
        _copy_num("stoch_rsi_k", "stoch_k")

    _copy_num("rsi_last")
    if "rsi_last" not in pattern:
        _copy_num("rsi", "rsi_last")

    _copy_num("macd_hist")
    if "macd_hist" not in pattern:
        _copy_num("macd_hist_last", "macd_hist")

    _copy_num("vol_zscore")
    if "vol_zscore" not in pattern:
        _copy_num("volume_zscore", "vol_zscore")

    _copy_num("wick_strength")
    _copy_num("liquidity_event_score")

    summary = None
    for key in ("pattern_summary", "regime_comment", "regime_label"):
        val = reg.get(key)
        if isinstance(val, str) and val.strip():
            summary = val.strip()
            break

    mf: Dict[str, Any] = {
        "timeframe": reg.get("timeframe", "5m"),
        "raw_regime_5m": reg,
        "pattern_features": pattern,
    }
    if summary:
        mf["pattern_summary"] = summary

    return mf


def _get_15m_trend_dir(symbol: str) -> str:
    """Binance WS 15m 캔들로 방향(LONG/SHORT)을 계산한다."""
    log(f"{LOG_PW}[TREND15] fetch 15m klines symbol={symbol}")
    try:
        candles_15m = ws_get_klines(symbol, "15m", 120)
        if not candles_15m:
            return ""

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
        log(f"{LOG_PW}[TREND15] ws_get_klines error symbol={symbol}: {e}")
        return ""


def _build_runtime_regime_features_for_gpt(symbol: str) -> Dict[str, Any]:
    """Binance WS 5m 캔들 기반 레짐/지표 스냅샷 생성."""
    try:
        candles_5m = ws_get_klines(symbol, "5m", 200)
    except Exception as e:
        log(f"{LOG_PW}[REGIME_FEAT] ws_get_klines error symbol={symbol}: {e}")
        return {}

    if not candles_5m or len(candles_5m) < 30:
        return {}

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
        log(f"{LOG_PW}[REGIME_FEAT] build_regime_features_from_candles error symbol={symbol}: {e}")
        return {}

    if not isinstance(base_features, dict) or not base_features:
        return {}

    features: Dict[str, Any] = _normalize_regime_keys(base_features)
    features.setdefault("version", "binance_ws_runtime_v1")
    features.setdefault("symbol", symbol)
    features.setdefault("timeframe", "5m")

    # 15m 방향 추가
    try:
        trend15_dir = _get_15m_trend_dir(symbol)
        if trend15_dir:
            features["trend15_dir"] = trend15_dir
    except Exception as e:
        log(f"{LOG_PW}[REGIME_FEAT] trend15_dir error symbol={symbol}: {e}")

    return features


def _get_last_candle_with_volume(
    symbol: str,
) -> Optional[Tuple[int, float, float, float, float, float]]:
    """Binance WS 기준 최근 캔들 1개 조회 (1m 우선, 없으면 5m)."""
    try:
        candles = ws_get_klines_with_vol(symbol, "1m", 1)
        if not candles:
            candles = ws_get_klines_with_vol(symbol, "5m", 1)
    except Exception as e:
        log(f"{LOG_PW}[EXIT] kline fetch error symbol={symbol}: {e}")
        return None

    if not candles:
        return None

    row = candles[-1]
    if len(row) < 5:
        return None

    try:
        ts = int(row[0])
        o = float(row[1])
        h = float(row[2])
        l = float(row[3])
        c = float(row[4])
        v = float(row[5]) if len(row) >= 6 else 0.0
    except (TypeError, ValueError):
        return None

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
    """GPT EXIT 판단용 공통 컨텍스트 생성 (Binance WS 기준)."""
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
        if str(trade.side).upper() in ("SELL", "SHORT"):
            pnl_pct = -pnl_pct
        ctx["pnl_pct"] = float(pnl_pct)
    else:
        ctx["pnl_pct"] = None

    if extra:
        ctx.update(extra)

    # 레짐/지표 + market_features
    try:
        regime_features = _build_runtime_regime_features_for_gpt(symbol=trade.symbol)
        if regime_features:
            ctx["regime_features"] = regime_features
            mf = _build_market_features_from_regime(regime_features)
            if mf:
                ctx["market_features"] = mf
    except Exception as e:  # pragma: no cover
        log(f"{LOG_PW}[REGIME_FEAT] context build error symbol={trade.symbol}: {e}")

    # full WS 피처(ENTRY와 동일) 전달
    full_features: Optional[Dict[str, Any]] = None
    try:
        from infra.market_features_ws import build_entry_features_ws

        full_features = build_entry_features_ws(trade.symbol)
        if isinstance(full_features, dict):
            ctx["full_market_features"] = full_features
    except Exception as e:
        log(f"{LOG_PW}[EXIT_FULL_FEATURES] failed symbol={trade.symbol}: {e}")
        full_features = None

    # 1m/5m/15m OHLCV + RSI/MACD 시리즈 평탄화
    try:
        if isinstance(full_features, dict):
            tf_map = full_features.get("timeframes", {})
            if not isinstance(tf_map, dict):
                tf_map = {}

            for iv in ("1m", "5m", "15m"):
                t = tf_map.get(iv)
                if not isinstance(t, dict):
                    continue

                if "raw_ohlcv_last20" in t:
                    ctx[f"{iv}_raw_ohlcv_last20"] = t["raw_ohlcv_last20"]

                indicators = t.get("indicators", {})
                if isinstance(indicators, dict):
                    if "rsi_series" in indicators:
                        ctx[f"{iv}_rsi_series"] = indicators["rsi_series"]
                    if "macd_hist_series" in indicators:
                        ctx[f"{iv}_macd_hist_series"] = indicators["macd_hist_series"]

            if "multi_timeframe" in full_features:
                ctx["multi_timeframe_summary"] = full_features["multi_timeframe"]

            if "orderbook" in full_features:
                ctx["orderbook_full"] = full_features["orderbook"]
    except Exception as e:
        log(f"{LOG_PW}[EXIT_PATTERN_SERIES] failed symbol={trade.symbol}: {e}")

    return ctx


def _get_trade_db_id(trade: Trade) -> Optional[int]:
    """Trade 객체에서 ORM PK 후보를 가져온다."""
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
    """bt_trades 레코드에 종료 정보 반영 (유효 데이터만, no-fallback)."""
    if SessionLocal is None or TradeORM is None:
        log(f"{LOG_DB} SessionLocal/TradeORM 없음 → trades 업데이트 생략 (no fallback)")
        return

    trade_db_id = _get_trade_db_id(trade)
    if trade_db_id is None:
        log(f"{LOG_DB} trade 객체에 db_id/id/trade_id 없음 → trades 업데이트 생략 (no fallback)")
        return

    if close_price <= 0 or getattr(trade, "entry", 0.0) <= 0 or getattr(trade, "qty", 0.0) <= 0:
        log(
            f"{LOG_DB} invalid price/entry/qty for trade_id={trade_db_id} "
            f"entry={getattr(trade, 'entry', None)} close={close_price} qty={getattr(trade, 'qty', None)} → 업데이트 생략"
        )
        return

    if event_ts_ms is None:
        log(f"{LOG_DB} event_ts_ms 없음 → trade_id={trade_db_id} 업데이트 생략 (no fallback)")
        return

    try:
        session = SessionLocal()
    except Exception as e:  # pragma: no cover
        log(f"{LOG_DB} Session 생성 실패 trade_id={trade_db_id}: {e}")
        return

    try:
        orm: Optional[TradeORM] = session.get(TradeORM, trade_db_id)  # type: ignore[arg-type]
        if orm is None:
            log(f"{LOG_DB} TradeORM 레코드 없음 id={trade_db_id} → 업데이트 생략")
            return

        exit_dt = datetime.fromtimestamp(event_ts_ms / 1000.0, tz=timezone.utc)

        if isinstance(pnl, (int, float)):
            real_pnl = float(pnl)
        else:
            if str(trade.side).upper() in ("BUY", "LONG"):
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
            if getattr(trade, "source", None):
                orm.regime_at_exit = getattr(trade, "source")
        except Exception:
            pass

        note_bits: List[str] = []
        if getattr(orm, "note", None):
            note_bits.append(orm.note)
        note_bits.append(f"runtime_close={close_reason}")
        if cooldown_tag:
            note_bits.append(f"cooldown={cooldown_tag}")
        orm.note = " | ".join(note_bits)

        session.commit()
        log(
            f"{LOG_DB} updated trade_id={trade_db_id} "
            f"reason={close_reason} pnl={real_pnl:.4f} pnl_pct={(pnl_pct or 0.0)*100:.4f}%"
        )
    except Exception as e:  # pragma: no cover
        session.rollback()
        log(f"{LOG_DB} update 실패 trade_id={trade_db_id}: {e}")
    finally:
        session.close()


# ─────────────────────────────────────────
# EXIT GPT 쿨다운 헬퍼
# ─────────────────────────────────────────


def _get_exit_gpt_cooldown_sec(settings: Any) -> float:
    """EXIT GPT 최소 호출 간격(초)을 설정에서 가져온다."""
    try:
        v = getattr(settings, "exit_gpt_cooldown_sec", None)
        if isinstance(v, (int, float)) and v > 0:
            return float(v)
    except Exception:
        pass
    return float(EXIT_CHECK_INTERVAL_SEC)


def _get_exit_gpt_trade_key(trade: Trade) -> str:
    """EXIT GPT 쿨다운용 포지션 키."""
    trade_db_id = _get_trade_db_id(trade)
    if trade_db_id is not None:
        return f"db:{trade_db_id}"

    try:
        symbol = getattr(trade, "symbol", "UNKNOWN")
        side = getattr(trade, "side", "UNKNOWN")
        entry = float(getattr(trade, "entry", 0.0) or 0.0)
        qty = float(getattr(trade, "qty", 0.0) or 0.0)
    except Exception:
        symbol = getattr(trade, "symbol", "UNKNOWN")
        side = getattr(trade, "side", "UNKNOWN")
        entry = 0.0
        qty = 0.0

    return f"{symbol}:{side}:{entry:.8f}:{qty:.8f}"


# ─────────────────────────────────────────
# RUNTIME EXIT용 추가 헬퍼
# ─────────────────────────────────────────


def _is_opposite_side(trade_side: str, trend_dir: str) -> bool:
    """포지션 방향과 15m 추세 방향이 반대인지 판별."""
    side = (trade_side or "").upper()
    trend = (trend_dir or "").upper()
    if side in ("BUY", "LONG") and trend == "SHORT":
        return True
    if side in ("SELL", "SHORT") and trend == "LONG":
        return True
    return False


def _runtime_force_close(
    *,
    trade: Trade,
    last_price: float,
    qty: float,
    regime_label: str,
    scenario: str,
    candle_ts: int,
    reason_suffix: str,
    extra_ctx: Optional[Dict[str, Any]] = None,
) -> bool:
    """규칙 기반(RUNTIME) 강제 청산 공통 처리."""
    try:
        close_position_market(trade.symbol, trade.side, qty)

        entry = float(getattr(trade, "entry", 0.0) or 0.0)
        if str(trade.side).upper() in ("BUY", "LONG"):
            pnl = (last_price - entry) * qty
        else:
            pnl = (entry - last_price) * qty

        base_notional = entry * qty if entry > 0 and qty > 0 else 0.0
        pnl_pct_signed = (pnl / base_notional) if base_notional > 0 else None

        reason = f"runtime_{reason_suffix}"
        send_tg(
            f"⚠️ [BINANCE WS] RUNTIME EXIT 실행: {trade.symbol} {trade.side} "
            f"scenario={scenario} reason={reason} 진입={entry:.2f} 현재={last_price:.2f} "
            f"pnl={pnl:.4f}USDT ({(pnl_pct_signed or 0.0)*100:.2f}%)"
        )

        log_signal(
            event="CLOSE",
            symbol=trade.symbol,
            strategy_type=regime_label,
            direction=trade.side,
            price=last_price,
            qty=qty,
            reason=reason,
            pnl=pnl,
        )

        try:
            log_gpt_exit_event(
                symbol=trade.symbol,
                regime=regime_label,
                side=trade.side,
                scenario=scenario,
                action="CLOSE",
                pnl_pct=pnl_pct_signed,
                extra={
                    "runtime_reason": reason,
                    "exit_ctx": extra_ctx or {},
                },
            )
        except Exception as e:
            log(f"{LOG_GPT}[RUNTIME_CLOSE_LOG] failed symbol={trade.symbol}: {e}")

        _update_trade_close_in_db(
            trade=trade,
            close_price=float(last_price),
            close_reason=reason,
            event_ts_ms=int(candle_ts),
            pnl=float(pnl),
            cooldown_tag=None,
        )
        return True

    except Exception as e:
        log(f"{LOG_PW}[RUNTIME_EXIT] close failed symbol={trade.symbol}: {e}")
        return False


# ─────────────────────────────────────────
# EXIT 디버그 요약 텔레그램 전송 헬퍼
# ─────────────────────────────────────────


def _extract_gpt_reason_for_tg(gpt_data: Any) -> str:
    """gpt_decider 응답에서 사람이 볼 코멘트만 추출."""
    text: str = ""
    try:
        if isinstance(gpt_data, dict):
            for key in ("reason", "exit_reason", "summary", "comment", "explanation"):
                val = gpt_data.get(key)
                if isinstance(val, str) and val.strip():
                    text = val.strip()
                    break

            if not text:
                parsed = gpt_data.get("parsed")
                if isinstance(parsed, dict):
                    for key in ("reason", "summary", "comment"):
                        val = parsed.get(key)
                        if isinstance(val, str) and val.strip():
                            text = val.strip()
                            break

            if not text:
                for key in ("raw_text", "response_text", "content"):
                    val = gpt_data.get(key)
                    if isinstance(val, str) and val.strip():
                        text = val.strip()
                        break

        elif isinstance(gpt_data, str):
            text = gpt_data.strip()

    except Exception:
        text = ""

    if not text:
        return ""
    if len(text) > 400:
        return text[:400] + " ... (생략)"
    return text


def _build_regime_summary_for_tg(regime_features: Dict[str, Any]) -> str:
    """regime_features dict 에서 핵심 지표 몇 개만 한 줄 요약으로 만든다."""
    if not isinstance(regime_features, dict) or not regime_features:
        return ""

    key_candidates = [
        "trend_label",
        "trend_strength",
        "trend_score",
        "boxiness",
        "volatility_score",
        "rsi",
        "stoch_rsi",
        "adx",
        "macd_hist",
        "atr_pct",
    ]

    parts: List[str] = []
    for key in key_candidates:
        if key not in regime_features:
            continue
        val = regime_features.get(key)
        try:
            if isinstance(val, float):
                parts.append(f"{key}={val:.3f}")
            else:
                parts.append(f"{key}={val}")
        except Exception:
            parts.append(f"{key}={val}")

    if not parts:
        return ""
    return ", ".join(parts)


def _send_exit_debug_summary(
    *,
    trade: Trade,
    scenario: str,
    action: str,
    gpt_ctx: Dict[str, Any],
    gpt_data: Any,
) -> None:
    """EXIT 판단 시 상세 분석 요약을 텔레그램으로 전송 (옵션)."""
    try:
        pnl_pct = gpt_ctx.get("pnl_pct")
        pnl_pct_str = f"{pnl_pct*100:.3f}%" if isinstance(pnl_pct, (int, float)) else "n/a"

        candle = gpt_ctx.get("candle_1m") or {}
        o = candle.get("open")
        h = candle.get("high")
        l = candle.get("low")
        c = candle.get("close")
        v = candle.get("volume")

        candle_bits: List[str] = []
        if all(isinstance(x, (int, float)) for x in (o, h, l, c)):
            candle_bits.append(
                f"O/H/L/C={float(o):.2f}/{float(h):.2f}/{float(l):.2f}/{float(c):.2f}"
            )
        if isinstance(v, (int, float)):
            candle_bits.append(f"거래량={float(v):.3f}")
        candle_line = " / ".join(candle_bits) if candle_bits else "캔들 정보 부족"

        regime_features = gpt_ctx.get("regime_features") or {}
        regime_summary = _build_regime_summary_for_tg(regime_features)

        gpt_reason = _extract_gpt_reason_for_tg(gpt_data)

        side_raw = (trade.side or "").upper()
        if side_raw in ("BUY", "LONG"):
            side_text = "롱"
        elif side_raw in ("SELL", "SHORT"):
            side_text = "숏"
        else:
            side_text = side_raw or "UNKNOWN"

        lines: List[str] = [
            "📘 [BINANCE WS EXIT 실시간 분석 요약]",
            f"- 심볼: {trade.symbol}",
            f"- 방향: {side_text}",
            f"- 시나리오: {scenario}",
            f"- GPT 결론: {action}",
            f"- 현재 PnL: {pnl_pct_str}",
            "",
            "📈 1분봉 캔들:",
            f"  · {candle_line}",
        ]

        if regime_summary:
            lines.extend(["", "📊 레짐/지표 요약:", f"  · {regime_summary}"])

        if gpt_reason:
            lines.extend(["", "🧠 GPT 코멘트:", f"  {gpt_reason}"])

        send_tg("\n".join(lines))

    except Exception as e:  # pragma: no cover
        log(f"{LOG_PW}[EXIT_DEBUG] build/send summary failed symbol={trade.symbol}: {e}")


# ─────────────────────────────────────────
# 공개 함수: GPT 기반 단일 EXIT 체크
# ─────────────────────────────────────────


def maybe_exit_with_gpt(
    trade: Trade,
    settings: Any,
    *,
    scenario: str = "GENERIC_EXIT_CHECK",
) -> bool:
    """현재 포지션에 대해 GPT EXIT/HOLD 판단 후, 필요 시 청산 실행.

    반환값:
        True  → 이 함수에서 실제 청산을 실행한 경우
        False → 청산을 실행하지 않은 경우(HOLD 또는 데이터 부족/오류)
    """
    regime_label = getattr(trade, "source", "") or "UNKNOWN"

    qty = float(getattr(trade, "qty", 0.0) or 0.0)
    if qty <= 0:
        log(f"{LOG_PW}[EXIT] invalid qty<=0 → skip (symbol={trade.symbol})")
        return False

    # 최신 캔들 확보 (WS 전용)
    last_candle = _get_last_candle_with_volume(trade.symbol)
    if last_candle is None:
        log(f"{LOG_PW}[EXIT] no recent WS candle → skip (symbol={trade.symbol})")
        return False

    candle_ts, o, h, l, c, v = last_candle
    entry = float(getattr(trade, "entry", 0.0) or 0.0)
    if c <= 0 or entry <= 0:
        log(
            f"{LOG_PW}[EXIT] invalid price data "
            f"(symbol={trade.symbol}, entry={entry}, last_close={c}) → skip"
        )
        return False

    # 캔들 스냅샷 로그
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
            extra=f"gpt_exit_check=1;scenario={scenario};market=binance_futures_ws",
        )
    except Exception as e:
        log(f"{LOG_PW}[EXIT] candle snapshot log failed symbol={trade.symbol}: {e}")

    # GPT EXIT 컨텍스트
    ctx_extra = {
        "check_type": "generic_exit",
        "timeframe": "1m",
        "market": "binance_usdtm_futures_ws",
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

    pnl_pct = gpt_ctx.get("pnl_pct")
    regime_features = gpt_ctx.get("regime_features") or {}
    trend15_dir = regime_features.get("trend15_dir") or ""

    opposite = _is_opposite_side(trade.side, trend15_dir) if trend15_dir else False

    # ── RUNTIME 규칙 1: 반대 방향 + 수익/손실 기준 강제 EXIT ─────────────────
    if isinstance(pnl_pct, (int, float)):
        # 반대 방향 + 수익 0.2% 이상 → 즉시 익절
        if opposite and pnl_pct >= 0.002:
            log(
                f"{LOG_PW}[RUNTIME_EXIT] opposite dir & pnl>=0.2% → force close "
                f"(symbol={trade.symbol}, pnl={pnl_pct*100:.3f}%)"
            )
            return _runtime_force_close(
                trade=trade,
                last_price=c,
                qty=qty,
                regime_label=regime_label,
                scenario=scenario,
                candle_ts=int(candle_ts),
                reason_suffix="opposite_profit_take",
                extra_ctx=gpt_ctx,
            )

        # 반대 방향 + 손실 -0.5% 이하 → 즉시 손절
        if opposite and pnl_pct <= -0.005:
            log(
                f"{LOG_PW}[RUNTIME_EXIT] opposite dir & pnl<=-0.5% → force close "
                f"(symbol={trade.symbol}, pnl={pnl_pct*100:.3f}%)"
            )
            return _runtime_force_close(
                trade=trade,
                last_price=c,
                qty=qty,
                regime_label=regime_label,
                scenario=scenario,
                candle_ts=int(candle_ts),
                reason_suffix="opposite_loss_cut",
                extra_ctx=gpt_ctx,
            )

    # EXIT GPT 쿨다운
    trade_key = _get_exit_gpt_trade_key(trade)
    cooldown_sec = _get_exit_gpt_cooldown_sec(settings)
    now_ts = time.time()
    last_call_ts = _EXIT_GPT_LAST_CALL_TS.get(trade_key)

    if last_call_ts is not None and (now_ts - last_call_ts) < cooldown_sec:
        remaining = cooldown_sec - (now_ts - last_call_ts)
        try:
            log_skip_event(
                symbol=trade.symbol,
                regime=regime_label,
                source="position_watch_ws.maybe_exit_with_gpt",
                side=trade.side,
                reason="gpt_exit_cooldown",
                extra={
                    "scenario": scenario,
                    "market": "binance_usdtm_futures_ws",
                    "cooldown_sec": cooldown_sec,
                    "remaining_sec": remaining,
                    "exit_ctx": gpt_ctx,
                },
            )
        except Exception as e:
            log(f"{LOG_GPT}[COOLDOWN][SKIP_LOG] failed symbol={trade.symbol}: {e}")

        log(
            f"{LOG_GPT}[COOLDOWN] skip GPT EXIT call "
            f"(trade_key={trade_key}, remaining={remaining:.1f}s)"
        )
        return False

    # GPT에 EXIT 여부 질의 (fallback_action='HOLD'로 추가 EXIT 레이어만 비활성화)
    action, gpt_data = ask_exit_decision_safe(
        symbol=trade.symbol,
        source=getattr(trade, "source", None),
        side=trade.side,
        scenario=scenario,
        last_price=c,
        entry_price=entry,
        leverage=float(getattr(settings, "leverage", 0.0) or 0.0),
        extra=gpt_ctx,
        fallback_action="HOLD",
    )
    _EXIT_GPT_LAST_CALL_TS[trade_key] = now_ts

    # ── GPT 기반 부분청산(close_ratio) ──────────────────────────────────────
    close_ratio: Optional[float] = None
    if isinstance(gpt_data, dict):
        cr = gpt_data.get("close_ratio")
        if isinstance(cr, (int, float)):
            close_ratio = max(0.0, min(float(cr), 1.0))

    if action == "CLOSE" and close_ratio is not None and 0 < close_ratio < 1:
        partial_qty = qty * close_ratio
        try:
            close_position_market(trade.symbol, trade.side, partial_qty)

            remaining_qty = qty - partial_qty
            setattr(trade, "qty", remaining_qty)

            send_tg(
                f"{LOG_GPT}[PARTIAL] {close_ratio*100:.1f}% 부분 청산 실행 "
                f"(old_qty={qty:.6f}, new_qty={remaining_qty:.6f})"
            )

            log_signal(
                event="CLOSE",
                symbol=trade.symbol,
                strategy_type=regime_label,
                direction=trade.side,
                price=c,
                qty=partial_qty,
                reason="gpt_partial_close",
                pnl=None,
            )

            # 부분 청산만 수행 → 포지션은 유지
            return False

        except Exception as e:
            log(f"{LOG_GPT}[PARTIAL] close failed symbol={trade.symbol}: {e}")

    # ── RUNTIME 규칙 2: 강한 추세 + PnL>0 + 방향 동일 → CLOSE→HOLD 오버라이드 ─────
    try:
        trend_strength = regime_features.get("trend_strength")
        atr_pct = regime_features.get("atr_pct")
        pnl_for_override = gpt_ctx.get("pnl_pct")

        same_dir = False
        if trend15_dir:
            same_dir = not _is_opposite_side(trade.side, trend15_dir)

        if (
            action == "CLOSE"
            and isinstance(trend_strength, (int, float))
            and isinstance(atr_pct, (int, float))
            and isinstance(pnl_for_override, (int, float))
            and trend_strength > 0.75
            and atr_pct > 0.004
            and pnl_for_override > 0.0
            and same_dir
        ):
            log(
                f"{LOG_PW}[TREND_HOLD_OVERRIDE] strong trend & pnl>0 & same dir "
                f"→ override CLOSE→HOLD (symbol={trade.symbol}, pnl={pnl_for_override*100:.3f}%)"
            )
            action = "HOLD"
    except Exception as e:
        log(f"{LOG_PW}[TREND_HOLD_OVERRIDE] failed symbol={trade.symbol}: {e}")

    # EXIT 디버그 요약 (옵션)
    if getattr(settings, "exit_debug_notify", False):
        _send_exit_debug_summary(
            trade=trade,
            scenario=scenario,
            action=action,
            gpt_ctx=gpt_ctx,
            gpt_data=gpt_data,
        )

    # ── GPT 결과가 HOLD 인 경우 ─────────────────────────────────────────────
    if action != "CLOSE":
        exit_reason = ""
        if isinstance(gpt_data, dict):
            exit_reason = str(gpt_data.get("reason", "") or "").strip()
        if not exit_reason:
            exit_reason = "추세 유지. 지표 근거 변화 없음 → HOLD."

        try:
            send_tg(f"{LOG_GPT}[HOLD] {exit_reason}")
        except Exception as e:
            log(f"{LOG_GPT}[HOLD] telegram send failed symbol={trade.symbol}: {e}")

        try:
            log_skip_event(
                symbol=trade.symbol,
                regime=regime_label,
                source="position_watch_ws.maybe_exit_with_gpt",
                side=trade.side,
                reason="gpt_exit_hold",
                extra={
                    "scenario": scenario,
                    "market": "binance_usdtm_futures_ws",
                    "gpt_action": action,
                    "gpt_data": gpt_data,
                    "exit_ctx": gpt_ctx,
                },
            )
        except Exception as e:
            log(f"{LOG_GPT}[HOLD][SKIP_LOG] failed symbol={trade.symbol}: {e}")

        pnl_pct_val = gpt_ctx.get("pnl_pct")
        pnl_pct_str = f"{pnl_pct_val*100:.3f}%" if isinstance(pnl_pct_val, (int, float)) else "n/a"

        log(
            f"{LOG_GPT}[HOLD] 포지션 유지 "
            f"(symbol={trade.symbol}, side={trade.side}, scenario={scenario}, pnl≈{pnl_pct_str})"
        )
        return False

    # ── action == "CLOSE" → 전체 청산 ──────────────────────────────────────
    try:
        exit_reason = ""
        if isinstance(gpt_data, dict):
            exit_reason = str(gpt_data.get("reason", "") or "").strip()
        if not exit_reason:
            exit_reason = "추세/지표 기반으로 포지션 정리 필요 → CLOSE."

        try:
            send_tg(f"{LOG_GPT}[CLOSE] {exit_reason}")
        except Exception as e:
            log(f"{LOG_GPT}[CLOSE] telegram send failed symbol={trade.symbol}: {e}")

        close_position_market(trade.symbol, trade.side, qty)

        if str(trade.side).upper() in ("BUY", "LONG"):
            pnl = (c - entry) * qty
        else:
            pnl = (entry - c) * qty

        base_notional = entry * qty if entry > 0 and qty > 0 else 0.0
        pnl_pct_signed = (pnl / base_notional) if base_notional > 0 else None

        reason = f"gpt_exit_{scenario.lower()}"
        send_tg(
            f"⚠️ [BINANCE WS] GPT EXIT 실행: {trade.symbol} {trade.side} "
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

        try:
            log_gpt_exit_event(
                symbol=trade.symbol,
                regime=regime_label,
                side=trade.side,
                scenario=scenario,
                action="CLOSE",
                pnl_pct=pnl_pct_signed,
                extra={
                    "market": "binance_usdtm_futures_ws",
                    "gpt_data": gpt_data,
                    "exit_ctx": gpt_ctx,
                },
            )
        except Exception as e:
            log(f"{LOG_GPT}[CLOSE_LOG] failed symbol={trade.symbol}: {e}")

        _update_trade_close_in_db(
            trade=trade,
            close_price=float(c),
            close_reason=reason,
            event_ts_ms=int(candle_ts),
            pnl=float(pnl),
        )
        return True

    except Exception as e:
        log(f"{LOG_PW}[EXIT] close failed symbol={trade.symbol}: {e}")
        return False


__all__ = [
    "maybe_exit_with_gpt",
    "EXIT_CHECK_INTERVAL_SEC",
]