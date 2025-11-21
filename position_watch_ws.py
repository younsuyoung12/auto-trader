# position_watch_ws.py
# ====================================================
# 역할
# ----------------------------------------------------
# - 웹소켓 캔들/호가에서 현재 시장 상태 요약 피처를 만들고,
#   열린 포지션(Trade)에 대해 GPT-5.1(gpt_decider)을 통해
#   EXIT 여부(HOLD/CLOSE)만 판단한 뒤, 실제 청산/로그/DB 업데이트만 수행하는 얇은 레이어.
#
# 2025-11-21 변경 사항 (1m 캔들 종가 기반 run_bot_ws 연동)
# ----------------------------------------------------
# - run_bot_ws.py 가 1m WS 캔들이 새로 생성되는 시점(직전 1분봉 종가 확정 시점)에
#   maybe_exit_with_gpt(...) 를 호출하도록 변경되었으며,
#   이 모듈은 "한 번 호출 시의 EXIT 판단" 로직에만 집중한다.
# - EXIT_CHECK_INTERVAL_SEC 는 여전히 이 모듈을 별도 워커에서 재사용할 때 참고용으로
#   권장 최소 호출 주기(현재 60초)를 의미하지만, run_bot_ws 메인 루프에서는 사용하지 않는다.
#
# 2025-11-22 변경 사항 (EXIT 실시간 분석 텔레그램 요약)
# ----------------------------------------------------
# - maybe_exit_with_gpt(...) 에서 GPT EXIT 판단 직후, 진입 때와 유사한 형태의
#   상세 분석 요약을 텔레그램으로 전송하는 옵션(exit_debug_notify)을 추가했다.
#   · BotSettings.exit_debug_notify 가 True 인 경우에만 전송(기본값 False).
#   · 심볼/방향/시나리오/PnL%/1m 캔들/레짐 요약/GPT 코멘트가 포함된다.
# - 기존 EXIT/HOLD 로직, DB 업데이트, CSV 로깅 동작에는 영향을 주지 않는다.
#
# 사용 방식
# ----------------------------------------------------
# - run_bot_ws 메인 루프:
#     · 1m WS 캔들이 새로 생성된 타이밍에 열린 포지션마다
#       maybe_exit_with_gpt(trade, settings, scenario="RUNTIME_EXIT_CHECK") 호출.
# - 별도 워커/스크립트에서 이 모듈을 직접 사용할 때는
#     maybe_exit_with_gpt(trade, settings, scenario="GENERIC_EXIT_CHECK")
#   를 **약 1분(60초)** 간격(EXIT_CHECK_INTERVAL_SEC)으로 호출하는 패턴을 권장한다.

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

# 이 모듈에서 권장하는 EXIT 체크 주기 (초 단위, 현재 60초)
EXIT_CHECK_INTERVAL_SEC: float = 60

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
# EXIT 디버그 요약 텔레그램 전송 헬퍼
# ─────────────────────────────────────────


def _extract_gpt_reason_for_tg(gpt_data: Any) -> str:
    """gpt_decider 가 돌려준 데이터에서 사람 눈으로 볼 만한 코멘트만 추출한다."""
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
        else:
            text = ""
    except Exception:
        text = ""

    if not text:
        return ""
    if len(text) > 400:
        return text[:400] + " ... (생략)"
    return text


def _build_regime_summary_for_tg(regime_features: Dict[str, Any]) -> str:
    """regime_features dict 에서 몇 가지 핵심 지표만 골라 한 줄 요약 문자열로 만든다."""
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
    """EXIT 판단 시 진입처럼 상세 분석 요약을 텔레그램으로 전송한다.

    - settings.exit_debug_notify 가 True 일 때만 호출되도록 maybe_exit_with_gpt 에서 제어한다.
    - 예외가 발생하더라도 본래 EXIT 흐름에는 영향을 주지 않는다.
    """
    try:
        pnl_pct = gpt_ctx.get("pnl_pct")
        if isinstance(pnl_pct, (int, float)):
            pnl_pct_str = f"{pnl_pct*100:.3f}%"
        else:
            pnl_pct_str = "n/a"

        candle = gpt_ctx.get("candle_1m") or {}
        o = candle.get("open")
        h = candle.get("high")
        l = candle.get("low")
        c = candle.get("close")
        v = candle.get("volume")

        candle_bits: List[str] = []
        try:
            if all(isinstance(x, (int, float)) for x in (o, h, l, c)):
                candle_bits.append(
                    f"O/H/L/C={float(o):.2f}/{float(h):.2f}/{float(l):.2f}/{float(c):.2f}"
                )
            if isinstance(v, (int, float)):
                candle_bits.append(f"거래량={float(v):.3f}")
        except Exception:
            pass
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
            "📘 [EXIT 실시간 분석 요약]",
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
            lines.append("")
            lines.append("📊 레짐/지표 요약:")
            lines.append(f"  · {regime_summary}")

        if gpt_reason:
            lines.append("")
            lines.append("🧠 GPT 코멘트:")
            lines.append(f"  {gpt_reason}")

        msg = "\n".join(lines)
        send_tg(msg)
    except Exception as e:  # pragma: no cover - 디버그 요약은 실패해도 무시
        log(f"[PW][EXIT_DEBUG] build/send summary failed: {e}")


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

    # 옵션 B: 진입처럼 상세 EXIT 분석 요약을 텔레그램으로 전송 (settings.exit_debug_notify=True 인 경우)
    if getattr(settings, "exit_debug_notify", False):
        _send_exit_debug_summary(
            trade=trade,
            scenario=scenario,
            action=action,
            gpt_ctx=gpt_ctx,
            gpt_data=gpt_data,
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
    "EXIT_CHECK_INTERVAL_SEC",
]
