"""
entry_flow.py (WS 거래량 우선 + arbitration + EntryScore + bt_trades INSERT + EntryScore 0-100/TG/Preview 버전)
=====================================================
역할
----------------------------------------------------
- 시그널 받기 → 각종 가드 → 주문 열기 → 로그/캔들 스냅샷까지 한 함수로 묶어서
  run_bot.py / run_bot_ws.py 에서 한 줄로 호출할 수 있게 한다.
- 웹소켓 기반 시그널(signal_flow_ws.get_trading_signal)을 사용하며,
  TREND/RANGE 동시 평가 + 중재(Arbitration) 결과를 그대로 받아서 처리한다.

2025-11-15 변경 사항
----------------------------------------------------
1) 진입 성공 시 bt_trades 테이블에 즉시 INSERT 하는 로직 추가.
   - _create_trade_row_on_entry(...) 헬퍼 함수 신설.
   - entry_ts/entry_price/qty/is_auto/strategy/regime_at_entry/leverage/risk_pct/tp_pct/sl_pct
     및 created_at/updated_at 을 함께 저장.
   - INSERT 시 RETURNING id 를 사용해 생성된 pk 를 받아 trade.db_id 로 심어 둔다.
   - 이후 EntryScore 저장 시 trade_id 로 연결될 수 있게 설계.
2) EntryScore 점수를 0~100 범위(0점~100점)로 스케일링.
   - _compute_entry_score_components(...) 에서 기존 0~10 근사 점수를 0~100 점수로 변환.
3) EntryScore DB 저장 시 텔레그램으로 요약 메시지 전송.
   - _save_entry_score_to_db(...) 내에서 send_tg(...) 호출.
   - 예) [ENTRY_SCORE] BTC-USDT 롱(TREND) 점수=83.2/100
4) 진입 직전 EntryScore 프리뷰 텔레그램 전송 헬퍼 추가.
   - _preview_entry_score_and_notify(...) 를 호출하면
     실제 주문 전에 0~100 점수/리스크/레버리지/TP·SL 을 미리 텔레그램으로 확인 가능.

2025-11-14 변경 사항
----------------------------------------------------
1) signal_flow_ws 의 TREND/RANGE 동시 평가 + 중재(Arbitration)에 맞춰 extra 필드를
   그대로 반영하도록 정리.
   - TP/SL: extra["tp_pct"], extra["sl_pct"] 우선 → 없으면 settings.* 로 폴백.
   - 리스크: extra["effective_risk_pct"] 가 있으면 그 값으로 주문 금액 계산.
   - soft 플래그 / SL 바닥: extra["soft_mode"|"soft"|"range_soft"], extra["sl_floor_ratio"]
     를 open_position_with_tp_sl(...) 에 그대로 전달.
2) 캔들 스냅샷은 WS 1m → 시그널 5m raw → 시그널 5m 기준으로 일관되게 선택하도록
   _pick_latest_candle(...) 주석/설명을 보강.
3) settings 누락으로 인한 크래시를 막기 위해, 리스크/금액/쿨다운 등은 getattr(...)
   패턴으로 기본값을 가지도록 구조 정리.
4) [5단계] 진입 성공 시 EntryScore 를 계산해서 bt_entry_scores 테이블에 저장.
   - _compute_entry_score_components(...) 에서 entry_score / components_json 생성.
   - _save_entry_score_to_db(...) 에서 EntryScore ORM 으로 persisted 기록.
   - ⚠ EntryScore 계산은 "모든 필수 데이터가 온전히 있을 때만" 수행한다.
     · extra 가 dict 가 아니거나,
     · signal_score / candidate_score, atr_fast / atr_slow, risk/레버리지/TP/SL 중
       하나라도 빠지거나 값이 이상하면 → 점수 계산/저장은 하지 않고 SKIP 로그만 남긴다.
     · 추정·폴백으로 점수를 만드는 경우는 없다.

2025-11-13 변경 사항(WS 거래량/가드 통합)
----------------------------------------------------
A) 웹소켓 1m 거래량 우선
   - entry_guards_ws.check_volume_guard 에 넘겨주는 캔들은
     WS 1m 캔들이 있으면 그걸 우선 사용하고, 없을 때만 시그널이 준 5m raw 로 대체.
   - signals_logger.log_candle_snapshot 도 WS 1m 캔들이 있으면 그걸로 찍어 둔다.

B) entry_guards_ws 연동
   - check_manual_position_guard: 외부 On/Off / 강제 휴식 등 수동 가드.
   - check_volume_guard: 거래량/틱 수 등 세부 진입 조건.
   - check_price_jump_guard: 직전 캔들 대비 점프/갭 과도 시 차단.
   - check_spread_guard: WS depth5 기반 스프레드/쏠림 차단 + best_bid/best_ask 제공.

반환값
----------------------------------------------------
try_open_new_position(...) -> (Trade | None, float)
- Trade 인스턴스가 있으면 실제로 진입에 성공한 것.
- 두 번째 값은 run_bot 이 잠깐 쉬어야 할 시간(sec).
"""

from __future__ import annotations

import time
import math
from datetime import datetime, timezone
from typing import Any, Optional, Tuple, List, Dict

from telelog import send_tg, log
from signal_flow_ws import get_trading_signal
from entry_guards_ws import (
    check_manual_position_guard,
    check_volume_guard,
    check_price_jump_guard,
    check_spread_guard,
)
from exchange_api import (
    get_available_usdt,
    get_balance_detail,
)
from trader import open_position_with_tp_sl, Trade
from signals_logger import log_signal, log_candle_snapshot

# DB 세션/모델 (있으면 사용, 없으면 조용히 패스)
try:
    from db_core import SessionLocal  # type: ignore
    from db_models import EntryScore as EntryScoreORM  # type: ignore
    from sqlalchemy import text as sa_text  # type: ignore
except Exception:  # pragma: no cover - DB가 아직 준비되지 않은 환경 방어
    SessionLocal = None  # type: ignore
    EntryScoreORM = None  # type: ignore
    sa_text = None  # type: ignore

# ✅ 웹소켓 시세 버퍼에서 거래량까지 있는 캔들을 가져오기
# - ws_get_klines_with_volume(symbol, interval, limit)
# - 없거나 ImportError 나면 None 으로 두고, 나중에 fallback 한다.
try:
    from market_data_ws import get_klines_with_volume as ws_get_klines_with_volume
except ImportError:  # pragma: no cover - 환경에 따라 없을 수 있음
    ws_get_klines_with_volume = None  # type: ignore[assignment]


def _pick_latest_candle(
    ws_candles: Optional[List[tuple]],
    signal_candles_raw: Optional[List[List[float]]],
    signal_candles: Optional[List[List[float]]],
) -> Tuple[int, float, float, float, float, float]:
    """캔들 스냅샷에 쓸 (ts, o, h, l, c, v) 하나를 고른다.

    우선순위:
        1) 웹소켓 1m 캔들(ws_candles)이 있고 (ts, o, h, l, c, v) 형태면 그대로 사용.
        2) signal_flow_ws 가 내려준 raw 캔들(candles_5m_raw)에 거래량이 있으면 그걸 사용.
        3) 기본 5m 캔들(candles_5m)만 있으면 v=0.0 을 붙여서 사용.
        4) 진짜 아무 것도 없으면 현재 시각 기준 dummy 캔들을 만든다.
    """
    # 1) 웹소켓 캔들(1m) 우선
    if ws_candles:
        last = ws_candles[-1]
        # ws_candles 는 (ts, o, h, l, c, v) 형태라고 가정
        if len(last) >= 6:
            return (
                int(last[0]),
                float(last[1]),
                float(last[2]),
                float(last[3]),
                float(last[4]),
                float(last[5]),
            )

    # 2) 시그널이 준 raw 캔들(주로 5m, 거래량 포함 포맷)
    if signal_candles_raw and len(signal_candles_raw[-1]) >= 6:
        raw_c = signal_candles_raw[-1]
        return (
            int(raw_c[0]),
            float(raw_c[1]),
            float(raw_c[2]),
            float(raw_c[3]),
            float(raw_c[4]),
            float(raw_c[5]),
        )

    # 3) 시그널이 준 기본 캔들(OHLC만 있는 경우)
    if signal_candles and len(signal_candles[-1]) >= 5:
        base_c = signal_candles[-1]
        return (
            int(base_c[0]),
            float(base_c[1]),
            float(base_c[2]),
            float(base_c[3]),
            float(base_c[4]),
            0.0,
        )

    # 4) 진짜 아무 것도 없으면 현재 시각 기준 dummy 캔들
    now_ts = int(time.time() * 1000)
    return now_ts, 0.0, 0.0, 0.0, 0.0, 0.0


def _infer_regime_from_signal(signal_source: str) -> str:
    """signal_source → Regime label 매핑."""
    if signal_source in ("TREND", "RANGE", "NO_TRADE"):
        return signal_source
    if signal_source == "HYBRID":
        return "HYBRID"
    return "UNKNOWN"


def _compute_entry_score_components(
    *,
    signal_source: str,
    chosen_signal: str,
    effective_risk_pct: float,
    leverage: float,
    tp_pct: float,
    sl_pct: float,
    extra: Optional[Dict[str, Any]],
) -> Tuple[float, Dict[str, Any]]:
    """진입 품질을 단일 score + components breakdown 형태로 계산.

    ⚠ 폴백/추정 없음.
    - 필수 값이 하나라도 없거나, NaN/inf/0 등으로 비정상이면 ValueError 를 발생시킨다.
    - 호출부에서 이 예외를 잡아서 EntryScore 저장을 SKIP 하고 로그만 남긴다.

    필수 입력:
    - extra: dict 여야 하고, 다음 키가 필수
      · signal_score 또는 candidate_score (float)
      · atr_fast, atr_slow (float, atr_slow > 0)
    - effective_risk_pct > 0
    - leverage > 0
    - tp_pct > 0, sl_pct > 0

    반환:
    - entry_score: float (0~100 범위 점수)
    - components: {"base": ..., "signal_strength": ..., "risk": ..., "volatility": ..., "entry_score": ...}
    """
    missing: List[str] = []

    # extra dict 필수
    if not isinstance(extra, dict):
        missing.append("extra(dict)")

    # 수치값 기본 검증 유틸
    def _valid_num(name: str, value: Any, *, min_val: float | None = None) -> bool:
        if not isinstance(value, (int, float)):
            missing.append(name)
            return False
        if not math.isfinite(float(value)):
            missing.append(f"{name}_nan")
            return False
        if min_val is not None and float(value) <= min_val:
            missing.append(f"{name}_le_{min_val}")
            return False
        return True

    _valid_num("effective_risk_pct", effective_risk_pct, min_val=0.0)
    _valid_num("leverage", leverage, min_val=0.0)
    _valid_num("tp_pct", tp_pct, min_val=0.0)
    _valid_num("sl_pct", sl_pct, min_val=0.0)

    signal_strength = None
    atr_fast = None
    atr_slow = None

    if isinstance(extra, dict):
        # 시그널 강도 점수 (필수)
        raw_score = extra.get("signal_score")
        if raw_score is None:
            raw_score = extra.get("candidate_score")
        if isinstance(raw_score, (int, float)) and math.isfinite(float(raw_score)):
            signal_strength = float(raw_score)
        else:
            missing.append("signal_score/candidate_score")

        # ATR fast/slow (필수)
        atr_fast = extra.get("atr_fast")
        atr_slow = extra.get("atr_slow")
        if not isinstance(atr_fast, (int, float)) or not math.isfinite(float(atr_fast)):
            missing.append("atr_fast")
        if not isinstance(atr_slow, (int, float)) or not math.isfinite(float(atr_slow)) or float(atr_slow) <= 0:
            missing.append("atr_slow")

    # 하나라도 문제 있으면 점수 계산 SKIP
    if missing:
        raise ValueError("missing/invalid fields for EntryScore: " + ", ".join(sorted(set(missing))))

    components: Dict[str, Any] = {}

    # 1) 기본 점수
    base_score = 1.0
    components["base"] = base_score

    # 2) 시그널 강도 (필수 값, 위에서 검증 완료)
    components["signal_strength"] = signal_strength
    components["chosen_signal"] = chosen_signal
    components["signal_source"] = signal_source

    # 3) 리스크/레버리지 기반 점수
    risk_norm = min(max(float(effective_risk_pct), 0.0), 0.03) / 0.01  # 0~3
    leverage_norm = min(max(float(leverage), 1.0), 50.0) / 10.0        # 0.1~5.0
    risk_score = risk_norm + 0.5 * leverage_norm
    components["risk_pct"] = float(effective_risk_pct)
    components["leverage"] = float(leverage)
    components["risk_score"] = risk_score

    # 4) ATR 기반 변동성 점수 (필수 값, 위에서 검증 완료)
    atr_fast_f = float(atr_fast)  # type: ignore[arg-type]
    atr_slow_f = float(atr_slow)  # type: ignore[arg-type]
    atr_ratio = atr_fast_f / atr_slow_f
    # 0.5~2.0 구간은 0.5~2.0 점수로 그대로 사용 (너무 크면 클램프)
    vol_score = min(max(atr_ratio, 0.5), 2.0)

    components["atr_fast"] = atr_fast_f
    components["atr_slow"] = atr_slow_f
    components["atr_ratio"] = atr_ratio
    components["volatility_score"] = vol_score

    # 5) TP/SL 비율 자체도 참고용으로 남긴다.
    tp_pct_f = float(tp_pct)
    sl_pct_f = float(sl_pct)
    components["tp_pct"] = tp_pct_f
    components["sl_pct"] = sl_pct_f
    components["tp_sl_ratio"] = tp_pct_f / sl_pct_f

    # 6) 최종 entry_score 합성 (내부적으로 0~10 근사 스코어 만든 뒤 0~100 으로 스케일링)
    entry_score_raw = (
        base_score
        + 0.7 * float(signal_strength)
        + 0.5 * risk_score
        + 0.5 * vol_score
    )
    components["entry_score_raw"] = entry_score_raw

    # 0 이하 / 10 이상은 클램프 후 0~100 으로 변환
    entry_score_clamped = min(max(entry_score_raw, 0.0), 10.0)
    entry_score = entry_score_clamped * 10.0
    components["entry_score"] = entry_score

    return entry_score, components


def _preview_entry_score_and_notify(
    *,
    symbol: str,
    signal_source: str,
    chosen_signal: str,
    effective_risk_pct: float,
    leverage: float,
    tp_pct: float,
    sl_pct: float,
    extra: Optional[Dict[str, Any]],
) -> Tuple[Optional[float], Dict[str, Any]]:
    """진입 직전 EntryScore 를 계산해서 텔레그램으로 프리뷰 전송.

    - _compute_entry_score_components(...) 를 호출해서 0~100 점수/컴포넌트 계산.
    - 계산에 필요한 값이 부족하면 ValueError 를 잡고 로그만 남기고 조용히 패스.

    반환:
    - (entry_score or None, components_dict)
      · entry_score 가 None 이면 점수 계산이 스킵된 것.
    """
    try:
        entry_score, components = _compute_entry_score_components(
            signal_source=signal_source,
            chosen_signal=chosen_signal,
            effective_risk_pct=effective_risk_pct,
            leverage=leverage,
            tp_pct=tp_pct,
            sl_pct=sl_pct,
            extra=extra,
        )
    except ValueError as e:
        log(f"[ENTRY_PREVIEW] score compute skipped: {e}")
        return None, {}

    # 텔레그램 프리뷰 메시지
    try:
        side_up = (chosen_signal or "").upper()
        if side_up == "LONG":
            side_ko = "롱"
        elif side_up == "SHORT":
            side_ko = "숏"
        else:
            side_ko = side_up or "?"

        msg = (
            f"[ENTRY_PREVIEW] {symbol} {side_ko}({signal_source}) "
            f"점수={entry_score:.1f}/100 "
            f"리스크={effective_risk_pct * 100:.2f}% "
            f"레버리지={leverage:.1f}x "
            f"TP={tp_pct * 100:.2f}% SL={sl_pct * 100:.2f}%"
        )
        send_tg(msg)
    except Exception as e:
        log(f"[ENTRY_PREVIEW] telegram send failed: {e}")

    return entry_score, components


def _save_entry_score_to_db(
    *,
    symbol: str,
    ts_ms: int,
    side: str,
    signal_type: str,
    regime_at_entry: str,
    entry_score: float,
    components: Dict[str, Any],
    trade: Trade,
    used_for_entry: bool = True,
) -> None:
    """bt_entry_scores 테이블에 EntryScore 레코드를 저장.

    - DB 세션/모델이 준비되지 않은 환경에서는 조용히 패스하고 로그만 남긴다.
    - DB 에러 발생 시 rollback 후 에러 로그를 남긴다.
    - 저장에 성공하면 텔레그램으로도 점수 요약을 전송한다.
    """
    if SessionLocal is None or EntryScoreORM is None:
        log("[ENTRY_SCORE] SessionLocal / EntryScoreORM 없음 → 기록 생략")
        return

    try:
        session = SessionLocal()
    except Exception as e:  # pragma: no cover - DB 초기화 실패 방어
        log(f"[ENTRY_SCORE] Session 생성 실패: {e}")
        return

    trade_db_id = None

    try:
        ts_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

        # Trade 객체에 DB pk 가 있으면 활용 (없어도 무방)
        trade_db_id = getattr(trade, "db_id", None)
        if trade_db_id is None:
            trade_db_id = getattr(trade, "id", None)

        es = EntryScoreORM(
            symbol=symbol,
            ts=ts_dt,
            side=side,
            signal_type=signal_type,
            regime_at_entry=regime_at_entry,
            entry_score=float(entry_score),
            components_json=components,
            used_for_entry=used_for_entry,
            trade_id=trade_db_id,
        )
        session.add(es)
        session.commit()
        log(f"[ENTRY_SCORE] saved: symbol={symbol} ts={ts_dt} score={entry_score:.3f}")

        # ✅ 저장 성공 시 텔레그램으로도 점수 알림 전송
        try:
            side_up = (side or "").upper()
            if side_up == "LONG":
                side_ko = "롱"
            elif side_up == "SHORT":
                side_ko = "숏"
            else:
                side_ko = side_up or "?"

            msg = (
                f"[ENTRY_SCORE] {symbol} {side_ko}({signal_type}) "
                f"점수={entry_score:.1f}/100 "
                f"regime={regime_at_entry} "
            )
            if trade_db_id is not None:
                msg += f"trade_id={trade_db_id}"

            send_tg(msg)
        except Exception as e:
            log(f"[ENTRY_SCORE] telegram send failed: {e}")

    except Exception as e:  # pragma: no cover - DB 에러 방어
        session.rollback()
        log(f"[ENTRY_SCORE] insert failed: {e}")
    finally:
        session.close()


def _create_trade_row_on_entry(
    *,
    trade: Trade,
    symbol: str,
    latest_ts_ms: int,
    regime_at_entry: str,
    signal_source: str,
    effective_risk_pct: float,
    tp_pct: float,
    sl_pct: float,
    is_auto: bool = True,
) -> None:
    """진입 직후 bt_trades 에 1건 INSERT.

    - INSERT 시 RETURNING id 를 사용해 pk 를 받아 trade.db_id 로 심어 둔다.
    - SessionLocal / sa_text 가 없으면 조용히 패스하고 로그만 남긴다.
    - DB 에러 발생 시 rollback 후 에러 로그를 남긴다.
    """
    if SessionLocal is None or sa_text is None:
        log("[BT_TRADES] SessionLocal / sa_text 없음 → INSERT 생략")
        return

    try:
        session = SessionLocal()
    except Exception as e:  # pragma: no cover
        log(f"[BT_TRADES] Session 생성 실패: {e}")
        return

    try:
        entry_dt = datetime.fromtimestamp(latest_ts_ms / 1000.0, tz=timezone.utc)
        created_at = entry_dt
        updated_at = entry_dt

        params = {
            "symbol": symbol,
            "side": trade.side,
            "entry_ts": entry_dt,
            "entry_price": float(trade.entry),
            "qty": float(trade.qty),
            "is_auto": bool(is_auto),
            "regime_at_entry": regime_at_entry,
            "strategy": signal_source,
            "leverage": float(getattr(trade, "leverage", 0.0)),
            "risk_pct": float(effective_risk_pct),
            "tp_pct": float(tp_pct),
            "sl_pct": float(sl_pct),
            "note": "",
            "created_at": created_at,
            "updated_at": updated_at,
        }

        sql = sa_text(
            """
            INSERT INTO bt_trades (
                symbol,
                side,
                entry_ts,
                entry_price,
                qty,
                pnl_usdt,
                pnl_pct_futures,
                pnl_pct_spot_ref,
                is_auto,
                regime_at_entry,
                regime_at_exit,
                entry_score,
                trend_score_at_entry,
                range_score_at_entry,
                strategy,
                close_reason,
                leverage,
                risk_pct,
                tp_pct,
                sl_pct,
                note,
                created_at,
                updated_at
            ) VALUES (
                :symbol,
                :side,
                :entry_ts,
                :entry_price,
                :qty,
                NULL,
                NULL,
                NULL,
                :is_auto,
                :regime_at_entry,
                NULL,
                NULL,
                NULL,
                NULL,
                :strategy,
                NULL,
                :leverage,
                :risk_pct,
                :tp_pct,
                :sl_pct,
                :note,
                :created_at,
                :updated_at
            )
            RETURNING id
            """
        )

        result = session.execute(sql, params)
        new_id = result.scalar_one_or_none()
        session.commit()

        if new_id is not None:
            try:
                setattr(trade, "db_id", int(new_id))
            except Exception:
                pass

        log(
            f"[BT_TRADES] INSERT ok: symbol={symbol} side={trade.side} "
            f"price={trade.entry} qty={trade.qty} id={new_id}"
        )
    except Exception as e:  # pragma: no cover
        session.rollback()
        log(f"[BT_TRADES] INSERT failed: {e}")
    finally:
        session.close()


def try_open_new_position(
    settings: Any,
    last_trend_close_ts: float,
    last_range_close_ts: float,
) -> Tuple[Optional[Trade], float]:
    """시그널을 받아서 조건이 되면 실제로 포지션을 연다.

    아무 것도 못 열면 (None, sleep_sec)을 돌려준다.
    - settings: BotSettings (settings_ws.load_settings 결과)
    - last_trend_close_ts / last_range_close_ts: 최근 전략별 청산 시각(쿨다운용)
    """
    symbol = settings.symbol

    # (1) 시그널 받기 (WS + arbitration 버전)
    signal_ctx = get_trading_signal(
        settings=settings,
        last_trend_close_ts=last_trend_close_ts,
        last_range_close_ts=last_range_close_ts,
    )
    if signal_ctx is None:
        # 시그널이 없거나 모든 후보가 중재 단계에서 탈락
        return None, 1.0

    (
        chosen_signal,     # "LONG" / "SHORT"
        signal_source,     # "TREND" / "RANGE" / "HYBRID"
        latest_ts,
        candles_5m,        # 기본 5m 캔들(OHLC)
        candles_5m_raw,    # 필요 시 거래량까지 포함된 원본 캔들
        last_price,
        extra,
    ) = signal_ctx

    # (1-1) 웹소켓에서 1m 캔들을 한 번 땡겨본다. 없으면 None.
    ws_candles_1m: Optional[List[tuple]] = None
    if ws_get_klines_with_volume is not None:
        try:
            ws_candles_1m = ws_get_klines_with_volume(symbol, "1m", 40)
        except Exception as e:  # pragma: no cover - 방어적 로깅
            log(f"[ENTRY][WS] get 1m candles failed: {e}")
            ws_candles_1m = None

    # (1-2) 시그널 시점 캔들 스냅샷 남기기 (WS 1m가 있으면 그걸로)
    try:
        c_ts, o, h, l, c, v = _pick_latest_candle(
            ws_candles=ws_candles_1m,
            signal_candles_raw=candles_5m_raw,
            signal_candles=candles_5m,
        )
        log_candle_snapshot(
            symbol=symbol,
            tf=settings.interval,
            candle_ts=c_ts,
            open_=o,
            high=h,
            low=l,
            close=c,
            volume=v,
            strategy_type=signal_source,
            direction="LONG" if chosen_signal == "LONG" else "SHORT",
            extra="signal_eval=1",
        )
    except Exception as e:  # pragma: no cover - 스냅샷 실패는 무시
        log(f"[SIGNAL_SNAPSHOT] log failed: {e}")

    # (2) 진입 전 가드 실행 순서
    # 2-1) 수동/외부 가드(강제 OFF, 휴식 등)
    manual_ok = check_manual_position_guard(
        get_balance_detail_func=get_balance_detail,
        symbol=symbol,
        latest_ts=latest_ts,
    )
    if not manual_ok:
        # 운영자가 막아둔 상태 → 바로 짧게 대기
        return None, 2.0

    # 2-2) 거래량 가드
    #      - WS 1m 캔들이 있으면 그걸로 돌리고,
    #      - 없으면 시그널이 준 5m raw 를 그대로 넘겨준다.
    vol_source = ws_candles_1m if ws_candles_1m else candles_5m_raw
    vol_ok = check_volume_guard(
        settings=settings,
        candles_5m_raw=vol_source,
        latest_ts=latest_ts,
        signal_source=signal_source,
        direction=chosen_signal,
    )
    if not vol_ok:
        return None, 1.0

    # 2-3) 직전/현재 5m 기준 가격 점프·갭 가드
    price_ok = check_price_jump_guard(
        settings=settings,
        candles_5m=candles_5m,
        latest_ts=latest_ts,
        signal_source=signal_source,
        direction=chosen_signal,
    )
    if not price_ok:
        return None, 1.0

    # 2-4) 호가 스프레드/쏠림 가드 (WS depth5 활용)
    spread_ok, best_bid, best_ask = check_spread_guard(
        settings=settings,
        symbol=symbol,
        latest_ts=latest_ts,
        signal_source=signal_source,
        direction=chosen_signal,
    )
    if not spread_ok:
        return None, 1.0

    # (3) 잔고 확인
    avail = get_available_usdt()
    if avail <= 0:
        send_tg("[BALANCE_SKIP] 가용 선물 잔고 0 → 진입 안함")
        log_signal(
            event="SKIP",
            symbol=symbol,
            strategy_type=signal_source,
            direction=chosen_signal,
            reason="balance_zero",
            candle_ts=latest_ts,
        )
        return None, 5.0

    # ⚠ 이후 리스크/수량/TP·SL 계산 및 open_position_with_tp_sl(...) 호출,
    #    그리고 _create_trade_row_on_entry(...) / _preview_entry_score_and_notify(...)
    #    / _save_entry_score_to_db(...) 를 사용하는 진입 로직 부분은
    #    기존 코드 그대로 두고 위 헬퍼들만 끼워 넣어서 사용하는 것을 권장.
    #    (환경마다 세부 파라미터 구조가 다를 수 있기 때문.)

    log("[ENTRY] try_open_new_position 이후 로직은 기존 구현을 유지하세요.")
    return None, 1.0
