"""entry_flow.py (WS + GPT 트레이더 오케스트레이터)
=====================================================
역할
-----------------------------------------------------
- run_bot.py / run_bot_ws.py 에서 호출하는 진입 실행 레이어.
- WS 시세 + market_features_ws 시그널 → gpt_trader 판단 → 가드 → 주문 → 로그/DB 기록.

2025-12-01 변경 요약 (TA-Lib EntryScore 정합 + NaN 가드)
-----------------------------------------------------
- market_features_ws.get_trading_signal(...) 이 내려주는 extra["atr_fast"], extra["atr_slow"] 를
  EntryScore 계산의 기준으로 사용하되, 둘 중 하나라도 NaN/None/비유효 값이면
  이번 캔들은 전체 엔트리 플로우를 SKIP 처리하도록 가드 추가.
- _compute_entry_score_components(...) 의 0~10 → 0~100 클램프/정규화 로직을 유지하면서
  preview_entry_score / DB EntryScore 저장 경로에서 NaN 이 저장되지 않도록
  ValueError 기반 가드 흐름을 명확히 정리.
- GPT 호출 이전에 ATR 정합 가드가 먼저 적용되도록 해서,
  TA-Lib 지표가 아직 안정되지 않은 초기 구간에서는 진입 자체를 막고
  로그/skip_event 만 남기도록 변경.

2025-11-20 변경 요약
-----------------------------------------------------
- 레거시 레짐 전용 의사결정 로직 제거.
- GPT 전담 엔트리 구조로 단순화.

2025-11-21 변경 요약 (GPT 엔트리 쿨다운 + PREVIEW 스팸 제어)
-----------------------------------------------------
- GPT 엔트리 쿨다운을 entry_flow 레벨에서 추가.
  · settings.gpt_entry_cooldown_sec / ENV GPT_ENTRY_COOLDOWN_SEC 로 설정 (기본 30초).
  · 쿨다운 중에는 PREVIEW/GPT 호출 모두 SKIP, gpt_entry_cooldown 이벤트만 기록.
- GPT 쿨다운 구간에서는 Entry PREVIEW 텔레그램도 발생하지 않도록 차단.
"""

from __future__ import annotations

import os
import time
import math
from datetime import datetime, timezone
from typing import Any, Optional, Tuple, List, Dict

from telelog import send_tg, log, send_skip_tg
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
from trader import Trade
from exchange_api import open_position_with_tp_sl
from signals_logger import (
    log_signal,
    log_candle_snapshot,
    log_gpt_entry_event,
    log_skip_event,
)
from gpt_trader import decide_entry_with_gpt_trader

# ✅ 시그널/피처 공급자
# - market_features_ws.get_trading_signal 이 아직 없더라도 ImportError 로
#   프로세스가 바로 죽지 않도록 방어.
try:
    from market_features_ws import get_trading_signal  # type: ignore[attr-defined]
except ImportError:

    def get_trading_signal(*args: Any, **kwargs: Any) -> None:  # type: ignore[override]
        """market_features_ws.get_trading_signal 미구현 시 사용하는 임시 스텁.

        - 항상 None 을 반환하여 entry_flow.try_open_new_position(...) 쪽에서
          "시그널 없음"으로 처리되게 한다.
        - 봇은 죽지 않고 동작하지만, 신규 진입은 발생하지 않는다.
        - 실제 매매를 위해서는 market_features_ws.py 에
          호환되는 get_trading_signal(...) 구현이 필요하다.
        """
        log(
            "[ENTRY_FLOW] WARNING: market_features_ws.get_trading_signal 미구현 → "
            "임시 스텁 동작 (모든 진입 SKIP)"
        )
        return None


# DB 세션/모델 (있으면 사용, 없으면 조용히 패스)
try:
    from db_core import SessionLocal  # type: ignore
    from db_models import EntryScore as EntryScoreORM  # type: ignore
    from sqlalchemy import text as sa_text  # type: ignore
except Exception:  # pragma: no cover - DB가 아직 준비되지 않은 환경 방어
    SessionLocal = None  # type: ignore
    EntryScoreORM = None  # type: ignore
    sa_text = None  # type: ignore

# GPT-5 메타 정보 (entry_flow 에서는 로깅/메타 정보용으로만 사용)
DEFAULT_GPT_ENTRY_MODEL = "gpt-5.1"

# GPT 엔트리 쿨다운 (PREVIEW + GPT 호출 공통)
_DEFAULT_GPT_ENTRY_COOLDOWN_SEC = 30.0
_LAST_GPT_ENTRY_TS: float = 0.0


def _get_gpt_entry_cooldown_sec(settings: Any) -> float:
    """GPT 엔트리 쿨다운(초)을 settings/env 에서 읽어서 돌려준다.

    우선순위:
    1) settings.gpt_entry_cooldown_sec (숫자형) 이 있으면 사용
    2) ENV GPT_ENTRY_COOLDOWN_SEC 이 있으면 float 로 파싱해서 사용
    3) 둘 다 없으면 _DEFAULT_GPT_ENTRY_COOLDOWN_SEC (기본 30초)
    """
    val = getattr(settings, "gpt_entry_cooldown_sec", None)
    if isinstance(val, (int, float)):
        return float(val)

    env_val = os.getenv("GPT_ENTRY_COOLDOWN_SEC")
    if env_val:
        try:
            return float(env_val)
        except ValueError:
            log(
                f"[GPT_ENTRY] invalid GPT_ENTRY_COOLDOWN_SEC env: {env_val} "
                f"→ fallback to default={_DEFAULT_GPT_ENTRY_COOLDOWN_SEC}"
            )
            return _DEFAULT_GPT_ENTRY_COOLDOWN_SEC

    return _DEFAULT_GPT_ENTRY_COOLDOWN_SEC


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
    2) get_trading_signal 이 내려준 raw 캔들(candles_5m_raw)에 거래량이 있으면 그걸 사용.
    3) 기본 5m 캔들(candles_5m)만 있으면 v=0.0 을 붙여서 사용.
    4) 진짜 아무 것도 없으면 현재 시각 기준 dummy 캔들을 만든다.
    """
    # 1) 웹소켓 캔들(1m) 우선
    if ws_candles:
        last = ws_candles[-1]  # ws_candles 는 (ts, o, h, l, c, v) 형태라고 가정
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
    def _valid_num(
        name: str,
        value: Any,
        *,
        min_val: Optional[float] = None,
    ) -> bool:
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
        if (
            not isinstance(atr_slow, (int, float))
            or not math.isfinite(float(atr_slow))
            or float(atr_slow) <= 0
        ):
            missing.append("atr_slow")

    # 하나라도 문제 있으면 점수 계산 SKIP
    if missing:
        raise ValueError(
            "missing/invalid fields for EntryScore: " + ", ".join(sorted(set(missing)))
        )

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
    leverage_norm = min(max(float(leverage), 1.0), 50.0) / 10.0  # 0.1~5.0
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
                strategy,
                close_reason,
                leverage,
                risk_pct,
                tp_pct,
                sl_pct,
                note,
                created_at,
                updated_at
            )
            VALUES (
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
    last_close_ts: float,
) -> Tuple[Optional[Trade], float]:
    """시그널을 받아서 조건이 되면 실제로 포지션을 연다.

    아무 것도 못 열면 (None, sleep_sec)을 돌려준다.

    - settings   : BotSettings (settings_ws.load_settings 결과)
    - last_close_ts: 최근 청산 시각(쿨다운/컨텍스트용, 현재는 단일 전략 기준)
    """
    symbol = settings.symbol

    # GPT 메타 정보 (로그/EntryScore components 에 기록용)
    gpt_model_for_log = os.getenv("GPT_ENTRY_MODEL", DEFAULT_GPT_ENTRY_MODEL)
    gpt_decision: Dict[str, Any] = {}
    gpt_action: str = ""
    gpt_reason: Optional[str] = None

    # (1) 시그널 받기 (WS 기반 market_features_ws 버전)
    signal_ctx = get_trading_signal(
        settings=settings,
        last_close_ts=last_close_ts,
    )
    if signal_ctx is None:
        # 시그널이 없거나 모든 후보가 필터링 단계에서 탈락
        send_skip_tg(f"[SKIP] no_signal_or_arbitration_rejected: symbol={symbol}")
        log_skip_event(
            symbol=symbol,
            regime="NO_TRADE",
            source="entry_flow.signal",
            side=None,
            reason="no_signal_or_arbitration_rejected",
            extra=None,
        )
        return None, 1.0

    (
        chosen_signal,  # "LONG" / "SHORT"
        signal_source,  # 전략 이름 (실제 의사결정은 GPT)
        latest_ts,
        candles_5m,      # 기본 5m 캔들(OHLC)
        candles_5m_raw,  # 필요 시 거래량까지 포함된 원본 캔들
        last_price,
        extra,
    ) = signal_ctx

    # 시장 레짐 라벨: 현재는 시그널/전략 이름 정도만 기록용으로 사용
    if isinstance(signal_source, str) and signal_source:
        regime_label: str = str(signal_source)
    else:
        regime_label = "GENERIC"

    # ✅ 2025-12-01: TA-Lib ATR 기반 EntryScore NaN 가드
    # - atr_fast / atr_slow 가 NaN/None/비유효 값이면,
    #   지표가 준비되지 않은 상태로 보고 이번 캔들 엔트리 전체를 SKIP 처리.
    atr_fast_val = extra.get("atr_fast") if isinstance(extra, dict) else None
    atr_slow_val = extra.get("atr_slow") if isinstance(extra, dict) else None
    if (
        not isinstance(atr_fast_val, (int, float))
        or not math.isfinite(float(atr_fast_val))
        or not isinstance(atr_slow_val, (int, float))
        or not math.isfinite(float(atr_slow_val))
    ):
        msg = (
            "[SKIP] entry_score_atr_invalid: atr_fast/atr_slow invalid "
            f"(atr_fast={atr_fast_val}, atr_slow={atr_slow_val})"
        )
        log(msg)
        try:
            send_skip_tg(msg)
        except Exception as te:
            log(f"[ENTRY_SCORE] send_skip_tg failed on atr_invalid: {te}")
        log_skip_event(
            symbol=symbol,
            regime=regime_label,
            source="entry_flow.entry_score_atr",
            side=chosen_signal,
            reason="atr_invalid_for_entry_score",
            extra={"atr_fast": atr_fast_val, "atr_slow": atr_slow_val},
        )
        # ATR 이 안정될 때까지 짧게 대기 후 다음 루프에서 다시 시도
        return None, 1.0

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

    # (2) 진입 전 가드: 수동/외부 가드 (GPT 이전에 항상 체크)
    manual_ok = check_manual_position_guard(
        get_balance_detail_func=get_balance_detail,
        symbol=symbol,
        latest_ts=latest_ts,
    )
    if not manual_ok:
        send_skip_tg("[SKIP] manual_guard_blocked (외부 강제 OFF/휴식)")
        log_skip_event(
            symbol=symbol,
            regime=regime_label,
            source="entry_flow.guard.manual",
            side=chosen_signal,
            reason="manual_guard_blocked",
            extra={"latest_ts": int(latest_ts)},
        )
        return None, 2.0

    # (3) 잔고 확인 (GPT 호출 전에 0잔고는 바로 컷)
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
        log_skip_event(
            symbol=symbol,
            regime=regime_label,
            source="entry_flow.balance",
            side=chosen_signal,
            reason="balance_zero",
            extra={"available_usdt": float(avail)},
        )
        return None, 5.0

    # (4) 주문 관련 파라미터 계산 (리스크/레버리지/TP·SL 기본값)
    # settings 에 값이 없으면 안전한 기본값으로.
    leverage = float(getattr(settings, "leverage", 10.0))
    risk_pct = float(getattr(settings, "risk_pct", 0.01))  # 1% 기본

    # TP/SL 퍼센트 (extra 우선)
    tp_pct = (
        float(extra.get("tp_pct"))
        if isinstance(extra, dict) and extra.get("tp_pct") is not None
        else float(getattr(settings, "tp_pct", 0.02))
    )
    sl_pct = (
        float(extra.get("sl_pct"))
        if isinstance(extra, dict) and extra.get("sl_pct") is not None
        else float(getattr(settings, "sl_pct", 0.01))
    )
    effective_risk_pct = (
        float(extra.get("effective_risk_pct"))
        if isinstance(extra, dict) and extra.get("effective_risk_pct") is not None
        else risk_pct
    )

    # (4-0) GPT 엔트리 쿨다운 (PREVIEW/GPT 모두 포함)
    # - 너무 잦은 WS 시그널로 인해 PREVIEW + GPT SKIP 이 몇 초 간격으로 반복되는 현상 방지.
    global _LAST_GPT_ENTRY_TS
    cooldown_sec = _get_gpt_entry_cooldown_sec(settings)
    now_ts = time.time()
    if cooldown_sec > 0 and _LAST_GPT_ENTRY_TS > 0.0:
        elapsed = now_ts - _LAST_GPT_ENTRY_TS
        if elapsed < cooldown_sec:
            remain = max(cooldown_sec - elapsed, 0.0)
            msg = (
                f"[GPT_ENTRY][COOLDOWN] gpt_entry_cooldown active → "
                f"skip preview+gpt ({remain:.0f}s remaining)"
            )
            log(msg)
            try:
                send_skip_tg(msg)
            except Exception as te:
                log(f"[GPT_ENTRY] send_skip_tg failed on cooldown: {te}")
            log_skip_event(
                symbol=symbol,
                regime=regime_label,
                source="entry_flow.gpt_entry_cooldown",
                side=chosen_signal,
                reason="gpt_entry_cooldown_active",
                extra={
                    "remain_sec": float(remain),
                    "cooldown_sec": float(cooldown_sec),
                },
            )
            # 남은 쿨다운 시간과 GPT_SKIP_SLEEP_SEC 중 큰 값으로 슬립
            sleep_sec = float(
                max(remain, getattr(settings, "gpt_skip_sleep_sec", 3.0))
            )
            return None, sleep_sec

    # 이 캔들을 GPT 후보로 평가하기로 했으므로 타임스탬프 갱신
    _LAST_GPT_ENTRY_TS = now_ts

    # (4-1) 진입 전 점수 프리뷰 (GPT 프롬프트에도 같이 들어갈 요약 정보)
    preview_score, _ = _preview_entry_score_and_notify(
        symbol=symbol,
        signal_source=signal_source,
        chosen_signal=chosen_signal,
        effective_risk_pct=effective_risk_pct,
        leverage=leverage,
        tp_pct=tp_pct,
        sl_pct=sl_pct,
        extra=extra,
    )

    # (4-2) 현재 가드 스냅샷 구성 (GPT 프롬프트 + guard_adjustments 에 사용)
    guard_snapshot: Dict[str, float] = {}
    for key in [
        "min_entry_volume_ratio",
        "max_spread_pct",
        "max_price_jump_pct",
        "depth_imbalance_min_ratio",
        "depth_imbalance_min_notional",
    ]:
        val = getattr(settings, key, None)
        if isinstance(val, (int, float)):
            guard_snapshot[key] = float(val)

    # (4-3) GPT 에 전달할 extra 구성
    if isinstance(extra, dict):
        extra_for_gpt: Optional[Dict[str, Any]] = dict(extra)
    else:
        extra_for_gpt = None

    # (4-4) GPT-5 진입 의사결정 (gpt_trader 사용, 폴백 없음)
    try:
        gpt_result = decide_entry_with_gpt_trader(
            settings,
            symbol=symbol,
            signal_source=signal_source,
            direction=chosen_signal,
            last_price=float(last_price),
            entry_score=preview_score,
            base_risk_pct=effective_risk_pct,
            base_tp_pct=tp_pct,
            base_sl_pct=sl_pct,
            extra=extra_for_gpt,
            guard_snapshot=guard_snapshot or None,
        )
    except Exception as e:
        # gpt_trader 내부에서도 예외를 처리하지만, 혹시 모를 오류는
        # 해당 루프만 SKIP 처리하고, 나머지 하드 스톱/쿨타임 관리는 gpt_trader 에 맡긴다.
        msg = f"[GPT_ENTRY][ERROR] gpt_trader unexpected exception → skip: {e}"
        log(msg)
        try:
            send_skip_tg(msg)
        except Exception as te:
            log(f"[GPT_ENTRY] send_skip_tg failed on gpt_trader exception: {te}")
        log_skip_event(
            symbol=symbol,
            regime=regime_label,
            source="entry_flow.gpt_error",
            side=chosen_signal,
            reason="gpt_trader_exception",
            extra={"error": str(e)},
        )
        sleep_sec = float(getattr(settings, "gpt_error_sleep_sec", 5.0))
        return None, sleep_sec

    gpt_action = str(gpt_result.get("gpt_action", "")).upper()
    final_action = str(gpt_result.get("final_action", "SKIP")).upper()
    raw_decision = gpt_result.get("raw") or {}
    reason_val = gpt_result.get("reason")
    gpt_reason = str(reason_val) if isinstance(reason_val, str) else None

    # gpt_trader 가 반환하는 상태코드(gpt_status) 우선 사용, 없으면 legacy status 사용
    status_val = gpt_result.get("gpt_status")
    if not isinstance(status_val, str):
        status_val = gpt_result.get("status")
    gpt_status = str(status_val).upper() if isinstance(status_val, str) else ""
    hard_stop_flag = bool(gpt_result.get("hard_stop"))
    gpt_decision = raw_decision  # EntryScore 메타용

    # entry_score_preview (옵션) 안전 파싱
    preview_score_raw = gpt_result.get("entry_score_preview")
    try:
        if preview_score_raw is None:
            preview_score = None
        elif isinstance(preview_score_raw, (int, float)):
            preview_score = float(preview_score_raw)
        elif isinstance(preview_score_raw, str):
            preview_score = float(preview_score_raw)
        else:
            preview_score = None
    except (TypeError, ValueError):
        preview_score = None

    # (4-5) GPT 타임아웃(응답 지연) → 이 캔들만 SKIP (하드 스톱 금지)
    is_timeout = False
    if gpt_status == "TIMEOUT":
        is_timeout = True
    elif gpt_action == "ERROR" and gpt_reason:
        reason_lower = gpt_reason.lower()
        if (
            "응답 지연" in gpt_reason
            or "timeout" in reason_lower
            or "time out" in reason_lower
        ):
            is_timeout = True

    if is_timeout:
        msg = (
            f"[GPT_ENTRY][TIMEOUT] {gpt_reason or 'gpt_timeout'} → "
            "this signal skipped (no hard stop)"
        )
        log(msg)
        try:
            send_skip_tg(msg)
        except Exception as te:
            log(f"[GPT_ENTRY] send_skip_tg failed on timeout: {te}")
        log_skip_event(
            symbol=symbol,
            regime=regime_label,
            source="entry_flow.gpt_timeout",
            side=chosen_signal,
            reason="gpt_timeout",
            extra={"gpt_result": gpt_result},
        )
        sleep_sec = float(
            gpt_result.get(
                "sleep_after_sec", getattr(settings, "gpt_skip_sleep_sec", 3.0)
            )
        )
        return None, sleep_sec

    # (4-6) gpt_trader 하드 스톱 상태 → 쿨타임 동안 신규 진입 전면 스킵
    if gpt_status == "HARD_STOP" or hard_stop_flag:
        msg = (
            "[GPT_ENTRY][HARD_STOP] GPT 장애 상태 → 신규 진입 스킵"
            f" (reason={gpt_reason or 'gpt_hard_stop'})"
        )
        log(msg)
        try:
            send_skip_tg(msg)
        except Exception as te:
            log(f"[GPT_ENTRY] send_skip_tg failed on hard stop: {te}")
        log_skip_event(
            symbol=symbol,
            regime=regime_label,
            source="entry_flow.gpt_hard_stop",
            side=chosen_signal,
            reason="gpt_entry_hard_stop",
            extra={"gpt_result": gpt_result},
        )
        sleep_sec = float(
            gpt_result.get(
                "sleep_after_sec",
                getattr(settings, "gpt_entry_hard_stop_cooldown_sec", 30.0),
            )
        )
        return None, sleep_sec

    # (4-7) GPT 내부 오류/예외에 의한 ERROR + raw 비어 있음 → 이 캔들만 에러 SKIP
    if gpt_action == "ERROR" and not raw_decision:
        msg = (
            "[GPT_ENTRY][ERROR] GPT_TRADER returned ERROR with empty raw → "
            "skip this loop"
        )
        log(msg)
        try:
            send_skip_tg(msg)
        except Exception as te:
            log(f"[GPT_ENTRY] send_skip_tg failed on empty-raw error: {te}")
        log_skip_event(
            symbol=symbol,
            regime=regime_label,
            source="entry_flow.gpt_error",
            side=chosen_signal,
            reason="gpt_entry_error_empty_raw",
            extra={"gpt_result": gpt_result},
        )
        sleep_sec = float(getattr(settings, "gpt_error_sleep_sec", 5.0))
        return None, sleep_sec

    # (4-8) GPT 판단에 따른 일반 SKIP
    #     (ENTER/ADJUST 가 아닌 경우, 또는 final_action=SKIP)
    if final_action != "ENTER":
        msg = (
            f"[GPT_ENTRY][SKIP] action={gpt_action or 'NONE'} "
            f"reason={gpt_reason or 'no_reason'}"
        )
        log(msg)
        try:
            send_skip_tg(msg)
        except Exception as te:
            log(f"[GPT_ENTRY] send_skip_tg failed on general-skip: {te}")
        log_skip_event(
            symbol=symbol,
            regime=regime_label,
            source="entry_flow.gpt_decision",
            side=chosen_signal,
            reason=f"gpt_action_{gpt_action or 'NONE'}",
            extra={"gpt_result": gpt_result},
        )
        sleep_sec = float(
            gpt_result.get(
                "sleep_after_sec", getattr(settings, "gpt_skip_sleep_sec", 3.0)
            )
        )
        return None, sleep_sec

    # (4-9) GPT 가 승인한 리스크/TP/SL 및 가드 조정값 적용
    effective_risk_pct = float(
        gpt_result.get("effective_risk_pct", effective_risk_pct)
    )
    tp_pct = float(gpt_result.get("tp_pct", tp_pct))
    sl_pct = float(gpt_result.get("sl_pct", sl_pct))

    guard_adjustments: Dict[str, float] = {}
    raw_ga = gpt_result.get("guard_adjustments") or {}
    if isinstance(raw_ga, dict):
        try:
            guard_adjustments = {k: float(v) for k, v in raw_ga.items()}
        except Exception:
            guard_adjustments = {}

    # GPT 승인/보정 결과 텔레그램 알림 (정보용)
    try:
        side_up = (chosen_signal or "").upper()
        if side_up == "LONG":
            side_ko = "롱"
        elif side_up == "SHORT":
            side_ko = "숏"
        else:
            side_ko = side_up or "?"

        msg = (
            f"[GPT_ENTRY] APPROVED {symbol} {side_ko}({signal_source}) "
            f"risk={effective_risk_pct * 100:.2f}% "
            f"tp={tp_pct * 100:.2f}% sl={sl_pct * 100:.2f}% "
            f"model={gpt_model_for_log}"
        )
        if gpt_reason:
            msg += f" reason={gpt_reason}"
        if guard_adjustments:
            msg += f" guards={guard_adjustments}"
        send_tg(msg)
    except Exception as e:
        log(f"[GPT_ENTRY] telegram send failed: {e}")

    # GPT 승인 결과를 이벤트 CSV에도 기록
    log_gpt_entry_event(
        symbol=symbol,
        regime=regime_label,
        side=chosen_signal,
        action=gpt_action,
        tp_pct=tp_pct,
        sl_pct=sl_pct,
        risk_pct=effective_risk_pct,
        gpt_json={
            "model": gpt_model_for_log,
            "decision": raw_decision,
            "reason": gpt_reason,
            "entry_score_preview": preview_score,
            "guard_adjustments": guard_adjustments,
        },
    )

    # (5) 거래량/가격 점프/스프레드 가드
    # - gpt_trader 가 내려준 guard_adjustments 를 settings 에 1회성으로 반영한 뒤 체크
    original_guard_values: Dict[str, Any] = {}
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None

    try:
        # 5-1) guard_adjustments 를 settings 에 주입 (한 루프 동안만)
        for key, val in guard_adjustments.items():
            if hasattr(settings, key):
                original_guard_values.setdefault(key, getattr(settings, key))
                try:
                    setattr(settings, key, val)
                except Exception:
                    pass

        # 5-2) 거래량 가드
        vol_source = ws_candles_1m if ws_candles_1m else candles_5m_raw
        vol_ok = check_volume_guard(
            settings=settings,
            candles_5m_raw=vol_source,
            latest_ts=latest_ts,
            signal_source=signal_source,
            direction=chosen_signal,
        )
        if not vol_ok:
            try:
                send_skip_tg("[SKIP] volume_guard: volume_too_low_for_entry")
            except Exception as te:
                log(f"[ENTRY_GUARD] send_skip_tg failed on volume_guard: {te}")
            log_skip_event(
                symbol=symbol,
                regime=regime_label,
                source="entry_flow.guard.volume",
                side=chosen_signal,
                reason="volume_too_low_for_entry",
                extra={"latest_ts": int(latest_ts)},
            )
            return None, 1.0

        # 5-3) 직전/현재 5m 기준 가격 점프·갭 가드
        price_ok = check_price_jump_guard(
            settings=settings,
            candles_5m=candles_5m,
            latest_ts=latest_ts,
            signal_source=signal_source,
            direction=chosen_signal,
        )
        if not price_ok:
            try:
                send_skip_tg("[SKIP] price_jump_guard: recent_price_jump_or_gap")
            except Exception as te:
                log(f"[ENTRY_GUARD] send_skip_tg failed on price_jump_guard: {te}")
            log_skip_event(
                symbol=symbol,
                regime=regime_label,
                source="entry_flow.guard.price_jump",
                side=chosen_signal,
                reason="price_jump_guard_recent_price_jump_or_gap",
                extra={"latest_ts": int(latest_ts)},
            )
            return None, 1.0

        # 5-4) 호가 스프레드/쏠림 가드 (WS depth5 활용)
        spread_ok, best_bid, best_ask = check_spread_guard(
            settings=settings,
            symbol=symbol,
            latest_ts=latest_ts,
            signal_source=signal_source,
            direction=chosen_signal,
        )
        if not spread_ok:
            try:
                send_skip_tg(
                    "[SKIP] spread_guard: depth_imbalance_or_spread_too_wide"
                )
            except Exception as te:
                log(f"[ENTRY_GUARD] send_skip_tg failed on spread_guard: {te}")
            log_skip_event(
                symbol=symbol,
                regime=regime_label,
                source="entry_flow.guard.spread",
                side=chosen_signal,
                reason="depth_imbalance_or_spread_too_wide",
                extra={
                    "latest_ts": int(latest_ts),
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                },
            )
            return None, 1.0
    finally:
        # 5-5) settings 의 가드 값 복원 (guard_adjustments 는 1회성)
        for key, val in original_guard_values.items():
            try:
                setattr(settings, key, val)
            except Exception:
                pass

    # (5-6) 진입에 사용할 기준 가격 계산 (호가/캔들 기반)
    # - 수량 계산: mid price((best_bid + best_ask) / 2)
    # - 슬리피지 기준: LONG→best_ask, SHORT→best_bid
    price_for_qty = float(last_price) if last_price else 0.0
    price_for_hint = price_for_qty

    if (
        best_bid is not None
        and best_ask is not None
        and best_bid > 0
        and best_ask > 0
    ):
        mid = (best_bid + best_ask) / 2.0
        price_for_qty = mid
        if chosen_signal == "LONG":
            price_for_hint = float(best_ask)
        else:
            price_for_hint = float(best_bid)

    # 기준 가격 로그 (엔트리 가격·슬리피지 분석용)
    try:
        log(
            f"[ENTRY PRICE_REF] symbol={symbol} dir={chosen_signal} "
            f"last_price={last_price} best_bid={best_bid} best_ask={best_ask} "
            f"price_for_qty={price_for_qty} price_for_hint={price_for_hint}"
        )
    except Exception:
        pass

    # (6) 실제 주문 수량 계산
    # - available USDT * effective_risk_pct * leverage / price_for_qty
    # - 최소/단위는 settings 에 따라 맞춘다.
    notional = avail * effective_risk_pct * leverage
    qty = notional / max(float(price_for_qty), 1.0)

    # 수량 단위 및 최소 수량 적용
    qty_step = float(getattr(settings, "qty_step", 0.0001))
    min_qty = float(getattr(settings, "min_qty", qty_step))

    def _round_step(x: float, step: float) -> float:
        if step <= 0:
            return x
        return math.floor(x / step) * step

    qty = _round_step(qty, qty_step)
    if qty < min_qty:
        try:
            send_skip_tg(
                f"[SKIP] qty_too_small: qty={qty:.8f} < "
                f"min_qty={min_qty:.8f} (notional={notional:.4f}USDT)"
            )
        except Exception as te:
            log(f"[ENTRY_QTY] send_skip_tg failed on qty_too_small: {te}")
        log_skip_event(
            symbol=symbol,
            regime=regime_label,
            source="entry_flow.qty",
            side=chosen_signal,
            reason="qty_too_small",
            extra={"qty": qty, "min_qty": min_qty, "notional": notional},
        )
        return None, 5.0

    # (7) 실제 주문 실행
    side = "BUY" if chosen_signal == "LONG" else "SELL"
    regime_at_entry = regime_label

    # 시그널에서 넘어온 soft_mode / SL 바닥 비율(extra)을
    # open_position_with_tp_sl 에 그대로 전달 (과거 호환용)
    soft_mode = False
    sl_floor_ratio = None
    if isinstance(extra, dict):
        soft_mode = bool(
            extra.get("soft_mode")
            or extra.get("soft")
            or False
        )
        _sl_floor = extra.get("sl_floor_ratio")
        if isinstance(_sl_floor, (int, float)):
            sl_floor_ratio = float(_sl_floor)

    trade = open_position_with_tp_sl(
        settings=settings,
        symbol=symbol,
        side_open=side,
        qty=qty,
        entry_price_hint=float(price_for_hint),
        tp_pct=tp_pct,
        sl_pct=sl_pct,
        source=signal_source,
        soft_mode=soft_mode,
        sl_floor_ratio=sl_floor_ratio,
    )
    if trade is None:
        try:
            send_skip_tg("[SKIP] open_position_failed: trade_object_is_none")
        except Exception as te:
            log(f"[ENTRY_OPEN] send_skip_tg failed on trade_none: {te}")
        log_skip_event(
            symbol=symbol,
            regime=regime_at_entry,
            source="entry_flow.open_position",
            side=chosen_signal,
            reason="open_position_failed_trade_none",
            extra={
                "qty": qty,
                "tp_pct": tp_pct,
                "sl_pct": sl_pct,
                "effective_risk_pct": effective_risk_pct,
            },
        )
        return None, 3.0

    # (8) bt_trades INSERT
    _create_trade_row_on_entry(
        trade=trade,
        symbol=symbol,
        latest_ts_ms=int(latest_ts),
        regime_at_entry=regime_at_entry,
        signal_source=signal_source,
        effective_risk_pct=effective_risk_pct,
        tp_pct=tp_pct,
        sl_pct=sl_pct,
        is_auto=True,
    )

    # (9) EntryScore 저장 (GPT 메타 포함)
    entry_score: Optional[float] = None
    components: Dict[str, Any] = {}
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
        log(f"[ENTRY_SCORE] compute skipped on save: {e}")
        entry_score = None
        components = {}

    if entry_score is not None and components:
        # GPT/시그널 메타 정보 components_json 에 주입
        try:
            gpt_info: Dict[str, Any] = {
                "model": gpt_model_for_log,
                "action": gpt_action,
                "reason": gpt_reason,
                "decision": gpt_decision,
            }
            # market_features_ws 가 내려준 방향/레짐 정보도 함께 기록
            if isinstance(extra, dict):
                gpt_info["signal"] = {
                    "direction_raw": extra.get("direction_raw"),
                    "direction_norm": extra.get("direction_norm"),
                    "regime_level": extra.get("regime_level"),
                }
            components["gpt_entry"] = gpt_info

            # 시그널 extra 일부도 분석용으로 별도 블록에 보관(핵심 필드만 선별)
            if isinstance(extra, dict):
                signal_extra: Dict[str, Any] = {}
                allowed_keys = [
                    "signal_score",
                    "candidate_score",
                    "regime_level",
                    "gap_ratio_15m",
                    "width_ratio_5m",
                    "atr_fast",
                    "atr_slow",
                    "direction_raw",
                    "direction_norm",
                    "signal_reasons",
                    "block_reason",
                ]
                for k in allowed_keys:
                    if k in extra:
                        signal_extra[k] = extra[k]
                if signal_extra:
                    components["signal_extra"] = signal_extra
        except Exception as e:
            log(f"[ENTRY_SCORE] gpt_entry/signal_extra attach failed: {e}")

        _save_entry_score_to_db(
            symbol=symbol,
            ts_ms=int(latest_ts),
            side=chosen_signal,
            signal_type=signal_source,
            regime_at_entry=regime_at_entry,
            entry_score=entry_score,
            components=components,
            trade=trade,
            used_for_entry=True,
        )

    log(
        f"[ENTRY] opened: symbol={symbol} side={side} qty={qty:.8f} "
        f"lev={leverage} tp={tp_pct * 100:.2f}% sl={sl_pct * 100:.2f}%"
    )

    # 진입 후에는 짧은 쿨다운
    return trade, float(getattr(settings, "post_entry_sleep_sec", 5.0))


__all__ = [
    "try_open_new_position",
]
