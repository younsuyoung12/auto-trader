"""
========================================================
FILE: events/event_store.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================
역할:
- bt_events 테이블에 이벤트를 저장한다.
- 거래 이벤트와 AI 분석 이벤트를 동일한 감사 로그 테이블에 기록한다.

정책(STRICT):
- DB 기록 실패 시 예외 전파 (삼키기 금지)
- side는 LONG/SHORT/CLOSE만 허용
- extra_json은 dict로 정규화하여 JSONB로 저장
- 선택 문자열(regime/source)은 공백 문자열 저장 금지
  - 값이 없으면 NULL
  - 값이 있으면 non-empty trimmed string 이어야 함
- 데이터 무결성 규칙
  - symbol 필수
  - timestamp timezone-aware 필수
  - price/qty/leverage/tp_pct/sl_pct/risk_pct 는 제공 시 유한 실수 + 양수여야 함
  - pnl_pct 는 제공 시 유한 실수여야 함
- AI 분석 이벤트는 event_type='QUANT_ANALYSIS' 로 저장하며
  bt_events side 정책에 따라 side='CLOSE' 를 사용한다.
  report_type 은 source 에 저장한다.

변경 이력
--------------------------------------------------------
- 2026-03-07:
  1) 헤더를 AI TRADING INTELLIGENCE SYSTEM 규약으로 정합화
  2) logging 추가(print 금지 정책 정합)
  3) 데이터 무결성 검증 강화
     - price/qty/leverage/tp_pct/sl_pct/risk_pct 제공 시 양수 강제
     - pnl_pct 제공 시 유한 실수 강제
  4) AI 분석 이벤트 기록 함수 추가
     - record_quant_analysis_event_db
     - event_type='QUANT_ANALYSIS'
     - source=report_type
     - extra_json에 분석 payload 저장
- 2026-03-06:
  1) source/regime 선택 문자열 정규화 추가
     - 공백 문자열("") 저장 금지
     - 미제공 값은 NULL로 저장
  2) 선택 numeric 필드 STRICT 검증 강화
     - bool/비수치/비유한값 입력 시 즉시 예외
  3) extra_json STRICT 정규화 강화
     - dict 또는 JSON object 문자열만 허용
     - 빈 문자열/비객체 JSON/기타 타입 묵살 금지
- 2026-03-02:
  1) commit 보장(STRICT)
     - get_session 구현에 의존하지 않고, INSERT 후 session.commit()을 명시 호출
- 2026-03-01:
  1) bt_events 저장 레이어 신규 추가
  2) SQLAlchemy 바인딩 정합 수정
     - ':extra_json::jsonb' → 'CAST(:extra_json AS JSONB)' 로 변경하여 psycopg2 SyntaxError 제거
========================================================
"""

from __future__ import annotations

import json
import logging
import math
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Optional

from sqlalchemy import text

from state.db_core import get_session

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))
_QUANT_ANALYSIS_EVENT_TYPE = "QUANT_ANALYSIS"
_NON_TRADE_ANALYSIS_SIDE = "CLOSE"


def _require_tz_aware_datetime(v: Any, name: str) -> datetime:
    if not isinstance(v, datetime):
        raise RuntimeError(f"{name} must be datetime (STRICT)")
    if v.tzinfo is None or v.tzinfo.utcoffset(v) is None:
        raise RuntimeError(f"{name} must be timezone-aware datetime (STRICT)")
    return v


def _require_nonempty_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        raise RuntimeError(f"{name} is required (STRICT)")
    return s


def _normalize_symbol(v: Any, name: str) -> str:
    s = _require_nonempty_str(v, name).replace("-", "").replace("/", "").upper().strip()
    if not s:
        raise RuntimeError(f"{name} normalized empty (STRICT)")
    return s


def _optional_nonempty_str(v: Any, name: str) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    return s


def _normalize_side(side: Any) -> str:
    s = str(side or "").upper().strip()
    if s in ("LONG", "SHORT", "CLOSE"):
        return s
    raise RuntimeError("payload.side must be LONG/SHORT/CLOSE (STRICT)")


def _opt_float_strict(v: Any, name: str) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, str) and not v.strip():
        return None
    if isinstance(v, bool):
        raise RuntimeError(f"{name} must be numeric (bool not allowed) (STRICT)")
    try:
        x = float(v)
    except Exception as e:
        raise RuntimeError(f"{name} must be numeric: {e} (STRICT)") from e
    if not math.isfinite(x):
        raise RuntimeError(f"{name} must be finite (STRICT)")
    return float(x)


def _opt_positive_float_strict(v: Any, name: str) -> Optional[float]:
    x = _opt_float_strict(v, name)
    if x is None:
        return None
    if x <= 0.0:
        raise RuntimeError(f"{name} must be > 0 when provided (STRICT)")
    return x


def _ensure_dict(v: Any, name: str) -> Dict[str, Any]:
    if v is None:
        return {}

    if isinstance(v, dict):
        return v

    if isinstance(v, str):
        ss = v.strip()
        if not ss:
            raise RuntimeError(f"{name} blank string is not allowed (STRICT)")
        try:
            obj = json.loads(ss)
        except Exception as e:
            raise RuntimeError(f"{name} string must be valid JSON object: {e} (STRICT)") from e
        if not isinstance(obj, dict):
            raise RuntimeError(f"{name} JSON must decode to object/dict (STRICT)")
        return obj

    raise RuntimeError(f"{name} must be dict, JSON object string, or None (STRICT)")


def _ensure_json_serializable_dict(v: Any, name: str) -> Dict[str, Any]:
    obj = _ensure_dict(v, name)
    try:
        json.dumps(obj, ensure_ascii=False)
    except (TypeError, ValueError) as e:
        raise RuntimeError(f"{name} must be JSON serializable object: {e} (STRICT)") from e
    return obj


def _kst_date_hour(ts: datetime) -> tuple[date, int]:
    kst = ts.astimezone(KST)
    return kst.date(), int(kst.hour)


def _insert_bt_event(
    *,
    ts_utc: datetime,
    event_type: str,
    symbol: str,
    regime: Optional[str],
    source: Optional[str],
    side: str,
    price: Optional[float],
    qty: Optional[float],
    leverage: Optional[float],
    tp_pct: Optional[float],
    sl_pct: Optional[float],
    risk_pct: Optional[float],
    pnl_pct: Optional[float],
    reason: str,
    extra: Dict[str, Any],
    is_test: bool,
) -> None:
    ts = _require_tz_aware_datetime(ts_utc, "ts_utc")
    et = _require_nonempty_str(event_type, "event_type")
    sym = _normalize_symbol(symbol, "symbol")
    rsn = _require_nonempty_str(reason, "reason")
    sd = _normalize_side(side)

    regime_norm = _optional_nonempty_str(regime, "regime")
    source_norm = _optional_nonempty_str(source, "source")
    kst_date, kst_hour = _kst_date_hour(ts)

    sql = text(
        """
        INSERT INTO bt_events (
          ts_utc, ts_kst_date, ts_kst_hour,
          event_type, symbol, regime, source, side,
          price, qty, leverage, tp_pct, sl_pct, risk_pct, pnl_pct,
          reason, extra_json, is_test
        ) VALUES (
          :ts_utc, :ts_kst_date, :ts_kst_hour,
          :event_type, :symbol, :regime, :source, :side,
          :price, :qty, :leverage, :tp_pct, :sl_pct, :risk_pct, :pnl_pct,
          :reason, CAST(:extra_json AS JSONB), :is_test
        )
        """
    )

    params = {
        "ts_utc": ts,
        "ts_kst_date": kst_date,
        "ts_kst_hour": kst_hour,
        "event_type": et,
        "symbol": sym,
        "regime": regime_norm,
        "source": source_norm,
        "side": sd,
        "price": price,
        "qty": qty,
        "leverage": leverage,
        "tp_pct": tp_pct,
        "sl_pct": sl_pct,
        "risk_pct": risk_pct,
        "pnl_pct": pnl_pct,
        "reason": rsn,
        "extra_json": json.dumps(extra, ensure_ascii=False),
        "is_test": bool(is_test),
    }

    try:
        with get_session() as session:
            session.execute(sql, params)
            session.commit()
    except Exception as e:
        logger.error(
            "bt_events insert failed: event_type=%s symbol=%s source=%s side=%s error=%s",
            et,
            sym,
            source_norm,
            sd,
            e.__class__.__name__,
        )
        raise


def record_event_db(
    *,
    ts_utc: datetime,
    event_type: str,
    symbol: str,
    regime: Optional[str] = None,
    source: Optional[str] = None,
    side: Any,
    price: Any = None,
    qty: Any = None,
    leverage: Any = None,
    tp_pct: Any = None,
    sl_pct: Any = None,
    risk_pct: Any = None,
    pnl_pct: Any = None,
    reason: str,
    extra_json: Any = None,
    is_test: bool = False,
) -> None:
    """
    STRICT:
    - 실패 시 예외 전파(폴백 금지)
    - extra_json은 JSONB로 저장(CAST 사용)
    - regime/source 는 공백 문자열을 저장하지 않는다.
      값이 없으면 NULL, 값이 있으면 trimmed non-empty string만 허용한다.
    - 데이터 무결성 규칙:
      - price/qty/leverage/tp_pct/sl_pct/risk_pct 는 제공 시 양수여야 한다.
      - pnl_pct 는 제공 시 유한 실수여야 한다.
    """
    extra = _ensure_json_serializable_dict(extra_json, "extra_json")

    _insert_bt_event(
        ts_utc=ts_utc,
        event_type=event_type,
        symbol=symbol,
        regime=regime,
        source=source,
        side=_normalize_side(side),
        price=_opt_positive_float_strict(price, "price"),
        qty=_opt_positive_float_strict(qty, "qty"),
        leverage=_opt_positive_float_strict(leverage, "leverage"),
        tp_pct=_opt_positive_float_strict(tp_pct, "tp_pct"),
        sl_pct=_opt_positive_float_strict(sl_pct, "sl_pct"),
        risk_pct=_opt_positive_float_strict(risk_pct, "risk_pct"),
        pnl_pct=_opt_float_strict(pnl_pct, "pnl_pct"),
        reason=reason,
        extra=extra,
        is_test=is_test,
    )


def record_quant_analysis_event_db(
    *,
    ts_utc: datetime,
    symbol: str,
    report_type: str,
    reason: str,
    analysis_payload: Any,
    regime: Optional[str] = None,
    is_test: bool = False,
) -> None:
    """
    AI 분석 이벤트 저장.

    저장 규칙:
    - event_type = QUANT_ANALYSIS
    - source     = report_type
    - side       = CLOSE  (bt_events side 정책 준수용 비거래 분석 이벤트 표준값)
    - extra_json = 분석 payload 전체(JSON object)

    report_type 예:
    - market_report
    - system_report
    - dashboard_query
    """
    report_type_norm = _require_nonempty_str(report_type, "report_type")
    payload = _ensure_json_serializable_dict(analysis_payload, "analysis_payload")
    if not payload:
        raise RuntimeError("analysis_payload must not be empty (STRICT)")

    _insert_bt_event(
        ts_utc=ts_utc,
        event_type=_QUANT_ANALYSIS_EVENT_TYPE,
        symbol=symbol,
        regime=regime,
        source=report_type_norm,
        side=_NON_TRADE_ANALYSIS_SIDE,
        price=None,
        qty=None,
        leverage=None,
        tp_pct=None,
        sl_pct=None,
        risk_pct=None,
        pnl_pct=None,
        reason=reason,
        extra=payload,
        is_test=is_test,
    )


__all__ = [
    "record_event_db",
    "record_quant_analysis_event_db",
]