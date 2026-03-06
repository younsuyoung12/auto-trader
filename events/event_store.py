"""
========================================================
FILE: events/event_store.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
역할:
- bt_events 테이블에 이벤트를 저장한다.

정책(STRICT):
- DB 기록 실패 시 예외 전파 (삼키기 금지)
- side는 LONG/SHORT/CLOSE만 허용
- extra_json은 dict로 정규화하여 JSONB로 저장
- 선택 문자열(regime/source)은 공백 문자열 저장 금지
  - 값이 없으면 NULL
  - 값이 있으면 non-empty trimmed string 이어야 함

변경 이력
--------------------------------------------------------
- 2026-03-01:
  1) bt_events 저장 레이어 신규 추가
  2) SQLAlchemy 바인딩 정합 수정
     - ':extra_json::jsonb' → 'CAST(:extra_json AS JSONB)' 로 변경하여 psycopg2 SyntaxError 제거
- 2026-03-02:
  1) commit 보장(STRICT)
     - get_session 구현에 의존하지 않고, INSERT 후 session.commit()을 명시 호출
- 2026-03-06:
  1) source/regime 선택 문자열 정규화 추가
     - 공백 문자열("") 저장 금지
     - 미제공 값은 NULL로 저장
  2) 선택 numeric 필드 STRICT 검증 강화
     - bool/비수치/비유한값 입력 시 즉시 예외
  3) extra_json STRICT 정규화 강화
     - dict 또는 JSON object 문자열만 허용
     - 빈 문자열/비객체 JSON/기타 타입 묵살 금지
========================================================
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from sqlalchemy import text

from state.db_core import get_session

KST = timezone(timedelta(hours=9))


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
    """
    ts = _require_tz_aware_datetime(ts_utc, "ts_utc")
    et = _require_nonempty_str(event_type, "event_type")
    sym = _normalize_symbol(symbol, "symbol")
    rsn = _require_nonempty_str(reason, "reason")
    sd = _normalize_side(side)

    regime_norm = _optional_nonempty_str(regime, "regime")
    source_norm = _optional_nonempty_str(source, "source")

    kst = ts.astimezone(KST)
    kst_date = kst.date()
    kst_hour = int(kst.hour)

    extra = _ensure_dict(extra_json, "extra_json")

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
        "price": _opt_float_strict(price, "price"),
        "qty": _opt_float_strict(qty, "qty"),
        "leverage": _opt_float_strict(leverage, "leverage"),
        "tp_pct": _opt_float_strict(tp_pct, "tp_pct"),
        "sl_pct": _opt_float_strict(sl_pct, "sl_pct"),
        "risk_pct": _opt_float_strict(risk_pct, "risk_pct"),
        "pnl_pct": _opt_float_strict(pnl_pct, "pnl_pct"),
        "reason": rsn,
        "extra_json": json.dumps(extra, ensure_ascii=False),
        "is_test": bool(is_test),
    }

    with get_session() as session:
        session.execute(sql, params)
        session.commit()


__all__ = [
    "record_event_db",
]