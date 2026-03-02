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

변경 이력
--------------------------------------------------------
- 2026-03-01: bt_events 저장 레이어 신규 추가
- 2026-03-01: SQLAlchemy 바인딩 정합 수정
  - ':extra_json::jsonb' → 'CAST(:extra_json AS JSONB)' 로 변경하여 psycopg2 SyntaxError 제거
- 2026-03-02: commit 보장(STRICT)
  - get_session 구현에 의존하지 않고, INSERT 후 session.commit()을 명시 호출
========================================================
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from sqlalchemy import text

from state.db_core import get_session

KST = timezone(timedelta(hours=9))


def _require_nonempty_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        raise RuntimeError(f"{name} is required")
    return s


def _normalize_side(side: Any) -> str:
    s = str(side or "").upper().strip()
    if s in ("LONG", "SHORT", "CLOSE"):
        return s
    raise RuntimeError("payload.side must be LONG/SHORT/CLOSE")


def _opt_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        return None
    try:
        x = float(v)
    except Exception:
        return None
    return x


def _ensure_dict(v: Any) -> Dict[str, Any]:
    if v is None:
        return {}
    if isinstance(v, dict):
        return v
    if isinstance(v, str):
        ss = v.strip()
        if not ss:
            return {}
        try:
            obj = json.loads(ss)
            return obj if isinstance(obj, dict) else {"value": obj}
        except Exception:
            return {"value": ss}
    return {"value": str(v)}


def record_event_db(
    *,
    ts_utc: datetime,
    event_type: str,
    symbol: str,
    regime: str = "",
    source: str = "",
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
    """
    if (
        not isinstance(ts_utc, datetime)
        or ts_utc.tzinfo is None
        or ts_utc.tzinfo.utcoffset(ts_utc) is None
    ):
        raise RuntimeError("ts_utc must be timezone-aware datetime")

    et = _require_nonempty_str(event_type, "event_type")
    sym = _require_nonempty_str(symbol, "symbol").upper()
    rsn = _require_nonempty_str(reason, "reason")
    sd = _normalize_side(side)

    kst = ts_utc.astimezone(KST)
    kst_date = kst.date()
    kst_hour = int(kst.hour)

    extra = _ensure_dict(extra_json)

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
        "ts_utc": ts_utc,
        "ts_kst_date": kst_date,
        "ts_kst_hour": kst_hour,
        "event_type": et,
        "symbol": sym,
        "regime": str(regime or ""),
        "source": str(source or ""),
        "side": sd,
        "price": _opt_float(price),
        "qty": _opt_float(qty),
        "leverage": _opt_float(leverage),
        "tp_pct": _opt_float(tp_pct),
        "sl_pct": _opt_float(sl_pct),
        "risk_pct": _opt_float(risk_pct),
        "pnl_pct": _opt_float(pnl_pct),
        "reason": rsn,
        "extra_json": json.dumps(extra, ensure_ascii=False),
        "is_test": bool(is_test),
    }

    with get_session() as session:
        session.execute(sql, params)
        # STRICT: commit을 명시한다 (get_session 구현에 의존하지 않음)
        session.commit()


__all__ = [
    "record_event_db",
]