# events/signals_logger.py
"""
========================================================
events/signals_logger.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
역할:
- 모든 이벤트를 bt_events(DB)에 저장하고, EventBus로 publish한다.

정책(STRICT):
- DB 기록 실패 시 예외 전파
- EventBus validate 실패 시 예외 전파
- side는 LONG/SHORT/CLOSE만 허용
- extra_json은 dict로 정규화하여 JSONB 저장

변경 이력
--------------------------------------------------------
- 2026-03-01:
  1) CSV 기록 제거 → bt_events DB 저장으로 전환
  2) legacy wrapper(log_candle_snapshot/log_gpt_entry_event/log_gpt_exit_event/_ensure_today_csv) 유지
- 2026-03-03:
  1) FIX(STRICT): log_signal()에서 side 미제공 시 direction을 STRICT 매핑해 side로 주입
     - 레거시 콜사이트(direction만 전달)가 존재
     - side/direction 모두 없으면 즉시 예외(폴백 금지)
========================================================
"""

from __future__ import annotations

import datetime
from typing import Any, Dict

from events.event_bus import publish_event
from events.event_store import record_event_db


def _now_utc() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


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


def _side_from_direction_strict(direction: Any) -> str:
    """
    STRICT:
    - 레거시 호환: direction -> side 매핑만 수행한다.
    - 잘못된 값이면 즉시 예외(폴백 금지)
    """
    d = str(direction or "").upper().strip()
    if d in ("LONG", "SHORT", "CLOSE"):
        return d
    if d == "BUY":
        return "LONG"
    if d == "SELL":
        return "SHORT"
    raise RuntimeError("payload.direction must be LONG/SHORT/CLOSE (or BUY/SELL)")


def _normalize_extra_json(v: Any) -> Dict[str, Any]:
    """
    STRICT:
    - bt_events.extra_json은 jsonb dict 전제.
    - dict가 아니면 즉시 예외(조용히 변환/폴백 금지)
    """
    if v is None:
        return {}
    if not isinstance(v, dict):
        raise RuntimeError(f"extra_json must be dict (got={type(v)})")
    return v


def log_event(event_type: str, **kwargs: Any) -> None:
    et = _require_nonempty_str(event_type, "event_type")
    symbol = _require_nonempty_str(kwargs.get("symbol"), "symbol")
    reason = _require_nonempty_str(kwargs.get("reason"), "reason")

    side = _normalize_side(kwargs.get("side"))

    ts_utc = _now_utc()

    extra_json = _normalize_extra_json(kwargs.get("extra_json", kwargs.get("extra")))

    payload: Dict[str, Any] = {
        "symbol": symbol,
        "regime": str(kwargs.get("regime") or ""),
        "source": str(kwargs.get("source") or ""),
        "side": side,
        "price": kwargs.get("price"),
        "qty": kwargs.get("qty"),
        "leverage": kwargs.get("leverage"),
        "tp_pct": kwargs.get("tp_pct"),
        "sl_pct": kwargs.get("sl_pct"),
        "risk_pct": kwargs.get("risk_pct"),
        "pnl_pct": kwargs.get("pnl_pct"),
        "reason": reason,
        "extra_json": extra_json,
        "ts_kst_epoch": ts_utc.timestamp(),  # legacy key
        "ts_iso": ts_utc.isoformat(),
    }

    # 1) DB 저장
    record_event_db(
        ts_utc=ts_utc,
        event_type=et,
        symbol=symbol,
        regime=payload["regime"],
        source=payload["source"],
        side=side,
        price=payload["price"],
        qty=payload["qty"],
        leverage=payload["leverage"],
        tp_pct=payload["tp_pct"],
        sl_pct=payload["sl_pct"],
        risk_pct=payload["risk_pct"],
        pnl_pct=payload["pnl_pct"],
        reason=reason,
        extra_json=payload["extra_json"],
        is_test=bool(kwargs.get("is_test", False)),
    )

    # 2) EventBus publish (strict validation 포함)
    publish_event(et, **payload)


def log_signal(**kwargs: Any) -> None:
    """
    STRICT:
    - event 필수
    - side 필수
    - (호환) side 미제공 시 direction을 STRICT 매핑하여 side로 주입
    """
    event = kwargs.pop("event", None)
    et = _require_nonempty_str(event, "event")

    # ✅ FIX: 레거시 호출(direction만 넘김) 호환
    if "side" not in kwargs or kwargs.get("side") in (None, ""):
        if "direction" in kwargs:
            kwargs["side"] = _side_from_direction_strict(kwargs.get("direction"))
        else:
            raise RuntimeError("payload.side is required (or provide payload.direction for compatibility)")

    log_event(et, **kwargs)


def log_skip_event(**kwargs: Any) -> None:
    log_event("SKIP", **kwargs)


def log_candle_snapshot(
    *,
    symbol: str,
    tf: str,
    candle_ts: int,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: float,
    strategy_type: str,
    direction: str,
    extra: str,
) -> None:
    sym = _require_nonempty_str(symbol, "symbol")
    tf_s = _require_nonempty_str(tf, "tf")
    strat = _require_nonempty_str(strategy_type, "strategy_type")
    dir_s = _require_nonempty_str(direction, "direction").upper()

    if dir_s not in ("LONG", "SHORT"):
        raise RuntimeError("direction must be LONG/SHORT")

    if not isinstance(candle_ts, int) or candle_ts <= 0:
        raise RuntimeError("candle_ts must be int > 0")

    log_event(
        "CANDLE_SNAPSHOT",
        symbol=sym,
        regime=str(strat),
        source="candle_snapshot",
        side=dir_s,
        price=float(close),
        qty=float(volume),
        reason="candle_snapshot",
        extra_json={
            "tf": tf_s,
            "candle_ts": int(candle_ts),
            "open": float(open_),
            "high": float(high),
            "low": float(low),
            "close": float(close),
            "volume": float(volume),
            "strategy_type": strat,
            "direction": dir_s,
            "extra": str(extra or ""),
        },
    )


def log_gpt_entry_event(**kwargs: Any) -> None:
    symbol = _require_nonempty_str(kwargs.get("symbol"), "symbol")
    reason = _require_nonempty_str(kwargs.get("reason"), "reason")

    regime = str(kwargs.get("regime") or "")
    side = _normalize_side(kwargs.get("side") or "CLOSE")
    action = str(kwargs.get("action") or kwargs.get("gpt_action") or "")

    log_event(
        "GPT_ENTRY",
        symbol=symbol,
        regime=regime,
        source=str(kwargs.get("source") or "gpt_entry"),
        side=side,
        tp_pct=kwargs.get("tp_pct"),
        sl_pct=kwargs.get("sl_pct"),
        risk_pct=kwargs.get("risk_pct"),
        reason=reason,
        extra_json={
            "action": action,
            "gpt_json": kwargs.get("gpt_json") if kwargs.get("gpt_json") is not None else {},
        },
    )


def log_gpt_exit_event(**kwargs: Any) -> None:
    symbol = _require_nonempty_str(kwargs.get("symbol"), "symbol")
    reason = _require_nonempty_str(kwargs.get("reason"), "reason")

    regime = str(kwargs.get("regime") or "")
    side = _normalize_side(kwargs.get("side") or "CLOSE")
    action = str(kwargs.get("action") or kwargs.get("gpt_action") or "")

    log_event(
        "GPT_EXIT",
        symbol=symbol,
        regime=regime,
        source=str(kwargs.get("source") or "gpt_exit"),
        side=side,
        reason=reason,
        extra_json={
            "action": action,
            "close_ratio": kwargs.get("close_ratio"),
            "new_tp_pct": kwargs.get("new_tp_pct"),
            "new_sl_pct": kwargs.get("new_sl_pct"),
            "gpt_json": kwargs.get("gpt_json") if kwargs.get("gpt_json") is not None else {},
        },
    )


def _ensure_today_csv() -> None:
    # legacy wrapper: CSV 제거됨. 호출되더라도 동작에 영향 없도록 no-op 유지.
    return None


__all__ = [
    "log_event",
    "log_signal",
    "log_skip_event",
    "log_candle_snapshot",
    "log_gpt_entry_event",
    "log_gpt_exit_event",
    "_ensure_today_csv",
]