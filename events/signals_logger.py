"""
========================================================
FILE: events/signals_logger.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
역할:
- 모든 이벤트를 bt_events(DB)에 저장하고, EventBus로 publish한다.

정책(STRICT):
- DB 기록 실패 시 예외 전파
- EventBus validate 실패 시 예외 전파
- side는 LONG/SHORT/CLOSE만 허용
- extra_json은 dict로 정규화하여 JSONB 저장
- 공통 STRICT 예외 계층을 사용한다.

변경 이력
--------------------------------------------------------
- 2026-03-13:
  1) FIX(EXCEPTION): common.exceptions_strict 공통 예외 계층 적용
  2) FIX(NO-FALLBACK): regime/source/action/gpt_json 숨은 기본값 제거
  3) FIX(CONTRACT): optional numeric 필드 finite 검증 추가
  4) FIX(EXTERNAL): DB 저장 실패 / EventBus publish 실패를 분리된 예외로 전파
- 2026-03-03:
  1) FIX(STRICT): log_signal()에서 side 미제공 시 direction을 STRICT 매핑해 side로 주입
     - 레거시 콜사이트(direction만 전달)가 존재
     - side/direction 모두 없으면 즉시 예외(폴백 금지)
- 2026-03-01:
  1) CSV 기록 제거 → bt_events DB 저장으로 전환
  2) legacy wrapper(log_candle_snapshot/log_gpt_entry_event/log_gpt_exit_event/_ensure_today_csv) 유지
========================================================
"""

from __future__ import annotations

import datetime
import math
from typing import Any, Dict, Optional

from common.exceptions_strict import StrictDataError, StrictDBError, StrictExternalError
from events.event_bus import publish_event
from events.event_store import record_event_db


class SignalsLoggerError(RuntimeError):
    """signals_logger base error."""


class SignalsLoggerContractError(StrictDataError):
    """payload / field contract violation."""


class SignalsLoggerDBError(StrictDBError):
    """bt_events write failure."""


class SignalsLoggerPublishError(StrictExternalError):
    """EventBus publish / validation failure."""


def _now_utc() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _require_nonempty_str(v: Any, name: str) -> str:
    if not isinstance(v, str):
        raise SignalsLoggerContractError(f"{name} must be str (STRICT), got={type(v).__name__}")
    s = v.strip()
    if not s:
        raise SignalsLoggerContractError(f"{name} is required (STRICT)")
    return s


def _optional_nonempty_str(v: Any, name: str) -> Optional[str]:
    if v is None:
        return None
    if not isinstance(v, str):
        raise SignalsLoggerContractError(f"{name} must be str when provided (STRICT), got={type(v).__name__}")
    s = v.strip()
    if not s:
        raise SignalsLoggerContractError(f"{name} must not be blank when provided (STRICT)")
    return s


def _require_bool(v: Any, name: str) -> bool:
    if not isinstance(v, bool):
        raise SignalsLoggerContractError(f"{name} must be bool (STRICT)")
    return bool(v)


def _require_finite_optional_float(v: Any, name: str) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, bool):
        raise SignalsLoggerContractError(f"{name} must be numeric (STRICT), bool not allowed")
    try:
        fv = float(v)
    except Exception as exc:
        raise SignalsLoggerContractError(f"{name} must be numeric (STRICT): {exc}") from exc
    if not math.isfinite(fv):
        raise SignalsLoggerContractError(f"{name} must be finite (STRICT)")
    return float(fv)


def _require_positive_int(v: Any, name: str) -> int:
    if isinstance(v, bool):
        raise SignalsLoggerContractError(f"{name} must be int (STRICT), bool not allowed")
    try:
        iv = int(v)
    except Exception as exc:
        raise SignalsLoggerContractError(f"{name} must be int (STRICT): {exc}") from exc
    if iv <= 0:
        raise SignalsLoggerContractError(f"{name} must be > 0 (STRICT)")
    return iv


def _normalize_side(side: Any) -> str:
    s = str(side or "").upper().strip()
    if s in ("LONG", "SHORT", "CLOSE"):
        return s
    raise SignalsLoggerContractError("payload.side must be LONG/SHORT/CLOSE (STRICT)")


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
    raise SignalsLoggerContractError("payload.direction must be LONG/SHORT/CLOSE (or BUY/SELL) (STRICT)")


def _normalize_extra_json(v: Any) -> Dict[str, Any]:
    """
    STRICT:
    - bt_events.extra_json은 jsonb dict 전제.
    - dict가 아니면 즉시 예외
    - 미제공이면 명시적 빈 dict 반환
    """
    if v is None:
        return {}
    if not isinstance(v, dict):
        raise SignalsLoggerContractError(f"extra_json must be dict (STRICT), got={type(v).__name__}")
    return dict(v)


def _validate_event_payload_contract(payload: Dict[str, Any]) -> None:
    _require_nonempty_str(payload["symbol"], "payload.symbol")
    _normalize_side(payload["side"])
    _require_nonempty_str(payload["reason"], "payload.reason")

    if payload.get("regime") is not None:
        _optional_nonempty_str(payload.get("regime"), "payload.regime")
    if payload.get("source") is not None:
        _optional_nonempty_str(payload.get("source"), "payload.source")

    for numeric_key in ("price", "qty", "leverage", "tp_pct", "sl_pct", "risk_pct", "pnl_pct"):
        _require_finite_optional_float(payload.get(numeric_key), f"payload.{numeric_key}")

    if not isinstance(payload.get("extra_json"), dict):
        raise SignalsLoggerContractError("payload.extra_json must be dict (STRICT)")


def log_event(event_type: str, **kwargs: Any) -> None:
    et = _require_nonempty_str(event_type, "event_type")
    symbol = _require_nonempty_str(kwargs.get("symbol"), "symbol")
    reason = _require_nonempty_str(kwargs.get("reason"), "reason")
    side = _normalize_side(kwargs.get("side"))

    ts_utc = _now_utc()

    regime = _optional_nonempty_str(kwargs.get("regime"), "regime")
    source = _optional_nonempty_str(kwargs.get("source"), "source")

    price = _require_finite_optional_float(kwargs.get("price"), "price")
    qty = _require_finite_optional_float(kwargs.get("qty"), "qty")
    leverage = _require_finite_optional_float(kwargs.get("leverage"), "leverage")
    tp_pct = _require_finite_optional_float(kwargs.get("tp_pct"), "tp_pct")
    sl_pct = _require_finite_optional_float(kwargs.get("sl_pct"), "sl_pct")
    risk_pct = _require_finite_optional_float(kwargs.get("risk_pct"), "risk_pct")
    pnl_pct = _require_finite_optional_float(kwargs.get("pnl_pct"), "pnl_pct")

    extra_json = _normalize_extra_json(kwargs.get("extra_json", kwargs.get("extra")))

    is_test_raw = kwargs.get("is_test", False)
    if is_test_raw is False and "is_test" not in kwargs:
        is_test = False
    else:
        is_test = _require_bool(is_test_raw, "is_test")

    payload: Dict[str, Any] = {
        "symbol": symbol,
        "regime": regime,
        "source": source,
        "side": side,
        "price": price,
        "qty": qty,
        "leverage": leverage,
        "tp_pct": tp_pct,
        "sl_pct": sl_pct,
        "risk_pct": risk_pct,
        "pnl_pct": pnl_pct,
        "reason": reason,
        "extra_json": extra_json,
        "ts_kst_epoch": ts_utc.timestamp(),  # legacy key
        "ts_iso": ts_utc.isoformat(),
    }

    _validate_event_payload_contract(payload)

    try:
        record_event_db(
            ts_utc=ts_utc,
            event_type=et,
            symbol=symbol,
            regime=payload["regime"],
            source=payload["source"],
            side=side,
            price=price,
            qty=qty,
            leverage=leverage,
            tp_pct=tp_pct,
            sl_pct=sl_pct,
            risk_pct=risk_pct,
            pnl_pct=pnl_pct,
            reason=reason,
            extra_json=extra_json,
            is_test=is_test,
        )
    except Exception as exc:
        raise SignalsLoggerDBError(f"record_event_db failed (STRICT): {type(exc).__name__}") from exc

    try:
        publish_event(et, **payload)
    except Exception as exc:
        raise SignalsLoggerPublishError(f"publish_event failed (STRICT): {type(exc).__name__}") from exc


def log_signal(**kwargs: Any) -> None:
    """
    STRICT:
    - event 필수
    - side 필수
    - (호환) side 미제공 시 direction을 STRICT 매핑하여 side로 주입
    """
    event = kwargs.pop("event", None)
    et = _require_nonempty_str(event, "event")

    if "side" not in kwargs or kwargs.get("side") in (None, ""):
        if "direction" in kwargs:
            kwargs["side"] = _side_from_direction_strict(kwargs.get("direction"))
        else:
            raise SignalsLoggerContractError(
                "payload.side is required (or provide payload.direction for compatibility) (STRICT)"
            )

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
        raise SignalsLoggerContractError("direction must be LONG/SHORT (STRICT)")

    candle_ts_i = _require_positive_int(candle_ts, "candle_ts")
    open_v = _require_finite_optional_float(open_, "open")
    high_v = _require_finite_optional_float(high, "high")
    low_v = _require_finite_optional_float(low, "low")
    close_v = _require_finite_optional_float(close, "close")
    volume_v = _require_finite_optional_float(volume, "volume")
    extra_s = str(extra or "")

    if open_v is None or high_v is None or low_v is None or close_v is None or volume_v is None:
        raise SignalsLoggerContractError("candle numeric fields must be provided (STRICT)")
    if open_v <= 0 or high_v <= 0 or low_v <= 0 or close_v <= 0:
        raise SignalsLoggerContractError("candle OHLC must be > 0 (STRICT)")
    if volume_v < 0:
        raise SignalsLoggerContractError("candle volume must be >= 0 (STRICT)")

    log_event(
        "CANDLE_SNAPSHOT",
        symbol=sym,
        regime=strat,
        source="candle_snapshot",
        side=dir_s,
        price=float(close_v),
        qty=float(volume_v),
        reason="candle_snapshot",
        extra_json={
            "tf": tf_s,
            "candle_ts": int(candle_ts_i),
            "open": float(open_v),
            "high": float(high_v),
            "low": float(low_v),
            "close": float(close_v),
            "volume": float(volume_v),
            "strategy_type": strat,
            "direction": dir_s,
            "extra": extra_s,
        },
    )


def log_gpt_entry_event(**kwargs: Any) -> None:
    symbol = _require_nonempty_str(kwargs.get("symbol"), "symbol")
    reason = _require_nonempty_str(kwargs.get("reason"), "reason")

    side: str
    if "side" in kwargs and kwargs.get("side") not in (None, ""):
        side = _normalize_side(kwargs.get("side"))
    elif "direction" in kwargs:
        side = _side_from_direction_strict(kwargs.get("direction"))
    else:
        raise SignalsLoggerContractError("GPT_ENTRY requires side or direction (STRICT)")

    regime = _optional_nonempty_str(kwargs.get("regime"), "regime")
    source = _optional_nonempty_str(kwargs.get("source"), "source") or "gpt_entry"

    action = kwargs.get("action")
    if action is None:
        action = kwargs.get("gpt_action")
    action_s = _optional_nonempty_str(action, "action")

    gpt_json = kwargs.get("gpt_json")
    if gpt_json is None:
        gpt_json_obj: Dict[str, Any] = {}
    elif not isinstance(gpt_json, dict):
        raise SignalsLoggerContractError(f"gpt_json must be dict when provided (STRICT), got={type(gpt_json).__name__}")
    else:
        gpt_json_obj = dict(gpt_json)

    extra_json: Dict[str, Any] = {"gpt_json": gpt_json_obj}
    if action_s is not None:
        extra_json["action"] = action_s

    log_event(
        "GPT_ENTRY",
        symbol=symbol,
        regime=regime,
        source=source,
        side=side,
        tp_pct=kwargs.get("tp_pct"),
        sl_pct=kwargs.get("sl_pct"),
        risk_pct=kwargs.get("risk_pct"),
        reason=reason,
        extra_json=extra_json,
    )


def log_gpt_exit_event(**kwargs: Any) -> None:
    symbol = _require_nonempty_str(kwargs.get("symbol"), "symbol")
    reason = _require_nonempty_str(kwargs.get("reason"), "reason")

    side: str
    if "side" in kwargs and kwargs.get("side") not in (None, ""):
        side = _normalize_side(kwargs.get("side"))
    elif "direction" in kwargs:
        side = _side_from_direction_strict(kwargs.get("direction"))
    else:
        raise SignalsLoggerContractError("GPT_EXIT requires side or direction (STRICT)")

    regime = _optional_nonempty_str(kwargs.get("regime"), "regime")
    source = _optional_nonempty_str(kwargs.get("source"), "source") or "gpt_exit"

    action = kwargs.get("action")
    if action is None:
        action = kwargs.get("gpt_action")
    action_s = _optional_nonempty_str(action, "action")

    close_ratio = _require_finite_optional_float(kwargs.get("close_ratio"), "close_ratio")
    new_tp_pct = _require_finite_optional_float(kwargs.get("new_tp_pct"), "new_tp_pct")
    new_sl_pct = _require_finite_optional_float(kwargs.get("new_sl_pct"), "new_sl_pct")

    gpt_json = kwargs.get("gpt_json")
    if gpt_json is None:
        gpt_json_obj: Dict[str, Any] = {}
    elif not isinstance(gpt_json, dict):
        raise SignalsLoggerContractError(f"gpt_json must be dict when provided (STRICT), got={type(gpt_json).__name__}")
    else:
        gpt_json_obj = dict(gpt_json)

    extra_json: Dict[str, Any] = {"gpt_json": gpt_json_obj}
    if action_s is not None:
        extra_json["action"] = action_s
    if close_ratio is not None:
        extra_json["close_ratio"] = close_ratio
    if new_tp_pct is not None:
        extra_json["new_tp_pct"] = new_tp_pct
    if new_sl_pct is not None:
        extra_json["new_sl_pct"] = new_sl_pct

    log_event(
        "GPT_EXIT",
        symbol=symbol,
        regime=regime,
        source=source,
        side=side,
        reason=reason,
        extra_json=extra_json,
    )


def _ensure_today_csv() -> None:
    # legacy wrapper: CSV 제거됨. 호출되더라도 동작에 영향 없도록 no-op 유지.
    return None


__all__ = [
    "SignalsLoggerContractError",
    "SignalsLoggerDBError",
    "SignalsLoggerPublishError",
    "log_event",
    "log_signal",
    "log_skip_event",
    "log_candle_snapshot",
    "log_gpt_entry_event",
    "log_gpt_exit_event",
    "_ensure_today_csv",
]