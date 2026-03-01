"""
========================================================
execution/risk_manager.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
설계 원칙:
- 하드 리스크 가드만 담당한다.
- 폴백(REST 백필/더미 값/임의 보정) 절대 금지.
- 설정/입력 값이 비정상이면 즉시 예외(ValueError/RuntimeError).
- DB 조회 실패는 즉시 예외(운영 로그에 남게).
========================================================
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import func, select

from execution.exchange_api import get_position
from state.db_core import SessionLocal
from state.db_models import Trade as TradeORM

logger = logging.getLogger(__name__)
KST = timezone(timedelta(hours=9))


def _as_float(value: Any, name: str, *, min_value: Optional[float] = None) -> float:
    try:
        if isinstance(value, bool):
            raise TypeError("bool is not allowed")
        v = float(value)
    except Exception as e:
        raise ValueError(f"{name} must be a number") from e

    if not math.isfinite(v):
        raise ValueError(f"{name} must be finite")

    if min_value is not None and v < min_value:
        raise ValueError(f"{name} must be >= {min_value}")

    return v


def _validate_hard_risk_settings(settings: Any) -> None:
    required = [
        "hard_daily_loss_limit_usdt",
        "hard_consecutive_losses_limit",
        "hard_position_value_pct_cap",
        "hard_liquidation_distance_pct_min",
    ]
    missing = [k for k in required if not hasattr(settings, k)]
    if missing:
        raise ValueError(f"settings missing hard risk fields: {missing}")

    daily = _as_float(getattr(settings, "hard_daily_loss_limit_usdt"), "hard_daily_loss_limit_usdt", min_value=0.0)
    consec = int(getattr(settings, "hard_consecutive_losses_limit"))
    pct_cap = _as_float(getattr(settings, "hard_position_value_pct_cap"), "hard_position_value_pct_cap", min_value=0.0)
    liq = _as_float(getattr(settings, "hard_liquidation_distance_pct_min"), "hard_liquidation_distance_pct_min", min_value=0.0)

    if daily < 0:
        raise ValueError("hard_daily_loss_limit_usdt must be >= 0")
    if consec < 0:
        raise ValueError("hard_consecutive_losses_limit must be >= 0")
    if not (0.0 <= pct_cap <= 100.0):
        raise ValueError("hard_position_value_pct_cap must be in [0,100]")
    if not (0.0 <= liq <= 100.0):
        raise ValueError("hard_liquidation_distance_pct_min must be in [0,100]")


def _kst_day_start_utc() -> datetime:
    now_kst = datetime.now(tz=KST)
    start_kst = datetime(now_kst.year, now_kst.month, now_kst.day, tzinfo=KST)
    return start_kst.astimezone(timezone.utc)


def _safe_float_from_dict(d: Dict[str, Any], key: str) -> Optional[float]:
    if key not in d:
        return None
    val = d.get(key)
    if val is None:
        return None
    try:
        f = float(val)
    except Exception:
        return None
    if not math.isfinite(f):
        return None
    return f


def _get_today_realized_pnl_usdt(symbol: str) -> float:
    """
    오늘(KST) 기준 실현 PnL 합계.
    - bt_trades.exit_ts가 오늘(KST)에 속하는 레코드만 합산
    - pnl_usdt NULL 제외
    """
    start_utc = _kst_day_start_utc()
    end_utc = start_utc + timedelta(days=1)

    try:
        session = SessionLocal()
    except Exception as e:
        raise RuntimeError(f"DB SessionLocal create failed: {e}") from e

    try:
        stmt = (
            select(func.coalesce(func.sum(TradeORM.pnl_usdt), 0))
            .where(TradeORM.symbol == symbol)
            .where(TradeORM.exit_ts.isnot(None))
            .where(TradeORM.pnl_usdt.isnot(None))
            .where(TradeORM.exit_ts >= start_utc)
            .where(TradeORM.exit_ts < end_utc)
        )
        result = session.execute(stmt).scalar_one()
        return float(result)
    except Exception as e:
        raise RuntimeError(f"daily pnl query failed: {e}") from e
    finally:
        session.close()


def _get_consecutive_losses(symbol: str, *, lookback: int = 50) -> int:
    """
    최근 종료 트레이드 기준 연속 손실 횟수.
    """
    if lookback <= 0:
        raise ValueError("lookback must be > 0")

    try:
        session = SessionLocal()
    except Exception as e:
        raise RuntimeError(f"DB SessionLocal create failed: {e}") from e

    try:
        stmt = (
            select(TradeORM.pnl_usdt)
            .where(TradeORM.symbol == symbol)
            .where(TradeORM.exit_ts.isnot(None))
            .where(TradeORM.pnl_usdt.isnot(None))
            .order_by(TradeORM.exit_ts.desc())
            .limit(int(lookback))
        )
        rows = session.execute(stmt).all()
        cnt = 0
        for (pnl_val,) in rows:
            pnl = float(pnl_val)
            if pnl < 0:
                cnt += 1
            else:
                break
        return cnt
    except Exception as e:
        raise RuntimeError(f"consecutive losses query failed: {e}") from e
    finally:
        session.close()


def hard_risk_guard_check(
    settings: Any,
    *,
    symbol: str,
    side: str,
    entry_price: float,
    notional: float,
    available_usdt: float,
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    문서 기준 하드 리스크 가드.

    Checks:
    - 일일 손실 한도
    - 연속 손실 한도
    - 계좌 대비 포지션 가치 상한
    - 청산거리 최소 % (현재 포지션이 있는 경우에만 liquidationPrice 기반 체크)

    Returns:
        (ok, reason, extra)
    """
    _validate_hard_risk_settings(settings)

    entry_price_f = _as_float(entry_price, "entry_price", min_value=0.0)
    notional_f = _as_float(notional, "notional", min_value=0.0)
    available_f = _as_float(available_usdt, "available_usdt", min_value=0.0)

    if entry_price_f <= 0:
        raise ValueError("entry_price must be > 0")
    if notional_f <= 0:
        raise ValueError("notional must be > 0")
    if available_f <= 0:
        raise ValueError("available_usdt must be > 0")

    extra: Dict[str, Any] = {
        "symbol": symbol,
        "side": side,
        "entry_price": entry_price_f,
        "notional": notional_f,
        "available_usdt": available_f,
    }

    # [1] 일일 손실 한도
    daily_limit = float(getattr(settings, "hard_daily_loss_limit_usdt"))
    if daily_limit > 0:
        pnl_today = _get_today_realized_pnl_usdt(symbol)
        extra["pnl_today_usdt"] = pnl_today
        extra["hard_daily_loss_limit_usdt"] = daily_limit
        if pnl_today <= -daily_limit:
            return False, "hard_daily_loss_limit_exceeded", extra

    # [2] 연속 손실 한도
    consec_limit = int(getattr(settings, "hard_consecutive_losses_limit"))
    if consec_limit > 0:
        consec_losses = _get_consecutive_losses(symbol)
        extra["consecutive_losses"] = consec_losses
        extra["hard_consecutive_losses_limit"] = consec_limit
        if consec_losses >= consec_limit:
            return False, "hard_consecutive_losses_limit_exceeded", extra

    # [3] 계좌 대비 포지션 가치 상한
    pct_cap = float(getattr(settings, "hard_position_value_pct_cap"))
    max_notional = available_f * (pct_cap / 100.0)
    extra["hard_position_value_pct_cap"] = pct_cap
    extra["max_notional_by_cap"] = max_notional
    if notional_f > max_notional:
        return False, "hard_position_value_pct_cap_exceeded", extra

    # [4] 청산거리 최소 %
    liq_min_pct = float(getattr(settings, "hard_liquidation_distance_pct_min"))
    if liq_min_pct > 0:
        pos = get_position(symbol)
        if not isinstance(pos, dict):
            raise RuntimeError("exchange_api.get_position() returned non-dict")

        pos_amt = _safe_float_from_dict(pos, "positionAmt")
        liq_price = _safe_float_from_dict(pos, "liquidationPrice")

        extra["positionAmt"] = pos_amt
        extra["liquidationPrice"] = liq_price
        extra["hard_liquidation_distance_pct_min"] = liq_min_pct

        # 포지션이 없는 경우(0)에는 liquidation guard 적용 불가 → 신규 진입은 허용
        if pos_amt is not None and abs(pos_amt) > 0:
            if liq_price is None or liq_price <= 0:
                raise RuntimeError("liquidationPrice unavailable while positionAmt != 0")

            dist_pct = abs(entry_price_f - liq_price) / entry_price_f * 100.0
            extra["liquidation_distance_pct"] = dist_pct
            if dist_pct < liq_min_pct:
                return False, "hard_liquidation_distance_too_close", extra

    return True, "OK", extra