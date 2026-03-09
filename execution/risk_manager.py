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
- 트레이딩 엔진(AWS) → DB/대시보드(Render) 원격 환경 전제.
- DB 접근은 state.db_core.get_session() SSOT만 사용한다.
========================================================

변경 이력
--------------------------------------------------------
2026-03-09
- FIX(ROOT-CAUSE): 포지션 가치 상한을 "신규 주문 notional"이 아니라
  "현재 포지션 반영 projected 총 익스포저" 기준으로 계산하도록 변경
- FIX(STRICT): DB 접근을 SessionLocal 직접 사용에서 get_session() SSOT로 변경
- ADD(TRADE-GRADE): 현재 거래소 포지션 방향/수량/진입가 기반 projected exposure 메타 기록
- FIX(RISK-MODEL): 반대 방향 신규 주문은 단순 차단이 아니라 순노출(net exposure) 기준으로 평가
- 기존 기능 삭제 없음

2026-03-02
- STRICT 파싱 강화: 거래소/DB 응답에서 필수 필드 누락 시 즉시 예외(폴백 제거)
- symbol/side 정규화 및 입력 검증 강화
- 선택(옵션) 하드 가드 추가:
  - hard_total_loss_limit_usdt: 누적(전체) 실현 손실 한도
  - hard_daily_loss_limit_pct_of_available: available_usdt 기준 일일 손실 % 한도
  - hard_consecutive_losses_lookback: 연속 손실 계산 조회 범위
  - hard_open_unrealized_loss_limit_usdt: 보유 포지션 미실현 손실 한도(물타기 방지)
========================================================
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import func, select

from execution.exchange_api import get_position
from state.db_core import get_session
from state.db_models import Trade as TradeORM

logger = logging.getLogger(__name__)
KST = timezone(timedelta(hours=9))


# ─────────────────────────────────────────────────────────────
# Strict helpers (NO-FALLBACK)
# ─────────────────────────────────────────────────────────────
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


def _normalize_symbol(symbol: str) -> str:
    s = str(symbol).replace("-", "").replace("/", "").upper().strip()
    if not s:
        raise ValueError("symbol is empty")
    return s


def _normalize_side(side: str) -> str:
    s = str(side or "").strip().upper()
    if s in {"LONG", "SHORT", "BUY", "SELL"}:
        return s
    raise ValueError("side must be one of LONG/SHORT/BUY/SELL")


def _normalize_trade_direction(side: str) -> str:
    s = _normalize_side(side)
    if s in {"LONG", "BUY"}:
        return "LONG"
    if s in {"SHORT", "SELL"}:
        return "SHORT"
    raise ValueError(f"invalid trade side: {side!r}")


def _req_float_from_dict(d: Dict[str, Any], key: str) -> float:
    if key not in d:
        raise RuntimeError(f"exchange position missing required field: {key}")
    val = d.get(key)
    if val is None:
        raise RuntimeError(f"exchange position field is null: {key}")
    try:
        f = float(val)
    except Exception as e:
        raise RuntimeError(f"exchange position field not a number: {key}") from e
    if not math.isfinite(f):
        raise RuntimeError(f"exchange position field not finite: {key}")
    return f


def _req_float_from_dict_multi(d: Dict[str, Any], keys: Tuple[str, ...], *, name: str) -> float:
    # "키 후보" 중 하나는 반드시 존재해야 한다. (폴백 추정 금지)
    for k in keys:
        if k in d and d.get(k) is not None:
            try:
                f = float(d.get(k))
            except Exception as e:
                raise RuntimeError(f"exchange position field not a number: {k}") from e
            if not math.isfinite(f):
                raise RuntimeError(f"exchange position field not finite: {k}")
            return f
    raise RuntimeError(f"exchange position missing required field for {name}: {list(keys)}")


def _validate_hard_risk_settings(settings: Any) -> None:
    # 기존 필수 하드 리스크 필드 (호환 유지)
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
    consec_raw = getattr(settings, "hard_consecutive_losses_limit")
    try:
        consec = int(consec_raw)
    except Exception as e:
        raise ValueError("hard_consecutive_losses_limit must be int") from e

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

    # 선택(옵션) 필드: 존재하면 엄격 검증
    if hasattr(settings, "hard_total_loss_limit_usdt"):
        _as_float(getattr(settings, "hard_total_loss_limit_usdt"), "hard_total_loss_limit_usdt", min_value=0.0)

    if hasattr(settings, "hard_daily_loss_limit_pct_of_available"):
        pct = _as_float(
            getattr(settings, "hard_daily_loss_limit_pct_of_available"),
            "hard_daily_loss_limit_pct_of_available",
            min_value=0.0,
        )
        if not (0.0 <= pct <= 100.0):
            raise ValueError("hard_daily_loss_limit_pct_of_available must be in [0,100]")

    if hasattr(settings, "hard_consecutive_losses_lookback"):
        lb_raw = getattr(settings, "hard_consecutive_losses_lookback")
        try:
            lb = int(lb_raw)
        except Exception as e:
            raise ValueError("hard_consecutive_losses_lookback must be int") from e
        if lb <= 0:
            raise ValueError("hard_consecutive_losses_lookback must be > 0")

    if hasattr(settings, "hard_open_unrealized_loss_limit_usdt"):
        _as_float(
            getattr(settings, "hard_open_unrealized_loss_limit_usdt"),
            "hard_open_unrealized_loss_limit_usdt",
            min_value=0.0,
        )


def _kst_day_start_utc() -> datetime:
    now_kst = datetime.now(tz=KST)
    start_kst = datetime(now_kst.year, now_kst.month, now_kst.day, tzinfo=KST)
    return start_kst.astimezone(timezone.utc)


def _calc_projected_abs_notional_strict(
    *,
    current_direction: Optional[str],
    current_abs_notional: float,
    incoming_direction: str,
    incoming_notional: float,
) -> Tuple[float, str]:
    """
    One-way 기준 projected 총 익스포저 계산.

    - 현재 포지션 없음: incoming_notional
    - 같은 방향 추가 진입: current + incoming
    - 반대 방향: 순노출(net) 기준 abs(current - incoming)

    Returns:
      (projected_abs_notional, mode)
    """
    if current_abs_notional < 0:
        raise ValueError("current_abs_notional must be >= 0")
    if incoming_notional <= 0:
        raise ValueError("incoming_notional must be > 0")

    incoming_dir = _normalize_trade_direction(incoming_direction)

    if current_direction is None or current_abs_notional == 0.0:
        return float(incoming_notional), "fresh_entry"

    current_dir = _normalize_trade_direction(current_direction)

    if current_dir == incoming_dir:
        return float(current_abs_notional + incoming_notional), "same_direction_add"

    if incoming_notional >= current_abs_notional:
        return float(incoming_notional - current_abs_notional), "opposite_direction_flip_or_reduce"

    return float(current_abs_notional - incoming_notional), "opposite_direction_reduce"


# ─────────────────────────────────────────────────────────────
# DB queries (STRICT)
# ─────────────────────────────────────────────────────────────
def _get_today_realized_pnl_usdt(symbol: str) -> float:
    """
    오늘(KST) 기준 실현 PnL 합계.
    - bt_trades.exit_ts가 오늘(KST)에 속하는 레코드만 합산
    - pnl_usdt NULL 제외
    """
    start_utc = _kst_day_start_utc()
    end_utc = start_utc + timedelta(days=1)

    try:
        with get_session() as session:
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


def _get_total_realized_pnl_usdt(symbol: str) -> float:
    """
    전체(누적) 실현 PnL 합계.
    - bt_trades.exit_ts가 존재하는 레코드 합산
    - pnl_usdt NULL 제외
    """
    try:
        with get_session() as session:
            stmt = (
                select(func.coalesce(func.sum(TradeORM.pnl_usdt), 0))
                .where(TradeORM.symbol == symbol)
                .where(TradeORM.exit_ts.isnot(None))
                .where(TradeORM.pnl_usdt.isnot(None))
            )
            result = session.execute(stmt).scalar_one()
            return float(result)
    except Exception as e:
        raise RuntimeError(f"total pnl query failed: {e}") from e


def _get_consecutive_losses(symbol: str, *, lookback: int = 50) -> int:
    """
    최근 종료 트레이드 기준 연속 손실 횟수.
    """
    if lookback <= 0:
        raise ValueError("lookback must be > 0")

    try:
        with get_session() as session:
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


# ─────────────────────────────────────────────────────────────
# Exchange position snapshot (STRICT)
# ─────────────────────────────────────────────────────────────
def _get_position_snapshot(symbol: str) -> Dict[str, Any]:
    pos = get_position(symbol)
    if not isinstance(pos, dict):
        raise RuntimeError("exchange_api.get_position() returned non-dict")
    return pos


# ─────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────
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
    - (옵션) 누적(전체) 실현 손실 한도
    - 일일 손실 한도(USDT)
    - (옵션) available_usdt 기준 일일 손실 % 한도
    - 연속 손실 한도
    - 계좌 대비 projected 총 포지션 가치 상한
    - 청산거리 최소 % (현재 포지션이 있는 경우에만 liquidationPrice 기반 체크)
    - (옵션) 보유 포지션 미실현 손실 한도(물타기 방지)

    Returns:
        (ok, reason, extra)
    """
    _validate_hard_risk_settings(settings)

    symbol_n = _normalize_symbol(symbol)
    side_n = _normalize_trade_direction(side)

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
        "symbol": symbol_n,
        "side": side_n,
        "entry_price": entry_price_f,
        "incoming_notional": notional_f,
        "available_usdt": available_f,
    }

    # [0] (옵션) 누적(전체) 실현 손실 한도
    total_loss_limit = float(getattr(settings, "hard_total_loss_limit_usdt", 0.0) or 0.0)
    if total_loss_limit > 0:
        pnl_total = _get_total_realized_pnl_usdt(symbol_n)
        extra["pnl_total_usdt"] = pnl_total
        extra["hard_total_loss_limit_usdt"] = total_loss_limit
        if pnl_total <= -total_loss_limit:
            return False, "hard_total_loss_limit_exceeded", extra

    # [1] 일일 손실 한도(USDT)
    daily_limit = float(getattr(settings, "hard_daily_loss_limit_usdt"))
    pnl_today = _get_today_realized_pnl_usdt(symbol_n)
    extra["pnl_today_usdt"] = pnl_today
    extra["hard_daily_loss_limit_usdt"] = daily_limit
    if daily_limit > 0 and pnl_today <= -daily_limit:
        return False, "hard_daily_loss_limit_exceeded", extra

    # [1b] (옵션) available_usdt 기준 일일 손실 % 한도
    daily_pct = float(getattr(settings, "hard_daily_loss_limit_pct_of_available", 0.0) or 0.0)
    if daily_pct > 0:
        derived_limit = available_f * (daily_pct / 100.0)
        extra["hard_daily_loss_limit_pct_of_available"] = daily_pct
        extra["hard_daily_loss_limit_usdt_derived"] = derived_limit
        if derived_limit > 0 and pnl_today <= -derived_limit:
            return False, "hard_daily_loss_limit_pct_exceeded", extra

    # [2] 연속 손실 한도
    consec_limit = int(getattr(settings, "hard_consecutive_losses_limit"))
    if consec_limit > 0:
        lookback = int(getattr(settings, "hard_consecutive_losses_lookback", 50) or 50)
        if lookback <= 0:
            raise ValueError("hard_consecutive_losses_lookback must be > 0")

        consec_losses = _get_consecutive_losses(symbol_n, lookback=lookback)
        extra["consecutive_losses"] = consec_losses
        extra["hard_consecutive_losses_limit"] = consec_limit
        extra["hard_consecutive_losses_lookback"] = lookback
        if consec_losses >= consec_limit:
            return False, "hard_consecutive_losses_limit_exceeded", extra

    # [3] 계좌 대비 projected 총 포지션 가치 상한
    pct_cap = float(getattr(settings, "hard_position_value_pct_cap"))
    extra["hard_position_value_pct_cap"] = pct_cap

    pos = _get_position_snapshot(symbol_n)

    pos_amt = _req_float_from_dict(pos, "positionAmt")
    extra["positionAmt"] = pos_amt

    current_direction: Optional[str] = None
    current_entry_price: Optional[float] = None
    current_abs_notional = 0.0

    if abs(pos_amt) > 0.0:
        current_direction = "LONG" if pos_amt > 0 else "SHORT"
        current_entry_price = _req_float_from_dict_multi(
            pos,
            ("entryPrice",),
            name="entry_price",
        )
        if current_entry_price <= 0:
            raise RuntimeError("entryPrice must be > 0 when positionAmt != 0")
        current_abs_notional = abs(pos_amt) * current_entry_price

    extra["current_position_direction"] = current_direction
    extra["current_position_entry_price"] = current_entry_price
    extra["current_position_abs_notional"] = current_abs_notional

    projected_abs_notional, projected_mode = _calc_projected_abs_notional_strict(
        current_direction=current_direction,
        current_abs_notional=float(current_abs_notional),
        incoming_direction=side_n,
        incoming_notional=float(notional_f),
    )
    extra["projected_abs_notional"] = projected_abs_notional
    extra["projected_exposure_mode"] = projected_mode

    max_notional = available_f * (pct_cap / 100.0)
    extra["max_notional_by_cap"] = max_notional
    if projected_abs_notional > max_notional:
        return False, "hard_position_value_pct_cap_exceeded", extra

    # [4] 청산거리 최소 % + [5] (옵션) 보유 포지션 미실현 손실 한도
    liq_min_pct = float(getattr(settings, "hard_liquidation_distance_pct_min"))
    open_unrealized_limit = float(getattr(settings, "hard_open_unrealized_loss_limit_usdt", 0.0) or 0.0)

    if liq_min_pct > 0 or open_unrealized_limit > 0:
        # [5] (옵션) 보유 포지션 미실현 손실 한도(물타기/추가진입 방지)
        if open_unrealized_limit > 0 and abs(pos_amt) > 0:
            unrl = _req_float_from_dict_multi(
                pos,
                ("unRealizedProfit", "unrealizedProfit"),
                name="unrealized_profit",
            )
            extra["unrealized_profit_usdt"] = unrl
            extra["hard_open_unrealized_loss_limit_usdt"] = open_unrealized_limit
            if unrl <= -open_unrealized_limit:
                return False, "hard_open_unrealized_loss_limit_exceeded", extra

        # [4] 청산거리 최소 % (포지션이 있을 때만 적용)
        extra["hard_liquidation_distance_pct_min"] = liq_min_pct
        if liq_min_pct > 0 and abs(pos_amt) > 0:
            liq_price = _req_float_from_dict(pos, "liquidationPrice")
            if liq_price <= 0:
                raise RuntimeError("liquidationPrice must be > 0 when positionAmt != 0")
            extra["liquidationPrice"] = liq_price

            # live 포지션이 있으면 거래소 live entryPrice 기준으로 평가
            liq_ref_price = float(current_entry_price) if current_entry_price is not None else entry_price_f
            if liq_ref_price <= 0:
                raise RuntimeError("liquidation reference price must be > 0 (STRICT)")
            extra["liquidation_distance_reference_price"] = liq_ref_price

            dist_pct = abs(liq_ref_price - liq_price) / liq_ref_price * 100.0
            extra["liquidation_distance_pct"] = dist_pct
            if dist_pct < liq_min_pct:
                return False, "hard_liquidation_distance_too_close", extra

    return True, "OK", extra