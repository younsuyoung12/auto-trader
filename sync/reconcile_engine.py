"""
========================================================
FILE: sync/reconcile_engine.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할
--------------------------------------------------------
- "내부 상태"와 "거래소 실제 상태"가 틀어지는(Desync) 상황을 조기에 감지한다.
- run_bot_ws.py 같은 메인 루프에서 주기적으로 호출하는 용도.
- DB 스키마/저장은 건드리지 않는다. (조회 + 판단 + 상위로 신호만)

핵심 원칙 (STRICT)
--------------------------------------------------------
1) 폴백 금지:
   - 거래소/로컬/DB 스냅샷에 필요한 필드가 없으면 즉시 예외.
   - None/빈값을 임의로 0으로 바꾸거나 추정하지 않는다.
2) 단발 mismatch로 즉시 HARD_STOP 금지 (TRADE-GRADE):
   - N회 연속 mismatch일 때만 "확정 desync"로 승격한다.
   - 단발/일시적 지연(거래소 반영/조회 지연)으로 봇이 과민 정지하는 것을 방지한다.
3) 본 모듈은 "판단"만 한다:
   - 주문 실행/청산/DB 저장은 여기서 하지 않는다.
   - 필요 시 on_desync 콜백으로만 알린다.

연동 방식 (최소 변경)
--------------------------------------------------------
- ReconcileEngine에 아래 의존성(콜러블)을 주입한다:
  - fetch_exchange_position(symbol) -> dict
  - get_local_position(symbol) -> dict
  - (선택) fetch_db_open_trade(symbol) -> dict | None
  - (선택) fetch_exchange_open_orders(symbol) -> list[dict]
  - (선택) on_desync(result) -> None  # 여기서 HARD_STOP 트리거

변경 이력
--------------------------------------------------------
- 2026-03-14:
  1) FEAT(STRUCTURE): exchange ↔ local 뿐 아니라 db_open_trade ↔ protection order visibility까지 정합 검사 확장
  2) FEAT(PROTECTION): TP/SL 보호주문 가시성 및 orderId 일치 검증 추가
  3) FEAT(DB): DB OPEN trade side / qty / remaining_qty / entry_price / reconciliation_status 검증 추가
  4) FIX(STRICT): partial fill 상태는 remaining_qty 기준으로 비교하고 qty 원본과의 불일치도 노출
  5) FIX(TRADE-GRADE): 단발 mismatch는 경고, N회 연속 mismatch만 desync_confirmed 유지
- 2026-03-04:
  1) 단발 mismatch 즉시 HARD_STOP 금지: N회 연속 mismatch로 desync 확정(TRADE-GRADE)
  2) 결과에 consecutive_mismatches / desync_confirmed 필드 추가(기존 호환 유지)
  3) 예외 삼키기/None 보정/조용한 무시는 그대로 금지(STRICT 유지)
========================================================
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


# =========================
# Data Models
# =========================

@dataclass(frozen=True)
class ReconcileConfig:
    symbol: str = "BTCUSDT"
    interval_sec: int = 30

    # "포지션이 열려있다"로 판정하는 최소 절대 수량(거래소 positionAmt 기준)
    qty_epsilon: Decimal = Decimal("0.00000001")

    # 수량/가격 오차 허용치
    qty_tolerance: Decimal = Decimal("0.000001")
    price_tolerance: Decimal = Decimal("0.50")

    # TRADE-GRADE: 단발 mismatch는 경고, N회 연속일 때만 확정
    desync_confirm_n: int = 3

    # 추가 STRICT 계약
    require_db_trade_when_open: bool = True
    require_protection_orders_when_open: bool = True
    require_protection_verified_when_open: bool = True


@dataclass(frozen=True)
class ExchangePosition:
    symbol: str
    position_amt: Decimal
    entry_price: Decimal
    raw: Dict[str, Any]


@dataclass(frozen=True)
class LocalPosition:
    symbol: str
    position_amt: Decimal
    entry_price: Decimal
    raw: Dict[str, Any]


@dataclass(frozen=True)
class DbOpenTrade:
    symbol: str
    side: str              # LONG / SHORT
    qty: Decimal
    remaining_qty: Decimal
    entry_price: Decimal
    tp_order_id: str
    sl_order_id: str
    reconciliation_status: str
    raw: Dict[str, Any]


@dataclass(frozen=True)
class ProtectionVisibility:
    open_orders_count: int
    tp_order_ids: Tuple[str, ...]
    sl_order_ids: Tuple[str, ...]


@dataclass(frozen=True)
class ReconcileIssue:
    code: str
    message: str
    details: Dict[str, Any]


@dataclass(frozen=True)
class ReconcileResult:
    ok: bool
    symbol: str
    issues: Tuple[ReconcileIssue, ...]
    exchange: ExchangePosition
    local: LocalPosition
    db_open_trade: Optional[DbOpenTrade] = None
    protection: Optional[ProtectionVisibility] = None
    open_orders_count: Optional[int] = None
    consecutive_mismatches: int = 0
    desync_confirmed: bool = False


# =========================
# Engine
# =========================

class ReconcileEngine:
    """
    단일 심볼 기준 reconcile 엔진.
    - run_bot_ws.py에서 주기적으로 run_if_due() 호출

    TRADE-GRADE:
    - 단발 mismatch(일시적 조회 지연/반영 지연)으로 즉시 HARD_STOP 금지
    - N회 연속 mismatch일 때만 on_desync 콜백 호출
    """

    def __init__(
        self,
        config: ReconcileConfig,
        *,
        fetch_exchange_position: Callable[[str], Dict[str, Any]],
        get_local_position: Callable[[str], Dict[str, Any]],
        fetch_db_open_trade: Optional[Callable[[str], Optional[Dict[str, Any]]]] = None,
        fetch_exchange_open_orders: Optional[Callable[[str], Sequence[Dict[str, Any]]]] = None,
        on_desync: Optional[Callable[[ReconcileResult], None]] = None,
    ) -> None:
        if config.interval_sec < 1:
            raise ValueError("interval_sec must be >= 1")
        if not isinstance(config.desync_confirm_n, int) or config.desync_confirm_n < 1:
            raise ValueError("desync_confirm_n must be int >= 1 (STRICT)")

        self._cfg = config
        self._fetch_exchange_position = fetch_exchange_position
        self._get_local_position = get_local_position
        self._fetch_db_open_trade = fetch_db_open_trade
        self._fetch_exchange_open_orders = fetch_exchange_open_orders
        self._on_desync = on_desync

        self._last_run_ts: float = 0.0
        self._consecutive_mismatches: int = 0

    def run_if_due(self, now_ts: Optional[float] = None) -> Optional[ReconcileResult]:
        ts = now_ts if now_ts is not None else time.time()
        if (ts - self._last_run_ts) < self._cfg.interval_sec:
            return None

        self._last_run_ts = ts

        base = reconcile_once(
            self._cfg,
            fetch_exchange_position=self._fetch_exchange_position,
            get_local_position=self._get_local_position,
            fetch_db_open_trade=self._fetch_db_open_trade,
            fetch_exchange_open_orders=self._fetch_exchange_open_orders,
        )

        if base.ok:
            if self._consecutive_mismatches != 0:
                logger.info(
                    "[RECONCILE] recovered (symbol=%s) consecutive_mismatches=%d -> 0",
                    base.symbol,
                    self._consecutive_mismatches,
                )
            self._consecutive_mismatches = 0
            return replace(base, consecutive_mismatches=0, desync_confirmed=False)

        self._consecutive_mismatches += 1
        confirm_n = int(self._cfg.desync_confirm_n)
        confirmed = self._consecutive_mismatches >= confirm_n

        result = replace(
            base,
            consecutive_mismatches=int(self._consecutive_mismatches),
            desync_confirmed=bool(confirmed),
        )

        logger.error(
            "[RECONCILE] mismatch symbol=%s issues=%d consecutive=%d/%d confirmed=%s",
            result.symbol,
            len(result.issues),
            result.consecutive_mismatches,
            confirm_n,
            result.desync_confirmed,
        )
        for issue in result.issues:
            logger.error("[RECONCILE] %s: %s | %s", issue.code, issue.message, issue.details)

        if confirmed:
            logger.critical("[RECONCILE] DESYNC CONFIRMED (symbol=%s) -> on_desync", result.symbol)
            if self._on_desync is not None:
                self._on_desync(result)
        else:
            logger.warning(
                "[RECONCILE] mismatch not confirmed yet (symbol=%s) - HARD_STOP NOT triggered",
                result.symbol,
            )

        return result


# =========================
# Core reconcile logic
# =========================

def reconcile_once(
    cfg: ReconcileConfig,
    *,
    fetch_exchange_position: Callable[[str], Dict[str, Any]],
    get_local_position: Callable[[str], Dict[str, Any]],
    fetch_db_open_trade: Optional[Callable[[str], Optional[Dict[str, Any]]]] = None,
    fetch_exchange_open_orders: Optional[Callable[[str], Sequence[Dict[str, Any]]]] = None,
) -> ReconcileResult:
    """
    단발 reconcile.
    - 필드 누락/형 변환 실패는 즉시 예외 (NO-FALLBACK)
    """
    symbol = cfg.symbol

    ex_raw = fetch_exchange_position(symbol)
    loc_raw = get_local_position(symbol)

    ex = _parse_exchange_position(symbol, ex_raw)
    loc = _parse_local_position(symbol, loc_raw)

    db_trade: Optional[DbOpenTrade] = None
    if fetch_db_open_trade is not None:
        db_raw = fetch_db_open_trade(symbol)
        if db_raw is not None:
            db_trade = _parse_db_open_trade(symbol, db_raw)

    protection: Optional[ProtectionVisibility] = None
    open_orders_count: Optional[int] = None
    if fetch_exchange_open_orders is not None:
        orders = fetch_exchange_open_orders(symbol)
        if orders is None:
            raise RuntimeError("fetch_exchange_open_orders returned None (STRICT)")
        ol = list(orders)
        for i, row in enumerate(ol):
            if not isinstance(row, dict):
                raise TypeError(f"open_orders[{i}] must be dict (STRICT)")
        protection = _parse_protection_visibility(symbol, ex, ol)
        open_orders_count = int(protection.open_orders_count)

    issues: List[ReconcileIssue] = []

    ex_open = abs(ex.position_amt) > cfg.qty_epsilon
    loc_open = abs(loc.position_amt) > cfg.qty_epsilon
    db_open = db_trade is not None

    # 1) 포지션 유무 불일치 (exchange vs local)
    if ex_open != loc_open:
        issues.append(
            ReconcileIssue(
                code="POS_PRESENCE_MISMATCH",
                message="Exchange position presence != Local position presence",
                details={
                    "exchange_open": ex_open,
                    "local_open": loc_open,
                    "exchange_position_amt": str(ex.position_amt),
                    "local_position_amt": str(loc.position_amt),
                },
            )
        )

    # 2) 거래소 포지션이 열려있으면 DB OPEN trade도 있어야 함
    if ex_open and cfg.require_db_trade_when_open and not db_open:
        issues.append(
            ReconcileIssue(
                code="DB_OPEN_TRADE_MISSING",
                message="Exchange position exists but DB OPEN trade is missing",
                details={
                    "exchange_position_amt": str(ex.position_amt),
                    "exchange_entry_price": str(ex.entry_price),
                },
            )
        )

    # 3) 거래소 포지션이 없는데 DB OPEN trade가 있으면 mismatch
    if not ex_open and db_open:
        issues.append(
            ReconcileIssue(
                code="DB_OPEN_TRADE_UNEXPECTED",
                message="DB OPEN trade exists but exchange position is closed",
                details={
                    "db_side": str(db_trade.side) if db_trade is not None else None,
                    "db_qty": str(db_trade.qty) if db_trade is not None else None,
                    "db_remaining_qty": str(db_trade.remaining_qty) if db_trade is not None else None,
                },
            )
        )

    # 4) 둘 다 열려 있으면 local 상세 비교
    if ex_open and loc_open:
        ex_sign = _sign(ex.position_amt)
        loc_sign = _sign(loc.position_amt)

        if ex_sign != loc_sign:
            issues.append(
                ReconcileIssue(
                    code="POS_SIDE_MISMATCH",
                    message="Exchange position side != Local position side",
                    details={
                        "exchange_position_amt": str(ex.position_amt),
                        "local_position_amt": str(loc.position_amt),
                    },
                )
            )

        if abs(ex.position_amt - loc.position_amt) > cfg.qty_tolerance:
            issues.append(
                ReconcileIssue(
                    code="POS_QTY_MISMATCH",
                    message="Exchange position quantity != Local position quantity (beyond tolerance)",
                    details={
                        "exchange_position_amt": str(ex.position_amt),
                        "local_position_amt": str(loc.position_amt),
                        "qty_tolerance": str(cfg.qty_tolerance),
                    },
                )
            )

        if abs(ex.entry_price - loc.entry_price) > cfg.price_tolerance:
            issues.append(
                ReconcileIssue(
                    code="ENTRY_PRICE_MISMATCH",
                    message="Exchange entryPrice != Local entry_price (beyond tolerance)",
                    details={
                        "exchange_entry_price": str(ex.entry_price),
                        "local_entry_price": str(loc.entry_price),
                        "price_tolerance": str(cfg.price_tolerance),
                    },
                )
            )

    # 5) DB OPEN trade 상세 비교
    if ex_open and db_trade is not None:
        ex_dir = _position_dir_from_amt(ex.position_amt)

        if db_trade.side != ex_dir:
            issues.append(
                ReconcileIssue(
                    code="DB_SIDE_MISMATCH",
                    message="Exchange position side != DB trade side",
                    details={
                        "exchange_direction": ex_dir,
                        "db_side": db_trade.side,
                    },
                )
            )

        if abs(db_trade.remaining_qty - abs(ex.position_amt)) > cfg.qty_tolerance:
            issues.append(
                ReconcileIssue(
                    code="DB_REMAINING_QTY_MISMATCH",
                    message="Exchange position abs qty != DB remaining_qty (beyond tolerance)",
                    details={
                        "exchange_abs_qty": str(abs(ex.position_amt)),
                        "db_remaining_qty": str(db_trade.remaining_qty),
                        "qty_tolerance": str(cfg.qty_tolerance),
                    },
                )
            )

        if db_trade.remaining_qty - db_trade.qty > cfg.qty_tolerance:
            issues.append(
                ReconcileIssue(
                    code="DB_REMAINING_GT_ORIGINAL_QTY",
                    message="DB remaining_qty exceeds original qty",
                    details={
                        "db_qty": str(db_trade.qty),
                        "db_remaining_qty": str(db_trade.remaining_qty),
                    },
                )
            )

        if abs(db_trade.entry_price - ex.entry_price) > cfg.price_tolerance:
            issues.append(
                ReconcileIssue(
                    code="DB_ENTRY_PRICE_MISMATCH",
                    message="Exchange entryPrice != DB entry_price (beyond tolerance)",
                    details={
                        "exchange_entry_price": str(ex.entry_price),
                        "db_entry_price": str(db_trade.entry_price),
                        "price_tolerance": str(cfg.price_tolerance),
                    },
                )
            )

        if cfg.require_protection_verified_when_open and db_trade.reconciliation_status != "PROTECTION_VERIFIED":
            issues.append(
                ReconcileIssue(
                    code="DB_RECON_STATUS_MISMATCH",
                    message="DB reconciliation_status is not PROTECTION_VERIFIED while exchange position is open",
                    details={
                        "db_reconciliation_status": db_trade.reconciliation_status,
                    },
                )
            )

    # 6) 보호주문 visibility 비교
    if ex_open and cfg.require_protection_orders_when_open:
        if protection is None:
            issues.append(
                ReconcileIssue(
                    code="PROTECTION_VISIBILITY_UNAVAILABLE",
                    message="Exchange open orders callback is missing while protection visibility is required",
                    details={},
                )
            )
        else:
            if len(protection.tp_order_ids) != 1:
                issues.append(
                    ReconcileIssue(
                        code="TP_VISIBILITY_MISMATCH",
                        message="TP protective order visibility must be exactly 1",
                        details={
                            "tp_order_ids": list(protection.tp_order_ids),
                            "open_orders_count": protection.open_orders_count,
                        },
                    )
                )

            if len(protection.sl_order_ids) != 1:
                issues.append(
                    ReconcileIssue(
                        code="SL_VISIBILITY_MISMATCH",
                        message="SL protective order visibility must be exactly 1",
                        details={
                            "sl_order_ids": list(protection.sl_order_ids),
                            "open_orders_count": protection.open_orders_count,
                        },
                    )
                )

            if db_trade is not None:
                if db_trade.tp_order_id not in protection.tp_order_ids:
                    issues.append(
                        ReconcileIssue(
                            code="TP_ORDER_ID_NOT_VISIBLE",
                            message="DB tp_order_id is not visible on exchange open orders",
                            details={
                                "db_tp_order_id": db_trade.tp_order_id,
                                "exchange_tp_order_ids": list(protection.tp_order_ids),
                            },
                        )
                    )

                if db_trade.sl_order_id not in protection.sl_order_ids:
                    issues.append(
                        ReconcileIssue(
                            code="SL_ORDER_ID_NOT_VISIBLE",
                            message="DB sl_order_id is not visible on exchange open orders",
                            details={
                                "db_sl_order_id": db_trade.sl_order_id,
                                "exchange_sl_order_ids": list(protection.sl_order_ids),
                            },
                        )
                    )

    ok = len(issues) == 0
    return ReconcileResult(
        ok=ok,
        symbol=symbol,
        issues=tuple(issues),
        exchange=ex,
        local=loc,
        db_open_trade=db_trade,
        protection=protection,
        open_orders_count=open_orders_count,
    )


# =========================
# Parsers (STRICT)
# =========================

def _parse_exchange_position(symbol: str, raw: Dict[str, Any]) -> ExchangePosition:
    """
    기대 필드(STRICT):
    - symbol
    - positionAmt
    - entryPrice
    """
    if raw is None:
        raise ValueError("exchange position raw is None (STRICT)")
    if not isinstance(raw, dict):
        raise TypeError("exchange position raw must be dict (STRICT)")

    raw_symbol = raw.get("symbol")
    if raw_symbol != symbol:
        raise ValueError(f"exchange symbol mismatch: expected={symbol} got={raw_symbol}")

    position_amt = _to_decimal_strict(raw, "positionAmt")
    entry_price = _to_decimal_strict(raw, "entryPrice")

    return ExchangePosition(
        symbol=symbol,
        position_amt=position_amt,
        entry_price=entry_price,
        raw=raw,
    )


def _parse_local_position(symbol: str, raw: Dict[str, Any]) -> LocalPosition:
    """
    로컬 스냅샷 기대 필드(STRICT):
    - symbol
    - position_amt
    - entry_price
    """
    if raw is None:
        raise ValueError("local position raw is None (STRICT)")
    if not isinstance(raw, dict):
        raise TypeError("local position raw must be dict (STRICT)")

    raw_symbol = raw.get("symbol")
    if raw_symbol != symbol:
        raise ValueError(f"local symbol mismatch: expected={symbol} got={raw_symbol}")

    position_amt = _to_decimal_strict(raw, "position_amt")
    entry_price = _to_decimal_strict(raw, "entry_price")

    return LocalPosition(
        symbol=symbol,
        position_amt=position_amt,
        entry_price=entry_price,
        raw=raw,
    )


def _parse_db_open_trade(symbol: str, raw: Dict[str, Any]) -> DbOpenTrade:
    """
    DB OPEN trade 스냅샷 기대 필드(STRICT):
    - symbol
    - side               # BUY/SELL/LONG/SHORT 허용 -> LONG/SHORT 정규화
    - qty
    - remaining_qty
    - entry_price
    - tp_order_id
    - sl_order_id
    - reconciliation_status
    """
    if raw is None:
        raise ValueError("db open trade raw is None (STRICT)")
    if not isinstance(raw, dict):
        raise TypeError("db open trade raw must be dict (STRICT)")

    raw_symbol = raw.get("symbol")
    if raw_symbol != symbol:
        raise ValueError(f"db open trade symbol mismatch: expected={symbol} got={raw_symbol}")

    side = _normalize_db_side_strict(raw.get("side"))
    qty = _to_decimal_strict(raw, "qty")
    remaining_qty = _to_decimal_strict(raw, "remaining_qty")
    entry_price = _to_decimal_strict(raw, "entry_price")

    if qty <= 0:
        raise ValueError("db qty must be > 0 (STRICT)")
    if remaining_qty <= 0:
        raise ValueError("db remaining_qty must be > 0 (STRICT)")
    if entry_price <= 0:
        raise ValueError("db entry_price must be > 0 (STRICT)")

    tp_order_id = _to_nonempty_strict(raw, "tp_order_id")
    sl_order_id = _to_nonempty_strict(raw, "sl_order_id")
    if tp_order_id == sl_order_id:
        raise ValueError("db tp_order_id and sl_order_id must be distinct (STRICT)")

    reconciliation_status = _to_nonempty_strict(raw, "reconciliation_status").upper()

    return DbOpenTrade(
        symbol=symbol,
        side=side,
        qty=qty,
        remaining_qty=remaining_qty,
        entry_price=entry_price,
        tp_order_id=tp_order_id,
        sl_order_id=sl_order_id,
        reconciliation_status=reconciliation_status,
        raw=raw,
    )


def _parse_protection_visibility(
    symbol: str,
    exchange: ExchangePosition,
    open_orders: Sequence[Dict[str, Any]],
) -> ProtectionVisibility:
    tp_ids: List[str] = []
    sl_ids: List[str] = []

    exit_side = _exit_side_from_amt(exchange.position_amt)

    for i, row in enumerate(open_orders):
        if not isinstance(row, dict):
            raise TypeError(f"open_orders[{i}] must be dict (STRICT)")

        row_symbol = row.get("symbol")
        if row_symbol != symbol:
            continue

        row_side = _to_nonempty_strict(row, "side").upper()
        if row_side != exit_side:
            continue

        if not _is_protective_order_strict(row):
            continue

        order_id = _to_nonempty_strict(row, "orderId")
        order_type = _to_nonempty_strict(row, "type").upper()

        if order_type in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT"):
            tp_ids.append(order_id)
        elif order_type in ("STOP_MARKET", "STOP"):
            sl_ids.append(order_id)

    return ProtectionVisibility(
        open_orders_count=len(list(open_orders)),
        tp_order_ids=tuple(tp_ids),
        sl_order_ids=tuple(sl_ids),
    )


# =========================
# Primitive helpers
# =========================

def _to_decimal_strict(d: Dict[str, Any], key: str) -> Decimal:
    if key not in d:
        raise KeyError(f"missing required field: {key} (STRICT)")
    v = d[key]
    if v is None:
        raise ValueError(f"field {key} is None (STRICT)")
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError) as e:
        raise ValueError(f"field {key} not convertible to Decimal (STRICT): {v}") from e


def _to_nonempty_strict(d: Dict[str, Any], key: str) -> str:
    if key not in d:
        raise KeyError(f"missing required field: {key} (STRICT)")
    v = d[key]
    if v is None:
        raise ValueError(f"field {key} is None (STRICT)")
    s = str(v).strip()
    if not s:
        raise ValueError(f"field {key} empty (STRICT)")
    return s


def _normalize_db_side_strict(v: Any) -> str:
    s = str(v).strip().upper()
    if s in ("BUY", "LONG"):
        return "LONG"
    if s in ("SELL", "SHORT"):
        return "SHORT"
    raise ValueError(f"db side invalid (STRICT): {v!r}")


def _is_protective_order_strict(row: Dict[str, Any]) -> bool:
    reduce_only = row.get("reduceOnly")
    close_position = row.get("closePosition")

    if not isinstance(reduce_only, bool):
        raise TypeError("open order reduceOnly must be bool (STRICT)")
    if not isinstance(close_position, bool):
        raise TypeError("open order closePosition must be bool (STRICT)")

    return bool(reduce_only or close_position)


def _position_dir_from_amt(x: Decimal) -> str:
    if x > 0:
        return "LONG"
    if x < 0:
        return "SHORT"
    raise ValueError("position amount is zero (STRICT)")


def _exit_side_from_amt(x: Decimal) -> str:
    if x > 0:
        return "SELL"
    if x < 0:
        return "BUY"
    raise ValueError("position amount is zero (STRICT)")


def _sign(x: Decimal) -> int:
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0


__all__ = [
    "ReconcileConfig",
    "ExchangePosition",
    "LocalPosition",
    "DbOpenTrade",
    "ProtectionVisibility",
    "ReconcileIssue",
    "ReconcileResult",
    "ReconcileEngine",
    "reconcile_once",
]