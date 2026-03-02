# sync/reconcile_engine.py
"""
========================================================
sync/reconcile_engine.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================

역할
--------------------------------------------------------
- "내부 상태"와 "거래소 실제 상태"가 틀어지는(Desync) 상황을 조기에 감지한다.
- run_bot_ws.py 같은 메인 루프에서 30초마다 호출하는 용도.
- DB 스키마/저장은 건드리지 않는다. (조회 + 판단 + 상위로 신호만)

핵심 원칙 (STRICT)
--------------------------------------------------------
1) 폴백 금지:
   - 거래소/로컬 스냅샷에 필요한 필드가 없으면 즉시 예외.
   - None/빈값을 임의로 0으로 바꾸거나 추정하지 않는다.
2) Desync는 "넘어가면" 안 된다:
   - 불일치 감지 시 상위(리스크 엔진)가 HARD_STOP 또는 강제 정리 정책을 수행해야 한다.
3) 본 모듈은 "판단"만 한다:
   - 주문 실행/청산/DB 저장은 여기서 하지 않는다.
   - 필요 시 on_desync 콜백으로만 알린다.

연동 방식 (최소 변경)
--------------------------------------------------------
- ReconcileEngine에 아래 의존성(콜러블)을 주입한다:
  - fetch_exchange_position(symbol) -> dict
  - get_local_position(symbol) -> dict
  - (선택) fetch_exchange_open_orders(symbol) -> list[dict]
  - (선택) on_desync(result) -> None  # 여기서 HARD_STOP 트리거

PATCH NOTES — 2026-03-02
--------------------------------------------------------
- Initial implementation: strict reconcile for single-symbol futures bot.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
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

    # 수량/가격 오차 허용치 (거래소/로컬 표현 차이를 최소 허용)
    qty_tolerance: Decimal = Decimal("0.000001")
    price_tolerance: Decimal = Decimal("0.50")  # BTC entryPrice 오차 0.5달러까지 허용 (필요 시 조정)


@dataclass(frozen=True)
class ExchangePosition:
    symbol: str
    position_amt: Decimal         # Binance Futures: positionAmt (양수=롱, 음수=숏)
    entry_price: Decimal          # entryPrice
    raw: Dict[str, Any]


@dataclass(frozen=True)
class LocalPosition:
    symbol: str
    position_amt: Decimal         # 내부 포지션 수량(롱=양수, 숏=음수, 없음=0)
    entry_price: Decimal          # 내부 엔트리 가격(없으면 0이 아니라 "필드 누락"으로 예외)
    raw: Dict[str, Any]


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
    open_orders_count: Optional[int] = None


# =========================
# Engine
# =========================

class ReconcileEngine:
    """
    단일 심볼(BTCUSDT) 기준 최소 reconcile 엔진.
    - run_bot_ws.py에서 주기적으로 run_if_due() 호출
    """

    def __init__(
        self,
        config: ReconcileConfig,
        *,
        fetch_exchange_position: Callable[[str], Dict[str, Any]],
        get_local_position: Callable[[str], Dict[str, Any]],
        fetch_exchange_open_orders: Optional[Callable[[str], Sequence[Dict[str, Any]]]] = None,
        on_desync: Optional[Callable[[ReconcileResult], None]] = None,
    ) -> None:
        if config.interval_sec < 1:
            raise ValueError("interval_sec must be >= 1")

        self._cfg = config
        self._fetch_exchange_position = fetch_exchange_position
        self._get_local_position = get_local_position
        self._fetch_exchange_open_orders = fetch_exchange_open_orders
        self._on_desync = on_desync

        self._last_run_ts: float = 0.0

    def run_if_due(self, now_ts: Optional[float] = None) -> Optional[ReconcileResult]:
        """
        주기 조건이 되면 reconcile 수행 후 결과 반환.
        - 실행 시점이 아니면 None 반환.
        """
        ts = now_ts if now_ts is not None else time.time()
        if (ts - self._last_run_ts) < self._cfg.interval_sec:
            return None

        self._last_run_ts = ts
        result = reconcile_once(
            self._cfg,
            fetch_exchange_position=self._fetch_exchange_position,
            get_local_position=self._get_local_position,
            fetch_exchange_open_orders=self._fetch_exchange_open_orders,
        )

        if not result.ok:
            logger.error(
                "[RECONCILE] DESYNC symbol=%s issues=%d",
                result.symbol,
                len(result.issues),
            )
            for issue in result.issues:
                logger.error("[RECONCILE] %s: %s | %s", issue.code, issue.message, issue.details)

            if self._on_desync is not None:
                # 상위(리스크 엔진)에서 HARD_STOP / 강제정리 정책 수행
                self._on_desync(result)

        return result


# =========================
# Core reconcile logic
# =========================

def reconcile_once(
    cfg: ReconcileConfig,
    *,
    fetch_exchange_position: Callable[[str], Dict[str, Any]],
    get_local_position: Callable[[str], Dict[str, Any]],
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

    open_orders_count: Optional[int] = None
    if fetch_exchange_open_orders is not None:
        orders = fetch_exchange_open_orders(symbol)
        if orders is None:
            raise RuntimeError("fetch_exchange_open_orders returned None (STRICT)")
        open_orders_count = len(list(orders))

    issues: List[ReconcileIssue] = []

    ex_open = abs(ex.position_amt) > cfg.qty_epsilon
    loc_open = abs(loc.position_amt) > cfg.qty_epsilon

    # 1) 포지션 유무 불일치
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

    # 2) 둘 다 열려있다면 방향(롱/숏) 일치 확인
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

        # 3) 수량 일치(허용 오차)
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

        # 4) 엔트리 가격 일치(허용 오차)
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

    ok = len(issues) == 0
    return ReconcileResult(
        ok=ok,
        symbol=symbol,
        issues=tuple(issues),
        exchange=ex,
        local=loc,
        open_orders_count=open_orders_count,
    )


# =========================
# Parsers (STRICT)
# =========================

def _parse_exchange_position(symbol: str, raw: Dict[str, Any]) -> ExchangePosition:
    """
    기대 필드(STRICT):
    - symbol
    - positionAmt  (string/number)
    - entryPrice   (string/number)
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
    - position_amt   (string/number)  # 롱=양수, 숏=음수, 없음=0
    - entry_price    (string/number)  # 포지션 없으면 0이 아니라 "필드 존재"가 중요. (없으면 예외)
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


def _sign(x: Decimal) -> int:
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0