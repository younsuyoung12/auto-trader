"""
========================================================
strategy/signal.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
설계 원칙:
- 전략 레이어는 반드시 Signal 객체만 반환한다.
- 주문 실행 / 거래소 API 호출 / DB 접근은 절대 금지.
- 데이터 누락/오류는 즉시 예외(ValueError/RuntimeError).
- 폴백(REST 백필/더미 값/임의 보정) 절대 금지.
========================================================
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass(frozen=True)
class Signal:
    # required fields
    action: str  # "ENTER" | "SKIP"
    direction: str  # "LONG" | "SHORT"
    tp_pct: float
    sl_pct: float
    risk_pct: float
    reason: str
    guard_adjustments: Dict[str, float]

    # execution context (engine에서 필요한 값들)
    # - strategy는 생성만 하고, execution에서 소비한다.
    meta: Dict[str, Any] = field(default_factory=dict)