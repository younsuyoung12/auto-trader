# execution/order_state.py
"""
========================================================
execution/order_state.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================

역할
--------------------------------------------------------
- 주문의 라이프사이클 상태를 명시적으로 표현한다.
- 실행 엔진(execution_engine.py)이 "현재 주문이 어디까지 진행됐는지"를
  애매하게 추정하지 않도록, 표준 상태 집합을 제공한다.

핵심 원칙 (STRICT)
--------------------------------------------------------
- 상태는 "추정"하지 않는다.
  - 거래소 응답/조회로 확인 가능한 사실만으로 상태를 전이한다.
- 불명확/모순 상태는 즉시 예외(또는 상위에서 HARD_STOP)로 처리한다.
- 상태 전이는 실행 엔진이 책임진다. (여기는 Enum만 제공)

PATCH NOTES — 2026-03-02
--------------------------------------------------------
- Initial implementation: OrderState enum for minimal FSM.
"""

from __future__ import annotations

from enum import Enum, unique


@unique
class OrderState(Enum):
    """
    최소 주문 FSM 상태.

    CREATED:
      - 내부적으로 주문 객체를 만들었으나 거래소로 아직 제출하지 않음

    SUBMITTED:
      - 거래소에 주문 요청을 보냈고, 최소한 orderId/클라이언트 주문ID를 확보함

    PARTIALLY_FILLED:
      - 일부 수량이 체결됨 (부분 체결)

    FILLED:
      - 전체 수량 체결 완료

    CANCELED:
      - 주문 취소 완료(거래소에서 취소 확인)

    REJECTED:
      - 거래소가 주문을 거부/실패 처리(파라미터/잔고/필터/권한 등)
    """

    CREATED = "CREATED"
    SUBMITTED = "SUBMITTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"