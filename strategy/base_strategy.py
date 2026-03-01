"""
========================================================
strategy/base_strategy.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
핵심 원칙:
- 이 파일은 "전략(판단) 레이어"의 인터페이스(계약)만 정의한다.
- 폴백(REST 백필/더미 값/임의 보정/None 반환/예외 삼키기) 절대 금지.
- market_data가 비어있거나 필수 키가 누락되면 즉시 RuntimeError 또는 ValueError.
- Strategy 구현체는 decide()에서 반드시 Signal을 반환해야 한다.
- Strategy 레이어는 아래를 절대 하면 안 된다:
  - exchange_api import / 주문 실행 / 포지션 조회
  - DB 접근 / 세션 생성
  - 실행 엔진(execution) 호출
- 이 파일 자체는 어떤 try/except ImportError(임포트 폴백)도 두지 않는다.
========================================================
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict

from strategy.signal import Signal


class BaseStrategy(ABC):
    """
    전략(판단) 레이어 공통 인터페이스.

    구현 규칙(STRICT):
    - decide(market_data)는 market_data가 부족하면 즉시 예외를 발생시킨다.
    - decide는 None을 반환하지 않는다.
    - decide는 반드시 Signal 객체를 반환한다.
    - 이 인터페이스를 구현하는 클래스는 '판단'만 한다(주문/DB/거래소 호출 금지).
    """

    @abstractmethod
    def decide(self, market_data: Dict[str, Any]) -> Signal:
        """
        Args:
            market_data: 오케스트레이터(run_bot_ws)에서 생성된 입력 dict.
                         (예: symbol, direction, last_price, signal_ts_ms, market_features 등)

        Returns:
            Signal: action/direction/tp_pct/sl_pct/risk_pct/reason/guard_adjustments/meta

        Raises:
            RuntimeError / ValueError:
                - market_data가 비어있거나 dict가 아니면 RuntimeError
                - 필수 키 누락/타입 오류/값 오류가 있으면 RuntimeError 또는 ValueError
                - 폴백 로직 없이 즉시 실패해야 한다.
        """
        raise NotImplementedError