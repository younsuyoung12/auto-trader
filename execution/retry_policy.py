"""
========================================================
execution/retry_policy.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
역할:
- 네트워크/거래소 호출 재시도 정책
- 실패 시 예외 그대로 raise
- 폴백 금지
========================================================
"""

from __future__ import annotations

import time
from typing import Callable, TypeVar

T = TypeVar("T")


def execute_with_retry(
    fn: Callable[[], T],
    *,
    max_retries: int = 1,
    retry_delay_sec: float = 0.2,
) -> T:
    """
    STRICT:
    - fn 실패 시 max_retries 만큼 재시도
    - 마지막 실패는 그대로 raise
    """

    last_err = None

    for attempt in range(max_retries + 1):
        try:
            return fn()
        except Exception as e:
            last_err = e
            if attempt >= max_retries:
                raise
            time.sleep(retry_delay_sec)

    # 이 줄은 논리적으로 도달 불가
    raise last_err  # type: ignore