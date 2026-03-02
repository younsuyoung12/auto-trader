# ============================================================
# execution/exceptions.py
# STRICT · NO-FALLBACK · PRODUCTION MODE
# ============================================================
# 역할:
#   - 엔진 전역 예외 타입을 단일 모듈로 고정한다.
#   - 레이어별(리스크/주문/동기화/상태) 오류를 명확히 분리한다.
#
# 설계 원칙:
#   - 예외를 삼키지 않는다. (catch 해서 무시/보정/폴백 금지)
#   - 모호하면 즉시 예외로 중단한다. (STRICT)
#
# CHANGELOG
# 2026-03-02
# - 엔진 예외 타입 표준화 모듈 신규 도입
# ============================================================

from __future__ import annotations


class EngineError(RuntimeError):
    """엔진 전역 베이스 예외."""


class StateViolation(EngineError):
    """상태 머신/상태 전이 규칙 위반."""


class RiskRejected(EngineError):
    """리스크 가드에 의해 시그널이 거절됨(정상 흐름일 수 있음)."""


class OrderFailed(EngineError):
    """주문 실행 실패(거래소 응답 이상, 거절, 미체결/부분체결 정책 위반 등)."""


class SyncMismatch(EngineError):
    """거래소 상태와 내부 상태/DB 스냅샷 정합 불일치."""


__all__ = [
    "EngineError",
    "StateViolation",
    "RiskRejected",
    "OrderFailed",
    "SyncMismatch",
]
