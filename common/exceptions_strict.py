# common/exceptions_strict.py
"""
========================================================
FILE: common/exceptions_strict.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

목적
- 프로젝트 전역에서 사용하는 "STRICT 예외 계층"을 단일화한다.
- 레이어별(설정/데이터/DB/상태/외부 I/O) 실패를 명확히 구분한다.
- 예외는 삼키지 않는다. (이 파일은 예외 정의만 제공)

설계 원칙
- 폴백 금지: 누락/불일치/모호성은 예외로 중단
- 호출부는 예외를 그대로 전파하거나, 명시적 SKIP로 기록 후 종료/차단한다.

변경 이력
--------------------------------------------------------
- 2026-03-03:
  1) 프로젝트 공통 STRICT 예외 계층 신규 도입
  2) 영역별 예외 클래스 제공 (Config/Data/DB/State/External/Security)
  3) 운영 로그/이벤트에서 원인 분류가 가능하도록 code 필드 지원
========================================================
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True, slots=True)
class StrictErrorInfo:
    """
    STRICT 예외에 부가 정보를 붙일 때 사용하는 구조체.

    - code: 운영 분류용 짧은 코드 (예: "DB_WRITE_FAIL", "MISSING_FIELD")
    - where: 발생 위치(모듈/함수/레이어)
    - detail: 사람이 읽을 수 있는 짧은 설명(민감정보 금지)
    - extra: 추가 컨텍스트(민감정보 금지, dict/str/num 등만 권장)
    """
    code: str
    where: str
    detail: str
    extra: Optional[Any] = None


class StrictError(RuntimeError):
    """
    STRICT 최상위 예외.

    - info: 운영 분류/추적을 위한 StrictErrorInfo(선택)
    - 기본 메시지는 민감정보를 포함하지 않는다.
    """

    def __init__(self, message: str, *, info: Optional[StrictErrorInfo] = None):
        super().__init__(message)
        self.info: Optional[StrictErrorInfo] = info


# ─────────────────────────────────────────────
# 영역별 예외 (TRADE-GRADE 분류)
# ─────────────────────────────────────────────
class StrictConfigError(StrictError):
    """설정/환경/SSOT 위반 (예: settings 누락, 금지된 env 직접 접근 등)."""


class StrictDataError(StrictError):
    """데이터 무결성 위반 (예: 필수 필드 누락, 타입/범위/정합성 실패)."""


class StrictDBError(StrictError):
    """DB 접근/트랜잭션/쓰기 실패 (예: commit 실패, 스키마 불일치 등)."""


class StrictStateError(StrictError):
    """상태 머신/동시성/순서 위반 (예: 레이스, 불가능한 상태 전이)."""


class StrictExternalError(StrictError):
    """외부 시스템 호출 실패 (거래소/네트워크/서드파티 API 등)."""


class StrictSecurityError(StrictError):
    """보안/민감정보 정책 위반 (예: 시크릿 로그 노출 시도 등)."""


# ─────────────────────────────────────────────
# 편의 생성 함수 (선택적)
# ─────────────────────────────────────────────
def make_info(code: str, where: str, detail: str, extra: Optional[Any] = None) -> StrictErrorInfo:
    """
    STRICT: 에러 정보 생성 헬퍼.
    - 민감정보(키/토큰/DB URL 등)는 extra/detail에 넣지 않는다.
    """
    c = str(code or "").strip()
    w = str(where or "").strip()
    d = str(detail or "").strip()
    if not c or not w or not d:
        raise ValueError("StrictErrorInfo fields must be non-empty (STRICT)")
    return StrictErrorInfo(code=c, where=w, detail=d, extra=extra)


__all__ = [
    "StrictErrorInfo",
    "StrictError",
    "StrictConfigError",
    "StrictDataError",
    "StrictDBError",
    "StrictStateError",
    "StrictExternalError",
    "StrictSecurityError",
    "make_info",
]