# common/strict_validators.py
"""
========================================================
FILE: common/strict_validators.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

목적
- 프로젝트 전역에서 중복으로 생기기 쉬운 "STRICT 검증 프리미티브"를 단일화한다.
- None→0/빈값 폴백, 예외 삼키기, 조용한 기본값 적용을 금지한다.
- 모든 검증 실패는 예외로 중단한다(호출 측에서 명시적 SKIP로 처리할지 결정).

설계 원칙
- 입력이 불완전/모호하면 즉시 예외 (폴백 금지)
- bool은 숫자로 취급하지 않는다
- NaN/INF 금지
- tz-aware datetime만 허용 (필요 시)
- 외부 의존성 없음

변경 이력
--------------------------------------------------------
- 2026-03-03:
  1) 프로젝트 공통 STRICT 검증 모듈 신규 도입
  2) require_* / as_* 계열 유틸 제공(숫자/문자열/컨테이너/시간)
  3) 중복 검증 로직을 이 모듈로 흡수할 기반 마련
========================================================
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, TypeVar


class StrictValidationError(RuntimeError):
    """STRICT: 입력 검증 실패(폴백 없이 즉시 중단)."""


T = TypeVar("T")


# ─────────────────────────────────────────────
# Primitive checks
# ─────────────────────────────────────────────
def require_nonempty_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        raise StrictValidationError(f"{name} is required (STRICT)")
    return s


def require_bool(v: Any, name: str) -> bool:
    """
    STRICT bool 파서.
    - bool이면 그대로 반환
    - 문자열/숫자 유사값을 엄격 파싱
    - 그 외는 예외
    """
    if isinstance(v, bool):
        return v
    if v is None:
        raise StrictValidationError(f"{name} is required (bool) (STRICT)")

    s = str(v).strip().lower()
    if s in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise StrictValidationError(f"{name} must be bool-like (STRICT): {v!r}")


def require_int(
    v: Any,
    name: str,
    *,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
) -> int:
    if isinstance(v, bool):
        raise StrictValidationError(f"{name} must be int (bool not allowed) (STRICT)")
    try:
        iv = int(v)
    except Exception as e:
        raise StrictValidationError(f"{name} must be int (STRICT)") from e

    if min_value is not None and iv < min_value:
        raise StrictValidationError(f"{name} must be >= {min_value} (STRICT)")
    if max_value is not None and iv > max_value:
        raise StrictValidationError(f"{name} must be <= {max_value} (STRICT)")
    return iv


def require_int_ms(v: Any, name: str) -> int:
    """
    STRICT epoch milliseconds.
    - 반드시 int > 0
    """
    iv = require_int(v, name, min_value=1)
    return iv


def require_float(
    v: Any,
    name: str,
    *,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> float:
    if isinstance(v, bool):
        raise StrictValidationError(f"{name} must be float (bool not allowed) (STRICT)")
    try:
        fv = float(v)
    except Exception as e:
        raise StrictValidationError(f"{name} must be float (STRICT)") from e

    if not math.isfinite(fv):
        raise StrictValidationError(f"{name} must be finite (STRICT): {fv!r}")

    if min_value is not None and fv < min_value:
        raise StrictValidationError(f"{name} must be >= {min_value} (STRICT)")
    if max_value is not None and fv > max_value:
        raise StrictValidationError(f"{name} must be <= {max_value} (STRICT)")
    return float(fv)


def opt_float(
    v: Any,
    name: str,
    *,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> Optional[float]:
    if v is None:
        return None
    return require_float(v, name, min_value=min_value, max_value=max_value)


def require_choice(v: Any, name: str, allowed: Sequence[str]) -> str:
    s = require_nonempty_str(v, name).upper()
    allowed_u = {a.upper() for a in allowed}
    if s not in allowed_u:
        raise StrictValidationError(f"{name} must be one of {sorted(allowed_u)} (STRICT): {s!r}")
    return s


# ─────────────────────────────────────────────
# Container checks
# ─────────────────────────────────────────────
def require_dict(v: Any, name: str, *, non_empty: bool = False) -> Dict[str, Any]:
    if not isinstance(v, dict):
        raise StrictValidationError(f"{name} must be dict (STRICT)")
    if non_empty and not v:
        raise StrictValidationError(f"{name} must be non-empty dict (STRICT)")
    return v


def require_mapping(v: Any, name: str, *, non_empty: bool = False) -> Mapping[str, Any]:
    if not isinstance(v, Mapping):
        raise StrictValidationError(f"{name} must be mapping (STRICT)")
    if non_empty and not dict(v):
        raise StrictValidationError(f"{name} must be non-empty mapping (STRICT)")
    return v


def require_list(v: Any, name: str, *, non_empty: bool = False) -> List[Any]:
    if not isinstance(v, list):
        raise StrictValidationError(f"{name} must be list (STRICT)")
    if non_empty and not v:
        raise StrictValidationError(f"{name} must be non-empty list (STRICT)")
    return v


def require_sequence(v: Any, name: str, *, non_empty: bool = False) -> Sequence[Any]:
    if not isinstance(v, (list, tuple)):
        raise StrictValidationError(f"{name} must be list/tuple (STRICT)")
    if non_empty and len(v) == 0:
        raise StrictValidationError(f"{name} must be non-empty sequence (STRICT)")
    return v


def require_keys(d: Mapping[str, Any], name: str, keys: Sequence[str]) -> None:
    missing = [k for k in keys if k not in d]
    if missing:
        raise StrictValidationError(f"{name} missing keys (STRICT): {missing}")


# ─────────────────────────────────────────────
# Date/Time checks
# ─────────────────────────────────────────────
def require_tz_aware_datetime(v: Any, name: str) -> datetime:
    if not isinstance(v, datetime):
        raise StrictValidationError(f"{name} must be datetime (STRICT)")
    if v.tzinfo is None or v.tzinfo.utcoffset(v) is None:
        raise StrictValidationError(f"{name} must be tz-aware datetime (STRICT)")
    return v


# ─────────────────────────────────────────────
# Domain helpers
# ─────────────────────────────────────────────
def normalize_symbol(symbol: Any, *, name: str = "symbol") -> str:
    """
    STRICT:
    - 빈 문자열 금지
    - 공백/구분자(-,/,_) 제거 후 대문자
    """
    s = require_nonempty_str(symbol, name)
    out = s.upper().replace("-", "").replace("/", "").replace("_", "").strip()
    if not out:
        raise StrictValidationError(f"{name} normalized empty (STRICT)")
    if any(ch.isspace() for ch in out):
        raise StrictValidationError(f"{name} contains whitespace (STRICT): {out!r}")
    return out


def pick_first_key(meta: Mapping[str, Any], keys: Tuple[str, ...]) -> Any:
    """
    STRICT-friendly pick:
    - 0/False 값이 '없음'으로 오해되지 않도록 key 존재 여부로만 선택.
    - 없으면 None 반환(폴백 값 주입 금지).
    """
    for k in keys:
        if k in meta:
            return meta[k]
    return None


# ─────────────────────────────────────────────
# Lightweight structured error context (optional)
# ─────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class StrictErrorContext:
    where: str
    detail: str


__all__ = [
    "StrictValidationError",
    "StrictErrorContext",
    "require_nonempty_str",
    "require_bool",
    "require_int",
    "require_int_ms",
    "require_float",
    "opt_float",
    "require_choice",
    "require_dict",
    "require_mapping",
    "require_list",
    "require_sequence",
    "require_keys",
    "require_tz_aware_datetime",
    "normalize_symbol",
    "pick_first_key",
]