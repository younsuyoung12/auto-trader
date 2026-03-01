"""
========================================================
strategy/regime_engine.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
역할
--------------------------------------------------------
- Regime Score(0~100)를 "시장 상태 점수"로 받아
  (a) 히스토리(rolling window)를 축적하고
  (b) 절대 기준으로 allocation(0.0~1.0)을 산출한다.
- 이 모듈은 "시장 레짐 기반 포지션 배분"만 담당한다.
  (DD/연속손실 기반 감쇠, 리스크 물리 보정, 주문 실행)은 다른 레이어에서 수행한다.

핵심 원칙 (공동 규칙)
--------------------------------------------------------
- 절대 폴백 금지:
  - 점수 누락/비정상/범위 이탈 시 즉시 예외.
  - 히스토리 미준비 상태에서 퍼센타일 계산 금지(즉시 예외).
- 설정/상태 추정 금지:
  - 값 클램프(임의 보정) 금지. (0~100 밖이면 예외)
- 외부 I/O 금지:
  - DB/거래소/네트워크 접근 금지. (caller가 데이터를 공급)
- 민감정보 로그 금지.

운영 정책 (확정) — Trade-grade (현 점수분포 반영)
--------------------------------------------------------
- 절대 기준 allocation (레버리지 1배 기준)
  - score < 35  -> allocation=0.0 (진입 금지)
  - 35~50       -> allocation=0.4
  - 50~65       -> allocation=0.7
  - >= 65       -> allocation=1.0 (전액)
- 퍼센타일 기반 배분은 "옵션"이며, 히스토리 준비가 필수다.

PATCH NOTES
--------------------------------------------------------
- 2026-03-02
  - RegimeEngine 도입(rolling window + 절대 기준 allocation).
  - 퍼센타일/스냅샷 API 제공(히스토리 준비 강제).
- 2026-03-02 (Trade-grade tuning)
  - 절대 기준을 65/75/85 → 35/50/65로 조정.
    이유: 엔진 total score의 실측 분포(20~40대 다수)에서
    NO_TRADE 고착을 방지하고, 전액 배분은 >=65에서만 허용.
========================================================
"""

from __future__ import annotations

import math
import threading
from dataclasses import dataclass
from typing import Deque, List, Optional, Tuple
from collections import deque


# ─────────────────────────────────────────────
# Exceptions (STRICT)
# ─────────────────────────────────────────────
class RegimeEngineError(RuntimeError):
    """Base error for regime engine."""


class RegimeScoreError(RegimeEngineError):
    """Invalid score input."""


class RegimeNotReadyError(RegimeEngineError):
    """Raised when history-dependent operation is requested before readiness."""


# ─────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class RegimeDecision:
    score: float
    allocation: float
    band: str
    history_len: int
    window_size: int
    percentile: Optional[float] = None  # only when explicitly computed


# ─────────────────────────────────────────────
# Helpers (STRICT)
# ─────────────────────────────────────────────
def _as_score(value: object, *, name: str = "regime_score") -> float:
    """
    STRICT:
    - bool 금지
    - finite 숫자만 허용
    - 0~100 범위 강제(클램프 금지)
    """
    if isinstance(value, bool):
        raise RegimeScoreError(f"{name} must be numeric (bool not allowed)")
    try:
        v = float(value)  # type: ignore[arg-type]
    except Exception as e:
        raise RegimeScoreError(f"{name} must be a number") from e

    if not math.isfinite(v):
        raise RegimeScoreError(f"{name} must be finite")

    if v < 0.0 or v > 100.0:
        raise RegimeScoreError(f"{name} out of range [0,100]: {v}")

    return v


def _band_and_allocation_by_absolute_threshold(score: float) -> Tuple[str, float]:
    """
    확정 정책(절대 기준) — Trade-grade tuning:
    - < 35  : NO_TRADE, 0.0
    - 35~50 : LOW,      0.4
    - 50~65 : MID,      0.7
    - >=65  : HIGH,     1.0
    """
    if score < 35.0:
        return "NO_TRADE", 0.0
    if score < 50.0:
        return "LOW", 0.4
    if score < 65.0:
        return "MID", 0.7
    return "HIGH", 1.0


# ─────────────────────────────────────────────
# Regime Engine
# ─────────────────────────────────────────────
class RegimeEngine:
    """
    STRICT · NO-FALLBACK

    사용법(권장):
    1) 매 루프마다 update(score)로 히스토리를 축적한다. (신호 유무와 무관)
    2) decide(score)로 절대 기준 allocation을 즉시 얻는다. (히스토리 불필요)
    3) 퍼센타일 기반 판단이 필요하면:
       - is_ready_for_percentile() 확인 후
       - percentile(score) 호출
    """

    def __init__(
        self,
        *,
        window_size: int = 200,
        percentile_min_history: int = 60,
    ) -> None:
        if not isinstance(window_size, int) or window_size <= 0:
            raise ValueError("window_size must be positive int")
        if not isinstance(percentile_min_history, int) or percentile_min_history <= 0:
            raise ValueError("percentile_min_history must be positive int")
        if percentile_min_history > window_size:
            raise ValueError("percentile_min_history must be <= window_size")

        self._window_size: int = window_size
        self._percentile_min_history: int = percentile_min_history

        self._lock = threading.Lock()
        self._scores: Deque[float] = deque(maxlen=window_size)

    # ── properties ─────────────────────────────
    @property
    def window_size(self) -> int:
        return self._window_size

    @property
    def history_len(self) -> int:
        with self._lock:
            return len(self._scores)

    # ── update / snapshot ──────────────────────
    def update(self, score: object) -> float:
        """
        히스토리 업데이트.

        STRICT:
        - 점수 유효성(0~100, finite) 강제
        - 실패 시 예외(폴백/무시 금지)
        """
        v = _as_score(score, name="regime_score")
        with self._lock:
            self._scores.append(v)
        return v

    def snapshot_scores(self) -> List[float]:
        """현재 히스토리를 복사해서 반환(정렬 아님)."""
        with self._lock:
            return list(self._scores)

    # ── absolute threshold decision ────────────
    def decide(self, score: object) -> RegimeDecision:
        """
        절대 기준 allocation 판단(히스토리 불필요).

        STRICT:
        - score 유효성 강제
        - allocation은 정책에 따른 결정값(폴백 없음)
        """
        v = _as_score(score, name="regime_score")
        band, alloc = _band_and_allocation_by_absolute_threshold(v)

        with self._lock:
            hlen = len(self._scores)

        return RegimeDecision(
            score=v,
            allocation=float(alloc),
            band=band,
            history_len=hlen,
            window_size=self._window_size,
            percentile=None,
        )

    # ── percentile (optional) ───────────────────
    def is_ready_for_percentile(self) -> bool:
        """
        퍼센타일 계산 준비 여부.
        STRICT:
        - 준비 안 됐으면 percentile()은 예외를 던진다.
        """
        with self._lock:
            return len(self._scores) >= self._percentile_min_history

    def percentile(self, score: object) -> float:
        """
        rolling percentile 계산 (0~100).

        STRICT:
        - 히스토리 부족 시 예외(폴백 금지)
        - score 유효성 강제
        """
        v = _as_score(score, name="regime_score")

        with self._lock:
            if len(self._scores) < self._percentile_min_history:
                raise RegimeNotReadyError(
                    f"percentile requires >= {self._percentile_min_history} history points "
                    f"(current={len(self._scores)})"
                )
            data = list(self._scores)

        le_count = 0
        for x in data:
            if x <= v:
                le_count += 1

        p = (le_count / len(data)) * 100.0
        if p < 0.0 or p > 100.0 or not math.isfinite(p):
            raise RegimeEngineError(f"percentile computed invalid: {p}")

        return p

    def decide_with_percentile(self, score: object) -> RegimeDecision:
        """
        절대 기준 decision + percentile 포함 스냅샷.
        STRICT:
        - percentile 준비 안 됐으면 예외.
        """
        d = self.decide(score)
        p = self.percentile(d.score)
        return RegimeDecision(
            score=d.score,
            allocation=d.allocation,
            band=d.band,
            history_len=d.history_len,
            window_size=d.window_size,
            percentile=p,
        )