from __future__ import annotations

"""
========================================================
FILE: strategy/regime_engine.py
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

추가 정책 — Microstructure Penalty (TRADE-GRADE)
--------------------------------------------------------
- Microstructure(왜곡/과열) 위험도는 micro_score_risk(0~100)로 입력받아
  allocation에 곱셈 페널티로 반영한다.
- 본 모듈은 micro_score_risk를 "결정"하지 않으며 caller가 제공해야 한다.
- 페널티는 명시 정책이며 폴백/클램프가 아니다.

  micro_score_risk < 40   -> multiplier = 1.0
  40 <= < 60              -> multiplier = 0.8
  60 <= < 80              -> multiplier = 0.5
  >= 80                   -> multiplier = 0.0 (진입 금지급 과열)

추가 정책 — Absolute Threshold Hysteresis (TRADE-GRADE)
--------------------------------------------------------
- 경계 점수(35/50/65) 근처에서 band/allocation 이 한 틱마다 흔들리는 것을 줄이기 위해
  명시적 hysteresis margin 을 적용한다.
- 이는 폴백이 아니라 "상태 전이 정책"이다.
- 기본 margin = 2.0 score points

  예시:
  - LOW -> MID 승격: score >= 52
  - MID -> LOW 강등: score < 48
  - MID -> HIGH 승격: score >= 67
  - HIGH -> MID 강등: score < 63

PATCH NOTES
--------------------------------------------------------
- 2026-03-10
  - FIX(ROOT-CAUSE): typing import 계약 누락 수정
    * snapshot_decision_state() 추가 후 Dict import 누락으로 Pylance reportUndefinedVariable 발생
    * typing import 를 현재 annotation 사용과 정합화
  - 기존 기능 삭제 없음
- 2026-03-09
  - TRADE-GRADE: absolute threshold hysteresis 추가
    * decide() 가 이전 band 상태를 기준으로 경계 흔들림을 완화
    * entry/no-entry 및 allocation 경계 출렁임 감소
  - decide_state snapshot API 추가
- 2026-03-03
  - TRADE-GRADE: micro_score_risk 기반 allocation 페널티 반영 API 추가(STRICT)
    * decide_with_microstructure(...)
    * decide_with_percentile_and_microstructure(...)
- 2026-03-02
  - RegimeEngine 도입(rolling window + 절대 기준 allocation).
  - 퍼센타일/스냅샷 API 제공(히스토리 준비 강제).
- 2026-03-02 (Trade-grade tuning)
  - 절대 기준을 65/75/85 → 35/50/65로 조정.
========================================================
"""

import math
import threading
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional, Tuple


# ─────────────────────────────────────────────
# Exceptions (STRICT)
# ─────────────────────────────────────────────
class RegimeEngineError(RuntimeError):
    """Base error for regime engine."""


class RegimeScoreError(RegimeEngineError):
    """Invalid score input."""


class RegimeNotReadyError(RegimeEngineError):
    """Raised when history-dependent operation is requested before readiness."""


class MicrostructureScoreError(RegimeEngineError):
    """Invalid microstructure score input."""


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

    # TRADE-GRADE: microstructure overlay (optional)
    micro_score_risk: Optional[float] = None
    micro_multiplier: Optional[float] = None


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


def _as_micro_score(value: object, *, name: str = "micro_score_risk") -> float:
    """
    STRICT:
    - bool 금지
    - finite 숫자만 허용
    - 0~100 범위 강제(클램프 금지)
    """
    if isinstance(value, bool):
        raise MicrostructureScoreError(f"{name} must be numeric (bool not allowed)")
    try:
        v = float(value)  # type: ignore[arg-type]
    except Exception as e:
        raise MicrostructureScoreError(f"{name} must be a number") from e

    if not math.isfinite(v):
        raise MicrostructureScoreError(f"{name} must be finite")

    if v < 0.0 or v > 100.0:
        raise MicrostructureScoreError(f"{name} out of range [0,100]: {v}")

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


def _allocation_by_band_strict(band: str) -> float:
    b = str(band).strip().upper()
    if b == "NO_TRADE":
        return 0.0
    if b == "LOW":
        return 0.4
    if b == "MID":
        return 0.7
    if b == "HIGH":
        return 1.0
    raise RegimeEngineError(f"unknown regime band: {band!r}")


def _band_with_hysteresis(score: float, prev_band: Optional[str], margin: float) -> str:
    """
    STRICT:
    - 이전 band 가 있으면 경계 전이를 hysteresis margin 기준으로 판단
    - 이전 band 가 없으면 절대 기준 그대로 사용
    """
    raw_band, _ = _band_and_allocation_by_absolute_threshold(score)
    if prev_band is None:
        return raw_band

    b = str(prev_band).strip().upper()
    if b not in {"NO_TRADE", "LOW", "MID", "HIGH"}:
        raise RegimeEngineError(f"invalid previous regime band: {prev_band!r}")

    if margin <= 0.0 or not math.isfinite(margin):
        raise RegimeEngineError(f"hysteresis margin invalid: {margin}")

    # 경계: 35 / 50 / 65
    if b == "NO_TRADE":
        if score >= 35.0 + margin:
            return "LOW"
        return "NO_TRADE"

    if b == "LOW":
        if score < 35.0 - margin:
            return "NO_TRADE"
        if score >= 50.0 + margin:
            return "MID"
        return "LOW"

    if b == "MID":
        if score < 50.0 - margin:
            return "LOW"
        if score >= 65.0 + margin:
            return "HIGH"
        return "MID"

    # HIGH
    if score < 65.0 - margin:
        return "MID"
    return "HIGH"


def _micro_multiplier(micro_score_risk: float) -> float:
    """
    확정 정책(명시 룰): micro_score_risk(0~100) -> allocation multiplier.
    """
    if micro_score_risk < 40.0:
        return 1.0
    if micro_score_risk < 60.0:
        return 0.8
    if micro_score_risk < 80.0:
        return 0.5
    return 0.0


# ─────────────────────────────────────────────
# Regime Engine
# ─────────────────────────────────────────────
class RegimeEngine:
    """
    STRICT · NO-FALLBACK

    사용법(권장):
    1) 매 루프마다 update(score)로 히스토리를 축적한다. (신호 유무와 무관)
    2) decide(score)로 절대 기준 allocation을 즉시 얻는다. (히스토리 불필요)
    3) microstructure를 반영하려면 decide_with_microstructure(score, micro_score_risk)
    4) 퍼센타일 기반 판단이 필요하면:
       - is_ready_for_percentile() 확인 후
       - percentile(score) 호출
    """

    def __init__(
        self,
        *,
        window_size: int = 200,
        percentile_min_history: int = 60,
        hysteresis_margin: float = 2.0,
    ) -> None:
        if not isinstance(window_size, int) or window_size <= 0:
            raise ValueError("window_size must be positive int")
        if not isinstance(percentile_min_history, int) or percentile_min_history <= 0:
            raise ValueError("percentile_min_history must be positive int")
        if percentile_min_history > window_size:
            raise ValueError("percentile_min_history must be <= window_size")

        if isinstance(hysteresis_margin, bool):
            raise ValueError("hysteresis_margin must be numeric (bool not allowed)")
        try:
            hysteresis_margin_f = float(hysteresis_margin)
        except Exception as e:
            raise ValueError("hysteresis_margin must be numeric") from e
        if not math.isfinite(hysteresis_margin_f) or hysteresis_margin_f <= 0.0:
            raise ValueError("hysteresis_margin must be finite > 0")

        self._window_size: int = window_size
        self._percentile_min_history: int = percentile_min_history
        self._hysteresis_margin: float = float(hysteresis_margin_f)

        self._lock = threading.Lock()
        self._scores: Deque[float] = deque(maxlen=window_size)

        # TRADE-GRADE: allocation band 경계 흔들림 완화용 상태
        self._last_decision_band: Optional[str] = None
        self._last_decision_score: Optional[float] = None

    # ── properties ─────────────────────────────
    @property
    def window_size(self) -> int:
        return self._window_size

    @property
    def history_len(self) -> int:
        with self._lock:
            return len(self._scores)

    @property
    def hysteresis_margin(self) -> float:
        return self._hysteresis_margin

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

    def snapshot_decision_state(self) -> Dict[str, Optional[float | str]]:
        """현재 hysteresis decision state 스냅샷."""
        with self._lock:
            return {
                "last_decision_band": self._last_decision_band,
                "last_decision_score": self._last_decision_score,
            }

    # ── absolute threshold decision ────────────
    def decide(self, score: object) -> RegimeDecision:
        """
        절대 기준 allocation 판단(히스토리 불필요).

        STRICT:
        - score 유효성 강제
        - allocation은 정책에 따른 결정값(폴백 없음)
        - 이전 decision state 를 사용해 band hysteresis 적용
        """
        v = _as_score(score, name="regime_score")

        with self._lock:
            hlen = len(self._scores)
            prev_band = self._last_decision_band
            band = _band_with_hysteresis(v, prev_band, self._hysteresis_margin)
            alloc = _allocation_by_band_strict(band)
            self._last_decision_band = band
            self._last_decision_score = float(v)

        return RegimeDecision(
            score=v,
            allocation=float(alloc),
            band=band,
            history_len=hlen,
            window_size=self._window_size,
            percentile=None,
            micro_score_risk=None,
            micro_multiplier=None,
        )

    # ── microstructure overlay (TRADE-GRADE) ────
    def decide_with_microstructure(self, score: object, *, micro_score_risk: object) -> RegimeDecision:
        """
        절대 기준 decision + microstructure penalty 적용.

        STRICT:
        - score, micro_score_risk 모두 0~100 범위 강제(클램프 금지)
        - multiplier는 명시 룰로만 산출
        """
        base = self.decide(score)
        ms = _as_micro_score(micro_score_risk, name="micro_score_risk")
        mul = _micro_multiplier(ms)

        alloc = float(base.allocation) * float(mul)

        band = base.band
        if mul == 0.0:
            band = f"{band}|MICRO_BLOCK"
        elif mul < 1.0:
            band = f"{band}|MICRO_PENALTY"

        return RegimeDecision(
            score=base.score,
            allocation=alloc,
            band=band,
            history_len=base.history_len,
            window_size=base.window_size,
            percentile=None,
            micro_score_risk=float(ms),
            micro_multiplier=float(mul),
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
            micro_score_risk=None,
            micro_multiplier=None,
        )

    def decide_with_percentile_and_microstructure(self, score: object, *, micro_score_risk: object) -> RegimeDecision:
        """
        절대 기준 decision + percentile + microstructure penalty.

        STRICT:
        - percentile 준비 안 됐으면 예외
        - micro_score_risk 유효성 강제
        """
        d = self.decide_with_microstructure(score, micro_score_risk=micro_score_risk)
        p = self.percentile(d.score)
        return RegimeDecision(
            score=d.score,
            allocation=d.allocation,
            band=d.band,
            history_len=d.history_len,
            window_size=d.window_size,
            percentile=p,
            micro_score_risk=d.micro_score_risk,
            micro_multiplier=d.micro_multiplier,
        )


__all__ = [
    "RegimeEngineError",
    "RegimeScoreError",
    "RegimeNotReadyError",
    "MicrostructureScoreError",
    "RegimeDecision",
    "RegimeEngine",
]