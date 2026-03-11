from __future__ import annotations

"""
========================================================
FILE: infra/drift_detector.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

ROLE:
- 런타임에서 “급변(Drift)”을 감지한다.
  - allocation(리스크 비중) 급변
  - risk_multiplier(레벨3 포함) 급변
  - regime band 급변
  - micro_score_risk 급변
- 임계치 초과 시 DriftDetectedError를 발생시킨다.
  (SAFE_STOP 결정/텔레그램/로그는 호출자 책임)

CORE RESPONSIBILITIES:
- allocation / multiplier / regime / micro drift STRICT 감지
- rolling median 기반 allocation spike ratio 탐지
- 안정 구간 이후 regime 변경 감지
- symbol 단위 단일 상태기계 보장
- 내부 상태 원자적 반영 보장

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- 폴백 금지(None→0 등 금지)
- 데이터 누락/비정상 값(NaN/inf/범위초과) 발견 시 즉시 예외
- 예외 삼키기 금지
- 환경변수 직접 접근 금지(settings.py 불필요: 순수 검증 모듈)
- DB 접근 금지(순수 상태/순수 가드)
- 시간 역전(timestamp rollback)은 data_integrity_guard가 담당(여기는 drift)
- 모든 drift 판정은 min_history 충족 이후에만 수행한다
- 상태 반영은 원자적으로 수행하며 partial commit 을 허용하지 않는다
- 하나의 DriftDetector 인스턴스는 하나의 symbol 에만 사용한다

CHANGE HISTORY:
- 2026-03-11:
  1) FIX(ROOT-CAUSE): regime drift 도 min_history 충족 이후에만 판정하도록 정책/구현 통일
  2) FIX(ATOMICITY): drift 감지 시 내부 상태가 부분 반영되지 않도록 원자적 상태 반영 구조로 수정
  3) FIX(STRICT): 1 detector = 1 symbol 강제, symbol 혼합 입력 차단
  4) FIX(STRICT): symbol / regime_band 문자열 타입 검증 강화(str() 강제변환 제거)
========================================================
"""

import math
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, List, Optional, Sequence


class DriftDetectorError(RuntimeError):
    """DriftDetector 자체 입력/설정 오류(STRICT)."""


class DriftDetectedError(RuntimeError):
    """급변(Drift) 감지됨(STRICT)."""


def _require_nonempty_str(v: Any, name: str) -> str:
    if v is None:
        raise DriftDetectorError(f"{name} is required (STRICT)")
    if not isinstance(v, str):
        raise DriftDetectorError(f"{name} must be str (STRICT), got={type(v).__name__}")
    s = v.strip()
    if not s:
        raise DriftDetectorError(f"{name} is required (STRICT)")
    return s


def _require_float(v: Any, name: str) -> float:
    if v is None:
        raise DriftDetectorError(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        raise DriftDetectorError(f"{name} must be numeric (bool not allowed)")
    try:
        x = float(v)
    except Exception as e:
        raise DriftDetectorError(f"{name} must be numeric (STRICT): {e}") from e
    if not math.isfinite(x):
        raise DriftDetectorError(f"{name} must be finite (STRICT)")
    return float(x)


def _require_float_range(v: Any, name: str, *, lo: float, hi: float) -> float:
    x = _require_float(v, name)
    if x < lo or x > hi:
        raise DriftDetectorError(f"{name} out of range [{lo},{hi}] (STRICT): {x}")
    return float(x)


def _median(values: Sequence[float]) -> float:
    if not values:
        raise DriftDetectorError("median requires non-empty values (STRICT)")
    s = sorted(values)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return float(s[mid])
    return float((s[mid - 1] + s[mid]) / 2.0)


def _abs(x: float) -> float:
    return float(x if x >= 0 else -x)


@dataclass(frozen=True, slots=True)
class DriftDetectorConfig:
    """
    STRICT config.

    window_size:
      - history buffer length. (>=10 권장)
    min_history:
      - drift 판정을 시작하는 최소 히스토리 수.

    allocation_spike_ratio:
      - rolling median 대비 현재 allocation 비율 임계치.
      - 예: 2.0 이면 median 대비 2배 이상이면 drift.
    allocation_abs_jump:
      - 직전 값 대비 절대 점프 임계치(예: 0.25면 +0.25 이상 점프 시 drift).
    multiplier_abs_jump:
      - 직전 multiplier 절대 점프 임계치.
    micro_abs_jump:
      - micro_score_risk 직전 대비 절대 점프 임계치(0~100 스케일).
    stable_regime_steps:
      - 동일 regime_band가 이 횟수 이상 유지된 뒤 변경되면 drift로 간주.
      - 단, 실제 판정은 min_history 충족 이후에만 수행한다.
    """

    window_size: int = 50
    min_history: int = 20

    allocation_spike_ratio: float = 2.0
    allocation_abs_jump: float = 0.25

    multiplier_abs_jump: float = 0.30
    micro_abs_jump: float = 30.0

    stable_regime_steps: int = 15

    def __post_init__(self) -> None:
        if not isinstance(self.window_size, int) or self.window_size < 10:
            raise DriftDetectorError("window_size must be int >= 10 (STRICT)")
        if not isinstance(self.min_history, int) or self.min_history < 5:
            raise DriftDetectorError("min_history must be int >= 5 (STRICT)")
        if self.min_history > self.window_size:
            raise DriftDetectorError("min_history must be <= window_size (STRICT)")

        if not math.isfinite(float(self.allocation_spike_ratio)) or float(self.allocation_spike_ratio) <= 1.0:
            raise DriftDetectorError("allocation_spike_ratio must be finite > 1.0 (STRICT)")
        if not math.isfinite(float(self.allocation_abs_jump)) or float(self.allocation_abs_jump) <= 0.0:
            raise DriftDetectorError("allocation_abs_jump must be finite > 0 (STRICT)")
        if float(self.allocation_abs_jump) > 1.0:
            raise DriftDetectorError("allocation_abs_jump must be <= 1.0 (STRICT)")

        if not math.isfinite(float(self.multiplier_abs_jump)) or float(self.multiplier_abs_jump) <= 0.0:
            raise DriftDetectorError("multiplier_abs_jump must be finite > 0 (STRICT)")
        if float(self.multiplier_abs_jump) > 1.0:
            raise DriftDetectorError("multiplier_abs_jump must be <= 1.0 (STRICT)")

        if not math.isfinite(float(self.micro_abs_jump)) or float(self.micro_abs_jump) <= 0.0:
            raise DriftDetectorError("micro_abs_jump must be finite > 0 (STRICT)")
        if float(self.micro_abs_jump) > 100.0:
            raise DriftDetectorError("micro_abs_jump must be <= 100 (STRICT)")

        if not isinstance(self.stable_regime_steps, int) or self.stable_regime_steps < 2:
            raise DriftDetectorError("stable_regime_steps must be int >= 2 (STRICT)")


@dataclass(frozen=True, slots=True)
class DriftSnapshot:
    """
    런타임 관측값(STRICT). caller는 반드시 값을 제공해야 한다(폴백 금지).
    """
    symbol: str
    allocation_ratio: float  # 0..1
    risk_multiplier: float  # 0..1
    regime_band: str  # e.g. NO_TRADE/LOW/MID/HIGH 등 (프로젝트 정의)
    micro_score_risk: float  # 0..100


@dataclass(frozen=True, slots=True)
class DriftEvent:
    kind: str
    message: str
    details: Dict[str, Any]


class DriftDetector:
    """
    STRICT:
    - update_and_check(snapshot) 호출 시, 임계치 초과 drift를 발견하면 DriftDetectedError를 발생.
    - 히스토리 부족(min_history 미만)인 경우 drift 판정 자체는 하지 않지만,
      입력 값의 범위/finite 검증은 항상 수행한다(데이터 오염 차단).
    - 하나의 인스턴스는 하나의 symbol 에만 사용한다.
    """

    def __init__(self, cfg: DriftDetectorConfig):
        if not isinstance(cfg, DriftDetectorConfig):
            raise DriftDetectorError("cfg must be DriftDetectorConfig (STRICT)")
        self._cfg = cfg

        self._alloc_hist: Deque[float] = deque(maxlen=cfg.window_size)
        self._mult_hist: Deque[float] = deque(maxlen=cfg.window_size)
        self._micro_hist: Deque[float] = deque(maxlen=cfg.window_size)

        self._symbol: Optional[str] = None
        self._last_regime: Optional[str] = None
        self._regime_stable_count: int = 0

        self._last_alloc: Optional[float] = None
        self._last_mult: Optional[float] = None
        self._last_micro: Optional[float] = None

    @property
    def cfg(self) -> DriftDetectorConfig:
        return self._cfg

    def reset(self) -> None:
        self._alloc_hist.clear()
        self._mult_hist.clear()
        self._micro_hist.clear()
        self._symbol = None
        self._last_regime = None
        self._regime_stable_count = 0
        self._last_alloc = None
        self._last_mult = None
        self._last_micro = None

    def _validate_snapshot_strict(self, s: DriftSnapshot) -> DriftSnapshot:
        sym = _require_nonempty_str(s.symbol, "snapshot.symbol").replace("-", "").replace("/", "").upper().strip()
        if not sym:
            raise DriftDetectorError("snapshot.symbol normalized empty (STRICT)")

        alloc = _require_float_range(s.allocation_ratio, "snapshot.allocation_ratio", lo=0.0, hi=1.0)
        if alloc <= 0.0:
            raise DriftDetectorError("snapshot.allocation_ratio must be > 0 (STRICT)")

        mult = _require_float_range(s.risk_multiplier, "snapshot.risk_multiplier", lo=0.0, hi=1.0)
        if mult <= 0.0:
            raise DriftDetectorError("snapshot.risk_multiplier must be > 0 (STRICT)")

        band = _require_nonempty_str(s.regime_band, "snapshot.regime_band")

        micro = _require_float_range(s.micro_score_risk, "snapshot.micro_score_risk", lo=0.0, hi=100.0)

        return DriftSnapshot(
            symbol=sym,
            allocation_ratio=float(alloc),
            risk_multiplier=float(mult),
            regime_band=band,
            micro_score_risk=float(micro),
        )

    def update_and_check(self, snapshot: DriftSnapshot) -> None:
        """
        STRICT:
        - 입력 검증은 항상 수행
        - 히스토리(min_history) 충족 시 drift 감지 수행
        - drift 감지 시 DriftDetectedError raise
        - 상태 반영은 원자적으로 수행한다
        """
        s = self._validate_snapshot_strict(snapshot)

        if self._symbol is None:
            next_symbol = s.symbol
        else:
            next_symbol = self._symbol
            if s.symbol != self._symbol:
                raise DriftDetectorError(
                    f"symbol mismatch for detector instance (STRICT): expected={self._symbol} got={s.symbol}"
                )

        next_alloc_hist = list(self._alloc_hist)
        next_alloc_hist.append(float(s.allocation_ratio))

        next_mult_hist = list(self._mult_hist)
        next_mult_hist.append(float(s.risk_multiplier))

        next_micro_hist = list(self._micro_hist)
        next_micro_hist.append(float(s.micro_score_risk))

        enough_history = len(next_alloc_hist) >= int(self._cfg.min_history)

        next_last_regime: Optional[str]
        next_regime_stable_count: int
        events: List[DriftEvent] = []

        if self._last_regime is None:
            next_last_regime = s.regime_band
            next_regime_stable_count = 1
        elif s.regime_band == self._last_regime:
            next_last_regime = self._last_regime
            next_regime_stable_count = self._regime_stable_count + 1
        else:
            prev_regime = self._last_regime
            prev_stable_count = int(self._regime_stable_count)
            next_last_regime = s.regime_band
            next_regime_stable_count = 1

            if enough_history and prev_stable_count >= int(self._cfg.stable_regime_steps):
                events.append(
                    DriftEvent(
                        kind="REGIME_BAND_DRIFT",
                        message="regime band changed after stable period (STRICT)",
                        details={
                            "prev": prev_regime,
                            "now": s.regime_band,
                            "stable_steps": prev_stable_count,
                        },
                    )
                )

        if enough_history:
            alloc_med = _median(next_alloc_hist)
            if alloc_med <= 0.0:
                raise DriftDetectorError(f"allocation median invalid (STRICT): {alloc_med}")

            alloc_ratio = float(s.allocation_ratio) / float(alloc_med)
            if not math.isfinite(alloc_ratio):
                raise DriftDetectorError("allocation_ratio/median not finite (STRICT)")

            if alloc_ratio >= float(self._cfg.allocation_spike_ratio):
                events.append(
                    DriftEvent(
                        kind="ALLOCATION_SPIKE_RATIO",
                        message="allocation spike vs rolling median (STRICT)",
                        details={
                            "alloc": s.allocation_ratio,
                            "median": alloc_med,
                            "ratio": alloc_ratio,
                            "threshold": self._cfg.allocation_spike_ratio,
                        },
                    )
                )

            if self._last_alloc is not None:
                alloc_jump = _abs(float(s.allocation_ratio) - float(self._last_alloc))
                if alloc_jump >= float(self._cfg.allocation_abs_jump):
                    events.append(
                        DriftEvent(
                            kind="ALLOCATION_ABS_JUMP",
                            message="allocation abs jump vs previous (STRICT)",
                            details={
                                "prev": self._last_alloc,
                                "now": s.allocation_ratio,
                                "jump": alloc_jump,
                                "threshold": self._cfg.allocation_abs_jump,
                            },
                        )
                    )

            if self._last_mult is not None:
                multiplier_jump = _abs(float(s.risk_multiplier) - float(self._last_mult))
                if multiplier_jump >= float(self._cfg.multiplier_abs_jump):
                    events.append(
                        DriftEvent(
                            kind="MULTIPLIER_ABS_JUMP",
                            message="risk_multiplier abs jump vs previous (STRICT)",
                            details={
                                "prev": self._last_mult,
                                "now": s.risk_multiplier,
                                "jump": multiplier_jump,
                                "threshold": self._cfg.multiplier_abs_jump,
                            },
                        )
                    )

            if self._last_micro is not None:
                micro_jump = _abs(float(s.micro_score_risk) - float(self._last_micro))
                if micro_jump >= float(self._cfg.micro_abs_jump):
                    events.append(
                        DriftEvent(
                            kind="MICRO_ABS_JUMP",
                            message="micro_score_risk abs jump vs previous (STRICT)",
                            details={
                                "prev": self._last_micro,
                                "now": s.micro_score_risk,
                                "jump": micro_jump,
                                "threshold": self._cfg.micro_abs_jump,
                            },
                        )
                    )

        self._symbol = next_symbol
        self._last_regime = next_last_regime
        self._regime_stable_count = next_regime_stable_count

        self._alloc_hist.append(float(s.allocation_ratio))
        self._mult_hist.append(float(s.risk_multiplier))
        self._micro_hist.append(float(s.micro_score_risk))

        self._last_alloc = float(s.allocation_ratio)
        self._last_mult = float(s.risk_multiplier)
        self._last_micro = float(s.micro_score_risk)

        if events:
            raise DriftDetectedError(self._format_events(events))

    @staticmethod
    def _format_events(events: Sequence[DriftEvent]) -> str:
        if not events:
            raise DriftDetectorError("format_events requires non-empty events (STRICT)")
        parts: List[str] = ["[DRIFT_DETECTED]"]
        for e in events:
            parts.append(f"{e.kind}: {e.message} | {e.details}")
        return " ; ".join(parts)


__all__ = [
    "DriftDetectorError",
    "DriftDetectedError",
    "DriftDetectorConfig",
    "DriftSnapshot",
    "DriftEvent",
    "DriftDetector",
]