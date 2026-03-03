from __future__ import annotations

"""
========================================================
FILE: strategy/ev_heatmap_engine.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
역할
- regime × distortion × score 구간별 EV(기대값)를 rolling window로 추정하고,
  음수 구간을 자동 차단(block)할 수 있는 "히트맵 엔진"을 제공한다.
- 이 모듈은 DB I/O를 하지 않는다. (외부 I/O 금지)
  -> 호출자가 최근 트레이드 결과(pnl_r 등)를 공급한다.

핵심 원칙 (STRICT · NO-FALLBACK)
- 입력 누락/타입 불일치/범위 이탈은 즉시 예외.
- 히트맵 셀 최소 샘플 미달 상태에서 EV를 "추정"하지 않는다.
  - min_samples 미달이면 상태=NOT_READY 로만 반환한다.
- EV가 계산 불가하면 None 반환 금지. (명시 상태로 반환)
- 이 모듈은 자동으로 과거 값으로 폴백하지 않는다.

정의
- 셀 키:
  - regime_band: 문자열(예: NO_TRADE/LOW/MID/HIGH or custom)
  - distortion_bucket: DI 또는 micro_score_risk 버킷
  - score_bucket: engine_total_score 버킷
- EV:
  - 여기서는 "R 기준" 기대값을 권장한다.
  - 입력 pnl_r: 1 트레이드의 결과를 R 단위로 정규화한 값(예: +1.2R, -1.0R)
  - EV = mean(pnl_r) (rolling window)

운영 정책(권장 기본)
- distortion_bucket (micro_score_risk 기준):
  - [0,40), [40,60), [60,80), [80,100]
- score_bucket (engine_total_score 기준):
  - [0,35), [35,50), [50,65), [65,100]
- block rule:
  - min_samples 이상이고 EV < 0 이면 해당 셀을 "BLOCK" 후보로 표시
  - 히스테리시스:
    * block_on_ev: 0.0 (기본)
    * unblock_on_ev: +0.10 (기본)  -> 회복 구간
  - NOTE: 실제 차단 적용은 risk/auto_block_engine이 수행한다(이 모듈은 상태만 제공).

변경 이력
--------------------------------------------------------
- 2026-03-03:
  1) 신규 생성: EV 히트맵 셀 집계/상태 판단/차단 후보 산출(STRICT)
========================================================
"""

import math
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


class EvHeatmapError(RuntimeError):
    """EV 히트맵 엔진 오류(STRICT)."""


@dataclass(frozen=True, slots=True)
class HeatmapKey:
    regime_band: str
    distortion_bucket: str
    score_bucket: str


@dataclass(frozen=True, slots=True)
class CellStats:
    n: int
    ev: Optional[float]  # None when NOT_READY
    status: str  # NOT_READY | OK | BLOCK
    block_reason: Optional[str]


@dataclass(frozen=True, slots=True)
class HeatmapSnapshot:
    window_size: int
    min_samples: int
    block_on_ev: float
    unblock_on_ev: float
    cells: Dict[str, CellStats]  # key_str -> stats


def _fail(stage: str, reason: str) -> None:
    raise EvHeatmapError(f"[EV-HEATMAP] {stage} 실패: {reason}")


def _require_nonempty_str(stage: str, v: Any, name: str) -> str:
    if v is None:
        _fail(stage, f"{name} is None")
    s = str(v).strip()
    if not s:
        _fail(stage, f"{name} is empty")
    return s


def _require_float(stage: str, v: Any, name: str) -> float:
    try:
        fv = float(v)
    except Exception:
        _fail(stage, f"{name} must be float (got={v!r})")
    if not math.isfinite(fv):
        _fail(stage, f"{name} must be finite (got={fv})")
    return fv


def _require_int(stage: str, v: Any, name: str) -> int:
    try:
        iv = int(v)
    except Exception:
        _fail(stage, f"{name} must be int (got={v!r})")
    return iv


def _bucket_micro_score_risk(micro_score_risk: float) -> str:
    # STRICT: input already validated by caller; still guard range.
    if micro_score_risk < 0.0 or micro_score_risk > 100.0 or not math.isfinite(micro_score_risk):
        _fail("bucket", f"micro_score_risk out of range [0,100]: {micro_score_risk}")
    if micro_score_risk < 40.0:
        return "M0_40"
    if micro_score_risk < 60.0:
        return "M40_60"
    if micro_score_risk < 80.0:
        return "M60_80"
    return "M80_100"


def _bucket_engine_score(score_0_100: float) -> str:
    if score_0_100 < 0.0 or score_0_100 > 100.0 or not math.isfinite(score_0_100):
        _fail("bucket", f"engine_total_score out of range [0,100]: {score_0_100}")
    if score_0_100 < 35.0:
        return "S0_35"
    if score_0_100 < 50.0:
        return "S35_50"
    if score_0_100 < 65.0:
        return "S50_65"
    return "S65_100"


def _key_str(k: HeatmapKey) -> str:
    return f"{k.regime_band}|{k.distortion_bucket}|{k.score_bucket}"


class EvHeatmapEngine:
    """
    STRICT · NO-FALLBACK EV Heatmap Engine

    사용법:
    - on_trade_close(...) 로 트레이드 결과(pnl_r)를 히트맵 셀에 누적
    - get_cell_status(...) 로 현재 셀 상태 조회
    - snapshot() 으로 전체 히트맵 상태 덤프

    주의:
    - 본 엔진은 DB를 읽지 않는다.
    - pnl_r은 caller가 R단위로 이미 정규화하여 공급해야 한다.
    """

    def __init__(
        self,
        *,
        window_size: int = 50,
        min_samples: int = 20,
        block_on_ev: float = 0.0,
        unblock_on_ev: float = 0.10,
    ) -> None:
        ws = _require_int("init", window_size, "window_size")
        ms = _require_int("init", min_samples, "min_samples")

        if ws <= 0:
            _fail("init", f"window_size must be >0 (got={ws})")
        if ms <= 0:
            _fail("init", f"min_samples must be >0 (got={ms})")
        if ms > ws:
            _fail("init", f"min_samples must be <= window_size (got={ms}>{ws})")

        boe = _require_float("init", block_on_ev, "block_on_ev")
        uoe = _require_float("init", unblock_on_ev, "unblock_on_ev")

        self._window_size = int(ws)
        self._min_samples = int(ms)
        self._block_on_ev = float(boe)
        self._unblock_on_ev = float(uoe)

        self._lock = threading.Lock()
        # key_str -> list[pnl_r] (rolling)
        self._cells: Dict[str, List[float]] = {}
        # key_str -> bool (blocked state with hysteresis)
        self._blocked: Dict[str, bool] = {}

    @property
    def window_size(self) -> int:
        return self._window_size

    @property
    def min_samples(self) -> int:
        return self._min_samples

    def build_key(
        self,
        *,
        regime_band: str,
        micro_score_risk: float,
        engine_total_score: float,
    ) -> HeatmapKey:
        rb = _require_nonempty_str("key", regime_band, "regime_band")
        msr = _require_float("key", micro_score_risk, "micro_score_risk")
        ets = _require_float("key", engine_total_score, "engine_total_score")

        return HeatmapKey(
            regime_band=rb,
            distortion_bucket=_bucket_micro_score_risk(msr),
            score_bucket=_bucket_engine_score(ets),
        )

    def on_trade_close(
        self,
        *,
        key: HeatmapKey,
        pnl_r: float,
    ) -> None:
        """
        트레이드 종료 결과를 히트맵에 반영한다.

        STRICT:
        - pnl_r finite 필수
        - key 문자열 필수
        """
        if not isinstance(key, HeatmapKey):
            _fail("on_trade_close", "key must be HeatmapKey")
        pr = _require_float("on_trade_close", pnl_r, "pnl_r")

        ks = _key_str(key)

        with self._lock:
            series = self._cells.get(ks)
            if series is None:
                series = []
                self._cells[ks] = series

            series.append(float(pr))
            if len(series) > self._window_size:
                # rolling 유지 (oldest drop)
                del series[: len(series) - self._window_size]

            # update block state (hysteresis)
            if len(series) < self._min_samples:
                # not ready: do not change blocked state (avoid flip-flop)
                return

            ev = sum(series) / float(len(series))
            if not math.isfinite(ev):
                _fail("on_trade_close", f"computed ev not finite (ks={ks})")

            blocked = bool(self._blocked.get(ks, False))

            if not blocked and ev < self._block_on_ev:
                self._blocked[ks] = True
            elif blocked and ev >= self._unblock_on_ev:
                self._blocked[ks] = False

    def get_cell_status(self, key: HeatmapKey) -> CellStats:
        if not isinstance(key, HeatmapKey):
            _fail("get_cell_status", "key must be HeatmapKey")
        ks = _key_str(key)

        with self._lock:
            series = self._cells.get(ks, [])
            n = len(series)
            blocked = bool(self._blocked.get(ks, False))

        if n < self._min_samples:
            return CellStats(n=n, ev=None, status="NOT_READY", block_reason=None)

        ev = sum(series) / float(n)
        if not math.isfinite(ev):
            _fail("get_cell_status", f"computed ev not finite (ks={ks})")

        if blocked:
            return CellStats(
                n=n,
                ev=float(ev),
                status="BLOCK",
                block_reason=f"EV<{self._block_on_ev} (hysteresis)",
            )
        return CellStats(n=n, ev=float(ev), status="OK", block_reason=None)

    def snapshot(self) -> HeatmapSnapshot:
        """
        전체 셀 상태를 덤프한다. (caller가 로깅/DB 저장 가능)
        """
        with self._lock:
            items = list(self._cells.items())
            blocked = dict(self._blocked)

        cells_out: Dict[str, CellStats] = {}

        for ks, series in items:
            n = len(series)
            if n < self._min_samples:
                cells_out[ks] = CellStats(n=n, ev=None, status="NOT_READY", block_reason=None)
                continue

            ev = sum(series) / float(n)
            if not math.isfinite(ev):
                _fail("snapshot", f"computed ev not finite (ks={ks})")

            if bool(blocked.get(ks, False)):
                cells_out[ks] = CellStats(
                    n=n,
                    ev=float(ev),
                    status="BLOCK",
                    block_reason=f"EV<{self._block_on_ev} (hysteresis)",
                )
            else:
                cells_out[ks] = CellStats(n=n, ev=float(ev), status="OK", block_reason=None)

        return HeatmapSnapshot(
            window_size=self._window_size,
            min_samples=self._min_samples,
            block_on_ev=self._block_on_ev,
            unblock_on_ev=self._unblock_on_ev,
            cells=cells_out,
        )


__all__ = [
    "EvHeatmapError",
    "HeatmapKey",
    "CellStats",
    "HeatmapSnapshot",
    "EvHeatmapEngine",
]