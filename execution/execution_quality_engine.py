from __future__ import annotations

"""
========================================================
FILE: execution/execution_quality_engine.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
역할
- 주문 실행 품질(Execution Quality)을 정량화하여 기록 가능한 스냅샷을 생성한다.
- 목표:
  1) 예상 체결가 vs 실제 체결가 차이(slippage) 산출
  2) 체결 후 1~5초 가격 이동(impact / adverse move) 산출
  3) execution_score(0~100) 산출
- 이 모듈은 "계산/스냅샷"만 한다. 주문 실행/리스크 조정/DB 기록은 상위 레이어가 수행한다.

절대 원칙 (STRICT · NO-FALLBACK)
- 필요한 입력(가격/타임스탬프/체결 정보) 누락/비정상 시 즉시 예외.
- 시장가/지정가 여부와 무관하게, 실제 체결가(filled_avg_price)가 없으면 계산 불가 → 예외.
- 가격 이동(1~5초)이 관측 불가하면 None 처리/추정 금지 → 예외.
  (이 모듈은 "폴백"을 만들지 않는다. 호출자는 이 모듈 호출 시점을 보장해야 한다.)

입력 계약(중요)
- expected_price: 주문 제출 당시 기대 체결가(예: best_ask/best_bid 또는 mid)
- filled_avg_price: 실제 평균 체결가(주문 체결 후 거래소 응답 기반)
- side: "BUY" 또는 "SELL"
- post_prices: { "t+1s": float, "t+3s": float, "t+5s": float } (필수 키)

점수 정의(명시)
- slippage_pct = |filled_avg_price - expected_price| / expected_price * 100
- adverse_move_pct:
    BUY  -> max(0, (filled_avg_price - post_price)/filled_avg_price*100)
    SELL -> max(0, (post_price - filled_avg_price)/filled_avg_price*100)
  각 시점(t+1s, t+3s, t+5s)의 최대값을 사용
- execution_score:
    score = 100 - (slippage_pct*alpha + adverse_move_pct*beta)
    clamp to [0,100]
  (alpha=250, beta=150 기본값; 값은 settings로 튜닝 가능)

변경 이력
--------------------------------------------------------
- 2026-03-03:
  1) 신규 생성: execution quality 스냅샷(slippage/impact/adverse move/score) 산출(STRICT)
========================================================
"""

import math
import time
from dataclasses import dataclass
from typing import Any, Dict, Literal, Optional, Tuple

from settings import load_settings

SET = load_settings()


class ExecutionQualityError(RuntimeError):
    """실행 품질 계산 단계 오류(STRICT)."""


@dataclass(frozen=True, slots=True)
class ExecutionQualitySnapshot:
    ts_ms: int
    symbol: str
    side: Literal["BUY", "SELL"]

    expected_price: float
    filled_avg_price: float

    slippage_pct: float
    adverse_move_pct: float

    post_prices: Dict[str, float]  # t+1s/t+3s/t+5s
    execution_score: float

    meta: Dict[str, Any]


def _now_ms() -> int:
    return int(time.time() * 1000)


def _fail(stage: str, reason: str, exc: Optional[BaseException] = None) -> None:
    msg = f"[EXEC-QUALITY] {stage} 실패: {reason}"
    if exc is None:
        raise ExecutionQualityError(msg)
    raise ExecutionQualityError(msg) from exc


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
    except Exception as e:
        _fail(stage, f"{name} must be float (got={v!r})", e)
    if not math.isfinite(fv):
        _fail(stage, f"{name} must be finite (got={fv})")
    return fv


def _require_pos_float(stage: str, v: Any, name: str) -> float:
    fv = _require_float(stage, v, name)
    if fv <= 0:
        _fail(stage, f"{name} must be >0 (got={fv})")
    return fv


def _clamp_0_100(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 100.0:
        return 100.0
    return x


def _load_tuning() -> Tuple[float, float]:
    """
    STRICT: 설정이 없으면 기본값 사용(정책 기본값).
    - 이는 데이터 폴백이 아닌 "상수 정책"이다.
    """
    a_raw = getattr(SET, "exec_quality_alpha", 250)
    b_raw = getattr(SET, "exec_quality_beta", 150)
    try:
        alpha = float(a_raw)
        beta = float(b_raw)
    except Exception as e:
        _fail("config", f"exec_quality_alpha/beta must be numeric (got={a_raw!r}/{b_raw!r})", e)

    if not math.isfinite(alpha) or alpha <= 0:
        _fail("config", f"exec_quality_alpha invalid (got={alpha})")
    if not math.isfinite(beta) or beta <= 0:
        _fail("config", f"exec_quality_beta invalid (got={beta})")

    return alpha, beta


def _require_post_prices(stage: str, v: Any) -> Dict[str, float]:
    d = v
    if not isinstance(d, dict):
        _fail(stage, f"post_prices must be dict (got={type(d)})")

    needed = ("t+1s", "t+3s", "t+5s")
    out: Dict[str, float] = {}
    for k in needed:
        if k not in d:
            _fail(stage, f"post_prices missing key: {k}")
        out[k] = _require_pos_float(stage, d.get(k), f"post_prices[{k}]")

    return out


def build_execution_quality_snapshot(
    *,
    symbol: str,
    side: str,
    expected_price: float,
    filled_avg_price: float,
    post_prices: Dict[str, float],
) -> Dict[str, Any]:
    snap = build_execution_quality_snapshot_obj(
        symbol=symbol,
        side=side,
        expected_price=expected_price,
        filled_avg_price=filled_avg_price,
        post_prices=post_prices,
    )
    return {
        "ts_ms": snap.ts_ms,
        "symbol": snap.symbol,
        "side": snap.side,
        "expected_price": snap.expected_price,
        "filled_avg_price": snap.filled_avg_price,
        "slippage_pct": snap.slippage_pct,
        "adverse_move_pct": snap.adverse_move_pct,
        "post_prices": dict(snap.post_prices),
        "execution_score": snap.execution_score,
        "meta": dict(snap.meta),
    }


def build_execution_quality_snapshot_obj(
    *,
    symbol: str,
    side: str,
    expected_price: float,
    filled_avg_price: float,
    post_prices: Dict[str, float],
) -> ExecutionQualitySnapshot:
    """
    STRICT: execution quality 스냅샷(typed) 생성
    """
    stage = "input"
    s = _require_nonempty_str(stage, symbol, "symbol").replace("-", "").replace("/", "").upper()
    sd = _require_nonempty_str(stage, side, "side").upper()
    if sd not in ("BUY", "SELL"):
        _fail(stage, f"side must be BUY/SELL (got={sd!r})")

    exp_px = _require_pos_float(stage, expected_price, "expected_price")
    fill_px = _require_pos_float(stage, filled_avg_price, "filled_avg_price")

    pp = _require_post_prices(stage, post_prices)

    # slippage
    slippage_pct = abs(fill_px - exp_px) / exp_px * 100.0
    if not math.isfinite(slippage_pct):
        _fail("compute", f"slippage_pct not finite (exp={exp_px}, fill={fill_px})")

    # adverse move: choose max across checkpoints
    adverse_candidates: Dict[str, float] = {}
    for k, px in pp.items():
        if sd == "BUY":
            # price falls after buy -> adverse
            adv = (fill_px - px) / fill_px * 100.0
        else:
            # price rises after sell -> adverse
            adv = (px - fill_px) / fill_px * 100.0
        if not math.isfinite(adv):
            _fail("compute", f"adverse move not finite at {k} (px={px})")
        adverse_candidates[k] = max(0.0, adv)

    adverse_move_pct = max(adverse_candidates.values())
    if not math.isfinite(adverse_move_pct):
        _fail("compute", "adverse_move_pct not finite")

    alpha, beta = _load_tuning()
    score_raw = 100.0 - (slippage_pct * alpha / 100.0) - (adverse_move_pct * beta / 100.0)
    if not math.isfinite(score_raw):
        _fail("compute", f"execution_score not finite (raw={score_raw})")

    execution_score = _clamp_0_100(score_raw)

    return ExecutionQualitySnapshot(
        ts_ms=_now_ms(),
        symbol=s,
        side=sd,  # type: ignore[arg-type]
        expected_price=exp_px,
        filled_avg_price=fill_px,
        slippage_pct=float(slippage_pct),
        adverse_move_pct=float(adverse_move_pct),
        post_prices=pp,
        execution_score=float(execution_score),
        meta={
            "alpha": alpha,
            "beta": beta,
            "adverse_candidates": adverse_candidates,
        },
    )


__all__ = [
    "ExecutionQualityError",
    "ExecutionQualitySnapshot",
    "build_execution_quality_snapshot",
    "build_execution_quality_snapshot_obj",
]