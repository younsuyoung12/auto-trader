"""
========================================================
strategy/account_state_builder.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
역할
--------------------------------------------------------
- 계정 상태(Account State)를 "정확한 데이터"로만 계산한다.
- 이 모듈은 다음 값들을 산출한다:
  1) dd_pct                : (피크 대비) 드로우다운 %
  2) equity_current_usdt   : 현재 평가금(USDT) (caller가 제공)
  3) equity_peak_usdt      : 피크 평가금(USDT)
  4) consecutive_losses    : 최근 청산 트레이드 기준 연속 손실 횟수
  5) recent_win_rate       : 최근 N회 청산 트레이드 승률
  6) recent_planned_rr_avg : 최근 N회 "계획 RR"(tp_pct/sl_pct) 평균 (존재하는 데이터만)

핵심 원칙 (공동 규칙)
--------------------------------------------------------
- 절대 폴백 금지:
  - 데이터 누락/형식 오류/범위 이탈 시 즉시 예외.
  - None → 0 치환, 임의 보정, 임의 추정 금지.
- 외부 I/O 금지:
  - DB/거래소/네트워크 접근 금지. (caller가 데이터 공급)
- 민감정보 로그 금지.
- settings 객체 불변.

입력 계약(Caller 책임)
--------------------------------------------------------
- current_equity_usdt: 현재 평가금(또는 가용/총자산 등) "단 하나"를 숫자로 제공.
- closed_trades: 최근 청산된 트레이드 목록(최신→과거 내림차순 권장).
  각 원소는 dict이며 최소 키가 필요:
    - id (int)
    - exit_ts (datetime or ISO8601 str)  *정렬 검증 목적(최신→과거)
    - pnl_usdt (float)                  *승률/연속손실
  선택 키(있으면 planned RR 계산):
    - tp_pct (float)
    - sl_pct (float)

DD(드로우다운) 정의
--------------------------------------------------------
- 본 구현은 "피크 기반 DD"를 계산한다.
- 외부 I/O를 하지 않기 때문에, 재시작 후에도 DD를 유지하려면 caller가
  persisted_equity_peak_usdt(이전에 저장해둔 피크)를 전달해야 한다.

PATCH NOTES — 2026-03-02
--------------------------------------------------------
- AccountStateBuilder 도입(런타임 DD + 최근 승률/연속손실/계획 RR).
- NO-FALLBACK 검증 강화(필수 키/타입/범위).

PATCH NOTES — 2026-03-02 (PATCH)
--------------------------------------------------------
- DD 재시작 리스크 완화(외부 I/O 없이):
  - build(..., persisted_equity_peak_usdt=...) 옵션 추가.
  - peak 계산은 max(persisted_peak, runtime_peak, current_equity)로 확정.
  - caller가 DB에 peak를 저장/복구하면 재시작 후에도 DD가 유지된다.
- closed_trades 정렬(최신→과거) 검증 추가(위반 시 즉시 예외).
========================================================
"""

from __future__ import annotations

import math
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple


# ─────────────────────────────────────────────
# Exceptions (STRICT)
# ─────────────────────────────────────────────
class AccountStateError(RuntimeError):
    """Base error for account state builder."""


class AccountStateInputError(AccountStateError):
    """Raised when inputs are missing or invalid."""


class AccountStateNotReadyError(AccountStateError):
    """Raised when there is not enough usable trade data for a requested metric."""


# ─────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class AccountState:
    symbol: str
    equity_current_usdt: float
    equity_peak_usdt: float
    dd_pct: float  # 0.0 ~ 100.0
    consecutive_losses: int
    recent_win_rate: float  # 0.0 ~ 1.0
    recent_trades_count: int
    recent_planned_rr_avg: Optional[float]  # None if not computable
    last_closed_trade_id: int
    last_closed_trade_ts: datetime


# ─────────────────────────────────────────────
# Helpers (STRICT)
# ─────────────────────────────────────────────
def _require_non_empty_str(v: Any, name: str) -> str:
    if not isinstance(v, str) or not v.strip():
        raise AccountStateInputError(f"{name} must be non-empty str")
    return v.strip()


def _as_float(v: Any, name: str, *, min_value: Optional[float] = None) -> float:
    if isinstance(v, bool):
        raise AccountStateInputError(f"{name} must be numeric (bool not allowed)")
    try:
        x = float(v)
    except Exception as e:
        raise AccountStateInputError(f"{name} must be a number") from e
    if not math.isfinite(x):
        raise AccountStateInputError(f"{name} must be finite")
    if min_value is not None and x < min_value:
        raise AccountStateInputError(f"{name} must be >= {min_value}")
    return x


def _as_int(v: Any, name: str, *, min_value: Optional[int] = None) -> int:
    if isinstance(v, bool):
        raise AccountStateInputError(f"{name} must be int (bool not allowed)")
    try:
        x = int(v)
    except Exception as e:
        raise AccountStateInputError(f"{name} must be int") from e
    if min_value is not None and x < min_value:
        raise AccountStateInputError(f"{name} must be >= {min_value}")
    return x


def _as_datetime(v: Any, name: str) -> datetime:
    if isinstance(v, datetime):
        if v.tzinfo is None or v.tzinfo.utcoffset(v) is None:
            raise AccountStateInputError(f"{name} must be timezone-aware datetime")
        return v
    if isinstance(v, str) and v.strip():
        # STRICT: ISO8601만 허용. 실패 시 예외.
        try:
            dt = datetime.fromisoformat(v.strip().replace("Z", "+00:00"))
        except Exception as e:
            raise AccountStateInputError(f"{name} must be ISO8601 datetime str") from e
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            raise AccountStateInputError(f"{name} must be timezone-aware datetime")
        return dt
    raise AccountStateInputError(f"{name} must be datetime or ISO8601 str")


def _extract_closed_trade_row(row: Any) -> Tuple[int, datetime, float, Optional[float], Optional[float]]:
    """
    row(dict)에서 필요한 필드를 STRICT로 추출한다.
    Returns: (id, exit_ts, pnl_usdt, tp_pct, sl_pct)
    """
    if not isinstance(row, dict) or not row:
        raise AccountStateInputError("closed_trades item must be non-empty dict")

    trade_id = _as_int(row.get("id"), "trade.id", min_value=1)
    exit_ts = _as_datetime(row.get("exit_ts"), "trade.exit_ts")
    pnl_usdt = _as_float(row.get("pnl_usdt"), "trade.pnl_usdt")

    # tp/sl은 선택(있을 때만 planned RR 계산)
    tp_pct = row.get("tp_pct")
    sl_pct = row.get("sl_pct")

    tp_f = None if tp_pct is None else _as_float(tp_pct, "trade.tp_pct", min_value=0.0)
    sl_f = None if sl_pct is None else _as_float(sl_pct, "trade.sl_pct", min_value=0.0)

    # STRICT: tp/sl이 둘 다 있으면 0보다 커야 RR이 의미있다.
    if tp_f is not None and tp_f <= 0:
        tp_f = None
    if sl_f is not None and sl_f <= 0:
        sl_f = None

    return trade_id, exit_ts, pnl_usdt, tp_f, sl_f


def _validate_closed_trades_sorted_desc(
    rows: List[Tuple[int, datetime, float, Optional[float], Optional[float]]]
) -> None:
    """
    STRICT: 최신→과거(내림차순) 정렬을 검증한다.
    - rows[0].exit_ts >= rows[1].exit_ts >= ...
    """
    if len(rows) < 2:
        return
    prev_ts = rows[0][1]
    for i in range(1, len(rows)):
        ts = rows[i][1]
        if ts > prev_ts:
            raise AccountStateInputError(
                "closed_trades must be sorted by exit_ts desc (most recent first)"
            )
        prev_ts = ts


def _compute_planned_rr_avg(rows: List[Tuple[int, datetime, float, Optional[float], Optional[float]]]) -> Optional[float]:
    """
    계획 RR = tp_pct / sl_pct (둘 다 있을 때만)
    STRICT: 임의 대체/추정 없음. 계산 불가면 None.
    """
    vals: List[float] = []
    for _, __, ___, tp, sl in rows:
        if tp is None or sl is None:
            continue
        rr = tp / sl
        if math.isfinite(rr) and rr > 0:
            vals.append(rr)
    if not vals:
        return None
    return float(sum(vals) / len(vals))


# ─────────────────────────────────────────────
# Builder
# ─────────────────────────────────────────────
class AccountStateBuilder:
    """
    계정 상태 계산기.

    사용 패턴(권장):
    - 프로세스 시작 시 1회 생성 후 재사용(런타임 peak 유지).
    - 재시작 DD 유지가 필요하면 caller가 persisted_equity_peak_usdt를 전달한다.
      (예: DB에 저장해둔 equity_peak_usdt 값을 꺼내 build()에 주입)
    """

    def __init__(
        self,
        *,
        win_rate_window: int = 20,
        min_trades_for_win_rate: int = 5,
        initial_equity_peak_usdt: Optional[float] = None,
    ) -> None:
        if not isinstance(win_rate_window, int) or win_rate_window <= 0:
            raise ValueError("win_rate_window must be positive int")
        if not isinstance(min_trades_for_win_rate, int) or min_trades_for_win_rate <= 0:
            raise ValueError("min_trades_for_win_rate must be positive int")
        if min_trades_for_win_rate > win_rate_window:
            raise ValueError("min_trades_for_win_rate must be <= win_rate_window")

        self._win_rate_window = win_rate_window
        self._min_trades_for_win_rate = min_trades_for_win_rate

        self._lock = threading.Lock()
        self._equity_peak_usdt: Optional[float] = None

        if initial_equity_peak_usdt is not None:
            peak = _as_float(initial_equity_peak_usdt, "initial_equity_peak_usdt", min_value=0.0)
            if peak <= 0:
                raise ValueError("initial_equity_peak_usdt must be > 0")
            self._equity_peak_usdt = float(peak)

    def get_peak_equity_usdt(self) -> Optional[float]:
        """caller가 외부 저장(예: DB)할 수 있도록 현재 peak를 반환한다(없으면 None)."""
        with self._lock:
            return None if self._equity_peak_usdt is None else float(self._equity_peak_usdt)

    def reset_peak(self) -> None:
        """의도적 리셋(운영에서 일반적으로 사용 금지)."""
        with self._lock:
            self._equity_peak_usdt = None

    def build(
        self,
        symbol: str,
        *,
        current_equity_usdt: Any,
        closed_trades: Iterable[Dict[str, Any]],
        persisted_equity_peak_usdt: Any = None,
    ) -> AccountState:
        sym = _require_non_empty_str(symbol, "symbol").upper()

        equity_now = _as_float(current_equity_usdt, "current_equity_usdt", min_value=0.0)
        if equity_now <= 0.0:
            raise AccountStateInputError("current_equity_usdt must be > 0")

        persisted_peak: Optional[float] = None
        if persisted_equity_peak_usdt is not None:
            persisted_peak = _as_float(persisted_equity_peak_usdt, "persisted_equity_peak_usdt", min_value=0.0)
            if persisted_peak <= 0.0:
                raise AccountStateInputError("persisted_equity_peak_usdt must be > 0")

        # peak equity 확정(STRICT, 외부 I/O 없이)
        with self._lock:
            runtime_peak = self._equity_peak_usdt

            # peak 후보: runtime_peak / persisted_peak / equity_now
            candidates: List[float] = [float(equity_now)]
            if runtime_peak is not None:
                candidates.append(float(runtime_peak))
            if persisted_peak is not None:
                candidates.append(float(persisted_peak))

            equity_peak = max(candidates)
            self._equity_peak_usdt = float(equity_peak)

        if equity_peak <= 0.0:
            raise AccountStateError("equity_peak_usdt invalid")

        dd_pct = ((equity_peak - equity_now) / equity_peak) * 100.0
        if dd_pct < 0.0:
            raise AccountStateError(f"dd_pct computed negative: {dd_pct}")
        if not math.isfinite(dd_pct):
            raise AccountStateError("dd_pct must be finite")

        if closed_trades is None:
            raise AccountStateInputError("closed_trades must not be None")

        parsed: List[Tuple[int, datetime, float, Optional[float], Optional[float]]] = []
        for row in closed_trades:
            parsed.append(_extract_closed_trade_row(row))

        if not parsed:
            raise AccountStateNotReadyError("no closed_trades provided")

        # STRICT: 최신→과거 정렬 검증
        _validate_closed_trades_sorted_desc(parsed)

        last_id, last_ts, *_ = parsed[0]

        # consecutive losses: from most recent backwards
        consec_losses = 0
        for _, __, pnl, ___, ____ in parsed:
            if pnl < 0:
                consec_losses += 1
            else:
                break

        # win rate over last N
        n = min(self._win_rate_window, len(parsed))
        window = parsed[:n]
        if len(window) < self._min_trades_for_win_rate:
            raise AccountStateNotReadyError(
                f"not enough trades for win_rate: need>={self._min_trades_for_win_rate}, got={len(window)}"
            )
        wins = sum(1 for _, __, pnl, ___, ____ in window if pnl > 0)
        win_rate = wins / len(window)

        planned_rr_avg = _compute_planned_rr_avg(window)

        return AccountState(
            symbol=sym,
            equity_current_usdt=float(equity_now),
            equity_peak_usdt=float(equity_peak),
            dd_pct=float(dd_pct),
            consecutive_losses=int(consec_losses),
            recent_win_rate=float(win_rate),
            recent_trades_count=int(len(window)),
            recent_planned_rr_avg=planned_rr_avg,
            last_closed_trade_id=int(last_id),
            last_closed_trade_ts=last_ts,
        )