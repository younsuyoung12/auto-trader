# state/trader_state.py
"""
========================================================
FILE: state/trader_state.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할
- in-memory Trade 상태와 거래소 포지션 상태를 동기화한다.
- “청산/부분청산”을 거래소 확정 데이터로만 판정하고, DB에 영속화한다.

핵심 원칙 (STRICT)
- 예외를 삼키지 않는다.
- 폴백 금지:
  - 누락/모호/정합 실패 시 즉시 예외
  - “첫 번째 row 선택”, “0이면 qty로 대체”, “시간 없으면 임의로 보정” 같은 추정 금지
- 민감정보(API키/토큰/DB비번 등) 로그/출력 금지
- 환경변수 직접 접근 금지: os.getenv 사용 금지 → settings.py(SSOT)만 사용
- 텔레그램 I/O는 비핵심: async_worker로 위임(실행 흐름을 블로킹하지 않음)

변경 이력
--------------------------------------------------------
- 2026-03-10:
  1) FIX(ROOT-CAUSE): timezone-aware datetime strict helper 명칭을 단일화
     - _require_tzaware_dt 를 canonical helper 로 추가
     - _require_tz_aware_datetime 는 compatibility alias 로 유지
     - helper naming drift 로 인한 Pylance undefined variable 오류 근본 제거
  2) FIX(STRICT): _persist_partial_progress_to_db_strict 의 synced_at 검증 경로 정합화
  3) 기존 기능 삭제 없음

- 2026-03-09:
  1) FIX(ROOT-CAUSE): 부분청산 누적 손익/잔량을 DB에 즉시 반영
     - 엔진 재시작 후에도 partial close 진행 상태가 유지되도록 개선
  2) FIX(ROOT-CAUSE): 최종 청산 summary 기대 수량을 trade.qty가 아니라 trade.remaining_qty 기준으로 변경
     - 부분청산 후 마지막 청산 orderId 추론 실패/오판 방지
  3) FIX(ROOT-CAUSE): 최종 청산 pnl_usdt를 마지막 slice pnl이 아니라
     accumulated realized_pnl_usdt + final pnl 로 집계
     - DB 최종 손익 누락 방지
  4) FIX(STRICT): 포지션 수량이 tracked remaining_qty 보다 증가하면 즉시 예외
     - 무단 증액/동기화 불일치 조용한 통과 금지
  5) FIX(STATE): 최종 청산 시 last_synced_at / reconciliation_status 를 거래소 close_time 기준으로 정합화
  6) 기존 기능 삭제 없음

- 2026-03-03 (TRADE-GRADE):
  1) ENV 직접 접근 제거:
     - os.getenv 기반 설정 읽기 제거(SLIPPAGE_MAX_TICKS, SLIPPAGE_MAX_NOTIONAL_PCT, STOP_ON_HEAVY_SLIPPAGE)
     - settings(SSOT) 필드로만 사용하도록 변경
  2) 폴백 제거/정합 강화:
     - close_time_ms가 없으면 now_ms로 대체하지 않음 → 즉시 예외
     - userTrades 요약에서 close_time(마지막 fill time) 필수화(없으면 예외)
     - _utc_now()로 DB exit_ts를 임의 보정하지 않음 → close_time 기반만 사용
  3) 텔레그램 비동기화:
     - send_tg 직접 호출 제거 → async_worker submit으로 위임
  4) 설정 의존성 명시:
     - slippage_max_ticks / slippage_max_notional_pct / stop_on_heavy_slippage / max_tp_sl_reset_failures
       가 Settings에 존재해야 함(없으면 즉시 예외)
- 2026-03-06:
  1) account websocket(USER DATA STREAM) 포지션 스냅샷 연동
     - slippage / close 판단에서 account_ws 스냅샷을 사용
  2) "첫 번째 row 선택" 제거
     - position 선택은 trade 방향 + position_side 기준 유일성 강제
  3) 부분청산/완전청산 판단 입력을 account_ws 실시간 스냅샷 기준으로 정합 강화
========================================================
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from execution.exceptions import OrderFailed, StateViolation, SyncMismatch
from execution.state_machine import TradeEvent, TradeLifecycleState, apply_event, get_state
from execution.exchange_api import req
from infra.account_ws import get_account_positions_snapshot
from infra.async_worker import submit as submit_async
from infra.telelog import log, send_tg
from settings import load_settings
from state.db_core import get_session
from state.db_models import Trade as TradeORM
from state.db_writer import close_latest_open_trade_returning_id, record_trade_exit_snapshot

SET = load_settings()


# ─────────────────────────────────────────────────────────────
# Telegram (non-blocking)
# ─────────────────────────────────────────────────────────────
def _submit_tg_nonblocking(msg: str) -> None:
    """
    STRICT:
    - 텔레그램은 비핵심 I/O → async_worker로 위임
    - 실패해도 핵심 흐름을 깨지 않는다(알림은 관측/운영 편의)
    """
    try:
        ok = submit_async(send_tg, msg, critical=False, label="send_tg")
        if not ok:
            log(f"[TG][DROP] queue full: {msg}")
    except Exception as e:
        log(f"[TG][SUBMIT_FAIL] {type(e).__name__}: {e} | msg={msg}")


# ─────────────────────────────────────────────────────────────
# Strict helpers
# ─────────────────────────────────────────────────────────────
def _require_nonempty_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        raise OrderFailed(f"{name} is required (STRICT)")
    return s


def _require_float(v: Any, name: str) -> float:
    if v is None:
        raise OrderFailed(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        raise OrderFailed(f"{name} must be numeric (bool not allowed) (STRICT)")
    try:
        f = float(v)
    except Exception as e:
        raise OrderFailed(f"{name} must be numeric: {e} (STRICT)") from e
    if not math.isfinite(f):
        raise OrderFailed(f"{name} must be finite (STRICT)")
    return float(f)


def _require_int(v: Any, name: str) -> int:
    if v is None:
        raise OrderFailed(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        raise OrderFailed(f"{name} must be int (bool not allowed) (STRICT)")
    try:
        i = int(v)
    except Exception as e:
        raise OrderFailed(f"{name} must be int: {e} (STRICT)") from e
    return i


def _require_tzaware_dt(v: Any, name: str) -> datetime:
    if not isinstance(v, datetime):
        raise OrderFailed(f"{name} must be datetime (STRICT)")
    if v.tzinfo is None or v.tzinfo.utcoffset(v) is None:
        raise OrderFailed(f"{name} must be timezone-aware (STRICT)")
    return v


def _require_tz_aware_datetime(v: Any, name: str) -> datetime:
    """
    Compatibility alias.
    기존 호출부/이전 코드와의 naming drift를 막기 위해 유지한다.
    """
    return _require_tzaware_dt(v, name)


def _epoch_ms_to_dt_utc(ms: int) -> datetime:
    if not isinstance(ms, int) or ms <= 0:
        raise OrderFailed("epoch ms must be int > 0 (STRICT)")
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def _dt_to_epoch_ms_utc(dt: datetime) -> int:
    dt2 = _require_tzaware_dt(dt, "dt")
    ms = int(dt2.timestamp() * 1000)
    if ms <= 0:
        raise OrderFailed("dt timestamp invalid (STRICT)")
    return ms


def _normalize_symbol(v: Any, name: str) -> str:
    s = _require_nonempty_str(v, name).replace("-", "").replace("/", "").upper().strip()
    if not s:
        raise OrderFailed(f"{name} normalized empty (STRICT)")
    return s


def _normalize_position_side(v: Any, name: str) -> str:
    s = str(v or "").upper().strip()
    if not s:
        raise OrderFailed(f"{name} is required (STRICT)")
    if s not in ("BOTH", "LONG", "SHORT"):
        raise OrderFailed(f"{name} must be BOTH/LONG/SHORT (STRICT), got={s!r}")
    return s


# ─────────────────────────────────────────────────────────────
# Trade (in-memory)
# ─────────────────────────────────────────────────────────────
@dataclass
class Trade:
    symbol: str
    side: str  # BUY/SELL 또는 LONG/SHORT
    qty: float
    entry_price: float
    leverage: int
    source: str = "GPT"

    tp_price: float = 0.0
    sl_price: float = 0.0
    ts: float = 0.0

    entry_order_id: Optional[str] = None
    tp_order_id: Optional[str] = None
    sl_order_id: Optional[str] = None
    close_order_id: Optional[str] = None

    client_entry_id: Optional[str] = None
    id: int = 0

    # legacy alias
    entry: float = 0.0

    # lifecycle (상태머신)
    lifecycle_state: TradeLifecycleState = TradeLifecycleState.ENTERED
    last_state_change_reason: str = ""
    last_state_change_at: Optional[datetime] = None

    # close state
    is_open: bool = True
    close_reason: str = ""
    close_ts: float = 0.0

    # 운영형 실행/복구 필드 (STRICT: 기본값 자동 보정 금지)
    exchange_position_side: str = ""  # BOTH / LONG / SHORT (필수)
    remaining_qty: float = 0.0        # 필수(>0)
    realized_pnl_usdt: float = 0.0    # 필수(초기 0 가능, 음수 가능)
    reconciliation_status: str = "INIT"
    last_synced_at: Optional[datetime] = None  # 필수(tz-aware)

    def __post_init__(self) -> None:
        self.symbol = _require_nonempty_str(self.symbol, "trade.symbol").upper()

        if self.qty <= 0 or not math.isfinite(float(self.qty)):
            raise OrderFailed("trade.qty must be finite > 0 (STRICT)")
        if self.entry_price <= 0 or not math.isfinite(float(self.entry_price)):
            raise OrderFailed("trade.entry_price must be finite > 0 (STRICT)")
        if int(self.leverage) < 1:
            raise OrderFailed("trade.leverage must be >= 1 (STRICT)")

        if self.entry_price > 0 and self.entry == 0:
            self.entry = float(self.entry_price)
        if self.entry > 0 and self.entry_price == 0:
            self.entry_price = float(self.entry)

        ps = str(self.exchange_position_side or "").upper().strip()
        if ps not in ("BOTH", "LONG", "SHORT"):
            raise OrderFailed(f"trade.exchange_position_side must be BOTH/LONG/SHORT (STRICT), got={ps!r}")
        self.exchange_position_side = ps

        if self.remaining_qty <= 0 or not math.isfinite(float(self.remaining_qty)):
            raise OrderFailed("trade.remaining_qty must be finite > 0 (STRICT)")
        if float(self.remaining_qty) - float(self.qty) > 1e-12:
            raise OrderFailed("trade.remaining_qty must be <= trade.qty (STRICT)")

        if not math.isfinite(float(self.realized_pnl_usdt)):
            raise OrderFailed("trade.realized_pnl_usdt must be finite (STRICT)")

        if self.last_synced_at is None:
            raise OrderFailed("trade.last_synced_at is required (STRICT)")
        _require_tzaware_dt(self.last_synced_at, "trade.last_synced_at")

        st = get_state(self)
        if self.is_open and st != TradeLifecycleState.ENTERED:
            raise StateViolation(f"open Trade must be ENTERED, got {st.value}")

    def calculate_pnl(self, current_price: float) -> float:
        cp = _require_float(current_price, "current_price")
        side_u = str(self.side).upper().strip()
        if side_u in ("BUY", "LONG"):
            return (cp - float(self.entry_price)) * float(self.qty)
        if side_u in ("SELL", "SHORT"):
            return (float(self.entry_price) - cp) * float(self.qty)
        raise OrderFailed(f"invalid trade.side: {side_u!r} (STRICT)")

    def calculate_pnl_pct(self, current_price: float) -> float:
        cp = _require_float(current_price, "current_price")
        ep = float(self.entry_price)
        if ep <= 0:
            raise OrderFailed("entry_price must be > 0 (STRICT)")
        side_u = str(self.side).upper().strip()
        if side_u in ("BUY", "LONG"):
            return (cp - ep) / ep * 100.0
        if side_u in ("SELL", "SHORT"):
            return (ep - cp) / ep * 100.0
        raise OrderFailed(f"invalid trade.side: {side_u!r} (STRICT)")


class TraderState:
    def __init__(self) -> None:
        self.tp_sl_reset_failures: int = 0

    def record_tp_sl_reset_failure(self) -> None:
        self.tp_sl_reset_failures += 1

    def reset_tp_sl_failures(self) -> None:
        self.tp_sl_reset_failures = 0

    def should_stop_bot(self) -> bool:
        if not hasattr(SET, "max_tp_sl_reset_failures"):
            raise OrderFailed("settings.max_tp_sl_reset_failures missing (STRICT)")
        max_fail = int(getattr(SET, "max_tp_sl_reset_failures"))
        if max_fail < 0:
            raise OrderFailed("settings.max_tp_sl_reset_failures must be >= 0 (STRICT)")

        if self.tp_sl_reset_failures >= max_fail:
            _submit_tg_nonblocking(
                "🚫 TP/SL 재설정 실패가 누적되었습니다.\n"
                f"- 실패 횟수: {self.tp_sl_reset_failures}\n"
                f"- 허용 한도: {max_fail}\n"
                "STRICT 모드: 운영자가 즉시 확인하세요."
            )
            return True
        return False


# ─────────────────────────────────────────────────────────────
# Account WS Position Snapshot (STRICT)
# ─────────────────────────────────────────────────────────────
def _fetch_account_position_rows_strict(symbol: str) -> List[Dict[str, Any]]:
    sym = _normalize_symbol(symbol, "symbol")
    rows = get_account_positions_snapshot(sym)
    if not isinstance(rows, list):
        raise OrderFailed("account ws positions snapshot must be list (STRICT)")
    if not rows:
        raise OrderFailed(f"account ws position snapshot empty: symbol={sym} (STRICT)")

    out: List[Dict[str, Any]] = []
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            raise OrderFailed(f"account ws position row[{i}] must be dict (STRICT)")
        row_sym = _normalize_symbol(row.get("symbol"), f"account_ws.position[{i}].symbol")
        if row_sym != sym:
            raise OrderFailed(
                f"account ws position symbol mismatch (STRICT): got={row_sym} expected={sym}"
            )

        pos_side = _normalize_position_side(row.get("position_side"), f"account_ws.position[{i}].position_side")
        pos_amt = _require_float(row.get("position_amt"), f"account_ws.position[{i}].position_amt")
        entry_price = _require_float(row.get("entry_price"), f"account_ws.position[{i}].entry_price")
        _ = _require_float(row.get("unrealized_pnl"), f"account_ws.position[{i}].unrealized_pnl")
        _ = _require_nonempty_str(row.get("margin_type"), f"account_ws.position[{i}].margin_type")

        if abs(pos_amt) > 1e-12 and entry_price <= 0:
            raise OrderFailed("live account ws position entry_price must be > 0 (STRICT)")

        out.append(
            {
                "symbol": row_sym,
                "position_side": pos_side,
                "position_amt": float(pos_amt),
                "entry_price": float(entry_price),
                "unrealized_pnl": float(row["unrealized_pnl"]),
                "margin_type": str(row["margin_type"]).upper(),
                "event_time_ms": _require_int(row.get("event_time_ms"), f"account_ws.position[{i}].event_time_ms"),
                "transaction_time_ms": _require_int(
                    row.get("transaction_time_ms"),
                    f"account_ws.position[{i}].transaction_time_ms",
                ),
            }
        )

    return out


def _trade_direction_strict(trade: Trade) -> str:
    side = str(trade.side or "").upper().strip()
    if side in ("BUY", "LONG"):
        return "LONG"
    if side in ("SELL", "SHORT"):
        return "SHORT"
    raise CloseSummaryError(f"invalid trade.side: {side!r} (STRICT)")


def _pick_position_row_for_trade(rows: List[Dict[str, Any]], trade: Trade) -> Dict[str, Any]:
    if not rows:
        raise CloseSummaryError("position rows empty (STRICT)")

    wanted = _trade_direction_strict(trade)

    matches = [r for r in rows if str(r.get("position_side") or "").upper() == wanted]
    if len(matches) == 1:
        row = matches[0]
    elif len(matches) > 1:
        raise CloseSummaryError("multiple position rows match position_side (ambiguous) (STRICT)")
    else:
        both = [r for r in rows if str(r.get("position_side") or "").upper() == "BOTH"]
        if len(both) == 1:
            row = both[0]
        elif len(both) > 1:
            raise CloseSummaryError("multiple BOTH position rows (ambiguous) (STRICT)")
        else:
            raise CloseSummaryError("cannot select position row for trade (STRICT)")

    pos_amt = _float_strict(row.get("position_amt"), "position.position_amt")
    if wanted == "LONG" and pos_amt < 0:
        raise CloseSummaryError("selected position row direction mismatch: expected LONG (STRICT)")
    if wanted == "SHORT" and pos_amt > 0:
        raise CloseSummaryError("selected position row direction mismatch: expected SHORT (STRICT)")
    return row


# ─────────────────────────────────────────────────────────────
# Slippage (legacy 유지)
# ─────────────────────────────────────────────────────────────
def evaluate_slippage(
    expected_entry: float,
    actual_entry: float,
    tick_size: float,
    max_ticks: int,
    max_notional_pct: float,
) -> Tuple[bool, str]:
    exp = _require_float(expected_entry, "expected_entry")
    act = _require_float(actual_entry, "actual_entry")

    tick = _require_float(tick_size, "tick_size")
    if tick <= 0:
        raise OrderFailed("tick_size must be > 0 (STRICT)")

    if not isinstance(max_ticks, int) or max_ticks <= 0:
        raise OrderFailed("max_ticks must be int > 0 (STRICT)")

    mnp = _require_float(max_notional_pct, "max_notional_pct")
    if mnp <= 0:
        raise OrderFailed("max_notional_pct must be > 0 (STRICT)")

    diff = abs(act - exp)
    ticks = int(diff / tick)

    if exp <= 0:
        raise OrderFailed("expected_entry must be > 0 (STRICT)")

    notional_pct = (diff / exp) * 100.0

    if ticks > max_ticks:
        return False, f"slippage too large: ticks={ticks} > max_ticks={max_ticks}"
    if notional_pct > mnp:
        return False, (
            f"slippage too large: notional_pct={notional_pct:.4f}% "
            f"> max_notional_pct={mnp:.4f}%"
        )
    return True, "OK"


def check_entry_slippage_once(trade: Trade, entry_price: float) -> bool:
    from execution.order_executor import close_position_market, get_symbol_filters

    sym = _normalize_symbol(trade.symbol, "trade.symbol")
    expected_entry = _require_float(entry_price, "entry_price")

    filters = get_symbol_filters(sym)

    tick_size_val: Any = None
    if hasattr(filters, "tick_size"):
        tick_size_val = getattr(filters, "tick_size")
    elif isinstance(filters, dict):
        tick_size_val = filters.get("tickSize") or filters.get("tick_size")

    if tick_size_val is None:
        raise OrderFailed(f"tick_size not found in symbol filters: symbol={sym} (STRICT)")

    tick_size = _require_float(tick_size_val, "tick_size")
    if tick_size <= 0:
        raise OrderFailed(f"tick_size must be > 0: symbol={sym}, value={tick_size} (STRICT)")

    if not hasattr(SET, "slippage_max_ticks"):
        raise OrderFailed("settings.slippage_max_ticks missing (STRICT)")
    if not hasattr(SET, "slippage_max_notional_pct"):
        raise OrderFailed("settings.slippage_max_notional_pct missing (STRICT)")
    if not hasattr(SET, "stop_on_heavy_slippage"):
        raise OrderFailed("settings.stop_on_heavy_slippage missing (STRICT)")

    max_ticks = int(getattr(SET, "slippage_max_ticks"))
    max_notional_pct = float(getattr(SET, "slippage_max_notional_pct"))
    stop_on_heavy = bool(getattr(SET, "stop_on_heavy_slippage"))

    pos_rows = _fetch_account_position_rows_strict(sym)
    pos_row = _pick_position_row_for_trade(pos_rows, trade)

    actual_entry = _require_float(pos_row.get("entry_price"), "actual_entry_price")
    if actual_entry <= 0:
        raise OrderFailed("actual_entry_price must be > 0 (STRICT)")

    ok, reason = evaluate_slippage(
        expected_entry=expected_entry,
        actual_entry=actual_entry,
        tick_size=float(tick_size),
        max_ticks=max_ticks,
        max_notional_pct=max_notional_pct,
    )
    if ok:
        return True

    msg = (
        "⚠️ 진입 슬리피지 경고\n"
        f"- 심볼: {sym}\n"
        f"- 기대 진입가: {expected_entry}\n"
        f"- 실제 진입가: {actual_entry}\n"
        f"- 사유: {reason}"
    )
    _submit_tg_nonblocking(msg)
    log(f"[SLIPPAGE] {msg}")

    if stop_on_heavy:
        side_u = str(trade.side).upper().strip()
        if side_u == "LONG":
            side_u = "BUY"
        elif side_u == "SHORT":
            side_u = "SELL"
        _submit_tg_nonblocking("🛑 설정(stop_on_heavy_slippage=True)에 따라 즉시 시장가로 포지션을 정리합니다.")
        close_position_market(symbol=sym, side_open=side_u, qty=float(trade.remaining_qty))

    return False


# ─────────────────────────────────────────────────────────────
# Close summary (STRICT)
# ─────────────────────────────────────────────────────────────
class CloseSummaryError(SyncMismatch):
    """거래소 확정값으로 청산 요약을 만들 수 없을 때 발생."""


def _float_strict(v: Any, name: str) -> float:
    if v is None:
        raise CloseSummaryError(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        raise CloseSummaryError(f"{name} must be numeric (bool not allowed) (STRICT)")
    try:
        f = float(v)
    except Exception as e:
        raise CloseSummaryError(f"{name} must be numeric: {e} (STRICT)") from e
    if not math.isfinite(f):
        raise CloseSummaryError(f"{name} must be finite (STRICT)")
    return float(f)


def _int_strict(v: Any, name: str) -> int:
    if v is None:
        raise CloseSummaryError(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        raise CloseSummaryError(f"{name} must be int (bool not allowed) (STRICT)")
    try:
        return int(v)
    except Exception as e:
        raise CloseSummaryError(f"{name} must be int: {e} (STRICT)") from e


def _trade_entry_side_db(trade: Trade) -> str:
    side = str(trade.side or "").upper().strip()
    if side in ("BUY", "SELL"):
        return side
    if side == "LONG":
        return "BUY"
    if side == "SHORT":
        return "SELL"
    raise CloseSummaryError(f"invalid trade.side: {side!r} (STRICT)")


def _close_side_for_trade(trade: Trade) -> str:
    side_u = str(trade.side or "").upper().strip()
    if side_u in ("BUY", "LONG"):
        return "SELL"
    if side_u in ("SELL", "SHORT"):
        return "BUY"
    raise CloseSummaryError(f"invalid trade.side: {side_u!r} (STRICT)")


def _fetch_order_strict(symbol: str, order_id: str) -> Dict[str, Any]:
    sym = _require_nonempty_str(symbol, "symbol").upper()
    oid = _int_strict(order_id, "order_id")
    data = req("GET", "/fapi/v1/order", params={"symbol": sym, "orderId": oid}, private=True)
    if not isinstance(data, dict):
        raise CloseSummaryError("order response must be dict (STRICT)")
    return data


def _fetch_user_trades_strict(symbol: str, *, start_ms: int, end_ms: int, limit: int = 1000) -> List[Dict[str, Any]]:
    sym = _require_nonempty_str(symbol, "symbol").upper()
    if start_ms <= 0 or end_ms <= 0 or end_ms < start_ms:
        raise CloseSummaryError("invalid userTrades time range (STRICT)")
    if not isinstance(limit, int) or limit <= 0 or limit > 1000:
        raise CloseSummaryError("userTrades limit must be int in [1, 1000] (STRICT)")

    data = req(
        "GET",
        "/fapi/v1/userTrades",
        params={"symbol": sym, "startTime": int(start_ms), "endTime": int(end_ms), "limit": int(limit)},
        private=True,
    )
    if not isinstance(data, list):
        raise CloseSummaryError("userTrades response must be list (STRICT)")
    return [r for r in data if isinstance(r, dict)]


def _summarize_user_trades_for_order(trades: List[Dict[str, Any]], *, order_id: int) -> Dict[str, Any]:
    qty_sum = 0.0
    px_qty_sum = 0.0
    realized_sum = 0.0
    fee_usdt_sum = 0.0
    last_time_ms: Optional[int] = None

    for r in trades:
        oid = r.get("orderId")
        if oid is None:
            continue
        if int(oid) != int(order_id):
            continue

        qty = _float_strict(r.get("qty"), "userTrade.qty")
        price = _float_strict(r.get("price"), "userTrade.price")
        realized = _float_strict(r.get("realizedPnl"), "userTrade.realizedPnl")
        commission = _float_strict(r.get("commission"), "userTrade.commission")
        commission_asset = str(r.get("commissionAsset") or "").upper()

        t_ms = r.get("time")
        t_ms_i = int(t_ms) if t_ms is not None else None

        if qty <= 0 or price <= 0:
            raise CloseSummaryError("userTrade.qty/price must be > 0 (STRICT)")

        qty_sum += qty
        px_qty_sum += price * qty
        realized_sum += realized
        if commission_asset == "USDT":
            fee_usdt_sum += commission

        if t_ms_i is not None:
            if last_time_ms is None or t_ms_i > last_time_ms:
                last_time_ms = t_ms_i

    if qty_sum <= 0:
        raise CloseSummaryError("no fills for orderId (STRICT)")

    if last_time_ms is None or last_time_ms <= 0:
        raise CloseSummaryError("userTrades missing time for close (STRICT)")

    avg_price = px_qty_sum / qty_sum
    if avg_price <= 0 or not math.isfinite(avg_price):
        raise CloseSummaryError("avg_price invalid (STRICT)")

    pnl_net = realized_sum - fee_usdt_sum
    out: Dict[str, Any] = {
        "qty": float(qty_sum),
        "avg_price": float(avg_price),
        "pnl": float(pnl_net),
        "fee": float(fee_usdt_sum),
        "realizedPnl": float(realized_sum),
        "close_time": int(last_time_ms),
    }
    return out


def _group_user_trades_by_order_id(trades: List[Dict[str, Any]], *, side: str) -> Dict[int, List[Dict[str, Any]]]:
    side_u = str(side).upper().strip()
    out: Dict[int, List[Dict[str, Any]]] = {}
    for r in trades:
        if str(r.get("side") or "").upper().strip() != side_u:
            continue
        oid = r.get("orderId")
        if oid is None:
            continue
        out.setdefault(int(oid), []).append(r)
    return out


def _infer_unique_close_order_id_strict(
    trade: Trade,
    trades_window: List[Dict[str, Any]],
    *,
    expected_qty: float,
    tolerance_ratio: float,
) -> Tuple[int, Dict[str, Any]]:
    close_side = _close_side_for_trade(trade)
    groups = _group_user_trades_by_order_id(trades_window, side=close_side)

    if expected_qty <= 0 or not math.isfinite(expected_qty):
        raise CloseSummaryError("expected_qty invalid (STRICT)")

    if tolerance_ratio <= 0 or not math.isfinite(tolerance_ratio):
        raise CloseSummaryError("tolerance_ratio must be finite > 0 (STRICT)")

    tol = abs(expected_qty) * float(tolerance_ratio)
    if tol <= 0:
        raise CloseSummaryError("tolerance invalid (STRICT)")

    candidates: List[Tuple[int, Dict[str, Any]]] = []
    for oid in groups.keys():
        summary = _summarize_user_trades_for_order(trades_window, order_id=int(oid))
        qty = _float_strict(summary.get("qty"), "summary.qty")
        if abs(qty - expected_qty) <= tol:
            candidates.append((int(oid), summary))

    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) == 0:
        raise CloseSummaryError("cannot infer close orderId uniquely (no candidates) (STRICT)")
    raise CloseSummaryError("cannot infer close orderId uniquely (multiple candidates) (STRICT)")


def build_close_summary_strict(
    trade: Trade,
    *,
    now_ms: Optional[int] = None,
    lookback_sec: int = 6 * 3600,
) -> Tuple[str, Dict[str, Any]]:
    sym = _require_nonempty_str(trade.symbol, "trade.symbol").upper()
    observed_at_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    if observed_at_ms <= 0:
        raise CloseSummaryError("now_ms invalid (STRICT)")
    if lookback_sec <= 0:
        raise CloseSummaryError("lookback_sec must be > 0 (STRICT)")

    tp_id = str(trade.tp_order_id or "").strip()
    sl_id = str(trade.sl_order_id or "").strip()
    close_id = str(trade.close_order_id or "").strip()

    filled: List[Tuple[str, Dict[str, Any]]] = []
    for oid in (tp_id, sl_id):
        if not oid:
            continue
        o = _fetch_order_strict(sym, oid)
        status = str(o.get("status") or "").upper()
        if status == "FILLED":
            filled.append((oid, o))

    if len(filled) == 1:
        close_oid, close_order = filled[0]
        reason = "TP" if close_oid == tp_id else "SL"

        close_time_ms: Optional[int] = None
        for k in ("updateTime", "time", "transactTime"):
            if close_order.get(k) is not None:
                close_time_ms = int(close_order.get(k))
                break
        if close_time_ms is None or close_time_ms <= 0:
            raise CloseSummaryError("close order time missing (STRICT)")

        win_ms = 2 * 3600 * 1000
        start_ms = max(1, close_time_ms - win_ms)
        end_ms = close_time_ms + win_ms
        user_trades = _fetch_user_trades_strict(sym, start_ms=start_ms, end_ms=end_ms)
        summary = _summarize_user_trades_for_order(user_trades, order_id=_int_strict(close_oid, "close_order_id"))
        summary["observed_at_ms"] = int(observed_at_ms)
        return reason, summary

    if len(filled) > 1:
        raise CloseSummaryError("multiple close orders FILLED (ambiguous) (STRICT)")

    if close_id:
        o = _fetch_order_strict(sym, close_id)
        status = str(o.get("status") or "").upper()
        if status != "FILLED":
            raise CloseSummaryError(f"close_order_id present but not FILLED (status={status}) (STRICT)")

        close_time_ms: Optional[int] = None
        for k in ("updateTime", "time", "transactTime"):
            if o.get(k) is not None:
                close_time_ms = int(o.get(k))
                break
        if close_time_ms is None or close_time_ms <= 0:
            raise CloseSummaryError("manual close order time missing (STRICT)")

        win_ms = 2 * 3600 * 1000
        start_ms = max(1, close_time_ms - win_ms)
        end_ms = close_time_ms + win_ms
        user_trades = _fetch_user_trades_strict(sym, start_ms=start_ms, end_ms=end_ms)
        summary = _summarize_user_trades_for_order(user_trades, order_id=_int_strict(close_id, "close_order_id"))
        summary["observed_at_ms"] = int(observed_at_ms)
        return "MANUAL", summary

    expected_qty = _require_float(trade.remaining_qty, "trade.remaining_qty")
    start_ms = max(1, observed_at_ms - int(lookback_sec * 1000))
    end_ms = observed_at_ms
    user_trades = _fetch_user_trades_strict(sym, start_ms=start_ms, end_ms=end_ms)

    oid, summary = _infer_unique_close_order_id_strict(
        trade,
        user_trades,
        expected_qty=float(expected_qty),
        tolerance_ratio=0.01,
    )
    trade.close_order_id = str(oid)
    summary["observed_at_ms"] = int(observed_at_ms)
    return "MANUAL", summary


def _calc_pnl_pct_futures(trade: Trade, pnl_usdt: float) -> Optional[float]:
    entry_price = float(trade.entry_price)
    qty = float(trade.qty)
    if entry_price <= 0 or qty <= 0:
        return None
    notional = abs(entry_price * qty)
    if notional <= 0:
        return None
    return float(pnl_usdt) / notional * 100.0


def _persist_partial_progress_to_db_strict(
    *,
    trade: Trade,
    synced_remaining_qty: float,
    realized_pnl_total_usdt: float,
    synced_at: datetime,
) -> None:
    sym = _require_nonempty_str(trade.symbol, "trade.symbol").upper()
    entry_side_db = _trade_entry_side_db(trade)
    rem = _require_float(synced_remaining_qty, "synced_remaining_qty")
    if rem <= 0:
        raise CloseSummaryError("synced_remaining_qty must be > 0 (STRICT)")
    rpnl_total = _require_float(realized_pnl_total_usdt, "realized_pnl_total_usdt")
    synced_at_dt = _require_tzaware_dt(synced_at, "synced_at")

    with get_session() as session:
        row = None
        if int(getattr(trade, "id", 0) or 0) > 0:
            row = (
                session.query(TradeORM)
                .filter(TradeORM.id == int(trade.id))
                .filter(TradeORM.exit_ts.is_(None))
                .first()
            )

        if row is None:
            row = (
                session.query(TradeORM)
                .filter(TradeORM.symbol == sym)
                .filter(TradeORM.side == entry_side_db)
                .filter(TradeORM.exit_ts.is_(None))
                .order_by(TradeORM.entry_ts.desc())
                .first()
            )

        if row is None:
            raise CloseSummaryError(f"open trade row not found for partial progress: symbol={sym} side={entry_side_db}")

        row.remaining_qty = float(rem)
        row.realized_pnl_usdt = float(rpnl_total)
        row.reconciliation_status = "PROTECTION_VERIFIED"
        row.last_synced_at = synced_at_dt
        if hasattr(type(row), "updated_at"):
            row.updated_at = synced_at_dt
        session.flush()


def _persist_close_to_db_strict(*, trade: Trade, reason: str, summary: Dict[str, Any]) -> int:
    sym = _require_nonempty_str(trade.symbol, "trade.symbol").upper()
    entry_side_db = _trade_entry_side_db(trade)

    if not isinstance(summary, dict):
        raise CloseSummaryError("summary must be dict (STRICT)")
    for k in ("avg_price", "pnl", "close_time"):
        if k not in summary:
            raise CloseSummaryError(f"summary missing required key: {k} (STRICT)")

    exit_price = _float_strict(summary.get("avg_price"), "summary.avg_price")
    final_slice_pnl_usdt = _float_strict(summary.get("pnl"), "summary.pnl")
    if exit_price <= 0:
        raise CloseSummaryError("summary.avg_price must be > 0 (STRICT)")

    prior_realized_pnl_usdt = _require_float(trade.realized_pnl_usdt, "trade.realized_pnl_usdt")
    total_pnl_usdt = float(prior_realized_pnl_usdt + final_slice_pnl_usdt)

    close_time_ms = _int_strict(summary.get("close_time"), "summary.close_time")
    exit_ts_dt = _epoch_ms_to_dt_utc(close_time_ms)

    close_reason = _require_nonempty_str(reason, "reason")[:32]
    pnl_pct_futures = _calc_pnl_pct_futures(trade, total_pnl_usdt)

    trade_id = close_latest_open_trade_returning_id(
        symbol=sym,
        side=entry_side_db,
        exit_price=float(exit_price),
        exit_ts=exit_ts_dt,
        pnl_usdt=float(total_pnl_usdt),
        close_reason=str(close_reason),
        regime_at_exit=None,
        pnl_pct_futures=pnl_pct_futures,
        pnl_pct_spot_ref=None,
        note=None,
    )

    record_trade_exit_snapshot(
        trade_id=int(trade_id),
        symbol=sym,
        close_ts=exit_ts_dt,
        close_price=float(exit_price),
        pnl=float(total_pnl_usdt),
        close_reason=str(close_reason),
        exit_atr_pct=None,
        exit_trend_strength=None,
        exit_volume_zscore=None,
        exit_depth_ratio=None,
    )

    return int(trade_id)


def _strict_reconcile_partial_close(
    trade: Trade,
    *,
    symbol: str,
    delta_qty: float,
    start_ms: int,
    end_ms: int,
) -> Tuple[float, int]:
    if delta_qty <= 0 or not math.isfinite(delta_qty):
        raise CloseSummaryError("delta_qty invalid (STRICT)")

    trades_window = _fetch_user_trades_strict(symbol, start_ms=start_ms, end_ms=end_ms)
    close_side = _close_side_for_trade(trade)

    qty_sum = 0.0
    realized_sum = 0.0
    fee_usdt_sum = 0.0
    last_time_ms: Optional[int] = None

    for r in trades_window:
        if str(r.get("side") or "").upper().strip() != close_side:
            continue

        qty = _float_strict(r.get("qty"), "userTrade.qty")
        realized = _float_strict(r.get("realizedPnl"), "userTrade.realizedPnl")
        commission = _float_strict(r.get("commission"), "userTrade.commission")
        commission_asset = str(r.get("commissionAsset") or "").upper()

        if qty <= 0:
            raise CloseSummaryError("userTrade.qty must be > 0 (STRICT)")

        t_ms = r.get("time")
        t_ms_i = int(t_ms) if t_ms is not None else None
        if t_ms_i is not None and (last_time_ms is None or t_ms_i > last_time_ms):
            last_time_ms = t_ms_i

        qty_sum += qty
        realized_sum += realized
        if commission_asset == "USDT":
            fee_usdt_sum += commission

    tol = max(delta_qty * 0.01, 1e-12)
    if abs(qty_sum - delta_qty) > tol:
        raise CloseSummaryError(f"partial close qty mismatch: expected_delta={delta_qty}, got={qty_sum} (STRICT)")

    if last_time_ms is None or last_time_ms <= 0:
        raise CloseSummaryError("partial close trades missing close_time (STRICT)")

    pnl_delta = realized_sum - fee_usdt_sum
    if not math.isfinite(pnl_delta):
        raise CloseSummaryError("partial pnl_delta not finite (STRICT)")
    return float(pnl_delta), int(last_time_ms)


# ─────────────────────────────────────────────────────────────
# Main close checker
# ─────────────────────────────────────────────────────────────
def check_closes(open_trades: List[Trade], trader_state: TraderState) -> Tuple[List[Trade], List[Dict[str, Any]]]:
    _ = trader_state

    if not open_trades:
        return [], []

    if len(open_trades) != 1:
        raise StateViolation("STRICT: multiple open_trades is not supported (single-trade only)")

    t = open_trades[0]

    if get_state(t) != TradeLifecycleState.ENTERED:
        raise StateViolation(f"check_closes requires ENTERED state, got {get_state(t).value}")

    symbol = _require_nonempty_str(t.symbol, "open_trades[0].symbol").upper()

    pos_rows = _fetch_account_position_rows_strict(symbol)
    row = _pick_position_row_for_trade(pos_rows, t)

    pos_qty = _float_strict(row.get("position_amt"), "position.position_amt")
    abs_qty = abs(pos_qty)

    eps = 1e-12

    prev_remaining = _require_float(t.remaining_qty, "trade.remaining_qty")
    if prev_remaining <= 0:
        raise CloseSummaryError("trade.remaining_qty must be > 0 (STRICT)")

    if abs_qty > prev_remaining + eps:
        raise CloseSummaryError(
            f"position size increased unexpectedly (STRICT): prev_remaining={prev_remaining} now_abs_qty={abs_qty}"
        )

    if abs_qty > eps and abs_qty < prev_remaining - eps:
        delta = prev_remaining - abs_qty

        if t.last_synced_at is None:
            raise CloseSummaryError("trade.last_synced_at is required for partial reconciliation (STRICT)")

        start_ms = _dt_to_epoch_ms_utc(t.last_synced_at) - 2000
        end_ms = int(row.get("transaction_time_ms"))
        if end_ms <= 0:
            raise CloseSummaryError("account_ws.position.transaction_time_ms invalid for partial reconciliation (STRICT)")
        end_ms = end_ms + 2000
        if start_ms <= 0:
            raise CloseSummaryError("partial reconciliation start_ms invalid (STRICT)")
        if end_ms < start_ms:
            raise CloseSummaryError("partial reconciliation end_ms < start_ms (STRICT)")

        pnl_delta, close_time_ms = _strict_reconcile_partial_close(
            t,
            symbol=symbol,
            delta_qty=float(delta),
            start_ms=int(start_ms),
            end_ms=int(end_ms),
        )

        synced_at_dt = _epoch_ms_to_dt_utc(close_time_ms)
        realized_total = float(_require_float(t.realized_pnl_usdt, "trade.realized_pnl_usdt") + pnl_delta)

        t.realized_pnl_usdt = realized_total
        t.remaining_qty = float(abs_qty)
        t.last_synced_at = synced_at_dt
        t.reconciliation_status = "PROTECTION_VERIFIED"

        _persist_partial_progress_to_db_strict(
            trade=t,
            synced_remaining_qty=float(abs_qty),
            realized_pnl_total_usdt=float(realized_total),
            synced_at=synced_at_dt,
        )

        _submit_tg_nonblocking(
            f"[PARTIAL_CLOSE] {symbol} side={t.side} "
            f"delta={delta:.6f} remaining={t.remaining_qty:.6f} pnl_delta={pnl_delta:.4f} pnl_total={t.realized_pnl_usdt:.4f}"
        )

        return [t], []

    if abs_qty < eps:
        reason, summary = build_close_summary_strict(t)

        total_pnl_usdt = float(_require_float(t.realized_pnl_usdt, "trade.realized_pnl_usdt") + _float_strict(summary.get("pnl"), "summary.pnl"))
        trade_id = _persist_close_to_db_strict(trade=t, reason=str(reason), summary=summary)
        if t.id <= 0:
            t.id = int(trade_id)

        apply_event(t, TradeEvent.SYNC_SET_CLOSED, reason="exchange_position_zero")

        t.is_open = False
        t.close_reason = str(reason)

        close_time_ms = _int_strict(summary.get("close_time"), "summary.close_time")
        close_dt = _epoch_ms_to_dt_utc(close_time_ms)
        t.close_ts = close_dt.timestamp()

        t.remaining_qty = 0.0
        t.realized_pnl_usdt = float(total_pnl_usdt)
        t.reconciliation_status = "CLOSED"
        t.last_synced_at = close_dt

        return [], [{"trade": t, "reason": reason, "summary": summary}]

    return [t], []


__all__ = [
    "Trade",
    "TraderState",
    "evaluate_slippage",
    "check_entry_slippage_once",
    "build_close_summary_strict",
    "check_closes",
]