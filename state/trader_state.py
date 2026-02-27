from __future__ import annotations

"""
trader_state.py
====================================================
포지션 엔진 (WS 버전)
----------------------------------------------------
- BingX Auto Trader에서 실제 포지션 상태를 나타내는 Trade 객체와
  TP/SL 체결 여부를 확인하는 check_closes 함수를 제공한다.
- GPT 의사결정(gpt_decider.py)은 이 파일을 직접 사용하지 않고,
  run_bot_ws / entry_flow / position_watch_ws 가 이 모듈을 통해
  "현재 열린 포지션 리스트"만 관리한다.

2025-11-20 변경 사항 (엔진 복구 + 슬리피지 가드 유틸 추가)
----------------------------------------------------
1) 기존 GPT 트레이더 로직(trader.py → gpt_trader.py 역할)을 분리하고,
   이 파일을 원래처럼 포지션 엔진(Trade / TraderState / check_closes) 전용으로 사용한다.
2) '진입 시 20틱 이상 비싸게 체결되는' 이상 체결을 감지할 수 있도록
   슬리피지 체크 유틸(check_entry_slippage_once)과 계산 함수(evaluate_slippage)를 추가했다.
   - tick_size, 허용 틱 수, 허용 퍼센트는 환경변수/설정으로 조정 가능:
       · PRICE_TICK_SIZE (기본 0.1)
       · MAX_ENTRY_SLIPPAGE_TICKS (기본 20)
       · MAX_ENTRY_SLIPPAGE_PCT   (기본 0.0015 ≒ 0.15%)
   - 슬리피지 경고만 보내거나, 심각한 경우 봇을 멈추도록 설정 가능:
       · settings_ws.BotSettings.stop_on_heavy_slippage (기본 False)
3) check_closes 는 BingX 측 open position 스냅샷을 기준으로
   '더 이상 열려 있지 않은' 포지션을 CLOSED 이벤트로 돌려준다.
   - 청산가는 WS 1분봉 현재가로 근사 계산한다.
   - TP/SL 근처에서 닫힌 경우 reason 을 TP/SL 로 추정한다.

2025-11-21 변경 사항 (Trade.entry 필드 추가 + entry_price 동기화 + entry_order_id 지원)
----------------------------------------------------
1) DB/EXIT 레이어에서 사용하는 trade.entry 필드를 Trade dataclass 에 정식 필드로 추가했다.
   - 기존 코드에서 trade.entry 를 getattr(..., "entry", ...) 형식으로 사용하던 부분과,
     open_position_with_tp_sl(...) 가 Trade(..., entry=...) 로 생성하던 부분을 정식 지원.
2) entry_price 와 entry 가 항상 같은 값을 가지도록 __post_init__ 에서 동기화 로직을 추가했다.
   - entry_price 만 설정된 경우 → entry 를 entry_price 로 자동 보정.
   - (향후) entry 만 설정된 경우 → entry_price 를 entry 로 자동 보정.
3) 수동/자동 진입 공통으로 최초 진입 주문 ID 를 저장할 수 있도록 entry_order_id 필드를 추가했다.
   - Trade(..., entry_order_id=...) 형태의 생성자를 정식으로 허용한다.
   - 없으면 None 으로 유지한다.
"""

import math
import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from settings import load_settings
from infra.telelog import log, send_tg

# settings_ws 는 다른 모듈에서도 이미 사용하므로, 여기서도 동일 인스턴스를 써도 무방하다.
SET = load_settings()


# ─────────────────────────────────────────
# Trade 데이터 구조
# ─────────────────────────────────────────


@dataclass
class Trade:
    """현재 열려 있는 포지션 1건을 표현하는 경량 객체.

    - symbol         : "BTC-USDT" 등
    - side           : "BUY"/"SELL" 또는 "LONG"/"SHORT" (내부에서는 LONG/SHORT 방향만 사용)
    - qty            : 계약 수량(기본 단위는 BingX positionAmt 와 동일하게 맞춘다)
    - entry_price    : 평균 진입가 (기존 필드, entry 와 항상 동일하게 유지)
    - leverage       : 레버리지 (PnL 계산에는 직접 사용하지 않지만 참고용으로 유지)
    - source         : 전략/시그널 출처 (예: "MARKET", "BACKTEST", "MANUAL")
    - entry          : DB/EXIT/리포트 레이어에서 사용하는 통합 진입가 필드 (entry_price alias)
    - entry_order_id : 최초 진입 주문의 ID (자동/수동 공통, 있을 때만 사용)
    """

    symbol: str
    side: str
    qty: float
    entry_price: float
    leverage: float
    source: str = "MARKET"

    # SL/TP 레벨 (있을 때만 사용)
    tp_price: Optional[float] = None
    sl_price: Optional[float] = None

    # 기타 메타데이터
    opened_at: float = field(default_factory=time.time)
    position_side: Optional[str] = None  # "LONG"/"SHORT"/"BOTH"
    extra: Dict[str, Any] = field(default_factory=dict)

    # 슬리피지 체크 등에서 동일 포지션을 식별하기 위한 내부 ID
    uid: str = field(
        default_factory=lambda: f"trade-{int(time.time() * 1000)}",
    )

    # 통합 진입가 필드 (백워드 호환용)
    # - 기존 코드: entry_price 만 사용.
    # - GPT EXIT/DB 로깅 코드: trade.entry 를 사용.
    # - __post_init__ 에서 entry 와 entry_price 를 서로 동기화한다.
    entry: float = 0.0
    entry_order_id: Optional[str] = None
    # ★ sync_exchange가 요구하는 필드 2개 추가
    tp_order_id: Optional[str] = None
    sl_order_id: Optional[str] = None


    # 최초 진입 주문 ID (자동/수동 공통).
    # - 수동 진입 시 open_position_with_tp_sl(..., entry_order_id=...) 에서 채워질 수 있다.
    # - 없으면 None 으로 남겨두고, 필요 시 로깅/리포트에서만 사용한다.
    entry_order_id: Optional[str] = None

    def __post_init__(self) -> None:
        """entry_price 와 entry 필드를 상호 동기화한다.

        - 기존 코드에서는 entry_price 만 사용했기 때문에
          entry 가 0 이하이면 entry_price 값을 그대로 복사한다.
        - (향후) 만약 entry 만 설정하고 entry_price 가 0 이하라면
          entry_price 를 entry 값으로 복사한다.
        """
        # dataclass(frozen=False)이므로 object.__setattr__ 로 안전하게 보정
        try:
            ep = float(self.entry_price)
        except Exception:
            ep = 0.0
        try:
            e_val = float(getattr(self, "entry", 0.0) or 0.0)
        except Exception:
            e_val = 0.0

        if e_val <= 0 and ep > 0:
            object.__setattr__(self, "entry", ep)
        elif e_val > 0 and ep <= 0:
            object.__setattr__(self, "entry_price", e_val)

    def direction(self) -> str:
        """포지션 방향을 LONG/SHORT 로 통일."""
        s = str(self.side).upper()
        if s in ("BUY", "LONG"):
            return "LONG"
        if s in ("SELL", "SHORT"):
            return "SHORT"
        # 알 수 없는 경우 수량 부호로 추정
        return "LONG" if self.qty >= 0 else "SHORT"

    def sign(self) -> int:
        """PnL 계산용 방향 부호 (+1: LONG, -1: SHORT)."""
        return 1 if self.direction() == "LONG" else -1


# ─────────────────────────────────────────
# TraderState: TP/SL 재설정 & 슬리피지 상태
# ─────────────────────────────────────────


class TraderState:
    """봇 전체에서 공유하는 포지션 엔진 상태.

    - TP/SL 재설정 실패 누적 관리 (여러 번 실패 시 봇 중단)
    - 심각한 슬리피지 감지 시 봇 중단 플래그 관리
    """

    def __init__(
        self,
        *,
        max_tp_sl_reset_failures: Optional[int] = None,
    ) -> None:
        if max_tp_sl_reset_failures is None:
            # settings 또는 환경변수에서 기본값 가져오기
            max_tp_sl_reset_failures = int(
                getattr(SET, "max_tp_sl_reset_failures", 5)
            )
        self.max_tp_sl_reset_failures: int = max_tp_sl_reset_failures
        self.tp_sl_reset_failures: int = 0
        self.last_error: Optional[str] = None

        # 슬리피지 관련 상태
        self.slippage_checked_ids: set[str] = set()
        self.heavy_slippage_detected: bool = False

    # TP/SL 재설정 실패 관련 ------------------------------

    def register_tp_sl_reset_failure(self, msg: str) -> None:
        """TP/SL 재설정 실패 1회 기록."""
        self.tp_sl_reset_failures += 1
        self.last_error = msg
        log(
            f"[TRADER_STATE] TP/SL 재설정 실패 {self.tp_sl_reset_failures}/{self.max_tp_sl_reset_failures}: {msg}"
        )

    def reset_tp_sl_failures(self) -> None:
        """성공적으로 TP/SL 재설정 시 카운터 초기화."""
        if self.tp_sl_reset_failures:
            log(
                f"[TRADER_STATE] TP/SL 재설정 실패 카운터 초기화 "
                f"({self.tp_sl_reset_failures} → 0)"
            )
        self.tp_sl_reset_failures = 0
        self.last_error = None

    # 슬리피지 관련 --------------------------------------

    def mark_heavy_slippage(self) -> None:
        """심각한 슬리피지 발생 플래그를 세팅."""
        self.heavy_slippage_detected = True
        log("[TRADER_STATE] 심각한 슬리피지 감지: stop_on_heavy_slippage 설정을 확인하세요.")

    def should_stop_bot(self) -> bool:
        """봇을 중단해야 할지 여부를 반환.

        - TP/SL 재설정 실패가 max_tp_sl_reset_failures 이상인 경우
        - stop_on_heavy_slippage 설정이 True 이고, heavy_slippage_detected 가 True 인 경우
        """
        if self.tp_sl_reset_failures >= self.max_tp_sl_reset_failures:
            return True

        stop_on_heavy = bool(getattr(SET, "stop_on_heavy_slippage", False))
        if stop_on_heavy and self.heavy_slippage_detected:
            return True

        return False


# ─────────────────────────────────────────
# 슬리피지 계산/체크 유틸
# ─────────────────────────────────────────


def _get_env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def evaluate_slippage(
    *,
    expected_price: float,
    fill_price: float,
    tick_size: Optional[float] = None,
    max_ticks: Optional[float] = None,
    max_pct: Optional[float] = None,
) -> Dict[str, Any]:
    """슬리피지 크기를 계산하고 허용 범위를 넘었는지 여부를 반환.

    반환 예시:
        {
          "ok": False,
          "diff_abs": 3.5,
          "diff_ticks": 35.0,
          "diff_pct": 0.0037,
          "reason": "diff_ticks=35.0 > max_ticks=20.0"
        }
    """
    if expected_price <= 0 or fill_price <= 0:
        return {
            "ok": True,
            "diff_abs": 0.0,
            "diff_ticks": 0.0,
            "diff_pct": 0.0,
            "reason": "invalid_price",
        }

    if tick_size is None:
        tick_size = _get_env_float("PRICE_TICK_SIZE", 0.1)
    if max_ticks is None:
        max_ticks = _get_env_float("MAX_ENTRY_SLIPPAGE_TICKS", 20.0)
    if max_pct is None:
        max_pct = _get_env_float("MAX_ENTRY_SLIPPAGE_PCT", 0.0015)

    diff_abs = abs(fill_price - expected_price)
    diff_ticks = diff_abs / tick_size if tick_size > 0 else 0.0
    diff_pct = diff_abs / expected_price

    ok = True
    reasons: List[str] = []

    if max_ticks > 0 and diff_ticks > max_ticks:
        ok = False
        reasons.append(f"diff_ticks={diff_ticks:.1f} > max_ticks={max_ticks:.1f}")
    if max_pct > 0 and diff_pct > max_pct:
        ok = False
        reasons.append(f"diff_pct={diff_pct:.6f} > max_pct={max_pct:.6f}")

    return {
        "ok": ok,
        "diff_abs": diff_abs,
        "diff_ticks": diff_ticks,
        "diff_pct": diff_pct,
        "reason": ", ".join(reasons) if reasons else "",
    }


def _get_last_ws_price(symbol: str) -> Optional[float]:
    """WS 버퍼에서 최근 1m 종가를 가져온다.

    - market_data_ws.get_klines_with_volume(symbol, interval, limit=1)의
      반환 형식이 (ts_ms, o, h, l, c, v) 라는 전제를 따른다.
    """
    try:
        from infra.market_data_ws import get_klines_with_volume as ws_get_klines_with_volume
    except Exception as e:  # pragma: no cover - 순환 import/환경 문제
        log(f"[TRADER] WS 캔들 함수 import 실패: {e}")
        return None

    try:
        interval = getattr(SET, "interval", "1m") or "1m"
        rows = ws_get_klines_with_volume(symbol, interval, limit=1)
        if not rows:
            return None
        _, _o, _h, _l, c, _v = rows[-1]
        return float(c)
    except Exception as e:  # pragma: no cover - WS 버퍼 문제
        log(f"[TRADER] 최근 WS 가격 조회 실패: {e}")
        return None


def check_entry_slippage_once(trade: Trade, state: TraderState) -> None:
    """특정 Trade 에 대해 '1번만' 슬리피지 체크를 수행한다.

    - 엔트리 직후 몇 루프 이내에 check_closes(...) 에서 호출되도록 설계했다.
    - 기준 가격은 WS 1m 현재가를 사용한다.
    - 허용 틱 수/퍼센트는 evaluate_slippage 의 설명을 따른다.
    - 심각한 슬리피지이면 Telegram 경고를 보내고, stop_on_heavy_slippage 가 True 이면
      TraderState.mark_heavy_slippage() 를 호출해 봇을 멈출 수 있게 한다.
    """
    if trade.uid in state.slippage_checked_ids:
        return

    last_price = _get_last_ws_price(trade.symbol)
    if last_price is None:
        # 데이터가 없으면 체크하지 않고 넘어간다.
        state.slippage_checked_ids.add(trade.uid)
        return

    result = evaluate_slippage(expected_price=last_price, fill_price=trade.entry_price)
    state.slippage_checked_ids.add(trade.uid)

    if result.get("ok", True):
        return

    diff_abs = result.get("diff_abs", 0.0)
    diff_ticks = result.get("diff_ticks", 0.0)
    diff_pct = result.get("diff_pct", 0.0) * 100.0  # %
    reason = result.get("reason") or ""

    side_ko = "롱" if trade.direction() == "LONG" else "숏"

    msg = (
        "⚠️ 진입 슬리피지 경고\n"
        f"- 심볼: {trade.symbol}\n"
        f"- 방향: {side_ko}\n"
        f"- WS 기준 현재가: {last_price:.2f}\n"
        f"- 체결가(엔트리): {trade.entry_price:.2f}\n"
        f"- 차이: {diff_abs:.2f} (≈ {diff_ticks:.1f}틱, {diff_pct:.3f}%)\n"
    )
    if reason:
        msg += f"- 판정 사유: {reason}\n"

    try:
        send_tg(msg)
    except Exception:
        log(msg)

    # 심각한 슬리피지 발생 시 봇을 멈출지 여부는 settings 에서 제어
    stop_on_heavy = bool(getattr(SET, "stop_on_heavy_slippage", False))
    if stop_on_heavy:
        state.mark_heavy_slippage()


# ─────────────────────────────────────────
# TP/SL 체결 및 포지션 종료 확인
# ─────────────────────────────────────────


def _infer_close_reason(trade: Trade, close_price: float) -> str:
    """TP/SL 근처면 이유를 추정해서 문자열로 돌려준다.

    - TP 근처: 'tp_hit'
    - SL 근처: 'sl_hit'
    - 나머지: 'manual_close'
    """
    try:
        tol_pct = 0.001  # 0.1% 이내면 같은 가격으로 본다.
        if trade.tp_price:
            if abs(close_price - trade.tp_price) / trade.tp_price <= tol_pct:
                return "tp_hit"
        if trade.sl_price:
            if abs(close_price - trade.sl_price) / trade.sl_price <= tol_pct:
                return "sl_hit"
    except Exception:
        pass
    return "manual_close"


def _approx_close_price(symbol: str, fallback: float) -> float:
    """WS 기준 가격이 있으면 그것을, 없으면 fallback 을 사용."""
    last_price = _get_last_ws_price(symbol)
    if last_price is None or last_price <= 0:
        return fallback
    return last_price


def check_closes(
    open_trades: List[Trade],
    state: TraderState,
) -> Tuple[List[Trade], List[Dict[str, Any]]]:
    """현재 열린 포지션 목록에서 '더 이상 거래소에 존재하지 않는' 포지션을 CLOSED 로 처리한다.

    반환:
        (new_open_trades, closed_list)

    closed_list 원소 예시:
        {
          "trade": <Trade>,
          "reason": "tp_hit" | "sl_hit" | "manual_close",
          "summary": {
              "qty": 0.001,
              "avg_price": 93000.5,
              "pnl": 12.34,
          },
        }
    """
    # 1) 먼저, 아직 슬리피지 체크가 안 된 Trade 들에 대해 1회씩만 체크.
    for t in open_trades:
        try:
            check_entry_slippage_once(t, state)
        except Exception as e:  # pragma: no cover - 슬리피지 체크 오류는 치명적 아님
            log(f"[TRADER] 슬리피지 체크 중 오류: {e}")

    if not open_trades:
        return open_trades, []

    # 2) 거래소의 현재 open position 스냅샷 조회
    try:
        from execution.exchange_api import fetch_open_positions
    except Exception as e:  # pragma: no cover - 환경 문제
        log(f"[TRADER] fetch_open_positions import 실패: {e}")
        # 거래소 상태를 알 수 없으면 청산 이벤트 없이 그대로 반환
        return open_trades, []

    # 심볼별/방향별로 남아 있는 포지션을 매핑
    positions_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}

    symbols = sorted({t.symbol for t in open_trades})
    for symbol in symbols:
        try:
            pos_list = fetch_open_positions(symbol) or []
        except Exception as e:  # pragma: no cover - API 오류
            log(f"[TRADER] fetch_open_positions({symbol}) 호출 오류: {e}")
            continue

        for p in pos_list:
            try:
                qty = float(
                    p.get("positionAmt")
                    or p.get("quantity")
                    or p.get("size")
                    or 0.0
                )
            except Exception:
                qty = 0.0
            if abs(qty) <= 0:
                continue

            raw_side = str(p.get("positionSide") or "").upper()
            if raw_side in ("LONG", "SHORT"):
                side = raw_side
            else:
                side = "LONG" if qty > 0 else "SHORT"

            key = (symbol, side)
            positions_by_key[key] = p

    # 3) open_trades 와 비교해서 사라진 포지션을 CLOSED 로 판단
    new_open: List[Trade] = []
    closed_events: List[Dict[str, Any]] = []

    for t in open_trades:
        dir_key = t.direction()
        key = (t.symbol, dir_key)

        if key in positions_by_key:
            # 아직 거래소에 포지션이 살아 있으므로 open 으로 유지
            new_open.append(t)
            continue

        # 거래소 스냅샷에서 해당 방향 포지션이 사라졌다면 → TP/SL/수동청산으로 본다.
        close_price = _approx_close_price(t.symbol, t.entry_price)

        # PnL 근사 계산 (선물 PnL = (close - entry) * qty * 방향부호)
        pnl = (close_price - t.entry_price) * t.qty * t.sign()

        reason = _infer_close_reason(t, close_price)
        summary = {
            "qty": t.qty,
            "avg_price": close_price,
            "pnl": pnl,
        }
        closed_events.append(
            {
                "trade": t,
                "reason": reason,
                "summary": summary,
            }
        )

    return new_open, closed_events


__all__ = [
    "Trade",
    "TraderState",
    "evaluate_slippage",
    "check_entry_slippage_once",
    "check_closes",
]
