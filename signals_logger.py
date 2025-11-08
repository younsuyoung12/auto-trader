# signals_logger.py
"""
시그널/진입/청산/스킵 이벤트를 CSV로 기록하는 모듈.

- 날짜별로 파일 분리: logs/signals/signals-YYYY-MM-DD.csv
- KST 타임스탬프 같이 남김
- 왜 안 들어갔는지(reason), 어떤 전략이었는지(strategy_type: TREND/RANGE)도 남김
- 분석용으로 ATR, RSI, 15m 방향, 스프레드, 잔고, 명목가도 넣을 수 있음
- 한 거래를 ENTRY ↔ CLOSE 로 묶을 수 있도록 trade_id / exchange_order_id 도 추가

event 예시:
- "ENTRY_SIGNAL"   : 조건은 나왔는데 아직 주문 전
- "ENTRY_OPENED"   : 실제로 주문 나가고 포지션 열린 시점
- "CLOSE"          : TP/SL/강제청산 등으로 닫혔을 때
- "SKIP"           : 조건 안 맞아서/가드에 걸려서 안 들어갔을 때
"""

from __future__ import annotations

import csv
import os
import datetime
from typing import Optional


# 로그가 쌓일 기본 폴더
BASE_DIR = os.path.join("logs", "signals")


def _now_kst_str() -> str:
    """현재 시간을 KST로 YYYY-MM-DD HH:MM:SS 문자열로 리턴."""
    # KST = UTC+9
    now_utc = datetime.datetime.utcnow()
    kst = now_utc + datetime.timedelta(hours=9)
    return kst.strftime("%Y-%m-%d %H:%M:%S")


def _today_kst_str() -> str:
    """오늘 날짜를 KST 기준 YYYY-MM-DD 로 리턴."""
    now_utc = datetime.datetime.utcnow()
    kst = now_utc + datetime.timedelta(hours=9)
    return kst.strftime("%Y-%m-%d")


def _ensure_dir(path: str) -> None:
    """디렉터리가 없으면 만든다."""
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def _get_csv_path() -> str:
    """오늘 날짜 기준 CSV 파일 경로를 돌려준다."""
    _ensure_dir(BASE_DIR)
    fname = f"signals-{_today_kst_str()}.csv"
    return os.path.join(BASE_DIR, fname)


def log_signal(
    *,
    event: str,                  # ENTRY_SIGNAL / ENTRY_OPENED / CLOSE / SKIP ...
    symbol: str,
    strategy_type: str,          # "TREND" / "RANGE" / "SYNC" ...
    # ---- 기본 거래 정보 ----
    direction: Optional[str] = None,      # LONG / SHORT
    price: Optional[float] = None,        # 체결가 or 기준가
    qty: Optional[float] = None,
    tp_price: Optional[float] = None,
    sl_price: Optional[float] = None,
    reason: Optional[str] = None,         # 왜 스킵했는지, 왜 닫혔는지
    pnl: Optional[float] = None,          # 청산 시 PnL
    # ---- 매칭/추적용 추가 ----
    trade_id: Optional[str] = None,       # 우리가 붙이는 거래 식별자 (ENTRY와 CLOSE 묶기용)
    exchange_order_id: Optional[str] = None,  # 거래소 주문 ID
    step: Optional[str] = None,           # "before_order" / "after_order" / "guard" ...
    # ---- 분석용 추가 필드 ----
    candle_ts: Optional[int] = None,      # 이 시그널이 나온 3m 캔들 시간(ms)
    signal_price: Optional[float] = None, # 시그널이 나온 캔들의 종가(실제 시장상황)
    rsi_3m: Optional[float] = None,
    trend_15m: Optional[str] = None,      # "UP" / "DOWN" / None
    atr_fast: Optional[float] = None,
    atr_slow: Optional[float] = None,
    spread_pct: Optional[float] = None,
    available_usdt: Optional[float] = None,
    notional: Optional[float] = None,
    strategy_version: Optional[str] = None,
    extra: Optional[str] = None,          # "guard=price_jump;cooldown=true" 같은 자유필드
) -> None:
    """
    한 줄을 CSV에 쓴다. 파일이 없으면 헤더를 먼저 쓴다.
    """
    path = _get_csv_path()
    file_exists = os.path.exists(path)

    row = {
        "ts_kst": _now_kst_str(),
        "event": event,
        "symbol": symbol,
        "strategy_type": strategy_type,
        "direction": direction or "",
        "price": price if price is not None else "",
        "qty": qty if qty is not None else "",
        "tp_price": tp_price if tp_price is not None else "",
        "sl_price": sl_price if sl_price is not None else "",
        "reason": reason or "",
        "pnl": pnl if pnl is not None else "",
        # 매칭/추적
        "trade_id": trade_id or "",
        "exchange_order_id": exchange_order_id or "",
        "step": step or "",
        # 분석용
        "candle_ts": candle_ts if candle_ts is not None else "",
        "signal_price": signal_price if signal_price is not None else "",
        "rsi_3m": rsi_3m if rsi_3m is not None else "",
        "trend_15m": trend_15m or "",
        "atr_fast": atr_fast if atr_fast is not None else "",
        "atr_slow": atr_slow if atr_slow is not None else "",
        "spread_pct": spread_pct if spread_pct is not None else "",
        "available_usdt": available_usdt if available_usdt is not None else "",
        "notional": notional if notional is not None else "",
        "strategy_version": strategy_version or "",
        "extra": extra or "",
    }

    fieldnames = [
        "ts_kst",
        "event",
        "symbol",
        "strategy_type",
        "direction",
        "price",
        "qty",
        "tp_price",
        "sl_price",
        "reason",
        "pnl",
        "trade_id",
        "exchange_order_id",
        "step",
        "candle_ts",
        "signal_price",
        "rsi_3m",
        "trend_15m",
        "atr_fast",
        "atr_slow",
        "spread_pct",
        "available_usdt",
        "notional",
        "strategy_version",
        "extra",
    ]

    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)
