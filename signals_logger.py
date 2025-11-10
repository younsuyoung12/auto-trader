# signals_logger.py
"""
시그널/진입/청산/스킵 이벤트를 CSV로 기록하는 모듈.

- 날짜별로 파일 분리: logs/signals/signals-YYYY-MM-DD.csv
- KST 타임스탬프 같이 남김
- 왜 안 들어갔는지(reason), 어떤 전략이었는지(strategy_type: TREND/RANGE)도 남김
- 분석용으로 ATR, RSI, 15m 방향, 스프레드, 잔고, 명목가도 넣을 수 있음
- 한 거래를 ENTRY ↔ CLOSE 로 묶을 수 있도록 trade_id / exchange_order_id 도 추가

2025-11-12 추가/변경
----------------------------------------------------
(박스 개선 로그 반영)
1) 박스 전략이 단계적으로 허용되는 경우를 로그에 남길 수 있게 필드를 추가했다.
   - range_level: RANGE 전략이 어떤 엄격도(0,1,2)에서 찍힌 건지 남긴다.
   - soft_block_reason: "soft_atr", "soft_ema" 같이 전략에서 완전 차단 대신
     약하게 허용한 이유를 텍스트로 남길 수 있다.
2) 박스 TP/SL 을 동적으로 계산했을 때 실제로 사용한 퍼센트를 같이 남길 수 있게 했다.
   - used_tp_pct, used_sl_pct: 최종으로 주문에 들어간 TP/SL 퍼센트
   이렇게 남겨두면 나중에 CSV만 봐도 "왜 이 진입은 0.004로 찍혔지?"를 역추적할 수 있다.
3) 필드가 늘어도 기존 로직이 깨지지 않게, 모듈 로드 시점에 만드는 헤더에도
   새 컬럼을 포함시켰다.

2025-11-10 추가/변경
1) 모듈이 import 되는 시점에 그날(KST) 날짜 파일이 없으면 헤더만 있는 빈 CSV를 미리 만들어 둔다.
   → 진입·스킵이 전혀 없어도 드라이브 업로더가 올릴 수 있게 됨.
2) log_signal()이 호출될 때마다도 해당 날짜 파일 존재여부를 다시 확인해서
   자정(KST) 이후에도 자동으로 새 파일이 생기도록 했다.
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


def _csv_path_for(day_str: str) -> str:
    """주어진 날짜(KST)용 CSV 파일 전체 경로를 돌려준다."""
    _ensure_dir(BASE_DIR)
    fname = f"signals-{day_str}.csv"
    return os.path.join(BASE_DIR, fname)


def _fieldnames() -> list[str]:
    """모든 로그에서 공통으로 쓸 헤더 정의.
    - 여기서 컬럼을 추가하면 _ensure_today_csv(), log_signal() 둘 다 반영된다.
    """
    return [
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
        # ─── 2025-11-12 추가 ───
        "range_level",        # RANGE_STRICT_LEVEL 이나 호출부에서 전달한 엄격도
        "soft_block_reason",  # "soft_atr" / "soft_ema" 등
        "used_tp_pct",        # 설정/동적 계산으로 실제 사용한 TP 퍼센트
        "used_sl_pct",        # 설정/동적 계산으로 실제 사용한 SL 퍼센트
    ]


def _ensure_today_csv() -> str:
    """
    오늘(KST) 날짜의 CSV가 없으면 헤더만 있는 빈 파일을 만들어둔다.
    - run_bot 이 켜지는 순간 바로 오늘자 파일이 생기게 하기 위함.
    """
    day = _today_kst_str()
    path = _csv_path_for(day)
    fields = _fieldnames()
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
    return path


# 모듈이 로드될 때 오늘자 파일을 한 번 만들어 둔다.
_ensure_today_csv()


def _get_csv_path() -> str:
    """
    오늘 날짜 기준 CSV 파일 경로를 돌려준다.
    이때 파일이 없으면 헤더만 있는 새 파일을 만든다.
    """
    return _ensure_today_csv()


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
    # ---- 2025-11-12: 박스 개선용 필드 ----
    range_level: Optional[int] = None,        # RANGE_STRICT_LEVEL 등
    soft_block_reason: Optional[str] = None,  # "soft_atr" 등
    used_tp_pct: Optional[float] = None,      # 실제 사용한 박스 TP (%)
    used_sl_pct: Optional[float] = None,      # 실제 사용한 박스 SL (%)
) -> None:
    """
    한 줄을 CSV에 쓴다. 파일이 없으면 헤더를 먼저 쓴다.
    (자정 이후에도 신규 파일이 자동으로 생기게 _get_csv_path()가 알아서 처리)
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
        # 2025-11-12 추가
        "range_level": range_level if range_level is not None else "",
        "soft_block_reason": soft_block_reason or "",
        "used_tp_pct": used_tp_pct if used_tp_pct is not None else "",
        "used_sl_pct": used_sl_pct if used_sl_pct is not None else "",
    }

    fieldnames = _fieldnames()

    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        # _ensure_today_csv()에서 이미 헤더를 써놨으면 file_exists는 True지만,
        # 자정 지나 새 파일이 생긴 경우에는 여기서도 헤더를 한 번 더 써준다.
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)
