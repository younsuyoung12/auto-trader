# signals_logger.py
# ====================================================
# 시그널/진입/청산/스킵/에러/GPT 이벤트 및 캔들 스냅샷을 CSV로 기록하는 모듈.
#
# 2025-11-17 변경 사항 (이벤트/포지션/GPT/에러 전용 CSV 추가)
# ----------------------------------------------------
# 1) logs/events/events-YYYY-MM-DD.csv 추가
#    - 필드: ts, ts_iso, event_type, symbol, regime, source, side, price,
#            qty, leverage, tp_pct, sl_pct, risk_pct, pnl_pct, reason, extra_json
#    - log_event() 를 공통 엔트리로 두고, 다음 편의 함수 제공:
#        · log_entry_event()
#        · log_exit_event()
#        · log_regime_switch_event()
#        · log_gpt_entry_event()
#        · log_gpt_exit_event()
#        · log_skip_event()  (별칭 log_skip)
#        · log_error_event()
#    - 나중에 분석할 때 "왜 진입 안 했는지 / 언제 얼마에 진입·청산했는지 /
#      TREND↔RANGE 전환 / GPT 판단"을 한 파일에서 조회 가능.
#
# 2) 기존 signals / candles CSV 는 그대로 유지
#    - log_signal(): 전략/가드/진입/청산 이벤트를 분석용으로 기록
#    - log_candle_snapshot(): 캔들(1m/5m/15m 등) 스냅샷 기록
#
# 3) KST 기준으로 signals / candles / events 세 종류 모두 헤더 자동 생성
#    - 모듈 로드시 오늘자 signals-YYYY-MM-DD.csv, candles-YYYY-MM-DD.csv,
#      events-YYYY-MM-DD.csv 가 없으면 헤더만 있는 빈 파일을 만들어 둔다.
#    - 자정(KST) 이후에도 각 로그 함수가 호출될 때 자동으로 새 파일 생성.
"""
시그널/진입/청산/스킵 이벤트를 CSV로 기록하는 모듈.

[2025-11-12 추가 사항]
----------------------------------------------------
1) 차트/시세 스냅샷 전용 CSV 추가
   - 파일 위치: logs/candles/candles-YYYY-MM-DD.csv
   - run_bot.py 등에서 "지금 본 3분봉/15분봉"을 그대로 남길 수 있도록 별도 함수 추가
   - 컬럼: ts_kst, symbol, tf, candle_ts, open, high, low, close, volume, strategy_type, direction, extra
   - 신호 CSV와 분리해서 저장하므로 신호 분석과 시세 분석을 따로 할 수 있음

2) 기존 시그널 CSV 형식은 그대로 유지
   - logs/signals/signals-YYYY-MM-DD.csv
   - RANGE 개선용 컬럼(range_level, soft_block_reason, used_tp_pct, used_sl_pct) 그대로 둠

이 파일에서 하는 일
- 날짜별로 파일 분리: logs/signals/signals-YYYY-MM-DD.csv
- KST 타임스탬프 같이 남김
- 왜 안 들어갔는지(reason), 어떤 전략이었는지(strategy_type: TREND/RANGE)도 남김
- 분석용으로 ATR, RSI, 15m 방향, 스프레드, 잔고, 명목가도 넣을 수 있음
- 한 거래를 ENTRY ↔ CLOSE 로 묶을 수 있도록 trade_id / exchange_order_id 도 추가

2025-11-12 추가/변경 (박스 개선 로그 반영)
----------------------------------------------------
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
import json
import datetime
from typing import Optional, List, Dict, Any

# ─────────────────────────────────────────────
# 경로 설정
# ─────────────────────────────────────────────
# 시그널 로그가 쌓일 기본 폴더
BASE_DIR = os.path.join("logs", "signals")
# 시세(차트) 스냅샷이 쌓일 기본 폴더
CANDLE_DIR = os.path.join("logs", "candles")
# 이벤트(ENTRY/EXIT/GPT/ERROR 등) 로그가 쌓일 기본 폴더
EVENTS_DIR = os.path.join("logs", "events")


# ─────────────────────────────────────────────
# 공통 유틸
# ─────────────────────────────────────────────
def _now_kst() -> datetime.datetime:
    """현재 시간을 KST timezone 기준 datetime 으로 반환."""
    now_utc = datetime.datetime.utcnow()
    return now_utc + datetime.timedelta(hours=9)


def _now_kst_str() -> str:
    """현재 시간을 KST로 YYYY-MM-DD HH:MM:SS 문자열로 리턴."""
    kst = _now_kst()
    return kst.strftime("%Y-%m-%d %H:%M:%S")


def _today_kst_str() -> str:
    """오늘 날짜를 KST 기준 YYYY-MM-DD 로 리턴."""
    kst = _now_kst()
    return kst.strftime("%Y-%m-%d")


def _ensure_dir(path: str) -> None:
    """디렉터리가 없으면 만든다."""
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


# ─────────────────────────────────────────────
# 시그널 CSV 관련
# ─────────────────────────────────────────────
def _csv_path_for(day_str: str) -> str:
    """주어진 날짜(KST)용 시그널 CSV 파일 전체 경로를 돌려준다."""
    _ensure_dir(BASE_DIR)
    fname = f"signals-{day_str}.csv"
    return os.path.join(BASE_DIR, fname)


def _fieldnames() -> List[str]:
    """시그널 CSV에서 공통으로 쓸 헤더 정의."""
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
        "range_level",
        "soft_block_reason",
        "used_tp_pct",
        "used_sl_pct",
    ]


def _ensure_today_csv() -> str:
    """오늘(KST) 날짜의 시그널 CSV가 없으면 헤더만 있는 빈 파일을 만든다."""
    day = _today_kst_str()
    path = _csv_path_for(day)
    fields = _fieldnames()
    if not os.path.exists(path):
        _ensure_dir(BASE_DIR)
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
    return path


# 모듈이 로드될 때 오늘자 시그널 파일을 한 번 만들어 둔다.
_ensure_today_csv()


def _get_csv_path() -> str:
    """오늘 날짜 기준 시그널 CSV 파일 경로를 돌려준다."""
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
    candle_ts: Optional[int] = None,      # 이 시그널이 나온 캔들 시간(ms)
    signal_price: Optional[float] = None, # 시그널이 나온 캔들의 종가
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
    """한 줄을 시그널 CSV에 쓴다."""
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
        # 박스 개선용
        "range_level": range_level if range_level is not None else "",
        "soft_block_reason": soft_block_reason or "",
        "used_tp_pct": used_tp_pct if used_tp_pct is not None else "",
        "used_sl_pct": used_sl_pct if used_sl_pct is not None else "",
    }

    fieldnames = _fieldnames()

    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


# ─────────────────────────────────────────────
# 2025-11-12 추가: 시세(캔들) 스냅샷 로그
# ─────────────────────────────────────────────
def _candle_csv_path_for(day_str: str) -> str:
    """주어진 날짜(KST)용 시세 스냅샷 CSV 전체 경로를 돌려준다."""
    _ensure_dir(CANDLE_DIR)
    fname = f"candles-{day_str}.csv"
    return os.path.join(CANDLE_DIR, fname)


def _candle_fieldnames() -> List[str]:
    """시세 스냅샷 전용 CSV 헤더 정의."""
    return [
        "ts_kst",        # 기록 시각(KST)
        "symbol",        # BTC-USDT
        "tf",            # 1m, 5m, 15m ...
        "candle_ts",     # 이 캔들의 원래 타임스탬프(ms 또는 sec)
        "open",
        "high",
        "low",
        "close",
        "volume",
        "strategy_type", # 이 시점에 어떤 전략을 평가 중이었는지
        "direction",     # LONG/SHORT (없으면 빈칸)
        "extra",         # 자유 텍스트 (예: "early_exit_check=1")
    ]


def _ensure_today_candle_csv() -> str:
    """오늘(KST) 날짜의 시세 스냅샷 CSV가 없으면 헤더만 있는 빈 파일 생성."""
    day = _today_kst_str()
    path = _candle_csv_path_for(day)
    fields = _candle_fieldnames()
    if not os.path.exists(path):
        _ensure_dir(CANDLE_DIR)
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
    return path


# 필요하면 모듈 로드시점에 만들어둔다.
_ensure_today_candle_csv()


def log_candle_snapshot(
    *,
    symbol: str,
    tf: str,
    candle_ts: int,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: float,
    strategy_type: Optional[str] = "",
    direction: Optional[str] = "",
    extra: Optional[str] = "",
) -> None:
    """시세(캔들) 스냅샷 한 줄을 별도 CSV에 남긴다."""
    path = _ensure_today_candle_csv()
    file_exists = os.path.exists(path)

    row = {
        "ts_kst": _now_kst_str(),
        "symbol": symbol,
        "tf": tf,
        "candle_ts": candle_ts,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "strategy_type": strategy_type or "",
        "direction": direction or "",
        "extra": extra or "",
    }

    fieldnames = _candle_fieldnames()

    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


# ─────────────────────────────────────────────
# 2025-11-17 추가: 이벤트(ENTRY/EXIT/GPT/ERROR 등) CSV
# ─────────────────────────────────────────────
def _events_csv_path_for(day_str: str) -> str:
    """주어진 날짜(KST)용 이벤트 CSV 전체 경로를 돌려준다."""
    _ensure_dir(EVENTS_DIR)
    fname = f"events-{day_str}.csv"
    return os.path.join(EVENTS_DIR, fname)


def _event_fieldnames() -> List[str]:
    """이벤트 CSV 헤더 정의."""
    return [
        "ts",           # float timestamp (KST 기준)
        "ts_iso",       # ISO 포맷 문자열
        "event_type",   # SIGNAL / SKIP / ENTRY / EXIT / REGIME_SWITCH / GPT_ENTRY / GPT_EXIT / ERROR ...
        "symbol",
        "regime",       # TREND / RANGE / HYBRID 등
        "source",       # 호출 위치, 시나리오 이름 등
        "side",         # LONG / SHORT / BUY / SELL
        "price",        # 관련 가격 (진입/청산/체크 가격)
        "qty",          # 수량
        "leverage",     # 레버리지
        "tp_pct",       # 당시 TP 비율
        "sl_pct",       # 당시 SL 비율
        "risk_pct",     # effective_risk_pct
        "pnl_pct",      # 청산 시 손익
        "reason",       # 한 줄 요약
        "extra_json",   # JSON 문자열(dict/list)
    ]


def _ensure_today_events_csv() -> str:
    """오늘(KST) 날짜의 이벤트 CSV가 없으면 헤더만 있는 빈 파일을 만든다."""
    day = _today_kst_str()
    path = _events_csv_path_for(day)
    fields = _event_fieldnames()
    if not os.path.exists(path):
        _ensure_dir(EVENTS_DIR)
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
    return path


# 모듈 로드시 오늘자 이벤트 파일도 한 번 만들어 둔다.
_ensure_today_events_csv()


def _default_event_row(event_type: str) -> Dict[str, Any]:
    """공통 이벤트 row 기본값 생성."""
    now = _now_kst()
    ts = now.timestamp()
    return {
        "ts": ts,
        "ts_iso": now.isoformat(),
        "event_type": event_type,
        "symbol": "",
        "regime": "",
        "source": "",
        "side": "",
        "price": "",
        "qty": "",
        "leverage": "",
        "tp_pct": "",
        "sl_pct": "",
        "risk_pct": "",
        "pnl_pct": "",
        "reason": "",
        "extra_json": "",
    }


def log_event(event_type: str, **kwargs: Any) -> None:
    """이벤트 CSV에 한 줄을 기록한다.

    event_type 예시:
      - "SIGNAL", "SKIP", "ENTRY", "EXIT", "REGIME_SWITCH",
        "GPT_ENTRY", "GPT_EXIT", "ERROR" 등

    kwargs 필드는 _event_fieldnames() 에 정의된 키를 사용.
      · extra 또는 extra_json 으로 dict/list 를 넘기면 JSON 문자열로 저장.
    """
    path = _ensure_today_events_csv()
    file_exists = os.path.exists(path)

    row = _default_event_row(event_type)

    # 기본 필드 매핑
    for key, val in kwargs.items():
        if key in row and key != "extra_json":
            row[key] = val

    # extra / extra_json 처리
    extra_val: Any = None
    if "extra_json" in kwargs:
        extra_val = kwargs["extra_json"]
    elif "extra" in kwargs:
        extra_val = kwargs["extra"]

    if isinstance(extra_val, (dict, list)):
        row["extra_json"] = json.dumps(extra_val, ensure_ascii=False)
    elif isinstance(extra_val, str):
        row["extra_json"] = extra_val

    fieldnames = _event_fieldnames()

    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in fieldnames})


# ─────────────────────────────────────────────
# 이벤트 전용 편의 함수들
# ─────────────────────────────────────────────

def log_entry_event(
    *,
    symbol: str,
    regime: str,
    side: str,
    price: float,
    qty: float,
    leverage: float,
    tp_pct: float,
    sl_pct: float,
    risk_pct: float,
    reason: str,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """실제 진입(주문 체결) 이벤트 기록."""
    log_event(
        "ENTRY",
        symbol=symbol,
        regime=regime,
        side=side,
        price=price,
        qty=qty,
        leverage=leverage,
        tp_pct=tp_pct,
        sl_pct=sl_pct,
        risk_pct=risk_pct,
        reason=reason,
        extra_json=extra or {},
    )


def log_exit_event(
    *,
    symbol: str,
    regime: str,
    side: str,
    price: float,
    qty: float,
    leverage: float,
    pnl_pct: float,
    scenario: str,
    reason: str,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """포지션 청산(일반 TP/SL, 조기청산 등) 이벤트 기록."""
    log_event(
        "EXIT",
        symbol=symbol,
        regime=regime,
        side=side,
        price=price,
        qty=qty,
        leverage=leverage,
        pnl_pct=pnl_pct,
        source=scenario,
        reason=reason,
        extra_json=extra or {},
    )


def log_regime_switch_event(
    *,
    symbol: str,
    from_regime: str,
    to_regime: str,
    side: str,
    price: float,
    reason: str,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """RANGE ↔ TREND 등 레짐 전환 시 기록."""
    log_event(
        "REGIME_SWITCH",
        symbol=symbol,
        regime=f"{from_regime}->{to_regime}",
        side=side,
        price=price,
        reason=reason,
        extra_json=extra or {},
    )


def log_gpt_entry_event(
    *,
    symbol: str,
    regime: str,
    side: str,
    action: str,
    tp_pct: float,
    sl_pct: float,
    risk_pct: float,
    gpt_json: Dict[str, Any],
) -> None:
    """GPT 진입 판단 결과 기록."""
    log_event(
        "GPT_ENTRY",
        symbol=symbol,
        regime=regime,
        side=side,
        tp_pct=tp_pct,
        sl_pct=sl_pct,
        risk_pct=risk_pct,
        reason=f"GPT action={action}",
        extra_json=gpt_json,
    )


def log_gpt_exit_event(
    *,
    symbol: str,
    side: str,
    action: str,
    scenario: str,
    pnl_pct: Optional[float],
    gpt_json: Dict[str, Any],
) -> None:
    """GPT 청산 판단 결과 기록."""
    log_event(
        "GPT_EXIT",
        symbol=symbol,
        side=side,
        pnl_pct=pnl_pct if pnl_pct is not None else "",
        source=scenario,
        reason=f"GPT action={action}",
        extra_json=gpt_json,
    )


def log_skip_event(
    *,
    symbol: str,
    regime: str,
    source: str,
    side: Optional[str],
    reason: str,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """가드/조건/GPT 등으로 인해 진입을 스킵한 경우 이벤트 기록."""
    log_event(
        "SKIP",
        symbol=symbol,
        regime=regime,
        source=source,
        side=side or "",
        reason=reason,
        extra_json=extra or {},
    )


# 기존 설계에서 log_skip 이름을 기대할 수 있으므로 별칭 제공
log_skip = log_skip_event


def log_error_event(
    *,
    where: str,
    message: str,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """예외/에러 상황 기록."""
    log_event(
        "ERROR",
        source=where,
        reason=message,
        extra_json=extra or {},
    )


__all__ = [
    # 시그널/캔들용
    "log_signal",
    "log_candle_snapshot",
    # 이벤트용
    "log_event",
    "log_entry_event",
    "log_exit_event",
    "log_regime_switch_event",
    "log_gpt_entry_event",
    "log_gpt_exit_event",
    "log_skip_event",
    "log_skip",
    "log_error_event",
]