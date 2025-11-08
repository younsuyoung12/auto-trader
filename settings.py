"""settings.py
BingX 자동매매 봇 공통 설정 모듈
- 환경변수 읽기
- 기본 타임존, 상수 정의
- 다른 모듈에서 import 해서 사용
"""

from __future__ import annotations
import os
import datetime
from dataclasses import dataclass
from typing import Dict

# 한국 시간대 (KST)
KST = datetime.timezone(datetime.timedelta(hours=9))


def _as_bool(val: str, default: bool = False) -> bool:
    """문자열 환경변수를 bool 로 변환 ("1", "true", "True" → True)
    빈값이면 default 사용"""
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "y"}


def _as_int(val: str, default: int) -> int:
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def _as_float(val: str, default: float) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _ensure_ascii_env(val: str, name: str) -> str:
    """요청 헤더 등에 올라갈 수 있는 문자열은 ASCII 만 남기도록 정리
    - 한글/공백이 들어있으면 제거해서 안전하게 만든다.
    - 원래 문자열은 로그에서 처리하고, 여기서는 예외를 던지지 않는다.
    이 함수는 편의상 여기(settings.py)에 두고, 실제 로그는 utils 쪽에서 처리한다.
    다른 모듈에서 재사용 가능하도록 그대로 export 한다.
    """
    if not val:
        return val
    try:
        val.encode("ascii")
        return val
    except UnicodeEncodeError:
        cleaned = val.encode("ascii", "ignore").decode("ascii").strip()
        # 여기서는 print/log 를 하지 않는다. 실제 로깅은 사용처에서.
        return cleaned


@dataclass(frozen=True)
class BotSettings:
    """봇 전체가 참조하는 설정 묶음"""
    # 인증
    api_key: str
    api_secret: str

    # 기본 심볼/주기
    symbol: str = "BTC-USDT"
    interval: str = "3m"  # 메인은 3분봉

    # 전략 on/off
    enable_trend: bool = True
    enable_range: bool = False
    enable_1m_confirm: bool = True

    # 레버리지/마진/리스크
    leverage: int = 10
    isolated: bool = True
    risk_pct: float = 0.3  # 계좌에서 이 비율만 사용
    min_notional_usdt: float = 5.0
    max_notional_usdt: float = 999999.0

    # 추세장 TP/SL 기본값
    tp_pct: float = 0.02
    sl_pct: float = 0.02

    # 박스장 전용 TP/SL
    range_tp_pct: float = 0.006
    range_sl_pct: float = 0.004

    # ATR 옵션
    use_atr: bool = True
    atr_len: int = 20
    atr_tp_mult: float = 2.0
    atr_sl_mult: float = 1.2
    min_tp_pct: float = 0.005
    min_sl_pct: float = 0.005
    atr_risk_high_mult: float = 1.5
    atr_risk_reduction: float = 0.5

    # 쿨다운/폴링
    cooldown_sec: int = 15
    cooldown_after_close: int = 30
    cooldown_after_3loss: int = 3600
    poll_fills_sec: int = 2
    # 전략별 청산 후 쿨다운
    cooldown_after_close_trend: int = 30
    cooldown_after_close_range: int = 30

    # 슬리피지/호가 가드
    max_price_jump_pct: float = 0.003
    max_spread_pct: float = 0.0008
    # ↓↓↓ 추가: 실제 체결가가 진입 힌트가격에서 얼마나 멀어지면 포기할지 설정
    # 예) 0.0005 = 0.05% 이상 미끄러지면 바로 닫게끔 상위 로직에서 사용할 값
    max_entry_slippage_pct: float = 0.0005
    # ↓↓↓ 추가: 호가(bid/ask) 기반 진입가 힌트를 사용할지 여부
    use_orderbook_entry_hint: bool = True

    # 텔레그램
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    # 로그/기타
    log_to_file: bool = False
    log_file: str = "bot.log"

    # 캔들 실패 관련
    max_kline_fails: int = 5
    kline_fail_sleep: int = 600

    # 거래 시간대(UTC 기준)
    trading_sessions_utc: str = "0-23"

    # health server
    health_port: int = 0

    # 박스장 일일 손절 제한
    range_max_daily_sl: int = 2

    # skip 텔레그램 스팸 억제
    skip_tg_cooldown: int = 30
    balance_skip_cooldown: int = 3600

    # 캔들 지연 허용치
    max_kline_delay_sec: int = 190

    # RSI 기준
    rsi_overbought: int = 70
    rsi_oversold: int = 30

    # 종료 관련
    min_uptime_for_stop: int = 5

    # BingX base url
    bingx_base: str = "https://open-api.bingx.com"

    def as_dict(self) -> Dict[str, object]:  # 필요한 경우 dict 변환용
        return self.__dict__.copy()


def load_settings() -> BotSettings:
    """환경변수에서 설정을 읽어서 BotSettings 로 만든다."""
    api_key = os.getenv("BINGX_API_KEY", "")
    api_secret = os.getenv("BINGX_API_SECRET", "")

    # 한글/공백 들어있을 수 있으니 정리
    api_key = _ensure_ascii_env(api_key, "BINGX_API_KEY")
    api_secret = _ensure_ascii_env(api_secret, "BINGX_API_SECRET")

    return BotSettings(
        api_key=api_key,
        api_secret=api_secret,
        symbol=os.getenv("SYMBOL", "BTC-USDT"),
        interval=os.getenv("INTERVAL", "3m"),
        enable_trend=_as_bool(os.getenv("ENABLE_TREND", "1"), True),
        enable_range=_as_bool(os.getenv("ENABLE_RANGE", "0"), False),
        enable_1m_confirm=_as_bool(os.getenv("ENABLE_1M_CONFIRM", "1"), True),
        leverage=_as_int(os.getenv("LEVERAGE", "10"), 10),
        isolated=_as_bool(os.getenv("ISOLATED", "1"), True),
        risk_pct=_as_float(os.getenv("RISK_PCT", "0.3"), 0.3),
        min_notional_usdt=_as_float(os.getenv("MIN_NOTIONAL_USDT", "5"), 5.0),
        max_notional_usdt=_as_float(os.getenv("MAX_NOTIONAL_USDT", "999999"), 999999.0),
        tp_pct=_as_float(os.getenv("TP_PCT", "0.02"), 0.02),
        sl_pct=_as_float(os.getenv("SL_PCT", "0.02"), 0.02),
        range_tp_pct=_as_float(os.getenv("RANGE_TP_PCT", "0.006"), 0.006),
        range_sl_pct=_as_float(os.getenv("RANGE_SL_PCT", "0.004"), 0.004),
        use_atr=_as_bool(os.getenv("USE_ATR", "1"), True),
        atr_len=_as_int(os.getenv("ATR_LEN", "20"), 20),
        atr_tp_mult=_as_float(os.getenv("ATR_TP_MULT", "2.0"), 2.0),
        atr_sl_mult=_as_float(os.getenv("ATR_SL_MULT", "1.2"), 1.2),
        min_tp_pct=_as_float(os.getenv("MIN_TP_PCT", "0.005"), 0.005),
        min_sl_pct=_as_float(os.getenv("MIN_SL_PCT", "0.005"), 0.005),
        atr_risk_high_mult=_as_float(os.getenv("ATR_RISK_HIGH_MULT", "1.5"), 1.5),
        atr_risk_reduction=_as_float(os.getenv("ATR_RISK_REDUCTION", "0.5"), 0.5),
        cooldown_sec=_as_int(os.getenv("COOLDOWN_SEC", "15"), 15),
        cooldown_after_close=_as_int(os.getenv("COOLDOWN_AFTER_CLOSE", "30"), 30),
        cooldown_after_3loss=_as_int(os.getenv("COOLDOWN_AFTER_3LOSS", "3600"), 3600),
        poll_fills_sec=_as_int(os.getenv("POLL_FILLS_SEC", "2"), 2),
        cooldown_after_close_trend=_as_int(
            os.getenv("COOLDOWN_AFTER_CLOSE_TREND", os.getenv("COOLDOWN_AFTER_CLOSE", "30")),
            30,
        ),
        cooldown_after_close_range=_as_int(
            os.getenv("COOLDOWN_AFTER_CLOSE_RANGE", os.getenv("COOLDOWN_AFTER_CLOSE", "30")),
            30,
        ),
        max_price_jump_pct=_as_float(os.getenv("MAX_PRICE_JUMP_PCT", "0.003"), 0.003),
        max_spread_pct=_as_float(os.getenv("MAX_SPREAD_PCT", "0.0008"), 0.0008),
        # ↓↓↓ 추가: 진입 시 허용할 슬리피지 비율 환경변수로도 제어
        max_entry_slippage_pct=_as_float(os.getenv("MAX_ENTRY_SLIPPAGE_PCT", "0.0005"), 0.0005),
        # ↓↓↓ 추가: 호가 기반 진입 힌트 사용 여부도 환경변수로 제어
        use_orderbook_entry_hint=_as_bool(os.getenv("USE_ORDERBOOK_ENTRY_HINT", "1"), True),
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
        log_to_file=_as_bool(os.getenv("LOG_TO_FILE", "0"), False),
        log_file=os.getenv("LOG_FILE", "bot.log"),
        max_kline_fails=_as_int(os.getenv("MAX_KLINE_FAILS", "5"), 5),
        kline_fail_sleep=_as_int(os.getenv("KLINE_FAIL_SLEEP", "600"), 600),
        trading_sessions_utc=os.getenv("TRADING_SESSIONS_UTC", "0-23"),
        health_port=_as_int(os.getenv("HEALTH_PORT", "0"), 0),
        range_max_daily_sl=_as_int(os.getenv("RANGE_MAX_DAILY_SL", "2"), 2),
        skip_tg_cooldown=_as_int(os.getenv("SKIP_TG_COOLDOWN", "30"), 30),
        balance_skip_cooldown=3600,  # 하드코딩 그대로 유지
        max_kline_delay_sec=_as_int(os.getenv("MAX_KLINE_DELAY_SEC", "190"), 190),
        rsi_overbought=_as_int(os.getenv("RSI_OVERBOUGHT", "70"), 70),
        rsi_oversold=_as_int(os.getenv("RSI_OVERSOLD", "30"), 30),
        min_uptime_for_stop=_as_int(os.getenv("MIN_UPTIME_FOR_STOP", "5"), 5),
        bingx_base="https://open-api.bingx.com",
    )


# 모듈을 직접 실행했을 때 설정 값을 프린트해서 확인할 수 있게 함
if __name__ == "__main__":
    s = load_settings()
    for k, v in s.as_dict().items():
        print(f"{k}: {v}")
