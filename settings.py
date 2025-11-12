"""
settings.py
BingX 자동매매 봇 공통 설정

이 파일에서 하는 일
- 환경변수를 읽어서 한 군데(BotSettings)로 묶는다
- 추세/박스 on-off, TP/SL, 슬리피지, 텔레그램, 박스 세부 옵션을 여기서 관리한다

이번 수정 내용 (2025-11-12)
----------------------------------------------------
1) 박스 진입 세분화/비대칭 옵션 유지
   - range_tp_long_pct / range_tp_short_pct
   - range_sl_long_pct / range_sl_short_pct
   - range_entry_upper_pct / range_entry_lower_pct
   - range_short_sl_floor_ratio

2) 박스 단계화/동적 TP 옵션 유지
   - range_strict_level
   - use_range_dynamic_tp, range_tp_min, range_tp_max

3) ✅ 박스 포지션 조기 청산/익절 옵션 추가
   - range_early_exit_enabled / range_early_exit_loss_pct
     → 박스로 들어간 뒤에 생각보다 반대로 세게 가면 거래소 TP/SL 기다리지 않고 먼저 닫기
   - range_early_tp_enabled / range_early_tp_pct
     → 박스로 들어간 뒤에 살짝이라도 수익 나면 바로 챙기기
     (실행은 run_bot.py → position_watch.py 에서 처리)

4) ✅ 추세(TREND) 포지션 조기 청산/익절 옵션 추가
   - trend_early_exit_enabled / trend_early_exit_loss_pct
   - trend_early_tp_enabled / trend_early_tp_pct
     (실행은 position_watch.py 에서 처리)

5) ✅ 추세(TREND) 포지션 횡보 감지 조기 청산 옵션 추가
   - trend_sideways_enabled
   - trend_sideways_need_bars
   - trend_sideways_range_pct
   - trend_sideways_max_pnl_pct

6) ✅ (기존 그대로) ATR/쿨다운/텔레그램/거래시간 옵션 유지

7) ✅ run_bot.py 에서 position_watch.py 의 실시간 대응을 바로 쓸 수 있도록 기본값 보수적으로 설정

8) ✅ 박스 → 추세 업그레이드 최소 수익률 옵션 추가
   - range_to_trend_min_gain_pct
     → 박스로 들어갔는데 다시 보니까 추세가 열렸을 때 “이 정도 이익 나 있으면 닫아라” 기준

9) ✅ 추세 횡보 1분봉 보조 체크 옵션 추가
   - trend_sideways_use_1m
   - trend_sideways_1m_need_bars
   - trend_sideways_1m_range_pct
     → position_watch.py 의 maybe_sideways_exit_trend() 에서 사용

10) ✅ 반대 시그널 감지 즉시 청산 on/off 추가
    - close_on_opposite_enabled
    - (기본값 True)
    - position_watch.py 의 maybe_close_on_opposite_signal(...) 에서 사용
"""

from __future__ import annotations
import os
import datetime
from dataclasses import dataclass
from typing import Dict

# 한국 시간대 (KST)
KST = datetime.timezone(datetime.timedelta(hours=9))


def _as_bool(val: str, default: bool = False) -> bool:
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
    """API 키처럼 헤더에 들어갈 값은 ASCII 로만 정리"""
    if not val:
        return val
    try:
        val.encode("ascii")
        return val
    except UnicodeEncodeError:
        return val.encode("ascii", "ignore").decode("ascii").strip()


@dataclass(frozen=True)
class BotSettings:
    """봇 전체가 import 해서 쓰는 설정 묶음"""

    # 필수 인증
    api_key: str
    api_secret: str

    # 기본 심볼/주기
    symbol: str = "BTC-USDT"
    interval: str = "3m"

    # 전략 on/off
    enable_trend: bool = True
    enable_range: bool = False
    enable_1m_confirm: bool = True

    # 레버리지/리스크
    leverage: int = 10
    isolated: bool = True
    risk_pct: float = 0.3
    min_notional_usdt: float = 5.0
    max_notional_usdt: float = 999999.0

    # 추세장 TP/SL
    tp_pct: float = 0.02
    sl_pct: float = 0.02

    # 박스장 기본 TP/SL
    range_tp_pct: float = 0.01
    range_sl_pct: float = 0.01

    # 박스장 방향별 비대칭
    range_tp_long_pct: float = 0.01
    range_tp_short_pct: float = 0.01
    range_sl_long_pct: float = 0.01
    range_sl_short_pct: float = 0.01

    # 박스 진입 위치/보정
    range_entry_upper_pct: float = 0.80
    range_entry_lower_pct: float = 0.20
    range_soft_tp_factor: float = 1.2
    range_short_sl_floor_ratio: float = 0.75

    # ✅ 박스 조기 청산/익절
    range_early_exit_enabled: bool = True
    range_early_exit_loss_pct: float = 0.003  # 0.3%
    range_early_tp_enabled: bool = False
    range_early_tp_pct: float = 0.0025        # 0.25%

    # ✅ 추세(TREND) 조기 청산/익절
    trend_early_exit_enabled: bool = True
    trend_early_exit_loss_pct: float = 0.003  # 0.3%
    trend_early_tp_enabled: bool = False
    trend_early_tp_pct: float = 0.0025        # 0.25%

    # ✅ 추세(TREND) 횡보 감지 조기 청산 (기본 주기)
    trend_sideways_enabled: bool = True
    trend_sideways_need_bars: int = 3
    trend_sideways_range_pct: float = 0.0008      # 0.08% 이하면 납작으로 본다
    trend_sideways_max_pnl_pct: float = 0.0015    # ±0.15% 안에서만 적용

    # ✅ 추세(TREND) 횡보 감지 1분 보조
    trend_sideways_use_1m: bool = True
    trend_sideways_1m_need_bars: int = 3
    trend_sideways_1m_range_pct: float = 0.0006   # 0.06% 정도로 더 타이트하게

    # ✅ 박스 → 추세 업그레이드 최소 이익률
    range_to_trend_min_gain_pct: float = 0.002    # 0.2% 이상 나 있으면 정리 허용

    # ✅ 반대 시그널 감지 즉시 청산 on/off
    close_on_opposite_enabled: bool = True

    # ATR/변동성 관련
    use_atr: bool = True
    atr_len: int = 20
    atr_tp_mult: float = 2.0
    atr_sl_mult: float = 1.2
    min_tp_pct: float = 0.01
    min_sl_pct: float = 0.01
    atr_risk_high_mult: float = 1.5
    atr_risk_reduction: float = 0.5

    # 진입 거래량 가드
    min_entry_volume_ratio: float = 0.15

    # 쿨다운/폴링
    cooldown_sec: int = 15
    cooldown_after_close: int = 30
    cooldown_after_3loss: int = 3600
    poll_fills_sec: int = 2
    cooldown_after_close_trend: int = 30
    cooldown_after_close_range: int = 30

    # 슬리피지/호가
    max_price_jump_pct: float = 0.003
    max_spread_pct: float = 0.0008
    max_entry_slippage_pct: float = 0.0005
    use_orderbook_entry_hint: bool = True

    # 텔레그램
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    notify_on_entry: bool = True
    notify_on_close: bool = True
    unrealized_notify_enabled: bool = False
    unrealized_notify_sec: int = 1800

    # KRW 환산
    krw_per_usdt: float = 1400.0

    # 로그
    log_to_file: bool = False
    log_file: str = "bot.log"

    # 캔들 실패
    max_kline_fails: int = 5
    kline_fail_sleep: int = 600

    # 거래 시간대
    trading_sessions_utc: str = "0-23"

    # health server
    health_port: int = 0

    # 박스 하루 손절 횟수 제한
    range_max_daily_sl: int = 2

    # 텔레그램 스팸 억제
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

    # 마진 기준 TP/SL
    use_margin_based_tp_sl: bool = False
    fut_tp_margin_pct: float = 0.5
    fut_sl_margin_pct: float = 0.5

    # 박스 단계화 / 동적 TP
    range_strict_level: int = 0
    use_range_dynamic_tp: bool = False
    range_tp_min: float = 0.0035
    range_tp_max: float = 0.0065

    def as_dict(self) -> Dict[str, object]:
        return self.__dict__.copy()


def load_settings() -> BotSettings:
    api_key = os.getenv("BINGX_API_KEY", "")
    api_secret = os.getenv("BINGX_API_SECRET", "")

    api_key = _ensure_ascii_env(api_key, "BINGX_API_KEY")
    api_secret = _ensure_ascii_env(api_secret, "BINGX_API_SECRET")

    return BotSettings(
        api_key=api_key,
        api_secret=api_secret,
        # 기본 심볼/주기
        symbol=os.getenv("SYMBOL", "BTC-USDT"),
        interval=os.getenv("INTERVAL", "3m"),
        # 전략 on/off
        enable_trend=_as_bool(os.getenv("ENABLE_TREND", "1"), True),
        enable_range=_as_bool(os.getenv("ENABLE_RANGE", "0"), False),
        enable_1m_confirm=_as_bool(os.getenv("ENABLE_1M_CONFIRM", "1"), True),
        # 레버리지/마진/리스크
        leverage=_as_int(os.getenv("LEVERAGE", "10"), 10),
        isolated=_as_bool(os.getenv("ISOLATED", "1"), True),
        risk_pct=_as_float(os.getenv("RISK_PCT", "0.3"), 0.3),
        min_notional_usdt=_as_float(os.getenv("MIN_NOTIONAL_USDT", "5"), 5.0),
        max_notional_usdt=_as_float(os.getenv("MAX_NOTIONAL_USDT", "999999"), 999999.0),
        # TP/SL
        tp_pct=_as_float(os.getenv("TP_PCT", "0.02"), 0.02),
        sl_pct=_as_float(os.getenv("SL_PCT", "0.02"), 0.02),
        range_tp_pct=_as_float(os.getenv("RANGE_TP_PCT", "0.006"), 0.006),
        range_sl_pct=_as_float(os.getenv("RANGE_SL_PCT", "0.004"), 0.004),
        # 박스 비대칭
        range_tp_long_pct=_as_float(os.getenv("RANGE_TP_LONG_PCT", "0.004"), 0.004),
        range_tp_short_pct=_as_float(os.getenv("RANGE_TP_SHORT_PCT", "0.006"), 0.006),
        range_sl_long_pct=_as_float(os.getenv("RANGE_SL_LONG_PCT", "0.0035"), 0.0035),
        range_sl_short_pct=_as_float(os.getenv("RANGE_SL_SHORT_PCT", "0.004"), 0.004),
        # 박스 진입 위치/보정
        range_entry_upper_pct=_as_float(os.getenv("RANGE_ENTRY_UPPER_PCT", "0.80"), 0.80),
        range_entry_lower_pct=_as_float(os.getenv("RANGE_ENTRY_LOWER_PCT", "0.20"), 0.20),
        range_soft_tp_factor=_as_float(os.getenv("RANGE_SOFT_TP_FACTOR", "1.2"), 1.2),
        range_short_sl_floor_ratio=_as_float(
            os.getenv("RANGE_SHORT_SL_FLOOR_RATIO", "0.75"),
            0.75,
        ),
        # ✅ 박스 조기 청산/익절
        range_early_exit_enabled=_as_bool(os.getenv("RANGE_EARLY_EXIT_ENABLED", "1"), True),
        range_early_exit_loss_pct=_as_float(os.getenv("RANGE_EARLY_EXIT_LOSS_PCT", "0.003"), 0.003),
        range_early_tp_enabled=_as_bool(os.getenv("RANGE_EARLY_TP_ENABLED", "0"), False),
        range_early_tp_pct=_as_float(os.getenv("RANGE_EARLY_TP_PCT", "0.0025"), 0.0025),
        # ✅ 추세 조기 청산/익절
        trend_early_exit_enabled=_as_bool(os.getenv("TREND_EARLY_EXIT_ENABLED", "1"), True),
        trend_early_exit_loss_pct=_as_float(os.getenv("TREND_EARLY_EXIT_LOSS_PCT", "0.003"), 0.003),
        trend_early_tp_enabled=_as_bool(os.getenv("TREND_EARLY_TP_ENABLED", "0"), False),
        trend_early_tp_pct=_as_float(os.getenv("TREND_EARLY_TP_PCT", "0.0025"), 0.0025),
        # ✅ 추세 횡보 감지 (기본 주기)
        trend_sideways_enabled=_as_bool(os.getenv("TREND_SIDEWAYS_ENABLED", "1"), True),
        trend_sideways_need_bars=_as_int(os.getenv("TREND_SIDEWAYS_NEED_BARS", "3"), 3),
        trend_sideways_range_pct=_as_float(os.getenv("TREND_SIDEWAYS_RANGE_PCT", "0.0008"), 0.0008),
        trend_sideways_max_pnl_pct=_as_float(os.getenv("TREND_SIDEWAYS_MAX_PNL_PCT", "0.0015"), 0.0015),
        # ✅ 추세 횡보 1m 보조
        trend_sideways_use_1m=_as_bool(os.getenv("TREND_SIDEWAYS_USE_1M", "1"), True),
        trend_sideways_1m_need_bars=_as_int(os.getenv("TREND_SIDEWAYS_1M_NEED_BARS", "3"), 3),
        trend_sideways_1m_range_pct=_as_float(os.getenv("TREND_SIDEWAYS_1M_RANGE_PCT", "0.0006"), 0.0006),
        # ✅ 박스 → 추세 업그레이드 최소 이익률
        range_to_trend_min_gain_pct=_as_float(os.getenv("RANGE_TO_TREND_MIN_GAIN_PCT", "0.002"), 0.002),
        # ✅ 반대 시그널 컷 on/off
        close_on_opposite_enabled=_as_bool(os.getenv("CLOSE_ON_OPPOSITE_ENABLED", "1"), True),
        # ATR
        use_atr=_as_bool(os.getenv("USE_ATR", "1"), True),
        atr_len=_as_int(os.getenv("ATR_LEN", "20"), 20),
        atr_tp_mult=_as_float(os.getenv("ATR_TP_MULT", "2.0"), 2.0),
        atr_sl_mult=_as_float(os.getenv("ATR_SL_MULT", "1.2"), 1.2),
        min_tp_pct=_as_float(os.getenv("MIN_TP_PCT", "0.005"), 0.005),
        min_sl_pct=_as_float(os.getenv("MIN_SL_PCT", "0.005"), 0.005),
        atr_risk_high_mult=_as_float(os.getenv("ATR_RISK_HIGH_MULT", "1.5"), 1.5),
        atr_risk_reduction=_as_float(os.getenv("ATR_RISK_REDUCTION", "0.5"), 0.5),
        # 쿨다운/폴링
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
        # 가드
        max_price_jump_pct=_as_float(os.getenv("MAX_PRICE_JUMP_PCT", "0.003"), 0.003),
        max_spread_pct=_as_float(os.getenv("MAX_SPREAD_PCT", "0.0008"), 0.0008),
        max_entry_slippage_pct=_as_float(os.getenv("MAX_ENTRY_SLIPPAGE_PCT", "0.0005"), 0.0005),
        use_orderbook_entry_hint=_as_bool(os.getenv("USE_ORDERBOOK_ENTRY_HINT", "1"), True),
        # 텔레그램
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
        notify_on_entry=_as_bool(os.getenv("NOTIFY_ON_ENTRY", "1"), True),
        notify_on_close=_as_bool(os.getenv("NOTIFY_ON_CLOSE", "1"), True),
        unrealized_notify_enabled=_as_bool(os.getenv("UNREALIZED_NOTIFY_ENABLED", "0"), False),
        unrealized_notify_sec=_as_int(os.getenv("UNREALIZED_NOTIFY_SEC", "1800"), 1800),
        # KRW 환산
        krw_per_usdt=_as_float(os.getenv("KRW_PER_USDT", "1400"), 1400.0),
        # 로그/기타
        log_to_file=_as_bool(os.getenv("LOG_TO_FILE", "0"), False),
        log_file=os.getenv("LOG_FILE", "bot.log"),
        # 캔들 실패
        max_kline_fails=_as_int(os.getenv("MAX_KLINE_FAILS", "5"), 5),
        kline_fail_sleep=_as_int(os.getenv("KLINE_FAIL_SLEEP", "600"), 600),
        # 거래 시간대
        trading_sessions_utc=os.getenv("TRADING_SESSIONS_UTC", "0-23"),
        # health
        health_port=_as_int(os.getenv("HEALTH_PORT", "0"), 0),
        # 박스 하루 손절 제한
        range_max_daily_sl=_as_int(os.getenv("RANGE_MAX_DAILY_SL", "2"), 2),
        # 텔레그램 스팸
        skip_tg_cooldown=_as_int(os.getenv("SKIP_TG_COOLDOWN", "30"), 30),
        balance_skip_cooldown=3600,
        # 캔들 지연 허용
        max_kline_delay_sec=_as_int(os.getenv("MAX_KLINE_DELAY_SEC", "190"), 190),
        # RSI
        rsi_overbought=_as_int(os.getenv("RSI_OVERBOUGHT", "70"), 70),
        rsi_oversold=_as_int(os.getenv("RSI_OVERSOLD", "30"), 30),
        # 종료
        min_uptime_for_stop=_as_int(os.getenv("MIN_UPTIME_FOR_STOP", "5"), 5),
        # BingX base
        bingx_base="https://open-api.bingx.com",
        # 마진 기준 TP/SL
        use_margin_based_tp_sl=_as_bool(os.getenv("USE_MARGIN_BASED_TP_SL", "0"), False),
        fut_tp_margin_pct=_as_float(os.getenv("FUT_TP_MARGIN_PCT", "0.5"), 0.5),
        fut_sl_margin_pct=_as_float(os.getenv("FUT_SL_MARGIN_PCT", "0.5"), 0.5),
        # 박스 단계화/동적 TP
        range_strict_level=_as_int(os.getenv("RANGE_STRICT_LEVEL", "0"), 0),
        use_range_dynamic_tp=_as_bool(os.getenv("USE_RANGE_DYNAMIC_TP", "0"), False),
        range_tp_min=_as_float(os.getenv("RANGE_TP_MIN", "0.0035"), 0.0035),
        range_tp_max=_as_float(os.getenv("RANGE_TP_MAX", "0.0065"), 0.0065),
    )


if __name__ == "__main__":
    s = load_settings()
    for k, v in s.as_dict().items():
        print(f"{k}: {v}")
