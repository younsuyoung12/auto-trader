"""
settings.py
BingX 자동매매 봇 공통 설정 모듈
- 환경변수 읽기
- 기본 타임존, 상수 정의
- 다른 모듈에서 import 해서 사용

2025-11-12 (2차) 추가
----------------------------------------------------
(선물 변동성 대응 + 박스 진입 개선 아이디어 반영)

1) 박스 진입 위치를 설정으로 뺌
   - range_entry_upper_pct: 박스 상단 몇 %부터 숏으로 볼지 (기본 0.80 = 80%)
   - range_entry_lower_pct: 박스 하단 몇 %부터 롱으로 볼지 (기본 0.20 = 20%)
   → strategies_range.py 에서 지금은 0.80/0.20으로 하드코딩했지만
     이 값을 읽어서 쓰게 하면 선물에서 너무 앞에서 들어가는 문제를
     환경변수만으로 조절할 수 있다.
   환경변수:
     RANGE_ENTRY_UPPER_PCT, RANGE_ENTRY_LOWER_PCT

2) soft 허용일 때 TP를 살짝만 높게 쓸 수 있는 배율 추가
   - range_soft_tp_factor: float = 1.2
     → soft_atr, soft_ema 같은 날이라고 해도
       최소 TP(range_tp_min)의 1.2배까지만 쓰도록 상한을 줄 수 있다.
     → 전략단에서 soft_reason 들어왔을 때 이 값을 쓰면 된다.
   환경변수:
     RANGE_SOFT_TP_FACTOR

3) 숏일 때 SL을 TP의 몇 %까지는 따라가게 할지 설정으로 뺌
   - range_short_sl_floor_ratio: float = 0.75
     → 예: TP가 0.006 이면 SL 최소 0.006 * 0.75 = 0.0045 까지는 허용
     → 선물에서 위로 한 번만 털려도 바로 손절 나는 문제를 줄이기 위한 최소 폭
   환경변수:
     RANGE_SHORT_SL_FLOOR_RATIO

2025-11-12 추가
----------------------------------------------------
(요청 아이디어 반영)
1) 박스장에서 방향별 비대칭 TP/SL 을 줄 수 있도록 옵션을 추가했다.
   - range_tp_long_pct / range_tp_short_pct
   - range_sl_long_pct / range_sl_short_pct
   전략 모듈이 "이 진입은 롱이다"라고 판단했을 때 이 값을 우선해서 사용할 수 있다.
   환경변수:
     RANGE_TP_LONG_PCT, RANGE_TP_SHORT_PCT,
     RANGE_SL_LONG_PCT, RANGE_SL_SHORT_PCT

2) 박스 전략을 한 번에 끄지 말고 단계적으로 조이는 플래그를 추가했다.
   - range_strict_level: int = 0
     0 = 지금처럼 정상
     1 = 전략이 "오늘 애매"라고 해도 낮은 TP/SL 로라도 허용
     2 = 전략이 "오늘 애매"라고 하면 완전 off
   실제 판정/분기는 strategies_range.py 에서 이 숫자를 보고 처리하면 된다.
   환경변수: RANGE_STRICT_LEVEL

3) 박스 TP 를 캔들(또는 변동폭) 기반으로 가변화할 때 사용할 하한/상한을 추가했다.
   - use_range_dynamic_tp: bool = False
   - range_tp_min: float = 0.0035
   - range_tp_max: float = 0.0065
   이렇게 해두면 전략단에서 "오늘 캔들 작으니까 0.0038만 쓰자" 식으로 계산해서
   이 범위 안으로만 넣을 수 있다.
   환경변수:
     USE_RANGE_DYNAMIC_TP, RANGE_TP_MIN, RANGE_TP_MAX

2025-11-11 추가
----------------------------------------------------
(요구사항 반영)
1) 텔레그램 알림을 켜고/끄는 플래그를 설정에서 관리할 수 있게 했다.
   - notify_on_entry: 진입(order + TP/SL 세팅) 알림 보낼지 여부
   - notify_on_close: TP/SL 등으로 청산됐을 때 알림 보낼지 여부
2) 열려 있는 포지션이 있을 때 주기적으로 "얼마 수익/손실 중" 같은
   상태 메시지를 보내고 싶어 해서 주기 알림 옵션을 넣었다.
   - unrealized_notify_enabled: 주기 알림 켤지
   - unrealized_notify_sec: 주기(기본 1800초 = 30분)
   이 값들은 run_bot.py 에서 실제로 타이머 돌릴 때 참조하면 된다.
3) 텔레그램에 USDT 만 나가면 불편하니 KRW 로도 같이 보이게 하려면
   환율이 필요해서 krw_per_usdt 옵션을 추가했다.
   - krw_per_usdt: float = 1400.0
   - 환경변수 KRW_PER_USDT 로 덮어쓰기 가능
   이 값은 run_bot.py / trader.py 에서 PnL 메시지 만들 때
   `pnl_krw = pnl_usdt * settings.krw_per_usdt` 식으로 곱해서 쓰면 된다.

2025-11-10 추가
----------------------------------------------------
3) 진입 거래량 가드 완화용 옵션 추가
   - min_entry_volume_ratio: float = 0.15
     → 3분봉 진입 직전에 "이 캔들 거래량 / 최근 20개 평균 거래량"을 비교할 때 쓰는 하한선
     → 원래는 0.3 정도로 보수적으로 해서 얇은 장에서는 전부 SKIP이 나왔는데,
        여기서는 기본을 0.15로 내려서 평균의 15%만 돼도 진입을 허용하도록 했다.
     → 환경변수(MIN_ENTRY_VOLUME_RATIO)로도 덮어쓸 수 있다.

2025-11-09 수정/추가 내용
----------------------------------------------------
1) 진입 안전성 관련
   - 선물 중심 운영을 위해 진입 슬리피지 한도 옵션 추가:
     max_entry_slippage_pct: float = 0.0005 (0.05%)
     → 실제 체결가가 진입 힌트 가격에서 0.05% 이상 밀리면 trader.py 쪽에서 포기하도록 쓸 수 있음
   - 호가 기반 진입가 힌트 사용 여부 추가:
     use_orderbook_entry_hint: bool = True
     → run_bot.py 에서 best bid/ask 를 진입 힌트로 사용하도록 허용
   - 위 두 옵션은 환경변수(MAX_ENTRY_SLIPPAGE_PCT, USE_ORDERBOOK_ENTRY_HINT)로도 제어 가능

2) "현물 %가 아니라 선물(마진) 기준 %로 TP/SL을 잡고 싶다"는 요구 반영
   - use_margin_based_tp_sl: bool = False
     → 이걸 True로 켜면 fut_tp_margin_pct, fut_sl_margin_pct 값을
        (마진% / 100) / 레버리지 로 바꿔서 가격 TP/SL로 사용하게 할 수 있음
   - fut_tp_margin_pct: float = 0.5
   - fut_sl_margin_pct: float = 0.5
   - run_bot.py 쪽에서 이 옵션을 보고 변환해서 trader.py 로 넘겨주면 된다.
"""

from __future__ import annotations
import os
import datetime
from dataclasses import dataclass
from typing import Dict

# 한국 시간대 (KST)
KST = datetime.timezone(datetime.timedelta(hours=9))


def _as_bool(val: str, default: bool = False) -> bool:
    """문자열 환경변수를 bool 로 변환 ("1", "true", "True" → True).
    빈값이면 default 사용."""
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "y"}


def _as_int(val: str, default: int) -> int:
    """문자열을 int 로 변환, 실패하면 기본값."""
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def _as_float(val: str, default: float) -> float:
    """문자열을 float 로 변환, 실패하면 기본값."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _ensure_ascii_env(val: str, name: str) -> str:
    """요청 헤더 등에 올라갈 수 있는 문자열은 ASCII 만 남기도록 정리.
    - 한글/공백이 들어있으면 제거해서 안전하게 만든다.
    - 예외는 던지지 않고, 가능한 문자열만 되돌린다.
    """
    if not val:
        return val
    try:
        val.encode("ascii")
        return val
    except UnicodeEncodeError:
        cleaned = val.encode("ascii", "ignore").decode("ascii").strip()
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
    risk_pct: float = 0.3  # 계좌(가용)에서 이 비율만 사용
    min_notional_usdt: float = 5.0
    max_notional_usdt: float = 999999.0

    # 추세장 TP/SL 기본값 (가격 기준 퍼센트)
    tp_pct: float = 0.02
    sl_pct: float = 0.02

    # 박스장 전용 TP/SL (기본값)
    range_tp_pct: float = 0.01      # 박스도 1% 먹고 나오자
    range_sl_pct: float = 0.01      # 박스 손절도 1%

    # 2025-11-12: 박스장 방향별 비대칭용
    range_tp_long_pct: float = 0.01
    range_tp_short_pct: float = 0.01
    range_sl_long_pct: float = 0.01
    range_sl_short_pct: float = 0.01

    # 2025-11-12 (2차): 박스 진입 위치/보정용
    range_entry_upper_pct: float = 0.80  # 박스 상단 이 비율 이상에서만 숏
    range_entry_lower_pct: float = 0.20  # 박스 하단 이 비율 이하에서만 롱
    range_soft_tp_factor: float = 1.2    # soft일 때 최소 TP의 1.2배까지만
    range_short_sl_floor_ratio: float = 0.75  # 숏 SL은 TP의 75% 이상으로 보정

    # ATR 옵션
    use_atr: bool = True
    atr_len: int = 20
    atr_tp_mult: float = 2.0
    atr_sl_mult: float = 1.2
    min_tp_pct: float = 0.01
    min_sl_pct: float = 0.01   # ← 0.5% → 1%로 올림
    atr_risk_high_mult: float = 1.5
    atr_risk_reduction: float = 0.5

    # 2025-11-10: 진입 거래량 가드 하한
    min_entry_volume_ratio: float = 0.15

    # 쿨다운/폴링
    cooldown_sec: int = 15
    cooldown_after_close: int = 30
    cooldown_after_3loss: int = 3600
    poll_fills_sec: int = 2
    # 전략별 청산 후 쿨다운
    cooldown_after_close_trend: int = 30
    cooldown_after_close_range: int = 30

    # 슬리피지/호가 가드
    max_price_jump_pct: float = 0.003     # 직전 캔들 대비 급등락 보호
    max_spread_pct: float = 0.0008        # 호가 스프레드가 이보다 크면 진입 안 함
    max_entry_slippage_pct: float = 0.0005  # 0.05%
    use_orderbook_entry_hint: bool = True

    # 텔레그램
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    # ───── 2025-11-11: 텔레그램 알림 세분화 ─────
    notify_on_entry: bool = True          # 진입 완료 + TP/SL 세팅 완료 메시지
    notify_on_close: bool = True          # 청산(TP/SL/강제청산) 메시지
    unrealized_notify_enabled: bool = False  # 열려있는 포지션 상태 주기 알림
    unrealized_notify_sec: int = 1800        # 기본 30분

    # KRW 환산용
    krw_per_usdt: float = 1400.0

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

    # 2025-11-09: 선물(마진) 기준 TP/SL 옵션
    use_margin_based_tp_sl: bool = False
    fut_tp_margin_pct: float = 0.5
    fut_sl_margin_pct: float = 0.5

    # 2025-11-12: 박스 단계화 / 동적 TP 범위
    range_strict_level: int = 0
    use_range_dynamic_tp: bool = False
    range_tp_min: float = 0.0035
    range_tp_max: float = 0.0065

    def as_dict(self) -> Dict[str, object]:
        """필요시 dict 로 변환."""
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
        # TP/SL (가격 기준 기본값)
        tp_pct=_as_float(os.getenv("TP_PCT", "0.02"), 0.02),
        sl_pct=_as_float(os.getenv("SL_PCT", "0.02"), 0.02),
        range_tp_pct=_as_float(os.getenv("RANGE_TP_PCT", "0.006"), 0.006),
        range_sl_pct=_as_float(os.getenv("RANGE_SL_PCT", "0.004"), 0.004),
        # 2025-11-12: 박스 방향별 비대칭 환경변수
        range_tp_long_pct=_as_float(os.getenv("RANGE_TP_LONG_PCT", "0.004"), 0.004),
        range_tp_short_pct=_as_float(os.getenv("RANGE_TP_SHORT_PCT", "0.006"), 0.006),
        range_sl_long_pct=_as_float(os.getenv("RANGE_SL_LONG_PCT", "0.0035"), 0.0035),
        range_sl_short_pct=_as_float(os.getenv("RANGE_SL_SHORT_PCT", "0.004"), 0.004),
        # 2025-11-12 (2차): 박스 진입 위치/보정용
        range_entry_upper_pct=_as_float(os.getenv("RANGE_ENTRY_UPPER_PCT", "0.80"), 0.80),
        range_entry_lower_pct=_as_float(os.getenv("RANGE_ENTRY_LOWER_PCT", "0.20"), 0.20),
        range_soft_tp_factor=_as_float(os.getenv("RANGE_SOFT_TP_FACTOR", "1.2"), 1.2),
        range_short_sl_floor_ratio=_as_float(
            os.getenv("RANGE_SHORT_SL_FLOOR_RATIO", "0.75"), 0.75
        ),
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
        max_entry_slippage_pct=_as_float(
            os.getenv("MAX_ENTRY_SLIPPAGE_PCT", "0.0005"),
            0.0005,
        ),
        use_orderbook_entry_hint=_as_bool(
            os.getenv("USE_ORDERBOOK_ENTRY_HINT", "1"),
            True,
        ),
        # 텔레그램
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
        # 텔레그램 알림 세분화
        notify_on_entry=_as_bool(os.getenv("NOTIFY_ON_ENTRY", "1"), True),
        notify_on_close=_as_bool(os.getenv("NOTIFY_ON_CLOSE", "1"), True),
        unrealized_notify_enabled=_as_bool(os.getenv("UNREALIZED_NOTIFY_ENABLED", "0"), False),
        unrealized_notify_sec=_as_int(os.getenv("UNREALIZED_NOTIFY_SEC", "1800"), 1800),
        # KRW 환산
        krw_per_usdt=_as_float(os.getenv("KRW_PER_USDT", "1400"), 1400.0),
        # 로그/기타
        log_to_file=_as_bool(os.getenv("LOG_TO_FILE", "0"), False),
        log_file=os.getenv("LOG_FILE", "bot.log"),
        # 캔들 실패 관련
        max_kline_fails=_as_int(os.getenv("MAX_KLINE_FAILS", "5"), 5),
        kline_fail_sleep=_as_int(os.getenv("KLINE_FAIL_SLEEP", "600"), 600),
        # 거래 시간대(UTC 기준)
        trading_sessions_utc=os.getenv("TRADING_SESSIONS_UTC", "0-23"),
        # health server
        health_port=_as_int(os.getenv("HEALTH_PORT", "0"), 0),
        # 박스장 일일 손절 제한
        range_max_daily_sl=_as_int(os.getenv("RANGE_MAX_DAILY_SL", "2"), 2),
        # skip 텔레그램 스팸 억제
        skip_tg_cooldown=_as_int(os.getenv("SKIP_TG_COOLDOWN", "30"), 30),
        balance_skip_cooldown=3600,
        # 캔들 지연 허용치
        max_kline_delay_sec=_as_int(os.getenv("MAX_KLINE_DELAY_SEC", "190"), 190),
        # RSI 기준
        rsi_overbought=_as_int(os.getenv("RSI_OVERBOUGHT", "70"), 70),
        rsi_oversold=_as_int(os.getenv("RSI_OVERSOLD", "30"), 30),
        # 2025-11-10: 진입 거래량 가드 환경변수
        min_entry_volume_ratio=_as_float(os.getenv("MIN_ENTRY_VOLUME_RATIO", "0.15"), 0.15),
        # 종료 관련
        min_uptime_for_stop=_as_int(os.getenv("MIN_UPTIME_FOR_STOP", "5"), 5),
        # BingX base url
        bingx_base="https://open-api.bingx.com",
        # 선물(마진) 기준 TP/SL 환경변수
        use_margin_based_tp_sl=_as_bool(os.getenv("USE_MARGIN_BASED_TP_SL", "0"), False),
        fut_tp_margin_pct=_as_float(os.getenv("FUT_TP_MARGIN_PCT", "0.5"), 0.5),
        fut_sl_margin_pct=_as_float(os.getenv("FUT_SL_MARGIN_PCT", "0.5"), 0.5),
        # 2025-11-12: 박스 단계화 / 동적 TP
        range_strict_level=_as_int(os.getenv("RANGE_STRICT_LEVEL", "0"), 0),
        use_range_dynamic_tp=_as_bool(os.getenv("USE_RANGE_DYNAMIC_TP", "0"), False),
        range_tp_min=_as_float(os.getenv("RANGE_TP_MIN", "0.0035"), 0.0035),
        range_tp_max=_as_float(os.getenv("RANGE_TP_MAX", "0.0065"), 0.0065),
    )


# 모듈을 직접 실행했을 때 설정 값을 프린트해서 확인할 수 있게 함
if __name__ == "__main__":
    s = load_settings()
    for k, v in s.as_dict().items():
        print(f"{k}: {v}")
