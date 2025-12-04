"""
settings_ws.py
====================================================
BingX 선물 웹소켓/자동매매 공용 설정 모듈.

핵심 정책
----------------------------------------------------
- .env 에서 반드시 읽는 값
  * BINGX_API_KEY / BINGX_API_SECRET  (거래소 API 키)
  * TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID  (텔레그램 알림)
- 그 외 모든 수치는 이 파일의 BotSettings 기본값을 '직접 수정해서' 사용한다.
  환경 변수로 조정하지 않는다.

WS / REST 관련 기본값
----------------------------------------------------
- ws_subscribe_tfs      = ["1m", "5m", "15m"]
- ws_backfill_tfs       = ["1m", "5m", "15m"]
- ws_backfill_limit     = 60  (REST 백필 캔들 개수)
- ws_min_kline_buffer   = 60  (WS 헬스 체크용 최소 버퍼 길이)
- ws_max_kline_delay_sec= 120 (WS 헬스 체크용 최대 지연, 초)
- ws_orderbook_max_delay_sec = 10 (오더북 최대 허용 지연, 초)

신호 레벨 캔들 지연
----------------------------------------------------
- max_kline_delay_sec = 600초 (신호/전략 레벨에서 사용하는 비교적 느슨한 기준)

env 를 통해 값이 바뀌지 않도록 설계를 단순화했고,
실제 동작 파라미터는 모두 이 파일 하나에서 관리한다.
"""

from __future__ import annotations

import os
import datetime
from dataclasses import dataclass
from typing import Dict, List

# 한국 시간대 (KST)
KST = datetime.timezone(datetime.timedelta(hours=9))


def _as_bool(val: str | None, default: bool = False) -> bool:
    """이제는 거의 사용하지 않지만, 하위 호환을 위해 남겨둔 헬퍼."""
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "y"}


def _as_int(val: str | None, default: int) -> int:
    """이제는 거의 사용하지 않지만, 하위 호환을 위해 남겨둔 헬퍼."""
    try:
        return int(val)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _as_float(val: str | None, default: float) -> float:
    """이제는 거의 사용하지 않지만, 하위 호환을 위해 남겨둔 헬퍼."""
    try:
        return float(val)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _ensure_ascii_env(val: str, name: str) -> str:
    """API 키처럼 헤더에 들어갈 값은 ASCII 만 남기도록 정리."""
    if not val:
        return val
    try:
        val.encode("ascii")
        return val
    except UnicodeEncodeError:
        return val.encode("ascii", "ignore").decode("ascii").strip()


@dataclass(frozen=True)
class BotSettings:
    """봇 전체에서 import 해서 공통으로 사용하는 설정 묶음."""

    # 필수 인증
    api_key: str
    api_secret: str

    # GPT 비용 보호 (그대로 유지)
    gpt_daily_call_limit: int = 500  # 500회 ≈ $0.50

    # 기본 심볼/주기
    symbol: str = "BTC-USDT"
    interval: str = "5m"

    # 웹소켓 기본
    ws_enabled: bool = True
    ws_base: str = "wss://open-api-ws.bingx.com"
    ws_swap_base: str = "wss://open-api-swap.bingx.com/swap-market"
    ws_subscribe_tfs: List[str] | None = None
    ws_log_enabled: bool = True

    # 디버깅용 원시/페이로드 로그
    ws_log_raw_enabled: bool = False
    ws_log_payload_enabled: bool = False

    # WS 히스토리 백필 & 헬스 기준
    ws_backfill_tfs: List[str] | None = None
    ws_backfill_limit: int = 60
    ws_required_tfs: List[str] | None = None
    ws_min_kline_buffer: int = 60
    ws_max_kline_delay_sec: int = 120
    ws_orderbook_max_delay_sec: int = 10

    # WS 웜업/부트스트랩
    min_bars_5m: int = 20
    min_bars_15m: int = 20
    warmup_target_5m: int = 50
    warmup_target_15m: int = 50
    ws_bootstrap_with_rest: bool = True
    ws_bootstrap_lookback_5m: int = 120
    ws_bootstrap_lookback_15m: int = 120

    # 전략 on/off
    enable_market: bool = True
    enable_1m_confirm: bool = True

    # 레버리지/리스크 (기본값은 이전 ENV 기본값과 동일하게 맞춤)
    leverage: int = 10
    isolated: bool = True
    risk_pct: float = 0.3
    min_notional_usdt: float = 5.0
    max_notional_usdt: float = 999999.0

    # 기본 TP/SL
    tp_pct: float = 0.01   # 1%
    sl_pct: float = 0.02   # 2%
    # ATR/변동성 보정
    use_atr: bool = True
    atr_len: int = 20
    atr_tp_mult: float = 2.0
    atr_sl_mult: float = 1.2
    min_tp_pct: float = 0.005
    min_sl_pct: float = 0.005
    atr_risk_high_mult: float = 1.5
    atr_risk_reduction: float = 0.5

    # 저변동성(박스장) 필터 기준
    low_vol_range_pct_threshold: float = 0.01
    low_vol_atr_pct_threshold: float = 0.004

    # GPT 진입 게이트/TP·SL 제약
    gpt_error_sleep_sec: float = 5.0
    gpt_skip_sleep_sec: float = 3.0
    gpt_max_risk_pct: float = 0.03
    gpt_entry_cooldown_sec: int = 1   # 기존 ENV 기본값(1초)을 그대로 사용
    entry_cooldown_sec: int = 45

    # GPT가 제안하는 TP/SL 범위
    gpt_min_tp_pct: float = 0.01
    gpt_max_tp_pct: float = 0.10
    gpt_min_sl_pct: float = 0.0
    gpt_max_sl_pct: float = 0.02

    post_entry_sleep_sec: float = 5.0

    # GPT soft TP 재판단 옵션
    gpt_soft_tp_enabled: bool = True
    gpt_soft_tp_pct: float = 0.01
    gpt_soft_tp_recheck_sec: int = 60

    # 진입 거래량 가드
    min_entry_volume_ratio: float = 0.3  # 기존 ENV 기본값(0.3)에 맞춤

    # 쿨다운/폴링
    cooldown_sec: int = 15
    cooldown_after_close: int = 1
    max_consecutive_losses: int = 2
    cooldown_after_consec_loss_sec: int = 10800

    poll_fills_sec: int = 2

    # 슬리피지/호가 관련
    max_price_jump_pct: float = 0.006
    max_spread_pct: float = 0.002
    max_entry_slippage_pct: float = 0.0005
    use_orderbook_entry_hint: bool = True

    # 선물 틱 기반 슬리피지 가드
    price_tick_size: float = 0.1
    max_entry_slippage_ticks: int = 5
    auto_close_on_heavy_slippage: bool = True

    # 호가 한쪽 쏠림(depth imbalance) 가드
    depth_imbalance_enabled: bool = True
    depth_imbalance_min_notional: float = 50.0
    depth_imbalance_min_ratio: float = 2.0

    # mark/last 괴리 가드
    price_deviation_guard_enabled: bool = True
    price_deviation_max_pct: float = 0.0015

    # 세션별 스프레드/점프 배수 (UTC)
    session_spread_mult_asia: float = 1.0
    session_spread_mult_eu: float = 1.1
    session_spread_mult_us: float = 1.2
    session_jump_mult_asia: float = 1.0
    session_jump_mult_eu: float = 1.1
    session_jump_mult_us: float = 1.2

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

    # 거래 시간대 (UTC)
    trading_sessions_utc: str = "0-23"

    # health server
    health_port: int = 0

    # 텔레그램 스팸 억제
    skip_tg_cooldown: int = 30
    balance_skip_cooldown: int = 3600

    # 캔들 지연 허용치 (신호 레벨)
    max_kline_delay_sec: int = 600

    # RSI 기준
    rsi_overbought: int = 70
    rsi_oversold: int = 30

    # 종료 관련
    min_uptime_for_stop: int = 5

    # BingX REST base url
    bingx_base: str = "https://open-api.bingx.com"

    # 마진 기준 TP/SL
    use_margin_based_tp_sl: bool = False
    fut_tp_margin_pct: float = 0.5
    fut_sl_margin_pct: float = 0.5

    # 반대 시그널 감지 즉시 청산 on/off
    close_on_opposite_enabled: bool = True

    def as_dict(self) -> Dict[str, object]:
        """dict 변환(로깅/디버깅용)."""
        return self.__dict__.copy()


def load_settings() -> BotSettings:
    """ENV 를 읽어 BotSettings 인스턴스를 생성한다.

    지금은 다음 값만 ENV 에서 읽는다.
    - BINGX_API_KEY / BINGX_API_SECRET
    - TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID

    나머지 수치는 모두 이 파일 안의 상수값(기본값)을 그대로 사용한다.
    """
    api_key = os.getenv("BINGX_API_KEY", "")
    api_secret = os.getenv("BINGX_API_SECRET", "")
    api_key = _ensure_ascii_env(api_key, "BINGX_API_KEY")
    api_secret = _ensure_ascii_env(api_secret, "BINGX_API_SECRET")

    # 심볼/기본 주기 (필요하면 직접 여기 값을 바꾸면 됨)
    symbol = "BTC-USDT"
    interval = "5m"

    # WS 구독/백필 타임프레임 및 헬스 기준(하드코딩)
    ws_tfs = ["1m", "5m", "15m"]
    ws_backfill_tfs = ["1m", "5m", "15m"]
    ws_backfill_limit = 60
    ws_required_tfs = None  # 필요하면 나중에 사용

    ws_min_kline_buffer = 60
    ws_max_kline_delay_sec = 120
    ws_orderbook_max_delay_sec = 10

    # 1m 보조 확인: 기본값 True
    enable_1m_confirm_value = True

    return BotSettings(
        api_key=api_key,
        api_secret=api_secret,
        symbol=symbol,
        interval=interval,
        # WS
        ws_enabled=True,
        ws_base="wss://open-api-ws.bingx.com",
        ws_swap_base="wss://open-api-swap.bingx.com/swap-market",
        ws_subscribe_tfs=ws_tfs,
        ws_log_enabled=True,
        ws_log_raw_enabled=False,
        ws_log_payload_enabled=False,
        # WS 백필/헬스 설정
        ws_backfill_tfs=ws_backfill_tfs,
        ws_backfill_limit=ws_backfill_limit,
        ws_required_tfs=ws_required_tfs,
        ws_min_kline_buffer=ws_min_kline_buffer,
        ws_max_kline_delay_sec=ws_max_kline_delay_sec,
        ws_orderbook_max_delay_sec=ws_orderbook_max_delay_sec,
        # 전략 on/off
        enable_market=True,
        enable_1m_confirm=enable_1m_confirm_value,
        # 텔레그램
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
        # BingX REST BASE
        bingx_base="https://open-api.bingx.com",
    )


if __name__ == "__main__":
    s = load_settings()
    for k, v in s.as_dict().items():
        print(f"{k}: {v}")
