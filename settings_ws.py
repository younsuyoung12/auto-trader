"""
settings_ws.py
====================================================
웹소켓으로 1m / 5m / 15m 캔들을 받아서 쓰는 구조에 맞춰 정리한 설정 모듈.

▶ 2025-11-15 패치 (WS 로우데이터 로그 옵션 추가)
----------------------------------------------------
H) WS 로우데이터/페이로드 로그 토글 추가
   - ws_log_raw_enabled: BingX WS 원시 프레임(raw) 로그 ON/OFF
       · ENV: WS_LOG_RAW_ENABLED (기본 0)
   - ws_log_payload_enabled: 정규화된 kline/depth payload 내용 로그 ON/OFF
       · ENV: WS_LOG_PAYLOAD_ENABLED (기본 0)
   - market_data_ws.py 에서 이 값을 읽어, 필요할 때만 상세 로우데이터를 Render 로그에 남길 수 있게 한다.

▶ 2025-11-14 패치 (warmup/bootstrap 포함) — 이번 변경 핵심
----------------------------------------------------
E) RANGE 전용 1m 확인 토글(ENV) 추가
   - enable_1m_confirm_range 필드 추가
   - ENV: ENABLE_1M_CONFIRM_RANGE (미지정 시 ENABLE_1M_CONFIRM 값으로 Fallback)

F) TREND→RANGE 다운그레이드(갈아타기) 제어 옵션 추가
   - trend_to_range_enable (ENV: TREND_TO_RANGE_ENABLE)
   - trend_to_range_min_gain_pct (ENV: TREND_TO_RANGE_MIN_GAIN_PCT)
   - trend_to_range_max_abs_pnl_pct (ENV: TREND_TO_RANGE_MAX_ABS_PNL_PCT)
   - trend_to_range_auto_reenter (ENV: TREND_TO_RANGE_AUTO_REENTER)

G) WS 히스토리 웜업/부트스트랩 옵션 추가 (신규)
   - min_bars_5m, min_bars_15m: 최소 캔들 개수 기준(미만이면 신호 스킵)
   - warmup_target_5m, warmup_target_15m: 웜업 타깃(미만이어도 진행하되 로그만)
   - ws_bootstrap_with_rest: 부팅 직후 REST로 과거 캔들을 1회 시드 여부
   - ws_bootstrap_lookback_5m, ws_bootstrap_lookback_15m: REST 시드 lookback 개수
   - 관련 ENV: MIN_BARS_5M, MIN_BARS_15M, WARMUP_TARGET_5M, WARMUP_TARGET_15M,
               WS_BOOTSTRAP_WITH_REST, WS_BOOTSTRAP_LOOKBACK_5M, WS_BOOTSTRAP_LOOKBACK_15M

▶ 2025-11-13 추가 보정 (이 버전에서 바뀐 핵심)
----------------------------------------------------
A) ENV 훅 추가
   - MIN_ENTRY_VOLUME_RATIO를 로더에서 읽어 BotSettings.min_entry_volume_ratio에 주입

B) TP/SL 하한 기본값 정합성
   - dataclass 기본값 min_tp_pct, min_sl_pct을 **0.005(=0.5%)**로 낮춰 로더 기본값과 일치

C) 레인지 TP/SL 기본값 정합성
   - dataclass 기본값을 로더 기본값과 맞춤:
     * range_tp_pct=0.006, range_sl_pct=0.004
     * range_tp_long_pct=0.004, range_tp_short_pct=0.006
     * range_sl_long_pct=0.0035, range_sl_short_pct=0.004

D) WS 스왑 엔드포인트 옵션 추가
   - ws_swap_base 필드와 BINGX_SWAP_WS_BASE ENV 추가 (필요 모듈에서 선택적으로 사용)

※ 참고: 박스 상/하단 80%/20% 진입선은 strategies_range_ws.decide_signal_range()에 **하드코딩**되어 있습니다.
   본 파일의 range_entry_upper_pct/lower_pct 값은 보존하되, 실제 적용은 해당 전략 모듈 수정 시 반영됩니다.

기존 2025-11-13 변경 사항
----------------------------------------------------
1) 기본 interval 을 3m → 5m 로 변경했다. (이제 3m 는 사용하지 않는다.)
2) WS 전용 플래그(ws_enabled)와 WS 엔드포인트(env: BINGX_WS_BASE)를 추가해서
   Render 에서도 어떤 모드로 돌고 있는지 로그로 확인할 수 있게 했다.
3) run_bot / market_data_ws 에서 참조할 수 있도록 "ws_subscribe_tfs" 리스트를 추가해
   1m, 5m, 15m 세 타임프레임을 한 번에 구독하도록 했다.
4) 기존 BingX REST BASE 는 그대로 두되, env BINGX_BASE 가 있으면 그걸 우선한다.
5) position_watch 가 사용하는 close_on_opposite_enabled 주석을 유지했다.
"""

from __future__ import annotations
import os
import datetime
from dataclasses import dataclass
from typing import Dict, List

# 한국 시간대 (KST)
KST = datetime.timezone(datetime.timedelta(hours=9))


def _as_bool(val: str, default: bool = False) -> bool:
    """ENV 문자열을 불리언으로.
    허용: 1/true/yes/y (대소문자 무시). None이면 default.
    """
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "y"}


def _as_int(val: str, default: int) -> int:
    """ENV 문자열을 int로 안전 변환."""
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def _as_float(val: str, default: float) -> float:
    """ENV 문자열을 float으로 안전 변환."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _ensure_ascii_env(val: str, name: str) -> str:
    """API 키처럼 헤더에 들어갈 값은 ASCII 로만 정리한다.
    비ASCII 문자가 섞여 있으면 제거해서 안전하게 만든다.
    """
    if not val:
        return val
    try:
        val.encode("ascii")
        return val
    except UnicodeEncodeError:
        return val.encode("ascii", "ignore").decode("ascii").strip()


@dataclass(frozen=True)
class BotSettings:
    """봇 전체가 import 해서 공통으로 쓰는 설정 묶음 (WS 전용)
    - dataclass는 불변(frozen)으로 유지: 런타임 변조를 방지하고, 구성은 loader에서만.
    """

    # 필수 인증
    api_key: str
    api_secret: str

    # 기본 심볼/주기 (3m → 5m)
    symbol: str = "BTC-USDT"
    interval: str = "5m"  # 웹소켓에서 기본으로 보는 주기

    # 웹소켓 관련
    ws_enabled: bool = True  # 웹소켓 사용 여부
    ws_base: str = "wss://open-api-ws.bingx.com"  # env 로 바뀔 수 있음
    ws_swap_base: str = "wss://open-api-swap.bingx.com/swap-market"  # 선택적: 스왑 마켓 전용 WS
    ws_subscribe_tfs: List[str] = None  # 런타임에서 ["1m","5m","15m"]로 채운다
    ws_log_enabled: bool = True  # 캔들 수신 시 단순 로그 남길지
    # WS 원시/페이로드 로그 (Render 디버깅용, 기본 OFF 권장)
    ws_log_raw_enabled: bool = False      # WS 원시 프레임 로깅 (ENV: WS_LOG_RAW_ENABLED)
    ws_log_payload_enabled: bool = False  # kline/depth payload 상세 로깅 (ENV: WS_LOG_PAYLOAD_ENABLED)

    # ── WS 히스토리 웜업/부트스트랩 ─────────────────────────
    # * min_bars_*: 이 값 미만이면 신호 자체를 스킵
    # * warmup_target_*: 이 값 미만이면 진행은 하되 "웜업 진행" 로그만 출력
    # * ws_bootstrap_with_rest: 부팅 직후 REST로 과거 캔들을 1회 시드하여 웜업 시간을 단축
    # * ws_bootstrap_lookback_*: REST 시드 시 가져올 lookback 수량
    min_bars_5m: int = 20
    min_bars_15m: int = 20
    warmup_target_5m: int = 50
    warmup_target_15m: int = 50
    ws_bootstrap_with_rest: bool = True
    ws_bootstrap_lookback_5m: int = 120
    ws_bootstrap_lookback_15m: int = 120

    # 전략 on/off
    enable_trend: bool = True
    enable_range: bool = False
    enable_1m_confirm: bool = True
    enable_1m_confirm_range: bool = False  # RANGE 전용 1m 확인(미지정 시 enable_1m_confirm으로 fallback)

    # 레버리지/리스크
    leverage: int = 10
    isolated: bool = True
    risk_pct: float = 0.3
    min_notional_usdt: float = 5.0
    max_notional_usdt: float = 999999.0

    # 추세장 TP/SL
    tp_pct: float = 0.02
    sl_pct: float = 0.02

    # 박스 기본 TP/SL (로더 기본과 일치)
    range_tp_pct: float = 0.006
    range_sl_pct: float = 0.004

    # 박스 방향별 비대칭 (로더 기본과 일치)
    range_tp_long_pct: float = 0.004
    range_tp_short_pct: float = 0.006
    range_sl_long_pct: float = 0.0035
    range_sl_short_pct: float = 0.004

    # 박스 진입 위치/보정
    range_entry_upper_pct: float = 0.80
    range_entry_lower_pct: float = 0.20
    range_soft_tp_factor: float = 1.2
    range_short_sl_floor_ratio: float = 0.75

    # 박스 조기 청산/익절
    range_early_exit_enabled: bool = True
    range_early_exit_loss_pct: float = 0.003
    range_early_tp_enabled: bool = False
    range_early_tp_pct: float = 0.0025

    # 추세 조기 청산/익절
    trend_early_exit_enabled: bool = True
    trend_early_exit_loss_pct: float = 0.003
    trend_early_tp_enabled: bool = False
    trend_early_tp_pct: float = 0.0025

    # 추세 횡보 감지
    trend_sideways_enabled: bool = True
    trend_sideways_need_bars: int = 3
    trend_sideways_range_pct: float = 0.0008
    trend_sideways_max_pnl_pct: float = 0.0015

    # 추세 횡보 1m 보조
    trend_sideways_use_1m: bool = True
    trend_sideways_1m_need_bars: int = 3
    trend_sideways_1m_range_pct: float = 0.0006

    # 박스 → 추세 업그레이드 최소 이익률
    range_to_trend_min_gain_pct: float = 0.002

    # 반대 시그널 감지 즉시 청산 on/off
    close_on_opposite_enabled: bool = True

    # ATR/변동성
    use_atr: bool = True
    atr_len: int = 20
    atr_tp_mult: float = 2.0
    atr_sl_mult: float = 1.2
    min_tp_pct: float = 0.005  # 0.5%
    min_sl_pct: float = 0.005  # 0.5%
    atr_risk_high_mult: float = 1.5
    atr_risk_reduction: float = 0.5

    # 진입 거래량 가드 (ENV 훅)
    min_entry_volume_ratio: float = 0.3  # 기본 0.30

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

    # 호가 한쪽 쏠림(depth imbalance) 가드
    depth_imbalance_enabled: bool = True
    depth_imbalance_min_notional: float = 50.0
    depth_imbalance_min_ratio: float = 2.0

    # mark/last 괴리 가드
    price_deviation_guard_enabled: bool = True
    price_deviation_max_pct: float = 0.0015  # 0.15%

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

    # 거래 시간대 (UTC 기준 문자열)
    trading_sessions_utc: str = "0-23"

    # health server
    health_port: int = 0

    # 박스 하루 손절 제한
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

    # BingX base url (env 우선)
    bingx_base: str = "https://open-api.bingx.com"

    # 마진 기준 TP/SL
    use_margin_based_tp_sl: bool = False
    fut_tp_margin_pct: float = 0.5
    fut_sl_margin_pct: float = 0.5

    # 박스 단계화 / 동적 TP
    use_range_dynamic_tp: bool = False
    range_strict_level: int = 0
    range_tp_min: float = 0.0035
    range_tp_max: float = 0.0065

    # TREND→RANGE 다운그레이드 옵션 (보유 중 전략용)
    trend_to_range_enable: bool = False
    trend_to_range_min_gain_pct: float = 0.0015
    trend_to_range_max_abs_pnl_pct: float = 0.0015
    trend_to_range_auto_reenter: bool = True

    def as_dict(self) -> Dict[str, object]:
        """dict 변환(로깅/디버깅용)."""
        d = self.__dict__.copy()
        return d


def load_settings() -> BotSettings:
    """ENV를 읽어 BotSettings 인스턴스를 생성한다.
    - 문자열 파싱은 _as_* 헬퍼를 통해 일관 처리
    - enable_1m_confirm_range는 미지정 시 enable_1m_confirm로 fallback
    - WS 구독 타임프레임은 기본 ["1m","5m","15m"]
    """
    api_key = os.getenv("BINGX_API_KEY", "")
    api_secret = os.getenv("BINGX_API_SECRET", "")
    api_key = _ensure_ascii_env(api_key, "BINGX_API_KEY")
    api_secret = _ensure_ascii_env(api_secret, "BINGX_API_SECRET")

    # REST BASE
    bingx_base_env = os.getenv("BINGX_BASE", "https://open-api.bingx.com")
    # WS BASES
    bingx_ws_base_env = os.getenv("BINGX_WS_BASE", "wss://open-api-ws.bingx.com")
    bingx_ws_swap_base_env = os.getenv("BINGX_SWAP_WS_BASE", "wss://open-api-swap.bingx.com/swap-market")

    ws_tfs = ["1m", "5m", "15m"]

    # 먼저 공통 1m 확인 값을 계산 (RANGE Fallback에 사용)
    enable_1m_confirm_value = _as_bool(os.getenv("ENABLE_1M_CONFIRM", "1"), True)
    range_confirm_env = os.getenv("ENABLE_1M_CONFIRM_RANGE")
    if range_confirm_env is None:
        enable_1m_confirm_range_value = enable_1m_confirm_value  # Fallback
    else:
        enable_1m_confirm_range_value = _as_bool(range_confirm_env, enable_1m_confirm_value)

    return BotSettings(
        api_key=api_key,
        api_secret=api_secret,
        symbol=os.getenv("SYMBOL", "BTC-USDT"),
        interval=os.getenv("INTERVAL", "5m"),  # 3m → 5m
        # WS
        ws_enabled=_as_bool(os.getenv("WS_ENABLED", "1"), True),
        ws_base=bingx_ws_base_env,
        ws_swap_base=bingx_ws_swap_base_env,
        ws_subscribe_tfs=ws_tfs,
        ws_log_enabled=_as_bool(os.getenv("WS_LOG_ENABLED", "1"), True),
        # WS 원시/페이로드 로그 ENV 매핑 (디버깅 시에만 ON 권장)
        ws_log_raw_enabled=_as_bool(os.getenv("WS_LOG_RAW_ENABLED", "0"), False),
        ws_log_payload_enabled=_as_bool(os.getenv("WS_LOG_PAYLOAD_ENABLED", "0"), False),
        # ── WS 웜업/부트스트랩 ENV 매핑 ──
        min_bars_5m=_as_int(os.getenv("MIN_BARS_5M", "20"), 20),
        min_bars_15m=_as_int(os.getenv("MIN_BARS_15M", "20"), 20),
        warmup_target_5m=_as_int(os.getenv("WARMUP_TARGET_5M", "50"), 50),
        warmup_target_15m=_as_int(os.getenv("WARMUP_TARGET_15M", "50"), 50),
        ws_bootstrap_with_rest=_as_bool(os.getenv("WS_BOOTSTRAP_WITH_REST", "1"), True),
        ws_bootstrap_lookback_5m=_as_int(os.getenv("WS_BOOTSTRAP_LOOKBACK_5M", "120"), 120),
        ws_bootstrap_lookback_15m=_as_int(os.getenv("WS_BOOTSTRAP_LOOKBACK_15M", "120"), 120),
        # 전략 on/off
        enable_trend=_as_bool(os.getenv("ENABLE_TREND", "1"), True),
        enable_range=_as_bool(os.getenv("ENABLE_RANGE", "0"), False),
        enable_1m_confirm=enable_1m_confirm_value,
        enable_1m_confirm_range=enable_1m_confirm_range_value,
        # 레버리지/리스크
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
        # 박스 진입 위치/보정 (전략 모듈에서 실제 사용 여부 주의)
        range_entry_upper_pct=_as_float(os.getenv("RANGE_ENTRY_UPPER_PCT", "0.80"), 0.80),
        range_entry_lower_pct=_as_float(os.getenv("RANGE_ENTRY_LOWER_PCT", "0.20"), 0.20),
        range_soft_tp_factor=_as_float(os.getenv("RANGE_SOFT_TP_FACTOR", "1.2"), 1.2),
        range_short_sl_floor_ratio=_as_float(os.getenv("RANGE_SHORT_SL_FLOOR_RATIO", "0.75"), 0.75),
        # 박스 조기 청산/익절
        range_early_exit_enabled=_as_bool(os.getenv("RANGE_EARLY_EXIT_ENABLED", "1"), True),
        range_early_exit_loss_pct=_as_float(os.getenv("RANGE_EARLY_EXIT_LOSS_PCT", "0.003"), 0.003),
        range_early_tp_enabled=_as_bool(os.getenv("RANGE_EARLY_TP_ENABLED", "0"), False),
        range_early_tp_pct=_as_float(os.getenv("RANGE_EARLY_TP_PCT", "0.0025"), 0.0025),
        # 추세 조기 청산/익절
        trend_early_exit_enabled=_as_bool(os.getenv("TREND_EARLY_EXIT_ENABLED", "1"), True),
        trend_early_exit_loss_pct=_as_float(os.getenv("TREND_EARLY_EXIT_LOSS_PCT", "0.003"), 0.003),
        trend_early_tp_enabled=_as_bool(os.getenv("TREND_EARLY_TP_ENABLED", "0"), False),
        trend_early_tp_pct=_as_float(os.getenv("TREND_EARLY_TP_PCT", "0.0025"), 0.0025),
        # 추세 횡보 감지
        trend_sideways_enabled=_as_bool(os.getenv("TREND_SIDEWAYS_ENABLED", "1"), True),
        trend_sideways_need_bars=_as_int(os.getenv("TREND_SIDEWAYS_NEED_BARS", "3"), 3),
        trend_sideways_range_pct=_as_float(os.getenv("TREND_SIDEWAYS_RANGE_PCT", "0.0008"), 0.0008),
        trend_sideways_max_pnl_pct=_as_float(os.getenv("TREND_SIDEWAYS_MAX_PNL_PCT", "0.0015"), 0.0015),
        # 추세 횡보 1m 보조
        trend_sideways_use_1m=_as_bool(os.getenv("TREND_SIDEWAYS_USE_1M", "1"), True),
        trend_sideways_1m_need_bars=_as_int(os.getenv("TREND_SIDEWAYS_1M_NEED_BARS", "3"), 3),
        trend_sideways_1m_range_pct=_as_float(os.getenv("TREND_SIDEWAYS_1M_RANGE_PCT", "0.0006"), 0.0006),
        # 박스 → 추세 업그레이드
        range_to_trend_min_gain_pct=_as_float(os.getenv("RANGE_TO_TREND_MIN_GAIN_PCT", "0.002"), 0.002),
        # 반대 시그널 컷 on/off
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
        # 진입 거래량 가드 (신규 ENV 훅)
        min_entry_volume_ratio=_as_float(os.getenv("MIN_ENTRY_VOLUME_RATIO", "0.3"), 0.3),
        # 쿨다운/폴링
        cooldown_sec=_as_int(os.getenv("COOLDOWN_SEC", "15"), 15),
        cooldown_after_close=_as_int(os.getenv("COOLDOWN_AFTER_CLOSE", "30"), 30),
        cooldown_after_3loss=_as_int(os.getenv("COOLDOWN_AFTER_3LOSS", "3600"), 3600),
        poll_fills_sec=_as_int(os.getenv("POLL_FILLS_SEC", "2"), 2),
        cooldown_after_close_trend=_as_int(os.getenv("COOLDOWN_AFTER_CLOSE_TREND", "30"), 30),
        cooldown_after_close_range=_as_int(os.getenv("COOLDOWN_AFTER_CLOSE_RANGE", "30"), 30),
        # 가드
        max_price_jump_pct=_as_float(os.getenv("MAX_PRICE_JUMP_PCT", "0.003"), 0.003),
        max_spread_pct=_as_float(os.getenv("MAX_SPREAD_PCT", "0.0008"), 0.0008),
        max_entry_slippage_pct=_as_float(os.getenv("MAX_ENTRY_SLIPPAGE_PCT", "0.0005"), 0.0005),
        use_orderbook_entry_hint=_as_bool(os.getenv("USE_ORDERBOOK_ENTRY_HINT", "1"), True),
        # depth env
        depth_imbalance_enabled=_as_bool(os.getenv("DEPTH_IMBALANCE_ENABLED", "1"), True),
        depth_imbalance_min_notional=_as_float(os.getenv("DEPTH_IMBALANCE_MIN_NOTIONAL", "50"), 50.0),
        depth_imbalance_min_ratio=_as_float(os.getenv("DEPTH_IMBALANCE_MIN_RATIO", "2.0"), 2.0),
        # mark vs last env
        price_deviation_guard_enabled=_as_bool(os.getenv("PRICE_DEVIATION_GUARD_ENABLED", "1"), True),
        price_deviation_max_pct=_as_float(os.getenv("PRICE_DEVIATION_MAX_PCT", "0.0015"), 0.0015),
        # 세션 배수 env
        session_spread_mult_asia=_as_float(os.getenv("SESSION_SPREAD_MULT_ASIA", "1.0"), 1.0),
        session_spread_mult_eu=_as_float(os.getenv("SESSION_SPREAD_MULT_EU", "1.1"), 1.1),
        session_spread_mult_us=_as_float(os.getenv("SESSION_SPREAD_MULT_US", "1.2"), 1.2),
        session_jump_mult_asia=_as_float(os.getenv("SESSION_JUMP_MULT_ASIA", "1.0"), 1.0),
        session_jump_mult_eu=_as_float(os.getenv("SESSION_JUMP_MULT_EU", "1.1"), 1.1),
        session_jump_mult_us=_as_float(os.getenv("SESSION_JUMP_MULT_US", "1.2"), 1.2),
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
        # 캔들 지연 허용
        max_kline_delay_sec=_as_int(os.getenv("MAX_KLINE_DELAY_SEC", "190"), 190),
        # RSI
        rsi_overbought=_as_int(os.getenv("RSI_OVERBOUGHT", "70"), 70),
        rsi_oversold=_as_int(os.getenv("RSI_OVERSOLD", "30"), 30),
        # 종료
        min_uptime_for_stop=_as_int(os.getenv("MIN_UPTIME_FOR_STOP", "5"), 5),
        # BingX BASE
        bingx_base=bingx_base_env,
        # 마진 기준 TP/SL
        use_margin_based_tp_sl=_as_bool(os.getenv("USE_MARGIN_BASED_TP_SL", "0"), False),
        fut_tp_margin_pct=_as_float(os.getenv("FUT_TP_MARGIN_PCT", "0.5"), 0.5),
        fut_sl_margin_pct=_as_float(os.getenv("FUT_SL_MARGIN_PCT", "0.5"), 0.5),
        # 박스 단계화
        range_strict_level=_as_int(os.getenv("RANGE_STRICT_LEVEL", "0"), 0),
        use_range_dynamic_tp=_as_bool(os.getenv("USE_RANGE_DYNAMIC_TP", "0"), False),
        range_tp_min=_as_float(os.getenv("RANGE_TP_MIN", "0.0035"), 0.0035),
        range_tp_max=_as_float(os.getenv("RANGE_TP_MAX", "0.0065"), 0.0065),
        # TREND→RANGE 다운그레이드 ENV 훅
        trend_to_range_enable=_as_bool(os.getenv("TREND_TO_RANGE_ENABLE", "0"), False),
        trend_to_range_min_gain_pct=_as_float(os.getenv("TREND_TO_RANGE_MIN_GAIN_PCT", "0.0015"), 0.0015),
        trend_to_range_max_abs_pnl_pct=_as_float(os.getenv("TREND_TO_RANGE_MAX_ABS_PNL_PCT", "0.0015"), 0.0015),
        trend_to_range_auto_reenter=_as_bool(os.getenv("TREND_TO_RANGE_AUTO_REENTER", "1"), True),
    )


if __name__ == "__main__":
    s = load_settings()
    for k, v in s.as_dict().items():
        print(f"{k}: {v}")
