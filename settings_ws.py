"""
settings_ws.py
====================================================
웹소켓 기반 BingX 선물 자동매매에서 공통으로 사용하는 설정 모듈.

주요 역할
----------------------------------------------------
- WS/REST 엔드포인트, 기본 심볼·주기, 구독 타임프레임 관리
- 레버리지/리스크, TP·SL 기본값, ATR 파라미터
- GPT 진입/청산 게이트(쿨다운, 리스크·TP·SL 상/하한, soft TP 재판단)
- 각종 가드(스프레드, 가격 점프, depth 쏠림, mark/
ast 괴리, 캔들 지연 등)
- 텔레그램 알림, 로그/헬스 포트 설정

2025-11-19 패치 (GPT MARKET 단일 전략 전환)
----------------------------------------------------
1) 기존 TREND / RANGE 전략 관련 설정 전부 제거.
   - enable_trend, enable_range, range_* / trend_* / trend_to_range_* 등 삭제.
2) 기본 TP/SL을 1% / 2% 로 통일.
   - tp_pct = 0.01 (익절 1%), sl_pct = 0.02 (손절 2%).
3) GPT TP/SL 범위 및 soft TP 옵션 추가.
   - gpt_min_tp_pct, gpt_max_tp_pct, gpt_min_sl_pct, gpt_max_sl_pct
   - gpt_soft_tp_enabled, gpt_soft_tp_pct, gpt_soft_tp_recheck_sec
4) 연속 손실 2회 이상 발생 시 3시간 신규 진입 쿨타임 옵션 추가.
   - max_consecutive_losses, cooldown_after_consec_loss_sec
5) max_kline_delay_sec 기본값을 600초로 통일.

2025-11-20 패치 (저변동성 시장 회피 옵션 추가)
----------------------------------------------------
1) 저변동성(range 기반) 필터 기준 추가.
   - low_vol_range_pct_threshold (기본 0.01 = 1%)
2) 선택적으로 ATR 기반 변동성 기준 추가.
   - low_vol_atr_pct_threshold (기본 0.004 = 0.4%)
3) 두 값은 market_features_ws.get_trading_signal(...) 에서
   저변동성 장(1% 박스장 등)을 진입 대상에서 제외하는 데 사용된다.

2025-11-20 패치 (WS REST 백필/헬스 설정 분리, B안)
----------------------------------------------------
1) ws_backfill_tfs / ws_backfill_limit 추가.
   - run_bot_ws 부팅 단계에서 REST /kline 백필 대상 TF 및 개수를 제어.
   - 기본값: ["1m","5m","15m"], 120개 (요청하신 B안).
2) WS 데이터 헬스 체크용 파라미터 추가.
   - ws_required_tfs, ws_min_kline_buffer, ws_max_kline_delay_sec,
     ws_orderbook_max_delay_sec
   - market_data_ws.get_health_snapshot(...) / is_data_healthy(...)에서 사용.
3) _as_bool(...) 헬퍼 오타 수정 (Pylance 오류 해결).

2025-11-21 패치 (GPT 진입 호출 쿨다운 추가)
----------------------------------------------------
1) gpt_entry_cooldown_sec 설정 추가.
   - ENV: GPT_ENTRY_COOLDOWN_SEC (기본 120초)
   - gpt_trader.py 에서 GPT ENTRY 호출 간 최소 간격(쿨다운)으로 사용.

이 모듈은 반드시 load_settings()를 통해 읽어 사용하며,
런타임에서 BotSettings 값을 직접 변조하지 않는다.
"""

from __future__ import annotations

import os
import datetime
from dataclasses import dataclass
from typing import Dict, List

# 한국 시간대 (KST) — 다른 모듈에서 공통 사용 가능
KST = datetime.timezone(datetime.timedelta(hours=9))


def _as_bool(val: str | None, default: bool = False) -> bool:
    """ENV 문자열을 불리언으로 변환.
    허용: 1/true/yes/y (대소문자 무시). None이면 default.
    """
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "y"}


def _as_int(val: str | None, default: int) -> int:
    """ENV 문자열을 int로 안전 변환."""
    try:
        return int(val)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _as_float(val: str | None, default: float) -> float:
    """ENV 문자열을 float으로 안전 변환."""
    try:
        return float(val)  # type: ignore[arg-type]
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
    - dataclass는 불변(frozen)으로 유지: 런타임 변조 방지, 구성은 loader에서만.
    """

    # 필수 인증
    api_key: str
    api_secret: str

    # 기본 심볼/주기
    symbol: str = "BTC-USDT"
    interval: str = "5m"  # 웹소켓에서 기본으로 보는 주기

    # 웹소켓 관련
    ws_enabled: bool = True  # 웹소켓 사용 여부
    ws_base: str = "wss://open-api-ws.bingx.com"  # 일반 WS BASE (env로 변경 가능)
    ws_swap_base: str = "wss://open-api-swap.bingx.com/swap-market"  # 스왑 마켓 전용 WS
    ws_subscribe_tfs: List[str] | None = None  # 런타임에서 ["1m","5m","15m"]로 채운다
    ws_log_enabled: bool = True  # 캔들 수신 시 단순 로그 남길지

    # WS 원시/페이로드 로그 (Render 디버깅용, 기본 OFF 권장)
    ws_log_raw_enabled: bool = False      # WS 원시 프레임 로깅 (ENV: WS_LOG_RAW_ENABLED)
    ws_log_payload_enabled: bool = False  # kline/depth payload 상세 로깅 (ENV: WS_LOG_PAYLOAD_ENABLED)

    # ── WS 히스토리 백필(B안) 및 헬스 기준 ──────────────────
    # - ws_backfill_tfs: 부팅 시 REST로 히스토리를 채울 TF 목록 (예: ["1m","5m","15m"])
    # - ws_backfill_limit: 각 TF당 REST로 가져올 최대 캔들 수
    # - ws_required_tfs: 데이터 헬스 체크에 필수로 요구하는 TF 목록 (None이면 ws_subscribe_tfs 사용)
    # - ws_min_kline_buffer: 헬스 체크용 최소 버퍼 길이
    # - ws_max_kline_delay_sec: 헬스 체크용 kline 최대 지연(sec)
    # - ws_orderbook_max_delay_sec: 헬스 체크용 오더북 최대 지연(sec)
    ws_backfill_tfs: List[str] | None = None
    ws_backfill_limit: int = 120
    ws_required_tfs: List[str] | None = None
    ws_min_kline_buffer: int = 120
    ws_max_kline_delay_sec: int = 600
    ws_orderbook_max_delay_sec: int = 10

    # ── WS 히스토리 웜업/부트스트랩 ─────────────────────────
    # * min_bars_*: 이 값 미만이면 신호 자체를 스킵
    # * warmup_target_*: 이 값 미만이면 진행은 하되 "웜업 진행" 로그만 출력
    # * ws_bootstrap_with_rest: 부팅 직후 REST로 과거 캔들을 1회 시드하여 웜업 시간 단축
    # * ws_bootstrap_lookback_*: REST 시드 시 가져올 lookback 수량
    min_bars_5m: int = 20
    min_bars_15m: int = 20
    warmup_target_5m: int = 50
    warmup_target_15m: int = 50
    ws_bootstrap_with_rest: bool = True
    ws_bootstrap_lookback_5m: int = 120
    ws_bootstrap_lookback_15m: int = 120

    # ── 전략 on/off ───────────────────────────────────────
    # 단일 GPT MARKET 전략 on/off 플래그
    enable_market: bool = True
    # 1m 캔들 추가 확인 사용 여부 (데이터 품질 보조용)
    enable_1m_confirm: bool = True

    # 레버리지/리스크
    leverage: int = 1
    isolated: bool = True
    risk_pct: float = 0.1
    min_notional_usdt: float = 5.0
    max_notional_usdt: float = 999999.0

    # ── 기본 TP/SL (GPT가 없을 때/기본값) ───────────────────
    # 익절/손절 기본 비율 (가격 기준)
    tp_pct: float = 0.005  # 기본 익절 0.5%
    sl_pct: float = 0.01    # 기본 손절 1.0%
    # ATR/변동성 보정
    use_atr: bool = True
    atr_len: int = 20
    atr_tp_mult: float = 2.0
    atr_sl_mult: float = 1.2
    min_tp_pct: float = 0.005  # 0.5%
    min_sl_pct: float = 0.005  # 0.5%
    atr_risk_high_mult: float = 1.5
    atr_risk_reduction: float = 0.5

    # 저변동성(박스장) 필터 기준
    # - range_pct 가 low_vol_range_pct_threshold 미만이면 "저변동성" 후보로 간주
    # - atr_pct 가 low_vol_atr_pct_threshold 미만이면 ATR 기준으로도 저변동성으로 간주
    low_vol_range_pct_threshold: float = 0.01   # 기본 1% 박스장
    low_vol_atr_pct_threshold: float = 0.004    # 기본 ATR 0.4%

    # ── GPT 진입 게이트/TP·SL 제약 ────────────────────────
    gpt_error_sleep_sec: float = 5.0      # GPT 오류 시 루프 대기(sec)
    gpt_skip_sleep_sec: float = 1.0      # GPT SKIP/비정상 응답 후 대기(sec)
    gpt_max_risk_pct: float = 0.03        # GPT 제안 리스크 상한 (3%)
    gpt_entry_cooldown_sec: int = 15     # GPT ENTRY 호출 쿨다운(sec) — ENV: GPT_ENTRY_COOLDOWN_SEC

    # GPT가 제안하는 TP/SL 범위 (가격 기준)
    gpt_min_tp_pct: float = 0.01          # TP 하한 (기본 1%)
    gpt_max_tp_pct: float = 0.10          # TP 상한 (기본 10%)
    gpt_min_sl_pct: float = 0.0           # SL 하한 (제한 없음, 필요 시 ENV로 조절)
    gpt_max_sl_pct: float = 0.02          # SL 상한 (기본 2%)

    post_entry_sleep_sec: float = 5.0     # 진입 성공 후 짧은 쿨다운(sec)

    # GPT soft TP 재판단 옵션
    # - 수익률이 gpt_soft_tp_pct 부근에 도달했을 때
    #   "그냥 1% 익절" vs "TP를 더 멀리 늘려서 계속 보유" 를 GPT에게 다시 묻게 될 때 사용.
    gpt_soft_tp_enabled: bool = True
    gpt_soft_tp_pct: float = 0.01         # 기본 1% 부근에서 재판단
    gpt_soft_tp_recheck_sec: int = 60     # soft TP 존에 있을 때 재판단 주기(sec)

    # 진입 거래량 가드 (ENV 훅)
    min_entry_volume_ratio: float = 0.3   # 기본 0.30

    # ── 쿨다운/폴링 ────────────────────────────────────────
    cooldown_sec: int = 1
    cooldown_after_close: int = 1
    # 연속 손실 기준 쿨타임 (예: 2번 연속 손절 나면 3시간 잠시 쉼)
    max_consecutive_losses: int = 2
    cooldown_after_consec_loss_sec: int = 10800  # 3시간

    poll_fills_sec: int = 2

    # ── 슬리피지/호가 관련 ─────────────────────────────────
    max_price_jump_pct: float = 0.003
    max_spread_pct: float =  0.0012
    max_entry_slippage_pct: float = 0.0005
    use_orderbook_entry_hint: bool = True

    # ── 선물 틱 기반 슬리피지 가드 ────────────────────────
    price_tick_size: float = 0.1
    max_entry_slippage_ticks: int = 5
    auto_close_on_heavy_slippage: bool = True

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

    # 텔레그램 스팸 억제
    skip_tg_cooldown: int = 5
    balance_skip_cooldown: int = 3600

    # 캔들 지연 허용치 (신호 레벨)
    max_kline_delay_sec: int = 600

    # RSI 기준
    rsi_overbought: int = 70
    rsi_oversold: int = 30

    # 종료 관련
    min_uptime_for_stop: int = 5

    # BingX base url (env 우선)
    bingx_base: str = "https://open-api.bingx.com"

    # 마진 기준 TP/SL (원하면 사용)
    use_margin_based_tp_sl: bool = False
    fut_tp_margin_pct: float = 0.5
    fut_sl_margin_pct: float = 0.5

    # 반대 시그널 감지 즉시 청산 on/off
    close_on_opposite_enabled: bool = True

    def as_dict(self) -> Dict[str, object]:
        """dict 변환(로깅/디버깅용)."""
        return self.__dict__.copy()


def load_settings() -> BotSettings:
    """ENV를 읽어 BotSettings 인스턴스를 생성한다.
    - 문자열 파싱은 _as_* 헬퍼를 통해 일관 처리
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
    bingx_ws_swap_base_env = os.getenv(
        "BINGX_SWAP_WS_BASE", "wss://open-api-swap.bingx.com/swap-market"
    )

    # WS 구독 타임프레임 (빈값이면 기본값 사용)
    ws_tfs_env = os.getenv("WS_SUBSCRIBE_TFS")
    if ws_tfs_env:
        ws_tfs = [x.strip() for x in ws_tfs_env.split(",") if x.strip()]
    else:
        ws_tfs = ["1m", "5m", "15m"]

    # REST 백필 타임프레임(B안): 기본 1m/5m/15m
    ws_backfill_tfs_env = os.getenv("WS_BACKFILL_TFS")
    if ws_backfill_tfs_env:
        ws_backfill_tfs = [x.strip() for x in ws_backfill_tfs_env.split(",") if x.strip()]
    else:
        ws_backfill_tfs = ["1m", "5m", "15m"]

    ws_backfill_limit = _as_int(os.getenv("WS_BACKFILL_LIMIT", "120"), 120)

    # WS 데이터 헬스 체크 기준
    ws_required_tfs_env = os.getenv("WS_REQUIRED_TFS")
    if ws_required_tfs_env:
        ws_required_tfs = [x.strip() for x in ws_required_tfs_env.split(",") if x.strip()]
    else:
        ws_required_tfs = None  # None이면 market_data_ws 에서 ws_subscribe_tfs 사용

    ws_min_kline_buffer = _as_int(os.getenv("WS_MIN_KLINE_BUFFER", "120"), 120)
    max_kline_delay_sec_val = _as_int(os.getenv("MAX_KLINE_DELAY_SEC", "600"), 600)
    ws_max_kline_delay_sec = _as_int(
        os.getenv("WS_MAX_KLINE_DELAY_SEC", str(max_kline_delay_sec_val)),
        max_kline_delay_sec_val,
    )
    ws_orderbook_max_delay_sec = _as_int(
        os.getenv("WS_ORDERBOOK_MAX_DELAY_SEC", "10"),
        10,
    )

    # 1m 보조 확인 사용 여부
    enable_1m_confirm_value = _as_bool(os.getenv("ENABLE_1M_CONFIRM", "1"), True)

    return BotSettings(
        api_key=api_key,
        api_secret=api_secret,
        symbol=os.getenv("SYMBOL", "BTC-USDT"),
        interval=os.getenv("INTERVAL", "5m"),
        # WS
        ws_enabled=_as_bool(os.getenv("WS_ENABLED", "1"), True),
        ws_base=bingx_ws_base_env,
        ws_swap_base=bingx_ws_swap_base_env,
        ws_subscribe_tfs=ws_tfs,
        ws_log_enabled=_as_bool(os.getenv("WS_LOG_ENABLED", "1"), True),
        # WS 원시/페이로드 로그 ENV 매핑 (디버깅 시에만 ON 권장)
        ws_log_raw_enabled=_as_bool(os.getenv("WS_LOG_RAW_ENABLED", "0"), False),
        ws_log_payload_enabled=_as_bool(os.getenv("WS_LOG_PAYLOAD_ENABLED", "0"), False),
        # WS 백필/헬스 설정
        ws_backfill_tfs=ws_backfill_tfs,
        ws_backfill_limit=ws_backfill_limit,
        ws_required_tfs=ws_required_tfs,
        ws_min_kline_buffer=ws_min_kline_buffer,
        ws_max_kline_delay_sec=ws_max_kline_delay_sec,
        ws_orderbook_max_delay_sec=ws_orderbook_max_delay_sec,
        # ── WS 웜업/부트스트랩 ENV 매핑 ──
        min_bars_5m=_as_int(os.getenv("MIN_BARS_5M", "20"), 20),
        min_bars_15m=_as_int(os.getenv("MIN_BARS_15M", "20"), 20),
        warmup_target_5m=_as_int(os.getenv("WARMUP_TARGET_5M", "50"), 50),
        warmup_target_15m=_as_int(os.getenv("WARMUP_TARGET_15M", "50"), 50),
        ws_bootstrap_with_rest=_as_bool(os.getenv("WS_BOOTSTRAP_WITH_REST", "1"), True),
        ws_bootstrap_lookback_5m=_as_int(
            os.getenv("WS_BOOTSTRAP_LOOKBACK_5M", "120"),
            120,
        ),
        ws_bootstrap_lookback_15m=_as_int(
            os.getenv("WS_BOOTSTRAP_LOOKBACK_15M", "120"),
            120,
        ),
        # 전략 on/off
        enable_market=_as_bool(os.getenv("ENABLE_MARKET", "1"), True),
        enable_1m_confirm=enable_1m_confirm_value,
        # 레버리지/리스크
        leverage=_as_int(os.getenv("LEVERAGE", "10"), 10),
        isolated=_as_bool(os.getenv("ISOLATED", "1"), True),
        risk_pct=_as_float(os.getenv("RISK_PCT", "0.3"), 0.3),
        min_notional_usdt=_as_float(os.getenv("MIN_NOTIONAL_USDT", "5"), 5.0),
        max_notional_usdt=_as_float(os.getenv("MAX_NOTIONAL_USDT", "999999"), 999999.0),
        # TP/SL 기본값 (1% / 2%)
        tp_pct=_as_float(os.getenv("TP_PCT", "0.01"), 0.01),
        sl_pct=_as_float(os.getenv("SL_PCT", "0.02"), 0.02),
        # ATR 설정
        use_atr=_as_bool(os.getenv("USE_ATR", "1"), True),
        atr_len=_as_int(os.getenv("ATR_LEN", "20"), 20),
        atr_tp_mult=_as_float(os.getenv("ATR_TP_MULT", "2.0"), 2.0),
        atr_sl_mult=_as_float(os.getenv("ATR_SL_MULT", "1.2"), 1.2),
        min_tp_pct=_as_float(os.getenv("MIN_TP_PCT", "0.005"), 0.005),
        min_sl_pct=_as_float(os.getenv("MIN_SL_PCT", "0.005"), 0.005),
        atr_risk_high_mult=_as_float(os.getenv("ATR_RISK_HIGH_MULT", "1.5"), 1.5),
        atr_risk_reduction=_as_float(os.getenv("ATR_RISK_REDUCTION", "0.5"), 0.5),
        # 저변동성(박스장) 필터 기준
        low_vol_range_pct_threshold=_as_float(
            os.getenv("LOW_VOL_RANGE_PCT_THRESHOLD", "0.01"),
            0.01,
        ),
        low_vol_atr_pct_threshold=_as_float(
            os.getenv("LOW_VOL_ATR_PCT_THRESHOLD", "0.004"),
            0.004,
        ),
        # GPT 진입 게이트/상한
        gpt_error_sleep_sec=_as_float(os.getenv("GPT_ERROR_SLEEP_SEC", "5.0"), 5.0),
        gpt_skip_sleep_sec=_as_float(os.getenv("GPT_SKIP_SLEEP_SEC", "3.0"), 3.0),
        gpt_max_risk_pct=_as_float(os.getenv("GPT_MAX_RISK_PCT", "0.03"), 0.03),
        gpt_entry_cooldown_sec=_as_int(
            os.getenv("GPT_ENTRY_COOLDOWN_SEC", "1"),
            1,
        ),
        # GPT TP/SL 범위
        gpt_min_tp_pct=_as_float(os.getenv("GPT_MIN_TP_PCT", "0.01"), 0.01),
        gpt_max_tp_pct=_as_float(os.getenv("GPT_MAX_TP_PCT", "0.10"), 0.10),
        gpt_min_sl_pct=_as_float(os.getenv("GPT_MIN_SL_PCT", "0.0"), 0.0),
        gpt_max_sl_pct=_as_float(os.getenv("GPT_MAX_SL_PCT", "0.02"), 0.02),
        post_entry_sleep_sec=_as_float(
            os.getenv("POST_ENTRY_SLEEP_SEC", "5.0"),
            5.0,
        ),
        # GPT soft TP 옵션
        gpt_soft_tp_enabled=_as_bool(os.getenv("GPT_SOFT_TP_ENABLED", "1"), True),
        gpt_soft_tp_pct=_as_float(os.getenv("GPT_SOFT_TP_PCT", "0.01"), 0.01),
        gpt_soft_tp_recheck_sec=_as_int(
            os.getenv("GPT_SOFT_TP_RECHECK_SEC", "60"),
            60,
        ),
        # 진입 거래량 가드
        min_entry_volume_ratio=_as_float(
            os.getenv("MIN_ENTRY_VOLUME_RATIO", "0.15"),
            0.3,
        ),
        # 쿨다운/폴링
        cooldown_sec=_as_int(os.getenv("COOLDOWN_SEC", "15"), 1),
        cooldown_after_close=_as_int(os.getenv("COOLDOWN_AFTER_CLOSE", "1"), 1),
        max_consecutive_losses=_as_int(
            os.getenv("MAX_CONSECUTIVE_LOSSES", "2"),
            2,
        ),
        cooldown_after_consec_loss_sec=_as_int(
            os.getenv("COOLDOWN_AFTER_CONSEC_LOSS_SEC", "10800"),
            10800,
        ),
        poll_fills_sec=_as_int(os.getenv("POLL_FILLS_SEC", "2"), 2),
        # 가드
        max_price_jump_pct=_as_float(os.getenv("MAX_PRICE_JUMP_PCT", "0.006"), 0.003),
        max_spread_pct=_as_float(os.getenv("MAX_SPREAD_PCT", "0.0008"), 0.0008),
        max_entry_slippage_pct=_as_float(
            os.getenv("MAX_ENTRY_SLIPPAGE_PCT", "0.0005"),
            0.0005,
        ),
        use_orderbook_entry_hint=_as_bool(
            os.getenv("USE_ORDERBOOK_ENTRY_HINT", "1"),
            True,
        ),
        # depth env
        depth_imbalance_enabled=_as_bool(
            os.getenv("DEPTH_IMBALANCE_ENABLED", "1"),
            True,
        ),
        depth_imbalance_min_notional=_as_float(
            os.getenv("DEPTH_IMBALANCE_MIN_NOTIONAL", "50"),
            50.0,
        ),
        depth_imbalance_min_ratio=_as_float(
            os.getenv("DEPTH_IMBALANCE_MIN_RATIO", "2.0"),
            2.0,
        ),
        # mark vs last env
        price_deviation_guard_enabled=_as_bool(
            os.getenv("PRICE_DEVIATION_GUARD_ENABLED", "1"),
            True,
        ),
        price_deviation_max_pct=_as_float(
            os.getenv("PRICE_DEVIATION_MAX_PCT", "0.0015"),
            0.0015,
        ),
        # 세션 배수 env
        session_spread_mult_asia=_as_float(
            os.getenv("SESSION_SPREAD_MULT_ASIA", "1.0"),
            1.0,
        ),
        session_spread_mult_eu=_as_float(
            os.getenv("SESSION_SPREAD_MULT_EU", "1.1"),
            1.1,
        ),
        session_spread_mult_us=_as_float(
            os.getenv("SESSION_SPREAD_MULT_US", "1.2"),
            1.2,
        ),
        session_jump_mult_asia=_as_float(
            os.getenv("SESSION_JUMP_MULT_ASIA", "1.0"),
            1.0,
        ),
        session_jump_mult_eu=_as_float(
            os.getenv("SESSION_JUMP_MULT_EU", "1.1"),
            1.1,
        ),
        session_jump_mult_us=_as_float(
            os.getenv("SESSION_JUMP_MULT_US", "1.2"),
            1.2,
        ),
        # 텔레그램
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
        notify_on_entry=_as_bool(os.getenv("NOTIFY_ON_ENTRY", "1"), True),
        notify_on_close=_as_bool(os.getenv("NOTIFY_ON_CLOSE", "1"), True),
        unrealized_notify_enabled=_as_bool(
            os.getenv("UNREALIZED_NOTIFY_ENABLED", "0"),
            False,
        ),
        unrealized_notify_sec=_as_int(
            os.getenv("UNREALIZED_NOTIFY_SEC", "1800"),
            1800,
        ),
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
        # 텔레그램 스팸
        skip_tg_cooldown=_as_int(os.getenv("SKIP_TG_COOLDOWN", "30"), 30),
        balance_skip_cooldown=_as_int(
            os.getenv("BALANCE_SKIP_COOLDOWN", "3600"),
            3600,
        ),
        # 캔들 지연 허용 (신호 레벨)
        max_kline_delay_sec=max_kline_delay_sec_val,
        # RSI
        rsi_overbought=_as_int(os.getenv("RSI_OVERBOUGHT", "70"), 70),
        rsi_oversold=_as_int(os.getenv("RSI_OVERSOLD", "30"), 30),
        # 종료
        min_uptime_for_stop=_as_int(os.getenv("MIN_UPTIME_FOR_STOP", "5"), 5),
        # BingX BASE
        bingx_base=bingx_base_env,
        # 마진 기준 TP/SL
        use_margin_based_tp_sl=_as_bool(
            os.getenv("USE_MARGIN_BASED_TP_SL", "0"),
            False,
        ),
        fut_tp_margin_pct=_as_float(os.getenv("FUT_TP_MARGIN_PCT", "0.5"), 0.5),
        fut_sl_margin_pct=_as_float(os.getenv("FUT_SL_MARGIN_PCT", "0.5"), 0.5),
        # 반대 시그널 컷 on/off
        close_on_opposite_enabled=_as_bool(
            os.getenv("CLOSE_ON_OPPOSITE_ENABLED", "1"),
            True,
        ),
    )


if __name__ == "__main__":
    s = load_settings()
    for k, v in s.as_dict().items():
        print(f"{k}: {v}")
