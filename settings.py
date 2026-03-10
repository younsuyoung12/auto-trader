# settings.py
"""
========================================================
FILE: settings.py
ROLE:
- AUTO-TRADER 전체 런타임 설정 SSOT
- env / optional .env / defaults를 단일 계약으로 로드하고 검증한다
- 운영/실시간/분석/대시보드/메타전략 설정의 불일치를 부팅 시점에 차단한다

CORE RESPONSIBILITIES:
- Settings frozen dataclass 제공
- 환경변수 파싱 / 타입 변환 / 충돌 검증
- 설정 계약 불일치 Fail-Fast
- 대문자 alias 접근 호환성 제공
- 민감정보를 로그에 남기지 않는 안전한 로딩 유지

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- settings.py 는 단일 설정 소스(SSOT)다
- 코드 내부에서 환경변수 직접 접근 금지
- 설정 누락/충돌/계약 불일치는 즉시 예외 처리
- 설정명은 실제 의미와 일치해야 하며, 실시간 health 기준과 payload 기준을 혼동하지 않는다
- Settings 는 런타임 불변(frozen) 객체다
- 민감정보(API key/secret, DB URL, token)는 절대 로그에 남기지 않는다

CHANGE HISTORY:
- 2026-03-11:
  1) FIX(SSOT): run_bot_preflight 가 사용하던 preflight_fake_usdt / meta_* 설정 필드를 정식 승격
  2) FIX(STRICT): meta strategy / preflight execution probe 설정 검증 추가
  3) FIX(COMPAT): META_* / PREFLIGHT_FAKE_USDT uppercase alias 접근 호환성 추가
- 2026-03-10:
  1) FIX(SSOT): unified_features_builder / order_executor / engine_watchdog 가 요구하는 설정 필드를 정식 승격
  2) FIX(SSOT): features_required_tfs / microstructure_* / low_vol_* / exit_fill_wait_sec / position_reflect_wait_sec 추가
  3) FIX(SSOT): engine_watchdog_interval_sec / engine_watchdog_max_db_ping_ms / engine_watchdog_emit_min_sec 추가
  4) FIX(ROOT-CAUSE): ALPHAVANTAGE_API_KEY 를 전역 부팅 필수에서 제거 (분석 계층 전용 optional)
  5) FIX(COMPAT): 신규 설정들의 uppercase alias 접근 호환성 추가
========================================================
"""

from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass, field
from datetime import timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Korea Standard Time (UTC+9)
KST = timezone(timedelta(hours=9))


# ---------------------------------------------------------------------
# Alias map for strict uppercase access compatibility
# ---------------------------------------------------------------------
_SETTINGS_ALIAS_MAP: Dict[str, str] = {
    # 기존 OpenAI / Trader OpenAI
    "OPENAI_API_KEY": "openai_api_key",
    "OPENAI_MODEL": "openai_model",
    "OPENAI_TRADER_MODEL": "openai_model",
    "OPENAI_SUPERVISOR_MODEL": "openai_model",
    "OPENAI_MAX_TOKENS": "openai_max_tokens",
    "OPENAI_TRADER_MAX_TOKENS": "openai_max_tokens",
    "OPENAI_TEMPERATURE": "openai_temperature",
    "OPENAI_TRADER_TEMPERATURE": "openai_temperature",
    "OPENAI_SUPERVISOR_TEMPERATURE": "openai_temperature",
    "OPENAI_MAX_LATENCY_SEC": "openai_max_latency_sec",
    "OPENAI_TRADER_MAX_LATENCY_SEC": "openai_max_latency_sec",
    "OPENAI_TRADER_MAX_LATENCY": "openai_max_latency_sec",
    # External intelligence
    "ALPHAVANTAGE_API_KEY": "alphavantage_api_key",
    # Exchange / market-data
    "BINANCE_FUTURES_BASE": "binance_futures_base",
    "BINANCE_FUTURES_BASE_URL": "binance_futures_base_url",
    "BINANCE_FUTURES_REST_BASE": "binance_futures_rest_base",
    "WS_MARKET_EVENT_MAX_DELAY_SEC": "ws_market_event_max_delay_sec",
    "WS_ORDERBOOK_MAX_DELAY_SEC": "ws_market_event_max_delay_sec",  # legacy env compatibility
    "WS_BOOTSTRAP_REST_ENABLED": "ws_bootstrap_rest_enabled",
    "WS_REST_TIMEOUT_SEC": "ws_rest_timeout_sec",
    "WS_MIN_KLINE_BUFFER_LONG_TF": "ws_min_kline_buffer_long_tf",
    "WS_MIN_KLINE_BUFFER_BY_INTERVAL": "ws_min_kline_buffer_by_interval",
    "WS_HEALTH_FAIL_LOG_SUPPRESS_SEC": "ws_health_fail_log_suppress_sec",
    "WS_NO_BUFFER_LOG_SUPPRESS_SEC": "ws_no_buffer_log_suppress_sec",
    "WS_PONG_MAX_DELAY_SEC": "ws_pong_max_delay_sec",
    "WS_PONG_STARTUP_GRACE_SEC": "ws_pong_startup_grace_sec",
    # Unified features / market features
    "FEATURES_REQUIRED_TFS": "features_required_tfs",
    "FEATURES_ERROR_TG_COOLDOWN_SEC": "features_error_tg_cooldown_sec",
    "LOW_VOL_RANGE_PCT_THRESHOLD": "low_vol_range_pct_threshold",
    "LOW_VOL_ATR_PCT_THRESHOLD": "low_vol_atr_pct_threshold",
    "MICROSTRUCTURE_PERIOD": "microstructure_period",
    "MICROSTRUCTURE_LOOKBACK": "microstructure_lookback",
    "MICROSTRUCTURE_CACHE_TTL_SEC": "microstructure_cache_ttl_sec",
    # Execution / sync
    "POSITION_REFLECT_WAIT_SEC": "position_reflect_wait_sec",
    "EXIT_FILL_WAIT_SEC": "exit_fill_wait_sec",
    # Watchdog
    "ENGINE_WATCHDOG_INTERVAL_SEC": "engine_watchdog_interval_sec",
    "ENGINE_WATCHDOG_MAX_DB_PING_MS": "engine_watchdog_max_db_ping_ms",
    "ENGINE_WATCHDOG_EMIT_MIN_SEC": "engine_watchdog_emit_min_sec",
    # Preflight / Meta
    "PREFLIGHT_FAKE_USDT": "preflight_fake_usdt",
    "META_LOOKBACK_TRADES": "meta_lookback_trades",
    "META_RECENT_WINDOW": "meta_recent_window",
    "META_BASELINE_WINDOW": "meta_baseline_window",
    "META_MIN_TRADES_REQUIRED": "meta_min_trades_required",
    "META_MAX_RECENT_TRADES": "meta_max_recent_trades",
    "META_LANGUAGE": "meta_language",
    "META_MAX_OUTPUT_SENTENCES": "meta_max_output_sentences",
    "META_PROMPT_VERSION": "meta_prompt_version",
    "META_COOLDOWN_SEC": "meta_cooldown_sec",
    # AI analyst
    "ANALYST_MARKET_SYMBOL": "analyst_market_symbol",
    "ANALYST_DB_MARKET_TIMEFRAME": "analyst_db_market_timeframe",
    "ANALYST_DB_MARKET_LOOKBACK_LIMIT": "analyst_db_market_lookback_limit",
    "ANALYST_PATTERN_ENTRY_THRESHOLD": "analyst_pattern_entry_threshold",
    "ANALYST_SPREAD_GUARD_BPS": "analyst_spread_guard_bps",
    "ANALYST_IMBALANCE_GUARD_ABS": "analyst_imbalance_guard_abs",
    "ANALYST_TRADE_LOOKBACK_LIMIT": "analyst_trade_lookback_limit",
    "ANALYST_EVENT_LOOKBACK_LIMIT": "analyst_event_lookback_limit",
    "ANALYST_INCLUDE_EXTERNAL_MARKET": "analyst_include_external_market",
    "ANALYST_AUTO_REPORT_MARKET_INTERVAL_SEC": "analyst_auto_report_market_interval_sec",
    "ANALYST_AUTO_REPORT_SYSTEM_INTERVAL_SEC": "analyst_auto_report_system_interval_sec",
    "ANALYST_AUTO_REPORT_PERSIST": "analyst_auto_report_persist",
    "ANALYST_AUTO_REPORT_NOTIFY": "analyst_auto_report_notify",
    "ANALYST_OPENAI_MODEL": "analyst_openai_model",
    "ANALYST_OPENAI_TIMEOUT_SEC": "analyst_openai_timeout_sec",
    "ANALYST_OPENAI_MAX_OUTPUT_TOKENS": "analyst_openai_max_output_tokens",
    "ANALYST_OPENAI_TEMPERATURE": "analyst_openai_temperature",
    "ANALYST_OPENAI_REASONING_EFFORT": "analyst_openai_reasoning_effort",
    "ANALYST_KLINE_INTERVAL": "analyst_kline_interval",
    "ANALYST_KLINE_LIMIT": "analyst_kline_limit",
    "ANALYST_HTTP_TIMEOUT_SEC": "analyst_http_timeout_sec",
    "ANALYST_RATIO_PERIOD": "analyst_ratio_period",
    "ANALYST_RATIO_LIMIT": "analyst_ratio_limit",
    "ANALYST_DEPTH_LIMIT": "analyst_depth_limit",
    "ANALYST_FUNDING_LIMIT": "analyst_funding_limit",
    "ANALYST_FORCE_ORDER_LIMIT": "analyst_force_order_limit",
    "ANALYST_MAX_SERVER_DRIFT_MS": "analyst_max_server_drift_ms",
}


# ---------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class Settings:
    """Runtime settings for the trading engine (Binance USDT-M Futures)."""

    # Binance credentials
    api_key: str = ""
    api_secret: str = ""

    # Core trading
    symbol: str = "BTCUSDT"
    interval: str = "5m"

    # Futures config
    leverage: int = 1
    isolated: bool = True
    margin_mode: str = "ISOLATED"
    futures_position_mode: str = "ONEWAY"

    # HTTP / signing
    recv_window_ms: int = 5000
    request_timeout_sec: int = 10

    # Symbol filters
    qty_step: float = 0.001
    min_qty: float = 0.001
    price_tick: float = 0.1

    # Strategy / sizing
    allocation_ratio: float = 0.35
    risk_pct: float = 0.35

    # Default TP/SL
    tp_pct: float = 0.006
    sl_pct: float = 0.003

    # TP/SL bounds
    tp_pct_min: float = 0.0005
    tp_pct_max: float = 0.05
    sl_pct_min: float = 0.0005
    sl_pct_max: float = 0.05

    # Legacy
    max_sl_pct: float = 0.015
    max_trade_qty: float = 1.0

    # Entry/exit cadence
    entry_cooldown_sec: float = 8.0
    cooldown_after_close: float = 3.0
    min_entry_score_for_gpt: float = 28.0
    gpt_entry_cooldown_sec: float = 10.0

    # GPT safety
    gpt_daily_call_limit: int = 2000
    gpt_max_risk_pct: float = 0.02
    gpt_min_confidence: float = 0.6
    gpt_reject_if_over_pnl_pct: float = 0.02

    # OpenAI (Trader engine)
    openai_api_key: str = ""
    openai_model: str = ""
    openai_max_tokens: int = 1500
    openai_temperature: float = 0.2
    openai_max_latency_sec: float = 12.0

    # External intelligence
    alphavantage_api_key: str = ""

    # Entry flow
    entry_score_threshold: float = 0.55
    entry_tp_pct: float = 0.01
    entry_sl_pct: float = 0.005
    entry_risk_pct: float = 0.02
    entry_max_spread_pct: float = 0.002
    entry_min_trend_strength: float = 0.2
    entry_min_abs_orderbook_imbalance: float = 0.05

    # Notifications
    unrealized_notify_sec: int = 1800

    # EXIT engine
    enable_exit_gpt: bool = True

    # Polling / order limits
    poll_fills_sec: float = 3.0
    max_open_orders: int = 5

    # Daily entry cap
    max_entry_per_day: int = 999
    max_entry_per_day_reset_hour_kst: int = 9

    # Logging
    log_to_file: bool = False
    log_file: str = "logs/bot.log"

    # Telegram
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    # WebSocket buffering / transport freshness
    ws_enabled: bool = True
    ws_base: str = "wss://fstream.binance.com/ws"
    ws_combined_base: str = "wss://fstream.binance.com/stream?streams="
    ws_subscribe_tfs: List[str] = field(default_factory=lambda: ["1m", "5m", "15m", "1h", "4h"])
    ws_required_tfs: List[str] = field(default_factory=lambda: ["1m", "5m", "15m", "1h", "4h"])
    preflight_ws_wait_sec: int = 60
    ws_min_kline_buffer: int = 300
    ws_max_kline_delay_sec: float = 120.0
    ws_market_event_max_delay_sec: float = 10.0
    ws_bootstrap_rest_enabled: bool = True
    ws_rest_timeout_sec: float = 10.0
    ws_min_kline_buffer_long_tf: int = 2
    ws_min_kline_buffer_by_interval: Dict[str, int] = field(default_factory=dict)
    ws_health_fail_log_suppress_sec: float = 10.0
    ws_no_buffer_log_suppress_sec: float = 30.0
    ws_pong_max_delay_sec: float = 15.0
    ws_pong_startup_grace_sec: float = 15.0
    ws_log_enabled: bool = False
    ws_log_interval_sec: int = 60
    ws_stale_reset_sec: float = 600.0
    ws_klines_stale_sec: float = 180.0

    # WS bootstrap/backfill
    ws_backfill_tfs: List[str] = field(default_factory=lambda: ["1m", "5m", "15m", "1h", "4h"])
    ws_backfill_limit: int = 500

    # Market-data store worker settings
    md_store_flush_sec: float = 2.0
    ob_store_interval_sec: float = 1.0
    md_store_tfs: List[str] = field(default_factory=lambda: ["1m", "5m", "15m", "1h", "4h"])

    # Guards
    max_spread_pct: float = 0.0015
    max_spread_abs: float = 0.0
    min_entry_volume_ratio: float = 0.15
    max_price_jump_pct: float = 0.003
    max_5m_delay_ms: int = 10 * 60 * 1000
    max_orderbook_age_ms: int = 3000
    min_bbo_notional_usdt: float = 0.0

    depth_imbalance_enabled: bool = True
    depth_imbalance_min_notional: float = 10.0
    depth_imbalance_min_ratio: float = 2.0

    price_deviation_guard_enabled: bool = True
    price_deviation_max_pct: float = 0.0015

    session_spread_mult_asia: float = 1.0
    session_spread_mult_eu: float = 1.1
    session_spread_mult_us: float = 1.2
    session_jump_mult_asia: float = 1.0
    session_jump_mult_eu: float = 1.1
    session_jump_mult_us: float = 1.2

    # Monitoring
    data_health_notify_sec: int = 900
    signal_analysis_interval_sec: int = 60
    krw_per_usdt: float = 1400.0

    # Operations / runtime safety
    sigterm_grace_sec: int = 30
    allow_start_without_leverage_setup: bool = False

    # 운영 파라미터
    async_worker_threads: int = 1
    async_worker_queue_size: int = 2000
    reconcile_interval_sec: int = 30
    reconcile_confirm_n: int = 3
    force_close_on_desync: bool = False
    max_signal_latency_ms: int = 200
    max_exec_latency_ms: int = 2500

    # run_bot_ws
    position_resync_sec: float = 20.0

    # Drift detector
    drift_allocation_abs_jump: float = 0.45
    drift_allocation_spike_ratio: float = 3.0
    drift_multiplier_abs_jump: float = 0.50
    drift_micro_abs_jump: float = 40.0
    drift_stable_regime_steps: int = 100

    # Exchange endpoints
    binance_futures_base: str = "https://fapi.binance.com"
    binance_futures_base_url: str = "https://fapi.binance.com"
    binance_futures_rest_base: str = "https://fapi.binance.com"

    # Legacy aliases
    binance_recv_window: int = 5000
    binance_http_timeout_sec: int = 10

    # Hard risk guards
    hard_daily_loss_limit_usdt: float = 0.0
    hard_consecutive_losses_limit: int = 0
    hard_position_value_pct_cap: float = 100.0
    hard_liquidation_distance_pct_min: float = 0.0

    # Slippage / protection
    slippage_block_pct: float = 0.002
    slippage_stop_engine: bool = False
    protection_mode_enabled: bool = True

    # 실행/멱등성/체결 확정
    require_deterministic_client_order_id: bool = True
    entry_fill_wait_sec: float = 2.0
    position_reflect_wait_sec: float = 1.2
    exit_fill_wait_sec: float = 2.0
    max_entry_slippage_pct: Optional[float] = None

    # Unified features / market features
    features_required_tfs: List[str] = field(default_factory=lambda: ["1m", "5m", "15m", "1h", "4h"])
    features_error_tg_cooldown_sec: float = 60.0
    low_vol_range_pct_threshold: float = 0.01
    low_vol_atr_pct_threshold: float = 0.004

    # Microstructure
    microstructure_period: str = "5m"
    microstructure_lookback: int = 30
    microstructure_cache_ttl_sec: int = 15

    # Engine watchdog
    engine_watchdog_interval_sec: float = 5.0
    engine_watchdog_max_db_ping_ms: int = 1500
    engine_watchdog_emit_min_sec: int = 30

    # Preflight / Meta strategy
    preflight_fake_usdt: float = 1000.0
    meta_lookback_trades: int = 200
    meta_recent_window: int = 20
    meta_baseline_window: int = 60
    meta_min_trades_required: int = 20
    meta_max_recent_trades: int = 20
    meta_language: str = "ko"
    meta_max_output_sentences: int = 3
    meta_prompt_version: str = "2026-03-03"
    meta_cooldown_sec: int = 60

    # TEST controls
    test_dry_run: bool = False
    test_bypass_guards: bool = False
    test_force_enter: bool = False
    test_fake_available_usdt: float = 0.0

    # Redis cache (Dashboard)
    redis_url: str = "redis://localhost:6379/0"
    dashboard_cache_prefix: str = "auto_trader_dashboard"
    dashboard_cache_ttl_sec: int = 5
    redis_socket_timeout_sec: float = 1.0
    redis_socket_connect_timeout_sec: float = 1.0
    redis_health_check_interval_sec: int = 30

    # AI Trading Intelligence
    analyst_market_symbol: str = "BTCUSDT"
    analyst_db_market_timeframe: str = "5m"
    analyst_db_market_lookback_limit: int = 60
    analyst_pattern_entry_threshold: float = 0.55
    analyst_spread_guard_bps: float = 15.0
    analyst_imbalance_guard_abs: float = 0.60
    analyst_trade_lookback_limit: int = 30
    analyst_event_lookback_limit: int = 300
    analyst_include_external_market: bool = True

    analyst_auto_report_market_interval_sec: int = 300
    analyst_auto_report_system_interval_sec: int = 1800
    analyst_auto_report_persist: bool = True
    analyst_auto_report_notify: bool = False

    analyst_openai_model: str = "gpt-5-mini"
    analyst_openai_timeout_sec: float = 120.0
    analyst_openai_max_output_tokens: int = 1200
    # Responses API 경로에서는 temperature 미사용. 호환성/레거시 설정 유지용 필드.
    analyst_openai_temperature: float = 0.1
    analyst_openai_reasoning_effort: Optional[str] = None

    analyst_kline_interval: str = "5m"
    analyst_kline_limit: int = 300
    analyst_http_timeout_sec: float = 10.0
    analyst_ratio_period: str = "5m"
    analyst_ratio_limit: int = 30
    analyst_depth_limit: int = 20
    analyst_funding_limit: int = 10
    analyst_force_order_limit: int = 50
    analyst_max_server_drift_ms: int = 5000

    def __getattr__(self, name: str) -> object:
        alias = _SETTINGS_ALIAS_MAP.get(name)
        if alias is not None:
            return object.__getattribute__(self, alias)
        raise AttributeError(name)


# Backward-compatible alias for legacy type hints
BotSettings = Settings


@dataclass(frozen=True, slots=True)
class PatternStrengthSettings:
    """Optional pattern-strength overrides used by pattern_detection."""
    pattern_strengths: Dict[str, float] = field(default_factory=dict)


# ---------------------------------------------------------------------
# Optional .env loader (no external deps)
# ---------------------------------------------------------------------
def _strip_inline_comment(s: str) -> str:
    out: list[str] = []
    in_single = False
    in_double = False

    for ch in s:
        if ch == "'" and not in_double:
            in_single = not in_single
            out.append(ch)
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            out.append(ch)
            continue
        if ch == "#" and not in_single and not in_double:
            break
        out.append(ch)

    return "".join(out).strip()


def _unquote(v: str) -> str:
    v = v.strip()
    if len(v) >= 2 and ((v[0] == v[-1] == '"') or (v[0] == v[-1] == "'")):
        return v[1:-1]
    return v


def _load_env_file_into_environ(dotenv_path: Path) -> None:
    try:
        text = dotenv_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return
    except Exception as e:
        raise RuntimeError(f"failed to read .env: {e.__class__.__name__}") from None

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        line = _strip_inline_comment(line)
        if not line:
            continue

        if "=" not in line:
            raise ValueError(f"invalid .env line (missing '='): {raw_line}")

        key, value = line.split("=", 1)
        key = key.strip()
        value = _unquote(value.strip())

        if not key:
            raise ValueError(f"invalid .env line (empty key): {raw_line}")

        if key in os.environ:
            continue

        os.environ[key] = value


def _try_load_local_dotenv() -> None:
    candidates: list[Path] = [Path.cwd() / ".env"]

    try:
        here = Path(__file__).resolve()
        base = here.parent
        for _ in range(6):
            candidates.append(base / ".env")
            if base.parent == base:
                break
            base = base.parent
    except Exception:
        pass

    for p in candidates:
        if p.is_file():
            _load_env_file_into_environ(p)
            logger.info(".env loaded (path=%s)", str(p))
            return


# ---------------------------------------------------------------------
# Type conversion helpers
# ---------------------------------------------------------------------
def _get_env(key: str) -> Optional[str]:
    v = os.environ.get(key)
    if v is None:
        return None
    vv = str(v).strip()
    return vv if vv != "" else None


def _as_str(key: str, default: str) -> str:
    v = _get_env(key)
    return v if v is not None else default


def _as_str_opt(key: str) -> Optional[str]:
    v = _get_env(key)
    return v if v is not None else None


def _as_int(key: str, default: int) -> int:
    v = _get_env(key)
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        raise ValueError(f"invalid int for {key}") from None


def _as_float(key: str, default: float) -> float:
    v = _get_env(key)
    if v is None:
        return default
    try:
        x = float(v)
    except Exception:
        raise ValueError(f"invalid float for {key}") from None

    if not math.isfinite(x):
        raise RuntimeError(f"non-finite float for {key}")

    return x


def _as_float_opt(key: str) -> Optional[float]:
    v = _get_env(key)
    if v is None:
        return None
    try:
        x = float(v)
    except Exception:
        raise ValueError(f"invalid float for {key}") from None
    if not math.isfinite(x):
        raise RuntimeError(f"non-finite float for {key}")
    return x


def _as_bool(key: str, default: bool) -> bool:
    v = _get_env(key)
    if v is None:
        return default

    vv = v.strip().lower()
    if vv in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if vv in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise ValueError(f"invalid bool for {key}")


def _as_csv_list(key: str, default: List[str]) -> List[str]:
    v = _get_env(key)
    if v is None:
        return list(default)

    parts = [p.strip() for p in v.split(",")]
    out = [p for p in parts if p]
    return out if out else list(default)


def _as_interval_int_map(key: str) -> Dict[str, int]:
    raw = _get_env(key)
    if raw is None:
        return {}

    try:
        parsed = json.loads(raw)
    except Exception:
        raise ValueError(f"invalid JSON object for {key}") from None

    if not isinstance(parsed, dict):
        raise ValueError(f"{key} must be JSON object mapping interval->int")

    out: Dict[str, int] = {}
    for raw_iv, raw_min in parsed.items():
        iv = str(raw_iv).strip()
        if not iv:
            raise ValueError(f"{key} contains empty interval key")
        try:
            iv_min = int(raw_min)
        except Exception:
            raise ValueError(f"{key}[{iv}] must be int") from None
        if iv_min <= 0:
            raise ValueError(f"{key}[{iv}] must be > 0")
        out[iv] = iv_min
    return out


def _resolve_unique_env_value(keys: List[str], label: str) -> Optional[str]:
    hits: List[tuple[str, str]] = []

    for key in keys:
        value = _get_env(key)
        if value is not None:
            hits.append((key, value))

    if not hits:
        return None

    distinct = {value for _, value in hits}
    if len(distinct) > 1:
        detail = ", ".join(f"{key}={value}" for key, value in hits)
        raise RuntimeError(f"Conflicting environment values for {label}: {detail}")

    return hits[0][1]


def _resolve_str_env(keys: List[str], *, default: str, label: str) -> str:
    value = _resolve_unique_env_value(keys, label)
    return value if value is not None else default


def _resolve_str_opt_env(keys: List[str], *, label: str) -> Optional[str]:
    return _resolve_unique_env_value(keys, label)


def _resolve_int_env(keys: List[str], *, default: int, label: str) -> int:
    raw = _resolve_unique_env_value(keys, label)
    if raw is None:
        return default
    try:
        return int(raw)
    except Exception:
        key_list = ", ".join(keys)
        raise ValueError(f"invalid int for {label} from keys [{key_list}]") from None


def _resolve_float_env(keys: List[str], *, default: float, label: str) -> float:
    raw = _resolve_unique_env_value(keys, label)
    if raw is None:
        return default
    try:
        parsed = float(raw)
    except Exception:
        key_list = ", ".join(keys)
        raise ValueError(f"invalid float for {label} from keys [{key_list}]") from None

    if not math.isfinite(parsed):
        raise RuntimeError(f"non-finite float for {label}")

    return parsed


# ---------------------------------------------------------------------
# Structured role loaders
# ---------------------------------------------------------------------
def _load_trader_openai_settings() -> Dict[str, Any]:
    return {
        "openai_model": _resolve_str_env(
            ["OPENAI_TRADER_MODEL", "OPENAI_MODEL", "OPENAI_SUPERVISOR_MODEL"],
            default="",
            label="trader_openai_model",
        ),
        "openai_max_tokens": _resolve_int_env(
            ["OPENAI_TRADER_MAX_TOKENS", "OPENAI_MAX_TOKENS"],
            default=1500,
            label="trader_openai_max_tokens",
        ),
        "openai_temperature": _resolve_float_env(
            ["OPENAI_TRADER_TEMPERATURE", "OPENAI_TEMPERATURE", "OPENAI_SUPERVISOR_TEMPERATURE"],
            default=0.2,
            label="trader_openai_temperature",
        ),
        "openai_max_latency_sec": _resolve_float_env(
            ["OPENAI_TRADER_MAX_LATENCY_SEC", "OPENAI_TRADER_MAX_LATENCY", "OPENAI_MAX_LATENCY_SEC"],
            default=12.0,
            label="trader_openai_max_latency_sec",
        ),
    }


def _load_analyst_openai_settings() -> Dict[str, Any]:
    return {
        "analyst_openai_model": _resolve_str_env(
            ["ANALYST_OPENAI_MODEL"],
            default="gpt-5-mini",
            label="analyst_openai_model",
        ),
        "analyst_openai_timeout_sec": _resolve_float_env(
            ["ANALYST_OPENAI_TIMEOUT_SEC"],
            default=120.0,
            label="analyst_openai_timeout_sec",
        ),
        "analyst_openai_max_output_tokens": _resolve_int_env(
            ["ANALYST_OPENAI_MAX_OUTPUT_TOKENS"],
            default=1200,
            label="analyst_openai_max_output_tokens",
        ),
        # Responses API 경로에서는 현재 미사용이나, 레거시 호환을 위해 로딩 유지
        "analyst_openai_temperature": _resolve_float_env(
            ["ANALYST_OPENAI_TEMPERATURE"],
            default=0.1,
            label="analyst_openai_temperature",
        ),
        "analyst_openai_reasoning_effort": _resolve_str_opt_env(
            ["ANALYST_OPENAI_REASONING_EFFORT"],
            label="analyst_openai_reasoning_effort",
        ),
    }


# ---------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------
def _validate_supported_ratio_period(period: str) -> None:
    supported = {"5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"}
    if period not in supported:
        raise ValueError(f"analyst_ratio_period must be one of {sorted(supported)}")


def _validate_interval_int_map(name: str, mapping: Dict[str, int]) -> None:
    if not isinstance(mapping, dict):
        raise ValueError(f"{name} must be dict[str,int]")
    for raw_iv, raw_min in mapping.items():
        iv = str(raw_iv).strip()
        if not iv:
            raise ValueError(f"{name} contains empty interval key")
        if not isinstance(raw_min, int):
            raise ValueError(f"{name}[{iv}] must be int")
        if raw_min <= 0:
            raise ValueError(f"{name}[{iv}] must be > 0")


def _validate_http_base_url(name: str, value: str) -> None:
    v = str(value or "").strip()
    if not v:
        raise ValueError(f"{name} is empty")
    if not (v.startswith("http://") or v.startswith("https://")):
        raise ValueError(f"{name} must start with http:// or https://")


def _validate_tf_list(name: str, values: List[str]) -> None:
    if not isinstance(values, list) or not values:
        raise ValueError(f"{name} must be non-empty list[str]")
    cleaned: List[str] = []
    for i, v in enumerate(values):
        s = str(v).strip()
        if not s:
            raise ValueError(f"{name}[{i}] is empty")
        cleaned.append(s)
    if len(set(cleaned)) != len(cleaned):
        raise ValueError(f"{name} contains duplicate intervals")


def _validate_settings(s: Settings) -> None:
    if not s.api_key or not s.api_secret:
        raise RuntimeError("missing Binance credentials (BINANCE_API_KEY / BINANCE_API_SECRET)")

    sym = str(s.symbol or "").strip().upper()
    if not sym:
        raise ValueError("symbol is empty")
    if any(ch.isspace() for ch in sym):
        raise ValueError("symbol contains whitespace")

    interval = str(s.interval or "").strip()
    if not interval:
        raise ValueError("interval is empty")

    if s.leverage < 1:
        raise ValueError("leverage must be >= 1")

    if s.recv_window_ms < 1:
        raise ValueError("recv_window_ms must be >= 1")
    if s.request_timeout_sec < 1:
        raise ValueError("request_timeout_sec must be >= 1")

    if s.qty_step <= 0:
        raise ValueError("qty_step must be > 0")
    if s.min_qty <= 0:
        raise ValueError("min_qty must be > 0")
    if s.price_tick <= 0:
        raise ValueError("price_tick must be > 0")

    mm = str(s.margin_mode or "").strip().upper()
    if mm not in {"ISOLATED", "CROSSED"}:
        raise ValueError("margin_mode must be ISOLATED or CROSSED")

    if not (0.0 < float(s.allocation_ratio) <= 1.0):
        raise ValueError("allocation_ratio must be within (0,1]")

    if not (0.0 < float(s.risk_pct) <= 1.0):
        raise ValueError("risk_pct must be within (0,1] (legacy alias)")
    if abs(float(s.risk_pct) - float(s.allocation_ratio)) > 1e-12:
        raise ValueError("risk_pct must equal allocation_ratio (do not set them differently)")

    for name, v in [
        ("tp_pct_min", s.tp_pct_min),
        ("tp_pct_max", s.tp_pct_max),
        ("sl_pct_min", s.sl_pct_min),
        ("sl_pct_max", s.sl_pct_max),
    ]:
        if not (isinstance(v, (int, float)) and math.isfinite(float(v))):
            raise ValueError(f"{name} must be finite number")
        if float(v) <= 0:
            raise ValueError(f"{name} must be > 0")

    if float(s.tp_pct_min) > float(s.tp_pct_max):
        raise ValueError("tp_pct_min must be <= tp_pct_max")
    if float(s.sl_pct_min) > float(s.sl_pct_max):
        raise ValueError("sl_pct_min must be <= sl_pct_max")

    if s.hard_daily_loss_limit_usdt < 0:
        raise ValueError("hard_daily_loss_limit_usdt must be >= 0")
    if s.hard_consecutive_losses_limit < 0:
        raise ValueError("hard_consecutive_losses_limit must be >= 0")
    if not (0.0 <= s.hard_position_value_pct_cap <= 100.0):
        raise ValueError("hard_position_value_pct_cap must be within 0..100")
    if not (0.0 <= s.hard_liquidation_distance_pct_min <= 100.0):
        raise ValueError("hard_liquidation_distance_pct_min must be within 0..100")

    if not (0.0 <= float(s.max_spread_pct) <= 1.0):
        raise ValueError("max_spread_pct must be within 0..1 (fraction)")
    if float(s.max_spread_abs) < 0:
        raise ValueError("max_spread_abs must be >= 0")
    if float(s.min_entry_volume_ratio) < 0:
        raise ValueError("min_entry_volume_ratio must be >= 0")
    if not (0.0 <= float(s.max_price_jump_pct) <= 1.0):
        raise ValueError("max_price_jump_pct must be within 0..1 (fraction)")
    if s.max_5m_delay_ms < 1:
        raise ValueError("max_5m_delay_ms must be >= 1")
    if s.max_orderbook_age_ms < 1:
        raise ValueError("max_orderbook_age_ms must be >= 1")
    if float(s.min_bbo_notional_usdt) < 0:
        raise ValueError("min_bbo_notional_usdt must be >= 0")

    if float(s.depth_imbalance_min_notional) < 0:
        raise ValueError("depth_imbalance_min_notional must be >= 0")
    if float(s.depth_imbalance_min_ratio) <= 0:
        raise ValueError("depth_imbalance_min_ratio must be > 0")

    if not (0.0 <= float(s.price_deviation_max_pct) <= 1.0):
        raise ValueError("price_deviation_max_pct must be within 0..1 (fraction)")

    if s.slippage_block_pct < 0:
        raise ValueError("slippage_block_pct must be >= 0")
    if s.slippage_block_pct > 1.0:
        raise ValueError("slippage_block_pct must be <= 1.0 (fraction)")
    if s.sigterm_grace_sec <= 0:
        raise ValueError("sigterm_grace_sec must be > 0")

    if s.async_worker_threads < 1:
        raise ValueError("async_worker_threads must be >= 1")
    if s.async_worker_queue_size < 1:
        raise ValueError("async_worker_queue_size must be >= 1")
    if s.reconcile_interval_sec < 1:
        raise ValueError("reconcile_interval_sec must be >= 1")
    if s.reconcile_confirm_n < 1:
        raise ValueError("reconcile_confirm_n must be >= 1")
    if s.max_signal_latency_ms < 1:
        raise ValueError("max_signal_latency_ms must be >= 1")
    if s.max_exec_latency_ms < 1:
        raise ValueError("max_exec_latency_ms must be >= 1")

    if s.preflight_ws_wait_sec < 1:
        raise ValueError("preflight_ws_wait_sec must be >= 1")
    if s.ws_min_kline_buffer < 1:
        raise ValueError("ws_min_kline_buffer must be >= 1")
    if s.ws_max_kline_delay_sec <= 0:
        raise ValueError("ws_max_kline_delay_sec must be > 0")
    if s.ws_market_event_max_delay_sec <= 0:
        raise ValueError("ws_market_event_max_delay_sec must be > 0")
    if s.ws_rest_timeout_sec <= 0:
        raise ValueError("ws_rest_timeout_sec must be > 0")
    if s.ws_min_kline_buffer_long_tf < 1:
        raise ValueError("ws_min_kline_buffer_long_tf must be >= 1")
    _validate_interval_int_map("ws_min_kline_buffer_by_interval", s.ws_min_kline_buffer_by_interval)
    if s.ws_health_fail_log_suppress_sec <= 0:
        raise ValueError("ws_health_fail_log_suppress_sec must be > 0")
    if s.ws_no_buffer_log_suppress_sec <= 0:
        raise ValueError("ws_no_buffer_log_suppress_sec must be > 0")
    if s.ws_pong_max_delay_sec <= 0:
        raise ValueError("ws_pong_max_delay_sec must be > 0")
    if s.ws_pong_startup_grace_sec <= 0:
        raise ValueError("ws_pong_startup_grace_sec must be > 0")
    if s.ws_log_interval_sec < 1:
        raise ValueError("ws_log_interval_sec must be >= 1")
    if s.ws_stale_reset_sec <= 0:
        raise ValueError("ws_stale_reset_sec must be > 0")
    if s.ws_klines_stale_sec <= 30:
        raise ValueError("ws_klines_stale_sec must be > 30")
    if s.ws_backfill_limit < 1:
        raise ValueError("ws_backfill_limit must be >= 1")
    if s.md_store_flush_sec <= 0:
        raise ValueError("md_store_flush_sec must be > 0")
    if s.ob_store_interval_sec <= 0:
        raise ValueError("ob_store_interval_sec must be > 0")
    if s.position_resync_sec <= 0:
        raise ValueError("position_resync_sec must be > 0")

    _validate_tf_list("ws_subscribe_tfs", s.ws_subscribe_tfs)
    _validate_tf_list("ws_required_tfs", s.ws_required_tfs)
    _validate_tf_list("ws_backfill_tfs", s.ws_backfill_tfs)
    _validate_tf_list("md_store_tfs", s.md_store_tfs)
    _validate_tf_list("features_required_tfs", s.features_required_tfs)

    if not set(s.ws_required_tfs).issubset(set(s.ws_subscribe_tfs)):
        raise ValueError("ws_required_tfs must be subset of ws_subscribe_tfs")
    if not set(s.features_required_tfs).issubset(set(s.ws_subscribe_tfs)):
        raise ValueError("features_required_tfs must be subset of ws_subscribe_tfs")

    if not (0.0 < float(s.drift_allocation_abs_jump) <= 1.0):
        raise ValueError("drift_allocation_abs_jump must be within (0,1]")
    if float(s.drift_allocation_spike_ratio) <= 1.0:
        raise ValueError("drift_allocation_spike_ratio must be > 1.0")
    if not (0.0 < float(s.drift_multiplier_abs_jump) <= 1.0):
        raise ValueError("drift_multiplier_abs_jump must be within (0,1]")
    if not (0.0 < float(s.drift_micro_abs_jump) <= 100.0):
        raise ValueError("drift_micro_abs_jump must be within (0,100]")
    if s.drift_stable_regime_steps < 2:
        raise ValueError("drift_stable_regime_steps must be >= 2")

    if not isinstance(s.require_deterministic_client_order_id, bool):
        raise ValueError("require_deterministic_client_order_id must be bool")

    if not (isinstance(s.entry_fill_wait_sec, (int, float)) and math.isfinite(float(s.entry_fill_wait_sec))):
        raise ValueError("entry_fill_wait_sec must be finite number")
    if float(s.entry_fill_wait_sec) <= 0:
        raise ValueError("entry_fill_wait_sec must be > 0")

    if not (isinstance(s.position_reflect_wait_sec, (int, float)) and math.isfinite(float(s.position_reflect_wait_sec))):
        raise ValueError("position_reflect_wait_sec must be finite number")
    if float(s.position_reflect_wait_sec) <= 0:
        raise ValueError("position_reflect_wait_sec must be > 0")

    if not (isinstance(s.exit_fill_wait_sec, (int, float)) and math.isfinite(float(s.exit_fill_wait_sec))):
        raise ValueError("exit_fill_wait_sec must be finite number")
    if float(s.exit_fill_wait_sec) <= 0:
        raise ValueError("exit_fill_wait_sec must be > 0")

    if s.max_entry_slippage_pct is not None:
        if not (isinstance(s.max_entry_slippage_pct, (int, float)) and math.isfinite(float(s.max_entry_slippage_pct))):
            raise ValueError("max_entry_slippage_pct must be finite number or None")
        if float(s.max_entry_slippage_pct) < 0:
            raise ValueError("max_entry_slippage_pct must be >= 0")

    if s.features_error_tg_cooldown_sec <= 0:
        raise ValueError("features_error_tg_cooldown_sec must be > 0")
    if not (0.0 < float(s.low_vol_range_pct_threshold) <= 1.0):
        raise ValueError("low_vol_range_pct_threshold must be within (0,1]")
    if not (0.0 < float(s.low_vol_atr_pct_threshold) <= 1.0):
        raise ValueError("low_vol_atr_pct_threshold must be within (0,1]")

    if not str(s.microstructure_period or "").strip():
        raise ValueError("microstructure_period is empty")
    if s.microstructure_lookback < 2 or s.microstructure_lookback > 500:
        raise ValueError("microstructure_lookback must be within 2..500")
    if s.microstructure_cache_ttl_sec < 1 or s.microstructure_cache_ttl_sec > 300:
        raise ValueError("microstructure_cache_ttl_sec must be within 1..300")

    if not (isinstance(s.engine_watchdog_interval_sec, (int, float)) and math.isfinite(float(s.engine_watchdog_interval_sec))):
        raise ValueError("engine_watchdog_interval_sec must be finite number")
    if float(s.engine_watchdog_interval_sec) <= 0:
        raise ValueError("engine_watchdog_interval_sec must be > 0")
    if s.engine_watchdog_max_db_ping_ms < 1:
        raise ValueError("engine_watchdog_max_db_ping_ms must be >= 1")
    if s.engine_watchdog_emit_min_sec < 1:
        raise ValueError("engine_watchdog_emit_min_sec must be >= 1")

    if s.preflight_fake_usdt <= 0:
        raise ValueError("preflight_fake_usdt must be > 0")

    if s.meta_lookback_trades < 1:
        raise ValueError("meta_lookback_trades must be >= 1")
    if s.meta_recent_window < 1:
        raise ValueError("meta_recent_window must be >= 1")
    if s.meta_baseline_window < 1:
        raise ValueError("meta_baseline_window must be >= 1")
    if s.meta_min_trades_required < 1:
        raise ValueError("meta_min_trades_required must be >= 1")
    if s.meta_max_recent_trades < 1:
        raise ValueError("meta_max_recent_trades must be >= 1")
    if s.meta_recent_window > s.meta_lookback_trades:
        raise ValueError("meta_recent_window must be <= meta_lookback_trades")
    if s.meta_baseline_window > s.meta_lookback_trades:
        raise ValueError("meta_baseline_window must be <= meta_lookback_trades")
    if s.meta_min_trades_required > s.meta_lookback_trades:
        raise ValueError("meta_min_trades_required must be <= meta_lookback_trades")
    if s.meta_max_recent_trades > s.meta_lookback_trades:
        raise ValueError("meta_max_recent_trades must be <= meta_lookback_trades")
    if not str(s.meta_language or "").strip():
        raise ValueError("meta_language is empty")
    if s.meta_max_output_sentences < 1:
        raise ValueError("meta_max_output_sentences must be >= 1")
    if not str(s.meta_prompt_version or "").strip():
        raise ValueError("meta_prompt_version is empty")
    if s.meta_cooldown_sec < 1:
        raise ValueError("meta_cooldown_sec must be >= 1")

    if not str(s.openai_api_key or "").strip():
        raise RuntimeError("missing OpenAI key (OPENAI_API_KEY)")
    if not str(s.openai_model or "").strip():
        raise RuntimeError("missing OpenAI model (OPENAI_MODEL or OPENAI_TRADER_MODEL or OPENAI_SUPERVISOR_MODEL)")
    if not (1 <= int(s.openai_max_tokens) <= 4096):
        raise ValueError("openai_max_tokens must be within 1..4096")
    if not (0.0 <= float(s.openai_temperature) <= 2.0):
        raise ValueError("openai_temperature must be within 0..2")
    if not (
        isinstance(s.openai_max_latency_sec, (int, float))
        and math.isfinite(float(s.openai_max_latency_sec))
        and float(s.openai_max_latency_sec) > 0
    ):
        raise ValueError("openai_max_latency_sec must be finite > 0")

    if s.test_bypass_guards and not s.test_dry_run:
        raise RuntimeError("test_bypass_guards is only allowed with test_dry_run=True (STRICT)")
    if s.test_force_enter and not s.test_dry_run:
        raise RuntimeError("test_force_enter is only allowed with test_dry_run=True (STRICT)")
    if float(s.test_fake_available_usdt) > 0 and not s.test_dry_run:
        raise RuntimeError("test_fake_available_usdt is only allowed with test_dry_run=True (STRICT)")
    if float(s.test_fake_available_usdt) < 0:
        raise ValueError("test_fake_available_usdt must be >= 0 (STRICT)")

    if not str(s.redis_url or "").strip():
        raise RuntimeError("redis_url is required (STRICT)")
    if not str(s.dashboard_cache_prefix or "").strip():
        raise RuntimeError("dashboard_cache_prefix is required (STRICT)")
    if s.dashboard_cache_ttl_sec < 1:
        raise ValueError("dashboard_cache_ttl_sec must be >= 1")
    if s.redis_socket_timeout_sec <= 0:
        raise ValueError("redis_socket_timeout_sec must be > 0")
    if s.redis_socket_connect_timeout_sec <= 0:
        raise ValueError("redis_socket_connect_timeout_sec must be > 0")
    if s.redis_health_check_interval_sec < 1:
        raise ValueError("redis_health_check_interval_sec must be >= 1")

    analyst_symbol = str(s.analyst_market_symbol or "").strip().upper()
    if not analyst_symbol:
        raise ValueError("analyst_market_symbol is empty")
    if any(ch.isspace() for ch in analyst_symbol):
        raise ValueError("analyst_market_symbol contains whitespace")

    if not str(s.analyst_db_market_timeframe or "").strip():
        raise ValueError("analyst_db_market_timeframe is empty")
    if s.analyst_db_market_lookback_limit < 5:
        raise ValueError("analyst_db_market_lookback_limit must be >= 5")
    if s.analyst_trade_lookback_limit < 5:
        raise ValueError("analyst_trade_lookback_limit must be >= 5")
    if s.analyst_event_lookback_limit < 1:
        raise ValueError("analyst_event_lookback_limit must be >= 1")
    if s.analyst_pattern_entry_threshold <= 0:
        raise ValueError("analyst_pattern_entry_threshold must be > 0")
    if s.analyst_spread_guard_bps <= 0:
        raise ValueError("analyst_spread_guard_bps must be > 0")
    if s.analyst_imbalance_guard_abs <= 0:
        raise ValueError("analyst_imbalance_guard_abs must be > 0")

    if not str(s.analyst_openai_model or "").strip():
        raise ValueError("analyst_openai_model is empty")
    if s.analyst_openai_timeout_sec <= 0:
        raise ValueError("analyst_openai_timeout_sec must be > 0")
    if not (1 <= s.analyst_openai_max_output_tokens <= 8192):
        raise ValueError("analyst_openai_max_output_tokens must be within 1..8192")
    if not (0.0 <= float(s.analyst_openai_temperature) <= 2.0):
        raise ValueError("analyst_openai_temperature must be within 0..2")
    if s.analyst_openai_reasoning_effort is not None:
        if s.analyst_openai_reasoning_effort not in {"low", "medium", "high"}:
            raise ValueError("analyst_openai_reasoning_effort must be one of low/medium/high")

    if s.analyst_auto_report_market_interval_sec < 1:
        raise ValueError("analyst_auto_report_market_interval_sec must be >= 1")
    if s.analyst_auto_report_system_interval_sec < 1:
        raise ValueError("analyst_auto_report_system_interval_sec must be >= 1")

    _validate_http_base_url("binance_futures_base", s.binance_futures_base)
    _validate_http_base_url("binance_futures_base_url", s.binance_futures_base_url)
    _validate_http_base_url("binance_futures_rest_base", s.binance_futures_rest_base)

    if not str(s.analyst_kline_interval or "").strip():
        raise ValueError("analyst_kline_interval is empty")
    if s.analyst_kline_limit < 1:
        raise ValueError("analyst_kline_limit must be >= 1")
    if s.analyst_http_timeout_sec <= 0:
        raise ValueError("analyst_http_timeout_sec must be > 0")
    _validate_supported_ratio_period(s.analyst_ratio_period)
    if s.analyst_ratio_limit < 1:
        raise ValueError("analyst_ratio_limit must be >= 1")
    if s.analyst_depth_limit < 1:
        raise ValueError("analyst_depth_limit must be >= 1")
    if s.analyst_funding_limit < 1:
        raise ValueError("analyst_funding_limit must be >= 1")
    if s.analyst_force_order_limit < 1:
        raise ValueError("analyst_force_order_limit must be >= 1")
    if s.analyst_max_server_drift_ms < 0:
        raise ValueError("analyst_max_server_drift_ms must be >= 0")


# ---------------------------------------------------------------------
# Public loader
# ---------------------------------------------------------------------
def load_settings() -> Settings:
    """Load and validate Settings (env-first, optional .env, then defaults)."""
    _try_load_local_dotenv()

    api_key = _as_str("BINANCE_API_KEY", "")
    api_secret = _as_str("BINANCE_API_SECRET", "")

    symbol = _as_str("SYMBOL", "BTCUSDT").upper().replace("-", "")
    interval = _as_str("INTERVAL", "5m")

    leverage = _as_int("LEVERAGE", 1)

    margin_mode = _as_str("MARGIN_MODE", "ISOLATED").upper()
    if margin_mode in {"CROSS", "CROSSED"}:
        margin_mode = "CROSSED"
    if margin_mode not in {"ISOLATED", "CROSSED"}:
        raise ValueError("MARGIN_MODE must be ISOLATED or CROSSED")

    isolated = margin_mode == "ISOLATED"
    futures_position_mode = _as_str("FUTURES_POSITION_MODE", "ONEWAY").upper()

    recv_window_ms = _as_int("RECV_WINDOW_MS", 5000)
    request_timeout_sec = _as_int("REQUEST_TIMEOUT_SEC", 10)

    qty_step = _as_float("QTY_STEP", 0.001)
    min_qty = _as_float("MIN_QTY", 0.001)
    price_tick = _as_float("PRICE_TICK", 0.1)

    alloc_env = _as_float_opt("ALLOCATION_RATIO")
    risk_env = _as_float_opt("RISK_PCT")

    if alloc_env is None and risk_env is None:
        allocation_ratio = 0.35
    elif alloc_env is not None:
        allocation_ratio = float(alloc_env)
        if risk_env is not None and abs(float(risk_env) - allocation_ratio) > 1e-12:
            raise ValueError("ALLOCATION_RATIO and RISK_PCT mismatch (set only one or make them equal)")
    else:
        allocation_ratio = float(risk_env)

    risk_pct = float(allocation_ratio)

    tp_pct = _as_float("TP_PCT", 0.006)
    sl_pct = _as_float("SL_PCT", 0.003)

    tp_pct_min = _as_float("TP_PCT_MIN", 0.0005)
    tp_pct_max = _as_float("TP_PCT_MAX", 0.05)
    sl_pct_min = _as_float("SL_PCT_MIN", 0.0005)
    sl_pct_max = _as_float("SL_PCT_MAX", 0.05)

    max_sl_pct = _as_float("MAX_SL_PCT", 0.015)
    max_trade_qty = _as_float("MAX_TRADE_QTY", 1.0)

    entry_cooldown_sec = _as_float("ENTRY_COOLDOWN_SEC", 8.0)
    cooldown_after_close = _as_float("COOLDOWN_AFTER_CLOSE", 3.0)
    min_entry_score_for_gpt = _as_float("MIN_ENTRY_SCORE_FOR_GPT", 28.0)
    gpt_entry_cooldown_sec = _as_float("GPT_ENTRY_COOLDOWN_SEC", 10.0)

    gpt_daily_call_limit = _as_int("GPT_DAILY_CALL_LIMIT", 2000)
    gpt_max_risk_pct = _as_float("GPT_MAX_RISK_PCT", 0.02)
    gpt_min_confidence = _as_float("GPT_MIN_CONFIDENCE", 0.6)
    gpt_reject_if_over_pnl_pct = _as_float("GPT_REJECT_IF_OVER_PNL_PCT", 0.02)

    openai_api_key = _as_str("OPENAI_API_KEY", "")
    alphavantage_api_key = _as_str("ALPHAVANTAGE_API_KEY", "")
    trader_openai = _load_trader_openai_settings()
    openai_model = str(trader_openai["openai_model"])
    openai_max_tokens = int(trader_openai["openai_max_tokens"])
    openai_temperature = float(trader_openai["openai_temperature"])
    openai_max_latency_sec = float(trader_openai["openai_max_latency_sec"])

    unrealized_notify_sec = _as_int("UNREALIZED_NOTIFY_SEC", 1800)
    enable_exit_gpt = _as_bool("ENABLE_EXIT_GPT", True)

    poll_fills_sec = _as_float("POLL_FILLS_SEC", 3.0)
    max_open_orders = _as_int("MAX_OPEN_ORDERS", 5)

    max_entry_per_day = _as_int("MAX_ENTRY_PER_DAY", 999)
    max_entry_per_day_reset_hour_kst = _as_int("MAX_ENTRY_PER_DAY_RESET_HOUR_KST", 9)

    log_to_file = _as_bool("LOG_TO_FILE", False)
    log_file = _as_str("LOG_FILE", "logs/bot.log")

    telegram_bot_token = _as_str("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id = _as_str("TELEGRAM_CHAT_ID", "")

    ws_enabled = _as_bool("WS_ENABLED", True)
    ws_base = _as_str("BINANCE_FUTURES_WS_BASE", "wss://fstream.binance.com/ws")
    ws_combined_base = _as_str("BINANCE_FUTURES_WS_COMBINED_BASE", "wss://fstream.binance.com/stream?streams=")

    ws_subscribe_tfs = _as_csv_list("WS_SUBSCRIBE_TFS", ["1m", "5m", "15m", "1h", "4h"])
    ws_required_tfs = _as_csv_list("WS_REQUIRED_TFS", ["1m", "5m", "15m", "1h", "4h"])
    preflight_ws_wait_sec = _as_int("PREFLIGHT_WS_WAIT_SEC", 60)
    ws_min_kline_buffer = _as_int("WS_MIN_KLINE_BUFFER", 300)
    ws_max_kline_delay_sec = _as_float("WS_MAX_KLINE_DELAY_SEC", 120.0)
    ws_market_event_max_delay_sec = _resolve_float_env(
        ["WS_MARKET_EVENT_MAX_DELAY_SEC", "WS_ORDERBOOK_MAX_DELAY_SEC"],
        default=10.0,
        label="ws_market_event_max_delay_sec",
    )
    ws_bootstrap_rest_enabled = _as_bool("WS_BOOTSTRAP_REST_ENABLED", True)
    ws_rest_timeout_sec = _as_float("WS_REST_TIMEOUT_SEC", 10.0)
    ws_min_kline_buffer_long_tf = _as_int("WS_MIN_KLINE_BUFFER_LONG_TF", 2)
    ws_min_kline_buffer_by_interval = _as_interval_int_map("WS_MIN_KLINE_BUFFER_BY_INTERVAL")
    ws_health_fail_log_suppress_sec = _as_float("WS_HEALTH_FAIL_LOG_SUPPRESS_SEC", 10.0)
    ws_no_buffer_log_suppress_sec = _as_float("WS_NO_BUFFER_LOG_SUPPRESS_SEC", 30.0)
    ws_pong_max_delay_sec = _as_float("WS_PONG_MAX_DELAY_SEC", 15.0)
    ws_pong_startup_grace_sec = _as_float("WS_PONG_STARTUP_GRACE_SEC", 15.0)
    ws_log_enabled = _as_bool("WS_LOG_ENABLED", False)
    ws_log_interval_sec = _as_int("WS_LOG_INTERVAL_SEC", 60)
    ws_stale_reset_sec = _as_float("WS_STALE_RESET_SEC", 600.0)
    ws_klines_stale_sec = _as_float("WS_KLINES_STALE_SEC", 180.0)

    ws_backfill_tfs = _as_csv_list("WS_BACKFILL_TFS", ["1m", "5m", "15m", "1h", "4h"])
    ws_backfill_limit = _as_int("WS_BACKFILL_LIMIT", 500)

    md_store_flush_sec = _as_float("MD_STORE_FLUSH_SEC", 2.0)
    ob_store_interval_sec = _as_float("OB_STORE_INTERVAL_SEC", 1.0)
    md_store_tfs = _as_csv_list("MD_STORE_TFS", ["1m", "5m", "15m", "1h", "4h"])

    max_spread_pct = _as_float("MAX_SPREAD_PCT", 0.0015)
    max_spread_abs = _as_float("MAX_SPREAD_ABS", 0.0)
    min_entry_volume_ratio = _as_float("MIN_ENTRY_VOLUME_RATIO", 0.15)
    max_price_jump_pct = _as_float("MAX_PRICE_JUMP_PCT", 0.003)

    max_5m_delay_ms = _as_int("MAX_5M_DELAY_MS", 10 * 60 * 1000)
    max_orderbook_age_ms = _as_int("MAX_ORDERBOOK_AGE_MS", 3000)
    min_bbo_notional_usdt = _as_float("MIN_BBO_NOTIONAL_USDT", 0.0)

    depth_imbalance_enabled = _as_bool("DEPTH_IMBALANCE_ENABLED", True)
    depth_imbalance_min_notional = _as_float("DEPTH_IMBALANCE_MIN_NOTIONAL", 10.0)
    depth_imbalance_min_ratio = _as_float("DEPTH_IMBALANCE_MIN_RATIO", 2.0)

    price_deviation_guard_enabled = _as_bool("PRICE_DEVIATION_GUARD_ENABLED", True)
    price_deviation_max_pct = _as_float("PRICE_DEVIATION_MAX_PCT", 0.0015)

    session_spread_mult_asia = _as_float("SESSION_SPREAD_MULT_ASIA", 1.0)
    session_spread_mult_eu = _as_float("SESSION_SPREAD_MULT_EU", 1.1)
    session_spread_mult_us = _as_float("SESSION_SPREAD_MULT_US", 1.2)

    session_jump_mult_asia = _as_float("SESSION_JUMP_MULT_ASIA", 1.0)
    session_jump_mult_eu = _as_float("SESSION_JUMP_MULT_EU", 1.1)
    session_jump_mult_us = _as_float("SESSION_JUMP_MULT_US", 1.2)

    data_health_notify_sec = _as_int("DATA_HEALTH_NOTIFY_SEC", 900)
    signal_analysis_interval_sec = _as_int("SIGNAL_ANALYSIS_INTERVAL_SEC", 60)
    krw_per_usdt = _as_float("KRW_PER_USDT", 1400.0)

    sigterm_grace_sec = _as_int("SIGTERM_GRACE_SEC", 30)
    allow_start_without_leverage_setup = _as_bool("ALLOW_START_WITHOUT_LEVERAGE_SETUP", False)

    async_worker_threads = _as_int("ASYNC_WORKER_THREADS", 1)
    async_worker_queue_size = _as_int("ASYNC_WORKER_QUEUE_SIZE", 2000)
    reconcile_interval_sec = _as_int("RECONCILE_INTERVAL_SEC", 30)
    reconcile_confirm_n = _as_int("RECONCILE_CONFIRM_N", 3)
    force_close_on_desync = _as_bool("FORCE_CLOSE_ON_DESYNC", False)
    max_signal_latency_ms = _as_int("MAX_SIGNAL_LATENCY_MS", 200)
    max_exec_latency_ms = _as_int("MAX_EXEC_LATENCY_MS", 2500)

    position_resync_sec = _as_float("POSITION_RESYNC_SEC", 20.0)

    drift_allocation_abs_jump = _as_float("DRIFT_ALLOCATION_ABS_JUMP", 0.45)
    drift_allocation_spike_ratio = _as_float("DRIFT_ALLOCATION_SPIKE_RATIO", 3.0)
    drift_multiplier_abs_jump = _as_float("DRIFT_MULTIPLIER_ABS_JUMP", 0.50)
    drift_micro_abs_jump = _as_float("DRIFT_MICRO_ABS_JUMP", 40.0)
    drift_stable_regime_steps = _as_int("DRIFT_STABLE_REGIME_STEPS", 100)

    resolved_binance_base = _resolve_str_env(
        ["BINANCE_FUTURES_REST_BASE", "BINANCE_FUTURES_BASE_URL", "BINANCE_FUTURES_BASE"],
        default="https://fapi.binance.com",
        label="binance_futures_base_family",
    )
    binance_futures_base = resolved_binance_base
    binance_futures_base_url = resolved_binance_base
    binance_futures_rest_base = resolved_binance_base
    binance_recv_window = recv_window_ms
    binance_http_timeout_sec = request_timeout_sec

    hard_daily_loss_limit_usdt = _as_float("HARD_DAILY_LOSS_LIMIT_USDT", 0.0)
    hard_consecutive_losses_limit = _as_int("HARD_CONSECUTIVE_LOSSES_LIMIT", 0)
    hard_position_value_pct_cap = _as_float("HARD_POSITION_VALUE_PCT_CAP", 100.0)
    hard_liquidation_distance_pct_min = _as_float("HARD_LIQUIDATION_DISTANCE_PCT_MIN", 0.0)

    slippage_block_pct = _as_float("SLIPPAGE_BLOCK_PCT", 0.002)
    slippage_stop_engine = _as_bool("SLIPPAGE_STOP_ENGINE", False)
    protection_mode_enabled = _as_bool("PROTECTION_MODE_ENABLED", True)

    require_deterministic_client_order_id = _as_bool("REQUIRE_DETERMINISTIC_CLIENT_ORDER_ID", True)
    entry_fill_wait_sec = _as_float("ENTRY_FILL_WAIT_SEC", 2.0)
    position_reflect_wait_sec = _as_float("POSITION_REFLECT_WAIT_SEC", 1.2)
    exit_fill_wait_sec = _as_float("EXIT_FILL_WAIT_SEC", 2.0)
    max_entry_slippage_pct = _as_float_opt("MAX_ENTRY_SLIPPAGE_PCT")

    features_required_tfs = _as_csv_list("FEATURES_REQUIRED_TFS", ["1m", "5m", "15m", "1h", "4h"])
    features_error_tg_cooldown_sec = _as_float("FEATURES_ERROR_TG_COOLDOWN_SEC", 60.0)
    low_vol_range_pct_threshold = _as_float("LOW_VOL_RANGE_PCT_THRESHOLD", 0.01)
    low_vol_atr_pct_threshold = _as_float("LOW_VOL_ATR_PCT_THRESHOLD", 0.004)

    microstructure_period = _as_str("MICROSTRUCTURE_PERIOD", "5m")
    microstructure_lookback = _as_int("MICROSTRUCTURE_LOOKBACK", 30)
    microstructure_cache_ttl_sec = _as_int("MICROSTRUCTURE_CACHE_TTL_SEC", 15)

    engine_watchdog_interval_sec = _as_float("ENGINE_WATCHDOG_INTERVAL_SEC", 5.0)
    engine_watchdog_max_db_ping_ms = _as_int("ENGINE_WATCHDOG_MAX_DB_PING_MS", 1500)
    engine_watchdog_emit_min_sec = _as_int("ENGINE_WATCHDOG_EMIT_MIN_SEC", 30)

    preflight_fake_usdt = _as_float("PREFLIGHT_FAKE_USDT", 1000.0)
    meta_lookback_trades = _as_int("META_LOOKBACK_TRADES", 200)
    meta_recent_window = _as_int("META_RECENT_WINDOW", 20)
    meta_baseline_window = _as_int("META_BASELINE_WINDOW", 60)
    meta_min_trades_required = _as_int("META_MIN_TRADES_REQUIRED", 20)
    meta_max_recent_trades = _as_int("META_MAX_RECENT_TRADES", 20)
    meta_language = _as_str("META_LANGUAGE", "ko")
    meta_max_output_sentences = _as_int("META_MAX_OUTPUT_SENTENCES", 3)
    meta_prompt_version = _as_str("META_PROMPT_VERSION", "2026-03-03")
    meta_cooldown_sec = _as_int("META_COOLDOWN_SEC", 60)

    redis_url = _as_str("REDIS_URL", "redis://localhost:6379/0")
    dashboard_cache_prefix = _as_str("DASHBOARD_CACHE_PREFIX", "auto_trader_dashboard")
    dashboard_cache_ttl_sec = _as_int("DASHBOARD_CACHE_TTL_SEC", 5)
    redis_socket_timeout_sec = _as_float("REDIS_SOCKET_TIMEOUT_SEC", 1.0)
    redis_socket_connect_timeout_sec = _as_float("REDIS_SOCKET_CONNECT_TIMEOUT_SEC", 1.0)
    redis_health_check_interval_sec = _as_int("REDIS_HEALTH_CHECK_INTERVAL_SEC", 30)

    test_dry_run = _as_bool("TEST_DRY_RUN", False)
    test_bypass_guards = _as_bool("TEST_BYPASS_GUARDS", False)
    test_force_enter = _as_bool("TEST_FORCE_ENTER", False)
    test_fake_available_usdt = _as_float("TEST_FAKE_AVAILABLE_USDT", 0.0)

    analyst_market_symbol = _as_str("ANALYST_MARKET_SYMBOL", symbol).upper().replace("-", "")
    analyst_db_market_timeframe = _as_str("ANALYST_DB_MARKET_TIMEFRAME", interval)
    analyst_db_market_lookback_limit = _as_int("ANALYST_DB_MARKET_LOOKBACK_LIMIT", 60)
    analyst_pattern_entry_threshold = _as_float("ANALYST_PATTERN_ENTRY_THRESHOLD", 0.55)
    analyst_spread_guard_bps = _as_float("ANALYST_SPREAD_GUARD_BPS", 15.0)
    analyst_imbalance_guard_abs = _as_float("ANALYST_IMBALANCE_GUARD_ABS", 0.60)
    analyst_trade_lookback_limit = _as_int("ANALYST_TRADE_LOOKBACK_LIMIT", 30)
    analyst_event_lookback_limit = _as_int("ANALYST_EVENT_LOOKBACK_LIMIT", 300)
    analyst_include_external_market = _as_bool("ANALYST_INCLUDE_EXTERNAL_MARKET", True)

    analyst_auto_report_market_interval_sec = _as_int("ANALYST_AUTO_REPORT_MARKET_INTERVAL_SEC", 300)
    analyst_auto_report_system_interval_sec = _as_int("ANALYST_AUTO_REPORT_SYSTEM_INTERVAL_SEC", 1800)
    analyst_auto_report_persist = _as_bool("ANALYST_AUTO_REPORT_PERSIST", True)
    analyst_auto_report_notify = _as_bool("ANALYST_AUTO_REPORT_NOTIFY", False)

    analyst_openai = _load_analyst_openai_settings()
    analyst_openai_model = str(analyst_openai["analyst_openai_model"])
    analyst_openai_timeout_sec = float(analyst_openai["analyst_openai_timeout_sec"])
    analyst_openai_max_output_tokens = int(analyst_openai["analyst_openai_max_output_tokens"])
    analyst_openai_temperature = float(analyst_openai["analyst_openai_temperature"])
    analyst_openai_reasoning_effort = analyst_openai["analyst_openai_reasoning_effort"]

    analyst_kline_interval = _as_str("ANALYST_KLINE_INTERVAL", "5m")
    analyst_kline_limit = _as_int("ANALYST_KLINE_LIMIT", 300)
    analyst_http_timeout_sec = _as_float("ANALYST_HTTP_TIMEOUT_SEC", 10.0)
    analyst_ratio_period = _as_str("ANALYST_RATIO_PERIOD", "5m")
    analyst_ratio_limit = _as_int("ANALYST_RATIO_LIMIT", 30)
    analyst_depth_limit = _as_int("ANALYST_DEPTH_LIMIT", 20)
    analyst_funding_limit = _as_int("ANALYST_FUNDING_LIMIT", 10)
    analyst_force_order_limit = _as_int("ANALYST_FORCE_ORDER_LIMIT", 50)
    analyst_max_server_drift_ms = _as_int("ANALYST_MAX_SERVER_DRIFT_MS", 5000)

    s = Settings(
        api_key=api_key,
        api_secret=api_secret,
        symbol=symbol,
        interval=interval,
        leverage=leverage,
        isolated=isolated,
        margin_mode=margin_mode,
        futures_position_mode=futures_position_mode,
        recv_window_ms=recv_window_ms,
        request_timeout_sec=request_timeout_sec,
        qty_step=qty_step,
        min_qty=min_qty,
        price_tick=price_tick,
        allocation_ratio=float(allocation_ratio),
        risk_pct=float(risk_pct),
        tp_pct=tp_pct,
        sl_pct=sl_pct,
        tp_pct_min=tp_pct_min,
        tp_pct_max=tp_pct_max,
        sl_pct_min=sl_pct_min,
        sl_pct_max=sl_pct_max,
        max_sl_pct=max_sl_pct,
        max_trade_qty=max_trade_qty,
        entry_cooldown_sec=entry_cooldown_sec,
        cooldown_after_close=cooldown_after_close,
        min_entry_score_for_gpt=min_entry_score_for_gpt,
        gpt_entry_cooldown_sec=gpt_entry_cooldown_sec,
        gpt_daily_call_limit=gpt_daily_call_limit,
        gpt_max_risk_pct=gpt_max_risk_pct,
        gpt_min_confidence=gpt_min_confidence,
        gpt_reject_if_over_pnl_pct=gpt_reject_if_over_pnl_pct,
        openai_api_key=openai_api_key,
        openai_model=openai_model,
        openai_max_tokens=openai_max_tokens,
        openai_temperature=openai_temperature,
        openai_max_latency_sec=openai_max_latency_sec,
        alphavantage_api_key=alphavantage_api_key,
        unrealized_notify_sec=unrealized_notify_sec,
        enable_exit_gpt=enable_exit_gpt,
        poll_fills_sec=poll_fills_sec,
        max_open_orders=max_open_orders,
        max_entry_per_day=max_entry_per_day,
        max_entry_per_day_reset_hour_kst=max_entry_per_day_reset_hour_kst,
        log_to_file=log_to_file,
        log_file=log_file,
        telegram_bot_token=telegram_bot_token,
        telegram_chat_id=telegram_chat_id,
        ws_enabled=ws_enabled,
        ws_base=ws_base,
        ws_combined_base=ws_combined_base,
        ws_subscribe_tfs=ws_subscribe_tfs,
        ws_required_tfs=ws_required_tfs,
        preflight_ws_wait_sec=preflight_ws_wait_sec,
        ws_min_kline_buffer=ws_min_kline_buffer,
        ws_max_kline_delay_sec=ws_max_kline_delay_sec,
        ws_market_event_max_delay_sec=ws_market_event_max_delay_sec,
        ws_bootstrap_rest_enabled=ws_bootstrap_rest_enabled,
        ws_rest_timeout_sec=ws_rest_timeout_sec,
        ws_min_kline_buffer_long_tf=ws_min_kline_buffer_long_tf,
        ws_min_kline_buffer_by_interval=ws_min_kline_buffer_by_interval,
        ws_health_fail_log_suppress_sec=ws_health_fail_log_suppress_sec,
        ws_no_buffer_log_suppress_sec=ws_no_buffer_log_suppress_sec,
        ws_pong_max_delay_sec=ws_pong_max_delay_sec,
        ws_pong_startup_grace_sec=ws_pong_startup_grace_sec,
        ws_log_enabled=ws_log_enabled,
        ws_log_interval_sec=ws_log_interval_sec,
        ws_stale_reset_sec=ws_stale_reset_sec,
        ws_klines_stale_sec=ws_klines_stale_sec,
        ws_backfill_tfs=ws_backfill_tfs,
        ws_backfill_limit=ws_backfill_limit,
        md_store_flush_sec=md_store_flush_sec,
        ob_store_interval_sec=ob_store_interval_sec,
        md_store_tfs=md_store_tfs,
        max_spread_pct=max_spread_pct,
        max_spread_abs=max_spread_abs,
        min_entry_volume_ratio=min_entry_volume_ratio,
        max_price_jump_pct=max_price_jump_pct,
        max_5m_delay_ms=max_5m_delay_ms,
        max_orderbook_age_ms=max_orderbook_age_ms,
        min_bbo_notional_usdt=min_bbo_notional_usdt,
        depth_imbalance_enabled=depth_imbalance_enabled,
        depth_imbalance_min_notional=depth_imbalance_min_notional,
        depth_imbalance_min_ratio=depth_imbalance_min_ratio,
        price_deviation_guard_enabled=price_deviation_guard_enabled,
        price_deviation_max_pct=price_deviation_max_pct,
        session_spread_mult_asia=session_spread_mult_asia,
        session_spread_mult_eu=session_spread_mult_eu,
        session_spread_mult_us=session_spread_mult_us,
        session_jump_mult_asia=session_jump_mult_asia,
        session_jump_mult_eu=session_jump_mult_eu,
        session_jump_mult_us=session_jump_mult_us,
        data_health_notify_sec=data_health_notify_sec,
        signal_analysis_interval_sec=signal_analysis_interval_sec,
        krw_per_usdt=krw_per_usdt,
        sigterm_grace_sec=sigterm_grace_sec,
        allow_start_without_leverage_setup=allow_start_without_leverage_setup,
        async_worker_threads=async_worker_threads,
        async_worker_queue_size=async_worker_queue_size,
        reconcile_interval_sec=reconcile_interval_sec,
        reconcile_confirm_n=reconcile_confirm_n,
        force_close_on_desync=force_close_on_desync,
        max_signal_latency_ms=max_signal_latency_ms,
        max_exec_latency_ms=max_exec_latency_ms,
        position_resync_sec=position_resync_sec,
        drift_allocation_abs_jump=drift_allocation_abs_jump,
        drift_allocation_spike_ratio=drift_allocation_spike_ratio,
        drift_multiplier_abs_jump=drift_multiplier_abs_jump,
        drift_micro_abs_jump=drift_micro_abs_jump,
        drift_stable_regime_steps=drift_stable_regime_steps,
        binance_futures_base=binance_futures_base,
        binance_futures_base_url=binance_futures_base_url,
        binance_futures_rest_base=binance_futures_rest_base,
        binance_recv_window=binance_recv_window,
        binance_http_timeout_sec=binance_http_timeout_sec,
        hard_daily_loss_limit_usdt=hard_daily_loss_limit_usdt,
        hard_consecutive_losses_limit=hard_consecutive_losses_limit,
        hard_position_value_pct_cap=hard_position_value_pct_cap,
        hard_liquidation_distance_pct_min=hard_liquidation_distance_pct_min,
        slippage_block_pct=slippage_block_pct,
        slippage_stop_engine=slippage_stop_engine,
        protection_mode_enabled=protection_mode_enabled,
        require_deterministic_client_order_id=require_deterministic_client_order_id,
        entry_fill_wait_sec=entry_fill_wait_sec,
        position_reflect_wait_sec=position_reflect_wait_sec,
        exit_fill_wait_sec=exit_fill_wait_sec,
        max_entry_slippage_pct=max_entry_slippage_pct,
        features_required_tfs=features_required_tfs,
        features_error_tg_cooldown_sec=features_error_tg_cooldown_sec,
        low_vol_range_pct_threshold=low_vol_range_pct_threshold,
        low_vol_atr_pct_threshold=low_vol_atr_pct_threshold,
        microstructure_period=microstructure_period,
        microstructure_lookback=microstructure_lookback,
        microstructure_cache_ttl_sec=microstructure_cache_ttl_sec,
        engine_watchdog_interval_sec=engine_watchdog_interval_sec,
        engine_watchdog_max_db_ping_ms=engine_watchdog_max_db_ping_ms,
        engine_watchdog_emit_min_sec=engine_watchdog_emit_min_sec,
        preflight_fake_usdt=preflight_fake_usdt,
        meta_lookback_trades=meta_lookback_trades,
        meta_recent_window=meta_recent_window,
        meta_baseline_window=meta_baseline_window,
        meta_min_trades_required=meta_min_trades_required,
        meta_max_recent_trades=meta_max_recent_trades,
        meta_language=meta_language,
        meta_max_output_sentences=meta_max_output_sentences,
        meta_prompt_version=meta_prompt_version,
        meta_cooldown_sec=meta_cooldown_sec,
        test_dry_run=test_dry_run,
        test_bypass_guards=test_bypass_guards,
        test_force_enter=test_force_enter,
        test_fake_available_usdt=test_fake_available_usdt,
        redis_url=redis_url,
        dashboard_cache_prefix=dashboard_cache_prefix,
        dashboard_cache_ttl_sec=dashboard_cache_ttl_sec,
        redis_socket_timeout_sec=redis_socket_timeout_sec,
        redis_socket_connect_timeout_sec=redis_socket_connect_timeout_sec,
        redis_health_check_interval_sec=redis_health_check_interval_sec,
        analyst_market_symbol=analyst_market_symbol,
        analyst_db_market_timeframe=analyst_db_market_timeframe,
        analyst_db_market_lookback_limit=analyst_db_market_lookback_limit,
        analyst_pattern_entry_threshold=analyst_pattern_entry_threshold,
        analyst_spread_guard_bps=analyst_spread_guard_bps,
        analyst_imbalance_guard_abs=analyst_imbalance_guard_abs,
        analyst_trade_lookback_limit=analyst_trade_lookback_limit,
        analyst_event_lookback_limit=analyst_event_lookback_limit,
        analyst_include_external_market=analyst_include_external_market,
        analyst_auto_report_market_interval_sec=analyst_auto_report_market_interval_sec,
        analyst_auto_report_system_interval_sec=analyst_auto_report_system_interval_sec,
        analyst_auto_report_persist=analyst_auto_report_persist,
        analyst_auto_report_notify=analyst_auto_report_notify,
        analyst_openai_model=analyst_openai_model,
        analyst_openai_timeout_sec=analyst_openai_timeout_sec,
        analyst_openai_max_output_tokens=analyst_openai_max_output_tokens,
        analyst_openai_temperature=analyst_openai_temperature,
        analyst_openai_reasoning_effort=analyst_openai_reasoning_effort,
        analyst_kline_interval=analyst_kline_interval,
        analyst_kline_limit=analyst_kline_limit,
        analyst_http_timeout_sec=analyst_http_timeout_sec,
        analyst_ratio_period=analyst_ratio_period,
        analyst_ratio_limit=analyst_ratio_limit,
        analyst_depth_limit=analyst_depth_limit,
        analyst_funding_limit=analyst_funding_limit,
        analyst_force_order_limit=analyst_force_order_limit,
        analyst_max_server_drift_ms=analyst_max_server_drift_ms,
    )

    _validate_settings(s)
    return s


SETTINGS = load_settings()


__all__ = [
    "Settings",
    "BotSettings",
    "PatternStrengthSettings",
    "SETTINGS",
    "load_settings",
    "KST",
]