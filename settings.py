# settings.py
"""
========================================================
FILE: settings.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

설계 원칙
- settings.py 는 단일 설정 소스(SSOT)다.
- env(os.environ) 우선, 로컬 .env는 옵션이며 env를 덮어쓰지 않는다.
- 잘못된 설정은 즉시 예외(Fail-Fast).
- 민감정보(API key/secret, DB URL 등) 로그 금지.
- 테스트 토글은 settings에서만 읽는다(다른 모듈의 os.getenv 직접 접근 금지).

변경 이력
--------------------------------------------------------
- 2026-03-04 (TRADE-GRADE):
  1) OpenAI 설정 SSOT 필드 추가:
     - openai_api_key / openai_model / openai_max_tokens / openai_temperature / openai_max_latency_sec
  2) ENV 매핑 추가:
     - OPENAI_API_KEY → openai_api_key
     - OPENAI_MODEL/OPENAI_TRADER_MODEL/OPENAI_SUPERVISOR_MODEL → openai_model (alias 우선순위)
     - OPENAI_MAX_TOKENS 등 OpenAI 관련 설정 매핑 추가
  3) STRICT 검증 추가:
     - openai_api_key / openai_model 누락 시 즉시 예외
     - openai_max_tokens 범위, temperature 범위, max_latency_sec > 0 검증

- 2026-03-03 (TRADE-GRADE):
  1) 테스트 토글을 settings로 공식화(SSOT):
     - test_dry_run / test_bypass_guards / test_force_enter / test_fake_available_usdt 추가
  2) 운영 사고 방지:
     - test_bypass_guards / test_force_enter / test_fake_available_usdt 는 test_dry_run=True에서만 허용(아니면 즉시 예외)
  3) run_bot_ws에서 getattr 기본값으로 읽던 운영 파라미터를 settings로 정식 수용:
     - ws_backfill_tfs / ws_backfill_limit
     - md_store_flush_sec / ob_store_interval_sec / md_store_tfs
     - position_resync_sec
  4) 누락된 env 매핑 보완:
     - session_jump_mult_* env 로딩 추가

========================================================
"""

from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass, field
from datetime import timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# Korea Standard Time (UTC+9) - (외부 모듈에서 사용할 수 있어 유지)
KST = timezone(timedelta(hours=9))


# -----------------------------------------------------------------------------
# Dataclasses
# -----------------------------------------------------------------------------
@dataclass(slots=True)
class Settings:
    """Runtime settings for the trading engine (Binance USDT-M Futures)."""

    # Binance credentials (exchange_api.py expects these names)
    api_key: str = ""
    api_secret: str = ""

    # Core trading
    symbol: str = "BTCUSDT"
    interval: str = "5m"

    # Futures config
    leverage: int = 1  # ✅ 전액 배분형 전제: 기본 1
    isolated: bool = True
    margin_mode: str = "ISOLATED"
    futures_position_mode: str = "ONEWAY"

    # HTTP / signing
    recv_window_ms: int = 5000
    request_timeout_sec: int = 10

    # Symbol filters (legacy; 실제 필터는 exchangeInfo로 확정)
    qty_step: float = 0.001
    min_qty: float = 0.001
    price_tick: float = 0.1

    # Strategy / sizing
    # ✅ canonical: allocation_ratio(0~1) = 계좌 투입 비율
    allocation_ratio: float = 1.0
    # ✅ legacy alias (호환): 의미는 allocation_ratio와 동일하게 강제
    risk_pct: float = 1.0

    # Default TP/SL (signal이 이 값을 override 가능)
    tp_pct: float = 0.006
    sl_pct: float = 0.003

    # TP/SL bounds (signal 값 가드)
    tp_pct_min: float = 0.0005
    tp_pct_max: float = 0.05
    sl_pct_min: float = 0.0005
    sl_pct_max: float = 0.05  # legacy max_sl_pct와 별개(운영형 가드)

    # Legacy (kept)
    max_sl_pct: float = 0.015
    max_trade_qty: float = 1.0

    # Entry/exit cadence
    entry_cooldown_sec: float = 8.0
    cooldown_after_close: float = 3.0
    min_entry_score_for_gpt: float = 28
    gpt_entry_cooldown_sec: float = 10.0

    # GPT safety (legacy fields kept)
    gpt_daily_call_limit: int = 2000
    gpt_max_risk_pct: float = 0.02
    gpt_min_confidence: float = 0.6
    gpt_reject_if_over_pnl_pct: float = 0.02

    # ─────────────────────────────────────────────
    # OpenAI (SSOT) — TRADE-GRADE
    # ─────────────────────────────────────────────
    # NOTE: OPENAI_API_KEY는 반드시 존재해야 한다(Preflight에서 강제).
    openai_api_key: str = ""
    # NOTE: 모델은 반드시 명시되어야 한다(OPENAI_MODEL 또는 alias env로 설정).
    openai_model: str = ""
    openai_max_tokens: int = 1500
    openai_temperature: float = 0.2
    openai_max_latency_sec: float = 12.0

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

    # WebSocket buffering
    ws_enabled: bool = True
    ws_base: str = "wss://fstream.binance.com/ws"
    ws_combined_base: str = "wss://fstream.binance.com/stream?streams="
    ws_subscribe_tfs: List[str] = field(default_factory=lambda: ["1m", "5m", "15m", "1h", "4h"])
    ws_required_tfs: List[str] = field(default_factory=lambda: ["1m", "5m", "15m", "1h", "4h"])
    # PRE-FLIGHT WS bootstrap timeout
    preflight_ws_wait_sec = 60
    ws_min_kline_buffer: int = 60
    ws_max_kline_delay_sec: float = 120.0
    ws_orderbook_max_delay_sec: float = 10.0
    ws_log_enabled: bool = False
    ws_log_interval_sec: int = 60
    ws_stale_reset_sec: float = 600.0

    # WS bootstrap/backfill (run_bot_ws)
    ws_backfill_tfs: List[str] = field(default_factory=lambda: ["1m", "5m", "15m", "1h", "4h"])
    ws_backfill_limit: int = 500

    # Market-data store worker settings (run_bot_ws)
    md_store_flush_sec: float = 5.0
    ob_store_interval_sec: float = 5.0
    md_store_tfs: List[str] = field(default_factory=lambda: ["1m", "5m", "15m"])

    # Guards (entry_guards_ws)
    max_spread_pct: float = 0.0008
    max_spread_abs: float = 0.0
    min_entry_volume_ratio: float = 0.3
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

    # 10점 근접(최소 변경) 운영 파라미터
    async_worker_threads: int = 1
    async_worker_queue_size: int = 2000
    reconcile_interval_sec: int = 30
    force_close_on_desync: bool = False
    max_signal_latency_ms: int = 200
    max_exec_latency_ms: int = 400

    # run_bot_ws: exchange state resync
    position_resync_sec: float = 20.0

    # Exchange endpoints
    binance_futures_base: str = "https://fapi.binance.com"

    # Legacy aliases
    binance_recv_window: int = 5000
    binance_http_timeout_sec: int = 10

    # Hard risk guards
    hard_daily_loss_limit_usdt: float = 0.0
    hard_consecutive_losses_limit: int = 0
    hard_position_value_pct_cap: float = 100.0
    hard_liquidation_distance_pct_min: float = 0.0

    # Slippage / protection
    slippage_block_pct: float = 0.3
    slippage_stop_engine: bool = False
    protection_mode_enabled: bool = True

    # 9점대 운영형 실행/멱등성/체결 확정
    require_deterministic_client_order_id: bool = True
    entry_fill_wait_sec: float = 2.0
    max_entry_slippage_pct: Optional[float] = None  # None이면 비활성

    # ─────────────────────────────────────────────
    # TEST controls (TRADE-GRADE)
    # ─────────────────────────────────────────────
    test_dry_run: bool = False
    test_bypass_guards: bool = False
    test_force_enter: bool = False
    test_fake_available_usdt: float = 0.0

    preflight_ws_wait_sec = 60
# Backward-compatible alias for legacy type hints
BotSettings = Settings


@dataclass(slots=True)
class PatternStrengthSettings:
    """Optional pattern-strength overrides used by pattern_detection."""
    pattern_strengths: Dict[str, float] = field(default_factory=dict)


# -----------------------------------------------------------------------------
# Optional .env loader (no external deps)
# -----------------------------------------------------------------------------
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

        # env 우선권: 이미 존재하면 덮어쓰지 않는다.
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


# -----------------------------------------------------------------------------
# Type conversion helpers
# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
# Validation
# -----------------------------------------------------------------------------
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
    if mm in {"CROSS", "CROSSED"}:
        mm = "CROSSED"
    if mm not in {"ISOLATED", "CROSSED"}:
        raise ValueError("margin_mode must be ISOLATED or CROSSED")

    # Allocation Mode: allocation_ratio(0~1] 강제
    if not (0.0 < float(s.allocation_ratio) <= 1.0):
        raise ValueError("allocation_ratio must be within (0,1]")

    # Legacy alias도 같은 범위 + 값 일치 강제(스케일 충돌 방지)
    if not (0.0 < float(s.risk_pct) <= 1.0):
        raise ValueError("risk_pct must be within (0,1] (legacy alias)")
    if abs(float(s.risk_pct) - float(s.allocation_ratio)) > 1e-12:
        raise ValueError("risk_pct must equal allocation_ratio (do not set them differently)")

    # TP/SL bounds
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

    if s.slippage_block_pct < 0:
        raise ValueError("slippage_block_pct must be >= 0")

    if s.sigterm_grace_sec <= 0:
        raise ValueError("sigterm_grace_sec must be > 0")

    # 10점 근접(최소 변경) 운영 파라미터
    if s.async_worker_threads < 1:
        raise ValueError("async_worker_threads must be >= 1")
    if s.async_worker_queue_size < 1:
        raise ValueError("async_worker_queue_size must be >= 1")
    if s.reconcile_interval_sec < 1:
        raise ValueError("reconcile_interval_sec must be >= 1")
    if s.max_signal_latency_ms < 1:
        raise ValueError("max_signal_latency_ms must be >= 1")
    if s.max_exec_latency_ms < 1:
        raise ValueError("max_exec_latency_ms must be >= 1")

    # WS bootstrap/store
    if s.ws_backfill_limit < 1:
        raise ValueError("ws_backfill_limit must be >= 1")
    if s.md_store_flush_sec <= 0:
        raise ValueError("md_store_flush_sec must be > 0")
    if s.ob_store_interval_sec <= 0:
        raise ValueError("ob_store_interval_sec must be > 0")
    if s.position_resync_sec <= 0:
        raise ValueError("position_resync_sec must be > 0")

    # 9점대 운영형
    if not isinstance(s.require_deterministic_client_order_id, bool):
        raise ValueError("require_deterministic_client_order_id must be bool")

    if not (isinstance(s.entry_fill_wait_sec, (int, float)) and math.isfinite(float(s.entry_fill_wait_sec))):
        raise ValueError("entry_fill_wait_sec must be finite number")
    if float(s.entry_fill_wait_sec) <= 0:
        raise ValueError("entry_fill_wait_sec must be > 0")

    if s.max_entry_slippage_pct is not None:
        if not (isinstance(s.max_entry_slippage_pct, (int, float)) and math.isfinite(float(s.max_entry_slippage_pct))):
            raise ValueError("max_entry_slippage_pct must be finite number or None")
        if float(s.max_entry_slippage_pct) < 0:
            raise ValueError("max_entry_slippage_pct must be >= 0")

    # OpenAI (SSOT) — STRICT
    if not str(s.openai_api_key or "").strip():
        raise RuntimeError("missing OpenAI key (OPENAI_API_KEY)")
    if not str(s.openai_model or "").strip():
        raise RuntimeError("missing OpenAI model (OPENAI_MODEL or OPENAI_TRADER_MODEL or OPENAI_SUPERVISOR_MODEL)")
    if not (1 <= int(s.openai_max_tokens) <= 4096):
        raise ValueError("openai_max_tokens must be within 1..4096")
    if not (0.0 <= float(s.openai_temperature) <= 2.0):
        raise ValueError("openai_temperature must be within 0..2")
    if not (isinstance(s.openai_max_latency_sec, (int, float)) and math.isfinite(float(s.openai_max_latency_sec)) and float(s.openai_max_latency_sec) > 0):
        raise ValueError("openai_max_latency_sec must be finite > 0")

    # TEST controls (TRADE-GRADE) — 운영 사고 방지
    if s.test_bypass_guards and not s.test_dry_run:
        raise RuntimeError("test_bypass_guards is only allowed with test_dry_run=True (STRICT)")
    if s.test_force_enter and not s.test_dry_run:
        raise RuntimeError("test_force_enter is only allowed with test_dry_run=True (STRICT)")
    if float(s.test_fake_available_usdt) > 0 and not s.test_dry_run:
        raise RuntimeError("test_fake_available_usdt is only allowed with test_dry_run=True (STRICT)")
    if float(s.test_fake_available_usdt) < 0:
        raise ValueError("test_fake_available_usdt must be >= 0 (STRICT)")


# -----------------------------------------------------------------------------
# Public loader
# -----------------------------------------------------------------------------
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

    # ✅ allocation_ratio: ALLOCATION_RATIO 우선, 없으면 RISK_PCT(호환)
    alloc_env = _as_float_opt("ALLOCATION_RATIO")
    risk_env = _as_float_opt("RISK_PCT")

    if alloc_env is None and risk_env is None:
        allocation_ratio = 1.0
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

    entry_cooldown_sec = _as_float("ENTRY_COOLDOWN_SEC", 1.0)
    cooldown_after_close = _as_float("COOLDOWN_AFTER_CLOSE", 3.0)

    min_entry_score_for_gpt = _as_float("MIN_ENTRY_SCORE_FOR_GPT", 28)
    gpt_entry_cooldown_sec = _as_float("GPT_ENTRY_COOLDOWN_SEC", 10.0)

    gpt_daily_call_limit = _as_int("GPT_DAILY_CALL_LIMIT", 2000)
    gpt_max_risk_pct = _as_float("GPT_MAX_RISK_PCT", 0.02)
    gpt_min_confidence = _as_float("GPT_MIN_CONFIDENCE", 0.6)
    gpt_reject_if_over_pnl_pct = _as_float("GPT_REJECT_IF_OVER_PNL_PCT", 0.02)

    # OpenAI (SSOT) — env alias 지원(동일 의미 필드 매핑)
    openai_api_key = _as_str("OPENAI_API_KEY", "")
    openai_model = (
        _as_str_opt("OPENAI_MODEL")
        or _as_str_opt("OPENAI_TRADER_MODEL")
        or _as_str_opt("OPENAI_SUPERVISOR_MODEL")
        or ""
    )
    openai_max_tokens = _as_int("OPENAI_MAX_TOKENS", _as_int("OPENAI_TRADER_MAX_TOKENS", 512))
    openai_temperature = _as_float("OPENAI_TEMPERATURE", _as_float("OPENAI_SUPERVISOR_TEMPERATURE", 0.2))
    openai_max_latency_sec = _as_float("OPENAI_MAX_LATENCY_SEC", _as_float("OPENAI_TRADER_MAX_LATENCY", 12.0))

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
    ws_min_kline_buffer = 120
    ws_max_kline_delay_sec = _as_float("WS_MAX_KLINE_DELAY_SEC", 120.0)
    ws_orderbook_max_delay_sec = _as_float("WS_ORDERBOOK_MAX_DELAY_SEC", 10.0)
    ws_log_enabled = _as_bool("WS_LOG_ENABLED", False)
    ws_log_interval_sec = _as_int("WS_LOG_INTERVAL_SEC", 60)
    ws_stale_reset_sec = _as_float("WS_STALE_RESET_SEC", 600.0)

    ws_backfill_tfs = _as_csv_list("WS_BACKFILL_TFS", ["1m", "5m", "15m", "1h", "4h"])
    ws_backfill_limit = 500

    md_store_flush_sec = _as_float("MD_STORE_FLUSH_SEC", 5.0)
    ob_store_interval_sec = _as_float("OB_STORE_INTERVAL_SEC", 5.0)
    md_store_tfs = _as_csv_list("MD_STORE_TFS", ["1m", "5m", "15m"])

    max_spread_pct = _as_float("MAX_SPREAD_PCT", 0.0008)
    max_spread_abs = _as_float("MAX_SPREAD_ABS", 0.0)
    min_entry_volume_ratio = _as_float("MIN_ENTRY_VOLUME_RATIO", 0.3)
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

    # ✅ 누락되었던 env 매핑 보완
    session_jump_mult_asia = _as_float("SESSION_JUMP_MULT_ASIA", 1.0)
    session_jump_mult_eu = _as_float("SESSION_JUMP_MULT_EU", 1.1)
    session_jump_mult_us = _as_float("SESSION_JUMP_MULT_US", 1.2)

    data_health_notify_sec = _as_int("DATA_HEALTH_NOTIFY_SEC", 900)
    signal_analysis_interval_sec = _as_int("SIGNAL_ANALYSIS_INTERVAL_SEC", 60)
    krw_per_usdt = _as_float("KRW_PER_USDT", 1400.0)

    sigterm_grace_sec = _as_int("SIGTERM_GRACE_SEC", 30)
    allow_start_without_leverage_setup = _as_bool("ALLOW_START_WITHOUT_LEVERAGE_SETUP", False)

    # 10점 근접(최소 변경) 운영 파라미터
    async_worker_threads = _as_int("ASYNC_WORKER_THREADS", 1)
    async_worker_queue_size = _as_int("ASYNC_WORKER_QUEUE_SIZE", 2000)
    reconcile_interval_sec = _as_int("RECONCILE_INTERVAL_SEC", 30)
    force_close_on_desync = _as_bool("FORCE_CLOSE_ON_DESYNC", False)
    max_signal_latency_ms = _as_int("MAX_SIGNAL_LATENCY_MS", 200)
    max_exec_latency_ms = 2500

    position_resync_sec = _as_float("POSITION_RESYNC_SEC", 20.0)

    binance_futures_base = _as_str("BINANCE_FUTURES_BASE", "https://fapi.binance.com")
    binance_recv_window = recv_window_ms
    binance_http_timeout_sec = request_timeout_sec

    hard_daily_loss_limit_usdt = _as_float("HARD_DAILY_LOSS_LIMIT_USDT", 0.0)
    hard_consecutive_losses_limit = _as_int("HARD_CONSECUTIVE_LOSSES_LIMIT", 0)
    hard_position_value_pct_cap = _as_float("HARD_POSITION_VALUE_PCT_CAP", 100.0)
    hard_liquidation_distance_pct_min = _as_float("HARD_LIQUIDATION_DISTANCE_PCT_MIN", 0.0)

    slippage_block_pct = _as_float("SLIPPAGE_BLOCK_PCT", 0.3)
    slippage_stop_engine = _as_bool("SLIPPAGE_STOP_ENGINE", False)
    protection_mode_enabled = _as_bool("PROTECTION_MODE_ENABLED", True)

    require_deterministic_client_order_id = _as_bool("REQUIRE_DETERMINISTIC_CLIENT_ORDER_ID", True)
    entry_fill_wait_sec = _as_float("ENTRY_FILL_WAIT_SEC", 2.0)
    max_entry_slippage_pct = _as_float_opt("MAX_ENTRY_SLIPPAGE_PCT")

    # TEST controls (TRADE-GRADE) — settings에서만 읽는다.
    test_dry_run = _as_bool("TEST_DRY_RUN", False)
    test_bypass_guards = _as_bool("TEST_BYPASS_GUARDS", False)
    test_force_enter = _as_bool("TEST_FORCE_ENTER", False)
    test_fake_available_usdt = _as_float("TEST_FAKE_AVAILABLE_USDT", 0.0)

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
        ws_min_kline_buffer=ws_min_kline_buffer,
        ws_max_kline_delay_sec=ws_max_kline_delay_sec,
        ws_orderbook_max_delay_sec=ws_orderbook_max_delay_sec,
        ws_log_enabled=ws_log_enabled,
        ws_log_interval_sec=ws_log_interval_sec,
        ws_stale_reset_sec=ws_stale_reset_sec,
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
        force_close_on_desync=force_close_on_desync,
        max_signal_latency_ms=max_signal_latency_ms,
        max_exec_latency_ms=max_exec_latency_ms,
        position_resync_sec=position_resync_sec,
        binance_futures_base=binance_futures_base,
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
        max_entry_slippage_pct=max_entry_slippage_pct,
        test_dry_run=test_dry_run,
        test_bypass_guards=test_bypass_guards,
        test_force_enter=test_force_enter,
        test_fake_available_usdt=test_fake_available_usdt,
    )

    _validate_settings(s)
    return s


__all__ = [
    "Settings",
    "BotSettings",
    "PatternStrengthSettings",
    "load_settings",
    "KST",
]