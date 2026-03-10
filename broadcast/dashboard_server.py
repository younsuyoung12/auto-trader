"""
========================================================
FILE: broadcast/dashboard_server.py
ROLE:
- Auto-Trader 대시보드 API 서버
- Trades / EntryScore / Events / Decision / Error / Position / Performance / System 분석 API 제공
- Engine Watchdog / Runtime Health / Dashboard WebSocket 엔드포인트 제공
- 초기 무데이터 상태(INIT)와 실제 장애를 구분해 UI에 전달
- 고비용 AI 분석 API에 대한 provider block / route cooldown / in-flight guard를 적용한다

CORE RESPONSIBILITIES:
- 대시보드용 조회/집계 API 제공
- 엔진 runtime 상태를 DB orderbook snapshot freshness 기준으로 판정
- WATCHDOG/DECISION/PERFORMANCE INIT 상태를 대시보드 계약으로 정규화
- 캐시/웹소켓/분석 API를 일관된 구조로 제공
- 외부 AI/시장데이터 provider quota 초과 시 재호출 폭주를 차단한다

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- DB 조회 실패/스키마 불일치/타입 불일치는 즉시 예외
- 민감정보(DB URL/키 등)는 출력 금지
- 잘못된 query parameter 는 즉시 예외
- 조용한 continue / 자동 보정 금지
- runtime/INIT/경고 상태를 명시적으로 분리한다
- WATCHDOG INIT는 엔진 장애로 취급하지 않는다
- AI 분석 API는 exact cache hit 우선, cache miss 시 route cooldown / provider block / in-flight guard를 적용한다
- provider quota 초과 상태는 route 단위가 아니라 provider scope 단위로 차단한다

CHANGE HISTORY:
- 2026-03-11
  1) FIX(ROOT-CAUSE): AI 분석 API에 provider block / route cooldown / in-flight guard 추가
  2) FIX(ROOT-CAUSE): 외부 provider quota 초과 시 동일 provider scope 재호출 폭주 차단
  3) FIX(STRICT): _raise_ai_provider_http_exception 의 잘못된 bare raise 제거
  4) FIX(OBSERVABILITY): AI route reject 사유를 HTTP detail / structured log 로 명시
  5) FIX(ROOT-CAUSE): dashboard health는 DB 최신 orderbook snapshot freshness를 기준 진실원천으로 사용
  6) FIX(ROOT-CAUSE): ws_orderbook_stale watchdog reason은 DB snapshot이 fresh/delayed면 ENGINE_WARNING으로 올리지 않음
  7) ADD(OBSERVABILITY): runtime에 is_warning / warning_reason / freshness_state 추가
  8) FIX(CONTRACT): runtime delayed는 SERVER_STOP이 아니라 SERVER_LIVE로 유지
  9) FIX(OBSERVABILITY): ws-status.orderbook에 stale_ms 추가
  10) FIX(ROOT-CAUSE): WATCHDOG INIT 상태는 engine health에서 경고/치명 사유로 취급하지 않음
  11) FIX(STRICT): recent trades enrich 경로의 None -> 0.0 보정 제거
========================================================
"""

from __future__ import annotations

import hashlib
import logging
import math
import re
import time
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any, Callable, Dict, List, Optional, Tuple

from fastapi import Depends, FastAPI, HTTPException, Query, Request, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import text
from sqlalchemy.orm import Session

from analysis.quant_analyst import QuantAnalyst
from broadcast.dashboard_db import get_db
from broadcast.dashboard_metrics import (
    build_entry_score_hist,
    events_recent,
    events_skip_hourly,
    events_skip_reason_top,
    get_daily_pnl,
    get_recent_entry_scores,
    get_recent_trades,
    get_summary,
)
from broadcast.dashboard_ws import dashboard_ws_endpoint
from events.event_store import record_quant_analysis_event_db
from infra.cache import (
    get_cache_json,
    init_cache_from_settings,
    is_cache_initialized,
    make_cache_key,
    ping_cache,
    set_cache_json,
)
from services.decision_service import get_latest_decision, get_recent_decisions
from services.error_monitor import (
    get_latest_error_event,
    get_recent_error_counts,
    get_recent_error_events,
)
from services.performance_service import (
    get_daily_pnl_series,
    get_drawdown_curve,
    get_equity_curve,
    get_performance_bundle,
    get_performance_summary,
)
from services.position_service import (
    get_latest_position,
    get_open_position,
    get_recent_positions,
)
from services.system_monitor import get_latest_watchdog_snapshot, get_system_status_snapshot
from settings import load_settings

logger = logging.getLogger(__name__)

_ENGINE_RUNTIME_STATUS_SERVER_LIVE = "SERVER_LIVE"
_ENGINE_RUNTIME_STATUS_SERVER_STOP = "SERVER_STOP"
_ENGINE_RUNTIME_SOURCE_DATABASE = "database_orderbook_snapshot"

# DB snapshot freshness 기준
# - 5초 이내: fresh
# - 5초 초과 ~ 120초 이하: delayed (서버는 살아있음, 경고만)
# - 120초 초과: stale (실질적으로 중지/장애)
_ENGINE_RUNTIME_DELAYED_THRESHOLD_MS = 5_000
_ENGINE_RUNTIME_STALE_THRESHOLD_MS = 120_000

_ENGINE_RUNTIME_FRESHNESS_FRESH = "fresh"
_ENGINE_RUNTIME_FRESHNESS_DELAYED = "delayed"
_ENGINE_RUNTIME_FRESHNESS_STALE = "stale"

_WATCHDOG_STATUS_OK = "OK"
_WATCHDOG_STATUS_INIT = "INIT"

# watchdog reason 중 dashboard에서 DB 최신 snapshot으로 정규화 가능한 항목
_DASHBOARD_DB_TRUTH_OVERRIDE_WATCHDOG_REASONS = {
    "ws_orderbook_stale",
}

# =====================================================
# AI analysis guard constants
# =====================================================

_AI_PROVIDER_SCOPE_EXTERNAL_MARKET = "external_market_provider"
_AI_PROVIDER_SCOPE_OPENAI_ANALYSIS = "openai_analysis_provider"

_AI_ROUTE_SCOPE_MARKET_ANALYSIS = "market_analysis_external"
_AI_ROUTE_SCOPE_QUANT_ANALYSIS_EXTERNAL = "quant_analysis_external"
_AI_ROUTE_SCOPE_QUANT_ANALYSIS_INTERNAL = "quant_analysis_internal"

# exact cache hit 전에는 이 간격보다 빠른 cache miss 재호출을 막는다.
# - external provider 사용 경로는 더 보수적으로 제한
# - internal-only quant path 는 상대적으로 짧게 유지
_AI_ROUTE_COOLDOWN_MS_MARKET_ANALYSIS = 30_000
_AI_ROUTE_COOLDOWN_MS_QUANT_ANALYSIS_EXTERNAL = 30_000
_AI_ROUTE_COOLDOWN_MS_QUANT_ANALYSIS_INTERNAL = 10_000

_AI_ANALYSIS_INFLIGHT_RETRY_AFTER_SEC = 2

_AI_GUARD_PREFIX = "ai-guard"
_AI_GUARD_PROVIDER_BLOCK = "provider-block"
_AI_GUARD_ROUTE_COOLDOWN = "route-cooldown"

_AI_INFLIGHT_LOCK = Lock()
_AI_INFLIGHT_KEYS: set[str] = set()

# =====================================================
# FastAPI App
# =====================================================

_SETTINGS = load_settings()
_QUANT_ANALYST = QuantAnalyst()

app = FastAPI(title="Binance Auto Trader Dashboard")
templates = Jinja2Templates(directory="dashboard/templates")

app.mount("/dashboard/static", StaticFiles(directory="dashboard/static"), name="dashboard-static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def _startup_init_cache() -> None:
    init_cache_from_settings(_SETTINGS)
    ping_cache()


# =====================================================
# Strict helpers
# =====================================================

def _require_positive_int(value: Any, name: str, *, max_value: Optional[int] = None) -> int:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if isinstance(value, bool):
        raise RuntimeError(f"{name} must be int, bool not allowed (STRICT)")
    try:
        iv = int(value)
    except Exception as exc:
        raise RuntimeError(f"{name} must be int (STRICT): {exc}") from exc
    if iv <= 0:
        raise RuntimeError(f"{name} must be > 0 (STRICT)")
    if max_value is not None and iv > max_value:
        raise RuntimeError(f"{name} must be <= {max_value} (STRICT)")
    return iv


def _require_nonnegative_int(value: Any, name: str) -> int:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if isinstance(value, bool):
        raise RuntimeError(f"{name} must be int, bool not allowed (STRICT)")
    try:
        iv = int(value)
    except Exception as exc:
        raise RuntimeError(f"{name} must be int (STRICT): {exc}") from exc
    if iv < 0:
        raise RuntimeError(f"{name} must be >= 0 (STRICT)")
    return iv


def _require_nonempty_str(value: Any, name: str) -> str:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if not isinstance(value, str):
        raise RuntimeError(f"{name} must be str (STRICT), got={type(value).__name__}")
    s = value.strip()
    if not s:
        raise RuntimeError(f"{name} must not be empty (STRICT)")
    return s


def _optional_nonempty_str(value: Any, name: str) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError(f"{name} must be str or null (STRICT), got={type(value).__name__}")
    s = value.strip()
    if not s:
        raise RuntimeError(f"{name} must not be blank when provided (STRICT)")
    return s


def _require_dict(value: Any, name: str) -> Dict[str, Any]:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if not isinstance(value, dict):
        raise RuntimeError(f"{name} must be dict (STRICT), got={type(value).__name__}")
    return dict(value)


def _require_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise RuntimeError(f"{name} must be bool (STRICT)")
    return value


def _require_finite_float(value: Any, name: str) -> float:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if isinstance(value, bool):
        raise RuntimeError(f"{name} must be float, bool not allowed (STRICT)")
    try:
        fv = float(value)
    except Exception as exc:
        raise RuntimeError(f"{name} must be float (STRICT): {exc}") from exc
    if not math.isfinite(fv):
        raise RuntimeError(f"{name} must be finite (STRICT): {fv!r}")
    return fv


def _normalize_symbol(symbol: Any) -> str:
    s = _require_nonempty_str(symbol, "symbol").replace("-", "").replace("/", "").upper()
    if not s:
        raise RuntimeError("symbol normalized empty (STRICT)")
    return s


def _normalize_optional_event_type(event_type: Optional[str]) -> Optional[str]:
    if event_type is None:
        return None
    return _require_nonempty_str(event_type, "event_type").upper()


def _cache_token_bool(value: bool) -> str:
    return "1" if bool(value) else "0"


def _datetime_to_epoch_ms_strict(value: Any, name: str) -> int:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if not isinstance(value, datetime):
        raise RuntimeError(f"{name} must be datetime (STRICT), got={type(value).__name__}")
    dt = value
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000.0)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _cached_json(
    *,
    key_parts: Tuple[str, ...],
    builder: Callable[[], Any],
) -> Any:
    if not is_cache_initialized():
        raise RuntimeError("Redis cache is not initialized (STRICT)")

    key = make_cache_key(*key_parts)
    cached = get_cache_json(key)
    if cached is not None:
        return cached

    data = builder()
    set_cache_json(key, data)
    return data


def _require_question(value: Any) -> str:
    q = _require_nonempty_str(value, "question")
    if len(q) > 2000:
        raise RuntimeError("question must be <= 2000 chars (STRICT)")
    return q


def _question_hash(question: str) -> str:
    q = _require_question(question)
    return hashlib.sha256(q.encode("utf-8")).hexdigest()[:24]


def _extract_market_regime_from_analysis_payload(payload: Dict[str, Any]) -> Optional[str]:
    market_cards = payload.get("market_cards")
    if not isinstance(market_cards, dict):
        return None

    internal_market = market_cards.get("internal_market")
    if isinstance(internal_market, dict):
        regime = internal_market.get("market_regime")
        if isinstance(regime, str) and regime.strip():
            return regime.strip()

    external_market = market_cards.get("external_market")
    if isinstance(external_market, dict):
        regime = external_market.get("market_regime")
        if isinstance(regime, str) and regime.strip():
            return regime.strip()

    return None


def _persist_analysis_event(
    *,
    report_type: str,
    payload: Dict[str, Any],
    question: str,
) -> None:
    normalized_report_type = _require_nonempty_str(report_type, "report_type")
    normalized_question = _require_question(question)
    normalized_payload = _require_dict(payload, "payload")

    symbol = _normalize_symbol(normalized_payload.get("symbol"))
    regime = _extract_market_regime_from_analysis_payload(normalized_payload)

    reason = f"{normalized_report_type}:{normalized_question}"
    record_quant_analysis_event_db(
        ts_utc=datetime.now(timezone.utc),
        symbol=symbol,
        report_type=normalized_report_type,
        reason=reason,
        analysis_payload=normalized_payload,
        regime=regime,
        is_test=False,
    )


def _is_alpha_vantage_daily_quota_error(message: str) -> bool:
    text_norm = str(message or "").strip().lower()
    if not text_norm:
        return False

    has_vendor_marker = ("alpha vantage" in text_norm) or ("alphavantage" in text_norm)
    if not has_vendor_marker:
        return False

    quota_markers = (
        "25 requests per day",
        "free api requests more sparingly",
        "please consider spreading out your free api requests more sparingly",
        "alphavantage.co/premium",
        "daily quota",
    )
    return any(marker in text_norm for marker in quota_markers)


def _extract_retry_after_sec_from_message(message: str) -> Optional[int]:
    text = _require_nonempty_str(message, "message")
    match = re.search(r"retry_after_sec\s*[:=]\s*(\d+)", text, flags=re.IGNORECASE)
    if match is None:
        return None
    return _require_positive_int(match.group(1), "retry_after_sec")


def _seconds_until_next_utc_reset() -> int:
    now_utc = datetime.now(timezone.utc)
    next_reset = (now_utc + timedelta(days=1)).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    seconds = int((next_reset - now_utc).total_seconds())
    if seconds <= 0:
        raise RuntimeError("next UTC reset calculation returned non-positive seconds")
    return seconds


def _build_provider_quota_detail(
    *,
    route_name: str,
    provider: str,
    retry_after_sec: int,
    reason: str,
    message: str,
) -> Dict[str, Any]:
    normalized_route_name = _require_nonempty_str(route_name, "route_name")
    normalized_provider = _require_nonempty_str(provider, "provider")
    normalized_retry_after_sec = _require_positive_int(retry_after_sec, "retry_after_sec")
    normalized_reason = _require_nonempty_str(reason, "reason")
    normalized_message = _require_nonempty_str(message, "message")

    retry_at_utc = (
        datetime.now(timezone.utc) + timedelta(seconds=normalized_retry_after_sec)
    ).isoformat().replace("+00:00", "Z")

    return {
        "status": "unavailable",
        "route": normalized_route_name,
        "reason": normalized_reason,
        "provider": normalized_provider,
        "message": normalized_message,
        "retry_after_sec": normalized_retry_after_sec,
        "retry_at_utc": retry_at_utc,
    }


def _raise_ai_provider_http_exception(exc: Exception, *, route_name: str) -> None:
    normalized_route_name = _require_nonempty_str(route_name, "route_name")
    message = str(exc)
    text_norm = message.strip().lower()

    if _is_alpha_vantage_daily_quota_error(message):
        retry_after_sec = _seconds_until_next_utc_reset()
        detail = _build_provider_quota_detail(
            route_name=normalized_route_name,
            provider="Alpha Vantage",
            retry_after_sec=retry_after_sec,
            reason="external_provider_quota_exceeded",
            message=(
                "External market analysis is temporarily unavailable because the external provider "
                "daily quota has been exceeded."
            ),
        )
        logger.warning(
            "AI analysis provider quota exceeded: route=%s provider=%s retry_after_sec=%s",
            normalized_route_name,
            detail["provider"],
            detail["retry_after_sec"],
        )
        raise HTTPException(status_code=503, detail=detail) from exc

    generic_retry_after_sec = _extract_retry_after_sec_from_message(message) if message.strip() else None
    if generic_retry_after_sec is not None and (
        "quota exceeded" in text_norm
        or "rate limit" in text_norm
        or "too many requests" in text_norm
    ):
        provider = "OpenAI" if "openai" in text_norm else "Unknown"
        detail = _build_provider_quota_detail(
            route_name=normalized_route_name,
            provider=provider,
            retry_after_sec=generic_retry_after_sec,
            reason="analysis_provider_rate_limited",
            message="AI analysis is temporarily unavailable because the upstream analysis provider is rate limited.",
        )
        logger.warning(
            "AI analysis provider quota exceeded: route=%s provider=%s retry_after_sec=%s",
            normalized_route_name,
            detail["provider"],
            detail["retry_after_sec"],
        )
        raise HTTPException(status_code=503, detail=detail) from exc

    raise exc


# =====================================================
# AI guard helpers
# =====================================================

def _make_ai_guard_cache_key(*parts: str) -> str:
    if not is_cache_initialized():
        raise RuntimeError("Redis cache is not initialized (STRICT)")
    normalized_parts = tuple(_require_nonempty_str(part, "guard_cache_part") for part in parts)
    return make_cache_key(_AI_GUARD_PREFIX, *normalized_parts)


def _get_ai_guard_state_optional(*parts: str) -> Optional[Dict[str, Any]]:
    key = _make_ai_guard_cache_key(*parts)
    cached = get_cache_json(key)
    if cached is None:
        return None
    return _require_dict(cached, f"ai_guard_state[{key}]")


def _set_ai_guard_state(*, parts: Tuple[str, ...], payload: Dict[str, Any]) -> None:
    key = _make_ai_guard_cache_key(*parts)
    normalized_payload = _require_dict(payload, "ai_guard_payload")
    set_cache_json(key, normalized_payload)


def _record_ai_route_started(
    *,
    route_scope: str,
    route_name: str,
    started_ms: int,
) -> None:
    normalized_route_scope = _require_nonempty_str(route_scope, "route_scope")
    normalized_route_name = _require_nonempty_str(route_name, "route_name")
    normalized_started_ms = _require_positive_int(started_ms, "started_ms")

    _set_ai_guard_state(
        parts=(_AI_GUARD_ROUTE_COOLDOWN, normalized_route_scope),
        payload={
            "route_scope": normalized_route_scope,
            "route": normalized_route_name,
            "last_started_ms": normalized_started_ms,
        },
    )


def _get_ai_route_last_started_ms(route_scope: str) -> Optional[int]:
    normalized_route_scope = _require_nonempty_str(route_scope, "route_scope")
    state = _get_ai_guard_state_optional(_AI_GUARD_ROUTE_COOLDOWN, normalized_route_scope)
    if state is None:
        return None

    state_scope = _require_nonempty_str(state.get("route_scope"), "route_cooldown.route_scope")
    if state_scope != normalized_route_scope:
        raise RuntimeError(
            f"route cooldown scope mismatch (STRICT): expected={normalized_route_scope} got={state_scope}"
        )

    return _require_positive_int(state.get("last_started_ms"), "route_cooldown.last_started_ms")


def _raise_if_ai_route_cooldown_active(
    *,
    route_scope: str,
    route_name: str,
    cooldown_ms: int,
) -> None:
    normalized_route_scope = _require_nonempty_str(route_scope, "route_scope")
    normalized_route_name = _require_nonempty_str(route_name, "route_name")
    normalized_cooldown_ms = _require_positive_int(cooldown_ms, "cooldown_ms")
    now_ms = _now_ms()

    last_started_ms = _get_ai_route_last_started_ms(normalized_route_scope)
    if last_started_ms is None:
        return

    elapsed_ms = now_ms - last_started_ms
    if elapsed_ms < 0:
        raise RuntimeError("route cooldown elapsed_ms must be >= 0 (STRICT)")

    if elapsed_ms >= normalized_cooldown_ms:
        return

    remaining_ms = normalized_cooldown_ms - elapsed_ms
    if remaining_ms <= 0:
        raise RuntimeError("route cooldown remaining_ms must be > 0 (STRICT)")

    retry_after_sec = max(1, int(math.ceil(remaining_ms / 1000.0)))
    logger.warning(
        "AI route cooldown active: route=%s route_scope=%s cooldown_ms=%s retry_after_sec=%s",
        normalized_route_name,
        normalized_route_scope,
        normalized_cooldown_ms,
        retry_after_sec,
    )
    raise HTTPException(
        status_code=429,
        detail={
            "status": "rejected",
            "route": normalized_route_name,
            "reason": "analysis_route_cooldown_active",
            "message": "AI analysis request was rejected because the route cooldown window is still active.",
            "retry_after_sec": retry_after_sec,
        },
    )


def _require_http_exception_detail_dict(exc: HTTPException) -> Dict[str, Any]:
    return _require_dict(exc.detail, "http_exception.detail")


def _record_provider_block_from_http_exception(
    *,
    exc: HTTPException,
    provider_scope: str,
    route_name: str,
) -> None:
    normalized_provider_scope = _require_nonempty_str(provider_scope, "provider_scope")
    normalized_route_name = _require_nonempty_str(route_name, "route_name")
    detail = _require_http_exception_detail_dict(exc)

    reason = _require_nonempty_str(detail.get("reason"), "provider_block.reason")
    provider = _require_nonempty_str(detail.get("provider"), "provider_block.provider")
    retry_after_sec = _require_positive_int(detail.get("retry_after_sec"), "provider_block.retry_after_sec")
    blocked_until_ms = _now_ms() + (retry_after_sec * 1000)

    _set_ai_guard_state(
        parts=(_AI_GUARD_PROVIDER_BLOCK, normalized_provider_scope),
        payload={
            "provider_scope": normalized_provider_scope,
            "route": normalized_route_name,
            "provider": provider,
            "reason": reason,
            "retry_after_sec": retry_after_sec,
            "blocked_until_ms": blocked_until_ms,
            "recorded_at_ms": _now_ms(),
        },
    )

    logger.warning(
        "AI provider block recorded: route=%s provider_scope=%s provider=%s retry_after_sec=%s reason=%s",
        normalized_route_name,
        normalized_provider_scope,
        provider,
        retry_after_sec,
        reason,
    )


def _get_active_provider_block_state(provider_scope: str) -> Optional[Dict[str, Any]]:
    normalized_provider_scope = _require_nonempty_str(provider_scope, "provider_scope")
    state = _get_ai_guard_state_optional(_AI_GUARD_PROVIDER_BLOCK, normalized_provider_scope)
    if state is None:
        return None

    state_scope = _require_nonempty_str(state.get("provider_scope"), "provider_block.provider_scope")
    if state_scope != normalized_provider_scope:
        raise RuntimeError(
            f"provider block scope mismatch (STRICT): expected={normalized_provider_scope} got={state_scope}"
        )

    blocked_until_ms = _require_positive_int(state.get("blocked_until_ms"), "provider_block.blocked_until_ms")
    now_ms = _now_ms()
    if blocked_until_ms <= now_ms:
        return None

    _require_nonempty_str(state.get("provider"), "provider_block.provider")
    _require_nonempty_str(state.get("reason"), "provider_block.reason")
    _require_positive_int(state.get("retry_after_sec"), "provider_block.retry_after_sec")
    _require_nonempty_str(state.get("route"), "provider_block.route")
    return state


def _raise_if_provider_blocked(
    *,
    provider_scope: Optional[str],
    route_name: str,
) -> None:
    if provider_scope is None:
        return

    normalized_route_name = _require_nonempty_str(route_name, "route_name")
    normalized_provider_scope = _require_nonempty_str(provider_scope, "provider_scope")

    state = _get_active_provider_block_state(normalized_provider_scope)
    if state is None:
        return

    blocked_until_ms = _require_positive_int(state.get("blocked_until_ms"), "provider_block.blocked_until_ms")
    now_ms = _now_ms()
    remaining_ms = blocked_until_ms - now_ms
    if remaining_ms <= 0:
        return

    retry_after_sec = max(1, int(math.ceil(remaining_ms / 1000.0)))
    provider = _require_nonempty_str(state.get("provider"), "provider_block.provider")
    reason = _require_nonempty_str(state.get("reason"), "provider_block.reason")

    logger.warning(
        "AI provider block active: route=%s provider_scope=%s provider=%s retry_after_sec=%s reason=%s",
        normalized_route_name,
        normalized_provider_scope,
        provider,
        retry_after_sec,
        reason,
    )
    raise HTTPException(
        status_code=503,
        detail={
            "status": "unavailable",
            "route": normalized_route_name,
            "reason": reason,
            "provider": provider,
            "message": "AI analysis is temporarily unavailable because the upstream provider is still blocked.",
            "retry_after_sec": retry_after_sec,
            "retry_at_utc": (
                datetime.now(timezone.utc) + timedelta(seconds=retry_after_sec)
            ).isoformat().replace("+00:00", "Z"),
        },
    )


def _is_provider_quota_http_exception(exc: HTTPException) -> bool:
    detail = _require_http_exception_detail_dict(exc)
    if exc.status_code != 503:
        return False

    reason = _require_nonempty_str(detail.get("reason"), "provider_quota.reason")
    retry_after_sec = detail.get("retry_after_sec")
    provider = detail.get("provider")

    if retry_after_sec is None:
        return False
    _require_positive_int(retry_after_sec, "provider_quota.retry_after_sec")
    _require_nonempty_str(provider, "provider_quota.provider")

    return reason in {
        "external_provider_quota_exceeded",
        "analysis_provider_rate_limited",
    }


def _acquire_ai_inflight_key_or_raise(
    *,
    request_key: str,
    route_name: str,
) -> None:
    normalized_request_key = _require_nonempty_str(request_key, "request_key")
    normalized_route_name = _require_nonempty_str(route_name, "route_name")

    with _AI_INFLIGHT_LOCK:
        if normalized_request_key in _AI_INFLIGHT_KEYS:
            logger.warning(
                "AI analysis request rejected because same request is already in-flight: route=%s request_key=%s",
                normalized_route_name,
                normalized_request_key,
            )
            raise HTTPException(
                status_code=429,
                detail={
                    "status": "rejected",
                    "route": normalized_route_name,
                    "reason": "analysis_request_in_progress",
                    "message": "The same AI analysis request is already in progress.",
                    "retry_after_sec": _AI_ANALYSIS_INFLIGHT_RETRY_AFTER_SEC,
                },
            )
        _AI_INFLIGHT_KEYS.add(normalized_request_key)


def _release_ai_inflight_key(request_key: str) -> None:
    normalized_request_key = _require_nonempty_str(request_key, "request_key")
    with _AI_INFLIGHT_LOCK:
        if normalized_request_key not in _AI_INFLIGHT_KEYS:
            logger.error(
                "AI in-flight key release mismatch: request_key=%s was not registered",
                normalized_request_key,
            )
            return
        _AI_INFLIGHT_KEYS.remove(normalized_request_key)


def _guarded_ai_cached_json(
    *,
    key_parts: Tuple[str, ...],
    route_name: str,
    route_scope: str,
    cooldown_ms: int,
    provider_scope: Optional[str],
    builder: Callable[[], Dict[str, Any]],
) -> Dict[str, Any]:
    if not is_cache_initialized():
        raise RuntimeError("Redis cache is not initialized (STRICT)")

    normalized_route_name = _require_nonempty_str(route_name, "route_name")
    normalized_route_scope = _require_nonempty_str(route_scope, "route_scope")
    normalized_cooldown_ms = _require_positive_int(cooldown_ms, "cooldown_ms")

    payload_cache_key = make_cache_key(*key_parts)
    cached = get_cache_json(payload_cache_key)
    if cached is not None:
        return _require_dict(cached, f"ai_payload_cache[{payload_cache_key}]")

    _raise_if_provider_blocked(
        provider_scope=provider_scope,
        route_name=normalized_route_name,
    )
    _raise_if_ai_route_cooldown_active(
        route_scope=normalized_route_scope,
        route_name=normalized_route_name,
        cooldown_ms=normalized_cooldown_ms,
    )

    _acquire_ai_inflight_key_or_raise(
        request_key=payload_cache_key,
        route_name=normalized_route_name,
    )
    try:
        cached_retry = get_cache_json(payload_cache_key)
        if cached_retry is not None:
            return _require_dict(cached_retry, f"ai_payload_cache[{payload_cache_key}]")

        _raise_if_provider_blocked(
            provider_scope=provider_scope,
            route_name=normalized_route_name,
        )
        _raise_if_ai_route_cooldown_active(
            route_scope=normalized_route_scope,
            route_name=normalized_route_name,
            cooldown_ms=normalized_cooldown_ms,
        )

        _record_ai_route_started(
            route_scope=normalized_route_scope,
            route_name=normalized_route_name,
            started_ms=_now_ms(),
        )

        data = _require_dict(builder(), "ai_builder.payload")
        set_cache_json(payload_cache_key, data)
        return data

    except HTTPException as exc:
        if provider_scope is not None and _is_provider_quota_http_exception(exc):
            _record_provider_block_from_http_exception(
                exc=exc,
                provider_scope=provider_scope,
                route_name=normalized_route_name,
            )
        raise

    finally:
        _release_ai_inflight_key(payload_cache_key)


# =====================================================
# Pages
# =====================================================

@app.get("/", response_class=HTMLResponse)
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("dashboard.html", {"request": request})


# =====================================================
# WebSocket
# =====================================================

@app.websocket("/ws/dashboard")
async def ws_dashboard(websocket: WebSocket) -> None:
    await dashboard_ws_endpoint(websocket)


# =====================================================
# Base health
# =====================================================

def _db_latency_ms(db: Session) -> int:
    t0 = time.perf_counter()
    result = db.execute(text("SELECT 1")).scalar()
    if result != 1:
        raise RuntimeError("SELECT 1 failed (STRICT)")
    latency_ms = int((time.perf_counter() - t0) * 1000.0)
    if latency_ms < 0:
        raise RuntimeError("db latency must be >= 0 (STRICT)")
    return latency_ms


@app.get("/healthz")
def health_check(db: Session = Depends(get_db)) -> Dict[str, Any]:
    return {
        "status": "ok",
        "db_latency_ms": _db_latency_ms(db),
        "cache_ok": ping_cache(),
    }


# =====================================================
# AI Trading Intelligence
# =====================================================

@app.get("/api/quant-analysis")
def api_quant_analysis(
    question: str = Query(...),
    include_external_market: bool = Query(default=True),
) -> Dict[str, Any]:
    normalized_question = _require_question(question)
    include_external_market_b = bool(include_external_market)

    route_scope = (
        _AI_ROUTE_SCOPE_QUANT_ANALYSIS_EXTERNAL
        if include_external_market_b
        else _AI_ROUTE_SCOPE_QUANT_ANALYSIS_INTERNAL
    )
    cooldown_ms = (
        _AI_ROUTE_COOLDOWN_MS_QUANT_ANALYSIS_EXTERNAL
        if include_external_market_b
        else _AI_ROUTE_COOLDOWN_MS_QUANT_ANALYSIS_INTERNAL
    )
    provider_scope = (
        _AI_PROVIDER_SCOPE_EXTERNAL_MARKET
        if include_external_market_b
        else None
    )

    def _build() -> Dict[str, Any]:
        try:
            result = _QUANT_ANALYST.analyze(
                question=normalized_question,
                include_external_market=include_external_market_b,
            )
        except Exception as exc:
            _raise_ai_provider_http_exception(exc, route_name="/api/quant-analysis")

        payload = _require_dict(result.dashboard_payload, "quant_analysis.dashboard_payload")
        _persist_analysis_event(
            report_type="dashboard_query",
            payload=payload,
            question=normalized_question,
        )
        return payload

    return _guarded_ai_cached_json(
        key_parts=(
            "ai",
            "quant-analysis",
            f"q-{_question_hash(normalized_question)}",
            f"ext-{_cache_token_bool(include_external_market_b)}",
        ),
        route_name="/api/quant-analysis",
        route_scope=route_scope,
        cooldown_ms=cooldown_ms,
        provider_scope=provider_scope,
        builder=_build,
    )


@app.get("/api/market-analysis")
def api_market_analysis(
    question: str = Query(...),
) -> Dict[str, Any]:
    normalized_question = _require_question(question)

    def _build() -> Dict[str, Any]:
        try:
            result = _QUANT_ANALYST.analyze_market_only(
                question=normalized_question,
            )
        except Exception as exc:
            _raise_ai_provider_http_exception(exc, route_name="/api/market-analysis")

        payload = _require_dict(result.dashboard_payload, "market_analysis.dashboard_payload")
        _persist_analysis_event(
            report_type="market_query",
            payload=payload,
            question=normalized_question,
        )
        return payload

    return _guarded_ai_cached_json(
        key_parts=(
            "ai",
            "market-analysis",
            f"q-{_question_hash(normalized_question)}",
        ),
        route_name="/api/market-analysis",
        route_scope=_AI_ROUTE_SCOPE_MARKET_ANALYSIS,
        cooldown_ms=_AI_ROUTE_COOLDOWN_MS_MARKET_ANALYSIS,
        provider_scope=_AI_PROVIDER_SCOPE_EXTERNAL_MARKET,
        builder=_build,
    )


# =====================================================
# Summary / PnL (legacy compatibility)
# =====================================================

@app.get("/api/summary")
def api_summary(db: Session = Depends(get_db)) -> Dict[str, Any]:
    return _cached_json(
        key_parts=("dashboard", "summary"),
        builder=lambda: get_summary(db),
    )


@app.get("/api/daily-pnl")
def api_daily_pnl(
    days: int = Query(default=30, ge=1, le=365),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    normalized_days = _require_positive_int(days, "days", max_value=365)
    return _cached_json(
        key_parts=("dashboard", "daily-pnl", f"days-{normalized_days}"),
        builder=lambda: {"days": normalized_days, "items": get_daily_pnl(db, days=normalized_days)},
    )


# =====================================================
# Trade label helpers
# =====================================================

def _map_trade_type(is_auto: bool) -> str:
    return "자동" if is_auto else "수동"


def _map_regime_label(strategy: Optional[str]) -> str:
    if not strategy:
        return "기타"
    s = strategy.upper()
    if "RANGE" in s:
        return "박스장"
    if "TREND" in s:
        return "추세장"
    if "HYBRID" in s:
        return "혼합"
    return "기타"


def _map_side_label(side: Optional[str]) -> str:
    if not side:
        return ""
    s = side.upper()
    if s in ("LONG", "BUY"):
        return "롱"
    if s in ("SHORT", "SELL"):
        return "숏"
    return s


def _map_close_reason_label(reason: Optional[str]) -> str:
    if not reason:
        return "기타"
    r = reason.lower()
    if "tp" in r and "early" not in r:
        return "익절"
    if "sl" in r and "early" not in r:
        return "손절"
    if "manual" in r:
        return "수동 청산"
    return reason


# =====================================================
# Trades
# =====================================================

@app.get("/api/trades/recent")
def api_recent_trades(
    limit: int = Query(default=50, ge=1, le=500),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    normalized_limit = _require_positive_int(limit, "limit", max_value=500)

    def _build() -> Dict[str, Any]:
        trades: List[Dict[str, Any]] = get_recent_trades(db, limit=normalized_limit)

        enriched: List[Dict[str, Any]] = []
        for idx, t in enumerate(trades):
            if not isinstance(t, dict):
                raise RuntimeError(f"recent trade item must be dict (STRICT), idx={idx}")

            pnl = _require_finite_float(t.get("pnl_usdt"), f"recent_trades[{idx}].pnl_usdt")
            is_auto = _require_bool(t.get("is_auto"), f"recent_trades[{idx}].is_auto")
            strategy = t.get("strategy")
            side = t.get("side")
            close_reason = t.get("close_reason")

            item = dict(t)
            item.update(
                {
                    "trade_type": _map_trade_type(is_auto),
                    "regime_label": _map_regime_label(strategy),
                    "side_label": _map_side_label(side),
                    "close_reason_label": _map_close_reason_label(close_reason),
                    "is_profit": pnl > 0.0,
                    "is_loss": pnl < 0.0,
                    "is_breakeven": pnl == 0.0,
                }
            )
            enriched.append(item)

        return {"limit": normalized_limit, "items": enriched}

    return _cached_json(
        key_parts=("trades", "recent", f"limit-{normalized_limit}"),
        builder=_build,
    )


# =====================================================
# Entry scores
# =====================================================

@app.get("/api/entry-scores/recent")
def api_recent_entry_scores(
    limit: int = Query(default=300, ge=1, le=1000),
    include_test: bool = False,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    normalized_limit = _require_positive_int(limit, "limit", max_value=1000)
    include_test_b = bool(include_test)

    def _build() -> Dict[str, Any]:
        scores = get_recent_entry_scores(db, limit=normalized_limit, include_test=include_test_b)
        labels, counts = build_entry_score_hist(scores)
        return {
            "limit": normalized_limit,
            "include_test": include_test_b,
            "items": scores,
            "hist_labels": labels,
            "hist_counts": counts,
        }

    return _cached_json(
        key_parts=(
            "entry-scores",
            "recent",
            f"limit-{normalized_limit}",
            f"test-{_cache_token_bool(include_test_b)}",
        ),
        builder=_build,
    )


# =====================================================
# Events
# =====================================================

@app.get("/api/events/skip-reasons")
def api_skip_reasons(
    days: int = Query(default=7, ge=1, le=365),
    limit: int = Query(default=15, ge=1, le=100),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    normalized_days = _require_positive_int(days, "days", max_value=365)
    normalized_limit = _require_positive_int(limit, "limit", max_value=100)
    return _cached_json(
        key_parts=(
            "events",
            "skip-reasons",
            f"days-{normalized_days}",
            f"limit-{normalized_limit}",
        ),
        builder=lambda: {
            "days": normalized_days,
            "limit": normalized_limit,
            "items": events_skip_reason_top(db, days=normalized_days, limit=normalized_limit),
        },
    )


@app.get("/api/events/skip-hourly")
def api_skip_hourly(
    days: int = Query(default=7, ge=1, le=365),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    normalized_days = _require_positive_int(days, "days", max_value=365)
    return _cached_json(
        key_parts=("events", "skip-hourly", f"days-{normalized_days}"),
        builder=lambda: {"days": normalized_days, "items": events_skip_hourly(db, days=normalized_days)},
    )


@app.get("/api/events/recent")
def api_events_recent(
    limit: int = Query(default=200, ge=1, le=1000),
    event_type: Optional[str] = None,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    normalized_limit = _require_positive_int(limit, "limit", max_value=1000)
    normalized_event_type = _normalize_optional_event_type(event_type)
    event_type_part = normalized_event_type if normalized_event_type is not None else "all"
    return _cached_json(
        key_parts=(
            "events",
            "recent",
            f"limit-{normalized_limit}",
            f"type-{event_type_part}",
        ),
        builder=lambda: {
            "limit": normalized_limit,
            "event_type": normalized_event_type,
            "items": events_recent(db, limit=normalized_limit, event_type=normalized_event_type),
        },
    )


# =====================================================
# Decision
# =====================================================

@app.get("/api/decision/latest")
def api_decision_latest(
    include_test: bool = False,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    include_test_b = bool(include_test)
    return _cached_json(
        key_parts=("decision", "latest", f"test-{_cache_token_bool(include_test_b)}"),
        builder=lambda: get_latest_decision(db, include_test=include_test_b),
    )


@app.get("/api/decision/recent")
def api_decision_recent(
    limit: int = Query(default=50, ge=1, le=500),
    include_test: bool = False,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    normalized_limit = _require_positive_int(limit, "limit", max_value=500)
    include_test_b = bool(include_test)
    return _cached_json(
        key_parts=(
            "decision",
            "recent",
            f"limit-{normalized_limit}",
            f"test-{_cache_token_bool(include_test_b)}",
        ),
        builder=lambda: {
            "limit": normalized_limit,
            "include_test": include_test_b,
            "items": get_recent_decisions(db, limit=normalized_limit, include_test=include_test_b),
        },
    )


# =====================================================
# Error monitor
# =====================================================

@app.get("/api/errors/latest")
def api_errors_latest(
    event_type: Optional[str] = None,
    include_test: bool = False,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    normalized_event_type = _normalize_optional_event_type(event_type)
    event_type_part = normalized_event_type if normalized_event_type is not None else "all"
    include_test_b = bool(include_test)
    return _cached_json(
        key_parts=(
            "errors",
            "latest",
            f"type-{event_type_part}",
            f"test-{_cache_token_bool(include_test_b)}",
        ),
        builder=lambda: get_latest_error_event(
            db,
            event_type=normalized_event_type,
            include_test=include_test_b,
        ),
    )


@app.get("/api/errors/recent")
def api_errors_recent(
    limit: int = Query(default=100, ge=1, le=1000),
    event_type: Optional[str] = None,
    include_test: bool = False,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    normalized_limit = _require_positive_int(limit, "limit", max_value=1000)
    normalized_event_type = _normalize_optional_event_type(event_type)
    event_type_part = normalized_event_type if normalized_event_type is not None else "all"
    include_test_b = bool(include_test)
    return _cached_json(
        key_parts=(
            "errors",
            "recent",
            f"limit-{normalized_limit}",
            f"type-{event_type_part}",
            f"test-{_cache_token_bool(include_test_b)}",
        ),
        builder=lambda: {
            "limit": normalized_limit,
            "event_type": normalized_event_type,
            "include_test": include_test_b,
            "items": get_recent_error_events(
                db,
                limit=normalized_limit,
                event_type=normalized_event_type,
                include_test=include_test_b,
            ),
        },
    )


@app.get("/api/errors/counts")
def api_errors_counts(
    minutes: int = Query(default=10, ge=1, le=1440),
    include_test: bool = False,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    normalized_minutes = _require_positive_int(minutes, "minutes", max_value=1440)
    include_test_b = bool(include_test)
    return _cached_json(
        key_parts=(
            "errors",
            "counts",
            f"minutes-{normalized_minutes}",
            f"test-{_cache_token_bool(include_test_b)}",
        ),
        builder=lambda: {
            "minutes": normalized_minutes,
            "include_test": include_test_b,
            "items": get_recent_error_counts(
                db,
                minutes=normalized_minutes,
                include_test=include_test_b,
            ),
        },
    )


# =====================================================
# Position
# =====================================================

@app.get("/api/position/current")
def api_position_current(
    include_test: bool = False,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    include_test_b = bool(include_test)
    return _cached_json(
        key_parts=("position", "current", f"test-{_cache_token_bool(include_test_b)}"),
        builder=lambda: get_open_position(db, include_test=include_test_b),
    )


@app.get("/api/position/latest")
def api_position_latest(
    include_test: bool = False,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    include_test_b = bool(include_test)
    return _cached_json(
        key_parts=("position", "latest", f"test-{_cache_token_bool(include_test_b)}"),
        builder=lambda: get_latest_position(db, include_test=include_test_b),
    )


@app.get("/api/position/recent")
def api_position_recent(
    limit: int = Query(default=50, ge=1, le=500),
    include_test: bool = False,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    normalized_limit = _require_positive_int(limit, "limit", max_value=500)
    include_test_b = bool(include_test)
    return _cached_json(
        key_parts=(
            "position",
            "recent",
            f"limit-{normalized_limit}",
            f"test-{_cache_token_bool(include_test_b)}",
        ),
        builder=lambda: {
            "limit": normalized_limit,
            "include_test": include_test_b,
            "items": get_recent_positions(db, limit=normalized_limit, include_test=include_test_b),
        },
    )


# =====================================================
# Performance
# =====================================================

@app.get("/api/performance/summary")
def api_performance_summary(db: Session = Depends(get_db)) -> Dict[str, Any]:
    return _cached_json(
        key_parts=("performance", "summary"),
        builder=lambda: get_performance_summary(db),
    )


@app.get("/api/performance/equity-curve")
def api_performance_equity_curve(
    limit: int = Query(default=1000, ge=1, le=5000),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    normalized_limit = _require_positive_int(limit, "limit", max_value=5000)
    return _cached_json(
        key_parts=("performance", "equity-curve", f"limit-{normalized_limit}"),
        builder=lambda: {"limit": normalized_limit, "items": get_equity_curve(db, limit=normalized_limit)},
    )


@app.get("/api/performance/drawdown-curve")
def api_performance_drawdown_curve(
    limit: int = Query(default=1000, ge=1, le=5000),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    normalized_limit = _require_positive_int(limit, "limit", max_value=5000)
    return _cached_json(
        key_parts=("performance", "drawdown-curve", f"limit-{normalized_limit}"),
        builder=lambda: {"limit": normalized_limit, "items": get_drawdown_curve(db, limit=normalized_limit)},
    )


@app.get("/api/performance/daily-pnl")
def api_performance_daily_pnl(
    days: int = Query(default=30, ge=1, le=365),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    normalized_days = _require_positive_int(days, "days", max_value=365)
    return _cached_json(
        key_parts=("performance", "daily-pnl", f"days-{normalized_days}"),
        builder=lambda: {"days": normalized_days, "items": get_daily_pnl_series(db, days=normalized_days)},
    )


@app.get("/api/performance/bundle")
def api_performance_bundle(
    curve_limit: int = Query(default=1000, ge=1, le=5000),
    daily_days: int = Query(default=30, ge=1, le=365),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    normalized_curve_limit = _require_positive_int(curve_limit, "curve_limit", max_value=5000)
    normalized_daily_days = _require_positive_int(daily_days, "daily_days", max_value=365)
    return _cached_json(
        key_parts=(
            "performance",
            "bundle",
            f"curve-{normalized_curve_limit}",
            f"days-{normalized_daily_days}",
        ),
        builder=lambda: get_performance_bundle(
            db,
            curve_limit=normalized_curve_limit,
            daily_days=normalized_daily_days,
        ),
    )


# =====================================================
# System monitor
# =====================================================

@app.get("/api/system/status")
def api_system_status(
    include_test: bool = False,
    error_window_minutes: int = Query(default=10, ge=1, le=1440),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    normalized_minutes = _require_positive_int(error_window_minutes, "error_window_minutes", max_value=1440)
    include_test_b = bool(include_test)
    return _cached_json(
        key_parts=(
            "system",
            "status",
            f"test-{_cache_token_bool(include_test_b)}",
            f"minutes-{normalized_minutes}",
        ),
        builder=lambda: get_system_status_snapshot(
            db,
            include_test=include_test_b,
            error_window_minutes=normalized_minutes,
        ),
    )


# =====================================================
# Engine Watchdog / Health
# =====================================================

def _engine_status_from_signals(
    *,
    db_latency_ms: int,
    recent_errors: int,
    recent_skips: int,
    latest_watchdog_status: str,
    latest_watchdog_reason: str,
) -> Tuple[str, List[str]]:
    reasons: List[str] = []

    if db_latency_ms >= 3000:
        reasons.append(f"db_latency_ms_high:{db_latency_ms}")
    if recent_errors >= 3:
        reasons.append(f"recent_errors_high:{recent_errors}")
    if recent_skips >= 20:
        reasons.append(f"recent_skips_high:{recent_skips}")

    watchdog_status = _require_nonempty_str(latest_watchdog_status, "latest_watchdog_status")
    watchdog_reason = _require_nonempty_str(latest_watchdog_reason, "latest_watchdog_reason")

    if watchdog_status not in {_WATCHDOG_STATUS_OK, _WATCHDOG_STATUS_INIT}:
        raise RuntimeError(f"unsupported latest_watchdog_status (STRICT): {watchdog_status!r}")

    # INIT 상태는 대시보드 초기 상태이지 장애가 아니다.
    if watchdog_status == _WATCHDOG_STATUS_OK and watchdog_reason != "ok":
        reasons.append(f"watchdog:{watchdog_reason}")

    fatal_watchdog_reasons = {"db_lag", "watchdog_internal_error"}
    warning_watchdog_reasons = {
        "ws_kline_stale",
        "ws_orderbook_stale",
        "orderbook_integrity_fail",
        "kline_rollback",
    }

    if db_latency_ms >= 3000 or recent_errors >= 3:
        return "ENGINE_FATAL", reasons

    if watchdog_status == _WATCHDOG_STATUS_OK and watchdog_reason in fatal_watchdog_reasons:
        return "ENGINE_FATAL", reasons

    if db_latency_ms >= 1200 or recent_skips >= 20:
        return "ENGINE_WARNING", reasons

    if watchdog_status == _WATCHDOG_STATUS_OK and watchdog_reason in warning_watchdog_reasons:
        return "ENGINE_WARNING", reasons

    return "ENGINE_OK", reasons


def _count_recent_events_strict(db: Session, *, minutes: int, event_type: str) -> int:
    normalized_minutes = _require_positive_int(minutes, "minutes", max_value=1440)
    normalized_event_type = _require_nonempty_str(event_type, "event_type").upper()

    sql = text(
        """
        SELECT COUNT(*) AS n
        FROM bt_events
        WHERE event_type = :event_type
          AND ts_utc >= now() - (:minutes * INTERVAL '1 minute')
          AND is_test = FALSE
        """
    )
    row = db.execute(
        sql,
        {
            "event_type": normalized_event_type,
            "minutes": normalized_minutes,
        },
    ).mappings().one_or_none()

    if row is None:
        raise RuntimeError("recent event count row not found (STRICT)")

    n = row.get("n")
    if n is None:
        raise RuntimeError("recent event count is required (STRICT)")
    if isinstance(n, bool):
        raise RuntimeError("recent event count must be int (STRICT)")
    try:
        iv = int(n)
    except Exception as exc:
        raise RuntimeError(f"recent event count must be int (STRICT): {exc}") from exc
    if iv < 0:
        raise RuntimeError("recent event count must be >= 0 (STRICT)")
    return iv


def _fetch_recent_candle_buffer_len_strict(
    db: Session,
    *,
    symbol: str,
    timeframe: str,
    row_limit: int,
) -> int:
    normalized_symbol = _normalize_symbol(symbol)
    normalized_tf = _require_nonempty_str(timeframe, "timeframe")
    normalized_limit = _require_positive_int(row_limit, "row_limit", max_value=100000)

    sql = text(
        """
        SELECT COUNT(*) AS n
        FROM (
            SELECT ts
            FROM bt_candles
            WHERE symbol = :symbol
              AND timeframe = :timeframe
            ORDER BY ts DESC
            LIMIT :row_limit
        ) q
        """
    )
    row = db.execute(
        sql,
        {
            "symbol": normalized_symbol,
            "timeframe": normalized_tf,
            "row_limit": normalized_limit,
        },
    ).mappings().one_or_none()

    if row is None:
        raise RuntimeError("recent candle buffer row not found (STRICT)")

    n = row.get("n")
    if n is None:
        raise RuntimeError("recent candle buffer len is required (STRICT)")
    if isinstance(n, bool):
        raise RuntimeError("recent candle buffer len must be int (STRICT)")
    try:
        iv = int(n)
    except Exception as exc:
        raise RuntimeError(f"recent candle buffer len must be int (STRICT): {exc}") from exc
    if iv < 0:
        raise RuntimeError("recent candle buffer len must be >= 0 (STRICT)")
    return iv


def _fetch_latest_orderbook_snapshot_strict(
    db: Session,
    *,
    symbol: str,
) -> Optional[Dict[str, Any]]:
    normalized_symbol = _normalize_symbol(symbol)

    sql = text(
        """
        SELECT
            symbol,
            ts,
            best_bid,
            best_ask,
            bids_raw,
            asks_raw
        FROM bt_orderbook_snapshots
        WHERE symbol = :symbol
        ORDER BY ts DESC
        LIMIT 1
        """
    )
    row = db.execute(sql, {"symbol": normalized_symbol}).mappings().one_or_none()
    if row is None:
        return None

    ts_value = row.get("ts")
    ts_ms = _datetime_to_epoch_ms_strict(ts_value, "orderbook.ts")

    bids_raw = row.get("bids_raw")
    asks_raw = row.get("asks_raw")

    bids_list = bids_raw if isinstance(bids_raw, list) else None
    asks_list = asks_raw if isinstance(asks_raw, list) else None

    best_bid = row.get("best_bid")
    best_ask = row.get("best_ask")

    return {
        "symbol": normalized_symbol,
        "ts": ts_ms,
        "bestBid": None if best_bid is None else _require_finite_float(best_bid, "orderbook.best_bid"),
        "bestAsk": None if best_ask is None else _require_finite_float(best_ask, "orderbook.best_ask"),
        "bids_len": len(bids_list) if bids_list is not None else None,
        "asks_len": len(asks_list) if asks_list is not None else None,
        "has_bids": bool(bids_list),
        "has_asks": bool(asks_list),
    }


def _build_engine_runtime_status(
    *,
    orderbook: Optional[Dict[str, Any]],
    now_ms: int,
    delayed_threshold_ms: int = _ENGINE_RUNTIME_DELAYED_THRESHOLD_MS,
    stale_threshold_ms: int = _ENGINE_RUNTIME_STALE_THRESHOLD_MS,
) -> Dict[str, Any]:
    normalized_now_ms = _require_positive_int(now_ms, "now_ms")
    normalized_delayed_threshold_ms = _require_positive_int(
        delayed_threshold_ms,
        "delayed_threshold_ms",
    )
    normalized_stale_threshold_ms = _require_positive_int(
        stale_threshold_ms,
        "stale_threshold_ms",
    )

    if normalized_delayed_threshold_ms >= normalized_stale_threshold_ms:
        raise RuntimeError("delayed_threshold_ms must be < stale_threshold_ms (STRICT)")

    if orderbook is None:
        return {
            "status": _ENGINE_RUNTIME_STATUS_SERVER_STOP,
            "reason": "orderbook_snapshot_not_found",
            "source": _ENGINE_RUNTIME_SOURCE_DATABASE,
            "freshness_state": _ENGINE_RUNTIME_FRESHNESS_STALE,
            "delayed_threshold_ms": normalized_delayed_threshold_ms,
            "threshold_ms": normalized_stale_threshold_ms,
            "snapshot_ts_ms": None,
            "stale_ms": None,
            "is_warning": False,
            "warning_reason": None,
        }

    normalized_orderbook = _require_dict(orderbook, "orderbook")
    orderbook_ok_raw = normalized_orderbook.get("ok")
    orderbook_ok = _require_bool(orderbook_ok_raw, "orderbook.ok")

    if not orderbook_ok:
        error_reason = normalized_orderbook.get("error")
        if error_reason is None:
            raise RuntimeError("orderbook.error is required when orderbook.ok is false (STRICT)")
        normalized_error_reason = _require_nonempty_str(error_reason, "orderbook.error")
        return {
            "status": _ENGINE_RUNTIME_STATUS_SERVER_STOP,
            "reason": normalized_error_reason,
            "source": _ENGINE_RUNTIME_SOURCE_DATABASE,
            "freshness_state": _ENGINE_RUNTIME_FRESHNESS_STALE,
            "delayed_threshold_ms": normalized_delayed_threshold_ms,
            "threshold_ms": normalized_stale_threshold_ms,
            "snapshot_ts_ms": None,
            "stale_ms": None,
            "is_warning": False,
            "warning_reason": None,
        }

    snapshot_ts_ms = _require_positive_int(normalized_orderbook.get("ts"), "orderbook.ts")
    stale_ms = normalized_now_ms - snapshot_ts_ms
    if stale_ms < 0:
        raise RuntimeError("orderbook stale_ms must be >= 0 (STRICT)")

    if stale_ms > normalized_stale_threshold_ms:
        return {
            "status": _ENGINE_RUNTIME_STATUS_SERVER_STOP,
            "reason": "orderbook_snapshot_stale",
            "source": _ENGINE_RUNTIME_SOURCE_DATABASE,
            "freshness_state": _ENGINE_RUNTIME_FRESHNESS_STALE,
            "delayed_threshold_ms": normalized_delayed_threshold_ms,
            "threshold_ms": normalized_stale_threshold_ms,
            "snapshot_ts_ms": snapshot_ts_ms,
            "stale_ms": stale_ms,
            "is_warning": False,
            "warning_reason": None,
        }

    if stale_ms > normalized_delayed_threshold_ms:
        return {
            "status": _ENGINE_RUNTIME_STATUS_SERVER_LIVE,
            "reason": "orderbook_snapshot_delayed",
            "source": _ENGINE_RUNTIME_SOURCE_DATABASE,
            "freshness_state": _ENGINE_RUNTIME_FRESHNESS_DELAYED,
            "delayed_threshold_ms": normalized_delayed_threshold_ms,
            "threshold_ms": normalized_stale_threshold_ms,
            "snapshot_ts_ms": snapshot_ts_ms,
            "stale_ms": stale_ms,
            "is_warning": True,
            "warning_reason": "orderbook_snapshot_delayed",
        }

    return {
        "status": _ENGINE_RUNTIME_STATUS_SERVER_LIVE,
        "reason": "orderbook_snapshot_fresh",
        "source": _ENGINE_RUNTIME_SOURCE_DATABASE,
        "freshness_state": _ENGINE_RUNTIME_FRESHNESS_FRESH,
        "delayed_threshold_ms": normalized_delayed_threshold_ms,
        "threshold_ms": normalized_stale_threshold_ms,
        "snapshot_ts_ms": snapshot_ts_ms,
        "stale_ms": stale_ms,
        "is_warning": False,
        "warning_reason": None,
    }


def _normalize_watchdog_signal_for_dashboard(
    *,
    latest_watchdog_status: str,
    latest_watchdog_reason: str,
    runtime: Dict[str, Any],
) -> Tuple[str, str, Optional[str]]:
    normalized_watchdog_status = _require_nonempty_str(
        latest_watchdog_status,
        "latest_watchdog_status",
    )
    normalized_watchdog_reason = _require_nonempty_str(
        latest_watchdog_reason,
        "latest_watchdog_reason",
    )
    normalized_runtime = _require_dict(runtime, "runtime")

    if normalized_watchdog_status not in {_WATCHDOG_STATUS_OK, _WATCHDOG_STATUS_INIT}:
        raise RuntimeError(f"unsupported latest_watchdog_status (STRICT): {normalized_watchdog_status!r}")

    # WATCHDOG INIT = 아직 첫 watchdog가 안 찍힌 초기 상태. 장애 아님.
    if normalized_watchdog_status == _WATCHDOG_STATUS_INIT:
        return normalized_watchdog_status, "ok", normalized_watchdog_reason

    runtime_status = _require_nonempty_str(normalized_runtime.get("status"), "runtime.status")
    runtime_reason = _require_nonempty_str(normalized_runtime.get("reason"), "runtime.reason")
    freshness_state = _require_nonempty_str(normalized_runtime.get("freshness_state"), "runtime.freshness_state")

    if normalized_watchdog_reason not in _DASHBOARD_DB_TRUTH_OVERRIDE_WATCHDOG_REASONS:
        return normalized_watchdog_status, normalized_watchdog_reason, None

    if runtime_status != _ENGINE_RUNTIME_STATUS_SERVER_LIVE:
        return normalized_watchdog_status, normalized_watchdog_reason, None

    if freshness_state not in {
        _ENGINE_RUNTIME_FRESHNESS_FRESH,
        _ENGINE_RUNTIME_FRESHNESS_DELAYED,
    }:
        return normalized_watchdog_status, normalized_watchdog_reason, None

    if runtime_reason not in {
        "orderbook_snapshot_fresh",
        "orderbook_snapshot_delayed",
    }:
        return normalized_watchdog_status, normalized_watchdog_reason, None

    return normalized_watchdog_status, "ok", normalized_watchdog_reason


def _ws_status(
    db: Session,
    symbol: str,
    *,
    min_buf: int,
    tfs: List[str],
) -> Dict[str, Any]:
    normalized_symbol = _normalize_symbol(symbol)
    normalized_min_buf = _require_positive_int(min_buf, "min_buf", max_value=100000)
    now_ms = _now_ms()

    out: Dict[str, Any] = {
        "available": True,
        "source": "database",
        "symbol": normalized_symbol,
        "min_buf": normalized_min_buf,
        "tfs": list(tfs),
        "buffers": {},
    }

    for tf in tfs:
        tf_name = _require_nonempty_str(tf, "tf")
        buf_len = _fetch_recent_candle_buffer_len_strict(
            db,
            symbol=normalized_symbol,
            timeframe=tf_name,
            row_limit=normalized_min_buf,
        )
        out["buffers"][tf_name] = {
            "ok": buf_len >= normalized_min_buf,
            "len": buf_len,
        }

    latest_orderbook = _fetch_latest_orderbook_snapshot_strict(db, symbol=normalized_symbol)
    if latest_orderbook is None:
        out["orderbook"] = {"ok": False, "error": "not_found"}
    else:
        stale_ms = now_ms - int(latest_orderbook["ts"])
        if stale_ms < 0:
            raise RuntimeError("orderbook stale_ms must be >= 0 (STRICT)")
        out["orderbook"] = {
            "ok": bool(latest_orderbook["has_bids"]) and bool(latest_orderbook["has_asks"]),
            "bestBid": latest_orderbook["bestBid"],
            "bestAsk": latest_orderbook["bestAsk"],
            "ts": latest_orderbook["ts"],
            "bids_len": latest_orderbook["bids_len"],
            "asks_len": latest_orderbook["asks_len"],
            "stale_ms": stale_ms,
        }

    out["runtime"] = _build_engine_runtime_status(
        orderbook=out["orderbook"],
        now_ms=now_ms,
    )
    return out


@app.get("/api/engine/ws-status")
def api_engine_ws_status(
    symbol: str = Query(default="BTCUSDT"),
    min_buf: int = Query(default=300, ge=1, le=100000),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    tfs = ["1m", "5m", "15m", "1h", "4h"]
    ws_status = _ws_status(db, symbol, min_buf=min_buf, tfs=tfs)
    ws_status["ts_ms"] = _now_ms()
    return ws_status


@app.get("/api/engine/latency")
def api_engine_latency(db: Session = Depends(get_db)) -> Dict[str, Any]:
    db_ms = _db_latency_ms(db)
    return {"db_latency_ms": db_ms}


@app.get("/api/engine/health")
@app.get("/api/engine/status")
def api_engine_health(
    symbol: str = Query(default="BTCUSDT"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    normalized_symbol = _normalize_symbol(symbol)
    now_ms = _now_ms()
    db_ms = _db_latency_ms(db)
    recent_errors = _count_recent_events_strict(db, minutes=10, event_type="ERROR")
    recent_skips = _count_recent_events_strict(db, minutes=10, event_type="SKIP")

    latest_watchdog = get_latest_watchdog_snapshot(db, include_test=False)
    latest_watchdog = _require_dict(latest_watchdog, "latest_watchdog")
    watchdog_status_raw = _require_nonempty_str(latest_watchdog.get("status"), "latest_watchdog.status")
    watchdog_reason_raw = _require_nonempty_str(latest_watchdog.get("reason"), "latest_watchdog.reason")

    ws_status = _ws_status(
        db,
        normalized_symbol,
        min_buf=300,
        tfs=["1m", "5m", "15m", "1h", "4h"],
    )
    runtime = _require_dict(ws_status.get("runtime"), "ws_status.runtime")

    watchdog_status_effective, watchdog_reason_effective, watchdog_reason_overridden = (
        _normalize_watchdog_signal_for_dashboard(
            latest_watchdog_status=watchdog_status_raw,
            latest_watchdog_reason=watchdog_reason_raw,
            runtime=runtime,
        )
    )

    status, reasons = _engine_status_from_signals(
        db_latency_ms=db_ms,
        recent_errors=recent_errors,
        recent_skips=recent_skips,
        latest_watchdog_status=watchdog_status_effective,
        latest_watchdog_reason=watchdog_reason_effective,
    )

    return {
        "status": status,
        "reasons": reasons,
        "runtime": runtime,
        "symbol": normalized_symbol,
        "db_latency_ms": db_ms,
        "recent_errors": int(recent_errors),
        "recent_skips": int(recent_skips),
        "latest_watchdog": latest_watchdog,
        "watchdog_status_raw": watchdog_status_raw,
        "watchdog_status_effective": watchdog_status_effective,
        "watchdog_reason_raw": watchdog_reason_raw,
        "watchdog_reason_effective": watchdog_reason_effective,
        "watchdog_reason_overridden": watchdog_reason_overridden,
        "ws_status": ws_status,
        "ts_ms": now_ms,
    }