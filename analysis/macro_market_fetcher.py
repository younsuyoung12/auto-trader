"""
========================================================
FILE: analysis/macro_market_fetcher.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할:
- Alpha Vantage 시세 데이터를 사용해 BTC 외부 시장 해석용 거시/크로스에셋 정보를 수집한다.
- 외부 시장 분석(Market Research) 레이어의 매크로 데이터 소스 역할만 수행한다.
- 주문/포지션/계정 관련 기능은 포함하지 않는다.

절대 원칙:
- STRICT · NO-FALLBACK
- settings.py(SSOT) 외 환경변수 직접 접근 금지
- print() 금지 / logging 사용
- 응답 누락/손상/모호성 발생 시 즉시 예외
- 민감정보 로그 금지

변경 이력:
2026-03-08
1) 신규 생성
2) Alpha Vantage GLOBAL_QUOTE 기반 cross-asset macro fetcher 추가
3) SPY / QQQ / GLD / UUP 해석 로직 추가

2026-03-08 (PATCH 2)
1) FIX(TRADE-GRADE): Alpha Vantage 무료 키 burst limit 대응 pacing 추가
2) GLOBAL_QUOTE 각 요청 전 최소 간격 sleep 강제
3) "1 request per second" 제한 위반 가능성 제거
4) 일일 quota(25/day) 초과는 기존대로 즉시 예외 전파

2026-03-08 (PATCH 3)
1) FIX(ROOT-CAUSE): 프로세스 내 macro snapshot cache 추가
2) FIX(TRADE-GRADE): 4-symbol 구조와 Alpha Vantage 무료 quota(25/day) 정합화
3) FIX(CONCURRENCY): 동시 호출 시 중복 외부 요청 방지 lock 추가
4) FIX(STRICT): stale cache fallback 금지, TTL 만료 후 fetch 실패 시 즉시 예외 전파
========================================================
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Mapping

import requests

from settings import SETTINGS

logger = logging.getLogger(__name__)

_ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"
_ALPHA_VANTAGE_MIN_REQUEST_INTERVAL_SEC = 1.25
_ALPHA_VANTAGE_FREE_DAILY_REQUEST_LIMIT = 25
_MACRO_SYMBOLS = ("SPY", "QQQ", "GLD", "UUP")
_SECONDS_PER_DAY = 86400


@dataclass(frozen=True)
class MacroAssetQuote:
    symbol: str
    price: Decimal
    change_abs: Decimal
    change_pct: Decimal
    latest_trading_day: str


@dataclass(frozen=True)
class MacroMarketSnapshot:
    as_of_ms: int
    risk_regime: str
    usd_regime: str
    cross_asset_bias: str
    spy: MacroAssetQuote
    qqq: MacroAssetQuote
    gld: MacroAssetQuote
    uup: MacroAssetQuote
    key_signals: List[str]
    dashboard_payload: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class MacroMarketFetcher:
    """
    Alpha Vantage 기반 거시/크로스에셋 fetcher.
    """

    def __init__(self) -> None:
        self._timeout_sec = self._require_float_setting("ANALYST_HTTP_TIMEOUT_SEC")
        self._api_key = self._require_alpha_vantage_api_key()
        self._cache_ttl_sec = self._build_required_cache_ttl_sec()

        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "auto-trader/macro-market-fetcher"})

        self._state_lock = threading.RLock()
        self._cached_snapshot: MacroMarketSnapshot | None = None
        self._cached_snapshot_fetched_monotonic: float | None = None
        self._last_alpha_vantage_request_monotonic: float | None = None

        logger.info(
            "MacroMarketFetcher initialized: symbols=%s cache_ttl_sec=%s min_request_interval_sec=%.2f",
            ",".join(_MACRO_SYMBOLS),
            self._cache_ttl_sec,
            _ALPHA_VANTAGE_MIN_REQUEST_INTERVAL_SEC,
        )

    def fetch(self) -> MacroMarketSnapshot:
        with self._state_lock:
            cached = self._get_fresh_cached_snapshot_locked()
            if cached is not None:
                age_sec = self._get_cached_snapshot_age_sec_locked()
                if age_sec is None:
                    raise RuntimeError("Cached macro snapshot age missing")
                logger.info(
                    "Macro market snapshot served from cache: age_sec=%.2f ttl_sec=%d",
                    age_sec,
                    self._cache_ttl_sec,
                )
                return cached

            quotes = {symbol: self._fetch_global_quote_locked(symbol) for symbol in _MACRO_SYMBOLS}

            spy = quotes["SPY"]
            qqq = quotes["QQQ"]
            gld = quotes["GLD"]
            uup = quotes["UUP"]

            risk_regime = self._classify_risk_regime(spy=spy, qqq=qqq)
            usd_regime = self._classify_usd_regime(uup=uup)
            cross_asset_bias = self._classify_cross_asset_bias(
                risk_regime=risk_regime,
                usd_regime=usd_regime,
                gld=gld,
            )
            key_signals = self._build_key_signals(
                risk_regime=risk_regime,
                usd_regime=usd_regime,
                cross_asset_bias=cross_asset_bias,
                spy=spy,
                qqq=qqq,
                gld=gld,
                uup=uup,
            )

            as_of_ms = self._now_ms()
            dashboard_payload = {
                "as_of_ms": as_of_ms,
                "risk_regime": risk_regime,
                "usd_regime": usd_regime,
                "cross_asset_bias": cross_asset_bias,
                "assets": {
                    "SPY": self._quote_to_payload(spy),
                    "QQQ": self._quote_to_payload(qqq),
                    "GLD": self._quote_to_payload(gld),
                    "UUP": self._quote_to_payload(uup),
                },
                "key_signals": list(key_signals),
            }

            result = MacroMarketSnapshot(
                as_of_ms=as_of_ms,
                risk_regime=risk_regime,
                usd_regime=usd_regime,
                cross_asset_bias=cross_asset_bias,
                spy=spy,
                qqq=qqq,
                gld=gld,
                uup=uup,
                key_signals=key_signals,
                dashboard_payload=dashboard_payload,
            )

            self._cached_snapshot = result
            self._cached_snapshot_fetched_monotonic = time.monotonic()

            logger.info(
                "Macro market snapshot fetched from Alpha Vantage: risk_regime=%s usd_regime=%s bias=%s cache_ttl_sec=%d",
                result.risk_regime,
                result.usd_regime,
                result.cross_asset_bias,
                self._cache_ttl_sec,
            )
            return result

    def _get_fresh_cached_snapshot_locked(self) -> MacroMarketSnapshot | None:
        if self._cached_snapshot is None:
            return None
        if self._cached_snapshot_fetched_monotonic is None:
            raise RuntimeError("Cached macro snapshot timestamp missing")

        age_sec = time.monotonic() - self._cached_snapshot_fetched_monotonic
        if age_sec < 0:
            raise RuntimeError("Cached macro snapshot age must not be negative")
        if age_sec >= self._cache_ttl_sec:
            logger.info(
                "Macro market snapshot cache expired: age_sec=%.2f ttl_sec=%d",
                age_sec,
                self._cache_ttl_sec,
            )
            return None
        return self._cached_snapshot

    def _get_cached_snapshot_age_sec_locked(self) -> float | None:
        if self._cached_snapshot_fetched_monotonic is None:
            return None
        age_sec = time.monotonic() - self._cached_snapshot_fetched_monotonic
        if age_sec < 0:
            raise RuntimeError("Cached macro snapshot age must not be negative")
        return age_sec

    def _fetch_global_quote_locked(self, symbol: str) -> MacroAssetQuote:
        self._sleep_before_alpha_vantage_request_locked(symbol)

        response = self._session.get(
            _ALPHA_VANTAGE_BASE_URL,
            params={
                "function": "GLOBAL_QUOTE",
                "symbol": symbol,
                "apikey": self._api_key,
            },
            timeout=self._timeout_sec,
        )
        self._last_alpha_vantage_request_monotonic = time.monotonic()

        if response.status_code != 200:
            raise RuntimeError(f"Alpha Vantage macro request failed: symbol={symbol}, status={response.status_code}")

        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(f"Alpha Vantage macro response is not valid JSON: symbol={symbol}") from exc

        if not isinstance(payload, Mapping):
            raise RuntimeError(f"Alpha Vantage macro response root must be an object: symbol={symbol}")

        if "Error Message" in payload:
            raise RuntimeError(f"Alpha Vantage macro error: symbol={symbol}, detail={payload['Error Message']}")
        if "Information" in payload and "Global Quote" not in payload:
            raise RuntimeError(f"Alpha Vantage macro information: symbol={symbol}, detail={payload['Information']}")

        raw_quote = payload.get("Global Quote")
        if not isinstance(raw_quote, Mapping) or not raw_quote:
            raise RuntimeError(f"Alpha Vantage Global Quote missing: symbol={symbol}")

        symbol_returned = self._require_nonempty_str(raw_quote.get("01. symbol"), f"{symbol}.01.symbol")
        if symbol_returned.upper() != symbol.upper():
            raise RuntimeError(f"Alpha Vantage Global Quote symbol mismatch: expected={symbol}, got={symbol_returned}")

        price = self._to_decimal(raw_quote.get("05. price"), f"{symbol}.05.price")
        change_abs = self._to_decimal(raw_quote.get("09. change"), f"{symbol}.09.change")
        change_pct_text = self._require_nonempty_str(raw_quote.get("10. change percent"), f"{symbol}.10.change_percent")
        if not change_pct_text.endswith("%"):
            raise RuntimeError(f"{symbol}.10.change_percent must end with %")
        change_pct = self._to_decimal(change_pct_text[:-1], f"{symbol}.10.change_percent")
        latest_trading_day = self._require_nonempty_str(raw_quote.get("07. latest trading day"), f"{symbol}.07.latest_trading_day")

        return MacroAssetQuote(
            symbol=symbol_returned.upper(),
            price=price,
            change_abs=change_abs,
            change_pct=change_pct,
            latest_trading_day=latest_trading_day,
        )

    def _sleep_before_alpha_vantage_request_locked(self, symbol: str) -> None:
        if _ALPHA_VANTAGE_MIN_REQUEST_INTERVAL_SEC <= 0:
            raise RuntimeError("Invalid Alpha Vantage request interval")

        if self._last_alpha_vantage_request_monotonic is None:
            logger.info(
                "Alpha Vantage pacing skip (first request): symbol=%s",
                symbol,
            )
            return

        elapsed_sec = time.monotonic() - self._last_alpha_vantage_request_monotonic
        if elapsed_sec < 0:
            raise RuntimeError("Alpha Vantage request elapsed time must not be negative")

        remain_sec = _ALPHA_VANTAGE_MIN_REQUEST_INTERVAL_SEC - elapsed_sec
        if remain_sec <= 0:
            logger.info(
                "Alpha Vantage pacing skip: symbol=%s elapsed_sec=%.4f min_interval_sec=%.2f",
                symbol,
                elapsed_sec,
                _ALPHA_VANTAGE_MIN_REQUEST_INTERVAL_SEC,
            )
            return

        logger.info(
            "Alpha Vantage pacing sleep before GLOBAL_QUOTE: symbol=%s sleep_sec=%.4f",
            symbol,
            remain_sec,
        )
        time.sleep(remain_sec)

    def _build_required_cache_ttl_sec(self) -> int:
        symbol_count = len(_MACRO_SYMBOLS)
        if symbol_count <= 0:
            raise RuntimeError("Macro symbols must not be empty")
        if _ALPHA_VANTAGE_FREE_DAILY_REQUEST_LIMIT <= 0:
            raise RuntimeError("Alpha Vantage daily request limit must be positive")

        snapshots_per_day = _ALPHA_VANTAGE_FREE_DAILY_REQUEST_LIMIT // symbol_count
        if snapshots_per_day <= 0:
            raise RuntimeError(
                "Alpha Vantage daily request limit is insufficient for configured macro symbols"
            )

        ttl_sec = _SECONDS_PER_DAY // snapshots_per_day
        if ttl_sec * snapshots_per_day < _SECONDS_PER_DAY:
            ttl_sec += 1
        if ttl_sec <= 0:
            raise RuntimeError("Computed macro snapshot cache TTL must be positive")

        return ttl_sec

    def _classify_risk_regime(self, *, spy: MacroAssetQuote, qqq: MacroAssetQuote) -> str:
        if spy.change_pct > Decimal("0") and qqq.change_pct > Decimal("0"):
            return "risk_on"
        if spy.change_pct < Decimal("0") and qqq.change_pct < Decimal("0"):
            return "risk_off"
        return "mixed"

    def _classify_usd_regime(self, *, uup: MacroAssetQuote) -> str:
        if uup.change_pct > Decimal("0"):
            return "usd_strength"
        if uup.change_pct < Decimal("0"):
            return "usd_weakness"
        return "usd_flat"

    def _classify_cross_asset_bias(
        self,
        *,
        risk_regime: str,
        usd_regime: str,
        gld: MacroAssetQuote,
    ) -> str:
        if risk_regime == "risk_on" and usd_regime == "usd_weakness":
            return "crypto_supportive"
        if risk_regime == "risk_off" and usd_regime == "usd_strength":
            return "crypto_headwind"
        if gld.change_pct > Decimal("0.30") and usd_regime == "usd_strength":
            return "defensive_rotation"
        return "mixed"

    def _build_key_signals(
        self,
        *,
        risk_regime: str,
        usd_regime: str,
        cross_asset_bias: str,
        spy: MacroAssetQuote,
        qqq: MacroAssetQuote,
        gld: MacroAssetQuote,
        uup: MacroAssetQuote,
    ) -> List[str]:
        return [
            f"risk_regime:{risk_regime}",
            f"usd_regime:{usd_regime}",
            f"cross_asset_bias:{cross_asset_bias}",
            f"spy_change_pct:{self._fmt_decimal(spy.change_pct, 2)}",
            f"qqq_change_pct:{self._fmt_decimal(qqq.change_pct, 2)}",
            f"gld_change_pct:{self._fmt_decimal(gld.change_pct, 2)}",
            f"uup_change_pct:{self._fmt_decimal(uup.change_pct, 2)}",
        ]

    def _quote_to_payload(self, quote: MacroAssetQuote) -> Dict[str, Any]:
        return {
            "symbol": quote.symbol,
            "price": self._fmt_decimal(quote.price, 4),
            "change_abs": self._fmt_decimal(quote.change_abs, 4),
            "change_pct": self._fmt_decimal(quote.change_pct, 4),
            "latest_trading_day": quote.latest_trading_day,
        }

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    def _to_decimal(self, value: Any, field_name: str) -> Decimal:
        if isinstance(value, bool):
            raise RuntimeError(f"{field_name} must not be bool")
        try:
            parsed = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise RuntimeError(f"Invalid decimal for {field_name}") from exc
        if not parsed.is_finite():
            raise RuntimeError(f"Non-finite decimal for {field_name}")
        return parsed

    def _require_nonempty_str(self, value: Any, field_name: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"{field_name} must be a non-empty string")
        return value.strip()

    def _fmt_decimal(self, value: Decimal, scale: int) -> str:
        return f"{value:.{scale}f}"

    def _require_float_setting(self, name: str) -> float:
        value = getattr(SETTINGS, name, None)
        if isinstance(value, bool):
            raise RuntimeError(f"Invalid bool value for float setting: {name}")
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Missing or invalid required float setting: {name}") from exc
        if parsed <= 0:
            raise RuntimeError(f"Invalid required float setting: {name}")
        return parsed

    def _require_alpha_vantage_api_key(self) -> str:
        value = getattr(SETTINGS, "ALPHAVANTAGE_API_KEY", None)
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError("Missing or invalid required setting: ALPHAVANTAGE_API_KEY")
        return value.strip()


__all__ = [
    "MacroMarketFetcher",
    "MacroMarketSnapshot",
    "MacroAssetQuote",
]