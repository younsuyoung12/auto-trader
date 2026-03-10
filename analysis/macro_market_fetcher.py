"""
========================================================
FILE: analysis/macro_market_fetcher.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

핵심 변경 요약
- Alpha Vantage macro snapshot을 프로세스 메모리 캐시 + Postgres 영속 캐시로 이중 관리
- 서버 재시작 후에도 DB의 신선한 snapshot을 재사용하도록 구조 변경
- stale DB 데이터 사용 금지, TTL 만료 시 외부 fetch 실패는 즉시 예외 전파 유지

코드 정리 내용
- snapshot 생성/직렬화/역직렬화 로직 분리
- DB schema 보장, 저장, 조회 경로를 본 파일 내부로 통합
- 미사용 코드/중복 조립 로직 정리

변경 이력
2026-03-09
1) FIX(ROOT-CAUSE): 프로세스 재시작 시 API 재호출로 quota가 누적되던 문제를 DB 영속 캐시로 수정
2) FIX(TRADE-GRADE): DB의 fresh snapshot만 사용, stale snapshot 재사용 금지
3) FIX(STRUCTURE): macro snapshot table 생성/저장/조회 책임을 fetcher 내부로 통합
4) FIX(LOG): cache source(memory/db/api) 구분 로그 추가

2026-03-08
1) Alpha Vantage GLOBAL_QUOTE 기반 cross-asset macro fetcher 추가
2) 무료 키 pacing(1 request/sec) 및 프로세스 내 cache TTL 적용
3) SPY / QQQ / GLD / UUP 해석 로직 추가
========================================================
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Mapping

import requests
from sqlalchemy import text

from settings import SETTINGS
from state.db_core import get_session

logger = logging.getLogger(__name__)

_ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"
_ALPHA_VANTAGE_MIN_REQUEST_INTERVAL_SEC = 1.25
_ALPHA_VANTAGE_FREE_DAILY_REQUEST_LIMIT = 25
_MACRO_SYMBOLS = ("SPY", "QQQ", "GLD", "UUP")
_SECONDS_PER_DAY = 86400

_MACRO_SNAPSHOT_TABLE = "analyst_macro_market_snapshots"
_MACRO_SNAPSHOT_SOURCE_KEY = "alphavantage_global_quote_macro_v1"


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

    설계:
    - fetch()는 메모리 캐시 → DB 영속 캐시 → 외부 API 순으로만 조회한다.
    - DB 영속 캐시는 주 소스이며, TTL 안의 fresh snapshot만 허용한다.
    - stale snapshot 재사용은 금지한다.
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
        self._db_schema_ready = False

        logger.info(
            "MacroMarketFetcher initialized: symbols=%s cache_ttl_sec=%s min_request_interval_sec=%.2f source_key=%s",
            ",".join(_MACRO_SYMBOLS),
            self._cache_ttl_sec,
            _ALPHA_VANTAGE_MIN_REQUEST_INTERVAL_SEC,
            _MACRO_SNAPSHOT_SOURCE_KEY,
        )

    def fetch(self) -> MacroMarketSnapshot:
        with self._state_lock:
            cached = self._get_fresh_cached_snapshot_locked()
            if cached is not None:
                age_sec = self._get_cached_snapshot_age_sec_locked()
                if age_sec is None:
                    raise RuntimeError("Cached macro snapshot age missing")
                logger.info(
                    "Macro market snapshot served from memory cache: age_sec=%.2f ttl_sec=%d",
                    age_sec,
                    self._cache_ttl_sec,
                )
                return cached

            persisted = self._load_fresh_db_snapshot_locked()
            if persisted is not None:
                self._set_memory_cache_locked(persisted)
                age_sec = self._compute_snapshot_age_sec_from_wall_clock(persisted.as_of_ms)
                logger.info(
                    "Macro market snapshot served from DB cache: age_sec=%.2f ttl_sec=%d as_of_ms=%d",
                    age_sec,
                    self._cache_ttl_sec,
                    persisted.as_of_ms,
                )
                return persisted

            quotes = {symbol: self._fetch_global_quote_locked(symbol) for symbol in _MACRO_SYMBOLS}
            snapshot = self._build_snapshot_from_quotes(quotes)
            self._persist_snapshot_locked(snapshot)
            self._set_memory_cache_locked(snapshot)

            logger.info(
                "Macro market snapshot fetched from Alpha Vantage and persisted: risk_regime=%s usd_regime=%s bias=%s cache_ttl_sec=%d as_of_ms=%d",
                snapshot.risk_regime,
                snapshot.usd_regime,
                snapshot.cross_asset_bias,
                self._cache_ttl_sec,
                snapshot.as_of_ms,
            )
            return snapshot

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
                "Macro market memory cache expired: age_sec=%.2f ttl_sec=%d",
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

    def _set_memory_cache_locked(self, snapshot: MacroMarketSnapshot) -> None:
        self._cached_snapshot = snapshot
        self._cached_snapshot_fetched_monotonic = time.monotonic()

    def _load_fresh_db_snapshot_locked(self) -> MacroMarketSnapshot | None:
        self._ensure_db_schema_locked()

        with get_session() as session:
            row = session.execute(
                text(
                    f"""
                    SELECT
                        id,
                        source_key,
                        as_of_ms,
                        cache_ttl_sec,
                        snapshot_payload
                    FROM {_MACRO_SNAPSHOT_TABLE}
                    WHERE source_key = :source_key
                    ORDER BY as_of_ms DESC, id DESC
                    LIMIT 1
                    """
                ),
                {"source_key": _MACRO_SNAPSHOT_SOURCE_KEY},
            ).mappings().first()

        if row is None:
            logger.info("No persisted macro snapshot found in DB")
            return None

        snapshot = self._snapshot_from_db_row(row)
        age_sec = self._compute_snapshot_age_sec_from_wall_clock(snapshot.as_of_ms)
        if age_sec >= self._cache_ttl_sec:
            logger.info(
                "Macro market DB cache expired: age_sec=%.2f ttl_sec=%d as_of_ms=%d",
                age_sec,
                self._cache_ttl_sec,
                snapshot.as_of_ms,
            )
            return None

        return snapshot

    def _persist_snapshot_locked(self, snapshot: MacroMarketSnapshot) -> None:
        self._ensure_db_schema_locked()

        snapshot_payload = self._snapshot_to_storage_payload(snapshot)
        snapshot_payload_json = json.dumps(snapshot_payload, ensure_ascii=False, separators=(",", ":"))

        with get_session() as session:
            session.execute(
                text(
                    f"""
                    INSERT INTO {_MACRO_SNAPSHOT_TABLE} (
                        source_key,
                        as_of_ms,
                        cache_ttl_sec,
                        snapshot_payload
                    ) VALUES (
                        :source_key,
                        :as_of_ms,
                        :cache_ttl_sec,
                        CAST(:snapshot_payload AS JSONB)
                    )
                    """
                ),
                {
                    "source_key": _MACRO_SNAPSHOT_SOURCE_KEY,
                    "as_of_ms": snapshot.as_of_ms,
                    "cache_ttl_sec": self._cache_ttl_sec,
                    "snapshot_payload": snapshot_payload_json,
                },
            )

    def _ensure_db_schema_locked(self) -> None:
        if self._db_schema_ready:
            return

        with get_session() as session:
            session.execute(
                text(
                    f"""
                    CREATE TABLE IF NOT EXISTS {_MACRO_SNAPSHOT_TABLE} (
                        id BIGSERIAL PRIMARY KEY,
                        source_key TEXT NOT NULL,
                        as_of_ms BIGINT NOT NULL,
                        cache_ttl_sec INTEGER NOT NULL,
                        snapshot_payload JSONB NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
            )
            session.execute(
                text(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_{_MACRO_SNAPSHOT_TABLE}_source_as_of
                    ON {_MACRO_SNAPSHOT_TABLE} (source_key, as_of_ms DESC, id DESC)
                    """
                )
            )

        self._db_schema_ready = True
        logger.info("Macro market DB schema ensured: table=%s", _MACRO_SNAPSHOT_TABLE)

    def _snapshot_to_storage_payload(self, snapshot: MacroMarketSnapshot) -> Dict[str, Any]:
        return {
            "as_of_ms": snapshot.as_of_ms,
            "risk_regime": snapshot.risk_regime,
            "usd_regime": snapshot.usd_regime,
            "cross_asset_bias": snapshot.cross_asset_bias,
            "assets": {
                "SPY": self._quote_to_storage_payload(snapshot.spy),
                "QQQ": self._quote_to_storage_payload(snapshot.qqq),
                "GLD": self._quote_to_storage_payload(snapshot.gld),
                "UUP": self._quote_to_storage_payload(snapshot.uup),
            },
            "key_signals": list(snapshot.key_signals),
            "dashboard_payload": snapshot.dashboard_payload,
        }

    def _snapshot_from_db_row(self, row: Mapping[str, Any]) -> MacroMarketSnapshot:
        source_key = self._require_nonempty_str(row.get("source_key"), "db.source_key")
        if source_key != _MACRO_SNAPSHOT_SOURCE_KEY:
            raise RuntimeError(
                f"Persisted macro snapshot source_key mismatch: expected={_MACRO_SNAPSHOT_SOURCE_KEY}, got={source_key}"
            )

        as_of_ms = self._require_int(row.get("as_of_ms"), "db.as_of_ms")
        if as_of_ms <= 0:
            raise RuntimeError("db.as_of_ms must be positive")

        cache_ttl_sec = self._require_int(row.get("cache_ttl_sec"), "db.cache_ttl_sec")
        if cache_ttl_sec != self._cache_ttl_sec:
            logger.info(
                "Persisted macro snapshot ttl differs from runtime ttl: persisted=%d runtime=%d",
                cache_ttl_sec,
                self._cache_ttl_sec,
            )

        raw_payload = row.get("snapshot_payload")
        payload = self._coerce_json_object(raw_payload, "db.snapshot_payload")
        payload_as_of_ms = self._require_int(payload.get("as_of_ms"), "snapshot_payload.as_of_ms")
        if payload_as_of_ms != as_of_ms:
            raise RuntimeError(
                f"Persisted macro snapshot as_of_ms mismatch: row={as_of_ms}, payload={payload_as_of_ms}"
            )

        risk_regime = self._require_nonempty_str(payload.get("risk_regime"), "snapshot_payload.risk_regime")
        usd_regime = self._require_nonempty_str(payload.get("usd_regime"), "snapshot_payload.usd_regime")
        cross_asset_bias = self._require_nonempty_str(
            payload.get("cross_asset_bias"),
            "snapshot_payload.cross_asset_bias",
        )

        assets_payload = payload.get("assets")
        if not isinstance(assets_payload, Mapping):
            raise RuntimeError("snapshot_payload.assets must be an object")

        spy = self._quote_from_storage_payload(assets_payload.get("SPY"), "snapshot_payload.assets.SPY")
        qqq = self._quote_from_storage_payload(assets_payload.get("QQQ"), "snapshot_payload.assets.QQQ")
        gld = self._quote_from_storage_payload(assets_payload.get("GLD"), "snapshot_payload.assets.GLD")
        uup = self._quote_from_storage_payload(assets_payload.get("UUP"), "snapshot_payload.assets.UUP")

        raw_key_signals = payload.get("key_signals")
        if not isinstance(raw_key_signals, list):
            raise RuntimeError("snapshot_payload.key_signals must be a list")
        key_signals = [
            self._require_nonempty_str(item, f"snapshot_payload.key_signals[{idx}]")
            for idx, item in enumerate(raw_key_signals)
        ]

        dashboard_payload = payload.get("dashboard_payload")
        if not isinstance(dashboard_payload, Mapping):
            raise RuntimeError("snapshot_payload.dashboard_payload must be an object")

        return MacroMarketSnapshot(
            as_of_ms=as_of_ms,
            risk_regime=risk_regime,
            usd_regime=usd_regime,
            cross_asset_bias=cross_asset_bias,
            spy=spy,
            qqq=qqq,
            gld=gld,
            uup=uup,
            key_signals=key_signals,
            dashboard_payload=dict(dashboard_payload),
        )

    def _quote_to_storage_payload(self, quote: MacroAssetQuote) -> Dict[str, str]:
        return {
            "symbol": quote.symbol,
            "price": self._fmt_decimal(quote.price, 10),
            "change_abs": self._fmt_decimal(quote.change_abs, 10),
            "change_pct": self._fmt_decimal(quote.change_pct, 10),
            "latest_trading_day": quote.latest_trading_day,
        }

    def _quote_from_storage_payload(self, payload: Any, field_name: str) -> MacroAssetQuote:
        if not isinstance(payload, Mapping):
            raise RuntimeError(f"{field_name} must be an object")

        return MacroAssetQuote(
            symbol=self._require_nonempty_str(payload.get("symbol"), f"{field_name}.symbol").upper(),
            price=self._to_decimal(payload.get("price"), f"{field_name}.price"),
            change_abs=self._to_decimal(payload.get("change_abs"), f"{field_name}.change_abs"),
            change_pct=self._to_decimal(payload.get("change_pct"), f"{field_name}.change_pct"),
            latest_trading_day=self._require_nonempty_str(
                payload.get("latest_trading_day"),
                f"{field_name}.latest_trading_day",
            ),
        )

    def _build_snapshot_from_quotes(self, quotes: Mapping[str, MacroAssetQuote]) -> MacroMarketSnapshot:
        spy = self._require_quote_from_mapping(quotes, "SPY")
        qqq = self._require_quote_from_mapping(quotes, "QQQ")
        gld = self._require_quote_from_mapping(quotes, "GLD")
        uup = self._require_quote_from_mapping(quotes, "UUP")

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

        return MacroMarketSnapshot(
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

    def _require_quote_from_mapping(self, quotes: Mapping[str, MacroAssetQuote], symbol: str) -> MacroAssetQuote:
        quote = quotes.get(symbol)
        if not isinstance(quote, MacroAssetQuote):
            raise RuntimeError(f"Macro quote missing or invalid: {symbol}")
        return quote

    def _compute_snapshot_age_sec_from_wall_clock(self, as_of_ms: int) -> float:
        now_ms = self._now_ms()
        age_ms = now_ms - as_of_ms
        if age_ms < 0:
            raise RuntimeError(f"Macro snapshot age must not be negative: now_ms={now_ms}, as_of_ms={as_of_ms}")
        return age_ms / 1000.0

    def _coerce_json_object(self, value: Any, field_name: str) -> Dict[str, Any]:
        if isinstance(value, Mapping):
            return dict(value)
        if isinstance(value, str):
            try:
                decoded = json.loads(value)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"{field_name} must be valid JSON object text") from exc
            if not isinstance(decoded, Mapping):
                raise RuntimeError(f"{field_name} JSON root must be an object")
            return dict(decoded)
        raise RuntimeError(f"{field_name} must be mapping or JSON string")

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
        change_pct_text = self._require_nonempty_str(
            raw_quote.get("10. change percent"),
            f"{symbol}.10.change_percent",
        )
        if not change_pct_text.endswith("%"):
            raise RuntimeError(f"{symbol}.10.change_percent must end with %")
        change_pct = self._to_decimal(change_pct_text[:-1], f"{symbol}.10.change_percent")
        latest_trading_day = self._require_nonempty_str(
            raw_quote.get("07. latest trading day"),
            f"{symbol}.07.latest_trading_day",
        )

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
            logger.info("Alpha Vantage pacing skip (first request): symbol=%s", symbol)
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
            raise RuntimeError("Alpha Vantage daily request limit is insufficient for configured macro symbols")

        ttl_sec = _SECONDS_PER_DAY // snapshots_per_day
        if ttl_sec * snapshots_per_day < _SECONDS_PER_DAY:
            ttl_sec += 1
        if ttl_sec <= 0:
            raise RuntimeError("Computed macro snapshot cache TTL must be positive")

        # 최소 TTL 보장 (매크로 데이터는 실시간 필요 없음)
        ttl_sec = max(ttl_sec, 21600)  # 6시간

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

    def _require_int(self, value: Any, field_name: str) -> int:
        if isinstance(value, bool):
            raise RuntimeError(f"{field_name} must not be bool")
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"{field_name} must be an integer") from exc
        return parsed

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