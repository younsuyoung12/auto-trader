"""
========================================================
FILE: analysis/derivatives_market_fetcher.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

핵심 변경 요약
- Bybit / OKX 파생시장 snapshot을 프로세스 메모리 캐시 + Postgres 영속 캐시로 이중 관리
- 서버 재시작 후에도 DB의 fresh snapshot을 재사용하도록 구조 변경
- stale DB/메모리 캐시 재사용 금지, TTL 만료 후 fetch 실패 시 즉시 예외 전파 유지

코드 정리 내용
- snapshot 생성/직렬화/역직렬화 로직 분리
- DB schema 보장, 저장, 조회 경로를 본 파일 내부로 통합
- 중복 조립 로직 정리

역할:
- Bybit / OKX 공개 파생시장 데이터를 수집해 외부 시장 분석용 파생상품 요약을 생성한다.
- 외부 시장 분석(Market Research) 레이어의 보조 데이터 소스 역할만 수행한다.
- 주문/포지션/계정 관련 기능은 포함하지 않는다.

절대 원칙:
- STRICT · NO-FALLBACK
- settings.py(SSOT) 외 환경변수 직접 접근 금지
- print() 금지 / logging 사용
- 응답 누락/손상/모호성 발생 시 즉시 예외
- 민감정보 로그 금지
- 공개 REST 데이터만 사용

변경 이력:
2026-03-09
1) FIX(ROOT-CAUSE): 파생시장 snapshot DB 영속 캐시 추가
2) FIX(STRICT): DB의 fresh snapshot만 사용, stale snapshot 재사용 금지
3) FIX(STRUCTURE): derivatives snapshot table 생성/저장/조회 책임을 fetcher 내부로 통합
4) FIX(LOG): cache source(memory/db/api) 구분 로그 추가

2026-03-08
1) Bybit / OKX 공개 파생시장 fetcher 추가
2) funding / open interest / exchange divergence 진단 추가
========================================================
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Mapping, Optional

import requests
from sqlalchemy import text

from settings import SETTINGS
from state.db_core import get_session

logger = logging.getLogger(__name__)

_BYBIT_BASE_URL = "https://api.bybit.com"
_OKX_BASE_URL = "https://www.okx.com"

_BYBIT_OPEN_INTEREST_LIMIT = 30
_BYBIT_FUNDING_LIMIT = 30
_OKX_FUNDING_LIMIT = 30

_DERIVATIVES_CACHE_TTL_SEC = 300
_DERIVATIVES_SNAPSHOT_TABLE = "analyst_derivatives_market_snapshots"
_DERIVATIVES_SNAPSHOT_SOURCE_KEY = "bybit_okx_derivatives_market_v1"


@dataclass(frozen=True)
class BybitOpenInterestPoint:
    timestamp_ms: int
    open_interest: Decimal


@dataclass(frozen=True)
class BybitFundingRatePoint:
    timestamp_ms: int
    funding_rate: Decimal


@dataclass(frozen=True)
class OkxOpenInterestPoint:
    timestamp_ms: int
    open_interest_contracts: Decimal
    open_interest_usd: Optional[Decimal]


@dataclass(frozen=True)
class OkxFundingRatePoint:
    timestamp_ms: int
    funding_rate: Decimal


@dataclass(frozen=True)
class DerivativesMarketSnapshot:
    symbol: str
    bybit_symbol: str
    okx_inst_id: str
    as_of_ms: int
    bybit_latest_open_interest: Decimal
    bybit_open_interest_change_pct: Decimal
    bybit_latest_funding_rate: Decimal
    okx_latest_open_interest_contracts: Decimal
    okx_latest_open_interest_usd: Optional[Decimal]
    okx_latest_funding_rate: Decimal
    funding_rate_spread: Decimal
    cross_exchange_crowding_bias: str
    key_signals: List[str]
    dashboard_payload: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class DerivativesMarketFetcher:
    """
    Bybit / OKX 공개 파생상품 데이터 fetcher.

    설계:
    - fetch()는 메모리 캐시 → DB 영속 캐시 → 외부 API 순으로만 조회한다.
    - DB 영속 캐시는 주 소스이며, TTL 안의 fresh snapshot만 허용한다.
    - stale snapshot 재사용은 금지한다.
    """

    def __init__(self) -> None:
        self._symbol = self._require_str_setting("ANALYST_MARKET_SYMBOL")
        self._timeout_sec = self._require_float_setting("ANALYST_HTTP_TIMEOUT_SEC")
        if self._timeout_sec <= 0:
            raise RuntimeError("ANALYST_HTTP_TIMEOUT_SEC must be > 0")

        self._cache_ttl_sec = _DERIVATIVES_CACHE_TTL_SEC

        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "auto-trader/derivatives-market-fetcher"})

        self._bybit_symbol = self._normalize_bybit_symbol(self._symbol)
        self._okx_inst_id = self._normalize_okx_inst_id(self._symbol)

        self._state_lock = threading.RLock()
        self._cached_snapshot: DerivativesMarketSnapshot | None = None
        self._cached_snapshot_fetched_monotonic: float | None = None
        self._db_schema_ready = False

        logger.info(
            "DerivativesMarketFetcher initialized: symbol=%s bybit_symbol=%s okx_inst_id=%s cache_ttl_sec=%d source_key=%s",
            self._symbol,
            self._bybit_symbol,
            self._okx_inst_id,
            self._cache_ttl_sec,
            _DERIVATIVES_SNAPSHOT_SOURCE_KEY,
        )

    @property
    def symbol(self) -> str:
        return self._symbol

    def fetch(self) -> DerivativesMarketSnapshot:
        with self._state_lock:
            cached = self._get_fresh_cached_snapshot_locked()
            if cached is not None:
                age_sec = self._get_cached_snapshot_age_sec_locked()
                if age_sec is None:
                    raise RuntimeError("Cached derivatives snapshot age missing")
                logger.info(
                    "Derivatives snapshot served from memory cache: symbol=%s age_sec=%.2f ttl_sec=%d",
                    self._symbol,
                    age_sec,
                    self._cache_ttl_sec,
                )
                return cached

            persisted = self._load_fresh_db_snapshot_locked()
            if persisted is not None:
                self._set_memory_cache_locked(persisted)
                logger.info(
                    "Derivatives snapshot served from DB cache: symbol=%s ttl_sec=%d as_of_ms=%d",
                    self._symbol,
                    self._cache_ttl_sec,
                    persisted.as_of_ms,
                )
                return persisted

            bybit_oi_points = self._fetch_bybit_open_interest_history()
            bybit_funding_points = self._fetch_bybit_funding_history()
            okx_oi_point = self._fetch_okx_open_interest()
            okx_funding_points = self._fetch_okx_funding_history()

            fetched_at_ms = self._now_ms()
            snapshot = self._build_snapshot_from_points(
                bybit_oi_points=bybit_oi_points,
                bybit_funding_points=bybit_funding_points,
                okx_oi_point=okx_oi_point,
                okx_funding_points=okx_funding_points,
                fetched_at_ms=fetched_at_ms,
            )

            self._persist_snapshot_locked(snapshot=snapshot, fetched_at_ms=fetched_at_ms)
            self._set_memory_cache_locked(snapshot)

            logger.info(
                "Derivatives snapshot fetched from APIs and persisted: symbol=%s bybit_funding=%s okx_funding=%s oi_change_pct=%s bias=%s cache_ttl_sec=%d",
                snapshot.symbol,
                self._fmt_decimal(snapshot.bybit_latest_funding_rate, 8),
                self._fmt_decimal(snapshot.okx_latest_funding_rate, 8),
                self._fmt_decimal(snapshot.bybit_open_interest_change_pct, 6),
                snapshot.cross_exchange_crowding_bias,
                self._cache_ttl_sec,
            )
            return snapshot

    def _build_snapshot_from_points(
        self,
        *,
        bybit_oi_points: List[BybitOpenInterestPoint],
        bybit_funding_points: List[BybitFundingRatePoint],
        okx_oi_point: OkxOpenInterestPoint,
        okx_funding_points: List[OkxFundingRatePoint],
        fetched_at_ms: int,
    ) -> DerivativesMarketSnapshot:
        bybit_latest_oi = bybit_oi_points[-1].open_interest
        bybit_oi_change_pct = self._calc_change_pct(
            base=bybit_oi_points[0].open_interest,
            latest=bybit_latest_oi,
            field_name="bybit_open_interest_change_pct",
        )
        bybit_latest_funding = bybit_funding_points[-1].funding_rate
        okx_latest_funding = okx_funding_points[-1].funding_rate
        funding_rate_spread = bybit_latest_funding - okx_latest_funding

        cross_exchange_crowding_bias = self._classify_crowding_bias(
            bybit_latest_funding=bybit_latest_funding,
            okx_latest_funding=okx_latest_funding,
            bybit_oi_change_pct=bybit_oi_change_pct,
        )

        as_of_ms = max(
            bybit_oi_points[-1].timestamp_ms,
            bybit_funding_points[-1].timestamp_ms,
            okx_oi_point.timestamp_ms,
            okx_funding_points[-1].timestamp_ms,
        )

        key_signals = self._build_key_signals(
            bybit_oi_change_pct=bybit_oi_change_pct,
            bybit_latest_funding=bybit_latest_funding,
            okx_latest_funding=okx_latest_funding,
            funding_rate_spread=funding_rate_spread,
            crowding_bias=cross_exchange_crowding_bias,
        )

        dashboard_payload = {
            "symbol": self._symbol,
            "as_of_ms": as_of_ms,
            "fetched_at_ms": fetched_at_ms,
            "bybit": {
                "symbol": self._bybit_symbol,
                "latest_open_interest": self._fmt_decimal(bybit_latest_oi, 8),
                "open_interest_change_pct": self._fmt_decimal(bybit_oi_change_pct, 6),
                "latest_funding_rate": self._fmt_decimal(bybit_latest_funding, 8),
            },
            "okx": {
                "inst_id": self._okx_inst_id,
                "latest_open_interest_contracts": self._fmt_decimal(okx_oi_point.open_interest_contracts, 8),
                "latest_open_interest_usd": self._fmt_decimal_optional(okx_oi_point.open_interest_usd, 2),
                "latest_funding_rate": self._fmt_decimal(okx_latest_funding, 8),
            },
            "cross_exchange": {
                "funding_rate_spread": self._fmt_decimal(funding_rate_spread, 8),
                "crowding_bias": cross_exchange_crowding_bias,
            },
            "key_signals": list(key_signals),
        }

        return DerivativesMarketSnapshot(
            symbol=self._symbol,
            bybit_symbol=self._bybit_symbol,
            okx_inst_id=self._okx_inst_id,
            as_of_ms=as_of_ms,
            bybit_latest_open_interest=bybit_latest_oi,
            bybit_open_interest_change_pct=bybit_oi_change_pct,
            bybit_latest_funding_rate=bybit_latest_funding,
            okx_latest_open_interest_contracts=okx_oi_point.open_interest_contracts,
            okx_latest_open_interest_usd=okx_oi_point.open_interest_usd,
            okx_latest_funding_rate=okx_latest_funding,
            funding_rate_spread=funding_rate_spread,
            cross_exchange_crowding_bias=cross_exchange_crowding_bias,
            key_signals=key_signals,
            dashboard_payload=dashboard_payload,
        )

    def _get_fresh_cached_snapshot_locked(self) -> DerivativesMarketSnapshot | None:
        if self._cached_snapshot is None:
            return None
        if self._cached_snapshot_fetched_monotonic is None:
            raise RuntimeError("Cached derivatives snapshot timestamp missing")

        age_sec = time.monotonic() - self._cached_snapshot_fetched_monotonic
        if age_sec < 0:
            raise RuntimeError("Cached derivatives snapshot age must not be negative")
        if age_sec >= self._cache_ttl_sec:
            logger.info(
                "Derivatives memory cache expired: symbol=%s age_sec=%.2f ttl_sec=%d",
                self._symbol,
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
            raise RuntimeError("Cached derivatives snapshot age must not be negative")
        return age_sec

    def _set_memory_cache_locked(self, snapshot: DerivativesMarketSnapshot) -> None:
        self._cached_snapshot = snapshot
        self._cached_snapshot_fetched_monotonic = time.monotonic()

    def _load_fresh_db_snapshot_locked(self) -> DerivativesMarketSnapshot | None:
        self._ensure_db_schema_locked()

        with get_session() as session:
            row = session.execute(
                text(
                    f"""
                    SELECT
                        id,
                        source_key,
                        symbol,
                        fetched_at_ms,
                        snapshot_as_of_ms,
                        cache_ttl_sec,
                        snapshot_payload
                    FROM {_DERIVATIVES_SNAPSHOT_TABLE}
                    WHERE source_key = :source_key
                      AND symbol = :symbol
                    ORDER BY fetched_at_ms DESC, id DESC
                    LIMIT 1
                    """
                ),
                {
                    "source_key": _DERIVATIVES_SNAPSHOT_SOURCE_KEY,
                    "symbol": self._symbol,
                },
            ).mappings().first()

        if row is None:
            logger.info("No persisted derivatives snapshot found in DB: symbol=%s", self._symbol)
            return None

        snapshot = self._snapshot_from_db_row(row)
        fetched_at_ms = self._require_int(row.get("fetched_at_ms"), "db.fetched_at_ms")
        age_sec = self._compute_age_sec_from_fetched_at_ms(fetched_at_ms)
        if age_sec >= self._cache_ttl_sec:
            logger.info(
                "Derivatives DB cache expired: symbol=%s age_sec=%.2f ttl_sec=%d fetched_at_ms=%d",
                self._symbol,
                age_sec,
                self._cache_ttl_sec,
                fetched_at_ms,
            )
            return None

        return snapshot

    def _persist_snapshot_locked(self, *, snapshot: DerivativesMarketSnapshot, fetched_at_ms: int) -> None:
        self._ensure_db_schema_locked()

        snapshot_payload = self._snapshot_to_storage_payload(snapshot)
        snapshot_payload_json = json.dumps(snapshot_payload, ensure_ascii=False, separators=(",", ":"))

        with get_session() as session:
            session.execute(
                text(
                    f"""
                    INSERT INTO {_DERIVATIVES_SNAPSHOT_TABLE} (
                        source_key,
                        symbol,
                        fetched_at_ms,
                        snapshot_as_of_ms,
                        cache_ttl_sec,
                        snapshot_payload
                    ) VALUES (
                        :source_key,
                        :symbol,
                        :fetched_at_ms,
                        :snapshot_as_of_ms,
                        :cache_ttl_sec,
                        CAST(:snapshot_payload AS JSONB)
                    )
                    """
                ),
                {
                    "source_key": _DERIVATIVES_SNAPSHOT_SOURCE_KEY,
                    "symbol": snapshot.symbol,
                    "fetched_at_ms": fetched_at_ms,
                    "snapshot_as_of_ms": snapshot.as_of_ms,
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
                    CREATE TABLE IF NOT EXISTS {_DERIVATIVES_SNAPSHOT_TABLE} (
                        id BIGSERIAL PRIMARY KEY,
                        source_key TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        fetched_at_ms BIGINT NOT NULL,
                        snapshot_as_of_ms BIGINT NOT NULL,
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
                    CREATE INDEX IF NOT EXISTS idx_{_DERIVATIVES_SNAPSHOT_TABLE}_source_symbol_fetched
                    ON {_DERIVATIVES_SNAPSHOT_TABLE} (source_key, symbol, fetched_at_ms DESC, id DESC)
                    """
                )
            )

        self._db_schema_ready = True
        logger.info("Derivatives DB schema ensured: table=%s", _DERIVATIVES_SNAPSHOT_TABLE)

    def _snapshot_to_storage_payload(self, snapshot: DerivativesMarketSnapshot) -> Dict[str, Any]:
        return {
            "symbol": snapshot.symbol,
            "bybit_symbol": snapshot.bybit_symbol,
            "okx_inst_id": snapshot.okx_inst_id,
            "as_of_ms": snapshot.as_of_ms,
            "bybit_latest_open_interest": self._fmt_decimal(snapshot.bybit_latest_open_interest, 10),
            "bybit_open_interest_change_pct": self._fmt_decimal(snapshot.bybit_open_interest_change_pct, 10),
            "bybit_latest_funding_rate": self._fmt_decimal(snapshot.bybit_latest_funding_rate, 10),
            "okx_latest_open_interest_contracts": self._fmt_decimal(snapshot.okx_latest_open_interest_contracts, 10),
            "okx_latest_open_interest_usd": self._fmt_decimal_optional(snapshot.okx_latest_open_interest_usd, 10),
            "okx_latest_funding_rate": self._fmt_decimal(snapshot.okx_latest_funding_rate, 10),
            "funding_rate_spread": self._fmt_decimal(snapshot.funding_rate_spread, 10),
            "cross_exchange_crowding_bias": snapshot.cross_exchange_crowding_bias,
            "key_signals": list(snapshot.key_signals),
            "dashboard_payload": snapshot.dashboard_payload,
        }

    def _snapshot_from_db_row(self, row: Mapping[str, Any]) -> DerivativesMarketSnapshot:
        source_key = self._require_nonempty_str(row.get("source_key"), "db.source_key")
        if source_key != _DERIVATIVES_SNAPSHOT_SOURCE_KEY:
            raise RuntimeError(
                f"Persisted derivatives snapshot source_key mismatch: expected={_DERIVATIVES_SNAPSHOT_SOURCE_KEY}, got={source_key}"
            )

        symbol = self._require_nonempty_str(row.get("symbol"), "db.symbol").upper()
        if symbol != self._symbol:
            raise RuntimeError(
                f"Persisted derivatives snapshot symbol mismatch: expected={self._symbol}, got={symbol}"
            )

        snapshot_as_of_ms = self._require_int(row.get("snapshot_as_of_ms"), "db.snapshot_as_of_ms")
        if snapshot_as_of_ms <= 0:
            raise RuntimeError("db.snapshot_as_of_ms must be positive")

        cache_ttl_sec = self._require_int(row.get("cache_ttl_sec"), "db.cache_ttl_sec")
        if cache_ttl_sec != self._cache_ttl_sec:
            logger.info(
                "Persisted derivatives snapshot ttl differs from runtime ttl: persisted=%d runtime=%d",
                cache_ttl_sec,
                self._cache_ttl_sec,
            )

        payload = self._coerce_json_object(row.get("snapshot_payload"), "db.snapshot_payload")

        payload_symbol = self._require_nonempty_str(payload.get("symbol"), "snapshot_payload.symbol").upper()
        if payload_symbol != self._symbol:
            raise RuntimeError(
                f"Persisted derivatives payload symbol mismatch: expected={self._symbol}, got={payload_symbol}"
            )

        payload_as_of_ms = self._require_int(payload.get("as_of_ms"), "snapshot_payload.as_of_ms")
        if payload_as_of_ms != snapshot_as_of_ms:
            raise RuntimeError(
                f"Persisted derivatives as_of_ms mismatch: row={snapshot_as_of_ms}, payload={payload_as_of_ms}"
            )

        bybit_symbol = self._require_nonempty_str(payload.get("bybit_symbol"), "snapshot_payload.bybit_symbol")
        okx_inst_id = self._require_nonempty_str(payload.get("okx_inst_id"), "snapshot_payload.okx_inst_id")

        raw_okx_oi_usd = payload.get("okx_latest_open_interest_usd")
        okx_latest_open_interest_usd: Optional[Decimal]
        if raw_okx_oi_usd is None:
            okx_latest_open_interest_usd = None
        else:
            okx_latest_open_interest_usd = self._to_decimal(
                raw_okx_oi_usd,
                "snapshot_payload.okx_latest_open_interest_usd",
            )

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

        return DerivativesMarketSnapshot(
            symbol=self._symbol,
            bybit_symbol=bybit_symbol,
            okx_inst_id=okx_inst_id,
            as_of_ms=snapshot_as_of_ms,
            bybit_latest_open_interest=self._to_decimal(
                payload.get("bybit_latest_open_interest"),
                "snapshot_payload.bybit_latest_open_interest",
            ),
            bybit_open_interest_change_pct=self._to_decimal(
                payload.get("bybit_open_interest_change_pct"),
                "snapshot_payload.bybit_open_interest_change_pct",
            ),
            bybit_latest_funding_rate=self._to_decimal(
                payload.get("bybit_latest_funding_rate"),
                "snapshot_payload.bybit_latest_funding_rate",
            ),
            okx_latest_open_interest_contracts=self._to_decimal(
                payload.get("okx_latest_open_interest_contracts"),
                "snapshot_payload.okx_latest_open_interest_contracts",
            ),
            okx_latest_open_interest_usd=okx_latest_open_interest_usd,
            okx_latest_funding_rate=self._to_decimal(
                payload.get("okx_latest_funding_rate"),
                "snapshot_payload.okx_latest_funding_rate",
            ),
            funding_rate_spread=self._to_decimal(
                payload.get("funding_rate_spread"),
                "snapshot_payload.funding_rate_spread",
            ),
            cross_exchange_crowding_bias=self._require_nonempty_str(
                payload.get("cross_exchange_crowding_bias"),
                "snapshot_payload.cross_exchange_crowding_bias",
            ),
            key_signals=key_signals,
            dashboard_payload=dict(dashboard_payload),
        )

    def _compute_age_sec_from_fetched_at_ms(self, fetched_at_ms: int) -> float:
        now_ms = self._now_ms()
        age_ms = now_ms - fetched_at_ms
        if age_ms < 0:
            raise RuntimeError(
                f"Derivatives snapshot fetched_at_ms is in the future: now_ms={now_ms}, fetched_at_ms={fetched_at_ms}"
            )
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

    # ========================================================
    # Bybit
    # ========================================================

    def _fetch_bybit_open_interest_history(self) -> List[BybitOpenInterestPoint]:
        payload = self._request_json(
            base_url=_BYBIT_BASE_URL,
            path="/v5/market/open-interest",
            params={
                "category": "linear",
                "symbol": self._bybit_symbol,
                "intervalTime": "5min",
                "limit": _BYBIT_OPEN_INTEREST_LIMIT,
            },
            source_name="bybit_open_interest",
        )
        if payload.get("retCode") != 0:
            raise RuntimeError(f"Bybit open interest request failed: retCode={payload.get('retCode')}")

        result = payload.get("result")
        if not isinstance(result, Mapping):
            raise RuntimeError("Bybit open interest result must be an object")

        rows = result.get("list")
        if not isinstance(rows, list) or not rows:
            raise RuntimeError("Bybit open interest list must be a non-empty list")

        points: List[BybitOpenInterestPoint] = []
        for row in rows:
            if not isinstance(row, Mapping):
                raise RuntimeError("Bybit open interest row must be an object")
            ts_ms = self._to_int(row.get("timestamp"), "bybit_open_interest.timestamp")
            open_interest = self._to_decimal(row.get("openInterest"), "bybit_open_interest.openInterest")
            points.append(BybitOpenInterestPoint(timestamp_ms=ts_ms, open_interest=open_interest))

        points_sorted = sorted(points, key=lambda x: x.timestamp_ms)
        self._validate_increasing_timestamps(
            [x.timestamp_ms for x in points_sorted],
            "bybit_open_interest",
        )
        return points_sorted

    def _fetch_bybit_funding_history(self) -> List[BybitFundingRatePoint]:
        payload = self._request_json(
            base_url=_BYBIT_BASE_URL,
            path="/v5/market/funding/history",
            params={
                "category": "linear",
                "symbol": self._bybit_symbol,
                "limit": _BYBIT_FUNDING_LIMIT,
            },
            source_name="bybit_funding_history",
        )
        if payload.get("retCode") != 0:
            raise RuntimeError(f"Bybit funding history request failed: retCode={payload.get('retCode')}")

        result = payload.get("result")
        if not isinstance(result, Mapping):
            raise RuntimeError("Bybit funding history result must be an object")

        rows = result.get("list")
        if not isinstance(rows, list) or not rows:
            raise RuntimeError("Bybit funding history list must be a non-empty list")

        points: List[BybitFundingRatePoint] = []
        for row in rows:
            if not isinstance(row, Mapping):
                raise RuntimeError("Bybit funding history row must be an object")
            ts_ms = self._to_int(row.get("fundingRateTimestamp"), "bybit_funding_history.fundingRateTimestamp")
            funding_rate = self._to_decimal(row.get("fundingRate"), "bybit_funding_history.fundingRate")
            points.append(BybitFundingRatePoint(timestamp_ms=ts_ms, funding_rate=funding_rate))

        points_sorted = sorted(points, key=lambda x: x.timestamp_ms)
        self._validate_increasing_timestamps(
            [x.timestamp_ms for x in points_sorted],
            "bybit_funding_history",
        )
        return points_sorted

    # ========================================================
    # OKX
    # ========================================================

    def _fetch_okx_open_interest(self) -> OkxOpenInterestPoint:
        payload = self._request_json(
            base_url=_OKX_BASE_URL,
            path="/api/v5/public/open-interest",
            params={
                "instType": "SWAP",
                "instId": self._okx_inst_id,
            },
            source_name="okx_open_interest",
        )

        code = payload.get("code")
        if code != "0":
            raise RuntimeError(f"OKX open interest request failed: code={code!r}")

        rows = payload.get("data")
        if not isinstance(rows, list) or not rows:
            raise RuntimeError("OKX open interest data must be a non-empty list")

        row = rows[0]
        if not isinstance(row, Mapping):
            raise RuntimeError("OKX open interest row must be an object")

        ts_ms = self._to_int(row.get("ts"), "okx_open_interest.ts")
        oi_contracts = self._to_decimal(row.get("oi"), "okx_open_interest.oi")

        oi_usd_raw = row.get("oiUsd")
        oi_usd = None if oi_usd_raw in (None, "") else self._to_decimal(oi_usd_raw, "okx_open_interest.oiUsd")

        return OkxOpenInterestPoint(
            timestamp_ms=ts_ms,
            open_interest_contracts=oi_contracts,
            open_interest_usd=oi_usd,
        )

    def _fetch_okx_funding_history(self) -> List[OkxFundingRatePoint]:
        payload = self._request_json(
            base_url=_OKX_BASE_URL,
            path="/api/v5/public/funding-rate-history",
            params={
                "instId": self._okx_inst_id,
                "limit": _OKX_FUNDING_LIMIT,
            },
            source_name="okx_funding_history",
        )

        code = payload.get("code")
        if code != "0":
            raise RuntimeError(f"OKX funding history request failed: code={code!r}")

        rows = payload.get("data")
        if not isinstance(rows, list) or not rows:
            raise RuntimeError("OKX funding history data must be a non-empty list")

        points: List[OkxFundingRatePoint] = []
        for row in rows:
            if not isinstance(row, Mapping):
                raise RuntimeError("OKX funding history row must be an object")
            ts_ms = self._to_int(row.get("fundingTime"), "okx_funding_history.fundingTime")
            funding_rate = self._to_decimal(row.get("fundingRate"), "okx_funding_history.fundingRate")
            points.append(OkxFundingRatePoint(timestamp_ms=ts_ms, funding_rate=funding_rate))

        points_sorted = sorted(points, key=lambda x: x.timestamp_ms)
        self._validate_increasing_timestamps(
            [x.timestamp_ms for x in points_sorted],
            "okx_funding_history",
        )
        return points_sorted

    # ========================================================
    # Inference
    # ========================================================

    def _classify_crowding_bias(
        self,
        *,
        bybit_latest_funding: Decimal,
        okx_latest_funding: Decimal,
        bybit_oi_change_pct: Decimal,
    ) -> str:
        positive_count = int(bybit_latest_funding > Decimal("0")) + int(okx_latest_funding > Decimal("0"))
        negative_count = int(bybit_latest_funding < Decimal("0")) + int(okx_latest_funding < Decimal("0"))

        if positive_count == 2 and bybit_oi_change_pct > Decimal("0"):
            return "long_crowded"
        if negative_count == 2 and bybit_oi_change_pct > Decimal("0"):
            return "short_crowded"
        if bybit_oi_change_pct <= Decimal("0"):
            return "deleveraging"
        return "mixed"

    def _build_key_signals(
        self,
        *,
        bybit_oi_change_pct: Decimal,
        bybit_latest_funding: Decimal,
        okx_latest_funding: Decimal,
        funding_rate_spread: Decimal,
        crowding_bias: str,
    ) -> List[str]:
        signals: List[str] = []

        if bybit_oi_change_pct > Decimal("1.0"):
            signals.append("open_interest_rising")
        elif bybit_oi_change_pct < Decimal("-1.0"):
            signals.append("open_interest_falling")
        else:
            signals.append("open_interest_flat")

        if bybit_latest_funding > Decimal("0") and okx_latest_funding > Decimal("0"):
            signals.append("cross_exchange_positive_funding")
        elif bybit_latest_funding < Decimal("0") and okx_latest_funding < Decimal("0"):
            signals.append("cross_exchange_negative_funding")
        else:
            signals.append("cross_exchange_mixed_funding")

        if abs(funding_rate_spread) >= Decimal("0.0001"):
            signals.append("funding_divergence_widened")
        else:
            signals.append("funding_divergence_stable")

        signals.append(f"crowding_bias:{crowding_bias}")
        return signals

    # ========================================================
    # Helpers
    # ========================================================

    def _request_json(
        self,
        *,
        base_url: str,
        path: str,
        params: Mapping[str, Any],
        source_name: str,
    ) -> Mapping[str, Any]:
        url = f"{base_url}{path}"
        response = self._session.get(url, params=dict(params), timeout=self._timeout_sec)

        if response.status_code != 200:
            raise RuntimeError(
                f"{source_name} request failed: status_code={response.status_code}, url={url}"
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(f"{source_name} response is not valid JSON") from exc

        if not isinstance(payload, Mapping):
            raise RuntimeError(f"{source_name} response root must be an object")
        return payload

    def _normalize_bybit_symbol(self, symbol: str) -> str:
        symbol_norm = symbol.strip().upper()
        if not symbol_norm.endswith("USDT"):
            raise RuntimeError("DerivativesMarketFetcher only supports USDT linear symbols")
        return symbol_norm

    def _normalize_okx_inst_id(self, symbol: str) -> str:
        symbol_norm = symbol.strip().upper()
        if not symbol_norm.endswith("USDT"):
            raise RuntimeError("DerivativesMarketFetcher only supports USDT linear symbols")
        base = symbol_norm[:-4]
        if not base:
            raise RuntimeError("Invalid market symbol for OKX instId conversion")
        return f"{base}-USDT-SWAP"

    def _calc_change_pct(self, *, base: Decimal, latest: Decimal, field_name: str) -> Decimal:
        if base == Decimal("0"):
            raise RuntimeError(f"{field_name} base must not be 0")
        return ((latest - base) / base) * Decimal("100")

    def _validate_increasing_timestamps(self, timestamps: List[int], field_name: str) -> None:
        if not timestamps:
            raise RuntimeError(f"{field_name} timestamps must not be empty")
        prev: Optional[int] = None
        for ts in timestamps:
            if prev is not None and ts <= prev:
                raise RuntimeError(f"{field_name} timestamps must be strictly increasing")
            prev = ts

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

    def _to_int(self, value: Any, field_name: str) -> int:
        if isinstance(value, bool):
            raise RuntimeError(f"{field_name} must not be bool")
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Invalid int for {field_name}") from exc

    def _require_int(self, value: Any, field_name: str) -> int:
        if isinstance(value, bool):
            raise RuntimeError(f"{field_name} must not be bool")
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"{field_name} must be an integer") from exc
        return parsed

    def _require_nonempty_str(self, value: Any, field_name: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"{field_name} must be a non-empty string")
        return value.strip()

    def _fmt_decimal(self, value: Decimal, scale: int) -> str:
        return f"{value:.{scale}f}"

    def _fmt_decimal_optional(self, value: Optional[Decimal], scale: int) -> Optional[str]:
        if value is None:
            return None
        return self._fmt_decimal(value, scale)

    def _now_ms(self) -> int:
        ts_ms = int(time.time() * 1000)
        if ts_ms <= 0:
            raise RuntimeError("current timestamp must be > 0")
        return ts_ms

    def _require_str_setting(self, name: str) -> str:
        value = getattr(SETTINGS, name, None)
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"Missing or invalid required setting: {name}")
        return value.strip()

    def _require_float_setting(self, name: str) -> float:
        value = getattr(SETTINGS, name, None)
        if isinstance(value, bool):
            raise RuntimeError(f"Invalid bool value for float setting: {name}")
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Missing or invalid required float setting: {name}") from exc
        if not isinstance(parsed, float) or not (parsed > 0):
            raise RuntimeError(f"Invalid required float setting: {name}")
        return parsed


__all__ = [
    "DerivativesMarketFetcher",
    "DerivativesMarketSnapshot",
    "BybitOpenInterestPoint",
    "BybitFundingRatePoint",
    "OkxOpenInterestPoint",
    "OkxFundingRatePoint",
]