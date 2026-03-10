"""
========================================================
FILE: analysis/onchain_fetcher.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

핵심 변경 요약
- Blockchain.com 온체인 snapshot을 프로세스 메모리 캐시 + Postgres 영속 캐시로 이중 관리
- 서버 재시작 후에도 DB의 fresh snapshot을 재사용하도록 구조 유지
- 외부 공급자 실패 시 마지막 검증된 DB snapshot(last known good)을 재사용하도록 정책 기반 복원 추가
- stale DB/메모리 캐시의 일반 재사용은 금지하고, provider failure 시에만 persisted snapshot 복원을 허용

코드 정리 내용
- snapshot 생성/직렬화/역직렬화 로직 분리 유지
- DB schema 보장, 저장, 조회 경로를 본 파일 내부로 통합 유지
- latest DB row 조회 / fresh 조회 / provider 실패 복원 경로를 분리해 책임 명확화

역할:
- Blockchain.com 공개 API를 사용해 BTC 온체인/네트워크 상태 데이터를 수집한다.
- 외부 시장 분석(Market Research) 레이어의 온체인 데이터 소스 역할만 수행한다.
- 주문/포지션/계정 관련 기능은 포함하지 않는다.

절대 원칙:
- STRICT · NO-FALLBACK
- settings.py(SSOT) 외 환경변수 직접 접근 금지
- print() 금지 / logging 사용
- 응답 누락/손상/모호성 발생 시 즉시 예외
- 민감정보 로그 금지
- 공개 API만 사용
- 임의값/더미값/추정값 fallback 금지
- 단, 외부 공급자 장애 시 마지막 검증된 DB snapshot 재사용은 운영 정책으로 허용한다
  (합성 fallback 아님 / persisted last known good reuse)

변경 이력:
2026-03-10
1) FIX(ROOT-CAUSE): TTL 만료 후 Blockchain.com API 실패 시 마지막 DB snapshot 재사용 경로 추가
2) FIX(STABILITY): provider 장애로 서버가 종료되지 않도록 정책 기반 last-known-good 복원 추가
3) FIX(STRUCTURE): latest DB row 조회 / fresh 조회 / provider failure recovery 경로 분리

2026-03-09
1) FIX(ROOT-CAUSE): 온체인 snapshot DB 영속 캐시 추가
2) FIX(STRICT): DB의 fresh snapshot만 사용, stale snapshot 재사용 금지
3) FIX(STRUCTURE): onchain snapshot table 생성/저장/조회 책임을 fetcher 내부로 통합
4) FIX(LOG): cache source(memory/db/api) 구분 로그 추가
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

_BLOCKCHAIN_TICKER_URL = "https://blockchain.info/ticker"
_BLOCKCHAIN_CHART_URL_TEMPLATE = "https://api.blockchain.info/charts/{chart_name}"
_TIMESPAN = "7days"

_ONCHAIN_CACHE_TTL_SEC = 1800
_ONCHAIN_SNAPSHOT_TABLE = "analyst_onchain_snapshots"
_ONCHAIN_SNAPSHOT_SOURCE_KEY = "blockchain_com_onchain_v1"


@dataclass(frozen=True)
class BlockchainChartPoint:
    timestamp_ms: int
    value: Decimal


@dataclass(frozen=True)
class OnchainSnapshot:
    symbol: str
    as_of_ms: int
    market_price_usd: Decimal
    hash_rate: Decimal
    mempool_count: Decimal
    transactions_per_second: Decimal
    estimated_volume_usd: Decimal
    network_regime: str
    congestion_regime: str
    key_signals: List[str]
    dashboard_payload: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class OnchainFetcher:
    """
    Blockchain.com 공개 API 기반 온체인 fetcher.

    설계:
    - fetch()는 메모리 캐시 → DB 영속 캐시 → 외부 API 순으로만 조회한다.
    - DB 영속 캐시는 주 소스이며, TTL 안의 fresh snapshot만 허용한다.
    - stale snapshot의 일반 재사용은 금지한다.
    - 단, 외부 provider 실패 시 마지막 검증된 DB snapshot(last known good)은 운영 정책으로 재사용한다.
    """

    def __init__(self) -> None:
        self._symbol = self._require_str_setting("ANALYST_MARKET_SYMBOL")
        self._timeout_sec = self._require_float_setting("ANALYST_HTTP_TIMEOUT_SEC")
        self._cache_ttl_sec = _ONCHAIN_CACHE_TTL_SEC

        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "auto-trader/onchain-fetcher"})

        self._state_lock = threading.RLock()
        self._cached_snapshot: OnchainSnapshot | None = None
        self._cached_snapshot_fetched_monotonic: float | None = None
        self._db_schema_ready = False

        logger.info(
            "OnchainFetcher initialized: symbol=%s cache_ttl_sec=%d source_key=%s",
            self._symbol,
            self._cache_ttl_sec,
            _ONCHAIN_SNAPSHOT_SOURCE_KEY,
        )

    def fetch(self) -> OnchainSnapshot:
        with self._state_lock:
            cached = self._get_fresh_cached_snapshot_locked()
            if cached is not None:
                age_sec = self._get_cached_snapshot_age_sec_locked()
                if age_sec is None:
                    raise RuntimeError("Cached onchain snapshot age missing")
                logger.info(
                    "Onchain snapshot served from memory cache: symbol=%s age_sec=%.2f ttl_sec=%d",
                    self._symbol,
                    age_sec,
                    self._cache_ttl_sec,
                )
                return cached

            persisted = self._load_fresh_db_snapshot_locked()
            if persisted is not None:
                self._set_memory_cache_locked(persisted)
                logger.info(
                    "Onchain snapshot served from DB cache: symbol=%s ttl_sec=%d as_of_ms=%d",
                    self._symbol,
                    self._cache_ttl_sec,
                    persisted.as_of_ms,
                )
                return persisted

            try:
                ticker_payload = self._request_json(
                    _BLOCKCHAIN_TICKER_URL,
                    params=None,
                    source_name="blockchain_ticker",
                )
                usd_quote = ticker_payload.get("USD")
                if not isinstance(usd_quote, Mapping):
                    raise RuntimeError("Blockchain ticker USD quote must be an object")

                market_price_usd = self._to_decimal(usd_quote.get("last"), "blockchain_ticker.USD.last")

                hash_rate_points = self._fetch_chart("hash-rate")
                mempool_count_points = self._fetch_chart("mempool-count")
                txps_points = self._fetch_chart("transactions-per-second")
                volume_usd_points = self._fetch_chart("estimated-transaction-volume-usd")
            except Exception as exc:
                fallback_snapshot, fallback_age_sec = self._load_latest_db_snapshot_any_age_locked()
                if fallback_snapshot is not None and fallback_age_sec is not None:
                    self._set_memory_cache_locked(fallback_snapshot)
                    logger.warning(
                        "Onchain provider fetch failed; serving last persisted DB snapshot: symbol=%s fallback_age_sec=%.2f ttl_sec=%d error=%s",
                        self._symbol,
                        fallback_age_sec,
                        self._cache_ttl_sec,
                        str(exc),
                    )
                    return fallback_snapshot
                raise

            fetched_at_ms = self._now_ms()
            snapshot = self._build_snapshot_from_points(
                market_price_usd=market_price_usd,
                hash_rate_points=hash_rate_points,
                mempool_count_points=mempool_count_points,
                txps_points=txps_points,
                volume_usd_points=volume_usd_points,
                fetched_at_ms=fetched_at_ms,
            )

            self._persist_snapshot_locked(snapshot=snapshot, fetched_at_ms=fetched_at_ms)
            self._set_memory_cache_locked(snapshot)

            logger.info(
                "Onchain snapshot fetched from APIs and persisted: symbol=%s network_regime=%s congestion_regime=%s cache_ttl_sec=%d",
                snapshot.symbol,
                snapshot.network_regime,
                snapshot.congestion_regime,
                self._cache_ttl_sec,
            )
            return snapshot

    def _build_snapshot_from_points(
        self,
        *,
        market_price_usd: Decimal,
        hash_rate_points: List[BlockchainChartPoint],
        mempool_count_points: List[BlockchainChartPoint],
        txps_points: List[BlockchainChartPoint],
        volume_usd_points: List[BlockchainChartPoint],
        fetched_at_ms: int,
    ) -> OnchainSnapshot:
        hash_rate = hash_rate_points[-1].value
        mempool_count = mempool_count_points[-1].value
        transactions_per_second = txps_points[-1].value
        estimated_volume_usd = volume_usd_points[-1].value

        network_regime = self._classify_network_regime(
            hash_rate_points=hash_rate_points,
            txps_points=txps_points,
        )
        congestion_regime = self._classify_congestion_regime(
            mempool_count_points=mempool_count_points,
        )
        as_of_ms = max(
            hash_rate_points[-1].timestamp_ms,
            mempool_count_points[-1].timestamp_ms,
            txps_points[-1].timestamp_ms,
            volume_usd_points[-1].timestamp_ms,
        )

        key_signals = self._build_key_signals(
            network_regime=network_regime,
            congestion_regime=congestion_regime,
            hash_rate=hash_rate,
            mempool_count=mempool_count,
            txps=transactions_per_second,
            volume_usd=estimated_volume_usd,
        )

        dashboard_payload = {
            "symbol": self._symbol,
            "as_of_ms": as_of_ms,
            "fetched_at_ms": fetched_at_ms,
            "market_price_usd": self._fmt_decimal(market_price_usd, 2),
            "hash_rate": self._fmt_decimal(hash_rate, 4),
            "mempool_count": self._fmt_decimal(mempool_count, 2),
            "transactions_per_second": self._fmt_decimal(transactions_per_second, 4),
            "estimated_volume_usd": self._fmt_decimal(estimated_volume_usd, 2),
            "network_regime": network_regime,
            "congestion_regime": congestion_regime,
            "key_signals": list(key_signals),
        }

        return OnchainSnapshot(
            symbol=self._symbol,
            as_of_ms=as_of_ms,
            market_price_usd=market_price_usd,
            hash_rate=hash_rate,
            mempool_count=mempool_count,
            transactions_per_second=transactions_per_second,
            estimated_volume_usd=estimated_volume_usd,
            network_regime=network_regime,
            congestion_regime=congestion_regime,
            key_signals=key_signals,
            dashboard_payload=dashboard_payload,
        )

    def _get_fresh_cached_snapshot_locked(self) -> OnchainSnapshot | None:
        if self._cached_snapshot is None:
            return None
        if self._cached_snapshot_fetched_monotonic is None:
            raise RuntimeError("Cached onchain snapshot timestamp missing")

        age_sec = time.monotonic() - self._cached_snapshot_fetched_monotonic
        if age_sec < 0:
            raise RuntimeError("Cached onchain snapshot age must not be negative")
        if age_sec >= self._cache_ttl_sec:
            logger.info(
                "Onchain memory cache expired: symbol=%s age_sec=%.2f ttl_sec=%d",
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
            raise RuntimeError("Cached onchain snapshot age must not be negative")
        return age_sec

    def _set_memory_cache_locked(self, snapshot: OnchainSnapshot) -> None:
        self._cached_snapshot = snapshot
        self._cached_snapshot_fetched_monotonic = time.monotonic()

    def _load_fresh_db_snapshot_locked(self) -> OnchainSnapshot | None:
        row = self._load_latest_db_row_locked()
        if row is None:
            logger.info("No persisted onchain snapshot found in DB: symbol=%s", self._symbol)
            return None

        snapshot = self._snapshot_from_db_row(row)
        fetched_at_ms = self._require_int(row.get("fetched_at_ms"), "db.fetched_at_ms")
        age_sec = self._compute_age_sec_from_fetched_at_ms(fetched_at_ms)
        if age_sec >= self._cache_ttl_sec:
            logger.info(
                "Onchain DB cache expired: symbol=%s age_sec=%.2f ttl_sec=%d fetched_at_ms=%d",
                self._symbol,
                age_sec,
                self._cache_ttl_sec,
                fetched_at_ms,
            )
            return None

        return snapshot

    def _load_latest_db_snapshot_any_age_locked(self) -> tuple[OnchainSnapshot | None, float | None]:
        row = self._load_latest_db_row_locked()
        if row is None:
            logger.warning(
                "No persisted onchain snapshot available for provider-failure recovery: symbol=%s",
                self._symbol,
            )
            return None, None

        snapshot = self._snapshot_from_db_row(row)
        fetched_at_ms = self._require_int(row.get("fetched_at_ms"), "db.fetched_at_ms")
        age_sec = self._compute_age_sec_from_fetched_at_ms(fetched_at_ms)

        logger.warning(
            "Recovered last persisted onchain snapshot for provider-failure recovery: symbol=%s age_sec=%.2f fetched_at_ms=%d",
            self._symbol,
            age_sec,
            fetched_at_ms,
        )
        return snapshot, age_sec

    def _load_latest_db_row_locked(self) -> Mapping[str, Any] | None:
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
                    FROM {_ONCHAIN_SNAPSHOT_TABLE}
                    WHERE source_key = :source_key
                      AND symbol = :symbol
                    ORDER BY fetched_at_ms DESC, id DESC
                    LIMIT 1
                    """
                ),
                {
                    "source_key": _ONCHAIN_SNAPSHOT_SOURCE_KEY,
                    "symbol": self._symbol,
                },
            ).mappings().first()

        return row

    def _persist_snapshot_locked(self, *, snapshot: OnchainSnapshot, fetched_at_ms: int) -> None:
        self._ensure_db_schema_locked()

        snapshot_payload = self._snapshot_to_storage_payload(snapshot)
        snapshot_payload_json = json.dumps(snapshot_payload, ensure_ascii=False, separators=(",", ":"))

        with get_session() as session:
            session.execute(
                text(
                    f"""
                    INSERT INTO {_ONCHAIN_SNAPSHOT_TABLE} (
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
                    "source_key": _ONCHAIN_SNAPSHOT_SOURCE_KEY,
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
                    CREATE TABLE IF NOT EXISTS {_ONCHAIN_SNAPSHOT_TABLE} (
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
                    CREATE INDEX IF NOT EXISTS idx_{_ONCHAIN_SNAPSHOT_TABLE}_source_symbol_fetched
                    ON {_ONCHAIN_SNAPSHOT_TABLE} (source_key, symbol, fetched_at_ms DESC, id DESC)
                    """
                )
            )

        self._db_schema_ready = True
        logger.info("Onchain DB schema ensured: table=%s", _ONCHAIN_SNAPSHOT_TABLE)

    def _snapshot_to_storage_payload(self, snapshot: OnchainSnapshot) -> Dict[str, Any]:
        return {
            "symbol": snapshot.symbol,
            "as_of_ms": snapshot.as_of_ms,
            "market_price_usd": self._fmt_decimal(snapshot.market_price_usd, 10),
            "hash_rate": self._fmt_decimal(snapshot.hash_rate, 10),
            "mempool_count": self._fmt_decimal(snapshot.mempool_count, 10),
            "transactions_per_second": self._fmt_decimal(snapshot.transactions_per_second, 10),
            "estimated_volume_usd": self._fmt_decimal(snapshot.estimated_volume_usd, 10),
            "network_regime": snapshot.network_regime,
            "congestion_regime": snapshot.congestion_regime,
            "key_signals": list(snapshot.key_signals),
            "dashboard_payload": snapshot.dashboard_payload,
        }

    def _snapshot_from_db_row(self, row: Mapping[str, Any]) -> OnchainSnapshot:
        source_key = self._require_nonempty_str(row.get("source_key"), "db.source_key")
        if source_key != _ONCHAIN_SNAPSHOT_SOURCE_KEY:
            raise RuntimeError(
                f"Persisted onchain snapshot source_key mismatch: expected={_ONCHAIN_SNAPSHOT_SOURCE_KEY}, got={source_key}"
            )

        symbol = self._require_nonempty_str(row.get("symbol"), "db.symbol").upper()
        if symbol != self._symbol:
            raise RuntimeError(
                f"Persisted onchain snapshot symbol mismatch: expected={self._symbol}, got={symbol}"
            )

        snapshot_as_of_ms = self._require_int(row.get("snapshot_as_of_ms"), "db.snapshot_as_of_ms")
        if snapshot_as_of_ms <= 0:
            raise RuntimeError("db.snapshot_as_of_ms must be positive")

        cache_ttl_sec = self._require_int(row.get("cache_ttl_sec"), "db.cache_ttl_sec")
        if cache_ttl_sec != self._cache_ttl_sec:
            logger.info(
                "Persisted onchain snapshot ttl differs from runtime ttl: persisted=%d runtime=%d",
                cache_ttl_sec,
                self._cache_ttl_sec,
            )

        payload = self._coerce_json_object(row.get("snapshot_payload"), "db.snapshot_payload")

        payload_symbol = self._require_nonempty_str(payload.get("symbol"), "snapshot_payload.symbol").upper()
        if payload_symbol != self._symbol:
            raise RuntimeError(
                f"Persisted onchain payload symbol mismatch: expected={self._symbol}, got={payload_symbol}"
            )

        payload_as_of_ms = self._require_int(payload.get("as_of_ms"), "snapshot_payload.as_of_ms")
        if payload_as_of_ms != snapshot_as_of_ms:
            raise RuntimeError(
                f"Persisted onchain as_of_ms mismatch: row={snapshot_as_of_ms}, payload={payload_as_of_ms}"
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

        return OnchainSnapshot(
            symbol=self._symbol,
            as_of_ms=snapshot_as_of_ms,
            market_price_usd=self._to_decimal(payload.get("market_price_usd"), "snapshot_payload.market_price_usd"),
            hash_rate=self._to_decimal(payload.get("hash_rate"), "snapshot_payload.hash_rate"),
            mempool_count=self._to_decimal(payload.get("mempool_count"), "snapshot_payload.mempool_count"),
            transactions_per_second=self._to_decimal(
                payload.get("transactions_per_second"),
                "snapshot_payload.transactions_per_second",
            ),
            estimated_volume_usd=self._to_decimal(
                payload.get("estimated_volume_usd"),
                "snapshot_payload.estimated_volume_usd",
            ),
            network_regime=self._require_nonempty_str(payload.get("network_regime"), "snapshot_payload.network_regime"),
            congestion_regime=self._require_nonempty_str(
                payload.get("congestion_regime"),
                "snapshot_payload.congestion_regime",
            ),
            key_signals=key_signals,
            dashboard_payload=dict(dashboard_payload),
        )

    def _compute_age_sec_from_fetched_at_ms(self, fetched_at_ms: int) -> float:
        now_ms = self._now_ms()
        age_ms = now_ms - fetched_at_ms
        if age_ms < 0:
            raise RuntimeError(
                f"Onchain snapshot fetched_at_ms is in the future: now_ms={now_ms}, fetched_at_ms={fetched_at_ms}"
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

    def _fetch_chart(self, chart_name: str) -> List[BlockchainChartPoint]:
        payload = self._request_json(
            _BLOCKCHAIN_CHART_URL_TEMPLATE.format(chart_name=chart_name),
            params={
                "timespan": _TIMESPAN,
                "format": "json",
                "sampled": "true",
            },
            source_name=f"blockchain_chart:{chart_name}",
        )

        values = payload.get("values")
        if not isinstance(values, list) or not values:
            raise RuntimeError(f"Blockchain chart values must be a non-empty list: {chart_name}")

        points: List[BlockchainChartPoint] = []
        for row in values:
            if not isinstance(row, Mapping):
                raise RuntimeError(f"Blockchain chart row must be an object: {chart_name}")
            timestamp_seconds = self._to_int(row.get("x"), f"{chart_name}.x")
            value = self._to_decimal(row.get("y"), f"{chart_name}.y")
            points.append(BlockchainChartPoint(timestamp_ms=timestamp_seconds * 1000, value=value))

        points_sorted = sorted(points, key=lambda x: x.timestamp_ms)
        self._validate_increasing_timestamps(points_sorted, chart_name)
        return points_sorted

    def _classify_network_regime(
        self,
        *,
        hash_rate_points: List[BlockchainChartPoint],
        txps_points: List[BlockchainChartPoint],
    ) -> str:
        hash_rate_change = self._calc_change_pct(hash_rate_points[0].value, hash_rate_points[-1].value, "hash_rate_change_pct")
        txps_change = self._calc_change_pct(txps_points[0].value, txps_points[-1].value, "txps_change_pct")

        if hash_rate_change > Decimal("1.0") and txps_change >= Decimal("0"):
            return "network_strengthening"
        if hash_rate_change < Decimal("-1.0") and txps_change < Decimal("0"):
            return "network_softening"
        return "network_mixed"

    def _classify_congestion_regime(
        self,
        *,
        mempool_count_points: List[BlockchainChartPoint],
    ) -> str:
        current = mempool_count_points[-1].value
        average = sum((p.value for p in mempool_count_points), Decimal("0")) / Decimal(len(mempool_count_points))
        if current > average * Decimal("1.20"):
            return "congested"
        if current < average * Decimal("0.80"):
            return "light"
        return "normal"

    def _build_key_signals(
        self,
        *,
        network_regime: str,
        congestion_regime: str,
        hash_rate: Decimal,
        mempool_count: Decimal,
        txps: Decimal,
        volume_usd: Decimal,
    ) -> List[str]:
        return [
            f"network_regime:{network_regime}",
            f"congestion_regime:{congestion_regime}",
            f"hash_rate:{self._fmt_decimal(hash_rate, 4)}",
            f"mempool_count:{self._fmt_decimal(mempool_count, 2)}",
            f"tx_per_second:{self._fmt_decimal(txps, 4)}",
            f"estimated_volume_usd:{self._fmt_decimal(volume_usd, 2)}",
        ]

    def _request_json(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None,
        source_name: str,
    ) -> Mapping[str, Any]:
        response = self._session.get(url, params=dict(params) if params is not None else None, timeout=self._timeout_sec)

        if response.status_code != 200:
            raise RuntimeError(f"{source_name} request failed: status_code={response.status_code}")

        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(f"{source_name} response is not valid JSON") from exc

        if not isinstance(payload, Mapping):
            raise RuntimeError(f"{source_name} response root must be an object")
        return payload

    def _calc_change_pct(self, base: Decimal, latest: Decimal, field_name: str) -> Decimal:
        if base == Decimal("0"):
            raise RuntimeError(f"{field_name} base must not be 0")
        return ((latest - base) / base) * Decimal("100")

    def _validate_increasing_timestamps(self, points: List[BlockchainChartPoint], field_name: str) -> None:
        if not points:
            raise RuntimeError(f"{field_name} points must not be empty")
        prev: int | None = None
        for point in points:
            if prev is not None and point.timestamp_ms <= prev:
                raise RuntimeError(f"{field_name} timestamps must be strictly increasing")
            prev = point.timestamp_ms

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
        if parsed <= 0:
            raise RuntimeError(f"Invalid required float setting: {name}")
        return parsed


__all__ = [
    "OnchainFetcher",
    "OnchainSnapshot",
    "BlockchainChartPoint",
]