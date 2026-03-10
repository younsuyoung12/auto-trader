"""
========================================================
FILE: analysis/crypto_news_fetcher.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

핵심 변경 요약
- Alpha Vantage 뉴스 snapshot을 프로세스 메모리 캐시 + Postgres 영속 캐시로 이중 관리
- 서버 재시작 후에도 DB의 fresh 뉴스 snapshot을 재사용하도록 구조 유지
- 외부 공급자 실패 시 마지막 검증된 DB snapshot(last known good)을 재사용하도록 정책 기반 복원 추가
- 뉴스 TTL 최소값을 4시간으로 상향해 Alpha Vantage 무료 quota 초과 위험을 구조적으로 축소

코드 정리 내용
- 뉴스 snapshot 생성/직렬화/역직렬화 로직 분리 유지
- DB schema 보장, 저장, 조회 경로를 본 파일 내부로 통합 유지
- latest DB row 조회 / fresh 조회 / provider 실패 복원 경로를 분리해 책임 명확화

역할:
- Alpha Vantage NEWS_SENTIMENT 를 사용해 BTC 관련 최신 뉴스/감성 데이터를 수집한다.
- 외부 시장 분석(Market Research) 레이어의 뉴스 데이터 소스 역할만 수행한다.
- 주문/포지션/계정 관련 기능은 포함하지 않는다.

절대 원칙:
- STRICT · NO-FALLBACK
- settings.py(SSOT) 외 환경변수 직접 접근 금지
- print() 금지 / logging 사용
- 응답 누락/손상/모호성 발생 시 즉시 예외
- 민감정보 로그 금지
- 뉴스/감성 데이터는 외부 공개/공식 API로만 수집한다
- 임의값/더미값/추정값 fallback 금지
- 단, 외부 공급자 장애/쿼터 초과 시 마지막 검증된 DB snapshot 재사용은 운영 정책으로 허용한다
  (합성 fallback 아님 / persisted last known good reuse)

변경 이력:
2026-03-10
1) FIX(ROOT-CAUSE): TTL 만료 후 Alpha Vantage 실패 시 마지막 DB snapshot 재사용 경로 추가
2) FIX(STABILITY): provider 장애/쿼터 초과로 서버가 종료되지 않도록 정책 기반 last-known-good 복원 추가
3) FIX(QUOTA): 뉴스 cache TTL 최소 4시간 보장으로 과도 호출 방지

2026-03-09
1) FIX(ROOT-CAUSE): 프로세스 재시작 시 Alpha Vantage 뉴스 재호출로 quota가 누적되던 문제를 DB 영속 캐시로 수정
2) FIX(STRICT): DB의 fresh 뉴스 snapshot만 사용, stale snapshot 재사용 금지
3) FIX(STRUCTURE): news snapshot table 생성/저장/조회 책임을 fetcher 내부로 통합
4) FIX(LOG): cache source(memory/db/api) 구분 로그 추가
========================================================
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Mapping, Optional

import requests
from sqlalchemy import text

from settings import SETTINGS
from state.db_core import get_session

logger = logging.getLogger(__name__)

_ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"
_ALPHA_VANTAGE_FREE_DAILY_REQUEST_LIMIT = 25
_ALPHA_VANTAGE_MIN_REQUEST_INTERVAL_SEC = 1.25
_SECONDS_PER_DAY = 86400
_NEWS_LIMIT = 20
_SENTIMENT_BULLISH_THRESHOLD = Decimal("0.15")
_SENTIMENT_BEARISH_THRESHOLD = Decimal("-0.15")
_NEWS_MIN_CACHE_TTL_SEC = 14400  # 4시간

_NEWS_SNAPSHOT_TABLE = "analyst_crypto_news_snapshots"
_NEWS_SNAPSHOT_SOURCE_KEY = "alphavantage_news_sentiment_crypto_v1"


@dataclass(frozen=True)
class CryptoNewsItem:
    title: str
    source: str
    published_at_utc: str
    published_ts_ms: int
    url: str
    overall_sentiment_score: Decimal
    overall_sentiment_label: str
    summary: Optional[str]


@dataclass(frozen=True)
class CryptoNewsSnapshot:
    symbol: str
    as_of_ms: int
    average_sentiment_score: Decimal
    bullish_count: int
    bearish_count: int
    neutral_count: int
    sentiment_bias: str
    latest_news: List[CryptoNewsItem]
    key_signals: List[str]
    dashboard_payload: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class CryptoNewsFetcher:
    """
    Alpha Vantage NEWS_SENTIMENT 기반 BTC 뉴스 fetcher.
    """

    def __init__(self) -> None:
        self._symbol = self._require_str_setting("ANALYST_MARKET_SYMBOL")
        self._timeout_sec = self._require_float_setting("ANALYST_HTTP_TIMEOUT_SEC")
        self._api_key = self._require_alpha_vantage_api_key()
        self._cache_ttl_sec = self._build_required_cache_ttl_sec()

        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "auto-trader/crypto-news-fetcher"})

        self._base_asset = self._extract_base_asset(self._symbol)
        self._ticker_filter = f"CRYPTO:{self._base_asset}"

        self._state_lock = threading.RLock()
        self._cached_snapshot: CryptoNewsSnapshot | None = None
        self._cached_snapshot_fetched_monotonic: float | None = None
        self._last_alpha_vantage_request_monotonic: float | None = None
        self._db_schema_ready = False

        logger.info(
            "CryptoNewsFetcher initialized: symbol=%s ticker_filter=%s cache_ttl_sec=%d source_key=%s",
            self._symbol,
            self._ticker_filter,
            self._cache_ttl_sec,
            _NEWS_SNAPSHOT_SOURCE_KEY,
        )

    @property
    def symbol(self) -> str:
        return self._symbol

    def fetch(self) -> CryptoNewsSnapshot:
        with self._state_lock:
            cached = self._get_fresh_cached_snapshot_locked()
            if cached is not None:
                age_sec = self._get_cached_snapshot_age_sec_locked()
                if age_sec is None:
                    raise RuntimeError("Cached crypto news snapshot age missing")
                logger.info(
                    "Crypto news snapshot served from memory cache: symbol=%s age_sec=%.2f ttl_sec=%d",
                    self._symbol,
                    age_sec,
                    self._cache_ttl_sec,
                )
                return cached

            persisted = self._load_fresh_db_snapshot_locked()
            if persisted is not None:
                self._set_memory_cache_locked(persisted)
                logger.info(
                    "Crypto news snapshot served from DB cache: symbol=%s ttl_sec=%d as_of_ms=%d",
                    self._symbol,
                    self._cache_ttl_sec,
                    persisted.as_of_ms,
                )
                return persisted

            try:
                payload = self._request_json_locked(
                    params=self._build_news_request_params_strict()
                )
            except Exception as exc:
                fallback_snapshot, fallback_age_sec = self._load_latest_db_snapshot_any_age_locked()
                if fallback_snapshot is not None and fallback_age_sec is not None:
                    self._set_memory_cache_locked(fallback_snapshot)
                    logger.warning(
                        "Alpha Vantage news fetch failed; serving last persisted DB snapshot: symbol=%s fallback_age_sec=%.2f ttl_sec=%d error=%s",
                        self._symbol,
                        fallback_age_sec,
                        self._cache_ttl_sec,
                        str(exc),
                    )
                    return fallback_snapshot
                raise

            feed = payload.get("feed")
            if not isinstance(feed, list) or not feed:
                raise RuntimeError("Alpha Vantage news feed must be a non-empty list")

            news_items: List[CryptoNewsItem] = []
            for raw in feed:
                if not isinstance(raw, Mapping):
                    raise RuntimeError("Alpha Vantage news item must be an object")

                title = self._require_nonempty_str(raw.get("title"), "news.title")
                source = self._extract_source_title(raw)
                published_at_utc, published_ts_ms = self._parse_alpha_vantage_timestamp(
                    raw.get("time_published")
                )
                url = self._require_nonempty_str(raw.get("url"), "news.url")
                overall_sentiment_score = self._to_decimal(
                    raw.get("overall_sentiment_score"),
                    "news.overall_sentiment_score",
                )
                overall_sentiment_label = self._require_nonempty_str(
                    raw.get("overall_sentiment_label"),
                    "news.overall_sentiment_label",
                )
                summary = self._optional_str(raw.get("summary"))

                news_items.append(
                    CryptoNewsItem(
                        title=title,
                        source=source,
                        published_at_utc=published_at_utc,
                        published_ts_ms=published_ts_ms,
                        url=url,
                        overall_sentiment_score=overall_sentiment_score,
                        overall_sentiment_label=overall_sentiment_label,
                        summary=summary,
                    )
                )

            news_items_sorted = sorted(news_items, key=lambda x: x.published_ts_ms, reverse=True)
            fetched_at_ms = self._now_ms()
            snapshot = self._build_snapshot_from_news_items(
                news_items_sorted=news_items_sorted,
                fetched_at_ms=fetched_at_ms,
            )

            self._persist_snapshot_locked(snapshot=snapshot, fetched_at_ms=fetched_at_ms)
            self._set_memory_cache_locked(snapshot)

            logger.info(
                "Crypto news snapshot fetched from Alpha Vantage and persisted: symbol=%s ticker_filter=%s bias=%s avg_score=%s latest_news_count=%s cache_ttl_sec=%d",
                snapshot.symbol,
                self._ticker_filter,
                snapshot.sentiment_bias,
                self._fmt_decimal(snapshot.average_sentiment_score, 6),
                len(snapshot.latest_news),
                self._cache_ttl_sec,
            )
            return snapshot

    def _build_news_request_params_strict(self) -> Dict[str, Any]:
        if not self._ticker_filter.startswith("CRYPTO:"):
            raise RuntimeError(f"Invalid Alpha Vantage ticker filter: {self._ticker_filter}")

        return {
            "function": "NEWS_SENTIMENT",
            "tickers": self._ticker_filter,
            "sort": "LATEST",
            "limit": _NEWS_LIMIT,
            "apikey": self._api_key,
        }

    def _request_json_locked(self, params: Mapping[str, Any]) -> Mapping[str, Any]:
        self._sleep_before_alpha_vantage_request_locked()

        response = self._session.get(
            _ALPHA_VANTAGE_BASE_URL,
            params=dict(params),
            timeout=self._timeout_sec,
        )
        self._last_alpha_vantage_request_monotonic = time.monotonic()

        if response.status_code != 200:
            raise RuntimeError(f"Alpha Vantage news request failed: status_code={response.status_code}")

        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError("Alpha Vantage news response is not valid JSON") from exc

        if not isinstance(payload, Mapping):
            raise RuntimeError("Alpha Vantage news response root must be an object")

        if "Error Message" in payload:
            detail = self._sanitize_alpha_vantage_detail(str(payload["Error Message"]))
            raise RuntimeError(f"Alpha Vantage news error: {detail}")

        if "Information" in payload and "feed" not in payload:
            detail = self._sanitize_alpha_vantage_detail(str(payload["Information"]))
            raise RuntimeError(f"Alpha Vantage news information: {detail}")

        if "Note" in payload and "feed" not in payload:
            detail = self._sanitize_alpha_vantage_detail(str(payload["Note"]))
            raise RuntimeError(f"Alpha Vantage news note: {detail}")

        return payload

    def _build_snapshot_from_news_items(
        self,
        *,
        news_items_sorted: List[CryptoNewsItem],
        fetched_at_ms: int,
    ) -> CryptoNewsSnapshot:
        if not news_items_sorted:
            raise RuntimeError("news_items_sorted must not be empty")

        as_of_ms = news_items_sorted[0].published_ts_ms

        bullish_count = 0
        bearish_count = 0
        neutral_count = 0
        score_sum = Decimal("0")

        for item in news_items_sorted:
            score_sum += item.overall_sentiment_score
            if item.overall_sentiment_score >= _SENTIMENT_BULLISH_THRESHOLD:
                bullish_count += 1
            elif item.overall_sentiment_score <= _SENTIMENT_BEARISH_THRESHOLD:
                bearish_count += 1
            else:
                neutral_count += 1

        average_sentiment_score = score_sum / Decimal(len(news_items_sorted))
        sentiment_bias = self._classify_sentiment_bias(
            average_score=average_sentiment_score,
            bullish_count=bullish_count,
            bearish_count=bearish_count,
        )
        key_signals = self._build_key_signals(
            sentiment_bias=sentiment_bias,
            bullish_count=bullish_count,
            bearish_count=bearish_count,
            average_sentiment_score=average_sentiment_score,
        )

        latest_news = list(news_items_sorted[:8])

        dashboard_payload = {
            "symbol": self._symbol,
            "as_of_ms": as_of_ms,
            "fetched_at_ms": fetched_at_ms,
            "ticker_filter": self._ticker_filter,
            "average_sentiment_score": self._fmt_decimal(average_sentiment_score, 6),
            "bullish_count": bullish_count,
            "bearish_count": bearish_count,
            "neutral_count": neutral_count,
            "sentiment_bias": sentiment_bias,
            "latest_news": [
                {
                    "title": item.title,
                    "source": item.source,
                    "published_at_utc": item.published_at_utc,
                    "url": item.url,
                    "overall_sentiment_score": self._fmt_decimal(item.overall_sentiment_score, 6),
                    "overall_sentiment_label": item.overall_sentiment_label,
                }
                for item in latest_news
            ],
            "key_signals": list(key_signals),
        }

        return CryptoNewsSnapshot(
            symbol=self._symbol,
            as_of_ms=as_of_ms,
            average_sentiment_score=average_sentiment_score,
            bullish_count=bullish_count,
            bearish_count=bearish_count,
            neutral_count=neutral_count,
            sentiment_bias=sentiment_bias,
            latest_news=latest_news,
            key_signals=key_signals,
            dashboard_payload=dashboard_payload,
        )

    def _get_fresh_cached_snapshot_locked(self) -> CryptoNewsSnapshot | None:
        if self._cached_snapshot is None:
            return None
        if self._cached_snapshot_fetched_monotonic is None:
            raise RuntimeError("Cached crypto news snapshot timestamp missing")

        age_sec = time.monotonic() - self._cached_snapshot_fetched_monotonic
        if age_sec < 0:
            raise RuntimeError("Cached crypto news snapshot age must not be negative")
        if age_sec >= self._cache_ttl_sec:
            logger.info(
                "Crypto news memory cache expired: symbol=%s age_sec=%.2f ttl_sec=%d",
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
            raise RuntimeError("Cached crypto news snapshot age must not be negative")
        return age_sec

    def _set_memory_cache_locked(self, snapshot: CryptoNewsSnapshot) -> None:
        self._cached_snapshot = snapshot
        self._cached_snapshot_fetched_monotonic = time.monotonic()

    def _load_fresh_db_snapshot_locked(self) -> CryptoNewsSnapshot | None:
        row = self._load_latest_db_row_locked()
        if row is None:
            logger.info("No persisted crypto news snapshot found in DB: symbol=%s", self._symbol)
            return None

        snapshot = self._snapshot_from_db_row(row)
        fetched_at_ms = self._require_int(row.get("fetched_at_ms"), "db.fetched_at_ms")
        age_sec = self._compute_age_sec_from_fetched_at_ms(fetched_at_ms)
        if age_sec >= self._cache_ttl_sec:
            logger.info(
                "Crypto news DB cache expired: symbol=%s age_sec=%.2f ttl_sec=%d fetched_at_ms=%d",
                self._symbol,
                age_sec,
                self._cache_ttl_sec,
                fetched_at_ms,
            )
            return None

        return snapshot

    def _load_latest_db_snapshot_any_age_locked(self) -> tuple[CryptoNewsSnapshot | None, float | None]:
        row = self._load_latest_db_row_locked()
        if row is None:
            logger.warning(
                "No persisted crypto news snapshot available for provider-failure recovery: symbol=%s",
                self._symbol,
            )
            return None, None

        snapshot = self._snapshot_from_db_row(row)
        fetched_at_ms = self._require_int(row.get("fetched_at_ms"), "db.fetched_at_ms")
        age_sec = self._compute_age_sec_from_fetched_at_ms(fetched_at_ms)

        logger.warning(
            "Recovered last persisted crypto news snapshot for provider-failure recovery: symbol=%s age_sec=%.2f fetched_at_ms=%d",
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
                    FROM {_NEWS_SNAPSHOT_TABLE}
                    WHERE source_key = :source_key
                      AND symbol = :symbol
                    ORDER BY fetched_at_ms DESC, id DESC
                    LIMIT 1
                    """
                ),
                {
                    "source_key": _NEWS_SNAPSHOT_SOURCE_KEY,
                    "symbol": self._symbol,
                },
            ).mappings().first()

        return row

    def _persist_snapshot_locked(self, *, snapshot: CryptoNewsSnapshot, fetched_at_ms: int) -> None:
        self._ensure_db_schema_locked()

        snapshot_payload = self._snapshot_to_storage_payload(snapshot)
        snapshot_payload_json = json.dumps(snapshot_payload, ensure_ascii=False, separators=(",", ":"))

        with get_session() as session:
            session.execute(
                text(
                    f"""
                    INSERT INTO {_NEWS_SNAPSHOT_TABLE} (
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
                    "source_key": _NEWS_SNAPSHOT_SOURCE_KEY,
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
                    CREATE TABLE IF NOT EXISTS {_NEWS_SNAPSHOT_TABLE} (
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
                    CREATE INDEX IF NOT EXISTS idx_{_NEWS_SNAPSHOT_TABLE}_source_symbol_fetched
                    ON {_NEWS_SNAPSHOT_TABLE} (source_key, symbol, fetched_at_ms DESC, id DESC)
                    """
                )
            )

        self._db_schema_ready = True
        logger.info("Crypto news DB schema ensured: table=%s", _NEWS_SNAPSHOT_TABLE)

    def _snapshot_to_storage_payload(self, snapshot: CryptoNewsSnapshot) -> Dict[str, Any]:
        return {
            "symbol": snapshot.symbol,
            "as_of_ms": snapshot.as_of_ms,
            "average_sentiment_score": self._fmt_decimal(snapshot.average_sentiment_score, 10),
            "bullish_count": snapshot.bullish_count,
            "bearish_count": snapshot.bearish_count,
            "neutral_count": snapshot.neutral_count,
            "sentiment_bias": snapshot.sentiment_bias,
            "latest_news": [
                self._news_item_to_storage_payload(item)
                for item in snapshot.latest_news
            ],
            "key_signals": list(snapshot.key_signals),
            "dashboard_payload": snapshot.dashboard_payload,
        }

    def _snapshot_from_db_row(self, row: Mapping[str, Any]) -> CryptoNewsSnapshot:
        source_key = self._require_nonempty_str(row.get("source_key"), "db.source_key")
        if source_key != _NEWS_SNAPSHOT_SOURCE_KEY:
            raise RuntimeError(
                f"Persisted crypto news snapshot source_key mismatch: expected={_NEWS_SNAPSHOT_SOURCE_KEY}, got={source_key}"
            )

        symbol = self._require_nonempty_str(row.get("symbol"), "db.symbol").upper()
        if symbol != self._symbol:
            raise RuntimeError(
                f"Persisted crypto news snapshot symbol mismatch: expected={self._symbol}, got={symbol}"
            )

        snapshot_as_of_ms = self._require_int(row.get("snapshot_as_of_ms"), "db.snapshot_as_of_ms")
        if snapshot_as_of_ms <= 0:
            raise RuntimeError("db.snapshot_as_of_ms must be positive")

        cache_ttl_sec = self._require_int(row.get("cache_ttl_sec"), "db.cache_ttl_sec")
        if cache_ttl_sec != self._cache_ttl_sec:
            logger.info(
                "Persisted crypto news snapshot ttl differs from runtime ttl: persisted=%d runtime=%d",
                cache_ttl_sec,
                self._cache_ttl_sec,
            )

        payload = self._coerce_json_object(row.get("snapshot_payload"), "db.snapshot_payload")

        payload_symbol = self._require_nonempty_str(payload.get("symbol"), "snapshot_payload.symbol").upper()
        if payload_symbol != self._symbol:
            raise RuntimeError(
                f"Persisted crypto news payload symbol mismatch: expected={self._symbol}, got={payload_symbol}"
            )

        payload_as_of_ms = self._require_int(payload.get("as_of_ms"), "snapshot_payload.as_of_ms")
        if payload_as_of_ms != snapshot_as_of_ms:
            raise RuntimeError(
                f"Persisted crypto news as_of_ms mismatch: row={snapshot_as_of_ms}, payload={payload_as_of_ms}"
            )

        average_sentiment_score = self._to_decimal(
            payload.get("average_sentiment_score"),
            "snapshot_payload.average_sentiment_score",
        )
        bullish_count = self._require_int(payload.get("bullish_count"), "snapshot_payload.bullish_count")
        bearish_count = self._require_int(payload.get("bearish_count"), "snapshot_payload.bearish_count")
        neutral_count = self._require_int(payload.get("neutral_count"), "snapshot_payload.neutral_count")
        sentiment_bias = self._require_nonempty_str(payload.get("sentiment_bias"), "snapshot_payload.sentiment_bias")

        raw_latest_news = payload.get("latest_news")
        if not isinstance(raw_latest_news, list) or not raw_latest_news:
            raise RuntimeError("snapshot_payload.latest_news must be a non-empty list")
        latest_news = [
            self._news_item_from_storage_payload(item, f"snapshot_payload.latest_news[{idx}]")
            for idx, item in enumerate(raw_latest_news)
        ]

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

        return CryptoNewsSnapshot(
            symbol=self._symbol,
            as_of_ms=snapshot_as_of_ms,
            average_sentiment_score=average_sentiment_score,
            bullish_count=bullish_count,
            bearish_count=bearish_count,
            neutral_count=neutral_count,
            sentiment_bias=sentiment_bias,
            latest_news=latest_news,
            key_signals=key_signals,
            dashboard_payload=dict(dashboard_payload),
        )

    def _news_item_to_storage_payload(self, item: CryptoNewsItem) -> Dict[str, Any]:
        return {
            "title": item.title,
            "source": item.source,
            "published_at_utc": item.published_at_utc,
            "published_ts_ms": item.published_ts_ms,
            "url": item.url,
            "overall_sentiment_score": self._fmt_decimal(item.overall_sentiment_score, 10),
            "overall_sentiment_label": item.overall_sentiment_label,
            "summary": item.summary,
        }

    def _news_item_from_storage_payload(self, payload: Any, field_name: str) -> CryptoNewsItem:
        if not isinstance(payload, Mapping):
            raise RuntimeError(f"{field_name} must be an object")

        summary = payload.get("summary")
        summary_text: Optional[str]
        if summary is None:
            summary_text = None
        else:
            summary_text = self._optional_str(summary)

        return CryptoNewsItem(
            title=self._require_nonempty_str(payload.get("title"), f"{field_name}.title"),
            source=self._require_nonempty_str(payload.get("source"), f"{field_name}.source"),
            published_at_utc=self._require_nonempty_str(payload.get("published_at_utc"), f"{field_name}.published_at_utc"),
            published_ts_ms=self._require_int(payload.get("published_ts_ms"), f"{field_name}.published_ts_ms"),
            url=self._require_nonempty_str(payload.get("url"), f"{field_name}.url"),
            overall_sentiment_score=self._to_decimal(
                payload.get("overall_sentiment_score"),
                f"{field_name}.overall_sentiment_score",
            ),
            overall_sentiment_label=self._require_nonempty_str(
                payload.get("overall_sentiment_label"),
                f"{field_name}.overall_sentiment_label",
            ),
            summary=summary_text,
        )

    def _compute_age_sec_from_fetched_at_ms(self, fetched_at_ms: int) -> float:
        now_ms = self._now_ms()
        age_ms = now_ms - fetched_at_ms
        if age_ms < 0:
            raise RuntimeError(
                f"Crypto news snapshot fetched_at_ms is in the future: now_ms={now_ms}, fetched_at_ms={fetched_at_ms}"
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

    def _sleep_before_alpha_vantage_request_locked(self) -> None:
        if _ALPHA_VANTAGE_MIN_REQUEST_INTERVAL_SEC <= 0:
            raise RuntimeError("Invalid Alpha Vantage request interval")

        if self._last_alpha_vantage_request_monotonic is None:
            logger.info(
                "Alpha Vantage news pacing skip (first request): symbol=%s",
                self._symbol,
            )
            return

        elapsed_sec = time.monotonic() - self._last_alpha_vantage_request_monotonic
        if elapsed_sec < 0:
            raise RuntimeError("Alpha Vantage news request elapsed time must not be negative")

        remain_sec = _ALPHA_VANTAGE_MIN_REQUEST_INTERVAL_SEC - elapsed_sec
        if remain_sec <= 0:
            logger.info(
                "Alpha Vantage news pacing skip: symbol=%s elapsed_sec=%.4f min_interval_sec=%.2f",
                self._symbol,
                elapsed_sec,
                _ALPHA_VANTAGE_MIN_REQUEST_INTERVAL_SEC,
            )
            return

        logger.info(
            "Alpha Vantage news pacing sleep: symbol=%s sleep_sec=%.4f",
            self._symbol,
            remain_sec,
        )
        time.sleep(remain_sec)

    def _build_required_cache_ttl_sec(self) -> int:
        ttl_sec = _SECONDS_PER_DAY // _ALPHA_VANTAGE_FREE_DAILY_REQUEST_LIMIT
        if ttl_sec * _ALPHA_VANTAGE_FREE_DAILY_REQUEST_LIMIT < _SECONDS_PER_DAY:
            ttl_sec += 1
        if ttl_sec <= 0:
            raise RuntimeError("Computed crypto news cache TTL must be positive")

        ttl_sec = max(ttl_sec, _NEWS_MIN_CACHE_TTL_SEC)
        return ttl_sec

    def _sanitize_alpha_vantage_detail(self, detail: str) -> str:
        text = str(detail or "").strip()
        if not text:
            return "empty detail"
        redacted = text.replace(self._api_key, "[REDACTED_API_KEY]")
        if len(redacted) > 500:
            return redacted[:500] + "...(truncated)"
        return redacted

    def _extract_source_title(self, item: Mapping[str, Any]) -> str:
        source = item.get("source")
        if not isinstance(source, str) or not source.strip():
            raise RuntimeError("news.source must be a non-empty string")
        return source.strip()

    def _parse_alpha_vantage_timestamp(self, raw: Any) -> tuple[str, int]:
        if not isinstance(raw, str) or not raw.strip():
            raise RuntimeError("news.time_published must be a non-empty string")
        text = raw.strip()
        try:
            dt = datetime.strptime(text, "%Y%m%dT%H%M%S")
        except ValueError as exc:
            raise RuntimeError("Invalid Alpha Vantage time_published format") from exc
        dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z"), int(dt.timestamp() * 1000)

    def _extract_base_asset(self, symbol: str) -> str:
        symbol_norm = symbol.strip().upper()
        if not symbol_norm.endswith("USDT"):
            raise RuntimeError("CryptoNewsFetcher only supports USDT quote symbols")
        base = symbol_norm[:-4]
        if not base:
            raise RuntimeError("Invalid market symbol")
        return base

    def _classify_sentiment_bias(
        self,
        *,
        average_score: Decimal,
        bullish_count: int,
        bearish_count: int,
    ) -> str:
        if bullish_count > bearish_count and average_score >= _SENTIMENT_BULLISH_THRESHOLD:
            return "bullish"
        if bearish_count > bullish_count and average_score <= _SENTIMENT_BEARISH_THRESHOLD:
            return "bearish"
        return "mixed"

    def _build_key_signals(
        self,
        *,
        sentiment_bias: str,
        bullish_count: int,
        bearish_count: int,
        average_sentiment_score: Decimal,
    ) -> List[str]:
        signals = [f"news_sentiment_bias:{sentiment_bias}"]

        if bullish_count > bearish_count:
            signals.append("bullish_headlines_dominant")
        elif bearish_count > bullish_count:
            signals.append("bearish_headlines_dominant")
        else:
            signals.append("headline_balance_mixed")

        if average_sentiment_score >= _SENTIMENT_BULLISH_THRESHOLD:
            signals.append("average_sentiment_positive")
        elif average_sentiment_score <= _SENTIMENT_BEARISH_THRESHOLD:
            signals.append("average_sentiment_negative")
        else:
            signals.append("average_sentiment_neutral")

        return signals

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

    def _optional_str(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        if not isinstance(value, str):
            value = str(value)
        value_norm = value.strip()
        return value_norm if value_norm else None

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

    def _require_alpha_vantage_api_key(self) -> str:
        value = getattr(SETTINGS, "ALPHAVANTAGE_API_KEY", None)
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError("Missing or invalid required setting: ALPHAVANTAGE_API_KEY")
        return value.strip()


__all__ = [
    "CryptoNewsFetcher",
    "CryptoNewsSnapshot",
    "CryptoNewsItem",
]