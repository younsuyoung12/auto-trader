"""
========================================================
FILE: analysis/crypto_news_fetcher.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

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

변경 이력:
2026-03-08
1) 신규 생성
2) Alpha Vantage NEWS_SENTIMENT 기반 BTC 뉴스 fetcher 추가
3) headline / overall sentiment / source / published time strict 파싱 추가

2026-03-08 (PATCH 2)
1) FIX(TRADE-GRADE): Alpha Vantage NEWS_SENTIMENT 요청 파라미터를 최소 유효 계약으로 축소
2) tickers 를 "CRYPTO:{base_asset}" 단일 필터로 고정
3) topics / FOREX:USD 결합 제거
4) 기존 Invalid inputs 오류 제거 목적

2026-03-08 (PATCH 3)
1) FIX(ROOT-CAUSE): Alpha Vantage 무료 daily quota(25/day) 정합화용 뉴스 snapshot cache 추가
2) FIX(CONCURRENCY): 동시 호출 시 중복 외부 요청 방지 lock 추가
3) FIX(SECURITY): Alpha Vantage Information/Note 메시지 내 API key 노출 마스킹
4) FIX(STRICT): cache 만료 후 fetch 실패 시 stale cache fallback 금지
========================================================
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Mapping, Optional

import requests

from settings import SETTINGS

logger = logging.getLogger(__name__)

_ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"
_ALPHA_VANTAGE_FREE_DAILY_REQUEST_LIMIT = 25
_ALPHA_VANTAGE_MIN_REQUEST_INTERVAL_SEC = 1.25
_SECONDS_PER_DAY = 86400
_NEWS_LIMIT = 20
_SENTIMENT_BULLISH_THRESHOLD = Decimal("0.15")
_SENTIMENT_BEARISH_THRESHOLD = Decimal("-0.15")


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

        logger.info(
            "CryptoNewsFetcher initialized: symbol=%s ticker_filter=%s cache_ttl_sec=%d",
            self._symbol,
            self._ticker_filter,
            self._cache_ttl_sec,
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
                    "Crypto news snapshot served from cache: symbol=%s age_sec=%.2f ttl_sec=%d",
                    self._symbol,
                    age_sec,
                    self._cache_ttl_sec,
                )
                return cached

            payload = self._request_json_locked(
                params=self._build_news_request_params_strict()
            )

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

            dashboard_payload = {
                "symbol": self._symbol,
                "as_of_ms": as_of_ms,
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
                    for item in news_items_sorted[:8]
                ],
                "key_signals": list(key_signals),
            }

            result = CryptoNewsSnapshot(
                symbol=self._symbol,
                as_of_ms=as_of_ms,
                average_sentiment_score=average_sentiment_score,
                bullish_count=bullish_count,
                bearish_count=bearish_count,
                neutral_count=neutral_count,
                sentiment_bias=sentiment_bias,
                latest_news=news_items_sorted[:8],
                key_signals=key_signals,
                dashboard_payload=dashboard_payload,
            )

            self._cached_snapshot = result
            self._cached_snapshot_fetched_monotonic = time.monotonic()

            logger.info(
                "Crypto news snapshot fetched: symbol=%s ticker_filter=%s bias=%s avg_score=%s news_count=%s cache_ttl_sec=%d",
                result.symbol,
                self._ticker_filter,
                result.sentiment_bias,
                self._fmt_decimal(result.average_sentiment_score, 6),
                len(result.latest_news),
                self._cache_ttl_sec,
            )
            return result

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
                "Crypto news snapshot cache expired: symbol=%s age_sec=%.2f ttl_sec=%d",
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
            return

        logger.info(
            "Alpha Vantage news pacing sleep: symbol=%s sleep_sec=%.4f",
            self._symbol,
            remain_sec,
        )
        time.sleep(remain_sec)

    def _build_required_cache_ttl_sec(self) -> int:
        if _ALPHA_VANTAGE_FREE_DAILY_REQUEST_LIMIT <= 0:
            raise RuntimeError("Alpha Vantage daily request limit must be positive")

        ttl_sec = _SECONDS_PER_DAY // _ALPHA_VANTAGE_FREE_DAILY_REQUEST_LIMIT
        if ttl_sec * _ALPHA_VANTAGE_FREE_DAILY_REQUEST_LIMIT < _SECONDS_PER_DAY:
            ttl_sec += 1
        if ttl_sec <= 0:
            raise RuntimeError("Computed crypto news cache TTL must be positive")
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

    def _optional_str(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        if not isinstance(value, str):
            value = str(value)
        value_norm = value.strip()
        return value_norm if value_norm else None

    def _fmt_decimal(self, value: Decimal, scale: int) -> str:
        return f"{value:.{scale}f}"

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