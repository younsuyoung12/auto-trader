"""
========================================================
FILE: analysis/sentiment_fetcher.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

핵심 변경 요약
- ROOT-CAUSE FIX: Alternative.me 단발성 1회 호출 구조를 재시도 가능한 HTTP 세션 구조로 교체
- ROOT-CAUSE FIX: connect/read timeout 분리 및 requests 계층 예외를 명시적 RuntimeError로 정규화
- STRICT 강화: Fear & Greed 30개 히스토리 미충족/값 범위 이상/타임스탬프 이상 시 즉시 예외
- 응답 파싱/검증/요약 생성 로직을 단계별 메서드로 분리해 장애 지점 추적성 강화

코드 정리 내용
- 미사용 import(InvalidOperation) 제거
- fetch 본문에 섞여 있던 HTTP 요청/파싱/검증 로직 분리
- 불필요한 주석/중복 표현 정리

역할
- Alternative.me Fear & Greed Index를 사용해 BTC 외부 심리 데이터를 수집한다.
- 외부 시장 분석(Market Research) 레이어의 심리 데이터 소스 역할만 수행한다.
- 주문/포지션/계정 관련 기능은 포함하지 않는다.

절대 원칙
- STRICT · NO-FALLBACK
- settings.py(SSOT) 외 환경변수 직접 접근 금지
- print() 금지 / logging 사용
- 응답 누락/손상/모호성 발생 시 즉시 예외
- 민감정보 로그 금지
- 대시보드 표시 시 Alternative.me 출처 표기는 호출부에서 수행한다

변경 이력
--------------------------------------------------------
- 2026-03-10
  1) FIX(ROOT-CAUSE): 단발성 외부 호출을 Retry Session 구조로 교체
  2) FIX(ROOT-CAUSE): connect/read timeout 분리 및 requests 예외 정규화
  3) FIX(STRICT): Fear & Greed 30개 히스토리/값 범위/타임스탬프 검증 추가
  4) CLEANUP: HTTP/파싱/검증 로직 메서드 분리 및 미사용 import 제거

- 2026-03-09
  1) 유지: Fear & Greed current / average / trend 계산 구조
========================================================
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any, Dict, List, Mapping, Sequence

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from settings import SETTINGS

logger = logging.getLogger(__name__)

_ALTERNATIVE_ME_FNG_URL = "https://api.alternative.me/fng/"
_FNG_HISTORY_LIMIT = 30
_HTTP_RETRY_TOTAL = 2
_HTTP_RETRY_BACKOFF_SEC = 0.5
_HTTP_STATUS_FORCE_LIST = (429, 500, 502, 503, 504)
_HTTP_ALLOWED_METHODS = frozenset({"GET"})
_SENTIMENT_VALUE_MIN = 0
_SENTIMENT_VALUE_MAX = 100


@dataclass(frozen=True, slots=True)
class FearGreedPoint:
    timestamp_ms: int
    value: int
    classification: str


@dataclass(frozen=True, slots=True)
class SentimentSnapshot:
    symbol: str
    as_of_ms: int
    current_value: int
    current_classification: str
    average_7d: Decimal
    average_30d: Decimal
    trend: str
    sentiment_regime: str
    key_signals: List[str]
    dashboard_payload: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class SentimentFetcher:
    """
    Alternative.me Fear & Greed sentiment fetcher.
    """

    def __init__(self) -> None:
        self._symbol = self._require_str_setting("ANALYST_MARKET_SYMBOL")
        self._timeout_sec = self._require_float_setting("ANALYST_HTTP_TIMEOUT_SEC")
        self._session = self._build_retry_session()

    def fetch(self) -> SentimentSnapshot:
        payload = self._request_payload_or_raise()
        points_sorted = self._parse_points_or_raise(payload)
        current = points_sorted[0]

        avg_7d = self._average([Decimal(p.value) for p in points_sorted[:7]])
        avg_30d = self._average([Decimal(p.value) for p in points_sorted[:30]])

        trend = self._classify_trend(
            current_value=current.value,
            avg_7d=avg_7d,
            avg_30d=avg_30d,
        )
        sentiment_regime = self._classify_regime(current.value)
        key_signals = self._build_key_signals(
            current_value=current.value,
            current_classification=current.classification,
            avg_7d=avg_7d,
            avg_30d=avg_30d,
            trend=trend,
            sentiment_regime=sentiment_regime,
        )

        dashboard_payload = {
            "symbol": self._symbol,
            "as_of_ms": current.timestamp_ms,
            "current_value": current.value,
            "current_classification": current.classification,
            "average_7d": self._fmt_decimal(avg_7d, 2),
            "average_30d": self._fmt_decimal(avg_30d, 2),
            "trend": trend,
            "sentiment_regime": sentiment_regime,
            "key_signals": list(key_signals),
        }

        result = SentimentSnapshot(
            symbol=self._symbol,
            as_of_ms=current.timestamp_ms,
            current_value=current.value,
            current_classification=current.classification,
            average_7d=avg_7d,
            average_30d=avg_30d,
            trend=trend,
            sentiment_regime=sentiment_regime,
            key_signals=key_signals,
            dashboard_payload=dashboard_payload,
        )

        logger.info(
            "Sentiment snapshot fetched: symbol=%s value=%s regime=%s trend=%s as_of_ms=%s",
            result.symbol,
            result.current_value,
            result.sentiment_regime,
            result.trend,
            result.as_of_ms,
        )
        return result

    def _build_retry_session(self) -> requests.Session:
        retry = Retry(
            total=_HTTP_RETRY_TOTAL,
            connect=_HTTP_RETRY_TOTAL,
            read=_HTTP_RETRY_TOTAL,
            status=_HTTP_RETRY_TOTAL,
            backoff_factor=_HTTP_RETRY_BACKOFF_SEC,
            allowed_methods=_HTTP_ALLOWED_METHODS,
            status_forcelist=_HTTP_STATUS_FORCE_LIST,
            raise_on_status=False,
            respect_retry_after_header=True,
        )

        adapter = HTTPAdapter(max_retries=retry)

        session = requests.Session()
        session.headers.update({"User-Agent": "auto-trader/sentiment-fetcher"})
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _request_payload_or_raise(self) -> Mapping[str, Any]:
        timeout = self._build_timeout_tuple()

        try:
            response = self._session.get(
                _ALTERNATIVE_ME_FNG_URL,
                params={
                    "limit": _FNG_HISTORY_LIMIT,
                    "format": "json",
                },
                timeout=timeout,
            )
        except requests.RequestException as exc:
            raise RuntimeError(
                "Alternative.me request failed after retry exhaustion: "
                f"error_type={exc.__class__.__name__}"
            ) from exc

        if response.status_code != 200:
            raise RuntimeError(f"Alternative.me request failed: status_code={response.status_code}")

        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError("Alternative.me response is not valid JSON") from exc

        if not isinstance(payload, Mapping):
            raise RuntimeError("Alternative.me response root must be an object")

        return payload

    def _build_timeout_tuple(self) -> tuple[float, float]:
        connect_timeout = min(3.0, self._timeout_sec)
        read_timeout = self._timeout_sec
        return (connect_timeout, read_timeout)

    def _parse_points_or_raise(self, payload: Mapping[str, Any]) -> List[FearGreedPoint]:
        rows = payload.get("data")
        if not isinstance(rows, list):
            raise RuntimeError("Alternative.me data must be a list")
        if len(rows) < _FNG_HISTORY_LIMIT:
            raise RuntimeError(
                "Alternative.me history length insufficient: "
                f"required={_FNG_HISTORY_LIMIT} actual={len(rows)}"
            )

        points: List[FearGreedPoint] = []
        for index, row in enumerate(rows):
            if not isinstance(row, Mapping):
                raise RuntimeError(f"Alternative.me point must be an object: index={index}")

            value = self._to_int(row.get("value"), f"fng.value[{index}]")
            if value < _SENTIMENT_VALUE_MIN or value > _SENTIMENT_VALUE_MAX:
                raise RuntimeError(
                    f"fng.value[{index}] out of range: value={value} "
                    f"expected={_SENTIMENT_VALUE_MIN}..{_SENTIMENT_VALUE_MAX}"
                )

            classification = self._require_nonempty_str(
                row.get("value_classification"),
                f"fng.value_classification[{index}]",
            )

            ts_seconds = self._to_int(row.get("timestamp"), f"fng.timestamp[{index}]")
            if ts_seconds <= 0:
                raise RuntimeError(f"fng.timestamp[{index}] must be > 0")

            points.append(
                FearGreedPoint(
                    timestamp_ms=ts_seconds * 1000,
                    value=value,
                    classification=classification,
                )
            )

        points_sorted = sorted(points, key=lambda x: x.timestamp_ms, reverse=True)
        self._validate_points_order_or_raise(points_sorted)
        return points_sorted

    def _validate_points_order_or_raise(self, points_sorted: Sequence[FearGreedPoint]) -> None:
        if len(points_sorted) < _FNG_HISTORY_LIMIT:
            raise RuntimeError(
                "sorted sentiment history length insufficient: "
                f"required={_FNG_HISTORY_LIMIT} actual={len(points_sorted)}"
            )

        prev_ts: int | None = None
        for index, point in enumerate(points_sorted):
            if point.timestamp_ms <= 0:
                raise RuntimeError(f"sentiment timestamp_ms must be > 0: index={index}")
            if prev_ts is not None and point.timestamp_ms > prev_ts:
                raise RuntimeError(
                    "sentiment history sort validation failed: "
                    f"index={index} prev_ts={prev_ts} current_ts={point.timestamp_ms}"
                )
            prev_ts = point.timestamp_ms

    def _average(self, values: List[Decimal]) -> Decimal:
        if not values:
            raise RuntimeError("average input must not be empty")
        return sum(values, Decimal("0")) / Decimal(len(values))

    def _classify_regime(self, value: int) -> str:
        if value >= 75:
            return "extreme_greed"
        if value >= 56:
            return "greed"
        if value >= 45:
            return "neutral"
        if value >= 25:
            return "fear"
        return "extreme_fear"

    def _classify_trend(self, *, current_value: int, avg_7d: Decimal, avg_30d: Decimal) -> str:
        current_dec = Decimal(current_value)
        if current_dec > avg_7d and avg_7d >= avg_30d:
            return "improving"
        if current_dec < avg_7d and avg_7d <= avg_30d:
            return "deteriorating"
        return "mixed"

    def _build_key_signals(
        self,
        *,
        current_value: int,
        current_classification: str,
        avg_7d: Decimal,
        avg_30d: Decimal,
        trend: str,
        sentiment_regime: str,
    ) -> List[str]:
        return [
            f"fear_greed_value:{current_value}",
            f"fear_greed_classification:{current_classification.lower().replace(' ', '_')}",
            f"average_7d:{self._fmt_decimal(avg_7d, 2)}",
            f"average_30d:{self._fmt_decimal(avg_30d, 2)}",
            f"trend:{trend}",
            f"sentiment_regime:{sentiment_regime}",
        ]

    def _to_int(self, value: Any, field_name: str) -> int:
        if isinstance(value, bool):
            raise RuntimeError(f"{field_name} must not be bool")
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Invalid int for {field_name}") from exc

    def _require_nonempty_str(self, value: Any, field_name: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"{field_name} must be a non-empty string")
        return value.strip()

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


__all__ = [
    "SentimentFetcher",
    "SentimentSnapshot",
    "FearGreedPoint",
]