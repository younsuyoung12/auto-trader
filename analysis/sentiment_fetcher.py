"""
========================================================
FILE: analysis/sentiment_fetcher.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할:
- Alternative.me Fear & Greed Index를 사용해 BTC 외부 심리 데이터를 수집한다.
- 외부 시장 분석(Market Research) 레이어의 심리 데이터 소스 역할만 수행한다.
- 주문/포지션/계정 관련 기능은 포함하지 않는다.

절대 원칙:
- STRICT · NO-FALLBACK
- settings.py(SSOT) 외 환경변수 직접 접근 금지
- print() 금지 / logging 사용
- 응답 누락/손상/모호성 발생 시 즉시 예외
- 민감정보 로그 금지
- 대시보드 표시 시 Alternative.me 출처 표기는 호출부에서 수행한다

변경 이력:
2026-03-08
1) 신규 생성
2) Fear & Greed current / average / trend 계산 추가
========================================================
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Mapping

import requests

from settings import SETTINGS

logger = logging.getLogger(__name__)

_ALTERNATIVE_ME_FNG_URL = "https://api.alternative.me/fng/"
_FNG_HISTORY_LIMIT = 30


@dataclass(frozen=True)
class FearGreedPoint:
    timestamp_ms: int
    value: int
    classification: str


@dataclass(frozen=True)
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

        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "auto-trader/sentiment-fetcher"})

    def fetch(self) -> SentimentSnapshot:
        response = self._session.get(
            _ALTERNATIVE_ME_FNG_URL,
            params={
                "limit": _FNG_HISTORY_LIMIT,
                "format": "json",
            },
            timeout=self._timeout_sec,
        )

        if response.status_code != 200:
            raise RuntimeError(f"Alternative.me request failed: status_code={response.status_code}")

        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError("Alternative.me response is not valid JSON") from exc

        if not isinstance(payload, Mapping):
            raise RuntimeError("Alternative.me response root must be an object")

        rows = payload.get("data")
        if not isinstance(rows, list) or not rows:
            raise RuntimeError("Alternative.me data must be a non-empty list")

        points: List[FearGreedPoint] = []
        for row in rows:
            if not isinstance(row, Mapping):
                raise RuntimeError("Alternative.me point must be an object")

            value = self._to_int(row.get("value"), "fng.value")
            classification = self._require_nonempty_str(row.get("value_classification"), "fng.value_classification")
            ts_raw = row.get("timestamp")
            ts_seconds = self._to_int(ts_raw, "fng.timestamp")
            points.append(
                FearGreedPoint(
                    timestamp_ms=ts_seconds * 1000,
                    value=value,
                    classification=classification,
                )
            )

        points_sorted = sorted(points, key=lambda x: x.timestamp_ms, reverse=True)
        current = points_sorted[0]
        avg_7d = self._average([Decimal(p.value) for p in points_sorted[:7]])
        avg_30d = self._average([Decimal(p.value) for p in points_sorted[:30]])

        trend = self._classify_trend(current_value=current.value, avg_7d=avg_7d, avg_30d=avg_30d)
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
            "Sentiment snapshot fetched: symbol=%s value=%s regime=%s trend=%s",
            result.symbol,
            result.current_value,
            result.sentiment_regime,
            result.trend,
        )
        return result

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