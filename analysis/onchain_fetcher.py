"""
========================================================
FILE: analysis/onchain_fetcher.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

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

변경 이력:
2026-03-08
1) 신규 생성
2) Blockchain.com ticker / charts API 기반 온체인 fetcher 추가
3) hash-rate / mempool-count / tx-per-second / estimated-volume-usd 요약 추가
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

_BLOCKCHAIN_TICKER_URL = "https://blockchain.info/ticker"
_BLOCKCHAIN_CHART_URL_TEMPLATE = "https://api.blockchain.info/charts/{chart_name}"
_TIMESPAN = "7days"


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
    """

    def __init__(self) -> None:
        self._symbol = self._require_str_setting("ANALYST_MARKET_SYMBOL")
        self._timeout_sec = self._require_float_setting("ANALYST_HTTP_TIMEOUT_SEC")

        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "auto-trader/onchain-fetcher"})

    def fetch(self) -> OnchainSnapshot:
        ticker_payload = self._request_json(_BLOCKCHAIN_TICKER_URL, params=None, source_name="blockchain_ticker")
        usd_quote = ticker_payload.get("USD")
        if not isinstance(usd_quote, Mapping):
            raise RuntimeError("Blockchain ticker USD quote must be an object")

        market_price_usd = self._to_decimal(usd_quote.get("last"), "blockchain_ticker.USD.last")

        hash_rate_points = self._fetch_chart("hash-rate")
        mempool_count_points = self._fetch_chart("mempool-count")
        txps_points = self._fetch_chart("transactions-per-second")
        volume_usd_points = self._fetch_chart("estimated-transaction-volume-usd")

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
            "market_price_usd": self._fmt_decimal(market_price_usd, 2),
            "hash_rate": self._fmt_decimal(hash_rate, 4),
            "mempool_count": self._fmt_decimal(mempool_count, 2),
            "transactions_per_second": self._fmt_decimal(transactions_per_second, 4),
            "estimated_volume_usd": self._fmt_decimal(estimated_volume_usd, 2),
            "network_regime": network_regime,
            "congestion_regime": congestion_regime,
            "key_signals": list(key_signals),
        }

        result = OnchainSnapshot(
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

        logger.info(
            "Onchain snapshot fetched: symbol=%s network_regime=%s congestion_regime=%s",
            result.symbol,
            result.network_regime,
            result.congestion_regime,
        )
        return result

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
    "OnchainFetcher",
    "OnchainSnapshot",
    "BlockchainChartPoint",
]