"""
========================================================
FILE: analysis/derivatives_market_fetcher.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

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
2026-03-08
1) 신규 생성
2) Bybit / OKX 공개 파생시장 fetcher 추가
3) funding / open interest / exchange divergence 진단 추가
========================================================
"""

from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Mapping, Optional

import requests

from settings import SETTINGS

logger = logging.getLogger(__name__)

_BYBIT_BASE_URL = "https://api.bybit.com"
_OKX_BASE_URL = "https://www.okx.com"

_BYBIT_OPEN_INTEREST_LIMIT = 30
_BYBIT_FUNDING_LIMIT = 30
_OKX_FUNDING_LIMIT = 30


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
    """

    def __init__(self) -> None:
        self._symbol = self._require_str_setting("ANALYST_MARKET_SYMBOL")
        self._timeout_sec = self._require_float_setting("ANALYST_HTTP_TIMEOUT_SEC")

        if self._timeout_sec <= 0:
            raise RuntimeError("ANALYST_HTTP_TIMEOUT_SEC must be > 0")

        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "auto-trader/derivatives-market-fetcher"})

        self._bybit_symbol = self._normalize_bybit_symbol(self._symbol)
        self._okx_inst_id = self._normalize_okx_inst_id(self._symbol)

    @property
    def symbol(self) -> str:
        return self._symbol

    def fetch(self) -> DerivativesMarketSnapshot:
        bybit_oi_points = self._fetch_bybit_open_interest_history()
        bybit_funding_points = self._fetch_bybit_funding_history()
        okx_oi_point = self._fetch_okx_open_interest()
        okx_funding_points = self._fetch_okx_funding_history()

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

        result = DerivativesMarketSnapshot(
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

        logger.info(
            "Derivatives market snapshot fetched: symbol=%s bybit_funding=%s okx_funding=%s oi_change_pct=%s bias=%s",
            result.symbol,
            self._fmt_decimal(result.bybit_latest_funding_rate, 8),
            self._fmt_decimal(result.okx_latest_funding_rate, 8),
            self._fmt_decimal(result.bybit_open_interest_change_pct, 6),
            result.cross_exchange_crowding_bias,
        )
        return result

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

    def _fmt_decimal(self, value: Decimal, scale: int) -> str:
        return f"{value:.{scale}f}"

    def _fmt_decimal_optional(self, value: Optional[Decimal], scale: int) -> Optional[str]:
        if value is None:
            return None
        return self._fmt_decimal(value, scale)

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