"""
========================================================
FILE: analysis/binance_market_fetcher.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할:
- Binance USDⓈ-M Futures 공개 REST API에서 외부 시장 분석용 데이터를 수집한다.
- 내부 DB 분석과 분리된 "외부 시장 분석" 데이터 소스 역할만 수행한다.
- 주문/포지션/계정 관련 기능은 포함하지 않는다.

절대 원칙:
- STRICT · NO-FALLBACK
- settings.py(SSOT) 외 환경변수 직접 접근 금지
- print() 금지 / logging 사용
- 응답 누락/손상/모호성 발생 시 즉시 예외
- 민감정보 로그 금지
- 외부 시장 분석은 Binance 공개 시장 데이터만 사용

변경 이력:
2026-03-07
- 신규 생성
- Binance 외부 시장 분석용 fetcher 추가

2026-03-07 (PATCH)
1) /fapi/v1/allForceOrders 제거
2) Binance 공개 REST 정책과 충돌하는 force orders 수집 경로 정리
3) 공개 REST snapshot 수집에서 force orders를 네트워크 호출 대상에서 제외
4) force orders는 공개 REST 미지원 사실을 명시적으로 로깅하고 빈 리스트 반환

2026-03-08 (PATCH)
1) ratio 계열 endpoint 응답에서 symbol 필드를 요청 symbol 기준으로 정합화
2) openInterestHist 응답 파싱/누적 구조 오류 수정
3) ratio/openInterestHist timestamp 유효성 검증 추가
4) takerlongshortRatio 응답 스키마(buySellRatio/buyVol/sellVol) 전용 파서 추가
========================================================
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, List, Mapping, Optional, Sequence

import requests

from settings import SETTINGS

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BinanceBookTicker:
    symbol: str
    bid_price: Decimal
    bid_qty: Decimal
    ask_price: Decimal
    ask_qty: Decimal
    spread_abs: Decimal
    spread_bps: Decimal


@dataclass(frozen=True)
class BinanceDepthLevel:
    price: Decimal
    qty: Decimal


@dataclass(frozen=True)
class BinanceDepthSnapshot:
    symbol: str
    last_update_id: int
    bids: List[BinanceDepthLevel]
    asks: List[BinanceDepthLevel]


@dataclass(frozen=True)
class BinanceKline:
    open_time_ms: int
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal
    close_time_ms: int
    quote_asset_volume: Decimal
    trade_count: int
    taker_buy_base_volume: Decimal
    taker_buy_quote_volume: Decimal


@dataclass(frozen=True)
class BinancePremiumIndex:
    symbol: str
    mark_price: Decimal
    index_price: Decimal
    estimated_settle_price: Optional[Decimal]
    last_funding_rate: Decimal
    interest_rate: Optional[Decimal]
    next_funding_time_ms: int
    server_time_ms: Optional[int]


@dataclass(frozen=True)
class BinanceOpenInterest:
    symbol: str
    open_interest: Decimal
    time_ms: int


@dataclass(frozen=True)
class BinanceFundingRatePoint:
    symbol: str
    funding_rate: Decimal
    funding_time_ms: int
    mark_price: Optional[Decimal]


@dataclass(frozen=True)
class BinanceRatioPoint:
    symbol: str
    long_short_ratio: Decimal
    long_account: Optional[Decimal]
    short_account: Optional[Decimal]
    timestamp_ms: int


@dataclass(frozen=True)
class BinanceOpenInterestHistPoint:
    symbol: str
    sum_open_interest: Decimal
    sum_open_interest_value: Decimal
    timestamp_ms: int


@dataclass(frozen=True)
class BinanceForceOrder:
    symbol: str
    side: str
    price: Decimal
    average_price: Decimal
    quantity: Decimal
    filled_quantity: Decimal
    status: str
    time_ms: int


@dataclass(frozen=True)
class BinanceMarketSnapshot:
    symbol: str
    server_time_ms: int
    local_time_ms: int
    drift_ms: int
    premium_index: BinancePremiumIndex
    book_ticker: BinanceBookTicker
    depth: BinanceDepthSnapshot
    open_interest: BinanceOpenInterest
    klines: List[BinanceKline]
    funding_rates: List[BinanceFundingRatePoint]
    open_interest_history: List[BinanceOpenInterestHistPoint]
    global_long_short_account_ratio: List[BinanceRatioPoint]
    top_long_short_position_ratio: List[BinanceRatioPoint]
    taker_long_short_ratio: List[BinanceRatioPoint]
    force_orders: List[BinanceForceOrder]


class BinanceMarketFetcher:
    """
    Binance USDⓈ-M Futures 외부 시장 분석 데이터 fetcher.

    주의:
    - 공개 REST endpoint만 사용한다.
    - 인증/서명/계정 데이터는 다루지 않는다.
    - STRICT 정책상 필수 설정 누락, 응답 손상, 타입 불일치는 즉시 예외 처리한다.

    force_orders 정책:
    - Binance 공개 REST 기준으로 liquidation / force order 조회용 유지되는 endpoint가 없다.
    - /fapi/v1/allForceOrders 는 유지 중단된 경로다.
    - /fapi/v1/forceOrders 는 USER_DATA 이므로 본 fetcher 정책과 충돌한다.
    - 따라서 본 fetcher는 force_orders 필드를 유지하되, 공개 REST 스냅샷에서는 빈 리스트로 반환한다.
    """

    def __init__(self) -> None:
        self._base_url = self._require_str_setting("BINANCE_FUTURES_BASE_URL")
        self._symbol = self._require_str_setting("ANALYST_MARKET_SYMBOL")
        self._kline_interval = self._require_str_setting("ANALYST_KLINE_INTERVAL")
        self._kline_limit = self._require_int_setting("ANALYST_KLINE_LIMIT")
        self._http_timeout_sec = self._require_float_setting("ANALYST_HTTP_TIMEOUT_SEC")
        self._ratio_period = self._require_str_setting("ANALYST_RATIO_PERIOD")
        self._ratio_limit = self._require_int_setting("ANALYST_RATIO_LIMIT")
        self._depth_limit = self._require_int_setting("ANALYST_DEPTH_LIMIT")
        self._funding_limit = self._require_int_setting("ANALYST_FUNDING_LIMIT")
        self._force_order_limit = self._require_int_setting("ANALYST_FORCE_ORDER_LIMIT")
        self._max_server_drift_ms = self._require_int_setting("ANALYST_MAX_SERVER_DRIFT_MS")

        if self._kline_limit <= 0:
            raise RuntimeError("ANALYST_KLINE_LIMIT must be > 0")
        if self._ratio_limit <= 0:
            raise RuntimeError("ANALYST_RATIO_LIMIT must be > 0")
        if self._depth_limit <= 0:
            raise RuntimeError("ANALYST_DEPTH_LIMIT must be > 0")
        if self._funding_limit <= 0:
            raise RuntimeError("ANALYST_FUNDING_LIMIT must be > 0")
        if self._force_order_limit <= 0:
            raise RuntimeError("ANALYST_FORCE_ORDER_LIMIT must be > 0")
        if self._http_timeout_sec <= 0:
            raise RuntimeError("ANALYST_HTTP_TIMEOUT_SEC must be > 0")
        if self._max_server_drift_ms < 0:
            raise RuntimeError("ANALYST_MAX_SERVER_DRIFT_MS must be >= 0")

        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "auto-trader/ai-market-analyst"})
        self._force_orders_public_rest_unavailable_logged = False

    # ========================================================
    # Public API
    # ========================================================

    @property
    def symbol(self) -> str:
        return self._symbol

    def fetch_market_snapshot(self) -> BinanceMarketSnapshot:
        """
        외부 시장 분석용 종합 스냅샷 수집.

        반환 데이터:
        - server time / drift
        - premium index / funding
        - best bid/ask / spread
        - orderbook depth
        - open interest
        - klines
        - funding history
        - long/short ratios
        - force orders
          * 공개 REST 미지원으로 인해 본 fetcher에서는 빈 리스트로 반환
        """
        server_time_ms, local_time_ms, drift_ms = self.fetch_server_time_or_raise()

        premium_index = self.fetch_premium_index(self._symbol)
        book_ticker = self.fetch_book_ticker(self._symbol)
        depth = self.fetch_depth(self._symbol, self._depth_limit)
        open_interest = self.fetch_open_interest(self._symbol)
        klines = self.fetch_klines(
            symbol=self._symbol,
            interval=self._kline_interval,
            limit=self._kline_limit,
        )
        funding_rates = self.fetch_funding_rate_history(
            symbol=self._symbol,
            limit=self._funding_limit,
        )
        open_interest_history = self.fetch_open_interest_history(
            symbol=self._symbol,
            period=self._ratio_period,
            limit=self._ratio_limit,
        )
        global_ratio = self.fetch_global_long_short_account_ratio(
            symbol=self._symbol,
            period=self._ratio_period,
            limit=self._ratio_limit,
        )
        top_ratio = self.fetch_top_long_short_position_ratio(
            symbol=self._symbol,
            period=self._ratio_period,
            limit=self._ratio_limit,
        )
        taker_ratio = self.fetch_taker_long_short_ratio(
            symbol=self._symbol,
            period=self._ratio_period,
            limit=self._ratio_limit,
        )
        force_orders = self.fetch_force_orders(
            symbol=self._symbol,
            limit=self._force_order_limit,
        )

        return BinanceMarketSnapshot(
            symbol=self._symbol,
            server_time_ms=server_time_ms,
            local_time_ms=local_time_ms,
            drift_ms=drift_ms,
            premium_index=premium_index,
            book_ticker=book_ticker,
            depth=depth,
            open_interest=open_interest,
            klines=klines,
            funding_rates=funding_rates,
            open_interest_history=open_interest_history,
            global_long_short_account_ratio=global_ratio,
            top_long_short_position_ratio=top_ratio,
            taker_long_short_ratio=taker_ratio,
            force_orders=force_orders,
        )

    def fetch_server_time_or_raise(self) -> tuple[int, int, int]:
        local_time_ms = self._now_ms()
        payload = self._request_json("GET", "/fapi/v1/time", params=None)

        if not isinstance(payload, Mapping):
            raise RuntimeError("Binance /fapi/v1/time response is not an object")

        server_time_ms = self._parse_int(payload, "serverTime")
        drift_ms = server_time_ms - local_time_ms

        if abs(drift_ms) > self._max_server_drift_ms:
            raise RuntimeError(
                f"Binance server time drift too large: drift_ms={drift_ms}, "
                f"max_allowed_ms={self._max_server_drift_ms}"
            )

        return server_time_ms, local_time_ms, drift_ms

    def fetch_premium_index(self, symbol: str) -> BinancePremiumIndex:
        payload = self._request_json(
            "GET",
            "/fapi/v1/premiumIndex",
            params={"symbol": symbol},
        )
        if not isinstance(payload, Mapping):
            raise RuntimeError("Binance /fapi/v1/premiumIndex response is not an object")

        estimated_settle_price = self._parse_optional_decimal(payload, "estimatedSettlePrice")
        interest_rate = self._parse_optional_decimal(payload, "interestRate")
        server_time_ms = self._parse_optional_int(payload, "time")

        return BinancePremiumIndex(
            symbol=self._parse_str(payload, "symbol"),
            mark_price=self._parse_decimal(payload, "markPrice"),
            index_price=self._parse_decimal(payload, "indexPrice"),
            estimated_settle_price=estimated_settle_price,
            last_funding_rate=self._parse_decimal(payload, "lastFundingRate"),
            interest_rate=interest_rate,
            next_funding_time_ms=self._parse_int(payload, "nextFundingTime"),
            server_time_ms=server_time_ms,
        )

    def fetch_book_ticker(self, symbol: str) -> BinanceBookTicker:
        payload = self._request_json(
            "GET",
            "/fapi/v1/ticker/bookTicker",
            params={"symbol": symbol},
        )
        if not isinstance(payload, Mapping):
            raise RuntimeError("Binance /fapi/v1/ticker/bookTicker response is not an object")

        bid_price = self._parse_decimal(payload, "bidPrice")
        ask_price = self._parse_decimal(payload, "askPrice")
        if bid_price <= Decimal("0") or ask_price <= Decimal("0"):
            raise RuntimeError("Invalid bid/ask price from Binance bookTicker")
        if ask_price < bid_price:
            raise RuntimeError("askPrice < bidPrice in Binance bookTicker response")

        spread_abs = ask_price - bid_price
        mid = (ask_price + bid_price) / Decimal("2")
        if mid <= Decimal("0"):
            raise RuntimeError("Invalid mid price derived from Binance bookTicker")
        spread_bps = (spread_abs / mid) * Decimal("10000")

        return BinanceBookTicker(
            symbol=self._parse_str(payload, "symbol"),
            bid_price=bid_price,
            bid_qty=self._parse_decimal(payload, "bidQty"),
            ask_price=ask_price,
            ask_qty=self._parse_decimal(payload, "askQty"),
            spread_abs=spread_abs,
            spread_bps=spread_bps,
        )

    def fetch_depth(self, symbol: str, limit: int) -> BinanceDepthSnapshot:
        payload = self._request_json(
            "GET",
            "/fapi/v1/depth",
            params={"symbol": symbol, "limit": limit},
        )
        if not isinstance(payload, Mapping):
            raise RuntimeError("Binance /fapi/v1/depth response is not an object")

        bids_raw = self._require_list(payload, "bids")
        asks_raw = self._require_list(payload, "asks")

        if not bids_raw:
            raise RuntimeError("Binance depth bids is empty")
        if not asks_raw:
            raise RuntimeError("Binance depth asks is empty")

        bids = [self._parse_depth_level(item, side="bid") for item in bids_raw]
        asks = [self._parse_depth_level(item, side="ask") for item in asks_raw]

        return BinanceDepthSnapshot(
            symbol=symbol,
            last_update_id=self._parse_int(payload, "lastUpdateId"),
            bids=bids,
            asks=asks,
        )

    def fetch_klines(self, symbol: str, interval: str, limit: int) -> List[BinanceKline]:
        payload = self._request_json(
            "GET",
            "/fapi/v1/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
        )
        if not isinstance(payload, list):
            raise RuntimeError("Binance /fapi/v1/klines response is not a list")
        if not payload:
            raise RuntimeError("Binance klines response is empty")

        return [self._parse_kline(row) for row in payload]

    def fetch_open_interest(self, symbol: str) -> BinanceOpenInterest:
        payload = self._request_json(
            "GET",
            "/fapi/v1/openInterest",
            params={"symbol": symbol},
        )
        if not isinstance(payload, Mapping):
            raise RuntimeError("Binance /fapi/v1/openInterest response is not an object")

        return BinanceOpenInterest(
            symbol=self._parse_str(payload, "symbol"),
            open_interest=self._parse_decimal(payload, "openInterest"),
            time_ms=self._parse_int(payload, "time"),
        )

    def fetch_funding_rate_history(self, symbol: str, limit: int) -> List[BinanceFundingRatePoint]:
        payload = self._request_json(
            "GET",
            "/fapi/v1/fundingRate",
            params={"symbol": symbol, "limit": limit},
        )
        if not isinstance(payload, list):
            raise RuntimeError("Binance /fapi/v1/fundingRate response is not a list")
        if not payload:
            raise RuntimeError("Binance fundingRate response is empty")

        result: List[BinanceFundingRatePoint] = []
        for row in payload:
            if not isinstance(row, Mapping):
                raise RuntimeError("Binance fundingRate row is not an object")
            result.append(
                BinanceFundingRatePoint(
                    symbol=symbol,
                    funding_rate=self._parse_decimal(row, "fundingRate"),
                    funding_time_ms=self._parse_int(row, "fundingTime"),
                    mark_price=self._parse_optional_decimal(row, "markPrice"),
                )
            )
        return result

    def fetch_open_interest_history(
        self,
        symbol: str,
        period: str,
        limit: int,
    ) -> List[BinanceOpenInterestHistPoint]:
        payload = self._request_json(
            "GET",
            "/futures/data/openInterestHist",
            params={"symbol": symbol, "period": period, "limit": limit},
        )
        if not isinstance(payload, list):
            raise RuntimeError("Binance /futures/data/openInterestHist response is not a list")
        if not payload:
            raise RuntimeError("Binance openInterestHist response is empty")

        result: List[BinanceOpenInterestHistPoint] = []
        for row in payload:
            if not isinstance(row, Mapping):
                raise RuntimeError("Binance openInterestHist row is not an object")

            ts = self._parse_int(row, "timestamp")
            if ts <= 0:
                raise RuntimeError("Invalid timestamp from Binance openInterestHist")

            result.append(
                BinanceOpenInterestHistPoint(
                    symbol=symbol,
                    sum_open_interest=self._parse_decimal(row, "sumOpenInterest"),
                    sum_open_interest_value=self._parse_decimal(row, "sumOpenInterestValue"),
                    timestamp_ms=ts,
                )
            )

        return result

    def fetch_global_long_short_account_ratio(
        self,
        symbol: str,
        period: str,
        limit: int,
    ) -> List[BinanceRatioPoint]:
        payload = self._request_json(
            "GET",
            "/futures/data/globalLongShortAccountRatio",
            params={"symbol": symbol, "period": period, "limit": limit},
        )
        return self._parse_standard_ratio_series(
            payload=payload,
            endpoint="/futures/data/globalLongShortAccountRatio",
            symbol=symbol,
        )

    def fetch_top_long_short_position_ratio(
        self,
        symbol: str,
        period: str,
        limit: int,
    ) -> List[BinanceRatioPoint]:
        payload = self._request_json(
            "GET",
            "/futures/data/topLongShortPositionRatio",
            params={"symbol": symbol, "period": period, "limit": limit},
        )
        return self._parse_standard_ratio_series(
            payload=payload,
            endpoint="/futures/data/topLongShortPositionRatio",
            symbol=symbol,
        )

    def fetch_taker_long_short_ratio(
        self,
        symbol: str,
        period: str,
        limit: int,
    ) -> List[BinanceRatioPoint]:
        payload = self._request_json(
            "GET",
            "/futures/data/takerlongshortRatio",
            params={"symbol": symbol, "period": period, "limit": limit},
        )
        return self._parse_taker_ratio_series(
            payload=payload,
            endpoint="/futures/data/takerlongshortRatio",
            symbol=symbol,
        )

    def fetch_force_orders(self, symbol: str, limit: int) -> List[BinanceForceOrder]:
        """
        force orders / liquidation orders 수집.

        중요:
        - 본 fetcher는 "공개 REST만 사용"이 절대 원칙이다.
        - Binance 공개 REST 기준으로 liquidation / force orders 를 안정적으로 조회할
          유지되는 endpoint가 없다.
        - /fapi/v1/allForceOrders 는 유지 중단된 경로다.
        - /fapi/v1/forceOrders 는 USER_DATA 이므로 정책 위반이다.

        따라서:
        - 네트워크 호출을 수행하지 않는다.
        - 사실 기반으로 빈 리스트를 반환한다.
        - 이 필드는 "공개 REST transport 에서는 미수집" 상태를 의미한다.
        """
        if not isinstance(symbol, str) or not symbol.strip():
            raise RuntimeError("symbol must be a non-empty string")
        if limit <= 0:
            raise RuntimeError("limit must be > 0 for force orders")

        self._log_force_orders_public_rest_unavailable_once(symbol=symbol, limit=limit)
        return []

    # ========================================================
    # Internal helpers
    # ========================================================

    def _request_json(
        self,
        method: str,
        path: str,
        params: Optional[Mapping[str, Any]],
    ) -> Any:
        url = f"{self._base_url.rstrip('/')}{path}"

        try:
            response = self._session.request(
                method=method,
                url=url,
                params=dict(params) if params is not None else None,
                timeout=self._http_timeout_sec,
            )
        except requests.RequestException as exc:
            logger.error("Binance public request failed: path=%s error=%s", path, exc.__class__.__name__)
            raise RuntimeError(f"Binance public request failed: path={path}") from exc

        if response.status_code != 200:
            body_preview = response.text[:300]
            logger.error(
                "Binance public request returned non-200: path=%s status=%s body_preview=%s",
                path,
                response.status_code,
                body_preview,
            )
            raise RuntimeError(
                f"Binance public request returned non-200: path={path}, status={response.status_code}"
            )

        try:
            return response.json()
        except ValueError as exc:
            logger.error("Binance response JSON decode failed: path=%s", path)
            raise RuntimeError(f"Binance response JSON decode failed: path={path}") from exc

    def _parse_standard_ratio_series(
        self,
        payload: Any,
        endpoint: str,
        symbol: str,
    ) -> List[BinanceRatioPoint]:
        if not isinstance(payload, list):
            raise RuntimeError(f"Binance {endpoint} response is not a list")
        if not payload:
            raise RuntimeError(f"Binance {endpoint} response is empty")

        result: List[BinanceRatioPoint] = []
        for row in payload:
            if not isinstance(row, Mapping):
                raise RuntimeError(f"Binance {endpoint} row is not an object")

            ts = self._parse_int(row, "timestamp")
            if ts <= 0:
                raise RuntimeError(f"Invalid timestamp from Binance ratio endpoint: {endpoint}")

            result.append(
                BinanceRatioPoint(
                    symbol=symbol,
                    long_short_ratio=self._parse_decimal(row, "longShortRatio"),
                    long_account=self._parse_optional_decimal(row, "longAccount"),
                    short_account=self._parse_optional_decimal(row, "shortAccount"),
                    timestamp_ms=ts,
                )
            )
        return result

    def _parse_taker_ratio_series(
        self,
        payload: Any,
        endpoint: str,
        symbol: str,
    ) -> List[BinanceRatioPoint]:
        if not isinstance(payload, list):
            raise RuntimeError(f"Binance {endpoint} response is not a list")
        if not payload:
            raise RuntimeError(f"Binance {endpoint} response is empty")

        result: List[BinanceRatioPoint] = []
        for row in payload:
            if not isinstance(row, Mapping):
                raise RuntimeError(f"Binance {endpoint} row is not an object")

            ts = self._parse_int(row, "timestamp")
            if ts <= 0:
                raise RuntimeError(f"Invalid timestamp from Binance taker ratio endpoint: {endpoint}")

            result.append(
                BinanceRatioPoint(
                    symbol=symbol,
                    long_short_ratio=self._parse_decimal(row, "buySellRatio"),
                    long_account=self._parse_optional_decimal(row, "buyVol"),
                    short_account=self._parse_optional_decimal(row, "sellVol"),
                    timestamp_ms=ts,
                )
            )
        return result

    def _parse_depth_level(self, item: Any, side: str) -> BinanceDepthLevel:
        if not isinstance(item, Sequence) or isinstance(item, (str, bytes)):
            raise RuntimeError(f"Binance depth {side} level is not an array")
        if len(item) < 2:
            raise RuntimeError(f"Binance depth {side} level length < 2")

        price = self._to_decimal(item[0], f"depth_{side}_price")
        qty = self._to_decimal(item[1], f"depth_{side}_qty")

        if price <= Decimal("0"):
            raise RuntimeError(f"Binance depth {side} price must be > 0")
        if qty < Decimal("0"):
            raise RuntimeError(f"Binance depth {side} qty must be >= 0")

        return BinanceDepthLevel(price=price, qty=qty)

    def _parse_kline(self, row: Any) -> BinanceKline:
        if not isinstance(row, Sequence) or isinstance(row, (str, bytes)):
            raise RuntimeError("Binance kline row is not an array")
        if len(row) < 11:
            raise RuntimeError(f"Binance kline row length invalid: len={len(row)}")

        open_time_ms = self._to_int(row[0], "kline_open_time")
        open_price = self._to_decimal(row[1], "kline_open")
        high_price = self._to_decimal(row[2], "kline_high")
        low_price = self._to_decimal(row[3], "kline_low")
        close_price = self._to_decimal(row[4], "kline_close")
        volume = self._to_decimal(row[5], "kline_volume")
        close_time_ms = self._to_int(row[6], "kline_close_time")
        quote_asset_volume = self._to_decimal(row[7], "kline_quote_asset_volume")
        trade_count = self._to_int(row[8], "kline_trade_count")
        taker_buy_base_volume = self._to_decimal(row[9], "kline_taker_buy_base_volume")
        taker_buy_quote_volume = self._to_decimal(row[10], "kline_taker_buy_quote_volume")

        if (
            open_price <= Decimal("0")
            or high_price <= Decimal("0")
            or low_price <= Decimal("0")
            or close_price <= Decimal("0")
        ):
            raise RuntimeError("Binance kline contains non-positive price")
        if trade_count < 0:
            raise RuntimeError("Binance kline trade_count must be >= 0")
        if close_time_ms < open_time_ms:
            raise RuntimeError("Binance kline close_time_ms < open_time_ms")

        return BinanceKline(
            open_time_ms=open_time_ms,
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            close_price=close_price,
            volume=volume,
            close_time_ms=close_time_ms,
            quote_asset_volume=quote_asset_volume,
            trade_count=trade_count,
            taker_buy_base_volume=taker_buy_base_volume,
            taker_buy_quote_volume=taker_buy_quote_volume,
        )

    def _require_list(self, payload: Mapping[str, Any], key: str) -> list[Any]:
        value = payload.get(key)
        if not isinstance(value, list):
            raise RuntimeError(f"Expected list field: {key}")
        return value

    def _parse_str(self, payload: Mapping[str, Any], key: str) -> str:
        value = payload.get(key)
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"Expected non-empty string field: {key}")
        return value

    def _parse_decimal(self, payload: Mapping[str, Any], key: str) -> Decimal:
        if key not in payload:
            raise RuntimeError(f"Missing required decimal field: {key}")
        return self._to_decimal(payload[key], key)

    def _parse_optional_decimal(self, payload: Mapping[str, Any], key: str) -> Optional[Decimal]:
        if key not in payload or payload[key] in (None, ""):
            return None
        return self._to_decimal(payload[key], key)

    def _parse_int(self, payload: Mapping[str, Any], key: str) -> int:
        if key not in payload:
            raise RuntimeError(f"Missing required int field: {key}")
        return self._to_int(payload[key], key)

    def _parse_optional_int(self, payload: Mapping[str, Any], key: str) -> Optional[int]:
        if key not in payload or payload[key] in (None, ""):
            return None
        return self._to_int(payload[key], key)

    def _to_decimal(self, value: Any, field_name: str) -> Decimal:
        if isinstance(value, bool):
            raise RuntimeError(f"Invalid decimal field type for {field_name}: bool")
        try:
            dec = Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise RuntimeError(f"Invalid decimal field for {field_name}") from exc
        if not dec.is_finite():
            raise RuntimeError(f"Non-finite decimal field for {field_name}")
        return dec

    def _to_int(self, value: Any, field_name: str) -> int:
        if isinstance(value, bool):
            raise RuntimeError(f"Invalid int field type for {field_name}: bool")
        try:
            ivalue = int(value)
        except (ValueError, TypeError) as exc:
            raise RuntimeError(f"Invalid int field for {field_name}") from exc
        return ivalue

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    def _require_str_setting(self, name: str) -> str:
        value = getattr(SETTINGS, name, None)
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"Missing or invalid required setting: {name}")
        return value

    def _require_int_setting(self, name: str) -> int:
        value = getattr(SETTINGS, name, None)
        if isinstance(value, bool):
            raise RuntimeError(f"Invalid bool value for integer setting: {name}")
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Missing or invalid required int setting: {name}") from exc
        return parsed

    def _require_float_setting(self, name: str) -> float:
        value = getattr(SETTINGS, name, None)
        if isinstance(value, bool):
            raise RuntimeError(f"Invalid bool value for float setting: {name}")
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Missing or invalid required float setting: {name}") from exc
        return parsed

    def _log_force_orders_public_rest_unavailable_once(self, symbol: str, limit: int) -> None:
        if self._force_orders_public_rest_unavailable_logged:
            return
        logger.warning(
            "force_orders not collected in public REST mode: "
            "symbol=%s limit=%s reason=Binance public REST endpoint unavailable_or_policy_conflict",
            symbol,
            limit,
        )
        self._force_orders_public_rest_unavailable_logged = True