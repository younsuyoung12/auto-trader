"""
========================================================
FILE: analysis/options_market_fetcher.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할:
- Deribit 공개 옵션 시장 데이터를 수집/정규화해 Options Market Snapshot 을 생성한다.
- Put/Call OI 비율, Put/Call 거래량 비율, ATM 옵션 OI 구조를 계산한다.
- 이 모듈은 "Feature Engine" 이며 주문/포지션/리스크 실행을 절대 수행하지 않는다.
- market_researcher / quant_analyst / entry_pipeline 에서 공통 피처로 사용된다.

절대 원칙:
- STRICT · NO-FALLBACK
- settings.py 외 환경변수 직접 접근 금지
- print() 금지 / logging 사용
- 데이터 누락/손상/모호성 발생 시 즉시 예외
- 예외 삼키기 금지
- 민감정보 로그 금지
- 공개 옵션 시장 데이터만 사용

데이터 소스:
- Deribit Public API
  - /public/get_book_summary_by_currency
  - /public/get_index_price

입력 전제:
- SETTINGS.ANALYST_MARKET_SYMBOL 이 존재해야 한다.
- 현재 symbol 에서 base currency(BTC / ETH 등)를 엄격하게 추출할 수 있어야 한다.
- Deribit 응답은 JSON-RPC 2.0 object 여야 한다.
- option instrument_name 은 Deribit 표준 형식을 만족해야 한다.
  예: BTC-28MAR25-90000-C

출력:
- OptionsMarketSnapshot
  - put_call_oi_ratio
  - put_call_volume_ratio
  - atm_call / atm_put 구조
  - options_bias
  - flow_bias
  - dashboard_payload

변경 이력:
2026-03-08
- 신규 생성
- Deribit 공개 옵션시장 Feature Engine 추가
- Put/Call OI 비율, 거래량 비율, ATM 옵션 OI 구조 계산 추가
- market_researcher / quant_analyst / entry_pipeline 연동용 구조 확정
========================================================
"""

from __future__ import annotations

import logging
import re
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import requests

from settings import SETTINGS

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DeribitOptionSummary:
    instrument_name: str
    option_type: str
    strike: Decimal
    open_interest: Decimal
    volume_24h: Decimal

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class OptionsMarketSnapshot:
    symbol: str
    currency: str
    as_of_ms: int
    source: str
    index_name: str
    index_price: Decimal
    option_count: int
    call_count: int
    put_count: int
    total_call_open_interest: Decimal
    total_put_open_interest: Decimal
    total_open_interest: Decimal
    put_call_oi_ratio: Decimal
    total_call_volume_24h: Decimal
    total_put_volume_24h: Decimal
    total_volume_24h: Decimal
    put_call_volume_ratio: Decimal
    atm_call_instrument: str
    atm_put_instrument: str
    atm_call_strike: Decimal
    atm_put_strike: Decimal
    atm_call_open_interest: Decimal
    atm_put_open_interest: Decimal
    atm_put_call_oi_ratio: Decimal
    oi_bias: str
    flow_bias: str
    options_bias: str
    analyst_summary_ko: str
    dashboard_payload: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class OptionsMarketFetcher:
    """
    Deribit 공개 옵션시장 데이터 수집기.
    """

    _BASE_URL = "https://www.deribit.com/api/v2"
    _REQUEST_TIMEOUT_SEC = 10
    _SUPPORTED_QUOTE_SUFFIXES: Tuple[str, ...] = ("USDT", "USDC", "BUSD", "USD")
    _INSTRUMENT_PATTERN = re.compile(
        r"^(?P<currency>[A-Z]+)-(?P<expiry>\d{1,2}[A-Z]{3}\d{2})-(?P<strike>\d+(?:\.\d+)*)-(?P<side>[CP])$"
    )

    def __init__(self) -> None:
        self._symbol = self._require_str_setting("ANALYST_MARKET_SYMBOL")
        self._currency = self._derive_base_currency_from_symbol_or_raise(self._symbol)
        self._index_name = self._derive_index_name_or_raise(self._currency)
        self._session = requests.Session()

    # ========================================================
    # Public API
    # ========================================================

    def fetch(self) -> OptionsMarketSnapshot:
        as_of_ms = self._now_ms()

        summaries_payload = self._request_result_or_raise(
            path="/public/get_book_summary_by_currency",
            params={
                "currency": self._currency,
                "kind": "option",
            },
        )
        if not isinstance(summaries_payload, list):
            raise RuntimeError("Deribit option summaries result must be a list")
        if len(summaries_payload) == 0:
            raise RuntimeError(f"Deribit option summaries returned empty list: currency={self._currency}")

        index_payload = self._request_result_or_raise(
            path="/public/get_index_price",
            params={
                "index_name": self._index_name,
            },
        )
        if not isinstance(index_payload, Mapping):
            raise RuntimeError("Deribit index price result must be an object")

        index_price = self._require_decimal_from_mapping(
            index_payload,
            "index_price",
            "deribit.index_price",
        )
        if index_price <= Decimal("0"):
            raise RuntimeError("Deribit index_price must be > 0")

        parsed_options = [
            self._parse_option_summary_row(row=row, idx=idx)
            for idx, row in enumerate(summaries_payload)
        ]
        if len(parsed_options) == 0:
            raise RuntimeError("Parsed Deribit options list is empty")

        calls = [item for item in parsed_options if item.option_type == "call"]
        puts = [item for item in parsed_options if item.option_type == "put"]

        if len(calls) == 0:
            raise RuntimeError("No call options found in Deribit payload")
        if len(puts) == 0:
            raise RuntimeError("No put options found in Deribit payload")

        total_call_open_interest = sum((item.open_interest for item in calls), Decimal("0"))
        total_put_open_interest = sum((item.open_interest for item in puts), Decimal("0"))
        total_open_interest = total_call_open_interest + total_put_open_interest

        if total_call_open_interest <= Decimal("0"):
            raise RuntimeError("total_call_open_interest must be > 0")
        if total_put_open_interest <= Decimal("0"):
            raise RuntimeError("total_put_open_interest must be > 0")
        if total_open_interest <= Decimal("0"):
            raise RuntimeError("total_open_interest must be > 0")

        total_call_volume_24h = sum((item.volume_24h for item in calls), Decimal("0"))
        total_put_volume_24h = sum((item.volume_24h for item in puts), Decimal("0"))
        total_volume_24h = total_call_volume_24h + total_put_volume_24h

        if total_call_volume_24h <= Decimal("0"):
            raise RuntimeError("total_call_volume_24h must be > 0")
        if total_put_volume_24h <= Decimal("0"):
            raise RuntimeError("total_put_volume_24h must be > 0")
        if total_volume_24h <= Decimal("0"):
            raise RuntimeError("total_volume_24h must be > 0")

        put_call_oi_ratio = total_put_open_interest / total_call_open_interest
        put_call_volume_ratio = total_put_volume_24h / total_call_volume_24h

        atm_call = self._select_atm_option_or_raise(
            options=calls,
            index_price=index_price,
            option_label="call",
        )
        atm_put = self._select_atm_option_or_raise(
            options=puts,
            index_price=index_price,
            option_label="put",
        )

        if atm_call.open_interest <= Decimal("0"):
            raise RuntimeError("ATM call open_interest must be > 0")
        if atm_put.open_interest <= Decimal("0"):
            raise RuntimeError("ATM put open_interest must be > 0")

        atm_put_call_oi_ratio = atm_put.open_interest / atm_call.open_interest

        oi_bias = self._classify_oi_bias(put_call_oi_ratio)
        flow_bias = self._classify_flow_bias(put_call_volume_ratio)
        options_bias = self._classify_options_bias(
            oi_bias=oi_bias,
            flow_bias=flow_bias,
        )

        analyst_summary_ko = self._build_korean_summary(
            symbol=self._symbol,
            currency=self._currency,
            index_price=index_price,
            option_count=len(parsed_options),
            put_call_oi_ratio=put_call_oi_ratio,
            put_call_volume_ratio=put_call_volume_ratio,
            atm_call=atm_call,
            atm_put=atm_put,
            atm_put_call_oi_ratio=atm_put_call_oi_ratio,
            oi_bias=oi_bias,
            flow_bias=flow_bias,
            options_bias=options_bias,
        )

        dashboard_payload = self._build_dashboard_payload(
            as_of_ms=as_of_ms,
            index_price=index_price,
            option_count=len(parsed_options),
            calls=calls,
            puts=puts,
            total_call_open_interest=total_call_open_interest,
            total_put_open_interest=total_put_open_interest,
            total_open_interest=total_open_interest,
            put_call_oi_ratio=put_call_oi_ratio,
            total_call_volume_24h=total_call_volume_24h,
            total_put_volume_24h=total_put_volume_24h,
            total_volume_24h=total_volume_24h,
            put_call_volume_ratio=put_call_volume_ratio,
            atm_call=atm_call,
            atm_put=atm_put,
            atm_put_call_oi_ratio=atm_put_call_oi_ratio,
            oi_bias=oi_bias,
            flow_bias=flow_bias,
            options_bias=options_bias,
            analyst_summary_ko=analyst_summary_ko,
        )

        logger.info(
            "Options market fetched: symbol=%s currency=%s option_count=%s pcr_oi=%s pcr_vol=%s options_bias=%s",
            self._symbol,
            self._currency,
            len(parsed_options),
            self._fmt_decimal(put_call_oi_ratio, 4),
            self._fmt_decimal(put_call_volume_ratio, 4),
            options_bias,
        )

        return OptionsMarketSnapshot(
            symbol=self._symbol,
            currency=self._currency,
            as_of_ms=as_of_ms,
            source="deribit",
            index_name=self._index_name,
            index_price=index_price,
            option_count=len(parsed_options),
            call_count=len(calls),
            put_count=len(puts),
            total_call_open_interest=total_call_open_interest,
            total_put_open_interest=total_put_open_interest,
            total_open_interest=total_open_interest,
            put_call_oi_ratio=put_call_oi_ratio,
            total_call_volume_24h=total_call_volume_24h,
            total_put_volume_24h=total_put_volume_24h,
            total_volume_24h=total_volume_24h,
            put_call_volume_ratio=put_call_volume_ratio,
            atm_call_instrument=atm_call.instrument_name,
            atm_put_instrument=atm_put.instrument_name,
            atm_call_strike=atm_call.strike,
            atm_put_strike=atm_put.strike,
            atm_call_open_interest=atm_call.open_interest,
            atm_put_open_interest=atm_put.open_interest,
            atm_put_call_oi_ratio=atm_put_call_oi_ratio,
            oi_bias=oi_bias,
            flow_bias=flow_bias,
            options_bias=options_bias,
            analyst_summary_ko=analyst_summary_ko,
            dashboard_payload=dashboard_payload,
        )

    # ========================================================
    # HTTP / API
    # ========================================================

    def _request_result_or_raise(
        self,
        *,
        path: str,
        params: Mapping[str, Any],
    ) -> Any:
        if not isinstance(path, str) or not path.startswith("/"):
            raise RuntimeError("path must start with '/'")
        if not isinstance(params, Mapping):
            raise RuntimeError("params must be mapping")

        url = f"{self._BASE_URL}{path}"

        try:
            response = self._session.get(
                url,
                params=dict(params),
                timeout=self._REQUEST_TIMEOUT_SEC,
            )
        except requests.RequestException as exc:
            raise RuntimeError(f"Deribit request failed: path={path}") from exc

        if response.status_code != 200:
            raise RuntimeError(f"Deribit HTTP status != 200: path={path}, status={response.status_code}")

        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(f"Deribit response JSON decode failed: path={path}") from exc

        if not isinstance(payload, Mapping):
            raise RuntimeError(f"Deribit response payload must be object: path={path}")

        error_obj = payload.get("error")
        if error_obj is not None:
            raise RuntimeError(f"Deribit API returned error for path={path}")

        if payload.get("jsonrpc") != "2.0":
            raise RuntimeError(f"Deribit response jsonrpc must be '2.0': path={path}")

        if "result" not in payload:
            raise RuntimeError(f"Deribit response missing result: path={path}")

        return payload["result"]

    # ========================================================
    # Parsing
    # ========================================================

    def _parse_option_summary_row(
        self,
        *,
        row: Mapping[str, Any],
        idx: int,
    ) -> DeribitOptionSummary:
        if not isinstance(row, Mapping):
            raise RuntimeError(f"option_summary[{idx}] must be object")

        instrument_name = self._require_str_from_mapping(
            row,
            "instrument_name",
            f"option_summary[{idx}].instrument_name",
        )
        strike, option_type = self._parse_instrument_name_or_raise(instrument_name)

        open_interest = self._require_decimal_from_mapping(
            row,
            "open_interest",
            f"option_summary[{idx}].open_interest",
        )
        volume_24h = self._require_decimal_from_mapping(
            row,
            "volume",
            f"option_summary[{idx}].volume",
        )

        if open_interest < Decimal("0"):
            raise RuntimeError(f"option_summary[{idx}].open_interest must be >= 0")
        if volume_24h < Decimal("0"):
            raise RuntimeError(f"option_summary[{idx}].volume must be >= 0")

        return DeribitOptionSummary(
            instrument_name=instrument_name,
            option_type=option_type,
            strike=strike,
            open_interest=open_interest,
            volume_24h=volume_24h,
        )

    def _parse_instrument_name_or_raise(
        self,
        instrument_name: str,
    ) -> Tuple[Decimal, str]:
        matched = self._INSTRUMENT_PATTERN.match(instrument_name)
        if matched is None:
            raise RuntimeError(f"Unexpected Deribit option instrument_name format: {instrument_name}")

        currency = matched.group("currency")
        if currency != self._currency:
            raise RuntimeError(
                f"Instrument currency mismatch: expected={self._currency}, got={currency}, instrument={instrument_name}"
            )

        strike = self._to_decimal(matched.group("strike"), "instrument_name.strike")
        if strike <= Decimal("0"):
            raise RuntimeError(f"Instrument strike must be > 0: {instrument_name}")

        side = matched.group("side")
        if side == "C":
            option_type = "call"
        elif side == "P":
            option_type = "put"
        else:
            raise RuntimeError(f"Unexpected option side in instrument_name: {instrument_name}")

        return strike, option_type

    # ========================================================
    # Selection / classification
    # ========================================================

    def _select_atm_option_or_raise(
        self,
        *,
        options: Sequence[DeribitOptionSummary],
        index_price: Decimal,
        option_label: str,
    ) -> DeribitOptionSummary:
        if len(options) == 0:
            raise RuntimeError(f"No options available for ATM selection: {option_label}")
        if index_price <= Decimal("0"):
            raise RuntimeError("index_price must be > 0")

        ranked = sorted(
            options,
            key=lambda item: (
                abs(item.strike - index_price),
                -(item.open_interest),
                item.instrument_name,
            ),
        )
        selected = ranked[0]

        if selected.strike <= Decimal("0"):
            raise RuntimeError(f"Selected ATM {option_label} strike must be > 0")

        return selected

    def _classify_oi_bias(
        self,
        put_call_oi_ratio: Decimal,
    ) -> str:
        if put_call_oi_ratio >= Decimal("1.20"):
            return "bearish"
        if put_call_oi_ratio <= Decimal("0.83"):
            return "bullish"
        return "neutral"

    def _classify_flow_bias(
        self,
        put_call_volume_ratio: Decimal,
    ) -> str:
        if put_call_volume_ratio >= Decimal("1.20"):
            return "bearish"
        if put_call_volume_ratio <= Decimal("0.83"):
            return "bullish"
        return "neutral"

    def _classify_options_bias(
        self,
        *,
        oi_bias: str,
        flow_bias: str,
    ) -> str:
        if oi_bias == "bearish" and flow_bias == "bearish":
            return "bearish"
        if oi_bias == "bullish" and flow_bias == "bullish":
            return "bullish"
        if oi_bias == "neutral" and flow_bias == "neutral":
            return "neutral"
        return "mixed"

    # ========================================================
    # Presentation
    # ========================================================

    def _build_korean_summary(
        self,
        *,
        symbol: str,
        currency: str,
        index_price: Decimal,
        option_count: int,
        put_call_oi_ratio: Decimal,
        put_call_volume_ratio: Decimal,
        atm_call: DeribitOptionSummary,
        atm_put: DeribitOptionSummary,
        atm_put_call_oi_ratio: Decimal,
        oi_bias: str,
        flow_bias: str,
        options_bias: str,
    ) -> str:
        return (
            f"{symbol} 옵션 시장 분석 결과 기준 통화는 {currency}, "
            f"지수 가격은 {self._fmt_decimal(index_price, 2)} 이며 "
            f"수집된 옵션 수는 {option_count}개 입니다. "
            f"전체 Put/Call OI 비율은 {self._fmt_decimal(put_call_oi_ratio, 4)}, "
            f"전체 Put/Call 거래량 비율은 {self._fmt_decimal(put_call_volume_ratio, 4)} 입니다. "
            f"ATM Call 은 {atm_call.instrument_name} / strike {self._fmt_decimal(atm_call.strike, 2)} / OI {self._fmt_decimal(atm_call.open_interest, 6)}, "
            f"ATM Put 은 {atm_put.instrument_name} / strike {self._fmt_decimal(atm_put.strike, 2)} / OI {self._fmt_decimal(atm_put.open_interest, 6)} 입니다. "
            f"ATM Put/Call OI 비율은 {self._fmt_decimal(atm_put_call_oi_ratio, 4)} 이며 "
            f"OI bias={oi_bias}, flow bias={flow_bias}, 최종 options bias={options_bias} 입니다."
        )

    def _build_dashboard_payload(
        self,
        *,
        as_of_ms: int,
        index_price: Decimal,
        option_count: int,
        calls: Sequence[DeribitOptionSummary],
        puts: Sequence[DeribitOptionSummary],
        total_call_open_interest: Decimal,
        total_put_open_interest: Decimal,
        total_open_interest: Decimal,
        put_call_oi_ratio: Decimal,
        total_call_volume_24h: Decimal,
        total_put_volume_24h: Decimal,
        total_volume_24h: Decimal,
        put_call_volume_ratio: Decimal,
        atm_call: DeribitOptionSummary,
        atm_put: DeribitOptionSummary,
        atm_put_call_oi_ratio: Decimal,
        oi_bias: str,
        flow_bias: str,
        options_bias: str,
        analyst_summary_ko: str,
    ) -> Dict[str, Any]:
        return {
            "symbol": self._symbol,
            "currency": self._currency,
            "source": "deribit",
            "as_of_ms": as_of_ms,
            "index_name": self._index_name,
            "index_price": self._fmt_decimal(index_price, 2),
            "market": {
                "option_count": option_count,
                "call_count": len(calls),
                "put_count": len(puts),
            },
            "open_interest": {
                "total_call_open_interest": self._fmt_decimal(total_call_open_interest, 6),
                "total_put_open_interest": self._fmt_decimal(total_put_open_interest, 6),
                "total_open_interest": self._fmt_decimal(total_open_interest, 6),
                "put_call_oi_ratio": self._fmt_decimal(put_call_oi_ratio, 4),
                "oi_bias": oi_bias,
            },
            "flow": {
                "total_call_volume_24h": self._fmt_decimal(total_call_volume_24h, 6),
                "total_put_volume_24h": self._fmt_decimal(total_put_volume_24h, 6),
                "total_volume_24h": self._fmt_decimal(total_volume_24h, 6),
                "put_call_volume_ratio": self._fmt_decimal(put_call_volume_ratio, 4),
                "flow_bias": flow_bias,
            },
            "atm_structure": {
                "atm_call_instrument": atm_call.instrument_name,
                "atm_put_instrument": atm_put.instrument_name,
                "atm_call_strike": self._fmt_decimal(atm_call.strike, 2),
                "atm_put_strike": self._fmt_decimal(atm_put.strike, 2),
                "atm_call_open_interest": self._fmt_decimal(atm_call.open_interest, 6),
                "atm_put_open_interest": self._fmt_decimal(atm_put.open_interest, 6),
                "atm_put_call_oi_ratio": self._fmt_decimal(atm_put_call_oi_ratio, 4),
            },
            "bias": {
                "options_bias": options_bias,
            },
            "analyst_summary_ko": analyst_summary_ko,
        }

    # ========================================================
    # Settings / utility
    # ========================================================

    def _require_str_setting(self, name: str) -> str:
        value = getattr(SETTINGS, name, None)
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"Missing or invalid required setting: {name}")
        return value.strip()

    def _derive_base_currency_from_symbol_or_raise(
        self,
        symbol: str,
    ) -> str:
        normalized = symbol.strip().upper()
        if not normalized:
            raise RuntimeError("ANALYST_MARKET_SYMBOL must not be empty")

        for suffix in self._SUPPORTED_QUOTE_SUFFIXES:
            if normalized.endswith(suffix):
                base = normalized[: -len(suffix)]
                if not base:
                    raise RuntimeError(f"Failed to derive base currency from symbol: {symbol}")
                return base

        raise RuntimeError(f"Unsupported ANALYST_MARKET_SYMBOL quote suffix: {symbol}")

    def _derive_index_name_or_raise(
        self,
        currency: str,
    ) -> str:
        normalized = currency.strip().lower()
        if not normalized:
            raise RuntimeError("currency must not be empty")
        return f"{normalized}_usd"

    def _require_str_from_mapping(
        self,
        row: Mapping[str, Any],
        key: str,
        field_name: str,
    ) -> str:
        if key not in row:
            raise RuntimeError(f"Missing required string field: {field_name}")
        value = row[key]
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"Invalid required string field: {field_name}")
        return value.strip()

    def _require_decimal_from_mapping(
        self,
        row: Mapping[str, Any],
        key: str,
        field_name: str,
    ) -> Decimal:
        if key not in row:
            raise RuntimeError(f"Missing required decimal field: {field_name}")
        return self._to_decimal(row[key], field_name)

    def _to_decimal(
        self,
        value: Any,
        field_name: str,
    ) -> Decimal:
        if isinstance(value, bool):
            raise RuntimeError(f"Invalid decimal field type for {field_name}: bool")
        try:
            dec = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise RuntimeError(f"Invalid decimal field for {field_name}") from exc
        if not dec.is_finite():
            raise RuntimeError(f"Non-finite decimal field for {field_name}")
        return dec

    def _fmt_decimal(
        self,
        value: Decimal,
        scale: int,
    ) -> str:
        if scale < 0:
            raise RuntimeError("scale must be >= 0")
        quant = Decimal("1").scaleb(-scale)
        normalized = value.quantize(quant, rounding=ROUND_HALF_UP)
        return f"{normalized:.{scale}f}"

    def _now_ms(self) -> int:
        import time

        ts_ms = int(time.time() * 1000)
        if ts_ms <= 0:
            raise RuntimeError("current timestamp must be > 0")
        return ts_ms


__all__ = [
    "DeribitOptionSummary",
    "OptionsMarketSnapshot",
    "OptionsMarketFetcher",
]