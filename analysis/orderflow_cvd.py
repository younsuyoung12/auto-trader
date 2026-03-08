"""
========================================================
FILE: analysis/orderflow_cvd.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할:
- Binance aggTrade(집계 체결) 데이터로부터 Order Flow / CVD 를 계산한다.
- 공격적 매수/매도 수급을 정량화해 BUY / SELL / HOLD 판단용 피처를 생성한다.
- 이 모듈은 "Feature Engine" 이며 주문/포지션/리스크 실행을 절대 수행하지 않는다.
- market_researcher / quant_analyst / entry_pipeline 에서 공통 피처로 사용된다.

절대 원칙:
- STRICT · NO-FALLBACK
- settings.py 외 환경변수 직접 접근 금지
- print() 금지 / logging 사용
- 데이터 누락/손상/모호성 발생 시 즉시 예외
- 예외 삼키기 금지
- 민감정보 로그 금지

입력 전제:
- aggTrade 는 시간 오름차순이어야 한다.
- Binance aggTrade 기준:
  - p: price
  - q: quantity
  - T: trade time ms
  - m: is buyer the market maker
- m=True  -> 공격적 매도(SELL aggressive)
- m=False -> 공격적 매수(BUY aggressive)

출력:
- OrderFlowCvdReport
  - aggressive_buy_qty
  - aggressive_sell_qty
  - net_delta_qty
  - cvd
  - delta_ratio_pct
  - aggression_bias
  - cvd_trend
  - divergence
  - dashboard_payload

변경 이력:
2026-03-08
- 신규 생성
- Binance aggTrade 기반 CVD Feature Engine 추가
- 공격적 매수/매도량, delta, CVD, divergence 계산 추가
- market_researcher / quant_analyst / entry_pipeline 연동용 구조 확정
========================================================
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Mapping, Optional, Sequence, Union

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AggTradeRecord:
    trade_id: int
    price: Decimal
    qty: Decimal
    ts_ms: int
    is_buyer_maker: bool
    first_trade_id: Optional[int]
    last_trade_id: Optional[int]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class OrderFlowCvdPoint:
    seq: int
    trade_id: int
    ts_ms: int
    price: Decimal
    qty: Decimal
    aggressive_side: str
    signed_qty: Decimal
    cvd: Decimal

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class OrderFlowCvdReport:
    symbol: str
    as_of_ms: int
    total_trades: int
    total_qty: Decimal
    aggressive_buy_qty: Decimal
    aggressive_sell_qty: Decimal
    buy_trade_count: int
    sell_trade_count: int
    net_delta_qty: Decimal
    cvd: Decimal
    delta_ratio_pct: Decimal
    buy_share_pct: Decimal
    sell_share_pct: Decimal
    price_open: Decimal
    price_close: Decimal
    price_change_pct: Decimal
    average_trade_qty: Decimal
    max_trade_qty: Decimal
    aggression_bias: str
    cvd_trend: str
    divergence: str
    cvd_path_tail: List[OrderFlowCvdPoint]
    analyst_summary_ko: str
    dashboard_payload: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class OrderFlowCvdEngine:
    """
    Binance aggTrade 기반 Order Flow / CVD 계산기.

    사용 예:
        engine = OrderFlowCvdEngine(
            min_trades=30,
            dominance_threshold_pct=10,
            divergence_price_threshold_pct=0.20,
            divergence_delta_threshold_pct=8.0,
            cvd_path_tail_size=20,
        )
        report = engine.build_from_binance_payload(symbol="BTCUSDT", payload=aggtrade_json)
    """

    def __init__(
        self,
        *,
        min_trades: int,
        dominance_threshold_pct: Decimal,
        divergence_price_threshold_pct: Decimal,
        divergence_delta_threshold_pct: Decimal,
        cvd_path_tail_size: int = 20,
    ) -> None:
        self._min_trades = self._require_positive_int(min_trades, "min_trades")
        self._dominance_threshold_pct = self._require_non_negative_decimal(
            dominance_threshold_pct,
            "dominance_threshold_pct",
        )
        self._divergence_price_threshold_pct = self._require_non_negative_decimal(
            divergence_price_threshold_pct,
            "divergence_price_threshold_pct",
        )
        self._divergence_delta_threshold_pct = self._require_non_negative_decimal(
            divergence_delta_threshold_pct,
            "divergence_delta_threshold_pct",
        )
        self._cvd_path_tail_size = self._require_positive_int(cvd_path_tail_size, "cvd_path_tail_size")

    # ========================================================
    # Public API
    # ========================================================

    def build_from_binance_payload(
        self,
        *,
        symbol: str,
        payload: Sequence[Mapping[str, Any]],
    ) -> OrderFlowCvdReport:
        records = [self._parse_binance_aggtrade_row(row, idx) for idx, row in enumerate(payload)]
        return self.build(symbol=symbol, trades=records)

    def build(
        self,
        *,
        symbol: str,
        trades: Sequence[Union[AggTradeRecord, Mapping[str, Any]]],
    ) -> OrderFlowCvdReport:
        symbol_norm = self._require_non_empty_str(symbol, "symbol")
        normalized = self._normalize_trade_input_or_raise(trades)

        if len(normalized) < self._min_trades:
            raise RuntimeError(
                f"OrderFlowCvdEngine requires at least {self._min_trades} trades, got={len(normalized)}"
            )

        validated = self._validate_trades_or_raise(normalized)

        total_qty = Decimal("0")
        aggressive_buy_qty = Decimal("0")
        aggressive_sell_qty = Decimal("0")
        buy_trade_count = 0
        sell_trade_count = 0
        cvd = Decimal("0")
        max_trade_qty = Decimal("0")
        path: List[OrderFlowCvdPoint] = []

        for seq, trade in enumerate(validated, start=1):
            total_qty += trade.qty
            if trade.qty > max_trade_qty:
                max_trade_qty = trade.qty

            if trade.is_buyer_maker:
                aggressive_side = "SELL"
                signed_qty = -trade.qty
                aggressive_sell_qty += trade.qty
                sell_trade_count += 1
            else:
                aggressive_side = "BUY"
                signed_qty = trade.qty
                aggressive_buy_qty += trade.qty
                buy_trade_count += 1

            cvd += signed_qty

            path.append(
                OrderFlowCvdPoint(
                    seq=seq,
                    trade_id=trade.trade_id,
                    ts_ms=trade.ts_ms,
                    price=trade.price,
                    qty=trade.qty,
                    aggressive_side=aggressive_side,
                    signed_qty=signed_qty,
                    cvd=cvd,
                )
            )

        if total_qty <= Decimal("0"):
            raise RuntimeError("Order flow total_qty must be > 0")

        net_delta_qty = aggressive_buy_qty - aggressive_sell_qty
        delta_ratio_pct = (net_delta_qty / total_qty) * Decimal("100")
        buy_share_pct = (aggressive_buy_qty / total_qty) * Decimal("100")
        sell_share_pct = (aggressive_sell_qty / total_qty) * Decimal("100")

        price_open = validated[0].price
        price_close = validated[-1].price
        price_change_pct = self._calc_price_change_pct(price_open=price_open, price_close=price_close)

        average_trade_qty = total_qty / Decimal(len(validated))
        aggression_bias = self._classify_aggression_bias(delta_ratio_pct)
        cvd_trend = self._classify_cvd_trend(delta_ratio_pct)
        divergence = self._classify_divergence(
            price_change_pct=price_change_pct,
            delta_ratio_pct=delta_ratio_pct,
        )

        cvd_path_tail = path[-self._cvd_path_tail_size :]

        analyst_summary_ko = self._build_korean_summary(
            symbol=symbol_norm,
            total_trades=len(validated),
            total_qty=total_qty,
            aggressive_buy_qty=aggressive_buy_qty,
            aggressive_sell_qty=aggressive_sell_qty,
            net_delta_qty=net_delta_qty,
            cvd=cvd,
            delta_ratio_pct=delta_ratio_pct,
            buy_share_pct=buy_share_pct,
            sell_share_pct=sell_share_pct,
            price_open=price_open,
            price_close=price_close,
            price_change_pct=price_change_pct,
            aggression_bias=aggression_bias,
            cvd_trend=cvd_trend,
            divergence=divergence,
        )

        dashboard_payload = self._build_dashboard_payload(
            symbol=symbol_norm,
            as_of_ms=validated[-1].ts_ms,
            total_trades=len(validated),
            total_qty=total_qty,
            aggressive_buy_qty=aggressive_buy_qty,
            aggressive_sell_qty=aggressive_sell_qty,
            buy_trade_count=buy_trade_count,
            sell_trade_count=sell_trade_count,
            net_delta_qty=net_delta_qty,
            cvd=cvd,
            delta_ratio_pct=delta_ratio_pct,
            buy_share_pct=buy_share_pct,
            sell_share_pct=sell_share_pct,
            price_open=price_open,
            price_close=price_close,
            price_change_pct=price_change_pct,
            average_trade_qty=average_trade_qty,
            max_trade_qty=max_trade_qty,
            aggression_bias=aggression_bias,
            cvd_trend=cvd_trend,
            divergence=divergence,
            cvd_path_tail=cvd_path_tail,
            analyst_summary_ko=analyst_summary_ko,
        )

        logger.info(
            "Order flow CVD built: symbol=%s as_of_ms=%s trades=%s delta_ratio_pct=%s aggression_bias=%s divergence=%s",
            symbol_norm,
            validated[-1].ts_ms,
            len(validated),
            self._fmt_decimal(delta_ratio_pct, 4),
            aggression_bias,
            divergence,
        )

        return OrderFlowCvdReport(
            symbol=symbol_norm,
            as_of_ms=validated[-1].ts_ms,
            total_trades=len(validated),
            total_qty=total_qty,
            aggressive_buy_qty=aggressive_buy_qty,
            aggressive_sell_qty=aggressive_sell_qty,
            buy_trade_count=buy_trade_count,
            sell_trade_count=sell_trade_count,
            net_delta_qty=net_delta_qty,
            cvd=cvd,
            delta_ratio_pct=delta_ratio_pct,
            buy_share_pct=buy_share_pct,
            sell_share_pct=sell_share_pct,
            price_open=price_open,
            price_close=price_close,
            price_change_pct=price_change_pct,
            average_trade_qty=average_trade_qty,
            max_trade_qty=max_trade_qty,
            aggression_bias=aggression_bias,
            cvd_trend=cvd_trend,
            divergence=divergence,
            cvd_path_tail=cvd_path_tail,
            analyst_summary_ko=analyst_summary_ko,
            dashboard_payload=dashboard_payload,
        )

    # ========================================================
    # Parsing / normalization
    # ========================================================

    def _normalize_trade_input_or_raise(
        self,
        trades: Sequence[Union[AggTradeRecord, Mapping[str, Any]]],
    ) -> List[AggTradeRecord]:
        if not isinstance(trades, Sequence):
            raise RuntimeError("trades must be a sequence")
        if len(trades) == 0:
            raise RuntimeError("trades must not be empty")

        normalized: List[AggTradeRecord] = []
        for idx, item in enumerate(trades):
            if isinstance(item, AggTradeRecord):
                normalized.append(item)
                continue

            if isinstance(item, Mapping):
                normalized.append(self._parse_generic_trade_mapping(item, idx))
                continue

            raise RuntimeError(f"trades[{idx}] must be AggTradeRecord or mapping")

        return normalized

    def _parse_binance_aggtrade_row(
        self,
        row: Mapping[str, Any],
        idx: int,
    ) -> AggTradeRecord:
        if not isinstance(row, Mapping):
            raise RuntimeError(f"payload[{idx}] is not an object")

        return AggTradeRecord(
            trade_id=self._require_int_from_mapping(row, "a", f"aggtrade[{idx}].a"),
            price=self._require_decimal_from_mapping(row, "p", f"aggtrade[{idx}].p"),
            qty=self._require_decimal_from_mapping(row, "q", f"aggtrade[{idx}].q"),
            ts_ms=self._require_int_from_mapping(row, "T", f"aggtrade[{idx}].T"),
            is_buyer_maker=self._require_bool_from_mapping(row, "m", f"aggtrade[{idx}].m"),
            first_trade_id=self._optional_int_from_mapping(row, "f", f"aggtrade[{idx}].f"),
            last_trade_id=self._optional_int_from_mapping(row, "l", f"aggtrade[{idx}].l"),
        )

    def _parse_generic_trade_mapping(
        self,
        row: Mapping[str, Any],
        idx: int,
    ) -> AggTradeRecord:
        trade_id = self._pick_required_int(
            row,
            ["trade_id", "id", "a"],
            field_name=f"trades[{idx}].trade_id",
        )
        price = self._pick_required_decimal(
            row,
            ["price", "p"],
            field_name=f"trades[{idx}].price",
        )
        qty = self._pick_required_decimal(
            row,
            ["qty", "quantity", "q"],
            field_name=f"trades[{idx}].qty",
        )
        ts_ms = self._pick_required_int(
            row,
            ["ts_ms", "timestamp_ms", "timestamp", "T"],
            field_name=f"trades[{idx}].ts_ms",
        )
        is_buyer_maker = self._pick_required_bool(
            row,
            ["is_buyer_maker", "buyer_is_maker", "m"],
            field_name=f"trades[{idx}].is_buyer_maker",
        )
        first_trade_id = self._pick_optional_int(
            row,
            ["first_trade_id", "first_id", "f"],
        )
        last_trade_id = self._pick_optional_int(
            row,
            ["last_trade_id", "last_id", "l"],
        )

        return AggTradeRecord(
            trade_id=trade_id,
            price=price,
            qty=qty,
            ts_ms=ts_ms,
            is_buyer_maker=is_buyer_maker,
            first_trade_id=first_trade_id,
            last_trade_id=last_trade_id,
        )

    # ========================================================
    # Validation
    # ========================================================

    def _validate_trades_or_raise(
        self,
        trades: Sequence[AggTradeRecord],
    ) -> List[AggTradeRecord]:
        if len(trades) == 0:
            raise RuntimeError("validated trades must not be empty")

        prev_ts_ms: Optional[int] = None
        prev_trade_id: Optional[int] = None
        validated: List[AggTradeRecord] = []

        for idx, trade in enumerate(trades):
            if trade.trade_id < 0:
                raise RuntimeError(f"trade[{idx}] trade_id must be >= 0")
            if trade.ts_ms <= 0:
                raise RuntimeError(f"trade[{idx}] ts_ms must be > 0")
            if trade.price <= Decimal("0"):
                raise RuntimeError(f"trade[{idx}] price must be > 0")
            if trade.qty <= Decimal("0"):
                raise RuntimeError(f"trade[{idx}] qty must be > 0")

            if prev_ts_ms is not None:
                if trade.ts_ms < prev_ts_ms:
                    raise RuntimeError("aggTrade sequence must be non-decreasing by ts_ms")
                if trade.ts_ms == prev_ts_ms and prev_trade_id is not None and trade.trade_id < prev_trade_id:
                    raise RuntimeError("aggTrade sequence must be non-decreasing by trade_id when ts_ms is equal")

            if trade.first_trade_id is not None and trade.last_trade_id is not None:
                if trade.last_trade_id < trade.first_trade_id:
                    raise RuntimeError(f"trade[{idx}] last_trade_id < first_trade_id")

            prev_ts_ms = trade.ts_ms
            prev_trade_id = trade.trade_id
            validated.append(trade)

        return validated

    # ========================================================
    # Classification
    # ========================================================

    def _classify_aggression_bias(self, delta_ratio_pct: Decimal) -> str:
        threshold = self._dominance_threshold_pct

        if delta_ratio_pct >= threshold:
            return "aggressive_buy_dominant"
        if delta_ratio_pct <= -threshold:
            return "aggressive_sell_dominant"
        return "balanced"

    def _classify_cvd_trend(self, delta_ratio_pct: Decimal) -> str:
        threshold = self._dominance_threshold_pct

        if delta_ratio_pct >= threshold:
            return "rising_strong"
        if delta_ratio_pct > Decimal("0"):
            return "rising"
        if delta_ratio_pct <= -threshold:
            return "falling_strong"
        if delta_ratio_pct < Decimal("0"):
            return "falling"
        return "flat"

    def _classify_divergence(
        self,
        *,
        price_change_pct: Decimal,
        delta_ratio_pct: Decimal,
    ) -> str:
        price_th = self._divergence_price_threshold_pct
        delta_th = self._divergence_delta_threshold_pct

        if price_change_pct >= price_th and delta_ratio_pct <= -delta_th:
            return "bearish_divergence"
        if price_change_pct <= -price_th and delta_ratio_pct >= delta_th:
            return "bullish_divergence"
        return "none"

    # ========================================================
    # Presentation
    # ========================================================

    def _build_korean_summary(
        self,
        *,
        symbol: str,
        total_trades: int,
        total_qty: Decimal,
        aggressive_buy_qty: Decimal,
        aggressive_sell_qty: Decimal,
        net_delta_qty: Decimal,
        cvd: Decimal,
        delta_ratio_pct: Decimal,
        buy_share_pct: Decimal,
        sell_share_pct: Decimal,
        price_open: Decimal,
        price_close: Decimal,
        price_change_pct: Decimal,
        aggression_bias: str,
        cvd_trend: str,
        divergence: str,
    ) -> str:
        return (
            f"{symbol} Order Flow 분석 결과 총 체결 수는 {total_trades}건이며 "
            f"총 체결 수량은 {self._fmt_decimal(total_qty, 6)} 입니다. "
            f"공격적 매수 수량은 {self._fmt_decimal(aggressive_buy_qty, 6)}, "
            f"공격적 매도 수량은 {self._fmt_decimal(aggressive_sell_qty, 6)} 이며 "
            f"순 델타는 {self._fmt_decimal(net_delta_qty, 6)}, 누적 CVD 는 {self._fmt_decimal(cvd, 6)} 입니다. "
            f"매수 비중은 {self._fmt_decimal(buy_share_pct, 2)}%, 매도 비중은 {self._fmt_decimal(sell_share_pct, 2)}%, "
            f"델타 비율은 {self._fmt_decimal(delta_ratio_pct, 2)}% 입니다. "
            f"가격은 {self._fmt_decimal(price_open, 2)} 에서 {self._fmt_decimal(price_close, 2)} 로 변해 "
            f"{self._fmt_decimal(price_change_pct, 4)}% 움직였고, "
            f"현재 수급 우위는 {aggression_bias}, CVD 추세는 {cvd_trend}, 다이버전스 판정은 {divergence} 입니다."
        )

    def _build_dashboard_payload(
        self,
        *,
        symbol: str,
        as_of_ms: int,
        total_trades: int,
        total_qty: Decimal,
        aggressive_buy_qty: Decimal,
        aggressive_sell_qty: Decimal,
        buy_trade_count: int,
        sell_trade_count: int,
        net_delta_qty: Decimal,
        cvd: Decimal,
        delta_ratio_pct: Decimal,
        buy_share_pct: Decimal,
        sell_share_pct: Decimal,
        price_open: Decimal,
        price_close: Decimal,
        price_change_pct: Decimal,
        average_trade_qty: Decimal,
        max_trade_qty: Decimal,
        aggression_bias: str,
        cvd_trend: str,
        divergence: str,
        cvd_path_tail: Sequence[OrderFlowCvdPoint],
        analyst_summary_ko: str,
    ) -> Dict[str, Any]:
        return {
            "symbol": symbol,
            "as_of_ms": as_of_ms,
            "summary": {
                "total_trades": total_trades,
                "total_qty": self._fmt_decimal(total_qty, 6),
                "average_trade_qty": self._fmt_decimal(average_trade_qty, 6),
                "max_trade_qty": self._fmt_decimal(max_trade_qty, 6),
            },
            "orderflow": {
                "aggressive_buy_qty": self._fmt_decimal(aggressive_buy_qty, 6),
                "aggressive_sell_qty": self._fmt_decimal(aggressive_sell_qty, 6),
                "buy_trade_count": buy_trade_count,
                "sell_trade_count": sell_trade_count,
                "buy_share_pct": self._fmt_decimal(buy_share_pct, 4),
                "sell_share_pct": self._fmt_decimal(sell_share_pct, 4),
                "net_delta_qty": self._fmt_decimal(net_delta_qty, 6),
                "delta_ratio_pct": self._fmt_decimal(delta_ratio_pct, 4),
                "aggression_bias": aggression_bias,
                "cvd_trend": cvd_trend,
                "divergence": divergence,
            },
            "price_context": {
                "price_open": self._fmt_decimal(price_open, 2),
                "price_close": self._fmt_decimal(price_close, 2),
                "price_change_pct": self._fmt_decimal(price_change_pct, 4),
            },
            "cvd": {
                "cvd": self._fmt_decimal(cvd, 6),
                "path_tail": [
                    {
                        "seq": point.seq,
                        "trade_id": point.trade_id,
                        "ts_ms": point.ts_ms,
                        "price": self._fmt_decimal(point.price, 2),
                        "qty": self._fmt_decimal(point.qty, 6),
                        "aggressive_side": point.aggressive_side,
                        "signed_qty": self._fmt_decimal(point.signed_qty, 6),
                        "cvd": self._fmt_decimal(point.cvd, 6),
                    }
                    for point in cvd_path_tail
                ],
            },
            "analyst_summary_ko": analyst_summary_ko,
        }

    # ========================================================
    # Extractors
    # ========================================================

    def _require_decimal_from_mapping(
        self,
        row: Mapping[str, Any],
        key: str,
        field_name: str,
    ) -> Decimal:
        if key not in row:
            raise RuntimeError(f"Missing required decimal field: {field_name}")
        return self._to_decimal(row[key], field_name)

    def _require_int_from_mapping(
        self,
        row: Mapping[str, Any],
        key: str,
        field_name: str,
    ) -> int:
        if key not in row:
            raise RuntimeError(f"Missing required int field: {field_name}")
        return self._to_int(row[key], field_name)

    def _require_bool_from_mapping(
        self,
        row: Mapping[str, Any],
        key: str,
        field_name: str,
    ) -> bool:
        if key not in row:
            raise RuntimeError(f"Missing required bool field: {field_name}")
        return self._to_bool(row[key], field_name)

    def _optional_int_from_mapping(
        self,
        row: Mapping[str, Any],
        key: str,
        field_name: str,
    ) -> Optional[int]:
        if key not in row or row[key] is None:
            return None
        return self._to_int(row[key], field_name)

    def _pick_required_decimal(
        self,
        row: Mapping[str, Any],
        candidate_keys: Sequence[str],
        field_name: str,
    ) -> Decimal:
        for key in candidate_keys:
            if key in row and row[key] is not None:
                return self._to_decimal(row[key], field_name)
        raise RuntimeError(f"Missing required decimal field: {field_name}; candidates={candidate_keys}")

    def _pick_required_int(
        self,
        row: Mapping[str, Any],
        candidate_keys: Sequence[str],
        field_name: str,
    ) -> int:
        for key in candidate_keys:
            if key in row and row[key] is not None:
                return self._to_int(row[key], field_name)
        raise RuntimeError(f"Missing required int field: {field_name}; candidates={candidate_keys}")

    def _pick_required_bool(
        self,
        row: Mapping[str, Any],
        candidate_keys: Sequence[str],
        field_name: str,
    ) -> bool:
        for key in candidate_keys:
            if key in row and row[key] is not None:
                return self._to_bool(row[key], field_name)
        raise RuntimeError(f"Missing required bool field: {field_name}; candidates={candidate_keys}")

    def _pick_optional_int(
        self,
        row: Mapping[str, Any],
        candidate_keys: Sequence[str],
    ) -> Optional[int]:
        for key in candidate_keys:
            if key in row and row[key] is not None:
                return self._to_int(row[key], key)
        return None

    # ========================================================
    # Utility
    # ========================================================

    def _calc_price_change_pct(
        self,
        *,
        price_open: Decimal,
        price_close: Decimal,
    ) -> Decimal:
        if price_open <= Decimal("0") or price_close <= Decimal("0"):
            raise RuntimeError("price_open/price_close must be > 0")
        return ((price_close - price_open) / price_open) * Decimal("100")

    def _require_non_empty_str(self, value: Any, field_name: str) -> str:
        if not isinstance(value, str):
            raise RuntimeError(f"{field_name} must be string")
        normalized = value.strip()
        if not normalized:
            raise RuntimeError(f"{field_name} must not be empty")
        return normalized

    def _require_positive_int(self, value: Any, field_name: str) -> int:
        ivalue = self._to_int(value, field_name)
        if ivalue <= 0:
            raise RuntimeError(f"{field_name} must be > 0")
        return ivalue

    def _require_non_negative_decimal(self, value: Any, field_name: str) -> Decimal:
        dec = self._to_decimal(value, field_name)
        if dec < Decimal("0"):
            raise RuntimeError(f"{field_name} must be >= 0")
        return dec

    def _to_decimal(self, value: Any, field_name: str) -> Decimal:
        if isinstance(value, bool):
            raise RuntimeError(f"Invalid decimal field type for {field_name}: bool")
        try:
            dec = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise RuntimeError(f"Invalid decimal field for {field_name}") from exc
        if not dec.is_finite():
            raise RuntimeError(f"Non-finite decimal field for {field_name}")
        return dec

    def _to_int(self, value: Any, field_name: str) -> int:
        if isinstance(value, bool):
            raise RuntimeError(f"Invalid int field type for {field_name}: bool")
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Invalid int field for {field_name}") from exc

    def _to_bool(self, value: Any, field_name: str) -> bool:
        if isinstance(value, bool):
            return value
        raise RuntimeError(f"Invalid bool field for {field_name}")

    def _fmt_decimal(self, value: Decimal, scale: int) -> str:
        if scale < 0:
            raise RuntimeError("scale must be >= 0")
        quant = Decimal("1").scaleb(-scale)
        normalized = value.quantize(quant, rounding=ROUND_HALF_UP)
        return f"{normalized:.{scale}f}"


__all__ = [
    "AggTradeRecord",
    "OrderFlowCvdPoint",
    "OrderFlowCvdReport",
    "OrderFlowCvdEngine",
]