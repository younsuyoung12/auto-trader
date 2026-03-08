"""
========================================================
FILE: analysis/volume_profile.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할:
- Binance Kline 시계열로부터 Volume Profile 을 계산한다.
- 가격대별 거래량 분포를 기반으로 POC / VAH / VAL / HVN / LVN 을 산출한다.
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
- BinanceKline 시퀀스는 시간 오름차순이어야 한다.
- 각 kline 은 open/high/low/close > 0 이어야 한다.
- high_price >= low_price 이어야 한다.
- volume 또는 quote_asset_volume 중 선택한 basis 의 거래량은 >= 0 이어야 한다.

출력:
- VolumeProfileReport
  - poc_price
  - value_area_low / value_area_high
  - hvn / lvn
  - 현재 가격의 profile 위치
  - dashboard_payload

변경 이력:
2026-03-08
- 신규 생성
- Volume Profile Feature Engine 추가
- POC / VAH / VAL / HVN / LVN 계산 추가
- market_researcher / quant_analyst / entry_pipeline 연동용 구조 확정
========================================================
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from typing import Any, Dict, List, Mapping, Sequence

from analysis.binance_market_fetcher import BinanceKline

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VolumeProfileBucket:
    bucket_index: int
    price_low: Decimal
    price_high: Decimal
    price_mid: Decimal
    volume: Decimal
    share_pct: Decimal

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class VolumeProfileNode:
    price_low: Decimal
    price_high: Decimal
    price_mid: Decimal
    volume: Decimal
    share_pct: Decimal

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class VolumeProfileReport:
    symbol: str
    as_of_ms: int
    volume_basis: str
    bucket_size: Decimal
    value_area_pct: Decimal
    total_volume: Decimal
    range_low: Decimal
    range_high: Decimal
    current_price: Decimal
    poc_price: Decimal
    poc_zone_low: Decimal
    poc_zone_high: Decimal
    poc_volume: Decimal
    poc_share_pct: Decimal
    value_area_low: Decimal
    value_area_high: Decimal
    value_area_coverage_pct: Decimal
    poc_distance_bps: Decimal
    price_location: str
    hvn_nodes: List[VolumeProfileNode]
    lvn_nodes: List[VolumeProfileNode]
    buckets: List[VolumeProfileBucket]
    analyst_summary_ko: str
    dashboard_payload: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class VolumeProfileEngine:
    """
    Kline 기반 Volume Profile 계산기.

    사용 예:
        engine = VolumeProfileEngine(
            bucket_size=Decimal("50"),
            value_area_pct=Decimal("0.70"),
            volume_basis="quote",
            hvn_count=3,
            lvn_count=3,
        )
        report = engine.build(symbol="BTCUSDT", klines=klines)
    """

    def __init__(
        self,
        *,
        bucket_size: Decimal,
        value_area_pct: Decimal,
        volume_basis: str,
        hvn_count: int = 3,
        lvn_count: int = 3,
    ) -> None:
        self._bucket_size = self._require_positive_decimal(bucket_size, "bucket_size")
        self._value_area_pct = self._require_ratio_decimal(value_area_pct, "value_area_pct")
        self._volume_basis = self._normalize_volume_basis(volume_basis)
        self._hvn_count = self._require_positive_int(hvn_count, "hvn_count")
        self._lvn_count = self._require_positive_int(lvn_count, "lvn_count")

    # ========================================================
    # Public API
    # ========================================================

    def build(
        self,
        *,
        symbol: str,
        klines: Sequence[BinanceKline],
    ) -> VolumeProfileReport:
        symbol_norm = self._require_non_empty_str(symbol, "symbol")
        validated = self._validate_klines_or_raise(klines)
        current_price = validated[-1].close_price
        as_of_ms = validated[-1].close_time_ms

        range_low = min(k.low_price for k in validated)
        range_high = max(k.high_price for k in validated)

        if range_low <= Decimal("0") or range_high <= Decimal("0"):
            raise RuntimeError("Volume profile price range must be > 0")
        if range_high < range_low:
            raise RuntimeError("Volume profile range_high must be >= range_low")

        bucket_map = self._build_bucket_volume_map(validated)
        if not bucket_map:
            raise RuntimeError("Volume profile bucket map is empty")

        total_volume = sum(bucket_map.values(), Decimal("0"))
        if total_volume <= Decimal("0"):
            raise RuntimeError("Volume profile total_volume must be > 0")

        poc_index = self._select_poc_index_or_raise(bucket_map)
        poc_bucket = self._build_bucket(bucket_index=poc_index, volume=bucket_map[poc_index], total_volume=total_volume)

        value_area_indices = self._build_value_area_indices_or_raise(
            bucket_map=bucket_map,
            poc_index=poc_index,
            total_volume=total_volume,
        )
        value_area_low_idx = min(value_area_indices)
        value_area_high_idx = max(value_area_indices)

        value_area_low = self._bucket_low_from_index(value_area_low_idx)
        value_area_high = self._bucket_high_from_index(value_area_high_idx)

        value_area_volume = sum((bucket_map[idx] for idx in value_area_indices), Decimal("0"))
        value_area_coverage_pct = (value_area_volume / total_volume) * Decimal("100")

        poc_distance_bps = self._calc_distance_bps(current_price=current_price, reference_price=poc_bucket.price_mid)
        price_location = self._classify_price_location(
            current_price=current_price,
            poc_price=poc_bucket.price_mid,
            value_area_low=value_area_low,
            value_area_high=value_area_high,
        )

        buckets_sorted = self._build_sorted_buckets(bucket_map=bucket_map, total_volume=total_volume)
        hvn_nodes = self._select_hvn_nodes(buckets=buckets_sorted)
        lvn_nodes = self._select_lvn_nodes(buckets=buckets_sorted)

        analyst_summary_ko = self._build_korean_summary(
            symbol=symbol_norm,
            current_price=current_price,
            poc_bucket=poc_bucket,
            value_area_low=value_area_low,
            value_area_high=value_area_high,
            value_area_coverage_pct=value_area_coverage_pct,
            price_location=price_location,
            poc_distance_bps=poc_distance_bps,
            hvn_nodes=hvn_nodes,
            lvn_nodes=lvn_nodes,
        )

        dashboard_payload = self._build_dashboard_payload(
            symbol=symbol_norm,
            as_of_ms=as_of_ms,
            current_price=current_price,
            range_low=range_low,
            range_high=range_high,
            total_volume=total_volume,
            poc_bucket=poc_bucket,
            value_area_low=value_area_low,
            value_area_high=value_area_high,
            value_area_coverage_pct=value_area_coverage_pct,
            poc_distance_bps=poc_distance_bps,
            price_location=price_location,
            hvn_nodes=hvn_nodes,
            lvn_nodes=lvn_nodes,
            buckets=buckets_sorted,
            analyst_summary_ko=analyst_summary_ko,
        )

        logger.info(
            "Volume profile built: symbol=%s as_of_ms=%s poc=%s val=%s vah=%s location=%s buckets=%s",
            symbol_norm,
            as_of_ms,
            self._fmt_decimal(poc_bucket.price_mid, 2),
            self._fmt_decimal(value_area_low, 2),
            self._fmt_decimal(value_area_high, 2),
            price_location,
            len(buckets_sorted),
        )

        return VolumeProfileReport(
            symbol=symbol_norm,
            as_of_ms=as_of_ms,
            volume_basis=self._volume_basis,
            bucket_size=self._bucket_size,
            value_area_pct=self._value_area_pct,
            total_volume=total_volume,
            range_low=range_low,
            range_high=range_high,
            current_price=current_price,
            poc_price=poc_bucket.price_mid,
            poc_zone_low=poc_bucket.price_low,
            poc_zone_high=poc_bucket.price_high,
            poc_volume=poc_bucket.volume,
            poc_share_pct=poc_bucket.share_pct,
            value_area_low=value_area_low,
            value_area_high=value_area_high,
            value_area_coverage_pct=value_area_coverage_pct,
            poc_distance_bps=poc_distance_bps,
            price_location=price_location,
            hvn_nodes=hvn_nodes,
            lvn_nodes=lvn_nodes,
            buckets=buckets_sorted,
            analyst_summary_ko=analyst_summary_ko,
            dashboard_payload=dashboard_payload,
        )

    # ========================================================
    # Core build
    # ========================================================

    def _build_bucket_volume_map(
        self,
        klines: Sequence[BinanceKline],
    ) -> Dict[int, Decimal]:
        bucket_map: Dict[int, Decimal] = {}

        for kline in klines:
            kline_volume = self._extract_kline_volume(kline)
            if kline_volume == Decimal("0"):
                continue

            low_idx = self._bucket_index_from_price(kline.low_price)
            high_idx = self._bucket_index_from_price(kline.high_price)

            if high_idx < low_idx:
                raise RuntimeError("Volume profile bucket index range is invalid")

            touched_count = (high_idx - low_idx) + 1
            if touched_count <= 0:
                raise RuntimeError("Volume profile touched_count must be > 0")

            distributed_volume = kline_volume / Decimal(touched_count)

            for bucket_index in range(low_idx, high_idx + 1):
                if bucket_index not in bucket_map:
                    bucket_map[bucket_index] = Decimal("0")
                bucket_map[bucket_index] += distributed_volume

        if not bucket_map:
            raise RuntimeError("No volume was accumulated into any volume-profile bucket")

        return bucket_map

    def _select_poc_index_or_raise(self, bucket_map: Mapping[int, Decimal]) -> int:
        if not bucket_map:
            raise RuntimeError("bucket_map must not be empty")

        selected_index = None
        selected_volume = None

        for idx in sorted(bucket_map.keys()):
            volume = bucket_map[idx]
            if selected_index is None:
                selected_index = idx
                selected_volume = volume
                continue

            if volume > selected_volume:  # type: ignore[operator]
                selected_index = idx
                selected_volume = volume

        if selected_index is None:
            raise RuntimeError("Failed to select POC bucket index")

        return selected_index

    def _build_value_area_indices_or_raise(
        self,
        *,
        bucket_map: Mapping[int, Decimal],
        poc_index: int,
        total_volume: Decimal,
    ) -> List[int]:
        if total_volume <= Decimal("0"):
            raise RuntimeError("total_volume must be > 0")
        if poc_index not in bucket_map:
            raise RuntimeError("poc_index not found in bucket_map")

        target_volume = total_volume * self._value_area_pct
        sorted_indices = sorted(bucket_map.keys())

        left_pos = sorted_indices.index(poc_index)
        right_pos = left_pos

        selected: set[int] = {poc_index}
        cumulative = bucket_map[poc_index]

        while cumulative < target_volume:
            left_candidate_pos = left_pos - 1
            right_candidate_pos = right_pos + 1

            has_left = left_candidate_pos >= 0
            has_right = right_candidate_pos < len(sorted_indices)

            if not has_left and not has_right:
                break

            left_volume = bucket_map[sorted_indices[left_candidate_pos]] if has_left else None
            right_volume = bucket_map[sorted_indices[right_candidate_pos]] if has_right else None

            if has_left and has_right:
                if left_volume > right_volume:  # type: ignore[operator]
                    chosen_side = "left"
                elif right_volume > left_volume:  # type: ignore[operator]
                    chosen_side = "right"
                else:
                    chosen_side = "left"
            elif has_left:
                chosen_side = "left"
            else:
                chosen_side = "right"

            if chosen_side == "left":
                chosen_index = sorted_indices[left_candidate_pos]
                selected.add(chosen_index)
                cumulative += bucket_map[chosen_index]
                left_pos = left_candidate_pos
            else:
                chosen_index = sorted_indices[right_candidate_pos]
                selected.add(chosen_index)
                cumulative += bucket_map[chosen_index]
                right_pos = right_candidate_pos

        if not selected:
            raise RuntimeError("Volume profile value area selection is empty")

        return sorted(selected)

    def _build_sorted_buckets(
        self,
        *,
        bucket_map: Mapping[int, Decimal],
        total_volume: Decimal,
    ) -> List[VolumeProfileBucket]:
        buckets: List[VolumeProfileBucket] = []
        for idx in sorted(bucket_map.keys()):
            buckets.append(
                self._build_bucket(
                    bucket_index=idx,
                    volume=bucket_map[idx],
                    total_volume=total_volume,
                )
            )
        if not buckets:
            raise RuntimeError("Volume profile buckets list is empty")
        return buckets

    def _build_bucket(
        self,
        *,
        bucket_index: int,
        volume: Decimal,
        total_volume: Decimal,
    ) -> VolumeProfileBucket:
        if total_volume <= Decimal("0"):
            raise RuntimeError("total_volume must be > 0")
        if volume < Decimal("0"):
            raise RuntimeError("bucket volume must be >= 0")

        price_low = self._bucket_low_from_index(bucket_index)
        price_high = self._bucket_high_from_index(bucket_index)
        price_mid = price_low + (self._bucket_size / Decimal("2"))
        share_pct = (volume / total_volume) * Decimal("100")

        return VolumeProfileBucket(
            bucket_index=bucket_index,
            price_low=price_low,
            price_high=price_high,
            price_mid=price_mid,
            volume=volume,
            share_pct=share_pct,
        )

    # ========================================================
    # HVN / LVN
    # ========================================================

    def _select_hvn_nodes(self, buckets: Sequence[VolumeProfileBucket]) -> List[VolumeProfileNode]:
        positive = [b for b in buckets if b.volume > Decimal("0")]
        if not positive:
            raise RuntimeError("No positive-volume buckets available for HVN selection")

        ranked = sorted(
            positive,
            key=lambda x: (-x.volume, x.bucket_index),
        )
        chosen = ranked[: self._hvn_count]

        return [
            VolumeProfileNode(
                price_low=item.price_low,
                price_high=item.price_high,
                price_mid=item.price_mid,
                volume=item.volume,
                share_pct=item.share_pct,
            )
            for item in chosen
        ]

    def _select_lvn_nodes(self, buckets: Sequence[VolumeProfileBucket]) -> List[VolumeProfileNode]:
        positive = [b for b in buckets if b.volume > Decimal("0")]
        if not positive:
            raise RuntimeError("No positive-volume buckets available for LVN selection")

        ranked = sorted(
            positive,
            key=lambda x: (x.volume, x.bucket_index),
        )
        chosen = ranked[: self._lvn_count]

        return [
            VolumeProfileNode(
                price_low=item.price_low,
                price_high=item.price_high,
                price_mid=item.price_mid,
                volume=item.volume,
                share_pct=item.share_pct,
            )
            for item in chosen
        ]

    # ========================================================
    # Classification / presentation
    # ========================================================

    def _classify_price_location(
        self,
        *,
        current_price: Decimal,
        poc_price: Decimal,
        value_area_low: Decimal,
        value_area_high: Decimal,
    ) -> str:
        if current_price <= Decimal("0"):
            raise RuntimeError("current_price must be > 0")
        if poc_price <= Decimal("0"):
            raise RuntimeError("poc_price must be > 0")
        if value_area_low <= Decimal("0") or value_area_high <= Decimal("0"):
            raise RuntimeError("value_area_low/high must be > 0")
        if value_area_high < value_area_low:
            raise RuntimeError("value_area_high must be >= value_area_low")

        if current_price > value_area_high:
            return "above_value_area"
        if current_price < value_area_low:
            return "below_value_area"
        if current_price > poc_price:
            return "inside_value_area_above_poc"
        if current_price < poc_price:
            return "inside_value_area_below_poc"
        return "at_poc"

    def _build_korean_summary(
        self,
        *,
        symbol: str,
        current_price: Decimal,
        poc_bucket: VolumeProfileBucket,
        value_area_low: Decimal,
        value_area_high: Decimal,
        value_area_coverage_pct: Decimal,
        price_location: str,
        poc_distance_bps: Decimal,
        hvn_nodes: Sequence[VolumeProfileNode],
        lvn_nodes: Sequence[VolumeProfileNode],
    ) -> str:
        hvn_text = ", ".join(self._fmt_decimal(node.price_mid, 2) for node in hvn_nodes)
        lvn_text = ", ".join(self._fmt_decimal(node.price_mid, 2) for node in lvn_nodes)

        return (
            f"{symbol} Volume Profile 분석 결과 현재 가격은 {self._fmt_decimal(current_price, 2)} 이며, "
            f"POC 는 {self._fmt_decimal(poc_bucket.price_mid, 2)} 입니다. "
            f"Value Area 는 {self._fmt_decimal(value_area_low, 2)} ~ {self._fmt_decimal(value_area_high, 2)} 구간이며 "
            f"전체 거래량의 {self._fmt_decimal(value_area_coverage_pct, 2)}% 를 포함합니다. "
            f"현재 가격 위치는 {price_location} 이고, POC 와의 거리 차이는 {self._fmt_decimal(poc_distance_bps, 2)} bps 입니다. "
            f"주요 HVN 은 {hvn_text}, 주요 LVN 은 {lvn_text} 입니다."
        )

    def _build_dashboard_payload(
        self,
        *,
        symbol: str,
        as_of_ms: int,
        current_price: Decimal,
        range_low: Decimal,
        range_high: Decimal,
        total_volume: Decimal,
        poc_bucket: VolumeProfileBucket,
        value_area_low: Decimal,
        value_area_high: Decimal,
        value_area_coverage_pct: Decimal,
        poc_distance_bps: Decimal,
        price_location: str,
        hvn_nodes: Sequence[VolumeProfileNode],
        lvn_nodes: Sequence[VolumeProfileNode],
        buckets: Sequence[VolumeProfileBucket],
        analyst_summary_ko: str,
    ) -> Dict[str, Any]:
        return {
            "symbol": symbol,
            "as_of_ms": as_of_ms,
            "config": {
                "bucket_size": self._fmt_decimal(self._bucket_size, 6),
                "value_area_pct": self._fmt_decimal(self._value_area_pct * Decimal("100"), 2),
                "volume_basis": self._volume_basis,
                "hvn_count": self._hvn_count,
                "lvn_count": self._lvn_count,
            },
            "range": {
                "range_low": self._fmt_decimal(range_low, 2),
                "range_high": self._fmt_decimal(range_high, 2),
                "current_price": self._fmt_decimal(current_price, 2),
            },
            "profile": {
                "total_volume": self._fmt_decimal(total_volume, 6),
                "poc_price": self._fmt_decimal(poc_bucket.price_mid, 2),
                "poc_zone_low": self._fmt_decimal(poc_bucket.price_low, 2),
                "poc_zone_high": self._fmt_decimal(poc_bucket.price_high, 2),
                "poc_volume": self._fmt_decimal(poc_bucket.volume, 6),
                "poc_share_pct": self._fmt_decimal(poc_bucket.share_pct, 4),
                "value_area_low": self._fmt_decimal(value_area_low, 2),
                "value_area_high": self._fmt_decimal(value_area_high, 2),
                "value_area_coverage_pct": self._fmt_decimal(value_area_coverage_pct, 4),
                "poc_distance_bps": self._fmt_decimal(poc_distance_bps, 4),
                "price_location": price_location,
            },
            "nodes": {
                "hvn": [
                    {
                        "price_low": self._fmt_decimal(node.price_low, 2),
                        "price_high": self._fmt_decimal(node.price_high, 2),
                        "price_mid": self._fmt_decimal(node.price_mid, 2),
                        "volume": self._fmt_decimal(node.volume, 6),
                        "share_pct": self._fmt_decimal(node.share_pct, 4),
                    }
                    for node in hvn_nodes
                ],
                "lvn": [
                    {
                        "price_low": self._fmt_decimal(node.price_low, 2),
                        "price_high": self._fmt_decimal(node.price_high, 2),
                        "price_mid": self._fmt_decimal(node.price_mid, 2),
                        "volume": self._fmt_decimal(node.volume, 6),
                        "share_pct": self._fmt_decimal(node.share_pct, 4),
                    }
                    for node in lvn_nodes
                ],
            },
            "buckets": [
                {
                    "bucket_index": bucket.bucket_index,
                    "price_low": self._fmt_decimal(bucket.price_low, 2),
                    "price_high": self._fmt_decimal(bucket.price_high, 2),
                    "price_mid": self._fmt_decimal(bucket.price_mid, 2),
                    "volume": self._fmt_decimal(bucket.volume, 6),
                    "share_pct": self._fmt_decimal(bucket.share_pct, 4),
                }
                for bucket in buckets
            ],
            "analyst_summary_ko": analyst_summary_ko,
        }

    # ========================================================
    # Validation
    # ========================================================

    def _validate_klines_or_raise(
        self,
        klines: Sequence[BinanceKline],
    ) -> List[BinanceKline]:
        if not isinstance(klines, Sequence):
            raise RuntimeError("klines must be a sequence")
        if len(klines) < 5:
            raise RuntimeError("Volume profile requires at least 5 klines")

        validated: List[BinanceKline] = []
        prev_open_time_ms = None

        for idx, item in enumerate(klines):
            if not isinstance(item, BinanceKline):
                raise RuntimeError(f"kline[{idx}] is not BinanceKline")

            if item.open_time_ms <= 0 or item.close_time_ms <= 0:
                raise RuntimeError(f"kline[{idx}] timestamps must be > 0")
            if item.close_time_ms < item.open_time_ms:
                raise RuntimeError(f"kline[{idx}] close_time_ms < open_time_ms")

            if prev_open_time_ms is not None and item.open_time_ms <= prev_open_time_ms:
                raise RuntimeError("Volume profile klines must be strictly increasing by open_time_ms")
            prev_open_time_ms = item.open_time_ms

            if item.open_price <= Decimal("0"):
                raise RuntimeError(f"kline[{idx}] open_price must be > 0")
            if item.high_price <= Decimal("0"):
                raise RuntimeError(f"kline[{idx}] high_price must be > 0")
            if item.low_price <= Decimal("0"):
                raise RuntimeError(f"kline[{idx}] low_price must be > 0")
            if item.close_price <= Decimal("0"):
                raise RuntimeError(f"kline[{idx}] close_price must be > 0")
            if item.high_price < item.low_price:
                raise RuntimeError(f"kline[{idx}] high_price < low_price")

            volume_value = self._extract_kline_volume(item)
            if volume_value < Decimal("0"):
                raise RuntimeError(f"kline[{idx}] selected volume basis must be >= 0")

            validated.append(item)

        return validated

    # ========================================================
    # Helpers
    # ========================================================

    def _extract_kline_volume(self, kline: BinanceKline) -> Decimal:
        if self._volume_basis == "base":
            volume = kline.volume
        elif self._volume_basis == "quote":
            volume = kline.quote_asset_volume
        else:
            raise RuntimeError(f"Unexpected volume_basis: {self._volume_basis}")

        if volume < Decimal("0"):
            raise RuntimeError("Selected kline volume must be >= 0")
        return volume

    def _bucket_index_from_price(self, price: Decimal) -> int:
        if price <= Decimal("0"):
            raise RuntimeError("bucket price must be > 0")
        ratio = price / self._bucket_size
        floored = ratio.to_integral_value(rounding=ROUND_FLOOR)
        return self._to_int(floored, "bucket_index")

    def _bucket_low_from_index(self, bucket_index: int) -> Decimal:
        return Decimal(bucket_index) * self._bucket_size

    def _bucket_high_from_index(self, bucket_index: int) -> Decimal:
        return (Decimal(bucket_index) + Decimal("1")) * self._bucket_size

    def _calc_distance_bps(
        self,
        *,
        current_price: Decimal,
        reference_price: Decimal,
    ) -> Decimal:
        if current_price <= Decimal("0") or reference_price <= Decimal("0"):
            raise RuntimeError("distance inputs must be > 0")
        return ((current_price - reference_price) / reference_price) * Decimal("10000")

    def _normalize_volume_basis(self, value: str) -> str:
        normalized = self._require_non_empty_str(value, "volume_basis").strip().lower()
        if normalized not in {"base", "quote"}:
            raise RuntimeError("volume_basis must be one of: base, quote")
        return normalized

    def _require_non_empty_str(self, value: Any, field_name: str) -> str:
        if not isinstance(value, str):
            raise RuntimeError(f"{field_name} must be string")
        normalized = value.strip()
        if not normalized:
            raise RuntimeError(f"{field_name} must not be empty")
        return normalized

    def _require_positive_decimal(self, value: Any, field_name: str) -> Decimal:
        dec = self._to_decimal(value, field_name)
        if dec <= Decimal("0"):
            raise RuntimeError(f"{field_name} must be > 0")
        return dec

    def _require_ratio_decimal(self, value: Any, field_name: str) -> Decimal:
        dec = self._to_decimal(value, field_name)
        if dec <= Decimal("0") or dec >= Decimal("1"):
            raise RuntimeError(f"{field_name} must satisfy 0 < value < 1")
        return dec

    def _require_positive_int(self, value: Any, field_name: str) -> int:
        ivalue = self._to_int(value, field_name)
        if ivalue <= 0:
            raise RuntimeError(f"{field_name} must be > 0")
        return ivalue

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

    def _fmt_decimal(self, value: Decimal, scale: int) -> str:
        if scale < 0:
            raise RuntimeError("scale must be >= 0")
        quant = Decimal("1").scaleb(-scale)
        normalized = value.quantize(quant, rounding=ROUND_CEILING if value < 0 else ROUND_FLOOR)
        return f"{normalized:.{scale}f}"


__all__ = [
    "VolumeProfileBucket",
    "VolumeProfileNode",
    "VolumeProfileReport",
    "VolumeProfileEngine",
]