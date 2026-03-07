"""
========================================================
FILE: analysis/trade_analyzer.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할:
- 내부 DB의 bt_trades / bt_events / bt_trade_snapshots 를 분석해
  최근 거래 성능, 손실 원인, 진입 실패 이유를 산출한다.
- 이 모듈은 "거래 분석(DB 기반)"만 담당한다.
- 주문 실행 / 포지션 변경 / TP·SL 수정은 절대 수행하지 않는다.

절대 원칙:
- STRICT · NO-FALLBACK
- settings.py(SSOT) 외 환경변수 직접 접근 금지
- state/db_core.py 외 DB 직접 연결 금지
- print() 금지 / logging 사용
- 데이터 누락/손상/모호성 발생 시 즉시 예외
- 민감정보 로그 금지

전제 테이블:
- bt_trades
- bt_events
- bt_trade_snapshots

변경 이력:
2026-03-07
- 신규 생성
- DB 기반 거래 분석기 추가

2026-03-08 (PATCH)
1) bt_events 실제 스키마(ts_utc / created_at) 기준으로 event_ts_ms SQL 정규화 추가
2) bt_events 에 top-level trade_id 가 없는 구조를 반영해 extra_json.trade_id → trade_id SQL 정규화 추가
3) bt_events 로딩을 SELECT * 에서 명시 컬럼 + canonical alias 방식으로 변경
4) EventRecord 파서는 정규화된 DB adapter 출력(event_ts_ms / trade_id)을 우선 해석하도록 유지/보강

2026-03-08 (PATCH 2)
1) bt_trade_snapshots 로딩을 SELECT * 에서 명시 컬럼 + canonical alias 방식으로 변경
2) bt_trade_snapshots.ts_ms → snapshot_ts_ms SQL alias 정규화 추가
3) Snapshot parser 는 snapshot_ts_ms 를 최우선으로 해석하도록 보강
4) DB 스키마 변경 없이 analyzer 입력 스키마만 정규화하도록 구조 고정
========================================================
"""

from __future__ import annotations

print("DEBUG TRADE_ANALYZER FILE:", __file__)

import json
import logging
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Mapping, Optional, Sequence

from sqlalchemy import bindparam, text

from settings import SETTINGS
from state.db_core import get_session

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TradeRecord:
    trade_id: int
    symbol: str
    side: str
    status: str
    entry_price: Decimal
    exit_price: Optional[Decimal]
    realized_pnl: Decimal
    opened_ts_ms: int
    closed_ts_ms: int
    exit_reason: Optional[str]


@dataclass(frozen=True)
class EventRecord:
    event_id: int
    ts_ms: int
    symbol: str
    event_type: str
    trade_id: Optional[int]
    side: Optional[str]
    reason: Optional[str]
    regime: Optional[str]
    extra_json: Optional[Dict[str, Any]]


@dataclass(frozen=True)
class TradeSnapshotRecord:
    snapshot_id: int
    trade_id: int
    ts_ms: int
    spread_bps: Decimal
    pattern_score: Decimal
    market_regime: str
    orderbook_imbalance: Decimal
    volatility_score: Optional[Decimal]


@dataclass(frozen=True)
class SidePerformance:
    side: str
    total: int
    wins: int
    losses: int
    breakeven: int
    win_rate_pct: Decimal
    total_pnl: Decimal
    avg_pnl: Decimal

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TradeAnalyzerSummary:
    symbol: str
    as_of_ts_ms: int
    total_trades: int
    wins: int
    losses: int
    breakeven: int
    win_rate_pct: Decimal
    total_pnl: Decimal
    avg_pnl: Decimal
    avg_win_pnl: Decimal
    avg_loss_pnl: Decimal
    long_performance: SidePerformance
    short_performance: SidePerformance
    entry_failure_reasons: List[str]
    loss_causes: List[str]
    recent_trade_briefs: List[Dict[str, Any]]
    analyst_summary_ko: str
    dashboard_payload: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class TradeAnalyzer:
    """
    최근 거래/이벤트/스냅샷 기반 거래 성능 분석기.
    """

    def __init__(self) -> None:
        self._symbol = self._require_str_setting("ANALYST_MARKET_SYMBOL")
        self._trade_limit = self._require_int_setting("ANALYST_TRADE_LOOKBACK_LIMIT")
        self._event_limit = self._require_int_setting("ANALYST_EVENT_LOOKBACK_LIMIT")
        self._pattern_entry_threshold = self._require_decimal_setting("ANALYST_PATTERN_ENTRY_THRESHOLD")
        self._spread_guard_bps = self._require_decimal_setting("ANALYST_SPREAD_GUARD_BPS")
        self._imbalance_guard_abs = self._require_decimal_setting("ANALYST_IMBALANCE_GUARD_ABS")

        if self._trade_limit < 5:
            raise RuntimeError("ANALYST_TRADE_LOOKBACK_LIMIT must be >= 5")
        if self._event_limit < 1:
            raise RuntimeError("ANALYST_EVENT_LOOKBACK_LIMIT must be >= 1")
        if self._pattern_entry_threshold <= Decimal("0"):
            raise RuntimeError("ANALYST_PATTERN_ENTRY_THRESHOLD must be > 0")
        if self._spread_guard_bps <= Decimal("0"):
            raise RuntimeError("ANALYST_SPREAD_GUARD_BPS must be > 0")
        if self._imbalance_guard_abs <= Decimal("0"):
            raise RuntimeError("ANALYST_IMBALANCE_GUARD_ABS must be > 0")

    # ========================================================
    # Public API
    # ========================================================

    def run(self) -> TradeAnalyzerSummary:
        trades = self._load_recent_closed_trades_or_raise(
            symbol=self._symbol,
            required_limit=self._trade_limit,
        )
        trade_ids = [t.trade_id for t in trades]
        events = self._load_recent_events(symbol=self._symbol, limit=self._event_limit)
        snapshots = self._load_trade_snapshots_or_raise(trade_ids=trade_ids)
        summary = self._build_summary(trades=trades, events=events, snapshots=snapshots)

        logger.info(
            "Trade analysis completed: symbol=%s trades=%s wins=%s losses=%s win_rate_pct=%s",
            summary.symbol,
            summary.total_trades,
            summary.wins,
            summary.losses,
            self._fmt_decimal(summary.win_rate_pct, 2),
        )
        return summary

    def build_prompt_context(self) -> Dict[str, Any]:
        return self.run().dashboard_payload

    # ========================================================
    # DB Load
    # ========================================================

    def _load_recent_closed_trades_or_raise(
        self,
        symbol: str,
        required_limit: int,
    ) -> List[TradeRecord]:
        raw_fetch_limit = required_limit * 6

        sql = text(
            """
            SELECT *
            FROM bt_trades
            WHERE symbol = :symbol
            ORDER BY id DESC
            LIMIT :limit
            """
        )

        with get_session() as session:
            rows = session.execute(
                sql,
                {
                    "symbol": symbol,
                    "limit": raw_fetch_limit,
                },
            ).mappings().all()

        if not rows:
            raise RuntimeError(f"No bt_trades rows found for symbol={symbol}")

        parsed_all = [self._parse_trade_row(row) for row in rows]
        closed_trades = [row for row in parsed_all if self._is_closed_trade(row)]

        if len(closed_trades) < required_limit:
            raise RuntimeError(
                f"Insufficient closed trades for analysis: required={required_limit}, got={len(closed_trades)}"
            )

        selected = closed_trades[:required_limit]
        selected_sorted = sorted(selected, key=lambda x: (x.closed_ts_ms, x.trade_id))
        self._validate_trade_order(selected_sorted)
        return selected_sorted

    def _load_recent_events(self, symbol: str, limit: int) -> List[EventRecord]:
        sql = text(
            """
            SELECT
                id,
                symbol,
                event_type,
                regime,
                source,
                side,
                price,
                qty,
                leverage,
                tp_pct,
                sl_pct,
                risk_pct,
                pnl_pct,
                reason,
                extra_json,
                is_test,
                ts_utc,
                created_at,
                (
                    EXTRACT(
                        EPOCH FROM COALESCE(ts_utc, created_at)
                    ) * 1000
                )::bigint AS event_ts_ms,
                CASE
                    WHEN extra_json IS NOT NULL
                         AND extra_json ? 'trade_id'
                         AND NULLIF(BTRIM(extra_json ->> 'trade_id'), '') IS NOT NULL
                         AND (extra_json ->> 'trade_id') ~ '^[0-9]+$'
                    THEN (extra_json ->> 'trade_id')::bigint
                    ELSE NULL
                END AS trade_id
            FROM bt_events
            WHERE symbol = :symbol
            ORDER BY id DESC
            LIMIT :limit
            """
        )

        with get_session() as session:
            rows = session.execute(
                sql,
                {
                    "symbol": symbol,
                    "limit": limit,
                },
            ).mappings().all()

        if not rows:
            logger.warning("No bt_events rows found for symbol=%s in recent window", symbol)
            return []

        parsed = [self._parse_event_row(row) for row in rows]
        return sorted(parsed, key=lambda x: (x.ts_ms, x.event_id))

    def _load_trade_snapshots_or_raise(
        self,
        trade_ids: Sequence[int],
    ) -> List[TradeSnapshotRecord]:
        if not trade_ids:
            raise RuntimeError("trade_ids must not be empty")

        sql = (
            text(
                """
                SELECT
                    id,
                    trade_id,
                    ts_ms AS snapshot_ts_ms,
                    spread_bps,
                    pattern_score,
                    market_regime,
                    orderbook_imbalance,
                    volatility_score
                FROM bt_trade_snapshots
                WHERE trade_id IN :trade_ids
                ORDER BY trade_id ASC, id ASC
                """
            ).bindparams(bindparam("trade_ids", expanding=True))
        )

        with get_session() as session:
            rows = session.execute(sql, {"trade_ids": list(trade_ids)}).mappings().all()

        if not rows:
            raise RuntimeError("No bt_trade_snapshots rows found for selected trades")

        parsed = [self._parse_trade_snapshot_row(row) for row in rows]
        by_trade_id: Dict[int, List[TradeSnapshotRecord]] = defaultdict(list)
        for row in parsed:
            by_trade_id[row.trade_id].append(row)

        missing_trade_ids = [trade_id for trade_id in trade_ids if trade_id not in by_trade_id]
        if missing_trade_ids:
            raise RuntimeError(f"Missing bt_trade_snapshots for trades: {missing_trade_ids}")

        for trade_id, items in by_trade_id.items():
            items_sorted = sorted(items, key=lambda x: (x.ts_ms, x.snapshot_id))
            self._validate_snapshot_order(trade_id=trade_id, snapshots=items_sorted)

        return parsed

    # ========================================================
    # Core Analysis
    # ========================================================

    def _build_summary(
        self,
        trades: Sequence[TradeRecord],
        events: Sequence[EventRecord],
        snapshots: Sequence[TradeSnapshotRecord],
    ) -> TradeAnalyzerSummary:
        total_trades = len(trades)
        wins = sum(1 for t in trades if t.realized_pnl > Decimal("0"))
        losses = sum(1 for t in trades if t.realized_pnl < Decimal("0"))
        breakeven = total_trades - wins - losses

        if total_trades <= 0:
            raise RuntimeError("total_trades must be > 0")

        total_pnl = sum((t.realized_pnl for t in trades), Decimal("0"))
        avg_pnl = total_pnl / Decimal(total_trades)

        win_trades = [t for t in trades if t.realized_pnl > Decimal("0")]
        loss_trades = [t for t in trades if t.realized_pnl < Decimal("0")]

        avg_win_pnl = self._avg_pnl_or_zero(win_trades)
        avg_loss_pnl = self._avg_pnl_or_zero(loss_trades)
        win_rate_pct = (Decimal(wins) / Decimal(total_trades)) * Decimal("100")

        long_performance = self._build_side_performance(trades, "LONG")
        short_performance = self._build_side_performance(trades, "SHORT")

        snapshots_by_trade = self._group_snapshots_by_trade(snapshots)
        events_by_trade = self._group_events_by_trade(events)

        entry_failure_reasons = self._extract_entry_failure_reasons(events)
        loss_causes = self._extract_loss_causes(
            loss_trades=loss_trades,
            snapshots_by_trade=snapshots_by_trade,
            events_by_trade=events_by_trade,
        )
        recent_trade_briefs = self._build_recent_trade_briefs(
            trades=trades,
            snapshots_by_trade=snapshots_by_trade,
        )

        analyst_summary_ko = self._build_korean_summary(
            symbol=self._symbol,
            total_trades=total_trades,
            wins=wins,
            losses=losses,
            breakeven=breakeven,
            win_rate_pct=win_rate_pct,
            total_pnl=total_pnl,
            avg_pnl=avg_pnl,
            avg_win_pnl=avg_win_pnl,
            avg_loss_pnl=avg_loss_pnl,
            long_perf=long_performance,
            short_perf=short_performance,
            entry_failure_reasons=entry_failure_reasons,
            loss_causes=loss_causes,
        )

        dashboard_payload = self._build_dashboard_payload(
            trades=trades,
            total_trades=total_trades,
            wins=wins,
            losses=losses,
            breakeven=breakeven,
            win_rate_pct=win_rate_pct,
            total_pnl=total_pnl,
            avg_pnl=avg_pnl,
            avg_win_pnl=avg_win_pnl,
            avg_loss_pnl=avg_loss_pnl,
            long_performance=long_performance,
            short_performance=short_performance,
            entry_failure_reasons=entry_failure_reasons,
            loss_causes=loss_causes,
            recent_trade_briefs=recent_trade_briefs,
            analyst_summary_ko=analyst_summary_ko,
        )

        as_of_ts_ms = trades[-1].closed_ts_ms

        return TradeAnalyzerSummary(
            symbol=self._symbol,
            as_of_ts_ms=as_of_ts_ms,
            total_trades=total_trades,
            wins=wins,
            losses=losses,
            breakeven=breakeven,
            win_rate_pct=win_rate_pct,
            total_pnl=total_pnl,
            avg_pnl=avg_pnl,
            avg_win_pnl=avg_win_pnl,
            avg_loss_pnl=avg_loss_pnl,
            long_performance=long_performance,
            short_performance=short_performance,
            entry_failure_reasons=entry_failure_reasons,
            loss_causes=loss_causes,
            recent_trade_briefs=recent_trade_briefs,
            analyst_summary_ko=analyst_summary_ko,
            dashboard_payload=dashboard_payload,
        )

    def _build_side_performance(self, trades: Sequence[TradeRecord], side: str) -> SidePerformance:
        side_norm = side.upper().strip()
        filtered = [t for t in trades if t.side == side_norm]

        total = len(filtered)
        wins = sum(1 for t in filtered if t.realized_pnl > Decimal("0"))
        losses = sum(1 for t in filtered if t.realized_pnl < Decimal("0"))
        breakeven = total - wins - losses
        total_pnl = sum((t.realized_pnl for t in filtered), Decimal("0"))
        avg_pnl = total_pnl / Decimal(total) if total > 0 else Decimal("0")
        win_rate_pct = (Decimal(wins) / Decimal(total) * Decimal("100")) if total > 0 else Decimal("0")

        return SidePerformance(
            side=side_norm,
            total=total,
            wins=wins,
            losses=losses,
            breakeven=breakeven,
            win_rate_pct=win_rate_pct,
            total_pnl=total_pnl,
            avg_pnl=avg_pnl,
        )

    def _extract_entry_failure_reasons(self, events: Sequence[EventRecord]) -> List[str]:
        counter: Counter[str] = Counter()

        for event in events:
            event_type_upper = event.event_type.upper().strip()
            if (
                "ENTRY_GUARD" in event_type_upper
                or "SKIP" in event_type_upper
                or "ENTRY" in event_type_upper and event.reason is not None
            ):
                reason = self._normalize_reason_string(
                    event.reason
                    or self._extract_reason_from_extra_json(event.extra_json)
                    or event.event_type
                )
                counter[reason] += 1

        if not counter:
            return ["none"]

        return [f"{reason}:{count}" for reason, count in counter.most_common(5)]

    def _extract_loss_causes(
        self,
        loss_trades: Sequence[TradeRecord],
        snapshots_by_trade: Mapping[int, Sequence[TradeSnapshotRecord]],
        events_by_trade: Mapping[int, Sequence[EventRecord]],
    ) -> List[str]:
        counter: Counter[str] = Counter()

        for trade in loss_trades:
            if trade.trade_id not in snapshots_by_trade:
                raise RuntimeError(f"Missing snapshots for loss trade_id={trade.trade_id}")

            per_trade_events = events_by_trade.get(trade.trade_id, [])
            reason_from_event = self._extract_loss_reason_from_events(per_trade_events)
            if reason_from_event is not None:
                counter[reason_from_event] += 1
                continue

            entry_snapshot = self._get_first_snapshot_or_raise(
                trade_id=trade.trade_id,
                snapshots=snapshots_by_trade[trade.trade_id],
            )
            inferred = self._infer_loss_cause_from_snapshot(entry_snapshot)
            counter[inferred] += 1

        if not counter:
            return ["none"]

        return [f"{reason}:{count}" for reason, count in counter.most_common(5)]

    def _build_recent_trade_briefs(
        self,
        trades: Sequence[TradeRecord],
        snapshots_by_trade: Mapping[int, Sequence[TradeSnapshotRecord]],
    ) -> List[Dict[str, Any]]:
        briefs: List[Dict[str, Any]] = []

        for trade in trades[-10:]:
            if trade.trade_id not in snapshots_by_trade:
                raise RuntimeError(f"Missing snapshots for trade_id={trade.trade_id}")

            first_snapshot = self._get_first_snapshot_or_raise(
                trade_id=trade.trade_id,
                snapshots=snapshots_by_trade[trade.trade_id],
            )

            briefs.append(
                {
                    "trade_id": trade.trade_id,
                    "side": trade.side,
                    "entry_price": self._fmt_decimal(trade.entry_price, 2),
                    "exit_price": self._fmt_decimal_optional(trade.exit_price, 2),
                    "realized_pnl": self._fmt_decimal(trade.realized_pnl, 4),
                    "opened_ts_ms": trade.opened_ts_ms,
                    "closed_ts_ms": trade.closed_ts_ms,
                    "exit_reason": trade.exit_reason,
                    "entry_context": {
                        "spread_bps": self._fmt_decimal(first_snapshot.spread_bps, 4),
                        "pattern_score": self._fmt_decimal(first_snapshot.pattern_score, 6),
                        "market_regime": first_snapshot.market_regime,
                        "orderbook_imbalance": self._fmt_decimal(first_snapshot.orderbook_imbalance, 6),
                        "volatility_score": self._fmt_decimal_optional(first_snapshot.volatility_score, 6),
                    },
                }
            )

        return briefs

    # ========================================================
    # Grouping / inference
    # ========================================================

    def _group_snapshots_by_trade(
        self,
        snapshots: Sequence[TradeSnapshotRecord],
    ) -> Dict[int, List[TradeSnapshotRecord]]:
        grouped: Dict[int, List[TradeSnapshotRecord]] = defaultdict(list)
        for snapshot in snapshots:
            grouped[snapshot.trade_id].append(snapshot)

        for trade_id, items in grouped.items():
            grouped[trade_id] = sorted(items, key=lambda x: (x.ts_ms, x.snapshot_id))
        return grouped

    def _group_events_by_trade(
        self,
        events: Sequence[EventRecord],
    ) -> Dict[int, List[EventRecord]]:
        grouped: Dict[int, List[EventRecord]] = defaultdict(list)
        for event in events:
            if event.trade_id is not None:
                grouped[event.trade_id].append(event)

        for trade_id, items in grouped.items():
            grouped[trade_id] = sorted(items, key=lambda x: (x.ts_ms, x.event_id))
        return grouped

    def _extract_loss_reason_from_events(
        self,
        events: Sequence[EventRecord],
    ) -> Optional[str]:
        for event in reversed(list(events)):
            event_type_upper = event.event_type.upper().strip()
            if "SL" in event_type_upper or "STOP" in event_type_upper or "LOSS" in event_type_upper:
                reason = self._normalize_reason_string(
                    event.reason or self._extract_reason_from_extra_json(event.extra_json) or event.event_type
                )
                return reason
            if event.reason:
                return self._normalize_reason_string(event.reason)
            extra_reason = self._extract_reason_from_extra_json(event.extra_json)
            if extra_reason:
                return self._normalize_reason_string(extra_reason)
        return None

    def _infer_loss_cause_from_snapshot(
        self,
        snapshot: TradeSnapshotRecord,
    ) -> str:
        regime_norm = snapshot.market_regime.strip().lower()

        if snapshot.pattern_score < self._pattern_entry_threshold:
            return "weak_pattern_entry"

        if snapshot.spread_bps > self._spread_guard_bps:
            return "wide_spread_entry"

        if abs(snapshot.orderbook_imbalance) > self._imbalance_guard_abs:
            return "depth_imbalance_entry"

        if "range" in regime_norm or "box" in regime_norm:
            return "range_market_entry"

        if "volatile" in regime_norm or "chaos" in regime_norm:
            return "high_volatility_entry"

        return "unclassified_loss_context"

    def _get_first_snapshot_or_raise(
        self,
        trade_id: int,
        snapshots: Sequence[TradeSnapshotRecord],
    ) -> TradeSnapshotRecord:
        if not snapshots:
            raise RuntimeError(f"Empty snapshots for trade_id={trade_id}")
        return sorted(snapshots, key=lambda x: (x.ts_ms, x.snapshot_id))[0]

    # ========================================================
    # Summary text / payload
    # ========================================================

    def _build_korean_summary(
        self,
        symbol: str,
        total_trades: int,
        wins: int,
        losses: int,
        breakeven: int,
        win_rate_pct: Decimal,
        total_pnl: Decimal,
        avg_pnl: Decimal,
        avg_win_pnl: Decimal,
        avg_loss_pnl: Decimal,
        long_perf: SidePerformance,
        short_perf: SidePerformance,
        entry_failure_reasons: Sequence[str],
        loss_causes: Sequence[str],
    ) -> str:
        entry_failure_text = ", ".join(entry_failure_reasons)
        loss_cause_text = ", ".join(loss_causes)

        return (
            f"{symbol} 최근 거래 분석 결과 총 {total_trades}건 중 승 {wins}건, 패 {losses}건, "
            f"보합 {breakeven}건이며 승률은 {self._fmt_decimal(win_rate_pct, 2)}% 입니다. "
            f"총 손익은 {self._fmt_decimal(total_pnl, 4)}, 평균 손익은 {self._fmt_decimal(avg_pnl, 4)} 입니다. "
            f"평균 이익은 {self._fmt_decimal(avg_win_pnl, 4)}, 평균 손실은 {self._fmt_decimal(avg_loss_pnl, 4)} 입니다. "
            f"LONG은 {long_perf.total}건 / 승률 {self._fmt_decimal(long_perf.win_rate_pct, 2)}% / 총손익 {self._fmt_decimal(long_perf.total_pnl, 4)}, "
            f"SHORT는 {short_perf.total}건 / 승률 {self._fmt_decimal(short_perf.win_rate_pct, 2)}% / 총손익 {self._fmt_decimal(short_perf.total_pnl, 4)} 입니다. "
            f"최근 진입 실패 주요 사유는 {entry_failure_text} 이고, 주요 손실 원인은 {loss_cause_text} 입니다."
        )

    def _build_dashboard_payload(
        self,
        trades: Sequence[TradeRecord],
        total_trades: int,
        wins: int,
        losses: int,
        breakeven: int,
        win_rate_pct: Decimal,
        total_pnl: Decimal,
        avg_pnl: Decimal,
        avg_win_pnl: Decimal,
        avg_loss_pnl: Decimal,
        long_performance: SidePerformance,
        short_performance: SidePerformance,
        entry_failure_reasons: Sequence[str],
        loss_causes: Sequence[str],
        recent_trade_briefs: Sequence[Dict[str, Any]],
        analyst_summary_ko: str,
    ) -> Dict[str, Any]:
        return {
            "symbol": self._symbol,
            "as_of_ts_ms": trades[-1].closed_ts_ms,
            "performance": {
                "total_trades": total_trades,
                "wins": wins,
                "losses": losses,
                "breakeven": breakeven,
                "win_rate_pct": self._fmt_decimal(win_rate_pct, 4),
                "total_pnl": self._fmt_decimal(total_pnl, 4),
                "avg_pnl": self._fmt_decimal(avg_pnl, 4),
                "avg_win_pnl": self._fmt_decimal(avg_win_pnl, 4),
                "avg_loss_pnl": self._fmt_decimal(avg_loss_pnl, 4),
            },
            "side_performance": {
                "long": {
                    "side": long_performance.side,
                    "total": long_performance.total,
                    "wins": long_performance.wins,
                    "losses": long_performance.losses,
                    "breakeven": long_performance.breakeven,
                    "win_rate_pct": self._fmt_decimal(long_performance.win_rate_pct, 4),
                    "total_pnl": self._fmt_decimal(long_performance.total_pnl, 4),
                    "avg_pnl": self._fmt_decimal(long_performance.avg_pnl, 4),
                },
                "short": {
                    "side": short_performance.side,
                    "total": short_performance.total,
                    "wins": short_performance.wins,
                    "losses": short_performance.losses,
                    "breakeven": short_performance.breakeven,
                    "win_rate_pct": self._fmt_decimal(short_performance.win_rate_pct, 4),
                    "total_pnl": self._fmt_decimal(short_performance.total_pnl, 4),
                    "avg_pnl": self._fmt_decimal(short_performance.avg_pnl, 4),
                },
            },
            "entry_failure_reasons": list(entry_failure_reasons),
            "loss_causes": list(loss_causes),
            "recent_trade_briefs": list(recent_trade_briefs),
            "analyst_summary_ko": analyst_summary_ko,
        }

    # ========================================================
    # Parsing
    # ========================================================

    def _parse_trade_row(self, row: Mapping[str, Any]) -> TradeRecord:
        trade_id = self._pick_int(
            row,
            ["id", "trade_id"],
            field_name="trade_id",
        )
        symbol = self._pick_str(row, ["symbol"], field_name="symbol")
        side = self._normalize_side(
            self._pick_str(row, ["side", "position_side"], field_name="side")
        )
        status = self._normalize_status(
            self._pick_optional_str(row, ["status", "state"])
            or "UNKNOWN"
        )
        entry_price = self._pick_decimal(
            row,
            ["entry_price", "avg_entry_price", "open_price"],
            field_name="entry_price",
        )
        exit_price = self._pick_optional_decimal(
            row,
            ["exit_price", "avg_exit_price", "close_price"],
        )
        realized_pnl = self._pick_decimal(
            row,
            ["realized_pnl", "realized_pnl_usdt", "pnl", "pnl_usdt"],
            field_name="realized_pnl",
        )
        opened_ts_ms = self._pick_int(
            row,
            ["opened_ts_ms", "open_ts_ms", "entry_ts_ms", "created_ts_ms", "created_at_ms"],
            field_name="opened_ts_ms",
        )
        closed_ts_ms = self._pick_int(
            row,
            ["closed_ts_ms", "close_ts_ms", "exit_ts_ms", "updated_ts_ms", "closed_at_ms"],
            field_name="closed_ts_ms",
        )
        exit_reason = self._pick_optional_str(row, ["exit_reason", "close_reason", "reason"])

        if entry_price <= Decimal("0"):
            raise RuntimeError(f"Trade {trade_id}: entry_price must be > 0")
        if exit_price is not None and exit_price <= Decimal("0"):
            raise RuntimeError(f"Trade {trade_id}: exit_price must be > 0")
        if closed_ts_ms < opened_ts_ms:
            raise RuntimeError(f"Trade {trade_id}: closed_ts_ms < opened_ts_ms")

        return TradeRecord(
            trade_id=trade_id,
            symbol=symbol,
            side=side,
            status=status,
            entry_price=entry_price,
            exit_price=exit_price,
            realized_pnl=realized_pnl,
            opened_ts_ms=opened_ts_ms,
            closed_ts_ms=closed_ts_ms,
            exit_reason=exit_reason,
        )

    def _parse_event_row(self, row: Mapping[str, Any]) -> EventRecord:
        extra_json_value = row.get("extra_json")
        extra_json = self._normalize_extra_json(extra_json_value)

        reason = self._pick_optional_str(
            row,
            ["reason", "message", "detail"],
        )
        if reason is None:
            reason = self._extract_reason_from_extra_json(extra_json)

        return EventRecord(
            event_id=self._pick_int(row, ["id", "event_id"], field_name="event_id"),
            ts_ms=self._pick_int(
                row,
                ["event_ts_ms", "ts_ms", "created_ts_ms", "created_at_ms"],
                field_name="event_ts_ms",
            ),
            symbol=self._pick_str(row, ["symbol"], field_name="symbol"),
            event_type=self._pick_str(row, ["event_type", "type"], field_name="event_type"),
            trade_id=self._pick_optional_int(row, ["trade_id"]),
            side=self._normalize_optional_side(self._pick_optional_str(row, ["side", "position_side"])),
            reason=reason,
            regime=self._pick_optional_str(row, ["regime", "market_regime"]),
            extra_json=extra_json,
        )

    def _parse_trade_snapshot_row(self, row: Mapping[str, Any]) -> TradeSnapshotRecord:
        snapshot_id = self._pick_int(
            row,
            ["id", "snapshot_id"],
            field_name="snapshot_id",
        )
        trade_id = self._pick_int(
            row,
            ["trade_id"],
            field_name="trade_id",
        )
        ts_ms = self._pick_int(
            row,
            ["snapshot_ts_ms", "ts_ms", "created_ts_ms", "created_at_ms"],
            field_name="snapshot_ts_ms",
        )
        spread_bps = self._pick_decimal(
            row,
            ["spread_bps"],
            field_name="spread_bps",
        )
        pattern_score = self._pick_decimal(
            row,
            ["pattern_score"],
            field_name="pattern_score",
        )
        market_regime = self._pick_str(
            row,
            ["market_regime", "regime"],
            field_name="market_regime",
        )
        orderbook_imbalance = self._pick_decimal(
            row,
            ["orderbook_imbalance", "depth_imbalance", "ob_imbalance"],
            field_name="orderbook_imbalance",
        )
        volatility_score = self._pick_optional_decimal(
            row,
            ["volatility_score", "vol_score"],
        )

        if spread_bps < Decimal("0"):
            raise RuntimeError(f"Snapshot {snapshot_id}: spread_bps must be >= 0")

        return TradeSnapshotRecord(
            snapshot_id=snapshot_id,
            trade_id=trade_id,
            ts_ms=ts_ms,
            spread_bps=spread_bps,
            pattern_score=pattern_score,
            market_regime=market_regime,
            orderbook_imbalance=orderbook_imbalance,
            volatility_score=volatility_score,
        )

    # ========================================================
    # Validation
    # ========================================================

    def _validate_trade_order(self, trades: Sequence[TradeRecord]) -> None:
        prev_closed_ts: Optional[int] = None
        for trade in trades:
            if prev_closed_ts is not None and trade.closed_ts_ms <= prev_closed_ts:
                raise RuntimeError("Closed trade order must be strictly increasing by closed_ts_ms")
            prev_closed_ts = trade.closed_ts_ms

    def _validate_snapshot_order(
        self,
        trade_id: int,
        snapshots: Sequence[TradeSnapshotRecord],
    ) -> None:
        prev_ts: Optional[int] = None
        for snapshot in snapshots:
            if prev_ts is not None and snapshot.ts_ms < prev_ts:
                raise RuntimeError(f"Snapshot order invalid for trade_id={trade_id}")
            prev_ts = snapshot.ts_ms

    def _is_closed_trade(self, trade: TradeRecord) -> bool:
        return trade.status in {"CLOSED", "CLOSE", "EXITED", "DONE", "FILLED_CLOSE"}

    # ========================================================
    # Helpers
    # ========================================================

    def _avg_pnl_or_zero(self, trades: Sequence[TradeRecord]) -> Decimal:
        if not trades:
            return Decimal("0")
        total = sum((t.realized_pnl for t in trades), Decimal("0"))
        return total / Decimal(len(trades))

    def _normalize_side(self, value: str) -> str:
        normalized = value.upper().strip()
        if normalized in {"LONG", "BUY"}:
            return "LONG"
        if normalized in {"SHORT", "SELL"}:
            return "SHORT"
        raise RuntimeError(f"Unexpected trade side: {value}")

    def _normalize_optional_side(self, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return self._normalize_side(value)

    def _normalize_status(self, value: str) -> str:
        normalized = value.upper().strip()
        if not normalized:
            raise RuntimeError("Trade status must not be empty")
        return normalized

    def _normalize_reason_string(self, value: str) -> str:
        normalized = value.strip().lower().replace(" ", "_")
        if not normalized:
            raise RuntimeError("Normalized reason must not be empty")
        return normalized

    def _normalize_extra_json(self, value: Any) -> Optional[Dict[str, Any]]:
        if value is None:
            return None
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return None
            try:
                decoded = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise RuntimeError("Invalid JSON in extra_json field") from exc
            if not isinstance(decoded, dict):
                raise RuntimeError("extra_json must decode to object")
            return decoded
        raise RuntimeError("extra_json field must be dict, string, or null")

    def _extract_reason_from_extra_json(self, value: Optional[Dict[str, Any]]) -> Optional[str]:
        if value is None:
            return None

        candidate_keys = [
            "reason",
            "skip_reason",
            "cause",
            "reason_code",
            "error",
            "exit_reason",
            "close_reason",
        ]

        for key in candidate_keys:
            if key in value and value[key] is not None:
                raw = value[key]
                if not isinstance(raw, str):
                    raw = str(raw)
                if raw.strip():
                    return raw.strip()

        return None

    # ========================================================
    # Mapping extractors
    # ========================================================

    def _pick_str(
        self,
        row: Mapping[str, Any],
        candidate_keys: Sequence[str],
        field_name: str,
    ) -> str:
        for key in candidate_keys:
            if key in row and row[key] is not None:
                value = row[key]
                if not isinstance(value, str):
                    value = str(value)
                if value.strip():
                    return value.strip()
        raise RuntimeError(f"Missing required string field: {field_name}; candidates={candidate_keys}")

    def _pick_optional_str(
        self,
        row: Mapping[str, Any],
        candidate_keys: Sequence[str],
    ) -> Optional[str]:
        for key in candidate_keys:
            if key in row and row[key] is not None:
                value = row[key]
                if not isinstance(value, str):
                    value = str(value)
                if value.strip():
                    return value.strip()
        return None

    def _pick_int(
        self,
        row: Mapping[str, Any],
        candidate_keys: Sequence[str],
        field_name: str,
    ) -> int:
        for key in candidate_keys:
            if key in row and row[key] is not None:
                return self._to_int(row[key], field_name)
        raise RuntimeError(f"Missing required int field: {field_name}; candidates={candidate_keys}")

    def _pick_optional_int(
        self,
        row: Mapping[str, Any],
        candidate_keys: Sequence[str],
    ) -> Optional[int]:
        for key in candidate_keys:
            if key in row and row[key] is not None:
                return self._to_int(row[key], key)
        return None

    def _pick_decimal(
        self,
        row: Mapping[str, Any],
        candidate_keys: Sequence[str],
        field_name: str,
    ) -> Decimal:
        for key in candidate_keys:
            if key in row and row[key] is not None:
                return self._to_decimal(row[key], field_name)
        raise RuntimeError(f"Missing required decimal field: {field_name}; candidates={candidate_keys}")

    def _pick_optional_decimal(
        self,
        row: Mapping[str, Any],
        candidate_keys: Sequence[str],
    ) -> Optional[Decimal]:
        for key in candidate_keys:
            if key in row and row[key] is not None:
                return self._to_decimal(row[key], key)
        return None

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

    # ========================================================
    # Settings helpers
    # ========================================================

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

    def _require_decimal_setting(self, name: str) -> Decimal:
        value = getattr(SETTINGS, name, None)
        if value is None:
            raise RuntimeError(f"Missing required decimal setting: {name}")
        return self._to_decimal(value, name)

    # ========================================================
    # Formatting helpers
    # ========================================================

    def _fmt_decimal(self, value: Decimal, scale: int) -> str:
        if scale < 0:
            raise RuntimeError("scale must be >= 0")
        return f"{value:.{scale}f}"

    def _fmt_decimal_optional(self, value: Optional[Decimal], scale: int) -> Optional[str]:
        if value is None:
            return None
        return self._fmt_decimal(value, scale)


__all__ = [
    "TradeAnalyzer",
    "TradeAnalyzerSummary",
    "TradeRecord",
    "EventRecord",
    "TradeSnapshotRecord",
    "SidePerformance",
]