"""
========================================================
FILE: analysis/auto_reporter.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할:
- AI Quant Analyst의 자동 리포트 생성 오케스트레이션을 담당한다.
- 주기적으로 시장 분석 리포트와 시스템 분석 리포트를 생성한다.
- 선택적으로 외부 주입 callback을 통해 DB 이벤트 기록 / 알림 전송을 수행한다.
- 주문 실행 / 포지션 변경 / TP·SL 수정은 절대 수행하지 않는다.

절대 원칙:
- STRICT · NO-FALLBACK
- settings.py(SSOT) 외 환경변수 직접 접근 금지
- print() 금지 / logging 사용
- 데이터 누락/손상/모호성 발생 시 즉시 예외
- 민감정보 로그 금지
- 예외 삼키기 금지
- 분석 실패 시 즉시 예외 전파

변경 이력:
2026-03-07
- 신규 생성
- 자동 시장/시스템 리포트 오케스트레이터 추가
========================================================
"""

from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass
from typing import Any, Callable, Dict, List, Optional

from analysis.quant_analyst import QuantAnalyst
from settings import SETTINGS

logger = logging.getLogger(__name__)

EventWriter = Callable[[str, str, Dict[str, Any]], None]
Notifier = Callable[[str, Dict[str, Any]], None]


@dataclass(frozen=True)
class AutoReportItem:
    report_type: str
    question: str
    generated_ts_ms: int
    payload: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AutoReporterResult:
    generated_ts_ms: int
    reports: List[Dict[str, Any]]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class AutoReporter:
    """
    자동 분석 리포트 생성기.

    리포트 종류:
    - market_report : 외부 시장 포함 시장 중심 리포트
    - system_report : 내부 거래/시장/외부 시장 포함 시스템 중심 리포트
    """

    _MARKET_REPORT_TYPE = "market_report"
    _SYSTEM_REPORT_TYPE = "system_report"

    _MARKET_REPORT_QUESTION = (
        "현재 BTC 선물 시장 상태를 요약하고 추세, 변동성, 유동성, 파생시장 구조, "
        "핵심 리스크와 시사점을 설명하라."
    )
    _SYSTEM_REPORT_QUESTION = (
        "최근 거래 성능, 진입 실패 이유, 손실 원인, 현재 시장 구조를 통합 분석하고 "
        "핵심 문제와 개선 방향을 설명하라."
    )

    def __init__(
        self,
        *,
        event_writer: Optional[EventWriter] = None,
        notifier: Optional[Notifier] = None,
    ) -> None:
        self._symbol = self._require_str_setting("ANALYST_MARKET_SYMBOL")
        self._market_interval_sec = self._require_int_setting("ANALYST_AUTO_REPORT_MARKET_INTERVAL_SEC")
        self._system_interval_sec = self._require_int_setting("ANALYST_AUTO_REPORT_SYSTEM_INTERVAL_SEC")
        self._include_external_market = self._require_bool_setting("ANALYST_INCLUDE_EXTERNAL_MARKET")
        self._persist_enabled = self._require_bool_setting("ANALYST_AUTO_REPORT_PERSIST")
        self._notify_enabled = self._require_bool_setting("ANALYST_AUTO_REPORT_NOTIFY")

        if self._market_interval_sec <= 0:
            raise RuntimeError("ANALYST_AUTO_REPORT_MARKET_INTERVAL_SEC must be > 0")
        if self._system_interval_sec <= 0:
            raise RuntimeError("ANALYST_AUTO_REPORT_SYSTEM_INTERVAL_SEC must be > 0")

        if self._persist_enabled and event_writer is None:
            raise RuntimeError("event_writer callback is required when ANALYST_AUTO_REPORT_PERSIST is enabled")
        if self._notify_enabled and notifier is None:
            raise RuntimeError("notifier callback is required when ANALYST_AUTO_REPORT_NOTIFY is enabled")

        self._event_writer = event_writer
        self._notifier = notifier
        self._quant_analyst = QuantAnalyst()

        self._last_market_report_ts_ms: Optional[int] = None
        self._last_system_report_ts_ms: Optional[int] = None

    # ========================================================
    # Public API
    # ========================================================

    def run_market_report_once(self) -> AutoReportItem:
        generated_ts_ms = self._now_ms()
        response = self._quant_analyst.analyze(
            question=self._MARKET_REPORT_QUESTION,
            include_external_market=self._include_external_market,
        )
        payload = response.to_dict()

        item = AutoReportItem(
            report_type=self._MARKET_REPORT_TYPE,
            question=self._MARKET_REPORT_QUESTION,
            generated_ts_ms=generated_ts_ms,
            payload=payload,
        )

        self._emit_report(item)
        self._last_market_report_ts_ms = generated_ts_ms

        logger.info(
            "Auto market report generated: symbol=%s ts_ms=%s scope=%s",
            self._symbol,
            generated_ts_ms,
            payload["gpt_result"]["scope"],
        )
        return item

    def run_system_report_once(self) -> AutoReportItem:
        generated_ts_ms = self._now_ms()
        response = self._quant_analyst.analyze(
            question=self._SYSTEM_REPORT_QUESTION,
            include_external_market=self._include_external_market,
        )
        payload = response.to_dict()

        item = AutoReportItem(
            report_type=self._SYSTEM_REPORT_TYPE,
            question=self._SYSTEM_REPORT_QUESTION,
            generated_ts_ms=generated_ts_ms,
            payload=payload,
        )

        self._emit_report(item)
        self._last_system_report_ts_ms = generated_ts_ms

        logger.info(
            "Auto system report generated: symbol=%s ts_ms=%s scope=%s",
            self._symbol,
            generated_ts_ms,
            payload["gpt_result"]["scope"],
        )
        return item

    def run_due_reports(self, *, now_ms: Optional[int] = None) -> AutoReporterResult:
        current_ms = self._now_ms() if now_ms is None else now_ms
        if not isinstance(current_ms, int):
            raise RuntimeError("now_ms must be int when provided")
        if current_ms <= 0:
            raise RuntimeError("now_ms must be > 0")

        reports: List[Dict[str, Any]] = []

        if self._is_due(
            now_ms=current_ms,
            last_run_ts_ms=self._last_market_report_ts_ms,
            interval_sec=self._market_interval_sec,
        ):
            reports.append(self.run_market_report_once().to_dict())

        if self._is_due(
            now_ms=current_ms,
            last_run_ts_ms=self._last_system_report_ts_ms,
            interval_sec=self._system_interval_sec,
        ):
            reports.append(self.run_system_report_once().to_dict())

        return AutoReporterResult(
            generated_ts_ms=current_ms,
            reports=reports,
        )

    def get_status(self) -> Dict[str, Any]:
        return {
            "symbol": self._symbol,
            "market_interval_sec": self._market_interval_sec,
            "system_interval_sec": self._system_interval_sec,
            "include_external_market": self._include_external_market,
            "persist_enabled": self._persist_enabled,
            "notify_enabled": self._notify_enabled,
            "last_market_report_ts_ms": self._last_market_report_ts_ms,
            "last_system_report_ts_ms": self._last_system_report_ts_ms,
        }

    # ========================================================
    # Internal emitters
    # ========================================================

    def _emit_report(self, item: AutoReportItem) -> None:
        if self._persist_enabled:
            if self._event_writer is None:
                raise RuntimeError("event_writer callback is missing")
            self._event_writer(
                "QUANT_ANALYSIS",
                item.report_type,
                item.to_dict(),
            )

        if self._notify_enabled:
            if self._notifier is None:
                raise RuntimeError("notifier callback is missing")
            self._notifier(
                item.report_type,
                item.to_dict(),
            )

    # ========================================================
    # Time helpers
    # ========================================================

    def _is_due(
        self,
        *,
        now_ms: int,
        last_run_ts_ms: Optional[int],
        interval_sec: int,
    ) -> bool:
        if now_ms <= 0:
            raise RuntimeError("now_ms must be > 0")
        if interval_sec <= 0:
            raise RuntimeError("interval_sec must be > 0")

        if last_run_ts_ms is None:
            return True
        if last_run_ts_ms <= 0:
            raise RuntimeError("last_run_ts_ms must be > 0 when provided")

        interval_ms = interval_sec * 1000
        return (now_ms - last_run_ts_ms) >= interval_ms

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    # ========================================================
    # Settings helpers
    # ========================================================

    def _require_str_setting(self, name: str) -> str:
        value = getattr(SETTINGS, name, None)
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError(f"Missing or invalid required setting: {name}")
        return value.strip()

    def _require_int_setting(self, name: str) -> int:
        value = getattr(SETTINGS, name, None)
        if isinstance(value, bool):
            raise RuntimeError(f"Invalid bool value for integer setting: {name}")
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Missing or invalid required int setting: {name}") from exc
        return parsed

    def _require_bool_setting(self, name: str) -> bool:
        value = getattr(SETTINGS, name, None)

        if isinstance(value, bool):
            return value

        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "y", "on"}:
                return True
            if normalized in {"0", "false", "no", "n", "off"}:
                return False

        raise RuntimeError(f"Missing or invalid required bool setting: {name}")


__all__ = [
    "AutoReporter",
    "AutoReportItem",
    "AutoReporterResult",
]