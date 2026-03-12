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
- 2026-03-13:
  1) FIX(EXCEPTION): common.exceptions_strict 공통 예외 계층 적용
  2) FIX(CONTRACT): QuantAnalyst payload(gpt_result.scope) strict 검증 추가
  3) FIX(ARCH): settings 주입 가능하도록 수정(load_settings 기본 사용)
  4) FIX(CONTRACT): AutoReportItem / AutoReporterResult dataclass 자체 검증 추가
  5) FIX(STRICT): now_ms / last_run_ts_ms monotonic 계약 검증 강화
- 2026-03-07:
  1) 신규 생성
  2) 자동 시장/시스템 리포트 오케스트레이터 추가
========================================================
"""

from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass
from typing import Any, Callable, Dict, List, Optional

from analysis.quant_analyst import QuantAnalyst
from common.exceptions_strict import (
    StrictConfigError,
    StrictDataError,
    StrictExternalError,
    StrictStateError,
)
from settings import load_settings

logger = logging.getLogger(__name__)

EventWriter = Callable[[str, str, Dict[str, Any]], None]
Notifier = Callable[[str, Dict[str, Any]], None]


class AutoReporterConfigError(StrictConfigError):
    """AutoReporter settings / callback 계약 위반."""


class AutoReporterContractError(StrictDataError):
    """AutoReporter payload / item / result 계약 위반."""


class AutoReporterExternalError(StrictExternalError):
    """QuantAnalyst 외부 분석 실패."""


class AutoReporterStateError(StrictStateError):
    """시간/스케줄 상태 불일치."""


def _require_nonempty_str(value: Any, name: str) -> str:
    if not isinstance(value, str):
        raise AutoReporterContractError(f"{name} must be str (STRICT), got={type(value).__name__}")
    s = value.strip()
    if not s:
        raise AutoReporterContractError(f"{name} must not be empty (STRICT)")
    return s


def _require_positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise AutoReporterContractError(f"{name} must be int (STRICT), bool not allowed")
    try:
        iv = int(value)
    except Exception as exc:
        raise AutoReporterContractError(f"{name} must be int (STRICT): {exc}") from exc
    if iv <= 0:
        raise AutoReporterContractError(f"{name} must be > 0 (STRICT)")
    return iv


def _require_dict(value: Any, name: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise AutoReporterContractError(f"{name} must be dict (STRICT), got={type(value).__name__}")
    if not value:
        raise AutoReporterContractError(f"{name} must not be empty (STRICT)")
    return dict(value)


def _require_bool_setting_value(value: Any, name: str) -> bool:
    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False

    raise AutoReporterConfigError(f"{name} must be bool or bool-like string (STRICT)")


@dataclass(frozen=True)
class AutoReportItem:
    report_type: str
    question: str
    generated_ts_ms: int
    payload: Dict[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "report_type", _require_nonempty_str(self.report_type, "AutoReportItem.report_type"))
        object.__setattr__(self, "question", _require_nonempty_str(self.question, "AutoReportItem.question"))
        object.__setattr__(
            self,
            "generated_ts_ms",
            _require_positive_int(self.generated_ts_ms, "AutoReportItem.generated_ts_ms"),
        )
        object.__setattr__(self, "payload", _require_dict(self.payload, "AutoReportItem.payload"))

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AutoReporterResult:
    generated_ts_ms: int
    reports: List[Dict[str, Any]]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "generated_ts_ms",
            _require_positive_int(self.generated_ts_ms, "AutoReporterResult.generated_ts_ms"),
        )
        if not isinstance(self.reports, list):
            raise AutoReporterContractError(
                f"AutoReporterResult.reports must be list (STRICT), got={type(self.reports).__name__}"
            )
        for idx, item in enumerate(self.reports):
            _require_dict(item, f"AutoReporterResult.reports[{idx}]")

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
        settings: Optional[Any] = None,
        event_writer: Optional[EventWriter] = None,
        notifier: Optional[Notifier] = None,
    ) -> None:
        self._settings = settings if settings is not None else load_settings()
        if self._settings is None:
            raise AutoReporterConfigError("settings resolution failed (STRICT)")

        self._symbol = self._require_str_setting("ANALYST_MARKET_SYMBOL")
        self._market_interval_sec = self._require_int_setting("ANALYST_AUTO_REPORT_MARKET_INTERVAL_SEC")
        self._system_interval_sec = self._require_int_setting("ANALYST_AUTO_REPORT_SYSTEM_INTERVAL_SEC")
        self._include_external_market = self._require_bool_setting("ANALYST_INCLUDE_EXTERNAL_MARKET")
        self._persist_enabled = self._require_bool_setting("ANALYST_AUTO_REPORT_PERSIST")
        self._notify_enabled = self._require_bool_setting("ANALYST_AUTO_REPORT_NOTIFY")

        if self._market_interval_sec <= 0:
            raise AutoReporterConfigError("ANALYST_AUTO_REPORT_MARKET_INTERVAL_SEC must be > 0 (STRICT)")
        if self._system_interval_sec <= 0:
            raise AutoReporterConfigError("ANALYST_AUTO_REPORT_SYSTEM_INTERVAL_SEC must be > 0 (STRICT)")

        if self._persist_enabled and event_writer is None:
            raise AutoReporterConfigError(
                "event_writer callback is required when ANALYST_AUTO_REPORT_PERSIST is enabled (STRICT)"
            )
        if self._notify_enabled and notifier is None:
            raise AutoReporterConfigError(
                "notifier callback is required when ANALYST_AUTO_REPORT_NOTIFY is enabled (STRICT)"
            )

        if event_writer is not None and not callable(event_writer):
            raise AutoReporterConfigError("event_writer must be callable (STRICT)")
        if notifier is not None and not callable(notifier):
            raise AutoReporterConfigError("notifier must be callable (STRICT)")

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
        payload = self._run_quant_analysis_strict(
            question=self._MARKET_REPORT_QUESTION,
            include_external_market=self._include_external_market,
        )

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
        payload = self._run_quant_analysis_strict(
            question=self._SYSTEM_REPORT_QUESTION,
            include_external_market=self._include_external_market,
        )

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
        current_ms = self._now_ms() if now_ms is None else _require_positive_int(now_ms, "now_ms")

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
                raise AutoReporterConfigError("event_writer callback is missing (STRICT)")
            self._event_writer(
                "QUANT_ANALYSIS",
                item.report_type,
                item.to_dict(),
            )

        if self._notify_enabled:
            if self._notifier is None:
                raise AutoReporterConfigError("notifier callback is missing (STRICT)")
            self._notifier(
                item.report_type,
                item.to_dict(),
            )

    # ========================================================
    # Quant analyst runner
    # ========================================================

    def _run_quant_analysis_strict(
        self,
        *,
        question: str,
        include_external_market: bool,
    ) -> Dict[str, Any]:
        q = _require_nonempty_str(question, "question")
        if not isinstance(include_external_market, bool):
            raise AutoReporterContractError("include_external_market must be bool (STRICT)")

        try:
            response = self._quant_analyst.analyze(
                question=q,
                include_external_market=include_external_market,
            )
        except Exception as exc:
            raise AutoReporterExternalError(
                f"QuantAnalyst.analyze failed (STRICT): {type(exc).__name__}: {exc}"
            ) from exc

        to_dict = getattr(response, "to_dict", None)
        if not callable(to_dict):
            raise AutoReporterContractError("QuantAnalyst response.to_dict is required (STRICT)")

        payload = to_dict()
        payload_dict = _require_dict(payload, "quant_analyst.payload")
        gpt_result = _require_dict(payload_dict.get("gpt_result"), "quant_analyst.payload.gpt_result")
        _require_nonempty_str(gpt_result.get("scope"), "quant_analyst.payload.gpt_result.scope")

        return payload_dict

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
        current_ms = _require_positive_int(now_ms, "now_ms")
        interval = _require_positive_int(interval_sec, "interval_sec")

        if last_run_ts_ms is None:
            return True

        last_run = _require_positive_int(last_run_ts_ms, "last_run_ts_ms")
        if last_run > current_ms:
            raise AutoReporterStateError(
                f"last_run_ts_ms must be <= now_ms (STRICT): last_run_ts_ms={last_run} now_ms={current_ms}"
            )

        interval_ms = interval * 1000
        return (current_ms - last_run) >= interval_ms

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    # ========================================================
    # Settings helpers
    # ========================================================

    def _require_setting_value(self, name: str) -> Any:
        if not hasattr(self._settings, name):
            raise AutoReporterConfigError(f"Missing required setting: {name} (STRICT)")
        return getattr(self._settings, name)

    def _require_str_setting(self, name: str) -> str:
        value = self._require_setting_value(name)
        if not isinstance(value, str) or not value.strip():
            raise AutoReporterConfigError(f"Missing or invalid required setting: {name} (STRICT)")
        return value.strip()

    def _require_int_setting(self, name: str) -> int:
        value = self._require_setting_value(name)
        if isinstance(value, bool):
            raise AutoReporterConfigError(f"Invalid bool value for integer setting: {name} (STRICT)")
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise AutoReporterConfigError(f"Missing or invalid required int setting: {name} (STRICT)") from exc
        if parsed <= 0:
            raise AutoReporterConfigError(f"{name} must be > 0 (STRICT)")
        return parsed

    def _require_bool_setting(self, name: str) -> bool:
        value = self._require_setting_value(name)
        return _require_bool_setting_value(value, f"settings.{name}")


__all__ = [
    "AutoReporterConfigError",
    "AutoReporterContractError",
    "AutoReporterExternalError",
    "AutoReporterStateError",
    "AutoReporter",
    "AutoReportItem",
    "AutoReporterResult",
]