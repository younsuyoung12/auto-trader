"""
========================================================
FILE: tests_tradegrade/test_ai_analyst_debug_runner.py
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================
역할:
- analysis 계열 파일(settings / fetchers / analyzers / orchestrators)의
  정적 점검 + 단계별 디버그 테스트를 수행한다.
- 오류 발생 시 어느 stage / 어느 모듈 / 어느 함수에서 깨졌는지
  정확한 traceback 과 함께 JSON 리포트로 남긴다.
- 네트워크 / DB / OpenAI 외부 의존성은 모두 테스트 더블로 대체한다.
- production 코드를 수정하지 않고도 원인 추적이 가능하도록 설계한다.

절대 원칙:
- STRICT · NO-FALLBACK
- 테스트 코드 내부에서도 예외를 삼키지 않는다.
- 각 stage 실패는 요약 리포트에 반드시 남긴다.
- 실패 원인, 예외 타입, traceback, 추정 위치를 구조화해서 출력한다.
- 외부 실서버 호출 금지. DB/OpenAI/HTTP는 mock/fake 로 대체한다.

변경 이력:
- 2026-03-08:
  1) 신규 생성
  2) settings / fetchers / analyzers / GPT engine / orchestrator / auto reporter 통합 디버그 러너 추가
  3) stage별 성공/실패 JSON 요약 리포트 출력 추가
  4) static policy scan(top-level print / swallow fallback) 추가
========================================================
"""

from __future__ import annotations

import argparse
import ast
import importlib
import json
import os
import sys
import traceback
from contextlib import contextmanager
import types
import time
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence
from unittest import mock


# ---------------------------------------------------------------------
# Path bootstrap
# ---------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


# ---------------------------------------------------------------------
# Environment bootstrap
# ---------------------------------------------------------------------
def _install_test_env() -> None:
    defaults = {
        "BINANCE_API_KEY": "test_binance_key",
        "BINANCE_API_SECRET": "test_binance_secret",
        "OPENAI_API_KEY": "test_openai_key",
        "OPENAI_MODEL": "gpt-5-mini",
        "ALPHAVANTAGE_API_KEY": "test_alpha_key",
        "SYMBOL": "BTCUSDT",
        "INTERVAL": "5m",
        "ANALYST_MARKET_SYMBOL": "BTCUSDT",
        "ANALYST_DB_MARKET_TIMEFRAME": "5m",
        "ANALYST_DB_MARKET_LOOKBACK_LIMIT": "10",
        "ANALYST_PATTERN_ENTRY_THRESHOLD": "0.55",
        "ANALYST_SPREAD_GUARD_BPS": "15",
        "ANALYST_IMBALANCE_GUARD_ABS": "0.60",
        "ANALYST_TRADE_LOOKBACK_LIMIT": "10",
        "ANALYST_EVENT_LOOKBACK_LIMIT": "50",
        "ANALYST_INCLUDE_EXTERNAL_MARKET": "true",
        "ANALYST_AUTO_REPORT_MARKET_INTERVAL_SEC": "30",
        "ANALYST_AUTO_REPORT_SYSTEM_INTERVAL_SEC": "1800",
        "ANALYST_AUTO_REPORT_PERSIST": "true",
        "ANALYST_AUTO_REPORT_NOTIFY": "false",
        "ANALYST_OPENAI_MODEL": "gpt-5-mini",
        "ANALYST_OPENAI_TIMEOUT_SEC": "30",
        "ANALYST_OPENAI_MAX_OUTPUT_TOKENS": "800",
        "ANALYST_OPENAI_TEMPERATURE": "0.1",
        "ANALYST_KLINE_INTERVAL": "5m",
        "ANALYST_KLINE_LIMIT": "6",
        "ANALYST_HTTP_TIMEOUT_SEC": "5",
        "ANALYST_RATIO_PERIOD": "5m",
        "ANALYST_RATIO_LIMIT": "6",
        "ANALYST_DEPTH_LIMIT": "5",
        "ANALYST_FUNDING_LIMIT": "5",
        "ANALYST_FORCE_ORDER_LIMIT": "5",
        "ANALYST_MAX_SERVER_DRIFT_MS": "5000",
        "REDIS_URL": "redis://localhost:6379/0",
        "DASHBOARD_CACHE_PREFIX": "auto_trader_dashboard",
        "DASHBOARD_CACHE_TTL_SEC": "5",
        "REDIS_SOCKET_TIMEOUT_SEC": "1",
        "REDIS_SOCKET_CONNECT_TIMEOUT_SEC": "1",
        "REDIS_HEALTH_CHECK_INTERVAL_SEC": "30",
    }
    for key, value in defaults.items():
        os.environ.setdefault(key, value)


_install_test_env()


# ---------------------------------------------------------------------
# Optional dependency shim (for test portability only)
# ---------------------------------------------------------------------
def _install_sqlalchemy_shim_if_missing() -> None:
    try:
        import sqlalchemy  # type: ignore # noqa: F401
        return
    except Exception:
        pass

    fake_sqlalchemy = types.ModuleType("sqlalchemy")

    class FakeTextClause:
        def __init__(self, sql: str) -> None:
            self.sql = sql

        def bindparams(self, *args: Any, **kwargs: Any) -> "FakeTextClause":
            _ = args, kwargs
            return self

        def __str__(self) -> str:
            return self.sql

    def text(sql: str) -> FakeTextClause:
        return FakeTextClause(sql)

    def bindparam(name: str, expanding: bool = False) -> Dict[str, Any]:
        return {"name": name, "expanding": expanding}

    fake_sqlalchemy.text = text
    fake_sqlalchemy.bindparam = bindparam
    sys.modules["sqlalchemy"] = fake_sqlalchemy


_install_sqlalchemy_shim_if_missing()

def _install_openai_shim_if_missing() -> None:
    try:
        import openai  # type: ignore # noqa: F401
        return
    except Exception:
        pass

    fake_openai = types.ModuleType("openai")

    class OpenAI:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            _ = args, kwargs
            self.responses = AttrObject(create=lambda **kw: {"output_parsed": {"scope": "out_of_scope", "answer_ko": "이 질문은 트레이딩 시스템 범위를 벗어납니다.", "root_causes": [], "recommendations": [], "confidence": 0.0, "used_inputs": []}})

    fake_openai.OpenAI = OpenAI
    sys.modules["openai"] = fake_openai


_install_openai_shim_if_missing()


# ---------------------------------------------------------------------
# Data classes for structured debug output
# ---------------------------------------------------------------------
@dataclass(frozen=True)
class StageSuccess:
    stage: str
    details: Dict[str, Any]


@dataclass(frozen=True)
class StageFailure:
    stage: str
    module: str
    function: str
    error_type: str
    error_message: str
    traceback_text: str


@dataclass(frozen=True)
class DebugReport:
    ok: bool
    successes: List[Dict[str, Any]]
    failures: List[Dict[str, Any]]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------
# Fake infra
# ---------------------------------------------------------------------
class FakeHttpResponse:
    def __init__(self, *, status_code: int, payload: Any, text: Optional[str] = None) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = text if text is not None else json.dumps(payload, ensure_ascii=False)

    def json(self) -> Any:
        return self._payload


class FakeHttpSession:
    def __init__(self, router: Callable[[str, str, Optional[Mapping[str, Any]]], FakeHttpResponse]) -> None:
        self._router = router
        self.headers: Dict[str, str] = {}

    def request(
        self,
        method: str,
        url: str,
        params: Optional[Mapping[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> FakeHttpResponse:
        _ = timeout
        return self._router(method.upper(), url, params)

    def get(
        self,
        url: str,
        params: Optional[Mapping[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> FakeHttpResponse:
        _ = timeout
        return self._router("GET", url, params)


class FakeResult:
    def __init__(self, rows: Optional[Sequence[Mapping[str, Any]]] = None, scalar: Any = None) -> None:
        self._rows = list(rows or [])
        self._scalar = scalar

    def mappings(self) -> "FakeResult":
        return self

    def all(self) -> List[Mapping[str, Any]]:
        return list(self._rows)

    def first(self) -> Optional[Mapping[str, Any]]:
        return self._rows[0] if self._rows else None

    def one_or_none(self) -> Optional[Mapping[str, Any]]:
        if len(self._rows) > 1:
            raise RuntimeError("FakeResult.one_or_none expected <= 1 row")
        return self._rows[0] if self._rows else None

    def scalar_one_or_none(self) -> Any:
        return self._scalar


class FakeSession:
    def __init__(self, executor: Callable[[str, Mapping[str, Any]], FakeResult]) -> None:
        self._executor = executor
        self.committed = False

    def execute(self, statement: Any, params: Optional[Mapping[str, Any]] = None) -> FakeResult:
        sql = str(statement)
        return self._executor(sql, dict(params or {}))

    def commit(self) -> None:
        self.committed = True


@contextmanager
def fake_session_cm(executor: Callable[[str, Mapping[str, Any]], FakeResult]):
    yield FakeSession(executor)


class FakeOpenAIResponses:
    def __init__(self, response_factory: Callable[[Dict[str, Any]], Any]) -> None:
        self._response_factory = response_factory

    def create(self, **kwargs: Any) -> Any:
        return self._response_factory(kwargs)


class FakeOpenAIClient:
    def __init__(self, response_factory: Callable[[Dict[str, Any]], Any]) -> None:
        self.responses = FakeOpenAIResponses(response_factory)


class AttrObject:
    def __init__(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)


# ---------------------------------------------------------------------
# Sample payload builders
# ---------------------------------------------------------------------
def _sample_binance_router() -> Callable[[str, str, Optional[Mapping[str, Any]]], FakeHttpResponse]:
    now_ms = int(time.time() * 1000)
    symbol = "BTCUSDT"
    kline_rows = [
        [1772813200000, "70000", "70100", "69950", "70050", "100", 1772813499999, "7000000", 1000, "55", "3850000", "0"],
        [1772813500000, "70050", "70200", "70000", "70150", "110", 1772813799999, "7716500", 1100, "60", "4209000", "0"],
        [1772813800000, "70150", "70300", "70100", "70250", "120", 1772814099999, "8430000", 1200, "65", "4566250", "0"],
        [1772814100000, "70250", "70400", "70200", "70350", "130", 1772814399999, "9145500", 1300, "70", "4924500", "0"],
        [1772814400000, "70350", "70500", "70300", "70450", "140", 1772814699999, "9863000", 1400, "75", "5283750", "0"],
        [1772814700000, "70450", "70600", "70400", "70550", "150", 1772814999999, "10582500", 1500, "80", "5644000", "0"],
    ]
    ratio_rows = [
        {"timestamp": 1772813200000, "longShortRatio": "1.10", "longAccount": "0.52", "shortAccount": "0.48"},
        {"timestamp": 1772813500000, "longShortRatio": "1.12", "longAccount": "0.53", "shortAccount": "0.47"},
        {"timestamp": 1772813800000, "longShortRatio": "1.15", "longAccount": "0.54", "shortAccount": "0.46"},
        {"timestamp": 1772814100000, "longShortRatio": "1.18", "longAccount": "0.55", "shortAccount": "0.45"},
        {"timestamp": 1772814400000, "longShortRatio": "1.21", "longAccount": "0.56", "shortAccount": "0.44"},
        {"timestamp": 1772814700000, "longShortRatio": "1.24", "longAccount": "0.57", "shortAccount": "0.43"},
    ]
    taker_rows = [
        {"timestamp": 1772813200000, "buySellRatio": "1.05", "buyVol": "100", "sellVol": "95"},
        {"timestamp": 1772813500000, "buySellRatio": "1.07", "buyVol": "107", "sellVol": "100"},
        {"timestamp": 1772813800000, "buySellRatio": "1.09", "buyVol": "109", "sellVol": "100"},
        {"timestamp": 1772814100000, "buySellRatio": "1.11", "buyVol": "111", "sellVol": "100"},
        {"timestamp": 1772814400000, "buySellRatio": "1.13", "buyVol": "113", "sellVol": "100"},
        {"timestamp": 1772814700000, "buySellRatio": "1.15", "buyVol": "115", "sellVol": "100"},
    ]
    oi_hist_rows = [
        {"sumOpenInterest": "10000", "sumOpenInterestValue": "705000000", "timestamp": 1772813200000},
        {"sumOpenInterest": "10100", "sumOpenInterestValue": "712000000", "timestamp": 1772813500000},
        {"sumOpenInterest": "10200", "sumOpenInterestValue": "719000000", "timestamp": 1772813800000},
        {"sumOpenInterest": "10350", "sumOpenInterestValue": "728000000", "timestamp": 1772814100000},
        {"sumOpenInterest": "10500", "sumOpenInterestValue": "740000000", "timestamp": 1772814400000},
        {"sumOpenInterest": "10700", "sumOpenInterestValue": "755000000", "timestamp": 1772814700000},
    ]

    def router(method: str, url: str, params: Optional[Mapping[str, Any]]) -> FakeHttpResponse:
        _ = method, params
        if url.endswith("/fapi/v1/time"):
            return FakeHttpResponse(status_code=200, payload={"serverTime": now_ms})
        if url.endswith("/fapi/v1/premiumIndex"):
            return FakeHttpResponse(
                status_code=200,
                payload={
                    "symbol": symbol,
                    "markPrice": "70550",
                    "indexPrice": "70520",
                    "estimatedSettlePrice": "70530",
                    "lastFundingRate": "0.0003",
                    "interestRate": "0.0001",
                    "nextFundingTime": now_ms + 3600000,
                    "time": now_ms,
                },
            )
        if url.endswith("/fapi/v1/ticker/bookTicker"):
            return FakeHttpResponse(
                status_code=200,
                payload={
                    "symbol": symbol,
                    "bidPrice": "70540",
                    "bidQty": "12",
                    "askPrice": "70560",
                    "askQty": "10",
                },
            )
        if url.endswith("/fapi/v1/depth"):
            return FakeHttpResponse(
                status_code=200,
                payload={
                    "lastUpdateId": 100,
                    "bids": [["70540", "5"], ["70530", "4"], ["70520", "3"]],
                    "asks": [["70560", "4"], ["70570", "3"], ["70580", "2"]],
                },
            )
        if url.endswith("/fapi/v1/klines"):
            return FakeHttpResponse(status_code=200, payload=kline_rows)
        if url.endswith("/fapi/v1/openInterest"):
            return FakeHttpResponse(
                status_code=200,
                payload={"symbol": symbol, "openInterest": "10700", "time": now_ms},
            )
        if url.endswith("/fapi/v1/fundingRate"):
            return FakeHttpResponse(
                status_code=200,
                payload=[
                    {"fundingRate": "0.0001", "fundingTime": 1772800000000, "markPrice": "70200"},
                    {"fundingRate": "0.0002", "fundingTime": 1772805000000, "markPrice": "70400"},
                    {"fundingRate": "0.0003", "fundingTime": 1772810000000, "markPrice": "70550"},
                ],
            )
        if url.endswith("/futures/data/openInterestHist"):
            return FakeHttpResponse(status_code=200, payload=oi_hist_rows)
        if url.endswith("/futures/data/globalLongShortAccountRatio"):
            return FakeHttpResponse(status_code=200, payload=ratio_rows)
        if url.endswith("/futures/data/topLongShortPositionRatio"):
            return FakeHttpResponse(status_code=200, payload=ratio_rows)
        if url.endswith("/futures/data/takerlongshortRatio"):
            return FakeHttpResponse(status_code=200, payload=taker_rows)
        raise RuntimeError(f"Unexpected Binance URL in fake router: {url}")

    return router


def _sample_external_router() -> Callable[[str, str, Optional[Mapping[str, Any]]], FakeHttpResponse]:
    def router(method: str, url: str, params: Optional[Mapping[str, Any]]) -> FakeHttpResponse:
        _ = method
        params = dict(params or {})

        if url == "https://api.bybit.com/v5/market/open-interest":
            return FakeHttpResponse(
                status_code=200,
                payload={
                    "retCode": 0,
                    "result": {
                        "list": [
                            {"timestamp": "1772813200000", "openInterest": "10000"},
                            {"timestamp": "1772813500000", "openInterest": "10200"},
                            {"timestamp": "1772813800000", "openInterest": "10450"},
                        ]
                    },
                },
            )

        if url == "https://api.bybit.com/v5/market/funding/history":
            return FakeHttpResponse(
                status_code=200,
                payload={
                    "retCode": 0,
                    "result": {
                        "list": [
                            {"fundingRateTimestamp": "1772800000000", "fundingRate": "0.00010"},
                            {"fundingRateTimestamp": "1772808000000", "fundingRate": "0.00015"},
                            {"fundingRateTimestamp": "1772816000000", "fundingRate": "0.00020"},
                        ]
                    },
                },
            )

        if url == "https://www.okx.com/api/v5/public/open-interest":
            return FakeHttpResponse(
                status_code=200,
                payload={
                    "code": "0",
                    "data": [{"ts": "1772816000000", "oi": "20500", "oiUsd": "1440000000"}],
                },
            )

        if url == "https://www.okx.com/api/v5/public/funding-rate-history":
            return FakeHttpResponse(
                status_code=200,
                payload={
                    "code": "0",
                    "data": [
                        {"fundingTime": "1772800000000", "fundingRate": "0.00008"},
                        {"fundingTime": "1772808000000", "fundingRate": "0.00010"},
                        {"fundingTime": "1772816000000", "fundingRate": "0.00011"},
                    ],
                },
            )

        if url == "https://www.alphavantage.co/query":
            function_name = params.get("function")
            if function_name == "NEWS_SENTIMENT":
                return FakeHttpResponse(
                    status_code=200,
                    payload={
                        "feed": [
                            {
                                "title": "BTC institutional demand expands",
                                "source": "Reuters",
                                "time_published": "20260308T000000",
                                "url": "https://example.com/news1",
                                "overall_sentiment_score": "0.40",
                                "overall_sentiment_label": "Bullish",
                                "summary": "Sample summary 1",
                            },
                            {
                                "title": "BTC options positioning remains constructive",
                                "source": "Bloomberg",
                                "time_published": "20260307T230000",
                                "url": "https://example.com/news2",
                                "overall_sentiment_score": "0.25",
                                "overall_sentiment_label": "Somewhat-Bullish",
                                "summary": "Sample summary 2",
                            },
                            {
                                "title": "Macro uncertainty caps upside",
                                "source": "CNBC",
                                "time_published": "20260307T220000",
                                "url": "https://example.com/news3",
                                "overall_sentiment_score": "-0.05",
                                "overall_sentiment_label": "Neutral",
                                "summary": "Sample summary 3",
                            },
                        ]
                    },
                )
            if function_name == "GLOBAL_QUOTE":
                symbol = str(params.get("symbol"))
                mapping = {
                    "SPY": ("510.10", "2.10", "0.41%", "2026-03-07"),
                    "QQQ": ("440.20", "3.40", "0.78%", "2026-03-07"),
                    "GLD": ("210.30", "0.40", "0.19%", "2026-03-07"),
                    "UUP": ("29.10", "-0.05", "-0.17%", "2026-03-07"),
                }
                price, change_abs, change_pct, day = mapping[symbol]
                return FakeHttpResponse(
                    status_code=200,
                    payload={
                        "Global Quote": {
                            "01. symbol": symbol,
                            "05. price": price,
                            "09. change": change_abs,
                            "10. change percent": change_pct,
                            "07. latest trading day": day,
                        }
                    },
                )

        if url == "https://api.alternative.me/fng/":
            rows = []
            values = [62, 58, 55, 52, 49, 47, 50, 53, 54, 56]
            base_ts = 1772816000
            for idx, value in enumerate(values):
                rows.append(
                    {
                        "value": str(value),
                        "value_classification": "Greed" if value >= 56 else "Neutral",
                        "timestamp": str(base_ts - idx * 86400),
                    }
                )
            return FakeHttpResponse(status_code=200, payload={"data": rows})

        if url == "https://blockchain.info/ticker":
            return FakeHttpResponse(status_code=200, payload={"USD": {"last": 70550}})

        if url.startswith("https://api.blockchain.info/charts/"):
            chart_name = url.split("/")[-1]
            values_map = {
                "hash-rate": [100, 101, 102, 103, 104],
                "mempool-count": [20, 21, 19, 18, 17],
                "transactions-per-second": [5.1, 5.2, 5.3, 5.4, 5.6],
                "estimated-transaction-volume-usd": [1000000, 1010000, 1025000, 1030000, 1050000],
            }
            values = values_map[chart_name]
            payload = {
                "values": [
                    {"x": 1772400000 + idx * 3600, "y": str(v)}
                    for idx, v in enumerate(values)
                ]
            }
            return FakeHttpResponse(status_code=200, payload=payload)

        raise RuntimeError(f"Unexpected external URL in fake router: {url} params={params}")

    return router


# ---------------------------------------------------------------------
# Module import helpers
# ---------------------------------------------------------------------
def _reload(module_name: str) -> Any:
    if module_name in sys.modules:
        return importlib.reload(sys.modules[module_name])
    return importlib.import_module(module_name)


def _import_core_modules() -> Dict[str, Any]:
    modules = {}
    modules["settings"] = _reload("settings")
    modules["analysis.binance_market_fetcher"] = _reload("analysis.binance_market_fetcher")
    modules["analysis.crypto_news_fetcher"] = _reload("analysis.crypto_news_fetcher")
    modules["analysis.derivatives_market_fetcher"] = _reload("analysis.derivatives_market_fetcher")
    modules["analysis.gpt_analyst_engine"] = _reload("analysis.gpt_analyst_engine")
    modules["analysis.macro_market_fetcher"] = _reload("analysis.macro_market_fetcher")
    modules["analysis.market_analyzer"] = _reload("analysis.market_analyzer")
    modules["analysis.market_researcher"] = _reload("analysis.market_researcher")
    modules["analysis.onchain_fetcher"] = _reload("analysis.onchain_fetcher")
    modules["analysis.quant_analyst"] = _reload("analysis.quant_analyst")
    modules["analysis.sentiment_fetcher"] = _reload("analysis.sentiment_fetcher")
    modules["analysis.trade_analyzer"] = _reload("analysis.trade_analyzer")
    modules["analysis.auto_reporter"] = _reload("analysis.auto_reporter")
    return modules


# ---------------------------------------------------------------------
# Stage runner
# ---------------------------------------------------------------------
def _run_stage(
    stage_name: str,
    successes: List[StageSuccess],
    failures: List[StageFailure],
    func: Callable[[], Dict[str, Any]],
) -> None:
    try:
        details = func()
        successes.append(StageSuccess(stage=stage_name, details=details))
    except Exception as exc:
        tb_text = traceback.format_exc()
        failures.append(
            StageFailure(
                stage=stage_name,
                module=getattr(func, "__module__", "unknown"),
                function=getattr(func, "__name__", stage_name),
                error_type=exc.__class__.__name__,
                error_message=str(exc),
                traceback_text=tb_text,
            )
        )


# ---------------------------------------------------------------------
# Individual stages
# ---------------------------------------------------------------------
def stage_static_policy_scan() -> Dict[str, Any]:
    target_files = {
        "settings.py": ROOT_DIR / "settings.py",
        "trade_analyzer.py": ROOT_DIR / "analysis" / "trade_analyzer.py",
        "market_researcher.py": ROOT_DIR / "analysis" / "market_researcher.py",
    }

    findings: List[Dict[str, Any]] = []

    # 1) top-level print detection
    trade_src = target_files["trade_analyzer.py"].read_text(encoding="utf-8")
    trade_tree = ast.parse(trade_src)
    for node in trade_tree.body:
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
            func = node.value.func
            if isinstance(func, ast.Name) and func.id == "print":
                findings.append(
                    {
                        "type": "top_level_print",
                        "file": "analysis/trade_analyzer.py",
                        "lineno": node.lineno,
                        "message": "import 시 실행되는 print() 발견",
                    }
                )

    # 2) swallow fallback detection in market_researcher
    market_src = target_files["market_researcher.py"].read_text(encoding="utf-8")
    if "force_orders = []" in market_src and "except Exception as e:" in market_src:
        findings.append(
            {
                "type": "swallowed_fallback",
                "file": "analysis/market_researcher.py",
                "lineno": 362,
                "message": "force_orders 예외를 잡고 빈 리스트로 진행하는 fallback 흔적 발견",
            }
        )

    if findings:
        raise RuntimeError(json.dumps(findings, ensure_ascii=False))

    return {"policy_scan": "clean"}


def stage_import_and_settings() -> Dict[str, Any]:
    modules = _import_core_modules()
    settings_mod = modules["settings"]
    loaded = settings_mod.load_settings()
    if loaded.symbol != "BTCUSDT":
        raise RuntimeError(f"Unexpected symbol loaded: {loaded.symbol}")
    if loaded.ANALYST_MARKET_SYMBOL != "BTCUSDT":
        raise RuntimeError("Uppercase alias access failed for ANALYST_MARKET_SYMBOL")
    return {
        "imported_module_count": len(modules),
        "symbol": loaded.symbol,
        "analyst_model": loaded.analyst_openai_model,
    }


def stage_binance_fetcher() -> Dict[str, Any]:
    mod = _reload("analysis.binance_market_fetcher")
    fetcher = mod.BinanceMarketFetcher()
    fetcher._session = FakeHttpSession(_sample_binance_router())
    snapshot = fetcher.fetch_market_snapshot()
    if snapshot.symbol != "BTCUSDT":
        raise RuntimeError("Binance snapshot symbol mismatch")
    if len(snapshot.klines) < 5:
        raise RuntimeError("Binance snapshot kline count too small")
    return {
        "symbol": snapshot.symbol,
        "kline_count": len(snapshot.klines),
        "spread_bps": str(snapshot.book_ticker.spread_bps),
        "force_orders": len(snapshot.force_orders),
    }


def stage_external_fetchers() -> Dict[str, Any]:
    derivatives_mod = _reload("analysis.derivatives_market_fetcher")
    news_mod = _reload("analysis.crypto_news_fetcher")
    macro_mod = _reload("analysis.macro_market_fetcher")
    sentiment_mod = _reload("analysis.sentiment_fetcher")
    onchain_mod = _reload("analysis.onchain_fetcher")

    session = FakeHttpSession(_sample_external_router())

    derivatives = derivatives_mod.DerivativesMarketFetcher()
    derivatives._session = session
    derivatives_snapshot = derivatives.fetch()

    news = news_mod.CryptoNewsFetcher()
    news._session = session
    news_snapshot = news.fetch()

    macro = macro_mod.MacroMarketFetcher()
    macro._session = session
    macro_snapshot = macro.fetch()

    sentiment = sentiment_mod.SentimentFetcher()
    sentiment._session = session
    sentiment_snapshot = sentiment.fetch()

    onchain = onchain_mod.OnchainFetcher()
    onchain._session = session
    onchain_snapshot = onchain.fetch()

    return {
        "derivatives_bias": derivatives_snapshot.cross_exchange_crowding_bias,
        "news_bias": news_snapshot.sentiment_bias,
        "macro_bias": macro_snapshot.cross_asset_bias,
        "sentiment_regime": sentiment_snapshot.sentiment_regime,
        "onchain_regime": onchain_snapshot.network_regime,
    }


def stage_market_analyzer() -> Dict[str, Any]:
    mod = _reload("analysis.market_analyzer")

    market_feature_rows = [
        {
            "ts_ms": 1772813200000 + idx * 300000,
            "symbol": "BTCUSDT",
            "timeframe": "5m",
            "close_price": Decimal("70000") + Decimal(str(idx * 100)),
            "spread_bps": Decimal("2.0"),
            "orderbook_imbalance": Decimal("0.10"),
            "pattern_score": Decimal("0.70"),
            "volatility_score": Decimal("0.50"),
            "trend_score": Decimal("0.80"),
            "market_regime": "TREND_EXPANSION",
            "liquidity_score": Decimal("0.90"),
        }
        for idx in range(6)
    ]
    trade_context_row = {
        "ts_ms": 1772815000000,
        "symbol": "BTCUSDT",
        "price": Decimal("70550"),
        "spread_bps": Decimal("2.1"),
        "pattern_score": Decimal("0.72"),
        "market_regime": "TREND_EXPANSION",
        "orderbook_imbalance": Decimal("0.12"),
        "funding_rate": Decimal("0.0003"),
        "open_interest": Decimal("10700"),
        "long_short_ratio": Decimal("1.24"),
    }

    def executor(sql: str, params: Mapping[str, Any]) -> FakeResult:
        _ = params
        if "FROM market_features" in sql:
            return FakeResult(rows=market_feature_rows)
        if "FROM trade_context_snapshots" in sql:
            return FakeResult(rows=[trade_context_row])
        raise RuntimeError(f"Unexpected SQL in MarketAnalyzer stage: {sql}")

    with mock.patch.object(mod, "get_session", side_effect=lambda: fake_session_cm(executor)):
        analyzer = mod.MarketAnalyzer()
        summary = analyzer.run()

    return {
        "symbol": summary.symbol,
        "regime": summary.market_regime,
        "trend": summary.trend,
        "entry_blockers": list(summary.entry_blockers),
    }


def stage_trade_analyzer() -> Dict[str, Any]:
    mod = _reload("analysis.trade_analyzer")

    trade_rows = [
        {
            "id": idx,
            "symbol": "BTCUSDT",
            "side": "LONG" if idx % 2 else "SHORT",
            "status": "CLOSED",
            "entry_price": Decimal("70000") + Decimal(str(idx * 10)),
            "exit_price": Decimal("70010") + Decimal(str(idx * 10)),
            "realized_pnl": Decimal("5") if idx % 3 else Decimal("-3"),
            "opened_ts_ms": 1772800000000 + idx * 100000,
            "closed_ts_ms": 1772800005000 + idx * 100000,
            "exit_reason": "tp_hit" if idx % 3 else "sl_hit",
        }
        for idx in range(1, 11)
    ]
    event_rows = [
        {
            "id": 1,
            "symbol": "BTCUSDT",
            "event_type": "ENTRY_GUARD_SKIP",
            "event_ts_ms": 1772810000000,
            "trade_id": None,
            "side": None,
            "reason": "spread_guard_blocked",
            "regime": "RANGE",
            "extra_json": None,
        },
        {
            "id": 2,
            "symbol": "BTCUSDT",
            "event_type": "STOP_LOSS",
            "event_ts_ms": 1772811000000,
            "trade_id": 3,
            "side": "LONG",
            "reason": "sl_hit",
            "regime": "RANGE",
            "extra_json": None,
        },
    ]
    snapshot_rows = []
    for idx in range(1, 11):
        snapshot_rows.append(
            {
                "id": idx,
                "trade_id": idx,
                "snapshot_ts_ms": 1772800000000 + idx * 100000,
                "spread_bps": Decimal("2.5"),
                "pattern_score": Decimal("0.70") if idx != 3 else Decimal("0.30"),
                "market_regime": "RANGE" if idx == 3 else "TREND_EXPANSION",
                "orderbook_imbalance": Decimal("0.10"),
                "volatility_score": Decimal("0.40"),
            }
        )

    def executor(sql: str, params: Mapping[str, Any]) -> FakeResult:
        if "FROM bt_trades" in sql:
            return FakeResult(rows=trade_rows)
        if "FROM bt_events" in sql:
            return FakeResult(rows=event_rows)
        if "FROM bt_trade_snapshots" in sql:
            _ = params
            return FakeResult(rows=snapshot_rows)
        raise RuntimeError(f"Unexpected SQL in TradeAnalyzer stage: {sql}")

    with mock.patch.object(mod, "get_session", side_effect=lambda: fake_session_cm(executor)):
        analyzer = mod.TradeAnalyzer()
        summary = analyzer.run()

    return {
        "total_trades": summary.total_trades,
        "wins": summary.wins,
        "losses": summary.losses,
        "entry_failure_reasons": list(summary.entry_failure_reasons),
        "loss_causes": list(summary.loss_causes),
    }


def stage_gpt_engine() -> Dict[str, Any]:
    mod = _reload("analysis.gpt_analyst_engine")

    good_response = {
        "output_parsed": {
            "scope": "market_analysis",
            "answer_ko": "외부 시장 구조는 완만한 상승 우위입니다.",
            "root_causes": ["펀딩이 양수이고 오픈이자가 증가했습니다."],
            "recommendations": ["외부 구조와 내부 진입 조건의 정합성만 점검하십시오."],
            "confidence": 0.82,
            "used_inputs": ["external_market_summary"],
        },
        "usage": {"input_tokens": 100, "output_tokens": 50, "total_tokens": 150},
    }

    bad_json_response = {
        "output": [
            {
                "type": "message",
                "content": [
                    {"type": "output_text", "text": '{"scope":"market_analysis" "answer_ko":"깨진 json"}'}
                ],
            }
        ]
    }

    incomplete_response = AttrObject(
        status="incomplete",
        incomplete_details={"reason": "max_output_tokens"},
        usage={"input_tokens": 120, "output_tokens": 800, "total_tokens": 920},
    )

    payload = {
        "question": "현재 시장 분위기 알려줘",
        "external_market_summary": {"symbol": "BTCUSDT", "trend": "strong_uptrend"},
    }

    with mock.patch.object(mod.GptAnalystEngine, "_make_client", return_value=FakeOpenAIClient(lambda _: good_response)):
        engine = mod.GptAnalystEngine()
        ok_result = engine._request_analysis_or_raise(payload)

    if ok_result.scope != "market_analysis":
        raise RuntimeError("GptAnalystEngine success parse failed")

    with mock.patch.object(mod.GptAnalystEngine, "_make_client", return_value=FakeOpenAIClient(lambda _: bad_json_response)):
        engine_bad = mod.GptAnalystEngine()
        try:
            engine_bad._request_analysis_or_raise(payload)
        except RuntimeError as exc:
            bad_message = str(exc)
        else:
            raise RuntimeError("Invalid JSON response was expected to fail")

    with mock.patch.object(mod.GptAnalystEngine, "_make_client", return_value=FakeOpenAIClient(lambda _: incomplete_response)):
        engine_incomplete = mod.GptAnalystEngine()
        try:
            engine_incomplete._request_analysis_or_raise(payload)
        except RuntimeError as exc:
            incomplete_message = str(exc)
        else:
            raise RuntimeError("Incomplete response was expected to fail")

    return {
        "success_scope": ok_result.scope,
        "invalid_json_error": bad_message,
        "incomplete_error": incomplete_message,
    }


def stage_market_researcher() -> Dict[str, Any]:
    mod = _reload("analysis.market_researcher")
    binance_mod = _reload("analysis.binance_market_fetcher")
    derivatives_mod = _reload("analysis.derivatives_market_fetcher")
    news_mod = _reload("analysis.crypto_news_fetcher")
    macro_mod = _reload("analysis.macro_market_fetcher")
    sentiment_mod = _reload("analysis.sentiment_fetcher")
    onchain_mod = _reload("analysis.onchain_fetcher")

    # build snapshots through real fetchers + fake HTTP
    binance_fetcher = binance_mod.BinanceMarketFetcher()
    binance_fetcher._session = FakeHttpSession(_sample_binance_router())
    snapshot = binance_fetcher.fetch_market_snapshot()

    external_session = FakeHttpSession(_sample_external_router())
    derivatives = derivatives_mod.DerivativesMarketFetcher()
    derivatives._session = external_session
    derivatives_snapshot = derivatives.fetch()

    news = news_mod.CryptoNewsFetcher()
    news._session = external_session
    news_snapshot = news.fetch()

    macro = macro_mod.MacroMarketFetcher()
    macro._session = external_session
    macro_snapshot = macro.fetch()

    sentiment = sentiment_mod.SentimentFetcher()
    sentiment._session = external_session
    sentiment_snapshot = sentiment.fetch()

    onchain = onchain_mod.OnchainFetcher()
    onchain._session = external_session
    onchain_snapshot = onchain.fetch()

    inserted = {"market_features": 0, "trade_context_snapshots": 0}

    def executor(sql: str, params: Mapping[str, Any]) -> FakeResult:
        if "SELECT to_regclass" in sql:
            return FakeResult(rows=[{"regname": "ok"}])
        if "FROM market_features" in sql and "LIMIT 1" in sql:
            return FakeResult(rows=[], scalar=None)
        if "FROM trade_context_snapshots" in sql and "LIMIT 1" in sql:
            return FakeResult(rows=[], scalar=None)
        if "INSERT INTO market_features" in sql:
            inserted["market_features"] += 1
            return FakeResult(rows=[])
        if "INSERT INTO trade_context_snapshots" in sql:
            inserted["trade_context_snapshots"] += 1
            return FakeResult(rows=[])
        raise RuntimeError(f"Unexpected SQL in MarketResearcher stage: {sql}")

    with mock.patch.object(mod.MarketResearcher, "_fetch_market_snapshot_strict", return_value=snapshot), \
         mock.patch.object(mod.DerivativesMarketFetcher, "fetch", return_value=derivatives_snapshot), \
         mock.patch.object(mod.CryptoNewsFetcher, "fetch", return_value=news_snapshot), \
         mock.patch.object(mod.MacroMarketFetcher, "fetch", return_value=macro_snapshot), \
         mock.patch.object(mod.SentimentFetcher, "fetch", return_value=sentiment_snapshot), \
         mock.patch.object(mod.OnchainFetcher, "fetch", return_value=onchain_snapshot), \
         mock.patch.object(mod, "get_session", side_effect=lambda: fake_session_cm(executor)):
        researcher = mod.MarketResearcher()
        report = researcher.run()
        sync_result = researcher.sync_once()

    return {
        "report_regime": report.market_regime,
        "report_conviction": report.conviction,
        "sync_market_features_inserted": sync_result.market_features_inserted,
        "sync_trade_context_inserted": sync_result.trade_context_inserted,
        "insert_counts": dict(inserted),
    }


def stage_quant_analyst() -> Dict[str, Any]:
    mod = _reload("analysis.quant_analyst")

    qa = mod.QuantAnalyst()

    class FakeInternal:
        as_of_ms = 1772815000000
        symbol = "BTCUSDT"
        timeframe = "5m"
        market_regime = "TREND_EXPANSION"
        trend = "strong_uptrend"
        volatility = "medium"
        liquidity = "high"
        latest_close_price = Decimal("70550")
        price_change_pct = Decimal("0.80")
        avg_spread_bps = Decimal("2.00")
        latest_spread_bps = Decimal("2.10")
        avg_pattern_score = Decimal("0.70")
        latest_pattern_score = Decimal("0.72")
        entry_blockers = ["none"]
        analyst_summary_ko = "내부 시장은 상승 추세입니다."
        def to_dict(self) -> Dict[str, Any]:
            return {"symbol": self.symbol, "dashboard_payload": {"ok": True}, "market_regime": self.market_regime}

    class FakeTrade:
        as_of_ts_ms = 1772815000000
        symbol = "BTCUSDT"
        total_trades = 10
        wins = 6
        losses = 4
        breakeven = 0
        win_rate_pct = Decimal("60")
        total_pnl = Decimal("12")
        avg_pnl = Decimal("1.2")
        avg_win_pnl = Decimal("3")
        avg_loss_pnl = Decimal("-1.5")
        entry_failure_reasons = ["spread_guard_blocked:2"]
        loss_causes = ["weak_pattern_entry:1"]
        recent_trade_briefs = []
        analyst_summary_ko = "최근 거래 성능은 보통입니다."
        def to_dict(self) -> Dict[str, Any]:
            return {"symbol": self.symbol, "dashboard_payload": {"ok": True}, "total_trades": self.total_trades}

    class FakeExternal:
        as_of_ms = 1772815000000
        symbol = "BTCUSDT"
        trend = "strong_uptrend"
        volatility = "medium"
        liquidity = "high"
        market_regime = "trend_expansion"
        conviction = "high"
        price = Decimal("70550")
        mark_price = Decimal("70550")
        index_price = Decimal("70520")
        spread_bps = Decimal("2.10")
        funding_rate = Decimal("0.0003")
        open_interest = Decimal("10700")
        open_interest_change_pct = Decimal("7.0")
        global_long_short_ratio = Decimal("1.24")
        top_long_short_ratio = Decimal("1.24")
        taker_long_short_ratio = Decimal("1.15")
        crowding_bias = "long_crowded"
        liquidation_pressure = "none"
        support_price = Decimal("69950")
        resistance_price = Decimal("70600")
        key_signals = ["trend=strong_uptrend"]
        analyst_summary_ko = "외부 시장은 완만한 상승 구조입니다."
        def to_dict(self) -> Dict[str, Any]:
            return {"symbol": self.symbol, "dashboard_payload": {"ok": True}, "market_regime": self.market_regime}

    fake_internal = FakeInternal()
    fake_trade = FakeTrade()
    fake_external = FakeExternal()

    qa._market_analyzer = AttrObject(run=lambda: fake_internal)
    qa._trade_analyzer = AttrObject(run=lambda: fake_trade)
    qa._market_researcher = AttrObject(run=lambda: fake_external)
    qa._gpt_engine = AttrObject(
        analyze=lambda **kwargs: mod.GptAnalystResult(
            scope="mixed",
            answer_ko="내부와 외부 모두 상승 우위입니다.",
            root_causes=["내부 추세와 외부 구조가 같은 방향입니다."],
            recommendations=["진입 필터 정합성만 점검하십시오."],
            confidence=0.85,
            used_inputs=["internal_market_summary", "trade_summary", "external_market_summary"],
            raw_response_text="{}",
        ),
        analyze_market_only=lambda **kwargs: mod.GptAnalystResult(
            scope="market_analysis",
            answer_ko="외부 구조만 보면 상승 우위입니다.",
            root_causes=["펀딩과 오픈이자가 동반 증가했습니다."],
            recommendations=["외부 구조 일치 여부를 확인하십시오."],
            confidence=0.80,
            used_inputs=["external_market_summary"],
            raw_response_text="{}",
        ),
    )

    full_result = qa.analyze(question="왜 최근 진입이 좋았는지 알려줘", include_external_market=True)
    market_only_result = qa.analyze_market_only(question="현재 외부 시장만 요약해줘")

    return {
        "full_scope": full_result.gpt_result["scope"],
        "full_used_inputs": list(full_result.gpt_result["used_inputs"]),
        "market_only_scope": market_only_result.gpt_result["scope"],
        "market_only_used_inputs": list(market_only_result.gpt_result["used_inputs"]),
    }


def stage_auto_reporter() -> Dict[str, Any]:
    mod = _reload("analysis.auto_reporter")
    quant_mod = _reload("analysis.quant_analyst")

    emitted_events: List[Dict[str, Any]] = []
    notifications: List[Dict[str, Any]] = []

    reporter = mod.AutoReporter(
        event_writer=lambda event_type, report_type, payload: emitted_events.append(
            {"event_type": event_type, "report_type": report_type, "payload": payload}
        ),
        notifier=lambda report_type, payload: notifications.append(
            {"report_type": report_type, "payload": payload}
        ),
    )

    reporter._quant_analyst = AttrObject(
        analyze=lambda **kwargs: quant_mod.QuantAnalystResponse(
            symbol="BTCUSDT",
            question=str(kwargs["question"]),
            generated_ts_ms=1772815000000,
            internal_market_summary=None,
            trade_summary=None,
            external_market_summary={"symbol": "BTCUSDT"},
            gpt_result={
                "scope": "market_analysis",
                "answer_ko": "시장 구조는 상승 우위입니다.",
                "root_causes": ["오픈이자가 증가했습니다."],
                "recommendations": ["내부 진입 조건만 확인하십시오."],
                "confidence": 0.8,
                "used_inputs": ["external_market_summary"],
            },
            dashboard_payload={"scope": "market_analysis", "question": kwargs["question"]},
        )
    )

    result = reporter.run_due_reports(now_ms=1772815000000)
    if len(result.reports) != 2:
        raise RuntimeError(f"Expected 2 due reports on first run, got={len(result.reports)}")

    return {
        "report_count": len(result.reports),
        "persisted_count": len(emitted_events),
        "notified_count": len(notifications),
        "status": reporter.get_status(),
    }


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="AI analyst debug runner")
    parser.add_argument("--json", action="store_true", help="print JSON only")
    args = parser.parse_args()

    successes: List[StageSuccess] = []
    failures: List[StageFailure] = []

    stages: List[tuple[str, Callable[[], Dict[str, Any]]]] = [
        ("static_policy_scan", stage_static_policy_scan),
        ("import_and_settings", stage_import_and_settings),
        ("binance_fetcher", stage_binance_fetcher),
        ("external_fetchers", stage_external_fetchers),
        ("market_analyzer", stage_market_analyzer),
        ("trade_analyzer", stage_trade_analyzer),
        ("gpt_engine", stage_gpt_engine),
        ("market_researcher", stage_market_researcher),
        ("quant_analyst", stage_quant_analyst),
        ("auto_reporter", stage_auto_reporter),
    ]

    for stage_name, func in stages:
        _run_stage(stage_name, successes, failures, func)

    report = DebugReport(
        ok=(len(failures) == 0),
        successes=[asdict(item) for item in successes],
        failures=[asdict(item) for item in failures],
    )

    if args.json:
        print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
    else:
        print("=" * 72)
        print("AI ANALYST DEBUG REPORT")
        print("=" * 72)
        print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))

    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
