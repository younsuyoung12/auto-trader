"""
========================================================
FILE: test_meta_strategy_engine.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

설계 원칙:
- 폴백 금지
- 데이터 누락 시 즉시 예외
- 예외 삼키기 금지
- 설정은 settings.py 단일 소스
- DB 접근은 db_core 경유

변경 이력
--------------------------------------------------------
- 2026-03-04:
  1) test_meta_strategy_engine.py 대상 단위 테스트 추가
  2) 실패/누락/정책 위반 케이스 포함
  3) 외부 I/O(OpenAI/DB) 없이 동작하도록 monkeypatch 사용
========================================================
"""

from __future__ import annotations

import importlib
import sys
import types
from dataclasses import dataclass
from typing import Any, Dict

from tests_tradegrade._test_util import expect_raises


# ---- Dummy strategy.gpt_engine module to avoid network and settings ----
@dataclass(frozen=True, slots=True)
class GptJsonResponse:
    model: str
    latency_sec: float
    text: str
    obj: Dict[str, Any]


class GptEngineError(RuntimeError):
    pass


def call_chat_json(*, system_prompt: str, user_payload: Dict[str, Any], model: str | None = None, max_latency_sec: float | None = None, **kwargs: Any) -> GptJsonResponse:
    # 항상 유효한 meta_decision_validator JSON을 반환
    txt = (
        '{'
        '"version":"2026-03-03",'
        '"action":"ADJUST_PARAMS",'
        '"severity":1,'
        '"tags":["NEGATIVE_DRIFT"],'
        '"confidence":0.7,'
        '"ttl_sec":600,'
        '"recommendation":{'
        '"risk_multiplier_delta":-0.1,'
        '"allocation_ratio_cap":0.3,'
        '"disable_directions":[],'
        '"guard_multipliers":{"max_spread_pct_mult":1.0,"max_price_jump_pct_mult":1.0,"min_entry_volume_ratio_mult":1.0}'
        '},'
        '"rationale_short":"최근 성능이 약화되어 리스크를 줄입니다."'
        '}'
    )
    return GptJsonResponse(model=(model or "gpt-test"), latency_sec=0.01, text=txt, obj={})


def _install_dummy_strategy_gpt_engine() -> None:
    m = types.ModuleType("strategy.gpt_engine")
    m.GptJsonResponse = GptJsonResponse  # type: ignore[attr-defined]
    m.GptEngineError = GptEngineError  # type: ignore[attr-defined]
    m.call_chat_json = call_chat_json  # type: ignore[attr-defined]
    sys.modules["strategy.gpt_engine"] = m


# ---- Dummy settings.load_settings ----
class _FakeSettings:
    allocation_ratio = 0.2
    leverage = 3.0
    max_spread_pct = 0.002
    max_price_jump_pct = 0.01
    min_entry_volume_ratio = 0.2
    slippage_block_pct = 0.002
    reconcile_interval_sec = 30
    force_close_on_desync = False
    max_signal_latency_ms = 1000
    max_exec_latency_ms = 2000
    exit_supervisor_enabled = False
    exit_supervisor_cooldown_sec = 180.0
    meta_max_rationale_sentences = 3
    meta_max_rationale_len = 800


def _import_meta_engine() -> Any:
    _install_dummy_strategy_gpt_engine()

    import settings
    orig = getattr(settings, "load_settings")
    settings.load_settings = lambda: _FakeSettings()  # type: ignore[assignment]
    try:
        if "meta.meta_strategy_engine" in sys.modules:
            del sys.modules["meta.meta_strategy_engine"]
        mod = importlib.import_module("meta.meta_strategy_engine")
        return mod, orig
    finally:
        settings.load_settings = orig  # type: ignore[assignment]


def test_run_once_and_cooldown() -> None:
    mod, _orig = _import_meta_engine()

    # stats/prompt를 외부 I/O 없이 고정
    mod.build_meta_stats_dict = lambda cfg: {  # type: ignore[attr-defined]
        "symbol": "BTCUSDT",
        "generated_at_utc": "2026-03-04T00:00:00+00:00",
        "n_trades": 30,
        "wins": 15,
        "losses": 14,
        "breakevens": 1,
        "win_rate_pct": 50.0,
        "total_pnl_usdt": 1.0,
        "avg_pnl_usdt": 0.03,
        "recent_win_rate_pct": 45.0,
        "baseline_win_rate_pct": 55.0,
        "recent_avg_pnl_usdt": -0.01,
        "baseline_avg_pnl_usdt": 0.10,
        "by_direction": {"LONG": {"n": 15}, "SHORT": {"n": 15}},
        "by_regime": {"RANGE": {"n": 15}, "TREND": {"n": 15}},
        "by_engine_total_bucket": {"50-60": {"n": 10}},
        "by_entry_score_bucket": {"40-50": {"n": 10}},
        "recent_trades": [{"trade_id": 1, "entry_ts": "x", "exit_ts": "y", "direction": "LONG", "regime": "RANGE", "pnl_usdt": 0.1}],
    }

    mod.build_meta_prompts = lambda stats, cfg, current_settings=None: ("sys", {"p": 1})  # type: ignore[attr-defined]

    cfg = mod.MetaStrategyConfig(
        stats_cfg=mod.MetaStatsConfig(symbol="BTCUSDT", lookback_trades=200, recent_window=20, baseline_window=60, min_trades_required=20, exclude_test_trades=True),
        prompt_cfg=mod.MetaPromptConfig(max_recent_trades=20, language="ko", max_output_sentences=3, prompt_version="2026-03-03"),
        cooldown_sec=99999,
    )
    eng = mod.MetaStrategyEngine(cfg)
    rec = eng.run_once()
    assert rec.action == "ADJUST_PARAMS"
    assert rec.final_risk_multiplier <= 1.0

    # cooldown 위반 시 즉시 예외
    expect_raises(mod.MetaStrategyError, lambda: eng.run_once())


def test_invalid_gpt_text_rejected() -> None:
    mod, _orig = _import_meta_engine()

    # call_chat_json을 강제로 비정상 텍스트 반환하도록 교체
    def bad_call(**kwargs: Any):
        return GptJsonResponse(model="gpt", latency_sec=0.01, text="not json", obj={})
    mod.call_chat_json = bad_call  # type: ignore[attr-defined]

    mod.build_meta_stats_dict = lambda cfg: {  # type: ignore[attr-defined]
        "symbol": "BTCUSDT",
        "generated_at_utc": "2026-03-04T00:00:00+00:00",
        "n_trades": 30,
        "wins": 15,
        "losses": 14,
        "breakevens": 1,
        "win_rate_pct": 50.0,
        "total_pnl_usdt": 1.0,
        "avg_pnl_usdt": 0.03,
        "recent_win_rate_pct": 45.0,
        "baseline_win_rate_pct": 55.0,
        "recent_avg_pnl_usdt": -0.01,
        "baseline_avg_pnl_usdt": 0.10,
        "by_direction": {"LONG": {"n": 15}, "SHORT": {"n": 15}},
        "by_regime": {"RANGE": {"n": 15}, "TREND": {"n": 15}},
        "by_engine_total_bucket": {"50-60": {"n": 10}},
        "by_entry_score_bucket": {"40-50": {"n": 10}},
        "recent_trades": [{"trade_id": 1, "entry_ts": "x", "exit_ts": "y", "direction": "LONG", "regime": "RANGE", "pnl_usdt": 0.1}],
    }
    mod.build_meta_prompts = lambda stats, cfg, current_settings=None: ("sys", {"p": 1})  # type: ignore[attr-defined]

    cfg = mod.MetaStrategyConfig(
        stats_cfg=mod.MetaStatsConfig(symbol="BTCUSDT", lookback_trades=200, recent_window=20, baseline_window=60, min_trades_required=20, exclude_test_trades=True),
        prompt_cfg=mod.MetaPromptConfig(),
        cooldown_sec=1,
    )
    eng = mod.MetaStrategyEngine(cfg)
    expect_raises(mod.MetaStrategyError, lambda: eng.run_once())


if __name__ == "__main__":
    test_run_once_and_cooldown()
    test_invalid_gpt_text_rejected()
    print("OK")
