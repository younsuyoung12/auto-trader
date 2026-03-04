"""
========================================================
FILE: test_meta_prompt_builder.py
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
  1) test_meta_prompt_builder.py 대상 단위 테스트 추가
  2) 실패/누락/정책 위반 케이스 포함
  3) 외부 I/O(OpenAI/DB) 없이 동작하도록 monkeypatch 사용
========================================================
"""

from __future__ import annotations

from typing import Any, Dict, List

from tests_tradegrade._test_util import expect_raises

from meta.meta_prompt_builder import MetaPromptBuilderError, MetaPromptConfig, build_meta_prompts


def _stats_ok() -> Dict[str, Any]:
    # meta_prompt_builder._require_stats_shape 요구 키를 모두 포함
    return {
        "symbol": "BTCUSDT",
        "generated_at_utc": "2026-03-04T00:00:00+00:00",
        "n_trades": 20,
        "wins": 10,
        "losses": 9,
        "breakevens": 1,
        "win_rate_pct": 50.0,
        "total_pnl_usdt": 1.2,
        "avg_pnl_usdt": 0.06,
        "recent_win_rate_pct": 45.0,
        "baseline_win_rate_pct": 55.0,
        "recent_avg_pnl_usdt": -0.01,
        "baseline_avg_pnl_usdt": 0.10,
        "by_direction": {"LONG": {"n": 10}, "SHORT": {"n": 10}},
        "by_regime": {"RANGE": {"n": 10}, "TREND": {"n": 10}},
        "by_engine_total_bucket": {"50-60": {"n": 5}},
        "by_entry_score_bucket": {"40-50": {"n": 5}},
        "recent_trades": [
            {
                "trade_id": 1,
                "entry_ts": "2026-03-03T00:00:00+00:00",
                "exit_ts": "2026-03-03T01:00:00+00:00",
                "direction": "LONG",
                "regime": "RANGE",
                "pnl_usdt": 0.1,
            }
        ],
    }


def test_build_prompts_ok() -> None:
    cfg = MetaPromptConfig(max_recent_trades=1, language="ko", max_output_sentences=3, prompt_version="2026-03-03")
    system_prompt, payload = build_meta_prompts(stats=_stats_ok(), cfg=cfg, current_settings={
        "allocation_ratio": 0.2,
        "leverage": 3,
        "max_spread_pct": 0.002,
        "max_price_jump_pct": 0.01,
        "min_entry_volume_ratio": 0.2,
        "slippage_block_pct": 0.002,
        "reconcile_interval_sec": 30,
        "force_close_on_desync": False,
        "max_signal_latency_ms": 1000,
        "max_exec_latency_ms": 2000,
    })
    assert "STRICT · NO-FALLBACK" in system_prompt
    assert payload["symbol"] == "BTCUSDT"
    assert isinstance(payload["recent_trades"], list) and len(payload["recent_trades"]) == 1


def test_missing_required_key() -> None:
    bad = _stats_ok()
    del bad["wins"]
    expect_raises(MetaPromptBuilderError, lambda: build_meta_prompts(stats=bad))


def test_sensitive_key_rejected() -> None:
    # current_settings에 민감 키 포함 → 즉시 예외
    expect_raises(MetaPromptBuilderError, lambda: build_meta_prompts(stats=_stats_ok(), current_settings={"openai_api_key": "x"}))


def test_recent_trade_missing_field() -> None:
    bad = _stats_ok()
    bad["recent_trades"] = [{"trade_id": 1}]  # 최소 필드 누락
    expect_raises(MetaPromptBuilderError, lambda: build_meta_prompts(stats=bad, cfg=MetaPromptConfig(max_recent_trades=5)))


if __name__ == "__main__":
    test_build_prompts_ok()
    test_missing_required_key()
    test_sensitive_key_rejected()
    test_recent_trade_missing_field()
    print("OK")
