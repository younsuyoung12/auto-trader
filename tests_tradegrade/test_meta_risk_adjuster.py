"""
========================================================
FILE: test_meta_risk_adjuster.py
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
  1) test_meta_risk_adjuster.py 대상 단위 테스트 추가
  2) 실패/누락/정책 위반 케이스 포함
  3) 외부 I/O(OpenAI/DB) 없이 동작하도록 monkeypatch 사용
========================================================
"""

from __future__ import annotations

from dataclasses import replace

from tests_tradegrade._test_util import expect_raises

from meta.meta_risk_adjuster import MetaApplyError, apply_allocation_ratio_from_recommendation, build_guard_adjustments_from_recommendation, build_meta_l3_audit_blob
from meta.meta_strategy_engine import MetaRecommendation


class _DummySettings:
    max_spread_pct = 0.002
    max_price_jump_pct = 0.01
    min_entry_volume_ratio = 0.2


def _rec_ok() -> MetaRecommendation:
    return MetaRecommendation(
        version="2026-03-03",
        action="ADJUST_PARAMS",
        severity=1,
        tags=["NEGATIVE_DRIFT"],
        confidence=0.7,
        ttl_sec=600,
        risk_multiplier_delta=-0.1,
        allocation_ratio_cap=0.3,
        disable_directions=[],
        guard_multipliers={"max_spread_pct_mult": 1.2, "max_price_jump_pct_mult": 1.0, "min_entry_volume_ratio_mult": 1.0},
        rationale_short="ok",
        final_risk_multiplier=0.8,
        effective_max_spread_pct=0.003,
        effective_max_price_jump_pct=0.01,
        effective_min_entry_volume_ratio=0.2,
        stats_generated_at_utc="2026-03-04T00:00:00+00:00",
        model="gpt-test",
        latency_sec=0.1,
    )


def test_apply_allocation_ratio_cap() -> None:
    rec = _rec_ok()
    # base*mult = 0.5*0.8=0.4, cap=0.3 -> 0.3
    out = apply_allocation_ratio_from_recommendation(0.5, rec)
    assert abs(out - 0.3) < 1e-12


def test_apply_allocation_ratio_invalid_mult() -> None:
    rec = _rec_ok()
    rec2 = replace(rec, final_risk_multiplier=1.5)
    expect_raises(MetaApplyError, lambda: apply_allocation_ratio_from_recommendation(0.2, rec2))


def test_build_guard_adjustments() -> None:
    rec = _rec_ok()
    g = build_guard_adjustments_from_recommendation(_DummySettings(), rec)
    assert g["max_spread_pct"] == rec.effective_max_spread_pct


def test_audit_blob() -> None:
    rec = _rec_ok()
    blob = build_meta_l3_audit_blob(rec, base_allocation_ratio=0.2, final_allocation_ratio=0.16)
    assert blob["final_allocation_ratio"] == 0.16
    assert "effective" in blob


if __name__ == "__main__":
    test_apply_allocation_ratio_cap()
    test_apply_allocation_ratio_invalid_mult()
    test_build_guard_adjustments()
    test_audit_blob()
    print("OK")
