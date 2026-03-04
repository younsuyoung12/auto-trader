"""
========================================================
FILE: test_meta_decision_validator.py
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
  1) test_meta_decision_validator.py 대상 단위 테스트 추가
  2) 실패/누락/정책 위반 케이스 포함
  3) 외부 I/O(OpenAI/DB) 없이 동작하도록 monkeypatch 사용
========================================================
"""

from __future__ import annotations

from typing import Any, Dict

from tests_tradegrade._test_util import expect_raises

from meta.meta_decision_validator import (
    MetaDecisionContext,
    MetaDecisionValidationError,
    validate_meta_decision_dict,
    validate_meta_decision_text,
)


def _ctx() -> MetaDecisionContext:
    return MetaDecisionContext(
        symbol="BTCUSDT",
        base_risk_multiplier=0.8,
        base_allocation_ratio=0.2,
        base_max_spread_pct=0.002,
        base_max_price_jump_pct=0.01,
        base_min_entry_volume_ratio=0.2,
        max_rationale_sentences=3,
        max_rationale_len=800,
    )


def _ok_obj(action: str = "ADJUST_PARAMS") -> Dict[str, Any]:
    return {
        "version": "2026-03-03",
        "action": action,
        "severity": 1,
        "tags": ["NEGATIVE_DRIFT"],
        "confidence": 0.7,
        "ttl_sec": 600,
        "recommendation": {
            "risk_multiplier_delta": -0.1,
            "allocation_ratio_cap": 0.2,
            "disable_directions": [],
            "guard_multipliers": {
                "max_spread_pct_mult": 1.2,
                "max_price_jump_pct_mult": 1.0,
                "min_entry_volume_ratio_mult": 1.0,
            },
        },
        "rationale_short": "최근 성능이 약화되어 리스크를 줄입니다.",
    }


def test_validate_ok() -> None:
    v = validate_meta_decision_dict(response_obj=_ok_obj(), ctx=_ctx())
    assert abs(v.final_risk_multiplier - 0.7) < 1e-12
    assert v.effective_max_spread_pct > 0


def test_unknown_key_rejected() -> None:
    obj = _ok_obj()
    obj["unknown"] = 1
    expect_raises(MetaDecisionValidationError, lambda: validate_meta_decision_dict(response_obj=obj, ctx=_ctx()))


def test_no_change_invariants() -> None:
    obj = _ok_obj(action="NO_CHANGE")
    # NO_CHANGE인데 delta가 0이 아니면 즉시 실패
    obj["recommendation"]["risk_multiplier_delta"] = 0.1
    expect_raises(MetaDecisionValidationError, lambda: validate_meta_decision_dict(response_obj=obj, ctx=_ctx()))


def test_final_risk_multiplier_out_of_range() -> None:
    obj = _ok_obj()
    # base=0.8 + delta=+0.5 -> 1.3 (범위 초과)
    obj["recommendation"]["risk_multiplier_delta"] = 0.5
    expect_raises(MetaDecisionValidationError, lambda: validate_meta_decision_dict(response_obj=obj, ctx=_ctx()))


def test_disable_both_requires_safe_stop() -> None:
    obj = _ok_obj(action="ADJUST_PARAMS")
    obj["recommendation"]["disable_directions"] = ["LONG", "SHORT"]
    expect_raises(MetaDecisionValidationError, lambda: validate_meta_decision_dict(response_obj=obj, ctx=_ctx()))


def test_validate_text_requires_single_object() -> None:
    # 앞뒤 텍스트 있으면 실패(STRICT)
    txt = "note\n" + '{"a":1}' + "\nend"
    expect_raises(MetaDecisionValidationError, lambda: validate_meta_decision_text(response_text=txt, ctx=_ctx()))


if __name__ == "__main__":
    test_validate_ok()
    test_unknown_key_rejected()
    test_no_change_invariants()
    test_final_risk_multiplier_out_of_range()
    test_disable_both_requires_safe_stop()
    test_validate_text_requires_single_object()
    print("OK")
