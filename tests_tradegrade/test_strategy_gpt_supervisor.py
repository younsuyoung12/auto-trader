"""
========================================================
FILE: test_strategy_gpt_supervisor.py
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
  1) test_strategy_gpt_supervisor.py 대상 단위 테스트 추가
  2) 실패/누락/정책 위반 케이스 포함
  3) 외부 I/O(OpenAI/DB) 없이 동작하도록 monkeypatch 사용
========================================================
"""

from __future__ import annotations

import importlib
import sys
from typing import Any, Dict

from tests_tradegrade._test_util import expect_raises


class _FakeSettings:
    openai_api_key = "TEST_KEY"
    openai_model = "gpt-test"
    openai_max_tokens = 256
    openai_temperature = 0.2
    openai_max_latency_sec = 10.0


def _import_supervisor() -> Any:
    """
    STRICT:
    - strategy.gpt_supervisor import 시 strategy.gpt_engine이 필요하므로,
      settings.load_settings를 테스트용으로 고정한 뒤 import 한다.
    """
    import settings

    orig = getattr(settings, "load_settings")
    settings.load_settings = lambda: _FakeSettings()  # type: ignore[assignment]
    try:
        # reload modules cleanly
        for k in ["strategy.gpt_engine", "strategy.gpt_supervisor"]:
            if k in sys.modules:
                del sys.modules[k]
        mod = importlib.import_module("strategy.gpt_supervisor")
        return mod
    finally:
        settings.load_settings = orig  # type: ignore[assignment]


def _ok_obj() -> Dict[str, Any]:
    return {
        "auditor": {
            "severity": 1,
            "tags": ["spread_risk", "micro_distortion"],
            "rationale_short": "스프레드와 미세구조 지표 조합이 불안정합니다.",
            "confidence_penalty": 0.7,
            "suggested_risk_multiplier": 0.6,
        },
        "narration": {
            "title": "관망 신호",
            "message": "가격 변동이 커서 신중하게 보는 흐름입니다. 급하게 판단하지 않습니다.",
            "tone": "caution",
        },
        "postmortem": None,
    }


def test_validate_ok() -> None:
    sup = _import_supervisor()
    obj = _ok_obj()
    r = sup._validate_output(decision_id="d1", event_type="ENTRY", raw_text="{}", obj=obj)  # type: ignore[attr-defined]
    assert r.auditor.severity == 1
    assert r.narration is not None


def test_forbidden_key_anywhere() -> None:
    sup = _import_supervisor()
    obj = _ok_obj()
    obj["auditor"]["action"] = "ENTER"  # 금지 키
    expect_raises(sup.GptSupervisorError, lambda: sup._validate_output(decision_id="d1", event_type="ENTRY", raw_text="{}", obj=obj))  # type: ignore[attr-defined]


def test_prohibited_phrase_anywhere() -> None:
    sup = _import_supervisor()
    obj = _ok_obj()
    obj["narration"]["message"] = "지금 사세요"  # 금지 문구
    expect_raises(sup.GptSupervisorError, lambda: sup._validate_output(decision_id="d1", event_type="ENTRY", raw_text="{}", obj=obj))  # type: ignore[attr-defined]


def test_unknown_top_key() -> None:
    sup = _import_supervisor()
    obj = _ok_obj()
    obj["extra"] = {"x": 1}
    expect_raises(sup.GptSupervisorError, lambda: sup._validate_output(decision_id="d1", event_type="ENTRY", raw_text="{}", obj=obj))  # type: ignore[attr-defined]


def test_missing_required_fields() -> None:
    sup = _import_supervisor()
    obj: Dict[str, Any] = {"auditor": {"severity": 1}}
    expect_raises(sup.GptSupervisorError, lambda: sup._validate_output(decision_id="d1", event_type="ENTRY", raw_text="{}", obj=obj))  # type: ignore[attr-defined]


if __name__ == "__main__":
    test_validate_ok()
    test_forbidden_key_anywhere()
    test_prohibited_phrase_anywhere()
    test_unknown_top_key()
    test_missing_required_fields()
    print("OK")
