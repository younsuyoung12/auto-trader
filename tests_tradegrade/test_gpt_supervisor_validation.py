"""
========================================================
FILE: test_gpt_supervisor_validation.py
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
  1) gpt_supervisor 출력 계약 검증 테스트 추가(네트워크/키 불필요)
  2) 금지 키/금지 문구/스키마 위반/필수 누락 실패 케이스 포함
========================================================
"""

from __future__ import annotations

from typing import Any, Dict

from strategy import gpt_supervisor as sup


def _ok_obj() -> Dict[str, Any]:
    return {
        "auditor": {
            "severity": 1,
            "tags": ["spread_risk", "micro_distortion"],
            "rationale_short": "스프레드와 미세구조 지표가 불안정한 조합입니다.",
            "confidence_penalty": 0.7,
            "suggested_risk_multiplier": 0.6,
        },
        "narration": {
            "title": "관망 신호",
            "message": "가격 변동이 커서 신중하게 보는 흐름입니다. 무리한 판단은 피하는 게 좋습니다.",
            "tone": "caution",
        },
        "postmortem": None,
    }


def test_validate_ok() -> None:
    obj = _ok_obj()
    # 금지어 포함되면 실패해야 하므로, 여기서는 금지어 제거
    obj["narration"]["message"] = "가격 변동이 커서 신중하게 보는 흐름입니다. 급하게 판단하지 않습니다."
    r = sup._validate_output(decision_id="d1", event_type="ENTRY", raw_text="{}", obj=obj)
    assert r.auditor.severity == 1
    assert r.narration is not None


def test_forbidden_key_anywhere() -> None:
    obj = _ok_obj()
    obj["auditor"]["action"] = "ENTER"  # 금지 키
    try:
        sup._validate_output(decision_id="d1", event_type="ENTRY", raw_text="{}", obj=obj)
        raise AssertionError("expected exception")
    except sup.GptSupervisorError:
        pass


def test_prohibited_phrase_anywhere() -> None:
    obj = _ok_obj()
    obj["narration"]["message"] = "지금 사세요"  # 금지 문구
    try:
        sup._validate_output(decision_id="d1", event_type="ENTRY", raw_text="{}", obj=obj)
        raise AssertionError("expected exception")
    except sup.GptSupervisorError:
        pass


def test_unknown_top_key() -> None:
    obj = _ok_obj()
    obj["extra"] = {"x": 1}
    try:
        sup._validate_output(decision_id="d1", event_type="ENTRY", raw_text="{}", obj=obj)
        raise AssertionError("expected exception")
    except sup.GptSupervisorError:
        pass


def test_missing_required_fields() -> None:
    obj = {"auditor": {"severity": 1}}
    try:
        sup._validate_output(decision_id="d1", event_type="ENTRY", raw_text="{}", obj=obj)  # type: ignore[arg-type]
        raise AssertionError("expected exception")
    except sup.GptSupervisorError:
        pass


if __name__ == "__main__":
    test_validate_ok()
    test_forbidden_key_anywhere()
    test_prohibited_phrase_anywhere()
    test_unknown_top_key()
    test_missing_required_fields()
    print("OK")