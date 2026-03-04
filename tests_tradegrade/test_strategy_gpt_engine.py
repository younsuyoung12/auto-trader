"""
========================================================
FILE: test_strategy_gpt_engine.py
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
  1) test_strategy_gpt_engine.py 대상 단위 테스트 추가
  2) 실패/누락/정책 위반 케이스 포함
  3) 외부 I/O(OpenAI/DB) 없이 동작하도록 monkeypatch 사용
========================================================
"""

from __future__ import annotations

import importlib
import sys
import types
from typing import Any, Dict

from tests_tradegrade._test_util import expect_raises


class _FakeSettings:
    # strategy.gpt_engine이 요구하는 필드들
    openai_api_key = "TEST_KEY"
    openai_model = "gpt-test"
    openai_max_tokens = 256
    openai_temperature = 0.2
    openai_max_latency_sec = 10.0


class _FakeMessage:
    def __init__(self, content: str):
        self.content = content


class _FakeChoice:
    def __init__(self, content: str):
        self.message = _FakeMessage(content)


class _FakeResp:
    def __init__(self, content: str):
        self.choices = [_FakeChoice(content)]


class _FakeChatCompletions:
    def __init__(self):
        self.last_kwargs: Dict[str, Any] = {}

    def create(self, **kwargs: Any) -> _FakeResp:
        # timeout 인자 전달을 검증하기 위해 저장
        self.last_kwargs = dict(kwargs)
        return _FakeResp('{"ok": true, "n": 1}')


class _FakeChat:
    def __init__(self):
        self.completions = _FakeChatCompletions()


class _FakeOpenAI:
    def __init__(self, api_key: str):
        if not isinstance(api_key, str) or not api_key.strip():
            raise RuntimeError("api_key must be non-empty (STRICT TEST)")
        self.api_key = api_key
        self.chat = _FakeChat()


def _import_gpt_engine() -> Any:
    """
    STRICT:
    - settings.load_settings를 테스트용으로 고정한 뒤 모듈을 reload 한다.
    """
    import settings  # 프로젝트 settings.py

    # monkeypatch: load_settings -> FakeSettings
    orig = getattr(settings, "load_settings")
    settings.load_settings = lambda: _FakeSettings()  # type: ignore[assignment]

    try:
        # strategy.gpt_engine reload
        if "strategy.gpt_engine" in sys.modules:
            del sys.modules["strategy.gpt_engine"]
        mod = importlib.import_module("strategy.gpt_engine")

        # OpenAI 교체(네트워크 차단)
        mod.OpenAI = _FakeOpenAI  # type: ignore[attr-defined]
        # SET 재주입(이미 import 시점에 load_settings가 호출될 수 있음)
        mod.SET = _FakeSettings()  # type: ignore[attr-defined]
        # client 캐시 초기화
        mod._CLIENT = None  # type: ignore[attr-defined]
        return mod
    finally:
        settings.load_settings = orig  # type: ignore[assignment]


def test_extract_first_json_object() -> None:
    m = _import_gpt_engine()

    # 전체가 JSON
    obj = m._extract_first_json_object('{"a": 1}')  # type: ignore[attr-defined]
    assert obj["a"] == 1

    # 앞뒤 텍스트 + JSON
    obj2 = m._extract_first_json_object("note: ok\n{\"b\": 2}\nthanks")  # type: ignore[attr-defined]
    assert obj2["b"] == 2

    # 문자열 내부의 '{' 때문에 첫 시도 실패 → 다음 JSON을 찾아야 함
    txt = "bad { not_json } ... then {\"c\": 3} end"
    obj3 = m._extract_first_json_object(txt)  # type: ignore[attr-defined]
    assert obj3["c"] == 3

    # JSON root가 object가 아니면 실패
    expect_raises(m.GptEngineError, lambda: m._extract_first_json_object("[1,2,3]"))  # type: ignore[attr-defined]


def test_call_chat_json_payload_nan_rejected() -> None:
    m = _import_gpt_engine()
    # allow_nan=False → NaN 포함 payload는 즉시 예외
    payload = {"x": float("nan")}
    expect_raises(m.GptEngineError, lambda: m.call_chat_json(system_prompt="s", user_payload=payload))  # type: ignore[attr-defined]


def test_client_cached_and_timeout_passed() -> None:
    m = _import_gpt_engine()

    # client cache 동작
    c1 = m._get_client()  # type: ignore[attr-defined]
    c2 = m._get_client()  # type: ignore[attr-defined]
    assert c1 is c2

    # call_chat에서 timeout 전달 검증
    raw = m.call_chat(system_prompt="sys", user_content="hi", max_latency_sec=3.0)  # type: ignore[attr-defined]
    assert raw.text.startswith("{")
    # FakeOpenAI의 last_kwargs 확인
    last = c1.chat.completions.last_kwargs
    assert float(last.get("timeout")) == 3.0


if __name__ == "__main__":
    test_extract_first_json_object()
    test_call_chat_json_payload_nan_rejected()
    test_client_cached_and_timeout_passed()
    print("OK")
