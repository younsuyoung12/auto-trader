#!/usr/bin/env python3
"""
test_openai_responses_probe.py

목적
- 현재 venv에서 import 되는 OpenAI SDK가 실제로 무엇인지(경로/버전/클래스 소스파일)
- OpenAI() 인스턴스에 responses 네임스페이스가 존재하는지
- with_options(timeout=...) 적용 시 responses 네임스페이스가 사라지는지
- Responses API 호출이 실제로 되는지(콘솔 Logs → Responses에 남는지)
- GPT-5에서 output_text가 None일 때도 output[]에서 "output_text"를 STRICT 추출해 진단

STRICT
- settings.py(SSOT)에서만 키/모델을 가져온다.
- 실패하면 즉시 예외(원인 출력 후 종료).
"""

from __future__ import annotations

import inspect
import os
import platform
import sys
import traceback
from typing import Any, Optional


def _banner(title: str) -> None:
    print("\n" + "=" * 92)
    print(title)
    print("=" * 92)


def _print_kv(k: str, v: Any) -> None:
    print(f"{k:28s}: {v}")


def _safe_exc(e: BaseException) -> str:
    return f"{e.__class__.__name__}: {str(e)}"


def _get_attr(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _extract_text_from_response(resp: Any) -> Optional[str]:
    """
    GPT-5 Responses에서 output_text가 None일 수 있으므로:
    1) resp.output_text (있고 non-empty면 사용)
    2) resp.output[*].content[*].type == "output_text" 인 text를 스캔해서 추출
    """
    t = _get_attr(resp, "output_text", None)
    if isinstance(t, str) and t.strip():
        return t.strip()

    output = _get_attr(resp, "output", None)
    if not isinstance(output, list):
        return None

    for item in output:
        content = _get_attr(item, "content", None)
        if not isinstance(content, list):
            continue
        for c in content:
            ctype = _get_attr(c, "type", None)
            if ctype == "output_text":
                text = _get_attr(c, "text", None)
                if isinstance(text, str) and text.strip():
                    return text.strip()
            if ctype == "refusal":
                # refusal은 텍스트가 아니므로 None 처리(상위에서 상태 출력)
                return None

    return None


def _debug_response_meta(resp: Any) -> None:
    # 민감정보 없이 상태/불완전 사유만 출력
    status = _get_attr(resp, "status", None)
    _print_kv("resp.status", status)
    inc = _get_attr(resp, "incomplete_details", None)
    if inc is not None:
        reason = _get_attr(inc, "reason", None)
        _print_kv("resp.incomplete_reason", reason)
    usage = _get_attr(resp, "usage", None)
    if usage is not None:
        _print_kv("resp.usage", usage)


def main() -> int:
    _banner("ENV / RUNTIME")
    _print_kv("python", sys.version.replace("\n", " "))
    _print_kv("executable", sys.executable)
    _print_kv("cwd", os.getcwd())
    _print_kv("platform", platform.platform())

    _banner("LOAD SETTINGS (SSOT)")
    try:
        from settings import load_settings  # type: ignore
    except Exception as e:
        print("[FATAL] settings import failed:", _safe_exc(e))
        traceback.print_exc()
        return 2

    try:
        SET = load_settings()
    except Exception as e:
        print("[FATAL] load_settings() failed:", _safe_exc(e))
        traceback.print_exc()
        return 3

    if not hasattr(SET, "openai_api_key"):
        print("[FATAL] settings.openai_api_key missing")
        return 4
    if not hasattr(SET, "openai_model"):
        print("[FATAL] settings.openai_model missing")
        return 5

    api_key = str(getattr(SET, "openai_api_key")).strip()
    model = str(getattr(SET, "openai_model")).strip()

    if not api_key:
        print("[FATAL] settings.openai_api_key is empty")
        return 6
    if not model:
        print("[FATAL] settings.openai_model is empty")
        return 7

    budget = 12.0
    if hasattr(SET, "openai_max_latency_sec"):
        try:
            budget = float(getattr(SET, "openai_max_latency_sec"))
        except Exception:
            budget = 12.0

    _print_kv("model", model)
    _print_kv("budget_sec", budget)
    _print_kv("api_key_len", len(api_key))
    _print_kv("api_key_prefix", api_key[:7] + "..." if len(api_key) >= 7 else "(short)")

    _banner("OPENAI SDK INTROSPECTION")
    try:
        import openai  # type: ignore
        from openai import OpenAI  # type: ignore
    except Exception as e:
        print("[FATAL] import openai/OpenAI failed:", _safe_exc(e))
        traceback.print_exc()
        return 8

    _print_kv("openai.__version__", getattr(openai, "__version__", "(missing)"))
    _print_kv("openai.__file__", getattr(openai, "__file__", "(missing)"))
    _print_kv("OpenAI.__module__", getattr(OpenAI, "__module__", "(missing)"))
    try:
        _print_kv("OpenAI source file", inspect.getsourcefile(OpenAI))
    except Exception as e:
        _print_kv("OpenAI source file", f"(unavailable) {_safe_exc(e)}")

    _banner("CLIENT OBJECT CHECK")
    try:
        client = OpenAI(api_key=api_key)
    except Exception as e:
        print("[FATAL] OpenAI(api_key=...) failed:", _safe_exc(e))
        traceback.print_exc()
        return 9

    _print_kv("client type", type(client))
    _print_kv("hasattr(client,'responses')", hasattr(client, "responses"))
    _print_kv("hasattr(client,'chat')", hasattr(client, "chat"))
    _print_kv("hasattr(client,'with_options')", hasattr(client, "with_options"))

    if not hasattr(client, "responses"):
        print("\n[FATAL] client.responses 가 없습니다.")
        print("1) openai 모듈 shadowing (프로젝트 내 openai.py / openai/ 폴더 등)")
        print("2) OpenAI 클래스가 다른 경로/버전에서 로드됨")
        d = [x for x in dir(client) if not x.startswith("_")]
        print("\n[DEBUG] client public attrs sample:", d[:60])
        return 10

    _banner("RESPONSES API CALL (should appear in Console → Logs → Responses)")
    try:
        resp = client.responses.create(
            model=model,
            instructions="Reply with the single word: pong",
            input="ping",
            max_output_tokens=64,
            store=True,
        )

        out_text = _extract_text_from_response(resp)

        _print_kv("resp type", type(resp))
        _print_kv("hasattr(resp,'output_text')", hasattr(resp, "output_text"))
        _print_kv("resp.output_text", getattr(resp, "output_text", None))

        if isinstance(out_text, str):
            _print_kv("extracted_text", out_text)
        else:
            _print_kv("extracted_text", None)

        if not isinstance(out_text, str) or not out_text.strip():
            _debug_response_meta(resp)
            print("[FATAL] response text empty (output_text may be None on GPT-5; output scan also empty)")
            return 11

    except Exception as e:
        print("[FATAL] client.responses.create failed:", _safe_exc(e))
        traceback.print_exc()
        return 12

    _banner("WITH_OPTIONS CHECK (timeout 적용 시 responses namespace 유지 여부)")
    if hasattr(client, "with_options"):
        try:
            client2 = client.with_options(timeout=float(budget))
            _print_kv("client2 type", type(client2))
            _print_kv("hasattr(client2,'responses')", hasattr(client2, "responses"))
            _print_kv("hasattr(client2,'chat')", hasattr(client2, "chat"))

            if hasattr(client2, "responses"):
                resp2 = client2.responses.create(
                    model=model,
                    instructions="Reply with the single word: pong",
                    input="ping2",
                    max_output_tokens=128,
                    store=True,
                )
                out_text2 = _extract_text_from_response(resp2)
                _print_kv("client2 resp.output_text", getattr(resp2, "output_text", None))
                _print_kv("client2 extracted_text", out_text2)
                if not isinstance(out_text2, str) or not out_text2.strip():
                    _debug_response_meta(resp2)
                    print("[FATAL] client2 response text empty")
                    return 13
            else:
                print("\n[RESULT] with_options(timeout=...) 적용 후 client2.responses 가 사라졌습니다.")
                print("=> preflight에서 AttributeError가 나면 이 케이스입니다.")
        except Exception as e:
            print("[FATAL] with_options path failed:", _safe_exc(e))
            traceback.print_exc()
            return 14
    else:
        print("[SKIP] client.with_options 가 없습니다(이 케이스는 드뭅니다).")

    _banner("DONE")
    print("OK: Responses API 호출 성공. Console → Logs → Responses 에 기록이 남아야 합니다.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())