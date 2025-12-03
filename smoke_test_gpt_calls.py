# smoke_test_gpt_calls.py
# ==========================================
# GPT 호출 추적 스모크 테스트 (10초)
# 실제 GPT 호출 대신 mock으로 가로채서 호출 카운트/경로 추적
# ==========================================

import time
from telelog import log

import gpt_decider_entry
import gpt_decider_entry

# 기록용
GPT_CALL_LOGS = []


# -----------------------------
# 1) GPT ENTRY/EXIT 가로채기
# -----------------------------
_original_ask_entry = gpt_decider_entry.ask_entry_decision
_original_ask_exit =  gpt_decider_entry.ask_exit_decision


def _mock_ask_entry_decision(*args, **kwargs):
    t = time.time()
    GPT_CALL_LOGS.append(("ENTRY", t))
    log(f"[GPT-MOCK] ENTRY 호출됨 {t}")
    return {"action": "SKIP", "reason": "mock"}


def _mock_ask_exit_decision(*args, **kwargs):
    t = time.time()
    GPT_CALL_LOGS.append(("EXIT", t))
    log(f"[GPT-MOCK] EXIT 호출됨 {t}")
    return {"action": "HOLD", "reason": "mock"}


# Monkeypatch
gpt_decider_entry.ask_entry_decision = _mock_ask_entry_decision
gpt_decider_entry.ask_exit_decision = _mock_ask_exit_decision


# -----------------------------
# 2) 10초간 엔진 실행
# -----------------------------
def run_test():
    log("=========== GPT 호출 추적 스모크 시작 (10초) ===========")

    import run_bot_ws

    start_t = time.time()
    while time.time() - start_t < 10:
        try:
            run_bot_ws.main(loop_once=True)   # ★ loop_once 옵션 있어야 단발 실행됨
        except Exception:
            pass
        time.sleep(0.2)

    log("=========== GPT 호출 추적 스모크 종료 ===========")

    # -----------------------------
    # 3) 결과 요약
    # -----------------------------
    total = len(GPT_CALL_LOGS)
    entry_calls = len([x for x in GPT_CALL_LOGS if x[0] == "ENTRY"])
    exit_calls = len([x for x in GPT_CALL_LOGS if x[0] == "EXIT"])

    log("=========== 결과 요약 ===========")
    log(f"총 GPT 호출 수: {total}")
    log(f" - ENTRY: {entry_calls}")
    log(f" - EXIT : {exit_calls}")

    if total == 0:
        log("👉 GPT 호출 없음 → 비용 폭발 원인 아님 (정상)")
    elif total <= 3:
        log("👉 GPT 호출 적음 → 정상 범위")
    else:
        log("❌ GPT 호출 과다 → 비용 폭발 원인 확정")


if __name__ == "__main__":
    run_test()
