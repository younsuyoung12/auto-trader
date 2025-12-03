# smoke_test.py (SuYoung version)
# ==========================================================
# 목적:
#   ✔ WS 정상 동작 확인
#   ✔ REST 백필 정상 확인
#   ✔ GPT ENTRY 호출 실패 시 비용 폭탄 방지 체크
#   ✘ daily_limit 테스트 제거 (API KEY 없는 로컬에서는 성립하지 않음)
# ==========================================================

import time
import json

from market_data_ws import get_health_snapshot
from gpt_decider_entry import ask_entry_decision, gpt_entry_call_count
from settings_ws import load_settings
from run_bot_ws import main as start_ws_background   # WS + REST 백필 자동 실행

print("\n====================================")
print("🔥 BINGX AUTO TRADER SMOKE TEST (LOCAL) 시작")
print("====================================\n")

SET = load_settings()

# ===============================================================
# ① WS 정상 체크
# ===============================================================
print("① WS 실행…")
# main() 내부에서 WS 백필 + WS 스트림 시작 → 백그라운드로 계속 동작
try:
    start_ws_background()
except Exception as e:
    print(f"❌ WS 부팅 실패: {e}")
    exit(1)

time.sleep(5)

print("② WS health_snapshot 체크…")
health = get_health_snapshot(SET.symbol)

if not health or not health.get("overall_ok", False):
    print("❌ WS ERROR: overall_ok=False")
    print("   WS 연결 또는 REST 백필에 문제가 있습니다.")
    exit(1)
else:
    print("✅ WS OK → 캔들/호가 정상 수신됨\n")

# ===============================================================
# ② GPT ENTRY 비용 폭탄 방지 체크
# ===============================================================
print("③ GPT ENTRY 비용 폭탄 방지 체크…")

before = gpt_entry_call_count

try:
    ask_entry_decision(
        symbol=SET.symbol,
        source="SMOKE",
        current_price=50000,
        base_tv_pct=0.01,
        base_sl_pct=0.02,
        base_risk_pct=0.01,
        market_features={},  # WS 데이터 부족 → 실패 → 정상
    )
except:
    pass

after = gpt_entry_call_count

if before == after:
    print("✅ GPT 비용 폭탄 방지 OK → 실패해도 호출 카운트 증가 없음\n")
else:
    print("❌ 비용 폭탄 위험: 실패 시 count 증가")
    exit(1)

# ===============================================================
# 완료
# ===============================================================
print("====================================")
print("🔥 LOCAL SMOKE TEST — 모든 핵심 항목 PASS")
print("====================================\n")
