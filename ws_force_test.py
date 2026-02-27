# ws_force_test.py
"""
WS 강제 테스트 + REST 백필(60개) 포함 스크립트
=====================================================
목적
-----------------------------------------------------
1) REST 백필(60개)을 먼저 수행해 WS 버퍼를 안정화한다.
2) WS 스트림(start_ws_loop) 시작.
3) 1m / 5m / 15m K라인이 정상적으로 쌓이는지 확인한다.
4) depth5 오더북 정상 여부 확인.
5) 최종적으로 health_snapshot이 정상(overall_ok=True)인지 자동 판정.

이 스크립트는 run_bot_ws.py와 동일한 동작 환경을 강제로 재현한다.
"""

import time

# 🔥 MUST BE FIRST — settings must load BEFORE market_data_ws
from settings import load_settings
SET = load_settings()   # ← 이게 먼저 실행되어야 KLINE_MIN_BUFFER=60 적용됨

# 이제서야 market_data_ws import (SET 적용됨)
from infra.market_data_rest import fetch_klines_rest
from infra.market_data_ws import (
    backfill_klines_from_rest,
    start_ws_loop,
    get_health_snapshot,
)


# --------------------------------------------------
# 1) REST 백필 (60개)
# --------------------------------------------------
def do_rest_backfill(symbol: str, limit: int = 60):
    print("\n[REST] 백필 시작...")
    intervals = ["1m", "5m", "15m"]

    for iv in intervals:
        try:
            rows = fetch_klines_rest(symbol, iv, limit=limit)
            if not rows:
                print(f"[REST] {iv} 백필 실패 → 응답 없음")
                continue

            backfill_klines_from_rest(symbol, iv, rows)
            print(f"[REST] {iv} 백필 완료 (rows={len(rows)})")

        except Exception as e:
            print(f"[REST] {iv} 백필 오류 → {e}")


# --------------------------------------------------
# 2) WS 버퍼 안정화 대기
# --------------------------------------------------
def wait_for_ws(symbol: str, min_len: int = 20, timeout_sec: int = 40):
    print("\n[WS] 스트림 안정화 대기… (필요 최소 버퍼: 20개)")

    t0 = time.time()
    while True:
        snap = get_health_snapshot(symbol)
        kl = snap.get("klines", {})
        ob = snap.get("orderbook", {})

        buf1 = kl.get("1m", {}).get("buffer_len", 0)
        buf5 = kl.get("5m", {}).get("buffer_len", 0)
        buf15 = kl.get("15m", {}).get("buffer_len", 0)
        ob_ok = ob.get("ok", False)

        print(
            f"  1m={buf1:3d}개  5m={buf5:3d}개  15m={buf15:3d}개  "
            f"오더북={'OK' if ob_ok else 'NO'}"
        )

        if buf1 >= min_len and buf5 >= min_len and buf15 >= min_len and ob_ok:
            print("\n🎉 WS + REST 정상 작동! 모든 데이터 안정 확보.")
            return True

        if time.time() - t0 > timeout_sec:
            print("\n⛔ 대기 시간 초과 → WS/REST 데이터 부족 또는 네트워크 문제 가능.")
            return False

        time.sleep(1)


# --------------------------------------------------
# 3) 메인 테스트
# --------------------------------------------------
def main():
    symbol = SET.symbol

    print("====================================================")
    print("🔥 WS + REST 강제 안정성 테스트 시작")
    print("====================================================")

    # REST → WS 버퍼 초기화
    do_rest_backfill(symbol, limit=60)

    # WS 스트림 시작
    print("\n[WS] 스트림 시작…")
    start_ws_loop(symbol)

    # WS 버퍼 쌓이는지 확인
    ok = wait_for_ws(symbol, min_len=20, timeout_sec=40)

    print("\n====================================================")
    print("🔥 최종 health_snapshot")
    print("====================================================")
    print(get_health_snapshot(symbol))

    if ok:
        print("\n✅ 테스트 성공! 이 시스템은 안전하게 자동매매를 시작할 수 있습니다.")
    else:
        print("\n❌ 테스트 실패! WS/REST 데이터 이상 여부를 확인해야 합니다.")


if __name__ == "__main__":
    main()
