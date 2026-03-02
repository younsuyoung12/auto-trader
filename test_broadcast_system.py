"""
test_broadcast_system.py
-----------------------------------------
목적:
- 현재 자동매매 구조가 방송용으로 정상 작동하는지 확인
- Regime 계산
- ENTRY GPT 호출
- 방송용 출력

전제:
- 프로젝트 루트에 .env 파일 존재
- .env 안에 OPENAI_API_KEY 설정됨
"""

import time
from strategy.gpt_decider_entry import (
    ask_entry_decision,
    _score_and_classify_regime,
    _load_regime_policy,
)
from settings import load_settings


def simulate_market_snapshot():
    """
    방송 테스트용 가짜 시장 데이터
    """
    return {
        "trend_strength": 1.7,
        "atr_pct": 0.004,        # 정상 변동성 (HIGH_VOL 아님)
        "range_pct": 0.006,
        "adx": 26,
        "is_low_volatility": 0,
    }


def broadcast_message(regime_meta, entry_result):
    """
    방송용 출력 포맷
    """
    print("\n====================================")
    print(f"[방송] 현재 장 상태: {regime_meta['regime']} (점수: {regime_meta['score']})")
    print(f"[방송] GPT 판단: {entry_result['action']}")
    print(f"[방송] 방향: {entry_result['direction']}")
    print(f"[방송] TP: {entry_result['tp_pct']}")
    print(f"[방송] SL: {entry_result['sl_pct']}")
    print(f"[방송] 리스크: {entry_result['effective_risk_pct']}")
    print(f"[방송] 사유: {entry_result['reason']}")
    print(f"[방송] GPT 레이턴시: {entry_result.get('_meta', {}).get('latency_sec', 0)}초")
    print("====================================\n")


def main():
    print("방송 시스템 테스트 시작...")

    SET = load_settings()
    policy = _load_regime_policy(SET)

    market_features = simulate_market_snapshot()

    # 1️⃣ 레짐 계산
    regime_meta = _score_and_classify_regime(
        policy=policy,
        current_price=70000,
        market_features=market_features,
    )

    # 2️⃣ ENTRY GPT 호출
    start = time.time()
    entry_result = ask_entry_decision(
        symbol="BTCUSDT",
        source="BROADCAST_TEST",
        current_price=70000,
        base_tv_pct=0.01,
        base_sl_pct=0.005,
        base_risk_pct=0.01,
        market_features=market_features,
        entry_score=60.0,  # 충분히 높게 → GPT 호출되도록
    )
    total_latency = time.time() - start

    # 3️⃣ 방송 출력
    broadcast_message(regime_meta, entry_result)

    print(f"전체 처리 시간: {round(total_latency, 3)}초")


if __name__ == "__main__":
    main()