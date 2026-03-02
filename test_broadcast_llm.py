from strategy.gpt_decider_entry import ask_entry_decision
from strategy.gpt_decider_entry import _score_and_classify_regime, _load_regime_policy
from settings import load_settings
from openai import OpenAI
import os

def generate_broadcast_commentary(entry_result, regime_meta):
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    prompt = f"""
당신은 실시간 비트코인 방송 진행자입니다.

현재 장 상태: {regime_meta['regime']}
레짐 점수: {regime_meta['score']}
GPT 판단: {entry_result['action']}
방향: {entry_result['direction']}
위험 비율: {entry_result['effective_risk_pct']}

위 내용을 일반 시청자가 이해할 수 있도록
전문 용어를 쉽게 풀어서
3~5문장으로 설명하세요.
영어는 절대 사용하지 마세요.
"""

    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.4,
    )

    print(resp.choices[0].message.content)


def main():
    SET = load_settings()
    policy = _load_regime_policy(SET)

    market_features = {
        "trend_strength": 1.7,
        "atr_pct": 0.004,
        "range_pct": 0.006,
        "adx": 26,
        "is_low_volatility": 0,
    }

    regime_meta = _score_and_classify_regime(
        policy=policy,
        current_price=70000,
        market_features=market_features,
    )

    entry_result = ask_entry_decision(
        symbol="BTCUSDT",
        source="BROADCAST_TEST",
        current_price=70000,
        base_tv_pct=0.01,
        base_sl_pct=0.005,
        base_risk_pct=0.01,
        market_features=market_features,
        entry_score=60.0,
    )

    generate_broadcast_commentary(entry_result, regime_meta)


if __name__ == "__main__":
    main()