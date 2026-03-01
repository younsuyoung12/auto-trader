"""
========================================================
test_event_pipeline.py
PIPELINE INTEGRATION TEST
========================================================

목적:
- signals_logger → event_bus → bt_events DB 저장이
  실제로 정상 동작하는지 검증

검증 항목:
1) log_event() 호출
2) bt_events에 row INSERT 확인
3) 실패 시 즉시 예외 발생
========================================================
"""

from datetime import datetime, timezone
from sqlalchemy import text

from events.signals_logger import log_event
from state.db_core import get_session


def main():
    print("=== EVENT PIPELINE TEST START ===")

    # 1️⃣ 테스트 이벤트 기록
    log_event(
        event_type="TEST_PIPELINE",
        symbol="BTCUSDT",
        regime="TEST",
        source="unit_test",
        side="LONG",
        price=60000.0,
        qty=0.001,
        leverage=5,
        tp_pct=0.01,
        sl_pct=0.005,
        risk_pct=0.02,
        reason="PIPELINE_TEST",
        extra_json={"test": True},
    )

    print("log_event() executed")

    # 2️⃣ DB 확인
    with get_session() as session:
        result = session.execute(
            text("""
                SELECT id, event_type, symbol, side, reason
                FROM bt_events
                WHERE event_type = 'TEST_PIPELINE'
                ORDER BY id DESC
                LIMIT 1
            """)
        ).fetchone()

        if result is None:
            raise RuntimeError("❌ bt_events INSERT FAILED")

        print("DB row inserted:")
        print(result)

    print("=== EVENT PIPELINE TEST SUCCESS ===")


if __name__ == "__main__":
    main()