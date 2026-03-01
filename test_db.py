from state.db_core import engine
from sqlalchemy import text

with engine.connect() as conn:
    result = conn.execute(
        text("SELECT column_name FROM information_schema.columns WHERE table_name='bt_regime_scores'")
    )
    print([row[0] for row in result])