"""
========================================================
FILE: state/engine_runtime_state.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할
- 엔진 런타임 상태를 DB에 영속 저장한다.
- 동일 5m candle(signal_ts_ms) 중복 진입 평가를 방지한다.
- 서버 재시작 후에도 마지막 평가 candle 상태를 복구한다.

핵심 원칙 (STRICT)
- DB 접근은 state.db_core.get_session()만 사용한다.
- symbol당 단일 row만 존재해야 한다.
- rollback / race condition 가능성은 명시적으로 차단한다.
- fallback / silent correction / default continue 금지.
- 입력 누락/손상/역전(ts rollback) 발생 시 즉시 예외.

테이블
--------------------------------------------------------
engine_runtime_state

symbol TEXT PRIMARY KEY
last_entry_signal_ts_ms BIGINT NOT NULL
updated_at TIMESTAMPTZ NOT NULL

변경 이력
--------------------------------------------------------
- 2026-03-09:
  1) ADD(ROOT-CAUSE): 마지막 진입 평가 signal_ts_ms 영속 저장 레이어 추가
  2) ADD(STRICT): 서버 재시작 후 bootstrap 복구 함수 추가
  3) ADD(RACE-GUARD): PostgreSQL advisory transaction lock 기반 symbol 단위 직렬화
  4) ADD(STRICT): 동일 ts 재평가 skip / 과거 ts rollback 즉시 예외
========================================================
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import text

from state.db_core import get_session

_TABLE_NAME = "engine_runtime_state"
_LOCK_NAMESPACE = "engine_runtime_state:entry_signal_gate"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _require_nonempty_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        raise RuntimeError(f"{name} is required (STRICT)")
    return s


def _normalize_symbol(symbol: Any) -> str:
    s = _require_nonempty_str(symbol, "symbol").replace("-", "").replace("/", "").upper().strip()
    if not s:
        raise RuntimeError("symbol normalized empty (STRICT)")
    return s


def _require_int_ms(v: Any, name: str) -> int:
    if v is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        raise RuntimeError(f"{name} must be int ms (bool not allowed) (STRICT)")
    try:
        iv = int(v)
    except Exception as e:
        raise RuntimeError(f"{name} must be int ms: {e} (STRICT)") from e
    if iv <= 0:
        raise RuntimeError(f"{name} must be > 0 (STRICT)")
    return iv


def _to_pg_bigint_signed_from_u64(v: int) -> int:
    if not isinstance(v, int):
        raise RuntimeError("lock key source must be int (STRICT)")
    if v < 0 or v >= (1 << 64):
        raise RuntimeError("lock key source out of uint64 range (STRICT)")
    if v >= (1 << 63):
        return v - (1 << 64)
    return v


def _advisory_lock_key_for_symbol(symbol: str) -> int:
    sym = _normalize_symbol(symbol)
    raw = f"{_LOCK_NAMESPACE}:{sym}".encode("utf-8")
    digest = hashlib.sha256(raw).digest()
    key_u64 = int.from_bytes(digest[:8], byteorder="big", signed=False)
    return _to_pg_bigint_signed_from_u64(key_u64)


def _lock_symbol_state_row_or_raise(session: Any, symbol: str) -> None:
    lock_key = _advisory_lock_key_for_symbol(symbol)
    session.execute(
        text("SELECT pg_advisory_xact_lock(:lock_key)"),
        {"lock_key": int(lock_key)},
    )


def load_last_entry_signal_ts_ms(symbol: str) -> Optional[int]:
    """
    현재 DB에 저장된 마지막 entry signal ts(ms)를 읽는다.
    row가 없으면 None 반환.
    """
    sym = _normalize_symbol(symbol)

    with get_session() as session:
        rows = (
            session.execute(
                text(
                    f"""
                    SELECT last_entry_signal_ts_ms
                    FROM {_TABLE_NAME}
                    WHERE symbol = :symbol
                    """
                ),
                {"symbol": sym},
            )
            .mappings()
            .all()
        )

    if len(rows) > 1:
        raise RuntimeError(f"{_TABLE_NAME} duplicate rows detected (STRICT): symbol={sym} n={len(rows)}")
    if not rows:
        return None

    return _require_int_ms(rows[0]["last_entry_signal_ts_ms"], "engine_runtime_state.last_entry_signal_ts_ms")


def bootstrap_last_entry_signal_ts_ms(symbol: str) -> Optional[int]:
    """
    엔진 부팅 시 마지막 entry signal ts(ms)를 복구한다.
    """
    return load_last_entry_signal_ts_ms(symbol)


def claim_entry_signal_ts_or_skip(symbol: str, signal_ts_ms: Any) -> bool:
    """
    STRICT 원자적 claim.

    반환:
    - True  -> 이번 signal_ts_ms 는 처음 처리하는 새 캔들이다. 진행 가능.
    - False -> 이미 처리한 동일 signal_ts_ms 이다. 즉시 SKIP.

    예외:
    - current_ts < persisted_ts 이면 rollback 으로 간주하고 즉시 예외.
    """
    sym = _normalize_symbol(symbol)
    current_ts = _require_int_ms(signal_ts_ms, "signal_ts_ms")
    now_utc = _utc_now()

    with get_session() as session:
        _lock_symbol_state_row_or_raise(session, sym)

        rows = (
            session.execute(
                text(
                    f"""
                    SELECT symbol, last_entry_signal_ts_ms
                    FROM {_TABLE_NAME}
                    WHERE symbol = :symbol
                    FOR UPDATE
                    """
                ),
                {"symbol": sym},
            )
            .mappings()
            .all()
        )

        if len(rows) > 1:
            raise RuntimeError(f"{_TABLE_NAME} duplicate rows detected under lock (STRICT): symbol={sym} n={len(rows)}")

        if not rows:
            session.execute(
                text(
                    f"""
                    INSERT INTO {_TABLE_NAME} (
                        symbol,
                        last_entry_signal_ts_ms,
                        updated_at
                    ) VALUES (
                        :symbol,
                        :last_entry_signal_ts_ms,
                        :updated_at
                    )
                    """
                ),
                {
                    "symbol": sym,
                    "last_entry_signal_ts_ms": int(current_ts),
                    "updated_at": now_utc,
                },
            )
            return True

        prev_ts = _require_int_ms(
            rows[0]["last_entry_signal_ts_ms"],
            "engine_runtime_state.last_entry_signal_ts_ms",
        )

        if current_ts < prev_ts:
            raise RuntimeError(
                f"entry signal ts rollback detected (STRICT): symbol={sym} prev={prev_ts} now={current_ts}"
            )

        if current_ts == prev_ts:
            return False

        session.execute(
            text(
                f"""
                UPDATE {_TABLE_NAME}
                SET
                    last_entry_signal_ts_ms = :last_entry_signal_ts_ms,
                    updated_at = :updated_at
                WHERE symbol = :symbol
                """
            ),
            {
                "symbol": sym,
                "last_entry_signal_ts_ms": int(current_ts),
                "updated_at": now_utc,
            },
        )
        return True


def force_set_last_entry_signal_ts_ms(symbol: str, signal_ts_ms: Any) -> None:
    """
    운영/복구용 강제 저장.
    STRICT:
    - 과거 ts로 덮어쓰기 금지.
    """
    sym = _normalize_symbol(symbol)
    current_ts = _require_int_ms(signal_ts_ms, "signal_ts_ms")
    now_utc = _utc_now()

    with get_session() as session:
        _lock_symbol_state_row_or_raise(session, sym)

        rows = (
            session.execute(
                text(
                    f"""
                    SELECT last_entry_signal_ts_ms
                    FROM {_TABLE_NAME}
                    WHERE symbol = :symbol
                    FOR UPDATE
                    """
                ),
                {"symbol": sym},
            )
            .mappings()
            .all()
        )

        if len(rows) > 1:
            raise RuntimeError(f"{_TABLE_NAME} duplicate rows detected under lock (STRICT): symbol={sym} n={len(rows)}")

        if not rows:
            session.execute(
                text(
                    f"""
                    INSERT INTO {_TABLE_NAME} (
                        symbol,
                        last_entry_signal_ts_ms,
                        updated_at
                    ) VALUES (
                        :symbol,
                        :last_entry_signal_ts_ms,
                        :updated_at
                    )
                    """
                ),
                {
                    "symbol": sym,
                    "last_entry_signal_ts_ms": int(current_ts),
                    "updated_at": now_utc,
                },
            )
            return

        prev_ts = _require_int_ms(
            rows[0]["last_entry_signal_ts_ms"],
            "engine_runtime_state.last_entry_signal_ts_ms",
        )
        if current_ts < prev_ts:
            raise RuntimeError(
                f"force_set_last_entry_signal_ts_ms rollback denied (STRICT): symbol={sym} prev={prev_ts} now={current_ts}"
            )

        session.execute(
            text(
                f"""
                UPDATE {_TABLE_NAME}
                SET
                    last_entry_signal_ts_ms = :last_entry_signal_ts_ms,
                    updated_at = :updated_at
                WHERE symbol = :symbol
                """
            ),
            {
                "symbol": sym,
                "last_entry_signal_ts_ms": int(current_ts),
                "updated_at": now_utc,
            },
        )


__all__ = [
    "load_last_entry_signal_ts_ms",
    "bootstrap_last_entry_signal_ts_ms",
    "claim_entry_signal_ts_or_skip",
    "force_set_last_entry_signal_ts_ms",
]