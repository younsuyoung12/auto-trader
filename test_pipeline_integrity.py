from __future__ import annotations

"""
test_pipeline_integrity.py
========================================================
STRICT · NO-FALLBACK · PIPELINE INTEGRITY TEST
========================================================
목표:
- REST 백필(부팅용) → WS 수신 → health OK → market_features_ws → unified_features_builder
  → engine_scores → regime_engine 까지 "연결/계산"이 끊기지 않는지 검증한다.

검증 항목:
1) REST /fapi/v1/klines 백필이 공통 포맷으로 정상 동작
2) WS(kline + depth5) 루프가 실제 버퍼를 채우고 health_snapshot overall_ok=True 도달
3) build_entry_features_ws(symbol) 성공
4) build_unified_features(symbol) 성공 + engine_scores.total.score 산출
5) RegimeEngine(decide) band/allocation 산출

운영 원칙:
- 데이터 부족/형식 오류/NaN/None은 즉시 예외로 실패한다.
- DB 저장 테스트는 기본 OFF (원하면 TEST_DB_WRITE=1).
========================================================
"""

import os
import sys
import time
import traceback
from typing import Any, Dict, List, Tuple


def _now() -> float:
    return time.time()


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    return int(v)


def _env_bool(name: str, default: str = "0") -> bool:
    v = os.getenv(name, default)
    return str(v).strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _print_header(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def _require(cond: bool, msg: str) -> None:
    if not cond:
        raise RuntimeError(msg)


def _safe_symbol(sym: str) -> str:
    s = (sym or "").strip().upper().replace("-", "").replace("/", "").replace("_", "")
    _require(bool(s), "symbol is empty")
    return s


def _backfill_bootstrap(symbol: str, intervals: List[str], limit: int = 120) -> None:
    """
    REST 백필 → WS 버퍼 preload (부팅용)
    """
    from infra.market_data_rest import fetch_klines_rest
    from infra.market_data_ws import backfill_klines_from_rest, get_kline_buffer_status

    _print_header("STEP 1) REST BACKFILL (BOOTSTRAP)")

    for iv in intervals:
        rows = fetch_klines_rest(symbol, iv, limit=limit)
        _require(isinstance(rows, list) and len(rows) > 0, f"REST rows empty: {symbol} {iv}")

        # WS 버퍼에 적재
        backfill_klines_from_rest(symbol, iv, rows)

        # 적재 결과 스냅샷
        st = get_kline_buffer_status(symbol, iv)
        buf_len = int(st.get("buffer_len") or 0)
        last_ts = st.get("last_ts")
        print(f"- backfill ok: {iv} buf_len={buf_len} last_ts={last_ts}")
        _require(buf_len > 0, f"WS buffer not filled after backfill: {iv}")


def _start_ws(symbol: str) -> None:
    from infra.market_data_ws import start_ws_loop

    _print_header("STEP 2) START WS LOOP")
    start_ws_loop(symbol)
    print("- ws loop started")


def _wait_ws_ready(symbol: str, timeout_sec: int) -> Dict[str, Any]:
    """
    health_snapshot overall_ok=True 될 때까지 대기
    """
    from infra.market_data_ws import get_health_snapshot

    _print_header(f"STEP 3) WAIT WS READY (timeout={timeout_sec}s)")

    deadline = _now() + float(timeout_sec)
    last_snap: Dict[str, Any] = {}

    while _now() < deadline:
        snap = get_health_snapshot(symbol)
        last_snap = snap

        overall_ok = bool(snap.get("overall_ok", False))
        if overall_ok:
            print("- WS HEALTH OK: overall_ok=True")
            return snap

        # 요약 출력(스팸 방지: 2초 간격)
        kl = snap.get("klines") or {}
        ob = snap.get("orderbook") or {}
        reasons_k = []
        for iv, st in kl.items():
            rs = st.get("reasons") or []
            if rs:
                reasons_k.append(f"{iv}:{','.join(map(str, rs))}")
        reasons_ob = ob.get("reasons") or []
        reason_line = ""
        if reasons_k:
            reason_line += " | klines=" + " ; ".join(reasons_k)
        if reasons_ob:
            reason_line += " | orderbook=" + ",".join(map(str, reasons_ob))

        print(f"- not ready yet...{reason_line}")
        time.sleep(2.0)

    raise RuntimeError(f"WS not ready within {timeout_sec}s. last_snapshot={last_snap}")


def _build_entry_features(symbol: str) -> Dict[str, Any]:
    """
    market_features_ws.build_entry_features_ws 검증
    """
    from infra.market_features_ws import build_entry_features_ws

    _print_header("STEP 4) BUILD ENTRY FEATURES (market_features_ws)")
    feats = build_entry_features_ws(symbol)
    _require(isinstance(feats, dict) and feats, "build_entry_features_ws returned empty/non-dict")

    _require("timeframes" in feats and isinstance(feats["timeframes"], dict) and feats["timeframes"], "features.timeframes missing/empty")
    _require("orderbook" in feats and isinstance(feats["orderbook"], dict) and feats["orderbook"], "features.orderbook missing/empty")
    _require("multi_timeframe" in feats and isinstance(feats["multi_timeframe"], dict) and feats["multi_timeframe"], "features.multi_timeframe missing/empty")

    # 간단 요약
    tfs = feats["timeframes"]
    print(f"- timeframes keys={list(tfs.keys())}")
    for k in ("1m", "5m", "15m", "1h", "4h"):
        if k in tfs:
            lc = tfs[k].get("last_close")
            rsi = (tfs[k].get("indicators") or {}).get("rsi")
            atrp = (tfs[k].get("indicators") or {}).get("atr_pct")
            print(f"  · {k}: last_close={lc} rsi={rsi} atr_pct={atrp}")

    ob = feats["orderbook"]
    print(f"- orderbook: best_bid={ob.get('best_bid')} best_ask={ob.get('best_ask')} spread_pct={ob.get('spread_pct')} depth_imb={ob.get('depth_imbalance')}")
    return feats


def _build_unified(symbol: str) -> Dict[str, Any]:
    """
    unified_features_builder.build_unified_features 검증
    """
    from strategy.unified_features_builder import build_unified_features

    _print_header("STEP 5) BUILD UNIFIED FEATURES (unified_features_builder)")
    uf = build_unified_features(symbol)
    _require(isinstance(uf, dict) and uf, "build_unified_features returned empty/non-dict")

    for k in ("timeframes", "orderbook", "pattern_features", "pattern_summary", "engine_scores"):
        _require(k in uf, f"unified_features missing key: {k}")

    es = uf["engine_scores"]
    _require(isinstance(es, dict) and "total" in es and isinstance(es["total"], dict), "engine_scores.total missing/invalid")
    total_score = es["total"].get("score")
    _require(isinstance(total_score, (int, float)), "engine_scores.total.score invalid")

    print("=== ENGINE SCORES ===")
    for k in ("trend_4h", "momentum_1h", "structure_15m", "timing_5m", "orderbook_micro"):
        sec = es.get(k) or {}
        sc = (sec.get("score") if isinstance(sec, dict) else None)
        print(f"- {k:14s} score={sc}")

    print(f"- total score={float(total_score):.2f}")
    ps = uf["pattern_summary"] or {}
    print(f"- pattern best={ps.get('best_pattern')} dir={ps.get('best_pattern_direction')} conf={ps.get('best_pattern_confidence')} tf={ps.get('best_timeframe')}")
    return uf


def _regime_decide(total_score: float) -> None:
    """
    regime_engine.RegimeEngine decide 검증
    """
    from strategy.regime_engine import RegimeEngine

    _print_header("STEP 6) REGIME ENGINE DECIDE")
    eng = RegimeEngine(window_size=200, percentile_min_history=60)
    eng.update(total_score)
    d = eng.decide(total_score)
    print(f"- score={d.score:.2f} band={d.band} allocation={d.allocation:.2f} history_len={d.history_len}")


def _optional_db_write_smoke(symbol: str) -> None:
    """
    선택: DB write smoke test (TEST_DB_WRITE=1일 때만)
    - market_data_store.save_candles_bulk_from_ws / save_orderbook_from_ws 호출
    - 실제 insert 확인을 위해 SessionLocal + ORM 조회를 시도
    """
    if not _env_bool("TEST_DB_WRITE", "0"):
        return

    _print_header("STEP 7) (OPTION) DB WRITE SMOKE TEST (TEST_DB_WRITE=1)")

    # DATABASE_URL / TRADER_DB_URL 존재 체크 (둘 다 없으면 실패)
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        dsn = os.getenv("TRADER_DB_URL")
    _require(bool(dsn), "DB write test requires DATABASE_URL or TRADER_DB_URL env")

    # DB 관련 모듈 import 시도
    from infra.market_data_ws import get_klines_with_volume, get_orderbook
    from infra.market_data_store import save_candles_bulk_from_ws, save_orderbook_from_ws
    from state.db_core import SessionLocal
    from state.db_models import Candle, OrderbookSnapshot

    # candle sample
    tf = "1m"
    rows = get_klines_with_volume(symbol, tf, limit=5)
    _require(bool(rows) and len(rows) >= 3, "not enough ws klines for db write test")

    candles_payload = []
    for ts_ms, o, h, l, c, v in rows:
        candles_payload.append(
            {
                "symbol": symbol,
                "interval": tf,
                "ts_ms": int(ts_ms),
                "open": float(o),
                "high": float(h),
                "low": float(l),
                "close": float(c),
                "volume": float(v),
                "quote_volume": None,
                "source": "ws",
            }
        )

    save_candles_bulk_from_ws(candles_payload)

    # orderbook sample
    ob = get_orderbook(symbol, limit=5)
    _require(isinstance(ob, dict) and ob.get("bids") and ob.get("asks"), "orderbook missing for db write test")
    ts_ms = int(ob.get("ts") or int(time.time() * 1000))
    save_orderbook_from_ws(symbol=symbol, ts_ms=ts_ms, bids=ob["bids"], asks=ob["asks"])

    # verify by query
    session = SessionLocal()
    try:
        last_candle = (
            session.query(Candle)
            .filter(Candle.symbol == symbol, Candle.timeframe == tf)
            .order_by(Candle.ts.desc())
            .first()
        )
        _require(last_candle is not None, "DB verify failed: Candle not found after insert")
        print(f"- DB verify candle ok: {last_candle.symbol} {last_candle.timeframe} ts={last_candle.ts}")

        last_ob = (
            session.query(OrderbookSnapshot)
            .filter(OrderbookSnapshot.symbol == symbol)
            .order_by(OrderbookSnapshot.ts.desc())
            .first()
        )
        _require(last_ob is not None, "DB verify failed: OrderbookSnapshot not found after insert")
        print(f"- DB verify orderbook ok: {last_ob.symbol} ts={last_ob.ts} spread={getattr(last_ob, 'spread', None)}")
    finally:
        session.close()


def main() -> None:
    # 기본 설정
    from settings import load_settings

    SET = load_settings()
    symbol = _safe_symbol(os.getenv("SYMBOL") or getattr(SET, "symbol", "BTCUSDT"))

    timeout_sec = _env_int("TEST_TIMEOUT_SEC", 60)
    backfill_limit = _env_int("TEST_BACKFILL_LIMIT", 120)

    # 필요한 TF들
    intervals = list(getattr(SET, "ws_required_tfs", None) or ["1m", "5m", "15m", "1h", "4h"])
    # 안전: entry/unified가 요구하는 1h/4h는 반드시 포함
    for tf in ("1h", "4h"):
        if tf not in intervals:
            intervals.append(tf)

    _print_header("PIPELINE TEST CONFIG")
    print(f"- symbol={symbol}")
    print(f"- intervals(backfill)={intervals}")
    print(f"- timeout_sec={timeout_sec}")
    print(f"- backfill_limit={backfill_limit}")
    print(f"- TEST_DB_WRITE={_env_bool('TEST_DB_WRITE', '0')}")

    # 1) REST backfill
    _backfill_bootstrap(symbol, intervals, limit=backfill_limit)

    # 2) WS loop start
    _start_ws(symbol)

    # 3) wait health ok
    _wait_ws_ready(symbol, timeout_sec)

    # 4) entry features
    _build_entry_features(symbol)

    # 5) unified features
    uf = _build_unified(symbol)

    # 6) regime decision (use engine_scores.total.score)
    total_score = float(((uf.get("engine_scores") or {}).get("total") or {}).get("score"))
    _regime_decide(total_score)

    # 7) optional DB write smoke
    _optional_db_write_smoke(symbol)

    _print_header("RESULT")
    print("✅ PIPELINE OK: REST → WS → FEATURES → UNIFIED → ENGINE_SCORES → REGIME")
    print("끝")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\n" + "=" * 80)
        print("❌ PIPELINE FAILED")
        print("=" * 80)
        print(f"{type(e).__name__}: {e}")
        print(traceback.format_exc())
        sys.exit(1)