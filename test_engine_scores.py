# test_engine_scores_ws.py
"""
WS + REST 백필 포함 엔진 점수 테스트
========================================================
목적
- REST로 필수 TF 캔들을 먼저 백필(부팅과 동일)
- WS를 시작해서 오더북/실시간 유지
- unified_features를 생성하고 engine_scores/pattern_summary 출력

STRICT · NO-FALLBACK
- 백필 실패/버퍼 부족/오더북 미준비면 즉시 예외
========================================================
"""

from __future__ import annotations

import time
import pprint

from settings import load_settings

from infra.market_data_rest import fetch_klines_rest, KlineRestError
from infra.market_data_ws import (
    backfill_klines_from_rest,
    start_ws_loop,
    get_klines_with_volume,
    get_orderbook,
)

from strategy.unified_features_builder import build_unified_features
from strategy.regime_engine import RegimeEngine


REQUIRED_TFS = ("1m", "5m", "15m", "1h", "4h")
REQUIRED_MIN = {"1m": 20, "5m": 20, "15m": 20, "1h": 60, "4h": 60}


def _require_ws_buffers(symbol: str) -> None:
    # orderbook
    ob = get_orderbook(symbol, limit=5)
    if not isinstance(ob, dict):
        raise RuntimeError("orderbook missing (non-dict/None)")
    bids = ob.get("bids")
    asks = ob.get("asks")
    if not isinstance(bids, list) or not bids:
        raise RuntimeError("orderbook bids empty")
    if not isinstance(asks, list) or not asks:
        raise RuntimeError("orderbook asks empty")

    # klines
    for tf in REQUIRED_TFS:
        need = REQUIRED_MIN[tf]
        # 내부 maxlen이 60인 경우가 많아서 limit는 max(60, need)로 둔다
        limit = max(60, need)
        buf = get_klines_with_volume(symbol, tf, limit=limit)
        if not isinstance(buf, list):
            raise RuntimeError(f"kline buffer invalid type for {tf}")
        if len(buf) < need:
            raise RuntimeError(f"kline buffer 부족: {tf} need={need} got={len(buf)}")


def _rest_backfill(symbol: str) -> None:
    # run_bot_ws와 동일하게 충분한 캔들을 당겨서 WS 버퍼에 적재
    limit = 120
    for tf in REQUIRED_TFS:
        try:
            rows = fetch_klines_rest(symbol, tf, limit=limit)
        except KlineRestError as e:
            raise RuntimeError(f"REST backfill failed: {symbol} {tf} ({e})") from e
        except Exception as e:
            raise RuntimeError(f"REST backfill failed (unexpected): {symbol} {tf} ({e})") from e

        if not rows:
            raise RuntimeError(f"REST backfill returned empty: {symbol} {tf}")

        backfill_klines_from_rest(symbol, tf, rows)


def main() -> None:
    SET = load_settings()
    symbol = str(SET.symbol).upper().strip()
    if not symbol:
        raise RuntimeError("settings.symbol is empty")

    print("=== REST BACKFILL (BOOTSTRAP) ===")
    _rest_backfill(symbol)
    print("ok")

    print("\n=== START WS LOOP (TEST) ===")
    start_ws_loop(symbol)

    # WS/오더북 준비 대기(짧게)
    timeout_sec = 20
    start = time.time()
    while True:
        try:
            _require_ws_buffers(symbol)
            break
        except Exception as e:
            if time.time() - start >= timeout_sec:
                raise RuntimeError(f"WS not ready within {timeout_sec}s: {e}") from e
            time.sleep(1)

    print("=== WS READY ===")

    print("\n=== BUILD UNIFIED FEATURES ===")
    features = build_unified_features(symbol)
    if not isinstance(features, dict) or not features:
        raise RuntimeError("build_unified_features returned empty/non-dict")

    engine_scores = features.get("engine_scores")
    if not isinstance(engine_scores, dict) or not engine_scores:
        raise RuntimeError("engine_scores missing/invalid")

    total = engine_scores.get("total")
    if not isinstance(total, dict) or "score" not in total:
        raise RuntimeError("engine_scores.total.score missing")

    total_score = float(total["score"])

    print("\n=== ENGINE SCORES ===")
    for k in ("trend_4h", "momentum_1h", "structure_15m", "timing_5m", "orderbook_micro", "total"):
        sec = engine_scores.get(k)
        if not isinstance(sec, dict) or "score" not in sec:
            raise RuntimeError(f"engine_scores.{k}.score missing")
        print(f"- {k:14s} score={float(sec['score']):.2f}")

    print(f"\nTOTAL SCORE: {total_score:.2f}")

    print("\n=== REGIME ENGINE (ABS THRESHOLDS) ===")
    reg = RegimeEngine(window_size=200, percentile_min_history=60)
    reg.update(total_score)
    d = reg.decide(total_score)
    print(f"- band={d.band} allocation={d.allocation:.2f}")

    print("\n=== PATTERN SUMMARY ===")
    pprint.pprint(features.get("pattern_summary"))

    print("\n=== DONE ===")

    print("5m atr_pct:", features["timeframes"]["5m"]["indicators"]["atr_pct"])

    print("15m range_pct:", features["timeframes"]["15m"]["range_pct"])


if __name__ == "__main__":
    main()