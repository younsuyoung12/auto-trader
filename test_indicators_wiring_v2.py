from __future__ import annotations

"""
test_indicators_wiring_v2.py
STRICT · NO-FALLBACK · INDICATORS WIRING TEST (TF_CONFIG-aware)
========================================================
목표:
- REST 백필 → WS 시작/헬스OK → build_entry_features_ws
- market_features_ws.TF_CONFIG(타임프레임별 EMA/RSI/ATR 길이) 그대로 사용해
  indicators.py로 재계산한 값과 build_entry_features_ws 산출값을 비교한다.
- build_unified_features → engine_scores.total.score → regime_engine decide까지 연결 확인.

특징:
- "실제 파이프라인 설정"을 그대로 사용한다(1m=9/21 등).
- mismatch 발생 시 값/차이를 상세 출력하고 즉시 실패한다.
========================================================
"""

import math
import os
import time
from typing import Any, Dict, List, Tuple


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    return int(v)


def _require(cond: bool, msg: str) -> None:
    if not cond:
        raise RuntimeError(msg)


def _isfinite(x: Any) -> bool:
    try:
        return isinstance(x, (int, float)) and math.isfinite(float(x))
    except Exception:
        return False


def _almost(a: float, b: float, *, rel: float = 1e-4, abs_tol: float = 1e-4) -> bool:
    return abs(a - b) <= max(abs_tol, rel * max(abs(a), abs(b), 1.0))


def _print_h(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def _safe_symbol(s: str) -> str:
    out = (s or "").strip().upper().replace("-", "").replace("/", "").replace("_", "")
    _require(bool(out), "symbol is empty")
    return out


def _backfill_bootstrap(symbol: str, intervals: List[str], limit: int) -> None:
    from infra.market_data_rest import fetch_klines_rest
    from infra.market_data_ws import backfill_klines_from_rest, get_kline_buffer_status

    _print_h("STEP 1) REST BACKFILL (BOOTSTRAP)")
    for iv in intervals:
        rows = fetch_klines_rest(symbol, iv, limit=limit)
        _require(isinstance(rows, list) and len(rows) > 0, f"REST rows empty: {symbol} {iv}")
        backfill_klines_from_rest(symbol, iv, rows)

        st = get_kline_buffer_status(symbol, iv)
        buf_len = int(st.get("buffer_len") or 0)
        print(f"- {iv}: buf_len={buf_len}")
        _require(buf_len > 0, f"WS buffer not filled: {iv}")


def _start_ws(symbol: str) -> None:
    from infra.market_data_ws import start_ws_loop

    _print_h("STEP 2) START WS LOOP")
    start_ws_loop(symbol)
    print("- ws loop started")


def _wait_health_ok(symbol: str, timeout_sec: int) -> None:
    from infra.market_data_ws import get_health_snapshot

    _print_h(f"STEP 3) WAIT WS HEALTH OK (timeout={timeout_sec}s)")
    deadline = time.time() + float(timeout_sec)

    while time.time() < deadline:
        snap = get_health_snapshot(symbol)
        if bool(snap.get("overall_ok", False)):
            print("- overall_ok=True")
            return
        time.sleep(1.5)

    raise RuntimeError("WS health not OK within timeout")


def _extract_candles_raw(tf: str, tf_data: Dict[str, Any], min_len: int) -> List[Tuple[int, float, float, float, float, float]]:
    cr = tf_data.get("candles_raw")
    _require(isinstance(cr, list) and len(cr) >= min_len, f"{tf}: candles_raw missing/too short (<{min_len})")

    out: List[Tuple[int, float, float, float, float, float]] = []
    for row in cr:
        _require(isinstance(row, (list, tuple)) and len(row) >= 6, f"{tf}: candles_raw row invalid")
        ts, o, h, l, c, v = row[:6]
        out.append((int(ts), float(o), float(h), float(l), float(c), float(v)))
    return out


def _to_candle5(candles_raw: List[Tuple[int, float, float, float, float, float]]) -> List[Tuple[int, float, float, float, float]]:
    # indicators.py Candle: (ts, open, high, low, close)
    return [(ts, o, h, l, c) for (ts, o, h, l, c, _v) in candles_raw]


def _fail_mismatch(tf: str, name: str, got: float, exp: float) -> None:
    diff = got - exp
    print(f"[MISMATCH] {tf}.{name}")
    print(f"  got={got:.10f}")
    print(f"  exp={exp:.10f}")
    print(f"  diff={diff:.10f}")
    raise RuntimeError(f"{tf}: {name} mismatch")


def _verify_indicators_match(tf: str, tf_data: Dict[str, Any], cfg: Dict[str, int]) -> None:
    """
    TF_CONFIG를 그대로 사용하여 indicators.py로 재계산하고,
    market_features_ws가 만든 indicators와 비교한다.
    """
    from strategy import indicators as ind

    indicators = tf_data.get("indicators")
    _require(isinstance(indicators, dict) and indicators, f"{tf}: indicators missing/empty")

    ema_fast_len = int(cfg.get("ema_fast", 20))
    ema_slow_len = int(cfg.get("ema_slow", 50))
    rsi_len = int(cfg.get("rsi", 14))
    atr_len = int(cfg.get("atr", 14))

    # market_features_ws는 need_len을 따로 잡지만, 여기서는 충분히 긴 candles_raw만 확보하면 된다.
    need_min = max(ema_slow_len + 5, rsi_len + 5, atr_len + 5, 60)
    candles_raw = _extract_candles_raw(tf, tf_data, min_len=need_min)

    closes = [c for (_ts, _o, _h, _l, c, _v) in candles_raw]
    candles5 = _to_candle5(candles_raw)

    # 재계산
    ema_fast_list = ind.ema(closes, ema_fast_len)
    ema_slow_list = ind.ema(closes, ema_slow_len)
    rsi_list = ind.rsi(closes, rsi_len)

    _require(isinstance(ema_fast_list, list) and len(ema_fast_list) > 0, f"{tf}: ema_fast calc failed")
    _require(isinstance(ema_slow_list, list) and len(ema_slow_list) > 0, f"{tf}: ema_slow calc failed")
    _require(isinstance(rsi_list, list) and len(rsi_list) > 0, f"{tf}: rsi calc failed")

    ema_fast_last = float(ema_fast_list[-1])
    ema_slow_last = float(ema_slow_list[-1])
    rsi_last = float(rsi_list[-1])

    # market_features_ws와 동일하게 (atr_len+1) 구간으로 ATR
    atr_val = ind.calc_atr(candles5[-(atr_len + 1):], length=atr_len)
    _require(atr_val is not None and _isfinite(atr_val), f"{tf}: atr calc returned None/NaN")
    atr_val_f = float(atr_val)

    last_close = float(closes[-1])
    _require(last_close > 0, f"{tf}: last_close<=0")
    atr_pct_ratio = atr_val_f / last_close  # ratio(0~1)

    def _get_num(key: str) -> float:
        v = indicators.get(key)
        _require(_isfinite(v), f"{tf}: indicators.{key} missing/NaN")
        return float(v)

    got_ema_fast = _get_num("ema_fast")
    got_ema_slow = _get_num("ema_slow")
    got_rsi = _get_num("rsi")
    got_atr = _get_num("atr")
    got_atr_pct = _get_num("atr_pct")

    # 비교(EMA는 누적/워밍업에 민감하므로 허용 오차를 조금 넓게)
    if not _almost(got_ema_fast, ema_fast_last, rel=5e-4, abs_tol=5e-2):
        _fail_mismatch(tf, "ema_fast", got_ema_fast, ema_fast_last)
    if not _almost(got_ema_slow, ema_slow_last, rel=5e-4, abs_tol=5e-2):
        _fail_mismatch(tf, "ema_slow", got_ema_slow, ema_slow_last)
    if not _almost(got_rsi, rsi_last, rel=5e-4, abs_tol=5e-2):
        _fail_mismatch(tf, "rsi", got_rsi, rsi_last)
    if not _almost(got_atr, atr_val_f, rel=1e-3, abs_tol=1e-2):
        _fail_mismatch(tf, "atr", got_atr, atr_val_f)
    if not _almost(got_atr_pct, atr_pct_ratio, rel=1e-2, abs_tol=1e-4):
        _fail_mismatch(tf, "atr_pct", got_atr_pct, atr_pct_ratio)

    print(f"- {tf}: indicators wiring OK (ema={ema_fast_len}/{ema_slow_len}, rsi={rsi_len}, atr={atr_len})")


def main() -> None:
    from settings import load_settings
    from infra.market_features_ws import build_entry_features_ws, TF_CONFIG
    from strategy.unified_features_builder import build_unified_features
    from strategy.regime_engine import RegimeEngine

    SET = load_settings()
    symbol = _safe_symbol(os.getenv("SYMBOL") or getattr(SET, "symbol", "BTCUSDT"))

    timeout_sec = _env_int("TEST_TIMEOUT_SEC", 60)
    backfill_limit = _env_int("TEST_BACKFILL_LIMIT", 120)
    intervals = ["1m", "5m", "15m", "1h", "4h"]

    _print_h("CONFIG")
    print(f"- symbol={symbol}")
    print(f"- backfill_intervals={intervals}")
    print(f"- timeout_sec={timeout_sec}")
    print(f"- backfill_limit={backfill_limit}")
    print(f"- TF_CONFIG keys={list(TF_CONFIG.keys())}")

    _backfill_bootstrap(symbol, intervals, limit=backfill_limit)
    _start_ws(symbol)
    _wait_health_ok(symbol, timeout_sec)

    _print_h("STEP 4) BUILD ENTRY FEATURES + VERIFY INDICATORS WIRING (TF_CONFIG)")
    feats = build_entry_features_ws(symbol)
    _require(isinstance(feats, dict) and feats, "build_entry_features_ws returned empty/non-dict")

    tfs = feats.get("timeframes")
    _require(isinstance(tfs, dict) and tfs, "features.timeframes missing/empty")

    for tf in ("1m", "5m", "15m", "1h", "4h"):
        tf_data = tfs.get(tf)
        _require(isinstance(tf_data, dict) and tf_data, f"timeframes[{tf}] missing/empty")
        cfg = TF_CONFIG.get(tf)
        _require(isinstance(cfg, dict) and cfg, f"TF_CONFIG missing for {tf}")
        _verify_indicators_match(tf, tf_data, cfg)

    _print_h("STEP 5) BUILD UNIFIED FEATURES (engine_scores) + REGIME")
    uf = build_unified_features(symbol)
    _require(isinstance(uf, dict) and uf, "build_unified_features returned empty/non-dict")

    es = uf.get("engine_scores") or {}
    total = (es.get("total") or {})
    score = total.get("score")
    _require(_isfinite(score), "engine_scores.total.score missing/NaN")
    score_f = float(score)
    _require(0.0 <= score_f <= 100.0, f"engine_scores.total.score out of range: {score_f}")
    print(f"- engine_total_score={score_f:.2f}")

    reg = RegimeEngine(window_size=200, percentile_min_history=60)
    reg.update(score_f)
    d = reg.decide(score_f)
    print(f"- regime: band={d.band} allocation={d.allocation:.2f}")

    _print_h("RESULT")
    print("✅ OK: indicators.py → market_features_ws → unified_features_builder → regime_engine 연결/계산 정상")


if __name__ == "__main__":
    main()