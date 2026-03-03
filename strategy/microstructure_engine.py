from __future__ import annotations

"""
========================================================
FILE: strategy/microstructure_engine.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
역할
- Funding / Open Interest(OI) / Long-Short Ratio(LSR) 기반 마이크로구조 지표를 수집/정규화하여
  Distortion Index(DI) 및 micro_score를 산출한다.
- 출력은 Quant Engine(결정권자)의 리스크/진입 가드/스냅샷 기록용이며,
  GPT는 이 결과를 '해석/감사'만 한다(결정권 없음).

절대 원칙 (STRICT · NO-FALLBACK)
- 외부 데이터(거래소 REST) 누락/비정상/빈 리스트/키 누락은 즉시 예외.
- 과거값이 부족하거나 표준편차 계산 불가(std=0 등) 시 명시 규칙을 적용하고,
  규칙으로도 결정 불가하면 즉시 예외.
- 캐시는 "성공한 결과"만 저장하며, 갱신 실패 시 과거 캐시로 진행하지 않는다(폴백 금지).

데이터 소스(필수)
- Funding:
  - current: /fapi/v1/premiumIndex (lastFundingRate)
  - history: /fapi/v1/fundingRate
- OI:
  - current: /fapi/v1/openInterest
  - history: /futures/data/openInterestHist
- LSR:
  - history+latest: /futures/data/globalLongShortAccountRatio
    (기본값: global account ratio; 필요 시 top account/position ratio로 확장)

지표 정의(권장)
- z-score는 로그 스케일을 사용한다(규모 차이 완화):
  - funding_z: funding_rate 자체 z-score (부호/스케일 유지)
  - oi_z     : zscore(ln(oi))
  - lsr_z    : zscore(ln(lsr))
- Distortion Index:
  - DI = w_f * funding_z + w_oi * oi_z + w_lsr * lsr_z
- micro_score_risk:
  - abs(DI) 기반 0~100 스케일 (클램프)

변경 이력
--------------------------------------------------------
- 2026-03-03:
  1) 신규 생성: Funding/OI/LSR 수집 + z-score + DI + micro_score 산출(STRICT)
========================================================
"""

import math
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from execution.exchange_api import (
    get_current_funding_rate,
    get_funding_rate_history,
    get_global_long_short_account_ratio,
    get_open_interest,
    get_open_interest_hist,
)

# -----------------------------------------------------------------------------
# Constants / Policy
# -----------------------------------------------------------------------------
_ALLOWED_PERIODS: Tuple[str, ...] = ("5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d")

# DI weights (sum=1.0)
_W_FUNDING: float = 0.40
_W_OI: float = 0.30
_W_LSR: float = 0.30

# Cache TTL (seconds). Only successful snapshots are cached.
_DEFAULT_CACHE_TTL_SEC: int = 15


class MicrostructureError(RuntimeError):
    """마이크로구조 데이터 수집/정규화 단계에서 사용하는 예외."""


@dataclass(frozen=True)
class MicrostructureSnapshot:
    ts_ms: int
    symbol: str

    funding_rate: float
    funding_z: float

    open_interest: float
    oi_z: float

    long_short_ratio: float
    lsr_z: float

    distortion_index: float
    micro_score_risk: float  # 0~100, higher = more distorted/risky

    meta: Dict[str, Any]


# In-memory cache (success-only). No stale fallback allowed.
# key: (symbol, period, lookback)
_CACHE: Dict[Tuple[str, str, int], Tuple[float, MicrostructureSnapshot]] = {}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _fail(stage: str, reason: str, exc: Optional[BaseException] = None) -> None:
    msg = f"[MICROSTRUCTURE] {stage} 실패: {reason}"
    if exc is None:
        raise MicrostructureError(msg)
    raise MicrostructureError(msg) from exc


def _require_symbol(symbol: Any) -> str:
    s = str(symbol).replace("-", "").replace("/", "").upper().strip()
    if not s:
        _fail("symbol", "symbol is empty")
    return s


def _require_period(period: Any) -> str:
    p = str(period).strip()
    if not p:
        _fail("period", "period is empty")
    if p not in _ALLOWED_PERIODS:
        _fail("period", f"unsupported period: {p!r} (allowed={list(_ALLOWED_PERIODS)})")
    return p


def _require_int(name: str, v: Any) -> int:
    try:
        iv = int(v)
    except Exception as e:
        _fail(name, f"must be int (got={v!r})", e)
    return iv


def _require_float(name: str, v: Any) -> float:
    try:
        fv = float(v)
    except Exception as e:
        _fail(name, f"must be float (got={v!r})", e)
    if not math.isfinite(fv):
        _fail(name, f"must be finite (got={fv})")
    return fv


def _require_pos_float(name: str, v: Any) -> float:
    fv = _require_float(name, v)
    if fv <= 0:
        _fail(name, f"must be > 0 (got={fv})")
    return fv


def _mean_std(values: List[float]) -> Tuple[float, float]:
    if not values:
        _fail("stats", "values is empty")
    n = len(values)
    if n < 2:
        _fail("stats", f"need >=2 values (got={n})")
    mu = sum(values) / float(n)
    var = sum((x - mu) * (x - mu) for x in values) / float(n - 1)
    if var < 0 or not math.isfinite(var):
        _fail("stats", f"invalid variance: {var}")
    sd = math.sqrt(var)
    if not math.isfinite(sd):
        _fail("stats", f"invalid std: {sd}")
    return mu, sd


def _zscore_strict(current: float, series: List[float], *, stage: str) -> float:
    mu, sd = _mean_std(series)
    # Explicit deterministic rule:
    # - If sd==0 and current==mu -> z=0
    # - else -> fail (cannot normalize)
    if sd == 0.0:
        if current == mu:
            return 0.0
        _fail(stage, f"std=0 but current!=mean (current={current}, mean={mu})")
    z = (current - mu) / sd
    if not math.isfinite(z):
        _fail(stage, f"invalid z (current={current}, mean={mu}, std={sd})")
    return z


def _zscore_log_strict(current: float, series: List[float], *, stage: str) -> float:
    # log-scale z-score for positive quantities
    cur = math.log(_require_pos_float(f"{stage}.current", current))
    ser = [math.log(_require_pos_float(f"{stage}.series[{i}]", v)) for i, v in enumerate(series)]
    return _zscore_strict(cur, ser, stage=stage)


def _extract_funding_series(symbol: str, rows: List[Dict[str, Any]], lookback: int) -> List[float]:
    if not isinstance(rows, list) or not rows:
        _fail("funding.history", "funding history is empty")
    if lookback <= 1:
        _fail("funding.history", f"lookback must be >=2 (got={lookback})")
    sliced = rows[-lookback:]
    out: List[float] = []
    for i, row in enumerate(sliced):
        if not isinstance(row, dict):
            _fail("funding.history", f"row[{i}] not dict")
        if "fundingRate" not in row:
            _fail("funding.history", f"row[{i}].fundingRate missing")
        out.append(_require_float(f"fundingRate[{i}]", row["fundingRate"]))
    if len(out) < 2:
        _fail("funding.history", f"need >=2 funding rates (got={len(out)})")
    return out


def _extract_oi_series(symbol: str, rows: List[Dict[str, Any]], lookback: int) -> List[float]:
    if not isinstance(rows, list) or not rows:
        _fail("oi.history", "open interest hist is empty")
    if lookback <= 1:
        _fail("oi.history", f"lookback must be >=2 (got={lookback})")
    sliced = rows[-lookback:]
    out: List[float] = []
    for i, row in enumerate(sliced):
        if not isinstance(row, dict):
            _fail("oi.history", f"row[{i}] not dict")
        # Binance openInterestHist typically returns: sumOpenInterest, sumOpenInterestValue, timestamp, symbol
        if "sumOpenInterest" not in row:
            _fail("oi.history", f"row[{i}].sumOpenInterest missing")
        out.append(_require_pos_float(f"sumOpenInterest[{i}]", row["sumOpenInterest"]))
    if len(out) < 2:
        _fail("oi.history", f"need >=2 OI points (got={len(out)})")
    return out


def _extract_lsr_series(symbol: str, rows: List[Dict[str, Any]], lookback: int) -> List[float]:
    if not isinstance(rows, list) or not rows:
        _fail("lsr.history", "long/short ratio hist is empty")
    if lookback <= 1:
        _fail("lsr.history", f"lookback must be >=2 (got={lookback})")
    sliced = rows[-lookback:]
    out: List[float] = []
    for i, row in enumerate(sliced):
        if not isinstance(row, dict):
            _fail("lsr.history", f"row[{i}] not dict")
        # Binance ratio endpoints typically return: longShortRatio, timestamp, symbol (strings)
        if "longShortRatio" not in row:
            _fail("lsr.history", f"row[{i}].longShortRatio missing")
        out.append(_require_pos_float(f"longShortRatio[{i}]", row["longShortRatio"]))
    if len(out) < 2:
        _fail("lsr.history", f"need >=2 LSR points (got={len(out)})")
    return out


def _compute_micro_score_risk(distortion_index: float) -> float:
    # Deterministic mapping: abs(DI) -> 0..100
    # |DI|=0 -> 0, |DI|>=4 -> 100 (clamp)
    x = abs(_require_float("distortion_index", distortion_index))
    score = (x / 4.0) * 100.0
    if score < 0:
        score = 0.0
    if score > 100.0:
        score = 100.0
    return score


def build_microstructure_snapshot(
    symbol: str,
    *,
    period: str = "5m",
    lookback: int = 30,
    cache_ttl_sec: int = _DEFAULT_CACHE_TTL_SEC,
) -> Dict[str, Any]:
    """
    마이크로구조 스냅샷(dict) 생성 (STRICT)
    - 반환 dict는 unified_features_builder에 그대로 합쳐 저장/전달 가능.
    """
    snap = build_microstructure_snapshot_obj(
        symbol,
        period=period,
        lookback=lookback,
        cache_ttl_sec=cache_ttl_sec,
    )
    return {
        "ts_ms": snap.ts_ms,
        "symbol": snap.symbol,
        "funding_rate": snap.funding_rate,
        "funding_z": snap.funding_z,
        "open_interest": snap.open_interest,
        "oi_z": snap.oi_z,
        "long_short_ratio": snap.long_short_ratio,
        "lsr_z": snap.lsr_z,
        "distortion_index": snap.distortion_index,
        "micro_score_risk": snap.micro_score_risk,
        "meta": snap.meta,
    }


def build_microstructure_snapshot_obj(
    symbol: str,
    *,
    period: str = "5m",
    lookback: int = 30,
    cache_ttl_sec: int = _DEFAULT_CACHE_TTL_SEC,
) -> MicrostructureSnapshot:
    """
    마이크로구조 스냅샷(typed) 생성 (STRICT)

    캐시 정책:
    - (symbol, period, lookback) 단위로 TTL 캐시
    - 캐시는 "성공한 결과"만 저장
    - 갱신 실패 시 과거 캐시로 계속 진행 금지(폴백 금지)
    """
    s = _require_symbol(symbol)
    p = _require_period(period)
    lb = _require_int("lookback", lookback)
    if lb < 2 or lb > 500:
        _fail("lookback", f"lookback must be in [2, 500] (got={lb})")

    ttl = _require_int("cache_ttl_sec", cache_ttl_sec)
    if ttl > 300:
        _fail("cache_ttl_sec", f"cache_ttl_sec too large (max 300, got={ttl})")

    key = (s, p, lb)
    now = time.time()
    if key in _CACHE:
        saved_at, cached = _CACHE[key]
        if (now - saved_at) <= float(ttl):
            return cached

    # --- Funding ---
    funding_current = _require_float("funding_rate", get_current_funding_rate(s))
    funding_hist_rows = get_funding_rate_history(s, limit=max(lb, 2))
    funding_series = _extract_funding_series(s, funding_hist_rows, lookback=min(lb, len(funding_hist_rows)))
    funding_z = _zscore_strict(funding_current, funding_series, stage="funding.z")

    # --- OI ---
    oi_current = _require_pos_float("open_interest", get_open_interest(s))
    oi_hist_rows = get_open_interest_hist(s, period=p, limit=max(lb, 2))
    oi_series = _extract_oi_series(s, oi_hist_rows, lookback=min(lb, len(oi_hist_rows)))
    oi_z = _zscore_log_strict(oi_current, oi_series, stage="oi.z_log")

    # --- LSR ---
    lsr_hist_rows = get_global_long_short_account_ratio(s, period=p, limit=max(lb, 2))
    lsr_series = _extract_lsr_series(s, lsr_hist_rows, lookback=min(lb, len(lsr_hist_rows)))
    long_short_ratio_current = lsr_series[-1]
    lsr_z = _zscore_log_strict(long_short_ratio_current, lsr_series, stage="lsr.z_log")

    # --- DI ---
    di = (_W_FUNDING * funding_z) + (_W_OI * oi_z) + (_W_LSR * lsr_z)
    if not math.isfinite(di):
        _fail("distortion_index", f"DI not finite (funding_z={funding_z}, oi_z={oi_z}, lsr_z={lsr_z})")

    micro_score_risk = _compute_micro_score_risk(di)

    snap = MicrostructureSnapshot(
        ts_ms=_now_ms(),
        symbol=s,
        funding_rate=funding_current,
        funding_z=funding_z,
        open_interest=oi_current,
        oi_z=oi_z,
        long_short_ratio=long_short_ratio_current,
        lsr_z=lsr_z,
        distortion_index=di,
        micro_score_risk=micro_score_risk,
        meta={
            "period": p,
            "lookback": lb,
            "weights": {"funding": _W_FUNDING, "oi": _W_OI, "lsr": _W_LSR},
            "series_lengths": {
                "funding": len(funding_series),
                "oi": len(oi_series),
                "lsr": len(lsr_series),
            },
        },
    )

    _CACHE[key] = (now, snap)
    return snap


__all__ = [
    "MicrostructureError",
    "MicrostructureSnapshot",
    "build_microstructure_snapshot",
    "build_microstructure_snapshot_obj",
]