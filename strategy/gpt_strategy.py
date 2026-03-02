"""
========================================================
strategy/gpt_strategy.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
설계 원칙:
- GPT 판단(진입 여부 / allocation_ratio/tp/sl / guard_adjustments)만 수행한다.
- 주문 실행 / 거래소 API 호출 / DB 접근 절대 금지.
- unified_features + engine_scores는 필수 입력.
- 데이터 누락/오류는 즉시 예외.
- 폴백(REST 백필/더미 값/임의 보정) 절대 금지.

PATCH NOTES — 2026-03-02
- market_data.account_state 필수 입력으로 강제(STRICT).
  - dd_pct / consecutive_losses / recent_win_rate / recent_trades_count 필수.
- account_state를 GPT 입력(extra_to_gpt)에 포함하여 의사결정 컨텍스트 강화.
  (I/O 없음, 추정 없음, caller가 제공한 실제 데이터만 전달)

PATCH NOTES — 2026-03-02 (PATCH)
- 용어/의미 통일:
  - 내부 변수명: allocation_ratio(0~1)로 통일.
  - 설정/입력 호환:
    - settings.allocation_ratio 우선, 없으면 settings.risk_pct 허용(전환 기간 호환).
    - extra.effective_allocation_ratio 우선, 없으면 extra.effective_risk_pct 허용.
    - GPT 결과: effective_allocation_ratio 우선, 없으면 effective_risk_pct 허용.
  - Signal 필드명(risk_pct)은 유지할 수 있으나, 의미는 allocation_ratio로 고정.
- 사용하지 않는 import 정리.
========================================================
"""

from __future__ import annotations

import logging
import math
from typing import Any, Dict, Optional, Tuple

from strategy.base_strategy import BaseStrategy
from strategy.gpt_trader import decide_entry_with_gpt_trader
from strategy.signal import Signal

logger = logging.getLogger(__name__)

_REQUIRED_ENGINE_SCORE_KEYS: Tuple[str, ...] = (
    "trend_4h",
    "momentum_1h",
    "structure_15m",
    "timing_5m",
    "orderbook_micro",
    "total",
)

_REQUIRED_MARKET_FEATURE_KEYS: Tuple[str, ...] = (
    "symbol",
    "timeframes",
    "orderbook",
    "multi_timeframe",
    "pattern_summary",
    "pattern_features",
    "engine_scores",
)

_REQUIRED_ACCOUNT_STATE_KEYS: Tuple[str, ...] = (
    "dd_pct",
    "consecutive_losses",
    "recent_win_rate",
    "recent_trades_count",
)


def _as_float(
    value: Any,
    name: str,
    *,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> float:
    try:
        if isinstance(value, bool):
            raise TypeError("bool is not allowed")
        v = float(value)
    except Exception as e:
        raise ValueError(f"{name} must be a number") from e

    if not math.isfinite(v):
        raise ValueError(f"{name} must be finite")

    if min_value is not None and v < min_value:
        raise ValueError(f"{name} must be >= {min_value}")
    if max_value is not None and v > max_value:
        raise ValueError(f"{name} must be <= {max_value}")

    return v


def _as_int(value: Any, name: str, *, min_value: Optional[int] = None) -> int:
    try:
        if isinstance(value, bool):
            raise TypeError("bool is not allowed")
        v = int(value)
    except Exception as e:
        raise ValueError(f"{name} must be an integer") from e

    if min_value is not None and v < min_value:
        raise ValueError(f"{name} must be >= {min_value}")

    return v


def _require_non_empty_str(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"{name} is required and must be non-empty str")
    return value.strip()


def _require_dict(value: Any, name: str) -> Dict[str, Any]:
    if not isinstance(value, dict) or not value:
        raise RuntimeError(f"{name} is required and must be non-empty dict")
    return value


def _require_account_state(value: Any) -> Dict[str, Any]:
    """
    STRICT:
    - account_state는 반드시 dict
    - 필수 키 존재
    - 각 값의 타입/범위 강제
    """
    st = _require_dict(value, "market_data.account_state")

    for k in _REQUIRED_ACCOUNT_STATE_KEYS:
        if k not in st:
            raise RuntimeError(f"account_state missing required key: {k}")

    dd_pct = _as_float(st.get("dd_pct"), "account_state.dd_pct", min_value=0.0, max_value=100.0)
    consecutive_losses = _as_int(st.get("consecutive_losses"), "account_state.consecutive_losses", min_value=0)
    recent_win_rate = _as_float(st.get("recent_win_rate"), "account_state.recent_win_rate", min_value=0.0, max_value=1.0)
    recent_trades_count = _as_int(st.get("recent_trades_count"), "account_state.recent_trades_count", min_value=0)

    st2: Dict[str, Any] = dict(st)
    st2["dd_pct"] = float(dd_pct)
    st2["consecutive_losses"] = int(consecutive_losses)
    st2["recent_win_rate"] = float(recent_win_rate)
    st2["recent_trades_count"] = int(recent_trades_count)

    # optional fields (있으면 검증)
    if "equity_current_usdt" in st2 and st2["equity_current_usdt"] is not None:
        st2["equity_current_usdt"] = _as_float(st2["equity_current_usdt"], "account_state.equity_current_usdt", min_value=0.0)
    if "equity_peak_usdt" in st2 and st2["equity_peak_usdt"] is not None:
        st2["equity_peak_usdt"] = _as_float(st2["equity_peak_usdt"], "account_state.equity_peak_usdt", min_value=0.0)

    if "recent_planned_rr_avg" in st2 and st2["recent_planned_rr_avg"] is not None:
        st2["recent_planned_rr_avg"] = _as_float(st2["recent_planned_rr_avg"], "account_state.recent_planned_rr_avg", min_value=0.0)

    return st2


def _compute_pre_entry_score(extra: Any) -> Optional[float]:
    """
    EntryScore(0~100) 프리뷰.
    STRICT:
    - 값 추정/폴백 금지. (존재할 때만 사용)
    """
    if not isinstance(extra, dict):
        return None

    raw = extra.get("signal_score")
    if raw is None:
        raw = extra.get("candidate_score")

    if not isinstance(raw, (int, float)) or not math.isfinite(float(raw)):
        return None

    v = float(raw)

    # 0~10 스케일이면 0~100으로 변환 (기존 관행)
    if 0.0 <= v <= 10.0:
        return v * 10.0

    # 이미 0~100이면 그대로
    if 0.0 <= v <= 100.0:
        return v

    return None


def _build_guard_snapshot(settings: Any) -> Dict[str, float]:
    snap: Dict[str, float] = {}
    keys = [
        "min_entry_volume_ratio",
        "max_spread_pct",
        "max_price_jump_pct",
        "depth_imbalance_min_ratio",
        "depth_imbalance_min_notional",
    ]
    for key in keys:
        if not hasattr(settings, key):
            continue

        v = getattr(settings, key)

        if isinstance(v, bool):
            raise ValueError(f"guard setting '{key}' must be numeric, not bool")
        if not isinstance(v, (int, float)):
            raise ValueError(f"guard setting '{key}' must be numeric")

        fv = float(v)
        if not math.isfinite(fv):
            raise ValueError(f"guard setting '{key}' must be finite")

        snap[key] = fv

    return snap


def _validate_market_features_structure(market_features: Dict[str, Any]) -> None:
    for k in _REQUIRED_MARKET_FEATURE_KEYS:
        if k not in market_features:
            raise RuntimeError(f"market_features missing required key: {k}")

    _require_non_empty_str(market_features.get("symbol"), "market_features.symbol")

    _require_dict(market_features.get("timeframes"), "market_features.timeframes")
    _require_dict(market_features.get("orderbook"), "market_features.orderbook")
    _require_dict(market_features.get("multi_timeframe"), "market_features.multi_timeframe")
    _require_dict(market_features.get("pattern_summary"), "market_features.pattern_summary")
    _require_dict(market_features.get("pattern_features"), "market_features.pattern_features")

    _require_dict(market_features.get("engine_scores"), "market_features.engine_scores")


def _extract_and_validate_engine_scores(market_features: Dict[str, Any]) -> Tuple[Dict[str, Any], float]:
    engine_scores = _require_dict(market_features.get("engine_scores"), "market_features.engine_scores")

    for k in _REQUIRED_ENGINE_SCORE_KEYS:
        if k not in engine_scores:
            raise RuntimeError(f"engine_scores missing required key: {k}")

    for section_key in ("trend_4h", "momentum_1h", "structure_15m", "timing_5m", "orderbook_micro", "total"):
        section = engine_scores.get(section_key)
        if not isinstance(section, dict) or not section:
            raise RuntimeError(f"engine_scores.{section_key} must be non-empty dict")

        if "score" not in section:
            raise RuntimeError(f"engine_scores.{section_key} missing required key: score")

        _as_float(section.get("score"), f"engine_scores.{section_key}.score", min_value=0.0, max_value=100.0)

        if section_key == "trend_4h":
            if "direction" in section:
                d = str(section.get("direction", "")).upper().strip()
                if d not in ("LONG", "SHORT", "NEUTRAL"):
                    raise RuntimeError("engine_scores.trend_4h.direction must be LONG|SHORT|NEUTRAL if present")

    total = engine_scores["total"]
    total_score = _as_float(total.get("score"), "engine_scores.total.score", min_value=0.0, max_value=100.0)
    return engine_scores, total_score


def _get_setting_allocation_ratio(settings: Any) -> float:
    """
    settings에서 allocation_ratio(0~1)를 가져온다.
    - settings.allocation_ratio 우선
    - 없으면 settings.risk_pct 허용(전환 기간 호환)
    """
    if hasattr(settings, "allocation_ratio") and getattr(settings, "allocation_ratio") is not None:
        v = _as_float(getattr(settings, "allocation_ratio"), "settings.allocation_ratio", min_value=0.0, max_value=1.0)
        if not (0.0 < v <= 1.0):
            raise ValueError(f"settings.allocation_ratio out of range (0,1]: {v}")
        return v

    v = _as_float(getattr(settings, "risk_pct"), "settings.risk_pct(allocation_ratio)", min_value=0.0, max_value=1.0)
    if not (0.0 < v <= 1.0):
        raise ValueError(f"settings.risk_pct(out of range, expected allocation_ratio (0,1]): {v}")
    return v


def _get_extra_effective_allocation(extra: Optional[Dict[str, Any]], base: float) -> float:
    if not isinstance(extra, dict):
        return base

    if extra.get("effective_allocation_ratio") is not None:
        v = _as_float(extra.get("effective_allocation_ratio"), "extra.effective_allocation_ratio", min_value=0.0, max_value=1.0)
        if not (0.0 < v <= 1.0):
            raise ValueError(f"effective_allocation_ratio out of range (0,1]: {v}")
        return v

    if extra.get("effective_risk_pct") is not None:
        v = _as_float(extra.get("effective_risk_pct"), "extra.effective_risk_pct(allocation_ratio)", min_value=0.0, max_value=1.0)
        if not (0.0 < v <= 1.0):
            raise ValueError(f"effective_risk_pct out of range (0,1]: {v}")
        return v

    return base


def _get_gpt_effective_allocation(gpt_result: Dict[str, Any]) -> float:
    if "effective_allocation_ratio" in gpt_result:
        v = _as_float(gpt_result.get("effective_allocation_ratio"), "gpt.effective_allocation_ratio", min_value=0.0, max_value=1.0)
        if not (0.0 < v <= 1.0):
            raise ValueError(f"gpt effective_allocation_ratio out of range (0,1]: {v}")
        return v

    # 전환 기간 호환
    if "effective_risk_pct" in gpt_result:
        v = _as_float(gpt_result.get("effective_risk_pct"), "gpt.effective_risk_pct(allocation_ratio)", min_value=0.0, max_value=1.0)
        if not (0.0 < v <= 1.0):
            raise ValueError(f"gpt effective_risk_pct out of range (0,1]: {v}")
        return v

    raise RuntimeError("gpt_result missing required key: effective_allocation_ratio (or effective_risk_pct)")


class GPTStrategy(BaseStrategy):
    def __init__(self, settings: Any):
        self.settings = settings

    def decide(self, market_data: Dict[str, Any]) -> Signal:
        if not isinstance(market_data, dict) or not market_data:
            raise RuntimeError("market_data is empty or not a dict")

        symbol = _require_non_empty_str(market_data.get("symbol"), "market_data.symbol")

        direction = str(market_data.get("direction", "")).upper().strip()
        if direction not in ("LONG", "SHORT"):
            raise RuntimeError("market_data.direction must be 'LONG' or 'SHORT'")

        signal_source = _require_non_empty_str(market_data.get("signal_source"), "market_data.signal_source")
        regime = _require_non_empty_str(market_data.get("regime"), "market_data.regime")

        signal_ts_ms = market_data.get("signal_ts_ms")
        if not isinstance(signal_ts_ms, (int, float)) or not math.isfinite(float(signal_ts_ms)):
            raise RuntimeError("market_data.signal_ts_ms is required and must be finite number")
        signal_ts_ms_i = int(signal_ts_ms)
        if signal_ts_ms_i <= 0:
            raise RuntimeError("market_data.signal_ts_ms must be > 0")

        last_price = _as_float(market_data.get("last_price"), "market_data.last_price", min_value=0.0)
        if last_price <= 0:
            raise ValueError("market_data.last_price must be > 0")

        # account_state (필수)
        account_state = _require_account_state(market_data.get("account_state"))

        # unified_features (필수)
        market_features = market_data.get("market_features")
        if not isinstance(market_features, dict) or not market_features:
            raise RuntimeError("market_data.market_features (unified_features) is required and must be dict")

        _validate_market_features_structure(market_features)
        engine_scores, engine_total_score = _extract_and_validate_engine_scores(market_features)

        extra = market_data.get("extra")
        if extra is not None and not isinstance(extra, dict):
            raise RuntimeError("market_data.extra must be dict or None")

        candles_5m = market_data.get("candles_5m")
        candles_5m_raw = market_data.get("candles_5m_raw")

        # base params from settings (STRICT validation)
        base_allocation_ratio = _get_setting_allocation_ratio(self.settings)
        base_tp_pct = _as_float(getattr(self.settings, "tp_pct"), "settings.tp_pct", min_value=0.0)
        base_sl_pct = _as_float(getattr(self.settings, "sl_pct"), "settings.sl_pct", min_value=0.0)

        if not (0.0 < base_tp_pct <= 1.0):
            raise ValueError(f"settings.tp_pct out of range (0,1]: {base_tp_pct}")
        if not (0.0 < base_sl_pct <= 1.0):
            raise ValueError(f"settings.sl_pct out of range (0,1]: {base_sl_pct}")

        # extra overrides (존재할 때만 적용)
        allocation_ratio = _get_extra_effective_allocation(extra, base_allocation_ratio)
        tp_pct = base_tp_pct
        sl_pct = base_sl_pct

        if isinstance(extra, dict):
            if extra.get("tp_pct") is not None:
                tp_pct = _as_float(extra.get("tp_pct"), "extra.tp_pct", min_value=0.0)
            if extra.get("sl_pct") is not None:
                sl_pct = _as_float(extra.get("sl_pct"), "extra.sl_pct", min_value=0.0)

        if not (0.0 < allocation_ratio <= 1.0):
            raise ValueError(f"allocation_ratio out of range (0,1]: {allocation_ratio}")
        if not (0.0 < tp_pct <= 1.0):
            raise ValueError(f"tp_pct out of range (0,1]: {tp_pct}")
        if not (0.0 < sl_pct <= 1.0):
            raise ValueError(f"sl_pct out of range (0,1]: {sl_pct}")

        entry_score = _compute_pre_entry_score(extra)

        # ── B) GPT 호출 전 1차 엔진 점수 게이트 ─────────────────────────
        if hasattr(self.settings, "min_engine_total_score") and getattr(self.settings, "min_engine_total_score") is not None:
            min_engine_total_score = _as_float(
                getattr(self.settings, "min_engine_total_score"),
                "settings.min_engine_total_score",
                min_value=0.0,
                max_value=100.0,
            )

            if engine_total_score < min_engine_total_score:
                pattern_summary = _require_dict(market_features.get("pattern_summary"), "market_features.pattern_summary")
                meta: Dict[str, Any] = {
                    "symbol": symbol,
                    "signal_source": signal_source,
                    "regime": regime,
                    "signal_ts_ms": signal_ts_ms_i,
                    "last_price": float(last_price),
                    "candles_5m": candles_5m,
                    "candles_5m_raw": candles_5m_raw,
                    "extra": extra,
                    "entry_score": entry_score,
                    "account_state": account_state,
                    "market_features": market_features,
                    "engine_total_score": float(engine_total_score),
                    "engine_scores": engine_scores,
                    "pattern_summary": pattern_summary,
                    "gpt_action": "SKIPPED_BY_ENGINE_GATE",
                }

                reason = (
                    f"engine_total_score({engine_total_score:.1f}) "
                    f"< min_engine_total_score({min_engine_total_score:.1f})"
                )

                return Signal(
                    action="SKIP",
                    direction=direction,
                    tp_pct=float(tp_pct),
                    sl_pct=float(sl_pct),
                    risk_pct=float(allocation_ratio),  # 의미는 allocation_ratio
                    reason=reason,
                    guard_adjustments={},
                    meta=meta,
                )

        # guard snapshot
        guard_snapshot = _build_guard_snapshot(self.settings)

        # GPT 입력 extra 구성 (account_state 포함)
        extra_to_gpt: Dict[str, Any] = dict(extra) if isinstance(extra, dict) else {}
        extra_to_gpt["account_state"] = account_state
        extra_to_gpt["engine_total_score"] = float(engine_total_score)

        # GPT 판단
        try:
            # NOTE: decide_entry_with_gpt_trader의 인자명은 기존 유지(base_risk_pct 등).
            #       의미는 allocation_ratio(0~1)로 고정.
            gpt_result = decide_entry_with_gpt_trader(
                self.settings,
                symbol=symbol,
                signal_source=signal_source,
                direction=direction,
                last_price=float(last_price),
                entry_score=entry_score,
                base_risk_pct=float(allocation_ratio),  # allocation_ratio 의미
                base_tp_pct=float(tp_pct),
                base_sl_pct=float(sl_pct),
                extra=extra_to_gpt,
                guard_snapshot=guard_snapshot,
                market_features=market_features,
            )
        except Exception as e:
            logger.exception("decide_entry_with_gpt_trader failed: %r", e)
            raise RuntimeError(f"gpt_strategy decide failed: {e!r}") from e

        if not isinstance(gpt_result, dict):
            raise RuntimeError("gpt_result must be dict")

        # ── D) GPT 반환 구조 강제 검증 ─────────────────────────────────
        if "final_action" not in gpt_result:
            raise RuntimeError("gpt_result missing required key: final_action")
        if "tp_pct" not in gpt_result:
            raise RuntimeError("gpt_result missing required key: tp_pct")
        if "sl_pct" not in gpt_result:
            raise RuntimeError("gpt_result missing required key: sl_pct")
        # allocation 키는 전환 기간 호환으로 별도 검증
        out_alloc = _get_gpt_effective_allocation(gpt_result)

        final_action = str(gpt_result.get("final_action", "")).upper().strip()
        if not final_action:
            raise RuntimeError("gpt_result.final_action must be non-empty")

        gpt_reason = gpt_result.get("reason")
        reason_s = str(gpt_reason) if isinstance(gpt_reason, str) else ""

        action = "ENTER" if final_action == "ENTER" else "SKIP"

        out_tp = _as_float(gpt_result.get("tp_pct"), "gpt.tp_pct", min_value=0.0)
        out_sl = _as_float(gpt_result.get("sl_pct"), "gpt.sl_pct", min_value=0.0)

        # ── E) 범위 검증 ───────────────────────────────────────────────
        if not (0.0 < out_tp <= 1.0):
            raise ValueError(f"gpt tp_pct out of range (0,1]: {out_tp}")
        if not (0.0 < out_sl <= 1.0):
            raise ValueError(f"gpt sl_pct out of range (0,1]: {out_sl}")
        if not (0.0 < out_alloc <= 1.0):
            raise ValueError(f"gpt allocation_ratio out of range (0,1]: {out_alloc}")

        # ── F) guard_adjustments 검증 ─────────────────────────────────
        guard_adjustments: Dict[str, float] = {}
        raw_ga = gpt_result.get("guard_adjustments")
        if raw_ga is not None and not isinstance(raw_ga, dict):
            raise RuntimeError("gpt_result.guard_adjustments must be dict if present")

        if isinstance(raw_ga, dict):
            for k, v in raw_ga.items():
                if not isinstance(k, str) or not k.strip():
                    raise ValueError("guard_adjustments key must be non-empty str")
                fv = _as_float(v, f"guard_adjustments.{k}", min_value=0.0)
                guard_adjustments[k] = fv

        pattern_summary = _require_dict(market_features.get("pattern_summary"), "market_features.pattern_summary")

        # ── G) Signal.meta 강화 ───────────────────────────────────────
        meta: Dict[str, Any] = {
            "symbol": symbol,
            "signal_source": signal_source,
            "regime": regime,
            "signal_ts_ms": signal_ts_ms_i,
            "last_price": float(last_price),
            "candles_5m": candles_5m,
            "candles_5m_raw": candles_5m_raw,
            "extra": extra,  # 원본 extra 보존
            "entry_score": entry_score,
            "account_state": account_state,
            "market_features": market_features,
            "engine_total_score": float(engine_total_score),
            "engine_scores": engine_scores,
            "pattern_summary": pattern_summary,
            "gpt_action": str(gpt_result.get("gpt_action", "")).upper().strip(),
            # 전환/분석용(명시): 실제 의미는 allocation_ratio
            "allocation_ratio": float(out_alloc),
        }

        return Signal(
            action=action,
            direction=direction,
            tp_pct=float(out_tp),
            sl_pct=float(out_sl),
            risk_pct=float(out_alloc),  # 필드명 유지, 의미는 allocation_ratio
            reason=reason_s or ("gpt_approved" if action == "ENTER" else "gpt_skip"),
            guard_adjustments=guard_adjustments,
            meta=meta,
        )