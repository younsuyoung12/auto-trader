# gpt_trader.py
"""
====================================================
FILE: gpt_trader.py
STRICT · NO-FALLBACK · PRODUCTION MODE
====================================================

역할
- WS 기반 마켓 피처(unified_features) + GPT 판단으로
  최종 진입 여부 / 리스크 / TP·SL / 가드 완화 정도를 결정한다.
- GPT 지연/에러가 반복되면 HARD_STOP 으로 신규 진입을 전면 차단한다.

핵심 원칙 (STRICT · NO-FALLBACK)
- 데이터 누락/타입 불일치/범위 위반은 “조용히 보정”하지 않는다.
  - 명시적으로 SKIP(+reason) 처리한다. (추정/대체 금지)
- 알림/로그는 트레이딩 판단/루프를 블로킹하면 안 된다.
  - infra/async_worker.submit()로 비동기 위임한다.
- HARD_STOP 상태는 정상 운영 상태다(엔진 보호).

PATCH NOTES — 2026-03-02 (UPGRADE, MIN-CHANGE)
- 텔레그램/로그 블로킹 제거:
  - infra.async_worker.submit 기반 비동기 전송 적용
- “폴백” 제거:
  - GPT가 값(tp/sl/risk)을 “제시”했는데 그 값이 invalid면 base로 대체하지 않고 SKIP 처리
  - (키가 아예 없으면) base 사용은 ‘정의된 기본값’으로 허용
- TP/SL 범위:
  - settings.tp_pct_min/max, settings.sl_pct_min/max 로 클램핑
====================================================
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from settings import BotSettings
from strategy.gpt_decider_entry import ask_entry_decision
from strategy.unified_features_builder import (
    UnifiedFeaturesError as UnifiedFeatureError,
    build_unified_features,
)
from infra.market_features_ws import FeatureBuildError
from infra.telelog import log as telelog_log, send_tg as telelog_send_tg
from infra.async_worker import submit as submit_async


# ─────────────────────────────────────────
# GPT 진입 장애 상태 전역 관리
# ─────────────────────────────────────────
_gpt_entry_error_streak: int = 0
_gpt_entry_last_error_ts: float = 0.0
_gpt_entry_hard_stop_until_ts: float = 0.0
_gpt_entry_last_call_ts: float = 0.0  # 마지막 GPT ENTRY 호출 시각 (쿨다운용)


# ─────────────────────────────────────────
# 내부 유틸 (로그/텔레그램) — non-blocking
# ─────────────────────────────────────────
def _log(msg: str) -> None:
    try:
        telelog_log(msg)
    except Exception:
        # 로그 실패로 엔진 판단을 깨지 않는다. (비핵심 I/O)
        # 단, 조용히 무시하지 않고 최소한의 흔적을 남긴다.
        # telelog 자체가 깨진 경우 logger 의존도 없이 표준 출력도 하지 않는다(프로젝트 정책).
        pass


def _tg(msg: str) -> None:
    """
    STRICT:
    - 텔레그램은 메인 판단/루프를 블로킹하면 안 된다.
    - 워커 미기동/큐 포화 등은 '전송 실패'로 처리하고, 판단 흐름을 망치지 않는다.
    """
    try:
        submit_async(telelog_send_tg, msg, critical=False, label="send_tg")
    except Exception:
        # 워커 미기동 등 운영 오류는 '알림 실패'로만 처리
        pass


def _now() -> float:
    return time.time()


def _finite_float(v: Any, name: str) -> float:
    if isinstance(v, bool):
        raise ValueError(f"{name} must be number (bool not allowed)")
    x = float(v)
    if not math.isfinite(x):
        raise ValueError(f"{name} must be finite")
    return x


# ─────────────────────────────────────────
# 가드 조정용 하드 범위 정의
# ─────────────────────────────────────────
@dataclass(frozen=True)
class GuardBounds:
    min_entry_volume_ratio_min: float = 0.10
    min_entry_volume_ratio_max: float = 0.60

    max_spread_pct_min: float = 0.0003
    max_spread_pct_max: float = 0.0015

    max_price_jump_pct_min: float = 0.0010
    max_price_jump_pct_max: float = 0.0040

    depth_imbalance_min_ratio_min: float = 1.5
    depth_imbalance_min_ratio_max: float = 4.0

    depth_imbalance_min_notional_min: float = 20.0
    depth_imbalance_min_notional_max: float = 200.0


GUARD_BOUNDS = GuardBounds()


# ─────────────────────────────────────────
# 리스크/TP/SL 클램핑 (NO-FALLBACK)
# ─────────────────────────────────────────
@dataclass(frozen=True)
class RiskParams:
    tp_pct: float
    sl_pct: float
    effective_risk_pct: float


def _pick_optional_float_strict(gpt_json: Dict[str, Any], key: str) -> Optional[float]:
    """
    STRICT:
    - 키가 존재하면 반드시 유효한 float 이어야 한다. (invalid면 예외)
    - 키가 없으면 None (기본값 사용은 호출자에서 처리)
    """
    if key not in gpt_json:
        return None
    v = gpt_json.get(key)
    if v is None:
        raise ValueError(f"gpt_json[{key!r}] is None (STRICT)")
    x = _finite_float(v, f"gpt_json.{key}")
    return float(x)


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _clamp_risk_params(
    settings: BotSettings,
    *,
    base_tp_pct: float,
    base_sl_pct: float,
    base_risk_pct: float,
    gpt_json: Dict[str, Any],
) -> RiskParams:
    """
    STRICT (NO-FALLBACK):
    - GPT가 값을 "제시"했는데 invalid면 base로 대체하지 않는다 → 예외(상위에서 SKIP 처리)
    - GPT가 키를 아예 안 보냈으면 base 사용(정의된 기본값)
    - 범위 초과는 클램핑(정책적 제한)
    """
    tp = _pick_optional_float_strict(gpt_json, "tp_pct")
    sl = _pick_optional_float_strict(gpt_json, "sl_pct")
    rk = _pick_optional_float_strict(gpt_json, "effective_risk_pct")

    tp_final = float(base_tp_pct) if tp is None else float(tp)
    sl_final = float(base_sl_pct) if sl is None else float(sl)
    rk_final = float(base_risk_pct) if rk is None else float(rk)

    if tp_final <= 0:
        raise ValueError("tp_pct must be > 0 (STRICT)")
    if sl_final <= 0:
        raise ValueError("sl_pct must be > 0 (STRICT)")
    if rk_final <= 0:
        raise ValueError("effective_risk_pct must be > 0 (STRICT)")

    tp_lo = float(getattr(settings, "tp_pct_min", 0.0005))
    tp_hi = float(getattr(settings, "tp_pct_max", 0.05))
    sl_lo = float(getattr(settings, "sl_pct_min", 0.0005))
    sl_hi = float(getattr(settings, "sl_pct_max", 0.05))

    if tp_lo <= 0 or tp_hi <= 0 or tp_lo > tp_hi:
        raise ValueError("settings tp_pct_min/max invalid (STRICT)")
    if sl_lo <= 0 or sl_hi <= 0 or sl_lo > sl_hi:
        raise ValueError("settings sl_pct_min/max invalid (STRICT)")

    tp_final = _clamp(tp_final, tp_lo, tp_hi)
    sl_final = _clamp(sl_final, sl_lo, sl_hi)

    # risk 상한: settings.gpt_max_risk_pct가 있으면 그걸 우선, 없으면 1.0
    max_rk = float(getattr(settings, "gpt_max_risk_pct", 1.0))
    if max_rk <= 0 or not math.isfinite(max_rk):
        raise ValueError("settings.gpt_max_risk_pct invalid (STRICT)")
    rk_final = _clamp(rk_final, 0.0, min(1.0, max_rk))

    if rk_final <= 0:
        raise ValueError("effective_risk_pct <= 0 after clamp (STRICT)")

    return RiskParams(tp_pct=tp_final, sl_pct=sl_final, effective_risk_pct=rk_final)


# ─────────────────────────────────────────
# 가드 조정 적용기
# ─────────────────────────────────────────
def _apply_guard_adjustments(
    settings: BotSettings,
    *,
    guard_snapshot: Optional[Dict[str, Any]],
    gpt_json: Dict[str, Any],
) -> Dict[str, float]:
    """
    GPT guard_adjustments 중 안전 범위 안에 있는 것만 선별한다.
    - invalid는 조용히 보정하지 않고 "무시(채택하지 않음)"한다. (폴백 아님: '거부'임)
    """
    raw_adj = gpt_json.get("guard_adjustments")
    if not isinstance(raw_adj, dict):
        return {}

    snap = guard_snapshot if isinstance(guard_snapshot, dict) else {}
    out: Dict[str, float] = {}

    def _base(key: str) -> float:
        if key in snap:
            return float(_finite_float(snap[key], f"guard_snapshot.{key}"))
        return float(_finite_float(getattr(settings, key), f"settings.{key}"))

    # 1) min_entry_volume_ratio
    if "min_entry_volume_ratio" in raw_adj:
        val = float(_finite_float(raw_adj["min_entry_volume_ratio"], "adj.min_entry_volume_ratio"))
        base = _base("min_entry_volume_ratio")
        if GUARD_BOUNDS.min_entry_volume_ratio_min <= val <= GUARD_BOUNDS.min_entry_volume_ratio_max and abs(val - base) <= 0.2:
            out["min_entry_volume_ratio"] = val

    # 2) max_spread_pct
    if "max_spread_pct" in raw_adj:
        val = float(_finite_float(raw_adj["max_spread_pct"], "adj.max_spread_pct"))
        base = _base("max_spread_pct")
        if GUARD_BOUNDS.max_spread_pct_min <= val <= GUARD_BOUNDS.max_spread_pct_max and val <= base * 2.0:
            out["max_spread_pct"] = val

    # 3) max_price_jump_pct
    if "max_price_jump_pct" in raw_adj:
        val = float(_finite_float(raw_adj["max_price_jump_pct"], "adj.max_price_jump_pct"))
        base = _base("max_price_jump_pct")
        if GUARD_BOUNDS.max_price_jump_pct_min <= val <= GUARD_BOUNDS.max_price_jump_pct_max and val <= base * 2.0:
            out["max_price_jump_pct"] = val

    # 4) depth_imbalance_min_ratio
    if "depth_imbalance_min_ratio" in raw_adj:
        val = float(_finite_float(raw_adj["depth_imbalance_min_ratio"], "adj.depth_imbalance_min_ratio"))
        base = _base("depth_imbalance_min_ratio")
        if GUARD_BOUNDS.depth_imbalance_min_ratio_min <= val <= GUARD_BOUNDS.depth_imbalance_min_ratio_max and val >= base * 0.5:
            out["depth_imbalance_min_ratio"] = val

    # 5) depth_imbalance_min_notional
    if "depth_imbalance_min_notional" in raw_adj:
        val = float(_finite_float(raw_adj["depth_imbalance_min_notional"], "adj.depth_imbalance_min_notional"))
        base = _base("depth_imbalance_min_notional")
        if GUARD_BOUNDS.depth_imbalance_min_notional_min <= val <= GUARD_BOUNDS.depth_imbalance_min_notional_max and val >= base * 0.5:
            out["depth_imbalance_min_notional"] = val

    if out:
        _log(f"[GPT_TRADER] guard adjustments accepted: {out}")

    return out


# ─────────────────────────────────────────
# GPT 프롬프트용 extra 컨텍스트 구성기
# ─────────────────────────────────────────
def _sanitize_extra_for_gpt(extra: Dict[str, Any]) -> Dict[str, Any]:
    """
    GPT에 넘길 extra 에서 경량 정보만 추린다.
    - 대용량/복잡 타입은 제거한다. (폴백 아님: '제한' 정책)
    """
    sanitized: Dict[str, Any] = {}

    for key, value in extra.items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            sanitized[key] = value
            continue

        if isinstance(value, (list, tuple)):
            if not value or len(value) > 32:
                continue
            new_list = []
            for item in value:
                if isinstance(item, (str, int, float, bool)) or item is None:
                    new_list.append(item)
                elif isinstance(item, dict):
                    small: Dict[str, Any] = {}
                    for k2, v2 in list(item.items())[:16]:
                        if isinstance(v2, (str, int, float, bool)) or v2 is None:
                            small[k2] = v2
                    if small:
                        new_list.append(small)
            if new_list:
                sanitized[key] = new_list
            continue

        if isinstance(value, dict):
            small2: Dict[str, Any] = {}
            for k2, v2 in list(value.items())[:32]:
                if isinstance(v2, (str, int, float, bool)) or v2 is None:
                    small2[k2] = v2
            if small2:
                sanitized[key] = small2
            continue

    return sanitized


def _build_extra_for_gpt(
    settings: BotSettings,
    *,
    signal_source: str,
    direction: str,
    extra: Optional[Dict[str, Any]],
    guard_snapshot: Optional[Dict[str, Any]],
    entry_score: Optional[float],
    entry_score_tier: Optional[str],
    min_entry_score_for_scalp: float,
    min_entry_score_for_trend: float,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}

    if isinstance(extra, dict) and extra:
        payload.update(_sanitize_extra_for_gpt(extra))

    src = (signal_source or "").upper()
    if src and "regime" not in payload:
        payload["regime"] = src

    dir_up = (direction or "").upper()
    if dir_up and "direction" not in payload:
        payload["direction"] = dir_up

    if isinstance(guard_snapshot, dict):
        guard_keys = [
            "min_entry_volume_ratio",
            "max_spread_pct",
            "max_price_jump_pct",
            "depth_imbalance_min_ratio",
            "depth_imbalance_min_notional",
        ]
        trimmed_guard: Dict[str, Any] = {k: guard_snapshot[k] for k in guard_keys if k in guard_snapshot}
        if trimmed_guard:
            payload["guard_snapshot"] = trimmed_guard

    if isinstance(entry_score, (int, float)) and "entry_score" not in payload:
        payload["entry_score"] = float(entry_score)
        payload["entry_score_tier"] = entry_score_tier or "UNKNOWN"
        payload["entry_score_min_scalp"] = float(min_entry_score_for_scalp)
        payload["entry_score_min_trend"] = float(min_entry_score_for_trend)

    return payload


# ─────────────────────────────────────────
# 메인 엔트리
# ─────────────────────────────────────────
def decide_entry_with_gpt_trader(
    settings: BotSettings,
    *,
    symbol: str,
    signal_source: str,  # "TREND" / "RANGE" / "HYBRID" 등
    direction: str,  # "LONG" / "SHORT"
    last_price: float,
    entry_score: Optional[float],
    base_risk_pct: float,
    base_tp_pct: float,
    base_sl_pct: float,
    extra: Optional[Dict[str, Any]] = None,
    guard_snapshot: Optional[Dict[str, Any]] = None,
    market_features: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    GPT + WS 피처 조합으로 최종 진입 결정을 내린다.
    반환 딕셔너리는 항상 동일 스키마를 유지한다.
    """
    global _gpt_entry_error_streak, _gpt_entry_last_error_ts, _gpt_entry_hard_stop_until_ts, _gpt_entry_last_call_ts

    hard_stop_min_errors = int(getattr(settings, "gpt_entry_hard_stop_min_errors", 3))
    hard_stop_cooldown = float(getattr(settings, "gpt_entry_hard_stop_cooldown_sec", 30.0))
    error_reset_window = float(getattr(settings, "gpt_entry_error_reset_window_sec", 60.0))

    now_ts = _now()

    result: Dict[str, Any] = {
        "final_action": "SKIP",
        "gpt_action": "ERROR",
        "reason": "init",
        "tp_pct": float(base_tp_pct),
        "sl_pct": float(base_sl_pct),
        "effective_risk_pct": float(base_risk_pct),
        "guard_adjustments": {},
        "sleep_after_sec": float(getattr(settings, "gpt_error_sleep_sec", 5.0)),
        "gpt_status": "OK",
        "gpt_latency_sec": None,
        "hard_stop": False,
        "raw": {},
    }

    # 0) HARD_STOP 윈도우
    if _gpt_entry_hard_stop_until_ts and now_ts < _gpt_entry_hard_stop_until_ts:
        remaining = max(_gpt_entry_hard_stop_until_ts - now_ts, 0.0)
        result.update(
            {
                "final_action": "SKIP",
                "gpt_action": "ERROR",
                "gpt_status": "HARD_STOP",
                "hard_stop": True,
                "reason": "GPT 진입 판단 장애(HARD_STOP) 상태로 신규 진입을 중단합니다.",
                "sleep_after_sec": max(remaining, hard_stop_cooldown),
            }
        )
        _log(f"[GPT_TRADER] HARD_STOP active → skip (remaining={remaining:.1f}s)")
        return result

    # HARD_STOP 해제 시 상태 리셋
    if _gpt_entry_hard_stop_until_ts and now_ts >= _gpt_entry_hard_stop_until_ts:
        _gpt_entry_hard_stop_until_ts = 0.0
        _gpt_entry_error_streak = 0
        _gpt_entry_last_error_ts = 0.0
        _log("[GPT_TRADER] HARD_STOP ended → gate reopened")

    # 0-1) EntryScore 기반 티어 + 최저 점수 가드
    min_entry_score_for_scalp = float(
        getattr(settings, "min_entry_score_for_scalp", getattr(settings, "min_entry_score_for_gpt", 40.0))
    )
    min_entry_score_for_trend = float(getattr(settings, "min_entry_score_for_trend", 50.0))

    entry_score_tier: Optional[str] = None
    if isinstance(entry_score, (int, float)):
        if entry_score >= min_entry_score_for_trend:
            entry_score_tier = "TREND"
        elif entry_score >= min_entry_score_for_scalp:
            entry_score_tier = "SCALP"
        else:
            entry_score_tier = "LOW"

        if entry_score < min_entry_score_for_scalp:
            result.update(
                {
                    "final_action": "SKIP",
                    "gpt_action": "SKIP",
                    "gpt_status": "OK",
                    "reason": f"EntryScore={entry_score:.1f} < min_scalp={min_entry_score_for_scalp:.1f} (GPT 생략)",
                    "sleep_after_sec": float(getattr(settings, "gpt_skip_sleep_sec", 3.0)),
                }
            )
            _log(f"[GPT_TRADER] entry_score<{min_entry_score_for_scalp:.1f} → SKIP without GPT")
            return result

    # 0-2) GPT ENTRY 호출 쿨다운
    cooldown_sec = float(getattr(settings, "gpt_entry_cooldown_sec", 1.0))
    if _gpt_entry_last_call_ts > 0:
        elapsed = now_ts - _gpt_entry_last_call_ts
        if elapsed < cooldown_sec:
            remaining = max(cooldown_sec - elapsed, 0.0)
            result.update(
                {
                    "final_action": "SKIP",
                    "gpt_action": "SKIP",
                    "gpt_status": "OK",
                    "reason": f"GPT ENTRY cooldown (remaining={remaining:.1f}s)",
                    "sleep_after_sec": float(getattr(settings, "gpt_skip_sleep_sec", 3.0)),
                }
            )
            return result

    # 0-3) WS 기반 unified_features 준비
    try:
        if market_features is None:
            market_features = build_unified_features(symbol=symbol)
    except (UnifiedFeatureError, FeatureBuildError) as e:
        result.update(
            {
                "final_action": "SKIP",
                "gpt_action": "SKIP",
                "gpt_status": "DATA_ERROR",
                "reason": f"feature build failed: {type(e).__name__}: {e}",
                "sleep_after_sec": float(getattr(settings, "gpt_skip_sleep_sec", 3.0)),
            }
        )
        return result

    # 1) GPT 프롬프트용 extra 구성
    extra_for_gpt = _build_extra_for_gpt(
        settings,
        signal_source=signal_source,
        direction=direction,
        extra=extra,
        guard_snapshot=guard_snapshot,
        entry_score=entry_score,
        entry_score_tier=entry_score_tier,
        min_entry_score_for_scalp=min_entry_score_for_scalp,
        min_entry_score_for_trend=min_entry_score_for_trend,
    )

    features_for_gpt: Dict[str, Any] = {}
    if isinstance(market_features, dict):
        features_for_gpt.update(market_features)
    if extra_for_gpt:
        features_for_gpt.update(extra_for_gpt)

    # 2) GPT에 진입 의사결정 요청
    _gpt_entry_last_call_ts = _now()
    t0 = time.perf_counter()

    try:
        gpt_json = ask_entry_decision(
            symbol=symbol,
            signal_source=signal_source,
            chosen_signal=direction,
            last_price=last_price,
            entry_score=entry_score,
            effective_risk_pct=base_risk_pct,
            market_features=features_for_gpt,
            # 기존 시그니처 호환(중복 파라미터 유지)
            source=signal_source,
            current_price=last_price,
            base_tv_pct=base_tp_pct,
            base_sl_pct=base_sl_pct,
            base_risk_pct=base_risk_pct,
        )
    except Exception as e:
        now_ts2 = _now()

        if _gpt_entry_last_error_ts and (now_ts2 - _gpt_entry_last_error_ts) > error_reset_window:
            _gpt_entry_error_streak = 0
        _gpt_entry_last_error_ts = now_ts2
        _gpt_entry_error_streak += 1

        if _gpt_entry_error_streak >= hard_stop_min_errors:
            _gpt_entry_hard_stop_until_ts = now_ts2 + hard_stop_cooldown
            result.update(
                {
                    "gpt_status": "HARD_STOP",
                    "hard_stop": True,
                    "reason": "GPT ENTRY 장애 반복 → HARD_STOP",
                    "sleep_after_sec": hard_stop_cooldown,
                }
            )
            _tg("⚠️ [GPT_ENTRY][HARD_STOP] GPT 진입 판단 장애 반복으로 신규 진입을 일시 중단합니다.")
        else:
            result.update(
                {
                    "gpt_status": "ERROR",
                    "hard_stop": False,
                    "reason": f"GPT 호출 오류 → SKIP ({type(e).__name__}: {e})",
                    "sleep_after_sec": float(getattr(settings, "gpt_error_sleep_sec", 5.0)),
                }
            )
            _tg("⚠️ GPT 진입 판단 호출 실패: 이번 루프는 SKIP 처리합니다.")
        return result
    finally:
        dt = (time.perf_counter() - t0)
        result["gpt_latency_sec"] = float(dt)

    result["raw"] = gpt_json

    # 정상 응답 → 에러 상태 리셋
    _gpt_entry_error_streak = 0
    _gpt_entry_last_error_ts = 0.0

    if not isinstance(gpt_json, dict):
        result.update(
            {
                "final_action": "SKIP",
                "gpt_action": "ERROR",
                "gpt_status": "ERROR",
                "reason": f"GPT 응답 타입 오류: {type(gpt_json).__name__}",
                "sleep_after_sec": float(getattr(settings, "gpt_error_sleep_sec", 5.0)),
            }
        )
        return result

    # 3) action 정규화
    action_raw = str(gpt_json.get("action", "SKIP")).upper().strip()
    if action_raw not in {"ENTER", "SKIP", "ADJUST"}:
        result.update(
            {
                "final_action": "SKIP",
                "gpt_action": "ERROR",
                "gpt_status": "ERROR",
                "reason": f"GPT invalid action: {action_raw!r}",
                "sleep_after_sec": float(getattr(settings, "gpt_skip_sleep_sec", 3.0)),
            }
        )
        return result

    result["gpt_action"] = action_raw

    # 4) 리스크/TP/SL 결정 (NO-FALLBACK)
    try:
        risk_params = _clamp_risk_params(
            settings,
            base_tp_pct=base_tp_pct,
            base_sl_pct=base_sl_pct,
            base_risk_pct=base_risk_pct,
            gpt_json=gpt_json,
        )
    except Exception as e:
        result.update(
            {
                "final_action": "SKIP",
                "gpt_action": "ERROR",
                "gpt_status": "ERROR",
                "reason": f"GPT risk params invalid → SKIP ({type(e).__name__}: {e})",
                "sleep_after_sec": float(getattr(settings, "gpt_error_sleep_sec", 5.0)),
            }
        )
        return result

    result["tp_pct"] = risk_params.tp_pct
    result["sl_pct"] = risk_params.sl_pct
    result["effective_risk_pct"] = risk_params.effective_risk_pct

    # 5) 가드 조정
    try:
        result["guard_adjustments"] = _apply_guard_adjustments(
            settings,
            guard_snapshot=guard_snapshot,
            gpt_json=gpt_json,
        )
    except Exception as e:
        # 가드 조정 파싱 오류는 '조정 거부'로만 처리 (트레이딩 판단 폴백/추정 금지)
        result["guard_adjustments"] = {}
        _log(f"[GPT_TRADER] guard_adjustments parse failed → ignored: {type(e).__name__}: {e}")

    # 6) 최종 액션
    if action_raw in {"ENTER", "ADJUST"}:
        result["final_action"] = "ENTER"
        result["sleep_after_sec"] = 0.0
        result["gpt_status"] = "OK"
    else:
        result["final_action"] = "SKIP"
        result["sleep_after_sec"] = float(getattr(settings, "gpt_skip_sleep_sec", 3.0))
        result["gpt_status"] = "OK"

    reason = gpt_json.get("reason") or "GPT 판단 결과 반영"
    result["reason"] = str(reason)

    _log(
        "[GPT_TRADER] final_action={} gpt_action={} tp={} sl={} risk={} guards={} symbol={} src={} dir={} status={} lat={}".format(
            result["final_action"],
            result["gpt_action"],
            result["tp_pct"],
            result["sl_pct"],
            result["effective_risk_pct"],
            result["guard_adjustments"],
            symbol,
            signal_source,
            direction,
            result.get("gpt_status"),
            result.get("gpt_latency_sec"),
        )
    )

    return result


__all__ = ["decide_entry_with_gpt_trader"]