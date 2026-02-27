# gpt_trader.py
# ====================================================
# 역할
# ----------------------------------------------------
# - WS 기반 마켓 피처(unified_features) + GPT 판단으로
#   최종 진입 여부 / 리스크 / TP·SL / 가드 완화 정도를 결정한다.
# - EntryScore 기준:
#     · min_entry_score_for_scalp(기본 40) 이상 → GPT 스캘핑/추세 판단에 사용
#     · min_entry_score_for_trend(기본 50) 이상 → 추세 이어먹기(TREND 티어) 컨텍스트 제공
# - GPT가 제안한 값은 settings_ws.BotSettings 의 gpt_max_* 범위 안으로만 허용한다.
# - GPT 지연/에러가 반복되면 일정 시간 HARD_STOP 으로 신규 진입을 전면 차단한다.

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from settings import BotSettings
from strategy.gpt_decider_entry import ask_entry_decision
from strategy.unified_features_builder import (
    build_unified_features,
    UnifiedFeaturesError as UnifiedFeatureError,
)
from infra.market_features_ws import FeatureBuildError


# ─────────────────────────────────────────
# 내부 유틸 (텔레그램/로그)
# ─────────────────────────────────────────


def _safe_log(msg: str) -> None:
    """telelog.log 가 있으면 사용, 없으면 조용히 무시."""
    try:
        from infra.telelog import log  # type: ignore

        log(msg)
    except Exception:
        pass


def _safe_tg(msg: str) -> None:
    """telelog.send_tg 가 있으면 사용, 없으면 무시."""
    try:
        from infra.telelog import send_tg  # type: ignore

        send_tg(msg)
    except Exception:
        pass


# ─────────────────────────────────────────
# GPT 진입 장애 상태 전역 관리
# ─────────────────────────────────────────


_gpt_entry_error_streak: int = 0
_gpt_entry_last_error_ts: float = 0.0
_gpt_entry_hard_stop_until_ts: float = 0.0
_gpt_entry_last_call_ts: float = 0.0  # 마지막 GPT ENTRY 호출 시각 (쿨다운용)


def _now_ts() -> float:
    """time.time() 래퍼 (예외 시 0.0)."""
    try:
        return time.time()
    except Exception:
        return 0.0


# ─────────────────────────────────────────
# 가드 조정용 하드 범위 정의
# ─────────────────────────────────────────


@dataclass(frozen=True)
class GuardBounds:
    """GPT가 조정할 수 있는 일부 가드 값의 안전 범위."""

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
# 리스크/TP/SL 클램핑
# ─────────────────────────────────────────


@dataclass
class RiskParams:
    """진입에 사용할 최종 리스크/TP/SL 파라미터."""

    tp_pct: float
    sl_pct: float
    effective_risk_pct: float


def _clamp_risk_params(
    settings: BotSettings,
    *,
    base_tp_pct: float,
    base_sl_pct: float,
    base_risk_pct: float,
    gpt_json: Dict[str, Any],
) -> RiskParams:
    """GPT 제안 + 기본값을 합쳐 최종 TP/SL/리스크를 결정한다."""

    tp_raw = float(gpt_json.get("tp_pct", base_tp_pct))
    sl_raw = float(gpt_json.get("sl_pct", base_sl_pct))
    risk_raw = float(gpt_json.get("effective_risk_pct", base_risk_pct))

    # 음수/0 보호
    tp_candidate = tp_raw if tp_raw > 0 else base_tp_pct
    sl_candidate = sl_raw if sl_raw > 0 else base_sl_pct
    risk_candidate = risk_raw if risk_raw > 0 else base_risk_pct

    # 상한 클램핑
    tp_clamped = min(tp_candidate, float(getattr(settings, "gpt_max_tp_pct", 0.10)))
    sl_clamped = min(sl_candidate, float(getattr(settings, "gpt_max_sl_pct", 0.05)))
    risk_clamped = min(
        risk_candidate,
        float(getattr(settings, "gpt_max_risk_pct", 0.03)),
    )

    return RiskParams(
        tp_pct=tp_clamped,
        sl_pct=sl_clamped,
        effective_risk_pct=risk_clamped,
    )


# ─────────────────────────────────────────
# 가드 조정 적용기
# ─────────────────────────────────────────


def _apply_guard_adjustments(
    settings: BotSettings,
    *,
    guard_snapshot: Optional[Dict[str, Any]],
    gpt_json: Dict[str, Any],
) -> Dict[str, float]:
    """GPT guard_adjustments 중 안전 범위 안에 있는 것만 선별."""

    adjustments: Dict[str, float] = {}

    raw_adj = gpt_json.get("guard_adjustments")
    if not isinstance(raw_adj, dict):
        return adjustments

    snap = guard_snapshot or {}

    # 1) min_entry_volume_ratio
    if "min_entry_volume_ratio" in raw_adj:
        try:
            val = float(raw_adj["min_entry_volume_ratio"])
            base = float(
                snap.get("min_entry_volume_ratio", settings.min_entry_volume_ratio)
            )
            lo = GUARD_BOUNDS.min_entry_volume_ratio_min
            hi = GUARD_BOUNDS.min_entry_volume_ratio_max
            if lo <= val <= hi and abs(val - base) <= 0.2:
                adjustments["min_entry_volume_ratio"] = val
        except Exception:
            pass

    # 2) max_spread_pct
    if "max_spread_pct" in raw_adj:
        try:
            val = float(raw_adj["max_spread_pct"])
            base = float(snap.get("max_spread_pct", settings.max_spread_pct))
            lo = GUARD_BOUNDS.max_spread_pct_min
            hi = GUARD_BOUNDS.max_spread_pct_max
            if lo <= val <= hi and val <= base * 2.0:
                adjustments["max_spread_pct"] = val
        except Exception:
            pass

    # 3) max_price_jump_pct
    if "max_price_jump_pct" in raw_adj:
        try:
            val = float(raw_adj["max_price_jump_pct"])
            base = float(snap.get("max_price_jump_pct", settings.max_price_jump_pct))
            lo = GUARD_BOUNDS.max_price_jump_pct_min
            hi = GUARD_BOUNDS.max_price_jump_pct_max
            if lo <= val <= hi and val <= base * 2.0:
                adjustments["max_price_jump_pct"] = val
        except Exception:
            pass

    # 4) depth_imbalance_min_ratio
    if "depth_imbalance_min_ratio" in raw_adj:
        try:
            val = float(raw_adj["depth_imbalance_min_ratio"])
            base = float(
                snap.get("depth_imbalance_min_ratio", settings.depth_imbalance_min_ratio)
            )
            lo = GUARD_BOUNDS.depth_imbalance_min_ratio_min
            hi = GUARD_BOUNDS.depth_imbalance_min_ratio_max
            if lo <= val <= hi and val >= base * 0.5:
                adjustments["depth_imbalance_min_ratio"] = val
        except Exception:
            pass

    # 5) depth_imbalance_min_notional
    if "depth_imbalance_min_notional" in raw_adj:
        try:
            val = float(raw_adj["depth_imbalance_min_notional"])
            base = float(
                snap.get(
                    "depth_imbalance_min_notional", settings.depth_imbalance_min_notional
                )
            )
            lo = GUARD_BOUNDS.depth_imbalance_min_notional_min
            hi = GUARD_BOUNDS.depth_imbalance_min_notional_max
            if lo <= val <= hi and val >= base * 0.5:
                adjustments["depth_imbalance_min_notional"] = val
        except Exception:
            pass

    if adjustments:
        _safe_log(f"[GPT_TRADER] guard adjustments accepted: {adjustments}")

    return adjustments


# ─────────────────────────────────────────
# GPT 프롬프트용 extra 컨텍스트 구성기
# ─────────────────────────────────────────


def _sanitize_extra_for_gpt(extra: Dict[str, Any]) -> Dict[str, Any]:
    """GPT에 넘길 extra 에서 가벼운 정보만 추려낸다."""
    sanitized: Dict[str, Any] = {}

    for key, value in extra.items():
        # 1) 스칼라 타입은 그대로
        if isinstance(value, (str, int, float, bool)) or value is None:
            sanitized[key] = value
            continue

        # 2) 리스트/튜플
        if isinstance(value, (list, tuple)):
            if not value or len(value) > 32:
                continue
            new_list = []
            for item in value:
                if isinstance(item, (str, int, float, bool)) or item is None:
                    new_list.append(item)
                elif isinstance(item, dict):
                    small_dict: Dict[str, Any] = {}
                    for k2, v2 in list(item.items())[:16]:
                        if isinstance(v2, (str, int, float, bool)) or v2 is None:
                            small_dict[k2] = v2
                    if small_dict:
                        new_list.append(small_dict)
            if new_list:
                sanitized[key] = new_list
            continue

        # 3) dict
        if isinstance(value, dict):
            small_dict2: Dict[str, Any] = {}
            for k2, v2 in list(value.items())[:32]:
                if isinstance(v2, (str, int, float, bool)) or v2 is None:
                    small_dict2[k2] = v2
            if small_dict2:
                sanitized[key] = small_dict2
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
    """ask_entry_decision 에 전달할 extra 컨텍스트를 구성."""

    payload: Dict[str, Any] = {}

    # 1) 호출측 extra → 경량화해서 병합
    if isinstance(extra, dict) and extra:
        payload.update(_sanitize_extra_for_gpt(extra))

    # 2) 장 종류/방향 명시
    src = (signal_source or "").upper()
    if src and "regime" not in payload:
        payload["regime"] = src

    dir_up = (direction or "").upper()
    if dir_up and "direction" not in payload:
        payload["direction"] = dir_up

    # 3) 현재 가드 스냅샷 요약
    if isinstance(guard_snapshot, dict):
        guard_keys = [
            "min_entry_volume_ratio",
            "max_spread_pct",
            "max_price_jump_pct",
            "depth_imbalance_min_ratio",
            "depth_imbalance_min_notional",
        ]
        trimmed_guard: Dict[str, Any] = {
            k: guard_snapshot[k] for k in guard_keys if k in guard_snapshot
        }
        if trimmed_guard:
            if "guard_snapshot" in payload and isinstance(
                payload["guard_snapshot"], dict
            ):
                merged = dict(payload["guard_snapshot"])  # 얕은 복사 후 업데이트
                merged.update(trimmed_guard)
                payload["guard_snapshot"] = merged
            else:
                payload["guard_snapshot"] = trimmed_guard

    # 4) 최근 PnL / 스킵 패턴 (옵션)
    if "recent_pnl_pct" not in payload and hasattr(settings, "recent_pnl_pct"):
        try:
            payload["recent_pnl_pct"] = float(getattr(settings, "recent_pnl_pct"))
        except Exception:
            pass

    if "skip_streak" not in payload and hasattr(settings, "skip_streak"):
        try:
            payload["skip_streak"] = int(getattr(settings, "skip_streak"))
        except Exception:
            pass

    # 5) EntryScore / 티어 정보
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
    """GPT + 하드 리스크/가드 + WS 피처 조합으로 최종 진입 결정을 내린다."""

    global _gpt_entry_error_streak, _gpt_entry_last_error_ts, _gpt_entry_hard_stop_until_ts, _gpt_entry_last_call_ts

    # 설정값
    hard_stop_min_errors = int(getattr(settings, "gpt_entry_hard_stop_min_errors", 3))
    hard_stop_cooldown = float(
        getattr(settings, "gpt_entry_hard_stop_cooldown_sec", 30.0)
    )
    error_reset_window = float(
        getattr(settings, "gpt_entry_error_reset_window_sec", 60.0)
    )

    now_ts = _now_ts()

    # 기본 응답 틀
    result: Dict[str, Any] = {
        "final_action": "SKIP",
        "gpt_action": "ERROR",
        "reason": "초기 상태",
        "tp_pct": base_tp_pct,
        "sl_pct": base_sl_pct,
        "effective_risk_pct": base_risk_pct,
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
        result["final_action"] = "SKIP"
        result["gpt_action"] = "ERROR"
        result["gpt_status"] = "HARD_STOP"
        result["hard_stop"] = True
        result["reason"] = "GPT 진입 판단 장애(HARD_STOP) 상태로 신규 진입을 잠시 중단합니다."
        result["sleep_after_sec"] = max(remaining, hard_stop_cooldown)
        _safe_log(
            f"[GPT_TRADER] HARD_STOP window active → skip entry (remaining={remaining:.1f}s)"
        )
        return result

    # HARD_STOP 해제 시 상태 리셋
    if _gpt_entry_hard_stop_until_ts and now_ts >= _gpt_entry_hard_stop_until_ts:
        _gpt_entry_hard_stop_until_ts = 0.0
        _gpt_entry_error_streak = 0
        _gpt_entry_last_error_ts = 0.0
        _safe_log("[GPT_TRADER] HARD_STOP window ended → GPT entry gate reopened")

    # 0-1) EntryScore 기반 티어 계산 및 최저 진입 점수 가드
    min_entry_score_for_scalp = float(
        getattr(
            settings,
            "min_entry_score_for_scalp",
            getattr(settings, "min_entry_score_for_gpt", 40.0),
        )
    )
    min_entry_score_for_trend = float(
        getattr(settings, "min_entry_score_for_trend", 50.0)
    )

    if isinstance(entry_score, (int, float)):
        if entry_score >= min_entry_score_for_trend:
            entry_score_tier: Optional[str] = "TREND"
        elif entry_score >= min_entry_score_for_scalp:
            entry_score_tier = "SCALP"
        else:
            entry_score_tier = "LOW"
    else:
        entry_score_tier = None

    # 40점 미만이면 GPT 호출 자체를 생략 (보수적 가드, 일반적으로 entry_flow에서 이미 걸러짐)
    if isinstance(entry_score, (int, float)) and entry_score < min_entry_score_for_scalp:
        result["final_action"] = "SKIP"
        result["gpt_action"] = "SKIP"
        result["gpt_status"] = "OK"
        result["reason"] = (
            f"EntryScore={entry_score:.1f} < 최소 진입 점수({min_entry_score_for_scalp:.1f})로 "
            "GPT 진입 평가를 생략합니다."
        )
        result["sleep_after_sec"] = float(
            getattr(settings, "gpt_skip_sleep_sec", 3.0)
        )
        _safe_log(
            f"[GPT_TRADER] pre_entry_score={entry_score:.1f} "
            f"< min_scalp={min_entry_score_for_scalp:.1f} → SKIP without GPT call"
        )
        return result

    # 0-2) GPT ENTRY 호출 쿨다운
    cooldown_sec = int(getattr(settings, "gpt_entry_cooldown_sec", 1))
    if _gpt_entry_last_call_ts > 0:
        elapsed = now_ts - _gpt_entry_last_call_ts
        if elapsed < cooldown_sec:
            remaining = int(cooldown_sec - elapsed)
            result["final_action"] = "SKIP"
            result["gpt_action"] = "SKIP"
            result["gpt_status"] = "OK"
            result["reason"] = (
                "GPT ENTRY 호출 쿨다운으로 이번 루프는 진입 없이 건너뜁니다. "
                f"({remaining}s 남음)"
            )
            result["sleep_after_sec"] = float(
                getattr(settings, "gpt_skip_sleep_sec", 3.0)
            )
            _safe_log(
                f"[GPT_TRADER] entry cooldown → SKIP (elapsed={elapsed:.1f}s, "
                f"remaining={remaining}s, cooldown={cooldown_sec}s)"
            )
            return result

    # 0-3) WS 기반 unified_features 준비
    try:
        if market_features is None:
            market_features = build_unified_features(symbol=symbol)
    except (UnifiedFeatureError, FeatureBuildError) as e:
        result["final_action"] = "SKIP"
        result["gpt_action"] = "SKIP"
        result["gpt_status"] = "DATA_ERROR"
        result["reason"] = (
            "WS 시세/패턴 피처를 만들지 못해 이번 루프는 진입 없이 건너뜁니다. "
            f"({type(e).__name__}: {e})"
        )
        result["sleep_after_sec"] = float(getattr(settings, "gpt_skip_sleep_sec", 3.0))
        _safe_log(f"[GPT_TRADER] feature build error → SKIP: {e}")
        return result
    except Exception as e:
        result["final_action"] = "SKIP"
        result["gpt_action"] = "ERROR"
        result["gpt_status"] = "ERROR"
        result["reason"] = (
            "WS 피처 생성 중 알 수 없는 오류로 이번 루프는 진입 없이 건너뜁니다. "
            f"({type(e).__name__}: {e})"
        )
        result["sleep_after_sec"] = float(getattr(settings, "gpt_error_sleep_sec", 5.0))
        _safe_log(
            f"[GPT_TRADER] unexpected feature build error → SKIP: {type(e).__name__}: {e}"
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

    # unified_features + extra_for_gpt 병합
    features_for_gpt: Dict[str, Any] = {}
    if isinstance(market_features, dict):
        features_for_gpt.update(market_features)
    if extra_for_gpt:
        features_for_gpt.update(extra_for_gpt)

    # 2) GPT에 진입 의사결정 요청
    try:
        _gpt_entry_last_call_ts = _now_ts()

        gpt_json = ask_entry_decision(
            symbol=symbol,
            signal_source=signal_source,
            chosen_signal=direction,
            last_price=last_price,
            entry_score=entry_score,
            effective_risk_pct=base_risk_pct,
            market_features=features_for_gpt,
            source=signal_source,
            current_price=last_price,
            base_tv_pct=base_tp_pct,  # 기존 시그니처에 맞춰 유지
            base_sl_pct=base_sl_pct,
            base_risk_pct=base_risk_pct,
        )
        result["raw"] = gpt_json

        # 정상 응답 → 에러 상태 리셋
        _gpt_entry_error_streak = 0
        _gpt_entry_last_error_ts = 0.0

        meta = gpt_json.get("_meta") if isinstance(gpt_json, dict) else None
        if isinstance(meta, dict):
            lat = meta.get("latency_sec")
            if isinstance(lat, (int, float)):
                result["gpt_latency_sec"] = float(lat)
    except Exception as e:
        now_ts = _now_ts()

        if _gpt_entry_last_error_ts and (now_ts - _gpt_entry_last_error_ts) > error_reset_window:
            _gpt_entry_error_streak = 0
        _gpt_entry_last_error_ts = now_ts
        _gpt_entry_error_streak += 1

        msg = (
            f"[GPT_TRADER] ask_entry_decision error "
            f"(streak={_gpt_entry_error_streak}) → SKIP: {type(e).__name__}: {e}"
        )
        _safe_log(msg)

        if _gpt_entry_error_streak >= hard_stop_min_errors:
            _gpt_entry_hard_stop_until_ts = now_ts + hard_stop_cooldown
            result["gpt_status"] = "HARD_STOP"
            result["hard_stop"] = True
            result["reason"] = (
                "GPT 진입 판단이 반복적으로 실패하여 일정 시간 신규 진입을 중단합니다."
            )
            result["sleep_after_sec"] = hard_stop_cooldown
            _safe_tg(
                "⚠️ [GPT_ENTRY][HARD_STOP] GPT 진입 판단 장애가 반복되어 일정 시간 신규 진입을 중단합니다."
            )
        else:
            result["gpt_status"] = "ERROR"
            result["hard_stop"] = False
            result["reason"] = "GPT 호출 오류로 이번 루프는 진입 없이 건너뜁니다."
            result["sleep_after_sec"] = float(
                getattr(settings, "gpt_error_sleep_sec", 5.0)
            )
            _safe_tg(
                "⚠️ GPT 진입 판단 호출에 실패했습니다. 이번 루프는 진입 없이 건너뜁니다."
            )

        return result

    # 2-1) 응답 타입 검증
    if not isinstance(gpt_json, dict):
        now_ts = _now_ts()

        if _gpt_entry_last_error_ts and (now_ts - _gpt_entry_last_error_ts) > error_reset_window:
            _gpt_entry_error_streak = 0
        _gpt_entry_last_error_ts = now_ts
        _gpt_entry_error_streak += 1

        _safe_log(
            f"[GPT_TRADER] invalid response type from ask_entry_decision: "
            f"{type(gpt_json).__name__} (streak={_gpt_entry_error_streak}) → SKIP"
        )

        if _gpt_entry_error_streak >= hard_stop_min_errors:
            _gpt_entry_hard_stop_until_ts = now_ts + hard_stop_cooldown
            result["gpt_status"] = "HARD_STOP"
            result["hard_stop"] = True
            result["reason"] = (
                "GPT 진입 판단 응답 포맷이 반복적으로 잘못되어 일정 시간 신규 진입을 중단합니다."
            )
            result["sleep_after_sec"] = hard_stop_cooldown
            _safe_tg(
                "⚠️ [GPT_ENTRY][HARD_STOP] GPT 진입 판단 응답 포맷이 반복적으로 잘못되어 일정 시간 신규 진입을 중단합니다."
            )
        else:
            result["gpt_status"] = "ERROR"
            result["hard_stop"] = False
            result["reason"] = "GPT 응답 포맷 오류로 이번 루프는 진입 없이 건너뜁니다."
            result["sleep_after_sec"] = float(
                getattr(settings, "gpt_error_sleep_sec", 5.0)
            )
            _safe_tg(
                "⚠️ GPT 진입 판단 응답 포맷이 잘못되었습니다. 이번 루프는 진입 없이 건너뜁니다."
            )

        return result

    # 3) action 정규화
    action_raw = str(gpt_json.get("action", "SKIP")).upper()
    if action_raw not in {"ENTER", "SKIP", "ADJUST"}:
        _safe_log(f"[GPT_TRADER] invalid action from GPT: {action_raw!r} → SKIP")
        result["reason"] = f"GPT가 알 수 없는 action을 반환했습니다: {action_raw!r}"
        result["sleep_after_sec"] = float(getattr(settings, "gpt_skip_sleep_sec", 3.0))
        result["gpt_status"] = "ERROR"
        return result

    result["gpt_action"] = action_raw

    # 4) 리스크/TP/SL 클램핑
    risk_params = _clamp_risk_params(
        settings,
        base_tp_pct=base_tp_pct,
        base_sl_pct=base_sl_pct,
        base_risk_pct=base_risk_pct,
        gpt_json=gpt_json,
    )

    result["tp_pct"] = risk_params.tp_pct
    result["sl_pct"] = risk_params.sl_pct
    result["effective_risk_pct"] = risk_params.effective_risk_pct

    # 리스크 0 → 강제 SKIP
    if risk_params.effective_risk_pct <= 0.0:
        result["final_action"] = "SKIP"
        result["reason"] = (
            "GPT가 리스크를 0% 이하로 제안했거나, 상한/하한 처리 후 0%가 되었습니다."
        )
        result["sleep_after_sec"] = float(
            getattr(settings, "gpt_skip_sleep_sec", 3.0)
        )
        result["gpt_status"] = "ERROR"
        _safe_log("[GPT_TRADER] effective_risk_pct <= 0 → SKIP")
        return result

    # 5) 가드 조정
    guard_adj = _apply_guard_adjustments(
        settings,
        guard_snapshot=guard_snapshot,
        gpt_json=gpt_json,
    )
    result["guard_adjustments"] = guard_adj

    # 6) 최종 액션 결정
    if action_raw in {"ENTER", "ADJUST"}:
        result["final_action"] = "ENTER"
        result["sleep_after_sec"] = 0.0
        result["gpt_status"] = "OK"
    else:
        result["final_action"] = "SKIP"
        result["sleep_after_sec"] = float(getattr(settings, "gpt_skip_sleep_sec", 3.0))
        if result["gpt_status"] == "OK":
            result["gpt_status"] = "OK"

    reason = gpt_json.get("reason") or "GPT 판단 결과 반영"
    result["reason"] = str(reason)

    # 7) 요약 로그
    try:
        _safe_log(
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
    except Exception:
        pass

    return result


__all__ = [
    "decide_entry_with_gpt_trader",
]
