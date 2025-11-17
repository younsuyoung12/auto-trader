# gpt_trader.py
# ====================================================
# 역할
# ----------------------------------------------------
# - BingX Auto Trader에서 "GPT 판단"과 "하드 리스크 가드" 사이의 중간 레이어.
# - gpt_decider.ask_entry_decision 이 반환한 JSON을 해석해서
#     · 너무 보수적인 조건 때문에 항상 SKIP 나는 상황에서는 GPT가 부드럽게 조정할 수 있게 하고,
#     · 반대로 너무 위험한 완화/레버리지 제안은 settings_ws.BotSettings 상한/하한으로 완전히 차단한다.
# - 장 흐름(최근 PnL/스킵 패턴 등)에 따라, 가드·리스크 파라미터를 동적으로 조정하는 AI 게이트웨이.
#
# 2025-11-17 변경 사항 (GPT 프롬프트 컨텍스트 강화)
# ----------------------------------------------------
# 1) ask_entry_decision 호출 전에 _build_extra_for_gpt(...) 로 extra 를 재구성.
#    - signal_source / direction 을 regime, direction 필드로 전달.
#    - guard_snapshot 의 핵심 가드 값(min_entry_volume_ratio, max_spread_pct 등)을
#      guard_snapshot 필드로 요약해 GPT 프롬프트에 포함.
#    - settings.recent_pnl_pct / settings.skip_streak 가 있으면 함께 전달해
#      최근 성과/스킵 패턴을 참고할 수 있게 함.
# 2) decide_entry_with_gpt_trader(...) docstring 에 프롬프트 연동 규칙을 보강.
#
# 사용 대상
# ----------------------------------------------------
# - entry_flow_ws.py (또는 entry_flow.py)의 try_open_new_position(...) 상단에서 호출.
#   예시 흐름:
#       settings = load_settings()
#       gpt_result = decide_entry_with_gpt_trader(settings, ...)
#       if gpt_result["final_action"] == "ENTER":
#           # gpt_result 에서 tp/sl/risk/guard_adjustments 를 읽어 주문/가드에 반영
#       else:
#           # 필요하면 gpt_result["sleep_after_sec"] 만큼 sleep 후 다음 루프
#
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from settings_ws import BotSettings
from gpt_decider import ask_entry_decision


# ─────────────────────────────────────────
# 내부 유틸 (텔레그램/로그)
# ─────────────────────────────────────────


def _safe_log(msg: str) -> None:
    """telelog.log 가 있으면 사용, 없으면 조용히 무시."""
    try:
        from telelog import log  # 지연 import

        log(msg)
    except Exception:
        pass


def _safe_tg(msg: str) -> None:
    """telelog.send_tg 가 있으면 사용, 없으면 무시."""
    try:
        from telelog import send_tg  # 지연 import

        send_tg(msg)
    except Exception:
        pass


# ─────────────────────────────────────────
# 가드 조정용 하드 범위 정의
# ─────────────────────────────────────────


@dataclass(frozen=True)
class GuardBounds:
    """GPT가 조정할 수 있는 일부 가드 값의 안전 범위.

    - 이 범위를 벗어나는 조정은 무시된다.
    - 기준값은 '실제로 써 본 값' 주변으로 보수적으로 설정.
    """

    # 최소 진입 거래량 비율 (최근 N캔들 평균 대비)
    min_entry_volume_ratio_min: float = 0.10
    min_entry_volume_ratio_max: float = 0.60

    # 진입 시 허용 스프레드 (비율)
    max_spread_pct_min: float = 0.0003
    max_spread_pct_max: float = 0.0015

    # 캔들 간 가격 점프 가드
    max_price_jump_pct_min: float = 0.0010
    max_price_jump_pct_max: float = 0.0040

    # depth imbalance (호가 쏠림) 최소 비율
    depth_imbalance_min_ratio_min: float = 1.5
    depth_imbalance_min_ratio_max: float = 4.0

    # depth imbalance 최소 노출 notional
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
    """GPT 응답(JSON)과 기본값을 합쳐 최종 TP/SL/리스크를 결정.

    - GPT가 값을 제안하면 그 값을 우선 사용하되, settings.gpt_max_* 로 상한을 걸고,
      0 이하/비정상 값은 버린다.
    - GPT가 해당 필드를 주지 않으면 기존 값을 그대로 사용 (단, 상한은 항상 적용).
    """

    # GPT 제안 값 or 기존 값
    tp_raw = float(gpt_json.get("tp_pct", base_tp_pct))
    sl_raw = float(gpt_json.get("sl_pct", base_sl_pct))
    risk_raw = float(gpt_json.get("effective_risk_pct", base_risk_pct))

    # 음수/0 보호 (0 이하이면 해당 필드는 "조정 없음" 으로 본다)
    tp_candidate = tp_raw if tp_raw > 0 else base_tp_pct
    sl_candidate = sl_raw if sl_raw > 0 else base_sl_pct
    risk_candidate = risk_raw if risk_raw > 0 else base_risk_pct

    # 상한 클램핑 (환경변수 없으면 기본 상한 사용)
    tp_clamped = min(tp_candidate, float(getattr(settings, "gpt_max_tp_pct", 0.10)))
    sl_clamped = min(sl_candidate, float(getattr(settings, "gpt_max_sl_pct", 0.05)))
    risk_clamped = min(
        risk_candidate,
        float(getattr(settings, "gpt_max_risk_pct", 0.03)),
    )

    # 리스크가 너무 작으면(예: 0.001 미만) 사실상 진입 의미가 없으므로,
    # 이 경우는 나중에 SKIP 처리할 수 있다.
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
    """GPT 응답의 guard_adjustments 필드를 해석해서, 허용 범위 내 조정만 돌려준다.

    반환 값 예시:
        {
          "min_entry_volume_ratio": 0.25,
          "max_spread_pct": 0.0010,
          ...
        }

    - guard_snapshot 은 현재 가드 설정(옵션)으로, GPT가 과하게 낮춰 버린 경우
      여기 기준으로 '조금만 완화/강화' 시킬 때 참고용이다.
      (지금 버전에서는 범위 체크에만 사용하고, 추후 프롬프트 튜닝 시 더 활용할 수 있다.)
    """

    adjustments: Dict[str, float] = {}

    raw_adj = gpt_json.get("guard_adjustments")
    if not isinstance(raw_adj, dict):
        return adjustments

    snap = guard_snapshot or {}

    # 1) min_entry_volume_ratio
    if "min_entry_volume_ratio" in raw_adj:
        try:
            val = float(raw_adj["min_entry_volume_ratio"])
            base = float(snap.get("min_entry_volume_ratio", settings.min_entry_volume_ratio))
            # 너무 과하게 올리면 진입 불능, 너무 낮추면 의미 없음 → 하드 범위 + 근처에서만 허용
            lo = GUARD_BOUNDS.min_entry_volume_ratio_min
            hi = GUARD_BOUNDS.min_entry_volume_ratio_max
            if lo <= val <= hi:
                # base 대비 ±0.2 이내에서만 허용 (과한 조정 방지)
                if abs(val - base) <= 0.2:
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
            if lo <= val <= hi:
                # base 대비 2배 이상 풀지 않게 제한
                if val <= base * 2.0:
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
            if lo <= val <= hi:
                if val <= base * 2.0:
                    adjustments["max_price_jump_pct"] = val
        except Exception:
            pass

    # 4) depth_imbalance_min_ratio
    if "depth_imbalance_min_ratio" in raw_adj:
        try:
            val = float(raw_adj["depth_imbalance_min_ratio"])
            base = float(snap.get("depth_imbalance_min_ratio", settings.depth_imbalance_min_ratio))
            lo = GUARD_BOUNDS.depth_imbalance_min_ratio_min
            hi = GUARD_BOUNDS.depth_imbalance_min_ratio_max
            if lo <= val <= hi:
                # 너무 낮추면 가드 무력화 → base의 0.5배 미만은 허용하지 않음
                if val >= base * 0.5:
                    adjustments["depth_imbalance_min_ratio"] = val
        except Exception:
            pass

    # 5) depth_imbalance_min_notional
    if "depth_imbalance_min_notional" in raw_adj:
        try:
            val = float(raw_adj["depth_imbalance_min_notional"])
            base = float(snap.get("depth_imbalance_min_notional", settings.depth_imbalance_min_notional))
            lo = GUARD_BOUNDS.depth_imbalance_min_notional_min
            hi = GUARD_BOUNDS.depth_imbalance_min_notional_max
            if lo <= val <= hi:
                if val >= base * 0.5:
                    adjustments["depth_imbalance_min_notional"] = val
        except Exception:
            pass

    if adjustments:
        _safe_log(f"[GPT_TRADER] guard adjustments accepted: {adjustments}")

    return adjustments


# ─────────────────────────────────────────
# GPT 프롬프트용 extra 컨텍스트 구성기
# ─────────────────────────────────────────


def _build_extra_for_gpt(
    settings: BotSettings,
    *,
    signal_source: str,
    direction: str,
    extra: Optional[Dict[str, Any]],
    guard_snapshot: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """ask_entry_decision 에 전달할 extra 컨텍스트를 구성한다.

    - 호출측 extra(dict)를 기본으로 사용.
    - 장 종류(TREND/RANGE/HYBRID)와 방향(LONG/SHORT)을 regime/direction 필드로 명시.
    - 현재 가드 설정(guard_snapshot)의 핵심 값들을 guard_snapshot 필드로 요약.
    - settings.recent_pnl_pct, settings.skip_streak 가 있으면 최근 성과/스킵 패턴도 포함.
    """

    payload: Dict[str, Any] = {}

    if isinstance(extra, dict):
        payload.update(extra)

    src = (signal_source or "").upper()
    if src and "regime" not in payload:
        payload["regime"] = src

    dir_up = (direction or "").upper()
    if dir_up and "direction" not in payload:
        payload["direction"] = dir_up

    # 현재 가드 스냅샷 요약
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
            # 기존 extra 에 동일 키가 있으면 병합
            if "guard_snapshot" in payload and isinstance(payload["guard_snapshot"], dict):
                merged = dict(payload["guard_snapshot"])
                merged.update(trimmed_guard)
                payload["guard_snapshot"] = merged
            else:
                payload["guard_snapshot"] = trimmed_guard

    # 최근 PnL / 스킵 패턴 (옵션)
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

    return payload


# ─────────────────────────────────────────
# 메인 엔트리: 진입 + 동적 가드 조절
# ─────────────────────────────────────────


def decide_entry_with_gpt_trader(
    settings: BotSettings,
    *,
    symbol: str,
    signal_source: str,  # "TREND" / "RANGE" / "HYBRID"
    direction: str,  # "LONG" / "SHORT"
    last_price: float,
    entry_score: Optional[float],
    base_risk_pct: float,
    base_tp_pct: float,
    base_sl_pct: float,
    extra: Optional[Dict[str, Any]] = None,
    guard_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """GPT + 하드 리스크/가드 조합으로 최종 진입 결정을 내린다.

    반환 예시:
        {
          "final_action": "ENTER" 또는 "SKIP",
          "gpt_action": "ENTER" | "SKIP" | "ADJUST" | "ERROR",
          "reason": "한국어 한 줄 설명 (가능하면 GPT 응답 기반)",
          "tp_pct": 0.006,
          "sl_pct": 0.004,
          "effective_risk_pct": 0.02,
          "guard_adjustments": {
              "min_entry_volume_ratio": 0.25,
              "max_spread_pct": 0.001
          },
          "sleep_after_sec": 3.0,   # SKIP 시 다음 루프 전 대기 시간 (옵션)
          "raw": { ... GPT full JSON ... }
        }

    중요한 점:
    - GPT 호출 실패 / JSON 이상 / 위험한 제안 등 모든 비정상 상황은
      → **final_action="SKIP"** 으로 통일 (폴백 진입 금지).
    - 리스크/TP/SL 은 항상 settings.gpt_max_* 으로 상한이 걸린 값만 사용.
    - extra / guard_snapshot 는 _build_extra_for_gpt(...) 에서 통합되어
      gpt_decider.ask_entry_decision 의 프롬프트에 들어간다.
      → GPT 가 장 흐름/가드 상태를 보고 진입 강도/리스크를 조절할 수 있게 하기 위한 것.
    """

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
        "raw": {},
    }

    # 1) GPT에 진입 의사결정 요청 (폴백 래퍼 사용 금지)
    extra_for_gpt = _build_extra_for_gpt(
        settings,
        signal_source=signal_source,
        direction=direction,
        extra=extra,
        guard_snapshot=guard_snapshot,
    )

    try:
        gpt_json = ask_entry_decision(
            symbol=symbol,
            signal_source=signal_source,
            chosen_signal=direction,
            last_price=last_price,
            entry_score=entry_score,
            effective_risk_pct=base_risk_pct,
            leverage=float(settings.leverage),
            tp_pct=base_tp_pct,
            sl_pct=base_sl_pct,
            extra=extra_for_gpt,
        )
        result["raw"] = gpt_json
    except Exception as e:
        # 폴백 진입 없이, 해당 루프는 단순 SKIP 처리
        msg = f"[GPT_TRADER] ask_entry_decision error → SKIP: {e}"
        _safe_log(msg)
        _safe_tg("⚠️ GPT 진입 판단 호출에 실패했습니다. 이번 루프는 진입 없이 건너뜁니다.")
        result["reason"] = "GPT 호출 오류로 진입 스킵"
        return result

    # 2) GPT action 정규화
    action_raw = str(gpt_json.get("action", "SKIP")).upper()
    if action_raw not in {"ENTER", "SKIP", "ADJUST"}:
        _safe_log(f"[GPT_TRADER] invalid action from GPT: {action_raw!r} → SKIP")
        result["reason"] = f"GPT가 알 수 없는 action을 반환했습니다: {action_raw!r}"
        result["sleep_after_sec"] = float(getattr(settings, "gpt_skip_sleep_sec", 3.0))
        return result

    result["gpt_action"] = action_raw

    # 3) 리스크/TP/SL 클램핑
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

    # 리스크가 사실상 0이면, 진입은 의미 없으므로 강제 SKIP
    if risk_params.effective_risk_pct <= 0.0:
        result["final_action"] = "SKIP"
        result["reason"] = "GPT가 리스크를 0% 이하로 제안했거나, 상한/하한 처리 후 0%가 되었습니다."
        result["sleep_after_sec"] = float(getattr(settings, "gpt_skip_sleep_sec", 3.0))
        _safe_log("[GPT_TRADER] effective_risk_pct <= 0 → SKIP")
        return result

    # 4) 가드 조정 적용 (옵션)
    guard_adj = _apply_guard_adjustments(
        settings,
        guard_snapshot=guard_snapshot,
        gpt_json=gpt_json,
    )
    result["guard_adjustments"] = guard_adj

    # 5) 최종 액션 결정
    #    - ENTER / ADJUST 둘 다 "진입 허용" 으로 본다.
    #    - SKIP 은 그대로 SKIP.
    if action_raw in {"ENTER", "ADJUST"}:
        result["final_action"] = "ENTER"
        result["sleep_after_sec"] = 0.0
    else:
        result["final_action"] = "SKIP"
        result["sleep_after_sec"] = float(getattr(settings, "gpt_skip_sleep_sec", 3.0))

    reason = gpt_json.get("reason") or "GPT 판단 결과 반영"
    result["reason"] = str(reason)

    # 6) 요약 로그
    try:
        _safe_log(
            "[GPT_TRADER] final_action={} gpt_action={} tp={} sl={} risk={} guards={} symbol={} src={} dir={}".format(
                result["final_action"],
                result["gpt_action"],
                result["tp_pct"],
                result["sl_pct"],
                result["effective_risk_pct"],
                result["guard_adjustments"],
                symbol,
                signal_source,
                direction,
            )
        )
    except Exception:
        pass

    return result


__all__ = [
    "decide_entry_with_gpt_trader",
]
