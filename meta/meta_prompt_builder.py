# meta/meta_prompt_builder.py
"""
========================================================
FILE: meta/meta_prompt_builder.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할
- Meta Strategy(레벨3)용 GPT 입력 프롬프트(system) + user_payload(JSON)를 생성한다.
- 이 모듈은 "프롬프트 구성"만 한다. 매매/주문/TP·SL 결정은 절대 하지 않는다.

절대 원칙 (STRICT · NO-FALLBACK)
- 입력(stats/current_settings) 누락/불일치/형식 오류는 즉시 예외.
- None→0/빈값 대체/조용한 continue 금지.
- 민감정보(API 키/토큰/DB URL 등)는 payload/로그에 포함 금지.
- 환경 변수 직접 접근 금지(os.getenv 금지). settings.py를 직접 읽지도 않는다(호출자가 현재 설정 스냅샷 전달).

변경 이력
--------------------------------------------------------
- 2026-03-03:
  1) 신규 생성: Meta Strategy(레벨3) 프롬프트 빌더
  2) 출력 JSON 스키마 고정 + 허용 범위/금지 행위(주문/TP·SL/진입·청산) 강제
  3) stats/current_settings STRICT 검증 + payload 크기 제한(최근 트레이드 상한)
========================================================
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from common.strict_validators import (
    StrictValidationError,
    normalize_symbol,
    require_bool,
    require_dict,
    require_float,
    require_int,
    require_list,
    require_nonempty_str,
)


class MetaPromptBuilderError(RuntimeError):
    """STRICT: meta prompt build failure."""


@dataclass(frozen=True, slots=True)
class MetaPromptConfig:
    """
    프롬프트 생성 옵션(STRICT).
    - max_recent_trades: user_payload에 포함할 최근 트레이드 최대 개수
    - language: GPT가 작성할 언어
    - max_output_sentences: rationale_short의 문장 제한(시청자/운영자용)
    """
    max_recent_trades: int = 20
    language: str = "ko"
    max_output_sentences: int = 3
    prompt_version: str = "2026-03-03"


# ─────────────────────────────────────────────
# STRICT helpers
# ─────────────────────────────────────────────
def _fail(reason: str, exc: Optional[BaseException] = None) -> None:
    if exc is None:
        raise MetaPromptBuilderError(reason)
    raise MetaPromptBuilderError(reason) from exc


def _is_finite_number(v: Any) -> bool:
    try:
        x = float(v)
    except Exception:
        return False
    return math.isfinite(x)


def _json_dumps_strict(obj: Dict[str, Any]) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    except Exception as e:
        _fail("user_payload json serialization failed (STRICT)", e)
    raise AssertionError("unreachable")


def _pick_first(meta: Dict[str, Any], keys: Tuple[str, ...]) -> Any:
    for k in keys:
        if k in meta:
            return meta[k]
    return None


def _require_stats_shape(stats: Dict[str, Any]) -> None:
    """
    STRICT: meta_stats_aggregator.build_meta_stats_dict()의 결과 포맷을 전제로 최소 키를 강제한다.
    """
    # top-level required keys
    for k in (
        "symbol",
        "generated_at_utc",
        "n_trades",
        "wins",
        "losses",
        "breakevens",
        "win_rate_pct",
        "total_pnl_usdt",
        "avg_pnl_usdt",
        "recent_win_rate_pct",
        "baseline_win_rate_pct",
        "recent_avg_pnl_usdt",
        "baseline_avg_pnl_usdt",
        "by_direction",
        "by_regime",
        "by_engine_total_bucket",
        "by_entry_score_bucket",
        "recent_trades",
    ):
        if k not in stats:
            _fail(f"stats missing required key (STRICT): {k}")

    # basic type checks
    _ = normalize_symbol(stats["symbol"], name="stats.symbol")
    require_nonempty_str(stats["generated_at_utc"], "stats.generated_at_utc")

    require_int(stats["n_trades"], "stats.n_trades", min_value=1)
    require_int(stats["wins"], "stats.wins", min_value=0)
    require_int(stats["losses"], "stats.losses", min_value=0)
    require_int(stats["breakevens"], "stats.breakevens", min_value=0)

    require_float(stats["win_rate_pct"], "stats.win_rate_pct", min_value=0.0, max_value=100.0)
    require_float(stats["total_pnl_usdt"], "stats.total_pnl_usdt")
    require_float(stats["avg_pnl_usdt"], "stats.avg_pnl_usdt")

    require_float(stats["recent_win_rate_pct"], "stats.recent_win_rate_pct", min_value=0.0, max_value=100.0)
    require_float(stats["baseline_win_rate_pct"], "stats.baseline_win_rate_pct", min_value=0.0, max_value=100.0)
    require_float(stats["recent_avg_pnl_usdt"], "stats.recent_avg_pnl_usdt")
    require_float(stats["baseline_avg_pnl_usdt"], "stats.baseline_avg_pnl_usdt")

    require_dict(stats["by_direction"], "stats.by_direction", non_empty=True)
    require_dict(stats["by_regime"], "stats.by_regime", non_empty=True)
    require_dict(stats["by_engine_total_bucket"], "stats.by_engine_total_bucket", non_empty=True)
    require_dict(stats["by_entry_score_bucket"], "stats.by_entry_score_bucket", non_empty=True)
    require_list(stats["recent_trades"], "stats.recent_trades", non_empty=True)


def _sanitize_recent_trades_strict(recent_trades: List[Dict[str, Any]], *, max_n: int) -> List[Dict[str, Any]]:
    """
    STRICT:
    - 최근 트레이드 payload를 max_n으로 제한한다(표현 제한이지, 폴백이 아님).
    - 각 row 최소 필드 검증.
    """
    if max_n <= 0:
        _fail("config.max_recent_trades must be > 0 (STRICT)")
    out: List[Dict[str, Any]] = []
    for i, r in enumerate(recent_trades[:max_n]):
        if not isinstance(r, dict):
            _fail(f"recent_trades[{i}] must be dict (STRICT)")
        # 최소 필드
        for k in ("trade_id", "entry_ts", "exit_ts", "direction", "regime", "pnl_usdt"):
            if k not in r:
                _fail(f"recent_trades[{i}] missing key (STRICT): {k}")

        tid = require_int(r["trade_id"], f"recent_trades[{i}].trade_id", min_value=1)
        require_nonempty_str(r["entry_ts"], f"recent_trades[{i}].entry_ts")
        require_nonempty_str(r["exit_ts"], f"recent_trades[{i}].exit_ts")

        direction = require_nonempty_str(r["direction"], f"recent_trades[{i}].direction").upper()
        if direction not in ("LONG", "SHORT"):
            _fail(f"recent_trades[{i}].direction invalid (STRICT): {direction!r}")

        regime = require_nonempty_str(r["regime"], f"recent_trades[{i}].regime")
        pnl = require_float(r["pnl_usdt"], f"recent_trades[{i}].pnl_usdt")

        # 선택 필드는 있으면 검증(없어도 대체값 주입 금지)
        def opt_num(key: str) -> Optional[float]:
            if key not in r or r.get(key) is None:
                return None
            if not _is_finite_number(r.get(key)):
                _fail(f"recent_trades[{i}].{key} must be finite (STRICT)")
            return float(r.get(key))

        out.append(
            {
                "trade_id": int(tid),
                "entry_ts": str(r["entry_ts"]),
                "exit_ts": str(r["exit_ts"]),
                "direction": direction,
                "regime": str(regime),
                "pnl_usdt": float(pnl),
                "pnl_pct_futures": opt_num("pnl_pct_futures"),
                "engine_total": opt_num("engine_total"),
                "entry_score": opt_num("entry_score"),
                "dd_pct": opt_num("dd_pct"),
                "micro_score_risk": opt_num("micro_score_risk"),
                "ev_cell_status": (str(r.get("ev_cell_status")).strip() if r.get("ev_cell_status") is not None else None),
                "auto_blocked": (bool(r.get("auto_blocked")) if r.get("auto_blocked") is not None else None),
                "auto_risk_multiplier": opt_num("auto_risk_multiplier"),
                "close_reason": (str(r.get("close_reason")).strip() if r.get("close_reason") is not None else None),
            }
        )
    if not out:
        _fail("recent_trades sanitized empty (STRICT)")
    return out


def _sanitize_settings_snapshot_strict(current_settings: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    STRICT:
    - settings.py를 직접 읽지 않는다(호출자가 전달한 스냅샷만 사용).
    - 민감정보가 섞일 수 있는 키는 금지한다(예: *_key, *_secret, *_url).
    - 필요한 운영 파라미터만 화이트리스트로 추려 포함한다.
    """
    if current_settings is None:
        return None
    if not isinstance(current_settings, dict) or not current_settings:
        _fail("current_settings must be non-empty dict when provided (STRICT)")

    # 금지 키 패턴(민감정보) — 포함 시 즉시 예외
    banned_substrings = ("key", "secret", "token", "password", "url")
    for k in current_settings.keys():
        kk = str(k).lower()
        if any(b in kk for b in banned_substrings):
            _fail(f"current_settings contains sensitive key (STRICT): {k}")

    # 화이트리스트(메타 조정 대상으로만 한정)
    allowed_keys = {
        # risk/operations
        "allocation_ratio",
        "leverage",
        "max_spread_pct",
        "max_price_jump_pct",
        "min_entry_volume_ratio",
        "slippage_block_pct",
        "reconcile_interval_sec",
        "force_close_on_desync",
        # timeouts/latency budgets
        "max_signal_latency_ms",
        "max_exec_latency_ms",
        # exit supervisor toggles (optional)
        "exit_supervisor_enabled",
        "exit_supervisor_cooldown_sec",
    }

    out: Dict[str, Any] = {}
    for k in allowed_keys:
        if k in current_settings:
            v = current_settings.get(k)
            # 타입/범위 최소 검증(없는 건 채우지 않음)
            if k in ("force_close_on_desync", "exit_supervisor_enabled"):
                out[k] = require_bool(v, f"current_settings.{k}")
            elif k in ("reconcile_interval_sec", "max_signal_latency_ms", "max_exec_latency_ms"):
                out[k] = require_int(v, f"current_settings.{k}", min_value=1)
            else:
                # numeric
                out[k] = require_float(v, f"current_settings.{k}")
    return out if out else None


# ─────────────────────────────────────────────
# System prompt builder (STRICT)
# ─────────────────────────────────────────────
def build_meta_system_prompt(cfg: MetaPromptConfig) -> str:
    lang = require_nonempty_str(cfg.language, "cfg.language")
    max_sent = require_int(cfg.max_output_sentences, "cfg.max_output_sentences", min_value=1, max_value=10)
    pv = require_nonempty_str(cfg.prompt_version, "cfg.prompt_version")

    # 출력 스키마 고정(파싱/검증 단순화)
    return f"""\
You are the Meta Strategy Analyst for a STRICT · NO-FALLBACK automated trading engine.

NON-NEGOTIABLE RULES:
- You MUST NOT decide ENTER/EXIT/HOLD for any single trade.
- You MUST NOT propose TP/SL values.
- You MUST NOT propose leverage changes beyond a safe multiplier recommendation (no hard leverage targets).
- You MUST output EXACTLY ONE JSON object and NOTHING ELSE.
- No markdown, no code fences, no extra text.

Your job:
- Diagnose strategy drift and risk regime mismatch using the provided statistics.
- Recommend ONLY SAFE, HIGH-LEVEL parameter adjustments that the engine can apply with validation.

Allowed recommendation types:
1) "NO_CHANGE"
2) "ADJUST_PARAMS"
3) "RECOMMEND_SAFE_STOP"

Output JSON schema (MUST match):
{{
  "version": "{pv}",
  "action": "NO_CHANGE" | "ADJUST_PARAMS" | "RECOMMEND_SAFE_STOP",
  "severity": 0 | 1 | 2 | 3,
  "tags": [string, ...],
  "confidence": number,              // 0.0..1.0
  "ttl_sec": integer,                // 60..86400 (how long this recommendation should be considered valid)
  "recommendation": {{
    "risk_multiplier_delta": number, // -0.50..+0.50 (final multiplier = clamp(0,1, base + delta))
    "allocation_ratio_cap": number,  // 0.00..1.00 (cap, not a target)
    "disable_directions": [ "LONG" | "SHORT" ],   // optional
    "guard_multipliers": {{
      "max_spread_pct_mult": number,      // 0.50..2.00
      "max_price_jump_pct_mult": number,  // 0.50..2.00
      "min_entry_volume_ratio_mult": number // 0.50..2.00
    }}
  }},
  "rationale_short": string           // <= {max_sent} sentences, language={lang}
}}

Constraints:
- If action is "NO_CHANGE", recommendation fields MUST still exist but must be neutral:
  risk_multiplier_delta=0, allocation_ratio_cap=1, disable_directions=[], guard_multipliers all = 1.
- If action is "RECOMMEND_SAFE_STOP", set severity>=2 and explain why (drift/instability).
- Use tags like: "NEGATIVE_DRIFT", "VOLATILITY_SHIFT", "REGIME_MISMATCH", "EXECUTION_ISSUE", "DATA_QUALITY", "OVERTRADING".
- Keep rationale short and operational.

Now read the provided JSON payload and respond with the single JSON object only.
"""


# ─────────────────────────────────────────────
# User payload builder (STRICT)
# ─────────────────────────────────────────────
def build_meta_user_payload(
    *,
    stats: Dict[str, Any],
    cfg: MetaPromptConfig,
    current_settings: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    require_dict(stats, "stats", non_empty=True)
    _require_stats_shape(stats)

    sym = normalize_symbol(stats["symbol"], name="stats.symbol")
    max_recent = require_int(cfg.max_recent_trades, "cfg.max_recent_trades", min_value=1, max_value=200)

    recent_trades_raw = require_list(stats["recent_trades"], "stats.recent_trades", non_empty=True)
    recent_trades = _sanitize_recent_trades_strict(recent_trades_raw, max_n=max_recent)

    # breakdowns: dict형만 확인(세부 검증은 meta_decision_validator에서 수행)
    by_direction = require_dict(stats["by_direction"], "stats.by_direction", non_empty=True)
    by_regime = require_dict(stats["by_regime"], "stats.by_regime", non_empty=True)
    by_engine_bucket = require_dict(stats["by_engine_total_bucket"], "stats.by_engine_total_bucket", non_empty=True)
    by_entry_bucket = require_dict(stats["by_entry_score_bucket"], "stats.by_entry_score_bucket", non_empty=True)

    settings_snapshot = _sanitize_settings_snapshot_strict(current_settings)

    payload: Dict[str, Any] = {
        "meta_type": "meta_strategy_stats",
        "symbol": sym,
        "generated_at_utc": require_nonempty_str(stats["generated_at_utc"], "stats.generated_at_utc"),

        "headline": {
            "n_trades": require_int(stats["n_trades"], "stats.n_trades", min_value=1),
            "win_rate_pct": require_float(stats["win_rate_pct"], "stats.win_rate_pct", min_value=0.0, max_value=100.0),
            "total_pnl_usdt": require_float(stats["total_pnl_usdt"], "stats.total_pnl_usdt"),
            "avg_pnl_usdt": require_float(stats["avg_pnl_usdt"], "stats.avg_pnl_usdt"),
            "wins": require_int(stats["wins"], "stats.wins", min_value=0),
            "losses": require_int(stats["losses"], "stats.losses", min_value=0),
            "breakevens": require_int(stats["breakevens"], "stats.breakevens", min_value=0),
        },

        "drift": {
            "recent_win_rate_pct": require_float(stats["recent_win_rate_pct"], "stats.recent_win_rate_pct", min_value=0.0, max_value=100.0),
            "baseline_win_rate_pct": require_float(stats["baseline_win_rate_pct"], "stats.baseline_win_rate_pct", min_value=0.0, max_value=100.0),
            "recent_avg_pnl_usdt": require_float(stats["recent_avg_pnl_usdt"], "stats.recent_avg_pnl_usdt"),
            "baseline_avg_pnl_usdt": require_float(stats["baseline_avg_pnl_usdt"], "stats.baseline_avg_pnl_usdt"),
        },

        "breakdowns": {
            "by_direction": dict(by_direction),
            "by_regime": dict(by_regime),
            "by_engine_total_bucket": dict(by_engine_bucket),
            "by_entry_score_bucket": dict(by_entry_bucket),
        },

        "recent_trades": recent_trades,

        # 안전/정책: GPT가 지켜야 할 제한을 payload에도 중복 명시(프롬프트 무시 대비)
        "constraints": {
            "no_trade_decision": True,
            "no_tp_sl": True,
            "no_direct_leverage_target": True,
            "allowed_actions": ["NO_CHANGE", "ADJUST_PARAMS", "RECOMMEND_SAFE_STOP"],
        },

        "current_settings_snapshot": settings_snapshot,  # 없으면 None(대체값 주입 금지)
    }

    # STRICT: payload 직렬화 가능 여부를 여기서 확정
    _ = _json_dumps_strict(payload)
    return payload


def build_meta_prompts(
    *,
    stats: Dict[str, Any],
    cfg: Optional[MetaPromptConfig] = None,
    current_settings: Optional[Dict[str, Any]] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    반환:
    - system_prompt(str)
    - user_payload(dict)
    """
    c = cfg if cfg is not None else MetaPromptConfig()
    system_prompt = build_meta_system_prompt(c)
    user_payload = build_meta_user_payload(stats=stats, cfg=c, current_settings=current_settings)
    return system_prompt, user_payload


__all__ = [
    "MetaPromptBuilderError",
    "MetaPromptConfig",
    "build_meta_system_prompt",
    "build_meta_user_payload",
    "build_meta_prompts",
]