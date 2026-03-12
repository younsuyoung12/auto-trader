"""
========================================================
FILE: meta/meta_prompt_builder.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할
- Meta Strategy(레벨3)용 GPT 입력 프롬프트(system) + user_payload(JSON)를 생성한다.
- 이 모듈은 "프롬프트 구성"만 한다. 매매/주문/TP·SL 결정은 절대 하지 않는다.

절대 원칙 (STRICT · NO-FALLBACK)
- 입력(stats/current_settings/cfg) 누락/불일치/형식 오류는 즉시 예외.
- None→0/빈값 대체/조용한 continue 금지.
- 민감정보(API 키/토큰/DB URL 등)는 payload/로그에 포함 금지.
- 환경 변수 직접 접근 금지(os.getenv 금지).
- settings.py를 직접 읽지 않는다. 호출자가 현재 설정 스냅샷을 전달한다.

변경 이력
--------------------------------------------------------
- 2026-03-13:
  1) FIX(EXCEPTION): common.exceptions_strict 공통 예외 계층 적용
  2) FIX(STRICT): cfg=None 시 MetaPromptConfig() 자동 생성 fallback 제거
  3) FIX(ARCH): common.strict_validators 의존 제거, 이 파일 내부 strict helper로 계약 고정
  4) FIX(CONTRACT): stats/current_settings/recent_trades/payload serialization 검증 강화
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

from common.exceptions_strict import (
    StrictConfigError,
    StrictDataError,
    StrictStateError,
)


class MetaPromptBuilderError(RuntimeError):
    """STRICT: meta prompt build failure."""


class MetaPromptBuilderConfigError(StrictConfigError):
    """프롬프트 설정/설정 스냅샷 계약 위반."""


class MetaPromptBuilderContractError(StrictDataError):
    """stats / payload / prompt 계약 위반."""


class MetaPromptBuilderStateError(StrictStateError):
    """프롬프트 빌더 내부 상태/불변식 위반."""


@dataclass(frozen=True, slots=True)
class MetaPromptConfig:
    """
    프롬프트 생성 옵션(STRICT).
    - max_recent_trades: user_payload에 포함할 최근 트레이드 최대 개수
    - language: GPT가 작성할 언어
    - max_output_sentences: rationale_short의 문장 제한
    """
    max_recent_trades: int
    language: str
    max_output_sentences: int
    prompt_version: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "max_recent_trades",
            _require_int(
                self.max_recent_trades,
                "cfg.max_recent_trades",
                min_value=1,
                max_value=200,
                error_cls=MetaPromptBuilderConfigError,
            ),
        )
        object.__setattr__(
            self,
            "language",
            _require_nonempty_str(
                self.language,
                "cfg.language",
                error_cls=MetaPromptBuilderConfigError,
            ),
        )
        object.__setattr__(
            self,
            "max_output_sentences",
            _require_int(
                self.max_output_sentences,
                "cfg.max_output_sentences",
                min_value=1,
                max_value=10,
                error_cls=MetaPromptBuilderConfigError,
            ),
        )
        object.__setattr__(
            self,
            "prompt_version",
            _require_nonempty_str(
                self.prompt_version,
                "cfg.prompt_version",
                error_cls=MetaPromptBuilderConfigError,
            ),
        )


def _fail_config(reason: str, exc: Optional[BaseException] = None) -> None:
    if exc is None:
        raise MetaPromptBuilderConfigError(reason)
    raise MetaPromptBuilderConfigError(reason) from exc


def _fail_contract(reason: str, exc: Optional[BaseException] = None) -> None:
    if exc is None:
        raise MetaPromptBuilderContractError(reason)
    raise MetaPromptBuilderContractError(reason) from exc


def _fail_state(reason: str, exc: Optional[BaseException] = None) -> None:
    if exc is None:
        raise MetaPromptBuilderStateError(reason)
    raise MetaPromptBuilderStateError(reason) from exc


def _require_nonempty_str(v: Any, name: str, *, error_cls=MetaPromptBuilderContractError) -> str:
    if not isinstance(v, str):
        raise error_cls(f"{name} must be str (STRICT), got={type(v).__name__}")
    s = v.strip()
    if not s:
        raise error_cls(f"{name} must not be empty (STRICT)")
    return s


def _require_int(
    v: Any,
    name: str,
    *,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
    error_cls=MetaPromptBuilderContractError,
) -> int:
    if v is None:
        raise error_cls(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        raise error_cls(f"{name} must be int (bool not allowed) (STRICT)")
    try:
        iv = int(v)
    except Exception as exc:
        raise error_cls(f"{name} must be int (STRICT): {exc}") from exc
    if min_value is not None and iv < min_value:
        raise error_cls(f"{name} must be >= {min_value} (STRICT)")
    if max_value is not None and iv > max_value:
        raise error_cls(f"{name} must be <= {max_value} (STRICT)")
    return iv


def _require_float(
    v: Any,
    name: str,
    *,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
    error_cls=MetaPromptBuilderContractError,
) -> float:
    if v is None:
        raise error_cls(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        raise error_cls(f"{name} must be float (bool not allowed) (STRICT)")
    try:
        fv = float(v)
    except Exception as exc:
        raise error_cls(f"{name} must be float (STRICT): {exc}") from exc
    if not math.isfinite(fv):
        raise error_cls(f"{name} must be finite (STRICT)")
    if min_value is not None and fv < min_value:
        raise error_cls(f"{name} must be >= {min_value} (STRICT)")
    if max_value is not None and fv > max_value:
        raise error_cls(f"{name} must be <= {max_value} (STRICT)")
    return float(fv)


def _require_bool(v: Any, name: str, *, error_cls=MetaPromptBuilderContractError) -> bool:
    if not isinstance(v, bool):
        raise error_cls(f"{name} must be bool (STRICT)")
    return bool(v)


def _require_dict(v: Any, name: str, *, non_empty: bool = False, error_cls=MetaPromptBuilderContractError) -> Dict[str, Any]:
    if not isinstance(v, dict):
        raise error_cls(f"{name} must be dict (STRICT)")
    if non_empty and not v:
        raise error_cls(f"{name} must not be empty (STRICT)")
    return dict(v)


def _require_list(v: Any, name: str, *, non_empty: bool = False, error_cls=MetaPromptBuilderContractError) -> List[Any]:
    if not isinstance(v, list):
        raise error_cls(f"{name} must be list (STRICT)")
    if non_empty and not v:
        raise error_cls(f"{name} must not be empty (STRICT)")
    return list(v)


def _normalize_symbol(v: Any, name: str) -> str:
    s = str(v or "").replace("-", "").replace("/", "").upper().strip()
    if not s:
        _fail_contract(f"{name} is required (STRICT)")
    return s


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
        _fail_contract("user_payload json serialization failed (STRICT)", e)
    raise AssertionError("unreachable")


def _require_stats_shape(stats: Dict[str, Any]) -> None:
    """
    STRICT: meta_stats_aggregator.build_meta_stats_dict() 결과 최소 키 강제
    """
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
            _fail_contract(f"stats missing required key (STRICT): {k}")

    _ = _normalize_symbol(stats["symbol"], "stats.symbol")
    _require_nonempty_str(stats["generated_at_utc"], "stats.generated_at_utc")

    _require_int(stats["n_trades"], "stats.n_trades", min_value=1)
    _require_int(stats["wins"], "stats.wins", min_value=0)
    _require_int(stats["losses"], "stats.losses", min_value=0)
    _require_int(stats["breakevens"], "stats.breakevens", min_value=0)

    _require_float(stats["win_rate_pct"], "stats.win_rate_pct", min_value=0.0, max_value=100.0)
    _require_float(stats["total_pnl_usdt"], "stats.total_pnl_usdt")
    _require_float(stats["avg_pnl_usdt"], "stats.avg_pnl_usdt")

    _require_float(stats["recent_win_rate_pct"], "stats.recent_win_rate_pct", min_value=0.0, max_value=100.0)
    _require_float(stats["baseline_win_rate_pct"], "stats.baseline_win_rate_pct", min_value=0.0, max_value=100.0)
    _require_float(stats["recent_avg_pnl_usdt"], "stats.recent_avg_pnl_usdt")
    _require_float(stats["baseline_avg_pnl_usdt"], "stats.baseline_avg_pnl_usdt")

    _require_dict(stats["by_direction"], "stats.by_direction", non_empty=True)
    _require_dict(stats["by_regime"], "stats.by_regime", non_empty=True)
    _require_dict(stats["by_engine_total_bucket"], "stats.by_engine_total_bucket", non_empty=True)
    _require_dict(stats["by_entry_score_bucket"], "stats.by_entry_score_bucket", non_empty=True)
    _require_list(stats["recent_trades"], "stats.recent_trades", non_empty=True)


def _sanitize_recent_trades_strict(recent_trades: List[Dict[str, Any]], *, max_n: int) -> List[Dict[str, Any]]:
    """
    STRICT:
    - 최근 트레이드 payload를 max_n으로 제한한다(표현 제한이지, 폴백이 아님).
    - 각 row 최소 필드 검증.
    """
    if max_n <= 0:
        _fail_config("cfg.max_recent_trades must be > 0 (STRICT)")

    out: List[Dict[str, Any]] = []
    for i, r in enumerate(recent_trades[:max_n]):
        if not isinstance(r, dict):
            _fail_contract(f"recent_trades[{i}] must be dict (STRICT)")

        for k in ("trade_id", "entry_ts", "exit_ts", "direction", "regime", "pnl_usdt"):
            if k not in r:
                _fail_contract(f"recent_trades[{i}] missing key (STRICT): {k}")

        tid = _require_int(r["trade_id"], f"recent_trades[{i}].trade_id", min_value=1)
        entry_ts = _require_nonempty_str(r["entry_ts"], f"recent_trades[{i}].entry_ts")
        exit_ts = _require_nonempty_str(r["exit_ts"], f"recent_trades[{i}].exit_ts")

        direction = _require_nonempty_str(r["direction"], f"recent_trades[{i}].direction").upper()
        if direction not in ("LONG", "SHORT"):
            _fail_contract(f"recent_trades[{i}].direction invalid (STRICT): {direction!r}")

        regime = _require_nonempty_str(r["regime"], f"recent_trades[{i}].regime")
        pnl = _require_float(r["pnl_usdt"], f"recent_trades[{i}].pnl_usdt")

        def opt_num(key: str) -> Optional[float]:
            if key not in r or r.get(key) is None:
                return None
            if not _is_finite_number(r.get(key)):
                _fail_contract(f"recent_trades[{i}].{key} must be finite (STRICT)")
            return float(r.get(key))

        def opt_str(key: str) -> Optional[str]:
            if key not in r or r.get(key) is None:
                return None
            return _require_nonempty_str(r.get(key), f"recent_trades[{i}].{key}")

        def opt_bool(key: str) -> Optional[bool]:
            if key not in r or r.get(key) is None:
                return None
            return _require_bool(r.get(key), f"recent_trades[{i}].{key}")

        out.append(
            {
                "trade_id": int(tid),
                "entry_ts": entry_ts,
                "exit_ts": exit_ts,
                "direction": direction,
                "regime": regime,
                "pnl_usdt": float(pnl),
                "pnl_pct_futures": opt_num("pnl_pct_futures"),
                "engine_total": opt_num("engine_total"),
                "entry_score": opt_num("entry_score"),
                "dd_pct": opt_num("dd_pct"),
                "micro_score_risk": opt_num("micro_score_risk"),
                "ev_cell_status": opt_str("ev_cell_status"),
                "auto_blocked": opt_bool("auto_blocked"),
                "auto_risk_multiplier": opt_num("auto_risk_multiplier"),
                "close_reason": opt_str("close_reason"),
            }
        )

    if not out:
        _fail_contract("recent_trades sanitized empty (STRICT)")
    return out


def _sanitize_settings_snapshot_strict(current_settings: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    STRICT:
    - settings.py를 직접 읽지 않는다(호출자가 전달한 스냅샷만 사용).
    - 민감정보가 섞일 수 있는 키는 금지한다.
    - 필요한 운영 파라미터만 화이트리스트로 추려 포함한다.
    """
    if current_settings is None:
        return None

    raw = _require_dict(current_settings, "current_settings", non_empty=True)

    banned_substrings = ("key", "secret", "token", "password", "url")
    for k in raw.keys():
        kk = str(k).lower()
        if any(b in kk for b in banned_substrings):
            _fail_contract(f"current_settings contains sensitive key (STRICT): {k}")

    allowed_keys = {
        "allocation_ratio",
        "leverage",
        "max_spread_pct",
        "max_price_jump_pct",
        "min_entry_volume_ratio",
        "slippage_block_pct",
        "reconcile_interval_sec",
        "force_close_on_desync",
        "max_signal_latency_ms",
        "max_exec_latency_ms",
        "exit_supervisor_enabled",
        "exit_supervisor_cooldown_sec",
    }

    out: Dict[str, Any] = {}
    for k in allowed_keys:
        if k not in raw:
            continue
        v = raw[k]

        if k in ("force_close_on_desync", "exit_supervisor_enabled"):
            out[k] = _require_bool(v, f"current_settings.{k}")
        elif k in ("reconcile_interval_sec", "max_signal_latency_ms", "max_exec_latency_ms"):
            out[k] = _require_int(v, f"current_settings.{k}", min_value=1)
        else:
            out[k] = _require_float(v, f"current_settings.{k}")

    return out if out else None


def build_meta_system_prompt(cfg: MetaPromptConfig) -> str:
    if not isinstance(cfg, MetaPromptConfig):
        _fail_config("cfg must be MetaPromptConfig (STRICT)")

    lang = _require_nonempty_str(cfg.language, "cfg.language", error_cls=MetaPromptBuilderConfigError)
    max_sent = _require_int(
        cfg.max_output_sentences,
        "cfg.max_output_sentences",
        min_value=1,
        max_value=10,
        error_cls=MetaPromptBuilderConfigError,
    )
    pv = _require_nonempty_str(cfg.prompt_version, "cfg.prompt_version", error_cls=MetaPromptBuilderConfigError)

    return f"""You are the Meta Strategy Analyst for a STRICT · NO-FALLBACK automated trading engine.

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
  "confidence": number,
  "ttl_sec": integer,
  "recommendation": {{
    "risk_multiplier_delta": number,
    "allocation_ratio_cap": number,
    "disable_directions": [ "LONG" | "SHORT" ],
    "guard_multipliers": {{
      "max_spread_pct_mult": number,
      "max_price_jump_pct_mult": number,
      "min_entry_volume_ratio_mult": number
    }}
  }},
  "rationale_short": string
}}

Constraints:
- If action is "NO_CHANGE", recommendation fields MUST still exist but must be neutral:
  risk_multiplier_delta=0, allocation_ratio_cap=1, disable_directions=[], guard_multipliers all = 1.
- If action is "RECOMMEND_SAFE_STOP", set severity>=2 and explain why.
- Use tags like: "NEGATIVE_DRIFT", "VOLATILITY_SHIFT", "REGIME_MISMATCH", "EXECUTION_ISSUE", "DATA_QUALITY", "OVERTRADING".
- Keep rationale short and operational.
- language={lang}
- rationale_short <= {max_sent} sentences

Now read the provided JSON payload and respond with the single JSON object only.
"""


def build_meta_user_payload(
    *,
    stats: Dict[str, Any],
    cfg: MetaPromptConfig,
    current_settings: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if not isinstance(cfg, MetaPromptConfig):
        _fail_config("cfg must be MetaPromptConfig (STRICT)")

    stats_dict = _require_dict(stats, "stats", non_empty=True)
    _require_stats_shape(stats_dict)

    sym = _normalize_symbol(stats_dict["symbol"], "stats.symbol")
    max_recent = _require_int(
        cfg.max_recent_trades,
        "cfg.max_recent_trades",
        min_value=1,
        max_value=200,
        error_cls=MetaPromptBuilderConfigError,
    )

    recent_trades_raw = _require_list(stats_dict["recent_trades"], "stats.recent_trades", non_empty=True)
    recent_trades = _sanitize_recent_trades_strict(recent_trades_raw, max_n=max_recent)

    by_direction = _require_dict(stats_dict["by_direction"], "stats.by_direction", non_empty=True)
    by_regime = _require_dict(stats_dict["by_regime"], "stats.by_regime", non_empty=True)
    by_engine_bucket = _require_dict(stats_dict["by_engine_total_bucket"], "stats.by_engine_total_bucket", non_empty=True)
    by_entry_bucket = _require_dict(stats_dict["by_entry_score_bucket"], "stats.by_entry_score_bucket", non_empty=True)

    settings_snapshot = _sanitize_settings_snapshot_strict(current_settings)

    payload: Dict[str, Any] = {
        "meta_type": "meta_strategy_stats",
        "symbol": sym,
        "generated_at_utc": _require_nonempty_str(stats_dict["generated_at_utc"], "stats.generated_at_utc"),
        "headline": {
            "n_trades": _require_int(stats_dict["n_trades"], "stats.n_trades", min_value=1),
            "win_rate_pct": _require_float(stats_dict["win_rate_pct"], "stats.win_rate_pct", min_value=0.0, max_value=100.0),
            "total_pnl_usdt": _require_float(stats_dict["total_pnl_usdt"], "stats.total_pnl_usdt"),
            "avg_pnl_usdt": _require_float(stats_dict["avg_pnl_usdt"], "stats.avg_pnl_usdt"),
            "wins": _require_int(stats_dict["wins"], "stats.wins", min_value=0),
            "losses": _require_int(stats_dict["losses"], "stats.losses", min_value=0),
            "breakevens": _require_int(stats_dict["breakevens"], "stats.breakevens", min_value=0),
        },
        "drift": {
            "recent_win_rate_pct": _require_float(stats_dict["recent_win_rate_pct"], "stats.recent_win_rate_pct", min_value=0.0, max_value=100.0),
            "baseline_win_rate_pct": _require_float(stats_dict["baseline_win_rate_pct"], "stats.baseline_win_rate_pct", min_value=0.0, max_value=100.0),
            "recent_avg_pnl_usdt": _require_float(stats_dict["recent_avg_pnl_usdt"], "stats.recent_avg_pnl_usdt"),
            "baseline_avg_pnl_usdt": _require_float(stats_dict["baseline_avg_pnl_usdt"], "stats.baseline_avg_pnl_usdt"),
        },
        "breakdowns": {
            "by_direction": dict(by_direction),
            "by_regime": dict(by_regime),
            "by_engine_total_bucket": dict(by_engine_bucket),
            "by_entry_score_bucket": dict(by_entry_bucket),
        },
        "recent_trades": recent_trades,
        "constraints": {
            "no_trade_decision": True,
            "no_tp_sl": True,
            "no_direct_leverage_target": True,
            "allowed_actions": ["NO_CHANGE", "ADJUST_PARAMS", "RECOMMEND_SAFE_STOP"],
        },
        "current_settings_snapshot": settings_snapshot,
    }

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

    STRICT:
    - cfg 누락 시 즉시 예외 (자동 기본 생성 fallback 금지)
    """
    if cfg is None:
        _fail_config("cfg is required (STRICT)")
    if not isinstance(cfg, MetaPromptConfig):
        _fail_config("cfg must be MetaPromptConfig (STRICT)")

    system_prompt = build_meta_system_prompt(cfg)
    user_payload = build_meta_user_payload(
        stats=stats,
        cfg=cfg,
        current_settings=current_settings,
    )
    return system_prompt, user_payload


__all__ = [
    "MetaPromptBuilderError",
    "MetaPromptBuilderConfigError",
    "MetaPromptBuilderContractError",
    "MetaPromptBuilderStateError",
    "MetaPromptConfig",
    "build_meta_system_prompt",
    "build_meta_user_payload",
    "build_meta_prompts",
]