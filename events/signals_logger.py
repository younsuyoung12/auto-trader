# events/signals_logger.py
# =============================================================================
# AUTO-TRADER - Event Logger (STRICT / NO-FALLBACK)
# -----------------------------------------------------------------------------
# 이 파일은 자동매매 엔진의 모든 이벤트를 기록하고
# 동시에 EventBus로 publish하는 중앙 허브이다.
#
# 정책:
# - 이벤트 기록 실패 시 즉시 예외 발생
# - 필수 필드 누락 시 예외 발생
# - JSON 직렬화 실패 시 예외 발생
# - 폴백 금지
# =============================================================================

from __future__ import annotations

import csv
import datetime
import json
import os
from typing import Any, Dict, List, Optional


# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------
EVENTS_DIR = os.path.join("logs", "events")


# -----------------------------------------------------------------------------
# Time helpers (KST = UTC+9)
# -----------------------------------------------------------------------------
def _now_kst() -> datetime.datetime:
    now_utc = datetime.datetime.utcnow()
    return now_utc + datetime.timedelta(hours=9)


def _today_kst_str() -> str:
    return _now_kst().strftime("%Y-%m-%d")


def _ensure_dir(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


# -----------------------------------------------------------------------------
# CSV helpers
# -----------------------------------------------------------------------------
def _events_csv_path_for(day_str: str) -> str:
    _ensure_dir(EVENTS_DIR)
    return os.path.join(EVENTS_DIR, f"events-{day_str}.csv")


def _event_fieldnames() -> List[str]:
    return [
        "ts",
        "ts_iso",
        "event_type",
        "symbol",
        "regime",
        "source",
        "side",
        "price",
        "qty",
        "leverage",
        "tp_pct",
        "sl_pct",
        "risk_pct",
        "pnl_pct",
        "reason",
        "extra_json",
    ]


def _ensure_today_events_csv() -> str:
    day = _today_kst_str()
    path = _events_csv_path_for(day)
    fields = _event_fieldnames()
    if not os.path.exists(path):
        _ensure_dir(EVENTS_DIR)
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
    return path


# Create today's events CSV on import (STRICT: any failure should surface)
_ensure_today_events_csv()


def _require_nonempty_str(v: Any, name: str) -> str:
    s = str(v or "").strip()
    if not s:
        raise RuntimeError(f"{name} is required")
    return s


def _json_stringify_extra(extra_val: Any) -> str:
    """Return a JSON string. STRICT: if string is provided, it must be valid JSON."""
    if extra_val is None:
        return "{}"

    if isinstance(extra_val, (dict, list)):
        return json.dumps(extra_val, ensure_ascii=False)

    if isinstance(extra_val, str):
        # STRICT: must be valid JSON string
        _ = json.loads(extra_val)  # raises if invalid
        return extra_val

    raise RuntimeError("extra_json must be dict/list/valid-json-string")


def log_event(event_type: str, **kwargs: Any) -> None:
    """
    1) 필수 필드 검증 (event_type, symbol, reason)
    2) row 구성
    3) CSV 기록
    4) publish_event(event_type, **payload)
    """
    et = _require_nonempty_str(event_type, "event_type")
    symbol = _require_nonempty_str(kwargs.get("symbol"), "symbol")
    reason = _require_nonempty_str(kwargs.get("reason"), "reason")

    now = _now_kst()
    row: Dict[str, Any] = {
        "ts": now.timestamp(),
        "ts_iso": now.isoformat(),
        "event_type": et,
        "symbol": symbol,
        "regime": "",
        "source": "",
        "side": "",
        "price": "",
        "qty": "",
        "leverage": "",
        "tp_pct": "",
        "sl_pct": "",
        "risk_pct": "",
        "pnl_pct": "",
        "reason": reason,
        "extra_json": "{}",
    }

    # Allowed keys mapping (ignore unknown keys)
    allowed = set(_event_fieldnames())
    for key, val in kwargs.items():
        if key in allowed and key not in {"event_type", "extra_json", "reason"}:
            row[key] = val

    # extra / extra_json -> JSON string (STRICT)
    extra_val: Any = None
    if "extra_json" in kwargs:
        extra_val = kwargs["extra_json"]
    elif "extra" in kwargs:
        extra_val = kwargs["extra"]

    row["extra_json"] = _json_stringify_extra(extra_val)

    # Write CSV (STRICT: no swallow)
    path = _ensure_today_events_csv()
    file_exists = os.path.exists(path)
    fieldnames = _event_fieldnames()

    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in fieldnames})

    # Publish to EventBus (STRICT: no swallow)
    from events.event_bus import publish_event

    payload: Dict[str, Any] = {
        "symbol": symbol,
        "regime": str(row.get("regime") or ""),
        "source": str(row.get("source") or ""),
        "side": str(row.get("side") or ""),
        "price": row.get("price"),
        "qty": row.get("qty"),
        "leverage": row.get("leverage"),
        "tp_pct": row.get("tp_pct"),
        "sl_pct": row.get("sl_pct"),
        "risk_pct": row.get("risk_pct"),
        "pnl_pct": row.get("pnl_pct"),
        "reason": reason,
        "extra_json": row.get("extra_json"),
        "ts_kst_epoch": row.get("ts"),
        "ts_iso": row.get("ts_iso"),
    }

    publish_event(et, **payload)


__all__ = ["log_event"]