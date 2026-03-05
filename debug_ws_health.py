#!/usr/bin/env python3
"""
debug_ws_health.py
========================================================
Purpose
- WS health fail의 "min_buffer(예: 320)"가 어디서 결정되는지 추적한다.
- 어떤 settings.py가 로드되는지, infra.market_data_ws.KLINE_MIN_BUFFER가 얼마인지,
  그리고 health snapshot이 실제로 어떤 min_buffer_required를 쓰는지 출력한다.

Safe
- API 키/시크릿/DB URL 등 민감정보는 절대 출력하지 않는다.
- 주문/계정 private endpoint 호출하지 않는다.

Usage
- python debug_ws_health.py
- python debug_ws_health.py --symbol BTCUSDT
- python debug_ws_health.py --show-files
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict


def _p(title: str) -> None:
    print("\n" + "=" * 88)
    print(title)
    print("=" * 88)


def _kv(k: str, v: Any) -> None:
    print(f"- {k}: {v!r}")


def _safe_env_get(keys: list[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k in keys:
        if k in os.environ:
            out[k] = os.environ.get(k, "")
    return out


def _find_occurrences(path: Path, needle: str, max_lines: int = 20) -> None:
    if not path.exists():
        print(f"[WARN] file not found: {path}")
        return
    try:
        text = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception as e:
        print(f"[WARN] failed to read {path}: {e}")
        return

    hits = []
    for i, line in enumerate(text, start=1):
        if needle in line:
            hits.append((i, line.rstrip()))
            if len(hits) >= max_lines:
                break

    if not hits:
        print(f"[OK] no occurrences for '{needle}' in {path}")
        return

    print(f"[HITS] '{needle}' in {path} (showing up to {max_lines})")
    for ln, line in hits:
        print(f"  L{ln}: {line}")


def _parse_entry_required_klines_min_from_text(text: str) -> Any:
    """
    core/run_bot_ws.py 안의 ENTRY_REQUIRED_KLINES_MIN dict를 import 없이 대충 파싱.
    (정확한 파서가 아니라 "대략 값 확인" 용도)
    """
    m = re.search(r"ENTRY_REQUIRED_KLINES_MIN\s*:\s*Dict\[.*?\]\s*=\s*(\{.*?\})", text, re.S)
    if not m:
        m = re.search(r"ENTRY_REQUIRED_KLINES_MIN\s*=\s*(\{.*?\})", text, re.S)
    if not m:
        return None
    raw = m.group(1)
    # 아주 단순 파싱: {"1m": 200, ...} 형태만 기대
    out: Dict[str, int] = {}
    for km in re.finditer(r'"([^"]+)"\s*:\s*(\d+)', raw):
        out[km.group(1)] = int(km.group(2))
    return out if out else raw


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--show-files", action="store_true", help="관련 파일에서 설정 키워드 등장 라인 출력")
    args = ap.parse_args()

    _p("Runtime / Python")
    _kv("cwd", os.getcwd())
    _kv("python", sys.executable)
    _kv("version", sys.version.split("\n")[0])
    _kv("sys.path[0:6]", sys.path[:6])

    # 1) settings 로딩 + 어떤 파일이 로드되는지
    _p("settings.py resolution")
    try:
        import settings as settings_mod  # type: ignore
        from settings import load_settings  # type: ignore
    except Exception as e:
        print(f"[FATAL] failed to import settings/load_settings: {type(e).__name__}: {e}")
        return 2

    _kv("settings.__file__", getattr(settings_mod, "__file__", None))
    s = load_settings()

    # 민감정보는 출력 금지: 존재 여부만 표시
    _kv("has BINANCE key/secret", bool(getattr(s, "api_key", "")) and bool(getattr(s, "api_secret", "")))
    _kv("has OPENAI key/model", bool(getattr(s, "openai_api_key", "")) and bool(getattr(s, "openai_model", "")))

    # 핵심 WS 설정만 출력
    _kv("symbol", getattr(s, "symbol", None))
    _kv("ws_min_kline_buffer", getattr(s, "ws_min_kline_buffer", None))
    _kv("ws_required_tfs", getattr(s, "ws_required_tfs", None))
    _kv("ws_subscribe_tfs", getattr(s, "ws_subscribe_tfs", None))
    _kv("ws_max_kline_delay_sec", getattr(s, "ws_max_kline_delay_sec", None))
    _kv("ws_orderbook_max_delay_sec", getattr(s, "ws_orderbook_max_delay_sec", None))

    # 혹시라도 환경변수가 개입하는지 (프로젝트에 따라 다름) 확인만
    _p("Environment variables (presence only)")
    env_hits = _safe_env_get(
        [
            "WS_MIN_KLINE_BUFFER",
            "WS_REQUIRED_TFS",
            "WS_SUBSCRIBE_TFS",
            "PYTHONPATH",
        ]
    )
    if env_hits:
        for k, v in env_hits.items():
            # 너무 길면 자름
            vv = v if len(v) <= 240 else v[:240] + "...(truncated)"
            _kv(k, vv)
    else:
        print("- (none of WS_MIN_KLINE_BUFFER / WS_REQUIRED_TFS / WS_SUBSCRIBE_TFS / PYTHONPATH set)")

    # 2) market_data_ws에서 실제로 쓰는 KLINE_MIN_BUFFER 값 확인
    _p("infra.market_data_ws constants + health snapshot")
    try:
        from infra import market_data_ws as mdws  # type: ignore
    except Exception as e:
        print(f"[FATAL] failed to import infra.market_data_ws: {type(e).__name__}: {e}")
        return 3

    _kv("infra.market_data_ws.__file__", getattr(mdws, "__file__", None))
    _kv("mdws.KLINE_MIN_BUFFER", getattr(mdws, "KLINE_MIN_BUFFER", None))
    _kv("mdws._LONG_TF_MIN_BUFFER_DEFAULT", getattr(mdws, "_LONG_TF_MIN_BUFFER_DEFAULT", None))
    _kv("mdws.REQUIRED_INTERVALS", getattr(mdws, "REQUIRED_INTERVALS", None))

    sym = str(args.symbol).strip().upper()
    snap = mdws.get_health_snapshot(sym)
    print("\n[health_snapshot] (pretty)")
    print(json.dumps(snap, ensure_ascii=False, indent=2, default=str))

    # 3) market_features_ws가 어디 파일을 바라보고 있는지(참고)
    _p("infra.market_features_ws (import linkage check)")
    try:
        from infra import market_features_ws as mfws  # type: ignore
    except Exception as e:
        print(f"[WARN] failed to import infra.market_features_ws: {type(e).__name__}: {e}")
        mfws = None

    if mfws is not None:
        _kv("infra.market_features_ws.__file__", getattr(mfws, "__file__", None))
        # mfws는 mdws에서 KLINE_MIN_BUFFER를 import 해오므로 값이 동일해야 정상
        _kv("mfws.KLINE_MIN_BUFFER", getattr(mfws, "KLINE_MIN_BUFFER", None))

    # 4) run_bot_ws / run_bot_preflight의 ENTRY_REQUIRED_KLINES_MIN 값(파일에서 직접 읽어 확인)
    _p("ENTRY_REQUIRED_KLINES_MIN check (read files, no import)")
    root = Path(os.getcwd())

    run_bot_ws_path = root / "core" / "run_bot_ws.py"
    run_bot_pf_path = root / "core" / "run_bot_preflight.py"

    for pth in (run_bot_ws_path, run_bot_pf_path):
        if not pth.exists():
            print(f"[WARN] not found: {pth}")
            continue
        txt = pth.read_text(encoding="utf-8", errors="replace")
        parsed = _parse_entry_required_klines_min_from_text(txt)
        _kv(str(pth), parsed)

    # 5) 원인 추적용: 실제로 health가 요구한 min_buffer_required가 얼마인지 바로 보여주기
    _p("Derived min_buffer_required per interval (from snapshot)")
    kl = snap.get("klines") or {}
    if isinstance(kl, dict) and kl:
        for iv, st in kl.items():
            if not isinstance(st, dict):
                continue
            _kv(f"{iv}.buffer_len", st.get("buffer_len"))
            _kv(f"{iv}.min_buffer_required", st.get("min_buffer_required"))
            _kv(f"{iv}.ok", st.get("ok"))
            _kv(f"{iv}.reasons", st.get("reasons"))
            print("-" * 40)
    else:
        print("[WARN] snapshot.klines is empty (WS not started or no required intervals?)")

    if args.show_files:
        _p("File scan (where ws_min_kline_buffer / KLINE_MIN_BUFFER appears)")
        settings_path = Path(getattr(settings_mod, "__file__", "settings.py"))
        _find_occurrences(settings_path, "ws_min_kline_buffer")
        _find_occurrences(settings_path, "WS_MIN_KLINE_BUFFER")
        _find_occurrences(root / "infra" / "market_data_ws.py", "KLINE_MIN_BUFFER")
        _find_occurrences(root / "infra" / "market_features_ws.py", "KLINE_MIN_BUFFER")
        _find_occurrences(run_bot_ws_path, "ENTRY_REQUIRED_KLINES_MIN")
        _find_occurrences(run_bot_pf_path, "ENTRY_REQUIRED_KLINES_MIN")

    _p("DONE")
    print("이 출력에서 아래 3가지만 보면 원인이 100% 확정됩니다.")
    print("1) settings.__file__ 경로")
    print("2) ws_min_kline_buffer 값 vs mdws.KLINE_MIN_BUFFER 값")
    print("3) health_snapshot 각 TF의 min_buffer_required 값")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())