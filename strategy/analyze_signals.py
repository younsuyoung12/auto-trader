"""
========================================================
FILE: strategy/analyze_signals.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

설계 원칙:
- 폴백 금지
- 데이터 누락 시 즉시 예외
- 예외 삼키기 금지
- 설정은 settings.py 단일 소스
- DB 접근은 db_core 경유

변경 이력
--------------------------------------------------------
- 2026-03-04:
  1) CLOSE pnl의 None/빈값→0 폴백 제거(누락/비숫자 즉시 예외)
  2) CSV 스키마(필수 컬럼) 검증 추가(누락 즉시 예외)
  3) 필수 필드(strategy_type/event, SKIP reason) STRICT 강화
========================================================
"""

from __future__ import annotations

import csv
import datetime
import os
from collections import defaultdict
from typing import Any, Dict, Tuple


BASE_DIR = os.path.join("logs", "signals")

_REQUIRED_COLUMNS = {"strategy_type", "event", "reason", "pnl"}


def _today_csv_path() -> str:
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    return os.path.join(BASE_DIR, f"signals-{today}.csv")


def _require_non_empty_str(v: Any, *, name: str) -> str:
    if v is None:
        raise ValueError(f"{name} is required (STRICT)")
    s = str(v).strip()
    if not s:
        raise ValueError(f"{name} is required (STRICT)")
    return s


def _parse_float_strict(v: Any, *, name: str) -> float:
    """
    STRICT:
    - None/빈문자열/비숫자면 즉시 예외
    - bool은 숫자로 인정하지 않음
    """
    if v is None:
        raise ValueError(f"{name} is required (STRICT)")
    if isinstance(v, bool):
        raise ValueError(f"{name} must be numeric (bool not allowed)")
    s = str(v).strip()
    if not s:
        raise ValueError(f"{name} is required (STRICT)")
    try:
        return float(s)
    except Exception as e:
        raise ValueError(f"{name} must be numeric (got {v!r})") from e


def _load_rows(path: str) -> list[dict[str, str]]:
    """
    STRICT:
    - 파일이 없으면 [] 반환(상위에서 ok=False 처리)
    - 파일이 있으면 CSV 헤더 필수 컬럼 누락 시 즉시 예외
    """
    if not os.path.exists(path):
        return []

    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise RuntimeError(f"CSV header missing (STRICT): {path}")

        header = set(reader.fieldnames)
        missing = sorted([c for c in _REQUIRED_COLUMNS if c not in header])
        if missing:
            raise RuntimeError(f"CSV schema missing columns {missing} (STRICT): {path}")

        return list(reader)


def analyze_rows(rows: list[dict[str, str]]) -> Dict[str, Dict[str, Any]]:
    """
    CSV 한 날치에 대해 전략별 집계를 해준다.
    반환 예:
    {
        "RANGE": {"entries": 3, "closes": 2, "wins": 1, "losses": 1, "pnl_sum": 0.5, ...},
        "TREND": {...},
    }
    """
    stats: Dict[str, Dict[str, Any]] = {
        "RANGE": defaultdict(float),
        "TREND": defaultdict(float),
        "UNKNOWN": defaultdict(float),
    }

    for i, row in enumerate(rows):
        # 필수 필드 STRICT
        strat = _require_non_empty_str(row.get("strategy_type"), name=f"row[{i}].strategy_type")
        event = _require_non_empty_str(row.get("event"), name=f"row[{i}].event")

        bucket = stats.setdefault(strat, defaultdict(float))

        if event == "ENTRY_OPENED":
            bucket["entries"] += 1

        elif event == "CLOSE":
            # STRICT: CLOSE는 pnl 필수
            pnl = _parse_float_strict(row.get("pnl"), name=f"row[{i}].pnl")
            bucket["closes"] += 1
            bucket["pnl_sum"] += pnl
            if pnl > 0:
                bucket["wins"] += 1
            else:
                bucket["losses"] += 1

        elif event == "SKIP":
            # STRICT: SKIP는 reason 필수
            reason = _require_non_empty_str(row.get("reason"), name=f"row[{i}].reason")
            bucket["skips"] += 1
            key = f"skip_reason::{reason}"
            bucket[key] += 1

        else:
            # STRICT: 알 수 없는 이벤트는 숨기지 않고 카운트로 노출
            key = f"unknown_event::{event}"
            bucket[key] += 1

    return stats


def make_report_text(stats: Dict[str, Dict[str, Any]], *, title: str = "") -> str:
    """
    집계 결과를 텔레그램/로그에 던지기 좋은 사람 읽는 문자열로 바꾼다.
    """
    lines: list[str] = []
    if title:
        lines.append(title)

    for strat, b in stats.items():
        if not b:
            continue

        entries = int(b.get("entries", 0))
        closes = int(b.get("closes", 0))
        wins = int(b.get("wins", 0))
        losses = int(b.get("losses", 0))
        pnl_sum = float(b.get("pnl_sum", 0.0))
        skips = int(b.get("skips", 0))

        winrate = (wins / closes * 100.0) if closes else 0.0

        lines.append(f"▶ {strat}")
        lines.append(f"- 진입 {entries}건 / 청산 {closes}건 / 스킵 {skips}건")
        lines.append(f"- 승 {wins} / 패 {losses} / 승률 {winrate:.2f}% / PnL {pnl_sum:.4f} USDT")

        # 많이 발생한 스킵 이유만 보여주기(상위 2개)
        top_skips = [
            (k.replace("skip_reason::", ""), int(v))
            for k, v in b.items()
            if isinstance(k, str) and k.startswith("skip_reason::") and float(v) > 0
        ]
        top_skips.sort(key=lambda x: x[1], reverse=True)
        for name, cnt in top_skips[:2]:
            lines.append(f"  · SKIP {name}: {cnt}회")

        # 알 수 없는 이벤트 노출(있으면 상위 2개)
        unknowns = [
            (k.replace("unknown_event::", ""), int(v))
            for k, v in b.items()
            if isinstance(k, str) and k.startswith("unknown_event::") and float(v) > 0
        ]
        unknowns.sort(key=lambda x: x[1], reverse=True)
        for name, cnt in unknowns[:2]:
            lines.append(f"  · UNKNOWN_EVENT {name}: {cnt}회")

        lines.append("")

    return "\n".join(lines).strip()


def analyze_today() -> Tuple[str, bool]:
    """
    오늘 CSV를 분석해서 문자열을 돌려준다.
    (report, ok) 형태로 리턴. 파일 없으면 ok=False.
    """
    path = _today_csv_path()
    rows = _load_rows(path)
    if not rows:
        return f"[ANALYZE] 오늘자 CSV가 없습니다: {path}", False

    stats = analyze_rows(rows)
    report = make_report_text(stats, title="📊 오늘자 매매 집계")
    return report, True


if __name__ == "__main__":
    text, ok = analyze_today()
    print(text)