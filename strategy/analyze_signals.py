"""
analyze_signals.py
====================================================
logs/signals/signals-YYYY-MM-DD.csv 파일을 읽어서
전략별(RANGE/TREND) 성과를 집계해 주는 스크립트.

- 단독으로 실행하면 오늘 CSV를 읽어서 콘솔에 찍는다.
- 다른 모듈에서 import 해서 `make_report_text(...)`만 써도 된다.
"""

from __future__ import annotations

import os
import csv
import datetime
from collections import defaultdict
from typing import Dict, Any, Tuple


BASE_DIR = os.path.join("logs", "signals")


def _today_csv_path() -> str:
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    return os.path.join(BASE_DIR, f"signals-{today}.csv")


def _load_rows(path: str) -> list[dict[str, str]]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
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

    for row in rows:
        strat = row.get("strategy_type") or "UNKNOWN"
        event = row.get("event") or ""
        pnl = float(row.get("pnl") or 0.0)
        reason = row.get("reason") or ""

        bucket = stats.setdefault(strat, defaultdict(float))

        if event == "ENTRY_OPENED":
            bucket["entries"] += 1
        elif event == "CLOSE":
            bucket["closes"] += 1
            bucket["pnl_sum"] += pnl
            if pnl > 0:
                bucket["wins"] += 1
            else:
                bucket["losses"] += 1
        elif event == "SKIP":
            bucket["skips"] += 1
            key = f"skip_reason::{reason}"
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
        pnl_sum = b.get("pnl_sum", 0.0)
        skips = int(b.get("skips", 0))

        winrate = (wins / closes * 100.0) if closes else 0.0

        lines.append(f"▶ {strat}")
        lines.append(
            f"- 진입 {entries}건 / 청산 {closes}건 / 스킵 {skips}건"
        )
        lines.append(
            f"- 승 {wins} / 패 {losses} / 승률 {winrate:.2f}% / PnL {pnl_sum:.4f} USDT"
        )

        # 많이 발생한 스킵 이유만 보여주기
        top_skips = [
            (k.replace("skip_reason::", ""), int(v))
            for k, v in b.items()
            if k.startswith("skip_reason::") and v > 0
        ]
        # 2개 정도만
        top_skips.sort(key=lambda x: x[1], reverse=True)
        for name, cnt in top_skips[:2]:
            lines.append(f"  · SKIP {name}: {cnt}회")

        lines.append("")  # 전략 사이 빈 줄

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
