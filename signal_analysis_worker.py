"""
signal_analysis_worker.py
====================================================
logs/signals/... 에 쌓인 CSV를 주기적으로 읽어서
요약 리포트를 텔레그램으로 보내는 백그라운드 스레드.

2025-11-12 보강
----------------------------------------------------
1) 오늘자 CSV가 있으면 같은 폴더에 XLSX도 만들어서 남긴다.
   - logs/signals/signals-YYYY-MM-DD.csv
   - logs/signals/signals-YYYY-MM-DD.xlsx
2) run_bot.py 없이 단독으로도 돌 수 있게 __main__ 블록 추가.
3) 프로젝트에 bot_workers.start_drive_sync_thread() 가 있으면
   여기서도 같이 켜서 드라이브 업로드가 되게 한다.
"""

from __future__ import annotations

import os
import time
import threading
from datetime import datetime

from telelog import send_tg, log
from analyze_signals import analyze_today  # 반드시 (str, bool) 형태를 리턴하게 만들 것

# CSV 가 쌓여 있는 기본 경로 (네 기존 구조랑 맞춰둠)
LOG_DIR = os.path.join("logs", "signals")


def _export_today_to_excel() -> str:
    """
    오늘자 signals-YYYY-MM-DD.csv 가 있으면 같은 폴더에 xlsx 로도 떨군다.
    pandas 가 없으면 조용히 스킵하고 로그만 남긴다.
    성공하면 생성된 xlsx 경로를 돌려주고, 아니면 "" 반환.
    """
    today = datetime.utcnow().strftime("%Y-%m-%d")
    csv_path = os.path.join(LOG_DIR, f"signals-{today}.csv")
    if not os.path.exists(csv_path):
        log(f"[ANALYZE_WORKER] no csv for today: {csv_path}")
        return ""

    xlsx_path = os.path.join(LOG_DIR, f"signals-{today}.xlsx")

    try:
        # pandas 있으면 이게 제일 간단
        import pandas as pd  # type: ignore

        df = pd.read_csv(csv_path)
        # 엑셀로 저장
        os.makedirs(LOG_DIR, exist_ok=True)
        df.to_excel(xlsx_path, index=False)
        log(f"[ANALYZE_WORKER] excel exported: {xlsx_path}")
        return xlsx_path
    except ImportError:
        # pandas 없는 환경일 수 있으니 그냥 로그만
        log("[ANALYZE_WORKER] pandas not installed, skip excel export")
        return ""
    except Exception as e:
        log(f"[ANALYZE_WORKER] excel export error: {e}")
        return ""


def start_signal_analysis_thread(interval_sec: int = 1800) -> None:
    """
    interval_sec 마다 한 번씩 CSV를 분석해서 텔레그램으로 보낸다.
    기본 1800초 = 30분.
    엑셀도 같이 생성한다.
    """

    def _worker() -> None:
        while True:
            try:
                # 1) 텍스트 리포트 생성
                report, ok = analyze_today()
                if ok and report:
                    send_tg(report)
                else:
                    # 파일이 없거나 오늘 기록이 없을 수 있으니 여기선 조용히 로그만
                    if report:
                        log(report)
                    else:
                        log("[ANALYZE_WORKER] no report generated")

                # 2) 엑셀로도 저장 시도
                _export_today_to_excel()

            except Exception as e:
                # 분석 중 오류가 나도 메인 봇은 죽지 않게 여기서만 잡는다
                log(f"[ANALYZE_WORKER] error: {e}")

            # 다음 주기까지 대기
            time.sleep(interval_sec)

    th = threading.Thread(target=_worker, daemon=True, name="signal-analysis")
    th.start()


# ================================================
# 단독 실행용 진입점
# python signal_analysis_worker.py 로도 돌릴 수 있게
# ================================================
if __name__ == "__main__":
    # 1) 드라이브 동기화 스레드도 있으면 같이 켠다
    try:
        from bot_workers import start_drive_sync_thread

        start_drive_sync_thread()
        log("[ANALYZE_WORKER] drive sync thread started")
    except Exception as e:
        # 없으면 그냥 넘어가도 됨
        log(f"[ANALYZE_WORKER] drive sync thread not started: {e}")

    # 2) 분석 워커 시작
    start_signal_analysis_thread(interval_sec=1800)
    log("[ANALYZE_WORKER] started standalone worker (every 1800s)")

    # 3) 메인 스레드는 그냥 살려둔다
    while True:
        time.sleep(3600)
