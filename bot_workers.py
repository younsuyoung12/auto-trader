"""
bot_workers.py
====================================================
보조 스레드/서비스 모음.
run_bot.py 에서 이 모듈만 import 해서 호출한다.

[2025-11-12 변경 사항]
----------------------------------------------------
1) 차트/시세 스냅샷 CSV도 드라이브에 같이 올리도록 확장
   - 기존: logs/signals/signals-YYYY-MM-DD.csv 만 업로드
   - 추가: logs/candles/candles-YYYY-MM-DD.csv 가 있으면 같은 이름으로 업로드
   - 업로드 주기는 그대로 5분

담당 기능:
1) health 서버 (/healthz, /metrics)
2) 드라이브 동기화 스레드 (오늘자 CSV 강제 생성 → 업로드 → 오래된 CSV 정리)
3) 텔레그램 종료 명령 수신 스레드 (종료/안전종료/봇종료/끝나면 종료)

주의:
- settings.load_settings() 를 이 모듈 안에서도 호출해서 SET 을 얻는다.
- drive_uploader, signals_logger 는 기존 파일 그대로 사용한다.
- 텔레그램 스레드는 콜백(on_stop_command) 을 run_bot.py 에서 넘겨받는다.
"""

from __future__ import annotations

import os
import time
import json
import threading
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Callable

from settings import load_settings
from telelog import log
from drive_uploader import upload_to_drive
from signals_logger import _ensure_today_csv  # 시그널 CSV
# ✅ 시세 스냅샷 CSV는 파일이 있을 때만 올릴 것이므로, 여기서는 경로만 계산해서 쓴다.

SET = load_settings()

# ─────────────────────────────
# 1. health / metrics HTTP 서버
# ─────────────────────────────
class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/healthz":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")
            return
        if self.path == "/metrics":
            # 최소 정보만 노출 (실제 메트릭은 run_bot 에서 관리)
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"bot_up 1")
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, *_: Any) -> None:
        # 기본 HTTP 로그 숨김
        return


def start_health_server() -> None:
    """환경설정에 health_port 가 있으면 HTTP 서버를 백그라운드로 띄운다."""
    if SET.health_port <= 0:
        return

    def _run() -> None:
        httpd = HTTPServer(("0.0.0.0", SET.health_port), _HealthHandler)
        log(f"[HEALTH] server started on 0.0.0.0:{SET.health_port}")
        httpd.serve_forever()

    th = threading.Thread(target=_run, daemon=True)
    th.start()


# ─────────────────────────────
# 2. 드라이브 동기화 스레드
# ─────────────────────────────
def start_drive_sync_thread() -> None:
    """5분마다 오늘자 CSV 생성 후 드라이브 업로드 + 오래된 CSV 삭제
    - signals-YYYY-MM-DD.csv 는 무조건 생성해서 올린다.
    - candles-YYYY-MM-DD.csv 는 run_bot 측에서 만들어둔 경우에만 같이 올린다.
    """
    SYNC_INTERVAL_SEC = 300  # 5분
    KEEP_DAYS = 3

    def _worker() -> None:
        while True:
            try:
                # 1) 오늘자 시그널 파일 확보
                local_path = _ensure_today_csv()
                day_str = os.path.basename(local_path).replace("signals-", "").replace(".csv", "")

                # 2) 시그널 CSV 업로드
                upload_to_drive(local_path, f"signals-{day_str}.csv")
                log("[DRIVE_SYNC] uploaded today's signals csv")

                # 3) 캔들 스냅샷 CSV가 있으면 같이 업로드
                candle_dir = os.path.join("logs", "candles")
                candle_name = f"candles-{day_str}.csv"
                candle_path = os.path.join(candle_dir, candle_name)
                if os.path.exists(candle_path):
                    upload_to_drive(candle_path, candle_name)
                    log("[DRIVE_SYNC] uploaded today's candles csv")

                # 4) 오래된 signals CSV 삭제
                local_dir = os.path.dirname(local_path)
                now_ts = time.time()
                for fname in os.listdir(local_dir):
                    if not (fname.startswith("signals-") and fname.endswith(".csv")):
                        continue
                    fpath = os.path.join(local_dir, fname)
                    mtime = os.path.getmtime(fpath)
                    if now_ts - mtime > KEEP_DAYS * 86400:
                        try:
                            os.remove(fpath)
                            log(f"[DRIVE_SYNC] old csv removed: {fname}")
                        except OSError:
                            pass

                # 5) 오래된 candles CSV 삭제 (있으면)
                if os.path.isdir(candle_dir):
                    for fname in os.listdir(candle_dir):
                        if not (fname.startswith("candles-") and fname.endswith(".csv")):
                            continue
                        fpath = os.path.join(candle_dir, fname)
                        mtime = os.path.getmtime(fpath)
                        if now_ts - mtime > KEEP_DAYS * 86400:
                            try:
                                os.remove(fpath)
                                log(f"[DRIVE_SYNC] old candle csv removed: {fname}")
                            except OSError:
                                pass

            except Exception as e:  # 업로드 실패해도 봇은 계속
                log(f"[DRIVE_SYNC] error: {e}")
            time.sleep(SYNC_INTERVAL_SEC)

    th = threading.Thread(target=_worker, daemon=True)
    th.start()


# ─────────────────────────────
# 3. 텔레그램 종료 명령 수신 스레드
# ─────────────────────────────
def start_telegram_command_thread(*, on_stop_command: Callable[[], None]) -> None:
    """
    텔레그램의 getUpdates 를 폴링해서 종료 명령을 받으면 콜백을 실행한다.
    - on_stop_command: run_bot.py 에서 넘긴 함수 (SAFE_STOP_REQUESTED 켜는 용도)
    - 409 Conflict 는 로그만 남기고 재시도
    """
    if not SET.telegram_bot_token or not SET.telegram_chat_id:
        return

    base_url = f"https://api.telegram.org/bot{SET.telegram_bot_token}/getUpdates"
    chat_id_str = str(SET.telegram_chat_id)

    def _worker() -> None:
        nonlocal base_url, chat_id_str  # type: ignore
        update_offset = 0
        while True:
            try:
                url = f"{base_url}?timeout=30"
                if update_offset:
                    url += f"&offset={update_offset}"
                with urllib.request.urlopen(url, timeout=35) as resp:
                    data = json.loads(resp.read().decode("utf-8"))

                for upd in data.get("result", []):
                    update_offset = upd["update_id"] + 1
                    msg = upd.get("message") or upd.get("channel_post")
                    if not msg:
                        continue
                    if str(msg["chat"]["id"]) != chat_id_str:
                        continue
                    text = (msg.get("text") or "").strip()
                    if text in ("종료", "봇종료", "끝나면 종료", "안전종료"):
                        on_stop_command()
            except urllib.error.HTTPError as e:
                if e.code == 409:
                    log("[TG CMD] 409 Conflict → 무시 후 재시도")
                else:
                    log(f"[TG CMD] http error: {e}")
                time.sleep(5)
            except Exception as e:
                log(f"[TG CMD] error: {e}")
                time.sleep(5)

    th = threading.Thread(target=_worker, daemon=True)
    th.start()
