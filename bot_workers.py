"""
수정 내용 (2025-11-13)
1) 드라이브 업로드 주기/보관일을 환경변수로도 조정할 수 있게 함 (없으면 기존값 300초/3일 유지)
2) 텔레그램 종료 명령에 영문 'stop', '/stop' 도 허용하여 운영 중 즉시 멈출 수 있게 함
3) 각 워커 스레드 시작 시 로그를 남겨 run_bot.py 콘솔에서 상태를 바로 확인할 수 있게 함
4) 기존 2025-11-12 기능(시그널 CSV + 캔들 CSV 동시 업로드, 오래된 파일 정리) 그대로 유지
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

from settings_ws import load_settings
from telelog import log
from drive_uploader import upload_to_drive
from signals_logger import _ensure_today_csv  # 시그널 CSV 생성/보장

# 전역 설정 로드
SET = load_settings()


# ─────────────────────────────
# 1. health / metrics HTTP 서버
# ─────────────────────────────
class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        """/healthz, /metrics 두 개만 응답한다."""
        if self.path == "/healthz":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")
            return
        if self.path == "/metrics":
            # 최소 메트릭만 실어 보낸다. 상세 메트릭은 run_bot.py 에서 관리.
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"bot_up 1")
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, *_: Any) -> None:
        # 기본 HTTP 요청 로그는 콘솔을 어지럽히므로 숨긴다.
        return


def start_health_server() -> None:
    """환경설정에 health_port 가 있으면 HTTP 서버를 백그라운드로 띄운다."""
    if SET.health_port <= 0:
        return

    def _run() -> None:
        httpd = HTTPServer(("0.0.0.0", SET.health_port), _HealthHandler)
        log(f"[HEALTH] server started on 0.0.0.0:{SET.health_port}")
        httpd.serve_forever()

    th = threading.Thread(target=_run, daemon=True, name="health-server")
    th.start()


# ─────────────────────────────
# 2. 드라이브 동기화 스레드
# ─────────────────────────────
def start_drive_sync_thread() -> None:
    """5분마다 오늘자 CSV 생성 후 드라이브 업로드 + 오래된 CSV 정리.
    - signals-YYYY-MM-DD.csv 는 무조건 생성해서 올린다.
    - candles-YYYY-MM-DD.csv 는 있으면 같이 올린다.
    - 업로드 주기/보관일은 환경변수로 조정 가능하다.
    """
    # env 없으면 기존 값 유지
    SYNC_INTERVAL_SEC = int(os.getenv("DRIVE_SYNC_INTERVAL_SEC", "300"))  # 5분
    KEEP_DAYS = int(os.getenv("DRIVE_KEEP_DAYS", "3"))

    def _worker() -> None:
        log(f"[DRIVE_SYNC] worker started interval={SYNC_INTERVAL_SEC}s keep_days={KEEP_DAYS}")
        while True:
            try:
                # 1) 오늘자 시그널 CSV 확보/생성
                local_path = _ensure_today_csv()
                day_str = os.path.basename(local_path).replace("signals-", "").replace(".csv", "")

                # 2) 시그널 CSV 업로드
                upload_to_drive(local_path, f"signals-{day_str}.csv")
                log("[DRIVE_SYNC] uploaded today's signals csv")

                # 3) 캔들 CSV 있으면 같이 업로드
                candle_dir = os.path.join("logs", "candles")
                candle_name = f"candles-{day_str}.csv"
                candle_path = os.path.join(candle_dir, candle_name)
                if os.path.exists(candle_path):
                    upload_to_drive(candle_path, candle_name)
                    log("[DRIVE_SYNC] uploaded today's candles csv")

                # 4) 오래된 signals-* 파일 정리
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

                # 5) 오래된 candles-* 파일 정리
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

            except Exception as e:  # 업로드 실패해도 봇은 계속 돌아가야 한다.
                log(f"[DRIVE_SYNC] error: {e}")
            time.sleep(SYNC_INTERVAL_SEC)

    th = threading.Thread(target=_worker, daemon=True, name="drive-sync")
    th.start()


# ─────────────────────────────
# 3. 텔레그램 종료 명령 수신 스레드
# ─────────────────────────────
def start_telegram_command_thread(*, on_stop_command: Callable[[], None]) -> None:
    """텔레그램 getUpdates 폴링으로 종료 명령을 받으면 on_stop_command 를 호출한다.
    허용 명령어: "종료", "봇종료", "끝나면 종료", "안전종료", "stop", "/stop"
    run_bot.py 에서는 이 콜백에서 SAFE_STOP_REQUESTED 플래그만 세팅하면 된다.
    """
    if not SET.telegram_bot_token or not SET.telegram_chat_id:
        return

    base_url = f"https://api.telegram.org/bot{SET.telegram_bot_token}/getUpdates"
    chat_id_str = str(SET.telegram_chat_id)
    stop_words = {"종료", "봇종료", "끝나면 종료", "안전종료", "stop", "/stop"}

    def _worker() -> None:
        nonlocal base_url, chat_id_str  # type: ignore
        log("[TG CMD] worker started")
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
                    if text in stop_words:
                        log(f"[TG CMD] stop command received: {text}")
                        on_stop_command()
            except urllib.error.HTTPError as e:
                if e.code == 409:
                    log("[TG CMD] 409 Conflict → ignore and retry")
                else:
                    log(f"[TG CMD] http error: {e}")
                time.sleep(5)
            except Exception as e:
                log(f"[TG CMD] error: {e}")
                time.sleep(5)

    th = threading.Thread(target=_worker, daemon=True, name="telegram-cmd")
    th.start()
