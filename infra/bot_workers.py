# bot_workers.py
# =====================================================
# Bot Workers (Production)
# -----------------------------------------------------
# 역할
# - health / metrics HTTP 서버
# - 텔레그램 종료 명령 워커
#
# 변경 사항 (v2026.03.01)
# - Google Drive 동기화 기능 완전 제거
#   (infra.drive_uploader 의존성 제거, drive-sync 스레드 제거)
# - 하위 호환을 위해 start_drive_sync_thread()는 "no-op"로 유지
#   (기존 run_bot.py가 호출하더라도 엔진이 크래시하지 않게 함)
# =====================================================

from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Callable

from infra.telelog import log
from settings import load_settings

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
        """기본 HTTP 요청 로그는 콘솔을 어지럽히므로 숨긴다."""
        return


def start_health_server() -> None:
    """환경설정에 health_port 가 있으면 HTTP 서버를 백그라운드로 띄운다."""
    # NOTE: settings.py에 health_port가 없으면 AttributeError가 날 수 있으므로
    #       안전하게 getattr로 접근한다.
    health_port = int(getattr(SET, "health_port", 0) or 0)
    if health_port <= 0:
        return

    def _run() -> None:
        httpd = HTTPServer(("0.0.0.0", health_port), _HealthHandler)
        log(f"[HEALTH] server started on 0.0.0.0:{health_port}")
        httpd.serve_forever()

    th = threading.Thread(target=_run, daemon=True, name="health-server")
    th.start()


# ─────────────────────────────
# 2. Drive Sync (REMOVED)
# ─────────────────────────────
def start_drive_sync_thread() -> None:
    """
    [DEPRECATED / NO-OP]
    - 기존 코드 호환을 위해 함수만 남긴다.
    - Google Drive 업로드/동기화는 운영에서 제거되었다.
    """
    log("[DRIVE_SYNC] disabled (DB-only mode).")
    return


# ─────────────────────────────
# 3. 텔레그램 종료 명령 수신 스레드
# ─────────────────────────────
def start_telegram_command_thread(*, on_stop_command: Callable[[], None]) -> None:
    """텔레그램 getUpdates 폴링으로 종료 명령을 받으면 on_stop_command 를 호출한다.

    허용 명령어: "종료", "봇종료", "끝나면 종료", "안전종료", "stop", "/stop"
    run_bot.py 에서는 이 콜백에서 SAFE_STOP_REQUESTED 플래그만 세팅하면 된다.
    """
    if not getattr(SET, "telegram_bot_token", "") or not getattr(SET, "telegram_chat_id", ""):
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


__all__ = [
    "start_health_server",
    "start_drive_sync_thread",
    "start_telegram_command_thread",
]