"""telelog.py
로그 출력, 텔레그램 전송, '스킵 사유' 전송에 쿨다운을 걸어주는 모듈.

이 모듈은 bot 전체에서 공통으로 쓰는 유틸을 분리한 것이다.
원래 긴 bot.py 안에 있던 것들을 그대로 가져왔고, settings_ws.py 에서 설정을 읽어온다.

사용 예시:
    from telelog import log, send_tg, send_skip_tg
    log("봇 시작")
    send_tg("✅ 봇이 시작되었습니다")
    send_skip_tg("[SKIP] 1m_confirm_range_too_small: 1분봉 변동폭이 너무 작습니다.")

주의:
- 텔레그램 토큰/챗아이디가 비어 있으면 실제 전송 대신 콘솔 로그로 대체한다.
- 같은 스킵 사유를 너무 자주 보내지 않도록 reason 별로 시간 쿨다운을 둔다.
- '가용잔고 0' 같은 건 1시간 쿨다운을 건다 (원래 코드와 동일하게 유지).
"""

from __future__ import annotations

import time
from typing import Dict
import requests

from settings_ws import load_settings, KST  # KST 는 필요하면 외부에서 같이 쓸 수 있게 가져온다.

# 설정을 한 번만 읽어서 전역으로 보관한다. 계속 읽을 필요 없음.
SET = load_settings()

# 파일 로그 옵션 (settings_ws.py 에서 온다)
LOG_TO_FILE: bool = SET.log_to_file
LOG_FILE: str = SET.log_file

# 스킵 텔레그램 쿨다운 관리용 딕셔너리
# key: reason 문자열, value: 마지막으로 이 reason 을 전송한 epoch 시각(float)
LAST_SKIP_TG: Dict[str, float] = {}


def log(msg: str) -> None:
    """콘솔에 로그를 남기고, 필요하면 파일에도 남긴다.

    - 시간 포맷은 "YYYY-MM-DD HH:MM:SS" 로 통일
    - flush=True 로 바로바로 찍히게 한다 (Render 로그 대응)
    - 파일로그는 실패해도 봇이 죽지 않게 예외는 삼킨다.
    """
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    # 콘솔 출력
    print(line, flush=True)

    # 파일 로그가 켜져 있으면 파일에도 남김
    if LOG_TO_FILE:
        try:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            # 로그 때문에 봇이 죽으면 안 되므로 무시
            pass


def send_tg(text: str) -> None:
    """중요 알림을 텔레그램으로 보낸다.

    - 토큰이나 chat_id 가 비어 있으면 실제 전송은 안 하고 콘솔에만 남긴다.
    - 네트워크 오류가 나도 봇이 멈추지 않도록 try/except 로 감싼다.
    """
    bot_token = SET.telegram_bot_token
    chat_id = SET.telegram_chat_id

    # 텔레그램 설정이 안 돼 있으면 콘솔에만 표시
    if not (bot_token and chat_id):
        log("[TG SKIP] " + text)
        return

    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {"chat_id": chat_id, "text": text}
        # timeout 을 짧게 잡아야 네트워크가 멈춰도 봇 메인이 멈추지 않는다.
        requests.post(url, json=payload, timeout=8)
        log("[TG OK] " + text)
    except Exception as e:
        # 텔레그램이 안 된다고 해서 전체 봇을 멈출 필요는 없다.
        log(f"[TG ERROR] {e} {text}")


def send_skip_tg(reason: str) -> None:
    """신호를 '스킵' 한 이유를 텔레그램으로 보내되, 스팸이 되지 않도록 같은 이유는 일정 시간만에 한 번씩만 보낸다.

    규칙은 원래 bot.py 의 것을 그대로 가져왔다:
    1) reason 이 "[BALANCE_SKIP]" 으로 시작하면 1시간 쿨다운을 건다.
       - 잔고가 없을 때는 1분마다 같은 말이 나올 수 있어서 길게 막는다.
    2) reason 에 "range_blocked_today" 가 포함돼 있으면 이것도 1시간 쿨다운
       - 박스장 진입이 오늘은 막혔다는 알림은 자주 올 필요가 없다.
    3) 그 외의 스킵 사유는 settings.skip_tg_cooldown(기본 30초) 으로 막는다.

    실제 전송이 스킵됐는지 아닌지는 콘솔 로그로 남겨서 추적 가능하게 한다.
    """
    now = time.time()

    # 1) 잔고 관련 스킵은 무조건 길게
    if reason.startswith("[BALANCE_SKIP]"):
        cooldown = SET.balance_skip_cooldown  # 기본 3600초
    # 2) 박스장 막힘 관련 스킵도 길게
    elif "range_blocked_today" in reason:
        cooldown = 3600
    else:
        # 3) 나머지는 짧게 (기본 30초)
        cooldown = SET.skip_tg_cooldown

    last_ts = LAST_SKIP_TG.get(reason, 0.0)
    if now - last_ts >= cooldown:
        # 쿨다운이 지났으면 실제로 텔레그램 전송
        send_tg(reason)
        LAST_SKIP_TG[reason] = now
    else:
        # 아직 쿨다운 중이면 콘솔에만 남겨둔다.
        # 이렇게 하면 왜 텔레그램이 안 나갔는지 로그로 확인 가능하다.
        log(f"[SKIP_TG_SUPPRESS] {reason}")


__all__ = ["log", "send_tg", "send_skip_tg", "LAST_SKIP_TG", "SET", "KST"]
