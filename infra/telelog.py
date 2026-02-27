"""telelog.py
로그 출력, 텔레그램 전송, '스킵 사유' 전송에 쿨다운을 걸어주는 모듈.

이 모듈은 bot 전체에서 공통으로 쓰는 유틸을 분리한 것이다.
원래 긴 bot.py 안에 있던 것들을 그대로 가져왔고, settings_ws.py 에서 설정을 읽어온다.

사용 예시:
    from telelog import log, send_tg, send_skip_tg, send_structured_tg
    log("봇 시작")
    send_tg("✅ 봇이 시작되었습니다")
    send_skip_tg("[SKIP] 1m_confirm_range_too_small: 1분봉 변동폭이 너무 작습니다.")
    send_structured_tg(
        title="ENTRY / TREND LONG",
        indicators={"5m_trend": "LONG", "15m_trend": "LONG"},
        scores={"entry_score": 7.2, "signal_strength": 1.3},
        cooldown_sec=1.0,
        cooldown_reason="신규 진입 기본 쿨타임",
    )

주의:
- 텔레그램 토큰/챗아이디가 비어 있으면 실제 전송 대신 콘솔 로그로 대체한다.
- 같은 스킵 사유를 너무 자주 보내지 않도록 reason 별로 시간 쿨다운을 둔다.
- '가용잔고 0' 같은 건 1시간 쿨다운을 건다 (원래 코드와 동일하게 유지).

2025-11-14 변경 사항
----------------------------------------------------
1) 텔레그램 알림 포맷을 "지표·점수·쿨타임·에러" 4섹션 템플릿으로 통일하는 헬퍼 추가
   - build_tg_template(...) : 실제 문자열을 만들어주는 공통 포맷터
   - send_structured_tg(...) : 위 템플릿을 사용해 알림을 보내는 래퍼
2) send_skip_tg(...) 도 템플릿 빌더를 사용해 쿨타임 정보를 함께 표시하도록 변경
3) 기존 send_tg(...) / log(...) 인터페이스는 그대로 유지 (하위호환)

2025-11-20 변경 사항 (텔레그램 한국어 표현 통일)
----------------------------------------------------
1) 텔레그램 메시지의 쿨타임 단위를 "s" 대신 "초"로 한국어 표기로 변경.
2) send_skip_tg(...) 제목을 "SKIP" → "스킵" 등 한국어로 정리.
3) 대표 스킵 사유(no_signal_or_arbitration_rejected, BALANCE, range_blocked_today 등)를
   한국어로 자동 변환하는 헬퍼(_localize_skip_reason)를 추가.
"""

from __future__ import annotations

import time
from typing import Dict, Any, Optional

import requests

from settings import load_settings, KST  # KST 는 필요하면 외부에서 같이 쓸 수 있게 가져온다.

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


def _format_dict_block(title: str, data: Optional[Dict[str, Any]]) -> str:
    """지표/점수 섹션을 공통 포맷으로 만들어준다.

    예)
        📊 지표
         - 5m_trend: LONG
         - 15m_trend: LONG
    """
    if not data:
        return ""

    lines = [title]
    for k, v in data.items():
        lines.append(f" - {k}: {v}")
    return "\n".join(lines)


def build_tg_template(
    *,
    title: str,
    indicators: Optional[Dict[str, Any]] = None,
    scores: Optional[Dict[str, Any]] = None,
    cooldown_sec: Optional[float] = None,
    cooldown_reason: Optional[str] = None,
    error: Optional[str] = None,
    note: Optional[str] = None,
) -> str:
    """"지표·점수·쿨타임·에러" 4섹션을 기준으로 텔레그램 메시지 문자열을 만든다.

    사용 예:
        text = build_tg_template(
            title="ENTRY / TREND LONG",
            indicators={"5m_trend": "LONG"},
            scores={"entry_score": 7.2},
            cooldown_sec=1.0,
            cooldown_reason="신규 진입 기본 쿨타임",
            error=None,
        )

    각 섹션은 값이 없으면 생략된다.
    """
    parts: list[str] = [f"📌 {title}"]

    block = _format_dict_block("📊 지표", indicators)
    if block:
        parts.append(block)

    block = _format_dict_block("⭐ 점수", scores)
    if block:
        parts.append(block)

    if cooldown_sec is not None or cooldown_reason:
        if cooldown_sec is not None:
            # 예: ⏱️ 쿨타임 30.0초 (사유: ...)
            cooldown_line = f"⏱️ 쿨타임 {cooldown_sec:.1f}초"
        else:
            cooldown_line = "⏱️ 쿨타임 -"
        if cooldown_reason:
            cooldown_line += f" (사유: {cooldown_reason})"
        parts.append(cooldown_line)

    if error:
        parts.append(f"⚠️ 에러: {error}")

    if note:
        parts.append(note)

    return "\n".join(parts)


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


def send_structured_tg(
    *,
    title: str,
    indicators: Optional[Dict[str, Any]] = None,
    scores: Optional[Dict[str, Any]] = None,
    cooldown_sec: Optional[float] = None,
    cooldown_reason: Optional[str] = None,
    error: Optional[str] = None,
    note: Optional[str] = None,
) -> None:
    """지표·점수·쿨타임·에러 템플릿을 사용해서 텔레그램을 보내는 헬퍼.

    다른 모듈에서는 가급적 이 함수를 써서 알림 포맷을 통일한다.
    """
    text = build_tg_template(
        title=title,
        indicators=indicators,
        scores=scores,
        cooldown_sec=cooldown_sec,
        cooldown_reason=cooldown_reason,
        error=error,
        note=note,
    )
    send_tg(text)


def _localize_skip_reason(reason: str) -> str:
    """대표적인 스킵 사유들을 한국어로 매핑한다.

    - 기존 reason 문자열은 그대로 두고, 사용자가 보기 쉬운 설명으로 치환.
    - 매핑되지 않은 코드는 원문을 그대로 쓴다.
    """
    # 1) 시그널/중재 관련: no_signal_or_arbitration_rejected
    if "no_signal_or_arbitration_rejected" in reason:
        symbol: Optional[str] = None
        if "symbol=" in reason:
            idx = reason.find("symbol=") + len("symbol=")
            symbol = reason[idx:].strip()
        if symbol:
            return f"시그널이 없거나 중재에서 탈락했습니다. (심볼: {symbol})"
        return "시그널이 없거나 중재에서 탈락했습니다."

    # 2) 잔고 부족 계열
    if reason.startswith("[BALANCE_SKIP]"):
        return "가용 잔고가 부족합니다."

    # 3) 박스장 차단 계열
    if "range_blocked_today" in reason:
        return "오늘은 박스장 진입이 차단된 상태입니다."

    # 4) 그 외: 기존 문자열 그대로 사용
    return reason


def send_skip_tg(reason: str) -> None:
    """신호를 '스킵' 한 이유를 텔레그램으로 보내되, 스팸이 되지 않도록 같은 이유는 일정 시간만에 한 번씩만 보낸다.

    규칙은 원래 bot.py 의 것을 그대로 가져왔다:
    1) reason 이 "[BALANCE_SKIP]" 으로 시작하면 1시간 쿨다운을 건다.
       - 잔고가 없을 때는 1분마다 같은 말이 나올 수 있어서 길게 막는다.
    2) reason 에 "range_blocked_today" 가 포함돼 있으면 이것도 1시간 쿨다운
       - 박스장 진입이 오늘은 막혔다는 알림은 자주 올 필요가 없다.
    3) 그 외의 스킵 사유는 settings.skip_tg_cooldown(기본 30초) 으로 막는다.

    실제 전송이 스킵됐는지 아닌지는 콘솔 로그로 남겨서 추적 가능하게 한다.

    2025-11-14: 템플릿 빌더를 사용해 쿨타임 정보를 함께 표시하도록 변경.
    2025-11-20: 쿨타임/사유 문구를 한국어로 표시하도록 변경.
    """
    SET = load_settings()   # ★ 이렇게 해야 최신 settings 반영됨
    now = time.time()

    # 1) 잔고 관련 스킵은 무조건 길게
    if reason.startswith("[BALANCE_SKIP]"):
        cooldown = SET.balance_skip_cooldown  # 기본 3600초
        title = "스킵 / 잔고"
    # 2) 박스장 막힘 관련 스킵도 길게
    elif "range_blocked_today" in reason:
        cooldown = 3600
        title = "스킵 / 박스장 차단"
    else:
        # 3) 나머지는 짧게 (기본 30초)
        cooldown = SET.skip_tg_cooldown
        title = "스킵"

    last_ts = LAST_SKIP_TG.get(reason, 0.0)
    if now - last_ts >= cooldown:
        # 사용자에게 보여 줄 설명은 한국어로 변환
        localized_reason = _localize_skip_reason(reason)

        # 쿨다운이 지났으면 실제로 텔레그램 전송
        text = build_tg_template(
            title=title,
            indicators=None,
            scores=None,
            cooldown_sec=float(cooldown),
            cooldown_reason=localized_reason,
            error=None,
            note=None,
        )
        send_tg(text)
        LAST_SKIP_TG[reason] = now
    else:
        # 아직 쿨다운 중이면 콘솔에만 남겨둔다.
        # 이렇게 하면 왜 텔레그램이 안 나갔는지 로그로 확인 가능하다.
        log(f"[SKIP_TG_SUPPRESS] {reason}")


__all__ = [
    "log",
    "send_tg",
    "send_structured_tg",
    "send_skip_tg",
    "build_tg_template",
    "LAST_SKIP_TG",
    "SET",
    "KST",
]
