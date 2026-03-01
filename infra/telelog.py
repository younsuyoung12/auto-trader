"""
========================================================
infra/telelog.py
STRICT · NO-FALLBACK · IMPORT-SAFE
========================================================
역할:
- 콘솔 로그 출력
- 텔레그램 전송
- '스킵 사유' 텔레그램 전송에 쿨다운 적용

중요 원칙(STRICT):
- import 시점에 settings.load_settings() 호출 금지 (IMPORT-SAFE)
- 민감정보(키/토큰/시크릿) 로그 출력 금지
- 텔레그램 전송 실패/네트워크 오류는 봇 전체를 멈추게 하지 않는다(유틸 보호)
- 파일 로그 쓰기 실패도 봇을 멈추게 하지 않는다(유틸 보호)

설정 소스:
- 환경변수(env-first)만 사용한다.
  - TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
  - LOG_TO_FILE (true/false), LOG_FILE
  - SKIP_TG_COOLDOWN_SEC, BALANCE_SKIP_COOLDOWN_SEC
========================================================
"""

from __future__ import annotations

import os
import time
from datetime import timedelta, timezone
from typing import Any, Dict, Optional

import requests

# KST(UTC+9) — 호환용으로 제공
KST = timezone(timedelta(hours=9))

# 스킵 텔레그램 쿨다운 관리용 딕셔너리
# key: reason 문자열, value: 마지막으로 이 reason 을 전송한 epoch 시각(float)
LAST_SKIP_TG: Dict[str, float] = {}


# ─────────────────────────────────────────────────────
# env helpers (STRICT: 타입 파싱 실패 시 예외)
# ─────────────────────────────────────────────────────
def _env_str(key: str, default: str = "") -> str:
    v = os.environ.get(key)
    if v is None:
        return default
    vv = str(v).strip()
    return vv if vv != "" else default


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.environ.get(key)
    if v is None:
        return default
    vv = str(v).strip().lower()
    if vv in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if vv in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise RuntimeError(f"invalid bool env: {key}")


def _env_float(key: str, default: float) -> float:
    v = os.environ.get(key)
    if v is None or str(v).strip() == "":
        return default
    try:
        return float(str(v).strip())
    except Exception as e:
        raise RuntimeError(f"invalid float env: {key}") from e


# ─────────────────────────────────────────────────────
# logging
# ─────────────────────────────────────────────────────
def log(msg: str) -> None:
    """콘솔에 로그를 남기고, 필요하면 파일에도 남긴다.

    - 시간 포맷: "YYYY-MM-DD HH:MM:SS"
    - flush=True
    - 파일 로그 쓰기 실패는 유틸 보호 차원에서만 무시(봇 중단 방지)
    """
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)

    log_to_file = _env_bool("LOG_TO_FILE", False)
    if not log_to_file:
        return

    log_file = _env_str("LOG_FILE", "logs/bot.log")
    try:
        os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        # 유틸 보호: 파일 로그 때문에 봇이 죽으면 안 됨
        pass


# ─────────────────────────────────────────────────────
# telegram template
# ─────────────────────────────────────────────────────
def _format_dict_block(title: str, data: Optional[Dict[str, Any]]) -> str:
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
    parts: list[str] = [f"📌 {title}"]

    block = _format_dict_block("📊 지표", indicators)
    if block:
        parts.append(block)

    block = _format_dict_block("⭐ 점수", scores)
    if block:
        parts.append(block)

    if cooldown_sec is not None or cooldown_reason:
        if cooldown_sec is not None:
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


# ─────────────────────────────────────────────────────
# telegram send
# ─────────────────────────────────────────────────────
def send_tg(text: str) -> None:
    """중요 알림을 텔레그램으로 보낸다.

    - 토큰/chat_id 없으면 콘솔 로그로 대체
    - 네트워크 오류는 유틸 보호 차원에서만 무시
    """
    bot_token = _env_str("TELEGRAM_BOT_TOKEN", "")
    chat_id = _env_str("TELEGRAM_CHAT_ID", "")

    if not (bot_token and chat_id):
        log("[TG SKIP] " + text)
        return

    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {"chat_id": chat_id, "text": text}
        requests.post(url, json=payload, timeout=8)
        log("[TG OK] " + text)
    except Exception as e:
        log(f"[TG ERROR] {type(e).__name__}: {e} | {text}")


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


# ─────────────────────────────────────────────────────
# skip reason localization
# ─────────────────────────────────────────────────────
def _localize_skip_reason(reason: str) -> str:
    if "no_signal_or_arbitration_rejected" in reason:
        symbol: Optional[str] = None
        if "symbol=" in reason:
            idx = reason.find("symbol=") + len("symbol=")
            symbol = reason[idx:].strip()
        if symbol:
            return f"시그널이 없거나 중재에서 탈락했습니다. (심볼: {symbol})"
        return "시그널이 없거나 중재에서 탈락했습니다."

    if reason.startswith("[BALANCE_SKIP]"):
        return "가용 잔고가 부족합니다."

    if "range_blocked_today" in reason:
        return "오늘은 박스장 진입이 차단된 상태입니다."

    return reason


def send_skip_tg(reason: str) -> None:
    """스킵 사유 전송 (reason 별 쿨다운 적용)."""
    now = time.time()

    # 쿨다운 설정 (env-first)
    # - 기본값: 일반 스킵 30초 / 잔고 스킵 3600초
    default_skip_cooldown = _env_float("SKIP_TG_COOLDOWN_SEC", 30.0)
    balance_skip_cooldown = _env_float("BALANCE_SKIP_COOLDOWN_SEC", 3600.0)

    if reason.startswith("[BALANCE_SKIP]"):
        cooldown = balance_skip_cooldown
        title = "스킵 / 잔고"
    elif "range_blocked_today" in reason:
        cooldown = 3600.0
        title = "스킵 / 박스장 차단"
    else:
        cooldown = default_skip_cooldown
        title = "스킵"

    last_ts = LAST_SKIP_TG.get(reason, 0.0)
    if now - last_ts >= cooldown:
        localized_reason = _localize_skip_reason(reason)
        text = build_tg_template(
            title=title,
            cooldown_sec=float(cooldown),
            cooldown_reason=localized_reason,
        )
        send_tg(text)
        LAST_SKIP_TG[reason] = now
    else:
        log(f"[SKIP_TG_SUPPRESS] {reason}")


__all__ = [
    "log",
    "send_tg",
    "send_structured_tg",
    "send_skip_tg",
    "build_tg_template",
    "LAST_SKIP_TG",
    "KST",
]