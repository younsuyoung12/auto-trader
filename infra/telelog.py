from __future__ import annotations

"""
========================================================
FILE: infra/telelog.py
STRICT · NO-FALLBACK · IMPORT-SAFE
========================================================
역할:
- 콘솔 로그 출력
- 텔레그램 전송
- 텔레그램 메시지를 운영자 기준의 쉬운 한글 문장으로 변환
- '스킵 사유' 텔레그램 전송에 쿨다운 적용

중요 원칙(STRICT):
- import 시점에 settings.load_settings() 호출 금지 (IMPORT-SAFE)
- 민감정보(키/토큰/시크릿) 로그 출력 금지
- 텔레그램 전송 실패/네트워크 오류는 봇 전체를 멈추게 하지 않는다(유틸 보호)
- 파일 로그 쓰기 실패도 봇을 멈추게 하지 않는다(유틸 보호)
- print() 금지 / logging 사용
- env-first 정책 유지 (이 파일은 운영 유틸 예외 모듈)

설정 소스:
- 환경변수(env-first)만 사용한다.
  - TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
  - LOG_TO_FILE (true/false), LOG_FILE
  - SKIP_TG_COOLDOWN_SEC, BALANCE_SKIP_COOLDOWN_SEC
  - TELEGRAM_TIMEOUT_SEC

변경 이력:
- 2026-03-13
  1) FIX(LOGGING): print() 제거, logging 기반 콘솔/파일 로그로 전환
  2) FIX(CONCURRENCY): LAST_SKIP_TG 접근에 RLock 적용
  3) FIX(IMPORT-SAFE): env-first / import-safe 유지하면서 logger lazy configure 구조로 정리
  4) FIX(UTILITY): telegram/file logging 실패는 유틸 보호 원칙대로 유지
- 2026-03-11
  1) 텔레그램 원문 로그를 사람이 읽기 쉬운 한글 운영 메시지로 변환하는
     _humanize_telegram_text(...) 파이프라인 추가
  2) PRE-FLIGHT OK / PRE-FLIGHT FAILED / DATA HEALTH OK / 자동매매 시작 /
     [SKIP][...] entry blocked 메시지 한글화 추가
  3) orderflow / direction stability 주요 reason code를 쉬운 한국어 설명으로 변환
  4) UTC / epoch 시각을 KST로 병기해 텔레그램에서 바로 이해 가능하도록 개선
  5) Telegram API 응답 코드 검증 추가 (유틸 보호 원칙 유지)
========================================================
"""

import logging
import os
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import requests

# KST(UTC+9) — 호환용으로 제공
KST = timezone(timedelta(hours=9))

# 스킵 텔레그램 쿨다운 관리용 딕셔너리
# key: reason 문자열, value: 마지막으로 이 reason 을 전송한 epoch 시각(float)
LAST_SKIP_TG: Dict[str, float] = {}

# internal logger state
_LOGGER = logging.getLogger("auto_trader.telelog")
_LOGGER.setLevel(logging.INFO)
_LOGGER.propagate = False

_STREAM_HANDLER_INSTALLED = False
_FILE_HANDLER_PATH: Optional[str] = None
_STATE_LOCK = threading.RLock()


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
        out = float(str(v).strip())
    except Exception as e:
        raise RuntimeError(f"invalid float env: {key}") from e
    return out


# ─────────────────────────────────────────────────────
# logging (IMPORT-SAFE / lazy configure)
# ─────────────────────────────────────────────────────
def _ensure_logger_configured() -> None:
    global _STREAM_HANDLER_INSTALLED, _FILE_HANDLER_PATH

    with _STATE_LOCK:
        if not _STREAM_HANDLER_INSTALLED:
            stream_handler = logging.StreamHandler()
            stream_handler.setLevel(logging.INFO)
            stream_handler.setFormatter(logging.Formatter("[%(asctime)s] %(message)s", "%Y-%m-%d %H:%M:%S"))
            _LOGGER.addHandler(stream_handler)
            _STREAM_HANDLER_INSTALLED = True

        log_to_file = _env_bool("LOG_TO_FILE", False)
        desired_path = _env_str("LOG_FILE", "logs/bot.log") if log_to_file else None

        if _FILE_HANDLER_PATH == desired_path:
            return

        # remove old file handlers if path changed or disabled
        for handler in list(_LOGGER.handlers):
            if isinstance(handler, logging.FileHandler):
                try:
                    _LOGGER.removeHandler(handler)
                    handler.close()
                except Exception:
                    pass

        _FILE_HANDLER_PATH = None

        if desired_path is None:
            return

        try:
            os.makedirs(os.path.dirname(desired_path) or ".", exist_ok=True)
            file_handler = logging.FileHandler(desired_path, encoding="utf-8")
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(logging.Formatter("[%(asctime)s] %(message)s", "%Y-%m-%d %H:%M:%S"))
            _LOGGER.addHandler(file_handler)
            _FILE_HANDLER_PATH = desired_path
        except Exception:
            # 유틸 보호: 파일 로그 실패로 봇을 멈추지 않는다.
            _FILE_HANDLER_PATH = None


def log(msg: str) -> None:
    """
    콘솔에 로그를 남기고, 필요하면 파일에도 남긴다.

    - 시간 포맷: "YYYY-MM-DD HH:MM:SS"
    - print 금지 / logging 사용
    - 파일 로그 쓰기 실패는 유틸 보호 차원에서만 무시(봇 중단 방지)
    """
    try:
        _ensure_logger_configured()
    except Exception:
        # logger configure 실패도 유틸 보호
        pass

    line = str(msg if isinstance(msg, str) else repr(msg))
    try:
        _LOGGER.info(line)
    except Exception:
        # 최후 보호: 로깅 실패가 엔진을 멈추지 않게 한다.
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
# human-readable telegram helpers
# ─────────────────────────────────────────────────────
def _parse_simple_key_values(text: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        out[key.strip()] = value.strip()
    return out


def _format_utc_or_keep(raw: str) -> str:
    value = str(raw).strip()
    if value == "":
        return value
    try:
        normalized = value.replace("Z", "+00:00")
        dt_utc = datetime.fromisoformat(normalized)
        if dt_utc.tzinfo is None:
            dt_utc = dt_utc.replace(tzinfo=timezone.utc)
        dt_kst = dt_utc.astimezone(KST)
        return dt_kst.strftime("%Y-%m-%d %H:%M:%S KST")
    except Exception:
        return value


def _format_epoch_kst_or_keep(raw: str) -> str:
    value = str(raw).strip()
    if value == "":
        return value
    try:
        epoch = float(value)
        dt_kst = datetime.fromtimestamp(epoch, tz=timezone.utc).astimezone(KST)
        return dt_kst.strftime("%Y-%m-%d %H:%M:%S KST")
    except Exception:
        return value


def _side_to_kr(side: str) -> str:
    s = str(side).strip().upper()
    if s == "LONG":
        return "롱"
    if s == "SHORT":
        return "숏"
    return s


def _filter_to_kr(name: str) -> str:
    mapping = {
        "STRUCTURE_FILTER": "구조 필터",
        "DIRECTION_STABILITY": "방향 안정성 필터",
        "RISK_FILTER": "리스크 필터",
        "REGIME_FILTER": "장세 필터",
        "FLOW_FILTER": "체결 흐름 필터",
    }
    return mapping.get(name, name)


def _stage_to_kr(name: str) -> str:
    mapping = {
        "SETTINGS": "설정 검사",
        "DB": "DB 연결 점검",
        "BINANCE_API": "바이낸스 API 연결 점검",
        "WS_BOOTSTRAP": "실시간 데이터 초기 부팅",
        "UNIFIED_FEATURES": "전략용 통합 피처 생성 점검",
        "PIPELINE_SIMULATION": "전략 파이프라인 시뮬레이션",
        "GPT_PING": "GPT 연결 점검",
        "META_L3_OBSERVABILITY": "성과 관측 모듈 점검",
        "EXECUTION_DRY_RUN": "주문 사전 실행 점검",
    }
    return mapping.get(name, name)


def _explain_meta_not_ready(extra: str) -> str:
    text = str(extra).strip()
    if "no closed trades found for symbol=" in text:
        match = re.search(r"symbol=([A-Z0-9_]+)", text)
        symbol = match.group(1) if match else "해당 종목"
        return (
            f"아직 {symbol} 종료 거래가 없어 성과 통계를 만들 수 없습니다. "
            "이 항목만 준비 전 상태이며 자동매매 시작 자체를 막는 오류는 아닙니다."
        )
    return text


def _describe_skip_reason(reason: str, side: str) -> tuple[str, list[str]]:
    reason_text = str(reason).strip()
    side_kr = _side_to_kr(side)

    if reason_text.startswith("orderflow_sell_dominant_long_block"):
        return (
            "실시간 호가 흐름에서 매도세가 더 강해 롱 진입을 막았습니다.",
            [
                "현재 시장에서는 사려는 힘보다 파는 힘이 더 강합니다.",
                "이 상태에서 롱에 들어가면 바로 눌릴 가능성이 있어 진입을 취소했습니다.",
                "결과적으로 이번 롱 시도는 안전장치에 의해 보류되었습니다.",
            ],
        )

    if reason_text.startswith("orderflow_buy_dominant_short_block"):
        return (
            "실시간 호가 흐름에서 매수세가 더 강해 숏 진입을 막았습니다.",
            [
                "현재 시장에서는 파는 힘보다 사려는 힘이 더 강합니다.",
                "이 상태에서 숏에 들어가면 위로 밀릴 가능성이 있어 진입을 취소했습니다.",
                "결과적으로 이번 숏 시도는 안전장치에 의해 보류되었습니다.",
            ],
        )

    if reason_text.startswith("primary_5m_momentum_opposed"):
        match_state = re.search(r"state=([A-Z]+)", reason_text)
        match_slope = re.search(r"slope_pct=([-0-9.]+)", reason_text)
        opposed_state = match_state.group(1) if match_state else "UNKNOWN"
        slope_pct = match_slope.group(1) if match_slope else None
        detail = f"5분 기준 기본 방향이 현재 {opposed_state} 쪽이라 {side_kr} 진입과 충돌했습니다."
        bullets = [
            "가장 기본이 되는 5분 방향성이 이번 진입 방향과 반대입니다.",
            "즉, 전략 일부는 진입을 보았더라도 핵심 흐름은 아직 반대라고 본 것입니다.",
        ]
        if slope_pct is not None:
            bullets.append(f"참고로 5분 기울기 값은 {slope_pct} 입니다.")
        bullets.append(f"결과적으로 이번 {side_kr} 진입은 방향 충돌로 취소되었습니다.")
        return detail, bullets

    if reason_text.startswith("primary_5m_momentum_neutral"):
        match_slope = re.search(r"slope_pct=([-0-9.]+)", reason_text)
        slope_pct = match_slope.group(1) if match_slope else None
        detail = f"5분 기준 기본 방향성이 약해 아직 뚜렷한 {side_kr} 흐름으로 보기 어렵습니다."
        bullets = [
            "한쪽으로 강하게 밀어주는 방향성이 아직 부족합니다.",
            f"이 상태에서는 억지 진입보다 관망이 더 안전하다고 판단했습니다.",
        ]
        if slope_pct is not None:
            bullets.append(f"참고로 5분 기울기 값은 {slope_pct} 입니다.")
        bullets.append(f"결과적으로 이번 {side_kr} 진입은 중립 구간으로 판단되어 취소되었습니다.")
        return detail, bullets

    if "range_blocked_today" in reason_text:
        return (
            "오늘은 박스장 진입 제한 정책 때문에 진입을 막았습니다.",
            [
                "현재 장세가 박스권으로 판단되었거나, 오늘 박스장 진입 제한이 이미 발동된 상태입니다.",
                f"결과적으로 이번 {side_kr} 진입은 정책적으로 차단되었습니다.",
            ],
        )

    return (
        f"{side_kr} 진입 후보가 있었지만 내부 안전장치가 이번 진입을 차단했습니다.",
        [
            f"세부 내부 코드: {reason_text}",
            "이 코드는 전략 내부 판단 사유를 그대로 담고 있습니다.",
            "필요하면 이 코드에 맞춰 추가 한글 설명을 계속 붙일 수 있습니다.",
        ],
    )


def _humanize_preflight_ok(text: str) -> str:
    lines = [line.rstrip() for line in text.splitlines() if line.strip()]
    fields = _parse_simple_key_values(text)

    symbol = fields.get("Symbol", "-")
    host = fields.get("Host", "-")
    utc_value = fields.get("UTC", "-")
    kst_value = _format_utc_or_keep(utc_value)

    stage_lines: list[str] = []
    stage_pattern = re.compile(r"^- ([A-Z0-9_]+): ([0-9]+)ms(?: \[(.+)\])?$")

    for raw_line in lines:
        match = stage_pattern.match(raw_line)
        if not match:
            continue
        stage_name = match.group(1)
        latency_ms = match.group(2)
        extra = match.group(3)
        stage_label = _stage_to_kr(stage_name)

        if extra:
            if stage_name == "META_L3_OBSERVABILITY":
                stage_lines.append(
                    f"• {stage_label}: 참고 상태 ({latency_ms}ms)\n"
                    f"  - {_explain_meta_not_ready(extra)}"
                )
            else:
                stage_lines.append(
                    f"• {stage_label}: 추가 확인 필요 ({latency_ms}ms)\n"
                    f"  - 원문: {extra}"
                )
        else:
            stage_lines.append(f"• {stage_label}: 정상 ({latency_ms}ms)")

    parts: list[str] = [
        "✅ 자동매매 시작 전 점검 완료",
        "",
        f"종목: {symbol}",
        f"서버: {host}",
        f"한국시간 기준: {kst_value}",
        "",
        "세부 점검 결과",
    ]

    if stage_lines:
        parts.extend(stage_lines)
    else:
        parts.append("• 세부 단계 정보 파싱 실패 - 원문을 확인해 주세요.")

    parts.extend(
        [
            "",
            "결론",
            "• 필수 사전 점검은 통과했습니다.",
            "• 따라서 자동매매 시작은 가능한 상태입니다.",
        ]
    )
    return "\n".join(parts)


def _humanize_preflight_failed(text: str) -> str:
    fields = _parse_simple_key_values(text)

    stage = fields.get("Stage", "-")
    host = fields.get("Host", "-")
    utc_value = fields.get("UTC", "-")
    kst_value = _format_utc_or_keep(utc_value)

    error_lines: list[str] = []
    for key, value in fields.items():
        if key in {"Stage", "Host", "UTC"}:
            continue
        error_lines.append(f"• {key}: {value}")

    parts: list[str] = [
        "⛔ 자동매매 시작 전 점검 실패",
        "",
        f"실패 단계: {_stage_to_kr(stage)}",
        f"서버: {host}",
        f"한국시간 기준: {kst_value}",
        "",
        "실패 내용",
    ]

    if error_lines:
        parts.extend(error_lines)
    else:
        parts.append("• 상세 실패 내용 파싱 실패 - 원문 확인 필요")

    parts.extend(
        [
            "",
            "의미",
            "• 사전 점검 단계에서 문제를 발견해 자동매매 시작을 차단했습니다.",
            "• 즉, 위험한 상태로 봇이 실행되는 것을 막은 정상적인 보호 동작입니다.",
        ]
    )
    return "\n".join(parts)


def _humanize_data_health_ok(text: str) -> str:
    fields = _parse_simple_key_values(text)

    symbol = fields.get("- symbol", fields.get("symbol", "-"))
    last_check_ts_raw = fields.get("- last_check_ts", fields.get("last_check_ts", "-"))
    last_check_kst = _format_epoch_kst_or_keep(last_check_ts_raw)

    ws_ok = "정상" if ("WS/ORDERBOOK 정상" in text or "WS 정상" in text) else "확인 필요"
    feature_ok = "정상" if "feature 생성 정상" in text else "확인 필요"
    gpt_ok = "사용 가능" if "GPT 사용 가능" in text else "확인 필요"

    parts = [
        "✅ 실시간 데이터 상태 정상",
        "",
        f"종목: {symbol}",
        f"최근 점검 시각(한국시간): {last_check_kst}",
        "",
        "현재 상태",
        f"• 실시간 WebSocket / 오더북: {ws_ok}",
        f"• 전략용 피처 생성: {feature_ok}",
        f"• GPT 분석 연결: {gpt_ok}",
        "",
        "결론",
        "• 자동매매에 필요한 실시간 데이터 흐름은 현재 정상입니다.",
    ]
    return "\n".join(parts)


def _humanize_bot_start_message(text: str) -> str:
    if "Binance USDT-M Futures 자동매매(WS 버전)를 시작합니다." not in text:
        return text
    return (
        "✅ 자동매매를 시작합니다.\n\n"
        "의미\n"
        "• 사전 점검이 끝났고\n"
        "• 실시간 데이터 수신과 전략 엔진 가동을 시작했다는 뜻입니다."
    )


def _humanize_skip_entry_block(text: str) -> Optional[str]:
    pattern = re.compile(
        r"^\[SKIP\]\[(?P<filter>[A-Z_]+)\]\s+entry blocked:\s+"
        r"(?P<symbol>[A-Z0-9_]+)\s+\((?P<side>LONG|SHORT)\)\s+reason=(?P<reason>.+)$"
    )
    match = pattern.match(text.strip())
    if not match:
        return None

    filter_name = match.group("filter")
    symbol = match.group("symbol")
    side = match.group("side")
    reason = match.group("reason").strip()

    lead, bullets = _describe_skip_reason(reason, side)

    parts: list[str] = [
        f"⛔ {_side_to_kr(side)} 진입 보류",
        "",
        f"종목: {symbol}",
        f"차단 단계: {_filter_to_kr(filter_name)}",
        "",
        "쉽게 말하면",
        lead,
        "",
        "자세한 설명",
    ]
    for bullet in bullets:
        parts.append(f"• {bullet}")

    parts.extend(
        [
            "",
            "내부 코드",
            f"• {reason}",
        ]
    )
    return "\n".join(parts)


def _humanize_telegram_text(text: str) -> str:
    raw = str(text).strip()
    if raw == "":
        return raw

    if raw.startswith("📌 "):
        return raw

    if raw.startswith("✅ PRE-FLIGHT OK"):
        return _humanize_preflight_ok(raw)

    if raw.startswith("⛔ PRE-FLIGHT FAILED"):
        return _humanize_preflight_failed(raw)

    if raw.startswith("✅ [DATA HEALTH OK]"):
        return _humanize_data_health_ok(raw)

    if "Binance USDT-M Futures 자동매매(WS 버전)를 시작합니다." in raw:
        return _humanize_bot_start_message(raw)

    humanized_skip = _humanize_skip_entry_block(raw)
    if humanized_skip is not None:
        return humanized_skip

    return raw


# ─────────────────────────────────────────────────────
# telegram send
# ─────────────────────────────────────────────────────
def send_tg(text: str) -> None:
    """
    Telegram 메시지 전송

    STRICT POLICY:
    - 텔레그램 실패로 엔진 중단 금지
    - 토큰 없으면 콘솔 로그 fallback
    - HTTP 오류는 로그만 남김
    """

    bot_token = _env_str("TELEGRAM_BOT_TOKEN", "")
    chat_id = _env_str("TELEGRAM_CHAT_ID", "")

    final_text = _humanize_telegram_text(text)

    # 토큰 없는 경우
    if not bot_token or not chat_id:
        log("[TG DISABLED] " + final_text)
        return

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    payload = {
        "chat_id": chat_id,
        "text": final_text,
    }

    try:
        resp = requests.post(url, json=payload, timeout=8)

        # 성공
        if resp.status_code == 200:
            log("[TG OK] " + final_text)
            return

        # 인증 실패
        if resp.status_code == 401:
            log("[TG AUTH ERROR] invalid telegram token")
            return

        # 기타 오류
        log(
            f"[TG HTTP ERROR] status={resp.status_code} "
            f"body={resp.text[:200]}"
        )

    except requests.exceptions.Timeout:
        log("[TG TIMEOUT] telegram request timeout")

    except requests.exceptions.ConnectionError:
        log("[TG NETWORK ERROR] telegram connection error")

    except Exception as e:
        log(f"[TG ERROR] {type(e).__name__}: {e}")

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
        return "가용 잔고가 부족해 진입을 생략했습니다."

    if "range_blocked_today" in reason:
        return "오늘은 박스장 진입 제한 정책 때문에 진입을 생략했습니다."

    if "orderflow_sell_dominant_long_block" in reason:
        return "현재 매도세가 더 강해 롱 진입을 생략했습니다."

    if "orderflow_buy_dominant_short_block" in reason:
        return "현재 매수세가 더 강해 숏 진입을 생략했습니다."

    return reason


def send_skip_tg(reason: str) -> None:
    """스킵 사유 전송 (reason 별 쿨다운 적용)."""
    localized_reason_key = str(reason).strip()
    if localized_reason_key == "":
        log("[SKIP_TG_SUPPRESS] empty_reason")
        return

    now = time.time()

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

    with _STATE_LOCK:
        last_ts = LAST_SKIP_TG.get(localized_reason_key, 0.0)
        if now - last_ts >= cooldown:
            localized_reason = _localize_skip_reason(reason)
            text = build_tg_template(
                title=title,
                cooldown_sec=float(cooldown),
                cooldown_reason=localized_reason,
            )
            send_tg(text)
            LAST_SKIP_TG[localized_reason_key] = now
            return

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