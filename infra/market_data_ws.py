# market_data_ws.py
from __future__ import annotations

"""
market_data_ws.py (Binance USDT-M Futures 전용 WebSocket 버퍼 모듈)
====================================================================

역할
-----------------------------------------------------
Binance USDT-M Futures WebSocket을 통해

1) 멀티 타임프레임 K라인(kline)
2) depth5 오더북(order book)

을 수신하여 메모리 버퍼에 저장하고,
상위 레이어(run_bot_ws, position_watch_ws 등)에
getter / 데이터 헬스 체크 유틸을 제공한다.

Binance 기준 스트림 구조
-----------------------------------------------------
KLINE:
  wss://fstream.binance.com/stream?streams=
    btcusdt@kline_1m/
    btcusdt@kline_5m/
    btcusdt@kline_15m ...

Depth:
  btcusdt@depth5@100ms

핵심 정책 (STRICT)
-----------------------------------------------------
- WS 원본 데이터는 보정/추론/폴백하지 않는다.
- 캔들/오더북이 비정상일 경우 상위 레이어에서 FAIL 처리.
- 이 파일은 "데이터 수신 + 버퍼링 + 헬스 체크"만 수행.
- 주문/전략/EXIT 로직은 절대 포함하지 않는다.
"""

# Binance 전환 요약 블록
# -----------------------------------------------------
# - Binance USDT-M Futures multiplex WebSocket(/stream?streams=...) 사용
# - kline: payload["data"]["k"] 기준 파싱 (모든 업데이트 그대로 버퍼 반영)
# - depth: payload["data"]["bids"]/["asks"] 기준 파싱
# - 별도 subscribe 메시지 전송 없음 (URL 기반 구독)
# - 로그 prefix: [MD_BINANCE_WS]
# - STRICT / NO-FALLBACK 유지 (보정/추정/가짜 데이터 생성 금지)

import gzip
import io
import json
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import websocket  # pip install websocket-client

from settings import load_settings
from infra.telelog import log

SET = load_settings()

# Binance Futures WebSocket base (/stream multiplex endpoint)
WS_BASE_URL = (
    os.getenv("BINANCE_FUTURES_WS_BASE")
    or os.getenv("BINANCE_WS_BASE")
    or "wss://fstream.binance.com/stream"
)

# Binance Kline 기본 타임프레임 세트 (분/시간/일/주/월)
DEFAULT_INTERVALS: List[str] = [
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "6h",
    "12h",
    "1d",
    "3d",
    "1w",
    "1M",
]

# settings 에 없으면 DEFAULT_INTERVALS 로 기본 구독
WS_INTERVALS: List[str] = list(getattr(SET, "ws_subscribe_tfs", None) or DEFAULT_INTERVALS)

# 데이터 헬스 체크용 필수 타임프레임 / 지연/버퍼 기준값
REQUIRED_INTERVALS: List[str] = list(getattr(SET, "ws_required_tfs", None) or ["1m", "5m", "15m"])

KLINE_MIN_BUFFER: int = int(getattr(SET, "ws_min_kline_buffer", 60))
KLINE_MAX_DELAY_SEC: float = float(getattr(SET, "ws_max_kline_delay_sec", 120.0))
ORDERBOOK_MAX_DELAY_SEC: float = float(getattr(SET, "ws_orderbook_max_delay_sec", 10.0))

# { (symbol, interval): [(ts, o, h, l, c, v), ...] }
_kline_buffers: Dict[Tuple[str, str], List[Tuple[int, float, float, float, float, float]]] = {}

# { (symbol, interval): last_recv_ts_ms }
_kline_last_recv_ts: Dict[Tuple[str, str], int] = {}

# { symbol: {"bids": [...], "asks": [...], "ts": ..., "exchTs": ..., "bestBid": ..., "bestAsk": ..., "spreadPct": ...} }
_orderbook_buffers: Dict[str, Dict[str, Any]] = {}

# 동시성 보호용 Lock
_kline_lock = threading.Lock()
_orderbook_lock = threading.Lock()

# 각 버퍼에 최대 500개 유지
MAX_KLINES = 500


def _now_ms() -> int:
    """현재 시각(ms) 반환."""
    return int(time.time() * 1000)


def _safe_dump_for_log(obj: Any, max_len: int = 2000) -> str:
    """로그 출력용 안전 문자열 변환."""
    try:
        s = json.dumps(obj, ensure_ascii=False, default=str)
    except Exception:
        s = str(obj)
    if len(s) > max_len:
        return s[:max_len] + f"... (truncated, total_len={len(s)})"
    return s


def _normalize_symbol(sym: str) -> str:
    """내부 저장용 심볼 표준화 (예: BTCUSDT)."""
    return (sym or "").upper().strip()


def _to_stream_symbol(sym: str) -> str:
    """Binance stream 이름용 심볼 (소문자, 예: btcusdt)."""
    return _normalize_symbol(sym).lower()


def _build_stream_names(symbol: str) -> List[str]:
    """심볼 기준 multiplex stream 목록 생성 (kline 멀티 TF + depth5)."""
    s = _to_stream_symbol(symbol)
    streams: List[str] = [f"{s}@kline_{iv}" for iv in WS_INTERVALS]
    streams.append(f"{s}@depth5@100ms")
    return streams


def _build_ws_url(symbol: str) -> str:
    """Binance multiplex WS URL 생성."""
    streams = _build_stream_names(symbol)
    return f"{WS_BASE_URL}?streams={'/'.join(streams)}"


def _push_kline(symbol: str, interval: str, kline_obj: Dict[str, Any]) -> None:
    """Binance kline payload['k'] 를 내부 버퍼에 반영한다.

    STRICT:
    - ts<=0 이면 버퍼 오염 방지를 위해 폐기
    - o/h/l/c/v 는 float 변환만 수행 (보정/추정 없음)
    - x(종가 확정 여부)는 참고값일 뿐 버퍼 반영 기준으로 사용하지 않음
    """
    sym = _normalize_symbol(symbol)
    key = (sym, interval)

    try:
        ts = int(kline_obj.get("t") or 0)
    except Exception:
        ts = 0

    if ts <= 0:
        try:
            log(
                f"[MD_BINANCE_WS WARN] invalid kline ts for {sym} {interval}: "
                f"payload={_safe_dump_for_log(kline_obj)}"
            )
        except Exception:
            pass
        return

    try:
        o = float(kline_obj.get("o"))
        h = float(kline_obj.get("h"))
        l = float(kline_obj.get("l"))
        c = float(kline_obj.get("c"))
        v = float(kline_obj.get("v"))
    except Exception:
        try:
            log(
                f"[MD_BINANCE_WS WARN] invalid kline numeric fields for {sym} {interval}: "
                f"payload={_safe_dump_for_log(kline_obj)}"
            )
        except Exception:
            pass
        return

    now_ms = _now_ms()
    with _kline_lock:
        buf = _kline_buffers.setdefault(key, [])
        before_len = len(buf)

        if buf and buf[-1][0] == ts:
            # 동일 openTime(ts) 캔들은 최신 업데이트로 대체
            buf[-1] = (ts, o, h, l, c, v)
        else:
            buf.append((ts, o, h, l, c, v))
            if len(buf) > MAX_KLINES:
                del buf[0 : len(buf) - MAX_KLINES]

        after_len = len(buf)
        _kline_last_recv_ts[key] = now_ms

    if getattr(SET, "ws_log_enabled", True):
        try:
            age_ms = max(0, now_ms - ts)
            is_closed = bool(kline_obj.get("x", False))
            log(
                f"[MD_BINANCE_WS] {sym} {interval} kline updated "
                f"(ts={ts}, age_ms={age_ms}, buf_len={after_len}, added={after_len - before_len}, closed={is_closed})"
            )
        except Exception:
            pass


def _normalize_depth_side(side_val: Any) -> List[List[float]]:
    """depth bids/asks 를 [[price, qty], ...] float 형태로 변환."""
    if not side_val:
        return []

    out: List[List[float]] = []
    for row in side_val:
        if isinstance(row, (list, tuple)) and len(row) >= 2:
            try:
                price = float(row[0])
                qty = float(row[1])
                out.append([price, qty])
            except Exception:
                continue
        elif isinstance(row, dict):
            # 일부 테스트/사내 래퍼 포맷 호환 (보정 아님: 키만 읽음)
            try:
                price = float(row.get("price"))
                qty = float(row.get("qty"))
                out.append([price, qty])
            except Exception:
                continue
    return out


def _compute_spread_pct(bids: List[List[float]], asks: List[List[float]]) -> Optional[float]:
    """오더북 상단 bid/ask 기준 spread(%) 계산."""
    if not bids or not asks:
        return None
    try:
        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
    except Exception:
        return None
    if best_bid <= 0 or best_ask <= 0 or best_ask <= best_bid:
        return None
    mid = (best_bid + best_ask) / 2.0
    return (best_ask - best_bid) / mid * 100.0


def _push_orderbook(symbol: str, payload: Dict[str, Any]) -> None:
    """Binance depth payload 를 내부 버퍼에 저장한다.

    STRICT:
    - bids/asks 는 float 변환만 수행
    - 누락/비정상은 상위 헬스 체크에서 FAIL 처리
    """
    sym = _normalize_symbol(symbol)

    normalized_bids = _normalize_depth_side(payload.get("bids") or [])
    normalized_asks = _normalize_depth_side(payload.get("asks") or [])

    now_ms = _now_ms()
    spread_pct = _compute_spread_pct(normalized_bids, normalized_asks)

    ob: Dict[str, Any] = {
        "bids": normalized_bids,
        "asks": normalized_asks,
        "ts": now_ms,  # 로컬 수신 시각
    }

    if "lastUpdateId" in payload:
        ob["lastUpdateId"] = payload.get("lastUpdateId")

    exch_ts = payload.get("E") or payload.get("T")
    if exch_ts is not None:
        try:
            ob["exchTs"] = int(exch_ts)
        except Exception:
            pass

    if normalized_bids:
        ob["bestBid"] = normalized_bids[0][0]
    if normalized_asks:
        ob["bestAsk"] = normalized_asks[0][0]
    if spread_pct is not None:
        ob["spreadPct"] = spread_pct

    with _orderbook_lock:
        _orderbook_buffers[sym] = ob


def _decode_msg(raw: Any) -> Any:
    """수신 프레임을 안전하게 JSON 파싱한다.

    Binance는 일반적으로 text JSON 을 사용한다.
    바이너리/압축 프레임이 들어오는 환경도 방어적으로 처리한다.
    """
    if isinstance(raw, (bytes, bytearray)):
        try:
            decompressed = gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
            txt = decompressed.decode("utf-8")
            return json.loads(txt)
        except Exception:
            try:
                return json.loads(bytes(raw).decode("utf-8"))
            except Exception:
                try:
                    return bytes(raw).decode("utf-8", errors="ignore")
                except Exception:
                    return None

    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return raw

    return None


def _handle_single_msg(expected_symbol: str, data: Any) -> None:
    """단일 WS 메시지 처리 (Binance multiplex payload)."""
    if not isinstance(data, dict):
        return

    stream = data.get("stream")
    payload = data.get("data")

    if not isinstance(stream, str) or not isinstance(payload, dict):
        return

    sym = _normalize_symbol(expected_symbol)

    # KLINE: stream 예) btcusdt@kline_1m
    if "@kline_" in stream:
        k = payload.get("k")
        if not isinstance(k, dict):
            try:
                log(
                    f"[MD_BINANCE_WS WARN] kline payload missing 'k': "
                    f"stream={stream} payload={_safe_dump_for_log(payload)}"
                )
            except Exception:
                pass
            return

        interval = str(k.get("i") or "").strip()
        if not interval:
            try:
                interval = stream.split("@kline_")[-1]
            except Exception:
                interval = ""

        if not interval:
            try:
                log(
                    f"[MD_BINANCE_WS WARN] kline interval parse failed: "
                    f"stream={stream} payload={_safe_dump_for_log(payload)}"
                )
            except Exception:
                pass
            return

        payload_symbol = _normalize_symbol(str(payload.get("s") or sym))
        if payload_symbol and payload_symbol != sym:
            # 멀티플렉스 URL이 심볼 단일 기준이므로, 불일치는 무시
            try:
                log(
                    f"[MD_BINANCE_WS WARN] unexpected symbol in kline stream "
                    f"(expected={sym}, got={payload_symbol}, stream={stream})"
                )
            except Exception:
                pass
            return

        if getattr(SET, "ws_log_payload_enabled", False):
            try:
                log(
                    f"[MD_BINANCE_WS PAYLOAD] {sym} {interval} kline: "
                    f"{_safe_dump_for_log(k)}"
                )
            except Exception:
                pass

        _push_kline(sym, interval, k)
        return

    # DEPTH: stream 예) btcusdt@depth5@100ms
    if "@depth" in stream:
        if getattr(SET, "ws_log_payload_enabled", False):
            try:
                log(
                    f"[MD_BINANCE_WS PAYLOAD] {sym} depth: {_safe_dump_for_log(payload)}"
                )
            except Exception:
                pass

        _push_orderbook(sym, payload)

        if getattr(SET, "ws_log_enabled", True):
            try:
                log(f"[MD_BINANCE_WS] {sym} depth updated")
            except Exception:
                pass
        return

    # 기타 이벤트(구독 ack, 에러 등)는 로깅 후 무시
    if getattr(SET, "ws_log_payload_enabled", False):
        try:
            log(f"[MD_BINANCE_WS INFO] unhandled stream message: {_safe_dump_for_log(data)}")
        except Exception:
            pass


def _on_message(symbol: str, ws: websocket.WebSocketApp, message: Any) -> None:
    """WebSocketApp on_message 콜백."""
    _ = ws  # 시그니처 유지용
    data = _decode_msg(message)
    if data is None:
        return

    if getattr(SET, "ws_log_raw_enabled", False):
        try:
            log(f"[MD_BINANCE_WS RAW] {_safe_dump_for_log(data)}")
        except Exception:
            pass

    # Binance는 app-level ping/pong 처리 불필요 (run_forever ping_interval 사용)
    if isinstance(data, list):
        for item in data:
            _handle_single_msg(symbol, item)
        return

    _handle_single_msg(symbol, data)


def _on_error(ws: websocket.WebSocketApp, error: Any) -> None:
    """WS 에러 콜백."""
    _ = ws
    log(f"[MD_BINANCE_WS] error: {error}")


def _on_close(ws: websocket.WebSocketApp, code: Any, msg: Any) -> None:
    """WS 연결 종료 콜백."""
    _ = ws
    log(f"[MD_BINANCE_WS] closed: {code} {msg}")


def _on_open(symbol: str, ws: websocket.WebSocketApp) -> None:
    """WS 연결 직후 콜백 (Binance multiplex는 URL 기반 구독)."""
    _ = ws
    streams = _build_stream_names(symbol)
    log(f"[MD_BINANCE_WS] opened: symbol={_normalize_symbol(symbol)} streams={streams}")


def preload_klines(
    symbol: str,
    interval: str,
    rows: List[Tuple[int, float, float, float, float, float]],
) -> None:
    """(ts, o, h, l, c, v) 튜플 리스트를 내부 버퍼에 세팅한다.

    - 부트스트랩 단계(초기 히스토리 로딩)에서만 사용하는 것을 권장
    - 라이브 운영 중에는 WS 데이터만 사용하는 것이 원칙
    - 데이터 생성/보정/추정 없음 (입력 rows 그대로 적재)
    """
    sym = _normalize_symbol(symbol)
    key = (sym, interval)

    with _kline_lock:
        trimmed = list(rows[-MAX_KLINES:])
        _kline_buffers[key] = trimmed
        buf_len = len(trimmed)
        if trimmed:
            _kline_last_recv_ts[key] = _now_ms()

    if getattr(SET, "ws_log_enabled", True):
        try:
            log(
                f"[MD_BINANCE_WS BACKFILL] {sym} {interval} preloaded "
                f"from bootstrap rows (len={buf_len})"
            )
        except Exception:
            pass


def backfill_klines_from_rest(
    symbol: str,
    interval: str,
    rest_klines: List[Any],
) -> None:
    """REST kline 응답을 받아 버퍼에 백필한다 (부트스트랩 전용).

    지원 포맷:
    - list[list]  : [openTime, open, high, low, close, volume, ...]
    - list[dict]  : {openTime/t, open/o, high/h, low/l, close/c, volume/v, ...}

    STRICT:
    - 유효 행만 선별/형변환 후 preload_klines 호출
    - 데이터 보정/추정/생성 없음
    """
    sym = _normalize_symbol(symbol)
    converted: List[Tuple[int, float, float, float, float, float]] = []
    raw_len = len(rest_klines)

    for row in rest_klines:
        try:
            if isinstance(row, (list, tuple)):
                if len(row) < 6:
                    continue
                ts = int(row[0])
                o = float(row[1])
                h = float(row[2])
                l = float(row[3])
                c = float(row[4])
                v = float(row[5])

            elif isinstance(row, dict):
                ts = int(
                    row.get("t")
                    or row.get("openTime")
                    or row.get("T")
                    or 0
                )
                o = float(row.get("o") or row.get("open"))
                h = float(row.get("h") or row.get("high"))
                l = float(row.get("l") or row.get("low"))
                c = float(row.get("c") or row.get("close"))
                v = float(row.get("v") or row.get("volume"))

            else:
                continue

            if ts <= 0:
                continue

        except Exception:
            continue

        converted.append((ts, o, h, l, c, v))

    if not converted:
        if getattr(SET, "ws_log_enabled", True):
            try:
                log(
                    f"[MD_BINANCE_WS BACKFILL] rest_klines for {sym} {interval} "
                    f"had no valid rows (raw_len={raw_len})"
                )
            except Exception:
                pass
        return

    try:
        converted.sort(key=lambda x: x[0])
    except Exception:
        pass

    if getattr(SET, "ws_log_enabled", True):
        try:
            first_ts = converted[0][0]
            last_ts = converted[-1][0]
            log(
                f"[MD_BINANCE_WS BACKFILL] parsed REST klines for {sym} {interval} "
                f"raw={raw_len} converted={len(converted)} "
                f"first_ts={first_ts} last_ts={last_ts}"
            )
        except Exception:
            pass

    preload_klines(sym, interval, converted)


def get_klines(symbol: str, interval: str, limit: int = 120):
    """(ts, o, h, l, c) 튜플 리스트 반환 (기존 포맷 호환)."""
    key = (_normalize_symbol(symbol), interval)
    with _kline_lock:
        buf = _kline_buffers.get(key, [])
        if not buf:
            return []
        sliced = list(buf[-limit:])
    return [(ts, o, h, l, c) for (ts, o, h, l, c, v) in sliced]


def get_klines_with_volume(symbol: str, interval: str, limit: int = 120):
    """(ts, o, h, l, c, v) 튜플 리스트 반환."""
    key = (_normalize_symbol(symbol), interval)
    with _kline_lock:
        buf = _kline_buffers.get(key, [])
        if not buf:
            try:
                log(
                    f"[MD_BINANCE_WS KLINES] no kline buffer (with volume) for "
                    f"{_normalize_symbol(symbol)} {interval} (requested={limit})"
                )
            except Exception:
                pass
            return []
        return list(buf[-limit:])


def get_last_kline_ts(symbol: str, interval: str) -> Optional[int]:
    """지정 심볼/주기의 마지막 캔들 openTime(ms)를 반환한다."""
    key = (_normalize_symbol(symbol), interval)
    with _kline_lock:
        buf = _kline_buffers.get(key, [])
        if not buf:
            return None
        return int(buf[-1][0])


def get_last_kline_delay_ms(symbol: str, interval: str) -> Optional[int]:
    """마지막 캔들이 로컬에 도착한 뒤 경과 시간(ms)을 반환한다."""
    key = (_normalize_symbol(symbol), interval)
    now = _now_ms()
    with _kline_lock:
        recv_ts = _kline_last_recv_ts.get(key)
    if recv_ts is None:
        return None
    return max(0, now - recv_ts)


def get_kline_buffer_status(symbol: str, interval: str) -> Dict[str, Any]:
    """디버그용: 버퍼 길이/마지막 ts/수신 지연(ms)."""
    sym = _normalize_symbol(symbol)
    key = (sym, interval)
    with _kline_lock:
        buf = _kline_buffers.get(key, [])
        last_ts = buf[-1][0] if buf else None
        last_recv = _kline_last_recv_ts.get(key)

    delay_ms = None
    if last_recv is not None:
        delay_ms = max(0, _now_ms() - last_recv)

    return {
        "symbol": sym,
        "interval": interval,
        "buffer_len": len(buf),
        "last_ts": last_ts,
        "last_recv_ts": last_recv,
        "delay_ms": delay_ms,
    }


def get_orderbook(symbol: str, limit: int = 5) -> Optional[Dict[str, Any]]:
    """현재 오더북 스냅샷 반환 (bids/asks 는 limit 까지 슬라이스)."""
    sym = _normalize_symbol(symbol)
    with _orderbook_lock:
        ob = _orderbook_buffers.get(sym)
        if not ob:
            return None
        result = dict(ob)

    if "bids" in result:
        result["bids"] = result.get("bids", [])[:limit]
        result["asks"] = result.get("asks", [])[:limit]
    return result


def _compute_kline_health(symbol: str, interval: str) -> Dict[str, Any]:
    """단일 타임프레임 버퍼 길이/지연/헬스 상태 계산."""
    status = get_kline_buffer_status(symbol, interval)
    buffer_len = status["buffer_len"]
    delay_ms = status["delay_ms"]

    ok = True
    reasons: List[str] = []

    if buffer_len < KLINE_MIN_BUFFER:
        ok = False
        reasons.append(f"buffer_len<{KLINE_MIN_BUFFER} (got={buffer_len})")

    if delay_ms is None:
        ok = False
        reasons.append("no_recv_ts")
    else:
        delay_sec = delay_ms / 1000.0
        if delay_sec > KLINE_MAX_DELAY_SEC:
            ok = False
            reasons.append(f"delay_sec>{KLINE_MAX_DELAY_SEC} (got={delay_sec:.1f})")

    status["ok"] = ok
    status["reasons"] = reasons
    return status


def _compute_orderbook_health(symbol: str) -> Dict[str, Any]:
    """오더북 버퍼 최신 여부/헬스 상태 계산."""
    sym = _normalize_symbol(symbol)
    now_ms = _now_ms()

    with _orderbook_lock:
        ob = _orderbook_buffers.get(sym)

    result: Dict[str, Any] = {
        "symbol": sym,
        "has_orderbook": ob is not None,
        "delay_ms": None,
        "ok": False,
        "reasons": [],
    }

    if ob is None:
        result["reasons"].append("no_orderbook")
        return result

    ts = ob.get("ts")
    if ts is None:
        result["reasons"].append("no_ts")
        return result

    delay_ms = max(0, now_ms - int(ts))
    delay_sec = delay_ms / 1000.0
    result["delay_ms"] = delay_ms

    if delay_sec > ORDERBOOK_MAX_DELAY_SEC:
        result["reasons"].append(f"delay_sec>{ORDERBOOK_MAX_DELAY_SEC} (got={delay_sec:.1f})")
        result["ok"] = False
    else:
        result["ok"] = True

    return result


def get_health_snapshot(symbol: str) -> Dict[str, Any]:
    """심볼 기준 전체 데이터 헬스 스냅샷 반환.

    반환 예시:
    {
        "symbol": "BTCUSDT",
        "overall_ok": True/False,
        "overall_kline_ok": True/False,
        "overall_orderbook_ok": True/False,
        "klines": {
            "1m": {...},
            "5m": {...},
            "15m": {...}
        },
        "orderbook": {...},
        "checked_at_ms": 1234567890,
    }

    STRICT:
    - 상태만 계산해 전달
    - 데이터 보정/조작 없음
    """
    sym = _normalize_symbol(symbol)
    snapshot: Dict[str, Any] = {
        "symbol": sym,
        "overall_ok": True,
        "overall_kline_ok": True,
        "overall_orderbook_ok": True,
        "klines": {},
        "orderbook": {},
        "checked_at_ms": _now_ms(),
    }

    overall_ok = True
    overall_kline_ok = True
    overall_orderbook_ok = True

    kline_map: Dict[str, Any] = {}
    for iv in REQUIRED_INTERVALS:
        k_status = _compute_kline_health(sym, iv)
        kline_map[iv] = k_status
        if not k_status.get("ok", False):
            overall_ok = False
            overall_kline_ok = False

    ob_status = _compute_orderbook_health(sym)
    if not ob_status.get("ok", False):
        overall_ok = False
        overall_orderbook_ok = False

    snapshot["klines"] = kline_map
    snapshot["orderbook"] = ob_status
    snapshot["overall_ok"] = overall_ok
    snapshot["overall_kline_ok"] = overall_kline_ok
    snapshot["overall_orderbook_ok"] = overall_orderbook_ok
    return snapshot


def is_data_healthy(symbol: str) -> bool:
    """심볼 기준 WS 데이터 헬스 OK 여부 반환."""
    snap = get_health_snapshot(symbol)
    return bool(snap.get("overall_ok", False))


# -------------------------------------
# WS 루프 및 자동 재연결 로직
# -------------------------------------

def start_ws_loop(symbol: str) -> None:
    """지정 심볼에 대한 Binance Futures WS 루프를 백그라운드로 시작한다.

    핵심 정책
    - ping_interval/ping_timeout 으로 dead connection 빠르게 감지
    - watchdog 으로 캔들/오더북 지연 감시
    - health FAIL 시 ws.close() 후 자동 재연결
    - 점진적 backoff + 장시간 안정 시 리셋
    """
    sym = _normalize_symbol(symbol)
    url = _build_ws_url(sym)

    def _runner() -> None:
        retry_wait = 1.0

        while True:
            session_dur = 0.0
            try:
                log(f"[MD_BINANCE_WS] connecting to {url} ...")

                ws = websocket.WebSocketApp(
                    url,
                    on_open=lambda ws_: _on_open(sym, ws_),
                    on_message=lambda ws_, message: _on_message(sym, ws_, message),
                    on_error=lambda ws_, error: _on_error(ws_, error),
                    on_close=lambda ws_, code, msg: _on_close(ws_, code, msg),
                )

                start_ts = time.time()

                def _watchdog() -> None:
                    """연결 상태/데이터 헬스 모니터링 후 필요 시 강제 재연결."""
                    WARMUP_SEC = 15.0
                    while True:
                        time.sleep(5.0)
                        try:
                            if ws.sock is None or not ws.sock.connected:
                                break

                            if (time.time() - start_ts) < WARMUP_SEC:
                                continue

                            snap = get_health_snapshot(sym)
                            need_close = False

                            # 캔들 지연 감시 (buffer_len 부족만으로는 재연결 대상 아님)
                            for iv, st in (snap.get("klines") or {}).items():
                                reasons = st.get("reasons", []) or []
                                for r in reasons:
                                    if isinstance(r, str) and r.startswith("delay_sec>"):
                                        log(
                                            f"[MD_BINANCE_WS WATCHDOG] {sym} {iv} unhealthy "
                                            f"(reason={r}) → ws.close()"
                                        )
                                        need_close = True
                                        break
                                if need_close:
                                    break

                            # 오더북 지연/부재 감시
                            if not need_close:
                                ob_status = snap.get("orderbook") or {}
                                for r in ob_status.get("reasons", []) or []:
                                    if (
                                        isinstance(r, str)
                                        and (r.startswith("delay_sec>") or r in ("no_orderbook", "no_ts"))
                                    ):
                                        log(
                                            f"[MD_BINANCE_WS WATCHDOG] {sym} orderbook unhealthy "
                                            f"(reason={r}) → ws.close()"
                                        )
                                        need_close = True
                                        break

                            if need_close:
                                try:
                                    ws.close()
                                except Exception:
                                    pass
                                break

                        except Exception as e:
                            try:
                                log(f"[MD_BINANCE_WS WATCHDOG] exception: {e}")
                            except Exception:
                                pass
                            continue

                threading.Thread(
                    target=_watchdog,
                    name=f"md-binance-ws-watchdog-{sym}",
                    daemon=True,
                ).start()

                # Binance는 app-level ping/pong 구현 불필요
                ws.run_forever(ping_interval=3, ping_timeout=2)

                session_dur = time.time() - start_ts
                log("[MD_BINANCE_WS] WS disconnected → retrying ...")

            except Exception as e:
                log(f"[MD_BINANCE_WS] run_forever exception: {e}")

            if session_dur > 60.0:
                retry_wait = 1.0
            else:
                retry_wait = min(retry_wait * 2.0, 10.0)

            try:
                log(f"[MD_BINANCE_WS] reconnecting after {retry_wait:.1f}s ...")
            except Exception:
                pass
            time.sleep(retry_wait)

    th = threading.Thread(target=_runner, name=f"md-binance-ws-{sym}", daemon=True)
    th.start()
    log(f"[MD_BINANCE_WS] background ws started for {sym}")


__all__ = [
    "start_ws_loop",
    "preload_klines",
    "backfill_klines_from_rest",
    "get_klines",
    "get_klines_with_volume",
    "get_last_kline_ts",
    "get_last_kline_delay_ms",
    "get_kline_buffer_status",
    "get_orderbook",
    "get_health_snapshot",
    "is_data_healthy",
]