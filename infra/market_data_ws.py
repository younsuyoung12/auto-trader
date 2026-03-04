# infra/market_data_ws.py
"""
========================================================
FILE: infra/market_data_ws.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================
역할:
- Binance USDT-M Futures WebSocket(multiplex /stream)으로부터
  멀티 타임프레임 kline + depth5 오더북을 수신해 메모리 버퍼에 저장한다.
- 상위 레이어(run_bot_ws, feature builder, health monitor)에
  getter / health snapshot만 제공한다.
- 주문/전략/EXIT 로직은 절대 포함하지 않는다.

핵심 원칙 (STRICT · NO-FALLBACK):
- WS 원본 데이터는 보정/추정/REST 폴백 없이 그대로 버퍼링한다.
- 데이터 누락/손상은 "정상 상태"로 취급하지 않는다. (health에서 FAIL)
- 더미 값 생성/None→0 치환/행 단위 조용한 skip 금지.
- 환경변수 직접 접근 금지: 이 모듈은 settings(SSOT)만 사용한다.

변경 이력
--------------------------------------------------------
- 2026-03-03 (TRADE-GRADE):
  1) ENV 직접 접근 제거:
     - os.getenv("BINANCE_FUTURES_WS_BASE") 제거
     - settings.ws_combined_base 를 단일 사용(SSOT)
  2) 프로토콜 무결성 강화:
     - kline/orderbook payload 불량(필수 필드 누락, 숫자 파싱 불가 등) 시 WSProtocolError로 연결 종료
     - “로그만 남기고 무시” 방식 제거(조용한 데이터 손상 금지)
  3) REST backfill 입력도 STRICT:
     - backfill_klines_from_rest 에서 invalid row continue 제거 → 즉시 예외
- 2026-03-04:
  1) Atomic Market Snapshot 추가
     - get_market_snapshot(symbol) 구현
     - kline + orderbook 동시 snapshot 제공
     - strategy layer에서 데이터 시점 불일치 방지
========================================================
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import websocket  # pip install websocket-client

from infra.telelog import log
from settings import load_settings

SET = load_settings()

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


class WSProtocolError(RuntimeError):
    """Binance multiplex payload protocol violation (STRICT)."""


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_dump_for_log(obj: Any, max_len: int = 2000) -> str:
    try:
        s = json.dumps(obj, ensure_ascii=False, default=str)
    except Exception:
        s = str(obj)
    if len(s) > max_len:
        return s[:max_len] + f"... (truncated, total_len={len(s)})"
    return s


def _normalize_symbol(sym: str) -> str:
    s = (sym or "").upper().strip()
    if not s:
        return ""
    return s.replace("-", "").replace("/", "").replace("_", "")


def _normalize_interval(iv: Any) -> str:
    s = str(iv or "").strip()
    if not s:
        return ""
    # 월(M)은 Binance 표기상 1M (대문자 M) 유지
    if s.endswith("M"):
        return s
    return s.lower()


def _to_stream_symbol(sym: str) -> str:
    return _normalize_symbol(sym).lower()


def _require_int_ms(v: Any, name: str) -> int:
    if isinstance(v, bool):
        raise WSProtocolError(f"{name} must be int ms (bool not allowed)")
    try:
        iv = int(v)
    except Exception as e:
        raise WSProtocolError(f"{name} must be int ms: {e}") from e
    if iv <= 0:
        raise WSProtocolError(f"{name} must be > 0")
    return iv


def _require_float(v: Any, name: str, *, allow_zero: bool = False) -> float:
    if v is None:
        raise WSProtocolError(f"{name} missing")
    if isinstance(v, bool):
        raise WSProtocolError(f"{name} must be float (bool not allowed)")
    try:
        fv = float(v)
    except Exception as e:
        raise WSProtocolError(f"{name} must be float: {e}") from e
    if not (fv == fv) or fv in (float("inf"), float("-inf")):
        raise WSProtocolError(f"{name} must be finite")
    if allow_zero:
        if fv < 0:
            raise WSProtocolError(f"{name} must be >= 0")
    else:
        if fv <= 0:
            raise WSProtocolError(f"{name} must be > 0")
    return float(fv)


# ─────────────────────────────────────────────
# Streams / URL (SSOT)
# ─────────────────────────────────────────────
WS_INTERVALS: List[str] = [_normalize_interval(x) for x in (list(getattr(SET, "ws_subscribe_tfs", None) or DEFAULT_INTERVALS))]
WS_INTERVALS = [x for x in WS_INTERVALS if x]

REQUIRED_INTERVALS: List[str] = [_normalize_interval(x) for x in (list(getattr(SET, "ws_required_tfs", None) or ["1m", "5m", "15m"]))]
REQUIRED_INTERVALS = [x for x in REQUIRED_INTERVALS if x]

if not WS_INTERVALS:
    raise RuntimeError("ws_subscribe_tfs is empty. At least one interval must be subscribed.")
if not REQUIRED_INTERVALS:
    raise RuntimeError("ws_required_tfs is empty. At least one required interval must be set.")

_missing_required = [iv for iv in REQUIRED_INTERVALS if iv not in WS_INTERVALS]
if _missing_required:
    raise RuntimeError(
        "ws_required_tfs contains intervals not present in ws_subscribe_tfs. "
        f"missing={_missing_required}. Fix your settings to subscribe required intervals."
    )

# TRADE-GRADE: multiplex base는 settings에서만 받는다.
_WS_COMBINED_BASE = str(getattr(SET, "ws_combined_base", "") or "").strip()
if not _WS_COMBINED_BASE:
    raise RuntimeError("settings.ws_combined_base is required (STRICT)")

# 기대 형태: ".../stream?streams="
if "?streams=" not in _WS_COMBINED_BASE:
    # 허용: ".../stream" 이면 suffix 부여
    if _WS_COMBINED_BASE.endswith("/stream"):
        _WS_COMBINED_BASE = _WS_COMBINED_BASE + "?streams="
    else:
        raise RuntimeError(f"settings.ws_combined_base must contain '?streams=' (STRICT): {_WS_COMBINED_BASE!r}")

if not _WS_COMBINED_BASE.endswith("streams="):
    # "?streams=" 다음에 아무 문자열이 붙어있으면 위험. (URL 구성 깨짐)
    raise RuntimeError(f"settings.ws_combined_base must end with 'streams=' (STRICT): {_WS_COMBINED_BASE!r}")


KLINE_MIN_BUFFER: int = int(getattr(SET, "ws_min_kline_buffer", 60))
KLINE_MAX_DELAY_SEC: float = float(getattr(SET, "ws_max_kline_delay_sec", 120.0))
ORDERBOOK_MAX_DELAY_SEC: float = float(getattr(SET, "ws_orderbook_max_delay_sec", 10.0))

_LONG_TF_MIN_BUFFER_DEFAULT: int = int(getattr(SET, "ws_min_kline_buffer_long_tf", 2))

_HEALTH_FAIL_LOG_SUPPRESS_SEC: float = float(getattr(SET, "ws_health_fail_log_suppress_sec", 10.0))
_last_health_fail_log_ts: float = 0.0
_last_health_fail_key: str = ""
_health_fail_lock = threading.Lock()

# { (symbol, interval): [(ts, o, h, l, c, v), ...] }  (닫힌 캔들만)
_kline_buffers: Dict[Tuple[str, str], List[Tuple[int, float, float, float, float, float]]] = {}
# { (symbol, interval): last_recv_ts_ms }  (x 여부 무관)
_kline_last_recv_ts: Dict[Tuple[str, str], int] = {}

# { symbol: {"bids": [...], "asks": [...], "ts": ..., "exchTs": ..., "bestBid": ..., "bestAsk": ..., "spreadPct": ...} }
_orderbook_buffers: Dict[str, Dict[str, Any]] = {}

_kline_lock = threading.Lock()
_orderbook_lock = threading.Lock()

MAX_KLINES = 500

_NO_BUF_LOG_SUPPRESS_SEC: float = float(getattr(SET, "ws_no_buffer_log_suppress_sec", 30.0))
_no_buf_log_last: Dict[Tuple[str, str], float] = {}
_no_buf_log_lock = threading.Lock()


def _min_buffer_for_interval(iv: str) -> int:
    s = _normalize_interval(iv)
    if not s:
        return max(1, KLINE_MIN_BUFFER)
    if s.endswith("d") or s.endswith("w") or s.endswith("M"):
        return max(1, _LONG_TF_MIN_BUFFER_DEFAULT)
    return max(1, KLINE_MIN_BUFFER)


def _build_stream_names(symbol: str) -> List[str]:
    s = _to_stream_symbol(symbol)
    if not s:
        raise RuntimeError("symbol is required to build WS streams")
    streams: List[str] = [f"{s}@kline_{iv}" for iv in WS_INTERVALS]
    streams.append(f"{s}@depth5@100ms")
    return streams


def _build_ws_url(symbol: str) -> str:
    streams = _build_stream_names(symbol)
    return f"{_WS_COMBINED_BASE}{'/'.join(streams)}"


def _decode_msg(raw: Any) -> Any:
    if isinstance(raw, (bytes, bytearray)):
        txt = bytes(raw).decode("utf-8")
        return json.loads(txt)
    if isinstance(raw, str):
        return json.loads(raw)
    raise RuntimeError(f"unsupported ws message type: {type(raw)}")


def _push_kline(symbol: str, interval: str, kline_obj: Dict[str, Any]) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise WSProtocolError("empty symbol in kline push")

    iv = _normalize_interval(interval)
    if not iv:
        raise WSProtocolError("empty interval in kline push")

    key = (sym, iv)

    # STRICT: 필수 필드/파싱 실패는 프로토콜 에러로 처리
    ts = _require_int_ms(kline_obj.get("t"), "kline.t")
    o = _require_float(kline_obj.get("o"), "kline.o")
    h = _require_float(kline_obj.get("h"), "kline.h")
    l = _require_float(kline_obj.get("l"), "kline.l")
    c = _require_float(kline_obj.get("c"), "kline.c")
    v = _require_float(kline_obj.get("v"), "kline.v", allow_zero=True)

    is_closed = bool(kline_obj.get("x", False))

    now_ms = _now_ms()
    with _kline_lock:
        _kline_last_recv_ts[key] = now_ms

        buf = _kline_buffers.setdefault(key, [])
        if is_closed:
            if buf and buf[-1][0] == ts:
                buf[-1] = (ts, o, h, l, c, v)
            else:
                buf.append((ts, o, h, l, c, v))
                if len(buf) > MAX_KLINES:
                    del buf[0 : len(buf) - MAX_KLINES]


def _normalize_depth_side_strict(side_val: Any, *, name: str) -> List[List[float]]:
    """
    STRICT:
    - depth levels는 [price, qty] 또는 {"price":..., "qty":...} 형태만 허용
    - price > 0, qty > 0, finite
    - 파싱 불가/불량 값이 하나라도 있으면 프로토콜 에러
    """
    if side_val is None:
        raise WSProtocolError(f"{name} missing")
    if not isinstance(side_val, (list, tuple)):
        raise WSProtocolError(f"{name} must be list/tuple")

    out: List[List[float]] = []
    for i, row in enumerate(side_val):
        if isinstance(row, (list, tuple)) and len(row) >= 2:
            p = _require_float(row[0], f"{name}[{i}].price")
            q = _require_float(row[1], f"{name}[{i}].qty")
            out.append([p, q])
            continue
        if isinstance(row, dict):
            p = _require_float(row.get("price"), f"{name}[{i}].price")
            q = _require_float(row.get("qty"), f"{name}[{i}].qty")
            out.append([p, q])
            continue
        raise WSProtocolError(f"{name}[{i}] invalid level type: {type(row).__name__}")

    if not out:
        raise WSProtocolError(f"{name} empty after parse (STRICT)")
    return out


def _compute_best_prices_strict(bids: List[List[float]], asks: List[List[float]]) -> Tuple[float, float]:
    if not bids or not asks:
        raise WSProtocolError("bids/asks empty (STRICT)")
    best_bid = max(float(r[0]) for r in bids)
    best_ask = min(float(r[0]) for r in asks)
    if best_bid <= 0 or best_ask <= 0:
        raise WSProtocolError("best prices invalid (STRICT)")
    if best_ask <= best_bid:
        raise WSProtocolError("crossed book (bestAsk<=bestBid) (STRICT)")
    return float(best_bid), float(best_ask)


def _compute_spread_pct(best_bid: float, best_ask: float) -> float:
    if best_bid <= 0 or best_ask <= 0:
        raise WSProtocolError("best prices invalid for spread (STRICT)")
    if best_ask <= best_bid:
        raise WSProtocolError("crossed book for spread (STRICT)")
    mid = (best_bid + best_ask) / 2.0
    if mid <= 0:
        raise WSProtocolError("mid invalid (STRICT)")
    return (best_ask - best_bid) / mid * 100.0


def _push_orderbook(symbol: str, payload: Dict[str, Any]) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise WSProtocolError("empty symbol in orderbook push")

    raw_bids = payload.get("b") if payload.get("b") is not None else payload.get("bids")
    raw_asks = payload.get("a") if payload.get("a") is not None else payload.get("asks")

    normalized_bids = _normalize_depth_side_strict(raw_bids, name="orderbook.bids")
    normalized_asks = _normalize_depth_side_strict(raw_asks, name="orderbook.asks")

    now_ms = _now_ms()

    best_bid, best_ask = _compute_best_prices_strict(normalized_bids, normalized_asks)
    spread_pct = _compute_spread_pct(best_bid, best_ask)

    ob: Dict[str, Any] = {
        "bids": normalized_bids,
        "asks": normalized_asks,
        "ts": now_ms,  # 로컬 수신 시각(필수)
        "bestBid": best_bid,
        "bestAsk": best_ask,
        "spreadPct": spread_pct,
    }

    last_update_id = payload.get("u") if payload.get("u") is not None else payload.get("lastUpdateId")
    if last_update_id is not None:
        ob["lastUpdateId"] = int(last_update_id)  # 실패하면 예외(STRICT)

    # Binance depth event time
    exch_ts = payload.get("E") or payload.get("T")
    if exch_ts is not None:
        ob["exchTs"] = _require_int_ms(exch_ts, "orderbook.exchTs")

    with _orderbook_lock:
        _orderbook_buffers[sym] = ob


def _handle_single_msg(expected_symbol: str, data: Any) -> None:
    if not isinstance(data, dict):
        raise WSProtocolError("message item must be dict (STRICT)")

    stream = data.get("stream")
    payload = data.get("data")
    if not isinstance(stream, str) or not isinstance(payload, dict):
        raise WSProtocolError("multiplex message missing stream/data (STRICT)")

    sym = _normalize_symbol(expected_symbol)
    if not sym:
        raise WSProtocolError("expected_symbol normalized to empty")

    if "@kline_" in stream:
        k = payload.get("k")
        if not isinstance(k, dict):
            raise WSProtocolError(f"kline stream missing 'k': stream={stream} payload={_safe_dump_for_log(payload)}")

        interval = str(k.get("i") or "").strip()
        if not interval:
            interval = stream.split("@kline_")[-1]
        interval = _normalize_interval(interval)
        if not interval:
            raise WSProtocolError(f"kline interval parse failed: stream={stream} payload={_safe_dump_for_log(payload)}")

        payload_symbol = _normalize_symbol(str(payload.get("s") or sym))
        if payload_symbol and payload_symbol != sym:
            raise WSProtocolError(f"unexpected symbol in stream (expected={sym}, got={payload_symbol})")

        _push_kline(sym, interval, k)
        return

    if "@depth" in stream:
        _push_orderbook(sym, payload)
        return

    # 알 수 없는 stream은 프로토콜 위반으로 처리
    raise WSProtocolError(f"unknown stream type (STRICT): {stream}")


def _on_message(symbol: str, ws: websocket.WebSocketApp, message: Any) -> None:
    try:
        data = _decode_msg(message)
    except Exception as e:
        log(f"[MD_BINANCE_WS] decode error: {type(e).__name__}: {e}")
        try:
            ws.close()
        except Exception:
            pass
        return

    try:
        if isinstance(data, list):
            for item in data:
                _handle_single_msg(symbol, item)
        else:
            _handle_single_msg(symbol, data)
    except WSProtocolError as e:
        log(f"[MD_BINANCE_WS] protocol error: {e}")
        try:
            ws.close()
        except Exception:
            pass


def _on_error(ws: websocket.WebSocketApp, error: Any) -> None:
    _ = ws
    log(f"[MD_BINANCE_WS] error: {error}")


def _on_close(ws: websocket.WebSocketApp, code: Any, msg: Any) -> None:
    _ = ws
    log(f"[MD_BINANCE_WS] closed: {code} {msg}")


def _on_open(symbol: str, ws: websocket.WebSocketApp) -> None:
    _ = ws
    streams = _build_stream_names(symbol)
    log(f"[MD_BINANCE_WS] opened: symbol={_normalize_symbol(symbol)} streams={streams}")


def preload_klines(symbol: str, interval: str, rows: List[Tuple[int, float, float, float, float, float]]) -> None:
    sym = _normalize_symbol(symbol)
    iv = _normalize_interval(interval)
    if not sym:
        raise ValueError("symbol is required")
    if not iv:
        raise ValueError("interval is required")

    # STRICT: preload rows 검증
    cleaned: List[Tuple[int, float, float, float, float, float]] = []
    for i, r in enumerate(rows):
        if not isinstance(r, (list, tuple)) or len(r) != 6:
            raise RuntimeError(f"preload row[{i}] must be 6-tuple (STRICT)")
        ts = _require_int_ms(r[0], f"preload[{i}].ts")
        o = _require_float(r[1], f"preload[{i}].o")
        h = _require_float(r[2], f"preload[{i}].h")
        l = _require_float(r[3], f"preload[{i}].l")
        c = _require_float(r[4], f"preload[{i}].c")
        v = _require_float(r[5], f"preload[{i}].v", allow_zero=True)
        cleaned.append((ts, o, h, l, c, v))

    key = (sym, iv)
    with _kline_lock:
        trimmed = list(cleaned[-MAX_KLINES:])
        _kline_buffers[key] = trimmed
        if trimmed:
            _kline_last_recv_ts[key] = _now_ms()


def backfill_klines_from_rest(symbol: str, interval: str, rest_klines: List[Any]) -> None:
    """
    STRICT:
    - REST 입력이 불량이면 조용히 건너뛰지 않는다(부팅 무결성).
    """
    sym = _normalize_symbol(symbol)
    iv = _normalize_interval(interval)
    if not sym:
        raise ValueError("symbol is required")
    if not iv:
        raise ValueError("interval is required")

    if not isinstance(rest_klines, list):
        raise RuntimeError("rest_klines must be list (STRICT)")
    if not rest_klines:
        raise RuntimeError("rest_klines empty (STRICT)")

    converted: List[Tuple[int, float, float, float, float, float]] = []
    for idx, row in enumerate(rest_klines):
        if isinstance(row, (list, tuple)):
            if len(row) < 6:
                raise RuntimeError(f"rest_klines[{idx}] invalid len<{6} (STRICT)")
            ts = _require_int_ms(row[0], f"rest_klines[{idx}].ts")
            o = _require_float(row[1], f"rest_klines[{idx}].o")
            h = _require_float(row[2], f"rest_klines[{idx}].h")
            l = _require_float(row[3], f"rest_klines[{idx}].l")
            c = _require_float(row[4], f"rest_klines[{idx}].c")
            v = _require_float(row[5], f"rest_klines[{idx}].v", allow_zero=True)
            converted.append((ts, o, h, l, c, v))
            continue

        if isinstance(row, dict):
            ts = _require_int_ms(row.get("t") or row.get("openTime") or row.get("T"), f"rest_klines[{idx}].ts")
            o = _require_float(row.get("o") or row.get("open"), f"rest_klines[{idx}].o")
            h = _require_float(row.get("h") or row.get("high"), f"rest_klines[{idx}].h")
            l = _require_float(row.get("l") or row.get("low"), f"rest_klines[{idx}].l")
            c = _require_float(row.get("c") or row.get("close"), f"rest_klines[{idx}].c")
            v = _require_float(row.get("v") or row.get("volume"), f"rest_klines[{idx}].v", allow_zero=True)
            converted.append((ts, o, h, l, c, v))
            continue

        raise RuntimeError(f"rest_klines[{idx}] invalid type (STRICT): {type(row).__name__}")

    converted.sort(key=lambda x: x[0])
    preload_klines(sym, iv, converted)


def _log_no_buffer_once(symbol: str, interval: str, requested: int) -> None:
    if not getattr(SET, "ws_log_enabled", False):
        return

    now = time.time()
    key = (_normalize_symbol(symbol), _normalize_interval(interval))
    with _no_buf_log_lock:
        last = _no_buf_log_last.get(key, 0.0)
        if (now - last) < _NO_BUF_LOG_SUPPRESS_SEC:
            return
        _no_buf_log_last[key] = now

    log(f"[MD_BINANCE_WS KLINES] no kline buffer for {symbol} {interval} (requested={requested})")


def get_klines_with_volume(symbol: str, interval: str, limit: int = 120) -> List[Tuple[int, float, float, float, float, float]]:
    if limit <= 0:
        raise ValueError("limit must be > 0")

    sym = _normalize_symbol(symbol)
    iv = _normalize_interval(interval)
    key = (sym, iv)

    with _kline_lock:
        buf = _kline_buffers.get(key, [])
        if not buf:
            _log_no_buffer_once(sym, iv, limit)
            return []
        return list(buf[-limit:])


# 레거시 호환: (ts,o,h,l,c)
def get_klines(symbol: str, interval: str, limit: int = 120) -> List[Tuple[int, float, float, float, float]]:
    rows = get_klines_with_volume(symbol, interval, limit=limit)
    return [(ts, o, h, l, c) for (ts, o, h, l, c, v) in rows]


def get_last_kline_ts(symbol: str, interval: str) -> Optional[int]:
    sym = _normalize_symbol(symbol)
    iv = _normalize_interval(interval)
    key = (sym, iv)
    with _kline_lock:
        buf = _kline_buffers.get(key, [])
        if not buf:
            return None
        return int(buf[-1][0])


def get_last_kline_delay_ms(symbol: str, interval: str) -> Optional[int]:
    sym = _normalize_symbol(symbol)
    iv = _normalize_interval(interval)
    key = (sym, iv)
    now = _now_ms()
    with _kline_lock:
        recv_ts = _kline_last_recv_ts.get(key)
    if recv_ts is None:
        return None
    return max(0, now - int(recv_ts))


def get_kline_buffer_status(symbol: str, interval: str) -> Dict[str, Any]:
    sym = _normalize_symbol(symbol)
    iv = _normalize_interval(interval)
    key = (sym, iv)

    with _kline_lock:
        buf = _kline_buffers.get(key, [])
        last_ts = buf[-1][0] if buf else None
        last_recv = _kline_last_recv_ts.get(key)

    delay_ms = None
    if last_recv is not None:
        delay_ms = max(0, _now_ms() - int(last_recv))

    return {
        "symbol": sym,
        "interval": iv,
        "buffer_len": len(buf),
        "last_ts": last_ts,
        "last_recv_ts": last_recv,
        "delay_ms": delay_ms,
    }


def get_orderbook(symbol: str, limit: int = 5) -> Optional[Dict[str, Any]]:
    if limit <= 0:
        raise ValueError("limit must be > 0")

    sym = _normalize_symbol(symbol)
    with _orderbook_lock:
        ob = _orderbook_buffers.get(sym)
        if not ob:
            return None

        bids = ob.get("bids") or []
        asks = ob.get("asks") or []
        if not bids or not asks:
            return None
        result = dict(ob)

    result["bids"] = list((result.get("bids") or [])[:limit])
    result["asks"] = list((result.get("asks") or [])[:limit])
    return result


def _compute_kline_health(symbol: str, interval: str) -> Dict[str, Any]:
    status = get_kline_buffer_status(symbol, interval)
    buffer_len = status["buffer_len"]
    delay_ms = status["delay_ms"]

    ok = True
    reasons: List[str] = []

    min_buf = _min_buffer_for_interval(interval)
    if buffer_len < min_buf:
        ok = False
        reasons.append(f"buffer_len<{min_buf} (got={buffer_len})")

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
    status["min_buffer_required"] = min_buf
    return status


def _compute_orderbook_health(symbol: str) -> Dict[str, Any]:
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

    bids = ob.get("bids") or []
    asks = ob.get("asks") or []
    if not bids:
        result["reasons"].append("empty_bids")
    if not asks:
        result["reasons"].append("empty_asks")

    best_bid = ob.get("bestBid")
    best_ask = ob.get("bestAsk")
    if best_bid is None or best_ask is None:
        result["reasons"].append("no_best_prices")
    else:
        try:
            bb = float(best_bid)
            ba = float(best_ask)
            if ba <= bb:
                result["reasons"].append("crossed_book(bestAsk<=bestBid)")
        except Exception:
            result["reasons"].append("invalid_best_prices")

    if delay_sec > ORDERBOOK_MAX_DELAY_SEC:
        result["reasons"].append(f"delay_sec>{ORDERBOOK_MAX_DELAY_SEC} (got={delay_sec:.1f})")

    result["ok"] = len(result["reasons"]) == 0
    return result


def _maybe_log_health_fail(snapshot: Dict[str, Any]) -> None:
    global _last_health_fail_log_ts, _last_health_fail_key

    if snapshot.get("overall_ok", True):
        return

    parts: List[str] = []
    for iv, st in (snapshot.get("klines") or {}).items():
        if not st.get("ok", False):
            parts.append(f"kline:{iv}:{'|'.join(st.get('reasons') or [])}")
    ob = snapshot.get("orderbook") or {}
    if not ob.get("ok", False):
        parts.append(f"ob:{'|'.join(ob.get('reasons') or [])}")

    key = ";".join(parts)[:600]
    now = time.time()

    with _health_fail_lock:
        if key == _last_health_fail_key and (now - _last_health_fail_log_ts) < _HEALTH_FAIL_LOG_SUPPRESS_SEC:
            return
        _last_health_fail_key = key
        _last_health_fail_log_ts = now

    log(f"[MD_BINANCE_WS HEALTH_FAIL] {snapshot.get('symbol')} reasons={key}")


def get_health_snapshot(symbol: str) -> Dict[str, Any]:
    sym = _normalize_symbol(symbol)
    snapshot: Dict[str, Any] = {
        "symbol": sym,
        "overall_ok": True,
        "overall_kline_ok": True,
        "overall_orderbook_ok": True,
        "klines": {},
        "orderbook": {},
        "overall_reasons": [],
        "checked_at_ms": _now_ms(),
        "required_intervals": list(REQUIRED_INTERVALS),
    }

    overall_ok = True
    overall_kline_ok = True
    overall_orderbook_ok = True
    overall_reasons: List[str] = []

    kline_map: Dict[str, Any] = {}
    for iv in REQUIRED_INTERVALS:
        k_status = _compute_kline_health(sym, iv)
        kline_map[iv] = k_status
        if not k_status.get("ok", False):
            overall_ok = False
            overall_kline_ok = False
            overall_reasons.append(f"kline:{iv}:{'|'.join(k_status.get('reasons') or [])}")

    ob_status = _compute_orderbook_health(sym)
    if not ob_status.get("ok", False):
        overall_ok = False
        overall_orderbook_ok = False
        overall_reasons.append(f"orderbook:{'|'.join(ob_status.get('reasons') or [])}")

    snapshot["klines"] = kline_map
    snapshot["orderbook"] = ob_status
    snapshot["overall_ok"] = overall_ok
    snapshot["overall_kline_ok"] = overall_kline_ok
    snapshot["overall_orderbook_ok"] = overall_orderbook_ok
    snapshot["overall_reasons"] = overall_reasons

    _maybe_log_health_fail(snapshot)
    return snapshot


def is_data_healthy(symbol: str) -> bool:
    return bool(get_health_snapshot(symbol).get("overall_ok", False))


def start_ws_loop(symbol: str) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise RuntimeError("symbol is required")

    url = _build_ws_url(sym)

    def _runner() -> None:
        retry_wait = 1.0
        while True:
            session_dur = 0.0
            try:
                log(f"[MD_BINANCE_WS] connecting ... url={url}")

                ws = websocket.WebSocketApp(
                    url,
                    on_open=lambda ws_: _on_open(sym, ws_),
                    on_message=lambda ws_, message: _on_message(sym, ws_, message),
                    on_error=lambda ws_, error: _on_error(ws_, error),
                    on_close=lambda ws_, code, msg: _on_close(ws_, code, msg),
                )

                start_ts = time.time()
                ws.run_forever(ping_interval=3, ping_timeout=2)
                session_dur = time.time() - start_ts
                log("[MD_BINANCE_WS] WS disconnected → retrying ...")

            except Exception as e:
                log(f"[MD_BINANCE_WS] run_forever exception: {type(e).__name__}: {e}")

            retry_wait = 1.0 if session_dur > 60.0 else min(retry_wait * 2.0, 10.0)
            log(f"[MD_BINANCE_WS] reconnecting after {retry_wait:.1f}s ...")
            time.sleep(retry_wait)

    th = threading.Thread(target=_runner, name=f"md-binance-ws-{sym}", daemon=True)
    th.start()
    log(f"[MD_BINANCE_WS] background ws started for {sym}")


def get_market_snapshot(symbol: str) -> Dict[str, Any]:
    """
    Atomic Market Snapshot (STRICT)

    목적:
    - strategy/feature builder가 kline + orderbook을 "동일 시점"으로 읽도록 한다.
    - 개별 getter(get_klines*, get_orderbook) 조합 호출 시 발생 가능한
      non-atomic read(데이터 시점 불일치)를 방지한다.

    반환:
    {
        "symbol": str,
        "orderbook": dict | None,
        "klines": { "<interval>": [ (ts,o,h,l,c,v), ... ], ... }
    }
    """
    sym = _normalize_symbol(symbol)

    # NOTE:
    # - STRICT 정책상 여기서 더미 값 생성/보정은 하지 않는다.
    # - 데이터가 없으면 orderbook=None 또는 klines={} 로 반환한다.
    #   (상위 레이어는 health snapshot으로 FAIL 처리 가능)
    with _kline_lock, _orderbook_lock:
        snapshot: Dict[str, Any] = {"symbol": sym, "orderbook": None, "klines": {}}

        ob = _orderbook_buffers.get(sym)
        if ob:
            snapshot["orderbook"] = dict(ob)

        kl_map: Dict[str, Any] = {}
        for (s, iv), rows in _kline_buffers.items():
            if s == sym:
                kl_map[iv] = list(rows)
        snapshot["klines"] = kl_map

        return snapshot


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
    "get_market_snapshot",
]