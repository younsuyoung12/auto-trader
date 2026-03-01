"""
========================================================
infra/market_data_ws.py
STRICT · NO-FALLBACK · PRODUCTION MODE
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
- 더미 값 생성/None→0 치환 금지.
- 이 모듈은 수신/버퍼/헬스만 담당하며, 운영 판단은 상위 레이어가 수행한다.

PATCH NOTES — 2026-02-28
1) depth5 오더북 파서 정합 유지/강화:
   - Binance depthUpdate(depth5@100ms)는 b/a 키를 사용.
   - b/a 및 bids/asks 모두 지원.
2) 오더북 정규화 강화:
   - price<=0 또는 qty<=0 레벨은 버퍼에 적재하지 않음(가짜 호가/삭제 이벤트 방지).
3) 오더북 헬스 체크 강화:
   - bids/asks 비어있음, bestAsk<=bestBid(crossed book), best price 누락 시 ok=False.
4) 설정 검증 추가(FAIL FAST):
   - REQUIRED_INTERVALS ⊆ WS_INTERVALS 가 아니면 import 시점에 RuntimeError.
   - WS_INTERVALS/REQUIRED_INTERVALS 비어있으면 RuntimeError.
5) WS 메시지 처리 STRICT 강화:
   - JSON decode 실패/프로토콜 위반(kline 필수 필드 누락) 시 ws.close()로 재연결 유도.
6) 과도한 로그 스팸 방지:
   - get_klines_with_volume()에서 버퍼 없음 로그는 interval별 rate-limit 적용.
========================================================
"""

from __future__ import annotations

import json
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import websocket  # pip install websocket-client

from infra.telelog import log
from settings import load_settings

SET = load_settings()

# Binance Futures WebSocket multiplex endpoint
# 기본값은 공식 fstream /stream
_ws_base = os.getenv("BINANCE_FUTURES_WS_BASE")
WS_BASE_URL = (_ws_base.strip() if _ws_base and _ws_base.strip() else "wss://fstream.binance.com/stream")

# Binance Kline 기본 타임프레임 세트
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

WS_INTERVALS: List[str] = list(getattr(SET, "ws_subscribe_tfs", None) or DEFAULT_INTERVALS)
REQUIRED_INTERVALS: List[str] = list(getattr(SET, "ws_required_tfs", None) or ["1m", "5m", "15m"])

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

KLINE_MIN_BUFFER: int = int(getattr(SET, "ws_min_kline_buffer", 60))
KLINE_MAX_DELAY_SEC: float = float(getattr(SET, "ws_max_kline_delay_sec", 120.0))
ORDERBOOK_MAX_DELAY_SEC: float = float(getattr(SET, "ws_orderbook_max_delay_sec", 10.0))

# { (symbol, interval): [(ts, o, h, l, c, v), ...] }
_kline_buffers: Dict[Tuple[str, str], List[Tuple[int, float, float, float, float, float]]] = {}
# { (symbol, interval): last_recv_ts_ms }
_kline_last_recv_ts: Dict[Tuple[str, str], int] = {}

# { symbol: {"bids": [...], "asks": [...], "ts": ..., "exchTs": ..., "bestBid": ..., "bestAsk": ..., "spreadPct": ...} }
_orderbook_buffers: Dict[str, Dict[str, Any]] = {}

_kline_lock = threading.Lock()
_orderbook_lock = threading.Lock()

MAX_KLINES = 500

# "no buffer" 로그 rate-limit
_NO_BUF_LOG_SUPPRESS_SEC: float = float(getattr(SET, "ws_no_buffer_log_suppress_sec", 30.0))
_no_buf_log_last: Dict[Tuple[str, str], float] = {}
_no_buf_log_lock = threading.Lock()


class WSProtocolError(RuntimeError):
    """Binance multiplex payload protocol violation."""


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
    """
    내부 저장용 심볼 표준화.
    - 'BTC-USDT', 'BTC/USDT', 'BTC_USDT' → 'BTCUSDT'
    """
    s = (sym or "").upper().strip()
    if not s:
        return ""
    return s.replace("-", "").replace("/", "").replace("_", "")


def _to_stream_symbol(sym: str) -> str:
    return _normalize_symbol(sym).lower()


def _build_stream_names(symbol: str) -> List[str]:
    s = _to_stream_symbol(symbol)
    if not s:
        raise RuntimeError("symbol is required to build WS streams")
    streams: List[str] = [f"{s}@kline_{iv}" for iv in WS_INTERVALS]
    streams.append(f"{s}@depth5@100ms")
    return streams


def _build_ws_url(symbol: str) -> str:
    streams = _build_stream_names(symbol)
    return f"{WS_BASE_URL}?streams={'/'.join(streams)}"


def _decode_msg(raw: Any) -> Any:
    """
    STRICT:
    - Binance multiplex는 기본적으로 text JSON 프레임을 사용한다.
    - bytes 프레임이면 utf-8 디코딩 후 JSON 파싱한다.
    - 파싱 실패는 예외로 올려 on_message에서 ws.close()로 재연결 유도.
    """
    if isinstance(raw, (bytes, bytearray)):
        txt = bytes(raw).decode("utf-8")
        return json.loads(txt)

    if isinstance(raw, str):
        return json.loads(raw)

    raise RuntimeError(f"unsupported ws message type: {type(raw)}")


def _push_kline(symbol: str, interval: str, kline_obj: Dict[str, Any]) -> None:
    """
    Binance kline payload['k'] 를 내부 버퍼에 반영한다.

    STRICT:
    - ts<=0 이면 버퍼 오염 방지를 위해 폐기(로그 후 무시)
    - o/h/l/c/v 는 float 변환만 수행 (보정/추정 없음)

    IMPORTANT (STRICT · NO-FALLBACK):
    - 시장데이터(캔들/오더북) 의사결정은 WS 버퍼만 사용한다.
    - 따라서 캔들 버퍼는 **닫힌 캔들만** 포함해야 한다.
      Binance kline 이벤트의 x == True(종가 확정) 일 때만 버퍼에 저장한다.
    - 단, WS feed health(지연/끊김) 판단을 위해 last_recv_ts는 x 여부와 무관하게 갱신한다.
    """
    sym = _normalize_symbol(symbol)
    if not sym:
        raise WSProtocolError("empty symbol in kline push")

    if not interval:
        raise WSProtocolError("empty interval in kline push")

    key = (sym, interval)

    try:
        ts = int(kline_obj.get("t") or 0)
    except Exception:
        ts = 0

    if ts <= 0:
        log(
            f"[MD_BINANCE_WS WARN] invalid kline ts for {sym} {interval}: "
            f"payload={_safe_dump_for_log(kline_obj)}"
        )
        return

    try:
        o = float(kline_obj.get("o"))
        h = float(kline_obj.get("h"))
        l = float(kline_obj.get("l"))
        c = float(kline_obj.get("c"))
        v = float(kline_obj.get("v"))
    except Exception:
        log(
            f"[MD_BINANCE_WS WARN] invalid kline numeric fields for {sym} {interval}: "
            f"payload={_safe_dump_for_log(kline_obj)}"
        )
        return

    # Binance kline 종가 확정 플래그
    is_closed = bool(kline_obj.get("x", False))

    now_ms = _now_ms()
    with _kline_lock:
        # health/delay 측정: x 여부와 무관하게 수신시각 갱신
        _kline_last_recv_ts[key] = now_ms

        buf = _kline_buffers.setdefault(key, [])
        before_len = len(buf)

        # STRICT: 닫힌 캔들만 버퍼에 반영
        if is_closed:
            if buf and buf[-1][0] == ts:
                buf[-1] = (ts, o, h, l, c, v)
            else:
                buf.append((ts, o, h, l, c, v))
                if len(buf) > MAX_KLINES:
                    del buf[0 : len(buf) - MAX_KLINES]

        after_len = len(buf)

    if getattr(SET, "ws_log_enabled", True):
        age_ms = max(0, now_ms - ts)
        log(
            f"[MD_BINANCE_WS] {sym} {interval} kline updated "
            f"(ts={ts}, age_ms={age_ms}, buf_len={after_len}, added={after_len - before_len}, closed={is_closed})"
        )


def _normalize_depth_side(side_val: Any) -> List[List[float]]:
    """
    depth bids/asks 를 [[price, qty], ...] float 형태로 변환.

    STRICT:
    - price<=0 또는 qty<=0 는 폐기 (삭제/비정상 레벨 제거)
    - 변환 실패는 해당 row만 스킵
    """
    if not side_val:
        return []

    out: List[List[float]] = []
    for row in side_val:
        if isinstance(row, (list, tuple)) and len(row) >= 2:
            try:
                price = float(row[0])
                qty = float(row[1])
                if price <= 0 or qty <= 0:
                    continue
                out.append([price, qty])
            except Exception:
                continue
        elif isinstance(row, dict):
            try:
                price = float(row.get("price"))
                qty = float(row.get("qty"))
                if price <= 0 or qty <= 0:
                    continue
                out.append([price, qty])
            except Exception:
                continue
    return out


def _compute_best_prices(bids: List[List[float]], asks: List[List[float]]) -> Optional[Tuple[float, float]]:
    if not bids or not asks:
        return None
    try:
        best_bid = max(float(r[0]) for r in bids if isinstance(r, list) and len(r) >= 2)
        best_ask = min(float(r[0]) for r in asks if isinstance(r, list) and len(r) >= 2)
    except Exception:
        return None
    if best_bid <= 0 or best_ask <= 0:
        return None
    return best_bid, best_ask


def _compute_spread_pct(best_bid: float, best_ask: float) -> Optional[float]:
    if best_bid <= 0 or best_ask <= 0:
        return None
    if best_ask <= best_bid:
        return None
    mid = (best_bid + best_ask) / 2.0
    return (best_ask - best_bid) / mid * 100.0


def _push_orderbook(symbol: str, payload: Dict[str, Any]) -> None:
    """
    Binance depth payload 를 내부 버퍼에 저장한다.

    STRICT:
    - bids/asks 는 float 변환만 수행 (보정/추정 없음)
    - Binance 표준 키(b/a)와 일부 래퍼 키(bids/asks) 모두 지원
    - bids/asks 비어있으면 저장은 하되 health에서 FAIL 처리
    """
    sym = _normalize_symbol(symbol)
    if not sym:
        raise WSProtocolError("empty symbol in orderbook push")

    # Binance 표준: b/a
    raw_bids = payload.get("b") if payload.get("b") is not None else payload.get("bids")
    raw_asks = payload.get("a") if payload.get("a") is not None else payload.get("asks")

    normalized_bids = _normalize_depth_side(raw_bids or [])
    normalized_asks = _normalize_depth_side(raw_asks or [])

    now_ms = _now_ms()

    best_bid = None
    best_ask = None
    spread_pct = None

    best = _compute_best_prices(normalized_bids, normalized_asks)
    if best is not None:
        best_bid, best_ask = best
        spread_pct = _compute_spread_pct(best_bid, best_ask)

    ob: Dict[str, Any] = {
        "bids": normalized_bids,
        "asks": normalized_asks,
        "ts": now_ms,  # 로컬 수신 시각
    }

    # futures depthUpdate: u(lastUpdateId) / spot partial: lastUpdateId
    last_update_id = payload.get("u") if payload.get("u") is not None else payload.get("lastUpdateId")
    if last_update_id is not None:
        try:
            ob["lastUpdateId"] = int(last_update_id)
        except Exception:
            ob["lastUpdateId"] = last_update_id

    exch_ts = payload.get("E") or payload.get("T")
    if exch_ts is not None:
        try:
            ob["exchTs"] = int(exch_ts)
        except Exception:
            pass

    if best_bid is not None:
        ob["bestBid"] = best_bid
    if best_ask is not None:
        ob["bestAsk"] = best_ask
    if spread_pct is not None:
        ob["spreadPct"] = spread_pct

    with _orderbook_lock:
        _orderbook_buffers[sym] = ob


def _handle_single_msg(expected_symbol: str, data: Any) -> None:
    """
    단일 WS 메시지 처리 (Binance multiplex payload).

    Binance multiplex 메시지 예:
    {"stream": "btcusdt@kline_1m", "data": {...}}
    """
    if not isinstance(data, dict):
        return

    stream = data.get("stream")
    payload = data.get("data")
    if not isinstance(stream, str) or not isinstance(payload, dict):
        return

    sym = _normalize_symbol(expected_symbol)
    if not sym:
        raise WSProtocolError("expected_symbol normalized to empty")

    if "@kline_" in stream:
        k = payload.get("k")
        if not isinstance(k, dict):
            raise WSProtocolError(
                f"kline stream missing 'k': stream={stream} payload={_safe_dump_for_log(payload)}"
            )

        interval = str(k.get("i") or "").strip()
        if not interval:
            try:
                interval = stream.split("@kline_")[-1]
            except Exception:
                interval = ""

        if not interval:
            raise WSProtocolError(
                f"kline interval parse failed: stream={stream} payload={_safe_dump_for_log(payload)}"
            )

        payload_symbol = _normalize_symbol(str(payload.get("s") or sym))
        if payload_symbol and payload_symbol != sym:
            # 멀티플렉스 URL이 심볼 단일 기준이면 불일치는 무시 (운영에서 거의 발생하지 않음)
            log(
                f"[MD_BINANCE_WS WARN] unexpected symbol in kline stream "
                f"(expected={sym}, got={payload_symbol}, stream={stream})"
            )
            return

        if getattr(SET, "ws_log_payload_enabled", False):
            log(f"[MD_BINANCE_WS PAYLOAD] {sym} {interval} kline: {_safe_dump_for_log(k)}")

        _push_kline(sym, interval, k)
        return

    if "@depth" in stream:
        if getattr(SET, "ws_log_payload_enabled", False):
            log(f"[MD_BINANCE_WS PAYLOAD] {sym} depth: {_safe_dump_for_log(payload)}")

        _push_orderbook(sym, payload)

        if getattr(SET, "ws_log_enabled", True):
            log(f"[MD_BINANCE_WS] {sym} depth updated")
        return

    if getattr(SET, "ws_log_payload_enabled", False):
        log(f"[MD_BINANCE_WS INFO] unhandled stream message: {_safe_dump_for_log(data)}")


def _on_message(symbol: str, ws: websocket.WebSocketApp, message: Any) -> None:
    """
    WebSocketApp on_message 콜백.

    STRICT:
    - 디코딩/파싱 실패 또는 프로토콜 위반은 ws.close()로 재연결 유도.
    """
    try:
        data = _decode_msg(message)
    except Exception as e:
        log(f"[MD_BINANCE_WS] decode error: {e}")
        try:
            ws.close()
        except Exception:
            pass
        return

    if getattr(SET, "ws_log_raw_enabled", False):
        log(f"[MD_BINANCE_WS RAW] {_safe_dump_for_log(data)}")

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


def preload_klines(
    symbol: str,
    interval: str,
    rows: List[Tuple[int, float, float, float, float, float]],
) -> None:
    """
    (ts, o, h, l, c, v) 튜플 리스트를 내부 버퍼에 세팅한다.

    - 부트스트랩 단계(초기 히스토리 로딩)에서만 사용 권장
    - 라이브 운영 중에는 WS 데이터만 사용하는 것이 원칙
    - 데이터 생성/보정/추정 없음 (입력 rows 그대로 적재)
    """
    sym = _normalize_symbol(symbol)
    if not sym:
        raise ValueError("symbol is required")
    if not interval:
        raise ValueError("interval is required")

    key = (sym, interval)

    with _kline_lock:
        trimmed = list(rows[-MAX_KLINES:])
        _kline_buffers[key] = trimmed
        if trimmed:
            _kline_last_recv_ts[key] = _now_ms()

    if getattr(SET, "ws_log_enabled", True):
        log(f"[MD_BINANCE_WS BACKFILL] {sym} {interval} preloaded from bootstrap rows (len={len(trimmed)})")


def backfill_klines_from_rest(symbol: str, interval: str, rest_klines: List[Any]) -> None:
    """
    REST kline 응답을 받아 버퍼에 백필한다 (부트스트랩 전용).

    주의:
    - 이 함수는 "REST 호출"을 수행하지 않는다. (파싱/적재만 담당)
    - 운영 중 WS 장애 시 REST로 대체하는 폴백 로직은 상위 레이어에서도 금지.

    지원 포맷:
    - list[list]  : [openTime, open, high, low, close, volume, ...]
    - list[dict]  : {openTime/t, open/o, high/h, low/l, close/c, volume/v, ...}

    STRICT:
    - 유효 행만 선별/형변환 후 preload_klines 호출
    - 데이터 보정/추정/생성 없음
    """
    sym = _normalize_symbol(symbol)
    if not sym:
        raise ValueError("symbol is required")
    if not interval:
        raise ValueError("interval is required")

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
                ts = int(row.get("t") or row.get("openTime") or row.get("T") or 0)
                o = float(row.get("o") or row.get("open"))
                h = float(row.get("h") or row.get("high"))
                l = float(row.get("l") or row.get("low"))
                c = float(row.get("c") or row.get("close"))
                v = float(row.get("v") or row.get("volume"))
            else:
                continue

            if ts <= 0:
                continue
            if o <= 0 or h <= 0 or l <= 0 or c <= 0 or v < 0:
                continue
        except Exception:
            continue

        converted.append((ts, o, h, l, c, v))

    if not converted:
        if getattr(SET, "ws_log_enabled", True):
            log(f"[MD_BINANCE_WS BACKFILL] rest_klines for {sym} {interval} had no valid rows (raw_len={raw_len})")
        return

    try:
        converted.sort(key=lambda x: x[0])
    except Exception:
        pass

    if getattr(SET, "ws_log_enabled", True):
        log(
            f"[MD_BINANCE_WS BACKFILL] parsed REST klines for {sym} {interval} "
            f"raw={raw_len} converted={len(converted)} first_ts={converted[0][0]} last_ts={converted[-1][0]}"
        )

    preload_klines(sym, interval, converted)


def get_klines(symbol: str, interval: str, limit: int = 120) -> List[Tuple[int, float, float, float, float]]:
    """(ts, o, h, l, c) 튜플 리스트 반환 (레거시 포맷 호환)."""
    if limit <= 0:
        raise ValueError("limit must be > 0")
    key = (_normalize_symbol(symbol), interval)
    with _kline_lock:
        buf = _kline_buffers.get(key, [])
        if not buf:
            return []
        sliced = list(buf[-limit:])
    return [(ts, o, h, l, c) for (ts, o, h, l, c, v) in sliced]


def _log_no_buffer_once(symbol: str, interval: str, requested: int) -> None:
    if not getattr(SET, "ws_log_enabled", True):
        return

    now = time.time()
    key = (_normalize_symbol(symbol), interval)

    with _no_buf_log_lock:
        last = _no_buf_log_last.get(key, 0.0)
        if (now - last) < _NO_BUF_LOG_SUPPRESS_SEC:
            return
        _no_buf_log_last[key] = now

    log(
        f"[MD_BINANCE_WS KLINES] no kline buffer (with volume) for "
        f"{_normalize_symbol(symbol)} {interval} (requested={requested})"
    )


def get_klines_with_volume(symbol: str, interval: str, limit: int = 120) -> List[Tuple[int, float, float, float, float, float]]:
    """(ts, o, h, l, c, v) 튜플 리스트 반환."""
    if limit <= 0:
        raise ValueError("limit must be > 0")

    key = (_normalize_symbol(symbol), interval)
    with _kline_lock:
        buf = _kline_buffers.get(key, [])
        if not buf:
            _log_no_buffer_once(symbol, interval, limit)
            return []
        return list(buf[-limit:])


def get_last_kline_ts(symbol: str, interval: str) -> Optional[int]:
    key = (_normalize_symbol(symbol), interval)
    with _kline_lock:
        buf = _kline_buffers.get(key, [])
        if not buf:
            return None
        return int(buf[-1][0])


def get_last_kline_delay_ms(symbol: str, interval: str) -> Optional[int]:
    key = (_normalize_symbol(symbol), interval)
    now = _now_ms()
    with _kline_lock:
        recv_ts = _kline_last_recv_ts.get(key)
    if recv_ts is None:
        return None
    return max(0, now - recv_ts)


def get_kline_buffer_status(symbol: str, interval: str) -> Dict[str, Any]:
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
    """
    현재 오더북 스냅샷 반환.
    - bids/asks는 limit까지 슬라이스한다.
    - bids/asks가 비어있으면 None (상위에서 데이터 부족으로 FAIL 처리)
    """
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

    result["bids"] = list(bids[:limit])
    result["asks"] = list(asks[:limit])
    return result


def _compute_kline_health(symbol: str, interval: str) -> Dict[str, Any]:
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


def get_health_snapshot(symbol: str) -> Dict[str, Any]:
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
    snap = get_health_snapshot(symbol)
    return bool(snap.get("overall_ok", False))


def start_ws_loop(symbol: str) -> None:
    """
    지정 심볼에 대한 Binance Futures WS 루프를 백그라운드로 시작한다.

    정책:
    - ping_interval/ping_timeout으로 dead connection 감지
    - watchdog으로 캔들/오더북 지연 감시
    - health FAIL(지연/오더북 부재/빈 bids/asks/crossed 등) 시 ws.close() 후 자동 재연결
    - 점진적 backoff + 장시간 안정 시 리셋
    """
    sym = _normalize_symbol(symbol)
    if not sym:
        raise RuntimeError("symbol is required")
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
                                for r in st.get("reasons", []) or []:
                                    if isinstance(r, str) and r.startswith("delay_sec>"):
                                        log(
                                            f"[MD_BINANCE_WS WATCHDOG] {sym} {iv} unhealthy "
                                            f"(reason={r}) → ws.close()"
                                        )
                                        need_close = True
                                        break
                                if need_close:
                                    break

                            # 오더북 지연/부재/빈 bids/asks/crossed 감시
                            if not need_close:
                                ob_status = snap.get("orderbook") or {}
                                for r in ob_status.get("reasons", []) or []:
                                    if isinstance(r, str) and (
                                        r.startswith("delay_sec>")
                                        or r in ("no_orderbook", "no_ts", "empty_bids", "empty_asks", "no_best_prices")
                                        or r.startswith("crossed_book")
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
                            log(f"[MD_BINANCE_WS WATCHDOG] exception: {e}")
                            continue

                threading.Thread(
                    target=_watchdog,
                    name=f"md-binance-ws-watchdog-{sym}",
                    daemon=True,
                ).start()

                ws.run_forever(ping_interval=3, ping_timeout=2)

                session_dur = time.time() - start_ts
                log("[MD_BINANCE_WS] WS disconnected → retrying ...")

            except Exception as e:
                log(f"[MD_BINANCE_WS] run_forever exception: {e}")

            if session_dur > 60.0:
                retry_wait = 1.0
            else:
                retry_wait = min(retry_wait * 2.0, 10.0)

            log(f"[MD_BINANCE_WS] reconnecting after {retry_wait:.1f}s ...")
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