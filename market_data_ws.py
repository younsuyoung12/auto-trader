"""
BingX swap-market 웹소켓으로 1m/5m/15m 캔들과 depth5를 받아서 메모리에 보관한다.

2025-11-14 보강 내용 (본 패치에서 추가/수정)
----------------------------------------------------
5) 메시지 디코더 안전화
   - gzip 여부와 무관하게 bytes/str 모두 처리.
   - 비압축 JSON 텍스트/바이너리 혼용 수신도 파싱.
6) Ping 포맷 다양성 대응
   - "Ping"(문자열), {"ping": ts}, {"op":"ping"|"event":"ping"} 모두에 대해
     "Pong" 문자열로 응답해 세션 유지.
7) 버퍼 동시성 안전화
   - 캔들/오더북 버퍼 접근에 Lock 적용(쓰기/읽기 경쟁 방지).
8) depth 원본 timestamp 보존
   - payload 내 time/T/E(있을 경우)를 exchTs로 저장.
9) 재확인: 구독 주기는 settings.ws_subscribe_tfs 우선, 미지정 시 ["1m","5m","15m"].

기존 2025-11-14 보강 요약
----------------------------------------------------
1) depth(payload)에 markPrice / lastPrice / time 같은 필드가 섞여서 올 때가 있어서
   그대로 버퍼에 남겨두도록 했다. (entry_guards_ws 에서 mark/last 괴리 가드가 이걸 본다.)
2) depth 형식이 list / dict 혼합으로 와도 최대한 bids/asks 를 뽑아서 저장하도록 보정했다.
3) WS 주소를 환경변수 BINGX_SWAP_WS_BASE 로도 바꿀 수 있게 했다.
   (없으면 기존처럼 wss://open-api-swap.bingx.com/swap-market 사용)
4) 모든 저장 시 수신 시각(ts_ms)을 붙여서 나중에 "깊이가 오래됐나?"를 볼 수 있게 했다.

기존 설명
----------------------------------------------------
- URL: wss://open-api-swap.bingx.com/swap-market
- 구독: {"id": "...", "reqType": "sub", "dataType": "BTC-USDT@kline_1m"}
- 메시지는 gzip 으로 온다 → 반드시 풀어야 한다
- Ping 이 오면 Pong 으로 응답해야 연결이 유지된다
"""

from __future__ import annotations

import json
import time
import threading
import gzip
import io
import os
from typing import Any, Dict, List, Tuple, Optional

import websocket  # pip install websocket-client

from settings_ws import load_settings
from telelog import log

SET = load_settings()

# 환경변수로도 바꿀 수 있게 (swap 우선 → 일반 ws 베이스 → 기본값 순)
WS_URL = (
    os.getenv("BINGX_SWAP_WS_BASE")
    or os.getenv("BINGX_WS_BASE")
    or "wss://open-api-swap.bingx.com/swap-market"
)

# settings 에 없으면 1m/5m/15m 로 기본 구독
WS_INTERVALS: List[str] = getattr(SET, "ws_subscribe_tfs", None) or ["1m", "5m", "15m"]

# { (symbol, interval): [(ts, o, h, l, c, v), ...] }
_kline_buffers: Dict[Tuple[str, str], List[Tuple[int, float, float, float, float, float]]] = {}

# { symbol: {"bids": [...], "asks": [...], "ts": ..., "exchTs": ..., "markPrice": ..., "lastPrice": ...} }
_orderbook_buffers: Dict[str, Dict[str, Any]] = {}

# 동시성 보호용 Lock (읽기/쓰기 경쟁 방지)
_kline_lock = threading.Lock()
_orderbook_lock = threading.Lock()

MAX_KLINES = 500
RECONNECT_WAIT = 5


def _now_ms() -> int:
    return int(time.time() * 1000)


def _to_ws_symbol(sym: str) -> str:
    # BingX 예제가 "BTC-USDT@kline_1m" 이니까 그대로 사용
    return sym.upper()


def _build_sub_msgs(symbol: str) -> List[Dict[str, Any]]:
    """
    BingX 는 한 번에 여러 개를 list 로 보내도 되고
    하나씩 보내도 되니까 여기서는 하나씩 보낸다고 가정.
    depth5 포함.
    """
    ws_sym = _to_ws_symbol(symbol)
    msgs: List[Dict[str, Any]] = []
    for iv in WS_INTERVALS:
        msgs.append(
            {
                "id": f"sub-{symbol}-{iv}",
                "reqType": "sub",
                "dataType": f"{ws_sym}@kline_{iv}",
            }
        )
    # depth5 도 같이
    msgs.append(
        {
            "id": f"sub-{symbol}-depth5",
            "reqType": "sub",
            "dataType": f"{ws_sym}@depth5",
        }
    )
    return msgs


def _push_kline(symbol: str, interval: str, item: Dict[str, Any]) -> None:
    """
    BingX kline 예시는 보통 이렇게 온다:
    {
      "dataType": "BTC-USDT@kline_1m",
      "data": {
          "t": 1731455220000,  # start time
          "o": "101000",
          "h": "...",
          ...
      }
    }
    """
    key = (symbol, interval)
    ts = int(item.get("t") or item.get("T") or 0)
    o = float(item.get("o") or 0)
    h = float(item.get("h") or 0)
    l = float(item.get("l") or 0)
    c = float(item.get("c") or 0)
    v = float(item.get("v") or 0)

    # 마지막 바 갱신/추가 (동시성 보호)
    with _kline_lock:
        buf = _kline_buffers.setdefault(key, [])
        if buf and buf[-1][0] == ts:
            buf[-1] = (ts, o, h, l, c, v)
        else:
            buf.append((ts, o, h, l, c, v))
            if len(buf) > MAX_KLINES:
                del buf[0 : len(buf) - MAX_KLINES]


def _normalize_depth_side(side_val: Any) -> List[List[float]]:
    """
    depth 쪽에 bids / asks 가 list 로도 오고, dict 의 list 로도 올 수 있으니
    여기서 통일해서 [ [price, qty], ... ] 형태로 만들어 준다.
    """
    if not side_val:
        return []
    out: List[List[float]] = []
    for row in side_val:
        if isinstance(row, list) and len(row) >= 2:
            # ["103000", "0.123"] 또는 [103000, 0.123]
            try:
                price = float(row[0])
                qty = float(row[1])
                out.append([price, qty])
            except Exception:
                continue
        elif isinstance(row, dict):
            try:
                price = float(row.get("price"))
                qty = float(row.get("qty") or row.get("quantity") or row.get("size") or 0.0)
                out.append([price, qty])
            except Exception:
                continue
    return out


def _push_orderbook(symbol: str, payload: Dict[str, Any]) -> None:
    """
    depth payload 를 그대로 버퍼에 넣되,
    bids/asks 는 우리가 쓸 수 있는 형태로 normalize.
    markPrice/lastPrice, 거래소 timestamp(time/T/E) 같은 건 그대로/별도 키로 둔다.
    """
    normalized_bids = _normalize_depth_side(payload.get("bids") or payload.get("buys") or [])
    normalized_asks = _normalize_depth_side(payload.get("asks") or payload.get("sells") or [])

    ob: Dict[str, Any] = {
        "bids": normalized_bids,
        "asks": normalized_asks,
        "ts": _now_ms(),  # 수신(로컬) 시각
    }

    # 원본에 markPrice / lastPrice / time류가 있으면 보존
    if "markPrice" in payload:
        ob["markPrice"] = payload.get("markPrice")
    if "lastPrice" in payload:
        ob["lastPrice"] = payload.get("lastPrice")

    exch_ts = payload.get("time") or payload.get("T") or payload.get("E")
    if exch_ts is not None:
        try:
            ob["exchTs"] = int(exch_ts)
        except Exception:
            pass

    with _orderbook_lock:
        _orderbook_buffers[symbol] = ob


def _decode_msg(raw: Any) -> Any:
    """
    수신 프레임을 안전하게 JSON 파싱.
    - bytes: gzip→JSON, 실패 시 UTF-8 텍스트→JSON
    - str: 바로 JSON 시도, 실패하면 원문 반환(예: "Ping")
    """
    # bytes
    if isinstance(raw, (bytes, bytearray)):
        try:
            decompressed = gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
            txt = decompressed.decode("utf-8")
            return json.loads(txt)
        except Exception:
            try:
                return json.loads(bytes(raw).decode("utf-8"))
            except Exception:
                # 마지막으로 바이너리를 문자열로만 리턴(비-JSON)
                try:
                    return bytes(raw).decode("utf-8", errors="ignore")
                except Exception:
                    return None

    # str
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return raw  # 비-JSON 문자열(Ping 등)

    return None


def _handle_ping(ws: websocket.WebSocketApp, data: Any) -> bool:
    """
    다양한 Ping 포맷을 감지하면 Pong 응답 후 True 반환.
    """
    # 문자열 Ping
    if isinstance(data, str) and data.strip().lower() == "ping":
        ws.send("Pong")
        return True

    # dict 기반 Ping
    if isinstance(data, dict):
        if "ping" in data or data.get("op") == "ping" or data.get("event") == "ping":
            # 서버가 특정 포맷을 요구하지 않는 한 문자열 Pong 으로 통일
            ws.send("Pong")
            return True

    return False


def _on_message(symbol: str, ws: websocket.WebSocketApp, message: Any) -> None:
    data = _decode_msg(message)
    if data is None:
        return

    # Ping 처리(여러 포맷)
    if _handle_ping(ws, data):
        return

    # BingX 가 배열로 여러 개를 보내는 경우도 대비
    if isinstance(data, list):
        for item in data:
            _handle_single_msg(symbol, ws, item)
        return

    _handle_single_msg(symbol, ws, data)


def _handle_single_msg(symbol: str, ws: websocket.WebSocketApp, data: Any) -> None:
    if not isinstance(data, dict):
        return

    data_type = data.get("dataType")
    payload = data.get("data")

    if not data_type:
        return

    # kline 예: "BTC-USDT@kline_1m"
    if "@kline_" in data_type and isinstance(payload, dict):
        interval = data_type.split("@kline_")[-1]
        _push_kline(symbol, interval, payload)
        if getattr(SET, "ws_log_enabled", True):
            log(f"[MD-WS] {symbol} {interval} kline updated")
        return

    # depth 예: "BTC-USDT@depth5"
    if "@depth" in data_type:
        # payload 가 dict 라고 가정하되, list 로 오면 빈 dict 로 넘긴다
        _push_orderbook(symbol, payload if isinstance(payload, dict) else {})
        if getattr(SET, "ws_log_enabled", True):
            log(f"[MD-WS] {symbol} depth updated")
        return


def _on_error(ws: websocket.WebSocketApp, error: Any) -> None:
    log(f"[MD-WS] error: {error}")


def _on_close(ws: websocket.WebSocketApp, code: Any, msg: Any) -> None:
    log(f"[MD-WS] closed: {code} {msg}")


def _on_open(symbol: str, ws: websocket.WebSocketApp) -> None:
    msgs = _build_sub_msgs(symbol)
    for m in msgs:
        try:
            ws.send(json.dumps(m))
        except Exception as e:
            log(f"[MD-WS] subscribe send error: {e}")
    log(f"[MD-WS] subscribed: {msgs}")


def start_ws_loop(symbol: str) -> None:
    """백그라운드 스레드에서 무한 재접속 루프로 WS를 유지한다."""
    url = WS_URL

    def _runner() -> None:
        while True:
            try:
                ws = websocket.WebSocketApp(
                    url,
                    on_open=lambda w: _on_open(symbol, w),
                    on_message=lambda w, m: _on_message(symbol, w, m),
                    on_error=_on_error,
                    on_close=_on_close,
                )
                # 서버 ping/pong과 별개로 클라이언트 ping도 주기적으로 보냄
                ws.run_forever(ping_interval=25, ping_timeout=10)
            except Exception as e:
                log(f"[MD-WS] run_forever error: {e}")
            log(f"[MD-WS] reconnect in {RECONNECT_WAIT}s ...")
            time.sleep(RECONNECT_WAIT)

    th = threading.Thread(target=_runner, name=f"md-ws-{symbol}", daemon=True)
    th.start()
    log(f"[MD-WS] background ws started for {symbol}")


# ─────────────────────────────
# getter 들 (잠금으로 동시성 보장)
# ─────────────────────────────

def get_klines(symbol: str, interval: str, limit: int = 120):
    key = (symbol, interval)
    with _kline_lock:
        buf = _kline_buffers.get(key, [])
        if not buf:
            return []
        sliced = list(buf[-limit:])
    # (ts, o, h, l, c) 만 주던 기존 포맷 유지
    return [(ts, o, h, l, c) for (ts, o, h, l, c, v) in sliced]


def get_klines_with_volume(symbol: str, interval: str, limit: int = 120):
    key = (symbol, interval)
    with _kline_lock:
        buf = _kline_buffers.get(key, [])
        if not buf:
            return []
        return list(buf[-limit:])


def get_orderbook(symbol: str, limit: int = 5) -> Optional[Dict[str, Any]]:
    with _orderbook_lock:
        ob = _orderbook_buffers.get(symbol)
        if not ob:
            return None
        # bids/asks 만 잘라서 반환 (markPrice 등은 그대로 둔다)
        if "bids" in ob:
            return {
                **ob,
                "bids": ob["bids"][:limit],
                "asks": ob.get("asks", [])[:limit],
            }
        return dict(ob)


__all__ = [
    "start_ws_loop",
    "get_klines",
    "get_klines_with_volume",
    "get_orderbook",
]
