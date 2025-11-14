"""
market_data_ws.py
=====================================================
BingX swap-market 웹소켓으로 1m/5m/15m 캔들과 depth5를 받아서
메모리 버퍼에 보관하고, 상위 모듈(run_bot_ws, entry_guards_ws 등)에
getter 로 제공하는 모듈.

PATCH NOTES — 2025-11-15 (3차: REST 히스토리 백필 지원)
----------------------------------------------------
1) REST 캔들 히스토리 백필용 헬퍼 추가
   - preload_klines(...): 외부에서 계산/가공된 (ts, o, h, l, c, v) 튜플 리스트를
     그대로 내부 버퍼(_kline_buffers)에 밀어넣어 초기화.
   - backfill_klines_from_rest(...): BingX REST /kline 응답(list[list])을 그대로 받아
     (ts, o, h, l, c, v) 튜플 리스트로 변환한 뒤 preload_klines(...) 를 호출.
   - 둘 다 MAX_KLINES 를 넘으면 최근 MAX_KLINES 개만 보관.
   - 백필 시 "[MD-WS BACKFILL] ..." 로그로 실제 채워진 개수와 심볼/타임프레임을 남김.
   - run_bot_ws 쪽에서 fetch_klines_rest(...) 결과를 받아서 이 헬퍼들을 사용하면,
     WS 연결 직후에도 5m/15m 지표 계산에 필요한 히스토리 캔들이 바로 준비된다.

PATCH NOTES — 2025-11-15 (2차 보정)
----------------------------------------------------
1) BingX WebSocket kline payload 형식 보정
   - 실제 수신 형식이 data: { ... } 가 아니라 data: [ { ... } ] (list 안에 dict) 인 것이 확인됨.
   - kline 처리부에서 payload 가 dict 인 경우뿐 아니라 list 인 경우도 지원하도록 수정.
   - list 인 경우, 내부의 dict 들을 하나씩 _push_kline(...) 에 전달하여 버퍼에 저장.
   - dict/list 가 아닌 예외적인 형식은 WARN 로그로 남기고 무시.

2) kline 관련 진단 로그 강화
   - payload 가 dict 가 아닌 경우, type 과 payload 일부를 함께 경고 로그로 남기도록 정리.
   - 정상 저장 시에도 ws_log_enabled 옵션이 켜져 있으면
     "[MD-WS] BTC-USDT 5m kline updated (added=..., buf_len=...)" 형식으로 버퍼 길이를 함께 기록.
   - get_klines_with_volume(...) 에서 버퍼가 비어 있는 경우
     "[MD-WS KLINES] no kline buffer (with volume) ..." 로그를 남기도록 유지.

2025-11-15 1차 변경 / 디버그 옵션 추가
----------------------------------------------------
1) BingX 에서 들어오는 WebSocket 프레임/캔들/호가 원본을 Render 로그에서 확인할 수 있도록
   디버그 로그 옵션을 추가했다.
   - settings_ws.ws_log_raw_enabled = True  이면 디코딩된 WS 프레임 전체를
     "[MD-WS RAW] ..." 형식으로 로그에 남긴다.
   - settings_ws.ws_log_payload_enabled = True 이면 개별 kline/depth payload 를
     "[MD-WS PAYLOAD] ..." 형식으로 로그에 남긴다.
   - 로그 폭주 방지를 위해 한 프레임/페이로드당 최대 2000자까지만 출력한 뒤
     잘라내고 '(truncated, total_len=...)' 표시를 붙인다.

2025-11-14 변경 / 중요사항
----------------------------------------------------
1) 메시지 디코더 안전화
   - gzip 여부와 무관하게 bytes/str 모두 처리.
   - 비압축 JSON 텍스트/바이너리 혼용 수신도 파싱.
2) Ping 포맷 다양성 대응
   - "Ping"(문자열), {"ping": ts}, {"op":"ping"}, {"event":"ping"} 모두에 대해
     "Pong" 문자열로 응답해 세션 유지.
3) 버퍼 동시성 안전화
   - 캔들/오더북 버퍼 접근에 Lock 적용(쓰기/읽기 경쟁 방지).
4) depth 원본 timestamp 보존
   - payload 내 time/T/E(있을 경우)를 exchTs 로 저장.
5) 구독 타임프레임 설정
   - settings.ws_subscribe_tfs 를 우선 사용, 미지정 시 ["1m","5m","15m"] 기본값.

기존 보강 요약
----------------------------------------------------
- depth(payload)에 markPrice / lastPrice / time 같은 필드가 섞여서 올 때가 있어서
  그대로 버퍼에 남겨두도록 했다. (entry_guards_ws 에서 mark/last 괴리 가드가 이걸 본다.)
- depth 형식이 list / dict 혼합으로 와도 최대한 bids/asks 를 뽑아서 저장하도록 보정했다.
- WS 주소를 환경변수 BINGX_SWAP_WS_BASE 로도 바꿀 수 있게 했다.
  (없으면 기존처럼 wss://open-api-swap.bingx.com/swap-market 사용)
- 모든 저장 시 수신 시각(ts_ms)을 붙여서 나중에 "깊이가 오래됐나?"를 볼 수 있게 했다.

기본 설명
----------------------------------------------------
- URL: wss://open-api-swap.bingx.com/swap-market
- 구독: {"id": "...", "reqType": "sub", "dataType": "BTC-USDT@kline_1m"}
- 메시지는 gzip 으로 올 수 있다 → 반드시 풀어야 한다
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
    """현재 시각을 ms 단위 정수로 반환."""
    return int(time.time() * 1000)


def _safe_dump_for_log(obj: Any, max_len: int = 2000) -> str:
    """WS raw/payload 를 로그로 찍을 때 문자열로 안전하게 변환한다.

    - JSON 직렬화를 시도하고, 실패하면 str(obj)를 사용한다.
    - max_len 보다 길면 잘라내고 '(truncated, total_len=...)' 를 붙인다.
    """
    try:
        s = json.dumps(obj, ensure_ascii=False, default=str)
    except Exception:
        s = str(obj)
    if len(s) > max_len:
        return s[:max_len] + f"... (truncated, total_len={len(s)})"
    return s


def _to_ws_symbol(sym: str) -> str:
    """심볼을 WS 구독용 포맷으로 변환 (예: BTC-USDT)."""
    return sym.upper()


def _build_sub_msgs(symbol: str) -> List[Dict[str, Any]]:
    """구독 메시지들을 구성한다 (1m/5m/15m + depth5)."""
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
    """WS 로부터 받은 kline 데이터를 내부 버퍼에 반영한다.

    item 예시 (BingX 현재 포맷):
        {
            "c": "96051.0",
            "o": "96306.1",
            "h": "96357.6",
            "l": "96050.8",
            "v": "56.9695",
            "T": 1763142300000
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
        before_len = len(buf)
        if buf and buf[-1][0] == ts:
            buf[-1] = (ts, o, h, l, c, v)
        else:
            buf.append((ts, o, h, l, c, v))
            if len(buf) > MAX_KLINES:
                del buf[0 : len(buf) - MAX_KLINES]
        after_len = len(buf)

    # 기본 로그는 _handle_single_msg 쪽에서 통합해서 찍는다.
    if getattr(SET, "ws_log_enabled", True):
        try:
            log(
                f"[MD-WS] {symbol} {interval} kline updated "
                f"(ts={ts}, buf_len={after_len}, added={after_len - before_len})"
            )
        except Exception:
            pass


def _normalize_depth_side(side_val: Any) -> List[List[float]]:
    """depth 쪽 bids/asks 를 [ [price, qty], ... ] 형태로 정규화한다."""
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
    """depth payload 를 내부 버퍼에 저장한다."""
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
    """수신 프레임을 안전하게 JSON 파싱한다."""
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
    """다양한 Ping 포맷을 감지하면 Pong 응답 후 True 를 반환한다."""
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


def _iter_kline_items(payload: Any) -> List[Dict[str, Any]]:
    """kline payload 를 통합 포맷(list[dict])으로 변환.

    BingX 현재 포맷:
        "data": [{ ... }]  # list 안에 dict 하나 이상
    과거/다른 포맷:
        "data": { ... }    # 단일 dict
    둘 다 지원하기 위해 list[dict] 로 정규화해서 반환한다.
    """
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        items: List[Dict[str, Any]] = []
        for elem in payload:
            if isinstance(elem, dict):
                items.append(elem)
        return items
    return []


def _handle_single_msg(symbol: str, ws: websocket.WebSocketApp, data: Any) -> None:
    """단일 WS 메시지를 처리한다."""
    if not isinstance(data, dict):
        return

    data_type = data.get("dataType")
    payload = data.get("data")

    if not data_type:
        return

    # kline 예: "BTC-USDT@kline_1m"
    if "@kline_" in data_type:
        interval = data_type.split("@kline_")[-1]

        # payload 타입에 따라 list/dict 모두 지원
        items = _iter_kline_items(payload)
        if not items:
            # 어떤 형식인지 Render 에서 바로 확인할 수 있도록 경고 로그 남김
            try:
                log(
                    f"[MD-WS WARN] kline payload has unexpected type="
                    f"{type(payload)} value={_safe_dump_for_log(payload)}"
                )
            except Exception:
                pass
            return

        # kline payload 디버그 로그 (옵션)
        if getattr(SET, "ws_log_payload_enabled", False):
            try:
                log(
                    f"[MD-WS PAYLOAD] {symbol} {interval} kline: "
                    f"{_safe_dump_for_log(items)}"
                )
            except Exception:
                # 로깅 실패는 시세 처리에 영향을 주지 않음
                pass

        # 각 item 을 버퍼에 반영
        for item in items:
            _push_kline(symbol, interval, item)
        return

    # depth 예: "BTC-USDT@depth5"
    if "@depth" in data_type:
        # depth payload 디버그 로그 (옵션)
        if getattr(SET, "ws_log_payload_enabled", False):
            try:
                log(
                    f"[MD-WS PAYLOAD] {symbol} depth: "
                    f"{_safe_dump_for_log(payload if isinstance(payload, dict) else {'raw': payload})}"
                )
            except Exception:
                pass

        # payload 가 dict 라고 가정하되, list 로 오면 빈 dict 로 넘긴다
        _push_orderbook(symbol, payload if isinstance(payload, dict) else {})
        if getattr(SET, "ws_log_enabled", True):
            log(f"[MD-WS] {symbol} depth updated")
        return


def _on_message(symbol: str, ws: websocket.WebSocketApp, message: Any) -> None:
    """WebSocketApp on_message 콜백."""
    data = _decode_msg(message)
    if data is None:
        return

    # RAW 프레임 디버그 로그 (옵션)
    if getattr(SET, "ws_log_raw_enabled", False):
        try:
            log(f"[MD-WS RAW] {_safe_dump_for_log(data)}")
        except Exception:
            # 로깅에서 예외가 나더라도 WS 루프는 계속 돌아가야 한다.
            pass

    # Ping 처리(여러 포맷)
    if _handle_ping(ws, data):
        return

    # BingX 가 배열로 여러 개를 보내는 경우도 대비
    if isinstance(data, list):
        for item in data:
            _handle_single_msg(symbol, ws, item)
        return

    _handle_single_msg(symbol, ws, data)


def _on_error(ws: websocket.WebSocketApp, error: Any) -> None:
    """WS 에러 콜백.

    예) "Connection to remote host was lost" 와 같이 나오면
    - 네트워크 끊김 / 서버 단절로 인해 연결이 끊어진 상황이고,
    - start_ws_loop 의 재시도 루프가 RECONNECT_WAIT 초 뒤에 자동 재접속한다.
    """
    log(f"[MD-WS] error: {error}")


def _on_close(ws: websocket.WebSocketApp, code: Any, msg: Any) -> None:
    """WS 연결 종료 콜백."""
    log(f"[MD-WS] closed: {code} {msg}")


def _on_open(symbol: str, ws: websocket.WebSocketApp) -> None:
    """WS 연결 직후 구독 메시지를 전송한다."""
    msgs = _build_sub_msgs(symbol)
    for m in msgs:
        try:
            ws.send(json.dumps(m))
        except Exception as e:
            log(f"[MD-WS] subscribe send error: {e}")
    log(f"[MD-WS] subscribed: {msgs}")


def start_ws_loop(symbol: str) -> None:
    """백그라운드 스레드에서 무한 재접속 루프로 WS를 유지한다.

    - run_bot_ws 에서는 보통 다음 순서로 사용하는 것을 권장:
      1) (선택) REST 로 5m/15m 히스토리 캔들 가져오기
         → fetch_klines_rest(...) 결과를 backfill_klines_from_rest(...) 에 넣어 백필
      2) start_ws_loop(symbol) 호출로 실시간 WS 시작
      3) 이후 get_klines_with_volume(...)/get_orderbook(...) 으로 시세/호가 조회
    """
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
# REST 히스토리 백필용 헬퍼
# ─────────────────────────────


def preload_klines(
    symbol: str,
    interval: str,
    rows: List[Tuple[int, float, float, float, float, float]],
) -> None:
    """(ts, o, h, l, c, v) 튜플 리스트를 그대로 내부 버퍼에 세팅한다.

    - rows 는 오래된 순서 → 최신 순서로 들어있다고 가정한다.
    - MAX_KLINES 를 초과하면 가장 최근 MAX_KLINES 개만 보관한다.
    - 보통은 REST /kline 응답을 가공해서 이 함수로 밀어넣는다.
    """
    key = (symbol, interval)
    with _kline_lock:
        # 너무 길면 뒤에서부터 자르기
        trimmed = list(rows[-MAX_KLINES:])
        _kline_buffers[key] = trimmed
        buf_len = len(trimmed)

    if getattr(SET, "ws_log_enabled", True):
        try:
            log(
                f"[MD-WS BACKFILL] {symbol} {interval} preloaded "
                f"from REST (len={buf_len})"
            )
        except Exception:
            pass


def backfill_klines_from_rest(
    symbol: str,
    interval: str,
    rest_klines: List[List[Any]],
) -> None:
    """BingX REST /kline 응답(list[list])을 받아 버퍼에 백필한다.

    예상 REST 포맷 예시:
        rest_klines = [
            [openTime, open, high, low, close, volume, closeTime, ...],
            ...
        ]

    - openTime/closeTime 는 ms 단위라고 가정한다.
    - REST 응답을 있는 그대로 넘기되, 필요 없는 필드는 이 함수에서 무시한다.
    - 변환에 실패하는 행은 건너뛴다.
    """
    converted: List[Tuple[int, float, float, float, float, float]] = []
    for row in rest_klines:
        if not isinstance(row, (list, tuple)) or len(row) < 6:
            continue
        try:
            ts = int(row[0])
            o = float(row[1])
            h = float(row[2])
            l = float(row[3])
            c = float(row[4])
            v = float(row[5])
        except Exception:
            continue
        converted.append((ts, o, h, l, c, v))

    if not converted:
        if getattr(SET, "ws_log_enabled", True):
            try:
                log(
                    f"[MD-WS BACKFILL] rest_klines for {symbol} {interval} "
                    f"had no valid rows"
                )
            except Exception:
                pass
        return

    preload_klines(symbol, interval, converted)


# ─────────────────────────────
# getter 들 (잠금으로 동시성 보장)
# ─────────────────────────────


def get_klines(symbol: str, interval: str, limit: int = 120):
    """(ts, o, h, l, c) 튜플 리스트를 반환한다 (기존 포맷).

    내부 버퍼에는 (ts, o, h, l, c, v)가 저장되어 있으나,
    기존 호출부 호환을 위해 v 를 잘라서 리턴한다.
    """
    key = (symbol, interval)
    with _kline_lock:
        buf = _kline_buffers.get(key, [])
        if not buf:
            return []
        sliced = list(buf[-limit:])
    # (ts, o, h, l, c) 만 주던 기존 포맷 유지
    return [(ts, o, h, l, c) for (ts, o, h, l, c, v) in sliced]


def get_klines_with_volume(symbol: str, interval: str, limit: int = 120):
    """(ts, o, h, l, c, v) 튜플 리스트를 반환한다.

    - 시그널/지표 계산에서 볼륨까지 필요할 때 사용.
    - 버퍼가 비어 있으면 Render 로그에 경고를 남긴다.
    """
    key = (symbol, interval)
    with _kline_lock:
        buf = _kline_buffers.get(key, [])
        if not buf:
            try:
                log(
                    f"[MD-WS KLINES] no kline buffer (with volume) for "
                    f"{symbol} {interval} (requested={limit})"
                )
            except Exception:
                pass
            return []
        return list(buf[-limit:])


def get_orderbook(symbol: str, limit: int = 5) -> Optional[Dict[str, Any]]:
    """최대 limit 까지 잘린 bids/asks 를 포함해 현재 오더북 스냅샷을 반환한다."""
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
    "preload_klines",
    "backfill_klines_from_rest",
    "get_klines",
    "get_klines_with_volume",
    "get_orderbook",
]
