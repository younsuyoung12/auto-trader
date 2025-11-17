"""
market_data_ws.py
=====================================================
BingX swap-market 웹소켓으로 1m/5m/15m 캔들과 depth5를 받아서
메모리 버퍼에 보관하고, 상위 모듈(run_bot_ws, entry_guards_ws 등)에
getter 로 제공하는 모듈.

PATCH NOTES — 2025-11-17 (5m/15m 딜레이 디버그 & 폴백 금지 보강)
----------------------------------------------------
1) kline 수신 시 교환소 ts 가 0 또는 누락된 레코드는 버퍼에 넣지 않고
   [MD-WS WARN] 로그만 남기도록 수정했다.
2) 심볼/주기별 마지막 수신 시각(_kline_last_recv_ts)을 별도 보관한다.
   - get_last_kline_delay_ms(...) 로 "로컬 수신 기준 딜레이(ms)"를
     상위 모듈에서 바로 확인할 수 있다.
   - get_kline_buffer_status(...) 로 버퍼 길이/마지막 ts/딜레이를
     한 번에 조회할 수 있다.
3) REST 백필(preload_klines/backfill_klines_from_rest) 시에도
   최근 수신 시각을 갱신해 WS/REST 모두에 대해 딜레이 체크가
   일관되게 동작하도록 정리했다.
4) 이 모듈은 BingX WS/REST 원본만 사용하며, 캔들을 인위적으로
   생성/보정하는 폴백 로직은 두지 않는다.

PATCH NOTES — 2025-11-15 (4차: REST dict 포맷 백필 호환)
----------------------------------------------------
1) REST /kline 응답 포맷 보강
   - BingX REST /kline 이 list[list] 뿐만 아니라 list[dict]
     (예: {"t","o","h","l","c","v"} 또는 {"T","open",...}) 형태로도
     내려오는 것이 확인됨.
   - backfill_klines_from_rest(...) 가 두 포맷을 모두 파싱하도록 수정.
   - dict/배열 행이 섞여 있어도 파싱 가능한 행만 골라 (ts, o, h, l, c, v)
     튜플로 변환해서 버퍼에 적재.
   - 일부 행 파싱에 실패해도 전체를 버리지 않고, 성공한 행들로만
     백필을 수행하도록 유지.

PATCH NOTES — 2025-11-15 (3차: REST 히스토리 백필 지원)
----------------------------------------------------
1) REST 캔들 히스토리 백필용 헬퍼 추가
   - preload_klines(...): 외부에서 계산/가공된 (ts, o, h, l, c, v) 튜플 리스트를
     그대로 내부 버퍼(_kline_buffers)에 밀어넣어 초기화.
   - backfill_klines_from_rest(...): BingX REST /kline 응답(list[list] 또는 list[dict])을
     받아 (ts, o, h, l, c, v) 튜플 리스트로 변환한 뒤 preload_klines(...) 를 호출.
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

# { (symbol, interval): last_recv_ts_ms }
_kline_last_recv_ts: Dict[Tuple[str, str], int] = {}

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
    """WS raw/payload 를 로그로 찍을 때 문자열로 안전하게 변환한다."""
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
    """WS 로부터 받은 kline 데이터를 내부 버퍼에 반영한다."""
    key = (symbol, interval)
    ts = int(item.get("t") or item.get("T") or 0)
    if ts <= 0:
        # ts 가 비정상이면 버퍼를 오염시키지 않고 경고만 남긴다.
        try:
            log(
                f"[MD-WS WARN] invalid kline ts for {symbol} {interval}: "
                f"payload={_safe_dump_for_log(item)}"
            )
        except Exception:
            pass
        return

    o = float(item.get("o") or 0)
    h = float(item.get("h") or 0)
    l = float(item.get("l") or 0)
    c = float(item.get("c") or 0)
    v = float(item.get("v") or 0)

    now_ms = _now_ms()
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
        _kline_last_recv_ts[key] = now_ms

    if getattr(SET, "ws_log_enabled", True):
        try:
            age_ms = max(0, now_ms - ts)
            log(
                f"[MD-WS] {symbol} {interval} kline updated "
                f"(ts={ts}, age_ms={age_ms}, buf_len={after_len}, added={after_len - before_len})"
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
            return raw  # 비-JSON 문자열(Ping 등)

    return None


def _handle_ping(ws: websocket.WebSocketApp, data: Any) -> bool:
    """다양한 Ping 포맷을 감지하면 Pong 응답 후 True 를 반환한다."""
    if isinstance(data, str) and data.strip().lower() == "ping":
        ws.send("Pong")
        return True

    if isinstance(data, dict):
        if "ping" in data or data.get("op") == "ping" or data.get("event") == "ping":
            ws.send("Pong")
            return True

    return False


def _iter_kline_items(payload: Any) -> List[Dict[str, Any]]:
    """kline payload 를 통합 포맷(list[dict])으로 변환."""
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

    if "@kline_" in data_type:
        interval = data_type.split("@kline_")[-1]

        items = _iter_kline_items(payload)
        if not items:
            try:
                log(
                    f"[MD-WS WARN] kline payload has unexpected type="
                    f"{type(payload)} value={_safe_dump_for_log(payload)}"
                )
            except Exception:
                pass
            return

        if getattr(SET, "ws_log_payload_enabled", False):
            try:
                log(
                    f"[MD-WS PAYLOAD] {symbol} {interval} kline: "
                    f"{_safe_dump_for_log(items)}"
                )
            except Exception:
                pass

        for item in items:
            _push_kline(symbol, interval, item)
        return

    if "@depth" in data_type:
        if getattr(SET, "ws_log_payload_enabled", False):
            try:
                log(
                    f"[MD-WS PAYLOAD] {symbol} depth: "
                    f"{_safe_dump_for_log(payload if isinstance(payload, dict) else {'raw': payload})}"
                )
            except Exception:
                pass

        _push_orderbook(symbol, payload if isinstance(payload, dict) else {})
        if getattr(SET, "ws_log_enabled", True):
            log(f"[MD-WS] {symbol} depth updated")
        return


def _on_message(symbol: str, ws: websocket.WebSocketApp, message: Any) -> None:
    """WebSocketApp on_message 콜백."""
    data = _decode_msg(message)
    if data is None:
        return

    if getattr(SET, "ws_log_raw_enabled", False):
        try:
            log(f"[MD-WS RAW] {_safe_dump_for_log(data)}")
        except Exception:
            pass

    if _handle_ping(ws, data):
        return

    if isinstance(data, list):
        for item in data:
            _handle_single_msg(symbol, ws, item)
        return

    _handle_single_msg(symbol, ws, data)


def _on_error(ws: websocket.WebSocketApp, error: Any) -> None:
    """WS 에러 콜백."""
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
                ws.run_forever(ping_interval=25, ping_timeout=10)
            except Exception as e:
                log(f"[MD-WS] run_forever error: {e}")
            log(f"[MD-WS] reconnect in {RECONNECT_WAIT}s ...")
            time.sleep(RECONNECT_WAIT)

    th = threading.Thread(target=_runner, name=f"md-ws-{symbol}", daemon=True)
    th.start()
    log(f"[MD-WS] background ws started for {symbol}")


def preload_klines(
    symbol: str,
    interval: str,
    rows: List[Tuple[int, float, float, float, float, float]],
) -> None:
    """(ts, o, h, l, c, v) 튜플 리스트를 그대로 내부 버퍼에 세팅한다."""
    key = (symbol, interval)
    with _kline_lock:
        trimmed = list(rows[-MAX_KLINES:])
        _kline_buffers[key] = trimmed
        buf_len = len(trimmed)
        if trimmed:
            _kline_last_recv_ts[key] = _now_ms()

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
    rest_klines: List[Any],
) -> None:
    """BingX REST /kline 응답을 받아 버퍼에 백필한다."""
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
                    or row.get("T")
                    or row.get("openTime")
                    or row.get("startTime")
                    or 0
                )
                o = float(row.get("o") or row.get("open") or 0)
                h = float(row.get("h") or row.get("high") or 0)
                l = float(row.get("l") or row.get("low") or 0)
                c = float(row.get("c") or row.get("close") or 0)
                v = float(row.get("v") or row.get("volume") or 0)
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
                    f"[MD-WS BACKFILL] rest_klines for {symbol} {interval} "
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
                f"[MD-WS BACKFILL] parsed REST klines for {symbol} {interval} "
                f"raw={raw_len} converted={len(converted)} "
                f"first_ts={first_ts} last_ts={last_ts}"
            )
        except Exception:
            pass

    preload_klines(symbol, interval, converted)


def get_klines(symbol: str, interval: str, limit: int = 120):
    """(ts, o, h, l, c) 튜플 리스트를 반환한다 (기존 포맷)."""
    key = (symbol, interval)
    with _kline_lock:
        buf = _kline_buffers.get(key, [])
        if not buf:
            return []
        sliced = list(buf[-limit:])
    return [(ts, o, h, l, c) for (ts, o, h, l, c, v) in sliced]


def get_klines_with_volume(symbol: str, interval: str, limit: int = 120):
    """(ts, o, h, l, c, v) 튜플 리스트를 반환한다."""
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


def get_last_kline_ts(symbol: str, interval: str) -> Optional[int]:
    """지정 심볼/주기의 마지막 캔들 교환소 타임스탬프(ms)를 반환한다."""
    key = (symbol, interval)
    with _kline_lock:
        buf = _kline_buffers.get(key, [])
        if not buf:
            return None
        return int(buf[-1][0])


def get_last_kline_delay_ms(symbol: str, interval: str) -> Optional[int]:
    """마지막 캔들이 로컬에 도착한 뒤 경과한 시간(ms)을 반환한다.

    - WS 가 멈췄는지/지연됐는지를 상위 레이어에서 판단할 때 사용한다.
    - kline 자체는 항상 BingX WS/REST 원본만 사용하고, 여기서 인위적으로
      보정하거나 생성하지 않는다(폴백 금지).
    """
    key = (symbol, interval)
    now = _now_ms()
    with _kline_lock:
        recv_ts = _kline_last_recv_ts.get(key)
    if recv_ts is None:
        return None
    return max(0, now - recv_ts)


def get_kline_buffer_status(symbol: str, interval: str) -> Dict[str, Any]:
    """디버그용: 버퍼 길이, 마지막 ts, 지연(ms)을 dict 로 돌려준다."""
    key = (symbol, interval)
    with _kline_lock:
        buf = _kline_buffers.get(key, [])
        last_ts = buf[-1][0] if buf else None
        last_recv = _kline_last_recv_ts.get(key)
    delay_ms = None
    if last_recv is not None:
        delay_ms = max(0, _now_ms() - last_recv)
    return {
        "symbol": symbol,
        "interval": interval,
        "buffer_len": len(buf),
        "last_ts": last_ts,
        "last_recv_ts": last_recv,
        "delay_ms": delay_ms,
    }


def get_orderbook(symbol: str, limit: int = 5) -> Optional[Dict[str, Any]]:
    """최대 limit 까지 잘린 bids/asks 를 포함해 현재 오더북 스냅샷을 반환한다."""
    with _orderbook_lock:
        ob = _orderbook_buffers.get(symbol)
        if not ob:
            return None
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
    "get_last_kline_ts",
    "get_last_kline_delay_ms",
    "get_kline_buffer_status",
    "get_orderbook",
]
