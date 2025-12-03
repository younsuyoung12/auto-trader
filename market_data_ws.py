"""market_data_ws.py
=====================================================
BingX swap-market WebSocket으로 멀티 타임프레임 K라인과 depth5 오더북을 받아
메모리 버퍼에 저장하고, 상위 모듈(run_bot_ws, entry_guards_ws 등)에
getter / 데이터 헬스 체크 유틸을 제공하는 모듈.

주요 역할
-----------------------------------------------------
- 설정된 타임프레임의 K라인 스트림 구독 및 버퍼링
  · 기본값: ["1m","3m","5m","15m","30m","1h","2h","4h","6h","12h","1d","3d","1w","1M"]
- 심볼별 depth5 오더북(bids/asks, bestBid/bestAsk, spreadPct) 버퍼링
- REST 히스토리 백필(preload_klines/backfill_klines_from_rest) 지원
  · run_bot_ws 부트스트랩(시작 시 초기 히스토리 채우기)에서만 사용
  · 라이브 운영 중에는 WS 데이터만 사용(인위적인 캔들 생성/보정 없음)
- 버퍼 길이/지연/오더북 최신 여부 기반 데이터 헬스 스냅샷
  · get_health_snapshot(symbol), is_data_healthy(symbol)

2025-11-21 설명 보강 (EXIT 1m 캔들 종가 기준 설계)
-----------------------------------------------------
1) EXIT 시점(포지션 감시)은 상위 레이어(run_bot_ws / position_watch_ws)에서
   get_klines_with_volume(...) 또는 get_last_kline_ts(...)를 이용해
   "새로운 1m 캔들이 생성된 순간(= 직전 1m 캔들 종가 확정 시점)"을 감지해서
   구현하도록 한다.
2) 이 모듈은 WS 버퍼 제공과 데이터 헬스 체크 역할만 수행하며,
   캔들 종가 이벤트 콜백/훅은 두지 않는다. (로직 단순화 및 책임 분리)

2025-11-20 변경 사항 (B안: 1m REST 백필 호환성 점검)
-----------------------------------------------------
1) run_bot_ws._backfill_ws_kline_history 에서 1m/5m/15m REST 히스토리 백필을
   사용할 수 있도록, backfill_klines_from_rest(...) / preload_klines(...)
   가 1m 주기에도 동일 포맷(ts,o,h,l,c,v)으로 동작함을 전제로 문서 정리.
2) WS 수신/버퍼/헬스 체크 로직은 멀티 타임프레임 공용 구조를 유지하며,
   B안 적용 시에도 별도 코드 수정 없이 그대로 사용한다.

2025-11-19 변경 사항 (데이터 헬스 & 멀티 TF 설명 정리)
-----------------------------------------------------
1) 필수 타임프레임/버퍼 길이/최대 지연 설정을 기반으로
   get_health_snapshot / is_data_healthy 헬퍼를 추가했다.
   - settings_ws.ws_required_tfs: 필수 타임프레임 목록
     (미지정 시 ws_subscribe_tfs 또는 기본 타임프레임 사용)
   - settings_ws.ws_min_kline_buffer: TF 공통 최소 버퍼 길이 (기본 120)
   - settings_ws.ws_max_kline_delay_sec: TF 공통 최대 허용 지연(sec, 기본 600)
   - settings_ws.ws_orderbook_max_delay_sec: 오더북 최대 허용 지연(sec, 기본 10)
2) 오더북 버퍼에 bestBid / bestAsk / spreadPct(%) 를 함께 저장해
   유동성 상태를 상위 레이어에서 바로 확인할 수 있게 했다.
3) 상단 설명에서 1m/5m/15m 언급을 제거하고, BingX가 지원하는
   멀티 타임프레임 구조로 정리했다.
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

# BingX K라인 기본 타임프레임 세트 (분/시간/일/주/월)
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
WS_INTERVALS: List[str] = getattr(SET, "ws_subscribe_tfs", None) or DEFAULT_INTERVALS

# 데이터 헬스 체크용 필수 타임프레임 / 지연/버퍼 기준값
REQUIRED_INTERVALS: List[str] = ["1m", "5m", "15m"]
KLINE_MIN_BUFFER: int = int(getattr(SET, "ws_min_kline_buffer", 120))
KLINE_MAX_DELAY_SEC: float = float(getattr(SET, "ws_max_kline_delay_sec", 600.0))  # 기본 10분
ORDERBOOK_MAX_DELAY_SEC: float = float(getattr(SET, "ws_orderbook_max_delay_sec", 10.0))

# { (symbol, interval): [(ts, o, h, l, c, v), ...] }
_kline_buffers: Dict[Tuple[str, str], List[Tuple[int, float, float, float, float, float]]] = {}

# { (symbol, interval): last_recv_ts_ms }
_kline_last_recv_ts: Dict[Tuple[str, str], int] = {}

# { symbol: {"bids": [...], "asks": [...], "ts": ..., "exchTs": ..., "markPrice": ..., "lastPrice": ..., "bestBid": ..., "bestAsk": ..., "spreadPct": ...} }
_orderbook_buffers: Dict[str, Dict[str, Any]] = {}

# 동시성 보호용 Lock (읽기/쓰기 경쟁 방지)
_kline_lock = threading.Lock()
_orderbook_lock = threading.Lock()

MAX_KLINES = 500
RECONNECT_WAIT = 15


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
    """구독 메시지들을 구성한다 (설정된 타임프레임 + depth5)."""
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


def _compute_spread_pct(bids: List[List[float]], asks: List[List[float]]) -> Optional[float]:
    """오더북 상단 bid/ask 기준 스프레드(%)를 계산한다."""
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
    """depth payload 를 내부 버퍼에 저장한다."""
    normalized_bids = _normalize_depth_side(payload.get("bids") or payload.get("buys") or [])
    normalized_asks = _normalize_depth_side(payload.get("asks") or payload.get("sells") or [])

    now_ms = _now_ms()
    spread_pct = _compute_spread_pct(normalized_bids, normalized_asks)

    ob: Dict[str, Any] = {
        "bids": normalized_bids,
        "asks": normalized_asks,
        "ts": now_ms,  # 수신(로컬) 시각
    }

    if normalized_bids:
        ob["bestBid"] = normalized_bids[0][0]
    if normalized_asks:
        ob["bestAsk"] = normalized_asks[0][0]
    if spread_pct is not None:
        ob["spreadPct"] = spread_pct

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
    """(ts, o, h, l, c, v) 튜플 리스트를 그대로 내부 버퍼에 세팅한다.

    - run_bot_ws 부트스트랩(REST 히스토리 로딩) 단계에서만 사용하는 것을 권장한다.
    - 라이브 운영 중에는 WS 데이터만 사용하는 것이 원칙이다.
    """
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
    """BingX REST /kline 응답을 받아 버퍼에 백필한다.

    - list[list] / list[dict] 포맷 모두 지원한다.
    - 유효한 행만 골라 (ts,o,h,l,c,v) 튜플로 변환 후 preload_klines 를 호출한다.
    """
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
        result = dict(ob)
        if "bids" in result:
            result["bids"] = result.get("bids", [])[:limit]
            result["asks"] = result.get("asks", [])[:limit]
        return result


def _compute_kline_health(
    symbol: str,
    interval: str,
) -> Dict[str, Any]:
    """단일 타임프레임에 대한 버퍼 길이/지연/헬스 상태를 계산한다."""
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
    """오더북 버퍼의 최신 여부/헬스 상태를 계산한다."""
    now_ms = _now_ms()
    with _orderbook_lock:
        ob = _orderbook_buffers.get(symbol)

    result: Dict[str, Any] = {
        "symbol": symbol,
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
        result["reasons"].append(
            f"delay_sec>{ORDERBOOK_MAX_DELAY_SEC} (got={delay_sec:.1f})"
        )
        result["ok"] = False
    else:
        result["ok"] = True

    return result


def get_health_snapshot(symbol: str) -> Dict[str, Any]:
    """심볼 기준 전체 데이터 헬스 스냅샷을 반환한다.

    반환 예시:
    {
        "symbol": "BTC-USDT",
        "overall_ok": True/False,
        "klines": {
            "1m": {...},
            "5m": {...},
            ...
        },
        "orderbook": {...},
        "checked_at_ms": 1234567890,
    }
    """
    snapshot: Dict[str, Any] = {
        "symbol": symbol,
        "overall_ok": True,
        "klines": {},
        "orderbook": {},
        "checked_at_ms": _now_ms(),
    }

    overall_ok = True
    kline_map: Dict[str, Any] = {}

    for iv in REQUIRED_INTERVALS:
        k_status = _compute_kline_health(symbol, iv)
        kline_map[iv] = k_status
        if not k_status.get("ok", False):
            overall_ok = False

    ob_status = _compute_orderbook_health(symbol)
    if not ob_status.get("ok", False):
        overall_ok = False

    snapshot["klines"] = kline_map
    snapshot["orderbook"] = ob_status
    snapshot["overall_ok"] = overall_ok
    return snapshot


def is_data_healthy(symbol: str) -> bool:
    """심볼 기준 WS 데이터 헬스가 양호한지 여부만 간단히 반환한다."""
    snap = get_health_snapshot(symbol)
    return bool(snap.get("overall_ok", False))


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
