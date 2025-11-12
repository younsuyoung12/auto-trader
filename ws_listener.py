# ws_listener.py
"""
BingX 웹소켓 캔들 수신 리스너 (swap-market 전용)
- settings_ws.py 에서 ws_enabled, symbol, ws_subscribe_tfs 를 읽어온다.
- 실제 BingX 포맷(reqType=sub, dataType="BTC-USDT@kline_1m")에 맞춰 구독한다.
- 메시지는 gzip 으로 오므로 반드시 풀어준다.
- Ping 이 오면 Pong 으로 응답한다.
- 1m / 5m / 15m 등 여러 타임프레임을 한 번에 SUBSCRIBE 한다.
- 들어온 캔들을 LATEST_CANDLES 에 타임프레임별로 저장한다.
- 끊기면 5초 후 재접속한다.
"""

from __future__ import annotations

import json
import threading
import time
import gzip
import io
from typing import Any, Dict, Optional

import websocket  # pip install websocket-client
from telelog import log
from settings_ws import load_settings

SET = load_settings()

# 타임프레임별 마지막 캔들 저장소
LATEST_CANDLES: Dict[str, Any] = {
    "1m": None,
    "5m": None,
    "15m": None,
    "raw": None,  # 디버그용: 마지막 원본
}

# BingX 문서 기준 기본 WS 엔드포인트
WS_URL_DEFAULT = "wss://open-api-swap.bingx.com/swap-market"


def _to_ws_symbol(sym: str) -> str:
    """
    BingX 예제는 "BTC-USDT@kline_1m" 처럼 대문자+하이픈을 그대로 쓴다.
    """
    return sym.upper()


def _build_sub_msgs() -> list[dict[str, Any]]:
    """
    settings_ws.ws_subscribe_tfs 를 보고 각각의 구독 메시지를 만든다.
    BingX 포맷: {"id": "...", "reqType": "sub", "dataType": "BTC-USDT@kline_1m"}
    """
    tfs = getattr(SET, "ws_subscribe_tfs", None) or ["1m", "5m", "15m"]
    ws_sym = _to_ws_symbol(SET.symbol)
    msgs: list[dict[str, Any]] = []
    for tf in tfs:
        msgs.append(
            {
                "id": f"sub-{ws_sym}-{tf}",
                "reqType": "sub",
                "dataType": f"{ws_sym}@kline_{tf}",
            }
        )
    return msgs


def _decode_ws_message(raw: bytes | str) -> Any:
    """
    BingX 예제처럼 gzip 으로 오면 풀고, 아니면 그대로 json 로드
    """
    # websocket-client 가 bytes 로 줄 수도 있고 str 로 줄 수도 있음
    if isinstance(raw, str):
        # str 이고 순수 Ping 이면 바로 돌려보낸다
        return raw

    # bytes 면 gzip 으로 시도
    try:
        decompressed = gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
        txt = decompressed.decode("utf-8")
        return json.loads(txt)
    except Exception:
        # 압축 안 돼 있으면 그냥 텍스트로
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return None


def _extract_tf_from_datatype(data_type: str) -> Optional[str]:
    """
    "BTC-USDT@kline_1m" → "1m"
    """
    if "@kline_" in data_type:
        tf = data_type.split("@kline_")[-1]
        if tf in ("1m", "5m", "15m"):
            return tf
    return None


def _on_message(ws: websocket.WebSocketApp, message: bytes | str) -> None:
    decoded = _decode_ws_message(message)

    # Ping 처리 (문서 예제대로 Ping/Pong)
    if decoded == "Ping":
        ws.send("Pong")
        return

    if not isinstance(decoded, dict):
        # 디버그만 남김
        LATEST_CANDLES["raw"] = decoded
        return

    LATEST_CANDLES["raw"] = decoded

    data_type = decoded.get("dataType")
    data = decoded.get("data")

    if not data_type:
        return

    tf = _extract_tf_from_datatype(data_type)
    if tf is None:
        return

    # kline 본체는 data 에 바로 들어있다고 가정
    # 예: {"t":..., "o": "...", "h": "...", ...}
    LATEST_CANDLES[tf] = data
    if getattr(SET, "ws_log_enabled", True):
        log(f"[WS] {SET.symbol} {tf} candle updated")


def _on_error(ws: websocket.WebSocketApp, error: Any) -> None:
    log(f"[WS] error: {error}")


def _on_close(ws: websocket.WebSocketApp, close_status_code: Any, close_msg: Any) -> None:
    log(f"[WS] closed: {close_status_code} {close_msg}")


def _on_open(ws: websocket.WebSocketApp) -> None:
    log("[WS] opened")
    msgs = _build_sub_msgs()
    for m in msgs:
        ws.send(json.dumps(m))
    log(f"[WS] subscribed: {[m['dataType'] for m in msgs]}")


def start_ws_listener() -> Dict[str, Any]:
    """
    run_bot_ws 같은 데서 한 번만 호출해두면 백그라운드에서 계속 캔들 받아서
    LATEST_CANDLES 에 쌓아둔다.
    ws_enabled 가 False 면 아무 것도 안 한다.
    """
    if not getattr(SET, "ws_enabled", True):
        log("[WS] ws_enabled=0 → ws_listener 안 띄움")
        return LATEST_CANDLES

    # settings_ws 에 ws_base 가 따로 있으면 그걸 쓰고, 없으면 공식 swap-market
    ws_url = getattr(SET, "ws_base", "") or WS_URL_DEFAULT

    # http → ws 방어
    if ws_url.startswith("http://"):
        ws_url = ws_url.replace("http://", "ws://", 1)
    elif ws_url.startswith("https://"):
        ws_url = ws_url.replace("https://", "wss://", 1)

    def _run() -> None:
        while True:
            try:
                ws = websocket.WebSocketApp(
                    ws_url,
                    on_open=_on_open,
                    on_message=_on_message,
                    on_error=_on_error,
                    on_close=_on_close,
                )
                # 빙엑스도 주기적으로 ping 날리니까 interval 줘서 유지
                ws.run_forever(ping_interval=25, ping_timeout=10)
            except Exception as e:
                log(f"[WS] run_forever error: {e}")

            log("[WS] disconnected, reconnect in 5s…")
            time.sleep(5)

    th = threading.Thread(target=_run, daemon=True, name="bingx-ws-candles")
    th.start()
    return LATEST_CANDLES


if __name__ == "__main__":
    # 단독 테스트용
    start_ws_listener()
    while True:
        time.sleep(5)
        log(f"[WS][DEBUG] latest 1m = {LATEST_CANDLES.get('1m')}")
