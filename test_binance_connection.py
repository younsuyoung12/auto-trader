"""
test_binance_connection.py

목적:
- Binance USDT-M Futures REST + WebSocket 직접 연결 테스트
- 우리 엔진 코드와 무관하게 네트워크/키/WS 상태 확인

테스트 항목:
1) REST klines 조회
2) WebSocket kline 수신
3) WebSocket depth5 수신
"""

import asyncio
import json
import requests
import websockets

REST_BASE = "https://fapi.binance.com"
WS_URL = "wss://fstream.binance.com/stream?streams=btcusdt@kline_1m/btcusdt@depth5@100ms"


def test_rest():
    print("=== REST TEST ===")

    url = f"{REST_BASE}/fapi/v1/klines"
    params = {
        "symbol": "BTCUSDT",
        "interval": "1m",
        "limit": 5,
    }

    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()

    data = r.json()
    print("REST OK. Received rows:", len(data))
    print("Sample:", data[0])
    print()


async def test_ws():
    print("=== WS TEST ===")

    async with websockets.connect(WS_URL) as ws:
        for _ in range(5):
            msg = await ws.recv()
            data = json.loads(msg)

            stream = data.get("stream")
            payload = data.get("data")

            if "kline" in stream:
                print("KLINE OK:",
                      payload["k"]["c"],
                      "closed:", payload["k"]["x"])

            if "depth5" in stream:
                print("DEPTH OK:",
                      "bid:", payload["b"][0][0],
                      "ask:", payload["a"][0][0])

        print("WS OK")
        print()


def main():
    test_rest()
    asyncio.run(test_ws())


if __name__ == "__main__":
    main()