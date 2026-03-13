import websocket
import json

ws = websocket.WebSocket()
ws.connect("wss://fstream.binance.com/ws/btcusdt@kline_1m")

for i in range(10):
    msg = ws.recv()
    print("MSG RECEIVED:", len(msg))

ws.close()