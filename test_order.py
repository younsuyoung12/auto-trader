# test_order.py
import os, time, hmac, hashlib, requests

BASE = "https://open-api.bingx.com"

API_KEY = os.getenv("BINGX_API_KEY", "")
API_SECRET = os.getenv("BINGX_API_SECRET", "")

def ts_ms():
    return int(time.time() * 1000)

def sign_query(params: dict) -> str:
    # 1) 키를 정렬하고
    items = sorted(params.items(), key=lambda x: x[0])
    qs = "&".join(f"{k}={v}" for k, v in items)
    # 2) 서명
    sig = hmac.new(API_SECRET.encode("utf-8"), qs.encode("utf-8"), hashlib.sha256).hexdigest()
    return qs + f"&signature={sig}"

def main():
    if not API_KEY or not API_SECRET:
        print("API 키/시크릿을 환경변수로 먼저 넣으세요.")
        return

    # ★ 여기는 네가 채울 자리
    symbol = "BTC-USDT"
    side = "BUY"
    position_side = "LONG"
    order_type = "LIMIT"       # 문서 예제처럼 LIMIT 로
    price = "101000"           # 대충 현재가 근처 숫자 하나 넣어도 됨
    quantity = "0.001"         # 너무 크지 않게

    params = {
        "symbol": symbol,
        "side": side,
        "positionSide": position_side,
        "type": order_type,
        "price": price,
        "quantity": quantity,
        "timestamp": str(ts_ms()),
    }

    qs_signed = sign_query(params)
    url = f"{BASE}/openApi/swap/v2/trade/order?{qs_signed}"

    headers = {
        "X-BX-APIKEY": API_KEY,
    }

    print("REQUEST URL:", url)
    resp = requests.post(url, headers=headers, timeout=10)
    print("status:", resp.status_code)
    print(resp.text)

if __name__ == "__main__":
    main()
