# main.py
from fastapi import FastAPI
import threading
import bot  # 같은 폴더에 bot.py 있다고 가정

app = FastAPI()

# 앱이 떠 있을 때 백그라운드로 봇도 돌리고 싶으면
def run_bot():
    bot.main()

threading.Thread(target=run_bot, daemon=True).start()

@app.get("/")
def read_root():
    return {"status": "ok", "message": "bingx bot running"}
