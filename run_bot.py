"""
run_bot.py
메인 루프 파일.

역할:
- 환경설정 로드
- health/metrics HTTP 서버 (옵션)
- 거래소 포지션/열린 주문 → 내부 상태로 동기화
- 무한 루프에서 시그널 판단 → 가드체크 → 주문 → TP/SL 확인
- 텔레그램 알림 전송
- signals_logger 로 모든 이벤트(SKIP/ENTRY/CLOSE/ERROR) CSV 기록
- (추가) 백그라운드에서 일정 주기로 오늘자 CSV를 구글 드라이브에 올리고,
         너무 오래된 CSV는 Render 디스크에서 지워서 용량이 안 불어나게 함

2025-11-09 1차 수정:
- settings.py에 추가된 use_orderbook_entry_hint 값을 반영해서,
  주문 직전에 받아둔 호가(best bid / best ask)로 진입가 힌트를 만든 뒤
  open_position_with_tp_sl(...)에 넘기도록 변경
- 스프레드 체크 시 가져온 best_bid / best_ask를 이후에도 재사용하도록 변수 유지

2025-11-09 2차 수정 (안전 종료 기능 ①):
- 텔레그램에서 한글로 "종료", "봇종료", "끝나면 종료", "안전종료" 를 보내면
  SAFE_STOP_REQUESTED 플래그를 세팅하는 스레드를 추가
- 메인 루프에서 이 플래그를 보고,
  ① 열려 있는 포지션이 없으면 즉시 종료하고
  ② 열려 있는 포지션이 있으면 새 포지션은 만들지 않고 기다렸다가 비는 순간 종료

2025-11-09 3차 수정 (안전 종료 기능 ②, 재시작 차단):
- 위와 같이 종료할 때 STOP_FLAG 파일을 생성해서, 컨테이너/호스트가
  run_bot.py를 다시 실행하더라도 이 파일이 있으면 즉시 종료하도록 추가
- 다시 시작하려면 컨테이너 안에서 STOP_FLAG 파일을 삭제한 뒤 run_bot.py를 실행

2025-11-09 4차 조정 (선물 기준 PnL 명시 + 텔레그램 스레드 누락 보완):
- PnL 계산 부분에 "선물(레버리지) 체결가 기준으로 qty * (체결가 차이)"라는 걸 주석으로 명확히 했다.
  spot/현물 퍼센트가 아니라 실제 우리가 청산한 선물 체결 가격을 기준으로 수익을 본다.
- main() 안에서 start_telegram_command_thread() 호출을 빠뜨리지 않도록 다시 넣었다.

2025-11-09 5차 조정 (현물 퍼센트가 아니라 '선물 수익률'로 익절/손절):
- settings.py 에 추가한
    use_margin_based_tp_sl
    fut_tp_margin_pct
    fut_sl_margin_pct
  값을 실제 진입 시점에 반영하도록 변경.
- 로직:
  1) 원래대로 전략별 TP/SL 퍼센트 계산 (박스장이면 range값, 아니면 기본값/ATR)
  2) 그 다음에 use_margin_based_tp_sl 이 켜져 있으면
     "마진기준퍼센트 ÷ 100 ÷ 레버리지" 로 다시 덮어쓴다.
     예) 10배 레버리지, 마진기준 tp=0.5% → 실제 가격변동률 tp = 0.5/100/10 = 0.0005 (=0.05%)
  3) 너무 작아져서 거래소가 안 받을 수 있으니 min_tp_pct / min_sl_pct 로 바닥은 깔아준다.

2025-11-09 6차 조정 (텔레그램 409 Conflict 무시):
- 텔레그램 getUpdates 에서 HTTP 409 가 계속 찍히던 걸
  urllib.error.HTTPError 로 구분해서 로그만 남기고 다시 시도하도록 수정
- 다른 곳(메인 루프)에는 영향 없음

2025-11-09 7차 수정 (거래소 포지션 주기적 재동기화):
- 봇이 시작할 때 한 번만 거래소 포지션을 동기화하면,
  사람이 거래소 앱에서 수동으로 포지션을 연 경우 봇이 그걸 모른 채로 새로 진입할 수 있다.
- 이를 막기 위해 루프 안에서 일정 주기(기본 20초, settings.position_resync_sec 가 있으면 그 값)로
  fetch_open_positions() / fetch_open_orders() 를 다시 호출해 OPEN_TRADES 를 교체하도록 했다.
- 이렇게 하면 "지금 내가 이미 사둔 포지션"도 봇이 인식해서 중복 진입을 방지한다.

2025-11-09 8차 수정 (포지션 API 미지원 계정용 usedMargin 진입 가드):
- 일부 계정은 /openApi/swap/v2/trade/positions 가 100400 을 내면서 아예 안 된다.
- 이런 계정에서는 주기 동기화를 해도 항상 "열려 있는 포지션이 없습니다." 로 찍히는데,
  balance 안의 data.balance.usedMargin 은 계속 증가/유지된다.
- 루프에서 포지션이 없다고 판단한 뒤에도 잔고를 한 번 더 읽어서
  usedMargin > 0 이면 "사람이 수동으로 들고 있는 포지션"으로 보고 신규 진입을 건너뜁니다.
- 이렇게 하면 봇이 만든 포지션만 익절/손절하고, 사람이 만든 포지션 위에 얹어서 진입하지 않는다.

2025-11-10 추가 (드라이브 업로드 시 오늘 CSV 강제 생성):
- 기존에는 "로컬에 오늘자 파일이 있으면"만 드라이브에 올렸기 때문에
  자정 이후 아무 로그가 없으면 드라이브에 안 올라갔다.
- drive_sync 스레드에서 업로드하기 전에 signals_logger._ensure_today_csv() 를 호출해
  오늘자 CSV를 먼저 만들고 그걸 올리도록 변경.

2025-11-10 9차 수정 (시그널 순서/레인지 1m 확인/일일 레인지 잠금 완화):
- 시그널 판단 순서를 "엄격 TREND → (없으면) RANGE → (RANGE가 이 캔들에서 불리하면) 느슨 TREND → SKIP" 으로 변경
- RANGE 시그널도 SET.enable_1m_confirm 가 켜져 있으면 1분봉으로 방향을 다시 확인
- RANGE 손절 일일 한도 초과 시 하루 전체 비활성화하던 줄을 주석 처리해 연구·관찰 상황에서도 다음 캔들에서 다시 기회를 보도록 변경

2025-11-10 10차 수정 (3m 거래량 가드 + 레인지/대체추세 로그 보강 + 추세 실패 이유 CSV 기록 + balance detail 에러 CSV 기록):
- 신호가 선택된 뒤, 실제 진입 직전에 3m 캔들의 거래량이 최근 20개 평균의 min_entry_volume_ratio (기본 0.3) 미만이면 그 캔들만 진입을 스킵
- range_blocked_today 분기에서 "대체 느슨 추세를 시도했는지"까지 로그로 남기도록 보강
- 엄격 추세가 실패했을 때 (3m 자체 없음 / 15m 방향 없음 / 3m-15m 불일치) 각각을 CSV에 남겨서 왜 RANGE로 내려갔는지 보이게 함
- balance detail 호출 실패 시에도 CSV에 남겨서 원인을 추적 가능하게 함
"""

from __future__ import annotations

import os
import datetime
import signal
import threading
import time
import json  # 텔레그램 getUpdates 응답 파싱용
import urllib.request  # 텔레그램 폴링용 HTTP 요청
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, List, Optional, Tuple

# 프로젝트 내부 모듈
from settings import load_settings, KST
from telelog import log, send_tg, send_skip_tg
from exchange_api import (
    get_available_usdt,
    fetch_open_positions,
    fetch_open_orders,
    set_leverage_and_mode,
    get_balance_detail,  # 8차: 수동 포지션 감지용
)
from market_data import get_klines, get_orderbook
from indicators import calc_atr
from strategies_trend import (
    decide_signal_3m_trend,
    decide_trend_15m,
    confirm_1m_direction,
)
from strategies_range import (
    decide_signal_range,
    should_block_range_today,
)
from trader import (
    Trade,
    TraderState,
    open_position_with_tp_sl,
    check_closes,
)
from drive_uploader import upload_to_drive
from signals_logger import log_signal, _ensure_today_csv  # ← 오늘 파일 강제 생성용 추가


# ─────────────────────────────
# 1. 설정/전역 상태
# ─────────────────────────────

# .env → settings.py → 여기
SET = load_settings()

# 프로세스 시작 시각
START_TS: float = time.time()

# 메인 루프 on/off
RUNNING: bool = True

# SIGTERM 으로 끝났는지
TERMINATED_BY_SIGNAL: bool = False

# 간단 메트릭
METRICS: Dict[str, Any] = {
    "start_ts": START_TS,
    "last_loop_ts": START_TS,
    "open_trades": 0,
    "consec_losses": 0,
    "kline_failures": 0,
}

# 현재 열려 있다고 알고 있는 포지션
OPEN_TRADES: List[Trade] = []

# TP/SL 재설정 실패 카운트 유지용
TRADER_STATE: TraderState = TraderState()

# 최근 청산 시각
LAST_CLOSE_TS: float = 0.0

# 연속 손실
CONSEC_LOSSES: int = 0

# 캔들 조회 실패 연속 카운트
CONSEC_KLINE_FAILS: int = 0

# 박스장 손절 횟수 / 비활성화 여부
RANGE_DAILY_SL: int = 0
RANGE_DISABLED_TODAY: bool = False  # 연구 모드에서는 하루잠금 안 걸도록 유지

# 전략별 최근 청산 시각
LAST_CLOSE_TS_TREND: float = 0.0
LAST_CLOSE_TS_RANGE: float = 0.0

# 같은 3m 캔들 재진입 막는 타임스탬프
LAST_SIGNAL_TS_3M: int = 0

# 텔레그램으로 "종료" 명령이 들어왔는지 체크하는 플래그
SAFE_STOP_REQUESTED: bool = False

# 텔레그램 getUpdates 오프셋
TG_UPDATE_OFFSET: int = 0

# 거래소 포지션 재동기화 시각
LAST_EXCHANGE_SYNC_TS: float = 0.0


# ─────────────────────────────
# STOP_FLAG 헬퍼
# ─────────────────────────────
def _write_stop_flag() -> None:
    """재시작 시 바로 종료하도록 STOP_FLAG 파일을 만든다."""
    try:
        with open("STOP_FLAG", "w", encoding="utf-8") as f:
            f.write("stop\n")
    except OSError as e:
        log(f"[STOP_FLAG] write failed: {e}")


# ─────────────────────────────
# 2. health / metrics HTTP 서버
# ─────────────────────────────
class _HealthHandler(BaseHTTPRequestHandler):
    """Render 에서 상태 확인할 때 쓰는 간단 서버"""

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/healthz":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")
            return

        if self.path == "/metrics":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            uptime = time.time() - METRICS["start_ts"]
            out = [
                f"bot_uptime_seconds {uptime}",
                f"bot_open_trades {METRICS['open_trades']}",
                f"bot_consec_losses {METRICS['consec_losses']}",
                f"bot_kline_failures {METRICS['kline_failures']}",
            ]
            self.wfile.write("\n".join(out).encode())
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, *_: Any) -> None:
        # 기본 HTTP 로그는 감춤
        return


def start_health_server() -> None:
    """환경변수에 포트가 있으면 백그라운드로 health 서버 띄움"""
    if SET.health_port <= 0:
        return

    def _run() -> None:
        httpd = HTTPServer(("0.0.0.0", SET.health_port), _HealthHandler)
        log(f"[HEALTH] server started on 0.0.0.0:{SET.health_port}")
        httpd.serve_forever()

    th = threading.Thread(target=_run, daemon=True)
    th.start()


# ─────────────────────────────
# 3. 드라이브 동기화 백그라운드 스레드
# ─────────────────────────────
def start_drive_sync_thread() -> None:
    """
    (수정됨)
    - 5분마다 '오늘자' CSV를 먼저 로컬에 생성하고
    - 생성된 그 파일을 구글 드라이브에 올린다.
    - 3일 지난 CSV는 로컬(Render 디스크)에서 지워서 용량을 유지한다.
    이렇게 하면 자정 이후에 신호가 한 번도 안 와도 드라이브에 날짜별 파일이 생긴다.
    """
    SYNC_INTERVAL_SEC = 300  # 5분
    KEEP_DAYS = 3  # 3일 지난 건 삭제

    def _worker():
        while True:
            try:
                # 1) 오늘자 CSV가 없으면 여기서 먼저 만든다
                local_path = _ensure_today_csv()
                day_str = os.path.basename(local_path).replace("signals-", "").replace(".csv", "")

                # 2) 드라이브로 업로드 (없으면 create, 있으면 update)
                upload_to_drive(local_path, f"signals-{day_str}.csv")
                log("[DRIVE_SYNC] uploaded today's signals csv")

                # 3) 오래된 CSV 정리
                local_dir = os.path.dirname(local_path)
                if os.path.exists(local_dir):
                    now_ts = time.time()
                    for fname in os.listdir(local_dir):
                        if not (fname.startswith("signals-") and fname.endswith(".csv")):
                            continue
                        fpath = os.path.join(local_dir, fname)
                        mtime = os.path.getmtime(fpath)
                        if now_ts - mtime > KEEP_DAYS * 86400:
                            try:
                                os.remove(fpath)
                                log(f"[DRIVE_SYNC] old csv removed: {fname}")
                            except OSError:
                                pass

            except Exception as e:
                # 업로드 실패해도 봇은 계속 돌아야 함
                log(f"[DRIVE_SYNC] error: {e}")

            time.sleep(SYNC_INTERVAL_SEC)

    th = threading.Thread(target=_worker, daemon=True)
    th.start()


# ─────────────────────────────
# 3-1. 텔레그램 명령 수신 스레드
# ─────────────────────────────
def start_telegram_command_thread() -> None:
    """
    텔레그램에서 '종료' 같은 한글 명령을 받아와 SAFE_STOP_REQUESTED 를 켜는 스레드
    409 Conflict (다른 곳에서 이미 getUpdates 중) 이 떠도 메인 봇은 계속 돌아야 하므로
    그 경우는 그냥 로그만 찍고 재시도한다.
    """
    if not SET.telegram_bot_token or not SET.telegram_chat_id:
        return  # 텔레그램 설정이 없으면 안 돌린다

    def _worker():
        import urllib.error  # 409 구분하려고 내부에서 import

        global SAFE_STOP_REQUESTED, TG_UPDATE_OFFSET
        base_url = f"https://api.telegram.org/bot{SET.telegram_bot_token}/getUpdates"
        chat_id_str = str(SET.telegram_chat_id)
        while True:
            try:
                url = f"{base_url}?timeout=30"
                if TG_UPDATE_OFFSET:
                    url += f"&offset={TG_UPDATE_OFFSET}"
                with urllib.request.urlopen(url, timeout=35) as resp:
                    data = json.loads(resp.read().decode("utf-8"))

                for upd in data.get("result", []):
                    # 다음 요청부터는 이 이후로만
                    TG_UPDATE_OFFSET = upd["update_id"] + 1
                    msg = upd.get("message") or upd.get("channel_post")
                    if not msg:
                        continue
                    if str(msg["chat"]["id"]) != chat_id_str:
                        continue
                    text = (msg.get("text") or "").strip()
                    # 종료 명령어 집합
                    if text in ("종료", "봇종료", "끝나면 종료", "안전종료"):
                        SAFE_STOP_REQUESTED = True
                        send_tg("🛑 종료 명령 수신: 포지션 정리 후 종료합니다.")
            except urllib.error.HTTPError as e:
                # 여기서 409 Conflict 가 나도 메인 봇은 계속 돌아야 한다
                if e.code == 409:
                    log("[TG CMD] 409 Conflict (다른 폴링/웹훅이 있는 것 같음) → 무시 후 재시도")
                else:
                    log(f"[TG CMD] http error: {e}")
                time.sleep(5)
            except Exception as e:
                log(f"[TG CMD] error: {e}")
                time.sleep(5)

    th = threading.Thread(target=_worker, daemon=True)
    th.start()


# ─────────────────────────────
# 4. 시간대(UTC) 필터
# ─────────────────────────────
def _parse_sessions(spec: str) -> List[Tuple[int, int]]:
    """예: '0-3,8-12' → [(0,3),(8,12)]"""
    out: List[Tuple[int, int]] = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            s, e = part.split("-", 1)
            out.append((int(s), int(e)))
        else:
            h = int(part)
            out.append((h, h))
    return out


SESSIONS: List[Tuple[int, int]] = _parse_sessions(SET.trading_sessions_utc)


def in_trading_session_utc() -> bool:
    """현재 UTC 시각이 우리가 설정한 거래 가능 시간대인지"""
    now_utc = datetime.datetime.now(datetime.UTC)
    hour = now_utc.hour
    for start_h, end_h in SESSIONS:
        if start_h <= hour <= end_h:
            return True
    return False


# ─────────────────────────────
# 5. SIGTERM 핸들러
# ─────────────────────────────
def _sigterm(*_: Any) -> None:
    """Render 가 SIGTERM 보냈을 때 자연스럽게 종료하려고"""
    global RUNNING, TERMINATED_BY_SIGNAL
    TERMINATED_BY_SIGNAL = True
    RUNNING = False


signal.signal(signal.SIGTERM, _sigterm)


# ─────────────────────────────
# 6. 거래소 → 내부 포지션 동기화
# ─────────────────────────────
def sync_open_trades_from_exchange(symbol: str, replace: bool = True) -> None:
    """
    실제 거래소에 열려 있는 포지션을 우리 OPEN_TRADES 에 반영해서 중복 진입을 막는다.

    - replace=True 이면 기존 리스트를 비우고 거래소 상태로 덮어쓴다.
      (주기적 재동기화에서 이 모드로 호출)
    - replace=False 이면 현재 리스트 뒤에 붙인다.
      (초기 로딩에서만 쓰고 싶으면 False 로 쓴다)
    """
    global OPEN_TRADES

    positions = fetch_open_positions(symbol)
    orders = fetch_open_orders(symbol)

    if replace:
        OPEN_TRADES = []

    if not positions:
        log("[SYNC] 열려 있는 포지션이 없습니다.")
        return

    for pos in positions:
        qty = float(
            pos.get("positionAmt")
            or pos.get("quantity")
            or pos.get("size")
            or 0.0
        )
        if qty == 0.0:
            continue

        side_raw = (pos.get("positionSide") or pos.get("side") or "").upper()
        side_open = "BUY" if side_raw in ("LONG", "BUY") else "SELL"
        entry_price = float(pos.get("entryPrice") or pos.get("avgPrice") or 0.0)

        tp_id: Optional[str] = None
        sl_id: Optional[str] = None
        tp_price: Optional[float] = None
        sl_price: Optional[float] = None

        # 열려 있는 주문들 중에서 TP/SL 찾아서 붙여줌
        for o in orders:
            o_type = (o.get("type") or o.get("orderType") or "").upper()
            oid = o.get("orderId") or o.get("id")
            trig = o.get("triggerPrice") or o.get("stopPrice")
            if "TAKE" in o_type:
                tp_id = oid
                tp_price = float(trig) if trig else None
            if "STOP" in o_type:
                sl_id = oid
                sl_price = float(trig) if trig else None

        OPEN_TRADES.append(
            Trade(
                symbol=symbol,
                side=side_open,
                qty=qty,
                entry=entry_price,
                entry_order_id=None,
                tp_order_id=tp_id,
                sl_order_id=sl_id,
                tp_price=tp_price,
                sl_price=sl_price,
                source="SYNC",
            )
        )

    if OPEN_TRADES:
        send_tg(f"🔁 거래소 포지션 {len(OPEN_TRADES)}건을 동기화했습니다. (중복 진입 방지)")
    else:
        log("[SYNC] 포지션을 못 가져와서 동기화할 게 없습니다.")


# ─────────────────────────────
# 7. 메인 루프
# ─────────────────────────────
def main() -> None:
    global RUNNING
    global OPEN_TRADES, LAST_CLOSE_TS, CONSEC_LOSSES, CONSEC_KLINE_FAILS
    global RANGE_DAILY_SL, RANGE_DISABLED_TODAY
    global LAST_CLOSE_TS_TREND, LAST_CLOSE_TS_RANGE
    global LAST_SIGNAL_TS_3M
    global SAFE_STOP_REQUESTED
    global LAST_EXCHANGE_SYNC_TS

    # 시작 시 STOP_FLAG 가 있으면 바로 종료해서 재시작을 막는다.
    if os.path.exists("STOP_FLAG"):
        log("STOP_FLAG detected on startup. exiting without start.")
        send_tg("⛔ 이전에 종료 명령이 있어 재시작하지 않습니다. (STOP_FLAG 지우고 다시 실행)")
        return

    # 필수 키 체크
    if not SET.api_key or not SET.api_secret:
        msg = "❗ BINGX_API_KEY 또는 BINGX_API_SECRET 이 비어있습니다. 환경변수를 먼저 설정하세요."
        log(msg)
        send_tg(msg)
        return

    # 시작 로그
    log(
        f"CONFIG: ENABLE_TREND={SET.enable_trend}, "
        f"ENABLE_RANGE={SET.enable_range}, "
        f"ENABLE_1M_CONFIRM={SET.enable_1m_confirm}"
    )
    send_tg("✅ [봇 시작] BingX 선물 자동매매 시작합니다.")

    # 레버리지/마진 설정 (실패해도 멈추지 않음)
    try:
        set_leverage_and_mode(SET.symbol, SET.leverage, SET.isolated)
    except Exception as e:
        log(f"[WARN] 레버리지/마진 설정 실패: {e}")

    # health 서버, 드라이브 동기화 스레드, 텔레그램 명령 스레드 시작
    start_health_server()
    start_drive_sync_thread()
    start_telegram_command_thread()

    # 거래소 포지션을 우리 내부에 반영 (시작 시 1회)
    sync_open_trades_from_exchange(SET.symbol, replace=True)
    LAST_EXCHANGE_SYNC_TS = time.time()

    now_kst = datetime.datetime.now(KST)
    last_report_date_kst = now_kst.strftime("%Y-%m-%d")

    daily_pnl: float = 0.0
    last_fill_check: float = 0.0
    last_balance_log: float = 0.0

    # 재동기화 주기 (settings 에 없으면 20초로)
    position_resync_sec = getattr(SET, "position_resync_sec", 20)

    while RUNNING:
        try:
            METRICS["last_loop_ts"] = time.time()
            METRICS["open_trades"] = len(OPEN_TRADES)
            METRICS["consec_losses"] = CONSEC_LOSSES
            METRICS["kline_failures"] = CONSEC_KLINE_FAILS

            now = time.time()

            # ─────────────────────────────
            # 0) 거래소 포지션 주기적 재동기화
            # ─────────────────────────────
            if now - LAST_EXCHANGE_SYNC_TS >= position_resync_sec:
                sync_open_trades_from_exchange(SET.symbol, replace=True)
                LAST_EXCHANGE_SYNC_TS = now

            # 잔고 로깅
            if now - last_balance_log >= 60:
                get_available_usdt()
                last_balance_log = now

            # KST 자정 리셋
            now_kst = datetime.datetime.now(KST)
            today_kst = now_kst.strftime("%Y-%m-%d")
            if now_kst.hour == 0 and now_kst.minute < 1:
                if last_report_date_kst != today_kst:
                    send_tg(
                        f"📊 일일 정산(KST): PnL {daily_pnl:.2f} USDT, 연속 손실 {CONSEC_LOSSES}"
                    )
                    daily_pnl = 0.0
                    CONSEC_LOSSES = 0
                    RANGE_DAILY_SL = 0
                    RANGE_DISABLED_TODAY = False
                    last_report_date_kst = today_kst

            # TP/SL 체결 확인
            if now - last_fill_check >= SET.poll_fills_sec:
                OPEN_TRADES, closed_list = check_closes(OPEN_TRADES, TRADER_STATE)
                for closed in closed_list:
                    t: Trade = closed["trade"]
                    reason: str = closed["reason"]
                    summary: Optional[Dict[str, Any]] = closed.get("summary")

                    closed_qty = summary.get("qty") if summary else t.qty
                    closed_price = summary.get("avg_price") if summary else 0.0
                    pnl = summary.get("pnl") if summary else None

                    # ───────── 여기서의 PnL 계산은 "선물 체결가 기준"이다 ─────────
                    if pnl is None or pnl == 0.0:
                        if reason == "TP":
                            if t.side == "BUY":
                                pnl = (t.tp_price - t.entry) * closed_qty
                            else:
                                pnl = (t.entry - t.tp_price) * closed_qty
                        else:  # SL or FORCE_CLOSE
                            if t.side == "BUY":
                                pnl = (t.sl_price - t.entry) * closed_qty
                            else:
                                pnl = (t.entry - t.sl_price) * closed_qty

                    daily_pnl += pnl
                    LAST_CLOSE_TS = now

                    if pnl < 0:
                        CONSEC_LOSSES += 1
                    else:
                        CONSEC_LOSSES = 0

                    if t.source == "TREND":
                        LAST_CLOSE_TS_TREND = now
                    elif t.source == "RANGE":
                        LAST_CLOSE_TS_RANGE = now

                    # 박스장 손절 누적 → 원래는 여기서 오늘 하루 끄던 줄이 있었음
                    if t.source == "RANGE" and reason == "SL" and pnl < 0:
                        RANGE_DAILY_SL += 1
                        if RANGE_DAILY_SL >= SET.range_max_daily_sl:
                            # RANGE_DISABLED_TODAY = True
                            send_tg(
                                f"🛑 [RANGE_WARN] 박스장 손절 {RANGE_DAILY_SL}회 → 다음 캔들부터도 다시 판단(하루잠금 해제 상태)"
                            )

                    send_tg(
                        f"💰 청산({reason}) {t.symbol} {t.side} 수량={closed_qty} "
                        f"가격={closed_price:.2f} PnL={pnl:.2f} USDT (금일 누적 {daily_pnl:.2f})"
                    )

                    log_signal(
                        event="CLOSE",
                        symbol=t.symbol,
                        strategy_type=t.source,
                        direction=t.side,
                        price=closed_price,
                        qty=closed_qty,
                        tp_price=t.tp_price,
                        sl_price=t.sl_price,
                        reason=reason,
                        pnl=pnl,
                        extra=f"entry_order_id={t.entry_order_id}",
                    )

                last_fill_check = now

                # TP/SL 재설정 실패로 인한 중단
                if TRADER_STATE.should_stop_bot():
                    send_tg("🛑 TP/SL 재설정이 연속으로 실패해서 봇을 중단합니다.")
                    _write_stop_flag()
                    RUNNING = False
                    break

                # 종료 명령이 있고, 포지션이 0개가 된 시점이면 종료 + STOP_FLAG
                if SAFE_STOP_REQUESTED and not OPEN_TRADES:
                    send_tg("🛑 종료 명령에 따라 포지션이 없어 즉시 종료합니다.")
                    _write_stop_flag()
                    RUNNING = False
                    break

            # 포지션 살아 있으면 신규 진입 안 함
            if OPEN_TRADES:
                time.sleep(1)
                continue

            # ─────────────────────────────
            # 8차: 포지션 API가 안 되는 계정용 수동 포지션 가드
            # ─────────────────────────────
            try:
                bal_detail = get_balance_detail()
                used_margin = float(bal_detail.get("usedMargin") or 0.0)
            except Exception as e:
                log(f"[MANUAL_GUARD] balance detail error: {e}")
                # CSV 에도 남겨서 왜 수동포지션 체크를 못 했는지 보이게 한다
                log_signal(
                    event="SKIP",
                    symbol=SET.symbol,
                    strategy_type="UNKNOWN",
                    reason="manual_guard_balance_error",
                    extra=str(e),
                )
                used_margin = 0.0

            if used_margin > 0:
                send_skip_tg("[SKIP] manual_position_detected: usedMargin>0 → 신규 진입 건너뜀")
                time.sleep(2)
                continue

            # 포지션이 없는데 종료 요청 플래그만 켜져 있으면 여기서 종료 + STOP_FLAG
            if SAFE_STOP_REQUESTED:
                send_tg("🛑 종료 명령에 따라 새 진입 없이 종료합니다.")
                _write_stop_flag()
                RUNNING = False
                break

            # 연속 손실 3회면 휴식
            if CONSEC_LOSSES >= 3:
                send_tg("⛔ 연속 3회 손실 → 1시간 휴식")
                time.sleep(SET.cooldown_after_3loss)
                CONSEC_LOSSES = 0
                continue

            # 청산 직후 쿨다운
            if LAST_CLOSE_TS > 0 and (time.time() - LAST_CLOSE_TS) < SET.cooldown_after_close:
                time.sleep(1)
                continue

            # 거래 가능 시간대 체크
            if not in_trading_session_utc():
                time.sleep(2)
                continue

            # 3분봉 데이터
            try:
                candles_3m = get_klines(SET.symbol, SET.interval, 120)
                CONSEC_KLINE_FAILS = 0
                if candles_3m:
                    log(
                        f"[DATA] 3m klines ok: symbol={SET.symbol} count={len(candles_3m)} last_close={candles_3m[-1][4]}"
                    )
            except Exception as e:
                CONSEC_KLINE_FAILS += 1
                log(f"[KLINE ERROR] {e} (fail={CONSEC_KLINE_FAILS})")
                if CONSEC_KLINE_FAILS >= SET.max_kline_fails:
                    send_tg(
                        f"⚠️ 시세 연속 {CONSEC_KLINE_FAILS}회 실패. {SET.kline_fail_sleep}s 대기."
                    )
                    time.sleep(SET.kline_fail_sleep)
                    CONSEC_KLINE_FAILS = 0
                else:
                    time.sleep(2)
                continue

            if len(candles_3m) < 50:
                time.sleep(1)
                continue

            latest_3m_ts = candles_3m[-1][0]
            now_ms = int(time.time() * 1000)
            if now_ms - latest_3m_ts > SET.max_kline_delay_sec * 1000:
                send_skip_tg("[SKIP] 3m_kline_delayed: 최근 3m 캔들이 지연되었습니다.")
                log_signal(
                    event="SKIP",
                    symbol=SET.symbol,
                    strategy_type="UNKNOWN",
                    reason="3m_kline_delayed",
                    candle_ts=latest_3m_ts,
                )
                time.sleep(1)
                continue

            if latest_3m_ts == LAST_SIGNAL_TS_3M:
                send_skip_tg("[SKIP] same_3m_candle: 이미 이 캔들에서 진입했습니다.")
                log_signal(
                    event="SKIP",
                    symbol=SET.symbol,
                    strategy_type="UNKNOWN",
                    reason="same_3m_candle",
                    candle_ts=latest_3m_ts,
                )
                time.sleep(1)
                continue

            # 잔고 확인
            avail = get_available_usdt()
            if avail <= 0:
                send_skip_tg("[BALANCE_SKIP] ⚠️ 가용 선물 잔고가 0입니다. 진입을 건너뜁니다.")
                log_signal(
                    event="SKIP",
                    symbol=SET.symbol,
                    strategy_type="UNKNOWN",
                    reason="balance_zero",
                    candle_ts=latest_3m_ts,
                    available_usdt=0.0,
                )
                time.sleep(3)
                continue

            # ─────────────────────────────
            # 시그널 판단
            # ─────────────────────────────
            chosen_signal: Optional[str] = None
            signal_source: Optional[str] = None
            trend_15m_val: Optional[str] = None

            candles_15m: Optional[List[Any]] = None

            # 1) 엄격 추세 먼저
            if SET.enable_trend:
                candles_15m = get_klines(SET.symbol, "15m", 120)
                if candles_15m:
                    log(
                        f"[DATA] 15m klines ok: symbol={SET.symbol} count={len(candles_15m)} last_close={candles_15m[-1][4]}"
                    )
                sig_3m = decide_signal_3m_trend(
                    candles_3m,
                    SET.rsi_overbought,
                    SET.rsi_oversold,
                )
                trend_15m_val = decide_trend_15m(candles_15m) if candles_15m else None

                # 엄격 추세가 왜 안 됐는지 CSV 로 남긴다 (연구용)
                if not sig_3m:
                    log_signal(
                        event="SKIP",
                        symbol=SET.symbol,
                        strategy_type="TREND",
                        reason="trend_3m_no_signal",
                        candle_ts=latest_3m_ts,
                    )
                elif not trend_15m_val:
                    log_signal(
                        event="SKIP",
                        symbol=SET.symbol,
                        strategy_type="TREND",
                        reason="trend_15m_unknown",
                        candle_ts=latest_3m_ts,
                    )
                elif sig_3m != trend_15m_val:
                    log_signal(
                        event="SKIP",
                        symbol=SET.symbol,
                        strategy_type="TREND",
                        reason="trend_3m_15m_mismatch",
                        candle_ts=latest_3m_ts,
                    )

                if sig_3m and trend_15m_val and sig_3m == trend_15m_val:
                    if (time.time() - LAST_CLOSE_TS_TREND) >= SET.cooldown_after_close_trend:
                        chosen_signal = sig_3m
                        signal_source = "TREND"
                        # 1분봉 확인이 켜져 있으면 여기서 확인
                        if SET.enable_1m_confirm:
                            candles_1m = get_klines(SET.symbol, "1m", 20)
                            if candles_1m:
                                log(
                                    f"[DATA] 1m klines ok: symbol={SET.symbol} count={len(candles_1m)} last_close={candles_1m[-1][4]}"
                                )
                            if not confirm_1m_direction(candles_1m, chosen_signal):
                                send_skip_tg("[SKIP] 1m_confirm_mismatch: 1분봉이 방향 불일치")
                                log_signal(
                                    event="SKIP",
                                    symbol=SET.symbol,
                                    strategy_type="TREND",
                                    direction=chosen_signal,
                                    reason="1m_confirm_mismatch",
                                    candle_ts=latest_3m_ts,
                                    trend_15m=trend_15m_val,
                                )
                                chosen_signal = None
                                signal_source = None
                    else:
                        send_skip_tg("[SKIP] trend_cooldown: 직전 TREND 포지션 대기중")
                        log_signal(
                            event="SKIP",
                            symbol=SET.symbol,
                            strategy_type="TREND",
                            direction=sig_3m,
                            reason="trend_cooldown",
                            candle_ts=latest_3m_ts,
                            trend_15m=trend_15m_val,
                        )

            # 2) 엄격 추세가 안 나왔을 때만 박스장 시도
            if (not chosen_signal) and SET.enable_range and (not RANGE_DISABLED_TODAY):
                if not candles_15m:
                    candles_15m = get_klines(SET.symbol, "15m", 120)
                    if candles_15m:
                        log(
                            f"[DATA] 15m klines ok: symbol={SET.symbol} count={len(candles_15m)} last_close={candles_15m[-1][4]}"
                        )

                # 직전 RANGE 청산 쿨다운
                if (time.time() - LAST_CLOSE_TS_RANGE) >= SET.cooldown_after_close_range:
                    # 이 캔들에서만 박스가 불리한지 확인
                    blocked_now = should_block_range_today(candles_3m, candles_15m)
                    if not blocked_now:
                        r_sig = decide_signal_range(candles_3m)
                        if r_sig:
                            # 박스장도 1분봉 확인을 옵션으로 추가
                            ok_range = True
                            if SET.enable_1m_confirm:
                                candles_1m = get_klines(SET.symbol, "1m", 20)
                                if candles_1m:
                                    log(
                                        f"[DATA] 1m klines ok: symbol={SET.symbol} count={len(candles_1m)} last_close={candles_1m[-1][4]}"
                                    )
                                if not confirm_1m_direction(candles_1m, r_sig):
                                    ok_range = False
                            if ok_range:
                                chosen_signal = r_sig
                                signal_source = "RANGE"
                            else:
                                send_skip_tg("[SKIP] 1m_confirm_mismatch_range")
                                log_signal(
                                    event="SKIP",
                                    symbol=SET.symbol,
                                    strategy_type="RANGE",
                                    direction=r_sig,
                                    reason="1m_confirm_mismatch_range",
                                    candle_ts=latest_3m_ts,
                                )
                    else:
                        # 3) 박스가 이 캔들에서는 불리하면 → 느슨한 추세로 한 번 더 본다
                        log("[INFO] range blocked for this candle, trying loose trend fallback")
                        loose_sig = decide_signal_3m_trend(
                            candles_3m,
                            SET.rsi_overbought + 5,  # 기준 조금 완화
                            SET.rsi_oversold - 5,
                        )
                        loose_trend_15m = decide_trend_15m(candles_15m) if candles_15m else None

                        if loose_sig and loose_trend_15m and loose_sig == loose_trend_15m:
                            chosen_signal = loose_sig
                            signal_source = "TREND"  # fallback 이지만 TREND로 표기
                            # CSV에도 남겨둔다
                            log_signal(
                                event="ENTRY_SIGNAL",
                                symbol=SET.symbol,
                                strategy_type="TREND",
                                direction=loose_sig,
                                reason="range_blocked_fallback_to_loose_trend",
                                candle_ts=latest_3m_ts,
                            )
                            send_skip_tg("[INFO] range blocked → loose TREND fallback used")
                        else:
                            # 그래도 없으면 이 캔들만 SKIP
                            send_skip_tg("[SKIP] range_blocked_today: 박스장 조건 불리")
                            log_signal(
                                event="SKIP",
                                symbol=SET.symbol,
                                strategy_type="RANGE",
                                reason="range_blocked_today",
                                candle_ts=latest_3m_ts,
                            )
                else:
                    send_skip_tg("[SKIP] range_cooldown: 직전 RANGE 포지션 대기중")
                    log_signal(
                        event="SKIP",
                        symbol=SET.symbol,
                        strategy_type="RANGE",
                        reason="range_cooldown",
                        candle_ts=latest_3m_ts,
                    )

            # 위에서 세 단계 다 해봤는데도 신호가 없으면 패스
            if not chosen_signal:
                time.sleep(1)
                continue

            # ─────────────────────────────
            # 3m 거래량 가드 (신호는 나왔으나 체결은 이 캔들만 스킵)
            # ─────────────────────────────
            min_vol_ratio = getattr(SET, "min_entry_volume_ratio", 0.3)  # 없으면 30%로
            last_vol = None
            avg_vol_20 = None
            try:
                # get_klines 가 [ts, o, h, l, c, vol, ...] 구조라는 전제
                last_vol = float(candles_3m[-1][5])
                vols_20 = [float(c[5]) for c in candles_3m[-20:]]
                avg_vol_20 = sum(vols_20) / len(vols_20)
            except Exception:
                # 캔들에 볼륨 없으면 그냥 통과
                last_vol = None
                avg_vol_20 = None

            if (
                last_vol is not None
                and avg_vol_20 is not None
                and avg_vol_20 > 0
                and last_vol < avg_vol_20 * min_vol_ratio
            ):
                send_skip_tg("[SKIP] volume_too_low_for_entry")
                log_signal(
                    event="SKIP",
                    symbol=SET.symbol,
                    strategy_type=signal_source or "UNKNOWN",
                    direction=chosen_signal,
                    reason="volume_too_low_for_entry",
                    candle_ts=latest_3m_ts,
                    extra=f"last_vol={last_vol}, avg_vol_20={avg_vol_20}, ratio={min_vol_ratio}",
                )
                time.sleep(1)
                continue

            # 진입 전 가드 (가격 점프)
            last_price = candles_3m[-1][4]
            prev_price = candles_3m[-2][4]
            move_pct = abs(last_price - prev_price) / prev_price
            if move_pct > SET.max_price_jump_pct:
                send_skip_tg(
                    f"[SKIP] price_jump_guard: {move_pct:.4f} > {SET.max_price_jump_pct:.4f}"
                )
                log_signal(
                    event="SKIP",
                    symbol=SET.symbol,
                    strategy_type=signal_source or "UNKNOWN",
                    direction=chosen_signal,
                    reason="price_jump_guard",
                    candle_ts=latest_3m_ts,
                    extra=f"move_pct={move_pct:.6f}",
                )
                time.sleep(1)
                continue

            # 호가 스프레드 체크
            spread_pct_logged: Optional[float] = None
            best_bid: Optional[float] = None
            best_ask: Optional[float] = None
            orderbook = get_orderbook(SET.symbol, 5)
            if orderbook:
                bids = orderbook.get("bids") or []
                asks = orderbook.get("asks") or []
                if bids and asks:
                    try:
                        best_bid = float(
                            bids[0][0] if isinstance(bids[0], list) else bids[0].get("price")
                        )
                        best_ask = float(
                            asks[0][0] if isinstance(asks[0], list) else asks[0].get("price")
                        )
                        if best_bid > 0:
                            spread_pct = (best_ask - best_bid) / best_bid
                            spread_pct_logged = spread_pct
                            if spread_pct > SET.max_spread_pct:
                                send_skip_tg(
                                    f"[SKIP] spread_guard: {spread_pct:.5f} > {SET.max_spread_pct:.5f}"
                                )
                                log_signal(
                                    event="SKIP",
                                    symbol=SET.symbol,
                                    strategy_type=signal_source or "UNKNOWN",
                                    direction=chosen_signal,
                                    reason="spread_guard",
                                    candle_ts=latest_3m_ts,
                                    spread_pct=spread_pct,
                                )
                                time.sleep(1)
                                continue
                    except Exception as e:
                        log(f"[SPREAD PARSE ERROR] {e}")

            # ATR 기반 리스크 조절
            effective_risk_pct = SET.risk_pct
            atr_fast_val: Optional[float] = None
            atr_slow_val: Optional[float] = None
            if SET.use_atr:
                atr_fast_val = calc_atr(candles_3m, SET.atr_len)
                atr_slow_val = calc_atr(candles_3m, max(SET.atr_len * 2, SET.atr_len + 10))
                if atr_fast_val and atr_slow_val and atr_slow_val > 0:
                    if atr_fast_val > atr_slow_val * SET.atr_risk_high_mult:
                        effective_risk_pct = SET.risk_pct * SET.atr_risk_reduction
                        log(
                            f"[ATR RISK] fast ATR high → risk {SET.risk_pct} -> {effective_risk_pct}"
                        )

            # 주문 명목가 계산 (선물: 가용선물잔고 × 내 리스크 × 레버리지)
            notional = avail * effective_risk_pct * SET.leverage
            if notional < SET.min_notional_usdt:
                send_tg(f"⚠️ 계산된 주문 금액이 너무 작습니다: {notional:.2f} USDT")
                log_signal(
                    event="SKIP",
                    symbol=SET.symbol,
                    strategy_type=signal_source or "UNKNOWN",
                    direction=chosen_signal,
                    reason="notional_too_small",
                    candle_ts=latest_3m_ts,
                    available_usdt=avail,
                    notional=notional,
                )
                time.sleep(3)
                continue
            notional = min(notional, SET.max_notional_usdt)

            qty = round(notional / last_price, 6)
            side_open = "BUY" if chosen_signal == "LONG" else "SELL"

            # ─────────────────────────────
            # TP/SL 퍼센트 선택
            # ─────────────────────────────
            if signal_source == "RANGE":
                # 박스장은 원래 짧은 값
                local_tp_pct = SET.range_tp_pct
                local_sl_pct = SET.range_sl_pct
            else:
                # 추세장 기본값
                local_tp_pct = SET.tp_pct
                local_sl_pct = SET.sl_pct
                # ATR 사용 시 시장 변동성에 맞춰 자동 보정
                if SET.use_atr:
                    atr_val = calc_atr(candles_3m, SET.atr_len)
                    if atr_val and last_price > 0:
                        sl_pct_atr = (atr_val * SET.atr_sl_mult) / last_price
                        tp_pct_atr = (atr_val * SET.atr_tp_mult) / last_price
                        local_sl_pct = max(sl_pct_atr, SET.min_sl_pct)
                        local_tp_pct = max(tp_pct_atr, SET.min_tp_pct)

            # ─────────────────────────────
            # ★ 선물(마진) 기준 TP/SL 강제 변환
            # ─────────────────────────────
            if SET.use_margin_based_tp_sl:
                margin_tp_pct = SET.fut_tp_margin_pct / 100.0
                margin_sl_pct = SET.fut_sl_margin_pct / 100.0
                lev = SET.leverage if SET.leverage > 0 else 1
                local_tp_pct = margin_tp_pct / lev
                local_sl_pct = margin_sl_pct / lev
                local_tp_pct = max(local_tp_pct, SET.min_tp_pct)
                local_sl_pct = max(local_sl_pct, SET.min_sl_pct)

            strategy_kr = "추세장" if signal_source == "TREND" else "박스장"
            direction_kr = "롱" if chosen_signal == "LONG" else "숏"

            send_tg(
                f"[ENTRY][{signal_source}] 🟢 진입 시도: {SET.symbol} 전략={strategy_kr} 방향={direction_kr} "
                f"레버리지={SET.leverage}x 사용비율={effective_risk_pct*100:.0f}% "
                f"명목가≈{notional:.2f}USDT 수량={qty}"
            )

            log_signal(
                event="ENTRY_SIGNAL",
                symbol=SET.symbol,
                strategy_type=signal_source or "UNKNOWN",
                direction=chosen_signal,
                price=last_price,
                qty=qty,
                candle_ts=latest_3m_ts,
                trend_15m=trend_15m_val,
                atr_fast=atr_fast_val,
                atr_slow=atr_slow_val,
                spread_pct=spread_pct_logged,
                available_usdt=avail,
                notional=notional,
            )

            # 진입가 힌트
            entry_price_hint = last_price
            if SET.use_orderbook_entry_hint and best_bid and best_ask:
                if side_open == "BUY":
                    entry_price_hint = best_ask
                else:
                    entry_price_hint = best_bid

            trade = open_position_with_tp_sl(
                settings=SET,
                symbol=SET.symbol,
                side_open=side_open,
                qty=qty,
                entry_price_hint=entry_price_hint,
                tp_pct=local_tp_pct,
                sl_pct=local_sl_pct,
                source=signal_source or "UNKNOWN",
            )
            if trade is None:
                log_signal(
                    event="SKIP",
                    symbol=SET.symbol,
                    strategy_type=signal_source or "UNKNOWN",
                    direction=chosen_signal,
                    reason="entry_failed",
                    candle_ts=latest_3m_ts,
                )
                time.sleep(2)
                continue

            send_tg(
                f"[ENTRY][{signal_source}] 📌 예약완료: TP={trade.tp_price} / SL={trade.sl_price}"
            )

            log_signal(
                event="ENTRY_OPENED",
                symbol=trade.symbol,
                strategy_type=trade.source,
                direction=trade.side,
                price=trade.entry,
                qty=trade.qty,
                tp_price=trade.tp_price,
                sl_price=trade.sl_price,
                candle_ts=latest_3m_ts,
                available_usdt=avail,
                notional=notional,
                extra=f"entry_order_id={trade.entry_order_id}",
            )

            OPEN_TRADES.append(trade)

            LAST_SIGNAL_TS_3M = latest_3m_ts

            time.sleep(SET.cooldown_sec)

        except Exception as e:
            log(f"ERROR: {e}")
            send_tg(f"❌ 오류 발생: {e}")
            log_signal(
                event="ERROR",
                symbol=SET.symbol,
                strategy_type="UNKNOWN",
                reason=str(e),
            )
            time.sleep(2)

    uptime = time.time() - START_TS
    if (not TERMINATED_BY_SIGNAL) and (uptime >= SET.min_uptime_for_stop):
        send_tg("🛑 봇이 종료되었습니다.")
    else:
        log(f"[SKIP STOP] sig={TERMINATED_BY_SIGNAL} uptime={uptime:.2f}s")


if __name__ == "__main__":
    main()
