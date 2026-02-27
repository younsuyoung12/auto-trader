"""
ws_store_worker.py
====================================================
BingX Auto Trader - WebSocket 버퍼 → Postgres 저장 워커.

역할
----------------------------------------------------
- market_data_ws.get_klines_with_volume / get_orderbook 로부터
  1m·5m·15m 캔들 + depth5 오더북을 가져와
  market_data_store.save_candle_from_ws / save_orderbook_from_ws 를 통해
  bt_candles / bt_orderbook_snapshots 테이블에 INSERT 한다.
- 실시간 매매(run_bot_ws)는 여전히 메모리 버퍼만 보고,
  이 워커는 분석/대시보드/백테스트용 로그 데이터 적재만 담당한다.

2025-11-14 최초 작성 / 중요사항
----------------------------------------------------
1) 중복 INSERT 완화
   - 프로세스가 살아 있는 동안 symbol+interval 별 마지막 ts_ms 와
     오더북 마지막 ts_ms 를 기억해서, 그 이후 데이터만 INSERT 한다.
2) 안전한 동작
   - DB/저장 오류는 market_data_store 내부에서 처리되며,
     여기서는 예외가 발생해도 잡아서 로그만 남기고 루프를 계속 돈다.
3) 구동 방법 (run_bot_ws 예시)
   - start_ws_loop(SET.symbol) 이후에 다음과 같이 호출:

       from ws_store_worker import start_ws_store_worker
       ...
       if getattr(SET, "ws_store_enabled", True):
           start_ws_store_worker(symbol=SET.symbol, interval_sec=5)

"""

from __future__ import annotations

import time
from typing import Dict, List, Optional, Sequence, Tuple

from settings import load_settings
from infra.telelog import log
from infra.market_data_ws import get_klines_with_volume, get_orderbook
from infra.market_data_store import save_candle_from_ws, save_orderbook_from_ws

# 설정 로드 (심볼, 구독 타임프레임 등)
SET = load_settings()

# settings 에 ws_subscribe_tfs 가 있으면 그대로 사용, 없으면 기본 1m/5m/15m
DEFAULT_TFS: List[str] = list(
    getattr(SET, "ws_subscribe_tfs", ["1m", "5m", "15m"])
)

# 프로세스 생존 중 마지막으로 저장한 캔들/오더북 timestamp(ms)
_last_candle_ts_ms: Dict[Tuple[str, str], int] = {}
_last_orderbook_ts_ms: Dict[str, int] = {}


# ─────────────────────────────
# 내부 유틸: 캔들 적재
# ─────────────────────────────

def _store_candles_for_symbol(
    symbol: str,
    intervals: Sequence[str],
) -> None:
    """WS 버퍼에서 심볼/타임프레임별 캔들을 가져와
    아직 저장하지 않은 ts_ms 이후의 것만 bt_candles 에 적재한다.
    """

    for tf in intervals:
        key = (symbol, tf)
        last_ts_ms: int = _last_candle_ts_ms.get(key, 0)

        # (ts_ms, o, h, l, c, v) 튜플 리스트
        rows = get_klines_with_volume(symbol, tf, limit=300)
        if not rows:
            continue

        # 새로 들어온 캔들만 필터링하고 과거→현재 순서로 정렬
        new_rows = [r for r in rows if r[0] > last_ts_ms]
        if not new_rows:
            continue

        new_rows.sort(key=lambda r: r[0])

        for ts_ms, o, h, l, c, v in new_rows:
            try:
                save_candle_from_ws(
                    symbol=symbol,
                    interval=tf,
                    ts_ms=int(ts_ms),
                    open_=float(o),
                    high=float(h),
                    low=float(l),
                    close=float(c),
                    volume=float(v),
                    quote_volume=None,  # WS 버퍼에는 quote_volume 이 없으므로 NULL 로 저장
                    source="ws",
                )
                _last_candle_ts_ms[key] = int(ts_ms)
            except Exception as e:
                # 개별 캔들 저장 실패는 로그만 남기고 계속 진행
                log(f"[WS-STORE] candle save error ({symbol} {tf} ts={ts_ms}): {e}")


# ─────────────────────────────
# 내부 유틸: 오더북 적재
# ─────────────────────────────

def _store_orderbook_for_symbol(symbol: str) -> None:
    """WS 버퍼에서 심볼의 depth5 스냅샷을 가져와
    마지막으로 저장한 ts_ms 이후의 것만 bt_orderbook_snapshots 에 적재한다.
    """

    ob = get_orderbook(symbol, limit=5)
    if not ob:
        return

    # 거래소 timestamp(exchTs)가 있으면 우선 사용, 없으면 수신 ts 사용
    ts_ms = ob.get("exchTs") or ob.get("ts")
    if not isinstance(ts_ms, int):
        try:
            ts_ms = int(ts_ms)  # type: ignore[assignment]
        except Exception:
            return

    last_ts_ms = _last_orderbook_ts_ms.get(symbol)
    if last_ts_ms is not None and ts_ms <= last_ts_ms:
        # 이미 저장한 스냅샷과 같거나 더 과거인 경우
        return

    bids = ob.get("bids") or []
    asks = ob.get("asks") or []
    if not bids or not asks:
        return

    try:
        save_orderbook_from_ws(
            symbol=symbol,
            ts_ms=int(ts_ms),
            bids=bids,
            asks=asks,
        )
        _last_orderbook_ts_ms[symbol] = int(ts_ms)
    except Exception as e:
        log(f"[WS-STORE] orderbook save error ({symbol} ts={ts_ms}): {e}")


# ─────────────────────────────
# 공개 함수: 백그라운드 워커 시작
# ─────────────────────────────

def start_ws_store_worker(
    *,
    symbol: Optional[str] = None,
    intervals: Optional[Sequence[str]] = None,
    interval_sec: int = 5,
) -> None:
    """WS 버퍼 → DB 저장 워커를 데몬 스레드로 시작한다.

    인자
    ----------------------------------------------------
    - symbol: 저장할 심볼 (없으면 settings.symbol 사용)
    - intervals: 저장할 타임프레임 리스트 (없으면 DEFAULT_TFS 사용)
    - interval_sec: 루프 주기(초). 기본 5초.
    """

    import threading

    sym = symbol or getattr(SET, "symbol", "BTC-USDT")
    tfs: Sequence[str] = intervals or DEFAULT_TFS

    def _worker() -> None:
        log(
            f"[WS-STORE] worker started for {sym}, tfs={list(tfs)}, "
            f"interval={interval_sec}s"
        )
        while True:
            try:
                _store_candles_for_symbol(sym, tfs)
                _store_orderbook_for_symbol(sym)
            except Exception as e:
                # 어떤 예외도 여기서 막아서 메인 봇이 죽지 않게 한다.
                log(f"[WS-STORE] worker loop error: {e}")
            time.sleep(interval_sec)

    th = threading.Thread(
        target=_worker,
        name=f"ws-store-{sym}",
        daemon=True,
    )
    th.start()


__all__ = [
    "start_ws_store_worker",
]
