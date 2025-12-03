# smoke_test_hybrid_single.py
# ==========================================================
# BingX BTC-USDT WS 버전 하이브리드 스모크 테스트 (단일 파일)
#
# - 실제 BingX REST/WS 시세 사용 (market_data_rest / market_data_ws)
# - BingX 포지션/주문/잔고 API 는 모킹 → 실거래/실잔고 변화 없음
# - GPT ENTRY 는 "빈 피처" 호출로 비용 가드만 확인 (실제 GPT 호출 X)
# - 아래 항목을 한 번에 점검:
#   1) WS/REST 백필 및 헬스 체크
#   2) market_features_ws.build_entry_features_ws
#   3) unified_features_builder.build_unified_features
#   4) market_features_ws.get_trading_signal (엔트리 시그널 생성)
#   5) gpt_decider_entry.gpt_entry_call_count 비용 폭탄 가드
#   6) trader.check_closes 포지션 청산 감지 로직
#   7) sync_exchange.sync_open_trades_from_exchange 모의 동기화
#   8) signals_logger CSV 기록 (signals / candles / events)
#
# 실행 예:
#   $ python smoke_test_hybrid_single.py
#
# 필요 조건:
#   - 이 파일을 프로젝트 루트(기존 *.py 들과 같은 폴더)에 두고 실행
#   - BingX REST/WS 에 접속 가능한 네트워크 환경
#   - settings_ws.py 에서 기본 설정(symbol, interval 등) 로드 가능
#   - DB/텔레그램/GDrive 설정이 안 되어 있어도, 테스트 자체는 돌아가도록 방어

from __future__ import annotations

import time
import traceback
from typing import Any, Dict, List, Optional, Tuple

from settings_ws import load_settings
import telelog
from telelog import log

import run_bot_ws
from market_data_ws import (
    start_ws_loop,
    get_health_snapshot,
    get_klines_with_volume,
)
from market_features_ws import (
    build_entry_features_ws,
    FeatureBuildError,
    get_trading_signal,
)
from unified_features_builder import (
    build_unified_features,
    UnifiedFeaturesError,
)
import gpt_decider_entry
import exchange_api
import sync_exchange
from trader import Trade, TraderState, check_closes
from signals_logger import (
    log_signal,
    log_candle_snapshot,
    log_entry_event,
    log_exit_event,
    log_error_event,
)

# ─────────────────────────────────────────────
# 전역 설정/상태
# ─────────────────────────────────────────────

SET = load_settings()

# 테스트 결과 (이름, 성공 여부, 메시지)
_TEST_RESULTS: List[Tuple[str, bool, str]] = []

# 중간 산출물 (필요 시 테스트 간 공유)
_LAST_MARKET_FEATURES: Optional[Dict[str, Any]] = None
_LAST_UNIFIED_FEATURES: Optional[Dict[str, Any]] = None
_LAST_ENTRY_SIGNAL: Optional[Tuple[Any, ...]] = None

# 모킹용 BingX 상태
_MOCK_EXCHANGE_STATE: Dict[str, Any] = {
    "balance_detail": {
        "usedMargin": 0.0,
        "availableMargin": 1000.0,
    },
    "positions": [],  # {"positionAmt": float, "positionSide": "LONG"/"SHORT"}
    "orders": [],     # 단순 dict 목록
    "available_usdt": 1000.0,
}


# ─────────────────────────────────────────────
# 공통 유틸
# ─────────────────────────────────────────────

def _record_result(name: str, ok: bool, msg: str) -> None:
    _TEST_RESULTS.append((name, ok, msg))


def _run_step(name: str, func) -> None:
    log(f"[SMOKE] [{name}] 시작")
    start = time.time()
    ok = True
    msg = "OK"
    try:
        func()
    except Exception as e:
        ok = False
        msg = f"{type(e).__name__}: {e}"
        log(f"[SMOKE][ERROR] [{name}] {msg}")
        traceback.print_exc()
    dur = time.time() - start
    log(f"[SMOKE] [{name}] 완료 - {'OK' if ok else 'FAIL'} ({dur:.1f}s)")
    _record_result(name, ok, msg)


# ─────────────────────────────────────────────
# 모킹 세팅 (텔레그램 / BingX 선물 API / sync_exchange)
# ─────────────────────────────────────────────

def _setup_mocks() -> None:
    """텔레그램/선물 API/Sync 모듈을 모킹해서 실거래/실알림을 막는다."""

    # 1) 텔레그램: 실제 전송 대신 로그만 남김
    def _mock_send_tg(text: str) -> None:
        try:
            log(f"[SMOKE][TG-MOCK] {text}")
        except Exception:
            print(f"[SMOKE][TG-MOCK-FALLBACK] {text}")

    def _mock_send_structured_tg(**kwargs: Any) -> None:
        try:
            # 기존 템플릿이 있으면 재사용, 없으면 dict 그대로 출력
            if hasattr(telelog, "build_tg_template"):
                text = telelog.build_tg_template(**kwargs)  # type: ignore[attr-defined]
            else:
                text = f"[STRUCTURED] {kwargs}"
        except Exception:
            text = f"[STRUCTURED] {kwargs}"
        _mock_send_tg(text)

    try:
        telelog.send_tg = _mock_send_tg  # type: ignore[attr-defined]
    except Exception:
        pass

    try:
        telelog.send_structured_tg = _mock_send_structured_tg  # type: ignore[attr-defined]
    except Exception:
        pass

    # 2) BingX 선물 API 모킹 (exchange_api + sync_exchange 에 동시에 반영)

    # get_available_usdt
    if hasattr(exchange_api, "get_available_usdt"):
        def _mock_get_available_usdt(symbol: Optional[str] = None) -> float:
            log(f"[SMOKE][EX-MOCK] get_available_usdt(symbol={symbol})")
            return float(_MOCK_EXCHANGE_STATE["available_usdt"])
        exchange_api.get_available_usdt = _mock_get_available_usdt  # type: ignore[attr-defined]

    # set_leverage_and_mode
    if hasattr(exchange_api, "set_leverage_and_mode"):
        def _mock_set_leverage_and_mode(symbol: str, leverage: int) -> None:
            log(f"[SMOKE][EX-MOCK] set_leverage_and_mode(symbol={symbol}, leverage={leverage})")
        exchange_api.set_leverage_and_mode = _mock_set_leverage_and_mode  # type: ignore[attr-defined]

    # fetch_open_positions
    if hasattr(exchange_api, "fetch_open_positions"):
        def _mock_fetch_open_positions(symbol: str) -> List[Dict[str, Any]]:
            log(f"[SMOKE][EX-MOCK] fetch_open_positions(symbol={symbol})")
            return list(_MOCK_EXCHANGE_STATE["positions"])
        exchange_api.fetch_open_positions = _mock_fetch_open_positions  # type: ignore[attr-defined]

    # fetch_open_orders
    if hasattr(exchange_api, "fetch_open_orders"):
        def _mock_fetch_open_orders(symbol: str) -> List[Dict[str, Any]]:
            log(f"[SMOKE][EX-MOCK] fetch_open_orders(symbol={symbol})")
            return list(_MOCK_EXCHANGE_STATE["orders"])
        exchange_api.fetch_open_orders = _mock_fetch_open_orders  # type: ignore[attr-defined]

    # get_balance_detail
    if hasattr(exchange_api, "get_balance_detail"):
        def _mock_get_balance_detail() -> Dict[str, Any]:
            log("[SMOKE][EX-MOCK] get_balance_detail()")
            # entry_guards_ws.check_manual_position_guard 에서 usedMargin 사용
            return {
                "usedMargin": _MOCK_EXCHANGE_STATE["balance_detail"]["usedMargin"],
                "availableMargin": _MOCK_EXCHANGE_STATE["balance_detail"]["availableMargin"],
            }
        exchange_api.get_balance_detail = _mock_get_balance_detail  # type: ignore[attr-defined]

    # sync_exchange 모듈 내부 심볼도 동일 모킹으로 교체
    try:
        sync_exchange.fetch_open_positions = exchange_api.fetch_open_positions  # type: ignore[attr-defined]
        sync_exchange.fetch_open_orders = exchange_api.fetch_open_orders        # type: ignore[attr-defined]
        sync_exchange.get_balance_detail = exchange_api.get_balance_detail      # type: ignore[attr-defined]
    except Exception:
        pass


# ─────────────────────────────────────────────
# 1) WS/REST 백필 및 부팅
# ─────────────────────────────────────────────

def _step_boot_ws_and_rest() -> None:
    """run_bot_ws 의 REST 백필 유틸 + market_data_ws WS 루프를 직접 부팅."""
    symbol = SET.symbol

    log(f"[SMOKE] WS/REST 백필 시작 (symbol={symbol})")
    try:
        # 내부 유틸 직접 호출 (REST 히스토리 백필)
        run_bot_ws._backfill_ws_kline_history(symbol)  # type: ignore[attr-defined]
    except Exception as e:
        log(f"[SMOKE][WARN] _backfill_ws_kline_history 예외: {e}")

    # WS 루프 시작 (daemon thread)
    log(f"[SMOKE] WebSocket 루프 시작 (symbol={symbol})")
    start_ws_loop(symbol)

    wait_sec = 10
    log(f"[SMOKE] WS 시세 수신 안정화 대기 {wait_sec}초…")
    time.sleep(wait_sec)


# ─────────────────────────────────────────────
# 2) WS 헬스 체크
# ─────────────────────────────────────────────

def _step_check_ws_health() -> None:
    """market_data_ws.get_health_snapshot 로 WS 상태 확인."""
    symbol = SET.symbol
    timeout_sec = 30
    last_snap: Optional[Dict[str, Any]] = None

    log(f"[SMOKE] WS health_snapshot 폴링 (timeout={timeout_sec}s)")
    for i in range(timeout_sec):
        snap = get_health_snapshot(symbol)
        last_snap = snap
        if snap and snap.get("overall_ok"):
            log(f"[SMOKE] WS health overall_ok=True (wait={i}s)")
            return
        time.sleep(1.0)

    raise RuntimeError(f"WS health overall_ok=False (마지막 스냅샷={last_snap})")


# ─────────────────────────────────────────────
# 3) market_features_ws.build_entry_features_ws
# ─────────────────────────────────────────────

def _step_market_features() -> None:
    """엔트리용 WS 마켓 피처를 한 번 생성해본다."""
    global _LAST_MARKET_FEATURES

    symbol = SET.symbol
    log(f"[SMOKE] build_entry_features_ws 호출 (symbol={symbol})")
    try:
        features = build_entry_features_ws(symbol)
    except FeatureBuildError as e:
        raise RuntimeError(f"FeatureBuildError: {e}")
    except Exception as e:
        raise RuntimeError(f"build_entry_features_ws 예외: {e}")

    if not isinstance(features, dict):
        raise RuntimeError("build_entry_features_ws 반환값이 dict 가 아님")

    # 최소 필드 검증
    for key in ("symbol", "timeframes"):
        if key not in features:
            raise RuntimeError(f"market_features 누락 키: {key}")

    tfs = features.get("timeframes", {})
    for tf in ("1m", "5m", "15m"):
        if tf not in tfs:
            raise RuntimeError(f"timeframes['{tf}'] 누락")
        buf_len = tfs[tf].get("buffer_len", 0)
        if buf_len <= 0:
            raise RuntimeError(f"timeframes['{tf}'].buffer_len <= 0")

    log("[SMOKE] build_entry_features_ws OK")
    _LAST_MARKET_FEATURES = features


# ─────────────────────────────────────────────
# 4) unified_features_builder.build_unified_features
# ─────────────────────────────────────────────

def _step_unified_features() -> None:
    """WS 마켓 피처 + 패턴 피처 통합 빌더 테스트."""
    global _LAST_UNIFIED_FEATURES

    symbol = SET.symbol
    log(f"[SMOKE] build_unified_features 호출 (symbol={symbol})")
    try:
        uf = build_unified_features(symbol)
    except UnifiedFeaturesError as e:
        raise RuntimeError(f"UnifiedFeaturesError: {e}")
    except Exception as e:
        raise RuntimeError(f"build_unified_features 예외: {e}")

    if not isinstance(uf, dict):
        raise RuntimeError("unified_features 반환값이 dict 가 아님")

    if "pattern_features" not in uf or "pattern_summary" not in uf:
        raise RuntimeError("unified_features 에 pattern_* 키가 없음")

    if not isinstance(uf["pattern_summary"], dict):
        raise RuntimeError("pattern_summary 가 dict 가 아님")

    log("[SMOKE] build_unified_features OK")
    _LAST_UNIFIED_FEATURES = uf


# ─────────────────────────────────────────────
# 5) market_features_ws.get_trading_signal
# ─────────────────────────────────────────────

def _step_entry_signal() -> None:
    """WS 기반 엔트리 시그널 생성 경로 점검."""
    global _LAST_ENTRY_SIGNAL

    symbol = SET.symbol
    log(f"[SMOKE] get_trading_signal 호출 (symbol={symbol})")

    # ★ 추가된 부분 (2줄)
    rows = get_klines_with_volume(symbol, "1m", limit=1)
    if not rows:
        raise RuntimeError("1m 캔들 없음")
    last_close_ts = int(rows[-1][0])
    
    # ★ 수정된 호출부
    sig_ctx = get_trading_signal(settings=SET, last_close_ts=last_close_ts)
    if sig_ctx is None:
        raise RuntimeError("get_trading_signal → None (시그널 없음 / 모든 후보 필터링)")

    (
        chosen_signal,
        signal_source,
        latest_ts,
        candles_5m,
        candles_5m_raw,
        last_price,
        extra,
    ) = sig_ctx

    if chosen_signal not in ("LONG", "SHORT"):
        raise RuntimeError(f"chosen_signal 이상: {chosen_signal}")

    if not isinstance(last_price, (int, float)) or float(last_price) <= 0:
        raise RuntimeError(f"last_price 이상: {last_price}")

    if not candles_5m or not candles_5m_raw:
        raise RuntimeError("5m 캔들 데이터 부족")

    if not isinstance(extra, dict) or "signal_score" not in extra:
        raise RuntimeError("extra['signal_score'] 누락")

    log(
        "[SMOKE] get_trading_signal OK: "
        f"side={chosen_signal}, source={signal_source}, "
        f"score={extra.get('signal_score')}"
    )
    _LAST_ENTRY_SIGNAL = sig_ctx


# ─────────────────────────────────────────────
# 6) GPT ENTRY 비용 폭탄 가드 (gpt_entry_call_count)
# ─────────────────────────────────────────────

def _step_gpt_cost_guard() -> None:
    """
    market_features 가 '빈 dict' 인 상황에서 ask_entry_decision 이
    GPT 를 호출하지 않고 로컬 가드에서 컷되는지 확인.
    """
    symbol = SET.symbol
    before = gpt_decider_entry.gpt_entry_call_count
    log(f"[SMOKE] GPT ENTRY 비용 가드 테스트 (기존 call_count={before})")

    resp: Any = None
    try:
        # market_features 를 비우면, gpt_decider_entry 내부 비용/품질 가드에서
        # GPT 호출 전에 SKIP 되도록 설계되어 있음.
        resp = gpt_decider_entry.ask_entry_decision(
            symbol=symbol,
            source="SMOKE_TEST",
            current_price=50_000.0,
            base_tv_pct=0.01,
            base_sl_pct=0.02,
            base_risk_pct=0.01,
            market_features={},  # 의도적으로 빈 피처
        )
    except Exception as e:
        log(f"[SMOKE][WARN] ask_entry_decision 예외 (무시): {e}")

    after = gpt_decider_entry.gpt_entry_call_count
    log(f"[SMOKE] gpt_entry_call_count: before={before}, after={after}")

    if before != after:
        raise RuntimeError(
            f"GPT 비용 가드 실패: gpt_entry_call_count {before} → {after} 증가"
        )

    if isinstance(resp, dict):
        reason = resp.get("reason") or resp.get("note") or ""
        log(f"[SMOKE] ask_entry_decision 로컬 SKIP 응답: reason={reason!r}")

    log("[SMOKE] GPT ENTRY 비용 폭탄 가드 OK (실제 GPT 호출 없음)")


# ─────────────────────────────────────────────
# 7) trader.check_closes 포지션 청산 감지
# ─────────────────────────────────────────────

def _step_trade_close_detection() -> None:
    """
    - 모의 포지션 1개를 만든 뒤:
      1) BingX(모킹) 쪽에 동일 포지션 존재 → 여전히 열린 상태로 인식하는지
      2) BingX 포지션이 사라진 경우 → 청산 이벤트로 인식하는지
    """
    symbol = SET.symbol
    interval = getattr(SET, "interval", "1m")

    rows = get_klines_with_volume(symbol, interval, limit=1)
    if not rows:
        raise RuntimeError("WS 캔들 없음 → 청산 감지 테스트 불가")

    ts_ms, o, h, l, c, v = rows[-1]
    last_close = float(c)

    trade = Trade(
        symbol=symbol,
        side="BUY",
        qty=0.001,
        entry_price=last_close,
        leverage=float(getattr(SET, "leverage", 3.0)),
        tp_price=last_close * 1.01,
        sl_price=last_close * 0.99,
    )
    state = TraderState(max_tp_sl_reset_failures=5)

    # (1) 거래소에 포지션이 그대로 있는 상태
    _MOCK_EXCHANGE_STATE["positions"] = [
        {
            "positionAmt": trade.qty,
            "positionSide": "LONG",
        }
    ]
    open_trades, closed_events = check_closes([trade], state)
    if len(open_trades) != 1 or closed_events:
        raise RuntimeError("포지션 유지 케이스에서 check_closes 결과가 예상과 다름")

    # (2) 거래소 포지션이 완전히 사라진 상태 (TP/SL/수동청산 등)
    _MOCK_EXCHANGE_STATE["positions"] = []
    open_trades2, closed_events2 = check_closes([trade], state)
    if open_trades2:
        raise RuntimeError("포지션 종료 후에도 open_trades 가 남아 있음")
    if len(closed_events2) != 1:
        raise RuntimeError(f"closed 이벤트 개수 이상: {len(closed_events2)} != 1")

    ev = closed_events2[0]
    summary = ev.get("summary") or {}
    pnl = summary.get("pnl")
    log(
        f"[SMOKE] check_closes OK → reason={ev.get('reason')} "
        f"pnl={pnl}, pnl_pct={summary.get('pnl_pct')}"
    )


# ─────────────────────────────────────────────
# 8) sync_exchange.sync_open_trades_from_exchange 모의 동기화
# ─────────────────────────────────────────────

def _step_sync_exchange_mock() -> None:
    """
    모킹된 BingX 포지션/주문 정보를 기반으로 sync_open_trades_from_exchange 가
    Trade 객체를 정상 생성하는지 확인.
    """
    symbol = SET.symbol

    _MOCK_EXCHANGE_STATE["positions"] = [
        {
            "positionAmt": 0.002,
            "positionSide": "LONG",
            "entryPrice": 50_000.0,
            "leverage": getattr(SET, "leverage", 3),
            "unrealizedProfit": 0.0,
        }
    ]
    _MOCK_EXCHANGE_STATE["orders"] = [
        {
            "orderId": "tp-1",
            "type": "TAKE_PROFIT_MARKET",
            "side": "SELL",
            "triggerPrice": 50_500.0,
            "origQty": 0.002,
        },
        {
            "orderId": "sl-1",
            "type": "STOP_MARKET",
            "side": "SELL",
            "triggerPrice": 49_500.0,
            "origQty": 0.002,
        },
    ]

    trades, pos_count = sync_exchange.sync_open_trades_from_exchange(
        symbol=symbol,
        replace=True,
        current_trades=[],
    )

    if pos_count != 1:
        raise RuntimeError(f"sync_exchange pos_count={pos_count} (기대=1)")
    if len(trades) != 1:
        raise RuntimeError(f"sync_exchange Trade 개수={len(trades)} (기대=1)")

    t = trades[0]
    if t.qty <= 0:
        raise RuntimeError("sync_exchange Trade.qty <= 0")
    if getattr(t, "tp_price", None) is None or getattr(t, "sl_price", None) is None:
        raise RuntimeError("sync_exchange Trade 의 TP/SL price 누락")

    log(
        "[SMOKE] sync_open_trades_from_exchange OK "
        f"(qty={t.qty}, tp={getattr(t, 'tp_price', None)}, sl={getattr(t, 'sl_price', None)})"
    )


# ─────────────────────────────────────────────
# 9) signals_logger CSV 기록 테스트
# ─────────────────────────────────────────────

def _step_csv_logging() -> None:
    """
    signals_logger 가 signals / candles / events CSV 3종에 정상 기록하는지 확인.
    실제 파일 내용은 사용자가 logs/ 하위에서 직접 확인 가능.
    """
    symbol = SET.symbol
    now_ts_ms = int(time.time() * 1000)

    # (1) 시그널 CSV
    log_signal(
        event="ENTRY_SIGNAL",
        symbol=symbol,
        strategy_type="SMOKE",
        direction="LONG",
        price=12_345.6,
        qty=0.001,
        tp_price=12_500.0,
        sl_price=12_000.0,
        reason="smoke_test_signal",
        candle_ts=now_ts_ms,
        strategy_version="smoke_v1",
        range_level=1,
        soft_block_reason="soft_atr",
        used_tp_pct=0.02,
        used_sl_pct=0.01,
    )

    # (2) 캔들 스냅샷 CSV
    rows = get_klines_with_volume(symbol, getattr(SET, "interval", "1m"), limit=1)
    if rows:
        ts_ms, o, h, l, c, v = rows[-1]
        log_candle_snapshot(
            symbol=symbol,
            tf=getattr(SET, "interval", "1m"),
            candle_ts=int(ts_ms),
            open_=float(o),
            high=float(h),
            low=float(l),
            close=float(c),
            volume=float(v),
            strategy_type="SMOKE",
            direction="LONG",
            extra="smoke_test_candle",
        )

    # (3) 이벤트 CSV (ENTRY/EXIT/ERROR)
    log_entry_event(
        symbol=symbol,
        regime="SMOKE",
        side="LONG",
        price=12_345.6,
        qty=0.001,
        leverage=float(getattr(SET, "leverage", 3.0)),
        tp_pct=0.02,
        sl_pct=0.01,
        risk_pct=float(getattr(SET, "risk_pct", 0.01)),
        reason="smoke_entry_event",
        extra={"smoke": True},
    )

    log_exit_event(
        symbol=symbol,
        regime="SMOKE",
        side="LONG",
        price=12_600.0,
        qty=0.001,
        leverage=float(getattr(SET, "leverage", 3.0)),
        pnl_pct=1.0,
        scenario="smoke_exit",
        reason="TP_HIT",
        extra={"smoke": True},
    )

    log_error_event(
        where="SMOKE_TEST",
        message="smoke_error",
        extra={"detail": "just_a_test"},
    )

    log("[SMOKE] signals_logger CSV 기록 OK (logs/ 하위 파일 생성 여부 직접 확인 가능)")


# ─────────────────────────────────────────────
# 메인 진입점
# ─────────────────────────────────────────────

def main() -> None:
    log("======================================================")
    log("[SMOKE] 하이브리드 스모크 테스트 시작")
    log("  - 실제 WS/REST 시세 사용")
    log("  - BingX 선물 API / GPT 호출은 모킹 또는 비용 가드로 방어")
    log("======================================================")

    _setup_mocks()

    _run_step("WS/REST 부팅", _step_boot_ws_and_rest)
    _run_step("WS 헬스체크", _step_check_ws_health)
    _run_step("마켓 피처 생성", _step_market_features)
    _run_step("유니파이드 피처 생성", _step_unified_features)
    _run_step("엔트리 시그널 생성", _step_entry_signal)
    _run_step("GPT ENTRY 비용 가드", _step_gpt_cost_guard)
    _run_step("포지션 청산 감지(check_closes)", _step_trade_close_detection)
    _run_step("sync_exchange 모의 동기화", _step_sync_exchange_mock)
    _run_step("signals_logger CSV 기록", _step_csv_logging)

    # 요약 출력
    log("======================================================")
    log("[SMOKE] 테스트 요약 결과")
    all_ok = True
    for name, ok, msg in _TEST_RESULTS:
        status = "OK " if ok else "FAIL"
        log(f"  - {status:4s} | {name} | {msg}")
        if not ok:
            all_ok = False

    log("======================================================")
    if all_ok:
        log("[SMOKE] ✅ 모든 스모크 테스트 통과")
    else:
        log("[SMOKE] ❌ 실패한 스텝이 있습니다. 위 로그를 확인하십시오.")


if __name__ == "__main__":
    main()
