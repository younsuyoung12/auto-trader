# -*- coding: utf-8 -*-
# infra/market_data_ws_orderbook.py
"""
========================================================
FILE: infra/market_data_ws_orderbook.py
ROLE:
- market_data_ws 분리 리팩터링 orderbook 전용 계층
- REST snapshot + diff-depth 정합, 로컬 북 상태, public orderbook getter를 담당한다

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- same-package import 는 상대 import로 통일한다
- orderbook 정합 로직은 이 파일에서만 담당한다
- Binance diff-depth gap / non-bridge 는 즉시 세션 종료보다
  "로컬 북 재동기화(resync)"를 우선 수행한다
- REST snapshot 과 WS diff 간 타이밍 차이로 발생하는 초기 gap은
  운영상 정상 시나리오로 취급하며 self-heal 해야 한다

CHANGE HISTORY:
- 2026-03-15:
  1) FIX(ROOT-CAUSE): 첫 diff-depth non-bridge / live gap / pu mismatch 를
     protocol fatal 대신 local orderbook resync 로 전환
  2) FIX(RECOVERY): resync 진입 시 placeholder state + seed event 를 재구성하고
     stale orderbook buffer 를 즉시 제거하도록 수정
  3) FIX(OPERABILITY): bootstrap 중 pending event 처리에서 재동기화 필요 이벤트를
     fatal 처리하지 않고 bootstrap 종료 후 다음 live event 로 회복 가능하게 정리
  4) FEAT(OBSERVABILITY): state.resync_reason / state.last_resync_ts 를 추가해
     watchdog / telemetry 에서 recovery 문맥을 추적 가능화
  5) FIX(ROOT-CAUSE): resync 시 REST bootstrap 을 동일 WS callback 스레드에서 동기 수행하지 않고
     background bootstrap worker 로 분리
  6) FIX(RECOVERY): bootstrap worker 실행 중 들어오는 diff-depth 이벤트를 placeholder pending queue 에
     적재할 수 있도록 구조 보강
========================================================
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from .market_data_store import save_orderbook_from_ws
from .market_data_ws_shared import (
    _ORDERBOOK_PENDING_EVENT_MAX,
    _ORDERBOOK_SNAPSHOT_LIMIT,
    _ORDERBOOK_TOP_N,
    WSProtocolError,
    _extract_orderbook_event_ts_ms_strict,
    _mark_ws_message,
    _normalize_symbol,
    _now_ms,
    _orderbook_book_state,
    _orderbook_buffers,
    _orderbook_last_recv_ts,
    _orderbook_last_update_id,
    _orderbook_lock,
    _require_float,
    _require_positive_int,
    _rest_fetch_depth_snapshot_strict,
    log,
)

_ORDERBOOK_BOOTSTRAP_RETRY_SEC: float = 0.5
_ORDERBOOK_BOOTSTRAP_MAX_RETRIES: int = 3

_ORDERBOOK_BOOTSTRAP_INFLIGHT: set[str] = set()
_ORDERBOOK_BOOTSTRAP_INFLIGHT_LOCK = threading.Lock()


class OrderbookResyncRequired(RuntimeError):
    """Recoverable orderbook desync that requires fresh REST snapshot re-bootstrap."""


def _make_orderbook_bootstrap_placeholder_state(
    *,
    pending_events: Optional[List[Dict[str, Any]]] = None,
    reason: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "ready": False,
        "bootstrapping": True,
        "stream_aligned": False,
        "snapshot_last_update_id": None,
        "last_update_id": None,
        "bids_map": {},
        "asks_map": {},
        "last_snapshot_recv_ts": None,
        "pending_events": list(pending_events or []),
        "resync_reason": str(reason or "").strip() or None,
        "last_resync_ts": _now_ms(),
    }


def _normalize_depth_side_strict(
    side_val: Any,
    *,
    name: str,
    allow_zero_qty: bool = False,
    allow_empty: bool = False,
) -> List[List[float]]:
    if side_val is None:
        raise WSProtocolError(f"{name} missing")
    if not isinstance(side_val, (list, tuple)):
        raise WSProtocolError(f"{name} must be list/tuple")

    out: List[List[float]] = []
    for i, row in enumerate(side_val):
        if isinstance(row, (list, tuple)) and len(row) >= 2:
            p = _require_float(row[0], f"{name}[{i}].price")
            q = _require_float(row[1], f"{name}[{i}].qty", allow_zero=allow_zero_qty)
            out.append([p, q])
            continue

        if isinstance(row, dict):
            p = _require_float(row.get("price"), f"{name}[{i}].price")
            q = _require_float(row.get("qty"), f"{name}[{i}].qty", allow_zero=allow_zero_qty)
            out.append([p, q])
            continue

        raise WSProtocolError(f"{name}[{i}] invalid level type: {type(row).__name__}")

    if not out and not allow_empty:
        raise WSProtocolError(f"{name} empty after parse (STRICT)")
    return out


def _compute_best_prices_strict(bids: List[List[float]], asks: List[List[float]]) -> Tuple[float, float]:
    if not bids or not asks:
        raise WSProtocolError("bids/asks empty (STRICT)")

    best_bid = max(float(r[0]) for r in bids)
    best_ask = min(float(r[0]) for r in asks)

    if best_bid <= 0 or best_ask <= 0:
        raise WSProtocolError("best prices invalid (STRICT)")
    if best_ask <= best_bid:
        raise WSProtocolError("crossed book (bestAsk<=bestBid) (STRICT)")

    return float(best_bid), float(best_ask)


def _compute_spread_pct(best_bid: float, best_ask: float) -> float:
    if best_bid <= 0 or best_ask <= 0:
        raise WSProtocolError("best prices invalid for spread (STRICT)")
    if best_ask <= best_bid:
        raise WSProtocolError("crossed book for spread (STRICT)")

    mid = (best_bid + best_ask) / 2.0
    if mid <= 0:
        raise WSProtocolError("mid invalid (STRICT)")

    return (best_ask - best_bid) / mid


def _persist_ws_orderbook_snapshot_strict(
    *,
    symbol: str,
    event_ts_ms: int,
    bids: List[List[float]],
    asks: List[List[float]],
) -> None:
    save_orderbook_from_ws(
        symbol=symbol,
        ts_ms=event_ts_ms,
        bids=bids,
        asks=asks,
    )


def _build_orderbook_price_map_strict(levels: List[List[float]], *, name: str) -> Dict[float, float]:
    price_map: Dict[float, float] = {}
    for idx, row in enumerate(levels):
        if not isinstance(row, list) or len(row) != 2:
            raise WSProtocolError(f"{name}[{idx}] must be [price, qty] (STRICT)")

        price = _require_float(row[0], f"{name}[{idx}].price")
        qty = _require_float(row[1], f"{name}[{idx}].qty")

        if qty <= 0.0:
            raise WSProtocolError(f"{name}[{idx}].qty must be > 0 for snapshot/book state (STRICT)")

        price_map[price] = qty

    if not price_map:
        raise WSProtocolError(f"{name} produced empty price_map (STRICT)")
    return price_map


def _apply_depth_updates_to_price_map_strict(
    price_map: Dict[float, float],
    updates: List[List[float]],
    *,
    name: str,
) -> None:
    if not isinstance(price_map, dict):
        raise WSProtocolError(f"{name} price_map must be dict (STRICT)")

    for idx, row in enumerate(updates):
        if not isinstance(row, list) or len(row) != 2:
            raise WSProtocolError(f"{name}[{idx}] must be [price, qty] (STRICT)")

        price = _require_float(row[0], f"{name}[{idx}].price")
        qty = _require_float(row[1], f"{name}[{idx}].qty", allow_zero=True)

        if qty == 0.0:
            price_map.pop(price, None)
        else:
            price_map[price] = qty


def _materialize_orderbook_side_top_n_strict(
    price_map: Dict[float, float],
    *,
    name: str,
    descending: bool,
    limit: int,
) -> List[List[float]]:
    if not isinstance(price_map, dict):
        raise WSProtocolError(f"{name} price_map must be dict (STRICT)")
    if limit <= 0:
        raise WSProtocolError(f"{name} limit must be > 0 (STRICT)")
    if not price_map:
        raise WSProtocolError(f"{name} empty (STRICT)")

    ordered = sorted(price_map.items(), key=lambda kv: kv[0], reverse=descending)
    out: List[List[float]] = []

    for price, qty in ordered[:limit]:
        if price <= 0.0:
            raise WSProtocolError(f"{name} contains non-positive price (STRICT)")
        if qty <= 0.0:
            raise WSProtocolError(f"{name} contains non-positive qty (STRICT)")
        out.append([float(price), float(qty)])

    if not out:
        raise WSProtocolError(f"{name} top-n materialization empty (STRICT)")
    return out


def _materialize_orderbook_top_levels_strict(
    bids_map: Dict[float, float],
    asks_map: Dict[float, float],
) -> Tuple[List[List[float]], List[List[float]], float, float, float]:
    bids_top = _materialize_orderbook_side_top_n_strict(
        bids_map,
        name="orderbook.bids_map",
        descending=True,
        limit=_ORDERBOOK_TOP_N,
    )
    asks_top = _materialize_orderbook_side_top_n_strict(
        asks_map,
        name="orderbook.asks_map",
        descending=False,
        limit=_ORDERBOOK_TOP_N,
    )
    best_bid, best_ask = _compute_best_prices_strict(bids_top, asks_top)
    spread_pct = _compute_spread_pct(best_bid, best_ask)
    return bids_top, asks_top, best_bid, best_ask, spread_pct


def _normalize_orderbook_diff_event_strict(payload: Dict[str, Any]) -> Dict[str, Any]:
    first_update_id = _require_positive_int(payload.get("U"), "orderbook.U")
    final_update_id = _require_positive_int(payload.get("u"), "orderbook.u")
    if final_update_id < first_update_id:
        raise WSProtocolError(
            f"orderbook update id range invalid (STRICT): U={first_update_id} u={final_update_id}"
        )

    prev_final_update_id: Optional[int] = None
    raw_prev_final_update_id = payload.get("pu")
    if raw_prev_final_update_id is not None:
        prev_final_update_id = _require_positive_int(raw_prev_final_update_id, "orderbook.pu")

    raw_bids = payload.get("b") if payload.get("b") is not None else payload.get("bids")
    raw_asks = payload.get("a") if payload.get("a") is not None else payload.get("asks")

    normalized_bids = _normalize_depth_side_strict(
        raw_bids,
        name="orderbook.bids",
        allow_zero_qty=True,
        allow_empty=True,
    )
    normalized_asks = _normalize_depth_side_strict(
        raw_asks,
        name="orderbook.asks",
        allow_zero_qty=True,
        allow_empty=True,
    )

    return {
        "U": int(first_update_id),
        "u": int(final_update_id),
        "pu": prev_final_update_id,
        "bids": normalized_bids,
        "asks": normalized_asks,
        "event_ts_ms": _extract_orderbook_event_ts_ms_strict(payload),
    }


def _queue_orderbook_pending_event_locked(symbol: str, state: Dict[str, Any], event: Dict[str, Any]) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise WSProtocolError("symbol is required for pending diff-depth queue (STRICT)")
    if not isinstance(state, dict):
        raise WSProtocolError("orderbook state must be dict for pending queue (STRICT)")

    pending = state.get("pending_events")
    if not isinstance(pending, list):
        raise WSProtocolError("orderbook_state.pending_events must be list (STRICT)")

    pending.append(dict(event))
    if len(pending) > _ORDERBOOK_PENDING_EVENT_MAX:
        raise WSProtocolError(
            f"pending diff-depth queue overflow (STRICT): symbol={sym} "
            f"size={len(pending)} cap={_ORDERBOOK_PENDING_EVENT_MAX}"
        )


def _replace_with_bootstrap_placeholder_locked(
    symbol: str,
    *,
    seed_event: Optional[Dict[str, Any]],
    reason: str,
) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise WSProtocolError("symbol is required for orderbook resync placeholder (STRICT)")

    pending_events: List[Dict[str, Any]] = []
    if seed_event is not None:
        pending_events.append(dict(seed_event))

    _orderbook_book_state[sym] = _make_orderbook_bootstrap_placeholder_state(
        pending_events=pending_events,
        reason=reason,
    )
    _orderbook_buffers.pop(sym, None)
    _orderbook_last_update_id.pop(sym, None)
    _orderbook_last_recv_ts.pop(sym, None)


def _build_orderbook_state_from_snapshot_strict(
    symbol: str,
    snapshot_payload: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise WSProtocolError("symbol is required for snapshot state build (STRICT)")

    last_update_id = _require_positive_int(snapshot_payload.get("lastUpdateId"), "depth_snapshot.lastUpdateId")

    bids = _normalize_depth_side_strict(
        snapshot_payload.get("bids"),
        name="depth_snapshot.bids",
        allow_zero_qty=False,
        allow_empty=False,
    )
    asks = _normalize_depth_side_strict(
        snapshot_payload.get("asks"),
        name="depth_snapshot.asks",
        allow_zero_qty=False,
        allow_empty=False,
    )

    bids_map = _build_orderbook_price_map_strict(bids, name="depth_snapshot.bids")
    asks_map = _build_orderbook_price_map_strict(asks, name="depth_snapshot.asks")
    bids_top, asks_top, best_bid, best_ask, spread_pct = _materialize_orderbook_top_levels_strict(
        bids_map,
        asks_map,
    )

    now_ms = _now_ms()

    state = {
        "ready": True,
        "bootstrapping": True,
        "stream_aligned": False,
        "snapshot_last_update_id": int(last_update_id),
        "last_update_id": int(last_update_id),
        "bids_map": bids_map,
        "asks_map": asks_map,
        "last_snapshot_recv_ts": now_ms,
        "pending_events": [],
        "resync_reason": None,
        "last_resync_ts": None,
    }
    snapshot = {
        "bids": bids_top,
        "asks": asks_top,
        "ts": now_ms,
        "bestBid": best_bid,
        "bestAsk": best_ask,
        "spreadPct": spread_pct,
        "lastUpdateId": int(last_update_id),
        "exchTs": None,
        "source": "rest_bootstrap",
        "streamAligned": False,
        "snapshotLastUpdateId": int(last_update_id),
    }
    return state, snapshot


def _advance_orderbook_state_with_event_strict(
    symbol: str,
    state: Dict[str, Any],
    event: Dict[str, Any],
) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise WSProtocolError("symbol is required for orderbook diff advance (STRICT)")
    if not isinstance(state, dict):
        raise WSProtocolError("orderbook state must be dict (STRICT)")
    if not isinstance(event, dict):
        raise WSProtocolError("orderbook diff event must be dict (STRICT)")

    ready = bool(state.get("ready", False))
    if not ready:
        raise WSProtocolError("orderbook state not ready for diff advance (STRICT)")

    current_last_update_id = _require_positive_int(
        state.get("last_update_id"),
        "orderbook_state.last_update_id",
    )
    snapshot_last_update_id = _require_positive_int(
        state.get("snapshot_last_update_id"),
        "orderbook_state.snapshot_last_update_id",
    )
    stream_aligned = bool(state.get("stream_aligned", False))

    first_update_id = _require_positive_int(event.get("U"), "orderbook_event.U")
    final_update_id = _require_positive_int(event.get("u"), "orderbook_event.u")

    if final_update_id <= current_last_update_id:
        log(
            f"[WS_INFO] orderbook replay/rollback ignored "
            f"symbol={sym} prev={current_last_update_id} new={final_update_id}"
        )
        return None

    prev_final_update_id = event.get("pu")
    if prev_final_update_id is not None:
        prev_final_update_id = _require_positive_int(prev_final_update_id, "orderbook_event.pu")

    bridge_update_id = current_last_update_id + 1
    if not stream_aligned:
        if first_update_id > bridge_update_id or final_update_id < bridge_update_id:
            raise OrderbookResyncRequired(
                "first diff depth event does not bridge REST snapshot (STRICT): "
                f"symbol={sym} snapshot_last_update_id={snapshot_last_update_id} "
                f"state_last_update_id={current_last_update_id} U={first_update_id} u={final_update_id}"
            )
    else:
        if prev_final_update_id is not None and prev_final_update_id != current_last_update_id:
            raise OrderbookResyncRequired(
                "diff depth chain broken by pu mismatch (STRICT): "
                f"symbol={sym} expected_pu={current_last_update_id} got_pu={prev_final_update_id}"
            )
        if first_update_id > bridge_update_id:
            raise OrderbookResyncRequired(
                "diff depth gap detected (STRICT): "
                f"symbol={sym} expected_U<={bridge_update_id} got_U={first_update_id}"
            )

    bids_map_raw = state.get("bids_map")
    asks_map_raw = state.get("asks_map")
    if not isinstance(bids_map_raw, dict) or not isinstance(asks_map_raw, dict):
        raise WSProtocolError("orderbook state bids_map/asks_map missing (STRICT)")

    next_bids_map = dict(bids_map_raw)
    next_asks_map = dict(asks_map_raw)

    _apply_depth_updates_to_price_map_strict(next_bids_map, list(event.get("bids") or []), name="orderbook.bids")
    _apply_depth_updates_to_price_map_strict(next_asks_map, list(event.get("asks") or []), name="orderbook.asks")

    bids_top, asks_top, best_bid, best_ask, spread_pct = _materialize_orderbook_top_levels_strict(
        next_bids_map,
        next_asks_map,
    )

    next_state = {
        "ready": True,
        "bootstrapping": bool(state.get("bootstrapping", False)),
        "stream_aligned": True,
        "snapshot_last_update_id": int(snapshot_last_update_id),
        "last_update_id": int(final_update_id),
        "bids_map": next_bids_map,
        "asks_map": next_asks_map,
        "last_snapshot_recv_ts": state.get("last_snapshot_recv_ts"),
        "pending_events": list(state.get("pending_events") or []),
        "resync_reason": state.get("resync_reason"),
        "last_resync_ts": state.get("last_resync_ts"),
    }
    snapshot = {
        "bids": bids_top,
        "asks": asks_top,
        "ts": int(event["event_ts_ms"]),
        "bestBid": best_bid,
        "bestAsk": best_ask,
        "spreadPct": spread_pct,
        "lastUpdateId": int(final_update_id),
        "exchTs": int(event["event_ts_ms"]),
        "source": "ws_diff_depth",
        "streamAligned": True,
        "snapshotLastUpdateId": int(snapshot_last_update_id),
    }
    return next_state, snapshot


def _bootstrap_orderbook_from_rest_snapshot_strict(symbol: str) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise RuntimeError("symbol is required for REST depth bootstrap")

    snapshot_payload = _rest_fetch_depth_snapshot_strict(sym, _ORDERBOOK_SNAPSHOT_LIMIT)
    state_from_snapshot, snapshot_view = _build_orderbook_state_from_snapshot_strict(sym, snapshot_payload)

    with _orderbook_lock:
        placeholder = _orderbook_book_state.get(sym)
        if not isinstance(placeholder, dict) or not bool(placeholder.get("bootstrapping", False)):
            raise WSProtocolError("orderbook bootstrap placeholder missing (STRICT)")

        pending_events_raw = placeholder.get("pending_events")
        if not isinstance(pending_events_raw, list):
            raise WSProtocolError("orderbook bootstrap placeholder pending_events invalid (STRICT)")

        state = dict(state_from_snapshot)
        state["pending_events"] = list(pending_events_raw)
        state["resync_reason"] = placeholder.get("resync_reason")
        state["last_resync_ts"] = placeholder.get("last_resync_ts")

        _orderbook_book_state[sym] = state
        _orderbook_buffers[sym] = dict(snapshot_view)
        _orderbook_last_update_id[sym] = int(snapshot_view["lastUpdateId"])
        _orderbook_last_recv_ts[sym] = int(state["last_snapshot_recv_ts"])

        last_ws_snapshot: Optional[Dict[str, Any]] = None
        while True:
            pending = state.get("pending_events")
            if not isinstance(pending, list):
                raise WSProtocolError("orderbook_state.pending_events must remain list (STRICT)")
            if not pending:
                break

            event = pending.pop(0)
            try:
                transition = _advance_orderbook_state_with_event_strict(sym, state, event)
            except OrderbookResyncRequired as e:
                log(
                    f"[MD_BINANCE_WS][ORDERBOOK][BOOTSTRAP_PENDING_SKIP] "
                    f"symbol={sym} reason={e}"
                )
                break

            if transition is None:
                continue

            next_state, ws_snapshot = transition
            _persist_ws_orderbook_snapshot_strict(
                symbol=sym,
                event_ts_ms=int(ws_snapshot["exchTs"]),
                bids=list(ws_snapshot["bids"]),
                asks=list(ws_snapshot["asks"]),
            )
            state = next_state
            _orderbook_book_state[sym] = state
            _orderbook_buffers[sym] = dict(ws_snapshot)
            _orderbook_last_update_id[sym] = int(ws_snapshot["lastUpdateId"])
            _orderbook_last_recv_ts[sym] = _now_ms()
            last_ws_snapshot = ws_snapshot

        state["bootstrapping"] = False
        _orderbook_book_state[sym] = state

        if last_ws_snapshot is not None:
            _orderbook_buffers[sym] = dict(last_ws_snapshot)
            _orderbook_last_update_id[sym] = int(last_ws_snapshot["lastUpdateId"])
        else:
            snapshot_view["streamAligned"] = False
            _orderbook_buffers[sym] = dict(snapshot_view)
            _orderbook_last_update_id[sym] = int(snapshot_view["lastUpdateId"])

    if last_ws_snapshot is None:
        log(
            f"[MD_BINANCE_WS] orderbook snapshot bootstrapped: "
            f"symbol={sym} lastUpdateId={snapshot_view['lastUpdateId']} "
            f"bestBid={snapshot_view['bestBid']} bestAsk={snapshot_view['bestAsk']}"
        )
    else:
        log(
            f"[MD_BINANCE_WS] orderbook snapshot bootstrapped+aligned: "
            f"symbol={sym} snapshot_lastUpdateId={snapshot_view['lastUpdateId']} "
            f"final_lastUpdateId={last_ws_snapshot['lastUpdateId']}"
        )


def _run_orderbook_bootstrap_worker(symbol: str) -> None:
    sym = _normalize_symbol(symbol)
    try:
        last_error: Optional[BaseException] = None
        for attempt in range(1, _ORDERBOOK_BOOTSTRAP_MAX_RETRIES + 1):
            try:
                _bootstrap_orderbook_from_rest_snapshot_strict(sym)
                return
            except Exception as e:
                last_error = e
                log(
                    f"[MD_BINANCE_WS][ORDERBOOK][BOOTSTRAP_WORKER_FAIL] "
                    f"symbol={sym} attempt={attempt}/{_ORDERBOOK_BOOTSTRAP_MAX_RETRIES} "
                    f"error={type(e).__name__}: {e}"
                )
                if attempt >= _ORDERBOOK_BOOTSTRAP_MAX_RETRIES:
                    break
                time.sleep(_ORDERBOOK_BOOTSTRAP_RETRY_SEC)

        if last_error is not None:
            log(
                f"[MD_BINANCE_WS][ORDERBOOK][BOOTSTRAP_WORKER_GIVEUP] "
                f"symbol={sym} error={type(last_error).__name__}: {last_error}"
            )
    finally:
        with _ORDERBOOK_BOOTSTRAP_INFLIGHT_LOCK:
            _ORDERBOOK_BOOTSTRAP_INFLIGHT.discard(sym)


def _start_async_orderbook_bootstrap_if_needed(symbol: str, *, reason: str) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise WSProtocolError("symbol is required for async orderbook bootstrap (STRICT)")

    with _ORDERBOOK_BOOTSTRAP_INFLIGHT_LOCK:
        if sym in _ORDERBOOK_BOOTSTRAP_INFLIGHT:
            log(
                f"[MD_BINANCE_WS][ORDERBOOK][BOOTSTRAP_WORKER_SKIP] "
                f"symbol={sym} reason={reason} already_inflight=True"
            )
            return
        _ORDERBOOK_BOOTSTRAP_INFLIGHT.add(sym)

    th = threading.Thread(
        target=_run_orderbook_bootstrap_worker,
        args=(sym,),
        name=f"md-orderbook-bootstrap-{sym}",
        daemon=True,
    )
    try:
        th.start()
    except Exception:
        with _ORDERBOOK_BOOTSTRAP_INFLIGHT_LOCK:
            _ORDERBOOK_BOOTSTRAP_INFLIGHT.discard(sym)
        raise

    log(
        f"[MD_BINANCE_WS][ORDERBOOK][BOOTSTRAP_WORKER_START] "
        f"symbol={sym} reason={reason}"
    )


def _resync_orderbook_from_event_or_raise(symbol: str, *, event: Dict[str, Any], reason: str) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise WSProtocolError("symbol is required for orderbook resync (STRICT)")

    with _orderbook_lock:
        _replace_with_bootstrap_placeholder_locked(
            sym,
            seed_event=event,
            reason=reason,
        )

    log(f"[MD_BINANCE_WS][ORDERBOOK][RESYNC] symbol={sym} reason={reason}")
    _start_async_orderbook_bootstrap_if_needed(sym, reason=reason)


def _push_orderbook(symbol: str, payload: Dict[str, Any]) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise WSProtocolError("empty symbol in orderbook push")

    event = _normalize_orderbook_diff_event_strict(payload)
    now_ms = _now_ms()
    _mark_ws_message(sym, now_ms)

    need_resync = False
    resync_reason: Optional[str] = None

    with _orderbook_lock:
        state = _orderbook_book_state.get(sym)
        if not isinstance(state, dict):
            raise WSProtocolError("orderbook state missing/not dict (STRICT)")

        if bool(state.get("bootstrapping", False)):
            _queue_orderbook_pending_event_locked(sym, state, event)
            return

        if not bool(state.get("ready", False)):
            raise WSProtocolError("orderbook state not bootstrapped from REST snapshot (STRICT)")

        try:
            transition = _advance_orderbook_state_with_event_strict(sym, state, event)
        except OrderbookResyncRequired as e:
            need_resync = True
            resync_reason = str(e)
            _replace_with_bootstrap_placeholder_locked(
                sym,
                seed_event=event,
                reason=resync_reason,
            )
            transition = None

        if transition is None:
            if not need_resync:
                return
        else:
            next_state, ob_snapshot = transition
            _persist_ws_orderbook_snapshot_strict(
                symbol=sym,
                event_ts_ms=int(ob_snapshot["exchTs"]),
                bids=list(ob_snapshot["bids"]),
                asks=list(ob_snapshot["asks"]),
            )

            _orderbook_book_state[sym] = next_state
            _orderbook_buffers[sym] = ob_snapshot
            _orderbook_last_update_id[sym] = int(ob_snapshot["lastUpdateId"])
            _orderbook_last_recv_ts[sym] = now_ms
            return

    if need_resync:
        assert resync_reason is not None
        log(f"[MD_BINANCE_WS][ORDERBOOK][RESYNC_TRIGGER] symbol={sym} reason={resync_reason}")
        _start_async_orderbook_bootstrap_if_needed(sym, reason=resync_reason)
        return


def get_orderbook(symbol: str, limit: int = 5) -> Optional[Dict[str, Any]]:
    if limit <= 0:
        raise ValueError("limit must be > 0")

    sym = _normalize_symbol(symbol)
    with _orderbook_lock:
        ob = _orderbook_buffers.get(sym)
        if not ob:
            return None

        bids = ob.get("bids") or []
        asks = ob.get("asks") or []
        if not bids or not asks:
            return None

        result = dict(ob)

    result["bids"] = list((result.get("bids") or [])[:limit])
    result["asks"] = list((result.get("asks") or [])[:limit])
    return result


def get_orderbook_buffer_status(symbol: str) -> Dict[str, Any]:
    sym = _normalize_symbol(symbol)
    now_ms = _now_ms()

    with _orderbook_lock:
        ob = _orderbook_buffers.get(sym)
        last_recv_ts = _orderbook_last_recv_ts.get(sym)
        state = _orderbook_book_state.get(sym)

    payload_ts = None
    last_update_id = None
    has_orderbook = ob is not None
    orderbook_source = None

    if ob is not None:
        payload_ts = ob.get("ts")
        last_update_id = ob.get("lastUpdateId")
        orderbook_source = ob.get("source")

    recv_delay_ms = None
    if last_recv_ts is not None:
        recv_delay_ms = max(0, now_ms - int(last_recv_ts))

    payload_delay_ms = None
    if payload_ts is not None:
        payload_delay_ms = max(0, now_ms - int(payload_ts))

    stream_aligned = None
    snapshot_last_update_id = None
    bootstrapping = None
    resync_reason = None
    last_resync_ts = None

    if isinstance(state, dict):
        if "stream_aligned" in state:
            stream_aligned = bool(state.get("stream_aligned"))
        if state.get("snapshot_last_update_id") is not None:
            snapshot_last_update_id = int(state["snapshot_last_update_id"])
        if "bootstrapping" in state:
            bootstrapping = bool(state.get("bootstrapping"))
        resync_reason = state.get("resync_reason")
        last_resync_ts = state.get("last_resync_ts")

    return {
        "symbol": sym,
        "has_orderbook": has_orderbook,
        "last_recv_ts": last_recv_ts,
        "recv_delay_ms": recv_delay_ms,
        "payload_ts": payload_ts,
        "payload_delay_ms": payload_delay_ms,
        "last_update_id": last_update_id,
        "source": orderbook_source,
        "stream_aligned": stream_aligned,
        "snapshot_last_update_id": snapshot_last_update_id,
        "bootstrapping": bootstrapping,
        "resync_reason": resync_reason,
        "last_resync_ts": last_resync_ts,
    }


__all__ = [
    "_make_orderbook_bootstrap_placeholder_state",
    "_bootstrap_orderbook_from_rest_snapshot_strict",
    "_push_orderbook",
    "get_orderbook",
    "get_orderbook_buffer_status",
]