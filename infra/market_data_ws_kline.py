# -*- coding: utf-8 -*-
# infra/market_data_ws_kline.py
"""
========================================================
FILE: infra/market_data_ws_kline.py
ROLE:
- market_data_ws 분리 리팩터링 kline 전용 계층
- REST bootstrap / WS kline push / buffer 상태 조회를 담당한다
- 상위 계층에는 기존 market_data_ws 공개 API와 동일한 kline getter를 제공한다

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- 거래소 정상 gap(복수 interval rollover)은 protocol fatal로 보지 않고
  authoritative REST backfill로 복구한다
- interval 미만 delta / non-aligned delta / timestamp rollback 만 치명 오류다
========================================================

CHANGE HISTORY:
- 2026-03-15
  1) FIX(ROOT-CAUSE): WS kline rollover delta 가 expected interval 과 정확히 같아야만 허용하던 로직 제거
  2) FIX(RECOVERY): delta_ms 가 interval 배수인 복수-candle gap 은 recoverable gap 으로 판단
  3) FIX(RECOVERY): recoverable kline gap 발생 시 REST klines backfill 로 buffer 재동기화 후 동일 WS row 재처리
  4) FIX(OPERABILITY): recoverable gap 을 protocol fatal 로 WS disconnect 하지 않고 self-heal 경로로 전환
========================================================
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Tuple

from infra.market_data_store import save_candle_from_ws
from infra.market_data_ws_shared import (
    KlineRow,
    LegacyKlineRow,
    LegacyKlineRowWithVolume,
    MAX_KLINES,
    SET,
    WS_BACKFILL_INTERVALS,
    WSProtocolError,
    _NO_BUF_LOG_SUPPRESS_SEC,
    _dedupe_keep_order,
    _derive_rest_kline_closed_strict,
    _extract_quote_volume_from_rest_row_optional,
    _interval_to_ms_strict,
    _kline_buffers,
    _kline_last_quote_volume,
    _kline_last_recv_ts,
    _kline_lock,
    _min_buffer_for_interval,
    _no_buf_log_last,
    _no_buf_log_lock,
    _normalize_interval,
    _normalize_symbol,
    _now_ms,
    _require_bool,
    _require_float,
    _require_int_ms,
    _rest_fetch_klines_strict,
    log,
)

_KLINE_GAP_REPAIR_MAX_ATTEMPTS: int = 2


def _classify_same_candle_update_strict(
    prev: KlineRow,
    new_row: KlineRow,
    *,
    prev_quote_volume: Optional[float],
    new_quote_volume: float,
    context: str,
) -> bool:
    prev_ts, prev_o, prev_h, prev_l, _prev_c, prev_v, prev_closed = prev
    new_ts, new_o, new_h, new_l, _new_c, new_v, new_closed = new_row

    if new_ts != prev_ts:
        raise WSProtocolError(f"{context} ts mismatch during same-candle update (STRICT)")
    if prev_closed and not new_closed:
        raise WSProtocolError(f"{context} closed candle cannot reopen (STRICT)")
    if abs(new_o - prev_o) > 1e-12:
        raise WSProtocolError(f"{context} open price changed within same candle (STRICT)")
    if new_h + 1e-12 < prev_h:
        log(f"[WS_ANOMALY] {context} high rollback prev={prev_h} new={new_h}")
        return False
    if new_l - 1e-12 > prev_l:
        log(f"[WS_ANOMALY] {context} low rollback prev={prev_l} new={new_l}")
        return False
    if new_v + 1e-12 < prev_v:
        log(f"[WS_ANOMALY] {context} volume rollback prev={prev_v} new={new_v}")
        return False
    if prev_quote_volume is not None and new_quote_volume + 1e-12 < prev_quote_volume:
        log(f"[WS_ANOMALY] {context} quote_volume rollback prev={prev_quote_volume} new={new_quote_volume}")
        return False
    return True


def _validate_rollover_delta_strict(
    *,
    last_ts: int,
    new_ts: int,
    interval: str,
    context: str,
) -> Tuple[int, int]:
    interval_ms = _interval_to_ms_strict(interval)
    delta_ms = int(new_ts) - int(last_ts)

    if delta_ms <= 0:
        raise WSProtocolError(
            f"{context} rollover delta must be > 0 (STRICT): prev_ts={last_ts} new_ts={new_ts} delta_ms={delta_ms}"
        )

    if delta_ms < interval_ms:
        raise WSProtocolError(
            f"{context} rollover delta shorter than interval (STRICT): "
            f"prev_ts={last_ts} new_ts={new_ts} delta_ms={delta_ms} expected_ms={interval_ms}"
        )

    if (delta_ms % interval_ms) != 0:
        raise WSProtocolError(
            f"{context} rollover delta not aligned to interval (STRICT): "
            f"prev_ts={last_ts} new_ts={new_ts} delta_ms={delta_ms} expected_ms={interval_ms}"
        )

    return interval_ms, delta_ms


def _repair_kline_gap_from_rest_or_raise(
    symbol: str,
    interval: str,
    *,
    min_limit: int,
    reason: str,
) -> None:
    sym = _normalize_symbol(symbol)
    iv = _normalize_interval(interval)

    if not sym:
        raise WSProtocolError("symbol is required for kline gap repair (STRICT)")
    if not iv:
        raise WSProtocolError("interval is required for kline gap repair (STRICT)")
    if min_limit <= 0:
        raise WSProtocolError(f"kline gap repair min_limit must be > 0 (STRICT), got={min_limit}")

    fetch_limit = max(int(min_limit), int(_min_buffer_for_interval(iv)))
    payload = _rest_fetch_klines_strict(sym, iv, fetch_limit)
    if not isinstance(payload, list) or not payload:
        raise WSProtocolError(
            f"REST gap repair returned empty/non-list payload (STRICT): symbol={sym} interval={iv}"
        )

    backfill_klines_from_rest(sym, iv, payload)
    log(
        f"[WS_RECOVERY] ws_kline[{sym}:{iv}] gap repaired from REST "
        f"loaded={len(payload)} reason={reason}"
    )


def _plan_kline_update_strict(
    buf: List[KlineRow],
    row: KlineRow,
    *,
    interval: str,
    quote_volume: float,
    prev_quote_volume: Optional[float],
    context: str,
) -> Tuple[bool, Optional[KlineRow], Optional[str]]:
    _ = quote_volume
    ts = int(row[0])
    if not buf:
        return True, None, None

    last = buf[-1]
    last_ts = int(last[0])

    if ts < last_ts:
        log(f"[WS_INFO] {context} stale kline ignored new_ts={ts} last_ts={last_ts}")
        return False, None, None

    if ts == last_ts:
        should_apply = _classify_same_candle_update_strict(
            last,
            row,
            prev_quote_volume=prev_quote_volume,
            new_quote_volume=quote_volume,
            context=context,
        )
        return should_apply, None, None

    if bool(last[6]) is False:
        interval_ms, delta_ms = _validate_rollover_delta_strict(
            last_ts=last_ts,
            new_ts=ts,
            interval=interval,
            context=context,
        )

        if delta_ms > interval_ms:
            repair_reason = (
                f"{context} rollover gap detected (STRICT-RECOVERABLE): "
                f"prev_ts={last_ts} new_ts={ts} delta_ms={delta_ms} expected_ms={interval_ms}"
            )
            return False, None, repair_reason

        sealed_prev: KlineRow = (
            int(last[0]),
            float(last[1]),
            float(last[2]),
            float(last[3]),
            float(last[4]),
            float(last[5]),
            True,
        )
        return True, sealed_prev, None

    return True, None, None


def _commit_kline_update_strict(
    buf: List[KlineRow],
    row: KlineRow,
    *,
    key: Tuple[str, str],
    quote_volume: float,
    should_apply: bool,
    sealed_prev: Optional[KlineRow],
    context: str,
) -> None:
    if not should_apply:
        return

    ts = int(row[0])

    if not buf:
        buf.append(row)
        _kline_last_quote_volume[key] = (ts, float(quote_volume))
        return

    last = buf[-1]
    last_ts = int(last[0])

    if ts < last_ts:
        raise WSProtocolError(f"{context} kline timestamp rollback on commit (STRICT): new_ts={ts} last_ts={last_ts}")

    if ts == last_ts:
        buf[-1] = row
        _kline_last_quote_volume[key] = (ts, float(quote_volume))
        return

    if sealed_prev is not None:
        if int(buf[-1][0]) != int(sealed_prev[0]):
            raise WSProtocolError(f"{context} sealed prev ts mismatch on commit (STRICT)")
        buf[-1] = sealed_prev
        log(f"[WS_RECOVERY] {context} previous open candle sealed on rollover prev_ts={sealed_prev[0]} new_ts={ts}")

    buf.append(row)
    if len(buf) > MAX_KLINES:
        del buf[0: len(buf) - MAX_KLINES]
    _kline_last_quote_volume[key] = (ts, float(quote_volume))


def _persist_ws_kline_strict(
    *,
    symbol: str,
    interval: str,
    ts_ms: int,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: float,
    quote_volume: float,
    is_closed: bool,
) -> None:
    save_candle_from_ws(
        symbol=symbol,
        interval=interval,
        ts_ms=ts_ms,
        open_=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
        quote_volume=quote_volume,
        source="ws",
        is_closed=is_closed,
    )


def _push_kline(symbol: str, interval: str, kline_obj: Dict[str, Any]) -> None:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise WSProtocolError("empty symbol in kline push")

    iv = _normalize_interval(interval)
    if not iv:
        raise WSProtocolError("empty interval in kline push")

    key = (sym, iv)

    ts = _require_int_ms(kline_obj.get("t"), "kline.t")
    o = _require_float(kline_obj.get("o"), "kline.o")
    h = _require_float(kline_obj.get("h"), "kline.h")
    l = _require_float(kline_obj.get("l"), "kline.l")
    c = _require_float(kline_obj.get("c"), "kline.c")
    v = _require_float(kline_obj.get("v"), "kline.v", allow_zero=True)
    q = _require_float(kline_obj.get("q"), "kline.q", allow_zero=True)
    is_closed = _require_bool(kline_obj.get("x"), "kline.x")

    row: KlineRow = (ts, o, h, l, c, v, is_closed)
    now_ms = _now_ms()

    should_apply: bool = False
    sealed_prev: Optional[KlineRow] = None
    gap_repair_reason: Optional[str] = None

    for attempt in range(1, _KLINE_GAP_REPAIR_MAX_ATTEMPTS + 2):
        with _kline_lock:
            buf = _kline_buffers.setdefault(key, [])
            prev_q_entry = _kline_last_quote_volume.get(key)
            prev_quote_volume: Optional[float] = None
            if prev_q_entry is not None and int(prev_q_entry[0]) == int(buf[-1][0] if buf else ts):
                prev_quote_volume = float(prev_q_entry[1])

            should_apply, sealed_prev, gap_repair_reason = _plan_kline_update_strict(
                buf,
                row,
                interval=iv,
                quote_volume=q,
                prev_quote_volume=prev_quote_volume,
                context=f"ws_kline[{sym}:{iv}]",
            )
            repair_limit = max(len(buf), _min_buffer_for_interval(iv), 2)

        if gap_repair_reason is None:
            break

        if attempt > _KLINE_GAP_REPAIR_MAX_ATTEMPTS:
            raise WSProtocolError(
                f"{gap_repair_reason} | repair_attempts_exceeded={_KLINE_GAP_REPAIR_MAX_ATTEMPTS}"
            )

        _repair_kline_gap_from_rest_or_raise(
            sym,
            iv,
            min_limit=repair_limit,
            reason=gap_repair_reason,
        )

    if sealed_prev is not None:
        if prev_quote_volume is None:
            raise WSProtocolError(
                f"ws_kline[{sym}:{iv}] rollover close missing previous quote_volume tracking (STRICT)"
            )
        _persist_ws_kline_strict(
            symbol=sym,
            interval=iv,
            ts_ms=int(sealed_prev[0]),
            open_=float(sealed_prev[1]),
            high=float(sealed_prev[2]),
            low=float(sealed_prev[3]),
            close=float(sealed_prev[4]),
            volume=float(sealed_prev[5]),
            quote_volume=float(prev_quote_volume),
            is_closed=True,
        )

    if should_apply:
        _persist_ws_kline_strict(
            symbol=sym,
            interval=iv,
            ts_ms=ts,
            open_=o,
            high=h,
            low=l,
            close=c,
            volume=v,
            quote_volume=q,
            is_closed=is_closed,
        )

    with _kline_lock:
        buf = _kline_buffers.setdefault(key, [])
        _commit_kline_update_strict(
            buf,
            row,
            key=key,
            quote_volume=q,
            should_apply=should_apply,
            sealed_prev=sealed_prev,
            context=f"ws_kline[{sym}:{iv}]",
        )
        if should_apply:
            _kline_last_recv_ts[key] = now_ms


def preload_klines(symbol: str, interval: str, rows: List[KlineRow]) -> None:
    sym = _normalize_symbol(symbol)
    iv = _normalize_interval(interval)
    if not sym:
        raise ValueError("symbol is required")
    if not iv:
        raise ValueError("interval is required")

    cleaned: List[KlineRow] = []
    last_ts: Optional[int] = None
    for i, r in enumerate(rows):
        if not isinstance(r, (list, tuple)) or len(r) != 7:
            raise RuntimeError(f"preload row[{i}] must be 7-tuple(ts,o,h,l,c,v,is_closed) (STRICT)")
        ts = _require_int_ms(r[0], f"preload[{i}].ts")
        o = _require_float(r[1], f"preload[{i}].o")
        h = _require_float(r[2], f"preload[{i}].h")
        l = _require_float(r[3], f"preload[{i}].l")
        c = _require_float(r[4], f"preload[{i}].c")
        v = _require_float(r[5], f"preload[{i}].v", allow_zero=True)
        is_closed = _require_bool(r[6], f"preload[{i}].is_closed")
        if last_ts is not None and ts <= last_ts:
            raise RuntimeError(
                f"preload rows must be strictly increasing by ts (STRICT): prev_ts={last_ts} now_ts={ts} idx={i}"
            )
        cleaned.append((ts, o, h, l, c, v, is_closed))
        last_ts = ts

    key = (sym, iv)
    with _kline_lock:
        trimmed = list(cleaned[-MAX_KLINES:])
        _kline_buffers[key] = trimmed
        if trimmed:
            _kline_last_recv_ts[key] = _now_ms()


def backfill_klines_from_rest(symbol: str, interval: str, rest_klines: List[Any]) -> None:
    sym = _normalize_symbol(symbol)
    iv = _normalize_interval(interval)
    if not sym:
        raise ValueError("symbol is required")
    if not iv:
        raise ValueError("interval is required")

    if not isinstance(rest_klines, list):
        raise RuntimeError("rest_klines must be list (STRICT)")
    if not rest_klines:
        raise RuntimeError("rest_klines empty (STRICT)")

    now_ms = _now_ms()
    converted: List[KlineRow] = []
    quote_volume_by_ts: Dict[int, float] = {}

    for idx, row in enumerate(rest_klines):
        quote_volume_optional = _extract_quote_volume_from_rest_row_optional(row, idx)

        if isinstance(row, (list, tuple)):
            if len(row) < 7:
                raise RuntimeError(f"rest_klines[{idx}] invalid len<7 (STRICT)")
            ts = _require_int_ms(row[0], f"rest_klines[{idx}].ts")
            o = _require_float(row[1], f"rest_klines[{idx}].o")
            h = _require_float(row[2], f"rest_klines[{idx}].h")
            l = _require_float(row[3], f"rest_klines[{idx}].l")
            c = _require_float(row[4], f"rest_klines[{idx}].c")
            v = _require_float(row[5], f"rest_klines[{idx}].v", allow_zero=True)
            is_closed = _derive_rest_kline_closed_strict(row, idx, now_ms)
            converted.append((ts, o, h, l, c, v, is_closed))
            if quote_volume_optional is not None:
                quote_volume_by_ts[ts] = quote_volume_optional
            continue

        if isinstance(row, dict):
            ts = _require_int_ms(row.get("t") or row.get("openTime") or row.get("T"), f"rest_klines[{idx}].ts")
            o = _require_float(row.get("o") or row.get("open"), f"rest_klines[{idx}].o")
            h = _require_float(row.get("h") or row.get("high"), f"rest_klines[{idx}].h")
            l = _require_float(row.get("l") or row.get("low"), f"rest_klines[{idx}].l")
            c = _require_float(row.get("c") or row.get("close"), f"rest_klines[{idx}].c")
            v = _require_float(row.get("v") or row.get("volume"), f"rest_klines[{idx}].v", allow_zero=True)
            is_closed = _derive_rest_kline_closed_strict(row, idx, now_ms)
            converted.append((ts, o, h, l, c, v, is_closed))
            if quote_volume_optional is not None:
                quote_volume_by_ts[ts] = quote_volume_optional
            continue

        raise RuntimeError(f"rest_klines[{idx}] invalid type (STRICT): {type(row).__name__}")

    converted.sort(key=lambda x: x[0])
    preload_klines(sym, iv, converted)

    key = (sym, iv)
    latest_ts = int(converted[-1][0])
    latest_quote_volume = quote_volume_by_ts.get(latest_ts)

    with _kline_lock:
        if latest_quote_volume is not None:
            _kline_last_quote_volume[key] = (latest_ts, float(latest_quote_volume))
        else:
            _kline_last_quote_volume.pop(key, None)


def bootstrap_klines_from_rest_strict(symbol: str, intervals: Optional[List[str]] = None) -> Dict[str, int]:
    from infra.market_data_ws_shared import _WS_BOOTSTRAP_REST_ENABLED  # local import to keep API narrow

    if not _WS_BOOTSTRAP_REST_ENABLED:
        raise RuntimeError("ws_bootstrap_rest_enabled is False. TRADE-GRADE startup requires explicit bootstrap.")

    sym = _normalize_symbol(symbol)
    if not sym:
        raise RuntimeError("symbol is required for REST bootstrap")

    targets = list(intervals or WS_BACKFILL_INTERVALS)
    targets = [_normalize_interval(x) for x in targets if _normalize_interval(x)]
    if not targets:
        raise RuntimeError("REST bootstrap intervals empty (STRICT)")

    loaded: Dict[str, int] = {}
    for iv in targets:
        need = _min_buffer_for_interval(iv)
        payload = _rest_fetch_klines_strict(sym, iv, need)
        backfill_klines_from_rest(sym, iv, payload)
        loaded[iv] = len(payload)
    return loaded


def _log_no_buffer_once(symbol: str, interval: str, requested: int) -> None:
    if not SET.ws_log_enabled:
        return

    now = time.time()
    key = (_normalize_symbol(symbol), _normalize_interval(interval))
    with _no_buf_log_lock:
        last = _no_buf_log_last.get(key, 0.0)
        if (now - last) < _NO_BUF_LOG_SUPPRESS_SEC:
            return
        _no_buf_log_last[key] = now

    log(f"[MD_BINANCE_WS KLINES] no kline buffer for {symbol} {interval} (requested={requested})")


def get_klines_with_volume_and_closed(symbol: str, interval: str, limit: int = 300) -> List[KlineRow]:
    if limit <= 0:
        raise ValueError("limit must be > 0")

    sym = _normalize_symbol(symbol)
    iv = _normalize_interval(interval)
    key = (sym, iv)

    with _kline_lock:
        buf = _kline_buffers.get(key, [])
        if not buf:
            _log_no_buffer_once(sym, iv, limit)
            return []
        return list(buf[-limit:])


def get_klines_with_volume(symbol: str, interval: str, limit: int = 300) -> List[LegacyKlineRowWithVolume]:
    rows = get_klines_with_volume_and_closed(symbol, interval, limit=limit)
    return [(ts, o, h, l, c, v) for (ts, o, h, l, c, v, _is_closed) in rows]


def get_klines(symbol: str, interval: str, limit: Optional[int] = None) -> List[LegacyKlineRow]:
    if limit is None:
        normalized_limit = max(300, _min_buffer_for_interval(interval))
    else:
        if limit <= 0:
            raise ValueError("limit must be > 0")
        normalized_limit = limit
    rows = get_klines_with_volume(symbol, interval, limit=normalized_limit)
    return [(ts, o, h, l, c) for (ts, o, h, l, c, _v) in rows]


def get_last_kline_ts(symbol: str, interval: str) -> Optional[int]:
    sym = _normalize_symbol(symbol)
    iv = _normalize_interval(interval)
    key = (sym, iv)
    with _kline_lock:
        buf = _kline_buffers.get(key, [])
        if not buf:
            return None
        return int(buf[-1][0])


def get_last_kline_delay_ms(symbol: str, interval: str) -> Optional[int]:
    sym = _normalize_symbol(symbol)
    iv = _normalize_interval(interval)
    key = (sym, iv)
    now = _now_ms()
    with _kline_lock:
        recv_ts = _kline_last_recv_ts.get(key)
    if recv_ts is None:
        return None
    return max(0, now - int(recv_ts))


def get_kline_buffer_status(symbol: str, interval: str) -> Dict[str, Any]:
    sym = _normalize_symbol(symbol)
    iv = _normalize_interval(interval)
    key = (sym, iv)

    with _kline_lock:
        buf = _kline_buffers.get(key, [])
        last_ts = buf[-1][0] if buf else None
        last_recv = _kline_last_recv_ts.get(key)

    delay_ms = None
    if last_recv is not None:
        delay_ms = max(0, _now_ms() - int(last_recv))

    return {
        "symbol": sym,
        "interval": iv,
        "buffer_len": len(buf),
        "last_ts": last_ts,
        "last_recv_ts": last_recv,
        "delay_ms": delay_ms,
    }


__all__ = [
    "bootstrap_klines_from_rest_strict",
    "backfill_klines_from_rest",
    "preload_klines",
    "get_klines_with_volume_and_closed",
    "get_klines_with_volume",
    "get_klines",
    "get_last_kline_ts",
    "get_last_kline_delay_ms",
    "get_kline_buffer_status",
    "_push_kline",
]