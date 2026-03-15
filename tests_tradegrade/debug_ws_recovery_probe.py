# -*- coding: utf-8 -*-
# tests_tradegrade/debug_ws_recovery_probe.py
"""
========================================================
FILE: tests_tradegrade/debug_ws_recovery_probe.py
ROLE:
- market_data_ws / orderbook / kline 복구 상태를 "소스 수정 없이" 런타임에서 정밀 진단한다.
- monkey-patch 기반으로 WS callback 흐름을 관측하고,
  stale orderbook / resync 반복 / replay backlog / kline gap repair를 구조적으로 기록한다.

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- 운영 소스 수정 금지
- 진단 스크립트 단독 실행
- 진단 결과(JSONL + 콘솔 요약) 확보 후에만 본 소스 수정

CHANGE HISTORY:
- 2026-03-15
  1) ADD: orderbook/kline/ws health 정밀 진단 스크립트 최초 추가
========================================================
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
import traceback
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import infra.market_data_ws as md
import infra.market_data_ws_health as md_health
import infra.market_data_ws_kline as md_kline
import infra.market_data_ws_orderbook as md_ob
import infra.market_data_ws_shared as shared
from infra.market_data_ws import start_ws_loop


class ProbeRuntimeError(RuntimeError):
    """Diagnostic runtime error."""


class JsonlRecorder:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def path(self) -> Path:
        return self._path

    def write(self, record: Dict[str, Any]) -> None:
        line = json.dumps(record, ensure_ascii=False, default=str)
        with self._lock:
            with self._path.open("a", encoding="utf-8") as fp:
                fp.write(line + "\n")


class DebugProbe:
    def __init__(self, *, symbol: str, seconds: int, poll_sec: float, recorder: JsonlRecorder) -> None:
        self.symbol = shared._normalize_symbol(symbol)
        if not self.symbol:
            raise ProbeRuntimeError("symbol is empty (STRICT)")
        if seconds < 0:
            raise ProbeRuntimeError("seconds must be >= 0 (STRICT)")
        if poll_sec <= 0:
            raise ProbeRuntimeError("poll_sec must be > 0 (STRICT)")

        self.seconds = int(seconds)
        self.poll_sec = float(poll_sec)
        self.recorder = recorder
        self.counters: Counter[str] = Counter()
        self._print_lock = threading.Lock()
        self._patched = False
        self._orig: Dict[str, Any] = {}
        self._last_summary_ms = 0
        self._last_suspect_key: Optional[str] = None

    def _utc_iso(self, ts_ms: Optional[int] = None) -> str:
        if ts_ms is None:
            ts_ms = shared._now_ms()
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        return dt.isoformat()

    def _safe_int(self, v: Any) -> Optional[int]:
        if v is None or isinstance(v, bool):
            return None
        try:
            return int(v)
        except Exception:
            return None

    def _copy_orderbook_runtime(self) -> Dict[str, Any]:
        sym = self.symbol
        with shared._orderbook_lock:
            state = shared._orderbook_book_state.get(sym)
            buf = shared._orderbook_buffers.get(sym)
            last_recv_ts = shared._orderbook_last_recv_ts.get(sym)
            last_update_id = shared._orderbook_last_update_id.get(sym)

            if isinstance(state, dict):
                pending = state.get("pending_events")
                pending_len = len(pending) if isinstance(pending, list) else None
                state_copy = {
                    "ready": bool(state.get("ready", False)),
                    "bootstrapping": bool(state.get("bootstrapping", False)),
                    "stream_aligned": bool(state.get("stream_aligned", False)),
                    "snapshot_last_update_id": state.get("snapshot_last_update_id"),
                    "state_last_update_id": state.get("last_update_id"),
                    "last_snapshot_recv_ts": state.get("last_snapshot_recv_ts"),
                    "resync_reason": state.get("resync_reason"),
                    "last_resync_ts": state.get("last_resync_ts"),
                    "pending_events_len": pending_len,
                }
            else:
                state_copy = {
                    "ready": None,
                    "bootstrapping": None,
                    "stream_aligned": None,
                    "snapshot_last_update_id": None,
                    "state_last_update_id": None,
                    "last_snapshot_recv_ts": None,
                    "resync_reason": None,
                    "last_resync_ts": None,
                    "pending_events_len": None,
                }

            buffer_copy = {
                "buffer_exists": isinstance(buf, dict),
                "buffer_ts": buf.get("ts") if isinstance(buf, dict) else None,
                "buffer_last_update_id": buf.get("lastUpdateId") if isinstance(buf, dict) else None,
                "buffer_source": buf.get("source") if isinstance(buf, dict) else None,
                "buffer_stream_aligned": buf.get("streamAligned") if isinstance(buf, dict) else None,
                "last_recv_ts": last_recv_ts,
                "shared_last_update_id": last_update_id,
            }

        return {**state_copy, **buffer_copy}

    def _emit(self, kind: str, **fields: Any) -> None:
        record = {
            "ts_ms": shared._now_ms(),
            "ts_utc": self._utc_iso(),
            "kind": kind,
            "symbol": self.symbol,
            **fields,
        }
        self.recorder.write(record)

        important = kind.startswith("SUSPECT_") or kind in {
            "orderbook_resync_enter",
            "orderbook_resync_clear",
            "orderbook_bootstrap_on_open_done",
            "orderbook_bootstrap_worker_done",
            "kline_gap_repair",
            "kline_gap_observed",
            "orderbook_push_error",
            "kline_push_error",
        }
        if important:
            with self._print_lock:
                print(json.dumps(record, ensure_ascii=False, default=str), flush=True)

    def _install_patches(self) -> None:
        if self._patched:
            return

        self._orig["md._push_orderbook"] = md._push_orderbook
        self._orig["md._push_kline"] = md._push_kline
        self._orig["md._bootstrap_orderbook_from_rest_snapshot_strict"] = md._bootstrap_orderbook_from_rest_snapshot_strict
        self._orig["md_ob._bootstrap_orderbook_from_rest_snapshot_strict"] = md_ob._bootstrap_orderbook_from_rest_snapshot_strict
        self._orig["md_ob._start_async_orderbook_bootstrap_if_needed"] = md_ob._start_async_orderbook_bootstrap_if_needed
        self._orig["md_kline._repair_kline_gap_from_rest_or_raise"] = md_kline._repair_kline_gap_from_rest_or_raise

        orig_push_orderbook = self._orig["md._push_orderbook"]
        orig_push_kline = self._orig["md._push_kline"]
        orig_bootstrap_on_open = self._orig["md._bootstrap_orderbook_from_rest_snapshot_strict"]
        orig_bootstrap_worker = self._orig["md_ob._bootstrap_orderbook_from_rest_snapshot_strict"]
        orig_start_async_bootstrap = self._orig["md_ob._start_async_orderbook_bootstrap_if_needed"]
        orig_kline_gap_repair = self._orig["md_kline._repair_kline_gap_from_rest_or_raise"]

        def wrapped_push_orderbook(symbol: str, payload: Dict[str, Any]) -> None:
            sym = shared._normalize_symbol(symbol)
            if sym != self.symbol:
                return orig_push_orderbook(symbol, payload)

            self.counters["ob_push_calls"] += 1
            pre = self._copy_orderbook_runtime()
            U = self._safe_int(payload.get("U"))
            u = self._safe_int(payload.get("u"))
            pu = self._safe_int(payload.get("pu"))
            event_ts_ms = self._safe_int(payload.get("E") or payload.get("T"))

            pre_last = pre.get("state_last_update_id")
            if pre_last is not None and u is not None and u <= int(pre_last):
                self.counters["ob_replay_candidate"] += 1
                if self.counters["ob_replay_candidate"] <= 20 or self.counters["ob_replay_candidate"] % 50 == 0:
                    self._emit(
                        "orderbook_replay_candidate",
                        U=U,
                        u=u,
                        pu=pu,
                        event_ts_ms=event_ts_ms,
                        pre_state_last_update_id=pre_last,
                        pre_buffer_last_update_id=pre.get("buffer_last_update_id"),
                    )

            try:
                return orig_push_orderbook(symbol, payload)
            except Exception as e:
                self.counters["ob_push_error"] += 1
                self._emit(
                    "orderbook_push_error",
                    U=U,
                    u=u,
                    pu=pu,
                    event_ts_ms=event_ts_ms,
                    error=f"{type(e).__name__}: {e}",
                    traceback=traceback.format_exc(),
                    pre=pre,
                    post=self._copy_orderbook_runtime(),
                )
                raise
            finally:
                post = self._copy_orderbook_runtime()

                if pre.get("resync_reason") != post.get("resync_reason"):
                    if post.get("resync_reason"):
                        self.counters["ob_resync_enter"] += 1
                        self._emit(
                            "orderbook_resync_enter",
                            U=U,
                            u=u,
                            pu=pu,
                            event_ts_ms=event_ts_ms,
                            reason=post.get("resync_reason"),
                            pre=pre,
                            post=post,
                        )
                    else:
                        self.counters["ob_resync_clear"] += 1
                        self._emit(
                            "orderbook_resync_clear",
                            U=U,
                            u=u,
                            pu=pu,
                            event_ts_ms=event_ts_ms,
                            pre=pre,
                            post=post,
                        )

                if pre.get("bootstrapping") != post.get("bootstrapping"):
                    self._emit(
                        "orderbook_bootstrapping_change",
                        U=U,
                        u=u,
                        pu=pu,
                        event_ts_ms=event_ts_ms,
                        pre_bootstrapping=pre.get("bootstrapping"),
                        post_bootstrapping=post.get("bootstrapping"),
                    )

                if pre.get("stream_aligned") != post.get("stream_aligned"):
                    self.counters["ob_stream_aligned_change"] += 1
                    self._emit(
                        "orderbook_stream_aligned_change",
                        U=U,
                        u=u,
                        pu=pu,
                        event_ts_ms=event_ts_ms,
                        pre_stream_aligned=pre.get("stream_aligned"),
                        post_stream_aligned=post.get("stream_aligned"),
                        post=post,
                    )

                if pre.get("state_last_update_id") != post.get("state_last_update_id"):
                    self.counters["ob_last_update_advanced"] += 1

        def wrapped_push_kline(symbol: str, interval: str, kline_obj: Dict[str, Any]) -> None:
            sym = shared._normalize_symbol(symbol)
            iv = shared._normalize_interval(interval)
            if sym != self.symbol:
                return orig_push_kline(symbol, interval, kline_obj)

            self.counters["kline_push_calls"] += 1
            pre_last_ts = md_kline.get_last_kline_ts(sym, iv)
            new_ts = self._safe_int(kline_obj.get("t"))
            is_closed = bool(kline_obj.get("x"))

            if pre_last_ts is not None and new_ts is not None and new_ts > pre_last_ts:
                expected_ms = shared._interval_to_ms_strict(iv)
                delta_ms = int(new_ts) - int(pre_last_ts)
                if delta_ms > expected_ms:
                    self.counters["kline_gap_observed"] += 1
                    self._emit(
                        "kline_gap_observed",
                        interval=iv,
                        prev_ts=pre_last_ts,
                        new_ts=new_ts,
                        delta_ms=delta_ms,
                        expected_ms=expected_ms,
                        is_closed=is_closed,
                    )

            try:
                return orig_push_kline(symbol, interval, kline_obj)
            except Exception as e:
                self.counters["kline_push_error"] += 1
                self._emit(
                    "kline_push_error",
                    interval=iv,
                    new_ts=new_ts,
                    error=f"{type(e).__name__}: {e}",
                    traceback=traceback.format_exc(),
                )
                raise

        def wrapped_bootstrap_on_open(symbol: str) -> None:
            sym = shared._normalize_symbol(symbol)
            if sym != self.symbol:
                return orig_bootstrap_on_open(symbol)
            self.counters["ob_bootstrap_on_open"] += 1
            self._emit("orderbook_bootstrap_on_open_start", state=self._copy_orderbook_runtime())
            try:
                return orig_bootstrap_on_open(symbol)
            finally:
                self._emit("orderbook_bootstrap_on_open_done", state=self._copy_orderbook_runtime())

        def wrapped_bootstrap_worker(symbol: str) -> None:
            sym = shared._normalize_symbol(symbol)
            if sym != self.symbol:
                return orig_bootstrap_worker(symbol)
            self.counters["ob_bootstrap_worker"] += 1
            self._emit("orderbook_bootstrap_worker_start", state=self._copy_orderbook_runtime())
            try:
                return orig_bootstrap_worker(symbol)
            finally:
                self._emit("orderbook_bootstrap_worker_done", state=self._copy_orderbook_runtime())

        def wrapped_start_async_bootstrap(symbol: str, *, reason: str) -> None:
            sym = shared._normalize_symbol(symbol)
            if sym != self.symbol:
                return orig_start_async_bootstrap(symbol, reason=reason)
            self.counters["ob_async_bootstrap_requested"] += 1
            self._emit(
                "orderbook_async_bootstrap_request",
                reason=reason,
                state=self._copy_orderbook_runtime(),
            )
            return orig_start_async_bootstrap(symbol, reason=reason)

        def wrapped_kline_gap_repair(symbol: str, interval: str, *, min_limit: int, reason: str) -> None:
            sym = shared._normalize_symbol(symbol)
            iv = shared._normalize_interval(interval)
            if sym != self.symbol:
                return orig_kline_gap_repair(symbol, interval, min_limit=min_limit, reason=reason)
            self.counters["kline_gap_repair"] += 1
            self._emit(
                "kline_gap_repair",
                interval=iv,
                min_limit=min_limit,
                reason=reason,
            )
            return orig_kline_gap_repair(symbol, interval, min_limit=min_limit, reason=reason)

        md._push_orderbook = wrapped_push_orderbook
        md._push_kline = wrapped_push_kline
        md._bootstrap_orderbook_from_rest_snapshot_strict = wrapped_bootstrap_on_open
        md_ob._bootstrap_orderbook_from_rest_snapshot_strict = wrapped_bootstrap_worker
        md_ob._start_async_orderbook_bootstrap_if_needed = wrapped_start_async_bootstrap
        md_kline._repair_kline_gap_from_rest_or_raise = wrapped_kline_gap_repair
        self._patched = True

    def _restore_patches(self) -> None:
        if not self._patched:
            return
        md._push_orderbook = self._orig["md._push_orderbook"]
        md._push_kline = self._orig["md._push_kline"]
        md._bootstrap_orderbook_from_rest_snapshot_strict = self._orig["md._bootstrap_orderbook_from_rest_snapshot_strict"]
        md_ob._bootstrap_orderbook_from_rest_snapshot_strict = self._orig["md_ob._bootstrap_orderbook_from_rest_snapshot_strict"]
        md_ob._start_async_orderbook_bootstrap_if_needed = self._orig["md_ob._start_async_orderbook_bootstrap_if_needed"]
        md_kline._repair_kline_gap_from_rest_or_raise = self._orig["md_kline._repair_kline_gap_from_rest_or_raise"]
        self._patched = False

    def _maybe_emit_suspects(self) -> None:
        now_ms = shared._now_ms()
        ob = md_ob.get_orderbook_buffer_status(self.symbol)
        health = md_health.get_health_snapshot(self.symbol)

        resync_reason = ob.get("resync_reason")
        last_resync_ts = ob.get("last_resync_ts")
        stream_aligned = ob.get("stream_aligned")
        bootstrapping = ob.get("bootstrapping")
        payload_delay_ms = ob.get("payload_delay_ms")
        recv_delay_ms = ob.get("recv_delay_ms")

        suspect_key: Optional[str] = None
        suspect_reason: Optional[str] = None

        if (
            stream_aligned is True
            and bootstrapping is False
            and isinstance(resync_reason, str)
            and resync_reason.strip()
            and last_resync_ts is not None
        ):
            resync_age_sec = max(0.0, (now_ms - int(last_resync_ts)) / 1000.0)
            if resync_age_sec >= 10.0 and (recv_delay_ms is None or int(recv_delay_ms) < 3000):
                suspect_key = "SUSPECT_RESYNC_FLAG_PERSIST"
                suspect_reason = (
                    f"stream_aligned=True 인데 resync_reason 이 {resync_age_sec:.1f}s 동안 유지됩니다. "
                    "recovery flag clear 누락 가능성이 높습니다."
                )

        if suspect_key is None and self.counters["ob_replay_candidate"] >= 50:
            if payload_delay_ms is not None and int(payload_delay_ms) >= int(shared.MARKET_EVENT_MAX_DELAY_SEC * 1000):
                suspect_key = "SUSPECT_REPLAY_BACKLOG_AFTER_RESYNC"
                suspect_reason = (
                    "snapshot bootstrap 이후 stale/replay candidate 가 과도하게 누적됩니다. "
                    "같은 WS 세션에서 낡은 diff backlog 를 계속 소비하는 구조일 가능성이 높습니다."
                )

        if suspect_key is None and not bool(health.get("overall_ok", True)):
            overall_reasons = list(health.get("overall_reasons") or [])
            joined = " | ".join(str(x) for x in overall_reasons if str(x).strip())
            suspect_key = "SUSPECT_HEALTH_FAIL"
            suspect_reason = joined or "health overall_ok=False"

        if suspect_key is not None and suspect_key != self._last_suspect_key:
            self._last_suspect_key = suspect_key
            self._emit(
                suspect_key,
                reason=suspect_reason,
                counters=dict(self.counters),
                orderbook=ob,
            )

    def _print_summary(self) -> None:
        now_ms = shared._now_ms()
        if self._last_summary_ms and (now_ms - self._last_summary_ms) < int(self.poll_sec * 1000):
            return
        self._last_summary_ms = now_ms

        ws = shared.get_ws_status(self.symbol)
        health = md_health.get_health_snapshot(self.symbol)
        ob = md_ob.get_orderbook_buffer_status(self.symbol)
        k1 = md_kline.get_kline_buffer_status(self.symbol, "1m")
        k5 = md_kline.get_kline_buffer_status(self.symbol, "5m")

        def _fmt_age_ms(v: Any) -> str:
            if v is None:
                return "-"
            try:
                return f"{int(v) / 1000.0:.1f}s"
            except Exception:
                return "?"

        summary = (
            f"[SUMMARY] t={self._utc_iso()} "
            f"ws={ws.get('state')} tx_ok={ws.get('transport_ok')} feed_ok={ws.get('market_feed_ok')} "
            f"health_ok={health.get('overall_ok')} warn={health.get('has_warning')} "
            f"1m_last={k1.get('last_ts')} 1m_age={_fmt_age_ms(k1.get('delay_ms'))} "
            f"5m_last={k5.get('last_ts')} 5m_age={_fmt_age_ms(k5.get('delay_ms'))} "
            f"ob_last_u={ob.get('last_update_id')} ob_payload_age={_fmt_age_ms(ob.get('payload_delay_ms'))} "
            f"ob_recv_age={_fmt_age_ms(ob.get('recv_delay_ms'))} "
            f"aligned={ob.get('stream_aligned')} boot={ob.get('bootstrapping')} "
            f"resync={'Y' if ob.get('resync_reason') else 'N'} "
            f"replay={self.counters.get('ob_replay_candidate', 0)} "
            f"resync_enter={self.counters.get('ob_resync_enter', 0)} "
            f"kline_repairs={self.counters.get('kline_gap_repair', 0)}"
        )
        with self._print_lock:
            print(summary, flush=True)

    def run(self) -> None:
        self._install_patches()
        self._emit("probe_start", seconds=self.seconds, poll_sec=self.poll_sec, jsonl=str(self.recorder.path))
        start_ws_loop(self.symbol)

        end_ts = None if self.seconds == 0 else (time.time() + self.seconds)

        try:
            while True:
                self._print_summary()
                self._maybe_emit_suspects()
                if end_ts is not None and time.time() >= end_ts:
                    break
                time.sleep(self.poll_sec)
        except KeyboardInterrupt:
            self._emit("probe_keyboard_interrupt")
        finally:
            self._emit("probe_end", counters=dict(self.counters))
            self._restore_patches()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="WS orderbook/kline recovery probe")
    parser.add_argument("--symbol", default="BTCUSDT", help="target symbol")
    parser.add_argument("--seconds", type=int, default=180, help="0이면 Ctrl+C까지 무한 실행")
    parser.add_argument("--poll-sec", type=float, default=1.0, help="summary polling interval")
    parser.add_argument(
        "--jsonl",
        default="",
        help="진단 JSONL 출력 경로. 비우면 ./_debug/ws_recovery_probe_<ts>.jsonl",
    )
    return parser.parse_args()


def build_output_path(raw: str) -> Path:
    if raw:
        return Path(raw).resolve()
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return (ROOT / "_debug" / f"ws_recovery_probe_{ts}.jsonl").resolve()


def main() -> None:
    args = parse_args()
    out_path = build_output_path(args.jsonl)
    recorder = JsonlRecorder(out_path)
    probe = DebugProbe(
        symbol=args.symbol,
        seconds=args.seconds,
        poll_sec=args.poll_sec,
        recorder=recorder,
    )
    probe.run()


if __name__ == "__main__":
    main()
