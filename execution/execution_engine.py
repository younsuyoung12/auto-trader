# execution/execution_engine.py
"""
========================================================
execution/execution_engine.py
STRICT · NO-FALLBACK · PRODUCTION MODE
========================================================
설계 원칙:
- ExecutionEngine은 "실행"만 담당한다.
- strategy 레이어가 만든 Signal을 받아:
  1) (필요 시) 엔트리 가드 검사
  2) 하드 리스크 가드 검사
  3) 주문 실행(order_executor)
  4) DB 기록(bt_trades + bt_trade_snapshots)
- 폴백(REST 백필/더미 값/임의 보정) 절대 금지.
- 예외 삼키기 금지: 모든 오류는 로그 남기고 예외 발생(혹은 정상적인 SKIP).
- 민감정보(키/토큰/시크릿)는 로그/출력 금지.
- settings 객체를 런타임에 mutate 하지 않는다(가드 조정은 read-only view로 적용).

전액 배분형(Allocation Mode)
--------------------------------------------------------
- signal.risk_pct 는 이제 "리스크%"가 아니라 "계좌 투입 비율"로 해석한다.
  - 1.0 = 전액
  - 0.7 = 70%
  - 0.4 = 40%
- 따라서 0 < risk_pct <= 1.0 을 강제한다.

PATCH NOTES — 2026-03-01
- bt_trades 실제 DB 스키마 정합:
  - open 기록: entry_ts/entry_price/qty/is_auto/regime_at_entry/strategy/risk_pct/tp_pct/sl_pct/leverage 등 저장
  - is_open/close_ts/close_price/pnl 사용 제거
- bt_trade_snapshots: trade_id 1:1 진입 스냅샷 저장 유지
- EventBus strict validation 정합:
  - publish payload.side 는 LONG/SHORT/CLOSE만 허용 → 이벤트 로그(side)는 direction(LONG/SHORT) 사용
- TEST harness (env-controlled; 기본 OFF):
  - TEST_FORCE_ENTER=1 : signal.action 무시하고 ENTER 강제
  - TEST_FAKE_AVAILABLE_USDT=1000 : 잔고를 강제로 설정
  - TEST_BYPASS_GUARDS=1 : volume/price_jump/spread/risk_guard 실패 시에도 계속 진행
  - TEST_DRY_RUN=1 : 실주문 금지. DB에 (open+snapshot) 기록 후 즉시 TEST_DRY_RUN으로 닫고 exit snapshot까지 저장
- TEST_BYPASS_GUARDS는 TEST_DRY_RUN=1일 때만 허용(운영 사고 방지)

PATCH NOTES — 2026-03-02
- dynamic_risk_pct 메타가 존재하면 signal.risk_pct와 일치 강제(불일치 시 예외).
- ENTER 시 risk_pct/tp_pct/sl_pct는 반드시 > 0 강제(0 허용 금지).
- 전액 배분형 안전장치:
  - leverage > 1 상태에서 risk_pct(투입 비율)가 큰 경우(>=0.8) 즉시 예외로 차단.
========================================================
"""

from __future__ import annotations

import logging
import math
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from events.signals_logger import log_event, log_signal, log_skip_event
from execution.exchange_api import get_available_usdt, get_balance_detail
from execution.order_executor import open_position_with_tp_sl
from infra.telelog import send_skip_tg, send_tg
from risk.entry_guards_ws import (
    check_manual_position_guard,
    check_price_jump_guard,
    check_spread_guard,
    check_volume_guard,
)
from strategy.signal import Signal
from execution.risk_manager import hard_risk_guard_check

from state.db_writer import (
    record_trade_open_returning_id,
    record_trade_snapshot,
    close_latest_open_trade_returning_id,
    record_trade_exit_snapshot,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# TEST harness (env)
# ─────────────────────────────────────────────
def _env_bool(name: str, default: str = "0") -> bool:
    v = os.getenv(name, default)
    return str(v).strip().lower() in {"1", "true", "t", "yes", "y", "on"}


TEST_FORCE_ENTER: bool = _env_bool("TEST_FORCE_ENTER", "0")
TEST_BYPASS_GUARDS: bool = _env_bool("TEST_BYPASS_GUARDS", "0")
TEST_DRY_RUN: bool = _env_bool("TEST_DRY_RUN", "0")

try:
    TEST_FAKE_AVAILABLE_USDT: float = float(os.getenv("TEST_FAKE_AVAILABLE_USDT", "0") or 0)
except Exception:
    TEST_FAKE_AVAILABLE_USDT = 0.0

# 운영 사고 방지: 가드 우회는 DRY_RUN에서만 허용
if TEST_BYPASS_GUARDS and not TEST_DRY_RUN:
    raise RuntimeError("TEST_BYPASS_GUARDS is only allowed with TEST_DRY_RUN=1")

# 테스트 모드 경고(한 번만)
if any([TEST_FORCE_ENTER, TEST_BYPASS_GUARDS, TEST_DRY_RUN, TEST_FAKE_AVAILABLE_USDT > 0]):
    logger.warning(
        "[TEST MODE ENABLED] FORCE_ENTER=%s BYPASS_GUARDS=%s DRY_RUN=%s FAKE_USDT=%s",
        TEST_FORCE_ENTER,
        TEST_BYPASS_GUARDS,
        TEST_DRY_RUN,
        TEST_FAKE_AVAILABLE_USDT,
    )


def _as_float(value: Any, name: str, *, min_value: Optional[float] = None, max_value: Optional[float] = None) -> float:
    try:
        if isinstance(value, bool):
            raise TypeError("bool is not allowed")
        v = float(value)
    except Exception as e:
        raise ValueError(f"{name} must be a number") from e

    if not math.isfinite(v):
        raise ValueError(f"{name} must be finite")

    if min_value is not None and v < min_value:
        raise ValueError(f"{name} must be >= {min_value}")
    if max_value is not None and v > max_value:
        raise ValueError(f"{name} must be <= {max_value}")

    return v


def _opt_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if isinstance(value, bool):
            return None
        v = float(value)
    except Exception:
        return None
    if not math.isfinite(v):
        return None
    return v


class _SettingsView:
    __slots__ = ("_base", "_overrides")

    def __init__(self, base: Any, overrides: Dict[str, float]):
        object.__setattr__(self, "_base", base)
        object.__setattr__(self, "_overrides", overrides)

    def __getattr__(self, name: str) -> Any:
        overrides = object.__getattribute__(self, "_overrides")
        if name in overrides:
            return overrides[name]
        base = object.__getattribute__(self, "_base")
        return getattr(base, name)

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("SettingsView is read-only")


def _build_guard_settings_view(settings: Any, guard_adjustments: Dict[str, float]) -> _SettingsView:
    if guard_adjustments is None:
        raise RuntimeError("guard_adjustments must not be None")
    if not isinstance(guard_adjustments, dict):
        raise RuntimeError("guard_adjustments must be dict")

    overrides: Dict[str, float] = {}
    for k, v in guard_adjustments.items():
        if not isinstance(k, str) or not k.strip():
            raise ValueError("guard_adjustments key must be non-empty str")
        if not hasattr(settings, k):
            raise RuntimeError(f"guard_adjustments contains unknown settings key: {k}")
        overrides[k] = _as_float(v, f"guard_adjustments.{k}", min_value=0.0)

    return _SettingsView(settings, overrides)


class ExecutionEngine:
    def __init__(self, settings: Any):
        self.settings = settings
        required = ["symbol", "leverage", "slippage_block_pct", "slippage_stop_engine"]
        missing = [k for k in required if not hasattr(settings, k)]
        if missing:
            raise ValueError(f"settings missing required fields: {missing}")

    def execute(self, signal: Signal):
        if not isinstance(signal, Signal):
            raise ValueError("signal must be Signal")

        action = str(signal.action).upper().strip()
        if action not in ("ENTER", "SKIP"):
            raise ValueError(f"signal.action invalid: {signal.action!r}")

        if TEST_FORCE_ENTER:
            action = "ENTER"

        meta = signal.meta
        if not isinstance(meta, dict):
            raise RuntimeError("signal.meta must be dict")

        symbol = meta.get("symbol")
        if not isinstance(symbol, str) or not symbol.strip():
            raise RuntimeError("signal.meta['symbol'] is required")
        symbol = symbol.strip().upper()

        regime = meta.get("regime")
        if not isinstance(regime, str) or not regime.strip():
            raise RuntimeError("signal.meta['regime'] is required")
        regime = regime.strip()

        signal_source = meta.get("signal_source")
        if not isinstance(signal_source, str) or not signal_source.strip():
            raise RuntimeError("signal.meta['signal_source'] is required")
        signal_source = signal_source.strip()

        signal_ts_ms = meta.get("signal_ts_ms")
        if not isinstance(signal_ts_ms, int) or signal_ts_ms <= 0:
            raise RuntimeError("signal.meta['signal_ts_ms'] must be int > 0")

        direction = str(signal.direction).upper().strip()
        if direction not in ("LONG", "SHORT"):
            raise ValueError("signal.direction must be LONG or SHORT")

        if action == "SKIP":
            msg = f"[SKIP] {symbol} {direction} reason={signal.reason}"
            logger.info(msg)
            log_skip_event(
                symbol=symbol,
                regime=regime,
                source="execution_engine",
                side=direction,
                reason=str(signal.reason) or "skip",
                extra={"signal_source": signal_source, "ts_ms": signal_ts_ms},
            )
            send_skip_tg(msg)
            return None

        last_price = _as_float(meta.get("last_price"), "meta.last_price", min_value=0.0)
        if last_price <= 0:
            raise RuntimeError("meta.last_price must be > 0")

        candles_5m = meta.get("candles_5m")
        candles_5m_raw = meta.get("candles_5m_raw")
        extra = meta.get("extra")
        if extra is not None and not isinstance(extra, dict):
            raise RuntimeError("meta.extra must be dict or None")

        # 0) dynamic_risk_pct 일치 강제(있으면)
        dyn = meta.get("dynamic_risk_pct")
        if dyn is not None:
            dyn_f = _as_float(dyn, "meta.dynamic_risk_pct", min_value=0.0, max_value=1.0)
            sig_r = _as_float(signal.risk_pct, "signal.risk_pct", min_value=0.0, max_value=1.0)
            if abs(dyn_f - sig_r) > 1e-12:
                raise RuntimeError(f"dynamic_risk_pct mismatch: meta={dyn_f} != signal.risk_pct={sig_r}")

        # 1) 수동 포지션 가드
        manual_ok = check_manual_position_guard(
            get_balance_detail_func=get_balance_detail,
            symbol=symbol,
            latest_ts=float(signal_ts_ms),
        )
        if not manual_ok:
            msg = f"[SKIP] manual_guard_blocked: {symbol}"
            logger.warning(msg)
            log_skip_event(
                symbol=symbol,
                regime=regime,
                source="execution_engine.guard.manual",
                side=direction,
                reason="manual_guard_blocked",
                extra={"ts_ms": signal_ts_ms},
            )
            send_skip_tg(msg)
            return None

        # 2) 가용 잔고
        if TEST_FAKE_AVAILABLE_USDT > 0:
            avail_usdt = float(TEST_FAKE_AVAILABLE_USDT)
        else:
            avail_usdt = _as_float(get_available_usdt(), "available_usdt", min_value=0.0)

        if avail_usdt <= 0:
            msg = f"[SKIP] available_usdt<=0: {symbol}"
            logger.warning(msg)
            log_skip_event(
                symbol=symbol,
                regime=regime,
                source="execution_engine.balance",
                side=direction,
                reason="balance_zero_or_invalid",
                extra={"available_usdt": avail_usdt},
            )
            send_skip_tg(msg)
            return None

        best_bid: Optional[float] = None
        best_ask: Optional[float] = None
        guard_settings = _build_guard_settings_view(self.settings, dict(signal.guard_adjustments))

        # 3-1) 볼륨 가드
        if candles_5m_raw is None:
            raise RuntimeError("meta.candles_5m_raw is required for volume guard")

        vol_ok = check_volume_guard(
            settings=guard_settings,
            candles_5m_raw=candles_5m_raw,
            latest_ts=float(signal_ts_ms),
            signal_source=signal_source,
            direction=direction,
        )
        if not vol_ok:
            msg = f"[SKIP] volume_guard_blocked: {symbol}"
            logger.info(msg)
            log_skip_event(
                symbol=symbol,
                regime=regime,
                source="execution_engine.guard.volume",
                side=direction,
                reason="volume_guard_blocked",
                extra={"ts_ms": signal_ts_ms},
            )
            send_skip_tg(msg)
            if not TEST_BYPASS_GUARDS:
                return None

        # 3-2) 가격점프 가드
        if candles_5m is None:
            raise RuntimeError("meta.candles_5m is required for price jump guard")

        price_ok = check_price_jump_guard(
            settings=guard_settings,
            candles_5m=candles_5m,
            latest_ts=float(signal_ts_ms),
            signal_source=signal_source,
            direction=direction,
        )
        if not price_ok:
            msg = f"[SKIP] price_jump_guard_blocked: {symbol}"
            logger.info(msg)
            log_skip_event(
                symbol=symbol,
                regime=regime,
                source="execution_engine.guard.price_jump",
                side=direction,
                reason="price_jump_guard_blocked",
                extra={"ts_ms": signal_ts_ms},
            )
            send_skip_tg(msg)
            if not TEST_BYPASS_GUARDS:
                return None

        # 3-3) 스프레드 가드
        spread_ok, best_bid, best_ask = check_spread_guard(
            settings=guard_settings,
            symbol=symbol,
            latest_ts=float(signal_ts_ms),
            signal_source=signal_source,
            direction=direction,
        )
        if not spread_ok:
            msg = f"[SKIP] spread_guard_blocked: {symbol}"
            logger.info(msg)
            log_skip_event(
                symbol=symbol,
                regime=regime,
                source="execution_engine.guard.spread",
                side=direction,
                reason="spread_guard_blocked",
                extra={"ts_ms": signal_ts_ms, "best_bid": best_bid, "best_ask": best_ask},
            )
            send_skip_tg(msg)
            if not TEST_BYPASS_GUARDS:
                return None

        entry_price_hint = float(last_price)
        price_for_qty = float(last_price)

        if best_bid is not None and best_ask is not None:
            bb = float(best_bid)
            ba = float(best_ask)
            if bb > 0 and ba > 0 and ba > bb:
                mid = (bb + ba) / 2.0
                if mid > 0:
                    price_for_qty = mid
                entry_price_hint = ba if direction == "LONG" else bb

        if price_for_qty <= 0:
            raise RuntimeError("price_for_qty must be > 0")

        spread_pct_snapshot: Optional[float] = None
        if best_bid is not None and best_ask is not None:
            bb = float(best_bid)
            ba = float(best_ask)
            if bb > 0 and ba > 0 and ba > bb:
                mid = (bb + ba) / 2.0
                if mid > 0:
                    spread_pct_snapshot = (ba - bb) / mid

        slippage_block_pct = _as_float(getattr(self.settings, "slippage_block_pct"), "settings.slippage_block_pct", min_value=0.0)
        slippage_stop_engine = bool(getattr(self.settings, "slippage_stop_engine"))

        if slippage_block_pct > 0:
            slippage_pct = abs(entry_price_hint - last_price) / last_price
            if slippage_pct > slippage_block_pct:
                msg = f"[SKIP] {symbol} {direction} slippage_block slippage_pct={slippage_pct}"
                logger.warning(msg)
                log_skip_event(
                    symbol=symbol,
                    regime=regime,
                    source="execution_engine.slippage",
                    side=direction,
                    reason="slippage_block",
                    extra={"slippage_pct": slippage_pct, "slippage_block_pct": slippage_block_pct},
                )
                send_skip_tg(msg)
                if slippage_stop_engine:
                    raise RuntimeError("slippage_block triggered and slippage_stop_engine=True")
                if not TEST_BYPASS_GUARDS:
                    return None

        leverage = _as_float(getattr(self.settings, "leverage"), "settings.leverage", min_value=1.0)

        # ENTER에서 0 금지 (STRICT) + 전액 배분형: 0<risk_pct<=1 강제
        risk_pct = _as_float(signal.risk_pct, "signal.risk_pct", min_value=0.0, max_value=1.0)
        tp_pct = _as_float(signal.tp_pct, "signal.tp_pct", min_value=0.0)
        sl_pct = _as_float(signal.sl_pct, "signal.sl_pct", min_value=0.0)

        if risk_pct <= 0:
            raise RuntimeError("ENTER requires signal.risk_pct > 0")
        if tp_pct <= 0:
            raise RuntimeError("ENTER requires signal.tp_pct > 0")
        if sl_pct <= 0:
            raise RuntimeError("ENTER requires signal.sl_pct > 0")

        # 전액 배분형 안전장치: 큰 투입 비율 + 레버리지>1은 사고 가능성이 매우 큼
        if leverage > 1.0 and risk_pct >= 0.8:
            raise RuntimeError(
                f"allocation_mode violation: leverage({leverage})>1 with large allocation(risk_pct={risk_pct})"
            )

        notional = avail_usdt * risk_pct * leverage
        if notional <= 0:
            raise RuntimeError("notional must be > 0")

        qty_raw = notional / price_for_qty
        if qty_raw <= 0:
            raise RuntimeError("qty_raw must be > 0")

        ok, guard_reason, guard_extra = hard_risk_guard_check(
            self.settings,
            symbol=symbol,
            side=direction,
            entry_price=float(entry_price_hint),
            notional=float(notional),
            available_usdt=float(avail_usdt),
        )
        if not ok:
            msg = f"[SKIP] hard_risk_guard_blocked: {guard_reason}"
            logger.warning("%s extra=%s", msg, guard_extra)
            log_skip_event(
                symbol=symbol,
                regime=regime,
                source="execution_engine.risk_guard",
                side=direction,
                reason=str(guard_reason),
                extra=guard_extra,
            )
            send_skip_tg(f"{msg} {symbol} {direction}")
            if not TEST_BYPASS_GUARDS:
                return None

        side_open = "BUY" if direction == "LONG" else "SELL"

        soft_mode = False
        sl_floor_ratio: Optional[float] = None
        if isinstance(extra, dict):
            soft_mode = bool(extra.get("soft_mode") or extra.get("soft") or False)
            if extra.get("sl_floor_ratio") is not None:
                sl_floor_ratio = _as_float(extra.get("sl_floor_ratio"), "extra.sl_floor_ratio", min_value=0.0)

        log_event(
            "on_entry_submitted",
            symbol=symbol,
            regime=regime,
            source="execution_engine",
            side=direction,
            price=float(entry_price_hint),
            qty=float(qty_raw),
            leverage=float(leverage),
            tp_pct=float(tp_pct),
            sl_pct=float(sl_pct),
            risk_pct=float(risk_pct),
            reason="entry_submit",
            extra_json={"signal_source": signal_source, "gpt_reason": str(signal.reason)[:200]},
        )

        log_signal(
            event="ENTRY_SIGNAL",
            symbol=symbol,
            regime=regime,
            source=str(signal_source),
            side=direction,
            price=float(entry_price_hint),
            qty=float(qty_raw),
            leverage=float(leverage),
            tp_pct=float(tp_pct),
            sl_pct=float(sl_pct),
            risk_pct=float(risk_pct),
            reason=str(signal.reason) or "enter",
            extra_json={
                "candle_ts": int(signal_ts_ms),
                "available_usdt": float(avail_usdt),
                "notional": float(notional),
                "regime_score": meta.get("regime_score"),
                "regime_band": meta.get("regime_band"),
                "regime_allocation": meta.get("regime_allocation"),
                "dd_pct": meta.get("dd_pct"),
                "consecutive_losses": meta.get("consecutive_losses"),
                "risk_physics_reason": meta.get("risk_physics_reason"),
                "dynamic_risk_pct": meta.get("dynamic_risk_pct"),
            },
        )

        if TEST_DRY_RUN:
            entry_price = float(entry_price_hint)
            entry_qty = float(qty_raw)
            entry_ts_dt = datetime.fromtimestamp(float(signal_ts_ms) / 1000.0, tz=timezone.utc)

            log_event(
                "on_entry_filled",
                symbol=symbol,
                regime=regime,
                source="execution_engine",
                side=direction,
                price=float(entry_price),
                qty=float(entry_qty),
                leverage=float(leverage),
                tp_pct=float(tp_pct),
                sl_pct=float(sl_pct),
                risk_pct=float(risk_pct),
                reason="entry_filled_simulated",
                extra_json={"test_dry_run": True},
                is_test=True,
            )

            trade_id = record_trade_open_returning_id(
                symbol=symbol,
                side=str(side_open),
                qty=float(entry_qty),
                entry_price=float(entry_price),
                entry_ts=entry_ts_dt,
                is_auto=True,
                regime_at_entry=str(regime),
                strategy=str(signal_source),
                entry_score=_opt_float(meta.get("entry_score") or meta.get("entryScore")),
                trend_score_at_entry=None,
                range_score_at_entry=None,
                leverage=float(leverage),
                risk_pct=float(risk_pct),
                tp_pct=float(tp_pct),
                sl_pct=float(sl_pct),
                note="TEST_DRY_RUN",
            )

            record_trade_snapshot(
                trade_id=int(trade_id),
                symbol=symbol,
                entry_ts=entry_ts_dt,
                direction=str(direction),
                signal_source=str(signal_source),
                regime=str(regime),
                entry_score=_opt_float(meta.get("entry_score") or meta.get("entryScore")),
                engine_total=_opt_float(meta.get("engine_total") or meta.get("engine_total_score")),
                trend_strength=None,
                atr_pct=None,
                volume_zscore=None,
                depth_ratio=None,
                spread_pct=spread_pct_snapshot,
                last_price=float(last_price),
                risk_pct=float(risk_pct),
                tp_pct=float(tp_pct),
                sl_pct=float(sl_pct),
                gpt_action="ENTER",
                gpt_reason=str(signal.reason or ""),
            )

            closed_trade_id = close_latest_open_trade_returning_id(
                symbol=symbol,
                side=str(side_open),
                exit_price=float(entry_price),
                exit_ts=entry_ts_dt,
                pnl_usdt=0.0,
                close_reason="TEST_DRY_RUN",
                regime_at_exit=str(regime),
                pnl_pct_futures=0.0,
                pnl_pct_spot_ref=None,
                note="TEST_DRY_RUN closed immediately",
            )

            record_trade_exit_snapshot(
                trade_id=int(closed_trade_id),
                symbol=symbol,
                close_ts=entry_ts_dt,
                close_price=float(entry_price),
                pnl=0.0,
                close_reason="TEST_DRY_RUN",
                exit_atr_pct=None,
                exit_trend_strength=None,
                exit_volume_zscore=None,
                exit_depth_ratio=None,
            )

            send_tg(f"[TEST_DRY_RUN] DB recorded + closed: {symbol} {direction} trade_id={trade_id}")
            return None

        trade = open_position_with_tp_sl(
            settings=self.settings,
            symbol=symbol,
            side_open=side_open,
            qty=float(qty_raw),
            entry_price_hint=float(entry_price_hint),
            tp_pct=float(tp_pct),
            sl_pct=float(sl_pct),
            source=str(signal_source),
            soft_mode=bool(soft_mode),
            sl_floor_ratio=sl_floor_ratio,
        )
        if trade is None:
            raise RuntimeError("open_position_with_tp_sl returned None")

        entry_price = getattr(trade, "entry", getattr(trade, "entry_price", entry_price_hint))
        entry_qty = getattr(trade, "qty", qty_raw)

        entry_ts_dt = getattr(trade, "entry_ts", None)
        if not isinstance(entry_ts_dt, datetime) or entry_ts_dt.tzinfo is None or entry_ts_dt.tzinfo.utcoffset(entry_ts_dt) is None:
            entry_ts_dt = datetime.fromtimestamp(float(signal_ts_ms) / 1000.0, tz=timezone.utc)

        log_event(
            "on_entry_filled",
            symbol=symbol,
            regime=regime,
            source="execution_engine",
            side=direction,
            price=float(entry_price),
            qty=float(entry_qty),
            leverage=float(leverage),
            tp_pct=float(tp_pct),
            sl_pct=float(sl_pct),
            risk_pct=float(risk_pct),
            reason="entry_filled",
            extra_json={"signal_source": signal_source},
        )

        trade_id = record_trade_open_returning_id(
            symbol=symbol,
            side=str(side_open),
            qty=float(entry_qty),
            entry_price=float(entry_price),
            entry_ts=entry_ts_dt,
            is_auto=True,
            regime_at_entry=str(regime),
            strategy=str(signal_source),
            entry_score=_opt_float(meta.get("entry_score") or meta.get("entryScore")),
            trend_score_at_entry=None,
            range_score_at_entry=None,
            leverage=float(leverage),
            risk_pct=float(risk_pct),
            tp_pct=float(tp_pct),
            sl_pct=float(sl_pct),
            note=f"gpt_reason={str(signal.reason or '')[:180]}",
        )

        try:
            setattr(trade, "id", int(trade_id))
        except Exception:
            pass

        record_trade_snapshot(
            trade_id=int(trade_id),
            symbol=symbol,
            entry_ts=entry_ts_dt,
            direction=str(direction),
            signal_source=str(signal_source),
            regime=str(regime),
            entry_score=_opt_float(meta.get("entry_score") or meta.get("entryScore")),
            engine_total=_opt_float(meta.get("engine_total") or meta.get("engine_total_score")),
            trend_strength=None,
            atr_pct=None,
            volume_zscore=None,
            depth_ratio=None,
            spread_pct=spread_pct_snapshot,
            last_price=float(last_price),
            risk_pct=float(risk_pct),
            tp_pct=float(tp_pct),
            sl_pct=float(sl_pct),
            gpt_action="ENTER",
            gpt_reason=str(signal.reason or ""),
        )

        send_tg(
            f"[ENTRY][FILLED] {symbol} {direction} "
            f"alloc={risk_pct*100:.1f}% tp={tp_pct*100:.2f}% sl={sl_pct*100:.2f}% "
            f"price={float(entry_price):.4f} qty={float(entry_qty):.6f} src={signal_source}"
        )
        return trade