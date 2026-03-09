/*
========================================================
FILE: dashboard/static/js/dashboard.js
AUTO-TRADER — AI TRADING INTELLIGENCE SYSTEM
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할
--------------------------------------------------------
- 대시보드 프론트엔드 동작 전담
- 기존 대시보드 기능 유지
  - 요약/성과 차트
  - 엔진 상태
  - 현재 포지션
  - 최근 판단
  - 최근 오류/감시 경고
  - 최근 거래
  - 진입 점수/스킵 사유/시간대 분석
  - WebSocket 실시간 반영
- AI 분석 기능 추가
  - 통합 분석(/api/quant-analysis)
  - 외부 시장 분석(/api/market-analysis)

STRICT · NO-FALLBACK
--------------------------------------------------------
- API 실패/JSON 오류/구조 오류는 즉시 화면에 표시
- 기존 값 묵시적 유지 금지
- polling 금지, WebSocket 실시간 반영
- 모바일 접속 차단
- 민감정보 출력 금지

변경 이력
--------------------------------------------------------
- 2026-03-07:
  1) 기존 inline script를 dashboard/static/js/dashboard.js 로 분리
  2) AI 분석 검색 기능 추가
  3) 기존 기능 삭제 없이 전체 유지
========================================================
*/

"use strict";

const $ = (id) => document.getElementById(id);

let ws = null;
let dailyPnlChart = null;
let entryScoreHistChart = null;
let skipHourlyChart = null;
let equityCurveChart = null;
let drawdownChart = null;

const API = {
  summary: "/api/summary",
  performanceSummary: "/api/performance/summary",
  dailyPnl: "/api/performance/daily-pnl?days=30",
  equityCurve: "/api/performance/equity-curve?limit=500",
  drawdownCurve: "/api/performance/drawdown-curve?limit=500",
  entryScores: "/api/entry-scores/recent?limit=300",
  skipReasons: "/api/events/skip-reasons?days=7&limit=15",
  skipHourly: "/api/events/skip-hourly?days=7",
  engineStatus: "/api/engine/status",
  positionCurrent: "/api/position/current",
  decisionLatest: "/api/decision/latest",
  errorCounts: "/api/errors/counts?minutes=10",
  errorsRecent: "/api/errors/recent?limit=50",
  tradesRecent: "/api/trades/recent?limit=30",
  quantAnalysis: "/api/quant-analysis",
  marketAnalysis: "/api/market-analysis",
};

const state = {
  decision: null,
  position: null,
  engine: null,
  errorCounts: null,
  recentErrors: [],
  recentTrades: [],
};

function isMobileBlocked() {
  const ua = navigator.userAgent || "";
  const isMobileUA =
    /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(ua);
  const isSmallViewport = window.innerWidth < 1024;
  return isMobileUA || isSmallViewport;
}

function enforceDesktopOnly() {
  const blocked = isMobileBlocked();
  const blocker = $("desktop-blocker");
  if (!blocker) {
    throw new Error("desktop-blocker element missing");
  }

  if (blocked) {
    blocker.classList.add("show");
    try {
      alert("PC 전용 대시보드입니다");
    } catch (_) {
      // alert 실패는 무시 가능
    }
    return false;
  }

  blocker.classList.remove("show");
  return true;
}

function showBannerError(msg) {
  const banner = $("error-banner");
  const messageEl = $("error-banner-msg");
  if (!banner || !messageEl) {
    throw new Error("error banner elements missing");
  }
  messageEl.textContent = String(msg || "알 수 없는 오류");
  banner.classList.remove("hidden");
}

function hideBannerError() {
  const banner = $("error-banner");
  const messageEl = $("error-banner-msg");
  if (!banner || !messageEl) {
    throw new Error("error banner elements missing");
  }
  banner.classList.add("hidden");
  messageEl.textContent = "";
}

function setWidgetError(elemId, msg) {
  const el = $(elemId);
  if (!el) {
    throw new Error(`${elemId} element missing`);
  }
  el.textContent = String(msg || "오류");
  el.classList.remove("hidden");
}

function clearWidgetError(elemId) {
  const el = $(elemId);
  if (!el) {
    throw new Error(`${elemId} element missing`);
  }
  el.classList.add("hidden");
  el.textContent = "";
}

function showEl(id) {
  const el = $(id);
  if (!el) {
    throw new Error(`${id} element missing`);
  }
  el.classList.remove("hidden");
}

function hideEl(id) {
  const el = $(id);
  if (!el) {
    throw new Error(`${id} element missing`);
  }
  el.classList.add("hidden");
}

async function fetchJsonStrict(url, label, timeoutMs = 15000) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);

  let res;
  try {
    res = await fetch(url, {
      signal: controller.signal,
      cache: "no-store",
    });
  } catch (e) {
    clearTimeout(t);
    throw new Error(`${label}: 요청 실패 (${e && e.name ? e.name : "ERR"})`);
  } finally {
    clearTimeout(t);
  }

  if (!res.ok) {
    throw new Error(`${label}: HTTP ${res.status}`);
  }

  let data;
  try {
    data = await res.json();
  } catch (_) {
    throw new Error(`${label}: JSON 형식 오류`);
  }

  if (data === null || typeof data !== "object") {
    throw new Error(`${label}: 응답 구조 오류`);
  }

  return data;
}

function fmtNumber(v, digits = 2) {
  const num = Number(v);
  if (Number.isNaN(num)) return "오류";
  return num.toLocaleString("en-US", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function fmtSigned(v, digits = 2) {
  const num = Number(v);
  if (Number.isNaN(num)) return "오류";
  const s = num.toLocaleString("en-US", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
  return num > 0 ? `+${s}` : s;
}

function fmtMaybe(v, digits = 2) {
  if (v === null || v === undefined) return "-";
  return fmtNumber(v, digits);
}

function setLastUpdatedKST() {
  const label = $("last-updated-label");
  if (!label) {
    throw new Error("last-updated-label element missing");
  }

  const now = new Date();
  const fmt = new Intl.DateTimeFormat("ko-KR", {
    timeZone: "Asia/Seoul",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });

  const parts = fmt.formatToParts(now).reduce((acc, p) => {
    acc[p.type] = p.value;
    return acc;
  }, {});

  label.textContent =
    `마지막 업데이트: ${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second} (KST)`;
}

function setWsLastMessage(tsMs) {
  const label = $("ws-last-message-label");
  if (!label) {
    throw new Error("ws-last-message-label element missing");
  }

  if (!tsMs) {
    label.textContent = "실시간 수신: -";
    return;
  }

  const d = new Date(Number(tsMs));
  if (Number.isNaN(d.getTime())) {
    label.textContent = "실시간 수신: 오류";
    return;
  }

  const fmt = new Intl.DateTimeFormat("ko-KR", {
    timeZone: "Asia/Seoul",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });

  label.textContent = `실시간 수신: ${fmt.format(d)} (KST)`;
}

function applyStatusChip(el, statusText) {
  const chip = typeof el === "string" ? $(el) : el;
  if (!chip) {
    throw new Error("status chip element missing");
  }

  let cls = "status-neutral";
  let dot = "tiny-dot-neutral";
  const s = String(statusText || "").toUpperCase();

  if (
    s.includes("OK") ||
    s === "OPEN" ||
    s === "CONNECTED" ||
    s === "ENTRY" ||
    s === "HOLD" ||
    s === "QUANT_ANALYSIS" ||
    s === "MARKET_ANALYSIS"
  ) {
    cls = "status-ok";
    dot = "tiny-dot-ok";
  } else if (s.includes("WARNING") || s.includes("WARN")) {
    cls = "status-warning";
    dot = "tiny-dot-warning";
  } else if (
    s.includes("FATAL") ||
    s.includes("ERROR") ||
    s.includes("EXIT") ||
    s.includes("NO_ENTRY") ||
    s.includes("CLOSED") ||
    s.includes("FLAT") ||
    s.includes("OUT_OF_SCOPE")
  ) {
    cls = "status-fatal";
    dot = "tiny-dot-fatal";
  }

  chip.className = `status-chip ${cls}`;
  chip.innerHTML = `<span class="tiny-dot ${dot}"></span>${statusText || "-"}`;
}

function applyValueColor(el, num) {
  if (!el) {
    throw new Error("value color element missing");
  }
  const n = Number(num);
  if (Number.isNaN(n)) {
    el.classList.remove("value-positive", "value-negative", "value-neutral");
    return;
  }
  el.classList.remove("value-positive", "value-negative", "value-neutral");
  if (n > 0) el.classList.add("value-positive");
  else if (n < 0) el.classList.add("value-negative");
  else el.classList.add("value-neutral");
}

function normalizeReasonText(reason) {
  const raw = String(reason || "").trim();
  if (!raw) return "-";
  const lower = raw.toLowerCase();

  const reasonMap = [
    ["depth_imbalance_guard", "호가창이 한쪽으로 많이 쏠려 진입 안 함"],
    ["spread_guard_blocked", "매수·매도 차이가 커서 진입 안 함"],
    ["slippage_block", "체결 미끄러짐 위험이 커서 진입 안 함"],
    ["volume_too_low_for_entry", "거래량이 부족해서 진입 안 함"],
    ["volume_guard_blocked", "거래량 조건이 부족해서 진입 안 함"],
    ["price_jump_guard_blocked", "가격이 갑자기 움직여 진입 안 함"],
    ["price_jump_guard", "가격 급변 감지"],
    ["candle_volatility_guard", "변동성이 커서 진입 안 함"],
    ["balance_zero_or_invalid", "잔고 정보가 없거나 이상함"],
    ["orderbook_unavailable", "호가창 데이터를 받지 못함"],
    ["entry_score_below_threshold", "진입 점수가 기준보다 낮음"],
    ["ws_orderbook_stale", "호가창 데이터가 늦게 들어옴"],
    ["ws_kline_stale", "캔들 데이터가 늦게 들어옴"],
    ["orderbook_integrity_fail", "호가창 데이터가 비정상임"],
    ["kline_rollback", "캔들 시간이 거꾸로 들어옴"],
    ["watchdog_internal_error", "감시 로직 내부 오류"],
    ["db_lag", "DB 응답이 느림"],
    ["recent_errors_high", "최근 오류가 많이 발생함"],
    ["recent_skips_high", "최근 진입 안 함이 많음"],
    ["db_latency_ms_high", "DB 지연 시간이 큼"],
    ["position event not found", "현재 보유 기록을 찾지 못함"],
    ["all trades were filtered as test trades", "테스트 거래만 있어서 실거래 데이터가 없음"],
    ["range_market_entry", "박스권에서 진입해 손실 발생"],
    ["weak_pattern_entry", "패턴 점수가 낮은 상태에서 진입"],
    ["wide_spread_entry", "매수·매도 차이가 큰 상태에서 진입"],
    ["depth_imbalance_entry", "호가창 불균형 상태에서 진입"],
    ["high_volatility_entry", "변동성이 큰 상태에서 진입"],
    ["unclassified_loss_context", "손실 원인을 분류하지 못함"],
    ["dashboard_query", "대시보드 AI 질의"],
    ["market_query", "외부 시장 AI 질의"],
    ["market_report", "자동 시장 분석 리포트"],
    ["system_report", "자동 시스템 분석 리포트"],
  ];

  for (const [key, label] of reasonMap) {
    if (lower.includes(key)) return label;
  }

  return raw
    .replaceAll("watchdog", "감시")
    .replaceAll("orderbook", "호가창")
    .replaceAll("spread", "매수·매도 차이")
    .replaceAll("depth", "호가")
    .replaceAll("guard", "보호 조건")
    .replaceAll("blocked", "차단됨")
    .replaceAll("stale", "지연")
    .replaceAll("entry", "진입")
    .replaceAll("exit", "청산")
    .replaceAll("error", "오류");
}

function normalizeDecisionReason(reason) {
  const raw = String(reason || "").trim();
  if (!raw) return "근거 없음";
  const lower = raw.toLowerCase();

  if (lower === "guards_passed") return "진입 전 검사 통과";
  if (lower === "entry_submit") return "진입 주문 조건 충족";
  if (lower === "hold") return "보유 유지";
  if (lower === "exit_submit") return "청산 주문 조건 충족";
  if (lower === "risk_blocked") return "위험 조건으로 차단";
  if (lower === "no_entry") return "진입 안 함";

  return raw
    .replaceAll("_", " ")
    .replace(/\bentry\b/gi, "진입")
    .replace(/\bexit\b/gi, "청산")
    .replace(/\bguard\b/gi, "보호")
    .replace(/\bsubmit\b/gi, "실행")
    .replace(/\bpassed\b/gi, "통과");
}

function normalizeWatchdogReason(reason) {
  return normalizeReasonText(reason);
}

function normalizeAiScope(scope) {
  const s = String(scope || "").trim();
  if (!s) return "-";
  const map = {
    quant_analysis: "내부 거래 분석",
    market_analysis: "외부 시장 분석",
    mixed: "통합 분석",
    out_of_scope: "범위 밖 질문",
  };
  return map[s] || s;
}

function normalizeUsedInputTag(input) {
  const map = {
    internal_market_summary: "내부 시장 요약",
    trade_summary: "거래 요약",
    external_market_summary: "외부 시장 요약",
  };
  return map[input] || input;
}

function createOrUpdateChart(existing, canvasId, type, labels, data, datasetLabel) {
  const canvas = $(canvasId);
  if (!canvas) throw new Error(`${canvasId}: canvas missing`);
  const ctx = canvas.getContext("2d");
  if (!ctx) throw new Error(`${canvasId}: context missing`);

  if (existing) existing.destroy();

  const palette = {
    lineBorder: "#111827",
    lineFill: "rgba(17, 24, 39, 0.08)",
    barBorder: "#1d4ed8",
    barFill: "rgba(37, 99, 235, 0.24)",
  };

  return new Chart(ctx, {
    type,
    data: {
      labels,
      datasets: [
        {
          label: datasetLabel,
          data,
          borderWidth: 2,
          tension: 0.25,
          fill: type === "line",
          borderColor: type === "line" ? palette.lineBorder : palette.barBorder,
          backgroundColor: type === "line" ? palette.lineFill : palette.barFill,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      plugins: {
        legend: { display: false },
      },
      scales: {
        x: {
          ticks: { color: "#64748b", maxTicksLimit: 10 },
          grid: { color: "rgba(15,23,42,0.06)" },
        },
        y: {
          ticks: { color: "#64748b" },
          grid: { color: "rgba(15,23,42,0.06)" },
        },
      },
    },
  });
}

function renderSummary(data, perfSummary) {
  $("summary-total-trades").textContent = fmtNumber(data.total_trades, 0);
  $("summary-win-rate").textContent = `${fmtNumber(data.win_rate_pct, 1)}%`;
  $("summary-win-loss").textContent =
    `이긴 거래 ${fmtNumber(data.wins, 0)} / 진 거래 ${fmtNumber(data.losses, 0)} / 본전 ${fmtNumber(data.breakevens, 0)}`;

  const totalPnlEl = $("summary-total-pnl");
  totalPnlEl.textContent = fmtSigned(data.total_pnl_usdt, 2);
  applyValueColor(totalPnlEl, data.total_pnl_usdt);

  const avgPnlEl = $("summary-avg-pnl");
  avgPnlEl.textContent = fmtSigned(data.avg_pnl_usdt, 2);
  applyValueColor(avgPnlEl, data.avg_pnl_usdt);

  $("summary-profit-factor").textContent =
    perfSummary && perfSummary.profit_factor !== null
      ? fmtNumber(perfSummary.profit_factor, 2)
      : "-";

  const ddText = perfSummary
    ? `${fmtMaybe(perfSummary.max_drawdown_usdt, 2)} USDT / ${
        perfSummary.max_drawdown_pct !== null ? `${fmtNumber(perfSummary.max_drawdown_pct, 2)}%` : "-"
      }`
    : "-";
  $("summary-max-dd").textContent = ddText;
}

function renderEngineStatus(data) {
  state.engine = data;
  applyStatusChip("engine-status-chip", data.status || "대기");
  applyStatusChip("engine-top-status", data.status || "대기");

  $("engine-db-latency").textContent = `${fmtMaybe(data.db_latency_ms, 0)} ms`;
  $("engine-recent-errors").textContent = fmtMaybe(data.recent_errors, 0);
  $("engine-recent-skips").textContent = fmtMaybe(data.recent_skips, 0);

  const watchdogReason =
    data.latest_watchdog && data.latest_watchdog.reason
      ? normalizeWatchdogReason(data.latest_watchdog.reason)
      : "-";
  $("engine-watchdog-reason").textContent = watchdogReason;

  const wsStatus = data.ws_status;
  if (!wsStatus || typeof wsStatus !== "object") {
    throw new Error("engine_status: ws_status invalid");
  }

  const sourceLabel = wsStatus.source === "database"
    ? "DB 기준 확인"
    : (wsStatus.available ? "확인 가능" : "확인 불가");
  $("engine-ws-available").textContent = sourceLabel;

  $("engine-best-bid").textContent =
    wsStatus.orderbook && wsStatus.orderbook.bestBid !== undefined && wsStatus.orderbook.bestBid !== null
      ? fmtMaybe(wsStatus.orderbook.bestBid, 4)
      : "-";
  $("engine-best-ask").textContent =
    wsStatus.orderbook && wsStatus.orderbook.bestAsk !== undefined && wsStatus.orderbook.bestAsk !== null
      ? fmtMaybe(wsStatus.orderbook.bestAsk, 4)
      : "-";

  const buffersBox = $("engine-buffers-box");
  if (!buffersBox) throw new Error("engine-buffers-box missing");
  buffersBox.innerHTML = "";

  const buffers = wsStatus.buffers;
  if (!buffers || typeof buffers !== "object") {
    throw new Error("engine_status: buffers invalid");
  }

  Object.entries(buffers).forEach(([tf, v]) => {
    const ok = !!(v && v.ok);
    const len = v && v.len !== undefined && v.len !== null ? String(v.len) : "오류";
    const div = document.createElement("div");
    div.className = "mini-row";
    div.innerHTML = `
      <div class="flex items-center gap-2">
        <span class="tiny-dot ${ok ? "tiny-dot-ok" : "tiny-dot-fatal"}"></span>
        <span>${tf} 버퍼</span>
      </div>
      <div class="${ok ? "value-positive" : "value-negative"} mono">${len}</div>
    `;
    buffersBox.appendChild(div);
  });
}

function renderPosition(data) {
  state.position = data;
  const status = data.status || "대기";
  applyStatusChip("position-status-chip", status);

  $("position-symbol").textContent = data.symbol || "-";
  $("position-side").textContent = data.side || "-";
  $("position-entry-price").textContent = data.entry_price !== undefined ? fmtMaybe(data.entry_price, 4) : "-";
  $("position-current-price").textContent = data.current_price !== undefined ? fmtMaybe(data.current_price, 4) : "-";

  const pnlEl = $("position-pnl");
  pnlEl.textContent = `${fmtSigned(data.pnl_usdt, 2)} USDT / ${fmtSigned(data.pnl_pct, 2)}%`;
  applyValueColor(pnlEl, data.pnl_usdt);

  $("position-leverage").textContent = data.leverage !== undefined ? `${fmtMaybe(data.leverage, 1)}x` : "-";
  $("position-qty").textContent = data.quantity !== undefined ? fmtMaybe(data.quantity, 6) : "-";
  $("position-liquidation").textContent =
    data.liquidation_price !== null && data.liquidation_price !== undefined
      ? fmtMaybe(data.liquidation_price, 4)
      : "-";
}

function renderDecision(data) {
  state.decision = data;

  const action = data.action || "대기";
  applyStatusChip("decision-action-chip", action);

  $("decision-summary").textContent = data.summary || "-";
  $("decision-entry-score").textContent = data.entry_score !== undefined ? fmtMaybe(data.entry_score, 3) : "-";

  const exitScoreText =
    data.exit_score !== undefined && data.exit_score !== null
      ? `${fmtMaybe(data.exit_score, 3)} / ${data.threshold !== undefined && data.threshold !== null ? fmtMaybe(data.threshold, 3) : "-"}` :
      data.threshold !== undefined && data.threshold !== null
        ? `- / ${fmtMaybe(data.threshold, 3)}`
        : "-";

  $("decision-exit-score").textContent = exitScoreText;
  $("decision-trend-strength").textContent = data.trend_strength !== undefined ? fmtMaybe(data.trend_strength, 3) : "-";
  $("decision-spread").textContent = data.spread !== undefined ? fmtMaybe(data.spread, 6) : "-";
  $("decision-orderbook").textContent =
    `호가창: ${data.orderbook_imbalance !== undefined ? fmtMaybe(data.orderbook_imbalance, 3) : "-"}`;

  const reasonsBox = $("decision-reasons-box");
  if (!reasonsBox) throw new Error("decision-reasons-box missing");
  reasonsBox.innerHTML = "";

  const reasons = Array.isArray(data.reasons) ? data.reasons : [];
  if (!reasons.length) {
    const span = document.createElement("span");
    span.className = "reason-pill";
    span.textContent = "근거 없음";
    reasonsBox.appendChild(span);
  } else {
    reasons.forEach((r) => {
      const span = document.createElement("span");
      span.className = "reason-pill";
      span.textContent = normalizeDecisionReason(r);
      reasonsBox.appendChild(span);
    });
  }
}

function renderErrorCounts(items) {
  state.errorCounts = items;
  $("error-count-error").textContent = fmtMaybe(items.ERROR ?? 0, 0);
  $("error-count-watchdog").textContent = fmtMaybe(items.WATCHDOG ?? 0, 0);

  const total = Number(items.ERROR ?? 0) + Number(items.WATCHDOG ?? 0);
  if (total > 0) applyStatusChip("error-panel-chip", "이상 감시 중");
  else applyStatusChip("error-panel-chip", "정상 감시 중");
}

function renderErrorsTable(items) {
  state.recentErrors = Array.isArray(items) ? items.slice(0, 100) : [];
  const body = $("error-table-body");
  if (!body) throw new Error("error-table-body missing");
  body.innerHTML = "";

  if (!state.recentErrors.length) {
    body.innerHTML = `<tr><td colspan="4" class="text-slate-500">표시할 문제가 없습니다.</td></tr>`;
    return;
  }

  state.recentErrors.forEach((item) => {
    const tr = document.createElement("tr");
    const typeRaw = String(item.event_type || "-");
    const type = typeRaw === "WATCHDOG" ? "감시 경고" : typeRaw === "ERROR" ? "오류" : typeRaw;
    const reason = normalizeReasonText(item.reason || "-");
    const symbol = String(item.symbol || "-");
    const ts = String(item.ts_utc || "-");
    tr.innerHTML = `
      <td class="mono text-xs">${ts}</td>
      <td>${type}</td>
      <td class="max-w-[22rem] break-words">${reason}</td>
      <td>${symbol}</td>
    `;
    body.appendChild(tr);
  });
  
}

function prependLiveError(item) {
  if (!item || typeof item !== "object") return;
  state.recentErrors.unshift(item);
  state.recentErrors = state.recentErrors.slice(0, 100);
  renderErrorsTable(state.recentErrors);
}

function renderTradesTable(items) {
  state.recentTrades = Array.isArray(items) ? items.slice(0, 100) : [];
  const body = $("trades-table-body");
  if (!body) throw new Error("trades-table-body missing");
  body.innerHTML = "";

  if (!state.recentTrades.length) {
    body.innerHTML = `<tr><td colspan="6" class="text-slate-500">표시할 거래가 없습니다.</td></tr>`;
    return;
  }

state.recentTrades.forEach((item) => {

  const pnl = Number(item.pnl_usdt || 0);
  const tr = document.createElement("tr");

  const ts = item.exit_ts || item.entry_ts;
  const timeKST = ts
    ? new Date(ts).toLocaleString("ko-KR", { timeZone: "Asia/Seoul" })
    : "-";

  tr.innerHTML = `
      <td class="mono text-xs">${timeKST}</td>
      <td>${item.trade_type || "-"} / ${item.side_label || "-"}</td>
      <td class="mono text-xs">${item.entry_price !== undefined ? fmtMaybe(item.entry_price, 4) : "-"} → ${item.exit_price !== undefined && item.exit_price !== null ? fmtMaybe(item.exit_price, 4) : "-"}</td>
      <td>${item.regime_label || "-"} / ${item.strategy || "-"}</td>
      <td>${item.close_reason_label || "-"}</td>
      <td class="${pnl > 0 ? "value-positive" : pnl < 0 ? "value-negative" : "value-neutral"} mono">${fmtSigned(pnl, 2)}</td>
  `;

  body.appendChild(tr);
});

}

function prependLiveTrade(item) {
  if (!item || typeof item !== "object") return;
  state.recentTrades.unshift(item);
  state.recentTrades = state.recentTrades.slice(0, 100);
  renderTradesTable(state.recentTrades);
}

function renderSkipReasons(items) {
  const box = $("skipReasonsBox");
  if (!box) throw new Error("skipReasonsBox missing");
  box.innerHTML = "";

  if (!Array.isArray(items) || !items.length) {
    box.innerHTML = `<div class="text-slate-500 text-sm">표시할 데이터가 없습니다.</div>`;
    return;
  }

  items.forEach((it) => {
    const reasonRaw = String(it.reason || "");
    const reason = normalizeReasonText(reasonRaw);
    const n = Number(it.n);
    if (!reason || Number.isNaN(n)) throw new Error("skipReasons invalid item");
    const div = document.createElement("div");
    div.className = "mini-row";
    div.innerHTML = `
      <div class="max-w-[80%] leading-6">${reason}</div>
      <div class="mono">${fmtMaybe(n, 0)}</div>
    `;
    box.appendChild(div);
  });
}

function renderAiList(targetId, items) {
  const box = $(targetId);
  if (!box) throw new Error(`${targetId} missing`);
  box.innerHTML = "";

  if (!Array.isArray(items) || !items.length) {
    const li = document.createElement("li");
    li.textContent = "없음";
    box.appendChild(li);
    return;
  }

  items.forEach((item) => {
    const li = document.createElement("li");
    li.textContent = String(item || "-");
    box.appendChild(li);
  });
}

function renderAiTags(targetId, items) {
  const box = $(targetId);
  if (!box) throw new Error(`${targetId} missing`);
  box.innerHTML = "";

  if (!Array.isArray(items) || !items.length) {
    const span = document.createElement("span");
    span.className = "ai-tag";
    span.textContent = "없음";
    box.appendChild(span);
    return;
  }

  items.forEach((item) => {
    const span = document.createElement("span");
    span.className = "ai-tag";
    span.textContent = normalizeUsedInputTag(item);
    box.appendChild(span);
  });
}

function renderAiAnalysis(prefix, data) {
  if (!data || typeof data !== "object") {
    throw new Error("AI 분석 응답 구조 오류");
  }

  const answer = typeof data.answer_ko === "string" ? data.answer_ko : null;
  const scope = typeof data.scope === "string" ? data.scope : null;
  const confidence = data.confidence;

  if (!answer || !scope) {
    throw new Error("AI 분석 핵심 필드 누락");
  }

  $(`${prefix}-analysis-answer`).textContent = answer;
  $(`${prefix}-analysis-scope`).textContent = normalizeAiScope(scope);
  $(`${prefix}-analysis-confidence`).textContent =
    typeof confidence === "number" ? `${fmtNumber(confidence * 100, 1)}%` : "-";

  renderAiList(`${prefix}-analysis-causes`, data.root_causes);
  renderAiList(`${prefix}-analysis-recommendations`, data.recommendations);
  renderAiTags(`${prefix}-analysis-used-inputs`, data.used_inputs);

  hideEl(`${prefix}-analysis-loading`);
  hideEl(`${prefix}-analysis-error`);
  showEl(`${prefix}-analysis-result`);
}

async function requestQuantAnalysis() {
  const questionEl = $("quant-question");
  const includeExternalEl = $("quant-include-external");
  if (!questionEl || !includeExternalEl) {
    throw new Error("quant analysis input elements missing");
  }

  const question = questionEl.value.trim();
  const includeExternal = includeExternalEl.checked;

  hideEl("quant-analysis-result");
  clearWidgetError("quant-analysis-error");
  showEl("quant-analysis-loading");

  if (!question) {
    hideEl("quant-analysis-loading");
    setWidgetError("quant-analysis-error", "질문을 입력해 주세요.");
    return;
  }

  try {
    const url = `${API.quantAnalysis}?question=${encodeURIComponent(question)}&include_external_market=${includeExternal ? "true" : "false"}`;
    const data = await fetchJsonStrict(url, "통합 분석", 30000);
    renderAiAnalysis("quant", data);
  } catch (e) {
    hideEl("quant-analysis-loading");
    setWidgetError("quant-analysis-error", `ERROR: ${e.message || String(e)}`);
    showBannerError(e.message || String(e));
  }
}

async function requestMarketAnalysis() {
  const questionEl = $("market-question");
  if (!questionEl) {
    throw new Error("market-question element missing");
  }

  const question = questionEl.value.trim();

  hideEl("market-analysis-result");
  clearWidgetError("market-analysis-error");
  showEl("market-analysis-loading");

  if (!question) {
    hideEl("market-analysis-loading");
    setWidgetError("market-analysis-error", "질문을 입력해 주세요.");
    return;
  }

  try {
    const url = `${API.marketAnalysis}?question=${encodeURIComponent(question)}`;
    const data = await fetchJsonStrict(url, "외부 시장 분석", 30000);
    renderAiAnalysis("market", data);
  } catch (e) {
    hideEl("market-analysis-loading");
    setWidgetError("market-analysis-error", `ERROR: ${e.message || String(e)}`);
    showBannerError(e.message || String(e));
  }
}

async function loadSummary() {
  try {
    const [summary, perfSummary] = await Promise.all([
      fetchJsonStrict(API.summary, "요약"),
      fetchJsonStrict(API.performanceSummary, "성과 요약"),
    ]);
    renderSummary(summary, perfSummary);
  } catch (e) {
    showBannerError(e.message || String(e));
    $("summary-total-trades").textContent = "오류";
    $("summary-win-rate").textContent = "오류";
    $("summary-win-loss").textContent = "오류";
    $("summary-total-pnl").textContent = "오류";
    $("summary-avg-pnl").textContent = "오류";
    $("summary-profit-factor").textContent = "오류";
    $("summary-max-dd").textContent = "오류";
  }
}

async function loadDailyPnlChart() {
  clearWidgetError("dailyPnlError");
  try {
    const data = await fetchJsonStrict(API.dailyPnl, "일별 손익");
    const items = Array.isArray(data.items) ? data.items : null;
    if (!items) throw new Error("일별 손익: items invalid");

    const labels = items.map((x) => String(x.date));
    const pnls = items.map((x) => {
      const v = Number(x.pnl_usdt);
      if (Number.isNaN(v)) throw new Error("일별 손익: pnl_usdt invalid");
      return v;
    });

    dailyPnlChart = createOrUpdateChart(
      dailyPnlChart,
      "dailyPnlChart",
      "line",
      labels,
      pnls,
      "일별 손익"
    );
  } catch (e) {
    setWidgetError("dailyPnlError", `ERROR: ${e.message || String(e)}`);
    showBannerError(e.message || String(e));
  }
}

async function loadEquityCurveChart() {
  clearWidgetError("equityCurveError");
  try {
    const data = await fetchJsonStrict(API.equityCurve, "누적 손익 흐름");
    const items = Array.isArray(data.items) ? data.items : null;
    if (!items) throw new Error("누적 손익 흐름: items invalid");

    const labels = items.map((x) => String(x.exit_ts || ""));
    const values = items.map((x) => {
      const v = Number(x.equity_usdt);
      if (Number.isNaN(v)) throw new Error("누적 손익 흐름: equity_usdt invalid");
      return v;
    });

    equityCurveChart = createOrUpdateChart(
      equityCurveChart,
      "equityCurveChart",
      "line",
      labels,
      values,
      "누적 손익"
    );
  } catch (e) {
    setWidgetError("equityCurveError", `ERROR: ${e.message || String(e)}`);
    showBannerError(e.message || String(e));
  }
}

async function loadDrawdownChart() {
  clearWidgetError("drawdownError");
  try {
    const data = await fetchJsonStrict(API.drawdownCurve, "하락폭 흐름");
    const items = Array.isArray(data.items) ? data.items : null;
    if (!items) throw new Error("하락폭 흐름: items invalid");

    const labels = items.map((x) => String(x.exit_ts || ""));
    const values = items.map((x) => {
      const v = x.drawdown_pct === null || x.drawdown_pct === undefined ? 0 : Number(x.drawdown_pct);
      if (Number.isNaN(v)) throw new Error("하락폭 흐름: drawdown_pct invalid");
      return v;
    });

    drawdownChart = createOrUpdateChart(
      drawdownChart,
      "drawdownChart",
      "line",
      labels,
      values,
      "하락폭"
    );
  } catch (e) {
    setWidgetError("drawdownError", `ERROR: ${e.message || String(e)}`);
    showBannerError(e.message || String(e));
  }
}

async function loadEntryScoreHist() {
  clearWidgetError("entryScoreError");
  try {
    const data = await fetchJsonStrict(API.entryScores, "진입 점수 분포");
    const labels = Array.isArray(data.hist_labels) ? data.hist_labels : null;
    const counts = Array.isArray(data.hist_counts) ? data.hist_counts : null;
    if (!labels || !counts) throw new Error("진입 점수 분포: histogram invalid");
    if (labels.length !== counts.length) throw new Error("진입 점수 분포: histogram length mismatch");

    entryScoreHistChart = createOrUpdateChart(
      entryScoreHistChart,
      "entryScoreHistChart",
      "bar",
      labels,
      counts,
      "진입 점수 분포"
    );
  } catch (e) {
    setWidgetError("entryScoreError", `ERROR: ${e.message || String(e)}`);
    showBannerError(e.message || String(e));
  }
}

async function loadSkipReasons() {
  clearWidgetError("skipReasonsError");
  try {
    const data = await fetchJsonStrict(API.skipReasons, "진입 안 한 이유");
    const items = Array.isArray(data.items) ? data.items : null;
    if (!items) throw new Error("진입 안 한 이유: items invalid");
    renderSkipReasons(items);
  } catch (e) {
    setWidgetError("skipReasonsError", `ERROR: ${e.message || String(e)}`);
    showBannerError(e.message || String(e));
  }
}

async function loadSkipHourly() {
  clearWidgetError("skipHourlyError");
  try {
    const data = await fetchJsonStrict(API.skipHourly, "시간대별 진입 안 함");
    const items = Array.isArray(data.items) ? data.items : null;
    if (!items) throw new Error("시간대별 진입 안 함: items invalid");

    const labels = items.map((x) => String(x.hour_kst));
    const vals = items.map((x) => {
      const v = Number(x.n);
      if (Number.isNaN(v)) throw new Error("시간대별 진입 안 함: n invalid");
      return v;
    });

    skipHourlyChart = createOrUpdateChart(
      skipHourlyChart,
      "skipHourlyChart",
      "bar",
      labels,
      vals,
      "시간대별 진입 안 함"
    );
  } catch (e) {
    setWidgetError("skipHourlyError", `ERROR: ${e.message || String(e)}`);
    showBannerError(e.message || String(e));
  }
}

async function loadEngineStatus() {
  clearWidgetError("engineError");
  try {
    const data = await fetchJsonStrict(API.engineStatus, "핵심 상태");
    renderEngineStatus(data);
  } catch (e) {
    setWidgetError("engineError", `ERROR: ${e.message || String(e)}`);
    showBannerError(e.message || String(e));
  }
}

async function loadPosition() {
  clearWidgetError("positionError");
  try {
    const data = await fetchJsonStrict(API.positionCurrent, "현재 보유");
    renderPosition(data);
  } catch (e) {
    setWidgetError("positionError", `ERROR: ${e.message || String(e)}`);
    showBannerError(e.message || String(e));
  }
}

async function loadDecision() {
  clearWidgetError("decisionError");
  try {
    const data = await fetchJsonStrict(API.decisionLatest, "판단 이유");
    renderDecision(data);
  } catch (e) {
    setWidgetError("decisionError", `ERROR: ${e.message || String(e)}`);
    showBannerError(e.message || String(e));
  }
}

async function loadErrors() {
  clearWidgetError("errorMonitorError");
  try {
    const [counts, recent] = await Promise.all([
      fetchJsonStrict(API.errorCounts, "문제 개수"),
      fetchJsonStrict(API.errorsRecent, "최근 문제"),
    ]);

    if (!counts.items || typeof counts.items !== "object") {
      throw new Error("문제 개수: items invalid");
    }
    if (!Array.isArray(recent.items)) {
      throw new Error("최근 문제: items invalid");
    }

    renderErrorCounts(counts.items);
    renderErrorsTable(recent.items);
  } catch (e) {
    setWidgetError("errorMonitorError", `ERROR: ${e.message || String(e)}`);
    showBannerError(e.message || String(e));
  }
}

async function loadTrades() {
  clearWidgetError("tradesError");
  try {
    const data = await fetchJsonStrict(API.tradesRecent, "최근 거래");
    if (!Array.isArray(data.items)) throw new Error("최근 거래: items invalid");
    renderTradesTable(data.items);
  } catch (e) {
    setWidgetError("tradesError", `ERROR: ${e.message || String(e)}`);
    showBannerError(e.message || String(e));
  }
}

function connectDashboardWebSocket() {
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  const url = `${protocol}//${window.location.host}/ws/dashboard`;

  if (ws) {
    try {
      ws.close();
    } catch (_) {
      // close 실패는 무시
    }
  }

  applyStatusChip("ws-status-chip", "WebSocket 연결 중");
  ws = new WebSocket(url);

  ws.onopen = () => {
    applyStatusChip("ws-status-chip", "WebSocket 연결됨");
    try {
      ws.send(JSON.stringify({ op: "snapshot" }));
    } catch (e) {
      showBannerError(`websocket snapshot request failed: ${e.message || String(e)}`);
    }
  };

  ws.onmessage = (event) => {
    let data;
    try {
      data = JSON.parse(event.data);
    } catch (_) {
      showBannerError("websocket: JSON 형식 오류");
      return;
    }

    if (!data || typeof data !== "object") {
      showBannerError("websocket: 응답 구조 오류");
      return;
    }

    setWsLastMessage(data.ts_ms || (data.payload && data.payload.ts_ms) || null);
    setLastUpdatedKST();

    if (data.type === "system") {
      const eventName = String(data.event || "");
      if (eventName === "snapshot") {
        const items = data.payload && Array.isArray(data.payload.items) ? data.payload.items : null;
        if (!items) {
          showBannerError("websocket snapshot: items invalid");
          return;
        }
        items.forEach(applyLiveEnvelope);
      } else if (eventName === "pong") {
        // no-op
      }
      return;
    }

    applyLiveEnvelope(data);
  };

  ws.onerror = () => {
    applyStatusChip("ws-status-chip", "WebSocket 오류");
  };

  ws.onclose = () => {
    applyStatusChip("ws-status-chip", "WebSocket 종료");
  };
}

function applyLiveEnvelope(envelope) {
  if (!envelope || typeof envelope !== "object") {
    showBannerError("websocket envelope invalid");
    return;
  }

  const type = String(envelope.type || "");
  const payload = envelope.payload;
  if (!type || !payload || typeof payload !== "object") {
    showBannerError("websocket payload invalid");
    return;
  }

  switch (type) {
    case "engine_status":
      renderEngineStatus(payload);
      break;
    case "decision":
      renderDecision(payload);
      break;
    case "position":
      renderPosition(payload);
      break;
    case "error":
    case "watchdog":
      prependLiveError({
        ts_utc: payload.ts_utc || new Date().toISOString(),
        event_type: type === "error" ? "ERROR" : "WATCHDOG",
        reason: payload.reason || payload.summary || type,
        symbol: payload.symbol || "-"
      });
      break;
    case "trade":
      prependLiveTrade(payload);
      break;
    default:
      showBannerError(`websocket: unsupported type ${type}`);
      break;
  }
}

function bindQuickQuestionButtons() {
  document.querySelectorAll(".ai-quick-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      const targetId = btn.getAttribute("data-target");
      const question = btn.getAttribute("data-question") || "";
      const target = targetId ? $(targetId) : null;
      if (!target) {
        throw new Error(`quick question target missing: ${targetId}`);
      }
      target.value = question;
      target.focus();
    });
  });
}

async function loadAll() {
  hideBannerError();

  await Promise.all([
    loadSummary(),
    loadDailyPnlChart(),
    loadEquityCurveChart(),
    loadDrawdownChart(),
    loadEntryScoreHist(),
    loadSkipReasons(),
    loadSkipHourly(),
    loadEngineStatus(),
    loadPosition(),
    loadDecision(),
    loadErrors(),
    loadTrades(),
  ]);

  connectDashboardWebSocket();
  setLastUpdatedKST();
}

window.addEventListener("resize", () => {
  enforceDesktopOnly();
});

window.addEventListener("DOMContentLoaded", async () => {
  if (!enforceDesktopOnly()) return;

  const refreshBtn = $("refresh-btn");
  const errorBannerClose = $("error-banner-close");
  const quantSearchBtn = $("quant-search-btn");
  const marketSearchBtn = $("market-search-btn");
  const quantQuestion = $("quant-question");
  const marketQuestion = $("market-question");

  if (!refreshBtn || !errorBannerClose || !quantSearchBtn || !marketSearchBtn || !quantQuestion || !marketQuestion) {
    throw new Error("required dashboard controls missing");
  }

  refreshBtn.addEventListener("click", () => {
    loadAll();
  });

  errorBannerClose.addEventListener("click", () => hideBannerError());

  quantSearchBtn.addEventListener("click", async () => {
    await requestQuantAnalysis();
  });

  marketSearchBtn.addEventListener("click", async () => {
    await requestMarketAnalysis();
  });

  quantQuestion.addEventListener("keydown", async (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
      e.preventDefault();
      await requestQuantAnalysis();
    }
  });

  marketQuestion.addEventListener("keydown", async (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
      e.preventDefault();
      await requestMarketAnalysis();
    }
  });

  bindQuickQuestionButtons();
  await loadAll();
});