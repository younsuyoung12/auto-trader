/*
========================================================
FILE: dashboard/static/js/dashboard.js
ROLE:
- 대시보드 프론트엔드 동작 전담
- 요약/성과 차트/핵심 상태/현재 보유/계좌 보유/판단 이유/최근 오류/최근 거래를 렌더링한다
- Dashboard WebSocket 실시간 반영과 AI 분석 검색을 처리한다

CORE RESPONSIBILITIES:
- 초기 화면은 REST 1회 조회로 구성하고 이후 WebSocket으로 실시간 반영
- 핵심 상태 카드에 WS 상태 / 데이터 신선도 / 마지막 신호 / 마지막 거래 표시
- 실시간 데이터 상태를 버퍼 개수 대신 latency 중심으로 표시
- 계좌 보유 카드와 계좌 손익/수익률을 운영형 UI로 표시
- INIT / READY / delayed / stale 상태를 장애와 분리하여 렌더링

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- API 실패/JSON 오류/구조 오류는 즉시 화면에 표시
- 기존 값 묵시적 유지 금지
- polling 금지, WebSocket 실시간 반영
- 모바일 접속 차단
- 민감정보 출력 금지
- 실시간 데이터 상태는 WebSocket 기준 계약을 우선 사용
- source=database 는 정상으로 숨기지 않고 개선 필요 상태로 드러낸다

CHANGE HISTORY:
- 2026-03-11:
  1) FEAT(UI): 핵심 상태 카드에 WS 상태 / 데이터 신선도 / 마지막 신호 / 마지막 거래 렌더링 추가
  2) FEAT(UI): 계좌 보유 카드 렌더링 추가
  3) FIX(ARCH): 실시간 버퍼 개수 렌더링 제거, latency 기반 실시간 데이터 상태 렌더링으로 전환
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
  account: null,
  errorCounts: null,
  recentErrors: [],
  recentTrades: [],
};

function requireElement(id) {
  const el = $(id);
  if (!el) {
    throw new Error(`${id} element missing`);
  }
  return el;
}

function showBannerError(msg) {
  const banner = requireElement("error-banner");
  const messageEl = requireElement("error-banner-msg");
  messageEl.textContent = String(msg || "알 수 없는 오류");
  banner.classList.remove("hidden");
}

function hideBannerError() {
  const banner = requireElement("error-banner");
  const messageEl = requireElement("error-banner-msg");
  banner.classList.add("hidden");
  messageEl.textContent = "";
}

function setWidgetError(elemId, msg) {
  const el = requireElement(elemId);
  el.textContent = String(msg || "오류");
  el.classList.remove("hidden");
}

function clearWidgetError(elemId) {
  const el = requireElement(elemId);
  el.classList.add("hidden");
  el.textContent = "";
}

function setWidgetNote(elemId, msg) {
  const el = requireElement(elemId);
  el.textContent = String(msg || "");
  el.classList.remove("hidden");
}

function clearWidgetNote(elemId) {
  const el = requireElement(elemId);
  el.classList.add("hidden");
  el.textContent = "";
}

function showEl(id) {
  requireElement(id).classList.remove("hidden");
}

function hideEl(id) {
  requireElement(id).classList.add("hidden");
}

function isMobileBlocked() {
  const ua = navigator.userAgent || "";
  const isMobileUA =
    /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(ua);
  const isSmallViewport = window.innerWidth < 1024;
  return isMobileUA || isSmallViewport;
}

function enforceDesktopOnly() {
  const blocker = requireElement("desktop-blocker");
  const blocked = isMobileBlocked();

  if (blocked) {
    blocker.classList.add("show");
    try {
      alert("PC 전용 대시보드입니다");
    } catch (_) {
      // UI alert 실패는 대시보드 동작과 무관
    }
    return false;
  }

  blocker.classList.remove("show");
  return true;
}

async function fetchJsonStrict(url, label, timeoutMs = 15000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  let res;
  try {
    res = await fetch(url, {
      signal: controller.signal,
      cache: "no-store",
    });
  } catch (e) {
    clearTimeout(timer);
    throw new Error(`${label}: 요청 실패 (${e && e.name ? e.name : "ERR"})`);
  } finally {
    clearTimeout(timer);
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

function requireObjectStrict(value, label) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${label} invalid`);
  }
  return value;
}

function requireArrayStrict(value, label) {
  if (!Array.isArray(value)) {
    throw new Error(`${label} invalid`);
  }
  return value;
}

function requireTextStrict(value, label) {
  const s = String(value || "").trim();
  if (!s) {
    throw new Error(`${label} invalid`);
  }
  return s;
}

function requireFiniteNumberStrict(value, label) {
  const n = Number(value);
  if (!Number.isFinite(n)) {
    throw new Error(`${label} invalid`);
  }
  return n;
}

function requireNullableFiniteNumberStrict(value, label) {
  if (value === null || value === undefined) {
    return null;
  }
  return requireFiniteNumberStrict(value, label);
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

function formatLatencySec(secValue) {
  const n = Number(secValue);
  if (!Number.isFinite(n)) {
    return "-";
  }
  return `${fmtNumber(n, 1)} sec`;
}

function parseDateLike(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : value;
  }

  let date;
  if (typeof value === "number") {
    date = new Date(value);
  } else {
    const raw = String(value).trim();
    if (!raw) return null;
    if (/^\d+$/.test(raw)) {
      date = new Date(Number(raw));
    } else {
      date = new Date(raw);
    }
  }

  return Number.isNaN(date.getTime()) ? null : date;
}

function formatTimestampKst(value, mode = "short") {
  const date = parseDateLike(value);
  if (!date) return "-";

  const options = {
    timeZone: "Asia/Seoul",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  };

  if (mode === "full") {
    options.year = "numeric";
    options.month = "2-digit";
    options.day = "2-digit";
  } else {
    options.month = "2-digit";
    options.day = "2-digit";
  }

  return new Intl.DateTimeFormat("ko-KR", options).format(date);
}

function setLastUpdatedKST() {
  const label = requireElement("last-updated-label");
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
  const label = requireElement("ws-last-message-label");

  if (!tsMs) {
    label.textContent = "실시간 수신: -";
    return;
  }

  const d = parseDateLike(tsMs);
  if (!d) {
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
  const chip = typeof el === "string" ? requireElement(el) : el;
  if (!chip) {
    throw new Error("status chip element missing");
  }

  let cls = "status-neutral";
  let dot = "tiny-dot-neutral";
  const raw = String(statusText || "");
  const s = raw.toUpperCase();

  if (
    s.includes("OK") ||
    s === "OPEN" ||
    s === "CONNECTED" ||
    s === "ENTRY" ||
    s === "HOLD" ||
    s === "QUANT_ANALYSIS" ||
    s === "MARKET_ANALYSIS" ||
    s === "RUNNING" ||
    s === "READY" ||
    raw.includes("정상") ||
    raw.includes("연결됨")
  ) {
    cls = "status-ok";
    dot = "tiny-dot-ok";
  } else if (
    s.includes("WARNING") ||
    s.includes("WARN") ||
    s.includes("DELAYED") ||
    s.includes("RECONNECT") ||
    raw.includes("지연") ||
    raw.includes("경고")
  ) {
    cls = "status-warning";
    dot = "tiny-dot-warning";
  } else if (
    s.includes("FATAL") ||
    s.includes("ERROR") ||
    s.includes("EXIT") ||
    s.includes("NO_ENTRY") ||
    s.includes("CLOSED") ||
    s.includes("FLAT") ||
    s.includes("OUT_OF_SCOPE") ||
    s.includes("DISCONNECT") ||
    s.includes("STOP") ||
    raw.includes("오류") ||
    raw.includes("중지") ||
    raw.includes("미연결")
  ) {
    cls = "status-fatal";
    dot = "tiny-dot-fatal";
  } else if (s.includes("INIT") || raw.includes("초기") || raw.includes("대기")) {
    cls = "status-neutral";
    dot = "tiny-dot-neutral";
  }

  chip.className = `status-chip ${cls}`;
  chip.innerHTML = `<span class="tiny-dot ${dot}"></span>${raw || "-"}`;
}

function removeValueToneClasses(el) {
  if (!el) return;
  el.classList.remove(
    "value-positive",
    "value-warning",
    "value-negative",
    "value-neutral",
    "value-rise",
    "value-fall",
    "value-flat"
  );
}

function applyValueColor(el, num) {
  if (!el) {
    throw new Error("value color element missing");
  }
  const n = Number(num);
  removeValueToneClasses(el);
  if (Number.isNaN(n)) return;

  if (n > 0) el.classList.add("value-positive");
  else if (n < 0) el.classList.add("value-negative");
  else el.classList.add("value-neutral");
}

function applyLatencyColor(el, secValue) {
  if (!el) {
    throw new Error("latency color element missing");
  }
  const n = Number(secValue);
  removeValueToneClasses(el);

  if (!Number.isFinite(n)) {
    el.classList.add("value-neutral");
    return;
  }
  if (n <= 2) {
    el.classList.add("value-positive");
    return;
  }
  if (n <= 5) {
    el.classList.add("value-warning");
    return;
  }
  el.classList.add("value-negative");
}

function applyRiseFallColor(el, num) {
  if (!el) {
    throw new Error("rise/fall color element missing");
  }
  const n = Number(num);
  removeValueToneClasses(el);

  if (!Number.isFinite(n)) {
    el.classList.add("value-flat");
    return;
  }
  if (n > 0) {
    el.classList.add("value-rise");
    return;
  }
  if (n < 0) {
    el.classList.add("value-fall");
    return;
  }
  el.classList.add("value-flat");
}

function applyConnectionStateValue(el, statusText) {
  if (!el) {
    throw new Error("connection state element missing");
  }
  const raw = String(statusText || "");
  const s = raw.toUpperCase();
  removeValueToneClasses(el);

  if (
    s.includes("CONNECTED") ||
    s.includes("RUNNING") ||
    s.includes("READY") ||
    s.includes("OK") ||
    raw.includes("정상") ||
    raw.includes("연결됨")
  ) {
    el.classList.add("value-positive");
    return;
  }

  if (
    s.includes("RECONNECT") ||
    s.includes("DELAY") ||
    s.includes("WARN") ||
    raw.includes("지연") ||
    raw.includes("경고")
  ) {
    el.classList.add("value-warning");
    return;
  }

  if (
    s.includes("DISCONNECT") ||
    s.includes("ERROR") ||
    s.includes("STOP") ||
    s.includes("FATAL") ||
    raw.includes("오류") ||
    raw.includes("중지") ||
    raw.includes("미연결")
  ) {
    el.classList.add("value-negative");
    return;
  }

  el.classList.add("value-neutral");
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
    ["no_watchdog_event", "초기 상태(아직 WATCHDOG 없음)"],
    ["no_decision_event", "초기 상태(아직 DECISION 없음)"],
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
  if (lower === "no_decision_event") return "초기 상태(아직 DECISION 없음)";

  return raw
    .replaceAll("_", " ")
    .replace(/\bentry\b/gi, "진입")
    .replace(/\bexit\b/gi, "청산")
    .replace(/\bguard\b/gi, "보호")
    .replace(/\bsubmit\b/gi, "실행")
    .replace(/\bpassed\b/gi, "통과");
}

function normalizeWatchdogReason(reason, status = null) {
  if (String(status || "").toUpperCase() === "INIT") {
    return "초기 상태(아직 WATCHDOG 없음)";
  }
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

function buildRuntimeTooltip(runtime) {
  if (!runtime || typeof runtime !== "object") {
    throw new Error("engine_status.runtime invalid");
  }

  const status = String(runtime.status || "").trim();
  const source = String(runtime.source || "").trim();
  const reason = String(runtime.reason || "").trim();
  const staleMs = runtime.stale_ms;
  const thresholdMs = runtime.threshold_ms;
  const delayedThresholdMs = runtime.delayed_threshold_ms;
  const runtimeTsMs = runtime.ts_ms;

  const parts = [];

  if (status) parts.push(`상태: ${status}`);
  if (source) parts.push(`기준: ${source}`);
  if (reason) parts.push(`사유: ${reason}`);
  if (staleMs !== undefined && staleMs !== null) parts.push(`지연(ms): ${staleMs}`);
  if (delayedThresholdMs !== undefined && delayedThresholdMs !== null) parts.push(`경고 기준(ms): ${delayedThresholdMs}`);
  if (thresholdMs !== undefined && thresholdMs !== null) parts.push(`중지 기준(ms): ${thresholdMs}`);
  if (runtimeTsMs !== undefined && runtimeTsMs !== null) parts.push(`판정시각(ms): ${runtimeTsMs}`);

  return parts.join(" | ");
}

function renderServerRuntime(runtime) {
  const chip = requireElement("server-runtime-chip");
  if (!runtime || typeof runtime !== "object") {
    throw new Error("engine_status.runtime invalid");
  }

  const status = requireTextStrict(runtime.status, "engine_status.runtime.status");
  let chipText = status;
  if (runtime.freshness_state === "delayed") {
    chipText = `${status} (지연)`;
  }

  applyStatusChip(chip, chipText);
  chip.title = buildRuntimeTooltip(runtime);
}

function createOrUpdateChart(existing, canvasId, type, labels, data, datasetLabel) {
  const canvas = requireElement(canvasId);
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

function destroyChart(chartRefName) {
  if (chartRefName === "dailyPnlChart" && dailyPnlChart) {
    dailyPnlChart.destroy();
    dailyPnlChart = null;
  } else if (chartRefName === "entryScoreHistChart" && entryScoreHistChart) {
    entryScoreHistChart.destroy();
    entryScoreHistChart = null;
  } else if (chartRefName === "skipHourlyChart" && skipHourlyChart) {
    skipHourlyChart.destroy();
    skipHourlyChart = null;
  } else if (chartRefName === "equityCurveChart" && equityCurveChart) {
    equityCurveChart.destroy();
    equityCurveChart = null;
  } else if (chartRefName === "drawdownChart" && drawdownChart) {
    drawdownChart.destroy();
    drawdownChart = null;
  }
}

function resolveRealtimeSourceLabel(source) {
  const s = String(source || "").trim().toLowerCase();
  if (!s) return "-";
  if (s === "database") return "DB 기준(개선 필요)";
  if (s === "websocket" || s === "ws" || s === "engine_memory" || s === "memory") {
    return "WS 기준 확인";
  }
  return `${source} 기준`;
}

function renderRealtimeRows(wsStatus) {
  const box = requireElement("engine-realtime-box");
  box.innerHTML = "";

  const latencyByTf = requireObjectStrict(
    wsStatus.kline_latency_sec_by_tf,
    "engine_status.ws_status.kline_latency_sec_by_tf"
  );

  const rows = [
    ["1분 캔들 지연", requireNullableFiniteNumberStrict(latencyByTf["1m"], "kline_latency_sec_by_tf.1m")],
    ["5분 캔들 지연", requireNullableFiniteNumberStrict(latencyByTf["5m"], "kline_latency_sec_by_tf.5m")],
    ["15분 캔들 지연", requireNullableFiniteNumberStrict(latencyByTf["15m"], "kline_latency_sec_by_tf.15m")],
    ["1시간 캔들 지연", requireNullableFiniteNumberStrict(latencyByTf["1h"], "kline_latency_sec_by_tf.1h")],
    ["4시간 캔들 지연", requireNullableFiniteNumberStrict(latencyByTf["4h"], "kline_latency_sec_by_tf.4h")],
    ["오더북 지연", requireNullableFiniteNumberStrict(wsStatus.orderbook_latency_sec, "engine_status.ws_status.orderbook_latency_sec")],
    ["마지막 WS 메시지", requireNullableFiniteNumberStrict(wsStatus.last_ws_message_latency_sec, "engine_status.ws_status.last_ws_message_latency_sec")],
  ];

  rows.forEach(([label, valueSec]) => {
    const div = document.createElement("div");
    div.className = "mini-row";
    div.innerHTML = `
      <div class="flex items-center gap-2">
        <span class="tiny-dot tiny-dot-neutral"></span>
        <span>${label}</span>
      </div>
      <div class="mono">${formatLatencySec(valueSec)}</div>
    `;
    box.appendChild(div);
    applyLatencyColor(div.querySelector(".mono"), valueSec);
  });
}

function resetAccountCard(noteMessage = null) {
  clearWidgetError("accountError");
  clearWidgetNote("accountNote");

  requireElement("account-total-balance").textContent = "-";
  requireElement("account-available-balance").textContent = "-";
  requireElement("account-pnl").textContent = "-";
  requireElement("account-roi").textContent = "-";
  requireElement("account-position-margin").textContent = "-";

  removeValueToneClasses(requireElement("account-pnl"));
  removeValueToneClasses(requireElement("account-roi"));
  applyStatusChip("account-status-chip", "대기");

  if (noteMessage) {
    setWidgetNote("accountNote", noteMessage);
  }
}

function renderAccountCard(accountInfo) {
  clearWidgetError("accountError");
  clearWidgetNote("accountNote");

  if (accountInfo === null || accountInfo === undefined) {
    resetAccountCard("계좌 보유 데이터가 아직 연결되지 않았습니다.");
    applyStatusChip("account-status-chip", "미연결");
    return;
  }

  const data = requireObjectStrict(accountInfo, "engine_status.account_info");
  state.account = data;

  const status = requireTextStrict(data.status, "engine_status.account_info.status");
  const totalBalanceUsdt = requireFiniteNumberStrict(
    data.total_balance_usdt,
    "engine_status.account_info.total_balance_usdt"
  );
  const availableBalanceUsdt = requireFiniteNumberStrict(
    data.available_balance_usdt,
    "engine_status.account_info.available_balance_usdt"
  );
  const positionMarginUsdt = requireFiniteNumberStrict(
    data.position_margin_usdt,
    "engine_status.account_info.position_margin_usdt"
  );
  const balanceDeltaUsdt = requireNullableFiniteNumberStrict(
    data.balance_delta_usdt,
    "engine_status.account_info.balance_delta_usdt"
  );
  const balanceDeltaPct = requireNullableFiniteNumberStrict(
    data.balance_delta_pct,
    "engine_status.account_info.balance_delta_pct"
  );

  requireElement("account-total-balance").textContent = `${fmtNumber(totalBalanceUsdt, 2)} USDT`;
  requireElement("account-available-balance").textContent = `${fmtNumber(availableBalanceUsdt, 2)} USDT`;
  requireElement("account-position-margin").textContent = `${fmtNumber(positionMarginUsdt, 2)} USDT`;

  const pnlEl = requireElement("account-pnl");
  if (balanceDeltaUsdt === null) {
    pnlEl.textContent = "-";
    applyRiseFallColor(pnlEl, null);
  } else {
    pnlEl.textContent = `${fmtSigned(balanceDeltaUsdt, 2)} USDT`;
    applyRiseFallColor(pnlEl, balanceDeltaUsdt);
  }

  const roiEl = requireElement("account-roi");
  if (balanceDeltaPct === null) {
    roiEl.textContent = "-";
    applyRiseFallColor(roiEl, null);
  } else {
    roiEl.textContent = `${fmtSigned(balanceDeltaPct, 2)}%`;
    applyRiseFallColor(roiEl, balanceDeltaPct);
  }

  applyStatusChip("account-status-chip", status);

  if (String(status).toUpperCase() === "INIT") {
    setWidgetNote("accountNote", "계좌 기준 잔고가 아직 준비되지 않아 계좌 손익과 수익률은 초기 상태입니다.");
  }
}

function renderSummary(data, perfSummary) {
  clearWidgetNote("summaryNote");

  requireElement("summary-total-trades").textContent = fmtNumber(data.total_trades, 0);
  requireElement("summary-win-rate").textContent = `${fmtNumber(data.win_rate_pct, 1)}%`;
  requireElement("summary-win-loss").textContent =
    `이긴 거래 ${fmtNumber(data.wins, 0)} / 진 거래 ${fmtNumber(data.losses, 0)} / 본전 ${fmtNumber(data.breakevens, 0)}`;

  const totalPnlEl = requireElement("summary-total-pnl");
  totalPnlEl.textContent = fmtSigned(data.total_pnl_usdt, 2);
  applyValueColor(totalPnlEl, data.total_pnl_usdt);

  const avgPnlEl = requireElement("summary-avg-pnl");
  avgPnlEl.textContent = fmtSigned(data.avg_pnl_usdt, 2);
  applyValueColor(avgPnlEl, data.avg_pnl_usdt);

  requireElement("summary-profit-factor").textContent =
    perfSummary && perfSummary.profit_factor !== null
      ? fmtNumber(perfSummary.profit_factor, 2)
      : "-";

  const ddText = perfSummary
    ? `${fmtMaybe(perfSummary.max_drawdown_usdt, 2)} USDT / ${perfSummary.max_drawdown_pct !== null ? `${fmtNumber(perfSummary.max_drawdown_pct, 2)}%` : "-"}`
    : "-";
  requireElement("summary-max-dd").textContent = ddText;

  const dataState = String((data && data.data_state) || (perfSummary && perfSummary.data_state) || "");
  if (dataState === "INIT_NO_CLOSED_TRADES" || dataState === "INIT_NO_TRADES") {
    setWidgetNote("summaryNote", "아직 종료 거래가 없어 성과 요약은 초기 상태입니다.");
  }
}

function renderEngineStatus(data) {
  clearWidgetError("engineError");
  clearWidgetNote("engineNote");

  state.engine = data;
  applyStatusChip("engine-status-chip", data.status || "대기");
  applyStatusChip("engine-top-status", data.status || "대기");
  renderServerRuntime(data.runtime);

  requireElement("engine-db-latency").textContent = `${fmtMaybe(data.db_latency_ms, 0)} ms`;
  requireElement("engine-recent-errors").textContent = fmtMaybe(data.recent_errors, 0);
  requireElement("engine-recent-skips").textContent = fmtMaybe(data.recent_skips, 0);

  const notes = [];
  const watchdogStatus = data.latest_watchdog && data.latest_watchdog.status
    ? String(data.latest_watchdog.status)
    : null;
  const watchdogReason =
    data.latest_watchdog && data.latest_watchdog.reason
      ? normalizeWatchdogReason(data.latest_watchdog.reason, watchdogStatus)
      : "-";
  requireElement("engine-watchdog-reason").textContent = watchdogReason;

  if (watchdogStatus === "INIT") {
    notes.push("감시 상태는 초기 상태입니다. 아직 첫 WATCHDOG 이벤트가 기록되지 않았습니다.");
  }

  const wsStatus = requireObjectStrict(data.ws_status, "engine_status.ws_status");
  const connectionStatus = requireTextStrict(
    wsStatus.connection_status,
    "engine_status.ws_status.connection_status"
  );
  const dataFreshnessSec = requireFiniteNumberStrict(
    wsStatus.data_freshness_sec,
    "engine_status.ws_status.data_freshness_sec"
  );
  const lastSignalTsMs = wsStatus.last_signal_ts_ms;
  const lastTradeTsMs = wsStatus.last_trade_ts_ms;
  const sourceLabel = resolveRealtimeSourceLabel(wsStatus.source);

  requireElement("engine-realtime-source").textContent = sourceLabel;
  if (String(wsStatus.source || "").toLowerCase() === "database") {
    notes.push("실시간 데이터 상태가 아직 DB 기준입니다. WS 기준으로 전환이 필요합니다.");
  }

  const wsStatusEl = requireElement("engine-ws-status");
  wsStatusEl.textContent = connectionStatus;
  applyConnectionStateValue(wsStatusEl, connectionStatus);

  const dataFreshnessEl = requireElement("engine-data-freshness");
  dataFreshnessEl.textContent = formatLatencySec(dataFreshnessSec);
  applyLatencyColor(dataFreshnessEl, dataFreshnessSec);

  requireElement("engine-last-signal").textContent = formatTimestampKst(lastSignalTsMs, "short");
  requireElement("engine-last-trade").textContent = formatTimestampKst(lastTradeTsMs, "short");

  const orderbook = requireObjectStrict(wsStatus.orderbook, "engine_status.ws_status.orderbook");
  requireElement("engine-best-bid").textContent =
    orderbook.bestBid !== undefined && orderbook.bestBid !== null ? fmtMaybe(orderbook.bestBid, 4) : "-";
  requireElement("engine-best-ask").textContent =
    orderbook.bestAsk !== undefined && orderbook.bestAsk !== null ? fmtMaybe(orderbook.bestAsk, 4) : "-";

  renderRealtimeRows(wsStatus);
  renderAccountCard(data.account_info ?? null);

  if (notes.length) {
    setWidgetNote("engineNote", notes.join("\n"));
  }
}

function renderPosition(data) {
  clearWidgetNote("positionNote");

  state.position = data;
  const status = data.status || "대기";
  applyStatusChip("position-status-chip", status);

  requireElement("position-symbol").textContent = data.symbol || "-";
  requireElement("position-side").textContent = data.side || "-";
  requireElement("position-entry-price").textContent = data.entry_price !== undefined ? fmtMaybe(data.entry_price, 4) : "-";
  requireElement("position-current-price").textContent = data.current_price !== undefined ? fmtMaybe(data.current_price, 4) : "-";

  const pnlEl = requireElement("position-pnl");
  const pnlUsdt = data.pnl_usdt === null || data.pnl_usdt === undefined ? null : Number(data.pnl_usdt);
  const pnlPct = data.pnl_pct === null || data.pnl_pct === undefined ? null : Number(data.pnl_pct);

  if (pnlUsdt === null || Number.isNaN(pnlUsdt) || pnlPct === null || Number.isNaN(pnlPct)) {
    pnlEl.textContent = "-";
    removeValueToneClasses(pnlEl);
  } else {
    pnlEl.textContent = `${fmtSigned(pnlUsdt, 2)} USDT / ${fmtSigned(pnlPct, 2)}%`;
    applyValueColor(pnlEl, pnlUsdt);
  }

  requireElement("position-leverage").textContent = data.leverage !== undefined ? `${fmtMaybe(data.leverage, 1)}x` : "-";
  requireElement("position-qty").textContent = data.quantity !== undefined ? fmtMaybe(data.quantity, 6) : "-";
  requireElement("position-liquidation").textContent =
    data.liquidation_price !== null && data.liquidation_price !== undefined
      ? fmtMaybe(data.liquidation_price, 4)
      : "-";
}

function renderDecision(data) {
  clearWidgetError("decisionError");
  clearWidgetNote("decisionNote");

  state.decision = data;
  const decisionStatus = String(data.status || "").toUpperCase();

  if (decisionStatus === "INIT" || data.has_decision === false) {
    applyStatusChip("decision-action-chip", "INIT");
    requireElement("decision-summary").textContent = "아직 DECISION 이벤트가 없습니다.";
    requireElement("decision-entry-score").textContent = "-";
    requireElement("decision-exit-score").textContent = "-";
    requireElement("decision-trend-strength").textContent = "-";
    requireElement("decision-spread").textContent = "-";
    requireElement("decision-orderbook").textContent = "호가창: -";

    const reasonsBox = requireElement("decision-reasons-box");
    reasonsBox.innerHTML = "";
    const span = document.createElement("span");
    span.className = "reason-pill";
    span.textContent = "초기 상태";
    reasonsBox.appendChild(span);

    setWidgetNote("decisionNote", "의사결정 패널은 초기 상태입니다. 아직 DECISION 이벤트가 기록되지 않았습니다.");
    return;
  }

  const action = data.action || "대기";
  applyStatusChip("decision-action-chip", action);

  requireElement("decision-summary").textContent = data.summary || "-";
  requireElement("decision-entry-score").textContent = data.entry_score !== undefined ? fmtMaybe(data.entry_score, 3) : "-";

  const exitScoreText =
    data.exit_score !== undefined && data.exit_score !== null
      ? `${fmtMaybe(data.exit_score, 3)} / ${data.threshold !== undefined && data.threshold !== null ? fmtMaybe(data.threshold, 3) : "-"}`
      : data.threshold !== undefined && data.threshold !== null
        ? `- / ${fmtMaybe(data.threshold, 3)}`
        : "-";

  requireElement("decision-exit-score").textContent = exitScoreText;
  requireElement("decision-trend-strength").textContent = data.trend_strength !== undefined ? fmtMaybe(data.trend_strength, 3) : "-";
  requireElement("decision-spread").textContent = data.spread !== undefined ? fmtMaybe(data.spread, 6) : "-";
  requireElement("decision-orderbook").textContent =
    `호가창: ${data.orderbook_imbalance !== undefined ? fmtMaybe(data.orderbook_imbalance, 3) : "-"}`;

  const reasonsBox = requireElement("decision-reasons-box");
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
  requireElement("error-count-error").textContent = fmtMaybe(items.ERROR ?? 0, 0);
  requireElement("error-count-watchdog").textContent = fmtMaybe(items.WATCHDOG ?? 0, 0);

  const total = Number(items.ERROR ?? 0) + Number(items.WATCHDOG ?? 0);
  if (total > 0) applyStatusChip("error-panel-chip", "이상 감시 중");
  else applyStatusChip("error-panel-chip", "정상 감시 중");
}

function renderErrorsTable(items) {
  clearWidgetNote("errorMonitorNote");

  state.recentErrors = Array.isArray(items) ? items.slice(0, 100) : [];
  const body = requireElement("error-table-body");
  body.innerHTML = "";

  if (!state.recentErrors.length) {
    body.innerHTML = `<tr><td colspan="4" class="text-slate-500">표시할 문제가 없습니다.</td></tr>`;
    setWidgetNote("errorMonitorNote", "최근 오류/감시 경고가 없습니다.");
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
  clearWidgetNote("tradesNote");

  state.recentTrades = Array.isArray(items) ? items.slice(0, 100) : [];
  const body = requireElement("trades-table-body");
  body.innerHTML = "";

  if (!state.recentTrades.length) {
    body.innerHTML = `<tr><td colspan="6" class="text-slate-500">표시할 거래가 없습니다.</td></tr>`;
    setWidgetNote("tradesNote", "아직 종료 거래가 없습니다.");
    return;
  }

  state.recentTrades.forEach((item) => {
    const pnl = Number(item.pnl_usdt);
    const timeValue = item.exit_ts || item.entry_ts || "-";
    const timeKst = timeValue === "-" ? "-" : formatTimestampKst(timeValue, "full");

    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td class="mono text-xs">${timeKst}</td>
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
  clearWidgetNote("skipReasonsNote");

  const box = requireElement("skipReasonsBox");
  box.innerHTML = "";

  if (!Array.isArray(items) || !items.length) {
    box.innerHTML = `<div class="text-slate-500 text-sm">표시할 데이터가 없습니다.</div>`;
    setWidgetNote("skipReasonsNote", "최근 7일 기준 SKIP 이벤트가 없습니다.");
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
  const box = requireElement(targetId);
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
  const box = requireElement(targetId);
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

  requireElement(`${prefix}-analysis-answer`).textContent = answer;
  requireElement(`${prefix}-analysis-scope`).textContent = normalizeAiScope(scope);
  requireElement(`${prefix}-analysis-confidence`).textContent =
    typeof confidence === "number" ? `${fmtNumber(confidence * 100, 1)}%` : "-";

  renderAiList(`${prefix}-analysis-causes`, data.root_causes);
  renderAiList(`${prefix}-analysis-recommendations`, data.recommendations);
  renderAiTags(`${prefix}-analysis-used-inputs`, data.used_inputs);

  hideEl(`${prefix}-analysis-loading`);
  hideEl(`${prefix}-analysis-error`);
  showEl(`${prefix}-analysis-result`);
}

async function requestQuantAnalysis() {
  const questionEl = requireElement("quant-question");
  const includeExternalEl = requireElement("quant-include-external");

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
  const questionEl = requireElement("market-question");
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
    requireElement("summary-total-trades").textContent = "오류";
    requireElement("summary-win-rate").textContent = "오류";
    requireElement("summary-win-loss").textContent = "오류";
    requireElement("summary-total-pnl").textContent = "오류";
    requireElement("summary-avg-pnl").textContent = "오류";
    requireElement("summary-profit-factor").textContent = "오류";
    requireElement("summary-max-dd").textContent = "오류";
  }
}

async function loadDailyPnlChart() {
  clearWidgetError("dailyPnlError");
  clearWidgetNote("dailyPnlNote");

  try {
    const data = await fetchJsonStrict(API.dailyPnl, "일별 손익");
    const items = requireArrayStrict(data.items, "일별 손익.items");

    if (!items.length) {
      destroyChart("dailyPnlChart");
      setWidgetNote("dailyPnlNote", "아직 최근 30일 기준 종료 거래가 없어 일별 손익 차트를 표시할 수 없습니다.");
      return;
    }

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
  clearWidgetNote("equityCurveNote");

  try {
    const data = await fetchJsonStrict(API.equityCurve, "누적 손익 흐름");
    const items = requireArrayStrict(data.items, "누적 손익 흐름.items");

    if (!items.length) {
      destroyChart("equityCurveChart");
      setWidgetNote("equityCurveNote", "아직 종료 거래가 없어 누적 손익 흐름을 표시할 수 없습니다.");
      return;
    }

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
  clearWidgetNote("drawdownNote");

  try {
    const data = await fetchJsonStrict(API.drawdownCurve, "하락폭 흐름");
    const items = requireArrayStrict(data.items, "하락폭 흐름.items");

    if (!items.length) {
      destroyChart("drawdownChart");
      setWidgetNote("drawdownNote", "아직 종료 거래가 없어 하락폭 흐름을 표시할 수 없습니다.");
      return;
    }

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
  clearWidgetNote("entryScoreNote");

  try {
    const data = await fetchJsonStrict(API.entryScores, "진입 점수 분포");
    const labels = requireArrayStrict(data.hist_labels, "진입 점수 분포.hist_labels");
    const counts = requireArrayStrict(data.hist_counts, "진입 점수 분포.hist_counts");
    if (labels.length !== counts.length) {
      throw new Error("진입 점수 분포: histogram length mismatch");
    }

    if (!labels.length) {
      destroyChart("entryScoreHistChart");
      setWidgetNote("entryScoreNote", "아직 진입 점수 데이터가 없어 분포를 표시할 수 없습니다.");
      return;
    }

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
  clearWidgetNote("skipReasonsNote");

  try {
    const data = await fetchJsonStrict(API.skipReasons, "진입 안 한 이유");
    const items = requireArrayStrict(data.items, "진입 안 한 이유.items");
    renderSkipReasons(items);
  } catch (e) {
    setWidgetError("skipReasonsError", `ERROR: ${e.message || String(e)}`);
    showBannerError(e.message || String(e));
  }
}

async function loadSkipHourly() {
  clearWidgetError("skipHourlyError");
  clearWidgetNote("skipHourlyNote");

  try {
    const data = await fetchJsonStrict(API.skipHourly, "시간대별 진입 안 함");
    const items = requireArrayStrict(data.items, "시간대별 진입 안 함.items");

    if (!items.length) {
      destroyChart("skipHourlyChart");
      setWidgetNote("skipHourlyNote", "최근 7일 기준 시간대별 SKIP 데이터가 없습니다.");
      return;
    }

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
  clearWidgetNote("engineNote");
  clearWidgetError("accountError");
  clearWidgetNote("accountNote");

  let data;
  try {
    data = await fetchJsonStrict(API.engineStatus, "핵심 상태");
  } catch (e) {
    resetAccountCard("핵심 상태 조회 실패로 계좌 보유를 표시할 수 없습니다.");
    applyStatusChip("account-status-chip", "오류");
    setWidgetError("engineError", `ERROR: ${e.message || String(e)}`);
    showBannerError(e.message || String(e));
    return;
  }

  try {
    renderEngineStatus(data);
  } catch (e) {
    setWidgetError("engineError", `ERROR: ${e.message || String(e)}`);
    showBannerError(e.message || String(e));
  }
}

async function loadPosition() {
  clearWidgetError("positionError");
  clearWidgetNote("positionNote");
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
  clearWidgetNote("decisionNote");
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
  clearWidgetNote("errorMonitorNote");
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
  clearWidgetNote("tradesNote");
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
      // close 실패는 치명적이지 않음
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
      try {
        renderEngineStatus(payload);
      } catch (e) {
        setWidgetError("engineError", `ERROR: ${e.message || String(e)}`);
        showBannerError(e.message || String(e));
      }
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
        symbol: payload.symbol || "-",
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
      const target = targetId ? requireElement(targetId) : null;
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

  resetAccountCard("계좌 보유 데이터가 아직 연결되지 않았습니다.");

  const refreshBtn = requireElement("refresh-btn");
  const errorBannerClose = requireElement("error-banner-close");
  const quantSearchBtn = requireElement("quant-search-btn");
  const marketSearchBtn = requireElement("market-search-btn");
  const quantQuestion = requireElement("quant-question");
  const marketQuestion = requireElement("market-question");

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