# Auto-Trader (Binance USDT-M Futures)
STRICT · NO-FALLBACK · TRADE-GRADE

이 저장소는 BTCUSDT 단일 심볼 기준의 실전형(운영형) 자동매매 엔진이다.
목표는 “수익률 과장”이 아니라 운영 안정성(무중단/무결성/Fail-Fast)이다.

- WebSocket 기반 시세 수집(캔들/오더북)
- 전략 판단(지표/피처 + Quant Engine + GPT Supervisor + Meta L3(multiplier only))
- 실행(주문/TP·SL) + DB 영속화(거래/스냅샷/이벤트)
- 대시보드/분석은 Render에서 DB 조회 전용(read-only analytics)

---------------------------------------------------------------------------
0) TL;DR
---------------------------------------------------------------------------

- 주문 실행(거래소 키)은 AWS Worker에만 존재한다.
- Render는 DB + Dashboard(조회 전용)만 담당한다. 주문 금지.
- 런타임 의사결정에서 REST 폴백 금지. WS 데이터만 사용한다.
- 데이터가 부족/불일치/모호하면:
  - 추정/보정/대체값으로 계속 진행하지 않는다.
  - 즉시 예외(raise) 또는 명시적 SKIP로 종료한다.

- 잔고가 없으면 실주문은 실패한다.
- 잔고 없는 환경에서 파이프라인 검증은 TEST_DRY_RUN=1로 진행한다.

- 운영 실행 엔트리 포인트는 core/run_bot_preflight.py 이다.
  - Preflight ALL GREEN 통과 후에만 core/run_bot_ws.py 실행을 허용한다(우회 금지).
  - run_bot_ws 직접 실행 금지(운영 사고 방지).

---------------------------------------------------------------------------
1) 설계 원칙(절대 준수)
---------------------------------------------------------------------------

1.1 STRICT · NO-FALLBACK
- 데이터 누락/불일치/모호성 발생 시 추정/보정/대체값 금지
- 즉시 예외(raise) 또는 명시적 SKIP 처리
- “조용히 넘어가기” 금지
- 예외를 삼키지 않는다(로그 남기고 재-raise는 허용)
- 시간 역전(timestamp rollback) 발생 시 즉시 SAFE_STOP + 예외 전파

1.2 보안(Security Boundary)
- 거래소 API Key/Secret은 AWS Worker에만 존재
- Render(대시보드/DB)에 거래소 실행 권한/키 저장 금지
- DB 연결은 TLS(SSL) 전제(sslmode=require 권장)
- 민감정보(키/토큰/시크릿)는 로그/출력 금지

1.3 운영 안정성(기관급 근접 최소 요건)
- 메인 루프에서 외부 I/O(Telegram/DB)로 블로킹 금지 → 비동기 위임
- 내부 상태 ↔ 거래소 상태 Reconcile(불일치 감지) 도입
- 지연(latency) 예산 초과 시 신규 진입 차단(SAFE_STOP) 가능
- 재시작 후 정합(sync_exchange) 및 불일치 감지(reconcile_engine) 필수
- 원시 데이터 무결성(DataIntegrityGuard) + 수학 무결성(InvariantGuard) + 급변 감지(DriftDetector)를 런타임에 강제

---------------------------------------------------------------------------
2) 배포 토폴로지(Production)
---------------------------------------------------------------------------

2.1 Execution (Auto-Trader Worker): AWS
- 24/7 트레이딩 워커 실행
- WebSocket ingest → strategy → execution
- 거래소 키 보관(절대 Render 저장 금지)
- Render Postgres External URL로 TLS 연결하여 DB 기록
- OpenAI 관련 키/모델(OPENAI_API_KEY/OPENAI_MODEL)은 AWS Worker에만 존재(권장)

2.2 Database + Dashboard: Render
- Render Managed Postgres에 trades/snapshots/events/market-data 저장
- Render에 Dashboard/API(조회 전용) 배포
- Render에서 주문 실행 로직/키/권한 존재 금지(READ ONLY)

---------------------------------------------------------------------------
3) 런타임 데이터 플로우
---------------------------------------------------------------------------

3.0 Preflight(운영 Gate, 필수)
- 운영 시작 전 반드시 core/run_bot_preflight.py 실행
- Preflight는 런타임 파이프라인을 1회 “완전 재현(B)”한다:
  1) Settings 정상
  2) DB 연결 정상
  3) Binance API 정상
  4) REST 백필 + WS 준비(필수 TF/오더북)
  5) UnifiedFeatures 계산(WS 기반 통합 피처 생성)
  6) Regime 계산(밴드/할당)
  7) RiskPhysics 계산(DD/연속손실/AutoBlock 포함 최종 allocation)
  8) Drift Detector(급변 감지)
  9) Invariant Guard(수학 무결성)
  10) ExecutionEngine DRY_RUN(주문 없이 DB 기록)
  11) DB Round-trip 정합 검증
- ALL GREEN 통과 시에만 run_bot_ws 실행을 허용한다.

부팅(BOOT)
- REST 백필(boot only):
  - 시장 데이터 초기 버퍼(candles/orderbook/필수 TF) 채우기
- WS 시작:
  - 캔들/오더북 버퍼 유지
- 워밍업:
  - 필수 TF 데이터 준비 확인
  - data_health_monitor 시작(신선도/지연 감시)
- 메인 루프 진입

런타임(RUNTIME)
- 의사결정 입력은 WS 기반 최신 데이터만 사용
- 런타임에서 REST 폴백으로 의사결정 금지(STRICT)
- 전략 판단 결과:
  - ENTER / SKIP
- 실행 엔진:
  - guards → 주문 실행 → DB 기록(trades/snapshots/events) → 알림
- 리콘실:
  - 내부 상태 ↔ 거래소 상태 비교(Desync 감지)
  - 불일치 발생 시 SAFE_STOP 또는 옵션 강제청산

추가(TRADE-GRADE Safety Layers)
- DataIntegrityGuard:
  - 캔들: timestamp rollback 금지, 미래 timestamp 금지, high>=low, close∈[low,high], volume>=0, finite 강제
  - 오더북: bestAsk>bestBid, bids/asks 레벨 price/qty>0, ts(exchTs|ts) 존재(폴백 금지)
  - 위반 시 SAFE_STOP + 예외 전파
- InvariantGuard:
  - allocation/risk_pct 0..1, tp/sl>0, dd_pct 0..100, micro_score_risk 0..100,
    notional>0, qty>0, slippage finite 등
  - 위반 시 SAFE_STOP + 예외 전파
- DriftDetector:
  - allocation/multiplier/regime/micro_score_risk 급변 감지
  - 위반 시 SAFE_STOP + 예외 전파

---------------------------------------------------------------------------
4) 구성 요소(책임 경계)
---------------------------------------------------------------------------

4.1 AWS Worker(핵심 엔진)
- core/run_bot_preflight.py
  - 운영형 사전검증 Gate(완전 재현) → 통과 시 run_bot_ws 실행 허용
- core/run_bot_ws.py
  - Binance USDT-M Futures WebSocket 메인 루프
- infra/market_data_ws.py
  - Binance Futures WS ingest(캔들/오더북 버퍼)
- infra/market_data_rest.py
  - 부팅 시 REST 백필 전용(런타임 폴백 금지)
- infra/data_health_monitor.py
  - WS 지연/신선도 감시(헬스 실패 시 SKIP)
- infra/async_worker.py
  - Telegram/비핵심 I/O 비동기 위임(블로킹 금지)

- infra/data_integrity_guard.py   (신규)
  - 원시 데이터 무결성(캔들/오더북) STRICT 검증 + timestamp rollback 차단
- execution/invariant_guard.py     (신규)
  - 수학 무결성(핵심 값) STRICT 검증
- infra/drift_detector.py          (신규)
  - 급변(Drift) 감지(alloc/multiplier/regime/micro) → SAFE_STOP 트리거

- strategy/unified_features_builder.py
  - WS 기반 통합 피처 생성(timeframes + orderbook + pattern + engine_scores + microstructure)
  - (중요) 4h.regime 의존 제거:
    - 4h OHLCV/EMA/구조는 필수
    - 4h.regime 객체가 없으면 trend_strength(0..1)를 “명시 규칙”으로 산출(폴백 아님)
- strategy/microstructure_engine.py
  - 기초 데이터 확장 계층(funding/OI/LSR 기반 과열/왜곡 점수)
- strategy/regime_engine.py
  - 레짐 점수 히스토리 + 절대 기준 allocation 산출(0.0/0.4/0.7/1.0)
- strategy/ev_heatmap_engine.py
  - regime × distortion × score 버킷별 EV(rolling mean of pnl_r) 추정/차단 후보
- risk/auto_block_engine.py
  - micro_score_risk + EV 셀 상태 기반 자동 차단/감쇠 결정
- execution/risk_physics_engine.py
  - Regime allocation + DD/연속손실 + RR + AutoBlock을 합쳐 최종 allocation 결정
  - action_override: ENTER / SKIP / STOP
- execution/execution_engine.py
  - 실행만 담당(가드→주문→DB→알림)
  - Execution Quality 이벤트 기록은 비동기 위임(메인 블로킹 금지)
- execution/order_executor.py
  - 실제 주문 실행 레이어(시장가/TP/SL, 결정적 clientOrderId, 멱등성)
- execution/execution_quality_engine.py
  - 예상체결가 vs 실체결가, t+1/3/5s 가격 이동, execution_score 산출
- state/sync_exchange.py
  - 거래소 truth를 읽어 내부 상태 정합(추정 금지)
- sync/reconcile_engine.py
  - 내부 상태 ↔ 거래소 상태 불일치 감지(Desync)
- strategy/supervision_report_engine.py
  - DD/EV/Execution/GPT severity/AutoBlock 기반 운영 안정성 점수/리포트 생성(순수 연산)

4.2 Render Postgres(실전 분석형 저장소)
- trades / snapshots / events + market-data 저장
- 대시보드/분석의 단일 소스(SSOT)

4.3 Render Dashboard/API(조회 전용)
- Postgres SELECT 기반 성과/리스크/드로다운/승률/로그 시각화
- 주문 실행 로직 금지, 거래소 키/권한 금지

---------------------------------------------------------------------------
5) 환경변수(중요)
---------------------------------------------------------------------------

5.1 AWS Worker 필수
- BINANCE_API_KEY
- BINANCE_API_SECRET
- TRADER_DB_URL
  - Render Postgres External URL
  - SSL 권장: ?sslmode=require
- OPENAI_API_KEY
- OPENAI_MODEL
  - 권장: gpt-4o-mini (비용/속도 균형)
  - 예: OPENAI_MODEL=gpt-4o-mini

선택
- TELEGRAM_BOT_TOKEN
- TELEGRAM_CHAT_ID

5.2 Render Dashboard/API 필수
- DATABASE_URL (Render 내부 URL)

선택
- DASHBOARD_SECRET_KEY 등

5.3 운영형 설정(선택)
- ASYNC_WORKER_THREADS (기본 1)
- ASYNC_WORKER_QUEUE_SIZE (기본 2000)
- RECONCILE_INTERVAL_SEC (기본 30)
- FORCE_CLOSE_ON_DESYNC (기본 False)
- MAX_SIGNAL_LATENCY_MS (기본 200)
- MAX_EXEC_LATENCY_MS (기본 400)

5.4 테스트/가상매매
- TEST_DRY_RUN=1
  - 주문 없이 DB 기록만 수행(구현에 따라 다름)
- TEST_BYPASS_GUARDS=1
  - 운영에서 절대 사용 금지

주의
- 잔고가 없으면 실주문은 실패한다.
- 잔고 없는 환경에서 파이프라인 검증은 TEST_DRY_RUN=1로 진행한다.

---------------------------------------------------------------------------
6) DB 스키마(Render Postgres / public)
---------------------------------------------------------------------------

6.1 테이블 목록
- bt_trades
- bt_trade_snapshots
- bt_trade_exit_snapshots
- bt_events
- bt_external_events
- bt_entry_scores
- bt_regime_scores
- bt_candles
- bt_indicators
- bt_orderbook_snapshots
- bt_funding_rates

6.2 VERIFIED(2026-03-03): bt_trade_snapshots (확장 포함)
목적
- “왜 진입/스킵했는가” 재현을 위한 진입 시점 상태 저장
- DD(equity curve / drawdown) 분석의 핵심
- (TRADE-GRADE) Decision/Micro/Exec/EV/AutoBlock까지 재현 가능한 형태로 확장

기본 컬럼(핵심)
- id (int, PK)
- trade_id (int, FK->bt_trades.id, UNIQUE, ON DELETE CASCADE)
- symbol (varchar(32), not null)
- entry_ts (timestamptz, not null)
- direction (varchar(10), not null)
- signal_source (varchar(32), null)
- regime (varchar(32), null)
- entry_score (double precision, null)
- engine_total (double precision, null)
- trend_strength / atr_pct / volume_zscore / depth_ratio / spread_pct (double precision, null)
- hour_kst / weekday_kst (int, null)
- last_price (numeric(24,8), null)
- risk_pct / tp_pct / sl_pct (double precision, null)
- gpt_action (varchar(16), null)
- gpt_reason (text, null)
- created_at (timestamptz, not null, default CURRENT_TIMESTAMP)

DD/Equity(중요)
- equity_current_usdt (double precision, null)
- equity_peak_usdt (double precision, null)
- dd_pct (double precision, null)

(TRADE-GRADE) Decision Reconciliation
- decision_id (varchar(64), UNIQUE, null)
- quant_decision_pre (jsonb, null)
- quant_constraints (jsonb, null)
- quant_final_decision (varchar(16), null)
- gpt_severity (int, null)
- gpt_tags (jsonb, null)
- gpt_confidence_penalty (double precision, null)
- gpt_suggested_risk_multiplier (double precision, null)
- gpt_rationale_short (text, null)

(TRADE-GRADE) Microstructure
- micro_funding_rate (double precision, null)
- micro_funding_z (double precision, null)
- micro_open_interest (double precision, null)
- micro_oi_z (double precision, null)
- micro_long_short_ratio (double precision, null)
- micro_lsr_z (double precision, null)
- micro_distortion_index (double precision, null)
- micro_score_risk (double precision, null)

(TRADE-GRADE) Execution Quality
- exec_expected_price (double precision, null)
- exec_filled_avg_price (double precision, null)
- exec_slippage_pct (double precision, null)
- exec_adverse_move_pct (double precision, null)
- exec_score (double precision, null)
- exec_post_prices (jsonb, null)

(TRADE-GRADE) EV Heatmap / AutoBlock
- ev_cell_key (varchar(96), null)
- ev_cell_ev (double precision, null)
- ev_cell_n (int, null)
- ev_cell_status (varchar(16), null)
- auto_blocked (bool, null)
- auto_risk_multiplier (double precision, null)
- auto_block_reasons (jsonb, null)

인덱스/제약(핵심)
- PK: bt_trade_snapshots_pkey(id)
- UNIQUE: ux_bt_trade_snapshots_tradeid(trade_id)
- UNIQUE: ux_bt_trade_snapshots_decision_id(decision_id)
- INDEX: ix_bt_trade_snapshots_symbol_entryts(symbol, entry_ts)
- INDEX: ix_bt_trade_snapshots_entry_ts(entry_ts)
- INDEX: ix_bt_trade_snapshots_regime(regime)
- FK: bt_trade_snapshots_trade_id_fkey(trade_id) ON DELETE CASCADE

운영 체크 포인트(중요)
- bt_trade_snapshots의 equity_current_usdt / equity_peak_usdt / dd_pct 가 NULL이면
  - DD 계산 불가
  - equity curve 복원 불가
  - 리스크/HARD_STOP 근거 상실
  - 분석형 DB가 반쪽이 된다(허용 불가)

6.3 VERIFIED(2026-03-03): bt_trades
목적
- 실제 진입/청산 거래 결과 저장(1 row = 1 trade)

컬럼(핵심)
- id (int, PK)
- symbol (varchar(32), not null)
- side (varchar(8), not null)
- entry_ts (timestamptz, not null)
- exit_ts (timestamptz, null)
- entry_price (numeric(24,8), not null)
- exit_price (numeric(24,8), null)
- qty (numeric(24,8), not null)
- pnl_usdt (numeric(24,8), null)
- pnl_pct_futures / pnl_pct_spot_ref (double precision, null)
- is_auto (bool, not null)
- regime_at_entry / regime_at_exit (varchar(16), null)
- entry_score / trend_score_at_entry / range_score_at_entry (double precision, null)
- strategy (varchar(16), null)
- close_reason (varchar(32), null)
- leverage / risk_pct / tp_pct / sl_pct (double precision, null)
- note (varchar(255), null)
- created_at / updated_at (timestamptz, not null)

실행/복구(운영형, 중요)
- entry_order_id / tp_order_id / sl_order_id (varchar(64), null)
- exchange_position_side (varchar(16), null)
- remaining_qty (numeric(24,8), null)
- realized_pnl_usdt (numeric(24,8), null)
- reconciliation_status (varchar(32), null)
- last_synced_at (timestamptz, null)

인덱스/참조(핵심)
- PK: bt_trades_pkey(id)
- INDEX: ix_bt_trades_symbol_entry_ts(symbol, entry_ts)

---------------------------------------------------------------------------
7) DB 점검/덤프 명령어(psql)
---------------------------------------------------------------------------

접속
- psql "$TRADER_DB_URL"

테이블 구조 확인
- \d+ bt_trade_snapshots
- \d+ bt_trades

확장 컬럼 존재 여부 체크(bt_trade_snapshots)
- SELECT column_name
  FROM information_schema.columns
  WHERE table_schema='public'
    AND table_name='bt_trade_snapshots'
    AND column_name IN (
      'decision_id','quant_decision_pre','quant_constraints','quant_final_decision',
      'micro_score_risk','exec_slippage_pct','ev_cell_status','auto_blocked'
    )
  ORDER BY column_name;

최근 스냅샷 확인
- SELECT id, trade_id, symbol, entry_ts,
         equity_current_usdt, equity_peak_usdt, dd_pct,
         micro_score_risk, exec_slippage_pct, ev_cell_status, auto_blocked
  FROM bt_trade_snapshots
  ORDER BY id DESC
  LIMIT 5;

---------------------------------------------------------------------------
8) 대시보드/분석 기본 쿼리(예시)
---------------------------------------------------------------------------

8.1 최근 거래 리스트
- SELECT id, symbol, side, entry_ts, exit_ts,
         entry_price, exit_price, qty,
         pnl_usdt, pnl_pct_futures,
         strategy, close_reason
  FROM bt_trades
  ORDER BY id DESC
  LIMIT 50;

8.2 승률(닫힌 거래만)
- SELECT
    COUNT(*) AS n_closed,
    SUM(CASE WHEN pnl_usdt > 0 THEN 1 ELSE 0 END) AS n_win,
    ROUND(100.0 * SUM(CASE WHEN pnl_usdt > 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2)
      AS win_rate_pct
  FROM bt_trades
  WHERE exit_ts IS NOT NULL;

8.3 평균 보유 시간(닫힌 거래만)
- SELECT
    AVG(EXTRACT(EPOCH FROM (exit_ts - entry_ts))) AS avg_hold_sec
  FROM bt_trades
  WHERE exit_ts IS NOT NULL;

8.4 최신 스냅샷(진입 근거 + 확장)
- SELECT
    s.trade_id, s.symbol, s.entry_ts, s.direction,
    s.entry_score, s.engine_total, s.regime,
    s.trend_strength, s.atr_pct, s.volume_zscore,
    s.depth_ratio, s.spread_pct,
    s.equity_current_usdt, s.equity_peak_usdt, s.dd_pct,
    s.micro_score_risk, s.exec_slippage_pct,
    s.ev_cell_key, s.ev_cell_status, s.auto_blocked, s.auto_risk_multiplier
  FROM bt_trade_snapshots s
  ORDER BY s.id DESC
  LIMIT 50;

---------------------------------------------------------------------------
9) 운영 권장
---------------------------------------------------------------------------

- 24/7 유지
  - systemd 또는 pm2/supervisor로 상시 실행
- 재시작 시 필수 점검
  - BINANCE_API_* / TRADER_DB_URL / OPENAI_* 환경변수 재주입 확인
  - reconcile_engine 동작 확인(Desync 감지)
- 로그 저장(옵션)
  - LOG_TO_FILE=1
  - LOG_FILE=/var/log/auto-trader.log

추가(운영 실행 순서, 권장)
1) Preflight만 실행(점검)
- python -m core.run_bot_preflight --preflight-only
2) ALL GREEN 확인 후 운영 실행
- python -m core.run_bot_ws
(또는 python -m core.run_bot_preflight 로 통과 후 자동 핸드오프)

---------------------------------------------------------------------------
10) 테스트/가상매매(잔고 없을 때)
---------------------------------------------------------------------------

권장 절차
1) TEST_DRY_RUN=1로 실행
2) Preflight ALL GREEN 확인
3) 엔트리 후보가 발생하도록 충분히 구동
4) bt_trade_snapshots에 레코드가 생성되는지 확인
5) equity_current_usdt / equity_peak_usdt / dd_pct가 NULL이 아닌지 확인
6) (확장) micro_score_risk / exec_slippage_pct / ev_cell_status / auto_blocked 확인

주의
- TEST_BYPASS_GUARDS=1은 운영에서 절대 사용 금지

---------------------------------------------------------------------------
11) 알려진 문제/주의사항
---------------------------------------------------------------------------

11.1 잔고 없음(Insufficient balance)
- 현상: 주문 단계에서 거래소가 insufficient balance 반환 → 예외로 중단
- 해결:
  - 잔고 없는 환경: TEST_DRY_RUN=1로 파이프라인 검증
  - 실주문 테스트: 선물(USDT-M) 지갑에 USDT 확보 필요

11.2 equity/dd가 NULL로 저장되는 경우
- 원인 A: execution_engine이 스냅샷 기록 시 meta에 equity/dd를 주입하지 못함
- 원인 B: TradeSnapshot ORM에 equity/dd 컬럼이 누락됨
- 해결:
  - account_state 생성 후 meta에 반드시 주입
  - ORM-DB 정합 유지(패치 반영)

11.3 EV heatmap NOT_READY 지속
- 원인: pnl_r(=R 단위) 업데이트가 아직 exit 단계에 연결되지 않음
- 해결:
  - exit 시점에 entry_price/qty/SL 기준으로 pnl_r 확정 후 ev_heatmap_engine.on_trade_close() 업데이트
  - 추정/대충 변환 금지(STRICT)

11.4 OpenAI 설정 누락
- 현상: Preflight SETTINGS 단계에서 즉시 실패(openai_api_key/openai_model 누락)
- 해결:
  - OPENAI_API_KEY / OPENAI_MODEL 환경변수 설정 후 재시도
  - 모델 예: OPENAI_MODEL=gpt-4o-mini

11.5 4H 관련(중요)
- 4H OHLCV/EMA/구조 계산은 필수(데이터 없으면 즉시 실패).
- 4h.regime 객체는 upstream에 없을 수 있으며, 이 경우 trend_strength는 규칙 기반으로 산출(폴백 아님).

---------------------------------------------------------------------------
12) 면책
---------------------------------------------------------------------------

자동매매는 손실 위험이 크다.
사용자는 모든 책임을 본인이 부담한다.
운영 전 반드시 소액/테스트로 검증한다.