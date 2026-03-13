# Auto-Trader (Binance USDT-M Futures)
STRICT · NO-FALLBACK · TRADE-GRADE

이 저장소는 BTCUSDT 단일 심볼 기준의 실전형(운영형) 자동매매 엔진이다.  
목표는 “수익률 과장”이 아니라 **운영 안정성(무중단/무결성/Fail-Fast)** 이다.

- WebSocket 기반 시세 수집(캔들/오더북)
- 전략 판단(지표/피처 + Quant Engine + GPT Supervisor + Meta L3(multiplier only))
- 실행(주문/TP·SL) + DB 영속화(거래/스냅샷/이벤트)
- 대시보드/분석은 Render에서 DB 조회 전용(read-only analytics)
- 외부시장 인텔리전스(거시/뉴스/온체인/파생/옵션)는 **DB 스냅샷 캐시 기반**으로 운용한다.

---------------------------------------------------------------------------
0) TL;DR
---------------------------------------------------------------------------

- 주문 실행(거래소 키)은 **AWS Worker** 에만 존재한다.
- Render는 **DB + Dashboard(조회 전용)** 만 담당한다. 주문 금지.
- 런타임 의사결정에서 **REST 폴백 금지**. WS 데이터만 사용한다.
- 데이터가 부족/불일치/모호하면:
  - 추정/보정/대체값으로 계속 진행하지 않는다.
  - 즉시 예외(raise) 또는 명시적 SKIP로 종료한다.
- 잔고가 없으면 실주문은 실패한다.
- 잔고 없는 환경에서 파이프라인 검증은 `TEST_DRY_RUN=1` 로 진행한다.
- 운영 실행 게이트는 `core/run_bot_preflight.py` 이다.
  - `python -m core.run_bot_preflight --preflight-only`
  - 또는 `python -m core.run_bot_preflight`
- `python -m core.run_bot_ws` 직접 실행도 가능하지만, 현재 구조상 **내부적으로 preflight를 선행**한 뒤 handoff 한다.
- Preflight는 **검사만 수행**해야 하며, runtime ownership을 가지면 안 된다.
  - Async Worker / Watchdog / Runtime loop / Account WS는 **run_bot_ws / engine_bootstrap 쪽 단일 ownership** 을 유지해야 한다.
- 외부시장 인텔리전스는 **API → DB snapshot 저장 → 이후 재사용** 구조를 따른다.
  - 서버 재시작 시 메모리 캐시만 초기화되어도, DB 스냅샷으로 재사용 가능해야 한다.
  - stale snapshot 재사용은 금지한다.
- Meta L3는 cold-start(종료 거래 부족) 상태를 허용한다.
  - 거래 부족 시 엔진 시작을 막지 않고 `NO_CHANGE` recommendation 으로 관측만 수행한다.

---------------------------------------------------------------------------
1) 설계 원칙(절대 준수)
---------------------------------------------------------------------------

### 1.1 STRICT · NO-FALLBACK
- 데이터 누락/불일치/모호성 발생 시 추정/보정/대체값 금지
- 즉시 예외(raise) 또는 명시적 SKIP 처리
- “조용히 넘어가기” 금지
- 예외를 삼키지 않는다(로그 남기고 재-raise는 허용)
- 시간 역전(timestamp rollback) 발생 시 즉시 SAFE_STOP + 예외 전파

### 1.2 보안(Security Boundary)
- 거래소 API Key/Secret은 AWS Worker에만 존재
- Render(대시보드/DB)에 거래소 실행 권한/키 저장 금지
- DB 연결은 TLS(SSL) 전제(`sslmode=require` 권장)
- 민감정보(키/토큰/시크릿)는 로그/출력 금지

### 1.3 운영 안정성(기관급 근접 최소 요건)
- 메인 루프에서 외부 I/O(Telegram/DB)로 블로킹 금지 → 비동기 위임
- 내부 상태 ↔ 거래소 상태 Reconcile(불일치 감지) 도입
- 지연(latency) 예산 초과 시 신규 진입 차단(SAFE_STOP) 가능
- 재시작 후 정합(`sync_exchange`) 및 불일치 감지(`reconcile_engine`) 필수
- 원시 데이터 무결성(DataIntegrityGuard) + 수학 무결성(InvariantGuard) + 급변 감지(DriftDetector)를 런타임에 강제
- 외부시장 인텔리전스는 **fetcher 내부 캐시 책임 단일화**
  - fetcher 내부: 메모리 캐시 + DB 영속 캐시
  - orchestrator(`market_researcher`)는 별도 stale fallback을 들고 있지 않는다.

### 1.4 Runtime Ownership (중요)
- Preflight는 **runtime resource의 owner가 아니다**
- 아래 리소스는 runtime에서만 시작되어야 한다.
  - Async Worker
  - Engine Watchdog
  - Account WS
  - Engine Loop
- 동일 프로세스에서 중복 start를 허용하지 않는 리소스는
  - 단일 owner
  - idempotent start
  - attach-only 재진입
  구조를 유지한다.

---------------------------------------------------------------------------
2) 배포 토폴로지(Production)
---------------------------------------------------------------------------

### 2.1 Execution (Auto-Trader Worker): AWS
- 24/7 트레이딩 워커 실행
- WebSocket ingest → strategy → execution
- 거래소 키 보관(절대 Render 저장 금지)
- Render Postgres External URL로 TLS 연결하여 DB 기록
- OpenAI 관련 키/모델(`OPENAI_API_KEY` / `OPENAI_MODEL`)은 AWS Worker에만 존재(권장)

### 2.2 Database + Dashboard: Render
- Render Managed Postgres에 trades / snapshots / events / market-data 저장
- Render에 Dashboard/API(조회 전용) 배포
- Render에서 주문 실행 로직 / 키 / 권한 존재 금지
- Dashboard DB 접근도 `state/db_core.py` 단일 세션 경로만 사용한다.

### 2.3 Telegram / Alerts
- 텔레그램 알림은 AWS Worker에서 전송한다.
- Render는 알림 발송 owner가 아니다.
- Telegram 오류는 엔진 치명 오류로 승격하지 않는다.
- 단, 토큰/챗아이디 오류는 운영자가 즉시 수정해야 한다.

---------------------------------------------------------------------------
3) 런타임 데이터 플로우
---------------------------------------------------------------------------

### 3.0 Preflight(운영 Gate, 필수)
운영 시작 전 반드시 `core/run_bot_preflight.py` 실행

Preflight는 런타임 파이프라인을 1회 “완전 재현”한다.

1. Settings 정상
2. DB 연결 정상
3. Binance API 정상
4. REST 백필 + WS 준비(필수 TF/오더북)
5. UnifiedFeatures 계산(WS 기반 통합 피처 생성)
6. Regime 계산(밴드/할당)
7. RiskPhysics 계산(DD/연속손실/AutoBlock 포함 최종 allocation)
8. Drift Detector(급변 감지)
9. Invariant Guard(수학 무결성)
10. ExecutionEngine DRY_RUN(주문 없이 계약 검증)
11. Meta L3 observability 확인
12. Handoff 가능 여부 판정

ALL GREEN 통과 시에만 `run_bot_ws` 실행 허용.

중요:
- Preflight는 **검사만** 해야 한다.
- Async Worker / Watchdog / Runtime Loop 소유 금지
- Handoff 이후 runtime owner가 별도로 리소스를 시작한다.

### 3.1 부팅(BOOT)
- REST 백필(boot only)
  - 시장 데이터 초기 버퍼(candles/orderbook/필수 TF) 채우기
- WS 시작
  - 캔들/오더북 버퍼 유지
- 워밍업
  - 필수 TF 데이터 준비 확인
  - `data_health_monitor` 시작(신선도/지연 감시)
- Async Worker 시작
- Watchdog 시작
- Account WS 시작
- 메인 루프 진입

### 3.2 런타임(RUNTIME)
- 의사결정 입력은 **WS 기반 최신 데이터만 사용**
- 런타임에서 REST 폴백으로 의사결정 금지
- 전략 판단 결과:
  - ENTER / SKIP
- 실행 엔진:
  - guards → 주문 실행 → DB 기록(trades/snapshots/events) → 알림
- 리콘실:
  - 내부 상태 ↔ 거래소 상태 비교(Desync 감지)
  - 불일치 발생 시 SAFE_STOP 또는 옵션 강제청산

### 3.3 외부시장 인텔리전스 플로우
- 외부시장 source:
  - Macro
  - Crypto News
  - Onchain
  - Derivatives
  - Options
  - Sentiment
- source fetcher 책임:
  1. 메모리 캐시 조회
  2. DB snapshot cache 조회
  3. 없거나 만료 시 외부 API 호출
  4. 최신 snapshot DB 저장
- `market_researcher` 책임:
  - 각 fetcher의 `fetch()` 결과만 사용
  - 자체 stale external snapshot fallback 금지
  - 리포트 생성 및 `market_features` / `trade_context_snapshots` 적재

### 3.4 TRADE-GRADE Safety Layers
- DataIntegrityGuard
  - 캔들: timestamp rollback 금지, 미래 timestamp 금지, high>=low, close∈[low,high], volume>=0, finite 강제
  - 오더북: bestAsk>bestBid, bids/asks 레벨 price/qty>0, ts(exchTs|ts) 존재(폴백 금지)
  - 위반 시 SAFE_STOP + 예외 전파
- InvariantGuard
  - allocation/risk_pct 0..1, tp/sl>0, dd_pct 0..100, micro_score_risk 0..100,
    notional>0, qty>0, slippage finite 등
  - 위반 시 SAFE_STOP + 예외 전파
- DriftDetector
  - allocation/multiplier/regime/micro_score_risk 급변 감지
  - 위반 시 SAFE_STOP + 예외 전파
- EngineWatchdog
  - WS / Orderbook / DB / rollback 상태를 주기적으로 검사
  - FAIL/WARNING/OK 구분
  - watchdog snapshot stale/fail/internal fatal 시 SAFE_STOP 승격
  - 단일 프로세스/단일 symbol 기준 **한 번만 시작**

---------------------------------------------------------------------------
4) 구성 요소(책임 경계)
---------------------------------------------------------------------------

### 4.1 AWS Worker(핵심 엔진)
- `core/run_bot_preflight.py`
  - 운영형 사전검증 Gate(완전 재현) → 통과 시 `run_bot_ws` 실행 허용
- `core/run_bot_ws.py`
  - Binance USDT-M Futures WebSocket 메인 루프
- `engine/engine_bootstrap.py`
  - runtime resource bootstrap owner
  - Async Worker / Watchdog / Account WS / Market WS / Runtime state 초기화
- `engine/engine_loop.py`
  - 단일 엔진 루프 orchestration
- `engine/cycles/monitoring_cycle.py`
  - monitoring layer orchestration
  - watchdog snapshot / ws liveness / account ws / protection / reconcile
- `engine/cycles/entry_cycle.py`
  - 진입 후보 평가 orchestration
- `engine/cycles/risk_cycle.py`
  - regime / risk physics / meta overlay orchestration
- `engine/cycles/execution_cycle.py`
  - risk 승인 packet 실행 orchestration
- `engine/cycles/exit_cycle.py`
  - open position 상태의 청산 orchestration

- `infra/market_data_ws.py`
  - Binance Futures WS ingest(캔들/오더북 버퍼)
- `infra/market_data_rest.py`
  - 부팅 시 REST 백필 전용(런타임 폴백 금지)
- `infra/data_health_monitor.py`
  - WS 지연/신선도 감시(헬스 실패 시 SKIP)
- `infra/account_ws.py`
  - account / protection order / listenKey stream
- `infra/async_worker.py`
  - Telegram/비핵심 I/O 비동기 위임
  - singleton queue + idempotent lifecycle 필요
- `infra/engine_watchdog.py`
  - runtime watchdog
  - 단일 ownership / 중복 시작 금지

- `infra/data_integrity_guard.py`
  - 원시 데이터 무결성 STRICT 검증 + timestamp rollback 차단
- `execution/invariant_guard.py`
  - 수학 무결성 STRICT 검증
- `infra/drift_detector.py`
  - 급변(Drift) 감지 → SAFE_STOP 트리거

- `strategy/unified_features_builder.py`
  - WS 기반 통합 피처 생성(timeframes + orderbook + pattern + engine_scores + microstructure)
- `strategy/microstructure_engine.py`
  - 기초 데이터 확장 계층(funding/OI/LSR 기반 과열/왜곡 점수)
- `strategy/regime_engine.py`
  - 레짐 점수 히스토리 + 절대 기준 allocation 산출
- `strategy/ev_heatmap_engine.py`
  - regime × distortion × score 버킷별 EV 추정/차단 후보
- `risk/auto_block_engine.py`
  - micro_score_risk + EV 셀 상태 기반 자동 차단/감쇠 결정
- `execution/risk_physics_engine.py`
  - Regime allocation + DD/연속손실 + RR + AutoBlock을 합쳐 최종 allocation 결정
- `execution/execution_engine.py`
  - 실행만 담당(가드→주문→DB→알림)
- `execution/order_executor.py`
  - 실제 주문 실행 레이어(시장가/TP/SL, 결정적 clientOrderId, 멱등성)
  - LONG→BUY / SHORT→SELL 계약 고정
- `execution/execution_quality_engine.py`
  - 예상체결가 vs 실체결가, t+1/3/5s 가격 이동, execution_score 산출
- `state/sync_exchange.py`
  - 거래소 truth를 읽어 내부 상태 정합(추정 금지)
  - LONG/SHORT ↔ BUY/SELL 변환 계약 강제
- `sync/reconcile_engine.py`
  - 내부 상태 ↔ 거래소 상태 불일치 감지(Desync)
- `strategy/supervision_report_engine.py`
  - DD/EV/Execution/GPT severity/AutoBlock 기반 운영 안정성 점수/리포트 생성

### 4.2 외부시장 fetcher
- `analysis/macro_market_fetcher.py`
- `analysis/crypto_news_fetcher.py`
- `analysis/onchain_fetcher.py`
- `analysis/derivatives_market_fetcher.py`
- `analysis/options_market_fetcher.py`
- `analysis/sentiment_fetcher.py`

공통 책임:
- 외부 API 호출
- 응답 strict 파싱
- 메모리 캐시
- DB 영속 캐시
- stale snapshot 재사용 금지
- fetcher 단일 진실원 유지

### 4.3 분석 오케스트레이션
- `analysis/market_researcher.py`
  - Binance 내부 시장 + 외부 인텔리전스 + volume profile + orderflow + options 결합
  - `market_features` / `trade_context_snapshots` 적재
- `analysis/quant_analyst.py`
  - 내부 DB 분석 + 외부 시장 분석 + GPT Analyst 오케스트레이션
- `analysis/gpt_analyst_engine.py`
  - OpenAI Responses API 기반 분석 엔진

### 4.4 Render Postgres(실전 분석형 저장소)
- trades / snapshots / events + market-data 저장
- 대시보드/분석의 단일 소스(SSOT)

### 4.5 Render Dashboard/API(조회 전용)
- Postgres SELECT 기반 성과/리스크/드로다운/승률/로그 시각화
- 주문 실행 로직 금지, 거래소 키/권한 금지

---------------------------------------------------------------------------
5) 환경변수(중요)
---------------------------------------------------------------------------

### 5.1 AWS Worker 필수
- `BINANCE_API_KEY`
- `BINANCE_API_SECRET`
- `TRADER_DB_URL`
  - Render Postgres External URL
  - SSL 권장: `?sslmode=require`
- `OPENAI_API_KEY`
- `OPENAI_MODEL`
  - 권장: `gpt-4o-mini`

선택
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

### 5.2 Render Dashboard/API 필수
- `DATABASE_URL` 또는 내부 설정에 맞는 Render DB URL
- Dashboard DB 접근은 반드시 `state/db_core.py` 단일 경로 경유

### 5.3 운영형 설정(선택)
- `ASYNC_WORKER_THREADS`
- `ASYNC_WORKER_QUEUE_SIZE`
- `RECONCILE_INTERVAL_SEC`
- `FORCE_CLOSE_ON_DESYNC`
- `MAX_SIGNAL_LATENCY_MS`
- `MAX_EXEC_LATENCY_MS`
- `ENGINE_WATCHDOG_INTERVAL_SEC`
- `ENGINE_WATCHDOG_MAX_DB_PING_MS`
- `ENGINE_WATCHDOG_EMIT_MIN_SEC`
- `ANALYST_AUTO_REPORT_MARKET_INTERVAL_SEC`
- `ANALYST_AUTO_REPORT_SYSTEM_INTERVAL_SEC`

### 5.4 테스트/가상매매
- `TEST_DRY_RUN=1`
- `TEST_BYPASS_GUARDS=1` → 운영 절대 금지

### 5.5 외부시장 인텔리전스
- `ALPHAVANTAGE_API_KEY`
- `ANALYST_MARKET_SYMBOL`
- `ANALYST_HTTP_TIMEOUT_SEC`
- `ANALYST_KLINE_INTERVAL`
- `ANALYST_RATIO_PERIOD`
- `ANALYST_OPENAI_MODEL`
- `ANALYST_OPENAI_TIMEOUT_SEC`
- `ANALYST_OPENAI_MAX_OUTPUT_TOKENS`

주의
- Alpha Vantage 무료 키는 quota가 매우 작다.
- 키 유출 시 본인이 호출하지 않아도 quota가 소진될 수 있다.
- 운영에서는 DB snapshot cache 구조를 전제로 사용한다.

---------------------------------------------------------------------------
6) DB 스키마(Render Postgres / public)
---------------------------------------------------------------------------

### 6.1 핵심 운영 테이블
- `bt_trades`
- `bt_trade_snapshots`
- `bt_trade_exit_snapshots`
- `bt_events`
- `bt_events_archive`
- `bt_external_events`
- `bt_entry_scores`
- `bt_regime_scores`
- `bt_candles`
- `bt_indicators`
- `bt_orderbook_snapshots`
- `bt_funding_rates`
- `engine_runtime_state`
- `market_features`
- `trade_context_snapshots`

### 6.2 외부시장 snapshot 테이블
- `bt_macro_market_snapshots`
- `bt_crypto_news_snapshots`
- `bt_onchain_snapshots`
- `bt_derivatives_snapshots`
- `bt_options_snapshots`

용도:
- 외부시장 인텔리전스 DB 캐시
- 재시작 후 API 재호출 최소화
- 향후 백테스트/리플레이/분석용 스냅샷 보관

### 6.3 VERIFIED: market_features / trade_context_snapshots
목적:
- `MarketResearcher.sync_once()` 결과 저장
- 외부시장 + 내부시장 정량화 결과의 운영용 SSOT

운영 검증:
- `market_features.ts_ms`
- `trade_context_snapshots.ts_ms`

두 값은 동일 주기로 적재되어야 한다.

### 6.4 VERIFIED: bt_trade_snapshots
목적:
- “왜 진입/스킵했는가” 재현
- DD / equity curve / AutoBlock / EV Heatmap 분석 근거

핵심 컬럼:
- `trade_id`
- `symbol`
- `entry_ts`
- `direction`
- `entry_score`
- `engine_total`
- `equity_current_usdt`
- `equity_peak_usdt`
- `dd_pct`
- `micro_score_risk`
- `exec_slippage_pct`
- `ev_cell_status`
- `auto_blocked`

### 6.5 VERIFIED: bt_trades
목적:
- 실제 거래 결과 저장

핵심 컬럼:
- `symbol`
- `side`
- `entry_ts`
- `exit_ts`
- `entry_price`
- `exit_price`
- `qty`
- `pnl_usdt`
- `strategy`
- `close_reason`
- `entry_order_id`
- `tp_order_id`
- `sl_order_id`
- `exchange_position_side`
- `remaining_qty`
- `reconciliation_status`
- `last_synced_at`

---------------------------------------------------------------------------
7) DB 점검/덤프 명령어(psql)
---------------------------------------------------------------------------

접속
- `psql "$TRADER_DB_URL"`

테이블 구조 확인
- `\d+ bt_trade_snapshots`
- `\d+ bt_trades`
- `\d+ market_features`
- `\d+ trade_context_snapshots`

외부시장 snapshot 테이블 확인
- `\dt *snapshots*`

최신 market research 적재 확인

```sql
SELECT symbol,timeframe,ts_ms
FROM market_features
ORDER BY ts_ms DESC
LIMIT 5;
```

```sql
SELECT symbol,ts_ms
FROM trade_context_snapshots
ORDER BY ts_ms DESC
LIMIT 5;
```

검증 기준:
- 최신 `ts_ms` 5개가 두 테이블에서 동일해야 한다.

외부시장 테이블 존재 확인

```sql
SELECT tablename
FROM pg_tables
WHERE schemaname='public'
  AND tablename IN (
    'bt_macro_market_snapshots',
    'bt_crypto_news_snapshots',
    'bt_onchain_snapshots',
    'bt_derivatives_snapshots',
    'bt_options_snapshots'
  )
ORDER BY tablename;
```

---------------------------------------------------------------------------
8) 대시보드/분석 기본 쿼리(예시)
---------------------------------------------------------------------------

### 8.1 최근 거래 리스트

```sql
SELECT id, symbol, side, entry_ts, exit_ts,
       entry_price, exit_price, qty,
       pnl_usdt, pnl_pct_futures,
       strategy, close_reason
FROM bt_trades
ORDER BY id DESC
LIMIT 50;
```

### 8.2 승률(닫힌 거래만)

```sql
SELECT
  COUNT(*) AS n_closed,
  SUM(CASE WHEN pnl_usdt > 0 THEN 1 ELSE 0 END) AS n_win,
  ROUND(
    100.0 * SUM(CASE WHEN pnl_usdt > 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0),
    2
  ) AS win_rate_pct
FROM bt_trades
WHERE exit_ts IS NOT NULL;
```

### 8.3 평균 보유 시간

```sql
SELECT
  AVG(EXTRACT(EPOCH FROM (exit_ts - entry_ts))) AS avg_hold_sec
FROM bt_trades
WHERE exit_ts IS NOT NULL;
```

### 8.4 최신 market research 적재

```sql
SELECT ts_ms, symbol, timeframe, market_regime, trend_score, volatility_score, liquidity_score
FROM market_features
ORDER BY ts_ms DESC
LIMIT 20;
```

### 8.5 외부시장 snapshot 적재 여부

```sql
SELECT COUNT(*) FROM bt_crypto_news_snapshots;
SELECT COUNT(*) FROM bt_macro_market_snapshots;
SELECT COUNT(*) FROM bt_onchain_snapshots;
SELECT COUNT(*) FROM bt_derivatives_snapshots;
SELECT COUNT(*) FROM bt_options_snapshots;
```

---------------------------------------------------------------------------
9) 운영 권장
---------------------------------------------------------------------------

- 24/7 유지
  - systemd 또는 pm2/supervisor로 상시 실행
- 재시작 시 필수 점검
  - `BINANCE_API_*` / `TRADER_DB_URL` / `OPENAI_*` / `ALPHAVANTAGE_API_KEY` 환경변수 확인
  - `reconcile_engine` 동작 확인
  - `sync_exchange` 정합 확인
- 로그 저장(옵션)
  - `LOG_TO_FILE=1`
  - `LOG_FILE=/var/log/auto-trader.log`

### 운영 실행 순서
1. Preflight만 실행
   - `python -m core.run_bot_preflight --preflight-only`
2. ALL GREEN 확인 후 운영 실행
   - `python -m core.run_bot_preflight`
   - 또는 `python -m core.run_bot_ws` (내부 preflight 선행)

### 권장 주기
- `ANALYST_AUTO_REPORT_MARKET_INTERVAL_SEC=300`
  - 너무 작으면 `market_features` / `trade_context_snapshots` row가 과도하게 증가한다.

### Runtime ownership 권장
- Async Worker: `engine_bootstrap` 단일 ownership
- Watchdog: `engine_bootstrap` 단일 ownership
- Monitoring cycle: snapshot 검증 / 상태 승격만 담당
- Preflight: runtime resource ownership 금지

---------------------------------------------------------------------------
10) 테스트/가상매매(잔고 없을 때)
---------------------------------------------------------------------------

권장 절차
1. `TEST_DRY_RUN=1` 로 실행
2. Preflight ALL GREEN 확인
3. 엔트리 후보가 발생하도록 충분히 구동
4. `bt_trade_snapshots` 레코드 생성 확인
5. `equity_current_usdt / equity_peak_usdt / dd_pct` NULL 여부 확인
6. `market_features / trade_context_snapshots` 적재 확인
7. 필요 시 외부 snapshot 테이블 row 증가 확인

주의
- `TEST_BYPASS_GUARDS=1` 은 운영에서 절대 사용 금지

---------------------------------------------------------------------------
11) 알려진 문제/주의사항
---------------------------------------------------------------------------

### 11.1 잔고 없음
- 현상: 주문 단계에서 거래소가 insufficient balance 반환 → 예외 중단
- 해결:
  - 테스트: `TEST_DRY_RUN=1`
  - 실주문: 선물 지갑 USDT 필요

### 11.2 equity/dd가 NULL 저장
- 원인 A: execution_engine이 meta에 equity/dd를 주입하지 못함
- 원인 B: ORM-DB 컬럼 정합 불일치
- 해결:
  - account_state 생성 후 meta 주입 강제
  - ORM-DB 정합 유지

### 11.3 EV heatmap NOT_READY 지속
- 원인: exit 단계 `pnl_r` 업데이트 미연결
- 해결:
  - exit 시점 `pnl_r` 확정 후 `ev_heatmap_engine.on_trade_close()` 호출
  - 추정 변환 금지

### 11.4 OpenAI 설정 누락
- 현상: Preflight SETTINGS 단계 실패
- 해결:
  - `OPENAI_API_KEY`
  - `OPENAI_MODEL`

### 11.5 4H 관련
- 4H OHLCV/EMA/구조 계산은 필수
- 4h.regime 객체는 upstream에 없을 수 있으며, 이 경우 trend_strength는 규칙 기반으로 산출

### 11.6 Alpha Vantage quota
- 현상: 내가 호출하지 않았는데 quota 초과
- 대표 원인:
  - API 키 유출
  - 다른 서버/프로세스 실행
  - 뉴스 + 거시가 같은 키를 공유
- 해결:
  - 키 재발급
  - DB snapshot cache 전제 운영
  - source fetcher 단일 진실원 유지

### 11.7 롱/숏 뒤집힘
- `core/entry_pipeline.py`
  - 5m-only 방향 진입 차단
  - higher timeframe alignment 강제
- `execution/order_executor.py`
  - LONG→BUY / SHORT→SELL 계약 고정
- `state/sync_exchange.py`
  - positionAmt 부호 기반 LONG/SHORT 판별 고정
- 따라서 실제 뒤집힘이 발생한다면 상위 signal 생성 레이어를 우선 점검한다.

### 11.8 Runtime resource duplicate-start
- 현상:
  - `Async worker already started with different config`
  - `engine_watchdog already running`
- 원인:
  - 동일 프로세스에서 runtime resource를 두 번 시작
  - ownership 불일치(preflight / bootstrap / monitoring 중복)
- 해결 원칙:
  - Preflight는 resource owner가 아니다.
  - Async Worker / Watchdog는 단일 owner만 start
  - 나머지는 attach / snapshot verification만 수행

### 11.9 Telegram 인증 실패
- 현상:
  - `401 Unauthorized`
  - `invalid telegram token`
- 해결:
  - `TELEGRAM_BOT_TOKEN`
  - `TELEGRAM_CHAT_ID`
  - 운영 토큰 회전 및 유출 토큰 폐기

---------------------------------------------------------------------------
12) 면책
---------------------------------------------------------------------------

자동매매는 손실 위험이 크다.  
사용자는 모든 책임을 본인이 부담한다.  
운영 전 반드시 소액/테스트로 검증한다.         