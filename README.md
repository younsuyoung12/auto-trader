# Auto-Trader (Binance USDT-M Futures) — STRICT · NO-FALLBACK · Production

이 저장소는 **BTCUSDT 단일 심볼** 기준의 실전형(운영형) 자동매매 엔진이다.  
핵심 목표는 “수익률 과장”이 아니라 **운영 안정성(무중단/무결성/Fail-Fast)** 이다.

- WebSocket 기반 시세 수집(캔들/오더북)
- 전략 판단(지표/피처 + GPT)
- 실행(주문/TP·SL) + DB 영속화(거래/스냅샷/이벤트)
- 대시보드/분석은 Render에서 **DB 조회 전용(read-only analytics)** 으로 분리

---

## 1. 설계 원칙 (절대 준수)

### 1.1 STRICT · NO-FALLBACK
- 데이터 누락/불일치/모호성 발생 시 **추정/보정/대체값**으로 계속 진행하지 않는다.
- 즉시 예외(raise) 또는 **명시적 SKIP** 처리한다.
- “조용히 넘어가기”는 금지.

### 1.2 보안
- 거래소 API Key/Secret은 **AWS 실행 워커에만 존재**한다.
- Render(대시보드/DB)에는 **거래소 실행 권한/키를 절대 저장하지 않는다.**
- DB는 **TLS(SSL)** 로만 연결한다.

### 1.3 운영 안정성 (기관급에 근접한 최소 변경)
- 메인 루프에서 **외부 I/O(Telegram/DB 등)** 로 블로킹되지 않도록 비동기 위임
- 내부 상태 ↔ 거래소 상태 **Reconcile(불일치 감지)** 도입
- 지연(latency) 예산 초과 시 신규 진입 차단(SAFE_STOP) 가능

---

## 2. 배포 토폴로지 (Production)

### 2.1 Execution (Auto-Trader Worker): AWS
- 24/7 트레이딩 워커 실행
- WebSocket ingest → strategy → execution
- 거래소 API keys/secrets 보관 (**절대 Render 저장 금지**)
- Render Postgres로 **TLS 연결**하여 trades/events/snapshots 기록

### 2.2 Database + Dashboard: Render
- Render Managed Postgres에 모든 거래/스냅샷/이벤트 저장
- Render에 Dashboard/API 서버 배포(분석/시각화)
- Render에서는 **주문 실행 절대 없음** (read-only / analytics only)

---

## 3. 보안 경계 (Security Boundary)

- **거래소 크리덴셜은 AWS에만** 존재
- Render 서비스는 거래소 실행 권한 없이 동작(= DB 조회/분석만)
- 누락/불일치 데이터는 fail-fast(STRICT · NO-FALLBACK)

---

## 4. 구성 요소 개요

### 4.1 AWS Worker (핵심 엔진)
- 실시간 데이터 수집: `infra/market_data_ws.py` 등
- 피처 생성: `strategy/unified_features_builder.py`
- 전략 판단(GPT 포함): `strategy/gpt_strategy.py` (또는 gpt_trader 계열)
- 실행: `execution/execution_engine.py`
- 주문 실행(거래소): `execution/order_executor.py`
- 거래소 상태 동기화: `state/sync_exchange.py`
- Desync 감지: `sync/reconcile_engine.py`
- 비동기 I/O 큐: `infra/async_worker.py`

### 4.2 Render Postgres (실전 분석형 저장소)
- 거래 기록, 스냅샷, 이벤트, 캔들/오더북/지표 저장
- 대시보드/분석의 단일 소스(SSOT)

### 4.3 Render Dashboard/API (조회 전용)
- Postgres에서 SELECT 기반으로 성과/리스크/드로다운/승률/신호 로그 시각화
- 주문 실행 로직은 존재하면 안 됨

---

## 5. 환경변수 (중요)

### 5.1 AWS Worker 필수
- `BINANCE_API_KEY`
- `BINANCE_API_SECRET`
- `TRADER_DB_URL`  
  - Render Postgres **External URL** 사용(외부 접속)
  - SSL 포함 권장: `?sslmode=require`
- (선택) `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`

### 5.2 Render Dashboard/API 필수
- `DATABASE_URL` (Render가 제공하는 내부 URL)
- (선택) `DASHBOARD_SECRET_KEY` 등

### 5.3 운영형(기관급 근접) 설정 키
- `ASYNC_WORKER_THREADS` (기본 1)
- `ASYNC_WORKER_QUEUE_SIZE` (기본 2000)
- `RECONCILE_INTERVAL_SEC` (기본 30)
- `FORCE_CLOSE_ON_DESYNC` (기본 False)
- `MAX_SIGNAL_LATENCY_MS` (기본 200)
- `MAX_EXEC_LATENCY_MS` (기본 400)

---

## 6. DB 스키마 (Render Postgres / public)

현재 public 스키마의 테이블 목록:

- `bt_trades`
- `bt_trade_snapshots`
- `bt_trade_exit_snapshots`
- `bt_events`
- `bt_external_events`
- `bt_entry_scores`
- `bt_regime_scores`
- `bt_candles`
- `bt_indicators`
- `bt_orderbook_snapshots`
- `bt_funding_rates`

---

### 6.1 `bt_trades` (거래 “결과” 중심 테이블)

**핵심 목적**
- 실제 진입/청산된 거래를 1 row = 1 trade로 저장
- 대시보드의 PnL/승률/거래 리스트 기본 소스

**주요 컬럼**
- `id` (PK, int)
- `symbol` (varchar(32), not null)
- `side` (varchar(8), not null)  
  - 예: BUY/SELL (거래소 실행 기준)
- `entry_ts` (timestamptz, not null)
- `exit_ts` (timestamptz, nullable)
- `entry_price` (numeric(24,8), not null)
- `exit_price` (numeric(24,8), nullable)
- `qty` (numeric(24,8), not null)
- `pnl_usdt` (numeric(24,8), nullable)
- `pnl_pct_futures` (double precision)
- `pnl_pct_spot_ref` (double precision)
- `is_auto` (boolean, not null)
- `regime_at_entry` / `regime_at_exit` (varchar(16))
- `entry_score` (double precision)
- `trend_score_at_entry`, `range_score_at_entry` (double precision)
- `strategy` (varchar(16))
- `close_reason` (varchar(32))
- `leverage` (double precision)
- `risk_pct` (double precision)
- `tp_pct`, `sl_pct` (double precision)
- `note` (varchar(255))
- `created_at`, `updated_at` (timestamptz, not null)

**실전 운영/복구 컬럼(중요)**
- `entry_order_id` (varchar(64))
- `tp_order_id` (varchar(64))
- `sl_order_id` (varchar(64))
- `exchange_position_side` (varchar(16))
- `remaining_qty` (numeric(24,8))
- `realized_pnl_usdt` (numeric(24,8))
- `reconciliation_status` (varchar(32))
- `last_synced_at` (timestamptz)

**인덱스**
- PK: `bt_trades_pkey(id)`
- 조회 최적화: `ix_bt_trades_symbol_entry_ts(symbol, entry_ts)`

---

### 6.2 `bt_trade_snapshots` (거래 “진입 시점” 분석 스냅샷)

**핵심 목적**
- “왜 진입/스킵했는가?”를 재현하기 위한 **결정 시점 상태 저장**
- 대시보드에서 DD(드로다운), equity curve, 전략 성능 분석의 핵심 데이터

**주요 컬럼**
- `id` (PK, int)
- `trade_id` (FK→bt_trades.id, UNIQUE)
- `symbol` (varchar(32), not null)
- `entry_ts` (timestamptz, not null)
- `direction` (varchar(10), not null)  
  - 예: LONG/SHORT
- `signal_source` (varchar(32))
- `regime` (varchar(32))
- `entry_score` (double precision)
- `engine_total` (double precision)
- `trend_strength`, `atr_pct`, `volume_zscore`, `depth_ratio`, `spread_pct` (double precision)
- `hour_kst`, `weekday_kst` (int)
- `last_price` (numeric(24,8))
- `risk_pct`, `tp_pct`, `sl_pct` (double precision)
- `gpt_action` (varchar(16))
- `gpt_reason` (text)
- `created_at` (timestamptz, not null, default CURRENT_TIMESTAMP)

**드로다운/자산곡선 핵심 컬럼**
- `equity_current_usdt` (double precision)
- `equity_peak_usdt` (double precision)
- `dd_pct` (double precision)

**인덱스/제약**
- PK: `bt_trade_snapshots_pkey(id)`
- UNIQUE: `ux_bt_trade_snapshots_tradeid(trade_id)`
- 인덱스: `ix_bt_trade_snapshots_symbol_entryts(symbol, entry_ts)`
- FK: `trade_id`는 `bt_trades(id)` 참조(ON DELETE CASCADE)

---

### 6.3 운영 체크 포인트 (중요)

#### (A) 스냅샷의 equity/dd는 NULL이면 안 된다
`equity_current_usdt`, `equity_peak_usdt`, `dd_pct`가 NULL이면
- DD 계산 불가
- equity curve 복원 불가
- 리스크/HARD_STOP 근거 상실

즉, 분석형 DB가 “반쪽짜리”가 된다.

---

## 7. DB 접속/점검 명령어 (psql)

### 7.1 접속
```bash
psql "$TRADER_DB_URL"
## 8. 운영 권장

- **24/7 유지**
  - `systemd` 또는 프로세스 매니저(`pm2` / `supervisor`)로 상시 실행
- **재시작 시 필수 점검**
  - 프로세스 재시작 후 **거래소 키/DB 환경변수 재주입**(로드) 여부 확인
- **로그 파일 저장(옵션)**
  - `LOG_TO_FILE=1`
  - `LOG_FILE=/var/log/auto-trader.log` (예시)

---

## 9. 테스트/가상매매 (주의)

프로젝트에는 테스트 플래그가 존재할 수 있다.

- `TEST_DRY_RUN=1`
  - **주문 없이 DB 기록만 수행**(구현에 따라 다름)
- `TEST_BYPASS_GUARDS=1`
  - **운영에서 절대 사용 금지**

> 운영 서버(AWS)에서는 테스트 플래그를 사용하지 않는다.

---

## 10. 파일 구조 (상세 설명)

아래 설명만 읽어도 “어디가 무엇을 하는지” 알 수 있도록 작성했다.

### 10.1 `settings.py`
- 모든 설정의 단일 소스(SSOT)
- ENV 우선, 로컬 `.env`는 선택(외부 의존성 없이 내장 로더)
- 잘못된 설정은 즉시 예외(Fail-Fast)
- 추가 운영형 키:
  - `ASYNC_WORKER_THREADS`, `ASYNC_WORKER_QUEUE_SIZE`
  - `RECONCILE_INTERVAL_SEC`, `FORCE_CLOSE_ON_DESYNC`
  - `MAX_SIGNAL_LATENCY_MS`, `MAX_EXEC_LATENCY_MS`

### 10.2 `core/run_bot_ws.py`
- WebSocket 기반 메인 루프
- 부팅 시 REST 백필 → WS 시작 → 워밍업 → 헬스 모니터 → 메인 루프 진입
- 엔트리 판단(전략) → 실행 엔진 호출
- 주기적 리콘실(Reconcile)로 내부 상태 ↔ 거래소 상태 불일치 감지
- SAFE_STOP / SIGTERM 처리 포함

### 10.3 `infra/market_data_ws.py`
- Binance Futures WS ingest
- 캔들/오더북 버퍼 유지
- 엔트리/엑싯 판단에 사용할 “WS 기반 최신 데이터” 제공

### 10.4 `infra/market_data_rest.py`
- 부팅 시점 REST 백필 전용
- 런타임에서 REST 폴백으로 의사결정하지 않는다(STRICT)

### 10.5 `infra/data_health_monitor.py`
- WS 데이터의 “신선도/지연” 감시
- HEALTH_FAIL 시 엔트리 차단(명시적 SKIP)

### 10.6 `infra/async_worker.py`
- 메인 루프가 텔레그램/DB 작업 때문에 멈추는 것을 방지
- `submit()`으로 비핵심 I/O 작업을 백그라운드 워커 큐로 위임
- 큐 포화 시 non-critical 작업은 DROP 가능(운영 안정성)

### 10.7 `sync/reconcile_engine.py`
- 내부 상태(OPEN_TRADES 등)와 거래소 포지션/주문 상태를 주기적으로 비교
- 불일치(Desync) 감지 시:
  - 신규 진입 차단(SAFE_STOP)
  - 옵션으로 강제 청산(`FORCE_CLOSE_ON_DESYNC=1`)

### 10.8 `strategy/*`
- `unified_features_builder.py`: WS 데이터 기반 피처 생성
- `gpt_strategy.py` 또는 `gpt_trader.py`: GPT 판단 포함 진입 결정
- `regime_engine.py`: 시장 국면(regime) 판정(진입 허용/비허용)
- `account_state_builder.py`: equity/peak/dd 등 계좌 상태 생성(스냅샷 핵심)

### 10.9 `execution/execution_engine.py`
- 실행만 담당: 가드 → 주문 실행 → DB 기록 → 알림
- 핵심 운영형 기능:
  - 전역 단일 락으로 `execute` 동시 실행 금지(레이스 방지)
  - 텔레그램은 비동기 위임(블로킹 금지)
  - deterministic `entry_client_order_id` 사용(중복 진입 방지)
  - `bt_trades` / `bt_trade_snapshots`에 분석 필드 영속화

### 10.10 `execution/order_executor.py`
- 거래소 주문 실행 레이어
- 시장가/TP/SL 등 실제 주문 관련 로직
- 실행 엔진과 분리되어 책임이 명확해야 한다.

### 10.11 `state/db_writer.py`
- DB INSERT/UPDATE 전용 모듈
- trade open/close, snapshot 기록을 담당
- STRICT 기준: 필수값 누락 시 즉시 예외

---

## 11. 운영 체크리스트 (필수)

### 11.1 AWS Worker
- 거래소 API 키가 AWS에만 존재
- `TRADER_DB_URL`은 Render External URL + SSL
- `async_worker` 시작 확인(블로킹 I/O 제거)
- `reconcile_engine` 활성화(Desync 감지)
- latency 예산 설정 가능(과도 지연 시 신규 진입 차단)

### 11.2 Render
- Render 서비스에 거래소 키/권한 없음
- Dashboard/API는 SELECT 위주
- DB에 trades/snapshots/events 정상 누적

---

## 12. 알려진 문제/주의사항

### 12.1 `bt_trade_snapshots`의 equity/dd 값이 NULL로 저장되는 경우
- 원인: `execution_engine`이 `record_trade_snapshot` 호출 시  
  `equity_current_usdt` / `equity_peak_usdt` / `dd_pct`를 meta에서 못 받는 경우
- 해결: entry 흐름에서 `account_state` 생성 후 meta에 반드시 주입해야 한다.

---

## 13. 면책
자동매매는 손실 위험이 크다.  
사용자는 모든 책임을 본인이 부담한다.  
운영 전 반드시 소액/테스트로 검증한다.