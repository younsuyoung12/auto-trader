"""
========================================================
FILE: execution/exchange_api.py
ROLE:
- Binance USDT-M Futures REST Adapter SSOT
- 서명/HTTP 요청/계정/포지션/주문 조회 및 레버리지/마진 모드 설정만 담당한다
- 주문 실행(진입/청산/TP/SL/슬리피지 계산)은 order_executor.py가 담당한다

CORE RESPONSIBILITIES:
- Binance Futures private/public REST 요청 전송
- timestamp/signature 생성 및 time sync 관리
- account / balance / position / order 조회
- leverage / margin mode 설정
- user data stream listenKey lifecycle 관리
- public microstructure 데이터 조회

IMPORTANT POLICY:
- STRICT · NO-FALLBACK · TRADE-GRADE
- Binance Futures endpoints only:
  - Trading/Account: /fapi/*
  - Public microstructure: /futures/data/*
- One-way mode only
- Hedge mode is NOT supported
- positionSide must always be BOTH in order layer
- 오류/누락/비정상 응답은 즉시 예외
- 민감정보(API key/secret/signature)는 로그/예외에 포함 금지
- settings.py 단일 객체(SSOT)만 사용
- stale signed URL 재사용 금지: private 재시도 시마다 timestamp/signature 재생성

CHANGE HISTORY:
- 2026-03-11:
  1) FIX(ROOT-CAUSE): private 요청 재시도 시 stale timestamp/signature URL 재사용 제거
  2) FIX(STRUCTURE): _request_with_resilience()를 고정 URL 방식에서 request_factory 방식으로 변경
  3) FIX(SSOT): load_settings() 직접 호출 제거, settings.SETTINGS 단일 객체 사용
  4) FIX(STRICT): private 예약 파라미터(timestamp/signature/recvWindow) 외부 주입 금지
  5) FIX(STRICT): BASE_URL trailing slash 정규화 및 스킴 검증 추가
  6) FIX(CONTRACT): assert_one_way_mode() 응답 타입 계약 강화
- 2026-03-06:
  1) USER DATA STREAM listenKey 전용 API 추가
     - create_user_data_listen_key()
     - keepalive_user_data_listen_key()
     - close_user_data_listen_key()
  2) listenKey API는 API-KEY only 흐름으로 분리
     - signature/timestamp 없이 전용 요청 함수로 처리
  3) get_position() STRICT 강화
     - rows[0] 선택 제거
     - one-way 기준 유일 position row 선택 로직 추가
- 2026-03-04:
  1) 네트워크 내구성 강화
     - HTTP 5xx/429/Timeout 재시도 + exponential backoff
     - Circuit breaker 추가
  2) 동시성 안전성 보강
     - _SERVER_TIME_OFFSET_MS / _TIME_SYNCED / _LAST_TIME_SYNC_AT Lock 보호
  3) -1021(timestamp) 대응 강화
     - -1021 감지 시 강제 time sync 후 재시도
  4) 예외 전파 구조 정비
     - URL(서명 포함) 노출 금지 유지
- 2026-03-03:
  1) Microstructure 데이터 계층 확장
  2) settings 값 누락/비정상 시 즉시 실패(Fail-Fast)
- 2026-03-02:
  1) 주문/체결/복구용 조회 함수 추가
========================================================
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import math
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple
from urllib.parse import urlencode

import requests

from execution.retry_policy import execute_with_retry
from settings import SETTINGS

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------#
# Global settings / session (STRICT: fail-fast / SSOT)
# -----------------------------------------------------------------------------#
SET = SETTINGS


def _require_setting_str(name: str) -> str:
    try:
        v = getattr(SET, name)
    except AttributeError as e:
        raise RuntimeError(f"settings.{name} is missing (STRICT)") from e
    s = str(v).strip()
    if not s:
        raise RuntimeError(f"settings.{name} is empty (STRICT)")
    return s


def _require_setting_int(name: str) -> int:
    try:
        v = getattr(SET, name)
    except AttributeError as e:
        raise RuntimeError(f"settings.{name} is missing (STRICT)") from e
    try:
        iv = int(v)
    except Exception as e:
        raise RuntimeError(f"settings.{name} must be int (got={v!r})") from e
    if iv <= 0:
        raise RuntimeError(f"settings.{name} must be positive (got={iv})")
    return iv


def _require_base_url() -> str:
    raw = _require_setting_str("binance_futures_base")
    norm = raw.rstrip("/")
    if not norm:
        raise RuntimeError("settings.binance_futures_base normalized to empty (STRICT)")
    if not (norm.startswith("https://") or norm.startswith("http://")):
        raise RuntimeError("settings.binance_futures_base must start with http:// or https:// (STRICT)")
    return norm


BASE_URL: str = _require_base_url()
DEFAULT_TIMEOUT_SEC: int = _require_setting_int("request_timeout_sec")
RECV_WINDOW_MS: int = _require_setting_int("recv_window_ms")

_API_KEY: str = _require_setting_str("api_key")
_API_SECRET: str = _require_setting_str("api_secret")

_SESSION: requests.Session = requests.Session()
_SESSION.headers.update(
    {
        "Accept": "application/json",
        "User-Agent": "auto-trader/binance-usdtm",
    }
)

# Server time offset in milliseconds: local_ms + offset_ms = server_ms
_SERVER_TIME_OFFSET_MS: int = 0
_TIME_SYNCED: bool = False
_LAST_TIME_SYNC_AT: float = 0.0
_TIME_SYNC_TTL_SEC: int = 60

# TIME state lock
_TIME_STATE_LOCK = threading.Lock()
_TIME_SYNC_LOCK = threading.Lock()

# Allowed endpoints
_ALLOWED_PUBLIC_PREFIXES: Tuple[str, ...] = ("/fapi/", "/futures/data/")
_ALLOWED_PRIVATE_PREFIX: str = "/fapi/"
_ALLOWED_APIKEY_ONLY_PATHS: Tuple[str, ...] = ("/fapi/v1/listenKey",)

_PRIVATE_RESERVED_PARAM_KEYS: Tuple[str, ...] = ("timestamp", "signature", "recvWindow")

# -----------------------------------------------------------------------------#
# Circuit breaker (TRADE-GRADE)
# -----------------------------------------------------------------------------#
_CB_LOCK = threading.Lock()
_CB_FAILURES: int = 0
_CB_OPEN_UNTIL_TS: float = 0.0

_CB_FAIL_THRESHOLD: int = 6
_CB_OPEN_SEC: float = 12.0
_CB_MAX_OPEN_SEC: float = 60.0
_CB_BYPASS_PATHS: Tuple[str, ...] = ("/fapi/v1/time",)


def _cb_is_open(now: float) -> bool:
    with _CB_LOCK:
        return now < _CB_OPEN_UNTIL_TS


def _cb_on_success() -> None:
    global _CB_FAILURES, _CB_OPEN_UNTIL_TS
    with _CB_LOCK:
        _CB_FAILURES = 0
        _CB_OPEN_UNTIL_TS = 0.0


def _cb_on_failure(*, reason: str) -> None:
    global _CB_FAILURES, _CB_OPEN_UNTIL_TS
    now = time.time()
    with _CB_LOCK:
        _CB_FAILURES += 1
        failures = _CB_FAILURES
        if _CB_FAILURES >= _CB_FAIL_THRESHOLD:
            extra = float(_CB_FAILURES - _CB_FAIL_THRESHOLD)
            open_sec = min(_CB_MAX_OPEN_SEC, _CB_OPEN_SEC * (1.0 + extra / 2.0))
            _CB_OPEN_UNTIL_TS = max(_CB_OPEN_UNTIL_TS, now + open_sec)
        open_until = _CB_OPEN_UNTIL_TS

    logger.warning(
        "circuit failure recorded (failures=%s open_until_ts=%.3f reason=%s)",
        failures,
        open_until,
        reason,
    )


# -----------------------------------------------------------------------------#
# Retry policy (TRADE-GRADE)
# -----------------------------------------------------------------------------#
@dataclass(frozen=True)
class _NetRetryPolicy:
    max_attempts: int = 5
    base_delay_sec: float = 0.6
    max_delay_sec: float = 8.0
    jitter_sec: float = 0.15


def _sleep_backoff(policy: _NetRetryPolicy, attempt_idx: int, *, reason: str) -> None:
    delay = policy.base_delay_sec * (2.0 ** attempt_idx)
    if delay > policy.max_delay_sec:
        delay = policy.max_delay_sec

    frac = time.time() % 1.0
    delay = delay + min(policy.jitter_sec, frac * policy.jitter_sec)

    logger.warning("net backoff sleep=%0.3fs reason=%s", delay, reason)
    time.sleep(delay)


def _timeout_tuple(timeout_sec: int) -> Tuple[float, float]:
    if not isinstance(timeout_sec, int) or timeout_sec <= 0:
        raise ValueError("timeout_sec must be positive int")
    connect = 3.5 if timeout_sec >= 4 else max(1.0, float(timeout_sec) * 0.7)
    read = float(timeout_sec)
    return (float(connect), float(read))


def _local_ts_ms() -> int:
    return int(time.time() * 1000)


def _get_time_state() -> Tuple[int, bool, float]:
    with _TIME_STATE_LOCK:
        return (_SERVER_TIME_OFFSET_MS, _TIME_SYNCED, _LAST_TIME_SYNC_AT)


def _set_time_state(*, offset_ms: int) -> None:
    global _SERVER_TIME_OFFSET_MS, _TIME_SYNCED, _LAST_TIME_SYNC_AT
    with _TIME_STATE_LOCK:
        _SERVER_TIME_OFFSET_MS = int(offset_ms)
        _TIME_SYNCED = True
        _LAST_TIME_SYNC_AT = time.time()


def _ts_ms() -> int:
    offset, _, _ = _get_time_state()
    return _local_ts_ms() + int(offset)


# -----------------------------------------------------------------------------#
# Internal helpers (STRICT)
# -----------------------------------------------------------------------------#
def _normalize_symbol(symbol: str) -> str:
    s = str(symbol).replace("-", "").replace("/", "").upper().strip()
    if not s:
        raise ValueError("symbol is empty")
    return s


def _normalize_margin_type(value: Any) -> str:
    v = str(value).strip().upper()
    if not v:
        raise RuntimeError("positionRisk.marginType is missing/empty")

    if v == "ISOLATED":
        return "ISOLATED"
    if v in {"CROSS", "CROSSED"}:
        return "CROSSED"

    raise RuntimeError(f"positionRisk.marginType unexpected value: {value!r}")


def _normalize_position_side(value: Any) -> str:
    v = str(value or "").strip().upper()
    if not v:
        return "BOTH"
    if v not in {"BOTH", "LONG", "SHORT"}:
        raise RuntimeError(f"positionRisk.positionSide unexpected value: {value!r}")
    return v


def _coerce_params(params: Optional[Mapping[str, Any]]) -> List[Tuple[str, str]]:
    items: List[Tuple[str, str]] = []
    if params is None:
        return items

    for k, v in params.items():
        if v is None:
            continue
        key = str(k)
        if isinstance(v, bool):
            val = "true" if v else "false"
        else:
            val = str(v)
        items.append((key, val))

    items.sort(key=lambda kv: kv[0])
    return items


def _encode_query(params: Optional[Mapping[str, Any]]) -> str:
    items = _coerce_params(params)
    if not items:
        return ""
    return urlencode(items, doseq=True)


def _sign(query_string: str) -> str:
    return hmac.new(
        _API_SECRET.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _require_int(name: str, v: Any) -> int:
    try:
        iv = int(v)
    except Exception as e:
        raise ValueError(f"{name} must be int (got={v!r})") from e
    return iv


def _require_float(name: str, v: Any) -> float:
    try:
        fv = float(v)
    except Exception as e:
        raise ValueError(f"{name} must be float (got={v!r})") from e
    if not math.isfinite(fv):
        raise ValueError(f"{name} must be finite (got={fv})")
    return fv


def _validate_ms_range(start_ms: Optional[int], end_ms: Optional[int]) -> None:
    if start_ms is not None:
        if not isinstance(start_ms, int) or start_ms <= 0:
            raise ValueError("start_time_ms must be int > 0")
    if end_ms is not None:
        if not isinstance(end_ms, int) or end_ms <= 0:
            raise ValueError("end_time_ms must be int > 0")
    if start_ms is not None and end_ms is not None and end_ms < start_ms:
        raise ValueError("end_time_ms must be >= start_time_ms")


def _validate_period(period: str) -> str:
    p = str(period).strip()
    if not p:
        raise ValueError("period is empty")
    allowed = {"5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"}
    if p not in allowed:
        raise ValueError(f"period must be one of {sorted(allowed)} (got={p!r})")
    return p


def _build_request_url(path: str, query: str) -> str:
    if not path.startswith("/"):
        raise ValueError(f"path must start with '/' (got={path!r})")
    return f"{BASE_URL}{path}" + (f"?{query}" if query else "")


def _assert_no_reserved_private_params(params: Mapping[str, Any]) -> None:
    for key in _PRIVATE_RESERVED_PARAM_KEYS:
        if key in params:
            raise ValueError(f"private req params must not include reserved key {key!r} (STRICT)")


def sync_server_time() -> int:
    with _TIME_SYNC_LOCK:
        t0 = time.time()
        data = req("GET", "/fapi/v1/time", private=False)
        t1 = time.time()

        if not isinstance(data, dict) or "serverTime" not in data:
            raise RuntimeError("GET /fapi/v1/time -> unexpected response shape")

        server_ms = _require_int("serverTime", data["serverTime"])
        local_mid_ms = int(((t0 + t1) / 2.0) * 1000)
        offset_ms = server_ms - local_mid_ms

        _set_time_state(offset_ms=offset_ms)

        logger.info("Binance server time synced (offset_ms=%s)", offset_ms)
        return int(offset_ms)


def sync_server_time_offset() -> int:
    return sync_server_time()


def _ensure_time_sync(force: bool = False) -> None:
    _, synced, last_ts = _get_time_state()

    if force or (not synced) or (time.time() - float(last_ts) > _TIME_SYNC_TTL_SEC):
        sync_server_time()
        _, synced2, _ = _get_time_state()
        if not synced2:
            raise RuntimeError("server time sync failed")


def _parse_error_payload(resp: requests.Response) -> Dict[str, Any]:
    try:
        j = resp.json()
        if isinstance(j, dict):
            return j
        return {"message": str(j)[:300]}
    except Exception:
        return {"message": (resp.text or "").strip()[:300]}


def _request_with_resilience(
    *,
    method: str,
    path_for_logs: str,
    policy: _NetRetryPolicy,
    request_factory: Callable[[], requests.Response],
) -> requests.Response:
    now = time.time()
    if path_for_logs not in _CB_BYPASS_PATHS and _cb_is_open(now):
        with _CB_LOCK:
            remain = max(0.0, _CB_OPEN_UNTIL_TS - now)
        raise RuntimeError(f"circuit_open (path={path_for_logs}, retry_after_sec={remain:.1f})")

    last_exc: Optional[BaseException] = None

    for attempt in range(policy.max_attempts):
        try:
            resp = execute_with_retry(request_factory)
        except requests.Timeout as e:
            last_exc = e
            _cb_on_failure(reason=f"timeout:{path_for_logs}")
            if attempt >= policy.max_attempts - 1:
                raise RuntimeError(f"{method} {path_for_logs} -> timeout after retries") from None
            _sleep_backoff(policy, attempt, reason=f"timeout:{path_for_logs}")
            continue
        except requests.RequestException as e:
            last_exc = e
            _cb_on_failure(reason=f"request_exception:{e.__class__.__name__}:{path_for_logs}")
            if attempt >= policy.max_attempts - 1:
                raise RuntimeError(f"{method} {path_for_logs} -> request failed: {e.__class__.__name__}") from None
            _sleep_backoff(policy, attempt, reason=f"request_exception:{e.__class__.__name__}:{path_for_logs}")
            continue
        except Exception as e:
            _cb_on_failure(reason=f"unexpected_exception:{e.__class__.__name__}:{path_for_logs}")
            raise RuntimeError(f"{method} {path_for_logs} -> unexpected error: {e.__class__.__name__}") from e

        sc = int(getattr(resp, "status_code", 0) or 0)

        if sc == 200:
            _cb_on_success()
            return resp

        err = _parse_error_payload(resp)
        code = err.get("code")
        msg = err.get("msg") or err.get("message")

        if isinstance(code, int) and int(code) == -1021:
            _cb_on_failure(reason=f"binance_-1021:{path_for_logs}")
            if attempt >= policy.max_attempts - 1:
                raise RuntimeError(f"{method} {path_for_logs} -> binance -1021 after retries") from None
            logger.warning("%s %s -> binance -1021, forcing time sync then retry", method, path_for_logs)
            _ensure_time_sync(force=True)
            _sleep_backoff(policy, attempt, reason=f"-1021:{path_for_logs}")
            continue

        if sc == 429:
            _cb_on_failure(reason=f"http_429:{path_for_logs}")
            if attempt >= policy.max_attempts - 1:
                raise RuntimeError(f"{method} {path_for_logs} -> HTTP 429 after retries") from None
            ra = None
            try:
                ra_h = resp.headers.get("Retry-After") if hasattr(resp, "headers") else None
                if ra_h is not None:
                    ra = float(str(ra_h).strip())
            except Exception:
                ra = None
            if ra is not None and ra > 0:
                logger.warning("%s %s -> HTTP 429 Retry-After=%s sec", method, path_for_logs, ra)
                time.sleep(min(float(ra), policy.max_delay_sec))
            else:
                _sleep_backoff(policy, attempt, reason=f"http_429:{path_for_logs}")
            continue

        if sc in (502, 503, 504):
            _cb_on_failure(reason=f"http_{sc}:{path_for_logs}")
            if attempt >= policy.max_attempts - 1:
                raise RuntimeError(f"{method} {path_for_logs} -> HTTP {sc} after retries, msg={msg}") from None
            _sleep_backoff(policy, attempt, reason=f"http_{sc}:{path_for_logs}")
            continue

        _cb_on_failure(reason=f"http_{sc}:{path_for_logs}")
        raise RuntimeError(f"{method} {path_for_logs} -> HTTP {sc}, code={code}, msg={msg}")

    if last_exc is not None:
        raise RuntimeError(f"{method} {path_for_logs} -> request failed after retries: {last_exc.__class__.__name__}") from None
    raise RuntimeError(f"{method} {path_for_logs} -> request failed after retries (unknown)")


def _req_api_key_only(
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
) -> Any:
    m = str(method).upper().strip()
    if m not in {"POST", "PUT", "DELETE", "GET"}:
        raise ValueError(f"unsupported http method: {method!r}")

    p = str(path).strip()
    if not p.startswith("/"):
        p = "/" + p

    if p not in _ALLOWED_APIKEY_ONLY_PATHS:
        raise ValueError(f"api-key-only endpoint not allowed (STRICT): {p}")

    if not isinstance(timeout_sec, int) or timeout_sec <= 0:
        raise ValueError("timeout_sec must be positive int")

    timeout = _timeout_tuple(timeout_sec)
    base_params: Dict[str, Any] = dict(params or {})

    def _do_request() -> requests.Response:
        query = _encode_query(base_params or None)
        url = _build_request_url(p, query)
        return _SESSION.request(
            m,
            url,
            headers={"X-MBX-APIKEY": _API_KEY},
            timeout=timeout,
        )

    resp = _request_with_resilience(
        method=m,
        path_for_logs=p,
        policy=_NetRetryPolicy(),
        request_factory=_do_request,
    )

    try:
        data = resp.json()
    except Exception as e:
        raise RuntimeError(f"{m} {p} -> invalid json: {e.__class__.__name__}") from None

    if isinstance(data, dict) and isinstance(data.get("code"), int) and int(data["code"]) < 0:
        raise RuntimeError(f"{m} {p} -> binance code={data.get('code')}, msg={data.get('msg')}")

    return data


def req(
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
    *,
    private: bool = True,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
) -> Any:
    if body is not None:
        raise ValueError("req(body=...) is not supported in this adapter (STRICT)")

    m = str(method).upper().strip()
    if m not in {"GET", "POST", "PUT", "DELETE"}:
        raise ValueError(f"unsupported http method: {method!r}")

    p = str(path).strip()
    if not p.startswith("/"):
        p = "/" + p

    if private:
        if not p.startswith(_ALLOWED_PRIVATE_PREFIX):
            raise ValueError(f"private endpoints must be /fapi/* (got={p})")
    else:
        if not any(p.startswith(pref) for pref in _ALLOWED_PUBLIC_PREFIXES):
            raise ValueError(f"public endpoints must be /fapi/* or /futures/data/* (got={p})")

    if not isinstance(timeout_sec, int) or timeout_sec <= 0:
        raise ValueError("timeout_sec must be positive int")

    timeout = _timeout_tuple(timeout_sec)
    base_params: Dict[str, Any] = dict(params or {})

    if private:
        _assert_no_reserved_private_params(base_params)
        _ensure_time_sync()

        def _do_request() -> requests.Response:
            q_params: Dict[str, Any] = dict(base_params)
            q_params["recvWindow"] = RECV_WINDOW_MS
            q_params["timestamp"] = _ts_ms()
            query = _encode_query(q_params)
            sig = _sign(query)
            url = f"{BASE_URL}{p}?{query}&signature={sig}"
            return _SESSION.request(
                m,
                url,
                headers={"X-MBX-APIKEY": _API_KEY},
                timeout=timeout,
            )

    else:
        def _do_request() -> requests.Response:
            query = _encode_query(base_params or None)
            url = _build_request_url(p, query)
            return _SESSION.request(
                m,
                url,
                headers=None,
                timeout=timeout,
            )

    resp = _request_with_resilience(
        method=m,
        path_for_logs=p,
        policy=_NetRetryPolicy(),
        request_factory=_do_request,
    )

    try:
        data = resp.json()
    except Exception as e:
        raise RuntimeError(f"{m} {p} -> invalid json: {e.__class__.__name__}") from None

    if isinstance(data, dict) and isinstance(data.get("code"), int) and int(data["code"]) < 0:
        code = data.get("code")
        msg = data.get("msg")

        if int(code) == -1021 and private:
            logger.warning("%s %s -> 200 payload has -1021, forcing time sync then re-request once", m, p)
            _ensure_time_sync(force=True)

            resp2 = _request_with_resilience(
                method=m,
                path_for_logs=p,
                policy=_NetRetryPolicy(max_attempts=2, base_delay_sec=0.4, max_delay_sec=1.2),
                request_factory=_do_request,
            )
            try:
                data2 = resp2.json()
            except Exception as e2:
                raise RuntimeError(f"{m} {p} -> invalid json after -1021 recovery: {e2.__class__.__name__}") from None

            if isinstance(data2, dict) and isinstance(data2.get("code"), int) and int(data2["code"]) < 0:
                raise RuntimeError(f"{m} {p} -> binance code={data2.get('code')}, msg={data2.get('msg')}")
            return data2

        raise RuntimeError(f"{m} {p} -> binance code={code}, msg={msg}")

    return data


def assert_one_way_mode() -> None:
    data = req("GET", "/fapi/v1/positionSide/dual", private=True)
    if not isinstance(data, dict):
        raise RuntimeError("GET /fapi/v1/positionSide/dual -> unexpected response shape")

    dual = data.get("dualSidePosition")
    if not isinstance(dual, bool):
        raise RuntimeError("positionSide/dual.dualSidePosition must be bool (STRICT)")

    if dual is True:
        raise RuntimeError("Account is in HEDGE mode. Switch Binance Futures to ONE-WAY mode.")


# -----------------------------------------------------------------------------#
# USER DATA STREAM listenKey (API-KEY only)
# -----------------------------------------------------------------------------#
def create_user_data_listen_key() -> str:
    data = _req_api_key_only("POST", "/fapi/v1/listenKey")
    if not isinstance(data, dict):
        raise RuntimeError("POST /fapi/v1/listenKey -> unexpected response shape")
    lk = data.get("listenKey")
    if not isinstance(lk, str) or not lk.strip():
        raise RuntimeError("POST /fapi/v1/listenKey -> listenKey missing/empty (STRICT)")
    return lk.strip()


def keepalive_user_data_listen_key(listen_key: str) -> None:
    lk = str(listen_key).strip()
    if not lk:
        raise ValueError("listen_key is empty")
    data = _req_api_key_only("PUT", "/fapi/v1/listenKey", {"listenKey": lk})
    if not isinstance(data, dict):
        raise RuntimeError("PUT /fapi/v1/listenKey -> unexpected response shape")
    if "listenKey" in data:
        lk2 = data.get("listenKey")
        if not isinstance(lk2, str) or not lk2.strip():
            raise RuntimeError("PUT /fapi/v1/listenKey -> invalid listenKey in response (STRICT)")


def close_user_data_listen_key(listen_key: str) -> None:
    lk = str(listen_key).strip()
    if not lk:
        raise ValueError("listen_key is empty")
    data = _req_api_key_only("DELETE", "/fapi/v1/listenKey", {"listenKey": lk})
    if not isinstance(data, dict):
        raise RuntimeError("DELETE /fapi/v1/listenKey -> unexpected response shape")


# -----------------------------------------------------------------------------#
# Public endpoints (exchange / microstructure)
# -----------------------------------------------------------------------------#
def get_exchange_info(symbol: Optional[str] = None) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    if symbol is not None:
        params["symbol"] = _normalize_symbol(symbol)

    data = req("GET", "/fapi/v1/exchangeInfo", params or None, private=False)
    if not isinstance(data, dict):
        raise RuntimeError("GET /fapi/v1/exchangeInfo -> unexpected response shape")
    return data


def get_premium_index(symbol: str) -> Dict[str, Any]:
    s = _normalize_symbol(symbol)
    data = req("GET", "/fapi/v1/premiumIndex", {"symbol": s}, private=False)
    if not isinstance(data, dict) or not data:
        raise RuntimeError("GET /fapi/v1/premiumIndex -> unexpected response shape")
    return data


def get_current_funding_rate(symbol: str) -> float:
    d = get_premium_index(symbol)
    if "lastFundingRate" not in d:
        raise RuntimeError("premiumIndex.lastFundingRate missing")
    return _require_float("lastFundingRate", d["lastFundingRate"])


def get_funding_rate_history(
    symbol: str,
    *,
    start_time_ms: Optional[int] = None,
    end_time_ms: Optional[int] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    s = _normalize_symbol(symbol)
    if not isinstance(limit, int) or limit <= 0 or limit > 1000:
        raise ValueError("limit must be int in [1, 1000]")
    _validate_ms_range(start_time_ms, end_time_ms)

    params: Dict[str, Any] = {"symbol": s, "limit": limit}
    if start_time_ms is not None:
        params["startTime"] = start_time_ms
    if end_time_ms is not None:
        params["endTime"] = end_time_ms

    data = req("GET", "/fapi/v1/fundingRate", params, private=False)
    if not isinstance(data, list):
        raise RuntimeError("GET /fapi/v1/fundingRate -> unexpected response shape")
    out: List[Dict[str, Any]] = []
    for i, row in enumerate(data):
        if not isinstance(row, dict):
            raise RuntimeError(f"fundingRate[{i}] is not dict (STRICT)")
        out.append(row)
    return out


def get_open_interest(symbol: str) -> float:
    s = _normalize_symbol(symbol)
    data = req("GET", "/fapi/v1/openInterest", {"symbol": s}, private=False)
    if not isinstance(data, dict) or "openInterest" not in data:
        raise RuntimeError("GET /fapi/v1/openInterest -> unexpected response shape")
    return _require_float("openInterest", data["openInterest"])


def get_open_interest_hist(
    symbol: str,
    *,
    period: str,
    start_time_ms: Optional[int] = None,
    end_time_ms: Optional[int] = None,
    limit: int = 30,
) -> List[Dict[str, Any]]:
    s = _normalize_symbol(symbol)
    p = _validate_period(period)
    if not isinstance(limit, int) or limit <= 0 or limit > 500:
        raise ValueError("limit must be int in [1, 500]")
    _validate_ms_range(start_time_ms, end_time_ms)

    params: Dict[str, Any] = {"symbol": s, "period": p, "limit": limit}
    if start_time_ms is not None:
        params["startTime"] = start_time_ms
    if end_time_ms is not None:
        params["endTime"] = end_time_ms

    data = req("GET", "/futures/data/openInterestHist", params, private=False)
    if not isinstance(data, list):
        raise RuntimeError("GET /futures/data/openInterestHist -> unexpected response shape")
    out: List[Dict[str, Any]] = []
    for i, row in enumerate(data):
        if not isinstance(row, dict):
            raise RuntimeError(f"openInterestHist[{i}] is not dict (STRICT)")
        out.append(row)
    return out


def get_global_long_short_account_ratio(
    symbol: str,
    *,
    period: str,
    start_time_ms: Optional[int] = None,
    end_time_ms: Optional[int] = None,
    limit: int = 30,
) -> List[Dict[str, Any]]:
    s = _normalize_symbol(symbol)
    p = _validate_period(period)
    if not isinstance(limit, int) or limit <= 0 or limit > 500:
        raise ValueError("limit must be int in [1, 500]")
    _validate_ms_range(start_time_ms, end_time_ms)

    params: Dict[str, Any] = {"symbol": s, "period": p, "limit": limit}
    if start_time_ms is not None:
        params["startTime"] = start_time_ms
    if end_time_ms is not None:
        params["endTime"] = end_time_ms

    data = req("GET", "/futures/data/globalLongShortAccountRatio", params, private=False)
    if not isinstance(data, list):
        raise RuntimeError("GET /futures/data/globalLongShortAccountRatio -> unexpected response shape")
    out: List[Dict[str, Any]] = []
    for i, row in enumerate(data):
        if not isinstance(row, dict):
            raise RuntimeError(f"globalLongShortAccountRatio[{i}] is not dict (STRICT)")
        out.append(row)
    return out


def get_top_long_short_account_ratio(
    symbol: str,
    *,
    period: str,
    start_time_ms: Optional[int] = None,
    end_time_ms: Optional[int] = None,
    limit: int = 30,
) -> List[Dict[str, Any]]:
    s = _normalize_symbol(symbol)
    p = _validate_period(period)
    if not isinstance(limit, int) or limit <= 0 or limit > 500:
        raise ValueError("limit must be int in [1, 500]")
    _validate_ms_range(start_time_ms, end_time_ms)

    params: Dict[str, Any] = {"symbol": s, "period": p, "limit": limit}
    if start_time_ms is not None:
        params["startTime"] = start_time_ms
    if end_time_ms is not None:
        params["endTime"] = end_time_ms

    data = req("GET", "/futures/data/topLongShortAccountRatio", params, private=False)
    if not isinstance(data, list):
        raise RuntimeError("GET /futures/data/topLongShortAccountRatio -> unexpected response shape")
    out: List[Dict[str, Any]] = []
    for i, row in enumerate(data):
        if not isinstance(row, dict):
            raise RuntimeError(f"topLongShortAccountRatio[{i}] is not dict (STRICT)")
        out.append(row)
    return out


def get_top_long_short_position_ratio(
    symbol: str,
    *,
    period: str,
    start_time_ms: Optional[int] = None,
    end_time_ms: Optional[int] = None,
    limit: int = 30,
) -> List[Dict[str, Any]]:
    s = _normalize_symbol(symbol)
    p = _validate_period(period)
    if not isinstance(limit, int) or limit <= 0 or limit > 500:
        raise ValueError("limit must be int in [1, 500]")
    _validate_ms_range(start_time_ms, end_time_ms)

    params: Dict[str, Any] = {"symbol": s, "period": p, "limit": limit}
    if start_time_ms is not None:
        params["startTime"] = start_time_ms
    if end_time_ms is not None:
        params["endTime"] = end_time_ms

    data = req("GET", "/futures/data/topLongShortPositionRatio", params, private=False)
    if not isinstance(data, list):
        raise RuntimeError("GET /futures/data/topLongShortPositionRatio -> unexpected response shape")
    out: List[Dict[str, Any]] = []
    for i, row in enumerate(data):
        if not isinstance(row, dict):
            raise RuntimeError(f"topLongShortPositionRatio[{i}] is not dict (STRICT)")
        out.append(row)
    return out


# -----------------------------------------------------------------------------#
# Private endpoints (account/positions/orders/settings)
# -----------------------------------------------------------------------------#
def get_balance() -> List[Dict[str, Any]]:
    data = req("GET", "/fapi/v2/balance", {}, private=True)
    if not isinstance(data, list):
        raise RuntimeError("GET /fapi/v2/balance -> unexpected response shape")
    out: List[Dict[str, Any]] = []
    for i, row in enumerate(data):
        if not isinstance(row, dict):
            raise RuntimeError(f"balance[{i}] is not dict (STRICT)")
        out.append(row)
    return out


def get_balance_detail(asset: str = "USDT") -> Dict[str, Any]:
    asset_u = str(asset).upper().strip()
    if not asset_u:
        raise ValueError("asset is empty")

    rows = get_balance()
    for row in rows:
        if str(row.get("asset", "")).upper() == asset_u:
            return row
    raise RuntimeError(f"balance row not found for asset={asset_u}")


def get_available_usdt() -> float:
    row = get_balance_detail("USDT")
    if "availableBalance" not in row:
        raise RuntimeError("USDT availableBalance missing")
    return _require_float("availableBalance", row["availableBalance"])


def _fetch_position_risk_raw(symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if symbol is not None:
        params["symbol"] = _normalize_symbol(symbol)

    data = req("GET", "/fapi/v2/positionRisk", params or None, private=True)
    if isinstance(data, dict) and "raw" in data:
        raw = data.get("raw")
        if not isinstance(raw, list):
            raise RuntimeError("positionRisk.raw is not list (STRICT)")
        data = raw

    if not isinstance(data, list):
        raise RuntimeError("GET /fapi/v2/positionRisk -> unexpected response shape")

    out: List[Dict[str, Any]] = []
    for i, row in enumerate(data):
        if not isinstance(row, dict):
            raise RuntimeError(f"positionRisk[{i}] is not dict (STRICT)")
        out.append(row)
    return out


def fetch_open_positions(symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    rows = _fetch_position_risk_raw(symbol)
    open_rows: List[Dict[str, Any]] = []
    for row in rows:
        if "positionAmt" not in row:
            raise RuntimeError("positionAmt missing in positionRisk row (STRICT)")
        amt = _require_float("positionAmt", row["positionAmt"])
        if amt != 0.0:
            open_rows.append(row)
    return open_rows


def _select_position_row_one_way_strict(rows: List[Dict[str, Any]], symbol: str) -> Dict[str, Any]:
    sym = _normalize_symbol(symbol)
    sym_rows: List[Dict[str, Any]] = []

    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            raise RuntimeError(f"positionRisk[{i}] is not dict (STRICT)")
        row_sym = _normalize_symbol(str(row.get("symbol", "")))
        if row_sym == sym:
            sym_rows.append(row)

    if not sym_rows:
        raise RuntimeError(f"position not found for symbol={sym}")

    live_rows: List[Dict[str, Any]] = []
    dirs: set[str] = set()
    pos_sides: set[str] = set()

    for row in sym_rows:
        if "positionAmt" not in row:
            raise RuntimeError("positionRisk.positionAmt is missing (STRICT)")
        amt = _require_float("positionRisk.positionAmt", row["positionAmt"])
        ps = _normalize_position_side(row.get("positionSide"))
        pos_sides.add(ps)

        if amt != 0.0:
            live_rows.append(row)
            row_dir = ps if ps in {"LONG", "SHORT"} else ("LONG" if amt > 0 else "SHORT")
            dirs.add(row_dir)

    if live_rows:
        if len(dirs) != 1:
            raise RuntimeError(f"ambiguous live position directions (STRICT): {sorted(list(dirs))}")
        if "BOTH" in pos_sides and len(pos_sides) > 1:
            raise RuntimeError(f"ambiguous live positionSide topology (STRICT): {sorted(list(pos_sides))}")
        if len(live_rows) != 1:
            raise RuntimeError(f"multiple live position rows found in one-way mode (STRICT): n={len(live_rows)}")
        return live_rows[0]

    both_zero = [r for r in sym_rows if _normalize_position_side(r.get("positionSide")) == "BOTH"]
    if len(both_zero) == 1:
        return both_zero[0]
    if len(sym_rows) == 1:
        return sym_rows[0]

    raise RuntimeError(f"ambiguous zero-position rows for symbol={sym} (STRICT): n={len(sym_rows)}")


def get_position(symbol: str) -> Dict[str, Any]:
    s = _normalize_symbol(symbol)
    rows = _fetch_position_risk_raw(s)
    return _select_position_row_one_way_strict(rows, s)


def get_open_orders(symbol: str) -> List[Dict[str, Any]]:
    s = _normalize_symbol(symbol)
    data = req("GET", "/fapi/v1/openOrders", {"symbol": s}, private=True)
    if not isinstance(data, list):
        raise RuntimeError("GET /fapi/v1/openOrders -> unexpected response shape")
    out: List[Dict[str, Any]] = []
    for i, row in enumerate(data):
        if not isinstance(row, dict):
            raise RuntimeError(f"openOrders[{i}] is not dict (STRICT)")
        out.append(row)
    return out


def fetch_open_orders(symbol: str) -> List[Dict[str, Any]]:
    return get_open_orders(symbol)


def get_order(symbol: str, order_id: int | str) -> Dict[str, Any]:
    s = _normalize_symbol(symbol)
    oid = _require_int("order_id", order_id)

    data = req("GET", "/fapi/v1/order", {"symbol": s, "orderId": oid}, private=True)
    if not isinstance(data, dict):
        raise RuntimeError("GET /fapi/v1/order -> unexpected response shape")
    return data


def get_order_by_client_id(symbol: str, client_order_id: str) -> Dict[str, Any]:
    s = _normalize_symbol(symbol)
    cid = str(client_order_id).strip()
    if not cid:
        raise ValueError("client_order_id is empty")

    data = req("GET", "/fapi/v1/order", {"symbol": s, "origClientOrderId": cid}, private=True)
    if not isinstance(data, dict):
        raise RuntimeError("GET /fapi/v1/order(by client id) -> unexpected response shape")
    return data


def get_user_trades(
    symbol: str,
    *,
    order_id: Optional[int | str] = None,
    start_time_ms: Optional[int] = None,
    end_time_ms: Optional[int] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    s = _normalize_symbol(symbol)
    if not isinstance(limit, int) or limit <= 0 or limit > 1000:
        raise ValueError("limit must be int in [1, 1000]")

    params: Dict[str, Any] = {"symbol": s, "limit": limit}

    if order_id is not None:
        params["orderId"] = _require_int("order_id", order_id)

    _validate_ms_range(start_time_ms, end_time_ms)
    if start_time_ms is not None:
        params["startTime"] = start_time_ms
    if end_time_ms is not None:
        params["endTime"] = end_time_ms

    data = req("GET", "/fapi/v1/userTrades", params, private=True)
    if not isinstance(data, list):
        raise RuntimeError("GET /fapi/v1/userTrades -> unexpected response shape")
    out: List[Dict[str, Any]] = []
    for i, row in enumerate(data):
        if not isinstance(row, dict):
            raise RuntimeError(f"userTrades[{i}] is not dict (STRICT)")
        out.append(row)
    return out


def cancel_order(symbol: str, order_id: int | str) -> Dict[str, Any]:
    s = _normalize_symbol(symbol)
    oid = _require_int("order_id", order_id)

    data = req("DELETE", "/fapi/v1/order", {"symbol": s, "orderId": oid}, private=True)
    if not isinstance(data, dict):
        raise RuntimeError("DELETE /fapi/v1/order -> unexpected response shape")
    return data


def set_leverage(symbol: str, leverage: int) -> Dict[str, Any]:
    s = _normalize_symbol(symbol)
    if not isinstance(leverage, int) or leverage <= 0:
        raise ValueError("leverage must be positive int")

    data = req("POST", "/fapi/v1/leverage", {"symbol": s, "leverage": leverage}, private=True)
    if not isinstance(data, dict):
        raise RuntimeError("POST /fapi/v1/leverage -> unexpected response shape")
    return data


def set_margin_mode(symbol: str, mode: str) -> Dict[str, Any]:
    s = _normalize_symbol(symbol)
    mode_u = str(mode).upper().strip()
    if mode_u in {"CROSS", "CROSSED"}:
        mode_u = "CROSSED"
    if mode_u not in {"ISOLATED", "CROSSED"}:
        raise ValueError("mode must be 'ISOLATED' or 'CROSSED'")

    data = req("POST", "/fapi/v1/marginType", {"symbol": s, "marginType": mode_u}, private=True)
    if not isinstance(data, dict):
        raise RuntimeError("POST /fapi/v1/marginType -> unexpected response shape")
    return data


def set_leverage_and_mode(symbol: str, leverage: int, isolated: bool = True) -> None:
    if not isinstance(leverage, int) or leverage <= 0:
        raise ValueError("leverage must be positive int")

    s = _normalize_symbol(symbol)
    current = get_position(s)

    if "leverage" not in current:
        raise RuntimeError("positionRisk.leverage is missing")
    current_leverage = _require_int("positionRisk.leverage", current["leverage"])
    current_margin = _normalize_margin_type(current.get("marginType"))

    target_leverage = leverage
    target_margin = "ISOLATED" if isolated else "CROSSED"

    if current_leverage != target_leverage:
        set_leverage(s, target_leverage)
    else:
        logger.info("leverage already set: %s", target_leverage)

    if current_margin != target_margin:
        set_margin_mode(s, target_margin)
    else:
        logger.info("marginType already set: %s", target_margin)


__all__ = [
    "req",
    "sync_server_time",
    "sync_server_time_offset",
    "assert_one_way_mode",
    "create_user_data_listen_key",
    "keepalive_user_data_listen_key",
    "close_user_data_listen_key",
    "get_exchange_info",
    "get_premium_index",
    "get_current_funding_rate",
    "get_funding_rate_history",
    "get_open_interest",
    "get_open_interest_hist",
    "get_global_long_short_account_ratio",
    "get_top_long_short_account_ratio",
    "get_top_long_short_position_ratio",
    "get_balance",
    "get_balance_detail",
    "get_available_usdt",
    "fetch_open_positions",
    "get_position",
    "get_open_orders",
    "fetch_open_orders",
    "get_order",
    "get_order_by_client_id",
    "get_user_trades",
    "cancel_order",
    "set_leverage",
    "set_margin_mode",
    "set_leverage_and_mode",
]