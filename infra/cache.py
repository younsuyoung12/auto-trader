"""
========================================================
FILE: infra/cache.py
STRICT · NO-FALLBACK · TRADE-GRADE MODE
========================================================

역할
- Redis 기반 대시보드/조회 캐시 레이어를 제공한다.
- 캐시 키 생성, JSON 직렬화 저장/조회, 삭제, ping 기능을 담당한다.
- 환경변수 직접 접근 없이 settings(SSOT) 또는 명시적 config로만 초기화한다.

절대 원칙 (STRICT · NO-FALLBACK)
- 환경변수 직접 접근 금지.
- Redis 설정 누락/오염/비정상 타입이면 즉시 예외.
- 초기화 전 사용 시 즉시 예외.
- JSON 직렬화 불가 데이터는 즉시 예외.
- Redis 연결/명령 실패는 즉시 예외.
- 조용한 fallback 금지.

변경 이력
--------------------------------------------------------
- 2026-03-07:
  1) 신규 생성: Redis 캐시 공통 모듈 추가
  2) settings(SSOT) 기반 초기화 지원
  3) JSON cache get/set/delete/ping 기능 추가
========================================================
"""

from __future__ import annotations

import json
import math
import threading
from dataclasses import dataclass
from typing import Any, Optional

from redis import Redis
from redis.exceptions import RedisError


@dataclass(frozen=True, slots=True)
class RedisCacheConfig:
    redis_url: str
    key_prefix: str
    default_ttl_sec: int
    socket_timeout_sec: float
    socket_connect_timeout_sec: float
    health_check_interval_sec: int


_LOCK = threading.Lock()
_CLIENT: Optional[Redis] = None
_CONFIG: Optional[RedisCacheConfig] = None


def _require_nonempty_str(value: Any, name: str) -> str:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if not isinstance(value, str):
        raise RuntimeError(f"{name} must be str (STRICT), got={type(value).__name__}")
    s = value.strip()
    if not s:
        raise RuntimeError(f"{name} must not be empty (STRICT)")
    return s


def _require_positive_int(value: Any, name: str) -> int:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if isinstance(value, bool):
        raise RuntimeError(f"{name} must be int, bool not allowed (STRICT)")
    try:
        iv = int(value)
    except Exception as exc:
        raise RuntimeError(f"{name} must be int (STRICT): {exc}") from exc
    if iv <= 0:
        raise RuntimeError(f"{name} must be > 0 (STRICT)")
    return iv


def _require_positive_float(value: Any, name: str) -> float:
    if value is None:
        raise RuntimeError(f"{name} is required (STRICT)")
    if isinstance(value, bool):
        raise RuntimeError(f"{name} must be float, bool not allowed (STRICT)")
    try:
        fv = float(value)
    except Exception as exc:
        raise RuntimeError(f"{name} must be float (STRICT): {exc}") from exc
    if not math.isfinite(fv):
        raise RuntimeError(f"{name} must be finite (STRICT)")
    if fv <= 0.0:
        raise RuntimeError(f"{name} must be > 0 (STRICT)")
    return fv


def build_cache_config_from_settings(settings: Any) -> RedisCacheConfig:
    """
    settings(SSOT)로부터 RedisCacheConfig를 구성한다.

    필수 settings 필드:
    - redis_url
    - dashboard_cache_prefix
    - dashboard_cache_ttl_sec
    - redis_socket_timeout_sec
    - redis_socket_connect_timeout_sec
    - redis_health_check_interval_sec
    """
    if settings is None:
        raise RuntimeError("settings is required (STRICT)")

    return RedisCacheConfig(
        redis_url=_require_nonempty_str(getattr(settings, "redis_url", None), "settings.redis_url"),
        key_prefix=_require_nonempty_str(
            getattr(settings, "dashboard_cache_prefix", None),
            "settings.dashboard_cache_prefix",
        ),
        default_ttl_sec=_require_positive_int(
            getattr(settings, "dashboard_cache_ttl_sec", None),
            "settings.dashboard_cache_ttl_sec",
        ),
        socket_timeout_sec=_require_positive_float(
            getattr(settings, "redis_socket_timeout_sec", None),
            "settings.redis_socket_timeout_sec",
        ),
        socket_connect_timeout_sec=_require_positive_float(
            getattr(settings, "redis_socket_connect_timeout_sec", None),
            "settings.redis_socket_connect_timeout_sec",
        ),
        health_check_interval_sec=_require_positive_int(
            getattr(settings, "redis_health_check_interval_sec", None),
            "settings.redis_health_check_interval_sec",
        ),
    )


def init_cache(*, config: RedisCacheConfig) -> None:
    """
    STRICT:
    - config 누락/오염 시 즉시 예외
    - Redis ping 실패 시 즉시 예외
    """
    global _CLIENT, _CONFIG

    if not isinstance(config, RedisCacheConfig):
        raise RuntimeError("config must be RedisCacheConfig (STRICT)")

    client = Redis.from_url(
        config.redis_url,
        decode_responses=True,
        socket_timeout=float(config.socket_timeout_sec),
        socket_connect_timeout=float(config.socket_connect_timeout_sec),
        health_check_interval=int(config.health_check_interval_sec),
    )

    try:
        pong = client.ping()
    except RedisError as exc:
        raise RuntimeError(f"Redis ping failed (STRICT): {exc}") from exc

    if pong is not True:
        raise RuntimeError("Redis ping returned non-true response (STRICT)")

    with _LOCK:
        _CLIENT = client
        _CONFIG = config


def init_cache_from_settings(settings: Any) -> None:
    init_cache(config=build_cache_config_from_settings(settings))


def is_cache_initialized() -> bool:
    return _CLIENT is not None and _CONFIG is not None


def _require_initialized() -> tuple[Redis, RedisCacheConfig]:
    client = _CLIENT
    config = _CONFIG
    if client is None or config is None:
        raise RuntimeError("Redis cache is not initialized (STRICT)")
    return client, config


def make_cache_key(*parts: Any) -> str:
    """
    예:
    make_cache_key("performance", "summary")
    -> "<prefix>:performance:summary"
    """
    _, config = _require_initialized()

    normalized_parts: list[str] = []
    for idx, part in enumerate(parts):
        if part is None:
            raise RuntimeError(f"cache key part[{idx}] is required (STRICT)")
        s = str(part).strip()
        if not s:
            raise RuntimeError(f"cache key part[{idx}] must not be empty (STRICT)")
        if ":" in s:
            raise RuntimeError(f"cache key part[{idx}] must not contain ':' (STRICT)")
        normalized_parts.append(s)

    if not normalized_parts:
        raise RuntimeError("cache key parts must not be empty (STRICT)")

    return f"{config.key_prefix}:{':'.join(normalized_parts)}"


def ping_cache() -> bool:
    client, _ = _require_initialized()
    try:
        pong = client.ping()
    except RedisError as exc:
        raise RuntimeError(f"Redis ping failed (STRICT): {exc}") from exc
    if pong is not True:
        raise RuntimeError("Redis ping returned non-true response (STRICT)")
    return True


def get_cache_json(key: str) -> Optional[Any]:
    """
    캐시 miss면 None 반환.
    miss는 정상 상태이므로 예외가 아니다.
    """
    client, _ = _require_initialized()
    key_s = _require_nonempty_str(key, "key")

    try:
        raw = client.get(key_s)
    except RedisError as exc:
        raise RuntimeError(f"Redis GET failed (STRICT): {exc}") from exc

    if raw is None:
        return None

    if not isinstance(raw, str) or not raw.strip():
        raise RuntimeError("Redis GET returned blank payload (STRICT)")

    try:
        return json.loads(raw)
    except Exception as exc:
        raise RuntimeError(f"cached JSON decode failed (STRICT): {exc}") from exc


def set_cache_json(key: str, data: Any, *, ttl_sec: Optional[int] = None) -> None:
    client, config = _require_initialized()
    key_s = _require_nonempty_str(key, "key")

    ttl = config.default_ttl_sec if ttl_sec is None else _require_positive_int(ttl_sec, "ttl_sec")

    try:
        payload = json.dumps(
            data,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except Exception as exc:
        raise RuntimeError(f"cache JSON serialization failed (STRICT): {exc}") from exc

    if not isinstance(payload, str) or not payload:
        raise RuntimeError("cache payload must not be empty (STRICT)")

    try:
        ok = client.setex(key_s, ttl, payload)
    except RedisError as exc:
        raise RuntimeError(f"Redis SETEX failed (STRICT): {exc}") from exc

    if ok is not True:
        raise RuntimeError("Redis SETEX returned non-true response (STRICT)")


def delete_cache_key(key: str) -> int:
    client, _ = _require_initialized()
    key_s = _require_nonempty_str(key, "key")

    try:
        deleted = client.delete(key_s)
    except RedisError as exc:
        raise RuntimeError(f"Redis DELETE failed (STRICT): {exc}") from exc

    if isinstance(deleted, bool):
        raise RuntimeError("Redis DELETE returned bool (STRICT)")
    try:
        count = int(deleted)
    except Exception as exc:
        raise RuntimeError(f"Redis DELETE result invalid (STRICT): {exc}") from exc
    if count < 0:
        raise RuntimeError("Redis DELETE result must be >= 0 (STRICT)")
    return count


def delete_cache_prefix(prefix_suffix: str) -> int:
    """
    예:
    prefix_suffix="performance"
    -> "<key_prefix>:performance:*" 삭제
    """
    client, config = _require_initialized()
    suffix = _require_nonempty_str(prefix_suffix, "prefix_suffix")
    if ":" in suffix:
        raise RuntimeError("prefix_suffix must not contain ':' (STRICT)")

    pattern = f"{config.key_prefix}:{suffix}:*"
    deleted_total = 0

    try:
        keys = list(client.scan_iter(match=pattern))
    except RedisError as exc:
        raise RuntimeError(f"Redis SCAN failed (STRICT): {exc}") from exc

    if not keys:
        return 0

    try:
        deleted = client.delete(*keys)
    except RedisError as exc:
        raise RuntimeError(f"Redis bulk DELETE failed (STRICT): {exc}") from exc

    if isinstance(deleted, bool):
        raise RuntimeError("Redis bulk DELETE returned bool (STRICT)")
    try:
        deleted_total = int(deleted)
    except Exception as exc:
        raise RuntimeError(f"Redis bulk DELETE result invalid (STRICT): {exc}") from exc
    if deleted_total < 0:
        raise RuntimeError("Redis bulk DELETE result must be >= 0 (STRICT)")
    return deleted_total


__all__ = [
    "RedisCacheConfig",
    "build_cache_config_from_settings",
    "init_cache",
    "init_cache_from_settings",
    "is_cache_initialized",
    "make_cache_key",
    "ping_cache",
    "get_cache_json",
    "set_cache_json",
    "delete_cache_key",
    "delete_cache_prefix",
]