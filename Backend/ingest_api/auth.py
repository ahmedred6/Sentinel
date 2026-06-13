"""
backend/ingest_api/auth.py

API key authentication and per-key rate limiting.

Auth flow:
  1. Read Sentinel-API-Key header
  2. SHA-256 hash the key — only the hash ever touches Redis/Postgres
  3. Check Redis cache (auth:{hash}, TTL 5 min) for fast path
  4. Cache miss → query api_keys table in PostgreSQL
  5. Rate limit: sliding window via Redis sorted set (1,000 req/min)
  6. Attach customer_id to request.state for downstream handlers
"""

import hashlib
import time
import uuid
from typing import Any, Optional

from fastapi import Depends, Header, HTTPException, Request

from dependencies import get_db_pool, get_redis

_AUTH_CACHE_TTL = 300     # 5 minutes — key → customer_id cached in Redis
_RATE_LIMIT = 1_000       # max requests per window per key
_RATE_WINDOW = 60.0       # sliding window width in seconds


def hash_key(api_key: str) -> str:
    """SHA-256 of the raw API key. Only this digest is stored or compared."""
    return hashlib.sha256(api_key.encode()).hexdigest()


async def _lookup_customer_id(
    key_hash: str,
    redis: Any,
    db_pool: Any,
) -> Optional[str]:
    # Fast path: Redis cache
    cached = await redis.get(f"auth:{key_hash}")
    if cached is not None:
        return cached.decode() if isinstance(cached, bytes) else str(cached)

    # Slow path: PostgreSQL — only the hash is queried, never plaintext
    row = await db_pool.fetchrow(
        "SELECT customer_id FROM api_keys WHERE key_hash = $1 AND is_active = TRUE",
        key_hash,
    )
    if row is None:
        return None

    customer_id: str = row["customer_id"]

    # Write back to Redis cache to short-circuit future lookups
    await redis.setex(f"auth:{key_hash}", _AUTH_CACHE_TTL, customer_id)
    return customer_id


async def _check_rate_limit(key_hash: str, redis: Any) -> tuple[bool, int]:
    """
    Sliding window via Redis sorted set.
    Returns (allowed, retry_after_seconds).
    retry_after is 0 when allowed.
    """
    now = time.time()
    window_start = now - _RATE_WINDOW
    rk = f"ratelimit:{key_hash}"

    # Evict expired members and count remaining in one round-trip
    pipe = redis.pipeline()
    pipe.zremrangebyscore(rk, 0, window_start)
    pipe.zcard(rk)
    results = await pipe.execute()
    current_count: int = results[1]

    if current_count >= _RATE_LIMIT:
        # Compute how long until the oldest entry leaves the window
        oldest = await redis.zrange(rk, 0, 0, withscores=True)
        if oldest:
            _, oldest_ts = oldest[0]
            retry_after = max(1, int(_RATE_WINDOW - (now - oldest_ts)) + 1)
        else:
            retry_after = 1
        return False, retry_after

    # Record this request in the window
    await redis.zadd(rk, {str(uuid.uuid4()): now})
    await redis.expire(rk, int(_RATE_WINDOW) + 10)  # slight buffer beyond window
    return True, 0


async def require_auth(
    request: Request,
    sentinel_api_key: Optional[str] = Header(None, alias="Sentinel-API-Key"),
    redis: Any = Depends(get_redis),
    db_pool: Any = Depends(get_db_pool),
) -> str:
    """
    FastAPI dependency. Returns customer_id on success; raises 401 or 429 on failure.
    Also sets request.state.customer_id for middleware/logging downstream.
    """
    if not sentinel_api_key:
        raise HTTPException(status_code=401, detail="Sentinel-API-Key header required")

    key_hash = hash_key(sentinel_api_key)

    customer_id = await _lookup_customer_id(key_hash, redis, db_pool)
    if customer_id is None:
        raise HTTPException(status_code=401, detail="Invalid API key")

    allowed, retry_after = await _check_rate_limit(key_hash, redis)
    if not allowed:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded",
            headers={"Retry-After": str(retry_after)},
        )

    request.state.customer_id = customer_id
    return customer_id
