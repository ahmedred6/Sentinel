"""
Tests for backend/ingest_api/auth.py

Covers acceptance criteria for INGEST-02:
  - Valid API key resolves to customer_id → 200
  - Invalid key → 401
  - Missing Sentinel-API-Key header → 401
  - Rate limit breach → 429 with Retry-After header
  - Cache hit: DB is not queried
  - Cache miss: DB is queried, result cached in Redis
  - Key hash (not plaintext) passed to DB query
"""

import time
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from auth import hash_key
from dependencies import get_db_pool, get_redis
from main import app, _reset_dedup

client = TestClient(app)

VALID_TRACE = {
    "customer_id": "cust_test",
    "session_id": "sess_001",
    "flow_name": "support",
    "model_name": "gpt-4",
    "user_prompt": "Hello",
    "llm_response": "Hi there!",
    "latency_ms": 100,
    "token_counts": {"prompt": 10, "completion": 5, "total": 15},
}

TEST_API_KEY = "sk_live_test_key_abc123"
TEST_CUSTOMER = "cust_acme_001"


# ---------------------------------------------------------------------------
# Mock factories
# ---------------------------------------------------------------------------

def make_redis_mock(
    customer_id: Optional[str] = TEST_CUSTOMER,
    from_cache: bool = True,
    rate_count: int = 0,
) -> Any:
    """
    Build a mock Redis client wired for a specific scenario.

    from_cache=True  → redis.get() returns customer_id (skips DB)
    from_cache=False → redis.get() returns None (forces DB lookup)
    rate_count       → current number of requests in the sliding window
    """
    mock = MagicMock()

    # Auth cache lookup
    if from_cache and customer_id:
        mock.get = AsyncMock(return_value=customer_id.encode())
    else:
        mock.get = AsyncMock(return_value=None)

    mock.setex = AsyncMock()   # cache write-back
    mock.zadd = AsyncMock()    # rate limit window entry
    mock.expire = AsyncMock()  # TTL refresh

    # Rate limit zrange — only called when rate_count >= limit
    if rate_count >= 1_000:
        oldest_ts = time.time() - 30.0  # oldest entry is 30s into the window
        mock.zrange = AsyncMock(return_value=[(b"entry", oldest_ts)])
    else:
        mock.zrange = AsyncMock(return_value=[])

    # Pipeline (sync call returns pipeline; execute() is async)
    pipe = MagicMock()
    pipe.zremrangebyscore = MagicMock()
    pipe.zcard = MagicMock()
    pipe.execute = AsyncMock(return_value=[None, rate_count])
    mock.pipeline = MagicMock(return_value=pipe)

    # Stream enqueue (called by route after auth passes)
    mock.xadd = AsyncMock(return_value=b"12345-0")

    return mock


def make_db_mock(customer_id: Optional[str] = TEST_CUSTOMER) -> Any:
    mock = AsyncMock()
    if customer_id:
        mock.fetchrow = AsyncMock(return_value={"customer_id": customer_id})
    else:
        mock.fetchrow = AsyncMock(return_value=None)
    return mock


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def reset_dedup():
    _reset_dedup()
    yield
    _reset_dedup()


def _inject(redis_mock: Any, db_mock: Any) -> None:
    """Wire mock clients into the app dependency graph."""
    async def get_r() -> Any:
        return redis_mock

    async def get_d() -> Any:
        return db_mock

    app.dependency_overrides[get_redis] = get_r
    app.dependency_overrides[get_db_pool] = get_d


def _clear() -> None:
    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Missing / malformed header → 401
# ---------------------------------------------------------------------------

def test_missing_api_key_header_returns_401():
    _inject(make_redis_mock(), make_db_mock())
    try:
        r = client.post("/ingest/trace", json=VALID_TRACE)
        assert r.status_code == 401
    finally:
        _clear()


def test_empty_api_key_header_returns_401():
    _inject(make_redis_mock(), make_db_mock())
    try:
        r = client.post("/ingest/trace", json=VALID_TRACE, headers={"Sentinel-API-Key": ""})
        assert r.status_code == 401
    finally:
        _clear()


# ---------------------------------------------------------------------------
# Invalid key (not in DB) → 401
# ---------------------------------------------------------------------------

def test_unknown_api_key_returns_401():
    # Cache miss + DB miss = unknown key
    redis_mock = make_redis_mock(customer_id=None, from_cache=False)
    db_mock = make_db_mock(customer_id=None)
    _inject(redis_mock, db_mock)
    try:
        r = client.post(
            "/ingest/trace",
            json=VALID_TRACE,
            headers={"Sentinel-API-Key": "sk_bad_key"},
        )
        assert r.status_code == 401
    finally:
        _clear()


# ---------------------------------------------------------------------------
# Valid key, cache hit — DB is NOT queried
# ---------------------------------------------------------------------------

def test_valid_key_cache_hit_returns_200():
    redis_mock = make_redis_mock(customer_id=TEST_CUSTOMER, from_cache=True, rate_count=0)
    db_mock = make_db_mock(TEST_CUSTOMER)
    _inject(redis_mock, db_mock)
    try:
        r = client.post(
            "/ingest/trace",
            json=VALID_TRACE,
            headers={"Sentinel-API-Key": TEST_API_KEY},
        )
        assert r.status_code == 200
        assert r.json()["status"] == "ok"
        # DB must NOT be touched on cache hit
        db_mock.fetchrow.assert_not_called()
    finally:
        _clear()


# ---------------------------------------------------------------------------
# Valid key, cache miss — DB IS queried, hash (not plaintext) is sent
# ---------------------------------------------------------------------------

def test_valid_key_cache_miss_queries_db():
    redis_mock = make_redis_mock(customer_id=TEST_CUSTOMER, from_cache=False, rate_count=0)
    db_mock = make_db_mock(TEST_CUSTOMER)
    _inject(redis_mock, db_mock)
    try:
        r = client.post(
            "/ingest/trace",
            json=VALID_TRACE,
            headers={"Sentinel-API-Key": TEST_API_KEY},
        )
        assert r.status_code == 200
        # DB must be queried with the HASH, never the plaintext key
        expected_hash = hash_key(TEST_API_KEY)
        db_mock.fetchrow.assert_called_once_with(
            "SELECT customer_id FROM api_keys WHERE key_hash = $1 AND is_active = TRUE",
            expected_hash,
        )
    finally:
        _clear()


def test_cache_miss_writes_customer_id_to_redis():
    redis_mock = make_redis_mock(customer_id=TEST_CUSTOMER, from_cache=False, rate_count=0)
    db_mock = make_db_mock(TEST_CUSTOMER)
    _inject(redis_mock, db_mock)
    try:
        client.post(
            "/ingest/trace",
            json=VALID_TRACE,
            headers={"Sentinel-API-Key": TEST_API_KEY},
        )
        # Result should be written back to Redis with 5-minute TTL
        expected_hash = hash_key(TEST_API_KEY)
        redis_mock.setex.assert_called_once_with(
            f"auth:{expected_hash}", 300, TEST_CUSTOMER
        )
    finally:
        _clear()


def test_key_hash_never_stored_as_plaintext():
    """Verify that neither the cache key nor the DB arg contains the raw API key."""
    redis_mock = make_redis_mock(customer_id=TEST_CUSTOMER, from_cache=False, rate_count=0)
    db_mock = make_db_mock(TEST_CUSTOMER)
    _inject(redis_mock, db_mock)
    try:
        client.post(
            "/ingest/trace",
            json=VALID_TRACE,
            headers={"Sentinel-API-Key": TEST_API_KEY},
        )
        # Redis cache key must not contain the raw key
        cache_key_arg = redis_mock.get.call_args[0][0]
        assert TEST_API_KEY not in cache_key_arg

        # DB query arg must not contain the raw key
        db_call_args = db_mock.fetchrow.call_args[0]
        assert TEST_API_KEY not in str(db_call_args)
    finally:
        _clear()


# ---------------------------------------------------------------------------
# Rate limit — 1,000 req/min per key
# ---------------------------------------------------------------------------

def test_rate_limit_breach_returns_429():
    redis_mock = make_redis_mock(customer_id=TEST_CUSTOMER, from_cache=True, rate_count=1_000)
    db_mock = make_db_mock(TEST_CUSTOMER)
    _inject(redis_mock, db_mock)
    try:
        r = client.post(
            "/ingest/trace",
            json=VALID_TRACE,
            headers={"Sentinel-API-Key": TEST_API_KEY},
        )
        assert r.status_code == 429
    finally:
        _clear()


def test_rate_limit_breach_includes_retry_after_header():
    redis_mock = make_redis_mock(customer_id=TEST_CUSTOMER, from_cache=True, rate_count=1_000)
    db_mock = make_db_mock(TEST_CUSTOMER)
    _inject(redis_mock, db_mock)
    try:
        r = client.post(
            "/ingest/trace",
            json=VALID_TRACE,
            headers={"Sentinel-API-Key": TEST_API_KEY},
        )
        assert "retry-after" in r.headers
        retry_after = int(r.headers["retry-after"])
        assert retry_after >= 1
    finally:
        _clear()


def test_rate_limit_breach_detail_message():
    redis_mock = make_redis_mock(customer_id=TEST_CUSTOMER, from_cache=True, rate_count=1_000)
    db_mock = make_db_mock(TEST_CUSTOMER)
    _inject(redis_mock, db_mock)
    try:
        r = client.post(
            "/ingest/trace",
            json=VALID_TRACE,
            headers={"Sentinel-API-Key": TEST_API_KEY},
        )
        assert "rate limit" in r.json()["detail"].lower()
    finally:
        _clear()


def test_at_limit_minus_one_still_passes():
    # 999 requests in window — should still be allowed
    redis_mock = make_redis_mock(customer_id=TEST_CUSTOMER, from_cache=True, rate_count=999)
    db_mock = make_db_mock(TEST_CUSTOMER)
    _inject(redis_mock, db_mock)
    try:
        r = client.post(
            "/ingest/trace",
            json=VALID_TRACE,
            headers={"Sentinel-API-Key": TEST_API_KEY},
        )
        assert r.status_code == 200
    finally:
        _clear()


# ---------------------------------------------------------------------------
# hash_key unit tests
# ---------------------------------------------------------------------------

def test_hash_key_is_deterministic():
    assert hash_key("sk_abc") == hash_key("sk_abc")


def test_hash_key_differs_from_input():
    key = "sk_test_key_123"
    assert hash_key(key) != key


def test_hash_key_length_is_64_chars():
    # SHA-256 hex digest is always 64 characters
    assert len(hash_key("anything")) == 64


def test_different_keys_produce_different_hashes():
    assert hash_key("sk_key_a") != hash_key("sk_key_b")
