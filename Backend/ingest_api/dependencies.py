"""
backend/ingest_api/dependencies.py

Injectable FastAPI dependency factories for Redis, PostgreSQL, and ClickHouse.

Singletons are set on app startup via set_*() helpers.
Tests override get_* with app.dependency_overrides.
"""

from typing import Any, Optional

_redis: Optional[Any] = None
_db_pool: Optional[Any] = None
_ch_client: Optional[Any] = None


def set_redis(client: Any) -> None:
    global _redis
    _redis = client


def set_db_pool(pool: Any) -> None:
    global _db_pool
    _db_pool = pool


def set_clickhouse_client(client: Any) -> None:
    global _ch_client
    _ch_client = client


async def get_redis() -> Any:
    if _redis is None:
        raise RuntimeError("Redis not initialized — check startup logs")
    return _redis


async def get_db_pool() -> Any:
    if _db_pool is None:
        raise RuntimeError("PostgreSQL pool not initialized — check startup logs")
    return _db_pool


async def get_clickhouse_client() -> Any:
    if _ch_client is None:
        raise RuntimeError("ClickHouse client not initialized — check startup logs")
    return _ch_client
