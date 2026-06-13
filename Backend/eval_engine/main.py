#!/usr/bin/env python3
"""
backend/eval_engine/main.py

Entry point for the eval-worker service.

Environment variables (all read from .env or Railway):
    REDIS_URL               redis://localhost:6379
    DATABASE_URL            postgresql://user:pass@host:5432/db
    EVAL_CONSUMER_NAME      eval-worker-1   (unique per replica)

Usage:
    python Backend/eval_engine/main.py
"""

import asyncio
import logging
import os
import sys
from pathlib import Path

# Load .env from repo root
_ROOT = Path(__file__).resolve().parents[2]
_env_file = _ROOT / ".env"
if _env_file.exists():
    for _line in _env_file.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("eval_worker.main")


async def main() -> None:
    import redis.asyncio as aioredis
    import asyncpg

    from worker import run_worker

    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    log.info("Connecting to Redis: %s", redis_url)
    redis_client = aioredis.from_url(redis_url, decode_responses=False, max_connections=5)
    await redis_client.ping()
    log.info("Redis OK")

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        log.error("DATABASE_URL is required")
        sys.exit(1)

    log.info("Connecting to PostgreSQL")
    db_pool = await asyncpg.create_pool(db_url)
    log.info("PostgreSQL OK")

    try:
        await run_worker(redis_client, db_pool)
    finally:
        await redis_client.aclose()
        await db_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
