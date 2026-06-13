#!/usr/bin/env python3
"""
backend/ch_writer/main.py

Entry point for the ClickHouse writer worker service.

Environment variables (all read from .env or Railway env):
    REDIS_URL             redis://localhost:6379
    CLICKHOUSE_HOST       your-cluster.clickhouse.cloud
    CLICKHOUSE_PORT       8443
    CLICKHOUSE_USER       default
    CLICKHOUSE_PASSWORD
    CLICKHOUSE_DB         default
    CH_WRITER_CONSUMER_NAME  ch-writer-1   (unique per replica)

Usage:
    python Backend/ch_writer/main.py
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
log = logging.getLogger("ch_writer.main")


async def main() -> None:
    import redis.asyncio as aioredis
    import clickhouse_connect

    from worker import run_worker

    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    log.info("Connecting to Redis: %s", redis_url)
    redis_client = aioredis.from_url(redis_url, decode_responses=False, max_connections=5)
    await redis_client.ping()
    log.info("Redis OK")

    ch_host = os.environ.get("CLICKHOUSE_HOST", "localhost")
    log.info("Connecting to ClickHouse: %s", ch_host)
    ch_client = clickhouse_connect.get_client(
        host=ch_host,
        port=int(os.environ.get("CLICKHOUSE_PORT", 8443)),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=os.environ.get("CLICKHOUSE_DB", "default"),
        secure=os.environ.get("CLICKHOUSE_PORT", "8443") == "8443",
    )
    log.info("ClickHouse OK")

    try:
        await run_worker(redis_client, ch_client)
    finally:
        await redis_client.aclose()
        ch_client.close()


if __name__ == "__main__":
    asyncio.run(main())
