"""
backend/ingest_api/main.py

Sentinel Ingest API — receives traces and signals from the SDK.

Endpoints:
    POST /ingest/trace   — full TracePayload from SDK
    POST /ingest/signal  — SignalPayload (behavioral signal)

Stub (replaced in INGEST-03 follow-on):
    Dedup : in-memory dict — will move to Redis SETNX + TTL
"""

import os
import sys
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI

_SDK_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "sentinel-sdk"))
if _SDK_PATH not in sys.path:
    sys.path.insert(0, _SDK_PATH)

from schema import DiffPayload, SignalPayload, TracePayload  # noqa: E402
from auth import require_auth  # noqa: E402
from dependencies import get_redis, set_clickhouse_client, set_db_pool, set_redis  # noqa: E402
from streams import create_consumer_groups, enqueue_diff, enqueue_signal, enqueue_trace  # noqa: E402

# ---------------------------------------------------------------------------
# Lifespan — connect Redis and PostgreSQL, bootstrap consumer groups
# Tests use TestClient(app) without `with`, so this never fires in tests.
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    db_url = os.environ.get("DATABASE_URL")

    redis_client = None
    db_pool = None

    try:
        import redis.asyncio as aioredis  # noqa: PLC0415
        redis_client = aioredis.from_url(
            redis_url,
            decode_responses=False,
            max_connections=20,  # connection pool — shared across all requests
        )
        await redis_client.ping()
        set_redis(redis_client)
        await create_consumer_groups(redis_client)
        print(f"[startup] Redis connected: {redis_url}")
        print(f"[startup] Consumer groups ready: {', '.join(['eval-worker', 'ch-writer', 'diff-analyzer'])}")
    except Exception as e:
        print(f"[startup] WARNING: Redis unavailable ({e})")

    if db_url:
        try:
            import asyncpg  # noqa: PLC0415
            db_pool = await asyncpg.create_pool(db_url)
            set_db_pool(db_pool)
            print("[startup] PostgreSQL connected")
        except Exception as e:
            print(f"[startup] WARNING: PostgreSQL unavailable ({e})")

    ch_host = os.environ.get("CLICKHOUSE_HOST")
    ch_client = None
    if ch_host:
        try:
            import clickhouse_connect  # noqa: PLC0415
            ch_client = clickhouse_connect.get_client(
                host=ch_host,
                port=int(os.environ.get("CLICKHOUSE_PORT", 8443)),
                username=os.environ.get("CLICKHOUSE_USER", "default"),
                password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
                database=os.environ.get("CLICKHOUSE_DB", "default"),
                secure=True,
            )
            set_clickhouse_client(ch_client)
            print(f"[startup] ClickHouse connected: {ch_host}")
        except Exception as e:
            print(f"[startup] WARNING: ClickHouse unavailable ({e})")

    yield

    if redis_client:
        await redis_client.aclose()
    if db_pool:
        await db_pool.close()
    if ch_client:
        ch_client.close()


app = FastAPI(title="Sentinel Ingest API", version="0.3.0", lifespan=lifespan)

# ---------------------------------------------------------------------------
# Dedup — in-memory stub (moves to Redis SETNX + TTL in a follow-on)
# ---------------------------------------------------------------------------

_DEDUP_WINDOW = 60.0
_dedup: dict[str, float] = {}


def _is_duplicate(trace_id: str) -> bool:
    now = time.monotonic()
    expired = [k for k, v in _dedup.items() if now - v > _DEDUP_WINDOW]
    for k in expired:
        del _dedup[k]
    if trace_id in _dedup:
        return True
    _dedup[trace_id] = now
    return False


def _reset_dedup() -> None:
    """Clear the dedup cache. Used in tests only."""
    _dedup.clear()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.post("/ingest/trace")
async def ingest_trace(
    payload: TracePayload,
    customer_id: str = Depends(require_auth),
    redis: Any = Depends(get_redis),
) -> dict[str, str]:
    if _is_duplicate(payload.trace_id):
        return {"status": "ok", "trace_id": payload.trace_id}
    await enqueue_trace(payload, redis)
    return {"status": "ok", "trace_id": payload.trace_id}


@app.post("/ingest/diff")
async def ingest_diff(
    payload: DiffPayload,
    customer_id: str = Depends(require_auth),
    redis: Any = Depends(get_redis),
) -> dict[str, str]:
    await enqueue_diff(payload, redis)
    return {"status": "ok", "experiment_id": payload.experiment_id}


@app.post("/ingest/signal")
async def ingest_signal(
    payload: SignalPayload,
    customer_id: str = Depends(require_auth),
    redis: Any = Depends(get_redis),
) -> dict[str, str]:
    await enqueue_signal(payload, redis)
    return {"status": "ok", "trace_id": payload.trace_id}


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
