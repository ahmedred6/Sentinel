"""
backend/ingest_api/streams.py

Redis Streams enqueue logic and consumer group bootstrap.

Stream design
  sentinel:traces
    Consumer groups: eval-worker  (Module 3 eval engine)
                     ch-writer    (INGEST-05 ClickHouse writer)
    MAXLEN         : 100,000 (approximate)

  sentinel:diffs
    Consumer group : diff-analyzer  (DIFF-02 diff-analyzer-worker)
    MAXLEN         : 50,000 (diffs are larger; rarer)
    Both streams use approximate trimming for write performance.

Payload format
  sentinel:traces  → {"type": "trace"|"signal", "payload": "<JSON>"}
  sentinel:diffs   → {"payload": "<JSON>"}
"""

import os
import sys
from typing import Any

_SDK_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "sentinel-sdk"))
if _SDK_PATH not in sys.path:
    sys.path.insert(0, _SDK_PATH)

from schema import DiffPayload, SignalPayload, TracePayload  # noqa: E402

STREAM_NAME = "sentinel:traces"
CONSUMER_GROUPS = ("eval-worker", "ch-writer")
MAX_STREAM_LEN = 100_000

DIFF_STREAM_NAME = "sentinel:diffs"
DIFF_CONSUMER_GROUP = "diff-analyzer"
MAX_DIFF_STREAM_LEN = 50_000


async def create_consumer_groups(redis: Any) -> None:
    """
    Idempotent startup setup: create both streams and all consumer groups.
    BUSYGROUP (group already exists) is silently ignored so restarts are safe.
    Uses id="$" so each group starts processing only new messages — no
    backlog replay on first deploy.
    """
    for group in CONSUMER_GROUPS:
        try:
            await redis.xgroup_create(STREAM_NAME, group, id="$", mkstream=True)
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    try:
        await redis.xgroup_create(DIFF_STREAM_NAME, DIFF_CONSUMER_GROUP, id="$", mkstream=True)
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def enqueue_trace(trace: TracePayload, redis: Any) -> bytes:
    """
    Append a trace payload to sentinel:traces.
    Stream is capped at MAX_STREAM_LEN with approximate trimming for performance.
    Returns the Redis stream message ID assigned to this entry.
    """
    return await redis.xadd(
        STREAM_NAME,
        {"type": "trace", "payload": trace.model_dump_json()},
        maxlen=MAX_STREAM_LEN,
        approximate=True,
    )


async def enqueue_signal(signal: SignalPayload, redis: Any) -> bytes:
    """
    Append a behavioral signal to sentinel:traces.
    Signals share the same stream as traces so eval-worker can correlate them.
    Returns the Redis stream message ID.
    """
    return await redis.xadd(
        STREAM_NAME,
        {"type": "signal", "payload": signal.model_dump_json()},
        maxlen=MAX_STREAM_LEN,
        approximate=True,
    )


async def enqueue_diff(diff: DiffPayload, redis: Any) -> bytes:
    """
    Append a diff payload to sentinel:diffs.
    No dedup — diffs are not idempotent (same experiment may legitimately
    produce different diffs if files change between retries).
    Returns the Redis stream message ID.
    """
    return await redis.xadd(
        DIFF_STREAM_NAME,
        {"payload": diff.model_dump_json()},
        maxlen=MAX_DIFF_STREAM_LEN,
        approximate=True,
    )
