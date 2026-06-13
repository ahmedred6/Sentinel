"""
backend/ingest_api/streams.py

Redis Streams enqueue logic and consumer group bootstrap.

Stream design
  Name           : sentinel:traces
  Consumer groups: eval-worker  (Module 3 eval engine)
                   ch-writer    (INGEST-05 ClickHouse writer)
  MAXLEN         : 100,000 (approximate — ~ prefix)
  Both groups consume the same stream independently at their own pace.
  Messages remain in the stream until trimmed by MAXLEN.

Payload format in each message
  {"type": "trace"|"signal", "payload": "<JSON>"}
"""

import os
import sys
from typing import Any

_SDK_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "sentinel-sdk"))
if _SDK_PATH not in sys.path:
    sys.path.insert(0, _SDK_PATH)

from schema import SignalPayload, TracePayload  # noqa: E402

STREAM_NAME = "sentinel:traces"
CONSUMER_GROUPS = ("eval-worker", "ch-writer")
MAX_STREAM_LEN = 100_000


async def create_consumer_groups(redis: Any) -> None:
    """
    Idempotent startup setup: create the stream and both consumer groups.
    BUSYGROUP (group already exists) is silently ignored so restarts are safe.
    Uses id="$" so each group starts processing only messages that arrive
    after it was created — no backlog replay on first deploy.
    """
    for group in CONSUMER_GROUPS:
        try:
            await redis.xgroup_create(STREAM_NAME, group, id="$", mkstream=True)
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
