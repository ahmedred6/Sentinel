"""
backend/ch_writer/worker.py

ClickHouse writer worker.

Drains the ch-writer consumer group from the sentinel:traces Redis Stream
and batch-inserts raw trace payloads into ClickHouse.

Batch flush triggers
  - 50 traces accumulated  (BATCH_SIZE)
  - 2 seconds since last flush (FLUSH_INTERVAL)
  Whichever comes first.

Insert failures are retried up to 3× with exponential backoff.
After all retries, the batch is dead-lettered to sentinel:dead-letter
and the messages are NOT acknowledged (they stay in the PEL for replay).

Signals are acknowledged immediately without writing to ClickHouse
(they belong in PostgreSQL — handled by the eval engine).
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger("ch_writer")

# ---------------------------------------------------------------------------
# Stream / group config
# ---------------------------------------------------------------------------

STREAM_NAME = "sentinel:traces"
GROUP_NAME = "ch-writer"
CONSUMER_NAME = os.environ.get("CH_WRITER_CONSUMER_NAME", "ch-writer-1")
DEAD_LETTER_STREAM = "sentinel:dead-letter"

BATCH_SIZE = 50
FLUSH_INTERVAL = 2.0        # seconds
XREAD_BLOCK_MS = 500        # how long each XREADGROUP blocks before re-checking timer
RETRY_DELAYS = (0.5, 1.0, 2.0)   # backoff between retries (seconds)

# ClickHouse column order — must match 001_clickhouse_traces.sql
CH_COLUMNS = [
    "trace_id", "customer_id", "session_id", "user_id",
    "flow_name", "environment", "model_name",
    "user_prompt", "llm_response",
    "latency_ms", "token_total",
    "retrieved_context", "tool_calls", "message_history",
    "timestamp",
]


# ---------------------------------------------------------------------------
# Row builder
# ---------------------------------------------------------------------------

def trace_to_ch_row(payload_json: str) -> list:
    """
    Convert a JSON-serialised TracePayload string into a ClickHouse row list.
    Order matches CH_COLUMNS exactly.
    """
    d = json.loads(payload_json)

    # Parse timestamp — Pydantic serialises datetime as ISO 8601 string
    ts_raw = d.get("timestamp")
    if isinstance(ts_raw, str):
        ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
    elif isinstance(ts_raw, datetime):
        ts = ts_raw
    else:
        ts = datetime.now(timezone.utc)

    tc = d.get("token_counts") or {}

    return [
        d.get("trace_id", ""),
        d.get("customer_id", ""),
        d.get("session_id", ""),
        d.get("user_id"),                              # Nullable(String)
        d.get("flow_name", ""),
        str(d.get("environment", "prod")),
        d.get("model_name", ""),
        d.get("user_prompt", ""),
        d.get("llm_response", ""),
        int(d.get("latency_ms", 0)),
        int(tc.get("total", 0)),
        json.dumps(d.get("retrieved_context") or []),
        json.dumps(d.get("tool_calls") or []),
        json.dumps(d.get("message_history") or []),
        ts,
    ]


# ---------------------------------------------------------------------------
# Batch flush with retry + dead-letter
# ---------------------------------------------------------------------------

# Type alias: each item in a batch is (redis_message_id, payload_json_string)
_BatchItem = tuple[bytes, str]


async def flush_batch(
    batch: list[_BatchItem],
    redis: Any,
    ch_client: Any,
) -> int:
    """
    Insert the batch into ClickHouse, then XACK on success.
    Returns the number of rows inserted (0 if dead-lettered).

    Retry policy: up to 3 attempts with delays RETRY_DELAYS.
    After all retries, messages are dead-lettered (NOT acked — stay in PEL).
    """
    if not batch:
        return 0

    rows = [trace_to_ch_row(payload_json) for _, payload_json in batch]
    msg_ids = [msg_id for msg_id, _ in batch]

    last_exc: Exception | None = None
    for attempt, delay in enumerate(RETRY_DELAYS, start=1):
        try:
            await asyncio.to_thread(
                ch_client.insert,
                "traces",
                rows,
                column_names=CH_COLUMNS,
            )
            # Success: acknowledge all messages in this batch
            await redis.xack(STREAM_NAME, GROUP_NAME, *msg_ids)
            log.info("flushed %d traces (attempt %d)", len(batch), attempt)
            return len(batch)
        except Exception as exc:
            last_exc = exc
            log.warning("insert attempt %d failed: %s — retrying in %.1fs", attempt, exc, delay)
            await asyncio.sleep(delay)

    # All retries exhausted — dead-letter (do NOT xack; messages stay in PEL)
    log.error("dead-lettering %d traces after %d attempts: %s", len(batch), len(RETRY_DELAYS), last_exc)
    for msg_id, payload_json in batch:
        await redis.xadd(
            DEAD_LETTER_STREAM,
            {
                "original_msg_id": msg_id,
                "payload": payload_json,
                "error": str(last_exc),
            },
        )
    return 0


# ---------------------------------------------------------------------------
# Main worker loop
# ---------------------------------------------------------------------------

async def run_worker(
    redis: Any,
    ch_client: Any,
    *,
    batch_size: int = BATCH_SIZE,
    flush_interval: float = FLUSH_INTERVAL,
    _stop_event: asyncio.Event | None = None,   # set in tests to stop the loop
) -> None:
    """
    Infinite loop: drain ch-writer group → batch → flush to ClickHouse.
    Set _stop_event to halt cleanly (used by tests).

    Uses non-blocking xreadgroup + explicit asyncio.sleep so the stop event
    can interrupt idle waits without leaving detached background tasks.
    """
    log.info("ch-writer started (batch_size=%d, flush_interval=%.1fs)", batch_size, flush_interval)

    batch: list[_BatchItem] = []
    last_flush = asyncio.get_event_loop().time()

    while True:
        if _stop_event and _stop_event.is_set():
            break

        # Time remaining until the flush deadline
        elapsed = asyncio.get_event_loop().time() - last_flush
        time_left_ms = max(1, int((flush_interval - elapsed) * 1000))
        poll_secs = min(time_left_ms, XREAD_BLOCK_MS) / 1000.0

        # Non-blocking read — avoids detached background tasks from fakeredis
        read_count = max(1, batch_size - len(batch))
        try:
            results = await redis.xreadgroup(
                GROUP_NAME,
                CONSUMER_NAME,
                {STREAM_NAME: ">"},
                count=read_count,
                block=0,
            )
        except Exception as exc:
            log.error("xreadgroup error: %s — sleeping 1s", exc)
            await asyncio.sleep(1)
            continue

        if not results:
            # No new messages; sleep up to poll_secs before re-polling.
            # Poll in short increments so _stop_event is detected promptly.
            deadline = asyncio.get_event_loop().time() + poll_secs
            while not (_stop_event and _stop_event.is_set()):
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    break
                await asyncio.sleep(min(0.05, remaining))
            continue

        for _stream_name, messages in results:
            for msg_id, fields in messages:
                msg_type = (fields.get(b"type") or b"").decode()
                payload_bytes = fields.get(b"payload") or b""

                if msg_type != "trace" or not payload_bytes:
                    # Signals and empty messages: ack without writing
                    await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
                    continue

                batch.append((msg_id, payload_bytes.decode()))

        # Flush if batch is full or the timer has fired
        now = asyncio.get_event_loop().time()
        batch_full = len(batch) >= batch_size
        timer_fired = batch and (now - last_flush) >= flush_interval

        if batch_full or timer_fired:
            await flush_batch(batch, redis, ch_client)
            batch = []
            last_flush = asyncio.get_event_loop().time()

    # Flush any remaining messages before exiting
    if batch:
        await flush_batch(batch, redis, ch_client)
        log.info("ch-writer flushed remaining %d traces on shutdown", len(batch))

    log.info("ch-writer stopped")
