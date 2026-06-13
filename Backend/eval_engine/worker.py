"""
backend/eval_engine/worker.py

Eval-worker: orchestrates the three-layer evaluation engine.

Consumption: reads from the eval-worker consumer group on the
sentinel:traces Redis Stream.

Processing order per trace:
  1. Layer 1 (heuristics) and Layer 2 (behavioral) run in parallel — await both.
  2. Any non-passing results are written to the PostgreSQL scores table.
  3. Redis XACK is sent only after scores are persisted.
  4. Layer 3 (LLM-as-judge) is fired as an asyncio background task.
     It never blocks the ACK and writes its own score when it completes.

Failure policy:
  - A layer that throws is logged and treated as "no failure detected".
  - A DB write that fails is logged; the ACK still proceeds.
  - Messages are never left permanently unACKed.
"""

import asyncio
import logging
import os
import sys
from typing import Any

_EVAL_DIR = os.path.dirname(__file__)
_SDK_DIR = os.path.abspath(os.path.join(_EVAL_DIR, "..", "sentinel-sdk"))
for _p in (_EVAL_DIR, _SDK_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from schema import TracePayload  # noqa: E402
from layers import LayerResult, run_layer1, run_layer2, run_layer3  # noqa: E402

log = logging.getLogger("eval_worker")

STREAM_NAME = "sentinel:traces"
GROUP_NAME = "eval-worker"
CONSUMER_NAME = os.environ.get("EVAL_CONSUMER_NAME", "eval-worker-1")
POLL_INTERVAL = 0.5   # seconds between polls when stream is empty
POLL_TICK = 0.05      # stop-event check granularity while idle


# ---------------------------------------------------------------------------
# Score persistence
# ---------------------------------------------------------------------------

async def write_score(result: LayerResult, trace: TracePayload, db_pool: Any) -> None:
    """
    Insert a failing layer result into the scores table.
    No-op for passing results. Logs and swallows DB errors so the caller
    can always proceed to ACK.
    """
    if result.passed:
        return
    try:
        await db_pool.execute(
            """
            INSERT INTO scores
                (trace_id, customer_id, failure_type, severity, confidence, evidence, layer)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            trace.trace_id,
            trace.customer_id,
            result.failure_type,
            result.severity,
            result.confidence,
            result.evidence,
            result.layer,
        )
    except Exception as exc:
        log.error(
            "write_score failed for trace %s layer %d: %s",
            trace.trace_id, result.layer, exc,
        )


# ---------------------------------------------------------------------------
# Layer helpers
# ---------------------------------------------------------------------------

async def _safe_layer(fn, trace: TracePayload, layer_num: int) -> LayerResult:
    """Run a layer coroutine; return a passing result if it raises."""
    try:
        return await fn(trace)
    except Exception as exc:
        log.error("Layer %d threw for trace %s: %s", layer_num, trace.trace_id, exc)
        return LayerResult(layer=layer_num, passed=True)


async def _run_layer3_bg(trace: TracePayload, db_pool: Any) -> None:
    """Background task: run Layer 3 and persist its score. Never raises."""
    try:
        l3 = await run_layer3(trace)
        await write_score(l3, trace, db_pool)
    except Exception as exc:
        log.error("Layer 3 background task failed for trace %s: %s", trace.trace_id, exc)


# ---------------------------------------------------------------------------
# Per-trace orchestration
# ---------------------------------------------------------------------------

async def process_trace(
    trace: TracePayload,
    msg_id: bytes,
    redis: Any,
    db_pool: Any,
) -> None:
    """
    Orchestrate a single trace through all three layers.

    Layers 1 and 2 run in parallel; their scores are written before ACK.
    Layer 3 is fired as a background task and never delays the ACK.
    """
    # L1 + L2 in parallel
    l1, l2 = await asyncio.gather(
        _safe_layer(run_layer1, trace, 1),
        _safe_layer(run_layer2, trace, 2),
    )

    # Persist L1 and L2 scores (write_score is a no-op for passing results)
    await write_score(l1, trace, db_pool)
    await write_score(l2, trace, db_pool)

    # ACK only after scores are written
    await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)

    # Fire L3 without blocking the loop
    asyncio.create_task(_run_layer3_bg(trace, db_pool))


# ---------------------------------------------------------------------------
# Worker loop
# ---------------------------------------------------------------------------

async def run_worker(
    redis: Any,
    db_pool: Any,
    *,
    _stop_event: asyncio.Event | None = None,
) -> None:
    """
    Infinite loop: drain the eval-worker consumer group and dispatch each
    trace to all three evaluation layers.

    Pass _stop_event to halt cleanly (used in tests and on SIGTERM).
    Uses non-blocking xreadgroup + explicit asyncio.sleep so the stop event
    can interrupt idle waits without leaving detached background tasks.
    """
    log.info("eval-worker started (consumer=%s)", CONSUMER_NAME)

    while True:
        if _stop_event and _stop_event.is_set():
            break

        try:
            results = await redis.xreadgroup(
                GROUP_NAME,
                CONSUMER_NAME,
                {STREAM_NAME: ">"},
                count=10,
                block=0,
            )
        except Exception as exc:
            log.error("xreadgroup error: %s — sleeping 1s", exc)
            await asyncio.sleep(1)
            continue

        if not results:
            # No new messages; sleep with fine-grained stop-event checks
            deadline = asyncio.get_event_loop().time() + POLL_INTERVAL
            while not (_stop_event and _stop_event.is_set()):
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    break
                await asyncio.sleep(min(POLL_TICK, remaining))
            continue

        for _stream, messages in results:
            for msg_id, fields in messages:
                msg_type = (fields.get(b"type") or b"").decode()
                payload_bytes = fields.get(b"payload") or b""

                if msg_type != "trace" or not payload_bytes:
                    # Signals belong to a future handler; ACK to clear PEL
                    await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
                    continue

                try:
                    trace = TracePayload.model_validate_json(payload_bytes)
                    await process_trace(trace, msg_id, redis, db_pool)
                except Exception as exc:
                    log.error(
                        "process_trace failed for msg %s: %s — ACKing to avoid PEL growth",
                        msg_id, exc,
                    )
                    await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)

    log.info("eval-worker stopped")
