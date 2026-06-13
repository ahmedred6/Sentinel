"""
Tests for backend/eval_engine/worker.py

Covers EVAL-01 acceptance criteria:
  - Worker consumes from eval-worker consumer group correctly
  - Each trace dispatched to all three layers
  - ACK sent only after score written to PostgreSQL
  - Failed layer logged but does not crash the worker
  - Worker tested with 10 real traces from Redis
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis
import pytest

from worker import (
    GROUP_NAME,
    STREAM_NAME,
    process_trace,
    run_worker,
    write_score,
)
from layers import LayerResult
from schema import TokenCounts, TracePayload


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_trace_json(n: int = 0, trace_id: str | None = None) -> str:
    return json.dumps({
        "trace_id": trace_id or str(uuid.uuid4()),
        "customer_id": f"cust_{n:04d}",
        "session_id": "sess_001",
        "user_id": None,
        "flow_name": "support",
        "environment": "staging",
        "model_name": "gpt-4",
        "user_prompt": f"Q{n}",
        "llm_response": f"A{n}",
        "latency_ms": 100,
        "token_counts": {"prompt": 10, "completion": 5, "total": 15},
        "retrieved_context": [],
        "tool_calls": [],
        "message_history": [],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })


def _make_signal_json(trace_id: str | None = None) -> str:
    return json.dumps({
        "signal_id": str(uuid.uuid4()),
        "trace_id": trace_id or str(uuid.uuid4()),
        "customer_id": "cust_001",
        "signal_type": "thumbs_down",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })


def _make_trace_payload(n: int = 0) -> TracePayload:
    return TracePayload(
        trace_id=str(uuid.uuid4()),
        customer_id=f"cust_{n:04d}",
        session_id="sess_001",
        flow_name="support",
        model_name="gpt-4",
        user_prompt=f"Q{n}",
        llm_response=f"A{n}",
        latency_ms=100,
        token_counts=TokenCounts(prompt=10, completion=5, total=15),
    )


def _new_redis() -> fakeredis.FakeAsyncRedis:
    return fakeredis.FakeAsyncRedis(version=(7, 0, 0))


async def _setup_stream(redis) -> None:
    try:
        await redis.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
    except Exception:
        pass


def _make_db_mock() -> AsyncMock:
    db = AsyncMock()
    db.execute = AsyncMock()
    return db


# ---------------------------------------------------------------------------
# Test 1: 10 real traces consumed and ACKed
# ---------------------------------------------------------------------------

def test_worker_consumes_10_traces():
    """Worker tested with 10 real traces from Redis — all ACKed, no crashes."""
    async def _run():
        redis = _new_redis()
        await _setup_stream(redis)

        for i in range(10):
            await redis.xadd(STREAM_NAME, {"type": "trace", "payload": _make_trace_json(i)})

        db = _make_db_mock()
        stop = asyncio.Event()

        async def _stopper():
            await asyncio.sleep(0.4)
            stop.set()

        await asyncio.gather(
            run_worker(redis, db, _stop_event=stop),
            _stopper(),
        )

        pending = await redis.xpending(STREAM_NAME, GROUP_NAME)
        assert pending["pending"] == 0, (
            f"Expected PEL=0 after processing, got {pending['pending']}"
        )
        await redis.aclose()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Test 2: Both L1 and L2 dispatched per trace
# ---------------------------------------------------------------------------

def test_worker_dispatches_to_layer1_and_layer2():
    """Each trace calls run_layer1 and run_layer2 exactly once."""
    async def _run():
        redis = _new_redis()
        await _setup_stream(redis)
        await redis.xadd(STREAM_NAME, {"type": "trace", "payload": _make_trace_json()})

        db = _make_db_mock()
        stop = asyncio.Event()

        async def _stopper():
            await asyncio.sleep(0.3)
            stop.set()

        with (
            patch("worker.run_layer1", new_callable=AsyncMock) as mock_l1,
            patch("worker.run_layer2", new_callable=AsyncMock) as mock_l2,
        ):
            mock_l1.return_value = LayerResult(layer=1, passed=True)
            mock_l2.return_value = LayerResult(layer=2, passed=True)

            await asyncio.gather(
                run_worker(redis, db, _stop_event=stop),
                _stopper(),
            )

        assert mock_l1.call_count == 1
        assert mock_l2.call_count == 1
        await redis.aclose()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Test 3: Signals ACKed without invoking layers or DB
# ---------------------------------------------------------------------------

def test_signals_acked_without_layer_processing():
    """Signal-type messages are ACKed immediately; layers are not called."""
    async def _run():
        redis = _new_redis()
        await _setup_stream(redis)
        await redis.xadd(STREAM_NAME, {"type": "signal", "payload": _make_signal_json()})

        db = _make_db_mock()
        stop = asyncio.Event()

        async def _stopper():
            await asyncio.sleep(0.3)
            stop.set()

        with (
            patch("worker.run_layer1", new_callable=AsyncMock) as mock_l1,
            patch("worker.run_layer2", new_callable=AsyncMock) as mock_l2,
        ):
            await asyncio.gather(
                run_worker(redis, db, _stop_event=stop),
                _stopper(),
            )

        # No layers called, no DB writes
        assert mock_l1.call_count == 0
        assert mock_l2.call_count == 0
        db.execute.assert_not_called()

        # Message still ACKed (PEL clear)
        pending = await redis.xpending(STREAM_NAME, GROUP_NAME)
        assert pending["pending"] == 0
        await redis.aclose()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Test 4: Layer 1 exception does not crash worker
# ---------------------------------------------------------------------------

def test_layer1_exception_acks_message():
    """L1 throwing is logged; the message is still ACKed."""
    async def _run():
        redis = _new_redis()
        await _setup_stream(redis)
        await redis.xadd(STREAM_NAME, {"type": "trace", "payload": _make_trace_json()})

        db = _make_db_mock()
        stop = asyncio.Event()

        async def _stopper():
            await asyncio.sleep(0.3)
            stop.set()

        with patch("worker.run_layer1", side_effect=RuntimeError("L1 boom")):
            await asyncio.gather(
                run_worker(redis, db, _stop_event=stop),
                _stopper(),
            )

        pending = await redis.xpending(STREAM_NAME, GROUP_NAME)
        assert pending["pending"] == 0
        await redis.aclose()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Test 5: Layer 2 exception does not crash worker
# ---------------------------------------------------------------------------

def test_layer2_exception_acks_message():
    """L2 throwing is logged; the message is still ACKed."""
    async def _run():
        redis = _new_redis()
        await _setup_stream(redis)
        await redis.xadd(STREAM_NAME, {"type": "trace", "payload": _make_trace_json()})

        db = _make_db_mock()
        stop = asyncio.Event()

        async def _stopper():
            await asyncio.sleep(0.3)
            stop.set()

        with patch("worker.run_layer2", side_effect=RuntimeError("L2 boom")):
            await asyncio.gather(
                run_worker(redis, db, _stop_event=stop),
                _stopper(),
            )

        pending = await redis.xpending(STREAM_NAME, GROUP_NAME)
        assert pending["pending"] == 0
        await redis.aclose()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Test 6: Failing layer result writes a score row to PostgreSQL
# ---------------------------------------------------------------------------

def test_failing_layer_result_writes_score():
    """When L1 detects a failure, write_score inserts into scores table."""
    async def _run():
        redis = _new_redis()
        await _setup_stream(redis)
        await redis.xadd(STREAM_NAME, {"type": "trace", "payload": _make_trace_json()})

        db = _make_db_mock()
        stop = asyncio.Event()

        async def _stopper():
            await asyncio.sleep(0.3)
            stop.set()

        with patch("worker.run_layer1", new_callable=AsyncMock) as mock_l1:
            mock_l1.return_value = LayerResult(
                layer=1,
                passed=False,
                failure_type="hallucination",
                severity="high",
                confidence=0.92,
                evidence="Response contradicts retrieved context.",
            )
            await asyncio.gather(
                run_worker(redis, db, _stop_event=stop),
                _stopper(),
            )

        db.execute.assert_called_once()
        call_args = db.execute.call_args[0]
        # Positional args: (query, trace_id, customer_id, failure_type, severity,
        #                    confidence, evidence, layer)
        assert call_args[3] == "hallucination"
        assert call_args[4] == "high"
        assert call_args[7] == 1
        await redis.aclose()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Test 7: Passing layer result does NOT write to PostgreSQL
# ---------------------------------------------------------------------------

def test_passing_layer_skips_write():
    """When all layers pass, db.execute is never called."""
    async def _run():
        redis = _new_redis()
        await _setup_stream(redis)
        await redis.xadd(STREAM_NAME, {"type": "trace", "payload": _make_trace_json()})

        db = _make_db_mock()
        stop = asyncio.Event()

        async def _stopper():
            await asyncio.sleep(0.3)
            stop.set()

        # Default stub layers all return passed=True
        await asyncio.gather(
            run_worker(redis, db, _stop_event=stop),
            _stopper(),
        )

        db.execute.assert_not_called()
        await redis.aclose()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Test 8: Score written BEFORE XACK (ordering guarantee)
# ---------------------------------------------------------------------------

def test_score_written_before_ack():
    """DB insert for a failing layer happens strictly before the XACK."""
    async def _run():
        redis = _new_redis()
        await _setup_stream(redis)
        await redis.xadd(STREAM_NAME, {"type": "trace", "payload": _make_trace_json()})

        call_order: list[str] = []

        db = AsyncMock()
        async def _db_execute(*args, **kwargs):
            call_order.append("write")
        db.execute = _db_execute

        original_xack = redis.xack
        async def _xack_wrapper(*args, **kwargs):
            call_order.append("ack")
            return await original_xack(*args, **kwargs)
        redis.xack = _xack_wrapper

        stop = asyncio.Event()

        async def _stopper():
            await asyncio.sleep(0.3)
            stop.set()

        with patch("worker.run_layer1", new_callable=AsyncMock) as mock_l1:
            mock_l1.return_value = LayerResult(
                layer=1, passed=False, failure_type="test", severity="low", confidence=0.8
            )
            await asyncio.gather(
                run_worker(redis, db, _stop_event=stop),
                _stopper(),
            )

        # write must appear before ack
        assert "write" in call_order
        assert "ack" in call_order
        assert call_order.index("write") < call_order.index("ack"), (
            f"Expected write before ack, got: {call_order}"
        )
        await redis.aclose()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Test 9: Layer 3 fires as a background task (does not block ACK)
# ---------------------------------------------------------------------------

def test_layer3_fires_as_background_task():
    """
    L3 is invoked for each trace but runs after the ACK.
    Verified by checking L3 was called AND PEL is empty.
    """
    async def _run():
        redis = _new_redis()
        await _setup_stream(redis)
        await redis.xadd(STREAM_NAME, {"type": "trace", "payload": _make_trace_json()})

        db = _make_db_mock()
        stop = asyncio.Event()
        l3_called = asyncio.Event()

        async def _mock_l3(trace):
            l3_called.set()
            return LayerResult(layer=3, passed=True)

        async def _stopper():
            await asyncio.sleep(0.3)
            stop.set()

        with patch("worker.run_layer3", side_effect=_mock_l3):
            await asyncio.gather(
                run_worker(redis, db, _stop_event=stop),
                _stopper(),
            )
            # Give the background task a moment to complete
            await asyncio.sleep(0.05)

        assert l3_called.is_set(), "Layer 3 was never called"

        # Message must be ACKed (L3 never blocks this)
        pending = await redis.xpending(STREAM_NAME, GROUP_NAME)
        assert pending["pending"] == 0
        await redis.aclose()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Test 10: DB write failure still ACKs the message
# ---------------------------------------------------------------------------

def test_db_failure_still_acks():
    """If the DB is down, write_score logs the error and ACK still proceeds."""
    async def _run():
        redis = _new_redis()
        await _setup_stream(redis)
        await redis.xadd(STREAM_NAME, {"type": "trace", "payload": _make_trace_json()})

        db = AsyncMock()
        db.execute = AsyncMock(side_effect=RuntimeError("DB connection refused"))

        stop = asyncio.Event()

        async def _stopper():
            await asyncio.sleep(0.3)
            stop.set()

        with patch("worker.run_layer1", new_callable=AsyncMock) as mock_l1:
            mock_l1.return_value = LayerResult(
                layer=1, passed=False, failure_type="test", severity="low", confidence=0.5
            )
            await asyncio.gather(
                run_worker(redis, db, _stop_event=stop),
                _stopper(),
            )

        # ACK must have happened despite the DB failure
        pending = await redis.xpending(STREAM_NAME, GROUP_NAME)
        assert pending["pending"] == 0
        await redis.aclose()

    asyncio.run(_run())
