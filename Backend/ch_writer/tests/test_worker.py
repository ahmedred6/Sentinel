"""
Tests for backend/ch_writer/worker.py

Covers INGEST-05 acceptance criteria:
  - trace_to_ch_row produces correct row in correct column order
  - flush_batch inserts rows and XACKs on success
  - flush_batch retries on ClickHouse failure (3× with backoff)
  - flush_batch dead-letters after all retries exhausted
  - run_worker: batch-size trigger (50 traces)
  - run_worker: timer trigger (flush_interval)
  - run_worker: signals are acked without writing to ClickHouse
  - Integration test: fakeredis → worker → ClickHouse mock
"""

import asyncio
import json
import time
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, call, patch

import fakeredis
import pytest

from worker import (
    CH_COLUMNS,
    DEAD_LETTER_STREAM,
    GROUP_NAME,
    RETRY_DELAYS,
    STREAM_NAME,
    flush_batch,
    run_worker,
    trace_to_ch_row,
)

# ---------------------------------------------------------------------------
# Fixtures and helpers
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
        "user_prompt": f"Question {n}",
        "llm_response": f"Answer {n}",
        "latency_ms": 100 + n,
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


def _make_ch_mock(fail_times: int = 0) -> MagicMock:
    """ClickHouse client mock. Raises on first `fail_times` calls, then succeeds."""
    mock = MagicMock()
    call_count = {"n": 0}

    def _insert(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] <= fail_times:
            raise RuntimeError(f"ClickHouse unavailable (simulated, attempt {call_count['n']})")

    mock.insert = _insert
    return mock


def _new_redis() -> fakeredis.FakeAsyncRedis:
    return fakeredis.FakeAsyncRedis(version=(7, 0, 0))


async def _setup_stream(redis) -> None:
    """Create the consumer group (id=0 so test messages are delivered)."""
    try:
        await redis.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
    except Exception:
        pass  # already exists


# ---------------------------------------------------------------------------
# trace_to_ch_row — pure function tests
# ---------------------------------------------------------------------------

def test_trace_to_ch_row_column_count():
    row = trace_to_ch_row(_make_trace_json())
    assert len(row) == len(CH_COLUMNS), (
        f"Expected {len(CH_COLUMNS)} columns, got {len(row)}"
    )


def test_trace_to_ch_row_column_order():
    tid = str(uuid.uuid4())
    row = trace_to_ch_row(_make_trace_json(7, trace_id=tid))
    mapping = dict(zip(CH_COLUMNS, row))
    assert mapping["trace_id"] == tid
    assert mapping["customer_id"] == "cust_0007"
    assert mapping["flow_name"] == "support"
    assert mapping["latency_ms"] == 107
    assert mapping["token_total"] == 15


def test_trace_to_ch_row_json_columns_are_strings():
    row = trace_to_ch_row(_make_trace_json())
    mapping = dict(zip(CH_COLUMNS, row))
    assert isinstance(mapping["retrieved_context"], str)
    assert isinstance(mapping["tool_calls"], str)
    assert isinstance(mapping["message_history"], str)
    # Must be valid JSON
    json.loads(mapping["retrieved_context"])
    json.loads(mapping["tool_calls"])
    json.loads(mapping["message_history"])


def test_trace_to_ch_row_timestamp_is_datetime():
    row = trace_to_ch_row(_make_trace_json())
    mapping = dict(zip(CH_COLUMNS, row))
    assert isinstance(mapping["timestamp"], datetime)


def test_trace_to_ch_row_nullable_user_id():
    row = trace_to_ch_row(_make_trace_json())
    mapping = dict(zip(CH_COLUMNS, row))
    assert mapping["user_id"] is None


def test_trace_to_ch_row_environment_cast_to_string():
    # environment might be an enum value like "staging" or Enum("staging")
    row = trace_to_ch_row(_make_trace_json())
    mapping = dict(zip(CH_COLUMNS, row))
    assert isinstance(mapping["environment"], str)


# ---------------------------------------------------------------------------
# flush_batch — success path
# ---------------------------------------------------------------------------

def test_flush_batch_inserts_and_acks():
    async def _run():
        redis = _new_redis()
        await _setup_stream(redis)
        ch = _make_ch_mock(fail_times=0)

        # Put a message in the stream and read it into the pending state
        payload = _make_trace_json(1)
        await redis.xadd(STREAM_NAME, {"type": "trace", "payload": payload})
        result = await redis.xreadgroup(GROUP_NAME, "c1", {STREAM_NAME: ">"}, count=10)
        batch = [(msg_id, payload) for msg_id, _ in result[0][1]]

        count = await flush_batch(batch, redis, ch)
        assert count == 1

        # Message should be acked (PEL should be empty)
        pending = await redis.xpending(STREAM_NAME, GROUP_NAME)
        assert pending["pending"] == 0

        await redis.aclose()

    asyncio.run(_run())


def test_flush_batch_calls_insert_with_correct_columns():
    async def _run():
        redis = _new_redis()
        await _setup_stream(redis)
        ch = MagicMock()
        ch.insert = MagicMock()

        payload = _make_trace_json()
        await redis.xadd(STREAM_NAME, {"type": "trace", "payload": payload})
        result = await redis.xreadgroup(GROUP_NAME, "c1", {STREAM_NAME: ">"}, count=10)
        batch = [(msg_id, payload) for msg_id, _ in result[0][1]]

        await flush_batch(batch, redis, ch)

        args, kwargs = ch.insert.call_args
        assert args[0] == "traces"
        assert kwargs["column_names"] == CH_COLUMNS or args[2] == CH_COLUMNS

        await redis.aclose()

    asyncio.run(_run())


def test_flush_batch_empty_batch_returns_zero():
    async def _run():
        redis = _new_redis()
        ch = _make_ch_mock()
        count = await flush_batch([], redis, ch)
        assert count == 0
        await redis.aclose()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# flush_batch — retry logic
# ---------------------------------------------------------------------------

def test_flush_batch_retries_on_failure_then_succeeds():
    """Fail twice, succeed on the third attempt."""
    async def _run():
        redis = _new_redis()
        await _setup_stream(redis)

        payload = _make_trace_json()
        await redis.xadd(STREAM_NAME, {"type": "trace", "payload": payload})
        result = await redis.xreadgroup(GROUP_NAME, "c1", {STREAM_NAME: ">"}, count=10)
        batch = [(msg_id, payload) for msg_id, _ in result[0][1]]

        ch = _make_ch_mock(fail_times=2)  # fail twice, succeed on attempt 3

        with patch("worker.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            count = await flush_batch(batch, redis, ch)

        assert count == 1
        # Two sleeps should have happened (after attempt 1 and 2)
        assert mock_sleep.call_count == 2

        # Message should be acked after eventual success
        pending = await redis.xpending(STREAM_NAME, GROUP_NAME)
        assert pending["pending"] == 0

        await redis.aclose()

    asyncio.run(_run())


def test_flush_batch_dead_letters_after_all_retries():
    """Fail all 3 retry attempts → dead-letter, messages NOT acked."""
    async def _run():
        redis = _new_redis()
        await _setup_stream(redis)

        payload = _make_trace_json()
        await redis.xadd(STREAM_NAME, {"type": "trace", "payload": payload})
        result = await redis.xreadgroup(GROUP_NAME, "c1", {STREAM_NAME: ">"}, count=10)
        batch = [(msg_id, payload) for msg_id, _ in result[0][1]]

        ch = _make_ch_mock(fail_times=99)  # always fail

        with patch("worker.asyncio.sleep", new_callable=AsyncMock):
            count = await flush_batch(batch, redis, ch)

        assert count == 0

        # Message must NOT be acked — stays in PEL for replay
        pending = await redis.xpending(STREAM_NAME, GROUP_NAME)
        assert pending["pending"] == 1

        # Dead-letter stream should have one entry
        dead = await redis.xrange(DEAD_LETTER_STREAM, "-", "+")
        assert len(dead) == 1
        _, dl_fields = dead[0]
        assert b"payload" in dl_fields
        assert b"error" in dl_fields

        await redis.aclose()

    asyncio.run(_run())


def test_flush_batch_retry_count_matches_retry_delays():
    """insert() is called exactly len(RETRY_DELAYS) times before giving up."""
    async def _run():
        redis = _new_redis()
        await _setup_stream(redis)

        call_count = {"n": 0}
        def _always_fail(*a, **k):
            call_count["n"] += 1
            raise RuntimeError("always fail")

        ch = MagicMock()
        ch.insert = _always_fail

        payload = _make_trace_json()
        await redis.xadd(STREAM_NAME, {"type": "trace", "payload": payload})
        result = await redis.xreadgroup(GROUP_NAME, "c1", {STREAM_NAME: ">"}, count=10)
        batch = [(msg_id, payload) for msg_id, _ in result[0][1]]

        with patch("worker.asyncio.sleep", new_callable=AsyncMock):
            await flush_batch(batch, redis, ch)

        assert call_count["n"] == len(RETRY_DELAYS)
        await redis.aclose()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# run_worker — batch-size trigger
# ---------------------------------------------------------------------------

def test_run_worker_flushes_when_batch_is_full():
    """50 messages → flush fires immediately without waiting for the timer."""
    async def _run():
        redis = _new_redis()
        await _setup_stream(redis)

        # Enqueue exactly 50 traces
        for i in range(50):
            payload = _make_trace_json(i)
            await redis.xadd(STREAM_NAME, {"type": "trace", "payload": payload})

        ch = MagicMock()
        inserted_rows = []
        def _capture_insert(table, rows, column_names):
            inserted_rows.extend(rows)
        ch.insert = _capture_insert

        stop = asyncio.Event()

        async def _run_then_stop():
            # Run the worker; stop it after 1 second
            await asyncio.wait_for(
                run_worker(redis, ch, batch_size=50, flush_interval=10.0, _stop_event=stop),
                timeout=3.0,
            )

        # Stop after a short delay
        async def _stopper():
            # Give the worker a moment to flush the full batch
            await asyncio.sleep(0.5)
            stop.set()

        await asyncio.gather(_run_then_stop(), _stopper())

        # All 50 traces should have been inserted
        assert len(inserted_rows) == 50, f"Expected 50, got {len(inserted_rows)}"
        # XACK: PEL should be empty
        pending = await redis.xpending(STREAM_NAME, GROUP_NAME)
        assert pending["pending"] == 0

        await redis.aclose()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# run_worker — timer trigger
# ---------------------------------------------------------------------------

def test_run_worker_flushes_on_timer():
    """Less than 50 messages, but timer fires after flush_interval seconds."""
    async def _run():
        redis = _new_redis()
        await _setup_stream(redis)

        # Only 5 traces — won't hit batch_size=50
        for i in range(5):
            await redis.xadd(STREAM_NAME, {"type": "trace", "payload": _make_trace_json(i)})

        ch = MagicMock()
        inserted_rows = []
        ch.insert = lambda t, rows, column_names: inserted_rows.extend(rows)

        stop = asyncio.Event()

        async def _stopper():
            # Stop after flush_interval + buffer
            await asyncio.sleep(0.4)
            stop.set()

        await asyncio.gather(
            asyncio.wait_for(
                run_worker(redis, ch, batch_size=50, flush_interval=0.2, _stop_event=stop),
                timeout=3.0,
            ),
            _stopper(),
        )

        assert len(inserted_rows) == 5, f"Expected 5, got {len(inserted_rows)}"

        await redis.aclose()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# run_worker — signal messages are acked without writing
# ---------------------------------------------------------------------------

def test_run_worker_acks_signals_without_inserting():
    async def _run():
        redis = _new_redis()
        await _setup_stream(redis)

        # Mix: 2 traces + 2 signals
        tid = str(uuid.uuid4())
        await redis.xadd(STREAM_NAME, {"type": "trace",  "payload": _make_trace_json(1)})
        await redis.xadd(STREAM_NAME, {"type": "signal", "payload": _make_signal_json(tid)})
        await redis.xadd(STREAM_NAME, {"type": "trace",  "payload": _make_trace_json(2)})
        await redis.xadd(STREAM_NAME, {"type": "signal", "payload": _make_signal_json(tid)})

        ch = MagicMock()
        inserted_rows = []
        ch.insert = lambda t, rows, column_names: inserted_rows.extend(rows)

        stop = asyncio.Event()

        async def _stopper():
            await asyncio.sleep(0.4)
            stop.set()

        await asyncio.gather(
            asyncio.wait_for(
                run_worker(redis, ch, batch_size=50, flush_interval=0.2, _stop_event=stop),
                timeout=3.0,
            ),
            _stopper(),
        )

        # Only 2 trace rows should be inserted
        assert len(inserted_rows) == 2, f"Expected 2 trace rows, got {len(inserted_rows)}"
        # ALL 4 messages should be acked (signals acked immediately, traces after flush)
        pending = await redis.xpending(STREAM_NAME, GROUP_NAME)
        assert pending["pending"] == 0

        await redis.aclose()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Integration: fakeredis → worker → mock ClickHouse
# ---------------------------------------------------------------------------

def test_integration_sdk_to_clickhouse_pipeline():
    """
    Simulate the full pipeline:
    SDK enqueue → Redis Stream → ch-writer worker → ClickHouse insert
    """
    async def _run():
        import sys, os
        # Import enqueue_trace from ingest_api/streams.py
        _INGEST = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "ingest_api")
        )
        _SDK = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "sentinel-sdk")
        )
        for p in (_INGEST, _SDK):
            if p not in sys.path:
                sys.path.insert(0, p)

        from streams import create_consumer_groups, enqueue_trace
        from schema import TokenCounts, TracePayload

        redis = _new_redis()
        await create_consumer_groups(redis)

        # Enqueue 10 traces the same way the ingest API does
        trace_ids = []
        for i in range(10):
            trace = TracePayload(
                trace_id=str(uuid.uuid4()),
                customer_id="cust_e2e",
                session_id="sess_e2e",
                flow_name="support",
                model_name="gpt-4",
                user_prompt=f"E2E question {i}",
                llm_response=f"E2E answer {i}",
                latency_ms=100,
                token_counts=TokenCounts(prompt=10, completion=5, total=15),
            )
            trace_ids.append(trace.trace_id)
            await enqueue_trace(trace, redis)

        # Run the worker until it flushes all 10
        ch = MagicMock()
        inserted = []
        ch.insert = lambda t, rows, column_names: inserted.extend(rows)

        stop = asyncio.Event()
        async def _stopper():
            await asyncio.sleep(0.4)
            stop.set()

        await asyncio.gather(
            asyncio.wait_for(
                run_worker(redis, ch, batch_size=50, flush_interval=0.2, _stop_event=stop),
                timeout=3.0,
            ),
            _stopper(),
        )

        assert len(inserted) == 10
        # Verify trace_ids round-trip
        col_idx = {col: i for i, col in enumerate(CH_COLUMNS)}
        inserted_ids = {row[col_idx["trace_id"]] for row in inserted}
        assert inserted_ids == set(trace_ids)

        # All messages acked
        pending = await redis.xpending(STREAM_NAME, GROUP_NAME)
        assert pending["pending"] == 0

        await redis.aclose()

    asyncio.run(_run())
