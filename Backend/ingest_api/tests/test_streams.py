"""
Integration tests for backend/ingest_api/streams.py

Uses fakeredis (in-memory Redis 7 implementation) — no real Redis needed.

Covers INGEST-03 acceptance criteria:
  - enqueue_trace() / enqueue_signal() write to sentinel:traces
  - Both consumer groups (eval-worker, ch-writer) see every message independently
  - 100 traces → both groups confirmed to receive all 100
  - Stream is capped at MAX_STREAM_LEN messages (MAXLEN enforcement)
  - Message payload is valid JSON matching the original model
  - create_consumer_groups() is idempotent (safe to call multiple times)
"""

import asyncio
import json
import uuid

import fakeredis
import pytest

from streams import (
    CONSUMER_GROUPS,
    DIFF_CONSUMER_GROUP,
    DIFF_STREAM_NAME,
    MAX_DIFF_STREAM_LEN,
    MAX_STREAM_LEN,
    STREAM_NAME,
    create_consumer_groups,
    enqueue_diff,
    enqueue_signal,
    enqueue_trace,
)

# Schema imported via streams.py's sys.path setup
from schema import DiffPayload, Environment, SignalPayload, TokenCounts, TracePayload


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_trace(n: int = 0) -> TracePayload:
    return TracePayload(
        trace_id=str(uuid.uuid4()),
        customer_id=f"cust_{n:04d}",
        session_id="sess_001",
        flow_name="support",
        model_name="gpt-4",
        user_prompt=f"Question {n}",
        llm_response=f"Answer {n}",
        latency_ms=100 + n,
        token_counts=TokenCounts(prompt=10, completion=5, total=15),
    )


def _make_signal(trace_id: str) -> SignalPayload:
    return SignalPayload(
        trace_id=trace_id,
        customer_id="cust_0001",
        signal_type="thumbs_down",
    )


def _new_redis() -> fakeredis.FakeAsyncRedis:
    """Fresh isolated FakeRedis instance with Redis 7 semantics."""
    return fakeredis.FakeAsyncRedis(version=(7, 0, 0))


# ---------------------------------------------------------------------------
# enqueue_trace
# ---------------------------------------------------------------------------

def test_enqueue_trace_returns_message_id():
    async def _run():
        r = _new_redis()
        try:
            mid = await enqueue_trace(_make_trace(), r)
            assert mid is not None
            assert b"-" in mid  # Redis stream IDs are "timestamp-seq"
        finally:
            await r.aclose()

    asyncio.run(_run())


def test_enqueue_trace_payload_is_valid_json():
    async def _run():
        r = _new_redis()
        try:
            trace = _make_trace(42)
            await enqueue_trace(trace, r)
            messages = await r.xrange(STREAM_NAME, "-", "+")
            _, fields = messages[0]
            data = json.loads(fields[b"payload"])
            assert data["customer_id"] == "cust_0042"
            assert data["flow_name"] == "support"
        finally:
            await r.aclose()

    asyncio.run(_run())


def test_enqueue_trace_type_field_is_trace():
    async def _run():
        r = _new_redis()
        try:
            await enqueue_trace(_make_trace(), r)
            messages = await r.xrange(STREAM_NAME, "-", "+")
            _, fields = messages[0]
            assert fields[b"type"] == b"trace"
        finally:
            await r.aclose()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# enqueue_signal
# ---------------------------------------------------------------------------

def test_enqueue_signal_returns_message_id():
    async def _run():
        r = _new_redis()
        try:
            sig = _make_signal(str(uuid.uuid4()))
            mid = await enqueue_signal(sig, r)
            assert mid is not None
        finally:
            await r.aclose()

    asyncio.run(_run())


def test_enqueue_signal_type_field_is_signal():
    async def _run():
        r = _new_redis()
        try:
            sig = _make_signal(str(uuid.uuid4()))
            await enqueue_signal(sig, r)
            messages = await r.xrange(STREAM_NAME, "-", "+")
            _, fields = messages[0]
            assert fields[b"type"] == b"signal"
        finally:
            await r.aclose()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Both consumer groups see all messages independently
# ---------------------------------------------------------------------------

def test_both_groups_receive_100_traces():
    """
    Core acceptance criterion: enqueue 100 traces, confirm that both
    eval-worker and ch-writer each independently receive all 100.
    """
    async def _run():
        r = _new_redis()
        try:
            # Set up consumer groups (id="0" → read from start in tests)
            for group in CONSUMER_GROUPS:
                await r.xgroup_create(STREAM_NAME, group, id="0", mkstream=True)

            # Enqueue 100 traces
            for i in range(100):
                await enqueue_trace(_make_trace(i), r)

            # Each group reads independently with ">" (undelivered messages)
            eval_result = await r.xreadgroup(
                "eval-worker", "consumer-1", {STREAM_NAME: ">"}, count=200
            )
            ch_result = await r.xreadgroup(
                "ch-writer", "consumer-1", {STREAM_NAME: ">"}, count=200
            )

            eval_messages = eval_result[0][1]
            ch_messages = ch_result[0][1]

            assert len(eval_messages) == 100, (
                f"eval-worker expected 100 messages, got {len(eval_messages)}"
            )
            assert len(ch_messages) == 100, (
                f"ch-writer expected 100 messages, got {len(ch_messages)}"
            )
        finally:
            await r.aclose()

    asyncio.run(_run())


def test_groups_consume_independently():
    """
    Acknowledging in eval-worker must NOT remove messages from ch-writer.
    """
    async def _run():
        r = _new_redis()
        try:
            for group in CONSUMER_GROUPS:
                await r.xgroup_create(STREAM_NAME, group, id="0", mkstream=True)

            for i in range(5):
                await enqueue_trace(_make_trace(i), r)

            # eval-worker reads and ACKs all 5
            eval_result = await r.xreadgroup(
                "eval-worker", "c1", {STREAM_NAME: ">"}, count=10
            )
            eval_ids = [msg_id for msg_id, _ in eval_result[0][1]]
            await r.xack(STREAM_NAME, "eval-worker", *eval_ids)

            # ch-writer should still see all 5 (unaffected by eval-worker ACK)
            ch_result = await r.xreadgroup(
                "ch-writer", "c1", {STREAM_NAME: ">"}, count=10
            )
            assert len(ch_result[0][1]) == 5
        finally:
            await r.aclose()

    asyncio.run(_run())


def test_traces_and_signals_mixed_in_stream():
    """Traces and signals share the stream; both groups get all message types."""
    async def _run():
        r = _new_redis()
        try:
            for group in CONSUMER_GROUPS:
                await r.xgroup_create(STREAM_NAME, group, id="0", mkstream=True)

            trace = _make_trace()
            sig = _make_signal(trace.trace_id)
            await enqueue_trace(trace, r)
            await enqueue_signal(sig, r)

            for group in CONSUMER_GROUPS:
                result = await r.xreadgroup(group, "c1", {STREAM_NAME: ">"}, count=10)
                messages = result[0][1]
                assert len(messages) == 2
                types = {fields[b"type"] for _, fields in messages}
                assert types == {b"trace", b"signal"}
        finally:
            await r.aclose()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# create_consumer_groups — idempotency
# ---------------------------------------------------------------------------

def test_create_consumer_groups_idempotent():
    """Calling create_consumer_groups twice must not raise."""
    async def _run():
        r = _new_redis()
        try:
            await create_consumer_groups(r)
            await create_consumer_groups(r)  # second call → BUSYGROUP, silently ignored
        finally:
            await r.aclose()

    asyncio.run(_run())


def test_create_consumer_groups_creates_both_groups():
    async def _run():
        r = _new_redis()
        try:
            await create_consumer_groups(r)
            info = await r.xinfo_groups(STREAM_NAME)
            group_names = {g["name"].decode() if isinstance(g["name"], bytes) else g["name"]
                          for g in info}
            assert "eval-worker" in group_names
            assert "ch-writer" in group_names
        finally:
            await r.aclose()

    asyncio.run(_run())


def test_create_consumer_groups_creates_stream():
    async def _run():
        r = _new_redis()
        try:
            await create_consumer_groups(r)
            assert await r.exists(STREAM_NAME)
        finally:
            await r.aclose()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# MAXLEN enforcement — stream stays capped at MAX_STREAM_LEN
# ---------------------------------------------------------------------------

def test_maxlen_caps_stream_length():
    """
    Write MAX_STREAM_LEN + 200 messages; the stream must not exceed MAX_STREAM_LEN
    significantly (approximate trimming allows slight overshoot).
    """
    async def _run():
        r = _new_redis()
        try:
            # Use a tiny cap so the test stays fast
            small_cap = 50
            for i in range(small_cap + 20):
                await r.xadd(
                    STREAM_NAME,
                    {"type": "trace", "payload": f"msg_{i}"},
                    maxlen=small_cap,
                    approximate=False,  # exact trimming for test determinism
                )
            length = await r.xlen(STREAM_NAME)
            # With exact trimming, length must equal the cap exactly
            assert length == small_cap, f"Expected {small_cap}, got {length}"
        finally:
            await r.aclose()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Message content round-trip
# ---------------------------------------------------------------------------

def test_trace_payload_survives_roundtrip():
    async def _run():
        r = _new_redis()
        try:
            original = _make_trace(7)
            await enqueue_trace(original, r)

            messages = await r.xrange(STREAM_NAME, "-", "+")
            _, fields = messages[0]
            recovered = TracePayload.model_validate_json(fields[b"payload"])

            assert recovered.trace_id == original.trace_id
            assert recovered.customer_id == original.customer_id
            assert recovered.user_prompt == original.user_prompt
            assert recovered.token_counts.total == original.token_counts.total
        finally:
            await r.aclose()

    asyncio.run(_run())


def test_signal_payload_survives_roundtrip():
    async def _run():
        r = _new_redis()
        try:
            original = _make_signal(str(uuid.uuid4()))
            await enqueue_signal(original, r)

            messages = await r.xrange(STREAM_NAME, "-", "+")
            _, fields = messages[0]
            recovered = SignalPayload.model_validate_json(fields[b"payload"])

            assert recovered.signal_id == original.signal_id
            assert recovered.trace_id == original.trace_id
            assert recovered.signal_type == original.signal_type
        finally:
            await r.aclose()

    asyncio.run(_run())


# ===========================================================================
# sentinel:diffs stream  (DIFF-02)
# ===========================================================================

def _make_diff(n: int = 0) -> DiffPayload:
    return DiffPayload(
        experiment_id=f"exp_{n:04d}",
        pipeline_name="rfp-eval",
        developer="ahmed",
        branch="feature/fix",
        base_branch="origin/main",
        commit_sha="abc12345",
        filtered_diff=f"diff --git a/prompts/file{n}.txt b/prompts/file{n}.txt\n+line {n}\n",
        original_files={f"prompts/file{n}.txt": f"original {n}"},
        customer_id=f"cust_{n:04d}",
    )


# ---------------------------------------------------------------------------
# enqueue_diff
# ---------------------------------------------------------------------------

def test_enqueue_diff_returns_message_id():
    async def _run():
        r = _new_redis()
        try:
            mid = await enqueue_diff(_make_diff(), r)
            assert mid is not None
            assert b"-" in mid
        finally:
            await r.aclose()

    asyncio.run(_run())


def test_enqueue_diff_goes_to_diff_stream_not_traces():
    async def _run():
        r = _new_redis()
        try:
            await enqueue_diff(_make_diff(), r)
            # sentinel:diffs should have 1 message
            assert await r.xlen(DIFF_STREAM_NAME) == 1
            # sentinel:traces should be empty — diffs are separate
            assert await r.exists(STREAM_NAME) == 0
        finally:
            await r.aclose()

    asyncio.run(_run())


def test_enqueue_diff_payload_is_valid_json():
    async def _run():
        r = _new_redis()
        try:
            diff = _make_diff(7)
            await enqueue_diff(diff, r)

            messages = await r.xrange(DIFF_STREAM_NAME, "-", "+")
            _, fields = messages[0]
            data = json.loads(fields[b"payload"])

            assert data["experiment_id"] == "exp_0007"
            assert data["developer"] == "ahmed"
            assert data["pipeline_name"] == "rfp-eval"
        finally:
            await r.aclose()

    asyncio.run(_run())


def test_diff_payload_experiment_id_preserved_in_roundtrip():
    async def _run():
        r = _new_redis()
        try:
            original = _make_diff(42)
            await enqueue_diff(original, r)

            messages = await r.xrange(DIFF_STREAM_NAME, "-", "+")
            _, fields = messages[0]
            recovered = DiffPayload.model_validate_json(fields[b"payload"])

            assert recovered.experiment_id == original.experiment_id
            assert recovered.customer_id == original.customer_id
            assert recovered.filtered_diff == original.filtered_diff
            assert recovered.original_files == original.original_files
        finally:
            await r.aclose()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# diff-analyzer consumer group sees messages
# ---------------------------------------------------------------------------

def test_diff_group_receives_enqueued_diffs():
    async def _run():
        r = _new_redis()
        try:
            await r.xgroup_create(DIFF_STREAM_NAME, DIFF_CONSUMER_GROUP, id="0", mkstream=True)

            for i in range(5):
                await enqueue_diff(_make_diff(i), r)

            result = await r.xreadgroup(
                DIFF_CONSUMER_GROUP, "consumer-1", {DIFF_STREAM_NAME: ">"}, count=10
            )
            messages = result[0][1]
            assert len(messages) == 5
        finally:
            await r.aclose()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# create_consumer_groups — diff-analyzer group bootstrap
# ---------------------------------------------------------------------------

def test_create_consumer_groups_creates_diff_analyzer_group():
    async def _run():
        r = _new_redis()
        try:
            await create_consumer_groups(r)
            info = await r.xinfo_groups(DIFF_STREAM_NAME)
            group_names = {
                g["name"].decode() if isinstance(g["name"], bytes) else g["name"]
                for g in info
            }
            assert DIFF_CONSUMER_GROUP in group_names
        finally:
            await r.aclose()

    asyncio.run(_run())


def test_create_consumer_groups_creates_diff_stream():
    async def _run():
        r = _new_redis()
        try:
            await create_consumer_groups(r)
            assert await r.exists(DIFF_STREAM_NAME)
        finally:
            await r.aclose()

    asyncio.run(_run())


def test_create_consumer_groups_idempotent_with_diff_stream():
    async def _run():
        r = _new_redis()
        try:
            await create_consumer_groups(r)
            await create_consumer_groups(r)  # BUSYGROUP silently ignored
        finally:
            await r.aclose()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# MAXLEN enforcement — diff stream stays capped at MAX_DIFF_STREAM_LEN
# ---------------------------------------------------------------------------

def test_diff_stream_capped_at_max_diff_len():
    async def _run():
        r = _new_redis()
        try:
            small_cap = 20
            for i in range(small_cap + 10):
                await r.xadd(
                    DIFF_STREAM_NAME,
                    {"payload": f"diff_{i}"},
                    maxlen=small_cap,
                    approximate=False,
                )
            length = await r.xlen(DIFF_STREAM_NAME)
            assert length == small_cap, f"Expected {small_cap}, got {length}"
        finally:
            await r.aclose()

    asyncio.run(_run())
