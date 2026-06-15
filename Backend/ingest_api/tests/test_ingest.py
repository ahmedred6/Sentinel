"""
Tests for backend/ingest_api/main.py — endpoint behaviour.

Auth is bypassed via app.dependency_overrides so these tests focus
on payload validation, dedup, and response shape. Auth-specific tests
live in test_auth.py.
"""

import json
import time
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from auth import require_auth
from dependencies import get_redis
from main import app, _reset_dedup
from streams import DIFF_STREAM_NAME

client = TestClient(app)

VALID_TRACE = {
    "customer_id": "cust_test",
    "session_id": "sess_001",
    "flow_name": "support",
    "model_name": "gpt-4",
    "user_prompt": "Hello",
    "llm_response": "Hi there!",
    "latency_ms": 100,
    "token_counts": {"prompt": 10, "completion": 5, "total": 15},
}

VALID_SIGNAL = {
    "trace_id": "00000000-0000-0000-0000-000000000001",
    "customer_id": "cust_test",
    "signal_type": "thumbs_down",
}

VALID_DIFF = {
    "experiment_id": "exp_abc123",
    "pipeline_name": "rfp-eval",
    "developer": "ahmed",
    "branch": "feature/fix",
    "base_branch": "origin/main",
    "commit_sha": "abc12345",
    "filtered_diff": "diff --git a/prompts/rubric.txt b/prompts/rubric.txt\n+new line\n",
    "original_files": {"prompts/rubric.txt": "old content"},
    "customer_id": "cust_test",
}


@pytest.fixture(autouse=True)
def reset_state():
    """Bypass auth and redis for every test. Focus: payload validation / dedup / response shape."""
    async def _mock_auth() -> str:
        return "cust_test"

    mock_redis = MagicMock()
    mock_redis.xadd = AsyncMock(return_value=b"12345-0")

    async def _mock_get_redis():
        return mock_redis

    app.dependency_overrides[require_auth] = _mock_auth
    app.dependency_overrides[get_redis] = _mock_get_redis
    _reset_dedup()
    yield
    _reset_dedup()
    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

def test_valid_trace_returns_200():
    r = client.post("/ingest/trace", json=VALID_TRACE)
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert "trace_id" in body


def test_valid_signal_returns_200():
    r = client.post("/ingest/signal", json=VALID_SIGNAL)
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["trace_id"] == VALID_SIGNAL["trace_id"]


def test_response_body_echoes_trace_id():
    fixed_id = "aaaabbbb-0000-0000-0000-000000000001"
    r = client.post("/ingest/trace", json={**VALID_TRACE, "trace_id": fixed_id})
    assert r.json()["trace_id"] == fixed_id


def test_health_endpoint():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


# ---------------------------------------------------------------------------
# Validation failures → 422
# ---------------------------------------------------------------------------

def test_empty_body_returns_422():
    r = client.post("/ingest/trace", json={})
    assert r.status_code == 422


def test_missing_required_fields_returns_422():
    r = client.post("/ingest/trace", json={"customer_id": "x"})
    assert r.status_code == 422


def test_invalid_signal_type_returns_422():
    bad = {**VALID_SIGNAL, "signal_type": "not_a_real_signal"}
    r = client.post("/ingest/signal", json=bad)
    assert r.status_code == 422


def test_negative_latency_returns_422():
    r = client.post("/ingest/trace", json={**VALID_TRACE, "latency_ms": -1})
    assert r.status_code == 422


def test_token_count_mismatch_returns_422():
    bad_counts = {"prompt": 10, "completion": 5, "total": 99}
    r = client.post("/ingest/trace", json={**VALID_TRACE, "token_counts": bad_counts})
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# Dedup — duplicate trace_id silently dropped
# ---------------------------------------------------------------------------

def test_duplicate_trace_silently_dropped():
    fixed_id = "00000000-0000-0000-0000-000000000099"
    trace = {**VALID_TRACE, "trace_id": fixed_id}
    r1 = client.post("/ingest/trace", json=trace)
    assert r1.status_code == 200
    r2 = client.post("/ingest/trace", json=trace)
    assert r2.status_code == 200
    assert r2.json()["trace_id"] == fixed_id


def test_different_trace_ids_both_accepted():
    r1 = client.post("/ingest/trace", json={**VALID_TRACE, "trace_id": "aaaa-0001"})
    r2 = client.post("/ingest/trace", json={**VALID_TRACE, "trace_id": "aaaa-0002"})
    assert r1.status_code == 200
    assert r2.status_code == 200


# ---------------------------------------------------------------------------
# Performance — under 50ms
# ---------------------------------------------------------------------------

def test_trace_response_under_50ms():
    t0 = time.monotonic()
    client.post("/ingest/trace", json=VALID_TRACE)
    elapsed_ms = (time.monotonic() - t0) * 1000
    assert elapsed_ms < 50, f"Response took {elapsed_ms:.1f}ms — limit is 50ms"


def test_signal_response_under_50ms():
    t0 = time.monotonic()
    client.post("/ingest/signal", json=VALID_SIGNAL)
    elapsed_ms = (time.monotonic() - t0) * 1000
    assert elapsed_ms < 50, f"Response took {elapsed_ms:.1f}ms — limit is 50ms"


# ---------------------------------------------------------------------------
# POST /ingest/diff — happy path
# ---------------------------------------------------------------------------

def test_valid_diff_returns_200():
    r = client.post("/ingest/diff", json=VALID_DIFF)
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert "experiment_id" in body


def test_diff_response_echoes_experiment_id():
    r = client.post("/ingest/diff", json=VALID_DIFF)
    assert r.json()["experiment_id"] == VALID_DIFF["experiment_id"]


def test_diff_with_only_required_field_returns_200():
    r = client.post("/ingest/diff", json={"customer_id": "cust_test"})
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


# ---------------------------------------------------------------------------
# POST /ingest/diff — validation failures → 422
# ---------------------------------------------------------------------------

def test_diff_empty_body_returns_422():
    r = client.post("/ingest/diff", json={})
    assert r.status_code == 422


def test_diff_missing_customer_id_returns_422():
    payload = {k: v for k, v in VALID_DIFF.items() if k != "customer_id"}
    r = client.post("/ingest/diff", json=payload)
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# POST /ingest/diff — auth applied same as /ingest/trace
# ---------------------------------------------------------------------------

def test_diff_auth_dependency_enforced():
    """Verify /ingest/diff routes through require_auth — a rejecting auth returns 403."""
    from fastapi import HTTPException

    async def _deny() -> str:
        raise HTTPException(status_code=403, detail="Forbidden")

    app.dependency_overrides[require_auth] = _deny
    r = client.post("/ingest/diff", json=VALID_DIFF)
    assert r.status_code == 403


# ---------------------------------------------------------------------------
# POST /ingest/diff — enqueue behavior
# ---------------------------------------------------------------------------

def test_diff_calls_redis_xadd():
    mock_redis = MagicMock()
    mock_redis.xadd = AsyncMock(return_value=b"12345-0")

    async def _get_mock():
        return mock_redis

    app.dependency_overrides[get_redis] = _get_mock
    r = client.post("/ingest/diff", json=VALID_DIFF)
    assert r.status_code == 200
    mock_redis.xadd.assert_called_once()


# ---------------------------------------------------------------------------
# POST /ingest/diff — integration: endpoint → correct Redis stream
# ---------------------------------------------------------------------------

def test_integration_diff_enqueues_to_correct_stream():
    """Integration: valid DiffPayload → endpoint → payload enqueued to sentinel:diffs."""
    xadd_calls: list[tuple] = []

    async def _capture_xadd(stream, fields, **kwargs):
        xadd_calls.append((stream, fields))
        return b"1234-0"

    mock_redis = MagicMock()
    mock_redis.xadd = _capture_xadd

    async def _get_mock():
        return mock_redis

    app.dependency_overrides[get_redis] = _get_mock

    r = client.post("/ingest/diff", json=VALID_DIFF)

    assert r.status_code == 200
    assert len(xadd_calls) == 1

    stream_name, fields = xadd_calls[0]
    assert stream_name == DIFF_STREAM_NAME, (
        f"Expected stream {DIFF_STREAM_NAME!r}, got {stream_name!r}"
    )

    data = json.loads(fields["payload"])
    assert data["experiment_id"] == VALID_DIFF["experiment_id"]
    assert data["developer"] == VALID_DIFF["developer"]
    assert data["pipeline_name"] == VALID_DIFF["pipeline_name"]


# ---------------------------------------------------------------------------
# POST /ingest/diff — performance
# ---------------------------------------------------------------------------

def test_diff_response_under_50ms():
    t0 = time.monotonic()
    client.post("/ingest/diff", json=VALID_DIFF)
    elapsed_ms = (time.monotonic() - t0) * 1000
    assert elapsed_ms < 50, f"Response took {elapsed_ms:.1f}ms — limit is 50ms"
