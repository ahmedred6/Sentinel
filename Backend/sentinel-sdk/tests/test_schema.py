"""
tests/test_schema.py

Unit tests for the Sentinel SDK trace schema.
Run with: pytest tests/test_schema.py -v
"""

import json
import sys
import os
import pytest
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from schema import (
    TracePayload,
    SignalPayload,
    SignalType,
    Environment,
    ContextDocument,
    ToolCall,
    TokenCounts,
    MessageTurn,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def minimal_trace(**overrides) -> dict:
    """Returns the minimum valid TracePayload dict, with optional overrides."""
    base = {
        "customer_id": "cust_test123",
        "session_id": "sess_test456",
        "flow_name": "support",
        "model_name": "gpt-4.1",
        "user_prompt": "Can I get a refund after 40 days?",
        "llm_response": "Yes, you can request a refund.",
        "latency_ms": 843,
        "token_counts": {"prompt": 312, "completion": 18, "total": 330},
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# TracePayload — happy path
# ---------------------------------------------------------------------------

class TestTracePayloadHappyPath:

    def test_minimal_valid_trace(self):
        trace = TracePayload(**minimal_trace())
        assert trace.customer_id == "cust_test123"
        assert trace.flow_name == "support"
        assert trace.environment == Environment.PROD  # default

    def test_trace_id_auto_generated(self):
        trace = TracePayload(**minimal_trace())
        assert trace.trace_id is not None
        assert len(trace.trace_id) == 36  # UUID format

    def test_two_traces_get_unique_ids(self):
        t1 = TracePayload(**minimal_trace())
        t2 = TracePayload(**minimal_trace())
        assert t1.trace_id != t2.trace_id

    def test_timestamp_is_utc(self):
        trace = TracePayload(**minimal_trace())
        assert trace.timestamp.tzinfo == timezone.utc

    def test_empty_lists_default_correctly(self):
        trace = TracePayload(**minimal_trace())
        assert trace.message_history == []
        assert trace.retrieved_context == []
        assert trace.tool_calls == []

    def test_full_trace_with_all_fields(self):
        trace = TracePayload(
            customer_id="cust_abc",
            session_id="sess_xyz",
            user_id="user_42",
            flow_name="support",
            environment=Environment.STAGING,
            model_name="gpt-4.1",
            model_version="2025-04-14",
            user_prompt="Can I get a refund after 40 days?",
            message_history=[
                MessageTurn(role="user", content="Hi I need help"),
                MessageTurn(role="assistant", content="Of course!"),
            ],
            retrieved_context=[
                ContextDocument(
                    content="Refunds are only allowed within 30 days.",
                    source="refund_policy_v2.pdf",
                    score=0.94,
                )
            ],
            llm_response="Yes, you can request a refund.",
            tool_calls=[
                ToolCall(
                    tool_name="lookup_order",
                    input={"order_id": "ORD-999"},
                    output={"status": "delivered"},
                    latency_ms=120,
                )
            ],
            latency_ms=843,
            token_counts=TokenCounts(prompt=312, completion=18, total=330),
        )
        assert trace.user_id == "user_42"
        assert trace.environment == Environment.STAGING
        assert len(trace.retrieved_context) == 1
        assert len(trace.tool_calls) == 1


# ---------------------------------------------------------------------------
# JSON serialization — critical for ingest API contract
# ---------------------------------------------------------------------------

class TestTraceSerialization:

    def test_serializes_to_json_string(self):
        trace = TracePayload(**minimal_trace())
        json_str = trace.model_dump_json()
        assert isinstance(json_str, str)
        parsed = json.loads(json_str)
        assert parsed["customer_id"] == "cust_test123"

    def test_round_trips_through_json(self):
        original = TracePayload(**minimal_trace())
        json_str = original.model_dump_json()
        restored = TracePayload.model_validate_json(json_str)
        assert restored.trace_id == original.trace_id
        assert restored.customer_id == original.customer_id
        assert restored.llm_response == original.llm_response

    def test_serialized_json_has_all_required_keys(self):
        trace = TracePayload(**minimal_trace())
        data = trace.model_dump()
        required_keys = [
            "trace_id", "customer_id", "session_id", "flow_name",
            "environment", "model_name", "user_prompt", "llm_response",
            "latency_ms", "token_counts", "timestamp",
            "message_history", "retrieved_context", "tool_calls",
        ]
        for key in required_keys:
            assert key in data, f"Missing key: {key}"

    def test_datetime_serializes_as_iso_string(self):
        trace = TracePayload(**minimal_trace())
        data = json.loads(trace.model_dump_json())
        ts = data["timestamp"]
        # Should be parseable as ISO 8601
        parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        assert parsed.tzinfo is not None


# ---------------------------------------------------------------------------
# TokenCounts validation
# ---------------------------------------------------------------------------

class TestTokenCounts:

    def test_valid_token_counts(self):
        tc = TokenCounts(prompt=100, completion=50, total=150)
        assert tc.total == 150

    def test_total_mismatch_raises(self):
        with pytest.raises(Exception):
            TokenCounts(prompt=100, completion=50, total=999)

    def test_zero_completion_valid(self):
        tc = TokenCounts(prompt=50, completion=0, total=50)
        assert tc.completion == 0


# ---------------------------------------------------------------------------
# Validation — missing required fields
# ---------------------------------------------------------------------------

class TestTracePayloadValidation:

    def test_missing_customer_id_raises(self):
        data = minimal_trace()
        del data["customer_id"]
        with pytest.raises(Exception):
            TracePayload(**data)

    def test_missing_llm_response_raises(self):
        data = minimal_trace()
        del data["llm_response"]
        with pytest.raises(Exception):
            TracePayload(**data)

    def test_negative_latency_raises(self):
        with pytest.raises(Exception):
            TracePayload(**minimal_trace(latency_ms=-1))

    def test_invalid_environment_raises(self):
        with pytest.raises(Exception):
            TracePayload(**minimal_trace(environment="production"))  # must be "prod"


# ---------------------------------------------------------------------------
# ContextDocument
# ---------------------------------------------------------------------------

class TestContextDocument:

    def test_valid_context_doc(self):
        doc = ContextDocument(
            content="Refunds allowed within 30 days.",
            source="policy.pdf",
            score=0.92,
        )
        assert doc.score == 0.92

    def test_score_is_optional(self):
        doc = ContextDocument(content="Some policy text.", source="kb://article-1")
        assert doc.score is None


# ---------------------------------------------------------------------------
# ToolCall
# ---------------------------------------------------------------------------

class TestToolCall:

    def test_valid_tool_call(self):
        tc = ToolCall(
            tool_name="lookup_order",
            input={"order_id": "ORD-123"},
            output={"status": "delivered"},
        )
        assert tc.error is None

    def test_failed_tool_call(self):
        tc = ToolCall(
            tool_name="lookup_order",
            input={"order_id": "ORD-999"},
            output=None,
            error="Order not found",
        )
        assert tc.error == "Order not found"
        assert tc.output is None


# ---------------------------------------------------------------------------
# SignalPayload
# ---------------------------------------------------------------------------

class TestSignalPayload:

    def test_thumbs_down_signal(self):
        sig = SignalPayload(
            trace_id="some-trace-uuid",
            customer_id="cust_abc",
            signal_type=SignalType.THUMBS_DOWN,
        )
        assert sig.signal_type == SignalType.THUMBS_DOWN
        assert sig.signal_id is not None

    def test_all_signal_types_valid(self):
        for signal_type in SignalType:
            sig = SignalPayload(
                trace_id="trace-id",
                customer_id="cust_abc",
                signal_type=signal_type,
            )
            assert sig.signal_type == signal_type

    def test_signal_serializes_to_json(self):
        sig = SignalPayload(
            trace_id="trace-id",
            customer_id="cust_abc",
            signal_type=SignalType.ESCALATE,
        )
        data = json.loads(sig.model_dump_json())
        assert data["signal_type"] == "escalate"
        assert data["trace_id"] == "trace-id"