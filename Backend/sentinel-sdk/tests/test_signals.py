"""
tests/test_signals.py

Tests for the behavioral signal helper methods (SDK-03).
Run with: pytest tests/test_signals.py -v
"""

import sys
import os
import json
from datetime import timezone
from unittest.mock import MagicMock, call

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from schema import SignalPayload, SignalType
from client import SentinelClient

_SIGNAL_ENDPOINT = "/v1/signals"
_TRACE_ID = "f47ac10b-58cc-4372-a567-0e02b2c3d479"
_CUSTOMER_ID = "cust_test123"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_shipper():
    return MagicMock()


@pytest.fixture
def sentinel(mock_shipper):
    return SentinelClient(
        api_key="sk_test",
        customer_id=_CUSTOMER_ID,
        _shipper=mock_shipper,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def captured_payload(mock_shipper) -> dict:
    """Extract the payload dict that was passed to shipper.enqueue."""
    assert mock_shipper.enqueue.call_count == 1
    _endpoint, payload = mock_shipper.enqueue.call_args[0]
    return payload


def captured_endpoint(mock_shipper) -> str:
    assert mock_shipper.enqueue.call_count == 1
    endpoint, _payload = mock_shipper.enqueue.call_args[0]
    return endpoint


# ---------------------------------------------------------------------------
# Each method sends the correct signal type
# ---------------------------------------------------------------------------

class TestSignalMethods:

    def test_thumbs_down_sends_correct_type(self, sentinel, mock_shipper):
        sentinel.thumbs_down(_TRACE_ID)
        payload = captured_payload(mock_shipper)
        assert payload["signal_type"] == SignalType.THUMBS_DOWN.value

    def test_escalate_sends_correct_type(self, sentinel, mock_shipper):
        sentinel.escalate(_TRACE_ID)
        payload = captured_payload(mock_shipper)
        assert payload["signal_type"] == SignalType.ESCALATE.value

    def test_rephrased_sends_correct_type(self, sentinel, mock_shipper):
        sentinel.rephrased(_TRACE_ID)
        payload = captured_payload(mock_shipper)
        assert payload["signal_type"] == SignalType.REPHRASED.value

    def test_abandoned_sends_correct_type(self, sentinel, mock_shipper):
        sentinel.abandoned(_TRACE_ID)
        payload = captured_payload(mock_shipper)
        assert payload["signal_type"] == SignalType.ABANDONED.value

    def test_acknowledged_sends_correct_type(self, sentinel, mock_shipper):
        sentinel.acknowledged(_TRACE_ID)
        payload = captured_payload(mock_shipper)
        assert payload["signal_type"] == SignalType.ACKNOWLEDGED.value


# ---------------------------------------------------------------------------
# Payload structure — contract with the ingest API
# ---------------------------------------------------------------------------

class TestSignalPayloadStructure:

    def test_payload_contains_trace_id(self, sentinel, mock_shipper):
        sentinel.thumbs_down(_TRACE_ID)
        payload = captured_payload(mock_shipper)
        assert payload["trace_id"] == _TRACE_ID

    def test_payload_contains_customer_id(self, sentinel, mock_shipper):
        sentinel.thumbs_down(_TRACE_ID)
        payload = captured_payload(mock_shipper)
        assert payload["customer_id"] == _CUSTOMER_ID

    def test_payload_has_auto_generated_signal_id(self, sentinel, mock_shipper):
        sentinel.thumbs_down(_TRACE_ID)
        payload = captured_payload(mock_shipper)
        assert "signal_id" in payload
        assert len(payload["signal_id"]) == 36  # UUID format

    def test_two_signals_get_unique_ids(self, sentinel, mock_shipper):
        sentinel.thumbs_down(_TRACE_ID)
        sentinel.thumbs_down(_TRACE_ID)
        calls = mock_shipper.enqueue.call_args_list
        id_1 = calls[0][0][1]["signal_id"]
        id_2 = calls[1][0][1]["signal_id"]
        assert id_1 != id_2

    def test_payload_has_utc_timestamp(self, sentinel, mock_shipper):
        sentinel.thumbs_down(_TRACE_ID)
        payload = captured_payload(mock_shipper)
        assert "timestamp" in payload
        # timestamp is serialized as an ISO string with timezone info
        ts_str = payload["timestamp"]
        assert isinstance(ts_str, str)
        assert "+" in ts_str or ts_str.endswith("Z")

    def test_payload_validates_as_signal_payload(self, sentinel, mock_shipper):
        sentinel.escalate(_TRACE_ID)
        payload = captured_payload(mock_shipper)
        # Round-trip through the schema to confirm ingest API can parse it
        reconstructed = SignalPayload.model_validate(payload)
        assert reconstructed.trace_id == _TRACE_ID
        assert reconstructed.signal_type == SignalType.ESCALATE

    def test_payload_serializes_to_json(self, sentinel, mock_shipper):
        sentinel.rephrased(_TRACE_ID)
        payload = captured_payload(mock_shipper)
        # The shipper will json.dumps this — it must not raise
        json_str = json.dumps(payload)
        parsed = json.loads(json_str)
        assert parsed["signal_type"] == "rephrased"

    def test_signals_sent_to_correct_endpoint(self, sentinel, mock_shipper):
        sentinel.thumbs_down(_TRACE_ID)
        endpoint = captured_endpoint(mock_shipper)
        assert endpoint == _SIGNAL_ENDPOINT


# ---------------------------------------------------------------------------
# Non-blocking — enqueue() is called, not a blocking HTTP call
# ---------------------------------------------------------------------------

class TestNonBlocking:

    def test_signal_calls_enqueue_not_blocking_send(self, sentinel, mock_shipper):
        # If enqueue() is called, dispatch is async (shipper owns the I/O).
        # If a hypothetical blocking _post() were called directly, this test would fail.
        sentinel.thumbs_down(_TRACE_ID)
        mock_shipper.enqueue.assert_called_once()

    def test_all_five_methods_use_enqueue(self, sentinel, mock_shipper):
        sentinel.thumbs_down(_TRACE_ID)
        sentinel.escalate(_TRACE_ID)
        sentinel.rephrased(_TRACE_ID)
        sentinel.abandoned(_TRACE_ID)
        sentinel.acknowledged(_TRACE_ID)
        assert mock_shipper.enqueue.call_count == 5


# ---------------------------------------------------------------------------
# AsyncShipper — unit tests (no real HTTP)
# ---------------------------------------------------------------------------

class TestAsyncShipper:

    def test_enqueue_returns_immediately(self):
        """enqueue() must not block even if no worker is consuming."""
        from shipper import AsyncShipper
        import unittest.mock

        shipper = AsyncShipper(base_url="http://localhost:9999", api_key="sk_test")
        # Patch _post so no real HTTP happens and flush() can complete
        with unittest.mock.patch.object(shipper, "_post"):
            shipper.enqueue(_SIGNAL_ENDPOINT, {"signal_type": "thumbs_down"})
            shipper.flush()  # confirm item was processed

    def test_failed_delivery_does_not_raise(self):
        """A network error must be silently swallowed — never crash the caller."""
        from shipper import AsyncShipper

        # base_url points nowhere — _post will raise URLError
        shipper = AsyncShipper(base_url="http://localhost:1", api_key="sk_test", timeout=1)
        shipper.enqueue(_SIGNAL_ENDPOINT, {"signal_type": "escalate"})
        shipper.flush()  # should complete without raising
