"""
tests/test_sdk.py

SDK-02 acceptance tests.
Run with: pytest tests/test_sdk.py -v
"""

import sys
import os
import json
import time
import threading
import unittest.mock
from unittest.mock import MagicMock, patch, call
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from schema import TokenCounts, Environment, TracePayload, SignalType
from client import SentinelClient
from context import SentinelTraceContext
from shipper import AsyncShipper

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

_CUSTOMER_ID = "cust_test123"
_SESSION_ID = "sess_test456"
_TRACE_ENDPOINT = "/v1/traces"
_SIGNAL_ENDPOINT = "/v1/signals"

_SAMPLE_MESSAGES = [
    {"role": "user", "content": "Hi, I need help with my order."},
    {"role": "assistant", "content": "Of course! What can I help you with?"},
    {"role": "user", "content": "Can I get a refund after 40 days?"},
]

_SAMPLE_TOKEN_COUNTS = TokenCounts(prompt=312, completion=18, total=330)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_shipper():
    return MagicMock()


@pytest.fixture
def client(mock_shipper):
    return SentinelClient(
        api_key="sk_test",
        customer_id=_CUSTOMER_ID,
        _shipper=mock_shipper,
    )


def _make_trace(client, **kwargs) -> SentinelTraceContext:
    return client.trace(flow="support", session_id=_SESSION_ID, **kwargs)


def _inject(ctx: SentinelTraceContext, **overrides) -> None:
    """Simulate a captured LLM call inside a trace context."""
    ctx.manual(
        user_prompt=overrides.get("user_prompt", "Can I get a refund after 40 days?"),
        llm_response=overrides.get("llm_response", "Yes, you can request a refund."),
        model_name=overrides.get("model_name", "gpt-4.1"),
        token_counts=overrides.get("token_counts", _SAMPLE_TOKEN_COUNTS),
        latency_ms=overrides.get("latency_ms", 843),
    )


def _captured_payload(mock_shipper) -> dict:
    assert mock_shipper.enqueue.call_count == 1
    _ep, payload = mock_shipper.enqueue.call_args[0]
    return payload


# ---------------------------------------------------------------------------
# Context manager — happy path
# ---------------------------------------------------------------------------

class TestContextManagerHappyPath:

    def test_context_manager_ships_trace_on_exit(self, client, mock_shipper):
        with _make_trace(client) as trace:
            _inject(trace)
        mock_shipper.enqueue.assert_called_once()

    def test_trace_sent_to_correct_endpoint(self, client, mock_shipper):
        with _make_trace(client) as trace:
            _inject(trace)
        endpoint, _ = mock_shipper.enqueue.call_args[0]
        assert endpoint == _TRACE_ENDPOINT

    def test_context_manager_returns_trace_context(self, client):
        with _make_trace(client) as trace:
            assert isinstance(trace, SentinelTraceContext)

    def test_no_llm_call_ships_nothing(self, client, mock_shipper):
        with _make_trace(client):
            pass  # no manual() and no interceptor fires
        mock_shipper.enqueue.assert_not_called()

    def test_exception_inside_with_block_still_ships(self, client, mock_shipper):
        with pytest.raises(ValueError):
            with _make_trace(client) as trace:
                _inject(trace)
                raise ValueError("app error")
        # trace was captured before the raise, so it should ship
        mock_shipper.enqueue.assert_called_once()

    def test_exception_inside_with_block_propagates(self, client, mock_shipper):
        with pytest.raises(RuntimeError, match="propagated"):
            with _make_trace(client) as trace:
                raise RuntimeError("propagated")


# ---------------------------------------------------------------------------
# All TracePayload fields are populated
# ---------------------------------------------------------------------------

class TestTracePayloadFields:

    def test_customer_id_set_from_client(self, client, mock_shipper):
        with _make_trace(client) as trace:
            _inject(trace)
        payload = _captured_payload(mock_shipper)
        assert payload["customer_id"] == _CUSTOMER_ID

    def test_session_id_passed_through(self, client, mock_shipper):
        with _make_trace(client) as trace:
            _inject(trace)
        payload = _captured_payload(mock_shipper)
        assert payload["session_id"] == _SESSION_ID

    def test_flow_name_set_correctly(self, client, mock_shipper):
        with client.trace(flow="billing", session_id=_SESSION_ID) as trace:
            _inject(trace)
        payload = _captured_payload(mock_shipper)
        assert payload["flow_name"] == "billing"

    def test_user_id_passed_through(self, client, mock_shipper):
        with _make_trace(client, user_id="user_42") as trace:
            _inject(trace)
        payload = _captured_payload(mock_shipper)
        assert payload["user_id"] == "user_42"

    def test_environment_defaults_to_prod(self, client, mock_shipper):
        with _make_trace(client) as trace:
            _inject(trace)
        payload = _captured_payload(mock_shipper)
        assert payload["environment"] == "prod"

    def test_environment_can_be_overridden(self, client, mock_shipper):
        with _make_trace(client, environment=Environment.STAGING) as trace:
            _inject(trace)
        payload = _captured_payload(mock_shipper)
        assert payload["environment"] == "staging"

    def test_trace_id_is_a_uuid(self, client, mock_shipper):
        with _make_trace(client) as trace:
            _inject(trace)
        payload = _captured_payload(mock_shipper)
        assert len(payload["trace_id"]) == 36
        assert payload["trace_id"].count("-") == 4

    def test_two_traces_get_unique_ids(self, client, mock_shipper):
        with _make_trace(client) as t1:
            _inject(t1)
        with _make_trace(client) as t2:
            _inject(t2)
        calls = mock_shipper.enqueue.call_args_list
        id1 = calls[0][0][1]["trace_id"]
        id2 = calls[1][0][1]["trace_id"]
        assert id1 != id2

    def test_session_id_auto_generated_when_omitted(self, client, mock_shipper):
        with client.trace(flow="support") as trace:
            _inject(trace)
        payload = _captured_payload(mock_shipper)
        assert payload["session_id"] is not None
        assert len(payload["session_id"]) == 36

    def test_user_prompt_captured(self, client, mock_shipper):
        with _make_trace(client) as trace:
            _inject(trace, user_prompt="What is the return policy?")
        payload = _captured_payload(mock_shipper)
        assert payload["user_prompt"] == "What is the return policy?"

    def test_llm_response_captured(self, client, mock_shipper):
        with _make_trace(client) as trace:
            _inject(trace, llm_response="Returns are accepted within 30 days.")
        payload = _captured_payload(mock_shipper)
        assert payload["llm_response"] == "Returns are accepted within 30 days."

    def test_token_counts_captured(self, client, mock_shipper):
        with _make_trace(client) as trace:
            _inject(trace, token_counts=TokenCounts(prompt=100, completion=50, total=150))
        payload = _captured_payload(mock_shipper)
        assert payload["token_counts"]["prompt"] == 100
        assert payload["token_counts"]["completion"] == 50
        assert payload["token_counts"]["total"] == 150

    def test_latency_ms_captured(self, client, mock_shipper):
        with _make_trace(client) as trace:
            _inject(trace, latency_ms=512)
        payload = _captured_payload(mock_shipper)
        assert payload["latency_ms"] == 512

    def test_model_name_captured(self, client, mock_shipper):
        with _make_trace(client) as trace:
            _inject(trace, model_name="claude-sonnet-4-6")
        payload = _captured_payload(mock_shipper)
        assert payload["model_name"] == "claude-sonnet-4-6"

    def test_payload_validates_as_trace_payload(self, client, mock_shipper):
        with _make_trace(client) as trace:
            _inject(trace)
        payload = _captured_payload(mock_shipper)
        restored = TracePayload.model_validate(payload)
        assert restored.customer_id == _CUSTOMER_ID

    def test_payload_round_trips_through_json(self, client, mock_shipper):
        with _make_trace(client) as trace:
            _inject(trace)
        payload = _captured_payload(mock_shipper)
        json_str = json.dumps(payload)
        parsed = json.loads(json_str)
        assert parsed["flow_name"] == "support"


# ---------------------------------------------------------------------------
# Decorator pattern
# ---------------------------------------------------------------------------

class TestDecoratorPattern:

    def test_decorator_ships_trace(self, client, mock_shipper):
        @_make_trace(client)
        def handler():
            ctx = _find_active_context()
            _inject(ctx)

        handler()
        mock_shipper.enqueue.assert_called_once()

    def test_decorated_function_return_value_preserved(self, client, mock_shipper):
        @_make_trace(client)
        def handler():
            ctx = _find_active_context()
            _inject(ctx)
            return 42

        assert handler() == 42

    def test_decorator_creates_fresh_context_per_call(self, client, mock_shipper):
        @_make_trace(client)
        def handler():
            ctx = _find_active_context()
            _inject(ctx)

        handler()
        handler()
        assert mock_shipper.enqueue.call_count == 2
        id1 = mock_shipper.enqueue.call_args_list[0][0][1]["trace_id"]
        id2 = mock_shipper.enqueue.call_args_list[1][0][1]["trace_id"]
        assert id1 != id2

    def test_functools_wraps_preserves_name(self, client):
        @_make_trace(client)
        def my_handler():
            pass

        assert my_handler.__name__ == "my_handler"


def _find_active_context():
    """Helper: get the currently active SentinelTraceContext from interceptors."""
    from interceptors import get_active
    return get_active()


# ---------------------------------------------------------------------------
# OpenAI / Anthropic interceptor callbacks
# ---------------------------------------------------------------------------

class TestInterceptorCallbacks:
    """Test _record_openai and _record_anthropic by calling them directly."""

    def test_record_openai_extracts_user_prompt(self, client, mock_shipper):
        with _make_trace(client) as trace:
            openai_response = _mock_openai_response("Refunds within 30 days.", 312, 18)
            trace._record_openai(
                {"model": "gpt-4.1", "messages": _SAMPLE_MESSAGES},
                openai_response,
                843,
            )
        payload = _captured_payload(mock_shipper)
        assert payload["user_prompt"] == "Can I get a refund after 40 days?"

    def test_record_openai_extracts_llm_response(self, client, mock_shipper):
        with _make_trace(client) as trace:
            openai_response = _mock_openai_response("Refunds within 30 days.", 312, 18)
            trace._record_openai(
                {"model": "gpt-4.1", "messages": _SAMPLE_MESSAGES},
                openai_response,
                843,
            )
        payload = _captured_payload(mock_shipper)
        assert payload["llm_response"] == "Refunds within 30 days."

    def test_record_openai_captures_token_counts(self, client, mock_shipper):
        with _make_trace(client) as trace:
            openai_response = _mock_openai_response("OK.", 100, 5)
            trace._record_openai(
                {"model": "gpt-4.1", "messages": _SAMPLE_MESSAGES},
                openai_response,
                200,
            )
        payload = _captured_payload(mock_shipper)
        assert payload["token_counts"]["prompt"] == 100
        assert payload["token_counts"]["completion"] == 5
        assert payload["token_counts"]["total"] == 105

    def test_record_anthropic_extracts_user_prompt(self, client, mock_shipper):
        with _make_trace(client) as trace:
            anthropic_response = _mock_anthropic_response("Policy text.", 200, 30)
            trace._record_anthropic(
                {"model": "claude-sonnet-4-6", "messages": _SAMPLE_MESSAGES},
                anthropic_response,
                500,
            )
        payload = _captured_payload(mock_shipper)
        assert payload["user_prompt"] == "Can I get a refund after 40 days?"

    def test_record_anthropic_captures_token_counts(self, client, mock_shipper):
        with _make_trace(client) as trace:
            anthropic_response = _mock_anthropic_response("Policy text.", 200, 30)
            trace._record_anthropic(
                {"model": "claude-sonnet-4-6", "messages": _SAMPLE_MESSAGES},
                anthropic_response,
                500,
            )
        payload = _captured_payload(mock_shipper)
        assert payload["token_counts"]["prompt"] == 200
        assert payload["token_counts"]["completion"] == 30
        assert payload["token_counts"]["total"] == 230

    def test_graceful_on_malformed_openai_response(self, client, mock_shipper):
        with _make_trace(client) as trace:
            trace._record_openai(
                {"model": "gpt-4.1", "messages": _SAMPLE_MESSAGES},
                SimpleNamespace(),  # missing all expected attributes
                100,
            )
        payload = _captured_payload(mock_shipper)
        assert payload["llm_response"] == ""
        assert payload["token_counts"]["total"] == 0


def _mock_openai_response(text: str, prompt_tokens: int, completion_tokens: int):
    return SimpleNamespace(
        model="gpt-4.1",
        choices=[SimpleNamespace(message=SimpleNamespace(content=text))],
        usage=SimpleNamespace(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=prompt_tokens + completion_tokens,
        ),
    )


def _mock_anthropic_response(text: str, input_tokens: int, output_tokens: int):
    return SimpleNamespace(
        model="claude-sonnet-4-6",
        content=[SimpleNamespace(text=text)],
        usage=SimpleNamespace(input_tokens=input_tokens, output_tokens=output_tokens),
    )


# ---------------------------------------------------------------------------
# init() — background thread starts on first call
# ---------------------------------------------------------------------------

class TestInit:

    def test_init_returns_sentinel_client(self):
        client = SentinelClient.init(
            api_key="sk_test",
            customer_id=_CUSTOMER_ID,
            base_url="http://localhost:9999",
        )
        assert isinstance(client, SentinelClient)

    def test_init_starts_background_thread(self):
        before = {t.name for t in threading.enumerate()}
        SentinelClient.init(
            api_key="sk_test",
            customer_id=_CUSTOMER_ID,
            base_url="http://localhost:9999",
        )
        after = {t.name for t in threading.enumerate()}
        assert "sentinel-shipper" in after


# ---------------------------------------------------------------------------
# Batching — 100 traces arrive as batches of ≤ 20
# ---------------------------------------------------------------------------

class TestBatching:

    def test_100_traces_batched_into_groups_of_at_most_20(self):
        shipper = AsyncShipper(
            base_url="http://localhost:9999",
            api_key="sk_test",
            batch_size=20,
            retry_delays=(0,),  # 1 attempt, no sleep
        )
        received: list[list] = []

        def record_post(endpoint, batch):
            received.append(list(batch))

        with patch.object(shipper, "_post", side_effect=record_post):
            for i in range(100):
                shipper.enqueue(_TRACE_ENDPOINT, {"index": i})
            shipper.flush()

        total = sum(len(b) for b in received)
        assert total == 100
        for batch in received:
            assert len(batch) <= 20

    def test_traces_and_signals_sent_to_separate_endpoints(self):
        shipper = AsyncShipper(
            base_url="http://localhost:9999",
            api_key="sk_test",
            batch_size=20,
            retry_delays=(0,),
        )
        posts: list[tuple[str, list]] = []

        def record_post(endpoint, batch):
            posts.append((endpoint, list(batch)))

        with patch.object(shipper, "_post", side_effect=record_post):
            for i in range(5):
                shipper.enqueue(_TRACE_ENDPOINT, {"trace": i})
            for i in range(3):
                shipper.enqueue(_SIGNAL_ENDPOINT, {"signal": i})
            shipper.flush()

        trace_posts = [p for ep, p in posts if ep == _TRACE_ENDPOINT]
        signal_posts = [p for ep, p in posts if ep == _SIGNAL_ENDPOINT]
        assert sum(len(b) for b in trace_posts) == 5
        assert sum(len(b) for b in signal_posts) == 3


# ---------------------------------------------------------------------------
# Retry logic
# ---------------------------------------------------------------------------

class TestRetryLogic:

    def test_2_failures_then_success_makes_3_attempts(self):
        shipper = AsyncShipper(
            base_url="http://localhost:9999",
            api_key="sk_test",
            retry_delays=(0.01, 0.01, 0.01),
        )
        attempt_count = [0]

        def flaky(endpoint, batch):
            attempt_count[0] += 1
            if attempt_count[0] < 3:
                raise ConnectionError("transient")

        with patch.object(shipper, "_post", side_effect=flaky):
            shipper.enqueue(_TRACE_ENDPOINT, {"data": "x"})
            shipper.flush()

        assert attempt_count[0] == 3

    def test_all_retries_fail_silently(self):
        shipper = AsyncShipper(
            base_url="http://localhost:9999",
            api_key="sk_test",
            retry_delays=(0.01, 0.01, 0.01),
        )

        with patch.object(shipper, "_post", side_effect=ConnectionError("always fails")):
            shipper.enqueue(_TRACE_ENDPOINT, {"data": "x"})
            shipper.flush()  # must not raise

    def test_retry_does_not_affect_other_batches(self):
        shipper = AsyncShipper(
            base_url="http://localhost:9999",
            api_key="sk_test",
            retry_delays=(0.01,),  # 1 attempt
        )
        succeeded: list[list] = []

        def record(endpoint, batch):
            succeeded.append(batch)

        with patch.object(shipper, "_post", side_effect=record):
            for i in range(3):
                shipper.enqueue(_TRACE_ENDPOINT, {"index": i})
            shipper.flush()

        total = sum(len(b) for b in succeeded)
        assert total == 3


# ---------------------------------------------------------------------------
# Zero-latency benchmark — enqueue() must not block
# ---------------------------------------------------------------------------

class TestLatencyBenchmark:

    def test_enqueue_averages_under_1ms(self):
        shipper = AsyncShipper(
            base_url="http://localhost:9999",
            api_key="sk_test",
            retry_delays=(0,),
        )
        N = 1_000

        with patch.object(shipper, "_post"):
            t0 = time.monotonic()
            for i in range(N):
                shipper.enqueue(_TRACE_ENDPOINT, {"index": i})
            elapsed = time.monotonic() - t0
            shipper.flush()

        avg_us = (elapsed / N) * 1_000_000
        assert avg_us < 1_000, f"enqueue() averaged {avg_us:.1f}µs, expected < 1000µs"

    def test_context_manager_overhead_under_1ms(self, client, mock_shipper):
        N = 500
        t0 = time.monotonic()
        for _ in range(N):
            with _make_trace(client) as trace:
                _inject(trace)
        elapsed = time.monotonic() - t0

        avg_ms = (elapsed / N) * 1_000
        assert avg_ms < 1.0, f"context manager averaged {avg_ms:.3f}ms, expected < 1ms"
