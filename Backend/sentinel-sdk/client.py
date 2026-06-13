"""
sentinel/client.py

SentinelClient is the main entry point for the Sentinel SDK.

Typical setup (once at app startup):

    sentinel = SentinelClient.init(api_key="sk_...", customer_id="cust_abc")

Wrapping an LLM call (context manager):

    with sentinel.trace(flow="support", user_id=user_id):
        response = openai.chat.completions.create(
            model="gpt-4.1",
            messages=messages,
        )

Wrapping an LLM call (decorator):

    @sentinel.trace(flow="support")
    def handle_message(messages):
        return openai.chat.completions.create(model="gpt-4.1", messages=messages)

Reporting a user behavior signal after the response is shown:

    sentinel.thumbs_down(trace_id)
    sentinel.rephrased(trace_id)
"""

from __future__ import annotations

import uuid
from typing import Optional

from context import SentinelTraceContext
from interceptors import install_all
from schema import ContextDocument, Environment, SignalPayload, SignalType
from shipper import AsyncShipper

_SIGNALS_ENDPOINT = "/v1/signals"
_DEFAULT_BASE_URL = "https://ingest.sentinel-ai.io"


class SentinelClient:
    """
    Thread-safe. All I/O is delegated to the background shipper thread.

    Args:
        api_key:      Your Sentinel API key (starts with sk_).
        customer_id:  Your Sentinel customer identifier (starts with cust_).
        base_url:     Override the ingest API base URL.
        _shipper:     Inject a custom shipper — used in tests to avoid real HTTP.
    """

    def __init__(
        self,
        api_key: str,
        customer_id: str,
        base_url: str = _DEFAULT_BASE_URL,
        _shipper: Optional[AsyncShipper] = None,
    ) -> None:
        self._customer_id = customer_id
        self._shipper = _shipper or AsyncShipper(base_url=base_url, api_key=api_key)

    @classmethod
    def init(
        cls,
        api_key: str,
        customer_id: str,
        base_url: str = _DEFAULT_BASE_URL,
    ) -> "SentinelClient":
        """
        Primary factory. Starts the background shipper thread and installs
        OpenAI/Anthropic interceptors. Call once at application startup.
        """
        install_all()
        return cls(api_key=api_key, customer_id=customer_id, base_url=base_url)

    # ------------------------------------------------------------------
    # Trace context manager / decorator
    # ------------------------------------------------------------------

    def trace(
        self,
        flow: str,
        session_id: Optional[str] = None,
        user_id: Optional[str] = None,
        environment: Environment = Environment.PROD,
        retrieved_context: Optional[list[ContextDocument]] = None,
    ) -> SentinelTraceContext:
        """
        Returns a SentinelTraceContext usable as a context manager or decorator.
        session_id is auto-generated if not provided.
        """
        return SentinelTraceContext(
            shipper=self._shipper,
            customer_id=self._customer_id,
            flow_name=flow,
            session_id=session_id or str(uuid.uuid4()),
            user_id=user_id,
            environment=environment,
            retrieved_context=retrieved_context,
        )

    # ------------------------------------------------------------------
    # Behavioral signal methods  (Layer 2 eval engine inputs)
    # ------------------------------------------------------------------

    def thumbs_down(self, trace_id: str) -> None:
        """User clicked negative feedback on this response."""
        self._send_signal(trace_id, SignalType.THUMBS_DOWN)

    def escalate(self, trace_id: str) -> None:
        """User requested a human agent after this response."""
        self._send_signal(trace_id, SignalType.ESCALATE)

    def rephrased(self, trace_id: str) -> None:
        """User repeated or rephrased their question within 60 seconds."""
        self._send_signal(trace_id, SignalType.REPHRASED)

    def abandoned(self, trace_id: str) -> None:
        """User ended the session within 30 seconds of this response."""
        self._send_signal(trace_id, SignalType.ABANDONED)

    def acknowledged(self, trace_id: str) -> None:
        """User sent a short acknowledgment and the session ended naturally."""
        self._send_signal(trace_id, SignalType.ACKNOWLEDGED)

    def _send_signal(self, trace_id: str, signal_type: SignalType) -> None:
        payload = SignalPayload(
            trace_id=trace_id,
            customer_id=self._customer_id,
            signal_type=signal_type,
        )
        self._shipper.enqueue(_SIGNALS_ENDPOINT, payload.model_dump(mode="json"))
