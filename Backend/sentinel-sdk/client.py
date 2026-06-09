"""
sentinel/client.py

SentinelClient is the main entry point for SDK consumers.
Instantiate once at application startup, then call signal methods
anywhere a user action is observed.

Usage:
    sentinel = SentinelClient(api_key="sk_...", customer_id="cust_abc")

    # after an LLM response is shown to the user:
    sentinel.thumbs_down(trace_id)   # user clicked negative feedback
    sentinel.rephrased(trace_id)     # user immediately re-asked the question
    sentinel.escalate(trace_id)      # user requested a human agent
    sentinel.abandoned(trace_id)     # user quit within 30s
    sentinel.acknowledged(trace_id)  # user said "thanks", session ended naturally
"""

from __future__ import annotations

from typing import Optional

from schema import SignalPayload, SignalType
from shipper import AsyncShipper

_SIGNALS_ENDPOINT = "/v1/signals"


class SentinelClient:
    """
    Thread-safe. Safe to call signal methods from any thread — all I/O
    is delegated to the background shipper thread.

    Args:
        api_key:      Your Sentinel API key (starts with sk_).
        customer_id:  Your Sentinel customer identifier (starts with cust_).
        base_url:     Override the ingest API base URL (default: production).
        _shipper:     Inject a custom shipper; used in tests to avoid real HTTP.
    """

    def __init__(
        self,
        api_key: str,
        customer_id: str,
        base_url: str = "https://ingest.sentinel-ai.io",
        _shipper: Optional[AsyncShipper] = None,
    ) -> None:
        self._customer_id = customer_id
        self._shipper = _shipper or AsyncShipper(base_url=base_url, api_key=api_key)

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

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _send_signal(self, trace_id: str, signal_type: SignalType) -> None:
        payload = SignalPayload(
            trace_id=trace_id,
            customer_id=self._customer_id,
            signal_type=signal_type,
        )
        self._shipper.enqueue(_SIGNALS_ENDPOINT, payload.model_dump(mode="json"))
