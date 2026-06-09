"""
sentinel/shipper.py

Background async shipper. Enqueues payloads on a daemon thread so the
caller never blocks waiting for network I/O. Uses only stdlib (no requests
dependency) so the SDK stays zero-dep beyond pydantic.
"""

from __future__ import annotations

import json
import logging
import queue
import threading
import urllib.error
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)

_SIGNALS_ENDPOINT = "/v1/signals"
_TRACES_ENDPOINT = "/v1/traces"


class AsyncShipper:
    """
    Drains an in-process queue on a single daemon thread and POSTs each
    payload to the Sentinel ingest API.

    - enqueue() returns immediately (non-blocking for the caller).
    - The daemon thread exits automatically when the host process exits.
    - Network errors are logged at DEBUG and silently dropped so they can
      never crash or slow down the customer's application.
    - flush() blocks until all queued items are delivered; use it in tests
      and in process shutdown hooks.
    """

    def __init__(self, base_url: str, api_key: str, timeout: int = 5) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._timeout = timeout
        self._queue: queue.Queue[tuple[str, dict[str, Any]]] = queue.Queue()
        self._thread = threading.Thread(target=self._worker, daemon=True, name="sentinel-shipper")
        self._thread.start()

    def enqueue(self, endpoint: str, payload: dict[str, Any]) -> None:
        """Add a payload to the send queue. Returns immediately."""
        self._queue.put((endpoint, payload))

    def flush(self) -> None:
        """Block until every queued item has been attempted. Intended for tests."""
        self._queue.join()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _worker(self) -> None:
        while True:
            endpoint, payload = self._queue.get()
            try:
                self._post(endpoint, payload)
            except Exception:
                logger.debug(
                    "Sentinel shipper: delivery failed for endpoint %s",
                    endpoint,
                    exc_info=True,
                )
            finally:
                self._queue.task_done()

    def _post(self, endpoint: str, payload: dict[str, Any]) -> None:
        url = f"{self._base_url}{endpoint}"
        body = json.dumps(payload, default=str).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=body,
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=self._timeout):
            pass
