"""
sentinel/shipper.py

Background async shipper. Batches payloads and POSTs to the ingest API
with exponential backoff. The caller never waits for network I/O.
"""

from __future__ import annotations

import json
import logging
import queue
import threading
import time
import urllib.request
from typing import Any, Sequence

logger = logging.getLogger(__name__)

_DEFAULT_BATCH_SIZE = 20
_DEFAULT_RETRY_DELAYS: tuple[float, ...] = (0.5, 1.0, 2.0)


class AsyncShipper:
    """
    Drains an in-process queue on a single daemon thread.

    - enqueue() returns immediately — zero latency impact on the caller.
    - Worker collects up to batch_size items per POST, grouping by endpoint.
    - Each POST is retried up to len(retry_delays) times with the given delays.
    - All failures are logged at DEBUG and swallowed — never raised to the caller.
    - flush() blocks until every queued item has been attempted; use in tests.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        timeout: int = 5,
        batch_size: int = _DEFAULT_BATCH_SIZE,
        retry_delays: Sequence[float] = _DEFAULT_RETRY_DELAYS,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._timeout = timeout
        self._batch_size = batch_size
        self._retry_delays = tuple(retry_delays)
        self._queue: queue.Queue[tuple[str, dict[str, Any]]] = queue.Queue()
        self._thread = threading.Thread(
            target=self._worker, daemon=True, name="sentinel-shipper"
        )
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
            # Block until at least one item arrives
            first = self._queue.get()
            raw_batch: list[tuple[str, dict[str, Any]]] = [first]

            # Drain more without blocking, up to batch_size
            while len(raw_batch) < self._batch_size:
                try:
                    raw_batch.append(self._queue.get_nowait())
                except queue.Empty:
                    break

            # Group by endpoint so traces and signals don't mix
            grouped: dict[str, list[dict[str, Any]]] = {}
            for endpoint, payload in raw_batch:
                grouped.setdefault(endpoint, []).append(payload)

            try:
                for endpoint, payloads in grouped.items():
                    try:
                        self._post_with_retry(endpoint, payloads)
                    except Exception:
                        logger.debug(
                            "Sentinel shipper: gave up on %d item(s) for %s",
                            len(payloads),
                            endpoint,
                            exc_info=True,
                        )
            finally:
                for _ in raw_batch:
                    self._queue.task_done()

    def _post_with_retry(self, endpoint: str, batch: list[dict[str, Any]]) -> None:
        last_exc: Exception | None = None
        for attempt, delay in enumerate(self._retry_delays):
            try:
                self._post(endpoint, batch)
                return  # success
            except Exception as exc:
                last_exc = exc
                logger.debug(
                    "Sentinel shipper: attempt %d/%d failed for %s: %s",
                    attempt + 1,
                    len(self._retry_delays),
                    endpoint,
                    exc,
                )
                if attempt < len(self._retry_delays) - 1:
                    time.sleep(delay)
        raise last_exc  # type: ignore[misc]

    def _post(self, endpoint: str, batch: list[dict[str, Any]]) -> None:
        url = f"{self._base_url}{endpoint}"
        body = json.dumps(batch, default=str).encode("utf-8")
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
