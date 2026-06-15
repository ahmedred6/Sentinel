"""
backend/diff_analyzer/worker.py

Diff-analyzer-worker: consumes from sentinel:diffs, runs Stage 2 semantic
filtering via Claude Haiku, generates attribution via Claude Sonnet, and
writes results to the experiments table.

Consumer group: diff-analyzer
Stream:         sentinel:diffs

Failure policy:
  - XACK is in a finally block — never left permanently in the PEL.
  - wait_for_scores polls for up to 30s; proceeds with zero deltas on timeout.
  - Stage 2 and attribution failures are logged; the error path still persists
    a neutral attribution so the experiment row is complete.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from typing import Any, Optional

_DIFF_DIR = os.path.dirname(__file__)
_EVAL_DIR = os.path.abspath(os.path.join(_DIFF_DIR, "..", "eval_engine"))
_SDK_DIR = os.path.abspath(os.path.join(_DIFF_DIR, "..", "sentinel-sdk"))

for _p in (_SDK_DIR, _EVAL_DIR, _DIFF_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from schema import DiffPayload  # noqa: E402
from experiments import write_experiment_attribution  # noqa: E402
from filter import stage2_filter  # noqa: E402
from attribution import (  # noqa: E402
    _extract_relevant_filenames,
    generate_attribution,
    wait_for_scores,
)

log = logging.getLogger("diff_analyzer")

STREAM_NAME = "sentinel:diffs"
GROUP_NAME = "diff-analyzer"
CONSUMER_NAME = os.environ.get("DIFF_CONSUMER_NAME", "diff-analyzer-1")
POLL_INTERVAL = 0.5
POLL_TICK = 0.05


async def process_diff(
    payload: DiffPayload,
    msg_id: bytes,
    redis: Any,
    db_pool: Any,
    *,
    _haiku_fn=None,
    _sonnet_fn=None,
) -> None:
    """
    End-to-end processing of one diff payload.

    Steps:
      1. Poll for eval-worker scores (up to 30s)
      2. Stage 2 semantic filter via Haiku
      3. Attribution via Sonnet
      4. Write attribution to experiments table
      5. XACK — always fires (finally block), even if an earlier step raises
    """
    try:
        scores = await wait_for_scores(payload.experiment_id, db_pool)

        if scores is None:
            log.warning(
                "Scores not ready after timeout for %s — proceeding with zero deltas",
                payload.experiment_id,
            )
            scores = {
                "pipeline_score": None,
                "agent_scores": {},
                "score_delta_vs_prev": 0.0,
                "score_delta_vs_main": 0.0,
            }

        semantic_diff = await stage2_filter(
            payload.filtered_diff,
            haiku_fn=_haiku_fn,
        )

        attribution = await generate_attribution(
            semantic_diff=semantic_diff,
            original_files=payload.original_files,
            agent_scores=scores["agent_scores"],
            score_delta_vs_prev=scores["score_delta_vs_prev"] or 0.0,
            score_delta_vs_main=scores["score_delta_vs_main"] or 0.0,
            sonnet_fn=_sonnet_fn,
        )

        changed_files = list(payload.original_files.keys())
        relevant_files = _extract_relevant_filenames(semantic_diff, changed_files)

        await write_experiment_attribution(
            experiment_id=payload.experiment_id,
            title=attribution["title"],
            description=attribution["description"],
            impact=attribution["impact"],
            affected_agents=attribution["affected_agents"],
            confidence=attribution["confidence"],
            changed_files=changed_files,
            relevant_files=relevant_files,
            filtered_diff=payload.filtered_diff,
            original_files=payload.original_files,
            db_pool=db_pool,
        )

        log.info("process_diff complete for experiment %s", payload.experiment_id)

    except Exception as exc:
        log.error(
            "process_diff failed for experiment %s: %s",
            payload.experiment_id, exc,
        )
    finally:
        await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)


async def run_worker(
    redis: Any,
    db_pool: Any,
    *,
    _stop_event: Optional[asyncio.Event] = None,
    _haiku_fn=None,
    _sonnet_fn=None,
) -> None:
    """
    Infinite consumer loop for sentinel:diffs → diff-analyzer group.

    Non-blocking xreadgroup + stop-event polling mirrors the eval-worker pattern.
    """
    log.info("diff-analyzer-worker started (consumer=%s)", CONSUMER_NAME)

    while True:
        if _stop_event and _stop_event.is_set():
            break

        try:
            results = await redis.xreadgroup(
                GROUP_NAME,
                CONSUMER_NAME,
                {STREAM_NAME: ">"},
                count=5,
                block=0,
            )
        except Exception as exc:
            log.error("xreadgroup error: %s — sleeping 1s", exc)
            await asyncio.sleep(1)
            continue

        if not results:
            deadline = asyncio.get_event_loop().time() + POLL_INTERVAL
            while not (_stop_event and _stop_event.is_set()):
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    break
                await asyncio.sleep(min(POLL_TICK, remaining))
            continue

        for _stream, messages in results:
            for msg_id, fields in messages:
                payload_bytes = fields.get(b"payload") or b""
                if not payload_bytes:
                    await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
                    continue
                try:
                    payload = DiffPayload.model_validate_json(payload_bytes)
                    await process_diff(
                        payload, msg_id, redis, db_pool,
                        _haiku_fn=_haiku_fn,
                        _sonnet_fn=_sonnet_fn,
                    )
                except Exception as exc:
                    log.error(
                        "Unexpected error for msg %s: %s — ACKing",
                        msg_id, exc,
                    )
                    await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)

    log.info("diff-analyzer-worker stopped")
