"""
backend/eval_engine/experiments.py

PostgreSQL write functions for the experiments table (DIFF-03).

Two workers write independently to the same row via experiment_id:

  write_experiment_scores()      — called by eval-worker after scoring all layers
  write_experiment_attribution() — called by diff-analyzer-worker after LLM attribution

The table supports partial writes — scores arrive before attribution.
Dashboards can read results as soon as scores_ready = TRUE.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

log = logging.getLogger(__name__)


async def write_experiment_scores(
    experiment_id: str,
    pipeline_score: float,
    agent_scores: dict,
    developer: str,
    branch: str,
    pipeline_name: str,
    customer_id: str,
    db_pool: Any,
) -> None:
    """
    Upsert pipeline-level scores for one experiment run.

    Computes two delta fields before writing:
      score_delta_vs_prev: pipeline_score minus the developer's most recent
                           score on the same branch (None if no prior run).
      score_delta_vs_main: pipeline_score minus the latest score on
                           origin/main (None if no main baseline exists).

    Uses ON CONFLICT so either worker may insert the row first without error.
    The current experiment_id is excluded from the look-back queries so a
    retry of this function never treats the first insert as the "previous" run.
    """
    prev_row = await db_pool.fetchrow(
        """
        SELECT pipeline_score FROM experiments
        WHERE developer = $1
          AND branch = $2
          AND pipeline_name = $3
          AND experiment_id != $4::uuid
        ORDER BY created_at DESC LIMIT 1
        """,
        developer, branch, pipeline_name, experiment_id,
    )
    delta_prev: Optional[float] = (
        pipeline_score - prev_row["pipeline_score"] if prev_row else None
    )

    main_row = await db_pool.fetchrow(
        """
        SELECT pipeline_score FROM experiments
        WHERE branch = 'origin/main'
          AND pipeline_name = $1
          AND experiment_id != $2::uuid
        ORDER BY created_at DESC LIMIT 1
        """,
        pipeline_name, experiment_id,
    )
    delta_main: Optional[float] = (
        pipeline_score - main_row["pipeline_score"] if main_row else None
    )

    await db_pool.execute(
        """
        INSERT INTO experiments (
            experiment_id, pipeline_name, customer_id, developer, branch,
            pipeline_score, score_delta_vs_prev, score_delta_vs_main,
            agent_scores, scores_ready
        ) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, TRUE)
        ON CONFLICT (experiment_id) DO UPDATE SET
            pipeline_score      = EXCLUDED.pipeline_score,
            score_delta_vs_prev = EXCLUDED.score_delta_vs_prev,
            score_delta_vs_main = EXCLUDED.score_delta_vs_main,
            agent_scores        = EXCLUDED.agent_scores,
            scores_ready        = TRUE
        """,
        experiment_id, pipeline_name, customer_id, developer, branch,
        pipeline_score, delta_prev, delta_main,
        json.dumps(agent_scores),
    )
    log.info(
        "Scores written: experiment=%s score=%.3f Δprev=%s Δmain=%s",
        experiment_id[:8], pipeline_score,
        f"{delta_prev:+.3f}" if delta_prev is not None else "n/a",
        f"{delta_main:+.3f}" if delta_main is not None else "n/a",
    )


async def write_experiment_attribution(
    experiment_id: str,
    title: str,
    description: str,
    impact: str,
    affected_agents: list[str],
    confidence: float,
    changed_files: list[str],
    relevant_files: list[str],
    filtered_diff: str,
    original_files: dict,
    db_pool: Any,
) -> None:
    """
    Update attribution and diff fields for an existing experiment row.

    Sets is_no_diff_run=TRUE when filtered_diff is empty — meaning the
    developer ran the pipeline with no LLM-relevant code changes, so
    regression cannot be attributed to a code change.

    This function is idempotent: calling it twice with the same arguments
    overwrites the previous attribution with identical data.
    """
    is_no_diff = not filtered_diff.strip()

    await db_pool.execute(
        """
        UPDATE experiments SET
            title             = $2,
            description       = $3,
            impact            = $4,
            affected_agents   = $5,
            confidence        = $6,
            changed_files     = $7,
            relevant_files    = $8,
            filtered_diff     = $9,
            original_files    = $10::jsonb,
            attribution_ready = TRUE,
            is_no_diff_run    = $11
        WHERE experiment_id = $1::uuid
        """,
        experiment_id,
        title, description, impact,
        affected_agents, confidence,
        changed_files, relevant_files,
        filtered_diff, json.dumps(original_files),
        is_no_diff,
    )
    log.info(
        "Attribution written: experiment=%s impact=%s no_diff=%s",
        experiment_id[:8], impact, is_no_diff,
    )
