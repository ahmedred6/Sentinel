"""
backend/diff_analyzer/attribution.py

LLM attribution for the Diff Analyzer Module.

  parse_json_response()         — parse Claude's JSON, strips markdown code fences
  _extract_relevant_filenames() — extract filenames from a semantic diff
  wait_for_scores()             — poll PostgreSQL for experiment scores (up to 30s)
  generate_attribution()        — Claude Sonnet attribution; no-diff case handled inline
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any, Awaitable, Callable, Optional

log = logging.getLogger(__name__)


async def _call_sonnet(prompt: str) -> str:
    import anthropic
    client = anthropic.AsyncAnthropic()
    response = await client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1000,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text


# ---------------------------------------------------------------------------
# JSON parsing
# ---------------------------------------------------------------------------

def parse_json_response(response: str) -> dict:
    """
    Parse a JSON response from Claude.

    Handles markdown code fences (```json...``` or ```...```).
    Returns a neutral fallback dict and logs an error on parse failure.
    """
    try:
        text = response.strip()
        if text.startswith("```"):
            lines = text.splitlines()
            end = -1 if lines[-1].strip() == "```" else None
            text = "\n".join(lines[1:end])
        return json.loads(text)
    except (json.JSONDecodeError, ValueError) as exc:
        log.error(
            "Failed to parse attribution JSON: %s — response: %.200s", exc, response
        )
        return {
            "title": "Attribution parse failed",
            "description": "The attribution model returned an invalid response.",
            "impact": "neutral",
            "affected_agents": [],
            "confidence": 0.0,
        }


# ---------------------------------------------------------------------------
# Filename extraction
# ---------------------------------------------------------------------------

def _extract_relevant_filenames(
    semantic_diff: str,
    changed_files: Optional[list[str]] = None,
) -> list[str]:
    """
    Extract filenames from a semantic diff.

    Tries git diff --git header format first. Falls back to scanning the text
    for any filename from changed_files that is mentioned by the LLM.
    """
    if not semantic_diff:
        return []
    matches = re.findall(r"^diff --git a/(.*?) b/", semantic_diff, re.MULTILINE)
    if matches:
        return list(dict.fromkeys(matches))
    if changed_files:
        return [f for f in changed_files if f in semantic_diff]
    return []


# ---------------------------------------------------------------------------
# Scores polling
# ---------------------------------------------------------------------------

async def wait_for_scores(
    experiment_id: str,
    db_pool: Any,
    *,
    timeout: float = 30.0,
    poll_interval: float = 1.0,
) -> Optional[dict]:
    """
    Poll PostgreSQL for scores_ready=TRUE on the given experiment.

    Returns a dict with pipeline_score, agent_scores, score_delta_vs_prev,
    score_delta_vs_main — or None if the timeout expires first.
    """
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout

    while True:
        row = await db_pool.fetchrow(
            """
            SELECT pipeline_score, agent_scores,
                   score_delta_vs_prev, score_delta_vs_main
            FROM experiments
            WHERE experiment_id = $1::uuid AND scores_ready = TRUE
            """,
            experiment_id,
        )
        if row:
            return {
                "pipeline_score": row["pipeline_score"],
                "agent_scores": json.loads(row["agent_scores"] or "{}"),
                "score_delta_vs_prev": row["score_delta_vs_prev"],
                "score_delta_vs_main": row["score_delta_vs_main"],
            }
        remaining = deadline - loop.time()
        if remaining <= 0:
            return None
        await asyncio.sleep(min(poll_interval, remaining))


# ---------------------------------------------------------------------------
# Attribution generation
# ---------------------------------------------------------------------------

_ATTRIBUTION_PROMPT = """\
You are analyzing a code change made to an LLM pipeline.

ORIGINAL FILE CONTENT (before changes):
{file_context}
WHAT CHANGED (diff — only LLM-relevant hunks):
{semantic_diff}

SCORE DELTA VS PREVIOUS RUN: {delta_prev:+.2f}
SCORE DELTA VS MAIN BRANCH:  {delta_main:+.2f}
AGENT SCORES THIS RUN: {agent_scores}

Based on the original file content and what changed, explain what the developer \
modified and why it caused this specific score delta. Be precise — quote the \
exact change and connect it to the specific agent and dimension that was affected.

Return JSON only:
{{
    "title": "under 8 words describing the change",
    "description": "2-3 sentences connecting the specific change to the score delta",
    "impact": "positive|negative|neutral|unstable",
    "affected_agents": ["agent-name"],
    "confidence": 0.0-1.0
}}"""


async def generate_attribution(
    semantic_diff: str,
    original_files: dict,
    agent_scores: dict,
    score_delta_vs_prev: float,
    score_delta_vs_main: float,
    *,
    sonnet_fn: Optional[Callable[[str], Awaitable[str]]] = None,
) -> dict:
    """
    Generate human-readable attribution connecting the code change to the score delta.

    When semantic_diff is empty (no-diff run), returns a hardcoded "score drift
    detected" result without calling the LLM — the pipeline ran identical code
    but scored differently, indicating non-determinism.
    """
    if not semantic_diff:
        return {
            "title": "No code changes — score drift detected",
            "description": (
                f"This run used identical code to the previous run but produced "
                f"a different score (delta: {score_delta_vs_prev:+.2f}). "
                f"This indicates non-determinism in the pipeline. Consider "
                f"lowering model temperature or adding output constraints "
                f"to improve stability."
            ),
            "impact": "unstable",
            "affected_agents": [],
            "confidence": 1.0,
        }

    file_context = "".join(
        f"\n--- ORIGINAL: {name} ---\n{content}\n"
        for name, content in original_files.items()
    )

    prompt = _ATTRIBUTION_PROMPT.format(
        file_context=file_context,
        semantic_diff=semantic_diff,
        delta_prev=score_delta_vs_prev,
        delta_main=score_delta_vs_main,
        agent_scores=agent_scores,
    )

    _sonnet = sonnet_fn or _call_sonnet

    try:
        response = await _sonnet(prompt)
        return parse_json_response(response)
    except Exception as exc:
        log.error("Attribution (Sonnet) failed: %s", exc)
        return {
            "title": "Attribution unavailable",
            "description": "The attribution model encountered an error.",
            "impact": "neutral",
            "affected_agents": [],
            "confidence": 0.0,
        }
