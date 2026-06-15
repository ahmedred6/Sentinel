"""
backend/diff_analyzer/filter.py

Stage 2 semantic diff filter.

stage2_filter() takes the already-Stage-1-filtered diff and asks Claude Haiku
to remove hunks that cannot plausibly affect LLM output quality: logging
changes, error handling, formatting, comments, and variable renames that
don't change observable behaviour.

Returns empty string for empty input — no LLM call is made.
"""

from __future__ import annotations

import logging
from typing import Awaitable, Callable, Optional

log = logging.getLogger(__name__)

_HAIKU_PROMPT = """\
Given these code changes in LLM-relevant files, which specific hunks could \
plausibly affect LLM output quality?

Ignore: logging changes, error handling, formatting, comments, variable \
renames that don't change logic.

Return only the relevant hunks with one-line reasoning each. \
If nothing is relevant, return an empty string.

Diff:
{diff}"""


async def _call_haiku(prompt: str) -> str:
    import anthropic
    client = anthropic.AsyncAnthropic()
    response = await client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=2000,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text


async def stage2_filter(
    filtered_diff: str,
    *,
    haiku_fn: Optional[Callable[[str], Awaitable[str]]] = None,
) -> str:
    """
    Stage 2 semantic filter: removes LLM-irrelevant hunks via Claude Haiku.

    Returns empty string immediately for empty input — no LLM call.
    haiku_fn allows test injection without patching.
    """
    if not filtered_diff:
        return ""

    _haiku = haiku_fn or _call_haiku
    prompt = _HAIKU_PROMPT.format(diff=filtered_diff)

    try:
        return await _haiku(prompt)
    except Exception as exc:
        log.error("Stage 2 filter (Haiku) failed: %s — returning unfiltered diff", exc)
        return filtered_diff
