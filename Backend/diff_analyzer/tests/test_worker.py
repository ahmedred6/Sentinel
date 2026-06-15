"""
Tests for backend/diff_analyzer/ — DIFF-04 acceptance criteria:
  - Worker consumes from diff-analyzer consumer group correctly
  - Stage 2 filter removes irrelevant hunks via injectable haiku_fn
  - No-diff case produces "score drift detected" attribution (no LLM call)
  - Invalid JSON from Sonnet → neutral result returned
  - wait_for_scores: polls for ready state, returns None on timeout
  - XACK always sent regardless of write success or failure
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import fakeredis
import pytest

from worker import GROUP_NAME, STREAM_NAME, process_diff, run_worker
from attribution import (
    _extract_relevant_filenames,
    generate_attribution,
    parse_json_response,
    wait_for_scores,
)
from filter import stage2_filter
from schema import DiffPayload


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_diff_payload(**overrides) -> DiffPayload:
    base = {
        "experiment_id": str(uuid.uuid4()),
        "pipeline_name": "rfp-eval",
        "developer": "ahmed",
        "branch": "feature/fix",
        "base_branch": "origin/main",
        "commit_sha": "abc12345",
        "filtered_diff": (
            "diff --git a/prompts/rubric.txt b/prompts/rubric.txt\n"
            "+improved rubric line\n"
        ),
        "original_files": {"prompts/rubric.txt": "old rubric content"},
        "customer_id": "cust_test",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    base.update(overrides)
    return DiffPayload(**base)


def _make_diff_json(**overrides) -> str:
    return _make_diff_payload(**overrides).model_dump_json()


def _new_redis():
    return fakeredis.FakeAsyncRedis(version=(7, 0, 0))


async def _setup_stream(redis) -> None:
    try:
        await redis.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
    except Exception:
        pass


_MOCK_SCORES = {
    "pipeline_score": 0.82,
    "agent_scores": {"l1": 0.9},
    "score_delta_vs_prev": 0.05,
    "score_delta_vs_main": -0.02,
}

_NEUTRAL_ATTRIBUTION = {
    "title": "Test attribution",
    "description": "Test description.",
    "impact": "positive",
    "affected_agents": ["agent_a"],
    "confidence": 0.9,
}


# ---------------------------------------------------------------------------
# Test 1: Worker consumes 10 payloads from diff-analyzer group
# ---------------------------------------------------------------------------

def test_worker_consumes_from_diff_analyzer_group():
    """10 diff payloads consumed and ACKed — PEL=0 after worker stops."""
    async def _run():
        redis = _new_redis()
        await _setup_stream(redis)

        for _ in range(10):
            await redis.xadd(STREAM_NAME, {"payload": _make_diff_json()})

        stop = asyncio.Event()

        async def _stopper():
            await asyncio.sleep(0.4)
            stop.set()

        with (
            patch("worker.wait_for_scores", new_callable=AsyncMock) as mock_wfs,
            patch("worker.stage2_filter", new_callable=AsyncMock) as mock_f,
            patch("worker.generate_attribution", new_callable=AsyncMock) as mock_attr,
            patch("worker.write_experiment_attribution", new_callable=AsyncMock),
        ):
            mock_wfs.return_value = _MOCK_SCORES
            mock_f.return_value = "filtered semantic diff"
            mock_attr.return_value = _NEUTRAL_ATTRIBUTION

            await asyncio.gather(
                run_worker(redis, AsyncMock(), _stop_event=stop),
                _stopper(),
            )

        pending = await redis.xpending(STREAM_NAME, GROUP_NAME)
        assert pending["pending"] == 0, (
            f"Expected PEL=0 after processing, got {pending['pending']}"
        )
        await redis.aclose()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Test 2: XACK sent after successful write
# ---------------------------------------------------------------------------

def test_xack_sent_after_successful_write():
    """Normal processing path: PEL empty after one message processed."""
    async def _run():
        redis = _new_redis()
        await _setup_stream(redis)
        await redis.xadd(STREAM_NAME, {"payload": _make_diff_json()})

        stop = asyncio.Event()

        async def _stopper():
            await asyncio.sleep(0.3)
            stop.set()

        with (
            patch("worker.wait_for_scores", new_callable=AsyncMock) as mock_wfs,
            patch("worker.stage2_filter", new_callable=AsyncMock) as mock_f,
            patch("worker.generate_attribution", new_callable=AsyncMock) as mock_attr,
            patch("worker.write_experiment_attribution", new_callable=AsyncMock),
        ):
            mock_wfs.return_value = _MOCK_SCORES
            mock_f.return_value = "filtered"
            mock_attr.return_value = _NEUTRAL_ATTRIBUTION

            await asyncio.gather(
                run_worker(redis, AsyncMock(), _stop_event=stop),
                _stopper(),
            )

        pending = await redis.xpending(STREAM_NAME, GROUP_NAME)
        assert pending["pending"] == 0
        await redis.aclose()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Test 3: XACK sent even when write_experiment_attribution raises
# ---------------------------------------------------------------------------

def test_xack_sent_on_write_failure():
    """DB write raises — XACK must still fire and PEL must be empty."""
    async def _run():
        redis = _new_redis()
        await _setup_stream(redis)
        await redis.xadd(STREAM_NAME, {"payload": _make_diff_json()})

        stop = asyncio.Event()

        async def _stopper():
            await asyncio.sleep(0.3)
            stop.set()

        with (
            patch("worker.wait_for_scores", new_callable=AsyncMock) as mock_wfs,
            patch("worker.stage2_filter", new_callable=AsyncMock) as mock_f,
            patch("worker.generate_attribution", new_callable=AsyncMock) as mock_attr,
            patch("worker.write_experiment_attribution",
                  side_effect=RuntimeError("DB connection refused")),
        ):
            mock_wfs.return_value = _MOCK_SCORES
            mock_f.return_value = "filtered"
            mock_attr.return_value = _NEUTRAL_ATTRIBUTION

            await asyncio.gather(
                run_worker(redis, AsyncMock(), _stop_event=stop),
                _stopper(),
            )

        pending = await redis.xpending(STREAM_NAME, GROUP_NAME)
        assert pending["pending"] == 0
        await redis.aclose()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Test 4: Scores timeout — worker proceeds with zero deltas, still writes
# ---------------------------------------------------------------------------

def test_scores_timeout_proceeds_gracefully():
    """When wait_for_scores returns None, worker uses zero deltas and writes attribution."""
    async def _run():
        redis = _new_redis()
        await _setup_stream(redis)
        await redis.xadd(STREAM_NAME, {"payload": _make_diff_json()})

        stop = asyncio.Event()

        async def _stopper():
            await asyncio.sleep(0.3)
            stop.set()

        with (
            patch("worker.wait_for_scores", new_callable=AsyncMock) as mock_wfs,
            patch("worker.stage2_filter", new_callable=AsyncMock) as mock_f,
            patch("worker.generate_attribution", new_callable=AsyncMock) as mock_attr,
            patch("worker.write_experiment_attribution", new_callable=AsyncMock) as mock_write,
        ):
            mock_wfs.return_value = None  # timeout
            mock_f.return_value = "filtered"
            mock_attr.return_value = _NEUTRAL_ATTRIBUTION

            await asyncio.gather(
                run_worker(redis, AsyncMock(), _stop_event=stop),
                _stopper(),
            )

        pending = await redis.xpending(STREAM_NAME, GROUP_NAME)
        assert pending["pending"] == 0
        mock_write.assert_called_once()
        await redis.aclose()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Test 5: Stage 2 filter removes irrelevant hunks
# ---------------------------------------------------------------------------

def test_stage2_filter_removes_irrelevant_hunks():
    """haiku_fn receives the full diff and stage2_filter returns its output."""
    MIXED_DIFF = (
        "diff --git a/prompts/rubric.txt b/prompts/rubric.txt\n"
        "+relevant: weight relevance by 0.7\n"
        "+logging.debug('processed batch')\n"
    )
    EXPECTED_FILTERED = (
        "diff --git a/prompts/rubric.txt b/prompts/rubric.txt\n"
        "+relevant: weight relevance by 0.7\n"
        "Reasoning: changes rubric scoring logic\n"
    )

    async def _fake_haiku(prompt: str) -> str:
        assert MIXED_DIFF in prompt
        return EXPECTED_FILTERED

    async def _run():
        result = await stage2_filter(MIXED_DIFF, haiku_fn=_fake_haiku)
        assert result == EXPECTED_FILTERED

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Test 6: stage2_filter returns empty string for empty input — no LLM call
# ---------------------------------------------------------------------------

def test_stage2_filter_empty_diff_skips_llm():
    """Empty filtered_diff → returns '' immediately; haiku_fn is never called."""
    called = []

    async def _should_not_call(prompt: str) -> str:
        called.append(prompt)
        return "unexpected"

    async def _run():
        result = await stage2_filter("", haiku_fn=_should_not_call)
        assert result == ""
        assert not called

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Test 7: No-diff case produces "score drift detected" attribution
# ---------------------------------------------------------------------------

def test_no_diff_case_produces_score_drift_attribution():
    """Empty semantic_diff → hardcoded drift attribution; Sonnet never called."""
    sonnet_calls = []

    async def _should_not_call_sonnet(prompt: str) -> str:
        sonnet_calls.append(prompt)
        return "{}"

    async def _run():
        result = await generate_attribution(
            semantic_diff="",
            original_files={},
            agent_scores={},
            score_delta_vs_prev=-0.05,
            score_delta_vs_main=0.0,
            sonnet_fn=_should_not_call_sonnet,
        )
        assert result["impact"] == "unstable"
        assert result["confidence"] == 1.0
        assert "score drift" in result["title"].lower()
        assert "-0.05" in result["description"]
        assert result["affected_agents"] == []
        assert not sonnet_calls

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Test 8: parse_json_response — valid JSON
# ---------------------------------------------------------------------------

def test_parse_json_response_valid():
    data = {
        "title": "Rubric weights updated",
        "description": "Relevance weight increased from 0.5 to 0.7.",
        "impact": "positive",
        "affected_agents": ["score_assignor"],
        "confidence": 0.88,
    }
    assert parse_json_response(json.dumps(data)) == data


# ---------------------------------------------------------------------------
# Test 9: parse_json_response — invalid JSON returns neutral fallback
# ---------------------------------------------------------------------------

def test_parse_json_response_invalid_returns_neutral():
    result = parse_json_response("This is not valid JSON at all {{{")
    assert result["impact"] == "neutral"
    assert result["confidence"] == 0.0
    assert "affected_agents" in result
    assert isinstance(result["affected_agents"], list)


# ---------------------------------------------------------------------------
# Test 10: parse_json_response — markdown code block stripped
# ---------------------------------------------------------------------------

def test_parse_json_response_strips_markdown_code_block():
    data = {
        "title": "Test",
        "description": "Desc.",
        "impact": "negative",
        "affected_agents": [],
        "confidence": 0.7,
    }
    wrapped = f"```json\n{json.dumps(data)}\n```"
    assert parse_json_response(wrapped) == data


# ---------------------------------------------------------------------------
# Test 11: wait_for_scores returns immediately when scores are ready
# ---------------------------------------------------------------------------

def test_wait_for_scores_returns_when_ready():
    exp_id = str(uuid.uuid4())

    async def _run():
        db = AsyncMock()
        db.fetchrow = AsyncMock(return_value={
            "pipeline_score": 0.85,
            "agent_scores": '{"l1": 0.9}',
            "score_delta_vs_prev": 0.05,
            "score_delta_vs_main": -0.02,
        })
        result = await wait_for_scores(exp_id, db, timeout=5.0, poll_interval=0.1)

        assert result is not None
        assert result["pipeline_score"] == 0.85
        assert result["agent_scores"] == {"l1": 0.9}
        assert result["score_delta_vs_prev"] == 0.05
        assert result["score_delta_vs_main"] == -0.02

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Test 12: wait_for_scores returns None on timeout
# ---------------------------------------------------------------------------

def test_wait_for_scores_returns_none_on_timeout():
    exp_id = str(uuid.uuid4())

    async def _run():
        db = AsyncMock()
        db.fetchrow = AsyncMock(return_value=None)
        result = await wait_for_scores(exp_id, db, timeout=0.1, poll_interval=0.05)
        assert result is None

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Test 13: generate_attribution passes original file content to Sonnet prompt
# ---------------------------------------------------------------------------

def test_generate_attribution_passes_original_files():
    """Original file content must appear in the Sonnet prompt."""
    captured = []

    async def _capture_sonnet(prompt: str) -> str:
        captured.append(prompt)
        return json.dumps({
            "title": "Rubric weights updated",
            "description": "Relevance weight increased.",
            "impact": "positive",
            "affected_agents": ["score_assignor"],
            "confidence": 0.88,
        })

    async def _run():
        await generate_attribution(
            semantic_diff="+ relevance weight: 0.7",
            original_files={"prompts/rubric.txt": "relevance weight: 0.5"},
            agent_scores={"score_assignor": 0.85},
            score_delta_vs_prev=0.05,
            score_delta_vs_main=0.0,
            sonnet_fn=_capture_sonnet,
        )
        assert captured, "Sonnet was not called"
        prompt = captured[0]
        assert "prompts/rubric.txt" in prompt
        assert "relevance weight: 0.5" in prompt

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Test 14: generate_attribution — invalid Sonnet JSON returns neutral
# ---------------------------------------------------------------------------

def test_generate_attribution_invalid_sonnet_json_returns_neutral():
    """When Sonnet returns non-JSON text, parse_json_response returns neutral fallback."""
    async def _bad_sonnet(prompt: str) -> str:
        return "Sorry, I cannot provide a JSON response right now."

    async def _run():
        result = await generate_attribution(
            semantic_diff="+ some relevant change",
            original_files={},
            agent_scores={},
            score_delta_vs_prev=0.0,
            score_delta_vs_main=0.0,
            sonnet_fn=_bad_sonnet,
        )
        assert result["impact"] == "neutral"
        assert result["confidence"] == 0.0

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Test 15: _extract_relevant_filenames — git diff format
# ---------------------------------------------------------------------------

def test_extract_relevant_filenames_from_git_diff():
    diff = (
        "diff --git a/prompts/rubric.txt b/prompts/rubric.txt\n"
        "index abc..def 100644\n"
        "--- a/prompts/rubric.txt\n"
        "+++ b/prompts/rubric.txt\n"
        "diff --git a/agents/score_assignor.py b/agents/score_assignor.py\n"
        "index ghi..jkl 100644\n"
    )
    result = _extract_relevant_filenames(diff)
    assert "prompts/rubric.txt" in result
    assert "agents/score_assignor.py" in result


# ---------------------------------------------------------------------------
# Test 16: _extract_relevant_filenames — fallback to changed_files scan
# ---------------------------------------------------------------------------

def test_extract_relevant_filenames_fallback_to_changed_files():
    """When no git diff headers exist, scan semantic_diff text for known filenames."""
    semantic_diff = (
        "The change in prompts/rubric.txt shifts relevance weights significantly. "
        "This is the primary driver of the observed score delta."
    )
    changed = ["prompts/rubric.txt", "agents/scorer.py"]
    result = _extract_relevant_filenames(semantic_diff, changed_files=changed)
    assert "prompts/rubric.txt" in result
    assert "agents/scorer.py" not in result
