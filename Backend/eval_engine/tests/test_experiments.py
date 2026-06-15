"""
Tests for backend/eval_engine/experiments.py

Covers DIFF-03 acceptance criteria:
  - write_experiment_scores() upserts row and computes both deltas
  - write_experiment_attribution() updates correct record by experiment_id
  - Both functions can write independently without conflict
  - is_no_diff_run correctly set when filtered_diff is empty
  - Dashboard query confirmed: scores_ready=TRUE readable before attribution_ready=TRUE
"""

import asyncio
import json
import uuid
from unittest.mock import AsyncMock

import pytest

from experiments import write_experiment_attribution, write_experiment_scores


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

EXP_ID = str(uuid.uuid4())
DEVELOPER = "ahmed"
BRANCH = "feature/fix"
PIPELINE = "rfp-eval"
CUSTOMER = "cust_001"


def _db(fetchrow_results=None):
    """Return a mock asyncpg pool with configurable fetchrow responses."""
    db = AsyncMock()
    db.execute = AsyncMock()
    if fetchrow_results is None:
        db.fetchrow = AsyncMock(return_value=None)
    elif isinstance(fetchrow_results, list):
        db.fetchrow = AsyncMock(side_effect=fetchrow_results)
    else:
        db.fetchrow = AsyncMock(return_value=fetchrow_results)
    return db


def _row(score: float) -> dict:
    return {"pipeline_score": score}


def _run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# write_experiment_scores — basic behavior
# ---------------------------------------------------------------------------

def test_write_scores_calls_execute_once():
    db = _db()

    _run(write_experiment_scores(
        experiment_id=EXP_ID,
        pipeline_score=0.82,
        agent_scores={"l1": 0.9, "l2": 0.75},
        developer=DEVELOPER,
        branch=BRANCH,
        pipeline_name=PIPELINE,
        customer_id=CUSTOMER,
        db_pool=db,
    ))

    db.execute.assert_called_once()


def test_write_scores_passes_experiment_id_first():
    db = _db()

    _run(write_experiment_scores(
        experiment_id=EXP_ID,
        pipeline_score=0.82,
        agent_scores={},
        developer=DEVELOPER,
        branch=BRANCH,
        pipeline_name=PIPELINE,
        customer_id=CUSTOMER,
        db_pool=db,
    ))

    positional = db.execute.call_args[0]
    # positional[0] = SQL, positional[1] = experiment_id
    assert positional[1] == EXP_ID


def test_write_scores_serializes_agent_scores_as_json():
    db = _db()
    scores = {"layer1": 0.9, "layer2": 0.7, "layer3": 0.85}

    _run(write_experiment_scores(
        experiment_id=EXP_ID,
        pipeline_score=0.80,
        agent_scores=scores,
        developer=DEVELOPER,
        branch=BRANCH,
        pipeline_name=PIPELINE,
        customer_id=CUSTOMER,
        db_pool=db,
    ))

    args = db.execute.call_args[0]
    assert json.loads(args[-1]) == scores


def test_write_scores_sql_contains_scores_ready_true():
    """The upsert SQL must set scores_ready = TRUE so dashboards can read early."""
    db = _db()

    _run(write_experiment_scores(
        experiment_id=EXP_ID,
        pipeline_score=0.80,
        agent_scores={},
        developer=DEVELOPER,
        branch=BRANCH,
        pipeline_name=PIPELINE,
        customer_id=CUSTOMER,
        db_pool=db,
    ))

    sql = db.execute.call_args[0][0].lower()
    assert "scores_ready" in sql
    assert "true" in sql


# ---------------------------------------------------------------------------
# write_experiment_scores — delta computation
# ---------------------------------------------------------------------------

def test_delta_prev_computed_as_current_minus_previous():
    db = _db(fetchrow_results=[_row(0.70), None])  # prev=0.70, no main baseline

    _run(write_experiment_scores(
        experiment_id=EXP_ID,
        pipeline_score=0.80,
        agent_scores={},
        developer=DEVELOPER,
        branch=BRANCH,
        pipeline_name=PIPELINE,
        customer_id=CUSTOMER,
        db_pool=db,
    ))

    # SQL arg order: sql, exp_id, pipeline_name, customer_id, developer, branch,
    #               pipeline_score, delta_prev, delta_main, agent_scores_json
    args = db.execute.call_args[0]
    delta_prev = args[7]
    assert delta_prev == pytest.approx(0.10, abs=1e-9)


def test_delta_main_computed_as_current_minus_main_baseline():
    db = _db(fetchrow_results=[None, _row(0.75)])  # no prev, main baseline=0.75

    _run(write_experiment_scores(
        experiment_id=EXP_ID,
        pipeline_score=0.85,
        agent_scores={},
        developer=DEVELOPER,
        branch=BRANCH,
        pipeline_name=PIPELINE,
        customer_id=CUSTOMER,
        db_pool=db,
    ))

    args = db.execute.call_args[0]
    delta_main = args[8]
    assert delta_main == pytest.approx(0.10, abs=1e-9)


def test_both_deltas_none_when_no_previous_run():
    db = _db(fetchrow_results=[None, None])

    _run(write_experiment_scores(
        experiment_id=EXP_ID,
        pipeline_score=0.80,
        agent_scores={},
        developer=DEVELOPER,
        branch=BRANCH,
        pipeline_name=PIPELINE,
        customer_id=CUSTOMER,
        db_pool=db,
    ))

    args = db.execute.call_args[0]
    assert args[7] is None  # delta_prev
    assert args[8] is None  # delta_main


def test_delta_queries_exclude_current_experiment_id():
    """Look-back queries must exclude the current experiment to avoid self-reference."""
    db = _db()

    _run(write_experiment_scores(
        experiment_id=EXP_ID,
        pipeline_score=0.80,
        agent_scores={},
        developer=DEVELOPER,
        branch=BRANCH,
        pipeline_name=PIPELINE,
        customer_id=CUSTOMER,
        db_pool=db,
    ))

    first_call_args = db.fetchrow.call_args_list[0][0]
    second_call_args = db.fetchrow.call_args_list[1][0]
    assert EXP_ID in first_call_args
    assert EXP_ID in second_call_args


def test_two_delta_queries_issued():
    """Exactly two fetchrow calls: one for prev-on-branch, one for main baseline."""
    db = _db()

    _run(write_experiment_scores(
        experiment_id=EXP_ID,
        pipeline_score=0.80,
        agent_scores={},
        developer=DEVELOPER,
        branch=BRANCH,
        pipeline_name=PIPELINE,
        customer_id=CUSTOMER,
        db_pool=db,
    ))

    assert db.fetchrow.call_count == 2


def test_negative_delta_when_score_regressed():
    db = _db(fetchrow_results=[_row(0.90), None])

    _run(write_experiment_scores(
        experiment_id=EXP_ID,
        pipeline_score=0.80,
        agent_scores={},
        developer=DEVELOPER,
        branch=BRANCH,
        pipeline_name=PIPELINE,
        customer_id=CUSTOMER,
        db_pool=db,
    ))

    args = db.execute.call_args[0]
    assert args[7] == pytest.approx(-0.10, abs=1e-9)


# ---------------------------------------------------------------------------
# write_experiment_attribution — basic behavior
# ---------------------------------------------------------------------------

def _run_attribution(db, filtered_diff="diff --git a/prompts/f.txt\n+line\n", original_files=None):
    return _run(write_experiment_attribution(
        experiment_id=EXP_ID,
        title="Improved rubric",
        description="Score assignor weights relevance higher.",
        impact="positive",
        affected_agents=["score_assignor"],
        confidence=0.88,
        changed_files=["prompts/rfp_rubric.txt"],
        relevant_files=["prompts/rfp_rubric.txt"],
        filtered_diff=filtered_diff,
        original_files=original_files or {"prompts/rfp_rubric.txt": "old"},
        db_pool=db,
    ))


def test_write_attribution_calls_execute_once():
    db = _db()
    _run_attribution(db)
    db.execute.assert_called_once()


def test_write_attribution_passes_experiment_id():
    db = _db()
    _run_attribution(db)
    positional = db.execute.call_args[0]
    assert positional[1] == EXP_ID


def test_write_attribution_does_not_call_fetchrow():
    """Attribution write is a pure UPDATE — no delta queries needed."""
    db = _db()
    _run_attribution(db)
    db.fetchrow.assert_not_called()


def test_write_attribution_sql_contains_attribution_ready_true():
    db = _db()
    _run_attribution(db)
    sql = db.execute.call_args[0][0].lower()
    assert "attribution_ready" in sql
    assert "true" in sql


def test_write_attribution_sql_is_update_not_insert():
    db = _db()
    _run_attribution(db)
    sql = db.execute.call_args[0][0].strip().lower()
    assert sql.startswith("update")


def test_write_attribution_serializes_original_files_as_json():
    db = _db()
    files = {"prompts/rubric.txt": "original content"}
    _run_attribution(db, original_files=files)
    args = db.execute.call_args[0]
    # original_files is args[-2] (before is_no_diff)
    assert json.loads(args[-2]) == files


# ---------------------------------------------------------------------------
# is_no_diff_run flag
# ---------------------------------------------------------------------------

def test_is_no_diff_run_true_when_filtered_diff_empty():
    db = _db()
    _run_attribution(db, filtered_diff="")
    args = db.execute.call_args[0]
    assert args[-1] is True


def test_is_no_diff_run_true_when_filtered_diff_whitespace_only():
    db = _db()
    _run_attribution(db, filtered_diff="   \n  ")
    args = db.execute.call_args[0]
    assert args[-1] is True


def test_is_no_diff_run_false_when_diff_present():
    db = _db()
    _run_attribution(db, filtered_diff="diff --git a/prompts/file.txt\n+new\n")
    args = db.execute.call_args[0]
    assert args[-1] is False


# ---------------------------------------------------------------------------
# Independence: both functions work without each other
# ---------------------------------------------------------------------------

def test_scores_independent_of_attribution():
    """write_experiment_scores works standalone with no attribution data."""
    db = _db()
    _run(write_experiment_scores(
        experiment_id=EXP_ID,
        pipeline_score=0.75,
        agent_scores={},
        developer=DEVELOPER,
        branch=BRANCH,
        pipeline_name=PIPELINE,
        customer_id=CUSTOMER,
        db_pool=db,
    ))
    db.execute.assert_called_once()


def test_attribution_independent_of_scores():
    """write_experiment_attribution works standalone with no scores data."""
    db = _db()
    _run_attribution(db)
    db.execute.assert_called_once()


def test_both_functions_use_separate_execute_calls():
    """Each function issues exactly one execute — total of 2 when called together."""
    db = _db()
    _run(write_experiment_scores(
        experiment_id=EXP_ID,
        pipeline_score=0.80,
        agent_scores={},
        developer=DEVELOPER,
        branch=BRANCH,
        pipeline_name=PIPELINE,
        customer_id=CUSTOMER,
        db_pool=db,
    ))
    _run_attribution(db)
    assert db.execute.call_count == 2


# ---------------------------------------------------------------------------
# Dashboard query support
# ---------------------------------------------------------------------------

def test_scores_ready_set_before_attribution_ready():
    """
    Dashboard query: after write_experiment_scores, scores_ready=TRUE even if
    attribution_ready is still FALSE. The SQL must include ON CONFLICT
    so the first writer (eval-worker) can create the row.
    """
    db = _db()
    _run(write_experiment_scores(
        experiment_id=EXP_ID,
        pipeline_score=0.80,
        agent_scores={},
        developer=DEVELOPER,
        branch=BRANCH,
        pipeline_name=PIPELINE,
        customer_id=CUSTOMER,
        db_pool=db,
    ))

    sql = db.execute.call_args[0][0].lower()
    # Must be an INSERT (not just an UPDATE) so the row exists before attribution
    assert "insert" in sql
    assert "on conflict" in sql
    assert "scores_ready" in sql
