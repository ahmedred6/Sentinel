-- 003_experiments.sql
-- experiments table for the Diff Analyzer Module (DIFF-03)
--
-- Two workers write to this table independently:
--   eval-worker           → pipeline_score, agent_scores, scores_ready
--   diff-analyzer-worker  → attribution fields, diff fields, attribution_ready
--
-- The table supports partial writes: scores_ready may be TRUE while
-- attribution_ready is still FALSE (eval runs faster than attribution).
-- Dashboards query WHERE scores_ready = TRUE to show results immediately.
-- All statements use IF NOT EXISTS — safe to re-run.

-- -----------------------------------------------------------------------
-- Core experiments table
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS experiments (
    experiment_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id                TEXT,
    pipeline_name         TEXT NOT NULL,
    customer_id           TEXT NOT NULL,
    developer             TEXT NOT NULL,
    branch                TEXT,
    base_branch           TEXT,
    commit_sha            TEXT,

    -- Scores written by eval-worker
    pipeline_score        FLOAT,
    score_delta_vs_prev   FLOAT,       -- vs developer's previous run on same branch
    score_delta_vs_main   FLOAT,       -- vs latest run on origin/main
    agent_scores          JSONB,       -- {"layer1": 0.9, "layer2": 0.75, ...}
    scores_ready          BOOLEAN DEFAULT FALSE,

    -- Diff metadata written by diff-analyzer-worker
    changed_files         TEXT[],
    relevant_files        TEXT[],
    filtered_diff         TEXT,
    original_files        JSONB,       -- {"filepath": "original content", ...}

    -- Attribution written by diff-analyzer-worker (after LLM analysis)
    title                 TEXT,
    description           TEXT,
    impact                TEXT CHECK (impact IN ('positive', 'negative', 'neutral', 'unstable')),
    affected_agents       TEXT[],
    confidence            FLOAT,
    attribution_ready     BOOLEAN DEFAULT FALSE,

    -- Set TRUE when the developer ran the pipeline with no LLM-relevant diff
    is_no_diff_run        BOOLEAN DEFAULT FALSE,

    created_at            TIMESTAMPTZ DEFAULT NOW()
);

-- -----------------------------------------------------------------------
-- Indexes
-- -----------------------------------------------------------------------

-- Dashboard: list runs per pipeline per developer, newest first
CREATE INDEX IF NOT EXISTS idx_experiments_pipeline_developer
    ON experiments (pipeline_name, developer, created_at DESC);

-- Regression detection: fetch baseline scores from origin/main
CREATE INDEX IF NOT EXISTS idx_experiments_branch
    ON experiments (branch, created_at DESC);

-- Partial result polling: find experiments with scores but not yet attribution
CREATE INDEX IF NOT EXISTS idx_experiments_scores_ready
    ON experiments (scores_ready, created_at DESC)
    WHERE scores_ready = TRUE;
