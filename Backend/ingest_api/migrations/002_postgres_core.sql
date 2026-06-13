-- 002_postgres_core.sql
-- PostgreSQL core tables: api_keys, customers, scores, golden_dataset
--
-- All tables use IF NOT EXISTS so this script is safe to re-run.

-- -----------------------------------------------------------------------
-- API keys (INGEST-02 auth)
-- key_hash is SHA-256 of the raw key — plaintext never stored
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS api_keys (
    key_hash    TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    is_active   BOOLEAN DEFAULT TRUE
);

-- -----------------------------------------------------------------------
-- Customers
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY,
    name        TEXT,
    flow_names  TEXT[],
    vertical    TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- -----------------------------------------------------------------------
-- Scores — one row per eval result for a trace
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS scores (
    score_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trace_id     TEXT NOT NULL,
    customer_id  TEXT NOT NULL,
    failure_type TEXT,
    severity     TEXT CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    confidence   FLOAT,
    evidence     TEXT,
    layer        INTEGER CHECK (layer IN (1, 2, 3)),
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scores_trace_id     ON scores (trace_id);
CREATE INDEX IF NOT EXISTS idx_scores_customer_id  ON scores (customer_id);
CREATE INDEX IF NOT EXISTS idx_scores_created_at   ON scores (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_scores_severity     ON scores (severity) WHERE severity IS NOT NULL;

-- -----------------------------------------------------------------------
-- Golden dataset — human-reviewed labels for evaluating the eval engine
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS golden_dataset (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trace_id      TEXT NOT NULL,
    customer_id   TEXT NOT NULL,
    correct_label BOOLEAN,
    reviewer_note TEXT,
    reviewed_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_golden_trace_id    ON golden_dataset (trace_id);
CREATE INDEX IF NOT EXISTS idx_golden_customer_id ON golden_dataset (customer_id);
