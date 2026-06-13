-- 001_clickhouse_traces.sql
-- ClickHouse raw traces table
--
-- Engine  : MergeTree — columnar, append-only, optimised for time-range scans
-- ORDER BY: (customer_id, timestamp) — primary sort key; filters by customer are free
-- PARTITION: toYYYYMM(timestamp) — one partition per month for cheap DROP PARTITION TTL

CREATE TABLE IF NOT EXISTS traces
(
    trace_id          String,
    customer_id       String,
    session_id        String,
    user_id           Nullable(String),
    flow_name         String,
    environment       String,
    model_name        String,
    user_prompt       String,
    llm_response      String,
    latency_ms        UInt32,
    token_total       UInt32,
    retrieved_context String,   -- JSON array of ContextDocument
    tool_calls        String,   -- JSON array of ToolCall
    message_history   String,   -- JSON array of MessageTurn
    timestamp         DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
ORDER BY (customer_id, timestamp)
PARTITION BY toYYYYMM(timestamp);
