"""
create_week3_tickets.py

Creates all 5 Week 5 (Module - Diff Analyzer) tickets in the Sentinel
GitHub repo and adds them to your GitHub Projects v2 kanban board.

Requirements:
    pip install requests

Setup:
    1. Go to https://github.com/settings/tokens
    2. Generate a classic Personal Access Token with scopes:
         - repo (full)
         - project (full)
    3. Set it as an environment variable:
         export GITHUB_TOKEN=ghp_your_token_here
    4. Run:
         python create_week3_tickets.py
"""

import os
import sys
import requests
import json
from dotenv import load_dotenv
load_dotenv()
# ─────────────────────────────────────────────
# CONFIG — edit if needed
# ─────────────────────────────────────────────
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
REPO_OWNER   = "ahmedred6"
REPO_NAME    = "Sentinel"
PROJECT_NUMBER = 1   # change this if your kanban project number is different

REST_URL     = "https://api.github.com"
GRAPHQL_URL  = "https://api.github.com/graphql"

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}

# ─────────────────────────────────────────────
# TICKETS
# ─────────────────────────────────────────────
TICKETS = [
    {
        "title": "[DIFF-01] Extend SDK with git diff capture and run metadata",
        "labels": ["core-path", "module-diff-analyzer", "week-5", "p0"],
        "body": """## Context
The diff analyzer depends on the SDK capturing two things automatically on every pipeline run: the git diff of the developer's current changes, and metadata about who ran it and on which branch. This must happen silently in the background — the developer should never have to think about it.

## Task
Extend the existing SDK (built in SDK-02) with git diff capture, Stage 1 file filtering, and run metadata collection.

## Implementation details

### Step 1 — Find git repo root
```python
def _find_git_root() -> str | None:
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True, text=True, timeout=3
    )
    return result.stdout.strip() if result.returncode == 0 else None
```

### Step 2 — Capture diff against origin/main
```python
def _capture_git_diff(root: str) -> str | None:
    # Primary: diff against origin/main
    result = subprocess.run(
        ["git", "diff", "HEAD", "origin/main"],
        capture_output=True, text=True, cwd=root, timeout=5
    )
    if result.returncode == 0:
        return result.stdout or None  # None if no changes vs main

    # Fallback: diff against last local commit
    result = subprocess.run(
        ["git", "diff", "HEAD"],
        capture_output=True, text=True, cwd=root, timeout=5
    )
    return result.stdout or None
```

### Step 3 — Stage 1 file filter
```python
LLM_RELEVANT_PATTERNS = [
    "**/*.txt", "**/prompts/**", "**/agents/**/*.py",
    "**/tools/**/*.py", "**/schemas/**",
    "**/config/models*", "**/retrieval/**", "**/rubrics/**",
]
ALWAYS_IRRELEVANT_PATTERNS = [
    "**/tests/**", "**/*.md", "**/frontend/**",
    "requirements.txt", "docker-compose*",
    "**/.env*", "**/migrations/**", "**/*.css", "**/*.html",
]

def _filter_diff(raw_diff: str) -> str:
    # Parse diff into per-file hunks
    # Keep only hunks from LLM_RELEVANT files
    # Remove hunks from ALWAYS_IRRELEVANT files
    # Return filtered diff string
    ...
```

### Step 4 — Capture branch and commit metadata
```python
def _get_git_metadata(root: str) -> dict:
    branch = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        capture_output=True, text=True, cwd=root
    ).stdout.strip()

    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        capture_output=True, text=True, cwd=root
    ).stdout.strip()

    return {"branch": branch, "commit_sha": commit}
```

### Step 5 — Extend sentinel.init()
```python
sentinel.init(
    api_key="sk_staging_xxx",
    environment="staging",
    developer="ahmed",        # who is running — required
    run_id="optional-label",  # auto-generated UUID if omitted
)
# branch and commit_sha auto-detected from git
```

### Step 6 — Ship diff alongside trace
The background shipper (SDK-02) sends a second payload to `POST /ingest/diff` after sending the trace. The diff payload is fire-and-forget — if it fails, it is logged and dropped. It never affects the trace delivery.

```python
@dataclass
class DiffPayload:
    experiment_id: str      # shared with the trace
    pipeline_name: str
    developer: str
    branch: str
    base_branch: str        # "origin/main" or "local HEAD"
    commit_sha: str
    filtered_diff: str      # output of Stage 1 filter
    original_files: dict    # filename -> original content (before changes)
    customer_id: str
    timestamp: datetime
```

### Important: original file content
For each file that appears in the filtered diff, capture the original version before the change using:
```python
git show origin/main:<filepath>
```
This gives the analyzer the "before" state of each changed file. Do not send the entire project — only the files that have changes.

## File location
`backend/sentinel_sdk/sentinel/diff_capture.py`
`backend/sentinel_sdk/sentinel/schema.py` (add DiffPayload)

## Acceptance criteria
- [ ] `_find_git_root()` works from any subdirectory within a repo
- [ ] Diff captured against origin/main with fallback to local HEAD
- [ ] Stage 1 filter correctly includes LLM-relevant files and excludes irrelevant ones
- [ ] Original file content captured for each changed file via `git show`
- [ ] Branch and commit SHA auto-detected from git
- [ ] `sentinel.init()` accepts `developer` and optional `run_id`
- [ ] DiffPayload schema defined and validated with Pydantic
- [ ] Diff capture runs on background thread — never blocks pipeline execution
- [ ] All git failures handled silently — run always proceeds regardless
- [ ] Unit tests: repo found, repo not found, no changes vs main, empty diff after filter
"""
    },
    {
        "title": "[DIFF-02] Add /ingest/diff endpoint and sentinel:diffs Redis Stream",
        "labels": ["core-path", "module-diff-analyzer", "week-5", "p0"],
        "body": """## Context
The ingest API needs a new endpoint to receive DiffPayload from the SDK, and a second Redis Stream to carry diff payloads to the diff-analyzer-worker independently of the trace stream.

## Task
Add `POST /ingest/diff` to the FastAPI ingest API and configure the `sentinel:diffs` Redis Stream with its consumer group.

## Implementation details

### New endpoint
```python
@app.post("/ingest/diff")
async def ingest_diff(payload: DiffPayload, customer_id: str = Depends(get_customer_id)):
    # Auth and rate limiting — same middleware as /ingest/trace
    # No dedup check needed — diffs are not idempotent
    await enqueue_diff(payload, redis)
    return {"status": "ok", "experiment_id": payload.experiment_id}
```

### New Redis Stream
```python
# Stream: sentinel:diffs
# Consumer group: diff-analyzer
# One worker consumes this group

async def enqueue_diff(payload: DiffPayload, redis: Redis):
    await redis.xadd(
        "sentinel:diffs",
        {"payload": payload.model_dump_json()},
        maxlen=50_000,
    )
```

### Stream configuration
- Stream name: `sentinel:diffs`
- Consumer group: `diff-analyzer`
- Max length: 50,000 messages (diffs are larger than traces)
- Messages persist 48 hours (longer than traces — diffs are rarer and more valuable)

### experiment_id linkage
The `experiment_id` field in DiffPayload must match the `experiment_id` in the corresponding TracePayload. This is how the two workers (eval-worker and diff-analyzer-worker) write their results to the same experiment record in PostgreSQL. The SDK generates experiment_id once per pipeline run and attaches it to both payloads.

## File location
`backend/ingest_api/main.py` (new endpoint)
`backend/ingest_api/streams.py` (new enqueue_diff function)

## Acceptance criteria
- [ ] `POST /ingest/diff` accepts valid DiffPayload and returns 200
- [ ] Invalid schema returns 422
- [ ] Payload enqueued to `sentinel:diffs` stream
- [ ] `sentinel:diffs` stream created with `diff-analyzer` consumer group
- [ ] Stream capped at 50,000 messages
- [ ] experiment_id confirmed present in payload and stored in stream message
- [ ] Auth and rate limiting applied same as /ingest/trace
- [ ] Integration test: SDK ships diff → endpoint receives → message appears in stream
"""
    },
    {
        "title": "[DIFF-03] Create experiments table in PostgreSQL",
        "labels": ["infrastructure", "module-diff-analyzer", "week-5"],
        "body": """## Context
The experiments table is where the eval-worker and diff-analyzer-worker write their results independently. It joins run scores with diff attribution into one record that the dashboard reads. It needs to support partial writes — scores arrive before attribution.

## Task
Add the experiments table to the PostgreSQL migration script (INGEST-04) and implement the write functions used by both workers.

## Schema
```sql
CREATE TABLE experiments (
    experiment_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id                TEXT,
    pipeline_name         TEXT NOT NULL,
    customer_id           TEXT NOT NULL,
    developer             TEXT NOT NULL,
    branch                TEXT,
    base_branch           TEXT,
    commit_sha            TEXT,

    -- Scores (written by eval-worker)
    pipeline_score        FLOAT,
    score_delta_vs_prev   FLOAT,   -- vs previous run on same branch
    score_delta_vs_main   FLOAT,   -- vs latest run on origin/main
    agent_scores          JSONB,
    scores_ready          BOOLEAN DEFAULT FALSE,

    -- Diff (written by diff-analyzer-worker)
    changed_files         TEXT[],
    relevant_files        TEXT[],
    filtered_diff         TEXT,
    original_files        JSONB,   -- filename -> original content

    -- Attribution (written by diff-analyzer-worker)
    title                 TEXT,
    description           TEXT,
    impact                TEXT CHECK (impact IN ('positive','negative','neutral','unstable')),
    affected_agents       TEXT[],
    confidence            FLOAT,
    attribution_ready     BOOLEAN DEFAULT FALSE,

    -- No-diff case
    is_no_diff_run        BOOLEAN DEFAULT FALSE,

    created_at            TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_experiments_pipeline_developer
    ON experiments (pipeline_name, developer, created_at DESC);

CREATE INDEX idx_experiments_branch
    ON experiments (branch, created_at DESC);
```

## Write functions

### Called by eval-worker after scoring
```python
async def write_experiment_scores(
    experiment_id: str,
    pipeline_score: float,
    agent_scores: dict,
    developer: str,
    branch: str,
    pipeline_name: str,
    customer_id: str,
):
    # Compute score_delta_vs_prev:
    # SELECT pipeline_score FROM experiments
    # WHERE developer = ? AND branch = ? AND pipeline_name = ?
    # ORDER BY created_at DESC LIMIT 1

    # Compute score_delta_vs_main:
    # SELECT pipeline_score FROM experiments
    # WHERE branch = 'origin/main' AND pipeline_name = ?
    # ORDER BY created_at DESC LIMIT 1

    # Upsert into experiments with scores_ready = TRUE
    ...
```

### Called by diff-analyzer-worker after attribution
```python
async def write_experiment_attribution(
    experiment_id: str,
    title: str,
    description: str,
    impact: str,
    affected_agents: list[str],
    confidence: float,
    changed_files: list[str],
    relevant_files: list[str],
):
    # UPDATE experiments SET
    #   title = ?, description = ?, impact = ?,
    #   affected_agents = ?, confidence = ?,
    #   changed_files = ?, relevant_files = ?,
    #   attribution_ready = TRUE
    # WHERE experiment_id = ?
    ...
```

## File location
`backend/ingest_api/migrations/003_experiments.sql`
`backend/eval_engine/experiments.py`

## Acceptance criteria
- [ ] Experiments table created with all columns and indexes
- [ ] `write_experiment_scores()` correctly computes both deltas
- [ ] `write_experiment_attribution()` updates correct record by experiment_id
- [ ] Both functions can write independently without conflict
- [ ] `is_no_diff_run` correctly set when filtered_diff is empty
- [ ] Dashboard query confirmed: can fetch experiments with scores_ready=TRUE before attribution_ready=TRUE
- [ ] Unit tests: write scores only, write attribution only, write both, compute delta with no previous run
"""
    },
    {
        "title": "[DIFF-04] Build diff-analyzer-worker",
        "labels": ["core-path", "module-diff-analyzer", "week-5", "p0"],
        "body": """## Context
The diff-analyzer-worker is the brain of this module. It consumes from the sentinel:diffs stream, runs a two-stage filter to isolate the most causally relevant changes, and uses Claude Sonnet to generate a human-readable attribution connecting the code change to the observed score delta.

## Task
Build the async worker that consumes from `sentinel:diffs`, runs Stage 2 semantic filtering, and generates attribution using Claude Sonnet.

## Implementation details

### Consumer loop
```python
async def run():
    while True:
        messages = await redis.xreadgroup(
            groupname="diff-analyzer",
            consumername="diff-analyzer-1",
            streams={"sentinel:diffs": ">"},
            count=5,
            block=2000,
        )
        for stream, entries in messages:
            for msg_id, data in entries:
                payload = DiffPayload.model_validate_json(data[b"payload"])
                await process_diff(payload, msg_id)
```

### Stage 2 semantic filter (Claude Haiku)
```python
async def stage2_filter(filtered_diff: str) -> str:
    if not filtered_diff:
        return ""

    response = await haiku(f'''
        Given these code changes in LLM-relevant files,
        which specific hunks could plausibly affect LLM output quality?

        Ignore: logging changes, error handling, formatting,
        comments, variable renames that don't change logic.

        Return only the relevant hunks with one-line reasoning each.
        If nothing is relevant, return empty string.

        Diff:
        {filtered_diff}
    ''')
    return response
```

### Attribution (Claude Sonnet)
```python
async def generate_attribution(
    semantic_diff: str,
    original_files: dict,
    agent_scores: dict,
    score_delta_vs_prev: float,
    score_delta_vs_main: float,
) -> dict:

    if not semantic_diff:
        # No-diff case — score changed without code changes
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

    # Build context: original files + what changed
    file_context = ""
    for filename, original_content in original_files.items():
        file_context += f"\\n--- ORIGINAL: {filename} ---\\n{original_content}\\n"

    response = await sonnet(f'''
        You are analyzing a code change made to an LLM pipeline.

        ORIGINAL FILE CONTENT (before changes):
        {file_context}

        WHAT CHANGED (diff — only LLM-relevant hunks):
        {semantic_diff}

        SCORE BEFORE THIS RUN: see agent_scores context
        SCORE DELTA VS PREVIOUS RUN: {score_delta_vs_prev:+.2f}
        SCORE DELTA VS MAIN BRANCH:  {score_delta_vs_main:+.2f}
        AGENT SCORES THIS RUN: {agent_scores}

        Based on the original file content and what changed,
        explain what the developer modified and why it caused
        this specific score delta. Be precise — quote the
        exact change and connect it to the specific agent and
        dimension that was affected.

        Return JSON only:
        {{
            "title": "under 8 words describing the change",
            "description": "2-3 sentences connecting the specific change to the score delta",
            "impact": "positive|negative|neutral|unstable",
            "affected_agents": ["agent-name"],
            "confidence": 0.0-1.0
        }}
    ''')

    return parse_json_response(response)
```

### Full process_diff flow
```python
async def process_diff(payload: DiffPayload, msg_id: str):
    try:
        # Wait for scores to be ready (poll with timeout)
        scores = await wait_for_scores(payload.experiment_id, timeout=30)

        # Stage 2 filter
        semantic_diff = await stage2_filter(payload.filtered_diff)

        # Attribution
        attribution = await generate_attribution(
            semantic_diff=semantic_diff,
            original_files=payload.original_files,
            agent_scores=scores.agent_scores,
            score_delta_vs_prev=scores.score_delta_vs_prev,
            score_delta_vs_main=scores.score_delta_vs_main,
        )

        # Write to experiments table
        await write_experiment_attribution(
            experiment_id=payload.experiment_id,
            changed_files=list(payload.original_files.keys()),
            relevant_files=_extract_relevant_filenames(semantic_diff),
            **attribution,
        )

        await redis.xack("sentinel:diffs", "diff-analyzer", msg_id)

    except Exception as e:
        log.error(f"diff-analyzer failed for {payload.experiment_id}: {e}")
        await redis.xack("sentinel:diffs", "diff-analyzer", msg_id)
```

## Important: waiting for scores
The diff-analyzer-worker needs the score delta to generate attribution. Since eval-worker and diff-analyzer-worker run in parallel, scores may not be ready yet when the diff arrives. The worker polls PostgreSQL for up to 30 seconds before proceeding. If scores never arrive, it generates attribution without the delta and notes this in the description.

## File location
`backend/diff_analyzer/worker.py`
`backend/diff_analyzer/filter.py`
`backend/diff_analyzer/attribution.py`

## Acceptance criteria
- [ ] Worker consumes from `diff-analyzer` consumer group correctly
- [ ] Stage 2 filter removes irrelevant hunks correctly (tested with mixed diff)
- [ ] No-diff case produces correct "score drift detected" attribution
- [ ] Attribution JSON parsed correctly — invalid JSON returns neutral result and logs error
- [ ] Original file content used correctly — attribution quotes actual before-state
- [ ] Score delta wait logic works — polls with 30s timeout, proceeds gracefully on timeout
- [ ] XACK sent after write regardless of success or failure
- [ ] Deployed as separate Railway service
- [ ] Unit tests: with diff, without diff, invalid JSON response, scores timeout
"""
    },
    {
        "title": "[DIFF-05] Build experiment log page in developer dashboard",
        "labels": ["demo-visible", "module-diff-analyzer", "week-5", "p0"],
        "body": """## Context
The experiment log is the developer-facing page that makes the diff analyzer visible. It shows every run made by every developer on the team with auto-generated titles, score deltas, attribution, and the ability to compare any two runs. This page is required for the demo.

## Task
Build the experiment log page in the developer dashboard. It reads from the experiments table in PostgreSQL and updates progressively — scores appear immediately, attribution appears when ready.

## Page layout

### Header stats
- Total runs today across all developers
- Best run score today (and who made it)
- Worst run score today (and who made it)
- Number of no-diff drift runs detected today

### Experiment list (one row per run, sorted newest first)
Each row shows:
run_id   developer   branch   timestamp   score   delta_vs_prev   delta_vs_main   impact

Clicking a row expands it to show:
Title:          "Softened score cap from must to should"

Description:    "Developer changed must not exceed to should not exceed..."

Files changed:  prompts/score_assignor.txt (+1 -1)

Agents:         score-assignor

Confidence:     0.97

Agent scores:   fact-extractor 4.4 | keyword-searcher 2.8 | score-assignor 1.8

### Progressive loading behaviour
- Row appears as soon as scores_ready = TRUE
- Title and description show as "Analyzing changes..." until attribution_ready = TRUE
- Dashboard polls every 3 seconds for attribution updates
- No full page reload needed — only the pending rows update

### No-diff run display
Runs where is_no_diff_run = TRUE display with a distinct visual treatment:
~ unstable   "No code changes — score drift detected"

"Same code produced different scores. Pipeline is non-deterministic."

### Compare button
Any two runs can be compared side by side:
- Left: selected run A (agent scores, files changed, attribution)
- Right: selected run B (agent scores, files changed, attribution)
- Diff between the two diffs shown in the middle

### Actions per run
- Open failing traces in trace inspector
- Add traces from this run to golden dataset
- Mark as team baseline (sets this run as the reference for score_delta_vs_main)
- Copy experiment_id to clipboard

## Backend query (FastAPI)
```python
GET /experiments?pipeline=rfp-evaluator&limit=50&offset=0

Response:
{
  "experiments": [
    {
      "experiment_id": "...",
      "run_id": "fix-score-assignor-v2",
      "developer": "ahmed",
      "branch": "fix/score-assignor",
      "pipeline_score": 1.8,
      "score_delta_vs_prev": -2.4,
      "score_delta_vs_main": -2.4,
      "agent_scores": {...},
      "title": "Softened score cap from must to should",
      "description": "...",
      "impact": "negative",
      "affected_agents": ["score-assignor"],
      "confidence": 0.97,
      "changed_files": ["prompts/score_assignor.txt"],
      "scores_ready": true,
      "attribution_ready": true,
      "is_no_diff_run": false,
      "created_at": "2026-06-14T10:44:00Z"
    }
  ]
}
```

## Demo script requirement
This page must support the exact demo scenario from the architecture doc:
1. Baseline run shown with score 4.2
2. After one-word prompt change: new run appears with score 1.8, delta -2.4, title auto-generated
3. After revert: new run appears with score 4.1, delta +2.3, title auto-generated

The page must be ready to demo before any investor or design partner meeting.

## File location
`frontend/src/pages/ExperimentLog.tsx`
`backend/ingest_api/routes/experiments.py`

## Acceptance criteria
- [ ] Experiment list loads correctly from PostgreSQL
- [ ] Rows appear immediately when scores_ready = TRUE
- [ ] Attribution updates without page reload when attribution_ready = TRUE
- [ ] No-diff runs displayed with distinct unstable styling
- [ ] Compare view works for any two selected runs
- [ ] All three actions work: open traces, add to dataset, mark as baseline
- [ ] Backend query filters by pipeline, supports pagination
- [ ] Demo scenario runs end to end: baseline → change → new run appears → attribution loads
- [ ] Page works with 0 runs (empty state shown with onboarding instructions)
"""
    },
]

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def graphql(query: str, variables: dict | None = None) -> dict:
    r = requests.post(
        GRAPHQL_URL,
        headers=HEADERS,
        json={"query": query, "variables": variables or {}},
    )
    r.raise_for_status()
    data = r.json()
    if "errors" in data:
        raise RuntimeError(f"GraphQL error: {json.dumps(data['errors'], indent=2)}")
    return data["data"]


def ensure_labels(labels: list[str]):
    """Create labels that don't exist yet."""
    existing_r = requests.get(
        f"{REST_URL}/repos/{REPO_OWNER}/{REPO_NAME}/labels",
        headers=HEADERS,
        params={"per_page": 100},
    )
    existing_r.raise_for_status()
    existing = {l["name"] for l in existing_r.json()}

    color_map = {
        "core-path":             "0075ca",
        "module-diff-analyzer":  "1D76DB",
        "week-5":                "BFD4F2",
        "p0":                    "B60205",
        "infrastructure":        "E4E669",
        "demo-visible":          "0E8A16",
        "nice-to-have":          "FEF2C0",
    }

    for label in labels:
        if label not in existing:
            color = color_map.get(label, "CCCCCC")
            r = requests.post(
                f"{REST_URL}/repos/{REPO_OWNER}/{REPO_NAME}/labels",
                headers=HEADERS,
                json={"name": label, "color": color},
            )
            if r.status_code == 422:
                pass  # already exists (race condition)
            else:
                r.raise_for_status()
                print(f"  ✓ Created label: {label}")


def create_issue(title: str, body: str, labels: list[str]) -> tuple[int, str]:
    """Creates a GitHub issue. Returns (issue_number, node_id)."""
    r = requests.post(
        f"{REST_URL}/repos/{REPO_OWNER}/{REPO_NAME}/issues",
        headers=HEADERS,
        json={"title": title, "body": body, "labels": labels},
    )
    r.raise_for_status()
    data = r.json()
    return data["number"], data["node_id"]


def get_project_node_id(project_number: int) -> tuple[str, str]:
    """Gets the GraphQL node ID for the user's project by number."""
    query = """
    query($login: String!, $number: Int!) {
      user(login: $login) {
        projectV2(number: $number) {
          id
          title
        }
      }
    }
    """
    data = graphql(query, {"login": REPO_OWNER, "number": project_number})
    project = data.get("user", {}).get("projectV2")
    if not project:
        raise RuntimeError(
            f"Project #{project_number} not found for user '{REPO_OWNER}'.\n"
            f"Check your PROJECT_NUMBER at the top of this script.\n"
            f"Find it in the URL: github.com/users/{REPO_OWNER}/projects/N"
        )
    return project["id"], project["title"]


def add_issue_to_project(project_node_id: str, issue_node_id: str) -> str:
    """Adds an issue to a Projects v2 board. Returns the project item ID."""
    mutation = """
    mutation($projectId: ID!, $contentId: ID!) {
      addProjectV2ItemById(input: {projectId: $projectId, contentId: $contentId}) {
        item {
          id
        }
      }
    }
    """
    data = graphql(mutation, {"projectId": project_node_id, "contentId": issue_node_id})
    return data["addProjectV2ItemById"]["item"]["id"]


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    if not GITHUB_TOKEN:
        print("ERROR: GITHUB_TOKEN environment variable not set.")
        print("Run: export GITHUB_TOKEN=ghp_your_token_here")
        sys.exit(1)

    print(f"\n{'─'*55}")
    print(f"  Sentinel — Week 5 (DIFF) Ticket Creator")
    print(f"  Repo: {REPO_OWNER}/{REPO_NAME}")
    print(f"{'─'*55}\n")

    # 1. Get project node ID
    print(f"Looking up project #{PROJECT_NUMBER}...")
    try:
        project_node_id, project_title = get_project_node_id(PROJECT_NUMBER)
        print(f"✓ Found project: '{project_title}' ({project_node_id})\n")
    except RuntimeError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    # 2. Create all labels up front
    print("Ensuring labels exist...")
    all_labels = set()
    for ticket in TICKETS:
        all_labels.update(ticket["labels"])
    ensure_labels(list(all_labels))
    print()

    # 3. Create each issue and add to project
    created = []
    for i, ticket in enumerate(TICKETS, 1):
        print(f"[{i}/{len(TICKETS)}] Creating: {ticket['title']}")

        # Create the issue
        issue_number, issue_node_id = create_issue(
            title=ticket["title"],
            body=ticket["body"],
            labels=ticket["labels"],
        )
        print(f"  ✓ Issue #{issue_number} created")

        # Add to kanban project
        item_id = add_issue_to_project(project_node_id, issue_node_id)
        print(f"  ✓ Added to project board (item: {item_id})")
        print(f"  → https://github.com/{REPO_OWNER}/{REPO_NAME}/issues/{issue_number}\n")

        created.append((issue_number, ticket["title"]))

    # 4. Summary
    print(f"{'─'*55}")
    print(f"  Done! {len(created)} tickets created and added to kanban.\n")
    for num, title in created:
        print(f"  #{num}  {title}")
    print(f"\n  Board: https://github.com/users/{REPO_OWNER}/projects/{PROJECT_NUMBER}")
    print(f"{'─'*55}\n")


if __name__ == "__main__":
    main()