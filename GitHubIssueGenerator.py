"""
create_week3_tickets.py

Creates all 6 Week 3 (Module 2 - Ingest Pipeline) tickets in the Sentinel
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
        "title": "[EVAL-01] Build Redis eval-worker consumer",
        "labels": ["core-path", "module-3-eval", "week-5"],
        "body": """## Context
The eval-worker is the entry point of the eval engine. It reads traces from the Redis Stream and dispatches each one through all three evaluation layers. It is the orchestrator — it doesn't do the scoring itself, it coordinates the layers and writes the final result.

## Task
Build the async worker that consumes from the `eval-worker` consumer group and dispatches each trace to Layer 1, Layer 2, and Layer 3.

## Implementation details

### Consumer loop
```python
async def run():
    while True:
        messages = await redis.xreadgroup(
            groupname="eval-worker",
            consumername="eval-worker-1",
            streams={"sentinel:traces": ">"},
            count=10,
            block=2000,
        )
        for stream, entries in messages:
            for msg_id, data in entries:
                trace = TracePayload.model_validate_json(data[b"payload"])
                await process_trace(trace, msg_id)

async def process_trace(trace: TracePayload, msg_id: str):
    l1 = await run_layer1(trace)
    l2 = await run_layer2(trace)
    l3 = await run_layer3(trace)
    report = aggregate(l1, l2, l3)
    await write_score(report)
    await redis.xack("sentinel:traces", "eval-worker", msg_id)
```

### Important
- ACK only after score is written to PostgreSQL
- If any layer throws, log the error and ACK anyway — never leave a message unACKed permanently
- Layers 1 and 2 run immediately; Layer 3 runs async (don't block on it)

## File location
`backend/eval_engine/worker.py`

## Acceptance criteria
- [ ] Worker consumes from `eval-worker` consumer group correctly
- [ ] Each trace dispatched to all three layers
- [ ] ACK sent only after score written to PostgreSQL
- [ ] Failed layer logged but does not crash the worker
- [ ] Worker tested with 10 real traces from Redis
- [ ] Deployed as separate service from ingest API
"""
    },
    {
        "title": "[EVAL-02] Implement Layer 1 — heuristic checks",
        "labels": ["core-path", "demo-visible", "module-3-eval", "week-5"],
        "body": """## Context
Layer 1 is the fastest and cheapest layer. Pure deterministic logic — no API calls, no ML, no latency. It catches clear-cut failures in milliseconds and is the first thing that runs on every trace.

## Task
Implement all 7 heuristic checks as a single `run_layer1(trace)` function that returns a list of findings.

## The 7 checks

### 1. Response length anomaly
- Trigger: complex question (user prompt > 50 words) answered in under 20 words
- Severity: medium
- Evidence: `f"Complex question answered in {word_count} words"`

### 2. Context contradiction
- Trigger: LLM response contains a claim that directly opposes a statement in retrieved context
- Implementation: for each retrieved document, check if response contradicts key claims using sentence-level comparison
- Severity: high
- Evidence: `f"Response says '{claim}' but context states '{contradiction}'"`

### 3. Repetition loop
- Trigger: same response pattern delivered 3+ times in message history
- Implementation: compare response against last 5 assistant turns using similarity ratio > 0.85
- Severity: medium
- Evidence: `f"Response repeated {n} times in session"`

### 4. Refusal when answer exists
- Trigger: response contains refusal phrases ("I don't know", "I can't help", "I'm not sure") but retrieved context clearly contains the answer
- Severity: high
- Evidence: `f"AI refused but context contains: '{relevant_excerpt}'"`

### 5. Tool call schema failure
- Trigger: tool call input does not match expected schema (missing required fields or wrong types)
- Severity: high
- Evidence: `f"Tool '{tool_name}' called with invalid schema: missing field '{field}'"`

### 6. Context window exhaustion
- Trigger: total token count in message history exceeds 80% of model context limit
- Model limits: gpt-4.1 = 128k, claude-sonnet = 200k, default = 8k
- Severity: medium
- Evidence: `f"Context at {pct}% of {model_name} limit ({token_count}/{max_tokens} tokens)"`

### 7. Hallucinated citation
- Trigger: response references a source (e.g. "according to policy X") not present in retrieved_context sources
- Severity: high
- Evidence: `f"AI cited '{source}' which was not in retrieved context"`

## Output format
```python
@dataclass
class Layer1Finding:
    check_name: str
    triggered: bool
    severity: str  # "low" | "medium" | "high" | "critical"
    evidence: str
    confidence: float  # 0.0 - 1.0
```

## File location
`backend/eval_engine/layer1.py`

## Acceptance criteria
- [ ] All 7 checks implemented and individually tested
- [ ] Each check returns a `Layer1Finding` regardless of whether it triggered
- [ ] Context contradiction tested against the demo scenario (refund after 40 days)
- [ ] Hallucinated citation tested with a trace that references a non-existent source
- [ ] Full layer runs in under 100ms on a typical trace
- [ ] Unit tests: one passing and one failing case per check (14 tests minimum)
"""
    },
    {
        "title": "[EVAL-03] Implement Layer 2 — behavioral signals",
        "labels": ["demo-visible", "module-3-eval", "week-5"],
        "body": """## Context
Layer 2 reads the behavioral signals already captured in the trace by the SDK. These are the most honest quality signals — a user who immediately rephrases their question is telling you directly the answer was bad. No API calls, no ML — just reading what the user did.

## Task
Implement `run_layer2(trace)` that reads signals from the trace and returns a Layer2Result.

## Signal logic

| Signal | Source field | Interpretation | Severity boost |
|---|---|---|---|
| `thumbs_down` | `SignalPayload` linked to trace | Confirmed failure | +2 severity levels |
| `escalate` | `SignalPayload` linked to trace | AI could not resolve | high |
| `rephrased` | `SignalPayload` linked to trace | Probable failure | medium |
| `abandoned` | `SignalPayload` linked to trace | Probable failure/frustration | medium |
| `acknowledged` | `SignalPayload` linked to trace | Probable success | reduces severity |

## Implementation details
- Query PostgreSQL for any `SignalPayload` records linked to this `trace_id`
- Signals may arrive after the trace (async) — if none found, return neutral result
- `thumbs_down` alone is enough to mark a trace as confirmed failure
- Multiple signals compound: `rephrased` + `abandoned` = high severity

## Output format
```python
@dataclass
class Layer2Result:
    has_signals: bool
    signals_found: list[str]
    severity_modifier: int  # -1 (reduce) to +2 (boost)
    confirmed_failure: bool
    evidence: str
```

## File location
`backend/eval_engine/layer2.py`

## Acceptance criteria
- [ ] All 5 signal types handled correctly
- [ ] `thumbs_down` alone marks trace as confirmed failure
- [ ] Multiple signals compound correctly
- [ ] No signals found returns neutral result (does not fail the trace)
- [ ] PostgreSQL query confirmed working against staging DB
- [ ] Unit tests: each signal type tested individually + combination test
"""
    },
    {
        "title": "[EVAL-04] Implement Layer 3 — LLM-as-judge with support AI rubric",
        "labels": ["demo-visible", "module-3-eval", "week-5"],
        "body": """## Context
Layer 3 sends the trace to Claude Haiku with a domain-specific rubric and asks it to score the response. This is the most expensive layer (500ms–2s, ~$0.001 per trace) but produces the richest failure attribution. For MVP we ship with the Support AI rubric pack only.

## Task
Implement `run_layer3(trace)` that sends the trace to Claude Haiku with the Support AI rubric and returns a structured score.

## Support AI rubric pack
Evaluate the AI response on these 4 dimensions, each scored 1–5:

1. **Issue resolution** — Did the AI actually solve the user's problem?
2. **Answer accuracy** — Is the answer factually correct given the retrieved context?
3. **Tone and empathy** — Was the response appropriately professional and empathetic?
4. **Correct escalation** — Did the AI escalate to a human when it should have?

## Judge prompt structure
```python
SUPPORT_RUBRIC_PROMPT = \"\"\"
You are an expert evaluator for customer support AI systems.

Evaluate the following AI interaction and score it on each dimension from 1-5.
Return ONLY valid JSON, no explanation outside the JSON.

Trace:
User prompt: {user_prompt}
Retrieved context: {retrieved_context}
AI response: {llm_response}

Score each dimension 1-5 (1=very poor, 5=excellent):
{{
  "issue_resolution": <1-5>,
  "answer_accuracy": <1-5>,
  "tone_and_empathy": <1-5>,
  "correct_escalation": <1-5>,
  "overall_severity": "low|medium|high|critical",
  "primary_failure_type": "<one sentence>",
  "evidence": "<specific quote from response that supports your score>"
}}
\"\"\"
```

## Implementation details
- Use `claude-haiku-4-5-20251001` model
- `max_tokens: 300` — response is always short JSON
- Parse response strictly — if JSON invalid, return a neutral result and log
- Run asynchronously — do not block Layer 1 or Layer 2
- Overall score below 3 on any dimension = failure flag

## Output format
```python
@dataclass
class Layer3Result:
    issue_resolution: int
    answer_accuracy: int
    tone_and_empathy: int
    correct_escalation: int
    overall_severity: str
    primary_failure_type: str
    evidence: str
    raw_response: str
```

## File location
`backend/eval_engine/layer3.py`

## Acceptance criteria
- [ ] Claude Haiku called correctly with support rubric prompt
- [ ] JSON response parsed into `Layer3Result`
- [ ] Invalid JSON handled gracefully — neutral result returned, error logged
- [ ] Tested against demo scenario (refund contradiction) — should score answer_accuracy: 1
- [ ] Async — does not block Layer 1 or Layer 2 results
- [ ] Cost logged per trace (token counts from API response)
- [ ] Unit tests with mocked Anthropic API response
"""
    },
    {
        "title": "[EVAL-05] Build score aggregator",
        "labels": ["core-path", "module-3-eval", "week-5"],
        "body": """## Context
The aggregator takes the outputs of all three layers and merges them into a single structured failure report written to PostgreSQL. This is the final output of the eval engine — what the dashboard reads to show failures.

## Task
Implement `aggregate(l1, l2, l3)` that merges the three layer outputs into one `FailureReport` and writes it to PostgreSQL.

## Aggregation rules

### Severity resolution
- Layer 1 heuristic trigger on a clear-cut check (contradiction, hallucinated citation, tool schema) → **overrides** Layer 3 score
- Layer 2 `thumbs_down` → bumps severity by 2 levels regardless of other scores
- Layer 2 `acknowledged` with no Layer 1 triggers → reduce severity by 1 level
- Layer 3 score below 3 on any dimension → minimum severity: medium

### Severity scale
`low → medium → high → critical`

### Failure type attribution
- If Layer 1 triggered: use Layer 1 check name as failure type
- If only Layer 3: use Layer 3 `primary_failure_type`
- If both: combine — `f"{l1_type} (confirmed by judge)"`

## Output format
```python
@dataclass
class FailureReport:
    trace_id: str
    customer_id: str
    failure_type: str | None   # None if no failure detected
    severity: str              # "low" | "medium" | "high" | "critical"
    confidence: float          # 0.0 - 1.0
    evidence: str
    layer1_triggered: bool
    layer2_signals: list[str]
    layer3_score: dict
    is_failure: bool           # True if severity >= medium
    created_at: datetime
```

## PostgreSQL write
Insert into `scores` table (already created in INGEST-04).

## File location
`backend/eval_engine/aggregator.py`

## Acceptance criteria
- [ ] All aggregation rules implemented and tested
- [ ] Layer 1 override of Layer 3 confirmed working
- [ ] Layer 2 thumbs_down severity boost confirmed working
- [ ] FailureReport written to PostgreSQL scores table
- [ ] Demo scenario produces: `failure_type="context_contradiction"`, `severity="high"`, `is_failure=True`
- [ ] Unit tests covering: no failure, layer1 only, layer2 only, layer3 only, all three combined
"""
    },
    {
        "title": "[EVAL-06] Create golden dataset schema (nice to have)",
        "labels": ["nice-to-have", "module-3-eval", "week-5"],
        "body": """## Context
The golden dataset is the long-term moat of Sentinel. Engineers reviewing traces can mark them as correctly or incorrectly scored — these accumulate in PostgreSQL and eventually feed back to recalibrate Layer 3 judge prompts per customer. For MVP we only need the schema and a basic write function — the recalibration loop itself is Phase 2.

## Task
Implement the golden dataset write path so the dashboard can mark traces as reviewed in a later ticket.

## Implementation details
- `golden_dataset` table already created in INGEST-04
- Add a `save_golden_example(trace_id, customer_id, correct_label, reviewer_note)` function
- Correct label: `True` = eval engine was right, `False` = eval engine was wrong
- This function will be called by the dashboard API in Module 4

## File location
`backend/eval_engine/golden_dataset.py`

## Acceptance criteria
- [ ] `save_golden_example()` function implemented and tested
- [ ] Writes correctly to PostgreSQL `golden_dataset` table
- [ ] Only implement if EVAL-01 through EVAL-05 are complete

## Notes
Do not build the recalibration loop — that is Phase 2. Schema and write path only.
"""
    },
]

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def graphql(query: str, variables: dict = {}) -> dict:
    r = requests.post(
        GRAPHQL_URL,
        headers=HEADERS,
        json={"query": query, "variables": variables},
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
        "core-path":        "0075ca",
        "module-2-ingest":  "1D76DB",
        "week-3":           "BFD4F2",
        "p0":               "B60205",
        "infrastructure":   "E4E669",
        "demo-visible":     "0E8A16",
        "nice-to-have":     "FEF2C0",
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
    print(f"  Sentinel — Week 3 Ticket Creator")
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