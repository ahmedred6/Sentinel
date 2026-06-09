# Sentinel
### Your LLM is failing in production right now. You have no idea. Sentry never will.

Sentinel is a runtime intelligence platform for AI-powered applications. Existing monitoring tools catch infrastructure failures — crashed servers, slow APIs, thrown exceptions. Sentinel catches a fundamentally different class of failure: **the AI gave a wrong answer.**

When an LLM hallucinates, contradicts a company policy, fails to escalate an urgent request, or degrades silently after a model update — nothing in your current stack catches it. No stack traces for hallucinations. No error logs for semantic failures. Companies discover these problems the same way they discovered software bugs in 1995: through user complaints.

**Sentinel fixes that.**

---

## What it does

Sentinel watches every LLM inference call at runtime, evaluates it immediately using domain-aware rubrics, attributes failures to their root cause in the codebase, generates a fix autonomously, validates it in a sandbox, and opens a pull request for human review.

This is not an observability dashboard with an AI button bolted on. It is a **closed-loop autonomous reliability system** for AI applications.

---

## How it works

### Detection
- **SDK** — A lightweight Python and TypeScript package that wraps your existing LLM API calls. Zero added latency (async background capture).
- **Eval engine** — Three-layer scoring pipeline:
  1. Heuristic checks (context contradiction, hallucinated citations, refusal-when-answer-exists, etc.)
  2. Behavioral signals (user rephrasing, thumbs-down, escalation, session abandonment)
  3. LLM-as-judge with domain-specific vertical rubrics
- **Trace store** — ClickHouse for high-volume append-only event storage.

### Autonomous Engineering
- **Root cause analyzer** — Maps runtime failures to specific files, prompts, tool schemas, or retrieval configs using git history and deployment timelines.
- **Fix generator** — Produces file-level diffs: prompt edits, retrieval config changes, routing rule updates.
- **Sandbox validator** — Replays failing traces against the proposed fix before any PR is opened.
- **PR generator** — Opens a GitHub PR with evidence, diffs, validation results, and reviewer assignment. Human approval always required.

---

## Quick start

```bash
pip install sentinel-ai
```

```python
import sentinel

with sentinel.trace(flow="support", user_id=user_id):
    response = openai.chat.completions.create(
        model="gpt-4.1",
        messages=messages
    )
```

That's it. Connect your repo in the dashboard and failures start surfacing within minutes.

---

## Tech stack

| Layer | Technology |
|---|---|
| Python SDK | Python 3.11+ |
| TypeScript SDK | Node 20+ |
| Ingest API | FastAPI on Railway |
| Queue | Redis Streams |
| Trace store | ClickHouse |
| Structured data | PostgreSQL |
| Dashboard | React + Cloudflare Workers |
| Auth | Clerk |

---

## Project structure

```
sentinel/
├── sdk/
│   ├── python/          # Python SDK (pip install sentinel-ai)
│   └── typescript/      # TypeScript SDK (npm install @sentinel-ai/sdk)
├── backend/
│   ├── ingest/          # FastAPI ingest API
│   ├── eval/            # Eval engine (heuristics, behavioral, LLM judge)
│   ├── root-cause/      # Root cause analyzer
│   ├── fix-generator/   # Fix generator + sandbox validator
│   └── pr-generator/    # GitHub PR automation
├── dashboard/           # React ops view
└── docs/                # Architecture docs
```

---

## Roadmap

- [x] Project documentation & architecture
- [ ] Python SDK (v0.1)
- [ ] TypeScript SDK (v0.1)
- [ ] Ingest API + Redis Streams pipeline
- [ ] Eval engine — Layer 1 heuristics
- [ ] Eval engine — Layer 2 behavioral signals
- [ ] Eval engine — Layer 3 LLM-as-judge
- [ ] ClickHouse trace store
- [ ] Dashboard (React)
- [ ] Root cause analyzer
- [ ] Fix generator + sandbox validator
- [ ] PR generator
- [ ] VS Code plugin

---

## Contributing

This project is in early development. If you're interested in contributing or becoming a design partner, reach out.

---

## License

MIT

---

*Sentinel — Confidential — May 2026*
