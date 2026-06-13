# Sentinel Python SDK

Instrument your LLM application in minutes. Sentinel captures every inference call, ships it asynchronously to the eval pipeline, and lets you report user behavior signals — all without adding latency to your application.

---

## Installation

```bash
pip install sentinel-ai       # not yet on PyPI — install from source for now
```

**From source (current):**

```bash
git clone https://github.com/ahmedred6/Sentinel.git
cd Sentinel/Backend/sentinel-sdk
pip install pydantic
```

---

## Setup

Call `SentinelClient.init()` once at application startup. This starts the background shipper thread and installs the OpenAI/Anthropic interceptors.

```python
from client import SentinelClient

sentinel = SentinelClient.init(
    api_key="sk_...",           # your Sentinel API key
    customer_id="cust_...",     # your Sentinel customer ID
    base_url="https://ingest.sentinel-ai.io",  # default
)
```

---

## Wrapping LLM calls

### Context manager

The `with sentinel.trace(...)` block captures everything that happens inside it. The interceptor fires automatically on the OpenAI or Anthropic call — no extra code required.

```python
with sentinel.trace(flow="support", session_id=session_id, user_id=user_id):
    response = openai.chat.completions.create(
        model="gpt-4.1",
        messages=messages,
    )
```

Use `as trace` to get the trace object back (e.g. to call signal methods later):

```python
with sentinel.trace(flow="support", session_id=session_id) as trace:
    response = openai.chat.completions.create(model="gpt-4.1", messages=messages)

# trace.trace_id is available here, after the with block
sentinel.thumbs_down(trace.trace_id)
```

### Decorator

```python
@sentinel.trace(flow="support")
def handle_message(messages):
    return openai.chat.completions.create(model="gpt-4.1", messages=messages)
```

### Manual capture (any provider)

For providers other than OpenAI or Anthropic, use `.manual()` inside the context:

```python
from schema import TokenCounts

with sentinel.trace(flow="support", session_id=session_id) as trace:
    response = my_llm.complete(prompt)
    trace.manual(
        user_prompt=prompt,
        llm_response=response.text,
        model_name="my-model-v1",
        token_counts=TokenCounts(prompt=200, completion=50, total=250),
        latency_ms=450,
    )
```

---

## Passing retrieved context (RAG)

If your application retrieves documents before the LLM call, pass them so Sentinel can run context-contradiction checks:

```python
from schema import ContextDocument

with sentinel.trace(
    flow="support",
    session_id=session_id,
    retrieved_context=[
        ContextDocument(
            content="Refunds are only allowed within 30 days of purchase.",
            source="refund_policy_v2.pdf",
            score=0.94,
        )
    ],
) as trace:
    response = openai.chat.completions.create(...)
```

---

## Behavioral signal methods

Call these after showing the LLM response to the user. They ship a lightweight signal event to the eval engine (Layer 2).

```python
# User clicked thumbs down
sentinel.thumbs_down(trace.trace_id)

# User requested a human agent
sentinel.escalate(trace.trace_id)

# User immediately rephrased the question (within 60s)
sentinel.rephrased(trace.trace_id)

# User quit within 30 seconds
sentinel.abandoned(trace.trace_id)

# User sent a short acknowledgment, session ended naturally
sentinel.acknowledged(trace.trace_id)
```

---

## Environments

```python
from schema import Environment

with sentinel.trace(flow="support", environment=Environment.STAGING) as trace:
    ...
```

Available: `Environment.PROD` (default), `Environment.STAGING`, `Environment.DEV`

---

## How it works

- **Zero latency impact.** The context manager adds nanoseconds of overhead. All network I/O happens on a background daemon thread.
- **Batching.** The shipper groups up to 20 payloads per HTTP POST.
- **Retry.** Three attempts with exponential backoff (0.5s → 1.0s → 2.0s). Failures are silently dropped — they never raise into your application.
- **Thread-safe.** Multiple threads can be inside different `sentinel.trace()` contexts simultaneously. Interceptors use a thread-local slot.

---

## Running the tests

```bash
cd Backend/sentinel-sdk
pytest tests/ -v
```

---

## Running the demo

```bash
# Set up environment
cp .env.example .env        # add your keys
export SENTINEL_BASE_URL=http://localhost:8000   # local ingest API

python demo/support_chatbot_demo.py
```

See [demo/support_chatbot_demo.py](../../demo/support_chatbot_demo.py) for the full instrumented demo scenario.
