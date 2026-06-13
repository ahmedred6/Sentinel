#!/usr/bin/env python3
"""
demo/support_chatbot_demo.py

Sentinel SDK Demo — Support Chatbot Instrumentation Script

Simulates a customer support AI and triggers three distinct failure types
on demand. Uses pre-set responses to guarantee the failures fire reliably
every run — a live LLM might answer correctly, which kills the demo.

Usage:
    python demo/support_chatbot_demo.py

Environment variables:
    SENTINEL_API_KEY       Sentinel API key       (default: sk_demo)
    SENTINEL_CUSTOMER_ID   Sentinel customer ID   (default: cust_demo)
    SENTINEL_BASE_URL      Ingest API URL         (default: http://localhost:8000)
    OPENAI_API_KEY         Optional — set to use real OpenAI in Scenario 4
"""

import os
import sys
import time
import uuid

# Locate the SDK relative to this script
_SDK_PATH = os.path.join(os.path.dirname(__file__), "..", "Backend", "sentinel-sdk")
sys.path.insert(0, os.path.abspath(_SDK_PATH))

from client import SentinelClient
from schema import ContextDocument, Environment, TokenCounts

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_KEY = os.getenv("SENTINEL_API_KEY", "sk_demo")
CUSTOMER_ID = os.getenv("SENTINEL_CUSTOMER_ID", "cust_demo")
BASE_URL = os.getenv("SENTINEL_BASE_URL", "http://localhost:8000")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# ---------------------------------------------------------------------------
# Retrieved context documents (simulates a RAG retrieval step)
# ---------------------------------------------------------------------------

REFUND_POLICY = ContextDocument(
    content=(
        "Refunds are only allowed within 30 days of purchase. "
        "After 30 days, no refunds will be processed under any circumstances. "
        "Customers outside the 30-day window may be offered store credit at the "
        "discretion of the support team."
    ),
    source="refund_policy_v2.pdf",
    score=0.94,
)

ESCALATION_POLICY = ContextDocument(
    content=(
        "If a customer is dissatisfied with a resolution, escalate to a human agent "
        "by saying: 'I will transfer you to a specialist now.' Do not attempt to "
        "resolve complaints that require managerial approval."
    ),
    source="escalation_policy_v1.pdf",
    score=0.88,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def hr(title: str = "") -> None:
    width = 62
    if title:
        pad = (width - len(title) - 2) // 2
        print(f"\n{'-' * pad} {title} {'-' * pad}")
    else:
        print("-" * width)


def step(label: str, text: str) -> None:
    print(f"  {label:<18} {text}")


# ---------------------------------------------------------------------------
# Main demo
# ---------------------------------------------------------------------------

def main() -> None:
    hr("Sentinel SDK Demo - Support Chatbot")
    print(f"  Ingest API : {BASE_URL}")
    print(f"  Customer   : {CUSTOMER_ID}")

    sentinel = SentinelClient.init(
        api_key=API_KEY,
        customer_id=CUSTOMER_ID,
        base_url=BASE_URL,
    )
    print("  SDK ready  : background shipper started, interceptors installed")

    # -----------------------------------------------------------------------
    # Scenario 1 — Context Contradiction
    # -----------------------------------------------------------------------

    hr("Scenario 1 - Context Contradiction")
    print("""
  The LLM contradicts a fact that is explicitly stated in the
  retrieved context. This is a Layer 1 heuristic catch.

  Retrieved context:  "Refunds only within 30 days."
  User asks:          "Can I get a refund after 40 days?"
  LLM responds:       "Yes, you can request a refund."  ← WRONG
    """)

    session_1 = str(uuid.uuid4())

    with sentinel.trace(
        flow="support",
        session_id=session_1,
        user_id="demo_user_001",
        environment=Environment.STAGING,
        retrieved_context=[REFUND_POLICY, ESCALATION_POLICY],
    ) as trace:
        trace.manual(
            user_prompt="Can I get a refund after 40 days?",
            llm_response="Yes, you can request a refund.",  # contradicts 30-day cap
            model_name="gpt-4.1",
            token_counts=TokenCounts(prompt=312, completion=18, total=330),
            latency_ms=843,
        )
        trace_id_1 = trace.trace_id

    step("trace_id:", trace_id_1)
    step("failure type:", "context_contradiction")
    step("eval layer:", "Layer 1 — heuristic")
    step("severity:", "HIGH")
    step("evidence:", '"Yes, you can" contradicts "only within 30 days"')
    print()

    # -----------------------------------------------------------------------
    # Scenario 2 — Behavioral Signal confirming Scenario 1
    # -----------------------------------------------------------------------

    hr("Scenario 2 - Behavioral Signal (thumbs_down)")
    print("""
  The user sees the wrong answer and clicks thumbs down.
  This is a Layer 2 behavioral signal that confirms and
  amplifies the context contradiction found in Scenario 1.
    """)

    time.sleep(0.3)
    sentinel.thumbs_down(trace_id_1)

    step("signal_type:", "thumbs_down")
    step("linked to:", trace_id_1[:8] + "…  (Scenario 1 trace)")
    step("effect:", "severity escalated  HIGH → CRITICAL")
    print()

    # -----------------------------------------------------------------------
    # Scenario 3 — Refusal When Answer Exists
    # -----------------------------------------------------------------------

    hr("Scenario 3 - Refusal When Answer Exists")
    print("""
  The LLM refuses to answer a question even though the answer
  is clearly present in the retrieved context. A different
  failure mode from contradiction — here the model fails to
  use the context at all.

  Retrieved context:  "Refunds only within 30 days."
  User asks:          "What is your refund policy?"
  LLM responds:       "I'm not sure. Please contact support."  ← WRONG
    """)

    session_3 = str(uuid.uuid4())

    with sentinel.trace(
        flow="support",
        session_id=session_3,
        user_id="demo_user_002",
        environment=Environment.STAGING,
        retrieved_context=[REFUND_POLICY],
    ) as trace:
        trace.manual(
            user_prompt="What is your refund policy?",
            llm_response=(
                "I'm not sure about our refund policy. "
                "Please contact our support team for more information."
            ),
            model_name="gpt-4.1",
            token_counts=TokenCounts(prompt=285, completion=22, total=307),
            latency_ms=612,
        )
        trace_id_3 = trace.trace_id

    step("trace_id:", trace_id_3)
    step("failure type:", "refusal_when_answer_exists")
    step("eval layer:", "Layer 1 — heuristic")
    step("severity:", "MEDIUM")
    step("evidence:", "Context contains answer; response defers to support team")
    print()

    # -----------------------------------------------------------------------
    # Scenario 4 — Rephrased signal following the refusal
    # -----------------------------------------------------------------------

    hr("Scenario 4 - Behavioral Signal (rephrased)")
    print("""
  The user immediately rephrases their question — the strongest
  behavioral signal that the previous response was unhelpful.
  Layer 2 confirms the Layer 1 finding.
    """)

    time.sleep(0.3)
    sentinel.rephrased(trace_id_3)

    step("signal_type:", "rephrased")
    step("linked to:", trace_id_3[:8] + "…  (Scenario 3 trace)")
    step("effect:", "Layer 2 confirms Layer 1 finding — severity stays MEDIUM")
    print()

    # -----------------------------------------------------------------------
    # Optional Scenario 5 — Real OpenAI call (if OPENAI_API_KEY is set)
    # -----------------------------------------------------------------------

    if OPENAI_API_KEY:
        hr("Scenario 5 - Live OpenAI Call (SDK intercepts automatically)")
        print("""
  OPENAI_API_KEY detected. Making a real OpenAI call inside a
  sentinel.trace() context. The interceptor fires automatically —
  no manual() needed. This is the zero-code-change integration.
        """)
        try:
            import openai

            client_oai = openai.OpenAI(api_key=OPENAI_API_KEY)
            session_5 = str(uuid.uuid4())

            with sentinel.trace(
                flow="support",
                session_id=session_5,
                user_id="demo_user_003",
                environment=Environment.STAGING,
                retrieved_context=[REFUND_POLICY],
            ) as trace:
                response = client_oai.chat.completions.create(
                    model="gpt-4.1-mini",
                    messages=[
                        {
                            "role": "system",
                            "content": (
                                "You are a customer support agent. "
                                "Answer based only on the provided policy. "
                                "Policy: " + REFUND_POLICY.content
                            ),
                        },
                        {
                            "role": "user",
                            "content": "Can I get a refund after 40 days?",
                        },
                    ],
                    max_tokens=80,
                )
                live_trace_id = trace.trace_id

            step("trace_id:", live_trace_id)
            step("model:", response.model)
            step("tokens:", str(response.usage.total_tokens))
            step("response:", response.choices[0].message.content[:70] + "…")
            step("status:", "captured automatically via OpenAI interceptor")

        except Exception as exc:
            print(f"  OpenAI call failed: {exc}")
        print()

    # -----------------------------------------------------------------------
    # Flush and summary
    # -----------------------------------------------------------------------

    sentinel._shipper.flush()

    hr("Demo Complete")
    print(f"""
  Failure types triggered:

    1. context_contradiction       trace {trace_id_1[:8]}…  (severity: HIGH)
    2. thumbs_down signal          confirms trace {trace_id_1[:8]}…  → CRITICAL
    3. refusal_when_answer_exists  trace {trace_id_3[:8]}…  (severity: MEDIUM)
    4. rephrased signal            confirms trace {trace_id_3[:8]}…

  {4 + (1 if OPENAI_API_KEY else 0)} trace(s) + 2 signal(s) shipped to {BASE_URL}

  Next step: open the Sentinel dashboard and check the Failure Feed.
  Each trace above should appear scored within a few seconds of
  the ingest API processing it.
    """)
    hr()


if __name__ == "__main__":
    main()
