#!/usr/bin/env python3
"""
simulation/run_simulation.py

Interactive Sentinel SDK simulation — a support chatbot you can chat with.

Type your message, see the bot reply, then react with a signal.
Everything is traced live through the real SDK and shown by the
mock ingest server running in the background.

Usage:
    python simulation/run_simulation.py

Requires OPENAI_API_KEY in .env (or set in environment).
Falls back to scripted mock responses if no key is found.
"""

import json
import os
import queue as _queue
import socket
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer

# --------------------------------------------------------------------------
# Locate SDK and load .env
# --------------------------------------------------------------------------

_SIM_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT    = os.path.dirname(_SIM_DIR)
_SDK     = os.path.join(_ROOT, "Backend", "sentinel-sdk")
sys.path.insert(0, _SDK)

def _load_dotenv() -> None:
    env_file = os.path.join(_ROOT, ".env")
    if not os.path.exists(env_file):
        return
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip())

_load_dotenv()

from client import SentinelClient
from schema import ContextDocument, Environment, TokenCounts

# --------------------------------------------------------------------------
# Colours
# --------------------------------------------------------------------------

try:
    import colorama; colorama.init()
    GRN = "\033[92m"; YLW = "\033[93m"; RED = "\033[91m"
    CYN = "\033[96m"; BLD = "\033[1m";  DIM = "\033[2m"; RST = "\033[0m"
except ImportError:
    GRN = YLW = RED = CYN = BLD = DIM = RST = ""

def g(s): return f"{GRN}{s}{RST}"
def y(s): return f"{YLW}{s}{RST}"
def r(s): return f"{RED}{s}{RST}"
def c(s): return f"{CYN}{s}{RST}"
def b(s): return f"{BLD}{s}{RST}"
def d(s): return f"{DIM}{s}{RST}"

# --------------------------------------------------------------------------
# Policy context (injected as retrieved_context into every trace)
# --------------------------------------------------------------------------

POLICY_DOCS = [
    ContextDocument(
        content=(
            "Refunds are only allowed within 30 days of purchase. "
            "After 30 days, no refunds will be processed under any circumstances. "
            "Customers outside the 30-day window may be offered store credit at "
            "the discretion of the support team."
        ),
        source="refund_policy_v2.pdf",
        score=0.94,
    ),
    ContextDocument(
        content=(
            "For damaged or defective items, an exchange is available regardless "
            "of purchase date. The customer must provide photo evidence. "
            "Exchanges must be processed by a human agent."
        ),
        source="damage_exchange_policy.pdf",
        score=0.81,
    ),
    ContextDocument(
        content=(
            "If a customer requests to speak with a human agent, respond with: "
            "'I will transfer you to a specialist now.' Do not attempt to resolve "
            "complaints that require managerial approval."
        ),
        source="escalation_policy_v1.pdf",
        score=0.77,
    ),
]

SYSTEM_PROMPT = (
    "You are a customer support agent for ShopRight, an online retailer. "
    "Answer questions based on company policy. Be helpful, concise, and professional.\n\n"
    "Policy summary:\n"
    "- Refunds: within 30 days of purchase only.\n"
    "- Damaged items: exchange available anytime (photo required).\n"
    "- Escalation: transfer to human agent when customer requests it."
)

# --------------------------------------------------------------------------
# Mock ingest server (buffers events — main thread prints them)
# --------------------------------------------------------------------------

_event_queue: _queue.Queue = _queue.Queue()


class _Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        items: list[dict] = json.loads(self.rfile.read(length))
        _event_queue.put((self.path, items))
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

    def log_message(self, format: str, *args: object) -> None:
        _ = format, args


def _drain_events(wait: float = 0.35) -> None:
    """Wait briefly then print everything the server received."""
    time.sleep(wait)
    printed = False
    while not _event_queue.empty():
        endpoint, items = _event_queue.get_nowait()
        if not printed:
            print()
            printed = True
        _print_event(endpoint, items)


def _print_event(endpoint: str, items: list[dict]) -> None:
    is_trace = "trace" in endpoint
    tag = g("[TRACE] ") if is_trace else y("[SIGNAL]")
    print(f"  {tag} {d('POST ' + endpoint)}  {d(str(len(items)) + ' item(s)')}")
    print(f"  {'-'*56}")

    for item in items:
        if is_trace:
            _fmt_trace(item)
        else:
            _fmt_signal(item)


def _fmt_trace(t: dict) -> None:
    tid = t.get("trace_id", "")
    print(f"  {b('trace_id')}     {d(tid)}")
    print(f"  {b('user_prompt')}  {t.get('user_prompt', '')}")
    print(f"  {b('llm_response')} {t.get('llm_response', '')}")

    docs = t.get("retrieved_context", [])
    if docs:
        print(f"  {b('context')}      {len(docs)} doc(s): "
              + ", ".join(d(doc.get("source", "?")) for doc in docs))

    tc = t.get("token_counts", {})
    print(f"  {b('tokens')}       {tc.get('total', 0)} "
          f"{d('(' + str(tc.get('prompt', 0)) + ' + ' + str(tc.get('completion', 0)) + ')')}"
          f"   {b('latency')} {t.get('latency_ms', 0)} ms")

    _layer1_flags(t)
    print(f"  {'-'*56}")


def _fmt_signal(s: dict) -> None:
    stype = s.get("signal_type", "")
    col = r(stype) if stype in ("thumbs_down", "abandoned") else y(stype)
    print(f"  {b('signal_type')}  {col}")
    print(f"  {b('trace_id')}     {d(s.get('trace_id', ''))}")
    print(f"  {'-'*56}")


def _layer1_flags(t: dict) -> None:
    resp = (t.get("llm_response") or "").lower()
    docs = t.get("retrieved_context", [])
    doc_text = " ".join(d_.get("content", "").lower() for d_ in docs)
    flags = []

    if "30 days" in doc_text and any(
        p in resp for p in ["yes, you can", "you can request", "you are eligible",
                            "refund is possible", "can get a refund"]
    ):
        flags.append(r("  EVAL >> context_contradiction  (HIGH)"))

    if doc_text and any(
        p in resp for p in ["i'm not sure", "i don't know", "contact support",
                            "please reach out", "i cannot", "i am unable"]
    ):
        flags.append(y("  EVAL >> refusal_when_answer_exists  (MEDIUM)"))

    if len(resp.split()) < 8 and len((t.get("user_prompt") or "").split()) > 5:
        flags.append(y("  EVAL >> response_length_anomaly  (LOW)"))

    for f in flags:
        print(f)


# --------------------------------------------------------------------------
# Signal menu
# --------------------------------------------------------------------------

_SIGNALS = {
    "1": ("thumbs_down",  r("1") + " thumbs_down"),
    "2": ("rephrased",    y("2") + " rephrased"),
    "3": ("escalate",     y("3") + " escalate"),
    "4": ("abandoned",    r("4") + " abandoned"),
    "5": ("acknowledged", g("5") + " acknowledged"),
}


def _signal_menu(sentinel: SentinelClient, trace_id: str) -> None:
    menu = "  ".join(v for _, v in _SIGNALS.values())
    print(f"\n  React?  {menu}  {d('[Enter] skip')}")
    try:
        choice = input("  > ").strip()
    except (EOFError, KeyboardInterrupt):
        return
    if choice in _SIGNALS:
        method_name, label = _SIGNALS[choice]
        getattr(sentinel, method_name)(trace_id)
        print(f"  Signal sent: {label}")
        _drain_events(wait=0.2)


# --------------------------------------------------------------------------
# LLM call (real OpenAI or mock fallback)
# --------------------------------------------------------------------------

def _call_llm(messages: list) -> tuple[str, str, TokenCounts, int]:
    """Returns (reply_text, model_name, token_counts, latency_ms)."""
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if api_key:
        try:
            import openai, time as _t
            client_oai = openai.OpenAI(api_key=api_key)
            t0 = _t.monotonic()
            resp = client_oai.chat.completions.create(
                model="gpt-4.1-mini",
                messages=messages,  # type: ignore[arg-type]
                max_tokens=120,
            )
            latency = int((_t.monotonic() - t0) * 1000)
            text = resp.choices[0].message.content or ""
            u = resp.usage
            if u is None:
                tc = TokenCounts(prompt=0, completion=0, total=0)
            else:
                tc = TokenCounts(
                    prompt=u.prompt_tokens,
                    completion=u.completion_tokens,
                    total=u.total_tokens,
                )
            return text, resp.model, tc, latency
        except Exception as e:
            print(f"  {y('OpenAI error:')} {e} — using mock response")

    # Mock fallback
    last_user = next(
        (m["content"] for m in reversed(messages) if m["role"] == "user"), ""
    ).lower()
    if any(w in last_user for w in ["40 days", "45 days", "60 days", "after 30"]):
        reply = "Yes, you can request a refund."          # intentional contradiction
    elif any(w in last_user for w in ["policy", "what is", "tell me about"]):
        reply = "I'm not sure about that. Please contact our support team."  # refusal
    elif any(w in last_user for w in ["30 days", "how long", "return", "exchange", "damaged"]):
        reply = "You have 30 days from the date of purchase to request a refund."
    elif any(w in last_user for w in ["human", "agent", "person", "speak", "talk"]):
        reply = "I will transfer you to a specialist now."
    else:
        reply = "I'm here to help! Could you tell me more about your order?"

    tc = TokenCounts(prompt=80, completion=len(reply.split()), total=80 + len(reply.split()))
    return reply, "mock-gpt", tc, 120


# --------------------------------------------------------------------------
# Main chat loop
# --------------------------------------------------------------------------

def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def main() -> None:
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"

    server = HTTPServer(("127.0.0.1", port), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    has_openai = bool(os.environ.get("OPENAI_API_KEY"))
    llm_label = g("gpt-4.1-mini  (live)") if has_openai else y("mock LLM  (no OPENAI_API_KEY found)")

    sep = "=" * 60
    print(f"\n{sep}")
    print(f"  {b('Sentinel  --  Interactive Support Chatbot')}")
    print(f"  Mock ingest : {c(base_url)}")
    print(f"  LLM         : {llm_label}")
    print(sep)
    print(f"  Policy context loaded  ({len(POLICY_DOCS)} documents):")
    for doc in POLICY_DOCS:
        print(f"    {d('- ' + doc.source)}")
    print()
    print(f"  Type your message and press {b('Enter')}.")
    print(f"  After each reply, pick a reaction signal or skip.")
    print(f"  Type {b('quit')} to exit.")
    print(sep + "\n")

    sentinel = SentinelClient.init(
        api_key=os.environ.get("SENTINEL_API_KEY", "sk_sim"),
        customer_id=os.environ.get("SENTINEL_CUSTOMER_ID", "cust_sim"),
        base_url=base_url,
    )

    session_id = str(uuid.uuid4())
    messages: list[dict] = [{"role": "system", "content": SYSTEM_PROMPT}]
    turn = 0

    while True:
        # Prompt
        try:
            print(b("You: "), end="", flush=True)
            user_input = input().strip()
        except (EOFError, KeyboardInterrupt):
            break

        if not user_input:
            continue
        if user_input.lower() in ("quit", "exit", "q"):
            break

        messages.append({"role": "user", "content": user_input})
        turn += 1

        # Call LLM inside a sentinel trace
        print(f"  {d('...')}", flush=True)
        reply = ""
        token_counts = TokenCounts(prompt=0, completion=0, total=0)
        latency_ms = 0
        model_name = "unknown"

        with sentinel.trace(
            flow="support",
            session_id=session_id,
            environment=Environment.STAGING,
            retrieved_context=POLICY_DOCS,
        ) as trace:
            reply, model_name, token_counts, latency_ms = _call_llm(messages)

            # If no OpenAI interceptor fired (mock path), capture manually
            if not has_openai:
                trace.manual(
                    user_prompt=user_input,
                    llm_response=reply,
                    model_name=model_name,
                    token_counts=token_counts,
                    latency_ms=latency_ms,
                    message_history=[],
                )

        messages.append({"role": "assistant", "content": reply})

        print(f"\r{b('Bot:')} {reply}\n")

        # Show what the ingest server received
        _drain_events()

        # Signal menu
        _signal_menu(sentinel, trace.trace_id)
        print()

    # Clean up
    sentinel._shipper.flush()
    server.shutdown()

    total = 0
    traces = signals = 0
    while not _event_queue.empty():
        ep, items = _event_queue.get_nowait()
        total += len(items)
        if "trace" in ep:
            traces += len(items)
        else:
            signals += len(items)

    print(f"\n{sep}")
    print(f"  Session ended.  {turn} turn(s).")
    print(f"  Ingest received: {traces} trace(s), {signals} signal(s).")
    print(sep + "\n")


if __name__ == "__main__":
    main()
