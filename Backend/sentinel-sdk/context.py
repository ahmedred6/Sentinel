"""
sentinel/context.py

SentinelTraceContext — the object returned by sentinel.trace().

Supports two usage patterns:

  # 1. Context manager
  with sentinel.trace(flow="support") as trace:
      response = openai.chat.completions.create(...)
      # OpenAI interceptor fires automatically; trace ships on __exit__

  # 2. Decorator
  @sentinel.trace(flow="support")
  def handle_message(messages):
      return openai.chat.completions.create(...)

  # 3. Manual capture (any other provider)
  with sentinel.trace(flow="support") as trace:
      response = my_llm.complete(prompt)
      trace.manual(
          user_prompt=prompt,
          llm_response=response.text,
          model_name="my-model",
          token_counts=TokenCounts(prompt=100, completion=50, total=150),
          latency_ms=200,
      )
"""

from __future__ import annotations

import functools
import uuid
from typing import Any, Callable, Optional

from schema import (
    ContextDocument,
    Environment,
    MessageTurn,
    TokenCounts,
    ToolCall,
    TracePayload,
)
from interceptors import clear_active, set_active

_TRACES_ENDPOINT = "/v1/traces"


class SentinelTraceContext:
    def __init__(
        self,
        shipper: Any,
        customer_id: str,
        flow_name: str,
        session_id: str,
        user_id: Optional[str] = None,
        environment: Environment = Environment.PROD,
        retrieved_context: Optional[list[ContextDocument]] = None,
        experiment_id: Optional[str] = None,
    ) -> None:
        self._shipper = shipper
        self._customer_id = customer_id
        self._flow_name = flow_name
        self._session_id = session_id
        self._user_id = user_id
        self._environment = environment
        self._retrieved_context = retrieved_context or []
        self._experiment_id = experiment_id
        self._trace_id = str(uuid.uuid4())
        self._captured: Optional[dict[str, Any]] = None

    @property
    def trace_id(self) -> str:
        """The UUID assigned to this trace. Available immediately after entering the context."""
        return self._trace_id

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "SentinelTraceContext":
        set_active(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        clear_active()
        self._ship_if_captured()
        return False  # never suppress exceptions

    # ------------------------------------------------------------------
    # Decorator  (@sentinel.trace(flow="support"))
    # ------------------------------------------------------------------

    def __call__(self, func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Fresh context per invocation so concurrent calls don't share state
            ctx = SentinelTraceContext(
                shipper=self._shipper,
                customer_id=self._customer_id,
                flow_name=self._flow_name,
                session_id=self._session_id,
                user_id=self._user_id,
                environment=self._environment,
                retrieved_context=list(self._retrieved_context),
                experiment_id=self._experiment_id,
            )
            with ctx:
                return func(*args, **kwargs)

        return wrapper

    # ------------------------------------------------------------------
    # Manual capture (providers other than OpenAI / Anthropic)
    # ------------------------------------------------------------------

    def manual(
        self,
        user_prompt: str,
        llm_response: str,
        model_name: str,
        token_counts: TokenCounts,
        latency_ms: int,
        message_history: Optional[list[MessageTurn]] = None,
        tool_calls: Optional[list[ToolCall]] = None,
    ) -> None:
        """Explicitly record an LLM call. Use for providers other than OpenAI/Anthropic."""
        self._captured = {
            "user_prompt": user_prompt,
            "message_history": message_history or [],
            "llm_response": llm_response,
            "latency_ms": latency_ms,
            "token_counts": token_counts,
            "model_name": model_name,
            "tool_calls": tool_calls or [],
        }

    # ------------------------------------------------------------------
    # Interceptor callbacks (invoked by interceptors.py)
    # ------------------------------------------------------------------

    def _record_openai(
        self,
        call_kwargs: dict[str, Any],
        response: Any,
        latency_ms: int,
    ) -> None:
        messages: list[dict] = call_kwargs.get("messages", [])
        model_name: str = call_kwargs.get("model", "unknown")

        # Last user message is the prompt that triggered this call
        user_prompt = ""
        for m in reversed(messages):
            if m.get("role") == "user":
                content = m.get("content", "")
                user_prompt = content if isinstance(content, str) else str(content)
                break

        history: list[MessageTurn] = [
            MessageTurn(
                role=m.get("role", "user"),
                content=(
                    m["content"]
                    if isinstance(m.get("content"), str)
                    else " ".join(
                        p.get("text", "")
                        for p in m.get("content", [])
                        if isinstance(p, dict)
                    )
                ),
            )
            for m in messages
        ]

        try:
            llm_response = response.choices[0].message.content or ""
        except (AttributeError, IndexError):
            llm_response = ""

        try:
            u = response.usage
            token_counts = TokenCounts(
                prompt=u.prompt_tokens,
                completion=u.completion_tokens,
                total=u.total_tokens,
            )
        except AttributeError:
            token_counts = TokenCounts(prompt=0, completion=0, total=0)

        self._captured = {
            "user_prompt": user_prompt,
            "message_history": history,
            "llm_response": llm_response,
            "latency_ms": latency_ms,
            "token_counts": token_counts,
            "model_name": getattr(response, "model", model_name),
            "tool_calls": [],
        }

    def _record_anthropic(
        self,
        call_kwargs: dict[str, Any],
        response: Any,
        latency_ms: int,
    ) -> None:
        messages: list[dict] = call_kwargs.get("messages", [])
        model_name: str = call_kwargs.get("model", "unknown")

        user_prompt = ""
        for m in reversed(messages):
            if m.get("role") == "user":
                content = m.get("content", "")
                user_prompt = content if isinstance(content, str) else str(content)
                break

        history: list[MessageTurn] = [
            MessageTurn(role=m.get("role", "user"), content=m.get("content", ""))
            for m in messages
        ]

        try:
            llm_response = response.content[0].text
        except (AttributeError, IndexError):
            llm_response = ""

        try:
            u = response.usage
            p, c = u.input_tokens, u.output_tokens
            token_counts = TokenCounts(prompt=p, completion=c, total=p + c)
        except AttributeError:
            token_counts = TokenCounts(prompt=0, completion=0, total=0)

        self._captured = {
            "user_prompt": user_prompt,
            "message_history": history,
            "llm_response": llm_response,
            "latency_ms": latency_ms,
            "token_counts": token_counts,
            "model_name": getattr(response, "model", model_name),
            "tool_calls": [],
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _ship_if_captured(self) -> None:
        if not self._captured:
            return
        try:
            payload = TracePayload(
                trace_id=self._trace_id,
                customer_id=self._customer_id,
                session_id=self._session_id,
                user_id=self._user_id,
                flow_name=self._flow_name,
                environment=self._environment,
                retrieved_context=self._retrieved_context,
                experiment_id=self._experiment_id,
                **self._captured,
            )
            self._shipper.enqueue(_TRACES_ENDPOINT, payload.model_dump(mode="json"))
        except Exception:
            pass  # silently drop — never raise into caller
