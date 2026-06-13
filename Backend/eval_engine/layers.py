"""
backend/eval_engine/layers.py

Three-layer evaluation engine.

Layer 1 — Heuristic checks (fast, deterministic, no LLM calls)
    Implemented in EVAL-02. Stub returns no failure for all traces.

Layer 2 — Behavioral signals (cross-trace patterns, user signals)
    Implemented in EVAL-03. Stub returns no failure for all traces.

Layer 3 — LLM-as-judge (semantic, requires an LLM API call)
    Implemented in EVAL-04. Stub returns no failure for all traces.
    Always runs as a background task — must never block the ACK.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from schema import TracePayload


@dataclass
class LayerResult:
    """Result produced by one evaluation layer for a single trace."""
    layer: int
    passed: bool                          # True = no failure detected
    failure_type: Optional[str] = None    # e.g. "hallucination", "context_contradiction"
    severity: Optional[str] = None        # 'low' | 'medium' | 'high' | 'critical'
    confidence: float = 0.0              # 0.0–1.0
    evidence: Optional[str] = None       # human-readable explanation


async def run_layer1(trace: "TracePayload") -> LayerResult:
    """
    Layer 1: Heuristic checks (latency spikes, response-length anomalies,
    known-bad patterns). Implemented in EVAL-02.
    """
    return LayerResult(layer=1, passed=True)


async def run_layer2(trace: "TracePayload") -> LayerResult:
    """
    Layer 2: Behavioral signal checks (escalation patterns, rephrased
    queries, thumbs-down signals). Implemented in EVAL-03.
    """
    return LayerResult(layer=2, passed=True)


async def run_layer3(trace: "TracePayload") -> LayerResult:
    """
    Layer 3: LLM-as-judge (semantic coherence, context contradiction,
    policy compliance). Implemented in EVAL-04.
    Runs as a background task in the eval-worker.
    """
    return LayerResult(layer=3, passed=True)
