"""
sentinel/interceptors.py

One-time global patches for OpenAI and Anthropic SDKs.

Each patch checks for an active SentinelTraceContext on the calling thread
(via a thread-local) and records the call into it. If no context is active,
the original function is called unmodified — zero overhead outside a trace.

install_all() is safe to call multiple times; patches are applied once.
"""

from __future__ import annotations

import threading
import time

# Thread-local slot for the currently active SentinelTraceContext.
# Each thread gets its own slot so concurrent traces don't interfere.
_local: threading.local = threading.local()

_openai_patched: bool = False
_anthropic_patched: bool = False


def get_active() -> object | None:
    return getattr(_local, "ctx", None)


def set_active(ctx: object) -> None:
    _local.ctx = ctx


def clear_active() -> None:
    _local.ctx = None


def install_all() -> None:
    """Install patches for all supported providers. Safe to call repeatedly."""
    _patch_openai()
    _patch_anthropic()


# ------------------------------------------------------------------
# OpenAI
# ------------------------------------------------------------------

def _patch_openai() -> None:
    global _openai_patched
    if _openai_patched:
        return
    try:
        from openai.resources.chat.completions import Completions as _C
        _orig = _C.create

        def _wrapped(self, *args, **kwargs):
            ctx = get_active()
            if ctx is None:
                return _orig(self, *args, **kwargs)
            t0 = time.monotonic()
            result = _orig(self, *args, **kwargs)
            ctx._record_openai(kwargs, result, int((time.monotonic() - t0) * 1000))
            return result

        _C.create = _wrapped
        _openai_patched = True
    except ImportError:
        pass  # openai not installed — skip silently


# ------------------------------------------------------------------
# Anthropic
# ------------------------------------------------------------------

def _patch_anthropic() -> None:
    global _anthropic_patched
    if _anthropic_patched:
        return
    try:
        from anthropic.resources.messages import Messages as _M
        _orig = _M.create

        def _wrapped(self, *args, **kwargs):
            ctx = get_active()
            if ctx is None:
                return _orig(self, *args, **kwargs)
            t0 = time.monotonic()
            result = _orig(self, *args, **kwargs)
            ctx._record_anthropic(kwargs, result, int((time.monotonic() - t0) * 1000))
            return result

        _M.create = _wrapped
        _anthropic_patched = True
    except ImportError:
        pass  # anthropic not installed — skip silently
