"""Common utilities and imports for LangGraph workflows.

This module is intentionally dependency-light and is imported by every phase graph.
"""

from __future__ import annotations

from typing import Dict, Any, Literal, Callable, Awaitable
import inspect
import asyncio

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from ..state import IRGenerationState
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


def _callable_name(fn: Any) -> str:
    """Best-effort name for clearer error messages."""
    try:
        return getattr(fn, "__qualname__", None) or getattr(fn, "__name__", None) or repr(fn)
    except Exception:
        return repr(fn)


def bind_and_validate_call_args(fn: Any, *args: Any, **kwargs: Any) -> inspect.BoundArguments:
    """Bind args/kwargs to fn signature and validate required params are present.

    This catches two common migration bugs early:
    - Wrapper passes an unexpected keyword argument (signature drift)
    - Wrapper forgets a required argument
    """
    name = _callable_name(fn)
    try:
        sig = inspect.signature(fn)
    except Exception as e:
        raise TypeError(f"Cannot inspect signature for {name}: {e}") from e

    try:
        bound = sig.bind(*args, **kwargs)
    except TypeError as e:
        # Re-raise with better context
        raise TypeError(f"Invalid call to {name}: {e}") from e

    missing: list[str] = []
    for p in sig.parameters.values():
        if p.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            continue
        if p.default is not inspect.Parameter.empty:
            continue
        if p.name not in bound.arguments:
            missing.append(p.name)

    if missing:
        raise TypeError(f"Invalid call to {name}: missing required argument(s): {missing}")

    return bound


async def invoke_step_checked(fn: Any, *args: Any, **kwargs: Any) -> Any:
    """Invoke a step with strict signature checking.

    Works with both sync and async step functions.
    """
    bound = bind_and_validate_call_args(fn, *args, **kwargs)
    result = fn(*bound.args, **bound.kwargs)
    if inspect.isawaitable(result):
        return await result
    return result

