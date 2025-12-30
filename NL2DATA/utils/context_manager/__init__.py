"""Context management and token budget allocation.

This module manages context to prevent token overflow, compresses context
when needed, and provides enhanced context for validation steps.
"""

from .manager import ContextManager, prepare_context

__all__ = [
    "ContextManager",
    "prepare_context",
]

