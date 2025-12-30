"""Observability utilities for LangSmith tracing and monitoring."""

# Auto-setup LangSmith if available
try:
    from .langsmith_setup import setup_langsmith, traceable_step, get_trace_config
    
    # Auto-setup on import (can be disabled via env var)
    import os
    if os.getenv("LANGCHAIN_TRACING_V2") != "false":
        setup_langsmith()
    
    __all__ = [
        "setup_langsmith",
        "traceable_step",
        "get_trace_config",
    ]
except ImportError:
    # Fallback if langsmith not installed
    def traceable_step(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
    
    def get_trace_config(*args, **kwargs):
        from langchain_core.runnables import RunnableConfig
        return RunnableConfig()
    
    def setup_langsmith(*args, **kwargs):
        return False
    
    __all__ = [
        "setup_langsmith",
        "traceable_step",
        "get_trace_config",
    ]
