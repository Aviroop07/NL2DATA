"""LangSmith setup and tracing utilities.

This module provides LangSmith integration for observability, tracing,
and monitoring of LLM calls.
"""

import os
from functools import wraps
from typing import Callable, Any, Optional, Dict

try:
    from langsmith import traceable
except ImportError:
    # Fallback if langsmith not installed
    def traceable(*args, **kwargs):
        def decorator(func):
            return func
        return decorator

from langchain_core.runnables import RunnableConfig

from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


def setup_langsmith(
    api_key: Optional[str] = None,
    project_name: Optional[str] = None,
    enabled: Optional[bool] = None
) -> bool:
    """Setup LangSmith for tracing and observability.
    
    Args:
        api_key: LangSmith API key (defaults to LANGCHAIN_API_KEY env var)
        project_name: Project name for traces (defaults to "nl2data")
        enabled: Whether to enable LangSmith (defaults to True if API key exists)
        
    Returns:
        True if LangSmith is enabled, False otherwise
        
    Example:
        >>> setup_langsmith(project_name="my_project")
        True
    """
    # Check if already configured
    if os.getenv("LANGCHAIN_TRACING_V2"):
        logger.info("LangSmith already configured via LANGCHAIN_TRACING_V2")
        return True
    
    # Get API key
    if api_key:
        os.environ["LANGCHAIN_API_KEY"] = api_key
    elif not os.getenv("LANGCHAIN_API_KEY"):
        # Try to get from langsmith-specific env var
        langsmith_key = os.getenv("LANGSMITH_API_KEY")
        if langsmith_key:
            os.environ["LANGCHAIN_API_KEY"] = langsmith_key
    
    # Check if we have an API key
    if not os.getenv("LANGCHAIN_API_KEY"):
        if enabled is True:
            logger.warning(
                "LangSmith enabled but no API key found. "
                "Set LANGCHAIN_API_KEY or LANGSMITH_API_KEY environment variable."
            )
            return False
        logger.info("LangSmith not configured (no API key). Tracing disabled.")
        return False
    
    # Enable tracing
    os.environ["LANGCHAIN_TRACING_V2"] = "true"
    
    # Set project name
    if project_name:
        os.environ["LANGCHAIN_PROJECT"] = project_name
    elif not os.getenv("LANGCHAIN_PROJECT"):
        os.environ["LANGCHAIN_PROJECT"] = "nl2data"
    
    # Set endpoint (defaults to langsmith.ai)
    if not os.getenv("LANGCHAIN_ENDPOINT"):
        os.environ["LANGCHAIN_ENDPOINT"] = "https://api.smith.langchain.com"
    
    logger.info(
        f"LangSmith configured: project={os.getenv('LANGCHAIN_PROJECT')}, "
        f"endpoint={os.getenv('LANGCHAIN_ENDPOINT')}"
    )
    
    return True


def traceable_step(
    step_id: str,
    phase: Optional[int] = None,
    tags: Optional[list] = None
):
    """Decorator to add LangSmith tracing to step functions.
    
    This is a convenience wrapper around @traceable that automatically
    adds step metadata and tags.
    
    Args:
        step_id: Step identifier (e.g., "1.4", "2.2")
        phase: Phase number (e.g., 1, 2, 3)
        tags: Additional tags for filtering
        
    Returns:
        Decorated function with tracing enabled
        
    Example:
        >>> @traceable_step("1.4", phase=1, tags=["entity_extraction"])
        >>> async def step_1_4_key_entity_extraction(...):
        ...     pass
    """
    def decorator(func: Callable) -> Callable:
        # Build name
        name = f"step_{step_id}"
        if phase:
            name = f"phase{phase}_{name}"
        
        # Build tags
        trace_tags = []
        if phase:
            trace_tags.append(f"phase{phase}")
        trace_tags.append(f"step_{step_id}")
        if tags:
            trace_tags.extend(tags)
        
        # Build metadata
        metadata = {
            "step_id": step_id,
        }
        if phase:
            metadata["phase"] = phase
        
        # Apply @traceable decorator (if langsmith is available)
        try:
            traced_func = traceable(
                name=name,
                tags=trace_tags,
                metadata=metadata
            )(func)
            return traced_func
        except Exception as e:
            logger.warning(f"Failed to apply @traceable decorator: {e}. Continuing without tracing.")
            return func
    
    return decorator


def get_trace_config(
    step_id: str,
    phase: Optional[int] = None,
    additional_metadata: Optional[Dict[str, Any]] = None,
    tags: Optional[list] = None
) -> RunnableConfig:
    """Get RunnableConfig with tracing metadata.
    
    Args:
        step_id: Step identifier
        phase: Phase number
        additional_metadata: Additional metadata to include
        tags: Tags for filtering
        
    Returns:
        RunnableConfig with metadata and tags
        
    Example:
        >>> config = get_trace_config("1.4", phase=1, tags=["entity_extraction"])
        >>> result = await chain.ainvoke(inputs, config=config)
    """
    metadata = {
        "step_id": step_id,
    }
    if phase:
        metadata["phase"] = phase
    if additional_metadata:
        metadata.update(additional_metadata)
    
    trace_tags = []
    if phase:
        trace_tags.append(f"phase{phase}")
    trace_tags.append(f"step_{step_id}")
    if tags:
        trace_tags.extend(tags)
    
    return RunnableConfig(
        configurable={
            "metadata": metadata,
            "tags": trace_tags
        }
    )

