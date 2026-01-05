"""Base model router with core model selection logic.

This module provides the foundational model selection functionality that can be
used by phase-specific routers.
"""

from typing import Literal, Optional, Dict, Any
from langchain_openai import ChatOpenAI
from langchain_core.runnables import RunnableConfig

from NL2DATA.config import get_config
from NL2DATA.utils.env import get_api_key
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)

# Task type definitions
TaskType = Literal[
    "simple",              # Simple tasks (domain detection, mention detection)
    "important",           # Important tasks (entity extraction, relation extraction)
    "reasoning",           # Complex reasoning tasks (consolidation, validation)
    "critical_reasoning",  # Most critical reasoning (foundation steps - use o3-pro)
    "advanced_reasoning",  # Advanced reasoning (important validation - use o3)
    "high_fanout",         # High-fan-out tasks (many parallel calls)
    "validation",          # Validation tasks
    "error_correction",    # Error correction tasks
]


def get_model_for_task(
    task_type: TaskType,
    model_name: Optional[str] = None,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
    timeout: Optional[int] = None,
    config: Optional[RunnableConfig] = None,
    metadata: Optional[Dict[str, Any]] = None,
    tags: Optional[list] = None,
) -> ChatOpenAI:
    """
    Get appropriate LLM model for a specific task type.
    
    This is the core model selection function used by phase-specific routers.
    
    Args:
        task_type: Type of task (simple, important, reasoning, high_fanout, validation, error_correction)
        model_name: Override model name (if provided, uses this instead of task-based selection)
        temperature: Temperature setting (defaults to config)
        max_tokens: Max tokens (defaults to config)
        timeout: Request timeout in seconds (defaults to config)
        
    Returns:
        ChatOpenAI: Configured LangChain model instance
        
    Example:
        >>> # For simple task
        >>> model = get_model_for_task("simple")
        >>> # Uses gpt-4o-mini
        
        >>> # For important task
        >>> model = get_model_for_task("important")
        >>> # Uses gpt-4o
        
        >>> # For reasoning task
        >>> model = get_model_for_task("reasoning")
        >>> # Uses o3-mini
    """
    # Get API key
    api_key = get_api_key()
    
    # Get config defaults
    openai_config = get_config("openai")
    
    # Select model based on task type (unless overridden)
    if model_name:
        selected_model = model_name
        logger.debug(f"Using override model: {selected_model}")
    else:
        # Get model selection policy
        model_selection = openai_config.get("model_selection", {})
        
        # Map task type to model
        # Note: o3 models may not be available in all regions/accounts
        # If unavailable, the code will fail at runtime - update config.yaml to use gpt-4o instead
        model_mapping = {
            "simple": model_selection.get("simple", "gpt-4o-mini"),
            "important": model_selection.get("important", "gpt-4o"),
            "reasoning": model_selection.get("reasoning", "o3-mini"),  # Use o3-mini for complex reasoning
            "critical_reasoning": model_selection.get("critical_reasoning", "o3-pro"),  # Use o3-pro for foundation steps
            "advanced_reasoning": model_selection.get("advanced_reasoning", "o3"),  # Use o3 for advanced validation
            "high_fanout": model_selection.get("high_fanout", "gpt-4o-mini"),
            "validation": model_selection.get("validation", "gpt-4o-mini"),
            "error_correction": model_selection.get("error_correction", "gpt-4o"),
        }
        
        selected_model = model_mapping.get(task_type, openai_config.get("model", "gpt-4o-mini"))
        logger.debug(f"Selected model for task type '{task_type}': {selected_model}")
    
    # Extract overrides from config if provided
    if config and config.get("configurable"):
        configurable = config["configurable"]
        temperature = configurable.get("temperature", temperature)
        max_tokens = configurable.get("max_tokens", max_tokens)
        timeout = configurable.get("timeout", timeout)
        # Extract metadata and tags from config
        if not metadata:
            metadata = configurable.get("metadata", {})
        if not tags:
            tags = configurable.get("tags", [])
    
    # Get other settings
    # Default to temperature=0 for maximum consistency (unless model doesn't support it)
    temp = temperature if temperature is not None else openai_config.get("temperature", 0)
    # Get max_tokens: use provided value, or task-specific config, or default
    if max_tokens is not None:
        tokens = max_tokens
    else:
        # Check for task-specific max_tokens config
        max_tokens_per_task = openai_config.get("max_tokens_per_task", {})
        # task_type is a string (Literal type), so we can use it directly
        task_specific_tokens = max_tokens_per_task.get(task_type)
        tokens = task_specific_tokens or openai_config.get("max_tokens", 16000)
    # Get timeout: use provided value, or task-specific config, or default
    if timeout is not None:
        req_timeout = timeout
    else:
        # Check for task-specific timeout config
        timeout_per_task = openai_config.get("timeout_per_task", {})
        # task_type is a string (Literal type), so we can use it directly
        task_specific_timeout = timeout_per_task.get(task_type)
        req_timeout = task_specific_timeout or openai_config.get("timeout", 180)
    
    # Note: ChatOpenAI doesn't directly support metadata/tags in constructor
    # Metadata and tags are passed via RunnableConfig when invoking the model
    # This is handled by get_trace_config() in observability module
    
    # Some models don't support custom temperature parameter
    # Models that only support default temperature (must use temperature=1 or omit it)
    models_without_custom_temp = {
        "o3-mini",
        "o3",
        "o3-pro",
        "gpt-5",
        "gpt-5.1",
        "gpt-5.2",
        # Note: gpt-5.2-pro doesn't exist - removed from list
    }
    
    # Build model kwargs conditionally
    model_kwargs = {
        "model": selected_model,
        "max_tokens": tokens,
        "timeout": req_timeout,
        "api_key": api_key,
    }
    
    # Only add temperature if model supports custom temperature
    if selected_model not in models_without_custom_temp:
        model_kwargs["temperature"] = temp
        logger.info(
            f"Initializing LLM model for task '{task_type}': {selected_model} "
            f"(temperature={temp}, max_tokens={tokens}, timeout={req_timeout})"
        )
    else:
        # For models that don't support custom temperature, explicitly set to 1 (default)
        # Some models (like gpt-5) require temperature=1 explicitly
        model_kwargs["temperature"] = 1.0
        logger.info(
            f"Initializing LLM model for task '{task_type}': {selected_model} "
            f"(temperature=1.0, max_tokens={tokens}, timeout={req_timeout}) - using default temperature (model doesn't support custom temperature)"
        )
    
    # Add metadata and tags for LangSmith tracing
    # Note: ChatOpenAI doesn't directly support metadata/tags in constructor,
    # but we can add them via RunnableConfig when invoking
    # For now, we'll add them via model_kwargs if supported in future versions
    
    return ChatOpenAI(**model_kwargs)

