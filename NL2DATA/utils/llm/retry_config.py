"""Centralized retry configuration for all LLM calls.

This module provides standardized retry configuration to ensure consistency
across the entire pipeline.
"""

from typing import Dict, Any, Optional

# Standard retry configuration (per user requirement: max 5 retries)
DEFAULT_MAX_RETRIES = 5
DEFAULT_RETRY_DELAY = 1.0


def get_retry_config(max_retries: Optional[int] = None, retry_delay: Optional[float] = None) -> Dict[str, Any]:
    """
    Get standardized retry configuration.
    
    Args:
        max_retries: Override max retries (default: DEFAULT_MAX_RETRIES = 5)
        retry_delay: Override retry delay in seconds (default: DEFAULT_RETRY_DELAY = 1.0)
        
    Returns:
        Dictionary with max_retries and retry_delay
    """
    return {
        "max_retries": max_retries if max_retries is not None else DEFAULT_MAX_RETRIES,
        "retry_delay": retry_delay if retry_delay is not None else DEFAULT_RETRY_DELAY
    }


def get_max_retries() -> int:
    """Get default max retries value."""
    return DEFAULT_MAX_RETRIES


def get_retry_delay() -> float:
    """Get default retry delay value."""
    return DEFAULT_RETRY_DELAY
