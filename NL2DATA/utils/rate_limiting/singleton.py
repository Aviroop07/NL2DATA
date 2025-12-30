"""Singleton rate limiter instance.

Provides a global rate limiter instance configured from config.yaml.
"""

from typing import Optional
from NL2DATA.config import get_config
from NL2DATA.utils.logging import get_logger
from .limiter import RateLimiter

logger = get_logger(__name__)

# Global rate limiter instance (lazy initialization)
_rate_limiter: Optional[RateLimiter] = None


def get_rate_limiter() -> Optional[RateLimiter]:
    """
    Get or create the global rate limiter instance.
    
    The rate limiter is configured from config.yaml and is a singleton.
    If rate limiting is disabled in config, returns None.
    
    Returns:
        RateLimiter instance or None if rate limiting is disabled
    """
    global _rate_limiter
    
    if _rate_limiter is not None:
        return _rate_limiter
    
    try:
        rate_config = get_config("rate_limiting")
        
        if not rate_config.get("enabled", True):
            logger.info("Rate limiting is disabled in config")
            return None
        
        _rate_limiter = RateLimiter(
            requests_per_minute=rate_config.get("requests_per_minute", 500),
            tokens_per_minute=rate_config.get("tokens_per_minute", 1_000_000),
            max_concurrent=rate_config.get("max_concurrent", 10),
            max_concurrency_per_step_type=rate_config.get("max_concurrency_per_step_type", {})
        )
        
        logger.info(
            f"Initialized rate limiter: {rate_config.get('requests_per_minute')} req/min, "
            f"{rate_config.get('tokens_per_minute')} tokens/min, "
            f"{rate_config.get('max_concurrent')} max concurrent"
        )
        
        return _rate_limiter
    except Exception as e:
        logger.warning(f"Failed to initialize rate limiter: {e}. Continuing without rate limiting.")
        return None


def reset_rate_limiter():
    """Reset the global rate limiter instance (useful for testing)."""
    global _rate_limiter
    _rate_limiter = None

