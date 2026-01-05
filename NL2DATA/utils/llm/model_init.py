"""Initialize LangChain LLM models."""

from langchain_openai import ChatOpenAI
from typing import Optional

from NL2DATA.config import get_config
from NL2DATA.utils.env import get_api_key
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


def get_llm_model(
    model_name: Optional[str] = None,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
    timeout: Optional[int] = None,
) -> ChatOpenAI:
    """
    Initialize and return a LangChain ChatOpenAI model.
    
    Args:
        model_name: Model name (defaults to config)
        temperature: Temperature setting (defaults to config)
        max_tokens: Max tokens (defaults to config)
        timeout: Request timeout in seconds (defaults to config)
        
    Returns:
        ChatOpenAI: Configured LangChain model instance
    """
    # Get API key
    api_key = get_api_key()
    
    # Get config defaults
    openai_config = get_config("openai")
    
    # Use provided values or fall back to config
    model = model_name or openai_config.get("model", "gpt-4o-mini")
    temp = temperature if temperature is not None else openai_config.get("temperature", 0.1)
    tokens = max_tokens or openai_config.get("max_tokens", 4000)
    req_timeout = timeout or openai_config.get("timeout", 60)
    
    # Note: Logging is handled by base_router.py to avoid duplicates
    # Only log here if this function is called directly (not via base_router)
    
    return ChatOpenAI(
        model=model,
        temperature=temp,
        max_tokens=tokens,
        timeout=req_timeout,
        api_key=api_key,
    )

