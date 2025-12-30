"""Simple LLM client for backend - no NL2DATA dependencies."""

import os
import logging
from pathlib import Path
from typing import Type, TypeVar
from pydantic import BaseModel
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.messages import SystemMessage

logger = logging.getLogger(__name__)

# Load .env file if it exists
try:
    from dotenv import load_dotenv
    # Load .env from project root (go up from backend/utils/ to project root)
    # Path(__file__) = backend/utils/llm_client.py
    # parent = backend/utils/
    # parent.parent = backend/
    # parent.parent.parent = project root
    project_root = Path(__file__).parent.parent.parent
    env_path = project_root / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=True)
        logger.debug(f"Loaded .env file from: {env_path}")
    else:
        logger.warning(f".env file not found at: {env_path}")
except ImportError:
    # dotenv not installed, try to use os.environ directly
    logger.warning("python-dotenv not installed, relying on environment variables")

T = TypeVar("T", bound=BaseModel)


def get_openai_client(model_name: str = "gpt-4o-mini", temperature: float = 0.1) -> ChatOpenAI:
    """
    Create a simple OpenAI client.
    
    Args:
        model_name: Model to use (default: gpt-4o-mini)
        temperature: Temperature setting (default: 0.1)
    
    Returns:
        ChatOpenAI instance
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY environment variable not set")
    
    return ChatOpenAI(
        model=model_name,
        temperature=temperature,
        max_tokens=4000,
        timeout=60,
        api_key=api_key
    )


def load_prompt(prompt_file: str) -> str:
    """
    Load a prompt from a file in backend/prompts/.
    
    Args:
        prompt_file: Filename in backend/prompts/ (e.g., "suggestion_prompt.txt")
    
    Returns:
        Prompt text as string
    """
    backend_dir = Path(__file__).parent.parent
    prompt_path = backend_dir / "prompts" / prompt_file
    
    if not prompt_path.exists():
        raise FileNotFoundError(f"Prompt file not found: {prompt_path}")
    
    with open(prompt_path, "r", encoding="utf-8") as f:
        return f.read()


async def call_llm_with_pydantic(
    prompt_text: str,
    output_schema: Type[T],
    input_data: dict,
    model_name: str = "gpt-4o-mini",
    temperature: float = 0.1
) -> T:
    """
    Call LLM with a prompt and get structured Pydantic output.
    
    Args:
        prompt_text: Full prompt text (can include {variable} placeholders)
        output_schema: Pydantic model class for output
        input_data: Dictionary with values for prompt placeholders
        model_name: Model to use
        temperature: Temperature setting
    
    Returns:
        Pydantic model instance
    """
    logger.info(f"Calling LLM with model: {model_name}")
    
    # Create LLM client
    llm = get_openai_client(model_name=model_name, temperature=temperature)
    
    # Create output parser
    parser = PydanticOutputParser(pydantic_object=output_schema)
    
    # Format prompt with input data
    # Use safe formatting that only replaces {nl_description} and escapes other braces
    # First, escape all braces that aren't our placeholder
    safe_prompt = prompt_text.replace("{", "{{").replace("}", "}}")
    # Then un-escape our specific placeholder
    safe_prompt = safe_prompt.replace("{{nl_description}}", "{nl_description}")
    # Now format safely
    formatted_prompt = safe_prompt.format(**input_data)
    
    # Create a system message with the formatted prompt
    message = SystemMessage(content=formatted_prompt)
    
    # Create chain: message -> llm -> parser
    logger.debug(f"Prompt length: {len(formatted_prompt)} characters")
    
    # Invoke LLM
    llm_response = await llm.ainvoke([message])
    
    # Parse response
    result = parser.parse(llm_response.content)
    
    logger.info(f"LLM call completed successfully")
    return result

