"""Structured output parsing and retry logic for agent executors.

Handles extraction of structured JSON output from agent responses,
with robust parsing and retry mechanisms for transient errors.
"""

from typing import TypeVar, Type, Optional, Any, Dict, List, Tuple
from pydantic import BaseModel, ValidationError
from langchain_core.runnables import RunnableConfig, Runnable
from langchain_core.exceptions import OutputParserException
import json
import re
import asyncio

from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.llm.error_feedback import create_error_feedback_message, NoneOutputError, NoneFieldError
from NL2DATA.utils.llm.parsing_helpers import (
    validate_and_return_parsed,
    parse_from_json_string,
    parse_from_dict,
)

logger = get_logger(__name__)

T = TypeVar("T", bound=BaseModel)


async def invoke_agent_with_structured_output(
    executor: Runnable,
    input_data: Dict[str, Any],
    output_schema: Type[T],
    config: Optional[RunnableConfig] = None,
    extract_json: bool = True,
    error_feedback: Optional[str] = None,
) -> Tuple[T, List[Dict[str, Any]]]:
    """
    Invoke agent executor and extract structured output from the final answer.
    
    The agent may use tools in a loop, but we extract the final answer
    and parse it as structured output matching the Pydantic schema.
    
    Args:
        executor: AgentExecutor instance
        input_data: Input dictionary for the agent
        output_schema: Pydantic model class for structured output
        config: Optional RunnableConfig with metadata and tags
        extract_json: If True, try to extract JSON from text response
        error_feedback: Optional error feedback message from previous attempt
        
    Returns:
        Parsed Pydantic model instance
        
    Raises:
        ValidationError: If the output cannot be parsed into the schema
        ValueError: If no valid output can be extracted
    """
    # Capture tool_call_errors outside try block so they're accessible in exception handler
    tool_call_errors = []
    
    try:
        # Enhance input with error feedback if provided
        enhanced_input = input_data.copy()
        if error_feedback:
            # Add error feedback to the input (will be included in prompt)
            enhanced_input["error_feedback"] = error_feedback
            logger.debug(f"Including error feedback in agent invocation (length: {len(error_feedback)} chars)")
        
        logger.debug("Invoking agent executor")
        result = await executor.ainvoke(enhanced_input, config=config)
        
        # Check if result itself is None
        if result is None:
            raise NoneOutputError("Agent executor returned None output")
        
        # Extract tool call errors if present (for error feedback) - capture BEFORE parsing
        tool_call_errors = result.get("tool_call_errors", []) if isinstance(result, dict) else []
        
        # Extract final answer from agent output
        final_answer = result.get("output", "") if isinstance(result, dict) else result
        
        # Check for None or empty output
        if final_answer is None:
            raise NoneOutputError("Agent returned None output")
        if not final_answer:
            raise NoneOutputError("Agent returned empty output")
        
        logger.debug(f"Agent output: {final_answer[:200]}...")  # Log first 200 chars
        
        # Try to parse the final answer using multiple strategies
        parsed_result = _parse_final_answer(
            final_answer=final_answer,
            output_schema=output_schema,
            extract_json=extract_json,
        )
        return parsed_result, tool_call_errors
            
    except Exception as e:
        # Attach tool_call_errors to exception for error feedback
        if tool_call_errors and not hasattr(e, 'tool_call_errors'):
            e.tool_call_errors = tool_call_errors  # type: ignore
        logger.error(f"Agent executor invocation failed: {e}", exc_info=True)
        raise


def _parse_final_answer(
    final_answer: Any,
    output_schema: Type[T],
    extract_json: bool = True,
) -> T:
    """
    Parse final answer from agent into Pydantic model using multiple strategies.
    
    Args:
        final_answer: Final answer from agent (string, dict, or other)
        output_schema: Pydantic model class
        extract_json: If True, try to extract JSON from text response
        
    Returns:
        Parsed Pydantic model instance
        
    Raises:
        ValueError: If parsing fails
        NoneOutputError: If result is None
        NoneFieldError: If required fields have None values
    """
    # Strategy 1: Try to extract and parse JSON from text response
    if extract_json and isinstance(final_answer, str):
        parsed = _extract_json_from_text(final_answer, output_schema)
        if parsed is not None:
            return validate_and_return_parsed(parsed, output_schema)
    
    # Strategy 2: Try to parse as JSON string directly
    if isinstance(final_answer, str):
        try:
            return parse_from_json_string(final_answer, output_schema)
        except ValidationError:
            pass  # Try next strategy
    
    # Strategy 3: Try to create from dict if it's already a dict
    if isinstance(final_answer, dict):
        try:
            return parse_from_dict(final_answer, output_schema)
        except ValidationError:
            pass  # Will raise ValueError below
    
    # All strategies failed
    raise ValueError(
        f"Could not parse agent output into {output_schema.__name__}. "
        f"Output: {str(final_answer)[:500]}"
    )


def _extract_json_from_text(text: str, output_schema: Type[T]) -> Optional[T]:
    """
    Extract and parse JSON from text response.
    
    Args:
        text: Text response from agent
        output_schema: Pydantic model class
        
    Returns:
        Parsed model instance or None if extraction fails
    """
    # First, try to remove markdown code blocks and common prefixes
    cleaned = text
    # Remove markdown code blocks
    cleaned = re.sub(r'```json\s*\n?', '', cleaned)
    cleaned = re.sub(r'```\s*\n?', '', cleaned)
    # Remove common prefixes that LLMs sometimes add
    cleaned = re.sub(r'^(Here is|Here\'s|The result is|The output is|Based on|According to)[\s:]*', '', cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.strip()
    
    # Try to find JSON object in the response (handle nested objects)
    json_str = _find_json_object(cleaned)
    if json_str:
        try:
            data = json.loads(json_str)
            return output_schema(**data)
        except (json.JSONDecodeError, ValidationError) as e:
            logger.debug(f"Failed to parse extracted JSON: {e}. Trying other methods.")
    
    # Fallback: try to parse entire cleaned response as JSON
    try:
        if cleaned.startswith('{') or cleaned.startswith('['):
            data = json.loads(cleaned)
            return output_schema(**data)
    except (json.JSONDecodeError, ValidationError):
        pass
    
    return None


def _find_json_object(text: str) -> Optional[str]:
    """
    Find the first complete JSON object in text.
    
    Args:
        text: Text to search
        
    Returns:
        JSON string or None if not found
    """
    json_start = text.find('{')
    if json_start < 0:
        return None
    
    # Find matching closing brace (handle nested objects and strings)
    brace_count = 0
    json_end = -1
    in_string = False
    escape_next = False
    
    for i in range(json_start, len(text)):
        char = text[i]
        
        if escape_next:
            escape_next = False
            continue
        
        if char == '\\':
            escape_next = True
            continue
        
        if char == '"' and not escape_next:
            in_string = not in_string
            continue
        
        if not in_string:
            if char == '{':
                brace_count += 1
            elif char == '}':
                brace_count -= 1
                if brace_count == 0:
                    json_end = i + 1
                    break
    
    if json_end > json_start:
        return text[json_start:json_end]
    
    return None


async def invoke_agent_with_retry(
    executor: Runnable,
    input_data: Dict[str, Any],
    output_schema: Type[T],
    config: Optional[RunnableConfig] = None,
    max_retries: int = 5,
    retry_delay: float = 1.0,
) -> T:
    """
    Invoke agent executor with retry logic and error feedback.
    
    On retries, provides error feedback to the LLM so it can learn from mistakes.
    
    Args:
        executor: AgentExecutor instance
        input_data: Input dictionary for the agent
        output_schema: Pydantic model class for structured output
        config: Optional RunnableConfig with metadata and tags
        max_retries: Maximum number of retry attempts
        retry_delay: Delay between retries in seconds
        
    Returns:
        Parsed Pydantic model instance
    """
    from openai import RateLimitError, APIError
    
    last_exception = None
    last_raw_output = None
    last_tool_call_errors = []
    
    for attempt in range(max_retries):
        try:
            # Create error feedback if this is a retry
            error_feedback = None
            if attempt > 0 and last_exception:
                error_feedback = create_error_feedback_message(
                    error=last_exception,
                    output_schema=output_schema,
                    attempt_number=attempt + 1,
                    raw_output=last_raw_output,
                    tool_call_errors=last_tool_call_errors,
                )
                logger.debug(
                    f"Retrying with error feedback (attempt {attempt + 1}/{max_retries}). "
                    f"Previous error: {type(last_exception).__name__}"
                )
            
            logger.debug(f"Invoking agent executor (attempt {attempt + 1}/{max_retries})")
            try:
                result, tool_call_errors = await invoke_agent_with_structured_output(
                    executor=executor,
                    input_data=input_data,
                    output_schema=output_schema,
                    config=config,
                    error_feedback=error_feedback,
                )
                # Store tool_call_errors for potential future retries
                last_tool_call_errors = tool_call_errors
                
                if attempt > 0:
                    logger.debug(
                        f"Agent executor invocation succeeded on attempt {attempt + 1} "
                        f"after receiving error feedback"
                    )
                return result
            except (ValueError, ValidationError, OutputParserException, NoneOutputError, NoneFieldError) as e:
                # Capture tool_call_errors from the result before exception
                # They are already captured in last_tool_call_errors from the try block above
                # Re-raise to be caught by outer exception handler
                raise
            
        except (RateLimitError, APIError) as e:
            # API errors - retry with backoff but no feedback needed
            last_exception = e
            if attempt < max_retries - 1:
                wait_time = retry_delay * (2 ** attempt)
                logger.warning(
                    f"API error (attempt {attempt + 1}/{max_retries}): {e}. "
                    f"Retrying in {wait_time:.1f}s..."
                )
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"Agent executor invocation failed after {max_retries} attempts: {e}")
                raise
                
        except (ValueError, ValidationError, OutputParserException, NoneOutputError, NoneFieldError) as e:
            # Output parsing/schema errors - capture for feedback
            last_exception = e
            # Try to extract raw output from exception
            if isinstance(e, OutputParserException) and hasattr(e, "llm_output"):
                last_raw_output = str(e.llm_output)
            elif isinstance(e, (ValueError, NoneOutputError)) and "Output:" in str(e):
                # Try to extract output from error message
                try:
                    output_part = str(e).split("Output:")[-1].strip()
                    last_raw_output = output_part[:500]
                except:
                    pass
            # Capture tool_call_errors from exception if attached
            if hasattr(e, 'tool_call_errors'):
                last_tool_call_errors = e.tool_call_errors  # type: ignore
            
            if attempt < max_retries - 1:
                wait_time = retry_delay * (2 ** attempt)
                error_type_name = type(e).__name__
                logger.warning(
                    f"Output parsing failed (attempt {attempt + 1}/{max_retries}): {error_type_name}: {e}. "
                    f"Will retry with error feedback..."
                )
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"Output parsing failed after {max_retries} attempts: {e}")
                raise
                
        except Exception as e:
            # For other non-retryable errors, fail immediately
            logger.error(f"Agent executor invocation failed with non-retryable error: {e}")
            raise
    
    # Should never reach here, but just in case
    if last_exception:
        raise last_exception
    raise RuntimeError("Agent executor invocation failed for unknown reason")

