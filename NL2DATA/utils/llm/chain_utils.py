"""Utility functions for creating LangChain chains with best practices."""

from typing import TypeVar, Type, Optional, Any, Dict, List
from pydantic import BaseModel, ValidationError

from langchain_core.prompts import ChatPromptTemplate, SystemMessagePromptTemplate, HumanMessagePromptTemplate
from langchain_core.runnables import Runnable, RunnableConfig
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.exceptions import OutputParserException
from langchain_openai import ChatOpenAI

# Try to import RunnableRetry (may not be available in all langchain versions)
try:
    from langchain_core.runnables import RunnableRetry
    HAS_RUNNABLE_RETRY = True
except ImportError:
    # RunnableRetry not available - will use custom retry logic
    HAS_RUNNABLE_RETRY = False
    RunnableRetry = None

from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.llm.prompt_validation import safe_create_prompt_template
from NL2DATA.utils.llm.error_feedback import create_error_feedback_message, NoneOutputError, NoneFieldError
from NL2DATA.utils.llm.model_validation import validate_no_none_fields
from NL2DATA.utils.llm.json_schema_fix import get_openai_compatible_json_schema

logger = get_logger(__name__)

# Try to import pipeline logger (may not be available in all contexts)
try:
    from NL2DATA.tests.utils.pipeline_logger import get_pipeline_logger
    PIPELINE_LOGGER_AVAILABLE = True
except ImportError:
    PIPELINE_LOGGER_AVAILABLE = False

T = TypeVar("T", bound=BaseModel)


class InvalidResponseFormatSchemaError(Exception):
    """Raised when OpenAI rejects the JSON schema passed via response_format."""


def _is_invalid_response_format_schema_error(err: Exception) -> bool:
    """Heuristic detector for OpenAI 'invalid_json_schema' / response_format schema rejection."""
    msg = str(err)
    low = msg.lower()
    return (
        "invalid schema for response_format" in low
        or "invalid_json_schema" in low
        or "text.format.schema" in low
        or "param': 'response_format'" in low
        or "param': 'text.format.schema'" in low
    )


def create_structured_chain(
    llm: ChatOpenAI,
    output_schema: Type[T],
    system_prompt: str,
    human_prompt_template: str,
    use_parser: bool = False,
    expected_variables: Optional[List[str]] = None,
    auto_fix_prompts: bool = True,
    tools: Optional[List[Any]] = None,
    enable_retry: bool = True,
    max_retries: int = 3,
) -> Runnable:
    """
    Create a LangChain chain with structured output following best practices.
    
    This function creates a chain that:
    1. Uses explicit message types (SystemMessage, HumanMessage)
    2. Uses with_structured_output for Pydantic models (preferred method)
    3. Provides fallback to PydanticOutputParser if needed
    
    Args:
        llm: ChatOpenAI model instance
        output_schema: Pydantic model class for structured output
        system_prompt: System message content
        human_prompt_template: Human message template (can include {variables})
        use_parser: If True, use PydanticOutputParser instead of with_structured_output
                    (fallback for models that don't support structured output)
        expected_variables: Optional list of expected variable names in human_prompt_template.
                           If None, will attempt to extract from template.
        auto_fix_prompts: If True, automatically escape JSON examples in prompts to prevent
                         LangChain from interpreting them as template variables.
        tools: Optional list of LangChain tools to bind to the LLM for self-validation
        enable_retry: If True, wrap chain with RunnableRetry for automatic retry
        max_retries: Maximum retry attempts if enable_retry is True
        
    Returns:
        Runnable chain that can be invoked with ainvoke()
        
    Example:
        >>> from NL2DATA.utils.llm import get_model_for_step
        >>> from pydantic import BaseModel, Field
        >>> 
        >>> class Output(BaseModel):
        ...     result: str = Field(description="Result")
        >>> 
        >>> llm = get_model_for_step("1.1")
        >>> chain = create_structured_chain(
        ...     llm=llm,
        ...     output_schema=Output,
        ...     system_prompt="You are a helpful assistant.",
        ...     human_prompt_template="Process: {input}"
        ... )
        >>> result = await chain.ainvoke({"input": "test"})
    """
    # Extract expected variables from template if not provided
    if expected_variables is None:
        import re
        pattern = r'(?<!\{)\{([^}]+)\}(?!\})'
        expected_variables = list(set(re.findall(pattern, human_prompt_template)))
    
    # Validate and fix prompts (escape JSON examples, check variables)
    if auto_fix_prompts:
        try:
            system_prompt, human_prompt_template = safe_create_prompt_template(
                system_prompt=system_prompt,
                human_prompt_template=human_prompt_template,
                expected_variables=expected_variables,
                auto_fix=True
            )
        except Exception as e:
            logger.warning(f"Prompt validation/fixing failed: {e}. Continuing with original prompts.")
    
    # Create prompt template with explicit message types
    # Include error_feedback variable in template (will be empty on first attempt)
    enhanced_human_template = human_prompt_template
    if "{error_feedback}" not in enhanced_human_template:
        # Add error_feedback as variable (will be empty string on first attempt)
        # Use single braces so it gets replaced during formatting
        enhanced_human_template = f"{enhanced_human_template}\n\n{{error_feedback}}"
    
    prompt = ChatPromptTemplate.from_messages([
        SystemMessagePromptTemplate.from_template(system_prompt),
        HumanMessagePromptTemplate.from_template(enhanced_human_template),
    ])
    
    # Bind tools if provided
    # Note: When tools are bound, with_structured_output may not work properly
    # because the LLM might return tool calls instead of structured output.
    # We'll bind tools AFTER applying structured output if possible.
    tools_bound = False
    if tools:
        # For now, we'll bind tools before structured output, but this may cause issues
        # with some models (e.g., o3-mini) that prioritize tool calls
        # TODO: Consider using an agent executor for tool-enabled chains
        llm = llm.bind_tools(tools)
        tools_bound = True
        logger.debug(f"Bound {len(tools)} tools to LLM")
    
    # Use with_structured_output (preferred method for OpenAI models)
    # Note: We can't directly customize the JSON schema that with_structured_output uses,
    # but Pydantic v2's model_json_schema() should handle additionalProperties correctly.
    # If we still get schema errors, they'll be caught and we'll fall back to parser.
    chain = None
    if not use_parser:
        try:
            structured_llm = llm.with_structured_output(output_schema)
            chain = prompt | structured_llm
            logger.debug(f"Created chain using with_structured_output for {output_schema.__name__}")
            if tools_bound:
                logger.warning(
                    f"Tools are bound with with_structured_output. "
                    f"If the LLM returns tool calls instead of structured output, "
                    f"this may cause parsing errors. Consider using use_parser=True or removing tools."
                )
        except Exception as e:
            logger.warning(
                f"with_structured_output failed for {output_schema.__name__}, "
                f"falling back to PydanticOutputParser: {e}"
            )
            # Fallback to parser if with_structured_output fails
            use_parser = True
    
    # Fallback: Use PydanticOutputParser
    if use_parser or chain is None:
        parser = PydanticOutputParser(pydantic_object=output_schema)
        chain = prompt | llm | parser
        logger.debug(f"Created chain using PydanticOutputParser for {output_schema.__name__}")
    
    # Wrap with RunnableRetry if enabled and available (LangChain best practice)
    if enable_retry and HAS_RUNNABLE_RETRY:
        try:
            chain = RunnableRetry(
                chain,
                stop_after_attempt=max_retries,
                wait_exponential_multiplier=1,
                wait_exponential_max=10
            )
            logger.debug(f"Wrapped chain with RunnableRetry (max_retries={max_retries})")
        except Exception as e:
            logger.warning(f"Failed to wrap chain with RunnableRetry: {e}. Will use custom retry logic in invoke_with_retry.")
    elif enable_retry and not HAS_RUNNABLE_RETRY:
        logger.debug(f"RunnableRetry not available in this langchain version. Will use custom retry logic in invoke_with_retry.")
    
    return chain


async def invoke_with_retry(
    chain: Runnable,
    input_data: Dict[str, Any],
    config: Optional[RunnableConfig] = None,
    max_retries: int = 3,
    retry_delay: float = 1.0,
) -> Any:
    """
    Invoke a chain with retry logic for transient errors.
    
    **Note**: If the chain was created with `enable_retry=True` (default),
    it already has RunnableRetry wrapper, so this function provides
    additional error handling and logging.
    
    Args:
        chain: Runnable chain to invoke (may already have RunnableRetry wrapper)
        input_data: Input dictionary for the chain
        config: Optional RunnableConfig with metadata and tags
        max_retries: Maximum number of retry attempts (if chain doesn't have RunnableRetry)
        retry_delay: Delay between retries in seconds (if chain doesn't have RunnableRetry)
        
    Returns:
        Chain output result
        
    Raises:
        Exception: If all retries fail
    """
    # If chain already has RunnableRetry, just invoke with config
    if HAS_RUNNABLE_RETRY and isinstance(chain, RunnableRetry):
        logger.debug("Chain already has RunnableRetry wrapper, invoking directly")
        return await chain.ainvoke(input_data, config=config)
    
    # Otherwise, use custom retry logic with error feedback
    import asyncio
    from openai import RateLimitError, APIError, BadRequestError
    
    # Try to extract output schema from chain (for better error feedback)
    output_schema = None
    try:
        # Check if chain has structured output (with_structured_output)
        if hasattr(chain, "runnable") and hasattr(chain.runnable, "bound") and hasattr(chain.runnable.bound, "kwargs"):
            # Try to get schema from bound kwargs
            bound_kwargs = chain.runnable.bound.kwargs
            if "response_format" in bound_kwargs:
                response_format = bound_kwargs["response_format"]
                if hasattr(response_format, "schema"):
                    output_schema = response_format.schema
        # Also check parser if available
        if output_schema is None and hasattr(chain, "runnable") and hasattr(chain.runnable, "pydantic_object"):
            output_schema = chain.runnable.pydantic_object
    except Exception:
        pass  # Schema extraction failed, continue without it
    
    last_exception = None
    last_raw_output = None
    
    for attempt in range(max_retries):
        try:
            # Enhance input with error feedback if this is a retry
            enhanced_input = input_data.copy()
            if attempt > 0 and last_exception:
                try:
                    error_feedback = create_error_feedback_message(
                        error=last_exception,
                        output_schema=output_schema,  # May be None
                        attempt_number=attempt + 1,
                        raw_output=last_raw_output,
                    )
                    enhanced_input["error_feedback"] = error_feedback
                    logger.info(
                        f"Retrying with error feedback (attempt {attempt + 1}/{max_retries}). "
                        f"Previous error: {type(last_exception).__name__}"
                    )
                except Exception as feedback_error:
                    # If error feedback creation fails, use basic feedback
                    logger.warning(f"Failed to create error feedback: {feedback_error}. Using basic feedback.")
                    enhanced_input["error_feedback"] = (
                        f"⚠️ PREVIOUS ATTEMPT #{attempt + 1} FAILED ⚠️\n\n"
                        f"Error: {type(last_exception).__name__}: {str(last_exception)[:300]}\n\n"
                        f"Please correct your output and try again. DO NOT return None or empty output."
                    )
            else:
                enhanced_input["error_feedback"] = ""  # Empty on first attempt
            
            logger.debug(f"Invoking chain (attempt {attempt + 1}/{max_retries})")
            
            # Capture messages before sending (for logging)
            messages_to_send = None
            
            # Try to extract and render messages from the chain before invocation
            if PIPELINE_LOGGER_AVAILABLE:
                try:
                    pipeline_logger = get_pipeline_logger()
                    if pipeline_logger.file_handle:
                        # If chain has a prompt template, render it to get actual messages
                        if hasattr(chain, 'first'):
                            from langchain_core.prompts import ChatPromptTemplate
                            if isinstance(chain.first, ChatPromptTemplate):
                                try:
                                    rendered = chain.first.format_messages(**enhanced_input)
                                    messages_to_send = rendered
                                    
                                    # Extract LLM parameters from chain
                                    llm_params = {}
                                    try:
                                        # Try to extract LLM from chain
                                        llm_obj = None
                                        if hasattr(chain, 'runnable'):
                                            if hasattr(chain.runnable, 'bound'):
                                                llm_obj = chain.runnable.bound
                                            elif hasattr(chain.runnable, 'runnable'):
                                                llm_obj = chain.runnable.runnable
                                        
                                        if llm_obj:
                                            llm_params = {
                                                "model": getattr(llm_obj, 'model_name', None) or getattr(llm_obj, 'model', None),
                                                "temperature": getattr(llm_obj, 'temperature', None),
                                                "max_tokens": getattr(llm_obj, 'max_tokens', None),
                                                "timeout": getattr(llm_obj, 'timeout', None),
                                            }
                                    except Exception as e:
                                        logger.debug(f"Could not extract LLM parameters: {e}")
                                    
                                    # Log the actual messages being sent
                                    pipeline_logger.log_llm_call(
                                        step_name=f"Standard Chain - Messages Sent (Attempt {attempt + 1})",
                                        messages_sent=messages_to_send,
                                        input_data=enhanced_input,
                                        attempt_number=attempt + 1 if attempt > 0 else None,
                                        llm_params=llm_params if llm_params else None,
                                    )
                                except Exception as e:
                                    logger.debug(f"Could not render prompt template: {e}")
                except Exception as log_error:
                    logger.debug(f"Failed to log chain messages: {log_error}")
            
            result = await chain.ainvoke(enhanced_input, config=config)
            
            # Log the result (which is already parsed for standard chains)
            if PIPELINE_LOGGER_AVAILABLE and attempt == max_retries - 1:
                try:
                    pipeline_logger = get_pipeline_logger()
                    if pipeline_logger.file_handle:
                        pipeline_logger.log_llm_call(
                            step_name=f"Standard Chain - Final Result (Attempt {attempt + 1})",
                            response=result,
                            attempt_number=attempt + 1 if attempt > 0 else None,
                        )
                except Exception as log_error:
                    logger.debug(f"Failed to log chain result: {log_error}")
            
            # CRITICAL: Check for None output - always retry with error feedback
            if result is None:
                raise NoneOutputError("Chain returned None output")
            
            # For Pydantic models, ensure they're not None and have no None fields
            if hasattr(result, '__class__') and isinstance(result, BaseModel):
                # Already a Pydantic model, check if it's valid
                if result is None:
                    raise NoneOutputError("Chain returned None Pydantic model")
                
                # Try to get schema from chain for validation
                try:
                    # Try to extract schema from chain
                    if output_schema is None:
                        # Try to get from result's class
                        output_schema = type(result)
                    
                    if output_schema:
                        # Validate no None values in required fields
                        validate_no_none_fields(result, output_schema)
                except NoneFieldError as e:
                    # Re-raise to trigger retry with feedback
                    raise
                except Exception as validation_error:
                    # If validation fails for other reasons, log but don't fail
                    logger.debug(f"Could not validate None fields: {validation_error}")
            
            if attempt > 0:
                logger.info(
                    f"Chain invocation succeeded on attempt {attempt + 1} "
                    f"after receiving error feedback"
                )
            return result
            
        except BadRequestError as e:
            # BadRequestError (e.g., unsupported parameter) should not be retried
            # Check if it's a permanent error (unsupported parameter)
            error_msg = str(e).lower()
            if "unsupported parameter" in error_msg:
                logger.error(f"Permanent error (unsupported parameter) encountered: {e}. Not retrying.")
                raise
            # OpenAI response_format schema rejection is permanent for a given schema.
            # Retrying will not help; let the caller fall back to non-response_format parsing.
            if _is_invalid_response_format_schema_error(e):
                logger.error(f"Invalid response_format JSON schema encountered: {e}. Triggering parser fallback.")
                raise InvalidResponseFormatSchemaError(str(e)) from e
            # Other BadRequestErrors might be retryable (e.g., invalid schema)
            last_exception = e
            if attempt < max_retries - 1:
                wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                logger.warning(
                    f"Chain invocation failed (attempt {attempt + 1}/{max_retries}): {e}. "
                    f"Retrying in {wait_time:.1f}s..."
                )
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"Chain invocation failed after {max_retries} attempts: {e}")
                raise
        except (RateLimitError, APIError) as e:
            last_exception = e
            if attempt < max_retries - 1:
                wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                logger.warning(
                    f"Chain invocation failed (attempt {attempt + 1}/{max_retries}): {e}. "
                    f"Retrying in {wait_time:.1f}s..."
                )
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"Chain invocation failed after {max_retries} attempts: {e}")
                raise
        except (ValidationError, OutputParserException, NoneOutputError, NoneFieldError) as e:
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
            # Check if it's an output parsing error (might be retryable)
            error_str = str(e).lower()
            if "outputparserexception" in error_str or "jsondecodeerror" in error_str or "invalid json" in error_str:
                # Output parsing errors might be retryable (LLM might return better output on retry)
                last_exception = e
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)
                    logger.warning(
                        f"Output parsing failed (attempt {attempt + 1}/{max_retries}): {e}. "
                        f"Will retry with error feedback..."
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"Output parsing failed after {max_retries} attempts: {e}")
                    raise
            else:
                # For other non-retryable errors, fail immediately
                logger.error(f"Chain invocation failed with non-retryable error: {e}")
                raise
    
    # Should never reach here, but just in case
    if last_exception:
        raise last_exception
    raise RuntimeError("Chain invocation failed for unknown reason")

