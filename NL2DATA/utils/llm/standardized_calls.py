"""Standardized LLM call interface with enforced Pydantic output.

This module provides a unified interface for all LLM calls that:
1. Always uses Pydantic models for input/output
2. Converts JSON responses to Pydantic immediately
3. Never returns raw JSON/dicts - only Pydantic objects
4. Provides consistent error handling and retry logic
"""

from typing import TypeVar, Type, Optional, Any, Dict, List
from pydantic import BaseModel, ValidationError
from langchain_openai import ChatOpenAI
from langchain_core.runnables import Runnable, RunnableConfig
import json

from NL2DATA.utils.llm.chain_utils import (
    create_structured_chain,
    invoke_with_retry,
    InvalidResponseFormatSchemaError,
)
from NL2DATA.utils.llm.agent_utils import (
    create_agent_executor_chain,
    invoke_agent_with_structured_output,
    invoke_agent_with_retry,
)
from NL2DATA.utils.llm.agent_chain import create_tool_only_executor
from NL2DATA.utils.llm.tool_result_extraction import format_tool_results_for_prompt
from NL2DATA.utils.llm.error_feedback import NoneOutputError, NoneFieldError
from NL2DATA.utils.llm.model_validation import validate_no_none_fields
from NL2DATA.utils.rate_limiting import get_rate_limiter
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)

# Try to import pipeline logger (may not be available in all contexts)
try:
    from NL2DATA.tests.utils.pipeline_logger import get_pipeline_logger
    PIPELINE_LOGGER_AVAILABLE = True
except ImportError:
    PIPELINE_LOGGER_AVAILABLE = False

T = TypeVar("T", bound=BaseModel)


class StandardizedLLMCall:
    """
    Standardized LLM call wrapper that enforces Pydantic input/output.
    
    This class ensures:
    - All inputs are validated as Pydantic models (if provided)
    - All outputs are Pydantic models (never raw JSON/dicts)
    - Consistent error handling
    - Automatic retry logic
    """
    
    def __init__(
        self,
        llm: ChatOpenAI,
        output_schema: Type[T],
        system_prompt: str,
        human_prompt_template: str,
        tools: Optional[List[Any]] = None,
        use_agent_executor: bool = False,
        decouple_tools: bool = False,
        max_retries: int = 3,
        agent_max_iterations: int = 20,
    ):
        """
        Initialize standardized LLM call.
        
        Args:
            llm: ChatOpenAI model instance
            output_schema: Pydantic model class for output
            system_prompt: System prompt
            human_prompt_template: Human prompt template with {variables}
            tools: Optional list of tools (if provided, uses agent executor)
            use_agent_executor: If True, use agent executor pattern (for tool calls)
            decouple_tools: If True, decouple tool calling from JSON generation:
                           - First call: Use tools only (no JSON expected)
                           - Second call: Generate JSON with tool results as context
            max_retries: Maximum retry attempts
        """
        self.llm = llm
        self.output_schema = output_schema
        self.system_prompt = system_prompt
        self.human_prompt_template = human_prompt_template
        self.tools = tools or []
        self.use_agent_executor = use_agent_executor or (len(self.tools) > 0)
        self.decouple_tools = decouple_tools
        self.max_retries = max_retries
        self.agent_max_iterations = agent_max_iterations
        
        # Create chain or executor
        if self.decouple_tools and self.tools:
            # Decoupled mode: create tool-only executor and structured chain separately
            self.tool_executor = create_tool_only_executor(
                llm=llm,
                tools=self.tools,
                system_prompt=system_prompt,
                human_prompt_template=human_prompt_template,
                max_iterations=self.agent_max_iterations,
            )
            # Structured chain for JSON generation (no tools)
            self.chain = create_structured_chain(
                llm=llm,
                output_schema=output_schema,
                system_prompt=system_prompt,
                human_prompt_template=human_prompt_template,
                tools=None,  # No tools in JSON generation phase
            )
        elif self.use_agent_executor:
            # Coupled mode: agent executor handles both tools and JSON
            self.executor = create_agent_executor_chain(
                llm=llm,
                tools=self.tools,
                system_prompt=system_prompt,
                human_prompt_template=human_prompt_template,
                max_iterations=self.agent_max_iterations,
            )
        else:
            # Standard structured chain (no tools or tools bound directly)
            self.chain = create_structured_chain(
                llm=llm,
                output_schema=output_schema,
                system_prompt=system_prompt,
                human_prompt_template=human_prompt_template,
                tools=self.tools if self.tools else None,
            )
    
    async def invoke(
        self,
        input_data: Dict[str, Any],
        config: Optional[RunnableConfig] = None,
        step_type: Optional[str] = None,
        estimated_tokens: int = 0,
    ) -> T:
        """
        Invoke LLM call and return Pydantic model.
        
        Args:
            input_data: Input dictionary (will be formatted into prompt)
            config: Optional RunnableConfig
            step_type: Optional step type for rate limiting (e.g., "per-entity", "per-relation")
            estimated_tokens: Estimated tokens for this call (for token-based rate limiting)
            
        Returns:
            Pydantic model instance (never a dict/JSON)
            
        Raises:
            ValidationError: If output cannot be parsed into Pydantic model
            ValueError: If LLM returns invalid output
        """
        # Ensure error_feedback is in input_data (empty on first attempt)
        enhanced_input = input_data.copy()
        if "error_feedback" not in enhanced_input:
            enhanced_input["error_feedback"] = ""
        
        # Extract step number from config if available (for logging)
        if config and config.get("configurable"):
            configurable = config["configurable"]
            # Try to get step number from metadata or tags
            if "metadata" in configurable:
                metadata = configurable["metadata"]
                if "step" in metadata:
                    enhanced_input["step_number"] = metadata["step"]
                elif "step_id" in metadata:
                    enhanced_input["step_number"] = metadata["step_id"]
            # Also check tags for step info
            if "tags" in configurable and not enhanced_input.get("step_number"):
                tags = configurable["tags"]
                for tag in tags:
                    if tag and ("step" in tag.lower() or tag.startswith("1.") or tag.startswith("2.") or tag.startswith("3.") or tag.startswith("4.") or tag.startswith("5.") or tag.startswith("6.") or tag.startswith("7.")):
                        enhanced_input["step_number"] = tag
                        break
        
        # Get rate limiter (may be None if disabled)
        rate_limiter = get_rate_limiter()
        
        # Define the actual invocation function
        async def _invoke_with_rate_limit():
            if self.decouple_tools and self.tools:
                # Decoupled mode: two-phase approach
                # Phase 1: Call tools only
                logger.debug("Decoupled mode: Phase 1 - Calling tools only")
                tool_results = await self.tool_executor.ainvoke(enhanced_input, config=config)
                
                # Format tool results for inclusion in prompt
                tool_results_str = format_tool_results_for_prompt(tool_results)
                
                # Phase 2: Generate JSON with tool results as context
                logger.debug("Decoupled mode: Phase 2 - Generating JSON with tool results")
                # Add tool results to input data
                json_input = enhanced_input.copy()
                json_input["tool_results"] = tool_results_str
                
                # Update human prompt template to include tool results placeholder if not present
                json_human_prompt = self.human_prompt_template
                if "{tool_results}" not in json_human_prompt:
                    json_human_prompt = f"{json_human_prompt}\n\nTool Call Results:\n{{tool_results}}"
                
                # Create a new chain with updated prompt (or use existing chain and inject tool_results)
                json_chain = create_structured_chain(
                    llm=self.llm,
                    output_schema=self.output_schema,
                    system_prompt=self.system_prompt,
                    human_prompt_template=json_human_prompt,
                    tools=None,
                )
                
                # Use standard chain with retry
                try:
                    return await invoke_with_retry(
                        chain=json_chain,
                        input_data=json_input,
                        config=config,
                        max_retries=self.max_retries,
                    )
                except InvalidResponseFormatSchemaError:
                    # Global fallback: OpenAI rejected response_format schema.
                    # Retry with parser-based chain (no response_format).
                    logger.warning(
                        "Falling back to parser-based structured output (decoupled JSON phase) "
                        "due to invalid response_format schema."
                    )
                    json_chain = create_structured_chain(
                        llm=self.llm,
                        output_schema=self.output_schema,
                        system_prompt=self.system_prompt,
                        human_prompt_template=json_human_prompt,
                        tools=None,
                        use_parser=True,
                    )
                    return await invoke_with_retry(
                        chain=json_chain,
                        input_data=json_input,
                        config=config,
                        max_retries=self.max_retries,
                    )
            elif self.use_agent_executor:
                # Coupled mode: Use agent executor with structured output (includes error feedback)
                return await invoke_agent_with_retry(
                    executor=self.executor,
                    input_data=enhanced_input,
                    output_schema=self.output_schema,
                    config=config,
                    max_retries=self.max_retries,
                )
            else:
                # Use standard chain with retry (includes error feedback)
                try:
                    return await invoke_with_retry(
                        chain=self.chain,
                        input_data=enhanced_input,
                        config=config,
                        max_retries=self.max_retries,
                    )
                except InvalidResponseFormatSchemaError:
                    # Global fallback: OpenAI rejected response_format schema.
                    # Rebuild chain with parser-based structured output (no response_format) and retry.
                    logger.warning(
                        "Falling back to parser-based structured output due to invalid response_format schema."
                    )
                    parser_chain = create_structured_chain(
                        llm=self.llm,
                        output_schema=self.output_schema,
                        system_prompt=self.system_prompt,
                        human_prompt_template=self.human_prompt_template,
                        tools=None,
                        use_parser=True,
                    )
                    return await invoke_with_retry(
                        chain=parser_chain,
                        input_data=enhanced_input,
                        config=config,
                        max_retries=self.max_retries,
                    )
        
        # Execute with rate limiting if enabled
        if rate_limiter:
            async with rate_limiter.acquire(step_type=step_type, estimated_tokens=estimated_tokens):
                result = await _invoke_with_rate_limit()
        else:
            result = await _invoke_with_rate_limit()
        
        # CRITICAL: Check for None output - always raise error for retry
        if result is None:
            raise NoneOutputError(
                f"LLM call returned None output. Expected {self.output_schema.__name__} instance."
            )
        
        # Ensure result is a Pydantic model (not dict)
        if isinstance(result, dict):
            logger.warning(
                f"LLM returned dict instead of Pydantic model. "
                f"Converting to {self.output_schema.__name__}"
            )
            result = self.output_schema.model_validate(result)
            # Double-check after conversion
            if result is None:
                raise NoneOutputError(
                    f"Pydantic validation returned None. Expected {self.output_schema.__name__} instance."
                )
            # Validate no None values in required fields
            try:
                validate_no_none_fields(result, self.output_schema)
            except NoneFieldError as e:
                raise  # Re-raise to trigger retry with feedback
        elif not isinstance(result, self.output_schema):
            # Try to convert
            try:
                result = self.output_schema.model_validate(result)
                # Double-check after conversion
                if result is None:
                    raise NoneOutputError(
                        f"Pydantic validation returned None. Expected {self.output_schema.__name__} instance."
                    )
                # Validate no None values in required fields
                try:
                    validate_no_none_fields(result, self.output_schema)
                except NoneFieldError as e:
                    raise  # Re-raise to trigger retry with feedback
            except ValidationError as e:
                logger.error(
                    f"Failed to convert result to {self.output_schema.__name__}: {e}"
                )
                raise ValueError(
                    f"LLM output is not compatible with {self.output_schema.__name__}"
                ) from e
        
        # Final None check before returning
        if result is None:
            raise NoneOutputError(
                f"Final result is None. Expected {self.output_schema.__name__} instance."
            )
        
        # Final validation: ensure no None values in required fields
        try:
            validate_no_none_fields(result, self.output_schema)
        except NoneFieldError as e:
            raise  # Re-raise to trigger retry with feedback
        
        # Log to pipeline logger if available
        if PIPELINE_LOGGER_AVAILABLE:
            try:
                pipeline_logger = get_pipeline_logger()
                if pipeline_logger.file_handle:
                    # Try to get actual messages that were sent
                    messages_sent = None
                    raw_response = None
                    
                    # For standard chains, try to render the prompt template to get actual messages
                    if not self.use_agent_executor and hasattr(self, 'chain'):
                        try:
                            from langchain_core.prompts import ChatPromptTemplate
                            from langchain_core.messages import SystemMessage, HumanMessage
                            
                            # Create prompt template and render it
                            enhanced_human_template = self.human_prompt_template
                            if "{error_feedback}" not in enhanced_human_template:
                                enhanced_human_template = f"{enhanced_human_template}\n\n{{error_feedback}}"
                            
                            prompt_template = ChatPromptTemplate.from_messages([
                                ("system", self.system_prompt),
                                ("human", enhanced_human_template),
                            ])
                            
                            # Render with actual input data
                            rendered_messages = prompt_template.format_messages(**input_data)
                            messages_sent = rendered_messages
                        except Exception as e:
                            logger.debug(f"Could not render prompt template for logging: {e}")
                            # Fallback to string format
                            try:
                                formatted_prompt = self.human_prompt_template.format(**input_data)
                                full_prompt = f"{self.system_prompt}\n\n{formatted_prompt}"
                            except:
                                full_prompt = f"{self.system_prompt}\n\n{self.human_prompt_template}"
                    
                    # Extract LLM parameters
                    llm_params = {}
                    try:
                        llm_params = {
                            "model": getattr(self.llm, 'model_name', None) or getattr(self.llm, 'model', None),
                            "temperature": getattr(self.llm, 'temperature', None),
                            "max_tokens": getattr(self.llm, 'max_tokens', None),
                            "timeout": getattr(self.llm, 'timeout', None),
                        }
                    except Exception as e:
                        logger.debug(f"Could not extract LLM parameters: {e}")
                    
                    pipeline_logger.log_llm_call(
                        step_name=step_type or f"{self.output_schema.__name__}",
                        prompt=None if messages_sent else (full_prompt if 'full_prompt' in locals() else None),
                        input_data=input_data,
                        response=result,
                        messages_sent=messages_sent,
                        raw_response=raw_response,
                        llm_params=llm_params if llm_params else None,
                    )
            except Exception as log_error:
                # Don't fail the call if logging fails
                logger.debug(f"Failed to log to pipeline logger: {log_error}")
        
        return result


async def standardized_llm_call(
    llm: ChatOpenAI,
    output_schema: Type[T],
    system_prompt: str,
    human_prompt_template: str,
    input_data: Dict[str, Any],
    tools: Optional[List[Any]] = None,
    use_agent_executor: bool = False,
    decouple_tools: bool = False,
    max_retries: int = 3,
    agent_max_iterations: int = 20,
    config: Optional[RunnableConfig] = None,
    step_type: Optional[str] = None,
    estimated_tokens: int = 0,
) -> T:
    """
    Convenience function for standardized LLM calls.
    
    This is the recommended way to make LLM calls throughout the codebase.
    It ensures:
    - All outputs are Pydantic models
    - No raw JSON/dict handling
    - Consistent error handling
    
    Args:
        llm: ChatOpenAI model instance
        output_schema: Pydantic model class for output
        system_prompt: System prompt
        human_prompt_template: Human prompt template
        input_data: Input data dictionary
        tools: Optional tools for agent executor
        use_agent_executor: If True, use agent executor pattern
        decouple_tools: If True, decouple tool calling from JSON generation:
                       - First call: Use tools only (no JSON expected)
                       - Second call: Generate JSON with tool results as context
        max_retries: Maximum retry attempts
        config: Optional RunnableConfig
        step_type: Optional step type for rate limiting (e.g., "per-entity", "per-relation")
        estimated_tokens: Estimated tokens for this call (for token-based rate limiting)
        
    Returns:
        Pydantic model instance (never a dict)
        
    Example:
        >>> from pydantic import BaseModel, Field
        >>> from NL2DATA.utils.llm import get_model_for_step
        >>> 
        >>> class Output(BaseModel):
        ...     result: str
        >>> 
        >>> llm = get_model_for_step("1.1")
        >>> result = await standardized_llm_call(
        ...     llm=llm,
        ...     output_schema=Output,
        ...     system_prompt="You are a helper.",
        ...     human_prompt_template="Process: {input}",
        ...     input_data={"input": "test"}
        ... )
        >>> # result is a Pydantic Output instance, not a dict
        >>> assert isinstance(result, Output)
        >>> print(result.result)
    """
    call = StandardizedLLMCall(
        llm=llm,
        output_schema=output_schema,
        system_prompt=system_prompt,
        human_prompt_template=human_prompt_template,
        tools=tools,
        use_agent_executor=use_agent_executor,
        decouple_tools=decouple_tools,
        max_retries=max_retries,
        agent_max_iterations=agent_max_iterations,
    )
    
    return await call.invoke(input_data, config=config, step_type=step_type, estimated_tokens=estimated_tokens)


def validate_pydantic_output(
    data: Any,
    schema: Type[T],
    context: str = "output"
) -> T:
    """
    Validate and convert data to Pydantic model.
    
    This function ensures that any data (dict, JSON string, or Pydantic model)
    is converted to the specified Pydantic model.
    
    Args:
        data: Data to validate (dict, JSON string, or Pydantic model)
        schema: Pydantic model class
        context: Context string for error messages
        
    Returns:
        Validated Pydantic model instance
        
    Raises:
        ValidationError: If data cannot be converted to schema
        ValueError: If data is invalid type
    """
    if isinstance(data, schema):
        # Already correct type
        return data
    
    if isinstance(data, dict):
        # Convert dict to Pydantic model
        try:
            return schema.model_validate(data)
        except ValidationError as e:
            logger.error(f"Failed to validate {context} dict as {schema.__name__}: {e}")
            raise
    
    if isinstance(data, str):
        # Try to parse as JSON, then validate
        try:
            parsed = json.loads(data)
            return schema.model_validate(parsed)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse {context} JSON string: {e}")
            raise ValueError(f"{context} is not valid JSON") from e
        except ValidationError as e:
            logger.error(f"Failed to validate {context} JSON as {schema.__name__}: {e}")
            raise
    
    # Try direct validation
    try:
        return schema.model_validate(data)
    except ValidationError as e:
        logger.error(f"Cannot convert {type(data)} to {schema.__name__}: {e}")
        raise ValueError(
            f"{context} must be dict, JSON string, or {schema.__name__} instance"
        ) from e

