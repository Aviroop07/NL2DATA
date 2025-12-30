"""Agent chain creation for LangChain agent executors.

Main entry point for creating agent executor chains using LangChain's built-in
AgentExecutor and create_tool_calling_agent for best practices.
"""

from typing import Any, Dict, List, Optional
from langchain_openai import ChatOpenAI
from langchain_core.runnables import Runnable, RunnableLambda
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage, ToolMessage
from langchain_core.tools import BaseTool

# LangChain import compatibility:
# - Newer versions may not export AgentExecutor from langchain.agents
try:
    from langchain.agents import create_tool_calling_agent
    from langchain.agents.agent import AgentExecutor  # type: ignore
except Exception:  # pragma: no cover
    from langchain.agents import AgentExecutor, create_tool_calling_agent  # type: ignore

# Try to import RunnableRetry for network retries
try:
    from langchain_core.runnables import RunnableRetry
    HAS_RUNNABLE_RETRY = True
except ImportError:
    HAS_RUNNABLE_RETRY = False
    RunnableRetry = None

from NL2DATA.utils.llm.tool_converter import convert_to_structured_tools
from NL2DATA.utils.llm.prompt_enhancement import enhance_system_prompt
from NL2DATA.utils.llm.tool_utils import (
    extract_tool_name,
    extract_tool_args,
    extract_tool_call_id,
)
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)

# Try to import pipeline logger (may not be available in all contexts)
try:
    from NL2DATA.tests.utils.pipeline_logger import get_pipeline_logger
    PIPELINE_LOGGER_AVAILABLE = True
except ImportError:
    PIPELINE_LOGGER_AVAILABLE = False


# Note: _safe_format_template removed - LangChain's ChatPromptTemplate handles this automatically


def create_agent_executor_chain(
    llm: ChatOpenAI,
    tools: List[Any],
    system_prompt: str,
    human_prompt_template: str,
    max_iterations: int = 5,
    return_intermediate_steps: bool = False,
    handle_parsing_errors: bool = True,
    enable_network_retry: bool = True,
    network_retry_attempts: int = 4,
) -> Runnable:
    """
    Create an agent executor chain using LangChain's built-in AgentExecutor.
    
    This uses LangChain best practices:
    - create_tool_calling_agent for agent creation
    - AgentExecutor for execution
    - .with_retry() for network retries with exponential backoff
    
    Args:
        llm: ChatOpenAI model instance (should have max_retries=0 to avoid double-retrying)
        tools: List of LangChain tools for the agent to use
        system_prompt: System message content
        human_prompt_template: Human message template (can include {variables})
        max_iterations: Maximum number of tool call iterations (safety limit)
        return_intermediate_steps: Whether to return intermediate tool call steps
        handle_parsing_errors: Whether to handle tool call parsing errors gracefully
        enable_network_retry: Whether to enable network retries with .with_retry()
        network_retry_attempts: Number of network retry attempts if enable_network_retry is True
        
    Returns:
        Runnable chain that can be invoked with ainvoke()
    """
    # Convert tools to StructuredTool objects
    structured_tools = convert_to_structured_tools(tools)
    
    # Enhance system prompt to emphasize structured output (with tool info)
    enhanced_system_prompt = enhance_system_prompt(system_prompt, tools=structured_tools)
    
    # Create prompt template
    # Note: LangChain's create_tool_calling_agent expects a ChatPromptTemplate
    # The human message should use {input} - we'll format the actual content in the wrapper
    prompt = ChatPromptTemplate.from_messages([
        ("system", enhanced_system_prompt),
        ("human", "{input}"),  # Use {input} placeholder - actual content formatted in wrapper
        ("placeholder", "{agent_scratchpad}"),
    ])
    
    # Create agent using LangChain's create_tool_calling_agent
    # Set max_retries=0 on LLM to avoid double-retrying (network retries handled by .with_retry())
    llm_for_agent = llm
    if hasattr(llm, 'max_retries'):
        # Create a copy with max_retries=0 if possible
        try:
            llm_for_agent = llm.__class__(
                model=llm.model_name if hasattr(llm, 'model_name') else getattr(llm, 'model', None),
                temperature=getattr(llm, 'temperature', 0),
                timeout=getattr(llm, 'timeout', None),
                max_retries=0,  # No retries here - handled by .with_retry() below
            )
        except Exception as e:
            logger.warning(f"Could not create LLM copy with max_retries=0: {e}. Using original LLM.")
            llm_for_agent = llm
    
    agent = create_tool_calling_agent(llm_for_agent, structured_tools, prompt)
    
    # Create AgentExecutor
    # Always return intermediate_steps so we can log tool calls
    executor = AgentExecutor(
        agent=agent,
        tools=structured_tools,
        verbose=False,  # Set to True for debugging
        handle_parsing_errors=handle_parsing_errors,
        return_intermediate_steps=True,  # Always return intermediate steps for logging
        max_iterations=max_iterations,
    )
    
    # Wrap with network retries using .with_retry() (LangChain best practice)
    if enable_network_retry and HAS_RUNNABLE_RETRY:
        try:
            executor = executor.with_retry(
                stop_after_attempt=network_retry_attempts,
                wait_exponential_jitter=True,
                retry_if_exception_type=(Exception,),  # Retry on all exceptions (tighten in prod)
            )
            logger.debug(f"Wrapped agent executor with RunnableRetry (max_attempts={network_retry_attempts})")
        except Exception as e:
            logger.warning(f"Failed to wrap agent executor with RunnableRetry: {e}. Continuing without network retries.")
    elif enable_network_retry and not HAS_RUNNABLE_RETRY:
        logger.debug("RunnableRetry not available. Network retries will not be applied.")
    
    # Wrap executor to handle input formatting
    # AgentExecutor expects {"input": "..."}, but our templates may have variables
    # We format the template with provided variables and pass as "input"
    async def wrapped_executor(inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Wrapper to format template variables and pass to AgentExecutor."""
        # Extract step number from inputs if available
        step_number = inputs.get("step_number", inputs.get("step", "unknown"))
        
        # If "input" key exists, use it directly (backward compatibility)
        if "input" in inputs:
            agent_input = {"input": inputs["input"]}
        else:
            # Format the human prompt template with provided variables
            try:
                formatted_input = human_prompt_template.format(**inputs)
                agent_input = {"input": formatted_input}
            except (KeyError, ValueError) as e:
                # If formatting fails, try to use template as-is or use first value
                logger.warning(f"Could not format template with provided variables: {e}. Using template as-is.")
                # Try to extract a meaningful input from the variables
                if inputs:
                    # Use the first string value or convert dict to string
                    first_value = next(iter(inputs.values()))
                    if isinstance(first_value, str):
                        agent_input = {"input": first_value}
                    else:
                        agent_input = {"input": str(inputs)}
                else:
                    agent_input = {"input": human_prompt_template}
        
        # Invoke AgentExecutor
        try:
            result = await executor.ainvoke(agent_input)
            
            # Extract tool calls and results from intermediate_steps if available
            tool_calls = []
            tool_results = []
            tool_errors = []
            
            if "intermediate_steps" in result:
                for step in result["intermediate_steps"]:
                    # Each step is a tuple: (AgentAction, observation)
                    if len(step) >= 2:
                        action = step[0]
                        observation = step[1]
                        
                        # Extract tool call info
                        tool_name = getattr(action, "tool", getattr(action, "tool_name", "unknown"))
                        tool_input = getattr(action, "tool_input", getattr(action, "input", {}))
                        tool_calls.append({
                            "tool": tool_name,
                            "args": tool_input,
                        })
                        
                        # Extract tool result
                        if isinstance(observation, str):
                            # Check if it's an error
                            if "error" in observation.lower() or "exception" in observation.lower():
                                tool_errors.append({
                                    "tool": tool_name,
                                    "error": observation,
                                    "args": tool_input,
                                })
                            tool_results.append({
                                "tool": tool_name,
                                "result": observation,
                            })
                        else:
                            tool_results.append({
                                "tool": tool_name,
                                "result": str(observation),
                            })
            
            # Log to pipeline logger if available
            try:
                from NL2DATA.tests.utils.pipeline_logger import get_pipeline_logger
                pipeline_logger = get_pipeline_logger()
                if pipeline_logger.file_handle:
                    step_name = f"Step {step_number}: Agent Executor"
                    pipeline_logger.log_llm_call(
                        step_name=step_name,
                        response=result.get("output"),
                        tool_calls=tool_calls if tool_calls else None,
                        tool_results=tool_results if tool_results else None,
                    )
                    # Log errors separately if any
                    if tool_errors:
                        pipeline_logger.log_error(step_name, tool_errors)
            except ImportError:
                pass  # Pipeline logger not available
            except Exception as e:
                logger.debug(f"Failed to log to pipeline logger: {e}")
            
            # LangChain AgentExecutor returns a dict with 'output' and 'intermediate_steps'
            # We need to ensure the output matches what our system expects (e.g., JSON)
            # For now, we return the raw output, assuming downstream parsing will handle it.
            return result
            
        except Exception as e:
            # Log error to pipeline logger if available
            try:
                from NL2DATA.tests.utils.pipeline_logger import get_pipeline_logger
                pipeline_logger = get_pipeline_logger()
                if pipeline_logger.file_handle:
                    step_name = f"Step {step_number}: Agent Executor"
                    pipeline_logger.log_error(step_name, [{"error": str(e), "type": type(e).__name__}])
            except ImportError:
                pass
            except Exception as log_error:
                logger.debug(f"Failed to log error to pipeline logger: {log_error}")
            
            # Re-raise the exception
            raise
    
    wrapped = RunnableLambda(wrapped_executor)
    
    logger.debug(f"Created LangChain AgentExecutor with max_iterations={max_iterations}")
    return wrapped


def create_tool_only_executor(
    llm: ChatOpenAI,
    tools: List[Any],
    system_prompt: str,
    human_prompt_template: str,
    max_iterations: int = 5,
    enable_network_retry: bool = True,
    network_retry_attempts: int = 4,
) -> Runnable:
    """
    Create an agent executor chain that ONLY calls tools and returns tool results.
    
    This is used in decoupled mode where:
    1. First call: Use this executor to call tools and get results
    2. Second call: Use structured chain (no tools) with tool results as context
    
    Uses LangChain's built-in AgentExecutor pattern.
    
    Args:
        llm: ChatOpenAI model instance
        tools: List of LangChain tools for the agent to use
        system_prompt: System message content
        human_prompt_template: Human message template (can include {variables})
        max_iterations: Maximum number of tool call iterations (safety limit)
        enable_network_retry: Whether to enable network retries with .with_retry()
        network_retry_attempts: Number of network retry attempts if enable_network_retry is True
        
    Returns:
        Runnable chain that returns tool results (not JSON)
    """
    # Convert tools to StructuredTool objects
    structured_tools = convert_to_structured_tools(tools)
    
    # Enhance system prompt for tool usage (but don't ask for JSON output)
    enhanced_system_prompt = enhance_system_prompt(system_prompt, tools=structured_tools)
    # Modify prompt to emphasize tool usage, not JSON output
    enhanced_system_prompt += "\n\nIMPORTANT: Use the available tools to gather information and validate your understanding. After using tools, you do NOT need to provide a final JSON answer - just use the tools as needed."
    
    # Create prompt template
    # The human message should use {input} - we'll format the actual content in the wrapper
    prompt = ChatPromptTemplate.from_messages([
        ("system", enhanced_system_prompt),
        ("human", "{input}"),  # Use {input} placeholder - actual content formatted in wrapper
        ("placeholder", "{agent_scratchpad}"),
    ])
    
    # Set max_retries=0 on LLM to avoid double-retrying
    llm_for_agent = llm
    if hasattr(llm, 'max_retries'):
        try:
            llm_for_agent = llm.__class__(
                model=llm.model_name if hasattr(llm, 'model_name') else getattr(llm, 'model', None),
                temperature=getattr(llm, 'temperature', 0),
                timeout=getattr(llm, 'timeout', None),
                max_retries=0,
            )
        except Exception as e:
            logger.warning(f"Could not create LLM copy with max_retries=0: {e}. Using original LLM.")
            llm_for_agent = llm
    
    # Create agent
    agent = create_tool_calling_agent(llm_for_agent, structured_tools, prompt)
    
    # Create AgentExecutor
    executor = AgentExecutor(
        agent=agent,
        tools=structured_tools,
        verbose=False,
        handle_parsing_errors=True,
        return_intermediate_steps=True,  # Need this to extract tool results
        max_iterations=max_iterations,
    )
    
    # Wrap with network retries
    if enable_network_retry and HAS_RUNNABLE_RETRY:
        try:
            executor = executor.with_retry(
                stop_after_attempt=network_retry_attempts,
                wait_exponential_jitter=True,
                retry_if_exception_type=(Exception,),
            )
            logger.debug(f"Wrapped tool-only executor with RunnableRetry (max_attempts={network_retry_attempts})")
        except Exception as e:
            logger.warning(f"Failed to wrap tool-only executor with RunnableRetry: {e}.")
    
    # Wrap executor to handle input formatting (same as create_agent_executor_chain)
    async def wrapped_tool_executor(inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Wrapper to format template variables and pass to AgentExecutor."""
        # Extract step number from inputs if available
        step_number = inputs.get("step_number", inputs.get("step", "unknown"))
        
        if "input" in inputs:
            agent_input = {"input": inputs["input"]}
        else:
            try:
                formatted_input = human_prompt_template.format(**inputs)
                agent_input = {"input": formatted_input}
            except (KeyError, ValueError) as e:
                logger.warning(f"Could not format template with provided variables: {e}. Using template as-is.")
                if inputs:
                    first_value = next(iter(inputs.values()))
                    if isinstance(first_value, str):
                        agent_input = {"input": first_value}
                    else:
                        agent_input = {"input": str(inputs)}
                else:
                    agent_input = {"input": human_prompt_template}
        
        try:
            result = await executor.ainvoke(agent_input)
            
            # Extract tool calls and results from intermediate_steps if available
            tool_calls = []
            tool_results = []
            tool_errors = []
            
            if "intermediate_steps" in result:
                for step in result["intermediate_steps"]:
                    # Each step is a tuple: (AgentAction, observation)
                    if len(step) >= 2:
                        action = step[0]
                        observation = step[1]
                        
                        # Extract tool call info
                        tool_name = getattr(action, "tool", getattr(action, "tool_name", "unknown"))
                        tool_input = getattr(action, "tool_input", getattr(action, "input", {}))
                        tool_calls.append({
                            "tool": tool_name,
                            "args": tool_input,
                        })
                        
                        # Extract tool result
                        if isinstance(observation, str):
                            # Check if it's an error
                            if "error" in observation.lower() or "exception" in observation.lower():
                                tool_errors.append({
                                    "tool": tool_name,
                                    "error": observation,
                                    "args": tool_input,
                                })
                            tool_results.append({
                                "tool": tool_name,
                                "result": observation,
                            })
                        else:
                            tool_results.append({
                                "tool": tool_name,
                                "result": str(observation),
                            })
            
            # Log to pipeline logger if available
            try:
                from NL2DATA.tests.utils.pipeline_logger import get_pipeline_logger
                pipeline_logger = get_pipeline_logger()
                if pipeline_logger.file_handle:
                    step_name = f"Step {step_number}: Tool-Only Executor"
                    pipeline_logger.log_llm_call(
                        step_name=step_name,
                        response=result.get("output"),
                        tool_calls=tool_calls if tool_calls else None,
                        tool_results=tool_results if tool_results else None,
                    )
                    # Log errors separately if any
                    if tool_errors:
                        pipeline_logger.log_error(step_name, tool_errors)
            except ImportError:
                pass  # Pipeline logger not available
            except Exception as e:
                logger.debug(f"Failed to log to pipeline logger: {e}")
            
            return result
            
        except Exception as e:
            # Log error to pipeline logger if available
            try:
                from NL2DATA.tests.utils.pipeline_logger import get_pipeline_logger
                pipeline_logger = get_pipeline_logger()
                if pipeline_logger.file_handle:
                    step_name = f"Step {step_number}: Tool-Only Executor"
                    pipeline_logger.log_error(step_name, [{"error": str(e), "type": type(e).__name__}])
            except ImportError:
                pass
            except Exception as log_error:
                logger.debug(f"Failed to log error to pipeline logger: {log_error}")
            
            # Re-raise the exception
            raise
    
    wrapped = RunnableLambda(wrapped_tool_executor)
    
    logger.debug(f"Created tool-only executor chain with max_iterations={max_iterations}")
    return wrapped

