"""Agent loop execution logic.

Handles the execution of agent loops with tool calls, including
tool call batching and message management.
"""

from typing import Any, Dict, List
from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.tools import BaseTool

from NL2DATA.utils.llm.tool_executor import execute_tool_call
from NL2DATA.utils.llm.tool_utils import (
    extract_tool_name,
    extract_tool_args,
    extract_tool_call_id,
    format_tool_error_for_llm,
)
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)

# Try to import pipeline logger (may not be available in all contexts)
try:
    from NL2DATA.tests.utils.pipeline_logger import get_pipeline_logger
    PIPELINE_LOGGER_AVAILABLE = True
except ImportError:
    PIPELINE_LOGGER_AVAILABLE = False


async def execute_agent_loop(
    llm_with_tools: Any,
    messages: List[Any],
    structured_tools: List[BaseTool],
    max_iterations: int,
    tool_call_errors: List[Dict[str, Any]],
) -> None:
    """
    Execute agent loop with tool calls.
    
    Args:
        llm_with_tools: LLM with tools bound
        messages: Message history
        structured_tools: List of available tools
        max_iterations: Maximum iterations
        tool_call_errors: List to collect tool call errors
    """
    iteration = 0
    all_tool_calls = []  # Track all tool calls for logging
    all_tool_results = []  # Track all tool results for logging
    
    while iteration < max_iterations:
        # Log messages being sent (for first iteration or when we have new messages)
        if PIPELINE_LOGGER_AVAILABLE and iteration == 0:
            try:
                pipeline_logger = get_pipeline_logger()
                if pipeline_logger.file_handle:
                    # Extract LLM parameters
                    llm_params = {}
                    try:
                        if hasattr(llm_with_tools, 'model_name') or hasattr(llm_with_tools, 'model'):
                            llm_params = {
                                "model": getattr(llm_with_tools, 'model_name', None) or getattr(llm_with_tools, 'model', None),
                                "temperature": getattr(llm_with_tools, 'temperature', None),
                                "max_tokens": getattr(llm_with_tools, 'max_tokens', None),
                                "timeout": getattr(llm_with_tools, 'timeout', None),
                            }
                    except Exception as e:
                        logger.debug(f"Could not extract LLM parameters: {e}")
                    
                    # Log the initial messages being sent
                    pipeline_logger.log_llm_call(
                        step_name="Agent Executor - Initial Messages",
                        messages_sent=messages.copy(),
                        llm_params=llm_params if llm_params else None,
                    )
            except Exception as log_error:
                logger.debug(f"Failed to log agent executor messages: {log_error}")
        
        # Get LLM response
        response = await llm_with_tools.ainvoke(messages)
        
        # Log raw response immediately after receiving
        if PIPELINE_LOGGER_AVAILABLE:
            try:
                pipeline_logger = get_pipeline_logger()
                if pipeline_logger.file_handle:
                    pipeline_logger.log_llm_call(
                        step_name=f"Agent Executor - LLM Response (Iteration {iteration + 1})",
                        raw_response=response,
                    )
            except Exception as log_error:
                logger.debug(f"Failed to log agent executor raw response: {log_error}")
        
        messages.append(response)
        
        # Check if response has tool calls
        tool_calls = getattr(response, 'tool_calls', None) or []
        if tool_calls:
            # Collect tool calls for logging
            for tool_call in tool_calls:
                tool_name = extract_tool_name(tool_call)
                tool_args, _ = extract_tool_args(tool_call)
                all_tool_calls.append({
                    "tool": tool_name,
                    "args": tool_args,
                })
            
            # Execute tools and track errors
            tool_results_batch = []
            await execute_tool_calls_batch(
                tool_calls=tool_calls,
                structured_tools=structured_tools,
                messages=messages,
                tool_call_errors=tool_call_errors,
            )
            
            # Collect tool results from messages (last added tool messages)
            for msg in messages[-len(tool_calls):]:
                if hasattr(msg, 'content'):
                    all_tool_results.append({
                        "result": msg.content,
                    })
            
            iteration += 1
        else:
            # No tool calls, return final answer
            break
    
    # Log final message history for agent executor
    if PIPELINE_LOGGER_AVAILABLE:
        try:
            pipeline_logger = get_pipeline_logger()
            if pipeline_logger.file_handle:
                pipeline_logger.log_llm_call(
                    step_name="Agent Executor - Complete Conversation",
                    messages_sent=messages,
                    tool_calls=all_tool_calls if all_tool_calls else None,
                    tool_results=all_tool_results if all_tool_results else None,
                )
        except Exception as log_error:
            logger.debug(f"Failed to log agent executor final messages: {log_error}")


async def execute_tool_calls_batch(
    tool_calls: List[Any],
    structured_tools: List[BaseTool],
    messages: List[Any],
    tool_call_errors: List[Dict[str, Any]],
) -> None:
    """
    Execute a batch of tool calls and add results to messages.
    
    Args:
        tool_calls: List of tool calls from LLM
        structured_tools: List of available tools
        messages: Message history to append tool messages
        tool_call_errors: List to collect tool call errors
    """
    for tool_call in tool_calls:
        tool_name = extract_tool_name(tool_call)
        tool_args, original_args = extract_tool_args(tool_call)
        tool_result, tool_error = await execute_tool_call(tool_call, structured_tools)
        
        # Track tool call errors
        if tool_error:
            # Preserve original args format for error tracking
            if isinstance(original_args, list):
                provided_args_repr = original_args  # Show the list format
            else:
                provided_args_repr = list(tool_args.keys()) if isinstance(tool_args, dict) else []
            
            tool_call_errors.append({
                "tool": tool_name,
                "error": tool_error,
                "provided_args": provided_args_repr,
                "error_message": str(tool_result) if tool_result else tool_error
            })
        
        # ALWAYS add tool message - even on error, so LLM can see the error immediately
        # This allows the LLM to correct its behavior in the same conversation turn
        tool_call_id = extract_tool_call_id(tool_call)
        
        if tool_error:
            # Create detailed error message for the LLM
            error_content = format_tool_error_for_llm(
                tool_name=tool_name,
                error_message=str(tool_result) if tool_result else tool_error,
                provided_args=original_args,
                tool_args=tool_args
            )
            messages.append(ToolMessage(
                content=error_content,
                tool_call_id=tool_call_id or ""
            ))
        elif tool_result is not None:
            # Success case - add normal result
            messages.append(ToolMessage(
                content=str(tool_result),
                tool_call_id=tool_call_id or ""
            ))

