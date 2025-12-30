"""Message extraction utilities.

Handles extraction of final answers from message history,
skipping tool messages and error messages.
"""

from typing import Any, Dict, List
from langchain_core.messages import AIMessage, ToolMessage

from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


def extract_final_answer(messages: List[Any]) -> Dict[str, Any]:
    """
    Extract final answer from message history.
    
    Looks for the last AIMessage (not ToolMessage) that doesn't have tool_calls,
    as that indicates the LLM has finished using tools and is providing the final answer.
    
    If the last message is a ToolMessage (error), we look for the previous AIMessage
    that might contain the final answer, even if it has tool_calls (the LLM might have
    provided both tool calls and a final answer in the same message).
    """
    if not messages:
        # No messages - this will trigger NoneOutputError in structured_output.py
        logger.warning("No messages found in message history")
        return {"output": ""}
    
    # Look backwards through messages to find the last AIMessage without tool_calls
    # This is the final answer, not a tool call or tool result
    final_answer_message = None
    for message in reversed(messages):
        # Skip ToolMessages - these are tool results/errors, not final answers
        if isinstance(message, ToolMessage):
            continue
        
        # Look for AIMessage without tool_calls (final answer)
        if isinstance(message, AIMessage):
            tool_calls = getattr(message, 'tool_calls', None) or []
            if not tool_calls:
                # This is the final answer - no more tool calls
                final_answer_message = message
                break
    
    # If no final AIMessage without tool_calls found, check if last AIMessage has content
    # even if it has tool_calls (some LLMs provide both)
    if final_answer_message is None:
        for message in reversed(messages):
            if isinstance(message, AIMessage):
                # Check if this message has content (even if it also has tool_calls)
                content = getattr(message, 'content', None)
                if content and str(content).strip():
                    final_answer_message = message
                    break
    
    # If still no AIMessage found, use the last non-ToolMessage
    if final_answer_message is None:
        for message in reversed(messages):
            if not isinstance(message, ToolMessage):
                final_answer_message = message
                break
    
    # If still no message found, use the last message
    if final_answer_message is None:
        final_answer_message = messages[-1]
    
    # Extract content
    if hasattr(final_answer_message, 'content'):
        content = final_answer_message.content
        # Check if content is None or empty
        if content is None:
            logger.warning("Final answer message has None content")
            return {"output": ""}
        if isinstance(content, str) and not content.strip():
            logger.warning("Final answer message has empty content")
            return {"output": ""}
    else:
        content_str = str(final_answer_message)
        if not content_str or content_str == "None" or not content_str.strip():
            logger.warning(f"Final answer message has no valid content: {content_str[:100]}")
            return {"output": ""}
        content = content_str
    
    # Check if content is an error message - if so, log warning
    if isinstance(content, str) and (
        content.startswith("ERROR: Tool") or 
        "CRITICAL FORMAT ERROR" in content or
        "failed to execute" in content.lower()
    ):
        logger.warning(
            f"Final answer appears to be an error message. "
            f"This suggests the LLM returned an error instead of retrying the tool call. "
            f"Content preview: {content[:200]}..."
        )
    
    return {"output": content}

