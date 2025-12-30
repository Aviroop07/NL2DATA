"""Extract tool results from agent executor messages for use in decoupled JSON generation.

This module provides utilities to extract tool call results from agent executor
message history, so they can be used as context for a separate LLM call that
generates the structured JSON output.
"""

from typing import Any, Dict, List, Optional
from langchain_core.messages import AIMessage, ToolMessage

from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


def extract_tool_results(messages: List[Any]) -> Dict[str, Any]:
    """
    Extract tool call results from agent executor message history.
    
    This function collects all tool calls and their results from the message history,
    formatting them for use as context in a second LLM call that generates JSON.
    
    Args:
        messages: Message history from agent executor
        
    Returns:
        Dictionary with:
        - tool_calls: List of tool calls made (name, args, result)
        - tool_results_summary: Human-readable summary of tool results
        - tool_call_errors: List of any tool call errors
    """
    tool_calls = []
    tool_results_summary = []
    tool_call_errors = []
    
    # Track tool calls and their results
    tool_call_map: Dict[str, Dict[str, Any]] = {}
    
    for message in messages:
        if isinstance(message, AIMessage):
            # Extract tool calls from AIMessage
            msg_tool_calls = getattr(message, 'tool_calls', None) or []
            for tool_call in msg_tool_calls:
                # Handle both dict and object formats
                if isinstance(tool_call, dict):
                    tool_call_id = tool_call.get('id') or tool_call.get('tool_call_id') or ""
                    tool_name = tool_call.get('name', '')
                    tool_args = tool_call.get('args', {})
                else:
                    tool_call_id = getattr(tool_call, 'id', None) or getattr(tool_call, 'tool_call_id', None) or ""
                    tool_name = getattr(tool_call, 'name', '') or (tool_call.get('name') if hasattr(tool_call, 'get') else '')
                    tool_args = getattr(tool_call, 'args', {}) or (tool_call.get('args', {}) if hasattr(tool_call, 'get') else {})
                
                if tool_call_id:
                    tool_call_map[tool_call_id] = {
                        'name': tool_name,
                        'args': tool_args,
                        'result': None,
                        'error': None,
                    }
        
        elif isinstance(message, ToolMessage):
            # Extract tool result from ToolMessage
            tool_call_id = getattr(message, 'tool_call_id', None) or ""
            content = getattr(message, 'content', '') or ""
            
            if tool_call_id in tool_call_map:
                # Check if this is an error message
                if 'ERROR' in content or 'error' in content.lower() or 'failed' in content.lower():
                    tool_call_map[tool_call_id]['error'] = content
                    tool_call_errors.append({
                        'tool': tool_call_map[tool_call_id]['name'],
                        'error': content,
                        'args': tool_call_map[tool_call_id]['args'],
                    })
                else:
                    tool_call_map[tool_call_id]['result'] = content
    
    # Build summary
    for tool_call_id, tool_info in tool_call_map.items():
        tool_calls.append({
            'name': tool_info['name'],
            'args': tool_info['args'],
            'result': tool_info['result'],
            'error': tool_info['error'],
        })
        
        if tool_info['error']:
            tool_results_summary.append(
                f"Tool '{tool_info['name']}' call failed: {tool_info['error']}"
            )
        elif tool_info['result']:
            tool_results_summary.append(
                f"Tool '{tool_info['name']}' returned: {tool_info['result']}"
            )
    
    return {
        'tool_calls': tool_calls,
        'tool_results_summary': '\n'.join(tool_results_summary) if tool_results_summary else 'No tool calls made.',
        'tool_call_errors': tool_call_errors,
    }


def format_tool_results_for_prompt(tool_results: Dict[str, Any]) -> str:
    """
    Format tool results as a human-readable string for inclusion in LLM prompt.
    
    Args:
        tool_results: Dictionary from extract_tool_results()
        
    Returns:
        Formatted string describing tool calls and results
    """
    if not tool_results.get('tool_calls'):
        return "No tools were called."
    
    lines = ["Tool Call Results:"]
    lines.append("=" * 50)
    
    for i, tool_call in enumerate(tool_results['tool_calls'], 1):
        lines.append(f"\n{i}. Tool: {tool_call['name']}")
        if tool_call['args']:
            args_str = ', '.join(f"{k}={v}" for k, v in tool_call['args'].items())
            lines.append(f"   Arguments: {args_str}")
        
        if tool_call['error']:
            lines.append(f"   Status: ERROR")
            lines.append(f"   Error: {tool_call['error']}")
        elif tool_call['result']:
            lines.append(f"   Status: SUCCESS")
            lines.append(f"   Result: {tool_call['result']}")
        else:
            lines.append(f"   Status: UNKNOWN")
    
    if tool_results.get('tool_call_errors'):
        lines.append("\n" + "=" * 50)
        lines.append("Tool Call Errors:")
        for error in tool_results['tool_call_errors']:
            lines.append(f"  - {error['tool']}: {error['error']}")
    
    return '\n'.join(lines)

