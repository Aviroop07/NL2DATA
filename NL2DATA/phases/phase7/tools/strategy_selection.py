"""Helper functions for tool-based strategy selection in Phase 7."""

from typing import Dict, Any, List, Optional
from langchain_openai import ChatOpenAI
from langchain_core.messages import AIMessage

from NL2DATA.phases.phase7.tools.langchain_tools import get_langchain_tools_for_column
from NL2DATA.phases.phase7.tools.mapping import create_strategy_from_tool_call
from NL2DATA.utils.llm.tool_utils import extract_tool_name, extract_tool_args
from NL2DATA.utils.llm.standardized_calls import StandardizedLLMCall
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


async def select_strategy_via_tool(
    llm: ChatOpenAI,
    attribute_name: str,
    attribute_type: str,
    attribute_description: Optional[str],
    entity_name: str,
    is_categorical: bool = False,
    is_boolean: bool = False,
    system_prompt: str = "",
    human_prompt_template: str = "",
    **kwargs
) -> Dict[str, Any]:
    """
    Select a generation strategy via LLM tool calling.
    
    Uses agent executor to let LLM call tools, then extracts the tool call
    and creates a strategy from it.
    
    Args:
        llm: ChatOpenAI model instance
        attribute_name: Name of the attribute
        attribute_type: SQL type
        attribute_description: Optional description
        entity_name: Entity name
        is_categorical: Whether attribute is categorical
        is_boolean: Whether attribute is boolean
        system_prompt: System prompt for LLM
        human_prompt_template: Human prompt template (with {variables})
        **kwargs: Additional context for prompt
        
    Returns:
        Dictionary with strategy information (compatible with existing Phase 7 output format)
    """
    # Get available tools for this column
    tools = get_langchain_tools_for_column(attribute_type, is_categorical, is_boolean)
    
    if not tools:
        logger.warning(f"No tools available for {entity_name}.{attribute_name} (type: {attribute_type})")
        # Return fallback strategy
        return {
            "tool_name": None,
            "strategy_name": "uniform",
            "kind": "distribution",
            "parameters": {},
            "reasoning": "No tools available, using fallback",
        }
    
    # Format human prompt with attribute context
    # If human_prompt_template contains {placeholders}, format it; otherwise use as-is
    try:
        human_prompt = human_prompt_template.format(
            attribute_name=attribute_name,
            attribute_type=attribute_type,
            attribute_description=attribute_description or "No description",
            entity_name=entity_name,
            **kwargs
        )
    except (KeyError, ValueError):
        # If formatting fails (no placeholders or already formatted), use as-is
        human_prompt = human_prompt_template
    
    # Use agent executor directly to get access to message history
    try:
        from NL2DATA.utils.llm.agent_chain import create_agent_executor_chain
        from NL2DATA.utils.llm.agent_executor import execute_agent_loop
        from NL2DATA.utils.llm.tool_converter import convert_to_structured_tools
        from NL2DATA.utils.llm.prompt_enhancement import enhance_system_prompt
        from langchain_core.messages import SystemMessage, HumanMessage
        
        # Convert tools
        structured_tools = convert_to_structured_tools(tools)
        
        # Enhance system prompt
        enhanced_system_prompt = enhance_system_prompt(system_prompt, tools=structured_tools)
        
        # Bind tools to LLM
        llm_with_tools = llm.bind_tools(structured_tools)
        
        # Create messages list
        messages = [
            SystemMessage(content=enhanced_system_prompt),
            HumanMessage(content=human_prompt),
        ]
        
        # Execute agent loop manually to capture messages
        tool_call_errors = []
        await execute_agent_loop(
            llm_with_tools=llm_with_tools,
            messages=messages,
            structured_tools=structured_tools,
            max_iterations=3,
            tool_call_errors=tool_call_errors,
        )
        
        # Extract tool call from messages
        tool_call_info = extract_tool_call_from_agent_messages(messages)
        
        if not tool_call_info:
            logger.warning("No tool call found in agent executor messages")
            return {
                "tool_name": None,
                "strategy_name": "uniform",
                "kind": "distribution",
                "parameters": {},
                "reasoning": "No tool call found in LLM response",
            }
        
        tool_name = tool_call_info["tool_name"]
        tool_args = tool_call_info["tool_args"]
        
        # Create strategy from tool call
        try:
            strategy = create_strategy_from_tool_call(tool_name, tool_args)
            
            # Convert to dict format compatible with existing Phase 7 output
            return {
                "tool_name": tool_name,
                "strategy_name": strategy.name,
                "kind": strategy.kind,
                "parameters": strategy.model_dump(exclude={"name", "kind", "description"}),
                "reasoning": f"Selected {strategy.name} strategy via tool call",
            }
        except Exception as e:
            logger.error(f"Failed to create strategy from tool call {tool_name}: {e}")
            raise
        
    except Exception as e:
        logger.error(f"Tool-based strategy selection failed: {e}")
        # Return fallback
        return {
            "tool_name": None,
            "strategy_name": "uniform",
            "kind": "distribution",
            "parameters": {},
            "reasoning": f"Tool selection failed: {str(e)}",
        }


def extract_tool_call_from_agent_messages(messages: List[Any]) -> Optional[Dict[str, Any]]:
    """
    Extract the first tool call from agent executor message history.
    
    Args:
        messages: Message history from agent executor
        
    Returns:
        Dictionary with 'tool_name' and 'tool_args', or None if not found
    """
    from langchain_core.messages import AIMessage
    
    for message in messages:
        if isinstance(message, AIMessage):
            tool_calls = getattr(message, 'tool_calls', None) or []
            if tool_calls:
                # Get first tool call
                tool_call = tool_calls[0]
                tool_name = extract_tool_name(tool_call)
                tool_args, _ = extract_tool_args(tool_call)
                return {
                    "tool_name": tool_name,
                    "tool_args": tool_args,
                }
    
    return None

