"""Utility functions for creating LangChain agent executors with structured output.

Agent-executor pattern handles tool calls automatically in a multi-turn loop,
making it more robust than direct tool binding for complex validation workflows.

This module provides a unified interface that delegates to specialized modules:
- tool_converter: Tool conversion logic
- agent_chain: Agent chain creation and execution
- structured_output: Structured output parsing and retry logic
"""

# Import from refactored modules
from NL2DATA.utils.llm.agent_chain import create_agent_executor_chain
from NL2DATA.utils.llm.structured_output import (
    invoke_agent_with_structured_output,
    invoke_agent_with_retry,
)

# Re-export for backward compatibility
__all__ = [
    "create_agent_executor_chain",
    "invoke_agent_with_structured_output",
    "invoke_agent_with_retry",
]
