"""Unit tests for agent-executor pattern utilities."""

import asyncio
import sys
from pathlib import Path
from unittest.mock import Mock, AsyncMock, patch
from typing import Dict, Any

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pydantic import BaseModel, Field
from langchain_openai import ChatOpenAI
from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.tools import tool

from NL2DATA.utils.llm.agent_utils import (
    create_agent_executor_chain,
    invoke_agent_with_structured_output,
    invoke_agent_with_retry,
)
from NL2DATA.phases.phase1.model_router import get_model_for_step
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config


class TestOutput(BaseModel):
    """Test output schema."""
    result: str = Field(description="Test result")
    value: int = Field(description="Test value")


@tool
def mock_validation_tool(input_str: str) -> str:
    """Mock validation tool for testing."""
    return f"Validated: {input_str}"


@tool
def mock_check_tool(entity: str) -> bool:
    """Mock check tool for testing."""
    return entity.lower() in ["customer", "order", "product"]


async def test_create_agent_executor_chain():
    """Test that agent executor chain can be created."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=False,
    )
    
    print("=" * 60)
    print("Testing create_agent_executor_chain")
    print("=" * 60)
    
    try:
        llm = get_model_for_step("1.10")  # Use model router which handles API key
        
        executor = create_agent_executor_chain(
            llm=llm,
            tools=[mock_validation_tool],
            system_prompt="You are a test assistant.",
            human_prompt_template="Process: {input}",
            max_iterations=3,
        )
        
        assert executor is not None
        assert hasattr(executor, 'ainvoke')
        print("[PASS] Agent executor chain created successfully")
        return True
        
    except Exception as e:
        print(f"[FAIL] Failed to create agent executor chain: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_agent_executor_with_tool_calls():
    """Test that agent executor handles tool calls correctly."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=False,
    )
    
    print("=" * 60)
    print("Testing agent executor with tool calls")
    print("=" * 60)
    
    try:
        llm = get_model_for_step("1.10")  # Use model router which handles API key
        
        executor = create_agent_executor_chain(
            llm=llm,
            tools=[mock_validation_tool, mock_check_tool],
            system_prompt="""You are a test assistant. 
            You have access to validation tools. Use them to validate your input.
            After using tools, return your final answer as JSON: {{"result": "...", "value": 42}}""",
            human_prompt_template="Validate and check: {input}",
            max_iterations=5,
        )
        
        # Test invocation
        result = await executor.ainvoke({"input": "Customer"})
        
        assert result is not None
        assert "output" in result
        print(f"[PASS] Agent executor invoked successfully")
        print(f"  Output: {result.get('output', '')[:200]}...")
        return True
        
    except Exception as e:
        print(f"[FAIL] Agent executor invocation failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_invoke_agent_with_structured_output():
    """Test structured output extraction from agent executor."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=False,
    )
    
    print("=" * 60)
    print("Testing invoke_agent_with_structured_output")
    print("=" * 60)
    
    try:
        llm = get_model_for_step("1.10")  # Use model router which handles API key
        
        executor = create_agent_executor_chain(
            llm=llm,
            tools=[mock_validation_tool],
            system_prompt="""You are a test assistant. 
            Return your final answer as JSON matching this schema:
            {{"result": "string", "value": integer}}""",
            human_prompt_template="Process: {input}",
            max_iterations=3,
        )
        
        # Test structured output extraction
        result, tool_call_errors = await invoke_agent_with_structured_output(
            executor=executor,
            input_data={"input": "test"},
            output_schema=TestOutput,
        )
        
        assert isinstance(result, TestOutput)
        assert isinstance(tool_call_errors, list)
        assert hasattr(result, 'result')
        assert hasattr(result, 'value')
        print(f"[PASS] Structured output extracted successfully")
        print(f"  Result: {result.result}")
        print(f"  Value: {result.value}")
        return True
        
    except Exception as e:
        print(f"[FAIL] Structured output extraction failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_invoke_agent_with_retry():
    """Test retry logic for agent executor."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=False,
    )
    
    print("=" * 60)
    print("Testing invoke_agent_with_retry")
    print("=" * 60)
    
    try:
        llm = get_model_for_step("1.10")  # Use model router which handles API key
        
        executor = create_agent_executor_chain(
            llm=llm,
            tools=[mock_validation_tool],
            system_prompt="""You are a test assistant. 
            Return your final answer as JSON: {{"result": "...", "value": 42}}""",
            human_prompt_template="Process: {input}",
            max_iterations=3,
        )
        
        # Test retry logic
        result = await invoke_agent_with_retry(
            executor=executor,
            input_data={"input": "test"},
            output_schema=TestOutput,
            max_retries=2,
        )
        
        assert isinstance(result, TestOutput)
        print(f"[PASS] Retry logic works correctly")
        return True
        
    except Exception as e:
        print(f"[FAIL] Retry logic test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_multiple_tools_sequential():
    """Test that agent executor can use multiple tools sequentially."""
    logger = get_logger(__name__)
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=False,
    )
    
    print("=" * 60)
    print("Testing multiple tools sequential execution")
    print("=" * 60)
    
    try:
        llm = get_model_for_step("1.10")  # Use model router which handles API key
        
        executor = create_agent_executor_chain(
            llm=llm,
            tools=[mock_validation_tool, mock_check_tool],
            system_prompt="""You are a test assistant. 
            You have two tools: mock_validation_tool and mock_check_tool.
            Use both tools to validate and check the input.
            Return your final answer as JSON: {{"result": "...", "value": 42}}""",
            human_prompt_template="Validate and check: {input}",
            max_iterations=5,
        )
        
        result = await executor.ainvoke({"input": "Customer"})
        
        assert result is not None
        assert "output" in result
        print(f"[PASS] Multiple tools executed successfully")
        print(f"  Output: {result.get('output', '')[:200]}...")
        return True
        
    except Exception as e:
        print(f"[FAIL] Multiple tools test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def run_all_tests():
    """Run all agent executor tests."""
    print("\n" + "=" * 60)
    print("Running Agent Executor Unit Tests")
    print("=" * 60 + "\n")
    
    tests = [
        ("Create Agent Executor Chain", test_create_agent_executor_chain),
        ("Agent Executor with Tool Calls", test_agent_executor_with_tool_calls),
        ("Structured Output Extraction", test_invoke_agent_with_structured_output),
        ("Retry Logic", test_invoke_agent_with_retry),
        ("Multiple Tools Sequential", test_multiple_tools_sequential),
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{'=' * 60}")
        print(f"Test: {test_name}")
        print(f"{'=' * 60}")
        try:
            result = await test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"[ERROR] Test {test_name} raised exception: {e}")
            import traceback
            traceback.print_exc()
            results.append((test_name, False))
    
    print("\n" + "=" * 60)
    print("Test Results Summary")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "PASS" if result else "FAIL"
        print(f"  {status}: {test_name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    print("=" * 60)
    
    return passed == total


if __name__ == "__main__":
    success = asyncio.run(run_all_tests())
    sys.exit(0 if success else 1)

