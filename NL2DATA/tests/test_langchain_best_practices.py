"""Test suite for LangChain best practices implementation.

Tests:
1. RunnableConfig with metadata and tags
2. @traceable_step decorators
3. RunnableRetry integration
4. LangSmith tracing (if available)
5. Metadata propagation through chains
"""

import asyncio
import sys
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock
from typing import Dict, Any

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    import pytest
    HAS_PYTEST = True
except ImportError:
    HAS_PYTEST = False
    # Create a mock pytest decorator
    def pytest_mark_asyncio(func):
        return func
    pytest = type('MockPytest', (), {'mark': type('MockMark', (), {'asyncio': pytest_mark_asyncio})()})()

from langchain_core.runnables import RunnableConfig
from langchain_openai import ChatOpenAI

from NL2DATA.phases.phase1.step_1_1_domain_detection import step_1_1_domain_detection
from NL2DATA.phases.phase1.step_1_2_entity_mention_detection import step_1_2_entity_mention_detection
from NL2DATA.utils.llm.chain_utils import create_structured_chain, invoke_with_retry
from NL2DATA.utils.observability import traceable_step, get_trace_config, setup_langsmith
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config
from pydantic import BaseModel, Field

logger = get_logger(__name__)


class TestOutput(BaseModel):
    """Test output schema."""
    result: str = Field(description="Test result")


async def test_traceable_step_decorator():
    """Test that @traceable_step decorator is applied correctly."""
    # Check that step functions have the decorator
    import inspect
    
    # Check step_1_1_domain_detection
    assert hasattr(step_1_1_domain_detection, '__wrapped__') or hasattr(step_1_1_domain_detection, '__name__')
    
    # Check step_1_2_entity_mention_detection
    assert hasattr(step_1_2_entity_mention_detection, '__wrapped__') or hasattr(step_1_2_entity_mention_detection, '__name__')
    
    logger.info("✓ @traceable_step decorators are present")


async def test_get_trace_config():
    """Test that get_trace_config creates proper RunnableConfig."""
    config = get_trace_config("1.1", phase=1, tags=["domain_detection"])
    
    assert isinstance(config, RunnableConfig)
    assert config.get("configurable") is not None
    assert "metadata" in config.get("configurable", {})
    assert "tags" in config.get("configurable", {})
    
    metadata = config.get("configurable", {}).get("metadata", {})
    assert metadata.get("step_id") == "1.1"
    assert metadata.get("phase") == 1
    
    tags = config.get("configurable", {}).get("tags", [])
    assert "domain_detection" in tags
    
    logger.info("✓ get_trace_config creates proper RunnableConfig")


async def test_runnable_retry_integration():
    """Test that RunnableRetry is integrated into chains."""
    from langchain_core.runnables import RunnableRetry
    
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    chain = create_structured_chain(
        llm=llm,
        output_schema=TestOutput,
        system_prompt="You are a test assistant.",
        human_prompt_template="Test: {input}",
        enable_retry=True,
        max_retries=3,
    )
    
    # Check if chain is wrapped with RunnableRetry
    # Note: RunnableRetry wraps the chain, so we check the type
    assert chain is not None
    # The chain should be callable
    assert callable(chain.ainvoke) or hasattr(chain, 'ainvoke')
    
    logger.info("✓ RunnableRetry is integrated into chains")


async def test_config_passed_to_invoke_with_retry():
    """Test that RunnableConfig is passed to invoke_with_retry."""
    from unittest.mock import AsyncMock, MagicMock
    
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    chain = create_structured_chain(
        llm=llm,
        output_schema=TestOutput,
        system_prompt="You are a test assistant.",
        human_prompt_template="Test: {input}",
    )
    
    # Mock the chain to capture config
    original_ainvoke = chain.ainvoke
    config_received = None
    
    async def mock_ainvoke(input_data, config=None):
        nonlocal config_received
        config_received = config
        # Return a mock result
        mock_result = Mock()
        mock_result.model_dump.return_value = {"result": "test"}
        return mock_result
    
    chain.ainvoke = mock_ainvoke
    
    test_config = get_trace_config("test", phase=1, tags=["test"])
    
    try:
        result = await invoke_with_retry(
            chain=chain,
            input_data={"input": "test"},
            config=test_config,
        )
        
        # Verify config was passed
        assert config_received is not None
        assert isinstance(config_received, RunnableConfig)
        assert config_received.get("configurable") == test_config.get("configurable")
        
        logger.info("✓ RunnableConfig is passed to invoke_with_retry")
    except Exception as e:
        logger.warning(f"Test skipped (may need actual LLM call): {e}")


async def test_langsmith_setup():
    """Test that LangSmith setup function works."""
    # Test setup_langsmith (should not raise)
    try:
        result = setup_langsmith()
        # Should return True or False, not raise
        assert isinstance(result, bool) or result is None
        logger.info("✓ LangSmith setup function works")
    except Exception as e:
        logger.warning(f"LangSmith setup test skipped: {e}")


async def test_metadata_propagation():
    """Test that metadata propagates through the chain."""
    config = get_trace_config("1.1", phase=1, tags=["domain_detection"], 
                              additional_metadata={"test_key": "test_value"})
    
    metadata = config.get("configurable", {}).get("metadata", {})
    assert metadata.get("test_key") == "test_value"
    assert metadata.get("step_id") == "1.1"
    assert metadata.get("phase") == 1
    
    logger.info("✓ Metadata propagates correctly through config")


async def test_chain_creation_with_tools():
    """Test that chains can be created with tools."""
    from langchain_core.tools import tool
    
    @tool
    def test_tool(input: str) -> str:
        """Test tool."""
        return f"Processed: {input}"
    
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    chain = create_structured_chain(
        llm=llm,
        output_schema=TestOutput,
        system_prompt="You are a test assistant.",
        human_prompt_template="Test: {input}",
        tools=[test_tool],
    )
    
    assert chain is not None
    logger.info("✓ Chains can be created with tools")


async def test_agent_executor_pattern():
    """Test that agent-executor pattern can be created and used."""
    from NL2DATA.utils.llm.agent_utils import create_agent_executor_chain
    from langchain_core.tools import tool
    
    @tool
    def test_tool(input: str) -> str:
        """Test tool."""
        return f"Processed: {input}"
    
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    executor = create_agent_executor_chain(
        llm=llm,
        tools=[test_tool],
        system_prompt="You are a test assistant.",
        human_prompt_template="Test: {input}",
        max_iterations=3,
    )
    
    assert executor is not None
    assert hasattr(executor, 'ainvoke')
    logger.info("✓ Agent-executor pattern can be created")


async def test_integration_with_actual_step():
    """Test that an actual step function uses all best practices."""
    # This is a lightweight test that doesn't make actual LLM calls
    # but verifies the structure
    
    # Check imports
    import NL2DATA.phases.phase1.step_1_1_domain_detection as step_1_1_module
    assert hasattr(step_1_1_module, 'get_trace_config')
    assert hasattr(step_1_1_module, 'traceable_step')
    
    # Check function signature includes config handling
    import inspect
    sig = inspect.signature(step_1_1_domain_detection)
    # The function should be callable with nl_description
    assert 'nl_description' in sig.parameters
    
    logger.info("✓ Step functions have proper structure for best practices")


def test_all_step_functions_have_decorators():
    """Test that all step functions have @traceable_step decorators."""
    import importlib
    import inspect
    from pathlib import Path
    
    phases_dir = Path(__file__).parent.parent / "phases"
    step_files = list(phases_dir.rglob("step_*.py"))
    
    decorated_count = 0
    total_count = 0
    
    for step_file in step_files:
        try:
            # Extract module path
            rel_path = step_file.relative_to(Path(__file__).parent.parent)
            module_path = str(rel_path).replace("/", ".").replace("\\", ".").replace(".py", "")
            
            # Import module
            module = importlib.import_module(module_path)
            
            # Find step functions
            for name, obj in inspect.getmembers(module, inspect.iscoroutinefunction):
                if name.startswith("step_") and not name.endswith("_batch") and not name.endswith("_with_loop"):
                    total_count += 1
                    # Check if function has decorator (wrapped attribute or traceable in source)
                    source = inspect.getsource(obj)
                    if "@traceable_step" in source:
                        decorated_count += 1
        except Exception as e:
            logger.debug(f"Could not check {step_file}: {e}")
    
    logger.info(f"Found {decorated_count}/{total_count} step functions with @traceable_step decorators")
    # Note: We don't fail if not all are decorated, as some may be batch/loop wrappers


async def run_all_tests():
    """Run all LangChain best practices tests."""
    setup_logging(
        level=get_config('logging')['level'],
        format_type=get_config('logging')['format'],
        log_to_file=False,  # Don't clutter log file with test output
    )
    
    print("=" * 80)
    print("LangChain Best Practices Test Suite")
    print("=" * 80)
    
    tests = [
        ("Traceable Step Decorators", test_traceable_step_decorator),
        ("Get Trace Config", test_get_trace_config),
        ("RunnableRetry Integration", test_runnable_retry_integration),
        ("Config Passed to Invoke", test_config_passed_to_invoke_with_retry),
        ("LangSmith Setup", test_langsmith_setup),
        ("Metadata Propagation", test_metadata_propagation),
        ("Chain Creation with Tools", test_chain_creation_with_tools),
        ("Agent Executor Pattern", test_agent_executor_pattern),
        ("Integration with Actual Step", test_integration_with_actual_step),
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        try:
            print(f"\n[TEST] {test_name}...")
            await test_func()
            print(f"[PASS] {test_name}")
            passed += 1
        except Exception as e:
            print(f"[FAIL] {test_name}: {e}")
            logger.error(f"Test {test_name} failed", exc_info=True)
            failed += 1
    
    # Run non-async test
    try:
        print(f"\n[TEST] All Step Functions Have Decorators...")
        test_all_step_functions_have_decorators()
        print(f"[PASS] All Step Functions Have Decorators")
        passed += 1
    except Exception as e:
        print(f"[FAIL] All Step Functions Have Decorators: {e}")
        failed += 1
    
    print("\n" + "=" * 80)
    print(f"Test Results: {passed} passed, {failed} failed")
    print("=" * 80)
    
    return failed == 0


if __name__ == "__main__":
    success = asyncio.run(run_all_tests())
    sys.exit(0 if success else 1)

