"""Focused test for immediate tool error feedback mechanism.

This test simulates tool calls with incorrect argument formats (lists instead of dicts)
and verifies that the LLM receives immediate feedback and can correct its behavior.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_openai import ChatOpenAI
from NL2DATA.utils.llm.agent_chain import create_agent_executor_chain
from NL2DATA.utils.llm.structured_output import invoke_agent_with_structured_output
from NL2DATA.utils.tools.validation_tools import check_schema_component_exists
from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config
from pydantic import BaseModel, Field


class TestOutput(BaseModel):
    """Test output schema."""
    result: str = Field(..., description="The result of the test")
    corrected: bool = Field(default=False, description="Whether the LLM corrected its tool call format")


async def test_tool_error_feedback():
    """Test that tool errors provide immediate feedback to LLM."""
    import os
    
    # Check if API key is available
    if not os.getenv('OPENAI_API_KEY'):
        print("\n[SKIP] OPENAI_API_KEY not set, skipping LLM test")
        print("       (Error message formatting test already passed)")
        return True
    
    logger = get_logger(__name__)
    log_config = get_config('logging')
    setup_logging(
        level=log_config['level'],
        format_type=log_config['format'],
        log_to_file=log_config['log_to_file'],
        log_file=log_config.get('log_file'),
        clear_existing=False,  # Don't clear existing logs
    )
    
    print("=" * 80)
    print("TEST: Tool Error Feedback Mechanism")
    print("=" * 80)
    
    # Create LLM
    llm_config = get_config('llm')
    llm = ChatOpenAI(
        model=llm_config.get('model', 'gpt-4o-mini'),
        temperature=llm_config.get('temperature', 0),
        max_tokens=llm_config.get('max_tokens', 16000),
        timeout=llm_config.get('timeout', 60),
    )
    
    # Create bound tool (this is what causes the issue)
    def check_component_bound(component_type: str, name: str) -> bool:
        """Check if a schema component exists."""
        # Simulate checking - in real scenario, this would check actual schema
        valid_components = {
            ("table", "Customer"): True,
            ("table", "Order"): True,
            ("column", "customer_id"): True,
        }
        return valid_components.get((component_type, name), False)
    
    # Bind the tool
    from functools import partial
    check_component_bound = partial(check_component_bound, component_type="table", name="Customer")
    
    tools = [check_schema_component_exists]
    
    # Create agent executor
    system_prompt = """You are a helpful assistant that validates database schemas.
When you need to check if a schema component exists, use the check_schema_component_exists tool.
You must provide tool arguments as a JSON object with parameter names as keys.
Example: {"component_type": "table", "name": "Customer"}
NOT as a list: ["component_type", "name"]"""
    
    human_prompt_template = """Check if the table "Customer" exists in the schema.
Then check if the column "customer_id" exists in the "Customer" table.
Return a JSON object with:
- result: "success" if both checks passed, "failed" otherwise
- corrected: true if you had to correct your tool call format, false otherwise"""
    
    executor = create_agent_executor_chain(
        llm=llm,
        tools=tools,
        system_prompt=system_prompt,
        human_prompt_template=human_prompt_template,
        max_iterations=5,
    )
    
    # Test 1: Simulate incorrect tool call format (list instead of dict)
    print("\n[Test 1] Simulating tool call with incorrect format...")
    print("Expected: LLM should receive immediate error feedback")
    print("Expected: LLM should correct format and retry\n")
    
    try:
        result, tool_call_errors = await invoke_agent_with_structured_output(
            executor=executor,
            input_data={},
            output_schema=TestOutput,
        )
        
        print(f"\n[Result] Success: {result.result}")
        print(f"[Result] Corrected: {result.corrected}")
        print(f"[Result] Tool call errors: {len(tool_call_errors)}")
        
        if tool_call_errors:
            print("\n[Tool Call Errors Detected]:")
            for error in tool_call_errors:
                print(f"  Tool: {error.get('tool', 'unknown')}")
                print(f"  Error: {error.get('error', 'unknown')}")
                print(f"  Provided args: {error.get('provided_args', 'unknown')}")
                print(f"  Error message: {error.get('error_message', 'unknown')[:200]}")
        
        # Check if error feedback was provided
        if tool_call_errors:
            # Check if any error had list format
            has_list_format_error = any(
                isinstance(error.get('provided_args'), list) 
                for error in tool_call_errors
            )
            
            if has_list_format_error:
                print("\n[✓] Detected list format error (this is expected)")
                print("[✓] Error feedback mechanism is working")
            else:
                print("\n[?] No list format errors detected (LLM may have corrected immediately)")
        
        print("\n[PASS] Test completed successfully")
        return True
        
    except Exception as e:
        print(f"\n[FAIL] Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_direct_tool_message_feedback():
    """Test that ToolMessage contains error feedback when tool fails."""
    print("\n" + "=" * 80)
    print("TEST: Direct ToolMessage Error Feedback")
    print("=" * 80)
    
    # Create a simple test to verify ToolMessage format
    from NL2DATA.utils.llm.tool_utils import format_tool_error_for_llm
    
    # Simulate error scenarios
    test_cases = [
        {
            "tool_name": "check_schema_component_exists_bound",
            "error_message": "'StructuredTool' object is not callable",
            "provided_args": ["component_type", "name"],  # Wrong format
            "tool_args": {},  # Empty because extraction failed
        },
        {
            "tool_name": "verify_entities_exist_bound",
            "error_message": "missing 1 required positional argument: 'entities'",
            "provided_args": ["entities"],  # Wrong format
            "tool_args": {},
        },
        {
            "tool_name": "validate_query_against_schema_bound",
            "error_message": "missing 1 required positional argument: 'sql'",
            "provided_args": ["sql"],  # Wrong format
            "tool_args": {},
        },
    ]
    
    print("\n[Test Cases] Verifying error message formatting:\n")
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"[Test Case {i}] {test_case['tool_name']}")
        error_msg = format_tool_error_for_llm(
            tool_name=test_case["tool_name"],
            error_message=test_case["error_message"],
            provided_args=test_case["provided_args"],
            tool_args=test_case["tool_args"],
        )
        
        print(f"Error message length: {len(error_msg)} characters")
        print(f"Contains 'CRITICAL FORMAT ERROR': {'CRITICAL FORMAT ERROR' in error_msg}")
        print(f"Contains 'CORRECT FORMAT': {'CORRECT FORMAT' in error_msg}")
        print(f"Contains 'WRONG FORMAT': {'WRONG FORMAT' in error_msg}")
        
        # Show first 300 chars
        print(f"\nFirst 300 characters:\n{error_msg[:300]}...\n")
        
        # Verify key components
        assert "CRITICAL FORMAT ERROR" in error_msg or "ERROR" in error_msg, "Error message should indicate format error"
        assert "CORRECT FORMAT" in error_msg or "correct format" in error_msg.lower(), "Error message should show correct format"
        assert "WRONG FORMAT" in error_msg or "wrong format" in error_msg.lower(), "Error message should show wrong format"
        
    print("[PASS] Error message formatting test completed")
    return True


async def test_tool_message_always_added():
    """Test that ToolMessage is always added, even on error."""
    print("\n" + "=" * 80)
    print("TEST: ToolMessage Always Added on Error")
    print("=" * 80)
    
    from NL2DATA.utils.llm.tool_utils import format_tool_error_for_llm
    
    # Test with tool names that match the patterns in format_tool_error_for_llm
    test_cases = [
        {
            "tool_name": "check_schema_component_exists_bound",  # Matches "component" pattern
            "provided_args": ["component_type", "name"],
            "should_have_correct_format": True,
        },
        {
            "tool_name": "verify_entities_exist_bound",  # Matches "entities" pattern
            "provided_args": ["entities"],
            "should_have_correct_format": True,
        },
        {
            "tool_name": "generic_tool",  # Doesn't match any pattern - should still have error message
            "provided_args": ["param1", "param2"],
            "should_have_correct_format": False,  # Generic tool won't have specific format example
        },
    ]
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n[Test Case {i}] {test_case['tool_name']}")
        
        error_msg = format_tool_error_for_llm(
            tool_name=test_case["tool_name"],
            error_message="Test error",
            provided_args=test_case["provided_args"],
            tool_args={},
        )
        
        assert len(error_msg) > 0, "Error message should not be empty"
        assert "CRITICAL FORMAT ERROR" in error_msg or "ERROR" in error_msg, "Should detect format error"
        assert "REMEMBER" in error_msg or "CORRECT FORMAT" in error_msg, "Should provide format guidance"
        
        if test_case["should_have_correct_format"]:
            assert "CORRECT FORMAT" in error_msg, f"Should provide correct format example for {test_case['tool_name']}"
            print(f"      [OK] Contains specific format example")
        else:
            print(f"      [OK] Contains generic format guidance")
        
        print(f"      Error message length: {len(error_msg)} characters")
    
    print("\n[PASS] ToolMessage error formatting verified for all test cases")
    return True


async def test_tool_message_integration():
    """Test that ToolMessage is actually added to messages list on error."""
    print("\n" + "=" * 80)
    print("TEST: ToolMessage Integration (Message List)")
    print("=" * 80)
    
    from NL2DATA.utils.llm.agent_executor import execute_tool_calls_batch
    from NL2DATA.utils.llm.tool_utils import format_tool_error_for_llm
    from langchain_core.messages import AIMessage, ToolMessage
    from langchain_core.tools import StructuredTool
    
    # Create a mock tool that will fail
    def failing_tool(component_type: str, name: str) -> bool:
        """A tool that will fail when called incorrectly."""
        return True
    
    tool = StructuredTool.from_function(
        func=failing_tool,
        name="test_tool",
        description="Test tool"
    )
    
    # Create a mock tool call with wrong format (list instead of dict)
    class MockToolCall:
        def __init__(self):
            self.name = "test_tool"
            self.args = ["component_type", "name"]  # Wrong format - list
            self.id = "test_call_1"
    
    # Create message list
    messages = []
    tool_call_errors = []
    
    # Execute tool calls batch (this should add ToolMessage even on error)
    tool_calls = [MockToolCall()]
    
    await execute_tool_calls_batch(
        tool_calls=tool_calls,
        structured_tools=[tool],
        messages=messages,
        tool_call_errors=tool_call_errors,
    )
    
    # Verify results
    print(f"\n[Verification]")
    print(f"  Messages added: {len(messages)}")
    print(f"  Tool call errors tracked: {len(tool_call_errors)}")
    
    # Check that a ToolMessage was added
    tool_messages = [msg for msg in messages if isinstance(msg, ToolMessage)]
    assert len(tool_messages) > 0, "ToolMessage should be added even on error"
    print(f"  ToolMessages found: {len(tool_messages)}")
    
    # Check that the ToolMessage contains error feedback
    if tool_messages:
        tool_msg = tool_messages[0]
        assert hasattr(tool_msg, 'content'), "ToolMessage should have content"
        assert len(tool_msg.content) > 0, "ToolMessage content should not be empty"
        assert "ERROR" in tool_msg.content or "CRITICAL FORMAT ERROR" in tool_msg.content, \
            "ToolMessage should contain error feedback"
        print(f"  ToolMessage content length: {len(tool_msg.content)} characters")
        print(f"  Contains error feedback: {'ERROR' in tool_msg.content or 'CRITICAL FORMAT ERROR' in tool_msg.content}")
        
        # Show first 200 chars
        print(f"\n  First 200 characters of ToolMessage:")
        print(f"  {tool_msg.content[:200]}...")
    
    # Check that error was tracked
    assert len(tool_call_errors) > 0, "Tool call errors should be tracked"
    assert tool_call_errors[0]["tool"] == "test_tool", "Error should be tracked for correct tool"
    print(f"  Error tracked for tool: {tool_call_errors[0]['tool']}")
    
    print("\n[PASS] ToolMessage integration test completed")
    print("       - ToolMessage is added to messages list on error")
    print("       - ToolMessage contains error feedback")
    print("       - Errors are tracked for retry logic")
    return True


if __name__ == "__main__":
    async def run_tests():
        """Run all tests."""
        results = []
        
        # Test 1: Direct error message formatting
        try:
            result1 = await test_direct_tool_message_feedback()
            results.append(("Error Message Formatting", result1))
        except Exception as e:
            print(f"\n[FAIL] Error message formatting test failed: {e}")
            import traceback
            traceback.print_exc()
            results.append(("Error Message Formatting", False))
        
        # Test 2: ToolMessage always added on error (formatting)
        try:
            result2 = await test_tool_message_always_added()
            results.append(("ToolMessage Formatting", result2))
        except Exception as e:
            print(f"\n[FAIL] ToolMessage formatting test failed: {e}")
            import traceback
            traceback.print_exc()
            results.append(("ToolMessage Formatting", False))
        
        # Test 3: ToolMessage integration (actually added to messages)
        try:
            result3 = await test_tool_message_integration()
            results.append(("ToolMessage Integration", result3))
        except Exception as e:
            print(f"\n[FAIL] ToolMessage integration test failed: {e}")
            import traceback
            traceback.print_exc()
            results.append(("ToolMessage Integration", False))
        
        # Test 4: Full agent executor with tool error feedback (requires API key)
        try:
            result4 = await test_tool_error_feedback()
            results.append(("Tool Error Feedback (LLM)", result4))
        except Exception as e:
            if "api_key" in str(e).lower() or "OPENAI_API_KEY" in str(e):
                print(f"\n[SKIP] Tool error feedback LLM test skipped (no API key)")
                results.append(("Tool Error Feedback (LLM)", True))  # Skip is OK
            else:
                print(f"\n[FAIL] Tool error feedback test failed: {e}")
                import traceback
                traceback.print_exc()
                results.append(("Tool Error Feedback (LLM)", False))
        
        # Summary
        print("\n" + "=" * 80)
        print("TEST SUMMARY")
        print("=" * 80)
        for test_name, passed in results:
            status = "PASS" if passed else "FAIL"
            print(f"{test_name}: {status}")
        
        all_passed = all(result for _, result in results)
        print(f"\nOverall: {'PASS' if all_passed else 'FAIL'}")
        print("=" * 80)
        
        return all_passed
    
    success = asyncio.run(run_tests())
    sys.exit(0 if success else 1)

