"""Test script for tool result extraction utilities."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.utils.llm.tool_result_extraction import extract_tool_results, format_tool_results_for_prompt
from langchain_core.messages import AIMessage, ToolMessage

async def test_extract_tool_results():
    """Test extracting tool results from messages."""
    print("=" * 80)
    print("Testing Tool Result Extraction")
    print("=" * 80)
    
    all_passed = True
    
    try:
        # Test Case 1: Simple tool call with success
        print("\n" + "-" * 80)
        print("Test Case 1: Simple tool call with success")
        print("-" * 80)
        
        # Create AIMessage with tool_calls (LangChain format)
        # LangChain AIMessage expects tool_calls to be a list of dicts with specific structure
        ai_msg = AIMessage(
            content="",
            tool_calls=[{
                "id": "call_123",
                "name": "check_entity_exists",
                "args": {"entity": "Customer"},
                "type": "function"
            }]
        )
        messages = [
            ai_msg,
            ToolMessage(content="True", tool_call_id="call_123")
        ]
        
        result = extract_tool_results(messages)
        
        assert len(result['tool_calls']) == 1, f"Expected 1 tool call, got {len(result['tool_calls'])}"
        assert result['tool_calls'][0]['name'] == "check_entity_exists", "Tool name mismatch"
        assert result['tool_calls'][0]['result'] == "True", "Tool result mismatch"
        assert result['tool_calls'][0]['error'] is None, "Should have no error"
        assert len(result['tool_call_errors']) == 0, "Should have no errors"
        
        print("[PASS] Tool result extraction works correctly")
        
        # Test Case 2: Tool call with error
        print("\n" + "-" * 80)
        print("Test Case 2: Tool call with error")
        print("-" * 80)
        
        ai_msg2 = AIMessage(content="")
        ai_msg2.tool_calls = [{
            "id": "call_456",
            "name": "validate_syntax",
            "args": {"code": "invalid"}
        }]
        messages = [
            ai_msg2,
            ToolMessage(content="ERROR: Invalid syntax", tool_call_id="call_456")
        ]
        
        result = extract_tool_results(messages)
        
        assert len(result['tool_calls']) == 1, f"Expected 1 tool call, got {len(result['tool_calls'])}"
        assert result['tool_calls'][0]['error'] is not None, "Should have error"
        assert len(result['tool_call_errors']) == 1, "Should have 1 error"
        
        print("[PASS] Tool error extraction works correctly")
        
        # Test Case 3: Multiple tool calls
        print("\n" + "-" * 80)
        print("Test Case 3: Multiple tool calls")
        print("-" * 80)
        
        ai_msg3 = AIMessage(content="")
        ai_msg3.tool_calls = [
            {"id": "call_1", "name": "tool1", "args": {"arg1": "value1"}},
            {"id": "call_2", "name": "tool2", "args": {"arg2": "value2"}}
        ]
        messages = [
            ai_msg3,
            ToolMessage(content="result1", tool_call_id="call_1"),
            ToolMessage(content="result2", tool_call_id="call_2")
        ]
        
        result = extract_tool_results(messages)
        
        assert len(result['tool_calls']) == 2, f"Expected 2 tool calls, got {len(result['tool_calls'])}"
        assert result['tool_calls'][0]['name'] == "tool1", "First tool name mismatch"
        assert result['tool_calls'][1]['name'] == "tool2", "Second tool name mismatch"
        
        print("[PASS] Multiple tool calls extraction works correctly")
        
        # Test Case 4: Format tool results for prompt
        print("\n" + "-" * 80)
        print("Test Case 4: Format tool results for prompt")
        print("-" * 80)
        
        tool_results = {
            'tool_calls': [
                {'name': 'check_entity', 'args': {'entity': 'Customer'}, 'result': 'True', 'error': None},
                {'name': 'validate', 'args': {'code': 'test'}, 'result': None, 'error': 'ERROR: Invalid'}
            ],
            'tool_results_summary': 'Tool check_entity returned: True\nTool validate call failed: ERROR: Invalid',
            'tool_call_errors': [{'tool': 'validate', 'error': 'ERROR: Invalid', 'args': {'code': 'test'}}]
        }
        
        formatted = format_tool_results_for_prompt(tool_results)
        
        assert "Tool Call Results:" in formatted, "Should contain header"
        assert "check_entity" in formatted, "Should contain tool name"
        assert "SUCCESS" in formatted, "Should contain success status"
        assert "ERROR" in formatted, "Should contain error status"
        
        print("[PASS] Tool results formatting works correctly")
        print("\nFormatted output preview:")
        print(formatted[:200] + "..." if len(formatted) > 200 else formatted)
        
        # Test Case 5: No tool calls
        print("\n" + "-" * 80)
        print("Test Case 5: No tool calls")
        print("-" * 80)
        
        messages = [AIMessage(content="No tools needed")]
        result = extract_tool_results(messages)
        
        assert len(result['tool_calls']) == 0, "Should have no tool calls"
        assert "No tool calls made" in result['tool_results_summary'], "Should indicate no tool calls"
        
        formatted = format_tool_results_for_prompt(result)
        assert "No tools were called" in formatted, "Should indicate no tools"
        
        print("[PASS] No tool calls handling works correctly")
        
        print("\n" + "=" * 80)
        if all_passed:
            print("[PASS] All tool result extraction tests passed!")
        else:
            print("[ERROR] Some tests failed")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    
    return all_passed

if __name__ == "__main__":
    success = asyncio.run(test_extract_tool_results())
    sys.exit(0 if success else 1)

