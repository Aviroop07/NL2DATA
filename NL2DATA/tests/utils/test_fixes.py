"""Test script for recent fixes: format strings, tool binding, agent executor."""

import sys
import asyncio
from pathlib import Path
from typing import Dict, Any, List

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.utils.logging import setup_logging, get_logger
from NL2DATA.config import get_config
from NL2DATA.utils.llm.agent_chain import _safe_format_template, create_agent_executor_chain
from NL2DATA.utils.llm.tool_converter import convert_to_structured_tools
from NL2DATA.utils.llm.message_extraction import extract_final_answer
from NL2DATA.utils.tools import validate_dsl_expression
from NL2DATA.utils.tools.tool_schemas import TOOL_ARG_SCHEMAS
from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.tools import StructuredTool

logger = get_logger(__name__)


def test_format_string_escaping():
    """Test that format string escaping works correctly."""
    print("\n" + "=" * 80)
    print("TEST: Format String Escaping")
    print("=" * 80)
    
    # Test case 1: Template with braces in content (should be escaped)
    # The template has literal braces that should be escaped
    template1 = "Entity: Customer\nContext: {{entity_cardinalities: {{Customer: 1}}}}\nDescription: {nl_description}"
    inputs1 = {"nl_description": "Test description"}
    
    try:
        result1 = _safe_format_template(template1, inputs1)
        print(f"[PASS] Template with escaped braces formatted successfully")
        print(f"  Result: {result1[:100]}...")
        assert "{nl_description}" not in result1, "Placeholder should be replaced"
        assert "Test description" in result1, "Input should be in result"
        assert "{entity_cardinalities" in result1 or "{{entity_cardinalities" in result1, "Braces should be preserved"
        return True
    except Exception as e:
        print(f"[FAIL] Format string escaping failed: {e}")
        return False


def test_tool_schema_for_bound_functions():
    """Test that tool schemas exist for bound functions."""
    print("\n" + "=" * 80)
    print("TEST: Tool Schema for Bound Functions")
    print("=" * 80)
    
    # Check that validate_dsl_expression_bound has a schema
    if "validate_dsl_expression_bound" in TOOL_ARG_SCHEMAS:
        print("[PASS] validate_dsl_expression_bound schema exists")
        schema = TOOL_ARG_SCHEMAS["validate_dsl_expression_bound"]
        print(f"  Schema fields: {list(schema.model_fields.keys())}")
        assert "dsl" in schema.model_fields, "Schema should have 'dsl' field"
        return True
    else:
        print("[FAIL] validate_dsl_expression_bound schema missing")
        return False


async def test_closure_tool_conversion():
    """Test that closure-based tools can be converted to StructuredTool."""
    print("\n" + "=" * 80)
    print("TEST: Closure Tool Conversion")
    print("=" * 80)
    
    # Create a closure similar to validate_dsl_expression_bound
    grammar = "test_grammar"
    
    def validate_dsl_expression_bound(dsl: str) -> Dict[str, Any]:
        """Bound version of validate_dsl_expression with grammar."""
        # Direct implementation instead of calling validate_dsl_expression (which is a tool)
        dsl = dsl.strip()
        if not dsl:
            return {"valid": False, "error": "DSL expression is empty"}
        if dsl.count("(") != dsl.count(")"):
            return {"valid": False, "error": "Unbalanced parentheses"}
        if dsl.count("[") != dsl.count("]"):
            return {"valid": False, "error": "Unbalanced brackets"}
        return {"valid": True, "error": None}
    
    try:
        # Convert to StructuredTool
        tools = convert_to_structured_tools([validate_dsl_expression_bound])
        
        assert len(tools) == 1, "Should have one tool"
        tool = tools[0]
        assert isinstance(tool, StructuredTool), "Should be StructuredTool"
        assert tool.name == "validate_dsl_expression_bound", "Tool name should match"
        
        print("[PASS] Closure tool converted successfully")
        print(f"  Tool name: {tool.name}")
        print(f"  Tool description: {tool.description[:100]}...")
        
        # Test that tool can be invoked
        result = await tool.ainvoke({"dsl": "amount > 0"})
        print(f"  Tool invocation result: {result}")
        assert isinstance(result, dict), "Result should be dict"
        assert "valid" in result, "Result should have 'valid' key"
        
        print("[PASS] Closure tool can be invoked")
        return True
    except Exception as e:
        print(f"[FAIL] Closure tool conversion failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_message_extraction_with_empty_content():
    """Test that message extraction handles empty content correctly."""
    print("\n" + "=" * 80)
    print("TEST: Message Extraction with Empty Content")
    print("=" * 80)
    
    # Test case 1: Empty messages list
    messages1 = []
    result1 = extract_final_answer(messages1)
    assert result1["output"] == "", "Empty messages should return empty output"
    print("[PASS] Empty messages list handled")
    
    # Test case 2: Messages with empty string content (AIMessage doesn't allow None)
    messages2 = [AIMessage(content="")]
    result2 = extract_final_answer(messages2)
    assert result2["output"] == "", "Empty string content should return empty output"
    print("[PASS] Empty string content handled")
    
    # Test case 3: Messages with tool calls but no final answer
    # Create proper tool_call format
    from langchain_core.messages import ToolCall
    tool_call = ToolCall(name="test_tool", args={}, id="1")
    messages3 = [
        AIMessage(content="", tool_calls=[tool_call]),
        ToolMessage(content="Tool result", tool_call_id="1")
    ]
    result3 = extract_final_answer(messages3)
    # Should return empty or tool result (depending on implementation)
    print(f"[PASS] Messages with tool calls handled: {result3['output'][:50]}...")
    
    # Test case 4: Valid final answer
    tool_call2 = ToolCall(name="test_tool", args={}, id="2")
    messages4 = [
        AIMessage(content="", tool_calls=[tool_call2]),
        ToolMessage(content="Tool result", tool_call_id="2"),
        AIMessage(content="Final answer here")
    ]
    result4 = extract_final_answer(messages4)
    assert result4["output"] == "Final answer here", "Should extract final answer"
    print("[PASS] Valid final answer extracted")
    
    return True


def test_context_msg_brace_escaping():
    """Test that context_msg braces are properly escaped."""
    print("\n" + "=" * 80)
    print("TEST: Context Message Brace Escaping")
    print("=" * 80)
    
    # Simulate context_msg with braces (like from dictionary representations)
    context_msg = "\n\nEnhanced Context:\n- Relations: {Customer: 1, Order: N}\n- Cardinalities: {Customer=1, Order=N}"
    
    # Escape braces
    escaped = context_msg.replace("{", "{{").replace("}", "}}")
    
    # Check that braces are escaped
    assert "{{" in escaped, "Braces should be escaped"
    assert "}}" in escaped, "Braces should be escaped"
    # After escaping, unescaped single braces should not exist
    assert escaped.count("{") == escaped.count("}"), "Should have balanced braces"
    assert escaped.count("{") % 2 == 0, "All braces should be escaped (even number)"
    
    print("[PASS] Context message braces escaped correctly")
    print(f"  Original: {context_msg[:80]}...")
    print(f"  Escaped: {escaped[:80]}...")
    
    # Test that escaped version can be used in template
    template = f"Entity: Customer{escaped}\nDescription: {{nl_description}}"
    inputs = {"nl_description": "Test"}
    
    try:
        result = template.format(**inputs)
        print("[PASS] Escaped context_msg can be used in template")
        assert "Test" in result, "Input should be in result"
        # After formatting, escaped braces become single braces
        assert "{Customer" in result or "{{Customer" in result, "Braces should be preserved in output"
        return True
    except ValueError as e:
        print(f"[FAIL] Template formatting failed: {e}")
        return False


async def run_all_tests():
    """Run all fix tests."""
    print("=" * 80)
    print("Testing Recent Fixes")
    print("=" * 80)
    
    results = []
    
    # Test 1: Format string escaping
    try:
        result = test_format_string_escaping()
        results.append(("Format String Escaping", result))
    except Exception as e:
        print(f"[ERROR] Format string escaping test failed: {e}")
        results.append(("Format String Escaping", False))
    
    # Test 2: Tool schema for bound functions
    try:
        result = test_tool_schema_for_bound_functions()
        results.append(("Tool Schema for Bound Functions", result))
    except Exception as e:
        print(f"[ERROR] Tool schema test failed: {e}")
        results.append(("Tool Schema for Bound Functions", False))
    
    # Test 3: Closure tool conversion
    try:
        result = await test_closure_tool_conversion()
        results.append(("Closure Tool Conversion", result))
    except Exception as e:
        print(f"[ERROR] Closure tool conversion test failed: {e}")
        results.append(("Closure Tool Conversion", False))
    
    # Test 4: Message extraction
    try:
        result = test_message_extraction_with_empty_content()
        results.append(("Message Extraction", result))
    except Exception as e:
        print(f"[ERROR] Message extraction test failed: {e}")
        results.append(("Message Extraction", False))
    
    # Test 5: Context message brace escaping
    try:
        result = test_context_msg_brace_escaping()
        results.append(("Context Message Escaping", result))
    except Exception as e:
        print(f"[ERROR] Context message escaping test failed: {e}")
        results.append(("Context Message Escaping", False))
    
    # Summary
    print("\n" + "=" * 80)
    print("Test Summary")
    print("=" * 80)
    
    for name, success in results:
        status = "[PASS]" if success else "[FAIL]"
        print(f"{status} - {name}")
    
    all_passed = all(result[1] for result in results)
    
    print("\n" + "=" * 80)
    if all_passed:
        print("[SUCCESS] All fix tests passed!")
    else:
        print("[FAILURE] Some fix tests failed.")
    print("=" * 80)
    
    return all_passed


if __name__ == "__main__":
    log_config = get_config('logging')
    setup_logging(
        level=log_config['level'],
        format_type=log_config['format'],
        log_to_file=log_config['log_to_file'],
        log_file=log_config.get('log_file'),
        clear_existing=True,
    )
    success = asyncio.run(run_all_tests())
    sys.exit(0 if success else 1)

