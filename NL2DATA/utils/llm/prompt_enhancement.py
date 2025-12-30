"""System prompt enhancement for agent executors.

Handles enhancement of system prompts with tool-specific instructions
and structured output requirements.
"""

from typing import Any, List, Optional

from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.llm.prompt_validation import escape_json_in_prompt

logger = get_logger(__name__)


def enhance_system_prompt(system_prompt: str, tools: Optional[List[Any]] = None) -> str:
    """Enhance system prompt with structured output requirements.
    
    Note: The returned prompt is escaped to prevent LangChain from interpreting
    JSON examples as template variables. The system prompt should not contain
    any template variables - it's a literal string.
    """
    
    # Build tool-specific examples if tools are provided
    tool_examples = ""
    if tools:
        tool_examples = "\n\nCRITICAL TOOL CALL FORMAT:\n"
        tool_examples += "When calling ANY tool, you MUST provide arguments as a JSON object (dictionary), NOT as a list or string.\n\n"
        tool_examples += "CORRECT FORMAT (what you MUST do):\n"
        tool_examples += "- Tool with 'entities' parameter: {{\"entities\": [\"Customer\", \"Order\"]}}\n"
        tool_examples += "- Tool with 'sql' parameter: {{\"sql\": \"SELECT * FROM Customer;\"}}\n"
        tool_examples += "- Tool with 'component_type' and 'name': {{\"component_type\": \"table\", \"name\": \"Customer\"}}\n\n"
        tool_examples += "WRONG FORMAT (what you MUST NOT do):\n"
        tool_examples += "- ❌ [\"entities\"] - This is a list, NOT a dictionary\n"
        tool_examples += "- ❌ [] - Empty list, NOT a dictionary\n"
        tool_examples += "- ❌ [\"component_type\", \"name\"] - List of parameter names, NOT a dictionary\n"
        tool_examples += "- ❌ \"entities\" - String, NOT a dictionary\n\n"
        tool_examples += "REMEMBER: Tool arguments MUST be a JSON object with key-value pairs where:\n"
        tool_examples += "- Keys are the parameter names (exactly as specified in the tool description)\n"
        tool_examples += "- Values are the actual argument values\n"
    
    enhanced_prompt = f"""{system_prompt}

CRITICAL OUTPUT REQUIREMENTS:
1. You have access to validation tools that you can use to check your work
2. **When calling tools**: Provide ALL required arguments as a JSON object (dictionary) with the exact parameter names
   - Example: If tool requires "entities", call with {{"entities": ["Entity1", "Entity2"]}}
   - Example: If tool requires "sql", call with {{"sql": "SELECT * FROM table;"}}
   - Example: If tool requires "component_type" and "name", call with {{"component_type": "table", "name": "Customer"}}
   - DO NOT provide arguments as a list: ["entities"] ❌
   - DO NOT provide arguments as an empty list: [] ❌
   - DO NOT provide arguments as a list of parameter names: ["component_type", "name"] ❌
   - DO NOT omit required parameters or use incorrect parameter names
   - ALWAYS use a dictionary/object format: {{"parameter_name": value}} ✅
{tool_examples}
3. **CRITICAL: When you see a tool error message**:
   - The error message is FEEDBACK to help you correct your tool call
   - You MUST retry the tool call with the CORRECT format shown in the error message
   - DO NOT return the error message as your final answer
   - DO NOT stop after seeing an error - continue using tools until you get valid results
   - If a tool fails, read the error message carefully, fix the format, and call the tool again
   - Example: If you see "Args provided: ['component_type', 'name']" and "CORRECT FORMAT: {{'component_type': 'table', 'name': 'Customer'}}", 
     then immediately retry the tool call using the correct format: {{"component_type": "table", "name": "Customer"}}
4. After using tools (if needed), you MUST return your final answer as a valid JSON object
5. The JSON must match the required output schema exactly
6. Do NOT return tool calls as your final answer
7. Do NOT return error messages as your final answer - errors are feedback, not answers
8. Do NOT return markdown formatting (no ```json``` blocks, no markdown headers, no bullet points)
9. Do NOT return explanatory text before or after the JSON
10. Return ONLY the raw JSON object, nothing else
11. The JSON should start with {{ and end with }}
12. Extract the final answer from tool results and format it as JSON

EXAMPLE OF CORRECT OUTPUT:
{{"field1": "value1", "field2": ["item1", "item2"], "field3": "value3"}}

EXAMPLE OF INCORRECT OUTPUT (DO NOT DO THIS):
```json
{{"field1": "value1"}}
```

ERROR: Tool 'some_tool' failed to execute.
(Do NOT return error messages - retry the tool call instead!)

Based on the analysis, here is the result:
{{"field1": "value1"}}

Remember: 
- Return ONLY the JSON object, nothing else
- If a tool fails, retry it with the correct format shown in the error message
- Never return error messages as your final answer"""
    
    # Escape all JSON examples to prevent LangChain from interpreting them as template variables
    # The system prompt should be a literal string with no template variables
    # Note: Some braces are already escaped ({{ and }}), but we need to escape any remaining single braces
    escaped_prompt = escape_json_in_prompt(enhanced_prompt, allowed_variables=[])
    
    return escaped_prompt
