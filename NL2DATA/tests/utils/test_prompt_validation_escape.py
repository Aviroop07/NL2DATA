"""Deterministic tests for prompt validation / brace escaping.

These tests ensure that brace-heavy prompts (especially those with JSON examples)
do not crash LangChain prompt template construction with errors like:
  ValueError: Single '}' encountered in format string
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from langchain_core.prompts import ChatPromptTemplate, SystemMessagePromptTemplate, HumanMessagePromptTemplate

from NL2DATA.utils.llm.prompt_validation import safe_create_prompt_template


def test_safe_create_prompt_template_escapes_json_but_preserves_variables():
    # This mimics the kind of system prompt that caused Step 3.2 crashes:
    # - a JSON code block example (lots of braces)
    # - double-brace text already escaped
    # - human template variables that must stay as placeholders
    system_prompt = """You are a validator.

Example output:
```json
{
  "ok": true,
  "missing": [
    {"entity": "Order", "attribute": "status"}
  ]
}
```

Also: list of {{entity, attribute, reasoning}} should stay escaped.
"""

    human_prompt_template = """Check this:
{context}

Description:
{nl_description}
"""

    fixed_system, fixed_human = safe_create_prompt_template(
        system_prompt=system_prompt,
        human_prompt_template=human_prompt_template,
        expected_variables=["context", "nl_description"],
        auto_fix=True,
    )

    # Placeholders must remain placeholders
    assert "{context}" in fixed_human
    assert "{nl_description}" in fixed_human

    # Ensure JSON example braces are escaped (we should see doubled braces somewhere)
    assert "{{" in fixed_system and "}}" in fixed_system

    # Most important: LangChain template construction must not raise
    prompt = ChatPromptTemplate.from_messages([
        SystemMessagePromptTemplate.from_template(fixed_system),
        HumanMessagePromptTemplate.from_template(fixed_human),
    ])
    rendered = prompt.format_messages(context="CTX", nl_description="DESC")
    assert len(rendered) == 2


def test_safe_create_prompt_template_handles_nested_json_objects():
    system_prompt = """Example:
```json
{
  "outer": {
    "inner": {"k": "v"}
  }
}
```"""

    fixed_system, _ = safe_create_prompt_template(
        system_prompt=system_prompt,
        human_prompt_template="{context}",
        expected_variables=["context"],
        auto_fix=True,
    )

    # Should not contain any single unescaped braces that could break formatting
    # (heuristic check: after escaping, every raw '{' should be part of '{{' or a placeholder)
    # We'll validate by attempting to create and format the prompt.
    prompt = ChatPromptTemplate.from_messages([
        SystemMessagePromptTemplate.from_template(fixed_system),
        HumanMessagePromptTemplate.from_template("{context}"),
    ])
    _ = prompt.format_messages(context="CTX")


