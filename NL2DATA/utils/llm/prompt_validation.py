"""Utility functions for validating and fixing prompt templates to prevent LangChain errors."""

import re
from typing import List, Tuple, Optional, Set
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


def validate_prompt_template(template: str, expected_variables: List[str]) -> Tuple[bool, List[str], List[str]]:
    """
    Validate a prompt template to ensure all required variables are present and no unexpected variables exist.
    
    Args:
        template: The prompt template string
        expected_variables: List of expected variable names (e.g., ["entity_name", "context"])
        
    Returns:
        Tuple of (is_valid, missing_variables, unexpected_variables)
    """
    # Find all template variables (e.g., {variable_name})
    # Pattern matches {variable_name} but not {{escaped}}
    pattern = r'(?<!\{)\{([^}]+)\}(?!\})'
    found_variables = set(re.findall(pattern, template))
    
    expected_set = set(expected_variables)
    missing = list(expected_set - found_variables)
    unexpected = list(found_variables - expected_set)
    
    is_valid = len(missing) == 0 and len(unexpected) == 0
    
    return is_valid, missing, unexpected


_TEMPLATE_VAR_PATTERN = re.compile(r'(?<!\{)\{([^}]+)\}(?!\})')


def _extract_template_variables(template: str) -> Set[str]:
    """Extract LangChain-style template variables (e.g., {context}) from a prompt string."""
    found = set(_TEMPLATE_VAR_PATTERN.findall(template))
    # Filter out clearly-non-variable patterns (e.g., whitespace, punctuation-heavy)
    # Keep only simple python identifier-like names.
    return {v for v in found if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", v)}


def escape_json_in_prompt(text: str, allowed_variables: Optional[List[str]] = None) -> str:
    """
    Escape curly braces in prompt text to prevent LangChain from interpreting JSON/examples
    as template variables.

    LangChain's prompt templating treats `{var}` as a placeholder. Real prompts often contain
    JSON examples or other brace-heavy content; any unmatched `{` / `}` can crash with
    `ValueError: Single '}' encountered in format string`.

    Strategy:
    - Preserve existing escaped braces `{{` and `}}`
    - Preserve placeholders for allowed variables (e.g., `{context}`, `{nl_description}`)
    - Escape all other single braces by doubling them.
    
    Args:
        text: Prompt text that may contain JSON or other brace-heavy examples
        allowed_variables: Variables that should remain as `{var}` placeholders.
                          If None, variables are auto-detected from the template itself.
        
    Returns:
        Text with safe escaping applied.
    """
    allowed: Set[str]
    if allowed_variables is None:
        allowed = _extract_template_variables(text)
    else:
        allowed = {v for v in allowed_variables if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", v)}

    out: List[str] = []
    i = 0
    n = len(text)
    while i < n:
        # Preserve already escaped braces
        if i + 1 < n and text[i:i + 2] == "{{":
            out.append("{{")
            i += 2
            continue
        if i + 1 < n and text[i:i + 2] == "}}":
            out.append("}}")
            i += 2
            continue

        ch = text[i]
        if ch == "{":
            # Try to preserve a valid placeholder {var}
            close = text.find("}", i + 1)
            if close != -1:
                candidate = text[i + 1:close]
                if candidate in allowed:
                    out.append("{")
                    out.append(candidate)
                    out.append("}")
                    i = close + 1
                    continue
            # Otherwise escape single '{'
            out.append("{{")
            i += 1
            continue
        if ch == "}":
            # Escape single '}'
            out.append("}}")
            i += 1
            continue

        out.append(ch)
        i += 1

    return "".join(out)


def check_and_fix_prompt_template(
    template: str,
    expected_variables: List[str],
    auto_fix: bool = False
) -> Tuple[str, bool, List[str], List[str]]:
    """
    Check a prompt template for issues and optionally fix them.
    
    Args:
        template: The prompt template string
        expected_variables: List of expected variable names
        auto_fix: If True, attempt to escape JSON examples automatically
        
    Returns:
        Tuple of (fixed_template, is_valid, missing_variables, unexpected_variables)
    """
    # First, check for missing/extra variables
    is_valid, missing, unexpected = validate_prompt_template(template, expected_variables)
    
    fixed_template = template
    
    if auto_fix:
        # Try to escape brace-heavy examples while preserving placeholders
        # If expected_variables is empty, auto-detect placeholders from template.
        fixed_template = escape_json_in_prompt(
            template,
            allowed_variables=expected_variables if expected_variables else None,
        )
        # Re-validate after fixing
        is_valid_after_fix, missing_after, unexpected_after = validate_prompt_template(
            fixed_template, expected_variables
        )
        if is_valid_after_fix:
            logger.debug("Auto-fixed prompt template by escaping JSON examples")
            return fixed_template, True, [], []
    
    return fixed_template, is_valid, missing, unexpected


def safe_create_prompt_template(
    system_prompt: str,
    human_prompt_template: str,
    expected_variables: List[str],
    auto_fix: bool = True
) -> Tuple[str, str]:
    """
    Safely create prompt templates with validation and auto-fixing.
    
    Args:
        system_prompt: System message content
        human_prompt_template: Human message template
        expected_variables: List of expected variable names in human_prompt_template
        auto_fix: If True, automatically escape JSON examples
        
    Returns:
        Tuple of (fixed_system_prompt, fixed_human_prompt_template)
        
    Raises:
        ValueError: If template validation fails and auto_fix is False
    """
    # Check and fix system prompt (may contain JSON examples)
    fixed_system, sys_valid, sys_missing, sys_unexpected = check_and_fix_prompt_template(
        system_prompt, [], auto_fix=auto_fix
    )
    
    # Check and fix human prompt template
    fixed_human, human_valid, human_missing, human_unexpected = check_and_fix_prompt_template(
        human_prompt_template, expected_variables, auto_fix=auto_fix
    )
    
    if not human_valid and not auto_fix:
        error_msg = (
            f"Prompt template validation failed:\n"
            f"  Missing variables: {human_missing}\n"
            f"  Unexpected variables: {human_unexpected}\n"
            f"  Expected variables: {expected_variables}"
        )
        raise ValueError(error_msg)
    
    if human_missing or human_unexpected:
        logger.warning(
            f"Prompt template has issues (auto-fix enabled):\n"
            f"  Missing variables: {human_missing}\n"
            f"  Unexpected variables: {human_unexpected}"
        )
    
    return fixed_system, fixed_human

