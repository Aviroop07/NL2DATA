"""Utilities to generate a concise, explicit DSL spec for LLM prompts.

Goal:
The LLM must NOT invent syntax or functions. We provide:
- Allowed operators/keywords
- Allowed function names (strict allowlist)
- Allowed distribution names
- Short syntax guide + examples
"""

from __future__ import annotations

from typing import List

from .function_registry import supported_distribution_signatures, supported_function_signatures


def dsl_prompt_spec_text(*, include_examples: bool = True) -> str:
    func_sigs = supported_function_signatures()
    dist_sigs = supported_distribution_signatures()

    # Keep this intentionally concise and copy-pastable.
    parts: List[str] = []
    parts.append("NL2DATA DSL SPEC (STRICT)")
    parts.append("")
    parts.append("Identifiers:")
    parts.append("- Column refs: use bare attribute names like `quantity` (no dots) unless explicitly allowed.")
    parts.append("")
    parts.append("Operators / keywords:")
    parts.append("- Arithmetic: + - * / %")
    parts.append("- Comparisons: = != < > <= >= LIKE IN")
    parts.append("- Boolean: AND OR NOT")
    parts.append("- Conditionals: IF <expr> THEN <expr> ELSE <expr>; CASE WHEN <expr> THEN <expr> ... ELSE <expr> END")
    parts.append("- Lists: [a, b, c]")
    parts.append("- Distributions: <identifier> ~ <DIST>(args...)")
    parts.append("")
    parts.append("Allowed functions (exact allowlist; with signatures):")
    parts.extend([f"- {s}" for s in func_sigs])
    parts.append("")
    parts.append("Allowed distributions (exact allowlist; with signatures):")
    parts.extend([f"- {s}" for s in dist_sigs])

    if include_examples:
        parts.append("")
        parts.append("Examples:")
        parts.append("- quantity * unit_price")
        parts.append("- CONCAT(first_name, ' ', last_name)")
        parts.append("- IF age >= 18 AND is_active = true THEN 1 ELSE 0")
        parts.append("- x ~ NORMAL(0, 1)")
        parts.append("- status IN ['active', 'paused']")
        parts.append("- name ~ CATEGORICAL(('a', 0.6), ('b', 0.4))")

    parts.append("")
    parts.append("Hard rules:")
    parts.append("- Do NOT output full SQL queries or DDL. This is DSL.")
    parts.append("- Do NOT invent functions or qualify names with dots.")
    parts.append("- Use only the allowlisted operators/functions/distributions above.")
    return "\n".join(parts)

