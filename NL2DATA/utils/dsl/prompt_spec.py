"""Utilities to generate a concise, explicit DSL spec for LLM prompts.

Goal:
The LLM must NOT invent syntax or functions. We provide:
- Allowed operators/keywords
- Allowed function names (strict allowlist)
- Allowed distribution names
- Short syntax guide + examples

This module generates DSL specifications that are automatically kept in sync with
the actual DSL implementation, ensuring LLMs receive accurate and up-to-date
information about allowed syntax and functions.
"""

from __future__ import annotations

from typing import List, Optional

from .function_registry import supported_distribution_signatures, supported_function_signatures
from .grammar_profile import FEATURE_RELATIONAL_CONSTRAINTS


def dsl_prompt_spec_text(
    *,
    include_examples: bool = True,
    detailed: bool = False,
    include_relational_constraints: bool = False,
) -> str:
    """Generate DSL specification text for LLM prompts.
    
    Args:
        include_examples: Whether to include example expressions
        detailed: Whether to include detailed function/distribution information with arity
        include_relational_constraints: Whether to include relational constraint functions
            (EXISTS, LOOKUP, COUNT_WHERE, etc.) - requires profile:v1+relational_constraints
    """
    from .function_registry import get_function_registry, get_distribution_registry
    
    func_reg = get_function_registry()
    dist_reg = get_distribution_registry()
    
    # Filter functions based on features
    if include_relational_constraints:
        func_sigs = supported_function_signatures()
    else:
        # Exclude relational constraint functions if not enabled
        # These functions have the "relational_constraints" feature tag
        func_reg_filtered = {
            name: spec for name, spec in func_reg.items()
            if not (hasattr(spec, 'feature') and spec.feature == FEATURE_RELATIONAL_CONSTRAINTS)
        }
        func_sigs = [func_reg_filtered[name].signature for name in sorted(func_reg_filtered.keys())]
    
    dist_sigs = supported_distribution_signatures()

    # Keep this intentionally concise and copy-pastable.
    parts: List[str] = []
    parts.append("NL2DATA DSL SPEC (STRICT)")
    parts.append("")
    parts.append("Identifiers:")
    parts.append("- Column refs: use bare attribute names like `quantity` (no dots) unless explicitly allowed.")
    parts.append("- Qualified refs: `Table.column` format is allowed for disambiguation.")
    if include_relational_constraints:
        parts.append("- THIS keyword: `THIS.column` refers to the current row's column (in constraint context).")
    parts.append("")
    parts.append("Operators / keywords:")
    parts.append("- Arithmetic: + - * / %")
    parts.append("- Comparisons: = != < > <= >= LIKE IN BETWEEN")
    parts.append("- Null checks: IS NULL, IS NOT NULL")
    parts.append("- Boolean: AND OR NOT")
    parts.append("- Conditionals: IF <expr> THEN <expr> ELSE <expr>; CASE WHEN <expr> THEN <expr> ... ELSE <expr> END")
    parts.append("- Lists: [a, b, c]")
    parts.append("- Distributions: <identifier> ~ <DIST>(args...)")
    if include_relational_constraints:
        parts.append("- Relational: EXISTS(table WHERE predicate), LOOKUP(table, value WHERE predicate), etc.")
    parts.append("")
    
    if detailed:
        # Include detailed function information with arity
        parts.append("Allowed functions (exact allowlist; with arity and types):")
        # Filter functions based on features for detailed mode too
        func_reg_to_show = func_reg if include_relational_constraints else {
            name: spec for name, spec in func_reg.items()
            if not (hasattr(spec, 'feature') and spec.feature == FEATURE_RELATIONAL_CONSTRAINTS)
        }
        for func_name in sorted(func_reg_to_show.keys()):
            func = func_reg_to_show[func_name]
            arity_str = f"{func.arity[0]}" if func.arity[0] == func.arity[1] else f"{func.arity[0]}-{func.arity[1]}"
            desc = f" - {func.description}" if func.description else ""
            parts.append(f"- {func.signature} [arity: {arity_str} args]{desc}")
        parts.append("")
        parts.append("Allowed distributions (exact allowlist; with arity):")
        for dist_name in sorted(dist_reg.keys()):
            dist = dist_reg[dist_name]
            arity_str = f"{dist.arity[0]}" if dist.arity[0] == dist.arity[1] else f"{dist.arity[0]}-{dist.arity[1]}"
            desc = f" - {dist.description}" if dist.description else ""
            parts.append(f"- {dist.signature} [arity: {arity_str} args]{desc}")
    else:
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
        parts.append("- age BETWEEN 18 AND 65")
        parts.append("- email IS NULL")
        parts.append("- x ~ NORMAL(0, 1)")
        parts.append("- status IN ['active', 'paused']")
        parts.append("- name ~ CATEGORICAL(('a', 0.6), ('b', 0.4))")
        if include_relational_constraints:
            parts.append("- EXISTS(Order WHERE Order.customer_id = Customer.customer_id)")
            parts.append("- LOOKUP(Order, Order.total_amount WHERE Order.customer_id = Customer.customer_id)")
            parts.append("- COUNT_WHERE(Order WHERE Order.status = 'active')")
            parts.append("- IN_RANGE(age, 18, 65)")
            parts.append("- THIS.total_amount <= 100")

    parts.append("")
    parts.append("Hard rules:")
    parts.append("- Do NOT output full SQL queries or DDL. This is DSL.")
    parts.append("- Do NOT invent functions or qualify names with dots (except Table.column format).")
    parts.append("- Use only the allowlisted operators/functions/distributions above.")
    parts.append("- Function names must be uppercase (e.g., UPPER, not upper).")
    parts.append("- Distribution names must be uppercase (e.g., NORMAL, not normal).")
    if include_relational_constraints:
        parts.append("- Relational constraint functions require profile:v1+relational_constraints.")
        parts.append("- THIS keyword is only valid in constraint contexts with anchor table.")
    
    parts.append("")
    parts.append("Error handling:")
    parts.append("- All DSL expressions are automatically validated with comprehensive error messages.")
    parts.append("- Errors include type mismatches, unknown identifiers, invalid parameters, and syntax issues.")
    parts.append("- Error messages are automatically generated and include suggestions for fixes.")
    
    return "\n".join(parts)

