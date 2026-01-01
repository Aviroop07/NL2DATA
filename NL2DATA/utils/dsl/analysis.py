"""Deterministic helpers to analyze DSL expressions (no LLM).

Used for:
- Dependency extraction
- Detecting aggregate metric expressions
"""

from __future__ import annotations

from typing import Any, Set

from NL2DATA.utils.dsl.parser import parse_dsl_expression


AGGREGATE_FUNCTIONS: Set[str] = {"COUNT", "SUM", "AVG", "MIN", "MAX"}


def dsl_identifiers_used(expr: str) -> Set[str]:
    """Return identifier strings used as column refs in a DSL expression (best-effort).

    We exclude function/distribution names by ignoring the callee identifier of func_call/dist_call.
    Returned identifiers can include dotted identifiers if the expression uses them.
    """
    try:
        tree = parse_dsl_expression(expr)
    except Exception:
        return set()

    used: Set[str] = set()

    def ident_str(ident_node: Any) -> str:
        parts = []
        for c in getattr(ident_node, "children", []) or []:
            parts.append(str(getattr(c, "value", "")))
        return ".".join([p for p in parts if p])

    def walk(node: Any) -> None:
        if getattr(node, "data", None) == "func_call":
            for i, ch in enumerate(getattr(node, "children", []) or []):
                if i == 0 and getattr(ch, "data", None) == "identifier":
                    continue
                walk(ch)
            return
        if getattr(node, "data", None) == "dist_call":
            for i, ch in enumerate(getattr(node, "children", []) or []):
                if i == 0 and getattr(ch, "data", None) == "identifier":
                    continue
                walk(ch)
            return
        if getattr(node, "data", None) == "identifier":
            s = ident_str(node)
            if s:
                used.add(s)
            return

        for ch in getattr(node, "children", []) or []:
            walk(ch)

    walk(tree)
    return used


def dsl_contains_aggregate(expr: str) -> bool:
    """Return True if expression uses aggregate functions (COUNT/SUM/AVG/MIN/MAX)."""
    try:
        t = parse_dsl_expression(expr)
    except Exception:
        return False

    def ident_str(ident_node: Any) -> str:
        parts = []
        for c in getattr(ident_node, "children", []) or []:
            parts.append(str(getattr(c, "value", "")))
        return ".".join([p for p in parts if p])

    for fn_call in t.find_data("func_call"):
        ident_node = fn_call.children[0] if getattr(fn_call, "children", None) else None
        if getattr(ident_node, "data", None) == "identifier":
            name = ident_str(ident_node).upper()
            if name in AGGREGATE_FUNCTIONS:
                return True
    return False

