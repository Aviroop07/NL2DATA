"""Deterministically seed functional dependencies from derived attributes.

Goal:
- Before running any FD mining (LLM-driven Step 4.1), we can already assert some FDs:
  if a derived attribute is computed from dependencies, then dependencies -> derived_attr.

This is deterministic and fast, and it reduces later noise.
"""

from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple


def _dedup_preserve_order(items: List[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for x in items:
        if not x or not isinstance(x, str):
            continue
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def seed_functional_dependencies_from_derived_formulas(
    derived_formula_results: Dict[str, Dict[str, Dict[str, Any]]],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Build FD seeds from Step 2.9 outputs.

    Expected structure (from Step 2.9):
    derived_formula_results[entity_name][derived_attr] = {
        "formula": "...",
        "expression_type": "...",
        "dependencies": ["attr_a", "attr_b", ...],
        "reasoning": "..."
    }

    Returns:
        entity_name -> list of FDs, each FD is:
        {"lhs": [...], "rhs": [...], "reasoning": "...", "source": "derived_attribute"}
    """
    out: Dict[str, List[Dict[str, Any]]] = {}

    for entity_name, derived_map in (derived_formula_results or {}).items():
        if not isinstance(derived_map, dict):
            continue

        # Merge-by-LHS so we don't create many tiny FDs with identical determinants.
        # key: tuple(lhs_sorted) -> set(rhs)
        buckets: Dict[Tuple[str, ...], Dict[str, Any]] = {}

        for derived_attr, formula_info in derived_map.items():
            if not isinstance(formula_info, dict):
                continue
            deps = formula_info.get("dependencies", []) or []
            if not isinstance(deps, list):
                deps = []

            lhs = _dedup_preserve_order([d for d in deps if isinstance(d, str)])
            rhs = _dedup_preserve_order([derived_attr] if isinstance(derived_attr, str) else [])

            # If we cannot assert any determinants, don't invent an FD.
            if not lhs or not rhs:
                continue

            lhs_key = tuple(sorted(lhs))
            b = buckets.get(lhs_key)
            if b is None:
                b = {
                    "lhs": list(lhs_key),
                    "rhs_set": set(),
                    "reasoning_parts": [],
                }
                buckets[lhs_key] = b

            b["rhs_set"].update(rhs)
            r = (formula_info.get("reasoning") or "").strip()
            if r:
                b["reasoning_parts"].append(r)

        fds: List[Dict[str, Any]] = []
        for b in buckets.values():
            rhs_list = sorted([a for a in b["rhs_set"] if isinstance(a, str) and a])
            if not rhs_list:
                continue
            fds.append(
                {
                    "lhs": b["lhs"],
                    "rhs": rhs_list,
                    "reasoning": " | ".join((b.get("reasoning_parts") or [])[:3])
                    or "Derived attributes are functionally determined by their dependencies.",
                    "source": "derived_attribute",
                }
            )

        if fds:
            out[entity_name] = fds

    return out

