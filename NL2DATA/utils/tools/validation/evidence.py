"""Evidence and constraint validation tools.

These tools are intended for LLM self-validation (tool calling) to reduce hallucinations
and enforce strict output constraints.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from langchain_core.tools import tool


def _verify_evidence_substring_impl(evidence: str, nl_description: str) -> Dict[str, Any]:
    """Pure implementation of verify_evidence_substring (safe to call from Python steps)."""
    ev = evidence or ""
    desc = nl_description or ""

    if not ev.strip():
        return {"is_substring": False, "error": "evidence is empty/blank"}
    if not desc:
        return {"is_substring": False, "error": "nl_description is empty"}

    if ev in desc:
        return {"is_substring": True, "error": None}

    return {
        "is_substring": False,
        "error": "evidence is not a substring of nl_description (case-sensitive match failed)",
    }


@tool
def verify_evidence_substring(evidence: str, nl_description: str) -> Dict[str, Any]:
    """Verify that evidence is an exact substring of the provided natural language description.

    Args:
        evidence: Evidence snippet to verify (case-sensitive)
        nl_description: Full natural-language description

    Returns:
        {"is_substring": bool, "error": str | None}
    """
    return _verify_evidence_substring_impl(evidence=evidence, nl_description=nl_description)


@tool
def verify_entity_in_known_entities(entity: str, known_entities: List[str]) -> Dict[str, Any]:
    """Verify that an entity name exists in a provided list of known entity names.

    Comparison is case-insensitive but returns only a boolean + optional error message.

    Args:
        entity: Entity name to verify
        known_entities: List of allowed canonical entity names

    Returns:
        {"exists": bool, "error": str | None}
    """
    return _verify_entity_in_known_entities_impl(entity=entity, known_entities=known_entities)


def _verify_entity_in_known_entities_impl(entity: str, known_entities: List[str]) -> Dict[str, Any]:
    """Pure implementation of verify_entity_in_known_entities (safe to call from Python steps)."""
    ent = (entity or "").strip()
    if not ent:
        return {"exists": False, "error": "entity is empty/blank"}

    if not known_entities:
        return {"exists": False, "error": "known_entities list is empty"}

    lowered = {str(e).strip().lower(): str(e).strip() for e in known_entities if str(e).strip()}
    if ent.lower() in lowered:
        return {"exists": True, "error": None}

    # Provide a small hint to help the model self-correct (best-effort).
    try:
        from difflib import get_close_matches

        suggestions = get_close_matches(ent, list(lowered.values()), n=1, cutoff=0.0)
        suggestion = suggestions[0] if suggestions else None
    except Exception:
        suggestion = None

    hint = f" Closest match: '{suggestion}'." if suggestion else ""
    return {"exists": False, "error": f"'{entity}' is not in known_entities.{hint}"}


@tool
def validate_subset(subset: List[str], superset: List[str]) -> Dict[str, Any]:
    """Validate that all items in subset are present in superset (case-insensitive).

    Args:
        subset: Candidate list that should be a subset
        superset: Allowed list

    Returns:
        {"is_subset": bool, "invalid_items": List[str], "error": str | None}
    """
    return _validate_subset_impl(subset=subset, superset=superset)


def _validate_subset_impl(subset: List[str], superset: List[str]) -> Dict[str, Any]:
    """Pure implementation of validate_subset (safe to call from Python steps)."""
    sub = [str(x) for x in (subset or [])]
    sup = [str(x) for x in (superset or [])]

    sup_l = {s.strip().lower() for s in sup if s.strip()}
    invalid = [x for x in sub if x.strip() and x.strip().lower() not in sup_l]

    if invalid:
        return {
            "is_subset": False,
            "invalid_items": invalid,
            "error": f"subset contains {len(invalid)} item(s) not present in superset",
        }
    return {"is_subset": True, "invalid_items": [], "error": None}


@tool
def validate_merge_decision(should_merge: bool, merged_entity_name: Optional[str]) -> Dict[str, Any]:
    """Validate merge decision invariants.

    Rules:
    - If should_merge is True, merged_entity_name must be a non-empty string.
    - If should_merge is False, merged_entity_name must be null/None.
    """
    return _validate_merge_decision_impl(should_merge=should_merge, merged_entity_name=merged_entity_name)


def _validate_merge_decision_impl(should_merge: bool, merged_entity_name: Optional[str]) -> Dict[str, Any]:
    """Pure implementation of validate_merge_decision (safe to call from Python steps)."""
    if should_merge:
        if merged_entity_name is None or not str(merged_entity_name).strip():
            return {
                "valid": False,
                "error": "should_merge=true requires merged_entity_name to be a non-empty string",
            }
        return {"valid": True, "error": None}

    # should_merge == False
    if merged_entity_name is not None:
        return {"valid": False, "error": "should_merge=false requires merged_entity_name to be null"}
    return {"valid": True, "error": None}


@tool
def validate_final_entities(entities: List[str]) -> Dict[str, Any]:
    """Validate that all entity names are SQL-safe identifiers.

    This duplicates (lightly) the SQL-safe check used elsewhere to avoid relying on
    ToolException-driven tools for a bulk validation pass.

    Returns:
        {"all_valid": bool, "invalid_entities": List[str], "errors": List[str]}
    """
    return _validate_final_entities_impl(entities=entities)


def _validate_final_entities_impl(entities: List[str]) -> Dict[str, Any]:
    """Pure implementation of validate_final_entities (safe to call from Python steps)."""
    import re

    invalid: List[str] = []
    errors: List[str] = []

    for name in (entities or []):
        n = (name or "").strip()
        if not n:
            invalid.append(str(name))
            errors.append("empty entity name")
            continue
        if not re.match(r"^[a-zA-Z][a-zA-Z0-9_]*$", n):
            invalid.append(n)
            errors.append("must start with a letter and contain only letters/numbers/underscore")

    return {"all_valid": len(invalid) == 0, "invalid_entities": invalid, "errors": errors}


@tool
def validate_relation_cardinality_output(
    entities: List[str],
    entity_cardinalities: Dict[str, str],
    entity_participations: Dict[str, str],
) -> Dict[str, Any]:
    """Validate relation cardinality output completeness and allowed values."""
    ents = [str(e) for e in (entities or []) if str(e).strip()]
    missing: List[str] = []
    bad_vals: List[str] = []

    card = entity_cardinalities or {}
    part = entity_participations or {}

    for e in ents:
        if e not in card:
            missing.append(f"missing cardinality for '{e}'")
        else:
            if card[e] not in ("1", "N"):
                bad_vals.append(f"invalid cardinality for '{e}': {card[e]}")
        if e not in part:
            missing.append(f"missing participation for '{e}'")
        else:
            if part[e] not in ("total", "partial"):
                bad_vals.append(f"invalid participation for '{e}': {part[e]}")

    if missing or bad_vals:
        msgs = missing + bad_vals
        return {"valid": False, "error": "; ".join(msgs)}
    return {"valid": True, "error": None}


