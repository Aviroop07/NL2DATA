"""Relation type definitions and normalization helpers.

Phase 1 intentionally restricts relation types to a small enum to keep downstream logic deterministic.
"""

from __future__ import annotations

from enum import Enum
from typing import Optional


class RelationType(str, Enum):
    ONE_TO_ONE = "one-to-one"
    ONE_TO_MANY = "one-to-many"
    MANY_TO_ONE = "many-to-one"
    MANY_TO_MANY = "many-to-many"
    TERNARY = "ternary"


def normalize_relation_type(value: Optional[str]) -> RelationType:
    """
    Normalize an arbitrary model-produced relation type string into the Phase-1 enum.

    Rules (deterministic):
    - Accept common aliases (1:1, 1:n, n:1, n:m, etc.)
    - Accept minor punctuation/spacing variants.
    - If unknown/blank, default to ONE_TO_MANY (conservative; most real schemas contain many 1:N edges).
    """
    raw = (value or "").strip().lower()
    if not raw:
        return RelationType.ONE_TO_MANY

    raw = raw.replace("_", "-")
    raw = raw.replace(" ", "-")
    raw = raw.replace("--", "-")

    # Common encodings
    if raw in {"1:1", "1-1", "one-to-one", "one-to-1", "one-1", "one-one"}:
        return RelationType.ONE_TO_ONE
    if raw in {"1:n", "1-n", "one-to-many", "one-to-n", "one-n", "one-many"}:
        return RelationType.ONE_TO_MANY
    if raw in {"n:1", "n-1", "many-to-one", "n-to-one", "many-1", "many-one"}:
        return RelationType.MANY_TO_ONE
    if raw in {"n:m", "n-m", "many-to-many", "n-to-many", "many-many"}:
        return RelationType.MANY_TO_MANY
    if raw in {"ternary", "3-ary", "3ary", "n-ary", "nary", "three-way", "three-way-relation"}:
        return RelationType.TERNARY

    # Heuristic keyword match (still deterministic)
    if "many" in raw and "one" in raw:
        if raw.startswith("many") or raw.endswith("one"):
            return RelationType.MANY_TO_ONE
        return RelationType.ONE_TO_MANY
    if "many" in raw:
        return RelationType.MANY_TO_MANY
    if "one" in raw:
        return RelationType.ONE_TO_ONE
    if "ternary" in raw or "n-ary" in raw or "nary" in raw:
        return RelationType.TERNARY

    return RelationType.ONE_TO_MANY


