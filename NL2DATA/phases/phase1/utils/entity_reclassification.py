"""Utilities for reclassifying 'entities' that are actually associative/link constructs.

We see false-positive entities like `OrderItem`, `BookAuthor`, etc. These are typically
relationships-with-attributes (junction tables) and should be handled as relations later,
not as core entities in Phase 2 (PK identification, intrinsic attributes, etc.).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, List, Optional, Set, Tuple


_JUNCTION_NAME_HINTS = {
    "item",
    "line",
    "lineitem",
    "orderitem",
    "detail",
    "link",
    "map",
    "mapping",
    "junction",
    "bridge",
    "assoc",
    "association",
    "xref",
    "join",
}


def _tokenize_name(name: str) -> List[str]:
    """Tokenize common entity naming styles into lowercase tokens.

    Handles:
    - CamelCase: BookAuthor -> ["book", "author"]
    - snake_case: book_author -> ["book", "author"]
    - kebab-case: book-author -> ["book", "author"]
    """
    if not name:
        return []
    # Normalize separators
    s = re.sub(r"[-_]+", " ", name.strip())
    # Split camel case boundaries into spaces (keep acronyms reasonably)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", s)
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", s)
    tokens = [t.lower() for t in re.split(r"\s+", s) if t.strip()]
    return tokens


def _token_set_from_names(names: Iterable[str]) -> Set[str]:
    out: Set[str] = set()
    for n in names:
        out.update(_tokenize_name(n))
    return out


@dataclass(frozen=True)
class AssociativeHeuristicResult:
    name: str
    score: float
    reasons: Tuple[str, ...]


def heuristic_associative_entity(
    entity_name: str,
    entity_description: Optional[str],
    all_entity_names: Iterable[str],
) -> AssociativeHeuristicResult:
    """Heuristically flag entities that look like associative/link constructs."""
    reasons: List[str] = []
    score = 0.0

    name_tokens = _tokenize_name(entity_name)
    name_token_set = set(name_tokens)
    all_tokens = _token_set_from_names([n for n in all_entity_names if n and n != entity_name])

    desc = (entity_description or "").lower()

    # Name contains typical junction hints
    if any(tok in _JUNCTION_NAME_HINTS for tok in name_tokens):
        score += 0.45
        reasons.append("name_contains_junction_hint")

    # Description hints
    if any(k in desc for k in ["junction", "link", "bridge", "associative", "many-to-many", "line item", "line-item"]):
        score += 0.45
        reasons.append("description_mentions_linking")

    # Compound name likely composed of other entity tokens (e.g., BookAuthor)
    compound_overlap = [t for t in name_tokens if t in all_tokens]
    if len(compound_overlap) >= 2:
        score += 0.35
        reasons.append("name_compound_of_other_entities")

    # Very short descriptions + compound name also tends to be a junction artifact
    if (not entity_description or len(entity_description.strip()) < 20) and len(compound_overlap) >= 2:
        score += 0.15
        reasons.append("weak_description_and_compound_name")

    # Cap
    if score > 1.0:
        score = 1.0

    return AssociativeHeuristicResult(
        name=entity_name,
        score=score,
        reasons=tuple(reasons),
    )


def pick_associative_candidates(
    entities: List[dict],
    threshold: float = 0.6,
) -> List[AssociativeHeuristicResult]:
    """Return heuristic candidates above a score threshold."""
    names = [
        e.get("name", "") if isinstance(e, dict) else getattr(e, "name", "")
        for e in entities
    ]
    results: List[AssociativeHeuristicResult] = []
    for e in entities:
        n = e.get("name", "") if isinstance(e, dict) else getattr(e, "name", "")
        d = e.get("description", "") if isinstance(e, dict) else getattr(e, "description", "")
        if not n:
            continue
        r = heuristic_associative_entity(n, d, names)
        if r.score >= threshold:
            results.append(r)
    # Sort by confidence descending
    return sorted(results, key=lambda r: r.score, reverse=True)


