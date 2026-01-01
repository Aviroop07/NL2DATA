"""Validation + normalization helpers for relations (pure Python, deterministic).

These helpers are intended to be called directly from pipeline steps (no agent/tool calling).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


def _normalize_entity_list(entities: Any) -> List[str]:
    out: List[str] = []
    for e in (entities or []):
        s = str(e).strip()
        if s:
            out.append(s)
    return out


def _canonical_items(d: Optional[Dict[str, Any]]) -> Tuple[tuple, ...]:
    """Canonicalize a dict into a stable, hashable tuple of (key, value) sorted by key."""
    if not d:
        return tuple()
    items = []
    for k, v in d.items():
        kk = str(k).strip()
        vv = str(v).strip()
        if kk:
            items.append((kk, vv))
    items.sort(key=lambda x: x[0])
    return tuple(items)


def _dedupe_relations_by_constraints_impl(
    relations: List[Dict[str, Any]],
    *,
    cardinalities_by_key: Optional[Dict[tuple, Dict[str, str]]] = None,
    participations_by_key: Optional[Dict[tuple, Dict[str, str]]] = None,
) -> Dict[str, Any]:
    """
    Deterministically detect and dedupe duplicate relations after cardinalities/participations are known.

    Duplicate definition (per user requirement):
    - participating entity SET is the same, AND
    - entity_cardinalities dict is the same, AND
    - entity_participations dict is the same.

    Behavior:
    - If duplicates are identical by that signature: keep the first, drop the rest.
    - If duplicates share entity set but differ in cardinalities/participations: report conflicts.

    Returns:
        {
          "deduped_relations": List[Dict[str, Any]],
          "removed_duplicate_count": int,
          "duplicate_groups": List[Dict[str, Any]],
          "conflicts": List[str],
        }
    """
    deduped: List[Dict[str, Any]] = []
    conflicts: List[str] = []
    removed = 0

    # key: entities-set (sorted tuple) -> map(signature -> first_seen_index)
    seen_per_entity_set: Dict[tuple, Dict[Tuple[tuple, tuple], int]] = {}
    sig_to_example: Dict[Tuple[tuple, Tuple[tuple, tuple]], Dict[str, Any]] = {}

    for idx, rel in enumerate(relations or []):
        if not isinstance(rel, dict):
            continue

        ents = _normalize_entity_list(rel.get("entities", []))
        if len(set(ents)) < 2:
            deduped.append(rel)
            continue

        entity_key = tuple(sorted(set(ents)))

        # Pull authoritative cards/parts from provided maps when available, else from relation object.
        cards = None
        parts = None
        if cardinalities_by_key and entity_key in cardinalities_by_key:
            cards = cardinalities_by_key.get(entity_key)
        else:
            cards = rel.get("entity_cardinalities") or {}
        if participations_by_key and entity_key in participations_by_key:
            parts = participations_by_key.get(entity_key)
        else:
            parts = rel.get("entity_participations") or {}

        signature = (_canonical_items(cards), _canonical_items(parts))

        bucket = seen_per_entity_set.setdefault(entity_key, {})
        if signature in bucket:
            # identical duplicate
            removed += 1
            continue

        # If same entity-set exists but with a different signature, that's a conflict.
        if bucket and signature not in bucket:
            examples = sig_to_example.get((entity_key, signature))
            # Emit a stable textual conflict summary (no truncation).
            conflicts.append(
                "Duplicate relation entity-set has conflicting constraints: "
                + f"entities={list(entity_key)}; "
                + f"cardinalities={dict(cards or {})}; participations={dict(parts or {})}"
            )

        bucket[signature] = idx
        sig_to_example[(entity_key, signature)] = rel
        deduped.append(rel)

    duplicate_groups = [
        {"entities": list(k), "distinct_signatures": len(v)} for k, v in seen_per_entity_set.items() if len(v) > 1
    ]

    return {
        "deduped_relations": deduped,
        "removed_duplicate_count": int(removed),
        "duplicate_groups": duplicate_groups,
        "conflicts": conflicts,
    }


