"""Connectivity and graph analysis tools for entity relationships."""

from typing import List, Dict, Any
from langchain_core.tools import tool
from langchain_core.tools.base import ToolException


@tool
def check_entity_name_similarity(entity1: str, entity2: str) -> Dict[str, Any]:
    """Compute similarity score between two entity names to detect potential duplicates or synonyms.
    
    Args:
        entity1: First entity name
        entity2: Second entity name
        
    Returns:
        Dictionary with 'similarity' (float 0-1) and 'are_synonyms' (bool)
        
    Purpose: Helps detect duplicate entities or synonyms (e.g., "User" vs "Customer") during
    entity consolidation. The LLM can use this to identify entities that should be merged.
    """
    from difflib import SequenceMatcher
    
    # Normalize names (lowercase, strip)
    n1 = entity1.lower().strip()
    n2 = entity2.lower().strip()
    
    # Exact match
    if n1 == n2:
        return {
            "similarity": 1.0,
            "are_synonyms": True,
        }
    
    # Sequence similarity
    similarity = SequenceMatcher(None, n1, n2).ratio()
    
    # Check for common synonyms
    common_synonyms = [
        ("user", "customer"),
        ("client", "customer"),
        ("person", "user"),
        ("item", "product"),
        ("order", "transaction"),
        ("purchase", "order"),
    ]
    
    are_synonyms = False
    
    for syn1, syn2 in common_synonyms:
        if (n1 == syn1 and n2 == syn2) or (n1 == syn2 and n2 == syn1):
            are_synonyms = True
            break
    
    # High similarity threshold
    if similarity > 0.8:
        are_synonyms = True
    
    return {
        "similarity": round(similarity, 3),
        "are_synonyms": are_synonyms,
    }


def _check_entity_connectivity_impl(entity: str, relations: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Pure (non-LangChain-tool) implementation of entity connectivity check."""
    entity_lower = entity.lower()
    connected_entities = set()

    for relation in relations:
        rel_entities = relation.get("entities", [])
        rel_entities_lower = [e.lower() for e in rel_entities]

        if entity_lower in rel_entities_lower:
            # Entity is in this relation, add all other entities
            for e in rel_entities:
                if e.lower() != entity_lower:
                    connected_entities.add(e)

    is_connected = len(connected_entities) > 0
    is_orphan = not is_connected

    return {
        "is_connected": is_connected,
        "connected_to": sorted(list(connected_entities)),
        "is_orphan": is_orphan,
        "reasoning": f"Entity '{entity}' is {'connected' if is_connected else 'orphaned'}",
    }


@tool
def check_entity_connectivity(entity: str, relations: List[Dict[str, Any]], schema_state: Dict[str, Any]) -> Dict[str, Any]:
    """Check if an entity is connected to other entities through relations.
    
    Args:
        entity: Entity name to check
        relations: List of relation dictionaries
        schema_state: Current schema state dictionary
        
    Returns:
        Dictionary with 'is_connected' (bool), 'connected_to' (List[str]), and 'is_orphan' (bool)
        
    Purpose: Identifies orphan entities that are not connected to any other entities through relations.
    This helps detect schema design issues early.
    """
    # NOTE: This function is decorated with @tool and becomes a StructuredTool.
    # Internal Python code should not call this directly (StructuredTool is not callable).
    # Delegate to a pure implementation.
    return _check_entity_connectivity_impl(entity, relations)


def _detect_circular_dependencies_impl(relations: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Pure (non-LangChain-tool) implementation of detect_circular_dependencies."""
    graph: Dict[str, List[str]] = {}
    for relation in relations:
        entities = relation.get("entities", []) or []
        if len(entities) == 2:
            e1, e2 = entities[0], entities[1]
            graph.setdefault(e1, []).append(e2)

    cycles: List[List[str]] = []
    visited: set[str] = set()
    rec_stack: set[str] = set()

    def dfs(node: str, path: List[str]) -> None:
        if node in rec_stack:
            if node in path:
                cycle_start = path.index(node)
                cycles.append(path[cycle_start:] + [node])
            return
        if node in visited:
            return
        visited.add(node)
        rec_stack.add(node)
        for neighbor in graph.get(node, []):
            dfs(neighbor, path + [node])
        rec_stack.remove(node)

    for node in list(graph.keys()):
        if node not in visited:
            dfs(node, [])

    has_circular_dependency = len(cycles) > 0
    return {
        "has_circular_dependency": has_circular_dependency,
        "cycles": cycles,
        "reasoning": f"Found {len(cycles)} circular dependencies" if has_circular_dependency else "No circular dependencies detected",
    }


@tool
def detect_circular_dependencies(relations: List[Dict[str, Any]], schema_state: Dict[str, Any]) -> Dict[str, Any]:
    """Detect circular reference patterns in relations.
    
    Args:
        relations: List of relation dictionaries
        schema_state: Current schema state dictionary
        
    Returns:
        Dictionary with 'has_circular_dependency' (bool), 'cycles' (List[List[str]]), and 'reasoning' (str)
        
    Purpose: Detects circular dependencies in relations that could cause issues in foreign key design
    or data integrity. For example: A -> B -> C -> A creates a cycle.
    """
    # NOTE: This function is decorated with @tool and becomes a StructuredTool.
    # Internal Python code should not call it directly; use the pure implementation.
    return _detect_circular_dependencies_impl(relations)


def _validate_cardinality_consistency_impl(relation: Dict[str, Any]) -> Dict[str, Any]:
    """Pure (non-LangChain-tool) implementation of validate_cardinality_consistency."""
    rel_type = (relation.get("type", "") or "").lower()
    entities = relation.get("entities", []) or []
    cardinalities = relation.get("entity_cardinalities", {}) or {}

    errors: List[str] = []

    if not cardinalities:
        return {
            "is_consistent": False,
            "errors": ["No cardinalities defined"],
            "reasoning": "Cardinalities must be defined",
        }

    for entity in entities:
        if entity not in cardinalities:
            errors.append(f"Entity '{entity}' missing cardinality")

    # Phase-1 deterministic consistency rules.
    #
    # IMPORTANT:
    # The pipeline does NOT currently encode a stable directional ordering for `entities`.
    # Therefore, checks for one-to-many / many-to-one are **order-independent** and validated
    # based on the per-entity cardinalities ("1"/"N").
    rel_type_norm = rel_type.strip().replace("_", "-")

    if rel_type_norm in {"one-to-one", "1:1", "1-1"}:
        if len(entities) != 2:
            errors.append(f"one-to-one relation must be binary (arity=2), got arity={len(entities)}")
        else:
            e1, e2 = entities[0], entities[1]
            if cardinalities.get(e1) != "1" or cardinalities.get(e2) != "1":
                errors.append(
                    f"one-to-one requires both cardinalities to be '1' (got {e1}={cardinalities.get(e1)}, {e2}={cardinalities.get(e2)})"
                )
    elif rel_type_norm in {"one-to-many", "many-to-one", "1:n", "1-n", "n:1", "n-1"}:
        if len(entities) != 2:
            errors.append(f"{rel_type_norm} relation must be binary (arity=2), got arity={len(entities)}")
        else:
            ones = [e for e in entities if cardinalities.get(e) == "1"]
            ns = [e for e in entities if cardinalities.get(e) == "N"]
            if len(ones) != 1 or len(ns) != 1:
                errors.append(
                    f"{rel_type_norm} requires exactly one '1' and one 'N' (got ones={ones}, Ns={ns})"
                )
    elif rel_type_norm in {"many-to-many", "n:m", "n-m", "n-n"}:
        if len(entities) != 2:
            errors.append(f"many-to-many relation must be binary (arity=2), got arity={len(entities)}")
        else:
            e1, e2 = entities[0], entities[1]
            if cardinalities.get(e1) != "N" or cardinalities.get(e2) != "N":
                errors.append(
                    f"many-to-many requires both cardinalities to be 'N' (got {e1}={cardinalities.get(e1)}, {e2}={cardinalities.get(e2)})"
                )
    elif rel_type_norm == "ternary":
        if len(entities) < 3:
            errors.append(f"ternary relation must have arity>=3, got arity={len(entities)}")

    is_consistent = len(errors) == 0
    return {
        "is_consistent": is_consistent,
        "errors": errors,
        "reasoning": "Cardinalities are consistent" if is_consistent else f"Found {len(errors)} consistency errors",
    }


@tool
def validate_cardinality_consistency(relation: Dict[str, Any], schema_state: Dict[str, Any]) -> Dict[str, Any]:
    """Validate cardinality values are consistent with relation type.
    
    Args:
        relation: Relation dictionary with entities, type, cardinalities
        schema_state: Current schema state dictionary
        
    Returns:
        Dictionary with 'is_consistent' (bool), 'errors' (List[str]), and 'reasoning' (str)
        
    Purpose: Validates that cardinality values match the relation type. For example, a "one-to-many"
    relation should have one entity with cardinality "1" and one with "N".
    """
    # NOTE: This function is decorated with @tool and becomes a StructuredTool.
    # Internal Python code should not call it directly; use the pure implementation.
    return _validate_cardinality_consistency_impl(relation)

