"""Phase 5, Step 5.1: Attribute Dependency Graph Construction.

Builds a dependency graph of attributes based on PK/FK relationships and derived attribute dependencies.
This enables Kahn's algorithm for topological sorting in subsequent steps.
"""

from typing import Dict, Any, List, Set, Tuple, Optional
from collections import defaultdict, deque
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.observability import traceable_step, get_trace_config

logger = get_logger(__name__)


class FkDependencyInfo(BaseModel):
    """Information about a foreign key dependency."""
    attribute_key: str = Field(description="Attribute key in format 'Entity.attribute'")
    referenced_entity: str = Field(description="Name of the referenced entity")
    referenced_attribute: str = Field(description="Name of the referenced attribute")

    model_config = ConfigDict(extra="forbid")


class DerivedDependencyInfo(BaseModel):
    """Information about a derived attribute dependency."""
    attribute_key: str = Field(description="Attribute key in format 'Entity.attribute'")
    base_attributes: List[str] = Field(description="List of base attribute keys that this derived attribute depends on")

    model_config = ConfigDict(extra="forbid")


class AttributeDependencyGraphOutput(BaseModel):
    """Output structure for attribute dependency graph construction."""
    dependency_graph: Dict[str, List[str]] = Field(
        description="Dependency graph mapping attribute keys to their dependency lists"
    )
    independent_attributes: List[Tuple[str, str]] = Field(
        description="List of (entity, attribute) pairs with no dependencies"
    )
    dependent_attributes: List[Tuple[str, str]] = Field(
        description="List of (entity, attribute) pairs with dependencies"
    )
    fk_dependencies: List[FkDependencyInfo] = Field(
        description="List of foreign key dependency information"
    )
    derived_dependencies: List[DerivedDependencyInfo] = Field(
        description="List of derived attribute dependency information"
    )
    created_foreign_keys: List[Dict[str, Any]] = Field(
        description="List of foreign key definitions created deterministically from relations"
    )

    model_config = ConfigDict(extra="forbid")


def _create_foreign_keys_from_relations(
    relations: List[Dict[str, Any]],
    primary_keys: Dict[str, List[str]],
    relation_cardinalities: Optional[Dict[str, Dict[str, str]]] = None,
) -> List[Dict[str, Any]]:
    """
    Create foreign keys deterministically from relations and cardinalities.
    
    Rules:
    - For 1:N or N:1: FK goes on the N side (many side)
    - For 1:1: FK can go on either side (choose first entity)
    - For M:N: No FK in entity tables (handled by junction table in Phase 3)
    - For n-ary (3+ entities): No FK in entity tables (handled by junction table in Phase 3)
    
    Args:
        relations: List of relations from Phase 1
        primary_keys: Dictionary mapping entity names to their primary keys
        relation_cardinalities: Optional dictionary mapping relation_id to entity_cardinalities
        
    Returns:
        List of FK definitions: [{"from_entity": str, "from_attributes": [str], "to_entity": str, "to_attributes": [str]}]
    """
    foreign_keys = []
    
    for relation in relations:
        rel_entities = relation.get("entities", [])
        if not rel_entities or len(rel_entities) != 2:
            # Skip n-ary relations (3+ entities) - handled by junction tables in Phase 3
            continue
        
        e1_name = rel_entities[0] if isinstance(rel_entities[0], str) else rel_entities[0].get("name", "")
        e2_name = rel_entities[1] if isinstance(rel_entities[1], str) else rel_entities[1].get("name", "")
        
        if not e1_name or not e2_name:
            continue
        
        # Get cardinalities
        rel_id = relation.get("id", "") or "+".join(sorted([e1_name, e2_name]))
        entity_cardinalities = {}
        if relation_cardinalities and rel_id in relation_cardinalities:
            entity_cardinalities = relation_cardinalities[rel_id]
        
        e1_cardinality = entity_cardinalities.get(e1_name, "N")
        e2_cardinality = entity_cardinalities.get(e2_name, "N")
        
        # Get PKs
        e1_pk = primary_keys.get(e1_name, [])
        e2_pk = primary_keys.get(e2_name, [])
        
        if not e1_pk or not e2_pk:
            # Skip if PKs not available
            continue
        
        # Determine FK placement based on cardinality
        if (e1_cardinality == "1" and e2_cardinality == "N") or (e1_cardinality == "N" and e2_cardinality == "1"):
            # 1:N or N:1 - FK goes on the N side
            if e1_cardinality == "N":
                # FK in e1, references e2
                foreign_keys.append({
                    "from_entity": e1_name,
                    "from_attributes": e2_pk,  # Use referenced entity's PK name(s) as FK attribute name(s)
                    "to_entity": e2_name,
                    "to_attributes": e2_pk,
                })
            else:
                # FK in e2, references e1
                foreign_keys.append({
                    "from_entity": e2_name,
                    "from_attributes": e1_pk,  # Use referenced entity's PK name(s) as FK attribute name(s)
                    "to_entity": e1_name,
                    "to_attributes": e1_pk,
                })
        elif e1_cardinality == "1" and e2_cardinality == "1":
            # 1:1 - FK can go on either side, choose e1 -> e2
            foreign_keys.append({
                "from_entity": e1_name,
                "from_attributes": e2_pk,
                "to_entity": e2_name,
                "to_attributes": e2_pk,
            })
        # M:N relations are skipped - handled by junction tables in Phase 4
    
    return foreign_keys


@traceable_step("5.1", phase=5, tags=["dependency_graph", "deterministic"])
def step_5_1_attribute_dependency_graph(
    entities: List[Dict[str, Any]],
    attributes: Dict[str, List[Dict[str, Any]]],  # entity -> list of attributes
    primary_keys: Dict[str, List[str]],  # entity -> list of PK attribute names
    relations: List[Dict[str, Any]],  # Relations from Phase 1 (for deterministic FK creation)
    relation_cardinalities: Optional[Dict[str, Dict[str, str]]] = None,  # relation_id -> entity_cardinalities from Phase 1
    foreign_keys: Optional[List[Dict[str, Any]]] = None,  # Optional: legacy FK list (if provided, use it; otherwise create from relations)
    derived_formulas: Optional[Dict[str, Dict[str, Any]]] = None,  # "Entity.attr" -> formula info
) -> AttributeDependencyGraphOutput:
    """
    Step 5.1 (deterministic): Build attribute dependency graph.
    
    Creates a graph where:
    - FK attributes depend on the PK attributes they reference
    - Derived attributes depend on the attributes used in their formulas
    - Composite attributes depend on their component attributes
    
    Foreign keys are created deterministically from relations and cardinalities if not provided.
    This aligns with the plan: FKs are created deterministically, not via LLM in Phase 2.
    
    Args:
        entities: List of entities
        attributes: Dictionary mapping entity names to their attributes
        primary_keys: Dictionary mapping entity names to their primary key attributes
        relations: List of relations from Phase 1 (used to create FKs deterministically)
        relation_cardinalities: Optional dictionary mapping relation_id to entity_cardinalities
        foreign_keys: Optional list of FK definitions (if provided, use it; otherwise create from relations)
        derived_formulas: Optional dictionary of derived attribute formulas (from Phase 2.9)
        
    Returns:
        dict: Dependency graph with:
            - dependency_graph: Dict[str, List[str]] - attribute -> list of dependencies
            - independent_attributes: List[Tuple[str, str]] - (entity, attribute) pairs with no dependencies
            - dependent_attributes: List[Tuple[str, str]] - (entity, attribute) pairs with dependencies
            - fk_dependencies: Dict[str, Tuple[str, str]] - FK attribute -> (referenced_entity, referenced_attr)
            - derived_dependencies: Dict[str, List[str]] - derived attribute -> list of base attributes
            
    Example:
        >>> graph = step_5_1_attribute_dependency_graph(
        ...     entities=[{"name": "Order"}],
        ...     attributes={"Order": [{"name": "order_id"}, {"name": "customer_id"}]},
        ...     primary_keys={"Order": ["order_id"]},
        ...     foreign_keys=[{"from_entity": "Order", "from_attributes": ["customer_id"], 
        ...                    "to_entity": "Customer", "to_attributes": ["customer_id"]}]
        ... )
        >>> "Order.customer_id" in graph["dependent_attributes"]
        True
    """
    logger.info("Starting Step 5.1: Attribute Dependency Graph Construction")
    
    # Create FKs deterministically from relations if not provided
    if not foreign_keys:
        foreign_keys = _create_foreign_keys_from_relations(
            relations=relations,
            primary_keys=primary_keys,
            relation_cardinalities=relation_cardinalities,
        )
        logger.info(f"Created {len(foreign_keys)} foreign keys deterministically from relations")
    
    # Build dependency graph: "Entity.attribute" -> list of "Entity.attribute" dependencies
    dependency_graph: Dict[str, List[str]] = defaultdict(list)
    fk_dependencies: Dict[str, Tuple[str, str]] = {}  # "Entity.attr" -> (ref_entity, ref_attr)
    derived_dependencies: Dict[str, List[str]] = {}  # "Entity.attr" -> list of base attrs
    
    # Get all attribute names per entity
    entity_attr_names: Dict[str, Set[str]] = {}
    for entity_name, attrs in attributes.items():
        attr_names = set()
        for attr in attrs:
            attr_name = attr.get("name", "") if isinstance(attr, dict) else getattr(attr, "name", "")
            if attr_name:
                attr_names.add(attr_name)
        entity_attr_names[entity_name] = attr_names
    
    # 1. FK dependencies: FK -> PK
    for fk in (foreign_keys or []):
        from_entity = fk.get("from_entity", "") if isinstance(fk, dict) else getattr(fk, "from_entity", "")
        to_entity = fk.get("to_entity", "") if isinstance(fk, dict) else getattr(fk, "to_entity", "")
        from_attrs = fk.get("from_attributes", []) if isinstance(fk, dict) else getattr(fk, "from_attributes", [])
        to_attrs = fk.get("to_attributes", []) if isinstance(fk, dict) else getattr(fk, "to_attributes", [])
        
        if not from_entity or not to_entity or not from_attrs or not to_attrs:
            continue
        
        # Match FK attributes to PK attributes (assuming 1:1 correspondence)
        for from_attr, to_attr in zip(from_attrs, to_attrs):
            fk_key = f"{from_entity}.{from_attr}"
            pk_key = f"{to_entity}.{to_attr}"
            
            # FK depends on PK
            dependency_graph[fk_key].append(pk_key)
            fk_dependencies[fk_key] = (to_entity, to_attr)
    
    # 2. Derived attribute dependencies (if available)
    if derived_formulas:
        for key, formula_info in derived_formulas.items():
            if not isinstance(key, str) or "." not in key:
                continue
            if not isinstance(formula_info, dict):
                continue
            
            entity_name, attr_name = key.split(".", 1)
            deps = formula_info.get("dependencies", []) or []
            
            if not isinstance(deps, list):
                continue
            
            # Derived attribute depends on base attributes
            derived_key = f"{entity_name}.{attr_name}"
            base_deps = []
            for dep in deps:
                if isinstance(dep, str):
                    # Assume dependency is within the same entity (entity-local)
                    base_key = f"{entity_name}.{dep}"
                    base_deps.append(base_key)
            
            if base_deps:
                dependency_graph[derived_key].extend(base_deps)
                derived_dependencies[derived_key] = base_deps
    
    # 3. Composite attribute dependencies (if available)
    # TODO: Add composite attribute dependency detection if composite_decompositions are available
    
    # Identify independent and dependent attributes
    all_attributes: Set[Tuple[str, str]] = set()  # (entity, attribute)
    for entity_name, attr_names in entity_attr_names.items():
        for attr_name in attr_names:
            all_attributes.add((entity_name, attr_name))
    
    # Find independent attributes (no incoming edges in dependency graph)
    dependent_keys = set()
    for deps in dependency_graph.values():
        dependent_keys.update(deps)
    
    independent_attributes: List[Tuple[str, str]] = []
    dependent_attributes: List[Tuple[str, str]] = []
    
    for entity_name, attr_name in all_attributes:
        attr_key = f"{entity_name}.{attr_name}"
        if attr_key not in dependent_keys:
            independent_attributes.append((entity_name, attr_name))
        else:
            dependent_attributes.append((entity_name, attr_name))
    
    logger.info(
        f"Dependency graph constructed: {len(independent_attributes)} independent, "
        f"{len(dependent_attributes)} dependent attributes"
    )
    
    # Convert fk_dependencies dict to list
    fk_dependencies_list = [
        FkDependencyInfo(
            attribute_key=k,
            referenced_entity=v[0],
            referenced_attribute=v[1]
        )
        for k, v in fk_dependencies.items()
    ]
    
    # Convert derived_dependencies dict to list
    derived_dependencies_list = [
        DerivedDependencyInfo(
            attribute_key=k,
            base_attributes=v
        )
        for k, v in derived_dependencies.items()
    ]
    
    return AttributeDependencyGraphOutput(
        dependency_graph=dict(dependency_graph),
        independent_attributes=independent_attributes,
        dependent_attributes=dependent_attributes,
        fk_dependencies=fk_dependencies_list,
        derived_dependencies=derived_dependencies_list,
        created_foreign_keys=foreign_keys,  # Return created FKs for Phase 5.3
    )
