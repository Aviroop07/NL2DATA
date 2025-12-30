"""Phase 2, Step 2.14: Relation Realization.

Determines how relationships are implemented (foreign keys, junction tables).
For binary relations: foreign keys. For n-ary relations (3+ entities): junction table.
Specifies which attributes realize relations and referential integrity rules.
"""

from typing import Dict, Any, List, Optional, Literal
from pydantic import BaseModel, Field

from NL2DATA.phases.phase2.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.phases.phase1.utils.data_extraction import (
    extract_entity_name,
    extract_entity_description,
    extract_attribute_name,
    extract_attribute_type_hint,
    extract_attribute_field,
    find_entity_by_name,
)

logger = get_logger(__name__)


class RelationRealizationOutput(BaseModel):
    """Output structure for relation realization."""
    realization_type: Literal["foreign_key", "junction_table"] = Field(
        description="Type of realization: 'foreign_key' for binary relations, 'junction_table' for n-ary relations (3+ entities)"
    )
    realization_attrs: Optional[Dict[str, str]] = Field(
        default=None,
        description="Dictionary mapping foreign key attribute names to their referenced table.column (e.g., {'customer_id': 'Customer.customer_id'}). Null for junction tables."
    )
    junction_table_name: Optional[str] = Field(
        default=None,
        description="Name of the junction table if realization_type is 'junction_table'. Null for foreign key realizations."
    )
    exists: bool = Field(
        description="Whether the foreign key attributes or junction table already exist in the schema"
    )
    needs_creation: bool = Field(
        description="Whether the foreign key attributes or junction table need to be created"
    )
    referential_integrity: Optional[Dict[str, Optional[Literal["CASCADE", "SET_NULL", "RESTRICT"]]]] = Field(
        default=None,
        description="Dictionary mapping foreign key attribute names to their referential integrity action (CASCADE, SET_NULL, RESTRICT, or null). Null for junction tables (FKs are in the junction table, not entity tables)."
    )
    attributes_to_remove: List[str] = Field(
        default_factory=list,
        description="List of attribute names that should be removed because they are redundant (replaced by foreign keys). Example: if 'sensor_type_id' FK is created, 'sensor_type' (string) should be removed."
    )
    reasoning: str = Field(description="Explanation of the realization decision and referential integrity choices")


def add_foreign_key_attributes_to_entities(
    relation_results: Dict[str, Dict[str, Any]],
    entity_attributes: Dict[str, List[Dict[str, Any]]],
    relations: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Add foreign key attributes to entity attribute lists when needs_creation=True.
    
    This function processes Step 2.14 results and actually creates the FK attributes
    in the entity attribute lists, ensuring they exist before Step 3.5 runs.
    
    Args:
        relation_results: Dictionary mapping relation_id to Step 2.14 realization results
        entity_attributes: Dictionary mapping entity names to their attribute lists
        relations: List of relation dictionaries from Step 1.9
        
    Returns:
        Updated entity_attributes dictionary with FK attributes added
    """
    logger.info("Adding foreign key attributes to entities based on Step 2.14 results")
    
    # Create a mapping from relation to its entities
    relation_to_entities = {}
    for relation in relations:
        rel_entities = relation.get("entities", [])
        rel_id = f"{'+'.join(sorted(rel_entities))}"
        relation_to_entities[rel_id] = rel_entities
    
    # Process each relation result
    for rel_id, rel_result in relation_results.items():
        realization_type = rel_result.get("realization_type")
        needs_creation = rel_result.get("needs_creation", False)
        realization_attrs = rel_result.get("realization_attrs", {})
        
        if realization_type == "foreign_key" and needs_creation and realization_attrs:
            # Find which entity should get the FK attributes
            rel_entities = relation_to_entities.get(rel_id, [])
            if not rel_entities:
                logger.warning(f"Could not find entities for relation {rel_id}")
                continue
            
            # For binary relations, determine which entity gets the FK
            # Based on cardinality: FK goes in the "many" side for 1:N, either side for 1:1
            # For now, we'll add FK to the entity that doesn't match the referenced entity
            for fk_attr_name, ref in realization_attrs.items():
                # Parse reference (e.g., "SensorType.sensor_type_id")
                if "." in ref:
                    ref_entity, ref_attr = ref.split(".", 1)
                    
                    # Find which entity in the relation should get this FK
                    # The FK should go in the entity that is NOT the referenced entity
                    target_entity = None
                    for entity_name in rel_entities:
                        if entity_name != ref_entity:
                            target_entity = entity_name
                            break
                    
                    if not target_entity:
                        logger.warning(
                            f"Could not determine target entity for FK {fk_attr_name} "
                            f"referencing {ref_entity} in relation {rel_id}"
                        )
                        continue
                    
                    # Check if attribute already exists
                    entity_attrs = entity_attributes.get(target_entity, [])
                    attr_names = [extract_attribute_name(attr) for attr in entity_attrs]
                    
                    if fk_attr_name not in attr_names:
                        # Add the FK attribute
                        fk_attr = {
                            "name": fk_attr_name,
                            "description": f"Foreign key to {ref_entity}",
                            "type_hint": "integer",  # Default to integer for FK, can be overridden later
                            "is_foreign_key": True,
                            "references_table": ref_entity,
                            "references_attribute": ref_attr,
                        }
                        entity_attributes.setdefault(target_entity, []).append(fk_attr)
                        logger.debug(
                            f"Added FK attribute {fk_attr_name} to entity {target_entity} "
                            f"(references {ref_entity}.{ref_attr})"
                        )
                    else:
                        # Attribute exists - mark it as a foreign key if not already
                        for attr in entity_attrs:
                            attr_name = extract_attribute_name(attr)
                            if attr_name == fk_attr_name:
                                if isinstance(attr, dict):
                                    attr["is_foreign_key"] = True
                                    attr["references_table"] = ref_entity
                                    attr["references_attribute"] = ref_attr
                                logger.debug(
                                    f"Updated existing attribute {fk_attr_name} in entity {target_entity} "
                                    f"to be a foreign key"
                                )
                                break
    
    return entity_attributes


def remove_redundant_relationship_attributes(
    entity_attributes: Dict[str, List[Dict[str, Any]]],
    foreign_key_results: Dict[str, Dict[str, Any]],
    relations: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Remove redundant string attributes that are replaced by foreign keys.
    
    This is a deterministic post-processing step that:
    1. Processes attributes_to_remove from Step 2.14 LLM results
    2. Applies deterministic rules to catch any missed redundant attributes
    
    Example: If 'sensor_type_id' FK exists, remove 'sensor_type' (string) attribute.
    
    Args:
        entity_attributes: Dictionary mapping entity names to their attribute lists
        foreign_key_results: Results from Step 2.14 with FK attribute names and attributes_to_remove
        relations: List of relations to determine which entities are involved
        
    Returns:
        Updated entity_attributes with redundant attributes removed
    """
    logger.info("Removing redundant relationship attributes replaced by foreign keys")
    
    # Step 1: Process attributes_to_remove from Step 2.14 LLM results
    entity_attrs_to_remove = {}  # entity_name -> set of attribute names to remove
    
    for rel_id, fk_result in foreign_key_results.items():
        if fk_result.get("realization_type") == "foreign_key":
            # Get attributes_to_remove from LLM result
            attrs_to_remove = fk_result.get("attributes_to_remove", [])
            if not attrs_to_remove:
                continue
            
            # Find which entity should have these attributes removed
            relation_entities = []
            for relation in relations:
                rel_entities = relation.get("entities", [])
                rel_id_check = f"{'+'.join(sorted(rel_entities))}"
                if rel_id_check == rel_id:
                    relation_entities = rel_entities
                    break
            
            if not relation_entities:
                continue
            
            # Determine target entity (entity that gets the FK)
            realization_attrs = fk_result.get("realization_attrs", {})
            for fk_attr_name, ref in realization_attrs.items():
                if "." in ref:
                    ref_entity, _ = ref.split(".", 1)
                    # FK goes in the entity that is NOT the referenced entity
                    for entity_name in relation_entities:
                        if entity_name != ref_entity:
                            entity_attrs_to_remove.setdefault(entity_name, set()).update(attrs_to_remove)
                            logger.debug(
                                f"Step 2.14 identified redundant attributes to remove from {entity_name}: {attrs_to_remove}"
                            )
                            break
    
    # Step 2: Apply deterministic rules to catch any missed redundant attributes
    # Build mapping: entity -> set of FK attribute names
    entity_fk_attrs = {}
    for rel_id, fk_result in foreign_key_results.items():
        if fk_result.get("realization_type") == "foreign_key":
            realization_attrs = fk_result.get("realization_attrs", {})
            
            # Find which entity gets the FK
            relation_entities = []
            for relation in relations:
                rel_entities = relation.get("entities", [])
                rel_id_check = f"{'+'.join(sorted(rel_entities))}"
                if rel_id_check == rel_id:
                    relation_entities = rel_entities
                    break
            
            if not relation_entities:
                continue
            
            # For each FK attribute, determine target entity
            for fk_attr_name, ref in realization_attrs.items():
                if "." in ref:
                    ref_entity, _ = ref.split(".", 1)
                    # FK goes in the entity that is NOT the referenced entity
                    for entity_name in relation_entities:
                        if entity_name != ref_entity:
                            entity_fk_attrs.setdefault(entity_name, set()).add(fk_attr_name)
                            break
    
    # Apply deterministic pattern: if "X_id" FK exists, check if "X" (string) should be removed
    for entity_name, fk_attrs in entity_fk_attrs.items():
        if entity_name not in entity_attributes:
            continue
        
        attrs = entity_attributes[entity_name]
        
        for attr in attrs:
            attr_name = extract_attribute_name(attr)
            
            # Skip if already marked for removal
            if entity_name in entity_attrs_to_remove and attr_name in entity_attrs_to_remove[entity_name]:
                continue
            
            # Check if this attribute should be removed based on FK pattern
            for fk_attr_name in fk_attrs:
                # Pattern: if "sensor_type_id" FK exists, remove "sensor_type" (string)
                if fk_attr_name.endswith("_id"):
                    base_name = fk_attr_name[:-3]  # Remove "_id"
                    if attr_name == base_name:
                        # Check if attr is NOT already a FK and is a string
                        is_fk = extract_attribute_field(attr, "is_foreign_key", False)
                        type_hint = extract_attribute_type_hint(attr)
                        
                        if not is_fk and (type_hint in ["string", "text", "varchar"] or not type_hint):
                            entity_attrs_to_remove.setdefault(entity_name, set()).add(attr_name)
                            logger.info(
                                f"Deterministic rule: Removing redundant attribute '{attr_name}' from entity '{entity_name}' "
                                f"(replaced by FK '{fk_attr_name}')"
                            )
                            break
    
    # Step 3: Actually remove the attributes
    for entity_name, attrs_to_remove_set in entity_attrs_to_remove.items():
        if entity_name not in entity_attributes:
            continue
        
        attrs = entity_attributes[entity_name]
        attrs_to_remove_list = []
        
        for attr in attrs:
            attr_name = extract_attribute_name(attr)
            if attr_name in attrs_to_remove_set:
                attrs_to_remove_list.append(attr)
        
        # Remove the attributes
        for attr in attrs_to_remove_list:
            if attr in attrs:
                attrs.remove(attr)
                logger.debug(f"Removed redundant attribute '{extract_attribute_name(attr)}' from entity '{entity_name}'")
    
    total_removed = sum(len(attrs) for attrs in entity_attrs_to_remove.values())
    if total_removed > 0:
        logger.info(f"Removed {total_removed} redundant relationship attribute(s) across {len(entity_attrs_to_remove)} entity(ies)")
    else:
        logger.debug("No redundant relationship attributes found to remove")
    
    return entity_attributes


@traceable_step("2.14", phase=2, tags=['phase_2_step_14'])
async def step_2_14_relation_realization(
    relation: Dict[str, Any],  # Relation from Step 1.9
    entities: List[Dict[str, Any]],  # All entities with descriptions
    entity_primary_keys: Dict[str, List[str]],  # entity_name -> primary key from Step 2.7
    entity_attributes: Dict[str, List[str]],  # entity_name -> all attributes from Steps 2.1-2.7
    relation_cardinalities: Optional[Dict[str, Dict[str, str]]] = None,  # relation_id -> entity_cardinalities from Step 1.11
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 2.14 (per-relation): Determine how a relationship is implemented.
    
    This is designed to be called in parallel for multiple relations.
    
    Args:
        relation: Relation dictionary from Step 1.9 with entities, type, description, arity, etc.
        entities: List of all entities with descriptions
        entity_primary_keys: Dictionary mapping entity names to their primary keys from Step 2.7
        entity_attributes: Dictionary mapping entity names to all their attributes
        relation_cardinalities: Optional dictionary mapping relation identifiers to entity cardinalities from Step 1.11
        nl_description: Optional original NL description
        domain: Optional domain context from Phase 1
        
    Returns:
        dict: Relation realization result with realization_type, realization_attrs, junction_table_name, etc.
        
    Example:
        >>> result = await step_2_14_relation_realization(
        ...     relation={"entities": ["Customer", "Order"], "type": "one-to-many"},
        ...     entities=[{"name": "Customer"}, {"name": "Order"}],
        ...     entity_primary_keys={"Customer": ["customer_id"], "Order": ["order_id"]},
        ...     entity_attributes={"Customer": ["customer_id", "name"], "Order": ["order_id", "customer_id"]}
        ... )
        >>> result["realization_type"]
        "foreign_key"
        >>> "customer_id" in result["realization_attrs"]
        True
    """
    relation_entities = relation.get("entities", [])
    relation_type = relation.get("type", "")
    relation_desc = relation.get("description", "")
    relation_arity = relation.get("arity", len(relation_entities))
    
    logger.debug(f"Realizing relation: {relation_type} among {relation_entities}")
    
    # Build enhanced context
    context_parts = []
    if domain:
        context_parts.append(f"Domain: {domain}")
    context_parts.append(f"Relation type: {relation_type}")
    context_parts.append(f"Relation arity: {relation_arity}")
    context_parts.append(f"Relation description: {relation_desc}")
    
    # Add entity information
    entity_info_parts = []
    for entity_name in relation_entities:
        entity_obj = find_entity_by_name(entities, entity_name)
        entity_desc = extract_entity_description(entity_obj) if entity_obj else ""
        
        pk = entity_primary_keys.get(entity_name, [])
        attrs = entity_attributes.get(entity_name, [])
        # Convert to list of attribute names if needed
        if attrs and isinstance(attrs[0], dict):
            attr_names = [attr.get("name", "") for attr in attrs]
        else:
            attr_names = attrs
        
        entity_info_parts.append(
            f"  {entity_name}: PK={pk}, attributes={attr_names}"
        )
        if entity_desc:
            entity_info_parts.append(f"    Description: {entity_desc}")
    
    context_parts.append(f"Entities in relation:\n" + "\n".join(entity_info_parts))
    
    # Add cardinality information if available
    if relation_cardinalities:
        relation_id = f"{'+'.join(sorted(relation_entities))}"
        card_info = relation_cardinalities.get(relation_id, {})
        if card_info:
            card_str = ", ".join(f"{ent}={card}" for ent, card in card_info.items())
            context_parts.append(f"Cardinalities: {card_str}")
    
    context_msg = "\n\nContext:\n" + "\n".join(f"- {part}" for part in context_parts)
    
    # System prompt
    system_prompt = """You are a database schema design expert. Your task is to determine how a relationship should be implemented in a relational database.

RELATION REALIZATION RULES:
1. BINARY RELATIONS (2 entities):
   - Use FOREIGN KEY: Add foreign key attribute(s) to one of the entities
   - For one-to-many (1:N): Foreign key goes in the "many" side (N side)
   - For many-to-one (N:1): Foreign key goes in the "many" side (N side)
     * Example: Customer (N) - Address (1) → FK address_id goes in Customer table
     * The entity with cardinality "N" gets the foreign key referencing the entity with cardinality "1"
   - For many-to-many: Use JUNCTION TABLE (special case of binary)
   - For one-to-one: Foreign key can go in either entity (choose based on business logic)

2. N-ARY RELATIONS (3+ entities):
   - Use JUNCTION TABLE: Create a new table with foreign keys to all participating entities
   - Junction table name should be descriptive (e.g., "OrderItem" for Order-Product relation)

3. FOREIGN KEY ATTRIBUTES:
   - Name should follow convention: {{referenced_entity}}_id (e.g., customer_id references Customer)
   - Reference the primary key of the referenced entity
   - Format: "attribute_name": "ReferencedEntity.primary_key_attribute"

4. REFERENTIAL INTEGRITY:
   - CASCADE: Delete/update propagates to related rows
   - SET_NULL: Set foreign key to NULL when referenced row is deleted
   - RESTRICT: Prevent delete/update if related rows exist
   - Choose based on business logic and data integrity requirements

5. EXISTENCE CHECK (CRITICAL):
   - Check if foreign key attributes already exist in the entity's attribute list
   - If an attribute exists with a similar name (e.g., "sensor_type" exists but we need "sensor_type_id"):
     * Determine if the existing attribute IS the foreign key (correct type, references the right entity)
     * OR if it's a different attribute (e.g., string "sensor_type" vs FK "sensor_type_id")
   - If the existing attribute is NOT a foreign key, you MUST specify a NEW attribute name for the FK
   - If no suitable attribute exists, specify the FK attribute name that needs to be CREATED
   - Set "exists": true only if the FK attribute already exists AND is correctly configured
   - Set "needs_creation": true if the FK attribute needs to be added to the entity's attribute list

6. RELATIONSHIP ATTRIBUTES:
   - Some relationships have attributes of their own (not attributes of the entities)
   - For example: "enrollment_date" for Student-Course relationship
   - These attributes belong to the RELATIONSHIP, not to either entity
   - For binary 1:N: Add relationship attributes to the N-side entity
   - For binary M:N or n-ary: Add relationship attributes to the junction table

7. REDUNDANT ATTRIBUTE IDENTIFICATION (CRITICAL):
   - If you create a new FK attribute (e.g., "sensor_type_id") because an existing attribute (e.g., "sensor_type") is a string and not a FK:
     * The existing string attribute is REDUNDANT and should be REMOVED
     * Add the redundant attribute name to "attributes_to_remove": ["sensor_type"]
   - Only remove if the existing attribute clearly represents the same relationship (same semantic meaning, just wrong type)
   - Example: If "sensor_type" (string) exists and you create "sensor_type_id" (FK), then "sensor_type" should be in attributes_to_remove
   - Do NOT remove if the existing attribute has different semantic meaning (e.g., "sensor_type" as category vs "sensor_type_id" as FK to SensorType entity)
   - If unsure, err on the side of NOT removing (can be cleaned up later)

Return a JSON object with:
- realization_type: "foreign_key" or "junction_table"
- realization_attrs: Dictionary mapping FK attribute names to referenced table.column (null for junction tables)
- junction_table_name: Name of junction table if needed (null for foreign keys)
- exists: Whether the FK attributes or junction table already exist
- needs_creation: Whether they need to be created
- referential_integrity: Dictionary mapping FK attribute names to integrity action (CASCADE, SET_NULL, RESTRICT, or null)
- attributes_to_remove: List of attribute names that are redundant and should be removed (empty list if none)
- reasoning: REQUIRED - Explanation of the realization decision (cannot be omitted)"""
    
    # Human prompt
    human_prompt_template = """Determine how to realize the relationship: {relation_type}

{context}

Natural Language Description:
{nl_description}

IMPORTANT: Check the entity attribute lists carefully. If an attribute with a similar name exists (e.g., "sensor_type" as a string), but you need a foreign key (e.g., "sensor_type_id"), you MUST:
1. Specify a NEW attribute name for the foreign key and set needs_creation=true
2. Add the redundant string attribute to "attributes_to_remove" (e.g., ["sensor_type"])

Return a JSON object specifying the realization type, foreign key attributes or junction table, referential integrity rules, attributes to remove, and reasoning."""
    
    try:
        # Get model for this step (important task)
        llm = get_model_for_step("2.14")
        
        # Invoke standardized LLM call
        config = get_trace_config("2.14", phase=2, tags=["phase_2_step_14"])
        result: RelationRealizationOutput = await standardized_llm_call(
            llm=llm,
            output_schema=RelationRealizationOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt_template,
            input_data={
                "relation_type": relation_type,
                "context": context_msg,
                "nl_description": nl_description or "",
            },
            config=config,
        )
        
        # Work with Pydantic model directly
        # Validate that referenced entities and primary keys exist
        realization_attrs = dict(result.realization_attrs) if result.realization_attrs else {}
        if realization_attrs:
            for fk_attr, ref in realization_attrs.items():
                # Parse reference (e.g., "Customer.customer_id")
                if "." in ref:
                    ref_entity, ref_attr = ref.split(".", 1)
                    if ref_entity not in relation_entities:
                        logger.warning(
                            f"Relation {relation_type}: Foreign key {fk_attr} references entity {ref_entity} "
                            f"which is not in the relation entities {relation_entities}"
                        )
                    elif ref_entity not in entity_primary_keys:
                        logger.warning(
                            f"Relation {relation_type}: Referenced entity {ref_entity} has no primary key defined"
                        )
                    elif ref_attr not in entity_primary_keys.get(ref_entity, []):
                        logger.warning(
                            f"Relation {relation_type}: Foreign key {fk_attr} references {ref} but {ref_attr} "
                            f"is not in the primary key of {ref_entity}"
                        )
        
        logger.debug(
            f"Relation {relation_type}: Realization type = {result.realization_type}, "
            f"needs_creation = {result.needs_creation}"
        )
        
        # Convert to dict only at return boundary
        return result.model_dump()
        
    except Exception as e:
        logger.error(f"Error realizing relation {relation_type}: {e}", exc_info=True)
        raise


async def step_2_14_relation_realization_batch(
    relations: List[Dict[str, Any]],  # Relations from Step 1.9
    entities: List[Dict[str, Any]],  # All entities with descriptions
    entity_primary_keys: Dict[str, List[str]],  # entity_name -> primary key from Step 2.7
    entity_attributes: Dict[str, List[str]],  # entity_name -> all attributes from Steps 2.1-2.7
    relation_cardinalities: Optional[Dict[str, Dict[str, str]]] = None,  # relation_id -> entity_cardinalities from Step 1.11
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 2.14: Realize all relations (parallel execution).
    
    Args:
        relations: List of relations from Step 1.9
        entities: List of all entities with descriptions
        entity_primary_keys: Dictionary mapping entity names to their primary keys from Step 2.7
        entity_attributes: Dictionary mapping entity names to all their attributes
        relation_cardinalities: Optional dictionary mapping relation identifiers to entity cardinalities from Step 1.11
        nl_description: Optional original NL description
        domain: Optional domain context from Phase 1
        
    Returns:
        dict: Relation realization results for all relations, keyed by relation identifier
    """
    logger.info(f"Starting Step 2.14: Relation Realization for {len(relations)} relations")
    
    if not relations:
        logger.warning("No relations provided for realization")
        return {"relation_results": {}}
    
    # Execute in parallel for all relations
    import asyncio
    
    tasks = []
    for relation in relations:
        task = step_2_14_relation_realization(
            relation=relation,
            entities=entities,
            entity_primary_keys=entity_primary_keys,
            entity_attributes=entity_attributes,
            relation_cardinalities=relation_cardinalities,
            nl_description=nl_description,
            domain=domain,
        )
        # Use relation identifier for tracking
        relation_entities = relation.get("entities", [])
        relation_id = f"{'+'.join(sorted(relation_entities))}"
        tasks.append((relation_id, task))
    
    # Wait for all tasks to complete
    results = await asyncio.gather(
        *[task for _, task in tasks],
        return_exceptions=True
    )
    
    # Process results
    relation_results = {}
    for i, ((relation_id, _), result) in enumerate(zip(tasks, results)):
        if isinstance(result, Exception):
            logger.error(f"Error processing relation {relation_id}: {result}")
            relation_results[relation_id] = {
                "realization_type": "foreign_key",
                "realization_attrs": None,
                "junction_table_name": None,
                "exists": False,
                "needs_creation": True,
                "referential_integrity": {},
                "reasoning": f"Error during analysis: {str(result)}"
            }
        else:
            relation_results[relation_id] = result
    
    total_foreign_keys = sum(
        1 for r in relation_results.values()
        if r.get("realization_type") == "foreign_key"
    )
    total_junction_tables = sum(
        1 for r in relation_results.values()
        if r.get("realization_type") == "junction_table"
    )
    logger.info(
        f"Relation realization completed: {total_foreign_keys} foreign key realizations and "
        f"{total_junction_tables} junction table realizations across {len(relation_results)} relations"
    )
    
    return {"relation_results": relation_results}
