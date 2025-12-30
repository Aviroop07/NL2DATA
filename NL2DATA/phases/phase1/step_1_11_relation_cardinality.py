"""Phase 1, Step 1.11: Relation Cardinality & Participation.

Determines relationship cardinality (1 or N) and participation (total or partial) for each entity in the relation.
"""

from typing import Dict, Any, List, Optional, Literal
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.phases.phase1.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.phases.phase1.utils import (
    extract_entity_name,
    build_entity_list_string,
)
from NL2DATA.utils.tools import validate_entity_cardinality, validate_relation_cardinality_output

logger = get_logger(__name__)


class RelationCardinalityOutput(BaseModel):
    """Output structure for relation cardinality and participation."""
    entity_cardinalities: Dict[str, Literal["1", "N"]] = Field(
        default_factory=dict,
        description="Dictionary mapping entity names to their cardinality in the relation ('1' for one, 'N' for many)"
    )
    entity_participations: Dict[str, Literal["total", "partial"]] = Field(
        default_factory=dict,
        description="Dictionary mapping entity names to their participation type ('total' means every instance must participate, 'partial' means some instances may not participate)"
    )
    reasoning: str = Field(description="Reasoning for the cardinality and participation decisions")

    model_config = ConfigDict(extra="forbid")


@traceable_step("1.11", phase=1, tags=["relation_cardinality"])
async def step_1_11_relation_cardinality_single(
    relation: Dict[str, Any],
    entities: List[Dict[str, Any]],
    nl_description: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 1.11 (per-relation): Determine cardinality and participation for each entity in a relation.
    
    This is designed to be called in parallel for multiple relations.
    
    Args:
        relation: Relation dictionary with entities, type, description, etc.
        entities: List of all entities for context
        nl_description: Optional original NL description
        
    Returns:
        dict: Cardinality and participation information with entity_cardinalities, entity_participations, and reasoning
        
    Example:
        >>> result = await step_1_11_relation_cardinality_single(
        ...     relation={"entities": ["Customer", "Order"], "type": "one-to-many"},
        ...     entities=[{"name": "Customer"}, {"name": "Order"}]
        ... )
        >>> result["entity_cardinalities"]["Customer"]
        "1"
        >>> result["entity_cardinalities"]["Order"]
        "N"
        >>> result["entity_participations"]["Order"]
        "total"
    """
    entities_in_rel = relation.get("entities", [])
    rel_type = relation.get("type", "unknown")
    rel_description = relation.get("description", "")
    
    # Create relation_id from entities for tracing
    relation_id = "_".join(sorted(entities_in_rel)) if entities_in_rel else "unknown"
    
    logger.debug(f"Analyzing cardinality and participation for relation: {', '.join(entities_in_rel)}")
    
    # Build entity context - filter entities that are in this relation
    relevant_entities = [
        entity for entity in entities
        if extract_entity_name(entity) in entities_in_rel
    ]
    entity_context_str = build_entity_list_string(
        relevant_entities,
        include_descriptions=True,
        prefix="- ",
    )
    
    # System prompt
    system_prompt = """You are a database design assistant. Your task is to determine the cardinality and participation of each entity participating in a relationship.

**Cardinality** indicates how many instances of one entity can relate to instances of another entity:
- **"1" (One)**: One instance of this entity participates in the relationship
- **"N" (Many)**: Many instances of this entity can participate in the relationship

**Participation** indicates whether every instance of an entity must participate in the relationship:
- **"total"**: Every instance of this entity MUST participate in the relationship (e.g., every order must belong to a customer)
- **"partial"**: Some instances of this entity may NOT participate in the relationship (e.g., not all customers place orders)

Examples:
- **One-to-Many (1:N)**: Customer (1, partial) - Order (N, total)
  - One customer can have many orders
  - Each order belongs to one customer
  - Not all customers place orders (Customer: partial)
  - Every order must belong to a customer (Order: total)
  - Result: {{"entity_cardinalities": {{"Customer": "1", "Order": "N"}}, "entity_participations": {{"Customer": "partial", "Order": "total"}}}}

- **Many-to-Many (N:M)**: Student (N, partial) - Course (N, partial)
  - Many students can enroll in many courses
  - Each course can have many students
  - Not all students enroll in all courses (Student: partial)
  - Not all courses have students enrolled (Course: partial)
  - Result: {{"entity_cardinalities": {{"Student": "N", "Course": "N"}}, "entity_participations": {{"Student": "partial", "Course": "partial"}}}}

- **Many-to-One (N:1) - CRITICAL**: Customer (N, partial) - Address (1, partial)
  - **MULTIPLE customers can share the SAME address** (e.g., family members, roommates)
  - Each address can be associated with many customers
  - Each customer has one address (for delivery purposes)
  - This is **NOT one-to-one** - it is **many-to-one** (customers are "N", address is "1")
  - Result: {{"entity_cardinalities": {{"Customer": "N", "Address": "1"}}, "entity_participations": {{"Customer": "partial", "Address": "partial"}}}}
  - **Common mistake**: Do NOT mark this as 1:1 just because "each customer has one address" - the key question is "can multiple customers share the same address?" If yes → many-to-one

- **One-to-One (1:1)**: User (1, partial) - Profile (1, total)
  - One user has one profile
  - Each profile belongs to one user
  - **CRITICAL**: In a true 1:1, instances are unique and cannot be shared
  - Not all users have profiles (User: partial)
  - Every profile must belong to a user (Profile: total)
  - Result: {{"entity_cardinalities": {{"User": "1", "Profile": "1"}}, "entity_participations": {{"User": "partial", "Profile": "total"}}}}

For each entity in the relationship, determine:
1. Cardinality: whether it participates as "1" or "N"
2. Participation: whether participation is "total" (every instance must participate) or "partial" (some instances may not participate)

**CRITICAL CARDINALITY RULES**:
- **Ask the key question**: "Can multiple instances of Entity A share/relate to the same instance of Entity B?"
  - If YES → Entity A is "N" (many), Entity B is "1" (one) → **Many-to-One**
  - If NO and reverse is also NO → Both are "1" → **One-to-One**
  - If YES in both directions → Both are "N" → **Many-to-Many**
- **Common mistake to avoid**: Do NOT confuse "each A has one B" with "one-to-one"
  - Example: "Each customer has one address" does NOT mean 1:1
  - The correct question: "Can multiple customers share the same address?" → YES → Many-to-One (Customer: N, Address: 1)
- **Real-world semantics matter**: Think about whether instances can be shared
  - Addresses, Categories, PaymentMethods, etc. are often shared → Many-to-One
  - Profiles, Passports, Social Security Numbers are unique → One-to-One

Important:
- Consider the relationship type and description
- Think about real-world semantics and whether instances can be shared
- For participation: ask "Must every instance of this entity participate?" If yes → total, if no → partial
- For ternary or n-ary relations, determine cardinality and participation for each entity independently
- Provide clear reasoning for your decisions, especially explaining why it's many-to-one vs one-to-one

CRITICAL: You MUST return ONLY valid JSON. Do NOT include any markdown formatting, explanations, or text outside the JSON object.

You have access to validation tools:
1) validate_entity_cardinality: validate that cardinality values are "1" or "N"
2) validate_relation_cardinality_output: validate completeness/allowed values for the full output

Return a JSON object with exactly these fields:
- entity_cardinalities: Dictionary mapping each entity name to "1" or "N"
- entity_participations: Dictionary mapping each entity name to "total" or "partial"
- reasoning: String with clear explanation of your cardinality and participation decisions

Example JSON output (NO markdown, NO text before/after):
{{"entity_cardinalities": {{"Customer": "1", "Order": "N"}}, "entity_participations": {{"Customer": "partial", "Order": "total"}}, "reasoning": "The relationship is one-to-many..."}}"""
    
    # Human prompt template
    # Note: Use format() placeholders for template variables, not f-string
    # to avoid conflicts with entity names that might contain special characters
    entities_str = ", ".join(entities_in_rel)
    human_prompt = """Relation:
- Entities: {entities_str}
- Type: {rel_type}
- Description: {rel_description}

Entity details:
{{entity_context}}

Original description (if available):
{{nl_description}}""".format(
        entities_str=entities_str,
        rel_type=rel_type,
        rel_description=rel_description or "No description",
    )
    
    # Initialize model
    llm = get_model_for_step("1.11")  # Step 1.11 maps to "high_fanout" task type
    
    try:
        config = get_trace_config("1.11", phase=1, tags=["relation_cardinality"], additional_metadata={"relation_id": relation_id})
        result: RelationCardinalityOutput = await standardized_llm_call(
            llm=llm,
            output_schema=RelationCardinalityOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={
                "entity_context": entity_context_str,
                "nl_description": nl_description or "",
            },
            tools=[validate_entity_cardinality, validate_relation_cardinality_output],
            use_agent_executor=True,  # Use agent executor for tool calls
            config=config,
        )
        
        # Work with Pydantic model directly
        cardinalities = result.entity_cardinalities
        participations = result.entity_participations
        logger.debug(
            f"Relation {', '.join(entities_in_rel)} cardinalities: {cardinalities}, participations: {participations}"
        )
        
        # Convert to dict only at return boundary
        return result.model_dump()
        
    except Exception as e:
        logger.error(
            f"Error analyzing cardinality for relation {', '.join(entities_in_rel)}: {e}",
            exc_info=True
        )
        raise


async def step_1_11_relation_cardinality(
    relations: List[Dict[str, Any]],
    entities: List[Dict[str, Any]],
    nl_description: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 1.11: Determine cardinality and participation for all relations (parallel execution).
    
    Args:
        relations: List of relations from Step 1.9
        entities: List of all entities for context
        nl_description: Optional original NL description
        
    Returns:
        dict: Cardinality and participation information for all relations
        
    Example:
        >>> result = await step_1_11_relation_cardinality(
        ...     relations=[{"entities": ["Customer", "Order"]}],
        ...     entities=[{"name": "Customer"}, {"name": "Order"}]
        ... )
        >>> len(result["relation_cardinalities"])
        1
    """
    logger.info(f"Starting Step 1.11: Relation Cardinality & Participation for {len(relations)} relations")
    
    if not relations:
        logger.warning("No relations provided for cardinality analysis")
        return {"relation_cardinalities": []}
    
    # Execute in parallel for all relations
    import asyncio
    
    tasks = []
    for relation in relations:
        task = step_1_11_relation_cardinality_single(
            relation=relation,
            entities=entities,
            nl_description=nl_description,
        )
        tasks.append(task)
    
    # Wait for all tasks to complete
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Process results
    relation_cardinalities = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"Error processing relation {i}: {result}")
            # Create a default entry for failed relations
            entities_in_rel = relations[i].get("entities", [])
            relation_cardinalities.append({
                "entities": entities_in_rel,
                "entity_cardinalities": {},
                "entity_participations": {},
                "reasoning": f"Error during analysis: {str(result)}"
            })
        else:
            # Add entity list to result for reference
            entities_in_rel = relations[i].get("entities", [])
            result["entities"] = entities_in_rel
            relation_cardinalities.append(result)
    
    logger.info(f"Relation cardinality and participation analysis completed for {len(relation_cardinalities)} relations")
    
    return {"relation_cardinalities": relation_cardinalities}

