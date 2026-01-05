"""Phase 2, Step 2.15: Relation Intrinsic Attributes.

For each relation, determine if the relation itself needs any intrinsic attributes
(attributes that belong to the relation, not to the entities it connects).

Examples:
- "enrollment_date" for Student-Course relation
- "quantity" for Order-Product relation
- "unit_price" for Order-Product relation (price at time of order)

CRITICAL: Only identify attributes that are intrinsic to the relation itself,
not attributes that belong to the entities.
"""

from typing import List, Optional
from pydantic import BaseModel, Field

from NL2DATA.phases.phase2.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.ir.models.state import AttributeInfo
from NL2DATA.utils.prompt_helpers import generate_output_structure_section_with_custom_requirements

logger = get_logger(__name__)


class RelationIntrinsicAttributesOutput(BaseModel):
    """Output structure for relation intrinsic attribute extraction."""
    relation_id: str = Field(description="Identifier for the relation (e.g., 'Customer+Order')")
    relation_attributes: List[AttributeInfo] = Field(
        default_factory=list,
        description="List of attributes that belong to the relation itself"
    )
    has_attributes: bool = Field(
        description="Whether the relation has any intrinsic attributes"
    )
    reasoning: str = Field(description="Explanation of why these attributes belong to the relation")


@traceable_step("2.15", phase=2, tags=['phase_2_step_15'])
async def step_2_15_relation_intrinsic_attributes(
    relation: dict,
    entity_intrinsic_attributes: dict,
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> RelationIntrinsicAttributesOutput:
    """
    Step 2.15 (per-relation): Identify attributes that belong to the relation itself.
    
    This is designed to be called in parallel for multiple relations.
    
    Args:
        relation: Relation dictionary from Step 1.9 with entities, type, description, arity, etc.
                  Should also include cardinalities and participations from Step 1.11
        entity_intrinsic_attributes: Dictionary mapping entity names to their intrinsic attributes
                                     (from Steps 2.2-2.5, after Step 2.14 cleanup)
        nl_description: Optional original NL description
        domain: Optional domain context from Phase 1
        
    Returns:
        dict: Relation intrinsic attributes result with relation_attributes list
        
    Example:
        >>> result = await step_2_15_relation_intrinsic_attributes(
        ...     relation={"entities": ["Order", "Product"], "type": "many-to-many"},
        ...     entity_intrinsic_attributes={
        ...         "Order": [{"name": "order_id"}, {"name": "order_date"}],
        ...         "Product": [{"name": "product_id"}, {"name": "price"}]
        ...     }
        ... )
        >>> "quantity" in [attr["name"] for attr in result["relation_attributes"]]
        True
    """
    relation_entities = relation.get("entities", [])
    relation_type = relation.get("type", "")
    relation_desc = relation.get("description", "")
    relation_arity = relation.get("arity", len(relation_entities))
    
    # Create relation identifier
    relation_id = f"{'+'.join(sorted(relation_entities))}"
    
    logger.debug(f"Extracting relation intrinsic attributes for: {relation_id}")
    
    # Build enhanced context
    context_parts = []
    
    # 1. Relation details
    context_parts.append(f"Relation: {relation_type}")
    if relation_desc:
        context_parts.append(f"Relation description: {relation_desc}")
    context_parts.append(f"Relation arity: {relation_arity}")
    context_parts.append(f"Entities involved: {', '.join(relation_entities)}")
    
    # 2. Cardinality and participation (if available)
    entity_cardinalities = relation.get("entity_cardinalities", {})
    entity_participations = relation.get("entity_participations", {})
    
    if entity_cardinalities:
        card_str = ", ".join(f"{ent}={card}" for ent, card in entity_cardinalities.items())
        context_parts.append(f"Cardinalities: {card_str}")
    
    if entity_participations:
        part_str = ", ".join(f"{ent}={part}" for ent, part in entity_participations.items())
        context_parts.append(f"Participations: {part_str}")
    
    # 3. Intrinsic attributes of all entities involved
    entity_attr_details = []
    for entity_name in relation_entities:
        entity_attrs = entity_intrinsic_attributes.get(entity_name, [])
        if entity_attrs:
            attr_names = [attr.get("name", "") if isinstance(attr, dict) else getattr(attr, "name", "") for attr in entity_attrs]
            entity_attr_details.append(f"  {entity_name}: {', '.join(attr_names)}")
        else:
            entity_attr_details.append(f"  {entity_name}: (no attributes)")
    
    if entity_attr_details:
        context_parts.append(f"Intrinsic attributes of entities:\n" + "\n".join(entity_attr_details))
    
    # 4. Domain context
    if domain:
        context_parts.append(f"Domain: {domain}")
    
    context_msg = "\n\nEnhanced Context:\n" + "\n".join(f"- {part}" for part in context_parts)
    
    # Generate output structure section from Pydantic model
    output_structure_section = generate_output_structure_section_with_custom_requirements(
        output_schema=RelationIntrinsicAttributesOutput,
        additional_requirements=[
            "The \"reasoning\" field in each relation attribute is REQUIRED and cannot be omitted",
            "Reasoning is REQUIRED and cannot be omitted"
        ]
    )
    
    # System prompt with explicit instruction and detailed example
    system_prompt = """You are a database schema design expert. Your task is to identify attributes that belong to a RELATIONSHIP itself, not to the entities it connects.

**CRITICAL INSTRUCTION**: Identify attributes that belong to the relation itself, not to the entities. These are attributes that describe properties of the relationship (e.g., when it was established, quantity in the relationship, status of the relationship).

**WHAT ARE RELATION ATTRIBUTES?**
Relation attributes are properties that belong to the RELATIONSHIP itself, not to either entity. Examples:
- "enrollment_date" for Student-Course relation (when the student enrolled in the course)
- "quantity" for Order-Product relation (how many of this product in the order)
- "unit_price" for Order-Product relation (price per unit at time of order, which may differ from Product's current price)
- "grade" for Student-Course relation (the student's grade in this course)
- "start_date" and "end_date" for Employee-Department relation (when employee joined/left department)

**WHAT ARE NOT RELATION ATTRIBUTES?**
- Attributes that belong to entities (e.g., "name" belongs to Student, "title" belongs to Course) - these are already in entity_intrinsic_attributes
- Foreign keys (e.g., "student_id", "course_id") - these are handled automatically in Step 3.5
- Primary keys of entities - these belong to the entities themselves

**DETAILED EXAMPLE**:

Context provided:
- Relation: Order-Product (M:N relation)
- Entities involved: Order, Product
- Relation description: "Order contains Product"
- Cardinalities: Order (N), Product (N)
- Intrinsic attributes of Order: ["order_id", "order_date", "total_amount", "status"]
- Intrinsic attributes of Product: ["product_id", "title", "author", "price"]

**EXPECTED OUTPUT**:
```json
{
  "relation_id": "Order+Product",
  "relation_attributes": [
    {
      "name": "quantity",
      "description": "Number of copies of this product in the order",
      "type_hint": "integer",
      "reasoning": "Quantity is a property of the Order-Product relationship, not of Order or Product individually"
    },
    {
      "name": "unit_price",
      "description": "Price per unit at the time of order",
      "type_hint": "decimal",
      "reasoning": "Price at time of order may differ from Product's current price, so it's a property of the relationship"
    }
  ],
  "has_attributes": true,
  "reasoning": "The Order-Product relation needs quantity (how many of this product in the order) and unit_price (price at time of order, which may differ from Product's current price). These are intrinsic to the relationship itself."
}
```

**Explanation**: The LLM correctly identifies "quantity" and "unit_price" as relation attributes. These belong to the Order-Product relationship, not to Order or Product individually. These will be stored in a junction table during schema compilation.

**RULES**:
1. **DO identify relation attributes** - Attributes that describe the relationship itself
2. **DO NOT identify entity attributes** - These are already in entity_intrinsic_attributes
3. **DO NOT identify foreign keys** - These are handled automatically
4. **Consider temporal attributes** - When the relationship was established, modified, or ended
5. **Consider quantitative attributes** - Quantities, amounts, counts in the relationship
6. **Consider qualitative attributes** - Status, grade, rating of the relationship

For each relation attribute, provide:
- name: Clear, singular attribute name (use snake_case for SQL compatibility)
- description: What this attribute represents in the context of the relationship
- type_hint: Suggested data type hint (e.g., "string", "integer", "decimal", "date", "boolean", "timestamp")
- reasoning: REQUIRED - Why this attribute belongs to the relation, not to the entities (cannot be omitted)

""" + output_structure_section
    
    # Human prompt template
    human_prompt_template = """Identify attributes that belong to the relation itself, not to the entities.

{context}

Natural Language Description:
{nl_description}

Return a JSON object specifying which attributes (if any) belong to the relation itself."""
    
    try:
        # Get model for this step
        llm = get_model_for_step("2.15")
        
        # Invoke standardized LLM call
        config = get_trace_config("2.15", phase=2, tags=["phase_2_step_15"])
        result: RelationIntrinsicAttributesOutput = await standardized_llm_call(
            llm=llm,
            output_schema=RelationIntrinsicAttributesOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt_template,
            input_data={
                "context": context_msg,
                "nl_description": nl_description or "",
            },
            config=config,
        )
        
        # Set relation_id if not set
        if not result.relation_id:
            result = RelationIntrinsicAttributesOutput(
                relation_id=relation_id,
                relation_attributes=result.relation_attributes,
                has_attributes=result.has_attributes,
                reasoning=result.reasoning
            )
        
        logger.debug(
            f"Relation {relation_id}: extracted {len(result.relation_attributes)} relation attributes: "
            f"{', '.join([attr.name for attr in result.relation_attributes])}"
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error extracting relation attributes for {relation_id}: {e}", exc_info=True)
        raise


class RelationIntrinsicAttributesBatchOutput(BaseModel):
    """Output structure for Step 2.15 batch processing."""
    relation_results: List[RelationIntrinsicAttributesOutput] = Field(
        description="List of relation intrinsic attribute extraction results, one per relation"
    )
    total_relations: int = Field(description="Total number of relations processed")


async def step_2_15_relation_intrinsic_attributes_batch(
    relations: List,
    entity_intrinsic_attributes: dict,
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> RelationIntrinsicAttributesBatchOutput:
    """
    Step 2.15: Extract relation intrinsic attributes for all relations (parallel execution).
    
    Args:
        relations: List of relations from Phase 1 (Steps 1.9, 1.11)
        entity_intrinsic_attributes: Dictionary mapping entity names to their intrinsic attributes
                                     (from Steps 2.2-2.5, after Step 2.14 cleanup)
        nl_description: Optional original NL description
        domain: Optional domain context from Phase 1
        
    Returns:
        dict: Relation intrinsic attributes results for all relations, keyed by relation identifier
    """
    logger.info(f"Starting Step 2.15: Relation Intrinsic Attributes for {len(relations)} relations")
    
    if not relations:
        logger.warning("No relations provided for relation attribute extraction")
        return RelationIntrinsicAttributesBatchOutput(relation_results=[], total_relations=0)
    
    # Execute in parallel for all relations
    import asyncio
    
    tasks = []
    for relation in relations:
        task = step_2_15_relation_intrinsic_attributes(
            relation=relation,
            entity_intrinsic_attributes=entity_intrinsic_attributes,
            nl_description=nl_description,
            domain=domain,
        )
        tasks.append(task)
    
    # Wait for all tasks to complete
    results = await asyncio.gather(
        *tasks,
        return_exceptions=True
    )
    
    # Process results
    relation_results_list = []
    for result in results:
        if isinstance(result, Exception):
            logger.error(f"Error processing relation: {result}")
            relation_results_list.append(
                RelationIntrinsicAttributesOutput(
                    relation_id="unknown",
                    relation_attributes=[],
                    has_attributes=False,
                    reasoning=f"Error during analysis: {str(result)}"
                )
            )
        else:
            relation_results_list.append(result)
    
    total_relations_with_attrs = sum(
        1 for r in relation_results_list
        if r.has_attributes
    )
    total_attrs = sum(
        len(r.relation_attributes)
        for r in relation_results_list
    )
    logger.info(
        f"Relation intrinsic attribute extraction completed: {total_relations_with_attrs}/{len(relation_results_list)} "
        f"relations have attributes, {total_attrs} total relation attributes"
    )
    
    return RelationIntrinsicAttributesBatchOutput(
        relation_results=relation_results_list,
        total_relations=len(relations),
    )

