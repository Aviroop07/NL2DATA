"""Phase 2, Step 2.2: Intrinsic Attributes.

Extracts attributes that are inherent to the entity (not relationship-based).
These form the core columns of each table.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

from NL2DATA.phases.phase2.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.ir.models.state import AttributeInfo

logger = get_logger(__name__)


class IntrinsicAttributesOutput(BaseModel):
    """Output structure for intrinsic attribute extraction."""
    attributes: List[AttributeInfo] = Field(
        description="List of intrinsic attributes with name, description, type_hint, and reasoning"
    )


@traceable_step("2.2", phase=2, tags=['phase_2_step_2'])
async def step_2_2_intrinsic_attributes(
    entity_name: str,
    nl_description: str,
    entity_description: Optional[str] = None,
    explicit_attributes: Optional[List[str]] = None,
    domain: Optional[str] = None,
    relations: Optional[List[Dict[str, Any]]] = None,
    primary_key: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Step 2.2 (per-entity): Extract attributes that are inherent to the entity.
    
    This is designed to be called in parallel for multiple entities.
    
    **CRITICAL**: Must explicitly instruct LLM to NOT generate any attributes that relate 
    entities through relations. Only generate intrinsic attributes that describe the entity itself.
    
    Args:
        entity_name: Name of the entity
        nl_description: Natural language description of the database requirements (FULL description)
        entity_description: Optional description of the entity from Step 1.4
        explicit_attributes: Optional list of explicitly mentioned attributes from Step 2.1
        domain: Optional domain context from Phase 1 (Steps 1.1-1.3)
        relations: Optional list of relations this entity participates in (from Steps 1.9, 1.11)
                  Each relation dict should have: entities, type, description, cardinalities, participations
        primary_key: Optional current primary key from Step 2.7 (if available)
        
    Returns:
        dict: Intrinsic attributes result with attributes list
        
    Example:
        >>> result = await step_2_2_intrinsic_attributes(
        ...     "Customer",
        ...     "Customers have name, email, and address",
        ...     entity_description="A customer who places orders",
        ...     domain="e-commerce",
        ...     relations=[{"entities": ["Customer", "Order"], "type": "one-to-many"}],
        ...     primary_key=["customer_id"]
        ... )
        >>> len(result["attributes"])
        3
        >>> result["attributes"][0]["name"]
        "name"
    """
    logger.debug(f"Extracting intrinsic attributes for entity: {entity_name}")
    
    # Build enhanced context
    context_parts = []
    
    # 1. Domain context
    if domain:
        context_parts.append(f"Domain: {domain}")
    
    # 2. Entity description
    if entity_description:
        context_parts.append(f"Entity description: {entity_description}")
    
    # 3. Current primary key (if available)
    if primary_key:
        context_parts.append(f"Current primary key: {', '.join(primary_key)} (will be determined in Step 2.7 if not yet set)")
    else:
        context_parts.append("Primary key: Will be determined in Step 2.7")
    
    # 4. All relations this entity participates in with full details
    if relations:
        relation_details = []
        for rel in relations:
            rel_entities = rel.get("entities", [])
            rel_type = rel.get("type", "")
            rel_desc = rel.get("description", "")
            rel_cardinalities = rel.get("entity_cardinalities", {})
            rel_participations = rel.get("entity_participations", {})
            
            rel_info = f"  - Relation: {rel_type}"
            if rel_desc:
                rel_info += f" ({rel_desc})"
            rel_info += f"\n    Entities: {', '.join(rel_entities)}"
            
            if rel_cardinalities:
                # Format cardinalities without braces that could interfere with template formatting
                card_items = [f"{ent}={card}" for ent, card in rel_cardinalities.items()]
                card_str = ", ".join(card_items)
                rel_info += f"\n    Cardinalities: {card_str}"
            
            if rel_participations:
                # Format participations without braces that could interfere with template formatting
                part_items = [f"{ent}={part}" for ent, part in rel_participations.items()]
                part_str = ", ".join(part_items)
                rel_info += f"\n    Participations: {part_str}"
            
            relation_details.append(rel_info)
        
        if relation_details:
            context_parts.append(f"Relations this entity participates in:\n" + "\n".join(relation_details))
    
    # 5. Explicit attributes
    if explicit_attributes:
        context_parts.append(f"Explicitly mentioned attributes: {', '.join(explicit_attributes)}")
    
    context_msg = ""
    if context_parts:
        context_msg = "\n\nEnhanced Context:\n" + "\n".join(f"- {part}" for part in context_parts)
        # Escape any braces in context_msg to prevent format string errors when human_prompt is formatted
        # This is necessary because context_msg might contain dictionary representations or other content with braces
        # We need to escape them so they're treated as literal braces, not format placeholders
        context_msg = context_msg.replace("{", "{{").replace("}", "}}")
    
    # System prompt with explicit instruction and detailed example
    # Note: JSON examples in the prompt use double braces {{ }} to escape them from template parsing
    system_prompt = """You are a database design assistant. Your task is to extract all intrinsic attributes (properties/columns) that are inherent to an entity.

**CRITICAL INSTRUCTION**: DO NOT generate any attributes that connect this entity to other entities through relations. Only generate intrinsic attributes that describe properties of the entity itself. Foreign keys and relation-connecting attributes will be handled separately in later steps.

Intrinsic attributes are properties that belong directly to the entity itself, not relationships with other entities. Examples:
- Customer: name, email, phone, address, date_of_birth
- Order: order_id, order_date, total_amount, status
- Product: product_id, name, price, description, category

Important distinctions:
- **Intrinsic attributes**: Properties of the entity itself (e.g., Customer.name, Order.date, Product.price)
- **Relation-connecting attributes**: Attributes that connect entities through relations (e.g., Order.customer_id, Product.category_id) - DO NOT GENERATE THESE
- **Relationship attributes**: Attributes that belong to the relationship itself (e.g., enrollment_date for Student-Course) - these are handled in Step 2.15

**DETAILED EXAMPLE**:

Context provided:
- Full NL description: "I need a database for an online bookstore. Customers can place orders for books. Each order can contain multiple books. Books have titles, authors, and prices. I need to track customer addresses and order dates."
- Domain: "e-commerce" / "online bookstore"
- Entity: "Customer"
- Entity description: "A customer who places orders"
- Relations Customer participates in:
  - Customer-Order: Customer places Order (1:N, Customer partial, Order total)
  - Customer-Card: Customer owns Card (1:N, Customer partial, Card total)
- Current primary key: "Will be determined in Step 2.7"

**CORRECT OUTPUT** (only intrinsic attributes):
```json
{{
  "attributes": [
    {{
      "name": "customer_id",
      "description": "Unique identifier for the customer",
      "type_hint": "integer",
      "reasoning": "Intrinsic identifier for Customer entity"
    }},
    {{
      "name": "name",
      "description": "Customer's full name",
      "type_hint": "string",
      "reasoning": "Intrinsic property describing the customer"
    }},
    {{
      "name": "email",
      "description": "Customer's email address",
      "type_hint": "string",
      "reasoning": "Intrinsic contact information"
    }},
    {{
      "name": "address",
      "description": "Customer's physical address",
      "type_hint": "string",
      "reasoning": "Explicitly mentioned in NL description as something to track"
    }},
    {{
      "name": "phone",
      "description": "Customer's phone number",
      "type_hint": "string",
      "reasoning": "Common intrinsic contact information"
    }}
  ]
}}
```

**INCORRECT OUTPUT** (includes relation-connecting attributes - DO NOT DO THIS):
```json
{{
  "attributes": [
    {{"name": "customer_id", ...}},
    {{"name": "name", ...}},
    {{"name": "email", ...}},
    {{"name": "order_id", ...}},  // WRONG: This connects Customer to Order
    {{"name": "card_id", ...}}     // WRONG: This connects Customer to Card
  ]
}}
```

**Explanation**: The LLM correctly generates only intrinsic attributes (customer_id, name, email, address, phone). It does NOT generate "order_id" or "card_id" because those would be relation-connecting attributes. The Customer's relationship to Order and Card will be handled through foreign keys in the Order and Card tables, not in Customer.

For each attribute, provide:
- name: Clear, singular attribute name (use snake_case for SQL compatibility)
- description: What this attribute represents
- type_hint: Suggested data type hint (e.g., "string", "integer", "decimal", "date", "boolean", "timestamp")
- reasoning: REQUIRED - Why this attribute is needed for the entity (cannot be omitted)

Consider:
- Explicitly mentioned attributes in the description
- Common attributes for entities in this domain
- Attributes needed for the entity to be useful
- Standard patterns (e.g., IDs, timestamps, status fields)
- The relations this entity participates in (to understand what NOT to include)

Do NOT include:
- Foreign key attributes (these are added automatically in Step 3.5)
- Relation-connecting attributes (e.g., customer_id in Customer, order_id in Customer)
- Relationship properties (these are handled separately in Step 2.15)
- Derived attributes (these are identified in Step 2.8)

Return a comprehensive list of all intrinsic attributes for the entity."""
    
    # Human prompt template - use single braces for template variable
    # Note: We format entity_name and context_msg here, but nl_description is passed via input_data
    human_prompt = f"""Entity: {entity_name}{context_msg}

Natural language description:
{{nl_description}}"""
    
    # Initialize model
    llm = get_model_for_step("2.2")  # Step 2.2 maps to "high_fanout" task type
    
    try:
        config = get_trace_config("2.2", phase=2, tags=["intrinsic_attributes"])
        result: IntrinsicAttributesOutput = await standardized_llm_call(
            llm=llm,
            output_schema=IntrinsicAttributesOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={"nl_description": nl_description},
            config=config,
        )
        
        # Work with Pydantic model directly
        attributes = result.attributes
        attribute_names = [attr.name for attr in attributes]
        
        logger.debug(
            f"Entity {entity_name}: extracted {len(attributes)} intrinsic attributes: "
            f"{', '.join(attribute_names)}"
        )
        
        # Convert to dict only at return boundary
        return result.model_dump()
        
    except Exception as e:
        logger.error(f"Error extracting intrinsic attributes for entity {entity_name}: {e}", exc_info=True)
        raise


async def step_2_2_intrinsic_attributes_batch(
    entities: List[Dict[str, Any]],
    nl_description: str,
    attribute_count_results: Optional[Dict[str, Dict[str, Any]]] = None,
    domain: Optional[str] = None,
    relations: Optional[List[Dict[str, Any]]] = None,
    primary_keys: Optional[Dict[str, List[str]]] = None,
) -> Dict[str, Any]:
    """
    Step 2.2: Extract intrinsic attributes for all entities (parallel execution).
    
    Args:
        entities: List of entities with name and description
        nl_description: Natural language description (FULL description)
        attribute_count_results: Optional results from Step 2.1 (entity_results dict)
        domain: Optional domain context from Phase 1 (Steps 1.1-1.3)
        relations: Optional list of all relations from Phase 1 (Steps 1.9, 1.11)
                   Used to find relations each entity participates in
        primary_keys: Optional dictionary mapping entity names to primary keys from Step 2.7
        
    Returns:
        dict: Intrinsic attributes results for all entities, keyed by entity name
        
    Example:
        >>> result = await step_2_2_intrinsic_attributes_batch(
        ...     entities=[{"name": "Customer"}],
        ...     nl_description="Customers have name and email"
        ... )
        >>> "Customer" in result["entity_results"]
        True
    """
    logger.info(f"Starting Step 2.2: Intrinsic Attributes for {len(entities)} entities")
    
    if not entities:
        logger.warning("No entities provided for intrinsic attribute extraction")
        return {"entity_results": {}}
    
    # Execute in parallel for all entities
    import asyncio
    
    tasks = []
    for entity in entities:
        entity_name = entity.get("name", "Unknown") if isinstance(entity, dict) else getattr(entity, "name", "Unknown")
        entity_desc = entity.get("description", "") if isinstance(entity, dict) else getattr(entity, "description", "")
        
        # Get explicit attributes from Step 2.1 if available
        explicit_attrs = None
        if attribute_count_results and entity_name in attribute_count_results.get("entity_results", {}):
            explicit_attrs = attribute_count_results["entity_results"][entity_name].get("explicit_attributes")
        
        # Find relations this entity participates in
        entity_relations = None
        if relations:
            entity_relations = [
                rel for rel in relations
                if entity_name in rel.get("entities", [])
            ]
        
        # Get primary key for this entity if available
        entity_pk = None
        if primary_keys and entity_name in primary_keys:
            entity_pk = primary_keys[entity_name]
        
        task = step_2_2_intrinsic_attributes(
            entity_name=entity_name,
            nl_description=nl_description,
            entity_description=entity_desc,
            explicit_attributes=explicit_attrs,
            domain=domain,
            relations=entity_relations,
            primary_key=entity_pk,
        )
        tasks.append((entity_name, task))
    
    # Wait for all tasks to complete
    results = await asyncio.gather(
        *[task for _, task in tasks],
        return_exceptions=True
    )
    
    # Process results
    entity_results = {}
    for i, ((entity_name, _), result) in enumerate(zip(tasks, results)):
        if isinstance(result, Exception):
            logger.error(f"Error processing entity {entity_name}: {result}")
            entity_results[entity_name] = {"attributes": []}
        else:
            entity_results[entity_name] = result
    
    total_attributes = sum(len(r.get("attributes", [])) for r in entity_results.values())
    logger.info(f"Intrinsic attribute extraction completed: {total_attributes} total attributes across {len(entity_results)} entities")
    
    return {"entity_results": entity_results}

