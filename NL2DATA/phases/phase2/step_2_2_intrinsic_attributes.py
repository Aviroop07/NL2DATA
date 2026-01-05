"""Phase 2, Step 2.2: Intrinsic Attributes.

Extracts attributes that are inherent to the entity (not relationship-based).
These form the core columns of each table.
"""

from typing import Dict, Any, List, Optional, Set
import json
import re
from pydantic import BaseModel, Field

from NL2DATA.phases.phase2.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.ir.models.state import AttributeInfo
from NL2DATA.utils.pipeline_config import get_phase2_config
from NL2DATA.utils.prompt_helpers import generate_output_structure_section_with_custom_requirements

logger = get_logger(__name__)


class IntrinsicAttributesOutput(BaseModel):
    """Output structure for intrinsic attribute extraction."""
    attributes: List[AttributeInfo] = Field(
        description="List of intrinsic attributes with name, description, type_hint, and reasoning"
    )


def _norm_text(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()


def _entity_tokens(entity_name: str) -> Set[str]:
    """Conservative tokens for entity-name matching."""
    base = _norm_text(entity_name)
    tokens: Set[str] = set()
    if base:
        tokens.add(base)
        tokens.add(base.replace(" ", ""))  # "sales funnel stage" -> "salesfunnelstage"
    return tokens


def _detect_intrinsic_attribute_issues(
    *,
    entity_name: str,
    attributes: List[AttributeInfo],
    all_entity_names: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Detect issues deterministically; do NOT mutate attributes.

    The returned issues are fed back to the LLM so the LLM decides the final list.
    """
    issues: List[Dict[str, Any]] = []
    seen_names: Set[str] = set()

    # Build other entity tokens, but exclude tokens that are part of the current entity name
    # This prevents false positives when entity names contain other entity names
    # (e.g., "ProductCategory" contains "Product", but we shouldn't flag attributes mentioning "product")
    entity_name_tokens = _entity_tokens(entity_name)
    other_entity_tokens: Set[str] = set()
    for en in (all_entity_names or []):
        if not en or en == entity_name:
            continue
        en_tokens = _entity_tokens(en)
        # Only add tokens that are NOT part of the current entity name
        # This prevents "ProductCategory" from flagging attributes that mention "product"
        for tok in en_tokens:
            if tok and tok not in entity_name_tokens:
                other_entity_tokens.add(tok)

    for idx, attr in enumerate(attributes):
        name = (attr.name or "").strip()
        name_lc = name.lower()
        blob = _norm_text(" ".join([name, attr.description or "", attr.reasoning or ""]))
        blob_compact = blob.replace(" ", "")

        # Duplicate name within entity
        if name_lc in seen_names:
            issues.append(
                {
                    "attribute": name,
                    "issue_type": "duplicate_name",
                    "detail": "Duplicate attribute name within the same entity (case-insensitive).",
                    "index": idx,
                }
            )
        seen_names.add(name_lc)

        # Name validity (lightweight)
        if name and not re.fullmatch(r"[a-z][a-z0-9_]*", name):
            issues.append(
                {
                    "attribute": name,
                    "issue_type": "invalid_name_format",
                    "detail": "Attribute name should be snake_case (letters/numbers/underscore) and start with a letter.",
                    "index": idx,
                }
            )

        # Check for FK-like attribute names (ends with _id and matches another entity)
        # Exception: entity's own ID (e.g., customer_id for Customer entity) is OK
        entity_name_lower = entity_name.lower()
        entity_base = entity_name_lower.replace("entity", "").replace("table", "").strip()
        is_own_id = (name_lc.endswith("_id") and 
                    (entity_base in name_lc or name_lc.startswith(entity_base + "_id")))
        
        # Mention of other entities (name/description/reasoning)
        # Skip if it's the entity's own ID attribute
        if not is_own_id:
            for tok in other_entity_tokens:
                if tok and (tok in blob or tok in blob_compact):
                    # Additional check: if attribute name ends with _id and matches another entity, it's definitely a FK
                    if name_lc.endswith("_id"):
                        other_entity_base = tok.replace("entity", "").replace("table", "").strip()
                        if other_entity_base in name_lc or name_lc.startswith(other_entity_base + "_id"):
                            issues.append(
                                {
                                    "attribute": name,
                                    "issue_type": "foreign_key_like_attribute",
                                    "detail": f"Attribute name '{name}' ends with '_id' and appears to reference another entity '{tok}'. This is a relation-connecting attribute and will be handled automatically in later steps. DO NOT include it here.",
                                    "index": idx,
                                }
                            )
                            break
                    else:
                        issues.append(
                            {
                                "attribute": name,
                                "issue_type": "mentions_other_entity",
                                "detail": f"Attribute name/description/reasoning appears to reference another entity token '{tok}'. Only include attributes that are intrinsic to this entity.",
                                "index": idx,
                            }
                        )
                        break

        # Collection-like attributes (nested structure smell)
        if any(p in blob for p in ["list of", "array", "json", "collection", "events that occurred"]):
            issues.append(
                {
                    "attribute": name,
                    "issue_type": "collection_like_attribute",
                    "detail": "Attribute looks like a nested collection (list/array/json). This often indicates it should be modeled as a separate entity/table.",
                    "index": idx,
                }
            )

    return issues


@traceable_step("2.2", phase=2, tags=['phase_2_step_2'])
async def step_2_2_intrinsic_attributes(
    entity_name: str,
    nl_description: str,
    entity_description: Optional[str] = None,
    explicit_attributes: Optional[List[str]] = None,
    domain: Optional[str] = None,
    relations: Optional[List] = None,
    primary_key: Optional[List[str]] = None,
    all_entity_names: Optional[List[str]] = None,
) -> IntrinsicAttributesOutput:
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
    
    # 5. Entity registry (helps avoid "attribute leakage" from connected entities)
    if all_entity_names:
        context_parts.append("All entities in schema: " + ", ".join([n for n in all_entity_names if n]))

    # 6. Explicit attributes
    if explicit_attributes:
        context_parts.append(f"Explicitly mentioned attributes: {', '.join(explicit_attributes)}")
    
    context_msg = ""
    if context_parts:
        context_msg = "\n\nEnhanced Context:\n" + "\n".join(f"- {part}" for part in context_parts)
        # Escape any braces in context_msg to prevent format string errors when human_prompt is formatted
        # This is necessary because context_msg might contain dictionary representations or other content with braces
        # We need to escape them so they're treated as literal braces, not format placeholders
        context_msg = context_msg.replace("{", "{{").replace("}", "}}")
    
    # Generate output structure section from Pydantic model
    output_structure_section = generate_output_structure_section_with_custom_requirements(
        output_schema=IntrinsicAttributesOutput,
        additional_requirements=[
            'The "reasoning" field is REQUIRED and cannot be empty or omitted'
        ]
    )
    
    # System prompt with explicit instruction and detailed example
    # Note: JSON examples in the prompt use double braces {{ }} to escape them from template parsing
    system_prompt = """You are a database design assistant. Your task is to extract all intrinsic attributes (properties/columns) that are inherent to an entity.

**CRITICAL REQUIREMENT**: You MUST return at least ONE attribute. An empty attributes list is not acceptable and will cause the pipeline to fail. Every entity must have at least one intrinsic attribute, even if minimal (e.g., an identifier attribute like entity_id or id, or at least one descriptive attribute).

**CRITICAL INSTRUCTION**: DO NOT generate any attributes that connect this entity to other entities through relations. Only generate intrinsic attributes that describe properties of the entity itself. Foreign keys and relation-connecting attributes will be handled separately in later steps.

Additional critical constraints:
- Do NOT output attributes that are collections / nested structures (e.g., lists of events, arrays, JSON blobs). If the description implies repeated records (events), that should be modeled as its own entity/table.
- Do NOT output attributes whose NAME suggests a foreign key connection to ANOTHER entity (e.g., if you are generating attributes for "Store" entity, do NOT include `product_id`, `customer_id`, `order_id`). The ONLY exception is the entity's own ID attribute (e.g., `store_id` for Store entity is OK, but `product_id` in Store is NOT OK).
- Rule of thumb: If an attribute name ends with `_id` and the prefix matches another entity name (not this entity), it is a foreign key and MUST NOT be included.
- It is OK to mention other entities in the attribute DESCRIPTION if it helps clarify the attribute's purpose (e.g., "The fare amount charged to the rider" is fine, even though it mentions "rider"). The issue is when the ATTRIBUTE NAME itself suggests a connection to another entity (e.g., `rider_id` when the current entity is not "Rider").

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

CRITICAL: SCHEMA ANCHORED VALIDATION
Before outputting any attribute name:
1. Check if it already exists in the provided schema context
2. Use EXACT names from the schema (case-sensitive) if the attribute already exists
3. Do NOT invent new names unless explicitly creating new components
4. If "All entities in schema" is provided, ensure attribute names don't conflict with entity names

EXAMPLES:
❌ BAD: Outputting "unit_price" when schema has "unit_price_value"
❌ BAD: Outputting "customer" when schema has "customer_id" (use exact name)
✅ GOOD: Using exact names from schema: "unit_price_value", "customer_id"
✅ GOOD: Creating new attribute "discount_percentage" if not in schema

COMMON MISTAKES TO AVOID:
1. ❌ Using non-existent attribute names (check schema first)
2. ❌ Mixing attribute names (e.g., "price" vs "unit_price" vs "price_value")
3. ❌ Inventing DSL functions not in allowlist
4. ❌ Suggesting aggregate queries as derived attributes
5. ❌ Including relation-connecting attributes (foreign keys)

If unsure, err on the side of conservatism (use existing names from context).

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

""" + output_structure_section + """

Return a comprehensive list of all intrinsic attributes for the entity. REMEMBER: You MUST return at least one attribute. An empty list is not acceptable."""
    
    # Human prompt template - use single braces for template variable
    # Note: We format entity_name and context_msg here, but nl_description is passed via input_data
    human_prompt = f"""Entity: {entity_name}{context_msg}

Natural language description:
{{nl_description}}"""
    
    # Initialize model
    llm = get_model_for_step("2.2")  # Step 2.2 maps to "high_fanout" task type
    
    cfg = get_phase2_config()
    try:
        config = get_trace_config("2.2", phase=2, tags=["intrinsic_attributes"])

        # Initial extraction
        result: IntrinsicAttributesOutput = await standardized_llm_call(
            llm=llm,
            output_schema=IntrinsicAttributesOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={"nl_description": nl_description},
            config=config,
        )

        # CRITICAL CONSTRAINT: Must have at least one attribute
        max_empty_retries = 3
        empty_retry_count = 0
        while not result.attributes and empty_retry_count < max_empty_retries:
            empty_retry_count += 1
            logger.warning(
                f"Entity {entity_name}: Step 2.2 returned no attributes (attempt {empty_retry_count}/{max_empty_retries}). "
                f"Retrying with explicit requirement for at least one attribute."
            )
            
            empty_retry_system_prompt = f"""You are a database design assistant. Your task is to extract intrinsic attributes for an entity.

CRITICAL REQUIREMENT: You MUST return at least ONE attribute. An empty attributes list is not acceptable.

{system_prompt}

IMPORTANT: Every entity must have at least one intrinsic attribute. Even if the description is minimal, you should identify:
- An identifier attribute (e.g., {entity_name.lower()}_id, id, or similar)
- Or at least one descriptive attribute mentioned in the description
- Or common attributes for this type of entity in the domain

Return at least one attribute. An empty list will cause the pipeline to fail."""

            result = await standardized_llm_call(
                llm=llm,
                output_schema=IntrinsicAttributesOutput,
                system_prompt=empty_retry_system_prompt,
                human_prompt_template=human_prompt,
                input_data={"nl_description": nl_description},
                config=config,
            )
        
        # If still empty after retries, raise an error
        if not result.attributes:
            error_msg = (
                f"Entity '{entity_name}' has no attributes after {max_empty_retries} retries. "
                f"This is a critical error - every entity must have at least one attribute. "
                f"Please check the entity description and NL description for '{entity_name}'."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Revision loop: detect issues deterministically, ask LLM to revise (no Python deletions)
        for round_idx in range(cfg.step_2_2_max_revision_rounds):
            issues = _detect_intrinsic_attribute_issues(
                entity_name=entity_name,
                attributes=result.attributes,
                all_entity_names=all_entity_names,
            )
            
            # Ensure at least one attribute is maintained during revisions
            if not result.attributes:
                issues.append({
                    "type": "empty_attributes",
                    "message": "Attributes list is empty. Must have at least one attribute."
                })
            
            if not issues:
                break

            logger.warning(
                f"Entity {entity_name}: Step 2.2 detected {len(issues)} issue(s); "
                f"requesting LLM revision round {round_idx + 1}/{cfg.step_2_2_max_revision_rounds}."
            )

            revision_system_prompt = """You are a database design assistant.

You previously produced a list of intrinsic attributes for an entity.
You will now receive:
- The entity and enhanced context (including relations and entity registry)
- Your previous attribute list (JSON)
- A deterministic list of issues detected in that list

Task:
Return a revised FULL attribute list that fixes the issues while obeying the original rules:
- CRITICAL: You MUST return at least ONE attribute. An empty list is not acceptable.
- Only intrinsic attributes
- No relation-connecting attributes / foreign keys (attributes whose NAME suggests a connection, e.g., `customer_id`, `order_id`, `zone_id`)
- No nested collection attributes (arrays/lists/json blobs)
- Use snake_case for attribute names
- It is OK to mention other entities in attribute descriptions for clarity, but the attribute NAME itself should not suggest a connection

Return ONLY valid JSON matching the required schema."""

            revision_human_prompt = f"""Entity: {entity_name}{context_msg}

Natural language description:
{{nl_description}}

Previous attributes (JSON):
{{previous_attributes_json}}

Detected issues (JSON):
{{issues_json}}

Return a revised JSON object with key "attributes" containing the corrected intrinsic attributes."""

            result = await standardized_llm_call(
                llm=llm,
                output_schema=IntrinsicAttributesOutput,
                system_prompt=revision_system_prompt,
                human_prompt_template=revision_human_prompt,
                input_data={
                    "nl_description": nl_description,
                    "previous_attributes_json": json.dumps(result.model_dump(), ensure_ascii=True),
                    "issues_json": json.dumps(issues, ensure_ascii=True),
                },
                config=config,
            )
            
            # Ensure revision didn't result in empty attributes
            if not result.attributes:
                logger.error(
                    f"Entity {entity_name}: Revision round {round_idx + 1} resulted in empty attributes. "
                    f"This violates the constraint that every entity must have at least one attribute."
                )
                # Add empty attributes as an issue to force another revision
                issues.append({
                    "type": "empty_attributes_after_revision",
                    "message": "Attributes list became empty after revision. Must have at least one attribute."
                })

        # Final check: ensure we still have at least one attribute after all revisions
        if not result.attributes:
            error_msg = (
                f"Entity '{entity_name}' has no attributes after revision loop. "
                f"This is a critical error - every entity must have at least one attribute."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        attributes = result.attributes
        attribute_names = [attr.name for attr in attributes]
        logger.debug(
            f"Entity {entity_name}: extracted {len(attributes)} intrinsic attributes: "
            f"{', '.join(attribute_names)}"
        )

        return result
        
    except Exception as e:
        logger.error(f"Error extracting intrinsic attributes for entity {entity_name}: {e}", exc_info=True)
        raise


class EntityIntrinsicAttributesResult(BaseModel):
    """Result for a single entity in batch processing."""
    entity_name: str = Field(description="Name of the entity")
    attributes: List[AttributeInfo] = Field(
        description="List of intrinsic attributes with name, description, type_hint, and reasoning"
    )


class IntrinsicAttributesBatchOutput(BaseModel):
    """Output structure for Step 2.2 batch processing."""
    entity_results: List[EntityIntrinsicAttributesResult] = Field(
        description="List of intrinsic attribute extraction results, one per entity"
    )
    total_entities: int = Field(description="Total number of entities processed")


async def step_2_2_intrinsic_attributes_batch(
    entities: List,
    nl_description: str,
    attribute_count_results: Optional[Any] = None,  # Can be AttributeCountBatchOutput (Pydantic) or dict
    domain: Optional[str] = None,
    relations: Optional[List] = None,
    primary_keys: Optional[dict] = None,
) -> IntrinsicAttributesBatchOutput:
    """
    Step 2.2: Extract intrinsic attributes for all entities (parallel execution).
    
    Args:
        entities: List of entities with name and description
        nl_description: Natural language description (FULL description)
        attribute_count_results: Optional results from Step 2.1 (AttributeCountBatchOutput Pydantic model or dict)
        domain: Optional domain context from Phase 1 (Steps 1.1-1.3)
        relations: Optional list of all relations from Phase 1 (Steps 1.9, 1.11)
                   Used to find relations each entity participates in
        primary_keys: Optional dictionary mapping entity names to primary keys from Step 2.7
        
    Returns:
        IntrinsicAttributesBatchOutput: Intrinsic attributes results for all entities
        
    Example:
        >>> result = await step_2_2_intrinsic_attributes_batch(
        ...     entities=[{"name": "Customer"}],
        ...     nl_description="Customers have name and email"
        ... )
        >>> len(result.entity_results)
        1
    """
    logger.info(f"Starting Step 2.2: Intrinsic Attributes for {len(entities)} entities")
    
    if not entities:
        logger.warning("No entities provided for intrinsic attribute extraction")
        return IntrinsicAttributesBatchOutput(entity_results=[], total_entities=0)
    
    # Execute in parallel for all entities
    import asyncio
    
    all_entity_names = [
        e.get("name", "") if isinstance(e, dict) else getattr(e, "name", "")
        for e in entities
    ]

    tasks = []
    task_metadata = []  # Store entity_name for each task
    for entity in entities:
        entity_name = entity.get("name", "Unknown") if isinstance(entity, dict) else getattr(entity, "name", "Unknown")
        entity_desc = entity.get("description", "") if isinstance(entity, dict) else getattr(entity, "description", "")
        
        # Get explicit attributes from Step 2.1 if available
        explicit_attrs = None
        if attribute_count_results:
            # Handle both Pydantic model and dict
            if hasattr(attribute_count_results, 'model_dump'):
                # Pydantic model - convert to dict or access directly
                entity_results = attribute_count_results.entity_results
                # Find matching entity in the list
                for entity_result in entity_results:
                    if entity_result.entity_name == entity_name:
                        explicit_attrs = entity_result.explicit_attributes
                        break
            elif isinstance(attribute_count_results, dict):
                # Dictionary - use existing logic
                entity_results_dict = attribute_count_results.get("entity_results", {})
                if entity_name in entity_results_dict:
                    entity_result = entity_results_dict[entity_name]
                    if isinstance(entity_result, dict):
                        explicit_attrs = entity_result.get("explicit_attributes")
                    elif hasattr(entity_result, 'explicit_attributes'):
                        explicit_attrs = entity_result.explicit_attributes
        
        # Find relations this entity participates in
        entity_relations = None
        if relations:
            entity_relations = [
                rel for rel in relations
                if entity_name in (rel.get("entities", []) if isinstance(rel, dict) else [])
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
            all_entity_names=all_entity_names,
        )
        tasks.append(task)
        task_metadata.append(entity_name)
    
    # Wait for all tasks to complete
    results = await asyncio.gather(
        *tasks,
        return_exceptions=True
    )
    
    # Process results
    entity_results_list = []
    for entity_name, result in zip(task_metadata, results):
        if isinstance(result, Exception):
            logger.error(f"Error processing entity {entity_name}: {result}")
            entity_results_list.append(
                EntityIntrinsicAttributesResult(
                    entity_name=entity_name,
                    attributes=[],
                )
            )
        else:
            entity_results_list.append(
                EntityIntrinsicAttributesResult(
                    entity_name=entity_name,
                    attributes=result.attributes,
                )
            )
    
    total_attributes = sum(len(r.attributes) for r in entity_results_list)
    logger.info(f"Intrinsic attribute extraction completed: {total_attributes} total attributes across {len(entity_results_list)} entities")
    
    return IntrinsicAttributesBatchOutput(
        entity_results=entity_results_list,
        total_entities=len(entities),
    )

