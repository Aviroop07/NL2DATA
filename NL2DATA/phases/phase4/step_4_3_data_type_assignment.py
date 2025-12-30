"""Phase 4, Step 4.3: Data Type Assignment.

Assign appropriate SQL data types to each attribute.
Critical for DDL generation - incorrect types cause schema creation failures or data loss.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

from NL2DATA.phases.phase4.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.phases.phase1.utils.data_extraction import (
    extract_attribute_name,
    extract_attribute_description,
    extract_attribute_type_hint,
)

logger = get_logger(__name__)


class AttributeTypeInfo(BaseModel):
    """Information about a single attribute's data type."""
    type: str = Field(description="SQL data type (e.g., 'VARCHAR', 'INT', 'DECIMAL', 'DATE', 'TIMESTAMP', 'BOOLEAN')")
    size: Optional[int] = Field(
        default=None,
        description="Size for VARCHAR/CHAR types (e.g., VARCHAR(255) -> size=255). Null for types that don't need size."
    )
    precision: Optional[int] = Field(
        default=None,
        description="Precision for DECIMAL/NUMERIC types (total number of digits). Null for non-decimal types."
    )
    scale: Optional[int] = Field(
        default=None,
        description="Scale for DECIMAL/NUMERIC types (digits after decimal point). Null for non-decimal types."
    )
    constraints: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional type constraints (e.g., {{\"unsigned\": True}} for INT UNSIGNED)"
    )
    reasoning: str = Field(description="REQUIRED - Explanation of why this data type was chosen (cannot be omitted)")


class DataTypeAssignmentOutput(BaseModel):
    """Output structure for data type assignment."""
    attribute_types: Dict[str, AttributeTypeInfo] = Field(
        description="Dictionary mapping attribute names to their type information. Every attribute MUST have an entry."
    )


@traceable_step("4.3", phase=4, tags=['phase_4_step_3'])
async def step_4_3_data_type_assignment(
    entity_name: str,
    attributes: List[Dict[str, Any]],  # All attributes with descriptions, constraints, etc.
    primary_key: Optional[List[str]] = None,  # Primary key from Step 2.7
    check_constraints: Optional[Dict[str, Dict[str, Any]]] = None,  # From Step 2.13
    unique_constraints: Optional[List[str]] = None,  # From Step 2.10
    nullable_attributes: Optional[List[str]] = None,  # From Step 2.11
    relations: Optional[List[Dict[str, Any]]] = None,  # Relations involving this entity
    nl_description: Optional[str] = None,
    entity_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 4.3 (per-entity): Assign appropriate SQL data types to each attribute.
    
    This is designed to be called in parallel for multiple entities.
    
    Args:
        entity_name: Name of the entity
        attributes: List of all attributes with descriptions, type_hints, constraints
        primary_key: Optional primary key attributes from Step 2.7
        check_constraints: Optional check constraints from Step 2.13 (attr_name -> constraint info)
        unique_constraints: Optional list of unique attributes from Step 2.10
        nullable_attributes: Optional list of nullable attributes from Step 2.11
        relations: Optional list of relations involving this entity
        nl_description: Optional original NL description
        entity_description: Optional description of the entity
        domain: Optional domain context from Phase 1
        
    Returns:
        dict: Data type assignment result with attribute_types dictionary
        
    Example:
        >>> result = await step_4_3_data_type_assignment(
        ...     entity_name="Customer",
        ...     attributes=[{"name": "customer_id", "description": "Unique identifier", "type_hint": "integer"}]
        ... )
        >>> "customer_id" in result["attribute_types"]
        True
        >>> result["attribute_types"]["customer_id"]["type"]
        "INT"
    """
    logger.debug(f"Assigning data types for entity: {entity_name}")
    
    # Validate that attributes exist
    if not attributes:
        logger.warning(f"No attributes provided for entity {entity_name}, cannot assign data types")
        return {"attribute_types": {}}
    
    # Build comprehensive context
    context_parts = []
    if domain:
        context_parts.append(f"Domain: {domain}")
    if entity_description:
        context_parts.append(f"Entity description: {entity_description}")
    
    # Attributes summary with descriptions and type hints
    attr_details = []
    for attr in attributes:
        attr_name = extract_attribute_name(attr)
        attr_desc = extract_attribute_description(attr)
        attr_type_hint = extract_attribute_type_hint(attr)
        
        attr_info = f"  - {attr_name}"
        if attr_desc:
            attr_info += f": {attr_desc}"
        if attr_type_hint:
            attr_info += f" (hint: {attr_type_hint})"
        attr_details.append(attr_info)
    
    context_parts.append(f"Attributes ({len(attributes)}):\n" + "\n".join(attr_details))
    
    # Primary key
    if primary_key:
        context_parts.append(f"Primary Key: {', '.join(primary_key)}")
        context_parts.append("Note: Primary key attributes are typically INTEGER or BIGINT for auto-increment, or appropriate type for natural keys")
    
    # Check constraints (affect type selection)
    if check_constraints:
        constraint_summary = []
        for attr_name, constraint_info in check_constraints.items():
            condition = constraint_info.get("condition", "")
            if condition:
                constraint_summary.append(f"  - {attr_name}: {condition}")
        if constraint_summary:
            context_parts.append(f"Check Constraints:\n" + "\n".join(constraint_summary))
    
    # Unique constraints
    if unique_constraints:
        context_parts.append(f"Unique Attributes: {', '.join(unique_constraints)}")
    
    # Nullability
    if nullable_attributes:
        context_parts.append(f"Nullable Attributes: {', '.join(nullable_attributes)}")
        non_nullable = [extract_attribute_name(attr) 
                       for attr in attributes 
                       if extract_attribute_name(attr) not in nullable_attributes]
        if non_nullable:
            context_parts.append(f"Non-Nullable Attributes: {', '.join(non_nullable)}")
    
    # Relations (for foreign key type matching)
    if relations:
        rel_summary = []
        for rel in relations[:5]:  # Limit to avoid too long context
            rel_entities = rel.get("entities", [])
            rel_type = rel.get("type", "")
            rel_info = f"  - {rel_type}: {', '.join(rel_entities)}"
            rel_summary.append(rel_info)
        context_parts.append(f"Relations:\n" + "\n".join(rel_summary))
        context_parts.append("Note: Foreign key attributes should match the type of the referenced primary key")
    
    context_msg = "\n\nContext:\n" + "\n".join(f"- {part}" for part in context_parts)
    
    # System prompt
    system_prompt = """You are a database schema design expert. Your task is to assign appropriate SQL data types to each attribute.

SQL DATA TYPES:
- **INTEGER/INT**: Whole numbers (e.g., customer_id, quantity, age)
- **BIGINT**: Large whole numbers (for very large tables, auto-increment IDs)
- **SMALLINT**: Small whole numbers (limited range, use sparingly)
- **DECIMAL/NUMERIC**: Exact decimal numbers (e.g., price, amount) - requires precision and scale
- **FLOAT/REAL/DOUBLE**: Approximate floating-point numbers (use DECIMAL for financial data)
- **VARCHAR(n)**: Variable-length strings (e.g., name, email, description) - specify size
- **CHAR(n)**: Fixed-length strings (e.g., country codes, status codes) - specify size
- **TEXT**: Long text (for descriptions, comments) - no size needed
- **DATE**: Date only (year-month-day)
- **TIME**: Time only (hour-minute-second)
- **TIMESTAMP/DATETIME**: Date and time combined
- **BOOLEAN**: True/false values
- **JSON/JSONB**: JSON data (PostgreSQL-specific, use TEXT for portability)
- **UUID**: Universally unique identifiers

TYPE SELECTION RULES:
1. **Primary Keys**: Usually INTEGER or BIGINT (for auto-increment), or appropriate type for natural keys (VARCHAR for email, etc.)
2. **Foreign Keys**: Must match the type of the referenced primary key
3. **Numeric Values**: Use DECIMAL for money/precise values, INT for counts/IDs, BIGINT for large numbers
4. **Strings**: Use VARCHAR for variable-length, CHAR for fixed-length, TEXT for long content
5. **Dates/Times**: Use DATE for dates only, TIMESTAMP for date+time, TIME for time only
6. **Booleans**: Use BOOLEAN (or TINYINT(1) for MySQL compatibility)
7. **Size Considerations**: 
   - VARCHAR: Common sizes are 50, 100, 255, 500, 1000
   - DECIMAL: Common precision/scale are (10,2) for money, (5,2) for percentages
8. **Portability**: Prefer standard SQL types that work across PostgreSQL, MySQL, SQLite

Return a JSON object with:
- attribute_types: Dictionary mapping each attribute name to type information
  Each entry must include:
  * type: SQL data type name (e.g., "VARCHAR", "INT", "DECIMAL")
  * size: Size for VARCHAR/CHAR (e.g., 255), null for other types
  * precision: Precision for DECIMAL (e.g., 10), null for other types
  * scale: Scale for DECIMAL (e.g., 2), null for other types
  * constraints: Additional constraints dict (e.g., {{\"unsigned\": True}}), empty dict if none
  * reasoning: REQUIRED - Explanation of why this type was chosen (cannot be omitted)

**CRITICAL REQUIREMENTS**:
- Every attribute MUST have an entry in attribute_types - do not skip any attributes
- The reasoning field is REQUIRED for every attribute - explain why you chose that specific type
- Match foreign key types EXACTLY to referenced primary key types (same type, size, precision, scale)
- Use appropriate sizes: not too small (will cause data truncation), not wastefully large (wastes storage)
- Consider CHECK constraints and business rules when selecting types
- Prefer portable types that work across PostgreSQL, MySQL, SQLite

**COMMON MISTAKES TO AVOID**:
- Using VARCHAR without size (always specify size, e.g., VARCHAR(255))
- Using DECIMAL without precision/scale (always specify both, e.g., DECIMAL(10,2))
- Mismatching foreign key types with referenced primary keys
- Using FLOAT for financial data (use DECIMAL for exact precision)
- Using TEXT for short strings (use VARCHAR with appropriate size)"""

    # Human prompt template
    human_prompt_template = """Assign SQL data types to attributes for the entity: {entity_name}

{context}

Natural Language Description:
{nl_description}

Return a JSON object with data types for all attributes, including reasoning for each type selection."""

    # Initialize model
    llm = get_model_for_step("4.3")  # Step 4.3 maps to "high_fanout" task type
    
    try:
        config = get_trace_config("4.3", phase=4, tags=["phase_4_step_3"])
        result: DataTypeAssignmentOutput = await standardized_llm_call(
            llm=llm,
            output_schema=DataTypeAssignmentOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt_template,
            input_data={
                "entity_name": entity_name,
                "context": context_msg,
                "nl_description": nl_description or "",
            },
            config=config,
        )
        
        # Work with Pydantic model directly
        attribute_count = len(result.attribute_types)
        logger.info(f"Data type assignment completed for {entity_name}: {attribute_count} attributes")
        
        # Convert to dict only at return boundary
        return result.model_dump()
        
    except Exception as e:
        logger.error(
            f"Error assigning data types for entity {entity_name}: {e}",
            exc_info=True
        )
        raise


async def step_4_3_data_type_assignment_batch(
    entities: List[Dict[str, Any]],
    entity_attributes: Dict[str, List[Dict[str, Any]]],  # entity_name -> attributes with descriptions
    entity_primary_keys: Optional[Dict[str, List[str]]] = None,  # entity_name -> primary key
    entity_check_constraints: Optional[Dict[str, Dict[str, Dict[str, Any]]]] = None,  # entity_name -> {attr: constraint}
    entity_unique_constraints: Optional[Dict[str, List[str]]] = None,  # entity_name -> unique attributes
    entity_nullable_attributes: Optional[Dict[str, List[str]]] = None,  # entity_name -> nullable attributes
    entity_relations: Optional[Dict[str, List[Dict[str, Any]]]] = None,  # entity_name -> relations
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 4.3: Assign data types for all entities (parallel execution).
    
    Args:
        entities: List of entities with name and description
        entity_attributes: Dictionary mapping entity names to their attributes (with descriptions, type_hints)
        entity_primary_keys: Optional dictionary mapping entity names to their primary keys
        entity_check_constraints: Optional dictionary mapping entity names to check constraints
        entity_unique_constraints: Optional dictionary mapping entity names to unique attributes
        entity_nullable_attributes: Optional dictionary mapping entity names to nullable attributes
        entity_relations: Optional dictionary mapping entity names to relations involving them
        nl_description: Optional original NL description
        domain: Optional domain context from Phase 1
        
    Returns:
        dict: Data type assignment results for all entities, keyed by entity name
        
    Example:
        >>> result = await step_4_3_data_type_assignment_batch(
        ...     entities=[{"name": "Customer"}],
        ...     entity_attributes={"Customer": [{"name": "customer_id", "type_hint": "integer"}]}
        ... )
        >>> "Customer" in result["entity_results"]
        True
    """
    logger.info(f"Starting Step 4.3: Data Type Assignment for {len(entities)} entities")
    
    if not entities:
        logger.warning("No entities provided for data type assignment")
        return {"entity_results": {}}
    
    # Execute in parallel for all entities
    import asyncio
    
    tasks = []
    for entity in entities:
        entity_name = entity.get("name", "Unknown") if isinstance(entity, dict) else getattr(entity, "name", "Unknown")
        entity_desc = entity.get("description", "") if isinstance(entity, dict) else getattr(entity, "description", "")
        attributes = entity_attributes.get(entity_name, [])
        primary_key = (entity_primary_keys or {}).get(entity_name)
        check_constraints = (entity_check_constraints or {}).get(entity_name)
        unique_constraints = (entity_unique_constraints or {}).get(entity_name)
        nullable_attributes = (entity_nullable_attributes or {}).get(entity_name)
        relations = (entity_relations or {}).get(entity_name)
        
        task = step_4_3_data_type_assignment(
            entity_name=entity_name,
            attributes=attributes,
            primary_key=primary_key,
            check_constraints=check_constraints,
            unique_constraints=unique_constraints,
            nullable_attributes=nullable_attributes,
            relations=relations,
            nl_description=nl_description,
            entity_description=entity_desc,
            domain=domain,
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
            entity_results[entity_name] = {
                "attribute_types": {},
                "error": str(result)
            }
        else:
            entity_results[entity_name] = result
    
    total_attributes = sum(
        len(result.get("attribute_types", {}))
        for result in entity_results.values()
        if not result.get("error")
    )
    logger.info(
        f"Data type assignment completed: {len(entity_results)} entities, {total_attributes} total attributes"
    )
    
    return {"entity_results": entity_results}

