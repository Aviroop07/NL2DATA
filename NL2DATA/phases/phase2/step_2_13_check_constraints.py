"""Phase 2, Step 2.13: Check Constraints (Value Ranges).

Identifies value range constraints (e.g., age > 0, price >= 0, percentage between 0-100).
These are structural constraints, different from statistical constraints.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
import re

from NL2DATA.phases.phase2.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


class CheckConstraintInfo(BaseModel):
    """Information about a check constraint."""
    condition: str = Field(description="The check constraint condition (e.g., 'age > 0', 'price >= 0', 'percentage BETWEEN 0 AND 100')")
    description: str = Field(description="Human-readable description of the constraint")
    reasoning: str = Field(description="Explanation of why this constraint is needed")


class CheckConstraintsOutput(BaseModel):
    """Output structure for check constraint identification."""
    # NOTE: Marked Optional to improve OpenAI response_format JSON-schema compatibility.
    check_constraints: Optional[Dict[str, CheckConstraintInfo]] = Field(
        default_factory=dict,
        description="Dictionary mapping attribute names to their check constraint information"
    )


def _validate_check_constraint_syntax(condition: str) -> tuple[bool, Optional[str]]:
    """
    Basic validation of check constraint condition syntax.
    
    Checks:
    - Not empty
    - Balanced parentheses
    - Contains valid comparison operators (>, <, >=, <=, =, !=, BETWEEN, IN, LIKE)
    - Basic SQL-like syntax
    
    Returns:
        (is_valid, error_message)
    """
    if not condition or not condition.strip():
        return False, "Check constraint condition cannot be empty"
    
    condition = condition.strip()
    
    # Check balanced parentheses
    paren_count = 0
    for char in condition:
        if char == '(':
            paren_count += 1
        elif char == ')':
            paren_count -= 1
            if paren_count < 0:
                return False, "Unbalanced parentheses: closing ')' before opening '('"
    
    if paren_count != 0:
        return False, f"Unbalanced parentheses: {paren_count} unclosed opening '('"
    
    # Check for at least one comparison operator or SQL keyword
    comparison_ops = ['>', '<', '>=', '<=', '=', '!=', '<>', 'BETWEEN', 'IN', 'LIKE', 'IS', 'IS NOT']
    condition_upper = condition.upper()
    has_operator = any(op in condition_upper for op in comparison_ops)
    
    if not has_operator:
        return False, "Check constraint condition must contain a comparison operator or SQL keyword"
    
    return True, None


@traceable_step("2.13", phase=2, tags=['phase_2_step_13'])
async def step_2_13_check_constraints(
    entity_name: str,
    attributes: List[str],  # All attributes for the entity
    nl_description: Optional[str] = None,
    entity_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 2.13 (per-entity): Identify value range constraints (check constraints).
    
    This is designed to be called in parallel for multiple entities.
    
    Args:
        entity_name: Name of the entity
        attributes: List of all attribute names
        nl_description: Optional original NL description for context
        entity_description: Optional description of the entity
        domain: Optional domain context from Phase 1
        
    Returns:
        dict: Check constraints result with check_constraints dictionary
        
    Example:
        >>> result = await step_2_13_check_constraints(
        ...     "Product",
        ...     ["product_id", "price", "quantity", "discount_percentage"]
        ... )
        >>> "price" in result["check_constraints"]
        True
        >>> result["check_constraints"]["price"]["condition"]
        "price >= 0"
    """
    logger.debug(f"Identifying check constraints for entity: {entity_name}")
    
    # Validate that attributes exist
    if not attributes:
        logger.warning(f"No attributes provided for entity {entity_name}, cannot identify check constraints")
        return {
            "check_constraints": {}
        }
    
    # Build context
    context_parts = []
    if domain:
        context_parts.append(f"Domain: {domain}")
    if entity_description:
        context_parts.append(f"Entity description: {entity_description}")
    context_parts.append(f"All attributes: {', '.join(attributes)}")
    
    context_msg = "\n\nContext:\n" + "\n".join(f"- {part}" for part in context_parts)
    
    # System prompt
    system_prompt = """You are a database schema design expert. Your task is to identify check constraints (value range constraints) for attributes.

CHECK CONSTRAINTS:
- Structural constraints that enforce value ranges or conditions
- Examples:
  * Numeric ranges: age > 0, price >= 0, quantity > 0
  * Percentage ranges: discount_percentage BETWEEN 0 AND 100
  * String patterns: email LIKE '%@%.%'
  * Value sets: status IN ('active', 'inactive', 'pending')
- These are different from statistical constraints (which are about distributions)
- Focus on constraints that ensure data validity and business rules

Common patterns:
- Non-negative numbers: price >= 0, quantity >= 0
- Positive numbers: age > 0, count > 0
- Percentage ranges: percentage BETWEEN 0 AND 100
- Date ranges: start_date < end_date
- String length: LENGTH(name) > 0

Return a JSON object with:
- check_constraints: Dictionary mapping attribute names to constraint information
  Each constraint should have:
  * condition: SQL-like check condition (e.g., 'price >= 0', 'age > 0')
  * description: Human-readable description
  * reasoning: REQUIRED - Explanation of why this constraint is needed (cannot be omitted)

Only include attributes that need check constraints. Not all attributes need constraints."""
    
    # Human prompt
    human_prompt_template = """Identify check constraints (value range constraints) for attributes in the entity: {entity_name}

{context}

Natural Language Description:
{nl_description}

Return a JSON object identifying which attributes need check constraints and what those constraints should be, along with descriptions and reasoning."""
    
    try:
        # Get model for this step
        llm = get_model_for_step("2.13")
        
        # Invoke standardized LLM call
        config = get_trace_config("2.13", phase=2, tags=["phase_2_step_13"])
        result: CheckConstraintsOutput = await standardized_llm_call(
            llm=llm,
            output_schema=CheckConstraintsOutput,
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
        # Validate that attributes exist and validate constraint syntax
        check_constraints = dict(result.check_constraints)
        validated_constraints = {}
        
        for attr, constraint_info in check_constraints.items():
            if attr not in attributes:
                logger.warning(
                    f"Entity {entity_name}: Check constraint attribute '{attr}' doesn't exist in attribute list"
                )
                continue
            
            # Validate constraint info structure (should be a Pydantic model or dict)
            if isinstance(constraint_info, dict):
                condition = constraint_info.get("condition", "")
                description = constraint_info.get("description", "")
                reasoning = constraint_info.get("reasoning", "")
            elif hasattr(constraint_info, "condition"):
                condition = constraint_info.condition
                description = getattr(constraint_info, "description", "")
                reasoning = getattr(constraint_info, "reasoning", "")
            else:
                logger.warning(
                    f"Entity {entity_name}, attribute {attr}: Invalid constraint info structure"
                )
                continue
            
            # Validate condition syntax
            is_valid, error_msg = _validate_check_constraint_syntax(condition)
            if is_valid:
                validated_constraints[attr] = {
                    "condition": condition,
                    "description": description,
                    "reasoning": reasoning
                }
            else:
                logger.warning(
                    f"Entity {entity_name}, attribute {attr}: Invalid check constraint syntax: {error_msg}. "
                    f"Condition: {condition}"
                )
        
        # Create new model instance if modifications were made
        if validated_constraints != check_constraints:
            result = CheckConstraintsOutput(
                check_constraints=validated_constraints
            )
        
        logger.debug(
            f"Entity {entity_name}: Found {len(result.check_constraints)} check constraints"
        )
        
        # Convert to dict only at return boundary
        return result.model_dump()
        
    except Exception as e:
        logger.error(f"Error identifying check constraints for entity {entity_name}: {e}", exc_info=True)
        raise


async def step_2_13_check_constraints_batch(
    entities: List[Dict[str, Any]],
    entity_attributes: Dict[str, List[str]],  # entity_name -> all attribute names
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 2.13: Identify check constraints for all entities (parallel execution).
    
    Args:
        entities: List of entities with name and description
        entity_attributes: Dictionary mapping entity names to all their attribute names
        nl_description: Optional original NL description
        domain: Optional domain context from Phase 1
        
    Returns:
        dict: Check constraints results for all entities, keyed by entity name
        
    Example:
        >>> result = await step_2_13_check_constraints_batch(
        ...     entities=[{"name": "Product"}],
        ...     entity_attributes={"Product": ["product_id", "price", "quantity"]}
        ... )
        >>> "Product" in result["entity_results"]
        True
    """
    logger.info(f"Starting Step 2.13: Check Constraints for {len(entities)} entities")
    
    if not entities:
        logger.warning("No entities provided for check constraint identification")
        return {"entity_results": {}}
    
    # Execute in parallel for all entities
    import asyncio
    
    tasks = []
    for entity in entities:
        entity_name = entity.get("name", "Unknown") if isinstance(entity, dict) else getattr(entity, "name", "Unknown")
        entity_desc = entity.get("description", "") if isinstance(entity, dict) else getattr(entity, "description", "")
        attributes = entity_attributes.get(entity_name, [])
        
        task = step_2_13_check_constraints(
            entity_name=entity_name,
            attributes=attributes,
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
                "check_constraints": {}
            }
        else:
            entity_results[entity_name] = result
    
    total_constraints = sum(len(r.get("check_constraints", {})) for r in entity_results.values())
    logger.info(
        f"Check constraint identification completed: {total_constraints} check constraints identified "
        f"across {len(entity_results)} entities"
    )
    
    return {"entity_results": entity_results}

