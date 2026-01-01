"""Phase 2, Step 2.14: Entity Cleanup - Remove Relation-Connecting Attributes.

For each entity, check if any of its current attributes are relation-connecting attributes
(attributes that help connect this entity to another entity through a relation).
If such attributes exist, remove them. Goal: ensure only intrinsic attributes remain in entities.

Loop continues until LLM confirms no relation-connecting attributes exist.
"""

from typing import Dict, Any, List, Optional
import json
from pydantic import BaseModel, Field

from NL2DATA.phases.phase2.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.loops import SafeLoopExecutor, LoopConfig
from NL2DATA.phases.phase1.utils.data_extraction import extract_attribute_name
from NL2DATA.ir.models.state import AttributeInfo
from NL2DATA.utils.pipeline_config import get_phase2_config

logger = get_logger(__name__)


class EntityCleanupOutput(BaseModel):
    """Output structure for entity cleanup."""
    entity: str = Field(description="Name of the entity being cleaned")
    relation_connecting_attributes: List[str] = Field(
        default_factory=list,
        description="List of attribute names that are relation-connecting and should be removed"
    )
    should_remove: bool = Field(
        description="Whether any attributes should be removed"
    )
    no_more_attributes: bool = Field(
        description="Whether no more relation-connecting attributes exist (termination condition for loop)"
    )
    reasoning: str = Field(description="Explanation of the cleanup decision")


class UpdatedAttributesOutput(BaseModel):
    """Output structure for applying a cleanup decision to an attribute list."""
    attributes: List[AttributeInfo] = Field(description="Updated attribute list for the entity")


def _validate_entity_cleanup_output(
    *,
    entity_name: str,
    result: EntityCleanupOutput,
    current_attr_names: List[str],
    primary_key: List[str],
) -> List[Dict[str, Any]]:
    """Deterministic validation for Step 2.14 output.

    Critical: The LLM must not invent attributes to remove. The removal list must be a subset of current attribute names.
    """
    issues: List[Dict[str, Any]] = []
    curr_set = {a for a in (current_attr_names or []) if a}
    pk_set = {p for p in (primary_key or []) if p}

    # Must not propose removals that don't exist
    unknown = [a for a in (result.relation_connecting_attributes or []) if a and a not in curr_set]
    if unknown:
        issues.append(
            {
                "issue_type": "unknown_attr_in_relation_connecting_attributes",
                "detail": "LLM suggested removing attributes not present in current attribute list",
                "value": unknown,
            }
        )

    # Must not propose removing primary key attributes
    pk_hits = [a for a in (result.relation_connecting_attributes or []) if a and a in pk_set]
    if pk_hits:
        issues.append(
            {
                "issue_type": "primary_key_suggested_for_removal",
                "detail": "LLM suggested removing primary key attributes; this is forbidden",
                "value": pk_hits,
            }
        )

    # Internal consistency checks
    should_remove_expected = bool(result.relation_connecting_attributes)
    if result.should_remove != should_remove_expected:
        issues.append(
            {
                "issue_type": "should_remove_inconsistent",
                "detail": "should_remove must be true iff relation_connecting_attributes is non-empty",
                "value": {"should_remove": result.should_remove, "expected": should_remove_expected},
            }
        )

    if not should_remove_expected and not result.no_more_attributes:
        issues.append(
            {
                "issue_type": "no_more_attributes_should_be_true_when_no_removals",
                "detail": "When relation_connecting_attributes is empty, no_more_attributes must be true (termination condition)",
                "value": {"no_more_attributes": result.no_more_attributes},
            }
        )

    if result.no_more_attributes and should_remove_expected:
        issues.append(
            {
                "issue_type": "no_more_attributes_inconsistent",
                "detail": "no_more_attributes cannot be true when relation_connecting_attributes is non-empty",
                "value": {"no_more_attributes": result.no_more_attributes, "relation_connecting_attributes": result.relation_connecting_attributes},
            }
        )

    return issues


async def _apply_attribute_removals_via_llm(
    *,
    entity_name: str,
    entity_description: Optional[str],
    current_attributes: List[Dict[str, Any]],
    primary_key: List[str],
    attrs_to_remove: List[str],
    relations: List[Dict[str, Any]],
    nl_description: Optional[str],
    domain: Optional[str],
) -> List[Dict[str, Any]]:
    """Apply attribute removals via LLM (LLM decides final list; Python does not delete)."""
    llm = get_model_for_step("2.14")
    config = get_trace_config("2.14", phase=2, tags=["phase_2_step_14", "apply_cleanup"])

    # Build minimal context (avoid braces issues by not dumping raw dicts into prompt)
    current_attr_names = [extract_attribute_name(a) for a in current_attributes]
    rel_summaries = []
    for rel in relations or []:
        rel_summaries.append(
            {
                "type": rel.get("type"),
                "entities": rel.get("entities", []),
                "description": rel.get("description"),
                "entity_cardinalities": rel.get("entity_cardinalities", {}),
                "entity_participations": rel.get("entity_participations", {}),
            }
        )

    system_prompt = """You are a database schema cleanup assistant.

Task:
Given an entity's current attribute list and a list of attributes requested for removal (because they are relation-connecting),
return an UPDATED FULL attribute list.

Rules:
- You MUST NOT remove any primary key attributes (explicitly provided).
- Remove only the attributes in the requested removal list (minus any PK attributes).
- Do not invent new attributes; keep the remaining attributes as-is.
- Keep attribute names in snake_case.

Return JSON only with the schema: {"attributes": [ ... ]}."""

    human_prompt_template = """Entity: {entity_name}
Entity description: {entity_description}
Domain: {domain}

Primary key (MUST KEEP): {primary_key}

Relations (for context): {relations_json}

Current attribute names: {current_attr_names}

Requested removals (non-PK): {attrs_to_remove}

Return the updated attribute list (full objects: name, description, type_hint, reasoning, etc.) as JSON."""

    result: UpdatedAttributesOutput = await standardized_llm_call(
        llm=llm,
        output_schema=UpdatedAttributesOutput,
        system_prompt=system_prompt,
        human_prompt_template=human_prompt_template,
        input_data={
            "entity_name": entity_name,
            "entity_description": entity_description or "",
            "domain": domain or "",
            "primary_key": ", ".join(primary_key or []),
            "relations_json": json.dumps(rel_summaries, ensure_ascii=True),
            "current_attr_names": ", ".join([n for n in current_attr_names if n]),
            "attrs_to_remove": ", ".join([a for a in attrs_to_remove if a]),
        },
        config=config,
    )

    # Convert to list[dict] for downstream consistency
    return [a.model_dump() for a in result.attributes]


@traceable_step("2.14", phase=2, tags=['phase_2_step_14'])
async def step_2_14_entity_cleanup_single(
    entity_name: str,
    entity_description: Optional[str],
    current_attributes: List[Dict[str, Any]],
    primary_key: List[str],
    relations: List[Dict[str, Any]],
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
    previous_iteration: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Step 2.14 (per-entity, single iteration): Check if entity has relation-connecting attributes.
    
    This is designed to be called in a loop until no_more_attributes is True.
    
    Args:
        entity_name: Name of the entity
        entity_description: Description of the entity from Step 1.4
        current_attributes: List of current attributes for this entity (from Steps 2.2-2.5)
        primary_key: Current primary key for this entity (from Step 2.7)
                     CRITICAL: LLM must NOT suggest removing primary key attributes
        relations: List of relations this entity participates in (from Steps 1.9, 1.11)
                   Each relation dict should have: entities, type, description, cardinalities, participations
        nl_description: Optional original NL description
        domain: Optional domain context from Phase 1
        previous_iteration: Optional result from previous iteration (for loop)
        
    Returns:
        dict: Cleanup result with relation_connecting_attributes, should_remove, no_more_attributes, reasoning
        
    Example:
        >>> result = await step_2_14_entity_cleanup_single(
        ...     entity_name="Order",
        ...     entity_description="An order placed by a customer",
        ...     current_attributes=[{"name": "order_id"}, {"name": "order_date"}, {"name": "customer_id"}],
        ...     primary_key=["order_id"],
        ...     relations=[{"entities": ["Customer", "Order"], "type": "one-to-many"}]
        ... )
        >>> "customer_id" in result["relation_connecting_attributes"]
        True
    """
    logger.debug(f"Checking entity cleanup for: {entity_name}")
    
    # Build enhanced context
    context_parts = []
    
    # 1. Entity information
    context_parts.append(f"Entity: {entity_name}")
    if entity_description:
        context_parts.append(f"Entity description: {entity_description}")
    
    # 2. Current primary key - CRITICAL: Must NOT be removed
    if primary_key:
        context_parts.append(
            f"**CRITICAL**: The following attributes are the primary key and MUST NOT be removed: {', '.join(primary_key)}"
        )
    else:
        context_parts.append("Primary key: Not yet determined (will be set in Step 2.7)")
    
    # 3. Current attributes
    attr_names = [extract_attribute_name(attr) for attr in current_attributes]
    context_parts.append(f"Current attributes: {', '.join(attr_names)}")
    
    # 4. All relations this entity participates in with details
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
                card_str = ", ".join(f"{ent}={card}" for ent, card in rel_cardinalities.items())
                rel_info += f"\n    Cardinalities: {card_str}"
            
            if rel_participations:
                part_str = ", ".join(f"{ent}={part}" for ent, part in rel_participations.items())
                rel_info += f"\n    Participations: {part_str}"
            
            relation_details.append(rel_info)
        
        if relation_details:
            context_parts.append(f"Relations this entity participates in:\n" + "\n".join(relation_details))
    
    # 5. Previous iteration results (for loop)
    if previous_iteration:
        prev_removed = previous_iteration.get("relation_connecting_attributes", [])
        if prev_removed:
            context_parts.append(
                f"Previous iteration removed: {', '.join(prev_removed)}"
            )
    
    # 6. Domain context
    if domain:
        context_parts.append(f"Domain: {domain}")
    
    context_msg = "\n\nEnhanced Context:\n" + "\n".join(f"- {part}" for part in context_parts)
    
    # System prompt with explicit instruction and detailed example
    system_prompt = """You are a database schema cleanup assistant. Your task is to identify and remove relation-connecting attributes from an entity.

**CRITICAL INSTRUCTION**: Check if any attribute in the entity's attribute list is used to connect this entity to another entity through a relation. If yes, list those attributes. If no, confirm no relation-connecting attributes exist.

**CRITICAL CONSTRAINTS**:
1) You MUST ONLY list attributes that already exist in the provided "Current attributes" list. Do NOT invent attributes like "opportunity_id" or "creative_id" if they are not present.
2) You MUST NOT list any primary key attributes for removal.
3) If a relation implies a foreign key but it is not present in current attributes, that is OK: output an empty removal list and set no_more_attributes=true.

**WHAT ARE RELATION-CONNECTING ATTRIBUTES?**
Relation-connecting attributes are attributes that establish a relationship between this entity and another entity. Examples:
- "customer_id" in Order entity (connects Order to Customer)
- "order_id" in OrderItem entity (connects OrderItem to Order)
- "product_id" in OrderItem entity (connects OrderItem to Product)
- "category_id" in Product entity (connects Product to Category)

**WHAT ARE NOT RELATION-CONNECTING ATTRIBUTES?**
- Primary key attributes (e.g., "order_id" in Order entity when it's the PK) - DO NOT REMOVE THESE
- Intrinsic attributes (e.g., "name", "email", "price", "status") - these describe the entity itself
- Attributes that belong to the relationship itself (e.g., "enrollment_date" for Student-Course) - these are handled in Step 2.15

**DETAILED EXAMPLE**:

Context provided:
- Entity: "Order"
- Current attributes: ["order_id", "order_date", "total_amount", "customer_id", "status"]
- Current primary key: ["order_id"] (MUST NOT be removed)
- Relations Order participates in:
  - Customer-Order: Order belongs to Customer (N:1, Order total, Customer partial)

**ITERATION 1 OUTPUT** (identifies relation-connecting attribute):
```json
{
  "entity": "Order",
  "relation_connecting_attributes": ["customer_id"],
  "should_remove": true,
  "no_more_attributes": false,
  "reasoning": "customer_id is used to connect Order to Customer through the Customer-Order relation. This is a relation-connecting attribute and should be removed from Order's intrinsic attributes. It will be handled as a foreign key during schema compilation."
}
```

**AFTER REMOVAL, ITERATION 2 OUTPUT** (confirms cleanup complete):
```json
{
  "entity": "Order",
  "relation_connecting_attributes": [],
  "should_remove": false,
  "no_more_attributes": true,
  "reasoning": "All remaining attributes (order_id, order_date, total_amount, status) are intrinsic to Order. order_id is the primary key and must remain. No relation-connecting attributes remain."
}
```

**Explanation**: The LLM correctly identifies "customer_id" as a relation-connecting attribute and removes it. It preserves "order_id" because it's the primary key. The loop continues until no more relation-connecting attributes are found.

**RULES**:
1. **DO NOT remove primary key attributes** - They are explicitly listed in context and must be preserved
2. **DO remove relation-connecting attributes** - Attributes that reference other entities through relations
3. **DO NOT remove intrinsic attributes** - Attributes that describe the entity itself (name, email, price, status, etc.)
4. **Be thorough** - Check all attributes against all relations this entity participates in

Return a JSON object with:
- entity: Name of the entity
- relation_connecting_attributes: List of attribute names that are relation-connecting (empty if none)
- should_remove: True if any attributes should be removed, False otherwise
- no_more_attributes: True if no relation-connecting attributes exist (termination condition), False if more checks needed
- reasoning: REQUIRED - Explanation of your decision (cannot be omitted)"""
    
    # Human prompt template
    human_prompt_template = """Check if any attributes in {entity_name} are relation-connecting attributes.

{context}

Natural Language Description:
{nl_description}

Return a JSON object specifying which attributes (if any) are relation-connecting and should be removed, and whether you're satisfied that no more such attributes exist."""
    
    try:
        # Get model for this step
        llm = get_model_for_step("2.14")
        
        # Invoke standardized LLM call
        config = get_trace_config("2.14", phase=2, tags=["phase_2_step_14"])
        result: EntityCleanupOutput = await standardized_llm_call(
            llm=llm,
            output_schema=EntityCleanupOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt_template,
            input_data={
                "entity_name": entity_name,
                "context": context_msg,
                "nl_description": nl_description or "",
            },
            config=config,
        )

        current_attr_names = [n for n in (attr_names or []) if n]
        cfg = get_phase2_config()

        # Validate and ask for corrected output rather than mutating LLM output in Python.
        for _round_idx in range(cfg.step_2_14_max_revision_rounds):
            issues = _validate_entity_cleanup_output(
                entity_name=entity_name,
                result=result,
                current_attr_names=current_attr_names,
                primary_key=primary_key,
            )
            if not issues:
                break

            revision_system_prompt = """You are a database schema cleanup assistant.

You previously produced a relation-connecting attribute cleanup decision.
You will now receive:
- The current attribute names (you MUST ONLY choose removals from this list)
- The primary key (you MUST NOT remove these)
- Your previous output JSON
- A deterministic list of validation issues

Task: Return a corrected JSON payload (same schema) that satisfies all constraints."""

            revision_human_prompt = """Entity: {entity_name}

Allowed current attribute names (ONLY choose from these):
{current_attr_names}

Primary key (MUST NOT remove):
{primary_key}

Previous output (JSON):
{previous_output_json}

Validation issues (JSON):
{issues_json}

Return corrected JSON (same schema)."""

            result = await standardized_llm_call(
                llm=llm,
                output_schema=EntityCleanupOutput,
                system_prompt=revision_system_prompt,
                human_prompt_template=revision_human_prompt,
                input_data={
                    "entity_name": entity_name,
                    "current_attr_names": ", ".join(current_attr_names),
                    "primary_key": ", ".join(primary_key or []),
                    "previous_output_json": json.dumps(result.model_dump(), ensure_ascii=True),
                    "issues_json": json.dumps(issues, ensure_ascii=True),
                },
                config=config,
            )

        final_issues = _validate_entity_cleanup_output(
            entity_name=entity_name,
            result=result,
            current_attr_names=current_attr_names,
            primary_key=primary_key,
        )
        if final_issues:
            logger.error(
                f"Entity {entity_name}: Step 2.14 cleanup output invalid after retries; "
                f"falling back to no-op cleanup decision (no removals)."
            )
            result = EntityCleanupOutput(
                entity=entity_name,
                relation_connecting_attributes=[],
                should_remove=False,
                no_more_attributes=True,
                reasoning="Fallback: invalid cleanup output after retries; performing no removals.",
            )
        
        logger.debug(
            f"Entity {entity_name}: cleanup check - "
            f"relation_connecting_attributes={result.relation_connecting_attributes}, "
            f"no_more_attributes={result.no_more_attributes}"
        )
        
        # Convert to dict only at return boundary
        return result.model_dump()
        
    except Exception as e:
        logger.error(f"Error in entity cleanup for entity {entity_name}: {e}", exc_info=True)
        raise


async def step_2_14_entity_cleanup_single_with_loop(
    entity_name: str,
    entity_description: Optional[str],
    current_attributes: List[Dict[str, Any]],
    primary_key: List[str],
    relations: List[Dict[str, Any]],
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
    max_iterations: int = 5,
    max_time_sec: int = 180,
) -> Dict[str, Any]:
    """
    Step 2.14 with automatic looping: continues until no_more_attributes is True.
    
    This function implements the iterative loop: continues checking and removing
    relation-connecting attributes until the LLM confirms no more such attributes exist.
    
    Args:
        entity_name: Name of the entity
        entity_description: Description of the entity from Step 1.4
        current_attributes: List of current attributes for this entity (from Steps 2.2-2.5)
        primary_key: Current primary key for this entity (from Step 2.7)
        relations: List of relations this entity participates in (from Steps 1.9, 1.11)
        nl_description: Optional original NL description
        domain: Optional domain context from Phase 1
        max_iterations: Maximum number of loop iterations per entity (default: 5)
        max_time_sec: Maximum wall time in seconds per entity (default: 180)
        
    Returns:
        dict: Final cleanup result with loop metadata and updated attributes list
    """
    logger.debug(f"Starting entity cleanup loop for: {entity_name}")
    
    # Track attributes to remove across iterations
    all_removed_attrs = []
    current_attrs = current_attributes.copy()
    previous_iteration = None
    
    async def cleanup_step(previous_result=None):
        """Single iteration of cleanup check."""
        nonlocal previous_iteration, current_attrs, all_removed_attrs
        
        if previous_result:
            previous_iteration = previous_result
            # Remove attributes from previous iteration
            attrs_to_remove = previous_result.get("relation_connecting_attributes", [])
            if attrs_to_remove:
                # Defensive: only request removal of attributes that still exist and are not PKs.
                curr_names = [extract_attribute_name(a) for a in current_attrs]
                curr_set = {n for n in curr_names if n}
                pk_set = {p for p in (primary_key or []) if p}
                valid_attrs_to_remove = [a for a in attrs_to_remove if a in curr_set and a not in pk_set]
                if not valid_attrs_to_remove:
                    logger.warning(
                        f"Entity {entity_name}: cleanup requested removals not applicable to current attribute list; "
                        f"requested={attrs_to_remove}, current={sorted(curr_set)}"
                    )
                else:
                    # Apply removals via LLM (do not delete in Python)
                    current_attrs = await _apply_attribute_removals_via_llm(
                        entity_name=entity_name,
                        entity_description=entity_description,
                        current_attributes=current_attrs,
                        primary_key=primary_key,
                        attrs_to_remove=valid_attrs_to_remove,
                        relations=relations,
                        nl_description=nl_description,
                        domain=domain,
                    )
                    all_removed_attrs.extend(valid_attrs_to_remove)
                    logger.debug(
                        f"Entity {entity_name}: Removed {valid_attrs_to_remove} from attributes list. "
                        f"Remaining: {[extract_attribute_name(a) for a in current_attrs]}"
                    )
        
        result = await step_2_14_entity_cleanup_single(
            entity_name=entity_name,
            entity_description=entity_description,
            current_attributes=current_attrs,
            primary_key=primary_key,
            relations=relations,
            nl_description=nl_description,
            domain=domain,
            previous_iteration=previous_iteration,
        )
        return result
    
    # Termination check: no_more_attributes must be True
    def termination_check(result: Dict[str, Any]) -> bool:
        return result.get("no_more_attributes", False)
    
    # Configure loop
    cfg = get_phase2_config()
    loop_config = LoopConfig(
        max_iterations=max_iterations or cfg.step_2_14_max_iterations,
        max_wall_time_sec=max_time_sec or cfg.step_2_14_max_time_sec,
        oscillation_window=3,
        enable_cycle_detection=True,
    )
    
    # Execute loop
    executor = SafeLoopExecutor()
    loop_result = await executor.run_loop(
        step_func=cleanup_step,
        termination_check=termination_check,
        config=loop_config,
    )
    
    final_result = loop_result["result"]
    iterations = loop_result["iterations"]
    terminated_by = loop_result["terminated_by"]
    
    logger.info(
        f"Entity cleanup loop for '{entity_name}' completed: {iterations} iterations, "
        f"terminated by: {terminated_by}, no_more_attributes={final_result.get('no_more_attributes', False)}, "
        f"removed {len(all_removed_attrs)} attribute(s): {all_removed_attrs}"
    )
    
    return {
        "entity": entity_name,
        "final_result": final_result,
        "removed_attributes": all_removed_attrs,
        "remaining_attributes": current_attrs,
        "loop_metadata": {
            "iterations": iterations,
            "terminated_by": terminated_by,
        }
    }


async def step_2_14_entity_cleanup_batch(
    entities: List[Dict[str, Any]],
    entity_attributes: Dict[str, List[Dict[str, Any]]],
    primary_keys: Dict[str, List[str]],
    relations: List[Dict[str, Any]],
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
    max_iterations: int = 5,
    max_time_sec: int = 180,
) -> Dict[str, Any]:
    """
    Step 2.14: Clean up all entities (parallel execution).
    
    Each entity is cleaned in parallel, and each cleanup loops until no_more_attributes is True.
    
    Args:
        entities: List of entities with name and description from Phase 1
        entity_attributes: Dictionary mapping entity names to their attributes (from Steps 2.2-2.5)
        primary_keys: Dictionary mapping entity names to their primary keys (from Step 2.7)
        relations: List of all relations from Phase 1 (Steps 1.9, 1.11)
        nl_description: Optional original NL description
        domain: Optional domain context from Phase 1
        max_iterations: Maximum number of loop iterations per entity (default: 5)
        max_time_sec: Maximum wall time in seconds per entity (default: 180)
        
    Returns:
        dict: Cleanup results for all entities with updated attribute lists
    """
    logger.info(f"Starting Step 2.14: Entity Cleanup for {len(entities)} entities")
    
    if not entities:
        logger.warning("No entities provided for cleanup")
        return {"entity_results": {}, "updated_attributes": {}}
    
    # Execute in parallel for all entities
    import asyncio
    
    cfg = get_phase2_config()
    tasks = []
    for entity in entities:
        entity_name = entity.get("name", "Unknown") if isinstance(entity, dict) else getattr(entity, "name", "Unknown")
        entity_desc = entity.get("description", "") if isinstance(entity, dict) else getattr(entity, "description", "")
        
        # Get current attributes for this entity
        current_attrs = entity_attributes.get(entity_name, [])
        
        # Get primary key for this entity
        entity_pk = primary_keys.get(entity_name, [])
        
        # Find relations this entity participates in
        entity_relations = [
            rel for rel in relations
            if entity_name in rel.get("entities", [])
        ]
        
        task = step_2_14_entity_cleanup_single_with_loop(
            entity_name=entity_name,
            entity_description=entity_desc,
            current_attributes=current_attrs,
            primary_key=entity_pk,
            relations=entity_relations,
            nl_description=nl_description,
            domain=domain,
            max_iterations=max_iterations or cfg.step_2_14_max_iterations,
            max_time_sec=max_time_sec or cfg.step_2_14_max_time_sec,
        )
        tasks.append((entity_name, task))
    
    # Wait for all tasks to complete
    results = await asyncio.gather(
        *[task for _, task in tasks],
        return_exceptions=True
    )
    
    # Process results
    entity_results = {}
    updated_attributes = {}
    total_removed = 0
    
    for i, ((entity_name, _), result) in enumerate(zip(tasks, results)):
        if isinstance(result, Exception):
            logger.error(f"Error processing entity {entity_name}: {result}")
            entity_results[entity_name] = {
                "entity": entity_name,
                "final_result": {
                    "entity": entity_name,
                    "relation_connecting_attributes": [],
                    "should_remove": False,
                    "no_more_attributes": True,
                    "reasoning": f"Error during cleanup: {str(result)}"
                },
                "removed_attributes": [],
                "remaining_attributes": entity_attributes.get(entity_name, []),
            }
            updated_attributes[entity_name] = entity_attributes.get(entity_name, [])
        else:
            entity_results[entity_name] = result
            updated_attributes[entity_name] = result.get("remaining_attributes", [])
            total_removed += len(result.get("removed_attributes", []))
    
    logger.info(
        f"Entity cleanup completed: {len(entity_results)} entities processed, "
        f"{total_removed} total relation-connecting attributes removed"
    )
    
    return {
        "entity_results": entity_results,
        "updated_attributes": updated_attributes,
    }

