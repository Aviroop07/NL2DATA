"""Phase 1, Step 1.10: Schema Connectivity Validation.

Ensures all entities are connected through relations (no orphan entities).
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.phases.phase1.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.phases.phase1.utils import (
    extract_entity_name,
    build_entity_list_string,
    build_relation_list_string,
)
from NL2DATA.utils.tools.validation_tools import _check_entity_connectivity_impl

logger = get_logger(__name__)


class ConnectivityValidationOutput(BaseModel):
    """Output structure for connectivity validation."""
    orphan_entities: List[str] = Field(
        default_factory=list,
        description="List of entity names that are not connected to any other entities"
    )
    connectivity_status: Optional[Dict[str, bool]] = Field(
        default_factory=dict,
        description="Dictionary mapping entity names to their connectivity status (True = connected, False = orphan)"
    )
    suggested_relations: List[str] = Field(
        default_factory=list,
        description="List of suggested relations to connect orphan entities"
    )
    reasoning: str = Field(description="Explanation of connectivity analysis and recommendations")
    
    model_config = ConfigDict(extra="forbid")


@traceable_step("1.10", phase=1, tags=["schema_connectivity"])
async def step_1_10_schema_connectivity(
    entities: List[Dict[str, Any]],
    relations: List[Dict[str, Any]],
    nl_description: Optional[str] = None,
    previous_result: Optional[Dict[str, Any]] = None,
    domain: Optional[str] = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    Step 1.10: Ensure all entities are connected through relations.
    
    Orphan entities may indicate missing relationships or unnecessary entities.
    This step catches schema design issues early.
    
    This step supports iterative refinement: if orphans are found, it should loop back
    to Step 1.9 to extract additional relations. Use step_1_10_schema_connectivity_with_loop()
    for automatic looping with safety guardrails.
    
    Args:
        entities: List of all entities in the schema
        relations: List of all relations from Step 1.9
        nl_description: Optional original NL description for context
        previous_result: Optional previous iteration result (for loop support)
        
    Returns:
        dict: Connectivity validation result with orphan_entities, connectivity_status, suggested_relations, and reasoning
        
    Example:
        >>> result = await step_1_10_schema_connectivity(
        ...     entities=[{"name": "Customer"}, {"name": "Order"}],
        ...     relations=[{"entities": ["Customer", "Order"]}]
        ... )
        >>> len(result["orphan_entities"])
        0
    """
    iteration_num = (previous_result.get("iteration", 0) + 1) if previous_result else 1
    if iteration_num > 1:
        logger.info(f"Starting Step 1.10: Schema Connectivity Validation (iteration {iteration_num})")
    else:
        logger.info("Starting Step 1.10: Schema Connectivity Validation")
    
    # Build entity and relation lists using utilities
    entity_list_str = build_entity_list_string(entities, include_descriptions=False, prefix="- ")
    relation_list_str = build_relation_list_string(relations)
    
    # System prompt
    system_prompt = """You are a database design assistant. Your task is to validate that all entities in a database schema are connected through relationships.

An entity is considered **connected** if it participates in at least one relationship with another entity. An entity is considered an **orphan** if it has no relationships with any other entities.

Orphan entities are problematic because:
- They cannot be accessed through joins from other entities
- They may indicate missing relationships
- They may be unnecessary entities that should be removed
- They break schema connectivity and data flow

Your task:
1. Identify all orphan entities (entities with no relationships)
2. For each entity, determine its connectivity status
3. Suggest relationships that could connect orphan entities to the rest of the schema
4. Provide reasoning for your analysis

Important:
- A schema should ideally have all entities connected (directly or indirectly)
- Some entities may be intentionally standalone (e.g., configuration tables), but this should be rare
- If orphans are found, suggest how to connect them based on domain knowledge and the original description
- You MUST respond with valid JSON format only

You have access to a validation tool: check_entity_connectivity. You may use this tool to check if entities are connected through relations, but you MUST still return your final answer as structured JSON.

CRITICAL: After using any tools, you MUST return your final response in the required JSON format. Do NOT return tool calls as your final answer.

Provide your response as a JSON object with:
- orphan_entities: List of entity names that are orphans (empty array if none)
- connectivity_status: Dictionary mapping each entity name to true (connected) or false (orphan)
- suggested_relations: List of suggested relationship descriptions to connect orphans (empty array if none)
- reasoning: REQUIRED - Clear explanation of your analysis and recommendations (cannot be omitted)"""
    
    # Human prompt template
    human_prompt = f"""Entities in the schema:
{{entity_list}}

Relations in the schema:
{{relation_list}}

Original description (if available):
{{nl_description}}"""
    
    # Initialize model
    llm = get_model_for_step("1.10")  # Step 1.10 maps to "reasoning" task type
    
    # Create bound version of check_entity_connectivity with schema_state
    schema_state = {"entities": entities, "relations": relations}
    def check_entity_connectivity_bound(entity: str) -> Dict[str, Any]:
        """Bound version of check_entity_connectivity with schema_state."""
        # IMPORTANT: check_entity_connectivity is a LangChain @tool (StructuredTool) and is not callable.
        # Use the pure implementation to avoid "'StructuredTool' object is not callable".
        return _check_entity_connectivity_impl(entity, relations)
    
    try:
        logger.debug("Invoking LLM for schema connectivity validation")
        config = get_trace_config("1.10", phase=1, tags=["schema_connectivity"])
        result: ConnectivityValidationOutput = await standardized_llm_call(
            llm=llm,
            output_schema=ConnectivityValidationOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={
                "entity_list": entity_list_str,
                "relation_list": relation_list_str,
                "nl_description": nl_description or "",
            },
            tools=[check_entity_connectivity_bound],
            use_agent_executor=True,  # Use agent executor for tool calls
            decouple_tools=True,  # Decouple tool calling from JSON generation
            config=config,
        )
        
        # Work with Pydantic model directly
        orphan_count = len(result.orphan_entities)
        logger.info(f"Schema connectivity validation completed: {orphan_count} orphan entities found")
        
        if orphan_count > 0:
            logger.warning(f"Orphan entities detected: {', '.join(result.orphan_entities)}")
            suggested_count = len(result.suggested_relations)
            if suggested_count > 0:
                logger.info(f"Suggested {suggested_count} relations to connect orphans")
        else:
            logger.info("All entities are connected - schema connectivity is good")
        
        # Convert to dict and add iteration info for loop tracking
        result_dict = result.model_dump()
        result_dict["iteration"] = iteration_num
        result_dict["needs_loop"] = orphan_count > 0
        
        return result_dict
        
    except Exception as e:
        logger.error(f"Error in schema connectivity validation: {e}", exc_info=True)
        raise


async def step_1_10_schema_connectivity_with_loop(
    entities: List[Dict[str, Any]],
    relations: List[Dict[str, Any]],
    nl_description: Optional[str] = None,
    max_iterations: int = 3,
    max_time_sec: int = 180,
) -> Dict[str, Any]:
    """
    Step 1.10 with automatic looping: loops back to relation extraction if orphans found.
    
    This function implements the conditional loop specified in the plan: if orphans are found,
    it loops back to Step 1.9 (relation extraction) to add missing relations, then re-validates.
    
    Args:
        entities: List of all entities in the schema
        relations: Initial list of relations from Step 1.9
        nl_description: Optional original NL description for context
        max_iterations: Maximum number of loop iterations (default: 3)
        max_time_sec: Maximum wall time in seconds (default: 180)
        
    Returns:
        dict: Final connectivity validation result with loop metadata
        
    Example:
        >>> result = await step_1_10_schema_connectivity_with_loop(
        ...     entities=[{"name": "Customer"}, {"name": "Order"}],
        ...     relations=[{"entities": ["Customer", "Order"]}]
        ... )
        >>> result["final_result"]["orphan_entities"]
        []
    """
    from NL2DATA.utils.loops import SafeLoopExecutor, LoopConfig
    
    logger.info("Starting Step 1.10: Schema Connectivity Validation (with loop support)")
    
    current_relations = relations.copy()
    loop_history = []
    
    def _parse_suggested_relations(suggested_relations: List[str], entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Parse suggested relation strings into relation dictionaries.
        
        Example: "Anomaly is detected by Sensor" -> {"entities": ["Anomaly", "Sensor"], "description": "Anomaly is detected by Sensor"}
        """
        entity_names = {extract_entity_name(e) for e in entities}
        parsed_relations = []
        
        for suggested_rel in suggested_relations:
            if not suggested_rel or not suggested_rel.strip():
                continue
            
            # Try to find entity names in the suggested relation string
            found_entities = []
            for entity_name in entity_names:
                # Check if entity name appears in the relation string (case-insensitive, whole word)
                import re
                pattern = r'\b' + re.escape(entity_name) + r'\b'
                if re.search(pattern, suggested_rel, re.IGNORECASE):
                    found_entities.append(entity_name)
            
            # Only create relation if we found at least 2 entities
            if len(found_entities) >= 2:
                # Remove duplicates while preserving order
                unique_entities = []
                seen = set()
                for e in found_entities:
                    if e not in seen:
                        unique_entities.append(e)
                        seen.add(e)
                
                parsed_relations.append({
                    "entities": unique_entities,
                    "description": suggested_rel.strip(),
                    "type": "binary" if len(unique_entities) == 2 else "n-ary",
                    "source": "connectivity_suggestion"
                })
            else:
                logger.debug(f"Could not parse suggested relation: {suggested_rel} (found {len(found_entities)} entities)")
        
        return parsed_relations
    
    async def connectivity_check_step(previous_result=None):
        """Single iteration of connectivity check."""
        result = await step_1_10_schema_connectivity(
            entities=entities,
            relations=current_relations,
            nl_description=nl_description,
            previous_result=previous_result,
        )
        loop_history.append(result)
        
        # If orphans found and relations suggested, add them to current_relations for next iteration
        if result.get("orphan_entities") and result.get("suggested_relations"):
            suggested = result.get("suggested_relations", [])
            parsed = _parse_suggested_relations(suggested, entities)
            
            if parsed:
                # Check if relations already exist (avoid duplicates)
                existing_relation_keys = {
                    tuple(sorted(rel.get("entities", []))) for rel in current_relations
                }
                
                new_relations = []
                for rel in parsed:
                    rel_key = tuple(sorted(rel.get("entities", [])))
                    if rel_key not in existing_relation_keys:
                        new_relations.append(rel)
                        existing_relation_keys.add(rel_key)
                
                if new_relations:
                    current_relations.extend(new_relations)
                    logger.info(
                        f"Added {len(new_relations)} suggested relations to resolve orphan entities. "
                        f"Total relations: {len(current_relations)}"
                    )
                else:
                    logger.debug("All suggested relations already exist in current relations")
        
        return result
    
    def should_terminate(result: Dict[str, Any]) -> bool:
        """Check if loop should terminate (no orphans found)."""
        orphan_count = len(result.get("orphan_entities", []))
        return orphan_count == 0
    
    # Run loop
    config = LoopConfig(
        max_iterations=max_iterations,
        max_wall_time_sec=max_time_sec,
        oscillation_window=3,
        enable_cycle_detection=True
    )
    
    executor = SafeLoopExecutor()
    loop_result = await executor.run_loop(
        step_func=connectivity_check_step,
        termination_check=should_terminate,
        config=config
    )
    
    final_result = loop_result["result"]
    
    # If orphans still exist after loop, log warning
    if final_result.get("needs_loop", False):
        logger.warning(
            f"Schema connectivity validation completed with {len(final_result.get('orphan_entities', []))} "
            f"orphan entities after {loop_result['iterations']} iterations. "
            f"Consider manual review or additional relation extraction."
        )
    else:
        logger.info(
            f"Schema connectivity validation passed after {loop_result['iterations']} iteration(s)"
        )
    
    return {
        "final_result": final_result,
        "loop_metadata": {
            "iterations": loop_result["iterations"],
            "terminated_by": loop_result["terminated_by"],
            "history": loop_history
        }
    }

