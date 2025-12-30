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
    # NOTE:
    # Some LLM tool-calling paths occasionally emit an empty tool payload `{}`.
    # If `reasoning` is required, Pydantic validation fails and the entire pipeline aborts.
    # We therefore provide a safe default and fill in a deterministic fallback reasoning in code.
    reasoning: str = Field(
        default="",
        description="Explanation of connectivity analysis and recommendations"
    )
    
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

    # Deterministic baseline connectivity (do not rely on LLM for orphan detection)
    entity_names: List[str] = [extract_entity_name(e) for e in entities]
    baseline_connectivity_status: Dict[str, bool] = {}
    baseline_orphan_entities: List[str] = []

    for entity_name in entity_names:
        conn = _check_entity_connectivity_impl(entity_name, relations)
        is_connected = bool(conn.get("is_connected", False))
        baseline_connectivity_status[entity_name] = is_connected
        if not is_connected:
            baseline_orphan_entities.append(entity_name)

    baseline_reasoning = (
        f"Deterministic connectivity check: {len(baseline_orphan_entities)} orphan entities found. "
        f"Orphans: {baseline_orphan_entities}"
        if baseline_orphan_entities
        else "Deterministic connectivity check: all entities are connected (no orphans found)."
    )
    
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

Deterministic connectivity check (baseline):
- Orphan entities: {baseline_orphan_entities}

Original description (if available):
{{nl_description}}"""
    
    # Initialize model
    llm = get_model_for_step("1.10")  # Step 1.10 maps to "reasoning" task type
    
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
            tools=None,
            use_agent_executor=False,
            config=config,
        )
        
        # Trust deterministic connectivity for control flow; keep LLM for suggestions/reasoning.
        orphan_count = len(baseline_orphan_entities)
        logger.info(f"Schema connectivity validation completed: {orphan_count} orphan entities found")
        
        if orphan_count > 0:
            logger.warning(f"Orphan entities detected: {', '.join(baseline_orphan_entities)}")
            suggested_count = len(result.suggested_relations or [])
            if suggested_count > 0:
                logger.info(f"Suggested {suggested_count} relations to connect orphans")
        else:
            logger.info("All entities are connected - schema connectivity is good")
        
        # Convert to dict and add iteration info for loop tracking
        result_dict = result.model_dump()
        result_dict["orphan_entities"] = baseline_orphan_entities
        result_dict["connectivity_status"] = baseline_connectivity_status
        if not (result_dict.get("reasoning") or "").strip():
            result_dict["reasoning"] = baseline_reasoning
        result_dict["iteration"] = iteration_num
        result_dict["needs_loop"] = orphan_count > 0
        
        return result_dict
        
    except Exception as e:
        # Do not abort the pipeline on LLM/tooling errors: return deterministic connectivity results.
        logger.error(f"Error in schema connectivity validation: {e}", exc_info=True)

        return {
            "orphan_entities": baseline_orphan_entities,
            "connectivity_status": baseline_connectivity_status,
            "suggested_relations": [],
            "reasoning": baseline_reasoning,
            "iteration": iteration_num,
            "needs_loop": len(baseline_orphan_entities) > 0,
        }


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
        dict: Final connectivity validation result with loop metadata.
              Also includes `updated_relations` (the relation list after applying any
              connectivity-suggested relations) so downstream steps can use the
              modified ER graph.
        
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
    
    # NOTE:
    # We intentionally maintain a working relation list (`current_relations`) that can be
    # augmented with connectivity-suggested relations during the loop. The caller may
    # optionally use `updated_relations` downstream.
    current_relations = relations.copy()
    loop_history = []
    
    def _parse_suggested_relations(suggested_relations: List[str], entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Parse suggested relation strings into relation dictionaries.
        
        Example: "Anomaly is detected by Sensor" -> {"entities": ["Anomaly", "Sensor"], "description": "Anomaly is detected by Sensor"}
        """
        import re

        def _camel_to_words(name: str) -> str:
            # "SensorType" -> "sensor type"; "IoTDevice" -> "io t device" (acceptable best-effort)
            s = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", name or "")
            return " ".join(s.split()).strip().lower()

        def _to_snake(name: str) -> str:
            # "SensorType" -> "sensor_type"
            s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name or "")
            return re.sub(r"_+", "_", s).strip("_").lower()

        def _variants(canonical: str) -> List[str]:
            c = (canonical or "").strip()
            if not c:
                return []
            snake = _to_snake(c)
            words = _camel_to_words(c)
            variants = [
                c,
                c.lower(),
                snake,
                snake.replace("_", " "),
                words,
            ]
            # naive plural for last token (helps match "sensors" vs "Sensor")
            if words and " " in words:
                parts = words.split()
                variants.append(" ".join(parts[:-1] + [parts[-1] + "s"]))
            elif words:
                variants.append(words + "s")
            # De-dup while preserving order
            out: List[str] = []
            seen: set[str] = set()
            for v in variants:
                vv = (v or "").strip().lower()
                if vv and vv not in seen:
                    out.append(v)
                    seen.add(vv)
            return out

        # canonical entity names
        entity_names = sorted({extract_entity_name(e) for e in entities if extract_entity_name(e)})

        # precompute regex patterns for each canonical entity name (variants)
        patterns: Dict[str, List[re.Pattern]] = {}
        for canonical in entity_names:
            pats: List[re.Pattern] = []
            for v in _variants(canonical):
                # Use word boundary-ish matching; allow spaces/underscores by matching exact variant text.
                pats.append(re.compile(r"\b" + re.escape(v) + r"\b", re.IGNORECASE))
            patterns[canonical] = pats
        parsed_relations = []
        
        for suggested_rel in suggested_relations:
            if not suggested_rel or not suggested_rel.strip():
                continue
            
            # Try to find entity names in the suggested relation string
            found_entities = []
            for canonical in entity_names:
                for pat in patterns.get(canonical, []):
                    if pat.search(suggested_rel):
                        found_entities.append(canonical)
                        break
            
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
        
        # If orphans found, actively try to enrich the ER graph for the next iteration.
        # 1) Prefer looping back to Step 1.9 (key relations extraction) with orphan guidance.
        # 2) Also accept Step 1.10 textual suggestions as a secondary source.
        orphan_entities = result.get("orphan_entities") or []
        if orphan_entities:
            try:
                from NL2DATA.phases.phase1.step_1_9_key_relations_extraction import (
                    step_1_9_key_relations_extraction,
                )

                relation_result = await step_1_9_key_relations_extraction(
                    entities=entities,
                    nl_description=nl_description or "",
                    domain=None,
                    mentioned_relations=None,
                    focus_entities=orphan_entities,
                )
                candidate_relations = relation_result.get("relations", []) if isinstance(relation_result, dict) else []
            except Exception as e:
                logger.warning(
                    f"Step 1.10 loop: failed to re-run Step 1.9 for orphan entities. error={e}",
                    exc_info=True,
                )
                candidate_relations = []

            # Secondary source: Step 1.10's own suggested_relations (string descriptions)
            suggested = result.get("suggested_relations") or []
            parsed = _parse_suggested_relations(suggested, entities) if suggested else []
            candidate_relations.extend(parsed)

            if candidate_relations:
                def _rel_key(rel: Dict[str, Any]) -> tuple:
                    ents = rel.get("entities", []) or []
                    ents_key = tuple(sorted([str(e).strip() for e in ents if str(e).strip()]))
                    rel_type = str(rel.get("type", "") or "").strip().lower()
                    desc = str(rel.get("description", "") or "").strip().lower()
                    return (ents_key, rel_type, desc)

                existing_keys = {_rel_key(r) for r in current_relations if isinstance(r, dict)}
                new_relations = []
                for rel in candidate_relations:
                    if not isinstance(rel, dict):
                        continue
                    k = _rel_key(rel)
                    # Require at least two entities for any new relation
                    if not k[0] or len(k[0]) < 2:
                        continue
                    if k not in existing_keys:
                        new_relations.append(rel)
                        existing_keys.add(k)

                if new_relations:
                    current_relations.extend(new_relations)
                    logger.info(
                        f"Step 1.10 loop: added {len(new_relations)} new relations to resolve orphan entities. "
                        f"Total relations: {len(current_relations)}"
                    )
                else:
                    logger.debug("Step 1.10 loop: no new unique relations to add")
        
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
        "updated_relations": current_relations,
        "loop_metadata": {
            "iterations": loop_result["iterations"],
            "terminated_by": loop_result["terminated_by"],
            "history": loop_history,
        },
    }

