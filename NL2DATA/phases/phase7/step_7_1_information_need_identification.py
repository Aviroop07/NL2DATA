"""Phase 7, Step 7.1: Information Need Identification.

Identify information needs (queries) from the natural language description.
Iterative loop continues until LLM suggests no more additions or deletions.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.phases.phase7.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.loops import SafeLoopExecutor, LoopConfig
from NL2DATA.utils.prompt_helpers import generate_output_structure_section_with_custom_requirements

logger = get_logger(__name__)


class InformationNeed(BaseModel):
    """A single information need."""
    description: str = Field(description="Description of the information need")
    entities_involved: List[str] = Field(
        default_factory=list,
        description="List of entity names involved in this information need"
    )
    conditions: Optional[List[str]] = Field(
        default=None,
        description="Optional list of conditions or filters for this information need"
    )

    model_config = ConfigDict(extra="forbid")


class InformationNeedOutput(BaseModel):
    """Output structure for information need identification."""
    information_needs: List[InformationNeed] = Field(
        default_factory=list,
        description="List of information needs"
    )
    additions: List[str] = Field(
        default_factory=list,
        description="List of newly added information need descriptions"
    )
    deletions: List[str] = Field(
        default_factory=list,
        description="List of information need descriptions that should be removed"
    )
    no_more_changes: bool = Field(
        description="Whether the LLM suggests no more additions or deletions (termination condition)"
    )
    reasoning: Optional[str] = Field(
        default=None,
        description="Reasoning for the information needs and termination decision"
    )

    model_config = ConfigDict(extra="forbid")


@traceable_step("7.1", phase=7, tags=['phase_7_step_1'])
async def step_7_1_information_need_identification(
    nl_description: str,
    entities: List[Dict[str, Any]],
    relations: List[Dict[str, Any]],
    attributes: Dict[str, List[Dict[str, Any]]],
    primary_keys: Dict[str, List[str]],
    foreign_keys: List[Dict[str, Any]],
    domain: Optional[str] = None,
    previous_information_needs: Optional[List[Dict[str, Any]]] = None,
) -> InformationNeedOutput:
    """
    Step 7.1 (loop, LLM): Identify information needs from the description.
    
    This step iteratively identifies information needs until no more additions or deletions
    are suggested. Designed to be called in a loop until no_more_changes is True.
    
    Args:
        nl_description: Original natural language description
        entities: List of entities
        relations: List of relations
        attributes: Dictionary mapping entity names to their attributes
        primary_keys: Dictionary mapping entity names to their primary keys
        foreign_keys: List of foreign key definitions
        domain: Optional domain context
        previous_information_needs: Optional previous information needs from loop iterations
        
    Returns:
        dict: Information need identification result with information_needs, additions, deletions, no_more_changes
        
    Example:
        >>> result = await step_7_1_information_need_identification("Find all customers", {...})
        >>> len(result["information_needs"]) > 0
        True
    """
    logger.debug("Identifying information needs from description")
    
    # Build entity/relation summary
    entity_summary = []
    for entity in entities:
        entity_name = entity.get("name", "") if isinstance(entity, dict) else str(entity)
        entity_desc = entity.get("description", "") if isinstance(entity, dict) else ""
        attrs = attributes.get(entity_name, [])
        pk = primary_keys.get(entity_name, [])
        entity_summary.append(f"- {entity_name}: {entity_desc} (PK: {', '.join(pk)}, Attributes: {len(attrs)})")
    
    relation_summary = []
    for relation in relations:
        rel_entities = relation.get("entities", [])
        rel_desc = relation.get("description", "")
        relation_summary.append(f"- {', '.join([e if isinstance(e, str) else e.get('name', '') for e in rel_entities])}: {rel_desc}")
    
    # Build prompt
    previous_context = ""
    if previous_information_needs:
        prev_descriptions = [need.get("description", "") for need in previous_information_needs if isinstance(need, dict)]
        previous_context = f"\n\nPrevious information needs:\n" + "\n".join(f"- {desc}" for desc in prev_descriptions)
    
    # Generate output structure section from Pydantic model
    output_structure_section = generate_output_structure_section_with_custom_requirements(
        output_schema=InformationNeedOutput,
        additional_requirements=[
            "Each information need should have a clear description, entities involved, and any specific conditions or filters",
        ]
    )
    
    system_prompt = f"""You are a database requirements analyst. Identify information needs (queries) from the natural language description.

An information need represents a query or information request that the database should support.
Each information need should have:
- A clear description of what information is needed
- The entities involved
- Any specific conditions or filters

Return a structured list of information needs.

{output_structure_section}"""
    
    human_prompt = f"""Natural Language Description:
{nl_description}

Domain: {domain or "Not specified"}

Entities:
{chr(10).join(entity_summary)}

Relations:
{chr(10).join(relation_summary) if relation_summary else "None"}
{previous_context}

Identify all information needs (queries) that this database should support. Consider:
- What questions can users ask?
- What reports or views are needed?
- What information needs to be retrieved?

If this is a follow-up iteration, review previous information needs and suggest additions or deletions."""
    
    llm = get_model_for_step("7.1")
    trace_config = get_trace_config("7.1", phase=7, tags=["phase_7_step_1"])
    
    result = await standardized_llm_call(
        llm=llm,
        output_schema=InformationNeedOutput,
        system_prompt=system_prompt,
        human_prompt_template=human_prompt,
        input_data={},
        config=trace_config,
    )
    
    return result


async def step_7_1_information_need_identification_with_loop(
    nl_description: str,
    entities: List[Dict[str, Any]],
    relations: List[Dict[str, Any]],
    attributes: Dict[str, List[Dict[str, Any]]],
    primary_keys: Dict[str, List[str]],
    foreign_keys: List[Dict[str, Any]],
    domain: Optional[str] = None,
    max_iterations: int = 10,
    max_time_sec: int = 300,
) -> Dict[str, Any]:
    """
    Step 7.1 with automatic looping: continues until no_more_changes is True.
    
    Args:
        nl_description: Original natural language description
        entities: List of entities
        relations: List of relations
        attributes: Dictionary mapping entity names to their attributes
        primary_keys: Dictionary mapping entity names to their primary keys
        foreign_keys: List of foreign key definitions
        domain: Optional domain context
        max_iterations: Maximum number of loop iterations
        max_time_sec: Maximum time in seconds for the loop
        
    Returns:
        dict: Final result with information_needs and loop_metadata
    """
    logger.info(f"Starting Step 7.1: Information Need Identification (with loop, max_iterations={max_iterations})")
    
    loop_config = LoopConfig(
        max_iterations=max_iterations,
        max_wall_time_sec=max_time_sec,
    )
    
    executor = SafeLoopExecutor()
    
    previous_information_needs = None
    
    async def step_func(previous_result=None):
        nonlocal previous_information_needs
        
        result = await step_7_1_information_need_identification(
            nl_description=nl_description,
            entities=entities,
            relations=relations,
            attributes=attributes,
            primary_keys=primary_keys,
            foreign_keys=foreign_keys,
            domain=domain,
            previous_information_needs=previous_information_needs,
        )
        
        # Update previous information needs for next iteration
        information_needs = result.information_needs if hasattr(result, 'information_needs') else result.get("information_needs", [])
        if information_needs:
            # Convert to dict format for next iteration
            if hasattr(information_needs[0], 'model_dump'):
                previous_information_needs = [need.model_dump() for need in information_needs]
            else:
                previous_information_needs = information_needs
        
        return result
    
    def should_terminate(result) -> bool:
        """Check if loop should terminate (no more changes)."""
        if result is None:
            return False
        if hasattr(result, 'no_more_changes'):
            return result.no_more_changes
        return result.get("no_more_changes", False)
    
    loop_result = await executor.run_loop(
        step_func=step_func,
        termination_check=should_terminate,
        config=loop_config
    )
    
    final_result = loop_result["result"]
    
    # Convert final_result to dict if it's a Pydantic model
    if hasattr(final_result, 'model_dump'):
        final_result_dict = final_result.model_dump()
    else:
        final_result_dict = final_result
    
    return {
        "final_result": final_result_dict,
        "loop_metadata": {
            "iterations": loop_result.get("iterations", 0),
            "terminated_by": loop_result.get("terminated_by", "unknown"),
        },
    }
