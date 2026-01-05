"""Phase 7, Step 7.2: Information Completeness Check.

Check if the current schema is complete for each information need.
Iterative loop continues until all information needs are satisfied or no more improvements are possible.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.phases.phase7.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.prompt_helpers import generate_output_structure_section_with_custom_requirements

logger = get_logger(__name__)


class InformationCompletenessOutput(BaseModel):
    """Output structure for information completeness check."""
    is_complete: bool = Field(
        description="Whether the schema is complete for this information need"
    )
    missing_attributes: List[str] = Field(
        default_factory=list,
        description="List of missing attributes needed to satisfy this information need"
    )
    missing_entities: List[str] = Field(
        default_factory=list,
        description="List of missing entities needed to satisfy this information need"
    )
    missing_relations: List[str] = Field(
        default_factory=list,
        description="List of missing relations needed to satisfy this information need"
    )
    suggestions: List[str] = Field(
        default_factory=list,
        description="Suggestions for improving schema completeness"
    )
    reasoning: str = Field(
        description="Reasoning for the completeness assessment"
    )

    model_config = ConfigDict(extra="forbid")


@traceable_step("7.2", phase=7, tags=['phase_7_step_2'])
async def step_7_2_information_completeness(
    information_need: Dict[str, Any],
    entities: List[Dict[str, Any]],
    relations: List[Dict[str, Any]],
    attributes: Dict[str, List[Dict[str, Any]]],
    domain: Optional[str] = None,
) -> InformationCompletenessOutput:
    """
    Step 6.2 (per-information need, LLM): Check if schema is complete for an information need.
    
    Args:
        information_need: Information need with description, entities_involved
        entities: List of entities
        relations: List of relations
        attributes: Dictionary mapping entity names to their attributes
        domain: Optional domain context
        
    Returns:
        dict: Completeness check result with is_complete, missing_attributes, missing_entities, missing_relations, suggestions
        
    Example:
        >>> info_need = {"description": "Find all customers", "entities_involved": ["Customer"]}
        >>> result = await step_6_2_information_completeness(info_need, {...})
        >>> "is_complete" in result
        True
    """
    logger.debug(f"Checking completeness for information need: {information_need.get('description', '')}")
    
    # Build entity/relation/attribute summary
    entity_summary = []
    for entity in entities:
        entity_name = entity.get("name", "") if isinstance(entity, dict) else str(entity)
        entity_desc = entity.get("description", "") if isinstance(entity, dict) else ""
        attrs = attributes.get(entity_name, [])
        attr_names = [attr.get("name", "") if isinstance(attr, dict) else str(attr) for attr in attrs]
        entity_summary.append(f"- {entity_name}: {entity_desc} (Attributes: {', '.join(attr_names)})")
    
    relation_summary = []
    for relation in relations:
        rel_entities = relation.get("entities", [])
        rel_desc = relation.get("description", "")
        relation_summary.append(f"- {', '.join([e if isinstance(e, str) else e.get('name', '') for e in rel_entities])}: {rel_desc}")
    
    # Generate output structure section from Pydantic model
    output_structure_section = generate_output_structure_section_with_custom_requirements(
        output_schema=InformationCompletenessOutput,
        additional_requirements=[
            "An information need is complete if the schema contains all necessary entities, attributes, and relations",
            "If something is missing, identify what needs to be added",
        ]
    )
    
    system_prompt = f"""You are a database schema analyst. Check if the current schema is complete for a given information need.

An information need is complete if the schema contains all necessary:
- Entities
- Attributes
- Relations

If something is missing, identify what needs to be added.

{output_structure_section}"""
    
    human_prompt = f"""Information Need:
Description: {information_need.get('description', '')}
Entities involved: {', '.join(information_need.get('entities_involved', []))}

Current Schema:

Entities:
{chr(10).join(entity_summary)}

Relations:
{chr(10).join(relation_summary) if relation_summary else "None"}

Domain: {domain or "Not specified"}

Check if the current schema is complete for this information need. Identify any missing entities, attributes, or relations."""
    
    llm = get_model_for_step("7.2")
    trace_config = get_trace_config("7.2", phase=7, tags=["phase_7_step_2"])
    
    result = await standardized_llm_call(
        llm=llm,
        output_schema=InformationCompletenessOutput,
        system_prompt=system_prompt,
        human_prompt_template=human_prompt,
        input_data={},
        config=trace_config,
    )
    
    return result


async def step_7_2_information_completeness_batch(
    information_needs: List[Dict[str, Any]],
    entities: List[Dict[str, Any]],
    relations: List[Dict[str, Any]],
    attributes: Dict[str, List[Dict[str, Any]]],
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 6.2: Check completeness for all information needs (parallel execution).
    
    Args:
        information_needs: List of information needs
        entities: List of entities
        relations: List of relations
        attributes: Dictionary mapping entity names to their attributes
        domain: Optional domain context
        
    Returns:
        dict: Batch completeness results with results list
    """
    logger.info(f"Starting Step 7.2: Information Completeness Check for {len(information_needs)} information needs")
    
    import asyncio
    
    tasks = [
        step_7_2_information_completeness(
            information_need=info_need,
            entities=entities,
            relations=relations,
            attributes=attributes,
            domain=domain,
        )
        for info_need in information_needs
    ]
    
    results = await asyncio.gather(*tasks)
    
    # Convert Pydantic models to dicts for backward compatibility
    results_dicts = []
    for r in results:
        if hasattr(r, 'model_dump'):
            results_dicts.append(r.model_dump())
        else:
            results_dicts.append(r)
    
    return {
        "results": results_dicts,
        "total_needs": len(information_needs),
        "complete_count": sum(1 for r in results if (r.is_complete if hasattr(r, 'is_complete') else r.get("is_complete", False))),
    }
