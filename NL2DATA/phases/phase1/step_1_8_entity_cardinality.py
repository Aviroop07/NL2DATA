"""Phase 1, Step 1.8: Entity Cardinality & Table Type.

Determines expected cardinality and classifies table type (fact vs dimension) for each entity.
"""

from typing import Dict, Any, List, Optional, Literal
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.phases.phase1.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.phases.phase1.utils.data_extraction import (
    extract_entity_name,
    extract_entity_description,
)
from NL2DATA.utils.tools.validation_tools import _verify_evidence_substring_impl

logger = get_logger(__name__)


class EntityCardinalityInfo(BaseModel):
    """Cardinality and table type information for a single entity."""
    entity: str = Field(description="Entity name")
    has_explicit_cardinality: bool = Field(description="Whether cardinality is explicitly mentioned")
    cardinality: Optional[Literal["small", "medium", "large", "very_large"]] = Field(
        default=None,
        description="Expected cardinality: small (<1K), medium (1K-100K), large (100K-10M), very_large (>10M)"
    )
    cardinality_hint: str = Field(
        default="",
        description="Verbatim substring from description if cardinality was explicitly mentioned, else empty string"
    )
    table_type: Optional[Literal["fact", "dimension"]] = Field(
        default=None,
        description="Table type classification: fact (transactional/event data) or dimension (reference data)"
    )
    reasoning: str = Field(description="Reasoning for cardinality and table type decisions")

    model_config = ConfigDict(extra="forbid")


class EntityCardinalityOutput(BaseModel):
    """Output structure for entity cardinality analysis."""
    entity_info: List[EntityCardinalityInfo] = Field(
        description="List of cardinality and table type information for each entity"
    )


@traceable_step("1.8", phase=1, tags=["entity_cardinality"])
async def step_1_8_entity_cardinality_single(
    entity_name: str,
    entity_description: Optional[str] = None,
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 1.8 (per-entity): Determine cardinality and table type for a single entity.
    
    This is designed to be called in parallel for multiple entities.
    
    Args:
        entity_name: Name of the entity
        entity_description: Optional description of the entity
        nl_description: Optional original NL description for context
        domain: Optional domain context
        
    Returns:
        dict: Cardinality and table type information for the entity
        
    Example:
        >>> result = await step_1_8_entity_cardinality_single(
        ...     "Transaction",
        ...     entity_description="Financial transaction records"
        ... )
        >>> result["cardinality"]
        "very_large"
        >>> result["table_type"]
        "fact"
    """
    logger.debug(f"Analyzing cardinality for entity: {entity_name}")
    
    # Build context
    context_parts = []
    if entity_description:
        context_parts.append(f"Entity description: {entity_description}")
    
    context_msg = ""
    if context_parts:
        context_msg = "\n\nContext:\n" + "\n".join(f"- {part}" for part in context_parts)
    
    # System prompt
    system_prompt = """You are a database design assistant.

Task
Determine the expected cardinality (row count scale) and table_type (fact vs dimension) for a database entity.

Cardinality categories:
- **small**: < 1,000 rows (e.g., configuration tables, small reference data)
- **medium**: 1,000 - 100,000 rows (e.g., typical dimension tables, moderate-sized entities)
- **large**: 100,000 - 10,000,000 rows (e.g., large dimension tables, moderate fact tables)
- **very_large**: > 10,000,000 rows (e.g., transaction fact tables, event logs, time-series data)

Table type classification:
- **fact**: Transactional or event data that grows over time (e.g., Order, Transaction, Event, Log)
- **dimension**: Reference data that changes slowly (e.g., Customer, Product, Category, Location)

Consider:
- Explicit mentions in the description (e.g., "≥ 50M rows", "millions of transactions")
- Entity characteristics (transactional entities are usually facts, reference entities are dimensions)

Provide:
- has_explicit_cardinality: Whether cardinality was explicitly mentioned
- cardinality: One of the categories above, or null if cannot be determined
- cardinality_hint: If has_explicit_cardinality=true, this MUST be a verbatim substring copied from the description; otherwise it MUST be "" (empty string)
- table_type: "fact" or "dimension", or null if cannot be determined
- reasoning: REQUIRED - Clear explanation of your decisions (cannot be omitted)

Output schema (STRICT)
Return ONLY JSON with exactly:
{
  "entity": string,
  "has_explicit_cardinality": boolean,
  "cardinality": "small"|"medium"|"large"|"very_large"|null,
  "cardinality_hint": string,
  "table_type": "fact"|"dimension"|null,
  "reasoning": string
}

No markdown. No extra keys. No extra text."""
    
    # Human prompt template
    human_prompt = f"""Entity: {entity_name}{context_msg}

Original description (if available):
{{nl_description}}"""
    
    # Initialize model
    llm = get_model_for_step("1.8")  # Step 1.8 maps to "high_fanout" task type
    
    try:
        config = get_trace_config("1.8", phase=1, tags=["entity_cardinality"], additional_metadata={"entity": entity_name})
        result: EntityCardinalityInfo = await standardized_llm_call(
            llm=llm,
            output_schema=EntityCardinalityInfo,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={"nl_description": nl_description or ""},
            tools=None,
            use_agent_executor=False,
            config=config,
        )

        # Deterministic grounding enforcement for cardinality_hint
        if result.has_explicit_cardinality:
            hint = (result.cardinality_hint or "").strip()
            if not hint:
                result = result.model_copy(update={"has_explicit_cardinality": False, "cardinality_hint": ""})
            else:
                check = _verify_evidence_substring_impl(hint, nl_description or "")
                if not check.get("is_substring", False):
                    result = result.model_copy(update={"has_explicit_cardinality": False, "cardinality_hint": ""})
        else:
            if result.cardinality_hint:
                result = result.model_copy(update={"cardinality_hint": ""})
        
        # Work with Pydantic model directly
        logger.debug(
            f"Entity {entity_name}: cardinality={result.cardinality}, "
            f"table_type={result.table_type}"
        )
        
        # Convert to dict only at return boundary
        return result.model_dump()
        
    except Exception as e:
        logger.error(f"Error analyzing cardinality for entity {entity_name}: {e}", exc_info=True)
        raise


async def step_1_8_entity_cardinality(
    entities: List[Dict[str, Any]],
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 1.8: Determine cardinality and table type for all entities (parallel execution).
    
    This function orchestrates parallel execution of cardinality analysis for all entities.
    
    Args:
        entities: List of entities with name and description
        nl_description: Optional original NL description for context
        domain: Optional domain context
        
    Returns:
        dict: Cardinality and table type information for all entities
        
    Example:
        >>> result = await step_1_8_entity_cardinality(
        ...     entities=[{"name": "Transaction", "description": "..."}]
        ... )
        >>> len(result["entity_info"])
        1
    """
    logger.info(f"Starting Step 1.8: Entity Cardinality & Table Type for {len(entities)} entities")
    
    if not entities:
        logger.warning("No entities provided for cardinality analysis")
        return {"entity_info": []}
    
    # Execute in parallel for all entities
    import asyncio
    
    tasks = []
    for entity in entities:
        entity_name = extract_entity_name(entity)
        entity_desc = extract_entity_description(entity)
        
        task = step_1_8_entity_cardinality_single(
            entity_name=entity_name,
            entity_description=entity_desc,
            nl_description=nl_description,
            domain=domain,
        )
        tasks.append(task)
    
    # Wait for all tasks to complete
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Process results
    entity_info = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"Error processing entity {entities[i].get('name', 'Unknown')}: {result}")
            # Create a default entry for failed entities
            entity_name = entities[i].get("name", "Unknown") if isinstance(entities[i], dict) else getattr(entities[i], "name", "Unknown")
            entity_info.append({
                "entity": entity_name,
                "has_explicit_cardinality": False,
                "cardinality": None,
                "cardinality_hint": "",
                "table_type": None,
                "reasoning": f"Error during analysis: {str(result)}"
            })
        else:
            entity_info.append(result)
    
    logger.info(f"Entity cardinality analysis completed for {len(entity_info)} entities")
    
    # Log summary
    fact_count = sum(1 for e in entity_info if e.get("table_type") == "fact")
    dimension_count = sum(1 for e in entity_info if e.get("table_type") == "dimension")
    logger.info(f"Table type summary: {fact_count} fact tables, {dimension_count} dimension tables")
    
    return {"entity_info": entity_info}

