"""Phase 7, Step 7.4: Data Volume Specifications.

Get actual row count expectations (not just "large" vs "small").
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

from NL2DATA.phases.phase7.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


class EntityVolumeInfo(BaseModel):
    """Volume information for a single entity."""
    min_rows: int = Field(description="Minimum number of rows")
    max_rows: int = Field(description="Maximum number of rows")
    expected_rows: int = Field(description="Expected number of rows (typical value)")
    reasoning: str = Field(description="Reasoning for the volume specification")


class DataVolumeSpecificationsOutput(BaseModel):
    """Output structure for data volume specifications."""
    entity_volumes: Optional[Dict[str, EntityVolumeInfo]] = Field(
        default=None,
        description="Dictionary mapping entity names to their volume specifications"
    )


@traceable_step("7.4", phase=7, tags=['phase_7_step_4'])
async def step_7_4_data_volume_specifications(
    entities: List[Dict[str, Any]],  # All entities with cardinality info from Step 1.8
    nl_description: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 7.4 (singular, LLM): Get actual row count expectations.
    
    Args:
        entities: List of entities with cardinality info from Step 1.8
        nl_description: Optional original NL description
        
    Returns:
        dict: Volume specifications with entity_volumes dictionary
    """
    logger.info("Determining data volume specifications")
    
    # Build entity context
    entity_context = []
    for entity in entities:
        name = entity.get("name", "")
        cardinality = entity.get("cardinality", "")
        cardinality_hint = entity.get("cardinality_hint", "")
        table_type = entity.get("table_type", "")
        
        entity_context.append(
            f"- {name}: cardinality={cardinality}, hint={cardinality_hint}, type={table_type}"
        )
    
    # Get model
    model = get_model_for_step("7.4")
    
    # Create prompt
    system_prompt = """You are a data generation expert. Your task is to determine actual row count expectations for each entity.

VOLUME SPECIFICATION:
- Convert cardinality hints ("small", "medium", "large", "very_large") into actual row counts
- Consider table type (fact tables are typically larger than dimension tables)
- Provide min, max, and expected row counts
- Ensure min <= expected <= max

TYPICAL RANGES:
- Small: 100 - 10,000 rows
- Medium: 10,000 - 1,000,000 rows
- Large: 1,000,000 - 100,000,000 rows
- Very Large: 100,000,000+ rows

Return actual row count specifications for realistic data generation."""
    
    human_prompt = f"""Entities with Cardinality Information:
{chr(10).join(entity_context)}

Natural Language Description:
{nl_description or 'No description provided'}

Determine actual row count expectations (min, max, expected) for each entity."""
    
    # Create structured chain
    # Invoke standardized LLM call
    try:
        result: DataVolumeSpecificationsOutput = await standardized_llm_call(
            llm=model,
            output_schema=DataVolumeSpecificationsOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={},  # No template variables since human_prompt is already formatted
        )
        
        # Work with Pydantic model directly
        # Convert EntityVolumeInfo objects to dicts
        entity_volumes = {}
        if result.entity_volumes:
            entity_volumes = {
                entity_name: volume_info.model_dump()
                for entity_name, volume_info in result.entity_volumes.items()
            }
        
        logger.info(f"Data volume specifications completed: {len(entity_volumes)} entities")
        
        return {"entity_volumes": entity_volumes}
    except Exception as e:
        logger.error(f"Data volume specifications failed: {e}")
        raise

