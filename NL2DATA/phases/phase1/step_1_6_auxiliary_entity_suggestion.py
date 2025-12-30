"""Phase 1, Step 1.6: Auxiliary Entity Suggestion.

Suggests supporting entities needed for realism and schema completeness.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, ConfigDict
from typing import Literal

from NL2DATA.phases.phase1.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.tools import check_entity_name_validity, verify_entity_in_known_entities

logger = get_logger(__name__)


class AuxiliaryEntitySuggestion(BaseModel):
    name: str = Field(description="Singular, SQL-safe, PascalCase entity name")
    description: str = Field(description="What this entity represents")
    reasoning: str = Field(description="Why this entity is suggested")
    motivation: Literal["completeness", "realism", "normalization"] = Field(description="Why this adds value")
    priority: Literal["must_have", "should_have", "nice_to_have"] = Field(description="Priority level")
    trigger: str = Field(description="Short phrase (<=20 words) from the description that motivated this suggestion")

    model_config = ConfigDict(extra="forbid")


class AuxiliaryEntityOutput(BaseModel):
    """Output structure for auxiliary entity suggestion."""
    suggested_entities: List[AuxiliaryEntitySuggestion] = Field(
        default_factory=list,
        description="List of suggested auxiliary entities with priority/motivation controls"
    )
    model_config = ConfigDict(extra="forbid")


@traceable_step("1.6", phase=1, tags=["auxiliary_entity_suggestion"])
async def step_1_6_auxiliary_entity_suggestion(
    nl_description: str,
    key_entities: Optional[List[Dict[str, Any]]] = None,
    domain: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Step 1.6: Suggest supporting entities needed for realism.
    
    This step suggests auxiliary entities (e.g., "User" for "Order") that are
    needed for schema completeness beyond explicit requirements. Must be done
    before relation extraction so that relations can include both key and
    auxiliary entities.
    
    Args:
        nl_description: Natural language description of the database requirements
        key_entities: Optional list of key entities from Step 1.4
        domain: Optional domain from Steps 1.1 or 1.3
        
    Returns:
        dict: Auxiliary entity suggestion result with suggested_entities list
        
    Example:
        >>> result = await step_1_6_auxiliary_entity_suggestion(
        ...     "I need to track orders",
        ...     key_entities=[{"name": "Order", "description": "Order entity"}]
        ... )
        >>> len(result["suggested_entities"])
        1
        >>> result["suggested_entities"][0]["name"]
        "Customer"
    """
    logger.info("Starting Step 1.6: Auxiliary Entity Suggestion")
    logger.debug(f"Input description length: {len(nl_description)} characters")
    
    key_names: List[str] = []
    if key_entities:
        for e in key_entities:
            if isinstance(e, dict):
                n = (e.get("name") or "").strip()
            else:
                n = (getattr(e, "name", "") or "").strip()
            if n:
                key_names.append(n)
    
    # System prompt
    system_prompt = """You are a database design assistant.

Task
Suggest auxiliary (supporting) entities that improve completeness, realism, or normalization for the described system, even if not explicitly mentioned.

Inputs you will receive:
- Natural language description
- KeyEntitiesAlreadyIdentified: canonical list of entity names that MUST NOT be suggested again

Hard constraints
- Do NOT suggest any entity that is already in KeyEntitiesAlreadyIdentified.
- Do NOT suggest junction/bridge tables (many-to-many link entities). Those are handled later.
- Do NOT suggest attributes, metrics, or relationships; only entities that merit their own table.
- Every suggestion must be justified by a specific need tied to the description (avoid generic "nice to have" entities).

Grounding rule
For each suggested entity, include a trigger phrase (≤20 words) copied verbatim from the description that motivates why this auxiliary entity adds value.

Prioritization (no cap, but controlled)
Assign priority:
- must_have: required to faithfully model requirements described
- should_have: strongly improves realism/normalization for stated requirements
- nice_to_have: optional improvements; include only if clearly beneficial

Output (JSON only; no extra keys)
{
  "suggested_entities": [
    {
      "name": string,
      "description": string,
      "reasoning": string,
      "motivation": "completeness" | "realism" | "normalization",
      "priority": "must_have" | "should_have" | "nice_to_have",
      "trigger": string
    }
  ]
}

Naming rules
- name must be singular, SQL-safe, PascalCase.
- Keep names specific (do not over-generalize).

Tool usage (mandatory)
You have access to two tools:
1) check_entity_name_validity(name: str) -> {valid: bool, error: str|null, suggestion: str|null}
2) verify_entity_in_known_entities(entity: str, known_entities: List[str]) -> {exists: bool, error: str|null}

Before finalizing your response:
1) For EACH suggested entity:
   a) Call check_entity_name_validity with:
      {"name": "<entity.name>"}
   b) If valid=false, fix the name (use suggestion if present) and re-check.
   c) Call verify_entity_in_known_entities to ensure it's NOT already in KeyEntitiesAlreadyIdentified:
      {"entity": "<entity.name>", "known_entities": [<KeyEntitiesAlreadyIdentified_list>]}
   d) If exists=true, REMOVE this entity from suggestions (it's a duplicate).

Return JSON only. No markdown. No extra text."""
    
    # Human prompt template
    human_prompt = """Natural language description:
{nl_description}

KeyEntitiesAlreadyIdentified: {key_entities}
"""
    
    # Initialize model
    llm = get_model_for_step("1.6")  # Step 1.6 maps to "important" task type
    
    try:
        logger.debug("Invoking LLM for auxiliary entity suggestion")
        config = get_trace_config("1.6", phase=1, tags=["auxiliary_entity_suggestion"])
        result: AuxiliaryEntityOutput = await standardized_llm_call(
            llm=llm,
            output_schema=AuxiliaryEntityOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={"nl_description": nl_description, "key_entities": ", ".join(key_names)},
            tools=[check_entity_name_validity, verify_entity_in_known_entities],
            use_agent_executor=True,
            config=config,
        )
        
        # Work with Pydantic model directly
        entity_count = len(result.suggested_entities)
        logger.info(f"Auxiliary entity suggestion completed: found {entity_count} suggested entities")
        
        if entity_count > 0:
            entity_names = [e.name for e in result.suggested_entities]
            logger.info(f"Suggested auxiliary entities: {', '.join(entity_names)}")
        
        # Convert to dict only at return boundary
        return result.model_dump()
        
    except Exception as e:
        logger.error(f"Error in auxiliary entity suggestion: {e}", exc_info=True)
        raise

