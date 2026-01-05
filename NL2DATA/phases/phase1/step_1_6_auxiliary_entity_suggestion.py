"""Phase 1, Step 1.6: Auxiliary Entity Suggestion.

Suggests supporting entities needed for realism and schema completeness.
"""

from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict
from typing import Literal

from NL2DATA.phases.phase1.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.tools.validation_tools import _check_entity_name_validity_impl, _verify_entity_in_known_entities_impl
from NL2DATA.utils.prompt_helpers import generate_output_structure_section_with_custom_requirements

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
    key_entities: Optional[List] = None,
    domain: Optional[str] = None,
) -> AuxiliaryEntityOutput:
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
    
    # Generate output structure section from Pydantic model
    output_structure_section = generate_output_structure_section_with_custom_requirements(
        output_schema=AuxiliaryEntityOutput,
        additional_requirements=[
            "Naming rules: name must be singular, SQL-safe, PascalCase - keep names specific (do not over-generalize)",
            "Grounding rule: trigger MUST be a verbatim phrase (<=20 words) copied from the description",
            "Do NOT suggest entities already in KeyEntitiesAlreadyIdentified (verify with tool)",
            "Do NOT suggest junction/bridge tables or attributes/metrics - only entities that merit their own table"
        ]
    )
    
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

""" + output_structure_section + """

Tool usage (mandatory):
You have access to two tools:
1) check_entity_name_validity(name: str) -> {valid: bool, error: str|null, suggestion: str|null}
2) verify_entity_in_known_entities(entity: str, known_entities: List[str]) -> {exists: bool, error: str|null}

Before finalizing your response:
1) For EACH suggested entity:
   a) Call check_entity_name_validity with: {"name": "<entity.name>"}
   b) If valid=false, fix the name (use suggestion if present) and re-check
   c) Call verify_entity_in_known_entities to ensure it's NOT already in KeyEntitiesAlreadyIdentified
   d) If exists=true, REMOVE this entity from suggestions (it's a duplicate)"""
    
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
            tools=None,
            use_agent_executor=False,
            config=config,
        )

        # Deterministic post-validation:
        # - name must be SQL-safe (via pure impl)
        # - must NOT already exist in key_names
        # - trigger must be a verbatim substring (best-effort enforcement)
        cleaned: List[AuxiliaryEntitySuggestion] = []
        seen_names: set[str] = set()
        key_set_lower = {k.lower() for k in key_names}
        for ent in result.suggested_entities or []:
            name = (ent.name or "").strip()
            if not name:
                continue

            # Reject duplicates of existing key entities
            if name.lower() in key_set_lower:
                continue

            # Validate SQL-safe identifier shape / reserved words
            valid_check = _check_entity_name_validity_impl(name)
            if not valid_check.get("valid", False):
                suggestion = (valid_check.get("suggestion") or "").strip()
                if suggestion:
                    name = suggestion
                else:
                    continue

                # Re-check after applying suggestion
                if not _check_entity_name_validity_impl(name).get("valid", False):
                    continue

            # Dedupe suggestions by name
            if name.lower() in seen_names:
                continue
            seen_names.add(name.lower())

            # Ensure NOT in key entities (again, after potential renaming)
            if _verify_entity_in_known_entities_impl(name, key_names).get("exists", False):
                continue

            trig = (ent.trigger or "").strip()
            if trig and trig not in (nl_description or ""):
                # If trigger isn't grounded, blank it rather than failing the entity.
                ent = ent.model_copy(update={"trigger": ""})

            ent = ent.model_copy(update={"name": name})
            cleaned.append(ent)

        result = result.model_copy(update={"suggested_entities": cleaned})
        
        # Work with Pydantic model directly
        entity_count = len(result.suggested_entities)
        logger.info(f"Auxiliary entity suggestion completed: found {entity_count} suggested entities")
        
        if entity_count > 0:
            entity_names = [e.name for e in result.suggested_entities]
            logger.info(f"Suggested auxiliary entities: {', '.join(entity_names)}")
        
        return result
        
    except Exception as e:
        logger.error(f"Error in auxiliary entity suggestion: {e}", exc_info=True)
        raise

