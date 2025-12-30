"""Phase 1, Step 1.2: Entity Mention Detection.

Checks if entities are explicitly named in the natural language description.
"""

from typing import Dict, Any, List
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.phases.phase1.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.tools import verify_evidence_substring

logger = get_logger(__name__)


class EntityWithEvidence(BaseModel):
    name: str = Field(description="Canonical form derived only from evidence")
    evidence: str = Field(description="Verbatim substring copied from the input")

    model_config = ConfigDict(extra="forbid")


class EntityMentionOutput(BaseModel):
    """Output structure for entity mention detection."""
    has_explicit_entities: bool = Field(description="Whether entities are explicitly mentioned in the description")
    mentioned_entities: List[EntityWithEvidence] = Field(
        default_factory=list,
        description="List of explicitly mentioned entities with evidence"
    )
    reasoning: str = Field(description="Reasoning (<= 25 words)")

    model_config = ConfigDict(extra="forbid")


@traceable_step("1.2", phase=1, tags=["entity_mention_detection"])
async def step_1_2_entity_mention_detection(nl_description: str) -> Dict[str, Any]:
    """
    Step 1.2: Check if entities are explicitly named in the description.
    
    This step separates explicit mentions from implicit concepts, allowing
    prioritized extraction of explicitly mentioned entities.
    
    Args:
        nl_description: Natural language description of the database requirements
        
    Returns:
        dict: Entity mention detection result with has_explicit_entities and mentioned_entities
        
    Example:
        >>> result = await step_1_2_entity_mention_detection("I need Customer and Order tables")
        >>> result["has_explicit_entities"]
        True
        >>> result["mentioned_entities"]
        ["Customer", "Order"]
    """
    logger.info("Starting Step 1.2: Entity Mention Detection")
    logger.debug(f"Input description length: {len(nl_description)} characters")
    
    system_prompt = """You are a database design assistant.

Task
Extract entity names that are EXPLICITLY mentioned in the input description (table names, entity names, or business concepts that should become tables).

Definition of "explicit"
An entity is explicit only if it appears as a verbatim noun/noun-phrase in the input text (e.g., "sensor_reading", "sensors", "plants", "maintenance events").
Do NOT infer entities that are not explicitly stated.
Do NOT treat attributes/metrics as entities (e.g., temperature, vibration, timestamps, percentages, row counts).

Output format (strict)
Return ONLY a single JSON object with EXACTLY these keys:
{
  "has_explicit_entities": boolean,
  "mentioned_entities": [{"name": string, "evidence": string}],
  "reasoning": string
}

Grounding rule (critical)
- For every item, evidence MUST be copied verbatim from the input (exact substring; preserve casing/spaces).
- name must be a canonical form derived ONLY from evidence (allowed transforms: trim whitespace, remove surrounding quotes, convert spaces to underscores). Do not add words.

Selection rules
- Include each unique entity once (deduplicate by evidence case-insensitively).
- If the input explicitly says "table called X" or "fact/dimension table", include that table name.
- If no explicit entities exist, set has_explicit_entities=false and mentioned_entities=[].

Reasoning constraints
- <= 25 words.
- Must reference the evidence (by quoting at least one evidence string if any exist).

Tool usage (mandatory when has_explicit_entities = true)
You have access to: verify_evidence_substring(evidence: str, nl_description: str) -> {is_substring: bool, error: str|null}
Before finalizing your response:
1) For EACH entity in mentioned_entities, call verify_evidence_substring with:
   {"evidence": "<entity.evidence>", "nl_description": "<full_nl_description>"}
2) If is_substring = false for any entity, correct the evidence to be an exact substring from nl_description, then re-check.

No extra text. No markdown. No code fences."""
    
    # Human prompt template
    human_prompt = "Natural language description:\n{nl_description}"
    
    # Initialize model
    llm = get_model_for_step("1.2")  # Step 1.2 maps to "simple" task type
    
    try:
        logger.debug("Invoking LLM for entity mention detection")
        config = get_trace_config("1.2", phase=1, tags=["entity_mention_detection"])
        result: EntityMentionOutput = await standardized_llm_call(
            llm=llm,
            output_schema=EntityMentionOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={"nl_description": nl_description},
            tools=[verify_evidence_substring],
            use_agent_executor=True,
            config=config,
        )
        
        # Work with Pydantic model directly
        logger.info(f"Entity mention detection completed: has_explicit_entities={result.has_explicit_entities}")
        if result.mentioned_entities:
            logger.info(f"Found {len(result.mentioned_entities)} explicitly mentioned entities: {result.mentioned_entities}")
        
        # Convert to dict only at return boundary
        return result.model_dump()
        
    except Exception as e:
        logger.error(f"Error in entity mention detection: {e}", exc_info=True)
        raise

