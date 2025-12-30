"""Phase 1, Step 1.4: Key Entity Extraction.

Extracts all key entities (business concepts) that need to be modeled.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.phases.phase1.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.ir.models.state import EntityInfo
from NL2DATA.utils.tools import check_entity_name_validity, verify_evidence_substring

logger = get_logger(__name__)


class EntityExtractionOutput(BaseModel):
    """Output structure for entity extraction."""
    entities: List[EntityInfo] = Field(description="List of extracted entities with name, description, and reasoning")
    model_config = ConfigDict(extra="forbid")


@traceable_step("1.4", phase=1, tags=["entity_extraction"])
async def step_1_4_key_entity_extraction(
    nl_description: str,
    domain: Optional[str] = None,
    mentioned_entities: Optional[List[str]] = None,
    domain_detection_result: Optional[Dict[str, Any]] = None,
    entity_mention_result: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Step 1.4: Extract all key entities (business concepts) that need to be modeled.
    
    This is a critical step - missing entities here cascade to all later phases.
    This step forms the foundation for the entire schema.
    
    Args:
        nl_description: Natural language description of the database requirements
        domain: Optional domain from Steps 1.1 or 1.3
        mentioned_entities: Optional list of explicitly mentioned entities from Step 1.2
        domain_detection_result: Optional result from Step 1.1 for context
        entity_mention_result: Optional result from Step 1.2 for context
        
    Returns:
        dict: Entity extraction result with entities list
        
    Example:
        >>> result = await step_1_4_key_entity_extraction(
        ...     "I need a database for an e-commerce store with customers and orders",
        ...     domain="e-commerce"
        ... )
        >>> len(result["entities"])
        2
        >>> result["entities"][0]["name"]
        "Customer"
    """
    logger.info("Starting Step 1.4: Key Entity Extraction")
    logger.debug(f"Input description length: {len(nl_description)} characters")
    
    # Build high-signal prior context (compact "known facts" block)
    explicit_entities_block: List[str] = []
    if entity_mention_result and isinstance(entity_mention_result, dict):
        ents = entity_mention_result.get("mentioned_entities") or []
        for item in ents:
            if isinstance(item, dict):
                raw = (item.get("evidence") or "").strip() or (item.get("name") or "").strip()
                evidence = (item.get("evidence") or "").strip()
                canonical = (item.get("name") or "").strip()
                if raw or canonical or evidence:
                    explicit_entities_block.append(
                        f'{{ raw: "{raw}", canonical: "{canonical}", evidence: "{evidence}" }}'
                    )

    # If caller provided a legacy mentioned_entities list, include it (best-effort).
    if mentioned_entities:
        for me in mentioned_entities:
            if me:
                explicit_entities_block.append(f'{{ raw: "{me}", canonical: "{me}", evidence: "{me}" }}')

    prior_context_lines: List[str] = []
    if domain:
        prior_context_lines.append(f'domain_inferred: "{domain}"')
    if explicit_entities_block:
        prior_context_lines.append("explicit_entities (include canonical + evidence):")
        prior_context_lines.extend(explicit_entities_block)
    prior_context_lines.append('naming_convention: "PascalCase singular"')

    prior_context = "\n".join(prior_context_lines)
    
    system_prompt = """You are an information-extraction assistant.

Task
Extract all entities that should be modeled as database tables from the user's description.

Definitions
An entity is a business concept or object that warrants its own table (e.g., Sensor, Plant, MaintenanceEvent).
Entities may be explicit (named directly) or implied (required by the described behavior or analytics).

What counts as an entity (inclusion rules)
Include an entity if it is:
1) Explicitly named as a table/entity (e.g., "table called X", "dimension table for Y", "fact table Z"), OR
2) Explicitly named as a record/event type that must be stored (e.g., "maintenance events", "incidents"), OR
3) Implied as an independent record type with its own lifecycle and repeated instances over time (i.e., it would naturally be stored as rows), and is necessary to satisfy the described requirements.

What does NOT count (exclusions)
Do NOT include:
- Pure attributes/metrics/fields (temperature, vibration, timestamps, percentages, row counts, bands)
- Pure operations/queries (joins, aggregations, anomaly detection)
- Relationships or junction tables

Grounding requirement (critical)
Every extracted entity MUST include evidence copied verbatim from the input text that supports it.
- evidence must be a short snippet (≤20 words) from the description that justifies the entity.
- evidence must be an exact substring from the input (preserve casing/spaces).
- If you cannot point to verbatim evidence, do not include the entity.

Naming rules
- Use singular, PascalCase SQL-safe identifiers (e.g., SensorReading, SensorType, MaintenanceEvent).
- name must be SQL-safe: starts with a letter, contains only letters/numbers/underscore.
- If the description contains an explicit table/entity name, prefer that concept and keep it specific.
- If evidence is plural ("sensors"), name should be singular ("Sensor").
- Do NOT invent words not supported by evidence; you may only normalize by:
  (a) singularizing, (b) removing surrounding quotes, (c) converting to PascalCase, (d) ensuring SQL-safe characters.

De-duplication
- De-duplicate semantically identical entities; pick the most canonical name.

Tool usage (mandatory)
You have access to two tools:
1) check_entity_name_validity(name: str) -> {valid: bool, error: str|null, suggestion: str|null}
2) verify_evidence_substring(evidence: str, nl_description: str) -> {is_substring: bool, error: str|null}

Before finalizing your response:
1) Call check_entity_name_validity for EACH entity.name:
   {"name": "<candidate_name>"}
2) If valid=false, fix the name (use suggestion if present) and re-check.
3) Call verify_evidence_substring for EACH entity.evidence:
   {"evidence": "<entity.evidence>", "nl_description": "<full_nl_description>"}
4) If is_substring=false, correct the evidence to be an exact substring, then re-check.

Output requirements (MUST follow)
Return ONLY a JSON object that matches this schema exactly:
{
  "entities": [
    {
      "name": "string",
      "mention_type": "explicit"|"implied",
      "evidence": "string",
      "description": "string",
      "confidence": number
    }
  ]
}

No extra text. No markdown. No code fences."""
    
    # Human prompt template
    human_prompt = """description: {nl_description}

prior_context:
{prior_context}
"""
    
    # Initialize model
    llm = get_model_for_step("1.4")  # Step 1.4 maps to "important" task type
    
    try:
        logger.debug("Invoking LLM for key entity extraction")
        # Get trace config with metadata (LangChain best practice)
        config = get_trace_config("1.4", phase=1, tags=["entity_extraction"])
        
        # Use standardized LLM call - returns Pydantic model directly
        result: EntityExtractionOutput = await standardized_llm_call(
            llm=llm,
            output_schema=EntityExtractionOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={"nl_description": nl_description, "prior_context": prior_context},
            tools=[check_entity_name_validity, verify_evidence_substring],
            use_agent_executor=True,  # Use agent executor for tool calls
            config=config,
        )
        
        # Work with Pydantic model directly (not dict)
        entity_count = len(result.entities)
        logger.info(f"Key entity extraction completed: found {entity_count} entities")
        
        if entity_count > 0:
            entity_names = [e.name for e in result.entities]
            logger.info(f"Extracted entities: {', '.join(entity_names)}")
        
        # Convert to dict only at the very end for return compatibility
        # Note: Return type is Dict[str, Any] for compatibility with pipeline.
        # Future enhancement: Consider changing return type to EntityExtractionOutput for type safety.
        return result.model_dump()
        
    except Exception as e:
        logger.error(f"Error in key entity extraction: {e}", exc_info=True)
        raise

