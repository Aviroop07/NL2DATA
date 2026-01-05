"""Phase 1, Step 1.4: Key Entity Extraction.

Extracts all key entities (business concepts) that need to be modeled.
"""

from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.phases.phase1.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.ir.models.state import EntityInfo
from NL2DATA.utils.prompt_helpers import generate_output_structure_section_with_custom_requirements

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
    domain_detection_result: Optional = None,
    entity_mention_result: Optional = None,
) -> EntityExtractionOutput:
    """
    Step 1.4: Extract all key entities (business concepts) that need to be modeled.
    
    This is a critical step - missing entities here cascade to all later phases.
    This step forms the foundation for the entire schema.
    
    Args:
        nl_description: Natural language description of the database requirements
        domain: Optional domain from Step 1.1 (Domain Detection & Inference)
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

    def _is_sql_safe_identifier(name: str) -> bool:
        import re
        n = (name or "").strip()
        if not n:
            return False
        if not re.match(r"^[a-zA-Z][a-zA-Z0-9_]*$", n):
            return False
        # Conservative reserved keyword check (subset)
        sql_keywords = {
            "SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP",
            "ALTER", "TABLE", "INDEX", "PRIMARY", "KEY", "FOREIGN", "REFERENCES",
            "CONSTRAINT", "UNIQUE", "NOT", "NULL", "DEFAULT", "CHECK", "AND", "OR",
        }
        return n.upper() not in sql_keywords

    def _sanitize_identifier(name: str) -> str:
        """
        Best-effort sanitizer that preserves readability.
        """
        import re
        raw = (name or "").strip()
        if not raw:
            return "Entity"
        cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", raw)
        if not cleaned:
            cleaned = "Entity"
        if not cleaned[0].isalpha():
            cleaned = f"Entity_{cleaned}"
        # Avoid reserved keywords (subset)
        if not _is_sql_safe_identifier(cleaned):
            cleaned = f"{cleaned}_Entity"
        # Collapse multiple underscores
        cleaned = re.sub(r"_+", "_", cleaned)
        return cleaned
    
    # Generate output structure section from Pydantic model
    output_structure_section = generate_output_structure_section_with_custom_requirements(
        output_schema=EntityExtractionOutput,
        additional_requirements=[
            "The \"evidence\" field MUST be a verbatim substring from the input description"
        ]
    )
    
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

CRITICAL: SCHEMA ANCHORED VALIDATION
Before outputting any entity name:
1. Check if it already exists in the provided schema context (if prior_context is provided)
2. Use EXACT names from the schema (case-sensitive) if the entity already exists
3. Do NOT invent new names unless explicitly creating new components
4. If prior_context mentions entities, prefer those exact names

EXAMPLES:
❌ BAD: Outputting "CustomerOrder" when schema has "Order"
❌ BAD: Outputting "ProductCategory" when schema has "Category"
✅ GOOD: Using exact names from schema: "Order", "Category"
✅ GOOD: Creating new entity "MaintenanceEvent" if not in prior context

COMMON MISTAKES TO AVOID:
1. ❌ Mixing entity names (e.g., "CustomerOrder" vs "Order")
2. ❌ Inventing compound names when simpler names exist
3. ❌ Using plural forms when singular is canonical
4. ❌ Ignoring prior context when it provides entity names

If unsure, err on the side of conservatism (use existing names from context).

""" + output_structure_section
    
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
            tools=None,
            use_agent_executor=False,
            config=config,
        )

        # Post-validate: drop entities whose evidence is not a verbatim substring (strict grounding)
        filtered_entities: List[EntityInfo] = []
        seen_names: set[str] = set()
        for ent in result.entities:
            name = (ent.name or "").strip()
            evidence = (ent.evidence or "").strip()
            if not evidence or evidence not in nl_description:
                continue

            if not _is_sql_safe_identifier(name):
                name = _sanitize_identifier(name)

            if name in seen_names:
                continue
            seen_names.add(name)

            filtered_entities.append(ent.model_copy(update={"name": name}))

        result = EntityExtractionOutput(entities=filtered_entities)
        
        # Work with Pydantic model directly (not dict)
        entity_count = len(result.entities)
        logger.info(f"Key entity extraction completed: found {entity_count} entities")
        
        if entity_count > 0:
            entity_names = [e.name for e in result.entities]
            logger.info(f"Extracted entities: {', '.join(entity_names)}")
        
        return result
        
    except Exception as e:
        logger.error(f"Error in key entity extraction: {e}", exc_info=True)
        raise

