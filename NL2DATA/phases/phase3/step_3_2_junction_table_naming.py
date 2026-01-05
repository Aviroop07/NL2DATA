"""Phase 3, Step 3.2: Junction Table Naming.

Before converting ER to relational schema, ask LLM to suggest better names
for junction tables (for many-to-many and ternary relations).

This step runs after ER design compilation (3.1) and before relational
schema compilation (4.1).
"""

from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.phases.phase3.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.ir.models.relation_type import RelationType
from NL2DATA.utils.prompt_helpers import generate_output_structure_section

logger = get_logger(__name__)


class JunctionTableNameOutput(BaseModel):
    """Output structure for junction table naming."""
    table_name: str = Field(
        description="Suggested name for the junction table (single word or snake_case, no reasoning)"
    )

    model_config = ConfigDict(extra="forbid")


class JunctionTableNameEntry(BaseModel):
    """Entry mapping a relation key to a suggested table name."""
    relation_key: str = Field(description="Relation key (sorted entity names joined by '_')")
    table_name: str = Field(description="Suggested name for the junction table")

    model_config = ConfigDict(extra="forbid")


class JunctionTableNamingOutput(BaseModel):
    """Output structure for junction table naming (batch)."""
    junction_table_names: List[JunctionTableNameEntry] = Field(
        default_factory=list,
        description="List of junction table name entries, one per junction relation"
    )

    model_config = ConfigDict(extra="forbid")


def _get_relation_key(entities: List[str]) -> str:
    """Generate a deterministic key for a relation based on entity names."""
    return "_".join(sorted(entities))


@traceable_step("3.2", phase=3, tags=['phase_3_step_2'])
async def step_3_2_junction_table_naming(
    relations: List,
    entities: List,
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
) -> JunctionTableNamingOutput:
    """
    Step 3.2: Suggest better names for junction tables (M:N and ternary relations).
    
    Args:
        relations: List of relations from ER design
        entities: List of entities with descriptions
        nl_description: Optional original NL description
        domain: Optional domain context
        
    Returns:
        dict: Mapping from relation_key (sorted entity names joined by "_") to suggested table name
        
    Example:
        >>> result = await step_3_2_junction_table_naming(
        ...     relations=[{"entities": ["Student", "Course"], "type": "many-to-many"}],
        ...     entities=[{"name": "Student"}, {"name": "Course"}]
        ... )
        >>> "Course_Student" in result or "Student_Course" in result
        True
    """
    logger.info("Starting Step 3.2: Junction Table Naming")
    
    # Filter to only M:N and ternary relations
    junction_relations = []
    for rel in relations:
        rel_type = rel.get("type", "").strip().lower()
        rel_arity = rel.get("arity", len(rel.get("entities", [])))
        if rel_type in ("many-to-many", "ternary") or rel_arity >= 3:
            junction_relations.append(rel)
    
    if not junction_relations:
        logger.info("No junction tables needed (no M:N or ternary relations)")
        return JunctionTableNamingOutput(junction_table_names=[])
    
    logger.info(f"Found {len(junction_relations)} junction relations requiring table names")
    
    # Build entity descriptions map for context
    entity_descriptions = {}
    for entity in entities:
        entity_name = entity.get("name", "")
        entity_desc = entity.get("description", "")
        if entity_name:
            entity_descriptions[entity_name] = entity_desc
    
    # Generate output structure section from Pydantic model
    output_structure_section = generate_output_structure_section(output_schema=JunctionTableNameOutput)
    
    # System prompt
    system_prompt = """You are a database schema design expert. Your task is to suggest a good name for a junction table (join table) that connects multiple entities.

A junction table is created for:
- Many-to-many (M:N) relationships between two entities
- Ternary (3-ary) or n-ary relationships involving three or more entities

IMPORTANT NAMING GUIDELINES:
- Use a single, descriptive name (not just entity1_entity2)
- Prefer business-domain terms that capture the relationship meaning
- Use snake_case (lowercase with underscores)
- Keep it concise (typically 1-3 words)
- Avoid redundant words like "link", "junction", "association" unless they add clarity

EXAMPLES:
- Student + Course → "enrollments" (not "Student_Course")
- Order + Product → "order_items" (not "Order_Product")
- Employee + Project + Role → "assignments" (not "Employee_Project_Role")
- Author + Book → "publications" (not "Author_Book")
- User + Group → "memberships" (not "User_Group")
- Doctor + Patient + Appointment → "appointments" (not "Doctor_Patient_Appointment")

""" + output_structure_section

    # Process each junction relation
    junction_table_entries = []
    
    for rel in junction_relations:
        rel_entities = rel.get("entities", [])
        rel_type = rel.get("type", "").strip().lower()
        rel_description = rel.get("description", "")
        rel_arity = rel.get("arity", len(rel_entities))
        
        if len(rel_entities) < 2:
            logger.warning(f"Skipping relation with < 2 entities: {rel_entities}")
            continue
        
        # Build context about entities
        entity_context_parts = []
        for entity_name in rel_entities:
            entity_desc = entity_descriptions.get(entity_name, "")
            if entity_desc:
                entity_context_parts.append(f"- {entity_name}: {entity_desc}")
            else:
                entity_context_parts.append(f"- {entity_name}")
        
        entity_context = "\n".join(entity_context_parts)
        
        # Determine relation type label
        if rel_arity >= 3:
            relation_type_label = f"{rel_arity}-ary (ternary or higher)"
        elif rel_type == "many-to-many":
            relation_type_label = "many-to-many"
        else:
            relation_type_label = "many-to-many"  # fallback
        
        # Human prompt
        human_prompt = f"""Suggest a name for the junction table connecting these entities:

Entities involved:
{entity_context}

Relation type: {relation_type_label}
Relation description: {rel_description}

Context:
- Domain: {domain or "general"}
- Original description: {nl_description or "N/A"}

Return ONLY a JSON object with "table_name" (no reasoning)."""
        
        try:
            llm = get_model_for_step("3.2")
            config = get_trace_config("3.2", phase=3, tags=["phase_3_step_2"])
            
            result: JunctionTableNameOutput = await standardized_llm_call(
                llm=llm,
                output_schema=JunctionTableNameOutput,
                system_prompt=system_prompt,
                human_prompt_template=human_prompt,
                input_data={},
                config=config,
            )
            
            suggested_name = (result.table_name or "").strip()
            if not suggested_name:
                # Fallback to deterministic name
                suggested_name = "_".join(sorted(rel_entities))
                logger.warning(f"LLM returned empty name for {rel_entities}, using fallback: {suggested_name}")
            else:
                # Normalize to snake_case
                suggested_name = suggested_name.lower().replace(" ", "_").replace("-", "_")
                # Remove any invalid characters
                suggested_name = "".join(c for c in suggested_name if c.isalnum() or c == "_")
                # Remove leading/trailing underscores
                suggested_name = suggested_name.strip("_")
                if not suggested_name:
                    suggested_name = "_".join(sorted(rel_entities))
                    logger.warning(f"Normalized name became empty for {rel_entities}, using fallback: {suggested_name}")
            
            relation_key = _get_relation_key(rel_entities)
            junction_table_entries.append(
                JunctionTableNameEntry(
                    relation_key=relation_key,
                    table_name=suggested_name
                )
            )
            logger.debug(f"Suggested junction table name for {rel_entities}: {suggested_name}")
            
        except Exception as e:
            logger.error(f"Error naming junction table for {rel_entities}: {e}", exc_info=True)
            # Fallback to deterministic name
            relation_key = _get_relation_key(rel_entities)
            junction_table_entries.append(
                JunctionTableNameEntry(
                    relation_key=relation_key,
                    table_name="_".join(sorted(rel_entities))
                )
            )
    
    logger.info(f"Junction table naming completed: {len(junction_table_entries)} names suggested")
    return JunctionTableNamingOutput(junction_table_names=junction_table_entries)
