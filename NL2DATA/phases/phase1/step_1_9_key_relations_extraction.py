"""Phase 1, Step 1.9: Key Relations Extraction.

Identifies all relationships among entities (can involve 2+ entities).
Critical for foreign key design and understanding data flow.
"""

import json
from typing import List, Optional, Dict, Any
import re
from pydantic import BaseModel, Field, ConfigDict

from NL2DATA.phases.phase1.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.utils.prompt_helpers import generate_output_structure_section_with_custom_requirements
from NL2DATA.ir.models.state import RelationInfo
from NL2DATA.phases.phase1.utils import (
    extract_entity_name,
    build_entity_list_string,
)
from NL2DATA.utils.tools.validation_tools import _verify_entities_exist_impl
from NL2DATA.utils.tools.validation_tools import _verify_evidence_substring_impl
from NL2DATA.ir.models.relation_type import RelationType, normalize_relation_type

logger = get_logger(__name__)

def _entity_name_variants_for_text(entity_name: str) -> List[str]:
    """
    Lightweight variants so evidence checks can match entity mentions in prose.
    Example: "CustomerOrder" -> ["customerorder", "customer order", "customer_order", "customer orders", ...]
    """
    name = (entity_name or "").strip()
    if not name:
        return []
    spaced = re.sub(r"(?<!^)([A-Z])", r" \1", name).strip().lower()
    snake = re.sub(r"(?<!^)([A-Z])", r"_\1", name).strip().lower()
    base = {name.lower(), spaced, snake}
    out: set[str] = set()
    for v in base:
        v = v.strip()
        if not v:
            continue
        out.add(v)
        if not v.endswith("s"):
            out.add(v + "s")
    return sorted(out)


def _evidence_mentions_all_entities(evidence: str, entities_in_rel: List[str]) -> bool:
    """
    Return True iff the evidence snippet likely mentions all participating entities,
    using simple variant matching (case-insensitive).
    """
    ev = (evidence or "").strip().lower()
    if not ev:
        return False
    for ent in (entities_in_rel or []):
        variants = _entity_name_variants_for_text(str(ent))
        if not variants:
            return False
        if not any(v in ev for v in variants):
            return False
    return True


class RelationExtractionOutput(BaseModel):
    """Output structure for relation extraction."""
    relations: List[RelationInfo] = Field(
        description="List of extracted relations with entities, type, description, arity, and reasoning"
    )
    model_config = ConfigDict(extra="forbid")


@traceable_step("1.9", phase=1, tags=["relation_extraction"])
async def step_1_9_key_relations_extraction(
    entities: List,
    nl_description: str,
    domain: Optional[str] = None,
    mentioned_relations: Optional[List[Any]] = None,
    focus_entities: Optional[List[str]] = None,
) -> RelationExtractionOutput:
    """
    Step 1.9: Identify all relationships among entities.
    
    This step extracts all relationships (can involve 2+ entities) using the complete
    entity set (key + auxiliary, after consolidation). Missing relations lead to
    incomplete schemas.
    
    Args:
        entities: List of all entities (key + auxiliary, after consolidation)
        nl_description: Natural language description of the database requirements
        domain: Optional domain context
        mentioned_relations: Optional list of explicitly mentioned relations from Step 1.5
        focus_entities: Optional list of entity names that MUST be connected by at least one relation.
            This is typically produced by Step 1.10 (schema connectivity) as orphan entities.
        
    Returns:
        dict: Relation extraction result with relations list
        
    Example:
        >>> result = await step_1_9_key_relations_extraction(
        ...     entities=[{"name": "Customer"}, {"name": "Order"}],
        ...     nl_description="Customers place orders"
        ... )
        >>> len(result["relations"])
        1
        >>> result["relations"][0]["entities"]
        ["Customer", "Order"]
    """
    logger.info("Starting Step 1.9: Key Relations Extraction")
    logger.debug(f"Extracting relations for {len(entities)} entities")
    
    # Build entity list for context using utilities
    entity_list_str = build_entity_list_string(entities, include_descriptions=True, prefix="- ")
    
    # Compact explicit relations context (from Step 1.5), if provided
    explicit_relations_json = ""
    if mentioned_relations:
        try:
            explicit_relations_json = json.dumps(mentioned_relations, indent=2, ensure_ascii=False)
        except Exception:
            explicit_relations_json = str(mentioned_relations)
    
    # Generate output structure section from Pydantic model
    output_structure_section = generate_output_structure_section_with_custom_requirements(
        output_schema=RelationExtractionOutput,
        additional_requirements=[
            "Top-level key MUST be \"relations\" (NOT \"relationships\", NOT any other key)",
            "Relation type MUST be one of the exact enum strings: 'one-to-one', 'one-to-many', 'many-to-one', 'many-to-many', 'ternary'",
            "entities list MUST contain at least 2 different entity names (NO self-relations, NO duplicate entity names)",
            "All entity names MUST exist in the provided entity list (verify with tool)",
            "If source='explicit_in_text', evidence MUST be verbatim substring from input and confidence MUST be null",
            "If source='schema_inferred', confidence MUST be 0.0-1.0 and evidence MUST be null"
        ]
    )
    
    # System prompt
    system_prompt = """You are a database design assistant.

Task
Identify relationships among entities in a database schema.

CRITICAL OUTPUT CONTRACT (read carefully)
- You MUST output a single JSON object with a SINGLE top-level key: "relations".
- Do NOT output top-level keys like "relationship", "relationships", "explicit_in_text", "schema_inferred", etc.
- Do NOT include any extra keys at the top level.
- Do NOT include markdown or code fences.

A relationship connects two or more entities and describes how they relate to each other. Relationships can be:
- **Binary**: Between two entities (e.g., Customer places Order, Order contains Product)
- **Ternary or N-ary**: Between three or more entities (e.g., Student enrolls in Course taught by Instructor)

Relationship types include:
- **One-to-One (1:1)**: One entity instance relates to exactly one instance of another (e.g., User has one Profile)
- **One-to-Many (1:N)**: One instance of one entity relates to many instances of another (e.g., Customer has many Orders)
- **Many-to-One (N:1)**: Many instances of one entity relate to a single shared instance of another (e.g., many Customers can share one Address)
- **Many-to-Many (N:M)**: Many instances of each relate to many instances of the other
- **Ternary (3-ary / n-ary)**: A relationship involving 3 (or more) entities

CRITICAL: Relation `type` MUST be one of these exact enum strings (no other values allowed):
- "one-to-one"
- "one-to-many"
- "many-to-one"
- "many-to-many"
- "ternary"

For each relation item:
- entities: List of canonical entity names involved (MUST have at least 2 unique entities; NO self-relations)
- type: Relationship type (e.g., "one-to-many", "many-to-many", "one-to-one", "belongs-to", "has", "contains")
- description: Clear description of the relationship
- arity: Number of entities in the relationship (2 for binary, 3 for ternary, etc.)
- source: "explicit_in_text" or "schema_inferred" (required)
- evidence: REQUIRED when source="explicit_in_text" (verbatim substring from description); MUST be null when source="schema_inferred"
- confidence: REQUIRED when source="schema_inferred" (0.0–1.0); MUST be null when source="explicit_in_text"
- reasoning: Brief explanation (required)

Important constraints:
- Be conservative with schema_inferred relationships; do NOT add star-schema joins unless truly supported.
- Do not include attributes or properties - only relationships between entities
- Ensure all entities in relationships exist in the provided entity list
- NO self-relations: entities list must contain at least 2 different entity names
- Each relationship must involve distinct entities (no duplicate entity names in the entities list)

CRITICAL: SCHEMA ANCHORED VALIDATION
Before outputting any entity name in a relation:
1. Check if it exists in the provided entity list (use verify_entities_exist_bound tool)
2. Use EXACT names from the entity list (case-sensitive)
3. Do NOT invent new entity names - only use entities from the provided list

EXAMPLES:
❌ BAD: Relation with entities ["CustomerOrder", "Product"] when entity list has ["Order", "Product"]
❌ BAD: Relation with entities ["Customer", "OrderItem"] when entity list has ["Customer", "Order"]
✅ GOOD: Using exact names from entity list: ["Customer", "Order"], ["Product", "Category"]

COMMON MISTAKES TO AVOID:
1. ❌ Listing relations with non-existent entities (always verify with tool first)
2. ❌ Mixing entity names (e.g., "CustomerOrder" vs "Order")
3. ❌ Creating self-relations (entity to itself)
4. ❌ Including attributes/properties instead of relationships
5. ❌ Adding unnecessary star-schema joins

If unsure, err on the side of conservatism (verify entities exist, use exact names).

Tool usage (mandatory)
You have access to two tools:
1) verify_entities_exist_bound(entities: List[str]) -> Dict[str, bool]
   - Use this to verify that all entities mentioned in relationships exist in the schema
   - CRITICAL: Provide arguments as a JSON object: {{"entities": ["Customer", "Order"]}}
   - NOT as a list: ["entities"] (WRONG)
   - NOT as a string: "entities" (WRONG)
2) verify_evidence_substring(evidence: str, nl_description: str) -> {{is_substring: bool, error: str|null}}
   - Use this to validate evidence when source="explicit_in_text"

Before finalizing your response:
1) For EACH relation in relations:
   a) Call verify_entities_exist_bound with: {{"entities": <relation.entities>}}
   b) If any entity doesn't exist, correct the entity name and re-check
   c) If source="explicit_in_text", call verify_evidence_substring with: {{"evidence": <relation.evidence>, "nl_description": <full_nl_description>}}
   d) If is_substring=false, correct the evidence to be an exact substring and re-check

""" + output_structure_section
    
    # Human prompt template
    focus_block = ""
    if focus_entities:
        # Keep it compact and stable for the LLM.
        focus_list = [e for e in focus_entities if isinstance(e, str) and e.strip()]
        if focus_list:
            focus_block = (
                "\nOrphan / focus entities that must be connected by relations (from connectivity check):\n"
                + "\n".join([f"- {e}" for e in focus_list])
                + "\n"
            )

    human_prompt = """Entities in the schema:
{entity_list}

Explicit relations from Step 1.5 (JSON, may be empty):
{explicit_relations}
{focus_block}

Natural language description:
{nl_description}
"""
    
    # Initialize model and create chain
    llm = get_model_for_step("1.9")  # Step 1.9 maps to "important" task type
    
    # Create bound version of verify_entities_exist with schema_state
    schema_state = {"entities": entities}
    def verify_entities_exist_bound(entities: List[str]) -> Dict[str, Any]:
        """Bound version of verify_entities_exist with schema_state.
        
        Args:
            entities: List of entity names to verify. Must be a list of strings.
                     Example: ["Customer", "Order", "Book"]
        
        Returns:
            Dictionary mapping entity name to existence status (True/False)
        
        IMPORTANT: When calling this tool, provide arguments as a JSON object:
        {"entities": ["Customer", "Order"]}
        NOT as a list: ["entities"] (WRONG)
        NOT as a string: "entities" (WRONG)
        But as a dict: {"entities": ["Customer", "Order"]} (CORRECT)
        """
        # NOTE: verify_entities_exist is a LangChain @tool (StructuredTool) and is not callable like a function.
        # Use the pure implementation to avoid "'StructuredTool' object is not callable".
        return _verify_entities_exist_impl(entities, schema_state)
    
    try:
        logger.debug("Invoking LLM for key relations extraction")
        config = get_trace_config("1.9", phase=1, tags=["relation_extraction"])
        result: RelationExtractionOutput = await standardized_llm_call(
            llm=llm,
            output_schema=RelationExtractionOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt,
            input_data={
                "entity_list": entity_list_str,
                "nl_description": nl_description,
                "explicit_relations": explicit_relations_json,
                "focus_block": focus_block,
            },
            tools=None,
            use_agent_executor=False,
            config=config,
        )
        
        # Work with Pydantic model directly
        relations = result.relations
        
        # Post-processing validation: filter out invalid relations
        valid_relations = []
        entity_names = {e.get("name") or extract_entity_name(e) for e in entities}
        # Create case-insensitive lookup for entity name matching
        entity_names_lower = {name.lower(): name for name in entity_names if name}
        
        for relation in relations:
            entities_in_rel = relation.entities
            
            # Validation: must have at least 2 entities
            if len(entities_in_rel) < 2:
                logger.warning(f"Skipping relation with <2 entities: {entities_in_rel}")
                continue
            
            # Validation: no self-relations (all entities must be unique)
            if len(set(entities_in_rel)) < len(entities_in_rel):
                logger.warning(f"Skipping self-relation: {entities_in_rel}")
                continue
            
            # Validation: all entities must exist in the schema (case-insensitive matching)
            # Normalize relation entity names to match schema entities
            normalized_entities_in_rel = []
            for rel_entity in entities_in_rel:
                rel_entity_lower = (rel_entity or "").strip().lower()
                if rel_entity_lower in entity_names_lower:
                    # Use the canonical entity name from schema
                    normalized_entities_in_rel.append(entity_names_lower[rel_entity_lower])
                elif rel_entity in entity_names:
                    # Already matches exactly
                    normalized_entities_in_rel.append(rel_entity)
                else:
                    # Entity doesn't exist - will be caught below
                    normalized_entities_in_rel.append(rel_entity)
            
            # Check if all entities exist (after normalization)
            if not all(e in entity_names for e in normalized_entities_in_rel):
                missing = [e for e in normalized_entities_in_rel if e not in entity_names]
                logger.warning(f"Skipping relation with unknown entities {missing}: {entities_in_rel}")
                continue
            
            # Update relation with normalized entity names
            if normalized_entities_in_rel != entities_in_rel:
                relation = relation.model_copy(update={"entities": normalized_entities_in_rel})

            # Validation: evidence must be grounded when explicit_in_text
            if relation.source == "explicit_in_text":
                ev = (relation.evidence or "").strip()
                if not ev:
                    logger.warning(f"Skipping explicit relation with missing evidence: {entities_in_rel}")
                    continue
                if not _verify_evidence_substring_impl(ev, nl_description).get("is_substring", False):
                    # Connectivity-focused extraction often proposes correct relations but fails to provide a
                    # verbatim evidence substring. Instead of dropping the relation (which breaks graph
                    # connectivity), conservatively downgrade it to schema_inferred.
                    logger.warning(
                        f"Explicit relation evidence not grounded; downgrading to schema_inferred: {entities_in_rel}"
                    )
                    relation = relation.model_copy(
                        update={
                            "source": "schema_inferred",
                            "evidence": None,
                            "confidence": relation.confidence if relation.confidence is not None else 0.5,
                        }
                    )
                else:
                    # Additional grounding: evidence must mention ALL participating entities (by name variant).
                    # If not, downgrade to schema_inferred rather than keeping a misleading explicit evidence.
                    if not _evidence_mentions_all_entities(ev, entities_in_rel):
                        logger.warning(
                            "Explicit relation evidence does not mention all entities; "
                            f"downgrading to schema_inferred: {entities_in_rel}"
                        )
                        relation = relation.model_copy(
                            update={
                                "source": "schema_inferred",
                                "evidence": None,
                                "confidence": relation.confidence if relation.confidence is not None else 0.5,
                            }
                        )
            
            # Fix arity mismatch if needed
            fixed_relation = relation
            if relation.arity != len(entities_in_rel):
                logger.warning(f"Fixing arity mismatch: {relation.arity} -> {len(entities_in_rel)} for {entities_in_rel}")
                fixed_relation = relation.model_copy(update={"arity": len(entities_in_rel)})
            
            # Fix source/evidence/confidence consistency
            updates = {}
            if fixed_relation.source == "explicit_in_text" and not fixed_relation.evidence:
                logger.warning(f"Missing evidence for explicit relation: {entities_in_rel}, setting evidence to description")
                updates["evidence"] = fixed_relation.description[:100]  # Fallback to description snippet
            elif fixed_relation.source == "schema_inferred" and fixed_relation.evidence:
                logger.warning(f"Removing evidence from schema_inferred relation: {entities_in_rel}")
                updates["evidence"] = None
            
            if fixed_relation.source == "schema_inferred" and fixed_relation.confidence is None:
                logger.warning(f"Missing confidence for schema_inferred relation: {entities_in_rel}, setting to 0.7")
                updates["confidence"] = 0.7  # Default confidence
            elif fixed_relation.source == "explicit_in_text" and fixed_relation.confidence is not None:
                logger.warning(f"Removing confidence from explicit_in_text relation: {entities_in_rel}")
                updates["confidence"] = None
            
            if updates:
                fixed_relation = fixed_relation.model_copy(update=updates)

            # Normalize relation type to Phase-1 enum (deterministic).
            # For n-ary relations, use "ternary" (represents 3-ary / n-ary).
            try:
                if (fixed_relation.arity or 0) > 2:
                    fixed_relation = fixed_relation.model_copy(update={"type": RelationType.TERNARY})
                else:
                    fixed_relation = fixed_relation.model_copy(update={"type": normalize_relation_type(str(fixed_relation.type))})
            except Exception:
                fixed_relation = fixed_relation.model_copy(update={"type": RelationType.ONE_TO_MANY})
            
            valid_relations.append(fixed_relation)
        
        relation_count = len(valid_relations)
        filtered_count = len(relations) - relation_count
        if filtered_count > 0:
            logger.warning(f"Filtered out {filtered_count} invalid relations")
        
        logger.info(f"Key relations extraction completed: found {relation_count} valid relations")
        
        if relation_count > 0:
            # Log relation summary
            binary_count = sum(1 for r in valid_relations if r.arity == 2)
            nary_count = relation_count - binary_count
            logger.info(f"Relation summary: {binary_count} binary, {nary_count} n-ary")
            
            # Log each relation
            for relation in valid_relations:
                entities_in_rel = relation.entities
                rel_type = relation.type
                logger.debug(f"Relation: {', '.join(entities_in_rel)} ({rel_type})")
        
        # Create updated result with validated relations
        validated_result = RelationExtractionOutput(relations=valid_relations)
        
        return validated_result
        
    except Exception as e:
        logger.error(f"Error in key relations extraction: {e}", exc_info=True)
        raise

