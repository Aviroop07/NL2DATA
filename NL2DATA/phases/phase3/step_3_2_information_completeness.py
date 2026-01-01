"""Phase 3, Step 3.2: Information Completeness Check.

For each information need, verify if all necessary relations, entities, and attributes are present.
Iterative loop per information need until LLM is satisfied.
"""

from typing import Dict, Any, List, Optional, Set, Tuple
from pydantic import BaseModel, Field

from NL2DATA.phases.phase3.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.phases.phase1.utils.data_extraction import (
    extract_attribute_name,
    extract_attribute_description,
)
from NL2DATA.utils.loops import SafeLoopExecutor, LoopConfig
from NL2DATA.utils.pipeline_config import get_phase3_config

logger = get_logger(__name__)


class MissingAttribute(BaseModel):
    """Missing attribute specification."""
    entity: str = Field(description="Entity name that needs this attribute")
    attribute: str = Field(description="Attribute name that is missing")


class MissingIntrinsicAttribute(BaseModel):
    """Missing intrinsic attribute specification."""
    entity: str = Field(description="Entity name that needs this intrinsic attribute")
    attribute: str = Field(description="Intrinsic attribute name that is missing")
    reasoning: str = Field(description="Why this intrinsic attribute is needed for the information need")


class MissingDerivedAttribute(BaseModel):
    """Missing derived attribute specification."""
    entity: str = Field(description="Entity name that needs this derived attribute")
    attribute: str = Field(description="Derived attribute name that is missing")
    derivation_hint: str = Field(description="Hint for how to derive this attribute (e.g., 'is_recent = (order_date >= CURRENT_DATE - INTERVAL \"1 month\")')")
    reasoning: str = Field(description="Why this derived attribute would make querying easier")


class MissingRelation(BaseModel):
    """Missing relation specification (structured)."""

    entities: List[str] = Field(
        description="Canonical entity names that participate in this relation (2 or more). Use ONLY entity names present in the provided context."
    )
    description: str = Field(
        description="Short description of the relation (what it means / why needed for this information need)."
    )


class InformationCompletenessOutput(BaseModel):
    """Output structure for information completeness check."""
    information_need: str = Field(description="The information need being checked")
    all_present: bool = Field(description="Whether all necessary components are present")
    missing_relations: List[MissingRelation] = Field(
        default_factory=list,
        description="List of missing relations as structured entries {entities, description}."
    )
    missing_entities: List[str] = Field(
        default_factory=list,
        description="List of missing entity names"
    )
    missing_intrinsic_attributes: List[MissingIntrinsicAttribute] = Field(
        default_factory=list,
        description="List of missing intrinsic attributes with their entity names and reasoning. Note: Foreign keys and relation-connecting attributes are NOT considered intrinsic attributes - they will be handled automatically."
    )
    missing_derived_attributes: List[MissingDerivedAttribute] = Field(
        default_factory=list,
        description="List of missing derived attributes that would make querying easier, with derivation hints"
    )
    satisfied: bool = Field(
        description="Whether the LLM is satisfied that all components are present (termination condition for loop)"
    )
    reasoning: str = Field(description="Reasoning for the completeness assessment and satisfaction decision")


def _canonicalize_entity_name(name: str, allowed_entities: Set[str]) -> Optional[str]:
    """Best-effort map of a model-produced entity name to canonical entity names."""
    if not name:
        return None
    n = name.strip()
    if n in allowed_entities:
        return n
    # case-insensitive exact
    for e in allowed_entities:
        if e.lower() == n.lower():
            return e
    # strip common plurals
    n2 = n[:-1] if n.lower().endswith("s") and len(n) > 3 else n
    for e in allowed_entities:
        if e.lower() == n2.lower():
            return e
    # substring containment (very conservative)
    n_l = n.lower()
    candidates = [e for e in allowed_entities if e.lower() in n_l or n_l in e.lower()]
    if len(candidates) == 1:
        return candidates[0]
    return None


def _relation_has_pair(rel: Dict[str, Any], a: str, b: str) -> bool:
    ents = rel.get("entities") or []
    if not isinstance(ents, list):
        return False
    s = {x for x in ents if isinstance(x, str)}
    return a in s and b in s


def _build_entity_graph(relations: List[Dict[str, Any]]) -> Dict[str, Set[str]]:
    graph: Dict[str, Set[str]] = {}
    for rel in relations or []:
        ents = rel.get("entities") or []
        if not isinstance(ents, list):
            continue
        ents = [e for e in ents if isinstance(e, str) and e]
        for i in range(len(ents)):
            for j in range(i + 1, len(ents)):
                a, b = ents[i], ents[j]
                graph.setdefault(a, set()).add(b)
                graph.setdefault(b, set()).add(a)
    return graph


def _has_join_path(graph: Dict[str, Set[str]], src: str, dst: str) -> bool:
    if not src or not dst:
        return False
    if src == dst:
        return True
    if src not in graph or dst not in graph:
        return False
    seen: Set[str] = {src}
    q: List[str] = [src]
    while q:
        cur = q.pop(0)
        for nxt in graph.get(cur, set()):
            if nxt == dst:
                return True
            if nxt in seen:
                continue
            seen.add(nxt)
            q.append(nxt)
    return False


def _looks_like_aggregate_metric(derivation_hint: str) -> bool:
    txt = (derivation_hint or "").lower()
    if not txt:
        return False
    needles = [
        "count(",
        "sum(",
        "avg(",
        "min(",
        "max(",
        "group by",
        " over(",
        " window ",
    ]
    return any(n in txt for n in needles)


def _extract_entities_from_relation_text(rel_text: str, allowed_entities: Set[str]) -> List[str]:
    """Extract canonical entity names that appear inside a free-form relation string."""
    if not rel_text:
        return []
    t = rel_text.lower()
    hits = [e for e in allowed_entities if e.lower() in t]
    # Deduplicate while preserving order
    seen: Set[str] = set()
    out: List[str] = []
    for e in hits:
        if e not in seen:
            seen.add(e)
            out.append(e)
    return out


def _build_rich_context(
    *,
    info_desc: str,
    info_entities: List[str],
    entities: List[Dict[str, Any]],
    relations: List[Dict[str, Any]],
    attributes: Dict[str, List[Dict[str, Any]]],
    primary_keys: Dict[str, List[str]],
    domain: Optional[str],
    previous_check: Optional[Dict[str, Any]],
) -> str:
    """Build a richer schema context: entities + participating relations + connected entities + attribute descriptions."""
    allowed_entities = {
        (e.get("name") if isinstance(e, dict) else getattr(e, "name", "")) for e in (entities or [])
    }
    allowed_entities = {e for e in allowed_entities if e}

    entity_by_name: Dict[str, Dict[str, Any]] = {}
    for e in entities or []:
        if isinstance(e, dict) and e.get("name"):
            entity_by_name[e["name"]] = e

    # Determine connected entities (1-hop) via relations that touch any info entity
    info_entities_canon = [x for x in info_entities if x in allowed_entities]
    connected: Set[str] = set(info_entities_canon)
    participating_relations: List[Dict[str, Any]] = []
    for rel in relations or []:
        rel_ents = rel.get("entities") or []
        if not isinstance(rel_ents, list):
            continue
        if any(e in rel_ents for e in info_entities_canon):
            participating_relations.append(rel)
            for e in rel_ents:
                if isinstance(e, str) and e in allowed_entities:
                    connected.add(e)

    # Build context parts
    parts: List[str] = []
    if domain:
        parts.append(f"Domain: {domain}")
    parts.append(f"Information Need (MUST echo this exactly in output.information_need): {info_desc}")
    parts.append(f"Entities Involved (canonical): {', '.join(info_entities_canon) if info_entities_canon else '(none)'}")

    def _fmt_entity_block(entity_name: str) -> str:
        e = entity_by_name.get(entity_name, {})
        desc = (e.get("description") or "").strip()
        pk = primary_keys.get(entity_name, []) or []
        attrs = attributes.get(entity_name, []) or []
        attr_lines: List[str] = []
        for a in attrs:
            a_name = extract_attribute_name(a)
            a_desc = (extract_attribute_description(a) or "").strip()
            if a_desc:
                attr_lines.append(f"    - {a_name}: {a_desc}")
            else:
                attr_lines.append(f"    - {a_name}")
        header = f"- {entity_name}"
        if desc:
            header += f": {desc}"
        if pk:
            header += f" (PK: {', '.join(pk)})"
        block = [header, "  Attributes (intrinsic only; foreign keys handled later):"]
        block.extend(attr_lines if attr_lines else ["    - (none)"])
        return "\n".join(block)

    parts.append("Entities + attributes:")
    for name in sorted(connected):
        parts.append(_fmt_entity_block(name))

    if participating_relations:
        rel_lines: List[str] = []
        for rel in participating_relations:
            rel_ents = rel.get("entities") or []
            rel_type = rel.get("type", "")
            rel_desc = rel.get("description", "")
            ent_str = ", ".join([e for e in rel_ents if isinstance(e, str)])
            # Make it explicit that this relation EXISTS
            line = f"- EXISTS: {rel_type} relation between {ent_str}"
            if rel_desc:
                line += f" ({rel_desc})"
            rel_lines.append(line)
        parts.append("Relations involving involved/connected entities (these relations ALREADY EXIST - do NOT list them as missing):")
        parts.extend(rel_lines)

    if previous_check:
        parts.append("Previous check (do NOT repeat already-resolved missing items):")
        prev = []
        if previous_check.get("missing_relations"):
            prev.append(f"- missing_relations: {previous_check.get('missing_relations')}")
        if previous_check.get("missing_entities"):
            prev.append(f"- missing_entities: {previous_check.get('missing_entities')}")
        if previous_check.get("missing_intrinsic_attributes"):
            prev.append(f"- missing_intrinsic_attributes: {previous_check.get('missing_intrinsic_attributes')}")
        if previous_check.get("missing_derived_attributes"):
            prev.append(f"- missing_derived_attributes: {previous_check.get('missing_derived_attributes')}")
        parts.extend(prev if prev else ["- (none)"])

    return "\n".join(parts)


def _apply_deterministic_cleanup(
    *,
    info_desc: str,
    entities: List[Dict[str, Any]],
    relations: List[Dict[str, Any]],
    attributes: Dict[str, List[Dict[str, Any]]],
    result: InformationCompletenessOutput,
) -> InformationCompletenessOutput:
    """Remove false-positive missing components deterministically."""
    allowed_entities = {
        (e.get("name") if isinstance(e, dict) else getattr(e, "name", "")) for e in (entities or [])
    }
    allowed_entities = {e for e in allowed_entities if e}

    # Canonicalize and filter missing_entities
    cleaned_missing_entities: List[str] = []
    for m in result.missing_entities or []:
        canon = _canonicalize_entity_name(m, allowed_entities)
        if canon is not None:
            # It's actually present (or a clear variant). Remove it.
            logger.warning(
                f"Information need '{info_desc}' reports missing entity '{m}' but it exists in the schema (as '{canon}') - filtering out false positive"
            )
            continue
        cleaned_missing_entities.append(m)

    # Build attribute name set per entity (case-insensitive)
    attr_names_by_entity_ci: Dict[str, Set[str]] = {}
    for e in allowed_entities:
        names = set()
        for a in attributes.get(e, []) or []:
            names.add(extract_attribute_name(a).lower())
        attr_names_by_entity_ci[e] = names

    cleaned_missing_intrinsic: List[MissingIntrinsicAttribute] = []
    for a in result.missing_intrinsic_attributes or []:
        ent = a.entity
        canon_ent = _canonicalize_entity_name(ent, allowed_entities)
        if canon_ent is None:
            logger.warning(
                f"Information need '{info_desc}' reports missing intrinsic attribute for entity '{ent}' but entity doesn't exist - filtering out"
            )
            continue
        attr_name = (a.attribute or "").strip()
        if attr_name and attr_name.lower() in attr_names_by_entity_ci.get(canon_ent, set()):
            logger.warning(
                f"Information need '{info_desc}' reports missing intrinsic attribute '{ent}.{attr_name}' but it exists - filtering out false positive"
            )
            continue
        cleaned_missing_intrinsic.append(
            MissingIntrinsicAttribute(entity=canon_ent, attribute=attr_name, reasoning=a.reasoning)
        )

    cleaned_missing_derived: List[MissingDerivedAttribute] = []
    for a in result.missing_derived_attributes or []:
        ent = a.entity
        canon_ent = _canonicalize_entity_name(ent, allowed_entities)
        if canon_ent is None:
            logger.warning(
                f"Information need '{info_desc}' reports missing derived attribute for entity '{ent}' but entity doesn't exist - filtering out"
            )
            continue
        attr_name = (a.attribute or "").strip()
        # If it already exists as an intrinsic attribute, don't claim it's missing
        if attr_name and attr_name.lower() in attr_names_by_entity_ci.get(canon_ent, set()):
            logger.warning(
                f"Information need '{info_desc}' reports missing derived attribute '{ent}.{attr_name}' but attribute exists - filtering out false positive"
            )
            continue
        if _looks_like_aggregate_metric(a.derivation_hint or ""):
            logger.warning(
                f"Information need '{info_desc}' suggests aggregate/query-metric derived attribute '{canon_ent}.{attr_name}' - filtering out (not a row-level derived attribute)"
            )
            continue
        cleaned_missing_derived.append(
            MissingDerivedAttribute(
                entity=canon_ent,
                attribute=attr_name,
                derivation_hint=a.derivation_hint,
                reasoning=a.reasoning,
            )
        )

    # Filter missing_relations (structured): require 2+ known entities; drop if relation already exists.
    graph = _build_entity_graph(relations or [])
    cleaned_missing_relations: List[MissingRelation] = []
    for mr in result.missing_relations or []:
        mr_entities = list(mr.entities or [])
        mr_desc = (mr.description or "").strip()

        canon_entities: List[str] = []
        for e in mr_entities:
            canon = _canonicalize_entity_name(e, allowed_entities)
            if canon is None:
                logger.warning(
                    f"Information need '{info_desc}' reports missing relation with unknown entity '{e}' - filtering out"
                )
                canon_entities = []
                break
            canon_entities.append(canon)

        # Dedup while preserving order
        seen_e: Set[str] = set()
        canon_entities = [e for e in canon_entities if e and not (e in seen_e or seen_e.add(e))]

        if len(canon_entities) < 2:
            logger.warning(
                f"Information need '{info_desc}' reports missing relation but does not specify 2+ valid entities - filtering out"
            )
            continue

        # If any entity pair already has a relation, treat as present
        found_present = False
        for i in range(len(canon_entities)):
            for j in range(i + 1, len(canon_entities)):
                a, b = canon_entities[i], canon_entities[j]
                if any(_relation_has_pair(rel, a, b) for rel in relations or []):
                    found_present = True
                    break
                if _has_join_path(graph, a, b):
                    found_present = True
                    break
            if found_present:
                break

        if found_present:
            logger.warning(
                f"Information need '{info_desc}' reports missing relation {canon_entities} but entities are already join-connected - filtering out false positive"
            )
            continue

        cleaned_missing_relations.append(MissingRelation(entities=canon_entities, description=mr_desc))

    all_missing_count = (
        len(cleaned_missing_entities)
        + len(cleaned_missing_relations)
        + len(cleaned_missing_intrinsic)
        + len(cleaned_missing_derived)
    )
    satisfied = all_missing_count == 0
    all_present = satisfied

    return InformationCompletenessOutput(
        information_need=info_desc,
        all_present=all_present,
        missing_relations=cleaned_missing_relations,
        missing_entities=cleaned_missing_entities,
        missing_intrinsic_attributes=cleaned_missing_intrinsic,
        missing_derived_attributes=cleaned_missing_derived,
        satisfied=satisfied,
        reasoning=result.reasoning,
    )


@traceable_step("3.2", phase=3, tags=['phase_3_step_2'])
async def step_3_2_information_completeness_single(
    information_need: Dict[str, Any],  # Information need from Step 3.1
    entities: List[Dict[str, Any]],  # All entities from Phase 1
    relations: List[Dict[str, Any]],  # All relations from Phase 1
    attributes: Dict[str, List[Dict[str, Any]]],  # entity -> attributes from Phase 2
    primary_keys: Dict[str, List[str]],  # entity -> PK from Phase 2
    foreign_keys: List[Dict[str, Any]],  # Foreign keys from Phase 2
    constraints: Optional[List[Dict[str, Any]]] = None,  # Constraints from Phase 2
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
    previous_check: Optional[Dict[str, Any]] = None,  # For loop iterations
) -> Dict[str, Any]:
    """
    Step 3.2 (per-information): Check if all necessary components are present for an information need.
    
    This is designed to be called in parallel for multiple information needs, and iteratively
    for each information need until satisfied.
    
    Args:
        information_need: Information need dictionary with description, frequency, entities_involved, reasoning
        entities: List of all entities with descriptions from Phase 1
        relations: List of all relations from Phase 1
        attributes: Dictionary mapping entity names to their attributes from Phase 2
        primary_keys: Dictionary mapping entity names to their primary keys from Phase 2
        foreign_keys: List of foreign key specifications from Phase 2
        constraints: Optional list of constraints from Phase 2
        nl_description: Optional original NL description
        domain: Optional domain context from Phase 1
        previous_check: Optional result from previous iteration (for loop)
        
    Returns:
        dict: Completeness check result with all_present, missing_components, satisfied, and reasoning
        
    Example:
        >>> result = await step_3_2_information_completeness_single(
        ...     information_need={"description": "List orders by customer", "entities_involved": ["Customer", "Order"]},
        ...     entities=[{"name": "Customer"}, {"name": "Order"}],
        ...     relations=[{"entities": ["Customer", "Order"]}],
        ...     attributes={"Customer": [], "Order": []},
        ...     primary_keys={"Customer": ["customer_id"], "Order": ["order_id"]},
        ...     foreign_keys=[]
        ... )
        >>> result["satisfied"]
        True
    """
    info_desc = information_need.get("description", "") if isinstance(information_need, dict) else getattr(information_need, "description", "")
    info_entities = information_need.get("entities_involved", []) if isinstance(information_need, dict) else getattr(information_need, "entities_involved", [])
    
    logger.debug(f"Checking completeness for information need: {info_desc}")
    
    context_msg = _build_rich_context(
        info_desc=info_desc,
        info_entities=info_entities,
        entities=entities,
        relations=relations,
        attributes=attributes,
        primary_keys=primary_keys,
        domain=domain,
        previous_check=previous_check,
    )
    
    # System prompt (NO tools; deterministic code will validate the output)
    system_prompt = """You are a database schema validation expert. Your task is to verify if all necessary components exist to answer a specific information need.

CRITICAL CONSTRAINTS:
1) The output field information_need MUST exactly equal the provided information need string in context.
2) You MUST NOT claim an entity or attribute is missing if it already appears in the provided context.
3) missing_relations MUST be a list of objects with:
   - entities: list of 2+ canonical entity names from context
   - description: short description of the relation
   Do NOT use free-form strings for missing_relations.

For this information need, check:
(a) Do all necessary entities and relations exist?
(b) Are we missing any intrinsic attributes in entities related to this info?
(c) Would adding any derived attributes make querying easier?

**WHAT ARE INTRINSIC ATTRIBUTES?**
Intrinsic attributes are properties that belong directly to the entity itself, not relationships. Examples:
- Customer: name, email, phone, address
- Order: order_id, order_date, total_amount, status
- Product: product_id, name, price, description

**WHAT ARE NOT INTRINSIC ATTRIBUTES?**
- Foreign keys (e.g., customer_id in Order) - these are handled automatically in Step 3.5
- Relation-connecting attributes - these are handled automatically
- Relation attributes (e.g., quantity in Order-Product) - these are handled in Step 2.15

**WHAT ARE DERIVED ATTRIBUTES?**
Derived attributes are computed from other attributes and can make querying easier. Examples:
- "is_recent" = (order_date >= CURRENT_DATE - INTERVAL '1 month') - makes filtering easier
- "full_name" = first_name + ' ' + last_name - makes output formatting easier
- "age" = CURRENT_DATE - birth_date - makes filtering/grouping easier

**DETAILED EXAMPLE**:

Information need: "Find all orders placed by a specific customer in the last month"

**EXPECTED OUTPUT**:
```json
{
  "information_need": "Find all orders placed by a specific customer in the last month",
  "all_present": true,
  "missing_relations": [],
  "missing_entities": [],
  "missing_intrinsic_attributes": [],
  "missing_derived_attributes": [
    {
      "entity": "Order",
      "attribute": "is_recent",
      "derivation_hint": "is_recent = (order_date >= CURRENT_DATE - INTERVAL '1 month')",
      "reasoning": "Adding a derived attribute 'is_recent' would make querying easier, allowing direct filtering without date calculations in queries"
    }
  ],
  "satisfied": true,
  "reasoning": "All necessary entities (Customer, Order) and relations (Customer-Order) exist. All required intrinsic attributes exist (customer_id, order_id, order_date). A derived attribute 'is_recent' would make querying easier but is optional. The information need can be satisfied."
}
```

**Explanation**: The LLM correctly identifies that all entities, relations, and intrinsic attributes exist. It suggests a derived attribute for easier querying but marks the information need as satisfied since it can be achieved with existing attributes.

COMPLETENESS CHECK:
1. **Entities**: Are all entities mentioned in the information need present in the schema?
2. **Relations**: Are all necessary relationships between entities present?
3. **Intrinsic Attributes**: Do all entities have the intrinsic attributes needed to answer the query?
   - Consider filtering attributes (e.g., date ranges, status filters)
   - Consider aggregation attributes (e.g., amounts, quantities)
   - Consider output attributes (what information is returned)
   - Note: Foreign keys and relation-connecting attributes are NOT considered intrinsic - they will be handled automatically
4. **Derived Attributes**: Would adding any derived attributes make querying easier?
   - Consider computed attributes that simplify filtering, grouping, or output formatting
   - These are optional but can improve query performance and readability

ITERATIVE REFINEMENT:
- If components are missing, specify exactly what is missing
- After missing components are added, re-check until satisfied
- Be thorough: consider all aspects of the query (filtering, joining, aggregating, sorting)

For each missing component, provide:
- missing_relations: Descriptions of missing relationships (e.g., "Customer-Order relation")
- missing_entities: Names of missing entities
- missing_intrinsic_attributes: List of {{entity, attribute, reasoning}} for missing intrinsic attributes
- missing_derived_attributes: List of {{entity, attribute, derivation_hint, reasoning}} for missing derived attributes

Return a JSON object with:
- information_need: The information need being checked
- all_present: True if all components are present, False otherwise
- missing_relations: List of missing relation descriptions
- missing_entities: List of missing entity names
- missing_intrinsic_attributes: List of missing intrinsic attributes (empty if none)
- missing_derived_attributes: List of missing derived attributes (empty if none)
- satisfied: True if you're confident all components are present, False if re-check needed after adding missing components
- reasoning: REQUIRED - Explanation of your assessment (cannot be omitted)"""
    
    # Human prompt template with explicit instructions
    human_prompt_template = """Check if all necessary components exist to answer this information need.

{context}

Natural Language Description:
{nl_description}

IMPORTANT:
- Do NOT list as missing anything that is already present in the context.
- missing_intrinsic_attributes should ONLY include attributes not listed under the entity's Attributes in context.
- missing_relations must be structured objects (entities list + description) using canonical entity names in the context.
- CRITICAL: If a relation is listed under "Relations involving involved/connected entities" with "EXISTS:" prefix, that relation ALREADY EXISTS. Do NOT list it as missing, even if the entity order is different (e.g., "Trip, Rider" vs "Rider, Trip").

Return JSON per schema."""
    
    try:
        # Get model for this step (important task)
        llm = get_model_for_step("3.2")

        # Invoke standardized LLM call WITHOUT tools (faster + deterministic validation)
        config = get_trace_config("3.2", phase=3, tags=["phase_3_step_2"])
        result: InformationCompletenessOutput = await standardized_llm_call(
            llm=llm,
            output_schema=InformationCompletenessOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt_template,
            input_data={
                "context": context_msg,
                "nl_description": nl_description or "",
            },
            tools=None,
            use_agent_executor=False,
            decouple_tools=False,
            config=config,
        )

        # Deterministic cleanup: remove false positives and enforce schema invariants
        result = _apply_deterministic_cleanup(
            info_desc=info_desc,
            entities=entities,
            relations=relations,
            attributes=attributes,
            result=result,
        )
        
        # Convert to dict only at return boundary
        result_dict = result.model_dump()
        result_dict["information_need"] = info_desc  # Ensure it's set
        return result_dict
        
    except Exception as e:
        logger.error(f"Error in completeness check for '{info_desc}': {e}", exc_info=True)
        raise


async def step_3_2_information_completeness_single_with_loop(
    information_need: Dict[str, Any],
    entities: List[Dict[str, Any]],
    relations: List[Dict[str, Any]],
    attributes: Dict[str, List[Dict[str, Any]]],
    primary_keys: Dict[str, List[str]],
    foreign_keys: List[Dict[str, Any]],
    constraints: Optional[List[Dict[str, Any]]] = None,
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
    max_iterations: int = 5,
    max_time_sec: int = 180,
) -> Dict[str, Any]:
    """
    Step 3.2 with automatic looping: continues until satisfied is True.
    
    This function implements the iterative loop specified in the plan: continues
    checking completeness until the LLM is satisfied that all components are present.
    
    Args:
        information_need: Information need dictionary from Step 3.1
        entities: List of all entities with descriptions from Phase 1
        relations: List of all relations from Phase 1
        attributes: Dictionary mapping entity names to their attributes from Phase 2
        primary_keys: Dictionary mapping entity names to their primary keys from Phase 2
        foreign_keys: List of foreign key specifications from Phase 2
        constraints: Optional list of constraints from Phase 2
        nl_description: Optional original NL description
        domain: Optional domain context from Phase 1
        max_iterations: Maximum number of loop iterations per information need (default: 5)
        max_time_sec: Maximum wall time in seconds per information need (default: 180)
        
    Returns:
        dict: Final completeness check result with loop metadata
        
    Example:
        >>> result = await step_3_2_information_completeness_single_with_loop(
        ...     information_need={"description": "List orders", "entities_involved": ["Order"]},
        ...     entities=[{"name": "Order"}],
        ...     relations=[],
        ...     attributes={"Order": []},
        ...     primary_keys={"Order": ["order_id"]},
        ...     foreign_keys=[]
        ... )
        >>> result["final_result"]["satisfied"]
        True
    """
    from NL2DATA.utils.loops import SafeLoopExecutor, LoopConfig
    
    info_desc = information_need.get("description", "") if isinstance(information_need, dict) else getattr(information_need, "description", "")
    logger.debug(f"Starting completeness check loop for: {info_desc}")
    
    previous_check = None
    
    async def completeness_check_step(previous_result=None):
        """Single iteration of completeness check."""
        nonlocal previous_check
        
        if previous_result:
            previous_check = previous_result
        
        result = await step_3_2_information_completeness_single(
            information_need=information_need,
            entities=entities,
            relations=relations,
            attributes=attributes,
            primary_keys=primary_keys,
            foreign_keys=foreign_keys,
            constraints=constraints,
            nl_description=nl_description,
            domain=domain,
            previous_check=previous_check,
        )
        return result
    
    # Termination check: satisfied must be True
    def termination_check(result: Dict[str, Any]) -> bool:
        return result.get("satisfied", False)
    
    # Configure loop
    loop_config = LoopConfig(
        max_iterations=max_iterations,
        max_wall_time_sec=max_time_sec,
        oscillation_window=3,
        enable_cycle_detection=True,
    )
    
    # Execute loop
    executor = SafeLoopExecutor()
    loop_result = await executor.run_loop(
        step_func=completeness_check_step,
        termination_check=termination_check,
        config=loop_config,
    )
    
    final_result = loop_result["result"]
    iterations = loop_result["iterations"]
    terminated_by = loop_result["terminated_by"]
    satisfied = final_result.get("satisfied", False)
    
    # Log warning if loop terminated without satisfaction
    if not satisfied:
        if terminated_by == "max_iterations":
            logger.warning(
                f"Completeness check loop for '{info_desc}' reached max iterations ({iterations}) "
                f"without satisfaction. Missing components may not have been fully addressed. "
                f"Consider increasing max_iterations or investigating why satisfaction is not being reached."
            )
        elif terminated_by == "timeout":
            logger.warning(
                f"Completeness check loop for '{info_desc}' timed out after {iterations} iterations "
                f"without satisfaction. Missing components may not have been fully addressed. "
                f"Consider increasing max_time_sec or investigating performance issues."
            )
        elif terminated_by == "oscillation":
            logger.warning(
                f"Completeness check loop for '{info_desc}' detected oscillation after {iterations} iterations "
                f"without satisfaction. The loop may be stuck in a cycle. "
                f"Missing components: {final_result.get('missing_entities', []) + final_result.get('missing_intrinsic_attributes', [])}"
            )
        else:
            logger.warning(
                f"Completeness check loop for '{info_desc}' terminated by '{terminated_by}' "
                f"without satisfaction. Missing components may not have been fully addressed."
            )
    else:
        logger.info(
            f"Completeness check loop for '{info_desc}' completed successfully: {iterations} iterations, "
            f"terminated by: {terminated_by}"
        )
    
    return {
        "final_result": final_result,
        "loop_metadata": {
            "iterations": iterations,
            "terminated_by": terminated_by,
            "satisfied": satisfied,
        }
    }


async def step_3_2_information_completeness_batch(
    information_needs: List[Dict[str, Any]],  # Information needs from Step 3.1
    entities: List[Dict[str, Any]],
    relations: List[Dict[str, Any]],
    attributes: Dict[str, List[Dict[str, Any]]],
    primary_keys: Dict[str, List[str]],
    foreign_keys: List[Dict[str, Any]],
    constraints: Optional[List[Dict[str, Any]]] = None,
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
    max_iterations: int = 5,
    max_time_sec: int = 180,
) -> Dict[str, Any]:
    """
    Step 3.2: Check completeness for all information needs (parallel execution).
    
    Each information need is checked in parallel, and each check loops until satisfied.
    
    Args:
        information_needs: List of information needs from Step 3.1
        entities: List of all entities with descriptions from Phase 1
        relations: List of all relations from Phase 1
        attributes: Dictionary mapping entity names to their attributes from Phase 2
        primary_keys: Dictionary mapping entity names to their primary keys from Phase 2
        foreign_keys: List of foreign key specifications from Phase 2
        constraints: Optional list of constraints from Phase 2
        nl_description: Optional original NL description
        domain: Optional domain context from Phase 1
        max_iterations: Maximum number of loop iterations per information need (default: 5)
        max_time_sec: Maximum wall time in seconds per information need (default: 180)
        
    Returns:
        dict: Completeness check results for all information needs
        
    Example:
        >>> result = await step_3_2_information_completeness_batch(
        ...     information_needs=[{"description": "List orders", "entities_involved": ["Order"]}],
        ...     entities=[{"name": "Order"}],
        ...     relations=[],
        ...     attributes={"Order": []},
        ...     primary_keys={"Order": ["order_id"]},
        ...     foreign_keys=[]
        ... )
        >>> len(result["completeness_results"]) > 0
        True
    """
    logger.info(f"Starting Step 3.2: Information Completeness Check for {len(information_needs)} information needs")
    
    if not information_needs:
        logger.warning("No information needs provided for completeness check")
        return {"completeness_results": {}}
    
    # Execute in parallel for all information needs
    import asyncio
    
    tasks = []
    for info_need in information_needs:
        task = step_3_2_information_completeness_single_with_loop(
            information_need=info_need,
            entities=entities,
            relations=relations,
            attributes=attributes,
            primary_keys=primary_keys,
            foreign_keys=foreign_keys,
            constraints=constraints,
            nl_description=nl_description,
            domain=domain,
            max_iterations=max_iterations,
            max_time_sec=max_time_sec,
        )
        # Use information need description as identifier
        info_desc = info_need.get("description", "") if isinstance(info_need, dict) else getattr(info_need, "description", "")
        info_id = info_desc if info_desc else f"info_{len(tasks)}"  # Use full description as ID
        tasks.append((info_id, task))
    
    # Wait for all tasks to complete
    results = await asyncio.gather(
        *[task for _, task in tasks],
        return_exceptions=True
    )
    
    # Process results
    completeness_results = {}
    for i, ((info_id, _), result) in enumerate(zip(tasks, results)):
        if isinstance(result, Exception):
            logger.error(f"Error processing information need {info_id}: {result}")
            completeness_results[info_id] = {
                "information_need": info_id,
                "all_present": False,
                "missing_relations": [],
                "missing_entities": [],
                "missing_intrinsic_attributes": [],
                "missing_derived_attributes": [],
                "satisfied": False,
                "reasoning": f"Error during analysis: {str(result)}"
            }
        else:
            completeness_results[info_id] = result.get("final_result", {})
    
    total_satisfied = sum(
        1 for r in completeness_results.values()
        if r.get("satisfied", False)
    )
    total_missing = sum(
        len(r.get("missing_entities", [])) + 
        len(r.get("missing_intrinsic_attributes", [])) + 
        len(r.get("missing_derived_attributes", [])) + 
        len(r.get("missing_relations", []))
        for r in completeness_results.values()
    )
    logger.info(
        f"Information completeness check completed: {total_satisfied}/{len(completeness_results)} satisfied, "
        f"{total_missing} total missing components identified"
    )
    
    return {"completeness_results": completeness_results}

