"""Phase 3, Step 3.2: Information Completeness Check.

For each information need, verify if all necessary relations, entities, and attributes are present.
Iterative loop per information need until LLM is satisfied.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

from NL2DATA.phases.phase3.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.phases.phase1.utils.data_extraction import extract_attribute_name
from NL2DATA.utils.loops import SafeLoopExecutor, LoopConfig
from NL2DATA.utils.tools.validation_tools import (
    _check_schema_component_exists_impl,
    _validate_attributes_exist_impl,
)

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


class InformationCompletenessOutput(BaseModel):
    """Output structure for information completeness check."""
    information_need: str = Field(description="The information need being checked")
    all_present: bool = Field(description="Whether all necessary components are present")
    missing_relations: List[str] = Field(
        default_factory=list,
        description="List of missing relation descriptions (e.g., 'Customer-Order relation')"
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
    
    # Build comprehensive schema context
    context_parts = []
    if domain:
        context_parts.append(f"Domain: {domain}")
    
    # Information need details
    context_parts.append(f"Information Need: {info_desc}")
    if info_entities:
        context_parts.append(f"Entities Involved: {', '.join(info_entities)}")
    
    # Entity details (only for involved entities)
    # Note: attributes shown here are intrinsic attributes (after Step 2.14 cleanup)
    entity_details = []
    for entity_name in info_entities:
        entity_obj = next(
            (e for e in entities if (e.get("name") if isinstance(e, dict) else getattr(e, "name", "")) == entity_name),
            None
        )
        if entity_obj:
            entity_desc = entity_obj.get("description", "") if isinstance(entity_obj, dict) else getattr(entity_obj, "description", "")
            entity_attrs = attributes.get(entity_name, [])
            entity_pk = primary_keys.get(entity_name, [])
            
            entity_info = f"  {entity_name}:"
            if entity_desc:
                entity_info += f" {entity_desc}"
            if entity_pk:
                entity_info += f" (PK: {', '.join(entity_pk)})"
            if entity_attrs:
                attr_names = [extract_attribute_name(attr) for attr in entity_attrs]
                entity_info += f" [Intrinsic Attributes: {', '.join(attr_names)}]"
            else:
                entity_info += " [Intrinsic Attributes: (none)]"
            entity_details.append(entity_info)
    
    if entity_details:
        context_parts.append(f"Entity Details (intrinsic attributes only - foreign keys handled automatically):\n" + "\n".join(entity_details))
    
    # Relations involving these entities
    relevant_relations = []
    for rel in relations:
        rel_entities = rel.get("entities", [])
        if any(e in rel_entities for e in info_entities):
            rel_type = rel.get("type", "")
            rel_desc = rel.get("description", "")
            rel_info = f"  {rel_type}: {', '.join(rel_entities)}"
            if rel_desc:
                rel_info += f" ({rel_desc})"
            relevant_relations.append(rel_info)
    
    if relevant_relations:
        context_parts.append(f"Relevant Relations:\n" + "\n".join(relevant_relations))
    
    # Foreign keys involving these entities
    relevant_fks = []
    for fk in foreign_keys:
        fk_from = fk.get("from_entity", "")
        fk_to = fk.get("to_entity", "")
        if fk_from in info_entities or fk_to in info_entities:
            fk_attrs = fk.get("attributes", [])
            fk_info = f"  {fk_from}.{', '.join(fk_attrs)} -> {fk_to}"
            relevant_fks.append(fk_info)
    
    if relevant_fks:
        context_parts.append(f"Relevant Foreign Keys:\n" + "\n".join(relevant_fks))
    
    # Previous check results (for loop iterations)
    if previous_check:
        prev_missing = []
        prev_relations = previous_check.get("missing_relations", [])
        prev_entities = previous_check.get("missing_entities", [])
        prev_intrinsic_attrs = previous_check.get("missing_intrinsic_attributes", [])
        prev_derived_attrs = previous_check.get("missing_derived_attributes", [])
        # Legacy support for old format
        prev_attrs = previous_check.get("missing_attributes", [])
        
        if prev_relations:
            prev_missing.append(f"  Missing relations: {', '.join(prev_relations)}")
        if prev_entities:
            prev_missing.append(f"  Missing entities: {', '.join(prev_entities)}")
        if prev_intrinsic_attrs:
            attr_strs = [f"{attr.get('entity', '')}.{attr.get('attribute', '')}" for attr in prev_intrinsic_attrs]
            prev_missing.append(f"  Missing intrinsic attributes: {', '.join(attr_strs)}")
        if prev_derived_attrs:
            attr_strs = [f"{attr.get('entity', '')}.{attr.get('attribute', '')}" for attr in prev_derived_attrs]
            prev_missing.append(f"  Missing derived attributes: {', '.join(attr_strs)}")
        # Legacy support
        if prev_attrs and not prev_intrinsic_attrs:
            attr_strs = [f"{attr.get('entity', '')}.{attr.get('attribute', '')}" for attr in prev_attrs]
            prev_missing.append(f"  Missing attributes: {', '.join(attr_strs)}")
        
        if prev_missing:
            context_parts.append(f"Previous Check Results:\n" + "\n".join(prev_missing))
    
    context_msg = "\n\nSchema Context:\n" + "\n".join(f"- {part}" for part in context_parts)
    
    # System prompt with enhanced instructions
    system_prompt = """You are a database schema validation expert. Your task is to verify if all necessary components exist to answer a specific information need.

**AVAILABLE TOOLS**: You have access to validation tools that you can use to verify your assessment:
- check_schema_component_exists_bound(component_type: str, name: str) -> bool: Check if a schema component (entity, attribute, relation) exists
- validate_attributes_exist_bound(entity: str, attributes: List[str]) -> Dict[str, bool]: Verify that attributes exist for an entity

**IMPORTANT**: Use these tools to verify your assessment before finalizing your response. For example:
- Before reporting a missing entity, use check_schema_component_exists_bound("entity", "EntityName") to verify it doesn't exist
- Before reporting missing attributes, use validate_attributes_exist_bound("EntityName", ["attr1", "attr2"]) to verify they don't exist

This ensures accuracy and prevents false positives.

**CRITICAL INSTRUCTIONS**: For this information need, check:
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

**IMPORTANT**: For this information need, check:
(a) Do all necessary entities and relations exist?
(b) Are we missing any intrinsic attributes in entities related to this info?
(c) Would adding any derived attributes make querying easier?

**Note**: Foreign keys and relation-connecting attributes are NOT considered intrinsic attributes - they will be handled automatically. Focus on intrinsic attributes that describe the entities themselves.

Return a JSON object specifying whether all components are present, what is missing (if anything), and whether you're satisfied."""
    
    try:
        # Get model for this step (important task)
        llm = get_model_for_step("3.2")
        
        # Build schema_state for tools
        schema_state = {
            "entities": entities,
            "relations": relations,
            "attributes": attributes,
            "primary_keys": primary_keys,
        }
        
        # Create bound versions of tools with schema_state
        def check_schema_component_exists_bound(component_type: str, name: str) -> bool:
            """Bound version of check_schema_component_exists with schema_state.
            
            Args:
                component_type: Type of component ("entity", "attribute", "relation", "table", "column")
                name: Name of the component to check
                
            Returns:
                True if component exists, False otherwise
                
            Purpose: Allows LLM to verify that schema components exist before reporting them as missing.
            """
            # NOTE: check_schema_component_exists is a LangChain @tool (StructuredTool) and is not callable.
            return _check_schema_component_exists_impl(component_type, name, schema_state)
        
        def validate_attributes_exist_bound(entity: str, attributes: List[str]) -> Dict[str, bool]:
            """Bound version of validate_attributes_exist with schema_state.
            
            Args:
                entity: Name of the entity to check
                attributes: List of attribute names to verify
                
            Returns:
                Dictionary mapping attribute name to existence status (True/False)
                
            Purpose: Allows LLM to verify that attributes exist for an entity before reporting them as missing.
            """
            # NOTE: validate_attributes_exist is a LangChain @tool (StructuredTool) and is not callable.
            return _validate_attributes_exist_impl(entity, attributes, schema_state)
        
        # Create tools list
        tools = [
            check_schema_component_exists_bound,
            validate_attributes_exist_bound,
        ]
        
        # Invoke standardized LLM call with tools
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
            tools=tools,
            use_agent_executor=True,  # Use agent executor for tool calls
            decouple_tools=True,  # Decouple tool calling from JSON generation
            config=config,
        )
        
        # Work with Pydantic model directly
        # Validate that mentioned entities exist
        all_entity_names = {
            e.get("name") if isinstance(e, dict) else getattr(e, "name", "")
            for e in entities
        }
        
        # Filter out false positives - entities that are reported as missing but actually exist
        missing_entities = result.missing_entities
        false_positives = []
        valid_missing = []
        for entity_name in missing_entities:
            if entity_name in all_entity_names:
                false_positives.append(entity_name)
                logger.warning(
                    f"Information need '{info_desc}' reports missing entity '{entity_name}' "
                    f"but it exists in the schema - filtering out false positive"
                )
            else:
                valid_missing.append(entity_name)
        
        # Validate missing_intrinsic_attributes and missing_derived_attributes entities exist
        valid_intrinsic_attrs = []
        invalid_intrinsic_attrs = []
        for attr in result.missing_intrinsic_attributes:
            attr_entity = attr.entity if isinstance(attr, MissingIntrinsicAttribute) else attr.get("entity", "")
            if attr_entity in all_entity_names:
                valid_intrinsic_attrs.append(attr)
            else:
                invalid_intrinsic_attrs.append(attr_entity)
                logger.warning(
                    f"Information need '{info_desc}' reports missing intrinsic attribute for entity '{attr_entity}' "
                    f"but entity doesn't exist - filtering out"
                )
        
        valid_derived_attrs = []
        invalid_derived_attrs = []
        for attr in result.missing_derived_attributes:
            attr_entity = attr.entity if isinstance(attr, MissingDerivedAttribute) else attr.get("entity", "")
            if attr_entity in all_entity_names:
                valid_derived_attrs.append(attr)
            else:
                invalid_derived_attrs.append(attr_entity)
                logger.warning(
                    f"Information need '{info_desc}' reports missing derived attribute for entity '{attr_entity}' "
                    f"but entity doesn't exist - filtering out"
                )
        
        # Update the result with filtered missing entities and attributes (create new model instance)
        if false_positives or invalid_intrinsic_attrs or invalid_derived_attrs:
            result = InformationCompletenessOutput(
                information_need=info_desc,
                all_present=result.all_present,
                missing_relations=result.missing_relations,
                missing_entities=valid_missing,
                missing_intrinsic_attributes=valid_intrinsic_attrs,
                missing_derived_attributes=valid_derived_attrs,
                satisfied=result.satisfied if (valid_missing or valid_intrinsic_attrs or valid_derived_attrs) else True,
                reasoning=result.reasoning
            )
            if false_positives and not valid_missing and not valid_intrinsic_attrs and not valid_derived_attrs:
                logger.info(
                    f"Information need '{info_desc}': All reported missing components were false positives. "
                    f"Marking as satisfied."
                )
        
        logger.debug(
            f"Completeness check for '{info_desc}': "
            f"all_present={result.all_present}, "
            f"satisfied={result.satisfied}"
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

