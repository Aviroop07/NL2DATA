"""Phase 3, Step 3.1: Information Need Identification.

Identifies what information users will frequently query.
Iterative loop continues until LLM suggests no further additions or deletions.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

from NL2DATA.phases.phase3.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.phases.phase1.utils.data_extraction import extract_attribute_name
from NL2DATA.utils.loops import SafeLoopExecutor, LoopConfig
from NL2DATA.utils.tools.validation_tools import _verify_entities_exist_impl

logger = get_logger(__name__)


class InformationNeed(BaseModel):
    """Single information need specification."""
    description: str = Field(description="Description of the information need (what users want to query)")
    frequency: str = Field(description="Frequency of this query (e.g., 'frequent', 'occasional', 'rare')")
    entities_involved: List[str] = Field(
        default_factory=list,
        description="List of entity names involved in answering this information need"
    )
    reasoning: str = Field(description="Reasoning for why this information need is important")


class InformationNeedIdentificationOutput(BaseModel):
    """Output structure for information need identification."""
    information_needs: List[InformationNeed] = Field(
        description="List of identified information needs with descriptions, frequency, entities, and reasoning"
    )
    additions: List[str] = Field(
        default_factory=list,
        description="List of newly added information need descriptions (for tracking changes)"
    )
    deletions: List[str] = Field(
        default_factory=list,
        description="List of information need descriptions that should be removed (for tracking changes)"
    )
    no_more_changes: bool = Field(
        description="Whether the LLM suggests no further additions or deletions (termination condition for loop)"
    )
    reasoning: str = Field(description="Reasoning for the additions, deletions, and termination decision")


@traceable_step("3.1", phase=3, tags=['phase_3_step_1'])
async def step_3_1_information_need_identification(
    nl_description: str,
    entities: List[Dict[str, Any]],  # All entities from Phase 1
    relations: List[Dict[str, Any]],  # All relations from Phase 1
    attributes: Dict[str, List[Dict[str, Any]]],  # entity -> attributes from Phase 2
    primary_keys: Dict[str, List[str]],  # entity -> PK from Phase 2
    foreign_keys: List[Dict[str, Any]],  # Foreign keys from Phase 2
    domain: Optional[str] = None,
    example_queries: Optional[List[Dict[str, str]]] = None,  # Optional example queries as context
    previous_information_needs: Optional[List[Dict[str, Any]]] = None,  # For loop iterations
) -> Dict[str, Any]:
    """
    Step 3.1: Identify what information users will frequently query.
    
    This step identifies information needs that the database should support.
    Designed to be called iteratively until no_more_changes is True.
    
    Args:
        nl_description: Original natural language description
        entities: List of all entities with descriptions from Phase 1
        relations: List of all relations from Phase 1
        attributes: Dictionary mapping entity names to their attributes from Phase 2
        primary_keys: Dictionary mapping entity names to their primary keys from Phase 2
        foreign_keys: List of foreign key specifications from Phase 2
        domain: Optional domain context from Phase 1
        example_queries: Optional list of example queries as context (format: [{"description": str, "sql": str}])
        previous_information_needs: Optional list of previously identified information needs (for loop iterations)
        
    Returns:
        dict: Information need identification result with information_needs, additions, deletions, no_more_changes, and reasoning
        
    Example:
        >>> result = await step_3_1_information_need_identification(
        ...     nl_description="E-commerce database",
        ...     entities=[{"name": "Customer"}, {"name": "Order"}],
        ...     relations=[{"entities": ["Customer", "Order"]}],
        ...     attributes={"Customer": [{"name": "name"}]},
        ...     primary_keys={"Customer": ["customer_id"]},
        ...     foreign_keys=[]
        ... )
        >>> len(result["information_needs"]) > 0
        True
    """
    logger.info("Starting Step 3.1: Information Need Identification")
    
    # Build comprehensive schema context
    context_parts = []
    if domain:
        context_parts.append(f"Domain: {domain}")
    
    # Entity summary
    entity_summary = []
    for entity in entities:
        entity_name = entity.get("name") if isinstance(entity, dict) else getattr(entity, "name", "")
        entity_desc = entity.get("description", "") if isinstance(entity, dict) else getattr(entity, "description", "")
        entity_attrs = attributes.get(entity_name, [])
        entity_pk = primary_keys.get(entity_name, [])
        
        entity_info = f"  - {entity_name}"
        if entity_desc:
            entity_info += f": {entity_desc}"
        if entity_pk:
            entity_info += f" (PK: {', '.join(entity_pk)})"
        if entity_attrs:
            attr_names = [extract_attribute_name(attr) for attr in entity_attrs]
            entity_info += f" [Attributes: {', '.join(attr_names)}]"
        entity_summary.append(entity_info)
    
    context_parts.append(f"Entities ({len(entities)}):\n" + "\n".join(entity_summary))
    
    # Relations summary
    if relations:
        rel_summary = []
        for rel in relations[:10]:  # Limit to avoid too long context
            rel_entities = rel.get("entities", [])
            rel_type = rel.get("type", "")
            rel_desc = rel.get("description", "")
            rel_info = f"  - {rel_type}: {', '.join(rel_entities)}"
            if rel_desc:
                rel_info += f" ({rel_desc})"
            rel_summary.append(rel_info)
        context_parts.append(f"Relations ({len(relations)}):\n" + "\n".join(rel_summary))
        if len(relations) > 10:
            context_parts.append(f"  ... and {len(relations) - 10} more relations")
    
    # Foreign keys summary
    if foreign_keys:
        fk_summary = []
        for fk in foreign_keys[:10]:
            fk_from = fk.get("from_entity", "")
            fk_to = fk.get("to_entity", "")
            fk_attrs = fk.get("attributes", [])
            fk_info = f"  - {fk_from}.{', '.join(fk_attrs)} -> {fk_to}"
            fk_summary.append(fk_info)
        context_parts.append(f"Foreign Keys ({len(foreign_keys)}):\n" + "\n".join(fk_summary))
        if len(foreign_keys) > 10:
            context_parts.append(f"  ... and {len(foreign_keys) - 10} more foreign keys")
    
    # Previous information needs (for loop iterations)
    if previous_information_needs:
        prev_summary = []
        for info in previous_information_needs:
            info_desc = info.get("description", "") if isinstance(info, dict) else getattr(info, "description", "")
            if info_desc:
                prev_summary.append(f"  - {info_desc}")
        context_parts.append(f"Previously Identified Information Needs ({len(previous_information_needs)}):\n" + "\n".join(prev_summary))
    
    # Example queries (if provided)
    if example_queries:
        example_summary = []
        for query in example_queries[:5]:
            query_desc = query.get("description", "")
            query_sql = query.get("sql", "")
            example_info = f"  - {query_desc}"
            if query_sql:
                example_info += f" (SQL: {query_sql})"
            example_summary.append(example_info)
        context_parts.append(f"Example Queries:\n" + "\n".join(example_summary))
    
    context_msg = "\n\nSchema Context:\n" + "\n".join(f"- {part}" for part in context_parts)
    
    # System prompt
    system_prompt = """You are a database design expert. Your task is to identify what information users will frequently need to query from the database.

**AVAILABLE TOOLS**: You have access to a validation tool:
- verify_entities_exist_bound(entities: List[str]) -> Dict[str, bool]: Verify that entities mentioned in information needs exist in the schema

**IMPORTANT**: Use this tool to verify that all entities mentioned in entities_involved actually exist in the schema before finalizing your response. This ensures accuracy and prevents referencing non-existent entities.

INFORMATION NEED IDENTIFICATION:
1. **Think like a user**: What questions will users ask? What reports will they need?
2. **Consider common queries**: 
   - Aggregations (counts, sums, averages)
   - Filtering (by date, status, category)
   - Joins (across related entities)
   - Grouping and sorting
   - Time-based analysis
3. **Consider business operations**: What information supports daily operations, reporting, analytics?
4. **Be comprehensive**: Think about different user roles and use cases

ITERATIVE REFINEMENT:
- Review previously identified information needs
- Add new information needs that were missed
- Remove information needs that are not realistic or important
- Continue until you're confident all important information needs are covered

For each information need, provide:
- description: Clear description of what information is needed
- frequency: How often this query will be run (frequent, occasional, rare)
- entities_involved: Which entities are needed to answer this query
- reasoning: REQUIRED - Why this information need is important (cannot be omitted)

Return a JSON object with:
- information_needs: Complete list of all information needs (including previous ones, with any modifications). Each information need MUST include a reasoning field.
- additions: List of newly added information need description strings only (e.g., ["Query for X", "Report on Y"]), NOT full objects
- deletions: List of information need description strings to remove (e.g., ["Query for Z"]), NOT full objects
- no_more_changes: True if you're confident no more additions or deletions are needed, False otherwise
- reasoning: REQUIRED - Explanation of your additions, deletions, and termination decision (cannot be omitted)"""
    
    # Human prompt template
    human_prompt_template = """Identify information needs for this database.

Natural Language Description:
{nl_description}

{context}

Return a JSON object specifying all information needs, any additions or deletions from previous iterations, and whether you're satisfied with the current list."""
    
    try:
        # Get model for this step (important task)
        llm = get_model_for_step("3.1")
        
        # Build schema_state for tools
        schema_state = {
            "entities": entities,
            "relations": relations,
            "attributes": attributes,
        }
        
        # Create bound version of verify_entities_exist with schema_state
        def verify_entities_exist_bound(entities: List[str]) -> Dict[str, Any]:
            """Bound version of verify_entities_exist with schema_state.
            
            Args:
                entities: List of entity names to verify. Must be a list of strings.
                         Example: ["Customer", "Order", "Book"]
            
            Returns:
                Dictionary mapping entity name to existence status (True/False)
            
            Purpose: Allows LLM to verify that entities mentioned in information needs exist in the schema.
            
            IMPORTANT: When calling this tool, provide arguments as a JSON object:
            {"entities": ["Customer", "Order"]}
            NOT as a list: ["entities"] ❌
            But as a dict: {"entities": ["Customer", "Order"]} ✅
            """
            # NOTE: verify_entities_exist is a LangChain @tool (StructuredTool) and is not callable like a function.
            # Use the pure implementation to avoid "'StructuredTool' object is not callable".
            return _verify_entities_exist_impl(entities, schema_state)
        
        # Create tools list
        tools = [verify_entities_exist_bound]
        
        # Invoke standardized LLM call with tools
        config = get_trace_config("3.1", phase=3, tags=["phase_3_step_1"])
        result: InformationNeedIdentificationOutput = await standardized_llm_call(
            llm=llm,
            output_schema=InformationNeedIdentificationOutput,
            system_prompt=system_prompt,
            human_prompt_template=human_prompt_template,
            input_data={
                "nl_description": nl_description,
                "context": context_msg,
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
        
        for info_need in result.information_needs:
            entities_involved = info_need.entities_involved
            for entity_name in entities_involved:
                if entity_name not in all_entity_names:
                    logger.warning(
                        f"Information need '{info_need.description}' references "
                        f"entity '{entity_name}' which does not exist in the schema"
                    )
        
        logger.info(
            f"Information need identification completed: {len(result.information_needs)} needs identified, "
            f"no_more_changes={result.no_more_changes}"
        )
        
        # Convert to dict only at return boundary
        return result.model_dump()
        
    except Exception as e:
        logger.error(f"Error in information need identification: {e}", exc_info=True)
        raise


async def step_3_1_information_need_identification_with_loop(
    nl_description: str,
    entities: List[Dict[str, Any]],
    relations: List[Dict[str, Any]],
    attributes: Dict[str, List[Dict[str, Any]]],
    primary_keys: Dict[str, List[str]],
    foreign_keys: List[Dict[str, Any]],
    domain: Optional[str] = None,
    example_queries: Optional[List[Dict[str, str]]] = None,
    max_iterations: int = 10,
    max_time_sec: int = 300,
) -> Dict[str, Any]:
    """
    Step 3.1 with automatic looping: continues until no_more_changes is True.
    
    This function implements the iterative loop specified in the plan: continues
    identifying information needs until the LLM suggests no further additions or deletions.
    
    Args:
        nl_description: Original natural language description
        entities: List of all entities with descriptions from Phase 1
        relations: List of all relations from Phase 1
        attributes: Dictionary mapping entity names to their attributes from Phase 2
        primary_keys: Dictionary mapping entity names to their primary keys from Phase 2
        foreign_keys: List of foreign key specifications from Phase 2
        domain: Optional domain context from Phase 1
        example_queries: Optional list of example queries as context
        max_iterations: Maximum number of loop iterations (default: 10)
        max_time_sec: Maximum wall time in seconds (default: 300)
        
    Returns:
        dict: Final information need identification result with loop metadata
        
    Example:
        >>> result = await step_3_1_information_need_identification_with_loop(
        ...     nl_description="E-commerce database",
        ...     entities=[{"name": "Customer"}],
        ...     relations=[],
        ...     attributes={"Customer": []},
        ...     primary_keys={"Customer": ["customer_id"]},
        ...     foreign_keys=[]
        ... )
        >>> result["final_result"]["no_more_changes"]
        True
    """
    from NL2DATA.utils.loops import SafeLoopExecutor, LoopConfig
    
    logger.info("Starting Step 3.1: Information Need Identification (with loop support)")
    
    previous_information_needs = None
    
    async def identification_step(previous_result=None):
        """Single iteration of information need identification."""
        nonlocal previous_information_needs
        
        # Extract previous information needs from last iteration
        if previous_result:
            previous_information_needs = previous_result.get("information_needs", [])
        
        result = await step_3_1_information_need_identification(
            nl_description=nl_description,
            entities=entities,
            relations=relations,
            attributes=attributes,
            primary_keys=primary_keys,
            foreign_keys=foreign_keys,
            domain=domain,
            example_queries=example_queries,
            previous_information_needs=previous_information_needs,
        )
        return result
    
    # Termination check: no_more_changes must be True
    def termination_check(result: Dict[str, Any]) -> bool:
        return result.get("no_more_changes", False)
    
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
        step_func=identification_step,
        termination_check=termination_check,
        config=loop_config,
    )
    
    final_result = loop_result["result"]
    iterations = loop_result["iterations"]
    terminated_by = loop_result["terminated_by"]
    
    logger.info(
        f"Information need identification loop completed: {iterations} iterations, "
        f"terminated by: {terminated_by}, {len(final_result.get('information_needs', []))} needs identified"
    )
    
    return {
        "final_result": final_result,
        "loop_metadata": {
            "iterations": iterations,
            "terminated_by": terminated_by,
        }
    }

