"""Phase 4, Step 4.1: Functional Dependency Analysis.

Identify logical dependencies between attributes (e.g., zipcode → city).
Needed for normalization, data generation constraints, and ensuring data consistency.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

from NL2DATA.phases.phase4.model_router import get_model_for_step
from NL2DATA.utils.llm import standardized_llm_call
from NL2DATA.utils.observability import traceable_step, get_trace_config
from NL2DATA.utils.logging import get_logger
from NL2DATA.phases.phase1.utils.data_extraction import (
    extract_attribute_name,
    extract_attribute_description,
)
from NL2DATA.utils.loops import SafeLoopExecutor, LoopConfig
from NL2DATA.utils.pipeline_config import get_phase4_config

logger = get_logger(__name__)

def _fd_is_table_local(fd: object, allowed: set[str]) -> bool:
    lhs = []
    rhs = []
    if isinstance(fd, dict):
        lhs = [a for a in (fd.get("lhs") or []) if isinstance(a, str) and a]
        rhs = [a for a in (fd.get("rhs") or []) if isinstance(a, str) and a]
    else:
        lhs = [a for a in (getattr(fd, "lhs", None) or []) if isinstance(a, str) and a]
        rhs = [a for a in (getattr(fd, "rhs", None) or []) if isinstance(a, str) and a]
    if not lhs or not rhs:
        return False
    return set(lhs).issubset(allowed) and set(rhs).issubset(allowed)


class FunctionalDependency(BaseModel):
    """Single functional dependency specification."""
    lhs: List[str] = Field(description="Left-hand side attributes (determinants)")
    rhs: List[str] = Field(description="Right-hand side attributes (dependent)")
    reasoning: str = Field(description="Reasoning for why this functional dependency exists - REQUIRED field")


class FunctionalDependencyAnalysisOutput(BaseModel):
    """Output structure for functional dependency analysis."""
    functional_dependencies: List[FunctionalDependency] = Field(
        description="List of all functional dependencies identified (including previous ones, with any modifications)"
    )
    should_add: List[FunctionalDependency] = Field(
        default_factory=list,
        description="List of newly added functional dependencies (for tracking changes)"
    )
    should_remove: List[FunctionalDependency] = Field(
        default_factory=list,
        description="List of functional dependencies that should be removed (for tracking changes)"
    )
    no_more_changes: bool = Field(
        description="Whether the LLM suggests no further additions or deletions (termination condition for loop)"
    )
    reasoning: Optional[str] = Field(
        default=None,
        description="Reasoning for the additions, removals, and termination decision"
    )


@traceable_step("4.1", phase=4, tags=['phase_4_step_1'])
async def step_4_1_functional_dependency_analysis_single(
    entity_name: str,
    entity_description: Optional[str],
    attributes: List[Dict[str, Any]],  # All attributes for this entity
    primary_key: List[str],  # Primary key from Step 2.7
    derived_attributes: Optional[Dict[str, str]] = None,  # derived_attr -> formula from Step 2.9
    relations: Optional[List[Dict[str, Any]]] = None,  # Relations involving this entity
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
    previous_dependencies: Optional[List[Dict[str, Any]]] = None,  # For loop iterations
) -> Dict[str, Any]:
    """
    Step 4.1 (per-entity): Identify functional dependencies for an entity.
    
    This is designed to be called in parallel for multiple entities, and iteratively
    for each entity until no_more_changes is True.
    
    Args:
        entity_name: Name of the entity
        entity_description: Optional description of the entity
        attributes: List of all attributes for this entity with descriptions
        primary_key: Primary key attributes from Step 2.7
        derived_attributes: Optional dictionary mapping derived attribute names to their formulas
        relations: Optional list of relations involving this entity
        nl_description: Optional original NL description
        domain: Optional domain context from Phase 1
        previous_dependencies: Optional list of previously identified functional dependencies (for loop)
        
    Returns:
        dict: Functional dependency analysis result with functional_dependencies, should_add, should_remove, no_more_changes, and reasoning
        
    Example:
        >>> result = await step_4_1_functional_dependency_analysis_single(
        ...     entity_name="Customer",
        ...     entity_description="A customer who places orders",
        ...     attributes=[{"name": "zipcode"}, {"name": "city"}],
        ...     primary_key=["customer_id"]
        ... )
        >>> len(result["functional_dependencies"]) > 0
        True
    """
    logger.debug(f"Analyzing functional dependencies for entity: {entity_name}")
    cfg = get_phase4_config()
    
    # Build comprehensive context
    context_parts = []
    if domain:
        context_parts.append(f"Domain: {domain}")
    if entity_description:
        context_parts.append(f"Entity description: {entity_description}")
    
    # Attributes summary
    attr_names = [extract_attribute_name(attr) for attr in attributes]
    attr_descriptions = []
    for attr in attributes:
        attr_name = extract_attribute_name(attr)
        attr_desc = extract_attribute_description(attr)
        if attr_desc:
            attr_descriptions.append(f"  - {attr_name}: {attr_desc}")
        else:
            attr_descriptions.append(f"  - {attr_name}")
    
    context_parts.append(f"Attributes ({len(attributes)}):\n" + "\n".join(attr_descriptions))
    
    # Primary key
    if primary_key:
        context_parts.append(f"Primary Key: {', '.join(primary_key)}")
        context_parts.append(
            f"Note: Primary key {', '.join(primary_key)} functionally determines all other attributes"
        )
    
    # Derived attributes
    if derived_attributes:
        derived_summary = []
        for attr_name, formula in derived_attributes.items():
            derived_summary.append(f"  - {attr_name} = {formula}")
        context_parts.append(f"Derived Attributes:\n" + "\n".join(derived_summary))
        context_parts.append(
            "Note: Derived attributes are functionally determined by their formula dependencies"
        )
    
    # Relations involving this entity
    if relations:
        rel_summary = []
        for rel in relations[:5]:  # Limit to avoid too long context
            rel_entities = rel.get("entities", [])
            rel_type = rel.get("type", "")
            rel_desc = rel.get("description", "")
            rel_info = f"  - {rel_type}: {', '.join(rel_entities)}"
            if rel_desc:
                rel_info += f" ({rel_desc})"
            rel_summary.append(rel_info)
        context_parts.append(f"Relations:\n" + "\n".join(rel_summary))
    
    # Previous dependencies (for loop iterations)
    if previous_dependencies:
        prev_summary = []
        for fd in previous_dependencies:
            lhs = fd.get("lhs", []) if isinstance(fd, dict) else getattr(fd, "lhs", [])
            rhs = fd.get("rhs", []) if isinstance(fd, dict) else getattr(fd, "rhs", [])
            if lhs and rhs:
                prev_summary.append(f"  - {', '.join(lhs)} → {', '.join(rhs)}")
        context_parts.append(f"Previously Identified Functional Dependencies ({len(previous_dependencies)}):\n" + "\n".join(prev_summary))
    
    context_msg = "\n\nContext:\n" + "\n".join(f"- {part}" for part in context_parts)
    
    # System prompt
    system_prompt = """You are a database normalization expert. Your task is to identify functional dependencies between attributes.

FUNCTIONAL DEPENDENCY ANALYSIS:
1. **Functional Dependency (FD)**: A → B means that if two tuples have the same value for A, they must have the same value for B.
2. **Common Patterns** (EXCLUDE OBVIOUS ONES):
   - **DO NOT include**: Primary key → all attributes (this is always true by definition and not useful for normalization)
   - **DO include**: Composite keys determining specific attributes (e.g., order_id, item_id → quantity, price)
   - **DO include**: Determinant attributes (e.g., zipcode → city, state; email → customer_id)
   - **DO include**: Derived attributes determined by their formula dependencies
3. **Consider**:
   - Business rules (e.g., zipcode determines city)
   - Data relationships (e.g., employee_id determines department_id)
   - Composite dependencies (multiple attributes determine others)
   - Transitive dependencies (A → B → C, where B → C is transitive)
   - **Exclude trivial dependencies**: PK → all attributes (always true, not useful)

IMPORTANT: Do NOT include functional dependencies where the left-hand side is the primary key (or contains the primary key) determining all other attributes. This is obvious and not useful for normalization analysis.

ITERATIVE REFINEMENT:
- Review previously identified functional dependencies
- Add new functional dependencies that were missed (excluding PK → all)
- Remove functional dependencies that are incorrect, redundant, or the obvious PK → all dependency
- Continue until you're confident all important functional dependencies are identified

For each functional dependency, you MUST provide:
- lhs: List of attributes that determine (left-hand side)
- rhs: List of attributes that are determined (right-hand side)
- reasoning: REQUIRED - A clear explanation of why this functional dependency exists (e.g., "zipcode determines city because postal codes are unique to geographic locations")

CRITICAL: Every functional dependency object in functional_dependencies, should_add, and should_remove MUST include a 'reasoning' field. This field cannot be omitted, left empty, or set to a placeholder value.

Return a JSON object with:
- functional_dependencies: Complete list of all functional dependencies (including previous ones, with any modifications, EXCLUDING PK → all). Each dependency MUST have a reasoning field.
- should_add: List of newly added functional dependencies. Each dependency MUST have a reasoning field.
- should_remove: List of functional dependencies to remove. Each dependency MUST have a reasoning field.
- no_more_changes: True if you're confident no more additions or removals are needed, False otherwise
- reasoning: Explanation of your additions, removals, and termination decision"""
    
    # Human prompt template
    human_prompt_template = """Identify functional dependencies for the entity: {entity_name}

{context}

{nl_section}

Return a JSON object specifying all functional dependencies, any additions or removals from previous iterations, and whether you're satisfied with the current list."""
    
    try:
        # Get model for this step (important task)
        llm = get_model_for_step("4.1")
        
        # Invoke standardized LLM call with validation loop for missing reasoning
        max_reasoning_retries = 2
        result: FunctionalDependencyAnalysisOutput = None
        current_system_prompt = system_prompt
        current_context_msg = context_msg
        
        for reasoning_attempt in range(max_reasoning_retries + 1):
            try:
                config = get_trace_config("4.1", phase=4, tags=["phase_4_step_1"])
                nl_section = ""
                if cfg.step_4_1_include_nl_context and (nl_description or "").strip():
                    nl_section = f"Natural Language Description:\n{nl_description}\n"
                else:
                    nl_section = "Natural Language Description: (omitted)\n"

                result = await standardized_llm_call(
                    llm=llm,
                    output_schema=FunctionalDependencyAnalysisOutput,
                    system_prompt=current_system_prompt,
                    human_prompt_template=human_prompt_template,
                    input_data={
                        "entity_name": entity_name,
                        "context": current_context_msg,
                        "nl_section": nl_section,
                    },
                    config=config,
                )
                
                # Work with Pydantic model directly
                # Check if any functional dependencies are missing reasoning
                missing_reasoning = []
                for fd in result.functional_dependencies:
                    if not fd.reasoning or fd.reasoning.strip() == "":
                        missing_reasoning.append(("functional_dependencies", fd))
                for fd in result.should_add:
                    if not fd.reasoning or fd.reasoning.strip() == "":
                        missing_reasoning.append(("should_add", fd))
                for fd in result.should_remove:
                    if not fd.reasoning or fd.reasoning.strip() == "":
                        missing_reasoning.append(("should_remove", fd))
                
                # If no missing reasoning, break out of retry loop
                if not missing_reasoning:
                    break
                
                # If this is not the last attempt, enhance the prompt
                if reasoning_attempt < max_reasoning_retries:
                    missing_fd_strs = []
                    for fd_list_name, fd in missing_reasoning[:3]:  # Limit to first 3 for brevity
                        lhs = fd.lhs
                        rhs = fd.rhs
                        missing_fd_strs.append(f"{', '.join(lhs)} -> {', '.join(rhs)}")
                    
                    logger.warning(
                        f"Entity {entity_name}: {len(missing_reasoning)} functional dependencies missing reasoning. "
                        f"Retrying with explicit reasoning requirement (attempt {reasoning_attempt + 1}/{max_reasoning_retries})"
                    )
                    
                    # Enhance the system prompt to be more explicit about reasoning requirement
                    current_system_prompt = system_prompt + (
                        "\n\nCRITICAL REQUIREMENT: Every functional dependency object MUST include a 'reasoning' field. "
                        "This field is REQUIRED and cannot be omitted or left empty. For each functional dependency in "
                        "functional_dependencies, should_add, and should_remove, you MUST provide a clear, specific explanation "
                        "of why that functional dependency exists. Do not return any functional dependency without a reasoning field."
                    )
                    
                    # Add feedback to context for next attempt
                    feedback_msg = (
                        f"\n\nIMPORTANT: The previous response was missing 'reasoning' fields for some functional dependencies. "
                        f"Please ensure EVERY functional dependency includes a 'reasoning' field. "
                        f"Examples of missing reasoning: {'; '.join(missing_fd_strs)}"
                    )
                    current_context_msg = context_msg + feedback_msg
                else:
                    # Last attempt failed, log warning but continue with defaults
                    logger.warning(
                        f"Entity {entity_name}: {len(missing_reasoning)} functional dependencies still missing reasoning "
                        f"after {max_reasoning_retries} retries. Adding default reasoning."
                    )
                    # Add default reasoning to missing entries - need to create new model instances
                    from copy import deepcopy
                    result_dict = result.model_dump()
                    for fd_list_name, fd in missing_reasoning:
                        lhs_str = ', '.join(fd.lhs)
                        rhs_str = ', '.join(fd.rhs)
                        fd_dict = fd.model_dump()
                        fd_dict["reasoning"] = f"Functional dependency where {lhs_str} determines {rhs_str}"
                        # Update the appropriate list in result_dict
                        if fd_list_name == "functional_dependencies":
                            idx = result.functional_dependencies.index(fd)
                            result_dict["functional_dependencies"][idx] = fd_dict
                        elif fd_list_name == "should_add":
                            idx = result.should_add.index(fd)
                            result_dict["should_add"][idx] = fd_dict
                        elif fd_list_name == "should_remove":
                            idx = result.should_remove.index(fd)
                            result_dict["should_remove"][idx] = fd_dict
                    result = FunctionalDependencyAnalysisOutput(**result_dict)
                    break
                    
            except Exception as e:
                # If it's a parsing error about missing reasoning, try again with enhanced prompt
                error_str = str(e).replace('\u2192', '->')  # Replace Unicode arrows
                if "reasoning" in error_str.lower() and ("required" in error_str.lower() or "missing" in error_str.lower()):
                    if reasoning_attempt < max_reasoning_retries:
                        logger.warning(
                            f"Entity {entity_name}: Parsing error due to missing reasoning fields. "
                            f"Retrying with explicit reasoning request (attempt {reasoning_attempt + 1}/{max_reasoning_retries})"
                        )
                        # Enhance the prompt to be more explicit about reasoning requirement
                        current_system_prompt = system_prompt + (
                            "\n\nCRITICAL: Every functional dependency object MUST include a 'reasoning' field. "
                            "This field is REQUIRED and cannot be omitted. For each functional dependency, "
                            "provide a clear explanation of why this dependency exists."
                        )
                        continue
                # For other errors, re-raise
                raise
        
        if result is None:
            raise RuntimeError(f"Failed to get valid result for entity {entity_name} after {max_reasoning_retries} reasoning retries")
        
        # Work with Pydantic model directly
        # Validate that mentioned attributes exist and filter out obvious PK → all dependencies
        all_attr_names = set([a for a in attr_names if isinstance(a, str) and a])
        pk_set = set(primary_key)
        
        # Filter out functional dependencies where LHS is PK (or contains PK) and RHS is all other attributes
        filtered_fds: List[FunctionalDependency] = []
        for fd in result.functional_dependencies:
            lhs = fd.lhs
            rhs = fd.rhs
            lhs_set = set(lhs)
            rhs_set = set(rhs)
            
            # Check if this is the obvious PK → all dependency
            # PK → all means: LHS contains PK (or is PK) AND RHS contains all non-PK attributes
            is_pk_superkey = pk_set.issubset(lhs_set) or lhs_set == pk_set
            non_pk_attrs = all_attr_names - pk_set
            is_all_non_pk = non_pk_attrs.issubset(rhs_set) and len(rhs_set) >= len(non_pk_attrs)
            
            if is_pk_superkey and is_all_non_pk:
                logger.debug(
                    f"Entity {entity_name}: Filtering out obvious PK → all dependency: "
                    f"{', '.join(lhs)} → {', '.join(rhs)}"
                )
                continue  # Skip this obvious dependency
            
            # Validate attributes exist; drop non-local dependencies (prevents cross-entity leakage)
            if not _fd_is_table_local(fd, all_attr_names):
                for attr in lhs:
                    if attr not in all_attr_names:
                        logger.warning(
                            f"Entity {entity_name}: Functional dependency LHS attribute '{attr}' does not exist in attribute list"
                        )
                for attr in rhs:
                    if attr not in all_attr_names:
                        logger.warning(
                            f"Entity {entity_name}: Functional dependency RHS attribute '{attr}' does not exist in attribute list"
                        )
                continue

            filtered_fds.append(fd)
        
        # Also filter should_add and should_remove
        filtered_should_add: List[FunctionalDependency] = []
        for fd in result.should_add:
            lhs = fd.lhs
            rhs = fd.rhs
            lhs_set = set(lhs)
            rhs_set = set(rhs)
            
            is_pk_superkey = pk_set.issubset(lhs_set) or lhs_set == pk_set
            non_pk_attrs = all_attr_names - pk_set
            is_all_non_pk = non_pk_attrs.issubset(rhs_set) and len(rhs_set) >= len(non_pk_attrs)
            
            if is_pk_superkey and is_all_non_pk:
                continue
            # Drop any non-local "global suggestions" so the loop won't thrash
            if not _fd_is_table_local(fd, all_attr_names):
                continue
            filtered_should_add.append(fd)

        filtered_should_remove: List[FunctionalDependency] = []
        for fd in result.should_remove:
            if _fd_is_table_local(fd, all_attr_names):
                filtered_should_remove.append(fd)
        
        # Create new model instance if modifications were made
        no_more_changes = result.no_more_changes
        if not filtered_should_add and not filtered_should_remove:
            # If the only "changes" were invalid/cross-entity, stop looping.
            no_more_changes = True

        if (
            filtered_fds != result.functional_dependencies
            or filtered_should_add != result.should_add
            or filtered_should_remove != result.should_remove
            or no_more_changes != result.no_more_changes
        ):
            result = FunctionalDependencyAnalysisOutput(
                functional_dependencies=filtered_fds,
                should_add=filtered_should_add,
                should_remove=filtered_should_remove,
                no_more_changes=no_more_changes,
                reasoning=result.reasoning  # Preserve reasoning if it exists
            )
        
        logger.debug(
            f"Functional dependency analysis for {entity_name}: "
            f"{len(result.functional_dependencies)} dependencies identified (after filtering PK → all), "
            f"no_more_changes={result.no_more_changes}"
        )
        
        # Convert to dict only at return boundary
        return result.model_dump()
        
    except Exception as e:
        logger.error(f"Error in functional dependency analysis for {entity_name}: {e}", exc_info=True)
        raise


async def step_4_1_functional_dependency_analysis_single_with_loop(
    entity_name: str,
    entity_description: Optional[str],
    attributes: List[Dict[str, Any]],
    primary_key: List[str],
    derived_attributes: Optional[Dict[str, str]] = None,
    relations: Optional[List[Dict[str, Any]]] = None,
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
    max_iterations: int = 5,
    max_time_sec: int = 180,
) -> Dict[str, Any]:
    """
    Step 4.1 with automatic looping: continues until no_more_changes is True.
    
    This function implements the iterative loop specified in the plan: continues
    identifying functional dependencies until the LLM suggests no further additions or removals.
    
    Args:
        entity_name: Name of the entity
        entity_description: Optional description of the entity
        attributes: List of all attributes for this entity with descriptions
        primary_key: Primary key attributes from Step 2.7
        derived_attributes: Optional dictionary mapping derived attribute names to their formulas
        relations: Optional list of relations involving this entity
        nl_description: Optional original NL description
        domain: Optional domain context from Phase 1
        max_iterations: Maximum number of loop iterations per entity (default: 5)
        max_time_sec: Maximum wall time in seconds per entity (default: 180)
        
    Returns:
        dict: Final functional dependency analysis result with loop metadata
        
    Example:
        >>> result = await step_4_1_functional_dependency_analysis_single_with_loop(
        ...     entity_name="Customer",
        ...     entity_description="A customer",
        ...     attributes=[{"name": "zipcode"}, {"name": "city"}],
        ...     primary_key=["customer_id"]
        ... )
        >>> result["final_result"]["no_more_changes"]
        True
    """
    from NL2DATA.utils.loops import SafeLoopExecutor, LoopConfig
    
    logger.debug(f"Starting functional dependency analysis loop for: {entity_name}")
    
    previous_dependencies = None
    
    async def fd_analysis_step(previous_result=None):
        """Single iteration of functional dependency analysis."""
        nonlocal previous_dependencies
        
        if previous_result:
            previous_dependencies = previous_result.get("functional_dependencies", [])
        
        result = await step_4_1_functional_dependency_analysis_single(
            entity_name=entity_name,
            entity_description=entity_description,
            attributes=attributes,
            primary_key=primary_key,
            derived_attributes=derived_attributes,
            relations=relations,
            nl_description=nl_description,
            domain=domain,
            previous_dependencies=previous_dependencies,
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
        step_func=fd_analysis_step,
        termination_check=termination_check,
        config=loop_config,
    )
    
    final_result = loop_result["result"]
    iterations = loop_result["iterations"]
    terminated_by = loop_result["terminated_by"]
    
    logger.info(
        f"Functional dependency analysis loop for {entity_name} completed: {iterations} iterations, "
        f"terminated by: {terminated_by}, {len(final_result.get('functional_dependencies', []))} dependencies identified"
    )
    
    return {
        "final_result": final_result,
        "loop_metadata": {
            "iterations": iterations,
            "terminated_by": terminated_by,
        }
    }


async def step_4_1_functional_dependency_analysis_batch(
    entities: List[Dict[str, Any]],  # All entities with descriptions
    entity_attributes: Dict[str, List[Dict[str, Any]]],  # entity -> attributes
    entity_primary_keys: Dict[str, List[str]],  # entity -> primary key
    relational_schema: Optional[Dict[str, Any]] = None,  # Relational schema from Step 3.5 (preferred source for FD mining)
    entity_derived_attributes: Optional[Dict[str, Dict[str, str]]] = None,  # entity -> derived_attr -> formula
    entity_relations: Optional[Dict[str, List[Dict[str, Any]]]] = None,  # entity -> relations
    nl_description: Optional[str] = None,
    domain: Optional[str] = None,
    max_iterations: int = 5,
    max_time_sec: int = 180,
) -> Dict[str, Any]:
    """
    Step 4.1: Analyze functional dependencies for all tables/entities (parallel execution).
    
    Each table/entity is analyzed in parallel, and each analysis loops until no_more_changes.
    
    IMPORTANT ORDERING / SOURCE OF TRUTH:
    - Functional dependencies should ideally be mined AFTER ER -> relational schema compilation (Step 3.5),
      because normalization (Step 4.2) operates on relational tables (including FKs, junction tables, and inferred PKs).
    - If `relational_schema` is provided, we mine FDs using its tables/columns as the attribute universe.
    - Otherwise we fall back to Phase-2 entity attributes + entity_primary_keys.
    
    Args:
        entities: List of all entities with descriptions from Phase 1 (used for descriptions/context)
        entity_attributes: Dictionary mapping entity names to their attributes (fallback source if relational_schema not provided)
        entity_primary_keys: Dictionary mapping entity names to their primary keys (fallback source if relational_schema not provided)
        relational_schema: Optional relational schema from Step 3.5 (preferred FD mining source)
        entity_derived_attributes: Optional dictionary mapping entity names to their derived attributes (attr -> formula)
        entity_relations: Optional dictionary mapping entity names to relations involving them
        nl_description: Optional original NL description
        domain: Optional domain context from Phase 1
        max_iterations: Maximum number of loop iterations per entity (default: 5)
        max_time_sec: Maximum wall time in seconds per entity (default: 180)
        
    Returns:
        dict: Functional dependency analysis results for all entities
        
    Example:
        >>> result = await step_4_1_functional_dependency_analysis_batch(
        ...     entities=[{"name": "Customer"}],
        ...     entity_attributes={"Customer": [{"name": "zipcode"}]},
        ...     entity_primary_keys={"Customer": ["customer_id"]}
        ... )
        >>> len(result["entity_results"]) > 0
        True
    """
    if relational_schema and relational_schema.get("tables"):
        logger.info(
            f"Starting Step 4.1: Functional Dependency Analysis for {len(relational_schema.get('tables', []))} tables "
            f"(using relational schema as source of truth)"
        )
    else:
        logger.info(f"Starting Step 4.1: Functional Dependency Analysis for {len(entities)} entities")
    
    if not entities and not (relational_schema and relational_schema.get("tables")):
        logger.warning("No entities or relational schema tables provided for functional dependency analysis")
        return {"entity_results": {}}
    
    # Execute in parallel for all entities
    import asyncio
    
    tasks = []

    # Map descriptions for context (prefer Phase-1 entities)
    entity_desc_map: Dict[str, str] = {}
    for e in entities or []:
        name = e.get("name") if isinstance(e, dict) else getattr(e, "name", "")
        desc = e.get("description", "") if isinstance(e, dict) else getattr(e, "description", "")
        if name:
            entity_desc_map[name] = desc or ""

    if relational_schema and relational_schema.get("tables"):
        # Mine FDs at the TABLE level using table columns + PKs.
        # This aligns with Step 4.2 normalization (which operates on tables).
        for table in relational_schema.get("tables", []):
            table_name = table.get("name", "")
            if not table_name:
                continue

            # Skip tables that are structurally already in 3NF by construction
            if table.get("is_junction_table") or table.get("is_multivalued_table"):
                async def _no_op(table_name_local: str = table_name) -> Dict[str, Any]:
                    return {
                        "final_result": {
                            "functional_dependencies": [],
                            "should_add": [],
                            "should_remove": [],
                            "no_more_changes": True,
                            "reasoning": "Skipped FD mining for junction/multivalued table (structural table).",
                        },
                        "loop_metadata": {"iterations": 0, "terminated_by": "skipped"},
                    }
                tasks.append((table_name, _no_op()))
                continue

            columns = table.get("columns", []) or []
            attributes = [
                {
                    "name": c.get("name", ""),
                    "description": c.get("description", ""),
                    "type_hint": c.get("type_hint"),
                    "is_primary_key": c.get("is_primary_key", False),
                    "is_foreign_key": c.get("is_foreign_key", False),
                }
                for c in columns
                if isinstance(c, dict) and c.get("name")
            ]
            primary_key = table.get("primary_key", []) or []
            derived_attrs = entity_derived_attributes.get(table_name, {}) if entity_derived_attributes else None
            relations = entity_relations.get(table_name, []) if entity_relations else None
            entity_desc = entity_desc_map.get(table_name, "")

            task = step_4_1_functional_dependency_analysis_single_with_loop(
                entity_name=table_name,
                entity_description=entity_desc,
                attributes=attributes,
                primary_key=primary_key,
                derived_attributes=derived_attrs,
                relations=relations,
                nl_description=nl_description,
                domain=domain,
                max_iterations=max_iterations,
                max_time_sec=max_time_sec,
            )
            tasks.append((table_name, task))
    else:
        # Backward-compatible fallback: mine FDs on Phase-2 entities/attributes.
        for entity in entities:
            entity_name = entity.get("name") if isinstance(entity, dict) else getattr(entity, "name", "")
            entity_desc = entity.get("description", "") if isinstance(entity, dict) else getattr(entity, "description", "")
            attributes = entity_attributes.get(entity_name, [])
            primary_key = entity_primary_keys.get(entity_name, [])
            derived_attrs = entity_derived_attributes.get(entity_name, {}) if entity_derived_attributes else None
            relations = entity_relations.get(entity_name, []) if entity_relations else None
            
            task = step_4_1_functional_dependency_analysis_single_with_loop(
                entity_name=entity_name,
                entity_description=entity_desc,
                attributes=attributes,
                primary_key=primary_key,
                derived_attributes=derived_attrs,
                relations=relations,
                nl_description=nl_description,
                domain=domain,
                max_iterations=max_iterations,
                max_time_sec=max_time_sec,
            )
            tasks.append((entity_name, task))
    
    # Wait for all tasks to complete
    results = await asyncio.gather(
        *[task for _, task in tasks],
        return_exceptions=True
    )
    
    # Process results
    entity_results = {}
    for i, ((entity_name, _), result) in enumerate(zip(tasks, results)):
        if isinstance(result, Exception):
            # Replace Unicode arrows with ASCII for Windows console compatibility
            error_msg = str(result).replace('\u2192', '->')
            logger.error(f"Error processing entity {entity_name}: {error_msg}")
            entity_results[entity_name] = {
                "functional_dependencies": [],
                "should_add": [],
                "should_remove": [],
                "no_more_changes": False,
                "reasoning": f"Error during analysis: {error_msg}"
            }
        else:
            entity_results[entity_name] = result.get("final_result", {})
    
    total_dependencies = sum(
        len(r.get("functional_dependencies", []))
        for r in entity_results.values()
    )
    logger.info(
        f"Functional dependency analysis completed: {total_dependencies} total dependencies identified "
        f"across {len(entity_results)} entities"
    )
    
    return {"entity_results": entity_results}

