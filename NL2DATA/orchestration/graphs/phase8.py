"""Phase 8: Functional Dependencies & Constraints Graph.

This phase handles functional dependency analysis and constraint detection/compilation.

Execution order:
1. Step 8.1: Functional Dependency Analysis
2. Step 8.2: Categorical Column Identification
3. Step 8.3: Categorical Value Identification
4. Step 8.4: Constraint Detection
5. Step 8.5: Constraint Scope Analysis
6. Step 8.6: Constraint Enforcement Strategy
7. Step 8.7: Constraint Conflict Detection
8. Step 8.8: Constraint Compilation
"""

from typing import Dict, Any

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from ..state import IRGenerationState
from .common import logger, invoke_step_checked


def create_phase_8_graph() -> StateGraph:
    """Create LangGraph StateGraph for Phase 8 (Functional Dependencies & Constraints)."""
    from NL2DATA.phases.phase8 import (
        step_8_1_functional_dependency_analysis_batch,
        step_8_2_categorical_column_identification_batch,
        step_8_3_categorical_value_identification_batch,
        step_8_4_constraint_detection_with_loop,
        step_8_5_constraint_scope_analysis_batch,
        step_8_6_constraint_enforcement_strategy_batch,
        step_8_7_constraint_conflict_detection,
        step_8_8_constraint_compilation,
    )
    
    workflow = StateGraph(IRGenerationState)
    
    async def functional_dependencies(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 8.1: Functional Dependency Analysis")
        
        # Get entities and attributes from state
        entities = state.get("entities", [])
        attributes = state.get("attributes", {})
        primary_keys = state.get("primary_keys", {})
        relational_schema = state.get("metadata", {}).get("relational_schema", {})
        
        # Convert attributes dict to entity -> attribute list
        entity_attributes = {}
        for entity_name, attrs in attributes.items():
            entity_attributes[entity_name] = attrs
        
        result = await invoke_step_checked(
            step_8_1_functional_dependency_analysis_batch,
            entities=entities,
            entity_attributes=entity_attributes,
            entity_primary_keys=primary_keys,
            relational_schema=relational_schema,
            entity_derived_attributes=None,  # Derived attributes are in derived_formulas
            entity_relations=None,  # Can extract from relations if needed
            nl_description=state.get("nl_description"),
            domain=state.get("domain"),
        )
        
        # Extract functional dependencies (batch function returns both entity_results and top-level list)
        # Handle both Pydantic model and dict formats
        if hasattr(result, 'functional_dependencies'):
            all_fds = result.functional_dependencies
        elif isinstance(result, dict):
            all_fds = result.get("functional_dependencies", [])
        else:
            all_fds = []
        
        if not all_fds:
            # Fallback: extract from entity_results if top-level list is empty
            if hasattr(result, 'entity_results'):
                entity_results = result.entity_results
            elif isinstance(result, dict):
                entity_results = result.get("entity_results", {})
            else:
                entity_results = {}
            
            for entity_name, fd_result in entity_results.items():
                if hasattr(fd_result, 'functional_dependencies'):
                    fds = fd_result.functional_dependencies
                elif isinstance(fd_result, dict):
                    fds = fd_result.get("functional_dependencies", [])
                else:
                    fds = []
                all_fds.extend(fds)
        
        return {
            "functional_dependencies": all_fds,
            "previous_answers": {**state.get("previous_answers", {}), "8.1": result},
            "current_step": "8.1",
            "phase": 8,
        }
    
    # Step 8.2: Categorical Column Identification
    async def categorical_identification(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 8.2: Categorical Column Identification")
        entities = state.get("entities", [])
        attributes = state.get("attributes", {})
        data_types = state.get("data_types", {})
        relational_schema = state.get("metadata", {}).get("relational_schema", {})
        derived_formulas = state.get("derived_formulas", {})
        multivalued_derived = state.get("multivalued_derived", {})
        
        result = await invoke_step_checked(
            step_8_2_categorical_column_identification_batch,
            entities=entities,
            entity_attributes=attributes,
            entity_attribute_types=data_types,
            relational_schema=relational_schema,
            nl_description=state.get("nl_description", ""),
            domain=state.get("domain"),
            derived_formulas=derived_formulas if derived_formulas else None,
            multivalued_derived=multivalued_derived if multivalued_derived else None,
        )
        
        # Extract categorical attributes from result
        # result is CategoricalIdentificationOutput (Pydantic model)
        categorical_attributes = {}
        if hasattr(result, 'entity_results'):
            entity_results = result.entity_results
        elif isinstance(result, dict):
            entity_results = result.get("entity_results", {})
        else:
            entity_results = {}
        
        for entity_name, cat_result in entity_results.items():
            if hasattr(cat_result, 'categorical_attributes'):
                categorical_attributes[entity_name] = cat_result.categorical_attributes
            elif isinstance(cat_result, dict):
                categorical_attributes[entity_name] = cat_result.get("categorical_attributes", [])
            else:
                categorical_attributes[entity_name] = []
        
        return {
            "categorical_attributes": categorical_attributes,
            "previous_answers": {**state.get("previous_answers", {}), "8.2": result},
            "current_step": "8.2",
        }
    
    # Step 8.3: Categorical Value Identification (after 8.2)
    async def categorical_value_identification(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 8.3: Categorical Value Identification")
        
        # Get categorical attributes from step 8.2 result (which should have run before this)
        categorical_attributes = state.get("categorical_attributes", {})
        
        if not categorical_attributes:
            logger.warning("No categorical attributes found in state. Step 8.2 may not have run yet. Proceeding with empty categorical values.")
            return {
                "categorical_values": {},
                "previous_answers": {**state.get("previous_answers", {}), "8.3": {}},
                "current_step": "8.3",
            }
        
        logger.info(f"Processing categorical value identification for {len(categorical_attributes)} entities with categorical attributes")
        
        # Get other required data
        attributes = state.get("attributes", {})
        data_types = state.get("data_types", {})
        entities = state.get("entities", [])
        
        # Build entity descriptions dict
        entity_descriptions = {}
        for entity in entities:
            entity_name = entity.get("name", "") if isinstance(entity, dict) else getattr(entity, "name", "")
            entity_desc = entity.get("description", "") if isinstance(entity, dict) else getattr(entity, "description", "")
            if entity_desc:
                entity_descriptions[entity_name] = entity_desc
        
        result = await invoke_step_checked(
            step_8_3_categorical_value_identification_batch,
            categorical_attributes=categorical_attributes,
            entity_attributes=attributes,
            data_types=data_types,
            entity_descriptions=entity_descriptions,
            domain=state.get("domain"),
            nl_description=state.get("nl_description", ""),
        )
        
        return {
            "categorical_values": result,
            "previous_answers": {**state.get("previous_answers", {}), "8.3": result},
            "current_step": "8.3",
        }
    
    # Constraint detection steps (8.4-8.8)
    async def constraint_detection(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 8.4: Constraint Detection")
        # Get categorical values from step 8.3 (should have run before this)
        categorical_values = state.get("categorical_values", {})
        if not categorical_values:
            logger.debug("No categorical values found in state. Step 8.3 may have returned empty results or not run.")
        derived_formulas = state.get("derived_formulas", {})
        multivalued_derived = state.get("multivalued_derived", {})
        functional_dependencies = state.get("functional_dependencies", [])
        
        # Import sanitization function
        from NL2DATA.phases.phase8.step_8_4_constraint_detection import _sanitize_for_json
        
        # Sanitize normalized_schema at the graph level to ensure no type objects
        raw_schema = state.get("metadata", {}).get("relational_schema", {})
        sanitized_schema = _sanitize_for_json(raw_schema)
        
        result = await invoke_step_checked(
            step_8_4_constraint_detection_with_loop,
            nl_description=state.get("nl_description", ""),
            normalized_schema=sanitized_schema,
            categorical_values=categorical_values if categorical_values else None,
            functional_dependencies=functional_dependencies if functional_dependencies else None,
            derived_formulas=derived_formulas if derived_formulas else None,
            multivalued_derived=multivalued_derived if multivalued_derived else None,
        )
        return {
            "constraints": result.get("constraints", []),
            "previous_answers": {**state.get("previous_answers", {}), "8.4": result},
            "current_step": "8.4",
        }
    
    async def constraint_scope(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 8.5: Constraint Scope Analysis")
        constraints = state.get("constraints", [])
        functional_dependencies = state.get("functional_dependencies", [])
        scope_results = await invoke_step_checked(
            step_8_5_constraint_scope_analysis_batch,
            constraints=constraints,
            normalized_schema=state.get("metadata", {}).get("relational_schema", {}),
            functional_dependencies=functional_dependencies if functional_dependencies else None,
        )
        # Merge scope analysis results with original constraints
        constraints_with_scope = []
        for constraint, scope_result in zip(constraints, scope_results):
            # Convert Pydantic model to dict if needed
            if hasattr(scope_result, 'model_dump'):
                scope_dict = scope_result.model_dump()
            elif hasattr(scope_result, 'dict'):
                scope_dict = scope_result.dict()
            else:
                scope_dict = scope_result if isinstance(scope_result, dict) else {}
            merged = {**constraint, **scope_dict}
            constraints_with_scope.append(merged)
        return {
            "constraints": constraints_with_scope,
            "previous_answers": {
                **state.get("previous_answers", {}),
                "8.5": constraints_with_scope
            },
            "current_step": "8.5",
        }
    
    async def constraint_enforcement(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 8.6: Constraint Enforcement Strategy")
        prev_answers = state.get("previous_answers", {})
        constraints_with_scope = prev_answers.get("8.5", [])
        if not constraints_with_scope:
            # Fallback to raw constraints if scope analysis didn't return list
            constraints = state.get("constraints", [])
            constraints_with_scope = constraints
        functional_dependencies = state.get("functional_dependencies", [])
        result = await invoke_step_checked(
            step_8_6_constraint_enforcement_strategy_batch,
            constraints_with_scope=constraints_with_scope,
            normalized_schema=state.get("metadata", {}).get("relational_schema", {}),
            functional_dependencies=functional_dependencies if functional_dependencies else None,
        )
        
        # Merge enforcement data back into constraints
        # result is a list of ConstraintWithEnforcement objects
        constraints_with_enforcement = []
        if result:
            for enforcement_result in result:
                if hasattr(enforcement_result, 'model_dump'):
                    enforcement_dict = enforcement_result.model_dump()
                elif hasattr(enforcement_result, 'dict'):
                    enforcement_dict = enforcement_result.dict()
                else:
                    enforcement_dict = enforcement_result if isinstance(enforcement_result, dict) else {}
                
                # Extract constraint_data and merge with enforcement fields
                constraint_data = enforcement_dict.get("constraint_data", {})
                merged_constraint = {
                    **constraint_data,
                    "enforcement_strategy": enforcement_dict.get("enforcement_strategy"),
                    "enforcement_level": enforcement_dict.get("enforcement_level"),
                    "column_dsl_expressions": enforcement_dict.get("column_dsl_expressions", {}),
                    "enforcement_reasoning": enforcement_dict.get("reasoning"),
                }
                constraints_with_enforcement.append(merged_constraint)
        
        return {
            "constraints": constraints_with_enforcement if constraints_with_enforcement else state.get("constraints", []),
            "previous_answers": {**state.get("previous_answers", {}), "8.6": result},
            "current_step": "8.6",
        }
    
    async def constraint_conflict(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 8.7: Constraint Conflict Detection")
        constraints = state.get("constraints", [])
        categorical_values = state.get("categorical_values", {})
        derived_formulas = state.get("derived_formulas", {})
        multivalued_derived = state.get("multivalued_derived", {})
        result = await invoke_step_checked(
            step_8_7_constraint_conflict_detection,
            constraints=constraints,
            categorical_values=categorical_values if categorical_values else None,
            derived_formulas=derived_formulas if derived_formulas else None,
            multivalued_derived=multivalued_derived if multivalued_derived else None,
        )
        
        # If conflicts were resolved, update constraints with resolved version
        resolved_constraints = None
        if hasattr(result, 'resolved_constraints'):
            resolved_constraints = result.resolved_constraints
        elif isinstance(result, dict):
            resolved_constraints = result.get("resolved_constraints")
        
        resolution_applied = False
        if hasattr(result, 'resolution_applied'):
            resolution_applied = result.resolution_applied
        elif isinstance(result, dict):
            resolution_applied = result.get("resolution_applied", False)
        
        # Build update dict
        update_dict = {
            "previous_answers": {**state.get("previous_answers", {}), "8.7": result},
            "current_step": "8.7",
        }
        
        # Update constraints if resolution was applied
        if resolution_applied and resolved_constraints:
            logger.info(f"Updating constraints with resolved version: {len(resolved_constraints)} constraints (from {len(constraints)} original)")
            update_dict["constraints"] = resolved_constraints
        elif resolution_applied:
            logger.warning("Resolution was applied but no resolved constraints returned, keeping original constraints")
        
        return update_dict
    
    async def constraint_compilation(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 8.8: Constraint Compilation")
        constraints = state.get("constraints", [])
        result = await invoke_step_checked(step_8_8_constraint_compilation, constraints=constraints)
        
        # Store compiled constraints in metadata for filtering in generation strategies
        return {
            "previous_answers": {**state.get("previous_answers", {}), "8.8": result},
            "metadata": {
                **state.get("metadata", {}),
                "compiled_constraints": result,
            },
            "current_step": "8.8",
        }
    
    workflow.add_node("functional_dependencies", functional_dependencies)
    workflow.add_node("categorical_identification", categorical_identification)
    workflow.add_node("categorical_value_identification", categorical_value_identification)
    workflow.add_node("constraint_detection", constraint_detection)
    workflow.add_node("constraint_scope", constraint_scope)
    workflow.add_node("constraint_enforcement", constraint_enforcement)
    workflow.add_node("constraint_conflict", constraint_conflict)
    workflow.add_node("constraint_compilation", constraint_compilation)
    
    workflow.set_entry_point("functional_dependencies")
    workflow.add_edge("functional_dependencies", "categorical_identification")
    workflow.add_edge("categorical_identification", "categorical_value_identification")
    workflow.add_edge("categorical_value_identification", "constraint_detection")
    workflow.add_edge("constraint_detection", "constraint_scope")
    workflow.add_edge("constraint_scope", "constraint_enforcement")
    workflow.add_edge("constraint_enforcement", "constraint_conflict")
    workflow.add_edge("constraint_conflict", "constraint_compilation")
    workflow.add_edge("constraint_compilation", END)
    
    checkpointer = MemorySaver()
    return workflow.compile(checkpointer=checkpointer)
