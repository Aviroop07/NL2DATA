"""Phase 5: Attribute Dependency Graph and Data Type Assignment.

This phase:
1. Builds the attribute dependency graph (Step 5.1)
2. Assigns types to independent attributes (Step 5.2)
3. Derives FK types from PK types (Step 5.3)
4. Assigns types to dependent attributes (Step 5.4)
"""

from typing import Dict, Any

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from ..state import IRGenerationState
from .common import logger, invoke_step_checked


def _wrap_step_5_1(step_func):
    """Wrap Step 5.1 to work as LangGraph node."""

    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 5.1: Attribute Dependency Graph Construction")
        prev_answers = state.get("previous_answers", {})
        
        # Extract relation_cardinalities from state (may be nested)
        relation_cardinalities_raw = state.get("relation_cardinalities", {})
        relation_cardinalities: Dict[str, Dict[str, str]] = {}
        if relation_cardinalities_raw:
            for rel_id, payload in relation_cardinalities_raw.items():
                if isinstance(payload, dict):
                    entity_cards = payload.get("entity_cardinalities", {})
                    if isinstance(entity_cards, dict):
                        relation_cardinalities[str(rel_id)] = {str(k): str(v) for k, v in entity_cards.items() if k and v}
        
        result = await invoke_step_checked(
            step_func,
            entities=state.get("entities", []),
            attributes=state.get("attributes", {}),
            primary_keys=state.get("primary_keys", {}),
            relations=state.get("relations", []),
            relation_cardinalities=relation_cardinalities,
            foreign_keys=state.get("foreign_keys"),  # Optional - will be created from relations if not provided
            # Derived formulas are defined later in the plan; this will usually be empty here.
            derived_formulas=state.get("derived_formulas", {}),
        )
        
        # Work directly with Pydantic model (AttributeDependencyGraphOutput)
        # Convert to dict only for state storage (backward compatibility)
        if hasattr(result, 'dependency_graph'):
            # It's AttributeDependencyGraphOutput - work with Pydantic model directly
            # Convert fk_dependencies list to dict for backward compatibility with state
            fk_deps_dict = {
                dep.attribute_key: {
                    "entity": dep.referenced_entity,
                    "attribute": dep.referenced_attribute
                }
                for dep in result.fk_dependencies
            }
            # Convert derived_dependencies list to dict for backward compatibility
            derived_deps_dict = {
                dep.attribute_key: dep.base_attributes
                for dep in result.derived_dependencies
            }
            
            # Store created FKs (already a list of dicts)
            created_fks = result.created_foreign_keys
            
            # Convert to dict only for previous_answers storage
            result_dict = result.model_dump()
            
            update_dict: Dict[str, Any] = {
                "current_step": "5.1",
                "previous_answers": {**prev_answers, "5.1": result_dict},
                "metadata": {
                    **state.get("metadata", {}),
                    "dependency_graph": result.dependency_graph,
                    "independent_attributes": result.independent_attributes,
                    "dependent_attributes": result.dependent_attributes,
                    "fk_dependencies": fk_deps_dict,
                    "derived_dependencies": derived_deps_dict,
                },
                "data_types": {},  # Will be populated by steps 5.2, 5.3, 5.4
            }
        else:
            # Fallback for dict format (shouldn't happen, but handle gracefully)
            result_dict = result
            fk_deps_dict = result_dict.get("fk_dependencies", {})
            derived_deps_dict = result_dict.get("derived_dependencies", {})
            created_fks = result_dict.get("created_foreign_keys", [])
            
            update_dict: Dict[str, Any] = {
                "current_step": "5.1",
                "previous_answers": {**prev_answers, "5.1": result_dict},
                "metadata": {
                    **state.get("metadata", {}),
                    "dependency_graph": result_dict.get("dependency_graph", {}),
                    "independent_attributes": result_dict.get("independent_attributes", []),
                    "dependent_attributes": result_dict.get("dependent_attributes", []),
                    "fk_dependencies": fk_deps_dict,
                    "derived_dependencies": derived_deps_dict,
                },
                "data_types": {},  # Will be populated by steps 5.2, 5.3, 5.4
            }
        
        if created_fks:
            update_dict["foreign_keys"] = created_fks
        
        return update_dict

    return node


def _wrap_step_5_2(step_func):
    """Wrap Step 5.2 to work as LangGraph node."""

    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 5.2: Independent Attribute Data Types (excluding FKs)")
        prev_answers = state.get("previous_answers", {})
        metadata = state.get("metadata", {})
        
        independent_attributes = metadata.get("independent_attributes", [])
        fk_dependencies = metadata.get("fk_dependencies", {})
        
        # Filter out foreign keys from independent attributes (FKs are handled deterministically in step 5.3)
        fk_keys = set(fk_dependencies.keys())
        non_fk_independent = [
            (entity, attr) for entity, attr in independent_attributes
            if f"{entity}.{attr}" not in fk_keys
        ]
        
        logger.info(f"Filtered {len(independent_attributes)} independent attributes to {len(non_fk_independent)} non-FK attributes")
        
        # Extract entity descriptions
        entities = state.get("entities", [])
        entity_descriptions = {}
        for entity in entities:
            entity_name = entity.get("name", "") if isinstance(entity, dict) else getattr(entity, "name", "")
            entity_desc = entity.get("description", "") if isinstance(entity, dict) else getattr(entity, "description", "")
            if entity_name:
                entity_descriptions[entity_name] = entity_desc
        
        result = await invoke_step_checked(
            step_func,
            independent_attributes=non_fk_independent,  # Only non-FK attributes
            attributes=state.get("attributes", {}),
            primary_keys=state.get("primary_keys", {}),
            domain=state.get("domain"),
            entity_descriptions=entity_descriptions,
            nl_description=state.get("nl_description"),
        )
        
        # Work directly with Pydantic model (IndependentAttributeDataTypesBatchOutput)
        if hasattr(result, 'data_types'):
            # It's IndependentAttributeDataTypesBatchOutput - work with Pydantic model directly
            # Convert AttributeTypeAssignment objects to dict format for state compatibility
            new_data_types = {
                assignment.attribute_key: assignment.type_info.model_dump() if hasattr(assignment.type_info, 'model_dump') else assignment.type_info
                for assignment in result.data_types
            }
            result_dict = result.model_dump()
        elif isinstance(result, dict):
            # Fallback for dict format
            result_dict = result
            new_data_types = result.get("data_types", {})
        else:
            # Try to convert if it's a Pydantic model
            result_dict = result.model_dump() if hasattr(result, 'model_dump') else result
            new_data_types = {}
        
        # Merge data_types into state
        existing_data_types = state.get("data_types", {})
        combined_data_types = {**existing_data_types, **new_data_types}
        
        return {
            "current_step": "5.2",
            "previous_answers": {**prev_answers, "5.2": result_dict},
            "data_types": combined_data_types,
        }

    return node


def _wrap_step_5_3(step_func):
    """Wrap Step 5.3 to work as LangGraph node."""

    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 5.3: Deterministic FK Data Types")
        prev_answers = state.get("previous_answers", {})
        
        foreign_keys = state.get("foreign_keys", [])
        independent_types = state.get("data_types", {})  # From Step 3.2
        
        result = await invoke_step_checked(
            step_func,
            foreign_keys=foreign_keys,
            independent_attribute_data_types=independent_types,
        )
        
        # Work directly with Pydantic model (FkDataTypesOutput)
        if hasattr(result, 'fk_data_types'):
            # It's FkDataTypesOutput - work with Pydantic model directly
            # Convert FkTypeAssignment objects to dict format for state compatibility
            fk_data_types = {
                assignment.attribute_key: assignment.type_info.model_dump() if hasattr(assignment.type_info, 'model_dump') else assignment.type_info
                for assignment in result.fk_data_types
            }
            result_dict = result.model_dump()
        elif isinstance(result, dict):
            # Fallback for dict format
            result_dict = result
            fk_data_types = result.get("fk_data_types", {})
        else:
            # Try to convert if it's a Pydantic model
            result_dict = result.model_dump() if hasattr(result, 'model_dump') else result
            fk_data_types = {}
        
        # Merge FK data_types into state
        existing_data_types = state.get("data_types", {})
        combined_data_types = {**existing_data_types, **fk_data_types}
        
        return {
            "current_step": "5.3",
            "previous_answers": {**prev_answers, "5.3": result_dict},
            "data_types": combined_data_types,
        }

    return node


def _wrap_step_5_4(step_func):
    """Wrap Step 5.4 to work as LangGraph node."""

    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 5.4: Dependent Attribute Data Types")
        prev_answers = state.get("previous_answers", {})
        metadata = state.get("metadata", {})
        
        dependent_attributes = metadata.get("dependent_attributes", [])
        dependency_graph = metadata.get("dependency_graph", {})
        fk_dependencies = metadata.get("fk_dependencies", {})
        derived_dependencies = metadata.get("derived_dependencies", {})
        
        # Get types from previous steps
        all_data_types = state.get("data_types", {})
        independent_types = {}
        fk_types = {}
        
        # Separate independent and FK types (FKs are already in data_types from step 3.3)
        # For step 3.4, we need to know which are independent vs FK
        # We'll use all_data_types as independent_types and fk_types will be empty
        # (since FKs are already merged in)
        independent_types = all_data_types
        
        result = await invoke_step_checked(
            step_func,
            dependent_attributes=dependent_attributes,
            attributes=state.get("attributes", {}),
            dependency_graph=dependency_graph,
            fk_dependencies=fk_dependencies,
            derived_dependencies=derived_dependencies,
            independent_types=independent_types,
            fk_types=fk_types,  # FKs already merged, but kept for API compatibility
            primary_keys=state.get("primary_keys", {}),
            derived_formulas=state.get("derived_formulas", {}),
            domain=state.get("domain"),
            nl_description=state.get("nl_description"),
        )
        
        # Work directly with Pydantic model (DependentAttributeDataTypesBatchOutput)
        if hasattr(result, 'data_types'):
            # It's DependentAttributeDataTypesBatchOutput - work with Pydantic model directly
            # Convert AttributeTypeAssignment objects to dict format for state compatibility
            dependent_data_types = {
                assignment.attribute_key: assignment.type_info.model_dump() if hasattr(assignment.type_info, 'model_dump') else assignment.type_info
                for assignment in result.data_types
            }
            result_dict = result.model_dump()
        elif isinstance(result, dict):
            # Fallback for dict format
            result_dict = result
            dependent_data_types = result.get("data_types", {})
        else:
            # Try to convert if it's a Pydantic model
            result_dict = result.model_dump() if hasattr(result, 'model_dump') else result
            dependent_data_types = {}
        
        # Merge dependent data_types into state
        existing_data_types = state.get("data_types", {})
        combined_data_types = {**existing_data_types, **dependent_data_types}
        
        return {
            "current_step": "5.4",
            "previous_answers": {**prev_answers, "5.4": result_dict},
            "data_types": combined_data_types,
        }

    return node


def _wrap_step_5_5(step_func):
    """Wrap Step 5.5 to work as LangGraph node."""

    async def node(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 5.5: Nullability Detection")
        prev_answers = state.get("previous_answers", {})
        metadata = state.get("metadata", {})
        
        relational_schema = metadata.get("relational_schema", {})
        
        if not relational_schema:
            logger.warning("No relational schema found in metadata. Skipping nullability detection.")
            return {
                "current_step": "5.5",
                "previous_answers": {**prev_answers, "5.5": {"table_results": {}, "total_tables": 0}},
                "metadata": {
                    **metadata,
                    "nullability": {},
                },
            }
        
        result = await invoke_step_checked(
            step_func,
            relational_schema=relational_schema,
            primary_keys=state.get("primary_keys", {}),
            foreign_keys=state.get("foreign_keys", []),
            nl_description=state.get("nl_description"),
            domain=state.get("domain"),
        )
        
        # Work directly with Pydantic model (NullabilityBatchOutput)
        if hasattr(result, 'table_results'):
            # It's NullabilityBatchOutput - work with Pydantic model directly
            # Convert TableNullabilityResult objects to dict format for state compatibility
            table_results_dict = {
                table_result.table_name: {
                    "nullable_columns": table_result.nullable_columns,
                    "not_nullable_columns": table_result.not_nullable_columns,
                    "reasoning": table_result.reasoning
                }
                for table_result in result.table_results
            }
            result_dict = result.model_dump()
        elif isinstance(result, dict):
            # Fallback for dict format
            result_dict = result
            table_results_list = result.get("table_results", [])
            table_results_dict = {
                table_result.get("table_name"): {
                    "nullable_columns": table_result.get("nullable_columns", []),
                    "not_nullable_columns": table_result.get("not_nullable_columns", []),
                    "reasoning": table_result.get("reasoning", "")
                }
                for table_result in table_results_list
            }
        else:
            # Try to convert if it's a Pydantic model
            result_dict = result.model_dump() if hasattr(result, 'model_dump') else result
            table_results_dict = {}
        
        # Store nullability results in metadata
        return {
            "current_step": "5.5",
            "previous_answers": {**prev_answers, "5.5": result_dict},
            "metadata": {
                **metadata,
                "nullability": table_results_dict,
            },
        }

    return node


def create_phase_5_graph() -> StateGraph:
    """Create LangGraph StateGraph for Phase 5 (Attribute Dependency Graph and Data Type Assignment).
    
    This phase:
    1. Builds the attribute dependency graph (Step 5.1)
    2. Assigns types to independent attributes (Step 5.2) - excludes FKs
    3. Derives FK types from PK types deterministically (Step 5.3)
    4. Assigns types to dependent attributes (Step 5.4) - derived types determined from DSL formulas
    5. Determines nullability for columns (Step 5.5) - excludes PKs and FKs with total participation
    """

    from NL2DATA.phases.phase5 import (
        step_5_1_attribute_dependency_graph,
        step_5_2_independent_attribute_data_types_batch,
        step_5_3_deterministic_fk_data_types,
        step_5_4_dependent_attribute_data_types_batch,
        step_5_5_nullability_detection_batch,
    )

    workflow = StateGraph(IRGenerationState)
    workflow.add_node("dependency_graph", _wrap_step_5_1(step_5_1_attribute_dependency_graph))
    workflow.add_node("independent_types", _wrap_step_5_2(step_5_2_independent_attribute_data_types_batch))
    workflow.add_node("fk_types", _wrap_step_5_3(step_5_3_deterministic_fk_data_types))
    workflow.add_node("dependent_types", _wrap_step_5_4(step_5_4_dependent_attribute_data_types_batch))
    workflow.add_node("nullability", _wrap_step_5_5(step_5_5_nullability_detection_batch))

    workflow.set_entry_point("dependency_graph")
    workflow.add_edge("dependency_graph", "independent_types")
    workflow.add_edge("independent_types", "fk_types")
    workflow.add_edge("fk_types", "dependent_types")
    workflow.add_edge("dependent_types", "nullability")
    workflow.add_edge("nullability", END)

    checkpointer = MemorySaver()
    return workflow.compile(checkpointer=checkpointer)

