"""Phase 9: Generation Strategies Graph.

This phase handles generation strategy definition for independent attributes.

Execution order:
1. Step 9.1: Numerical Range Definition
2. Step 9.2: Text Generation Strategy
3. Step 9.3: Boolean Dependency Analysis
4. Step 9.4: Data Volume Specifications
5. Step 9.5: Partitioning Strategy
6. Step 9.6: Distribution Compilation
"""

from typing import Dict, Any, List, Set

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from ..state import IRGenerationState
from .common import logger, invoke_step_checked


def _extract_independent_attributes(state: IRGenerationState) -> Dict[str, List[Dict[str, Any]]]:
    """
    Extract independent attributes from relational schema.
    
    Excludes:
    - Derived attributes (from derived_formulas)
    - Constrained columns (from compiled_constraints)
    - Primary keys
    - Foreign keys
    
    Returns:
        Dictionary mapping table_name -> list of attribute metadata dicts
    """
    relational_schema = state.get("metadata", {}).get("relational_schema", {})
    if not relational_schema:
        logger.warning("No relational schema found in state")
        return {}
    
    # Get exclusion sets
    derived_formulas = state.get("derived_formulas", {})
    derived_keys = set(derived_formulas.keys())
    
    # Get constrained columns from compiled constraints
    compiled_constraints = state.get("metadata", {}).get("compiled_constraints", [])
    excluded_columns = set()
    for constraint in compiled_constraints:
        if isinstance(constraint, dict):
            columns = constraint.get("columns", [])
            if isinstance(columns, list):
                for col in columns:
                    if isinstance(col, str):
                        excluded_columns.add(col)
                    elif isinstance(col, dict):
                        col_name = col.get("name") or col.get("column")
                        if col_name:
                            excluded_columns.add(col_name)
    
    # Get primary keys
    primary_keys = state.get("primary_keys", {})
    all_pk_set = set()
    for entity_pks in primary_keys.values():
        if isinstance(entity_pks, list):
            all_pk_set.update(entity_pks)
    
    # Get foreign keys
    foreign_keys = state.get("foreign_keys", [])
    fk_columns = set()
    for fk in foreign_keys:
        if isinstance(fk, dict):
            fk_col = fk.get("column") or fk.get("attribute")
            if fk_col:
                fk_columns.add(fk_col)
    
    # Build entity to table mapping
    entities = state.get("entities", [])
    entity_to_table_map = {}
    for entity in entities:
        entity_name = entity.get("name", "") if isinstance(entity, dict) else getattr(entity, "name", "")
        if entity_name:
            # Table name is usually the entity name, but check relational schema
            entity_to_table_map[entity_name] = entity_name
    
    # Extract independent attributes from relational schema
    independent_attributes = {}
    
    tables = relational_schema.get("tables", [])
    if not tables:
        # Try alternative structure
        tables = list(relational_schema.values()) if isinstance(relational_schema, dict) else []
    
    for table in tables:
        if not isinstance(table, dict):
            continue
        
        table_name = table.get("name", "")
        if not table_name:
            continue
        
        columns = table.get("columns", [])
        if not columns:
            continue
        
        independent_attrs = []
        for col in columns:
            if not isinstance(col, dict):
                continue
            
            col_name = col.get("name", "")
            if not col_name:
                continue
            
            # Check if derived
            attr_key = f"{table_name}.{col_name}"
            is_derived = attr_key in derived_keys
            if is_derived:
                continue
            
            # Check if constrained/excluded
            is_excluded = False
            if attr_key in excluded_columns:
                is_excluded = True
            else:
                # Also check entity name format
                for entity_name, mapped_table in entity_to_table_map.items():
                    if mapped_table == table_name and entity_name != table_name:
                        entity_attr_key = f"{entity_name}.{col_name}"
                        if entity_attr_key in excluded_columns:
                            is_excluded = True
                            break
            
            if is_excluded:
                continue
            
            # Check if primary key
            if col_name in all_pk_set:
                continue
            
            # Check if foreign key
            if col_name in fk_columns:
                continue
            
            # This is an independent attribute
            independent_attrs.append({
                "entity_name": table_name,
                "attribute_name": col_name,
                "description": col.get("description", ""),
                "type_hint": col.get("type_hint", ""),
            })
        
        if independent_attrs:
            independent_attributes[table_name] = independent_attrs
    
    return independent_attributes


def _group_attributes_by_type(
    independent_attributes: Dict[str, List[Dict[str, Any]]],
    data_types: Dict[str, Dict[str, Dict[str, Any]]]
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Group independent attributes by data type (numerical, text, boolean).
    
    Args:
        independent_attributes: Dictionary mapping table_name -> list of attribute metadata
        data_types: Dictionary mapping entity -> attribute -> type info
        
    Returns:
        Dictionary with keys: "numerical", "text", "boolean"
    """
    numerical_attrs = []
    text_attrs = []
    boolean_attrs = []
    
    for table_name, attrs in independent_attributes.items():
        for attr in attrs:
            attr_name = attr.get("attribute_name", "")
            entity_name = attr.get("entity_name", table_name)
            
            # Get data type
            type_info = None
            if entity_name in data_types:
                entity_types = data_types[entity_name]
                if attr_name in entity_types:
                    type_info = entity_types[attr_name]
            
            # Determine type from type_info or type_hint
            sql_type = None
            if type_info:
                sql_type = type_info.get("sql_type") or type_info.get("type")
            
            if not sql_type:
                type_hint = attr.get("type_hint", "")
                if type_hint:
                    sql_type = type_hint
            
            # Classify attribute
            sql_type_lower = (sql_type or "").lower()
            
            if any(t in sql_type_lower for t in ["int", "float", "decimal", "numeric", "real", "double", "number"]):
                numerical_attrs.append(attr)
            elif any(t in sql_type_lower for t in ["bool", "boolean"]):
                boolean_attrs.append(attr)
            else:
                # Default to text for string types and unknown types
                text_attrs.append(attr)
    
    return {
        "numerical": numerical_attrs,
        "text": text_attrs,
        "boolean": boolean_attrs,
    }


def create_phase_9_graph() -> StateGraph:
    """Create LangGraph StateGraph for Phase 9 (Generation Strategies)."""
    from NL2DATA.phases.phase9 import (
        step_9_1_numerical_range_definition_batch,
        step_9_2_text_generation_strategy_batch,
        step_9_3_boolean_dependency_analysis_batch,
        step_9_4_data_volume_specifications,
        step_9_5_partitioning_strategy_batch,
        step_9_6_distribution_compilation,
    )
    
    workflow = StateGraph(IRGenerationState)
    
    # Step 9.1: Numerical Range Definition
    async def numerical_range_definition(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 9.1: Numerical Range Definition")
        
        # Extract independent attributes
        independent_attributes = _extract_independent_attributes(state)
        
        # Group by type
        data_types = state.get("data_types", {})
        grouped_attrs = _group_attributes_by_type(independent_attributes, data_types)
        numerical_attrs = grouped_attrs.get("numerical", [])
        
        if not numerical_attrs:
            logger.info("No numerical attributes found, skipping numerical range definition")
            result = {"strategies": {}}
        else:
            # Get constraints map for numerical attributes
            constraints = state.get("constraints", [])
            constraints_map = {}
            for constraint in constraints:
                if isinstance(constraint, dict):
                    constraint_type = constraint.get("type", "")
                    if constraint_type in ["range", "check"]:
                        columns = constraint.get("columns", [])
                        for col in columns:
                            if isinstance(col, str):
                                constraints_map[col] = constraint
                            elif isinstance(col, dict):
                                col_name = col.get("name") or col.get("column")
                                if col_name:
                                    constraints_map[col_name] = constraint
            
            result = await invoke_step_checked(
                step_9_1_numerical_range_definition_batch,
                numerical_attributes=numerical_attrs,
                constraints_map=constraints_map if constraints_map else None,
            )
        
        # Extract strategies from result
        numerical_strategies = {}
        if hasattr(result, "strategies"):
            strategies_dict = result.strategies
        elif isinstance(result, dict):
            strategies_dict = result.get("strategies", {})
        else:
            strategies_dict = {}
        
        # Convert to dict format if needed
        for attr_key, strategy in strategies_dict.items():
            if hasattr(strategy, "model_dump"):
                numerical_strategies[attr_key] = strategy.model_dump()
            elif hasattr(strategy, "dict"):
                numerical_strategies[attr_key] = strategy.dict()
            else:
                numerical_strategies[attr_key] = strategy
        
        return {
            **state,
            "previous_answers": {**state.get("previous_answers", {}), "9.1": result},
            "metadata": {
                **state.get("metadata", {}),
                "numerical_strategies": numerical_strategies,
            }
        }
    
    # Step 9.2: Text Generation Strategy
    async def text_generation_strategy(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 9.2: Text Generation Strategy")
        
        # Extract independent attributes
        independent_attributes = _extract_independent_attributes(state)
        
        # Group by type
        data_types = state.get("data_types", {})
        grouped_attrs = _group_attributes_by_type(independent_attributes, data_types)
        text_attrs = grouped_attrs.get("text", [])
        
        if not text_attrs:
            logger.info("No text attributes found, skipping text generation strategy")
            result = {"strategies": {}}
        else:
            # Get generator catalog if available
            generator_catalog = state.get("metadata", {}).get("generator_catalog")
            
            result = await invoke_step_checked(
                step_9_2_text_generation_strategy_batch,
                text_attributes=text_attrs,
                generator_catalog=generator_catalog,
            )
        
        # Extract strategies from result
        text_strategies = {}
        if hasattr(result, "strategies"):
            strategies_dict = result.strategies
        elif isinstance(result, dict):
            strategies_dict = result.get("strategies", {})
        else:
            strategies_dict = {}
        
        # Convert to dict format if needed
        for attr_key, strategy in strategies_dict.items():
            if hasattr(strategy, "model_dump"):
                text_strategies[attr_key] = strategy.model_dump()
            elif hasattr(strategy, "dict"):
                text_strategies[attr_key] = strategy.dict()
            else:
                text_strategies[attr_key] = strategy
        
        return {
            **state,
            "previous_answers": {**state.get("previous_answers", {}), "9.2": result},
            "metadata": {
                **state.get("metadata", {}),
                "text_strategies": text_strategies,
            }
        }
    
    # Step 9.3: Boolean Dependency Analysis
    async def boolean_dependency_analysis(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 9.3: Boolean Dependency Analysis")
        
        # Extract independent attributes
        independent_attributes = _extract_independent_attributes(state)
        
        # Group by type
        data_types = state.get("data_types", {})
        grouped_attrs = _group_attributes_by_type(independent_attributes, data_types)
        boolean_attrs = grouped_attrs.get("boolean", [])
        
        if not boolean_attrs:
            logger.info("No boolean attributes found, skipping boolean dependency analysis")
            result = {"strategies": {}}
        else:
            # Get related attributes map and DSL grammar if available
            related_attributes_map = state.get("metadata", {}).get("related_attributes_map")
            dsl_grammar = state.get("metadata", {}).get("dsl_grammar")
            
            result = await invoke_step_checked(
                step_9_3_boolean_dependency_analysis_batch,
                boolean_attributes=boolean_attrs,
                related_attributes_map=related_attributes_map,
                dsl_grammar=dsl_grammar,
            )
        
        # Extract strategies from result
        boolean_strategies = {}
        if hasattr(result, "strategies"):
            strategies_dict = result.strategies
        elif isinstance(result, dict):
            strategies_dict = result.get("strategies", {})
        else:
            strategies_dict = {}
        
        # Convert to dict format if needed
        for attr_key, strategy in strategies_dict.items():
            if hasattr(strategy, "model_dump"):
                boolean_strategies[attr_key] = strategy.model_dump()
            elif hasattr(strategy, "dict"):
                boolean_strategies[attr_key] = strategy.dict()
            else:
                boolean_strategies[attr_key] = strategy
        
        return {
            **state,
            "previous_answers": {**state.get("previous_answers", {}), "9.3": result},
            "metadata": {
                **state.get("metadata", {}),
                "boolean_strategies": boolean_strategies,
            }
        }
    
    # Step 9.4: Data Volume Specifications
    async def data_volume_specifications(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 9.4: Data Volume Specifications")
        
        entities = state.get("entities", [])
        
        # Convert entities to list of dicts if needed
        entity_list = []
        for entity in entities:
            if isinstance(entity, dict):
                entity_list.append(entity)
            else:
                entity_dict = {
                    "name": getattr(entity, "name", ""),
                    "description": getattr(entity, "description", ""),
                }
                entity_list.append(entity_dict)
        
        result = await invoke_step_checked(
            step_9_4_data_volume_specifications,
            entities=entity_list,
            nl_description=state.get("nl_description"),
        )
        
        # Extract entity volumes from result
        entity_volumes = {}
        if hasattr(result, "entity_volumes"):
            volumes_dict = result.entity_volumes
        elif isinstance(result, dict):
            volumes_dict = result.get("entity_volumes", {})
        else:
            volumes_dict = {}
        
        # Convert to dict format if needed
        for entity_name, volume_spec in volumes_dict.items():
            if hasattr(volume_spec, "model_dump"):
                entity_volumes[entity_name] = volume_spec.model_dump()
            elif hasattr(volume_spec, "dict"):
                entity_volumes[entity_name] = volume_spec.dict()
            else:
                entity_volumes[entity_name] = volume_spec
        
        return {
            **state,
            "previous_answers": {**state.get("previous_answers", {}), "9.4": result},
            "metadata": {
                **state.get("metadata", {}),
                "entity_volumes": entity_volumes,
            }
        }
    
    # Step 9.5: Partitioning Strategy
    async def partitioning_strategy(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 9.5: Partitioning Strategy")
        
        # Get entities with volumes
        entity_volumes = state.get("metadata", {}).get("entity_volumes", {})
        entities = state.get("entities", [])
        
        # Build entities with volumes list
        entities_with_volumes = []
        for entity in entities:
            entity_name = entity.get("name", "") if isinstance(entity, dict) else getattr(entity, "name", "")
            if entity_name and entity_name in entity_volumes:
                entities_with_volumes.append({
                    "entity_name": entity_name,
                    "volume": entity_volumes[entity_name],
                })
        
        if not entities_with_volumes:
            logger.info("No entities with volumes found, skipping partitioning strategy")
            result = {"strategies": {}}
        else:
            result = await invoke_step_checked(
                step_9_5_partitioning_strategy_batch,
                entities_with_volumes=entities_with_volumes,
            )
        
        # Extract partitioning strategies from result
        partitioning_strategies = {}
        if hasattr(result, "strategies"):
            strategies_dict = result.strategies
        elif isinstance(result, dict):
            strategies_dict = result.get("strategies", {})
        else:
            strategies_dict = {}
        
        # Convert to dict format if needed
        for entity_name, strategy in strategies_dict.items():
            if hasattr(strategy, "model_dump"):
                partitioning_strategies[entity_name] = strategy.model_dump()
            elif hasattr(strategy, "dict"):
                partitioning_strategies[entity_name] = strategy.dict()
            else:
                partitioning_strategies[entity_name] = strategy
        
        return {
            **state,
            "previous_answers": {**state.get("previous_answers", {}), "9.5": result},
            "metadata": {
                **state.get("metadata", {}),
                "partitioning_strategies": partitioning_strategies,
            }
        }
    
    # Step 9.6: Distribution Compilation
    async def distribution_compilation(state: IRGenerationState) -> Dict[str, Any]:
        logger.info("[LangGraph] Executing Step 9.6: Distribution Compilation")
        
        # Get all strategies from metadata
        numerical_strategies = state.get("metadata", {}).get("numerical_strategies", {})
        text_strategies = state.get("metadata", {}).get("text_strategies", {})
        boolean_strategies = state.get("metadata", {}).get("boolean_strategies", {})
        entity_volumes = state.get("metadata", {}).get("entity_volumes", {})
        partitioning_strategies = state.get("metadata", {}).get("partitioning_strategies", {})
        
        # Get categorical values from phase 8
        categorical_values = state.get("categorical_values", {})
        
        result = await invoke_step_checked(
            step_9_6_distribution_compilation,
            numerical_strategies=numerical_strategies,
            text_strategies=text_strategies,
            boolean_strategies=boolean_strategies,
            categorical_strategies=None,  # Deprecated, use categorical_values instead
            categorical_values=categorical_values if categorical_values else None,
            entity_volumes=entity_volumes if entity_volumes else None,
            partitioning_strategies=partitioning_strategies if partitioning_strategies else None,
        )
        
        # Extract column generation specs and build generation_strategies dict
        column_gen_specs = []
        if hasattr(result, "column_gen_specs"):
            specs_list = result.column_gen_specs
        elif isinstance(result, dict):
            specs_list = result.get("column_gen_specs", [])
        else:
            specs_list = []
        
        # Convert to dict format if needed
        for spec in specs_list:
            if hasattr(spec, "model_dump"):
                column_gen_specs.append(spec.model_dump())
            elif hasattr(spec, "dict"):
                column_gen_specs.append(spec.dict())
            else:
                column_gen_specs.append(spec)
        
        # Build generation_strategies dict: entity -> attribute -> strategy
        generation_strategies = {}
        for spec in column_gen_specs:
            if not isinstance(spec, dict):
                continue
            
            table_name = spec.get("table", "")
            column_name = spec.get("column", "")
            strategy_type = spec.get("type", "")
            strategy_data = spec.get("strategy_data", {})
            
            if not table_name or not column_name:
                continue
            
            if table_name not in generation_strategies:
                generation_strategies[table_name] = {}
            
            generation_strategies[table_name][column_name] = {
                "type": strategy_type,
                **strategy_data,
            }
        
        return {
            **state,
            "generation_strategies": generation_strategies,
            "previous_answers": {**state.get("previous_answers", {}), "9.6": result},
            "metadata": {
                **state.get("metadata", {}),
                "column_gen_specs": column_gen_specs,
            }
        }
    
    # Add nodes
    workflow.add_node("numerical_range_definition", numerical_range_definition)
    workflow.add_node("text_generation_strategy", text_generation_strategy)
    workflow.add_node("boolean_dependency_analysis", boolean_dependency_analysis)
    workflow.add_node("data_volume_specifications", data_volume_specifications)
    workflow.add_node("partitioning_strategy", partitioning_strategy)
    workflow.add_node("distribution_compilation", distribution_compilation)
    
    # Set entry point
    workflow.set_entry_point("numerical_range_definition")
    
    # Add sequential edges
    workflow.add_edge("numerical_range_definition", "text_generation_strategy")
    workflow.add_edge("text_generation_strategy", "boolean_dependency_analysis")
    workflow.add_edge("boolean_dependency_analysis", "data_volume_specifications")
    workflow.add_edge("data_volume_specifications", "partitioning_strategy")
    workflow.add_edge("partitioning_strategy", "distribution_compilation")
    workflow.add_edge("distribution_compilation", END)
    
    checkpointer = MemorySaver()
    return workflow.compile(checkpointer=checkpointer)
