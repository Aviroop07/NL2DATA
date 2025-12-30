"""TypedDict state model for LangGraph workflows.

This module defines the IRGenerationState TypedDict that serves as the
centralized state for all LangGraph workflows. It accumulates information
across all phases of the IR generation process.
"""

from typing import TypedDict, Annotated, List, Dict, Optional, Any
from operator import add, or_


class IRGenerationState(TypedDict, total=False):
    """Centralized state for IR generation workflow.
    
    This TypedDict accumulates all discovered information as the workflow
    progresses through phases. Fields marked with Annotated[list, add] are
    accumulated (merged) when multiple nodes update them.
    
    Fields are optional (total=False) to allow incremental state building.
    """
    
    # Input
    nl_description: str  # Original natural language description
    
    # Phase tracking
    phase: int  # Current phase (1-7)
    current_step: str  # Current step identifier (e.g., "1.4")
    
    # Phase 1: Domain & Entity Discovery
    domain: Optional[str]  # Detected or inferred domain
    has_explicit_domain: Optional[bool]  # Whether domain was explicitly mentioned
    entities: Annotated[List[Dict[str, Any]], add]  # Accumulated entities
    relations: Annotated[List[Dict[str, Any]], add]  # Accumulated relations
    entity_cardinalities: Dict[str, Dict[str, str]]  # entity -> cardinality info
    relation_cardinalities: Dict[str, Dict[str, Any]]  # relation_id -> cardinality info
    
    # Phase 2: Attribute Discovery & Schema Design
    attributes: Dict[str, List[Dict[str, Any]]]  # entity -> list of attributes
    primary_keys: Dict[str, List[str]]  # entity -> list of PK attribute names
    foreign_keys: List[Dict[str, Any]]  # List of foreign key definitions
    constraints: Annotated[List[Dict[str, Any]], add]  # Accumulated constraints
    
    # Phase 3: Query Requirements & Schema Refinement
    information_needs: Annotated[List[Dict[str, Any]], add]  # Accumulated information needs
    sql_queries: Annotated[List[Dict[str, Any]], add]  # Accumulated SQL queries
    
    # Phase 4: Functional Dependencies & Data Types
    functional_dependencies: Annotated[List[Dict[str, Any]], add]  # Accumulated FDs
    data_types: Dict[str, Dict[str, Dict[str, Any]]]  # entity -> attribute -> type info
    categorical_attributes: Dict[str, List[str]]  # entity -> list of categorical attrs
    
    # Phase 5: DDL & SQL Generation
    ddl_statements: Annotated[List[str], add]  # Accumulated DDL statements
    ddl_validation_errors: List[Dict[str, Any]]  # DDL validation errors
    
    # Phase 6: Constraints & Distributions
    constraint_specs: Annotated[List[Dict[str, Any]], add]  # Accumulated constraint specs
    
    # Phase 7: Generation Strategies
    generation_strategies: Dict[str, Dict[str, Dict[str, Any]]]  # entity -> attribute -> strategy
    
    # Metadata & Tracking
    errors: Annotated[List[Dict[str, Any]], add]  # Accumulated errors
    warnings: Annotated[List[str], add]  # Accumulated warnings
    previous_answers: Dict[str, Any]  # Answers from previous steps (for context)
    # Flexible metadata storage.
    # IMPORTANT: This must be mergeable because parallel nodes may write to metadata in the same tick.
    # We use dict union (operator.or_) to merge dictionaries.
    metadata: Annotated[Dict[str, Any], or_]
    
    # Loop tracking
    loop_iterations: Dict[str, int]  # step_id -> iteration count
    loop_termination_reasons: Dict[str, str]  # step_id -> termination reason


def create_initial_state(nl_description: str) -> IRGenerationState:
    """Create initial state for IR generation workflow.
    
    Args:
        nl_description: Natural language description of the database requirements
        
    Returns:
        Initial IRGenerationState with nl_description set
    """
    return {
        "nl_description": nl_description,
        "phase": 1,
        "current_step": "",
        "domain": None,
        "has_explicit_domain": None,
        "entities": [],
        "relations": [],
        "entity_cardinalities": {},
        "relation_cardinalities": {},
        "attributes": {},
        "primary_keys": {},
        "foreign_keys": [],
        "constraints": [],
        "information_needs": [],
        "sql_queries": [],
        "functional_dependencies": [],
        "data_types": {},
        "categorical_attributes": {},
        "ddl_statements": [],
        "ddl_validation_errors": [],
        "constraint_specs": [],
        "generation_strategies": {},
        "errors": [],
        "warnings": [],
        "previous_answers": {},
        "metadata": {},
        "loop_iterations": {},
        "loop_termination_reasons": {},
    }

