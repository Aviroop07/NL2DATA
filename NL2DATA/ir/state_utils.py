"""Utilities for working with GenerationState."""

from typing import List, Optional
from .models.state import GenerationState, AttributeInfo


def create_empty_state(description: Optional[str] = None) -> GenerationState:
    """
    Create an empty GenerationState with optional description.
    
    Args:
        description: Optional natural language description
        
    Returns:
        GenerationState: Empty state ready for phase execution
    """
    return GenerationState(description=description)


def get_entity_names(state: GenerationState) -> List[str]:
    """
    Get list of all entity names from state.
    
    Args:
        state: GenerationState
        
    Returns:
        List of entity names
    """
    return [entity.name for entity in state.entities]


def get_attributes_for_entity(state: GenerationState, entity_name: str) -> List[AttributeInfo]:
    """
    Get attributes for a specific entity.
    
    Args:
        state: GenerationState
        entity_name: Name of the entity
        
    Returns:
        List of AttributeInfo for the entity
    """
    return state.attributes.get(entity_name, [])


def has_entity(state: GenerationState, entity_name: str) -> bool:
    """
    Check if entity exists in state.
    
    Args:
        state: GenerationState
        entity_name: Name of the entity to check
        
    Returns:
        True if entity exists, False otherwise
    """
    return any(entity.name == entity_name for entity in state.entities)

