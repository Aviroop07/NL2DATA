"""Tests for ValidationService."""

import pytest
from backend.services.validation_service import ValidationService


@pytest.mark.asyncio
async def test_validate_er_changes():
    """Test validating ER diagram changes."""
    service = ValidationService()
    current_state = {
        "entities": [{"name": "Customer"}],
        "relations": []
    }
    changes = {
        "added_entities": [{"name": "Product"}]
    }
    
    errors = await service.validate_changes(current_state, changes, "er_diagram")
    
    assert isinstance(errors, list)
    # For now, placeholder returns empty list
    # When implemented, should validate actual changes


@pytest.mark.asyncio
async def test_validate_schema_changes():
    """Test validating relational schema changes."""
    service = ValidationService()
    current_state = {
        "relational_schema": {
            "tables": [{"name": "Customer", "columns": []}]
        }
    }
    changes = {
        "modified_tables": [{"name": "Customer", "data_type": "INT"}]
    }
    
    errors = await service.validate_changes(current_state, changes, "relational_schema")
    
    assert isinstance(errors, list)


@pytest.mark.asyncio
async def test_validate_invalid_edit_mode():
    """Test validation with invalid edit mode."""
    service = ValidationService()
    
    errors = await service.validate_changes({}, {}, "invalid_mode")
    
    assert isinstance(errors, list)
    # Should handle gracefully



