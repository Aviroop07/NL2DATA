"""Validation service - deterministic schema validation."""

from typing import List, Dict, Any
from backend.models.responses import ValidationError


class ValidationService:
    """Deterministic schema validation."""
    
    async def validate_changes(
        self,
        current_state: Dict[str, Any],
        changes: Dict[str, Any],
        edit_mode: str
    ) -> List[ValidationError]:
        """Validate schema changes."""
        errors = []
        
        if edit_mode == "er_diagram":
            errors.extend(self._validate_er_changes(current_state, changes))
        elif edit_mode == "relational_schema":
            errors.extend(self._validate_schema_changes(current_state, changes))
        
        return errors
    
    def _validate_er_changes(
        self,
        state: Dict[str, Any],
        changes: Dict[str, Any]
    ) -> List[ValidationError]:
        """Validate ER diagram changes."""
        errors = []
        
        # TODO: Use NL2DATA's validation utilities
        # from NL2DATA.utils.validation.state_validation import validate_state_consistency
        
        # Check for:
        # - Referenced entities exist
        # - Referenced attributes exist
        # - Cardinalities are valid
        # - Relations are valid
        
        return errors
    
    def _validate_schema_changes(
        self,
        state: Dict[str, Any],
        changes: Dict[str, Any]
    ) -> List[ValidationError]:
        """Validate relational schema changes."""
        errors = []
        
        # TODO: Validate:
        # - Data types are valid SQL types
        # - Generation strategies are valid
        # - Constraints are valid
        # - Foreign keys reference existing tables
        
        return errors

