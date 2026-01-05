"""Conversion service - handles ER ↔ Relational Schema conversion."""

from typing import Dict, Any
try:
    from NL2DATA.phases.phase3.step_4_3_relational_schema_compilation import (
        step_4_3_relational_schema_compilation
    )
    from NL2DATA.ir.models.er_relational import ERDesign, RelationalSchema
except ImportError:
    # Fallback if NL2DATA not available
    step_4_3_relational_schema_compilation = None
    ERDesign = None
    RelationalSchema = None


class ConversionService:
    """Handles ER ↔ Relational Schema conversion."""
    
    async def apply_changes(
        self,
        current_state: Dict[str, Any],
        changes: Dict[str, Any],
        edit_mode: str
    ) -> Dict[str, Any]:
        """Apply changes and convert between ER and Relational."""
        updated_state = current_state.copy()
        
        if edit_mode == "er_diagram":
            # Apply ER changes
            updated_state = self._apply_er_changes(updated_state, changes)
            
            # Convert ER → Relational
            if step_4_3_relational_schema_compilation and ERDesign:
                er_design = ERDesign(**updated_state.get("er_design", {}))
                relational_schema = await step_4_3_relational_schema_compilation(er_design)
                updated_state["relational_schema"] = relational_schema.dict() if hasattr(relational_schema, 'dict') else relational_schema
            
        elif edit_mode == "relational_schema":
            # Apply Schema changes (data types, strategies)
            updated_state = self._apply_schema_changes(updated_state, changes)
            # ER structure remains unchanged
        
        return updated_state
    
    def _apply_er_changes(self, state: Dict[str, Any], changes: Dict[str, Any]) -> Dict[str, Any]:
        """Apply ER diagram changes."""
        # Implementation: merge changes into state
        # Handle added_entities, modified_entities, deleted_entities, etc.
        # TODO: Implement proper merging logic
        return state
    
    def _apply_schema_changes(self, state: Dict[str, Any], changes: Dict[str, Any]) -> Dict[str, Any]:
        """Apply relational schema changes."""
        # Implementation: update data types, generation strategies
        # TODO: Implement proper update logic
        return state

