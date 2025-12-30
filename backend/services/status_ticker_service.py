"""Status ticker service - generates human-readable status messages."""

from typing import Dict, Any, Optional
from ..utils.websocket_manager import WebSocketManager


class StatusTickerService:
    """Generates human-readable status messages from step metadata."""
    
    def get_message(self, step_id: str, scope: Dict[str, Any] = None) -> str:
        """Get human-readable message for step.
        
        Dynamically imports message templates from NL2DATA step registry.
        """
        try:
            from NL2DATA.orchestration.step_registry.messages import get_step_message
            return get_step_message(step_id, scope)
        except ImportError:
            # Fallback if NL2DATA not available
            return "Processing"
    
    async def send_tick(
        self,
        job_id: str,
        phase: int,
        step: str,
        state: Dict[str, Any],
        ws_manager: WebSocketManager
    ):
        """Send status tick event."""
        step_def, scope, message = self._resolve_step(step, phase, state)
        await ws_manager.send_status_tick(
            job_id=job_id,
            phase=phase,
            step=step,
            step_name=step_def.name if step_def else f"Step {step}",
            scope=scope if step_def else {},
            message=message
        )

    async def send_step_start(
        self,
        job_id: str,
        phase: int,
        step: str,
        state: Dict[str, Any],
        ws_manager: WebSocketManager
    ):
        """Send step start event."""
        step_def, scope, message = self._resolve_step(step, phase, state)
        await ws_manager.send_step_start(
            job_id=job_id,
            phase=phase,
            step=step,
            step_name=step_def.name if step_def else f"Step {step}",
            step_id=step_def.step_id if step_def else f"P{phase}_S{step.replace('.', '_')}",
            scope=scope if step_def else {},
            message=message
        )

    async def send_step_complete(
        self,
        job_id: str,
        phase: int,
        step: str,
        state: Dict[str, Any],
        ws_manager: WebSocketManager
    ):
        """Send step complete event with a lightweight summary."""
        step_def, scope, message = self._resolve_step(step, phase, state)
        summary = self._build_summary(state)
        await ws_manager.send_step_complete(
            job_id=job_id,
            phase=phase,
            step=step,
            step_name=step_def.name if step_def else f"Step {step}",
            step_id=step_def.step_id if step_def else f"P{phase}_S{step.replace('.', '_')}",
            scope=scope if step_def else {},
            message=message,
            summary=summary
        )

    def _resolve_step(self, step: str, phase: int, state: Dict[str, Any]):
        """Locate step definition, scope, and message."""
        try:
            from NL2DATA.orchestration.step_registry.registry import STEP_REGISTRY
            step_def = None
            for step_id, definition in STEP_REGISTRY.items():
                if definition.step_number == step:
                    step_def = definition
                    break
            if not step_def:
                return None, {}, f"Processing phase {phase}, step {step}"
            scope = self._extract_scope(state, step_def)
            message = self.get_message(step_def.step_id, scope)
            return step_def, scope, message
        except Exception:
            return None, {}, f"Processing phase {phase}, step {step}"

    def _build_summary(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Lightweight summary of key outputs for the frontend.
        
        Keep it compact: small samples and counts only.
        """
        summary: Dict[str, Any] = {}
        try:
            entities = state.get("entities")
            if isinstance(entities, list):
                summary["entities_count"] = len(entities)
                summary["entities_sample"] = [e.get("name") for e in entities if isinstance(e, dict) and e.get("name")][:3]
            
            relations = state.get("relations")
            if isinstance(relations, list):
                summary["relations_count"] = len(relations)
                # Build a small sample of relation strings
                rel_samples = []
                for rel in relations[:3]:
                    if isinstance(rel, dict):
                        ents = rel.get("entities") or []
                        rel_type = rel.get("type") or ""
                        rel_samples.append({"entities": ents, "type": rel_type})
                    elif hasattr(rel, "entities"):
                        rel_samples.append({"entities": getattr(rel, "entities"), "type": getattr(rel, "type", "")})
                summary["relations_sample"] = rel_samples
            
            attributes = state.get("attributes")
            if isinstance(attributes, dict):
                summary["attribute_entities"] = len(attributes)
                # Sample one entity's attributes
                first_entity = next(iter(attributes.keys()), None)
                if first_entity:
                    attrs = attributes.get(first_entity)
                    if isinstance(attrs, list):
                        summary["attributes_sample"] = {
                            "entity": first_entity,
                            "attributes": [a.get("name") for a in attrs if isinstance(a, dict) and a.get("name")][:3]
                        }
            
            generation_strategies = state.get("generation_strategies")
            if isinstance(generation_strategies, dict):
                summary["generation_strategies_entities"] = len(generation_strategies)
        except Exception:
            pass
        return summary
    
    def _extract_scope(self, state: Dict[str, Any], step_def) -> Dict[str, Any]:
        """Extract entity/attribute/relation from state based on step type.
        
        For high-fanout steps (PER_ENTITY, PER_ATTRIBUTE, etc.), extracts
        the current scope from the state structure.
        """
        from NL2DATA.orchestration.step_registry.types import CallType
        
        scope = {
            "entity": None, 
            "attribute": None, 
            "relation": None,
            "information_need": None,
            "constraint": None
        }
        
        # Extract scope based on call_type
        call_type = step_def.call_type if hasattr(step_def, 'call_type') else None
        
        if call_type == CallType.PER_ENTITY:
            # For PER_ENTITY steps, try to extract entity from state
            # Check if state has entity-specific updates
            if "entity_cardinalities" in state and isinstance(state["entity_cardinalities"], dict):
                # Get the most recently added entity (last key in dict)
                entity_names = list(state["entity_cardinalities"].keys())
                if entity_names:
                    scope["entity"] = entity_names[-1]
            elif "attributes" in state and isinstance(state["attributes"], dict):
                # Attributes dict is keyed by entity name
                entity_names = list(state["attributes"].keys())
                if entity_names:
                    scope["entity"] = entity_names[-1]
            elif "primary_keys" in state and isinstance(state["primary_keys"], dict):
                entity_names = list(state["primary_keys"].keys())
                if entity_names:
                    scope["entity"] = entity_names[-1]
            elif "entities" in state and isinstance(state["entities"], list) and state["entities"]:
                # If entities list exists, use the last one (most recent)
                last_entity = state["entities"][-1]
                if isinstance(last_entity, dict):
                    scope["entity"] = last_entity.get("name")
                elif hasattr(last_entity, "name"):
                    scope["entity"] = last_entity.name
            elif "metadata" in state:
                # Check metadata for current entity being processed
                metadata = state["metadata"]
                if "current_entity" in metadata:
                    scope["entity"] = metadata["current_entity"]
                elif "processing_entity" in metadata:
                    scope["entity"] = metadata["processing_entity"]
        
        elif call_type == CallType.PER_RELATION:
            # For PER_RELATION steps, extract relation info
            if "relation_cardinalities" in state and isinstance(state["relation_cardinalities"], dict):
                relation_ids = list(state["relation_cardinalities"].keys())
                if relation_ids:
                    # Relation ID is typically "Entity1+Entity2"
                    relation_id = relation_ids[-1]
                    scope["relation"] = relation_id
            elif "relations" in state and isinstance(state["relations"], list) and state["relations"]:
                last_relation = state["relations"][-1]
                if isinstance(last_relation, dict):
                    entities = last_relation.get("entities", [])
                    if entities:
                        scope["relation"] = "+".join(sorted(entities))
                elif hasattr(last_relation, "entities"):
                    entities = last_relation.entities
                    if entities:
                        scope["relation"] = "+".join(sorted(entities))
            elif "metadata" in state:
                metadata = state["metadata"]
                if "current_relation" in metadata:
                    scope["relation"] = metadata["current_relation"]
        
        elif call_type == CallType.PER_ATTRIBUTE:
            # For PER_ATTRIBUTE steps, extract attribute info
            if "metadata" in state:
                metadata = state["metadata"]
                if "current_attribute" in metadata:
                    scope["attribute"] = metadata["current_attribute"]
                    scope["entity"] = metadata.get("current_entity")
                elif "processing_attribute" in metadata:
                    scope["attribute"] = metadata["processing_attribute"]
                    scope["entity"] = metadata.get("processing_entity")
        
        elif call_type == CallType.PER_INFORMATION_NEED:
            # For PER_INFORMATION_NEED steps
            if "information_needs" in state and isinstance(state["information_needs"], list):
                if state["information_needs"]:
                    last_need = state["information_needs"][-1]
                    if isinstance(last_need, dict):
                        # Extract description or ID for information need
                        need_desc = last_need.get("description", "")
                        if need_desc:
                            scope["information_need"] = need_desc[:50]  # Truncate for display
                        elif "id" in last_need:
                            scope["information_need"] = f"need {last_need['id']}"
            elif "metadata" in state:
                metadata = state["metadata"]
                if "current_information_need" in metadata:
                    scope["information_need"] = metadata["current_information_need"]
        
        elif call_type == CallType.PER_CONSTRAINT:
            # For PER_CONSTRAINT steps
            if "metadata" in state:
                metadata = state["metadata"]
                if "current_constraint" in metadata:
                    constraint_info = metadata["current_constraint"]
                    if isinstance(constraint_info, dict):
                        scope["constraint"] = constraint_info.get("name") or constraint_info.get("type", "constraint")
                    else:
                        scope["constraint"] = str(constraint_info)
                elif "processing_constraint" in metadata:
                    scope["constraint"] = metadata["processing_constraint"]
        
        elif call_type in [
            CallType.PER_ATTRIBUTE,
            CallType.PER_TEXT_ATTRIBUTE,
            CallType.PER_NUMERIC_ATTRIBUTE,
            CallType.PER_BOOLEAN_ATTRIBUTE,
            CallType.PER_TEMPORAL_ATTRIBUTE,
            CallType.PER_DERIVED_ATTRIBUTE,
            CallType.PER_CATEGORICAL_ATTRIBUTE
        ]:
            # For PER_ATTRIBUTE and variants, extract attribute and entity
            if "metadata" in state:
                metadata = state["metadata"]
                if "current_attribute" in metadata:
                    scope["attribute"] = metadata["current_attribute"]
                    scope["entity"] = metadata.get("current_entity")
                elif "processing_attribute" in metadata:
                    scope["attribute"] = metadata["processing_attribute"]
                    scope["entity"] = metadata.get("processing_entity")
            
            # Also try to extract from attributes dict structure
            if "attributes" in state and isinstance(state["attributes"], dict):
                # Find the most recently updated attribute
                for entity_name, attr_list in state["attributes"].items():
                    if attr_list and isinstance(attr_list, list):
                        last_attr = attr_list[-1]
                        if isinstance(last_attr, dict):
                            scope["attribute"] = last_attr.get("name")
                            scope["entity"] = entity_name
                            break
        
        # Check metadata for explicit scope information (highest priority)
        if "metadata" in state:
            metadata = state["metadata"]
            if "scope" in metadata and isinstance(metadata["scope"], dict):
                # Explicit scope override
                scope.update(metadata["scope"])
        
        return scope



