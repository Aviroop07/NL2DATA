"""Context manager for token budget allocation and compression."""

from typing import Dict, Any, List, Optional
import tiktoken

from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


class ContextManager:
    """Manage context to prevent token overflow.
    
    Allocates token budgets across different context categories and
    compresses context when needed to fit within limits.
    """
    
    def __init__(
        self,
        total_limit: int = 100000,
        model: str = "gpt-4o"
    ):
        """
        Initialize context manager.
        
        Args:
            total_limit: Total token limit (default 100k)
            model: Model name for token encoding
        """
        self.budget = {
            "current_step": int(total_limit * 0.30),  # 30%
            "recent_phase": int(total_limit * 0.25),  # 25%
            "earlier_phases": int(total_limit * 0.25),  # 25%
            "external": int(total_limit * 0.15),  # 15%
            "buffer": int(total_limit * 0.05),  # 5% buffer
        }
        
        try:
            self.encoder = tiktoken.encoding_for_model(model)
        except KeyError:
            # Fallback to cl100k_base (GPT-4 encoding)
            self.encoder = tiktoken.get_encoding("cl100k_base")
            logger.warning(f"Unknown model '{model}', using cl100k_base encoding")
    
    def count_tokens(self, text: str) -> int:
        """Count tokens in text."""
        return len(self.encoder.encode(text))
    
    def prepare_context(
        self,
        current_step: str,
        phase_outputs: Dict[str, Any],
        external_context: Optional[Dict[str, Any]] = None,
        requires_enhanced_context: bool = False
    ) -> Dict[str, Any]:
        """
        Prepare context respecting token budgets.
        
        Args:
            current_step: Current step identifier (e.g., "3.1", "2.14")
            phase_outputs: All phase outputs
            external_context: External context (DSL grammar, catalogs, etc.)
            requires_enhanced_context: If True, provide comprehensive schema state
            
        Returns:
            Prepared context dictionary
        """
        prepared = {}
        
        if requires_enhanced_context:
            # Enhanced context: build comprehensive schema state
            comprehensive_schema = self._build_comprehensive_schema(phase_outputs)
            # Allocate more budget for comprehensive context
            comprehensive_budget = int(self.budget["current_step"] * 1.5)  # 50% more
            prepared["comprehensive_schema"] = self._fit_to_budget(
                comprehensive_schema,
                comprehensive_budget
            )
        else:
            # Standard context: current step only
            step_context = self._get_step_context(current_step, phase_outputs)
            prepared["step_context"] = self._fit_to_budget(
                step_context,
                self.budget["current_step"]
            )
        
        # Recent phase: summarize if needed
        recent = phase_outputs.get("phase_current", {})
        prepared["recent_phase"] = self._summarize_phase(
            recent,
            self.budget["recent_phase"]
        )
        
        # Earlier phases: aggressive summarization
        earlier = {
            k: v for k, v in phase_outputs.items()
            if k.startswith("phase_") and k != "phase_current"
        }
        prepared["earlier_phases"] = self._summarize_multiple_phases(
            earlier,
            self.budget["earlier_phases"]
        )
        
        # External: trim to essentials
        if external_context:
            prepared["external"] = self._trim_external(
                external_context,
                self.budget["external"]
            )
        
        return prepared
    
    def _build_comprehensive_schema(self, phase_outputs: Dict[str, Any]) -> Dict[str, Any]:
        """Build comprehensive schema state from all phase outputs."""
        schema = {
            "entities": [],
            "relations": [],
            "attributes": {},
            "primary_keys": {},
            "foreign_keys": [],
            "constraints": [],
            "data_types": {},
            "domain": None
        }
        
        # Phase 1: Entities, relations, domain
        if "phase_1" in phase_outputs:
            p1 = phase_outputs["phase_1"]
            schema["entities"] = p1.get("entities", [])
            schema["relations"] = p1.get("relations", [])
            schema["domain"] = p1.get("domain")
        
        # Phase 2: Attributes, PKs, FKs, constraints
        if "phase_2" in phase_outputs:
            p2 = phase_outputs["phase_2"]
            schema["attributes"] = p2.get("attributes", {})
            schema["primary_keys"] = p2.get("primary_keys", {})
            schema["foreign_keys"] = p2.get("foreign_keys", [])
            schema["constraints"].extend(p2.get("check_constraints", []))
        
        # Phase 3: Updated schema from refinement
        if "phase_3" in phase_outputs:
            p3 = phase_outputs["phase_3"]
            if "attributes" in p3:
                schema["attributes"].update(p3["attributes"])
        
        # Phase 4: Data types, categorical info
        if "phase_4" in phase_outputs:
            p4 = phase_outputs["phase_4"]
            schema["data_types"] = p4.get("data_types", {})
            schema["constraints"].extend(p4.get("check_constraints", []))
        
        return schema
    
    def _get_step_context(self, step: str, phase_outputs: Dict[str, Any]) -> Dict[str, Any]:
        """Get context for a specific step."""
        # Extract step number (e.g., "3.1" -> phase 3)
        phase_num = int(step.split(".")[0])
        phase_key = f"phase_{phase_num}"
        
        return phase_outputs.get(phase_key, {})
    
    def _fit_to_budget(self, content: Any, budget: int) -> Any:
        """Fit content to token budget by truncating if needed."""
        if isinstance(content, str):
            tokens = self.count_tokens(content)
            if tokens <= budget:
                return content
            
            # Truncate to fit budget (with some margin)
            target_tokens = int(budget * 0.9)  # 90% of budget
            encoded = self.encoder.encode(content)
            truncated = self.encoder.decode(encoded[:target_tokens])
            logger.warning(
                f"Truncated content from {tokens} to {target_tokens} tokens "
                f"to fit budget of {budget}"
            )
            return truncated
        
        elif isinstance(content, dict):
            # For dicts, summarize or truncate values
            result = {}
            total_tokens = 0
            for key, value in content.items():
                value_str = str(value)
                value_tokens = self.count_tokens(value_str)
                if total_tokens + value_tokens <= budget:
                    result[key] = value
                    total_tokens += value_tokens
                else:
                    # Truncate this value
                    remaining = budget - total_tokens
                    if remaining > 100:  # Only if meaningful space left
                        truncated = self._fit_to_budget(value_str, remaining)
                        result[key] = truncated
                    break
            return result
        
        elif isinstance(content, list):
            # For lists, keep first N items that fit
            result = []
            total_tokens = 0
            for item in content:
                item_str = str(item)
                item_tokens = self.count_tokens(item_str)
                if total_tokens + item_tokens <= budget:
                    result.append(item)
                    total_tokens += item_tokens
                else:
                    break
            return result
        
        return content
    
    def _summarize_phase(self, phase_output: Dict[str, Any], budget: int) -> Dict[str, Any]:
        """Summarize phase output to fit budget."""
        # Keep only essential fields
        summary = {}
        
        # Keep counts and key identifiers
        if "entities" in phase_output:
            entities = phase_output["entities"]
            summary["entity_count"] = len(entities)
            summary["entity_names"] = [
                e.get("name", "") if isinstance(e, dict) else getattr(e, "name", "")
                for e in entities[:10]  # First 10 only
            ]
        
        if "relations" in phase_output:
            relations = phase_output["relations"]
            summary["relation_count"] = len(relations)
            summary["relation_types"] = [
                r.get("type", "") if isinstance(r, dict) else getattr(r, "type", "")
                for r in relations[:10]
            ]
        
        # Fit to budget
        summary_str = str(summary)
        if self.count_tokens(summary_str) > budget:
            return self._fit_to_budget(summary_str, budget)
        
        return summary
    
    def _summarize_multiple_phases(
        self,
        phases: Dict[str, Dict[str, Any]],
        budget: int
    ) -> Dict[str, Any]:
        """Aggressively summarize multiple phases."""
        summary = {}
        
        for phase_key, phase_output in phases.items():
            # Very aggressive summarization - just counts
            phase_summary = {
                "entity_count": len(phase_output.get("entities", [])),
                "relation_count": len(phase_output.get("relations", [])),
                "attribute_count": sum(
                    len(attrs) for attrs in phase_output.get("attributes", {}).values()
                ),
            }
            summary[phase_key] = phase_summary
        
        # Fit to budget
        summary_str = str(summary)
        if self.count_tokens(summary_str) > budget:
            return self._fit_to_budget(summary_str, budget)
        
        return summary
    
    def _trim_external(
        self,
        external_context: Dict[str, Any],
        budget: int
    ) -> Dict[str, Any]:
        """Trim external context to essentials."""
        trimmed = {}
        
        # Keep only essential external context
        essential_keys = ["dsl_grammar", "generator_catalog", "categorical_definition"]
        
        for key in essential_keys:
            if key in external_context:
                value = external_context[key]
                value_str = str(value)
                if self.count_tokens(value_str) <= budget:
                    trimmed[key] = value
                else:
                    # Truncate
                    trimmed[key] = self._fit_to_budget(value_str, budget)
                    budget -= self.count_tokens(str(trimmed[key]))
        
        return trimmed


def prepare_context(
    current_step: str,
    phase_outputs: Dict[str, Any],
    external_context: Optional[Dict[str, Any]] = None,
    requires_enhanced_context: bool = False,
    total_limit: int = 100000,
    model: str = "gpt-4o"
) -> Dict[str, Any]:
    """
    Convenience function to prepare context.
    
    Args:
        current_step: Current step identifier
        phase_outputs: All phase outputs
        external_context: External context
        requires_enhanced_context: Whether to provide comprehensive schema
        total_limit: Total token limit
        model: Model name
        
    Returns:
        Prepared context dictionary
    """
    manager = ContextManager(total_limit=total_limit, model=model)
    return manager.prepare_context(
        current_step=current_step,
        phase_outputs=phase_outputs,
        external_context=external_context,
        requires_enhanced_context=requires_enhanced_context
    )

