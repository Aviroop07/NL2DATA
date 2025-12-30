"""Type definitions for step registry.

Defines enums and dataclasses for step metadata.
"""

from dataclasses import dataclass, field
from typing import List, Optional
from enum import Enum


class CallType(str, Enum):
    """How many times a step executes."""
    SINGULAR = "singular"  # Once per phase
    PER_ENTITY = "per_entity"  # Once per entity
    PER_ATTRIBUTE = "per_attribute"  # Once per attribute
    PER_RELATION = "per_relation"  # Once per relation
    PER_INFORMATION_NEED = "per_information_need"  # Once per information need
    PER_CONSTRAINT = "per_constraint"  # Once per constraint
    PER_TEXT_ATTRIBUTE = "per_text_attribute"  # Once per text attribute
    PER_NUMERIC_ATTRIBUTE = "per_numeric_attribute"  # Once per numeric attribute
    PER_BOOLEAN_ATTRIBUTE = "per_boolean_attribute"  # Once per boolean attribute
    PER_TEMPORAL_ATTRIBUTE = "per_temporal_attribute"  # Once per temporal attribute
    PER_DERIVED_ATTRIBUTE = "per_derived_attribute"  # Once per derived attribute
    PER_CATEGORICAL_ATTRIBUTE = "per_categorical_attribute"  # Once per categorical attribute
    LOOP = "loop"  # Iterative loop (max_iters applies)


class StepType(str, Enum):
    """Type of step execution."""
    LLM = "llm"  # Requires LLM API call
    DETERMINISTIC = "deterministic"  # Pure code, no LLM


@dataclass
class StepDefinition:
    """Canonical step definition. All step metadata lives here.
    
    This is the single source of truth for step information, eliminating
    inconsistencies in step numbering, call-type classification, and
    call-count estimates.
    """
    # Stable identifier (independent of numbering)
    step_id: str  # e.g., "P1_S1_DOMAIN_CONTEXT"
    
    # Human-readable info
    phase: int
    step_number: str  # e.g., "1.1" (cosmetic, can change)
    name: str
    
    # Execution metadata
    step_type: StepType  # LLM or deterministic
    call_type: CallType  # How many times it runs
    fanout_unit: str  # What the call_type multiplies by (e.g., "entity", "attribute")
    
    # Loop metadata (if call_type == LOOP)
    is_loop: bool = False
    max_iters: Optional[int] = None  # None = unbounded (dangerous, avoid)
    
    # Parallelization
    can_parallelize: bool = False  # True if instances can run in parallel
    
    # Dependencies (step_ids this step depends on)
    dependencies: List[str] = field(default_factory=list)
    
    # Cost estimation
    avg_tokens_per_call: int = 0  # Average tokens consumed per call (for budgeting)
    
    def __post_init__(self):
        """Validate step definition."""
        if self.call_type == CallType.LOOP and not self.is_loop:
            raise ValueError(
                f"Step {self.step_id}: call_type=LOOP but is_loop=False"
            )
        if self.is_loop and self.max_iters is None:
            raise ValueError(
                f"Step {self.step_id}: is_loop=True but max_iters=None "
                "(unbounded loops are dangerous)"
            )

