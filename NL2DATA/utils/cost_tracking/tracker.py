"""Cost tracker for LLM API calls."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime

from .pricing import ModelPricing, MODEL_PRICING
from NL2DATA.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class CostBudget:
    """Budget configuration."""
    total_budget: float = 10.0  # Total budget in USD
    warning_threshold: float = 0.8  # Warn at 80% of budget
    phase_budgets: Optional[Dict[int, float]] = None  # Per-phase limits
    
    def __post_init__(self):
        if self.phase_budgets is None:
            self.phase_budgets = {}


class BudgetExceededError(Exception):
    """Exception raised when budget is exceeded."""
    pass


@dataclass
class CostRecord:
    """Record of a single API call cost."""
    timestamp: datetime
    model: str
    phase: int
    step: str
    input_tokens: int
    output_tokens: int
    cost: float
    success: bool


class CostTracker:
    """Track and manage LLM API costs."""
    
    def __init__(self, budget: CostBudget):
        """
        Initialize cost tracker.
        
        Args:
            budget: Budget configuration
        """
        self.budget = budget
        self.records: List[CostRecord] = []
        self.total_cost: float = 0.0
        self.phase_costs: Dict[int, float] = {}
    
    def record_call(
        self,
        model: str,
        phase: int,
        step: str,
        input_tokens: int,
        output_tokens: int,
        success: bool = True
    ):
        """
        Record an API call and update costs.
        
        Args:
            model: Model name
            phase: Phase number
            step: Step identifier
            input_tokens: Input tokens used
            output_tokens: Output tokens used
            success: Whether call succeeded
        """
        pricing = MODEL_PRICING.get(model)
        if pricing:
            cost = pricing.calculate_cost(input_tokens, output_tokens)
        else:
            # Unknown model, estimate using gpt-4o-mini pricing
            pricing = MODEL_PRICING["gpt-4o-mini"]
            cost = pricing.calculate_cost(input_tokens, output_tokens)
            logger.warning(f"Unknown model '{model}', using gpt-4o-mini pricing")
        
        record = CostRecord(
            timestamp=datetime.now(),
            model=model,
            phase=phase,
            step=step,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost=cost,
            success=success
        )
        
        self.records.append(record)
        self.total_cost += cost
        self.phase_costs[phase] = self.phase_costs.get(phase, 0) + cost
        
        # Check budget
        if self.total_cost >= self.budget.total_budget * self.budget.warning_threshold:
            logger.warning(
                f"Cost warning: ${self.total_cost:.2f} spent "
                f"({self.total_cost/self.budget.total_budget*100:.1f}% of budget)"
            )
        
        if self.total_cost >= self.budget.total_budget:
            raise BudgetExceededError(
                f"Budget exceeded: ${self.total_cost:.2f} > ${self.budget.total_budget}"
            )
        
        # Check phase budget
        phase_budget = self.budget.phase_budgets.get(phase)
        if phase_budget and self.phase_costs[phase] >= phase_budget:
            logger.warning(
                f"Phase {phase} budget warning: ${self.phase_costs[phase]:.2f} spent "
                f"(budget: ${phase_budget})"
            )
    
    def get_summary(self) -> Dict[str, any]:
        """Get cost summary."""
        cost_by_model: Dict[str, float] = {}
        for record in self.records:
            cost_by_model[record.model] = cost_by_model.get(record.model, 0) + record.cost
        
        return {
            "total_cost": round(self.total_cost, 4),
            "budget": self.budget.total_budget,
            "remaining": round(self.budget.total_budget - self.total_cost, 4),
            "percentage_used": round(self.total_cost / self.budget.total_budget * 100, 1),
            "total_calls": len(self.records),
            "cost_by_phase": {phase: round(cost, 4) for phase, cost in self.phase_costs.items()},
            "cost_by_model": {model: round(cost, 4) for model, cost in cost_by_model.items()},
        }
    
    def estimate_remaining_cost(
        self,
        remaining_phases: List[int],
        entity_count: int = 0
    ) -> float:
        """
        Estimate cost for remaining phases.
        
        Args:
            remaining_phases: List of phase numbers remaining
            entity_count: Number of entities (for per-entity step estimation)
            
        Returns:
            Estimated cost in USD
        """
        # Rough estimates based on phase complexity
        PHASE_ESTIMATES = {
            1: 12, 2: 14, 3: 5, 4: 7, 5: 5, 6: 5, 7: 6
        }
        PER_ENTITY_PHASES = {2, 4, 7}
        
        estimated_calls = 0
        for phase in remaining_phases:
            base_calls = PHASE_ESTIMATES.get(phase, 5)
            if phase in PER_ENTITY_PHASES:
                estimated_calls += base_calls * entity_count
            else:
                estimated_calls += base_calls
        
        # Estimate average cost per call
        avg_cost_per_call = (
            self.total_cost / len(self.records)
            if self.records else 0.005
        )
        
        return estimated_calls * avg_cost_per_call

