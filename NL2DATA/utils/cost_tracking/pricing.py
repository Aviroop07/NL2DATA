"""Model pricing definitions for cost calculation."""

from dataclasses import dataclass
from enum import Enum


class ModelProvider(str, Enum):
    """LLM provider names."""
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    GOOGLE = "google"


@dataclass
class ModelPricing:
    """Pricing per 1M tokens for a model."""
    input_price: float  # per 1M input tokens
    output_price: float  # per 1M output tokens
    
    def calculate_cost(self, input_tokens: int, output_tokens: int) -> float:
        """Calculate cost for given token counts."""
        input_cost = (input_tokens / 1_000_000) * self.input_price
        output_cost = (output_tokens / 1_000_000) * self.output_price
        return input_cost + output_cost


# Pricing as of December 2024 (update as needed)
MODEL_PRICING = {
    "gpt-4o": ModelPricing(input_price=2.50, output_price=10.00),
    "gpt-4o-mini": ModelPricing(input_price=0.15, output_price=0.60),
    "gpt-4": ModelPricing(input_price=30.00, output_price=60.00),
    "gpt-3.5-turbo": ModelPricing(input_price=0.50, output_price=1.50),
    "claude-3-5-sonnet": ModelPricing(input_price=3.00, output_price=15.00),
    "claude-3-opus": ModelPricing(input_price=15.00, output_price=75.00),
    "gemini-1.5-pro": ModelPricing(input_price=1.25, output_price=5.00),
}

