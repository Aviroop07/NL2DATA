"""Numerical and categorical distribution strategies."""

from typing import List, Dict, Any, Optional
from pydantic import Field, field_validator
import numpy as np

from NL2DATA.phases.phase7.strategies.base import BaseGenerationStrategy


class NormalDistribution(BaseGenerationStrategy):
    """Normal (Gaussian) distribution strategy."""
    
    name: str = Field(default="normal", frozen=True)
    kind: str = Field(default="distribution", frozen=True)
    description: str = Field(
        default="Generate values from a normal (Gaussian) distribution. Use for attributes that follow a bell curve (e.g., heights, test scores, measurement errors).",
        frozen=True
    )
    
    mu: float = Field(description="Mean (center) of the distribution")
    sigma: float = Field(description="Standard deviation (spread), must be > 0")
    min: Optional[float] = Field(default=None, description="Optional minimum clamp")
    max: Optional[float] = Field(default=None, description="Optional maximum clamp")
    
    @field_validator("sigma")
    @classmethod
    def validate_sigma(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("sigma must be > 0")
        return v
    
    def generate(self, size: int) -> List[float]:
        """Generate normal distribution values."""
        values = np.random.normal(self.mu, self.sigma, size)
        if self.min is not None:
            values = np.clip(values, self.min, None)
        if self.max is not None:
            values = np.clip(values, None, self.max)
        return values.tolist()


class LognormalDistribution(BaseGenerationStrategy):
    """Log-normal distribution strategy."""
    
    name: str = Field(default="lognormal", frozen=True)
    kind: str = Field(default="distribution", frozen=True)
    description: str = Field(
        default="Generate values from a log-normal distribution. Use for positive attributes with multiplicative effects (e.g., transaction amounts, incomes, prices).",
        frozen=True
    )
    
    mu: float = Field(description="Mean of underlying normal (log-space)")
    sigma: float = Field(description="Standard deviation of underlying normal (log-space), must be > 0")
    min: float = Field(default=0.0, description="Minimum value (must be > 0)")
    max: Optional[float] = Field(default=None, description="Optional maximum clamp")
    
    @field_validator("sigma")
    @classmethod
    def validate_sigma(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("sigma must be > 0")
        return v
    
    @field_validator("min")
    @classmethod
    def validate_min(cls, v: float) -> float:
        if v < 0:
            raise ValueError("min must be >= 0")
        return v
    
    def generate(self, size: int) -> List[float]:
        """Generate log-normal distribution values."""
        values = np.random.lognormal(self.mu, self.sigma, size)
        if self.min > 0:
            values = np.clip(values, self.min, None)
        if self.max is not None:
            values = np.clip(values, None, self.max)
        return values.tolist()


class UniformDistribution(BaseGenerationStrategy):
    """Uniform distribution strategy."""
    
    name: str = Field(default="uniform", frozen=True)
    kind: str = Field(default="distribution", frozen=True)
    description: str = Field(
        default="Generate values from a uniform distribution. Use for attributes with no natural clustering (e.g., random IDs, quantities, percentages).",
        frozen=True
    )
    
    min: float = Field(description="Minimum value (inclusive)")
    max: float = Field(description="Maximum value (inclusive, must be > min)")
    
    @field_validator("max")
    @classmethod
    def validate_max(cls, v: float, info) -> float:
        if hasattr(info, "data") and "min" in info.data and v <= info.data["min"]:
            raise ValueError("max must be > min")
        return v
    
    def generate(self, size: int) -> List[float]:
        """Generate uniform distribution values."""
        values = np.random.uniform(self.min, self.max, size)
        return values.tolist()


class ParetoDistribution(BaseGenerationStrategy):
    """Pareto (power-law) distribution strategy."""
    
    name: str = Field(default="pareto", frozen=True)
    kind: str = Field(default="distribution", frozen=True)
    description: str = Field(
        default="Generate values from a Pareto (power-law) distribution. Use for attributes following power-law (e.g., wealth distribution, file sizes, city populations).",
        frozen=True
    )
    
    alpha: float = Field(description="Shape parameter (lower = heavier tail), must be > 0")
    scale: float = Field(description="Scale parameter (minimum value), must be > 0")
    max: Optional[float] = Field(default=None, description="Optional maximum clamp")
    
    @field_validator("alpha")
    @classmethod
    def validate_alpha(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("alpha must be > 0")
        return v
    
    @field_validator("scale")
    @classmethod
    def validate_scale(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("scale must be > 0")
        return v
    
    def generate(self, size: int) -> List[float]:
        """Generate Pareto distribution values."""
        values = np.random.pareto(self.alpha, size) * self.scale
        if self.max is not None:
            values = np.clip(values, None, self.max)
        return values.tolist()


class ZipfDistribution(BaseGenerationStrategy):
    """Zipfian distribution strategy for discrete values."""
    
    name: str = Field(default="zipf", frozen=True)
    kind: str = Field(default="distribution", frozen=True)
    description: str = Field(
        default="Generate discrete values from a Zipfian distribution. Use for discrete numeric attributes with popularity rankings (e.g., product rankings, page views).",
        frozen=True
    )
    
    n: int = Field(description="Number of distinct values (ranks 1..n), must be > 0")
    s: float = Field(description="Exponent (typically 1.0-2.0, higher = more skewed), must be > 0")
    
    @field_validator("n")
    @classmethod
    def validate_n(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("n must be > 0")
        return v
    
    @field_validator("s")
    @classmethod
    def validate_s(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("s must be > 0")
        return v
    
    def generate(self, size: int) -> List[int]:
        """Generate Zipfian distribution values."""
        ranks = np.arange(1, self.n + 1)
        probs = 1.0 / (ranks ** self.s)
        probs = probs / probs.sum()
        values = np.random.choice(ranks, size=size, p=probs)
        return values.tolist()


class ExponentialDistribution(BaseGenerationStrategy):
    """Exponential distribution strategy."""
    
    name: str = Field(default="exponential", frozen=True)
    kind: str = Field(default="distribution", frozen=True)
    description: str = Field(
        default="Generate values from an exponential distribution. Use for time-based attributes (e.g., inter-arrival times, session durations).",
        frozen=True
    )
    
    lambda_: float = Field(description="Rate parameter (1/mean), must be > 0", alias="lambda")
    min: float = Field(default=0.0, description="Minimum value (default 0)")
    max: Optional[float] = Field(default=None, description="Optional maximum clamp")
    
    @field_validator("lambda_")
    @classmethod
    def validate_lambda(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("lambda must be > 0")
        return v
    
    def generate(self, size: int) -> List[float]:
        """Generate exponential distribution values."""
        values = np.random.exponential(1.0 / self.lambda_, size)
        if self.min > 0:
            values = values + self.min
        if self.max is not None:
            values = np.clip(values, None, self.max)
        return values.tolist()


class CategoricalDistribution(BaseGenerationStrategy):
    """Categorical (discrete) distribution strategy."""
    
    name: str = Field(default="categorical", frozen=True)
    kind: str = Field(default="distribution", frozen=True)
    description: str = Field(
        default="Generate values from a categorical (discrete) distribution. Use for attributes with a fixed set of discrete values (e.g., status, category, type).",
        frozen=True
    )
    
    pmf: Dict[str, float] = Field(description="Probability mass function: dictionary mapping value (string) -> probability (float). Must sum to ~1.0.")
    
    @field_validator("pmf")
    @classmethod
    def validate_pmf(cls, v: Dict[str, float]) -> Dict[str, float]:
        if not v:
            raise ValueError("pmf cannot be empty")
        total = sum(v.values())
        if abs(total - 1.0) > 0.01:  # Allow small floating point errors
            raise ValueError(f"pmf probabilities must sum to ~1.0, got {total}")
        return v
    
    def generate(self, size: int) -> List[str]:
        """Generate categorical distribution values."""
        values = list(self.pmf.keys())
        probs = list(self.pmf.values())
        probs = np.array(probs)
        probs = probs / probs.sum()  # Normalize
        samples = np.random.choice(values, size=size, p=probs)
        return samples.tolist()


class BernoulliDistribution(BaseGenerationStrategy):
    """Bernoulli distribution strategy for boolean values."""
    
    name: str = Field(default="bernoulli", frozen=True)
    kind: str = Field(default="distribution", frozen=True)
    description: str = Field(
        default="Generate boolean values from a Bernoulli distribution. Use for boolean attributes (true/false, yes/no, 1/0).",
        frozen=True
    )
    
    p_true: float = Field(default=0.5, description="Probability of generating true (0.0 to 1.0, default 0.5)")
    
    @field_validator("p_true")
    @classmethod
    def validate_p_true(cls, v: float) -> float:
        if not 0.0 <= v <= 1.0:
            raise ValueError("p_true must be between 0.0 and 1.0")
        return v
    
    def generate(self, size: int) -> List[bool]:
        """Generate Bernoulli distribution values."""
        values = np.random.binomial(1, self.p_true, size).astype(bool)
        return values.tolist()

