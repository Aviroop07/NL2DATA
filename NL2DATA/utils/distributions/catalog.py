"""Statistical distributions catalog for numerical attributes.

This module defines the catalog of statistical distributions supported
for numerical data generation. New distributions can be added here.

These distributions are used for numerical/continuous attributes (INT, FLOAT, DECIMAL, etc.)
and return distribution type + parameters (e.g., {"type": "normal", "mean": 100, "std_dev": 15}).

For categorical attributes, use probability mappings instead (see Step 4.7).
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum


class DistributionType(str, Enum):
    """Types of statistical distributions for numerical attributes."""
    UNIFORM = "uniform"
    NORMAL = "normal"
    LOGNORMAL = "lognormal"
    BETA = "beta"
    GAMMA = "gamma"
    EXPONENTIAL = "exponential"
    TRIANGULAR = "triangular"
    WEIBULL = "weibull"
    POISSON = "poisson"  # For count data (integers)
    ZIPF = "zipf"  # For rank-frequency data (integers)


@dataclass
class ParameterInfo:
    """Information about a distribution parameter."""
    type: str  # "decimal", "integer", "array", "string"
    description: str


@dataclass
class DistributionInfo:
    """Information about a statistical distribution for numerical attributes."""
    name: str
    type: DistributionType
    description: str
    use_cases: List[str]
    parameters: Dict[str, ParameterInfo]  # Parameter name -> ParameterInfo (type + description)
    example_parameters: Dict[str, Any]  # Example parameter values
    notes: Optional[str] = None


# Distribution Catalog for Numerical Attributes
DISTRIBUTION_CATALOG: Dict[str, DistributionInfo] = {
    "uniform": DistributionInfo(
        name="Uniform Distribution",
        type=DistributionType.UNIFORM,
        description="All values in the range are equally likely. No bias toward any particular value.",
        use_cases=[
            "When no value is more likely than others within the range",
            "For attributes with no natural clustering",
            "When domain knowledge doesn't suggest any particular pattern",
            "IDs, random numbers, timestamps (if no patterns)",
        ],
        parameters={
            "min": ParameterInfo(type="decimal", description="Minimum value (inclusive)"),
            "max": ParameterInfo(type="decimal", description="Maximum value (inclusive)"),
        },
        example_parameters={"min": 0, "max": 100},
        notes="Simple distribution where every value in [min, max] has equal probability.",
    ),
    
    "normal": DistributionInfo(
        name="Normal (Gaussian) Distribution",
        type=DistributionType.NORMAL,
        description="Bell-shaped distribution centered around a mean value. Values cluster around the mean, with decreasing probability toward extremes.",
        use_cases=[
            "Heights, weights, measurements (natural clustering around average)",
            "Test scores, ratings (most values near average)",
            "Any attribute where values cluster around a central value",
            "Errors, deviations from expected values",
        ],
        parameters={
            "mean": ParameterInfo(type="decimal", description="Mean (center) of the distribution"),
            "std_dev": ParameterInfo(type="decimal", description="Standard deviation (spread) - larger values = more spread"),
        },
        example_parameters={"mean": 100, "std_dev": 15},
        notes="68% of values fall within 1 std_dev of mean, 95% within 2 std_dev. Use when data naturally clusters around a central value.",
    ),
    
    "lognormal": DistributionInfo(
        name="Log-Normal Distribution",
        type=DistributionType.LOGNORMAL,
        description="Right-skewed distribution where the logarithm of values follows a normal distribution. Heavy tail on the right side.",
        use_cases=[
            "Prices, amounts, incomes (many small values, few very large values)",
            "Transaction amounts (most transactions small, few very large)",
            "Response times, durations (most fast, few very slow)",
            "Any positive-valued attribute with heavy right tail",
        ],
        parameters={
            "mu": ParameterInfo(type="decimal", description="Mean of the underlying normal distribution (log space)"),
            "sigma": ParameterInfo(type="decimal", description="Standard deviation of the underlying normal distribution (log space)"),
        },
        example_parameters={"mu": 3.5, "sigma": 1.2},
        notes="Values are always positive. mu controls the center, sigma controls the tail length. Larger sigma = longer tail.",
    ),
    
    "beta": DistributionInfo(
        name="Beta Distribution",
        type=DistributionType.BETA,
        description="Flexible distribution bounded between 0 and 1. Can model various shapes: uniform, U-shaped, J-shaped, bell-shaped.",
        use_cases=[
            "Percentages, proportions, rates (0-1 range)",
            "Success rates, completion rates",
            "Confidence scores, probabilities",
            "Any attribute bounded between 0 and 1",
        ],
        parameters={
            "alpha": ParameterInfo(type="decimal", description="Shape parameter (controls left side)"),
            "beta": ParameterInfo(type="decimal", description="Shape parameter (controls right side)"),
        },
        example_parameters={"alpha": 2, "beta": 5},
        notes="When alpha = beta = 1: uniform. When alpha > beta: skewed left. When beta > alpha: skewed right. When both > 1: bell-shaped.",
    ),
    
    "gamma": DistributionInfo(
        name="Gamma Distribution",
        type=DistributionType.GAMMA,
        description="Right-skewed distribution for positive values. Flexible shape controlled by shape and scale parameters.",
        use_cases=[
            "Waiting times, durations (most short, few very long)",
            "Service times, processing times",
            "Amounts with minimum value > 0",
            "Any positive-valued attribute with right skew",
        ],
        parameters={
            "shape": ParameterInfo(type="decimal", description="Shape parameter (k) - controls the shape"),
            "scale": ParameterInfo(type="decimal", description="Scale parameter (theta) - controls the spread"),
        },
        example_parameters={"shape": 2, "scale": 1.5},
        notes="Mean = shape * scale. When shape = 1: exponential distribution. Larger shape = less skew.",
    ),
    
    "exponential": DistributionInfo(
        name="Exponential Distribution",
        type=DistributionType.EXPONENTIAL,
        description="Right-skewed distribution for positive values. Models time between events, waiting times.",
        use_cases=[
            "Time between events (e.g., time between transactions)",
            "Waiting times, inter-arrival times",
            "Lifetimes, failure times",
            "Any positive-valued attribute with exponential decay pattern",
        ],
        parameters={
            "lambda": ParameterInfo(type="decimal", description="Rate parameter (lambda) - higher lambda = faster decay"),
        },
        example_parameters={"lambda": 0.5},
        notes="Mean = 1/lambda. Special case of Gamma distribution with shape = 1. Most values are small, with exponential tail.",
    ),
    
    "triangular": DistributionInfo(
        name="Triangular Distribution",
        type=DistributionType.TRIANGULAR,
        description="Triangular-shaped distribution with a peak at the mode. Values cluster around the mode.",
        use_cases=[
            "When you know min, max, and most likely value",
            "Project estimates, durations with uncertainty",
            "Any attribute with a clear peak and bounded range",
        ],
        parameters={
            "min": ParameterInfo(type="decimal", description="Minimum value"),
            "max": ParameterInfo(type="decimal", description="Maximum value"),
            "mode": ParameterInfo(type="decimal", description="Most likely value (peak of triangle)"),
        },
        example_parameters={"min": 10, "max": 100, "mode": 50},
        notes="Mode must be between min and max. Probability increases linearly from min to mode, then decreases to max.",
    ),
    
    "weibull": DistributionInfo(
        name="Weibull Distribution",
        type=DistributionType.WEIBULL,
        description="Flexible distribution for positive values. Can model various failure patterns and lifetimes.",
        use_cases=[
            "Failure times, lifetimes",
            "Wind speeds, weather data",
            "Any positive-valued attribute with flexible shape",
        ],
        parameters={
            "shape": ParameterInfo(type="decimal", description="Shape parameter (k) - controls the shape"),
            "scale": ParameterInfo(type="decimal", description="Scale parameter (lambda) - controls the spread"),
        },
        example_parameters={"shape": 2, "scale": 10},
        notes="When shape = 1: exponential. When shape = 2: Rayleigh. Larger shape = less skew, more symmetric.",
    ),
    
    "poisson": DistributionInfo(
        name="Poisson Distribution",
        type=DistributionType.POISSON,
        description="Discrete distribution for count data (non-negative integers). Models number of events in a fixed interval.",
        use_cases=[
            "Count data (number of items, events, occurrences)",
            "Number of orders, transactions, clicks",
            "Any integer attribute representing counts",
        ],
        parameters={
            "lambda": ParameterInfo(type="decimal", description="Mean (and variance) of the distribution - average number of events"),
        },
        example_parameters={"lambda": 5},
        notes="Returns non-negative integers. Mean = variance = lambda. For large lambda, approximates normal distribution.",
    ),
    
    "zipf": DistributionInfo(
        name="Zipf Distribution",
        type=DistributionType.ZIPF,
        description="Discrete distribution for rank-frequency data. The k-th ranked value has frequency proportional to 1/k^s. Follows power law - few values are very common, most are rare.",
        use_cases=[
            "Popularity rankings (few very popular items, many unpopular items)",
            "Word frequencies, term frequencies",
            "Product popularity (few bestsellers, many niche products)",
            "Website visit counts (few popular pages, many rarely visited)",
            "User engagement levels (few power users, many casual users)",
            "Any integer attribute following power law / 80-20 rule",
        ],
        parameters={
            "s": ParameterInfo(type="decimal", description="Zipf exponent (shape parameter) - typically between 1 and 2. Higher s = steeper decline"),
            "n": ParameterInfo(type="integer", description="Number of possible values (upper bound for ranks)"),
        },
        example_parameters={"s": 1.5, "n": 1000},
        notes="The most common value (rank 1) has frequency ~1/1^s, second most common (rank 2) has frequency ~1/2^s, etc. Follows 80/20 rule: 20% of values account for 80% of occurrences. Larger s = more concentration in top values.",
    ),
}


def get_distribution_catalog() -> Dict[str, DistributionInfo]:
    """
    Get the complete distribution catalog for numerical attributes.
    
    Returns:
        dict: Dictionary mapping distribution names to DistributionInfo objects
    """
    return DISTRIBUTION_CATALOG.copy()


def get_distribution_info(distribution_name: str) -> Optional[DistributionInfo]:
    """
    Get information about a specific distribution.
    
    Args:
        distribution_name: Name of the distribution (e.g., "uniform", "normal")
        
    Returns:
        DistributionInfo if found, None otherwise
    """
    return DISTRIBUTION_CATALOG.get(distribution_name.lower())


def format_distributions_for_prompt(
    include_examples: bool = True,
) -> str:
    """
    Format the distribution catalog as a string for inclusion in LLM prompts.
    
    Args:
        include_examples: Whether to include example parameter values
        
    Returns:
        str: Formatted string describing available distributions
    """
    catalog = get_distribution_catalog()
    
    lines = ["SUPPORTED STATISTICAL DISTRIBUTIONS FOR NUMERICAL ATTRIBUTES:"]
    lines.append("=" * 80)
    lines.append("")
    lines.append("These distributions are used for numerical/continuous attributes (INT, FLOAT, DECIMAL, etc.).")
    lines.append("Return distribution_type and parameters (not probability mappings).")
    lines.append("")
    
    for name, info in catalog.items():
        lines.append(f"{info.name.upper()} (type: '{info.type.value}'):")
        lines.append(f"  Description: {info.description}")
        lines.append(f"  Use Cases:")
        for use_case in info.use_cases:
            lines.append(f"    - {use_case}")
        
        lines.append(f"  Required Parameters:")
        for param_name, param_info in info.parameters.items():
            lines.append(f"    - {param_name} ({param_info.type}): {param_info.description}")
        
        if include_examples:
            example_str = ", ".join(f"{k}={v}" for k, v in info.example_parameters.items())
            lines.append(f"  Example Parameters: {example_str}")
        
        if info.notes:
            lines.append(f"  Notes: {info.notes}")
        
        lines.append("")
    
    lines.append("=" * 80)
    lines.append("")
    lines.append("OUTPUT FORMAT:")
    lines.append("Return a JSON object with:")
    lines.append("  - distribution_type: One of the supported distribution types above")
    lines.append("  - parameters: Dictionary with parameter values (e.g., {{\"mean\": 100, \"std_dev\": 15}})")
    lines.append("  - min: Optional minimum value (for bounded distributions or truncation)")
    lines.append("  - max: Optional maximum value (for bounded distributions or truncation)")
    lines.append("  - reasoning: Explanation of why this distribution and parameters were chosen")
    lines.append("")
    lines.append("Example outputs:")
    lines.append("  - Normal: {{\"distribution_type\": \"normal\", \"parameters\": {{\"mean\": 100, \"std_dev\": 15}}, \"min\": 0, \"max\": 200}}")
    lines.append("  - Uniform: {{\"distribution_type\": \"uniform\", \"parameters\": {{\"min\": 0, \"max\": 1000}}}}")
    lines.append("  - LogNormal: {{\"distribution_type\": \"lognormal\", \"parameters\": {{\"mu\": 3.5, \"sigma\": 1.2}}, \"min\": 0.01}}")
    
    return "\n".join(lines)


def list_available_distributions() -> List[str]:
    """
    Get a list of all available distribution names.
    
    Returns:
        list: List of distribution names
    """
    return list(DISTRIBUTION_CATALOG.keys())
