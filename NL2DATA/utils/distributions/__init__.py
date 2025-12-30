"""Statistical distributions catalog for data generation.

This module provides a catalog of supported statistical distributions
that can be used for generating realistic data. The catalog can be
extended with new distributions as needed.
"""

from .catalog import (
    get_distribution_catalog,
    get_distribution_info,
    format_distributions_for_prompt,
    DistributionInfo,
    ParameterInfo,
    DistributionType,
)

__all__ = [
    "get_distribution_catalog",
    "get_distribution_info",
    "format_distributions_for_prompt",
    "DistributionInfo",
    "ParameterInfo",
    "DistributionType",
]

