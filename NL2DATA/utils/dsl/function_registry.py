"""Single source of truth for NL2DATA DSL functions + signatures.

User requirements:
- Strict allowlist (no unknown functions)
- When DSL is included in system prompt, include explicit function signatures
- In the future, adding a new DSL function should only require editing this registry
  (prompt spec and validator should update automatically).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass(frozen=True)
class DSLFunctionSpec:
    name: str
    # Signature string to show in prompts (parameter names are documentation-only)
    signature: str
    # (min_args, max_args)
    arity: Tuple[int, int]
    description: Optional[str] = None
    # Optional feature tag (for controllable enablement later)
    feature: Optional[str] = None


@dataclass(frozen=True)
class DSLDistributionSpec:
    name: str
    signature: str
    arity: Tuple[int, int]
    description: Optional[str] = None


def get_distribution_registry() -> Dict[str, DSLDistributionSpec]:
    # Numerical distributions catalog + generation strategies
    # NOTE: CATEGORICAL uses 2+ (value, weight) pairs: CATEGORICAL((v1,w1),(v2,w2),...)
    dists = [
        DSLDistributionSpec("UNIFORM", "UNIFORM(min, max)", (2, 2)),
        DSLDistributionSpec("NORMAL", "NORMAL(mean, std_dev)", (2, 2)),
        DSLDistributionSpec("LOGNORMAL", "LOGNORMAL(mu, sigma)", (2, 2)),
        DSLDistributionSpec("BETA", "BETA(alpha, beta)", (2, 2)),
        DSLDistributionSpec("GAMMA", "GAMMA(shape, scale)", (2, 2)),
        DSLDistributionSpec("EXPONENTIAL", "EXPONENTIAL(lambda)", (1, 1)),
        DSLDistributionSpec("TRIANGULAR", "TRIANGULAR(min, max, mode)", (3, 3)),
        DSLDistributionSpec("WEIBULL", "WEIBULL(shape, scale)", (2, 2)),
        DSLDistributionSpec("POISSON", "POISSON(lambda)", (1, 1)),
        DSLDistributionSpec("ZIPF", "ZIPF(s, n)", (2, 2)),
        DSLDistributionSpec("PARETO", "PARETO(alpha, scale)", (2, 2)),
        DSLDistributionSpec("BERNOULLI", "BERNOULLI(p_true)", (1, 1)),
        DSLDistributionSpec("CATEGORICAL", "CATEGORICAL((value, weight), (value, weight), ...)", (2, 1000000)),
    ]
    return {d.name: d for d in dists}


def get_function_registry() -> Dict[str, DSLFunctionSpec]:
    # NOTE: Distributions are also allowed as plain function calls (UNIFORM(...), etc.)
    dists = get_distribution_registry()

    funcs: List[DSLFunctionSpec] = [
        # String
        DSLFunctionSpec("LOWER", "LOWER(s)", (1, 1)),
        DSLFunctionSpec("UPPER", "UPPER(s)", (1, 1)),
        DSLFunctionSpec("TRIM", "TRIM(s)", (1, 1)),
        DSLFunctionSpec("LTRIM", "LTRIM(s)", (1, 1)),
        DSLFunctionSpec("RTRIM", "RTRIM(s)", (1, 1)),
        DSLFunctionSpec("CONCAT", "CONCAT(a, b, ...)", (2, 1000000)),
        DSLFunctionSpec("COALESCE", "COALESCE(a, b, ...)", (2, 1000000)),
        DSLFunctionSpec("NULLIF", "NULLIF(a, b)", (2, 2)),
        DSLFunctionSpec("LENGTH", "LENGTH(s)", (1, 1)),
        DSLFunctionSpec("REPLACE", "REPLACE(s, old, new)", (3, 3)),
        # SQL-like decomposition helpers (kept in DSL allowlist; still not "full SQL")
        DSLFunctionSpec("SUBSTR", "SUBSTR(s, start, [len])", (2, 3)),
        DSLFunctionSpec("SUBSTRING", "SUBSTRING(s, start, [len])", (2, 3)),
        DSLFunctionSpec("SPLIT_PART", "SPLIT_PART(s, delim, index)", (3, 3)),
        DSLFunctionSpec("REGEXP_EXTRACT", "REGEXP_EXTRACT(s, pattern, [group])", (2, 3)),
        DSLFunctionSpec("REGEXP_REPLACE", "REGEXP_REPLACE(s, pattern, replacement)", (3, 3)),
        # Casting / typing (type is a string literal)
        DSLFunctionSpec("CAST", "CAST(expr, type_name)", (2, 2)),
        # Numeric
        DSLFunctionSpec("ABS", "ABS(x)", (1, 1)),
        DSLFunctionSpec("ROUND", "ROUND(x, [digits])", (1, 2)),
        DSLFunctionSpec("FLOOR", "FLOOR(x)", (1, 1)),
        DSLFunctionSpec("CEIL", "CEIL(x)", (1, 1)),
        DSLFunctionSpec("CEILING", "CEILING(x)", (1, 1)),
        # Datetime
        DSLFunctionSpec("DATEADD", "DATEADD(unit, amount, ts)", (3, 3)),
        DSLFunctionSpec("DATEDIFF", "DATEDIFF(unit, start_ts, end_ts)", (3, 3)),
        DSLFunctionSpec("DATE_TRUNC", "DATE_TRUNC(unit, ts)", (2, 2)),
        DSLFunctionSpec("EXTRACT", "EXTRACT(part, ts)", (2, 2)),
        # Aggregates (query-level metrics)
        DSLFunctionSpec("COUNT", "COUNT(x)", (1, 1)),
        DSLFunctionSpec("SUM", "SUM(x)", (1, 1)),
        DSLFunctionSpec("AVG", "AVG(x)", (1, 1)),
        DSLFunctionSpec("MIN", "MIN(x)", (1, 1)),
        DSLFunctionSpec("MAX", "MAX(x)", (1, 1)),
    ]

    # Merge in distributions as functions too
    for d in dists.values():
        funcs.append(DSLFunctionSpec(d.name, d.signature, d.arity))

    return {f.name: f for f in funcs}


def supported_function_names() -> List[str]:
    return sorted(get_function_registry().keys())


def supported_distribution_names() -> List[str]:
    return sorted(get_distribution_registry().keys())


def supported_function_signatures() -> List[str]:
    reg = get_function_registry()
    return [reg[name].signature for name in sorted(reg.keys())]


def supported_distribution_signatures() -> List[str]:
    reg = get_distribution_registry()
    return [reg[name].signature for name in sorted(reg.keys())]

