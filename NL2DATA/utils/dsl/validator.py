"""Deterministic validation helpers for NL2DATA DSL.

This module currently provides syntactic validation (grammar) plus a small amount
of semantic validation where it is safe and deterministic:
- For distribution expressions `x ~ DIST(...)`, ensure `DIST` is one of the supported
  distribution generators that NL2DATA offers.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple

from lark import Tree, Token

from .parser import DSLParseError, parse_dsl_expression
from .grammar_profile import DSLGrammarProfile
from .schema_context import DSLSchemaContext
from .function_registry import get_distribution_registry, get_function_registry, supported_distribution_names, supported_function_names


_DIST_REGISTRY = get_distribution_registry()
_FUNC_REGISTRY = get_function_registry()
_SUPPORTED_DISTRIBUTIONS: Set[str] = set(_DIST_REGISTRY.keys())
_SUPPORTED_FUNCTIONS: Set[str] = set(_FUNC_REGISTRY.keys())
_DIST_ARITY: Dict[str, Tuple[int, int]] = {k: v.arity for k, v in _DIST_REGISTRY.items()}
_FUNC_ARITY: Dict[str, Tuple[int, int]] = {k: v.arity for k, v in _FUNC_REGISTRY.items()}


def get_supported_dsl_functions() -> List[str]:
    """Return the strict allowlist of supported DSL function names (sorted)."""
    return supported_function_names()


def get_supported_dsl_distributions() -> List[str]:
    """Return the allowlist of supported DSL distribution generator names (sorted)."""
    return supported_distribution_names()


def _count_arg_list_exprs(call_node: Tree) -> int:
    # call_node is func_call or dist_call: identifier LPAREN [arg_list] RPAREN
    arg_list_node = None
    for ch in call_node.children:
        if isinstance(ch, Tree) and ch.data == "arg_list":
            arg_list_node = ch
            break
    n_expr = 0
    if arg_list_node is not None:
        for c in arg_list_node.children:
            if isinstance(c, Tree):
                n_expr += 1
    return n_expr


def _validate_function_calls(tree: Tree) -> Optional[str]:
    """Validate func_call nodes against a strict allowlist and arity rules."""
    for fn_call in tree.find_data("func_call"):
        ident = None
        for ch in fn_call.children:
            if isinstance(ch, Tree) and ch.data == "identifier":
                ident = _identifier_tree_to_str(ch)
                break
        if not ident:
            return "DSL validation error: function call is missing a name"
        if "." in ident:
            return (
                "Unsupported function name in DSL: "
                f"'{ident}'. Function names must not be qualified (no dots)."
            )

        base = ident.split(".", 1)[0].strip().upper()
        if base not in _SUPPORTED_FUNCTIONS:
            return (
                "Unsupported function in DSL: "
                f"'{base}'. Supported: {sorted(_SUPPORTED_FUNCTIONS)}"
            )

        arity = _FUNC_ARITY.get(base)
        if arity is None:
            continue
        min_args, max_args = arity
        n_expr = _count_arg_list_exprs(fn_call)

        # Special-case: categorical is only valid as distribution via "~" (dist_call).
        # As a plain function call, we still enforce the same "pairs only" constraints,
        # because syntactically it is allowed and we want deterministic behavior.
        if base == "CATEGORICAL":
            # Reuse the same checks used for dist_call by calling _validate_distribution_calls
            # on a small subtree is awkward; just re-check here.
            # Expect arg_list of 2+ pair nodes.
            arg_list_node = None
            for ch in fn_call.children:
                if isinstance(ch, Tree) and ch.data == "arg_list":
                    arg_list_node = ch
                    break
            if arg_list_node is None:
                return "Invalid CATEGORICAL: expected 2+ (value, weight) pairs, got 0."
            pair_count = 0
            non_pair_count = 0
            for c in arg_list_node.children:
                if isinstance(c, Tree) and c.data == "pair":
                    pair_count += 1
                elif isinstance(c, Tree):
                    non_pair_count += 1
            if pair_count < 2:
                return f"Invalid CATEGORICAL: expected at least 2 (value, weight) pairs, got {pair_count}."
            if non_pair_count > 0:
                return "Invalid CATEGORICAL: all arguments must be (value, weight) pairs."
            continue

        if not (min_args <= n_expr <= max_args):
            return (
                f"Invalid argument count for function '{base}': got {n_expr}, expected {min_args}"
                + (f"..{max_args}" if max_args != min_args else "")
            )
    return None


def _identifier_tree_to_str(t: Tree) -> str:
    # identifier: CNAME ("." CNAME)*
    parts = []
    for c in t.children:
        if isinstance(c, Token):
            parts.append(str(c.value))
        elif isinstance(c, Tree):
            # Defensive: flatten any nested structures by extracting tokens
            parts.extend([str(x.value) for x in c.scan_values(lambda v: isinstance(v, Token))])
    return ".".join([p for p in parts if p])


def _validate_distribution_calls(tree: Tree) -> Optional[str]:
    """Return error string if a distribution call is not supported; otherwise None."""
    for dist_call in tree.find_data("dist_call"):
        # dist_call: identifier "(" [arg_list] ")"
        ident = None
        for ch in dist_call.children:
            if isinstance(ch, Tree) and ch.data == "identifier":
                ident = _identifier_tree_to_str(ch)
                break
        if not ident:
            return "DSL validation error: distribution call is missing a name"
        if "." in ident:
            return (
                "Unsupported distribution name in DSL: "
                f"'{ident}'. Distribution function names must not be qualified (no dots)."
            )
        base = ident.split(".", 1)[0].strip().upper()
        if base not in _SUPPORTED_DISTRIBUTIONS:
            return (
                "Unsupported distribution function in DSL: "
                f"'{base}'. Supported: {sorted(_SUPPORTED_DISTRIBUTIONS)}"
            )

        # Enforce signatures (arity) when known.
        arity = _DIST_ARITY.get(base)
        if arity is None:
            continue

        min_args, max_args = arity
        # dist_call: identifier LPAREN [arg_list] RPAREN
        arg_list_node = None
        for ch in dist_call.children:
            if isinstance(ch, Tree) and ch.data == "arg_list":
                arg_list_node = ch
                break
        # arg_list: expr (COMMA expr)*
        # In the tree, children are expr nodes (commas are tokens but Lark usually drops literals;
        # since we tokenized COMMA explicitly, it will appear as Token('COMMA', ',') in children.
        n_expr = 0
        if arg_list_node is not None:
            for c in arg_list_node.children:
                if isinstance(c, Tree):
                    n_expr += 1

        # Special-case: categorical requires that every argument is a pair literal.
        if base == "CATEGORICAL":
            if arg_list_node is None:
                return "Invalid CATEGORICAL: expected 2+ (value, weight) pairs, got 0."
            pair_count = 0
            non_pair_count = 0
            for c in arg_list_node.children:
                if isinstance(c, Tree) and c.data == "pair":
                    pair_count += 1
                elif isinstance(c, Tree):
                    non_pair_count += 1
            if pair_count < 2:
                return f"Invalid CATEGORICAL: expected at least 2 (value, weight) pairs, got {pair_count}."
            if non_pair_count > 0:
                return "Invalid CATEGORICAL: all arguments must be (value, weight) pairs."
            continue

        if not (min_args <= n_expr <= max_args):
            return (
                f"Invalid argument count for distribution '{base}': got {n_expr}, expected {min_args}"
                + (f"..{max_args}" if max_args != min_args else "")
            )
    return None


def validate_dsl_expression_strict(dsl: str) -> Dict[str, Any]:
    """Validate DSL expression against the formal grammar.

    Returns a tool-friendly dict:
      {"valid": bool, "error": Optional[str]}
    """
    try:
        tree = parse_dsl_expression(dsl)
        err = _validate_distribution_calls(tree)
        if err:
            return {"valid": False, "error": err}
        err2 = _validate_function_calls(tree)
        if err2:
            return {"valid": False, "error": err2}
        return {"valid": True, "error": None}
    except DSLParseError as e:
        return {"valid": False, "error": str(e)}
    except Exception as e:
        return {"valid": False, "error": f"DSL validation error: {e}"}


def validate_dsl_expression(
    dsl: str,
    grammar: Optional[str] = None,
) -> Dict[str, Any]:
    """Validate DSL expression.

    Note:
    - `grammar` is reserved for future use (e.g., swapping grammars by phase).
    - For now, we validate against NL2DATA's built-in DSL grammar.
    """
    # Support a controlled "profile:" grammar selector without exposing internals everywhere.
    # Examples:
    # - "profile:v1" (default)
    # - "profile:v1+between+is_null"
    profile = None
    if isinstance(grammar, str) and grammar.startswith("profile:"):
        spec = grammar[len("profile:") :].strip()
        parts = [p for p in spec.split("+") if p]
        if parts:
            version = parts[0]
            feats = frozenset(parts[1:])
            profile = DSLGrammarProfile(version=version, features=feats)

    try:
        tree = parse_dsl_expression(dsl, profile=profile)
        err = _validate_distribution_calls(tree)
        if err:
            return {"valid": False, "error": err}
        err2 = _validate_function_calls(tree)
        if err2:
            return {"valid": False, "error": err2}
        return {"valid": True, "error": None}
    except DSLParseError as e:
        return {"valid": False, "error": str(e)}
    except Exception as e:
        return {"valid": False, "error": f"DSL validation error: {e}"}


def validate_dsl_expression_with_schema(
    dsl: str,
    schema: DSLSchemaContext,
    grammar: Optional[str] = None,
) -> Dict[str, Any]:
    """Validate DSL expression with schema-aware semantic checks.

    The user requested: "full schema: tables, attributes, attribute type. That's it."

    Semantic checks included (deterministic):
    - Identifier resolution:
      - Unknown columns/tables are rejected.
      - Bare column names that exist in multiple tables are rejected (must use Table.column).
      - Identifiers deeper than Table.column are rejected.
    - Lightweight type checks:
      - Numeric-only operators (+,-,*,/,%) require numeric operands when operand types are known.
      - AND/OR/NOT require boolean operands when operand types are known.
      - LIKE requires string operands when operand types are known.
      - IN list elements should match LHS type when both are known.
    - Distribution target checks:
      - `x ~ DIST(...)` requires x to be a column identifier resolvable against schema.
      - Numeric distributions require numeric target columns when target type is known.
      - BERNOULLI allows boolean/numeric targets.

    Notes:
    - We keep this conservative: if a type cannot be inferred, we do not fail unless the identifier itself is invalid.
    - `grammar` is reserved for future use (kept for signature compatibility).
    """
    _ = grammar

    strict = validate_dsl_expression(dsl, grammar=grammar)
    if not strict.get("valid", False):
        return strict

    # If caller passes a profile via `grammar`, parse with that same profile.
    profile = None
    if isinstance(grammar, str) and grammar.startswith("profile:"):
        spec = grammar[len("profile:") :].strip()
        if spec:
            parts = [p for p in spec.split("+") if p]
            version = parts[0]
            feats = frozenset(parts[1:])
            profile = DSLGrammarProfile(version=version, features=feats)

    try:
        tree = parse_dsl_expression(dsl, profile=profile)
    except DSLParseError as e:
        return {"valid": False, "error": str(e)}
    except Exception as e:
        return {"valid": False, "error": f"DSL validation error: {e}"}

    errors: List[str] = []

    def _is_tok(x: Any, typ: str) -> bool:
        return isinstance(x, Token) and x.type == typ

    def _parse_number_literal(node: Any) -> Optional[float]:
        if isinstance(node, Tree) and node.data == "number" and node.children:
            tok = node.children[0]
            if isinstance(tok, Token):
                try:
                    return float(str(tok.value))
                except Exception:
                    return None
        return None

    def _is_integer_literal(node: Any) -> bool:
        val = _parse_number_literal(node)
        if val is None:
            return False
        return float(val).is_integer()

    def _infer_literal_type(node: Any) -> str:
        if isinstance(node, Tree):
            if node.data == "number":
                return "number"
            if node.data == "string":
                return "string"
            if node.data in {"true", "false"}:
                return "boolean"
            if node.data == "null":
                return "null"
        return "unknown"

    def _infer_expr_type(node: Any) -> str:
        lit = _infer_literal_type(node)
        if lit != "unknown":
            return lit

        if isinstance(node, Tree):
            if node.data == "identifier":
                ident = _identifier_tree_to_str(node)
                _, _, t, err = schema.resolve_identifier(ident)
                if err:
                    errors.append(err)
                    return "unknown"
                return t or "unknown"

            if node.data == "unary":
                # unary: (PLUS|MINUS) factor
                op = node.children[0] if node.children else None
                rhs = node.children[1] if len(node.children) > 1 else None
                rhs_t = _infer_expr_type(rhs)
                if _is_tok(op, "MINUS") and rhs_t not in {"number", "unknown", "null"}:
                    errors.append(f"Unary '-' expects number, got {rhs_t}")
                # unary plus/minus yields numeric if rhs is numeric; else unknown
                return "number" if rhs_t == "number" else "unknown"

            if node.data == "list":
                # list: [arg_list]
                elem_types: List[str] = []
                for ch in node.children:
                    if isinstance(ch, Tree) and ch.data == "arg_list":
                        for a in ch.children:
                            if isinstance(a, Tree):
                                elem_types.append(_infer_expr_type(a))
                known = [t for t in elem_types if t not in {"unknown", "null"}]
                return known[0] if known and all(t == known[0] for t in known) else "unknown"

            if node.data == "pair":
                # pair: (expr, expr)
                return "unknown"

            if node.data == "func_call":
                # func_call: identifier "(" [arg_list] ")"
                fname = None
                if node.children and isinstance(node.children[0], Tree) and node.children[0].data == "identifier":
                    fname = _identifier_tree_to_str(node.children[0]).split(".", 1)[0].upper()
                args: List[Any] = []
                for ch in node.children:
                    if isinstance(ch, Tree) and ch.data == "arg_list":
                        for a in ch.children:
                            if isinstance(a, Tree):
                                args.append(a)
                arg_types = [_infer_expr_type(a) for a in args]

                if fname in {"LOWER", "UPPER", "TRIM", "LTRIM", "RTRIM"}:
                    if arg_types and arg_types[0] not in {"string", "unknown", "null"}:
                        errors.append(f"{fname} expects string argument, got {arg_types[0]}")
                    return "string"
                if fname in {"CONCAT"}:
                    return "string"
                if fname in {"LENGTH"}:
                    if arg_types and arg_types[0] not in {"string", "unknown", "null"}:
                        errors.append(f"{fname} expects string argument, got {arg_types[0]}")
                    return "number"
                if fname in {"REPLACE"}:
                    # REPLACE(str, from, to) -> string
                    if len(arg_types) >= 1 and arg_types[0] not in {"string", "unknown", "null"}:
                        errors.append(f"REPLACE expects string as first argument, got {arg_types[0]}")
                    if len(arg_types) >= 2 and arg_types[1] not in {"string", "unknown", "null"}:
                        errors.append(f"REPLACE expects string as second argument, got {arg_types[1]}")
                    if len(arg_types) >= 3 and arg_types[2] not in {"string", "unknown", "null"}:
                        errors.append(f"REPLACE expects string as third argument, got {arg_types[2]}")
                    return "string"
                if fname in {"SUBSTR", "SUBSTRING"}:
                    # SUBSTR(str, start[, len]) -> string
                    if len(arg_types) >= 1 and arg_types[0] not in {"string", "unknown", "null"}:
                        errors.append(f"{fname} expects string as first argument, got {arg_types[0]}")
                    if len(arg_types) >= 2 and arg_types[1] not in {"number", "unknown", "null"}:
                        errors.append(f"{fname} expects numeric start index as second argument, got {arg_types[1]}")
                    if len(arg_types) >= 3 and arg_types[2] not in {"number", "unknown", "null"}:
                        errors.append(f"{fname} expects numeric length as third argument, got {arg_types[2]}")
                    return "string"
                if fname in {"SPLIT_PART"}:
                    # SPLIT_PART(str, delim, index) -> string
                    if len(arg_types) >= 1 and arg_types[0] not in {"string", "unknown", "null"}:
                        errors.append(f"SPLIT_PART expects string as first argument, got {arg_types[0]}")
                    if len(arg_types) >= 2 and arg_types[1] not in {"string", "unknown", "null"}:
                        errors.append(f"SPLIT_PART expects string delimiter as second argument, got {arg_types[1]}")
                    if len(arg_types) >= 3 and arg_types[2] not in {"number", "unknown", "null"}:
                        errors.append(f"SPLIT_PART expects numeric index as third argument, got {arg_types[2]}")
                    return "string"
                if fname in {"REGEXP_EXTRACT"}:
                    # REGEXP_EXTRACT(str, pattern[, group]) -> string
                    if len(arg_types) >= 1 and arg_types[0] not in {"string", "unknown", "null"}:
                        errors.append(f"REGEXP_EXTRACT expects string as first argument, got {arg_types[0]}")
                    if len(arg_types) >= 2 and arg_types[1] not in {"string", "unknown", "null"}:
                        errors.append(f"REGEXP_EXTRACT expects string pattern as second argument, got {arg_types[1]}")
                    if len(arg_types) >= 3 and arg_types[2] not in {"number", "unknown", "null"}:
                        errors.append(f"REGEXP_EXTRACT expects numeric group index as third argument, got {arg_types[2]}")
                    return "string"
                if fname in {"ABS", "ROUND", "FLOOR", "CEIL", "CEILING"}:
                    if arg_types and arg_types[0] not in {"number", "unknown", "null"}:
                        errors.append(f"{fname} expects numeric argument, got {arg_types[0]}")
                    return "number"
                if fname in {"DATEADD"}:
                    # DATEADD(unit, value, datetime)
                    # We keep this minimal: unit should be string literal when known, value numeric, dt datetime/date/time.
                    if len(arg_types) >= 1 and arg_types[0] not in {"string", "unknown", "null"}:
                        errors.append(f"DATEADD expects string unit as first argument, got {arg_types[0]}")
                    if len(arg_types) >= 2 and arg_types[1] not in {"number", "unknown", "null"}:
                        errors.append(f"DATEADD expects numeric value as second argument, got {arg_types[1]}")
                    if len(arg_types) >= 3 and arg_types[2] not in {"datetime", "date", "time", "unknown", "null"}:
                        errors.append(f"DATEADD expects datetime/date/time as third argument, got {arg_types[2]}")
                    # Return same family as the 3rd arg if known
                    if len(arg_types) >= 3 and arg_types[2] in {"datetime", "date", "time"}:
                        return arg_types[2]
                    return "unknown"
                if fname in {"DATEDIFF"}:
                    # DATEDIFF(unit, start, end) -> number
                    if len(arg_types) >= 1 and arg_types[0] not in {"string", "unknown", "null"}:
                        errors.append(f"DATEDIFF expects string unit as first argument, got {arg_types[0]}")
                    if len(arg_types) >= 2 and arg_types[1] not in {"datetime", "date", "time", "unknown", "null"}:
                        errors.append(f"DATEDIFF expects datetime/date/time as second argument, got {arg_types[1]}")
                    if len(arg_types) >= 3 and arg_types[2] not in {"datetime", "date", "time", "unknown", "null"}:
                        errors.append(f"DATEDIFF expects datetime/date/time as third argument, got {arg_types[2]}")
                    return "number"
                if fname in {"COUNT", "SUM", "AVG", "MIN", "MAX"}:
                    # Aggregate functions return number (COUNT always numeric; MIN/MAX depend but keep numeric to be conservative)
                    return "number"
                if fname in {"COALESCE"}:
                    for t in arg_types:
                        if t not in {"unknown", "null"}:
                            return t
                    return "unknown"
                return "unknown"

            if node.data == "if_expr":
                # if_expr: IF expr THEN expr ELSE expr
                if len(node.children) >= 6:
                    cond = node.children[1]
                    then_ = node.children[3]
                    else_ = node.children[5]
                    t_cond = _infer_expr_type(cond)
                    if t_cond not in {"boolean", "unknown"}:
                        errors.append(f"IF condition expects boolean, got {t_cond}")
                    t_then = _infer_expr_type(then_)
                    t_else = _infer_expr_type(else_)
                    known = {t_then, t_else} - {"unknown", "null"}
                    if len(known) == 2 and t_then != t_else:
                        errors.append(f"IF branch type mismatch: then is {t_then}, else is {t_else}")
                    return t_then if t_then == t_else else "unknown"
                return "unknown"

            if node.data == "case_expr":
                # case_expr: CASE when_clause+ [ELSE expr] END
                # Conservative: ensure each WHEN condition is boolean if known; branch types align if known.
                branch_types: List[str] = []
                for wc in node.find_data("when_clause"):
                    # when_clause: WHEN expr THEN expr
                    if len(wc.children) >= 4:
                        t_cond = _infer_expr_type(wc.children[1])
                        if t_cond not in {"boolean", "unknown"}:
                            errors.append(f"CASE WHEN condition expects boolean, got {t_cond}")
                        branch_types.append(_infer_expr_type(wc.children[3]))
                # ELSE expr if present
                for i, ch in enumerate(node.children):
                    if _is_tok(ch, "ELSE") and i + 1 < len(node.children):
                        branch_types.append(_infer_expr_type(node.children[i + 1]))
                        break
                known = [t for t in branch_types if t not in {"unknown", "null"}]
                if known and any(t != known[0] for t in known):
                    errors.append(f"CASE branch type mismatch: {sorted(set(known))}")
                return known[0] if known else "unknown"

            if node.data == "distribution_expr":
                # distribution_expr: or_expr TILDE dist_call
                # Enforce target is a resolvable identifier.
                if len(node.children) >= 3:
                    target = node.children[0]
                    dist_call = node.children[2]
                    if not (isinstance(target, Tree) and target.data == "identifier"):
                        errors.append(
                            "Distribution target must be a column identifier (column or Table.column), not an expression."
                        )
                        return "unknown"
                    target_ident = _identifier_tree_to_str(target)
                    _, _, target_type, err = schema.resolve_identifier(target_ident)
                    if err:
                        errors.append(err)
                        target_type = "unknown"

                    dist_name = None
                    if isinstance(dist_call, Tree) and dist_call.data == "dist_call":
                        for ch in dist_call.children:
                            if isinstance(ch, Tree) and ch.data == "identifier":
                                dist_name = _identifier_tree_to_str(ch).split(".", 1)[0].upper()
                                break

                    numeric_required = {
                        "UNIFORM",
                        "NORMAL",
                        "LOGNORMAL",
                        "BETA",
                        "GAMMA",
                        "EXPONENTIAL",
                        "TRIANGULAR",
                        "WEIBULL",
                        "POISSON",
                        "ZIPF",
                        "PARETO",
                    }
                    if dist_name in numeric_required and target_type not in {"number", "unknown", "null"}:
                        errors.append(
                            f"Distribution {dist_name} requires numeric target column, got {target_type}"
                        )
                    if dist_name == "BERNOULLI" and target_type not in {"boolean", "number", "unknown", "null"}:
                        errors.append(
                            f"Distribution BERNOULLI requires boolean/numeric target column, got {target_type}"
                        )

                    # Literal-only argument bounds checks (safe + deterministic).
                    # We only enforce when we can parse numeric literals.
                    args: List[Tree] = []
                    if isinstance(dist_call, Tree) and dist_call.data == "dist_call":
                        for ch in dist_call.children:
                            if isinstance(ch, Tree) and ch.data == "arg_list":
                                for a in ch.children:
                                    if isinstance(a, Tree):
                                        args.append(a)
                                break

                    def num(i: int) -> Optional[float]:
                        if i < 0 or i >= len(args):
                            return None
                        return _parse_number_literal(args[i])

                    if dist_name == "UNIFORM":
                        a0, a1 = num(0), num(1)
                        if a0 is not None and a1 is not None and not (a0 < a1):
                            errors.append("Invalid UNIFORM(min, max): expected min < max.")
                    if dist_name == "NORMAL":
                        sigma = num(1)
                        if sigma is not None and not (sigma > 0):
                            errors.append("Invalid NORMAL(mean, std_dev): expected std_dev > 0.")
                    if dist_name == "LOGNORMAL":
                        sigma = num(1)
                        if sigma is not None and not (sigma > 0):
                            errors.append("Invalid LOGNORMAL(mu, sigma): expected sigma > 0.")
                    if dist_name == "BETA":
                        a, b = num(0), num(1)
                        if a is not None and not (a > 0):
                            errors.append("Invalid BETA(alpha, beta): expected alpha > 0.")
                        if b is not None and not (b > 0):
                            errors.append("Invalid BETA(alpha, beta): expected beta > 0.")
                    if dist_name == "GAMMA":
                        shape, scale = num(0), num(1)
                        if shape is not None and not (shape > 0):
                            errors.append("Invalid GAMMA(shape, scale): expected shape > 0.")
                        if scale is not None and not (scale > 0):
                            errors.append("Invalid GAMMA(shape, scale): expected scale > 0.")
                    if dist_name == "EXPONENTIAL":
                        lam = num(0)
                        if lam is not None and not (lam > 0):
                            errors.append("Invalid EXPONENTIAL(lambda): expected lambda > 0.")
                    if dist_name == "TRIANGULAR":
                        mn, mx, mode = num(0), num(1), num(2)
                        if mn is not None and mx is not None and not (mn < mx):
                            errors.append("Invalid TRIANGULAR(min, max, mode): expected min < max.")
                        if mn is not None and mode is not None and mx is not None:
                            if not (mn <= mode <= mx):
                                errors.append("Invalid TRIANGULAR(min, max, mode): expected min <= mode <= max.")
                    if dist_name == "WEIBULL":
                        shape, scale = num(0), num(1)
                        if shape is not None and not (shape > 0):
                            errors.append("Invalid WEIBULL(shape, scale): expected shape > 0.")
                        if scale is not None and not (scale > 0):
                            errors.append("Invalid WEIBULL(shape, scale): expected scale > 0.")
                    if dist_name == "POISSON":
                        lam = num(0)
                        if lam is not None and not (lam > 0):
                            errors.append("Invalid POISSON(lambda): expected lambda > 0.")
                    if dist_name == "ZIPF":
                        s = num(0)
                        if s is not None and not (s > 0):
                            errors.append("Invalid ZIPF(s, n): expected s > 0.")
                        if len(args) >= 2:
                            if _parse_number_literal(args[1]) is not None:
                                if not _is_integer_literal(args[1]) or _parse_number_literal(args[1]) < 1:
                                    errors.append("Invalid ZIPF(s, n): expected integer n >= 1.")
                    if dist_name == "PARETO":
                        alpha, scale = num(0), num(1)
                        if alpha is not None and not (alpha > 0):
                            errors.append("Invalid PARETO(alpha, scale): expected alpha > 0.")
                        if scale is not None and not (scale > 0):
                            errors.append("Invalid PARETO(alpha, scale): expected scale > 0.")
                    if dist_name == "BERNOULLI":
                        p = num(0)
                        if p is not None and not (0 <= p <= 1):
                            errors.append("Invalid BERNOULLI(p_true): expected 0 <= p_true <= 1.")
                    if dist_name == "CATEGORICAL":
                        # pair literals validated syntactically already; here we check numeric literal weights when possible.
                        weights: List[float] = []
                        any_negative = False
                        for a in args:
                            if isinstance(a, Tree) and a.data == "pair" and len(a.children) >= 5:
                                # pair: LPAREN expr COMMA expr RPAREN
                                w_expr = a.children[3]
                                w = _parse_number_literal(w_expr)
                                if w is not None:
                                    weights.append(w)
                                    if w < 0:
                                        any_negative = True
                        if any_negative:
                            errors.append("Invalid CATEGORICAL: weight literals must be >= 0.")
                        if weights and sum(weights) <= 0:
                            errors.append("Invalid CATEGORICAL: sum of weight literals must be > 0.")
                return "unknown"

            # Generic operator scanning (because many rules are inlined via '?' in grammar)
            # - AND/OR: require boolean
            # - Arithmetic: require numeric
            # - LIKE: require string
            # - Comparisons: check type compatibility when both known
            expr_children = [c for c in node.children if isinstance(c, Tree)]
            # Pre-compute types for tree children in order
            child_types = {id(c): _infer_expr_type(c) for c in expr_children}

            # Scan boolean connectors in this node's flat children list
            for i, ch in enumerate(node.children):
                if _is_tok(ch, "AND") or _is_tok(ch, "OR"):
                    # Find nearest Tree to the left and right
                    left = None
                    right = None
                    for j in range(i - 1, -1, -1):
                        if isinstance(node.children[j], Tree):
                            left = node.children[j]
                            break
                    for j in range(i + 1, len(node.children)):
                        if isinstance(node.children[j], Tree):
                            right = node.children[j]
                            break
                    if left is not None:
                        t = child_types.get(id(left), "unknown")
                        if t not in {"boolean", "unknown", "null"}:
                            errors.append(f"{ch.type} expects boolean left operand, got {t}")
                    if right is not None:
                        t = child_types.get(id(right), "unknown")
                        if t not in {"boolean", "unknown", "null"}:
                            errors.append(f"{ch.type} expects boolean right operand, got {t}")

                if _is_tok(ch, "PLUS") or _is_tok(ch, "MINUS") or _is_tok(ch, "MUL") or _is_tok(ch, "DIV") or _is_tok(ch, "MOD"):
                    left = None
                    right = None
                    for j in range(i - 1, -1, -1):
                        if isinstance(node.children[j], Tree):
                            left = node.children[j]
                            break
                    for j in range(i + 1, len(node.children)):
                        if isinstance(node.children[j], Tree):
                            right = node.children[j]
                            break
                    if left is not None:
                        t = child_types.get(id(left), "unknown")
                        if t not in {"number", "unknown", "null"}:
                            errors.append(f"Operator {ch.value} expects numeric left operand, got {t}")
                    if right is not None:
                        t = child_types.get(id(right), "unknown")
                        if t not in {"number", "unknown", "null"}:
                            errors.append(f"Operator {ch.value} expects numeric right operand, got {t}")

                if _is_tok(ch, "LIKE"):
                    left = None
                    right = None
                    for j in range(i - 1, -1, -1):
                        if isinstance(node.children[j], Tree):
                            left = node.children[j]
                            break
                    for j in range(i + 1, len(node.children)):
                        if isinstance(node.children[j], Tree):
                            right = node.children[j]
                            break
                    if left is not None:
                        t = child_types.get(id(left), "unknown")
                        if t not in {"string", "unknown", "null"}:
                            errors.append(f"LIKE expects string left operand, got {t}")
                    if right is not None:
                        t = child_types.get(id(right), "unknown")
                        if t not in {"string", "unknown", "null"}:
                            errors.append(f"LIKE expects string right operand, got {t}")

            # Scan comparison tails where available
            for i, ch in enumerate(node.children):
                if isinstance(ch, Tree) and ch.data == "cmp_tail":
                    # LHS is nearest Tree to the left
                    lhs = None
                    for j in range(i - 1, -1, -1):
                        if isinstance(node.children[j], Tree):
                            lhs = node.children[j]
                            break
                    if lhs is None:
                        continue
                    lhs_t = child_types.get(id(lhs), "unknown")
                    # Determine op + rhs
                    op_tok = None
                    rhs_node = None
                    for c in ch.children:
                        if isinstance(c, Token) and op_tok is None:
                            op_tok = c
                        if isinstance(c, Tree):
                            rhs_node = c
                    rhs_t = _infer_expr_type(rhs_node)
                    if op_tok and op_tok.type == "IN":
                        # rhs is list
                        if lhs_t not in {"unknown", "null"} and rhs_t not in {"unknown", "null"} and lhs_t != rhs_t:
                            errors.append(f"IN type mismatch: left is {lhs_t}, list elements are {rhs_t}")
                    elif op_tok and op_tok.type == "LIKE":
                        # Already handled above but keep safe
                        if lhs_t not in {"string", "unknown", "null"}:
                            errors.append(f"LIKE expects string left operand, got {lhs_t}")
                        if rhs_t not in {"string", "unknown", "null"}:
                            errors.append(f"LIKE expects string right operand, got {rhs_t}")
                    else:
                        # EQ/NE/LT/LE/GT/GE
                        known = {lhs_t, rhs_t} - {"unknown", "null"}
                        if len(known) == 2 and lhs_t != rhs_t:
                            errors.append(f"Type mismatch in comparison: left is {lhs_t}, right is {rhs_t}")
                        # Ordering comparisons shouldn't be used on booleans when known.
                        if op_tok and op_tok.type in {"LT", "LE", "GT", "GE"}:
                            if lhs_t == "boolean" or rhs_t == "boolean":
                                errors.append("Invalid ordering comparison on boolean values.")

        return "unknown"

    # Walk the tree and infer types; inference function appends to `errors` as needed.
    _infer_expr_type(tree)

    if errors:
        # Tool-friendly single error string
        return {"valid": False, "error": "; ".join(errors)}
    return {"valid": True, "error": None}

