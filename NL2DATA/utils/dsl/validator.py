"""Semantic analyzer for NL2DATA DSL.

This module provides semantic validation (type checking, semantic rules) for DSL expressions.
It uses the parser to obtain an AST, then validates it for semantic correctness.

Architecture:
- Lexer: Pure tokenization (lexer.py) - independent
- Parser: Uses grammar to parse tokens into AST (parser.py) - independent
- Semantic Analyzer: Validates AST for type/semantic correctness (this module) - uses parser

This module provides:
- Type checking (operand types, function argument types, etc.)
- Distribution parameter validation
- Function/distribution allowlist validation
- Identifier resolution and ambiguity checking
- Schema-aware validation
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple, Union

from lark import Tree, Token

from .parser import DSLParseError, parse_dsl_expression, parse_tokens
from .lexer import tokenize_dsl
from .grammar_profile import DSLGrammarProfile
from .schema_context import DSLSchemaContext
from .function_registry import get_distribution_registry, get_function_registry, supported_distribution_names, supported_function_names
from .models import ValidationResult, SemanticError, ErrorSeverity, ColumnBoundDSL
from .errors import SemanticErrorDetail


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


def _validate_function_calls(tree: Tree) -> Optional[SemanticErrorDetail]:
    """Validate func_call, aggregate_func_call, and window_func_call nodes against a strict allowlist and arity rules."""
    # Check all function call types
    for call_type in ["func_call", "aggregate_func_call", "window_func_call"]:
        for fn_call in tree.find_data(call_type):
            ident = None
            for ch in fn_call.children:
                if isinstance(ch, Tree) and ch.data == "identifier":
                    ident = _identifier_tree_to_str(ch)
                    break
            if not ident:
                return SemanticErrorDetail.create_invalid_function(
                    "Function call is missing a function name",
                    suggestion="Ensure function calls have the format: FUNCTION_NAME(arg1, arg2, ...)"
                )
            if "." in ident:
                return SemanticErrorDetail.create_invalid_function(
                    f"Function name '{ident}' contains a dot (qualified identifier)",
                    identifier=ident,
                    suggestion="Function names must not be qualified. Use 'UPPER' instead of 'Table.UPPER'"
                )

            base = ident.split(".", 1)[0].strip().upper()
            if base not in _SUPPORTED_FUNCTIONS:
                similar = [f for f in _SUPPORTED_FUNCTIONS if f.startswith(base[:3].upper()) or base[:3].upper() in f]
                suggestion = None
                if similar:
                    suggestion = f"Did you mean one of: {', '.join(similar[:5])}?"
                else:
                    suggestion = f"Supported functions: {', '.join(sorted(_SUPPORTED_FUNCTIONS)[:10])}..."
                
                return SemanticErrorDetail.create_invalid_function(
                    f"Function '{base}' is not supported in DSL",
                    identifier=base,
                    suggestion=suggestion
                )

            arity = _FUNC_ARITY.get(base)
            if arity is None:
                continue
            min_args, max_args = arity
            
            # Handle different call types: aggregate_func_call has different structure
            if call_type == "aggregate_func_call":
                # aggregate_func_call: identifier LPAREN [DISTINCT] expr RPAREN [OVER window_spec]
                # Only validate if this is actually an aggregate function (COUNT, SUM, AVG, MIN, MAX)
                # If a non-aggregate function is incorrectly parsed as aggregate_func_call, skip validation
                # (this is a parser issue, but we don't want to fail validation for it)
                if base not in {"COUNT", "SUM", "AVG", "MIN", "MAX"}:
                    continue  # Skip validation for non-aggregate functions parsed as aggregate_func_call
                
                # Count the expr argument (skip DISTINCT token if present)
                n_expr = 0
                found_expr = False
                for ch in fn_call.children:
                    if isinstance(ch, Tree):
                        if ch.data == "expr":
                            n_expr = 1  # aggregate functions take exactly 1 expr argument
                            found_expr = True
                            break
                        elif ch.data == "arg_list":
                            # Some parsers might create arg_list for aggregates, count it
                            n_expr = sum(1 for c in ch.children if isinstance(c, Tree))
                            found_expr = True
                            break
                if not found_expr:
                    # No expr or arg_list found, treat as 0 arguments
                    n_expr = 0
            else:
                # func_call and window_func_call: identifier LPAREN [arg_list] RPAREN
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
                    return SemanticErrorDetail.create_invalid_parameter(
                        "CATEGORICAL function: expected 2+ (value, weight) pairs, got 0",
                        identifier="CATEGORICAL(...)",
                        suggestion="CATEGORICAL requires at least 2 (value, weight) pairs. Example: CATEGORICAL(('a', 0.6), ('b', 0.4))"
                    )
                pair_count = 0
                non_pair_count = 0
                for c in arg_list_node.children:
                    if isinstance(c, Tree) and c.data == "pair":
                        pair_count += 1
                    elif isinstance(c, Tree):
                        non_pair_count += 1
                if pair_count < 2:
                    return SemanticErrorDetail.create_invalid_parameter(
                        f"CATEGORICAL function: expected at least 2 (value, weight) pairs, got {pair_count}",
                        identifier="CATEGORICAL(...)",
                        suggestion=f"CATEGORICAL requires at least 2 pairs. Add {2 - pair_count} more (value, weight) pair(s)"
                    )
                if non_pair_count > 0:
                    return SemanticErrorDetail.create_invalid_parameter(
                        "CATEGORICAL function: all arguments must be (value, weight) pairs",
                        identifier="CATEGORICAL(...)",
                        suggestion="Ensure all arguments are in the format (value, weight). Example: CATEGORICAL(('a', 0.6), ('b', 0.4))"
                    )
                continue

            if not (min_args <= n_expr <= max_args):
                expected_str = f"{min_args}" if min_args == max_args else f"{min_args}..{max_args}"
                suggestion = f"Function '{base}' requires {expected_str} argument(s), but {n_expr} were provided"
                if n_expr < min_args:
                    suggestion += f". Add {min_args - n_expr} more argument(s)."
                else:
                    suggestion += f". Remove {n_expr - max_args} argument(s)."
                
                return SemanticErrorDetail.create_invalid_parameter(
                    f"Function '{base}' called with incorrect number of arguments",
                    identifier=f"{base}(...)",
                    context=f"Got {n_expr} argument(s), expected {expected_str}",
                    suggestion=suggestion
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


def _validate_distribution_calls(tree: Tree) -> Optional[SemanticErrorDetail]:
    """Return error if a distribution call is not supported; otherwise None."""
    for dist_call in tree.find_data("dist_call"):
        # dist_call: identifier "(" [arg_list] ")"
        ident = None
        for ch in dist_call.children:
            if isinstance(ch, Tree) and ch.data == "identifier":
                ident = _identifier_tree_to_str(ch)
                break
        if not ident:
            return SemanticErrorDetail.create_invalid_distribution(
                "Distribution call is missing a distribution name",
                suggestion="Ensure distribution calls have the format: column ~ DISTRIBUTION_NAME(arg1, arg2, ...)"
            )
        if "." in ident:
            return SemanticErrorDetail.create_invalid_distribution(
                f"Distribution name '{ident}' contains a dot (qualified identifier)",
                identifier=ident,
                suggestion="Distribution names must not be qualified. Use 'UNIFORM' instead of 'Table.UNIFORM'"
            )
        base = ident.split(".", 1)[0].strip().upper()
        if base not in _SUPPORTED_DISTRIBUTIONS:
            similar = [d for d in _SUPPORTED_DISTRIBUTIONS if d.startswith(base[:3].upper()) or base[:3].upper() in d]
            suggestion = None
            if similar:
                suggestion = f"Did you mean one of: {', '.join(similar[:5])}?"
            else:
                suggestion = f"Supported distributions: {', '.join(sorted(_SUPPORTED_DISTRIBUTIONS)[:10])}..."
            
            return SemanticErrorDetail.create_invalid_distribution(
                f"Distribution '{base}' is not supported in DSL",
                identifier=base,
                suggestion=suggestion
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
            expected_str = f"{min_args}" if min_args == max_args else f"{min_args}..{max_args}"
            suggestion = f"Distribution '{base}' requires {expected_str} argument(s), but {n_expr} were provided"
            if n_expr < min_args:
                suggestion += f". Add {min_args - n_expr} more argument(s)."
            else:
                suggestion += f". Remove {n_expr - max_args} argument(s)."
            
            return SemanticErrorDetail.create_invalid_parameter(
                f"Distribution '{base}' called with incorrect number of arguments",
                identifier=f"{base}(...)",
                context=f"Got {n_expr} argument(s), expected {expected_str}",
                suggestion=suggestion
            )
    return None


def validate_dsl_expression_strict(dsl: str) -> Dict[str, Any]:
    """Validate DSL expression against the formal grammar.

    Returns a tool-friendly dict:
      {"valid": bool, "error": Optional[str]}
    """
    # Tokenize first, then parse tokens (proper separation)
    from .lexer import tokenize_dsl
    from .parser import parse_tokens
    
    try:
        tokens = tokenize_dsl(dsl, return_model=False)
        if not tokens:
            return {"valid": False, "error": "Empty or whitespace-only expression"}
        tree = parse_tokens(tokens, original_text=dsl)
        err = _validate_distribution_calls(tree)
        if err:
            return {"valid": False, "error": err.format_message()}
        err2 = _validate_function_calls(tree)
        if err2:
            return {"valid": False, "error": err2.format_message()}
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

    # Tokenize and parse (proper separation: lexer → tokens → parser)
    try:
        tokens = tokenize_dsl(dsl, profile=profile, return_model=False)
        tree = parse_tokens(tokens, original_text=dsl, profile=profile)
        err = _validate_distribution_calls(tree)
        if err:
            return {"valid": False, "error": err.format_message()}
        err2 = _validate_function_calls(tree)
        if err2:
            return {"valid": False, "error": err2.format_message()}
        return {"valid": True, "error": None}
    except DSLParseError as e:
        return {"valid": False, "error": str(e)}
    except Exception as e:
        return {"valid": False, "error": f"DSL validation error: {e}"}


def validate_column_bound_dsl(
    column_bound_dsl: ColumnBoundDSL,
    schema: DSLSchemaContext,
    return_model: bool = False,
) -> Union[Dict[str, Any], ValidationResult]:
    """Validate a column-bound DSL expression with anchor-first identifier resolution.
    
    This is the primary entry point for validating DSL expressions that are bound to
    a specific column. It enforces anchor-first resolution where bare identifiers
    resolve to columns in the anchor table by default.
    
    Args:
        column_bound_dsl: ColumnBoundDSL object containing anchor context and expression
        schema: Schema context containing table and column definitions with types
        return_model: If True, returns ValidationResult (Pydantic model) instead of dict
        
    Returns:
        If return_model=False: Dictionary with:
            - "valid": bool - True if expression passes all semantic checks
            - "error": str | None - Error message if validation fails, None if valid
        If return_model=True: ValidationResult with structured validation information
        
    Examples:
        >>> from NL2DATA.utils.dsl.models import ColumnBoundDSL, DSLKind
        >>> schema = DSLSchemaContext(tables={
        ...     "Order": DSLTableSchema(columns={"discount_pct": "number", "total_amount": "number"})
        ... })
        >>> dsl = ColumnBoundDSL(
        ...     anchor_table="Order",
        ...     anchor_column="discount_pct",
        ...     dsl_kind=DSLKind.CONSTRAINT,
        ...     expression="discount_pct IN_RANGE(0, 10)"
        ... )
        >>> validate_column_bound_dsl(dsl, schema)
        {"valid": True, "error": None}
    """
    return validate_dsl_expression_with_schema(
        dsl=column_bound_dsl.expression,
        schema=schema,
        grammar=column_bound_dsl.profile,
        anchor_table=column_bound_dsl.anchor_table,
        anchor_column=column_bound_dsl.anchor_column,
        return_model=return_model,
    )


def validate_dsl_expression_with_schema(
    dsl: str,
    schema: DSLSchemaContext,
    grammar: Optional[str] = None,
    anchor_table: Optional[str] = None,
    anchor_column: Optional[str] = None,
    return_model: bool = False,
) -> Union[Dict[str, Any], ValidationResult]:
    """Validate DSL expression with comprehensive schema-aware semantic checks.

    This is the semantic analyzer phase of DSL validation. It performs:
    
    1. SCHEMA EXISTENCE VALIDATION:
       - Verifies all tables exist in the schema
       - Verifies all columns exist in their respective tables
       - Rejects unknown tables: "UnknownTable.column" → error
       - Rejects unknown columns: "Table.unknown_column" → error
       - Handles ambiguous columns: bare "column" that exists in multiple tables → error
         (must use "Table.column" format)
       - Rejects deeper paths: "schema.Table.column" → error (only Table.column allowed)
    
    2. TYPE COMPATIBILITY VALIDATION:
       - Infers types from schema for all identifiers (Table.column → type from schema)
       - Validates operator type requirements:
         * Arithmetic operators (+, -, *, /, %): require numeric operands
         * Comparison operators (<, <=, >, >=): require orderable types (not boolean)
         * Boolean operators (AND, OR, NOT): require boolean operands
         * LIKE operator: requires string operands
         * IN operator: list elements must match LHS type
         * BETWEEN operator: all operands must be orderable and compatible
       - Validates function argument types:
         * String functions (UPPER, LOWER, LENGTH, etc.): require string arguments
         * Numeric functions (ROUND, ABS, FLOOR, etc.): require numeric arguments
         * Date functions (DATEADD, DATEDIFF, etc.): require appropriate date/numeric types
       - Validates distribution compatibility:
         * Numeric distributions (UNIFORM, NORMAL, etc.): require numeric target columns
         * BERNOULLI: accepts boolean or numeric target columns
         * CATEGORICAL: accepts any target column type
       - Validates conditional expressions:
         * IF conditions: must be boolean
         * CASE WHEN conditions: must be boolean
         * Branch types: should be compatible (IF then/else, CASE branches)
    
    3. DISTRIBUTION PARAMETER VALIDATION:
       - Validates distribution parameter constraints (min < max, positive values, etc.)
       - Validates distribution target is a valid column identifier
    
    4. FUNCTION/DISTRIBUTION ALLOWLIST:
       - Ensures only allowed functions are used
       - Ensures only allowed distributions are used
       - Validates function/distribution arity

    Args:
        dsl: The DSL expression string to validate
        schema: Schema context containing table and column definitions with types
        grammar: Optional grammar profile (for extensions)
        anchor_table: Optional anchor table name for anchor-first identifier resolution
        anchor_column: Optional anchor column name (for constraint validation warnings)

    Returns:
        If return_model=False: Dictionary with:
            - "valid": bool - True if expression passes all semantic checks
            - "error": str | None - Error message if validation fails, None if valid
        If return_model=True: ValidationResult (Pydantic model) with structured validation information

    Examples:
        >>> schema = DSLSchemaContext(tables={
        ...     "Customer": DSLTableSchema(columns={"age": "number", "name": "string"})
        ... })
        >>> validate_dsl_expression_with_schema("Customer.age + 10", schema)
        {"valid": True, "error": None}
        >>> validate_dsl_expression_with_schema("Customer.name + 10", schema)
        {"valid": False, "error": "..."}  # String + number type mismatch
        >>> validate_dsl_expression_with_schema("UnknownTable.column + 10", schema)
        {"valid": False, "error": "Unknown table 'UnknownTable'..."}
        >>> validate_dsl_expression_with_schema("Customer.unknown_col + 10", schema)
        {"valid": False, "error": "Unknown column 'unknown_col' in table 'Customer'"}

    Notes:
        - This is a conservative validator: if a type cannot be inferred, it does not
          fail unless the identifier itself is invalid.
        - Schema validation happens first (identifier resolution), then type checking.
        - All type checks use types from the schema when available.
    """
    _ = grammar

    strict = validate_dsl_expression(dsl, grammar=grammar)
    if not strict.get("valid", False):
        if return_model:
            # Convert dict error to ValidationResult
            error_msg = strict.get("error", "Unknown validation error")
            semantic_errors = [SemanticError(
                error_type="Syntax Error",
                message=error_msg,
                severity=ErrorSeverity.ERROR,
            )]
            return ValidationResult.from_errors(dsl, semantic_errors)
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
        # Tokenize first, then parse tokens (proper separation: lexer → tokens → parser)
        tokens = tokenize_dsl(dsl, profile=profile, return_model=False)
        tree = parse_tokens(tokens, original_text=dsl, profile=profile)
    except DSLParseError as e:
        if return_model:
            semantic_errors = [SemanticError(
                error_type="Parse Error",
                message=str(e),
                severity=ErrorSeverity.ERROR,
            )]
            return ValidationResult.from_errors(dsl, semantic_errors)
        return {"valid": False, "error": str(e)}
    except Exception as e:
        if return_model:
            semantic_errors = [SemanticError(
                error_type="Validation Error",
                message=f"DSL validation error: {e}",
                severity=ErrorSeverity.ERROR,
            )]
            return ValidationResult.from_errors(dsl, semantic_errors)
        return {"valid": False, "error": f"DSL validation error: {e}"}

    errors: List[SemanticErrorDetail] = []
    warnings: List[str] = []
    
    # Validate anchor_table exists if provided
    if anchor_table:
        if anchor_table not in schema.tables:
            error_msg = f"Anchor table '{anchor_table}' does not exist in schema"
            if return_model:
                semantic_errors = [SemanticError(
                    error_type="Invalid Anchor Table",
                    message=error_msg,
                    identifier=anchor_table,
                    severity=ErrorSeverity.ERROR,
                )]
                return ValidationResult.from_errors(dsl, semantic_errors)
            return {"valid": False, "error": error_msg}
        
        # Validate anchor_column exists in anchor_table if provided
        if anchor_column:
            anchor_schema = schema.tables[anchor_table]
            if anchor_column not in anchor_schema.columns:
                error_msg = f"Anchor column '{anchor_column}' does not exist in table '{anchor_table}'"
                if return_model:
                    semantic_errors = [SemanticError(
                        error_type="Invalid Anchor Column",
                        message=error_msg,
                        identifier=anchor_column,
                        severity=ErrorSeverity.ERROR,
                    )]
                    return ValidationResult.from_errors(dsl, semantic_errors)
                return {"valid": False, "error": error_msg}
            
            # Check if constraint expression references anchor column (soft warning)
            # This is a best-effort check - we'll look for the column name in the expression
            if anchor_column not in dsl and "THIS." + anchor_column not in dsl:
                warning_msg = f"Constraint expression does not reference anchor column '{anchor_column}'"
                warnings.append(warning_msg)


    def _is_tok(x: Any, typ: str) -> bool:
        return isinstance(x, Token) and x.type == typ
    
    def _extract_identifiers_from_where_clause(where_node: Any) -> List[str]:
        """Extract identifier strings from a WHERE clause expression node.
        
        This is a helper to analyze WHERE clauses for uniqueness validation.
        """
        identifiers = []
        if isinstance(where_node, Tree):
            if where_node.data == "identifier":
                identifiers.append(_identifier_tree_to_str(where_node))
            else:
                # Recursively extract identifiers from child nodes
                for child in where_node.children:
                    identifiers.extend(_extract_identifiers_from_where_clause(child))
        return identifiers

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
                
                # Special handling for THIS keyword
                # THIS.column refers to a column in the current context, not a table name
                if ident.upper().startswith("THIS."):
                    # Extract column name after THIS.
                    parts = ident.split(".", 1)
                    if len(parts) == 2:
                        column_name = parts[1]
                        # THIS.column should be treated as a column reference
                        # We need to resolve it in the current context
                        # For now, try to find the column in any table (similar to bare column resolution)
                        # But THIS is context-dependent, so we'll be lenient
                        # In a real implementation, THIS would refer to the current row's table
                        # For validation purposes, we check if the column exists in at least one table
                        all_columns = schema.all_columns_index()
                        if column_name in all_columns:
                            # Column exists in at least one table - THIS.column is valid
                            # Return the type from the first table that has this column
                            table_with_col = list(all_columns[column_name])[0]
                            col_type = schema.tables[table_with_col].columns.get(column_name, "unknown")
                            return col_type or "unknown"
                        else:
                            errors.append(SemanticErrorDetail.create_unknown_identifier(
                                f"Column '{column_name}' referenced via THIS does not exist in any table",
                                identifier=ident,
                                suggestion=f"Available columns: {', '.join(sorted(set().union(*[set(t.columns.keys()) for t in schema.tables.values()])))}"
                            ))
                            return "unknown"
                    else:
                        # Just "THIS" without a column - this is invalid
                        errors.append(SemanticErrorDetail(
                            error_type="Invalid THIS Usage",
                            message="THIS must be followed by a column name: THIS.column_name",
                            identifier=ident,
                            suggestion="Use THIS.column_name to reference a column in the current context"
                        ))
                        return "unknown"
                
                # Use anchor-first resolution if anchor_table is provided
                _, _, t, err = schema.resolve_identifier(ident, anchor_table=anchor_table)
                if err:
                    # err already contains formatted error from schema.resolve_identifier
                    errors.append(SemanticErrorDetail.create_unknown_identifier(
                        err,
                        identifier=ident,
                        suggestion=f"Ensure the identifier exists in the schema. Anchor table: {anchor_table or 'none'}"
                    ))
                    return "unknown"
                return t or "unknown"

            if node.data == "unary":
                # unary: (PLUS|MINUS) factor
                op = node.children[0] if node.children else None
                rhs = node.children[1] if len(node.children) > 1 else None
                rhs_t = _infer_expr_type(rhs)
                if _is_tok(op, "MINUS") and rhs_t not in {"number", "unknown", "null"}:
                    errors.append(SemanticErrorDetail.create_type_mismatch(
                        "Unary minus operator expects numeric operand",
                        actual_type=rhs_t,
                        expected_type="number",
                        suggestion=f"Convert the operand to a number or use a numeric expression"
                    ))
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

            if node.data == "func_call" or node.data == "aggregate_func_call":
                # func_call: identifier "(" [arg_list] ")"
                # aggregate_func_call: identifier "(" [DISTINCT] expr ")" [OVER window_spec]
                # Handle both as function calls for type checking
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
                        errors.append(SemanticErrorDetail.create_type_mismatch(
                            f"Function {fname} expects string argument",
                            identifier=f"{fname}(...)",
                            actual_type=arg_types[0],
                            expected_type="string",
                            suggestion=f"Convert the argument to a string using CAST or string conversion functions"
                        ))
                    return "string"
                if fname in {"CONCAT"}:
                    return "string"
                if fname in {"LENGTH"}:
                    if arg_types and arg_types[0] not in {"string", "unknown", "null"}:
                        errors.append(SemanticErrorDetail.create_type_mismatch(
                            f"Function {fname} expects string argument",
                            identifier=f"{fname}(...)",
                            actual_type=arg_types[0],
                            expected_type="string",
                            suggestion="Convert the argument to a string or use a string expression"
                        ))
                    return "number"
                if fname in {"REPLACE"}:
                    # REPLACE(str, from, to) -> string
                    if len(arg_types) >= 1 and arg_types[0] not in {"string", "unknown", "null"}:
                        errors.append(SemanticErrorDetail.create_type_mismatch(
                            f"REPLACE expects string as first argument, got {arg_types[0]}",
                            identifier=f"{fname}(...)",
                            actual_type=arg_types[0],
                            expected_type="string",
                        ))
                    if len(arg_types) >= 2 and arg_types[1] not in {"string", "unknown", "null"}:
                        errors.append(SemanticErrorDetail.create_type_mismatch(
                            f"REPLACE expects string as second argument, got {arg_types[1]}",
                            identifier=f"{fname}(...)",
                            actual_type=arg_types[1],
                            expected_type="string",
                        ))
                    if len(arg_types) >= 3 and arg_types[2] not in {"string", "unknown", "null"}:
                        errors.append(SemanticErrorDetail.create_type_mismatch(
                            f"REPLACE expects string as third argument, got {arg_types[2]}",
                            identifier=f"{fname}(...)",
                            actual_type=arg_types[2],
                            expected_type="string",
                        ))
                    return "string"
                if fname in {"SUBSTR", "SUBSTRING"}:
                    # SUBSTR(str, start[, len]) -> string
                    if len(arg_types) >= 1 and arg_types[0] not in {"string", "unknown", "null"}:
                        errors.append(SemanticErrorDetail.create_type_mismatch(
                            f"{fname} expects string as first argument, got {arg_types[0]}",
                            identifier=f"{fname}(...)",
                            actual_type=arg_types[0],
                            expected_type="string",
                        ))
                    if len(arg_types) >= 2 and arg_types[1] not in {"number", "unknown", "null"}:
                        errors.append(SemanticErrorDetail.create_type_mismatch(
                            f"{fname} expects numeric start index as second argument, got {arg_types[1]}",
                            identifier=f"{fname}(...)",
                            actual_type=arg_types[1],
                            expected_type="number",
                        ))
                    if len(arg_types) >= 3 and arg_types[2] not in {"number", "unknown", "null"}:
                        errors.append(SemanticErrorDetail.create_type_mismatch(
                            f"{fname} expects numeric length as third argument, got {arg_types[2]}",
                            identifier=f"{fname}(...)",
                            actual_type=arg_types[2],
                            expected_type="number",
                        ))
                    return "string"
                if fname in {"SPLIT_PART"}:
                    # SPLIT_PART(str, delim, index) -> string
                    if len(arg_types) >= 1 and arg_types[0] not in {"string", "unknown", "null"}:
                        errors.append(SemanticErrorDetail.create_type_mismatch(
                            f"SPLIT_PART expects string as first argument, got {arg_types[0]}",
                            identifier=f"{fname}(...)",
                            actual_type=arg_types[0],
                            expected_type="string",
                        ))
                    if len(arg_types) >= 2 and arg_types[1] not in {"string", "unknown", "null"}:
                        errors.append(SemanticErrorDetail.create_type_mismatch(
                            f"SPLIT_PART expects string delimiter as second argument, got {arg_types[1]}",
                            identifier=f"{fname}(...)",
                            actual_type=arg_types[1],
                            expected_type="string",
                        ))
                    if len(arg_types) >= 3 and arg_types[2] not in {"number", "unknown", "null"}:
                        errors.append(SemanticErrorDetail.create_type_mismatch(
                            f"SPLIT_PART expects numeric index as third argument, got {arg_types[2]}",
                            identifier=f"{fname}(...)",
                            actual_type=arg_types[2],
                            expected_type="number",
                        ))
                    return "string"
                if fname in {"REGEXP_EXTRACT"}:
                    # REGEXP_EXTRACT(str, pattern[, group]) -> string
                    if len(arg_types) >= 1 and arg_types[0] not in {"string", "unknown", "null"}:
                        errors.append(SemanticErrorDetail.create_type_mismatch(
                            f"REGEXP_EXTRACT expects string as first argument, got {arg_types[0]}",
                            identifier=f"{fname}(...)",
                            actual_type=arg_types[0],
                            expected_type="string",
                        ))
                    if len(arg_types) >= 2 and arg_types[1] not in {"string", "unknown", "null"}:
                        errors.append(SemanticErrorDetail.create_type_mismatch(
                            f"REGEXP_EXTRACT expects string pattern as second argument, got {arg_types[1]}",
                            identifier=f"{fname}(...)",
                            actual_type=arg_types[1],
                            expected_type="string",
                        ))
                    if len(arg_types) >= 3 and arg_types[2] not in {"number", "unknown", "null"}:
                        errors.append(SemanticErrorDetail.create_type_mismatch(
                            f"REGEXP_EXTRACT expects numeric group index as third argument, got {arg_types[2]}",
                            identifier=f"{fname}(...)",
                            actual_type=arg_types[2],
                            expected_type="number",
                        ))
                    return "string"
                if fname in {"ABS", "ROUND", "FLOOR", "CEIL", "CEILING"}:
                    if arg_types and arg_types[0] not in {"number", "unknown", "null"}:
                        errors.append(SemanticErrorDetail.create_type_mismatch(
                            f"Function {fname} expects numeric argument",
                            identifier=f"{fname}(...)",
                            actual_type=arg_types[0],
                            expected_type="number",
                            suggestion="Convert the argument to a number or use a numeric expression"
                        ))
                    return "number"
                if fname in {"DATEADD"}:
                    # DATEADD(unit, value, datetime)
                    # We keep this minimal: unit should be string literal when known, value numeric, dt datetime/date/time.
                    if len(arg_types) >= 1 and arg_types[0] not in {"string", "unknown", "null"}:
                        errors.append(SemanticErrorDetail.create_type_mismatch(
                            f"DATEADD expects string unit as first argument, got {arg_types[0]}",
                            identifier=f"{fname}(...)",
                            actual_type=arg_types[0],
                            expected_type="string",
                        ))
                    if len(arg_types) >= 2 and arg_types[1] not in {"number", "unknown", "null"}:
                        errors.append(SemanticErrorDetail.create_type_mismatch(
                            f"DATEADD expects numeric value as second argument, got {arg_types[1]}",
                            identifier=f"{fname}(...)",
                            actual_type=arg_types[1],
                            expected_type="number",
                        ))
                    if len(arg_types) >= 3 and arg_types[2] not in {"datetime", "date", "time", "unknown", "null"}:
                        errors.append(SemanticErrorDetail.create_type_mismatch(
                            f"DATEADD expects datetime/date/time as third argument, got {arg_types[2]}",
                            identifier=f"{fname}(...)",
                            actual_type=arg_types[2],
                            expected_type="datetime, date, or time",
                        ))
                    # Return same family as the 3rd arg if known
                    if len(arg_types) >= 3 and arg_types[2] in {"datetime", "date", "time"}:
                        return arg_types[2]
                    return "unknown"
                if fname in {"DATEDIFF"}:
                    # DATEDIFF(unit, start, end) -> number
                    if len(arg_types) >= 1 and arg_types[0] not in {"string", "unknown", "null"}:
                        errors.append(SemanticErrorDetail.create_type_mismatch(
                            f"DATEDIFF expects string unit as first argument, got {arg_types[0]}",
                            identifier=f"{fname}(...)",
                            actual_type=arg_types[0],
                            expected_type="string",
                        ))
                    if len(arg_types) >= 2 and arg_types[1] not in {"datetime", "date", "time", "unknown", "null"}:
                        errors.append(SemanticErrorDetail.create_type_mismatch(
                            f"DATEDIFF expects datetime/date/time as second argument, got {arg_types[1]}",
                            identifier=f"{fname}(...)",
                            actual_type=arg_types[1],
                            expected_type="datetime, date, or time",
                        ))
                    if len(arg_types) >= 3 and arg_types[2] not in {"datetime", "date", "time", "unknown", "null"}:
                        errors.append(SemanticErrorDetail.create_type_mismatch(
                            f"DATEDIFF expects datetime/date/time as third argument, got {arg_types[2]}",
                            identifier=f"{fname}(...)",
                            actual_type=arg_types[2],
                            expected_type="datetime, date, or time",
                        ))
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
                        errors.append(SemanticErrorDetail.create_type_mismatch(
                            "IF condition must evaluate to boolean",
                            actual_type=t_cond,
                            expected_type="boolean",
                            suggestion="Use a comparison (>, <, =, etc.) or boolean expression in the IF condition"
                        ))
                    t_then = _infer_expr_type(then_)
                    t_else = _infer_expr_type(else_)
                    known = {t_then, t_else} - {"unknown", "null"}
                    if len(known) == 2 and t_then != t_else:
                        errors.append(SemanticErrorDetail.create_type_mismatch(
                            "IF expression branches have incompatible types",
                            context=f"THEN branch: {t_then}, ELSE branch: {t_else}",
                            suggestion="Ensure both branches return the same type, or use type conversion functions"
                        ))
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
                            errors.append(SemanticErrorDetail.create_type_mismatch(
                                "CASE WHEN condition must evaluate to boolean",
                                actual_type=t_cond,
                                expected_type="boolean",
                                suggestion="Use a comparison or boolean expression in the WHEN condition"
                            ))
                        branch_types.append(_infer_expr_type(wc.children[3]))
                # ELSE expr if present
                for i, ch in enumerate(node.children):
                    if _is_tok(ch, "ELSE") and i + 1 < len(node.children):
                        branch_types.append(_infer_expr_type(node.children[i + 1]))
                        break
                known = [t for t in branch_types if t not in {"unknown", "null"}]
                if known and any(t != known[0] for t in known):
                    errors.append(SemanticErrorDetail.create_type_mismatch(
                        "CASE expression branches have incompatible types",
                        context=f"Branch types found: {sorted(set(known))}",
                        suggestion="Ensure all CASE branches return the same type, or use type conversion functions"
                    ))
                return known[0] if known else "unknown"

            # Handle relational constraint functions
            # These functions have table names as their first argument
            if node.data == "relational_exists":
                # relational_exists: EXISTS LPAREN identifier WHERE expr RPAREN
                # First identifier is a table name, not a column
                if len(node.children) >= 5:
                    # Find the identifier node (should be after LPAREN)
                    table_ident_node = None
                    for i, ch in enumerate(node.children):
                        if isinstance(ch, Tree) and ch.data == "identifier":
                            table_ident_node = ch
                            break
                    
                    if table_ident_node:
                        table_name = _identifier_tree_to_str(table_ident_node)
                        # Validate that this is a table name (not Table.Column format)
                        if "." in table_name:
                            errors.append(SemanticErrorDetail(
                                error_type="Invalid Table Name",
                                message=f"Table name in EXISTS must be a simple identifier, not qualified: '{table_name}'",
                                identifier=table_name,
                                suggestion=f"Use just the table name: '{table_name.split('.')[0]}'"
                            ))
                        elif table_name not in schema.tables:
                            errors.append(SemanticErrorDetail.create_unknown_identifier(
                                f"Table '{table_name}' does not exist in the schema",
                                identifier=table_name,
                                suggestion=f"Available tables: {', '.join(sorted(schema.tables.keys()))}"
                            ))
                    
                    # Validate WHERE clause (should be boolean)
                    where_expr = None
                    for i, ch in enumerate(node.children):
                        if _is_tok(ch, "WHERE") and i + 1 < len(node.children):
                            where_expr = node.children[i + 1]
                            break
                    
                    if where_expr:
                        where_type = _infer_expr_type(where_expr)
                        if where_type not in {"boolean", "unknown", "null"}:
                            errors.append(SemanticErrorDetail.create_type_mismatch(
                                "EXISTS WHERE clause must evaluate to boolean",
                                actual_type=where_type,
                                expected_type="boolean",
                                suggestion="Use a comparison or boolean expression in the WHERE clause"
                            ))
                
                return "boolean"  # EXISTS returns boolean
            
            if node.data == "relational_lookup":
                # relational_lookup: LOOKUP LPAREN identifier COMMA expr WHERE expr RPAREN
                # First identifier is a table name
                if len(node.children) >= 6:
                    # Find the identifier node (table name)
                    table_ident_node = None
                    value_expr = None
                    where_expr = None
                    
                    for i, ch in enumerate(node.children):
                        if isinstance(ch, Tree) and ch.data == "identifier" and table_ident_node is None:
                            table_ident_node = ch
                        elif _is_tok(ch, "COMMA") and i + 1 < len(node.children):
                            # Value expression comes after COMMA
                            value_expr = node.children[i + 1]
                        elif _is_tok(ch, "WHERE") and i + 1 < len(node.children):
                            where_expr = node.children[i + 1]
                    
                    if table_ident_node:
                        table_name = _identifier_tree_to_str(table_ident_node)
                        if "." in table_name:
                            errors.append(SemanticErrorDetail(
                                error_type="Invalid Table Name",
                                message=f"Table name in LOOKUP must be a simple identifier, not qualified: '{table_name}'",
                                identifier=table_name,
                                suggestion=f"Use just the table name: '{table_name.split('.')[0]}'"
                            ))
                        elif table_name not in schema.tables:
                            errors.append(SemanticErrorDetail.create_unknown_identifier(
                                f"Table '{table_name}' does not exist in the schema",
                                identifier=table_name,
                                suggestion=f"Available tables: {', '.join(sorted(schema.tables.keys()))}"
                            ))
                    
                    if where_expr:
                        where_type = _infer_expr_type(where_expr)
                        if where_type not in {"boolean", "unknown", "null"}:
                            errors.append(SemanticErrorDetail.create_type_mismatch(
                                "LOOKUP WHERE clause must evaluate to boolean",
                                actual_type=where_type,
                                expected_type="boolean",
                                suggestion="Use a comparison or boolean expression in the WHERE clause"
                            ))
                        
                        # Validate LOOKUP uniqueness: WHERE clause must be provably unique
                        # This is a conservative check - we look for PK equality, unique key equality,
                        # or 1-1 relationship patterns in the WHERE clause
                        if table_name in schema.tables:
                            lookup_table = schema.tables[table_name]
                            # Extract identifiers from WHERE clause to check for uniqueness patterns
                            where_identifiers = _extract_identifiers_from_where_clause(where_expr)
                            
                            # Check if WHERE clause filters by a column that could be a PK or unique key
                            # For now, we require at least one equality comparison with a column from lookup_table
                            # This is a simplified check - in a full implementation, we'd check actual PK/unique constraints
                            has_equality_filter = False
                            for ident in where_identifiers:
                                # Check if identifier references a column in the lookup table
                                if "." in ident:
                                    parts = ident.split(".", 1)
                                    if len(parts) == 2:
                                        ident_table, ident_col = parts
                                        if ident_table == table_name and ident_col in lookup_table.columns:
                                            has_equality_filter = True
                                            break
                                else:
                                    # Bare identifier - check if it's in lookup table
                                    if ident in lookup_table.columns:
                                        has_equality_filter = True
                                        break
                            
                            # If no equality filter found, warn (but don't error - could be valid with proper constraints)
                            # In a full implementation, we'd check schema metadata for PK/unique constraints
                            if not has_equality_filter:
                                errors.append(SemanticErrorDetail(
                                    error_type="LOOKUP Uniqueness Violation",
                                    message=f"LOOKUP WHERE clause may return multiple rows. WHERE clause should filter by primary key, unique key, or 1-1 relationship column to ensure uniqueness.",
                                    identifier=f"LOOKUP({table_name}, ...)",
                                    suggestion="Ensure WHERE clause filters by a unique column (primary key, unique key, or 1-1 relationship column) to guarantee at most one matching row"
                                ))
                    
                    # Return type is the type of the value expression
                    if value_expr:
                        return _infer_expr_type(value_expr)
                
                return "unknown"
            
            if node.data == "relational_agg":
                # relational_agg: (COUNT_WHERE | SUM_WHERE | AVG_WHERE | MIN_WHERE | MAX_WHERE) LPAREN identifier [COMMA expr] WHERE expr RPAREN
                # First identifier is a table name
                if len(node.children) >= 5:
                    # Find the function name token
                    func_name = None
                    for ch in node.children:
                        if isinstance(ch, Token) and ch.type in {"COUNT_WHERE", "SUM_WHERE", "AVG_WHERE", "MIN_WHERE", "MAX_WHERE"}:
                            func_name = ch.type
                            break
                    
                    # Find the identifier node (table name)
                    table_ident_node = None
                    value_expr = None
                    where_expr = None
                    
                    for i, ch in enumerate(node.children):
                        if isinstance(ch, Tree) and ch.data == "identifier" and table_ident_node is None:
                            table_ident_node = ch
                        elif _is_tok(ch, "COMMA") and i + 1 < len(node.children) and func_name in {"SUM_WHERE", "AVG_WHERE", "MIN_WHERE", "MAX_WHERE"}:
                            # Value expression comes after COMMA (for SUM_WHERE, AVG_WHERE, etc.)
                            value_expr = node.children[i + 1]
                        elif _is_tok(ch, "WHERE") and i + 1 < len(node.children):
                            where_expr = node.children[i + 1]
                    
                    if table_ident_node:
                        table_name = _identifier_tree_to_str(table_ident_node)
                        if "." in table_name:
                            errors.append(SemanticErrorDetail(
                                error_type="Invalid Table Name",
                                message=f"Table name in {func_name} must be a simple identifier, not qualified: '{table_name}'",
                                identifier=table_name,
                                suggestion=f"Use just the table name: '{table_name.split('.')[0]}'"
                            ))
                        elif table_name not in schema.tables:
                            errors.append(SemanticErrorDetail.create_unknown_identifier(
                                f"Table '{table_name}' does not exist in the schema",
                                identifier=table_name,
                                suggestion=f"Available tables: {', '.join(sorted(schema.tables.keys()))}"
                            ))
                    
                    if where_expr:
                        where_type = _infer_expr_type(where_expr)
                        if where_type not in {"boolean", "unknown", "null"}:
                            errors.append(SemanticErrorDetail.create_type_mismatch(
                                f"{func_name} WHERE clause must evaluate to boolean",
                                actual_type=where_type,
                                expected_type="boolean",
                                suggestion="Use a comparison or boolean expression in the WHERE clause"
                            ))
                    
                    # Validate value_expr type for numeric aggregates
                    if value_expr and func_name in {"SUM_WHERE", "AVG_WHERE"}:
                        value_type = _infer_expr_type(value_expr)
                        if value_type not in {"number", "unknown", "null"}:
                            errors.append(SemanticErrorDetail.create_type_mismatch(
                                f"{func_name} value expression must be numeric, got {value_type}",
                                actual_type=value_type,
                                expected_type="number",
                                suggestion=f"Use a numeric expression in {func_name}. String and boolean values cannot be summed or averaged."
                            ))
                    
                    # All relational aggregates return numbers
                    return "number"
                
                return "number"
            
            if node.data == "in_range_call":
                # in_range_call: IN_RANGE LPAREN expr COMMA expr COMMA expr RPAREN
                # All three arguments should be orderable (numeric)
                if len(node.children) >= 6:
                    args = []
                    for i, ch in enumerate(node.children):
                        if isinstance(ch, Tree) and ch.data != "identifier":  # Skip the IN_RANGE identifier
                            # Collect expressions (skip tokens like LPAREN, COMMA, RPAREN)
                            if not _is_tok(ch, "LPAREN") and not _is_tok(ch, "RPAREN") and not _is_tok(ch, "COMMA"):
                                args.append(ch)
                    
                    for arg in args[:3]:  # First 3 expressions
                        arg_type = _infer_expr_type(arg)
                        if arg_type not in {"number", "unknown", "null"}:
                            errors.append(SemanticErrorDetail.create_type_mismatch(
                                "IN_RANGE requires numeric arguments",
                                actual_type=arg_type,
                                expected_type="number",
                                suggestion="Use numeric expressions for all IN_RANGE arguments"
                            ))
                
                return "boolean"  # IN_RANGE returns boolean
            
            if node.data == "distribution_expr":
                # distribution_expr: or_expr TILDE dist_call
                # Enforce target is a resolvable identifier.
                if len(node.children) >= 3:
                    target = node.children[0]
                    dist_call = node.children[2]
                    if not (isinstance(target, Tree) and target.data == "identifier"):
                        errors.append(SemanticErrorDetail.create_invalid_distribution(
                            "Distribution target must be a column identifier, not an expression",
                            suggestion="Use a column name (e.g., 'age') or qualified identifier (e.g., 'Customer.age') as the distribution target"
                        ))
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
                        errors.append(SemanticErrorDetail.create_type_mismatch(
                            f"Distribution {dist_name} requires numeric target column",
                            identifier=target_ident,
                            actual_type=target_type,
                            expected_type="number",
                            suggestion=f"Use a numeric column or convert the column to numeric type for {dist_name} distribution"
                        ))
                    if dist_name == "BERNOULLI" and target_type not in {"boolean", "number", "unknown", "null"}:
                        errors.append(SemanticErrorDetail.create_type_mismatch(
                            "Distribution BERNOULLI requires boolean or numeric target column",
                            identifier=target_ident,
                            actual_type=target_type,
                            expected_type="boolean or number",
                            suggestion="Use a boolean or numeric column for BERNOULLI distribution"
                        ))

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
                            errors.append(SemanticErrorDetail.create_invalid_parameter(
                                f"UNIFORM distribution: min ({a0}) must be less than max ({a1})",
                                identifier=f"{target_ident} ~ UNIFORM({a0}, {a1})",
                                suggestion=f"Swap the parameters or ensure min < max. Example: UNIFORM({min(a0, a1)}, {max(a0, a1)})"
                            ))
                    if dist_name == "NORMAL":
                        sigma = num(1)
                        if sigma is not None and not (sigma > 0):
                            errors.append(SemanticErrorDetail.create_invalid_parameter(
                                f"NORMAL distribution: standard deviation ({sigma}) must be greater than 0",
                                identifier=f"{target_ident} ~ NORMAL(..., {sigma})",
                                suggestion="Use a positive value for standard deviation. Example: NORMAL(0, 1.0)"
                            ))
                    if dist_name == "LOGNORMAL":
                        sigma = num(1)
                        if sigma is not None and not (sigma > 0):
                            errors.append(SemanticErrorDetail.create_invalid_parameter(
                                f"LOGNORMAL distribution: sigma ({sigma}) must be greater than 0",
                                identifier=f"{target_ident} ~ LOGNORMAL(..., {sigma})",
                                suggestion="Use a positive value for sigma"
                            ))
                    if dist_name == "BETA":
                        a, b = num(0), num(1)
                        if a is not None and not (a > 0):
                            errors.append(SemanticErrorDetail.create_invalid_parameter(
                                f"BETA distribution: alpha ({a}) must be greater than 0",
                                identifier=f"{target_ident} ~ BETA({a}, ...)",
                                suggestion="Use a positive value for alpha"
                            ))
                        if b is not None and not (b > 0):
                            errors.append(SemanticErrorDetail.create_invalid_parameter(
                                f"BETA distribution: beta ({b}) must be greater than 0",
                                identifier=f"{target_ident} ~ BETA(..., {b})",
                                suggestion="Use a positive value for beta"
                            ))
                    if dist_name == "GAMMA":
                        shape, scale = num(0), num(1)
                        if shape is not None and not (shape > 0):
                            errors.append(SemanticErrorDetail.create_invalid_parameter(
                                f"GAMMA distribution: shape ({shape}) must be greater than 0",
                                identifier=f"{target_ident} ~ GAMMA({shape}, ...)",
                                suggestion="Use a positive value for shape"
                            ))
                        if scale is not None and not (scale > 0):
                            errors.append(SemanticErrorDetail.create_invalid_parameter(
                                f"GAMMA distribution: scale ({scale}) must be greater than 0",
                                identifier=f"{target_ident} ~ GAMMA(..., {scale})",
                                suggestion="Use a positive value for scale"
                            ))
                    if dist_name == "EXPONENTIAL":
                        lam = num(0)
                        if lam is not None and not (lam > 0):
                            errors.append(SemanticErrorDetail.create_invalid_parameter(
                                f"EXPONENTIAL distribution: lambda ({lam}) must be greater than 0",
                                identifier=f"{target_ident} ~ EXPONENTIAL({lam})",
                                suggestion="Use a positive value for lambda"
                            ))
                    if dist_name == "TRIANGULAR":
                        mn, mx, mode = num(0), num(1), num(2)
                        if mn is not None and mx is not None and not (mn < mx):
                            errors.append(SemanticErrorDetail.create_invalid_parameter(
                                f"TRIANGULAR distribution: min ({mn}) must be less than max ({mx})",
                                identifier=f"{target_ident} ~ TRIANGULAR({mn}, {mx}, ...)",
                                suggestion=f"Ensure min < max. Example: TRIANGULAR({min(mn, mx)}, {max(mn, mx)}, ...)"
                            ))
                        if mn is not None and mode is not None and mx is not None:
                            if not (mn <= mode <= mx):
                                errors.append(SemanticErrorDetail.create_invalid_parameter(
                                    f"TRIANGULAR distribution: mode ({mode}) must be between min ({mn}) and max ({mx})",
                                    identifier=f"{target_ident} ~ TRIANGULAR({mn}, {mx}, {mode})",
                                    suggestion=f"Ensure min <= mode <= max"
                                ))
                    if dist_name == "WEIBULL":
                        shape, scale = num(0), num(1)
                        if shape is not None and not (shape > 0):
                            errors.append(SemanticErrorDetail.create_invalid_parameter(
                                f"WEIBULL distribution: shape ({shape}) must be greater than 0",
                                identifier=f"{target_ident} ~ WEIBULL({shape}, ...)",
                                suggestion="Use a positive value for shape"
                            ))
                        if scale is not None and not (scale > 0):
                            errors.append(SemanticErrorDetail.create_invalid_parameter(
                                f"WEIBULL distribution: scale ({scale}) must be greater than 0",
                                identifier=f"{target_ident} ~ WEIBULL(..., {scale})",
                                suggestion="Use a positive value for scale"
                            ))
                    if dist_name == "POISSON":
                        lam = num(0)
                        if lam is not None and not (lam > 0):
                            errors.append(SemanticErrorDetail.create_invalid_parameter(
                                f"POISSON distribution: lambda ({lam}) must be greater than 0",
                                identifier=f"{target_ident} ~ POISSON({lam})",
                                suggestion="Use a positive value for lambda"
                            ))
                    if dist_name == "ZIPF":
                        s = num(0)
                        if s is not None and not (s > 0):
                            errors.append(SemanticErrorDetail.create_invalid_parameter(
                                f"ZIPF distribution: s ({s}) must be greater than 0",
                                identifier=f"{target_ident} ~ ZIPF({s}, ...)",
                                suggestion="Use a positive value for s"
                            ))
                        if len(args) >= 2:
                            if _parse_number_literal(args[1]) is not None:
                                n_val = _parse_number_literal(args[1])
                                if not _is_integer_literal(args[1]) or n_val < 1:
                                    errors.append(SemanticErrorDetail.create_invalid_parameter(
                                        f"ZIPF distribution: n ({n_val}) must be an integer >= 1",
                                        identifier=f"{target_ident} ~ ZIPF(..., {n_val})",
                                        suggestion="Use an integer value >= 1 for n"
                                    ))
                    if dist_name == "PARETO":
                        alpha, scale = num(0), num(1)
                        if alpha is not None and not (alpha > 0):
                            errors.append(SemanticErrorDetail.create_invalid_parameter(
                                f"PARETO distribution: alpha ({alpha}) must be greater than 0",
                                identifier=f"{target_ident} ~ PARETO({alpha}, ...)",
                                suggestion="Use a positive value for alpha"
                            ))
                        if scale is not None and not (scale > 0):
                            errors.append(SemanticErrorDetail.create_invalid_parameter(
                                f"PARETO distribution: scale ({scale}) must be greater than 0",
                                identifier=f"{target_ident} ~ PARETO(..., {scale})",
                                suggestion="Use a positive value for scale"
                            ))
                    if dist_name == "BERNOULLI":
                        p = num(0)
                        if p is not None and not (0 <= p <= 1):
                            errors.append(SemanticErrorDetail.create_invalid_parameter(
                                f"BERNOULLI distribution: probability ({p}) must be between 0 and 1 (inclusive)",
                                identifier=f"{target_ident} ~ BERNOULLI({p})",
                                suggestion=f"Use a probability value between 0 and 1. Example: BERNOULLI({max(0, min(1, p))})"
                            ))
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
                            errors.append(SemanticErrorDetail.create_invalid_parameter(
                                "CATEGORICAL distribution: weight values must be non-negative",
                                identifier=f"{target_ident} ~ CATEGORICAL(...)",
                                suggestion="Ensure all weight values in the categorical distribution are >= 0"
                            ))
                        if weights and sum(weights) <= 0:
                            errors.append(SemanticErrorDetail.create_invalid_parameter(
                                f"CATEGORICAL distribution: sum of weights ({sum(weights)}) must be greater than 0",
                                identifier=f"{target_ident} ~ CATEGORICAL(...)",
                                suggestion="Ensure at least one weight is positive and the sum of all weights is > 0"
                            ))
                return "unknown"

            # Generic operator scanning (because many rules are inlined via '?' in grammar)
            # - AND/OR: require boolean
            # - Arithmetic: require numeric
            # - LIKE: require string
            # - Comparisons: check type compatibility when both known
            expr_children = [c for c in node.children if isinstance(c, Tree)]
            # Pre-compute types for tree children in order
            child_types = {id(c): _infer_expr_type(c) for c in expr_children}

            # FIRST: Scan comparison tails (including BETWEEN) BEFORE boolean operators
            # This ensures BETWEEN is handled correctly and its AND token is not treated as boolean AND
            # Note: We need to check if this node is an and_expr or or_expr to avoid scanning AND/OR
            # that are inside cmp_tail nodes (like in BETWEEN ... AND ...)
            is_boolean_operator_node = node.data in ("and_expr", "or_expr")
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
                    
                    # Check for BETWEEN operator (special case: BETWEEN expr AND expr)
                    has_between = False
                    min_node = None
                    max_node = None
                    for c in ch.children:
                        if isinstance(c, Token) and c.type == "BETWEEN":
                            has_between = True
                        elif isinstance(c, Tree) and has_between and min_node is None:
                            min_node = c
                        elif isinstance(c, Tree) and has_between and min_node is not None:
                            max_node = c
                    
                    if has_between and min_node is not None and max_node is not None:
                        # BETWEEN validation: expr BETWEEN min_expr AND max_expr
                        min_t = _infer_expr_type(min_node)
                        max_t = _infer_expr_type(max_node)
                        
                        # All three operands must be orderable (not boolean)
                        if lhs_t == "boolean":
                            errors.append(SemanticErrorDetail.create_type_mismatch(
                                "BETWEEN operator cannot be used with boolean left operand",
                                actual_type="boolean",
                                expected_type="number, string, or datetime",
                                suggestion="Use a numeric, string, or datetime expression on the left side of BETWEEN"
                            ))
                        if min_t == "boolean":
                            errors.append(SemanticErrorDetail.create_type_mismatch(
                                "BETWEEN operator cannot be used with boolean min operand",
                                actual_type="boolean",
                                expected_type="number, string, or datetime",
                                suggestion="Use a numeric, string, or datetime expression for the min value in BETWEEN"
                            ))
                        if max_t == "boolean":
                            errors.append(SemanticErrorDetail.create_type_mismatch(
                                "BETWEEN operator cannot be used with boolean max operand",
                                actual_type="boolean",
                                expected_type="number, string, or datetime",
                                suggestion="Use a numeric, string, or datetime expression for the max value in BETWEEN"
                            ))
                        
                        # All three operands must have compatible types
                        known_types = {t for t in [lhs_t, min_t, max_t] if t not in {"unknown", "null"}}
                        if len(known_types) > 1:
                            errors.append(SemanticErrorDetail.create_type_mismatch(
                                "BETWEEN operator operands have incompatible types",
                                context=f"Left operand: {lhs_t}, Min operand: {min_t}, Max operand: {max_t}",
                                suggestion="Ensure all three operands (left, min, max) have the same type"
                            ))
                        
                        # If min and max are numeric literals, validate min <= max
                        min_val = _parse_number_literal(min_node)
                        max_val = _parse_number_literal(max_node)
                        if min_val is not None and max_val is not None:
                            if min_val > max_val:
                                errors.append(SemanticErrorDetail.create_invalid_parameter(
                                    f"BETWEEN operator: min value ({min_val}) is greater than max value ({max_val})",
                                    suggestion=f"Swap the values or ensure min <= max. Example: BETWEEN {min(max_val, min_val)} AND {max(max_val, min_val)}"
                                ))
                        # Skip further processing for this cmp_tail (BETWEEN is handled)
                        continue

            # Scan boolean connectors in this node's flat children list
            # Only scan for AND/OR if this is an and_expr or or_expr node
            # AND tokens inside "BETWEEN ... AND ..." are inside cmp_tail nodes,
            # so they won't appear as direct children here.
            if is_boolean_operator_node:
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
                                errors.append(SemanticErrorDetail.create_type_mismatch(
                                    f"Operator {ch.type} expects boolean left operand",
                                    actual_type=t,
                                    expected_type="boolean",
                                    suggestion="Use a boolean expression or comparison on the left side of the operator"
                                ))
                        if right is not None:
                            t = child_types.get(id(right), "unknown")
                            if t not in {"boolean", "unknown", "null"}:
                                errors.append(SemanticErrorDetail.create_type_mismatch(
                                    f"Operator {ch.type} expects boolean right operand",
                                    actual_type=t,
                                    expected_type="boolean",
                                    suggestion="Use a boolean expression or comparison on the right side of the operator"
                                ))

            # Scan arithmetic operators
            has_arithmetic_op = False
            arithmetic_op_types = []
            for i, ch in enumerate(node.children):
                if _is_tok(ch, "PLUS") or _is_tok(ch, "MINUS") or _is_tok(ch, "MUL") or _is_tok(ch, "DIV") or _is_tok(ch, "MOD"):
                    has_arithmetic_op = True
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
                        arithmetic_op_types.append(t)
                        if t not in {"number", "unknown", "null"}:
                            errors.append(SemanticErrorDetail.create_type_mismatch(
                                f"Arithmetic operator {ch.value} expects numeric left operand",
                                actual_type=t,
                                expected_type="number",
                                suggestion="Convert the left operand to a number or use a numeric expression"
                            ))
                    if right is not None:
                        t = child_types.get(id(right), "unknown")
                        arithmetic_op_types.append(t)
                        if t not in {"number", "unknown", "null"}:
                            errors.append(SemanticErrorDetail.create_type_mismatch(
                                f"Arithmetic operator {ch.value} expects numeric right operand",
                                actual_type=t,
                                expected_type="number",
                                suggestion="Convert the right operand to a number or use a numeric expression"
                            ))
            
            # If this node contains arithmetic operators and all operands are numeric, return "number"
            if has_arithmetic_op:
                known_types = [t for t in arithmetic_op_types if t not in {"unknown", "null"}]
                if known_types and all(t == "number" for t in known_types):
                    return "number"
                # If we have at least one numeric type and no non-numeric errors, still return "number"
                # (conservative: if we can't determine, assume it might be valid)
                if any(t == "number" for t in known_types) and not any(t not in {"number", "unknown", "null"} for t in arithmetic_op_types):
                    return "number"

            # Scan LIKE operators
            for i, ch in enumerate(node.children):
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
                            errors.append(SemanticErrorDetail.create_type_mismatch(
                                "LIKE operator expects string left operand",
                                actual_type=t,
                                expected_type="string",
                                suggestion="Use a string column or expression on the left side of LIKE"
                            ))
                    if right is not None:
                        t = child_types.get(id(right), "unknown")
                        if t not in {"string", "unknown", "null"}:
                            errors.append(SemanticErrorDetail.create_type_mismatch(
                                "LIKE operator expects string right operand (pattern)",
                                actual_type=t,
                                expected_type="string",
                                suggestion="Use a string pattern on the right side of LIKE (e.g., 'John%')"
                            ))

            # Continue scanning comparison tails for other operators (non-BETWEEN)
            for i, ch in enumerate(node.children):
                if isinstance(ch, Tree) and ch.data == "cmp_tail":
                    # Skip if this cmp_tail was already handled as BETWEEN above
                    has_between = False
                    for c in ch.children:
                        if isinstance(c, Token) and c.type == "BETWEEN":
                            has_between = True
                            break
                    if has_between:
                        continue  # Already handled above
                    
                    # LHS is nearest Tree to the left
                    lhs = None
                    for j in range(i - 1, -1, -1):
                        if isinstance(node.children[j], Tree):
                            lhs = node.children[j]
                            break
                    if lhs is None:
                        continue
                    lhs_t = child_types.get(id(lhs), "unknown")
                    
                    # Check for IS NULL / IS NOT NULL (special case: no rhs expression)
                    has_is_null = False
                    has_is_not_null = False
                    for c in ch.children:
                        if isinstance(c, Token) and c.type == "IS":
                            has_is_null = True
                        if isinstance(c, Token) and c.type == "NOT":
                            has_is_not_null = True
                        if isinstance(c, Token) and c.type == "NULL":
                            if has_is_null:
                                # IS NULL - no type restrictions (can check any type for null)
                                pass
                            elif has_is_not_null:
                                # IS NOT NULL - no type restrictions
                                pass
                    if has_is_null or has_is_not_null:
                        # IS NULL / IS NOT NULL - no type restrictions (can check any type for null)
                        continue
                    
                    # Determine op + rhs for other comparison operators
                    op_tok = None
                    rhs_node = None
                    for c in ch.children:
                        if isinstance(c, Token) and op_tok is None and c.type not in {"BETWEEN", "AND", "IS", "NOT", "NULL"}:
                            op_tok = c
                        if isinstance(c, Tree) and rhs_node is None:
                            rhs_node = c
                    rhs_t = _infer_expr_type(rhs_node) if rhs_node else "unknown"
                    
                    if op_tok and op_tok.type == "IN":
                        # rhs is list
                        if lhs_t not in {"unknown", "null"} and rhs_t not in {"unknown", "null"} and lhs_t != rhs_t:
                            errors.append(SemanticErrorDetail.create_type_mismatch(
                                "IN operator: left operand type does not match list element types",
                                context=f"Left operand: {lhs_t}, List elements: {rhs_t}",
                                suggestion="Ensure the left operand type matches the types of elements in the list"
                            ))
                    elif op_tok and op_tok.type == "LIKE":
                        # Already handled above but keep safe
                        if lhs_t not in {"string", "unknown", "null"}:
                            errors.append(SemanticErrorDetail.create_type_mismatch(
                                "LIKE operator expects string left operand",
                                actual_type=lhs_t,
                                expected_type="string",
                                suggestion="Use a string column or expression on the left side of LIKE"
                            ))
                        if rhs_t not in {"string", "unknown", "null"}:
                            errors.append(SemanticErrorDetail.create_type_mismatch(
                                "LIKE operator expects string right operand (pattern)",
                                actual_type=rhs_t,
                                expected_type="string",
                                suggestion="Use a string pattern on the right side of LIKE (e.g., 'John%')"
                            ))
                    else:
                        # EQ/NE/LT/LE/GT/GE
                        known = {lhs_t, rhs_t} - {"unknown", "null"}
                        if len(known) == 2 and lhs_t != rhs_t:
                            errors.append(SemanticErrorDetail.create_type_mismatch(
                                "Comparison operator operands have incompatible types",
                                context=f"Left operand: {lhs_t}, Right operand: {rhs_t}",
                                suggestion="Ensure both operands have the same type, or use type conversion functions"
                            ))
                        # Ordering comparisons shouldn't be used on booleans when known.
                        if op_tok and op_tok.type in {"LT", "LE", "GT", "GE"}:
                            if lhs_t == "boolean" or rhs_t == "boolean":
                                errors.append(SemanticErrorDetail(
                                    error_type="Invalid Comparison",
                                    message="Ordering comparison operators (<, <=, >, >=) cannot be used with boolean values",
                                    suggestion="Use equality operators (=, !=) for boolean comparisons, or convert to numeric/string for ordering"
                                ))

        return "unknown"

    # Walk the tree and infer types; inference function appends to `errors` as needed.
    inferred_type = _infer_expr_type(tree)
    
    # Get schema table names for metadata
    schema_tables = list(schema.tables.keys()) if schema else None

    # Convert warning strings to SemanticError objects
    semantic_warnings = []
    for warn_str in warnings:
        semantic_warnings.append(SemanticError(
            error_type="Warning",
            message=warn_str,
            severity=ErrorSeverity.WARNING,
        ))
    
    if errors:
        # Convert SemanticErrorDetail objects to SemanticError objects
        semantic_errors = []
        for err_detail in errors:
            if isinstance(err_detail, SemanticErrorDetail):
                semantic_errors.append(SemanticError.from_detail(err_detail))
            elif isinstance(err_detail, str):
                # Legacy string error - convert to SemanticErrorDetail
                semantic_errors.append(SemanticError(
                    error_type="Semantic Error",
                    message=err_detail,
                    severity=ErrorSeverity.ERROR,
                ))
            else:
                semantic_errors.append(SemanticError(
                    error_type="Semantic Error",
                    message=str(err_detail),
                    severity=ErrorSeverity.ERROR,
                ))
        
        if return_model:
            return ValidationResult.from_errors(
                dsl,
                semantic_errors,
                warnings=semantic_warnings if semantic_warnings else None,
                inferred_type=inferred_type if inferred_type != "unknown" else None,
                schema_tables=schema_tables,
            )
        
        # Format multiple errors with clear separation (backward compatibility)
        if len(errors) == 1:
            err_detail = errors[0]
            if isinstance(err_detail, SemanticErrorDetail):
                error_msg = err_detail.format_message()
            else:
                error_msg = str(err_detail)
        else:
            error_msgs = []
            for i, err_detail in enumerate(errors, 1):
                if isinstance(err_detail, SemanticErrorDetail):
                    error_msgs.append(f"{i}. {err_detail.format_message()}")
                else:
                    error_msgs.append(f"{i}. {str(err_detail)}")
            error_msg = f"Found {len(errors)} semantic error(s):\n\n" + "\n\n".join(error_msgs)
        return {"valid": False, "error": error_msg}
    
    if return_model:
        return ValidationResult.from_success(
            dsl,
            inferred_type=inferred_type if inferred_type != "unknown" else None,
            schema_tables=schema_tables,
        )
    # Include warnings in dict return format if any
    if warnings:
        return {"valid": True, "error": None, "warnings": warnings}
    return {"valid": True, "error": None}

