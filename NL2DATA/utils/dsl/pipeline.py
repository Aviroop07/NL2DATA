"""DSL validation pipeline that runs all three phases with Pydantic models.

This module provides a complete validation pipeline that:
1. Tokenizes the DSL expression (lexer)
2. Parses the tokens into an AST (parser)
3. Validates the AST semantically (semantic analyzer)

All intermediate results are stored in Pydantic models for type safety,
serialization, and structured error reporting.
"""

from __future__ import annotations

from typing import Optional

from .grammar_profile import DSLGrammarProfile, FEATURE_RELATIONAL_CONSTRAINTS
from .lexer import tokenize_dsl
from .parser import parse_tokens
from .validator import validate_dsl_expression_with_schema
from .schema_context import DSLSchemaContext
from .models import (
    DSLValidationPipelineResult,
    TokenizationResult,
    ParseResult,
    ValidationResult,
)


def validate_dsl_pipeline(
    dsl: str,
    schema: Optional[DSLSchemaContext] = None,
    grammar_profile: Optional[str] = None,
) -> DSLValidationPipelineResult:
    """Run the complete DSL validation pipeline and return structured results.
    
    This function executes all three phases of DSL validation:
    1. Lexical analysis (tokenization)
    2. Syntax analysis (parsing)
    3. Semantic analysis (type checking and validation)
    
    Args:
        dsl: The DSL expression string to validate
        schema: Optional schema context for semantic validation
        grammar_profile: Optional grammar profile string (e.g., "profile:v1+relational_constraints")
    
    Returns:
        DSLValidationPipelineResult containing results from all three phases
    
    Example:
        >>> from NL2DATA.utils.dsl.schema_context import DSLSchemaContext, DSLTableSchema
        >>> schema = DSLSchemaContext(tables={
        ...     "Customer": DSLTableSchema(columns={"age": "number", "name": "string"})
        ... })
        >>> result = validate_dsl_pipeline("Customer.age + 10", schema)
        >>> print(result.overall_success)  # True if all phases passed
        >>> print(result.get_summary())  # Human-readable summary
    """
    # Phase 1: Tokenization
    tokenization_result = tokenize_dsl(dsl, return_model=True)
    
    # If tokenization failed, return early
    if not tokenization_result.success:
        return DSLValidationPipelineResult.from_pipeline(
            dsl,
            tokenization_result,
            parsing=None,
            validation=None,
        )
    
    # Phase 2: Parsing (works on tokens from lexer)
    # Check if expression uses relational constraint functions
    uses_relational = any(keyword in dsl.upper() for keyword in [
        "EXISTS(", "LOOKUP(", "COUNT_WHERE(", "SUM_WHERE(", "AVG_WHERE(",
        "MIN_WHERE(", "MAX_WHERE(", "IN_RANGE(", "THIS."
    ])
    
    profile = None
    if uses_relational:
        profile = DSLGrammarProfile(version="v1", features={FEATURE_RELATIONAL_CONSTRAINTS})
    elif grammar_profile:
        if isinstance(grammar_profile, str) and grammar_profile.startswith("profile:"):
            spec = grammar_profile[len("profile:"):].strip()
            if spec:
                parts = [p for p in spec.split("+") if p]
                version = parts[0]
                feats = frozenset(parts[1:])
                profile = DSLGrammarProfile(version=version, features=feats)
    
    # Get tokens from tokenization result
    if tokenization_result.success and tokenization_result.tokens:
        # Convert DSLTokenModel back to DSLToken for parsing
        from .lexer import DSLToken
        tokens = [
            DSLToken(
                type=token.type,
                value=token.value,
                category=token.category,
                line=token.line,
                column=token.column,
            )
            for token in tokenization_result.tokens
        ]
        parse_result = parse_tokens(tokens, original_text=dsl, profile=profile, return_model=True)
    else:
        # If tokenization failed, create a failed parse result
        parse_result = ParseResult.from_error(
            dsl,
            "Cannot parse: tokenization failed",
            line=tokenization_result.error_line,
            column=tokenization_result.error_column,
        )
    
    # If parsing failed, return early
    if not parse_result.success:
        return DSLValidationPipelineResult.from_pipeline(
            dsl,
            tokenization_result,
            parsing=parse_result,
            validation=None,
        )
    
    # Phase 3: Semantic Validation
    validation_result = None
    if schema:
        # Use the same grammar profile for validation
        grammar = grammar_profile
        if uses_relational and not grammar_profile:
            grammar = f"profile:v1+{FEATURE_RELATIONAL_CONSTRAINTS}"
        
        validation_result = validate_dsl_expression_with_schema(
            dsl,
            schema,
            grammar=grammar,
            return_model=True,
        )
    else:
        # Without schema, we can't do full semantic validation
        # But we can still validate syntax-level issues
        validation_result = ValidationResult.from_success(
            dsl,
            inferred_type=None,
            schema_tables=None,
        )
    
    return DSLValidationPipelineResult.from_pipeline(
        dsl,
        tokenization_result,
        parsing=parse_result,
        validation=validation_result,
    )
