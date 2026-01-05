"""Domain-specific language (DSL) for derivations, decompositions, and generators.

This package provides:
- A formal grammar
- A lexer (tokenization) - produces tokens from DSL strings
- A parser (deterministic) - works ONLY on tokens from lexer, not raw strings
- A validator (deterministic) - semantic analysis
- Pydantic models for structured intermediate results
- Complete validation pipeline

Architecture:
- Lexer: tokenize_dsl() - converts DSL string → tokens
- Parser: parse_tokens() - converts tokens → AST (PRIMARY function)
- Parser: parse_dsl_expression() - DEPRECATED, for backward compatibility only
- Semantic Analyzer: validate_dsl_expression_with_schema() - validates AST
- Pipeline: validate_dsl_pipeline() - runs all three phases
"""

from .validator import validate_dsl_expression_strict, validate_column_bound_dsl, validate_dsl_expression_with_schema
from .lexer import tokenize_dsl, DSLToken
from .parser import parse_tokens, parse_dsl_expression  # parse_dsl_expression is deprecated
from .models import (
    DSLTokenModel,
    TokenizationResult,
    ParseResult,
    ParseErrorDetail,
    ValidationResult,
    SemanticError,
    DSLValidationPipelineResult,
    ValidationStage,
    ErrorSeverity,
    ColumnBoundDSL,
    DSLKind,
)
from .pipeline import validate_dsl_pipeline
from .errors import LexicalError, SyntaxError, SemanticErrorDetail, create_lexical_error, create_syntax_error

__all__ = [
    "validate_dsl_expression_strict",
    "validate_column_bound_dsl",  # Primary entry point for column-bound DSL validation
    "validate_dsl_expression_with_schema",  # Full semantic validation (supports anchor context)
    # Lexer
    "tokenize_dsl",
    "DSLToken",
    # Parser (parse_tokens is primary, parse_dsl_expression is deprecated)
    "parse_tokens",
    "parse_dsl_expression",  # Deprecated, use tokenize_dsl + parse_tokens
    # Pydantic models
    "DSLTokenModel",
    "TokenizationResult",
    "ParseResult",
    "ParseErrorDetail",
    "ValidationResult",
    "SemanticError",
    "DSLValidationPipelineResult",
    "ValidationStage",
    "ErrorSeverity",
    "ColumnBoundDSL",  # Column-bound DSL model
    "DSLKind",  # DSL kind enum (CONSTRAINT, DERIVED, DISTRIBUTION)
    # Pipeline function
    "validate_dsl_pipeline",
    # Error classes
    "LexicalError",
    "SyntaxError",
    "SemanticErrorDetail",
    "create_lexical_error",
    "create_syntax_error",
]

