"""Pydantic models for DSL validation pipeline intermediate steps.

This module defines structured data models for representing intermediate
results at each stage of the DSL validation pipeline:
- Lexer: TokenizationResult
- Parser: ParseResult
- Semantic Analyzer: ValidationResult
- Full Pipeline: DSLValidationPipelineResult

These models provide type safety, validation, and serialization capabilities
for the DSL validation process.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, ConfigDict

from .errors import SemanticErrorDetail


# ============================================================================
# Enums for Status and Error Types
# ============================================================================

class ValidationStage(str, Enum):
    """Stages of the DSL validation pipeline."""
    LEXICAL = "lexical"
    SYNTAX = "syntax"
    SEMANTIC = "semantic"
    COMPLETE = "complete"


class ErrorSeverity(str, Enum):
    """Severity levels for validation errors."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


# ============================================================================
# Lexer Models
# ============================================================================

class DSLTokenModel(BaseModel):
    """Pydantic model for a DSL token."""
    
    type: str = Field(description="Token type (e.g., 'IDENTIFIER', 'PLUS', 'LPAREN')")
    value: str = Field(description="Token value (the actual text)")
    category: Literal["keyword", "operator", "identifier", "literal", "punctuation", "unknown"] = Field(
        description="Semantic category of the token"
    )
    line: Optional[int] = Field(None, description="Line number where token appears (1-indexed)")
    column: Optional[int] = Field(None, description="Column number where token appears (1-indexed)")
    
    model_config = ConfigDict(frozen=True)


class TokenizationResult(BaseModel):
    """Result of lexical analysis (tokenization) phase."""
    
    success: bool = Field(description="Whether tokenization succeeded")
    tokens: List[DSLTokenModel] = Field(default_factory=list, description="List of tokens produced")
    original_expression: str = Field(description="Original DSL expression that was tokenized")
    token_count: int = Field(description="Total number of tokens")
    error: Optional[str] = Field(None, description="Error message if tokenization failed")
    error_line: Optional[int] = Field(None, description="Line number where error occurred")
    error_column: Optional[int] = Field(None, description="Column number where error occurred")
    timestamp: datetime = Field(default_factory=datetime.now, description="When tokenization was performed")
    
    @classmethod
    def from_tokens(cls, tokens: List[Any], expression: str) -> TokenizationResult:
        """Create TokenizationResult from a list of DSLToken objects."""
        token_models = [
            DSLTokenModel(
                type=token.type,
                value=token.value,
                category=token.category,
                line=token.line,
                column=token.column,
            )
            for token in tokens
        ]
        return cls(
            success=True,
            tokens=token_models,
            original_expression=expression,
            token_count=len(token_models),
        )
    
    @classmethod
    def from_error(cls, expression: str, error: str, line: Optional[int] = None, column: Optional[int] = None) -> TokenizationResult:
        """Create TokenizationResult from an error."""
        return cls(
            success=False,
            tokens=[],
            original_expression=expression,
            token_count=0,
            error=error,
            error_line=line,
            error_column=column,
        )


# ============================================================================
# Parser Models
# ============================================================================

class ParseErrorDetail(BaseModel):
    """Detailed information about a parse error."""
    
    message: str = Field(description="Error message")
    line: Optional[int] = Field(None, description="Line number where error occurred")
    column: Optional[int] = Field(None, description="Column number where error occurred")
    found: Optional[str] = Field(None, description="Token or character that was found")
    expected: Optional[List[str]] = Field(None, description="List of expected tokens")
    context: Optional[str] = Field(None, description="Context snippet showing error location")
    suggestions: Optional[List[str]] = Field(None, description="Suggestions for fixing the error")


class ParseResult(BaseModel):
    """Result of syntax analysis (parsing) phase."""
    
    success: bool = Field(description="Whether parsing succeeded")
    ast_available: bool = Field(False, description="Whether AST is available (only if parsing succeeded)")
    original_expression: str = Field(description="Original DSL expression that was parsed")
    error: Optional[ParseErrorDetail] = Field(None, description="Detailed error information if parsing failed")
    timestamp: datetime = Field(default_factory=datetime.now, description="When parsing was performed")
    
    # Metadata about the parse tree (if successful)
    tree_depth: Optional[int] = Field(None, description="Depth of the parse tree")
    node_count: Optional[int] = Field(None, description="Number of nodes in the parse tree")
    
    @classmethod
    def from_success(cls, expression: str, tree_depth: Optional[int] = None, node_count: Optional[int] = None) -> ParseResult:
        """Create ParseResult from a successful parse."""
        return cls(
            success=True,
            ast_available=True,
            original_expression=expression,
            tree_depth=tree_depth,
            node_count=node_count,
        )
    
    @classmethod
    def from_error(
        cls,
        expression: str,
        error_message: str,
        line: Optional[int] = None,
        column: Optional[int] = None,
        found: Optional[str] = None,
        expected: Optional[List[str]] = None,
        context: Optional[str] = None,
        suggestions: Optional[List[str]] = None,
    ) -> ParseResult:
        """Create ParseResult from a parse error."""
        return cls(
            success=False,
            ast_available=False,
            original_expression=expression,
            error=ParseErrorDetail(
                message=error_message,
                line=line,
                column=column,
                found=found,
                expected=expected,
                context=context,
                suggestions=suggestions,
            ),
        )


# ============================================================================
# Semantic Analyzer Models
# ============================================================================

class SemanticError(BaseModel):
    """A single semantic validation error.
    
    This is a wrapper around SemanticErrorDetail that adds severity.
    For creating errors, use SemanticErrorDetail factory methods from errors module.
    """
    
    error_type: str = Field(description="Type/category of error (e.g., 'Type Mismatch', 'Unknown Identifier')")
    message: str = Field(description="Error message")
    identifier: Optional[str] = Field(None, description="Identifier or expression that caused the error")
    expected_type: Optional[str] = Field(None, description="Expected type (if applicable)")
    actual_type: Optional[str] = Field(None, description="Actual type found (if applicable)")
    context: Optional[str] = Field(None, description="Additional context about the error")
    suggestion: Optional[str] = Field(None, description="Suggestion for fixing the error")
    severity: ErrorSeverity = Field(ErrorSeverity.ERROR, description="Severity of the error")
    
    @classmethod
    def from_detail(cls, detail: SemanticErrorDetail, severity: ErrorSeverity = ErrorSeverity.ERROR) -> SemanticError:
        """Create SemanticError from SemanticErrorDetail."""
        return cls(
            error_type=detail.error_type,
            message=detail.message,
            identifier=detail.identifier,
            expected_type=detail.expected_type,
            actual_type=detail.actual_type,
            context=detail.context,
            suggestion=detail.suggestion,
            severity=severity,
        )


class ValidationResult(BaseModel):
    """Result of semantic analysis (type checking and validation) phase."""
    
    valid: bool = Field(description="Whether the expression passed all semantic checks")
    original_expression: str = Field(description="Original DSL expression that was validated")
    errors: List[SemanticError] = Field(default_factory=list, description="List of semantic errors found")
    error_count: int = Field(0, description="Total number of errors")
    warnings: List[SemanticError] = Field(default_factory=list, description="List of warnings (non-fatal issues)")
    timestamp: datetime = Field(default_factory=datetime.now, description="When validation was performed")
    
    # Type inference results (if available)
    inferred_type: Optional[str] = Field(None, description="Inferred type of the expression")
    
    # Schema information used for validation
    schema_tables: Optional[List[str]] = Field(None, description="List of table names in the schema used for validation")
    
    @classmethod
    def from_success(
        cls,
        expression: str,
        inferred_type: Optional[str] = None,
        schema_tables: Optional[List[str]] = None,
    ) -> ValidationResult:
        """Create ValidationResult from successful validation."""
        return cls(
            valid=True,
            original_expression=expression,
            errors=[],
            error_count=0,
            inferred_type=inferred_type,
            schema_tables=schema_tables,
        )
    
    @classmethod
    def from_errors(
        cls,
        expression: str,
        errors: List[SemanticError],
        warnings: Optional[List[SemanticError]] = None,
        inferred_type: Optional[str] = None,
        schema_tables: Optional[List[str]] = None,
    ) -> ValidationResult:
        """Create ValidationResult from validation errors."""
        return cls(
            valid=False,
            original_expression=expression,
            errors=errors,
            error_count=len(errors),
            warnings=warnings or [],
            inferred_type=inferred_type,
            schema_tables=schema_tables,
        )
    
    def get_error_summary(self) -> str:
        """Get a formatted summary of all errors."""
        if not self.errors:
            return "No errors found."
        
        if len(self.errors) == 1:
            err = self.errors[0]
            parts = [f"[{err.error_type}] {err.message}"]
            if err.identifier:
                parts.append(f"  Expression: {err.identifier}")
            if err.expected_type and err.actual_type:
                parts.append(f"  Expected: {err.expected_type}, Actual: {err.actual_type}")
            if err.suggestion:
                parts.append(f"  Suggestion: {err.suggestion}")
            return "\n".join(parts)
        else:
            parts = [f"Found {len(self.errors)} semantic error(s):"]
            for i, err in enumerate(self.errors, 1):
                parts.append(f"\n{i}. [{err.error_type}] {err.message}")
                if err.identifier:
                    parts.append(f"   Expression: {err.identifier}")
                if err.expected_type and err.actual_type:
                    parts.append(f"   Expected: {err.expected_type}, Actual: {err.actual_type}")
                if err.suggestion:
                    parts.append(f"   Suggestion: {err.suggestion}")
            return "\n".join(parts)


# ============================================================================
# Full Pipeline Model
# ============================================================================

class DSLValidationPipelineResult(BaseModel):
    """Complete result of the DSL validation pipeline (all three phases)."""
    
    original_expression: str = Field(description="Original DSL expression that was validated")
    timestamp: datetime = Field(default_factory=datetime.now, description="When validation pipeline was executed")
    
    # Results from each phase
    tokenization: TokenizationResult = Field(description="Result from lexical analysis phase")
    parsing: Optional[ParseResult] = Field(None, description="Result from syntax analysis phase (None if tokenization failed)")
    validation: Optional[ValidationResult] = Field(None, description="Result from semantic analysis phase (None if parsing failed)")
    
    # Overall status
    overall_success: bool = Field(description="Whether all phases passed successfully")
    stage_failed: Optional[ValidationStage] = Field(None, description="First stage that failed (None if all passed)")
    
    # Summary statistics
    total_tokens: int = Field(0, description="Total number of tokens produced")
    total_errors: int = Field(0, description="Total number of errors across all phases")
    total_warnings: int = Field(0, description="Total number of warnings")
    
    def get_failure_stage(self) -> Optional[ValidationStage]:
        """Get the stage where validation failed, or None if all passed."""
        if not self.tokenization.success:
            return ValidationStage.LEXICAL
        if not self.parsing or not self.parsing.success:
            return ValidationStage.SYNTAX
        if not self.validation or not self.validation.valid:
            return ValidationStage.SEMANTIC
        return ValidationStage.COMPLETE
    
    def get_summary(self) -> str:
        """Get a human-readable summary of the validation pipeline result."""
        parts = [f"DSL Validation Pipeline Result for: {self.original_expression[:50]}..."]
        parts.append(f"Overall Status: {'SUCCESS' if self.overall_success else 'FAILED'}")
        
        if self.stage_failed:
            parts.append(f"Failed at stage: {self.stage_failed.value}")
        
        parts.append("\nPhase Results:")
        parts.append(f"  1. Lexical (Tokenization): {'PASSED' if self.tokenization.success else 'FAILED'}")
        if self.tokenization.success:
            parts.append(f"     - Tokens: {self.total_tokens}")
        else:
            parts.append(f"     - Error: {self.tokenization.error}")
        
        if self.parsing:
            parts.append(f"  2. Syntax (Parsing): {'PASSED' if self.parsing.success else 'FAILED'}")
            if not self.parsing.success and self.parsing.error:
                parts.append(f"     - Error: {self.parsing.error.message}")
        
        if self.validation:
            parts.append(f"  3. Semantic (Validation): {'PASSED' if self.validation.valid else 'FAILED'}")
            if not self.validation.valid:
                parts.append(f"     - Errors: {self.validation.error_count}")
                parts.append(f"     - Summary: {self.validation.get_error_summary()[:200]}...")
        
        return "\n".join(parts)
    
    @classmethod
    def from_pipeline(
        cls,
        expression: str,
        tokenization: TokenizationResult,
        parsing: Optional[ParseResult] = None,
        validation: Optional[ValidationResult] = None,
    ) -> DSLValidationPipelineResult:
        """Create a pipeline result from individual phase results."""
        overall_success = (
            tokenization.success
            and (parsing is None or parsing.success)
            and (validation is None or validation.valid)
        )
        
        stage_failed = None
        if not tokenization.success:
            stage_failed = ValidationStage.LEXICAL
        elif parsing and not parsing.success:
            stage_failed = ValidationStage.SYNTAX
        elif validation and not validation.valid:
            stage_failed = ValidationStage.SEMANTIC
        
        total_errors = 0
        total_warnings = 0
        if validation:
            total_errors = validation.error_count
            total_warnings = len(validation.warnings)
        
        return cls(
            original_expression=expression,
            tokenization=tokenization,
            parsing=parsing,
            validation=validation,
            overall_success=overall_success,
            stage_failed=stage_failed,
            total_tokens=tokenization.token_count,
            total_errors=total_errors,
            total_warnings=total_warnings,
        )


# ============================================================================
# Column-Bound DSL Model
# ============================================================================

class DSLKind(str, Enum):
    """Kinds of DSL expressions."""
    CONSTRAINT = "constraint"
    DERIVED = "derived"
    DISTRIBUTION = "distribution"


class ColumnBoundDSL(BaseModel):
    """Column-bound DSL expression with anchor table and column context.
    
    Every DSL expression is always associated with a specific column (anchor column)
    in a specific table (anchor table). This enables anchor-first identifier resolution
    where bare identifiers resolve to columns in the anchor table by default.
    
    Attributes:
        anchor_table: The table that contains the anchor column
        anchor_column: The column this DSL expression is bound to
        dsl_kind: The kind of DSL expression (CONSTRAINT, DERIVED, DISTRIBUTION)
        profile: Grammar profile (e.g., "base", "profile:v1+relational_constraints")
        expression: The DSL expression string
    """
    
    anchor_table: str = Field(description="Table name containing the anchor column")
    anchor_column: str = Field(description="Column name this DSL expression is bound to")
    dsl_kind: DSLKind = Field(description="Kind of DSL expression")
    profile: str = Field(default="base", description="Grammar profile for parsing")
    expression: str = Field(description="DSL expression string")
    
    model_config = ConfigDict(frozen=True)
    
    def __str__(self) -> str:
        """String representation of the column-bound DSL."""
        return f"{self.anchor_table}.{self.anchor_column} :: {self.dsl_kind.value} :: {self.expression}"
