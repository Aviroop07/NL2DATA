"""Centralized error handling for DSL validation.

This module provides Pydantic-based error classes for all DSL validation phases:
- Lexical errors (tokenization)
- Syntax errors (parsing)
- Semantic errors (type checking and validation)

All error messages are automatically generated from these structured error classes.
"""

from __future__ import annotations

from typing import List, Optional
from pydantic import BaseModel, Field


class LexicalError(BaseModel):
    """Lexical analysis error (tokenization phase)."""
    
    message: str = Field(description="Error message")
    line: Optional[int] = Field(None, description="Line number where error occurred (1-indexed)")
    column: Optional[int] = Field(None, description="Column number where error occurred (1-indexed)")
    invalid_char: Optional[str] = Field(None, description="Invalid character that caused the error")
    context: Optional[str] = Field(None, description="Context snippet showing error location")
    
    def format_message(self) -> str:
        """Format a comprehensive lexical error message."""
        parts = []
        parts.append(f"Lexical error: {self.message}")
        
        if self.line is not None and self.column is not None:
            parts.append(f"Location: line {self.line}, column {self.column}")
        
        if self.invalid_char:
            char_repr = repr(self.invalid_char) if self.invalid_char else "unknown"
            parts.append(f"Invalid character: {char_repr}")
            if ord(self.invalid_char) > 127:
                parts.append(f"Character code: U+{ord(self.invalid_char):04X}")
        
        if self.context:
            parts.append(f"Context:\n{self.context}")
        
        suggestions = self._get_suggestions()
        if suggestions:
            parts.append("Suggestions:")
            parts.extend(f"  - {s}" for s in suggestions)
        
        return "\n".join(parts)
    
    def _get_suggestions(self) -> List[str]:
        """Get suggestions based on the error."""
        suggestions = []
        if self.invalid_char:
            if self.invalid_char in ['@', '#', '$', '%', '^', '&', '|']:
                suggestions.append("This character is not allowed in DSL expressions.")
                if self.invalid_char == '&':
                    suggestions.append("Did you mean 'AND' for boolean logic?")
                elif self.invalid_char == '|':
                    suggestions.append("Did you mean 'OR' for boolean logic?")
            elif self.invalid_char in ['**', '//']:
                suggestions.append("These operators are not supported in DSL.")
                if self.invalid_char == '**':
                    suggestions.append("Use POW() function for exponentiation.")
        return suggestions


class SyntaxError(BaseModel):
    """Syntax analysis error (parsing phase)."""
    
    message: str = Field(description="Error message")
    line: Optional[int] = Field(None, description="Line number where error occurred (1-indexed)")
    column: Optional[int] = Field(None, description="Column number where error occurred (1-indexed)")
    found: Optional[str] = Field(None, description="Token or character that was found")
    expected: Optional[List[str]] = Field(None, description="List of expected tokens")
    context: Optional[str] = Field(None, description="Context snippet showing error location")
    
    def format_message(self) -> str:
        """Format a comprehensive syntax error message."""
        parts = []
        parts.append(f"Syntax error: {self.message}")
        
        if self.line is not None and self.column is not None:
            parts.append(f"Location: line {self.line}, column {self.column}")
        
        if self.found:
            parts.append(f"Found: {repr(self.found)}")
        
        if self.expected:
            if len(self.expected) == 1:
                parts.append(f"Expected: {self.expected[0]}")
            elif len(self.expected) <= 5:
                parts.append(f"Expected one of: {', '.join(self.expected)}")
            else:
                parts.append(f"Expected one of: {', '.join(self.expected[:5])} (and {len(self.expected) - 5} more)")
        
        if self.context:
            parts.append(f"\nContext:\n{self.context}")
        
        suggestions = self._get_suggestions()
        if suggestions:
            parts.append("\nSuggestions:")
            parts.extend(f"  - {s}" for s in suggestions)
        
        return "\n".join(parts)
    
    def _get_suggestions(self) -> List[str]:
        """Get suggestions based on the error."""
        suggestions = []
        if self.found:
            if self.found in ['(', ')']:
                if self.found == '(':
                    suggestions.append("Check for missing closing parenthesis ')'")
                else:
                    suggestions.append("Check for missing opening parenthesis '('")
            elif self.found in ['[', ']']:
                if self.found == '[':
                    suggestions.append("Check for missing closing bracket ']'")
                else:
                    suggestions.append("Check for missing opening bracket '['")
            elif self.found in ['IF', 'THEN', 'ELSE']:
                suggestions.append("IF expression requires: IF condition THEN expr ELSE expr")
            elif self.found in ['CASE', 'WHEN', 'END']:
                suggestions.append("CASE expression requires: CASE WHEN condition THEN expr [WHEN ...] [ELSE expr] END")
        
        if self.expected:
            if any('identifier' in str(e).lower() for e in self.expected):
                suggestions.append("An identifier (table.column or column name) is expected here")
            if any('number' in str(e).lower() or 'literal' in str(e).lower() for e in self.expected):
                suggestions.append("A numeric or string literal is expected here")
            if any('operator' in str(e).lower() for e in self.expected):
                suggestions.append("An operator (+, -, *, /, =, <, >, etc.) is expected here")
        
        return suggestions


class SemanticErrorDetail(BaseModel):
    """Detailed semantic validation error."""
    
    error_type: str = Field(description="Type/category of error (e.g., 'Type Mismatch', 'Unknown Identifier')")
    message: str = Field(description="Error message")
    identifier: Optional[str] = Field(None, description="Identifier or expression that caused the error")
    expected_type: Optional[str] = Field(None, description="Expected type (if applicable)")
    actual_type: Optional[str] = Field(None, description="Actual type found (if applicable)")
    context: Optional[str] = Field(None, description="Additional context about the error")
    suggestion: Optional[str] = Field(None, description="Suggestion for fixing the error")
    
    def format_message(self) -> str:
        """Format a comprehensive semantic error message."""
        parts = []
        parts.append(f"[{self.error_type}] {self.message}")
        
        if self.identifier:
            parts.append(f"  Expression: {self.identifier}")
        
        if self.expected_type and self.actual_type:
            parts.append(f"  Expected type: {self.expected_type}")
            parts.append(f"  Actual type: {self.actual_type}")
        elif self.expected_type:
            parts.append(f"  Expected type: {self.expected_type}")
        elif self.actual_type:
            parts.append(f"  Actual type: {self.actual_type}")
        
        if self.context:
            parts.append(f"  Context: {self.context}")
        
        if self.suggestion:
            parts.append(f"  Suggestion: {self.suggestion}")
        
        return "\n".join(parts)
    
    @classmethod
    def create_type_mismatch(
        cls,
        message: str,
        identifier: Optional[str] = None,
        expected_type: Optional[str] = None,
        actual_type: Optional[str] = None,
        suggestion: Optional[str] = None,
        context: Optional[str] = None,
    ) -> SemanticErrorDetail:
        """Create a type mismatch error."""
        if not suggestion and expected_type and actual_type:
            suggestion = f"Ensure both operands have the same type, or use type conversion functions"
        return cls(
            error_type="Type Mismatch",
            message=message,
            identifier=identifier,
            expected_type=expected_type,
            actual_type=actual_type,
            context=context,
            suggestion=suggestion,
        )
    
    @classmethod
    def create_unknown_identifier(
        cls,
        message: str,
        identifier: Optional[str] = None,
        suggestion: Optional[str] = None,
    ) -> SemanticErrorDetail:
        """Create an unknown identifier error."""
        return cls(
            error_type="Unknown Identifier",
            message=message,
            identifier=identifier,
            suggestion=suggestion,
        )
    
    @classmethod
    def create_invalid_function(
        cls,
        message: str,
        identifier: Optional[str] = None,
        suggestion: Optional[str] = None,
    ) -> SemanticErrorDetail:
        """Create an invalid function error."""
        return cls(
            error_type="Invalid Function",
            message=message,
            identifier=identifier,
            suggestion=suggestion,
        )
    
    @classmethod
    def create_invalid_distribution(
        cls,
        message: str,
        identifier: Optional[str] = None,
        suggestion: Optional[str] = None,
    ) -> SemanticErrorDetail:
        """Create an invalid distribution error."""
        return cls(
            error_type="Invalid Distribution",
            message=message,
            identifier=identifier,
            suggestion=suggestion,
        )
    
    @classmethod
    def create_invalid_parameter(
        cls,
        message: str,
        identifier: Optional[str] = None,
        suggestion: Optional[str] = None,
        context: Optional[str] = None,
    ) -> SemanticErrorDetail:
        """Create an invalid parameter error."""
        return cls(
            error_type="Invalid Parameter",
            message=message,
            identifier=identifier,
            suggestion=suggestion,
            context=context,
        )


def create_lexical_error(
    message: str,
    line: Optional[int] = None,
    column: Optional[int] = None,
    invalid_char: Optional[str] = None,
    context: Optional[str] = None,
) -> LexicalError:
    """Factory function to create a lexical error."""
    return LexicalError(
        message=message,
        line=line,
        column=column,
        invalid_char=invalid_char,
        context=context,
    )


def create_syntax_error(
    message: str,
    line: Optional[int] = None,
    column: Optional[int] = None,
    found: Optional[str] = None,
    expected: Optional[List[str]] = None,
    context: Optional[str] = None,
) -> SyntaxError:
    """Factory function to create a syntax error."""
    return SyntaxError(
        message=message,
        line=line,
        column=column,
        found=found,
        expected=expected,
        context=context,
    )
