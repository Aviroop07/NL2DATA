"""Lexer/tokenizer for the NL2DATA DSL.

This module provides pure tokenization functionality, independent of parsing.
It uses the same grammar as the parser to ensure consistency, but creates
its own lexer instance to maintain separation of concerns.

Architecture:
- Lexer: Pure tokenization (this module)
- Parser: Uses grammar to parse tokens into AST (parser.py)
- Semantic Analyzer: Validates AST for type/semantic correctness (validator.py)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Literal, Optional, Set, Tuple, Union

from lark import Lark, Token

from .grammar_profile import DSLGrammarProfile, build_dsl_grammar
from .schema_context import DSLSchemaContext
from .models import TokenizationResult, DSLTokenModel
from .errors import LexicalError, create_lexical_error


TokenCategory = Literal["keyword", "operator", "identifier", "literal", "punctuation", "unknown"]


@dataclass(frozen=True)
class DSLToken:
    type: str
    value: str
    category: TokenCategory
    line: Optional[int] = None
    column: Optional[int] = None


_KEYWORDS = {
    "IF",
    "THEN",
    "ELSE",
    "CASE",
    "WHEN",
    "END",
    "AND",
    "OR",
    "NOT",
    "LIKE",
    "IN",
    "IS",
    "TRUE",
    "FALSE",
    "NULL",
    # SQL-like keywords
    "SELECT",
    "FROM",
    "WHERE",
    "GROUP",
    "BY",
    "HAVING",
    "ORDER",
    "LIMIT",
    "EXISTS",
    "DISTINCT",
    "AS",
    "OVER",
    "PARTITION",
    "ROWS",
    "RANGE",
    "BETWEEN",
    "PRECEDING",
    "FOLLOWING",
    "UNBOUNDED",
    "CURRENT",
    "ROW",
    "ASC",
    "DESC",
    # Window function names
    "ROW_NUMBER",
    "RANK",
    "DENSE_RANK",
    "PERCENT_RANK",
    "CUME_DIST",
    "LAG",
    "LEAD",
    "FIRST_VALUE",
    "LAST_VALUE",
    "NTH_VALUE",
    # Relational constraint functions
    "COUNT_WHERE",
    "SUM_WHERE",
    "AVG_WHERE",
    "MIN_WHERE",
    "MAX_WHERE",
    "LOOKUP",
    "IN_RANGE",
    "THIS",
}

_OPERATORS = {
    "EQ",
    "NE",
    "LE",
    "LT",
    "GE",
    "GT",
    "PLUS",
    "MINUS",
    "MUL",
    "DIV",
    "MOD",
}

_LITERALS = {"SIGNED_NUMBER", "ESCAPED_STRING", "SINGLE_QUOTED_STRING", "STRING"}

_PUNCTUATION = {"LPAREN", "RPAREN", "LBRACK", "RBRACK", "COMMA", "TILDE", "DOT", "STAR"}


def _categorize_token_type(token_type: str) -> TokenCategory:
    """Categorize a token type into a semantic category.
    
    Note: CNAME tokens from Lark are categorized as "identifier".
    Based on their position relative to '.' (DOT) tokens, we identify
    them as table names (before '.') or column names (after '.' or standalone).
    """
    if token_type in _KEYWORDS:
        return "keyword"
    if token_type in _OPERATORS:
        return "operator"
    if token_type in _LITERALS:
        return "literal"
    if token_type in _PUNCTUATION:
        return "punctuation"
    if token_type in ("CNAME", "IDENTIFIER"):
        return "identifier"
    return "unknown"


# Lexer cache (separate from parser cache to maintain independence)
_LEXER_CACHE: Dict[Tuple[str, Tuple[str, ...]], Lark] = {}


def _get_lexer(profile: Optional[DSLGrammarProfile] = None) -> Lark:
    """Get a Lark lexer instance for tokenization.
    
    This is separate from the parser to maintain independence.
    Both use the same grammar to ensure consistency.
    """
    prof = profile or DSLGrammarProfile()
    key = (prof.version, tuple(sorted(prof.features)))
    cached = _LEXER_CACHE.get(key)
    if cached is not None:
        return cached
    grammar = build_dsl_grammar(prof)
    # Create a lexer-only instance (we only need lexing, not parsing)
    lexer = Lark(
        grammar,
        parser="lalr",
        lexer="contextual",
        start="start",
        propagate_positions=True,
        maybe_placeholders=False,
    )
    _LEXER_CACHE[key] = lexer
    return lexer


class DSLLexerError(Exception):
    """Exception raised when lexer encounters an error.
    
    This exception wraps a LexicalError from the errors module for backward compatibility.
    """
    
    def __init__(self, message: str, line: Optional[int] = None, column: Optional[int] = None, 
                 context: Optional[str] = None, invalid_char: Optional[str] = None):
        self.message = message
        self.line = line
        self.column = column
        self.context = context
        self.invalid_char = invalid_char
        # Create the lexical error for formatting
        self._lexical_error = create_lexical_error(
            message=message,
            line=line,
            column=column,
            invalid_char=invalid_char,
            context=context,
        )
        super().__init__(self._lexical_error.format_message())
    
    def __str__(self) -> str:
        """Format a comprehensive error message."""
        return self._lexical_error.format_message()


def tokenize_dsl(
    dsl: str,
    profile: Optional[DSLGrammarProfile] = None,
    return_model: bool = False,
) -> Union[List[DSLToken], TokenizationResult]:
    """Tokenize a DSL string into a list of typed tokens.
    
    This is a pure lexer function - it only tokenizes and does not parse.
    It is independent of the parser module.

    Args:
        dsl: The DSL expression string to tokenize
        profile: Optional grammar profile (for extensions)
        return_model: If True, returns TokenizationResult (Pydantic model) instead of List[DSLToken]

    Returns:
        If return_model=False: List of DSLToken objects representing the tokenized expression
        If return_model=True: TokenizationResult with structured tokenization information

    Raises:
        DSLLexerError: If tokenization fails due to invalid characters or syntax

    Notes:
    - Identifiers (CNAME tokens) are categorized as "identifier"
    - Identifiers like `table.column` are emitted as separate tokens:
      CNAME (table), DOT (.), CNAME (column)
    - Based on position relative to '.' (DOT):
      * Identifier before '.' → table name
      * Identifier after '.' → column name
      * Identifier with no '.' → column name (default)
    """
    text = (dsl or "").strip()
    if not text:
        if return_model:
            return TokenizationResult.from_error(text, "Empty or whitespace-only expression")
        return []
    
    lexer = _get_lexer(profile=profile)
    
    # Store original Lark tokens for parser compatibility
    lark_tokens: List[Token] = []
    
    try:
        toks: List[Token] = list(lexer.lex(text))
        lark_tokens = toks  # Keep reference to original Lark tokens
    except Exception as e:
        # Try to extract position information from the error
        error_msg = str(e)
        line = None
        column = None
        invalid_char = None
        context = None
        
        # Try to extract line/column from error message or exception attributes
        if hasattr(e, 'line'):
            line = getattr(e, 'line', None)
        if hasattr(e, 'column'):
            column = getattr(e, 'column', None)
        
        # Try to find invalid character
        if line and column:
            lines = text.splitlines()
            if line <= len(lines):
                line_text = lines[line - 1]
                if column <= len(line_text):
                    invalid_char = line_text[column - 1]
                    # Create context snippet
                    start = max(0, column - 20)
                    end = min(len(line_text), column + 20)
                    snippet = line_text[start:end]
                    pointer = " " * (column - 1 - start) + "^"
                    context = f"  {snippet}\n  {pointer}"
        
        if return_model:
            return TokenizationResult.from_error(text, error_msg, line, column)
        
        raise DSLLexerError(
            f"Failed to tokenize DSL expression: {error_msg}",
            line=line,
            column=column,
            context=context,
            invalid_char=invalid_char
        ) from e
    
    out: List[DSLToken] = []
    for t in toks:
        # Normalize CNAME to IDENTIFIER for clarity
        token_type = str(t.type)
        if token_type == "CNAME":
            token_type = "IDENTIFIER"
        
        out.append(
            DSLToken(
                type=token_type,
                value=str(t.value),
                category=_categorize_token_type(str(t.type)),  # Use original for categorization
                line=getattr(t, "line", None),
                column=getattr(t, "column", None),
            )
        )
    
    if return_model:
        return TokenizationResult.from_tokens(out, text)
    return out


@dataclass(frozen=True)
class IdentifierClassification:
    """Classification of an identifier from a DSL expression."""
    identifier: str
    is_table: bool
    is_column: bool
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    is_qualified: bool = False


def extract_table_and_column_names(
    dsl: str,
    schema_context: Optional[DSLSchemaContext] = None,
) -> Dict[str, IdentifierClassification]:
    """Extract and classify table and column names from a DSL expression.

    This function uses tokenization to extract identifiers. For more accurate
    extraction, it may use parsing (via parser module), but the lexer itself
    remains independent.

    Args:
        dsl: The DSL expression to analyze
        schema_context: Optional schema context for resolving ambiguous identifiers

    Returns:
        Dictionary mapping identifier strings to their classifications

    Examples:
        >>> result = extract_table_and_column_names("orders.total_amount + tax")
        >>> result["orders.total_amount"].is_qualified  # True (qualified identifier)
        >>> result["orders.total_amount"].table_name  # "orders"
        >>> result["orders.total_amount"].column_name  # "total_amount"

        >>> result = extract_table_and_column_names("quantity * price", schema_context)
        >>> # For bare identifiers, schema context is used to resolve them
    """
    text = (dsl or "").strip()
    if not text:
        return {}

    classifications: Dict[str, IdentifierClassification] = {}

    try:
        # Import parser here to avoid circular dependency and maintain separation
        from .parser import parse_tokens
        
        # Tokenize first, then parse tokens (proper separation: lexer → tokens → parser)
        tokens = tokenize_dsl(text, profile=profile, return_model=False)
        tree = parse_tokens(tokens, original_text=text, profile=profile)

        # Extract all identifiers from the parse tree
        # We need to walk the tree to find identifier nodes, but skip function/distribution names
        def ident_str(ident_node) -> str:
            """Extract identifier string from an identifier node."""
            parts = []
            for c in getattr(ident_node, "children", []) or []:
                val = str(getattr(c, "value", ""))
                if val:
                    parts.append(val)
            return ".".join(parts)

        def walk(node) -> None:
            """Walk the parse tree to extract identifiers."""
            node_data = getattr(node, "data", None)

            # Skip function/distribution names (first child of func_call/dist_call)
            if node_data == "func_call":
                children = getattr(node, "children", []) or []
                for i, ch in enumerate(children):
                    if i == 0 and getattr(ch, "data", None) == "identifier":
                        # This is the function name, skip it
                        continue
                    walk(ch)
                return

            if node_data == "dist_call":
                children = getattr(node, "children", []) or []
                for i, ch in enumerate(children):
                    if i == 0 and getattr(ch, "data", None) == "identifier":
                        # This is the distribution name, skip it
                        continue
                    walk(ch)
                return

            # Process identifier nodes
            if node_data == "identifier":
                ident = ident_str(node)
                if ident and ident not in classifications:
                    # Classify this identifier
                    classification = _classify_identifier(ident, schema_context)
                    classifications[ident] = classification
                return

            # Recursively walk children
            for ch in getattr(node, "children", []) or []:
                walk(ch)

        walk(tree)

    except Exception:
        # If parsing fails, fall back to lexer-only approach
        # This is less accurate but can still extract some information
        tokens = tokenize_dsl(text)
        identifiers = _extract_identifiers_from_tokens(tokens)
        for ident in identifiers:
            if ident not in classifications:
                classification = _classify_identifier(ident, schema_context)
                classifications[ident] = classification

    return classifications


def _extract_identifiers_from_tokens(tokens: List[DSLToken]) -> Set[str]:
    """Extract identifier strings from token stream, handling qualified identifiers.

    Rules:
    - IDENTIFIER tokens (CNAME from Lark) are extracted
    - If IDENTIFIER is followed by DOT then IDENTIFIER → reconstruct as "table.column"
    - If IDENTIFIER has no DOT → treat as column name
    
    Note: This is a best-effort approach. For accurate extraction, parsing is preferred.
    """
    identifiers: Set[str] = set()
    i = 0
    while i < len(tokens):
        token = tokens[i]
        # Check for IDENTIFIER or CNAME (both represent identifiers)
        if token.type in ("IDENTIFIER", "CNAME"):
            # Check if this is part of a qualified identifier (table.column)
            if i + 1 < len(tokens) and tokens[i + 1].type == "DOT" and i + 2 < len(tokens):
                # This is a qualified identifier: table.column
                next_token = tokens[i + 2]
                if next_token.type in ("IDENTIFIER", "CNAME"):
                    qualified = f"{token.value}.{next_token.value}"
                    identifiers.add(qualified)
                    i += 3  # Skip IDENTIFIER, DOT, IDENTIFIER
                    continue
            # Bare identifier (no following DOT) - treat as column
            identifiers.add(token.value)
        i += 1
    return identifiers


def _classify_identifier(
    identifier: str,
    schema_context: Optional[DSLSchemaContext] = None,
) -> IdentifierClassification:
    """Classify an identifier as table, column, or both.
    
    Rules:
    - If identifier contains '.': Table.Column format
      * Part before '.' → table name
      * Part after '.' → column name
    - If identifier has no '.': treated as column name (default)
      * Schema context can be used to resolve the actual table if needed
    """
    parts = [p for p in identifier.split(".") if p]

    if len(parts) == 1:
        # Bare identifier - no '.' present, so it's a column name by default
        bare_name = parts[0]
        if schema_context:
            table, column, _, error = schema_context.resolve_identifier(bare_name)
            if not error and table and column:
                return IdentifierClassification(
                    identifier=identifier,
                    is_table=False,
                    is_column=True,
                    table_name=table,
                    column_name=column,
                    is_qualified=False,
                )
        # Without schema context, treat as column (default behavior)
        return IdentifierClassification(
            identifier=identifier,
            is_table=False,
            is_column=True,  # Default: bare identifier is a column
            is_qualified=False,
        )

    if len(parts) == 2:
        # Qualified identifier: Table.Column
        # Part before '.' is table name, part after '.' is column name
        table_name, column_name = parts
        is_table = False
        is_column = True

        # Verify against schema context if available
        if schema_context:
            table, column, _, error = schema_context.resolve_identifier(identifier)
            if error:
                # Invalid identifier
                is_column = False
            else:
                # Valid identifier
                table_name = table or table_name
                column_name = column or column_name

        return IdentifierClassification(
            identifier=identifier,
            is_table=is_table,
            is_column=is_column,
            table_name=table_name,
            column_name=column_name,
            is_qualified=True,
        )

    # Unsupported format (e.g., schema.table.column)
    return IdentifierClassification(
        identifier=identifier,
        is_table=False,
        is_column=False,
        is_qualified=False,
    )


def get_table_names_from_expression(
    dsl: str,
    schema_context: Optional[DSLSchemaContext] = None,
) -> Set[str]:
    """Extract table names from a DSL expression using the lexer.

    Returns:
        Set of table names found in the expression
    """
    classifications = extract_table_and_column_names(dsl, schema_context)
    table_names = set()
    for classification in classifications.values():
        if classification.is_table or (classification.table_name and classification.is_qualified):
            if classification.table_name:
                table_names.add(classification.table_name)
    return table_names


def get_column_names_from_expression(
    dsl: str,
    schema_context: Optional[DSLSchemaContext] = None,
) -> Set[str]:
    """Extract column names from a DSL expression using the lexer.

    Returns:
        Set of column names found in the expression
    """
    classifications = extract_table_and_column_names(dsl, schema_context)
    column_names = set()
    for classification in classifications.values():
        if classification.is_column and classification.column_name:
            column_names.add(classification.column_name)
    return column_names

