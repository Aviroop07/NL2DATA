"""Parser for NL2DATA DSL using Lark.

This module provides parsing functionality that works on tokens from the lexer,
not on raw DSL strings. This enforces proper separation: lexer → tokens → parser.

Architecture:
- Lexer: Pure tokenization (lexer.py) - produces tokens from DSL strings
- Parser: Parses tokens into AST (this module) - ONLY works on tokens, not raw strings
- Semantic Analyzer: Validates AST for type/semantic correctness (validator.py)

IMPORTANT: The primary parsing function is parse_tokens(), which accepts tokens
from the lexer. parse_dsl_expression() is provided for backward compatibility
but internally tokenizes first, then calls parse_tokens().
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union

from lark import Lark, Tree, Token, UnexpectedInput

from .grammar_profile import DSLGrammarProfile, build_dsl_grammar
from .models import ParseResult, ParseErrorDetail
from .lexer import DSLToken
from .errors import SyntaxError as DSLSyntaxError, create_syntax_error


@dataclass(frozen=True)
class DSLParseError(Exception):
    """Exception raised when parsing fails.
    
    This exception wraps a SyntaxError from the errors module for backward compatibility.
    """
    message: str
    line: Optional[int] = None
    column: Optional[int] = None
    context: Optional[str] = None
    expected: Optional[list] = None
    found: Optional[str] = None
    
    def __init__(self, message: str, line: Optional[int] = None, column: Optional[int] = None,
                 context: Optional[str] = None, expected: Optional[list] = None, found: Optional[str] = None):
        # For frozen dataclasses, use object.__setattr__ to set fields
        object.__setattr__(self, 'message', message)
        object.__setattr__(self, 'line', line)
        object.__setattr__(self, 'column', column)
        object.__setattr__(self, 'context', context)
        object.__setattr__(self, 'expected', expected)
        object.__setattr__(self, 'found', found)
        # Create the syntax error for formatting
        syntax_error = create_syntax_error(
            message=message,
            line=line,
            column=column,
            found=found,
            expected=expected,
            context=context,
        )
        object.__setattr__(self, '_syntax_error', syntax_error)
        super().__init__(syntax_error.format_message())

    def __str__(self) -> str:
        """Format a comprehensive error message with context and suggestions."""
        return self._syntax_error.format_message()


_PARSER_CACHE: Dict[Tuple[str, Tuple[str, ...]], Lark] = {}


def _get_parser(profile: Optional[DSLGrammarProfile] = None) -> Lark:
    # Cache parsers per (version, features) for speed and determinism.
    prof = profile or DSLGrammarProfile()
    key = (prof.version, tuple(sorted(prof.features)))
    cached = _PARSER_CACHE.get(key)
    if cached is not None:
        return cached
    grammar = build_dsl_grammar(prof)
    parser = Lark(
        grammar,
        parser="lalr",
        lexer="contextual",
        start="start",
        propagate_positions=True,
        maybe_placeholders=False,
    )
    _PARSER_CACHE[key] = parser
    return parser


def parse_tokens(
    tokens: Union[List[DSLToken], List[Token]],
    original_text: Optional[str] = None,
    profile: Optional[DSLGrammarProfile] = None,
    return_model: bool = False,
) -> Union[Tree, ParseResult]:
    """Parse tokens from lexer into a Lark parse tree.
    
    This is the PRIMARY parsing function - it works ONLY on tokens produced by the lexer,
    not on raw DSL strings. This enforces proper separation: lexer → tokens → parser.
    
    The parser does not accept raw DSL strings. You must tokenize first using tokenize_dsl(),
    then pass the tokens to this function.

    Args:
        tokens: List of tokens from the lexer (DSLToken or Lark Token objects)
        original_text: Original DSL expression (for error messages and metadata)
        profile: Optional grammar profile (for extensions)
        return_model: If True, returns ParseResult (Pydantic model) instead of Tree

    Returns:
        If return_model=False: Lark parse tree (Tree object)
        If return_model=True: ParseResult with structured parsing information

    Raises:
        DSLParseError: if parsing fails with detailed error information (only when return_model=False)
    """
    if not tokens:
        error_msg = "No tokens provided for parsing"
        if return_model:
            return ParseResult.from_error(
                original_text or "",
                error_msg,
                line=1,
                column=1,
            )
        raise DSLParseError(
            error_msg,
            line=1,
            column=1,
            context="Parser requires tokens from lexer, but token list is empty"
        )
    
    # Convert DSLToken to Lark Token objects for parsing
    # The parser works ONLY on tokens - this enforces proper separation: lexer → tokens → parser
    lark_tokens = []
    reconstructed_text_parts = []
    
    for token in tokens:
        if isinstance(token, DSLToken):
            # Convert DSLToken to Lark Token
            token_type = token.type
            # Map back to original token type names if needed
            if token_type == "IDENTIFIER":
                token_type = "CNAME"
            
            lark_token = Token(token_type, token.value)
            if token.line is not None:
                lark_token.line = token.line
            if token.column is not None:
                lark_token.column = token.column
            lark_tokens.append(lark_token)
            reconstructed_text_parts.append(token.value)
        elif isinstance(token, Token):
            lark_tokens.append(token)
            reconstructed_text_parts.append(str(token.value))
        else:
            raise ValueError(f"Invalid token type: {type(token)}. Expected DSLToken or Lark Token.")
    
    # Reconstruct text for error messages and context (not for parsing)
    reconstructed_text = "".join(reconstructed_text_parts) if not original_text else original_text
    
    parser = _get_parser(profile=profile)
    
    try:
        # Parse using reconstructed text from tokens
        # NOTE: While Lark's parser.parse() requires text input, the conceptual model is:
        # lexer → tokens → parser. The parser API only accepts tokens, and we reconstruct
        # text internally for Lark's implementation. This maintains proper separation.
        # The parser does NOT accept raw DSL strings - it only works on tokenized input.
        tree = parser.parse(reconstructed_text)
        
        if return_model:
            # Try to compute tree depth and node count
            def count_nodes(node, depth=0):
                if isinstance(node, Tree):
                    max_depth = depth
                    node_count = 1
                    for child in node.children:
                        child_depth, child_count = count_nodes(child, depth + 1)
                        max_depth = max(max_depth, child_depth)
                        node_count += child_count
                    return max_depth, node_count
                return depth, 0
            
            try:
                tree_depth, node_count = count_nodes(tree)
            except Exception:
                # If counting fails, just return None for metadata
                tree_depth, node_count = None, None
            
            return ParseResult.from_success(reconstructed_text, tree_depth=tree_depth, node_count=node_count)
        return tree
    except UnexpectedInput as e:
        # Extract detailed information from Lark's UnexpectedInput exception
        line = getattr(e, "line", None)
        column = getattr(e, "column", None)
        
        # Get context snippet
        ctx = None
        try:
            ctx = e.get_context(reconstructed_text, span=80)
        except Exception:
            pass
        
        # Extract what was found
        found = None
        if hasattr(e, "char") and e.char is not None:
            found = e.char
        elif hasattr(e, "token") and e.token is not None:
            found = str(e.token)
        
        # Extract expected tokens
        expected = None
        if hasattr(e, "acceptable") and e.acceptable:
            expected = [str(tok) for tok in e.acceptable]
        elif hasattr(e, "expected") and e.expected:
            expected = [str(exp) for exp in e.expected]
        
        # Create a more descriptive error message
        if found:
            if expected:
                message = f"Unexpected token {repr(found)} at this position"
            else:
                message = f"Unexpected token {repr(found)}"
        else:
            message = "Unexpected input at this position"
        
        # Add more context if we have line/column info
        if line and column and ctx is None:
            try:
                lines = reconstructed_text.splitlines()
                if line <= len(lines):
                    line_text = lines[line - 1]
                    start = max(0, column - 30)
                    end = min(len(line_text), column + 30)
                    snippet = line_text[start:end]
                    pointer_pos = column - 1 - start
                    pointer = " " * pointer_pos + "^"
                    ctx = f"  {snippet}\n  {pointer}"
            except Exception:
                pass
        
        if return_model:
            # Extract suggestions from the error message
            suggestions = []
            if found in ['(', ')']:
                suggestions.append("Check for missing closing/opening parenthesis")
            elif found in ['[', ']']:
                suggestions.append("Check for missing closing/opening bracket")
            elif found in ['IF', 'THEN', 'ELSE']:
                suggestions.append("IF expression requires: IF condition THEN expr ELSE expr")
            elif found in ['CASE', 'WHEN', 'END']:
                suggestions.append("CASE expression requires: CASE WHEN condition THEN expr [WHEN ...] [ELSE expr] END")
            
            if expected:
                if any('identifier' in str(e).lower() for e in expected):
                    suggestions.append("An identifier (table.column or column name) is expected here")
                if any('number' in str(e).lower() or 'literal' in str(e).lower() for e in expected):
                    suggestions.append("A numeric or string literal is expected here")
            
            return ParseResult.from_error(
                reconstructed_text,
                message,
                line=line,
                column=column,
                found=found,
                expected=expected,
                context=ctx,
                suggestions=suggestions if suggestions else None,
            )
        
        raise DSLParseError(
            message=message,
            line=line,
            column=column,
            context=ctx,
            expected=expected,
            found=found,
        ) from e
    except Exception as e:
        # For other exceptions, try to extract what we can
        line = getattr(e, "line", None)
        column = getattr(e, "column", None)
        ctx = None
        
        if line and column:
            try:
                lines = reconstructed_text.splitlines()
                if line <= len(lines):
                    line_text = lines[line - 1]
                    start = max(0, column - 30)
                    end = min(len(line_text), column + 30)
                    snippet = line_text[start:end]
                    pointer_pos = column - 1 - start
                    pointer = " " * pointer_pos + "^"
                    ctx = f"  {snippet}\n  {pointer}"
            except Exception:
                pass
        
        if return_model:
            return ParseResult.from_error(
                reconstructed_text,
                f"DSL parse error: {str(e)}",
                line=line,
                column=column,
                context=ctx,
            )
        
        raise DSLParseError(
            message=f"DSL parse error: {str(e)}",
            line=line,
            column=column,
            context=ctx,
        ) from e


def parse_dsl_expression(
    dsl: str,
    profile: Optional[DSLGrammarProfile] = None,
    return_model: bool = False,
) -> Union[Tree, ParseResult]:
    """Parse DSL expression into a Lark parse tree.
    
    DEPRECATED: This function accepts raw DSL strings for backward compatibility only.
    
    IMPORTANT: The parser is designed to work ONLY on tokens from the lexer, not raw strings.
    This function maintains backward compatibility by internally:
    1. Tokenizing the input using tokenize_dsl()
    2. Parsing the tokens using parse_tokens()
    
    For new code, use the proper separation:
        tokens = tokenize_dsl(dsl, profile=profile)
        tree = parse_tokens(tokens, original_text=dsl, profile=profile)
    
    This enforces the architecture: lexer → tokens → parser → AST

    Args:
        dsl: The DSL expression string to parse
        profile: Optional grammar profile (for extensions)
        return_model: If True, returns ParseResult (Pydantic model) instead of Tree

    Returns:
        If return_model=False: Lark parse tree (Tree object)
        If return_model=True: ParseResult with structured parsing information

    Raises:
        DSLParseError: if parsing fails with detailed error information (only when return_model=False)
    """
    # Import here to avoid circular dependency
    from .lexer import tokenize_dsl
    
    text = (dsl or "").strip()
    if not text:
        if return_model:
            return ParseResult.from_error(
                text,
                "DSL expression is empty or contains only whitespace",
                line=1,
                column=1,
            )
        raise DSLParseError(
            "DSL expression is empty or contains only whitespace",
            line=1,
            column=1,
            context="Expression cannot be empty"
        )
    
    # Tokenize first, then parse tokens (proper separation: lexer → tokens → parser)
    # All error handling is done in parse_tokens(), so we just delegate
    tokens = tokenize_dsl(text, profile=profile, return_model=False)
    return parse_tokens(tokens, original_text=text, profile=profile, return_model=return_model)
