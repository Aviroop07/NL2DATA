"""Lexer/tokenizer for the NL2DATA DSL.

Why:
- We sometimes need a token stream for lightweight deterministic checks
  before/alongside parsing.
- We align tokenization with the DSL grammar (Lark) so parsing is consistent.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Literal, Optional

from lark import Token

from .parser import _get_parser  # intentional: reuse the exact grammar+lexer


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
    "TRUE",
    "FALSE",
    "NULL",
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

_PUNCTUATION = {"LPAREN", "RPAREN", "LBRACK", "RBRACK", "COMMA", "TILDE"}


def _categorize_token_type(token_type: str) -> TokenCategory:
    if token_type in _KEYWORDS:
        return "keyword"
    if token_type in _OPERATORS:
        return "operator"
    if token_type in _LITERALS:
        return "literal"
    if token_type in _PUNCTUATION:
        return "punctuation"
    if token_type == "CNAME":
        return "identifier"
    return "unknown"


def tokenize_dsl(dsl: str) -> List[DSLToken]:
    """Tokenize a DSL string into a list of typed tokens.

    Notes:
    - Identifiers like `entity.column` are emitted as separate `CNAME` tokens
      around a `DOT`-like punctuation that is embedded inside the identifier rule.
      For deterministic checks, it's usually enough to recombine adjacent CNAME tokens
      when you later parse, but this tokenizer stays faithful to Lark's lexer output.
    """
    text = (dsl or "").strip()
    if not text:
        return []
    parser = _get_parser()
    toks: List[Token] = list(parser.lex(text))
    out: List[DSLToken] = []
    for t in toks:
        out.append(
            DSLToken(
                type=str(t.type),
                value=str(t.value),
                category=_categorize_token_type(str(t.type)),
                line=getattr(t, "line", None),
                column=getattr(t, "column", None),
            )
        )
    return out

