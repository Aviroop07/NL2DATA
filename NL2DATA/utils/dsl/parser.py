"""Parser for NL2DATA DSL using Lark."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple

from lark import Lark, UnexpectedInput

from .grammar_profile import DSLGrammarProfile, build_dsl_grammar


@dataclass(frozen=True)
class DSLParseError(Exception):
    message: str
    line: Optional[int] = None
    column: Optional[int] = None
    context: Optional[str] = None

    def __str__(self) -> str:
        loc = ""
        if self.line is not None and self.column is not None:
            loc = f" (line {self.line}, col {self.column})"
        if self.context:
            return f"{self.message}{loc}\n{self.context}"
        return f"{self.message}{loc}"


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


def parse_dsl_expression(dsl: str, profile: Optional[DSLGrammarProfile] = None):
    """Parse DSL expression into a Lark parse tree.

    Raises:
        DSLParseError: if parsing fails.
    """
    text = (dsl or "").strip()
    if not text:
        raise DSLParseError("DSL expression is empty")
    try:
        return _get_parser(profile=profile).parse(text)
    except UnexpectedInput as e:
        ctx = None
        try:
            ctx = e.get_context(text, span=80)
        except Exception:
            ctx = None
        raise DSLParseError(
            "DSL parse error: unexpected token/input",
            line=getattr(e, "line", None),
            column=getattr(e, "column", None),
            context=ctx,
        ) from e
    except Exception as e:
        raise DSLParseError(f"DSL parse error: {e}") from e

