"""Lark grammar for NL2DATA DSL.

Design goals:
- Be able to express row-level derived attributes and decomposition rules deterministically.
- Accept SQL-like expressions (function calls, CASE WHEN) as a subset.
- Support distribution expressions via `~`, e.g. `fraud ~ Bernoulli(0.05)`.
"""

# NOTE: We intentionally keep this as a single grammar string for portability.
# If you need a human-facing spec later, we can generate it from this grammar.

DSL_GRAMMAR = r"""
// --------------------
// Entry point
// --------------------
?start: expr

// --------------------
// High-level expression forms
// --------------------
?expr: distribution_expr
     | if_expr
     | case_expr
     | or_expr

distribution_expr: or_expr TILDE dist_call
dist_call: identifier LPAREN [arg_list] RPAREN

if_expr: IF expr THEN expr ELSE expr

case_expr: CASE when_clause+ [ELSE expr] END
when_clause: WHEN expr THEN expr

// --------------------
// Boolean logic (SQL-ish precedence)
// --------------------
?or_expr: and_expr (OR and_expr)*
?and_expr: not_expr (AND not_expr)*
?not_expr: NOT not_expr  -> not_expr
         | comparison

// --------------------
// Comparisons
// --------------------
?comparison: sum_expr (cmp_tail)*
cmp_tail: (EQ|NE|LT|LE|GT|GE|LIKE) sum_expr
        | IN list
        //__EXT_CMP_TAIL__

// --------------------
// Arithmetic
// --------------------
?sum_expr: term ((PLUS|MINUS) term)*
?term: factor ((MUL|DIV|MOD) factor)*
?factor: (PLUS|MINUS) factor  -> unary
       | atom

// --------------------
// Atoms: literals, identifiers, calls, grouping, lists
// --------------------
?atom: literal
     | func_call
     | identifier
     | pair
     | LPAREN expr RPAREN
     | list

// Pair literal for categorical distributions and structured args.
// Example: ('active', 0.6) or (status_active, 0.6)
pair: LPAREN expr COMMA expr RPAREN

list: LBRACK [arg_list] RBRACK
arg_list: expr (COMMA expr)*

func_call: identifier LPAREN [arg_list] RPAREN

identifier: CNAME ("." CNAME)*

// --------------------
// Literals
// --------------------
literal: SIGNED_NUMBER           -> number
       | STRING                  -> string
       | TRUE                    -> true
       | FALSE                   -> false
       | NULL                    -> null

// --------------------
// Tokens / imports
// --------------------
%import common.CNAME
%import common.SIGNED_NUMBER
%import common.ESCAPED_STRING
%import common.WS_INLINE
%ignore WS_INLINE

// Support both "double-quoted" and 'single-quoted' strings (SQL-like).
SINGLE_QUOTED_STRING: /'([^'\\\\]|\\\\.)*'/
STRING: ESCAPED_STRING | SINGLE_QUOTED_STRING

// Punctuation (explicit tokens so lexer output is stable/readable)
TILDE: "~"
LPAREN: "("
RPAREN: ")"
LBRACK: "["
RBRACK: "]"
COMMA: ","

// Case-insensitive keywords
// Give keywords higher priority than CNAME so they don't lex as identifiers.
IF.2: /(?i:if)/
THEN.2: /(?i:then)/
ELSE.2: /(?i:else)/
CASE.2: /(?i:case)/
WHEN.2: /(?i:when)/
END.2: /(?i:end)/
AND.2: /(?i:and)/
OR.2: /(?i:or)/
NOT.2: /(?i:not)/
LIKE.2: /(?i:like)/
IN.2: /(?i:in)/
TRUE.2: /(?i:true)/
FALSE.2: /(?i:false)/
NULL.2: /(?i:null)/
//__EXT_KEYWORDS__

// Operators
EQ: "="
NE: "!=" | "<>"
LE: "<="
LT: "<"
GE: ">="
GT: ">"

PLUS: "+"
MINUS: "-"
MUL: "*"
DIV: "/"
MOD: "%"
"""

