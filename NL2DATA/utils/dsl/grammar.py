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
     | exists_expr
     | or_expr

distribution_expr: or_expr TILDE dist_call
dist_call: identifier LPAREN [arg_list] RPAREN

if_expr: IF expr THEN expr ELSE expr

case_expr: CASE when_clause+ [ELSE expr] END
when_clause: WHEN expr THEN expr

// SQL-like EXISTS (simplified: EXISTS (SELECT 1 FROM table WHERE condition))
exists_expr: EXISTS LPAREN SELECT [DISTINCT] (expr | STAR) FROM identifier [WHERE expr] RPAREN

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
        | BETWEEN sum_expr AND sum_expr
        | IS NULL
        | IS NOT NULL

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
     | aggregate_func_call
     | window_func_call
     | relational_exists
     | relational_lookup
     | relational_agg
     | in_range_call
     | identifier
     | pair
     | LPAREN expr RPAREN
     | scalar_subquery
     | list
     | STAR
     //__EXT_ATOM__

// Scalar subquery (returns single value) - used as an atom in expressions
scalar_subquery: LPAREN SELECT [DISTINCT] (expr | STAR) FROM identifier [WHERE expr] [GROUP BY expr (COMMA expr)*] [HAVING expr] [ORDER BY expr [ASC|DESC] (COMMA expr [ASC|DESC])*] [LIMIT expr] RPAREN

// Aggregate function with optional DISTINCT and OVER clause
aggregate_func_call: identifier LPAREN [DISTINCT] expr RPAREN [OVER window_spec]
// Note: aggregate_name is validated semantically (COUNT, SUM, AVG, MIN, MAX)

// Window function call
window_func_call: identifier LPAREN [arg_list] RPAREN OVER window_spec
// Note: window_func_name is validated semantically (ROW_NUMBER, RANK, etc.)

// Window function specification (SQL-like OVER clause)
window_spec: LPAREN [PARTITION BY expr (COMMA expr)*] [ORDER BY expr [ASC|DESC] (COMMA expr [ASC|DESC])*] [ROWS|RANGE frame_clause] RPAREN
frame_clause: BETWEEN frame_bound AND frame_bound
            | frame_bound
frame_bound: UNBOUNDED PRECEDING
           | UNBOUNDED FOLLOWING
           | CURRENT ROW
           | expr PRECEDING
           | expr FOLLOWING

// Relational constraint functions (extension: profile:v1+relational_constraints)
relational_exists: EXISTS LPAREN identifier WHERE expr RPAREN
relational_lookup: LOOKUP LPAREN identifier COMMA expr WHERE expr RPAREN
relational_agg: (COUNT_WHERE | SUM_WHERE | AVG_WHERE | MIN_WHERE | MAX_WHERE) LPAREN identifier [COMMA expr] WHERE expr RPAREN
in_range_call: IN_RANGE LPAREN expr COMMA expr COMMA expr RPAREN

// Relational function names
COUNT_WHERE.2: /(?i:\bcount_where\b)/
SUM_WHERE.2: /(?i:\bsum_where\b)/
AVG_WHERE.2: /(?i:\bavg_where\b)/
MIN_WHERE.2: /(?i:\bmin_where\b)/
MAX_WHERE.2: /(?i:\bmax_where\b)/
LOOKUP.2: /(?i:\blookup\b)/
IN_RANGE.2: /(?i:\bin_range\b)/
THIS.2: /(?i:\bthis\b)/

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
STAR: "*"

// Case-insensitive keywords
// Give keywords higher priority than CNAME so they don't lex as identifiers.
// Use word boundaries (\b) to prevent matching keywords as prefixes of identifiers
// e.g., "or" should not match in "orders", "if" should not match in "if_condition"
IF.2: /(?i:\bif\b)/
THEN.2: /(?i:\bthen\b)/
ELSE.2: /(?i:\belse\b)/
CASE.2: /(?i:\bcase\b)/
WHEN.2: /(?i:\bwhen\b)/
END.2: /(?i:\bend\b)/
AND.2: /(?i:\band\b)/
OR.2: /(?i:\bor\b)/
NOT.2: /(?i:\bnot\b)/
LIKE.2: /(?i:\blike\b)/
IN.2: /(?i:\bin\b)/
TRUE.2: /(?i:\btrue\b)/
FALSE.2: /(?i:\bfalse\b)/
NULL.2: /(?i:\bnull\b)/
// SQL-like keywords
SELECT.2: /(?i:\bselect\b)/
FROM.2: /(?i:\bfrom\b)/
WHERE.2: /(?i:\bwhere\b)/
GROUP.2: /(?i:\bgroup\b)/
BY.2: /(?i:\bby\b)/
HAVING.2: /(?i:\bhaving\b)/
ORDER.2: /(?i:\border\b)/
LIMIT.2: /(?i:\blimit\b)/
EXISTS.2: /(?i:\bexists\b)/
DISTINCT.2: /(?i:\bdistinct\b)/
AS.2: /(?i:\bas\b)/
OVER.2: /(?i:\bover\b)/
PARTITION.2: /(?i:\bpartition\b)/
ROWS.2: /(?i:\brows\b)/
RANGE.2: /(?i:\brange\b)/
BETWEEN.2: /(?i:\bbetween\b)/
PRECEDING.2: /(?i:\bpreceding\b)/
FOLLOWING.2: /(?i:\bfollowing\b)/
UNBOUNDED.2: /(?i:\bunbounded\b)/
CURRENT.2: /(?i:\bcurrent\b)/
ROW.2: /(?i:\brow\b)/
ASC.2: /(?i:\basc\b)/
DESC.2: /(?i:\bdesc\b)/
// Window function names
ROW_NUMBER.2: /(?i:\brow_number\b)/
RANK.2: /(?i:\brank\b)/
DENSE_RANK.2: /(?i:\bdense_rank\b)/
PERCENT_RANK.2: /(?i:\bpercent_rank\b)/
CUME_DIST.2: /(?i:\bcume_dist\b)/
LAG.2: /(?i:\blag\b)/
LEAD.2: /(?i:\blead\b)/
FIRST_VALUE.2: /(?i:\bfirst_value\b)/
LAST_VALUE.2: /(?i:\blast_value\b)/
NTH_VALUE.2: /(?i:\bnth_value\b)/
IS.2: /(?i:\bis\b)/

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

