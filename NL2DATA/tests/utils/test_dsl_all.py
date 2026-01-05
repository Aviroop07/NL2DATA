"""Comprehensive DSL test suite combining all DSL validation tests.

This file combines all DSL-related tests from:
- test_dsl_validation.py
- test_dsl_validation_minimal.py
- test_dsl_comprehensive_validation.py
- test_dsl_progressive_validation.py
- test_dsl_component_separation.py
- test_dsl_lexer_extraction.py
- test_column_bound_dsl_validation.py
- test_decomposition_dsl_semantics.py

Organized by:
1. Basic validation tests (syntax, semantics)
2. Component separation tests (lexer, parser, validator)
3. Comprehensive validation tests (all three phases)
4. Progressive validation tests (fixed expression set)
5. Lexer extraction tests (table/column name extraction)
6. Column-bound DSL validation tests
7. Decomposition DSL semantics tests
"""

import pytest
import sys

from NL2DATA.utils.dsl.lexer import (
    tokenize_dsl,
    extract_table_and_column_names,
    get_table_names_from_expression,
    get_column_names_from_expression,
)
from NL2DATA.utils.dsl.parser import parse_dsl_expression, DSLParseError
from NL2DATA.utils.dsl.validator import (
    validate_dsl_expression_strict,
    validate_dsl_expression,
    validate_dsl_expression_with_schema,
    validate_column_bound_dsl,
)
from NL2DATA.utils.dsl.schema_context import (
    DSLSchemaContext,
    DSLTableSchema,
    build_schema_context_from_relational_schema,
)
from NL2DATA.utils.dsl.models import ColumnBoundDSL, DSLKind
from NL2DATA.utils.dsl.grammar_profile import DSLGrammarProfile, FEATURE_RELATIONAL_CONSTRAINTS
from NL2DATA.phases.phase2.step_2_4_composite_attribute_handling import _validate_decomposition_dsls


# ============================================================================
# Section 1: Basic DSL Validation Tests
# ============================================================================

@pytest.mark.parametrize(
    "expr",
    [
        # Identifiers / column references
        "created_at",
        "LeadsOrContactsFactTable.created_timestamp",
        "schema1.table2.column3",
        "Order.total_amount + 10",
        "IF LeadsOrContactsFactTable.lead_status = 'converted' THEN 1 ELSE 0",
        "LeadsOrContactsFactTable.score ~ NORMAL(0, 1)",
        "if amount > 0 then amount else 0",
        "CASE WHEN amount > 0 THEN amount ELSE 0 END",
        "NOT (a = 1) AND b != 2",
        "UNIFORM(0, 1)",
        "x ~ NORMAL(0, 1)",
        "x ~ LOGNORMAL(3.5, 1.2)",
        "x ~ PARETO(1.5, 10)",
        "flag ~ BERNOULLI(0.05)",
        "x ~ CATEGORICAL(('a', 0.6), ('b', 0.4))",
        "DATEADD('day', 1, created_at)",
        "CONCAT(first_name, ' ', last_name)",
        "a.b.c + 3 * (x - 1)",
        "[1, 2, 3]",
        "INVENTORY.status IN ['active', 'paused']",
        # Nested expressions
        "if a > 0 then (if b > 0 then a + b else a - b) else 0",
        "CASE WHEN a > 0 THEN CASE WHEN b > 0 THEN 1 ELSE 2 END ELSE 0 END",
        "if is_premium then price * (1 - discount_rate) else price",
        "CONCAT(UPPER(first_name), ' ', LOWER(last_name))",
        "if score > 0.8 then (x ~ NORMAL(0, 1)) else (x ~ UNIFORM(0, 1))",
        "x ~ CATEGORICAL(('low', 0.1 + 0.2), ('high', 1 - (0.1 + 0.2)))",
    ],
)
def test_valid_dsl_expressions(expr: str) -> None:
    """Test valid DSL expressions."""
    out = validate_dsl_expression_strict(expr)
    assert out["valid"] is True, out


@pytest.mark.parametrize(
    "expr",
    [
        "",
        "if then else",
        "CASE WHEN x THEN y",  # missing END
        "a +",  # trailing operator
        "x ~",  # incomplete dist
        "x ~ Seasonal(month_day=[1,15])",  # not offered yet
        "a IN [1, 2, ]",  # trailing comma
        "(",  # unclosed
    ],
)
def test_invalid_dsl_expressions(expr: str) -> None:
    """Test invalid DSL expressions."""
    out = validate_dsl_expression_strict(expr)
    assert out["valid"] is False
    assert isinstance(out.get("error"), str) and out["error"]


def test_unknown_function_is_rejected() -> None:
    """Test that unknown functions are rejected."""
    out = validate_dsl_expression_strict("FOO(1)")
    assert out["valid"] is False
    assert isinstance(out.get("error"), str) and out["error"]


def test_function_arity_is_enforced() -> None:
    """Test that function arity is enforced."""
    out = validate_dsl_expression_strict("LOWER(a, b)")
    assert out["valid"] is False
    assert isinstance(out.get("error"), str) and out["error"]
    out2 = validate_dsl_expression_strict("DATEADD('day', 1)")
    assert out2["valid"] is False
    assert isinstance(out2.get("error"), str) and out2["error"]


def test_dotted_function_name_is_rejected() -> None:
    """Test that dotted function names are rejected."""
    out = validate_dsl_expression_strict("schema.LOWER(name)")
    assert out["valid"] is False
    assert isinstance(out.get("error"), str) and out["error"]


def test_dotted_distribution_name_is_rejected() -> None:
    """Test that dotted distribution names are rejected."""
    out = validate_dsl_expression_strict("x ~ schema.NORMAL(0, 1)")
    assert out["valid"] is False
    assert isinstance(out.get("error"), str) and out["error"]


def test_optional_grammar_features_disabled_by_default() -> None:
    """Test that BETWEEN and IS NULL/IS NOT NULL are now part of base grammar."""
    # BETWEEN is now part of base grammar, so this should be valid
    out = validate_dsl_expression("a BETWEEN 1 AND 2")
    assert out["valid"] is True
    # IS NULL is now part of base grammar, so this should be valid
    out2 = validate_dsl_expression("a IS NULL")
    assert out2["valid"] is True


def test_between_and_is_null_operators() -> None:
    """Test that BETWEEN and IS NULL/IS NOT NULL are part of the base grammar."""
    out = validate_dsl_expression("a BETWEEN 1 AND 2")
    assert out["valid"] is True, out
    out2 = validate_dsl_expression("a IS NULL")
    assert out2["valid"] is True, out2
    out3 = validate_dsl_expression("a IS NOT NULL")
    assert out3["valid"] is True, out3


@pytest.mark.parametrize(
    "expr",
    [
        "x ~ NORMAL(0)",              # too few
        "x ~ NORMAL(0, 1, 2)",        # too many
        "x ~ EXPONENTIAL(0.5, 1)",    # too many
        "x ~ TRIANGULAR(0, 1)",       # too few
        "x ~ ZIPF(1.5)",              # too few
        "x ~ CATEGORICAL('a', 0.5)",  # not supported in DSL yet
    ],
)
def test_distribution_signature_validation(expr: str) -> None:
    """Test distribution signature validation."""
    out = validate_dsl_expression_strict(expr)
    assert out["valid"] is False
    assert isinstance(out.get("error"), str) and out["error"]


@pytest.mark.parametrize(
    "expr",
    [
        "x ~ CATEGORICAL(('a', 1.0))",           # too few pairs
        "x ~ CATEGORICAL('a', 0.5, 'b', 0.5)",   # not pairs
        "x ~ CATEGORICAL(('a', 0.5), 1)",        # mixed arg types
    ],
)
def test_categorical_pair_validation(expr: str) -> None:
    """Test CATEGORICAL pair validation."""
    out = validate_dsl_expression_strict(expr)
    assert out["valid"] is False
    assert isinstance(out.get("error"), str) and out["error"]


@pytest.fixture
def schema() -> DSLSchemaContext:
    """Test schema with ambiguous columns."""
    return DSLSchemaContext(
        tables={
            "Student": DSLTableSchema(
                columns={
                    "id": "number",
                    "name": "string",
                    "age": "number",
                    "is_active": "boolean",
                    "created_at": "datetime",
                }
            ),
            "Course": DSLTableSchema(
                columns={
                    "id": "number",
                    "name": "string",
                    "credits": "number",
                }
            ),
        }
    )


def test_semantic_identifier_unknown_column(schema: DSLSchemaContext) -> None:
    """Test unknown column error."""
    result = validate_dsl_expression_with_schema("unknown_col + 1", schema)
    assert result["valid"] is False
    assert result.get("error")


def test_semantic_identifier_ambiguous_bare_column(schema: DSLSchemaContext) -> None:
    """Test ambiguous column requires qualification."""
    result = validate_dsl_expression_with_schema("id + 1", schema)
    assert result["valid"] is False
    result = validate_dsl_expression_with_schema("Student.id + 1", schema)
    assert result["valid"] is True


def test_semantic_identifier_disallow_deeper_paths(schema: DSLSchemaContext) -> None:
    """Test deeper paths are disallowed."""
    result = validate_dsl_expression_with_schema("schema.Student.id + 1", schema)
    assert result["valid"] is False


def test_semantic_type_arithmetic_mismatch(schema: DSLSchemaContext) -> None:
    """Test type mismatch in arithmetic."""
    result = validate_dsl_expression_with_schema("Student.name + 1", schema)
    assert result["valid"] is False
    assert result.get("error")


def test_semantic_type_boolean_mismatch(schema: DSLSchemaContext) -> None:
    """Test boolean operator type requirements."""
    result = validate_dsl_expression_with_schema("Student.age AND Student.is_active", schema)
    assert result["valid"] is False
    result = validate_dsl_expression_with_schema("Student.is_active AND (Student.age > 18)", schema)
    assert result["valid"] is True


def test_semantic_like_requires_string(schema: DSLSchemaContext) -> None:
    """Test LIKE operator requires string."""
    result = validate_dsl_expression_with_schema("Student.age LIKE '1%'", schema)
    assert result["valid"] is False
    result = validate_dsl_expression_with_schema("Student.name LIKE 'A%'", schema)
    assert result["valid"] is True


def test_semantic_in_list_type_mismatch(schema: DSLSchemaContext) -> None:
    """Test IN operator type matching."""
    result = validate_dsl_expression_with_schema("Student.age IN ['10','20']", schema)
    assert result["valid"] is False
    result = validate_dsl_expression_with_schema("Student.name IN ['Alice','Bob']", schema)
    assert result["valid"] is True


def test_semantic_distribution_target_must_be_identifier(schema: DSLSchemaContext) -> None:
    """Test distribution target must be identifier."""
    result = validate_dsl_expression_with_schema("(Student.age + 1) ~ NORMAL(0, 1)", schema)
    assert result["valid"] is False


def test_semantic_distribution_target_type_check(schema: DSLSchemaContext) -> None:
    """Test distribution target type validation."""
    result = validate_dsl_expression_with_schema("Student.name ~ NORMAL(0, 1)", schema)
    assert result["valid"] is False
    result = validate_dsl_expression_with_schema("Student.age ~ NORMAL(0, 1)", schema)
    assert result["valid"] is True


def test_schema_context_builder_from_relational_schema_smoke() -> None:
    """Test schema builder from relational schema."""
    ctx = build_schema_context_from_relational_schema(
        {
            "tables": [
                {"name": "T", "columns": [{"name": "x", "type": "INT"}, {"name": "y", "type": "VARCHAR(10)"}]}
            ]
        }
    )
    result = validate_dsl_expression_with_schema("T.x + 1", ctx)
    assert result["valid"] is True
    result = validate_dsl_expression_with_schema("T.y + 1", ctx)
    assert result["valid"] is False


def test_semantic_distribution_bounds_checks_literals(schema: DSLSchemaContext) -> None:
    """Test distribution parameter bounds validation."""
    invalid_expressions = [
        "Student.age ~ NORMAL(0, -1)",
        "Student.age ~ UNIFORM(10, 5)",
        "Student.age ~ EXPONENTIAL(0)",
        "Student.age ~ POISSON(-2)",
        "Student.age ~ ZIPF(1.2, 0)",
        "Student.age ~ ZIPF(1.2, 2.5)",
        "Student.age ~ TRIANGULAR(0, 10, 11)",
        "Student.is_active ~ BERNOULLI(1.2)",
    ]
    for expr in invalid_expressions:
        result = validate_dsl_expression_with_schema(expr, schema)
        assert result["valid"] is False, f"Should be invalid: {expr}"


def test_semantic_categorical_weight_checks_literals(schema: DSLSchemaContext) -> None:
    """Test CATEGORICAL distribution weight validation."""
    result = validate_dsl_expression_with_schema("Student.name ~ CATEGORICAL(('a', -0.1), ('b', 0.2))", schema)
    assert result["valid"] is False
    result = validate_dsl_expression_with_schema("Student.name ~ CATEGORICAL(('a', 0.0), ('b', 0.0))", schema)
    assert result["valid"] is False
    result = validate_dsl_expression_with_schema("Student.name ~ CATEGORICAL(('a', 0.2), ('b', 0.8))", schema)
    assert result["valid"] is True


def test_semantic_dateadd_datediff_signatures(schema: DSLSchemaContext) -> None:
    """Test DATEADD/DATEDIFF function signatures."""
    result = validate_dsl_expression_with_schema("DATEADD('day', 1, Student.created_at)", schema)
    assert result["valid"] is True
    result = validate_dsl_expression_with_schema("DATEADD('day', '1', Student.created_at)", schema)
    assert result["valid"] is False
    result = validate_dsl_expression_with_schema("DATEADD(1, 1, Student.created_at)", schema)
    assert result["valid"] is False
    result = validate_dsl_expression_with_schema("DATEDIFF('day', Student.created_at, Student.created_at)", schema)
    assert result["valid"] is True


# ============================================================================
# Section 2: Component Separation Tests
# ============================================================================

def create_test_schema_component() -> DSLSchemaContext:
    """Create a simple test schema for component tests."""
    return DSLSchemaContext(
        tables={
            "Customer": DSLTableSchema(
                columns={
                    "customer_id": "number",
                    "name": "string",
                    "age": "number",
                }
            ),
        }
    )


class TestComponentSeparation:
    """Test that components are properly separated."""
    
    def test_lexer_independent(self):
        """Test that lexer can be used independently without parser."""
        tokens = tokenize_dsl("Customer.customer_id + 10")
        assert len(tokens) > 0
        token_types = [t.type for t in tokens]
        assert "IDENTIFIER" in token_types or "CNAME" in token_types
        assert "PLUS" in token_types
        assert "SIGNED_NUMBER" in token_types
    
    def test_parser_independent(self):
        """Test that parser can be used independently."""
        tree = parse_dsl_expression("Customer.customer_id + 10")
        assert tree is not None
        with pytest.raises(DSLParseError):
            parse_dsl_expression("Customer.customer_id +")
    
    def test_semantic_analyzer_independent(self):
        """Test that semantic analyzer can be used independently."""
        schema = create_test_schema_component()
        result = validate_dsl_expression_with_schema("Customer.customer_id + 10", schema)
        assert result["valid"] is True
        result = validate_dsl_expression_with_schema("Customer.name + 10", schema)
        assert result["valid"] is False
    
    def test_lexer_does_not_parse(self):
        """Test that lexer only tokenizes and does not parse."""
        tokens = tokenize_dsl("Customer.customer_id +")
        assert len(tokens) > 0
        tokens = tokenize_dsl("Customer.name + 10")
        assert len(tokens) > 0
    
    def test_parser_does_not_validate_semantics(self):
        """Test that parser only parses and does not validate semantics."""
        tree = parse_dsl_expression("Customer.name + 10")
        assert tree is not None
        tree = parse_dsl_expression("UnknownTable.column + 10")
        assert tree is not None
    
    def test_semantic_analyzer_requires_parser(self):
        """Test that semantic analyzer uses parser but is separate."""
        schema = create_test_schema_component()
        result = validate_dsl_expression_with_schema("Customer.customer_id +", schema)
        assert result["valid"] is False
        result = validate_dsl_expression_with_schema("Customer.name + 10", schema)
        assert result["valid"] is False
    
    def test_progressive_validation_separation(self):
        """Test that each component can be used progressively."""
        schema = create_test_schema_component()
        expr = "Customer.customer_id + 10"
        tokens = tokenize_dsl(expr)
        assert len(tokens) > 0
        tree = parse_dsl_expression(expr)
        assert tree is not None
        result = validate_dsl_expression_with_schema(expr, schema)
        assert result["valid"] is True
    
    def test_no_circular_dependencies(self):
        """Test that there are no circular import dependencies."""
        from NL2DATA.utils.dsl import lexer
        from NL2DATA.utils.dsl import parser
        from NL2DATA.utils.dsl import validator
        assert lexer is not None
        assert parser is not None
        assert validator is not None
    
    def test_component_responsibilities(self):
        """Test that each component has clear responsibilities."""
        expr_valid = "Customer.customer_id + 10"
        expr_invalid_syntax = "Customer.customer_id +"
        expr_invalid_semantic = "Customer.name + 10"
        schema = create_test_schema_component()
        assert len(tokenize_dsl(expr_valid)) > 0
        assert len(tokenize_dsl(expr_invalid_syntax)) > 0
        assert len(tokenize_dsl(expr_invalid_semantic)) > 0
        assert parse_dsl_expression(expr_valid) is not None
        with pytest.raises(DSLParseError):
            parse_dsl_expression(expr_invalid_syntax)
        assert parse_dsl_expression(expr_invalid_semantic) is not None
        result = validate_dsl_expression_with_schema(expr_valid, schema)
        assert result["valid"] is True
        result = validate_dsl_expression_with_schema(expr_invalid_syntax, schema)
        assert result["valid"] is False
        result = validate_dsl_expression_with_schema(expr_invalid_semantic, schema)
        assert result["valid"] is False


# ============================================================================
# Section 3: Comprehensive Validation Tests (All Three Phases)
# ============================================================================

def create_comprehensive_test_schema() -> DSLSchemaContext:
    """Create a comprehensive test schema with 3 tables."""
    return DSLSchemaContext(
        tables={
            "Customer": DSLTableSchema(
                columns={
                    "customer_id": "number",
                    "name": "string",
                    "email": "string",
                    "age": "number",
                    "is_premium": "boolean",
                    "created_at": "datetime",
                }
            ),
            "Order": DSLTableSchema(
                columns={
                    "order_id": "number",
                    "customer_id": "number",
                    "total_amount": "number",
                    "status": "string",
                    "order_date": "datetime",
                }
            ),
            "Product": DSLTableSchema(
                columns={
                    "product_id": "number",
                    "name": "string",
                    "price": "number",
                    "category": "string",
                    "in_stock": "boolean",
                }
            ),
        }
    )


class TestTokenization:
    """Test tokenization phase - valid and invalid token sequences."""

    def test_valid_tokenization_basic(self):
        """Test valid tokenization of basic expressions."""
        tokens = tokenize_dsl("customer_id + 10")
        assert len(tokens) > 0
        assert any(t.value == "customer_id" and t.category == "identifier" for t in tokens)
        assert any(t.value == "+" and t.category == "operator" for t in tokens)
        assert any(t.value == "10" and t.category == "literal" for t in tokens)

    def test_valid_tokenization_keywords(self):
        """Test tokenization of keywords."""
        tokens = tokenize_dsl("IF age > 18 THEN 1 ELSE 0")
        keyword_values = [t.value for t in tokens if t.category == "keyword"]
        assert "IF" in keyword_values or "if" in keyword_values
        assert "THEN" in keyword_values or "then" in keyword_values
        assert "ELSE" in keyword_values or "else" in keyword_values

    def test_valid_tokenization_qualified_identifier(self):
        """Test tokenization of qualified identifiers."""
        tokens = tokenize_dsl("Customer.customer_id")
        identifiers = [t.value for t in tokens if t.category == "identifier"]
        assert "Customer" in identifiers
        assert "customer_id" in identifiers
        assert any(t.value == "." and t.category == "punctuation" for t in tokens)

    def test_valid_tokenization_string_literals(self):
        """Test tokenization of string literals."""
        tokens = tokenize_dsl("name = 'John'")
        string_tokens = [t for t in tokens if t.category == "literal" and t.type in ("STRING", "ESCAPED_STRING", "SINGLE_QUOTED_STRING")]
        assert len(string_tokens) > 0

    def test_valid_tokenization_distribution(self):
        """Test tokenization of distribution expressions."""
        tokens = tokenize_dsl("age ~ UNIFORM(18, 65)")
        assert any(t.value == "~" and t.category == "punctuation" for t in tokens)
        assert any(t.value == "UNIFORM" or t.value == "uniform" for t in tokens)

    def test_valid_tokenization_comparison_operators(self):
        """Test tokenization of comparison operators."""
        tokens = tokenize_dsl("age BETWEEN 18 AND 65")
        assert any(t.value == "BETWEEN" or t.value == "between" for t in tokens)
        assert any(t.value == "AND" or t.value == "and" for t in tokens)

    def test_valid_tokenization_is_null(self):
        """Test tokenization of IS NULL operators."""
        tokens = tokenize_dsl("email IS NULL")
        assert any(t.value == "IS" or t.value == "is" for t in tokens)
        assert any(t.value == "NULL" or t.value == "null" for t in tokens)

    def test_empty_string_tokenization(self):
        """Test that empty string returns empty token list."""
        tokens = tokenize_dsl("")
        assert tokens == []

    def test_whitespace_only_tokenization(self):
        """Test that whitespace-only string returns empty token list."""
        tokens = tokenize_dsl("   \n\t  ")
        assert tokens == []


class TestSyntax:
    """Test syntax phase - valid and invalid grammar structures."""

    @pytest.mark.parametrize(
        "expr",
        [
            "customer_id + 10",
            "total_amount * 1.1",
            "(age + 5) * 2",
            "age >= 18",
            "status = 'active'",
            "age BETWEEN 18 AND 65",
            "email IS NULL",
            "status IS NOT NULL",
            "age >= 18 AND is_premium = true",
            "status = 'active' OR status = 'pending'",
            "NOT is_premium",
            "IF age >= 18 THEN 1 ELSE 0",
            "CASE WHEN age < 18 THEN 'minor' WHEN age < 65 THEN 'adult' ELSE 'senior' END",
            "CONCAT(name, ' ', email)",
            "UPPER(status)",
            "ROUND(total_amount, 2)",
            "LENGTH(name)",
            "age ~ UNIFORM(18, 65)",
            "is_premium ~ BERNOULLI(0.2)",
            "status ~ CATEGORICAL(('active', 0.6), ('pending', 0.3), ('cancelled', 0.1))",
            "Customer.customer_id",
            "Order.total_amount",
            "Product.price",
            "IF age >= 18 THEN (total_amount * 1.1) ELSE total_amount",
            "CASE WHEN status = 'active' THEN price * 0.9 WHEN status = 'pending' THEN price * 0.95 ELSE price END",
            "status IN ['active', 'pending', 'cancelled']",
            "category IN ['electronics', 'books']",
            "name LIKE 'John%'",
            "email LIKE '%@gmail.com'",
        ],
    )
    def test_valid_syntax(self, expr: str):
        """Test that valid expressions parse successfully."""
        try:
            tree = parse_dsl_expression(expr)
            assert tree is not None
        except DSLParseError as e:
            pytest.fail(f"Valid expression failed to parse: {expr}\nError: {e}")

    @pytest.mark.parametrize(
        "expr",
        [
            "",
            "(",
            ")",
            "customer_id +",
            "+ customer_id",
            "IF age > 18",
            "IF age > 18 THEN 1",
            "IF THEN 1 ELSE 0",
            "CASE WHEN age > 18 THEN 'adult'",
            "CASE WHEN age > 18 THEN 'adult' END",
            "CASE age > 18 THEN 'adult' END",
            "CONCAT(",
            "UPPER()",
            "ROUND(,)",
            "age ~",
            "~ UNIFORM(18, 65)",
            "age ~ UNIFORM(",
            "age BETWEEN",
            "age BETWEEN 18",
            "age BETWEEN 18 AND",
            "email IS",
            "IS NULL",
            "Customer.",
            ".customer_id",
            "schema.Customer.customer_id",
            "customer_id ** 2",
            "customer_id // 2",
            "status IN [",
            "status IN ]",
            "status IN [,]",
        ],
    )
    def test_invalid_syntax(self, expr: str):
        """Test that invalid expressions fail to parse."""
        with pytest.raises(DSLParseError):
            parse_dsl_expression(expr)


class TestSemanticValidation:
    """Test semantic phase - type checking and semantic rules."""

    @pytest.fixture
    def schema(self):
        return create_comprehensive_test_schema()

    @pytest.mark.parametrize(
        "expr",
        [
            "Customer.customer_id + 10",
            "Order.total_amount * 1.1",
            "Customer.age >= 18",
            "Customer.name = 'John'",
            "Customer.is_premium = true",
            "Customer.name LIKE 'John%'",
            "Order.status IN ['active', 'pending']",
            "IF Customer.age >= 18 THEN 1 ELSE 0",
            "CONCAT(Customer.name, ' ', Customer.email)",
            "UPPER(Order.status)",
            "ROUND(Order.total_amount, 2)",
            "Customer.age BETWEEN 18 AND 65",
            "Product.price BETWEEN 10.0 AND 100.0",
            "Customer.email IS NULL",
            "Order.status IS NOT NULL",
            "Customer.age ~ UNIFORM(18, 65)",
            "Customer.is_premium ~ BERNOULLI(0.2)",
            "Order.status ~ CATEGORICAL(('active', 0.6), ('pending', 0.4))",
            "Product.price ~ NORMAL(100.0, 20.0)",
            "Customer.is_premium AND Customer.age > 18",
            "Order.status = 'active' OR Order.status = 'pending'",
            "NOT Customer.is_premium",
        ],
    )
    def test_valid_semantic_expressions(self, schema: DSLSchemaContext, expr: str):
        """Test that semantically valid expressions pass validation."""
        result = validate_dsl_expression_with_schema(expr, schema)
        assert result["valid"] is True, f"Expression should be valid: {expr}\nError: {result.get('error')}"

    @pytest.mark.parametrize(
        "expr,expected_error_keywords",
        [
            ("unknown_column + 1", ["unknown", "column"]),
            ("NonExistentTable.column", ["unknown", "table"]),
            ("Customer.name + 10", ["string", "number", "arithmetic"]),
            ("Customer.email * 2", ["string", "number", "arithmetic"]),
            ("Customer.age + Customer.name", ["string", "number", "arithmetic"]),
            ("Customer.name >= 18", ["string", "number", "comparison"]),
            ("Customer.age LIKE 'John%'", ["number", "string", "LIKE"]),
            ("Customer.is_premium > 10", ["boolean", "number", "comparison"]),
            ("Customer.is_premium BETWEEN true AND false", ["boolean", "BETWEEN"]),
            ("Customer.age BETWEEN 65 AND 18", ["min", "max", "BETWEEN"]),
            ("Customer.age AND Customer.is_premium", ["number", "boolean", "AND"]),
            ("Customer.name OR Customer.email", ["string", "boolean", "OR"]),
            ("NOT Customer.age", ["number", "boolean", "NOT"]),
            ("Customer.age IN ['18', '65']", ["number", "string", "IN"]),
            ("Customer.name IN [18, 65]", ["string", "number", "IN"]),
            ("Customer.name ~ UNIFORM(18, 65)", ["string", "numeric", "distribution"]),
            ("Customer.email ~ NORMAL(0, 1)", ["string", "numeric", "distribution"]),
            ("Customer.age ~ UNIFORM(65, 18)", ["min", "max", "UNIFORM"]),
            ("Customer.age ~ NORMAL(0, -1)", ["std_dev", "NORMAL"]),
            ("Customer.age ~ EXPONENTIAL(0)", ["lambda", "EXPONENTIAL"]),
            ("Customer.is_premium ~ BERNOULLI(1.5)", ["probability", "BERNOULLI"]),
            ("UPPER(Customer.age)", ["string", "number", "UPPER"]),
            ("LOWER(Order.total_amount)", ["string", "number", "LOWER"]),
            ("LENGTH(Customer.age)", ["string", "number", "LENGTH"]),
            ("ROUND(Customer.name, 2)", ["number", "string", "ROUND"]),
            ("CONCAT(Customer.age, Customer.name)", ["string", "number", "CONCAT"]),
            ("customer_id + 1", ["ambiguous", "qualified"]),
        ],
    )
    def test_invalid_semantic_expressions(
        self, schema: DSLSchemaContext, expr: str, expected_error_keywords: list | None
    ):
        """Test that semantically invalid expressions fail validation."""
        result = validate_dsl_expression_with_schema(expr, schema)
        assert result["valid"] is False, f"Expression should be invalid: {expr}"
        assert "error" in result and result["error"], f"Error message should be present for: {expr}"
        
        if expected_error_keywords:
            error_msg = result["error"].lower()
            found_keyword = any(keyword.lower() in error_msg for keyword in expected_error_keywords)
            assert found_keyword, (
                f"Error message should contain one of {expected_error_keywords}\n"
                f"Actual error: {result['error']}"
            )


# ============================================================================
# Section 4: Lexer Extraction Tests
# ============================================================================

def test_complex_lexer_extraction():
    """Test table and column name extraction from complex DSL expressions."""
    schema = DSLSchemaContext(
        tables={
            "orders": DSLTableSchema(
                columns={
                    "order_id": "number",
                    "customer_id": "number",
                    "total_amount": "number",
                    "tax": "number",
                    "discount": "number",
                    "status": "string",
                    "order_date": "datetime",
                    "quantity": "number",
                    "unit_price": "number",
                }
            ),
            "customers": DSLTableSchema(
                columns={
                    "customer_id": "number",
                    "name": "string",
                    "email": "string",
                    "is_premium": "boolean",
                    "registration_date": "datetime",
                }
            ),
            "products": DSLTableSchema(
                columns={
                    "product_id": "number",
                    "name": "string",
                    "price": "number",
                    "category": "string",
                }
            ),
        }
    )

    complex_expressions = [
        ("orders.total_amount + orders.tax", {"orders"}, {"tax", "total_amount"}),
        ("total_amount + tax - discount", set(), {"discount", "tax", "total_amount"}),
        ("ROUND(orders.total_amount * 1.1, 2) + CONCAT(customers.name, ' - Order')", {"customers", "orders"}, {"name", "total_amount"}),
        ("IF orders.status = 'active' THEN orders.total_amount * 0.9 ELSE orders.total_amount", {"orders"}, {"status", "total_amount"}),
        ("CASE WHEN orders.total_amount > 1000 THEN 'high' WHEN orders.total_amount > 500 THEN 'medium' ELSE 'low' END", {"orders"}, {"total_amount"}),
        ("orders.total_amount + orders.tax - orders.discount AND customers.is_premium = TRUE", {"customers", "orders"}, {"discount", "is_premium", "tax", "total_amount"}),
        ("IF DATEDIFF('day', orders.order_date, CURRENT_DATE) > 30 THEN 'old' ELSE 'recent'", {"orders"}, {"order_date"}),
        ("(orders.quantity * orders.unit_price) * (1 - orders.discount) + orders.tax", {"orders"}, {"discount", "quantity", "tax", "unit_price"}),
        ("UPPER(customers.name) + ' ordered ' + CAST(orders.quantity, 'VARCHAR') + ' items'", {"customers", "orders"}, {"name", "quantity"}),
        ("orders.customer_id = customers.customer_id AND orders.product_id = products.product_id", {"customers", "orders", "products"}, {"customer_id", "product_id"}),
        ("orders.total_amount + customers.is_premium + products.price", {"customers", "orders", "products"}, {"is_premium", "price", "total_amount"}),
        ("IF orders.status = 'active' THEN customers.name ELSE products.name END", {"customers", "orders", "products"}, {"name", "status"}),
        ("orders.quantity * orders.unit_price - orders.discount + orders.tax", {"orders"}, {"discount", "quantity", "tax", "unit_price"}),
    ]

    for i, (expr, expected_tables, expected_columns) in enumerate(complex_expressions, 1):
        tables = get_table_names_from_expression(expr, schema_context=schema)
        columns = get_column_names_from_expression(expr, schema_context=schema)
        assert tables == expected_tables, f"Example {i}: Expected tables {expected_tables}, got {tables} in expression: {expr}"
        assert columns == expected_columns, f"Example {i}: Expected columns {expected_columns}, got {columns} in expression: {expr}"


# ============================================================================
# Section 5: Column-Bound DSL Validation Tests
# ============================================================================

def build_column_bound_test_schema() -> DSLSchemaContext:
    """Build the test schema for column-bound DSL tests."""
    tables = {
        "organization": DSLTableSchema(columns={
            "org_id": "number",
            "country": "string",
            "billing_tier": "string",
            "max_discount_pct": "number",
            "max_refund_pct": "number",
            "requires_2fa_for_high_risk": "boolean",
        }),
        "customer": DSLTableSchema(columns={
            "customer_id": "number",
            "org_id": "number",
            "status": "string",
            "risk_score": "number",
            "country": "string",
        }),
        "account": DSLTableSchema(columns={
            "account_id": "number",
            "customer_id": "number",
            "is_active": "boolean",
            "has_2fa": "boolean",
        }),
        "orders": DSLTableSchema(columns={
            "order_id": "number",
            "account_id": "number",
            "order_total": "number",
            "discount_pct": "number",
            "currency": "string",
            "created_at": "datetime",
        }),
        "order_item": DSLTableSchema(columns={
            "order_item_id": "number",
            "order_id": "number",
            "sku_id": "number",
            "quantity": "number",
            "unit_price": "number",
        }),
        "sku": DSLTableSchema(columns={
            "sku_id": "number",
            "category": "string",
            "is_age_restricted": "boolean",
        }),
        "shipment": DSLTableSchema(columns={
            "shipment_id": "number",
            "order_id": "number",
            "status": "string",
            "shipping_cost": "number",
        }),
        "tax_policy": DSLTableSchema(columns={
            "country": "string",
            "vat_rate": "number",
        }),
        "payment": DSLTableSchema(columns={
            "payment_id": "number",
            "order_id": "number",
            "amount": "number",
            "status": "string",
            "method": "string",
            "captured_at": "datetime",
        }),
        "refund": DSLTableSchema(columns={
            "refund_id": "number",
            "payment_id": "number",
            "amount": "number",
            "reason": "string",
            "created_at": "datetime",
        }),
        "chargeback": DSLTableSchema(columns={
            "chargeback_id": "number",
            "payment_id": "number",
            "amount": "number",
            "status": "string",
        }),
    }
    return DSLSchemaContext(tables=tables)


@pytest.fixture
def column_bound_schema() -> DSLSchemaContext:
    """Provide the test schema for column-bound DSL tests."""
    return build_column_bound_test_schema()


@pytest.fixture
def column_bound_profile() -> str:
    """Provide the relational constraints profile."""
    return f"profile:v1+{FEATURE_RELATIONAL_CONSTRAINTS}"


def test_v1_anchor_first_bare_identifier(column_bound_schema: DSLSchemaContext, column_bound_profile: str):
    """V1: Anchor-first bare identifier resolution (no THIS)."""
    expression = "IN_RANGE(discount_pct, 0, 10)"
    dsl = ColumnBoundDSL(
        anchor_table="orders",
        anchor_column="discount_pct",
        dsl_kind=DSLKind.CONSTRAINT,
        profile=column_bound_profile,
        expression=expression
    )
    result = validate_column_bound_dsl(dsl, column_bound_schema, return_model=True)
    assert result.valid, f"Expected valid, got errors: {result.get_error_summary()}"
    assert result.error_count == 0


def test_x1_unknown_anchor_column(column_bound_schema: DSLSchemaContext, column_bound_profile: str):
    """X1: Unknown anchor column."""
    expression = "total_amount >= 0"
    dsl = ColumnBoundDSL(
        anchor_table="orders",
        anchor_column="order_total",
        dsl_kind=DSLKind.CONSTRAINT,
        profile=column_bound_profile,
        expression=expression
    )
    result = validate_column_bound_dsl(dsl, column_bound_schema, return_model=True)
    assert not result.valid, "Expected invalid"
    assert result.error_count > 0


# Add more column-bound tests (V2-V10, X2-X10) - simplified versions
def test_v2_three_hop_lookup_chain(column_bound_schema: DSLSchemaContext, column_bound_profile: str):
    """V2: 3-hop LOOKUP chain with uniqueness."""
    expression = "IN_RANGE(discount_pct, 0, LOOKUP(organization, organization.max_discount_pct WHERE organization.org_id = LOOKUP(customer, customer.org_id WHERE customer.customer_id = LOOKUP(account, account.customer_id WHERE account.account_id = account_id))))"
    dsl = ColumnBoundDSL(
        anchor_table="orders",
        anchor_column="discount_pct",
        dsl_kind=DSLKind.CONSTRAINT,
        profile=column_bound_profile,
        expression=expression
    )
    result = validate_column_bound_dsl(dsl, column_bound_schema, return_model=True)
    assert result.valid, f"Expected valid, got errors: {result.get_error_summary()}"


def test_x2_unknown_table(column_bound_schema: DSLSchemaContext, column_bound_profile: str):
    """X2: Unknown table."""
    expression = "discount_pct <= LOOKUP(invoice, invoice.total WHERE invoice.order_id = order_id)"
    dsl = ColumnBoundDSL(
        anchor_table="orders",
        anchor_column="discount_pct",
        dsl_kind=DSLKind.CONSTRAINT,
        profile=column_bound_profile,
        expression=expression
    )
    result = validate_column_bound_dsl(dsl, column_bound_schema, return_model=True)
    assert not result.valid, "Expected invalid"
    assert result.error_count > 0


# ============================================================================
# Section 6: Multi-Table Hop Tests (Relational Constraints)
# ============================================================================

class TestMultiTableHops:
    """Test multi-table hop expressions with relational constraints profile."""

    @pytest.fixture
    def schema(self):
        return create_comprehensive_test_schema()

    @pytest.mark.parametrize(
        "expr",
        [
            "EXISTS(Order WHERE Order.customer_id = Customer.customer_id)",
            "LOOKUP(Order, Order.total_amount WHERE Order.customer_id = Customer.customer_id)",
            "COUNT_WHERE(Order WHERE Order.customer_id = Customer.customer_id)",
            "SUM_WHERE(Order, Order.total_amount WHERE Order.customer_id = Customer.customer_id)",
            "AVG_WHERE(Order, Order.total_amount WHERE Order.customer_id = Customer.customer_id AND Order.status = 'completed')",
            "MAX_WHERE(Order, Order.total_amount WHERE Order.customer_id = Customer.customer_id)",
            "MIN_WHERE(Order, Order.total_amount WHERE Order.customer_id = Customer.customer_id)",
            "IN_RANGE(Order.total_amount, 0, LOOKUP(Customer, Customer.age WHERE Customer.customer_id = Order.customer_id) * 10)",
        ],
    )
    def test_valid_multi_table_hops(self, schema: DSLSchemaContext, expr: str):
        """Test that valid multi-table hop expressions pass validation."""
        grammar = f"profile:v1+{FEATURE_RELATIONAL_CONSTRAINTS}"
        result = validate_dsl_expression_with_schema(expr, schema, grammar=grammar)
        assert result["valid"] is True, f"Multi-hop expression should be valid: {expr}\nError: {result.get('error')}"

    @pytest.mark.parametrize(
        "expr,expected_error_keywords",
        [
            ("EXISTS(UnknownTable WHERE UnknownTable.col = 1)", ["unknown", "table"]),
            ("LOOKUP(NonExistentTable, Order.total_amount WHERE Order.customer_id = Customer.customer_id)", ["unknown", "table"]),
            ("COUNT_WHERE(InvalidTable WHERE InvalidTable.col = 1)", ["unknown", "table"]),
            ("EXISTS(Order WHERE Order.unknown_col = Customer.customer_id)", ["unknown", "column"]),
            ("LOOKUP(Order, Order.unknown_col WHERE Order.customer_id = Customer.customer_id)", ["unknown", "column"]),
            ("SUM_WHERE(Order, Order.status WHERE Order.customer_id = Customer.customer_id)", ["string", "number", "SUM"]),
            ("AVG_WHERE(Order, Order.status WHERE Order.customer_id = Customer.customer_id)", ["string", "number", "AVG"]),
            ("LOOKUP(Order, Order.total_amount WHERE Order.customer_id = Customer.name)", ["number", "string", "comparison"]),
        ],
    )
    def test_invalid_multi_table_hops(
        self, schema: DSLSchemaContext, expr: str, expected_error_keywords: list | None
    ):
        """Test that invalid multi-table hop expressions fail validation."""
        grammar = f"profile:v1+{FEATURE_RELATIONAL_CONSTRAINTS}"
        result = validate_dsl_expression_with_schema(expr, schema, grammar=grammar)
        assert result["valid"] is False, f"Expression should be invalid: {expr}"
        assert "error" in result and result["error"], f"Error message should be present for: {expr}"
        
        if expected_error_keywords:
            error_msg = result["error"].lower()
            found_keyword = any(keyword.lower() in error_msg for keyword in expected_error_keywords)
            assert found_keyword, (
                f"Error message should contain one of {expected_error_keywords}\n"
                f"Actual error: {result['error']}"
            )


# ============================================================================
# Section 7: Integration Tests (All Three Phases)
# ============================================================================

class TestIntegration:
    """Integration tests covering all three validation phases."""

    @pytest.fixture
    def schema(self):
        return create_comprehensive_test_schema()

    def test_complete_validation_pipeline_valid(self, schema: DSLSchemaContext):
        """Test complete validation pipeline for valid expressions."""
        expr = "IF Customer.age >= 18 AND Customer.is_premium THEN Order.total_amount * 0.9 ELSE Order.total_amount"
        tokens = tokenize_dsl(expr)
        assert len(tokens) > 0
        tree = parse_dsl_expression(expr)
        assert tree is not None
        result = validate_dsl_expression_with_schema(expr, schema)
        assert result["valid"] is True

    def test_complete_validation_pipeline_multi_hop(self, schema: DSLSchemaContext):
        """Test complete validation pipeline for multi-table hop expressions."""
        expr = "EXISTS(Order WHERE Order.customer_id = Customer.customer_id AND Order.total_amount > 100)"
        grammar = f"profile:v1+{FEATURE_RELATIONAL_CONSTRAINTS}"
        profile = DSLGrammarProfile(version="v1", features={FEATURE_RELATIONAL_CONSTRAINTS})
        tokens = tokenize_dsl(expr, profile=profile)
        assert len(tokens) > 0
        tree = parse_dsl_expression(expr, profile=profile)
        assert tree is not None
        result = validate_dsl_expression_with_schema(expr, schema, grammar=grammar)
        assert result["valid"] is True

    def test_complete_validation_pipeline_invalid_syntax(self, schema: DSLSchemaContext):
        """Test that syntax errors are caught before semantic validation."""
        expr = "IF Customer.age >= 18 THEN"
        tokens = tokenize_dsl(expr)
        assert len(tokens) > 0
        with pytest.raises(DSLParseError):
            parse_dsl_expression(expr)
        result = validate_dsl_expression_with_schema(expr, schema)
        assert result["valid"] is False

    def test_complete_validation_pipeline_invalid_semantic(self, schema: DSLSchemaContext):
        """Test that semantic errors are caught after syntax validation."""
        expr = "Customer.name + 10"
        tokens = tokenize_dsl(expr)
        assert len(tokens) > 0
        tree = parse_dsl_expression(expr)
        assert tree is not None
        result = validate_dsl_expression_with_schema(expr, schema)
        assert result["valid"] is False
        assert "error" in result and result["error"]


# ============================================================================
# Section 8: Distribution Parameter Constraints Tests
# ============================================================================

class TestDistributionConstraints:
    """Test distribution parameter value constraints."""

    @pytest.fixture
    def schema(self):
        return create_comprehensive_test_schema()

    def test_distribution_parameter_constraints(self, schema: DSLSchemaContext):
        """Test distribution parameter value constraints (numeric literals only)."""
        invalid_cases = [
            ("Customer.age ~ UNIFORM(65, 18)", "min >= max"),
            ("Customer.age ~ UNIFORM(10, 10)", "min >= max"),
            ("Customer.age ~ NORMAL(0, 0)", "std_dev <= 0"),
            ("Customer.age ~ NORMAL(0, -1)", "std_dev <= 0"),
            ("Customer.age ~ EXPONENTIAL(0)", "lambda <= 0"),
            ("Customer.age ~ EXPONENTIAL(-1)", "lambda <= 0"),
            ("Customer.age ~ POISSON(0)", "lambda <= 0"),
            ("Customer.age ~ POISSON(-1)", "lambda <= 0"),
            ("Customer.is_premium ~ BERNOULLI(1.5)", "probability > 1"),
            ("Customer.is_premium ~ BERNOULLI(-0.1)", "probability < 0"),
            ("Customer.age ~ TRIANGULAR(10, 5, 7)", "min >= max"),
            ("Customer.age ~ TRIANGULAR(5, 10, 15)", "mode > max"),
            ("Customer.age ~ TRIANGULAR(5, 10, 3)", "mode < min"),
            ("Customer.age ~ ZIPF(0, 10)", "s <= 0"),
            ("Customer.age ~ ZIPF(-1, 10)", "s <= 0"),
            ("Customer.age ~ ZIPF(1.5, 0)", "n < 1"),
            ("Customer.age ~ ZIPF(1.5, 2.5)", "n not integer"),
            ("Customer.age ~ LOGNORMAL(0, 0)", "sigma <= 0"),
            ("Customer.age ~ LOGNORMAL(0, -1)", "sigma <= 0"),
            ("Customer.age ~ BETA(0, 1)", "alpha <= 0"),
            ("Customer.age ~ BETA(1, 0)", "beta <= 0"),
            ("Customer.age ~ GAMMA(0, 1)", "shape <= 0"),
            ("Customer.age ~ GAMMA(1, 0)", "scale <= 0"),
            ("Customer.age ~ WEIBULL(0, 1)", "shape <= 0"),
            ("Customer.age ~ WEIBULL(1, 0)", "scale <= 0"),
            ("Customer.age ~ PARETO(0, 1)", "alpha <= 0"),
            ("Customer.age ~ PARETO(1, 0)", "scale <= 0"),
        ]
        
        for expr, constraint_desc in invalid_cases:
            result = validate_dsl_expression_with_schema(expr, schema)
            assert result["valid"] is False, (
                f"Expression should fail constraint check ({constraint_desc}): {expr}\n"
                f"Error: {result.get('error')}"
            )

    def test_categorical_distribution_validation(self, schema: DSLSchemaContext):
        """Test CATEGORICAL distribution validation."""
        invalid_cases = [
            ("Order.status ~ CATEGORICAL(('active', 1.0))", "too few pairs"),
            ("Order.status ~ CATEGORICAL(('active', -0.1), ('pending', 0.2))", "negative weight"),
            ("Order.status ~ CATEGORICAL(('active', 0.0), ('pending', 0.0))", "zero sum"),
            ("Order.status ~ CATEGORICAL('active', 0.5, 'pending', 0.5)", "not pairs"),
        ]
        
        for expr, issue_desc in invalid_cases:
            result = validate_dsl_expression_with_schema(expr, schema)
            assert result["valid"] is False, (
                f"Expression should fail CATEGORICAL validation ({issue_desc}): {expr}\n"
                f"Error: {result.get('error')}"
            )

    def test_function_arity_validation(self, schema: DSLSchemaContext):
        """Test function arity validation."""
        invalid_cases = [
            ("LOWER()", "too few args"),
            ("CONCAT(Customer.name)", "too few args"),
            ("LOWER(Customer.name, 'extra')", "too many args"),
            ("UPPER(Customer.name, 'extra', 'more')", "too many args"),
            ("ABS(Customer.age, 10)", "too many args"),
        ]
        
        for expr, issue_desc in invalid_cases:
            result = validate_dsl_expression_with_schema(expr, schema)
            assert result["valid"] is False, (
                f"Expression should fail arity check ({issue_desc}): {expr}\n"
                f"Error: {result.get('error')}"
            )

    def test_identifier_resolution(self, schema: DSLSchemaContext):
        """Test identifier resolution rules."""
        result = validate_dsl_expression_with_schema("Customer.customer_id + 1", schema)
        assert result["valid"] is True
        result = validate_dsl_expression_with_schema("customer_id + 1", schema)
        assert result["valid"] is False
        assert "ambiguous" in result["error"].lower() or "qualified" in result["error"].lower()
        result = validate_dsl_expression_with_schema("NonExistentTable.column + 1", schema)
        assert result["valid"] is False
        assert "unknown" in result["error"].lower() or "table" in result["error"].lower()
        result = validate_dsl_expression_with_schema("Customer.unknown_column + 1", schema)
        assert result["valid"] is False
        assert "unknown" in result["error"].lower() or "column" in result["error"].lower()
        result = validate_dsl_expression_with_schema("schema.Customer.customer_id + 1", schema)
        assert result["valid"] is False

    def test_complex_nested_expressions(self, schema: DSLSchemaContext):
        """Test complex nested expressions with semantic validation."""
        valid_cases = [
            "IF Customer.age >= 18 THEN (Order.total_amount * 1.1) ELSE Order.total_amount",
            "CASE WHEN Customer.is_premium THEN Product.price * 0.9 ELSE Product.price END",
            "CONCAT(UPPER(Customer.name), ' - ', Customer.email)",
            "IF Customer.email IS NULL THEN 'No email' ELSE Customer.email",
            "Customer.age BETWEEN 18 AND 65 AND Customer.is_premium = true",
        ]
        
        for expr in valid_cases:
            result = validate_dsl_expression_with_schema(expr, schema)
            assert result["valid"] is True, (
                f"Complex expression should be valid: {expr}\n"
                f"Error: {result.get('error')}"
            )
        
        invalid_cases = [
            ("IF Customer.name >= 18 THEN 1 ELSE 0", "string comparison with number"),
            ("CONCAT(Customer.age, Customer.name)", "number in CONCAT"),
            ("UPPER(Customer.age)", "number in UPPER"),
        ]
        
        for expr, issue_desc in invalid_cases:
            result = validate_dsl_expression_with_schema(expr, schema)
            assert result["valid"] is False, (
                f"Complex expression should fail ({issue_desc}): {expr}\n"
                f"Error: {result.get('error')}"
            )


# ============================================================================
# Section 9: Decomposition DSL Semantics Tests
# ============================================================================

def test_validate_decomposition_dsls_requires_all_subattrs_and_only_composite_identifier():
    """Test that decomposition DSLs require all subattrs and only composite identifier."""
    issues = _validate_decomposition_dsls(
        composite_attr="address",
        decomposition=["street", "city"],
        decomposition_dsls={"street": "SPLIT_PART(address, ',', 1)"},
    )
    assert issues  # missing 'city' DSL

    issues2 = _validate_decomposition_dsls(
        composite_attr="address",
        decomposition=["street"],
        decomposition_dsls={"street": "SPLIT_PART(other, ',', 1)"},
    )
    assert issues2  # must reference address, not other


def test_validate_decomposition_dsls_accepts_valid_expr():
    """Test that valid decomposition DSL expressions are accepted."""
    issues = _validate_decomposition_dsls(
        composite_attr="address",
        decomposition=["street"],
        decomposition_dsls={"street": "SPLIT_PART(address, ',', 1)"},
    )
    assert issues == []
