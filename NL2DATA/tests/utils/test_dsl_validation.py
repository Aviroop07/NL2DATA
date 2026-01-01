import pytest

from NL2DATA.utils.dsl.validator import validate_dsl_expression_strict, validate_dsl_expression
from NL2DATA.utils.dsl.validator import validate_dsl_expression_with_schema
from NL2DATA.utils.dsl.schema_context import DSLSchemaContext, DSLTableSchema, build_schema_context_from_relational_schema


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
    out = validate_dsl_expression_strict(expr)
    assert out["valid"] is False
    assert isinstance(out.get("error"), str) and out["error"]


def test_unknown_function_is_rejected() -> None:
    out = validate_dsl_expression_strict("FOO(1)")
    assert out["valid"] is False
    assert isinstance(out.get("error"), str) and out["error"]


def test_function_arity_is_enforced() -> None:
    out = validate_dsl_expression_strict("LOWER(a, b)")
    assert out["valid"] is False
    assert isinstance(out.get("error"), str) and out["error"]
    out2 = validate_dsl_expression_strict("DATEADD('day', 1)")
    assert out2["valid"] is False
    assert isinstance(out2.get("error"), str) and out2["error"]


def test_dotted_function_name_is_rejected() -> None:
    out = validate_dsl_expression_strict("schema.LOWER(name)")
    assert out["valid"] is False
    assert isinstance(out.get("error"), str) and out["error"]


def test_dotted_distribution_name_is_rejected() -> None:
    out = validate_dsl_expression_strict("x ~ schema.NORMAL(0, 1)")
    assert out["valid"] is False
    assert isinstance(out.get("error"), str) and out["error"]


def test_optional_grammar_features_disabled_by_default() -> None:
    out = validate_dsl_expression("a BETWEEN 1 AND 2")
    assert out["valid"] is False
    out2 = validate_dsl_expression("a IS NULL")
    assert out2["valid"] is False


def test_optional_grammar_features_enabled_via_profile() -> None:
    out = validate_dsl_expression("a BETWEEN 1 AND 2", grammar="profile:v1+between")
    assert out["valid"] is True, out
    out2 = validate_dsl_expression("a IS NULL", grammar="profile:v1+is_null")
    assert out2["valid"] is True, out2
    out3 = validate_dsl_expression("a IS NOT NULL", grammar="profile:v1+is_null")
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
    out = validate_dsl_expression_strict(expr)
    assert out["valid"] is False
    assert isinstance(out.get("error"), str) and out["error"]


def _schema() -> DSLSchemaContext:
    # Minimal schema context: tables, columns, and coarse types.
    # Note: We intentionally make "id" ambiguous across tables to test ambiguity handling.
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


def _ok_sem(expr: str) -> None:
    out = validate_dsl_expression_with_schema(expr, _schema())
    assert out["valid"] is True, out


def _bad_sem(expr: str) -> None:
    out = validate_dsl_expression_with_schema(expr, _schema())
    assert out["valid"] is False, out
    assert isinstance(out.get("error"), str) and out["error"]


def test_semantic_identifier_unknown_column() -> None:
    _bad_sem("unknown_col + 1")


def test_semantic_identifier_ambiguous_bare_column() -> None:
    # "id" exists in both Student and Course -> must qualify
    _bad_sem("id + 1")
    _ok_sem("Student.id + 1")


def test_semantic_identifier_disallow_deeper_paths() -> None:
    _bad_sem("schema.Student.id + 1")


def test_semantic_type_arithmetic_mismatch() -> None:
    # name is string -> numeric operator should fail
    _bad_sem("Student.name + 1")


def test_semantic_type_boolean_mismatch() -> None:
    _bad_sem("Student.age AND Student.is_active")
    _ok_sem("Student.is_active AND (Student.age > 18)")


def test_semantic_like_requires_string() -> None:
    _bad_sem("Student.age LIKE '1%'")
    _ok_sem("Student.name LIKE 'A%'")


def test_semantic_in_list_type_mismatch() -> None:
    _bad_sem("Student.age IN ['10','20']")
    _ok_sem("Student.name IN ['Alice','Bob']")


def test_semantic_distribution_target_must_be_identifier() -> None:
    _bad_sem("(Student.age + 1) ~ NORMAL(0, 1)")


def test_semantic_distribution_target_type_check() -> None:
    _bad_sem("Student.name ~ NORMAL(0, 1)")
    _ok_sem("Student.age ~ NORMAL(0, 1)")


def test_schema_context_builder_from_relational_schema_smoke() -> None:
    ctx = build_schema_context_from_relational_schema(
        {
            "tables": [
                {"name": "T", "columns": [{"name": "x", "type": "INT"}, {"name": "y", "type": "VARCHAR(10)"}]}
            ]
        }
    )
    out1 = validate_dsl_expression_with_schema("T.x + 1", ctx)
    assert out1["valid"] is True, out1
    out2 = validate_dsl_expression_with_schema("T.y + 1", ctx)
    assert out2["valid"] is False, out2


def test_semantic_distribution_bounds_checks_literals() -> None:
    _bad_sem("Student.age ~ NORMAL(0, -1)")
    _bad_sem("Student.age ~ UNIFORM(10, 5)")
    _bad_sem("Student.age ~ EXPONENTIAL(0)")
    _bad_sem("Student.age ~ POISSON(-2)")
    _bad_sem("Student.age ~ ZIPF(1.2, 0)")
    _bad_sem("Student.age ~ ZIPF(1.2, 2.5)")
    _bad_sem("Student.age ~ TRIANGULAR(0, 10, 11)")
    _bad_sem("Student.is_active ~ BERNOULLI(1.2)")


def test_semantic_categorical_weight_checks_literals() -> None:
    _bad_sem("Student.name ~ CATEGORICAL(('a', -0.1), ('b', 0.2))")
    _bad_sem("Student.name ~ CATEGORICAL(('a', 0.0), ('b', 0.0))")
    _ok_sem("Student.name ~ CATEGORICAL(('a', 0.2), ('b', 0.8))")


def test_semantic_dateadd_datediff_signatures() -> None:
    _ok_sem("DATEADD('day', 1, Student.created_at)")
    _bad_sem("DATEADD('day', '1', Student.created_at)")
    _bad_sem("DATEADD(1, 1, Student.created_at)")
    _ok_sem("DATEDIFF('day', Student.created_at, Student.created_at)")

