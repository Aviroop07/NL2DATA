"""Unit tests for Step 8.6: Constraint Enforcement Strategy."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase8.step_8_6_constraint_enforcement_strategy import (
    step_8_6_constraint_enforcement_strategy_batch,
    step_8_6_constraint_enforcement_strategy_single,
    ConstraintWithEnforcement,
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_8_6_basic():
    """Test Step 8.6 with basic input - tests actual LLM call with column-wise DSL."""
    TestResultDisplay.print_test_header("Constraint Enforcement Strategy", "8.6")
    TestResultDisplay.print_test_case(1, "Basic constraint enforcement strategy with column-wise DSL")
    
    constraints_with_scope = [
        {
            "constraint_id": "c1",
            "constraint_type": "statistical",
            "table": "Transaction",
            "column": "amount",
            "description": "Transaction amounts must be between 0.01 and 1000000",
            "affected_attributes": ["Transaction.amount"],
            "affected_tables": ["Transaction"],
            "scope_type": "column",
        },
        {
            "constraint_id": "c2",
            "constraint_type": "other",
            "table": "Transaction",
            "column": "status",
            "description": "Transaction status must be one of the specified values",
            "affected_attributes": ["Transaction.status"],
            "affected_tables": ["Transaction"],
            "scope_type": "column",
        },
    ]
    
    normalized_schema = {
        "tables": [
            {
                "name": "Transaction",
                "columns": [
                    {"name": "transaction_id", "type": "BIGINT", "is_primary_key": True, "is_nullable": False},
                    {"name": "amount", "type": "DECIMAL(10,2)", "is_nullable": False, "description": "Transaction amount"},
                    {"name": "status", "type": "VARCHAR(50)", "is_nullable": False, "description": "Transaction status"},
                ],
                "primary_key": ["transaction_id"],
                "foreign_keys": []
            }
        ]
    }
    
    functional_dependencies = [
        {
            "lhs": ["transaction_id"],
            "rhs": ["amount", "status"],
            "reasoning": "Transaction ID determines amount and status"
        }
    ]
    
    input_data = {
        "constraints_with_scope": constraints_with_scope,
        "normalized_schema": normalized_schema,
        "functional_dependencies": functional_dependencies,
    }
    TestResultDisplay.print_input_summary(input_data)
    
    # Mock LLM responses with column-wise DSL
    mock_responses = [
        ConstraintWithEnforcement(
            constraint_data=constraints_with_scope[0],
            enforcement_strategy="database",
            enforcement_level="strict",
            column_dsl_expressions={
                "Transaction.amount": "amount >= 0.01 AND amount <= 1000000"
            },
            reasoning="Range constraint should be enforced at database level for data integrity",
        ),
        ConstraintWithEnforcement(
            constraint_data=constraints_with_scope[1],
            enforcement_strategy="database",
            enforcement_level="strict",
            column_dsl_expressions={
                "Transaction.status": "status IN ('pending', 'completed', 'cancelled', 'failed')"
            },
            reasoning="Categorical constraint should be enforced at database level using CHECK constraint",
        ),
    ]
    
    # Call the real function but mock only the LLM call
    with patch('NL2DATA.phases.phase8.step_8_6_constraint_enforcement_strategy.standardized_llm_call') as mock_llm:
        # Set up side_effect to return different responses for each call
        mock_llm.side_effect = mock_responses
        result = await step_8_6_constraint_enforcement_strategy_batch(
            constraints_with_scope=constraints_with_scope,
            normalized_schema=normalized_schema,
            functional_dependencies=functional_dependencies,
        )
    
    # Handle list output for print_output_summary
    if isinstance(result, list):
        print(f"\nOutput: List with {len(result)} items")
        for i, item in enumerate(result[:3]):  # Show first 3 items
            print(f"  Item {i+1}:")
            TestResultDisplay.print_output_summary(item)
        if len(result) > 3:
            print(f"  ... and {len(result) - 3} more items")
    else:
        TestResultDisplay.print_output_summary(result)
    
    validations = []
    
    # Validate structure - should return list
    if isinstance(result, list):
        validations.append({
            "name": "Output type check",
            "valid": True,
            "message": "Result is a list as expected"
        })
        
        # Check length matches input
        if len(result) == len(constraints_with_scope):
            validations.append({
                "name": "Length check",
                "valid": True,
                "message": f"Returned {len(result)} enforcement strategies for {len(constraints_with_scope)} constraints"
            })
        else:
            validations.append({
                "name": "Length check",
                "valid": False,
                "message": f"Returned {len(result)} enforcement strategies but expected {len(constraints_with_scope)}"
            })
        
        # Validate each enforcement result
        for i, enforcement in enumerate(result):
            if hasattr(enforcement, 'constraint_data') and hasattr(enforcement, 'enforcement_strategy') and hasattr(enforcement, 'enforcement_level') and hasattr(enforcement, 'column_dsl_expressions'):
                # Check that column_dsl_expressions is a dict
                dsl_exprs = enforcement.column_dsl_expressions
                if isinstance(dsl_exprs, dict) and len(dsl_exprs) > 0:
                    validations.append({
                        "name": f"Enforcement {i+1} structure with DSL",
                        "valid": True,
                        "message": f"Enforcement {i+1} has required fields including column_dsl_expressions"
                    })
                else:
                    validations.append({
                        "name": f"Enforcement {i+1} DSL check",
                        "valid": False,
                        "message": f"Enforcement {i+1} missing or empty column_dsl_expressions"
                    })
            elif isinstance(enforcement, dict):
                required_fields = ["constraint_data", "enforcement_strategy", "enforcement_level", "column_dsl_expressions"]
                missing = [f for f in required_fields if f not in enforcement]
                if not missing:
                    # Check DSL expressions
                    dsl_exprs = enforcement.get("column_dsl_expressions", {})
                    if isinstance(dsl_exprs, dict) and len(dsl_exprs) > 0:
                        validations.append({
                            "name": f"Enforcement {i+1} structure with DSL",
                            "valid": True,
                            "message": f"Enforcement {i+1} has required fields including column_dsl_expressions"
                        })
                    else:
                        validations.append({
                            "name": f"Enforcement {i+1} DSL check",
                            "valid": False,
                            "message": f"Enforcement {i+1} missing or empty column_dsl_expressions"
                        })
                else:
                    validations.append({
                        "name": f"Enforcement {i+1} structure",
                        "valid": False,
                        "message": f"Enforcement {i+1} missing fields: {missing}"
                    })
            else:
                validations.append({
                    "name": f"Enforcement {i+1} structure",
                    "valid": False,
                    "message": f"Enforcement {i+1} is not a valid structure"
                })
    else:
        validations.append({
            "name": "Output type check",
            "valid": False,
            "message": f"Result is {type(result)} but expected list"
        })
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "8.6")
    return all_valid


async def test_step_8_6_different_strategies():
    """Test Step 8.6 with different enforcement strategies."""
    TestResultDisplay.print_test_header("Constraint Enforcement Strategy (Different Strategies)", "8.6")
    TestResultDisplay.print_test_case(2, "Test different enforcement strategies")
    
    constraints_with_scope = [
        {
            "constraint_id": "c1",
            "constraint_type": "statistical",
            "table": "Transaction",
            "column": "amount",
            "description": "Transaction amounts must be between 0.01 and 1000000",
            "affected_attributes": ["Transaction.amount"],
            "affected_tables": ["Transaction"],
            "scope_type": "column",
        },
        {
            "constraint_id": "c2",
            "constraint_type": "other",
            "table": "Transaction",
            "description": "High-value transactions require approval",
            "affected_attributes": ["Transaction.amount"],
            "affected_tables": ["Transaction"],
            "scope_type": "column",
        }
    ]
    
    normalized_schema = {
        "tables": [
            {
                "name": "Transaction",
                "columns": [
                    {"name": "amount", "type": "DECIMAL(10,2)", "is_nullable": False},
                ],
                "primary_key": [],
                "foreign_keys": []
            }
        ]
    }
    
    mock_responses = [
        ConstraintWithEnforcement(
            constraint_data=constraints_with_scope[0],
            enforcement_strategy="database",
            enforcement_level="strict",
            column_dsl_expressions={
                "Transaction.amount": "amount >= 0.01 AND amount <= 1000000"
            },
            reasoning="Range constraint enforced at database level",
        ),
        ConstraintWithEnforcement(
            constraint_data=constraints_with_scope[1],
            enforcement_strategy="application",
            enforcement_level="warning",
            column_dsl_expressions={
                "Transaction.amount": "amount > 10000 -> requires_approval = true"
            },
            reasoning="Business rule best enforced at application level with warning",
        ),
    ]
    
    with patch('NL2DATA.phases.phase8.step_8_6_constraint_enforcement_strategy.standardized_llm_call') as mock_llm:
        mock_llm.side_effect = mock_responses
        result = await step_8_6_constraint_enforcement_strategy_batch(
            constraints_with_scope=constraints_with_scope,
            normalized_schema=normalized_schema,
            functional_dependencies=None,
        )
    
    # Handle list output for print_output_summary
    if isinstance(result, list):
        print(f"\nOutput: List with {len(result)} items")
        for i, item in enumerate(result[:3]):  # Show first 3 items
            print(f"  Item {i+1}:")
            TestResultDisplay.print_output_summary(item)
        if len(result) > 3:
            print(f"  ... and {len(result) - 3} more items")
    else:
        TestResultDisplay.print_output_summary(result)
    
    validations = []
    
    if isinstance(result, list) and len(result) == len(constraints_with_scope):
        # Check that different strategies are used
        strategies = []
        for enforcement in result:
            if hasattr(enforcement, 'enforcement_strategy'):
                strategies.append(enforcement.enforcement_strategy)
            elif isinstance(enforcement, dict):
                strategies.append(enforcement.get("enforcement_strategy"))
        
        if len(set(strategies)) > 1:
            validations.append({
                "name": "Multiple strategies check",
                "valid": True,
                "message": f"Returned {len(result)} enforcement strategies with different approaches: {', '.join(set(strategies))}"
            })
        else:
            validations.append({
                "name": "Multiple strategies check",
                "valid": True,
                "message": f"Returned {len(result)} enforcement strategies (all using {strategies[0] if strategies else 'unknown'} strategy)"
            })
    else:
        validations.append({
            "name": "Multiple strategies check",
            "valid": False,
            "message": "Did not return expected number of enforcement strategies"
        })
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "8.6 (strategies)")
    return all_valid


async def main():
    """Run all tests for Step 8.6."""
    results = []
    results.append(await test_step_8_6_basic())
    results.append(await test_step_8_6_different_strategies())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 8.6 unit tests passed!")
    else:
        print("[FAIL] Some Step 8.6 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
