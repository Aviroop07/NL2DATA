"""Unit tests for Step 6.3: DDL Error Correction."""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase6.step_6_3_ddl_error_correction import (
    step_6_3_ddl_error_correction,
    DDLErrorCorrectionOutput,
    Patch,
    DDLCorrection
)
from NL2DATA.tests.utils.test_helpers import (
    LLMMockHelper,
    ValidationHelper,
    TestResultDisplay
)


async def test_step_6_3_basic():
    """Test Step 6.3 with basic input."""
    TestResultDisplay.print_test_header("DDL Error Correction", "6.3")
    TestResultDisplay.print_test_case(1, "Basic DDL error correction")
    
    validation_errors = {
        "validation_passed": False,
        "syntax_errors": [
            {
                "error": "Syntax error: missing closing parenthesis and comma",
                "statement": "CREATE TABLE Transaction (transaction_id BIGINT PRIMARY KEY, card_id BIGINT NOT NULL, merchant_id BIGINT NOT NULL"
            },
            {
                "error": "Type mismatch: DECIMAL precision/scale syntax error",
                "statement": "fraud_risk_score DECIMAL(5,2) NULL"
            }
        ],
        "naming_conflicts": []
    }
    original_ddl = [
        "CREATE TABLE Transaction (transaction_id BIGINT PRIMARY KEY, card_id BIGINT NOT NULL, merchant_id BIGINT NOT NULL",
        "transaction_datetime TIMESTAMP NOT NULL, amount_usd DECIMAL(12,2) NOT NULL, exchange_rate DECIMAL(8,4) NULL, fraud_risk_score DECIMAL(5,2) NULL"
    ]
    normalized_schema = {
        "normalized_tables": [
            {
                "name": "Transaction",
                "columns": [
                    {"name": "transaction_id", "type": "BIGINT", "nullable": False},
                    {"name": "card_id", "type": "BIGINT", "nullable": False},
                    {"name": "merchant_id", "type": "BIGINT", "nullable": False},
                    {"name": "transaction_datetime", "type": "TIMESTAMP", "nullable": False},
                    {"name": "amount_usd", "type": "DECIMAL(12,2)", "nullable": False},
                    {"name": "exchange_rate", "type": "DECIMAL(8,4)", "nullable": True},
                    {"name": "fraud_risk_score", "type": "DECIMAL(5,2)", "nullable": True}
                ],
                "primary_key": ["transaction_id"],
                "foreign_keys": [
                    {"from_attributes": ["card_id"], "to_entity": "Card", "to_attributes": ["card_id"]},
                    {"from_attributes": ["merchant_id"], "to_entity": "Merchant", "to_attributes": ["merchant_id"]}
                ]
            }
        ]
    }
    input_data = {
        "validation_errors": validation_errors,
        "original_ddl": original_ddl,
        "normalized_schema": normalized_schema
    }
    TestResultDisplay.print_input_summary(input_data)
    
    mock_response = DDLErrorCorrectionOutput(
        ir_patches=[
            Patch(
                operation="fix_syntax",
                target="Transaction.card_id",
                changes={"add_comma": True, "add_closing_paren": True},
                reasoning="Missing comma after merchant_id and closing parenthesis"
            ),
            Patch(
                operation="fix_type",
                target="Transaction.fraud_risk_score",
                changes={"fix_decimal_syntax": True},
                reasoning="DECIMAL type syntax is correct, but statement needs proper closing"
            )
        ],
        corrections=[
            DDLCorrection(
                original="CREATE TABLE Transaction (transaction_id BIGINT PRIMARY KEY, card_id BIGINT NOT NULL, merchant_id BIGINT NOT NULL",
                corrected="CREATE TABLE Transaction (transaction_id BIGINT PRIMARY KEY, card_id BIGINT NOT NULL, merchant_id BIGINT NOT NULL,",
                reasoning="Added missing comma after merchant_id"
            ),
            DDLCorrection(
                original="transaction_datetime TIMESTAMP NOT NULL, amount_usd DECIMAL(12,2) NOT NULL, exchange_rate DECIMAL(8,4) NULL, fraud_risk_score DECIMAL(5,2) NULL",
                corrected="transaction_datetime TIMESTAMP NOT NULL, amount_usd DECIMAL(12,2) NOT NULL, exchange_rate DECIMAL(8,4) NULL, fraud_risk_score DECIMAL(5,2) NULL);",
                reasoning="Added missing closing parenthesis and semicolon"
            )
        ],
        reasoning="Fixed syntax errors: added missing comma and closing parenthesis with semicolon"
    )
    
    with LLMMockHelper.patch_standardized_llm_call(mock_response):
        with LLMMockHelper.patch_model_router("6.3"):
            result = await step_6_3_ddl_error_correction(
                validation_errors=validation_errors,
                original_ddl=original_ddl,
                normalized_schema=normalized_schema
            )
    
    TestResultDisplay.print_output_summary(result)
    
    validations = []
    struct_validation = ValidationHelper.validate_output_structure(
        result,
        ["ir_patches", "corrections", "reasoning"]
    )
    validations.append(struct_validation)
    
    type_validation = ValidationHelper.validate_types(
        result,
        {
            "ir_patches": list,
            "corrections": list,
            "reasoning": str
        }
    )
    validations.append(type_validation)
    
    all_valid = all(v.get("valid", False) for v in validations)
    for validation in validations:
        TestResultDisplay.print_validation_results(validation)
    
    TestResultDisplay.print_test_summary(all_valid, "6.3")
    return all_valid


async def main():
    """Run all tests for Step 6.3."""
    results = []
    results.append(await test_step_6_3_basic())
    
    print("\n" + "=" * 80)
    if all(results):
        print("[PASS] All Step 6.3 unit tests passed!")
    else:
        print("[FAIL] Some Step 6.3 unit tests failed")
    print("=" * 80)
    
    return all(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
